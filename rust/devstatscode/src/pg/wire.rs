//! Minimal blocking PostgreSQL wire-protocol (v3) client.
//!
//! This is the Rust stand-in for `github.com/lib/pq` as used by DevStats
//! through `database/sql`. Only the parts DevStats needs are implemented and
//! they follow lib/pq's choices so that the values DevStats sees are identical:
//!
//! * startup parameters `client_encoding=UTF8`, `extra_float_digits=2`,
//!   `datestyle=ISO, MDY`, `database`, `user` (+ `PGAPPNAME`, `PGTZ`,
//!   `PGOPTIONS`, `PGGEQO` from the environment, like lib/pq's
//!   `parseEnviron`);
//! * trust, cleartext, MD5 and SCRAM-SHA-256 authentication;
//! * statements **without** parameters use the simple query protocol
//!   (multi-statement strings are allowed, exactly like lib/pq);
//! * statements **with** parameters use the extended protocol with the unnamed
//!   statement/portal: `Parse` + `Describe` + `Sync`, then `Bind` + `Execute`
//!   and `Sync`, all parameters and all result columns in **text** format
//!   (lib/pq asks for binary int/bytea results but decodes them to the very
//!   same Go values, so the text form is observably identical);
//! * `FATAL` server errors, EOF and I/O errors mark the connection as bad and
//!   surface as [`PgError::BadConn`] (Go `driver.ErrBadConn`) so the pool can
//!   retry, other server errors surface as [`PgError::Server`] (Go `*pq.Error`).
//!
//! TLS is not implemented: `sslmode` `disable`/`allow`/`prefer` connect in
//! clear text (`prefer`/`allow` fall back to clear text with lib/pq too when
//! the server has no SSL, which is the DevStats deployment reality), the
//! `require`/`verify-*` modes are rejected with a clear error.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::{ErrorFields, Message};
use postgres_protocol::message::frontend;
use postgres_protocol::IsNull;

use super::value::{encode_arg, Column, OID_BYTEA};
use super::{PgError, ServerError, SqlArg};
use crate::context::Ctx;
use crate::error::go_io_error_string;

/// Everything needed to open a connection (lib/pq's `values` map after all
/// defaults/environment/DSN processing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnConfig {
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub sslmode: String,
    /// Extra run-time parameters sent in the startup packet
    /// (`application_name`, `timezone`, `options`, `geqo`).
    pub params: Vec<(String, String)>,
    /// `PGCONNECT_TIMEOUT` (`connect_timeout`), `None`/0 = wait indefinitely.
    pub connect_timeout: Option<Duration>,
}

impl ConnConfig {
    /// Configuration for DevStats' connection string
    /// `client_encoding=UTF8 sslmode='..' host='..' port=.. dbname='..' user='..' password='..'`
    /// plus the environment variables lib/pq honours (`parseEnviron`) that the
    /// DSN does not override.
    pub fn from_ctx(ctx: &Ctx, db: &str) -> ConnConfig {
        let mut params = Vec::new();
        for (env, key) in [
            ("PGOPTIONS", "options"),
            ("PGAPPNAME", "application_name"),
            ("PGTZ", "timezone"),
            ("PGGEQO", "geqo"),
        ] {
            if let Ok(v) = std::env::var(env) {
                params.push((key.to_string(), v));
            }
        }
        let connect_timeout = std::env::var("PGCONNECT_TIMEOUT")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|s| *s > 0)
            .map(Duration::from_secs);
        ConnConfig {
            host: ctx.pg_host.clone(),
            port: ctx.pg_port.clone(),
            user: ctx.pg_user.clone(),
            password: ctx.pg_pass.clone(),
            database: db.to_string(),
            sslmode: ctx.pg_ssl.clone(),
            params,
            connect_timeout,
        }
    }

    /// lib/pq `network()`: a host starting with `/` is a Unix socket directory.
    pub fn is_unix(&self) -> bool {
        self.host.starts_with('/')
    }

    /// Socket path (`<host>/.s.PGSQL.<port>`) or `host:port`.
    pub fn address(&self) -> String {
        if self.is_unix() {
            format!("{}/.s.PGSQL.{}", self.host.trim_end_matches('/'), self.port)
        } else {
            join_host_port(&self.host, &self.port)
        }
    }
}

/// Go `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    }
}

enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            Stream::Unix(s) => s.read(buf),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Tcp(s) => s.write_all(buf),
            Stream::Unix(s) => s.write_all(buf),
        }
    }
}

/// Transaction status reported by `ReadyForQuery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    /// `I` — idle, not in a transaction.
    Idle,
    /// `T` — inside a transaction block.
    InTransaction,
    /// `E` — inside a failed transaction block.
    Failed,
}

/// Rows-affected information from a `CommandComplete` tag (Go `driver.Result`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecResult {
    /// `RowsAffected(n)` — `n` is 0 for tags without a count (`CREATE TABLE`).
    RowsAffected(i64),
    /// Response to an empty query string: `RowsAffected()` errors in Go.
    EmptyQuery,
}

impl ExecResult {
    /// Go `sql.Result.RowsAffected()`.
    pub fn rows_affected(&self) -> Result<i64, PgError> {
        match self {
            ExecResult::RowsAffected(n) => Ok(*n),
            ExecResult::EmptyQuery => Err(PgError::Other(
                "no RowsAffected available after the empty statement".to_string(),
            )),
        }
    }
}

/// lib/pq `parseComplete`: rows affected from a command tag.
pub fn parse_command_tag(tag: &str) -> Result<i64, PgError> {
    for prefix in ["SELECT ", "UPDATE ", "DELETE ", "FETCH ", "MOVE ", "COPY "] {
        if let Some(rest) = tag.strip_prefix(prefix) {
            return rest.parse::<i64>().map_err(|e| {
                PgError::pq(format!(
                    "could not parse commandTag: strconv.ParseInt: parsing {:?}: {}",
                    rest,
                    go_parse_int_error(e)
                ))
            });
        }
    }
    if tag.starts_with("INSERT ") {
        let parts: Vec<&str> = tag.split(' ').collect();
        if parts.len() != 3 {
            return Err(PgError::pq(format!(
                "unexpected INSERT command tag {}",
                tag
            )));
        }
        return parts[2].parse::<i64>().map_err(|e| {
            PgError::pq(format!(
                "could not parse commandTag: strconv.ParseInt: parsing {:?}: {}",
                parts[2],
                go_parse_int_error(e)
            ))
        });
    }
    Ok(0)
}

fn go_parse_int_error(e: std::num::ParseIntError) -> &'static str {
    match e.kind() {
        std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
            "value out of range"
        }
        _ => "invalid syntax",
    }
}

/// One open backend connection.
pub struct Conn {
    stream: Stream,
    rbuf: BytesMut,
    readbuf: Vec<u8>,
    /// Message saved by a look-ahead (`postExecuteWorkaround`), delivered by
    /// the next `recv`.
    pub(super) saved: Option<Message>,
    /// `server_version` as `major*10000 + minor*100` (lib/pq `serverVersion`).
    pub server_version: i32,
    /// Session `TimeZone` parameter.
    pub timezone: String,
    pub process_id: i32,
    pub secret_key: i32,
    pub txn_status: TxnStatus,
    /// Set once the connection must not be reused (Go `cn.err` = ErrBadConn).
    pub broken: bool,
    /// A query was sent and its `ReadyForQuery` has not been consumed yet.
    pub in_flight: bool,
    address: String,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("address", &self.address)
            .field("server_version", &self.server_version)
            .field("timezone", &self.timezone)
            .field("txn_status", &self.txn_status)
            .field("broken", &self.broken)
            .finish()
    }
}

fn dial_error(network: &str, address: &str, err: &std::io::Error) -> PgError {
    let msg = go_io_error_string(err);
    if err.kind() == std::io::ErrorKind::TimedOut {
        return PgError::Net(format!("dial {network} {address}: i/o timeout"));
    }
    PgError::Net(format!("dial {network} {address}: connect: {msg}"))
}

impl Conn {
    /// Dial and run the startup/authentication handshake.
    pub fn connect(cfg: &ConnConfig) -> Result<Conn, PgError> {
        match cfg.sslmode.as_str() {
            "" | "disable" | "allow" | "prefer" => {}
            other => {
                return Err(PgError::pq(format!(
                    "sslmode '{}' is not supported by this build (no TLS support), use 'disable'",
                    other
                )))
            }
        }
        let address = cfg.address();
        let stream = if cfg.is_unix() {
            let s = UnixStream::connect(&address).map_err(|e| dial_error("unix", &address, &e))?;
            Stream::Unix(s)
        } else {
            let s = Self::dial_tcp(cfg, &address)?;
            let _ = s.set_nodelay(true);
            Stream::Tcp(s)
        };
        let mut conn = Conn {
            stream,
            rbuf: BytesMut::with_capacity(16 * 1024),
            readbuf: vec![0u8; 64 * 1024],
            saved: None,
            server_version: 0,
            timezone: String::new(),
            process_id: 0,
            secret_key: 0,
            txn_status: TxnStatus::Idle,
            broken: false,
            in_flight: false,
            address,
        };
        if let Some(t) = cfg.connect_timeout {
            conn.set_deadline(Some(t));
        }
        conn.startup(cfg)?;
        conn.set_deadline(None);
        Ok(conn)
    }

    fn dial_tcp(cfg: &ConnConfig, address: &str) -> Result<TcpStream, PgError> {
        let addrs: Vec<std::net::SocketAddr> = match address.to_socket_addrs() {
            Ok(a) => a.collect(),
            Err(_) => {
                return Err(PgError::Net(format!(
                    "dial tcp: lookup {}: no such host",
                    cfg.host
                )))
            }
        };
        if addrs.is_empty() {
            return Err(PgError::Net(format!(
                "dial tcp: lookup {}: no such host",
                cfg.host
            )));
        }
        let mut last_err = None;
        for addr in &addrs {
            let res = match cfg.connect_timeout {
                Some(t) => TcpStream::connect_timeout(addr, t),
                None => TcpStream::connect(addr),
            };
            match res {
                Ok(s) => return Ok(s),
                Err(e) => last_err = Some((addr.to_string(), e)),
            }
        }
        let (addr, e) = last_err.expect("at least one address");
        Err(dial_error("tcp", &addr, &e))
    }

    fn set_deadline(&mut self, d: Option<Duration>) {
        match &self.stream {
            Stream::Tcp(s) => {
                let _ = s.set_read_timeout(d);
                let _ = s.set_write_timeout(d);
            }
            Stream::Unix(s) => {
                let _ = s.set_read_timeout(d);
                let _ = s.set_write_timeout(d);
            }
        }
    }

    /// Write a complete frontend message buffer.
    fn send(&mut self, buf: &[u8]) -> Result<(), PgError> {
        if let Err(e) = self.stream.write_all(buf) {
            self.broken = true;
            // lib/pq returns ErrBadConn when nothing was written and the raw
            // net error otherwise; a write failure always means the connection
            // is unusable, so it is treated as a bad connection (retryable).
            let _ = e;
            return Err(PgError::BadConn);
        }
        Ok(())
    }

    /// Read the next raw backend message (no async-message handling).
    fn recv_raw(&mut self) -> Result<Message, PgError> {
        if let Some(m) = self.saved.take() {
            return Ok(m);
        }
        loop {
            match Message::parse(&mut self.rbuf) {
                Ok(Some(m)) => return Ok(m),
                Ok(None) => {}
                Err(e) => {
                    self.broken = true;
                    return Err(PgError::pq(e.to_string()));
                }
            }
            match self.stream.read(&mut self.readbuf) {
                Ok(0) => {
                    self.broken = true;
                    return Err(PgError::BadConn);
                }
                Ok(n) => self.rbuf.put_slice(&self.readbuf[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.broken = true;
                    return Err(PgError::BadConn);
                }
            }
        }
    }

    /// Read the next message, transparently consuming asynchronous
    /// `NoticeResponse`/`NotificationResponse`/`ParameterStatus` messages
    /// (lib/pq `recv1Buf`).
    pub fn recv(&mut self) -> Result<Message, PgError> {
        loop {
            let m = self.recv_raw()?;
            match m {
                Message::NoticeResponse(_) | Message::NotificationResponse(_) => continue,
                Message::ParameterStatus(body) => {
                    self.process_parameter_status(body.name().ok(), body.value().ok());
                    continue;
                }
                other => return Ok(other),
            }
        }
    }

    fn process_parameter_status(&mut self, name: Option<&str>, value: Option<&str>) {
        let (Some(name), Some(value)) = (name, value) else {
            return;
        };
        match name {
            "server_version" => {
                // lib/pq: fmt.Sscanf("%d.%d") → major*10000 + minor*100.
                let mut it = value.split(|c: char| !c.is_ascii_digit());
                let major = it.next().and_then(|v| v.parse::<i32>().ok());
                let minor = it.next().and_then(|v| v.parse::<i32>().ok());
                if let (Some(major), Some(minor)) = (major, minor) {
                    self.server_version = major * 10000 + minor * 100;
                }
            }
            "TimeZone" => self.timezone = value.to_string(),
            _ => {}
        }
    }

    fn startup(&mut self, cfg: &ConnConfig) -> Result<(), PgError> {
        let mut params: Vec<(&str, &str)> = vec![
            ("client_encoding", "UTF8"),
            ("extra_float_digits", "2"),
            ("datestyle", "ISO, MDY"),
            ("database", cfg.database.as_str()),
            ("user", cfg.user.as_str()),
        ];
        for (k, v) in &cfg.params {
            if !params.iter().any(|(pk, _)| pk == k) {
                params.push((k.as_str(), v.as_str()));
            }
        }
        let mut buf = BytesMut::new();
        frontend::startup_message(params, &mut buf).map_err(io_other)?;
        self.send(&buf)?;
        loop {
            match self.recv()? {
                Message::AuthenticationOk => {}
                Message::AuthenticationCleartextPassword => {
                    let mut buf = BytesMut::new();
                    frontend::password_message(cfg.password.as_bytes(), &mut buf)
                        .map_err(io_other)?;
                    self.send(&buf)?;
                }
                Message::AuthenticationMd5Password(body) => {
                    let hashed =
                        md5_hash(cfg.user.as_bytes(), cfg.password.as_bytes(), body.salt());
                    let mut buf = BytesMut::new();
                    frontend::password_message(hashed.as_bytes(), &mut buf).map_err(io_other)?;
                    self.send(&buf)?;
                }
                Message::AuthenticationSasl(body) => {
                    let mut has_scram = false;
                    let mut mechs = body.mechanisms();
                    while let Some(m) = mechs.next().map_err(io_other)? {
                        if m == "SCRAM-SHA-256" {
                            has_scram = true;
                        }
                    }
                    if !has_scram {
                        return Err(PgError::pq("SASL authentication: server offers no supported mechanism (need SCRAM-SHA-256)"
                                .to_string(),
                        ));
                    }
                    self.scram_auth(cfg)?;
                }
                Message::AuthenticationSaslContinue(_) | Message::AuthenticationSaslFinal(_) => {
                    return Err(PgError::pq("unexpected SASL response".to_string()))
                }
                Message::AuthenticationKerberosV5
                | Message::AuthenticationScmCredential
                | Message::AuthenticationGss
                | Message::AuthenticationSspi
                | Message::AuthenticationGssContinue(_) => {
                    return Err(PgError::pq(
                        "unknown authentication response: unsupported method".to_string(),
                    ))
                }
                Message::BackendKeyData(body) => {
                    self.process_id = body.process_id();
                    self.secret_key = body.secret_key();
                }
                Message::ReadyForQuery(body) => {
                    self.set_txn_status(body.status());
                    return Ok(());
                }
                Message::ErrorResponse(body) => {
                    // Startup errors are returned verbatim (lib/pq
                    // errRecoverNoErrBadConn): even FATAL ones are *pq.Error.
                    return Err(PgError::server_err(parse_error_fields(body.fields())?));
                }
                _ => return Err(PgError::pq("unknown response for startup".to_string())),
            }
        }
    }

    fn scram_auth(&mut self, cfg: &ConnConfig) -> Result<(), PgError> {
        let mut scram = ScramSha256::new(cfg.password.as_bytes(), ChannelBinding::unsupported());
        let mut buf = BytesMut::new();
        frontend::sasl_initial_response("SCRAM-SHA-256", scram.message(), &mut buf)
            .map_err(io_other)?;
        self.send(&buf)?;
        match self.recv()? {
            Message::AuthenticationSaslContinue(body) => {
                scram
                    .update(body.data())
                    .map_err(|e| PgError::pq(format!("SCRAM-SHA-256 error: {}", e)))?;
            }
            Message::ErrorResponse(body) => {
                return Err(PgError::server_err(parse_error_fields(body.fields())?))
            }
            _ => return Err(PgError::pq("unexpected SCRAM-SHA-256 response".to_string())),
        }
        let mut buf = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut buf).map_err(io_other)?;
        self.send(&buf)?;
        match self.recv()? {
            Message::AuthenticationSaslFinal(body) => scram
                .finish(body.data())
                .map_err(|e| PgError::pq(format!("SCRAM-SHA-256 error: {}", e))),
            Message::ErrorResponse(body) => {
                Err(PgError::server_err(parse_error_fields(body.fields())?))
            }
            _ => Err(PgError::pq("unexpected SCRAM-SHA-256 response".to_string())),
        }
    }

    pub(super) fn set_txn_status(&mut self, status: u8) {
        self.txn_status = match status {
            b'T' => TxnStatus::InTransaction,
            b'E' => TxnStatus::Failed,
            _ => TxnStatus::Idle,
        };
    }

    /// Consume messages up to and including `ReadyForQuery`, remembering the
    /// first error seen (used to resynchronise after an error).
    pub fn drain_to_ready(&mut self) -> Result<(), PgError> {
        loop {
            match self.recv()? {
                Message::ReadyForQuery(body) => {
                    self.set_txn_status(body.status());
                    return Ok(());
                }
                _ => continue,
            }
        }
    }

    /// Send a simple-protocol `Query` message.
    pub fn send_simple_query(&mut self, query: &str) -> Result<(), PgError> {
        let mut buf = BytesMut::new();
        frontend::query(query, &mut buf).map_err(io_other)?;
        self.send(&buf)
    }

    /// Extended protocol: parse + describe the unnamed statement, bind the
    /// parameters (text format), execute and sync. Returns the result column
    /// description (empty for statements returning no rows). After a
    /// successful return the next messages are the DataRows/CommandComplete
    /// of the execution; execution errors (constraint violations, ...) are
    /// already detected and returned here (lib/pq `postExecuteWorkaround`).
    pub fn send_extended_query(
        &mut self,
        query: &str,
        args: &[SqlArg],
    ) -> Result<Vec<Column>, PgError> {
        if args.len() >= 65536 {
            return Err(PgError::pq(format!(
                "got {} parameters but PostgreSQL only supports 65535 parameters",
                args.len()
            )));
        }
        // Round trip 1: Parse / Describe(statement) / Sync.
        let mut buf = BytesMut::new();
        frontend::parse("", query, std::iter::empty(), &mut buf).map_err(io_other)?;
        frontend::describe(b'S', "", &mut buf).map_err(io_other)?;
        frontend::sync(&mut buf);
        self.send(&buf)?;
        match self.recv()? {
            Message::ParseComplete => {}
            Message::ErrorResponse(body) => {
                let err = self.server_error(body.fields())?;
                self.drain_to_ready()?;
                return Err(err);
            }
            _ => {
                self.broken = true;
                return Err(PgError::pq("unexpected Parse response".to_string()));
            }
        }
        let mut param_oids: Vec<u32> = Vec::new();
        let mut columns: Vec<Column> = Vec::new();
        loop {
            match self.recv()? {
                Message::ParameterDescription(body) => {
                    let mut it = body.parameters();
                    while let Some(oid) = it.next().map_err(io_other)? {
                        param_oids.push(oid);
                    }
                }
                Message::NoData => columns.clear(),
                Message::RowDescription(body) => {
                    columns = parse_row_description(body.fields())?;
                }
                Message::ErrorResponse(body) => {
                    let err = self.server_error(body.fields())?;
                    self.drain_to_ready()?;
                    return Err(err);
                }
                Message::ReadyForQuery(body) => {
                    self.set_txn_status(body.status());
                    break;
                }
                _ => {
                    self.broken = true;
                    return Err(PgError::pq(
                        "unexpected Describe statement response".to_string(),
                    ));
                }
            }
        }
        if param_oids.len() != args.len() {
            return Err(PgError::pq(format!(
                "got {} parameters but the statement requires {}",
                args.len(),
                param_oids.len()
            )));
        }
        // Round trip 2: Bind / Execute / Sync.
        let mut buf = BytesMut::new();
        let values: Vec<(&SqlArg, u32)> = args.iter().zip(param_oids.iter().copied()).collect();
        frontend::bind(
            "",
            "",
            std::iter::empty::<i16>(),
            values,
            |(arg, oid), buf| match encode_arg(arg, oid == OID_BYTEA) {
                Some(bytes) => {
                    buf.put_slice(&bytes);
                    Ok(IsNull::No)
                }
                None => Ok(IsNull::Yes),
            },
            std::iter::empty::<i16>(),
            &mut buf,
        )
        .map_err(|e| match e {
            frontend::BindError::Conversion(e) => PgError::pq(e.to_string()),
            frontend::BindError::Serialization(e) => PgError::pq(e.to_string()),
        })?;
        frontend::execute("", 0, &mut buf).map_err(io_other)?;
        frontend::sync(&mut buf);
        self.send(&buf)?;
        match self.recv()? {
            Message::BindComplete => {}
            Message::ErrorResponse(body) => {
                let err = self.server_error(body.fields())?;
                self.drain_to_ready()?;
                return Err(err);
            }
            _ => {
                self.broken = true;
                return Err(PgError::pq("unexpected bind response".to_string()));
            }
        }
        // postExecuteWorkaround: peek at the first execution message so that
        // execution errors are reported by Query/Exec, not by Next.
        let first = self.recv()?;
        if let Message::ErrorResponse(body) = first {
            let err = self.server_error(body.fields())?;
            self.drain_to_ready()?;
            return Err(err);
        }
        self.saved = Some(first);
        Ok(columns)
    }

    /// Build the error for an `ErrorResponse`; FATAL severity marks the
    /// connection bad and becomes [`PgError::BadConn`] like lib/pq
    /// `errRecover` does for `Error.Fatal()`.
    pub fn server_error(&mut self, fields: ErrorFields<'_>) -> Result<PgError, PgError> {
        let e = parse_error_fields(fields)?;
        if e.severity == "FATAL" {
            self.broken = true;
            return Ok(PgError::BadConn);
        }
        Ok(PgError::server_err(e))
    }

    /// Send `Terminate` and drop the connection.
    pub fn terminate(mut self) {
        if !self.broken {
            let mut buf = BytesMut::new();
            frontend::terminate(&mut buf);
            let _ = self.send(&buf);
        }
    }
}

fn io_other(e: std::io::Error) -> PgError {
    PgError::pq(e.to_string())
}

/// Decode an `ErrorResponse`/`NoticeResponse` field list.
pub fn parse_error_fields(mut fields: ErrorFields<'_>) -> Result<ServerError, PgError> {
    let mut e = ServerError::default();
    while let Some(f) = fields.next().map_err(io_other)? {
        let v = String::from_utf8_lossy(f.value_bytes()).into_owned();
        match f.type_() {
            b'S' => e.severity = v,
            b'C' => e.code = v,
            b'M' => e.message = v,
            b'D' => e.detail = v,
            b'H' => e.hint = v,
            b'P' => e.position = v,
            b'p' => e.internal_position = v,
            b'q' => e.internal_query = v,
            b'W' => e.where_ = v,
            b's' => e.schema = v,
            b't' => e.table = v,
            b'c' => e.column = v,
            b'd' => e.data_type_name = v,
            b'n' => e.constraint = v,
            b'F' => e.file = v,
            b'L' => e.line = v,
            b'R' => e.routine = v,
            _ => {}
        }
    }
    Ok(e)
}

/// Decode a `RowDescription`.
pub fn parse_row_description(
    mut fields: postgres_protocol::message::backend::Fields<'_>,
) -> Result<Vec<Column>, PgError> {
    let mut cols = Vec::new();
    while let Some(f) = fields.next().map_err(io_other)? {
        cols.push(Column {
            name: f.name().to_string(),
            type_oid: f.type_oid(),
            format: f.format(),
        });
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_tags() {
        assert_eq!(parse_command_tag("INSERT 0 1").unwrap(), 1);
        assert_eq!(parse_command_tag("INSERT 0 0").unwrap(), 0);
        assert_eq!(parse_command_tag("UPDATE 12").unwrap(), 12);
        assert_eq!(parse_command_tag("DELETE 3").unwrap(), 3);
        assert_eq!(parse_command_tag("SELECT 7").unwrap(), 7);
        assert_eq!(parse_command_tag("COPY 100").unwrap(), 100);
        assert_eq!(parse_command_tag("FETCH 2").unwrap(), 2);
        assert_eq!(parse_command_tag("MOVE 2").unwrap(), 2);
        assert_eq!(parse_command_tag("CREATE TABLE").unwrap(), 0);
        assert_eq!(parse_command_tag("BEGIN").unwrap(), 0);
        assert_eq!(parse_command_tag("ALTER TABLE").unwrap(), 0);
        assert_eq!(parse_command_tag("SELECT").unwrap(), 0);
        assert_eq!(
            parse_command_tag("INSERT 1").unwrap_err().to_string(),
            "pq: unexpected INSERT command tag INSERT 1"
        );
        assert_eq!(
            parse_command_tag("UPDATE x").unwrap_err().to_string(),
            "pq: could not parse commandTag: strconv.ParseInt: parsing \"x\": invalid syntax"
        );
    }

    #[test]
    fn addresses() {
        let ctx = Ctx {
            pg_host: "localhost".into(),
            pg_port: "5432".into(),
            pg_user: "gha_admin".into(),
            pg_pass: "password".into(),
            pg_ssl: "disable".into(),
            ..Ctx::default()
        };
        let cfg = ConnConfig::from_ctx(&ctx, "gha");
        assert!(!cfg.is_unix());
        assert_eq!(cfg.address(), "localhost:5432");
        assert_eq!(cfg.database, "gha");
        let mut cfg2 = cfg.clone();
        cfg2.host = "/tmp".into();
        assert!(cfg2.is_unix());
        assert_eq!(cfg2.address(), "/tmp/.s.PGSQL.5432");
        cfg2.host = "/var/run/postgresql/".into();
        assert_eq!(cfg2.address(), "/var/run/postgresql/.s.PGSQL.5432");
        assert_eq!(join_host_port("::1", "5432"), "[::1]:5432");
    }

    #[test]
    fn connection_refused_error_looks_like_go() {
        // Port 1 on localhost is (practically) never listening.
        let ctx = Ctx {
            pg_host: "127.0.0.1".into(),
            pg_port: "1".into(),
            pg_ssl: "disable".into(),
            ..Ctx::default()
        };
        let cfg = ConnConfig::from_ctx(&ctx, "x");
        let err = Conn::connect(&cfg).unwrap_err();
        assert_eq!(
            err.to_string(),
            "dial tcp 127.0.0.1:1: connect: connection refused"
        );
        assert_eq!(err.go_type_name(), "*net.OpError");
        let mut cfg2 = cfg.clone();
        cfg2.host = "/nonexistent-dir".into();
        let err = Conn::connect(&cfg2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "dial unix /nonexistent-dir/.s.PGSQL.1: connect: no such file or directory"
        );
        let mut cfg3 = cfg.clone();
        cfg3.sslmode = "require".into();
        assert!(Conn::connect(&cfg3)
            .unwrap_err()
            .to_string()
            .contains("sslmode 'require' is not supported"));
    }
}
