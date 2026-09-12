//! PostgreSQL layer — port of `pg_conn.go` (and the parts of Go
//! `database/sql` + `github.com/lib/pq` DevStats relies on).
//!
//! Layout:
//!
//! * [`wire`] — blocking pure-Rust protocol client (startup, authentication,
//!   simple and extended query protocol, error decoding),
//! * [`query`] — the per-connection query state machine,
//! * [`value`] — result decoding and `Scan` conversions with the
//!   `database/sql` semantics (and error texts),
//! * [`errcodes`] — SQLSTATE → condition name table (lib/pq `ErrorCode.Name`),
//! * this module — [`SqlArg`] (query parameters), [`PgError`], the
//!   connection pool [`PgConn`] (Go `*sql.DB`), [`Rows`]/[`Row`]/[`PgTx`]
//!   and the DevStats helper API (`query_sql*`, `exec_sql*`, `write_ts_points`,
//!   `database_exists`, ...).
//!
//! Behavioural notes (see also `rust/README.md`):
//!
//! * connections are made lazily on first use, like `sql.Open`,
//! * at most two idle connections are kept (Go `defaultMaxIdleConns`),
//! * statements without parameters use the simple protocol, statements with
//!   parameters the unnamed-prepared-statement extended protocol — the same
//!   split lib/pq makes, so server-side behaviour (e.g. multi-statement
//!   strings, type inference of `$n`) is identical,
//! * all results are requested in text format,
//! * TLS is not supported: `PG_SSL` must be `disable` (DevStats always uses
//!   `disable`).

pub mod api;
pub mod errcodes;
pub mod query;
pub mod value;
pub mod wire;

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use chrono::{DateTime, FixedOffset, Local, Utc};

pub use api::*;
pub use query::QueryStart;
pub use value::{go_quote, Column, DriverValue, ScanDest};
pub use wire::{Conn, ConnConfig, ExecResult, TxnStatus};

use crate::context::Ctx;
use crate::gofmt;
use crate::string::format_raw_bytes;

/// Maximum number of idle connections kept per pool (`database/sql`
/// `defaultMaxIdleConns`).
pub const MAX_IDLE_CONNS: usize = 2;

/// `database/sql` `maxBadConnRetries`: attempts with a cached-or-new
/// connection before forcing a brand new one.
const MAX_BAD_CONN_RETRIES: usize = 2;

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// A query parameter — the Go `interface{}` values DevStats passes to
/// `database/sql` (`nil`, integers, floats, bools, strings, `[]byte`,
/// `time.Time`).
///
/// A time keeps its UTC offset: lib/pq sends `time.Time` values in their own
/// location (`2026-09-11 09:21:49+02:00` for a local `time.Now()` on a CEST
/// host), and a `timestamp` column then stores that wall-clock time. `From`
/// is implemented for `DateTime<Utc>`, `DateTime<Local>` and
/// `DateTime<FixedOffset>`.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum SqlArg {
    #[default]
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    Bytes(Vec<u8>),
    Time(DateTime<FixedOffset>),
    /// A time read back from the database and passed on as a parameter
    /// (`merge_dbs` copying rows). Sent like [`Time`](Self::Time); differs
    /// only in its Go `%v` rendering: lib/pq decodes `timestamp` values into
    /// a nameless `time.FixedZone`, printed `+0000 +0000` rather than the
    /// `+0000 UTC` of a `time.Now().UTC()` / `time.Parse` value.
    DbTime(DateTime<FixedOffset>),
}

impl SqlArg {
    /// Go `queryOut` rendering of one argument (`%+v`, `FormatRawBytes` for
    /// byte slices, `(null)` for nil).
    pub fn go_arg_string(&self) -> String {
        match self {
            SqlArg::Null => "(null)".to_string(),
            SqlArg::Int(i) => i.to_string(),
            SqlArg::Float(f) => gofmt::float(*f),
            SqlArg::Bool(b) => b.to_string(),
            SqlArg::Str(s) => s.clone(),
            SqlArg::Bytes(b) => format_raw_bytes(b),
            SqlArg::Time(t) => gofmt::time(*t),
            SqlArg::DbTime(t) => value::go_time_string(t),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, SqlArg::Null)
    }
}

macro_rules! sqlarg_from_int {
    ($($t:ty),*) => {
        $(impl From<$t> for SqlArg {
            fn from(v: $t) -> Self {
                SqlArg::Int(v as i64)
            }
        })*
    };
}
sqlarg_from_int!(i8, i16, i32, i64, u8, u16, u32, usize, isize);

impl From<f64> for SqlArg {
    fn from(v: f64) -> Self {
        SqlArg::Float(v)
    }
}
impl From<f32> for SqlArg {
    fn from(v: f32) -> Self {
        SqlArg::Float(v as f64)
    }
}
impl From<bool> for SqlArg {
    fn from(v: bool) -> Self {
        SqlArg::Bool(v)
    }
}
impl From<&str> for SqlArg {
    fn from(v: &str) -> Self {
        SqlArg::Str(v.to_string())
    }
}
impl From<String> for SqlArg {
    fn from(v: String) -> Self {
        SqlArg::Str(v)
    }
}
impl From<&String> for SqlArg {
    fn from(v: &String) -> Self {
        SqlArg::Str(v.clone())
    }
}
impl From<Vec<u8>> for SqlArg {
    fn from(v: Vec<u8>) -> Self {
        SqlArg::Bytes(v)
    }
}
impl From<&[u8]> for SqlArg {
    fn from(v: &[u8]) -> Self {
        SqlArg::Bytes(v.to_vec())
    }
}
impl From<DateTime<Utc>> for SqlArg {
    fn from(v: DateTime<Utc>) -> Self {
        SqlArg::Time(v.fixed_offset())
    }
}
impl From<DateTime<Local>> for SqlArg {
    fn from(v: DateTime<Local>) -> Self {
        SqlArg::Time(v.fixed_offset())
    }
}
impl From<DateTime<FixedOffset>> for SqlArg {
    fn from(v: DateTime<FixedOffset>) -> Self {
        SqlArg::Time(v)
    }
}
/// A value read from the database passed on unchanged as a parameter — what
/// `database/sql` does with a scanned `interface{}` (times become
/// [`SqlArg::DbTime`]).
impl From<&DriverValue> for SqlArg {
    fn from(v: &DriverValue) -> Self {
        match v {
            DriverValue::Null => SqlArg::Null,
            DriverValue::Int(i) => SqlArg::Int(*i),
            DriverValue::Float(f) => SqlArg::Float(*f),
            DriverValue::Bool(b) => SqlArg::Bool(*b),
            DriverValue::Str(s) => SqlArg::Str(s.clone()),
            DriverValue::Bytes(b) => SqlArg::Bytes(b.clone()),
            DriverValue::Time(t) => SqlArg::DbTime(*t),
        }
    }
}
impl From<DriverValue> for SqlArg {
    fn from(v: DriverValue) -> Self {
        SqlArg::from(&v)
    }
}
impl<T: Into<SqlArg>> From<Option<T>> for SqlArg {
    fn from(v: Option<T>) -> Self {
        match v {
            None => SqlArg::Null,
            Some(v) => v.into(),
        }
    }
}
impl From<&SqlArg> for SqlArg {
    fn from(v: &SqlArg) -> Self {
        v.clone()
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// An `ErrorResponse` from the server (lib/pq `*pq.Error`). Protocol-level
/// errors lib/pq raises itself (`errorf`) are also of this kind, with an
/// empty `code`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerError {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: String,
    pub hint: String,
    pub position: String,
    pub internal_position: String,
    pub internal_query: String,
    pub where_: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    pub data_type_name: String,
    pub constraint: String,
    pub file: String,
    pub line: String,
    pub routine: String,
}

impl ServerError {
    /// Condition name of the SQLSTATE code (lib/pq `ErrorCode.Name()`),
    /// `""` for unknown/empty codes.
    pub fn name(&self) -> &'static str {
        errcodes::error_code_name(&self.code)
    }

    /// lib/pq `Error.Fatal()`.
    pub fn is_fatal(&self) -> bool {
        self.severity == "FATAL"
    }
}

/// Errors of the PostgreSQL layer.
#[derive(Debug, Clone, PartialEq)]
pub enum PgError {
    /// Go `driver.ErrBadConn` — the connection is unusable; retried by the
    /// pool when it happens at statement start.
    BadConn,
    /// Error reported by the server (or a lib/pq-style protocol error).
    /// Boxed: keeps `Result<_, PgError>` small (clippy `result_large_err`).
    Server(Box<ServerError>),
    /// Network error while dialing (Go `*net.OpError`).
    Net(String),
    /// Go `sql.ErrNoRows`.
    NoRows,
    /// `database/sql` Scan conversion error.
    Scan(String),
    /// Any other error (`database/sql` state errors, ...).
    Other(String),
}

impl PgError {
    /// Wraps a server error (boxing it).
    pub fn server_err(e: ServerError) -> PgError {
        PgError::Server(Box::new(e))
    }

    /// A lib/pq `errorf` error: `pq: <message>` of type `*pq.Error`.
    pub fn pq<S: Into<String>>(message: S) -> PgError {
        PgError::server_err(ServerError {
            message: message.into(),
            ..ServerError::default()
        })
    }

    /// Go `%T` of the corresponding Go error value.
    pub fn go_type_name(&self) -> &'static str {
        match self {
            PgError::Server(_) => "*pq.Error",
            PgError::Net(_) => "*net.OpError",
            PgError::BadConn | PgError::NoRows | PgError::Scan(_) | PgError::Other(_) => {
                "*errors.errorString"
            }
        }
    }

    pub fn is_bad_conn(&self) -> bool {
        matches!(self, PgError::BadConn)
    }

    pub fn is_no_rows(&self) -> bool {
        matches!(self, PgError::NoRows)
    }

    /// The server error, if this is one.
    pub fn server(&self) -> Option<&ServerError> {
        match self {
            PgError::Server(e) => Some(e),
            _ => None,
        }
    }

    /// SQLSTATE code (`""` when not a server error).
    pub fn code(&self) -> &str {
        self.server().map(|e| e.code.as_str()).unwrap_or("")
    }

    /// Condition name (`""` when not a server error or unknown code).
    pub fn name(&self) -> &'static str {
        self.server().map(|e| e.name()).unwrap_or("")
    }
}

impl fmt::Display for PgError {
    /// Go `err.Error()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PgError::BadConn => f.write_str("driver: bad connection"),
            PgError::Server(e) => write!(f, "pq: {}", e.message),
            PgError::Net(s) | PgError::Scan(s) | PgError::Other(s) => f.write_str(s),
            PgError::NoRows => f.write_str("sql: no rows in result set"),
        }
    }
}

impl std::error::Error for PgError {}

fn tx_done_error() -> PgError {
    PgError::Other("sql: transaction has already been committed or rolled back".to_string())
}

// ---------------------------------------------------------------------------
// Connection pool (Go *sql.DB)
// ---------------------------------------------------------------------------

/// A lazily connecting pool of connections to one database — the Rust
/// `*sql.DB`. Cheap to create; the first statement dials.
pub struct PgConn {
    cfg: ConnConfig,
    /// The lib/pq style connection string (as printed by `PgConnectString:`).
    pub connection_string: String,
    idle: Mutex<Vec<Conn>>,
    closed: AtomicBool,
    opened: AtomicU64,
}

impl fmt::Debug for PgConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgConn")
            .field("address", &self.cfg.address())
            .field("database", &self.cfg.database)
            .field("user", &self.cfg.user)
            .field("idle", &self.idle.lock().map(|v| v.len()).unwrap_or(0))
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .finish()
    }
}

/// Where a [`Rows`] gets its connection from.
// `Pooled` is much larger than `Borrowed`: fine, `Rows` are short-lived
// handles, one per statement.
#[allow(clippy::large_enum_variant)]
enum ConnRef<'a> {
    /// A pool connection, returned to the pool when the rows are closed.
    Pooled {
        conn: Option<Conn>,
        pool: &'a PgConn,
    },
    /// The connection of a transaction.
    Borrowed(&'a mut Conn),
}

impl ConnRef<'_> {
    fn conn(&mut self) -> Option<&mut Conn> {
        match self {
            ConnRef::Pooled { conn, .. } => conn.as_mut(),
            ConnRef::Borrowed(c) => Some(c),
        }
    }
}

impl PgConn {
    /// Create a pool for `cfg` (no I/O happens here).
    pub fn new(cfg: ConnConfig, connection_string: String) -> PgConn {
        PgConn {
            cfg,
            connection_string,
            idle: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            opened: AtomicU64::new(0),
        }
    }

    /// Pool configuration.
    pub fn config(&self) -> &ConnConfig {
        &self.cfg
    }

    /// Number of physical connections opened so far.
    pub fn connections_opened(&self) -> u64 {
        self.opened.load(Ordering::SeqCst)
    }

    /// Number of idle connections currently kept.
    pub fn idle_connections(&self) -> usize {
        self.idle.lock().map(|v| v.len()).unwrap_or(0)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Take an idle connection or dial a new one. Returns `(conn, cached)`.
    fn checkout(&self) -> Result<(Conn, bool), PgError> {
        if self.is_closed() {
            return Err(PgError::Other("sql: database is closed".to_string()));
        }
        let cached = self.idle.lock().ok().and_then(|mut v| v.pop());
        if let Some(c) = cached {
            return Ok((c, true));
        }
        let c = Conn::connect(&self.cfg)?;
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok((c, false))
    }

    /// Return a connection to the pool (or drop it when it is unusable, in
    /// a transaction, or the pool already holds [`MAX_IDLE_CONNS`]).
    fn checkin(&self, mut conn: Conn) {
        if conn.in_flight && conn.finish().is_err() {
            return;
        }
        if conn.broken || conn.txn_status != TxnStatus::Idle || self.is_closed() {
            conn.terminate();
            return;
        }
        let mut idle = match self.idle.lock() {
            Ok(g) => g,
            Err(_) => {
                conn.terminate();
                return;
            }
        };
        if idle.len() >= MAX_IDLE_CONNS {
            drop(idle);
            conn.terminate();
            return;
        }
        idle.push(conn);
    }

    /// Run `f` on a connection with the `database/sql` bad-connection retry
    /// policy: a `BadConn` at statement start is retried on another
    /// connection (twice cached-or-new, then once more), everything else is
    /// returned as is.
    fn with_conn<T>(
        &self,
        mut f: impl FnMut(&mut Conn) -> Result<T, PgError>,
    ) -> Result<(Conn, T), PgError> {
        let mut attempt = 0;
        loop {
            let (mut conn, _cached) = self.checkout()?;
            match f(&mut conn) {
                Ok(v) => return Ok((conn, v)),
                Err(PgError::BadConn) if attempt < MAX_BAD_CONN_RETRIES => {
                    conn.terminate();
                    attempt += 1;
                }
                Err(e) => {
                    self.checkin(conn);
                    return Err(e);
                }
            }
        }
    }

    /// Go `db.Query`: start a statement and stream its rows.
    pub fn query(&self, query: &str, args: &[SqlArg]) -> Result<Rows<'_>, PgError> {
        let (conn, start) = self.with_conn(|c| c.start_query(query, args))?;
        Ok(Rows::new(
            ConnRef::Pooled {
                conn: Some(conn),
                pool: self,
            },
            start,
        ))
    }

    /// Go `db.QueryRow`.
    pub fn query_row(&self, query: &str, args: &[SqlArg]) -> Row<'_> {
        Row {
            rows: self.query(query, args),
        }
    }

    /// Go `db.Exec`.
    pub fn exec(&self, query: &str, args: &[SqlArg]) -> Result<ExecResult, PgError> {
        let (conn, res) = self.with_conn(|c| c.exec(query, args))?;
        self.checkin(conn);
        Ok(res)
    }

    /// Go `db.Begin`.
    pub fn begin(&self) -> Result<PgTx<'_>, PgError> {
        let (conn, ()) = self.with_conn(|c| {
            let tag = c.simple_exec_tag("BEGIN")?;
            if tag != "BEGIN" {
                c.broken = true;
                return Err(PgError::Other(format!("unexpected command tag {}", tag)));
            }
            if c.txn_status != TxnStatus::InTransaction {
                c.broken = true;
                return Err(PgError::Other(format!(
                    "unexpected transaction status {}",
                    c.txn_status.go_string()
                )));
            }
            Ok(())
        })?;
        Ok(PgTx {
            pool: self,
            conn: Some(conn),
            done: false,
        })
    }

    /// Go `db.Ping`: make sure a connection can be established.
    pub fn ping(&self) -> Result<(), PgError> {
        let (conn, ()) = self.with_conn(|_| Ok(()))?;
        self.checkin(conn);
        Ok(())
    }

    /// Go `db.Close`: drop the idle connections and refuse further use.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.drop_idle();
    }

    /// Drop all idle connections; the next statement dials again. This is
    /// what the "Reconnect" handling of the `*WithErr` helpers does.
    pub fn reset(&self) {
        self.drop_idle();
    }

    fn drop_idle(&self) {
        let conns: Vec<Conn> = match self.idle.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(_) => Vec::new(),
        };
        for c in conns {
            c.terminate();
        }
    }
}

impl Drop for PgConn {
    fn drop(&mut self) {
        self.drop_idle();
    }
}

// ---------------------------------------------------------------------------
// Rows / Row (Go *sql.Rows / *sql.Row)
// ---------------------------------------------------------------------------

/// A streamed result set (Go `*sql.Rows`). Dropping it drains the remaining
/// rows and returns the connection to its pool.
pub struct Rows<'a> {
    conn: ConnRef<'a>,
    columns: Vec<Column>,
    current: Vec<DriverValue>,
    has_current: bool,
    done: bool,
    closed: bool,
    lasterr: Option<PgError>,
    result: Option<ExecResult>,
}

impl fmt::Debug for Rows<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rows")
            .field("columns", &self.columns)
            .field("done", &self.done)
            .field("closed", &self.closed)
            .field("lasterr", &self.lasterr)
            .finish()
    }
}

impl<'a> Rows<'a> {
    fn new(conn: ConnRef<'a>, start: QueryStart) -> Rows<'a> {
        Rows {
            conn,
            columns: start.columns,
            current: Vec::new(),
            has_current: false,
            done: start.done,
            closed: false,
            lasterr: None,
            result: start.result,
        }
    }

    /// Result column descriptions.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Go `rows.Columns()`.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    /// Go `rows.Next()`: advance to the next row; `false` at the end or on
    /// error (see [`err`](Self::err)). The rows are closed automatically
    /// when this returns `false`.
    // Named after Go's `rows.Next()` on purpose (it is not an iterator: it
    // returns `bool` and the row is read with `scan`/`values`).
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        if self.closed || self.done {
            self.has_current = false;
            return false;
        }
        let Some(conn) = self.conn.conn() else {
            self.has_current = false;
            return false;
        };
        match conn.next_row(&self.columns, &mut self.result) {
            Ok(Some(values)) => {
                self.current = values;
                self.has_current = true;
                true
            }
            Ok(None) => {
                self.has_current = false;
                self.done = true;
                self.close_internal();
                false
            }
            Err(e) => {
                self.has_current = false;
                self.done = true;
                self.lasterr = Some(e);
                self.close_internal();
                false
            }
        }
    }

    /// The driver values of the current row.
    pub fn values(&self) -> &[DriverValue] {
        &self.current
    }

    /// Go `rows.Scan(dest...)`: convert the current row into `dest`.
    pub fn scan(&self, dest: &mut [&mut dyn ScanDest]) -> Result<(), PgError> {
        if self.closed {
            return Err(PgError::Other("sql: Rows are closed".to_string()));
        }
        if !self.has_current {
            return Err(PgError::Other(
                "sql: Scan called without calling Next".to_string(),
            ));
        }
        if dest.len() != self.current.len() {
            return Err(PgError::Other(format!(
                "sql: expected {} destination arguments in Scan, not {}",
                self.current.len(),
                dest.len()
            )));
        }
        for (i, (d, v)) in dest.iter_mut().zip(self.current.iter()).enumerate() {
            d.scan_from(v).map_err(|e| {
                PgError::Scan(format!(
                    "sql: Scan error on column index {}, name {}: {}",
                    i,
                    go_quote(&self.columns[i].name),
                    e
                ))
            })?;
        }
        Ok(())
    }

    /// Go `rows.Err()`: the error that stopped iteration, if any.
    pub fn err(&self) -> Result<(), PgError> {
        match &self.lasterr {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Command result (rows affected) once the statement completed.
    pub fn result(&self) -> Option<ExecResult> {
        self.result
    }

    fn close_internal(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.has_current = false;
        let mut drain_err = None;
        if let Some(conn) = self.conn.conn() {
            if let Err(e) = conn.finish_with_result(&mut self.result) {
                drain_err = Some(e);
            }
        }
        if let (Some(e), None) = (drain_err, &self.lasterr) {
            self.lasterr = Some(e);
        }
        if let ConnRef::Pooled { conn, pool } = &mut self.conn {
            if let Some(c) = conn.take() {
                pool.checkin(c);
            }
        }
    }

    /// Go `rows.Close()`: drain and release the connection; returns the
    /// error hit while draining, if any.
    pub fn close(mut self) -> Result<(), PgError> {
        let before = self.lasterr.clone();
        self.close_internal();
        match (&before, &self.lasterr) {
            (None, Some(e)) => Err(e.clone()),
            _ => Ok(()),
        }
    }
}

impl Drop for Rows<'_> {
    fn drop(&mut self) {
        self.close_internal();
    }
}

/// A single-row query (Go `*sql.Row`): the statement is already executed,
/// [`scan`](Self::scan) delivers the first row.
pub struct Row<'a> {
    rows: Result<Rows<'a>, PgError>,
}

impl Row<'_> {
    /// Go `row.Scan(dest...)`: [`PgError::NoRows`] when the query returned
    /// nothing, otherwise the first row converted into `dest`.
    pub fn scan(self, dest: &mut [&mut dyn ScanDest]) -> Result<(), PgError> {
        let mut rows = self.rows?;
        if !rows.next() {
            rows.err()?;
            return Err(PgError::NoRows);
        }
        rows.scan(dest)?;
        rows.close()
    }

    /// The first row as driver values (`None` when there is no row).
    pub fn values(self) -> Result<Option<Vec<DriverValue>>, PgError> {
        let mut rows = self.rows?;
        if !rows.next() {
            rows.err()?;
            return Ok(None);
        }
        let v = rows.values().to_vec();
        rows.close()?;
        Ok(Some(v))
    }
}

/// `pg_scan!(row_or_rows, a, b, c)` → `row_or_rows.scan(&mut [&mut a, &mut b, &mut c])`.
#[macro_export]
macro_rules! pg_scan {
    ($row:expr, $($dest:expr),+ $(,)?) => {
        $row.scan(&mut [$(&mut $dest as &mut dyn $crate::pg::ScanDest),+])
    };
}

// ---------------------------------------------------------------------------
// Transactions (Go *sql.Tx)
// ---------------------------------------------------------------------------

/// A transaction on a dedicated connection (Go `*sql.Tx`). Dropping an
/// unfinished transaction rolls it back.
pub struct PgTx<'a> {
    pool: &'a PgConn,
    conn: Option<Conn>,
    done: bool,
}

impl fmt::Debug for PgTx<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgTx")
            .field("done", &self.done)
            .field("conn", &self.conn)
            .finish()
    }
}

impl PgTx<'_> {
    fn conn_mut(&mut self) -> Result<&mut Conn, PgError> {
        if self.done {
            return Err(tx_done_error());
        }
        self.conn.as_mut().ok_or_else(tx_done_error)
    }

    /// Go `tx.Query`.
    pub fn query(&mut self, query: &str, args: &[SqlArg]) -> Result<Rows<'_>, PgError> {
        let conn = self.conn_mut()?;
        let start = conn.start_query(query, args)?;
        Ok(Rows::new(ConnRef::Borrowed(conn), start))
    }

    /// First half of [`query`](Self::query): send the statement and read
    /// its description (the borrow of the transaction ends with the call).
    pub fn start_query(&mut self, query: &str, args: &[SqlArg]) -> Result<QueryStart, PgError> {
        self.conn_mut()?.start_query(query, args)
    }

    /// Second half of [`query`](Self::query): the rows of a statement
    /// started with [`start_query`](Self::start_query).
    pub fn rows_after_start(&mut self, start: QueryStart) -> Rows<'_> {
        let conn = self
            .conn
            .as_mut()
            .expect("rows_after_start: transaction has no connection");
        Rows::new(ConnRef::Borrowed(conn), start)
    }

    /// Go `tx.QueryRow`.
    pub fn query_row(&mut self, query: &str, args: &[SqlArg]) -> Row<'_> {
        Row {
            rows: self.query(query, args),
        }
    }

    /// Go `tx.Exec`.
    pub fn exec(&mut self, query: &str, args: &[SqlArg]) -> Result<ExecResult, PgError> {
        self.conn_mut()?.exec(query, args)
    }

    fn end(&mut self, statement: &str) -> Result<(), PgError> {
        if self.done {
            return Err(tx_done_error());
        }
        self.done = true;
        let Some(mut conn) = self.conn.take() else {
            return Err(tx_done_error());
        };
        if conn.broken {
            return Err(PgError::BadConn);
        }
        let res = (|| {
            if statement == "COMMIT" && conn.txn_status == TxnStatus::Failed {
                // lib/pq: abort instead, and tell the caller.
                let tag = conn.simple_exec_tag("ROLLBACK")?;
                if tag != "ROLLBACK" {
                    return Err(PgError::Other(format!("unexpected command tag {}", tag)));
                }
                return Err(PgError::pq(
                    "Could not complete operation in a failed transaction",
                ));
            }
            let tag = conn.simple_exec_tag(statement)?;
            if tag != statement {
                conn.broken = true;
                return Err(PgError::Other(format!("unexpected command tag {}", tag)));
            }
            Ok(())
        })();
        if res.is_err() && conn.txn_status != TxnStatus::Idle {
            conn.broken = true;
        }
        self.pool.checkin(conn);
        res
    }

    /// Go `tx.Commit`. A transaction in the failed state is rolled back and
    /// `pq: Could not complete operation in a failed transaction` returned.
    pub fn commit(mut self) -> Result<(), PgError> {
        self.end("COMMIT")
    }

    /// Go `tx.Rollback`.
    pub fn rollback(mut self) -> Result<(), PgError> {
        self.end("ROLLBACK")
    }
}

impl Drop for PgTx<'_> {
    fn drop(&mut self) {
        if !self.done && self.conn.is_some() {
            let _ = self.end("ROLLBACK");
        }
    }
}

impl TxnStatus {
    /// lib/pq `%v` of the transaction status.
    pub fn go_string(&self) -> &'static str {
        match self {
            TxnStatus::Idle => "idle",
            TxnStatus::InTransaction => "idle in transaction",
            TxnStatus::Failed => "in a failed transaction",
        }
    }
}

// ---------------------------------------------------------------------------
// Connecting (Go PgConn / PgConnErr / PgConnDB)
// ---------------------------------------------------------------------------

/// The lib/pq connection string DevStats builds (printed with `GHA2DB_QOUT`).
pub fn pg_connection_string(ctx: &Ctx, db_name: &str) -> String {
    format!(
        "client_encoding=UTF8 sslmode='{}' host='{}' port={} dbname='{}' user='{}' password='{}'",
        ctx.pg_ssl, ctx.pg_host, ctx.pg_port, db_name, ctx.pg_user, ctx.pg_pass
    )
}

fn new_pool(ctx: &Ctx, db_name: &str) -> PgConn {
    PgConn::new(
        ConnConfig::from_ctx(ctx, db_name),
        pg_connection_string(ctx, db_name),
    )
}

/// Go `PgConnErr`: pool for `ctx.pg_db`. Like `sql.Open` this never fails —
/// connection problems surface on the first statement.
pub fn pg_conn_err(ctx: &Ctx) -> Result<PgConn, PgError> {
    if ctx.q_out {
        println!("PgConnectString: {}", pg_connection_string(ctx, &ctx.pg_db));
    }
    Ok(new_pool(ctx, &ctx.pg_db))
}

/// Go `PgConn`: pool for `ctx.pg_db`.
pub fn pg_conn(ctx: &Ctx) -> PgConn {
    if ctx.q_out {
        // Plain println (not the DB logger) — nothing may be logged to the
        // database while connecting.
        println!("PgConnectString: {}", pg_connection_string(ctx, &ctx.pg_db));
    }
    new_pool(ctx, &ctx.pg_db)
}

/// Go `PgConnDB`: pool for the given database instead of `ctx.pg_db`.
/// Disables the reconnect handling (`ctx.can_reconnect = false`) because the
/// helpers could only reconnect to `ctx.pg_db`.
pub fn pg_conn_db(ctx: &mut Ctx, db_name: &str) -> PgConn {
    ctx.can_reconnect = false;
    pg_conn_db_shared(ctx, db_name)
}

/// [`pg_conn_db`] for callers sharing an immutable `Ctx` between threads
/// (Go's `PgConnDB` called from several goroutines): the caller must clear
/// `ctx.can_reconnect` itself before spawning the threads.
pub fn pg_conn_db_shared(ctx: &Ctx, db_name: &str) -> PgConn {
    if ctx.q_out {
        println!("ConnectString: {}", pg_connection_string(ctx, db_name));
    }
    new_pool(ctx, db_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn sql_arg_conversions() {
        assert_eq!(SqlArg::from(5i32), SqlArg::Int(5));
        assert_eq!(SqlArg::from(5usize), SqlArg::Int(5));
        assert_eq!(SqlArg::from(2.5f64), SqlArg::Float(2.5));
        assert_eq!(SqlArg::from(true), SqlArg::Bool(true));
        assert_eq!(SqlArg::from("x"), SqlArg::Str("x".into()));
        assert_eq!(SqlArg::from(String::from("y")), SqlArg::Str("y".into()));
        assert_eq!(SqlArg::from(vec![1u8, 2]), SqlArg::Bytes(vec![1, 2]));
        assert_eq!(SqlArg::from(None::<i64>), SqlArg::Null);
        assert_eq!(SqlArg::from(Some(3i64)), SqlArg::Int(3));
        assert_eq!(SqlArg::from(Some("s")), SqlArg::Str("s".into()));
        let t = Utc.with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(SqlArg::from(t), SqlArg::Time(t.fixed_offset()));
        let local = Local.with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(SqlArg::from(local), SqlArg::Time(local.fixed_offset()));
    }

    #[test]
    fn sql_arg_go_strings() {
        assert_eq!(SqlArg::Null.go_arg_string(), "(null)");
        assert_eq!(SqlArg::Int(-3).go_arg_string(), "-3");
        assert_eq!(SqlArg::Float(1.5).go_arg_string(), "1.5");
        assert_eq!(SqlArg::Float(1e21).go_arg_string(), "1e+21");
        assert_eq!(SqlArg::Bool(false).go_arg_string(), "false");
        assert_eq!(SqlArg::Str("abc".into()).go_arg_string(), "abc");
        let t = Utc.with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(
            SqlArg::from(t).go_arg_string(),
            "2020-01-02 03:04:05 +0000 UTC"
        );
    }

    #[test]
    fn error_display_and_types() {
        assert_eq!(PgError::BadConn.to_string(), "driver: bad connection");
        assert_eq!(PgError::NoRows.to_string(), "sql: no rows in result set");
        let e = PgError::server_err(ServerError {
            severity: "ERROR".into(),
            code: "42601".into(),
            message: "syntax error at or near \"selec\"".into(),
            ..ServerError::default()
        });
        assert_eq!(e.to_string(), "pq: syntax error at or near \"selec\"");
        assert_eq!(e.go_type_name(), "*pq.Error");
        assert_eq!(e.name(), "syntax_error");
        assert_eq!(e.code(), "42601");
        let p = PgError::pq("unexpected bind response");
        assert_eq!(p.to_string(), "pq: unexpected bind response");
        assert_eq!(p.name(), "");
        assert_eq!(
            PgError::Net("dial tcp 1.2.3.4:1: connect: connection refused".into()).go_type_name(),
            "*net.OpError"
        );
        assert_eq!(
            PgError::Other("x".into()).go_type_name(),
            "*errors.errorString"
        );
    }

    #[test]
    fn connection_string_matches_go() {
        let ctx = Ctx {
            pg_host: "localhost".into(),
            pg_port: "5432".into(),
            pg_db: "gha".into(),
            pg_user: "gha_admin".into(),
            pg_pass: "password".into(),
            pg_ssl: "disable".into(),
            ..Ctx::default()
        };
        assert_eq!(
            pg_connection_string(&ctx, &ctx.pg_db),
            "client_encoding=UTF8 sslmode='disable' host='localhost' port=5432 dbname='gha' user='gha_admin' password='password'"
        );
        let con = pg_conn(&ctx);
        assert_eq!(con.connection_string, pg_connection_string(&ctx, "gha"));
        assert_eq!(con.connections_opened(), 0);
        assert!(!con.is_closed());
        con.close();
        assert!(con.is_closed());
        assert_eq!(
            con.exec("select 1", &[]).unwrap_err().to_string(),
            "sql: database is closed"
        );
    }

    #[test]
    fn pg_conn_db_disables_reconnect() {
        let mut ctx = Ctx {
            can_reconnect: true,
            ..Ctx::default()
        };
        let con = pg_conn_db(&mut ctx, "other");
        assert!(!ctx.can_reconnect);
        assert_eq!(con.config().database, "other");
    }
}
