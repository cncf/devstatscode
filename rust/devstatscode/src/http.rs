//! Minimal HTTP/1.x server reproducing the observable behaviour of Go's
//! `net/http` default server and `ServeMux` (as used by `webhook` and `api`):
//! one thread per connection, keep-alive, `Content-Length` and chunked
//! request bodies, `Expect: 100-continue`, Go's error responses (`400 Bad
//! Request`, `431 Request Header Fields Too Large`, missing `Host`), Go's
//! response header layout (`Content-Type`, `Date`, `Content-Length`,
//! `Connection: close`), the mux's `404 page not found`, clean-path and
//! trailing-slash redirects, and Go's `listen tcp …` error wording.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::error::go_io_error_string;
use crate::gourl;

/// Go `http.DefaultMaxHeaderBytes`.
pub const DEFAULT_MAX_HEADER_BYTES: usize = 1 << 20;

/// Largest form body `Request::form_value` parses (Go: 10 MB).
const MAX_FORM_SIZE: usize = 10 << 20;

/// A parsed request (Go `*http.Request` subset).
#[derive(Debug, Clone, Default)]
pub struct Request {
    /// `GET`, `POST`, …
    pub method: String,
    /// The request target exactly as sent.
    pub request_uri: String,
    /// Unescaped path (`r.URL.Path`).
    pub path: String,
    /// Query string without `?` (`r.URL.RawQuery`).
    pub raw_query: String,
    /// `HTTP/1.0` or `HTTP/1.1`.
    pub proto: String,
    /// `Host` header (or the authority of an absolute request target).
    pub host: String,
    /// Headers in wire order (names as sent).
    pub headers: Vec<(String, String)>,
    /// The complete request body (what could be read of it).
    pub body: Vec<u8>,
    /// The error Go's `io.ReadAll(r.Body)` would return (`unexpected EOF`,
    /// `invalid byte in chunk length`, …); the server reads the body up front,
    /// handlers report this like a body read failure.
    pub body_error: Option<String>,
    /// `ip:port` of the peer (`r.RemoteAddr`).
    pub remote_addr: String,
}

impl Request {
    /// Go `r.Header.Get(name)`: first value, case-insensitive name, `""` when
    /// absent.
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    }

    /// Whether the header is present (with any value).
    pub fn has_header(&self, name: &str) -> bool {
        self.headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(name))
    }

    /// Go `r.FormValue(key)`: the first value of `key` in the POST/PUT/PATCH
    /// `application/x-www-form-urlencoded` body (bodies above 10 MB are not
    /// parsed), then in the URL query; `""` when absent. Parse errors are
    /// ignored like Go.
    pub fn form_value(&self, key: &str) -> String {
        let mut pairs = Vec::new();
        if matches!(self.method.as_str(), "POST" | "PUT" | "PATCH") {
            let ct = self.header("Content-Type");
            let media = ct.split(';').next().unwrap_or("").trim();
            if media.eq_ignore_ascii_case("application/x-www-form-urlencoded")
                && self.body.len() <= MAX_FORM_SIZE
            {
                pairs = gourl::parse_query(&self.body).0;
            }
        }
        pairs.extend(gourl::parse_query(self.raw_query.as_bytes()).0);
        pairs
            .into_iter()
            .find(|(k, _)| k == key.as_bytes())
            .map(|(_, v)| String::from_utf8_lossy(&v).into_owned())
            .unwrap_or_default()
    }
}

/// A response to write (Go `ResponseWriter` usage: set headers, write the
/// status, write the body).
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    /// Handler-set headers (written sorted by name like Go's `Header.Write`).
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: u16) -> Self {
        Response {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// `w.Header().Set("Content-Type", ct); w.WriteHeader(status); w.Write(body)`.
    pub fn with_body(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        let mut r = Response::new(status);
        r.set_header("Content-Type", content_type);
        r.body = body.into();
        r
    }

    /// Go `Header.Set`: replace every value of `name`.
    pub fn set_header(&mut self, name: &str, value: &str) {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.headers.push((name.to_string(), value.to_string()));
    }

    /// Go `http.NotFound`.
    pub fn not_found() -> Self {
        Response::error("404 page not found", 404)
    }

    /// Go `http.Error(w, error, code)`.
    pub fn error(error: &str, code: u16) -> Self {
        let mut r = Response::new(code);
        r.set_header("Content-Type", "text/plain; charset=utf-8");
        r.set_header("X-Content-Type-Options", "nosniff");
        r.body = format!("{}\n", error).into_bytes();
        r
    }

    /// Go `http.Redirect` for a relative `url` (the HTML body is only written
    /// for GET and HEAD requests).
    pub fn redirect(method: &str, url: &str, code: u16) -> Self {
        let mut r = Response::new(code);
        r.set_header("Location", url);
        if method == "GET" || method == "HEAD" {
            r.set_header("Content-Type", "text/html; charset=utf-8");
            r.body = format!(
                "<a href=\"{}\">{}</a>.\n\n",
                html_escape(url),
                status_text(code)
            )
            .into_bytes();
        }
        r
    }
}

/// Go `html.EscapeString`: escapes `<`, `>`, `&`, `'` and `"`.
pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// Go `http.StatusText`.
pub fn status_text(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Request Entity Too Large",
        414 => "Request URI Too Long",
        415 => "Unsupported Media Type",
        417 => "Expectation Failed",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        _ => "",
    }
}

/// `Date` header value: `Fri, 11 Sep 2026 11:54:11 GMT`.
pub fn http_date() -> String {
    chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

/// Go `path.Clean`.
pub fn path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let rooted = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if let Some(last) = out.last() {
                    if *last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !rooted {
                    out.push("..");
                }
            }
            s => out.push(s),
        }
    }
    let joined = out.join("/");
    if rooted {
        format!("/{}", joined)
    } else if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}

/// Go `net/http` `cleanPath`: `path.Clean` keeping a trailing slash.
pub fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return "/".to_string();
    }
    let p = if p.starts_with('/') {
        p.to_string()
    } else {
        format!("/{}", p)
    };
    let mut np = path_clean(&p);
    if p.ends_with('/') && np != "/" {
        np.push('/');
    }
    np
}

/// A request handler (Go `http.HandlerFunc`).
pub type Handler = Arc<dyn Fn(&Request) -> Response + Send + Sync>;

/// Go `http.ServeMux` for path-only patterns: an exact pattern (`/hook`)
/// matches its path only, a pattern ending in `/` matches the whole subtree,
/// the longest pattern wins; unclean paths are redirected (307) to their
/// cleaned form, `/tree` is redirected (301) to `/tree/` when only the
/// subtree pattern is registered, everything else is `404 page not found`.
#[derive(Default, Clone)]
pub struct ServeMux {
    entries: Vec<(String, Handler)>,
}

impl ServeMux {
    pub fn new() -> Self {
        ServeMux::default()
    }

    /// Go `mux.HandleFunc(pattern, f)`.
    pub fn handle_func<F>(&mut self, pattern: &str, f: F)
    where
        F: Fn(&Request) -> Response + Send + Sync + 'static,
    {
        self.entries.push((pattern.to_string(), Arc::new(f)));
    }

    fn matches(pattern: &str, path: &str) -> bool {
        if pattern.ends_with('/') {
            path.starts_with(pattern)
        } else {
            pattern == path
        }
    }

    fn find(&self, path: &str) -> Option<&Handler> {
        self.entries
            .iter()
            .filter(|(p, _)| ServeMux::matches(p, path))
            .max_by_key(|(p, _)| p.len())
            .map(|(_, h)| h)
    }

    fn with_query(&self, path: &str, req: &Request) -> String {
        if req.raw_query.is_empty() {
            path.to_string()
        } else {
            format!("{}?{}", path, req.raw_query)
        }
    }

    /// Route `req` (Go `ServeMux.ServeHTTP`).
    pub fn serve(&self, req: &Request) -> Response {
        // Go: a `*` request target is a bad request (closing the connection
        // on HTTP/1.1).
        if req.request_uri == "*" {
            let mut r = Response::new(400);
            if req.proto != "HTTP/1.0" {
                r.set_header("Connection", "close");
            }
            return r;
        }
        // Go: CONNECT paths are matched as they are (no cleaning/redirect).
        if req.method == "CONNECT" {
            return match self.find(&req.path) {
                Some(h) => h(req),
                None => Response::not_found(),
            };
        }
        let path = clean_path(&req.path);
        // /tree → /tree/ when only the subtree pattern exists.
        if self.find(&path).is_none() {
            let slashed = format!("{}/", path);
            if self.entries.iter().any(|(p, _)| p == &slashed) {
                return Response::redirect(&req.method, &self.with_query(&slashed, req), 301);
            }
        }
        if path != req.path {
            return Response::redirect(&req.method, &self.with_query(&path, req), 307);
        }
        match self.find(&path) {
            Some(h) => h(req),
            None => Response::not_found(),
        }
    }

    /// The mux as a [`Handler`].
    pub fn into_handler(self) -> Handler {
        Arc::new(move |req: &Request| self.serve(req))
    }
}

/// Listen on `addr` (Go `net.Listen("tcp", addr)` forms: `host:port`,
/// `:port` — every interface, dual-stack when IPv6 is available — and
/// `[v6]:port`; IPv4 is preferred for names resolving to several addresses).
/// The error is worded like Go: `listen tcp 127.0.0.1:1982: bind: address
/// already in use`.
pub fn listen(addr: &str) -> Result<TcpListener, String> {
    let (host, port) = match addr.rfind(':') {
        Some(p) => (&addr[..p], &addr[p + 1..]),
        None => {
            return Err(format!(
                "listen tcp: address {}: missing port in address",
                addr
            ))
        }
    };
    // Go resolves a non-numeric port as a service name (`/etc/services` or
    // getaddrinfo's EAI_SERVICE) and reports `unknown port` either way.
    let port: u16 = match port.parse() {
        Ok(p) => p,
        Err(_) => return Err(format!("listen tcp: lookup tcp/{}: unknown port", port)),
    };
    if host.is_empty() {
        return listen_all(port).map_err(|e| format!("listen tcp :{}: bind: {}", port, e));
    }
    let host_bare = host.trim_start_matches('[').trim_end_matches(']');
    let resolved: Vec<SocketAddr> = match (host_bare, port).to_socket_addrs() {
        Ok(it) => it.collect(),
        Err(_) => return Err(format!("listen tcp: lookup {}: no such host", host_bare)),
    };
    let chosen = resolved
        .iter()
        .find(|a| a.is_ipv4())
        .or(resolved.first())
        .copied()
        .ok_or_else(|| format!("listen tcp: lookup {}: no such host", host_bare))?;
    TcpListener::bind(chosen)
        .map_err(|e| format!("listen tcp {}: bind: {}", chosen, go_io_error_string(&e)))
}

#[cfg(unix)]
fn listen_all(port: u16) -> Result<TcpListener, String> {
    use std::os::fd::FromRawFd;
    // Go listens dual-stack on `[::]:port` (IPV6_V6ONLY off), falling back to
    // IPv4 when the system has no IPv6.
    unsafe {
        let fd = libc::socket(libc::AF_INET6, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return TcpListener::bind(("0.0.0.0", port)).map_err(|e| go_io_error_string(&e));
        }
        let listener = TcpListener::from_raw_fd(fd);
        let on: libc::c_int = 1;
        let off: libc::c_int = 0;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &on as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd,
            libc::IPPROTO_IPV6,
            libc::IPV6_V6ONLY,
            &off as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        let mut sa: libc::sockaddr_in6 = std::mem::zeroed();
        sa.sin6_family = libc::AF_INET6 as libc::sa_family_t;
        sa.sin6_port = port.to_be();
        if libc::bind(
            fd,
            &sa as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
        ) != 0
        {
            let e = io::Error::last_os_error();
            drop(listener);
            return Err(go_io_error_string(&e));
        }
        if libc::listen(fd, 4096) != 0 {
            let e = io::Error::last_os_error();
            drop(listener);
            return Err(go_io_error_string(&e));
        }
        Ok(listener)
    }
}

#[cfg(not(unix))]
fn listen_all(port: u16) -> Result<TcpListener, String> {
    TcpListener::bind(("0.0.0.0", port)).map_err(|e| go_io_error_string(&e))
}

/// Go `http.ListenAndServe(addr, handler)`: returns only when listening
/// fails (accept errors are retried with Go's backoff).
pub fn listen_and_serve(addr: &str, handler: Handler) -> Result<(), String> {
    let listener = listen(addr)?;
    serve(listener, handler)
}

/// Go `http.Serve`: accept connections forever, one thread each.
pub fn serve(listener: TcpListener, handler: Handler) -> Result<(), String> {
    let mut delay = Duration::from_millis(0);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                delay = Duration::from_millis(0);
                let handler = handler.clone();
                thread::spawn(move || serve_conn(stream, handler));
            }
            Err(e) => {
                delay = if delay.is_zero() {
                    Duration::from_millis(5)
                } else {
                    (delay * 2).min(Duration::from_secs(1))
                };
                eprintln!(
                    "{} http: Accept error: accept tcp {}: {}; retrying in {}",
                    chrono::Local::now().format("%Y/%m/%d %H:%M:%S"),
                    listener
                        .local_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_default(),
                    go_io_error_string(&e),
                    crate::time::format_go_duration(delay)
                );
                thread::sleep(delay);
            }
        }
    }
}

enum HeadError {
    Eof,
    Io,
    TooLarge,
    Malformed,
    /// `HTTP/X.Y` with a major version other than 1.
    UnsupportedVersion,
}

struct Head {
    method: String,
    request_uri: String,
    proto: String,
    /// Protocol is at least HTTP/1.1 (Go `ProtoAtLeast(1, 1)`).
    http11: bool,
    headers: Vec<(String, String)>,
}

/// Go `http.ParseHTTPVersion`: `HTTP/X.Y` with single-digit X and Y.
fn parse_http_version(v: &str) -> Option<(u8, u8)> {
    match v {
        "HTTP/1.1" => Some((1, 1)),
        "HTTP/1.0" => Some((1, 0)),
        _ => {
            let b = v.as_bytes();
            if b.len() != 8 || !v.starts_with("HTTP/") || b[6] != b'.' {
                return None;
            }
            if !b[5].is_ascii_digit() || !b[7].is_ascii_digit() {
                return None;
            }
            Some((b[5] - b'0', b[7] - b'0'))
        }
    }
}

/// Go `httpguts.ValidHeaderFieldValue`: no control characters except HTAB.
fn valid_header_value(v: &str) -> bool {
    v.bytes().all(|b| b == b'\t' || (b >= 0x20 && b != 0x7f))
}

/// Go `httpguts.ValidHostHeader`: unreserved, sub-delims, `%`, `:`, `[`, `]`.
fn valid_host_header(h: &str) -> bool {
    h.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"!$%&'()*+,-.:;=[]_~".contains(&b))
}

fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c))
}

fn read_line(reader: &mut BufReader<TcpStream>, total: &mut usize) -> Result<String, HeadError> {
    let mut line = Vec::new();
    let n = reader
        .read_until(b'\n', &mut line)
        .map_err(|_| HeadError::Io)?;
    if n == 0 {
        return Err(HeadError::Eof);
    }
    *total += n;
    if *total > DEFAULT_MAX_HEADER_BYTES {
        return Err(HeadError::TooLarge);
    }
    if line.ends_with(b"\n") {
        line.pop();
        if line.ends_with(b"\r") {
            line.pop();
        }
    }
    // Go reads the head as bytes; non-UTF-8 bytes are kept (lossily) so that
    // the same validation (e.g. `malformed Host header`) applies.
    Ok(String::from_utf8_lossy(&line).into_owned())
}

fn read_head(reader: &mut BufReader<TcpStream>) -> Result<Head, HeadError> {
    let mut total = 0;
    let line = read_line(reader, &mut total)?;
    let (method, rest) = line.split_once(' ').ok_or(HeadError::Malformed)?;
    let (request_uri, proto) = rest.split_once(' ').ok_or(HeadError::Malformed)?;
    if !is_token(method) {
        return Err(HeadError::Malformed);
    }
    let (major, minor) = parse_http_version(proto).ok_or(HeadError::Malformed)?;
    if major != 1 {
        return Err(HeadError::UnsupportedVersion);
    }
    let mut headers = Vec::new();
    loop {
        let line = match read_line(reader, &mut total) {
            Ok(l) => l,
            Err(HeadError::Eof) => return Err(HeadError::Malformed),
            Err(e) => return Err(e),
        };
        if line.is_empty() {
            break;
        }
        // obs-fold: a line starting with whitespace continues the previous
        // header value (Go's textproto joins them with one space).
        if line.starts_with([' ', '\t']) {
            let Some(last) = headers.last_mut() else {
                return Err(HeadError::Malformed);
            };
            let cont = line.trim_matches([' ', '\t']);
            if !valid_header_value(cont) {
                return Err(HeadError::Malformed);
            }
            let last: &mut (String, String) = last;
            if !last.1.is_empty() && !cont.is_empty() {
                last.1.push(' ');
            }
            last.1.push_str(cont);
            continue;
        }
        let (name, value) = line.split_once(':').ok_or(HeadError::Malformed)?;
        let value = value.trim_matches([' ', '\t']);
        if !is_token(name) || !valid_header_value(value) {
            return Err(HeadError::Malformed);
        }
        headers.push((name.to_string(), value.to_string()));
    }
    // Go: more than one Host header is a bad request.
    if headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("Host"))
        .count()
        > 1
    {
        return Err(HeadError::Malformed);
    }
    Ok(Head {
        method: method.to_string(),
        request_uri: request_uri.to_string(),
        proto: proto.to_string(),
        http11: minor >= 1,
        headers,
    })
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Outcome of reading a request body: the bytes plus the error Go's
/// `io.ReadAll(r.Body)` would have returned (`Err(None)` = connection I/O
/// failure, nothing to answer).
type BodyResult = Result<(Vec<u8>, Option<String>), ()>;

/// Go `net/http/internal.chunkedReader` (error wording included).
fn read_chunked(reader: &mut BufReader<TcpStream>) -> BodyResult {
    let mut body = Vec::new();
    let fail = |body: Vec<u8>, msg: &str| Ok((body, Some(msg.to_string())));
    loop {
        let mut line = Vec::new();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => return fail(body, "unexpected EOF"),
            Ok(_) => {}
            Err(_) => return Err(()),
        }
        if line.len() > 4096 {
            return fail(body, "header line too long");
        }
        let mut size_part: &[u8] = &line;
        while size_part.last().is_some_and(|b| b" \t\r\n".contains(b)) {
            size_part = &size_part[..size_part.len() - 1];
        }
        if let Some(p) = size_part.iter().position(|b| *b == b';') {
            size_part = &size_part[..p];
        }
        if size_part.is_empty() {
            return fail(body, "empty hex number for chunk length");
        }
        let mut size: usize = 0;
        for (i, b) in size_part.iter().enumerate() {
            let d = match b {
                b'0'..=b'9' => b - b'0',
                b'a'..=b'f' => b - b'a' + 10,
                b'A'..=b'F' => b - b'A' + 10,
                _ => return fail(body, "invalid byte in chunk length"),
            };
            if i == 16 {
                return fail(body, "http chunk length too large");
            }
            size = (size << 4) | d as usize;
        }
        if size == 0 {
            // Trailers until the blank line.
            loop {
                let mut t = Vec::new();
                match reader.read_until(b'\n', &mut t) {
                    Ok(0) => return fail(body, "unexpected EOF"),
                    Ok(_) if t == b"\r\n" || t == b"\n" => break,
                    Ok(_) => {}
                    Err(_) => return Err(()),
                }
            }
            return Ok((body, None));
        }
        let mut chunk = vec![0u8; size];
        match reader.read_exact(&mut chunk) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return fail(body, "unexpected EOF")
            }
            Err(_) => return Err(()),
        }
        body.extend_from_slice(&chunk);
        let mut crlf = [0u8; 2];
        match reader.read_exact(&mut crlf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return fail(body, "unexpected EOF")
            }
            Err(_) => return Err(()),
        }
        if crlf != *b"\r\n" {
            return fail(body, "malformed chunked encoding");
        }
    }
}

/// Go's `501 Not Implemented` for unsupported transfer encodings (body
/// without the status prefix).
fn write_simple_error_body(stream: &mut TcpStream, status_line: &str, body: &str) {
    write_simple_error(stream, status_line, body)
}

fn write_simple_error(stream: &mut TcpStream, status_line: &str, body: &str) {
    let _ = stream.write_all(
        format!(
            "HTTP/1.1 {}\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n{}",
            status_line, body
        )
        .as_bytes(),
    );
    let _ = stream.flush();
}

/// Go `net/http` `bodyAllowedForStatus`.
fn body_allowed_for_status(status: u16) -> bool {
    !((100..=199).contains(&status) || status == 204 || status == 304)
}

fn write_response(
    stream: &mut TcpStream,
    http11: bool,
    method: &str,
    resp: &Response,
    close: bool,
) -> io::Result<()> {
    // Go answers HTTP/1.1 to any HTTP/1.x request but HTTP/1.0.
    let proto = if http11 { "HTTP/1.1" } else { "HTTP/1.0" };
    // Go announces the connection state only when it differs from the
    // protocol default: `Connection: close` for HTTP/1.1, `Connection:
    // keep-alive` for HTTP/1.0 — unless the handler set the header itself.
    let handler_set_connection = header(&resp.headers, "Connection").is_some();
    let connection = if handler_set_connection {
        None
    } else if http11 && close {
        Some("close")
    } else if !http11 && !close {
        Some("keep-alive")
    } else {
        None
    };
    let mut out = Vec::with_capacity(resp.body.len() + 256);
    let text = status_text(resp.status);
    if text.is_empty() {
        out.extend_from_slice(
            format!("{} {} status code {}\r\n", proto, resp.status, resp.status).as_bytes(),
        );
    } else {
        out.extend_from_slice(format!("{} {} {}\r\n", proto, resp.status, text).as_bytes());
    }
    let mut headers = resp.headers.clone();
    headers.sort_by_key(|h| h.0.to_ascii_lowercase());
    for (k, v) in &headers {
        out.extend_from_slice(format!("{}: {}\r\n", k, v).as_bytes());
    }
    if header(&headers, "Date").is_none() {
        out.extend_from_slice(format!("Date: {}\r\n", http_date()).as_bytes());
    }
    // Go: 1xx, 204 and 304 responses never carry a body nor a Content-Length
    // (`bodyAllowedForStatus`); a body written by the handler is dropped.
    let body_allowed = body_allowed_for_status(resp.status);
    if body_allowed && header(&headers, "Content-Length").is_none() {
        out.extend_from_slice(format!("Content-Length: {}\r\n", resp.body.len()).as_bytes());
    }
    if let Some(c) = connection {
        out.extend_from_slice(format!("Connection: {}\r\n", c).as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    if method != "HEAD" && body_allowed {
        out.extend_from_slice(&resp.body);
    }
    stream.write_all(&out)?;
    stream.flush()
}

fn serve_conn(stream: TcpStream, handler: Handler) {
    let _ = stream.set_nodelay(true);
    let remote = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let mut writer = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut reader = BufReader::new(stream);
    loop {
        let head = match read_head(&mut reader) {
            Ok(h) => h,
            Err(HeadError::Eof) | Err(HeadError::Io) => return,
            Err(HeadError::TooLarge) => {
                write_simple_error(
                    &mut writer,
                    "431 Request Header Fields Too Large",
                    "431 Request Header Fields Too Large",
                );
                return;
            }
            Err(HeadError::Malformed) => {
                write_simple_error(&mut writer, "400 Bad Request", "400 Bad Request");
                return;
            }
            Err(HeadError::UnsupportedVersion) => {
                write_simple_error(
                    &mut writer,
                    "505 HTTP Version Not Supported: unsupported protocol version",
                    "505 HTTP Version Not Supported: unsupported protocol version",
                );
                return;
            }
        };
        let mut host = header(&head.headers, "Host").unwrap_or("").to_string();
        // Request target: origin form, absolute form, `*` or (CONNECT) an authority.
        let (path_raw, raw_query) = {
            let target = head.request_uri.as_str();
            let target = if let Some(rest) = target
                .strip_prefix("http://")
                .or_else(|| target.strip_prefix("https://"))
            {
                match rest.find('/') {
                    Some(p) => {
                        host = rest[..p].to_string();
                        &rest[p..]
                    }
                    None => {
                        host = rest.to_string();
                        ""
                    }
                }
            } else if head.method == "CONNECT" && !target.starts_with('/') {
                // authority form: Go's URL has an empty path
                ""
            } else {
                target
            };
            if !(target.starts_with('/') || target == "*" || target.is_empty()) {
                write_simple_error(&mut writer, "400 Bad Request", "400 Bad Request");
                return;
            }
            match target.split_once('?') {
                Some((p, q)) => (p.to_string(), q.to_string()),
                None => (target.to_string(), String::new()),
            }
        };
        let path = match gourl::path_unescape_bytes(path_raw.as_bytes()) {
            Ok(p) => String::from_utf8_lossy(&p).into_owned(),
            Err(_) => {
                write_simple_error(&mut writer, "400 Bad Request", "400 Bad Request");
                return;
            }
        };
        // Go: HTTP/1.1 needs a Host *header* (CONNECT excepted) and the
        // header, when present, must be well-formed.
        let host_header = header(&head.headers, "Host");
        if head.http11 && host_header.is_none() && head.method != "CONNECT" {
            write_simple_error(
                &mut writer,
                "400 Bad Request: missing required Host header",
                "400 Bad Request: missing required Host header",
            );
            return;
        }
        if let Some(h) = host_header {
            if !valid_host_header(h) {
                write_simple_error(
                    &mut writer,
                    "400 Bad Request: malformed Host header",
                    "400 Bad Request: malformed Host header",
                );
                return;
            }
        }
        // Transfer-Encoding: only `chunked` is supported (ignored on HTTP/1.0,
        // like Go); with it present Content-Length is ignored.
        let transfer_encodings: Vec<&str> = head
            .headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case("Transfer-Encoding"))
            .map(|(_, v)| v.as_str())
            .collect();
        let chunked = if transfer_encodings.is_empty() || !head.http11 {
            false
        } else if transfer_encodings.len() == 1
            && transfer_encodings[0].trim().eq_ignore_ascii_case("chunked")
        {
            true
        } else {
            write_simple_error_body(
                &mut writer,
                "501 Not Implemented",
                "Unsupported transfer encoding",
            );
            return;
        };
        let content_length = if chunked {
            None
        } else {
            // Go: several Content-Length headers must agree; a value must be
            // decimal digits only; an empty value means no body.
            let values: Vec<&str> = head
                .headers
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
                .map(|(_, v)| v.trim())
                .collect();
            if values.iter().any(|v| *v != values[0]) {
                write_simple_error(&mut writer, "400 Bad Request", "400 Bad Request");
                return;
            }
            match values.first() {
                Some(v) => {
                    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                        write_simple_error(&mut writer, "400 Bad Request", "400 Bad Request");
                        return;
                    }
                    match v.parse::<usize>() {
                        Ok(n) => Some(n),
                        Err(_) => {
                            write_simple_error(&mut writer, "400 Bad Request", "400 Bad Request");
                            return;
                        }
                    }
                }
                None => Some(0),
            }
        };
        let has_token = |name: &str, token: &str| {
            head.headers
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case(name))
                .any(|(_, v)| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token)))
        };
        let mut close = if head.http11 {
            has_token("Connection", "close")
        } else {
            !has_token("Connection", "keep-alive")
        };
        // `Expect`: only `100-continue` is understood (Go answers 417 to
        // anything else and closes the connection).
        let expects_body = chunked || content_length.unwrap_or(0) > 0;
        if has_token("Expect", "100-continue") {
            if expects_body && head.http11 {
                if writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").is_err() {
                    return;
                }
                let _ = writer.flush();
            }
        } else if header(&head.headers, "Expect").is_some_and(|v| !v.is_empty()) {
            let mut r = Response::new(417);
            r.set_header("Connection", "close");
            let _ = write_response(&mut writer, head.http11, &head.method, &r, true);
            let _ = writer.shutdown(std::net::Shutdown::Both);
            return;
        }
        let (body, body_error) = if chunked {
            match read_chunked(&mut reader) {
                Ok(r) => r,
                Err(()) => return,
            }
        } else {
            let n = content_length.unwrap_or(0);
            let mut b = vec![0u8; n];
            let mut got = 0;
            let mut err = None;
            while got < n {
                match reader.read(&mut b[got..]) {
                    Ok(0) => {
                        err = Some("unexpected EOF".to_string());
                        break;
                    }
                    Ok(k) => got += k,
                    Err(_) => return,
                }
            }
            b.truncate(got);
            (b, err)
        };
        if body_error.is_some() {
            close = true;
        }
        let req = Request {
            method: head.method.clone(),
            request_uri: head.request_uri.clone(),
            path,
            raw_query,
            proto: head.proto.clone(),
            host,
            headers: head.headers.clone(),
            body,
            body_error,
            remote_addr: remote.clone(),
        };
        // Go's server answers `OPTIONS *` itself (globalOptionsHandler).
        let resp = if head.request_uri == "*" && head.method == "OPTIONS" {
            let mut r = Response::new(200);
            r.set_header("Content-Length", "0");
            Ok(r)
        } else {
            catch_unwind(AssertUnwindSafe(|| handler(&req)))
        };
        let resp = match resp {
            Ok(r) => r,
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "panic".to_string());
                eprintln!(
                    "{} http: panic serving {}: {}",
                    chrono::Local::now().format("%Y/%m/%d %H:%M:%S"),
                    remote,
                    msg
                );
                return;
            }
        };
        // A handler-set `Connection: close` closes the connection too.
        if header(&resp.headers, "Connection")
            .map(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case("close")))
            .unwrap_or(false)
        {
            close = true;
        }
        if write_response(&mut writer, head.http11, &head.method, &resp, close).is_err() {
            return;
        }
        if close {
            let _ = writer.shutdown(std::net::Shutdown::Both);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_clean_like_go() {
        for (input, want) in [
            ("", "."),
            ("abc", "abc"),
            ("abc/def", "abc/def"),
            ("a/b/c", "a/b/c"),
            (".", "."),
            ("..", ".."),
            ("../..", "../.."),
            ("../../abc", "../../abc"),
            ("/abc", "/abc"),
            ("/", "/"),
            ("abc/", "abc"),
            ("abc/def/", "abc/def"),
            ("a/b/c/", "a/b/c"),
            ("./", "."),
            ("../", ".."),
            ("../../", "../.."),
            ("/abc/", "/abc"),
            ("abc//def//ghi", "abc/def/ghi"),
            ("//abc", "/abc"),
            ("///abc", "/abc"),
            ("//abc//", "/abc"),
            ("abc//", "abc"),
            ("abc/./def", "abc/def"),
            ("/./abc/def", "/abc/def"),
            ("abc/.", "abc"),
            ("abc/def/ghi/../jkl", "abc/def/jkl"),
            ("abc/def/../ghi/../jkl", "abc/jkl"),
            ("abc/def/..", "abc"),
            ("abc/def/../..", "."),
            ("/abc/def/../..", "/"),
            ("abc/def/../../..", ".."),
            ("/abc/def/../../..", "/"),
            ("abc/def/../../../ghi/jkl/../../../mno", "../../mno"),
            ("abc/./../def", "def"),
            ("abc//./../def", "def"),
            ("abc/../../././../def", "../../def"),
        ] {
            assert_eq!(path_clean(input), want, "path_clean({input:?})");
        }
    }

    #[test]
    fn clean_path_keeps_trailing_slash() {
        assert_eq!(clean_path(""), "/");
        assert_eq!(clean_path("hook"), "/hook");
        assert_eq!(clean_path("//hook"), "/hook");
        assert_eq!(clean_path("/hook/"), "/hook/");
        assert_eq!(clean_path("/a/./b/../c/"), "/a/c/");
        assert_eq!(clean_path("/"), "/");
        assert_eq!(clean_path("//"), "/");
    }

    fn req(method: &str, path: &str, query: &str) -> Request {
        Request {
            method: method.to_string(),
            path: path.to_string(),
            raw_query: query.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn mux_routing() {
        let mut mux = ServeMux::new();
        mux.handle_func("/hook", |_r| Response::with_body(200, "text/plain", "hook"));
        mux.handle_func("/tree/", |_r| {
            Response::with_body(200, "text/plain", "tree")
        });
        assert_eq!(mux.serve(&req("GET", "/hook", "")).body, b"hook");
        assert_eq!(mux.serve(&req("POST", "/hook", "x=1")).body, b"hook");
        assert_eq!(mux.serve(&req("GET", "/hook/", "")).status, 404);
        assert_eq!(mux.serve(&req("GET", "/other", "")).status, 404);
        assert_eq!(mux.serve(&req("GET", "/tree/", "")).body, b"tree");
        assert_eq!(mux.serve(&req("GET", "/tree/a/b", "")).body, b"tree");
        let r = mux.serve(&req("GET", "/tree", "q=1"));
        assert_eq!(r.status, 301);
        assert_eq!(
            r.headers.iter().find(|h| h.0 == "Location").unwrap().1,
            "/tree/?q=1"
        );
        let r = mux.serve(&req("GET", "//hook", "x=1"));
        assert_eq!(r.status, 307);
        assert_eq!(
            r.headers.iter().find(|h| h.0 == "Location").unwrap().1,
            "/hook?x=1"
        );
        assert_eq!(r.body, b"<a href=\"/hook?x=1\">Temporary Redirect</a>.\n\n");
        let r = mux.serve(&req("POST", "//hook", ""));
        assert_eq!(r.status, 307);
        assert!(r.body.is_empty());
        let mut root = ServeMux::new();
        root.handle_func("/", |_r| Response::with_body(200, "text/plain", "root"));
        assert_eq!(root.serve(&req("GET", "/anything/x", "")).body, b"root");
        assert_eq!(root.serve(&req("GET", "/", "")).body, b"root");
    }

    #[test]
    fn not_found_response() {
        let r = Response::not_found();
        assert_eq!(r.status, 404);
        assert_eq!(r.body, b"404 page not found\n");
        assert_eq!(status_text(404), "Not Found");
        assert_eq!(status_text(431), "Request Header Fields Too Large");
    }

    #[test]
    fn form_value_like_go() {
        let mut r = req("POST", "/hook", "payload=fromquery&b=2");
        r.headers.push((
            "content-type".into(),
            "application/x-www-form-urlencoded; charset=utf-8".into(),
        ));
        r.body = b"payload=%7B%22a%22%3A+1%7D&payload=second".to_vec();
        assert_eq!(r.form_value("payload"), "{\"a\": 1}");
        assert_eq!(r.form_value("b"), "2");
        assert_eq!(r.form_value("c"), "");
        r.headers.clear();
        assert_eq!(r.form_value("payload"), "fromquery");
        let mut g = req("GET", "/hook", "");
        g.body = b"payload=x".to_vec();
        assert_eq!(g.form_value("payload"), "");
    }

    #[test]
    fn header_lookup() {
        let mut r = req("GET", "/", "");
        r.headers.push(("Signature".into(), "abc".into()));
        r.headers.push(("signature".into(), "def".into()));
        assert_eq!(r.header("SIGNATURE"), "abc");
        assert_eq!(r.header("Missing"), "");
        assert!(r.has_header("signature"));
    }

    #[test]
    fn http_date_format() {
        let d = http_date();
        assert!(d.ends_with(" GMT"), "{d}");
        assert_eq!(d.len(), 29, "{d}");
    }

    #[test]
    fn end_to_end_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut mux = ServeMux::new();
        mux.handle_func("/echo", |r| {
            Response::with_body(
                200,
                "application/json",
                format!(
                    "{{\"method\": \"{}\", \"body\": {}, \"remote\": \"{}\"}}",
                    r.method,
                    r.body.len(),
                    r.remote_addr.starts_with("127.0.0.1:")
                ),
            )
        });
        mux.handle_func("/boom", |_r| panic!("kaboom"));
        let handler = mux.into_handler();
        thread::spawn(move || {
            let _ = serve(listener, handler);
        });
        let send = |req: &[u8]| -> String {
            let mut s = TcpStream::connect(addr).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.write_all(req).unwrap();
            let mut out = Vec::new();
            let _ = s.read_to_end(&mut out);
            String::from_utf8_lossy(&out).into_owned()
        };
        let r = send(
            b"POST /echo HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: 3\r\n\r\nabc",
        );
        assert!(
            r.starts_with("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nDate: "),
            "{r}"
        );
        assert!(r.contains("\r\nContent-Length: 47\r\nConnection: close\r\n\r\n{\"method\": \"POST\", \"body\": 3, \"remote\": \"true\"}"), "{r}");
        let r = send(b"GET /nope HTTP/1.0\r\n\r\n");
        assert!(r.starts_with("HTTP/1.0 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nX-Content-Type-Options: nosniff\r\nDate: "), "{r}");
        assert!(
            r.ends_with("\r\nContent-Length: 19\r\n\r\n404 page not found\n"),
            "{r}"
        );
        assert!(!r.contains("Connection:"), "{r}");
        let r = send(b"GET /echo HTTP/1.1\r\n\r\n");
        assert_eq!(r, "HTTP/1.1 400 Bad Request: missing required Host header\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request: missing required Host header");
        let r = send(b"BLAH\r\n\r\n");
        assert_eq!(r, "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request");
        let r = send(b"POST /echo HTTP/1.1\r\nHost: x\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n");
        assert!(r.contains("\"body\": 5"), "{r}");
        let r = send(b"POST /echo HTTP/1.1\r\nHost: x\r\nConnection: close\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nhi");
        assert!(
            r.starts_with("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n"),
            "{r}"
        );
        let r = send(b"HEAD /echo HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        assert!(
            r.contains("Content-Length: 47\r\nConnection: close\r\n\r\n"),
            "{r}"
        );
        assert!(r.ends_with("\r\n\r\n"), "{r}");
        let r = send(b"GET /boom HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!(
            r, "",
            "panicking handler closes the connection without a response"
        );
        // keep-alive: two requests on one connection.
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.write_all(b"GET /echo HTTP/1.1\r\nHost: x\r\n\r\nGET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        let text = String::from_utf8_lossy(&out);
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "{text}");
        assert!(text.contains("HTTP/1.1 404 Not Found\r\n"), "{text}");
        assert!(text.ends_with("404 page not found\n"), "{text}");
    }

    #[test]
    fn listen_errors_like_go() {
        let l = listen("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let err = listen(&format!("127.0.0.1:{}", port)).unwrap_err();
        assert_eq!(
            err,
            format!(
                "listen tcp 127.0.0.1:{}: bind: address already in use",
                port
            )
        );
        assert_eq!(
            listen("127.0.0.1").unwrap_err(),
            "listen tcp: address 127.0.0.1: missing port in address"
        );
        assert_eq!(
            listen("127.0.0.1:abc").unwrap_err(),
            "listen tcp: lookup tcp/abc: unknown port"
        );
        let all = listen(":0").unwrap();
        assert!(all.local_addr().is_ok());
    }
}
