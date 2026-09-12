//! A scripted stand-in for the GitHub REST API, for the Go ⇄ Rust tests of
//! the tools that talk to GitHub (`sync_issues`, `ghapi2db`).
//!
//! Both binaries under test are pointed at it with `GHA2DB_GITHUB_API_URL`.
//! Every route (`METHOD /path`, optionally with an exact `?query`) serves a
//! sequence of scripted responses — the last one sticks — with GitHub-like
//! `X-RateLimit-*` headers taken from a per-token rate state; unknown routes
//! answer GitHub's `404 Not Found`; `GET /rate_limit` renders the rate state
//! unless scripted. Every request is logged (method, path, sorted query,
//! bearer token, `Accept`, body) so a test can assert that the Go and the
//! Rust binary made the same calls.

use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use devstatscode::http::{self, Handler, Request, Response};
use serde_json::{json, Value};

/// GitHub's documentation link in error bodies.
pub const DOCS_URL: &str = "https://docs.github.com/rest";
/// The `documentation_url` that makes go-github report an
/// `*github.AbuseRateLimitError`.
pub const ABUSE_DOCS_URL: &str =
    "https://docs.github.com/en/rest/overview/resources-in-the-rest-api#abuse-rate-limits";

/// Seconds since the Unix epoch.
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One scripted response.
#[derive(Debug, Clone)]
pub struct Scripted {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: String,
    /// Extra headers (override the generated ones of the same name).
    pub headers: Vec<(String, String)>,
    /// Add `X-RateLimit-*` headers from the caller's rate state.
    pub rate_headers: bool,
    /// Drop the connection without answering (a transport error for the
    /// client: go-github returns a nil `*Response`). Not logged.
    pub hangup: bool,
}

impl Scripted {
    /// `status` with a JSON body.
    pub fn json(status: u16, body: &Value) -> Scripted {
        Scripted {
            status,
            body: serde_json::to_vec(body).unwrap(),
            content_type: "application/json; charset=utf-8".to_string(),
            headers: Vec::new(),
            rate_headers: true,
            hangup: false,
        }
    }

    /// `200 OK` with a JSON body.
    pub fn ok(body: &Value) -> Scripted {
        Scripted::json(200, body)
    }

    /// A raw body (for malformed JSON and the like).
    pub fn raw(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Scripted {
        Scripted {
            status,
            body: body.into(),
            content_type: content_type.to_string(),
            headers: Vec::new(),
            rate_headers: true,
            hangup: false,
        }
    }

    /// GitHub's error body: `{"message": …, "documentation_url": …}`.
    pub fn error(status: u16, message: &str) -> Scripted {
        Scripted::json(
            status,
            &json!({"message": message, "documentation_url": DOCS_URL}),
        )
    }

    /// `404 Not Found`.
    pub fn not_found() -> Scripted {
        Scripted::error(404, "Not Found")
    }

    /// `403` with `X-RateLimit-Remaining: 0` (a `*github.RateLimitError`),
    /// resetting `reset_in` seconds from now.
    pub fn rate_limited(limit: i64, reset_in: i64) -> Scripted {
        Scripted::json(
            403,
            &json!({
                "message": "API rate limit exceeded for user ID 1.",
                "documentation_url": "https://docs.github.com/rest/overview/resources-in-the-rest-api#rate-limiting"
            }),
        )
        .header("X-RateLimit-Limit", &limit.to_string())
        .header("X-RateLimit-Remaining", "0")
        .header("X-RateLimit-Reset", &(now_unix() + reset_in).to_string())
    }

    /// `403` abuse detection (a `*github.AbuseRateLimitError`), optionally
    /// with `Retry-After`.
    pub fn abuse(retry_after: Option<u64>) -> Scripted {
        let s = Scripted::json(
            403,
            &json!({
                "message": "You have triggered an abuse detection mechanism. Please wait a few minutes before you try again.",
                "documentation_url": ABUSE_DOCS_URL
            }),
        );
        match retry_after {
            Some(r) => s.header("Retry-After", &r.to_string()),
            None => s,
        }
    }

    /// `202 Accepted` with an empty body (a `*github.AcceptedError`).
    pub fn accepted() -> Scripted {
        Scripted::raw(202, "application/json; charset=utf-8", Vec::new())
    }

    /// A redirect (`Location` may be empty: Go returns such a 3xx as-is).
    pub fn redirect(status: u16, location: &str) -> Scripted {
        let s = Scripted::json(
            status,
            &json!({"message": http::status_text(status), "url": location, "documentation_url": DOCS_URL}),
        );
        if location.is_empty() {
            s
        } else {
            s.header("Location", location)
        }
    }

    /// Add GitHub's `Link` pagination header for page `page` of `last`
    /// (`base` is the path with its fixed query, e.g.
    /// `/repos/o/r/forks?per_page=100&sort=newest`): `next`/`last` while
    /// more pages follow, `prev`/`first` after the first page.
    pub fn paged(self, base: &str, page: i64, last: i64) -> Scripted {
        let sep = if base.contains('?') { '&' } else { '?' };
        let mut parts = Vec::new();
        if page < last {
            parts.push(format!("<{base}{sep}page={}>; rel=\"next\"", page + 1));
            parts.push(format!("<{base}{sep}page={last}>; rel=\"last\""));
        }
        if page > 1 {
            parts.push(format!("<{base}{sep}page={}>; rel=\"prev\"", page - 1));
            parts.push(format!("<{base}{sep}page=1>; rel=\"first\""));
        }
        if parts.is_empty() {
            return self;
        }
        self.header("Link", &parts.join(", "))
    }

    /// Close the connection without a response (Go: `Get "…": EOF`).
    pub fn hangup() -> Scripted {
        let mut s = Scripted::raw(0, "text/plain", Vec::new());
        s.hangup = true;
        s
    }

    pub fn header(mut self, name: &str, value: &str) -> Scripted {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// No `X-RateLimit-*` headers on this response.
    pub fn without_rate_headers(mut self) -> Scripted {
        self.rate_headers = false;
        self
    }
}

/// The rate limit state of one token (or of anonymous access).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateState {
    pub limit: i64,
    pub remaining: i64,
    /// Unix time of the reset.
    pub reset: i64,
}

/// One logged request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Logged {
    pub method: String,
    pub path: String,
    /// Query parameters sorted and re-joined with `&` (`""` when none).
    pub query: String,
    /// The bearer token (`None` for anonymous requests).
    pub token: Option<String>,
    pub accept: String,
    pub body: Vec<u8>,
}

impl Logged {
    /// `GET /repos/o/r/issues/1?page=2&per_page=100 accept=<…> auth=<tok>`.
    pub fn summary(&self) -> String {
        let q = if self.query.is_empty() {
            String::new()
        } else {
            format!("?{}", self.query)
        };
        format!(
            "{} {}{} accept={} auth={}",
            self.method,
            self.path,
            q,
            self.accept,
            self.token.as_deref().unwrap_or("-")
        )
    }
}

struct State {
    routes: Mutex<HashMap<String, (Vec<Scripted>, usize)>>,
    rates: Mutex<HashMap<Option<String>, RateState>>,
    default_rate: Mutex<RateState>,
    /// Decrement `remaining` for every non-`rate_limit` request (GitHub does).
    count_points: Mutex<bool>,
    log: Mutex<Vec<Logged>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Percent-decode one query component (`+` is a space).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                Ok(b) => {
                    out.push(b);
                    i += 2;
                }
                Err(_) => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

/// `a=1&b=x%3Ay` → `{a: 1, b: x:y}` (decoded keys and values).
fn decode_query(raw: &str) -> BTreeMap<String, String> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

impl State {
    fn rate_for(&self, token: &Option<String>) -> RateState {
        let default = *lock(&self.default_rate);
        *lock(&self.rates).entry(token.clone()).or_insert(default)
    }

    fn take_point(&self, token: &Option<String>) {
        if !*lock(&self.count_points) {
            return;
        }
        let default = *lock(&self.default_rate);
        let mut rates = lock(&self.rates);
        let r = rates.entry(token.clone()).or_insert(default);
        if r.remaining > 0 {
            r.remaining -= 1;
        }
    }

    fn scripted(&self, key: &str) -> Option<Scripted> {
        let mut routes = lock(&self.routes);
        let (seq, idx) = routes.get_mut(key)?;
        if seq.is_empty() {
            return None;
        }
        let i = (*idx).min(seq.len() - 1);
        *idx += 1;
        Some(seq[i].clone())
    }

    /// The most specific `METHOD path?params` route whose (percent-decoded)
    /// parameters are all present in the request with the same values.
    fn subset_route(&self, method: &str, path: &str, raw_query: &str) -> Option<String> {
        let want = decode_query(raw_query);
        let prefix = format!("{method} {path}?");
        let routes = lock(&self.routes);
        let mut best: Option<(usize, String)> = None;
        for key in routes.keys() {
            let q = match key.strip_prefix(&prefix) {
                Some(q) => q,
                None => continue,
            };
            let have = decode_query(q);
            if have
                .iter()
                .all(|(k, v)| want.get(k).map(|w| w == v).unwrap_or(false))
                && best.as_ref().map(|(n, _)| have.len() > *n).unwrap_or(true)
            {
                best = Some((have.len(), key.clone()));
            }
        }
        best.map(|(_, k)| k)
    }

    fn handle(&self, req: &Request) -> Response {
        let auth = req.header("Authorization");
        let token = ["Bearer ", "bearer ", "token "]
            .iter()
            .find_map(|p| auth.strip_prefix(p))
            .map(|t| t.to_string());
        let mut params: Vec<String> = req
            .raw_query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| p.to_string())
            .collect();
        params.sort();
        lock(&self.log).push(Logged {
            method: req.method.clone(),
            path: req.path.clone(),
            query: params.join("&"),
            token: token.clone(),
            accept: req.header("Accept").to_string(),
            body: req.body.clone(),
        });
        let is_rate_limit = req.method == "GET" && req.path == "/rate_limit";
        let mut scripted = None;
        if !req.raw_query.is_empty() {
            scripted = self.scripted(&format!("{} {}?{}", req.method, req.path, req.raw_query));
            if scripted.is_none() {
                if let Some(key) = self.subset_route(&req.method, &req.path, &req.raw_query) {
                    scripted = self.scripted(&key);
                }
            }
        }
        if scripted.is_none() {
            scripted = self.scripted(&format!("{} {}", req.method, req.path));
        }
        if scripted.as_ref().map(|s| s.hangup).unwrap_or(false) {
            lock(&self.log).pop();
            // Unwind through the server's `catch_unwind`, which drops the
            // connection (no panic hook output).
            std::panic::resume_unwind(Box::new("hangup"));
        }
        if !is_rate_limit {
            self.take_point(&token);
        }
        let rate = self.rate_for(&token);
        let scripted = match scripted {
            Some(s) => s,
            None if is_rate_limit => {
                let core = json!({
                    "limit": rate.limit,
                    "used": rate.limit - rate.remaining,
                    "remaining": rate.remaining,
                    "reset": rate.reset
                });
                let search =
                    json!({"limit": 30, "used": 0, "remaining": 30, "reset": now_unix() + 60});
                Scripted::ok(&json!({
                    "resources": {
                        "core": core,
                        "search": search,
                        "graphql": {"limit": 5000, "used": 0, "remaining": 5000, "reset": rate.reset},
                        "integration_manifest": {"limit": 5000, "used": 0, "remaining": 5000, "reset": rate.reset}
                    },
                    "rate": core
                }))
            }
            None => Scripted::not_found(),
        };
        let mut resp = Response::new(scripted.status);
        resp.set_header("Content-Type", &scripted.content_type);
        resp.set_header("Server", "GitHub.com");
        resp.set_header("X-GitHub-Media-Type", "github.v3; format=json");
        resp.set_header("X-GitHub-Request-Id", "0000:0000:000000:000000:00000000");
        if scripted.rate_headers {
            resp.set_header("X-RateLimit-Limit", &rate.limit.to_string());
            resp.set_header("X-RateLimit-Remaining", &rate.remaining.to_string());
            resp.set_header("X-RateLimit-Reset", &rate.reset.to_string());
            resp.set_header(
                "X-RateLimit-Used",
                &(rate.limit - rate.remaining).to_string(),
            );
            resp.set_header("X-RateLimit-Resource", "core");
        }
        for (k, v) in &scripted.headers {
            resp.set_header(k, v);
        }
        resp.body = scripted.body;
        resp
    }
}

/// A running fake GitHub API server (lives until the test process exits).
pub struct FakeGitHub {
    base_url: String,
    state: Arc<State>,
}

impl std::fmt::Debug for FakeGitHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeGitHub")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl FakeGitHub {
    /// Listen on an ephemeral `127.0.0.1` port. Ports containing `301` are
    /// skipped: `HandlePossibleError` treats any error message containing
    /// `301` (and the message contains the URL) as "moved permanently".
    pub fn start() -> FakeGitHub {
        let mut held = Vec::new();
        let listener = loop {
            let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
            let port = l.local_addr().unwrap().port().to_string();
            if port.contains("301") {
                held.push(l);
                assert!(held.len() < 200, "cannot find a usable port");
                continue;
            }
            break l;
        };
        drop(held);
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(State {
            routes: Mutex::new(HashMap::new()),
            rates: Mutex::new(HashMap::new()),
            default_rate: Mutex::new(RateState {
                limit: 5000,
                remaining: 4999,
                reset: now_unix() + 3600,
            }),
            count_points: Mutex::new(false),
            log: Mutex::new(Vec::new()),
        });
        let st = state.clone();
        let handler: Handler = Arc::new(move |req: &Request| st.handle(req));
        thread::spawn(move || {
            let _ = http::serve(listener, handler);
        });
        FakeGitHub {
            base_url: format!("http://127.0.0.1:{port}/"),
            state,
        }
    }

    /// `http://127.0.0.1:<port>/` — the value for `GHA2DB_GITHUB_API_URL`.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Script `method path`. `path` may carry a `?query`: an exact raw-query
    /// match wins, then the route whose (percent-decoded) parameters are all
    /// present in the request (most parameters first), then the query-less
    /// route. Replaces an earlier script of the same key.
    pub fn route(&self, method: &str, path: &str, responses: Vec<Scripted>) {
        lock(&self.state.routes).insert(format!("{method} {path}"), (responses, 0));
    }

    /// `route("GET", path, …)`.
    pub fn get(&self, path: &str, responses: Vec<Scripted>) {
        self.route("GET", path, responses);
    }

    /// `route("GET", path, vec![Scripted::ok(body)])`.
    pub fn get_ok(&self, path: &str, body: &Value) {
        self.get(path, vec![Scripted::ok(body)]);
    }

    /// Rate state used for tokens not set explicitly (and anonymous access).
    pub fn set_default_rate(&self, limit: i64, remaining: i64, reset_in: i64) {
        *lock(&self.state.default_rate) = RateState {
            limit,
            remaining,
            reset: now_unix() + reset_in,
        };
    }

    /// Rate state of one token (`None` = anonymous).
    pub fn set_rate(&self, token: Option<&str>, limit: i64, remaining: i64, reset_in: i64) {
        lock(&self.state.rates).insert(
            token.map(|t| t.to_string()),
            RateState {
                limit,
                remaining,
                reset: now_unix() + reset_in,
            },
        );
    }

    /// Count every non-`rate_limit` request against the token's `remaining`.
    pub fn count_points(&self, on: bool) {
        *lock(&self.state.count_points) = on;
    }

    /// Everything received so far, in arrival order.
    pub fn requests(&self) -> Vec<Logged> {
        lock(&self.state.log).clone()
    }

    /// Sorted request summaries (the binaries under test are multi-threaded,
    /// so the arrival order is not comparable).
    pub fn summaries(&self) -> Vec<String> {
        let mut v: Vec<String> = self.requests().iter().map(Logged::summary).collect();
        v.sort();
        v
    }

    /// Forget the logged requests (routes and rate states stay).
    pub fn clear_log(&self) {
        lock(&self.state.log).clear();
    }

    /// Forget the logged requests and rewind every route to its first
    /// response, so the same server can serve the Go and the Rust run.
    pub fn rewind(&self) {
        self.clear_log();
        for (_, idx) in lock(&self.state.routes).values_mut() {
            *idx = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_queries() {
        assert_eq!(percent_decode("a%3Ab+c%zz"), "a:b c%zz");
        let q = decode_query("since=2026-07-01T00%3A00%3A00Z&per_page=100&x");
        assert_eq!(q.get("since").unwrap(), "2026-07-01T00:00:00Z");
        assert_eq!(q.get("per_page").unwrap(), "100");
        assert_eq!(q.get("x").unwrap(), "");
    }

    #[test]
    fn paged_link_header() {
        let s = Scripted::ok(&json!([])).paged("/repos/o/r/forks?per_page=100", 1, 3);
        let link = s.headers.iter().find(|(k, _)| k == "Link").unwrap();
        assert_eq!(
            link.1,
            "</repos/o/r/forks?per_page=100&page=2>; rel=\"next\", </repos/o/r/forks?per_page=100&page=3>; rel=\"last\""
        );
        let s = Scripted::ok(&json!([])).paged("/x", 2, 3);
        let link = s.headers.iter().find(|(k, _)| k == "Link").unwrap();
        assert_eq!(
            link.1,
            "</x?page=3>; rel=\"next\", </x?page=3>; rel=\"last\", </x?page=1>; rel=\"prev\", </x?page=1>; rel=\"first\""
        );
        let s = Scripted::ok(&json!([])).paged("/x", 1, 1);
        assert!(s.headers.iter().all(|(k, _)| k != "Link"));
    }

    #[test]
    fn subset_routes_prefer_most_specific() {
        let gh = FakeGitHub::start();
        gh.get("/repos/o/r/comments", vec![Scripted::ok(&json!(["plain"]))]);
        gh.get(
            "/repos/o/r/comments?sort=updated",
            vec![Scripted::ok(&json!(["sorted"]))],
        );
        gh.get(
            "/repos/o/r/comments?sort=updated&since=2026-07-01T00:00:00Z",
            vec![Scripted::ok(&json!(["sorted-since"]))],
        );
        let body = |q: &str| {
            let url = format!("{}repos/o/r/comments{}", gh.base_url(), q);
            let mut r = ureq::get(&url).call().unwrap();
            r.body_mut().read_to_string().unwrap()
        };
        assert_eq!(body(""), "[\"plain\"]");
        assert_eq!(body("?per_page=100&sort=updated&page=1"), "[\"sorted\"]");
        assert_eq!(
            body("?per_page=100&since=2026-07-01T00%3A00%3A00Z&sort=updated"),
            "[\"sorted-since\"]"
        );
        assert_eq!(body("?sort=created"), "[\"plain\"]");
        let sums = gh.summaries();
        assert_eq!(sums.len(), 4);
        assert!(sums
            .iter()
            .any(|s| s.starts_with("GET /repos/o/r/comments?page=1&per_page=100&sort=updated ")));
    }
}
