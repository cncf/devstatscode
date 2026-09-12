//! A scripted stand-in for `data.gharchive.org`, for the Go ⇄ Rust tests of
//! `gha2db`, which is pointed at it with `GHA2DB_GHARCHIVE_URL`.
//!
//! Every hour (`YYYY-MM-DD-H`) serves a sequence of scripted answers — the
//! last one sticks — so the retry paths can be exercised: a gzipped set of
//! JSON lines, a raw body (a 404 HTML page is what the real archive returns
//! for a missing hour — `gzip: invalid header` for the tools; an empty body
//! is `EOF`; a truncated gzip stream fails while reading), or dropping the
//! connection. Unknown hours get the archive's 404 page. Every request path
//! is logged so a test can assert that both binaries fetched the same hours
//! (and retried the same number of times).

use std::collections::HashMap;
use std::io::Write;
use std::net::TcpListener;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use devstatscode::http::{self, Handler, Request, Response};

/// One scripted answer.
#[derive(Debug, Clone)]
pub struct Archive {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
    /// Drop the connection without answering (a transport error for the
    /// client). Logged like any other request.
    pub hangup: bool,
}

/// gzip `data` (one member, default compression) — what the archive serves.
pub fn gzip(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(data).expect("gzip write");
    enc.finish().expect("gzip finish")
}

/// The archive's hour file: one JSON document per line, gzipped.
pub fn gzip_lines<S: AsRef<str>>(lines: &[S]) -> Vec<u8> {
    let mut data = Vec::new();
    for l in lines {
        data.extend_from_slice(l.as_ref().as_bytes());
        data.push(b'\n');
    }
    gzip(&data)
}

/// The archive's answer for a missing hour (S3 `NoSuchKey`).
pub fn not_found_body(key: &str) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code>\
         <Message>The specified key does not exist.</Message><Key>{key}</Key>\
         <RequestId>0000000000000000</RequestId><HostId>fake</HostId></Error>"
    )
    .into_bytes()
}

impl Archive {
    /// `200 OK` with an already gzipped body.
    pub fn gz(body: Vec<u8>) -> Archive {
        Archive {
            status: 200,
            content_type: "application/gzip".to_string(),
            body,
            hangup: false,
        }
    }

    /// `200 OK` serving these JSON lines gzipped.
    pub fn lines<S: AsRef<str>>(lines: &[S]) -> Archive {
        Archive::gz(gzip_lines(lines))
    }

    /// A raw (not gzipped) body with the given status.
    pub fn raw(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Archive {
        Archive {
            status,
            content_type: content_type.to_string(),
            body: body.into(),
            hangup: false,
        }
    }

    /// The archive's `404` for a missing hour.
    pub fn not_found(key: &str) -> Archive {
        Archive::raw(404, "application/xml", not_found_body(key))
    }

    /// `200 OK` with an empty body (`EOF` for the gzip reader).
    pub fn empty() -> Archive {
        Archive::raw(200, "application/gzip", Vec::new())
    }

    /// `200 OK` with the gzip stream of `lines` cut after `keep` bytes
    /// (the gzip header opens fine, reading the data fails).
    pub fn truncated<S: AsRef<str>>(lines: &[S], keep: usize) -> Archive {
        let mut body = gzip_lines(lines);
        body.truncate(keep);
        Archive::gz(body)
    }

    /// Drop the connection.
    pub fn hangup() -> Archive {
        Archive {
            status: 0,
            content_type: String::new(),
            body: Vec::new(),
            hangup: true,
        }
    }
}

struct State {
    /// `YYYY-MM-DD-H` → (answers, next index).
    hours: Mutex<HashMap<String, (Vec<Archive>, usize)>>,
    /// Request paths in arrival order (`/2015-01-01-15.json.gz`).
    log: Mutex<Vec<String>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl State {
    fn handle(&self, req: &Request) -> Response {
        lock(&self.log).push(req.path.clone());
        let key = req
            .path
            .strip_prefix('/')
            .unwrap_or(&req.path)
            .strip_suffix(".json.gz")
            .map(|s| s.to_string());
        let scripted = key.as_ref().and_then(|k| {
            let mut hours = lock(&self.hours);
            hours.get_mut(k).map(|(answers, idx)| {
                let a = answers[(*idx).min(answers.len() - 1)].clone();
                *idx += 1;
                a
            })
        });
        let scripted = match scripted {
            Some(s) => s,
            None => Archive::not_found(req.path.strip_prefix('/').unwrap_or(&req.path)),
        };
        if scripted.hangup {
            // Unwind through the server's `catch_unwind`, which drops the
            // connection (no panic hook output).
            std::panic::resume_unwind(Box::new("hangup"));
        }
        let mut resp = Response::new(scripted.status);
        resp.set_header("Content-Type", &scripted.content_type);
        resp.set_header("Server", "AmazonS3");
        resp.body = scripted.body;
        resp
    }
}

/// A running fake GH Archive server (lives until the test process exits).
pub struct FakeGHArchive {
    base_url: String,
    state: Arc<State>,
}

impl std::fmt::Debug for FakeGHArchive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeGHArchive")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl FakeGHArchive {
    /// Listen on an ephemeral `127.0.0.1` port.
    pub fn start() -> FakeGHArchive {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(State {
            hours: Mutex::new(HashMap::new()),
            log: Mutex::new(Vec::new()),
        });
        let st = state.clone();
        let handler: Handler = Arc::new(move |req: &Request| st.handle(req));
        thread::spawn(move || {
            let _ = http::serve(listener, handler);
        });
        FakeGHArchive {
            base_url: format!("http://127.0.0.1:{port}/"),
            state,
        }
    }

    /// `http://127.0.0.1:<port>/` — the value for `GHA2DB_GHARCHIVE_URL`.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Script the answers of hour `YYYY-MM-DD-H` (the last one sticks).
    pub fn hour(&self, key: &str, answers: Vec<Archive>) {
        assert!(!answers.is_empty(), "at least one answer for {key}");
        lock(&self.state.hours).insert(key.to_string(), (answers, 0));
    }

    /// Script hour `key` to serve these JSON lines.
    pub fn hour_lines<S: AsRef<str>>(&self, key: &str, lines: &[S]) {
        self.hour(key, vec![Archive::lines(lines)]);
    }

    /// Request paths received so far, in arrival order.
    pub fn requests(&self) -> Vec<String> {
        lock(&self.state.log).clone()
    }

    /// Sorted request paths (the binary under test fetches hours from
    /// several threads).
    pub fn sorted_requests(&self) -> Vec<String> {
        let mut v = self.requests();
        v.sort();
        v
    }

    /// Forget the requests logged so far (between the Go and the Rust run).
    pub fn clear_requests(&self) {
        lock(&self.state.log).clear();
    }

    /// Rewind every scripted hour to its first answer (between the Go and
    /// the Rust run).
    pub fn rewind(&self) {
        for (_, idx) in lock(&self.state.hours).values_mut() {
            *idx = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn serves_scripted_hours_and_404s() {
        let srv = FakeGHArchive::start();
        srv.hour_lines("2015-01-01-15", &["{\"a\":1}", "{\"b\":2}"]);
        srv.hour(
            "2015-01-01-16",
            vec![Archive::empty(), Archive::lines(&["{\"c\":3}"])],
        );
        let get = |key: &str| {
            let mut r = ureq::get(&format!("{}{key}.json.gz", srv.base_url()))
                .config()
                .http_status_as_error(false)
                .build()
                .call()
                .unwrap();
            let status = r.status().as_u16();
            let mut body = Vec::new();
            r.body_mut().as_reader().read_to_end(&mut body).unwrap();
            (status, body)
        };
        let (st, body) = get("2015-01-01-15");
        assert_eq!(st, 200);
        let mut out = String::new();
        flate2::read::GzDecoder::new(&body[..])
            .read_to_string(&mut out)
            .unwrap();
        assert_eq!(out, "{\"a\":1}\n{\"b\":2}\n");
        let (st, body) = get("2015-01-01-16");
        assert_eq!((st, body.len()), (200, 0));
        let (st, body) = get("2015-01-01-16");
        assert_eq!(st, 200);
        assert!(body.len() > 10);
        let (st, body) = get("2015-01-01-16");
        assert_eq!(st, 200);
        assert!(body.len() > 10, "the last answer sticks");
        let (st, body) = get("2015-01-01-17");
        assert_eq!(st, 404);
        assert!(String::from_utf8_lossy(&body).contains("NoSuchKey"));
        assert_eq!(
            srv.requests(),
            vec![
                "/2015-01-01-15.json.gz",
                "/2015-01-01-16.json.gz",
                "/2015-01-01-16.json.gz",
                "/2015-01-01-16.json.gz",
                "/2015-01-01-17.json.gz"
            ]
        );
        srv.rewind();
        srv.clear_requests();
        let (_, body) = get("2015-01-01-16");
        assert_eq!(body.len(), 0, "rewound to the first answer");
        assert_eq!(srv.requests(), vec!["/2015-01-01-16.json.gz"]);
    }
}
