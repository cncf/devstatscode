//! Go ⇄ Rust compatibility tests for `webhook`.
//!
//! Every case starts the Go server and the Rust server (one after the other,
//! on a free port each) with its own scratch "project root" and a fake `git`
//! / `make` / `./devel/deploy_all.sh` on `PATH` that record how they were
//! called (arguments, working directory, `FROM_WEBHOOK`, whether
//! `/tmp/webhook.pid` names the server) and fail or misbehave on demand. Raw
//! HTTP requests are sent over TCP and compared byte for byte (the `Date`
//! header masked); also compared: the server's stdout (remote address, time
//! stamps, port, scratch paths and durations masked), the interesting stderr
//! lines, the recorded command invocations and the absence of the PID file
//! afterwards.
//!
//! The cases run serially (`/tmp/webhook.pid` is process-global).

use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use devstats_compat::pg::PG_ENV_VARS;
use devstats_compat::{
    go_binary, is_go_duration, run, rust_binary, Invocation, LIB_ENV_PREFIX, TOOL_ENV_VARS,
};
use tempfile::TempDir;

static SERIAL: Mutex<()> = Mutex::new(());

const PID_FILE: &str = "/tmp/webhook.pid";

fn go_bin() -> Option<PathBuf> {
    go_binary("webhook")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_webhook"))
}

/// The fake command: records the call, then fails (`G2R_FAIL=<name-args>`),
/// removes the PID file (`G2R_RM_PID=<name-args>`) or succeeds.
const SCRIPT: &str = r#"#!/bin/sh
PATH=/bin:/usr/bin:/usr/local/bin; export PATH
name="$(basename "$0")"
key="$name"
for a in "$@"; do key="$key-$a"; done
{
  printf '%s' "$name"
  for a in "$@"; do printf ' [%s]' "$a"; done
  printf ' cwd=%s' "$(pwd -P)"
  printf ' FROM_WEBHOOK=%s' "${FROM_WEBHOOK-unset}"
  printf ' PG_PASS=%s' "${PG_PASS:+set}"
  if [ -f /tmp/webhook.pid ]; then
    if [ "$(cat /tmp/webhook.pid)" = "$PPID" ]; then printf ' pidfile=parent'; else printf ' pidfile=other'; fi
  else
    printf ' pidfile=none'
  fi
  printf '\n'
} >> "$G2R_RECORD"
if [ "$key" = "$G2R_FAIL" ]; then
  echo "fake $key: failing (stdout)"
  echo "fake $key: failing (stderr)" >&2
  exit 3
fi
if [ "$key" = "$G2R_RM_PID" ]; then
  rm -f /tmp/webhook.pid
fi
exit 0
"#;

fn write_exec(dir: &Path, name: &str, content: &str) {
    let p = dir.join(name);
    fs::write(&p, content).unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
}

/// How to start a server.
#[derive(Clone, Default)]
struct Config {
    env: Vec<(String, String)>,
    /// Literal `GHA2DB_PROJECT_ROOT` (default: the scratch `root` directory).
    project_root: Option<String>,
    /// Do not provide a fake `make` (and put only the fake dir on `PATH`).
    no_make: bool,
    /// Do not set `GHA2DB_SKIP_VERIFY_PAYLOAD` (Travis signature check mode).
    verify: bool,
}

impl Config {
    fn env(mut self, k: &str, v: &str) -> Self {
        self.env.push((k.to_string(), v.to_string()));
        self
    }
}

struct Server {
    child: Child,
    port: u16,
    _dir: TempDir,
    root: PathBuf,
    record: PathBuf,
    out: PathBuf,
    err: PathBuf,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn(bin: &Path, cfg: &Config, tag: &str, port: u16) -> Server {
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_webhook_{tag}_"))
        .tempdir()
        .unwrap();
    let root = dir.path().join("root");
    fs::create_dir_all(root.join("devel")).unwrap();
    let fakes = dir.path().join("bin");
    fs::create_dir_all(&fakes).unwrap();
    write_exec(&fakes, "git", SCRIPT);
    if !cfg.no_make {
        write_exec(&fakes, "make", SCRIPT);
    }
    write_exec(&root.join("devel"), "deploy_all.sh", SCRIPT);
    let record = dir.path().join("record.txt");
    fs::write(&record, "").unwrap();
    let out = dir.path().join("stdout.txt");
    let err = dir.path().join("stderr.txt");

    let mut cmd = Command::new(bin);
    for v in TOOL_ENV_VARS {
        cmd.env_remove(v);
    }
    for v in PG_ENV_VARS {
        cmd.env_remove(v);
    }
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with(LIB_ENV_PREFIX) {
            cmd.env_remove(&k);
        }
    }
    let path = if cfg.no_make {
        fakes.display().to_string()
    } else {
        format!(
            "{}:{}",
            fakes.display(),
            std::env::var("PATH").unwrap_or_default()
        )
    };
    cmd.env("PATH", path)
        .env("G2R_RECORD", &record)
        .env("NO_FATAL_DELAY", "1")
        .env("GHA2DB_SKIPLOG", "1")
        .env("GHA2DB_SKIPTIME", "1")
        .env("GHA2DB_WHPORT", format!(":{port}"))
        .env(
            "GHA2DB_PROJECT_ROOT",
            cfg.project_root
                .clone()
                .unwrap_or_else(|| root.display().to_string()),
        );
    if !cfg.verify {
        cmd.env("GHA2DB_SKIP_VERIFY_PAYLOAD", "1");
    }
    for (k, v) in &cfg.env {
        cmd.env(k, v);
    }
    cmd.current_dir(dir.path())
        .stdin(Stdio::null())
        .stdout(File::create(&out).unwrap())
        .stderr(File::create(&err).unwrap());
    let child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("cannot spawn {}: {e}", bin.display()));
    Server {
        child,
        port,
        _dir: dir,
        root,
        record,
        out,
        err,
    }
}

/// Start `bin` on a free port and wait until it accepts connections.
fn start(bin: &Path, cfg: &Config, tag: &str) -> Server {
    let mut srv = spawn(bin, cfg, tag, free_port());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", srv.port)).is_ok() {
            return srv;
        }
        if let Some(status) = srv.child.try_wait().unwrap() {
            panic!(
                "{} exited early with {status}: stdout={:?} stderr={:?}",
                bin.display(),
                fs::read_to_string(&srv.out).unwrap_or_default(),
                fs::read_to_string(&srv.err).unwrap_or_default()
            );
        }
        assert!(Instant::now() < deadline, "{} did not start", bin.display());
        thread::sleep(Duration::from_millis(20));
    }
}

/// What a case produced on one side.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Captured {
    responses: Vec<String>,
    stdout: String,
    stderr: String,
    record: String,
    pid_file_left: bool,
}

impl Server {
    /// Parse complete HTTP responses out of `buf`; `head` marks a HEAD request
    /// (responses without body). Returns how many complete non-1xx responses
    /// the buffer holds.
    fn complete_responses(buf: &[u8], head: bool) -> usize {
        let mut pos = 0;
        let mut n = 0;
        loop {
            let rest = &buf[pos..];
            let Some(hdr_end) = rest.windows(4).position(|w| w == b"\r\n\r\n") else {
                return n;
            };
            let header = String::from_utf8_lossy(&rest[..hdr_end]).into_owned();
            let mut lines = header.split("\r\n");
            let status: u16 = lines
                .next()
                .and_then(|l| l.split(' ').nth(1))
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            let content_length: usize = lines
                .filter_map(|l| l.split_once(':'))
                .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, v)| v.trim().parse().ok())
                .unwrap_or(0);
            pos += hdr_end + 4;
            if (100..200).contains(&status) {
                continue;
            }
            let body_len = if head { 0 } else { content_length };
            if buf.len() - pos < body_len {
                return n;
            }
            pos += body_len;
            n += 1;
        }
    }

    /// Send one raw request and read `expect` responses (or up to EOF).
    fn request_n(&self, raw: &[u8], expect: usize) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
        s.write_all(raw).unwrap();
        let head = raw.starts_with(b"HEAD ");
        let mut buf = Vec::new();
        let mut chunk = [0u8; 65536];
        loop {
            if Self::complete_responses(&buf, head) >= expect {
                break;
            }
            match s.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e) => panic!("read from webhook: {e}"),
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn request(&self, raw: &[u8]) -> String {
        self.request_n(raw, 1)
    }

    /// Kill the server and collect everything it left behind.
    fn stop(mut self, responses: Vec<String>) -> Captured {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let pid_file_left = Path::new(PID_FILE).exists();
        let _ = fs::remove_file(PID_FILE);
        // lossy: jsoniter puts a raw 0xFF byte into some error messages
        let lossy =
            |p: &Path| String::from_utf8_lossy(&fs::read(p).unwrap_or_default()).into_owned();
        Captured {
            responses,
            stdout: lossy(&self.out),
            stderr: lossy(&self.err),
            record: lossy(&self.record),
            pid_file_left,
        }
    }
}

/// Go `url.QueryEscape`.
fn query_escape(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// A Travis payload JSON.
#[derive(Clone)]
struct Payload {
    repo: &'static str,
    owner: &'static str,
    branch: &'static str,
    status: &'static str,
    result: i64,
    typ: &'static str,
    message: String,
}

impl Payload {
    fn good() -> Self {
        Payload {
            repo: "devstats",
            owner: "cncf",
            branch: "master",
            status: "Passed",
            result: 0,
            typ: "push",
            message: "Fix the thing".to_string(),
        }
    }

    fn json(&self) -> String {
        format!(
            r#"{{"id":1,"number":"42","status":null,"result":{},"result_message":"{}","type":"{}","branch":"{}","author_email":"dev@example.com","author_name":"Dev Eloper","message":{},"repository":{{"id":7,"name":"{}","owner_name":"{}","url":null}},"matrix":[{{"id":2}}]}}"#,
            self.result,
            self.status,
            self.typ,
            self.branch,
            serde_json::to_string(&self.message).unwrap(),
            self.repo,
            self.owner
        )
    }
}

/// The `payload=<escaped json>` form body Travis posts.
fn form_body(json: &str) -> Vec<u8> {
    format!("payload={}", query_escape(json)).into_bytes()
}

/// A `POST /hook` with the given body and extra headers, on a closing connection.
fn post_to(path: &str, body: &[u8], extra_headers: &str) -> Vec<u8> {
    let mut req = format!(
        "POST {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/x-www-form-urlencoded\r\n{}Content-Length: {}\r\n\r\n",
        path,
        extra_headers,
        body.len()
    )
    .into_bytes();
    req.extend_from_slice(body);
    req
}

fn post(body: &[u8]) -> Vec<u8> {
    post_to("/hook", body, "")
}

fn post_payload(p: &Payload) -> Vec<u8> {
    post(&form_body(&p.json()))
}

/// Mask what legitimately differs between two runs/sides.
fn normalize(c: &Captured, root: &Path, port: u16) -> Captured {
    let root_s = root.display().to_string();
    let responses = c
        .responses
        .iter()
        .map(|r| {
            let mut out = String::new();
            for (i, line) in r.split("\r\n").enumerate() {
                if i > 0 {
                    out.push_str("\r\n");
                }
                if line.starts_with("Date: ") {
                    out.push_str("Date: <date>");
                } else {
                    out.push_str(line);
                }
            }
            out.replace(&root_s, "<root>")
        })
        .collect();
    let mut stdout_lines = Vec::new();
    for line in c.stdout.split('\n') {
        let line = line.replace(&root_s, "<root>");
        let masked = if line.starts_with("WebHook processing event ") {
            "WebHook processing event <addr> at <time>".to_string()
        } else if let Some(pos) = line.find(" in ") {
            if line.starts_with("WebHook: deployed via ") {
                let dur = &line[pos + 4..];
                assert!(is_go_duration(dur), "not a Go duration: {line:?}");
                format!("{} in <duration>", &line[..pos])
            } else {
                line.clone()
            }
        } else if line.starts_with("Another `webhook` instance was running, waited ") {
            "Another `webhook` instance was running, waited <n> seconds".to_string()
        } else {
            line.replace(&format!("Port::{port} "), "Port::<port> ")
        };
        stdout_lines.push(masked);
    }
    let stdout = stdout_lines.join("\n");
    // stderr: keep the lines both implementations define (Go adds
    // `ErrorType:` lines and goroutine dumps, Rust `thread … panicked` lines).
    let mut stderr = String::new();
    for line in c.stderr.split('\n') {
        let line = line.replace(&root_s, "<root>");
        let masked = if line.starts_with("Error(time=") {
            "Error(time=<time>):".to_string()
        } else if let Some(pos) = line.find(" http: panic serving 127.0.0.1:") {
            let rest = &line[pos + " http: panic serving 127.0.0.1:".len()..];
            let msg = rest.split_once(": ").map(|(_, m)| m).unwrap_or("");
            format!("http: panic serving <addr>: {msg}")
        } else {
            line.clone()
        };
        if masked.starts_with("webhook: ")
            || masked.starts_with("Environment variable ")
            || masked.starts_with("Error(time=")
            || masked.starts_with("Error: '")
            || masked.starts_with("http: panic serving")
            || masked.starts_with("panic: stacktrace: ")
        {
            stderr.push_str(&masked.replace(&format!("127.0.0.1:{port}"), "127.0.0.1:<port>"));
            stderr.push('\n');
        }
    }
    Captured {
        responses,
        stdout,
        stderr,
        record: c.record.replace(&root_s, "<root>"),
        pid_file_left: c.pid_file_left,
    }
}

/// One scripted interaction with a running server.
enum Step {
    /// Send the request, read one response.
    Req(Vec<u8>),
    /// Send the request, read `n` responses (keep-alive).
    ReqN(Vec<u8>, usize),
    /// Create `/tmp/webhook.pid` (as another instance would) and remove it
    /// again after the delay, in the background.
    PidFileFor(Duration),
}

fn drive(srv: Server, steps: &[Step]) -> Captured {
    let mut responses = Vec::new();
    for step in steps {
        match step {
            Step::Req(raw) => responses.push(srv.request(raw)),
            Step::ReqN(raw, n) => responses.push(srv.request_n(raw, *n)),
            Step::PidFileFor(delay) => {
                fs::write(PID_FILE, "424242").unwrap();
                let delay = *delay;
                thread::spawn(move || {
                    thread::sleep(delay);
                    let _ = fs::remove_file(PID_FILE);
                });
            }
        }
    }
    srv.stop(responses)
}

/// Run the case on both sides and compare; returns the (normalized) Rust capture.
fn run_case(name: &str, cfg: &Config, steps: &[Step]) -> Captured {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let _ = fs::remove_file(PID_FILE);
    let go = go_bin().map(|bin| {
        let srv = start(&bin, cfg, &format!("{name}_go"));
        let (root, port) = (srv.root.clone(), srv.port);
        normalize(&drive(srv, steps), &root, port)
    });
    let srv = start(&rust_bin(), cfg, &format!("{name}_rs"));
    let (root, port) = (srv.root.clone(), srv.port);
    let rs = normalize(&drive(srv, steps), &root, port);
    if let Some(go) = go {
        assert_eq!(go.responses, rs.responses, "{name}: responses");
        assert_eq!(go.stdout, rs.stdout, "{name}: stdout");
        assert_eq!(go.stderr, rs.stderr, "{name}: stderr");
        assert_eq!(go.record, rs.record, "{name}: recorded commands");
        assert_eq!(go.pid_file_left, rs.pid_file_left, "{name}: PID file");
    }
    rs
}

fn status_line(resp: &str) -> &str {
    resp.split("\r\n").next().unwrap_or("")
}

fn body(resp: &str) -> &str {
    resp.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("")
}

// ---------------------------------------------------------------------------

#[test]
fn deploys_devstats_on_a_passed_master_push() {
    let mut p = Payload::good();
    p.message = "Fix the thing (ünïcödé ✓) & more".to_string();
    let c = run_case(
        "devstats_ok",
        &Config::default(),
        &[Step::Req(post_payload(&p))],
    );
    let r = &c.responses[0];
    assert_eq!(status_line(r), "HTTP/1.1 200 OK");
    assert!(r.contains("\r\nContent-Type: application/json\r\n"), "{r}");
    assert_eq!(body(r), "{\"message\": \"ok\"}");
    assert_eq!(
        c.record,
        "git [checkout] [master] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n\
         git [pull] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n\
         make cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n\
         make [install] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n"
    );
    assert!(!c.pid_file_left);
    assert_eq!(
        c.stdout,
        "Compiled None, commit: None on None using None\n\
         WebHook processing event <addr> at <time>\n\
         WebHook config is Host:127.0.0.1 Port::<port> Root:/hook\n\
         WebHook: repo: cncf/devstats, allowed: cncf/devstats, cncf/devstatscode\n\
         WebHook: branch: master, allowed branches: [master]\n\
         WebHook: status: Passed, allowed statuses: [Passed Fixed]\n\
         WebHook: type: push, allowed types: [push]\n\
         WebHook: result: 0, allowed results: [0]\n\
         WebHook: author: name: Dev Eloper, email: dev@example.com\n\
         WebHook: message: Fix the thing (ünïcödé ✓) & more\n\
         WebHook: deploying via `make install`\n\
         WebHook: git checkout master\n\
         WebHook: git pull\n\
         WebHook: make\n\
         WebHook: make install\n\
         WebHook: deployed via 'make install' in <duration>\n"
    );
    assert_eq!(c.stderr, "");
}

#[test]
fn devstatscode_skips_make_and_port_without_colon_works() {
    let mut p = Payload::good();
    p.repo = "devstatscode";
    p.status = "Fixed";
    // GHA2DB_WHPORT without the leading colon gets one prepended.
    let port = free_port();
    let cfg = Config::default().env("GHA2DB_WHPORT", &port.to_string());
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let _ = fs::remove_file(PID_FILE);
    let both = |bin: &Path, tag: &str| {
        let mut srv = spawn(bin, &cfg, tag, port);
        srv.port = port;
        let deadline = Instant::now() + Duration::from_secs(30);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "{} did not start", bin.display());
            thread::sleep(Duration::from_millis(20));
        }
        let root = srv.root.clone();
        normalize(&drive(srv, &[Step::Req(post_payload(&p))]), &root, port)
    };
    let go = go_bin().map(|bin| both(&bin, "code_go"));
    let rs = both(&rust_bin(), "code_rs");
    if let Some(go) = go {
        assert_eq!(go, rs);
    }
    assert_eq!(status_line(&rs.responses[0]), "HTTP/1.1 200 OK");
    assert_eq!(
        rs.record,
        "git [checkout] [master] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n\
         git [pull] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n\
         make [install] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n"
    );
    assert!(rs
        .stdout
        .contains("WebHook config is Host:127.0.0.1 Port::<port> Root:/hook\n"));
    assert!(rs
        .stdout
        .contains("WebHook: status: Fixed, allowed statuses: [Passed Fixed]\n"));
    assert!(!rs.stdout.contains("WebHook: make\n"));
}

#[test]
fn full_deploy_runs_deploy_all_with_from_webhook() {
    let mut p = Payload::good();
    p.message = "Rebuild everything [deploy]".to_string();
    let cfg = Config::default().env("PG_PASS", "secret");
    let c = run_case("full_deploy", &cfg, &[Step::Req(post_payload(&p))]);
    assert_eq!(body(&c.responses[0]), "{\"message\": \"ok\"}");
    assert_eq!(
        c.record,
        "git [checkout] [master] cwd=<root> FROM_WEBHOOK=unset PG_PASS=set pidfile=parent\n\
         git [pull] cwd=<root> FROM_WEBHOOK=unset PG_PASS=set pidfile=parent\n\
         make cwd=<root> FROM_WEBHOOK=unset PG_PASS=set pidfile=parent\n\
         make [install] cwd=<root> FROM_WEBHOOK=unset PG_PASS=set pidfile=parent\n\
         deploy_all.sh cwd=<root> FROM_WEBHOOK=1 PG_PASS=set pidfile=parent\n"
    );
    assert!(c.stdout.contains(
        "WebHook: make install\n\
         WebHook: ./devel/deploy_all.sh\n\
         WebHook: ./devel/deploy_all.sh succeeded\n\
         WebHook: deployed via 'make install', './devel/deploy_all.sh' in <duration>\n"
    ));
}

#[test]
fn full_deploy_without_pg_pass_is_refused_after_make_install() {
    let mut p = Payload::good();
    p.message = "[deploy] now".to_string();
    let c = run_case(
        "deploy_no_pg_pass",
        &Config::default(),
        &[Step::Req(post_payload(&p))],
    );
    let r = &c.responses[0];
    assert_eq!(status_line(r), "HTTP/1.1 401 Unauthorized");
    // Bug 25: the newline must be JSON-escaped.
    assert_eq!(
        body(r),
        "{\"message\": \"webhook: Environment variable 'PG_PASS' must be set in [deploy] mode\\n\"}"
    );
    let v: serde_json::Value = serde_json::from_str(body(r)).unwrap();
    assert!(v["message"].as_str().unwrap().ends_with("mode\n"));
    assert!(c.record.contains("make [install]"));
    assert!(!c.record.contains("deploy_all"));
    assert!(c.stdout.ends_with(
        "WebHook: make install\n\
         webhook: error: Environment variable 'PG_PASS' must be set in [deploy] mode\n\n"
    ));
    assert_eq!(
        c.stderr,
        "webhook: error: Environment variable 'PG_PASS' must be set in [deploy] mode\n"
    );
    assert!(!c.pid_file_left);
}

#[test]
fn skip_full_deploy_ignores_deploy_marker() {
    let mut p = Payload::good();
    p.message = "[deploy]".to_string();
    let cfg = Config::default().env("GHA2DB_SKIP_FULL_DEPLOY", "1");
    let c = run_case("skip_full_deploy", &cfg, &[Step::Req(post_payload(&p))]);
    assert_eq!(body(&c.responses[0]), "{\"message\": \"ok\"}");
    assert!(!c.record.contains("deploy_all"));
    assert!(c
        .stdout
        .contains("WebHook: deployed via 'make install' in <duration>\n"));
}

#[test]
fn payloads_that_must_not_deploy() {
    let mut steps = Vec::new();
    let mut variants: Vec<Payload> = Vec::new();
    let mut p = Payload::good();
    p.repo = "devstats-docker-images";
    variants.push(p);
    let mut p = Payload::good();
    p.owner = "lukaszgryglicki";
    variants.push(p);
    let mut p = Payload::good();
    p.message = "Docs [no deploy]".to_string();
    variants.push(p);
    let mut p = Payload::good();
    p.message = "[wip] half done [deploy]".to_string();
    variants.push(p);
    let mut p = Payload::good();
    p.status = "Failed";
    variants.push(p);
    let mut p = Payload::good();
    p.result = 1;
    variants.push(p);
    let mut p = Payload::good();
    p.typ = "pull_request";
    variants.push(p);
    let mut p = Payload::good();
    p.branch = "dev";
    variants.push(p);
    let mut p = Payload::good();
    p.status = "passed";
    variants.push(p);
    for v in &variants {
        steps.push(Step::Req(post_payload(v)));
    }
    // Empty object: everything is the zero value.
    steps.push(Step::Req(post(b"payload={}")));
    let c = run_case("skips", &Config::default(), &steps);
    assert_eq!(c.responses.len(), variants.len() + 1);
    for r in &c.responses {
        assert_eq!(status_line(r), "HTTP/1.1 401 Unauthorized");
        assert_eq!(
            body(r),
            "{\"message\": \"webhook: webhook: skipping deploy due to wrong status, result, branch, message and/or type\"}"
        );
    }
    assert_eq!(c.record, "");
    assert!(c.stdout.contains(
        "WebHook: repo: lukaszgryglicki/devstats, allowed: cncf/devstats, cncf/devstatscode\n"
    ));
    assert!(c
        .stdout
        .contains("WebHook: message: [wip] half done [deploy]\n"));
    assert!(c.stdout.contains("WebHook: repo: /, allowed: cncf/devstats, cncf/devstatscode\nWebHook: branch: , allowed branches: [master]\n"));
    assert!(!c.stdout.contains("webhook: warning"), "{}", c.stdout);
    assert_eq!(c.stderr, "");
}

#[test]
fn custom_deploy_lists_from_environment() {
    let mut p = Payload::good();
    p.branch = "dev";
    p.status = "Errored";
    p.result = 2;
    p.typ = "api";
    let cfg = Config::default()
        .env("GHA2DB_DEPLOY_BRANCHES", "master,dev,release-1.0")
        .env("GHA2DB_DEPLOY_STATUSES", "Passed,Errored")
        .env("GHA2DB_DEPLOY_RESULTS", "0,2,3")
        .env("GHA2DB_DEPLOY_TYPES", "push,api")
        .env("GHA2DB_WHROOT", "/travis/hook");
    let mut p2 = p.clone();
    p2.result = 1;
    let c = run_case(
        "custom_lists",
        &cfg,
        &[
            Step::Req(post_to("/travis/hook", &form_body(&p.json()), "")),
            Step::Req(post_to("/travis/hook", &form_body(&p2.json()), "")),
            Step::Req(post_payload(&p)),
        ],
    );
    assert_eq!(status_line(&c.responses[0]), "HTTP/1.1 200 OK");
    assert_eq!(status_line(&c.responses[1]), "HTTP/1.1 401 Unauthorized");
    assert_eq!(status_line(&c.responses[2]), "HTTP/1.1 404 Not Found");
    assert!(c.stdout.contains(
        "WebHook config is Host:127.0.0.1 Port::<port> Root:/travis/hook\n\
         WebHook: repo: cncf/devstats, allowed: cncf/devstats, cncf/devstatscode\n\
         WebHook: branch: dev, allowed branches: [master dev release-1.0]\n\
         WebHook: status: Errored, allowed statuses: [Passed Errored]\n\
         WebHook: type: api, allowed types: [push api]\n\
         WebHook: result: 2, allowed results: [0 2 3]\n"
    ));
    assert!(c.record.starts_with("git [checkout] [dev] cwd=<root>"));
}

#[test]
fn failing_git_checkout_reports_exit_status_and_output() {
    let cfg = Config::default().env("G2R_FAIL", "git-checkout-master");
    let c = run_case(
        "git_fails",
        &cfg,
        &[Step::Req(post_payload(&Payload::good()))],
    );
    let r = &c.responses[0];
    assert_eq!(status_line(r), "HTTP/1.1 401 Unauthorized");
    assert_eq!(body(r), "{\"message\": \"webhook: exit status 3\"}");
    assert_eq!(
        c.record,
        "git [checkout] [master] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n"
    );
    assert!(
        c.stdout.ends_with(
            "WebHook: deploying via `make install`\n\
         WebHook: git checkout master\n\
         fake git-checkout-master: failing (stdout)\n\n\
         STDERR:\n\
         fake git-checkout-master: failing (stderr)\n\n\
         Command, arguments, environment:\n\
         [git checkout master]\n\
         map[]\n\
         Command and arguments:\n\
         [git checkout master]\n\
         map[]\n\
         webhook: error: exit status 3\n"
        ),
        "{}",
        c.stdout
    );
    assert_eq!(c.stderr, "webhook: error: exit status 3\n");
    assert!(!c.pid_file_left);
}

#[test]
fn failing_deploy_all_shows_environment_override() {
    let mut p = Payload::good();
    p.message = "x [deploy]".to_string();
    let cfg = Config::default()
        .env("PG_PASS", "pw")
        .env("G2R_FAIL", "deploy_all.sh");
    let c = run_case("deploy_all_fails", &cfg, &[Step::Req(post_payload(&p))]);
    assert_eq!(
        body(&c.responses[0]),
        "{\"message\": \"webhook: exit status 3\"}"
    );
    assert!(c.stdout.contains(
        "Command, arguments, environment:\n\
         [./devel/deploy_all.sh]\n\
         map[FROM_WEBHOOK:1]\n"
    ));
    assert!(c
        .record
        .ends_with("deploy_all.sh cwd=<root> FROM_WEBHOOK=1 PG_PASS=set pidfile=parent\n"));
}

#[test]
fn missing_make_is_reported_with_json_escaped_quotes() {
    let cfg = Config {
        no_make: true,
        ..Config::default()
    };
    let c = run_case(
        "no_make",
        &cfg,
        &[Step::Req(post_payload(&Payload::good()))],
    );
    let r = &c.responses[0];
    assert_eq!(status_line(r), "HTTP/1.1 401 Unauthorized");
    // Bug 25: quotes in the message used to produce invalid JSON.
    assert_eq!(
        body(r),
        "{\"message\": \"webhook: exec: \\\"make\\\": executable file not found in $PATH\"}"
    );
    let v: serde_json::Value = serde_json::from_str(body(r)).unwrap();
    assert_eq!(
        v["message"],
        "webhook: exec: \"make\": executable file not found in $PATH"
    );
    assert!(
        c.stdout.ends_with(
            "WebHook: git pull\n\
         WebHook: make\n\
         Command, arguments, environment:\n\
         [make]\n\
         map[]\n\
         Command and arguments:\n\
         [make]\n\
         map[]\n\
         webhook: error: exec: \"make\": executable file not found in $PATH\n"
        ),
        "{}",
        c.stdout
    );
    assert!(!c.pid_file_left);
}

#[test]
fn nonexistent_project_root_fails_at_chdir() {
    let cfg = Config {
        project_root: Some("/nonexistent/devstats/root".to_string()),
        ..Config::default()
    };
    let c = run_case(
        "bad_root",
        &cfg,
        &[Step::Req(post_payload(&Payload::good()))],
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"message\": \"webhook: chdir /nonexistent/devstats/root: no such file or directory\"}"
    );
    assert_eq!(c.record, "");
    assert_eq!(
        c.stderr,
        "webhook: error: chdir /nonexistent/devstats/root: no such file or directory\n"
    );
}

#[test]
fn waits_for_another_instance_pid_file() {
    let c = run_case(
        "pid_wait",
        &Config::default(),
        &[
            Step::PidFileFor(Duration::from_millis(2500)),
            Step::Req(post_payload(&Payload::good())),
        ],
    );
    assert_eq!(body(&c.responses[0]), "{\"message\": \"ok\"}");
    assert!(
        c.stdout.contains(
            "WebHook: message: Fix the thing\n\
         Another `webhook` instance is running, PID file '/tmp/webhook.pid' exists, waiting\n\
         Another `webhook` instance was running, waited <n> seconds\n\
         WebHook: deploying via `make install`\n"
        ),
        "{}",
        c.stdout
    );
    assert!(c.record.contains("pidfile=parent"));
    assert!(!c.pid_file_left);
}

#[test]
fn pid_file_removed_during_deploy_is_a_recovered_panic() {
    let cfg = Config::default().env("G2R_RM_PID", "make-install");
    let c = run_case(
        "pid_removed",
        &cfg,
        &[
            Step::Req(post_payload(&Payload::good())),
            // the server survives and serves the next request
            Step::Req(post(b"payload={}")),
        ],
    );
    assert_eq!(c.responses[0], "", "connection closed without a response");
    assert_eq!(status_line(&c.responses[1]), "HTTP/1.1 401 Unauthorized");
    assert!(c
        .stdout
        .contains("WebHook: deployed via 'make install' in <duration>\n"));
    assert_eq!(
        c.stderr,
        "Error(time=<time>):\n\
         Error: 'remove /tmp/webhook.pid: no such file or directory'\n\
         http: panic serving <addr>: stacktrace: remove /tmp/webhook.pid: no such file or directory\n"
    );
    assert!(c
        .record
        .ends_with("make [install] cwd=<root> FROM_WEBHOOK=unset PG_PASS= pidfile=parent\n"));
    assert!(!c.pid_file_left);
}

#[test]
fn malformed_bodies() {
    let c = run_case(
        "malformed",
        &Config::default(),
        &[
            // bug 24: bodies shorter than "payload=" used to panic
            Step::Req(post(b"abc")),
            Step::Req(post(b"")),
            Step::Req(
                b"GET /hook HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
            ),
            Step::Req(post(b"payload")),
            Step::Req(post(b"payload=%zz")),
            Step::Req(post(b"payload=%7B%22branch%22%3A%22master%22%7D%2")),
            Step::Req(post(b"payload=%")),
        ],
    );
    let expect_exact = [
        (0, "webhook: payload too short"),
        (1, "webhook: payload too short"),
        (2, "webhook: payload too short"),
        (3, "webhook: payload too short"),
        (4, "webhook: invalid URL escape \\\"%zz\\\""),
        (5, "webhook: invalid URL escape \\\"%2\\\""),
        (6, "webhook: invalid URL escape \\\"%\\\""),
    ];
    for (i, msg) in expect_exact {
        let r = &c.responses[i];
        assert_eq!(status_line(r), "HTTP/1.1 401 Unauthorized", "#{i}");
        assert_eq!(body(r), format!("{{\"message\": \"{msg}\"}}"), "#{i}");
    }
    assert_eq!(c.responses.len(), expect_exact.len());
    assert_eq!(c.record, "");
    assert!(!c.pid_file_left);
}

/// The JSON decoding error paths compared with the wording masked.
#[test]
fn json_decoding_errors_are_401_on_both_sides() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let _ = fs::remove_file(PID_FILE);
    let bodies: [&[u8]; 10] = [
        // exactly 8 bytes: an empty JSON document (not `payload too short`)
        b"payload=",
        b"payload=[]",
        b"payload={\"branch\": 1}",
        b"payload={\"result\": \"0\"}",
        b"payload={\"result\": 1.5}",
        b"payload={\"repository\": \"x\"}",
        b"payload=nope",
        b"payload=not json",
        b"garbage=but+eight+bytes+or+more",
        b"payload={\"branch\": \"master\"",
    ];
    let side = |bin: &Path, tag: &str| {
        let srv = start(bin, &Config::default(), tag);
        let (root, port) = (srv.root.clone(), srv.port);
        let steps: Vec<Step> = bodies.iter().map(|b| Step::Req(post(b))).collect();
        let mut c = normalize(&drive(srv, &steps), &root, port);
        // mask the library-specific wording
        c.responses = c
            .responses
            .iter()
            .map(|r| {
                let (head, _) = r.split_once("\r\n\r\n").unwrap();
                let head: Vec<&str> = head
                    .split("\r\n")
                    .filter(|l| !l.starts_with("Content-Length: "))
                    .collect();
                head.join("\r\n")
            })
            .collect();
        c.stdout = c
            .stdout
            .lines()
            .map(|l| {
                if l.starts_with("webhook: error: ") {
                    "webhook: error: <json error>".to_string()
                } else {
                    l.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        c.stderr = c
            .stderr
            .lines()
            .map(|l| {
                if l.starts_with("webhook: error: ") {
                    "webhook: error: <json error>".to_string()
                } else {
                    l.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        c
    };
    let go = go_bin().map(|b| side(&b, "json_go"));
    let rs = side(&rust_bin(), "json_rs");
    if let Some(go) = go {
        assert_eq!(go, rs);
    }
    assert_eq!(rs.responses.len(), bodies.len());
    for r in &rs.responses {
        assert!(
            r.starts_with(
                "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nDate: <date>"
            ),
            "{r}"
        );
    }
    assert_eq!(
        rs.stderr
            .lines()
            .filter(|l| *l == "webhook: error: <json error>")
            .count(),
        bodies.len()
    );
}

#[test]
fn routing_and_wire_level_behaviour() {
    let skip = form_body(&Payload::good().json().replace("\"master\"", "\"other\""));
    let mut chunked = b"POST /hook HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    chunked.extend_from_slice(format!("{:x}\r\n", skip.len()).as_bytes());
    chunked.extend_from_slice(&skip);
    chunked.extend_from_slice(b"\r\n0\r\n\r\n");
    let mut http10 = format!(
        "POST /hook HTTP/1.0\r\nContent-Length: {}\r\n\r\n",
        skip.len()
    )
    .into_bytes();
    http10.extend_from_slice(&skip);
    let mut http10_keepalive = format!(
        "POST /hook HTTP/1.0\r\nConnection: keep-alive\r\nContent-Length: {}\r\n\r\n",
        skip.len()
    )
    .into_bytes();
    http10_keepalive.extend_from_slice(&skip);
    let mut two = Vec::new();
    two.extend_from_slice(&post_to("/hook", &skip, "").splice_connection_keepalive());
    two.extend_from_slice(&post_to("/hook", &skip, ""));
    let c = run_case(
        "wire",
        &Config::default(),
        &[
            Step::Req(
                b"GET /nope HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
            ),
            Step::Req(
                b"GET /hook/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
            ),
            Step::Req(
                b"GET //hook HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
            ),
            Step::Req(
                b"GET /a/../hook HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
            ),
            Step::Req(
                b"HEAD /nope HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
            ),
            Step::Req(
                b"HEAD /hook HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
            ),
            Step::Req(b"GET /hook HTTP/1.1\r\nConnection: close\r\n\r\n".to_vec()),
            Step::Req(b"BLAH\r\n\r\n".to_vec()),
            Step::Req(b"GET /hook HTTP/9.9\r\nHost: localhost\r\n\r\n".to_vec()),
            Step::Req(chunked),
            Step::Req(http10),
            Step::ReqN(http10_keepalive, 1),
            Step::Req(post_to("/hook", &skip, "Expect: 100-continue\r\n")),
            Step::ReqN(two, 2),
            Step::Req(post_to("/hook?x=1&y=2", &skip, "")),
            Step::Req(post_to("/HOOK", &skip, "")),
        ],
    );
    let r = &c.responses;
    assert!(r[0].starts_with("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nX-Content-Type-Options: nosniff\r\n"), "{}", r[0]);
    assert!(
        r[0].ends_with("\r\nContent-Length: 19\r\nConnection: close\r\n\r\n404 page not found\n"),
        "{}",
        r[0]
    );
    assert!(r[1].starts_with("HTTP/1.1 404 Not Found\r\n"), "{}", r[1]);
    assert!(r[2].starts_with("HTTP/1.1 307 Temporary Redirect\r\nContent-Type: text/html; charset=utf-8\r\nLocation: /hook\r\n"), "{}", r[2]);
    assert!(
        r[2].ends_with("\r\n\r\n<a href=\"/hook\">Temporary Redirect</a>.\n\n"),
        "{}",
        r[2]
    );
    assert!(
        r[3].starts_with("HTTP/1.1 307 Temporary Redirect\r\n"),
        "{}",
        r[3]
    );
    assert!(
        r[4].ends_with("\r\nContent-Length: 19\r\nConnection: close\r\n\r\n"),
        "{}",
        r[4]
    );
    assert!(
        r[5].starts_with("HTTP/1.1 401 Unauthorized\r\n"),
        "{}",
        r[5]
    );
    assert!(
        r[5].ends_with("\r\nContent-Length: 41\r\nConnection: close\r\n\r\n"),
        "HEAD has no body: {}",
        r[5]
    );
    assert_eq!(r[6], "HTTP/1.1 400 Bad Request: missing required Host header\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request: missing required Host header");
    assert_eq!(r[7], "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request");
    assert_eq!(r[8], "HTTP/1.1 505 HTTP Version Not Supported: unsupported protocol version\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n505 HTTP Version Not Supported: unsupported protocol version");
    assert!(
        r[9].starts_with("HTTP/1.1 401 Unauthorized\r\n"),
        "chunked: {}",
        r[9]
    );
    assert!(r[9].contains("skipping deploy"), "chunked: {}", r[9]);
    assert!(
        r[10].starts_with("HTTP/1.0 401 Unauthorized\r\n"),
        "{}",
        r[10]
    );
    assert!(!r[10].contains("Connection:"), "{}", r[10]);
    assert!(
        r[11].starts_with("HTTP/1.0 401 Unauthorized\r\n"),
        "{}",
        r[11]
    );
    assert!(
        r[11].contains("\r\nConnection: keep-alive\r\n"),
        "{}",
        r[11]
    );
    assert!(
        r[12].starts_with("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 401 Unauthorized\r\n"),
        "{}",
        r[12]
    );
    assert_eq!(
        r[13].matches("HTTP/1.1 401 Unauthorized\r\n").count(),
        2,
        "{}",
        r[13]
    );
    assert!(
        r[14].starts_with("HTTP/1.1 401 Unauthorized\r\n"),
        "{}",
        r[14]
    );
    assert!(r[15].starts_with("HTTP/1.1 404 Not Found\r\n"), "{}", r[15]);
    assert_eq!(c.record, "");
}

/// Small helper to turn the first request of a pipelined pair into a keep-alive one.
trait KeepAlive {
    fn splice_connection_keepalive(self) -> Vec<u8>;
}

impl KeepAlive for Vec<u8> {
    fn splice_connection_keepalive(self) -> Vec<u8> {
        let s = String::from_utf8(self).unwrap();
        s.replace("Connection: close\r\n", "").into_bytes()
    }
}

#[test]
fn root_slash_serves_every_path() {
    let cfg = Config::default().env("GHA2DB_WHROOT", "/");
    let c = run_case(
        "root_slash",
        &cfg,
        &[
            Step::Req(
                b"GET /anything/at/all HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                    .to_vec(),
            ),
            Step::Req(post_to("/", &form_body(&Payload::good().json()), "")),
        ],
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"message\": \"webhook: payload too short\"}"
    );
    assert_eq!(body(&c.responses[1]), "{\"message\": \"ok\"}");
    assert!(c
        .stdout
        .contains("WebHook config is Host:127.0.0.1 Port::<port> Root:/\n"));
}

#[test]
fn case_insensitive_keys_and_nulls_like_jsoniter() {
    let json = r#"{"BRANCH":"master","Result":0,"RESULT_MESSAGE":"Passed","Type":"push","Author_Name":null,"author_email":null,"MESSAGE":"ok","Repository":{"NAME":"devstats","Owner_Name":"cncf"}}"#;
    let c = run_case(
        "case_insensitive",
        &Config::default(),
        &[Step::Req(post(&form_body(json)))],
    );
    assert_eq!(body(&c.responses[0]), "{\"message\": \"ok\"}");
    assert!(c.stdout.contains("WebHook: author: name: , email: \n"));
}

#[test]
fn check_payload_mode_needs_the_travis_key() {
    // Travis CI .org is gone: the public key cannot be obtained, so every
    // verified payload is refused — identically on both sides (the exact
    // message depends on network access: `invalid public key` when the site
    // answers, `cannot fetch travis public key` when it is unreachable).
    let cfg = Config {
        verify: true,
        ..Config::default()
    };
    let c = run_case(
        "check_mode",
        &cfg,
        &[Step::Req(post_to(
            "/hook",
            &form_body(&Payload::good().json()),
            "Signature: aGVsbG8=\r\n",
        ))],
    );
    let r = &c.responses[0];
    assert_eq!(status_line(r), "HTTP/1.1 401 Unauthorized");
    assert!(
        body(r) == "{\"message\": \"webhook: invalid public key\"}"
            || body(r) == "{\"message\": \"webhook: cannot fetch travis public key\"}",
        "{r}"
    );
    assert_eq!(c.record, "");
}

#[test]
fn without_project_root_it_explains_and_exits() {
    let inv = Invocation::new()
        .env("GHA2DB_SKIPLOG", "1")
        .env("GHA2DB_SKIPTIME", "1");
    let rs_bin = rust_bin();
    let rs = run(&rs_bin, &inv);
    let mask = |s: &str, bin: &Path| s.replace(&bin.display().to_string(), "<bin>");
    let rs_out = mask(&rs.stdout_str(), &rs_bin);
    assert_eq!(rs.code(), 0);
    assert_eq!(
        rs_out,
        "Compiled None, commit: None on None using None\n\
         You need to define reposiory path via GHA2DB_PROJECT_ROOT=/path/to/repo <bin>\n"
    );
    if let Some(go_bin) = go_bin() {
        let go = run(&go_bin, &inv);
        assert_eq!(go.code(), rs.code());
        assert_eq!(mask(&go.stdout_str(), &go_bin), rs_out);
        assert_eq!(go.stderr_str(), rs.stderr_str());
    }
}

#[test]
fn bind_failure_is_fatal() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let blocker = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = blocker.local_addr().unwrap().port();
    let side = |bin: &Path, tag: &str| {
        let mut srv = spawn(bin, &Config::default(), tag, port);
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(s) = srv.child.try_wait().unwrap() {
                break s;
            }
            assert!(Instant::now() < deadline, "{} keeps running", bin.display());
            thread::sleep(Duration::from_millis(20));
        };
        let c = Captured {
            responses: vec![],
            stdout: fs::read_to_string(&srv.out).unwrap(),
            stderr: fs::read_to_string(&srv.err).unwrap(),
            record: String::new(),
            pid_file_left: false,
        };
        (status.code(), normalize(&c, &srv.root, port))
    };
    let go = go_bin().map(|b| side(&b, "bind_go"));
    let (code, rs) = side(&rust_bin(), "bind_rs");
    // Bug 23: the Go tool used to exit 0 silently here.
    assert_eq!(code, Some(2));
    assert_eq!(rs.stdout, "");
    assert_eq!(
        rs.stderr,
        "Error(time=<time>):\n\
         Error: 'listen tcp 127.0.0.1:<port>: bind: address already in use'\n\
         panic: stacktrace: listen tcp 127.0.0.1:<port>: bind: address already in use\n"
    );
    if let Some((go_code, go)) = go {
        assert_eq!(go_code, code);
        assert_eq!(go, rs);
    }
    drop(blocker);
}
