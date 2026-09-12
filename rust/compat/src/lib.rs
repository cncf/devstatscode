//! Go ⇄ Rust compatibility test harness for the DevStats binaries.
//!
//! Each ported binary keeps its differential tests in its own crate
//! (`cmd/<name>/tests/compat.rs`); this crate provides the shared plumbing:
//!
//! * locating the repository root and test fixtures,
//! * building the **Go** reference binary from `cmd/<name>/*.go` into
//!   `target/go-bin/<name>` (once per test process, requires `go` on `PATH`),
//! * running a binary with a controlled environment / stdin / working dir,
//! * comparing the two [`Outcome`]s.
//!
//! Set `DEVSTATS_SKIP_GO_COMPAT=1` to skip the Go comparison (e.g. on a machine
//! without a Go toolchain); the Rust-only tests still run.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};

pub mod gharchive;
pub mod github;
pub mod pg;

/// Environment variables understood by the ported tools. They are removed from
/// the child environment before a run so the ambient shell cannot leak into a test.
pub const TOOL_ENV_VARS: &[&str] = &[
    // tsplit / replacer
    "KIND",
    "SIZE",
    "DEBUG",
    "FROM",
    "TO",
    "NO_TO",
    "MODE",
    "NREPLACES",
    "REPLACEFROM",
    "NO_FATAL_DELAY",
    // splitcrons
    "MONTHLY",
    "KUBERNETES_HOURS",
    "ALL_HOURS",
    "GHA_OFFSET",
    "SYNC_HOURS",
    "OFFSET_HOURS",
    "ALWAYS_PATCH",
    "NEVER_PATCH",
    "ONLY_ENV",
    "ONLY_SUSPEND",
    "SUSPEND_ALL",
    "NO_SUSPEND_H",
    "NO_SUSPEND_A",
    "SKIP_AFFS_ENV",
    "SKIP_SYNC_ENV",
    "ONLY_PROD",
    "ONLY_TEST",
    "OLD_ALGORITHM",
    "NO_DB_SIZES",
    "SPLIT_ALGO",
    "WEIGHT_POWER",
    "DAILY_RANGE",
    "DAILY_REPOS_RANGE",
    "DAILY_PROJECTS",
    "NO_AFFS_ANCHOR",
    "DAILY_AFFS_OFFSET_HOURS",
    "PATCH_ENV",
    "SIZES_POD",
    "CTX_TEST",
    "CTX_PROD",
    // devstats / website_data (GetProjectsList)
    "ONLY",
];

/// Prefix of the DevStats library variables (`GHA2DB_DEBUG`, `GHA2DB_SKIPTIME`, ...);
/// all of them are removed from the child environment as well.
pub const LIB_ENV_PREFIX: &str = "GHA2DB_";

/// Result of running one binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl Outcome {
    pub fn stdout_str(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }
    pub fn stderr_str(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }
    pub fn code(&self) -> i32 {
        self.code.expect("process terminated by a signal")
    }
}

/// Root of the `cncf/devstatscode` checkout (parent of `rust/`).
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("compat crate lives in <repo>/rust/compat")
        .to_path_buf()
}

/// `<repo>/rust/compat/fixtures/<rel>`.
pub fn fixture(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(rel)
}

/// Read a fixture file.
pub fn fixture_bytes(rel: &str) -> Vec<u8> {
    let p = fixture(rel);
    fs::read(&p).unwrap_or_else(|e| panic!("cannot read fixture {}: {e}", p.display()))
}

fn target_dir() -> PathBuf {
    if let Some(t) = std::env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(t);
    }
    repo_root().join("rust").join("target")
}

/// True when the Go comparison was disabled via `DEVSTATS_SKIP_GO_COMPAT`.
pub fn go_compat_skipped() -> bool {
    std::env::var_os("DEVSTATS_SKIP_GO_COMPAT").is_some_and(|v| !v.is_empty() && v != "0")
}

fn go_tool() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("DEVSTATS_GO").map(PathBuf::from) {
        return Some(p);
    }
    let candidates = ["go", "/usr/local/go/bin/go"];
    candidates.iter().map(PathBuf::from).find(|go| {
        Command::new(go)
            .arg("version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    })
}

/// Go tools that need cgo (`mattn/go-sqlite3` compiles the SQLite amalgamation),
/// so they are built with `CGO_ENABLED=1` and require a C compiler.
const CGO_TOOLS: &[&str] = &["sqlitedb"];

/// Build (once per process) and return the Go reference binary for `name`.
///
/// Sources are `<repo>/cmd/<name>/*.go` (all files of the `main` package, e.g.
/// `get_repos` has two). Returns `None` only when the comparison is skipped
/// via `DEVSTATS_SKIP_GO_COMPAT`; a missing/failing Go toolchain is a test failure.
pub fn go_binary(name: &str) -> Option<PathBuf> {
    static CACHE: OnceLock<Mutex<HashMap<String, PathBuf>>> = OnceLock::new();
    if go_compat_skipped() {
        eprintln!("[compat] DEVSTATS_SKIP_GO_COMPAT set — skipping Go comparison for {name}");
        return None;
    }
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap();
    if let Some(p) = guard.get(name) {
        return Some(p.clone());
    }
    let go = go_tool().expect(
        "Go toolchain not found (need `go` on PATH, /usr/local/go/bin/go, or DEVSTATS_GO=/path/to/go); \
         set DEVSTATS_SKIP_GO_COMPAT=1 to skip the Go comparison",
    );
    let root = repo_root();
    let src_dir = root.join("cmd").join(name);
    let mut sources: Vec<PathBuf> = fs::read_dir(&src_dir)
        .unwrap_or_else(|e| panic!("cannot list {}: {e}", src_dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension() == Some(OsStr::new("go")))
        .filter(|p| !p.to_string_lossy().ends_with("_test.go"))
        .collect();
    sources.sort();
    assert!(
        !sources.is_empty(),
        "no Go sources in {}",
        src_dir.display()
    );
    let out_dir = target_dir().join("go-bin");
    fs::create_dir_all(&out_dir).expect("create target/go-bin");
    let out = out_dir.join(name);
    let cgo = if CGO_TOOLS.contains(&name) { "1" } else { "0" };
    let status = Command::new(&go)
        .current_dir(&root)
        .env("CGO_ENABLED", cgo)
        .arg("build")
        .arg("-o")
        .arg(&out)
        .args(&sources)
        .status()
        .unwrap_or_else(|e| panic!("cannot run {}: {e}", go.display()));
    assert!(status.success(), "go build of {name} failed");
    guard.insert(name.to_string(), out.clone());
    Some(out)
}

/// Build (once per process) a Go *probe* program — a small `main` package
/// under `<repo>/rust/compat/go/testdata/<name>/` that exercises a Go library
/// (e.g. `database/sql` + lib/pq) so its behaviour can be compared with the
/// Rust port live. `testdata` keeps the probes out of the Go module's
/// `./...` patterns. Returns `None` when the Go comparison is skipped.
pub fn go_probe(name: &str) -> Option<PathBuf> {
    static CACHE: OnceLock<Mutex<HashMap<String, PathBuf>>> = OnceLock::new();
    if go_compat_skipped() {
        eprintln!("[compat] DEVSTATS_SKIP_GO_COMPAT set — skipping Go probe {name}");
        return None;
    }
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap();
    if let Some(p) = guard.get(name) {
        return Some(p.clone());
    }
    let go = go_tool().expect(
        "Go toolchain not found (need `go` on PATH, /usr/local/go/bin/go, or DEVSTATS_GO=/path/to/go); \
         set DEVSTATS_SKIP_GO_COMPAT=1 to skip the Go comparison",
    );
    let root = repo_root();
    let pkg = format!("./rust/compat/go/testdata/{name}");
    assert!(
        root.join(&pkg).is_dir(),
        "no Go probe sources in {}",
        root.join(&pkg).display()
    );
    let out_dir = target_dir().join("go-bin");
    fs::create_dir_all(&out_dir).expect("create target/go-bin");
    let out = out_dir.join(format!("probe-{name}"));
    let status = Command::new(&go)
        .current_dir(&root)
        .env("CGO_ENABLED", "0")
        .arg("build")
        .arg("-o")
        .arg(&out)
        .arg(&pkg)
        .status()
        .unwrap_or_else(|e| panic!("cannot run {}: {e}", go.display()));
    assert!(status.success(), "go build of probe {name} failed");
    guard.insert(name.to_string(), out.clone());
    Some(out)
}

/// One invocation: environment, stdin, arguments, working directory.
#[derive(Debug, Default, Clone)]
pub struct Invocation<'a> {
    pub env: Vec<(&'a str, &'a str)>,
    pub args: Vec<String>,
    pub stdin: Vec<u8>,
    pub cwd: Option<PathBuf>,
}

impl<'a> Invocation<'a> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn env(mut self, k: &'a str, v: &'a str) -> Self {
        self.env.push((k, v));
        self
    }
    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }
    pub fn stdin(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.stdin = data.into();
        self
    }
    pub fn cwd(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cwd = Some(dir.into());
        self
    }
}

/// Run `bin` as described by `inv`; tool env vars not listed in `inv.env` are unset.
pub fn run(bin: &Path, inv: &Invocation<'_>) -> Outcome {
    let mut cmd = Command::new(bin);
    for v in TOOL_ENV_VARS {
        cmd.env_remove(v);
    }
    for v in pg::PG_ENV_VARS {
        cmd.env_remove(v);
    }
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with(LIB_ENV_PREFIX) {
            cmd.env_remove(&k);
        }
    }
    // Never wait a minute for a fatal error inside the test-suite.
    cmd.env("NO_FATAL_DELAY", "1");
    for (k, v) in &inv.env {
        cmd.env(k, v);
    }
    cmd.args(&inv.args);
    if let Some(d) = &inv.cwd {
        cmd.current_dir(d);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("cannot spawn {}: {e}", bin.display()));
    {
        let mut stdin = child.stdin.take().expect("piped stdin");
        // The child may exit before reading everything (usage errors) — ignore EPIPE.
        let _ = stdin.write_all(&inv.stdin);
    }
    let out = child.wait_with_output().expect("wait for child");
    Outcome {
        code: out.status.code(),
        stdout: out.stdout,
        stderr: out.stderr,
    }
}

/// Path of the Rust binary under test (`CARGO_BIN_EXE_<name>` from the caller's crate).
///
/// Integration tests call this as `rust_binary(env!("CARGO_BIN_EXE_tsplit"))`.
pub fn rust_binary(path: &str) -> PathBuf {
    PathBuf::from(path)
}

/// What to compare between the Go and the Rust outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compare {
    /// exit code + stdout + stderr must be identical
    All,
    /// exit code + stdout identical (stderr free-form, e.g. differing error wording)
    CodeAndStdout,
    /// only exit code identical
    Code,
}

/// Run the same invocation with both binaries and assert they agree per `what`.
/// Returns `(go, rust)` outcomes (`go` is `None` when the Go comparison is skipped).
pub fn run_both(
    go_bin: Option<&Path>,
    rust_bin: &Path,
    inv: &Invocation<'_>,
    what: Compare,
) -> (Option<Outcome>, Outcome) {
    let rust = run(rust_bin, inv);
    let Some(go_bin) = go_bin else {
        return (None, rust);
    };
    let go = run(go_bin, inv);
    let ctx = || {
        format!(
            "\n--- invocation ---\nenv: {:?}\nargs: {:?}\nstdin: {} bytes\n--- go (code {:?}) stdout ---\n{}\n--- go stderr ---\n{}\n--- rust (code {:?}) stdout ---\n{}\n--- rust stderr ---\n{}\n",
            inv.env,
            inv.args,
            inv.stdin.len(),
            go.code,
            go.stdout_str(),
            go.stderr_str(),
            rust.code,
            rust.stdout_str(),
            rust.stderr_str()
        )
    };
    assert_eq!(go.code, rust.code, "exit code differs{}", ctx());
    if matches!(what, Compare::All | Compare::CodeAndStdout) {
        assert!(go.stdout == rust.stdout, "stdout differs{}", ctx());
    }
    if what == Compare::All {
        assert!(go.stderr == rust.stderr, "stderr differs{}", ctx());
    }
    (Some(go), rust)
}

/// Copy `content` into a fresh file inside `dir` and return its path.
pub fn write_temp_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
    let p = dir.join(name);
    fs::write(&p, content).unwrap_or_else(|e| panic!("write {}: {e}", p.display()));
    p
}

/// Is `s` a Go `time.Duration.String()` (`1.5s`, `2m31.2s`, `1h0m0s`,
/// `625.293486ms`, `12µs`, `40ns`, `0s`)?
pub fn is_go_duration(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    if s.is_empty() {
        return false;
    }
    if let Some(sub) = s
        .strip_suffix("ns")
        .or_else(|| s.strip_suffix("µs"))
        .or_else(|| s.strip_suffix("ms"))
    {
        return is_go_number(sub);
    }
    // [Nh][Nm]N[.frac]s
    let Some(rest) = s.strip_suffix('s') else {
        return false;
    };
    let mut rest = rest;
    if let Some((h, r)) = rest.split_once('h') {
        if h.is_empty() || !h.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        rest = r;
        let Some((m, r)) = rest.split_once('m') else {
            return false;
        };
        if m.is_empty() || !m.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        rest = r;
    } else if let Some((m, r)) = rest.split_once('m') {
        if m.is_empty() || !m.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        rest = r;
    }
    is_go_number(rest)
}

fn is_go_number(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    !int.is_empty()
        && int.bytes().all(|b| b.is_ascii_digit())
        && frac.bytes().all(|b| b.is_ascii_digit())
        && (s.find('.').is_none() || !frac.is_empty())
}

/// Replace the run-time dependent durations printed by the tools — the
/// trailing `<duration>` of lines `Time: <duration>`, `…: took <duration>`,
/// `…, took: <duration>`, `… in: <duration>` and `<command> ... <duration>`
/// (`ExecCommand` with `GHA2DB_CMDDEBUG`), and the parenthesised
/// `(took <duration>)` in
/// the middle of a line — with `<duration>`; panics when the text after the
/// marker is not a Go duration (so a formatting regression is still caught).
pub fn mask_go_durations(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, line) in s.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        if let Some(pos) = line.find("(took ") {
            let start = pos + "(took ".len();
            let end = line[start..]
                .find(')')
                .map(|e| start + e)
                .unwrap_or_else(|| panic!("unterminated (took … in line {line:?}"));
            let dur = &line[start..end];
            assert!(
                is_go_duration(dur),
                "not a Go duration in line {line:?}: {dur:?}"
            );
            out.push_str(&line[..start]);
            out.push_str("<duration>");
            out.push_str(&line[end..]);
            continue;
        }
        let marker = if line.starts_with("Time: ") {
            Some("Time: ")
        } else if line.contains(": took ") {
            Some(": took ")
        } else if line.contains(", took: ") {
            Some(", took: ")
        } else if line.contains(" in: ") {
            Some(" in: ")
        } else if line.contains(" ... ") {
            Some(" ... ")
        } else {
            None
        };
        match marker {
            Some(m) => {
                let pos = line.rfind(m).unwrap() + m.len();
                let (head, dur) = line.split_at(pos);
                let dur_trim = dur.trim_end();
                assert!(
                    is_go_duration(dur_trim),
                    "not a Go duration in line {line:?}: {dur_trim:?}"
                );
                out.push_str(head);
                out.push_str("<duration>");
                out.push_str(&dur[dur_trim.len()..]);
            }
            None => out.push_str(line),
        }
    }
    out
}

/// Mask the Go `%v` renderings of `time.Now()`-derived values — a wall clock
/// followed by the monotonic reading, e.g.
/// `2026-09-11 16:53:58.123456789 +0200 CEST m=+0.001234` — with `<now>`:
/// they differ between two runs (and Go prints the local zone's
/// abbreviation where Rust prints the offset twice). Times without an `m=`
/// part (dates read from the database or a yaml) are left alone.
pub fn mask_go_now(s: &str) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)? [+-]\d{4} \S+ m=[+-]\d+\.\d+",
        )
        .unwrap()
    });
    re.replace_all(s, "<now>").into_owned()
}

#[cfg(test)]
mod duration_tests {
    use super::*;

    #[test]
    fn now_values_are_masked() {
        assert_eq!(
            mask_go_now("for date to 2026-09-11 16:53:58.123456789 +0200 CEST m=+0.001234, x"),
            "for date to <now>, x"
        );
        assert_eq!(
            mask_go_now(
                "a 2026-09-11 16:53:58 +0000 UTC m=-7199.999 b 2020-03-02 03:00:00 +0000 +0000 c"
            ),
            "a <now> b 2020-03-02 03:00:00 +0000 +0000 c"
        );
        assert_eq!(mask_go_now("no time here"), "no time here");
    }

    #[test]
    fn go_durations() {
        for d in [
            "0s",
            "40ns",
            "12µs",
            "1.5µs",
            "625.293486ms",
            "1.5s",
            "2m31.2s",
            "1h0m0s",
            "10h2m3.5s",
        ] {
            assert!(is_go_duration(d), "{d}");
        }
        for d in ["", "s", "1", "1.s", "h0m0s", "1x", "1m", "1h5s", "1.2.3s"] {
            assert!(!is_go_duration(d), "{d}");
        }
        assert_eq!(
            mask_go_durations("Compiled\nTime: 1.5s\nExecuted script: a.sql: took 12µs\n"),
            "Compiled\nTime: <duration>\nExecuted script: a.sql: took <duration>\n"
        );
        assert_eq!(
            mask_go_durations("Mass updated \"t\", columns: 3, took: 4.525462ms"),
            "Mass updated \"t\", columns: 3, took: <duration>"
        );
        assert_eq!(
            mask_go_durations("x: Error result for p (took 434.038µs): exit status 3"),
            "x: Error result for p (took <duration>): exit status 3"
        );
        assert_eq!(
            mask_go_durations("Synced all projects in: 1.772666891s"),
            "Synced all projects in: <duration>"
        );
        assert_eq!(
            mask_go_durations("./gha2db_sync ... 303.353479ms"),
            "./gha2db_sync ... <duration>"
        );
    }
}
