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

/// Environment variables understood by the ported tools. They are removed from
/// the child environment before a run so the ambient shell cannot leak into a test.
pub const TOOL_ENV_VARS: &[&str] = &[
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
];

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
    let status = Command::new(&go)
        .current_dir(&root)
        .env("CGO_ENABLED", "0")
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
