//! Go ⇄ Rust differential tests for `splitcrons`.
//!
//! Every scenario runs each binary in its own temporary directory that holds a
//! copy of the input `values.yaml`, a fake `kubectl` (a POSIX shell script that
//! logs every call and answers from canned files) and the canned cluster state.
//! The exit code, stdout, the written `new-values.yaml` bytes and the exact
//! sequence of `kubectl` invocations must be identical; stderr is compared for
//! successful runs (fatal errors print Go stack traces / timestamps).

use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat as compat;
use devstats_compat::{fixture, go_binary, rust_binary, Invocation, Outcome};

const FIX: &str = "splitcrons";

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_splitcrons"))
}

fn go_bin() -> Option<PathBuf> {
    go_binary("splitcrons")
}

/// One test scenario.
#[derive(Debug, Clone)]
struct Scenario {
    /// file copied in as `values.yaml` (`None`: no input file at all)
    values: Option<PathBuf>,
    env: Vec<(String, String)>,
    /// install the fake `kubectl` (otherwise `PATH` holds only an empty directory)
    kubectl: bool,
    /// canned cluster state files to install (`cronjobs-*.txt`, `sizes-*.txt`)
    cluster: bool,
    /// marker files for the fake kubectl: `fail-get`, `fail-exec`, `fail-patch`, `fail-patch-<cronjob>`
    fail: Vec<&'static str>,
    /// command line (default: `values.yaml new-values.yaml`)
    args: Option<Vec<String>>,
    /// custom `cronjobs-devstats-{test,prod}.txt` contents (installed even without `cluster`)
    cronjobs: Option<(String, String)>,
}

impl Default for Scenario {
    fn default() -> Self {
        Scenario {
            values: Some(fixture(&format!("{FIX}/values-small.yaml"))),
            env: Vec::new(),
            kubectl: true,
            cluster: true,
            fail: Vec::new(),
            args: None,
            cronjobs: None,
        }
    }
}

impl Scenario {
    fn new() -> Self {
        Self::default()
    }
    fn values(mut self, f: &'static str) -> Self {
        self.values = Some(fixture(&format!("{FIX}/{f}")));
        self
    }
    fn values_path(mut self, p: PathBuf) -> Self {
        self.values = Some(p);
        self
    }
    fn env(mut self, k: &str, v: &str) -> Self {
        self.env.push((k.to_string(), v.to_string()));
        self
    }
    fn envs(mut self, kvs: &[(&str, &str)]) -> Self {
        for (k, v) in kvs {
            self.env.push((k.to_string(), v.to_string()));
        }
        self
    }
    fn fail(mut self, marker: &'static str) -> Self {
        self.fail.push(marker);
        self
    }
    fn no_cluster(mut self) -> Self {
        self.cluster = false;
        self
    }
    fn no_kubectl(mut self) -> Self {
        self.kubectl = false;
        self
    }
    fn args(mut self, args: &[&str]) -> Self {
        self.args = Some(args.iter().map(|s| s.to_string()).collect());
        self
    }
    fn cronjobs(mut self, test: &str, prod: &str) -> Self {
        self.cronjobs = Some((test.to_string(), prod.to_string()));
        self
    }
}

/// Result of one binary run: process outcome + written YAML + kubectl call log.
#[derive(Debug)]
struct RunResult {
    outcome: Outcome,
    yaml: Option<Vec<u8>>,
    log: Option<String>,
}

impl RunResult {
    fn stdout(&self) -> String {
        self.outcome.stdout_str()
    }
    fn log_lines(&self) -> Vec<Vec<String>> {
        self.log
            .as_deref()
            .unwrap_or("")
            .lines()
            .map(|l| l.split('\u{1f}').map(str::to_string).collect())
            .collect()
    }
}

/// Makes `src` available as the executable `dst`.
///
/// A symlink whenever `src` is already executable (the fixture is tracked with
/// mode 0755): the test process never opens an executable for writing, which
/// avoids the classic multi-threaded fork/exec race where a child spawned by
/// another test between `fork` and `exec` still holds the write descriptor and
/// the subsequent `exec` fails with "text file busy". Falls back to copy+chmod.
fn install_executable(src: &Path, dst: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let executable = fs::metadata(src)
            .map(|m| m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false);
        if executable {
            std::os::unix::fs::symlink(src, dst).unwrap();
            return;
        }
        fs::copy(src, dst).unwrap();
        fs::set_permissions(dst, fs::Permissions::from_mode(0o755)).unwrap();
    }
    #[cfg(not(unix))]
    {
        fs::copy(src, dst).unwrap();
    }
}

fn install_fake(dir: &Path, sc: &Scenario) {
    let bin = dir.join("bin");
    fs::create_dir_all(&bin).unwrap();
    if sc.kubectl {
        install_executable(&fixture(&format!("{FIX}/kubectl")), &bin.join("kubectl"));
    }
    if sc.cluster {
        for f in [
            "cronjobs-devstats-test.txt",
            "cronjobs-devstats-prod.txt",
            "sizes-devstats-test.txt",
            "sizes-devstats-prod.txt",
        ] {
            fs::copy(fixture(&format!("{FIX}/{f}")), dir.join(f)).unwrap();
        }
    }
    if let Some((test, prod)) = &sc.cronjobs {
        fs::write(dir.join("cronjobs-devstats-test.txt"), test).unwrap();
        fs::write(dir.join("cronjobs-devstats-prod.txt"), prod).unwrap();
    }
    for m in &sc.fail {
        fs::write(dir.join(m), b"").unwrap();
    }
    if let Some(v) = &sc.values {
        fs::copy(v, dir.join("values.yaml")).unwrap();
    }
}

fn run_in(bin: &Path, sc: &Scenario) -> RunResult {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    install_fake(dir, sc);
    let dir_s = dir.to_string_lossy().into_owned();
    let path = if sc.kubectl {
        format!(
            "{}:{}",
            dir.join("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        )
    } else {
        dir.join("bin").to_string_lossy().into_owned()
    };
    let mut inv = Invocation::new()
        .env("PATH", &path)
        .env("SPLITCRONS_FAKE_DIR", &dir_s)
        .env("GHA2DB_SKIPTIME", "1")
        .env("GHA2DB_SKIPLOG", "1")
        .cwd(dir);
    for (k, v) in &sc.env {
        inv = inv.env(k, v);
    }
    match &sc.args {
        Some(a) => {
            for x in a {
                inv = inv.arg(x.clone());
            }
        }
        None => inv = inv.arg("values.yaml").arg("new-values.yaml"),
    }
    let outcome = compat::run(bin, &inv);
    let yaml = fs::read(dir.join("new-values.yaml")).ok();
    let log = fs::read_to_string(dir.join("kubectl.log")).ok();
    RunResult { outcome, yaml, log }
}

/// Run both binaries and assert full agreement. Returns `(go, rust)` results.
fn compare(sc: &Scenario) -> (Option<RunResult>, RunResult) {
    let rust = run_in(&rust_bin(), sc);
    let Some(go) = go_bin() else {
        return (None, rust);
    };
    let go = run_in(&go, sc);
    let ctx = || {
        format!(
            "\n--- scenario ---\n{sc:#?}\n--- go (code {:?}) stdout ---\n{}\n--- go stderr ---\n{}\n--- rust (code {:?}) stdout ---\n{}\n--- rust stderr ---\n{}\n--- go kubectl log ---\n{}\n--- rust kubectl log ---\n{}\n",
            go.outcome.code,
            go.stdout(),
            go.outcome.stderr_str(),
            rust.outcome.code,
            rust.stdout(),
            rust.outcome.stderr_str(),
            go.log.as_deref().unwrap_or("<none>"),
            rust.log.as_deref().unwrap_or("<none>"),
        )
    };
    assert_eq!(go.outcome.code, rust.outcome.code, "exit code{}", ctx());
    assert_eq!(go.stdout(), rust.stdout(), "stdout{}", ctx());
    if go.outcome.code == Some(0) {
        assert_eq!(
            go.outcome.stderr_str(),
            rust.outcome.stderr_str(),
            "stderr{}",
            ctx()
        );
    }
    assert_eq!(go.log, rust.log, "kubectl invocations{}", ctx());
    match (&go.yaml, &rust.yaml) {
        (Some(g), Some(r)) => {
            if g != r {
                panic!(
                    "written YAML differs{}\n--- go yaml ---\n{}\n--- rust yaml ---\n{}\n",
                    ctx(),
                    String::from_utf8_lossy(g),
                    String::from_utf8_lossy(r)
                );
            }
        }
        (None, None) => {}
        _ => panic!(
            "output file presence differs (go: {}, rust: {}){}",
            go.yaml.is_some(),
            rust.yaml.is_some(),
            ctx()
        ),
    }
    (Some(go), rust)
}

fn ok(sc: &Scenario) -> RunResult {
    let (_, rust) = compare(sc);
    assert_eq!(
        rust.outcome.code,
        Some(0),
        "expected success:\n{}",
        rust.outcome.stderr_str()
    );
    assert!(rust.yaml.is_some(), "no new-values.yaml written");
    rust
}

fn fatal(sc: &Scenario, needle: &str) -> RunResult {
    let (_, rust) = compare(sc);
    assert_eq!(
        rust.outcome.code,
        Some(2),
        "expected fatal exit 2:\n{}",
        rust.stdout()
    );
    assert!(
        rust.yaml.is_none(),
        "fatal run must not write new-values.yaml"
    );
    let err = rust.outcome.stderr_str();
    assert!(
        err.contains(needle),
        "stderr should mention {needle:?}:\n{err}"
    );
    rust
}

/// The fake logs its arguments only (no `kubectl` in front): the verb is field 0.
fn count_verb(res: &RunResult, verb: &str) -> usize {
    res.log_lines()
        .iter()
        .filter(|l| l.first().map(String::as_str) == Some(verb))
        .count()
}

// ---------------------------------------------------------------- new algorithm

#[test]
fn default_new_algorithm() {
    let r = ok(&Scenario::new());
    let out = r.stdout();
    assert!(out.starts_with("read values.yaml\nnew algorithm: probing alive cronjobs & DB sizes"));
    assert!(out.contains("weights: SPLIT_ALGO=geom, weight = size^0.5\n"));
    assert!(out.contains("sync happens from HH:04, every 6 hours; affs spread over 7 days\n"));
    assert!(out.contains("test: affs anchored to own sync: regular projects mid-gap (sync slot +180m), daily projects sync +8h"));
    assert!(out.ends_with("written new-values.yaml\n"));
    // alive/eligible projects for test: cncf, envoy, tikv, nats (containerd suspended, helm test-suspended)
    assert!(out.contains("test: 4 alive projects (0 daily)"));
    // prod: kubernetes, prometheus, all, istio, jenkins, allcdf, envoy, helm, tikv, spiffe, opentelemetry, nats
    assert!(out.contains("prod: 12 alive projects (6 daily)"));
    // suspend patches: 2 per eligible+alive cronjob; schedule patches; env patches for dailies
    assert!(count_verb(&r, "patch") > 20);
    assert_eq!(count_verb(&r, "get"), 2);
    assert_eq!(count_verb(&r, "exec"), 2);
    let yaml = String::from_utf8(r.yaml.clone().unwrap()).unwrap();
    assert!(yaml.starts_with("nSyncCPUs: 12\nnAffsCPUs: 8\naffiliationsImportCron: 10 2 * * *\naffiliationsImportCronTest: 10 1 * * *\nprojects:\n- proj: kubernetes\n"));
    // unrelated top-level keys are dropped, the project list keeps its order
    assert!(!yaml.contains("namespace:"));
    assert!(yaml.contains("- proj: nats\n"));
    // daily project ranges get set
    assert!(yaml.contains(
        "  recentRange: 26 hours\n  orphanCommitsRange: 26 hours\n  recentReposRange: 2 days\n"
    ));
}

#[test]
fn default_matches_recorded_go_output() {
    // Recorded from the Go binary: protects the Rust behaviour even when the Go comparison is skipped.
    let r = ok(&Scenario::new());
    let want_out = compat::fixture_bytes(&format!("{FIX}/expected-default.stdout"));
    assert_eq!(r.stdout(), String::from_utf8(want_out).unwrap());
    let want_yaml = compat::fixture_bytes(&format!("{FIX}/expected-default.yaml"));
    assert_eq!(r.yaml.as_deref(), Some(want_yaml.as_slice()));
    let want_log = compat::fixture_bytes(&format!("{FIX}/expected-default.kubectl.log"));
    assert_eq!(
        r.log.as_deref(),
        Some(String::from_utf8(want_log).unwrap().as_str())
    );
}

#[test]
fn debug_output() {
    let r = ok(&Scenario::new().env("DEBUG", "1"));
    let out = r.stdout();
    // devstats-postgres-backup counts as a sync cronjob named "postgres-backup"
    assert!(out.contains("getAliveCronjobs: -n devstats-test: 13 sync, 13 affs cronjobs found\n"));
    assert!(out.contains("getDBSizes: -n devstats-test: got 16 database sizes\n"));
    assert!(out.contains("patch: -n devstats-test devstats-cncf: suspend=false\n"));
    assert!(out.contains("patchEnv: -n devstats-prod devstats-kubernetes: [GHA2DB_RECENT_RANGE GHA2DB_ORPHAN_COMMITS_RANGE GHA2DB_RECENT_REPOS_RANGE]=[26 hours 26 hours 2 days]\n"));
}

#[test]
fn never_patch() {
    let r = ok(&Scenario::new().env("NEVER_PATCH", "1"));
    assert_eq!(count_verb(&r, "patch"), 0);
    assert!(r.stdout().contains("patched 0/0 cronjobs\n"));
    // values are still rewritten
    assert!(r.yaml.is_some());
}

#[test]
fn always_patch() {
    let base = ok(&Scenario::new());
    let r = ok(&Scenario::new().env("ALWAYS_PATCH", "1"));
    assert!(count_verb(&r, "patch") >= count_verb(&base, "patch"));
    assert_eq!(base.yaml, r.yaml);
}

#[test]
fn only_env_and_patch_env() {
    let r = ok(&Scenario::new()
        .env("ONLY_ENV", "1")
        .env("PATCH_ENV", "MaxHist, NCPUs,AffSkipTemp,SkipUpdAffs,Bogus"));
    // ONLY_ENV: no schedule/suspend patches, only env patches
    for l in r.log_lines() {
        if l[1] == "patch" {
            assert!(l.iter().any(|a| a.contains("\"env\":[")), "{l:?}");
        }
    }
    assert!(r.stdout().contains("patched"));
    let log = r.log.clone().unwrap();
    assert!(log.contains(r#"{"name":"GHA2DB_MAX_HIST","value":"2"}"#));
    assert!(log.contains(r#"{"name":"GHA2DB_NCPUS","value":"12"}"#)); // sync
    assert!(log.contains(r#"{"name":"GHA2DB_NCPUS","value":"8"}"#)); // affs
    assert!(log.contains(r#"{"name":"SKIPTEMP","value":"1"}"#));
    assert!(log.contains(r#"{"name":"SKIP_UPD_AFFS","value":"60"}"#));
    // MaxHist 0 → empty value (unset)
    assert!(log.contains(r#"{"name":"GHA2DB_MAX_HIST","value":""}"#));
}

#[test]
fn patch_env_all_names() {
    let all = "AffSkipTemp,MaxHist,SkipAffsLock,AffsLockDB,NoDurable,DurablePQ,MaxRunDuration,RecentRange,OrphanCommitsRange,RecentReposRange,SkipGHAPI,SkipGetRepos,NCPUs,SkipImpAffs,SkipUpdAffs";
    let r = ok(&Scenario::new().env("PATCH_ENV", all));
    let log = r.log.clone().unwrap();
    for name in [
        "SKIPTEMP",
        "GHA2DB_MAX_HIST",
        "SKIP_AFFS_LOCK",
        "AFFS_LOCK_DB",
        "NO_DURABLE",
        "DURABLE_PQ",
        "GHA2DB_MAX_RUN_DURATION",
        "GHA2DB_RECENT_RANGE",
        "GHA2DB_ORPHAN_COMMITS_RANGE",
        "GHA2DB_RECENT_REPOS_RANGE",
        "GHA2DB_GHAPISKIP",
        "GHA2DB_GETREPOSSKIP",
        "GHA2DB_NCPUS",
        "SKIP_IMP_AFFS",
        "SKIP_UPD_AFFS",
    ] {
        assert!(
            log.contains(&format!("\"name\":\"{name}\"")),
            "{name} missing"
        );
    }
    ok(&Scenario::new()
        .env("PATCH_ENV", all)
        .env("SKIP_AFFS_ENV", "1"));
    ok(&Scenario::new()
        .env("PATCH_ENV", all)
        .env("SKIP_SYNC_ENV", "1"));
    ok(&Scenario::new()
        .env("PATCH_ENV", all)
        .env("SKIP_SYNC_ENV", "1")
        .env("SKIP_AFFS_ENV", "1"));
    ok(&Scenario::new().env("PATCH_ENV", " , ,"));
}

#[test]
fn suspend_variants() {
    let r = ok(&Scenario::new().env("ONLY_SUSPEND", "1"));
    for l in r.log_lines() {
        if l[1] == "patch" {
            assert!(l.last().unwrap().contains("\"suspend\":"), "{l:?}");
        }
    }
    assert!(!r.stdout().contains("alive projects"));
    let r = ok(&Scenario::new().env("SUSPEND_ALL", "1"));
    assert!(r.log.clone().unwrap().contains("\"suspend\":true"));
    assert!(!r.log.clone().unwrap().contains("\"suspend\":false"));
    ok(&Scenario::new().env("NO_SUSPEND_H", "1"));
    ok(&Scenario::new().env("NO_SUSPEND_A", "1"));
    ok(&Scenario::new()
        .env("NO_SUSPEND_A", "1")
        .env("NO_SUSPEND_H", "1"));
    ok(&Scenario::new()
        .env("ONLY_SUSPEND", "1")
        .env("SUSPEND_ALL", "1")
        .env("ONLY_TEST", "1"));
}

#[test]
fn only_test_only_prod() {
    let t = ok(&Scenario::new().env("ONLY_TEST", "1"));
    assert!(t.stdout().contains("test: 4 alive projects"));
    assert!(!t.stdout().contains("prod: "));
    let p = ok(&Scenario::new().env("ONLY_PROD", "1"));
    assert!(p.stdout().contains("prod: 12 alive projects"));
    assert!(!p.stdout().contains("test: "));
    let none = ok(&Scenario::new().env("ONLY_PROD", "1").env("ONLY_TEST", "1"));
    assert_eq!(count_verb(&none, "get"), 0);
}

#[test]
fn monthly() {
    let r = ok(&Scenario::new().env("MONTHLY", "1"));
    let out = r.stdout();
    assert!(out.contains("affs spread over 28 days\n"));
    assert!(out.contains("affs space 37632 minutes (28 days):"));
    ok(&Scenario::new()
        .env("MONTHLY", "1")
        .env("SYNC_HOURS", "1")
        .env("GHA_OFFSET", "10"));
}

#[test]
fn sync_hours_and_gha_offset() {
    for h in ["1", "2", "3", "4", "5", "6"] {
        ok(&Scenario::new().env("SYNC_HOURS", h));
    }
    ok(&Scenario::new().env("GHA_OFFSET", "2"));
    ok(&Scenario::new()
        .env("GHA_OFFSET", "10")
        .env("SYNC_HOURS", "2"));
}

#[test]
fn split_algorithms() {
    let r = ok(&Scenario::new().env("SPLIT_ALGO", "prop"));
    assert!(r
        .stdout()
        .contains("weights: SPLIT_ALGO=prop, weight = size^1\n"));
    let r = ok(&Scenario::new().env("SPLIT_ALGO", "invgeom"));
    assert!(r
        .stdout()
        .contains("weights: SPLIT_ALGO=invgeom, weight = size^1.5\n"));
    let r = ok(&Scenario::new().env("SPLIT_ALGO", "geom"));
    assert!(r
        .stdout()
        .contains("weights: SPLIT_ALGO=geom, weight = size^0.5\n"));
    for p in ["0", "0.75", "2", "4", "1e0", ".5", "0.333333"] {
        let r = ok(&Scenario::new().env("WEIGHT_POWER", p));
        assert!(
            r.stdout().contains("weights: SPLIT_ALGO=power="),
            "{}",
            r.stdout()
        );
    }
    let r = ok(&Scenario::new()
        .env("SPLIT_ALGO", "prop")
        .env("WEIGHT_POWER", "3"));
    assert!(r
        .stdout()
        .contains("weights: SPLIT_ALGO=power=3, weight = size^3\n"));
    ok(&Scenario::new().env("NO_DB_SIZES", "1"));
    let r = ok(&Scenario::new()
        .env("NO_DB_SIZES", "1")
        .env("SPLIT_ALGO", "invgeom"));
    assert_eq!(count_verb(&r, "exec"), 0);
    assert!(r.stdout().contains("weight=      1.0"));
}

#[test]
fn daily_knobs() {
    let r = ok(&Scenario::new().env("DAILY_PROJECTS", "-"));
    assert!(r.stdout().contains("prod: 12 alive projects (0 daily)"));
    // formerly-daily projects get their ranges cleared and env patched to empty
    let yaml = String::from_utf8(r.yaml.clone().unwrap()).unwrap();
    assert!(!yaml.contains("recentRange"));
    assert!(r
        .log
        .clone()
        .unwrap()
        .contains(r#"{"name":"GHA2DB_RECENT_RANGE","value":""}"#));
    let r = ok(&Scenario::new().env("DAILY_PROJECTS", " envoy , tikv,,nats"));
    assert!(r.stdout().contains("test: 4 alive projects (3 daily)"));
    let r = ok(&Scenario::new()
        .env("DAILY_RANGE", "30 hours")
        .env("DAILY_REPOS_RANGE", "3 days"));
    let yaml = String::from_utf8(r.yaml.clone().unwrap()).unwrap();
    assert!(yaml.contains(
        "  recentRange: 30 hours\n  orphanCommitsRange: 30 hours\n  recentReposRange: 3 days\n"
    ));
    assert!(r.stdout().contains("ranges='30 hours'"));
    let r = ok(&Scenario::new().env("NO_AFFS_ANCHOR", "1"));
    assert!(!r.stdout().contains("affs anchored"));
    ok(&Scenario::new().env("DAILY_AFFS_OFFSET_HOURS", "1"));
    let r = ok(&Scenario::new().env("DAILY_AFFS_OFFSET_HOURS", "23"));
    assert!(r.stdout().contains("daily projects sync +23h"));
    ok(&Scenario::new()
        .env("DAILY_AFFS_OFFSET_HOURS", "23")
        .env("SYNC_HOURS", "5"));
}

#[test]
fn kube_context_and_pod() {
    let r = ok(&Scenario::new());
    let log = r.log.clone().unwrap();
    assert!(log.contains("--context\u{1f}test\u{1f}-n\u{1f}devstats-test"));
    assert!(log.contains("--context\u{1f}prod\u{1f}-n\u{1f}devstats-prod"));
    assert!(log.contains("\u{1f}devstats-postgres-0\u{1f}--\u{1f}psql"));
    let r = ok(&Scenario::new()
        .env("CTX_TEST", "-")
        .env("CTX_PROD", "my-prod"));
    let log = r.log.clone().unwrap();
    assert!(!log.contains("--context\u{1f}test"));
    assert!(log
        .lines()
        .any(|l| l.starts_with("get\u{1f}cronjob\u{1f}-n\u{1f}devstats-test")));
    assert!(log.contains("--context\u{1f}my-prod\u{1f}-n\u{1f}devstats-prod"));
    let r = ok(&Scenario::new()
        .env("SIZES_POD", "pg-0")
        .env("CTX_PROD", "-"));
    assert!(r
        .log
        .clone()
        .unwrap()
        .contains("\u{1f}pg-0\u{1f}--\u{1f}psql"));
}

#[test]
fn kubectl_failures() {
    // get fails -> everything considered alive (values.yaml eligibility)
    let r = ok(&Scenario::new().fail("fail-get"));
    let out = r.stdout();
    assert!(out.contains("Compiled None, commit: None on None using None\n"));
    assert!(out.contains("STDERR:\nerror: You must be logged in to the server (Unauthorized)\n"));
    assert!(out.contains("getAliveCronjobs: -n devstats-test: error: exit status 1 (falling back to values.yaml based eligibility)\n"));
    assert!(out.contains("test: 4 alive projects (0 daily)"));
    assert!(out.contains("prod: 12 alive projects (6 daily)"));
    // sizes fail -> even weights
    let r = ok(&Scenario::new().fail("fail-exec"));
    let out = r.stdout();
    assert!(out.contains("some partial output\n"));
    assert!(out.contains("getDBSizes: -n devstats-test devstats-postgres-0: error: exit status 1 (falling back to even weights)\n"));
    assert!(out.contains("weight=      1.0"));
    // all patches fail
    let r = ok(&Scenario::new().fail("fail-patch"));
    let out = r.stdout();
    assert!(out.contains("[kubectl patch cronjob --context test -n devstats-test devstats-cncf -p {\"spec\":{\"suspend\":false}}]: error: exit status 1\n"));
    assert!(out.contains("patched 0/"));
    // one cronjob fails
    let r = ok(&Scenario::new().fail("fail-patch-devstats-affiliations-istio"));
    let out = r.stdout();
    assert!(out.contains("[kubectl patch cronjob --context prod -n devstats-prod devstats-affiliations-istio -p {\"spec\":{\"suspend\":false}}]: error: exit status 1\n"));
    assert!(out.contains("devstats-affiliations-istio -p {\"spec\":{\"schedule\":\""));
    assert!(out.contains("cronjobs.batch \"devstats-affiliations-istio\" not found\n"));
    assert!(out.contains("devstats-affiliations-istio -p {\"spec\":{\"schedule\":\"40 3 * * 5\"}}]: error: exit status 1\n"));
    assert!(!out.contains("devstats-affiliations-cncf -p {\"spec\":{\"suspend\":false}}]: error"));
    ok(&Scenario::new()
        .fail("fail-get")
        .fail("fail-exec")
        .fail("fail-patch")
        .env("DEBUG", "1"));
    // no canned cluster state at all
    ok(&Scenario::new().no_cluster());
}

#[test]
fn kubectl_missing() {
    let r = ok(&Scenario::new().no_kubectl());
    let out = r.stdout();
    assert!(out.contains("getAliveCronjobs: -n devstats-test: error: exec: \"kubectl\": executable file not found in $PATH (falling back to values.yaml based eligibility)\n"));
    assert!(out.contains("getDBSizes: -n devstats-test devstats-postgres-0: error: exec: \"kubectl\": executable file not found in $PATH (falling back to even weights)\n"));
    assert!(r.log.is_none());
}

#[test]
fn other_fixtures_new_algorithm() {
    ok(&Scenario::new().values("values-nokube.yaml").no_cluster());
    ok(&Scenario::new()
        .values("values-nokube.yaml")
        .no_cluster()
        .env("MONTHLY", "1"));
    ok(&Scenario::new()
        .values("values-allsuspended.yaml")
        .no_cluster());
    ok(&Scenario::new()
        .values("values-allsuspended.yaml")
        .no_cluster()
        .env("DAILY_PROJECTS", "-"));
    // cluster reachable but no cronjobs at all in test, a single sync-only project in prod
    let r = ok(&Scenario::new().cronjobs("", "devstats-tikv\n"));
    let out = r.stdout();
    assert!(out.contains("test: no alive projects to schedule\n"));
    assert!(out.contains("prod: 1 alive projects (0 daily)"));
    assert_eq!(count_verb(&r, "patch"), 2); // suspend + schedule for the one alive cronjob
                                            // nothing alive anywhere, cronjobs listed for unknown projects are ignored
    let r = ok(&Scenario::new().cronjobs("devstats-nosuch\n", "devstats-affiliations-nosuch\n"));
    let out = r.stdout();
    assert!(out.contains("test: no alive projects to schedule\n"));
    assert!(out.contains("prod: no alive projects to schedule\n"));
    assert_eq!(count_verb(&r, "patch"), 0);
    ok(&Scenario::new()
        .values("values-importcron.yaml")
        .no_cluster());
    ok(&Scenario::new()
        .values("values-importcron.yaml")
        .no_cluster()
        .env("DAILY_PROJECTS", "kubernetes"));
    ok(&Scenario::new().values("values-edge.yaml").no_cluster());
    ok(&Scenario::new()
        .values("values-edge.yaml")
        .no_cluster()
        .env("NEVER_PATCH", "1")
        .env("OLD_ALGORITHM", "1"));
    let r = ok(&Scenario::new().values("values-empty.yaml").no_cluster());
    assert_eq!(
        r.yaml.as_deref(),
        Some(b"nSyncCPUs: 0\nnAffsCPUs: 0\nprojects: []\n".as_slice())
    );
    let r = ok(&Scenario::new().values("values-comment.yaml").no_cluster());
    assert_eq!(
        r.yaml.as_deref(),
        Some(b"nSyncCPUs: 0\nnAffsCPUs: 0\nprojects: []\n".as_slice())
    );
    let r = ok(&Scenario::new()
        .values("values-noprojects.yaml")
        .no_cluster());
    assert_eq!(
        r.yaml.as_deref(),
        Some(
            b"nSyncCPUs: 3\nnAffsCPUs: 5\naffiliationsImportCron: 1 2 3 4 5\nprojects: []\n"
                .as_slice()
        )
    );
    ok(&Scenario::new()
        .values("values-nullprojects.yaml")
        .no_cluster());
}

// ---------------------------------------------------------------- old algorithm

#[test]
fn old_algorithm_default() {
    let r = ok(&Scenario::new().env("OLD_ALGORITHM", "1"));
    let out = r.stdout();
    assert!(out.contains("sync happens from HH:04, every 6 hours, which gives 336min for hourly syncs, middle of weekend offset is -4h\n"));
    assert!(out.contains("test: Kubernetes(#0) needs 24h, All(#5) needs 20h, 4 others all have 124h, intervals are 1860.0min, 5040.0s\n"));
    assert!(out.contains("prod: Kubernetes(#0) needs 24h, All(#5) needs 20h, 10 others all have 124h, intervals are 744.0min, 2016.0s\n"));
    assert_eq!(count_verb(&r, "get"), 0);
    assert_eq!(count_verb(&r, "exec"), 0);
    assert!(count_verb(&r, "patch") > 10);
    let want_out = compat::fixture_bytes(&format!("{FIX}/expected-old.stdout"));
    assert_eq!(r.stdout(), String::from_utf8(want_out).unwrap());
    let want_yaml = compat::fixture_bytes(&format!("{FIX}/expected-old.yaml"));
    assert_eq!(r.yaml.as_deref(), Some(want_yaml.as_slice()));
}

#[test]
fn old_algorithm_variants() {
    let old = |kvs: &[(&str, &str)]| Scenario::new().env("OLD_ALGORITHM", "1").envs(kvs);
    ok(&old(&[("MONTHLY", "1")]));
    ok(&old(&[
        ("MONTHLY", "1"),
        ("KUBERNETES_HOURS", "48"),
        ("ALL_HOURS", "3"),
    ]));
    ok(&old(&[("ONLY_SUSPEND", "1")]));
    ok(&old(&[("SUSPEND_ALL", "1")]));
    ok(&old(&[("SUSPEND_ALL", "1"), ("NO_SUSPEND_H", "1")]));
    ok(&old(&[("NO_SUSPEND_A", "1"), ("NO_SUSPEND_H", "1")]));
    ok(&old(&[("ONLY_TEST", "1")]));
    ok(&old(&[("ONLY_PROD", "1")]));
    ok(&old(&[("ONLY_TEST", "1"), ("ONLY_PROD", "1")]));
    ok(&old(&[("ALWAYS_PATCH", "1")]));
    ok(&old(&[("NEVER_PATCH", "1")]));
    ok(&old(&[
        ("ONLY_ENV", "1"),
        ("PATCH_ENV", "MaxHist,NCPUs,MaxRunDuration,RecentRange"),
    ]));
    ok(&old(&[("PATCH_ENV", "AffSkipTemp,SkipAffsLock,AffsLockDB,NoDurable,DurablePQ,SkipGHAPI,SkipGetRepos,SkipImpAffs,SkipUpdAffs,OrphanCommitsRange,RecentReposRange")]));
    ok(&old(&[("PATCH_ENV", "MaxHist"), ("SKIP_AFFS_ENV", "1")]));
    ok(&old(&[("PATCH_ENV", "MaxHist"), ("SKIP_SYNC_ENV", "1")]));
    ok(&old(&[("OFFSET_HOURS", "-84")]));
    ok(&old(&[("OFFSET_HOURS", "0")]));
    ok(&old(&[("OFFSET_HOURS", "84")]));
    ok(&old(&[("KUBERNETES_HOURS", "3")]));
    ok(&old(&[("KUBERNETES_HOURS", "30"), ("ALL_HOURS", "30")]));
    ok(&old(&[("ALL_HOURS", "3")]));
    ok(&old(&[("SYNC_HOURS", "1")]));
    ok(&old(&[("SYNC_HOURS", "3"), ("GHA_OFFSET", "10")]));
    ok(&old(&[("GHA_OFFSET", "2")]));
    ok(&old(&[("DEBUG", "1")]));
    ok(&old(&[]).fail("fail-patch"));
    ok(&old(&[]).fail("fail-patch-devstats-kubernetes"));
    ok(&old(&[]).no_kubectl());
    // the new-algorithm knobs are ignored in legacy mode
    ok(&old(&[
        ("SPLIT_ALGO", "prop"),
        ("DAILY_PROJECTS", "-"),
        ("NO_AFFS_ANCHOR", "1"),
    ]));
}

#[test]
fn old_algorithm_other_fixtures() {
    let old = |v: &'static str| {
        Scenario::new()
            .env("OLD_ALGORITHM", "1")
            .values(v)
            .no_cluster()
    };
    // no gha/allprj: Kubernetes(#-1), All(#-1)
    let r = ok(&old("values-nokube.yaml"));
    assert!(r
        .stdout()
        .contains("test: Kubernetes(#-1) needs 24h, All(#-1) needs 20h, 4 others"));
    // every regular project suspended/archived: division by zero -> +Inf intervals
    let r = ok(&old("values-allsuspended.yaml"));
    assert!(r
        .stdout()
        .contains("0 others all have 124h, intervals are +Infmin, +Infs\n"));
    ok(&old("values-allsuspended.yaml").env("MONTHLY", "1"));
    ok(&old("values-importcron.yaml"));
    ok(&old("values-edge.yaml"));
    let r = ok(&old("values-empty.yaml"));
    assert_eq!(
        r.yaml.as_deref(),
        Some(b"nSyncCPUs: 0\nnAffsCPUs: 0\nprojects: []\n".as_slice())
    );
    ok(&old("values-noprojects.yaml"));
}

// ---------------------------------------------------------------- errors

#[test]
fn bad_int_knobs() {
    for (k, v) in [
        ("KUBERNETES_HOURS", "abc"),
        ("KUBERNETES_HOURS", "2"),
        ("KUBERNETES_HOURS", "31"),
        ("KUBERNETES_HOURS", "1.5"),
        ("KUBERNETES_HOURS", "99999999999999999999"),
        ("ALL_HOURS", "x"),
        ("ALL_HOURS", "2"),
        ("ALL_HOURS", "31"),
        ("GHA_OFFSET", "1"),
        ("GHA_OFFSET", "11"),
        ("GHA_OFFSET", ""),
        ("GHA_OFFSET", "-"),
        ("SYNC_HOURS", "0"),
        ("SYNC_HOURS", "7"),
        ("SYNC_HOURS", "six"),
        ("OFFSET_HOURS", "-85"),
        ("OFFSET_HOURS", "85"),
        ("OFFSET_HOURS", "+"),
        ("DAILY_AFFS_OFFSET_HOURS", "0"),
        ("DAILY_AFFS_OFFSET_HOURS", "24"),
        ("DAILY_AFFS_OFFSET_HOURS", "8h"),
    ] {
        if v.is_empty() {
            // empty means unset for every knob → fine
            ok(&Scenario::new().env(k, v));
            continue;
        }
        let needle = if v.parse::<i64>().is_ok() {
            k
        } else {
            "strconv.Atoi"
        };
        fatal(&Scenario::new().env(k, v), needle);
    }
    // MONTHLY widens the allowed range
    ok(&Scenario::new()
        .env("MONTHLY", "1")
        .env("KUBERNETES_HOURS", "48"));
    fatal(
        &Scenario::new()
            .env("MONTHLY", "1")
            .env("KUBERNETES_HOURS", "49"),
        "KUBERNETES_HOURS must be from [3,48]",
    );
    fatal(
        &Scenario::new().env("ALL_HOURS", "31"),
        "ALL_HOURS must be from [3,30]",
    );
    fatal(
        &Scenario::new().env("SYNC_HOURS", "7"),
        "SYNC_HOURS must be from 1 to 6",
    );
    fatal(
        &Scenario::new().env("GHA_OFFSET", "11"),
        "GHA_OFFSET must be from [2,10]",
    );
    fatal(
        &Scenario::new().env("OFFSET_HOURS", "85"),
        "OFFSET_HOURS must be from [-84,84]",
    );
    fatal(
        &Scenario::new().env("DAILY_AFFS_OFFSET_HOURS", "24"),
        "DAILY_AFFS_OFFSET_HOURS must be from [1,23]",
    );
}

#[test]
fn bad_algo_knobs() {
    fatal(
        &Scenario::new().env("SPLIT_ALGO", "linear"),
        "SPLIT_ALGO must be one of",
    );
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "abc"),
        "strconv.ParseFloat",
    );
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "5"),
        "WEIGHT_POWER must be from 0.0 to 4.0",
    );
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "-0.1"),
        "WEIGHT_POWER must be from 0.0 to 4.0",
    );
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "4.0001"),
        "WEIGHT_POWER must be from 0.0 to 4.0",
    );
    // Bug 8: NaN used to pass the range check and crash later
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "nan"),
        "WEIGHT_POWER must be from 0.0 to 4.0",
    );
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "NaN"),
        "WEIGHT_POWER must be from 0.0 to 4.0",
    );
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "inf"),
        "WEIGHT_POWER must be from 0.0 to 4.0",
    );
    fatal(
        &Scenario::new().env("WEIGHT_POWER", "1e400"),
        "WEIGHT_POWER must be from 0.0 to 4.0",
    );
    // the checks happen after "read <file>" and in this order
    let r = fatal(
        &Scenario::new()
            .env("SPLIT_ALGO", "bad")
            .env("SYNC_HOURS", "0"),
        "SYNC_HOURS must be from 1 to 6",
    );
    assert_eq!(r.stdout(), "read values.yaml\n");
}

#[test]
fn bad_input() {
    fatal(&Scenario::new().values("values-baddomains.yaml"), "yaml:");
    fatal(&Scenario::new().values("values-malformed.yaml"), "yaml:");
    fatal(&Scenario::new().values("values-notalist.yaml"), "yaml:");
    let sc = Scenario {
        values: None,
        ..Scenario::new()
    };
    let r = fatal(&sc, "open values.yaml: no such file or directory");
    assert_eq!(r.stdout(), "");
    // output directory missing
    let r = compare(&Scenario::new().args(&["values.yaml", "no/such/dir/out.yaml"]));
    assert_eq!(r.1.outcome.code, Some(2));
    assert!(r.1.stdout().ends_with("cronjobs\n"), "{}", r.1.stdout());
}

#[test]
fn usage() {
    // argv[0] is printed verbatim, so both binaries are run as ./splitcrons from their own directory
    let run_usage = |bin: &Path, args: &[&str]| -> Outcome {
        let tmp = tempfile::tempdir().unwrap();
        install_executable(bin, &tmp.path().join("splitcrons"));
        let mut inv = Invocation::new().cwd(tmp.path());
        for a in args {
            inv = inv.arg(*a);
        }
        compat::run(Path::new("./splitcrons"), &inv)
    };
    for args in [&[][..], &["values.yaml"][..]] {
        let rust = run_usage(&rust_bin(), args);
        assert_eq!(rust.code, Some(0));
        assert_eq!(
            rust.stdout_str(),
            "usage: ./splitcrons path/to/devstats-helm/values.yaml new-values.yaml\n"
        );
        assert_eq!(rust.stderr_str(), "");
        if let Some(go) = go_bin() {
            let go = run_usage(&go, args);
            assert_eq!(go, rust);
        }
    }
}

/// Real `devstats-helm` values file (sibling checkout, not part of this repo):
/// the rewritten YAML must be byte-identical.
#[test]
fn real_values_yaml_if_available() {
    let real = compat::repo_root()
        .parent()
        .map(|p| p.join("devstats-helm/devstats-helm/values.yaml"))
        .filter(|p| p.is_file());
    let Some(real) = real else {
        eprintln!("[compat] ../devstats-helm/devstats-helm/values.yaml not found — skipping");
        return;
    };
    let sc = Scenario::new().values_path(real).no_cluster();
    let r = ok(&sc.clone().env("NEVER_PATCH", "1"));
    assert!(r.stdout().contains("alive projects"));
    ok(&sc.clone().env("NEVER_PATCH", "1").env("OLD_ALGORITHM", "1"));
    ok(&sc.clone().env("NEVER_PATCH", "1").env("MONTHLY", "1"));
    ok(&sc.clone().env("ONLY_SUSPEND", "1"));
    ok(&sc.clone().env("PATCH_ENV", "MaxHist,NCPUs,AffSkipTemp"));
}
