//! Go ⇄ Rust compatibility tests for `devstats`.
//!
//! Every case runs the Go binary and the Rust binary in their own scratch
//! "checkout" (a temporary directory holding `projects.yaml` and fake
//! `get_repos` / `gha2db_sync` / `website_data` shell scripts that record
//! their arguments and the environment they were given — and fail on demand)
//! against their own set of scratch project databases
//! (`dbtest_devstats_<case>_<go|rs>_<project>`, each with the `gha_computed`
//! table of `structure` and the flag rows the case needs). Compared: exit
//! code, stdout (durations, database names and the running-flag age masked),
//! stderr (the `time.Now()` prefix of the error lines masked, `Error: '…'`
//! lines of fatal errors), the recorded command invocations and the final
//! `gha_computed` rows of every database.
//!
//! Needs a PostgreSQL server (`test.sh` finds one; skipped otherwise).

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("devstats")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_devstats"))
}

/// `gha_computed` as `structure` creates it.
const DDL: &str =
    "create table gha_computed(metric text not null, dt timestamp not null, primary key(metric, dt))";
/// The `provisioned` flag (UTC wall time, like production servers store it).
const PROVISIONED: &str =
    "insert into gha_computed(metric, dt) values('provisioned', now() at time zone 'UTC')";

/// A `devstats_running` flag `age` old (`age` is a PostgreSQL interval).
fn running_flag(age: &str) -> String {
    format!(
        "insert into gha_computed(metric, dt) values('devstats_running', (now() at time zone 'UTC') - interval '{age}')"
    )
}

/// The default test `projects.yaml`: `{db:<key>}` becomes the side's database
/// name of that project (created when the case lists the key in `dbs`).
const YAML: &str = r#"---
projects:
  p1:
    name: Project One
    command_line: [gha2db, '2015-01-01', 0, today, now, 'org1']
    psql_db: {db:p1}
    order: 1
    start_date: 2015-01-01
    main_repo: org1/repo1
    env:
      TESTVAR_A: alpha
      TESTVAR_N: 4
  p2:
    name: Project Two
    command_line: [gha2db, '2016-01-01', 0, today, now, 'org2']
    psql_db: {db:p2}
    order: 2
  p3:
    name: Project Three (disabled)
    psql_db: {db:p3}
    order: 3
    disabled: true
  p4:
    name: Project Four
    psql_db: {db:p4}
    order: 4
    env:
      TESTVAR_B: 'with space'
      GHA2DB_PROJECT: overridden-by-project-env
"#;

/// Which `projects.yaml` a case uses.
#[derive(Clone)]
enum Yaml {
    /// A template with `{db:key}` placeholders.
    Template(&'static str),
    /// The real `cncf/devstats` `projects.yaml` (272 projects).
    Real,
    /// No file at all.
    Missing,
}

/// Extra shell appended to a fake command (after the recording), e.g. `exit 3`.
#[derive(Clone, Default)]
struct Scripts {
    get_repos: &'static str,
    gha2db_sync: &'static str,
    website_data: &'static str,
}

struct Case {
    name: &'static str,
    yaml: Yaml,
    /// File name the yaml is written to.
    yaml_file: &'static str,
    /// Project keys whose database is created (with `gha_computed`).
    dbs: Vec<&'static str>,
    /// Per project extra seed SQL (after the DDL).
    seeds: Vec<(&'static str, String)>,
    env: Vec<(&'static str, String)>,
    scripts: Scripts,
    /// Pre-create the PID file.
    pre_pid: bool,
    /// `GHA2DB_LOCAL` + `GHA2DB_LOCAL_CMD` (otherwise `GHA2DB_DATADIR` and
    /// commands from `$PATH`).
    local: bool,
    /// Compare the `Error: '…'` lines of fatal errors.
    compare_errors: bool,
    /// Projects with a fractional `sync_probabilty` (randomly skipped): their
    /// lines and invocations are left out of the comparison.
    ignore_projects: Vec<&'static str>,
}

impl Case {
    fn new(name: &'static str) -> Self {
        Case {
            name,
            yaml: Yaml::Template(YAML),
            yaml_file: "projects.yaml",
            dbs: vec!["p1", "p2", "p4"],
            seeds: Vec::new(),
            env: vec![("GHA2DB_GETREPOSSKIP", "1".to_string())],
            scripts: Scripts::default(),
            pre_pid: false,
            local: true,
            compare_errors: true,
            ignore_projects: Vec::new(),
        }
    }
    fn ignore_projects(mut self, p: &[&'static str]) -> Self {
        self.ignore_projects = p.to_vec();
        self
    }
    fn env(mut self, k: &'static str, v: &str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self.env.push((k, v.to_string()));
        self
    }
    fn no_env(mut self, k: &str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self
    }
    fn yaml(mut self, y: Yaml) -> Self {
        self.yaml = y;
        self
    }
    fn dbs(mut self, dbs: &[&'static str]) -> Self {
        self.dbs = dbs.to_vec();
        self
    }
    fn seed(mut self, proj: &'static str, sql: &str) -> Self {
        self.seeds.push((proj, sql.to_string()));
        self
    }
    /// `provisioned` in every database of the case.
    fn provisioned(mut self) -> Self {
        for db in self.dbs.clone() {
            self = self.seed(db, PROVISIONED);
        }
        self
    }
    fn scripts(mut self, s: Scripts) -> Self {
        self.scripts = s;
        self
    }
    fn pre_pid(mut self) -> Self {
        self.pre_pid = true;
        self
    }
    fn not_local(mut self) -> Self {
        self.local = false;
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
    fn pid_root(&self) -> String {
        format!("g2r_devstats_{}", self.name)
    }
    fn pid_file(&self) -> PathBuf {
        PathBuf::from(format!("/tmp/{}.pid", self.pid_root()))
    }
}

/// One side of a case.
struct Side {
    dir: TempDir,
    dbs: BTreeMap<&'static str, TestDb>,
    /// Database name prefix of the side (masked in the outputs).
    prefix: String,
    out: Outcome,
    /// Was the PID file left behind by the run?
    pid_left: bool,
    ignore_projects: Vec<&'static str>,
}

impl Side {
    /// The recorded fake command invocations (without the randomly skipped
    /// projects').
    fn record(&self) -> String {
        let text = fs::read_to_string(self.dir.path().join("calls.log")).unwrap_or_default();
        text.split_inclusive("--\n")
            .filter(|block| {
                !self
                    .ignore_projects
                    .iter()
                    .any(|p| block.lines().any(|l| l == format!("GHA2DB_PROJECT={p}")))
            })
            .collect()
    }
    /// Is `line` about one of the randomly skipped projects?
    fn ignored_line(&self, line: &str) -> bool {
        self.ignore_projects.iter().any(|p| {
            line.ends_with(&format!(" {p}"))
                && (line.starts_with("Skipping #") || line.starts_with("Syncing #"))
                || line.starts_with(&format!("Synced {p}, took: "))
        })
    }
    fn stdout(&self) -> String {
        self.out.stdout_str()
    }
    fn mask(&self, s: &str) -> String {
        s.replace(&self.prefix, "<dbs>_")
    }
    /// `metric` values of `gha_computed` in every database, in order.
    fn flags(&self) -> BTreeMap<&'static str, Vec<String>> {
        let mut res = BTreeMap::new();
        for (proj, db) in &self.dbs {
            let con = db.conn();
            let snap = cpg::snapshot(&con, "select metric from gha_computed order by metric", &[]);
            con.close();
            res.insert(*proj, snap.column(0));
        }
        res
    }
}

/// The fake command script: records its name, arguments and the interesting
/// part of its environment, whether the PID file names its parent, then runs
/// the case's extra shell.
fn script(name: &str, extra: &str) -> String {
    format!(
        r#"#!/bin/sh
{{
  printf '%s' '{name}'
  for a in "$@"; do printf ' [%s]' "$a"; done
  printf '\n'
  env | grep -E '^(GHA2DB_[A-Z0-9_]*|PG_DB|ENV_SET|TESTVAR_[A-Z0-9_]*)=' \
    | grep -v -E '^GHA2DB_(SKIPLOG|SKIPTIME|LOCAL|LOCAL_CMD|PID_FILE_ROOT|GETREPOSSKIP|DEBUG|CMDDEBUG|WEBSITEDATA|CHECK_[A-Z_]*|SET_RUNNING_FLAG|MAX_RUNNING_FLAG_AGE|SKIP_PIDFILE|PROJECTS_OVERRIDE|PROJECTS_YAML|DATADIR)=' \
    | LC_ALL=C sort
  pf="/tmp/$GHA2DB_PID_FILE_ROOT.pid"
  if [ -f "$pf" ]; then
    if [ "$(cat "$pf")" = "$PPID" ]; then echo 'pidfile=parent'; else echo 'pidfile=other'; fi
  else
    echo 'pidfile=none'
  fi
  printf -- '--\n'
}} >> "$G2R_RECORD"
{extra}
"#
    )
}

fn write_exec(dir: &Path, name: &str, content: &str) {
    let p = dir.join(name);
    fs::write(&p, content).unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
}

/// Prepare the side's checkout and databases and run `bin`.
fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let prefix = format!("dbtest_devstats_{}_{}_", case.name, suffix);
    let mut dbs = BTreeMap::new();
    for proj in &case.dbs {
        let db = TestDb::fresh(&format!("devstats_{}_{}_{}", case.name, suffix, proj))?;
        db.exec(DDL);
        for (p, sql) in &case.seeds {
            if p == proj {
                db.exec(sql);
            }
        }
        dbs.insert(*proj, db);
    }
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_devstats_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    let db_name = |key: &str| format!("{prefix}{key}");
    match &case.yaml {
        Yaml::Template(t) => {
            let mut text = t.to_string();
            while let Some(start) = text.find("{db:") {
                let end = text[start..].find('}').unwrap() + start;
                let key = text[start + 4..end].to_string();
                text.replace_range(start..=end, &db_name(&key));
            }
            fs::write(dir.path().join(case.yaml_file), text).unwrap();
        }
        Yaml::Real => {
            fs::copy(
                fixture("devstats/projects.yaml"),
                dir.path().join(case.yaml_file),
            )
            .unwrap();
        }
        Yaml::Missing => {}
    }
    write_exec(
        dir.path(),
        "get_repos",
        &script("get_repos", case.scripts.get_repos),
    );
    write_exec(
        dir.path(),
        "gha2db_sync",
        &script("gha2db_sync", case.scripts.gha2db_sync),
    );
    write_exec(
        dir.path(),
        "website_data",
        &script("website_data", case.scripts.website_data),
    );
    let pid_file = case.pid_file();
    let _ = fs::remove_file(&pid_file);
    if case.pre_pid {
        fs::write(&pid_file, "12345").unwrap();
    }

    // Environment: the test server (PG_DB = the first project database, the
    // one `ClearOrphanedLocks` uses), no logging, the case's variables.
    let ctx = cpg::test_ctx();
    let record = dir.path().join("calls.log").to_string_lossy().into_owned();
    let pid_root = case.pid_root();
    let dir_s = dir.path().to_string_lossy().into_owned();
    let data_dir = format!("{dir_s}/");
    let path = format!("{dir_s}:{}", std::env::var("PATH").unwrap_or_default());
    let mut env: Vec<(String, String)> = vec![
        ("PG_HOST".into(), ctx.pg_host.clone()),
        ("PG_PORT".into(), ctx.pg_port.clone()),
        ("PG_USER".into(), ctx.pg_user.clone()),
        ("PG_PASS".into(), ctx.pg_pass.clone()),
        ("PG_SSL".into(), ctx.pg_ssl.clone()),
        (
            "PG_DB".into(),
            case.dbs
                .first()
                .map(|p| db_name(p))
                .unwrap_or_else(|| cpg::GUARD_DB.to_string()),
        ),
        ("GHA2DB_SKIPLOG".into(), "1".into()),
        ("GHA2DB_SKIPTIME".into(), "1".into()),
        ("GHA2DB_PID_FILE_ROOT".into(), pid_root),
        ("G2R_RECORD".into(), record),
    ];
    if case.local {
        env.push(("GHA2DB_LOCAL".into(), "1".into()));
        env.push(("GHA2DB_LOCAL_CMD".into(), "1".into()));
    } else {
        env.push(("GHA2DB_DATADIR".into(), data_dir));
        env.push(("PATH".into(), path));
    }
    for (k, v) in &case.env {
        env.retain(|(key, _)| key != k);
        env.push((k.to_string(), v.clone()));
    }
    let env: Vec<(&'static str, &'static str)> =
        env.iter().map(|(k, v)| (leak(k), leak(v))).collect();
    let mut inv = Invocation::new().cwd(dir.path().to_path_buf());
    for (k, v) in env {
        inv = inv.env(k, v);
    }
    let out = run(bin, &inv);
    let pid_left = pid_file.exists();
    let _ = fs::remove_file(&pid_file);
    Some(Side {
        dir,
        dbs,
        prefix,
        out,
        pid_left,
        ignore_projects: case.ignore_projects.clone(),
    })
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// stdout with durations, the side's database names and the running flag
/// age masked.
fn normalize_stdout(side: &Side) -> Vec<String> {
    let text = side.mask(&mask_go_durations(&side.stdout()));
    text.lines()
        .filter(|l| !side.ignored_line(l))
        .map(|l| {
            if let (Some(a), Some(b)) = (l.find("' set, age "), l.find(", maximum allowed age: ")) {
                if a < b {
                    let a = a + "' set, age ".len();
                    let age = &l[a..b];
                    assert!(
                        devstats_compat::is_go_duration(age),
                        "not a Go duration in {l:?}: {age:?}"
                    );
                    return format!("{}<duration>{}", &l[..a], &l[b..]);
                }
            }
            l.to_string()
        })
        .collect()
}

/// The comparable stderr lines: the program's own error lines (their
/// `time.Now()` prefix `… m=+0.012: ` replaced by `<now>: `, durations and
/// database names masked) and, when `with_errors`, the `Error: '…'` lines of
/// fatal errors. Everything else there (Go's `ErrorType:` lines and stack
/// traces) is not reproduced.
fn normalize_stderr(side: &Side, with_errors: bool) -> Vec<String> {
    let text = side.mask(&mask_go_durations(&side.out.stderr_str()));
    text.lines()
        .filter_map(|l| {
            if let Some(m) = l.find(" m=+") {
                if let Some(c) = l[m..].find(": ") {
                    return Some(format!("<now>{}", &l[m + c..]));
                }
            }
            if with_errors && l.starts_with("Error: '") {
                return Some(l.to_string());
            }
            None
        })
        .collect()
}

/// Run both binaries and compare everything; returns the Rust side for
/// further assertions (`None` when the DB tests are skipped).
fn both(case: &Case) -> Option<Side> {
    let rust = run_side(&rust_bin(), case, "rs")?;
    if let Some(go) = go_bin() {
        let go = run_side(&go, case, "go").unwrap();
        let ctx = format!(
            "\ncase {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- go record:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}--- rust record:\n{}",
            case.name,
            case.env,
            go.out.code,
            go.stdout(),
            go.out.stderr_str(),
            go.record(),
            rust.out.code,
            rust.stdout(),
            rust.out.stderr_str(),
            rust.record(),
        );
        assert_eq!(go.out.code, rust.out.code, "exit code{ctx}");
        assert_eq!(
            normalize_stdout(&go),
            normalize_stdout(&rust),
            "stdout{ctx}"
        );
        assert_eq!(
            normalize_stderr(&go, case.compare_errors),
            normalize_stderr(&rust, case.compare_errors),
            "stderr{ctx}"
        );
        assert_eq!(
            go.mask(&go.record()),
            rust.mask(&rust.record()),
            "recorded command invocations{ctx}"
        );
        assert_eq!(go.flags(), rust.flags(), "gha_computed rows{ctx}");
        assert_eq!(go.pid_left, rust.pid_left, "PID file left over{ctx}");
    }
    Some(rust)
}

/// One recorded invocation.
#[derive(Debug, PartialEq, Eq)]
struct Call {
    cmd: String,
    env: Vec<String>,
    pidfile: String,
}

fn parse_record(rec: &str) -> Vec<Call> {
    rec.split("--\n")
        .filter(|b| !b.trim().is_empty())
        .map(|b| {
            let mut lines = b.lines();
            let cmd = lines.next().unwrap().to_string();
            let mut env = Vec::new();
            let mut pidfile = String::new();
            for l in lines {
                if let Some(p) = l.strip_prefix("pidfile=") {
                    pidfile = p.to_string();
                } else {
                    env.push(l.to_string());
                }
            }
            Call { cmd, env, pidfile }
        })
        .collect()
}

fn calls(side: &Side) -> Vec<Call> {
    parse_record(&side.mask(&side.record()))
}

fn env_of(call: &Call, key: &str) -> Option<String> {
    call.env
        .iter()
        .find_map(|e| e.strip_prefix(&format!("{key}=")).map(|v| v.to_string()))
}

fn errors_of(side: &Side) -> Vec<String> {
    side.out
        .stderr_str()
        .lines()
        .filter(|l| l.starts_with("Error: '"))
        .map(|l| side.mask(l))
        .collect()
}

// ---------------------------------------------------------------------------
// Plain syncs: ordering, environment, get_repos / website_data
// ---------------------------------------------------------------------------

#[test]
fn syncs_enabled_projects_in_order_with_their_env() {
    let side = both(&Case::new("plain")).unwrap();
    assert_eq!(side.out.code(), 0);
    let out = side.stdout();
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Syncing #1 p1",
            "Synced p1, took: <duration>",
            "Syncing #2 p2",
            "Synced p2, took: <duration>",
            "Syncing #4 p4",
            "Synced p4, took: <duration>",
            "Synced all projects in: <duration>",
        ],
        "{out}"
    );
    assert!(
        side.out.stderr_str().is_empty(),
        "{}",
        side.out.stderr_str()
    );
    let calls = calls(&side);
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].cmd, "gha2db_sync");
    assert_eq!(
        calls[0].env,
        [
            "ENV_SET=1",
            "GHA2DB_PROJECT=p1",
            "PG_DB=<dbs>_p1",
            "TESTVAR_A=alpha",
            "TESTVAR_N=4"
        ]
    );
    assert_eq!(
        calls[1].env,
        ["ENV_SET=1", "GHA2DB_PROJECT=p2", "PG_DB=<dbs>_p2"]
    );
    // the project's own env wins over the standard variables
    assert_eq!(
        calls[2].env,
        [
            "ENV_SET=1",
            "GHA2DB_PROJECT=overridden-by-project-env",
            "PG_DB=<dbs>_p4",
            "TESTVAR_B=with space"
        ]
    );
    // the PID file held the devstats PID while the commands ran and is gone now
    assert!(calls.iter().all(|c| c.pidfile == "parent"), "{calls:?}");
    assert!(!side.pid_left);
    // no flags were touched
    for (_, flags) in side.flags() {
        assert!(flags.is_empty());
    }
}

#[test]
fn get_repos_runs_first_with_its_env() {
    let side = both(&Case::new("getrepos").no_env("GHA2DB_GETREPOSSKIP")).unwrap();
    assert_eq!(side.out.code(), 0);
    let out = normalize_stdout(&side);
    assert_eq!(out[1], "Updating git repos for all projects");
    assert_eq!(out[2], "Updated git repos, took: <duration>");
    assert_eq!(out[3], "Syncing #1 p1");
    let calls = calls(&side);
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[0].cmd, "get_repos");
    // (PG_DB is inherited from the devstats environment)
    assert_eq!(
        calls[0].env,
        [
            "GHA2DB_FETCH_COMMITS_MODE=0",
            "GHA2DB_PROCESS_REPOS=1",
            "PG_DB=<dbs>_p1"
        ]
    );
}

#[test]
fn fetch_commits_mode_2_is_passed_to_get_repos() {
    let side = both(
        &Case::new("fetchmode2")
            .no_env("GHA2DB_GETREPOSSKIP")
            .env("GHA2DB_FETCH_COMMITS_MODE", "2"),
    )
    .unwrap();
    let c = calls(&side);
    assert_eq!(
        c[0].env,
        [
            "GHA2DB_FETCH_COMMITS_MODE=2",
            "GHA2DB_PROCESS_REPOS=1",
            "PG_DB=<dbs>_p1"
        ]
    );
    // mode 1 (the default) is passed as 0: only "missing + truncated" is forwarded
    let side = both(
        &Case::new("fetchmode1")
            .no_env("GHA2DB_GETREPOSSKIP")
            .env("GHA2DB_FETCH_COMMITS_MODE", "1"),
    )
    .unwrap();
    assert_eq!(
        calls(&side)[0].env,
        [
            "GHA2DB_FETCH_COMMITS_MODE=0",
            "GHA2DB_PROCESS_REPOS=1",
            "PG_DB=<dbs>_p1"
        ]
    );
}

#[test]
fn get_repos_failure_aborts_the_sync() {
    let side = both(
        &Case::new("getrepos_fail")
            .no_env("GHA2DB_GETREPOSSKIP")
            .scripts(Scripts {
                get_repos: "echo 'repos stdout'; echo 'repos stderr' >&2; exit 7",
                ..Scripts::default()
            }),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = normalize_stdout(&side);
    assert!(out.contains(&"repos stdout".to_string()), "{out:?}");
    assert!(out.contains(&"STDERR:".to_string()), "{out:?}");
    assert!(out.contains(&"repos stderr".to_string()), "{out:?}");
    assert!(
        out.contains(&"Error updating git repos (took <duration>): exit status 7".to_string()),
        "{out:?}"
    );
    assert_eq!(
        out.last().unwrap(),
        "There were sync errors, took: <duration>"
    );
    assert_eq!(
        normalize_stderr(&side, true),
        ["<now>: Error updating git repos (took <duration>): exit status 7"]
    );
    assert_eq!(calls(&side).len(), 1, "no project synced");
}

#[test]
fn a_failing_project_sync_does_not_stop_the_others() {
    let side = both(&Case::new("sync_fail").scripts(Scripts {
        gha2db_sync: r#"if [ "$GHA2DB_PROJECT" = "p2" ]; then echo "p2 says hi"; echo "p2 complains" >&2; exit 3; fi"#,
        ..Scripts::default()
    }))
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = normalize_stdout(&side);
    assert_eq!(
        out,
        [
            "Compiled None, commit: None on None using None",
            "Syncing #1 p1",
            "Synced p1, took: <duration>",
            "Syncing #2 p2",
            "p2 says hi",
            "",
            "STDERR:",
            "p2 complains",
            "",
            "Command, arguments, environment:",
            "[./gha2db_sync]",
            "map[ENV_SET:1 GHA2DB_PROJECT:p2 PG_DB:<dbs>_p2]",
            "Command and arguments:",
            "[./gha2db_sync]",
            "map[ENV_SET:1 GHA2DB_PROJECT:p2 PG_DB:<dbs>_p2]",
            "Error result for p2 (took <duration>): exit status 3",
            "Syncing #4 p4",
            "Synced p4, took: <duration>",
            // per-project failures are not "sync errors"
            "Synced all projects in: <duration>",
        ]
    );
    assert_eq!(
        normalize_stderr(&side, true),
        ["<now>: Error result for p2 (took <duration>): exit status 3"]
    );
    assert_eq!(calls(&side).len(), 3);
}

#[test]
fn website_data_runs_after_the_syncs() {
    let side = both(&Case::new("website").env("GHA2DB_WEBSITEDATA", "1")).unwrap();
    assert_eq!(side.out.code(), 0);
    let out = normalize_stdout(&side);
    let n = out.len();
    assert_eq!(out[n - 3], "Generating website data for all projects");
    assert_eq!(out[n - 2], "Generated website data, took: <duration>");
    assert_eq!(out[n - 1], "Synced all projects in: <duration>");
    let calls = calls(&side);
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[3].cmd, "website_data");
    // no environment of its own (PG_DB is inherited)
    assert_eq!(calls[3].env, ["PG_DB=<dbs>_p1"]);
}

#[test]
fn website_data_failure_is_a_sync_error() {
    let side = both(
        &Case::new("website_fail")
            .env("GHA2DB_WEBSITEDATA", "1")
            .scripts(Scripts {
                website_data: "exit 1",
                ..Scripts::default()
            }),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = normalize_stdout(&side);
    assert!(
        out.contains(&"Error generating website data (took <duration>): exit status 1".to_string()),
        "{out:?}"
    );
    assert_eq!(
        out.last().unwrap(),
        "There were sync errors, took: <duration>"
    );
    // sic: the stderr line says "website", not "website data"
    assert_eq!(
        normalize_stderr(&side, true),
        ["<now>: Error generating website (took <duration>): exit status 1"]
    );
}

#[test]
fn cmd_debug_pipes_the_command_output() {
    let side = both(
        &Case::new("cmddebug")
            .env("GHA2DB_CMDDEBUG", "2")
            .scripts(Scripts {
                gha2db_sync: r#"echo "hello from $GHA2DB_PROJECT""#,
                ..Scripts::default()
            }),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = side.stdout();
    assert!(out.contains("hello from p1\n"), "{out}");
    assert!(
        out.contains("hello from overridden-by-project-env\n"),
        "{out}"
    );
    assert!(
        out.contains("Environment Override: map[ENV_SET:1 GHA2DB_PROJECT:p2 PG_DB:"),
        "{out}"
    );
}

#[test]
fn commands_come_from_path_and_yaml_from_datadir_when_not_local() {
    let side = both(&Case::new("notlocal").not_local()).unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(calls(&side).len(), 3);
    assert_eq!(
        normalize_stdout(&side).last().unwrap(),
        "Synced all projects in: <duration>"
    );
}

// ---------------------------------------------------------------------------
// Project selection: disabled, overrides, ONLY, probability, custom yaml
// ---------------------------------------------------------------------------

#[test]
fn projects_override_enables_and_disables() {
    let side = both(
        &Case::new("override")
            .dbs(&["p1", "p2", "p3", "p4"])
            .env("GHA2DB_PROJECTS_OVERRIDE", "+p3,-p2"),
    )
    .unwrap();
    let names: Vec<String> = calls(&side)
        .iter()
        .filter_map(|c| env_of(c, "GHA2DB_PROJECT"))
        .collect();
    assert_eq!(
        names,
        ["p1", "p3", "overridden-by-project-env"],
        "{}",
        side.stdout()
    );
}

#[test]
fn only_restricts_the_projects() {
    let side = both(&Case::new("only").env("ONLY", " p4  p1 nosuch p3 ")).unwrap();
    let names: Vec<String> = calls(&side)
        .iter()
        .map(|c| env_of(c, "PG_DB").unwrap())
        .collect();
    assert_eq!(names, ["<dbs>_p1", "<dbs>_p4"]);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Syncing #1 p1",
            "Synced p1, took: <duration>",
            "Syncing #4 p4",
            "Synced p4, took: <duration>",
            "Synced all projects in: <duration>",
        ]
    );
}

const PROBABILITY_YAML: &str = r#"projects:
  never:
    psql_db: {db:never}
    order: 1
    sync_probabilty: 0
  always:
    psql_db: {db:always}
    order: 2
    sync_probabilty: 1
  nokey:
    psql_db: {db:nokey}
    order: 3
"#;

#[test]
fn sync_probability_zero_skips_one_always_syncs() {
    let side = both(
        &Case::new("probability")
            .yaml(Yaml::Template(PROBABILITY_YAML))
            .dbs(&["never", "always", "nokey"]),
    )
    .unwrap();
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Skipping #1 never",
            "Syncing #2 always",
            "Synced always, took: <duration>",
            "Syncing #3 nokey",
            "Synced nokey, took: <duration>",
            "Synced all projects in: <duration>",
        ]
    );
    assert_eq!(calls(&side).len(), 2);
}

const DUPLICATE_ORDER_YAML: &str = r#"projects:
  zeta:
    psql_db: {db:zeta}
    order: 2
  beta:
    psql_db: {db:beta}
    order: 2
  alpha:
    psql_db: {db:alpha}
    order: 1
"#;

/// Bug 19: projects sharing an `order` used to be reduced to one random
/// project listed twice.
#[test]
fn projects_sharing_an_order_are_all_synced_with_a_warning() {
    let side = both(
        &Case::new("duporder")
            .yaml(Yaml::Template(DUPLICATE_ORDER_YAML))
            .dbs(&["alpha", "beta", "zeta"]),
    )
    .unwrap();
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Warning: projects 'beta' and 'zeta' have the same order 2",
            "Syncing #1 alpha",
            "Synced alpha, took: <duration>",
            "Syncing #2 beta",
            "Synced beta, took: <duration>",
            "Syncing #2 zeta",
            "Synced zeta, took: <duration>",
            "Synced all projects in: <duration>",
        ]
    );
}

#[test]
fn custom_projects_yaml_name() {
    let mut case = Case::new("customyaml").env("GHA2DB_PROJECTS_YAML", "my_projects.yaml");
    case.yaml_file = "my_projects.yaml";
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(calls(&side).len(), 3);
}

#[test]
fn missing_projects_yaml_is_fatal() {
    let side = both(&Case::new("noyaml").yaml(Yaml::Missing).dbs(&[])).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'open ./projects.yaml: no such file or directory'"]
    );
    assert!(side.record().is_empty());
}

#[test]
fn malformed_projects_yaml_is_fatal() {
    let side = both(
        &Case::new("badyaml")
            .yaml(Yaml::Template("projects:\n  p1:\n    order: [1\n"))
            .dbs(&[])
            .code_only_errors(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(errors_of(&side).len(), 1);
    let side = both(
        &Case::new("badyaml2")
            .yaml(Yaml::Template("projects:\n  p1:\n    order: one\n"))
            .dbs(&[])
            .code_only_errors(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    let side = both(
        &Case::new("badyaml3")
            .yaml(Yaml::Template(
                "projects:\n  p1:\n    start_date: 2014-13-01\n",
            ))
            .dbs(&[])
            .code_only_errors(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
}

#[test]
fn empty_projects_yaml_syncs_nothing() {
    let side = both(
        &Case::new("emptyyaml")
            .yaml(Yaml::Template("---\n"))
            .dbs(&[])
            .no_env("GHA2DB_GETREPOSSKIP"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Updating git repos for all projects",
            "Updated git repos, took: <duration>",
            "Synced all projects in: <duration>",
        ]
    );
}

// ---------------------------------------------------------------------------
// The real projects.yaml
// ---------------------------------------------------------------------------

#[test]
fn real_projects_yaml_syncs_every_enabled_project_in_order() {
    // kubernetes, istio and all have `sync_probabilty: 0.99` — randomly
    // skipped, so left out of the Go ⇄ Rust comparison
    let side = both(
        &Case::new("real")
            .yaml(Yaml::Real)
            .dbs(&[])
            .ignore_projects(&["kubernetes", "istio", "all"]),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    let calls = calls(&side);
    // 272 projects, 18 disabled, 3 probabilistic ones ignored
    assert_eq!(calls.len(), 251, "{}", side.stdout());
    assert_eq!(env_of(&calls[0], "GHA2DB_PROJECT").unwrap(), "prometheus");
    assert_eq!(env_of(&calls[0], "PG_DB").unwrap(), "prometheus");
    assert_eq!(env_of(&calls[1], "GHA2DB_PROJECT").unwrap(), "fluentd");
    // the last one by `order` (255; `all` at 1255 is ignored)
    assert_eq!(
        env_of(calls.last().unwrap(), "GHA2DB_PROJECT").unwrap(),
        "sdc"
    );
    let all_calls =
        parse_record(&side.mask(&fs::read_to_string(side.dir.path().join("calls.log")).unwrap()));
    let k8s = all_calls
        .iter()
        .find(|c| env_of(c, "GHA2DB_PROJECT").as_deref() == Some("kubernetes"));
    if let Some(k8s) = k8s {
        // project env from the yaml is forwarded
        assert_eq!(env_of(k8s, "PG_DB").unwrap(), "gha");
        assert!(
            env_of(k8s, "GHA2DB_EXCLUDE_REPOS")
                .unwrap()
                .starts_with("kubernetes/api,kubernetes/apiextensions-apiserver,"),
            "{k8s:?}"
        );
    }
    let out = side.stdout();
    // bug 19 hits real data: two projects share order 245
    assert!(
        out.contains("Warning: projects 'agones' and 'kaischeduler' have the same order 245\n"),
        "{out}"
    );
    assert!(out.contains("Syncing #245 agones\n"), "{out}");
    assert!(out.contains("Syncing #245 kaischeduler\n"), "{out}");
    // disabled projects are not synced
    assert!(!out.contains("Syncing #3 opentracing\n"), "{out}");
    assert!(
        out.contains("Syncing #1 kubernetes\n") || out.contains("Skipping #1 kubernetes\n"),
        "{out}"
    );
}

#[test]
fn real_projects_yaml_with_only() {
    let side = both(
        &Case::new("real_only")
            .yaml(Yaml::Real)
            .dbs(&[])
            .env("ONLY", "prometheus linkerd")
            .env("GHA2DB_WEBSITEDATA", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let calls = calls(&side);
    assert_eq!(calls.len(), 3);
    assert_eq!(env_of(&calls[0], "GHA2DB_PROJECT").unwrap(), "prometheus");
    assert_eq!(env_of(&calls[1], "GHA2DB_PROJECT").unwrap(), "linkerd");
    assert_eq!(calls[2].cmd, "website_data");
}

// ---------------------------------------------------------------------------
// Provision flag
// ---------------------------------------------------------------------------

#[test]
fn provision_flag_present_everywhere_syncs() {
    let side = both(
        &Case::new("prov_ok")
            .provisioned()
            .env("GHA2DB_CHECK_PROVISION_FLAG", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(calls(&side).len(), 3);
    assert_eq!(
        normalize_stdout(&side).last().unwrap(),
        "Synced all projects in: <duration>"
    );
}

#[test]
fn provision_flag_missing_on_a_database_exits() {
    let side = both(
        &Case::new("prov_missing")
            .seed("p1", PROVISIONED)
            .seed("p4", PROVISIONED)
            .env("GHA2DB_CHECK_PROVISION_FLAG", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Missing provisioned flag on '<dbs>_p2' database and check provisioned flag is set",
            "Not all databases provisioned, pending: 1, exiting",
            "There were sync errors, took: <duration>",
        ]
    );
    assert!(side.record().is_empty());
}

/// Bug 17: a missing database used to crash the check (`sql: database is
/// closed`) instead of being counted.
#[test]
fn provision_check_counts_missing_databases() {
    let side = both(
        &Case::new("prov_nodb")
            .dbs(&["p1"])
            .seed("p1", PROVISIONED)
            .env("GHA2DB_CHECK_PROVISION_FLAG", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "No '<dbs>_p2' database, missing provisioning flag",
            "No '<dbs>_p4' database, missing provisioning flag",
            "Not all databases provisioned, pending: 2, exiting",
            "There were sync errors, took: <duration>",
        ]
    );
}

#[test]
fn provision_check_is_skipped_without_the_flag() {
    // no `provisioned` rows anywhere, but no check requested
    let side = both(&Case::new("prov_nocheck")).unwrap();
    assert_eq!(calls(&side).len(), 3);
}

// ---------------------------------------------------------------------------
// Running flag
// ---------------------------------------------------------------------------

#[test]
fn fresh_running_flag_exits() {
    let side = both(
        &Case::new("run_fresh")
            .seed("p2", &running_flag("1 hour"))
            .env("GHA2DB_CHECK_RUNNING_FLAG", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Running flag on '<dbs>_p2' set, age <duration>, maximum allowed age: 9h0m0s",
            "Running flag on '<dbs>_p2' set, exiting",
            "There were sync errors, took: <duration>",
        ]
    );
    let out = side.stdout();
    // (the test server's clock may be a few minutes off)
    assert!(out.contains("' set, age 1h"), "{out}");
    assert!(side.record().is_empty());
    assert_eq!(side.flags()["p2"], ["devstats_running"]);
}

#[test]
fn expired_running_flag_is_removed_and_the_sync_runs() {
    let side = both(
        &Case::new("run_expired")
            .seed("p1", &running_flag("10 hours"))
            .seed("p4", &running_flag("2 hours"))
            .env("GHA2DB_CHECK_RUNNING_FLAG", "1")
            .env("GHA2DB_MAX_RUNNING_FLAG_AGE", "90m"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Running flag on '<dbs>_p1' set, age <duration>, maximum allowed age: 1h30m0s",
            "Running flag on '<dbs>_p1' expired, removing (this may be due to some error)",
            "Running flag on '<dbs>_p1' force removed",
            "Running flag on '<dbs>_p4' set, age <duration>, maximum allowed age: 1h30m0s",
            "Running flag on '<dbs>_p4' expired, removing (this may be due to some error)",
            "Running flag on '<dbs>_p4' force removed",
            "Syncing #1 p1",
            "Synced p1, took: <duration>",
            "Syncing #2 p2",
            "Synced p2, took: <duration>",
            "Syncing #4 p4",
            "Synced p4, took: <duration>",
            "Synced all projects in: <duration>",
        ]
    );
    for (_, flags) in side.flags() {
        assert!(flags.is_empty());
    }
}

#[test]
fn running_flag_check_stops_at_a_missing_database() {
    let side = both(
        &Case::new("run_nodb")
            .dbs(&["p1", "p4"])
            .env("GHA2DB_CHECK_RUNNING_FLAG", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "No '<dbs>_p2' database, cannot check running flag",
            "There were sync errors, took: <duration>",
        ]
    );
}

#[test]
fn running_flag_in_the_future_counts_as_fresh() {
    // clock skew between servers: a negative age is still "≤ max age"
    let side = both(
        &Case::new("run_future")
            .seed("p1", &running_flag("-30 minutes"))
            .env("GHA2DB_CHECK_RUNNING_FLAG", "1"),
    )
    .unwrap();
    let out = side.stdout();
    assert!(out.contains("' set, age -"), "{out}");
    assert!(out.contains("set, exiting\n"), "{out}");
}

#[test]
fn running_flag_is_set_during_the_run_and_cleared() {
    let side = both(
        &Case::new("set_flag")
            .env("GHA2DB_SET_RUNNING_FLAG", "1")
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = normalize_stdout(&side);
    assert_eq!(
        &out[1..5],
        [
            "Setting running flag",
            "Set running flag on <dbs>_p1",
            "Set running flag on <dbs>_p2",
            "Set running flag on <dbs>_p4",
        ]
    );
    let n = out.len();
    assert_eq!(
        &out[n - 5..],
        [
            "Deleting running flag",
            "Cleared running flag on <dbs>_p1",
            "Cleared running flag on <dbs>_p2",
            "Cleared running flag on <dbs>_p4",
            "Synced all projects in: <duration>",
        ]
    );
    for (_, flags) in side.flags() {
        assert!(flags.is_empty(), "{flags:?}");
    }
}

#[test]
fn running_flag_set_is_silent_without_debug() {
    let side = both(&Case::new("set_flag_quiet").env("GHA2DB_SET_RUNNING_FLAG", "1")).unwrap();
    let out = side.stdout();
    assert!(!out.contains("running flag"), "{out}");
    for (_, flags) in side.flags() {
        assert!(flags.is_empty(), "{flags:?}");
    }
}

/// Bug 18: the deferred clearing used to retry (for ~67 minutes) on the
/// databases that do not exist.
#[test]
fn set_running_flag_with_a_missing_database_exits_and_clears_the_set_ones() {
    let side = both(
        &Case::new("set_flag_nodb")
            .dbs(&["p1", "p4"])
            .env("GHA2DB_SET_RUNNING_FLAG", "1")
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Setting running flag",
            "Set running flag on <dbs>_p1",
            "No '<dbs>_p2' database, cannot set running flag",
            "Set running flag on <dbs>_p4",
            "Not all databases present, missing: 1, exiting",
            "Deleting running flag",
            "Cleared running flag on <dbs>_p1",
            "Cleared running flag on <dbs>_p4",
            "There were sync errors, took: <duration>",
        ]
    );
    assert!(side.record().is_empty());
    for (_, flags) in side.flags() {
        assert!(flags.is_empty(), "{flags:?}");
    }
}

#[test]
fn all_flags_together_on_a_healthy_setup() {
    let side = both(
        &Case::new("all_flags")
            .provisioned()
            .env("GHA2DB_CHECK_PROVISION_FLAG", "1")
            .env("GHA2DB_CHECK_RUNNING_FLAG", "1")
            .env("GHA2DB_SET_RUNNING_FLAG", "1")
            .env("GHA2DB_WEBSITEDATA", "1")
            .no_env("GHA2DB_GETREPOSSKIP"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(calls(&side).len(), 5);
    for (_, flags) in side.flags() {
        assert_eq!(flags, ["provisioned"]);
    }
}

// ---------------------------------------------------------------------------
// PID file
// ---------------------------------------------------------------------------

#[test]
fn existing_pid_file_means_another_instance() {
    let case = Case::new("pid_exists").pre_pid();
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        normalize_stdout(&side),
        [
            "Compiled None, commit: None on None using None",
            "Another `devstats` instance is running, PID file '/tmp/g2r_devstats_pid_exists.pid' exists, exiting (not an error)",
            "There were sync errors, took: <duration>",
        ]
    );
    assert!(side.record().is_empty());
}

#[test]
fn skip_pidfile_ignores_an_existing_pid_file() {
    let case = Case::new("pid_skip")
        .pre_pid()
        .env("GHA2DB_SKIP_PIDFILE", "1");
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0);
    let calls = calls(&side);
    assert_eq!(calls.len(), 3);
    // the foreign PID file is neither used nor removed
    assert!(calls.iter().all(|c| c.pidfile == "other"), "{calls:?}");
}

#[test]
fn pid_file_removal_failure_is_fatal_after_the_flags_are_cleared() {
    let side = both(
        &Case::new("pid_removed")
            .env("GHA2DB_SET_RUNNING_FLAG", "1")
            .env("GHA2DB_DEBUG", "1")
            .scripts(Scripts {
                gha2db_sync: r#"rm -f "/tmp/$GHA2DB_PID_FILE_ROOT.pid""#,
                ..Scripts::default()
            }),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'remove /tmp/g2r_devstats_pid_removed.pid: no such file or directory'"]
    );
    // the deferred clearing ran before the fatal error
    let out = normalize_stdout(&side);
    assert!(
        out.contains(&"Cleared running flag on <dbs>_p4".to_string()),
        "{out:?}"
    );
    for (_, flags) in side.flags() {
        assert!(flags.is_empty(), "{flags:?}");
    }
}

// ---------------------------------------------------------------------------
// Orphaned locks and connection problems
// ---------------------------------------------------------------------------

#[test]
fn orphaned_locks_are_cleared_in_the_pg_db_database() {
    let side = both(&Case::new("locks").seed(
        "p1",
        "insert into gha_computed(metric, dt) values \
         ('affs_lock_old', now() - interval '17 hours'), \
         ('affs_lock_fresh', now() - interval '15 hours'), \
         ('giant_lock_old', now() - interval '41 hours'), \
         ('other', now() - interval '100 days')",
    ))
    .unwrap();
    assert_eq!(side.out.code(), 0);
    // PG_DB is p1: the affs locks older than 16 hours go; giant locks are
    // only cleared in the `devstats` database (which the test server lacks)
    assert_eq!(
        side.flags()["p1"],
        ["affs_lock_fresh", "giant_lock_old", "other"]
    );
}

#[test]
fn unreachable_server_is_fatal_when_a_flag_check_needs_it() {
    let side = both(
        &Case::new("unreachable")
            .env("PG_PORT", "1")
            .env("GHA2DB_CHECK_PROVISION_FLAG", "1")
            .code_only_errors(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    let errs = errors_of(&side);
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(errs[0].contains("connection refused"), "{errs:?}");
}

#[test]
fn unreachable_server_does_not_matter_without_flag_checks() {
    // ClearOrphanedLocks ignores its errors
    let side = both(&Case::new("unreachable_ok").env("PG_PORT", "1")).unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(calls(&side).len(), 3);
}
