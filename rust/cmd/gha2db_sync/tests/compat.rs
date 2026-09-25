//! Go ⇄ Rust compatibility tests for `gha2db_sync`.
//!
//! Every case runs the Go binary and the Rust binary in their own scratch
//! "checkout" (a temporary directory holding `projects.yaml`,
//! `metrics/<project>/metrics.yaml` and fake `gha2db` / `get_repos` /
//! `ghapi2db` / `structure` / `tags` / `columns` / `annotations` /
//! `calc_metric` / `vars` shell scripts that record their arguments and the
//! interesting part of their environment — and fail on demand) against their
//! own scratch project database (`dbtest_gha2db_sync_<case>_<go|rs>` with the
//! `gha_parsed`, `sevents_h`, `tquick_ranges` and `gha_computed` tables the
//! sync reads) and the shared `devstats` logs database (whose old `gha_logs`
//! rows the sync clears). Compared: exit code, stdout (durations, `time.Now()`
//! values and the database name masked), the `Error: '…'` lines of fatal
//! errors, the recorded command invocations (sorted when the case runs
//! histograms in parallel or randomizes the metric order) and whether the
//! seeded old log row was cleared.
//!
//! Which periods are due is a pure function of the previous (`sevents_h`) and
//! current sync dates plus the `gha_computed` markers, so the default seed
//! (2020) makes every period due; the marker cases seed `sevents_h` with the
//! current hour instead. Everything random in the Go program is pinned or
//! masked: `GHA2DB_SKIP_RAND` (metric order), `GHA2DB_COMPUTE_ALL` /
//! `GHA2DB_FORCE_PERIODS` / `always_recalc`, the current hour (`to`). A case
//! is re-run when the local hour changed between the two runs.
//!
//! Needs a PostgreSQL server (`test.sh` finds one; skipped otherwise).

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, mask_go_now, run, rust_binary, Invocation, Outcome,
};
use devstatscode::chrono::{DateTime, Local, Timelike, Utc};
use devstatscode::computed::period_computed_key;
use devstatscode::pg::SqlArg;
use devstatscode::time::{
    day_start, hour_start, month_start, quarter_start, week_start, year_start,
};
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("gha2db_sync")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_gha2db_sync"))
}

const COMMANDS: &[&str] = &[
    "gha2db",
    "get_repos",
    "ghapi2db",
    "reconcile_dbs",
    "structure",
    "tags",
    "columns",
    "annotations",
    "calc_metric",
    "vars",
];

/// The tables the sync reads, as `structure` / `tags` / `calc_metric` create
/// them (`gha_computed` holds the markers of successful `calc_metric` runs).
const DDL: &[&str] = &[
    "create table gha_parsed(dt timestamp not null, primary key(dt))",
    "create table sevents_h(time timestamp primary key, period text not null default '', value bigint)",
    "create table tquick_ranges(time timestamp primary key, quick_ranges_suffix text not null default '', quick_ranges_name text, quick_ranges_data text)",
    "create table gha_computed(metric text not null, dt timestamp not null, primary key(metric, dt))",
];
/// The default seed: GHA data parsed up to 2020-03-04 05:00, the `events_h`
/// series computed up to 2020-03-01 03:00 and three quick ranges.
const SEED: &[&str] = &[
    "insert into gha_parsed(dt) values('2020-03-04 03:00:00'), ('2020-03-04 05:00:00'), ('2020-03-04 04:00:00')",
    "insert into sevents_h(time, value) values('2020-03-01 01:00:00', 1), ('2020-03-01 03:00:00', 3), ('2020-03-01 02:00:00', 2)",
    "insert into tquick_ranges(time, quick_ranges_suffix, quick_ranges_name, quick_ranges_data) values\
 ('2020-01-01 00:00:03', 'a_0_1', 'v1.0 - v1.1', 'a_0_1;;2019-01-01 00:00:00;2019-06-01 00:00:00'),\
 ('2020-01-01 00:00:01', 'd7', 'Last week', 'd7;7 days;;'),\
 ('2020-01-01 00:00:02', 'a_1_n', 'v1.1 - now', 'a_1_n;;2019-06-01 00:00:00;2020-03-05 00:00:00')",
];

/// The default `metrics.yaml`: hourly / daily metrics (always due), one
/// disabled.
const METRICS: &str = r#"---
metrics:
  - name: Events hourly
    series_name_or_func: events_h
    sql: events
    periods: h
  - name: Daily stats
    series_name_or_func: multi_row_single_column
    sql: daily
    periods: d
    aggregate: 1,7
    skip: d7
    multi_value: true
    merge_series: dstats
    drop: sdstats
  - name: Disabled one
    series_name_or_func: single_row_multi_column
    sql: nope
    periods: h
    disabled: true
"#;

/// The default `projects.yaml` (used when a case runs without arguments).
const PROJECTS: &str = r#"---
projects:
  p1:
    name: Project One
    command_line: ["org1,org2", "repo1"]
    psql_db: p1
    order: 1
    start_date: 2019-05-01
    project_scale: 2.5
    env:
      TESTVAR_A: alpha
      TESTVAR_B: 'with space'
      GHA2DB_VARS_FN_YAML: custom_vars.yaml
  p2:
    name: Project Two
    command_line: ["orgx"]
    psql_db: p2
    order: 2
    project_scale: -1
  p3:
    name: Project Three
    command_line: []
    psql_db: p3
    order: 3
    env:
      'BAD=KEY': value
"#;

struct Case {
    name: &'static str,
    /// Command line arguments (`org[,org…] [repo[,repo…]]`); without them
    /// the project's `command_line` from `projects.yaml` is used.
    args: Vec<&'static str>,
    /// `GHA2DB_PROJECT` (empty → not set).
    project: &'static str,
    env: Vec<(&'static str, String)>,
    projects_yaml: Option<String>,
    projects_file: &'static str,
    metrics_yaml: Option<String>,
    /// Where the metrics yaml is written (relative to the checkout);
    /// `None` → the default `metrics/<project>/metrics.yaml`.
    metrics_file: Option<&'static str>,
    /// Create the tables and the default seed.
    tables: bool,
    seed: bool,
    /// Extra SQL run on the project database after the seed.
    extra_sql: Vec<String>,
    /// Extra shell appended to a fake command (after the recording).
    scripts: BTreeMap<&'static str, &'static str>,
    /// `GHA2DB_LOCAL` + `GHA2DB_LOCAL_CMD` (otherwise `GHA2DB_DATADIR` and
    /// commands from `$PATH`).
    local: bool,
    /// Compare the `Error: '…'` lines of fatal errors.
    compare_errors: bool,
    /// Compare stdout lines and recorded invocations sorted (parallel
    /// histograms / random metric order).
    sorted: bool,
    /// Run with no other case in parallel (see [`LOGS_LOCK`]).
    exclusive_logs: bool,
}

impl Case {
    fn new(name: &'static str) -> Self {
        Case {
            name,
            args: vec!["org1,org2", "repo1"],
            project: "p1",
            env: vec![("GHA2DB_SKIP_RAND", "1".to_string())],
            projects_yaml: Some(PROJECTS.to_string()),
            projects_file: "projects.yaml",
            metrics_yaml: Some(METRICS.to_string()),
            metrics_file: None,
            tables: true,
            seed: true,
            extra_sql: Vec::new(),
            scripts: BTreeMap::new(),
            local: true,
            compare_errors: true,
            sorted: false,
            exclusive_logs: false,
        }
    }
    fn args(mut self, a: &[&'static str]) -> Self {
        self.args = a.to_vec();
        self
    }
    fn project(mut self, p: &'static str) -> Self {
        self.project = p;
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
    fn metrics(mut self, yaml: &str) -> Self {
        self.metrics_yaml = Some(yaml.to_string());
        self
    }
    fn no_metrics(mut self) -> Self {
        self.metrics_yaml = None;
        self
    }
    fn metrics_file(mut self, rel: &'static str) -> Self {
        self.metrics_file = Some(rel);
        self
    }
    fn projects(mut self, yaml: Option<&str>) -> Self {
        self.projects_yaml = yaml.map(String::from);
        self
    }
    fn projects_file(mut self, f: &'static str) -> Self {
        self.projects_file = f;
        self
    }
    fn no_tables(mut self) -> Self {
        self.tables = false;
        self.seed = false;
        self
    }
    fn no_seed(mut self) -> Self {
        self.seed = false;
        self
    }
    fn sql(mut self, s: &str) -> Self {
        self.extra_sql.push(s.to_string());
        self
    }
    fn script(mut self, cmd: &'static str, extra: &'static str) -> Self {
        self.scripts.insert(cmd, extra);
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
    fn sorted(mut self) -> Self {
        self.sorted = true;
        self
    }
    fn exclusive_logs(mut self) -> Self {
        self.exclusive_logs = true;
        self
    }
    /// Skip everything that depends on the current hour / randomness apart
    /// from the metrics: no tags, annotations, columns.
    fn metrics_only(self) -> Self {
        self.env("GHA2DB_SKIP_TAGS", "1")
            .env("GHA2DB_SKIP_ANNOTATIONS", "1")
            .env("GHA2DB_SKIP_COLUMNS", "1")
    }
}

/// One side of a case.
struct Side {
    dir: TempDir,
    /// Keeps the scratch database alive (dropped with the side).
    _db: TestDb,
    /// Database name of the side (masked in the outputs).
    db_name: String,
    out: Outcome,
    /// Was the old log row seeded for this side cleared by the run?
    log_cleared: bool,
}

impl Side {
    /// The recorded fake command invocations.
    fn record(&self) -> String {
        fs::read_to_string(self.dir.path().join("calls.log")).unwrap_or_default()
    }
    fn stdout(&self) -> String {
        self.out.stdout_str()
    }
    fn mask(&self, s: &str) -> String {
        s.replace(&self.db_name, "<db>")
            .replace(&self.dir.path().to_string_lossy().into_owned(), "<dir>")
    }
}

/// The fake command script: records its name, arguments and the interesting
/// part of its environment (in one atomic append, the histograms run in
/// parallel), then runs the case's extra shell.
fn script(name: &str, extra: &str) -> String {
    format!(
        r#"#!/bin/sh
tmp="$G2R_RECORD.$$.tmp"
{{
  printf '%s' '{name}'
  for a in "$@"; do printf ' [%s]' "$a"; done
  printf '\n'
  env | grep -E '^(GHA2DB_[A-Z0-9_]*|PG_DB|ENV_SET|TESTVAR_[A-Z0-9_]*)=' \
    | grep -v -E '^GHA2DB_(SKIPLOG|SKIPTIME|LOCAL|LOCAL_CMD|DATADIR)=' \
    | LC_ALL=C sort
  printf -- '--\n'
}} > "$tmp"
cat "$tmp" >> "$G2R_RECORD"
rm -f "$tmp"
{extra}
"#
    )
}

fn write_exec(dir: &Path, name: &str, content: &str) {
    let p = dir.join(name);
    fs::write(&p, content).unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
}

/// `ClearDBLogs` deletes *every* old row of the shared `devstats.gha_logs`
/// table, so a case observing that its own seeded row was *not* cleared must
/// not overlap with any other case's run: such cases take this lock
/// exclusively, all the others share it.
static LOGS_LOCK: RwLock<()> = RwLock::new(());

/// Seed an old row in `devstats.gha_logs` (older than the default
/// `GHA2DB_MAXLOGAGE` of 1 week) and return its unique message.
fn seed_old_log(msg: &str) {
    let mut ctx = cpg::test_ctx();
    ctx.pg_db = devstatscode::consts::DEVSTATS.to_string();
    let con = devstatscode::pg::pg_conn(&ctx);
    con.exec(
        "insert into gha_logs(id, dt, prog, proj, run_dt, msg) values(0, now() - interval '30 days', 'g2r', 'g2r', now(), $1)",
        &[SqlArg::from(msg)],
    )
    .unwrap();
    con.close();
}

fn old_log_present(msg: &str) -> bool {
    let mut ctx = cpg::test_ctx();
    ctx.pg_db = devstatscode::consts::DEVSTATS.to_string();
    let con = devstatscode::pg::pg_conn(&ctx);
    let snap = cpg::snapshot(
        &con,
        "select count(*) from gha_logs where msg = $1",
        &[SqlArg::from(msg)],
    );
    con.close();
    snap.column(0) != ["0"]
}

/// Prepare the side's checkout and database and run `bin`.
fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    // a library fatal inside the test process must not sleep a minute
    std::env::set_var("NO_FATAL_DELAY", "1");
    cpg::ensure_logs_db()?;
    let db = TestDb::fresh(&format!("gha2db_sync_{}_{}", case.name, suffix))?;
    let db_name = db.name.clone();
    if case.tables {
        for stmt in DDL {
            db.exec(stmt);
        }
        if case.seed {
            for stmt in SEED {
                db.exec(stmt);
            }
        }
    }
    for stmt in &case.extra_sql {
        db.exec(stmt);
    }
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_gha2db_sync_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    if let Some(text) = &case.projects_yaml {
        fs::write(dir.path().join(case.projects_file), text).unwrap();
    }
    if let Some(text) = &case.metrics_yaml {
        let rel = match case.metrics_file {
            Some(rel) => rel.to_string(),
            None if case.project.is_empty() => "metrics/metrics.yaml".to_string(),
            None => format!("metrics/{}/metrics.yaml", case.project),
        };
        let p = dir.path().join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, text).unwrap();
    }
    for cmd in COMMANDS {
        let extra = case.scripts.get(cmd).copied().unwrap_or("");
        write_exec(dir.path(), cmd, &script(cmd, extra));
    }
    let log_msg = format!(
        "g2r gha2db_sync {} {} {}",
        case.name,
        suffix,
        std::process::id()
    );
    seed_old_log(&log_msg);

    // Environment: the test server and the scratch database, no logging,
    // the case's variables.
    let record = dir.path().join("calls.log").to_string_lossy().into_owned();
    let dir_s = dir.path().to_string_lossy().into_owned();
    let data_dir = format!("{dir_s}/");
    let path = format!("{dir_s}:{}", std::env::var("PATH").unwrap_or_default());
    let mut env: Vec<(String, String)> = db
        .env()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    env.push(("G2R_RECORD".into(), record));
    if !case.project.is_empty() {
        env.push(("GHA2DB_PROJECT".into(), case.project.into()));
    }
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
    for a in &case.args {
        inv = inv.arg(*a);
    }
    let out = run(bin, &inv);
    let log_cleared = !old_log_present(&log_msg);
    Some(Side {
        dir,
        _db: db,
        db_name,
        out,
        log_cleared,
    })
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// stdout with durations, `time.Now()` values and the database name masked.
fn normalize_stdout(side: &Side, sorted: bool) -> Vec<String> {
    let text = side.mask(&mask_go_now(&mask_go_durations(&side.stdout())));
    let mut lines: Vec<String> = text.lines().map(String::from).collect();
    if sorted {
        lines.sort();
    }
    lines
}

/// The comparable stderr lines: `PqError:` lines and, when `with_errors`,
/// the `Error: '…'` lines of fatal errors (masked like stdout). Everything
/// else there (Go's `ErrorType:` lines and stack traces) is not reproduced.
fn normalize_stderr(side: &Side, with_errors: bool) -> Vec<String> {
    let text = side.mask(&mask_go_now(&side.out.stderr_str()));
    text.lines()
        .filter_map(|l| {
            if l.starts_with("PqError: ")
                || l.starts_with("Error executing ghapi2db: ")
                || l.starts_with("Error executing reconcile_dbs: ")
            {
                return Some(l.to_string());
            }
            if with_errors && l.starts_with("Error: '") {
                return Some(l.to_string());
            }
            None
        })
        .collect()
}

/// The recorded invocations as comparable blocks.
fn normalize_record(side: &Side, sorted: bool) -> Vec<String> {
    let text = side.mask(&side.record());
    let mut blocks: Vec<String> = text
        .split("--\n")
        .filter(|b| !b.trim().is_empty())
        .map(String::from)
        .collect();
    if sorted {
        blocks.sort();
    }
    blocks
}

/// Run both binaries and compare everything; returns the Rust side for
/// further assertions (`None` when the DB tests are skipped). A case is
/// repeated when the local hour changed between the two runs (the sync's
/// `to` date and the hour-dependent decisions would differ).
fn both(case: &Case) -> Option<Side> {
    let mut exclusive = case.exclusive_logs;
    for attempt in 1..=4 {
        // poisoning (a failed exclusive case) is irrelevant for a unit lock
        let _shared;
        let _excl;
        if exclusive {
            _excl = LOGS_LOCK.write().unwrap_or_else(|e| e.into_inner());
        } else {
            _shared = LOGS_LOCK.read().unwrap_or_else(|e| e.into_inner());
        }
        let hour_before = Local::now().hour();
        let rust = run_side(&rust_bin(), case, "rs")?;
        let Some(go_path) = go_bin() else {
            return Some(rust);
        };
        let go = run_side(&go_path, case, "go").unwrap();
        if Local::now().hour() != hour_before && attempt < 4 {
            eprintln!(
                "[compat] local hour changed during case {:?}, re-running",
                case.name
            );
            continue;
        }
        if go.log_cleared != rust.log_cleared && !exclusive && attempt < 4 {
            eprintln!(
                "[compat] gha_logs clearing observation of case {:?} raced with a parallel case, re-running exclusively",
                case.name
            );
            exclusive = true;
            continue;
        }
        let ctx = format!(
            "\ncase {:?} args {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- go record:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}--- rust record:\n{}",
            case.name,
            case.args,
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
            normalize_stdout(&go, case.sorted),
            normalize_stdout(&rust, case.sorted),
            "stdout{ctx}"
        );
        assert_eq!(
            normalize_stderr(&go, case.compare_errors),
            normalize_stderr(&rust, case.compare_errors),
            "stderr{ctx}"
        );
        assert_eq!(
            normalize_record(&go, case.sorted),
            normalize_record(&rust, case.sorted),
            "recorded command invocations{ctx}"
        );
        assert_eq!(
            go.log_cleared, rust.log_cleared,
            "old gha_logs row cleared{ctx}"
        );
        return Some(rust);
    }
    unreachable!()
}

/// One recorded invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Call {
    /// `name [arg] [arg]…`
    cmd: String,
    env: Vec<String>,
}

impl Call {
    fn name(&self) -> &str {
        self.cmd.split(' ').next().unwrap()
    }
    /// The arguments (`[…]` stripped).
    fn args(&self) -> Vec<String> {
        // arguments may themselves contain `]` (`series_name_map:map[a:b]`),
        // so split on the `] [` boundaries instead of the first `]`
        let rest = &self.cmd[self.name().len()..];
        match rest.strip_prefix(" [").and_then(|r| r.strip_suffix(']')) {
            None => Vec::new(),
            Some(inner) => inner.split("] [").map(String::from).collect(),
        }
    }
    fn env(&self, key: &str) -> Option<String> {
        self.env
            .iter()
            .find_map(|e| e.strip_prefix(&format!("{key}=")).map(|v| v.to_string()))
    }
}

fn calls(side: &Side) -> Vec<Call> {
    side.mask(&side.record())
        .split("--\n")
        .filter(|b| !b.trim().is_empty())
        .map(|b| {
            let mut lines = b.lines();
            let cmd = lines.next().unwrap().to_string();
            Call {
                cmd,
                env: lines.map(String::from).collect(),
            }
        })
        .collect()
}

fn names(side: &Side) -> Vec<String> {
    calls(side).iter().map(|c| c.name().to_string()).collect()
}

/// The `calc_metric` invocations: `(series, sql file, from, to, period, params)`.
fn metric_calls(side: &Side) -> Vec<Vec<String>> {
    calls(side)
        .iter()
        .filter(|c| c.name() == "calc_metric")
        .map(|c| c.args())
        .collect()
}

fn errors_of(side: &Side) -> Vec<String> {
    side.out
        .stderr_str()
        .lines()
        .filter(|l| l.starts_with("Error: '"))
        .map(|l| side.mask(l))
        .collect()
}

fn stdout_lines(side: &Side) -> Vec<String> {
    normalize_stdout(side, false)
}

/// `to` of the sync as `calc_metric` receives it: the current local hour.
fn now_ymdh() -> String {
    devstatscode::time::to_ymdh_date(Local::now().fixed_offset())
}

/// The current local wall clock (the sync's `to`).
fn now_wall() -> DateTime<Utc> {
    devstatscode::time::wall_as_utc(&Local::now())
}

/// A `timestamp` literal.
fn ts(dt: DateTime<Utc>) -> String {
    devstatscode::time::to_ymdhms_date(dt)
}

/// Seed the newest TSDB hour (`from`) at the current hour: nothing but `h*` is due by the
/// calendar rules, the `gha_computed` markers decide.
fn synced_this_hour(case: Case) -> Case {
    case.sql("delete from sevents_h").sql(&format!(
        "insert into sevents_h(time, value) values('{}', 1)",
        ts(hour_start(now_wall()))
    ))
}

/// Seed `gha_computed` markers: `(key, dt)`.
fn markers(mut case: Case, rows: &[(String, DateTime<Utc>)]) -> Case {
    for (key, dt) in rows {
        case = case.sql(&format!(
            "insert into gha_computed(metric, dt) values('{}', '{}')",
            key,
            ts(*dt)
        ));
    }
    case
}

// ---------------------------------------------------------------------------
// The default flow
// ---------------------------------------------------------------------------

#[test]
fn default_flow_runs_every_stage_in_order() {
    let side = both(&Case::new("default")).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    assert!(
        side.log_cleared,
        "ClearDBLogs must delete the old gha_logs row"
    );
    let calls = calls(&side);
    let mut names: Vec<&str> = calls.iter().map(|c| c.name()).collect();
    // tags / columns / annotations depend on the hour (checked separately)
    names.retain(|n| !["tags", "columns", "annotations"].contains(n));
    assert_eq!(
        names,
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "calc_metric",
            "calc_metric",
            "vars"
        ]
    );
    // gha2db: from the hour after the newest parsed one to now, orgs/repos
    let gha2db = &calls[0];
    let args = gha2db.args();
    assert_eq!(args[0], "2020-03-04");
    assert_eq!(args[1], "6");
    assert_eq!(args[4], "org1,org2");
    assert_eq!(args[5], "repo1");
    assert_eq!(gha2db.env("GHA2DB_PROJECT").as_deref(), Some("p1"));
    // get_repos: commits of the current project only
    let get_repos = &calls[1];
    assert_eq!(
        get_repos.env("GHA2DB_FETCH_COMMITS_MODE").as_deref(),
        Some("1")
    );
    assert_eq!(
        get_repos.env("GHA2DB_PROCESS_COMMITS").as_deref(),
        Some("1")
    );
    assert_eq!(
        get_repos.env("GHA2DB_PROJECTS_COMMITS").as_deref(),
        Some("p1")
    );
    // reconcile_dbs: between ghapi2db and structure, plain sync environment
    let reconcile = &calls[3];
    assert_eq!(reconcile.name(), "reconcile_dbs");
    assert!(reconcile.args().is_empty());
    assert_eq!(reconcile.env, calls[2].env);
    // structure: only the post-process SQLs
    let structure = &calls[4];
    assert_eq!(structure.env("GHA2DB_SKIPTABLE").as_deref(), Some("1"));
    assert_eq!(structure.env("GHA2DB_MGETC").as_deref(), Some("y"));
    // metrics: from the newest events_h point to now, skip_past, d7 skipped,
    // no drop without GHA2DB_ENABLE_METRICS_DROP
    let metrics = metric_calls(&side);
    assert_eq!(
        metrics,
        [
            vec![
                "events_h".to_string(),
                "./metrics/p1/events.sql".to_string(),
                "2020-03-01 3".to_string(),
                now_ymdh(),
                "h".to_string(),
                "skip_past".to_string(),
            ],
            vec![
                "multi_row_single_column".to_string(),
                "./metrics/p1/daily.sql".to_string(),
                "2020-03-01 3".to_string(),
                now_ymdh(),
                "d".to_string(),
                "multivalue,merge_series:dstats,skip_past".to_string(),
            ],
        ]
    );
    // vars with the default file name
    let vars = calls.last().unwrap();
    assert_eq!(vars.name(), "vars");
    assert_eq!(
        vars.env("GHA2DB_VARS_FN_YAML").as_deref(),
        Some("sync_vars.yaml")
    );
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"gha2db_sync.go: Running on: org1+org2/repo1".to_string()),
        "{out:?}"
    );
    assert!(
        out.contains(&"Using start dates: pg: 2020-03-04 6, tsdb: 2020-03-01 3".to_string()),
        "{out:?}"
    );
    assert!(
        out.contains(&"Quick ranges: [d7 a_1_n a_0_1], compute periods: map[]".to_string()),
        "{out:?}"
    );
    assert!(out.contains(&"Sync success".to_string()), "{out:?}");
    assert_eq!(out.last().unwrap(), "Time: <duration>");
}

#[test]
fn commands_come_from_path_and_yamls_from_datadir_when_not_local() {
    let side = both(&Case::new("notlocal").not_local()).unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    assert_eq!(metrics.len(), 2);
    assert_eq!(metrics[0][1], "<dir>/metrics/p1/events.sql");
}

#[test]
fn skip_pdb_skips_the_gha_stages_and_vars() {
    let side = both(
        &Case::new("skippdb")
            .env("GHA2DB_SKIPPDB", "1")
            .metrics_only()
            .exclusive_logs(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(names(&side), ["calc_metric", "calc_metric"]);
    assert!(
        !side.log_cleared,
        "ClearDBLogs must not run with GHA2DB_SKIPPDB"
    );
}

#[test]
fn skip_tsdb_skips_the_metrics() {
    let side = both(&Case::new("skiptsdb").env("GHA2DB_SKIPTSDB", "1")).unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "vars"
        ]
    );
}

#[test]
fn individual_stage_skips() {
    let side = both(
        &Case::new("skips")
            .env("GHA2DB_GETREPOSSKIP", "1")
            .env("GHA2DB_GHAPISKIP", "1")
            .env("GHA2DB_RECONCILESKIP", "1")
            .env("GHA2DB_SKIP_VARS", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        names(&side),
        ["gha2db", "structure", "calc_metric", "calc_metric"]
    );
}

#[test]
fn reset_tsdb_recomputes_from_the_start_date_with_tags_columns_annotations() {
    let side = both(&Case::new("reset").env("GHA2DB_RESETTSDB", "1")).unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "tags",
            "columns",
            "annotations",
            "calc_metric",
            "calc_metric",
            "columns",
            "vars"
        ]
    );
    let metrics = metric_calls(&side);
    // from the default start date, no skip_past
    assert_eq!(metrics[0][2], "2012-07-01 0");
    assert_eq!(metrics[0][5], "");
    assert_eq!(metrics[1][5], "multivalue,merge_series:dstats");
    let out = stdout_lines(&side);
    assert!(out.contains(&"Run tags finished, will also run columns later".to_string()));
    assert!(out.contains(&format!("TS range: 2012-07-01 0 - {}", now_ymdh())));
}

#[test]
fn reset_ranges_drops_skip_past_only() {
    let side = both(
        &Case::new("resetranges")
            .env("GHA2DB_RESETRANGES", "1")
            .metrics_only(),
    )
    .unwrap();
    let metrics = metric_calls(&side);
    assert_eq!(metrics[0][2], "2020-03-01 3");
    assert_eq!(metrics[0][5], "");
}

#[test]
fn forced_start_date_ignores_the_database() {
    let side = both(
        &Case::new("forcestart")
            .env("GHA2DB_STARTDT", "2021-02-03 04:00")
            .env("GHA2DB_STARTDT_FORCE", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Using start dates: pg: 2021-02-03 4, tsdb: 2021-02-03 4".to_string()),
        "{out:?}"
    );
    assert_eq!(
        calls(&side)[0].args()[..2],
        ["2021-02-03".to_string(), "4".to_string()]
    );
    assert_eq!(metric_calls(&side)[0][2], "2021-02-03 4");
}

#[test]
fn start_date_without_force_only_applies_to_empty_tables() {
    let side = both(
        &Case::new("startdt")
            .env("GHA2DB_STARTDT", "2021-02-03")
            .no_seed()
            .metrics_only(),
    )
    .unwrap();
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Using start dates: pg: 2021-02-03 0, tsdb: 2021-02-03 0".to_string()),
        "{out:?}"
    );
}

#[test]
fn missing_series_table_falls_back_to_the_start_date() {
    let side = both(
        &Case::new("noseries")
            .sql("drop table sevents_h")
            .metrics_only(),
    )
    .unwrap();
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Using start dates: pg: 2020-03-04 6, tsdb: 2012-07-01 0".to_string()),
        "{out:?}"
    );
}

#[test]
fn custom_last_series() {
    let side = both(
        &Case::new("lastseries")
            .env("GHA2DB_LASTSERIES", "other")
            .sql("create table sother(time timestamp primary key, value bigint)")
            .sql("insert into sother values('2019-12-31 23:00:00', 1)")
            .metrics_only(),
    )
    .unwrap();
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Using start dates: pg: 2020-03-04 6, tsdb: 2019-12-31 23".to_string()),
        "{out:?}"
    );
}

#[test]
fn missing_gha_parsed_table_is_fatal() {
    let side = both(&Case::new("noparsed").no_tables()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'pq: relation \"gha_parsed\" does not exist'"]
    );
    assert!(side.record().is_empty());
}

#[test]
fn missing_quick_ranges_table_is_fatal_after_the_gha_stages() {
    let side = both(
        &Case::new("noranges")
            .sql("drop table tquick_ranges")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'pq: relation \"tquick_ranges\" does not exist'"]
    );
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure"
        ]
    );
}

// ---------------------------------------------------------------------------
// Sub-command failures
// ---------------------------------------------------------------------------

#[test]
fn gha2db_failure_is_fatal() {
    let side = both(&Case::new("gha2dbfail").script("gha2db", "exit 3")).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(names(&side), ["gha2db"]);
    assert_eq!(errors_of(&side), ["Error: 'exit status 3'"]);
}

#[test]
fn get_repos_failure_is_fatal() {
    let side = both(&Case::new("getreposfail").script("get_repos", "exit 1")).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(names(&side), ["gha2db", "get_repos"]);
}

#[test]
fn ghapi2db_failure_is_reported_and_ignored() {
    let side = both(
        &Case::new("ghapifail")
            .script("ghapi2db", "exit 1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Error executing ghapi2db: exit status 1".to_string()),
        "{out:?}"
    );
    assert!(side
        .out
        .stderr_str()
        .contains("Error executing ghapi2db: exit status 1"));
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "calc_metric",
            "calc_metric",
            "vars"
        ]
    );
}

#[test]
fn reconcile_dbs_failure_is_reported_and_ignored() {
    let side = both(
        &Case::new("reconcilefail")
            .script("reconcile_dbs", "exit 1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Error executing reconcile_dbs: exit status 1".to_string()),
        "{out:?}"
    );
    assert!(side
        .out
        .stderr_str()
        .contains("Error executing reconcile_dbs: exit status 1"));
    assert!(
        out.contains(&"Reconcile with peer databases".to_string()),
        "{out:?}"
    );
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "calc_metric",
            "calc_metric",
            "vars"
        ]
    );
}

#[test]
fn reconcile_skip_skips_only_the_reconcile_step() {
    let side = both(
        &Case::new("reconcileskip")
            .env("GHA2DB_RECONCILESKIP", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        !out.contains(&"Reconcile with peer databases".to_string()),
        "{out:?}"
    );
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "structure",
            "calc_metric",
            "calc_metric",
            "vars"
        ]
    );
}

#[test]
fn reconcile_dbs_runs_between_ghapi2db_and_structure_with_the_sync_env() {
    let side = both(&Case::new("reconcileenv").metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0);
    let calls = calls(&side);
    let idx = calls
        .iter()
        .position(|c| c.name() == "reconcile_dbs")
        .expect("reconcile_dbs call");
    assert_eq!(calls[idx - 1].name(), "ghapi2db");
    assert_eq!(calls[idx + 1].name(), "structure");
    let rc = &calls[idx];
    assert!(rc.args().is_empty(), "{:?}", rc.args());
    // inherits the sync environment (project, database) like ghapi2db, nothing extra
    assert_eq!(
        rc.env("GHA2DB_PROJECT"),
        calls[idx - 1].env("GHA2DB_PROJECT")
    );
    assert_eq!(rc.env("PG_DB"), calls[idx - 1].env("PG_DB"));
    assert_eq!(rc.env, calls[idx - 1].env);
    assert_eq!(rc.env("GHA2DB_SKIPTABLE"), None);
    let out = stdout_lines(&side);
    let pos = |needle: &str| out.iter().position(|l| l == needle);
    let (a, b, c) = (
        pos("Update data from GitHub API"),
        pos("Reconcile with peer databases"),
        pos("Update structure"),
    );
    assert!(a.is_some() && b.is_some() && c.is_some(), "{out:?}");
    assert!(a < b && b < c, "{out:?}");
}

#[test]
fn structure_failure_is_fatal() {
    let side = both(&Case::new("structurefail").script("structure", "exit 1")).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure"
        ]
    );
}

#[test]
fn tags_failure_is_fatal() {
    let side = both(
        &Case::new("tagsfail")
            .env("GHA2DB_RESETTSDB", "1")
            .script("tags", "exit 1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(names(&side).last().unwrap(), "tags");
}

#[test]
fn annotations_failure_is_fatal() {
    let side = both(
        &Case::new("annotationsfail")
            .env("GHA2DB_RESETTSDB", "1")
            .script("annotations", "exit 1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(names(&side).last().unwrap(), "annotations");
}

#[test]
fn calc_metric_failure_is_fatal() {
    let side = both(
        &Case::new("metricfail")
            .script("calc_metric", r#"case "$2" in *daily*) exit 1;; esac"#)
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "calc_metric",
            "calc_metric"
        ]
    );
    assert_eq!(errors_of(&side), ["Error: 'exit status 1'"]);
}

#[test]
fn vars_failure_is_fatal() {
    let side = both(
        &Case::new("varsfail")
            .script("vars", "exit 1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert!(!stdout_lines(&side).contains(&"Sync success".to_string()));
}

#[test]
fn allow_metric_fail_env_tolerates_failures() {
    let side = both(
        &Case::new("allowfailenv")
            .env("GHA2DB_ALLOW_METRIC_FAIL", "1")
            .script("calc_metric", r#"case "$2" in *daily*) exit 1;; esac"#)
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.iter()
            .any(|l| l.starts_with("WARNING: {Name:Daily stats Periods:d ")
                && l.ends_with(" failed: exit status 1")),
        "{out:?}"
    );
    assert!(out.contains(&"Sync success".to_string()));
}

#[test]
fn allow_fail_metric_with_wait_after_fail() {
    let yaml = r#"---
metrics:
  - name: Fragile
    series_name_or_func: fragile
    sql: fragile_fail
    periods: h
    allow_fail: true
    wait_after_fail: 1
  - name: Fragile no wait
    series_name_or_func: fragile2
    sql: fragile2_fail
    periods: h
    allow_fail: true
  - name: Solid
    series_name_or_func: solid
    sql: solid
    periods: h
"#;
    let side = both(
        &Case::new("allowfail")
            .metrics(yaml)
            .script("calc_metric", r#"case "$2" in *fail*) exit 1;; esac"#)
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.iter()
            .any(|l| l.starts_with("WARNING: {Name:Fragile Periods:h ")
                && l.ends_with(" failed: waiting 1 seconds")),
        "{out:?}"
    );
    assert!(
        out.iter()
            .any(|l| l.starts_with("WARNING: {Name:Fragile Periods:h ")
                && l.ends_with(" failed: waited 1 seconds")),
        "{out:?}"
    );
    assert!(out.contains(
        &"There was at least one failure that requested wait (non-hist), waiting: 1 seconds"
            .to_string()
    ));
    assert!(out.contains(
        &"There was at least one failure that requested wait (non-hist), waited: 1 seconds"
            .to_string()
    ));
    assert_eq!(metric_calls(&side).len(), 3);
}

// ---------------------------------------------------------------------------
// Metric definitions
// ---------------------------------------------------------------------------

#[test]
fn compute_all_runs_every_period_and_aggregate() {
    let yaml = r#"---
metrics:
  - name: Many periods
    series_name_or_func: many
    sql: many
    periods: h,d,w,m,q,y
    aggregate: 1,7,24
    skip: h7,w7,m7,q7,y7,d24,w24,m24,q24,y24
    add_period_to_name: true
    desc: time_diff_as_string
    escape_value_name: true
    skip_escape_series_name: true
    custom_data: true
    custom_data_unique_time: true
    hll: true
    series_name_map:
      b: two
      a: one
"#;
    let side = both(
        &Case::new("computeall")
            .metrics(yaml)
            .env("GHA2DB_COMPUTE_ALL", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    let periods: Vec<String> = metrics.iter().map(|m| m[4].clone()).collect();
    assert_eq!(periods, ["h", "d", "w", "m", "q", "y", "d7", "h24"]);
    assert_eq!(metrics[0][0], "many_h");
    assert_eq!(metrics[6][0], "many_d7");
    assert_eq!(
        metrics[0][5],
        "escape_value_name,skip_escape_series_name,desc:time_diff_as_string,custom_data,custom_data_unique_time,series_name_map:map[a:one b:two],hll,skip_past"
    );
}

#[test]
fn force_periods_skips_the_others_with_a_message() {
    let yaml = r#"---
metrics:
  - name: Many periods
    series_name_or_func: many
    sql: many
    periods: h,d,w,m
    aggregate: 1,7
    skip: h7
  - name: Hist
    series_name_or_func: hist
    sql: hist
    periods: w,m
    histogram: true
"#;
    let side = both(
        &Case::new("forceperiods")
            .metrics(yaml)
            .env("GHA2DB_FORCE_PERIODS", "d:f,m:t,w7:f,zzz:x")
            .env("GHA2DB_ST", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    let periods: Vec<(String, String)> = metrics
        .iter()
        .map(|m| (m[0].clone(), m[4].clone()))
        .collect();
    // ComputePeriods is keyed by the bare period (no aggregate suffix): `d:f`
    // enables both `d` and `d7`, while `w7:f` matches nothing (`w` is looked up)
    assert_eq!(
        periods,
        [
            ("many".to_string(), "d".to_string()),
            ("many".to_string(), "d7".to_string()),
            ("hist".to_string(), "m".to_string()),
        ]
    );
    let out = stdout_lines(&side);
    assert!(out.contains(&"Quick ranges: [d7 a_1_n a_0_1], compute periods: map[d:map[false:{}] m:map[true:{}] w7:map[false:{}]]".to_string()), "{out:?}");
    assert!(out.contains(&"Skipping recalculating period \"w7\", hist false for date to <now>, computePeriods: map[d:map[false:{}] m:map[true:{}] w7:map[false:{}]], metric: Many periods".to_string()), "{out:?}");
    assert!(out.contains(&"Skipping recalculating period \"h\", hist false for date to <now>, computePeriods: map[d:map[false:{}] m:map[true:{}] w7:map[false:{}]], metric: Many periods".to_string()), "{out:?}");
    assert!(out.contains(&"Skipping recalculating period \"w\", hist true for date to <now>, computePeriods: map[d:map[false:{}] m:map[true:{}] w7:map[false:{}]], metric: Hist".to_string()), "{out:?}");
    assert!(
        out.contains(&"Now processing 1 histograms using ST version".to_string()),
        "{out:?}"
    );
}

#[test]
fn force_periods_with_reset_tsdb_still_skips() {
    let side = both(
        &Case::new("forcereset")
            .env("GHA2DB_FORCE_PERIODS", "d:f")
            .env("GHA2DB_RESETTSDB", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0][4], "d");
    assert_eq!(metrics[0][5], "multivalue,merge_series:dstats");
}

const MARKER_YAML: &str = r#"---
metrics:
  - name: Events hourly
    series_name_or_func: events_h
    sql: events
    periods: h
  - name: Daily
    series_name_or_func: daily
    sql: daily
    periods: d
  - name: Weekly
    series_name_or_func: weekly
    sql: weekly
    periods: w
  - name: Quarterly
    series_name_or_func: quarterly
    sql: quarterly
    periods: q
    aggregate: 1,2
  - name: Named
    series_name_or_func: named
    sql: named
    periods: y
    add_period_to_name: true
"#;

/// The `gha_computed` key of a `p1` metric (`GHA2DB_LOCAL`: `./metrics/p1/<sql>.sql`).
fn p1_key(series: &str, sql: &str, period: &str) -> String {
    period_computed_key(series, &format!("./metrics/p1/{sql}.sql"), period)
}

fn series_periods(side: &Side) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = metric_calls(side)
        .iter()
        .map(|m| (m[0].clone(), m[4].clone()))
        .collect();
    v.sort();
    v
}

#[test]
fn markers_decide_the_periods_not_due_by_the_calendar() {
    // Nothing but `h` is due by the calendar (synced this hour): a period is skipped only
    // when a `gha_computed` marker of its exact key was written since the period started
    let now = now_wall();
    let case = markers(
        synced_this_hour(Case::new("markers").metrics(MARKER_YAML).metrics_only()),
        &[
            (p1_key("daily", "daily", "d"), hour_start(now)),
            (p1_key("quarterly", "quarterly", "q2"), hour_start(now)),
            // stale: written before the current quarter started
            (
                p1_key("quarterly", "quarterly", "q"),
                quarter_start(now) - devstatscode::chrono::Duration::hours(1),
            ),
            // `add_period_to_name`: the key uses the suffixed series name
            (p1_key("named_y", "named", "y"), year_start(now)),
            (p1_key("named", "named", "y"), hour_start(now)),
        ],
    );
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    assert_eq!(
        series_periods(&side),
        [
            ("events_h".to_string(), "h".to_string()),
            ("quarterly".to_string(), "q".to_string()),
            ("weekly".to_string(), "w".to_string()),
        ]
    );
    let out = stdout_lines(&side);
    for expected in [
        "Period \"w\", hist false of metric Weekly was not computed successfully since the current period started, recalculating",
        "Period \"q\", hist false of metric Quarterly was not computed successfully since the current period started, recalculating",
        "Skipping recalculating period \"d\", hist false for date to <now>, computePeriods: map[], metric: Daily",
        "Skipping recalculating period \"q2\", hist false for date to <now>, computePeriods: map[], metric: Quarterly",
        "Skipping recalculating period \"y\", hist false for date to <now>, computePeriods: map[], metric: Named",
    ] {
        assert!(out.contains(&expected.to_string()), "{expected}\n{out:?}");
    }
    assert!(
        !out.iter()
            .any(|l| l.contains("hist false of metric Daily was not computed")),
        "{out:?}"
    );
}

#[test]
fn marker_repair_recalculates_from_the_previous_period_start() {
    // The first sync after a period boundary computes the previous period's final point together
    // with the current one; when it was lost (no marker since the period started) the repair must
    // start at the previous period start, not at the newest TSDB hour (already in the current period).
    // Histogram metrics are not time series: they keep the newest TSDB hour.
    let now = now_wall();
    let this_hour = hour_start(now);
    let week = week_start(now);
    let start_from = week - devstatscode::chrono::Duration::days(2);
    let yaml = format!(
        r#"---
metrics:
  - name: Events hourly
    series_name_or_func: events_h
    sql: events
    periods: h
  - name: Daily
    series_name_or_func: daily
    sql: daily
    periods: d
  - name: Weekly
    series_name_or_func: weekly
    sql: weekly
    periods: w
  - name: Weekly marked
    series_name_or_func: marked
    sql: marked
    periods: w
  - name: Quarterly
    series_name_or_func: quarterly
    sql: quarterly
    periods: q
    aggregate: 1,2
  - name: Weekly from
    series_name_or_func: wfrom
    sql: wfrom
    periods: w
    start_from: {}
  - name: Weekly last hours
    series_name_or_func: wlast
    sql: wlast
    periods: w
    last_hours: 5
  - name: Hist
    series_name_or_func: hist
    sql: hist
    periods: m
    histogram: true
"#,
        start_from.format("%Y-%m-%dT%H:%M:%SZ")
    );
    let case = markers(
        synced_this_hour(Case::new("repairfrom").metrics(&yaml).metrics_only()),
        &[(p1_key("marked", "marked", "w"), this_hour)],
    );
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    let ymdh = devstatscode::time::to_ymdh_date;
    let mut got: Vec<(String, String, String)> = metric_calls(&side)
        .iter()
        .map(|m| (m[0].clone(), m[4].clone(), m[2].clone()))
        .collect();
    got.sort();
    let prev_week = ymdh((week - devstatscode::chrono::Duration::days(7)).fixed_offset());
    let prev_quarter = ymdh(
        quarter_start(quarter_start(now) - devstatscode::chrono::Duration::seconds(1))
            .fixed_offset(),
    );
    let prev_day = ymdh((day_start(now) - devstatscode::chrono::Duration::days(1)).fixed_offset());
    let last_hours = ymdh((Local::now() - devstatscode::chrono::Duration::hours(5)).fixed_offset());
    assert_eq!(
        got,
        [
            ("daily".to_string(), "d".to_string(), prev_day.clone()),
            ("events_h".to_string(), "h".to_string(), now_ymdh()),
            ("hist".to_string(), "m".to_string(), now_ymdh()),
            (
                "quarterly".to_string(),
                "q".to_string(),
                prev_quarter.clone()
            ),
            (
                "quarterly".to_string(),
                "q2".to_string(),
                prev_quarter.clone()
            ),
            ("weekly".to_string(), "w".to_string(), prev_week.clone()),
            (
                "wfrom".to_string(),
                "w".to_string(),
                ymdh(start_from.fixed_offset())
            ),
            ("wlast".to_string(), "w".to_string(), last_hours),
        ]
    );
    let out = stdout_lines(&side);
    for expected in [
        format!("Period \"w\" of metric Weekly recalculated from {prev_week} (previous period start) instead of {}", now_ymdh()),
        format!("Period \"d\" of metric Daily recalculated from {prev_day} (previous period start) instead of {}", now_ymdh()),
        format!("Period \"q2\" of metric Quarterly recalculated from {prev_quarter} (previous period start) instead of {}", now_ymdh()),
        format!("Period \"w\" of metric Weekly from recalculated from {} (previous period start) instead of {}", ymdh(start_from.fixed_offset()), now_ymdh()),
        "Period \"m\", hist true of metric Hist was not computed successfully since the current period started, recalculating".to_string(),
        "Skipping recalculating period \"w\", hist false for date to <now>, computePeriods: map[], metric: Weekly marked".to_string(),
    ] {
        assert!(out.contains(&expected), "{expected}\n{out:?}");
    }
    assert!(
        !out.iter()
            .any(|l| l.contains("of metric Hist recalculated from")
                || l.contains("of metric Weekly marked recalculated from")
                || l.contains("of metric Events hourly recalculated from")),
        "{out:?}"
    );
}

#[test]
fn force_periods_and_compute_all_ignore_the_markers() {
    let now = now_wall();
    // GHA2DB_FORCE_PERIODS: exactly the listed periods, markers are neither consulted nor needed
    let case = markers(
        synced_this_hour(
            Case::new("forcemark")
                .metrics(MARKER_YAML)
                .env("GHA2DB_FORCE_PERIODS", "d:f")
                .metrics_only(),
        ),
        &[(p1_key("daily", "daily", "d"), hour_start(now))],
    );
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    assert_eq!(
        series_periods(&side),
        [("daily".to_string(), "d".to_string())]
    );
    let out = stdout_lines(&side);
    assert!(out.contains(&"Skipping recalculating period \"w\", hist false for date to <now>, computePeriods: map[d:map[false:{}]], metric: Weekly".to_string()), "{out:?}");
    assert!(
        !out.iter()
            .any(|l| l.contains("was not computed successfully")),
        "{out:?}"
    );

    // GHA2DB_COMPUTE_ALL: everything, markers or not
    let case = markers(
        synced_this_hour(
            Case::new("allmark")
                .metrics(MARKER_YAML)
                .env("GHA2DB_COMPUTE_ALL", "1")
                .metrics_only(),
        ),
        &[
            (p1_key("daily", "daily", "d"), hour_start(now)),
            (p1_key("weekly", "weekly", "w"), hour_start(now)),
        ],
    );
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    assert_eq!(
        series_periods(&side),
        [
            ("daily".to_string(), "d".to_string()),
            ("events_h".to_string(), "h".to_string()),
            ("named_y".to_string(), "y".to_string()),
            ("quarterly".to_string(), "q".to_string()),
            ("quarterly".to_string(), "q2".to_string()),
            ("weekly".to_string(), "w".to_string()),
        ]
    );
}

const RANGES_YAML: &str = r#"---
metrics:
  - name: Hist ranges
    series_name_or_func: hranges
    sql: hranges
    periods: d
    histogram: true
    annotations_ranges: true
"#;

/// Quick ranges of every class (`quick_ranges_data` as `annotations` writes it): the
/// past `a_0_1`, ranges ending now 10 days (d), 60 days (m), 200 days (q) and years (y)
/// long, one with an unknown start (d) and the plain `d7`.
fn quick_ranges_of_every_class(case: Case, now: DateTime<Utc>) -> Case {
    let tomorrow = ts(devstatscode::time::next_day_start(now));
    let ago = |days: i64| ts(now - devstatscode::chrono::Duration::days(days));
    case.sql("delete from tquick_ranges").sql(&format!(
        "insert into tquick_ranges(time, quick_ranges_suffix, quick_ranges_name, quick_ranges_data) values\
 ('2020-01-01 00:00:01', 'd7', 'Last week', 'd7;7 days;;'),\
 ('2020-01-01 00:00:02', 'a_0_1', 'v1.0 - v1.1', 'a_0_1;;2019-01-01 00:00:00;2019-06-01 00:00:00'),\
 ('2020-01-01 00:00:03', 'a_1_n', 'v1.1 - now', 'a_1_n;;{};{tomorrow}'),\
 ('2020-01-01 00:00:04', 'a_2_n', 'v1.2 - now', 'a_2_n;;{};{tomorrow}'),\
 ('2020-01-01 00:00:05', 'a_3_n', 'v1.3 - now', 'a_3_n;;{};{tomorrow}'),\
 ('2020-01-01 00:00:06', 'c_n', 'Since joining CNCF', 'c_n;;2019-06-01 00:00:00;{tomorrow}'),\
 ('2020-01-01 00:00:07', 'c_i_n', 'Since incubating', 'c_i_n;;;{tomorrow}')",
        ago(10),
        ago(60),
        ago(200),
    ))
}

/// The class period start of every quick range of `quick_ranges_of_every_class`.
fn range_period_starts(now: DateTime<Utc>) -> Vec<(&'static str, DateTime<Utc>)> {
    vec![
        ("d7", day_start(now)),
        ("a_0_1", day_start(now)),
        ("a_1_n", day_start(now)),
        ("a_2_n", month_start(now)),
        ("a_3_n", quarter_start(now)),
        ("c_n", year_start(now)),
        ("c_i_n", day_start(now)),
    ]
}

#[test]
fn quick_ranges_ending_now_follow_the_class_given_by_their_length() {
    let now = now_wall();
    let hour = devstatscode::chrono::Duration::hours(1);
    // markers written when each range's class period started: everything is skipped
    let rows: Vec<(String, DateTime<Utc>)> = range_period_starts(now)
        .into_iter()
        .map(|(sfx, dt)| (p1_key("hranges", "hranges", sfx), dt))
        .collect();
    let case = markers(
        quick_ranges_of_every_class(
            synced_this_hour(
                Case::new("qrclass")
                    .metrics(RANGES_YAML)
                    .env("GHA2DB_ST", "1")
                    .metrics_only(),
            ),
            now,
        ),
        &rows,
    );
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    assert!(
        series_periods(&side).is_empty(),
        "{:?}",
        series_periods(&side)
    );
    let out = stdout_lines(&side);
    assert!(
        out.contains(
            &"Quick ranges: [d7 a_0_1 a_1_n a_2_n a_3_n c_n c_i_n], compute periods: map[]"
                .to_string()
        ),
        "{out:?}"
    );
    for sfx in ["d7", "a_0_1", "a_1_n", "a_2_n", "a_3_n", "c_n", "c_i_n"] {
        let expected = format!("Skipping recalculating period \"{sfx}\", hist true for date to <now>, computePeriods: map[], metric: Hist ranges");
        assert!(out.contains(&expected), "{expected}\n{out:?}");
    }

    // markers written an hour before today started: the daily ranges are recalculated, the
    // monthly / quarterly / yearly ones only when their period started today
    let rows: Vec<(String, DateTime<Utc>)> = range_period_starts(now)
        .iter()
        .map(|(sfx, _)| (p1_key("hranges", "hranges", sfx), day_start(now) - hour))
        .collect();
    let case = markers(
        quick_ranges_of_every_class(
            synced_this_hour(
                Case::new("qrmixed")
                    .metrics(RANGES_YAML)
                    .env("GHA2DB_ST", "1")
                    .metrics_only(),
            ),
            now,
        ),
        &rows,
    );
    let side = both(&case).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    let mut expected: Vec<(String, String)> = range_period_starts(now)
        .into_iter()
        .filter(|(_, start)| *start >= day_start(now))
        .map(|(sfx, _)| ("hranges".to_string(), sfx.to_string()))
        .collect();
    expected.sort();
    assert!(expected.len() >= 4, "{expected:?}");
    assert_eq!(series_periods(&side), expected);
    let out = stdout_lines(&side);
    for (sfx, start) in range_period_starts(now) {
        let line = if start >= day_start(now) {
            format!("Period \"{sfx}\", hist true of metric Hist ranges was not computed successfully since the current period started, recalculating")
        } else {
            format!("Skipping recalculating period \"{sfx}\", hist true for date to <now>, computePeriods: map[], metric: Hist ranges")
        };
        assert!(out.contains(&line), "{line}\n{out:?}");
    }
}

#[test]
fn always_recalc_is_always_due() {
    let yaml = r#"---
metrics:
  - name: Yearly always
    series_name_or_func: yearly
    sql: yearly
    periods: y
    always_recalc: true
  - name: Yearly forced off
    series_name_or_func: yearly2
    sql: yearly2
    periods: y
"#;
    let side = both(
        &Case::new("alwaysrecalc")
            .metrics(yaml)
            .env("GHA2DB_FORCE_PERIODS", "d:f")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0][0], "yearly");
    let out = stdout_lines(&side);
    assert!(out.contains(&"Skipping recalculating period \"y\", hist false for date to <now>, computePeriods: map[d:map[false:{}]], metric: Yearly forced off".to_string()), "{out:?}");
}

#[test]
fn sqls_expand_to_one_metric_per_sql_with_drop_once() {
    let yaml = r#"---
metrics:
  - name: Multi
    series_name_or_func: multi
    sqls: [first, second, third]
    periods: h,d
    drop: smulti
  - name: Single
    series_name_or_func: single
    sql: single
    periods: h
    drop: ssingle
"#;
    let side = both(
        &Case::new("sqls")
            .metrics(yaml)
            .env("GHA2DB_ENABLE_METRICS_DROP", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    let got: Vec<(String, String, String)> = metrics
        .iter()
        .map(|m| (m[1].clone(), m[4].clone(), m[5].clone()))
        .collect();
    assert_eq!(
        got,
        [
            (
                "./metrics/p1/first.sql".to_string(),
                "h".to_string(),
                "skip_past,drop:smulti".to_string()
            ),
            (
                "./metrics/p1/first.sql".to_string(),
                "d".to_string(),
                "skip_past".to_string()
            ),
            (
                "./metrics/p1/second.sql".to_string(),
                "h".to_string(),
                "skip_past".to_string()
            ),
            (
                "./metrics/p1/second.sql".to_string(),
                "d".to_string(),
                "skip_past".to_string()
            ),
            (
                "./metrics/p1/third.sql".to_string(),
                "h".to_string(),
                "skip_past".to_string()
            ),
            (
                "./metrics/p1/third.sql".to_string(),
                "d".to_string(),
                "skip_past".to_string()
            ),
            (
                "./metrics/p1/single.sql".to_string(),
                "h".to_string(),
                "skip_past,drop:ssingle".to_string()
            ),
        ]
    );
}

#[test]
fn drop_is_ignored_without_enable_metrics_drop() {
    let side = both(&Case::new("nodrop").metrics_only()).unwrap();
    assert!(metric_calls(&side).iter().all(|m| !m[5].contains("drop:")));
}

#[test]
fn sql_and_sqls_together_is_fatal() {
    let yaml = r#"---
metrics:
  - name: Both
    series_name_or_func: both
    sql: one
    sqls: [two]
    periods: h
"#;
    let side = both(&Case::new("sqlsqls").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'you cannot use both 'sql' and 'sqls' fields''"]
    );
}

#[test]
fn drop_on_a_histogram_is_fatal_and_prints_the_metric() {
    let yaml = r#"---
metrics:
  - name: Hist drop
    series_name_or_func: hd
    sqls: [a, b]
    periods: h
    histogram: true
    drop: shd
    start_from: 2018-01-01T00:00:00Z
    env:
      GHA2DB_NCPUS?: 2
"#;
    let side = both(&Case::new("histdrop").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'you cannot use drop series property on histogram metrics: {Name:Hist drop Periods:h SeriesNameOrFunc:hd MetricSQL: MetricSQLs:[a b] AddPeriodToName:false Histogram:true Aggregate: Skip: Desc: MultiValue:false EscapeValueName:false SkipEscapeSeriesName:false AnnotationsRanges:false MergeSeries: CustomData:false CustomDataUniqueTime:false StartFrom:2018-01-01 00:00:00 +0000 UTC LastHours:0 SeriesNameMap:map[] EnvMap:map[GHA2DB_NCPUS?:2] Disabled:false Drop:shd Project: AllowFail:false WaitAfterFail:0 HLL:false AlwaysRecalc:false}'"]
    );
}

#[test]
fn project_include_and_exclude() {
    let yaml = r#"---
metrics:
  - name: Not for p1
    series_name_or_func: a
    sql: a
    periods: h
    project: '!p1'
  - name: Only p1 and p2
    series_name_or_func: b
    sql: b
    periods: h
    project: p1,p2
  - name: Only p2
    series_name_or_func: c
    sql: c
    periods: h
    project: p2
  - name: Not for p2
    series_name_or_func: d
    sql: d
    periods: h
    project: '!p2'
  - name: Everyone
    series_name_or_func: e
    sql: e
    periods: h
"#;
    let side = both(&Case::new("projects").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0);
    let series: Vec<String> = metric_calls(&side).iter().map(|m| m[0].clone()).collect();
    assert_eq!(series, ["b", "d", "e"]);
    let out = stdout_lines(&side);
    assert!(out.contains(&"Metric Not for p1 have project setting !p1 which is skipped for the current p1 project".to_string()), "{out:?}");
    assert!(
        out.contains(
            &"Metric Only p2 have project setting p2 which is skipped for the current p1 project"
                .to_string()
        ),
        "{out:?}"
    );
}

#[test]
fn only_metrics_and_skip_metrics_select_by_sql() {
    let side = both(
        &Case::new("onlymetrics")
            .env("GHA2DB_ONLY_METRICS", "daily,nope")
            .metrics_only(),
    )
    .unwrap();
    let series: Vec<String> = metric_calls(&side).iter().map(|m| m[0].clone()).collect();
    assert_eq!(series, ["multi_row_single_column"]);
    let side = both(
        &Case::new("skipmetrics")
            .env("GHA2DB_SKIP_METRICS", "daily")
            .metrics_only(),
    )
    .unwrap();
    let series: Vec<String> = metric_calls(&side).iter().map(|m| m[0].clone()).collect();
    assert_eq!(series, ["events_h"]);
}

#[test]
fn start_from_and_last_hours() {
    let yaml = r#"---
metrics:
  - name: From 2020-03-02
    series_name_or_func: later
    sql: later
    periods: h
    start_from: 2020-03-02T10:00:00Z
  - name: From the past
    series_name_or_func: earlier
    sql: earlier
    periods: h
    start_from: 2019-01-01T00:00:00Z
  - name: From the future
    series_name_or_func: future
    sql: future
    periods: h
    start_from: 2099-01-01T00:00:00Z
  - name: Last day
    series_name_or_func: lastday
    sql: lastday
    periods: h
    last_hours: 24
"#;
    let side = both(&Case::new("startfrom").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    let got: Vec<(String, String)> = metrics
        .iter()
        .map(|m| (m[0].clone(), m[2].clone()))
        .collect();
    let day_ago = devstatscode::time::to_ymdh_date(
        (Local::now() - devstatscode::chrono::Duration::hours(24)).fixed_offset(),
    );
    assert_eq!(
        got,
        [
            ("later".to_string(), "2020-03-02 10".to_string()),
            ("earlier".to_string(), "2020-03-01 3".to_string()),
            ("lastday".to_string(), day_ago),
        ]
    );
    let out = stdout_lines(&side);
    assert!(out.contains(&"Non-standard start date: 2099-01-01 00:00:00 +0000 UTC (used instead of 2020-03-01 03:00:00 +0000 +0000) is after end date <now>, skipping".to_string()), "{out:?}");
}

#[test]
fn start_from_with_last_hours_is_fatal() {
    let yaml = r#"---
metrics:
  - name: Both
    series_name_or_func: both
    sql: both
    periods: h
    start_from: 2020-03-02T10:00:00Z
    last_hours: 5
"#;
    let side = both(&Case::new("startfromlast").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'you cannot use both StartFrom 2020-03-02 10:00:00 +0000 UTC and LastHours 5'"]
    );
}

#[test]
fn debug_prints_the_non_standard_start_dates_and_decisions() {
    let yaml = r#"---
metrics:
  - name: From 2020-03-02
    series_name_or_func: later
    sql: later
    periods: h,d
    aggregate: 1,7
    skip: d7
    start_from: 2020-03-02T10:00:00Z
"#;
    let side = both(
        &Case::new("debug")
            .metrics(yaml)
            .env("GHA2DB_DEBUG", "1")
            .env("GHA2DB_SKIPPDB", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(out.contains(&"Using non-standard start date: 2020-03-02 10:00:00 +0000 UTC, instead of 2020-03-01 03:00:00 +0000 +0000".to_string()), "{out:?}");
    assert!(out.contains(&"Skipped period d7".to_string()), "{out:?}");
    assert!(
        out.contains(&"Recalculate period \"h\", hist false for date to <now>: true".to_string()),
        "{out:?}"
    );
}

#[test]
fn metric_env_map_conditions() {
    let yaml = r#"---
metrics:
  - name: Env
    series_name_or_func: env
    sql: env
    periods: h,d
    aggregate: 1,7
    skip: h7
    env:
      TESTVAR_ALL: all
      TESTVAR_H@h: hourly
      TESTVAR_NOTH!h: not-hourly
      TESTVAR_D7@d7: daily7
      TESTVAR_UNSET?: was-unset
      TESTVAR_EMPTY?: was-empty
      TESTVAR_SET?: overrides-set
      TESTVAR_SET??: only-if-unset
      TESTVAR_EMPTY??: only-if-unset2
"#;
    let side = both(
        &Case::new("envmap")
            .metrics(yaml)
            .env("TESTVAR_SET", "already")
            .env("TESTVAR_EMPTY", "")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let calls: Vec<Call> = calls(&side)
        .into_iter()
        .filter(|c| c.name() == "calc_metric")
        .collect();
    assert_eq!(calls.len(), 3);
    let envs = |c: &Call| -> Vec<String> {
        c.env
            .iter()
            .filter(|e| e.starts_with("TESTVAR_"))
            .cloned()
            .collect()
    };
    assert_eq!(
        envs(&calls[0]),
        [
            "TESTVAR_ALL=all",
            "TESTVAR_EMPTY=was-empty",
            "TESTVAR_H=hourly",
            "TESTVAR_SET=already",
            "TESTVAR_UNSET=was-unset",
        ]
    );
    assert_eq!(
        envs(&calls[1]),
        [
            "TESTVAR_ALL=all",
            "TESTVAR_EMPTY=was-empty",
            "TESTVAR_NOTH=not-hourly",
            "TESTVAR_SET=already",
            "TESTVAR_UNSET=was-unset",
        ]
    );
    assert_eq!(
        envs(&calls[2]),
        [
            "TESTVAR_ALL=all",
            "TESTVAR_D7=daily7",
            "TESTVAR_EMPTY=was-empty",
            "TESTVAR_NOTH=not-hourly",
            "TESTVAR_SET=already",
            "TESTVAR_UNSET=was-unset",
        ]
    );
}

#[test]
fn invalid_aggregate_is_fatal() {
    let yaml = r#"---
metrics:
  - name: Bad aggregate
    series_name_or_func: bad
    sql: bad
    periods: h
    aggregate: 1,x
"#;
    let side = both(&Case::new("badaggregate").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'strconv.Atoi: parsing \"x\": invalid syntax'"]
    );
    // the first aggregate ran before the fatal one
    assert_eq!(metric_calls(&side).len(), 1);
}

#[test]
fn unknown_period_is_fatal() {
    let yaml = r#"---
metrics:
  - name: Bad period
    series_name_or_func: bad
    sql: bad
    periods: h,x
"#;
    let side = both(&Case::new("badperiod").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'ComputePeriodAtThisDate: unknown period: 'x', hist: false'"]
    );
}

#[test]
fn empty_period_is_silently_skipped() {
    // `strings.Split("", ",")` yields `[""]`, so the (default, empty) `skip:`
    // list always contains the empty period: an empty `periods:` entry is
    // skipped, not fatal.  Go bug 30 (a slice panic in
    // ComputePeriodAtThisDate when called with "") is covered by lib unit tests.
    let yaml = r#"---
metrics:
  - name: Empty period
    series_name_or_func: bad
    sql: bad
    periods: 'h,'
"#;
    let side = both(&Case::new("emptyperiod").metrics(yaml).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0][4], "h");
}

#[test]
fn missing_metrics_yaml_is_fatal() {
    // lib.ReadFile falls back to `/shared/` and reports that path's error
    let side = both(&Case::new("nometrics").no_metrics().metrics_only()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'open ./metrics/shared/metrics.yaml: no such file or directory'"]
    );
}

#[test]
fn custom_metrics_yaml_path() {
    let side = both(
        &Case::new("custommetrics")
            .metrics_file("my_metrics.yaml")
            .env("GHA2DB_METRICS_YAML", "my_metrics.yaml")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(metric_calls(&side).len(), 2);
}

#[test]
fn malformed_metrics_yaml_is_fatal() {
    let side = both(
        &Case::new("badmetrics")
            .metrics("metrics:\n  - name: [unclosed\n")
            .code_only_errors()
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(errors_of(&side).len(), 1);
}

#[test]
fn empty_metrics_yaml_computes_nothing() {
    let side = both(&Case::new("emptymetrics").metrics("").metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0);
    assert!(metric_calls(&side).is_empty());
    let out = stdout_lines(&side);
    assert!(
        out.iter()
            .any(|l| l.starts_with("Now processing 0 histograms using ")),
        "{out:?}"
    );
}

#[test]
fn no_project_uses_the_shared_metrics_dir_and_skips_annotations() {
    let side = both(
        &Case::new("noproject")
            .project("")
            .env("GHA2DB_RESETTSDB", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let metrics = metric_calls(&side);
    assert_eq!(metrics[0][1], "./metrics/events.sql");
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "tags",
            "columns",
            "calc_metric",
            "calc_metric",
            "columns",
            "vars"
        ]
    );
    let out = stdout_lines(&side);
    assert!(out.contains(&"Skipping `annotations` recalculation, it is only computed once per day, on the first sync after a day boundary, or if tags were ran during this sync".to_string()), "{out:?}");
    assert_eq!(
        calls(&side)[1].env("GHA2DB_PROJECTS_COMMITS").as_deref(),
        Some("")
    );
}

#[test]
fn real_kubernetes_metrics_yaml_with_compute_all() {
    let yaml = fs::read_to_string(fixture("gha2db_sync/kubernetes_metrics.yaml")).unwrap();
    let side = both(
        &Case::new("realk8s")
            .project("kubernetes")
            .metrics(&yaml)
            .env("GHA2DB_COMPUTE_ALL", "1")
            .env("GHA2DB_ENABLE_METRICS_DROP", "1")
            .env("GHA2DB_MAX_HIST", "1")
            .env("GHA2DB_ST", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    let metrics = metric_calls(&side);
    assert!(metrics.len() > 100, "{}", metrics.len());
    // annotations_ranges metrics use the quick ranges as periods
    assert!(
        metrics
            .iter()
            .any(|m| m[4] == "a_1_n" && m[5].contains("annotations_ranges")),
        "{metrics:?}"
    );
    assert!(metrics.iter().any(|m| m[5].contains("hist")));
}

#[test]
fn real_all_metrics_yaml_with_compute_all() {
    let yaml = fs::read_to_string(fixture("gha2db_sync/all_metrics.yaml")).unwrap();
    let side = both(
        &Case::new("realall")
            .project("all")
            .metrics(&yaml)
            .env("GHA2DB_COMPUTE_ALL", "1")
            .env("GHA2DB_ST", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    assert!(metric_calls(&side).len() > 50);
}

// ---------------------------------------------------------------------------
// Histograms
// ---------------------------------------------------------------------------

const HIST_YAML: &str = r#"---
metrics:
  - name: Hist one
    series_name_or_func: hone
    sql: hone
    periods: h,d
    histogram: true
    desc: time_diff_as_string
  - name: Hist ranges
    series_name_or_func: hranges
    sql: hranges_fail
    periods: d
    histogram: true
    annotations_ranges: true
    allow_fail: true
    wait_after_fail: 1
    env:
      GHA2DB_NCPUS?: 2
  - name: Plain
    series_name_or_func: plain
    sql: plain
    periods: h
"#;

#[test]
fn histograms_run_after_the_metrics_single_threaded() {
    let side = both(
        &Case::new("histst")
            .metrics(HIST_YAML)
            .env("GHA2DB_ST", "1")
            .env("GHA2DB_COMPUTE_ALL", "1")
            .script("calc_metric", r#"case "$2" in *fail*) exit 1;; esac"#)
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    let metrics = metric_calls(&side);
    let got: Vec<(String, String, String)> = metrics
        .iter()
        .map(|m| (m[0].clone(), m[4].clone(), m[5].clone()))
        .collect();
    assert_eq!(
        got,
        [
            (
                "plain".to_string(),
                "h".to_string(),
                "skip_past".to_string()
            ),
            (
                "hone".to_string(),
                "h".to_string(),
                "hist,desc:time_diff_as_string,skip_past".to_string()
            ),
            (
                "hone".to_string(),
                "d".to_string(),
                "hist,desc:time_diff_as_string,skip_past".to_string()
            ),
            (
                "hranges".to_string(),
                "d7".to_string(),
                "hist,annotations_ranges,skip_past".to_string()
            ),
            (
                "hranges".to_string(),
                "a_1_n".to_string(),
                "hist,annotations_ranges,skip_past".to_string()
            ),
            (
                "hranges".to_string(),
                "a_0_1".to_string(),
                "hist,annotations_ranges,skip_past".to_string()
            ),
        ]
    );
    let out = stdout_lines(&side);
    assert!(out.contains(&"Scheduled histogram metric Hist one, period h, desc: 'time_diff_as_string', aggregate: '' ...".to_string()), "{out:?}");
    assert!(
        out.contains(&"Now processing 5 histograms using ST version".to_string()),
        "{out:?}"
    );
    assert!(out.contains(&format!("Calculate histogram hone,./metrics/p1/hone.sql,2020-03-01 3,{},h,hist,desc:time_diff_as_string,skip_past,false,0 ...", now_ymdh())), "{out:?}");
    assert!(out.contains(&format!("Calculated histogram hranges,./metrics/p1/hranges_fail.sql,2020-03-01 3,{},d7,hist,annotations_ranges,skip_past,true,1 ... <duration>", now_ymdh())), "{out:?}");
    assert!(out.iter().any(|l| l.starts_with("WARNING: histogram map[GHA2DB_NCPUS:2] [./calc_metric hranges ./metrics/p1/hranges_fail.sql 2020-03-01 3 ") && l.ends_with(" failed: exit status 1")), "{out:?}");
    assert!(
        out.contains(
            &"There was at least one failure that requested wait (hist), waiting: 1 seconds"
                .to_string()
        ),
        "{out:?}"
    );
    assert!(
        out.contains(
            &"There was at least one failure that requested wait (hist), waited: 1 seconds"
                .to_string()
        ),
        "{out:?}"
    );
    // the metric's env reached calc_metric
    let hist_calls: Vec<Call> = calls(&side)
        .into_iter()
        .filter(|c| c.cmd.starts_with("calc_metric [hranges]"))
        .collect();
    assert_eq!(hist_calls.len(), 3);
    assert_eq!(hist_calls[0].env("GHA2DB_NCPUS").as_deref(), Some("2"));
}

#[test]
fn histograms_multi_threaded() {
    let side = both(
        &Case::new("histmt")
            .metrics(HIST_YAML)
            .env("GHA2DB_NCPUS", "2")
            .env("GHA2DB_COMPUTE_ALL", "1")
            .script("calc_metric", r#"case "$2" in *fail*) exit 1;; esac"#)
            .sorted()
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    assert_eq!(metric_calls(&side).len(), 6);
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Now processing 5 histograms using MT2 version".to_string()),
        "{out:?}"
    );
    assert!(
        out.contains(&"Final threads join (processed 4)".to_string()),
        "{out:?}"
    );
    // GHA2DB_NCPUS was set: the metric's `GHA2DB_NCPUS?` does not override it
    let hist_calls: Vec<Call> = calls(&side)
        .into_iter()
        .filter(|c| c.cmd.starts_with("calc_metric [hranges]"))
        .collect();
    assert_eq!(hist_calls[0].env("GHA2DB_NCPUS").as_deref(), Some("2"));
}

#[test]
fn max_histograms_limits_the_threads() {
    let side = both(
        &Case::new("maxhist")
            .metrics(HIST_YAML)
            .env("GHA2DB_NCPUS", "4")
            .env("GHA2DB_MAX_HIST", "2")
            .env("GHA2DB_COMPUTE_ALL", "1")
            .env("GHA2DB_ALLOW_METRIC_FAIL", "1")
            .script("calc_metric", r#"case "$2" in *fail*) exit 1;; esac"#)
            .sorted()
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    let out = stdout_lines(&side);
    // Same CPU-count source as the binary under test (Go `runtime.NumCPU()`).
    if devstatscode::threads::num_cpu() >= 4 {
        assert!(
            out.contains(&"Number of parallel histograms limited to 4 -> 2".to_string()),
            "{out:?}"
        );
        assert!(
            out.contains(&"Now processing 5 histograms using MT2 version".to_string()),
            "{out:?}"
        );
    }
}

#[test]
fn max_histograms_one_is_single_threaded() {
    let side = both(
        &Case::new("maxhist1")
            .metrics(HIST_YAML)
            .env("GHA2DB_MAX_HIST", "1")
            .env("GHA2DB_COMPUTE_ALL", "1")
            .script("calc_metric", r#"case "$2" in *fail*) exit 1;; esac"#)
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Now processing 5 histograms using ST version".to_string()),
        "{out:?}"
    );
}

#[test]
fn histogram_failure_without_allow_fail_is_fatal() {
    let yaml = r#"---
metrics:
  - name: Hist
    series_name_or_func: hone
    sql: hone_fail
    periods: h
    histogram: true
"#;
    let side = both(
        &Case::new("histfail")
            .metrics(yaml)
            .env("GHA2DB_ST", "1")
            .script("calc_metric", r#"case "$2" in *fail*) exit 1;; esac"#)
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(errors_of(&side), ["Error: 'exit status 1'"]);
}

#[test]
fn random_metric_order_keeps_the_last_series_last() {
    let yaml = r#"---
metrics:
  - name: Events hourly
    series_name_or_func: events_h
    sql: events
    periods: h
  - name: A
    series_name_or_func: a
    sql: a
    periods: h
  - name: B
    series_name_or_func: b
    sql: b
    periods: h
  - name: C
    series_name_or_func: c
    sql: c
    periods: h
  - name: H
    series_name_or_func: h
    sql: h
    periods: h
    histogram: true
"#;
    let side = both(
        &Case::new("random")
            .metrics(yaml)
            .no_env("GHA2DB_SKIP_RAND")
            .env("GHA2DB_ST", "1")
            .sorted()
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let series: Vec<String> = metric_calls(&side).iter().map(|m| m[0].clone()).collect();
    assert_eq!(series.len(), 5);
    assert_eq!(series[3], "events_h", "{series:?}");
    assert_eq!(series[4], "h", "{series:?}");
    let out = stdout_lines(&side);
    assert!(out.contains(&"Randomizing metrics calculation order".to_string()));
    assert!(out.contains(&"Randomizing histogram metrics calculation order".to_string()));
}

// ---------------------------------------------------------------------------
// tags / columns / annotations
// ---------------------------------------------------------------------------

#[test]
fn run_columns_forces_the_final_columns() {
    let side = both(
        &Case::new("runcolumns")
            .env("GHA2DB_RUN_COLUMNS", "1")
            .env("GHA2DB_SKIP_TAGS", "1")
            .env("GHA2DB_SKIP_ANNOTATIONS", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "calc_metric",
            "calc_metric",
            "columns",
            "vars"
        ]
    );
}

#[test]
fn skip_columns_wins_over_reset_tsdb() {
    let side = both(
        &Case::new("skipcolumns")
            .env("GHA2DB_RESETTSDB", "1")
            .env("GHA2DB_SKIP_COLUMNS", "1"),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        names(&side),
        [
            "gha2db",
            "get_repos",
            "ghapi2db",
            "reconcile_dbs",
            "structure",
            "tags",
            "annotations",
            "calc_metric",
            "calc_metric",
            "vars"
        ]
    );
}

#[test]
fn daily_stages_run_on_the_first_sync_after_a_day_boundary() {
    // The default seed's newest TSDB hour is in 2020: a day boundary was crossed since
    let side = both(&Case::new("dayfirst")).unwrap();
    assert_eq!(side.out.code(), 0);
    let n = names(&side);
    assert!(n.contains(&"tags".to_string()), "{n:?}");
    assert!(n.contains(&"annotations".to_string()), "{n:?}");
    assert_eq!(n.iter().filter(|x| *x == "columns").count(), 2, "{n:?}");
}

#[test]
fn daily_stages_are_skipped_within_the_same_day() {
    let side = both(&synced_this_hour(Case::new("sameday"))).unwrap();
    assert_eq!(side.out.code(), 0);
    let n = names(&side);
    assert!(!n.contains(&"tags".to_string()), "{n:?}");
    assert!(!n.contains(&"annotations".to_string()), "{n:?}");
    assert!(!n.contains(&"columns".to_string()), "{n:?}");
    let out = stdout_lines(&side);
    for expected in [
        "Skipping `tags` recalculation, it is only computed once per day, on the first sync after a day boundary",
        "Skipping `annotations` recalculation, it is only computed once per day, on the first sync after a day boundary, or if tags were ran during this sync",
        "Skipping `columns` recalculation, it is only computed once per day, on the first sync after a day boundary, or if tags were ran during this sync",
        // the daily metric has no marker yet: recalculated although not due by the calendar
        "Period \"d\", hist false of metric Daily stats was not computed successfully since the current period started, recalculating",
    ] {
        assert!(out.contains(&expected.to_string()), "{expected}\n{out:?}");
    }
    assert_eq!(
        series_periods(&side),
        [
            ("events_h".to_string(), "h".to_string()),
            ("multi_row_single_column".to_string(), "d".to_string()),
        ]
    );
}

// ---------------------------------------------------------------------------
// Arguments from projects.yaml
// ---------------------------------------------------------------------------

#[test]
fn project_command_line_env_start_date_and_scale_from_projects_yaml() {
    let side = both(&Case::new("projyaml").args(&[]).no_seed().metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0, "{}", side.out.stderr_str());
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"gha2db_sync.go: Running on: org1+org2/repo1".to_string()),
        "{out:?}"
    );
    // start_date applies when the tables are empty
    assert!(
        out.contains(&"Using start dates: pg: 2019-05-01 0, tsdb: 2019-05-01 0".to_string()),
        "{out:?}"
    );
    let c = calls(&side);
    // the project's env reached the sub-commands
    assert_eq!(c[0].env("TESTVAR_A").as_deref(), Some("alpha"));
    assert_eq!(c[0].env("TESTVAR_B").as_deref(), Some("with space"));
    // project_scale is passed to calc_metric first
    let metrics = metric_calls(&side);
    assert_eq!(metrics[0][5], "project_scale:2.500000,skip_past");
    assert_eq!(
        metrics[1][5],
        "project_scale:2.500000,multivalue,merge_series:dstats,skip_past"
    );
    // GHA2DB_VARS_FN_YAML from the project env
    let vars = c.last().unwrap();
    assert_eq!(vars.name(), "vars");
    assert_eq!(
        vars.env("GHA2DB_VARS_FN_YAML").as_deref(),
        Some("custom_vars.yaml")
    );
}

#[test]
fn env_set_skips_the_project_env() {
    let side = both(
        &Case::new("envset")
            .args(&[])
            .env("ENV_SET", "1")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let c = calls(&side);
    assert_eq!(c[0].env("TESTVAR_A"), None);
    assert_eq!(
        c.last().unwrap().env("GHA2DB_VARS_FN_YAML").as_deref(),
        Some("sync_vars.yaml")
    );
    // the scale still applies
    assert_eq!(
        metric_calls(&side)[0][5],
        "project_scale:2.500000,skip_past"
    );
}

#[test]
fn forced_start_date_wins_over_the_project_start_date() {
    let side = both(
        &Case::new("projforce")
            .args(&[])
            .env("GHA2DB_STARTDT", "2020-01-01")
            .env("GHA2DB_STARTDT_FORCE", "1")
            .metrics_only(),
    )
    .unwrap();
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"Using start dates: pg: 2020-01-01 0, tsdb: 2020-01-01 0".to_string()),
        "{out:?}"
    );
}

#[test]
fn negative_project_scale_is_ignored_and_empty_command_line_allowed() {
    let side = both(&Case::new("proj2").args(&[]).project("p2").metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"gha2db_sync.go: Running on: orgx/".to_string()),
        "{out:?}"
    );
    assert_eq!(metric_calls(&side)[0][5], "skip_past");
    assert_eq!(
        calls(&side)[0].args()[4..],
        ["orgx".to_string(), "".to_string()]
    );
}

#[test]
fn invalid_project_env_key_is_fatal() {
    let side = both(&Case::new("proj3").args(&[]).project("p3").metrics_only()).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(errors_of(&side), ["Error: 'setenv: invalid argument'"]);
}

#[test]
fn explicit_project_scale_env() {
    let side = both(
        &Case::new("scaleenv")
            .env("GHA2DB_PROJECT_SCALE", "0.5")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(
        metric_calls(&side)[0][5],
        "project_scale:0.500000,skip_past"
    );
}

#[test]
fn unknown_project_is_fatal() {
    let side = both(&Case::new("unknownproj").args(&[]).project("nope")).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'project 'nope' is not defined in 'projects.yaml''"]
    );
}

#[test]
fn no_arguments_and_no_project_is_fatal() {
    let side = both(&Case::new("noargs").args(&[]).project("")).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'you have to set project via GHA2DB_PROJECT environment variable if you provide no commandline arguments'"]
    );
}

#[test]
fn missing_projects_yaml_is_fatal() {
    let side = both(&Case::new("noprojyaml").args(&[]).projects(None)).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        errors_of(&side),
        ["Error: 'open ./projects.yaml: no such file or directory'"]
    );
}

#[test]
fn custom_projects_yaml_name() {
    let side = both(
        &Case::new("customproj")
            .args(&[])
            .projects_file("my_projects.yaml")
            .env("GHA2DB_PROJECTS_YAML", "my_projects.yaml")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(metric_calls(&side).len(), 2);
}

#[test]
fn malformed_projects_yaml_is_fatal() {
    let side = both(
        &Case::new("badprojyaml")
            .args(&[])
            .projects(Some("projects:\n  p1: [unclosed\n"))
            .code_only_errors(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(errors_of(&side).len(), 1);
}

#[test]
fn single_org_argument_and_whitespace_trimming() {
    let side = both(
        &Case::new("oneorg")
            .args(&[" a , b ", " r1 ,r2"])
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    let out = stdout_lines(&side);
    assert!(
        out.contains(&"gha2db_sync.go: Running on: a+b/r1+r2".to_string()),
        "{out:?}"
    );
    assert_eq!(
        calls(&side)[0].args()[4..],
        ["a,b".to_string(), "r1,r2".to_string()]
    );
}

#[test]
fn project_yaml_is_not_read_with_arguments() {
    let side = both(&Case::new("argsnoyaml").projects(None).metrics_only()).unwrap();
    assert_eq!(side.out.code(), 0);
}

#[test]
fn fetch_commits_mode_is_passed_to_get_repos() {
    let side = both(
        &Case::new("fetchmode")
            .env("GHA2DB_FETCH_COMMITS_MODE", "2")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(
        calls(&side)[1].env("GHA2DB_FETCH_COMMITS_MODE").as_deref(),
        Some("2")
    );
    let side = both(
        &Case::new("fetchmode0")
            .env("GHA2DB_FETCH_COMMITS_MODE", "0")
            .metrics_only(),
    )
    .unwrap();
    assert_eq!(
        calls(&side)[1].env("GHA2DB_FETCH_COMMITS_MODE").as_deref(),
        Some("0")
    );
}
