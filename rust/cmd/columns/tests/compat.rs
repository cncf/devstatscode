//! Go ⇄ Rust compatibility tests for `columns`.
//!
//! Every case runs the Go binary and the Rust binary with the same
//! environment and working directory (`compat/fixtures/columns/data`, laid
//! out like a `cncf/devstats` checkout: the `shared`, `all` and `kubernetes`
//! `columns*.yaml`, `devel/test_columns.yaml` and `metrics/shared/empty.yaml`
//! are real files, `metrics/testproj/*` are test-specific) on two identical
//! scratch databases (`dbtest_columns_<name>_go` / `_rs`: the small TSDB of
//! `compat/fixtures/columns/seed.sql` — tag tables and series tables) and
//! compares exit code, stdout (durations masked; as a multiset of lines,
//! because the Go program processes the tables of its second phase in map
//! order — and, single-threaded, the first phase line by line), the
//! `Error: '…'` lines of fatal errors and the resulting database: every
//! table's columns (type, nullability, default) and every row.
//!
//! Needs a PostgreSQL server (`test.sh` finds one; skipped otherwise). The
//! `hll` cases test the success path when the server has the `hll` extension
//! and the (identical) error path otherwise.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};

fn go_bin() -> Option<PathBuf> {
    go_binary("columns")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_columns"))
}

/// `compat/fixtures/columns/data` — the "devstats checkout" the tool runs in.
fn data_dir() -> PathBuf {
    fixture("columns/data")
}

fn seed_sql() -> String {
    fs::read_to_string(fixture("columns/seed.sql")).unwrap()
}

/// One compatibility case.
struct Case<'a> {
    name: &'a str,
    /// Environment on top of the `PG_*` connection variables.
    env: Vec<(&'a str, &'a str)>,
    cwd: PathBuf,
    /// Run against a scratch database (otherwise the case brings its own
    /// `PG_*` variables, e.g. an unreachable server, or needs none).
    db: bool,
    /// Install the `hll` extension in the scratch databases when the server
    /// offers it.
    hll: bool,
    /// Extra runs on the same database, each with its own additional
    /// environment (idempotency tests).
    more_runs: Vec<Vec<(&'a str, &'a str)>>,
    /// Compare the `Error: '…'` stderr lines of fatal errors (off where the
    /// wording comes from a third-party library: yaml decoding).
    compare_errors: bool,
    /// The run is single-threaded (`GHA2DB_ST`), so the first phase's output
    /// is compared line by line, not only as a multiset.
    st: bool,
}

impl<'a> Case<'a> {
    fn new(name: &'a str) -> Self {
        Case {
            name,
            env: vec![
                ("GHA2DB_LOCAL", "1"),
                ("GHA2DB_PROJECT", "testproj"),
                ("GHA2DB_ST", "1"),
            ],
            cwd: data_dir(),
            db: true,
            hll: false,
            more_runs: Vec::new(),
            compare_errors: true,
            st: true,
        }
    }
    fn env(mut self, k: &'a str, v: &'a str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self.env.push((k, v));
        self
    }
    fn no_env(mut self, k: &str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        if k == "GHA2DB_ST" {
            self.st = false;
        }
        self
    }
    fn project(self, p: &'a str) -> Self {
        self.env("GHA2DB_PROJECT", p)
    }
    fn yaml(self, path: &'a str) -> Self {
        self.env("GHA2DB_COLUMNS_YAML", path)
    }
    fn debug(self, level: &'a str) -> Self {
        self.env("GHA2DB_DEBUG", level)
    }
    fn no_db(mut self) -> Self {
        self.db = false;
        self
    }
    fn with_hll(mut self) -> Self {
        self.hll = true;
        self
    }
    fn then_run(mut self, env: &[(&'a str, &'a str)]) -> Self {
        self.more_runs.push(env.to_vec());
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
}

/// One side of a case: its database (if any) and the outcome of every run.
struct Side {
    db: Option<TestDb>,
    outs: Vec<Outcome>,
}

impl Side {
    fn out(&self) -> &Outcome {
        self.outs.last().unwrap()
    }
}

fn invocation(case: &Case<'_>, db: Option<&TestDb>, extra: &[(&str, &str)]) -> Invocation<'static> {
    let mut inv = Invocation::new().cwd(case.cwd.clone());
    if let Some(db) = db {
        for (k, v) in db.env() {
            inv = inv.env(leak(k), leak(v));
        }
    } else {
        inv = inv.env("GHA2DB_SKIPLOG", "1").env("GHA2DB_SKIPTIME", "1");
    }
    for (k, v) in &case.env {
        inv = inv.env(leak(k), leak(v));
    }
    for (k, v) in extra {
        inv = inv.env(leak(k), leak(v));
    }
    inv
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// Run `bin` per the case, on its own database when one is needed.
fn run_side(bin: &Path, case: &Case<'_>, suffix: &str) -> Option<Side> {
    let db = if case.db {
        let db = TestDb::fresh(&format!("columns_{}_{}", case.name, suffix))?;
        db.exec(&seed_sql());
        if case.hll {
            let con = db.conn();
            let available = cpg::hll_available(&con);
            con.close();
            if available {
                db.exec("create extension if not exists hll");
            } else if suffix == "rs" {
                eprintln!("[compat] hll extension not available on the test server — testing the error path");
            }
        }
        Some(db)
    } else {
        None
    };
    let mut outs = vec![run(bin, &invocation(case, db.as_ref(), &[]))];
    for extra in &case.more_runs {
        outs.push(run(bin, &invocation(case, db.as_ref(), extra)));
    }
    Some(Side { db, outs })
}

/// `Error: '…'` lines of a fatal error report (the scratch database name
/// masked).
fn error_lines(stderr: &str, side: &Side) -> Vec<String> {
    stderr
        .lines()
        .filter(|l| l.starts_with("Error: '"))
        .map(|l| mask_db(l, side))
        .collect()
}

fn mask_db(s: &str, side: &Side) -> String {
    match &side.db {
        Some(db) => s.replace(&db.name, "<db>"),
        None => s.to_string(),
    }
}

/// Sort the comma separated clauses of a multi-column `update … set a, b` /
/// `alter table … alter column a, alter column b` statement echoed by
/// `GHA2DB_QOUT`: the Go program emits them in (random) map order.
fn normalize_sql(line: &str) -> String {
    let split_sort = |head: &str, tail: &str| {
        let mut parts: Vec<&str> = tail.split(", ").collect();
        parts.sort();
        format!("{head}{}", parts.join(", "))
    };
    if line.starts_with("update \"") {
        if let Some(i) = line.find(" set ") {
            return split_sort(&line[..i + 5], &line[i + 5..]);
        }
    }
    if line.starts_with("alter table \"") && line.contains("\" alter column \"") {
        if let Some(i) = line.find("\" alter column \"") {
            return split_sort(&line[..i + 2], &line[i + 2..]);
        }
    }
    line.to_string()
}

fn normalize_lines(stdout: &[u8], side: &Side) -> Vec<String> {
    let text = String::from_utf8_lossy(stdout).into_owned();
    let text = mask_db(&mask_go_durations(&text), side);
    // `GHA2DB_QOUT` echoes the `gha_logs` insert arguments, including
    // `time.Now()` (Go also appends the monotonic reading `m=+0.0123`)
    text.lines()
        .map(|l| {
            if l.starts_with("[1:columns 2:") {
                if let (Some(a), Some(b)) = (l.find(" 3:"), l.find(" 4:")) {
                    if a < b {
                        return format!("{} 3:<now>{}", &l[..a], &l[b..]);
                    }
                }
            }
            normalize_sql(l)
        })
        .collect()
}

/// Is `line` printed by the first phase (per column config) of the program?
/// Everything else (`Cfg:`, the per-table mass update/alter, the
/// `DropLeastUsedCol` reports, `Time:`) is emitted in an order that depends on
/// Go's map iteration even single-threaded.
fn is_phase1_line(line: &str) -> bool {
    !(line.starts_with("Tables: ")
        || line.starts_with("Columns: ")
        || line.starts_with("HLLs: ")
        || line.starts_with("Cfg: ")
        || line.starts_with("Mass updated \"")
        || line.starts_with("Altered \"")
        || line.starts_with("Give up 'mass ")
        || line.starts_with("Error handle row is too big mass ")
        || line.starts_with("Table '")
        || line.starts_with("Two least used columns are: ")
        || line.starts_with("Dropped '")
        || line.starts_with("Error ")
        || line.starts_with("update \"")
        || line.starts_with("alter table \"")
        || line.starts_with("select ")
        || line.starts_with("[1:")
        || line.starts_with("Time: "))
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// Every table's columns (name, type, nullable, default), its rows and the
/// row counts.
type DbState = (
    Vec<cpg::TableSchema>,
    BTreeMap<String, Vec<Vec<String>>>,
    Vec<(String, i64)>,
);

/// The whole database as text.
fn db_state(db: &TestDb) -> DbState {
    let con = db.conn();
    let schema = cpg::schema(&con);
    let mut data = BTreeMap::new();
    for (table, cols, _) in &schema {
        // `hll` has no ordering operator: sort by the other columns only
        let order: Vec<String> = cols
            .iter()
            .enumerate()
            .filter(|(_, c)| c.1 != "USER-DEFINED")
            .map(|(i, _)| (i + 1).to_string())
            .collect();
        let rows = cpg::snapshot(
            &con,
            &format!("select * from \"{}\" order by {}", table, order.join(", ")),
            &[],
        )
        .rows;
        data.insert(table.clone(), rows);
    }
    let counts = cpg::table_counts(&con);
    con.close();
    (schema, data, counts)
}

/// Run both binaries and compare everything; returns the Rust side for
/// further assertions (`None` when the DB tests are skipped).
fn both(case: &Case<'_>) -> Option<Side> {
    let rust = run_side(&rust_bin(), case, "rs")?;
    if let Some(go) = go_bin() {
        let go = run_side(&go, case, "go").unwrap();
        for (i, (g, r)) in go.outs.iter().zip(rust.outs.iter()).enumerate() {
            let ctx = format!(
                "\ncase {:?} run {} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
                case.name,
                i,
                case.env,
                g.code,
                g.stdout_str(),
                g.stderr_str(),
                r.code,
                r.stdout_str(),
                r.stderr_str()
            );
            assert_eq!(g.code, r.code, "exit code{ctx}");
            let gl = normalize_lines(&g.stdout, &go);
            let rl = normalize_lines(&r.stdout, &rust);
            assert_eq!(
                sorted(gl.clone()),
                sorted(rl.clone()),
                "stdout as a multiset of lines{ctx}"
            );
            if case.st {
                let p1 = |v: &[String]| -> Vec<String> {
                    v.iter().filter(|l| is_phase1_line(l)).cloned().collect()
                };
                assert_eq!(p1(&gl), p1(&rl), "stdout order of the first phase{ctx}");
            }
            if case.compare_errors {
                assert_eq!(
                    error_lines(&g.stderr_str(), &go),
                    error_lines(&r.stderr_str(), &rust),
                    "fatal error lines{ctx}"
                );
            }
        }
        if let (Some(gdb), Some(rdb)) = (&go.db, &rust.db) {
            let (gs, gd, gc) = db_state(gdb);
            let (rs, rd, rc) = db_state(rdb);
            assert_eq!(gc, rc, "row counts (case {:?})", case.name);
            assert_eq!(gs, rs, "schema (case {:?})", case.name);
            assert_eq!(gd, rd, "table data (case {:?})", case.name);
        }
    }
    Some(rust)
}

fn stdout_of(side: &Side) -> String {
    String::from_utf8_lossy(&side.out().stdout).into_owned()
}

fn errors_of(side: &Side) -> Vec<String> {
    error_lines(&side.out().stderr_str(), side)
}

/// Column names of `table` on the Rust side, in definition order.
fn cols_of(side: &Side, table: &str) -> Vec<String> {
    let con = side.db.as_ref().unwrap().conn();
    let cols = cpg::table_columns(&con, table);
    con.close();
    cols.into_iter().map(|c| c.0).collect()
}

/// `(type, nullable, default)` of one column on the Rust side.
fn col_info(side: &Side, table: &str, col: &str) -> (String, String, String) {
    let con = side.db.as_ref().unwrap().conn();
    let cols = cpg::table_columns(&con, table);
    con.close();
    let c = cols
        .into_iter()
        .find(|c| c.0 == col)
        .unwrap_or_else(|| panic!("no column {col} in {table}"));
    (c.1, c.2, c.3)
}

/// A rendered query result on the Rust side.
fn query(side: &Side, sql: &str) -> Vec<Vec<String>> {
    let con = side.db.as_ref().unwrap().conn();
    let snap = cpg::snapshot(&con, sql, &[]);
    con.close();
    snap.rows
}

fn count_lines(out: &str, prefix: &str) -> usize {
    out.lines().filter(|l| l.starts_with(prefix)).count()
}

const DOUBLE_DEFAULT: (&str, &str, &str) = ("double precision", "NO", "0.0");

fn s3(t: (&str, &str, &str)) -> (String, String, String) {
    (t.0.to_string(), t.1.to_string(), t.2.to_string())
}

// ---------------------------------------------------------------------------
// The test project: every first-phase path on a deterministic dataset
// ---------------------------------------------------------------------------

#[test]
fn test_project_ensures_every_series() {
    let side = both(&Case::new("testproj")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);

    // debug 0 wording
    assert!(out.contains("Ensure column config: &{TableRegexp:^s(act|commits|grp_pr_merg)$ Tag:trepo_groups Column:repo_group_name HLL:false}\n"), "{out}");
    assert!(out.contains("Ensure 5 columns in '&{TableRegexp:^s(act|commits|grp_pr_merg)$ Tag:trepo_groups Column:repo_group_name HLL:false}'\n"), "{out}");
    assert!(out.contains("Currently 6 columns in 'sact'\n"), "{out}");
    assert!(
        out.contains("Need to delete 1 columns: [Stale] from 'sact' table\n"),
        "{out}"
    );
    assert!(
        out.contains("Deleted column \"Stale\" from 'sact' table\n"),
        "{out}"
    );
    assert!(
        out.contains("Added column \"Ünïcode Group\" to 'sact' table\n"),
        "{out}"
    );
    assert!(
        !out.contains("Added column \"Kubernetes\" to 'sact' table"),
        "{out}"
    );
    assert!(
        out.contains("Warning: no tag values for (users_name, tusers)\n"),
        "{out}"
    );
    assert!(out.contains("Warning: '&{TableRegexp:^snothing_here$ Tag:tcountries Column:country_name HLL:false}': no table hits\n"), "{out}");
    assert!(
        out.contains("Mass updated \"sact\", columns: 4, took: "),
        "{out}"
    );
    assert!(
        out.contains("Altered \"sact\" defaults and restrictions, columns: 4, took: "),
        "{out}"
    );
    assert!(
        out.contains("Mass updated \"scountries\", columns: 5, took: "),
        "{out}"
    );
    assert!(!out.contains("Mass updated \"suser_activity\""), "{out}");
    assert!(!out.contains("Cfg: "), "{out}");
    assert_eq!(
        count_lines(&out, "Added column "),
        4 + 5 + 4 + 4 + 3 + 3 + 3 + 5,
        "{out}"
    );
    assert_eq!(count_lines(&out, "Deleted column "), 2, "{out}");
    assert_eq!(count_lines(&out, "Mass updated "), 8, "{out}");
    assert_eq!(count_lines(&out, "Altered "), 8, "{out}");
    assert!(out.trim_end().ends_with('s'), "{out}"); // Time: …ms

    // sact: stale column gone, protected ones and the pre-existing tag
    // column kept with their data, the new ones present with default 0.0
    assert_eq!(
        cols_of(&side, "sact"),
        [
            "time",
            "period",
            "Kubernetes",
            "all",
            "None",
            "Docs",
            "Prometheus",
            "Envoy",
            "Ünïcode Group"
        ]
    );
    for c in ["Docs", "Prometheus", "Envoy", "Ünïcode Group"] {
        assert_eq!(col_info(&side, "sact", c), s3(DOUBLE_DEFAULT), "{c}");
    }
    assert_eq!(
        query(&side, "select \"Kubernetes\", \"all\", \"None\", \"Docs\", \"Ünïcode Group\" from sact order by time, period"),
        vec![
            vec!["1.5", "3.5", "4.5", "0", "0"],
            vec!["100", "300", "400", "0", "0"],
            vec!["10", "30", "40", "0", "0"],
        ]
    );
    // tag order (by time) is the column order
    assert_eq!(
        cols_of(&side, "scommits"),
        [
            "time",
            "period",
            "Kubernetes",
            "Docs",
            "Prometheus",
            "Envoy",
            "Ünïcode Group"
        ]
    );
    assert_eq!(
        cols_of(&side, "sgrp_pr_merg"),
        [
            "time",
            "period",
            "Docs",
            "Kubernetes",
            "Prometheus",
            "Envoy",
            "Ünïcode Group"
        ]
    );
    assert_eq!(
        cols_of(&side, "scompany_activity"),
        [
            "time",
            "period",
            "Google",
            "Red Hat",
            "Microsoft",
            "(Unknown)",
            "Ünïcode Ltd."
        ]
    );
    assert_eq!(
        query(
            &side,
            "select \"Google\", \"Red Hat\" from scompany_activity order by period"
        ),
        vec![vec!["7", "0"], vec!["70", "0"]]
    );
    // untouched: tag without values / no regexp match
    assert_eq!(
        cols_of(&side, "suser_activity"),
        ["time", "period", "Whatever"]
    );
    assert_eq!(cols_of(&side, "sother"), ["time", "period", "Kubernetes"]);
    assert_eq!(
        cols_of(&side, "scompany_activity_repos"),
        ["time", "period"]
    );
    // prefix regexp, lower-case `all` value
    assert_eq!(
        cols_of(&side, "spr_appr"),
        ["time", "period", "all", "kubernetes", "docs"]
    );
    assert_eq!(
        cols_of(&side, "spr_appr_by_group"),
        ["time", "period", "all", "kubernetes", "docs"]
    );
    assert_eq!(
        cols_of(&side, "siopened"),
        ["time", "period", "All", "Kubernetes", "Docs"]
    );
    assert_eq!(cols_of(&side, "siclosed"), ["time", "period", "gone"]);
    assert_eq!(
        cols_of(&side, "scountries"),
        [
            "time",
            "period",
            "Poland",
            "United States",
            "Germany",
            "Japan",
            "Côte d'Ivoire"
        ]
    );
    assert_eq!(
        col_info(&side, "scountries", "Côte d'Ivoire"),
        s3(DOUBLE_DEFAULT)
    );
    assert_eq!(cols_of(&side, "scountries_cum"), ["time", "period"]);
}

#[test]
fn debug_one_prints_configs_columns_and_cfg() {
    let side = both(&Case::new("debug1").debug("1")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains("Read 7 columns configs from './metrics/testproj/columns.yaml'\n"),
        "{out}"
    );
    assert!(out.contains("Ensure columns(5): &{TableRegexp:^s(act|commits|grp_pr_merg)$ Tag:trepo_groups Column:repo_group_name HLL:false} --> [Kubernetes Docs Prometheus Envoy Ünïcode Group]\n"), "{out}");
    assert!(
        out.contains("Current columns(6): sact --> [Kubernetes None Stale all period time]\n"),
        "{out}"
    );
    assert!(
        out.contains("Current columns(2): scommits --> [period time]\n"),
        "{out}"
    );
    assert!(out.contains("Ensure columns(3): &{TableRegexp:^spr_appr Tag:tall_repo_groups Column:all_repo_group_value HLL:false} --> [all kubernetes docs]\n"), "{out}");
    assert!(out.contains("Ensure columns(5): &{TableRegexp:^snothing_here$ Tag:tcountries Column:country_name HLL:false} --> [Poland United States Germany Japan Côte d'Ivoire]\n"), "{out}");
    assert!(out.contains("\nCfg: map[sact:map[Docs:n Envoy:n Prometheus:n Ünïcode Group:n] scommits:map[Docs:n Envoy:n Kubernetes:n Prometheus:n Ünïcode Group:n] scompany_activity:map[(Unknown):n Microsoft:n Red Hat:n Ünïcode Ltd.:n] scountries:map[Côte d'Ivoire:n Germany:n Japan:n Poland:n United States:n] sgrp_pr_merg:map[Envoy:n Kubernetes:n Prometheus:n Ünïcode Group:n] siopened:map[All:n Docs:n Kubernetes:n] spr_appr:map[all:n docs:n kubernetes:n] spr_appr_by_group:map[all:n docs:n kubernetes:n]]\n"), "{out}");
    assert!(!out.contains("Ensure 5 columns in"), "{out}");
    assert!(!out.contains("Currently "), "{out}");
    assert!(!out.contains("Tables: "), "{out}");
}

#[test]
fn debug_two_prints_the_collected_lists() {
    let side = both(&Case::new("debug2").debug("2")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("\nTables: [sact sact sact sact scommits scommits scommits scommits scommits sgrp_pr_merg sgrp_pr_merg sgrp_pr_merg sgrp_pr_merg scompany_activity scompany_activity scompany_activity scompany_activity spr_appr spr_appr spr_appr spr_appr_by_group spr_appr_by_group spr_appr_by_group siopened siopened siopened scountries scountries scountries scountries scountries]\n"), "{out}");
    assert!(out.contains("\nColumns: [Docs Prometheus Envoy Ünïcode Group Kubernetes Docs Prometheus Envoy Ünïcode Group Kubernetes Prometheus Envoy Ünïcode Group Red Hat Microsoft (Unknown) Ünïcode Ltd. all kubernetes docs all kubernetes docs All Kubernetes Docs Poland United States Germany Japan Côte d'Ivoire]\n"), "{out}");
    assert!(
        out.contains("\nHLLs: [n n n n n n n n n n n n n n n n n n n n n n n n n n n n n n n]\n"),
        "{out}"
    );
    assert!(out.contains("\nCfg: map["), "{out}");
}

#[test]
fn negative_debug_hides_the_config_lines() {
    let side = both(&Case::new("debugneg").debug("-1")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(!out.contains("Ensure column config: "), "{out}");
    assert!(
        out.contains("Ensure 5 columns in '&{TableRegexp:^s(act|commits|grp_pr_merg)$"),
        "{out}"
    );
    assert_eq!(count_lines(&out, "Added column "), 31, "{out}");
}

#[test]
fn second_run_changes_nothing() {
    let side = both(
        &Case::new("twice")
            .debug("1")
            .then_run(&[])
            .then_run(&[("GHA2DB_DEBUG", "0")]),
    )
    .unwrap();
    assert_eq!(side.outs.len(), 3);
    for o in &side.outs {
        assert_eq!(o.code(), 0);
    }
    let second = side.outs[1].stdout_str();
    assert!(!second.contains("Added column"), "{second}");
    assert!(!second.contains("Deleted column"), "{second}");
    assert!(!second.contains("Mass updated"), "{second}");
    assert!(second.contains("\nCfg: map[]\n"), "{second}");
    assert!(second.contains("Current columns(9): sact --> [Docs Envoy Kubernetes None Prometheus all period time Ünïcode Group]\n"), "{second}");
    let third = side.outs[2].stdout_str();
    assert!(third.contains("Currently 9 columns in 'sact'\n"), "{third}");
    assert!(!third.contains("Cfg: "), "{third}");
    assert_eq!(count_lines(&third, "Warning: "), 2, "{third}");
}

#[test]
fn multithreaded_runs_reach_the_same_state() {
    // all CPUs, then a pool of 2 (the "waiting on the channel" path)
    let side = both(&Case::new("mt").no_env("GHA2DB_ST").debug("1")).unwrap();
    assert_eq!(side.out().code(), 0);
    assert_eq!(count_lines(&stdout_of(&side), "Added column "), 31);
    let side = both(
        &Case::new("mt2")
            .no_env("GHA2DB_ST")
            .env("GHA2DB_NCPUS", "2"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert_eq!(count_lines(&out, "Added column "), 31, "{out}");
    assert_eq!(count_lines(&out, "Mass updated "), 8, "{out}");
    assert_eq!(cols_of(&side, "sact").len(), 9);
}

// ---------------------------------------------------------------------------
// "row is too big": HandleRowIsTooBig / DropLeastUsedCol
// ---------------------------------------------------------------------------

#[test]
fn row_too_big_drops_least_used_columns_and_retries() {
    let side = both(&Case::new("wide").yaml("metrics/testproj/columns_wide.yaml")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    // swide: recovers after two rounds
    assert!(
        out.contains(
            "Ensure 104 columns in '&{TableRegexp:^swide$ Tag:twide Column:wide_name HLL:false}'\n"
        ),
        "{out}"
    );
    assert!(out.contains("Currently 103 columns in 'swide'\n"), "{out}");
    assert!(out.contains("Table 'swide' has 100 column\n"), "{out}");
    assert!(out.contains("Two least used columns are: 'c001' and 'c002' with averages: 1.000000, 2.000000, indices: 0, 1\n"), "{out}");
    assert!(
        out.contains("Dropped 'c001' and 'c002' from 'swide' table\n"),
        "{out}"
    );
    assert!(out.contains("Table 'swide' has 98 column\n"), "{out}");
    assert!(out.contains("Two least used columns are: 'c003' and 'c004' with averages: 3.000000, 4.000000, indices: 0, 1\n"), "{out}");
    assert!(
        out.contains("Dropped 'c003' and 'c004' from 'swide' table\n"),
        "{out}"
    );
    assert!(
        out.contains("Mass updated \"swide\", columns: 4, took: "),
        "{out}"
    );
    assert!(
        out.contains("Altered \"swide\" defaults and restrictions, columns: 4, took: "),
        "{out}"
    );
    assert!(!out.contains("Table 'swide' has 96 column"), "{out}");
    // sgiveup: three rounds, give up, the not-null alter then fails
    assert!(out.contains("Table 'sgiveup' has 96 column\n"), "{out}");
    assert!(out.contains("Two least used columns are: 'c005' and 'c006' with averages: 5.000000, 6.000000, indices: 0, 1\n"), "{out}");
    assert!(
        out.contains("Dropped 'c005' and 'c006' from 'sgiveup' table\n"),
        "{out}"
    );
    assert!(
        out.contains("Give up 'mass add columns' after 3 trials\n"),
        "{out}"
    );
    assert!(out.contains("Error handle row is too big mass alter defaults: pq: column \"Docs\" of relation \"sgiveup\" contains null values\n"), "{out}");
    assert!(!out.contains("Mass updated \"sgiveup\""), "{out}");
    assert!(!out.contains("Altered \"sgiveup\""), "{out}");
    // snarrow: fewer than 80 candidate columns, nothing dropped, no retry
    assert!(out.contains("Table 'snarrow' has 10 column\n"), "{out}");
    assert!(!out.contains("from 'snarrow' table\n"), "{out}");
    assert!(out.contains("Error handle row is too big mass alter defaults: pq: column \"Docs\" of relation \"snarrow\" contains null values\n"), "{out}");
    assert_eq!(count_lines(&out, "Give up "), 1, "{out}");
    assert_eq!(
        count_lines(&out, "Two least used columns are: "),
        5,
        "{out}"
    );

    // database: dropped columns gone, new ones final (swide) or still
    // nullable/NULL (sgiveup, snarrow)
    let swide = cols_of(&side, "swide");
    assert_eq!(swide.len(), 3 + 96 + 4);
    for c in ["c001", "c002", "c003", "c004"] {
        assert!(!swide.contains(&c.to_string()), "{c}");
    }
    assert!(swide.contains(&"c005".to_string()));
    assert_eq!(col_info(&side, "swide", "Docs"), s3(DOUBLE_DEFAULT));
    assert_eq!(
        query(
            &side,
            "select \"Docs\", \"Prometheus\", \"c005\", \"c100\" from swide"
        ),
        vec![vec!["0", "0", "5", "100"]]
    );
    let sgiveup = cols_of(&side, "sgiveup");
    assert_eq!(sgiveup.len(), 3 + 94 + 4);
    assert!(!sgiveup.contains(&"c006".to_string()));
    assert!(sgiveup.contains(&"c007".to_string()));
    assert_eq!(
        col_info(&side, "sgiveup", "Docs"),
        s3(("double precision", "YES", ""))
    );
    assert_eq!(
        query(
            &side,
            "select \"Docs\", \"Prometheus\", \"c007\" from sgiveup"
        ),
        vec![vec!["<nil>", "<nil>", "7"]]
    );
    assert_eq!(cols_of(&side, "snarrow").len(), 3 + 10 + 4);
    assert_eq!(
        col_info(&side, "snarrow", "Kubernetes"),
        s3(("double precision", "YES", ""))
    );
    assert_eq!(
        query(&side, "select \"Kubernetes\", \"c001\" from snarrow"),
        vec![vec!["<nil>", "1"]]
    );
}

#[test]
fn row_too_big_debug_lists_columns_and_averages() {
    let side = both(
        &Case::new("wide_dbg")
            .yaml("metrics/testproj/columns_wide_ok.yaml")
            .debug("1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    let names: Vec<String> = (1..=100).map(|i| format!("c{i:03}")).collect();
    let avgs: Vec<String> = (1..=100).map(|i| i.to_string()).collect();
    assert!(
        out.contains(&format!(
            "Table 'swide' has 100 columns: [{}]\n",
            names.join(" ")
        )),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "Table 'swide' columns averages: [{}]\n",
            avgs.join(" ")
        )),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "Table 'swide' has 98 columns: [{}]\n",
            names[2..].join(" ")
        )),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "Table 'swide' columns averages: [{}]\n",
            avgs[2..].join(" ")
        )),
        "{out}"
    );
    assert!(
        out.contains("Current columns(103): swide --> [c001 c002"),
        "{out}"
    );
    assert!(
        out.contains("\nCfg: map[swide:map[Docs:n Envoy:n Kubernetes:n Prometheus:n]]\n"),
        "{out}"
    );
}

// ---------------------------------------------------------------------------
// Odd tag values, HLL columns
// ---------------------------------------------------------------------------

#[test]
fn tag_value_with_a_double_quote_is_reported_and_skipped() {
    let side = both(&Case::new("quotes").yaml("metrics/testproj/columns_quotes.yaml")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("Error handle row is too big add column O\"Reilly/double precision: pq: unterminated quoted identifier at or near \"\" double precision\"\n"), "{out}");
    assert!(out.contains("Added column \"Plain\" to 'squotes' table\nError handle row is too big add column O\"Reilly"), "{out}");
    assert!(
        out.contains("Added column \"Also plain\" to 'squotes' table\n"),
        "{out}"
    );
    assert!(
        out.contains("Mass updated \"squotes\", columns: 2, took: "),
        "{out}"
    );
    assert_eq!(
        cols_of(&side, "squotes"),
        ["time", "period", "Plain", "Also plain"]
    );
}

#[test]
fn hll_columns() {
    let side = both(
        &Case::new("hll")
            .yaml("metrics/testproj/columns_hll.yaml")
            .with_hll(),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("Ensure column config: &{TableRegexp:^shll$ Tag:trepo_groups Column:repo_group_name HLL:true}\n"), "{out}");
    let con = side.db.as_ref().unwrap().conn();
    let available = cpg::hll_available(&con);
    con.close();
    if available {
        assert_eq!(count_lines(&out, "Added column "), 5 + 4, "{out}");
        assert!(
            out.contains("Mass updated \"shll\", columns: 5, took: "),
            "{out}"
        );
        assert!(
            out.contains("Altered \"sother\" defaults and restrictions, columns: 4, took: "),
            "{out}"
        );
        assert!(!out.contains("Error handle"), "{out}");
        assert_eq!(
            col_info(&side, "shll", "Kubernetes"),
            s3(("USER-DEFINED", "NO", "hll_empty()"))
        );
        assert_eq!(
            col_info(&side, "shll", "Ünïcode Group"),
            s3(("USER-DEFINED", "NO", "hll_empty()"))
        );
        // the pre-existing double precision column is kept as is
        assert_eq!(col_info(&side, "sother", "Kubernetes"), s3(DOUBLE_DEFAULT));
        assert_eq!(
            col_info(&side, "sother", "Docs"),
            s3(("USER-DEFINED", "NO", "hll_empty()"))
        );
        assert_eq!(
            query(&side, "select hll_cardinality(\"Kubernetes\")::int, hll_cardinality(\"Docs\")::int from shll"),
            vec![vec!["0", "0"]]
        );
    } else {
        // `sother` already has a "Kubernetes" column, so only 4 are attempted there
        assert_eq!(
            count_lines(&out, "Error handle row is too big add column "),
            5 + 4,
            "{out}"
        );
        assert!(out.contains("Error handle row is too big add column Ünïcode Group/hll: pq: type \"hll\" does not exist\n"), "{out}");
        assert!(!out.contains("Added column"), "{out}");
        assert!(!out.contains("Mass updated"), "{out}");
        assert_eq!(cols_of(&side, "shll"), ["time", "period"]);
        assert_eq!(cols_of(&side, "sother"), ["time", "period", "Kubernetes"]);
    }
}

// ---------------------------------------------------------------------------
// Fatal paths
// ---------------------------------------------------------------------------

#[test]
fn missing_tag_table_is_fatal() {
    let side = both(&Case::new("notag").yaml("metrics/testproj/columns_missing_tag.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'pq: relation \"tnonexistent\" does not exist'".to_string()]
    );
    assert!(stdout_of(&side).contains("Ensure column config: &{TableRegexp:^sact$ Tag:tnonexistent Column:nonexistent_name HLL:false}\n"));
    assert_eq!(cols_of(&side, "sact").len(), 6);
}

#[test]
fn missing_tag_column_is_fatal() {
    let side =
        both(&Case::new("nocol").yaml("metrics/testproj/columns_missing_column.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'pq: column \"no_such_column\" does not exist'".to_string()]
    );
}

#[test]
fn null_tag_value_is_a_fatal_scan_error() {
    let side = both(&Case::new("nulltag").yaml("metrics/testproj/columns_null_tag.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'sql: Scan error on column index 0, name \"nulls_name\": converting NULL to string is unsupported'".to_string()]
    );
    assert_eq!(cols_of(&side, "snulls"), ["time", "period"]);
}

#[test]
fn invalid_table_regexp_is_fatal() {
    let side = both(&Case::new("badre").yaml("metrics/testproj/columns_bad_regexp.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'pq: invalid regular expression: parentheses () not balanced'".to_string()]
    );
}

#[test]
fn scalar_yaml_values_decode_like_yaml_v2() {
    let side = both(&Case::new("scalars").yaml("metrics/testproj/scalars.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert!(stdout_of(&side)
        .contains("Ensure column config: &{TableRegexp:42 Tag:17 Column:1.5 HLL:true}\n"));
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'pq: relation \"17\" does not exist'".to_string()]
    );
}

#[test]
fn missing_yaml_is_fatal() {
    let side = both(&Case::new("missing").yaml("metrics/testproj/nope.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        // `lib.ReadFile` falls back to `metrics/shared/` and reports that path
        vec!["Error: 'open ./metrics/shared/nope.yaml: no such file or directory'".to_string()]
    );
    // an unknown project with the default yaml name falls back to the shared one
    let side = both(&Case::new("missing_proj").project("nosuchproj").debug("1")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains("Read 8 columns configs from './metrics/nosuchproj/columns.yaml'\n"),
        "{out}"
    );
    assert!(
        out.contains("lib.ReadFile('./metrics/shared/columns.yaml'): ok\n"),
        "{out}"
    );
    assert!(
        !out.contains("lib.ReadFile('./metrics/nosuchproj/"),
        "{out}"
    );
}

#[test]
fn malformed_yaml_is_fatal() {
    // the error text comes from the yaml library (differs by design)
    for (name, yaml) in [
        ("malformed", "metrics/testproj/malformed.yaml"),
        ("shape", "metrics/testproj/wrong_shape.yaml"),
    ] {
        let side = both(&Case::new(name).yaml(yaml).code_only_errors()).unwrap();
        assert_eq!(side.out().code(), 2);
        let errs = errors_of(&side);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].starts_with("Error: 'yaml: "), "{errs:?}");
        assert!(!stdout_of(&side).contains("Ensure "));
    }
}

#[test]
fn unreachable_server_is_fatal() {
    let side = both(
        &Case::new("noconn")
            .no_db()
            .env("PG_HOST", "127.0.0.1")
            .env("PG_PORT", "1")
            .env("PG_DB", "nope")
            .env("PG_USER", "gha_admin")
            .env("PG_PASS", "password"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'dial tcp 127.0.0.1:1: connect: connection refused'".to_string()]
    );
}

// ---------------------------------------------------------------------------
// Environment: SKIPTSDB, project, yaml path, data dir, QOUT, empty yamls
// ---------------------------------------------------------------------------

#[test]
fn skip_tsdb_does_nothing() {
    let side = both(
        &Case::new("skiptsdb")
            .no_db()
            .env("GHA2DB_SKIPTSDB", "1")
            .env("PG_HOST", "127.0.0.1")
            .env("PG_PORT", "1")
            .env("PG_DB", "nope"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    let lines: Vec<&str> = out
        .lines()
        .filter(|l| !l.starts_with("Compiled "))
        .collect();
    assert_eq!(lines.len(), 1, "{out}");
    assert!(lines[0].starts_with("Time: "), "{out}");
}

#[test]
fn no_project_uses_metrics_columns_yaml() {
    let side = both(&Case::new("noproj").no_env("GHA2DB_PROJECT").debug("1")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains("Read 1 columns configs from './metrics/columns.yaml'\n"),
        "{out}"
    );
    assert_eq!(
        cols_of(&side, "sevent_types"),
        [
            "time",
            "period",
            "PushEvent",
            "PullRequestEvent",
            "IssuesEvent"
        ]
    );
    assert_eq!(cols_of(&side, "sact").len(), 6);
}

#[test]
fn datadir_mode_uses_the_absolute_prefix() {
    let dir = format!("{}/", data_dir().to_string_lossy());
    let side = both(
        &Case::new("datadir")
            .no_env("GHA2DB_LOCAL")
            .env("GHA2DB_DATADIR", leak(&dir))
            .debug("1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains(&format!(
            "Read 7 columns configs from '{dir}metrics/testproj/columns.yaml'\n"
        )),
        "{out}"
    );
    assert_eq!(cols_of(&side, "sact").len(), 9);
}

#[test]
fn default_datadir_is_etc_gha2db() {
    let side = both(&Case::new("etc").no_env("GHA2DB_LOCAL")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec![
            "Error: 'open /etc/gha2db/metrics/shared/columns.yaml: no such file or directory'"
                .to_string()
        ]
    );
}

#[test]
fn qout_echoes_every_statement() {
    let side = both(
        &Case::new("qout")
            .env("GHA2DB_QOUT", "1")
            .yaml("metrics/testproj/columns_one.yaml"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    for sql in [
        "select \"sig_mentions_labels_value\" from \"tsig_mentions_labels\" order by time asc\n",
        "select tablename from pg_catalog.pg_tables where schemaname = 'public' and tablename ~ $1 order by tablename\n[1:^s(commits|event_types)$ ]\n",
        "select column_name from information_schema.columns where table_schema = 'public' and table_name = $1\n[1:scommits ]\n",
        "alter table \"scommits\" add column if not exists \"sig_api_machinery\" double precision\nAdded column \"sig_api_machinery\" to 'scommits' table\n",
        "alter table \"sevent_types\" add column if not exists \"sig_node\" double precision\nAdded column \"sig_node\" to 'sevent_types' table\n",
        "update \"scommits\" set \"sig_api_machinery\" = 0.0, \"sig_node\" = 0.0\n",
        "alter table \"sevent_types\" alter column \"sig_api_machinery\" set not null, alter column \"sig_api_machinery\" set default 0.0, alter column \"sig_node\" set not null, alter column \"sig_node\" set default 0.0\n",
    ] {
        assert!(out.contains(sql), "missing {sql:?} in {out}");
    }
    assert!(!out.contains("drop column"), "{out}");
    // the multi-column statements of the testproj yaml compare after
    // normalisation (Go emits the clauses in map order)
    let side = both(&Case::new("qout_all").env("GHA2DB_QOUT", "1")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("alter table \"sact\" drop column if exists \"Stale\"\nDeleted column \"Stale\" from 'sact' table\n"), "{out}");
    assert_eq!(count_lines(&out, "update \""), 8, "{out}");
}

#[test]
fn empty_yamls_do_nothing() {
    for (name, yaml) in [
        ("empty", "metrics/shared/empty.yaml"),
        ("nocolumns", "devel/test_columns.yaml"),
    ] {
        let side = both(&Case::new(name).yaml(yaml).debug("2")).unwrap();
        assert_eq!(side.out().code(), 0);
        let out = stdout_of(&side);
        assert!(
            out.contains(&format!("Read 0 columns configs from './{yaml}'\n")),
            "{out}"
        );
        assert!(
            out.contains("\nTables: []\nColumns: []\nHLLs: []\nCfg: map[]\n"),
            "{out}"
        );
        assert_eq!(cols_of(&side, "sact").len(), 6);
    }
}

// ---------------------------------------------------------------------------
// Real yamls (files from cncf/devstats)
// ---------------------------------------------------------------------------

#[test]
fn real_shared_columns() {
    let side = both(&Case::new("shared").project("shared")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert_eq!(count_lines(&out, "Ensure column config: "), 8, "{out}");
    assert!(
        out.contains("Warning: no tag values for (users_name, tusers)\n"),
        "{out}"
    );
    assert_eq!(count_lines(&out, "Warning: '"), 0, "{out}");
    assert_eq!(cols_of(&side, "sact").len(), 9);
    assert_eq!(
        cols_of(&side, "suser_reviews"),
        ["time", "period", "alice", "bob", "carol"]
    );
    assert_eq!(
        cols_of(&side, "siclosed"),
        ["time", "period", "all", "kubernetes", "docs"]
    );
    assert_eq!(
        cols_of(&side, "siopened"),
        ["time", "period", "all", "kubernetes", "docs"]
    );
    assert_eq!(
        cols_of(&side, "spr_appr_by_group"),
        ["time", "period", "all", "kubernetes", "docs"]
    );
    assert_eq!(
        cols_of(&side, "sevent_types"),
        [
            "time",
            "period",
            "PushEvent",
            "PullRequestEvent",
            "IssuesEvent"
        ]
    );
    assert_eq!(cols_of(&side, "scountries_cum").len(), 7);
    assert_eq!(
        cols_of(&side, "scompany_activity_repos"),
        ["time", "period"]
    );
    assert_eq!(cols_of(&side, "sprjcntr"), ["time", "series", "period"]);
}

#[test]
fn real_all_columns_with_hll() {
    let side = both(&Case::new("all").project("all").with_hll()).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert_eq!(count_lines(&out, "Ensure column config: "), 9, "{out}");
    assert!(out.contains("Ensure column config: &{TableRegexp:^sprjcntr$ Tag:trepo_groups Column:repo_group_name HLL:true}\n"), "{out}");
    let con = side.db.as_ref().unwrap().conn();
    let available = cpg::hll_available(&con);
    con.close();
    if available {
        assert_eq!(
            col_info(&side, "sprjcntr", "Kubernetes"),
            s3(("USER-DEFINED", "NO", "hll_empty()"))
        );
        assert_eq!(cols_of(&side, "sprjcntr").len(), 8);
        assert!(
            out.contains("Mass updated \"sprjcntr\", columns: 5, took: "),
            "{out}"
        );
    } else {
        assert_eq!(
            count_lines(&out, "Error handle row is too big add column "),
            5,
            "{out}"
        );
        assert_eq!(cols_of(&side, "sprjcntr"), ["time", "series", "period"]);
    }
    assert_eq!(cols_of(&side, "sact").len(), 9);
}

#[test]
fn real_kubernetes_columns() {
    let side = both(&Case::new("k8s").project("kubernetes")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert_eq!(count_lines(&out, "Ensure column config: "), 10, "{out}");
    assert_eq!(count_lines(&out, "Warning: "), 0, "{out}");
    assert_eq!(
        cols_of(&side, "ssig_pr_wl"),
        ["time", "period", "sig/api-machinery", "sig/node"]
    );
    assert_eq!(
        cols_of(&side, "sawaiting_prs"),
        ["time", "period", "sig/node", "sig/api-machinery"]
    );
    assert!(
        out.contains("Deleted column \"sig/old\" from 'sawaiting_prs' table\n"),
        "{out}"
    );
    assert_eq!(cols_of(&side, "sgh_stats_rgrp").len(), 7);
    assert_eq!(
        cols_of(&side, "spr_appr"),
        [
            "time",
            "period",
            "Kubernetes",
            "Docs",
            "Prometheus",
            "Envoy",
            "Ünïcode Group"
        ]
    );
    assert_eq!(
        cols_of(&side, "sgh_stats_r"),
        [
            "time",
            "period",
            "kubernetes/kubernetes",
            "kubernetes/website"
        ]
    );
    assert_eq!(
        cols_of(&side, "sbot_commands"),
        ["time", "period", "/lgtm", "/approve", "/retest"]
    );
    assert_eq!(cols_of(&side, "sbot_commands_repos").len(), 5);
    assert_eq!(cols_of(&side, "sbot_commands_other"), ["time", "period"]);
    assert_eq!(cols_of(&side, "scompany_activity").len(), 7);
    assert_eq!(cols_of(&side, "scompany_activity_repos").len(), 7);
    assert_eq!(cols_of(&side, "suser_reviews").len(), 5);
    assert_eq!(
        cols_of(&side, "ssigm_txt"),
        ["time", "period", "sig-node", "sig-api-machinery"]
    );
    assert_eq!(
        cols_of(&side, "sprblck_all"),
        ["time", "period", "All", "Kubernetes", "Docs"]
    );
    assert_eq!(cols_of(&side, "scountries").len(), 7);
    // not in the kubernetes yaml
    assert_eq!(cols_of(&side, "sact").len(), 6);
    assert_eq!(cols_of(&side, "siopened"), ["time", "period"]);
}

#[test]
fn real_affs_and_repo_groups_yamls() {
    for (name, project, yaml, act_cols) in [
        (
            "shared_affs",
            "shared",
            "metrics/shared/columns_affs.yaml",
            6,
        ),
        ("all_affs", "all", "metrics/all/columns_affs.yaml", 6),
        (
            "k8s_affs",
            "kubernetes",
            "metrics/kubernetes/columns_affs.yaml",
            6,
        ),
        (
            "shared_rg",
            "shared",
            "metrics/shared/columns_repo_groups.yaml",
            9,
        ),
        (
            "k8s_rg",
            "kubernetes",
            "metrics/kubernetes/columns_repo_groups.yaml",
            6,
        ),
    ] {
        let side = both(&Case::new(name).project(project).yaml(yaml)).unwrap();
        assert_eq!(side.out().code(), 0, "{name}");
        assert_eq!(cols_of(&side, "sact").len(), act_cols, "{name}");
        let out = stdout_of(&side);
        if yaml.contains("affs") {
            assert_eq!(cols_of(&side, "scompany_activity").len(), 7, "{name}");
            assert_eq!(cols_of(&side, "scountries").len(), 7, "{name}");
            assert!(!out.contains("Deleted column \"Stale\""), "{name}: {out}");
        } else {
            assert_eq!(cols_of(&side, "scompany_activity").len(), 4, "{name}");
        }
    }
}
