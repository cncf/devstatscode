//! Go ⇄ Rust compatibility tests for `calc_metric`.
//!
//! Every case gets its own scratch database per side
//! (`dbtest_calc_metric_<case>_<go|rs>`, built from
//! `compat/fixtures/calc_metric/schema.sql`: the two bookkeeping tables,
//! a small deterministic `gha_events` log and the `tquick_ranges` tags
//! table the annotations tool fills) and a scratch directory: the binaries
//! run in `<dir>/work` with `GHA2DB_LOCAL=1` (so `{{exclude_bots}}` comes
//! from `./util_sql/exclude_bots.sql`) unless a case says otherwise. The
//! metric SQL files are the fixtures under `compat/fixtures/calc_metric/`
//! (`{fx}` in the arguments), one per code path: single value, multi-row
//! single/multi column, `single_row_multi_column`, multivalue, custom data,
//! histograms (plain / annotations ranges / `range:` periods), HLL columns,
//! temp tables and the error paths.
//!
//! Compared per run: exit code, stdout (the `Time(…)` line and the
//! `Error(time=…)` stamp masked; the DDL / `grant` / `Ignored grant` lines
//! as a sorted multiset — Go emits them per table in map order; everything
//! sorted for multi-threaded runs; in `GHA2DB_DEBUG` output the `added`
//! time of every point masked), the `Error: '…'` / `PqError:` stderr lines
//! (or only their count where the wording legitimately differs) and
//! afterwards every series table (columns with types, indexes and rows) plus
//! the `gha_last_computed` / `gha_computed` bookkeeping.
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise); the HLL cases additionally need the `hll` extension to be
//! installable and are skipped when it is not. The unit tests of the binary
//! cover the pure helpers (naming functions, option parsing).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{fixture, go_binary, run, rust_binary, Invocation, Outcome};
use regex::Regex;
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("calc_metric")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_calc_metric"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";

/// Tables created by `schema.sql` — everything else in the database was
/// created by the tool under test.
const FIXTURE_TABLES: &[&str] = &[
    "gha_actors",
    "gha_computed",
    "gha_events",
    "gha_last_computed",
    "tquick_ranges",
];

/// Prefixes of the lines Go prints per series table in map iteration order.
const UNORDERED_PREFIXES: &[&str] = &[
    "Ignored grant select on",
    "create table if not exists",
    "create index if not exists",
    "grant select on",
    "alter table",
];

static ERROR_TIME: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"Error\(time=[^)]*\)").unwrap());
/// `NewTSPoint: <time> <added> <name> …` / `AddTSPoint: …` / `#<n> …` debug
/// lines: the `added` stamp is `now()`.
static ADDED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^((?:NewTSPoint:|AddTSPoint:|#\d+) \d{4}-\d{2}-\d{2} \d+) \d{4}-\d{2}-\d{2} \d+ ")
        .unwrap()
});
/// The usage lines are logged before the context is set up, so they carry a
/// timestamp even with `GHA2DB_SKIPTIME`.
static TIMESTAMP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} ").unwrap());

/// One step of a case.
#[derive(Clone)]
enum Step {
    /// Run both binaries with the case's arguments plus extra environment
    /// and compare.
    Run(Vec<(&'static str, &'static str)>),
    /// Run both binaries with these arguments instead of the case's.
    RunArgs(Vec<&'static str>),
    /// Execute SQL on the side's database.
    Sql(&'static str),
}

struct Case {
    name: &'static str,
    /// Command line arguments (`{fx}` expands to the fixture directory,
    /// `{dir}` to the scratch directory).
    args: Vec<&'static str>,
    /// Environment of every run (same placeholders).
    env: Vec<(&'static str, &'static str)>,
    /// `GHA2DB_LOCAL=1` (`util_sql/` in the current directory) — off for the
    /// `GHA2DB_DATADIR` case.
    local: bool,
    /// Provide `util_sql/exclude_bots.sql`.
    exclude_bots: bool,
    /// Needs the `hll` extension (case skipped when unavailable).
    hll: bool,
    /// SQL executed on the fresh database before the first run.
    seed: Vec<&'static str>,
    steps: Vec<Step>,
    /// Compare stdout as a sorted multiset (concurrent workers).
    sorted: bool,
    /// Compare stdout at all (off for concurrent runs that die on an error:
    /// the interleaving decides how far the workers got).
    stdout: bool,
    /// Compare the `Error: '…'` lines (off when their wording legitimately
    /// differs).
    compare_errors: bool,
}

impl Case {
    fn new(name: &'static str, args: &[&'static str]) -> Self {
        Case {
            name,
            args: args.to_vec(),
            env: vec![("GHA2DB_NCPUS", "1")],
            local: true,
            exclude_bots: true,
            hll: false,
            seed: Vec::new(),
            steps: vec![Step::Run(Vec::new())],
            sorted: false,
            stdout: true,
            compare_errors: true,
        }
    }
    fn env(mut self, k: &'static str, v: &'static str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self.env.push((k, v));
        self
    }
    fn threads(mut self, n: &'static str) -> Self {
        self = self.env("GHA2DB_NCPUS", n);
        if n != "1" {
            self.sorted = true;
        }
        self
    }
    fn no_local(mut self) -> Self {
        self.local = false;
        self
    }
    fn no_exclude_bots(mut self) -> Self {
        self.exclude_bots = false;
        self
    }
    fn hll(mut self) -> Self {
        self.hll = true;
        self
    }
    fn steps(mut self, steps: Vec<Step>) -> Self {
        self.steps = steps;
        self
    }
    /// Only exit codes, the presence of an error and the database contents.
    fn code_only(mut self) -> Self {
        self.stdout = false;
        self.compare_errors = false;
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
}

/// One series table: columns (sorted by name), indexes and rows (every
/// column as text, ordered by all columns).
type TableDump = (Vec<cpg::ColumnInfo>, Vec<cpg::IndexInfo>, Vec<Vec<String>>);

struct Side {
    db: TestDb,
    _dir: TempDir,
    /// The scratch directory path (masked as `<dir>` in stdout and data).
    dir_str: String,
    outs: Vec<Outcome>,
}

fn unordered_line(l: &str) -> bool {
    UNORDERED_PREFIXES.iter().any(|p| l.starts_with(p))
}

/// Go builds the column list of a `create table if not exists "…"(…)`
/// debug line by iterating a map (random order); Rust sorts the columns.
/// Sort the column definitions on both sides so the lines compare equal.
fn sort_create_table_columns(l: &str) -> String {
    if !l.starts_with("create table if not exists") {
        return l.to_string();
    }
    let Some(open) = l.find('(') else {
        return l.to_string();
    };
    let (head, rest) = l.split_at(open + 1);
    // Series tables end with `, primary key(time, period))`, tag tables with `)`.
    let (cols, tail) = match rest.rfind(", primary key(") {
        Some(pk) => (&rest[..pk], &rest[pk..]),
        None => (&rest[..rest.len() - 1], &rest[rest.len() - 1..]),
    };
    let mut parts: Vec<&str> = cols.split(", ").collect();
    parts.sort_unstable();
    format!("{head}{}{tail}", parts.join(", "))
}

impl Side {
    fn mask(&self, l: &str) -> String {
        let l = l.replace(&self.dir_str, "<dir>");
        if l.starts_with("Time(") || l.starts_with("Time: ") {
            return "Time: <masked>".to_string();
        }
        let l = sort_create_table_columns(&l);
        let l = ERROR_TIME.replace_all(&l, "Error(time=<masked>)");
        let l = ADDED.replace(&l, "$1 <added> ");
        TIMESTAMP.replace(&l, "<ts> ").into_owned()
    }
    /// stdout of run `i` split into the ordered lines and the sorted
    /// multiset of the order-free lines (all of them when `sorted`).
    fn stdout(&self, i: usize, sorted: bool) -> (Vec<String>, Vec<String>) {
        let mut ordered = Vec::new();
        let mut unordered = Vec::new();
        for l in self.outs[i].stdout_str().lines() {
            let l = self.mask(l);
            if sorted || unordered_line(&l) {
                unordered.push(l);
            } else {
                ordered.push(l);
            }
        }
        unordered.sort();
        (ordered, unordered)
    }
    /// All stdout lines of run `i`, masked.
    fn lines(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stdout_str()
            .lines()
            .map(|l| self.mask(l))
            .collect()
    }
    /// Assert run `i` printed `line` (listing all lines otherwise).
    fn expect_line(&self, i: usize, line: &str) {
        let lines = self.lines(i);
        assert!(
            lines.iter().any(|l| l == line),
            "missing {line:?} in run #{i}: {lines:#?}"
        );
    }
    fn expect_no_line(&self, i: usize, line: &str) {
        let lines = self.lines(i);
        assert!(
            !lines.iter().any(|l| l == line),
            "unexpected {line:?} in run #{i}: {lines:#?}"
        );
    }
    /// Assert run `i` printed a line starting with `prefix`.
    fn expect_prefix(&self, i: usize, prefix: &str) {
        let lines = self.lines(i);
        assert!(
            lines.iter().any(|l| l.starts_with(prefix)),
            "no line starting with {prefix:?} in run #{i}: {lines:#?}"
        );
    }
    fn stderr_lines(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stderr_str()
            .lines()
            .filter(|l| l.starts_with("Error: '") || l.starts_with("PqError: "))
            .map(|l| l.replace(&self.dir_str, "<dir>"))
            .collect()
    }
    /// The `Error: '…'` message of run `i`.
    fn error(&self, i: usize) -> Option<String> {
        self.stderr_lines(i).into_iter().find_map(|l| {
            l.strip_prefix("Error: '")
                .and_then(|r| r.strip_suffix('\''))
                .map(str::to_string)
        })
    }
    fn code(&self, i: usize) -> Option<i32> {
        self.outs[i].code
    }
    /// Tables created by the tool (everything not in `schema.sql`), sorted.
    fn tables(&self) -> Vec<String> {
        let con = self.db.conn();
        let t: Vec<String> = cpg::tables(&con)
            .into_iter()
            .filter(|t| !FIXTURE_TABLES.contains(&t.as_str()))
            .collect();
        con.close();
        t
    }
    /// Column names of `table`, sorted.
    fn columns(&self, table: &str) -> Vec<String> {
        let con = self.db.conn();
        let mut c: Vec<String> = cpg::table_columns(&con, table)
            .into_iter()
            .map(|c| c.0)
            .collect();
        con.close();
        c.sort();
        c
    }
    fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let con = self.db.conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows
    }
    /// One column of `table` as text (`<null>` for NULL), ordered by `order`.
    fn column(&self, table: &str, column: &str, order: &str) -> Vec<String> {
        self.query(&format!(
            "select coalesce({column}::text, '<null>') from \"{table}\" order by {order}"
        ))
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
    }
    fn count(&self, table: &str) -> i64 {
        self.query(&format!("select count(*) from \"{table}\""))[0][0]
            .parse()
            .unwrap()
    }
    /// `(metric, command)` rows of `gha_last_computed`, the scratch
    /// directory masked.
    fn last_computed(&self) -> Vec<(String, String)> {
        self.query("select metric, command from gha_last_computed order by 1")
            .into_iter()
            .map(|r| (r[0].clone(), r[1].replace(&self.dir_str, "<dir>")))
            .collect()
    }
    /// `(metric, dt)` rows of `gha_computed`.
    fn computed(&self) -> Vec<(String, String)> {
        self.query("select metric, dt::text from gha_computed order by 1, 2")
            .into_iter()
            .map(|r| (r[0].clone(), r[1].clone()))
            .collect()
    }
    /// Every series table with its structure and contents plus the
    /// bookkeeping tables.
    fn data(&self) -> BTreeMap<String, TableDump> {
        let con = self.db.conn();
        let mut res = BTreeMap::new();
        for t in cpg::tables(&con) {
            if FIXTURE_TABLES.contains(&t.as_str()) {
                continue;
            }
            let mut cols = cpg::table_columns(&con, &t);
            cols.sort();
            let idx = cpg::table_indexes(&con, &t);
            let names: Vec<String> = cols.iter().map(|c| format!("\"{}\"::text", c.0)).collect();
            let order: Vec<String> = (1..=names.len()).map(|i| i.to_string()).collect();
            let rows = cpg::snapshot(
                &con,
                &format!(
                    "select {} from \"{t}\" order by {}",
                    names.join(", "),
                    order.join(", ")
                ),
                &[],
            )
            .rows;
            res.insert(t, (cols, idx, rows));
        }
        con.close();
        let lc: Vec<Vec<String>> = self
            .last_computed()
            .into_iter()
            .map(|(m, c)| vec![m, c])
            .collect();
        res.insert("gha_last_computed".into(), (Vec::new(), Vec::new(), lc));
        let c: Vec<Vec<String>> = self
            .computed()
            .into_iter()
            .map(|(m, d)| vec![m, d])
            .collect();
        res.insert("gha_computed".into(), (Vec::new(), Vec::new(), c));
        res
    }
}

fn fixture_dir() -> String {
    fixture("calc_metric").to_str().unwrap().to_string()
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let db = TestDb::fresh(&format!("calc_metric_{}_{}", case.name, suffix))?;
    db.exec(&fs::read_to_string(fixture("calc_metric/schema.sql")).unwrap());
    if case.hll {
        db.exec("create extension if not exists hll");
    }
    for sql in &case.seed {
        db.exec(sql);
    }
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_calc_metric_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    let work = dir.path().join("work");
    let data = dir.path().join("data");
    fs::create_dir_all(&work).unwrap();
    fs::create_dir_all(&data).unwrap();
    if case.exclude_bots {
        let base = if case.local { &work } else { &data };
        fs::create_dir_all(base.join("util_sql")).unwrap();
        fs::copy(
            fixture("calc_metric/exclude_bots.sql"),
            base.join("util_sql/exclude_bots.sql"),
        )
        .unwrap();
    }
    let dir_str = dir.path().to_str().unwrap().to_string();
    let fx = fixture_dir();
    let expand = |s: &str| s.replace("{dir}", &dir_str).replace("{fx}", &fx);
    let mut env: Vec<(String, String)> = db
        .env()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    if case.local {
        env.push(("GHA2DB_LOCAL".into(), "1".into()));
    }
    for (k, v) in &case.env {
        env.retain(|(key, _)| key != k);
        env.push((k.to_string(), expand(v)));
    }
    let mut outs = Vec::new();
    for step in &case.steps {
        let (extra, args): (Vec<(&str, &str)>, &Vec<&str>) = match step {
            Step::Run(extra) => (extra.clone(), &case.args),
            Step::RunArgs(args) => (Vec::new(), args),
            Step::Sql(sql) => {
                db.exec(sql);
                continue;
            }
        };
        let mut run_env = env.clone();
        for (k, v) in extra {
            run_env.retain(|(key, _)| key != k);
            run_env.push((k.to_string(), expand(v)));
        }
        let mut inv = Invocation::new().cwd(work.clone());
        for (k, v) in &run_env {
            inv = inv.env(leak(k), leak(v));
        }
        for a in args {
            inv = inv.arg(expand(a));
        }
        outs.push(run(bin, &inv));
    }
    Some(Side {
        db,
        _dir: dir,
        dir_str,
        outs,
    })
}

/// Is the `hll` extension installable? `None` when the DB tests are skipped.
fn hll_ok(name: &str) -> Option<bool> {
    let db = TestDb::fresh(&format!("calc_metric_{name}_hll"))?;
    let con = db.conn();
    let ok = cpg::hll_available(&con);
    con.close();
    if !ok {
        eprintln!("[compat] hll extension not available — skipping {name}");
    }
    Some(ok)
}

/// Run both binaries through the case's steps and compare everything;
/// returns the Rust side for further assertions (`None` when the DB tests
/// are skipped or the case needs an unavailable extension).
fn both(case: &Case) -> Option<Side> {
    if case.hll && !hll_ok(case.name)? {
        return None;
    }
    let rust = run_side(&rust_bin(), case, "rs")?;
    if let Some(go) = go_bin() {
        let go = run_side(&go, case, "go").unwrap();
        assert_eq!(go.outs.len(), rust.outs.len());
        for i in 0..rust.outs.len() {
            let ctx = format!(
                "\ncase {:?} run #{i} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
                case.name,
                case.env,
                go.outs[i].code,
                go.outs[i].stdout_str(),
                go.outs[i].stderr_str(),
                rust.outs[i].code,
                rust.outs[i].stdout_str(),
                rust.outs[i].stderr_str(),
            );
            assert_eq!(go.outs[i].code, rust.outs[i].code, "exit code{ctx}");
            if !case.stdout {
                assert!(
                    go.stderr_lines(i).iter().any(|l| l.starts_with("Error: '")),
                    "go printed no error{ctx}"
                );
                assert!(
                    rust.stderr_lines(i)
                        .iter()
                        .any(|l| l.starts_with("Error: '")),
                    "rust printed no error{ctx}"
                );
                continue;
            }
            let (go_ordered, go_unordered) = go.stdout(i, case.sorted);
            let (rs_ordered, rs_unordered) = rust.stdout(i, case.sorted);
            assert_eq!(go_ordered, rs_ordered, "stdout{ctx}");
            assert_eq!(go_unordered, rs_unordered, "stdout (order-free lines){ctx}");
            if case.compare_errors {
                assert_eq!(go.stderr_lines(i), rust.stderr_lines(i), "stderr{ctx}");
            } else {
                assert_eq!(
                    go.stderr_lines(i).len(),
                    rust.stderr_lines(i).len(),
                    "stderr line count{ctx}"
                );
            }
        }
        assert_eq!(
            go.data(),
            rust.data(),
            "database contents (case {:?})",
            case.name
        );
    }
    Some(rust)
}

/// The `time, period, value` rows of a plain series table.
fn tpv(rs: &Side, table: &str) -> Vec<(String, String, String)> {
    rs.query(&format!(
        "select time::text, period, value::text from \"{table}\" order by time"
    ))
    .into_iter()
    .map(|r| (r[0].clone(), r[1].clone(), r[2].clone()))
    .collect()
}

fn s(v: &[(&str, &str, &str)]) -> Vec<(String, String, String)> {
    v.iter()
        .map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
        .collect()
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

const BY_TYPE_TABLES: &[&str] = &[
    "sevtforkevent",
    "sevtissuesevent",
    "sevtpullrequestevent",
    "sevtpushevent",
    "sevtwatchevent",
];

// ---------------------------------------------------------------- usage

#[test]
fn usage_without_arguments() {
    let Some(rs) = both(&Case::new("usage_no_args", &[])) else {
        return;
    };
    assert_eq!(rs.code(0), Some(1));
    let lines = rs.lines(0);
    assert_eq!(lines.len(), 6, "{lines:#?}");
    assert_eq!(lines[0], BANNER);
    assert!(
        lines[1].starts_with("Required series name, SQL file name, from, to, period [series_name_or_func some.sql '2015-08-03' '2017-08-21' h|d|w|m|q|y [hist,desc:time_diff_as_string,multivalue,"),
        "{lines:#?}"
    );
    assert!(
        lines[5].starts_with("Example run: GHA2DB_QOUT=1 "),
        "{lines:#?}"
    );
    assert!(rs.tables().is_empty());
    assert!(rs.last_computed().is_empty());
}

#[test]
fn usage_with_too_few_arguments() {
    let Some(rs) = both(&Case::new(
        "usage_few_args",
        &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-03"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(1));
    assert_eq!(rs.lines(0).len(), 6);
    assert!(rs.tables().is_empty());
}

// --------------------------------------------- single numeric value series

#[test]
fn count_daily() {
    let Some(rs) = both(&Case::new(
        "count_d",
        &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-03", "d"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["scnt"]));
    // bots (dependabot[bot], k8s-ci-robot) excluded; `to` is inclusive
    assert_eq!(
        tpv(&rs, "scnt"),
        s(&[
            ("2015-08-01 00:00:00", "d", "3"),
            ("2015-08-02 00:00:00", "d", "4"),
            ("2015-08-03 00:00:00", "d", "4"),
        ])
    );
    assert_eq!(rs.columns("scnt"), strs(&["period", "time", "value"]));
    let lc = rs.last_computed();
    assert_eq!(lc.len(), 1);
    assert_eq!(lc[0].0, "count d");
    assert!(lc[0].1.starts_with("cnt ") && lc[0].1.ends_with("/count.sql 2015-08-01 2015-08-03 d"));
    assert!(rs.computed().is_empty());
    rs.expect_line(0, BANNER);
    rs.expect_line(0, "Using single threaded version");
    rs.expect_line(
        0,
        "WriteTSPoints: writing 3 points in batches of up to 1000",
    );
    rs.expect_line(0, "All done.");
}

#[test]
fn count_hourly() {
    let Some(rs) = both(&Case::new(
        "count_h",
        &["cnt", "{fx}/count.sql", "2015-08-01 0", "2015-08-01 6", "h"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // alice pushes at 00:00, bob's PR at 05:00 — 7 hourly points, `to` inclusive
    assert_eq!(
        rs.column("scnt", "value", "time"),
        strs(&["1", "0", "0", "0", "0", "1", "0"])
    );
    assert_eq!(rs.column("scnt", "period", "time"), strs(&["h"; 7]));
}

#[test]
fn count_every_24_hours() {
    let Some(rs) = both(&Case::new(
        "count_h24",
        &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-03", "h24"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // `hN` steps every hour, each point covering the N hours ending there
    assert_eq!(rs.column("scnt", "period", "time"), strs(&["h24"; 49]));
    assert_eq!(
        rs.query(
            "select time::text, value::text from scnt where time in ('2015-08-01 00:00:00', \
             '2015-08-01 01:00:00', '2015-08-02 00:00:00', '2015-08-03 00:00:00') order by 1"
        ),
        vec![
            strs(&["2015-08-01 00:00:00", "1"]),
            strs(&["2015-08-01 01:00:00", "1"]),
            strs(&["2015-08-02 00:00:00", "2"]),
            strs(&["2015-08-03 00:00:00", "4"]),
        ]
    );
}

#[test]
fn count_every_two_weeks() {
    let Some(rs) = both(&Case::new(
        "count_w2",
        &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-13", "w2"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert!(rs.count("scnt") >= 1);
    assert_eq!(rs.last_computed()[0].0, "count w2");
}

#[test]
fn count_monthly() {
    let Some(rs) = both(&Case::new(
        "count_m",
        &["cnt", "{fx}/count.sql", "2015-07-01", "2015-09-01", "m"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // July: nothing, August: 42 non-bot events, September: nothing
    assert_eq!(
        tpv(&rs, "scnt"),
        s(&[
            ("2015-07-01 00:00:00", "m", "0"),
            ("2015-08-01 00:00:00", "m", "40"),
            ("2015-09-01 00:00:00", "m", "0"),
        ])
    );
}

#[test]
fn count_quarterly() {
    let Some(rs) = both(&Case::new(
        "count_q",
        &["cnt", "{fx}/count.sql", "2015-01-01", "2015-12-31", "q"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.column("scnt", "value", "time"),
        strs(&["0", "0", "40", "0"])
    );
    assert_eq!(rs.column("scnt", "period", "time"), strs(&["q"; 4]));
}

#[test]
fn count_yearly() {
    let Some(rs) = both(&Case::new(
        "count_y",
        &["cnt", "{fx}/count.sql", "2014-01-01", "2016-12-31", "y"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("scnt", "value", "time"), strs(&["0", "40", "0"]));
}

#[test]
fn count_multi_threaded() {
    let Some(rs) = both(
        &Case::new(
            "count_mt",
            &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-13", "d"],
        )
        .threads("4"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.column("scnt", "value", "time"),
        strs(&["3", "4", "4", "3", "2", "3", "4", "4", "3", "2", "3", "4", "1"])
    );
    assert!(
        rs.lines(0)
            .iter()
            .any(|l| l.contains("/count.sql: Running (on 4 CPUs): ")),
        "{:#?}",
        rs.lines(0)
    );
}

#[test]
fn count_with_project_scale() {
    let Some(rs) = both(&Case::new(
        "count_scaled",
        &[
            "cnts",
            "{fx}/count_scaled.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "project_scale:0.5",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // 5 events per day (bots included), scaled by 0.5
    assert_eq!(
        rs.column("scnts", "value", "time"),
        strs(&["2.5", "2.5", "2.5"])
    );
}

#[test]
fn count_with_invalid_project_scale_is_unscaled() {
    let Some(rs) = both(&Case::new(
        "count_scaled_bad",
        &[
            "cnts",
            "{fx}/count_scaled.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "project_scale:abc",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("scnts", "value", "time"), strs(&["5", "5", "5"]));
}

#[test]
fn count_with_negative_project_scale_is_unscaled() {
    let Some(rs) = both(&Case::new(
        "count_scaled_negative",
        &[
            "cnts",
            "{fx}/count_scaled.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "project_scale:-2",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("scnts", "value", "time"), strs(&["5", "5", "5"]));
}

#[test]
fn count_null_value_is_zero() {
    let Some(rs) = both(&Case::new(
        "count_null",
        &[
            "cntn",
            "{fx}/count_null.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("scntn", "value", "time"), strs(&["0", "0", "0"]));
}

#[test]
fn count_query_without_rows() {
    let Some(rs) = both(&Case::new(
        "count_norows",
        &[
            "cntn",
            "{fx}/count_norows.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("scntn", "value", "time"), strs(&["0", "0", "0"]));
    rs.expect_line(0, "Error:");
    rs.expect_line(
        0,
        "Query should return either single value or multiple rows, each containing string and numbers",
    );
    rs.expect_line(0, "Got 0 rows, each containing single number");
}

#[test]
fn count_query_with_several_single_number_rows() {
    let Some(rs) = both(&Case::new(
        "count_multirows",
        &[
            "cntn",
            "{fx}/count_multirows.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // the last row wins (actor of the last event of each day)
    assert_eq!(rs.column("scntn", "value", "time"), strs(&["5", "4", "3"]));
    rs.expect_line(0, "Got 5 rows, each containing single number");
}

#[test]
fn value_description_time_diff_as_string() {
    let Some(rs) = both(&Case::new(
        "hours_since_desc",
        &[
            "hs",
            "{fx}/hours_since.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "desc:time_diff_as_string",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.columns("shs"),
        strs(&["descr", "period", "time", "value"])
    );
    assert_eq!(rs.count("shs"), 3);
    for d in rs.column("shs", "descr", "time") {
        assert!(!d.is_empty() && d != "<null>", "descr {d:?}");
    }
}

#[test]
fn unknown_value_description_function() {
    let Some(rs) = both(&Case::new(
        "desc_unknown",
        &[
            "hs",
            "{fx}/hours_since.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "desc:nosuch",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(1));
    rs.expect_line(0, "Unknown value description function 'nosuch'");
    assert!(rs.tables().is_empty());
}

#[test]
fn from_after_to_single_threaded() {
    let Some(rs) = both(&Case::new(
        "from_after_to_st",
        &["cnt", "{fx}/count.sql", "2015-08-03", "2015-08-01", "d"],
    )) else {
        return;
    };
    // Go bug 36: this used to panic in the single threaded version
    assert_eq!(rs.code(0), Some(0));
    assert!(rs.tables().is_empty());
    assert_eq!(rs.last_computed().len(), 1);
}

#[test]
fn from_after_to_multi_threaded() {
    let Some(rs) = both(
        &Case::new(
            "from_after_to_mt",
            &["cnt", "{fx}/count.sql", "2015-08-03", "2015-08-01", "d"],
        )
        .threads("4"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert!(rs.tables().is_empty());
    assert_eq!(rs.last_computed().len(), 1);
}

// ------------------------------------------------ multi_row_single_column

#[test]
fn by_type_weekly_points_of_7_days() {
    let Some(rs) = both(&Case::new(
        "by_type_d7",
        &[
            "multi_row_single_column",
            "{fx}/by_type.sql",
            "2015-08-01",
            "2015-08-13",
            "d7",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(BY_TYPE_TABLES));
    for t in BY_TYPE_TABLES {
        assert_eq!(rs.columns(t), strs(&["period", "time", "value"]));
        assert!(rs.column(t, "period", "time").iter().all(|p| p == "d7"));
    }
    assert_eq!(rs.last_computed()[0].0, "by_type d7");
}

#[test]
fn by_type_with_range_hours_weekly() {
    let Some(rs) = both(&Case::new(
        "by_type_range_w",
        &[
            "multi_row_single_column",
            "{fx}/by_type_range.sql",
            "2015-08-01",
            "2015-08-13",
            "w",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "sevtrforkevent",
            "sevtrissuesevent",
            "sevtrpullrequestevent",
            "sevtrpushevent",
            "sevtrwatchevent",
        ])
    );
}

#[test]
fn by_type_with_odd_row_names() {
    let Some(rs) = both(&Case::new(
        "by_type_odd",
        &[
            "multi_row_single_column",
            "{fx}/by_type_odd.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // ',PushEvent' has an empty prefix and 'evt,-/-' normalizes to nothing
    assert_eq!(
        rs.tables(),
        strs(&["sevtforkevent", "sevtpullrequestevent", "sevtwatchevent"])
    );
    rs.expect_prefix(0, "multiRowSingleColumn: Info: prefix '' (");
    rs.expect_prefix(0, "multiRowSingleColumn: Info: rowName '-/-' (");
}

#[test]
fn by_type_skip_escape_series_name() {
    let Some(rs) = both(&Case::new(
        "by_type_skip_escape",
        &[
            "multi_row_single_column",
            "{fx}/by_type.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "skip_escape_series_name",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "sevtForkEvent",
            "sevtIssuesEvent",
            "sevtPullRequestEvent",
            "sevtPushEvent",
            "sevtWatchEvent",
        ])
    );
}

#[test]
fn by_type_multi_threaded() {
    let Some(rs) = both(
        &Case::new(
            "by_type_mt",
            &[
                "multi_row_single_column",
                "{fx}/by_type.sql",
                "2015-08-01",
                "2015-08-13",
                "d",
            ],
        )
        .threads("3"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(BY_TYPE_TABLES));
    // 8 non-bot events of every type over the 13 days
    for t in BY_TYPE_TABLES {
        let sum: f64 = rs
            .column(t, "value", "time")
            .iter()
            .map(|v| v.parse::<f64>().unwrap())
            .sum();
        assert_eq!(sum, 8.0, "{t}");
    }
}

#[test]
fn unknown_metric_function() {
    let Some(rs) = both(&Case::new(
        "unknown_metric_func",
        &[
            "no_such_func",
            "{fx}/by_type.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(1));
    rs.expect_line(0, "Error");
    rs.expect_line(0, "Unknown metric 'no_such_func'");
    assert!(rs.tables().is_empty());
}

// ------------------------------------------------- multi_row_multi_column

#[test]
fn by_repo_multi_column_monthly() {
    let Some(rs) = both(&Case::new(
        "by_repo_multi_m",
        &[
            "multi_row_multi_column",
            "{fx}/by_repo_multi.sql",
            "2015-07-01",
            "2015-09-01",
            "m",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "srgorgrepo1actors",
            "srgorgrepo1events",
            "srgorgrepotwoactors",
            "srgorgrepotwoevents",
            "srgotherrepo3actors",
            "srgotherrepo3events",
        ])
    );
    assert_eq!(
        tpv(&rs, "srgorgrepotwoevents"),
        s(&[("2015-08-01 00:00:00", "m", "10")])
    );
    assert_eq!(
        tpv(&rs, "srgorgrepotwoactors"),
        s(&[("2015-08-01 00:00:00", "m", "1")])
    );
}

#[test]
fn by_repo_multi_column_with_odd_row_names() {
    let Some(rs) = both(&Case::new(
        "by_repo_multi_odd",
        &[
            "multi_row_multi_column",
            "{fx}/by_repo_multi_odd.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&["srgorgrepotwoactors", "srgorgrepotwoevents"])
    );
    rs.expect_prefix(0, "multiRowMultiColumn: Info: prefix '' (");
    rs.expect_prefix(0, "multiRowMultiColumn: Info: rowName '(-)' (");
}

#[test]
fn single_row_multi_column_yearly() {
    let Some(rs) = both(&Case::new(
        "single_row_multi_y",
        &[
            "single_row_multi_column",
            "{fx}/single_row_multi.sql",
            "2014-01-01",
            "2016-12-31",
            "y",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["sissues", "sprs", "spushes"]));
    assert_eq!(
        rs.column("spushes", "value", "time"),
        strs(&["0", "12", "0"])
    );
    assert_eq!(rs.column("sprs", "value", "time"), strs(&["0", "12", "0"]));
    assert_eq!(
        rs.column("sissues", "value", "time"),
        strs(&["0", "12", "0"])
    );
}

#[test]
fn series_name_map_single_column() {
    let Some(rs) = both(&Case::new(
        "series_name_map_single",
        &[
            "multi_row_single_column",
            "{fx}/by_type.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "series_name_map:map[issuesevent:issues pushevent:pushes]",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "sevtforkevent",
            "sevtissues",
            "sevtpullrequestevent",
            "sevtpushes",
            "sevtwatchevent",
        ])
    );
}

#[test]
fn series_name_map_multi_column() {
    let Some(rs) = both(&Case::new(
        "series_name_map_multi",
        &[
            "multi_row_multi_column",
            "{fx}/by_repo_multi.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "series_name_map:map[orgrepo1:one orgrepotwo:two]",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "srgoneactors",
            "srgoneevents",
            "srgotherrepo3actors",
            "srgotherrepo3events",
            "srgtwoactors",
            "srgtwoevents",
        ])
    );
}

// ------------------------------------------------------------- multivalue

#[test]
fn multivalue_by_type() {
    let Some(rs) = both(&Case::new(
        "mv_by_type",
        &[
            "multi_row_single_column",
            "{fx}/mv_by_type.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "multivalue",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["smv"]));
    // one column per event type (not escaped)
    assert_eq!(
        rs.columns("smv"),
        strs(&[
            "ForkEvent",
            "IssuesEvent",
            "PullRequestEvent",
            "PushEvent",
            "WatchEvent",
            "period",
            "time",
        ])
    );
    assert_eq!(rs.count("smv"), 3);
}

#[test]
fn multivalue_by_type_escaped_value_names() {
    let Some(rs) = both(&Case::new(
        "mv_by_type_escape",
        &[
            "multi_row_single_column",
            "{fx}/mv_by_type.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "multivalue,escape_value_name",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.columns("smv"),
        strs(&[
            "forkevent",
            "issuesevent",
            "period",
            "pullrequestevent",
            "pushevent",
            "time",
            "watchevent",
        ])
    );
}

#[test]
fn multivalue_by_type_and_repo_escaped() {
    let Some(rs) = both(&Case::new(
        "mv_by_type_repo_escape",
        &[
            "multi_row_single_column",
            "{fx}/mv_by_type_repo.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "multivalue,escape_value_name",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // the part after ` becomes the series name, the part before a column
    assert_eq!(
        rs.tables(),
        strs(&["smvorgrepo1", "smvorgrepotwo", "smvotherrepo3"])
    );
}

#[test]
fn multivalue_by_type_and_repo_skip_escape_series_name() {
    let Some(rs) = both(&Case::new(
        "mv_by_type_repo_skip_escape",
        &[
            "multi_row_single_column",
            "{fx}/mv_by_type_repo.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "multivalue,skip_escape_series_name",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&["smvorg/Repo.Two", "smvorg/repo1", "smvother/repo3"])
    );
}

#[test]
fn multivalue_merged_series_multi_column() {
    let Some(rs) = both(&Case::new(
        "mvm_by_repo",
        &[
            "multi_row_multi_column",
            "{fx}/mvm_by_repo.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "multivalue,merge_series:mvm",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["smvm"]));
    // one row per (time, series) with a value column per repository
    assert_eq!(
        rs.columns("smvm"),
        strs(&[
            "org/Repo.Two",
            "org/repo1",
            "other/repo3",
            "period",
            "series",
            "time"
        ])
    );
    assert_eq!(
        rs.query("select time::text, series, \"org/repo1\"::text from smvm order by 1, 2"),
        vec![
            strs(&["2015-08-01 00:00:00", "mvmactors", "2"]),
            strs(&["2015-08-01 00:00:00", "mvmevents", "2"]),
            strs(&["2015-08-02 00:00:00", "mvmactors", "2"]),
            strs(&["2015-08-02 00:00:00", "mvmevents", "2"]),
            strs(&["2015-08-03 00:00:00", "mvmactors", "1"]),
            strs(&["2015-08-03 00:00:00", "mvmevents", "1"]),
        ]
    );
}

#[test]
fn multivalue_merged_series_multi_column_with_backtick_names() {
    let Some(rs) = both(&Case::new(
        "mvm_by_repo_type",
        &[
            "multi_row_multi_column",
            "{fx}/mvm_by_repo_type.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "multivalue,merge_series:mvm,escape_value_name",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["smvm"]));
    assert!(rs.columns("smvm").len() > 3);
}

// ------------------------------------------------------------ custom data

#[test]
fn custom_data_merged() {
    let Some(rs) = both(&Case::new(
        "custom",
        &[
            "multi_row_single_column",
            "{fx}/custom.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "custom_data,merge_series:ncd",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["sncd"]));
    // one point per non-bot event, keyed by its own time
    assert_eq!(rs.count("sncd"), 11);
    assert_eq!(
        rs.columns("sncd"),
        strs(&["dt", "period", "series", "str", "time", "value"])
    );
}

#[test]
fn custom_data_merged_unique_time() {
    let Some(rs) = both(&Case::new(
        "custom_unique",
        &[
            "multi_row_single_column",
            "{fx}/custom.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "custom_data,custom_data_unique_time,merge_series:ncd",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.count("sncd"), 11);
}

#[test]
fn custom_data_same_time_collapses() {
    let Some(rs) = both(&Case::new(
        "custom_same_time",
        &[
            "multi_row_single_column",
            "{fx}/custom_same_time.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "custom_data",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["sncdall"]));
    // every row of a day shares the day's `{{from}}` — one point per day
    assert_eq!(rs.count("sncdall"), 3);
}

#[test]
fn custom_data_same_time_made_unique() {
    let Some(rs) = both(&Case::new(
        "custom_same_time_unique",
        &[
            "multi_row_single_column",
            "{fx}/custom_same_time.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "custom_data,custom_data_unique_time",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // times are shifted by a second while they collide: 5 rows per day
    assert_eq!(rs.count("sncdall"), 15);
}

#[test]
fn custom_data_null_time() {
    let Some(rs) = both(&Case::new(
        "custom_null_time",
        &[
            "multi_row_single_column",
            "{fx}/custom_null_time.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "custom_data",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(1));
    assert!(rs.tables().is_empty());
}

#[test]
fn custom_data_unique_time_without_custom_data() {
    let Some(rs) = both(&Case::new(
        "custom_unique_alone",
        &[
            "multi_row_single_column",
            "{fx}/custom.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "custom_data_unique_time",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.error(0).is_some());
    assert!(rs.tables().is_empty());
}

#[test]
fn custom_data_multi_column() {
    let Some(rs) = both(&Case::new(
        "custom_multi",
        &[
            "multi_row_multi_column",
            "{fx}/custom_multi.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "custom_data",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "sncmorgrepo1first",
            "sncmorgrepo1last",
            "sncmorgrepotwofirst",
            "sncmorgrepotwolast",
            "sncmotherrepo3first",
            "sncmotherrepo3last",
        ])
    );
    assert_eq!(
        rs.columns("sncmorgrepo1first"),
        strs(&["dt", "period", "str", "time", "value"])
    );
}

#[test]
fn multivalue_custom_data() {
    let Some(rs) = both(&Case::new(
        "mv_custom",
        &[
            "multi_row_single_column",
            "{fx}/mv_custom.sql",
            "2015-08-01",
            "2015-08-03",
            "d",
            "multivalue,custom_data",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["smvc"]));
    // every value is a (time, value, string) triple of columns
    let cols = rs.columns("smvc");
    for c in ["PushEvent_t", "PushEvent_v", "PushEvent_s", "ForkEvent_v"] {
        assert!(cols.contains(&c.to_string()), "{cols:?}");
    }
    assert_eq!(rs.count("smvc"), 3);
}

// ------------------------------------------------------------------ misc

#[test]
fn temp_tables_in_metric_sql() {
    let Some(rs) = both(&Case::new(
        "temp_tables",
        &[
            "multi_row_single_column",
            "{fx}/temp_tables.sql",
            "2015-08-01",
            "2015-08-05",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "sttforkevent",
            "sttissuesevent",
            "sttpullrequestevent",
            "sttpushevent",
            "sttwatchevent",
        ])
    );
    // bots included: one event of every type per day, but no fork on 08-05
    assert_eq!(rs.column("sttforkevent", "value", "time"), strs(&["1"; 4]));
    for t in [
        "sttissuesevent",
        "sttpullrequestevent",
        "sttpushevent",
        "sttwatchevent",
    ] {
        assert_eq!(rs.column(t, "value", "time"), strs(&["1"; 5]), "{t}");
    }
}

#[test]
fn temp_tables_without_final_drop() {
    let Some(rs) = both(&Case::new(
        "temp_tables_last",
        &[
            "multi_row_single_column",
            "{fx}/temp_tables_last.sql",
            "2015-08-01",
            "2015-08-05",
            "d",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables().len(), 5);
}

#[test]
fn bad_sql_single_threaded() {
    let Some(rs) = both(&Case::new(
        "bad_sql_st",
        &["cnt", "{fx}/bad.sql", "2015-08-01", "2015-08-03", "d"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(
        rs.error(0)
            .unwrap()
            .contains("relation \"no_such_table\" does not exist"),
        "{:?}",
        rs.error(0)
    );
    rs.expect_line(0, "PqError: code=42P01, name=undefined_table, detail=");
    assert!(rs.tables().is_empty());
    // the deferred bookkeeping still runs on a fatal error
    assert_eq!(rs.last_computed().len(), 1);
}

#[test]
fn bad_sql_multi_threaded() {
    let Some(rs) = both(
        &Case::new(
            "bad_sql_mt",
            &["cnt", "{fx}/bad.sql", "2015-08-01", "2015-08-13", "d"],
        )
        .threads("4")
        .code_only(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.tables().is_empty());
}

#[test]
fn missing_sql_file() {
    let Some(rs) = both(&Case::new(
        "missing_sql_file",
        &["cnt", "{dir}/nosuch.sql", "2015-08-01", "2015-08-03", "d"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(
        rs.error(0).unwrap().contains("<dir>/nosuch.sql"),
        "{:?}",
        rs.error(0)
    );
    assert!(rs.tables().is_empty());
    assert!(rs.last_computed().is_empty());
}

#[test]
fn exclude_bots_from_data_dir() {
    let Some(rs) = both(
        &Case::new(
            "datadir_mode",
            &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-03", "d"],
        )
        .no_local()
        .env("GHA2DB_DATADIR", "{dir}/data/"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("scnt", "value", "time"), strs(&["3", "4", "4"]));
}

#[test]
fn missing_exclude_bots_file() {
    let Some(rs) = both(
        &Case::new(
            "missing_exclude_bots",
            &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-03", "d"],
        )
        .no_exclude_bots(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(
        rs.error(0).unwrap().contains("util_sql/exclude_bots.sql"),
        "{:?}",
        rs.error(0)
    );
    assert!(rs.tables().is_empty());
}

#[test]
fn unknown_interval() {
    let Some(rs) = both(&Case::new(
        "unknown_interval",
        &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-03", "zz"],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(1));
    assert!(rs.tables().is_empty());
}

#[test]
fn skip_tsdb_writes_nothing() {
    let Some(rs) = both(
        &Case::new(
            "skip_tsdb",
            &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-03", "d"],
        )
        .env("GHA2DB_SKIPTSDB", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert!(rs.tables().is_empty());
    assert!(rs.last_computed().is_empty());
    rs.expect_line(0, "All done.");
}

#[test]
fn debug_output_by_type() {
    let Some(rs) = both(
        &Case::new(
            "debug_by_type",
            &[
                "multi_row_single_column",
                "{fx}/by_type.sql",
                "2015-08-01",
                "2015-08-08",
                "d7",
            ],
        )
        .env("GHA2DB_DEBUG", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(BY_TYPE_TABLES));
    rs.expect_line(0, "nameForMetricsRow: evt,IssuesEvent -> [evtissuesevent]");
    rs.expect_line(0, "structural sqls:");
    rs.expect_prefix(
        0,
        "NewTSPoint: 2015-08-01 0 <added> evtpushevent period: d7 ",
    );
    rs.expect_prefix(0, "upserts: ");
}

#[test]
fn debug_output_count() {
    let Some(rs) = both(
        &Case::new(
            "debug_count",
            &["cnt", "{fx}/count.sql", "2015-08-01", "2015-08-02", "d"],
        )
        .env("GHA2DB_DEBUG", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_prefix(0, "lib.ReadFile('");
    rs.expect_line(
        0,
        "<ts> +0000 UTC - 2015-08-02 00:00:00 +0000 UTC -> cnt, 3",
    );
}

// ------------------------------------------------------------------ drop

const DROP_FIRST: &[&str] = &[
    "multi_row_single_column",
    "{fx}/by_type.sql",
    "2015-08-01",
    "2015-08-03",
    "d",
];
const DROP_SECOND: &[&str] = &[
    "multi_row_single_column",
    "{fx}/by_type.sql",
    "2015-08-01",
    "2015-08-03",
    "d",
    "drop:sevtpushevent;sevtwatchevent;snosuch",
];
/// A view depending on one table to drop and a stale row in the other.
const DROP_PREP: &[&str] = &[
    "create view vw as select * from sevtpushevent",
    "insert into sevtwatchevent(time, period, value) values ('2010-01-01', 'd', 99)",
];

fn drop_steps() -> Vec<Step> {
    let mut v = vec![Step::RunArgs(DROP_FIRST.to_vec())];
    v.extend(DROP_PREP.iter().map(|sql| Step::Sql(sql)));
    v.push(Step::RunArgs(DROP_SECOND.to_vec()));
    v
}

#[test]
fn drop_series_enabled() {
    let Some(rs) = both(
        &Case::new("drop_enabled", &[])
            .env("GHA2DB_ENABLE_METRICS_DROP", "1")
            .steps(drop_steps()),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(1, "Truncating table sevtpushevent");
    rs.expect_prefix(
        1,
        "warning: failed dropping table 'sevtpushevent': pq: cannot drop table sevtpushevent because other objects depend on it",
    );
    rs.expect_line(1, "Truncating table sevtwatchevent");
    rs.expect_no_line(1, "Truncating table snosuch");
    // recreated: the stale row is gone; the other table kept its rows
    assert_eq!(
        tpv(&rs, "sevtwatchevent"),
        s(&[
            ("2015-08-02 00:00:00", "d", "1"),
            ("2015-08-03 00:00:00", "d", "1"),
        ])
    );
    assert_eq!(rs.count("sevtpushevent"), 2);
    assert_eq!(rs.last_computed().len(), 1);
}

#[test]
fn drop_series_disabled() {
    let Some(rs) = both(&Case::new("drop_disabled", &[]).steps(drop_steps())) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_no_line(1, "Truncating table sevtwatchevent");
    assert_eq!(rs.count("sevtwatchevent"), 3);
}

#[test]
fn drop_series_skip_tsdb() {
    let Some(rs) = both(
        &Case::new("drop_skip_tsdb", DROP_SECOND)
            .env("GHA2DB_ENABLE_METRICS_DROP", "1")
            .steps(vec![
                Step::RunArgs(DROP_FIRST.to_vec()),
                Step::Sql(DROP_PREP[1]),
                Step::Run(vec![("GHA2DB_SKIPTSDB", "1")]),
            ]),
    ) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_no_line(1, "Truncating table sevtwatchevent");
    // nothing is touched with GHA2DB_SKIPTSDB
    assert_eq!(rs.count("sevtwatchevent"), 3);
}

#[test]
fn drop_series_quiet() {
    let Some(rs) = both(
        &Case::new("drop_quiet", &[])
            .env("GHA2DB_ENABLE_METRICS_DROP", "1")
            .env("GHA2DB_DEBUG", "-1")
            .steps(drop_steps()),
    ) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_no_line(1, "Truncating table sevtwatchevent");
    assert_eq!(rs.count("sevtwatchevent"), 2);
}

// ------------------------------------------------------------ histograms

#[test]
fn hist_daily() {
    let Some(rs) = both(&Case::new(
        "hist_d",
        &[
            "hbt",
            "{fx}/hist_by_type.sql",
            "2015-08-01",
            "2015-08-13",
            "d",
            "hist",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shbt"]));
    assert_eq!(
        rs.columns("shbt"),
        strs(&["name", "period", "time", "value"])
    );
    // last day before 2015-08-13 plus the now()-relative events, bots excluded
    assert_eq!(
        rs.query("select name, value::text from shbt order by name"),
        vec![
            strs(&["ForkEvent", "2"]),
            strs(&["IssuesEvent", "1"]),
            strs(&["PullRequestEvent", "2"]),
            strs(&["PushEvent", "3"]),
            strs(&["WatchEvent", "1"]),
        ]
    );
    // rows are timestamped from 2012-07-01 backwards, one hour apart
    assert_eq!(
        rs.column("shbt", "time", "value desc, name"),
        strs(&[
            "2012-07-01 00:00:00",
            "2012-06-30 23:00:00",
            "2012-06-30 22:00:00",
            "2012-06-30 21:00:00",
            "2012-06-30 20:00:00",
        ])
    );
    assert_eq!(rs.last_computed()[0].0, "hist_by_type d");
    assert!(rs.computed().is_empty());
    rs.expect_line(
        0,
        "calc_metric.go: Histogram running interval 'day,d' n:1 anno:false past:false multi:false",
    );
}

#[test]
fn hist_quarterly() {
    let Some(rs) = both(&Case::new(
        "hist_q",
        &[
            "hbt",
            "{fx}/hist_by_type.sql",
            "2015-08-01",
            "2015-08-13",
            "q",
            "hist",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("shbt", "period", "name"), strs(&["q"; 5]));
    assert_eq!(
        rs.column("shbt", "value", "name"),
        strs(&["8", "9", "9", "10", "8"])
    );
}

#[test]
fn hist_seven_days_with_n() {
    let Some(rs) = both(&Case::new(
        "hist_d7_n",
        &[
            "hbtn",
            "{fx}/hist_by_type_n.sql",
            "2015-08-01",
            "2015-08-13",
            "d7",
            "hist",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shbtn"]));
    assert_eq!(rs.column("shbtn", "period", "name"), strs(&["d7"; 5]));
}

#[test]
fn hist_rerun_replaces_period_rows() {
    let Some(rs) = both(
        &Case::new(
            "hist_rerun",
            &["hbt", "{fx}/hist_by_type.sql", "2015-08-01", "2015-08-13", "d", "hist"],
        )
        .steps(vec![
            Step::Run(Vec::new()),
            Step::Sql("insert into shbt(time, period, name, value) values ('2010-01-01', 'd', 'stale', 1), ('2010-01-02', 'w', 'other period', 1)"),
            Step::Run(Vec::new()),
        ]),
    ) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    // the period's rows are cleared before writing, other periods stay
    assert_eq!(
        rs.column("shbt", "name", "period, name"),
        strs(&[
            "ForkEvent",
            "IssuesEvent",
            "PullRequestEvent",
            "PushEvent",
            "WatchEvent",
            "other period",
        ])
    );
}

#[test]
fn hist_annotations_range() {
    let Some(rs) = both(&Case::new(
        "hist_anno_a01",
        &[
            "hbtf",
            "{fx}/hist_by_type_fromto.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shbtf"]));
    // [2015-08-03, 2015-08-06), bots excluded
    assert_eq!(
        rs.query("select name, value::text from shbtf order by name"),
        vec![
            strs(&["ForkEvent", "2"]),
            strs(&["IssuesEvent", "2"]),
            strs(&["PullRequestEvent", "1"]),
            strs(&["PushEvent", "1"]),
            strs(&["WatchEvent", "3"]),
        ]
    );
    assert_eq!(rs.column("shbtf", "period", "name"), strs(&["a_0_1"; 5]));
    // a past range is marked as computed (key: last two path components)
    assert_eq!(
        rs.computed(),
        vec![(
            "calc_metric/hist_by_type_fromto.sql".to_string(),
            "2015-08-06 00:00:00".to_string()
        )]
    );
    assert_eq!(rs.last_computed()[0].0, "hist_by_type_fromto a_0_1");
    rs.expect_line(
        0,
        "Found quick range: [a_0_1  2015-08-03 00:00:00 2015-08-06 00:00:00]",
    );
}

#[test]
fn hist_annotations_period_day() {
    let Some(rs) = both(&Case::new(
        "hist_anno_d",
        &[
            "hbtr",
            "{fx}/hist_by_type_range.sql",
            "2015-08-01",
            "2015-08-13",
            "d",
            "hist,annotations_ranges",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shbtr"]));
    // only the now()-1h push falls into the last day
    assert_eq!(
        rs.query("select name, value::text from shbtr order by name"),
        vec![strs(&["PushEvent", "1"])]
    );
    assert!(rs.computed().is_empty());
}

#[test]
fn hist_annotations_period_week() {
    let Some(rs) = both(&Case::new(
        "hist_anno_w",
        &[
            "hbtr",
            "{fx}/hist_by_type_range.sql",
            "2015-08-01",
            "2015-08-13",
            "w",
            "hist,annotations_ranges",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.column("shbtr", "period", "name"), strs(&["w"; 2]));
}

#[test]
fn hist_annotations_skip_past() {
    let args = [
        "hbtf",
        "{fx}/hist_by_type_fromto.sql",
        "2015-08-01",
        "2015-08-13",
        "a_1_2",
        "hist,annotations_ranges,skip_past",
    ];
    let Some(rs) = both(
        &Case::new("hist_skip_past", &args)
            .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.code(1), Some(0));
    rs.expect_no_line(
        0,
        "Skipping past quick range: 2015-08-06 00:00:00-2015-08-10 00:00:00 (already computed)",
    );
    rs.expect_line(
        1,
        "Skipping past quick range: 2015-08-06 00:00:00-2015-08-10 00:00:00 (already computed)",
    );
    assert_eq!(rs.count("shbtf"), 5);
    assert_eq!(rs.computed().len(), 1);
}

#[test]
fn hist_annotations_range_reaching_the_future() {
    let Some(rs) = both(&Case::new(
        "hist_anno_future",
        &[
            "hbtf",
            "{fx}/hist_by_type_fromto.sql",
            "2015-08-01",
            "2015-08-13",
            "c_n",
            "hist,annotations_ranges,skip_past",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.count("shbtf"), 5);
    // not marked: the range is still open
    assert!(rs.computed().is_empty());
}

#[test]
fn hist_annotations_range_not_found() {
    let Some(rs) = both(&Case::new(
        "hist_anno_notfound",
        &[
            "hbtf",
            "{fx}/hist_by_type_fromto.sql",
            "2015-08-01",
            "2015-08-13",
            "zz",
            "hist,annotations_ranges",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(
        rs.error(0)
            .unwrap()
            .starts_with("quick range not found: 'zz'"),
        "{:?}",
        rs.error(0)
    );
    assert!(rs.tables().is_empty());
}

#[test]
fn hist_explicit_range_period() {
    let Some(rs) = both(&Case::new(
        "hist_range",
        &[
            "hbtf",
            "{fx}/hist_by_type_fromto.sql",
            "2015-08-01",
            "2015-08-13",
            "range:2015-08-01,2015-08-10",
            "hist",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.column("shbtf", "period", "name"),
        strs(&["range:2015-08-01 00:00:00,2015-08-10 00:00:00"; 5])
    );
    assert_eq!(
        rs.last_computed()[0].0,
        "hist_by_type_fromto range:2015-08-01 00:00:00,2015-08-10 00:00:00"
    );
    assert!(rs.computed().is_empty());
}

#[test]
fn hist_explicit_range_period_malformed() {
    let Some(rs) = both(&Case::new(
        "hist_range_bad",
        &[
            "hbtf",
            "{fx}/hist_by_type_fromto.sql",
            "2015-08-01",
            "2015-08-13",
            "range:2015-08-01",
            "hist",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.tables().is_empty());
}

#[test]
fn hist_unknown_interval() {
    let Some(rs) = both(&Case::new(
        "hist_unknown_interval",
        &[
            "hbt",
            "{fx}/hist_by_type.sql",
            "2015-08-01",
            "2015-08-13",
            "zz",
            "hist",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(1));
    assert!(rs.tables().is_empty());
}

#[test]
fn hist_multi_row_single_column() {
    let Some(rs) = both(&Case::new(
        "hist_multi",
        &[
            "multi_row_single_column",
            "{fx}/hist_multi.sql",
            "2015-08-01",
            "2015-08-13",
            "a_1_2",
            "hist,annotations_ranges",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&["shmorgrepo1", "shmorgrepotwo", "shmotherrepo3"])
    );
    assert_eq!(
        rs.columns("shmorgrepo1"),
        strs(&["name", "period", "time", "value"])
    );
    assert_eq!(
        rs.computed(),
        vec![(
            "calc_metric/hist_multi.sql".to_string(),
            "2015-08-10 00:00:00".to_string()
        )]
    );
}

#[test]
fn hist_multi_row_single_column_yearly() {
    let Some(rs) = both(&Case::new(
        "hist_multi_period_y",
        &[
            "multi_row_single_column",
            "{fx}/hist_multi_period.sql",
            "2015-08-01",
            "2015-08-13",
            "y",
            "hist",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables().len(), 3);
    assert_eq!(rs.column("shmorgrepo1", "period", "name"), strs(&["y"; 5]));
}

#[test]
fn hist_multi_row_multi_column_pairs() {
    let Some(rs) = both(&Case::new(
        "hist_multi_pairs",
        &[
            "multi_row_multi_column",
            "{fx}/hist_multi_pairs.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "shmporgrepo1others",
            "shmporgrepo1pushes",
            "shmporgrepotwoothers",
            "shmporgrepotwopushes",
            "shmpotherrepo3others",
            "shmpotherrepo3pushes",
        ])
    );
    assert_eq!(
        rs.query("select name, value::text from shmporgrepotwopushes"),
        vec![strs(&["push", "1"])]
    );
}

#[test]
fn hist_multi_row_multi_column_pairs_merged() {
    let Some(rs) = both(&Case::new(
        "hist_multi_pairs_merge",
        &[
            "multi_row_multi_column",
            "{fx}/hist_multi_pairs.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,merge_series:hmpm",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shmpm"]));
    assert_eq!(
        rs.columns("shmpm"),
        strs(&["name", "period", "series", "time", "value"])
    );
    assert_eq!(rs.count("shmpm"), 6);
}

#[test]
fn hist_multi_custom_data() {
    let Some(rs) = both(&Case::new(
        "hist_multi_custom",
        &[
            "multi_row_single_column",
            "{fx}/hist_multi_custom.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,custom_data",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&["shmcorgrepo1", "shmcorgrepotwo", "shmcotherrepo3"])
    );
    assert_eq!(
        rs.columns("shmcorgrepo1"),
        strs(&["dt", "name", "period", "str", "time", "value"])
    );
}

#[test]
fn hist_multi_custom_data_unique_time() {
    let Some(rs) = both(&Case::new(
        "hist_multi_custom_unique",
        &[
            "multi_row_single_column",
            "{fx}/hist_multi_custom.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,custom_data,custom_data_unique_time",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables().len(), 3);
}

#[test]
fn hist_multi_custom_data_merged() {
    let Some(rs) = both(&Case::new(
        "hist_multi_custom_merge",
        &[
            "multi_row_single_column",
            "{fx}/hist_multi_custom.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,custom_data,merge_series:hmc",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shmc"]));
    assert_eq!(
        rs.columns("shmc"),
        strs(&["dt", "name", "period", "series", "str", "time", "value"])
    );
    // 9 non-bot events in [2015-08-03, 2015-08-06)
    assert_eq!(rs.count("shmc"), 9);
}

#[test]
fn hist_multi_custom_data_pairs() {
    let Some(rs) = both(&Case::new(
        "hist_multi_custom_pairs",
        &[
            "multi_row_multi_column",
            "{fx}/hist_multi_custom_pairs.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,custom_data",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables().len(), 6);
    assert_eq!(
        rs.columns("shmcporgrepo1first"),
        strs(&["dt", "name", "period", "str", "time", "value"])
    );
}

#[test]
fn hist_multivalue() {
    let Some(rs) = both(&Case::new(
        "hist_mv",
        &[
            "multi_row_single_column",
            "{fx}/hist_mv.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,multivalue",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&["shmvorgrepo1", "shmvorgrepotwo", "shmvotherrepo3"])
    );
    let cols = rs.columns("shmvorgrepo1");
    assert!(cols.contains(&"evs".to_string()), "{cols:?}");
}

#[test]
fn hist_multivalue_typed_specs() {
    let Some(rs) = both(&Case::new(
        "hist_mv_specs",
        &[
            "multi_row_multi_column",
            "{fx}/hist_mv_specs.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,multivalue",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shmvstypes"]));
    let cols = rs.columns("shmvstypes");
    for c in ["typ", "evs", "acts", "who"] {
        assert!(cols.contains(&c.to_string()), "{cols:?}");
    }
    assert_eq!(rs.count("shmvstypes"), 5);
}

#[test]
fn hist_multivalue_typed_specs_merged() {
    let Some(rs) = both(&Case::new(
        "hist_mv_specs_merge",
        &[
            "multi_row_multi_column",
            "{fx}/hist_mv_specs.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,multivalue,merge_series:hmvsm",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shmvsm"]));
}

#[test]
fn hist_multivalue_null_float() {
    let Some(rs) = both(
        &Case::new(
            "hist_mv_null",
            &[
                "multi_row_multi_column",
                "{fx}/hist_mv_null.sql",
                "2015-08-01",
                "2015-08-13",
                "d",
                "hist,multivalue",
            ],
        )
        .code_only_errors(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.tables().is_empty());
}

#[test]
fn hist_multivalue_bad_type_spec() {
    let Some(rs) = both(&Case::new(
        "hist_mv_badtype",
        &[
            "multi_row_multi_column",
            "{fx}/hist_mv_badtype.sql",
            "2015-08-01",
            "2015-08-13",
            "d",
            "hist,multivalue",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.tables().is_empty());
}

#[test]
fn hist_multivalue_two_series_names() {
    let Some(rs) = both(&Case::new(
        "hist_mv_two_names",
        &[
            "multi_row_multi_column",
            "{fx}/hist_mv_two_names.sql",
            "2015-08-01",
            "2015-08-13",
            "d",
            "hist,multivalue",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.tables().is_empty());
}

#[test]
fn hist_null_name() {
    let Some(rs) = both(
        &Case::new(
            "hist_null_name",
            &[
                "hnn",
                "{fx}/hist_null_name.sql",
                "2015-08-01",
                "2015-08-13",
                "d",
                "hist",
            ],
        )
        .code_only_errors(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.tables().is_empty());
}

#[test]
fn hist_project_scale() {
    let Some(rs) = both(&Case::new(
        "hist_project_scale",
        &[
            "hps",
            "{fx}/hist_project_scale.sql",
            "2015-08-01",
            "2015-08-13",
            "a_0_1",
            "hist,annotations_ranges,project_scale:2.5",
        ],
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // bots included in [2015-08-03, 2015-08-06): events per type × 2.5
    assert_eq!(
        rs.column("shps", "value", "name"),
        strs(&["5", "7.5", "7.5", "7.5", "7.5"])
    );
}

#[test]
fn hist_skip_tsdb() {
    let Some(rs) = both(
        &Case::new(
            "hist_skip_tsdb",
            &[
                "hbt",
                "{fx}/hist_by_type.sql",
                "2015-08-01",
                "2015-08-13",
                "d",
                "hist",
            ],
        )
        .env("GHA2DB_SKIPTSDB", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert!(rs.tables().is_empty());
    assert!(rs.last_computed().is_empty());
}

#[test]
fn hist_debug_output() {
    let Some(rs) = both(
        &Case::new(
            "hist_debug",
            &[
                "hbt",
                "{fx}/hist_by_type.sql",
                "2015-08-01",
                "2015-08-13",
                "d",
                "hist",
            ],
        )
        .env("GHA2DB_DEBUG", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.count("shbt"), 5);
}

// ------------------------------------------------------------------- HLL

#[test]
fn hll_single_value() {
    let Some(rs) = both(
        &Case::new(
            "hll_count",
            &[
                "hcnt",
                "{fx}/hll_count.sql",
                "2015-08-01",
                "2015-08-03",
                "d",
                "hll",
            ],
        )
        .hll(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shcnt"]));
    assert_eq!(
        rs.query("select time::text, hll_cardinality(value)::int::text from shcnt order by 1"),
        vec![
            strs(&["2015-08-01 00:00:00", "3"]),
            strs(&["2015-08-02 00:00:00", "4"]),
            strs(&["2015-08-03 00:00:00", "4"]),
        ]
    );
}

#[test]
fn hll_null_value() {
    let Some(rs) = both(
        &Case::new(
            "hll_null",
            &[
                "hcnt",
                "{fx}/hll_null.sql",
                "2015-08-01",
                "2015-08-03",
                "d",
                "hll",
            ],
        )
        .hll(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.column("shcnt", "hll_cardinality(value)::int", "time"),
        strs(&["0", "0", "0"])
    );
}

#[test]
fn hll_multi_row_single_column() {
    let Some(rs) = both(
        &Case::new(
            "hll_by_type",
            &[
                "multi_row_single_column",
                "{fx}/hll_by_type.sql",
                "2015-08-01",
                "2015-08-13",
                "d",
                "hll",
            ],
        )
        .hll(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.tables(),
        strs(&[
            "shlltforkevent",
            "shlltissuesevent",
            "shlltpullrequestevent",
            "shlltpushevent",
            "shlltwatchevent",
        ])
    );
}

#[test]
fn hll_multi_row_multi_column() {
    let Some(rs) = both(
        &Case::new(
            "hll_by_repo_multi",
            &[
                "multi_row_multi_column",
                "{fx}/hll_by_repo_multi.sql",
                "2015-08-01",
                "2015-08-03",
                "d",
                "hll",
            ],
        )
        .hll(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables().len(), 6);
    assert_eq!(
        rs.column("shllrorgrepo1types", "hll_cardinality(value)::int", "time"),
        strs(&["2", "2", "1"])
    );
}

#[test]
fn hll_multivalue() {
    let Some(rs) = both(
        &Case::new(
            "hll_mv_by_type",
            &[
                "multi_row_single_column",
                "{fx}/hll_mv_by_type.sql",
                "2015-08-01",
                "2015-08-03",
                "d",
                "multivalue,escape_value_name,hll",
            ],
        )
        .hll(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables(), strs(&["shllmv"]));
    assert!(rs.columns("shllmv").contains(&"pushevent".to_string()));
}

#[test]
fn hll_custom_data() {
    let Some(rs) = both(
        &Case::new(
            "hll_custom",
            &[
                "multi_row_single_column",
                "{fx}/hll_custom.sql",
                "2015-08-01",
                "2015-08-03",
                "d",
                "custom_data,hll",
            ],
        )
        .hll(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables().len(), 5);
}

#[test]
fn hll_multi_threaded() {
    let Some(rs) = both(
        &Case::new(
            "hll_mt",
            &[
                "multi_row_single_column",
                "{fx}/hll_by_type.sql",
                "2015-08-01",
                "2015-08-13",
                "d",
                "hll",
            ],
        )
        .hll()
        .threads("4"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.tables().len(), 5);
}
