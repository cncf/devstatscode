//! Go ⇄ Rust compatibility tests for `tags`.
//!
//! Every scenario builds two identical scratch databases (the real DevStats
//! schema from `compat/fixtures/structure/full_structure.sql` plus the seed
//! data of `compat/fixtures/tags/seed.sql`), runs the Go binary against one
//! (`dbtest_tags_<name>_go`) and the Rust binary against the other
//! (`dbtest_tags_<name>_rs`) — same environment, same working directory
//! (`compat/fixtures/tags/data`, the layout of a `cncf/devstats` checkout:
//! `metrics/shared/*` are the real files, `metrics/testproj/*` test-specific
//! ones) — and compares:
//!
//! * exit code, stdout (durations and `time.Now()` stamps masked; sorted when
//!   several worker threads make the line order nondeterministic) and, for
//!   fatal errors, the `Error: '…'` lines of stderr;
//! * the resulting schema (the `t<series>` tables and their indexes);
//! * the content of every `t<series>` table — row by row when the tag SQL has
//!   a total `order by`, otherwise as the sorted set of tag values plus the
//!   list of `time` slots (PostgreSQL does not promise an order for
//!   `select distinct` / `union` results, so two runs may legitimately assign
//!   the hourly slots to the values in a different order).
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped otherwise).

use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};
use devstatscode::pg::exec_sql_tx;

fn go_bin() -> Option<PathBuf> {
    go_binary("tags")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_tags"))
}

/// `compat/fixtures/tags/data` — the "devstats checkout" the tool runs in.
fn data_dir() -> PathBuf {
    fixture("tags/data")
}

fn structure_sql() -> String {
    fs::read_to_string(fixture("structure/full_structure.sql")).unwrap()
}

fn seed_sql() -> String {
    fs::read_to_string(fixture("tags/seed.sql")).unwrap()
}

/// A lock held on a table by another session while the binary runs.
#[derive(Clone)]
struct HeldLock {
    table: String,
    mode: String,
    hold: Duration,
}

/// How a scenario prepares its databases and runs the binaries.
struct Scenario<'a> {
    name: &'a str,
    /// Load the schema + seed data (almost always).
    seed: bool,
    /// Extra SQL run after the seed.
    setup: Vec<String>,
    /// Extra environment on top of the `PG_*` connection variables.
    env: Vec<(&'a str, &'a str)>,
    cwd: PathBuf,
    lock: Option<HeldLock>,
    /// The worker threads interleave their output: compare sorted lines.
    sorted_stdout: bool,
    /// Compare the `t*` tables row by row (all tag SQLs totally ordered).
    exact_rows: bool,
    /// Compare the `Error: '…'` lines (off for errors whose wording is the
    /// yaml library's).
    compare_errors: bool,
}

impl<'a> Scenario<'a> {
    fn new(name: &'a str) -> Self {
        Scenario {
            name,
            seed: true,
            setup: Vec::new(),
            env: vec![
                ("GHA2DB_LOCAL", "1"),
                ("GHA2DB_PROJECT", "testproj"),
                ("GHA2DB_ST", "1"),
            ],
            cwd: data_dir(),
            lock: None,
            sorted_stdout: false,
            exact_rows: true,
            compare_errors: true,
        }
    }
    fn no_seed(mut self) -> Self {
        self.seed = false;
        self
    }
    fn setup(mut self, sql: &str) -> Self {
        self.setup.push(sql.to_string());
        self
    }
    fn env(mut self, k: &'a str, v: &'a str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self.env.push((k, v));
        self
    }
    fn no_env(mut self, k: &str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self
    }
    fn cwd(mut self, p: impl Into<PathBuf>) -> Self {
        self.cwd = p.into();
        self
    }
    /// Run with `GHA2DB_NCPUS=4` instead of single-threaded.
    fn threads4(self) -> Self {
        let mut s = self.no_env("GHA2DB_ST");
        s.env.push(("GHA2DB_NCPUS", "4"));
        s.sorted_stdout = true;
        s
    }
    fn lock(mut self, table: &str, mode: &str, secs: u64) -> Self {
        self.lock = Some(HeldLock {
            table: table.to_string(),
            mode: mode.to_string(),
            hold: Duration::from_secs(secs),
        });
        self
    }
    fn unordered_rows(mut self) -> Self {
        self.exact_rows = false;
        self
    }
    fn ignore_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
}

/// One side of a scenario: the database it worked on and its outcome.
struct Side {
    db: TestDb,
    out: Outcome,
}

/// Run `bin` on its own database per the scenario.
fn run_side(bin: &Path, sc: &Scenario<'_>, suffix: &str) -> Option<Side> {
    let db = TestDb::fresh(&format!("tags_{}_{}", sc.name, suffix))?;
    if sc.seed {
        db.exec(&structure_sql());
        db.exec(&seed_sql());
    }
    for sql in &sc.setup {
        db.exec(sql);
    }
    let env = db.env();
    let mut inv = Invocation::new().cwd(sc.cwd.clone());
    for (k, v) in &env {
        inv = inv.env(k, v);
    }
    for (k, v) in &sc.env {
        inv = inv.env(k, v);
    }
    let out = match &sc.lock {
        None => run(bin, &inv),
        Some(lock) => {
            // Another session takes the lock, signals it holds it and keeps
            // it for `hold`; the binary starts once the lock is in place.
            let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
            let ctx = db.ctx.clone();
            let lock = lock.clone();
            let holder = thread::spawn(move || {
                let con = devstatscode::pg::pg_conn(&ctx);
                let mut tx = con.begin().unwrap();
                exec_sql_tx(
                    &mut tx,
                    &ctx,
                    &format!("lock table \"{}\" in {} mode", lock.table, lock.mode),
                    &[],
                )
                .unwrap();
                ready_tx.send(()).unwrap();
                thread::sleep(lock.hold);
                tx.commit().unwrap();
                con.close();
            });
            ready_rx.recv().unwrap();
            let out = run(bin, &inv);
            holder.join().unwrap();
            out
        }
    };
    Some(Side { db, out })
}

/// `Error: '…'` lines of a fatal error report.
fn error_lines(stderr: &str) -> Vec<String> {
    stderr
        .lines()
        .filter(|l| l.starts_with("Error: '"))
        .map(str::to_string)
        .collect()
}

fn is_ymd(tok: &str) -> bool {
    tok.len() == 10
        && tok.as_bytes()[4] == b'-'
        && tok.as_bytes()[7] == b'-'
        && tok
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
}

/// Mask the `added` stamp (`time.Now()`, `YYYY-MM-DD H`) of a `TSPoint.Str()`
/// line: `NewTSPoint: 2012-07-01 0 2026-09-11 7 repos period: …` — the second
/// date/hour pair before ` period: `.
fn mask_added(line: &str) -> String {
    let Some(pos) = line.find(" period: ") else {
        return line.to_string();
    };
    let (head, tail) = line.split_at(pos);
    let toks: Vec<&str> = head.split(' ').collect();
    let dates: Vec<usize> = toks
        .iter()
        .enumerate()
        .filter(|(_, t)| is_ymd(t))
        .map(|(i, _)| i)
        .collect();
    if dates.len() < 2 || dates[1] + 1 >= toks.len() {
        return line.to_string();
    }
    let j = dates[1];
    let mut out: Vec<&str> = toks[..j].to_vec();
    out.push("<added>");
    out.extend_from_slice(&toks[j + 2..]);
    out.join(" ") + tail
}

/// `create table if not exists "t…"(time timestamp primary key, "a" text, …)`
/// echoed in debug mode: Go lists the columns in map order (random), so sort
/// the column definitions.
fn mask_create_table(line: &str) -> String {
    if !line.starts_with("create table if not exists \"") || !line.ends_with(')') {
        return line.to_string();
    }
    let Some(open) = line.find('(') else {
        return line.to_string();
    };
    let inner = &line[open + 1..line.len() - 1];
    let mut cols: Vec<&str> = inner.split(", ").collect();
    cols.sort();
    format!("{}({})", &line[..open], cols.join(", "))
}

/// Make the stdout of the two sides comparable.
fn normalize_stdout(stdout: &str, sorted: bool, debug: bool) -> String {
    let masked = mask_go_durations(stdout);
    let mut lines: Vec<String> = masked
        .split('\n')
        .map(|l| mask_create_table(&mask_added(l)))
        .collect();
    // Runs of `create index if not exists …` lines (debug echo of the
    // structural SQLs) come in Go's random map order: sort each run.
    let mut i = 0;
    while i < lines.len() {
        if lines[i].starts_with("create index if not exists \"") {
            let mut j = i;
            while j < lines.len() && lines[j].starts_with("create index if not exists \"") {
                j += 1;
            }
            lines[i..j].sort();
            i = j;
        } else {
            i += 1;
        }
    }
    if sorted {
        lines.sort();
    } else if debug {
        // In debug mode the main thread's `threading: …` lines and the
        // worker's `Synced tag …` line race each other (in Go as well):
        // compare the rest in order and those two kinds as a sorted multiset
        // appended at the end.
        let mut racing: Vec<String> = lines
            .iter()
            .filter(|l| l.starts_with("threading: ") || l.starts_with("Synced tag "))
            .cloned()
            .collect();
        racing.sort();
        lines.retain(|l| !(l.starts_with("threading: ") || l.starts_with("Synced tag ")));
        lines.push("--- racing lines, sorted:".to_string());
        lines.append(&mut racing);
    }
    lines.join("\n")
}

/// One `t<series>` table as compared between the two sides.
#[derive(Debug, PartialEq, Eq)]
struct SeriesData {
    table: String,
    /// Full rows in `time` order (exact mode) or tag values sorted (unordered mode).
    rows: cpg::Snapshot,
    /// `time` column, sorted (unordered mode only; empty otherwise).
    times: Vec<String>,
}

/// Everything we compare about a database after a run.
struct DbState {
    schema: String,
    counts: Vec<(String, i64)>,
    series: Vec<SeriesData>,
}

fn series_tables(con: &devstatscode::pg::PgConn) -> Vec<String> {
    cpg::tables(con)
        .into_iter()
        .filter(|t| t.starts_with('t') && !t.starts_with("gha_"))
        .collect()
}

/// Go builds the column lists of the `t<series>` tables by iterating a
/// `map[string]…` — the column order is random per run (the Rust port uses
/// sorted order). Compare schemas with the `column` lines of every table
/// sorted.
fn normalize_schema(dump: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut cols: Vec<String> = Vec::new();
    for line in dump.lines() {
        if line.starts_with("  column ") {
            cols.push(line.to_string());
        } else {
            cols.sort();
            out.append(&mut cols);
            out.push(line.to_string());
        }
    }
    cols.sort();
    out.append(&mut cols);
    out.join("\n")
}

fn db_state(db: &TestDb, exact_rows: bool) -> DbState {
    let con = db.conn();
    let series = series_tables(&con)
        .into_iter()
        .map(|t| {
            let q = format!("\"{}\"", t.replace('"', "\"\""));
            let mut cols: Vec<String> = cpg::table_columns(&con, &t)
                .into_iter()
                .map(|c| format!("\"{}\"", c.0))
                .filter(|c| c != "\"time\"")
                .collect();
            cols.sort();
            let list = cols.join(", ");
            if exact_rows {
                SeriesData {
                    rows: cpg::snapshot(
                        &con,
                        &format!("select time, {list} from {q} order by time"),
                        &[],
                    ),
                    times: Vec::new(),
                    table: t,
                }
            } else {
                SeriesData {
                    rows: cpg::snapshot(
                        &con,
                        &format!("select {list} from {q} order by {list}"),
                        &[],
                    ),
                    times: cpg::snapshot(&con, &format!("select time from {q} order by time"), &[])
                        .column(0),
                    table: t,
                }
            }
        })
        .collect();
    let st = DbState {
        schema: normalize_schema(&cpg::schema_dump(&con)),
        counts: cpg::table_counts(&con),
        series,
    };
    con.close();
    st
}

/// Run both binaries and assert they agree (outcome + database). Returns the
/// Rust side for scenario-specific assertions; `None` when DB tests are off.
fn both(sc: &Scenario<'_>) -> Option<Side> {
    let rust = run_side(&rust_bin(), sc, "rs")?;
    if let Some(go) = go_bin() {
        let go = run_side(&go, sc, "go").unwrap();
        let ctx = format!(
            "\nscenario {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
            sc.name,
            sc.env,
            go.out.code,
            go.out.stdout_str(),
            go.out.stderr_str(),
            rust.out.code,
            rust.out.stdout_str(),
            rust.out.stderr_str()
        );
        assert_eq!(go.out.code, rust.out.code, "exit code{ctx}");
        let debug = sc.env.iter().any(|(k, _)| *k == "GHA2DB_DEBUG");
        assert_eq!(
            normalize_stdout(&go.out.stdout_str(), sc.sorted_stdout, debug),
            normalize_stdout(&rust.out.stdout_str(), sc.sorted_stdout, debug),
            "stdout{ctx}"
        );
        if sc.compare_errors {
            assert_eq!(
                error_lines(&go.out.stderr_str()),
                error_lines(&rust.out.stderr_str()),
                "fatal error lines{ctx}"
            );
        }
        let g = db_state(&go.db, sc.exact_rows);
        let r = db_state(&rust.db, sc.exact_rows);
        assert_eq!(g.schema, r.schema, "schema (catalog dump){ctx}");
        assert_eq!(g.counts, r.counts, "row counts{ctx}");
        assert_eq!(g.series, r.series, "series tables{ctx}");
    }
    Some(rust)
}

fn tables_of(db: &TestDb) -> Vec<String> {
    let con = db.conn();
    let t = series_tables(&con);
    con.close();
    t
}

fn rows(db: &TestDb, sql: &str) -> Vec<Vec<String>> {
    let con = db.conn();
    let s = cpg::snapshot(&con, sql, &[]);
    con.close();
    s.rows
}

fn count(db: &TestDb, sql: &str) -> i64 {
    rows(db, sql)[0][0].parse().unwrap()
}

/// The series the real `metrics/shared/tags.yaml` defines.
const SHARED_SERIES: &[&str] = &[
    "tall_repo_groups",
    "tcompanies",
    "tcountries",
    "tcumperiods",
    "tevent_types",
    "tlanguages",
    "tlicenses",
    "tpriority_labels_with_all",
    "trepo_groups",
    "trepos",
    "treviewers",
    "ttop_repo_names",
    "ttop_repos_with_all",
    "tusers",
];

/// The series `metrics/testproj/tags.yaml` defines (the disabled one is not
/// processed, the empty one is created but stays empty).
const TESTPROJ_SERIES: &[&str] = &[
    "taliases",
    "tcompanies",
    "tcountries",
    "tcumperiods",
    "tevent_types_all",
    "tevent_types_top",
    "thumans",
    "trepos",
];

// ---------------------------------------------------------------------------
// The real shared configuration (project without its own files → fallback)
// ---------------------------------------------------------------------------

#[test]
fn shared_tags_single_threaded() {
    let Some(rs) = both(
        &Scenario::new("shared_st")
            .env("GHA2DB_PROJECT", "fallback")
            .unordered_rows(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert_eq!(tables_of(&rs.db), SHARED_SERIES);
    let stdout = rs.out.stdout_str();
    assert!(stdout.contains("Final 0 threads join\n"), "{stdout}");
    assert!(!stdout.contains("Warning"), "{stdout}");
    // 14 series written, one batch each
    assert_eq!(stdout.matches("WriteTSPoints: writing ").count(), 14);
    // spot checks of the tag values
    assert_eq!(
        rows(&rs.db, "select repo_name from trepos order by repo_name"),
        vec![
            vec!["cncf/devstats"],
            vec!["envoyproxy/envoy"],
            vec!["kubernetes/kubernetes"],
            vec!["kubernetes/website"],
            vec!["prometheus/prometheus"],
            vec!["zé/ünïcode-ok"],
        ]
    );
    // bots excluded, order by activity
    assert_eq!(
        rows(&rs.db, "select users_name from tusers order by time"),
        vec![
            vec!["alice"],
            vec!["bob"],
            vec!["carol"],
            vec!["eve"],
            vec!["None"]
        ]
    );
    assert_eq!(
        rows(
            &rs.db,
            "select companies_name, companies_value from tcompanies order by time"
        ),
        vec![
            vec!["Google", "google"],
            vec!["Red Hat", "redhat"],
            vec!["Microsoft", "microsoft"],
            vec!["Ünïcode Ltd.", "unicodeltd"],
            vec!["None", "none"],
        ]
    );
    assert_eq!(
        rows(
            &rs.db,
            "select country_name, country_value from tcountries order by time"
        ),
        vec![
            vec!["Poland", "poland"],
            vec!["Germany", "germany"],
            vec!["United States", "unitedstates"],
            vec!["Japan", "japan"],
        ]
    );
    // hourly slots from 2012-07-01
    assert_eq!(
        rows(&rs.db, "select time::text from tcumperiods order by time"),
        vec![
            vec!["2012-07-01 00:00:00"],
            vec!["2012-07-01 01:00:00"],
            vec!["2012-07-01 02:00:00"],
            vec!["2012-07-01 03:00:00"],
            vec!["2012-07-01 04:00:00"],
            vec!["2012-07-01 05:00:00"],
            vec!["2012-07-01 06:00:00"],
            vec!["2012-07-01 07:00:00"],
        ]
    );
    assert_eq!(
        rows(
            &rs.db,
            "select priority_labels_name_with_all from tpriority_labels_with_all order by 1"
        ),
        vec![
            vec!["All"],
            vec!["P1"],
            vec!["backlog"],
            vec!["critical-urgent"]
        ]
    );
    assert_eq!(
        rows(&rs.db, "select reviewers_name from treviewers order by 1"),
        vec![vec!["bob"], vec!["carol"], vec!["eve"], vec!["none"]]
    );
    assert_eq!(
        rows(&rs.db, "select lang_name from tlanguages order by 1"),
        vec![vec!["C++"], vec!["Go"], vec!["HTML"], vec!["Shell"]]
    );
}

#[test]
fn shared_tags_multi_threaded() {
    let Some(rs) = both(
        &Scenario::new("shared_mt")
            .env("GHA2DB_PROJECT", "fallback")
            .threads4()
            .unordered_rows(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert_eq!(tables_of(&rs.db), SHARED_SERIES);
    // 14 tags, 4 workers: 11 joined in the loop, 3 remain for the final join
    assert!(
        rs.out.stdout_str().contains("Final 3 threads join\n"),
        "{}",
        rs.out.stdout_str()
    );
}

// ---------------------------------------------------------------------------
// Project-specific configuration (deterministic row order)
// ---------------------------------------------------------------------------

#[test]
fn testproj_single_threaded() {
    let Some(rs) = both(&Scenario::new("testproj_st")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert_eq!(tables_of(&rs.db), TESTPROJ_SERIES);
    let stdout = rs.out.stdout_str();
    assert!(
        stdout.contains(
            "Warning: Tag '&{Name:Nothing SQLFile:empty SeriesName:nothing NameTag:nothing_name \
             ValueTag:nothing_value OtherTags:map[a_first:[name ] z_last:[name 1]] Limit:5 Disabled:false}' have no values\n"
        ),
        "{stdout}"
    );
    // other_tags: plain, normalized (`1`, `Y`, `t`, `y`) and not (`no`, ``)
    assert_eq!(
        rows(
            &rs.db,
            "select time::text, companies_name, companies_value, company_first, company_len, company_len_norm, \
             company_lower, company_upper, company_upper_norm from tcompanies order by time"
        ),
        vec![
            vec!["2012-07-01 00:00:00", "(Unknown)", "unknown", "(", "9", "9", "(unknown)", "(UNKNOWN)", "unknown"],
            vec!["2012-07-01 01:00:00", "Google", "google", "G", "6", "6", "google", "GOOGLE", "google"],
            vec!["2012-07-01 02:00:00", "Microsoft", "microsoft", "M", "9", "9", "microsoft", "MICROSOFT", "microsoft"],
            vec!["2012-07-01 03:00:00", "NotFound", "notfound", "N", "8", "8", "notfound", "NOTFOUND", "notfound"],
            vec!["2012-07-01 04:00:00", "Red Hat", "redhat", "R", "7", "7", "red hat", "RED HAT", "redhat"],
            vec!["2012-07-01 05:00:00", "Ünïcode Ltd.", "unicodeltd", "Ü", "12", "12", "ünïcode ltd.", "ÜNÏCODE LTD.", "unicodeltd"],
        ]
    );
    let cols: Vec<String> = {
        let con = rs.db.conn();
        let c = cpg::table_columns(&con, "tcompanies")
            .into_iter()
            .map(|c| c.0)
            .collect();
        con.close();
        c
    };
    assert_eq!(
        cols,
        [
            "time",
            "companies_name",
            "companies_value",
            "company_first",
            "company_len",
            "company_len_norm",
            "company_lower",
            "company_upper",
            "company_upper_norm"
        ]
    );
    // NULLs scan as empty strings; normalization of "" and of numbers
    assert_eq!(
        rows(
            &rs.db,
            "select alias_name, alias_value, license, license_norm, probability, probability_norm from taliases order by time"
        ),
        vec![
            vec!["devstats", "devstats", "", "", "", ""],
            vec!["envoy", "envoy", "Not found", "notfound", "", ""],
            vec!["kubernetes/kubernetes", "kuberneteskubernetes", "Apache License 2.0", "apachelicense20", "99.5", "995"],
            vec!["k8s website", "k8swebsite", "Creative Commons Attribution 4.0", "creativecommonsattribution40", "98", "98"],
            vec!["", "", "Apache License 2.0", "apachelicense20", "100", "100"],
            vec!["ünïcode", "unicode", "MIT License", "mitlicense", "50", "50"],
        ]
    );
    // limit: 3 vs default {{lim}} = 127
    assert_eq!(
        rows(
            &rs.db,
            "select event_type_name, event_type_value from tevent_types_top order by time"
        ),
        vec![
            vec!["IssueCommentEvent", "issuecommentevent"],
            vec!["PushEvent", "pushevent"],
            vec!["ForkEvent", "forkevent"],
        ]
    );
    assert_eq!(count(&rs.db, "select count(*) from tevent_types_all"), 8);
    // {{exclude_bots}}
    assert_eq!(
        rows(&rs.db, "select human_name from thumans order by time"),
        vec![
            vec!["alice"],
            vec!["bob"],
            vec!["carol"],
            vec!["dave"],
            vec!["eve"]
        ]
    );
    // fallback to metrics/shared/cumulative_periods.sql
    assert_eq!(count(&rs.db, "select count(*) from tcumperiods"), 8);
    // duplicate values get their own hourly slots
    assert_eq!(
        rows(
            &rs.db,
            "select time::text, country_name from tcountries order by time"
        ),
        vec![
            vec!["2012-07-01 00:00:00", "Poland"],
            vec!["2012-07-01 01:00:00", "United States"],
            vec!["2012-07-01 02:00:00", "Germany"],
            vec!["2012-07-01 03:00:00", "Poland"],
            vec!["2012-07-01 04:00:00", "Japan"],
        ]
    );
}

#[test]
fn testproj_debug_output() {
    let Some(rs) = both(&Scenario::new("testproj_debug").env("GHA2DB_DEBUG", "1")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    let stdout = rs.out.stdout_str();
    for needle in [
        "Start Tag 'Repositories' --> 'repos'\n",
        "End Tag 'Repositories' --> 'repos'\n",
        "'repos': map[repo_name:cncf/devstats]\n",
        "threading: 1 >= 1, waiting on the channel\n",
        "threading: thread joined, num threads: 0\n",
        "lib.ReadFile('./metrics/testproj/tags.yaml'): ok\n",
        "lib.ReadFile('./metrics/testproj/repos_tags.sql'): ok\n",
        "lib.ReadFile('./util_sql/exclude_bots.sql'): ok\n",
        // fallback to the shared file (Go bug 10: the line had no newline)
        "lib.ReadFile('./metrics/shared/cumulative_periods.sql'): ok\n",
        "Points:\n#1 2012-07-01 0 ",
        "upserts: 6\n",
        "create table if not exists \"tcompanies\"(time timestamp primary key, \"companies_name\" text, \
         \"companies_value\" text, \"company_first\" text, \"company_len\" text, \"company_len_norm\" text, \
         \"company_lower\" text, \"company_upper\" text, \"company_upper_norm\" text)\n",
        "Final 0 threads join\n",
    ] {
        assert!(stdout.contains(needle), "missing {needle:?} in:\n{stdout}");
    }
    // the disabled tag is started and ended but does nothing
    assert!(
        stdout.contains("Start Tag 'Disabled' --> 'disabled'\nEnd Tag 'Disabled' --> 'disabled'\n")
    );
    assert!(!stdout.contains("does_not_exist"));
}

#[test]
fn many_tags_multi_threaded() {
    let Some(rs) = both(
        &Scenario::new("many_mt")
            .env("GHA2DB_TAGS_YAML", "metrics/testproj/many.yaml")
            .threads4(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    let stdout = rs.out.stdout_str();
    assert!(stdout.contains("Final 3 threads join\n"), "{stdout}");
    assert_eq!(stdout.matches("have no values").count(), 2);
    // the two tags without values write nothing, so no table is created for them
    let tables = tables_of(&rs.db);
    assert_eq!(tables.len(), 18);
    assert!(!tables.contains(&"tmany16".to_string()));
    assert_eq!(count(&rs.db, "select count(*) from tmany18"), 2);
    assert_eq!(count(&rs.db, "select count(*) from tmany19"), 4);
    assert_eq!(
        rows(&rs.db, "select n, v from tmany15 order by time"),
        vec![
            vec!["Poland", "poland"],
            vec!["United States", "unitedstates"],
            vec!["Germany", "germany"],
            vec!["Poland", "poland"],
            vec!["Japan", "japan"],
        ]
    );
}

#[test]
fn many_tags_debug_multi_threaded() {
    // debug output from 4 workers at once: same lines, any order
    let Some(rs) = both(
        &Scenario::new("many_mt_debug")
            .env("GHA2DB_TAGS_YAML", "metrics/testproj/many.yaml")
            .env("GHA2DB_DEBUG", "1")
            .threads4(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    let stdout = rs.out.stdout_str();
    assert!(stdout.contains("threading: 4 >= 4, waiting on the channel\n"));
    assert!(stdout.contains("threading: fianl thread joined, num threads: 0\n"));
    assert_eq!(
        stdout
            .matches("threading: thread joined, num threads: 3\n")
            .count(),
        17
    );
    assert_eq!(stdout.matches("Synced tag ").count(), 20);
}

// ---------------------------------------------------------------------------
// Existing series tables
// ---------------------------------------------------------------------------

const OLD_SERIES: &str = "
create table trepos(time timestamp primary key, repo_name text, stale_column text);
insert into trepos values ('2012-07-01 00:00:00', 'old/value', 'x'), ('2011-01-01 00:00:00', 'older/value', 'y'), ('2012-07-01 05:00:00', 'gone/soon', 'z');
create table tnothing(time timestamp primary key, nothing_name text, nothing_value text);
insert into tnothing values ('2012-07-01 00:00:00', 'a', 'a'), ('2012-07-01 01:00:00', 'b', 'b');
";

#[test]
fn existing_series_are_truncated() {
    let Some(rs) = both(&Scenario::new("existing").setup(OLD_SERIES)) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert!(!rs.out.stdout_str().contains("warning"));
    // truncated then re-filled; the stale column stays in the table (NULL)
    assert_eq!(
        rows(&rs.db, "select time::text, repo_name, coalesce(stale_column, '<null>') from trepos order by time"),
        vec![
            vec!["2012-07-01 00:00:00", "cncf/devstats", "<null>"],
            vec!["2012-07-01 01:00:00", "envoyproxy/envoy", "<null>"],
            vec!["2012-07-01 02:00:00", "kubernetes/kubernetes", "<null>"],
            vec!["2012-07-01 03:00:00", "kubernetes/website", "<null>"],
            vec!["2012-07-01 04:00:00", "prometheus/prometheus", "<null>"],
            vec!["2012-07-01 05:00:00", "zé/ünïcode-ok", "<null>"],
        ]
    );
    // a tag with no values still empties its table
    assert_eq!(count(&rs.db, "select count(*) from tnothing"), 0);
    let mut expected: Vec<&str> = TESTPROJ_SERIES.to_vec();
    expected.push("tnothing");
    expected.sort();
    assert_eq!(tables_of(&rs.db), expected);
}

#[test]
fn reader_lock_falls_back_to_delete() {
    // Another session holds ACCESS SHARE on trepos: `truncate` hits the 500 ms
    // lock timeout, the transaction is rolled back and `delete` (which does
    // not conflict) empties the table instead — Go bug 9 (fixed) made the
    // delete fail with "current transaction is aborted".
    let Some(rs) = both(&Scenario::new("reader_lock").setup(OLD_SERIES).lock(
        "trepos",
        "access share",
        4,
    )) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    let stdout = rs.out.stdout_str();
    assert!(
        stdout.contains(
            "truncate failed for trepos (warning): pq: canceling statement due to lock timeout\n"
        ),
        "{stdout}"
    );
    assert!(!stdout.contains("delete failed"), "{stdout}");
    assert_eq!(
        rows(
            &rs.db,
            "select time::text, repo_name from trepos order by time"
        ),
        vec![
            vec!["2012-07-01 00:00:00", "cncf/devstats"],
            vec!["2012-07-01 01:00:00", "envoyproxy/envoy"],
            vec!["2012-07-01 02:00:00", "kubernetes/kubernetes"],
            vec!["2012-07-01 03:00:00", "kubernetes/website"],
            vec!["2012-07-01 04:00:00", "prometheus/prometheus"],
            vec!["2012-07-01 05:00:00", "zé/ünïcode-ok"],
        ]
    );
}

#[test]
fn exclusive_lock_keeps_old_rows() {
    // EXCLUSIVE conflicts with both truncate and delete: both time out with a
    // warning, the tool goes on and the upserts wait for the lock; old rows
    // at other times survive, the row at 2012-07-01 00:00 is overwritten.
    let Some(rs) = both(&Scenario::new("exclusive_lock").setup(OLD_SERIES).lock(
        "trepos",
        "exclusive",
        4,
    )) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    let stdout = rs.out.stdout_str();
    assert!(
        stdout.contains(
            "truncate failed for trepos (warning): pq: canceling statement due to lock timeout\n\
             delete failed for trepos (warning): pq: canceling statement due to lock timeout\n"
        ),
        "{stdout}"
    );
    assert_eq!(
        rows(&rs.db, "select time::text, repo_name, coalesce(stale_column, '<null>') from trepos order by time"),
        vec![
            vec!["2011-01-01 00:00:00", "older/value", "y"],
            vec!["2012-07-01 00:00:00", "cncf/devstats", "x"],
            vec!["2012-07-01 01:00:00", "envoyproxy/envoy", "<null>"],
            vec!["2012-07-01 02:00:00", "kubernetes/kubernetes", "<null>"],
            vec!["2012-07-01 03:00:00", "kubernetes/website", "<null>"],
            vec!["2012-07-01 04:00:00", "prometheus/prometheus", "<null>"],
            vec!["2012-07-01 05:00:00", "zé/ünïcode-ok", "z"],
        ]
    );
}

#[test]
fn skip_tsdb_touches_nothing() {
    let Some(rs) = both(
        &Scenario::new("skip_tsdb")
            .setup(OLD_SERIES)
            .env("GHA2DB_SKIPTSDB", "1")
            .env("GHA2DB_DEBUG", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    let stdout = rs.out.stdout_str();
    // every processed tag (9 of 10: one disabled) reports the skip
    assert_eq!(stdout.matches("Skipping tags series write\n").count(), 9);
    assert!(!stdout.contains("WriteTSPoints"));
    // the SQLs still run (and the "no values" warning is still printed)
    assert!(stdout.contains("'repos': map[repo_name:cncf/devstats]\n"));
    assert!(stdout.contains("have no values"));
    assert_eq!(tables_of(&rs.db), ["tnothing", "trepos"]);
    assert_eq!(count(&rs.db, "select count(*) from trepos"), 3);
    assert_eq!(count(&rs.db, "select count(*) from tnothing"), 2);
}

// ---------------------------------------------------------------------------
// Configuration variants
// ---------------------------------------------------------------------------

#[test]
fn only_disabled_tags() {
    let Some(rs) = both(
        &Scenario::new("disabled_only")
            .env("GHA2DB_TAGS_YAML", "metrics/testproj/disabled_only.yaml")
            .threads4(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert_eq!(
        mask_go_durations(&rs.out.stdout_str()),
        "Compiled None, commit: None on None using None\nFinal 2 threads join\nTime: <duration>\n"
    );
    assert!(tables_of(&rs.db).is_empty());
}

#[test]
fn empty_tags_list() {
    let Some(rs) =
        both(&Scenario::new("empty_yaml").env("GHA2DB_TAGS_YAML", "metrics/testproj/empty.yaml"))
    else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert_eq!(
        mask_go_durations(&rs.out.stdout_str()),
        "Compiled None, commit: None on None using None\nFinal 0 threads join\nTime: <duration>\n"
    );
    assert!(tables_of(&rs.db).is_empty());
}

#[test]
fn no_project_uses_metrics_root() {
    // GHA2DB_PROJECT unset: metrics/tags.yaml and metrics/<sql>.sql, no fallback
    let Some(rs) = both(&Scenario::new("no_project").no_env("GHA2DB_PROJECT")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert_eq!(tables_of(&rs.db), ["torgs"]);
    assert_eq!(
        rows(
            &rs.db,
            "select org_name, org_value from torgs order by time"
        ),
        vec![
            vec!["cncf", "cncf"],
            vec!["envoyproxy", "envoyproxy"],
            vec!["kubernetes", "kubernetes"],
            vec!["prometheus", "prometheus"],
            vec!["zé", "ze"],
        ]
    );
}

#[test]
fn datadir_mode() {
    // Without GHA2DB_LOCAL files come from GHA2DB_DATADIR, whatever the cwd.
    let data = data_dir().to_string_lossy().into_owned();
    let tmp = tempfile::tempdir().unwrap();
    let Some(rs) = both(
        &Scenario::new("datadir")
            .no_env("GHA2DB_LOCAL")
            .env("GHA2DB_DATADIR", &data)
            .env("GHA2DB_DEBUG", "1")
            .cwd(tmp.path()),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0), "{}", rs.out.stderr_str());
    assert_eq!(tables_of(&rs.db), TESTPROJ_SERIES);
    let stdout = rs.out.stdout_str();
    // the data dir gets a trailing slash appended when missing
    assert!(
        stdout.contains(&format!(
            "lib.ReadFile('{data}/metrics/testproj/tags.yaml'): ok\n"
        )),
        "{stdout}"
    );
}

#[test]
fn datadir_default_is_missing() {
    // Neither GHA2DB_LOCAL nor GHA2DB_DATADIR: /etc/gha2db/ does not exist here.
    if Path::new("/etc/gha2db").exists() {
        eprintln!("[compat] /etc/gha2db exists — skipping");
        return;
    }
    let Some(rs) = both(&Scenario::new("datadir_default").no_env("GHA2DB_LOCAL")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2), "{}", rs.out.stderr_str());
    assert!(rs
        .out
        .stderr_str()
        .contains("Error: 'open /etc/gha2db/metrics/shared/tags.yaml: no such file or directory'"));
    assert!(rs.out.stdout_str().contains(
        "lib.ReadFile('/etc/gha2db/metrics/shared/tags.yaml'): error: open /etc/gha2db/metrics/shared/tags.yaml: no such file or directory\n"
    ));
}

// ---------------------------------------------------------------------------
// Fatal errors
// ---------------------------------------------------------------------------

#[test]
fn unknown_other_tag_column() {
    let Some(rs) = both(
        &Scenario::new("bad_column").env("GHA2DB_TAGS_YAML", "metrics/testproj/bad_column.yaml"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2), "{}", rs.out.stderr_str());
    assert!(
        rs.out
            .stderr_str()
            .contains("Error: 'other tag: name: extra: column no_such_column not found'"),
        "{}",
        rs.out.stderr_str()
    );
    // the table was truncated/created... no: created only on write, which never happens
    assert!(tables_of(&rs.db).is_empty());
}

#[test]
fn sql_error_is_fatal() {
    let Some(rs) =
        both(&Scenario::new("bad_sql").env("GHA2DB_TAGS_YAML", "metrics/testproj/bad_sql.yaml"))
    else {
        return;
    };
    assert_eq!(rs.out.code, Some(2), "{}", rs.out.stderr_str());
    let stdout = rs.out.stdout_str();
    assert!(
        stdout.contains("PqError: code=42601, name=syntax_error, detail=\n"),
        "{stdout}"
    );
    // the failing query is echoed
    assert!(stdout.contains("frm\n  gha_repos"), "{stdout}");
    assert!(
        rs.out
            .stderr_str()
            .contains("Error: 'pq: syntax error at or near \"gha_repos\"'"),
        "{}",
        rs.out.stderr_str()
    );
}

#[test]
fn missing_sql_file() {
    let Some(rs) = both(
        &Scenario::new("missing_sql").env("GHA2DB_TAGS_YAML", "metrics/testproj/missing_sql.yaml"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2), "{}", rs.out.stderr_str());
    // ReadFile tried the project file, then the shared one
    assert!(
        rs.out.stdout_str().ends_with(
            "lib.ReadFile('./metrics/shared/no_such_file.sql'): error: open ./metrics/shared/no_such_file.sql: no such file or directory\n"
        ),
        "{}",
        rs.out.stdout_str()
    );
    assert!(rs
        .out
        .stderr_str()
        .contains("Error: 'open ./metrics/shared/no_such_file.sql: no such file or directory'"));
}

#[test]
fn missing_tags_yaml() {
    let Some(rs) =
        both(&Scenario::new("missing_yaml").env("GHA2DB_TAGS_YAML", "metrics/testproj/nope.yaml"))
    else {
        return;
    };
    assert_eq!(rs.out.code, Some(2), "{}", rs.out.stderr_str());
    assert!(rs
        .out
        .stderr_str()
        .contains("Error: 'open ./metrics/shared/nope.yaml: no such file or directory'"));
}

#[test]
fn malformed_tags_yaml() {
    let Some(rs) = both(
        &Scenario::new("malformed_yaml")
            .env("GHA2DB_TAGS_YAML", "metrics/testproj/malformed.yaml")
            .ignore_errors(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2), "{}", rs.out.stderr_str());
    assert!(
        rs.out.stderr_str().contains("Error: 'yaml: "),
        "{}",
        rs.out.stderr_str()
    );
}

#[test]
fn unreachable_server() {
    let Some(rs) = both(&Scenario::new("unreachable").no_seed().env("PG_PORT", "1")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2), "{}", rs.out.stderr_str());
    assert!(
        rs.out.stderr_str().contains("connect: connection refused'"),
        "{}",
        rs.out.stderr_str()
    );
}
