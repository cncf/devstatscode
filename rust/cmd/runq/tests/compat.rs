//! Go ⇄ Rust compatibility tests for `runq`.
//!
//! Every case runs the Go binary and the Rust binary with the same arguments,
//! environment and working directory (`compat/fixtures/runq/data`, laid out
//! like a `cncf/devstats` checkout: `util_sql/*` and `metrics/shared/*` are
//! real files, `sql/*` test-specific ones) and compares exit code, stdout
//! (`Time:` durations masked) and, for fatal errors, the `Error: '…'` lines of
//! stderr. Cases that need a database get two identical scratch databases
//! (`dbtest_runq_<name>_go` / `_rs`: the DevStats schema, the `tags` seed and
//! `compat/fixtures/runq/seed.sql`); after a run the content of the tables the
//! SQL may have modified is compared too, and `GHA2DB_CSVOUT` files are
//! compared byte by byte.
//!
//! The DB cases need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise); the dry-run / usage cases always run.

use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};

fn go_bin() -> Option<PathBuf> {
    go_binary("runq")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_runq"))
}

/// `compat/fixtures/runq/data` — the "devstats checkout" the tool runs in.
fn data_dir() -> PathBuf {
    fixture("runq/data")
}

fn structure_sql() -> String {
    fs::read_to_string(fixture("structure/full_structure.sql")).unwrap()
}

fn tags_seed_sql() -> String {
    fs::read_to_string(fixture("tags/seed.sql")).unwrap()
}

fn seed_sql() -> String {
    fs::read_to_string(fixture("runq/seed.sql")).unwrap()
}

/// One compatibility case.
struct Case<'a> {
    name: &'a str,
    args: Vec<String>,
    /// Environment on top of the `PG_*` connection variables (`GHA2DB_LOCAL=1`
    /// by default: SQL paths relative to `cwd`).
    env: Vec<(&'a str, &'a str)>,
    cwd: PathBuf,
    /// Run against a scratch database (otherwise no `PG_*` variables at all).
    db: bool,
    /// Tables whose full content is compared after the run.
    tables: Vec<&'a str>,
    /// Queries whose results are compared after the run (for tables whose
    /// seed uses `now()`-relative stamps, which differ between the two
    /// databases).
    queries: Vec<&'a str>,
    /// Write `GHA2DB_CSVOUT` into a temp dir and compare the files.
    csv: bool,
    /// The SQL has no total order (`union`, `distinct`): compare the data
    /// rows of the table as a sorted multiset.
    unordered_rows: bool,
    /// Compare the `Error: '…'` stderr lines of fatal errors.
    compare_errors: bool,
}

impl<'a> Case<'a> {
    fn new(name: &'a str) -> Self {
        Case {
            name,
            args: Vec::new(),
            env: vec![("GHA2DB_LOCAL", "1")],
            cwd: data_dir(),
            db: true,
            tables: Vec::new(),
            queries: Vec::new(),
            csv: false,
            unordered_rows: false,
            compare_errors: true,
        }
    }
    fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args = args.into_iter().map(Into::into).collect();
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
    fn no_db(mut self) -> Self {
        self.db = false;
        self
    }
    fn tables(mut self, t: &[&'a str]) -> Self {
        self.tables = t.to_vec();
        self
    }
    fn queries(mut self, q: &[&'a str]) -> Self {
        self.queries = q.to_vec();
        self
    }
    fn csv(mut self) -> Self {
        self.csv = true;
        self
    }
    fn unordered_rows(mut self) -> Self {
        self.unordered_rows = true;
        self
    }
}

/// One side of a case: its database (if any), CSV directory and outcome.
struct Side {
    db: Option<TestDb>,
    csv_dir: Option<tempfile::TempDir>,
    out: Outcome,
}

impl Side {
    fn csv_path(&self) -> PathBuf {
        self.csv_dir.as_ref().unwrap().path().join("out.csv")
    }
    fn csv_bytes(&self) -> Option<Vec<u8>> {
        fs::read(self.csv_path()).ok()
    }
}

/// Run `bin` per the case, on its own database when one is needed.
fn run_side(bin: &Path, case: &Case<'_>, suffix: &str) -> Option<Side> {
    let db = if case.db {
        let db = TestDb::fresh(&format!("runq_{}_{}", case.name, suffix))?;
        db.exec(&structure_sql());
        db.exec(&tags_seed_sql());
        db.exec(&seed_sql());
        Some(db)
    } else {
        None
    };
    let csv_dir = if case.csv {
        Some(tempfile::tempdir().unwrap())
    } else {
        None
    };
    let csv_path = csv_dir
        .as_ref()
        .map(|d| d.path().join("out.csv").to_string_lossy().into_owned());
    let mut inv = Invocation::new().cwd(case.cwd.clone());
    if let Some(db) = &db {
        for (k, v) in db.env() {
            inv = inv.env(k, v);
        }
    } else {
        inv = inv.env("GHA2DB_SKIPLOG", "1").env("GHA2DB_SKIPTIME", "1");
    }
    for (k, v) in &case.env {
        inv = inv.env(k, v);
    }
    if let Some(p) = &csv_path {
        inv = inv.env("GHA2DB_CSVOUT", p);
    }
    for a in &case.args {
        inv = inv.arg(a.clone());
    }
    let out = run(bin, &inv);
    Some(Side { db, csv_dir, out })
}

/// `Error: '…'` lines of a fatal error report.
fn error_lines(stderr: &str) -> Vec<String> {
    stderr
        .lines()
        .filter(|l| l.starts_with("Error: '"))
        .map(str::to_string)
        .collect()
}

/// `{{rnd}}` expands to a random hex string: mask `tmp_<hex>` identifiers.
fn mask_rnd(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(pos) = rest.find("tmp_") {
        out.push_str(&rest[..pos + 4]);
        let tail = &rest[pos + 4..];
        let n = tail.bytes().take_while(|b| b.is_ascii_hexdigit()).count();
        if n > 0 {
            out.push_str("<rnd>");
        }
        rest = &tail[n..];
    }
    out.push_str(rest);
    out
}

/// Sort the data rows of the ASCII table (the lines between the `+---+`
/// separator and the `\---/` bottom frame).
fn sort_data_rows(s: &str) -> String {
    let mut lines: Vec<String> = s.split('\n').map(str::to_string).collect();
    let sep = lines
        .iter()
        .position(|l| l.starts_with('+') && l.ends_with('+'));
    let bottom = lines
        .iter()
        .position(|l| l.starts_with('\\') && l.ends_with('/'));
    if let (Some(a), Some(b)) = (sep, bottom) {
        if a + 1 < b {
            lines[a + 1..b].sort();
        }
    }
    lines.join("\n")
}

fn normalize_stdout(stdout: &[u8], case: &Case<'_>, side: &Side) -> Vec<u8> {
    // Raw bytes (bytea columns) may not be UTF-8: normalize the lossy text
    // only when a mask applies, otherwise compare the bytes as they are.
    let text = String::from_utf8_lossy(stdout).into_owned();
    let mut s = mask_go_durations(&text);
    s = mask_rnd(&s);
    // each side writes its CSV into its own temp dir and works on its own
    // database
    if case.csv {
        s = s.replace(&side.csv_path().to_string_lossy().to_string(), "<csv>");
    }
    if let Some(db) = &side.db {
        s = s.replace(&db.name, "<db>");
    }
    // `GHA2DB_QOUT` dumps the arguments of the log insert: `time.Now()`
    // differs (and Go appends its monotonic reading `m=+0.0123`).
    s = s
        .split('\n')
        .map(|l| {
            if l.starts_with("[1:runq 2:") {
                if let (Some(a), Some(b)) = (l.find(" 3:"), l.find(" 4:")) {
                    if a < b {
                        return format!("{} 3:<now>{}", &l[..a], &l[b..]);
                    }
                }
            }
            l.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    if case.unordered_rows {
        s = sort_data_rows(&s);
    }
    if s == text {
        return stdout.to_vec();
    }
    s.into_bytes()
}

/// Content of the compared tables.
fn table_states(db: &TestDb, case: &Case<'_>) -> Vec<(String, cpg::Snapshot)> {
    let con = db.conn();
    let mut out: Vec<(String, cpg::Snapshot)> = case
        .tables
        .iter()
        .map(|t| (t.to_string(), cpg::table_data(&con, t)))
        .collect();
    out.extend(
        case.queries
            .iter()
            .map(|q| (q.to_string(), cpg::snapshot(&con, q, &[]))),
    );
    con.close();
    out
}

fn counts(db: &TestDb) -> Vec<(String, i64)> {
    let con = db.conn();
    let c = cpg::table_counts(&con);
    con.close();
    c
}

/// Run both binaries and compare everything the case asks for; returns the
/// Rust side for further assertions (`None` when the DB tests are skipped).
fn both(case: &Case<'_>) -> Option<Side> {
    let rust = run_side(&rust_bin(), case, "rs")?;
    if let Some(go) = go_bin() {
        let go = run_side(&go, case, "go").unwrap();
        let ctx = format!(
            "\ncase {:?} args {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
            case.name,
            case.args,
            case.env,
            go.out.code,
            go.out.stdout_str(),
            go.out.stderr_str(),
            rust.out.code,
            rust.out.stdout_str(),
            rust.out.stderr_str()
        );
        assert_eq!(go.out.code, rust.out.code, "exit code{ctx}");
        let g = normalize_stdout(&go.out.stdout, case, &go);
        let r = normalize_stdout(&rust.out.stdout, case, &rust);
        assert!(
            g == r,
            "stdout{ctx}\n--- normalized go:\n{}\n--- normalized rust:\n{}",
            String::from_utf8_lossy(&g),
            String::from_utf8_lossy(&r)
        );
        if case.compare_errors {
            assert_eq!(
                error_lines(&go.out.stderr_str()),
                error_lines(&rust.out.stderr_str()),
                "fatal error lines{ctx}"
            );
        }
        if case.csv {
            assert_eq!(go.csv_bytes(), rust.csv_bytes(), "CSV file{ctx}");
        }
        if let (Some(gdb), Some(rdb)) = (&go.db, &rust.db) {
            assert_eq!(counts(gdb), counts(rdb), "row counts{ctx}");
            assert_eq!(
                table_states(gdb, case),
                table_states(rdb, case),
                "table content{ctx}"
            );
        }
    }
    Some(rust)
}

fn stdout_of(side: &Side) -> String {
    side.out.stdout_str()
}

fn rows_of(side: &Side, sql: &str) -> Vec<Vec<String>> {
    let con = side.db.as_ref().unwrap().conn();
    let s = cpg::snapshot(&con, sql, &[]);
    con.close();
    s.rows
}

// ---------------------------------------------------------------------------
// Usage / argument handling (no database)
// ---------------------------------------------------------------------------

#[test]
fn no_arguments_prints_usage_and_exits_1() {
    let side = both(&Case::new("usage").no_db()).unwrap();
    assert_eq!(side.out.code(), 1);
    let out = stdout_of(&side);
    assert!(out.contains("Required SQL file name [param1 value1 [param2 value2 ...]]\n"));
    assert!(out.contains(
        "Special replace 'qr' 'period,from,to' is used for {{period.alias.name}} replacements\n"
    ));
    assert!(out.contains("Example: GHA2DB_QOUT=1 PG_DB=allprj runq metrics/shared/bus_factor.sql qr '1 week,,' {{exclude_bots}} \"not in ('')\"\n"));
}

#[test]
fn odd_parameter_count_exits_1() {
    let side = both(&Case::new("odd").no_db().args(["sql/simple.sql", "{{x}}"])).unwrap();
    assert_eq!(side.out.code(), 1);
    assert!(stdout_of(&side).contains("Must provide correct parameter value pairs: [{{x}}]\n"));

    let side = both(
        &Case::new("odd3")
            .no_db()
            .args(["sql/simple.sql", "{{x}}", "1", "{{y}}"]),
    )
    .unwrap();
    assert_eq!(side.out.code(), 1);
    assert!(
        stdout_of(&side).contains("Must provide correct parameter value pairs: [{{x}} 1 {{y}}]\n")
    );
}

#[test]
fn missing_sql_file_is_fatal() {
    let side = both(&Case::new("missing").no_db().args(["sql/nope.sql"])).unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        error_lines(&side.out.stderr_str()),
        vec!["Error: 'open ./sql/nope.sql: no such file or directory'".to_string()]
    );
}

#[test]
fn readfile_of_missing_file_is_fatal() {
    let side = both(&Case::new("readfile_missing").no_db().args([
        "sql/readfile.sql",
        "{{ids}}",
        "readfile:sql/nope.dat",
    ]))
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        error_lines(&side.out.stderr_str()),
        vec!["Error: 'open sql/nope.dat: no such file or directory'".to_string()]
    );
}

#[test]
fn qr_with_less_than_three_parts_is_fatal() {
    let side = both(
        &Case::new("qr_bad")
            .no_db()
            .args(["sql/qr.sql", "qr", "1 week"]),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        error_lines(&side.out.stderr_str()),
        vec!["Error: 'qr parameter must be 'period,from,to', got: '1 week''".to_string()]
    );
    let side = both(
        &Case::new("qr_bad2")
            .no_db()
            .args(["sql/qr.sql", "qr", "1 week,"]),
    )
    .unwrap();
    assert_eq!(side.out.code(), 2);
}

// ---------------------------------------------------------------------------
// Dry run: the transformed SQL is printed, no database is touched
// ---------------------------------------------------------------------------

#[test]
fn dry_run_prints_replaced_sql() {
    let side = both(&Case::new("dry").no_db().env("GHA2DB_DRY_RUN", "1").args([
        "sql/simple.sql",
        "{{x}}",
        "'a%b'",
    ]))
    .unwrap();
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        mask_go_durations(&stdout_of(&side)),
        "Compiled None, commit: None on None using None\nselect 'a%b' as v;\n\nTime: <duration>\n"
    );
}

#[test]
fn dry_run_with_negative_debug_uses_plain_printf() {
    let side = both(
        &Case::new("dry_neg")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .env("GHA2DB_DEBUG", "-1")
            .args(["sql/simple.sql", "{{x}}", "1"]),
    )
    .unwrap();
    assert_eq!(side.out.code(), 0);
    // no `Compiled …` header, no `Time:` line
    assert_eq!(stdout_of(&side), "select 1 as v;\n\n");
}

#[test]
fn dry_run_qr_period_mode() {
    let side = both(
        &Case::new("dry_qr_period")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/qr.sql", "qr", "1 week,,"]),
    )
    .unwrap();
    let out = stdout_of(&side);
    assert!(
        out.contains(" (e.created_at >= now() - '1 week'::interval) "),
        "{out}"
    );
    // IntervalHours renders with `%f`; {{from}}/{{to}} become the interval
    // expressions in period mode
    assert!(out.contains("  168.000000 as hours,"), "{out}");
    assert!(
        out.contains("  '(now() -'1 week'::interval)' as sfrom,"),
        "{out}"
    );
    assert!(out.contains("  '(now())' as sto"), "{out}");
}

#[test]
fn dry_run_qr_range_mode() {
    let side = both(
        &Case::new("dry_qr_range")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/qr.sql", "qr", ",2015-01-01,2015-02-01"]),
    )
    .unwrap();
    let out = stdout_of(&side);
    assert!(
        out.contains(
            " (e.created_at >= '2015-01-01 00:00:00' and e.created_at < '2015-02-01 00:00:00') "
        ),
        "{out}"
    );
    assert!(out.contains("  744.000000 as hours,"), "{out}");
    // {{from}}/{{to}} are replaced with quoted stamps (the file quotes them
    // again — the real files use them bare)
    assert!(out.contains("  ''2015-01-01 00:00:00'' as sfrom,"), "{out}");
    assert!(out.contains("  ''2015-02-01 00:00:00'' as sto"), "{out}");
}

#[test]
fn dry_run_qr_extra_parts_and_period_aliases() {
    // more than 3 parts: extras ignored; period aliases like `d`, `w`, `q`
    for (name, qr) in [
        ("dry_qr_extra", "1 month,,,ignored"),
        ("dry_qr_d", "d,,"),
        ("dry_qr_w", "w,,"),
        ("dry_qr_q", "q,,"),
        ("dry_qr_y", "y,,"),
        ("dry_qr_hours", "36 hours,,"),
    ] {
        let side = both(&Case::new(name).no_db().env("GHA2DB_DRY_RUN", "1").args([
            "sql/qr.sql",
            "qr",
            qr,
        ]))
        .unwrap();
        assert_eq!(side.out.code(), 0, "{name}");
    }
}

#[test]
fn dry_run_all_placeholders_real_file() {
    let side = both(
        &Case::new("dry_all_placeholders")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args([
                "util_sql/all_placeholders.sql",
                "qr",
                ",2020-01-01,2020-01-08",
                "{{exclude_bots}}",
                "not in ('bot')",
                "{{project_scale}}",
                "1.5",
                "{{n}}",
                "7",
                "{{lim}}",
                "10",
                "{{period}}",
                "'1 week'",
            ]),
    )
    .unwrap();
    let out = stdout_of(&side);
    assert!(out.contains("  not in ('bot') as exclude,"), "{out}");
    assert!(out.contains("  168.000000 as range,"), "{out}");
    assert!(out.contains("  1.5 as project_scale,"), "{out}");
    assert!(out.contains("  '2020-01-01 00:00:00' as sfrom,"), "{out}");
    assert!(out.contains("  '2020-01-08 00:00:00' as sto,"), "{out}");
    assert!(
        out.contains(
            "   (now() >= '2020-01-01 00:00:00' and now() < '2020-01-08 00:00:00')  as period,"
        ),
        "{out}"
    );
    assert!(out.contains("  '1 week' as period2,"), "{out}");
    assert!(out.contains("  7 as n,"), "{out}");
    assert!(out.contains("  10 as lim"), "{out}");
}

#[test]
fn dry_run_explain_replaces_every_select_newline() {
    let side = both(
        &Case::new("dry_explain")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .env("GHA2DB_EXPLAIN", "1")
            .args(["sql/multi_select.sql"]),
    )
    .unwrap();
    let out = stdout_of(&side);
    assert_eq!(out.matches("explain select\n").count(), 2, "{out}");
}

#[test]
fn dry_run_rnd_is_random_hex() {
    let side = both(
        &Case::new("dry_rnd")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/rnd.sql"]),
    )
    .unwrap();
    let out = stdout_of(&side);
    assert!(!out.contains("{{rnd}}"), "{out}");
    let masked = mask_rnd(&out);
    assert_eq!(masked.matches("tmp_<rnd>").count(), 3, "{out}");
    // the same random string for every occurrence within a run
    let first = out.find("tmp_").unwrap();
    let ident: String = out[first..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    assert_eq!(out.matches(&ident).count(), 3, "{out}");
    // and a different one on the next run
    let again = run_side(
        &rust_bin(),
        &Case::new("dry_rnd")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/rnd.sql"]),
        "rs",
    )
    .unwrap();
    assert_ne!(stdout_of(&again), out);
}

#[test]
fn dry_run_readfile_replacement_and_debug() {
    let side = both(
        &Case::new("dry_readfile")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .env("GHA2DB_DEBUG", "1")
            .args([
                "sql/readfile.sql",
                "{{exclude_bots}}",
                "readfile:util_sql/exclude_bots.sql",
                "{{ids}}",
                "readfile:sql/ids.dat",
            ]),
    )
    .unwrap();
    let out = stdout_of(&side);
    assert!(
        out.contains("Reading file: util_sql/exclude_bots.sql\n"),
        "{out}"
    );
    assert!(out.contains("Reading file: sql/ids.dat\n"), "{out}");
    assert!(out.contains("not like all(array["), "{out}");
    assert!(out.contains("id in (100, 101, 103,\n104, 105\n)"), "{out}");
}

#[test]
fn dry_run_readfile_literal_when_only_prefix() {
    // exactly "readfile:" (9 chars) is a literal value, not a file
    let side = both(
        &Case::new("dry_readfile_literal")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/simple.sql", "{{x}}", "readfile:"]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("select readfile: as v;\n"));
}

#[test]
fn dry_run_duplicate_param_last_wins_and_empty_name() {
    let side = both(
        &Case::new("dry_dup_param")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/simple.sql", "{{x}}", "1", "{{x}}", "2"]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("select 2 as v;\n"));
    // an empty parameter name inserts the value at every character boundary
    let side = both(
        &Case::new("dry_empty_name")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/simple.sql", "", "_"]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("_s_e_l_e_c_t_ _{_{_x_}_}_ _a_s_ _v_;_\n_"));
}

#[test]
fn dry_run_unicode_and_multiline_values() {
    let side = both(
        &Case::new("dry_unicode")
            .no_db()
            .env("GHA2DB_DRY_RUN", "1")
            .args(["sql/simple.sql", "{{x}}", "'Zażółć\ngęślą 日本'"]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("select 'Zażółć\ngęślą 日本' as v;\n"));
}

#[test]
fn dry_run_absolute_and_datadir_prefixes() {
    let abs = data_dir().join("sql/simple.sql");
    let side = both(
        &Case::new("dry_absolute")
            .no_db()
            .no_env("GHA2DB_LOCAL")
            .env("GHA2DB_ABSOLUTE", "1")
            .env("GHA2DB_DRY_RUN", "1")
            .cwd(std::env::temp_dir())
            .args([
                abs.to_string_lossy().to_string(),
                "{{x}}".to_string(),
                "3".to_string(),
            ]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("select 3 as v;\n"));
    // GHA2DB_ABSOLUTE wins over GHA2DB_LOCAL
    let side = both(
        &Case::new("dry_absolute_local")
            .no_db()
            .env("GHA2DB_ABSOLUTE", "1")
            .env("GHA2DB_DRY_RUN", "1")
            .cwd(std::env::temp_dir())
            .args([
                abs.to_string_lossy().to_string(),
                "{{x}}".to_string(),
                "4".to_string(),
            ]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("select 4 as v;\n"));
    // data dir (cron mode): DataDir + relative path
    let dd = format!("{}/", data_dir().to_string_lossy());
    let side = both(
        &Case::new("dry_datadir")
            .no_db()
            .no_env("GHA2DB_LOCAL")
            .env("GHA2DB_DATADIR", &dd)
            .env("GHA2DB_DRY_RUN", "1")
            .cwd(std::env::temp_dir())
            .args(["sql/simple.sql", "{{x}}", "5"]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("select 5 as v;\n"));
    // without the trailing slash the paths are simply concatenated
    let dd = data_dir().to_string_lossy().to_string();
    let side = both(
        &Case::new("dry_datadir_noslash")
            .no_db()
            .no_env("GHA2DB_LOCAL")
            .env("GHA2DB_DATADIR", &dd)
            .env("GHA2DB_DRY_RUN", "1")
            .cwd(std::env::temp_dir())
            .args(["/sql/simple.sql", "{{x}}", "6"]),
    )
    .unwrap();
    assert!(stdout_of(&side).contains("select 6 as v;\n"));
}

// ---------------------------------------------------------------------------
// Queries against the database
// ---------------------------------------------------------------------------

#[test]
fn every_column_type_renders_like_go() {
    let Some(side) = both(
        &Case::new("types")
            .args(["sql/types.sql", "{{cond}}", ">= 0"])
            .csv(),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    // spot checks of Go's *[]byte conversions
    assert!(out.contains("|1e+06"), "{out}");
    assert!(out.contains("|2012-07-01T00:00:00Z"), "{out}");
    assert!(out.contains("|2012-07-01T12:34:56.123456Z"), "{out}");
    assert!(out.contains("|0000-01-01T12:34:56.789Z"), "{out}");
    assert!(out.contains("|1 year 2 mons 3 days 04:05:06.5"), "{out}");
    assert!(out.contains("|true"), "{out}");
    assert!(out.contains("|infinity"), "{out}");
    assert!(out.contains("|NaN"), "{out}");
    assert!(out.contains("Rows: 5\n"), "{out}");
    assert!(side.csv_bytes().unwrap().starts_with(
        b"id,i2,i4,i8,num,f4,f8,b,t,vc,ch,c1,nm,d,ts,tstz,tm,tmtz,iv,by,j,jb,u,ip,ia,ta,oid_\n"
    ));
}

#[test]
fn types_subset_with_nulls() {
    let Some(side) =
        both(&Case::new("types_nulls").args(["sql/types.sql", "{{cond}}", "in (3, 4)"]))
    else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert!(stdout_of(&side).contains("Rows: 2\n"));
}

#[test]
fn duplicate_and_odd_column_names() {
    let Some(side) = both(&Case::new("dup_columns").args(["sql/dup_columns.sql"]).csv()) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("|a|a|?column?|?column?|?column?|?column?|?column?|?column?|?column?|?column?|?column?|%x|aż | | |a|\n"), "{out}");
    assert!(out.contains("|1|2|3       |4       |5       |6       |7       |8       |9       |10      |11      |12|13 | | |z|\n"), "{out}");
    assert_eq!(
        side.csv_bytes().unwrap(),
        b"a,a,?column?,?column?,?column?,?column?,?column?,?column?,?column?,?column?,?column?,%x,a\xc5\xbc,\" \",\" \",a\n1,2,3,4,5,6,7,8,9,10,11,12,13,,,z\n".to_vec()
    );
}

#[test]
fn more_than_100_columns() {
    let Some(side) = both(&Case::new("wide").args(["sql/wide.sql"]).csv()) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    // header names intact for 3-digit column indexes (Go bug 11)
    assert!(out.contains("|c99|c100|c101|c102|c103|\n"), "{out}");
    assert!(out.contains("|id|c2|c3|"), "{out}");
    assert!(out.contains("|3 |6 |9 |"), "{out}");
    assert!(out.contains("|303 |306 |last|\n"), "{out}");
    assert!(out.contains("Rows: 3\n"), "{out}");
}

#[test]
fn percent_signs_in_values_and_headers() {
    let Some(side) = both(&Case::new("pct").args(["sql/pct.sql", "{{n}}", "2"]).csv()) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    // printed verbatim (Go bug 12: used to show `%%`)
    assert!(out.contains("|a%b|100%|%s %d %v|%\n%|%%|\n"), "{out}");
    assert!(out.contains("|v  |w   |verbs   |nl |%%|\n"), "{out}");
}

#[test]
fn unicode_column_widths_are_byte_based() {
    let Some(side) = both(
        &Case::new("unicode_widths")
            .args(["sql/unicode_widths.sql"])
            .csv(),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    // widths are Go `len` (bytes), padding is rune based: multi-byte values
    // get extra spaces
    assert!(out.contains("|ż      |pl                        |jp                   |em        |日本    |short         |\n"), "{out}");
    assert!(out.contains("|ż      |Zażółć gęślą jaźń         |日本語テキスト              |emoji 😀   |a     |x             |\n"), "{out}");
    assert!(out.contains("|żółw   |p                         |日                    |😀         |bb    |a longer value|\n"), "{out}");
}

#[test]
fn empty_result_prints_no_data_and_writes_no_csv() {
    let Some(side) = both(
        &Case::new("empty")
            .args(["sql/empty.sql", "{{max}}", "1000"])
            .csv(),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        mask_go_durations(&stdout_of(&side)),
        "Compiled None, commit: None on None using None\nMetric returned no data\nTime: <duration>\n"
    );
    assert!(
        side.csv_bytes().is_none(),
        "no CSV file for an empty result"
    );
}

#[test]
fn csv_quoting_rules() {
    let Some(side) = both(&Case::new("csv").args(["sql/csv.sql"]).csv()) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let csv = String::from_utf8(side.csv_bytes().unwrap()).unwrap();
    let mut lines = csv.split_inclusive('\n');
    assert_eq!(lines.next().unwrap(), "a,b,c,d,e,f,g,h,i,j,k,l,m,n\n");
    // multi-line fields keep their newlines inside the quotes
    assert_eq!(
        csv,
        "a,b,c,d,e,f,g,h,i,j,k,l,m,n\n\
         plain,\"with,comma\",\"with \"\"quotes\"\"\",\"multi\nline\",\"cr\rhere\",\" leading space\",trailing space ,\"\ttab\",\"\\.\",,,Zażółć,\"\u{a0}nbsp\",a'b\n\
         row2,,\"\"\"\",\"\n\",\"\r\n\",\"  \",\" \",,\\\\.,x,,\"ż,ź\",\"\"\"ż\"\"\",\n"
    );
    assert!(
        stdout_of(&side).ends_with(&format!(
            "{} written\nTime: <duration>\n",
            side.csv_path().display()
        )) || {
            let masked = mask_go_durations(&stdout_of(&side));
            masked.ends_with(&format!(
                "{} written\nTime: <duration>\n",
                side.csv_path().display()
            ))
        }
    );
}

#[test]
fn csv_to_unwritable_path_is_fatal() {
    let Some(side) = both(
        &Case::new("csv_bad")
            .env("GHA2DB_CSVOUT", "/nonexistent-dir/out.csv")
            .args(["sql/simple.sql", "{{x}}", "1"]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 2);
    assert_eq!(
        error_lines(&side.out.stderr_str()),
        vec!["Error: 'open /nonexistent-dir/out.csv: no such file or directory'".to_string()]
    );
}

#[test]
fn bytea_raw_bytes_are_printed_verbatim() {
    let Some(side) = both(&Case::new("bytea").args(["sql/bytea.sql"]).csv()) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = &side.out.stdout;
    let pos = out
        .windows(4)
        .position(|w| w == b"\xDE\xAD\xBE\xEF")
        .expect("raw bytes in stdout");
    // width 4 bytes; Go counts DE AD (a valid 2-byte sequence) as one rune
    // and each of the two invalid bytes as one: 3 runes, one space of padding
    assert_eq!(&out[pos + 4..pos + 6], b" |");
    // the 0x00 0xff 0x10 value is printed raw as well
    assert!(out.windows(3).any(|w| w == b"\x00\xff\x10"));
    let csv = side.csv_bytes().unwrap();
    assert!(csv.windows(4).any(|w| w == b"\xDE\xAD\xBE\xEF"));
    assert!(csv.windows(3).any(|w| w == b"\x00\xff\x10"));
}

#[test]
fn big_result_set() {
    let Some(side) = both(
        &Case::new("big")
            .args(["sql/big.sql", "{{n}}", "2500"])
            .csv(),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert!(stdout_of(&side).contains("Rows: 2500\n"));
    assert_eq!(side.csv_bytes().unwrap().lines().count(), 2501);
}

use std::io::BufRead;

#[test]
fn qout_echoes_the_query() {
    let Some(side) =
        both(
            &Case::new("qout")
                .env("GHA2DB_QOUT", "1")
                .args(["sql/simple.sql", "{{x}}", "42"]),
        )
    else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("select 42 as v;\n"), "{out}");
    assert!(out.contains("|42|\n"), "{out}");
}

#[test]
fn explain_runs_the_plan() {
    let Some(side) = both(&Case::new("explain").env("GHA2DB_EXPLAIN", "1").args([
        "sql/explain.sql",
        "{{id}}",
        "100",
    ])) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("|QUERY PLAN"), "{out}");
    assert!(
        out.contains("Filter: (id = 100)") || out.contains("Index Cond: (id = 100)"),
        "{out}"
    );
}

#[test]
fn negative_debug_suppresses_time_line() {
    let Some(side) = both(&Case::new("neg_debug").env("GHA2DB_DEBUG", "-1").args([
        "sql/simple.sql",
        "{{x}}",
        "'x'",
    ])) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(out.ends_with("Rows: 1\n"), "{out}");
    assert!(!out.contains("Time:"), "{out}");
}

#[test]
fn rnd_temp_tables() {
    let Some(side) = both(&Case::new("rnd").args(["sql/rnd.sql"])) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains("|100|alice|\n|101|bob  |\n|102|carol|\n"),
        "{out}"
    );
}

#[test]
fn readfile_replacements_against_db() {
    let Some(side) = both(&Case::new("readfile").args([
        "sql/readfile.sql",
        "{{exclude_bots}}",
        "readfile:util_sql/exclude_bots.sql",
        "{{ids}}",
        "readfile:sql/ids.dat",
    ])) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    // bots (k8s-ci-robot, dependabot[bot]) excluded by the real pattern list
    assert!(
        out.contains("|100|alice|\n|101|bob  |\n|105|dave |\n"),
        "{out}"
    );
    assert!(out.contains("Rows: 3\n"), "{out}");
}

#[test]
fn overlapping_replacements_are_applied_in_sorted_order() {
    // Go iterates the replacement map in random order, so overlapping
    // replacements are only well defined when the order does not matter —
    // here the Rust order (sorted keys) gives the same result as any order.
    let Some(side) = both(&Case::new("ambiguous").args([
        "sql/ambiguous.sql",
        "{{aaa}}",
        "3",
        "{{aa}}",
        "2",
        "{{a}}",
        "1",
    ])) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert!(stdout_of(&side).contains("|1|2|3|\n"));
}

#[test]
fn sql_error_is_fatal_with_pq_details() {
    let Some(side) = both(&Case::new("bad_sql").args(["sql/bad.sql", "{{id}}", "1"])) else {
        return;
    };
    assert_eq!(side.out.code(), 2);
    let out = stdout_of(&side);
    // the failing query is echoed, then the PqError line
    assert!(
        out.contains("select\n  id, nosuchcolumn\nfrom\n  runq_types\nwhere\n  id = 1\n;\n"),
        "{out}"
    );
    assert!(
        out.contains("PqError: code=42703, name=undefined_column, detail=\n"),
        "{out}"
    );
    assert_eq!(
        error_lines(&side.out.stderr_str()),
        vec!["Error: 'pq: column \"nosuchcolumn\" does not exist'".to_string()]
    );
    let Some(side) = both(&Case::new("syntax_error").args(["sql/syntax_error.sql"])) else {
        return;
    };
    assert_eq!(side.out.code(), 2);
    assert!(stdout_of(&side).contains("PqError: code=42601, name=syntax_error, detail=\n"));
}

#[test]
fn unreachable_server_is_fatal() {
    let Some(side) =
        both(
            &Case::new("noconn")
                .env("PG_PORT", "1")
                .args(["sql/simple.sql", "{{x}}", "1"]),
        )
    else {
        return;
    };
    assert_eq!(side.out.code(), 2);
    let errs = error_lines(&side.out.stderr_str());
    assert_eq!(errs.len(), 1);
    assert!(errs[0].contains("connection refused"), "{errs:?}");
}

// ---------------------------------------------------------------------------
// Multi-statement / DML files (the way devstats scripts use runq)
// ---------------------------------------------------------------------------

#[test]
fn dml_only_file_reports_no_data_and_changes_the_db() {
    let Some(side) = both(
        &Case::new("dml")
            .args([
                "sql/dml.sql",
                "{{ord}}",
                "50",
                "{{path}}",
                "util_sql/fifty.sql",
            ])
            .tables(&["gha_postprocess_scripts", "gha_actors", "gha_countries"]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert!(stdout_of(&side).contains("Metric returned no data\n"));
    assert_eq!(
        rows_of(
            &side,
            "select path from gha_postprocess_scripts where ord = 50"
        ),
        vec![vec!["util_sql/fifty.sql".to_string()]]
    );
    assert_eq!(
        rows_of(&side, "select name from gha_actors where id = 106"),
        vec![vec!["Updated by runq".to_string()]]
    );
    assert_eq!(
        rows_of(
            &side,
            "select count(*) from gha_countries where code = 'fr'"
        ),
        vec![vec!["0".to_string()]]
    );
}

#[test]
fn first_result_set_is_printed_later_statements_still_run() {
    let Some(side) = both(
        &Case::new("dml_then_select")
            .args(["sql/dml_then_select.sql"])
            .tables(&["gha_postprocess_scripts"]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("|100|first.sql"), "{out}");
    assert!(!out.contains("not shown"), "{out}");
    assert!(out.contains("Rows: 3\n"), "{out}");
    // the insert after the select ran too
    assert_eq!(
        rows_of(
            &side,
            "select count(*) from gha_postprocess_scripts where ord in (100, 101)"
        ),
        vec![vec!["2".to_string()]]
    );
}

#[test]
fn statement_after_select_runs() {
    let Some(side) = both(
        &Case::new("select_then_dml")
            .args(["sql/select_then_dml.sql"])
            .tables(&["gha_actors"]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert!(stdout_of(&side).contains("|106|Eve |\n"));
    assert_eq!(
        rows_of(&side, "select name from gha_actors where id = 106"),
        vec![vec!["changed after select".to_string()]]
    );
}

// ---------------------------------------------------------------------------
// Real devstats files
// ---------------------------------------------------------------------------

#[test]
fn real_get_keywords() {
    for (name, kw, n) in [
        ("kw_error", "error", 3),
        ("kw_warning", "warning", 1),
        ("kw_none", "nothing", 0),
    ] {
        let Some(side) = both(
            &Case::new(name)
                .args(["util_sql/get_keywords.sql", "{{msg}}", kw])
                .csv(),
        ) else {
            return;
        };
        assert_eq!(side.out.code(), 0, "{name}");
        let out = stdout_of(&side);
        if n == 0 {
            assert!(out.contains("Metric returned no data\n"), "{out}");
        } else {
            assert!(out.contains(&format!("Rows: {n}\n")), "{out}");
        }
    }
}

#[test]
fn real_remove_dups() {
    let Some(side) = both(
        &Case::new("remove_dups")
            .args([
                "util_sql/remove_dups.sql",
                "{{table}}",
                "gha_issues_pull_requests",
            ])
            .tables(&["gha_issues_pull_requests"]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert!(stdout_of(&side).contains("Metric returned no data\n"));
    assert_eq!(
        rows_of(&side, "select count(*), count(distinct (issue_id, pull_request_id, number, repo_id, repo_name, created_at)) from gha_issues_pull_requests"),
        vec![vec!["4".to_string(), "4".to_string()]]
    );
}

#[test]
fn real_default_postprocess_scripts() {
    let Some(side) = both(
        &Case::new("postprocess")
            .args(["util_sql/default_postprocess_scripts.sql"])
            .tables(&["gha_postprocess_scripts"]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        rows_of(
            &side,
            "select ord::text, path from gha_postprocess_scripts order by ord"
        ),
        vec![
            vec![
                "1".to_string(),
                "util_sql/postprocess_texts.sql".to_string()
            ],
            vec![
                "2".to_string(),
                "util_sql/postprocess_labels.sql".to_string()
            ],
            vec![
                "3".to_string(),
                "util_sql/postprocess_issues_prs.sql".to_string()
            ],
            vec![
                "6".to_string(),
                "util_sql/postprocess_commits.sql".to_string()
            ],
            vec!["9".to_string(), "util_sql/custom.sql".to_string()],
        ]
    );
}

#[test]
fn real_delete_artificial() {
    let Some(side) = both(
        &Case::new("delete_artificial")
            .args(["util_sql/delete_artificial.sql"])
            .tables(&["gha_issues", "gha_payloads"])
            .queries(&[
                "select id, type, actor_id, repo_id, dup_actor_login from gha_events order by id",
                "select event_id, body, actor_login from gha_texts order by 1, 2",
                "select issue_id, event_id, label_id, dup_label_name from gha_issues_labels order by 1, 2, 3",
                "select issue_id, event_id, label_id, label_name from gha_issues_events_labels order by 1, 2, 3",
            ]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        rows_of(
            &side,
            "select count(*) from gha_events where id > 281474976710656"
        ),
        vec![vec!["0".to_string()]]
    );
    assert_eq!(
        rows_of(
            &side,
            "select count(*) from gha_events where id = 281474976710656"
        ),
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        rows_of(
            &side,
            "select count(*) from gha_texts where body like '%artificial%'"
        ),
        vec![vec!["0".to_string()]]
    );
    assert_eq!(
        rows_of(
            &side,
            "select count(*) from gha_texts where body = 'real text'"
        ),
        vec![vec!["1".to_string()]]
    );
}

#[test]
fn real_update_country_names() {
    let Some(side) = both(
        &Case::new("country_names")
            .args(["util_sql/update_country_names.sql"])
            .tables(&["gha_actors"]),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert_eq!(
        rows_of(&side, "select id::text, coalesce(country_name, '<null>') from gha_actors where id in (100, 105, 107, 108) order by id"),
        vec![
            vec!["100".to_string(), "Poland".to_string()],
            vec!["105".to_string(), "Poland".to_string()],
            vec!["107".to_string(), "France".to_string()],
            vec!["108".to_string(), "<null>".to_string()],
        ]
    );
}

#[test]
fn real_count_all_tables_union_unordered() {
    let Some(side) = both(
        &Case::new("count_all")
            .args(["util_sql/count_all_tables.sql"])
            .unordered_rows()
            .csv(),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("|name "), "{out}");
    assert!(out.contains("Rows: "), "{out}");
}

#[test]
fn real_actors_distinct_unordered() {
    let Some(side) = both(
        &Case::new("actors")
            .args(["util_sql/actors.sql"])
            .unordered_rows(),
    ) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("|actor"), "{out}");
}

#[test]
fn real_hist_commenters_metric_with_qr_rnd_and_exclude_bots() {
    let Some(side) = both(&Case::new("hist_commenters").args([
        "metrics/shared/hist_commenters.sql",
        "qr",
        ",2026-01-01,2027-01-01",
        "{{exclude_bots}}",
        "readfile:util_sql/exclude_bots.sql",
    ])) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains("|htop_commenters,All       |alice|2       |\n"),
        "{out}"
    );
    assert!(
        out.contains("|htop_commenters,Kubernetes|alice|2       |\n"),
        "{out}"
    );
    assert!(
        out.contains("|htop_commenters,Docs      |bob  |1       |\n"),
        "{out}"
    );
    assert!(
        out.contains("|htop_commenters,Prometheus|dave |1       |\n"),
        "{out}"
    );
    // eve's 2026 comment is in a repo without a repo group: only in `All`
    assert!(
        out.contains("|htop_commenters,All       |eve  |1       |\n"),
        "{out}"
    );
    assert!(out.contains("Rows: 7\n"), "{out}");
    assert!(!out.contains("k8s-ci-robot"), "{out}");
    assert!(!out.contains("dependabot"), "{out}");
    // period mode with the real usage syntax
    let Some(side) = both(&Case::new("hist_commenters_period").args([
        "metrics/shared/hist_commenters.sql",
        "qr",
        "100 years,,",
        "{{exclude_bots}}",
        "readfile:util_sql/exclude_bots.sql",
    ])) else {
        return;
    };
    assert_eq!(side.out.code(), 0);
    assert!(stdout_of(&side).contains("|htop_commenters,All       |alice|2       |\n"));
}

#[test]
fn real_all_placeholders_missing_params_is_sql_error() {
    // without `{{n}}` etc. the SQL is invalid: both fail the same way
    let Some(side) = both(&Case::new("placeholders_err").args([
        "util_sql/all_placeholders.sql",
        "qr",
        "1 week,,",
    ])) else {
        return;
    };
    assert_eq!(side.out.code(), 2);
    assert!(stdout_of(&side).contains("PqError: code=42601, name=syntax_error"));
}
