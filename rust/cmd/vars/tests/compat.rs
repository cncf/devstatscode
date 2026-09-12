//! Go ⇄ Rust compatibility tests for `vars`.
//!
//! Every case runs the Go binary and the Rust binary with the same
//! environment and working directory (`compat/fixtures/vars/data`, laid out
//! like a `cncf/devstats` checkout: the `all`, `kubernetes` and `prestodb`
//! `vars.yaml`/`sync_vars.yaml` with the `docs/dashboards/*.md` and
//! `partials/*.html` they read are real files, `metrics/testproj/*` and
//! `templates/*` are test-specific) on two identical scratch databases
//! (`dbtest_vars_<name>_go` / `_rs`: the DevStats schema plus
//! `compat/fixtures/vars/seed.sql`) and compares exit code, stdout (`Time:`
//! durations masked), the `Error: '…'` lines of fatal errors and the resulting
//! `gha_vars` table (every column, every row).
//!
//! Needs a PostgreSQL server (`test.sh` finds one; skipped otherwise).

use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};

fn go_bin() -> Option<PathBuf> {
    go_binary("vars")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_vars"))
}

/// `compat/fixtures/vars/data` — the "devstats checkout" the tool runs in.
fn data_dir() -> PathBuf {
    fixture("vars/data")
}

fn structure_sql() -> String {
    fs::read_to_string(fixture("structure/full_structure.sql")).unwrap()
}

fn seed_sql() -> String {
    fs::read_to_string(fixture("vars/seed.sql")).unwrap()
}

/// One compatibility case.
struct Case<'a> {
    name: &'a str,
    /// Environment on top of the `PG_*` connection variables.
    env: Vec<(&'a str, &'a str)>,
    cwd: PathBuf,
    /// Run against a scratch database (otherwise the case brings its own
    /// `PG_*` variables, e.g. an unreachable server).
    db: bool,
    /// Extra runs on the same database, each with its own additional
    /// environment (upsert / idempotency tests).
    more_runs: Vec<Vec<(&'a str, &'a str)>>,
    /// Compare the `Error: '…'` stderr lines of fatal errors (off where the
    /// wording comes from a third-party library: yaml decoding).
    compare_errors: bool,
}

impl<'a> Case<'a> {
    fn new(name: &'a str) -> Self {
        Case {
            name,
            env: vec![
                ("GHA2DB_LOCAL", "1"),
                ("GHA2DB_PROJECT", "testproj"),
                // referenced by `metrics/testproj/vars.yaml` (`$VARS_TEST_*`)
                ("VARS_TEST_HOME", "/home/tester"),
                ("VARS_TEST_EQUALS", "a=b=c"),
            ],
            cwd: data_dir(),
            db: true,
            more_runs: Vec::new(),
            compare_errors: true,
        }
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
    fn project(self, p: &'a str) -> Self {
        self.env("GHA2DB_PROJECT", p)
    }
    fn yaml(self, fn_yaml: &'a str) -> Self {
        self.env("GHA2DB_VARS_FN_YAML", fn_yaml)
    }
    fn no_db(mut self) -> Self {
        self.db = false;
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
        let db = TestDb::fresh(&format!("vars_{}_{}", case.name, suffix))?;
        db.exec(&structure_sql());
        db.exec(&seed_sql());
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
/// masked: the "defined variables" dump of a failed replacement lists the
/// environment, including `PG_DB`).
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

fn normalize_stdout(stdout: &[u8], side: &Side) -> String {
    let text = String::from_utf8_lossy(stdout).into_owned();
    let text = mask_db(&mask_go_durations(&text), side);
    // `GHA2DB_QOUT` echoes the `gha_logs` insert arguments, including
    // `time.Now()` (Go also appends the monotonic reading `m=+0.0123`)
    text.lines()
        .map(|l| {
            if l.starts_with("[1:vars 2:") {
                if let (Some(a), Some(b)) = (l.find(" 3:"), l.find(" 4:")) {
                    if a < b {
                        return format!("{} 3:<now>{}", &l[..a], &l[b..]);
                    }
                }
            }
            l.to_string()
        })
        .map(|l| l + "\n")
        .collect()
}

/// `gha_vars` (all columns, rendered by PostgreSQL as text; NULL → `<nil>`)
/// plus the counts of every table.
fn db_state(db: &TestDb) -> (cpg::Snapshot, Vec<(String, i64)>) {
    let con = db.conn();
    let vars = cpg::snapshot(
        &con,
        "select name, value_i::text, value_f::text, value_s, value_dt::text from gha_vars order by name",
        &[],
    );
    let counts = cpg::table_counts(&con);
    con.close();
    (vars, counts)
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
            assert_eq!(
                normalize_stdout(&g.stdout, &go),
                normalize_stdout(&r.stdout, &rust),
                "stdout{ctx}"
            );
            if case.compare_errors {
                assert_eq!(
                    error_lines(&g.stderr_str(), &go),
                    error_lines(&r.stderr_str(), &rust),
                    "fatal error lines{ctx}"
                );
            }
        }
        if let (Some(gdb), Some(rdb)) = (&go.db, &rust.db) {
            let (gv, gc) = db_state(gdb);
            let (rv, rc) = db_state(rdb);
            assert_eq!(gc, rc, "row counts (case {:?})", case.name);
            assert_eq!(gv.rows, rv.rows, "gha_vars content (case {:?})", case.name);
        }
    }
    Some(rust)
}

/// `gha_vars` of the Rust side as `name → (value_i, value_f, value_s, value_dt)`.
fn vars_of(side: &Side) -> std::collections::BTreeMap<String, [String; 4]> {
    let (snap, _) = db_state(side.db.as_ref().unwrap());
    snap.rows
        .into_iter()
        .map(|r| {
            (
                r[0].clone(),
                [r[1].clone(), r[2].clone(), r[3].clone(), r[4].clone()],
            )
        })
        .collect()
}

/// Expected `gha_vars` row `(value_i, value_f, value_s, value_dt)`; `""` in
/// the numeric/timestamp columns stands for NULL (they cannot hold an empty
/// string), `value_s` is taken literally (`"<nil>"` for NULL).
fn row(i: &str, f: &str, s: &str, dt: &str) -> [String; 4] {
    let null = |v: &str| {
        if v.is_empty() {
            "<nil>".to_string()
        } else {
            v.to_string()
        }
    };
    [null(i), null(f), s.to_string(), null(dt)]
}

/// A `value_s`-only row.
fn s(v: &str) -> [String; 4] {
    row("", "", v, "")
}

fn stdout_of(side: &Side) -> String {
    side.out().stdout_str()
}

fn errors_of(side: &Side) -> Vec<String> {
    error_lines(&side.out().stderr_str(), side)
}

// ---------------------------------------------------------------------------
// The test project: every feature on a deterministic dataset
// ---------------------------------------------------------------------------

#[test]
fn test_project_writes_every_variable() {
    let side = both(&Case::new("testproj")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert_eq!(
        out.matches("Incorrect variable configuration, skipping\n")
            .count(),
        4,
        "{out}"
    );
    assert!(out.ends_with("ms\n") || out.contains("Time: "), "{out}");

    let vars = vars_of(&side);
    // literal values of every type; the pre-existing row keeps its other columns
    assert_eq!(
        vars["full_name"],
        row("7", "1.5", "Test Project", "2000-01-01 00:00:00")
    );
    assert_eq!(
        vars["int_var"],
        row("42", "1.5", "junk", "2000-01-01 00:00:00")
    );
    assert_eq!(vars["negative_int"], row("-17", "", "<nil>", ""));
    assert_eq!(vars["float_var"], row("", "3.5", "<nil>", ""));
    assert_eq!(vars["float_exp"], row("", "0.001", "<nil>", ""));
    assert_eq!(vars["dt_var"], row("", "", "<nil>", "2020-02-29 12:34:56"));
    assert_eq!(vars["dt_iso"], row("", "", "<nil>", "2021-12-31 23:59:59"));
    assert_eq!(vars["quoted_number"], s("007"));
    assert_eq!(vars["yaml_bool_as_string"], s("yes"));
    assert_eq!(
        vars["untouched"],
        row("1", "2", "stays", "1999-12-31 23:59:59")
    );
    assert_eq!(vars["upsert_existing_type_s"], s("replaced"));
    // disabled / incomplete / no_write definitions are not written
    for absent in [
        "disabled_var",
        "disabled_with_command",
        "no_type",
        "no_value_no_command",
        "empty_value",
        "os_hostname",
    ] {
        assert!(!vars.contains_key(absent), "{absent} must not be written");
    }
    // commands: trimming, empty output keeps the literal value, merged stderr
    assert_eq!(vars["trimmed"], s("hello world"));
    assert_eq!(vars["empty_output_keeps_value"], s("default kept"));
    assert_eq!(vars["whitespace_output_keeps_value"], s("default kept too"));
    assert_eq!(vars["empty_output_no_value"], s(""));
    assert_eq!(vars["multiline"], s("line1\nline2\n\nline4"));
    assert_eq!(vars["stderr_merged"], s("out\nerr\nout2"));
    assert_eq!(
        vars["percent_and_quotes"],
        s("100% \"quoted\" 'single' $HOME `tick` \\backslash")
    );
    assert_eq!(
        vars["sql_injection_attempt"],
        s("'; drop table gha_vars; --")
    );
    assert_eq!(
        vars["unicode"],
        s("Zażółć gęślą jaźń 日本語 😀 Test Project")
    );
    assert_eq!(
        vars["with_project_and_datadir"],
        s("project=testproj datadir=./ again=testproj./")
    );
    assert_eq!(
        vars["command_arg0_template"],
        s("script args: 2: arg one | arg two")
    );
    // replacements: $ENV (incl. a value containing '='), :literal, non-template
    // `:from`, variables defined so far, chains through stored replacements
    assert_eq!(
        vars["from_env"],
        s("project=testproj home=/home/tester eq=a=b=c")
    );
    assert_eq!(
        vars["literal_replaces"],
        s("x=literal a y=Test Project z=<literal> w=Test Project c=literal a")
    );
    assert_eq!(
        vars["chain"],
        s("literal a-Test Project-project=testproj home=/home/tester eq=a=b=c-x=literal a y=Test Project z=<literal> w=Test Project c=literal a")
    );
    let host = vars["os_hostname_written"][2].clone();
    assert!(!host.is_empty());
    assert_eq!(vars["write_after_no_write"], s(&format!("host={host}")));
    // queries: `name:column:value:row:col`, NULL → "", non-text columns as
    // their raw text (RFC3339 timestamps like Go's []byte scan), unknown
    // cells stay
    assert_eq!(
        vars["from_sql_query"],
        s("a0: 1|a|first|1.50|2020-01-01T10:00:00Z|true\n\
           a1: 2|a|second|2.00|2020-01-02T10:00:00Z|false\n\
           b0: 3|b||||\n\
           b1: 4|b||0.00|2020-01-04T10:00:00Z|true\n\
           null grp: 6|no group\n\
           unicode grp: Zażółć gęślą jaźń 日本語 😀|12345.67\n\
           by id: first|no group|b|\n\
           out of range: data:grp:a:2:0|data:grp:a:0:6|data:grp:c:0:0\n\
           health: Activity status=Active Releases: Last release description=Helm release v4.3.0 health:series:phealthhelm:76:2\n\
           empty: empty:grp:a:0:0\n\
           prefix: a0 ax")
    );
    // loops before + queries before; loops after replacements; flags off
    assert_eq!(
        vars["loops_before_queries_before"],
        s("<table>\n\n  <tr><td>row 0</td><td>1</td><td>b</td></tr>\n\n  <tr><td>row 1</td><td>2</td><td>b</td></tr>\n\n  <tr><td>row 2</td><td>again:grp:a:2:0</td><td>again:grp:b:2:1</td></tr>\n\n</table>\nodd: [1][5][9]\nempty range: .\nreversed range: .\nmissing end: loop:4:start<kept>\nmissing start: <kept>loop:4:end\nsecond block: (0)(1)(2)\nunknown: again:grp:zzz:0:0 again:grp:a:9:0")
    );
    assert_eq!(vars["loops_after_replaces"], s("count=3\n[0][1][2]"));
    assert_eq!(
        vars["no_loops_no_queries_flags"][2],
        fs::read_to_string(data_dir().join("templates/loops.txt"))
            .unwrap()
            .trim()
    );
    // 31 written + the seeded `untouched`
    assert_eq!(vars.len(), 32, "{:?}", vars.keys().collect::<Vec<_>>());
}

#[test]
fn debug_output_lists_every_variable() {
    let side = both(&Case::new("debug").env("GHA2DB_DEBUG", "1")).unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains("lib.ReadFile('./metrics/testproj/vars.yaml'): ok\n"),
        "{out}"
    );
    assert!(out.contains("Variable Name 'os_hostname', Value '', Type 's', Command [hostname], Replaces [], Queries: [], Loops: [], Disabled: false, Skip: false, NoWrite: true\n"), "{out}");
    assert!(out.contains("Variable Name 'disabled_var', Value 'never written', Type 's', Command [], Replaces [], Queries: [], Loops: [], Disabled: true, Skip: false, NoWrite: false\n"), "{out}");
    assert!(out.contains("Variable Name 'literal_replaces', Value '', Type 's', Command [echo x=[[a]] y=[[b]] z=:lit: w=[[full_name]] c=[[a]]], Replaces [[a :literal a] [b full_name] [:z=:lit: :z=<literal>] [full_name full_name] [: :colon]], Queries: [], Loops: [], Disabled: false, Skip: false, NoWrite: false\n"), "{out}");
    assert!(out.contains("Variable Name 'loops_before_queries_before', Value '', Type 's', Command [cat {{datadir}}templates/loops.txt], Replaces [], Queries: [[again select id, grp from vars_test_data where grp is not null order by id grp]], Loops: [[0 0 3 1] [1 1 10 4] [2 5 5 1] [3 10 0 1]], Disabled: false, Skip: false, NoWrite: false\n"), "{out}");
    assert!(
        out.contains("Name 'trimmed', New Value 'hello world', Type 's'\n"),
        "{out}"
    );
    assert!(
        out.contains("Name 'multiline', New Value 'line1\nline2\n\nline4', Type 's'\n"),
        "{out}"
    );
    assert!(out.contains("Name 'with_project_and_datadir', New Value 'project=testproj datadir=./ again=testproj./', Type 's'\n"), "{out}");
    // literal values have no "New Value" line; the only unwritten variable
    // (`os_hostname`, no_write) reports the skipped write
    assert!(!out.contains("Name 'full_name', New Value"), "{out}");
    assert_eq!(
        out.matches("Skipping postgres vars write\n").count(),
        1,
        "{out}"
    );
}

#[test]
fn skip_pdb_computes_but_never_writes() {
    let side = both(
        &Case::new("skip_pdb")
            .env("GHA2DB_SKIPPDB", "1")
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    // every processed variable (31 written normally + os_hostname) reports the skip
    assert_eq!(
        out.matches("Skipping postgres vars write\n").count(),
        32,
        "{out}"
    );
    assert!(
        out.contains("Name 'chain', New Value 'literal a-Test Project-project=testproj"),
        "{out}"
    );
    let vars = vars_of(&side);
    assert_eq!(vars.len(), 3);
    assert_eq!(vars["full_name"][2], "OLD VALUE");
}

#[test]
fn exclude_vars_skips_listed_names() {
    let side = both(
        &Case::new("exclude")
            .env(
                "GHA2DB_EXCLUDE_VARS",
                "lower_name,int_var,chain,no_such_var,,disabled_var",
            )
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("Variable Name 'lower_name', Value 'testproj', Type 's', Command [], Replaces [], Queries: [], Loops: [], Disabled: false, Skip: true, NoWrite: false\n"), "{out}");
    assert!(out.contains("Variable Name 'chain', Value '', Type 's', Command [echo [[a]]-[[b]]-[[from_env]]-[[literal_replaces]]], Replaces [[a a] [b b] [from_env from_env] [literal_replaces literal_replaces]], Queries: [], Loops: [], Disabled: false, Skip: true, NoWrite: false\n"), "{out}");
    assert!(out.contains("Variable Name 'disabled_var', Value 'never written', Type 's', Command [], Replaces [], Queries: [], Loops: [], Disabled: true, Skip: true, NoWrite: false\n"), "{out}");
    let vars = vars_of(&side);
    // excluded variables keep their seeded rows, others are written
    assert_eq!(
        vars["int_var"],
        row("7", "1.5", "junk", "2000-01-01 00:00:00")
    );
    assert!(!vars.contains_key("lower_name"));
    assert!(!vars.contains_key("chain"));
    assert_eq!(vars["full_name"][2], "Test Project");
    assert_eq!(vars.len(), 30, "{:?}", vars.keys().collect::<Vec<_>>());
}

#[test]
fn excluding_a_referenced_variable_is_fatal() {
    // `literal_replaces` replaces `[[b]]` with the variable `full_name`,
    // which is excluded and therefore undefined: fatal, and the dump of the
    // defined variables includes the environment.
    let side = both(&Case::new("exclude_ref").env("GHA2DB_EXCLUDE_VARS", "full_name")).unwrap();
    assert_eq!(side.out().code(), 2);
    let errs = errors_of(&side);
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(errs[0].starts_with("Error: 'Variable 'literal_replaces' requests replacing 'full_name', but not such variable is defined, defined: map["), "{errs:?}");
    assert!(errs[0].contains("$GHA2DB_PROJECT:testproj"), "{errs:?}");
    assert!(errs[0].contains("$VARS_TEST_EQUALS:a=b=c"), "{errs:?}");
    assert!(errs[0].contains(" a:literal a "), "{errs:?}");
    assert!(errs[0].contains(" lower_name:testproj "), "{errs:?}");
    assert!(errs[0].contains(" os_hostname:"), "{errs:?}");
    assert!(
        errs[0].contains(" from_env:project=testproj home=/home/tester eq=a=b=c "),
        "{errs:?}"
    );
    // variables before the failure were written
    let vars = vars_of(&side);
    assert_eq!(
        vars["from_env"],
        s("project=testproj home=/home/tester eq=a=b=c")
    );
    assert!(!vars.contains_key("literal_replaces"));
}

#[test]
fn only_vars_writes_the_listed_names_only() {
    let side = both(
        &Case::new("only")
            .env("GHA2DB_ONLY_VARS", "full_name,chain,os_hostname,no_such")
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    // everything is still computed (chain depends on earlier, unwritten
    // variables); the 30 unwritten ones report the skip
    assert!(
        out.contains(
            "Name 'from_env', New Value 'project=testproj home=/home/tester eq=a=b=c', Type 's'\n"
        ),
        "{out}"
    );
    assert_eq!(
        out.matches("Skipping postgres vars write\n").count(),
        30,
        "{out}"
    );
    let vars = vars_of(&side);
    assert_eq!(vars["full_name"][2], "Test Project");
    assert!(vars["chain"][2].starts_with("literal a-Test Project-"));
    // no_write wins over only_vars
    assert!(!vars.contains_key("os_hostname"));
    assert!(!vars.contains_key("lower_name"));
    assert_eq!(vars["int_var"][0], "7");
    assert_eq!(vars.len(), 4, "{:?}", vars.keys().collect::<Vec<_>>());
}

#[test]
fn only_and_exclude_combine() {
    let side = both(
        &Case::new("only_exclude")
            .env("GHA2DB_ONLY_VARS", "lower_name,int_var,trimmed")
            .env("GHA2DB_EXCLUDE_VARS", "int_var"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    assert_eq!(vars["lower_name"], s("testproj"));
    assert_eq!(vars["trimmed"], s("hello world"));
    assert_eq!(vars["int_var"][0], "7");
    assert_eq!(vars.len(), 5);
}

#[test]
fn running_twice_is_idempotent_and_updates_in_place() {
    let side = both(
        &Case::new("twice")
            .env("GHA2DB_VARS_YAML", "metrics/testproj/other.yaml")
            .then_run(&[("GHA2DB_VARS_YAML", "")])
            .then_run(&[("GHA2DB_VARS_YAML", "")]),
    )
    .unwrap();
    assert_eq!(side.outs.len(), 3);
    assert!(side.outs.iter().all(|o| o.code() == 0));
    let vars = vars_of(&side);
    assert_eq!(vars["other_yaml"], s("loaded via GHA2DB_VARS_YAML"));
    assert_eq!(vars["full_name"][2], "Test Project");
    assert_eq!(vars.len(), 33);
}

// ---------------------------------------------------------------------------
// Locating the yaml file
// ---------------------------------------------------------------------------

#[test]
fn vars_yaml_env_selects_the_file() {
    let side = both(&Case::new("vars_yaml").env("GHA2DB_VARS_YAML", "metrics/testproj/other.yaml"))
        .unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    assert_eq!(vars["other_yaml"], s("loaded via GHA2DB_VARS_YAML"));
    assert_eq!(vars.len(), 4);
}

#[test]
fn vars_fn_yaml_env_selects_the_file_name() {
    let side = both(
        &Case::new("fn_yaml")
            .yaml("sync_vars.yaml")
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    assert!(stdout_of(&side).contains("lib.ReadFile('./metrics/testproj/sync_vars.yaml'): ok\n"));
    let vars = vars_of(&side);
    assert_eq!(
        vars["sync_only"],
        s("loaded via GHA2DB_VARS_FN_YAML=sync_vars.yaml")
    );
    assert!(!vars.contains_key("sync_not_written"));
    assert_eq!(vars.len(), 4);
}

#[test]
fn datadir_mode_uses_the_absolute_prefix() {
    let dir = format!("{}/", data_dir().to_string_lossy());
    let side = both(
        &Case::new("datadir")
            .no_env("GHA2DB_LOCAL")
            .env("GHA2DB_DATADIR", leak(&dir))
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains(&format!(
            "lib.ReadFile('{dir}metrics/testproj/vars.yaml'): ok\n"
        )),
        "{out}"
    );
    let vars = vars_of(&side);
    assert_eq!(
        vars["with_project_and_datadir"],
        s(&format!(
            "project=testproj datadir={dir} again=testproj{dir}"
        ))
    );
    assert_eq!(
        vars["command_arg0_template"],
        s("script args: 2: arg one | arg two")
    );
    assert_eq!(vars.len(), 32);
}

#[test]
fn default_datadir_is_etc_gha2db() {
    let side = both(&Case::new("etc").no_env("GHA2DB_LOCAL")).unwrap();
    assert_eq!(side.out().code(), 2);
    let out = stdout_of(&side);
    assert!(out.contains("lib.ReadFile('/etc/gha2db/metrics/shared/vars.yaml'): error: open /etc/gha2db/metrics/shared/vars.yaml: no such file or directory\n"), "{out}");
    assert_eq!(
        errors_of(&side),
        vec![
            "Error: 'open /etc/gha2db/metrics/shared/vars.yaml: no such file or directory'"
                .to_string()
        ]
    );
    assert_eq!(vars_of(&side).len(), 3);
}

#[test]
fn no_project_reads_metrics_vars_yaml() {
    let side = both(&Case::new("noproj").no_env("GHA2DB_PROJECT")).unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    assert_eq!(vars["no_project"], s("project-less run"));
    assert_eq!(vars.len(), 4);
}

#[test]
fn missing_project_yaml_falls_back_to_shared() {
    let side = both(
        &Case::new("fallback")
            .project("fallbackproj")
            .env("GHA2DB_DEBUG", "1"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(
        out.contains("lib.ReadFile('./metrics/shared/vars.yaml'): ok\n"),
        "{out}"
    );
    assert!(!out.contains("fallbackproj/vars.yaml'): ok"), "{out}");
    let vars = vars_of(&side);
    assert_eq!(vars["shared_fallback"], s("from metrics/shared/vars.yaml"));
    assert_eq!(vars.len(), 4);
}

#[test]
fn missing_yaml_everywhere_is_fatal() {
    let side = both(&Case::new("missing").yaml("nope.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert!(stdout_of(&side).contains("lib.ReadFile('./metrics/shared/nope.yaml'): error: open ./metrics/shared/nope.yaml: no such file or directory\n"));
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'open ./metrics/shared/nope.yaml: no such file or directory'".to_string()]
    );
}

#[test]
fn missing_yaml_without_project_is_fatal_directly() {
    let side = both(
        &Case::new("missing_noproj")
            .no_env("GHA2DB_PROJECT")
            .yaml("nope.yaml"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 2);
    assert!(!stdout_of(&side).contains("lib.ReadFile"));
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'open ./metrics/nope.yaml: no such file or directory'".to_string()]
    );
}

#[test]
fn empty_and_var_less_yamls_do_nothing() {
    for (name, yaml) in [("empty", "empty.yaml"), ("novars", "no_vars_key.yaml")] {
        let side = both(&Case::new(name).yaml(yaml)).unwrap();
        assert_eq!(side.out().code(), 0);
        assert_eq!(vars_of(&side).len(), 3);
        let out = stdout_of(&side);
        assert_eq!(out.lines().count(), 2, "{out}");
    }
}

#[test]
fn duplicate_keys_and_extra_documents_decode_like_yaml_v2() {
    // yaml.v2: the last value of a repeated key wins (a repeated `vars:` list
    // replaces the first one), only the first document of a stream is decoded.
    let side = both(&Case::new("dup_keys").yaml("dup_keys.yaml")).unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    assert_eq!(vars["dup_scalar"], s("last value wins"));
    assert_eq!(vars["dup_bool"], s("written"));
    assert_eq!(vars["dup_type"], row("42", "", "<nil>", ""));
    assert_eq!(vars["dup_second_vars_key"], s("from the second vars key"));
    assert!(!vars.contains_key("dup_disabled"), "{vars:?}");
    assert!(!vars.contains_key("dup_top_vars_first_list"), "{vars:?}");
    assert!(!vars.contains_key("second_document"), "{vars:?}");
    assert_eq!(vars.len(), 3 + 4);
}

#[test]
fn malformed_yaml_is_fatal() {
    // the error text comes from the yaml library (differs by design)
    for (name, yaml) in [
        ("malformed", "malformed.yaml"),
        ("shape", "wrong_shape.yaml"),
    ] {
        let side = both(&Case::new(name).yaml(yaml).code_only_errors()).unwrap();
        assert_eq!(side.out().code(), 2);
        let errs = errors_of(&side);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].starts_with("Error: 'yaml: "), "{errs:?}");
        assert_eq!(vars_of(&side).len(), 3);
    }
}

// ---------------------------------------------------------------------------
// Fatal paths of the definitions
// ---------------------------------------------------------------------------

#[test]
fn replacement_with_wrong_arity_is_fatal() {
    let side = both(&Case::new("repl_arity").yaml("fail_replace_arity.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec![
            "Error: 'Replacement definition should be array with 2 elements, got: [a b c]'"
                .to_string()
        ]
    );
    // the variable before the failure was written
    let vars = vars_of(&side);
    assert_eq!(vars["ok_first"], s("written before the failure"));
    assert!(!vars.contains_key("bad_replaces"));
}

#[test]
fn undefined_replacement_variable_is_fatal() {
    let side = both(&Case::new("undef").yaml("fail_undefined_var.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    let errs = errors_of(&side);
    assert_eq!(errs.len(), 1);
    assert!(errs[0].starts_with("Error: 'Variable 'bad_ref' requests replacing 'no_such_variable', but not such variable is defined, defined: map[$"), "{errs:?}");
    assert!(errs[0].contains("$PG_DB:<db> "), "{errs:?}");
}

#[test]
fn duplicate_query_names_are_fatal() {
    for (name, yaml) in [
        ("qdup", "fail_query_dup.yaml"),
        ("qdup2", "fail_query_dup_across_vars.yaml"),
    ] {
        let side = both(&Case::new(name).yaml(yaml)).unwrap();
        assert_eq!(side.out().code(), 2);
        assert_eq!(
            errors_of(&side),
            vec!["Error: 'query 'q' already defined'".to_string()]
        );
    }
}

#[test]
fn unknown_query_column_is_fatal() {
    let side = both(&Case::new("qcol").yaml("fail_query_column.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'column 'c' not found in query results: [a b]'".to_string()]
    );
}

#[test]
fn query_sql_error_is_fatal() {
    let side = both(&Case::new("qsql").yaml("fail_query_sql.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    let errs = errors_of(&side);
    assert_eq!(
        errs,
        vec!["Error: 'pq: syntax error at or near \"selectx\"'".to_string()]
    );
    assert!(stdout_of(&side).contains("selectx 1 as a"));
}

#[test]
fn malformed_query_and_loop_definitions_are_fatal() {
    let side = both(&Case::new("qarity").yaml("fail_query_arity.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'Query definition should be array with at least 2 elements [name, sql, columns...], got: [only_name]'".to_string()]
    );
    let side = both(&Case::new("larity").yaml("fail_loop_arity.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'Loop definition should be array with 4 elements [n, from, to, inc], got: [0 0 3]'".to_string()]
    );
    let side = both(&Case::new("linc").yaml("fail_loop_inc.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'Loop increment must be positive, got: [0 0 3 0]'".to_string()]
    );
}

#[test]
fn failing_command_is_fatal() {
    let side = both(&Case::new("cmd_exit").yaml("fail_command_exit.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    let out = stdout_of(&side);
    assert!(
        out.contains("Failed command: sh [-c echo partial output; echo on stderr 1>&2; exit 3]\n"),
        "{out}"
    );
    // the (combined) output of the failed command is not printed
    assert!(
        !out.contains("partial output\n") && !out.contains("on stderr\n"),
        "{out}"
    );
    assert_eq!(errors_of(&side), vec!["Error: 'exit status 3'".to_string()]);
    assert_eq!(vars_of(&side)["ok_first"], s("written before the failure"));
}

#[test]
fn missing_command_is_fatal() {
    let side = both(&Case::new("cmd_missing").yaml("fail_command_missing.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    // the templates are expanded before the command runs
    assert!(stdout_of(&side).contains("Failed command: no-such-binary-xyz [./ testproj]\n"));
    assert_eq!(
        errors_of(&side),
        vec![
            "Error: 'exec: \"no-such-binary-xyz\": executable file not found in $PATH'".to_string()
        ]
    );
    let side = both(&Case::new("cmd_path").yaml("fail_command_path.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert!(stdout_of(&side).contains("Failed command: ./templates/no_such_script.sh []\n"));
    assert_eq!(
        errors_of(&side),
        vec![
            "Error: 'fork/exec ./templates/no_such_script.sh: no such file or directory'"
                .to_string()
        ]
    );
}

#[test]
fn values_that_do_not_fit_the_column_type_are_fatal() {
    let side = both(&Case::new("type_i").yaml("fail_type_i.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'pq: invalid input syntax for type bigint: \"abc\"'".to_string()]
    );
    let side = both(&Case::new("type_dt").yaml("fail_type_dt.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'pq: invalid input syntax for type timestamp: \"yesterday-ish\"'".to_string()]
    );
    let side = both(&Case::new("type_x").yaml("fail_type_unknown.yaml")).unwrap();
    assert_eq!(side.out().code(), 2);
    assert_eq!(
        errors_of(&side),
        vec!["Error: 'pq: column \"value_x\" of relation \"gha_vars\" does not exist'".to_string()]
    );
    assert!(stdout_of(&side).contains("insert into gha_vars(name, value_x) values($1, $2) on conflict(name) do update set value_x = $3 where gha_vars.name = $4"));
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

#[test]
fn qout_echoes_every_statement() {
    let side = both(
        &Case::new("qout")
            .env("GHA2DB_QOUT", "1")
            .yaml("sync_vars.yaml"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let out = stdout_of(&side);
    assert!(out.contains("insert into gha_vars(name, value_s) values($1, $2) on conflict(name) do update set value_s = $3 where gha_vars.name = $4\n[1:sync_only 2:loaded via GHA2DB_VARS_FN_YAML=sync_vars.yaml 3:loaded via GHA2DB_VARS_FN_YAML=sync_vars.yaml 4:sync_only ]\n"), "{out}");
    assert!(!out.contains("4:sync_not_written"), "{out}");
}

// ---------------------------------------------------------------------------
// Real projects (files from cncf/devstats)
// ---------------------------------------------------------------------------

#[test]
fn real_all_project_vars() {
    let side = both(&Case::new("all").project("all")).unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    assert_eq!(vars["full_name"][2], "All CNCF");
    assert_eq!(vars["lower_name"], s("all"));
    let host = vars["os_hostname"][2].clone();
    assert!(!host.is_empty());
    // `[[full_name]]`, `$GHA2DB_PROJECT` and the chained
    // `url_prefix → proj_name` replacement
    let lu = &vars["last_updated_docs_html"][2];
    assert!(lu.contains("All CNCF last updated dashboard"), "{lu}");
    assert!(
        lu.contains("/grafana/dashboards/all/last-updated.json"),
        "{lu}"
    );
    assert!(!lu.contains("[["), "{lu}");
    let dd = &vars["dashboards_docs_html"][2];
    assert!(dd.contains(&format!("https://all.{host}\"")), "{dd}");
    assert!(dd.contains(&format!("https://{host}/backups")), "{dd}");
    assert!(!dd.contains("[["), "{dd}");
    // `partials/projects.html`: `[[hostname]]` filled (978 times), then the
    // `:testsrv=…` / `:prodsrv=…` literal replacements comment out the
    // server-specific rows
    let pp = &vars["projects_partial_html"][2];
    assert_eq!(
        pp.matches(&format!("https://envoy.{host}\"")).count(),
        4,
        "{pp}"
    );
    assert!(pp.contains(&format!("<!-- {host} \n")), "{pp}");
    assert!(pp.contains(&format!("{host} -->\n")), "{pp}");
    assert!(
        !pp.contains("testsrv") && !pp.contains("prodsrv") && !pp.contains("[["),
        "{pp}"
    );
    assert_eq!(vars.len(), 50 + 2, "{:?}", vars.keys().collect::<Vec<_>>());
}

#[test]
fn real_all_project_sync_vars_expand_projects_health() {
    let side = both(&Case::new("all_sync").project("all").yaml("sync_vars.yaml")).unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    // os_hostname is no_write here
    assert_eq!(vars.len(), 4, "{:?}", vars.keys().collect::<Vec<_>>());
    let html = &vars["projects_health_partial_html"][2];
    // loops expanded: 253 `<col>` (+2 literal ones inside the commented-out
    // test-server row), 76 metric rows, no markers left
    assert_eq!(html.matches("<col>").count(), 255, "{}", html.len());
    assert!(!html.contains("loop:"), "loop markers left");
    // query cells filled from the real rows and the synthetic ones
    assert!(html.contains("<td style=\"white-space:nowrap;text-align:left;\" class=\"cncf-bb cncf-bl cncf-br\">Activity status</td>"), "{}", &html[..3000]);
    assert!(html.contains("<td class=\"cncf-bb cncf-bl\">Active</td>"));
    assert!(html.contains(">Helm release v4.3.0<"));
    assert!(html.contains(">test phealthlinkerd 1<"));
    assert!(html.contains(">test phealthlinkerd 76<"));
    // `:>Up<` / `:>Down<` literal replacements run after the queries
    assert!(
        html.contains(">⇧<")
            && html.contains(">⇩<")
            && !html.contains(">Up<")
            && !html.contains(">Down<")
    );
    // `phealthcncf` has 73 rows only: the last three cells stay unreplaced
    assert!(!html.contains("metrics:series:phealthkubernetes:"));
    assert!(!html.contains("metrics:series:phealthcncf:72:2"));
    assert!(html.contains("metrics:series:phealthcncf:73:2"));
    assert!(html.contains("metrics:series:phealthcncf:75:2"));
    // `[[hostname]]` filled and the test-server row commented out
    let host = {
        let out = std::process::Command::new("hostname").output().unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    assert!(
        html.contains(&format!("<!-- {host} <col><col> {host} -->")),
        "{}",
        &html[..3000]
    );
    assert!(html.len() > 900_000, "{}", html.len());
}

#[test]
fn real_all_project_excluding_the_health_partial_writes_nothing() {
    // `all/psql.sh`: GHA2DB_EXCLUDE_VARS="projects_health_partial_html"
    let side = both(
        &Case::new("all_excl")
            .project("all")
            .yaml("sync_vars.yaml")
            .env("GHA2DB_EXCLUDE_VARS", "projects_health_partial_html"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    assert_eq!(vars_of(&side).len(), 3);
}

#[test]
fn real_kubernetes_project_vars() {
    let side = both(&Case::new("k8s").project("kubernetes")).unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    assert_eq!(vars["full_name"][2], "Kubernetes");
    let host = vars["os_hostname"][2].clone();
    // `url_prefix → ':k8s'` literal, `[[hostname]]` from the command output
    let dd = &vars["dashboards_docs_html"][2];
    assert!(dd.contains(&format!("https://k8s.{host}\"")), "{dd}");
    assert!(dd.contains("Kubernetes"), "{dd}");
    assert!(!dd.contains("[["), "{dd}");
    let lu = &vars["last_updated_docs_html"][2];
    assert!(
        lu.contains("/grafana/dashboards/kubernetes/last-updated.json"),
        "{lu}"
    );
    assert_eq!(vars.len(), 51 + 2, "{:?}", vars.keys().collect::<Vec<_>>());
}

#[test]
fn real_kubernetes_sync_vars_is_empty() {
    let side = both(
        &Case::new("k8s_sync")
            .project("kubernetes")
            .yaml("sync_vars.yaml"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    assert_eq!(vars_of(&side).len(), 3);
}

#[test]
fn real_prestodb_sync_vars() {
    let side = both(
        &Case::new("presto")
            .project("prestodb")
            .yaml("sync_vars.yaml"),
    )
    .unwrap();
    assert_eq!(side.out().code(), 0);
    let vars = vars_of(&side);
    let html = &vars["projects_health_partial_html"][2];
    assert!(!html.contains("loop:"));
    // 75 rows of the (synthetic) `phealthall` series
    assert!(html.contains(">test phealthall 1<"), "{html}");
    assert!(
        html.contains(">test phealthall 75<") && !html.contains(">test phealthall 76<"),
        "{html}"
    );
    assert!(html.contains(">Activity status<"), "{html}");
    assert!(!html.contains(">Up<") && !html.contains(">Down<"));
    assert_eq!(vars.len(), 4);
}
