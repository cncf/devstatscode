//! Go ⇄ Rust compatibility tests for `import_affs`.
//!
//! Every case gets its own scratch database per side
//! (`dbtest_import_affs_<case>_<go|rs>`, the six affiliation tables from
//! `compat/fixtures/import_affs/schema.sql`) and a scratch directory holding
//! `github_users.json` / `companies.yaml` / `hide/hide.csv`; the binaries run
//! there with `GHA2DB_LOCAL=1` unless a case says otherwise. A case is a list
//! of steps — runs (each compared), SQL applied between runs (the
//! `util_sh/test_affs.sh` two-phase correlation scenario) and JSON rewrites.
//!
//! Compared per run: exit code, stdout (the `Time:` value masked; the
//! `Mapped to` / `Used mapping` / `Non-acquired` summary and the
//! `gone too deep` lines as a sorted multiset — Go prints them in map
//! order; everything sorted for multi-threaded runs), the `Error: '…'`
//! stderr lines (or only their count where the wording legitimately
//! differs) and afterwards every table (`gha_imported_shas` without its
//! `now()` column).
//!
//! Logins with several names / several equally long affiliation
//! definitions in the JSON get the smallest one on both sides (bug 49: Go
//! used to pick a random map key, so names and affiliations changed on every
//! import) — see [`ties_are_broken_deterministically`].
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise). The unit tests of the binary cover the pure helpers.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{fixture, go_binary, run, rust_binary, Invocation, Outcome};
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("import_affs")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_import_affs"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";

/// Every table the tool writes.
const TABLES: &[&str] = &[
    "gha_actors",
    "gha_actors_affiliations",
    "gha_actors_emails",
    "gha_actors_names",
    "gha_companies",
    "gha_imported_shas",
];

/// `hide/hide.csv` hiding the login `bob` (its SHA-1).
const HIDE_BOB: &str = "sha1\n48181acd22b3edaebc8a447868a7df7ce629920a\n";

/// Correlation SQL of `devstats/util_sh/test_affs.sh`, applied between the
/// two imports of `test_affs.json`.
const CORRELATION_SQL: &[&str] = &[
    "update gha_actors set id = (select id from gha_actors where login = 'lukaszgryglicki') where login = 'lgryglicki'",
    "update gha_actors set id = (select id from gha_actors where login = 'other2') where login = 'other'",
    "update gha_actors set id = (select id from gha_actors where login = 'aother') where login = 'aother2'",
    "insert into gha_actors(id, login, name, country_id, sex, sex_prob, tz, country_name, age) select -id/112, login, name, country_id, sex, sex_prob, tz, country_name, age from gha_actors where login = 'lukaszgryglicki'",
    "insert into gha_actors(id, login, name, country_id, sex, sex_prob, tz, country_name, age) select -id/997, login, name, country_id, sex, sex_prob, tz, country_name, age from gha_actors where login = 'lgryglicki'",
    "insert into gha_actors(id, login, name, country_id, sex, sex_prob, tz, country_name, age) select id, 'other3', name, country_id, sex, sex_prob, tz, country_name, age from gha_actors where login = 'other2'",
    "insert into gha_actors(id, login, name, country_id, sex, sex_prob, tz, country_name, age) select id, 'aother3', name, country_id, sex, sex_prob, tz, country_name, age from gha_actors where login = 'aother'",
    "update gha_actors set id = 1982 where lower(login) like 'src%'",
    "delete from gha_actors_affiliations",
    "delete from gha_companies",
    "delete from gha_actors_emails",
    "delete from gha_actors_names",
];

/// Where the users JSON of a case comes from.
#[derive(Clone, Copy)]
enum Json {
    /// `compat/fixtures/import_affs/<name>`.
    Fixture(&'static str),
    /// Literal content.
    Inline(&'static str),
    /// No file at all.
    Missing,
}

/// Where the acquisitions YAML of a case comes from.
#[derive(Clone, Copy)]
enum Yaml {
    /// The real `devstats/companies.yaml`.
    Fixture,
    /// Literal content.
    Inline(&'static str),
    /// No file at all.
    Missing,
}

/// One step of a case.
#[derive(Clone)]
enum Step {
    /// Run both binaries with extra environment and compare.
    Run(Vec<(&'static str, &'static str)>),
    /// Execute SQL on the side's database.
    Sql(&'static str),
    /// Replace the users JSON.
    Json(&'static str),
}

struct Case {
    name: &'static str,
    json: Json,
    /// File name of the users JSON in the scratch directory.
    json_name: &'static str,
    yaml: Yaml,
    /// File name of the acquisitions YAML in the scratch directory.
    yaml_name: &'static str,
    /// Content of `hide/hide.csv` in the scratch directory.
    hide: Option<&'static str>,
    /// SQL executed on the fresh database before the first run.
    seed: Vec<&'static str>,
    /// Command line arguments (`{dir}` expands to the scratch directory).
    args: Vec<&'static str>,
    /// Environment of every run (`{dir}` expands to the scratch directory).
    env: Vec<(&'static str, &'static str)>,
    /// `GHA2DB_LOCAL=1` (files in the current directory) — off for the
    /// `GHA2DB_DATADIR` case.
    local: bool,
    steps: Vec<Step>,
    /// Compare stdout as a sorted multiset (concurrent workers).
    sorted: bool,
    /// Compare the `Error: '…'` lines (off when their wording legitimately
    /// differs).
    compare_errors: bool,
}

impl Case {
    fn new(name: &'static str) -> Self {
        Case {
            name,
            json: Json::Fixture("probe.json"),
            json_name: "github_users.json",
            yaml: Yaml::Fixture,
            yaml_name: "companies.yaml",
            hide: None,
            seed: Vec::new(),
            args: Vec::new(),
            env: vec![("GHA2DB_NCPUS", "1")],
            local: true,
            steps: vec![Step::Run(Vec::new())],
            sorted: false,
            compare_errors: true,
        }
    }
    fn json(mut self, j: Json) -> Self {
        self.json = j;
        self
    }
    fn json_name(mut self, n: &'static str) -> Self {
        self.json_name = n;
        self
    }
    fn yaml(mut self, y: Yaml) -> Self {
        self.yaml = y;
        self
    }
    fn yaml_name(mut self, n: &'static str) -> Self {
        self.yaml_name = n;
        self
    }
    fn hide(mut self, csv: &'static str) -> Self {
        self.hide = Some(csv);
        self
    }
    fn seed(mut self, sql: &[&'static str]) -> Self {
        self.seed = sql.to_vec();
        self
    }
    fn arg(mut self, a: &'static str) -> Self {
        self.args.push(a);
        self
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
    fn steps(mut self, steps: Vec<Step>) -> Self {
        self.steps = steps;
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
}

struct Side {
    db: TestDb,
    _dir: TempDir,
    /// The scratch directory path (masked as `<dir>` in stdout).
    dir_str: String,
    outs: Vec<Outcome>,
}

/// Lines Go prints in map iteration order.
fn unordered_line(l: &str) -> bool {
    l.starts_with("Mapped to '")
        || l.starts_with("Used mapping '")
        || l.starts_with("Non-acquired companies:")
        || l.starts_with("Error (non fatal): gone too deep:")
}

impl Side {
    /// stdout of run `i` split into the ordered lines and the sorted
    /// multiset of the order-free lines (all of them when `sorted`), with the
    /// `Time:` value masked.
    fn stdout(&self, i: usize, sorted: bool) -> (Vec<String>, Vec<String>) {
        let mut ordered = Vec::new();
        let mut unordered = Vec::new();
        for l in self.outs[i].stdout_str().lines() {
            let l = l.replace(&self.dir_str, "<dir>");
            let l = if l.starts_with("Time: ") {
                "Time: <masked>".to_string()
            } else {
                l
            };
            if sorted || unordered_line(&l) {
                unordered.push(l);
            } else {
                ordered.push(l);
            }
        }
        unordered.sort();
        (ordered, unordered)
    }
    /// All stdout lines of run `i` (`Time:` masked).
    fn lines(&self, i: usize) -> Vec<String> {
        let (mut o, u) = self.stdout(i, false);
        o.extend(u);
        o
    }
    fn has_line(&self, i: usize, line: &str) -> bool {
        self.lines(i).iter().any(|l| l == line)
    }
    /// Assert run `i` printed `line` (listing all lines otherwise).
    fn expect_line(&self, i: usize, line: &str) {
        let lines = self.lines(i);
        assert!(
            lines.iter().any(|l| l == line),
            "missing {line:?} in run #{i}: {lines:#?}"
        );
    }
    /// Like [`Self::expect_line`] on the raw stdout (no `Time:` masking).
    fn expect_raw_line(&self, i: usize, line: &str) {
        let out = self.outs[i].stdout_str();
        let lines: Vec<&str> = out.lines().collect();
        assert!(
            lines.contains(&line),
            "missing {line:?} in run #{i}: {lines:#?}"
        );
    }
    fn stderr_lines(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stderr_str()
            .lines()
            .filter(|l| l.starts_with("Error: '") || l.starts_with("PqError: "))
            .map(str::to_string)
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
    /// Every table (rows ordered by all columns), `gha_imported_shas.dt`
    /// (`now()`) dropped.
    fn data(&self) -> BTreeMap<String, Vec<Vec<String>>> {
        let con = self.db.conn();
        let mut res = BTreeMap::new();
        for &t in TABLES {
            let snap = if t == "gha_imported_shas" {
                cpg::snapshot(&con, "select sha from gha_imported_shas order by 1", &[])
            } else {
                cpg::table_data(&con, t)
            };
            res.insert(t.to_string(), snap.rows);
        }
        con.close();
        res
    }
    fn count(&self, table: &str) -> i64 {
        let con = self.db.conn();
        let n = cpg::snapshot(&con, &format!("select count(*) from {table}"), &[]).rows[0][0]
            .parse()
            .unwrap();
        con.close();
        n
    }
    /// One column of `table` (`<null>` for NULL), ordered by `order`.
    fn column(&self, table: &str, column: &str, order: &str) -> Vec<String> {
        let con = self.db.conn();
        let snap = cpg::snapshot(
            &con,
            &format!("select coalesce({column}::text, '<null>') from {table} order by {order}"),
            &[],
        );
        con.close();
        snap.column(0)
    }
    fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let con = self.db.conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows
    }
}

fn write_json(dir: &Path, case: &Case, json: Json) -> Option<Vec<u8>> {
    let content = match json {
        Json::Fixture(name) => fs::read(fixture(&format!("import_affs/{name}"))).unwrap(),
        Json::Inline(s) => s.as_bytes().to_vec(),
        Json::Missing => return None,
    };
    fs::write(dir.join(case.json_name), &content).unwrap();
    Some(content)
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let db = TestDb::fresh(&format!("import_affs_{}_{}", case.name, suffix))?;
    db.exec(&fs::read_to_string(fixture("import_affs/schema.sql")).unwrap());
    for sql in &case.seed {
        db.exec(sql);
    }
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_import_affs_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    let dir_str = dir.path().to_str().unwrap().to_string();
    write_json(dir.path(), case, case.json);
    match case.yaml {
        Yaml::Fixture => {
            fs::copy(
                fixture("import_affs/companies.yaml"),
                dir.path().join(case.yaml_name),
            )
            .unwrap();
        }
        Yaml::Inline(s) => fs::write(dir.path().join(case.yaml_name), s).unwrap(),
        Yaml::Missing => {}
    }
    if let Some(csv) = case.hide {
        fs::create_dir_all(dir.path().join("hide")).unwrap();
        fs::write(dir.path().join("hide/hide.csv"), csv).unwrap();
    }
    let expand = |s: &str| s.replace("{dir}", &dir_str);
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
        match step {
            Step::Run(extra) => {
                let mut run_env = env.clone();
                for (k, v) in extra {
                    run_env.retain(|(key, _)| key != k);
                    run_env.push((k.to_string(), expand(v)));
                }
                let mut inv = Invocation::new().cwd(dir.path().to_path_buf());
                for (k, v) in &run_env {
                    inv = inv.env(leak(k), leak(v));
                }
                for a in &case.args {
                    inv = inv.arg(expand(a));
                }
                outs.push(run(bin, &inv));
            }
            Step::Sql(sql) => db.exec(sql),
            Step::Json(content) => {
                write_json(dir.path(), case, Json::Inline(content));
            }
        }
    }
    Some(Side {
        db,
        _dir: dir,
        dir_str,
        outs,
    })
}

/// Run both binaries through the case's steps and compare everything;
/// returns the Rust side for further assertions (`None` when the DB tests
/// are skipped).
fn both(case: &Case) -> Option<Side> {
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

/// The two users of `probe.json`: `Alice` (all data) and `bob` (nothing).
const PROBE_LINES: &[&str] = &[
    "Processing 2 JSON entries",
    "Processing non-empty: 1 name lists, 1 email lists, 1 affiliations lists, 2 objects",
    "Empty/Not found: names: 1, emails: 1, affiliations: 1",
    "Added actors: 2, updated actors: 0, empty names: 1, non-unique names: 0, non-changed: 0",
    "0 new logins added by correlations, copied affiliations: 0 (0 different priority)",
    "Added up to 1 actors emails",
    "Added up to 1 actors names",
    "Affiliations unique: 1, non-unique: 0, with multiple priorities: 0, all user-company connections: 2",
    "Processed 2 companies",
    "Affiliations added up to: 2",
    // every company is mapped once when inserted (regexp) and once more
    // when the affiliations are written (cache)
    "Mapped to 'Apprenda Inc.': checked regexp: 1, cache hit: 1",
    "Non-acquired companies: checked all regexp: 1, cache hit: 1",
    "Used mapping 'Kismatic' --> 'Apprenda Inc.'",
];

// ---------------------------------------------------------------------------
// Basic imports
// ---------------------------------------------------------------------------

#[test]
fn probe_single_threaded() {
    let Some(rs) = both(&Case::new("probe_st")) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    let lines = rs.lines(0);
    assert_eq!(lines[0], BANNER);
    assert_eq!(lines[1], "Importing ./github_users.json");
    rs.expect_line(0, "Processing using ST version");
    for l in PROBE_LINES {
        rs.expect_line(0, l);
    }
    assert!(rs.outs[0]
        .stdout_str()
        .lines()
        .last()
        .unwrap()
        .starts_with("Time: "));
    assert_eq!(
        rs.column("gha_actors", "login", "login"),
        vec!["alice", "bob"]
    );
    assert_eq!(
        rs.query(
            "select login, name, country_id, sex, sex_prob, tz, age from gha_actors order by login"
        ),
        vec![
            vec!["alice", "Alice A", "pl", "f", "0.9", "Europe/Warsaw", "30"],
            vec!["bob", "", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"],
        ]
    );
    // Europe/Warsaw is UTC+1 or UTC+2 depending on the season
    let off = rs.column("gha_actors", "tz_offset", "login");
    assert!(off[0] == "60" || off[0] == "120", "{off:?}");
    assert_eq!(off[1], "<null>");
    assert_eq!(
        rs.query("select email from gha_actors_emails"),
        vec![vec!["alice@example.com"]]
    );
    assert_eq!(
        rs.query("select name from gha_actors_names"),
        vec![vec!["Alice A"]]
    );
    // both the original and the mapped company names are stored
    assert_eq!(
        rs.column("gha_companies", "name", "name"),
        vec!["ACME", "Apprenda Inc.", "Kismatic"]
    );
    assert_eq!(
        rs.query(
            "select company_name, original_company_name, dt_from, dt_to, source from gha_actors_affiliations order by dt_from"
        ),
        vec![
            vec![
                "Apprenda Inc.",
                "Kismatic",
                "1900-01-01T00:00:00Z",
                "2018-01-01T00:00:00Z",
                "user"
            ],
            vec![
                "ACME",
                "ACME",
                "2018-01-01T00:00:00Z",
                "2100-01-01T00:00:00Z",
                "user"
            ],
        ]
    );
    // No SHA bookkeeping without GHA2DB_CHECK_IMPORTED_SHA
    assert_eq!(rs.count("gha_imported_shas"), 0);
}

#[test]
fn probe_multi_threaded() {
    let Some(rs) = both(&Case::new("probe_mt").threads("3")) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Processing using MT3 version");
    rs.expect_line(0, "Final threads join");
    for l in PROBE_LINES {
        rs.expect_line(0, l);
    }
}

#[test]
fn probe_more_threads_than_users() {
    let Some(rs) = both(&Case::new("probe_mt8").threads("8")) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Processing using MT8 version");
    assert_eq!(rs.count("gha_actors_affiliations"), 2);
}

/// `GHA2DB_ST=1` is the other way to get the single-threaded version.
#[test]
fn st_env() {
    let case = Case::new("st_env")
        .env("GHA2DB_NCPUS", "")
        .env("GHA2DB_ST", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    rs.expect_line(0, "Processing using ST version");
}

#[test]
fn test_affs_single_threaded() {
    let case = Case::new("affs_st").json(Json::Fixture("test_affs.json"));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    let expected = [
        "Processing 26 JSON entries",
        "Processing non-empty: 5 name lists, 3 email lists, 14 affiliations lists, 19 objects",
        "Empty/Not found: names: 16, emails: 20, affiliations: 9",
        "Added actors: 19, updated actors: 0, empty names: 14, non-unique names: 4, non-changed: 0",
        "0 new logins added by correlations, copied affiliations: 0 (0 different priority)",
        "Added up to 5 actors emails",
        "Added up to 10 actors names",
        "Affiliations unique: 13, non-unique: 1, with multiple priorities: 1, all user-company connections: 31",
        "Processed 18 companies",
        "Affiliations added up to: 31",
        "Non-acquired companies: checked all regexp: 18, cache hit: 31",
    ];
    let raw: Vec<String> = rs.outs[0]
        .stdout_str()
        .lines()
        .map(str::to_string)
        .collect();
    for l in expected {
        assert!(raw.iter().any(|x| x == l), "missing {l:?} in {raw:#?}");
    }
    assert!(!raw.iter().any(|l| l.starts_with("Mapped to")));
    assert_eq!(rs.count("gha_actors"), 19);
    // Case variants of a login collapse into one (lower-cased) actor
    assert_eq!(
        rs.query("select count(*) from gha_actors where login <> lower(login)"),
        vec![vec!["0"]]
    );
    // The smallest of several names is the pick (both sides, bug 49)
    assert_eq!(
        rs.query("select name from gha_actors where login = 'lukaszgryglicki'"),
        vec![vec!["Lukasz Gryglicki"]]
    );
    assert_eq!(
        rs.query("select name from gha_actors_names where actor_id = (select id from gha_actors where login = 'lukaszgryglicki') order by name"),
        vec![
            vec!["Lukasz Gryglicki"],
            vec!["LukaszGryglicki"],
            vec!["Łukasz Gryglicki"]
        ]
    );
    // The highest-priority source wins (user_manual's 7-company definition
    // over config's 8-company one for lukaszgryglicki), `!` decoded in emails
    assert_eq!(
        rs.query("select count(*) from gha_actors_affiliations where actor_id = (select id from gha_actors where login = 'lukaszgryglicki')"),
        vec![vec!["7"]]
    );
    assert_eq!(
        rs.query("select distinct source from gha_actors_affiliations where actor_id = (select id from gha_actors where login = 'lukaszgryglicki')"),
        vec![vec!["user_manual"]]
    );
    assert_eq!(
        rs.query("select email from gha_actors_emails where actor_id = (select id from gha_actors where login = 'lukaszgryglicki') order by email"),
        vec![
            vec!["lgryglicki@o2.pl"],
            vec!["lukaszgryglicki@o2.pl"],
            vec!["lukaszgryglicki@users.noreply.github.com"]
        ]
    );
    // Sources: user (40) beats manual (20) beats config etc.
    assert_eq!(
        rs.query("select source, company_name from gha_actors_affiliations where actor_id = (select id from gha_actors where login = 'src11') order by dt_from"),
        vec![vec!["user", "USER1"], vec!["user", "FINAL"]]
    );
    assert_eq!(
        rs.query("select distinct source from gha_actors_affiliations order by 1"),
        vec![
            vec![""],
            vec!["config"],
            vec!["domain"],
            vec!["manual"],
            vec!["user"],
            vec!["user_manual"]
        ]
    );
    // The lgryglicki names sort: "L. Gryglicki" < "Lukasz Gryglicki"
    assert_eq!(
        rs.query("select name from gha_actors where login = 'lgryglicki'"),
        vec![vec!["L. Gryglicki"]]
    );
}

#[test]
fn test_affs_multi_threaded() {
    let case = Case::new("affs_mt")
        .json(Json::Fixture("test_affs.json"))
        .threads("4");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Processing using MT4 version");
    assert_eq!(rs.count("gha_actors"), 19);
    assert_eq!(rs.count("gha_actors_affiliations"), 31);
}

/// The `util_sh/test_affs.sh` scenario: import, correlate logins by SQL
/// (shared ids, extra ids, extra logins), clean and import again.
#[test]
fn two_phase_correlations() {
    let mut steps = vec![Step::Run(Vec::new())];
    steps.extend(CORRELATION_SQL.iter().map(|s| Step::Sql(s)));
    steps.push(Step::Run(Vec::new()));
    let case = Case::new("two_phase")
        .json(Json::Fixture("test_affs.json"))
        .steps(steps);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs.len(), 2);
    assert_eq!(rs.outs[1].code(), 0);
    let raw: Vec<String> = rs.outs[1]
        .stdout_str()
        .lines()
        .map(str::to_string)
        .collect();
    let expected = [
        // 2 actors are updated because their DB name (the previous pick) is
        // not the smallest of their names any more after the id shuffle
        "Added actors: 0, updated actors: 2, empty names: 14, non-unique names: 4, non-changed: 17",
        "7 new logins added by correlations, copied affiliations: 109 (53 different priority)",
        "Added up to 13 actors emails",
        "Added up to 20 actors names",
        "Affiliations unique: 8, non-unique: 13, with multiple priorities: 21, all user-company connections: 49",
        "Processed 10 companies",
        "Affiliations added up to: 77",
        "Non-acquired companies: checked all regexp: 10, cache hit: 49",
    ];
    for l in expected {
        assert!(raw.iter().any(|x| x == l), "missing {l:?} in {raw:#?}");
    }
    // other3 (same id as other2) got other2's affiliations
    assert_eq!(
        rs.query("select company_name from gha_actors_affiliations a, gha_actors b where a.actor_id = b.id and b.login = 'other3' order by dt_from"),
        vec![vec!["The X"], vec!["CNCF"]]
    );
    // all src* logins share id 1982 now
    assert_eq!(
        rs.query("select count(distinct login) from gha_actors where id = 1982"),
        vec![vec!["13"]]
    );
}

/// The same scenario, multi-threaded (`gone too deep` never triggers here).
#[test]
fn two_phase_correlations_mt() {
    let mut steps = vec![Step::Run(Vec::new())];
    steps.extend(CORRELATION_SQL.iter().map(|s| Step::Sql(s)));
    steps.push(Step::Run(Vec::new()));
    let case = Case::new("two_phase_mt")
        .json(Json::Fixture("test_affs.json"))
        .threads("5")
        .steps(steps);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[1].code(), 0);
    rs.expect_line(1, "Affiliations added up to: 77");
}

// ---------------------------------------------------------------------------
// File locations
// ---------------------------------------------------------------------------

#[test]
fn json_path_argument() {
    let case = Case::new("argv")
        .json_name("users_here.json")
        .arg("{dir}/users_here.json");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Importing <dir>/users_here.json");
    assert_eq!(rs.count("gha_actors"), 2);
}

#[test]
fn file_names_from_env() {
    let case = Case::new("env_names")
        .json_name("affs.json")
        .yaml_name("acq.yaml")
        .env("GHA2DB_AFFILIATIONS_JSON", "affs.json")
        .env("GHA2DB_COMPANY_ACQ_YAML", "acq.yaml");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Importing ./affs.json");
    rs.expect_line(0, "Used mapping 'Kismatic' --> 'Apprenda Inc.'");
}

/// Without `GHA2DB_LOCAL` the files live in `GHA2DB_DATADIR`.
#[test]
fn data_dir_mode() {
    let case = Case::new("datadir")
        .no_local()
        .hide(HIDE_BOB)
        .env("GHA2DB_DATADIR", "{dir}");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Importing <dir>/github_users.json");
    rs.expect_line(0, "Used mapping 'Kismatic' --> 'Apprenda Inc.'");
    // hide/hide.csv is found under the data directory too
    assert_eq!(
        rs.column("gha_actors", "login", "login"),
        vec!["alice", "anon-48181acd22b3edaebc8a447868a7df7ce629920a"]
    );
}

#[test]
fn missing_json_is_fatal() {
    let Some(rs) = both(&Case::new("no_json").json(Json::Missing)) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    assert_eq!(
        rs.error(0).unwrap(),
        "open ./github_users.json: no such file or directory"
    );
    assert_eq!(rs.count("gha_actors"), 0);
}

#[test]
fn missing_yaml_continues_without_mapping() {
    let Some(rs) = both(&Case::new("no_yaml").yaml(Yaml::Missing)) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Cannot read company acquisitions mapping 'open ./companies.yaml: no such file or directory', continuying without");
    rs.expect_line(
        0,
        "Non-acquired companies: checked all regexp: 2, cache hit: 2",
    );
    assert!(!rs.lines(0).iter().any(|l| l.starts_with("Used mapping")));
    assert_eq!(
        rs.column("gha_companies", "name", "name"),
        vec!["ACME", "Kismatic"]
    );
}

#[test]
fn malformed_yaml_is_fatal() {
    let case = Case::new("bad_yaml")
        .yaml(Yaml::Inline("acquisitions: [\n  - x\n"))
        .code_only_errors();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    assert_eq!(rs.count("gha_actors"), 0);
}

// ---------------------------------------------------------------------------
// Acquisitions mapping
// ---------------------------------------------------------------------------

#[test]
fn debug_prints_acquisitions() {
    let case = Case::new("debug")
        .yaml(Yaml::Inline(
            "acquisitions:\n  - ['(?i)^kismatic$', 'Apprenda Inc.']\n  - ['(?i)^acme(\\s+inc\\.?)?$', 'ACME Corp']\n",
        ))
        .env("GHA2DB_DEBUG", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Acquisitions: {Acquisitions:[[(?i)^kismatic$ Apprenda Inc.] [(?i)^acme(\\s+inc\\.?)?$ ACME Corp]]}");
    rs.expect_line(0, "Used mapping 'ACME' --> 'ACME Corp'");
    assert_eq!(
        rs.column("gha_companies", "name", "name"),
        vec!["ACME", "ACME Corp", "Apprenda Inc.", "Kismatic"]
    );
}

#[test]
fn duplicate_acquisition_source_is_fatal() {
    let case = Case::new("acq_dup_src").yaml(Yaml::Inline(
        "acquisitions:\n  - ['(?i)^kismatic$', 'Apprenda Inc.']\n  - ['(?i)^kismatic$', 'Other']\n",
    ));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    assert_eq!(
        rs.error(0).unwrap(),
        "Acquisition number 1 '[(?i)^kismatic$ Other]' is already present in the mapping and maps into 'Apprenda Inc.'"
    );
}

#[test]
fn duplicate_acquisition_result_is_fatal() {
    let case = Case::new("acq_dup_res").yaml(Yaml::Inline(
        "acquisitions:\n  - ['(?i)^kismatic$', 'Apprenda Inc.']\n  - ['(?i)^apprenda$', 'Apprenda Inc.']\n",
    ));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    assert_eq!(
        rs.error(0).unwrap(),
        "Acquisition number 1 '[(?i)^apprenda$ Apprenda Inc.]': some other acquisition already maps into 'Apprenda Inc.', merge them"
    );
}

#[test]
fn acquisition_result_matching_other_regexp_is_fatal() {
    let case = Case::new("acq_res_match").yaml(Yaml::Inline(
        "acquisitions:\n  - ['(?i)^kismatic$', 'Apprenda Inc.']\n  - ['(?i)^apprenda.*$', 'Kismatic']\n",
    ));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    // Both violations point at each other, whichever regexp is checked first
    // (a random map order in Go) reports the same pair
    let err = rs.error(0).unwrap();
    assert!(
        err == "Acquisition's number 1 '(?i)^apprenda.*$' result 'Kismatic' matches other acquisition number 0 '(?i)^kismatic$' which maps to 'Apprenda Inc.', simplify it: '(?i)^apprenda.*$' -> 'Apprenda Inc.'"
            || err == "Acquisition's number 0 '(?i)^kismatic$' result 'Apprenda Inc.' matches other acquisition number 1 '(?i)^apprenda.*$' which maps to 'Kismatic', simplify it: '(?i)^kismatic$' -> 'Kismatic'",
        "{err}"
    );
}

#[test]
fn acquisition_source_matching_other_regexp_is_fatal() {
    let case = Case::new("acq_src_match").yaml(Yaml::Inline(
        "acquisitions:\n  - ['(?i)^k.*$', 'Apprenda Inc.']\n  - ['kismatic', 'Other Inc.']\n",
    ));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    assert_eq!(
        rs.error(0).unwrap(),
        "Acquisition's number 1 '[kismatic Other Inc.]' regexp 'kismatic' matches other acquisition number 0 '(?i)^k.*$' which maps to 'Apprenda Inc.': result is different 'Other Inc.'"
    );
}

#[test]
fn invalid_regexp_is_fatal() {
    let case = Case::new("acq_bad_re")
        .yaml(Yaml::Inline(
            "acquisitions:\n  - ['(?i)^kis(matic$', 'X']\n",
        ))
        .code_only_errors();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    assert_eq!(rs.count("gha_actors"), 0);
}

#[test]
fn skip_company_acquisitions() {
    let case = Case::new("skip_acq")
        .env("GHA2DB_SKIP_COMPANY_ACQ", "1")
        .env("GHA2DB_CHECK_IMPORTED_SHA", "1")
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(
        0,
        "Non-acquired companies: checked all regexp: 2, cache hit: 2",
    );
    assert!(!rs.lines(0).iter().any(|l| l.starts_with("Used mapping")));
    assert_eq!(
        rs.column("gha_companies", "name", "name"),
        vec!["ACME", "Kismatic"]
    );
    // Only the JSON's SHA is recorded, the second run stops early
    assert_eq!(rs.count("gha_imported_shas"), 1);
    assert_eq!(rs.outs[1].code(), 3);
    let sha = rs.column("gha_imported_shas", "sha", "sha")[0].clone();
    rs.expect_line(
        1,
        &format!("./github_users.json (SHA: {sha}) was already imported and skip company acquisitions mode is set, exiting"),
    );
}

/// Every company is mapped once (regexp) and then served from the cache;
/// long names are shortened to 63 bytes (both halves kept).
#[test]
fn company_mapping_stats_and_long_names() {
    let long = "Very Long Company Name Which Exceeds Sixty Three Bytes For Sure Yes";
    let long_u = "Bardzo Długa Nazwa Firmy Która Przekracza Sześćdziesiąt Trzy Bajty";
    assert!(long.len() > 63 && long_u.len() > 63);
    let json = Box::leak(
        format!(
            r#"[{{"login":"u1","affiliation":"Kismatic < 2017-01-01, Deis < 2018-01-01, GitHub < 2019-01-01, Azure","source":"user"}},
 {{"login":"u2","affiliation":"kismatic < 2016-01-01, Kinvolk GmbH","source":"user"}},
 {{"login":"u3","affiliation":"{long} < 2015-06-01, {long_u}","source":"manual"}},
 {{"login":"u4","affiliation":"{long}","source":"manual"}},
 {{"login":"u5","affiliation":"Independent","source":"config"}}]"#
        )
        .into_boxed_str(),
    );
    let Some(rs) = both(&Case::new("mapping").json(Json::Inline(json))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    let lines = rs.lines(0);
    let stats: Vec<&String> = lines
        .iter()
        .filter(|l| {
            l.starts_with("Mapped to") || l.starts_with("Used mapping") || l.starts_with("Non-acq")
        })
        .collect();
    assert_eq!(
        stats,
        vec![
            "Mapped to 'Apprenda Inc.': checked regexp: 2, cache hit: 2",
            "Mapped to 'Microsoft Corporation': checked regexp: 4, cache hit: 4",
            // 3 distinct unmapped companies checked when inserted; the 2 long
            // ones are checked again by their full (unshortened) names when
            // the affiliations are written, the short one is a cache hit
            "Non-acquired companies: checked all regexp: 5, cache hit: 2",
            "Used mapping 'Azure' --> 'Microsoft Corporation'",
            "Used mapping 'Deis' --> 'Microsoft Corporation'",
            "Used mapping 'GitHub' --> 'Microsoft Corporation'",
            "Used mapping 'Kinvolk GmbH' --> 'Microsoft Corporation'",
            "Used mapping 'Kismatic' --> 'Apprenda Inc.'",
            "Used mapping 'kismatic' --> 'Apprenda Inc.'",
        ]
    );
    // 9 distinct original names (shortened) + the 2 mapped ones
    let companies = rs.column("gha_companies", "name", "name");
    assert_eq!(companies.len(), 11);
    for c in &companies {
        assert!(c.len() <= 63, "{c:?}");
    }
    // first 32 bytes + last 31 bytes; the multi-byte one is cut inside a
    // character and de-unicoded afterwards
    assert!(companies
        .contains(&"Very Long Company Name Which Exc Sixty Three Bytes For Sure Yes".to_string()));
    assert!(companies
        .contains(&"Bardzo Duga Nazwa Firmy Ktoracza Szescdziesiat Trzy Bajty".to_string()));
    // The affiliations keep the full names (up to 160 bytes) — only
    // `gha_companies` (whose names become column identifiers) is shortened
    assert_eq!(
        rs.query("select b.login, company_name = original_company_name, length(company_name) from gha_actors_affiliations a, gha_actors b where a.actor_id = b.id and length(company_name) > 63 order by 1, 3"),
        vec![
            vec!["u3", "true", "66"],
            vec!["u3", "true", "67"],
            vec!["u4", "true", "67"],
        ]
    );
}

// ---------------------------------------------------------------------------
// Imported SHA bookkeeping
// ---------------------------------------------------------------------------

#[test]
fn check_imported_sha_flow() {
    let case = Case::new("sha_flow")
        .env("GHA2DB_CHECK_IMPORTED_SHA", "1")
        .steps(vec![
            Step::Run(Vec::new()),
            Step::Run(Vec::new()),
            Step::Json(
                r#"[{"login":"carol","name":"Carol","affiliation":"Kismatic","source":"user"}]"#,
            ),
            Step::Run(Vec::new()),
            Step::Run(vec![("GHA2DB_ONLY_CHECK_IMPORTED_SHA", "1")]),
        ]);
    let Some(rs) = both(&case) else {
        return;
    };
    // 1st import records both SHAs
    assert_eq!(rs.outs[0].code(), 0);
    // 2nd import: both already imported -> 3
    assert_eq!(rs.outs[1].code(), 3);
    let lines = rs.lines(1);
    assert!(
        lines[2]
            .ends_with(") was already imported, checking company acquisitions file import status"),
        "{lines:#?}"
    );
    assert!(lines[3].starts_with("./companies.yaml (SHA: "));
    assert!(lines[3].ends_with(") also imported, exiting"));
    assert!(lines[4].starts_with("Time: "));
    assert_eq!(lines.len(), 5);
    // 3rd: a new JSON, the yaml still imported -> continue with a full import
    assert_eq!(rs.outs[2].code(), 0);
    let lines = rs.lines(2);
    assert!(lines[2].starts_with("./companies.yaml (SHA: "));
    assert!(lines[2].contains(") was already imported, but ./github_users.json (SHA: "));
    assert!(lines[2].ends_with(") wasn't, continuying"));
    rs.expect_line(2, "Processing 1 JSON entries");
    assert_eq!(rs.count("gha_imported_shas"), 3);
    assert_eq!(
        rs.column("gha_actors", "login", "login"),
        vec!["alice", "bob", "carol"]
    );
    // 4th: only checking — both imported -> 3 again
    assert_eq!(rs.outs[3].code(), 3);
}

#[test]
fn only_check_imported_sha_of_new_file() {
    let case = Case::new("sha_only_check")
        .env("GHA2DB_CHECK_IMPORTED_SHA", "1")
        .env("GHA2DB_ONLY_CHECK_IMPORTED_SHA", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Returining not-imported state");
    assert!(!rs.has_line(0, "Processing 2 JSON entries"));
    assert_eq!(rs.count("gha_actors"), 0);
    assert_eq!(rs.count("gha_imported_shas"), 0);
}

/// `GHA2DB_ONLY_CHECK_IMPORTED_SHA` without `GHA2DB_CHECK_IMPORTED_SHA` is
/// ignored.
#[test]
fn only_check_without_check_imports() {
    let case = Case::new("sha_only_nocheck").env("GHA2DB_ONLY_CHECK_IMPORTED_SHA", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    assert!(!rs.has_line(0, "Returining not-imported state"));
    assert_eq!(rs.count("gha_actors"), 2);
}

#[test]
fn dry_run_stops_before_writing() {
    let Some(rs) = both(&Case::new("dryrun").env("GHA2DB_DRY_RUN", "1")) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2);
    let lines = rs.lines(0);
    rs.expect_line(0, "Empty/Not found: names: 1, emails: 1, affiliations: 1");
    rs.expect_line(0, "Exiting due to dry-run mode.");
    assert!(lines.last().unwrap().starts_with("Time: "));
    assert!(!lines
        .iter()
        .any(|l| l.starts_with("Added actors") || l.starts_with("Processing using")));
    assert_eq!(rs.count("gha_actors"), 0);
}

// ---------------------------------------------------------------------------
// Hidden logins, actor data, updates
// ---------------------------------------------------------------------------

#[test]
fn hidden_logins() {
    let json = r#"[{"login":"bob","email":"bob!example.com","name":"Bob Builder","affiliation":"ACME","source":"user","country_id":"us","sex":"m","sex_prob":0.99,"tz":"America/New_York","age":41},
 {"login":"BOB","email":"bobby!example.com","affiliation":"ACME < 2019-01-01, Kismatic","source":"user_manual"},
 {"login":"alice","name":"Alice","affiliation":"Deis","source":"user"}]"#;
    let case = Case::new("hidden").json(Json::Inline(json)).hide(HIDE_BOB);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    assert_eq!(
        rs.query(
            "select login, name, country_id, sex, sex_prob, tz, age from gha_actors order by login"
        ),
        vec![
            vec!["alice", "Alice", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"],
            vec![
                "anon-48181acd22b3edaebc8a447868a7df7ce629920a",
                "Bob Builder",
                "us",
                "m",
                "0.99",
                "America/New_York",
                "41"
            ],
        ]
    );
    // Both variants of the login collapse; user_manual (30) < user (40)
    assert_eq!(
        rs.query("select company_name from gha_actors_affiliations a, gha_actors b where a.actor_id = b.id and b.login like 'anon-%' order by 1"),
        vec![vec!["ACME"]]
    );
    assert_eq!(
        rs.column("gha_actors_emails", "email", "email"),
        vec!["bob@example.com", "bobby@example.com"]
    );
    let off = rs.query("select tz_offset from gha_actors where tz = 'America/New_York'");
    assert!(off[0][0] == "-240" || off[0][0] == "-300", "{off:?}");
}

#[test]
fn hide_csv_without_header_and_unknown_shas() {
    let case = Case::new("hidden_nohdr").hide(
        "0000000000000000000000000000000000000000\n48181acd22b3edaebc8a447868a7df7ce629920a\n",
    );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    assert_eq!(
        rs.column("gha_actors", "login", "login"),
        vec!["alice", "anon-48181acd22b3edaebc8a447868a7df7ce629920a"]
    );
}

/// The country/sex/tz/age set with the best score wins for a login that
/// appears several times; unknown time zones give a NULL offset.
#[test]
fn best_scored_actor_data_wins() {
    let json = r#"[{"login":"dan","name":"Dan","country_id":"de","sex":"m","sex_prob":0.7,"tz":"Europe/Berlin","age":50},
 {"login":"Dan","country_id":"pl","tz":"Mars/Olympus_Mons"},
 {"login":"DAN","country_id":"fr","sex":"m","sex_prob":0.99,"tz":"Europe/Paris","age":33},
 {"login":"eve","tz":"Mars/Olympus_Mons","sex":"f","sex_prob":1},
 {"login":"fay","tz":"UTC","age":0,"sex_prob":0},
 {"login":"gus","tz":"","sex":"","country_id":""}]"#;
    let Some(rs) = both(&Case::new("score").json(Json::Inline(json))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(
        0,
        "Processing non-empty: 1 name lists, 0 email lists, 0 affiliations lists, 4 objects",
    );
    assert_eq!(
        rs.query("select login, name, country_id, sex, sex_prob, tz, tz_offset is null, age from gha_actors order by login"),
        vec![
            // 3 entries for dan: the one with more data (fr/Paris/0.99/33)
            // scores highest
            vec!["dan", "Dan", "fr", "m", "0.99", "Europe/Paris", "false", "33"],
            vec!["eve", "", "<nil>", "f", "1", "Mars/Olympus_Mons", "true", "<nil>"],
            vec!["fay", "", "<nil>", "<nil>", "0", "UTC", "false", "0"],
            vec!["gus", "", "", "", "<nil>", "", "true", "<nil>"],
        ]
    );
    assert_eq!(
        rs.query("select tz_offset from gha_actors where login = 'fay'"),
        vec![vec!["0"]]
    );
}

/// Actors already in the database (as GHA import creates them: any login
/// case, empty name) are updated with the JSON data; matching ones counted
/// as non-changed; ids are kept.
#[test]
fn existing_actors_are_updated() {
    let json = r#"[{"login":"alice","name":"Alice A","email":"alice!example.com","affiliation":"ACME","source":"user","country_id":"pl","sex":"f","sex_prob":0.9,"tz":"UTC","age":30},
 {"login":"bob","affiliation":"Kismatic","source":"user","country_id":"us"},
 {"login":"carol","name":"Carol","affiliation":"Deis","source":"config","country_id":"de","sex":"f","sex_prob":0.8,"tz":"UTC","age":20}]"#;
    let case = Case::new("update")
        .json(Json::Inline(json))
        .seed(&[
            "insert into gha_actors(id, login, name) values (1, 'Alice', '')",
            "insert into gha_actors(id, login, name) values (2, 'ALICE', 'old')",
            "insert into gha_actors(id, login, name, country_id) values (3, 'bob', 'Bob', 'us')",
            "insert into gha_actors(id, login, name, country_id, sex, sex_prob, tz, tz_offset, age) values (4, 'carol', 'Carol', 'de', 'f', 0.8, 'UTC', 0, 20)",
        ]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    // alice: found as ALICE/old -> updated; bob: no name in the JSON and the
    // same country -> unchanged; carol: identical -> unchanged
    rs.expect_line(
        0,
        "Added actors: 0, updated actors: 1, empty names: 1, non-unique names: 0, non-changed: 2",
    );
    // the database logins Alice/ALICE are correlated with alice
    rs.expect_line(
        0,
        "2 new logins added by correlations, copied affiliations: 2 (2 different priority)",
    );
    rs.expect_line(0, "Affiliations unique: 5, non-unique: 0, with multiple priorities: 0, all user-company connections: 5");
    // insert attempts (per login and id), not distinct rows
    rs.expect_line(0, "Affiliations added up to: 8");
    assert_eq!(
        rs.query("select id, login, name, country_id, sex, sex_prob, tz, tz_offset, age from gha_actors order by id"),
        vec![
            vec!["1", "Alice", "Alice A", "pl", "f", "0.9", "UTC", "0", "30"],
            vec!["2", "ALICE", "Alice A", "pl", "f", "0.9", "UTC", "0", "30"],
            // no name in the JSON: the existing one is kept
            vec!["3", "bob", "Bob", "us", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"],
            vec!["4", "carol", "Carol", "de", "f", "0.8", "UTC", "0", "20"],
        ]
    );
    // Affiliations/emails/names attach to every id of a login
    assert_eq!(
        rs.query("select actor_id, company_name from gha_actors_affiliations order by 1, 2"),
        vec![
            vec!["1", "ACME"],
            vec!["2", "ACME"],
            vec!["3", "Apprenda Inc."],
            vec!["4", "Microsoft Corporation"],
        ]
    );
    assert_eq!(
        rs.query("select actor_id, email from gha_actors_emails order by 1"),
        vec![
            vec!["1", "alice@example.com"],
            vec!["2", "alice@example.com"]
        ]
    );
    assert_eq!(
        rs.query("select actor_id, name from gha_actors_names order by 1"),
        vec![
            vec!["1", "Alice A"],
            vec!["2", "Alice A"],
            vec!["4", "Carol"]
        ]
    );
}

/// Names longer than 120 bytes are truncated (on a character boundary).
#[test]
fn long_names_are_truncated() {
    let name = "Ąę".repeat(70);
    assert!(name.len() > 120);
    let json = Box::leak(
        format!(r#"[{{"login":"lon","name":"{name}","affiliation":"ACME","source":"user"}}]"#)
            .into_boxed_str(),
    );
    let Some(rs) = both(&Case::new("long_name").json(Json::Inline(json))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    let got = rs.column("gha_actors", "name", "login");
    assert_eq!(got[0], "Ąę".repeat(30));
    assert_eq!(
        rs.column("gha_actors_names", "name", "name")[0],
        "Ąę".repeat(30)
    );
}

/// Login/id correlation chains deeper than 10 rounds are reported (and cut).
#[test]
fn deep_correlation_chain_is_reported() {
    let mut seed: Vec<&'static str> = Vec::new();
    for i in 1..=14 {
        seed.push(leak(&format!(
            "insert into gha_actors(id, login, name) values ({i}, 'a{}', ''), ({i}, 'a{i}', '')",
            i - 1
        )));
    }
    let json = r#"[{"login":"a0","affiliation":"ACME","source":"user"}]"#;
    let case = Case::new("too_deep").json(Json::Inline(json)).seed(&seed);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    let lines = rs.lines(0);
    let deep: Vec<&String> = lines
        .iter()
        .filter(|l| l.starts_with("Error (non fatal): gone too deep"))
        .collect();
    assert_eq!(deep.len(), 2, "{lines:#?}");
    assert_eq!(
        deep[0],
        "Error (non fatal): gone too deep: Logins: 'a0,a1,a2,a3,a4,a5,a6,a7,a8,a9'=='a0,a1,a10,a2,a3,a4,a5,a6,a7,a8,a9', IDs: '1,2,3,4,5,6,7,8,9'=='1,10,2,3,4,5,6,7,8,9'"
    );
    assert_eq!(
        deep[1],
        "Error (non fatal): gone too deep: logins map: map[a0:{} a1:{} a10:{} a2:{} a3:{} a4:{} a5:{} a6:{} a7:{} a8:{} a9:{}], ids map: map[1:{} 2:{} 3:{} 4:{} 5:{} 6:{} 7:{} 8:{} 9:{} 10:{}]"
    );
    // The affiliation is copied to the 10 ids reached
    assert_eq!(
        rs.query("select count(*) from gha_actors_affiliations"),
        vec![vec!["10"]]
    );
    rs.expect_line(
        0,
        "10 new logins added by correlations, copied affiliations: 10 (10 different priority)",
    );
}

// ---------------------------------------------------------------------------
// JSON parsing
// ---------------------------------------------------------------------------

#[test]
fn affiliation_markers_and_quotes() {
    let json = r#"[{"login":"a","affiliation":"NotFound","source":"notfound","email":"a!x.com"},
 {"login":"b","affiliation":"(Unknown)"},
 {"login":"c","affiliation":"?"},
 {"login":"d","affiliation":"-"},
 {"login":"e","affiliation":""},
 {"login":"f","affiliation":"\"Quoted, Inc\" < 2010-01-01, \"Other\"","source":"manual"},
 {"login":"g","affiliation":"  Spaced  <  2011-02-03  ,  , Next ","source":"user"},
 {"login":"h","affiliation":"Only < 2012-12-12","source":"domain"},
 {"login":"i","affiliation":"ACME","source":"WhatIsThis"}]"#;
    let Some(rs) = both(&Case::new("markers").json(Json::Inline(json))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Empty/Not found: names: 9, emails: 8, affiliations: 5");
    rs.expect_line(0, "Affiliations unique: 4, non-unique: 0, with multiple priorities: 0, all user-company connections: 7");
    rs.expect_line(0, "Processed 7 companies");
    assert_eq!(
        rs.query("select b.login, company_name, dt_from, dt_to, source from gha_actors_affiliations a, gha_actors b where a.actor_id = b.id order by 1, 3, 2"),
        vec![
            // quotes are dropped, so "Quoted, Inc" splits into two companies
            // and the date attaches to `Inc` (after the open-ended `Quoted`)
            vec!["f", "Quoted", "1900-01-01T00:00:00Z", "2100-01-01T00:00:00Z", "manual"],
            vec!["f", "Other", "2010-01-01T00:00:00Z", "2100-01-01T00:00:00Z", "manual"],
            vec!["f", "Inc", "2100-01-01T00:00:00Z", "2010-01-01T00:00:00Z", "manual"],
            vec!["g", "Spaced", "1900-01-01T00:00:00Z", "2011-02-03T00:00:00Z", "user"],
            vec!["g", "Next", "2011-02-03T00:00:00Z", "2100-01-01T00:00:00Z", "user"],
            vec!["h", "Only", "1900-01-01T00:00:00Z", "2012-12-12T00:00:00Z", "domain"],
            // unknown source -> priority 0 -> empty source
            vec!["i", "ACME", "1900-01-01T00:00:00Z", "2100-01-01T00:00:00Z", ""],
        ]
    );
}

/// jsoniter semantics: case-insensitive keys, last duplicate wins, unknown
/// keys skipped, `null` fields and elements, integers for floats.
#[test]
fn json_key_and_null_handling() {
    let json = r#"[{"Login":"kim","NAME":"Kim","Email":"kim!x.com","AFFILIATION":"ACME","Source":"user","Country_ID":"kr","SEX":"f","Sex_Prob":1,"TZ":"Asia/Seoul","Age":29,"commits":123,"location":"Seoul"},
 {"login":"dup","login":"dup2","name":"first","name":"second","affiliation":null,"email":null,"source":null,"country_id":null,"sex":null,"sex_prob":null,"tz":null,"age":null},
 null,
 {}]"#;
    let Some(rs) = both(&Case::new("json_keys").json(Json::Inline(json))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Processing 4 JSON entries");
    // the null element and `{}` are users with an empty login
    rs.expect_line(
        0,
        "Processing non-empty: 2 name lists, 1 email lists, 1 affiliations lists, 3 objects",
    );
    assert_eq!(
        rs.query(
            "select login, name, country_id, sex, sex_prob, tz, age from gha_actors order by login"
        ),
        vec![
            vec!["", "", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"],
            vec!["dup2", "second", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"],
            vec!["kim", "Kim", "kr", "f", "1", "Asia/Seoul", "29"],
        ]
    );
    assert_eq!(
        rs.query("select tz_offset from gha_actors where login = 'kim'"),
        vec![vec!["540"]]
    );
}

#[test]
fn top_level_null_imports_nothing() {
    let Some(rs) = both(&Case::new("json_null").json(Json::Inline("null"))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Processing 0 JSON entries");
    rs.expect_line(
        0,
        "Added actors: 0, updated actors: 0, empty names: 0, non-unique names: 0, non-changed: 0",
    );
    rs.expect_line(0, "Processed 0 companies");
    rs.expect_line(0, "Affiliations added up to: 0");
    assert_eq!(rs.count("gha_actors"), 0);
}

#[test]
fn empty_array_imports_nothing() {
    let Some(rs) = both(&Case::new("json_empty_ary").json(Json::Inline("[]"))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(0, "Processing 0 JSON entries");
}

fn malformed(name: &'static str, json: &'static str) {
    let case = Case::new(name).json(Json::Inline(json)).code_only_errors();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 2, "{name}: {}", rs.outs[0].stderr_str());
    rs.expect_line(0, "Importing ./github_users.json");
    assert!(!rs.has_line(0, "Processing 0 JSON entries"));
    assert_eq!(rs.count("gha_actors"), 0);
}

#[test]
fn malformed_json_syntax() {
    malformed("json_syntax", r#"[{"login":"a",]"#);
}

#[test]
fn malformed_json_empty_file() {
    malformed("json_empty", "");
}

#[test]
fn malformed_json_object_instead_of_array() {
    malformed("json_object", r#"{"login":"a"}"#);
}

#[test]
fn malformed_json_wrong_field_type() {
    malformed("json_type", r#"[{"login":"a","age":"thirty"}]"#);
}

#[test]
fn malformed_json_string_element() {
    malformed("json_string_elem", r#"["a"]"#);
}

#[test]
fn malformed_json_trailing_garbage() {
    malformed("json_trailing", r#"[{"login":"a"}] x"#);
}

#[test]
fn malformed_json_float_for_int() {
    malformed("json_float_int", r#"[{"login":"a","age":30.5}]"#);
}

// ---------------------------------------------------------------------------
// Emails and names
// ---------------------------------------------------------------------------

#[test]
fn emails_and_names_are_aggregated_per_login() {
    let json = r#"[{"login":"Zed","email":"Zed!Example.COM","name":"Zed One"},
 {"login":"zed","email":"zed!example.com","name":"Zed One"},
 {"login":"ZED","email":"z2!example.com","name":""},
 {"login":"ann","email":"ann!x.org"},
 {"login":"ann","email":"ANN!x.org"}]"#;
    let Some(rs) = both(&Case::new("emails").json(Json::Inline(json))) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(
        0,
        "Processing non-empty: 1 name lists, 2 email lists, 0 affiliations lists, 2 objects",
    );
    rs.expect_line(0, "Added up to 3 actors emails");
    rs.expect_line(0, "Added up to 1 actors names");
    rs.expect_line(0, "Affiliations unique: 0, non-unique: 0, with multiple priorities: 0, all user-company connections: 0");
    assert_eq!(
        rs.query("select b.login, email from gha_actors_emails a, gha_actors b where a.actor_id = b.id order by 1, 2"),
        vec![
            vec!["ann", "ann@x.org"],
            vec!["zed", "z2@example.com"],
            vec!["zed", "zed@example.com"],
        ]
    );
    assert_eq!(
        rs.query("select name from gha_actors where login = 'zed'"),
        vec![vec!["Zed One"]]
    );
    assert_eq!(rs.count("gha_companies"), 0);
    assert_eq!(rs.count("gha_actors_affiliations"), 0);
}

/// Bug 49: a login with several names, or with several equally long
/// affiliation definitions of the same source priority, gets the smallest
/// one — Go used to pick a random map key (397 such affiliation ties and 215
/// multi-name logins in the real `github_users.json`, so ~100 actor names
/// and the company of e.g. `jstrachan` — CloudBees vs Red Hat — changed on
/// every daily import). Runs twice: the second import must change nothing.
#[test]
fn ties_are_broken_deterministically() {
    let json = r#"[{"login":"jstrachan","name":"James Strachan","source":"config","affiliation":"Red Hat Inc."},
 {"login":"jstrachan","name":"James","source":"config","affiliation":"CloudBees Inc."},
 {"login":"grobie","source":"user","affiliation":"groom gbr < 2016-07-01, SoundCloud Global Limited & Co. KG"},
 {"login":"grobie","source":"user","affiliation":"groom gbr < 2012-07-01, SoundCloud Global Limited & Co. KG"},
 {"login":"stanley","source":"manual","affiliation":"Independent < 2018-06-01, LSCM < 2018-09-01, Independent"},
 {"login":"stanley","source":"manual","affiliation":"Independent < 2018-06-01, LSCM < 2018-09-01, Google LLC"},
 {"login":"stanley","source":"domain","affiliation":"Zzz Corp, Yyy Corp, Xxx Corp, Www Corp"},
 {"login":"longer","source":"config","affiliation":"Solo Inc."},
 {"login":"longer","source":"config","affiliation":"Alpha < 2020-01-01, Solo Inc."}]"#;
    let case = Case::new("ties")
        .json(Json::Inline(json))
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(
        0,
        "Added actors: 4, updated actors: 0, empty names: 3, non-unique names: 1, non-changed: 0",
    );
    rs.expect_line(
        0,
        "Affiliations unique: 0, non-unique: 4, with multiple priorities: 1, all user-company connections: 8",
    );
    // the smallest of the tied names/definitions wins; the top priority
    // ('manual' over 'domain') and the longest definition still win first
    assert_eq!(
        rs.query("select name from gha_actors where login = 'jstrachan'"),
        vec![vec!["James"]]
    );
    assert_eq!(
        rs.query(
            "select b.login, company_name, dt_from::date::text, dt_to::date::text, source \
             from gha_actors_affiliations a, gha_actors b where a.actor_id = b.id order by 1, 3"
        ),
        vec![
            vec!["grobie", "groom gbr", "1900-01-01", "2012-07-01", "user"],
            vec![
                "grobie",
                "SoundCloud Global Limited & Co. KG",
                "2012-07-01",
                "2100-01-01",
                "user"
            ],
            vec![
                "jstrachan",
                "CloudBees Inc.",
                "1900-01-01",
                "2100-01-01",
                "config"
            ],
            vec!["longer", "Alpha", "1900-01-01", "2020-01-01", "config"],
            vec!["longer", "Solo Inc.", "2020-01-01", "2100-01-01", "config"],
            vec![
                "stanley",
                "Independent",
                "1900-01-01",
                "2018-06-01",
                "manual"
            ],
            vec!["stanley", "LSCM", "2018-06-01", "2018-09-01", "manual"],
            vec![
                "stanley",
                "Google LLC",
                "2018-09-01",
                "2100-01-01",
                "manual"
            ],
        ]
    );
    assert_eq!(rs.count("gha_companies"), 8);
    // second import: nothing changes
    assert_eq!(rs.outs[1].code(), 0);
    rs.expect_line(
        1,
        "Added actors: 0, updated actors: 0, empty names: 3, non-unique names: 1, non-changed: 4",
    );
    assert_eq!(rs.count("gha_actors_affiliations"), 8);
}

/// Re-importing the same data is idempotent (`on conflict do nothing`).
/// Bug 51: the stored name is `maybeHide(TruncToBytes(name, 120))`, but a
/// re-import compared the raw name with it, so every actor whose name is
/// longer than 120 bytes (9 in the live `github_users.json`) or hidden was
/// "updated" to the very same value on every import.
#[test]
fn stored_name_form_is_compared_on_reimport() {
    let long_name = format!("{}新疆改造", "A".repeat(115));
    let json = format!(
        r#"[{{"login":"longname","name":"{long_name}","source":"user","affiliation":"ACME"}},
 {{"login":"carol","name":"bob","source":"user","affiliation":"ACME"}},
 {{"login":"alice","name":"Alice","source":"user","affiliation":"Deis"}}]"#
    );
    let json: &'static str = Box::leak(json.into_boxed_str());
    let case = Case::new("storedname")
        .json(Json::Inline(json))
        .hide(HIDE_BOB)
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[0].code(), 0);
    rs.expect_line(
        0,
        "Added actors: 3, updated actors: 0, empty names: 0, non-unique names: 0, non-changed: 0",
    );
    // the name is cut at 120 bytes on a character boundary, the hidden name is anonymized
    assert_eq!(
        rs.query("select login, name, octet_length(name) from gha_actors order by login"),
        vec![
            vec!["alice".to_string(), "Alice".to_string(), "5".to_string()],
            vec![
                "carol".to_string(),
                "anon-48181acd22b3edaebc8a447868a7df7ce629920a".to_string(),
                "45".to_string()
            ],
            vec![
                "longname".to_string(),
                format!("{}新", "A".repeat(115)),
                "118".to_string()
            ],
        ]
    );
    assert_eq!(rs.outs[1].code(), 0);
    rs.expect_line(
        1,
        "Added actors: 0, updated actors: 0, empty names: 0, non-unique names: 0, non-changed: 3",
    );
}

#[test]
fn reimport_is_idempotent() {
    let case = Case::new("reimport")
        .json(Json::Fixture("test_affs.json"))
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.outs[1].code(), 0);
    rs.expect_raw_line(
        1,
        "Added actors: 0, updated actors: 0, empty names: 14, non-unique names: 4, non-changed: 19",
    );
    rs.expect_line(1, "Affiliations added up to: 31");
    assert_eq!(rs.count("gha_actors"), 19);
    assert_eq!(rs.count("gha_actors_affiliations"), 31);
    assert_eq!(rs.count("gha_companies"), 18);
}
