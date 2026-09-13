//! Go ⇄ Rust compatibility tests for `hide_data`.
//!
//! Two modes are covered:
//!
//! * `hide_data <name> …` — adds SHA1s to `hide/hide.csv` in the current
//!   directory. Each side runs in its own scratch directory; compared: exit
//!   code, stdout (durations masked), the `Error: '…'` lines of fatal errors
//!   and the resulting file (as a set of lines — the Go tool writes its map in
//!   random order, the Rust port in sorted order — plus the header position).
//! * `hide_data` — anonymizes the hidden actors in every enabled project
//!   database of `projects.yaml`. Each side gets its own scratch databases
//!   (`dbtest_hide_<case>_<go|rs>_<project>`, schema from
//!   `compat/fixtures/hide_data/schema.sql`, the same seed rows); compared:
//!   exit code, stdout lines as a set (the tool reports from concurrent
//!   workers walking a randomly ordered map), stderr program lines and every
//!   table of every database afterwards.
//!
//! The database cases need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise); the file cases always run.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("hide_data")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_hide_data"))
}

const SHA_ALICE: &str = "522b276a356bdf39013dfabea2cd43e141ecc9e8";
const SHA_BOB: &str = "48181acd22b3edaebc8a447868a7df7ce629920a";
const SHA_CAROL: &str = "28b92b56ee64b92ebb72d865f172ef00c708df83";
const SHA_DAVE: &str = "bfcdf3e6ca6cef45543bfbb57509c92aec9a39fb";
/// `Żółw`
const SHA_ZOLW: &str = "06ab29dba3a7836b681be9a77c10ebbf3ec5d95d";
/// `""`
const SHA_EMPTY: &str = "da39a3ee5e6b4b0d3255bfef95601890afd80709";

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The comparable stderr lines: `Error: '…'` lines of fatal errors and the
/// `PqError:` lines of `FatalOnError` (Go's `ErrorType:` lines and stack
/// traces are not reproduced).
fn stderr_lines(out: &Outcome) -> Vec<String> {
    out.stderr_str()
        .lines()
        .filter(|l| l.starts_with("Error: '") || l.starts_with("PqError: "))
        .map(str::to_string)
        .collect()
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";

/// All stdout lines, durations masked (for the Go ⇄ Rust comparison).
fn all_lines(out: &Outcome) -> Vec<String> {
    mask_go_durations(&out.stdout_str())
        .lines()
        .map(str::to_string)
        .collect()
}

/// stdout lines without the banner (for the expectations of the tests).
fn stdout_lines(out: &Outcome) -> Vec<String> {
    all_lines(out).into_iter().filter(|l| l != BANNER).collect()
}

// ---------------------------------------------------------------------------
// File mode: `hide_data <names…>`
// ---------------------------------------------------------------------------

struct FileCase {
    name: &'static str,
    args: Vec<&'static str>,
    /// Create `hide/` in the scratch directory.
    hide_dir: bool,
    /// Initial `hide/hide.csv` content (needs `hide_dir`).
    csv: Option<&'static str>,
    /// Put this `hide/hide.csv` into a `GHA2DB_DATADIR` directory.
    datadir_csv: Option<&'static str>,
}

impl FileCase {
    fn new(name: &'static str, args: &[&'static str]) -> Self {
        FileCase {
            name,
            args: args.to_vec(),
            hide_dir: true,
            csv: None,
            datadir_csv: None,
        }
    }
    fn csv(mut self, c: &'static str) -> Self {
        self.csv = Some(c);
        self
    }
    fn no_hide_dir(mut self) -> Self {
        self.hide_dir = false;
        self
    }
    fn datadir_csv(mut self, c: &'static str) -> Self {
        self.datadir_csv = Some(c);
        self
    }
}

struct FileSide {
    dir: TempDir,
    out: Outcome,
}

impl FileSide {
    fn csv(&self) -> Option<String> {
        fs::read_to_string(self.dir.path().join("hide/hide.csv")).ok()
    }
    /// The file as (header, sorted body lines); `None` when it does not exist.
    fn csv_set(&self) -> Option<(String, BTreeSet<String>)> {
        let text = self.csv()?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default().to_string();
        Some((header, lines.map(str::to_string).collect()))
    }
}

fn run_file_side(bin: &Path, case: &FileCase, suffix: &str) -> FileSide {
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_hide_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    if case.hide_dir {
        fs::create_dir(dir.path().join("hide")).unwrap();
        if let Some(c) = case.csv {
            fs::write(dir.path().join("hide/hide.csv"), c).unwrap();
        }
    }
    let mut inv = Invocation::new()
        .cwd(dir.path().to_path_buf())
        .env("GHA2DB_SKIPLOG", "1")
        .env("GHA2DB_SKIPTIME", "1");
    if let Some(c) = case.datadir_csv {
        let dd = dir.path().join("datadir");
        fs::create_dir_all(dd.join("hide")).unwrap();
        fs::write(dd.join("hide/hide.csv"), c).unwrap();
        inv = inv.env("GHA2DB_DATADIR", leak(&dd.to_string_lossy()));
    }
    for a in &case.args {
        inv = inv.arg(*a);
    }
    let out = run(bin, &inv);
    FileSide { dir, out }
}

/// Run both binaries in file mode and compare; returns the Rust side.
fn both_file(case: &FileCase) -> FileSide {
    let rust = run_file_side(&rust_bin(), case, "rs");
    if let Some(go) = go_bin() {
        let go = run_file_side(&go, case, "go");
        let ctx = format!(
            "\ncase {:?} args {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- go file:\n{:?}\n--- rust code {:?} stdout:\n{}--- rust stderr:\n{}--- rust file:\n{:?}",
            case.name,
            case.args,
            go.out.code,
            go.out.stdout_str(),
            go.out.stderr_str(),
            go.csv(),
            rust.out.code,
            rust.out.stdout_str(),
            rust.out.stderr_str(),
            rust.csv(),
        );
        assert_eq!(go.out.code, rust.out.code, "exit code{ctx}");
        assert_eq!(all_lines(&go.out), all_lines(&rust.out), "stdout{ctx}");
        assert_eq!(
            stderr_lines(&go.out),
            stderr_lines(&rust.out),
            "stderr{ctx}"
        );
        assert_eq!(go.csv_set(), rust.csv_set(), "hide.csv{ctx}");
    }
    rust
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn file_add_two_new_names() {
    let rs = both_file(&FileCase::new("add2", &["alice", "bob"]));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(stdout_lines(&rs.out), vec!["Time: <duration>"]);
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_BOB])))
    );
    // Rust writes the map sorted, one SHA1 per line, `\n` endings.
    assert_eq!(rs.csv().unwrap(), format!("sha1\n{SHA_BOB}\n{SHA_ALICE}\n"));
}

#[test]
fn file_add_to_existing_file_skips_known() {
    let csv = "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n06ab29dba3a7836b681be9a77c10ebbf3ec5d95d\n";
    let rs = both_file(&FileCase::new("addex", &["carol", "alice", "Żółw", "carol"]).csv(csv));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'alice', SHA1 '{SHA_ALICE}' - already added"),
            format!("Skipping 'Żółw', SHA1 '{SHA_ZOLW}' - already added"),
            // The second `carol` was added by the first one (in the map, not
            // the file) — skipped as well.
            format!("Skipping 'carol', SHA1 '{SHA_CAROL}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_ZOLW, SHA_CAROL])))
    );
}

#[test]
fn file_all_known_leaves_file_untouched() {
    // Odd content on purpose: an unsorted body and no trailing newline — it
    // must survive as is (nothing added → nothing rewritten).
    let csv =
        "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n48181acd22b3edaebc8a447868a7df7ce629920a";
    let rs = both_file(&FileCase::new("known", &["bob", "alice"]).csv(csv));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(rs.csv().unwrap(), csv);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'bob', SHA1 '{SHA_BOB}' - already added"),
            format!("Skipping 'alice', SHA1 '{SHA_ALICE}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
}

#[test]
fn file_arguments_are_trimmed() {
    let csv = "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n";
    let rs = both_file(&FileCase::new("trim", &["  alice ", "\tbob\n", " "]).csv(csv));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'alice', SHA1 '{SHA_ALICE}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
    // `" "` trims to the empty string, whose SHA1 is added too.
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_BOB, SHA_EMPTY])))
    );
}

#[test]
fn file_special_characters() {
    let rs = both_file(&FileCase::new(
        "special",
        &["O'Reilly", "a,b", "x\"y", "Żółw"],
    ));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.csv_set(),
        Some((
            "sha1".to_string(),
            set(&[
                "4c7b710b3c15c998736f367285e128ff1c279158",
                "5d8b1241b0484dd20c2cfeca6f692becfbab5d18",
                "94756446fce53ca011332f169d34a4640dca5f2f",
                SHA_ZOLW,
            ])
        ))
    );
}

#[test]
fn file_no_hide_directory_is_fatal() {
    let rs = both_file(&FileCase::new("nodir", &["alice"]).no_hide_dir());
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        stderr_lines(&rs.out),
        vec!["Error: 'open hide/hide.csv: no such file or directory'"]
    );
    assert_eq!(rs.csv(), None);
}

#[test]
fn file_no_hide_directory_all_known_is_fine() {
    // Nothing to add → the file is never opened for writing.
    let dd = "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n";
    let rs = both_file(
        &FileCase::new("nodirknown", &["alice"])
            .no_hide_dir()
            .datadir_csv(dd),
    );
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'alice', SHA1 '{SHA_ALICE}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
}

#[test]
fn file_datadir_fallback_read_but_cwd_written() {
    // The SHA1s are read from `$GHA2DB_DATADIR/hide/hide.csv` (no file in the
    // current directory) but the result is written to `./hide/hide.csv`.
    let dd = "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n";
    let rs = both_file(&FileCase::new("datadir", &["alice", "dave"]).datadir_csv(dd));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'alice', SHA1 '{SHA_ALICE}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_DAVE])))
    );
    assert_eq!(
        fs::read_to_string(rs.dir.path().join("datadir/hide/hide.csv")).unwrap(),
        dd
    );
}

#[test]
fn file_cwd_file_wins_over_datadir() {
    let csv = "sha1\n48181acd22b3edaebc8a447868a7df7ce629920a\n";
    let dd = "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n";
    let rs = both_file(
        &FileCase::new("cwdwins", &["alice", "bob"])
            .csv(csv)
            .datadir_csv(dd),
    );
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'bob', SHA1 '{SHA_BOB}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_BOB])))
    );
}

#[test]
fn file_header_only_and_blank_lines() {
    // Blank lines are skipped by the CSV reader, the header is not a SHA1.
    let csv = "sha1\n\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n\n";
    let rs = both_file(&FileCase::new("blank", &["alice", "bob"]).csv(csv));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_BOB])))
    );
    let rs = both_file(&FileCase::new("hdronly", &["bob"]).csv("sha1\n"));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(rs.csv_set(), Some(("sha1".to_string(), set(&[SHA_BOB]))));
}

#[test]
fn file_without_header_is_fine() {
    // The `sha1` header is optional for the reader (`sha1` rows are skipped
    // wherever they are) but always written.
    let csv = "522b276a356bdf39013dfabea2cd43e141ecc9e8\nsha1\n";
    let rs = both_file(&FileCase::new("nohdr", &["alice", "bob"]).csv(csv));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'alice', SHA1 '{SHA_ALICE}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_BOB])))
    );
}

#[test]
fn file_wrong_field_count_is_fatal() {
    let csv = "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8,extra\n";
    let rs = both_file(&FileCase::new("badcsv", &["bob"]).csv(csv));
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        stderr_lines(&rs.out),
        vec!["Error: 'record on line 2: wrong number of fields'"]
    );
    assert_eq!(rs.csv().unwrap(), csv);
}

#[test]
fn file_only_first_column_matters() {
    // Consistent two-column rows are accepted; only the first column is used.
    let csv = "sha1,comment\n522b276a356bdf39013dfabea2cd43e141ecc9e8,alice\n";
    let rs = both_file(&FileCase::new("twocols", &["alice", "bob"]).csv(csv));
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            format!("Skipping 'alice', SHA1 '{SHA_ALICE}' - already added"),
            "Time: <duration>".to_string(),
        ]
    );
    // Rewritten as single-column rows.
    assert_eq!(
        rs.csv_set(),
        Some(("sha1".to_string(), set(&[SHA_ALICE, SHA_BOB])))
    );
}

// ---------------------------------------------------------------------------
// Database mode: `hide_data`
// ---------------------------------------------------------------------------

/// The test `projects.yaml`: `{db:<key>}` becomes the side's database name of
/// that project.
const YAML: &str = r#"---
projects:
  p1:
    name: Project One
    psql_db: {db:p1}
    order: 1
    main_repo: org1/repo1
  p2:
    name: Project Two
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
"#;

/// A single project.
const YAML_P1: &str = r#"---
projects:
  p1:
    name: Project One
    psql_db: {db:p1}
    order: 1
"#;

/// Two projects sharing an `order` (bug 19).
const YAML_DUP_ORDER: &str = r#"---
projects:
  zeta:
    psql_db: {db:zeta}
    order: 7
  alpha:
    psql_db: {db:alpha}
    order: 7
  first:
    psql_db: {db:first}
    order: 1
"#;

/// Default `hide/hide.csv`: alice, bob and Żółw are hidden.
const CSV: &str = "sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n48181acd22b3edaebc8a447868a7df7ce629920a\n06ab29dba3a7836b681be9a77c10ebbf3ec5d95d\n";

/// Seed rows: every anonymized column carries at least one hidden value,
/// plus visible values, NULLs, multi-row matches, several hidden columns in
/// one row and values that are already anonymized.
const DATA: &str = r#"
insert into gha_actors(login, name) values ('alice', 'Alice A.'), ('bob', 'bob'), ('carol', null), ('anon-522b276a356bdf39013dfabea2cd43e141ecc9e8', 'alice'), ('Żółw', 'Żółw');
insert into gha_actors_emails(email) values ('alice'), ('alice@example.com'), ('bob');
insert into gha_actors_names(name) values ('alice'), ('Alice'), ('Żółw');
insert into gha_actors_affiliations(company_name, original_company_name) values ('alice', 'alice'), ('Acme', 'bob'), ('carol', 'Carol Inc');
insert into gha_companies(name) values ('bob'), ('Acme'), ('');
insert into gha_events(dup_actor_login) values ('bob'), ('bob'), ('bob'), ('carol'), ('alice');
insert into gha_payloads(dup_actor_login) values ('alice'), ('dave');
insert into gha_commits(dup_actor_login, dup_author_login, dup_committer_login, author_name, author_email, committer_name, committer_email) values
  ('alice', 'alice', 'alice', 'alice', 'alice', 'alice', 'alice'),
  ('bob', 'carol', 'dave', 'Bob', 'bob', 'Żółw', 'zolw@example.com'),
  ('carol', 'carol', 'carol', 'Carol', 'carol@example.com', 'Carol', 'carol@example.com');
insert into gha_commits_roles(actor_login, actor_name, actor_email) values ('alice', 'Alice', 'alice@example.com'), ('bob', 'bob', 'bob'), ('carol', 'Żółw', 'carol');
insert into gha_pages(dup_actor_login) values ('Żółw'), ('carol');
insert into gha_comments(dup_actor_login, dup_user_login) values ('alice', 'bob'), ('bob', 'alice'), ('carol', 'carol');
insert into gha_reviews(dup_actor_login, dup_user_login) values ('alice', 'alice'), ('dave', 'bob');
insert into gha_issues(dup_actor_login, dup_user_login) values ('alice', 'carol'), ('bob', 'bob'), ('Żółw', 'Żółw'), ('carol', 'alice');
insert into gha_milestones(dup_actor_login, dupn_creator_login) values ('alice', null), ('bob', 'alice'), ('carol', 'carol');
insert into gha_issues_labels(dup_actor_login) values ('alice'), ('alice'), ('bob');
insert into gha_releases(dup_actor_login, dup_author_login) values ('bob', 'alice'), ('carol', 'Żółw');
insert into gha_assets(dup_actor_login, dup_uploader_login) values ('alice', 'bob'), ('carol', 'carol');
insert into gha_pull_requests(dup_actor_login, dup_user_login) values ('alice', 'alice'), ('bob', 'carol'), ('carol', 'bob');
insert into gha_teams(dup_actor_login) values ('alice'), ('carol');
insert into gha_texts(actor_login) values ('alice'), ('bob'), ('Żółw'), ('carol'), ('');
insert into gha_issues_events_labels(actor_login) values ('bob'), ('bob'), ('dave');
"#;

/// Every anonymized table (snapshot order).
const TABLES: &[&str] = &[
    "gha_actors",
    "gha_actors_emails",
    "gha_actors_names",
    "gha_actors_affiliations",
    "gha_companies",
    "gha_events",
    "gha_payloads",
    "gha_commits",
    "gha_commits_roles",
    "gha_pages",
    "gha_comments",
    "gha_reviews",
    "gha_issues",
    "gha_milestones",
    "gha_issues_labels",
    "gha_releases",
    "gha_assets",
    "gha_pull_requests",
    "gha_teams",
    "gha_texts",
    "gha_issues_events_labels",
];

#[derive(Clone)]
enum Yaml {
    Template(&'static str),
    /// The real `cncf/devstats` `projects.yaml` (272 projects).
    Real,
    Missing,
}

struct Case {
    name: &'static str,
    yaml: Yaml,
    /// Project keys whose database is created (with the schema and `DATA`
    /// unless listed in `empty_dbs` / `no_pgcrypto`).
    dbs: Vec<&'static str>,
    empty_dbs: Vec<&'static str>,
    no_pgcrypto: Vec<&'static str>,
    /// `hide/hide.csv` content (`None` — no file).
    csv: Option<&'static str>,
    /// Write `projects.yaml` and `hide/hide.csv` to a `GHA2DB_DATADIR`
    /// directory instead of the current one (no `GHA2DB_LOCAL`).
    datadir: bool,
    env: Vec<(&'static str, String)>,
    args: Vec<&'static str>,
    /// Compare the `Error: '…'` lines (off for yaml syntax errors, whose
    /// wording differs between yaml.v2 and the Rust decoder).
    compare_errors: bool,
}

impl Case {
    fn new(name: &'static str) -> Self {
        Case {
            name,
            yaml: Yaml::Template(YAML),
            dbs: vec!["p1", "p2", "p4"],
            empty_dbs: Vec::new(),
            no_pgcrypto: Vec::new(),
            csv: Some(CSV),
            datadir: false,
            env: Vec::new(),
            args: Vec::new(),
            compare_errors: true,
        }
    }
    fn args(mut self, a: &[&'static str]) -> Self {
        self.args = a.to_vec();
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
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
    fn empty_dbs(mut self, dbs: &[&'static str]) -> Self {
        self.empty_dbs = dbs.to_vec();
        self
    }
    fn no_pgcrypto(mut self, dbs: &[&'static str]) -> Self {
        self.no_pgcrypto = dbs.to_vec();
        self
    }
    fn csv(mut self, c: Option<&'static str>) -> Self {
        self.csv = c;
        self
    }
    fn datadir(mut self) -> Self {
        self.datadir = true;
        self
    }
    fn env(mut self, k: &'static str, v: &str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self.env.push((k, v.to_string()));
        self
    }
}

struct Side {
    dbs: BTreeMap<&'static str, TestDb>,
    /// Database name prefix of the side (masked in the outputs).
    prefix: String,
    out: Outcome,
}

impl Side {
    fn mask(&self, s: &str) -> String {
        s.replace(&self.prefix, "<dbs>_")
    }
    /// All stdout lines (durations and database names masked) as a
    /// multiset.
    fn all_set(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .mask(&mask_go_durations(&self.out.stdout_str()))
            .lines()
            .map(|l| {
                // `GHA2DB_QOUT`: the arguments of the banner's `gha_logs`
                // insert carry `time.Now()` (Go with the monotonic reading).
                if l.starts_with("[1:hide_data 2:") {
                    if let (Some(a), Some(b)) = (l.find(" 3:"), l.find(" 4:")) {
                        if a < b {
                            return format!("{}<time>{}", &l[..a + 3], &l[b..]);
                        }
                    }
                }
                l.to_string()
            })
            .collect();
        lines.sort();
        lines
    }
    /// [`all_set`](Self::all_set) without the banner.
    fn stdout_set(&self) -> Vec<String> {
        self.all_set().into_iter().filter(|l| l != BANNER).collect()
    }
    fn stderr_set(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .out
            .stderr_str()
            .lines()
            .filter(|l| l.starts_with("Error: '") || l.starts_with("PqError: "))
            .map(|l| self.mask(l))
            .collect();
        lines.sort();
        lines
    }
    /// Every table of every database.
    fn data(&self) -> BTreeMap<String, Vec<Vec<String>>> {
        let mut res = BTreeMap::new();
        for (proj, db) in &self.dbs {
            let con = db.conn();
            for t in TABLES {
                let snap = cpg::snapshot(&con, &format!("select * from {t} order by id"), &[]);
                res.insert(format!("{proj}.{t}"), snap.rows);
            }
            con.close();
        }
        res
    }
    /// Values of one column of one table of one project's database.
    fn column(&self, proj: &str, table: &str, column: &str) -> Vec<String> {
        let con = self.dbs[proj].conn();
        let snap = cpg::snapshot(
            &con,
            &format!("select coalesce({column}, '<null>') from {table} order by id"),
            &[],
        );
        con.close();
        snap.column(0)
    }
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let prefix = format!("dbtest_hide_{}_{}_", case.name, suffix);
    let schema = fs::read_to_string(fixture("hide_data/schema.sql")).unwrap();
    let schema_no_pgcrypto = schema.replace("create extension if not exists pgcrypto;", "");
    let mut dbs = BTreeMap::new();
    for proj in &case.dbs {
        let db = TestDb::fresh(&format!("hide_{}_{}_{}", case.name, suffix, proj))?;
        if case.no_pgcrypto.contains(proj) {
            db.exec(&schema_no_pgcrypto);
        } else {
            db.exec(&schema);
        }
        if !case.empty_dbs.contains(proj) {
            db.exec(DATA);
        }
        dbs.insert(*proj, db);
    }
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_hide_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    let files = if case.datadir {
        dir.path().join("datadir")
    } else {
        dir.path().to_path_buf()
    };
    fs::create_dir_all(files.join("hide")).unwrap();
    let db_name = |key: &str| format!("{prefix}{key}");
    match &case.yaml {
        Yaml::Template(t) => {
            let mut text = t.to_string();
            while let Some(start) = text.find("{db:") {
                let end = text[start..].find('}').unwrap() + start;
                let key = text[start + 4..end].to_string();
                text.replace_range(start..=end, &db_name(&key));
            }
            fs::write(files.join("projects.yaml"), text).unwrap();
        }
        Yaml::Real => {
            fs::copy(
                fixture("devstats/projects.yaml"),
                files.join("projects.yaml"),
            )
            .unwrap();
        }
        Yaml::Missing => {}
    }
    if let Some(c) = case.csv {
        fs::write(files.join("hide/hide.csv"), c).unwrap();
    }

    let ctx = cpg::test_ctx();
    let mut env: Vec<(String, String)> = vec![
        ("PG_HOST".into(), ctx.pg_host.clone()),
        ("PG_PORT".into(), ctx.pg_port.clone()),
        ("PG_USER".into(), ctx.pg_user.clone()),
        ("PG_PASS".into(), ctx.pg_pass.clone()),
        ("PG_SSL".into(), ctx.pg_ssl.clone()),
        ("PG_DB".into(), cpg::GUARD_DB.to_string()),
        ("GHA2DB_SKIPLOG".into(), "1".into()),
        ("GHA2DB_SKIPTIME".into(), "1".into()),
    ];
    if case.datadir {
        env.push((
            "GHA2DB_DATADIR".into(),
            format!("{}/", files.to_string_lossy()),
        ));
    } else {
        env.push(("GHA2DB_LOCAL".into(), "1".into()));
    }
    for (k, v) in &case.env {
        env.retain(|(key, _)| key != k);
        env.push((k.to_string(), v.clone()));
    }
    let mut inv = Invocation::new().cwd(dir.path().to_path_buf());
    for (k, v) in &env {
        inv = inv.env(leak(k), leak(v));
    }
    for a in &case.args {
        inv = inv.arg(*a);
    }
    let out = run(bin, &inv);
    Some(Side { dbs, prefix, out })
}

/// Run both binaries and compare everything; returns the Rust side for
/// further assertions (`None` when the DB tests are skipped).
fn both(case: &Case) -> Option<Side> {
    let rust = run_side(&rust_bin(), case, "rs")?;
    if let Some(go) = go_bin() {
        let go = run_side(&go, case, "go").unwrap();
        let ctx = format!(
            "\ncase {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
            case.name,
            case.env,
            go.out.code,
            go.out.stdout_str(),
            go.out.stderr_str(),
            rust.out.code,
            rust.out.stdout_str(),
            rust.out.stderr_str(),
        );
        assert_eq!(go.out.code, rust.out.code, "exit code{ctx}");
        assert_eq!(go.all_set(), rust.all_set(), "stdout{ctx}");
        if case.compare_errors {
            assert_eq!(go.stderr_set(), rust.stderr_set(), "stderr{ctx}");
        } else {
            assert_eq!(
                go.stderr_set().len(),
                rust.stderr_set().len(),
                "stderr line count{ctx}"
            );
        }
        // The banner and `Processing databases:` always come first, `Time:`
        // last.
        let first = |o: &Outcome| {
            mask_go_durations(&o.stdout_str())
                .lines()
                .take(2)
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        let last = |o: &Outcome| o.stdout_str().lines().last().map(str::to_string);
        assert_eq!(
            first(&go.out)
                .iter()
                .map(|l| go.mask(l))
                .collect::<Vec<_>>(),
            first(&rust.out)
                .iter()
                .map(|l| rust.mask(l))
                .collect::<Vec<_>>(),
            "first lines{ctx}"
        );
        if go.out.code == Some(0) {
            assert!(
                last(&go.out).is_some_and(|l| l.starts_with("Time: ")),
                "last go line{ctx}"
            );
            assert!(
                last(&rust.out).is_some_and(|l| l.starts_with("Time: ")),
                "last rust line{ctx}"
            );
        }
        assert_eq!(go.data(), rust.data(), "database contents{ctx}");
    }
    Some(rust)
}

fn anon(sha: &str) -> String {
    format!("anon-{sha}")
}

#[test]
fn db_basic_all_columns() {
    let Some(rs) = both(&Case::new("basic")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.stdout_set();
    assert!(lines.contains(&"Processing databases: [<dbs>_p1 <dbs>_p2 <dbs>_p4]".to_string()));
    // 3 databases × the same seed → the same report lines three times.
    let p1: Vec<&String> = lines
        .iter()
        .filter(|l| l.starts_with("DB: <dbs>_p1,"))
        .collect();
    let p2: Vec<String> = lines
        .iter()
        .filter(|l| l.starts_with("DB: <dbs>_p2,"))
        .map(|l| l.replace("<dbs>_p2", "<dbs>_p1"))
        .collect();
    assert_eq!(
        p1.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        p2.iter().map(String::as_str).collect::<Vec<_>>()
    );
    let expect = |table: &str, column: &str, sha: &str, n: i64| {
        format!("DB: <dbs>_p1, table: {table}, column: {column}, sha: {sha}, updated {n} rows")
    };
    for l in [
        expect("gha_actors", "login", SHA_ALICE, 1),
        expect("gha_actors", "login", SHA_BOB, 1),
        expect("gha_actors", "login", SHA_ZOLW, 1),
        expect("gha_actors", "name", SHA_ALICE, 1),
        expect("gha_actors", "name", SHA_BOB, 1),
        expect("gha_actors", "name", SHA_ZOLW, 1),
        expect("gha_actors_emails", "email", SHA_ALICE, 1),
        expect("gha_actors_emails", "email", SHA_BOB, 1),
        expect("gha_events", "dup_actor_login", SHA_BOB, 3),
        expect("gha_events", "dup_actor_login", SHA_ALICE, 1),
        expect("gha_commits", "committer_name", SHA_ZOLW, 1),
        expect("gha_commits", "author_email", SHA_ALICE, 1),
        expect("gha_milestones", "dupn_creator_login", SHA_ALICE, 1),
        expect("gha_issues_labels", "dup_actor_login", SHA_ALICE, 2),
        expect("gha_issues_events_labels", "actor_login", SHA_BOB, 2),
        expect("gha_texts", "actor_login", SHA_ZOLW, 1),
    ] {
        assert!(lines.contains(&l), "missing {l:?} in {lines:#?}");
    }
    // No line about visible values or NULLs.
    assert!(!lines
        .iter()
        .any(|l| l.contains(SHA_CAROL) || l.contains(SHA_DAVE)));
    // The data: hidden values replaced, the rest untouched.
    assert_eq!(
        rs.column("p1", "gha_actors", "login"),
        vec![
            anon(SHA_ALICE),
            anon(SHA_BOB),
            "carol".to_string(),
            anon(SHA_ALICE),
            anon(SHA_ZOLW)
        ]
    );
    assert_eq!(
        rs.column("p1", "gha_actors", "name"),
        vec![
            "Alice A.".to_string(),
            anon(SHA_BOB),
            "<null>".to_string(),
            anon(SHA_ALICE),
            anon(SHA_ZOLW)
        ]
    );
    assert_eq!(
        rs.column("p4", "gha_events", "dup_actor_login"),
        vec![
            anon(SHA_BOB),
            anon(SHA_BOB),
            anon(SHA_BOB),
            "carol".to_string(),
            anon(SHA_ALICE)
        ]
    );
    assert_eq!(
        rs.column("p2", "gha_milestones", "dupn_creator_login"),
        vec!["<null>".to_string(), anon(SHA_ALICE), "carol".to_string()]
    );
    assert_eq!(
        rs.column("p2", "gha_texts", "actor_login"),
        vec![
            anon(SHA_ALICE),
            anon(SHA_BOB),
            anon(SHA_ZOLW),
            "carol".to_string(),
            "".to_string()
        ]
    );
}

#[test]
fn db_single_thread() {
    let Some(rs) = both(&Case::new("st").env("GHA2DB_ST", "1")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    // Rust walks the projects in order and the SHA1 map sorted: the report is
    // deterministic (`sha` values ascending within a database).
    let lines: Vec<String> = rs
        .out
        .stdout_str()
        .lines()
        .filter(|l| l.starts_with("DB: "))
        .map(str::to_string)
        .collect();
    let mut prev_db = String::new();
    let mut prev_sha = String::new();
    for l in &lines {
        let db = l.split(',').next().unwrap().to_string();
        let sha = l
            .split("sha: ")
            .nth(1)
            .unwrap()
            .split(',')
            .next()
            .unwrap()
            .to_string();
        if db == prev_db {
            assert!(sha >= prev_sha, "{l}");
        }
        prev_db = db;
        prev_sha = sha;
    }
}

#[test]
fn db_ncpus() {
    let Some(rs) = both(&Case::new("ncpus").env("GHA2DB_NCPUS", "2")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let Some(rs) = both(&Case::new("ncpusbig").env("GHA2DB_NCPUS", "4096")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
}

#[test]
fn db_only() {
    let Some(rs) = both(&Case::new("only").env("ONLY", " p4  p3 nosuch ")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.stdout_set();
    assert!(lines.contains(&"Processing databases: [<dbs>_p4]".to_string()));
    assert!(lines.iter().all(|l| !l.starts_with("DB: <dbs>_p1")));
    // p1 untouched, p4 anonymized.
    assert_eq!(
        rs.column("p1", "gha_teams", "dup_actor_login"),
        vec!["alice", "carol"]
    );
    assert_eq!(
        rs.column("p4", "gha_teams", "dup_actor_login"),
        vec![anon(SHA_ALICE), "carol".to_string()]
    );
}

#[test]
fn db_only_nothing_matches() {
    let Some(rs) = both(&Case::new("onlynone").env("ONLY", "nosuch")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.stdout_set(),
        vec!["Processing databases: []", "Time: <duration>"]
    );
}

#[test]
fn db_no_hide_csv() {
    let Some(rs) = both(&Case::new("nocsv").csv(None)) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.stdout_set(),
        vec![
            "Processing databases: [<dbs>_p1 <dbs>_p2 <dbs>_p4]",
            "Time: <duration>"
        ]
    );
    assert_eq!(
        rs.column("p1", "gha_teams", "dup_actor_login"),
        vec!["alice", "carol"]
    );
}

#[test]
fn db_header_only_csv() {
    let Some(rs) = both(&Case::new("hdr").csv(Some("sha1\n"))) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.stdout_set(),
        vec![
            "Processing databases: [<dbs>_p1 <dbs>_p2 <dbs>_p4]",
            "Time: <duration>"
        ]
    );
}

#[test]
fn db_no_matching_data() {
    let Some(rs) = both(&Case::new("nomatch").csv(Some(
        "sha1\n0000000000000000000000000000000000000000\nnot-a-sha\n",
    ))) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.stdout_set(),
        vec![
            "Processing databases: [<dbs>_p1 <dbs>_p2 <dbs>_p4]",
            "Time: <duration>"
        ]
    );
}

#[test]
fn db_empty_databases() {
    let Some(rs) = both(&Case::new("empty").empty_dbs(&["p1", "p2", "p4"])) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.stdout_set(),
        vec![
            "Processing databases: [<dbs>_p1 <dbs>_p2 <dbs>_p4]",
            "Time: <duration>"
        ]
    );
}

#[test]
fn db_second_run_is_a_no_op() {
    let case = Case::new("twice");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    // Run the Rust binary again against its (already anonymized) databases.
    let ctx = cpg::test_ctx();
    let dir = tempfile::Builder::new()
        .prefix("g2r_hide_twice2_")
        .tempdir()
        .unwrap();
    fs::create_dir(dir.path().join("hide")).unwrap();
    fs::write(dir.path().join("hide/hide.csv"), CSV).unwrap();
    let mut yaml = YAML.to_string();
    for p in ["p1", "p2", "p3", "p4"] {
        yaml = yaml.replace(&format!("{{db:{p}}}"), &format!("{}{p}", rs.prefix));
    }
    fs::write(dir.path().join("projects.yaml"), yaml).unwrap();
    let inv = Invocation::new()
        .cwd(dir.path().to_path_buf())
        .env("PG_HOST", leak(&ctx.pg_host))
        .env("PG_PORT", leak(&ctx.pg_port))
        .env("PG_USER", leak(&ctx.pg_user))
        .env("PG_PASS", leak(&ctx.pg_pass))
        .env("PG_SSL", leak(&ctx.pg_ssl))
        .env("PG_DB", cpg::GUARD_DB)
        .env("GHA2DB_SKIPLOG", "1")
        .env("GHA2DB_SKIPTIME", "1")
        .env("GHA2DB_LOCAL", "1");
    let out = run(&rust_bin(), &inv);
    assert_eq!(out.code, Some(0), "{}", out.stderr_str());
    let lines: Vec<String> = mask_go_durations(&out.stdout_str())
        .lines()
        .filter(|l| *l != BANNER)
        .map(|l| rs.mask(l))
        .collect();
    assert_eq!(
        lines,
        vec![
            "Processing databases: [<dbs>_p1 <dbs>_p2 <dbs>_p4]",
            "Time: <duration>"
        ]
    );
}

#[test]
fn db_disabled_project_skipped_and_projects_override() {
    // p3 is disabled in the yaml; `GHA2DB_PROJECTS_OVERRIDE` enables it and
    // disables p2.
    let Some(rs) = both(
        &Case::new("override")
            .dbs(&["p1", "p2", "p3", "p4"])
            .env("GHA2DB_PROJECTS_OVERRIDE", "+p3,-p2"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.stdout_set();
    assert!(lines.contains(&"Processing databases: [<dbs>_p1 <dbs>_p3 <dbs>_p4]".to_string()));
    assert_eq!(
        rs.column("p2", "gha_teams", "dup_actor_login"),
        vec!["alice", "carol"]
    );
    assert_eq!(
        rs.column("p3", "gha_teams", "dup_actor_login"),
        vec![anon(SHA_ALICE), "carol".to_string()]
    );
}

#[test]
fn db_duplicate_order_keeps_both_projects() {
    // Bug 19: projects sharing an `order` were dropped/duplicated.
    let Some(rs) = both(
        &Case::new("duporder")
            .yaml(Yaml::Template(YAML_DUP_ORDER))
            .dbs(&["zeta", "alpha", "first"]),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.stdout_set();
    assert!(
        lines.contains(&"Processing databases: [<dbs>_first <dbs>_alpha <dbs>_zeta]".to_string())
    );
    assert!(
        lines.contains(&"Warning: projects 'alpha' and 'zeta' have the same order 7".to_string())
    );
    for p in ["zeta", "alpha", "first"] {
        assert_eq!(
            rs.column(p, "gha_teams", "dup_actor_login"),
            vec![anon(SHA_ALICE), "carol".to_string()]
        );
    }
}

#[test]
fn db_datadir_mode() {
    // No `GHA2DB_LOCAL`: `projects.yaml` from `GHA2DB_DATADIR`, `hide/hide.csv`
    // from `GHA2DB_DATADIR/hide/hide.csv` (the current directory has none).
    let Some(rs) = both(&Case::new("datadir").datadir()) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.column("p1", "gha_teams", "dup_actor_login"),
        vec![anon(SHA_ALICE), "carol".to_string()]
    );
}

#[test]
fn db_custom_yaml_name() {
    let Some(rs) = both(
        &Case::new("yamlname")
            .env("GHA2DB_PROJECTS_YAML", "other.yaml")
            .yaml(Yaml::Missing),
    ) else {
        return;
    };
    // `projects.yaml` was never written and `other.yaml` does not exist.
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.stderr_set(),
        vec!["Error: 'open ./other.yaml: no such file or directory'"]
    );
    assert_eq!(rs.stdout_set(), Vec::<String>::new());
}

#[test]
fn db_missing_projects_yaml() {
    let Some(rs) = both(&Case::new("noyaml").yaml(Yaml::Missing)) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.stderr_set(),
        vec!["Error: 'open ./projects.yaml: no such file or directory'"]
    );
    // Not local: `/etc/gha2db/projects.yaml`.
    let Some(rs) = both(
        &Case::new("noyamletc")
            .yaml(Yaml::Missing)
            .env("GHA2DB_LOCAL", ""),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.stderr_set(),
        vec!["Error: 'open /etc/gha2db/projects.yaml: no such file or directory'"]
    );
}

#[test]
fn db_malformed_yaml() {
    // The error wording differs (yaml.v2 vs the Rust decoder); both fail
    // with one error before touching any database.
    let Some(rs) = both(
        &Case::new("badyaml")
            .yaml(Yaml::Template("projects: [1, 2\n"))
            .code_only_errors(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(rs.stderr_set().len(), 1, "{:?}", rs.stderr_set());
    assert!(
        rs.stderr_set()[0].starts_with("Error: 'yaml: "),
        "{:?}",
        rs.stderr_set()
    );
    assert_eq!(rs.stdout_set(), Vec::<String>::new());
    let Some(rs) = both(
        &Case::new("badyaml2")
            .yaml(Yaml::Template(
                "projects:\n  p1:\n    order: notanumber\n    psql_db: x\n",
            ))
            .code_only_errors(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(rs.stderr_set().len(), 1, "{:?}", rs.stderr_set());
    assert_eq!(
        rs.column("p1", "gha_teams", "dup_actor_login"),
        vec!["alice", "carol"]
    );
}

#[test]
fn db_empty_yaml() {
    let Some(rs) = both(&Case::new("emptyyaml").yaml(Yaml::Template("---\n"))) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.stdout_set(),
        vec!["Processing databases: []", "Time: <duration>"]
    );
}

#[test]
fn db_missing_database_is_fatal() {
    // p2's database does not exist: the first statement fails, which is not
    // retryable → fatal. Single thread and one SHA1 keep the output ordered.
    let Some(rs) = both(
        &Case::new("nodb")
            .dbs(&["p1", "p4"])
            .csv(Some("sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n"))
            .env("GHA2DB_ST", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    let err = rs.stderr_set();
    assert!(
        err.contains(&"Error: 'pq: database \"<dbs>_p2\" does not exist'".to_string()),
        "{err:?}"
    );
    assert!(
        err.contains(&"PqError: code=3D000, name=invalid_catalog_name, detail=".to_string()),
        "{err:?}"
    );
    // p1 was processed before p2 (in order), p4 never reached.
    assert_eq!(
        rs.column("p1", "gha_teams", "dup_actor_login"),
        vec![anon(SHA_ALICE), "carol".to_string()]
    );
    assert_eq!(
        rs.column("p4", "gha_teams", "dup_actor_login"),
        vec!["alice", "carol"]
    );
    let lines = rs.stdout_set();
    assert!(lines.iter().any(|l| l.starts_with("Failed sql: update gha_actors set login = $1 where encode(digest(login, 'sha1'), 'hex') = $2")), "{lines:#?}");
}

#[test]
fn db_without_pgcrypto_is_fatal() {
    let Some(rs) = both(
        &Case::new("nocrypto")
            .yaml(Yaml::Template(YAML_P1))
            .dbs(&["p1"])
            .no_pgcrypto(&["p1"])
            .csv(Some("sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n"))
            .env("GHA2DB_ST", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    let err = rs.stderr_set();
    assert!(
        err.contains(
            &"Error: 'pq: function digest(character varying, unknown) does not exist'".to_string()
        ),
        "{err:?}"
    );
    assert!(
        err.contains(&"PqError: code=42883, name=undefined_function, detail=".to_string()),
        "{err:?}"
    );
}

#[test]
fn db_unreachable_server_is_fatal() {
    let Some(rs) = both(
        &Case::new("noserver")
            .yaml(Yaml::Template(YAML_P1))
            .dbs(&["p1"])
            .csv(Some("sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n"))
            .env("PG_PORT", "1")
            .env("GHA2DB_ST", "1")
            .env("GHA2DB_TRIALS", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    // A refused connection is not one of the retried conditions: fatal at
    // the first statement.
    assert_eq!(
        rs.stderr_set(),
        vec!["Error: 'dial tcp 127.0.0.1:1: connect: connection refused'"]
    );
    assert_eq!(
        rs.column("p1", "gha_teams", "dup_actor_login"),
        vec!["alice", "carol"]
    );
}

#[test]
fn db_qout_prints_connection_strings_and_queries() {
    let Some(rs) = both(
        &Case::new("qout")
            .yaml(Yaml::Template(YAML_P1))
            .dbs(&["p1"])
            .csv(Some("sha1\n522b276a356bdf39013dfabea2cd43e141ecc9e8\n"))
            .env("GHA2DB_QOUT", "1")
            .env("GHA2DB_ST", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let text = rs.mask(&rs.out.stdout_str());
    assert!(
        text.contains("ConnectString: client_encoding=UTF8 sslmode='disable' host='"),
        "{text}"
    );
    assert!(text.contains("dbname='<dbs>_p1'"), "{text}");
    assert!(
        text.contains(
            "update gha_actors set login = $1 where encode(digest(login, 'sha1'), 'hex') = $2"
        ),
        "{text}"
    );
    assert!(text.contains(&format!("anon-{SHA_ALICE}")), "{text}");
    // 39 update statements for the single task.
    assert_eq!(
        text.lines().filter(|l| l.starts_with("update ")).count(),
        39,
        "{text}"
    );
}

#[test]
fn db_extra_argument_switches_to_file_mode() {
    // With an argument the databases are never touched (and the SHA1 of the
    // argument is appended to the hide file).
    let Some(rs) = both(
        &Case::new("filemode")
            .yaml(Yaml::Template(YAML_P1))
            .dbs(&["p1"])
            .args(&["dave"]),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(rs.stdout_set(), vec!["Time: <duration>"]);
    assert_eq!(
        rs.column("p1", "gha_teams", "dup_actor_login"),
        vec!["alice", "carol"]
    );
}

#[test]
fn db_real_projects_yaml_without_hidden_shas() {
    // The real `projects.yaml` (254 enabled projects, every `order` unique
    // since 2026-09-13 — agones/kaischeduler used to share 245): without
    // SHA1s to hide no database is touched — the database list in order is
    // all there is to compare (and no bug 19 warning any more).
    let Some(rs) = both(&Case::new("real").yaml(Yaml::Real).dbs(&[]).csv(None)) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.stdout_set();
    assert_eq!(lines.len(), 2, "{lines:#?}");
    let dbs = lines
        .iter()
        .find(|l| l.starts_with("Processing databases: ["))
        .unwrap();
    assert!(
        dbs.starts_with("Processing databases: [gha prometheus fluentd linkerd grpc "),
        "{dbs}"
    );
    assert!(dbs.ends_with(" sdc allprj]"), "{dbs}");
    assert_eq!(dbs.split(' ').count() - 2, 254, "{dbs}");
    assert!(!lines.iter().any(|l| l.contains("have the same order")));
    // `ONLY` with the real file.
    let Some(rs) = both(
        &Case::new("realonly")
            .yaml(Yaml::Real)
            .dbs(&[])
            .csv(None)
            .env("ONLY", "kubernetes all prometheus nosuch"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs
        .stdout_set()
        .contains(&"Processing databases: [gha prometheus allprj]".to_string()));
}
