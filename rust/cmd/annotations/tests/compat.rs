//! Go ⇄ Rust compatibility tests for `annotations`.
//!
//! Every case gets, per side, a scratch database
//! (`dbtest_annotations_<case>_<go|rs>`, optionally a second one for the
//! project's `shared_db`) and a scratch directory: the binaries run in
//! `<dir>/work` with `GHA2DB_LOCAL=1` (`./projects.yaml` written from the
//! case's yaml) and `GHA2DB_LOCAL_CMD=1` (`./git/git_tags.sh` — the real
//! script from `../devstats/git/`, a stand-in printing what a broken `git`
//! would, or nothing) unless a case says otherwise; the project's main
//! repository is a real git repository built under `<dir>/repos/org/repo`
//! (`GHA2DB_REPOS_DIR`) with lightweight and annotated tags at fixed dates.
//! `TZ=UTC` for both binaries unless a case tests another zone (Go stamps
//! tags in local time and relabels the wall clock as UTC — the port does the
//! same).
//!
//! Compared per run: exit code, stdout (durations, the `added` stamp of the
//! debug point lines and time zone names masked; the per-table DDL lines Go
//! prints in map order as a sorted multiset), the `Error: '…'` stderr lines
//! (or only their count where the wording legitimately differs) and
//! afterwards the `sannotations` / `tquick_ranges` tables (columns, indexes,
//! rows) of the project database plus `sannotations_shared` of the shared
//! one.
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise) and `git` on the PATH.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{fixture, go_binary, run, rust_binary, Invocation, Outcome};
use devstatscode::chrono::{Duration, Utc};
use regex::Regex;
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("annotations")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_annotations"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";

/// Prefixes of the lines Go prints per series table in map iteration order.
const UNORDERED_PREFIXES: &[&str] = &[
    "Ignored grant select on",
    "create table if not exists",
    "create index if not exists",
    "grant select on",
    "alter table",
];

/// `NewTSPoint: <time> <added> <name> …` / `AddTSPoint: …` debug lines: the
/// `added` stamp is `now()`.
static ADDED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^((?:NewTSPoint:|AddTSPoint:) \d{4}-\d{2}-\d{2} \d+) \d{4}-\d{2}-\d{2} \d+ ")
        .unwrap()
});
/// `Got 3 tags for org/repo, took 1.2ms`.
static TOOK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r", took \S+$").unwrap());
/// Go's `%v` of a time: `+0200 CEST`; the port has no zone names
/// (`+0200 +0200`).
static ZONE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"([+-]\d{4}) (?:[A-Z]{3,5}|[+-]\d{4})\b").unwrap());

/// A git tag of the test repository.
#[derive(Clone)]
struct Tag {
    name: &'static str,
    /// Creator date (ISO 8601 with zone).
    date: &'static str,
    /// Tag message (annotated) or commit subject (lightweight).
    msg: &'static str,
    annotated: bool,
}

/// An annotated tag.
fn tag(name: &'static str, date: &'static str, msg: &'static str) -> Tag {
    Tag {
        name,
        date,
        msg,
        annotated: true,
    }
}

/// A lightweight tag: its creator date and subject are the commit's.
fn light(name: &'static str, date: &'static str, msg: &'static str) -> Tag {
    Tag {
        name,
        date,
        msg,
        annotated: false,
    }
}

/// The default repository: lightweight + annotated tags, a message longer
/// than 40 bytes with a multi-byte character, a second tag in the same hour
/// (skipped), a tag from before 2012-07-01 (skipped), a tag not matching the
/// usual regexp, a multi-line message with a tab and tags listed by git in
/// name order but created out of date order.
fn default_tags() -> Vec<Tag> {
    vec![
        tag("old", "2011-01-01T00:00:00Z", "too old"),
        light("v1.0", "2015-08-01T10:15:00Z", "first"),
        tag(
            "v1.1",
            "2015-09-01T10:15:00Z",
            "Release v1.1 — with a very long message exceeding forty bytes for sure",
        ),
        tag("v1.1.1", "2015-09-01T10:45:00Z", "same hour"),
        tag("rc-2.0", "2016-01-05T00:00:00Z", "multi\nline\tmsg"),
        tag("v2.0", "2016-02-05T00:00:00Z", "multi\nline\tmsg"),
        light(
            "v0.9",
            "2015-07-15T23:59:59Z",
            "sorted by date, not by name",
        ),
    ]
}

fn git(repo: &Path, date: &str, args: &[&str]) {
    let out = Command::new("git")
        .args([
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
            "-c",
            "init.defaultBranch=main",
            "-c",
            "user.name=DevStats",
            "-c",
            "user.email=devstats@example.com",
        ])
        .args(args)
        .current_dir(repo)
        // Never pick up the developer's git configuration (signing keys …).
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_DATE", date)
        .env("GIT_COMMITTER_DATE", date)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap_or_else(|e| panic!("cannot run git: {e}"));
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Build a repository with one commit per tag (so every tag — lightweight
/// ones included — carries its own creator date and subject).
fn build_repo(repo: &Path, tags: &[Tag]) {
    fs::create_dir_all(repo).unwrap();
    git(repo, "2012-01-01T00:00:00Z", &["init", "-q"]);
    for (i, t) in tags.iter().enumerate() {
        fs::write(repo.join("file"), format!("{i}\n")).unwrap();
        git(repo, t.date, &["add", "file"]);
        let commit_msg = if t.annotated {
            format!("commit {i}")
        } else {
            t.msg.to_string()
        };
        git(repo, t.date, &["commit", "-q", "-m", &commit_msg]);
        if t.annotated {
            git(repo, t.date, &["tag", "-a", t.name, "-m", t.msg]);
        } else {
            git(repo, t.date, &["tag", t.name]);
        }
    }
}

/// Where `git_tags.sh` comes from.
#[derive(Clone)]
enum Script {
    /// The real script (`compat/fixtures/annotations/git_tags.sh`).
    Real,
    /// A stand-in `sh` script body printing what a broken git would.
    Fake(&'static str),
    /// No script at all.
    Missing,
}

/// One step of a case.
#[derive(Clone)]
enum Step {
    /// Run both binaries with extra environment and compare.
    Run(Vec<(&'static str, &'static str)>),
    /// Execute SQL on the side's project database.
    Sql(&'static str),
}

struct Case {
    name: &'static str,
    /// `projects.yaml` body (`{shared}` → the side's shared database name).
    yaml: &'static str,
    /// Tags of `repos/org/repo` (`None`: no repository at all).
    tags: Option<Vec<Tag>>,
    script: Script,
    /// `GHA2DB_LOCAL=1` (`./projects.yaml`) — off for the `GHA2DB_DATADIR`
    /// case (`<dir>/data/projects.yaml`).
    local: bool,
    /// `GHA2DB_LOCAL_CMD=1` (`./git/git_tags.sh`) — off for the PATH lookup
    /// case (`<dir>/bin/git_tags.sh`).
    local_cmd: bool,
    /// Create the side's shared database beforehand.
    shared: bool,
    /// Environment of every run (`{dir}` expands to the scratch directory).
    env: Vec<(&'static str, &'static str)>,
    steps: Vec<Step>,
    /// Compare stdout at all.
    stdout: bool,
    /// Compare the `Error: '…'` lines (off when their wording legitimately
    /// differs).
    compare_errors: bool,
    /// With `stdout` off: both sides must still print an `Error: '…'` line
    /// (off where Go panics without one).
    require_error: bool,
    /// Do not write `projects.yaml` at all.
    no_yaml: bool,
}

impl Case {
    fn new(name: &'static str, yaml: &'static str) -> Self {
        Case {
            name,
            yaml,
            tags: Some(default_tags()),
            script: Script::Real,
            local: true,
            local_cmd: true,
            shared: false,
            env: vec![("GHA2DB_PROJECT", "proj"), ("TZ", "UTC")],
            steps: vec![Step::Run(Vec::new())],
            stdout: true,
            compare_errors: true,
            require_error: true,
            no_yaml: false,
        }
    }
    fn tags(mut self, tags: Vec<Tag>) -> Self {
        self.tags = Some(tags);
        self
    }
    fn no_repo(mut self) -> Self {
        self.tags = None;
        self
    }
    fn script(mut self, script: Script) -> Self {
        self.script = script;
        self
    }
    fn env(mut self, k: &'static str, v: &'static str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self.env.push((k, v));
        self
    }
    fn no_env(mut self, k: &'static str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self
    }
    fn debug(self) -> Self {
        self.env("GHA2DB_DEBUG", "1")
    }
    fn no_local(mut self) -> Self {
        self.local = false;
        self
    }
    fn path_mode(mut self) -> Self {
        self.local_cmd = false;
        self
    }
    fn shared(mut self) -> Self {
        self.shared = true;
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
    /// Only the exit code (Go panics without an `Error: '…'` line).
    fn code_only_no_error(mut self) -> Self {
        self.stdout = false;
        self.compare_errors = false;
        self.require_error = false;
        self
    }
    fn no_yaml(mut self) -> Self {
        self.no_yaml = true;
        self
    }
}

/// A `projects.yaml` with one project `proj` having the given extra fields.
fn proj(fields: &[&str]) -> &'static str {
    let mut y = String::from(
        "projects:\n  proj:\n    name: Proj\n    command_line: [ \"x\" ]\n    psql_db: proj\n    order: 1\n",
    );
    for f in fields {
        y.push_str("    ");
        y.push_str(f);
        y.push('\n');
    }
    leak(&y)
}

const MAIN_REPO: &str = "main_repo: org/repo";
const REGEXP: &str = r"annotation_regexp: '^v\d+\.\d+(\.\d+)?$'";
const START: &str = "start_date: 2015-06-01T00:00:00Z";
const JOIN: &str = "join_date: 2015-10-01T00:00:00Z";
const INCUBATING: &str = "incubating_date: 2016-03-01T00:00:00Z";
const GRADUATED: &str = "graduated_date: 2017-01-01T00:00:00Z";
const ARCHIVED: &str = "archived_date: 2019-05-01T00:00:00Z";

/// One table: columns (sorted by name), indexes and rows (every column as
/// text, ordered by all columns).
type TableDump = (Vec<cpg::ColumnInfo>, Vec<cpg::IndexInfo>, Vec<Vec<String>>);

struct Side {
    db: TestDb,
    shared: Option<TestDb>,
    _dir: TempDir,
    /// The scratch directory path (masked as `<dir>` in stdout).
    dir_str: String,
    outs: Vec<Outcome>,
}

fn unordered_line(l: &str) -> bool {
    UNORDERED_PREFIXES.iter().any(|p| l.starts_with(p))
}

/// Go builds the `create table if not exists "x"(…)` statement from a map,
/// so the column definitions come in random order: sort them (splitting on
/// top-level commas only — `primary key(time, period)` stays intact).
fn normalize_ddl(l: &str) -> String {
    if !l.starts_with("create table if not exists") {
        return l.to_string();
    }
    let Some(open) = l.find('(') else {
        return l.to_string();
    };
    let Some(close) = l.rfind(')') else {
        return l.to_string();
    };
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = open + 1;
    let body = &l[..close];
    for (i, c) in body.char_indices().skip(open + 1) {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(body[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(body[start..].trim().to_string());
    parts.sort();
    format!("{}({}){}", &l[..open], parts.join(", "), &l[close + 1..])
}

impl Side {
    fn mask(&self, l: &str) -> String {
        let l = l.replace(&self.dir_str, "<dir>");
        if l.starts_with("Time: ") {
            return "Time: <masked>".to_string();
        }
        let l = ADDED.replace(&l, "$1 <added> ");
        let l = TOOK.replace(&l, ", took <dur>");
        let l = ZONE.replace_all(&l, "$1 <zone>");
        if let Some(sh) = &self.shared {
            return l.replace(&sh.name, "<shared>");
        }
        l.into_owned()
    }
    /// stdout of run `i` split into the ordered lines and the sorted
    /// multiset of the order-free lines.
    fn stdout(&self, i: usize) -> (Vec<String>, Vec<String>) {
        let mut ordered = Vec::new();
        let mut unordered = Vec::new();
        for l in self.outs[i].stdout_str().lines() {
            let l = self.mask(l);
            if unordered_line(&l) {
                unordered.push(normalize_ddl(&l));
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
    fn tables(&self) -> Vec<String> {
        let con = self.db.conn();
        let t = cpg::tables(&con);
        con.close();
        t
    }
    fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let con = self.db.conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows
    }
    /// `(time, title, description)` of `sannotations` ordered by time
    /// (empty when the table was never created).
    fn annotations(&self) -> Vec<(String, String, String)> {
        if !self.tables().iter().any(|t| t == "sannotations") {
            return Vec::new();
        }
        self.query("select time::text, title, description from sannotations order by time")
            .into_iter()
            .map(|r| (r[0].clone(), r[1].clone(), r[2].clone()))
            .collect()
    }
    /// `(time, suffix, name, data)` of `tquick_ranges` ordered by time.
    fn ranges(&self) -> Vec<(String, String, String, String)> {
        self.query(
            "select time::text, quick_ranges_suffix, quick_ranges_name, quick_ranges_data from tquick_ranges order by time",
        )
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone(), r[2].clone(), r[3].clone()))
        .collect()
    }
    /// The quick range suffixes in time order.
    fn suffixes(&self) -> Vec<String> {
        self.ranges().into_iter().map(|r| r.1).collect()
    }
    /// The quick range with `suffix`: `(name, data)`.
    fn range(&self, suffix: &str) -> Option<(String, String)> {
        self.ranges()
            .into_iter()
            .find(|r| r.1 == suffix)
            .map(|r| (r.2, r.3))
    }
    /// `(time, period, title, description, repo)` of `sannotations_shared`
    /// in the shared database (`None` when it has no such table).
    fn shared_annotations(&self) -> Option<Vec<Vec<String>>> {
        let sh = self.shared.as_ref()?;
        let con = sh.conn();
        let has = cpg::tables(&con).iter().any(|t| t == "sannotations_shared");
        let rows = has.then(|| {
            cpg::snapshot(
                &con,
                "select time::text, period, title, description, repo from sannotations_shared order by time, title",
                &[],
            )
            .rows
        });
        con.close();
        rows
    }
    /// Every table of the project database with its structure and contents
    /// (the scratch directory masked), plus (when present) the shared
    /// database's tables under `shared.`.
    fn data(&self) -> BTreeMap<String, TableDump> {
        fn dump(
            con: &devstatscode::pg::PgConn,
            prefix: &str,
            dir: &str,
            res: &mut BTreeMap<String, TableDump>,
        ) {
            for t in cpg::tables(con) {
                let mut cols = cpg::table_columns(con, &t);
                cols.sort();
                let idx = cpg::table_indexes(con, &t);
                let names: Vec<String> =
                    cols.iter().map(|c| format!("\"{}\"::text", c.0)).collect();
                let order: Vec<String> = (1..=names.len()).map(|i| i.to_string()).collect();
                let rows = cpg::snapshot(
                    con,
                    &format!(
                        "select {} from \"{t}\" order by {}",
                        names.join(", "),
                        order.join(", ")
                    ),
                    &[],
                )
                .rows
                .into_iter()
                .map(|r| r.into_iter().map(|v| v.replace(dir, "<dir>")).collect())
                .collect();
                res.insert(format!("{prefix}{t}"), (cols, idx, rows));
            }
        }
        let mut res = BTreeMap::new();
        let con = self.db.conn();
        dump(&con, "", &self.dir_str, &mut res);
        con.close();
        if let Some(sh) = &self.shared {
            let con = sh.conn();
            dump(&con, "shared.", &self.dir_str, &mut res);
            con.close();
        }
        res
    }
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let db = TestDb::fresh(&format!("annotations_{}_{}", case.name, suffix))?;
    let shared = if case.shared {
        Some(TestDb::fresh(&format!("annotations_{}_shared_{}", case.name, suffix)).unwrap())
    } else {
        None
    };
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_annotations_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    let work = dir.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let dir_str = dir.path().to_str().unwrap().to_string();
    let shared_name = shared.as_ref().map(|s| s.name.clone()).unwrap_or_default();
    let expand = |s: &str| {
        s.replace("{dir}", &dir_str)
            .replace("{shared}", &shared_name)
    };

    // projects.yaml — in the working directory (GHA2DB_LOCAL) or in the
    // data directory (GHA2DB_DATADIR).
    let yaml_name = case
        .env
        .iter()
        .find(|(k, _)| *k == "GHA2DB_PROJECTS_YAML")
        .map(|(_, v)| *v)
        .unwrap_or("projects.yaml");
    let yaml_dir = if case.local {
        work.clone()
    } else {
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        data
    };
    if !case.no_yaml {
        fs::write(yaml_dir.join(yaml_name), expand(case.yaml)).unwrap();
    }

    // git_tags.sh — ./git/ (GHA2DB_LOCAL_CMD) or <dir>/bin on the PATH.
    let script_dir = if case.local_cmd {
        work.join("git")
    } else {
        dir.path().join("bin")
    };
    fs::create_dir_all(&script_dir).unwrap();
    let script = script_dir.join("git_tags.sh");
    match &case.script {
        Script::Real => {
            fs::copy(fixture("annotations/git_tags.sh"), &script).unwrap();
        }
        Script::Fake(body) => {
            fs::write(&script, format!("#!/bin/sh\n{body}\n")).unwrap();
        }
        Script::Missing => {}
    }
    if script.exists() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    // The main repository.
    let repos = dir.path().join("repos");
    fs::create_dir_all(&repos).unwrap();
    if let Some(tags) = &case.tags {
        build_repo(&repos.join("org").join("repo"), tags);
    }

    let mut env: Vec<(String, String)> = db
        .env()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    if case.local {
        env.push(("GHA2DB_LOCAL".into(), "1".into()));
    } else {
        env.push(("GHA2DB_DATADIR".into(), format!("{dir_str}/data/")));
    }
    if case.local_cmd {
        env.push(("GHA2DB_LOCAL_CMD".into(), "1".into()));
    } else {
        let path = std::env::var("PATH").unwrap_or_default();
        env.push(("PATH".into(), format!("{dir_str}/bin:{path}")));
    }
    env.push(("GHA2DB_REPOS_DIR".into(), format!("{dir_str}/repos/")));
    for (k, v) in &case.env {
        env.retain(|(key, _)| key != k);
        env.push((k.to_string(), expand(v)));
    }
    let mut outs = Vec::new();
    for step in &case.steps {
        let extra = match step {
            Step::Run(extra) => extra,
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
        outs.push(run(bin, &inv));
    }
    Some(Side {
        db,
        shared,
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
            if !case.stdout {
                if case.require_error {
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
                }
                continue;
            }
            let (go_ordered, go_unordered) = go.stdout(i);
            let (rs_ordered, rs_unordered) = rust.stdout(i);
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

/// `YYYY-MM-DD 00:00:00` of tomorrow — the `to` of the open-ended quick
/// ranges (the binaries run with `TZ=UTC`, so the UTC date).
fn tomorrow() -> String {
    let t = Utc::now().date_naive() + Duration::days(1);
    format!("{} 00:00:00", t.format("%Y-%m-%d"))
}

fn s3(v: &[(&str, &str, &str)]) -> Vec<(String, String, String)> {
    v.iter()
        .map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
        .collect()
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

/// The 12 fixed quick ranges every project gets.
const FIXED: &[&str] = &[
    "d", "w", "d10", "m", "q", "m6", "y", "y2", "y3", "y5", "y10", "y100",
];

fn with_fixed(rest: &[&str]) -> Vec<String> {
    let mut v = strs(FIXED);
    v.extend(strs(rest));
    v
}

// ------------------------------------------------------- main repository

/// A project with a main repository, a regexp and all four CNCF dates.
fn full() -> &'static str {
    proj(&[MAIN_REPO, REGEXP, START, JOIN, INCUBATING, GRADUATED])
}

#[test]
fn full_project() {
    let Some(rs) = both(&Case::new("full", full())) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, BANNER);
    rs.expect_line(
        0,
        "WriteTSPoints: writing 29 points in batches of up to 1000",
    );
    rs.expect_line(0, "WriteTSPointsBatch: writing 29 points - finished");
    rs.expect_line(0, "Time: <masked>");
    assert_eq!(rs.tables(), strs(&["sannotations", "tquick_ranges"]));
    // Tags sorted by date (v0.9 was created last), `v1.1.1` dropped (same
    // hour as `v1.1`), `old` / `rc-2.0` not matching the regexp, the long
    // message cut at 40 bytes, the tab replaced by a space.
    assert_eq!(
        rs.annotations(),
        s3(&[
            (
                "2015-06-01 00:00:00",
                "Project start date",
                "2015-06-01 - project starts"
            ),
            ("2015-07-15 23:00:00", "v0.9", "sorted by date, not by name"),
            ("2015-08-01 10:00:00", "v1.0", "first"),
            (
                "2015-09-01 10:00:00",
                "v1.1",
                "Release v1.1 — with a very long messag"
            ),
            (
                "2015-10-01 00:00:00",
                "CNCF join date",
                "2015-10-01 - joined CNCF"
            ),
            ("2016-02-05 00:00:00", "v2.0", "multi line msg"),
            (
                "2016-03-01 00:00:00",
                "Moved to incubating state",
                "2016-03-01 - project moved to incubating state",
            ),
            (
                "2017-01-01 00:00:00",
                "Graduated",
                "2017-01-01 - project graduated"
            ),
        ])
    );
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n", "c_b", "c_n", "c_j_i", "c_i_g", "c_g_n"])
    );
    let ranges = rs.ranges();
    // Fixed periods: 2012-07-01 00:00 + 1 h each.
    assert_eq!(
        ranges[0],
        (
            "2012-07-01 00:00:00".into(),
            "d".into(),
            "Last day".into(),
            "d;1 day;;".into()
        )
    );
    assert_eq!(
        ranges[11],
        (
            "2012-07-01 11:00:00".into(),
            "y100".into(),
            "Last century".into(),
            "y100;100 years;;".into()
        )
    );
    assert_eq!(ranges[12].0, "2012-07-01 12:00:00");
    assert_eq!(ranges[20].0, "2012-07-01 20:00:00");
    // Annotation ranges use the exact tag times (not the hour starts).
    assert_eq!(
        rs.range("a_0_1"),
        Some((
            "v0.9 - v1.0".into(),
            "a_0_1;;2015-07-15 23:59:59;2015-08-01 10:15:00".into()
        ))
    );
    assert_eq!(
        rs.range("a_2_3"),
        Some((
            "v1.1 - v2.0".into(),
            "a_2_3;;2015-09-01 10:15:00;2016-02-05 00:00:00".into()
        ))
    );
    assert_eq!(
        rs.range("a_3_n"),
        Some((
            "v2.0 - now".into(),
            format!("a_3_n;;2016-02-05 00:00:00;{}", tomorrow())
        ))
    );
    assert_eq!(
        rs.range("c_b"),
        Some((
            "Before joining CNCF".into(),
            "c_b;;2015-06-01 00:00:00;2015-10-01 00:00:00".into()
        ))
    );
    assert_eq!(
        rs.range("c_n"),
        Some((
            "Since joining CNCF".into(),
            format!("c_n;;2015-10-01 00:00:00;{}", tomorrow())
        ))
    );
    assert_eq!(
        rs.range("c_j_i"),
        Some((
            "CNCF join date - moved to incubation".into(),
            "c_j_i;;2015-10-01 00:00:00;2016-03-01 00:00:00".into()
        ))
    );
    assert_eq!(
        rs.range("c_i_g"),
        Some((
            "Moved to incubation - graduated".into(),
            "c_i_g;;2016-03-01 00:00:00;2017-01-01 00:00:00".into()
        ))
    );
    assert_eq!(
        rs.range("c_g_n"),
        Some((
            "Since graduating".into(),
            format!("c_g_n;;2017-01-01 00:00:00;{}", tomorrow())
        ))
    );
    // Table shapes (Go creates the columns in map order — compare sorted).
    let con = rs.db.conn();
    let mut cols: Vec<String> = cpg::table_columns(&con, "sannotations")
        .into_iter()
        .map(|c| c.0)
        .collect();
    cols.sort();
    assert_eq!(cols, strs(&["description", "period", "time", "title"]));
    let mut cols: Vec<String> = cpg::table_columns(&con, "tquick_ranges")
        .into_iter()
        .map(|c| c.0)
        .collect();
    cols.sort();
    assert_eq!(
        cols,
        strs(&[
            "quick_ranges_data",
            "quick_ranges_name",
            "quick_ranges_suffix",
            "time"
        ])
    );
    con.close();
    assert_eq!(
        rs.query("select distinct period from sannotations"),
        vec![vec!["".to_string()]]
    );
}

#[test]
fn full_project_debug() {
    let Some(rs) = both(&Case::new("full_debug", full()).debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, BANNER);
    rs.expect_line(0, "lib.ReadFile('./projects.yaml'): ok");
    rs.expect_line(0, "Getting tags for repo org/repo");
    rs.expect_line(0, "Got 5 tags for org/repo, took <dur>");
    rs.expect_line(
        0,
        "Skipping annotation {v1.1.1 same hour 2015-09-01 10:45:00 +0000 <zone>} because its hour date is the same as the previous one",
    );
    rs.expect_line(
        0,
        "Series: annotations: Date: 2015-07-15: 'v0.9', 'sorted by date, not by name'",
    );
    rs.expect_line(
        0,
        "Series: annotations: Date: 2015-09-01: 'v1.1', 'Release v1.1 — with a very long messag'",
    );
    rs.expect_line(
        0,
        "Project start date: 2015-06-01: 'Project start date', '2015-06-01 - project starts'",
    );
    rs.expect_line(
        0,
        "CNCF join date: 2015-10-01: 'CNCF join date', '2015-10-01 - joined CNCF'",
    );
    rs.expect_line(
        0,
        "Project moved to incubating state: 2016-03-01: 'Moved to incubating state', '2016-03-01 - project moved to incubating state'",
    );
    rs.expect_line(
        0,
        "Project graduated: 2017-01-01: 'Graduated', '2017-01-01 - project graduated'",
    );
    rs.expect_line(
        0,
        "Series: quick_ranges: map[quick_ranges_data:d;1 day;; quick_ranges_name:Last day quick_ranges_suffix:d]",
    );
    rs.expect_line(
        0,
        "Series: quick_ranges: map[quick_ranges_data:a_0_1;;2015-07-15 23:59:59;2015-08-01 10:15:00 quick_ranges_name:v0.9 - v1.0 quick_ranges_suffix:a_0_1]",
    );
    rs.expect_line(
        0,
        "NewTSPoint: 2015-08-01 10 <added> annotations period:  tags: map[] fields: map[description:first title:v1.0]",
    );
    rs.expect_prefix(0, "AddTSPoint: point added, now 29 points");
    rs.expect_no_line(0, "Skipping annotations series write");
}

#[test]
fn no_regexp_takes_all_tags() {
    let Some(rs) = both(&Case::new("no_regexp", proj(&[MAIN_REPO, START, JOIN])).debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // `old` (2011) is now seen and skipped, `rc-2.0` is in.
    rs.expect_line(0, "Got 6 tags for org/repo, took <dur>");
    rs.expect_line(
        0,
        "Skipping annotation 2011-01-01 00:00:00 +0000 <zone> because it is before 2012-07-01 00:00:00 +0000 <zone>",
    );
    assert_eq!(
        rs.annotations()
            .into_iter()
            .map(|a| a.1)
            .collect::<Vec<_>>(),
        strs(&[
            "Project start date",
            "v0.9",
            "v1.0",
            "v1.1",
            "CNCF join date",
            "rc-2.0",
            "v2.0"
        ])
    );
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_4", "a_4_n", "c_b", "c_n"])
    );
    assert_eq!(
        rs.range("a_3_4"),
        Some((
            "rc-2.0 - v2.0".into(),
            "a_3_4;;2016-01-05 00:00:00;2016-02-05 00:00:00".into()
        ))
    );
}

#[test]
fn regexp_matching_nothing_leaves_only_milestones() {
    let Some(rs) = both(&Case::new(
        "regexp_nothing",
        proj(&[
            MAIN_REPO,
            "annotation_regexp: '^nomatch$'",
            START,
            JOIN,
            GRADUATED,
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[
            (
                "2015-06-01 00:00:00",
                "Project start date",
                "2015-06-01 - project starts"
            ),
            (
                "2015-10-01 00:00:00",
                "CNCF join date",
                "2015-10-01 - joined CNCF"
            ),
            (
                "2017-01-01 00:00:00",
                "Graduated",
                "2017-01-01 - project graduated"
            ),
        ])
    );
    // No `a_*` ranges without tag annotations; no incubating date → `c_j_g`.
    assert_eq!(rs.suffixes(), with_fixed(&["c_b", "c_n", "c_j_g", "c_g_n"]));
    assert_eq!(
        rs.range("c_j_g"),
        Some((
            "CNCF join date - graduated".into(),
            "c_j_g;;2015-10-01 00:00:00;2017-01-01 00:00:00".into()
        ))
    );
}

#[test]
fn invalid_regexp_is_fatal() {
    // Go: `regexp.MustCompile` panics (exit 2, no `Error:` line); the port
    // reports it through the usual fatal path (exit 2 too).
    let Some(rs) = both(
        &Case::new(
            "bad_regexp",
            proj(&[MAIN_REPO, "annotation_regexp: '('", START]),
        )
        .code_only_no_error(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    let stderr = rs.outs[0].stderr_str();
    assert!(stderr.contains("regexp: Compile(`(`)"), "{stderr}");
    assert!(rs.tables().is_empty());
}

#[test]
fn main_repo_without_slash_is_fatal() {
    let Some(rs) = both(&Case::new("bad_repo", proj(&["main_repo: org", START]))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("main repository format must be 'org/repo', found 'org'")
    );
}

#[test]
fn main_repo_with_two_slashes_is_fatal() {
    let Some(rs) = both(&Case::new("bad_repo3", proj(&["main_repo: a/b/c"]))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("main repository format must be 'org/repo', found 'a/b/c'")
    );
}

#[test]
fn missing_repository_directory_is_fatal() {
    let Some(rs) = both(&Case::new("no_repo", full()).no_repo()) else {
        return;
    };
    // `cd` in git_tags.sh fails → exit 3; the shell's message is echoed.
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(rs.error(0).as_deref(), Some("exit status 3"));
    rs.expect_line(0, "STDERR:");
    rs.expect_prefix(0, "./git/git_tags.sh:");
    rs.expect_line(0, "Command, arguments, environment:");
    rs.expect_line(0, "[./git/git_tags.sh <dir>/repos/org/repo]");
    rs.expect_line(0, "map[GIT_TERMINAL_PROMPT:0]");
    assert!(rs.tables().is_empty());
}

#[test]
fn missing_script_is_fatal() {
    let Some(rs) = both(&Case::new("no_script", full()).script(Script::Missing)) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("fork/exec ./git/git_tags.sh: no such file or directory")
    );
}

#[test]
fn script_found_on_path_without_local_cmd() {
    let Some(rs) = both(&Case::new("path_mode", full()).path_mode()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.annotations().len(), 8);
    assert_eq!(rs.suffixes().len(), 21);
}

#[test]
fn script_missing_on_path_is_fatal() {
    let Some(rs) = both(
        &Case::new("path_missing", full())
            .path_mode()
            .script(Script::Missing),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("exec: \"git_tags.sh\": executable file not found in $PATH")
    );
}

#[test]
fn projects_yaml_from_data_dir() {
    let Some(rs) = both(&Case::new("datadir", full()).no_local().debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "lib.ReadFile('<dir>/data/projects.yaml'): ok");
    assert_eq!(rs.annotations().len(), 8);
}

#[test]
fn custom_projects_yaml_name() {
    let Some(rs) = both(
        &Case::new("custom_yaml", full())
            .env("GHA2DB_PROJECTS_YAML", "custom.yaml")
            .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "lib.ReadFile('./custom.yaml'): ok");
    assert_eq!(rs.annotations().len(), 8);
}

#[test]
fn missing_projects_yaml_is_fatal() {
    let Some(rs) = both(&Case::new("no_yaml", full()).no_yaml()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("open ./projects.yaml: no such file or directory")
    );
}

#[test]
fn missing_project_variable_is_fatal() {
    let Some(rs) = both(&Case::new("no_project", full()).no_env("GHA2DB_PROJECT")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("you have to set project via GHA2DB_PROJECT environment variable")
    );
}

#[test]
fn unknown_project_is_fatal() {
    let Some(rs) = both(&Case::new("unknown_project", full()).env("GHA2DB_PROJECT", "other"))
    else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("project 'other' not found in 'projects.yaml'")
    );
}

#[test]
fn unknown_project_names_the_custom_yaml() {
    let Some(rs) = both(
        &Case::new("unknown_custom", full())
            .env("GHA2DB_PROJECT", "other")
            .env("GHA2DB_PROJECTS_YAML", "p.yaml"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("project 'other' not found in 'p.yaml'")
    );
}

#[test]
fn invalid_yaml_is_fatal() {
    let Some(rs) = both(&Case::new("bad_yaml", "projects: [\n  proj: {\n").code_only_errors())
    else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.tables().is_empty());
}

#[test]
fn empty_yaml_has_no_projects() {
    let Some(rs) = both(&Case::new("empty_yaml", "")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("project 'proj' not found in 'projects.yaml'")
    );
}

// ----------------------------------------------------------- milestones

#[test]
fn start_date_only() {
    let Some(rs) = both(&Case::new("start_only", proj(&[MAIN_REPO, REGEXP, START]))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert_eq!(
        titles,
        strs(&["Project start date", "v0.9", "v1.0", "v1.1", "v2.0"])
    );
    // No join date → no `c_*` ranges at all.
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n"])
    );
}

#[test]
fn join_date_only() {
    let Some(rs) = both(&Case::new(
        "join_only",
        proj(&[MAIN_REPO, REGEXP, JOIN, GRADUATED]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert_eq!(
        titles,
        strs(&[
            "v0.9",
            "v1.0",
            "v1.1",
            "CNCF join date",
            "v2.0",
            "Graduated"
        ])
    );
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n"])
    );
}

#[test]
fn join_before_start_drops_both_milestones() {
    let Some(rs) = both(&Case::new(
        "join_before_start",
        proj(&[
            MAIN_REPO,
            REGEXP,
            START,
            "join_date: 2015-01-01T00:00:00Z",
            INCUBATING,
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert_eq!(
        titles,
        strs(&["v0.9", "v1.0", "v1.1", "v2.0", "Moved to incubating state"])
    );
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n"])
    );
}

#[test]
fn join_equal_to_start_drops_both_milestones() {
    let Some(rs) = both(&Case::new(
        "join_eq_start",
        proj(&[MAIN_REPO, REGEXP, START, "join_date: 2015-06-01T00:00:00Z"]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert_eq!(titles, strs(&["v0.9", "v1.0", "v1.1", "v2.0"]));
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n"])
    );
}

#[test]
fn no_dates_at_all() {
    let Some(rs) = both(&Case::new("no_dates", proj(&[MAIN_REPO, REGEXP]))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.annotations().len(), 4);
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n"])
    );
}

#[test]
fn incubating_without_graduation() {
    let Some(rs) = both(&Case::new(
        "incubating",
        proj(&[MAIN_REPO, REGEXP, START, JOIN, INCUBATING]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n", "c_b", "c_n", "c_j_i", "c_i_n"])
    );
    assert_eq!(
        rs.range("c_i_n"),
        Some((
            "Since moving to incubating state".into(),
            format!("c_i_n;;2016-03-01 00:00:00;{}", tomorrow())
        ))
    );
}

#[test]
fn graduated_before_incubating_is_wrong_order() {
    let Some(rs) = both(&Case::new(
        "wrong_order",
        proj(&[
            MAIN_REPO,
            REGEXP,
            START,
            JOIN,
            "incubating_date: 2017-01-01T00:00:00Z",
            "graduated_date: 2016-03-01T00:00:00Z",
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // Both milestone annotations are still written …
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert!(titles.contains(&"Graduated".to_string()));
    assert!(titles.contains(&"Moved to incubating state".to_string()));
    // … but no incubating/graduated ranges.
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n", "c_b", "c_n"])
    );
}

#[test]
fn graduated_equal_to_incubating_is_wrong_order() {
    let Some(rs) = both(&Case::new(
        "same_order",
        proj(&[
            MAIN_REPO,
            REGEXP,
            START,
            JOIN,
            INCUBATING,
            "graduated_date: 2016-03-01T00:00:00Z",
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n", "c_b", "c_n"])
    );
}

#[test]
fn incubating_before_join_is_ignored_for_ranges() {
    let Some(rs) = both(&Case::new(
        "incubating_before_join",
        proj(&[
            MAIN_REPO,
            REGEXP,
            START,
            JOIN,
            "incubating_date: 2015-08-01T00:00:00Z",
            GRADUATED,
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // Incubating not after join → no `c_j_i`; graduated after join with an
    // incubating date present → only `c_g_n` (no `c_j_g`).
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n", "c_b", "c_n", "c_g_n"])
    );
}

#[test]
fn graduated_before_join_is_ignored_for_ranges() {
    let Some(rs) = both(&Case::new(
        "graduated_before_join",
        proj(&[
            MAIN_REPO,
            REGEXP,
            START,
            JOIN,
            "graduated_date: 2015-08-01T00:00:00Z",
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n", "c_b", "c_n"])
    );
}

#[test]
fn archived_project() {
    let Some(rs) = both(
        &Case::new(
            "archived",
            proj(&[
                MAIN_REPO, REGEXP, START, JOIN, INCUBATING, GRADUATED, ARCHIVED,
            ]),
        )
        .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Project was archived: 2019-05-01: 'Archived', '2019-05-01 - project was archived'",
    );
    let last = rs.annotations().pop().unwrap();
    assert_eq!(
        last,
        (
            "2019-05-01 00:00:00".into(),
            "Archived".into(),
            "2019-05-01 - project was archived".into()
        )
    );
    // Archiving adds no quick range.
    assert_eq!(rs.suffixes().len(), 21);
}

#[test]
fn incubating_without_join_has_no_ranges() {
    let Some(rs) = both(&Case::new(
        "incubating_no_join",
        proj(&[MAIN_REPO, REGEXP, START, INCUBATING, GRADUATED]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert_eq!(
        titles,
        strs(&[
            "Project start date",
            "v0.9",
            "v1.0",
            "v1.1",
            "v2.0",
            "Moved to incubating state",
            "Graduated"
        ])
    );
    assert_eq!(
        rs.suffixes(),
        with_fixed(&["a_0_1", "a_1_2", "a_2_3", "a_3_n"])
    );
}

#[test]
fn dates_with_time_and_offset() {
    // Milestone dates keep their zone: the annotation point is the hour
    // start of the wall clock (`10:30+02:00` → `10:00`, Go `HourStart`
    // relabels the wall clock as UTC) and the range strings use the wall
    // clock too.
    let Some(rs) = both(&Case::new(
        "dates_offset",
        proj(&[
            MAIN_REPO,
            REGEXP,
            "start_date: 2015-06-01T10:30:00+02:00",
            "join_date: 2015-10-01T23:59:59Z",
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let a = rs.annotations();
    assert_eq!(
        a[0],
        (
            "2015-06-01 10:00:00".into(),
            "Project start date".into(),
            "2015-06-01 - project starts".into()
        )
    );
    assert_eq!(
        rs.range("c_b"),
        Some((
            "Before joining CNCF".into(),
            "c_b;;2015-06-01 10:30:00;2015-10-01 23:59:59".into()
        ))
    );
}

// ------------------------------------------- projects without main_repo

#[test]
fn fake_annotations_from_start_and_join() {
    let Some(rs) = both(&Case::new("fake_start_join", proj(&[START, JOIN, INCUBATING])).debug())
    else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_no_line(0, "Getting tags for repo org/repo");
    // The two fake annotations; the start/join milestone annotations are
    // not added (the dates are not passed on) but incubating is.
    assert_eq!(
        rs.annotations(),
        s3(&[
            (
                "2015-06-01 00:00:00",
                "Project start",
                "2015-06-01 - project starts"
            ),
            (
                "2015-10-01 00:00:00",
                "First CNCF project join date",
                "2015-10-01"
            ),
            (
                "2016-03-01 00:00:00",
                "Moved to incubating state",
                "2016-03-01 - project moved to incubating state",
            ),
        ])
    );
    // Ranges only from the fake annotations — no `c_*` (no join date passed).
    assert_eq!(rs.suffixes(), with_fixed(&["a_0_1", "a_1_n"]));
    assert_eq!(
        rs.range("a_0_1"),
        Some((
            "Project start - First CNCF project join date".into(),
            "a_0_1;;2015-06-01 00:00:00;2015-10-01 00:00:00".into()
        ))
    );
    assert_eq!(
        rs.range("a_1_n"),
        Some((
            "First CNCF project join date - now".into(),
            format!("a_1_n;;2015-10-01 00:00:00;{}", tomorrow())
        ))
    );
}

#[test]
fn fake_annotation_from_start_only() {
    let Some(rs) = both(&Case::new("fake_start", proj(&[START, GRADUATED]))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[
            (
                "2015-06-01 00:00:00",
                "Project start",
                "2015-06-01 - project starts"
            ),
            (
                "2017-01-01 00:00:00",
                "Graduated",
                "2017-01-01 - project graduated"
            ),
        ])
    );
    assert_eq!(rs.suffixes(), with_fixed(&["a_0_n"]));
    assert_eq!(
        rs.range("a_0_n"),
        Some((
            "Project start - now".into(),
            format!("a_0_n;;2015-06-01 00:00:00;{}", tomorrow())
        ))
    );
}

#[test]
fn fake_annotations_with_join_before_start_are_empty() {
    let Some(rs) = both(&Case::new(
        "fake_bad_join",
        proj(&[START, "join_date: 2015-01-01T00:00:00Z"]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // GetFakeAnnotations returns nothing → only the fixed ranges are written.
    assert_eq!(rs.annotations(), Vec::new());
    assert_eq!(rs.suffixes(), strs(FIXED));
}

#[test]
fn fake_annotations_before_2012_are_empty() {
    let Some(rs) = both(&Case::new(
        "fake_old",
        proj(&[
            "start_date: 2010-01-01T00:00:00Z",
            "join_date: 2011-06-01T00:00:00Z",
        ]),
    )) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.annotations(), Vec::new());
    assert_eq!(rs.suffixes(), strs(FIXED));
}

#[test]
fn no_main_repo_and_no_start_date_does_nothing() {
    let Some(rs) = both(&Case::new("nothing", proj(&[JOIN, INCUBATING, GRADUATED])).debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert!(rs.tables().is_empty(), "{:?}", rs.tables());
    assert_eq!(
        rs.lines(0).last().map(String::as_str),
        Some("Time: <masked>")
    );
    rs.expect_no_line(0, "Getting tags for repo org/repo");
}

// -------------------------------------------------------------- shared db

#[test]
fn annotations_copied_to_shared_db() {
    let Some(rs) = both(
        &Case::new(
            "shared",
            proj(&[
                MAIN_REPO,
                REGEXP,
                START,
                JOIN,
                INCUBATING,
                GRADUATED,
                "shared_db: {shared}",
            ]),
        )
        .shared(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let rows = rs
        .shared_annotations()
        .expect("sannotations_shared missing");
    // Every `annotations` point, with the project as period and the main
    // repository as an extra field.
    assert_eq!(rows.len(), rs.annotations().len());
    assert_eq!(
        rows[0],
        strs(&[
            "2015-06-01 00:00:00",
            "proj",
            "Project start date",
            "2015-06-01 - project starts",
            "org/repo"
        ])
    );
    assert_eq!(
        rows[3],
        strs(&[
            "2015-09-01 10:00:00",
            "proj",
            "v1.1",
            "Release v1.1 — with a very long messag",
            "org/repo"
        ])
    );
    let con = rs.shared.as_ref().unwrap().conn();
    assert_eq!(cpg::tables(&con), strs(&["sannotations_shared"]));
    let mut cols: Vec<String> = cpg::table_columns(&con, "sannotations_shared")
        .into_iter()
        .map(|c| c.0)
        .collect();
    cols.sort();
    assert_eq!(
        cols,
        strs(&["description", "period", "repo", "time", "title"])
    );
    con.close();
    // The project database is not affected by the copy.
    assert_eq!(rs.tables(), strs(&["sannotations", "tquick_ranges"]));
    assert_eq!(
        rs.query("select distinct period from sannotations"),
        vec![vec!["".to_string()]]
    );
}

#[test]
fn shared_db_skipped_by_env() {
    let Some(rs) = both(
        &Case::new(
            "shared_skip",
            proj(&[MAIN_REPO, REGEXP, START, JOIN, "shared_db: {shared}"]),
        )
        .shared()
        .env("GHA2DB_SKIP_SHAREDDB", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.shared_annotations(), None);
    assert_eq!(rs.annotations().len(), 6);
}

#[test]
fn shared_db_for_project_without_main_repo() {
    let Some(rs) =
        both(&Case::new("shared_fake", proj(&[START, JOIN, "shared_db: {shared}"])).shared())
    else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let rows = rs.shared_annotations().unwrap();
    assert_eq!(
        rows,
        vec![
            strs(&[
                "2015-06-01 00:00:00",
                "proj",
                "Project start",
                "2015-06-01 - project starts",
                ""
            ]),
            strs(&[
                "2015-10-01 00:00:00",
                "proj",
                "First CNCF project join date",
                "2015-10-01",
                ""
            ]),
        ]
    );
}

#[test]
fn shared_db_rerun_upserts() {
    let Some(rs) = both(
        &Case::new(
            "shared_rerun",
            proj(&[MAIN_REPO, REGEXP, START, JOIN, "shared_db: {shared}"]),
        )
        .shared()
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]),
    ) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    assert_eq!(rs.shared_annotations().unwrap().len(), 6);
    assert_eq!(rs.annotations().len(), 6);
}

#[test]
fn skip_tsdb_writes_nothing() {
    let Some(rs) = both(
        &Case::new(
            "skip_tsdb",
            proj(&[MAIN_REPO, REGEXP, START, JOIN, "shared_db: {shared}"]),
        )
        .shared()
        .env("GHA2DB_SKIPTSDB", "1")
        .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Skipping annotations series write");
    rs.expect_line(0, "Got 5 tags for org/repo, took <dur>");
    assert!(rs.tables().is_empty());
    assert_eq!(rs.shared_annotations(), None);
}

// ------------------------------------------------------------------ reruns

#[test]
fn rerun_is_idempotent() {
    let Some(rs) =
        both(&Case::new("rerun", full()).steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]))
    else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.code(1), Some(0));
    assert_eq!(rs.annotations().len(), 8);
    assert_eq!(rs.suffixes().len(), 21);
    rs.expect_line(
        1,
        "WriteTSPoints: writing 29 points in batches of up to 1000",
    );
}

#[test]
fn stale_open_ended_ranges_are_deleted_on_rerun() {
    let Some(rs) = both(&Case::new("stale", full()).steps(vec![
        Step::Run(Vec::new()),
        // Left-overs of an earlier run with more tags plus unrelated rows;
        // `like '%_n'` — `_` is a wildcard — matches every suffix whose
        // last two characters end in `n`.
        Step::Sql(
            "insert into tquick_ranges(time, quick_ranges_suffix, quick_ranges_name, quick_ranges_data) values \
             ('2011-01-01 01:00:00', 'a_9_n', 'stale', 'a_9_n;;2011;2012'), \
             ('2011-01-01 02:00:00', 'zzn', 'ends in n', 'zzn;;;'), \
             ('2011-01-01 03:00:00', 'zz', 'stays', 'zz;;;'), \
             ('2011-01-01 04:00:00', 'n', 'single n stays', 'n;;;'), \
             ('2011-01-01 05:00:00', 'nope', 'stays too', 'nope;;;')",
        ),
        Step::Run(Vec::new()),
    ])) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    let mut sfx = rs.suffixes();
    sfx.sort();
    let mut expected = with_fixed(&[
        "a_0_1", "a_1_2", "a_2_3", "a_3_n", "c_b", "c_n", "c_j_i", "c_i_g", "c_g_n", "zz", "n",
        "nope",
    ]);
    expected.sort();
    assert_eq!(sfx, expected);
    assert_eq!(rs.range("a_9_n"), None);
    assert_eq!(rs.range("zzn"), None);
    assert_eq!(rs.range("zz"), Some(("stays".into(), "zz;;;".into())));
}

#[test]
fn existing_ranges_table_without_suffix_column() {
    // No `quick_ranges_suffix` column → no stale-row deletion; the missing
    // columns are added by the writer.
    let Some(rs) = both(&Case::new("no_suffix_col", full()).steps(vec![
        Step::Sql(
            "create table tquick_ranges(time timestamp primary key, quick_ranges_name text); \
             insert into tquick_ranges values ('2011-01-01 01:00:00', 'a_9_n old')",
        ),
        Step::Run(Vec::new()),
    ])) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.query(
            "select quick_ranges_name, coalesce(quick_ranges_suffix, '<null>') from tquick_ranges where time = '2011-01-01 01:00:00'"
        ),
        vec![strs(&["a_9_n old", "<null>"])]
    );
    assert_eq!(rs.suffixes().len(), 22);
}

#[test]
fn existing_annotations_table_with_extra_column() {
    let Some(rs) = both(&Case::new("extra_col", full()).steps(vec![
        Step::Sql(
            "create table sannotations(time timestamp not null, period text not null default '', \
             title text not null default '', description text not null default '', extra int, \
             primary key(time, period)); \
             insert into sannotations(time, title, description, extra) values ('2015-08-01 10:00:00', 'old v1.0', 'old', 7)",
        ),
        Step::Run(Vec::new()),
    ])) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // The point at the same (time, period) is updated, `extra` untouched.
    assert_eq!(
        rs.query(
            "select title, description, extra::text from sannotations where time = '2015-08-01 10:00:00'"
        ),
        vec![strs(&["v1.0", "first", "7"])]
    );
    assert_eq!(rs.annotations().len(), 8);
}

#[test]
fn modified_rows_are_restored_on_rerun() {
    let Some(rs) = both(&Case::new("restore", full()).steps(vec![
        Step::Run(Vec::new()),
        Step::Sql(
            "update sannotations set description = 'edited' where title = 'v1.0'; \
             update tquick_ranges set quick_ranges_name = 'edited' where quick_ranges_suffix = 'd'; \
             delete from sannotations where title = 'Graduated'",
        ),
        Step::Run(Vec::new()),
    ])) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    assert_eq!(
        rs.query("select description from sannotations where title = 'v1.0'"),
        vec![strs(&["first"])]
    );
    assert_eq!(rs.range("d"), Some(("Last day".into(), "d;1 day;;".into())));
    assert_eq!(rs.annotations().len(), 8);
}

// ------------------------------------------------------- tag edge cases

#[test]
fn first_hour_of_2012_07_01_is_dropped() {
    // The dedupe starts with `prev = 2012-07-01 00:00`, so a tag in that
    // very hour looks like a duplicate; the next hour is fine and a tag one
    // second before the minimum date is skipped as too old.
    let Some(rs) = both(
        &Case::new("first_hour", proj(&[MAIN_REPO, START]))
            .tags(vec![
                tag("v0.0", "2012-06-30T23:59:59Z", "too old by a second"),
                tag("v0.1", "2012-07-01T00:30:00Z", "first hour"),
                tag("v0.2", "2012-07-01T01:30:00Z", "second hour"),
                tag("v0.3", "2012-07-01T01:59:59Z", "second hour again"),
                tag("v0.4", "2012-07-01T02:00:00Z", "third hour"),
            ])
            .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Got 4 tags for org/repo, took <dur>");
    rs.expect_line(
        0,
        "Skipping annotation {v0.1 first hour 2012-07-01 00:30:00 +0000 <zone>} because its hour date is the same as the previous one",
    );
    rs.expect_line(
        0,
        "Skipping annotation {v0.3 second hour again 2012-07-01 01:59:59 +0000 <zone>} because its hour date is the same as the previous one",
    );
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert_eq!(titles, strs(&["v0.2", "v0.4", "Project start date"]));
}

#[test]
fn many_tags_in_one_hour_keep_the_first() {
    let Some(rs) = both(&Case::new("one_hour", proj(&[MAIN_REPO])).tags(vec![
        light("c", "2015-08-01T10:59:59Z", "third"),
        tag("a", "2015-08-01T10:00:00Z", "first"),
        tag("b", "2015-08-01T10:30:00Z", "second"),
        tag("d", "2015-08-01T11:00:00Z", "next hour"),
    ])) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[
            ("2015-08-01 10:00:00", "a", "first"),
            ("2015-08-01 11:00:00", "d", "next hour"),
        ])
    );
    assert_eq!(
        rs.range("a_0_1"),
        Some((
            "a - d".into(),
            "a_0_1;;2015-08-01 10:00:00;2015-08-01 11:00:00".into()
        ))
    );
}

#[test]
fn messages_are_cut_at_forty_bytes() {
    let Some(rs) = both(&Case::new("cut", proj(&[MAIN_REPO])).tags(vec![
        tag(
            "v1",
            "2015-08-01T10:00:00Z",
            "12345678901234567890123456789012345678😀tail",
        ),
        tag(
            "v2",
            "2015-08-02T10:00:00Z",
            "1234567890123456789012345678901234567890",
        ),
        tag(
            "v3",
            "2015-08-03T10:00:00Z",
            "12345678901234567890123456789012345678901",
        ),
        tag(
            "v4",
            "2015-08-04T10:00:00Z",
            "ąęćłńóśźż ąęćłńóśźż ąęćłńóśźż",
        ),
        tag(
            "v5",
            "2015-08-05T10:00:00Z",
            "日本語のタグメッセージはとても長いですね本当に",
        ),
        light("v6", "2015-08-06T10:00:00Z", "x"),
    ])) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let desc: Vec<String> = rs.annotations().into_iter().map(|a| a.2).collect();
    assert_eq!(
        desc,
        strs(&[
            // 38 ASCII bytes + a 4-byte emoji cut in half → dropped
            "12345678901234567890123456789012345678",
            "1234567890123456789012345678901234567890",
            "1234567890123456789012345678901234567890",
            // 2-byte letters: 9 × 2 + 1 + 9 × 2 + 1 = 38 bytes, then one more letter
            "ąęćłńóśźż ąęćłńóśźż ą",
            // 3-byte characters: 13 = 39 bytes, the 14th cut → dropped
            "日本語のタグメッセージはと",
            "x",
        ])
    );
}

#[test]
fn unicode_tag_names_and_subjects() {
    let Some(rs) = both(
        &Case::new("unicode", proj(&[MAIN_REPO, r"annotation_regexp: '^v'"]))
            .tags(vec![
                tag("v1.0-ß", "2015-08-01T10:00:00Z", "Grüße 🎉"),
                light("v2.0_日本", "2015-08-02T10:00:00Z", "日本語"),
                tag("w3", "2015-08-03T10:00:00Z", "not matching"),
            ])
            .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[
            ("2015-08-01 10:00:00", "v1.0-ß", "Grüße 🎉"),
            ("2015-08-02 10:00:00", "v2.0_日本", "日本語"),
        ])
    );
    assert_eq!(
        rs.range("a_0_1"),
        Some((
            "v1.0-ß - v2.0_日本".into(),
            "a_0_1;;2015-08-01 10:00:00;2015-08-02 10:00:00".into()
        ))
    );
    rs.expect_line(
        0,
        "Series: annotations: Date: 2015-08-01: 'v1.0-ß', 'Grüße 🎉'",
    );
}

#[test]
fn repository_without_tags() {
    let Some(rs) = both(
        &Case::new("no_tags", proj(&[MAIN_REPO, REGEXP, START, JOIN]))
            .tags(Vec::new())
            .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Got 0 tags for org/repo, took <dur>");
    assert_eq!(rs.annotations().len(), 2);
    assert_eq!(rs.suffixes(), with_fixed(&["c_b", "c_n"]));
}

#[test]
fn many_tags() {
    // 30 hourly tags → 30 annotation ranges after the 12 fixed ones.
    let dates: Vec<&'static str> = (0..30)
        .map(|i| leak(&format!("2015-08-{:02}T{:02}:00:00Z", 1 + i / 24, i % 24)))
        .collect();
    let tags: Vec<Tag> = dates
        .iter()
        .enumerate()
        .map(|(i, d)| tag(leak(&format!("v{i}")), d, leak(&format!("release {i}"))))
        .collect();
    let Some(rs) = both(&Case::new("many", proj(&[MAIN_REPO])).tags(tags)) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.annotations().len(), 30);
    let sfx = rs.suffixes();
    assert_eq!(sfx.len(), 42);
    assert_eq!(sfx[12], "a_0_1");
    assert_eq!(sfx[40], "a_28_29");
    assert_eq!(sfx[41], "a_29_n");
    let ranges = rs.ranges();
    assert_eq!(ranges[41].0, "2012-07-02 17:00:00");
    assert_eq!(
        rs.range("a_9_10"),
        Some((
            "v9 - v10".into(),
            "a_9_10;;2015-08-01 09:00:00;2015-08-01 10:00:00".into()
        ))
    );
}

// ---------------------------------------------------- broken git output

#[test]
fn empty_tag_time_is_skipped() {
    let Some(rs) = both(
        &Case::new("empty_time", proj(&[MAIN_REPO, START]))
            .script(Script::Fake(
                "printf 'v1♂♀♂♀no time\\nv2♂♀1441102500♂♀ok\\n'",
            ))
            .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Empty time returned for repo: org/repo, tag: v1");
    rs.expect_line(0, "Got 1 tags for org/repo, took <dur>");
    assert_eq!(
        rs.annotations(),
        s3(&[
            (
                "2015-06-01 00:00:00",
                "Project start date",
                "2015-06-01 - project starts"
            ),
            ("2015-09-01 10:00:00", "v2", "ok"),
        ])
    );
}

#[test]
fn invalid_tag_time_is_reported_and_skipped() {
    // Reported even without GHA2DB_DEBUG.
    let Some(rs) = both(
        &Case::new("bad_time", proj(&[MAIN_REPO])).script(Script::Fake(
            "printf 'v1♂♀abc♂♀msg\\nv2♂♀12.5♂♀m\\nv3♂♀1441102500♂♀ok\\nv4♂♀ 1♂♀x\\n'",
        )),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Invalid time returned for repo: org/repo, tag: v1: 'v1♂♀abc♂♀msg'",
    );
    rs.expect_line(
        0,
        "Invalid time returned for repo: org/repo, tag: v2: 'v2♂♀12.5♂♀m'",
    );
    rs.expect_line(
        0,
        "Invalid time returned for repo: org/repo, tag: v4: 'v4♂♀ 1♂♀x'",
    );
    assert_eq!(rs.annotations(), s3(&[("2015-09-01 10:00:00", "v3", "ok")]));
}

#[test]
fn two_fields_are_fatal() {
    let Some(rs) = both(
        &Case::new("two_fields", proj(&[MAIN_REPO]))
            .script(Script::Fake("printf 'v0♂♀1441102500♂♀fine\\nv1♂♀123\\n'")),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("invalid tagData returned for repo: org/repo: 'v1♂♀123'")
    );
    assert!(rs.tables().is_empty());
}

#[test]
fn four_fields_are_fatal() {
    let Some(rs) = both(
        &Case::new("four_fields", proj(&[MAIN_REPO]))
            .script(Script::Fake("printf 'v1♂♀1441102500♂♀a♂♀b\\n'")),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("invalid tagData returned for repo: org/repo: 'v1♂♀1441102500♂♀a♂♀b'")
    );
}

#[test]
fn garbage_line_is_fatal_even_when_regexp_would_skip_it() {
    // The field count is checked before the regexp.
    let Some(rs) = both(
        &Case::new("garbage", proj(&[MAIN_REPO, REGEXP]))
            .script(Script::Fake("printf 'garbage\\n'")),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("invalid tagData returned for repo: org/repo: 'garbage'")
    );
}

#[test]
fn lines_are_trimmed_and_blank_lines_skipped() {
    let Some(rs) = both(&Case::new("trim", proj(&[MAIN_REPO])).script(Script::Fake(
        "printf '\\n  v1♂♀1441102500♂♀ spaced  \\r\\n\\r\\n\\tv2♂♀1441106100♂♀x\\n\\302\\240v3♂♀1441109700♂♀nbsp\\302\\240\\n   \\n'",
    ))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[
            ("2015-09-01 10:00:00", "v1", " spaced"),
            ("2015-09-01 11:00:00", "v2", "x"),
            ("2015-09-01 12:00:00", "v3", "nbsp"),
        ])
    );
}

#[test]
fn control_characters_in_messages() {
    let Some(rs) = both(&Case::new("control", proj(&[MAIN_REPO])).script(Script::Fake(
        "printf 'v1♂♀1441102500♂♀a\\rb\\tc\\nv2♂♀1441106100♂♀nul\\000byte\\nv3♂♀1441109700♂♀tab\\tin\\tname\\n'",
    ))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // `\\r` / `\\t` → space; NUL removed when written.
    assert_eq!(
        rs.annotations(),
        s3(&[
            ("2015-09-01 10:00:00", "v1", "a b c"),
            ("2015-09-01 11:00:00", "v2", "nulbyte"),
            ("2015-09-01 12:00:00", "v3", "tab in name"),
        ])
    );
}

#[test]
fn invalid_utf8_is_dropped_when_written() {
    // Go cuts the message at 40 bytes and drops invalid bytes only when the
    // point is written; a tag name with invalid bytes loses them as well.
    let Some(rs) = both(&Case::new("bad_utf8", proj(&[MAIN_REPO])).script(Script::Fake(
        "printf 'v1♂♀1441102500♂♀caf\\351 latin1\\nv\\3772♂♀1441106100♂♀name\\nv3♂♀1441109700♂♀1234567890123456789012345678901234567\\303\\251tail\\n'",
    ))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[
            ("2015-09-01 10:00:00", "v1", "caf latin1"),
            ("2015-09-01 11:00:00", "v2", "name"),
            // 37 ASCII bytes + `é` (2 bytes) = 39 bytes, then `t` → 40
            (
                "2015-09-01 12:00:00",
                "v3",
                "1234567890123456789012345678901234567ét"
            ),
        ])
    );
    assert_eq!(
        rs.range("a_1_2"),
        Some((
            "v2 - v3".into(),
            "a_1_2;;2015-09-01 11:15:00;2015-09-01 12:15:00".into()
        ))
    );
}

#[test]
fn invalid_utf8_tag_name_does_not_match_regexp() {
    let Some(rs) = both(
        &Case::new(
            "bad_utf8_re",
            proj(&[MAIN_REPO, r"annotation_regexp: '^v\d'"]),
        )
        .script(Script::Fake(
            "printf 'v\\3772♂♀1441106100♂♀name\\nv3♂♀1441109700♂♀ok\\n'",
        )),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.annotations(), s3(&[("2015-09-01 12:00:00", "v3", "ok")]));
}

#[test]
fn duplicate_tag_names_in_different_hours_are_kept() {
    let Some(rs) = both(
        &Case::new("dup_names", proj(&[MAIN_REPO])).script(Script::Fake(
            "printf 'v1♂♀1441102500♂♀a\\nv1♂♀1441106100♂♀b\\n'",
        )),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[
            ("2015-09-01 10:00:00", "v1", "a"),
            ("2015-09-01 11:00:00", "v1", "b"),
        ])
    );
    assert_eq!(
        rs.range("a_0_1"),
        Some((
            "v1 - v1".into(),
            "a_0_1;;2015-09-01 10:15:00;2015-09-01 11:15:00".into()
        ))
    );
}

#[test]
fn unsorted_output_is_sorted_by_date() {
    let Some(rs) = both(
        &Case::new("unsorted", proj(&[MAIN_REPO])).script(Script::Fake(
            "printf 'z♂♀1441109700♂♀third\\na♂♀1441102500♂♀first\\nm♂♀1441106100♂♀second\\n'",
        )),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let titles: Vec<String> = rs.annotations().into_iter().map(|a| a.1).collect();
    assert_eq!(titles, strs(&["a", "m", "z"]));
    assert_eq!(rs.range("a_0_1").unwrap().0, "a - m");
    assert_eq!(rs.range("a_1_2").unwrap().0, "m - z");
    assert_eq!(rs.range("a_2_n").unwrap().0, "z - now");
}

#[test]
fn negative_and_far_future_times() {
    let Some(rs) = both(&Case::new("odd_times", proj(&[MAIN_REPO]))
        .script(Script::Fake(
            "printf 'v1♂♀-100♂♀before epoch\\nv2♂♀0♂♀epoch\\nv3♂♀4102444800♂♀year 2100\\nv4♂♀+1441102500♂♀plus sign\\n'",
        ))
        .debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Skipping annotation 1969-12-31 23:58:20 +0000 <zone> because it is before 2012-07-01 00:00:00 +0000 <zone>",
    );
    rs.expect_line(
        0,
        "Skipping annotation 1970-01-01 00:00:00 +0000 <zone> because it is before 2012-07-01 00:00:00 +0000 <zone>",
    );
    assert_eq!(
        rs.annotations(),
        s3(&[
            ("2015-09-01 10:00:00", "v4", "plus sign"),
            ("2100-01-01 00:00:00", "v3", "year 2100"),
        ])
    );
}

#[test]
fn script_failure_after_output_is_fatal() {
    let Some(rs) = both(
        &Case::new("script_fails", proj(&[MAIN_REPO])).script(Script::Fake(
            "printf 'v1♂♀1441102500♂♀ok\\n'; echo 'something broke' >&2; exit 4",
        )),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(rs.error(0).as_deref(), Some("exit status 4"));
    // The captured stdout and stderr are echoed before the error.
    rs.expect_line(0, "v1♂♀1441102500♂♀ok");
    rs.expect_line(0, "STDERR:");
    rs.expect_line(0, "something broke");
    assert!(rs.tables().is_empty());
}

#[test]
fn script_stderr_is_ignored_on_success() {
    let Some(rs) = both(
        &Case::new("script_stderr", proj(&[MAIN_REPO]))
            .script(Script::Fake(
                "echo 'warning: whatever' >&2; printf 'v1♂♀1441102500♂♀ok\\n'",
            ))
            .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_no_line(0, "warning: whatever");
    assert_eq!(rs.annotations(), s3(&[("2015-09-01 10:00:00", "v1", "ok")]));
}

#[test]
fn script_gets_repo_path_and_git_terminal_prompt() {
    let Some(rs) = both(&Case::new("script_env", proj(&[MAIN_REPO])).script(Script::Fake(
        // The scratch directory would not fit into the 40 bytes: print the
        // part of the argument after `/repos/`.
        "printf 'arg♂♀1441102500♂♀%s\\n' \"${1#*/repos/}\"; printf 'prompt♂♀1441106100♂♀%s\\n' \"$GIT_TERMINAL_PROMPT\"",
    ))) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let a = rs.annotations();
    assert_eq!(a[0].2, "org/repo");
    assert_eq!(a[1].2, "0");
}

#[test]
fn script_empty_output_with_no_dates_writes_only_fixed_ranges() {
    let Some(rs) =
        both(&Case::new("script_empty", proj(&[MAIN_REPO])).script(Script::Fake("true")))
    else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.annotations(), Vec::new());
    assert_eq!(rs.suffixes(), strs(FIXED));
}

// -------------------------------------------------------------- time zones

#[test]
fn local_time_zone_shifts_tag_times() {
    // Go stamps tags with `time.Unix` (local) and relabels the wall clock as
    // UTC: in Warsaw (CEST) `10:15Z` becomes `12:00` / `12:15`.
    let Some(rs) = both(
        &Case::new("tz_warsaw", full())
            .env("TZ", "Europe/Warsaw")
            .debug(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Skipping annotation {v1.1.1 same hour 2015-09-01 12:45:00 +0200 <zone>} because its hour date is the same as the previous one",
    );
    let a = rs.annotations();
    assert_eq!(
        a[1],
        (
            "2015-07-16 01:00:00".into(),
            "v0.9".into(),
            "sorted by date, not by name".into()
        )
    );
    assert_eq!(
        a[2],
        ("2015-08-01 12:00:00".into(), "v1.0".into(), "first".into())
    );
    // v2.0 at 2016-02-05 00:00Z is 01:00 CET.
    assert_eq!(
        a[5],
        (
            "2016-02-05 01:00:00".into(),
            "v2.0".into(),
            "multi line msg".into()
        )
    );
    // Milestone dates from the yaml are UTC and stay as they are.
    assert_eq!(a[0].0, "2015-06-01 00:00:00");
    assert_eq!(
        rs.range("a_0_1"),
        Some((
            "v0.9 - v1.0".into(),
            "a_0_1;;2015-07-16 01:59:59;2015-08-01 12:15:00".into()
        ))
    );
}

#[test]
fn western_time_zone() {
    let Some(rs) = both(&Case::new("tz_ny", full()).env("TZ", "America/New_York")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let a = rs.annotations();
    // 2015-07-15 23:59:59Z is 19:59:59 EDT the same day.
    assert_eq!(a[1].0, "2015-07-15 19:00:00");
    assert_eq!(a[2].0, "2015-08-01 06:00:00");
    // 2016-02-05 00:00Z is 2016-02-04 19:00 EST.
    assert_eq!(a[5].0, "2016-02-04 19:00:00");
    assert_eq!(
        rs.range("a_2_3"),
        Some((
            "v1.1 - v2.0".into(),
            "a_2_3;;2015-09-01 06:15:00;2016-02-04 19:00:00".into()
        ))
    );
}

#[test]
fn half_hour_time_zone() {
    let Some(rs) = both(&Case::new("tz_kolkata", full()).env("TZ", "Asia/Kolkata")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let a = rs.annotations();
    assert_eq!(a[2].0, "2015-08-01 15:00:00");
    assert_eq!(
        rs.range("a_1_2"),
        Some((
            "v1.0 - v1.1".into(),
            "a_1_2;;2015-08-01 15:45:00;2015-09-01 15:45:00".into()
        ))
    );
}

#[test]
fn time_zone_moves_tags_across_the_hour_dedupe() {
    // Two tags 10:50Z and 11:10Z are in different UTC hours but in Kolkata
    // (+05:30) both fall into the 16:xx hour → the second is dropped.
    let Some(rs) = both(
        &Case::new("tz_dedupe", proj(&[MAIN_REPO]))
            .tags(vec![
                tag("a", "2015-08-01T10:50:00Z", "first"),
                tag("b", "2015-08-01T11:10:00Z", "second"),
            ])
            .env("TZ", "Asia/Kolkata"),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.annotations(),
        s3(&[("2015-08-01 16:00:00", "a", "first")])
    );
    assert_eq!(rs.suffixes(), with_fixed(&["a_0_n"]));
}
