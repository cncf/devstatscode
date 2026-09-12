//! Go ⇄ Rust compatibility tests for `get_repos`.
//!
//! Every case gets, per side, a scratch database
//! (`dbtest_get_repos_<case>_<go|rs>`, optionally a second one for a second
//! project) seeded with the full DevStats schema plus the case's rows, and a
//! scratch directory `<dir>`:
//!
//! * `<dir>/up/<org>_<repo>` — a deterministic "upstream" repository (fixed
//!   authors and dates, so the SHAs are identical on both sides): five
//!   commits including a multi-file commit, a deletion, a message with
//!   `Signed-off-by` / `Reviewed-by` trailers and non-ASCII characters and a
//!   commit by another author;
//! * `<dir>/orig/<org>_<repo>` — its bare clone taken after commit 4 (stands
//!   in for GitHub: what `git_reset_pull.sh` fetches from and what the fake
//!   `git clone` copies; commit 5 exists upstream only until a case fetches
//!   it);
//! * `<dir>/repos/<org>/<repo>` — the working clone (`GHA2DB_REPOS_DIR`),
//!   reset to commit 3 so a pull has something to fetch — or absent;
//! * `<dir>/work` — the working directory: `projects.yaml` (`GHA2DB_LOCAL`),
//!   `git/*.sh` (`GHA2DB_LOCAL_CMD`: the real scripts from `../devstats/git/`,
//!   wrappers failing on chosen SHAs, or stand-ins), `util_sql/*.sql` and
//!   optionally `hide/hide.csv`; alternatively `<dir>/data` + `<dir>/bin` for
//!   the `GHA2DB_DATADIR` / PATH lookup mode;
//! * `<dir>/bin/git` — optionally a fake `git` whose `clone` copies the bare
//!   origin (or fails like a missing GitHub repository) and delegates every
//!   other sub-command to the real git.
//!
//! Compared per run: exit code, stdout (scratch paths, database names,
//! durations and the `now()`-derived orphan window masked; as a sorted
//! multiset for multi-threaded cases), the `Error: '…'` / `Warning …` stderr
//! lines, afterwards every table of the database(s) — columns, indexes and
//! rows with `now()`-stamped cells masked — and the working clone's `HEAD`.
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise), `git` and `bash` on the PATH.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{fixture, go_binary, is_go_duration, run, rust_binary, Invocation, Outcome};
use devstatscode::chrono::{NaiveDate, Utc};
use devstatscode::hash::negative_artificial_id;
use regex::Regex;
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("get_repos")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_get_repos"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";
const ZERO_SHA: &str = "0000000000000000000000000000000000000000";
/// SHA-1 of `dev@example.com`: `hide.csv` rows are SHA-1s of the values to
/// anonymise, which then show up as `anon-<sha1>`.
const DEV_EMAIL_SHA1: &str = "8157080df4e553e1df0bce31c7193971fe7cdd6c";
/// Wide enough for the 2020 test commits to fall into the orphan window.
const WIDE_RANGE: &str = "5000 days";

/// `took 1.234ms`, `(took 1.2s)`, `in: 3m1.5s` — Go `time.Duration`s.
static DURATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(took |in: )([0-9][0-9.hmsµn]*)").unwrap());
/// The `now()`-derived orphan window: `… since 2012-07-01 00:00:00.123456 +0000 UTC`.
static SINCE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r" since .*$").unwrap());
/// `lib.ProgressInfo`: `3/10 (30.000%), ETA: 1.2s: org/repo` — only after
/// 10 seconds, so masked out entirely.
static PROGRESS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\d+/\d+ \(\d+\.\d{3}%\), ETA: ").unwrap());
/// A `now()` argument in a dumped SQL argument list: `[1:2026-09-11 23:28:39 2:…`.
static ARG_NOW: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\[\d+:)\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?").unwrap());
/// A timestamp cell (`2020-01-02 01:00:00`, `2026-09-11 20:25:01.123456+02`).
static TIMESTAMP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:\d{2})?)?$").unwrap()
});

/// Run `git` with a fixed identity / date and no user or system configuration
/// (signing keys, hooks templates …); returns stdout.
fn git(dir: &Path, date: &str, extra_env: &[(&str, &str)], args: &[&str]) -> String {
    let mut cmd = Command::new("git");
    cmd.args([
        "-c",
        "commit.gpgsign=false",
        "-c",
        "tag.gpgsign=false",
        "-c",
        "init.defaultBranch=main",
        "-c",
        "user.name=Dev",
        "-c",
        "user.email=dev@example.com",
    ])
    .args(args)
    .current_dir(dir)
    .env("GIT_CONFIG_GLOBAL", "/dev/null")
    .env("GIT_CONFIG_NOSYSTEM", "1")
    .env("GIT_CONFIG_COUNT", "0")
    .env("GIT_AUTHOR_DATE", date)
    .env("GIT_COMMITTER_DATE", date)
    .env_remove("GIT_DIR")
    .env_remove("GIT_WORK_TREE");
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("cannot run git: {e}"));
    assert!(
        out.status.success(),
        "git {args:?} in {} failed: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// `org/repo` → `org_repo` (directory names of the upstream / origin copies).
fn flat(org_repo: &str) -> String {
    org_repo.replace('/', "_")
}

/// Build the upstream repository of `org_repo` under `<dir>/up/`, its bare
/// clone under `<dir>/orig/` (taken after commit 4) and return the five SHAs
/// (oldest first). `salt` (the repository name) goes into the first file so
/// every repository has distinct SHAs.
fn build_upstream(dir: &Path, org_repo: &str) -> Vec<String> {
    let up = dir.join("up").join(flat(org_repo));
    fs::create_dir_all(&up).unwrap();
    git(&up, "2020-01-01T00:00:00Z", &[], &["init", "-q"]);
    // 1: a single file.
    fs::write(up.join("a.txt"), format!("a {org_repo}\n")).unwrap();
    git(&up, "2020-01-01T00:00:00Z", &[], &["add", "a.txt"]);
    git(
        &up,
        "2020-01-01T00:00:00Z",
        &[],
        &["commit", "-q", "-m", "first commit"],
    );
    // 2: two files (one matching `files_skip_pattern`), trailers.
    fs::create_dir_all(up.join("src")).unwrap();
    fs::write(up.join("src").join("main.go"), "line1\nline2\n").unwrap();
    fs::write(up.join("b.md"), "b\n").unwrap();
    git(&up, "2020-01-02T00:00:00Z", &[], &["add", "."]);
    git(
        &up,
        "2020-01-02T00:00:00Z",
        &[],
        &[
            "commit",
            "-q",
            "-m",
            "second commit\n\nSigned-off-by: Alice Smith <alice@example.com>\nReviewed-by: Bob <bob@example.com>",
        ],
    );
    // 3: a modification plus a deletion, non-ASCII / quotes in the message.
    fs::write(
        up.join("src").join("main.go"),
        "line1\nline2 changed\nline3\n",
    )
    .unwrap();
    git(&up, "2020-01-03T00:00:00Z", &[], &["rm", "-q", "a.txt"]);
    git(
        &up,
        "2020-01-03T00:00:00Z",
        &[],
        &[
            "commit",
            "-q",
            "-a",
            "-m",
            "third commit ünïcode ♂♀ 'quote' \"dq\"",
        ],
    );
    // 4: another author (a known actor).
    fs::write(up.join("c.txt"), "c\n").unwrap();
    git(&up, "2020-01-04T00:00:00Z", &[], &["add", "c.txt"]);
    git(
        &up,
        "2020-01-04T00:00:00Z",
        &[
            ("GIT_AUTHOR_NAME", "Carol Jones"),
            ("GIT_AUTHOR_EMAIL", "carol@example.com"),
        ],
        &["commit", "-q", "-m", "fourth commit"],
    );
    // The bare "GitHub" copy: commits 1–4.
    let orig = dir.join("orig").join(flat(org_repo));
    fs::create_dir_all(orig.parent().unwrap()).unwrap();
    git(
        dir,
        "2020-01-04T00:00:00Z",
        &[],
        &[
            "clone",
            "-q",
            "--bare",
            up.to_str().unwrap(),
            orig.to_str().unwrap(),
        ],
    );
    // 5: upstream only, until a case fetches it into the bare copy.
    fs::write(up.join("d.txt"), "d\n").unwrap();
    git(&up, "2020-01-05T00:00:00Z", &[], &["add", "d.txt"]);
    git(
        &up,
        "2020-01-05T00:00:00Z",
        &[],
        &["commit", "-q", "-m", "fifth commit"],
    );
    git(
        &up,
        "2020-01-05T00:00:00Z",
        &[],
        &["log", "--reverse", "--format=%H"],
    )
    .split_whitespace()
    .map(str::to_string)
    .collect()
}

/// Clone the bare copy of `org_repo` into `<dir>/repos/<org>/<repo>` and
/// reset it to `shas[reset_idx]`.
fn clone_working(dir: &Path, org_repo: &str, shas: &[String], reset_idx: usize) -> PathBuf {
    let orig = dir.join("orig").join(flat(org_repo));
    let target = dir.join("repos").join(org_repo);
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    git(
        dir,
        "2020-01-04T00:00:00Z",
        &[],
        &[
            "clone",
            "-q",
            orig.to_str().unwrap(),
            target.to_str().unwrap(),
        ],
    );
    git(
        &target,
        "2020-01-04T00:00:00Z",
        &[],
        &["reset", "-q", "--hard", &shas[reset_idx]],
    );
    target
}

/// Where a `git/*.sh` script comes from.
#[derive(Clone)]
enum Script {
    /// The real script (`compat/fixtures/get_repos/git/<name>`).
    Real,
    /// A stand-in `bash` script body.
    Fake(&'static str),
    /// The real script renamed to `<stem>_real.sh` plus a `bash` wrapper
    /// (`$REAL` points at the real one) running `body` first.
    Wrap(&'static str),
    /// No script at all.
    Missing,
}

/// The working clone of the main repository.
#[derive(Clone, Copy)]
enum Repo {
    /// Cloned from the bare copy and reset to commit `n` (0-based).
    Cloned(usize),
    /// Not cloned at all.
    Missing,
}

/// One step of a case.
#[derive(Clone)]
enum Step {
    /// Run both binaries with extra environment and compare.
    Run(Vec<(&'static str, &'static str)>),
    /// Execute SQL on the side's (first) database.
    Sql(&'static str),
    /// Execute SQL on the side's second database.
    Sql2(&'static str),
    /// Run a `bash -c` command in the scratch directory.
    Shell(&'static str),
}

struct Case {
    name: &'static str,
    /// `projects.yaml` body (placeholders expanded).
    yaml: &'static str,
    /// Do not write `projects.yaml` at all.
    no_yaml: bool,
    /// SQL seeding the first database (placeholders expanded).
    seed: Vec<&'static str>,
    /// Create a second database, seeded with this SQL.
    db2: Option<Vec<&'static str>>,
    /// Environment of every run (placeholders expanded).
    env: Vec<(&'static str, &'static str)>,
    steps: Vec<Step>,
    /// `git/*.sh` overrides (everything else is the real script).
    scripts: Vec<(&'static str, Script)>,
    repo: Repo,
    /// `git remote remove origin` in the working clone (fetch / pull fail,
    /// no `refs/remotes/origin/HEAD`).
    remove_origin: bool,
    /// Extra repositories (`org/name`) built and cloned at commit 4.
    extra: Vec<&'static str>,
    /// Put a fake `git` first on the PATH (its `clone` copies the bare
    /// origin of `org/repo` or, with `fake_git_fail`, fails).
    fake_git: bool,
    fake_git_fail: bool,
    /// Do not set `GHA2DB_REPOS_DIR` (the default is `$HOME/devstats_repos/`).
    no_repos_dir: bool,
    /// `hide/hide.csv` rows (SHA-1s) — `None`: no file.
    hide: Option<&'static str>,
    /// `GHA2DB_LOCAL=1` (`./projects.yaml`, `./util_sql/`) — off for the
    /// `GHA2DB_DATADIR` mode (`<dir>/data/…`).
    local: bool,
    /// `GHA2DB_LOCAL_CMD=1` (`./git/*.sh`) — off for the PATH lookup mode
    /// (`<dir>/bin/*.sh`).
    local_cmd: bool,
    /// Compare stdout at all.
    stdout: bool,
    /// Compare stdout as a sorted multiset (multi-threaded runs).
    unordered: bool,
    /// Compare the `Error: '…'` / `Warning …` stderr lines (off when their
    /// wording legitimately differs).
    compare_errors: bool,
    /// With `stdout` off: both sides must still print an `Error: '…'` line.
    require_error: bool,
    /// Compare `HEAD` of the working clone afterwards.
    check_head: bool,
    /// Compare the database contents afterwards (off where a fatal error
    /// leaves them depending on how far the run got).
    compare_data: bool,
}

impl Case {
    fn new(name: &'static str) -> Self {
        Case {
            name,
            yaml: PROJ_YAML,
            no_yaml: false,
            seed: default_seed(),
            db2: None,
            env: vec![
                ("GHA2DB_PROJECT", "proj"),
                ("TZ", "UTC"),
                ("GHA2DB_ST", "1"),
                ("GHA2DB_FETCH_COMMITS_MODE", "0"),
            ],
            steps: vec![Step::Run(Vec::new())],
            scripts: Vec::new(),
            repo: Repo::Cloned(2),
            remove_origin: false,
            extra: Vec::new(),
            fake_git: false,
            fake_git_fail: false,
            no_repos_dir: false,
            hide: None,
            local: true,
            local_cmd: true,
            stdout: true,
            unordered: false,
            compare_errors: true,
            require_error: true,
            check_head: true,
            compare_data: true,
        }
    }
    fn yaml(mut self, yaml: &'static str) -> Self {
        self.yaml = yaml;
        self
    }
    fn no_yaml(mut self) -> Self {
        self.no_yaml = true;
        self
    }
    fn seed(mut self, seed: Vec<&'static str>) -> Self {
        self.seed = seed;
        self
    }
    /// Append SQL to the default seed.
    fn also(mut self, sql: &'static str) -> Self {
        self.seed.push(sql);
        self
    }
    fn db2(mut self, seed: Vec<&'static str>) -> Self {
        self.db2 = Some(seed);
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
    fn process_repos(self) -> Self {
        self.env("GHA2DB_PROCESS_REPOS", "1")
    }
    fn process_commits(self) -> Self {
        self.env("GHA2DB_PROCESS_COMMITS", "1")
    }
    fn mode(self, m: &'static str) -> Self {
        self.env("GHA2DB_FETCH_COMMITS_MODE", m)
    }
    fn orphan(self, range: &'static str) -> Self {
        self.env("GHA2DB_RESTORE_ORPHAN_COMMITS", "1")
            .env("GHA2DB_ORPHAN_COMMITS_RANGE", range)
    }
    fn steps(mut self, steps: Vec<Step>) -> Self {
        self.steps = steps;
        self
    }
    fn script(mut self, name: &'static str, script: Script) -> Self {
        self.scripts.push((name, script));
        self
    }
    fn repo(mut self, repo: Repo) -> Self {
        self.repo = repo;
        self
    }
    fn remove_origin(mut self) -> Self {
        self.remove_origin = true;
        self
    }
    fn extra(mut self, repos: &[&'static str]) -> Self {
        self.extra = repos.to_vec();
        self
    }
    fn fake_git(mut self, fail: bool) -> Self {
        self.fake_git = true;
        self.fake_git_fail = fail;
        self
    }
    fn no_repos_dir(mut self) -> Self {
        self.no_repos_dir = true;
        self
    }
    fn hide(mut self, rows: &'static str) -> Self {
        self.hide = Some(rows);
        self
    }
    fn datadir_mode(mut self) -> Self {
        self.local = false;
        self.local_cmd = false;
        self
    }
    fn unordered(mut self) -> Self {
        self.unordered = true;
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
    /// Only the exit code and the presence of an error line.
    fn code_only(mut self) -> Self {
        self.stdout = false;
        self.compare_errors = false;
        self
    }
    /// Only the exit code (successful runs whose stdout legitimately differs).
    fn code_only_ok(mut self) -> Self {
        self.stdout = false;
        self.compare_errors = false;
        self.require_error = false;
        self
    }
    fn no_head(mut self) -> Self {
        self.check_head = false;
        self
    }
    fn no_data(mut self) -> Self {
        self.compare_data = false;
        self
    }
}

/// The default `projects.yaml`: one project on the first database skipping
/// Markdown files.
const PROJ_YAML: &str = "projects:\n  proj:\n    name: Proj\n    command_line: [ \"x\" ]\n    psql_db: {db}\n    order: 1\n    files_skip_pattern: '\\.md$'\n";
/// Two projects on two databases.
const TWO_YAML: &str = "projects:\n  proj:\n    name: Proj\n    command_line: [ \"x\" ]\n    psql_db: {db}\n    order: 1\n    files_skip_pattern: '\\.md$'\n  proj2:\n    name: Proj2\n    command_line: [ \"y\" ]\n    psql_db: {db2}\n    order: 2\n";
/// The project disabled.
const DISABLED_YAML: &str = "projects:\n  proj:\n    name: Proj\n    command_line: [ \"x\" ]\n    psql_db: {db}\n    order: 1\n    disabled: true\n";
/// No `files_skip_pattern`.
const NO_SKIP_YAML: &str = "projects:\n  proj:\n    name: Proj\n    command_line: [ \"x\" ]\n    psql_db: {db}\n    order: 1\n";

const SEED_REPO: &str =
    "insert into gha_repos(id, name, org_id, org_login) values (100, 'org/repo', 10, 'org');";
const SEED_ACTORS: &str = "insert into gha_actors(id, login, name) values (1, 'alice', 'Alice Smith'), (2, 'bob', 'Bob'), (3, 'carol', 'Carol Jones');
insert into gha_actors_emails(actor_id, email) values (1, 'alice@example.com'), (2, 'bob@example.com'), (3, 'carol@example.com');
insert into gha_actors_names(actor_id, name) values (1, 'Alice Smith'), (2, 'Bob'), (3, 'Carol Jones');";
/// Two PushEvents: commit 2 (before = commit 1) and commits 3–4 (before =
/// commit 2), both with `size` 2 (so the first is "truncated" for mode 2).
const SEED_EVENTS: &str = "insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values
 (1001, 'PushEvent', 1, 100, '2020-01-02 01:00:00', 10, 'alice', 'org/repo'),
 (1002, 'PushEvent', 3, 100, '2020-01-04 01:00:00', 10, 'carol', 'org/repo');
insert into gha_payloads(event_id, push_id, size, ref, head, befor, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values
 (1001, 1, 2, 'refs/heads/main', '{sha1}', '{sha0}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-02 01:00:00'),
 (1002, 2, 2, 'refs/heads/main', '{sha3}', '{sha1}', 'carol', 100, 'org/repo', 'PushEvent', '2020-01-04 01:00:00');";

fn default_seed() -> Vec<&'static str> {
    vec![SEED_REPO, SEED_ACTORS, SEED_EVENTS]
}

/// One table: columns (sorted by name), indexes and rows (every column as
/// text, ordered by all columns).
type TableDump = (Vec<cpg::ColumnInfo>, Vec<cpg::IndexInfo>, Vec<Vec<String>>);

struct Side {
    db: TestDb,
    db2: Option<TestDb>,
    _dir: TempDir,
    /// The scratch directory path (masked as `<dir>`).
    dir_str: String,
    /// SHAs of `org/repo` (oldest first; index 4 is upstream-only).
    shas: Vec<String>,
    outs: Vec<Outcome>,
}

/// A cell of a table dump: scratch paths masked, `now()`-stamped timestamps
/// (within two days of today) replaced by `<now>`.
fn mask_cell(v: &str, dir: &str) -> String {
    let v = v.replace(dir, "<dir>");
    if let Some(c) = TIMESTAMP.captures(&v) {
        if let Ok(d) = NaiveDate::parse_from_str(&c[1], "%Y-%m-%d") {
            let today = Utc::now().date_naive();
            if (d - today).num_days().abs() <= 2 {
                return "<now>".to_string();
            }
        }
    }
    v
}

/// `git rev-parse HEAD` of a repository (`None` when it is not one).
fn head_of(repo: &Path) -> Option<String> {
    if !repo.join(".git").exists() {
        return None;
    }
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

impl Side {
    fn dir(&self) -> &Path {
        Path::new(&self.dir_str)
    }
    fn mask(&self, l: &str) -> String {
        let mut l = l.replace(&self.dir_str, "<dir>");
        if let Some(d2) = &self.db2 {
            l = l.replace(&d2.name, "<db2>");
        }
        let l = l.replace(&self.db.name, "<db>");
        let l = DURATION.replace_all(&l, |c: &regex::Captures| {
            if is_go_duration(&c[2]) {
                format!("{}<dur>", &c[1])
            } else {
                c[0].to_string()
            }
        });
        if l.contains("commits found since ") || l.contains("commits since ") {
            return SINCE.replace(&l, " since <ts>").into_owned();
        }
        ARG_NOW.replace_all(&l, "${1}<now>").into_owned()
    }
    /// All stdout lines of run `i`, masked (progress lines dropped).
    fn lines(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stdout_str()
            .lines()
            .filter(|l| !PROGRESS.is_match(l))
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
    fn expect_no_prefix(&self, i: usize, prefix: &str) {
        let lines = self.lines(i);
        assert!(
            !lines.iter().any(|l| l.starts_with(prefix)),
            "unexpected line starting with {prefix:?} in run #{i}: {lines:#?}"
        );
    }
    /// Assert run `i` printed a line containing `needle`.
    fn expect_contains(&self, i: usize, needle: &str) {
        let lines = self.lines(i);
        assert!(
            lines.iter().any(|l| l.contains(needle)),
            "no line containing {needle:?} in run #{i}: {lines:#?}"
        );
    }
    /// Number of lines of run `i` starting with `prefix`.
    fn count_prefix(&self, i: usize, prefix: &str) -> usize {
        self.lines(i)
            .iter()
            .filter(|l| l.starts_with(prefix))
            .count()
    }
    /// The relevant stderr lines of run `i`: `Error: '…'`, `PqError: …` and
    /// the `Warning …` lines `get_repos` writes to stderr, masked.
    fn stderr_lines(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stderr_str()
            .lines()
            .filter(|l| {
                l.starts_with("Error: '") || l.starts_with("PqError: ") || l.starts_with("Warning")
            })
            .map(|l| self.mask(l))
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
    fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let con = self.db.conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows
    }
    fn query2(&self, sql: &str) -> Vec<Vec<String>> {
        let con = self.db2.as_ref().expect("no second database").conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows
    }
    /// A single-cell query as an integer.
    fn count(&self, sql: &str) -> i64 {
        self.query(sql)[0][0].parse().unwrap()
    }
    /// A single-column query.
    fn column(&self, sql: &str) -> Vec<String> {
        self.query(sql).into_iter().map(|r| r[0].clone()).collect()
    }
    /// SHAs of `gha_commits` in order.
    fn commit_shas(&self) -> Vec<String> {
        self.column("select sha from gha_commits order by sha")
    }
    /// `(sha, path, size, dt, ext)` of `gha_commits_files`.
    fn files(&self) -> Vec<Vec<String>> {
        self.query(
            "select sha, path, size::text, dt::text, ext from gha_commits_files order by sha, path",
        )
    }
    /// `(sha, reason)` of `gha_skip_commits`.
    fn skipped(&self) -> Vec<(String, String)> {
        self.query("select sha, reason::text from gha_skip_commits order by sha")
            .into_iter()
            .map(|r| (r[0].clone(), r[1].clone()))
            .collect()
    }
    /// `(sha, loc_added, loc_removed, files_changed)` of `gha_commits`.
    fn loc(&self) -> Vec<Vec<String>> {
        self.query("select sha, loc_added::text, loc_removed::text, files_changed::text from gha_commits order by sha")
    }
    /// `HEAD` of the working clone of `org/repo`.
    fn head(&self) -> Option<String> {
        head_of(&self.dir().join("repos").join("org").join("repo"))
    }
    /// Every table of the database(s) with structure and contents.
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
                .map(|r| r.into_iter().map(|v| mask_cell(&v, dir)).collect())
                .collect::<Vec<Vec<String>>>();
                let mut rows = rows;
                rows.sort();
                res.insert(format!("{prefix}{t}"), (cols, idx, rows));
            }
        }
        let mut res = BTreeMap::new();
        let con = self.db.conn();
        dump(&con, "", &self.dir_str, &mut res);
        con.close();
        if let Some(d2) = &self.db2 {
            let con = d2.conn();
            dump(&con, "db2.", &self.dir_str, &mut res);
            con.close();
        }
        res
    }
}

const SCRIPT_NAMES: &[&str] = &[
    "git_reset_pull.sh",
    "git_files.sh",
    "git_loc.sh",
    "git_commits.sh",
    "git_commits_range.sh",
];

fn make_executable(p: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(p, fs::Permissions::from_mode(0o755)).unwrap();
    }
}

/// Absolute path of the real `git` (the fake one shadows it on the PATH).
fn real_git() -> String {
    let out = Command::new("sh")
        .args(["-c", "command -v git"])
        .output()
        .expect("sh");
    let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(!p.is_empty(), "git not found on the PATH");
    p
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let schema = fs::read_to_string(fixture("structure/full_structure.sql")).unwrap();
    let db = TestDb::fresh(&format!("get_repos_{}_{}", case.name, suffix))?;
    db.exec(&schema);
    let db2 = case.db2.as_ref().map(|_| {
        let d = TestDb::fresh(&format!("get_repos_{}_2_{}", case.name, suffix)).unwrap();
        d.exec(&schema);
        d
    });
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_get_repos_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    let dir_str = dir.path().to_str().unwrap().to_string();
    let work = dir.path().join("work");
    let home = dir.path().join("home");
    let bin_dir = dir.path().join("bin");
    let data = dir.path().join("data");
    for d in [&work, &home, &bin_dir, &data, &dir.path().join("repos")] {
        fs::create_dir_all(d).unwrap();
    }

    // Repositories.
    let shas = build_upstream(dir.path(), "org/repo");
    for r in &case.extra {
        let s = build_upstream(dir.path(), r);
        clone_working(dir.path(), r, &s, 3);
    }
    if let Repo::Cloned(n) = case.repo {
        let target = clone_working(dir.path(), "org/repo", &shas, n);
        if case.remove_origin {
            git(
                &target,
                "2020-01-04T00:00:00Z",
                &[],
                &["remote", "remove", "origin"],
            );
        }
    }

    let db2_name = db2.as_ref().map(|d| d.name.clone()).unwrap_or_default();
    let expand = |s: &str| {
        let mut s = s
            .replace("{dir}", &dir_str)
            .replace("{home}", home.to_str().unwrap())
            .replace("{db2}", &db2_name)
            .replace("{db}", &db.name)
            .replace("{zero}", ZERO_SHA);
        for (i, sha) in shas.iter().enumerate() {
            s = s.replace(&format!("{{sha{i}}}"), sha).replace(
                &format!("{{nid{i}}}"),
                &negative_artificial_id(&["PushEvent", "org/repo", sha]).to_string(),
            );
        }
        s
    };

    // Seeds.
    for sql in &case.seed {
        db.exec(&expand(sql));
    }
    if let (Some(d2), Some(seed2)) = (&db2, &case.db2) {
        for sql in seed2 {
            d2.exec(&expand(sql));
        }
    }

    // projects.yaml, util_sql/, hide/hide.csv — ./ (GHA2DB_LOCAL) or the
    // data directory (GHA2DB_DATADIR).
    let data_dir = if case.local {
        work.clone()
    } else {
        data.clone()
    };
    if !case.no_yaml {
        fs::write(data_dir.join("projects.yaml"), expand(case.yaml)).unwrap();
    }
    let util = data_dir.join("util_sql");
    fs::create_dir_all(&util).unwrap();
    for e in fs::read_dir(fixture("get_repos/util_sql")).unwrap() {
        let p = e.unwrap().path();
        fs::copy(&p, util.join(p.file_name().unwrap())).unwrap();
    }
    if let Some(rows) = case.hide {
        let hide = data_dir.join("hide");
        fs::create_dir_all(&hide).unwrap();
        fs::write(hide.join("hide.csv"), format!("sha1\n{rows}\n")).unwrap();
    }

    // git/*.sh — ./git/ (GHA2DB_LOCAL_CMD) or <dir>/bin on the PATH.
    let script_dir = if case.local_cmd {
        work.join("git")
    } else {
        bin_dir.clone()
    };
    fs::create_dir_all(&script_dir).unwrap();
    for name in SCRIPT_NAMES {
        let script = case
            .scripts
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| s.clone())
            .unwrap_or(Script::Real);
        let real = fixture(&format!("get_repos/git/{name}"));
        let dest = script_dir.join(name);
        match script {
            Script::Real => {
                fs::copy(&real, &dest).unwrap();
            }
            Script::Fake(body) => {
                fs::write(&dest, format!("#!/bin/bash\n{}\n", expand(body))).unwrap();
            }
            Script::Wrap(body) => {
                let stem = name.strip_suffix(".sh").unwrap();
                let real_dest = script_dir.join(format!("{stem}_real.sh"));
                fs::copy(&real, &real_dest).unwrap();
                make_executable(&real_dest);
                fs::write(
                    &dest,
                    format!(
                        "#!/bin/bash\nREAL=\"$(dirname \"$0\")/{stem}_real.sh\"\n{}\n",
                        expand(body)
                    ),
                )
                .unwrap();
            }
            Script::Missing => continue,
        }
        make_executable(&dest);
    }

    // The fake git.
    if case.fake_git {
        let fake = bin_dir.join("git");
        let fail = if case.fake_git_fail {
            "echo \"fatal: repository '$2' not found\" >&2; exit 128"
        } else {
            "echo \"Cloning into '$3'...\" >&2; exec \"$REAL_GIT\" clone -q \"$TEMPLATE\" \"$3\""
        };
        fs::write(
            &fake,
            format!(
                "#!/bin/bash\nREAL_GIT=\"{}\"\nTEMPLATE=\"{}/orig/org_repo\"\nif [ \"$1\" = \"clone\" ]; then\n  {fail}\nfi\nexec \"$REAL_GIT\" \"$@\"\n",
                real_git(),
                dir_str
            ),
        )
        .unwrap();
        make_executable(&fake);
    }

    // Environment.
    let mut env: Vec<(String, String)> = db
        .env()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    env.push(("HOME".into(), home.to_str().unwrap().to_string()));
    env.push(("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()));
    env.push(("GIT_CONFIG_NOSYSTEM".into(), "1".into()));
    if case.local {
        env.push(("GHA2DB_LOCAL".into(), "1".into()));
    } else {
        env.push(("GHA2DB_DATADIR".into(), format!("{dir_str}/data/")));
    }
    if case.local_cmd {
        env.push(("GHA2DB_LOCAL_CMD".into(), "1".into()));
    }
    if !case.local_cmd || case.fake_git {
        let path = std::env::var("PATH").unwrap_or_default();
        env.push(("PATH".into(), format!("{dir_str}/bin:{path}")));
    }
    if !case.no_repos_dir {
        env.push(("GHA2DB_REPOS_DIR".into(), format!("{dir_str}/repos/")));
    }
    for (k, v) in &case.env {
        env.retain(|(key, _)| key != k);
        env.push((k.to_string(), expand(v)));
    }

    let mut outs = Vec::new();
    for step in &case.steps {
        let extra = match step {
            Step::Run(extra) => extra,
            Step::Sql(sql) => {
                db.exec(&expand(sql));
                continue;
            }
            Step::Sql2(sql) => {
                db2.as_ref().expect("no second database").exec(&expand(sql));
                continue;
            }
            Step::Shell(cmd) => {
                let out = Command::new("bash")
                    .args(["-c", &expand(cmd)])
                    .current_dir(dir.path())
                    .env("GIT_CONFIG_GLOBAL", "/dev/null")
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .env("GIT_CONFIG_COUNT", "0")
                    .output()
                    .expect("bash");
                assert!(
                    out.status.success(),
                    "shell step {cmd:?} failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
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
        db2,
        _dir: dir,
        dir_str,
        shas,
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
        assert_eq!(go.shas, rust.shas, "test repositories differ between sides");
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
            let mut go_lines = go.lines(i);
            let mut rs_lines = rust.lines(i);
            if case.unordered {
                go_lines.sort();
                rs_lines.sort();
            }
            assert_eq!(go_lines, rs_lines, "stdout{ctx}");
            let mut go_err = go.stderr_lines(i);
            let mut rs_err = rust.stderr_lines(i);
            if case.unordered {
                go_err.sort();
                rs_err.sort();
            }
            if case.compare_errors {
                assert_eq!(go_err, rs_err, "stderr{ctx}");
            } else {
                assert_eq!(go_err.len(), rs_err.len(), "stderr line count{ctx}");
            }
        }
        if case.compare_data {
            assert_eq!(
                go.data(),
                rust.data(),
                "database contents (case {:?})",
                case.name
            );
        }
        if case.check_head {
            assert_eq!(
                go.head(),
                rust.head(),
                "HEAD of the working clone (case {:?})",
                case.name
            );
        }
    }
    Some(rust)
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

// -------------------------------------------------- repositories (clone/pull)

#[test]
fn pull_only() {
    let Some(rs) = both(&Case::new("pull_only").process_repos().debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, BANNER);
    rs.expect_line(0, "Pulling org/repo");
    rs.expect_line(0, "Pulled org/repo: took <dur>");
    rs.expect_line(0, "Successfully processed 1/1 repos");
    rs.expect_line(0, "All repos processed in: <dur>");
    // Reset to commit 3 before, the pull brought commit 4.
    assert_eq!(rs.head(), Some(rs.shas[3].clone()));
    assert!(rs.commit_shas().is_empty());
    assert!(rs.stderr_lines(0).is_empty());
}

#[test]
fn pull_quiet() {
    let Some(rs) = both(&Case::new("pull_quiet").process_repos()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.lines(0),
        strs(&[
            BANNER,
            "Successfully processed 1/1 repos",
            "All repos processed in: <dur>",
        ])
    );
}

#[test]
fn pull_fail() {
    // No `origin` remote: `git fetch origin` fails, the script exits 3.
    let Some(rs) = both(
        &Case::new("pull_fail")
            .process_repos()
            .debug()
            .remove_origin(),
    ) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_prefix(
        0,
        "Warning git_reset_pull.sh failed: org/repo (took <dur>): ",
    );
    rs.expect_line(0, "Successfully processed 0/1 repos");
    let err = rs.stderr_lines(0);
    assert_eq!(err.len(), 1, "{err:?}");
    assert!(
        err[0].starts_with("Warning git_reset_pull.sh failed: org/repo (took <dur>): "),
        "{err:?}"
    );
    assert!(err[0].contains("exit status 3"), "{err:?}");
    assert_eq!(rs.head(), Some(rs.shas[2].clone()));
}

#[test]
fn pull_fail_quiet() {
    // Without debug the warning goes to stderr only.
    let Some(rs) = both(&Case::new("pull_fail_quiet").process_repos().remove_origin()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_no_prefix(0, "Warning");
    assert_eq!(rs.stderr_lines(0).len(), 1);
}

#[test]
fn pull_new_commit() {
    // The bare "GitHub" copy receives commit 5 first; the pull fetches it.
    let case = Case::new("pull_new_commit").process_repos().steps(vec![
        Step::Shell("git -C {dir}/orig/org_repo fetch -q {dir}/up/org_repo main:main"),
        Step::Run(Vec::new()),
    ]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.head(), Some(rs.shas[4].clone()));
}

#[test]
fn pull_twice() {
    let case = Case::new("pull_twice")
        .process_repos()
        .debug()
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(1, "Successfully processed 1/1 repos");
    assert_eq!(rs.head(), Some(rs.shas[3].clone()));
}

#[test]
fn clone_ok() {
    let case = Case::new("clone_ok")
        .process_repos()
        .debug()
        .repo(Repo::Missing)
        .fake_git(false);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Cloning org/repo");
    rs.expect_line(0, "Cloned org/repo: took <dur>");
    rs.expect_line(0, "Successfully processed 1/1 repos");
    assert_eq!(rs.head(), Some(rs.shas[3].clone()));
    assert!(rs.stderr_lines(0).is_empty());
}

#[test]
fn clone_fail() {
    let case = Case::new("clone_fail")
        .process_repos()
        .debug()
        .repo(Repo::Missing)
        .fake_git(true);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_prefix(0, "Warning git-clone failed: org/repo (took <dur>): ");
    rs.expect_line(0, "Successfully processed 0/1 repos");
    let err = rs.stderr_lines(0);
    assert_eq!(err.len(), 1, "{err:?}");
    assert!(
        err[0].starts_with("Warning git-clone failed: org/repo (took <dur>): "),
        "{err:?}"
    );
    assert!(err[0].contains("exit status 128"), "{err:?}");
    assert_eq!(rs.head(), None);
}

#[test]
fn clone_fail_quiet() {
    let case = Case::new("clone_fail_quiet")
        .process_repos()
        .repo(Repo::Missing)
        .fake_git(true);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_no_prefix(0, "Warning");
    assert_eq!(rs.stderr_lines(0).len(), 1);
}

#[test]
fn clone_mkdir() {
    // GHA2DB_REPOS_DIR without a trailing slash and not existing yet: the
    // tool creates it and the org subdirectory.
    let case = Case::new("clone_mkdir")
        .process_repos()
        .repo(Repo::Missing)
        .fake_git(false)
        .env("GHA2DB_REPOS_DIR", "{dir}/newrepos");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 1/1 repos");
    assert_eq!(
        head_of(&rs.dir().join("newrepos").join("org").join("repo")),
        Some(rs.shas[3].clone())
    );
}

#[test]
fn default_repos_dir() {
    // No GHA2DB_REPOS_DIR: `$HOME/devstats_repos/`.
    let case = Case::new("default_repos_dir")
        .process_repos()
        .debug()
        .repo(Repo::Missing)
        .fake_git(false)
        .no_repos_dir();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 1/1 repos");
    assert_eq!(
        head_of(
            &rs.dir()
                .join("home")
                .join("devstats_repos")
                .join("org")
                .join("repo")
        ),
        Some(rs.shas[3].clone())
    );
}

#[test]
fn repos_dir_is_file() {
    let case = Case::new("repos_dir_is_file")
        .process_repos()
        .env("GHA2DB_REPOS_DIR", "{dir}/afile/")
        .steps(vec![
            Step::Shell("touch {dir}/afile"),
            Step::Run(Vec::new()),
        ])
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("<dir>/afile: exists, but is not a directory")
    );
}

#[test]
fn org_dir_is_file() {
    let case = Case::new("org_dir_is_file")
        .process_repos()
        .repo(Repo::Missing)
        .steps(vec![
            Step::Shell("touch {dir}/repos/org"),
            Step::Run(Vec::new()),
        ])
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(
        rs.error(0).as_deref(),
        Some("<dir>/repos/org: exists, but is not a directory")
    );
}

#[test]
fn external_info() {
    let case = Case::new("external_info")
        .process_repos()
        .env("GHA2DB_EXTERNAL_INFO", "1")
        .extra(&["org/zeta", "acme/alpha"])
        .also("insert into gha_repos(id, name, org_id, org_login) values (101, 'org/zeta', 10, 'org'), (102, 'acme/alpha', 20, 'acme');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let lines = rs.lines(0);
    let start = lines
        .iter()
        .position(|l| l == "AllRepos:")
        .unwrap_or_else(|| panic!("no AllRepos: {lines:#?}"));
    assert_eq!(
        &lines[start..start + 9],
        &strs(&[
            "AllRepos:",
            "[",
            "  'acme/alpha',",
            "  'org/repo',",
            "  'org/zeta',",
            "]",
            "Final command:",
            "./all_repos_log.sh <dir>/repos/acme/* \\",
            "<dir>/repos/org/*",
        ])[..]
    );
    rs.expect_line(0, "Successfully processed 3/3 repos");
}

#[test]
fn external_info_one_failed() {
    // Failed repositories are left out of the list, orgs are listed anyway.
    let case = Case::new("external_info_one_failed")
        .process_repos()
        .env("GHA2DB_EXTERNAL_INFO", "1")
        .remove_origin()
        .extra(&["acme/alpha"])
        .also("insert into gha_repos(id, name, org_id, org_login) values (102, 'acme/alpha', 20, 'acme');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let lines = rs.lines(0);
    let start = lines.iter().position(|l| l == "AllRepos:").unwrap();
    assert_eq!(
        &lines[start..start + 7],
        &strs(&[
            "AllRepos:",
            "[",
            "  'acme/alpha',",
            "]",
            "Final command:",
            "./all_repos_log.sh <dir>/repos/acme/* \\",
            "<dir>/repos/org/*",
        ])[..]
    );
    rs.expect_line(0, "Successfully processed 1/2 repos");
}

#[test]
fn multi_repo_mt() {
    // Several repositories over several orgs, multi-threaded: the per-repo
    // lines come in any order.
    let case = Case::new("multi_repo_mt")
        .process_repos()
        .debug()
        .no_env("GHA2DB_ST")
        .unordered()
        .extra(&["org/zeta", "acme/alpha", "acme/beta"])
        .also("insert into gha_repos(id, name, org_id, org_login) values (101, 'org/zeta', 10, 'org'), (102, 'acme/alpha', 20, 'acme'), (103, 'acme/beta', 20, 'acme');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 4/4 repos");
    for r in ["org/repo", "org/zeta", "acme/alpha", "acme/beta"] {
        rs.expect_line(0, &format!("Pulled {r}: took <dur>"));
    }
}

#[test]
fn multi_repo_ncpus() {
    let case = Case::new("multi_repo_ncpus")
        .process_repos()
        .no_env("GHA2DB_ST")
        .env("GHA2DB_NCPUS", "2")
        .unordered()
        .extra(&["org/zeta", "acme/alpha"])
        .also("insert into gha_repos(id, name, org_id, org_login) values (101, 'org/zeta', 10, 'org'), (102, 'acme/alpha', 20, 'acme');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 3/3 repos");
}

#[test]
fn same_repo_two_dbs() {
    // `org/repo` in both databases is processed once.
    let case = Case::new("same_repo_two_dbs")
        .unordered()
        .yaml(TWO_YAML)
        .db2(vec![SEED_REPO])
        .process_repos()
        .debug();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(rs.count_prefix(0, "Pulled org/repo"), 1);
    rs.expect_line(0, "Successfully processed 1/1 repos");
}

// ---------------------------------------------------------- configuration

#[test]
fn getreposskip() {
    let case = Case::new("getreposskip")
        .process_repos()
        .process_commits()
        .mode("1")
        .env("GHA2DB_GETREPOSSKIP", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.lines(0),
        strs(&[BANNER, "All repos processed in: <dur>"])
    );
    assert_eq!(rs.head(), Some(rs.shas[2].clone()));
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn nothing_enabled() {
    // Neither repos nor commits processing, backfill off: only the summary.
    let Some(rs) = both(&Case::new("nothing_enabled").debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "dbs: map[<db>:\\.md$]");
    rs.expect_line(0, "repos: map[org:map[org/repo:{}]]");
    rs.expect_line(0, "repoDBs: map[<db>:map[org/repo:{}]]");
    rs.expect_line(0, "All repos processed in: <dur>");
    assert_eq!(rs.head(), Some(rs.shas[2].clone()));
}

#[test]
fn no_dbs_disabled() {
    let case = Case::new("no_dbs_disabled")
        .yaml(DISABLED_YAML)
        .process_repos()
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(rs.error(0).as_deref(), Some("No databases to process"));
}

#[test]
fn no_dbs_env_disabled() {
    let case = Case::new("no_dbs_env_disabled")
        .process_repos()
        .env("GHA2DB_PROJECTS_OVERRIDE", "-proj")
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(rs.error(0).as_deref(), Some("No databases to process"));
}

#[test]
fn no_dbs_projects_commits() {
    let case = Case::new("no_dbs_projects_commits")
        .process_repos()
        .env("GHA2DB_PROJECTS_COMMITS", "other")
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(rs.error(0).as_deref(), Some("No databases to process"));
}

#[test]
fn no_repos() {
    let case = Case::new("no_repos")
        .seed(vec![SEED_ACTORS])
        .process_repos()
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(rs.error(0).as_deref(), Some("No repos to process"));
}

#[test]
fn bad_repo_names() {
    // Only `org/name` shaped names are repositories.
    let case = Case::new("bad_repo_names")
        .debug()
        .also("insert into gha_repos(id, name, org_id, org_login) values (201, 'noslash', 0, ''), (202, 'a/b/c', 0, ''), (203, '/x', 0, ''), (204, 'x/', 0, ''), (205, 'org/repo', 10, 'org');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "repos: map[org:map[org/repo:{}]]");
}

#[test]
fn only_bad_repo_names() {
    let case = Case::new("only_bad_repo_names")
        .seed(vec![
            "insert into gha_repos(id, name, org_id, org_login) values (201, 'noslash', 0, ''), (202, 'a/b/c', 0, '');",
        ])
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert_eq!(rs.error(0).as_deref(), Some("No repos to process"));
}

#[test]
fn projects_commits_one() {
    let case = Case::new("projects_commits_one")
        .yaml(TWO_YAML)
        .db2(vec![
            "insert into gha_repos(id, name, org_id, org_login) values (300, 'org/repo2', 10, 'org');",
        ])
        .debug()
        .mode("1")
        .env("GHA2DB_PROJECTS_COMMITS", "proj");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "dbs: map[<db>:\\.md$]");
    rs.expect_line(0, "repos: map[org:map[org/repo:{}]]");
    rs.expect_no_prefix(0, "FetchCommitsMode=1: processing DB '<db2>'");
    assert_eq!(rs.commit_shas().len(), 3);
}

#[test]
fn projects_commits_both() {
    // Spaces around the names are ignored; `org/repo2` is not cloned.
    let case = Case::new("projects_commits_both")
        .unordered()
        .yaml(TWO_YAML)
        .db2(vec![
            "insert into gha_repos(id, name, org_id, org_login) values (300, 'org/repo2', 10, 'org');",
        ])
        .debug()
        .mode("1")
        .env("GHA2DB_PROJECTS_COMMITS", " proj , proj2 ");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "dbs: map[<db2>: <db>:\\.md$]");
    rs.expect_line(0, "repos: map[org:map[org/repo:{} org/repo2:{}]]");
    rs.expect_line(
        0,
        "FetchCommitsMode=1: processing DB '<db2>' (1 repos, threads 1, batch 1000)",
    );
    rs.expect_line(
        0,
        "backfillRepo(DB=<db2>, repo=org/repo2) error: <db2>: repo not cloned: <dir>/repos/org/repo2",
    );
    assert_eq!(rs.commit_shas().len(), 3);
    assert!(rs.query2("select sha from gha_commits").is_empty());
}

#[test]
fn yaml_missing() {
    let case = Case::new("yaml_missing")
        .no_yaml()
        .process_repos()
        .code_only_errors()
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    assert!(rs.error(0).is_some_and(|e| e.contains("projects.yaml")));
}

#[test]
fn yaml_invalid() {
    let case = Case::new("yaml_invalid")
        .yaml("projects: [\n  not: a map\n")
        .process_repos()
        .code_only()
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
}

#[test]
fn projects_yaml_env() {
    let case = Case::new("projects_yaml_env")
        .process_repos()
        .env("GHA2DB_PROJECTS_YAML", "other.yaml")
        .steps(vec![
            Step::Shell("mv {dir}/work/projects.yaml {dir}/work/other.yaml"),
            Step::Run(Vec::new()),
        ]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 1/1 repos");
}

#[test]
fn datadir_path_mode() {
    // GHA2DB_DATADIR for projects.yaml / util_sql / hide, scripts on the PATH.
    let case = Case::new("datadir_path_mode")
        .datadir_mode()
        .process_repos()
        .process_commits()
        .debug()
        .hide(DEV_EMAIL_SHA1)
        .mode("1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "lib.ReadFile('<dir>/data/projects.yaml'): ok");
    rs.expect_line(0, "Successfully processed 1/1 repos");
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 3 commits and 2 commit roles in: <dur>",
    );
    // The hidden e-mail found via the data directory.
    assert_eq!(
        rs.column(&format!(
            "select author_email from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        vec![format!("anon-{DEV_EMAIL_SHA1}")]
    );
    assert_eq!(rs.files().len(), 4);
}

#[test]
fn script_missing() {
    // No git_reset_pull.sh at all: the pull fails, nothing else does.
    let case = Case::new("script_missing")
        .process_repos()
        .debug()
        .script("git_reset_pull.sh", Script::Missing)
        .code_only_errors();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 0/1 repos");
    assert_eq!(rs.stderr_lines(0).len(), 1);
    assert_eq!(rs.head(), Some(rs.shas[2].clone()));
}

#[test]
fn invalid_mode() {
    let case = Case::new("invalid_mode").mode("abc").code_only().no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    let err = rs.outs[0].stderr_str();
    assert!(
        err.contains("Error: 'strconv.Atoi: parsing \"abc\": invalid syntax'"),
        "{err}"
    );
}

#[test]
fn invalid_batch() {
    let case = Case::new("invalid_batch")
        .env("GHA2DB_GIT_COMMITS_BATCH", "x1")
        .code_only()
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    let err = rs.outs[0].stderr_str();
    assert!(
        err.contains("Error: 'strconv.Atoi: parsing \"x1\": invalid syntax'"),
        "{err}"
    );
}

#[test]
fn negative_mode_ignored() {
    // A negative mode keeps the default (1).
    let Some(rs) = both(&Case::new("negative_mode_ignored").mode("-3").debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "FetchCommitsMode=1: processing DB '<db>' (1 repos, threads 1, batch 1000)",
    );
}

#[test]
fn tz_warsaw() {
    let case = Case::new("tz_warsaw")
        .env("TZ", "Europe/Warsaw")
        .process_repos()
        .process_commits()
        .debug()
        .mode("1")
        .orphan(WIDE_RANGE);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 3 commits and 2 commit roles in: <dur>",
    );
    assert_eq!(rs.commit_shas().len(), 4);
}

// ------------------------------------------- backfill (FETCH_COMMITS_MODE)

/// `(sha, role, actor_id, actor_login, actor_name, actor_email)` of the
/// commit roles.
fn roles(rs: &Side) -> Vec<Vec<String>> {
    rs.query("select sha, role, actor_id::text, actor_login, actor_name, actor_email from gha_commits_roles order by sha, role")
}

#[test]
fn mode1_basic() {
    let Some(rs) = both(&Case::new("mode1_basic").mode("1").debug()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "FetchCommitsMode=1: processing DB '<db>' (1 repos, threads 1, batch 1000)",
    );
    rs.expect_line(
        0,
        "<db>/org/repo: need to backfill 2 events since 2012-07-01 00:00:00 +0000 UTC",
    );
    rs.expect_line(
        0,
        &format!(
            "<db>/org/repo PushEvent 1001: found 1 commits (before {}, head {})",
            rs.shas[0], rs.shas[1]
        ),
    );
    rs.expect_line(
        0,
        &format!(
            "<db>/org/repo PushEvent 1002: found 2 commits (before {}, head {})",
            rs.shas[1], rs.shas[3]
        ),
    );
    rs.expect_line(
        0,
        &format!(
            "Warning: <db>/org/repo PushEvent 1001 payload size=2, computed commits=1 (before {}, head {})",
            rs.shas[0], rs.shas[1]
        ),
    );
    rs.expect_line(
        0,
        "lookupActorNameEmailCachedTx: name=\"Dev\", email=\"dev@example.com\" -> id=0, login=\"\"",
    );
    rs.expect_line(
        0,
        "lookupActorNameEmailCachedTx: name=\"Carol Jones\", email=\"carol@example.com\" -> id=3, login=\"carol\"",
    );
    rs.expect_line(
        0,
        "<db>/org/repo: successfully backfilled 3 commits and 2 commit roles for 2 events",
    );
    rs.expect_line(
        0,
        "Finished DB '<db>': backfilled 3 commits and 2 commit roles for 1 repos",
    );
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 3 commits and 2 commit roles in: <dur>",
    );
    // Nothing else ran.
    rs.expect_no_prefix(0, "Restoring orphan");
    rs.expect_no_prefix(0, "Got ");
    assert_eq!(rs.head(), Some(rs.shas[2].clone()));

    let mut expected = vec![rs.shas[1].clone(), rs.shas[2].clone(), rs.shas[3].clone()];
    expected.sort();
    assert_eq!(rs.commit_shas(), expected);
    // Commit 2 belongs to PushEvent 1001, commits 3–4 to 1002.
    let mut expected_rows = vec![
        strs(&[
            &rs.shas[1],
            "1001",
            "1",
            "alice",
            "100",
            "org/repo",
            "PushEvent",
            "2020-01-02 01:00:00",
        ]),
        strs(&[
            &rs.shas[2],
            "1002",
            "3",
            "carol",
            "100",
            "org/repo",
            "PushEvent",
            "2020-01-04 01:00:00",
        ]),
        strs(&[
            &rs.shas[3],
            "1002",
            "3",
            "carol",
            "100",
            "org/repo",
            "PushEvent",
            "2020-01-04 01:00:00",
        ]),
    ];
    expected_rows.sort_by(|a, b| (&a[1], &a[0]).cmp(&(&b[1], &b[0])));
    assert_eq!(
        rs.query("select sha, event_id::text, dup_actor_id::text, dup_actor_login, dup_repo_id::text, dup_repo_name, dup_type, dup_created_at::text from gha_commits order by event_id, sha"),
        expected_rows
    );
    // Author / committer resolution: Dev is unknown, Carol is actor 3.
    assert_eq!(
        rs.query(&format!(
            "select author_name, author_email, committer_name, committer_email, coalesce(author_id, -1)::text, coalesce(committer_id, -1)::text, dup_author_login, dup_committer_login from gha_commits where sha = '{}'",
            rs.shas[3]
        )),
        vec![strs(&[
            "Carol Jones",
            "carol@example.com",
            "Dev",
            "dev@example.com",
            "3",
            "0",
            "carol",
            "",
        ])]
    );
    assert_eq!(
        rs.query(&format!(
            "select author_name, author_email, coalesce(author_id, -1)::text, dup_author_login from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        vec![strs(&["Dev", "dev@example.com", "0", ""])]
    );
    // Messages (git's `%B` keeps the trailing newline).
    let msg = rs.column(&format!(
        "select message from gha_commits where sha = '{}'",
        rs.shas[2]
    ));
    assert!(
        msg[0].starts_with("third commit ünïcode ♂♀ 'quote' \"dq\""),
        "{msg:?}"
    );
    // Trailer roles of commit 2.
    assert_eq!(
        roles(&rs),
        vec![
            strs(&[
                &rs.shas[1],
                "Reviewed-by",
                "2",
                "bob",
                "Bob",
                "bob@example.com"
            ]),
            strs(&[
                &rs.shas[1],
                "Signed-off-by",
                "1",
                "alice",
                "Alice Smith",
                "alice@example.com"
            ]),
        ]
    );
    assert_eq!(
        rs.query("select event_id::text, dup_repo_id::text, dup_repo_name, dup_created_at::text from gha_commits_roles order by role"),
        vec![
            strs(&["1001", "100", "org/repo", "2020-01-02 01:00:00"]),
            strs(&["1001", "100", "org/repo", "2020-01-02 01:00:00"]),
        ]
    );
    // No files / LOC processing happened.
    assert!(rs.files().is_empty());
    assert_eq!(
        rs.count("select count(*) from gha_commits where loc_added is not null"),
        0
    );
}

#[test]
fn mode1_quiet() {
    let Some(rs) = both(&Case::new("mode1_quiet").mode("1")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.lines(0),
        strs(&[
            BANNER,
            "FetchCommitsMode=1: processing DB '<db>' (1 repos, threads 1, batch 1000)",
            "<db>/org/repo: need to backfill 2 events since 2012-07-01 00:00:00 +0000 UTC",
            "<db>/org/repo: need to backfill 3 commits for 2 events",
            "<db>/org/repo: inserting commits for 2 events",
            &format!(
                "Warning: <db>/org/repo PushEvent 1001 payload size=2, computed commits=1 (before {}, head {})",
                rs.shas[0], rs.shas[1]
            ),
            "<db>/org/repo: successfully backfilled 3 commits and 2 commit roles for 2 events",
            "Finished DB '<db>': backfilled 3 commits and 2 commit roles for 1 repos",
            "Finished all DBs: backfilled 3 commits and 2 commit roles in: <dur>",
            "All repos processed in: <dur>",
        ])
    );
    assert_eq!(rs.commit_shas().len(), 3);
}

#[test]
fn mode1_default_env() {
    // No GHA2DB_FETCH_COMMITS_MODE: mode 1 is the default.
    let Some(rs) = both(&Case::new("mode1_default_env").no_env("GHA2DB_FETCH_COMMITS_MODE")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 3 commits and 2 commit roles in: <dur>",
    );
    assert_eq!(rs.commit_shas().len(), 3);
}

#[test]
fn mode1_mt() {
    let case = Case::new("mode1_mt")
        .mode("1")
        .debug()
        .no_env("GHA2DB_ST")
        .unordered();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 3 commits and 2 commit roles in: <dur>",
    );
    assert_eq!(rs.commit_shas().len(), 3);
}

#[test]
fn mode1_rerun() {
    // The second run starts from the newest backfilled commit: nothing to do.
    let case = Case::new("mode1_rerun")
        .mode("1")
        .debug()
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(1, "<db>/org/repo: no need to backfill commits since <ts>");
    rs.expect_line(
        1,
        "Finished DB '<db>': backfilled 0 commits and 0 commit roles for 1 repos",
    );
    assert_eq!(rs.commit_shas().len(), 3);
    assert_eq!(roles(&rs).len(), 2);
}

#[test]
fn mode1_new_event_after_rerun() {
    // A new PushEvent (commit 5, fetched into the clone) after the first run.
    let case = Case::new("mode1_new_event_after_rerun")
        .mode("1")
        .debug()
        .steps(vec![
            Step::Run(Vec::new()),
            Step::Shell("git -C {dir}/orig/org_repo fetch -q {dir}/up/org_repo main:main && git -C {dir}/repos/org/repo fetch -q origin"),
            Step::Sql("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values (1003, 'PushEvent', 1, 100, '2020-01-05 01:00:00', 10, 'alice', 'org/repo');
insert into gha_payloads(event_id, push_id, size, ref, head, befor, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values (1003, 3, 1, 'refs/heads/main', '{sha4}', '{sha3}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-05 01:00:00');"),
            Step::Run(Vec::new()),
        ]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(
        1,
        "<db>/org/repo: need to backfill 1 events since 2020-01-04 01:00:00 +0000 +0000",
    );
    rs.expect_line(
        1,
        "<db>/org/repo: successfully backfilled 1 commits and 0 commit roles for 1 events",
    );
    assert_eq!(rs.commit_shas().len(), 4);
    assert_eq!(
        rs.column(&format!(
            "select event_id::text from gha_commits where sha = '{}'",
            rs.shas[4]
        )),
        strs(&["1003"])
    );
}

#[test]
fn mode1_startdt() {
    // Only events since GHA2DB_STARTDT.
    let case = Case::new("mode1_startdt")
        .mode("1")
        .debug()
        .env("GHA2DB_STARTDT", "2020-01-03");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "<db>/org/repo: need to backfill 1 events since 2020-01-03 00:00:00 +0000 UTC",
    );
    let mut expected = vec![rs.shas[2].clone(), rs.shas[3].clone()];
    expected.sort();
    assert_eq!(rs.commit_shas(), expected);
}

#[test]
fn mode1_not_cloned() {
    let case = Case::new("mode1_not_cloned")
        .mode("1")
        .debug()
        .repo(Repo::Missing);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "backfillRepo(DB=<db>, repo=org/repo) error: <db>: repo not cloned: <dir>/repos/org/repo",
    );
    rs.expect_line(
        0,
        "Finished DB '<db>': backfilled 0 commits and 0 commit roles for 1 repos",
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn mode1_repo_path_is_file() {
    let case = Case::new("mode1_repo_path_is_file")
        .mode("1")
        .debug()
        .repo(Repo::Missing)
        .steps(vec![
            Step::Shell("mkdir -p {dir}/repos/org && touch {dir}/repos/org/repo"),
            Step::Run(Vec::new()),
        ])
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // The path exists, so the repo counts as cloned; the range script fails
    // quietly for every event.
    assert_eq!(
        rs.count_prefix(0, "Warning: no commits found for <db>/org/repo PushEvent "),
        2
    );
    rs.expect_line(
        0,
        "<db>/org/repo: no commits to backfill after processing 2 events",
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn mode1_hide() {
    // hide.csv anonymises the author's e-mail everywhere it is stored.
    let case = Case::new("mode1_hide").mode("1").hide(DEV_EMAIL_SHA1);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let anon = format!("anon-{DEV_EMAIL_SHA1}");
    assert_eq!(
        rs.query(&format!(
            "select author_email, committer_email from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        vec![vec![anon.clone(), anon.clone()]]
    );
    assert_eq!(
        rs.query(&format!(
            "select author_email, committer_email from gha_commits where sha = '{}'",
            rs.shas[3]
        )),
        vec![vec!["carol@example.com".to_string(), anon]]
    );
    // Trailer e-mails are not hidden (not in hide.csv).
    assert_eq!(roles(&rs).len(), 2);
}

#[test]
fn mode1_hide_name_and_login() {
    // SHA-1s of `Dev`, `Carol Jones` and the login `carol`.
    let case = Case::new("mode1_hide_name_and_login")
        .mode("1")
        .debug()
        .hide("f67bc6dad74f08d4d8b6187fc92476b5a2aa4a2b\n2f41bd975020b5a66ed1f1ffcdc855b0149f96bd\n28b92b56ee64b92ebb72d865f172ef00c708df83");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.query(&format!(
            "select author_name, committer_name, dup_actor_login, dup_author_login from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        vec![strs(&[
            "anon-f67bc6dad74f08d4d8b6187fc92476b5a2aa4a2b",
            "anon-f67bc6dad74f08d4d8b6187fc92476b5a2aa4a2b",
            "alice",
            "",
        ])]
    );
    // Carol is still resolved through her (unhidden) e-mail, but her name and
    // login are anonymised wherever they are stored.
    assert_eq!(
        rs.query(&format!(
            "select author_name, author_email, author_id::text, dup_actor_login, dup_author_login from gha_commits where sha = '{}'",
            rs.shas[3]
        )),
        vec![strs(&[
            "anon-2f41bd975020b5a66ed1f1ffcdc855b0149f96bd",
            "carol@example.com",
            "3",
            "anon-28b92b56ee64b92ebb72d865f172ef00c708df83",
            "anon-28b92b56ee64b92ebb72d865f172ef00c708df83",
        ])]
    );
}

#[test]
fn mode1_actor_by_login() {
    // Unknown e-mail / names, `gha_actors.name` differs — matched by login.
    let case = Case::new("mode1_actor_by_login")
        .mode("1")
        .debug()
        .also("insert into gha_actors(id, login, name) values (4, 'dev', 'Somebody Else');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "lookupActorNameEmailCachedTx: name=\"Dev\", email=\"dev@example.com\" -> id=4, login=\"dev\"",
    );
    assert_eq!(
        rs.query(&format!(
            "select author_id::text, committer_id::text, dup_author_login, dup_committer_login from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        vec![strs(&["4", "4", "dev", "dev"])]
    );
}

#[test]
fn mode1_actor_by_actor_name() {
    let case = Case::new("mode1_actor_by_actor_name")
        .mode("1")
        .debug()
        .also("insert into gha_actors(id, login, name) values (4, 'somebody', 'dev'), (5, 'dev', 'Other');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // `gha_actors.name` (case-insensitive) wins over the login.
    rs.expect_line(
        0,
        "lookupActorNameEmailCachedTx: name=\"Dev\", email=\"dev@example.com\" -> id=4, login=\"somebody\"",
    );
}

#[test]
fn mode1_actor_by_names_table() {
    let case = Case::new("mode1_actor_by_names_table")
        .mode("1")
        .debug()
        .also("insert into gha_actors(id, login, name) values (4, 'somebody', 'X'), (5, 'other', 'DEV'); insert into gha_actors_names(actor_id, name) values (4, 'DEV');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // `gha_actors_names` wins over `gha_actors.name`.
    rs.expect_line(
        0,
        "lookupActorNameEmailCachedTx: name=\"Dev\", email=\"dev@example.com\" -> id=4, login=\"somebody\"",
    );
}

#[test]
fn mode1_actor_by_email_highest_id() {
    // Two actors share the e-mail: the highest id wins.
    let case = Case::new("mode1_actor_by_email_highest_id")
        .mode("1")
        .debug()
        .also("insert into gha_actors(id, login, name) values (4, 'dev1', 'D1'), (7, 'dev7', 'D7'); insert into gha_actors_emails(actor_id, email) values (4, 'DEV@example.com'), (7, 'dev@EXAMPLE.com');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "lookupActorNameEmailCachedTx: name=\"Dev\", email=\"dev@example.com\" -> id=7, login=\"dev7\"",
    );
}

#[test]
fn mode1_weird_payloads() {
    // Extra PushEvents: zero before (strict skip), garbage head, head not in
    // the clone, before == head, non-fast-forward, NULL size, size 0 with a
    // zero before (not selected at all).
    let case = Case::new("mode1_weird_payloads")
        .mode("1")
        .debug()
        .also("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values
 (1003, 'PushEvent', 1, 100, '2020-01-04 02:00:00', 10, 'alice', 'org/repo'),
 (1004, 'PushEvent', 1, 100, '2020-01-04 03:00:00', 10, 'alice', 'org/repo'),
 (1005, 'PushEvent', 1, 100, '2020-01-04 04:00:00', 10, 'alice', 'org/repo'),
 (1006, 'PushEvent', 1, 100, '2020-01-04 05:00:00', 10, 'alice', 'org/repo'),
 (1007, 'PushEvent', 1, 100, '2020-01-04 06:00:00', 10, 'alice', 'org/repo'),
 (1008, 'PushEvent', 1, 100, '2020-01-04 07:00:00', 10, 'alice', 'org/repo'),
 (1009, 'PushEvent', 1, 100, '2020-01-04 08:00:00', 10, 'alice', 'org/repo'),
 (1010, 'IssuesEvent', 1, 100, '2020-01-04 09:00:00', 10, 'alice', 'org/repo');
insert into gha_payloads(event_id, push_id, size, ref, head, befor, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values
 (1003, 3, 1, 'refs/heads/main', '{sha2}', '{zero}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-04 02:00:00'),
 (1004, 4, 1, 'refs/heads/main', 'xyz', '{sha1}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-04 03:00:00'),
 (1005, 5, 1, 'refs/heads/main', 'ffffffffffffffffffffffffffffffffffffffff', '{sha1}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-04 04:00:00'),
 (1006, 6, 1, 'refs/heads/main', '{sha2}', '{sha2}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-04 05:00:00'),
 (1007, 7, 1, 'refs/heads/main', '{sha1}', '{sha3}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-04 06:00:00'),
 (1008, 8, null, 'refs/heads/main', '{sha2}', '{sha1}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-04 07:00:00'),
 (1009, 9, 0, 'refs/heads/main', '{sha3}', '{zero}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-04 08:00:00'),
 (1010, null, null, null, null, null, 'alice', 100, 'org/repo', 'IssuesEvent', '2020-01-04 09:00:00');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "<db>/org/repo: need to backfill 8 events since 2012-07-01 00:00:00 +0000 UTC",
    );
    rs.expect_line(
        0,
        &format!(
            "Warning: strict mode: skipping PushEvent 1003 in <db>/org/repo: invalid/empty/zero before SHA \"{ZERO_SHA}\""
        ),
    );
    rs.expect_line(
        0,
        "Warning: skipping PushEvent 1004 in <db>/org/repo: invalid/empty/zero head SHA \"xyz\"",
    );
    rs.expect_line(
        0,
        &format!(
            "Warning: no commits found for <db>/org/repo PushEvent 1005 (before {}, head ffffffffffffffffffffffffffffffffffffffff)",
            rs.shas[1]
        ),
    );
    rs.expect_no_line(
        0,
        &format!(
            "<db>/org/repo PushEvent 1006: found 0 commits (before {}, head {})",
            rs.shas[2], rs.shas[2]
        ),
    );
    // Non-fast-forward: the range is empty (ancestry is not enforced).
    rs.expect_line(
        0,
        &format!(
            "Warning: no commits found for <db>/org/repo PushEvent 1007 (before {}, head {})",
            rs.shas[3], rs.shas[1]
        ),
    );
    rs.expect_line(0, "<db>/org/repo: need to backfill 3 commits for 8 events");
    rs.expect_line(
        0,
        "<db>/org/repo: successfully backfilled 4 commits and 2 commit roles for 8 events",
    );
    rs.expect_no_line(0, "<db>/org/repo PushEvent 1009: found 0 commits");
    // Commit 3 is (also) attributed to event 1008 (NULL size).
    assert_eq!(
        rs.column(&format!(
            "select event_id::text from gha_commits where sha = '{}' order by event_id",
            rs.shas[2]
        )),
        strs(&["1002", "1008"])
    );
    assert_eq!(rs.count("select count(*) from gha_commits"), 4);
}

#[test]
fn mode2_truncated() {
    // Mode 2 re-processes events whose commit count is below the payload
    // size (1001 has size 2 but a single commit).
    let case = Case::new("mode2_truncated").debug().steps(vec![
        Step::Run(vec![("GHA2DB_FETCH_COMMITS_MODE", "1")]),
        Step::Run(vec![("GHA2DB_FETCH_COMMITS_MODE", "2")]),
        Step::Run(vec![("GHA2DB_FETCH_COMMITS_MODE", "1")]),
    ]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(
        1,
        "FetchCommitsMode=2: processing DB '<db>' (1 repos, threads 1, batch 1000)",
    );
    rs.expect_prefix(1, "<db>/org/repo: need to backfill 1 events since ");
    rs.expect_prefix(2, "<db>/org/repo: no need to backfill commits since ");
    assert_eq!(rs.count("select count(*) from gha_commits"), 3);
    assert_eq!(roles(&rs).len(), 2);
}

#[test]
fn mode2_fresh() {
    let Some(rs) = both(&Case::new("mode2_fresh").mode("2")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 3 commits and 2 commit roles in: <dur>",
    );
}

#[test]
fn mode1_range_128_retry() {
    // The range script fails once with 128: PR refs are fetched and the
    // range retried.
    let case = Case::new("mode1_range_128_retry")
        .mode("1")
        .debug()
        .script(
            "git_commits_range.sh",
            Script::Wrap("marker=\"$(dirname \"$0\")/.range_failed_once\"\nif [ ! -f \"$marker\" ]; then touch \"$marker\"; echo 'fatal: bad object' >&2; exit 128; fi\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        &format!(
            "Warning: git range failed for <db>/org/repo event 1001 ({}..{}): exit status 128, trying to fetch GitHub PR refs and retry",
            rs.shas[0], rs.shas[1]
        ),
    );
    rs.expect_line(
        0,
        "<db>/org/repo: successfully backfilled 3 commits and 2 commit roles for 2 events",
    );
}

#[test]
fn mode1_range_fails() {
    // The range script always fails: one retry (after fetching PR refs),
    // then every event is reported and skipped.
    let case = Case::new("mode1_range_fails").mode("1").debug().script(
        "git_commits_range.sh",
        Script::Wrap("echo 'fatal: bad object' >&2; exit 128"),
    );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        &format!(
            "Error listing commits range for <db>/org/repo after fetching PR refs (strict mode, event 1001, before {}, head {}): exit status 128",
            rs.shas[0], rs.shas[1]
        ),
    );
    rs.expect_line(
        0,
        &format!(
            "Error listing commits range for <db>/org/repo (strict mode, event 1002, before {}, head {}): exit status 128",
            rs.shas[1], rs.shas[3]
        ),
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn mode1_range_other_error() {
    // A non-128 failure is not retried.
    let case = Case::new("mode1_range_other_error")
        .mode("1")
        .debug()
        .script("git_commits_range.sh", Script::Wrap("exit 3"));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_no_prefix(0, "Warning: git range failed");
    assert_eq!(
        rs.count_prefix(
            0,
            "Error listing commits range for <db>/org/repo (strict mode"
        ),
        2
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn mode1_commits_bisect() {
    // git_commits.sh fails whenever commit 3 is among its arguments: the
    // batch is bisected and only that commit's metadata is missing.
    let case = Case::new("mode1_commits_bisect")
        .mode("1")
        .debug()
        .env("BAD_SHA", "{sha2}")
        .script(
            "git_commits.sh",
            Script::Wrap("for a in \"$@\"; do if [ \"$a\" = \"$BAD_SHA\" ]; then echo \"fatal: bad object $a\" >&2; exit 128; fi; done\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_prefix(0, "Warning: git_commits.sh error for <db>/org/repo batch ");
    rs.expect_line(
        0,
        &format!(
            "Warning: missing git metadata for <db>/org/repo sha {} (event 1002)",
            rs.shas[2]
        ),
    );
    rs.expect_line(
        0,
        "<db>/org/repo: successfully backfilled 2 commits and 2 commit roles for 2 events",
    );
    let mut expected = vec![rs.shas[1].clone(), rs.shas[3].clone()];
    expected.sort();
    assert_eq!(rs.commit_shas(), expected);
}

#[test]
fn mode1_commits_all_fail() {
    let case = Case::new("mode1_commits_all_fail")
        .mode("1")
        .debug()
        .script("git_commits.sh", Script::Wrap("echo 'boom' >&2; exit 1"));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Warning: git_commits.sh error for <db>/org/repo batch 0-3/3: git_commits.sh error for both halves: (exit status 1) and (git_commits.sh error for both halves: (exit status 1) and (exit status 1))",
    );
    rs.expect_line(
        0,
        "backfillRepo(DB=<db>, repo=org/repo) error: git_commits.sh returned no commit metadata for db=<db>, repo=org/repo (shas=3)",
    );
    assert_eq!(
        rs.count_prefix(
            0,
            "Error running git_commits.sh for repo <dir>/repos/org/repo, batch size "
        ),
        5
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn mode1_bad_base64() {
    // Corrupt metadata for the first SHA of every call.
    let case = Case::new("mode1_bad_base64").mode("1").debug().script(
        "git_commits.sh",
        Script::Fake("printf '%s,!!!!,YQ==,YQ==,YQ==,YQ==;\\n' \"$2\""),
    );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_contains(0, "illegal base64 data at input byte 0");
    assert!(rs.commit_shas().len() < 3);
}

#[test]
fn mode1_no_metadata() {
    let case = Case::new("mode1_no_metadata")
        .mode("1")
        .debug()
        .script("git_commits.sh", Script::Fake("exit 0"));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_contains(0, "returned no commit metadata for db=<db>, repo=org/repo");
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn mode1_partial_metadata() {
    // Metadata for the first SHA only: the others are reported missing.
    let case = Case::new("mode1_partial_metadata")
        .mode("1")
        .debug()
        .script(
            "git_commits.sh",
            Script::Wrap("exec \"$REAL\" \"$1\" \"$2\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert!(rs.count_prefix(0, "Warning: missing git metadata for <db>/org/repo sha ") >= 1);
    assert!(!rs.commit_shas().is_empty());
}

#[test]
fn mode1_batch1() {
    let case = Case::new("mode1_batch1")
        .mode("1")
        .debug()
        .env("GHA2DB_GIT_COMMITS_BATCH", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "FetchCommitsMode=1: processing DB '<db>' (1 repos, threads 1, batch 1)",
    );
    rs.expect_line(
        0,
        "<db>/org/repo: successfully backfilled 3 commits and 2 commit roles for 2 events",
    );
}

#[test]
fn mode1_batch2_bisect() {
    let case = Case::new("mode1_batch2_bisect")
        .mode("1")
        .debug()
        .env("GHA2DB_GIT_COMMITS_BATCH", "2")
        .env("BAD_SHA", "{sha3}")
        .script(
            "git_commits.sh",
            Script::Wrap("for a in \"$@\"; do if [ \"$a\" = \"$BAD_SHA\" ]; then exit 128; fi; done\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "<db>/org/repo: successfully backfilled 2 commits and 2 commit roles for 2 events",
    );
}

#[test]
fn mode1_two_repos_one_db() {
    // Two repositories in one database (processed in map order by Go).
    let case = Case::new("mode1_two_repos_one_db")
        .mode("1")
        .debug()
        .unordered()
        .extra(&["org/zeta"])
        .also("insert into gha_repos(id, name, org_id, org_login) values (101, 'org/zeta', 10, 'org');
insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values (2001, 'PushEvent', 2, 101, '2020-01-04 01:00:00', 10, 'bob', 'org/zeta');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "FetchCommitsMode=1: processing DB '<db>' (2 repos, threads 1, batch 1000)",
    );
    rs.expect_prefix(0, "<db>/org/zeta: no need to backfill commits since ");
    rs.expect_line(
        0,
        "Finished DB '<db>': backfilled 3 commits and 2 commit roles for 2 repos",
    );
}

// ----------------------------------------------------------- orphan restore

/// `(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name)`
/// of the artificial (negative id) events.
fn restored_events(rs: &Side) -> Vec<Vec<String>> {
    rs.query("select id::text, type, actor_id::text, repo_id::text, created_at::text, dup_actor_login, dup_repo_name from gha_events where id < 0 order by id")
}

#[test]
fn orphan_wide_range() {
    // No backfill ran: all 4 commits reachable from origin/main (the clone's
    // HEAD reset does not matter) are orphans.
    let case = Case::new("orphan_wide_range").debug().orphan(WIDE_RANGE);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Restoring orphan commits: processing DB '<db>' (1 repos, threads 1)",
    );
    rs.expect_line(0, "<db>/org/repo: found 4 commits since <ts>");
    rs.expect_line(0, "<db>/org/repo: need to restore 4 orphan commits");
    rs.expect_line(
        0,
        "Fetched commit metadata for <db>/org/repo: 4 SHAs, 4 records",
    );
    rs.expect_line(0, "<db>/org/repo: successfully restored 4 orphan commits");
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 1 repos, checked 4 commits, restored 4",
    );
    rs.expect_line(
        0,
        "Finished orphan commit restore: processed 1 repos, checked 4 commits, restored 4 in: <dur>",
    );
    rs.expect_line(
        0,
        "targeted postprocess skipped: gha_texts is empty, full structure rebuild pending",
    );
    rs.expect_no_prefix(0, "FetchCommitsMode=");
    let mut expected: Vec<String> = rs.shas[..4].to_vec();
    expected.sort();
    assert_eq!(rs.commit_shas(), expected);

    // Artificial events / payloads / commits (Carol is a known actor).
    let nids: Vec<String> = (0..4)
        .map(|i| negative_artificial_id(&["PushEvent", "org/repo", &rs.shas[i]]).to_string())
        .collect();
    let mut events = vec![
        strs(&[
            &nids[0],
            "PushEvent",
            "0",
            "100",
            "2020-01-01 00:00:00",
            "Dev",
            "org/repo",
        ]),
        strs(&[
            &nids[1],
            "PushEvent",
            "0",
            "100",
            "2020-01-02 00:00:00",
            "Dev",
            "org/repo",
        ]),
        strs(&[
            &nids[2],
            "PushEvent",
            "0",
            "100",
            "2020-01-03 00:00:00",
            "Dev",
            "org/repo",
        ]),
        strs(&[
            &nids[3],
            "PushEvent",
            "3",
            "100",
            "2020-01-04 00:00:00",
            "carol",
            "org/repo",
        ]),
    ];
    // `order by id` sorts the `id::text` output column, i.e. as text.
    events.sort();
    assert_eq!(restored_events(&rs), events);
    assert_eq!(
        rs.query(&format!(
            "select size::text, ref, head, befor, action, dup_actor_login, dup_repo_id::text, dup_repo_name, dup_type, dup_created_at::text from gha_payloads where event_id = {}",
            nids[1]
        )),
        vec![strs(&[
            "1",
            "refs/remotes/origin/main",
            &rs.shas[1],
            "",
            "restored_orphan_commit",
            "Dev",
            "100",
            "org/repo",
            "PushEvent",
            "2020-01-02 00:00:00",
        ])]
    );
    assert_eq!(
        rs.query(&format!(
            "select event_id::text, author_name, author_email, committer_name, committer_email, author_id::text, committer_id::text, dup_author_login, dup_committer_login, dup_actor_id::text, dup_actor_login, dup_repo_id::text, dup_repo_name, dup_type, dup_created_at::text, origin::text, is_distinct::text from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        vec![strs(&[
            &nids[1],
            "Dev",
            "dev@example.com",
            "Dev",
            "dev@example.com",
            "0",
            "0",
            "Dev",
            "",
            "0",
            "Dev",
            "100",
            "org/repo",
            "PushEvent",
            "2020-01-02 00:00:00",
            "2",
            "true",
        ])]
    );
    // Trailer roles of commit 2 are restored too.
    assert_eq!(
        roles(&rs),
        vec![
            strs(&[
                &rs.shas[1],
                "Reviewed-by",
                "2",
                "bob",
                "Bob",
                "bob@example.com"
            ]),
            strs(&[
                &rs.shas[1],
                "Signed-off-by",
                "1",
                "alice",
                "Alice Smith",
                "alice@example.com"
            ]),
        ]
    );
    assert_eq!(
        rs.column("select event_id::text from gha_commits_roles order by role"),
        vec![nids[1].clone(), nids[1].clone()]
    );
    // No files / LOC processing happened.
    assert!(rs.files().is_empty());
}

#[test]
fn orphan_quiet() {
    let Some(rs) = both(&Case::new("orphan_quiet").orphan(WIDE_RANGE)) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.lines(0),
        strs(&[
            BANNER,
            "Restoring orphan commits: processing DB '<db>' (1 repos, threads 1)",
            "Finished DB '<db>': processed 1 repos, checked 4 commits, restored 4",
            "targeted postprocess skipped: gha_texts is empty, full structure rebuild pending",
            "Finished orphan commit restore: processed 1 repos, checked 4 commits, restored 4 in: <dur>",
            "All repos processed in: <dur>",
        ])
    );
}

#[test]
fn orphan_after_backfill() {
    // Backfill first (commits 2–4), then only commit 1 is an orphan.
    let case = Case::new("orphan_after_backfill")
        .debug()
        .mode("1")
        .orphan(WIDE_RANGE);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "<db>/org/repo: need to restore 1 orphan commits");
    rs.expect_line(0, "<db>/org/repo: successfully restored 1 orphan commits");
    assert_eq!(rs.commit_shas().len(), 4);
    assert_eq!(
        rs.column(&format!(
            "select origin::text from gha_commits where sha = '{}'",
            rs.shas[0]
        )),
        strs(&["2"])
    );
    assert_eq!(
        rs.column(&format!(
            "select origin::text from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        strs(&["1"])
    );
}

#[test]
fn orphan_texts_present() {
    // gha_texts is not empty: the targeted postprocess runs for the
    // restored event ids.
    let case = Case::new("orphan_texts_present")
        .debug()
        .orphan(WIDE_RANGE)
        .also("insert into gha_texts(event_id, body, created_at, actor_id, actor_login, repo_id, repo_name, type) values (1001, 'x', '2020-01-02 01:00:00', 1, 'alice', 100, 'org/repo', 'PushEvent');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "targeted postprocess executed for 4 restored event id(s)",
    );
    // The commit messages are now in gha_texts.
    assert_eq!(rs.count("select count(*) from gha_texts"), 5);
    assert_eq!(
        rs.count("select count(*) from gha_texts where event_id < 0 and type = 'PushEvent' and repo_name = 'org/repo' and repo_id = 100"),
        4
    );
    assert_eq!(
        rs.column("select actor_id::text || '/' || actor_login from gha_texts where event_id < 0 group by 1 order by 1"),
        strs(&["0/Dev", "3/carol"])
    );
}

#[test]
fn orphan_rerun() {
    let case = Case::new("orphan_rerun")
        .debug()
        .orphan(WIDE_RANGE)
        .steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(1, "<db>/org/repo: no orphan commits to restore");
    rs.expect_line(
        1,
        "Finished DB '<db>': processed 1 repos, checked 4 commits, restored 0",
    );
    rs.expect_no_prefix(1, "targeted postprocess");
    assert_eq!(rs.commit_shas().len(), 4);
}

#[test]
fn orphan_default_range() {
    // Default range: 8 hours — nothing from 2020 qualifies.
    let case = Case::new("orphan_default_range")
        .debug()
        .env("GHA2DB_RESTORE_ORPHAN_COMMITS", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "<db>/org/repo: no commits found since <ts>");
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 0 repos, checked 0 commits, restored 0",
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn orphan_range_days() {
    // A range that covers commit 5 only if it were in the clone: it is not,
    // and the 2020 commits are too old for "1 day".
    let case = Case::new("orphan_range_days").debug().orphan("1 day");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "<db>/org/repo: no commits found since <ts>");
}

#[test]
fn orphan_invalid_range() {
    let case = Case::new("orphan_invalid_range")
        .debug()
        .orphan("notaninterval")
        .code_only_errors()
        .no_data()
        .no_head();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
}

#[test]
fn orphan_not_cloned() {
    let case = Case::new("orphan_not_cloned")
        .debug()
        .orphan(WIDE_RANGE)
        .repo(Repo::Missing);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "restoreOrphanRepo(DB=<db>, repo=org/repo) error: <db>: repo not cloned: <dir>/repos/org/repo",
    );
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 0 repos, checked 0 commits, restored 0",
    );
}

#[test]
fn orphan_default_ref_fallback() {
    // No `origin` remote: HEAD is scanned instead.
    let case = Case::new("orphan_default_ref_fallback")
        .debug()
        .orphan(WIDE_RANGE)
        .remove_origin();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_prefix(
        0,
        "Warning: could not determine default ref for <db>/org/repo: ",
    );
    rs.expect_line(0, "<db>/org/repo: successfully restored 3 orphan commits");
    assert_eq!(
        rs.column("select distinct ref from gha_payloads where action = 'restored_orphan_commit'"),
        strs(&["HEAD"])
    );
}

#[test]
fn orphan_skip_commits() {
    // Commit 1 is in gha_skip_commits: not restored.
    let case = Case::new("orphan_skip_commits")
        .debug()
        .orphan(WIDE_RANGE)
        .also("insert into gha_skip_commits(sha, dt, reason) values ('{sha0}', '2021-01-01', 1);");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "<db>/org/repo: found 4 commits since <ts>");
    rs.expect_line(0, "<db>/org/repo: need to restore 3 orphan commits");
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 1 repos, checked 4 commits, restored 3",
    );
    let mut expected = vec![rs.shas[1].clone(), rs.shas[2].clone(), rs.shas[3].clone()];
    expected.sort();
    assert_eq!(rs.commit_shas(), expected);
}

#[test]
fn orphan_conflict() {
    // The artificial event id of commit 1 is taken by a different event.
    let case = Case::new("orphan_conflict")
        .debug()
        .orphan(WIDE_RANGE)
        .also("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values ({nid0}, 'IssuesEvent', 1, 100, '2019-01-01 12:00:00', 10, 'alice', 'org/repo');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        &format!(
            "orphan event id {} conflict: existing (IssuesEvent, org/repo, 2019-01-01 12:00:00 +0000 +0000), skipping",
            negative_artificial_id(&["PushEvent", "org/repo", &rs.shas[0]])
        ),
    );
    rs.expect_line(0, "<db>/org/repo: successfully restored 3 orphan commits");
    assert_eq!(rs.commit_shas().len(), 3);
}

#[test]
fn orphan_same_id_same_event() {
    // The artificial event already exists and matches: no conflict, and the
    // inserts are no-ops (`on conflict do nothing`) except the commit itself.
    let case = Case::new("orphan_same_id_same_event")
        .debug()
        .orphan(WIDE_RANGE)
        .also("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values ({nid0}, 'PushEvent', 1, 100, '2020-01-01 00:00:00', 10, 'alice', 'org/repo');
insert into gha_payloads(event_id, size, ref, head, befor, action, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values ({nid0}, 1, 'refs/heads/main', '{sha0}', '', 'x', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-01 00:00:00');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_no_prefix(0, "orphan event id");
    rs.expect_line(0, "<db>/org/repo: successfully restored 4 orphan commits");
    assert_eq!(rs.commit_shas().len(), 4);
    assert_eq!(
        rs.column(&format!(
            "select action from gha_payloads where event_id = {}",
            negative_artificial_id(&["PushEvent", "org/repo", &rs.shas[0]])
        )),
        strs(&["x"])
    );
}

#[test]
fn orphan_no_events_for_repo() {
    // gha_events has no rows for the repo: repo id unknown, nothing restored.
    let case = Case::new("orphan_no_events_for_repo")
        .debug()
        .orphan(WIDE_RANGE)
        .seed(vec![SEED_REPO]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "<db>/org/repo: no gha_events rows for this repo, skipping orphan commits restore",
    );
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 1 repos, checked 4 commits, restored 0",
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn orphan_hide() {
    let case = Case::new("orphan_hide")
        .orphan(WIDE_RANGE)
        .hide(DEV_EMAIL_SHA1);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let anon = format!("anon-{DEV_EMAIL_SHA1}");
    assert_eq!(
        rs.query(&format!(
            "select author_email, committer_email, dup_author_login from gha_commits where sha = '{}'",
            rs.shas[0]
        )),
        vec![vec![anon.clone(), anon, "Dev".to_string()]]
    );
}

#[test]
fn orphan_actor_resolved() {
    // Dev is a known actor: ids / logins are stored instead of the name.
    let case = Case::new("orphan_actor_resolved")
        .debug()
        .orphan(WIDE_RANGE)
        .also("insert into gha_actors(id, login, name) values (9, 'thedev', 'Dev');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.query(&format!(
            "select author_id::text, committer_id::text, dup_author_login, dup_committer_login, dup_actor_id::text, dup_actor_login from gha_commits where sha = '{}'",
            rs.shas[0]
        )),
        vec![strs(&["9", "9", "thedev", "thedev", "9", "thedev"])]
    );
    assert_eq!(
        rs.column("select actor_id::text || '/' || dup_actor_login from gha_events where id < 0 group by 1 order by 1"),
        strs(&["3/carol", "9/thedev"])
    );
}

#[test]
fn orphan_metadata_fails() {
    let case = Case::new("orphan_metadata_fails")
        .debug()
        .orphan(WIDE_RANGE)
        .script("git_commits.sh", Script::Fake("exit 0"));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "restoreOrphanRepo(DB=<db>, repo=org/repo) error: git_commits.sh returned no commit metadata for db=<db>, repo=org/repo (shas=4)",
    );
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 1 repos, checked 4 commits, restored 0",
    );
}

#[test]
fn orphan_metadata_partial() {
    // Metadata for one SHA only.
    let case = Case::new("orphan_metadata_partial")
        .debug()
        .orphan(WIDE_RANGE)
        .script(
            "git_commits.sh",
            Script::Wrap("exec \"$REAL\" \"$1\" \"$2\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.count_prefix(0, "Warning: missing git metadata for <db>/org/repo sha "),
        3
    );
    rs.expect_line(0, "<db>/org/repo: successfully restored 1 orphan commits");
}

#[test]
fn orphan_batch1() {
    let case = Case::new("orphan_batch1")
        .debug()
        .orphan(WIDE_RANGE)
        .env("GHA2DB_GIT_COMMITS_BATCH", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "<db>/org/repo: successfully restored 4 orphan commits");
}

#[test]
fn orphan_two_repos_same_commits() {
    // A renamed repo cloned twice from the same upstream: the commits are
    // restored once (DB-wide existence check). Which name wins depends on
    // the (random, in Go) processing order, so only the totals are checked.
    let case = Case::new("orphan_two_repos_same_commits")
        .debug()
        .orphan(WIDE_RANGE)
        .code_only_ok()
        .no_data()
        .steps(vec![
            Step::Shell("git clone -q {dir}/orig/org_repo {dir}/repos/org/renamed"),
            Step::Run(Vec::new()),
        ])
        .also("insert into gha_repos(id, name, org_id, org_login) values (100, 'org/renamed', 10, 'org');
insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values (3001, 'PushEvent', 1, 100, '2020-01-02 01:00:00', 10, 'alice', 'org/renamed');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Restoring orphan commits: processing DB '<db>' (2 repos, threads 1)",
    );
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 2 repos, checked 8 commits, restored 4",
    );
    assert_eq!(rs.count("select count(*) from gha_commits"), 4);
    assert_eq!(rs.count("select count(distinct sha) from gha_commits"), 4);
    assert_eq!(
        rs.count("select count(distinct dup_repo_name) from gha_commits"),
        1
    );
}

#[test]
fn orphan_mt() {
    let case = Case::new("orphan_mt")
        .debug()
        .orphan(WIDE_RANGE)
        .no_env("GHA2DB_ST")
        .unordered();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_prefix(
        0,
        "Restoring orphan commits: processing DB '<db>' (1 repos, threads ",
    );
    rs.expect_line(0, "<db>/org/repo: successfully restored 4 orphan commits");
}

#[test]
fn orphan_two_dbs() {
    let case = Case::new("orphan_two_dbs")
        .yaml(TWO_YAML)
        .db2(vec![SEED_REPO])
        .debug()
        .orphan(WIDE_RANGE)
        .unordered();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Finished DB '<db>': processed 1 repos, checked 4 commits, restored 4",
    );
    rs.expect_line(
        0,
        "<db2>/org/repo: no gha_events rows for this repo, skipping orphan commits restore",
    );
    rs.expect_line(
        0,
        "Finished orphan commit restore: processed 2 repos, checked 8 commits, restored 4 in: <dur>",
    );
    assert_eq!(rs.commit_shas().len(), 4);
    assert_eq!(rs.count("select count(*) from gha_commits"), 4);
    assert!(rs.query2("select sha from gha_commits").is_empty());
}

// ---------------------------------------------------------- processCommits

/// The `process_commits` default: PushEvent payloads reference commits 1, 2
/// and 4 (the clone has all their objects; its HEAD does not matter). Commit
/// 1 is the root commit: `git diff-tree` lists no files for it.
fn commits_case(name: &'static str) -> Case {
    Case::new(name).process_commits().debug()
}

#[test]
fn commits_only() {
    let Some(rs) = both(&commits_case("commits_only")) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Running on database: <db>");
    rs.expect_line(0, "Database '<db>' processed took <dur>, new commits: 3");
    rs.expect_line(0, "Got 1 DBs new commits list: took <dur>");
    rs.expect_line(
        0,
        "Got 2 (66.67%) new commit's files, 1 without files, 0 failed, all 3, took <dur>",
    );
    rs.expect_line(0, "Postprocessed all new commits, took <dur>");
    rs.expect_line(0, "BOC stats running on database: <db>");
    rs.expect_line(
        0,
        "BOC stats database '<db>' processed took <dur>, new commits: 3",
    );
    rs.expect_line(0, "Got 1 DBs new commits BOC stats: took <dur>");
    // Nothing is in gha_commits: no LOC row is ever updated.
    for i in [0usize, 1, 3] {
        rs.expect_line(0, &format!("No rows updated for SHA {}", rs.shas[i]));
    }
    rs.expect_line(
        0,
        "Got 0 (0.00%) new commit's BOC stats, 3 without stats, 0 failed, all 3, took <dur>",
    );
    rs.expect_no_prefix(0, "Warning git_");
    assert!(rs.stderr_lines(0).is_empty());
    // Files of commits 2 and 4 (b.md skipped by the files_skip_pattern).
    let mut expected = vec![
        strs(&[
            &rs.shas[1],
            "src/main.go",
            "12",
            "2020-01-02 00:00:00",
            "go",
        ]),
        strs(&[&rs.shas[3], "c.txt", "2", "2020-01-04 00:00:00", "txt"]),
    ];
    expected.sort();
    assert_eq!(rs.files(), expected);
    // Commit 1 has no files (1); no commit has LOC stats (2).
    let mut skipped = vec![
        (rs.shas[0].clone(), "1".to_string()),
        (rs.shas[0].clone(), "2".to_string()),
        (rs.shas[1].clone(), "2".to_string()),
        (rs.shas[3].clone(), "2".to_string()),
    ];
    skipped.sort();
    assert_eq!(rs.skipped(), skipped);
    assert_eq!(
        rs.count("select count(*) from gha_skip_commits where dt > now() - interval '1 day'"),
        4
    );
    // gha_events_commits_files links the files to the PushEvents (by head).
    let mut ecf = vec![
        strs(&[
            &rs.shas[1],
            "1001",
            "org/repo/src/main.go",
            "go",
            "12",
            "100",
            "org/repo",
        ]),
        strs(&[
            &rs.shas[3],
            "1002",
            "org/repo/c.txt",
            "txt",
            "2",
            "100",
            "org/repo",
        ]),
    ];
    ecf.sort();
    assert_eq!(
        rs.query("select sha, event_id::text, path, ext, size::text, dup_repo_id::text, dup_repo_name from gha_events_commits_files order by sha, event_id, path"),
        ecf
    );
    assert!(rs.commit_shas().is_empty());
}

#[test]
fn commits_quiet() {
    let Some(rs) = both(&Case::new("commits_quiet").process_commits()) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.lines(0),
        strs(&[
            BANNER,
            "Running on database: <db>",
            "Database '<db>' processed took <dur>, new commits: 3",
            "Got 1 DBs new commits list: took <dur>",
            "Got 2 (66.67%) new commit's files, 1 without files, 0 failed, all 3, took <dur>",
            "Postprocessed all new commits, took <dur>",
            "BOC stats running on database: <db>",
            "BOC stats database '<db>' processed took <dur>, new commits: 3",
            "Got 1 DBs new commits BOC stats: took <dur>",
            "Got 0 (0.00%) new commit's BOC stats, 3 without stats, 0 failed, all 3, took <dur>",
            "All repos processed in: <dur>",
        ])
    );
    // No debug: nothing on stderr.
    assert!(rs.stderr_lines(0).is_empty());
}

#[test]
fn commits_after_backfill() {
    // With the commits in gha_commits, LOC stats are stored.
    let case = Case::new("commits_after_backfill")
        .mode("1")
        .process_commits()
        .debug()
        .repo(Repo::Cloned(3));
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Database '<db>' processed took <dur>, new commits: 4");
    rs.expect_line(
        0,
        "Got 3 (75.00%) new commit's files, 1 without files, 0 failed, all 4, took <dur>",
    );
    // Commit 1 (the payload's `befor`) is a LOC candidate too, but it is not
    // in gha_commits.
    rs.expect_line(
        0,
        "BOC stats database '<db>' processed took <dur>, new commits: 4",
    );
    rs.expect_line(0, &format!("No rows updated for SHA {}", rs.shas[0]));
    rs.expect_line(
        0,
        "Got 3 (75.00%) new commit's BOC stats, 1 without stats, 0 failed, all 4, took <dur>",
    );
    assert!(rs.stderr_lines(0).is_empty());
    let mut expected = vec![
        strs(&[
            &rs.shas[1],
            "src/main.go",
            "12",
            "2020-01-02 00:00:00",
            "go",
        ]),
        strs(&[&rs.shas[2], "a.txt", "-1", "2020-01-03 00:00:00", "txt"]),
        strs(&[
            &rs.shas[2],
            "src/main.go",
            "26",
            "2020-01-03 00:00:00",
            "go",
        ]),
        strs(&[&rs.shas[3], "c.txt", "2", "2020-01-04 00:00:00", "txt"]),
    ];
    expected.sort();
    assert_eq!(rs.files(), expected);
    let mut loc = vec![
        strs(&[&rs.shas[1], "3", "0", "2"]),
        strs(&[&rs.shas[2], "2", "2", "2"]),
        strs(&[&rs.shas[3], "1", "0", "1"]),
    ];
    loc.sort();
    assert_eq!(rs.loc(), loc);
    assert_eq!(
        rs.skipped(),
        vec![
            (rs.shas[0].clone(), "1".to_string()),
            (rs.shas[0].clone(), "2".to_string())
        ]
    );
    assert_eq!(rs.count("select count(*) from gha_events_commits_files"), 4);
}

#[test]
fn commits_skip_files() {
    let case = commits_case("commits_skip_files").env("GHA2DB_SKIP_COMMITS_FILES", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_no_prefix(0, "Running on database");
    rs.expect_no_prefix(0, "Got 1 DBs new commits list");
    rs.expect_line(0, "BOC stats running on database: <db>");
    assert!(rs.files().is_empty());
    assert_eq!(rs.skipped().len(), 3);
}

#[test]
fn commits_skip_loc() {
    let case = commits_case("commits_skip_loc").env("GHA2DB_SKIP_COMMITS_LOC", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Postprocessed all new commits, took <dur>");
    rs.expect_no_prefix(0, "BOC stats");
    assert_eq!(rs.files().len(), 2);
    assert_eq!(rs.skipped(), vec![(rs.shas[0].clone(), "1".to_string())]);
}

#[test]
fn commits_skip_both() {
    let case = Case::new("commits_skip_both")
        .process_commits()
        .env("GHA2DB_SKIP_COMMITS_FILES", "1")
        .env("GHA2DB_SKIP_COMMITS_LOC", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.lines(0),
        strs(&[BANNER, "All repos processed in: <dur>"])
    );
}

#[test]
fn commits_nothing_to_do() {
    let case = Case::new("commits_nothing_to_do")
        .process_commits()
        .debug()
        .seed(vec![SEED_REPO, SEED_ACTORS]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Database '<db>' processed took <dur>, new commits: 0");
    rs.expect_line(
        0,
        "Got 0 (0.00%) new commit's files, 0 without files, 0 failed, all 0, took <dur>",
    );
    rs.expect_line(
        0,
        "Got 0 (0.00%) new commit's BOC stats, 0 without stats, 0 failed, all 0, took <dur>",
    );
}

#[test]
fn commits_rerun() {
    // Skipped / processed commits are not processed again.
    let case =
        commits_case("commits_rerun").steps(vec![Step::Run(Vec::new()), Step::Run(Vec::new())]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(1, "Database '<db>' processed took <dur>, new commits: 0");
    rs.expect_line(
        1,
        "BOC stats database '<db>' processed took <dur>, new commits: 0",
    );
}

#[test]
fn commits_debug2() {
    let case = commits_case("commits_debug2").env("GHA2DB_DEBUG", "2");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    for i in [0usize, 1, 3] {
        rs.expect_line(
            0,
            &format!("Getting files for commit org/repo:{}", rs.shas[i]),
        );
        rs.expect_line(
            0,
            &format!("Getting BOC stats for commit org/repo:{}", rs.shas[i]),
        );
    }
    rs.expect_line(
        0,
        &format!("Got org/repo:{} commit: 1 files: took <dur>", rs.shas[1]),
    );
    rs.expect_line(
        0,
        &format!("Got org/repo:{} commit: 1 files: took <dur>", rs.shas[3]),
    );
    // The root commit has no files: no "Got" line.
    rs.expect_no_prefix(0, &format!("Got org/repo:{} commit", rs.shas[0]));
    assert!(rs.stderr_lines(0).is_empty());
}

#[test]
fn commits_debug2_failures() {
    // debug 2: git_files.sh failures are reported too (stdout + stderr).
    let case = Case::new("commits_debug2_failures")
        .process_commits()
        .env("GHA2DB_DEBUG", "2")
        .also("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values (1003, 'PushEvent', 1, 100, '2020-01-05 01:00:00', 10, 'alice', 'org/repo');
insert into gha_payloads(event_id, push_id, size, ref, head, befor, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values (1003, 3, 1, 'refs/heads/main', 'ffffffffffffffffffffffffffffffffffffffff', '{sha3}', 'alice', 100, 'org/repo', 'PushEvent', '2020-01-05 01:00:00');");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let ff = "ffffffffffffffffffffffffffffffffffffffff";
    rs.expect_line(
        0,
        &format!("Warning git_files.sh failed: org/repo:{ff} (took <dur>): exit status 4"),
    );
    rs.expect_line(
        0,
        &format!("Warning git_loc.sh failed: org/repo:{ff} (took <dur>): exit status 4"),
    );
    rs.expect_line(
        0,
        "Got 2 (50.00%) new commit's files, 1 without files, 1 failed, all 4, took <dur>",
    );
    rs.expect_line(
        0,
        "Got 0 (0.00%) new commit's BOC stats, 3 without stats, 1 failed, all 4, took <dur>",
    );
    let mut errs = rs.stderr_lines(0);
    errs.sort();
    assert_eq!(
        errs,
        vec![
            format!("Warning git_files.sh failed: org/repo:{ff} (took <dur>): exit status 4"),
            format!("Warning git_loc.sh failed: org/repo:{ff} (took <dur>): exit status 4"),
        ]
    );
    assert!(rs.skipped().contains(&(ff.to_string(), "1".to_string())));
    assert!(rs.skipped().contains(&(ff.to_string(), "2".to_string())));
}

#[test]
fn commits_files_fail() {
    // git_files.sh fails for commit 2 as well.
    let case = commits_case("commits_files_fail")
        .env("BAD_SHA", "{sha1}")
        .script(
        "git_files.sh",
        Script::Wrap(
            "if [ \"$2\" = \"$BAD_SHA\" ]; then echo 'boom' >&2; exit 7; fi\nexec \"$REAL\" \"$@\"",
        ),
    );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Got 1 (33.33%) new commit's files, 1 without files, 1 failed, all 3, took <dur>",
    );
    assert_eq!(rs.files().len(), 1);
    let mut skipped = rs.skipped();
    skipped.retain(|(_, r)| r == "1");
    let mut expected = vec![
        (rs.shas[0].clone(), "1".to_string()),
        (rs.shas[1].clone(), "1".to_string()),
    ];
    expected.sort();
    assert_eq!(skipped, expected);
}

#[test]
fn commits_files_none() {
    // A commit whose files all match the skip pattern counts as "without
    // files" and is skipped (reason 1).
    let case = Case::new("commits_files_none")
        .process_commits()
        .debug()
        .script(
            "git_files.sh",
            Script::Fake("echo 1577836800\necho 'README.md♂♀5'\necho 'docs/x.md♂♀6'"),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Got 0 (0.00%) new commit's files, 3 without files, 0 failed, all 3, took <dur>",
    );
    assert!(rs.files().is_empty());
    assert_eq!(rs.skipped().iter().filter(|(_, r)| r == "1").count(), 3);
}

#[test]
fn commits_no_skip_pattern() {
    let case = commits_case("commits_no_skip_pattern").yaml(NO_SKIP_YAML);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let mut expected = vec![
        strs(&[&rs.shas[1], "b.md", "2", "2020-01-02 00:00:00", "md"]),
        strs(&[
            &rs.shas[1],
            "src/main.go",
            "12",
            "2020-01-02 00:00:00",
            "go",
        ]),
        strs(&[&rs.shas[3], "c.txt", "2", "2020-01-04 00:00:00", "txt"]),
    ];
    expected.sort();
    assert_eq!(rs.files(), expected);
}

#[test]
fn commits_files_empty_time() {
    // An empty first line: the files get the zero time.
    let case = commits_case("commits_files_empty_time")
        .env("EMPTY_SHA", "{sha0}")
        .script(
            "git_files.sh",
            Script::Wrap("if [ \"$2\" = \"$EMPTY_SHA\" ]; then echo; echo 'x.txt♂♀5'; exit 0; fi\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        &format!(
            "Empty time returned for repo: org/repo, sha: {}",
            rs.shas[0]
        ),
    );
    assert_eq!(
        rs.query(&format!(
            "select path, size::text, dt::text, ext from gha_commits_files where sha = '{}'",
            rs.shas[0]
        )),
        vec![strs(&["x.txt", "5", "0001-01-01 00:00:00", "txt"])]
    );
}

#[test]
fn commits_files_invalid_time() {
    let case = commits_case("commits_files_invalid_time")
        .env("BADTIME_SHA", "{sha0}")
        .script(
            "git_files.sh",
            Script::Wrap("if [ \"$2\" = \"$BADTIME_SHA\" ]; then echo notanumber; echo 'x.txt♂♀5'; exit 0; fi\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        &format!(
            "Invalid time returned for repo: org/repo, sha: {}: 'notanumber'",
            rs.shas[0]
        ),
    );
    rs.expect_line(
        0,
        "Got 2 (66.67%) new commit's files, 1 without files, 0 failed, all 3, took <dur>",
    );
    assert!(rs
        .skipped()
        .contains(&(rs.shas[0].clone(), "1".to_string())));
}

#[test]
fn commits_files_invalid_line() {
    // A line without the `♂♀` separator is fatal.
    let case = Case::new("commits_files_invalid_line")
        .process_commits()
        .debug()
        .script(
            "git_files.sh",
            Script::Fake("echo 1577836800\necho 'weird line without separator'"),
        )
        .code_only_errors()
        .no_data();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    rs.expect_no_prefix(0, "Postprocessed");
    let err = rs.error(0).unwrap_or_default();
    assert!(
        err.contains("invalid fileData returned for repo: org/repo, sha: ")
            && err.contains(": 'weird line without separator'"),
        "{err}"
    );
}

#[test]
fn commits_files_special_sizes() {
    // `-` sizes (special entries) are stored as -2; empty names are skipped.
    let case = commits_case("commits_files_special_sizes")
        .env("SPECIAL_SHA", "{sha0}")
        .script(
            "git_files.sh",
            Script::Wrap("if [ \"$2\" = \"$SPECIAL_SHA\" ]; then echo 1577836800; echo 'sub♂♀-'; echo '♂♀7'; echo 'Makefile♂♀0'; echo '   '; exit 0; fi\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.query(&format!(
            "select path, size::text, ext from gha_commits_files where sha = '{}' order by path",
            rs.shas[0]
        )),
        vec![
            strs(&["Makefile", "0", "makefile"]),
            strs(&["sub", "-2", "sub"])
        ]
    );
}

#[test]
fn commits_loc_fail() {
    let case = Case::new("commits_loc_fail")
        .mode("1")
        .process_commits()
        .debug()
        .repo(Repo::Cloned(3))
        .env("BAD_SHA", "{sha1}")
        .script(
            "git_loc.sh",
            Script::Wrap("if [ \"$2\" = \"$BAD_SHA\" ]; then exit 9; fi\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        &format!(
            "Warning git_loc.sh failed: org/repo:{} (took <dur>): exit status 9",
            rs.shas[1]
        ),
    );
    rs.expect_line(
        0,
        "Got 2 (50.00%) new commit's BOC stats, 1 without stats, 1 failed, all 4, took <dur>",
    );
    let mut skipped = vec![
        (rs.shas[0].clone(), "1".to_string()),
        (rs.shas[0].clone(), "2".to_string()),
        (rs.shas[1].clone(), "2".to_string()),
    ];
    skipped.sort();
    assert_eq!(rs.skipped(), skipped);
    assert_eq!(
        rs.column(&format!(
            "select coalesce(loc_added::text, 'null') from gha_commits where sha = '{}'",
            rs.shas[1]
        )),
        strs(&["null"])
    );
}

#[test]
fn commits_loc_garbage() {
    // Unparseable output: zeros are stored (and the commit is not skipped),
    // but it does not count as a commit with stats.
    let case = Case::new("commits_loc_garbage")
        .mode("1")
        .process_commits()
        .debug()
        .repo(Repo::Cloned(3))
        .env("WEIRD_SHA", "{sha1}")
        .script(
            "git_loc.sh",
            Script::Wrap("if [ \"$2\" = \"$WEIRD_SHA\" ]; then echo 'garbage output here'; exit 0; fi\nexec \"$REAL\" \"$@\""),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Got 2 (50.00%) new commit's BOC stats, 2 without stats, 0 failed, all 4, took <dur>",
    );
    assert_eq!(
        rs.query(&format!("select loc_added::text, loc_removed::text, files_changed::text from gha_commits where sha = '{}'", rs.shas[1])),
        vec![strs(&["0", "0", "0"])]
    );
    assert_eq!(
        rs.skipped(),
        vec![
            (rs.shas[0].clone(), "1".to_string()),
            (rs.shas[0].clone(), "2".to_string())
        ]
    );
}

#[test]
fn commits_loc_singular_plural() {
    // All the shortstat spellings.
    let case = Case::new("commits_loc_singular_plural")
        .mode("1")
        .process_commits()
        .debug()
        .repo(Repo::Cloned(3))
        .env("S1", "{sha1}")
        .env("S2", "{sha2}")
        .env("S3", "{sha3}")
        .script(
            "git_loc.sh",
            Script::Wrap("case \"$2\" in\n\"$S1\") echo ' 1 file changed, 1 insertion(+), 1 deletion(-)';;\n\"$S2\") echo ' 12 files changed, 34 insertions(+), 56 deletions(-)';;\n\"$S3\") echo '  7 files changed, 8 deletions(-)  ';;\n*) exec \"$REAL\" \"$@\";;\nesac"),
        );
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    let mut loc = vec![
        strs(&[&rs.shas[1], "1", "1", "1"]),
        strs(&[&rs.shas[2], "34", "56", "12"]),
        strs(&[&rs.shas[3], "0", "8", "7"]),
    ];
    loc.sort();
    assert_eq!(rs.loc(), loc);
}

#[test]
fn commits_repo_not_cloned() {
    // Payload for a repo that is not cloned: both scripts fail.
    let case = commits_case("commits_repo_not_cloned")
        .also("insert into gha_repos(id, name, org_id, org_login) values (101, 'org/other', 10, 'org');
insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values (1003, 'PushEvent', 1, 101, '2020-01-02 01:00:00', 10, 'alice', 'org/other');
insert into gha_payloads(event_id, push_id, size, ref, head, befor, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values (1003, 3, 1, 'refs/heads/main', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '', 'alice', 101, 'org/other', 'PushEvent', '2020-01-02 01:00:00');")
        .env("GHA2DB_PROCESS_REPOS", "");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Database '<db>' processed took <dur>, new commits: 4");
    rs.expect_line(
        0,
        "Got 2 (50.00%) new commit's files, 1 without files, 1 failed, all 4, took <dur>",
    );
    rs.expect_prefix(
        0,
        "Warning git_loc.sh failed: org/other:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (took <dur>): exit status 3",
    );
}

#[test]
fn commits_mt() {
    let case = Case::new("commits_mt")
        .process_commits()
        .debug()
        .no_env("GHA2DB_ST")
        .unordered();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(
        0,
        "Got 2 (66.67%) new commit's files, 1 without files, 0 failed, all 3, took <dur>",
    );
    assert_eq!(rs.files().len(), 2);
}

#[test]
fn commits_two_dbs() {
    let case = Case::new("commits_two_dbs")
        .yaml(TWO_YAML)
        .db2(default_seed())
        .process_commits()
        .debug()
        .unordered();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Running on database: <db>");
    rs.expect_line(0, "Running on database: <db2>");
    rs.expect_line(0, "Got 2 DBs new commits list: took <dur>");
    rs.expect_line(
        0,
        "Got 4 (66.67%) new commit's files, 2 without files, 0 failed, all 6, took <dur>",
    );
    rs.expect_line(0, "Got 2 DBs new commits BOC stats: took <dur>");
    assert_eq!(rs.files().len(), 2);
    // No `files_skip_pattern` for the second project: b.md is kept there.
    assert_eq!(rs.query2("select sha from gha_commits_files").len(), 3);
}

#[test]
fn commits_missing_sql() {
    let case = Case::new("commits_missing_sql")
        .process_commits()
        .steps(vec![
            Step::Shell("rm {dir}/work/util_sql/list_unprocessed_commits_files.sql"),
            Step::Run(Vec::new()),
        ])
        .code_only_errors()
        .no_data();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(2));
    let err = rs.error(0).unwrap_or_default();
    assert!(err.contains("list_unprocessed_commits_files.sql"), "{err}");
}

// -------------------------------------------------------------- everything

#[test]
fn full_default() {
    // repos + backfill + orphan restore + files/LOC, single-threaded.
    let case = Case::new("full_default")
        .process_repos()
        .mode("1")
        .orphan(WIDE_RANGE)
        .process_commits()
        .debug();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    // Order of the phases.
    let lines = rs.lines(0);
    let pos = |p: &str| {
        lines
            .iter()
            .position(|l| l.starts_with(p))
            .unwrap_or_else(|| panic!("{p}"))
    };
    let p_pull = pos("Pulled org/repo: took ");
    let p_fetch = pos("FetchCommitsMode=1: processing DB");
    let p_orphan = pos("Restoring orphan commits: processing DB");
    let p_files = pos("Running on database: ");
    let p_loc = pos("BOC stats running on database: ");
    let p_end = pos("All repos processed in: ");
    assert!(
        p_pull < p_fetch
            && p_fetch < p_orphan
            && p_orphan < p_files
            && p_files < p_loc
            && p_loc < p_end
    );
    rs.expect_line(0, "Successfully processed 1/1 repos");
    rs.expect_line(
        0,
        "<db>/org/repo: successfully backfilled 3 commits and 2 commit roles for 2 events",
    );
    rs.expect_line(0, "<db>/org/repo: successfully restored 1 orphan commits");
    // The pull moved the clone to commit 4 — all payload/commit SHAs resolve.
    rs.expect_line(0, "Database '<db>' processed took <dur>, new commits: 4");
    rs.expect_line(
        0,
        "Got 3 (75.00%) new commit's files, 1 without files, 0 failed, all 4, took <dur>",
    );
    rs.expect_line(
        0,
        "Got 4 (100.00%) new commit's BOC stats, 0 without stats, 0 failed, all 4, took <dur>",
    );
    assert!(rs.stderr_lines(0).is_empty());
    assert_eq!(rs.head(), Some(rs.shas[3].clone()));
    assert_eq!(rs.commit_shas().len(), 4);
    assert_eq!(rs.files().len(), 4);
    assert_eq!(rs.skipped(), vec![(rs.shas[0].clone(), "1".to_string())]);
    let mut loc = vec![
        strs(&[&rs.shas[0], "1", "0", "1"]),
        strs(&[&rs.shas[1], "3", "0", "2"]),
        strs(&[&rs.shas[2], "2", "2", "2"]),
        strs(&[&rs.shas[3], "1", "0", "1"]),
    ];
    loc.sort();
    assert_eq!(rs.loc(), loc);
    assert_eq!(rs.count("select count(*) from gha_events_commits_files"), 4);
}

#[test]
fn full_default_mt() {
    let case = Case::new("full_default_mt")
        .process_repos()
        .mode("1")
        .orphan(WIDE_RANGE)
        .process_commits()
        .debug()
        .no_env("GHA2DB_ST")
        .unordered();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 1/1 repos");
    rs.expect_line(0, "<db>/org/repo: successfully restored 1 orphan commits");
    assert_eq!(rs.commit_shas().len(), 4);
    assert_eq!(rs.files().len(), 4);
}

#[test]
fn full_two_dbs_two_repos() {
    let case = Case::new("full_two_dbs_two_repos")
        .yaml(TWO_YAML)
        .db2(default_seed())
        .extra(&["org/zeta"])
        .also("insert into gha_repos(id, name, org_id, org_login) values (101, 'org/zeta', 10, 'org');")
        .process_repos()
        .mode("1")
        .orphan(WIDE_RANGE)
        .process_commits()
        .debug()
        .unordered();
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    rs.expect_line(0, "Successfully processed 2/2 repos");
    rs.expect_line(
        0,
        "FetchCommitsMode=1: processing DB '<db>' (2 repos, threads 1, batch 1000)",
    );
    rs.expect_line(
        0,
        "FetchCommitsMode=1: processing DB '<db2>' (1 repos, threads 1, batch 1000)",
    );
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 6 commits and 4 commit roles in: <dur>",
    );
    // org/zeta has no gha_events rows in <db>: its orphans are not restored.
    rs.expect_line(
        0,
        "<db>/org/zeta: no gha_events rows for this repo, skipping orphan commits restore",
    );
    rs.expect_line(0, "Finished orphan commit restore: processed 3 repos, checked 12 commits, restored 2 in: <dur>");
    assert_eq!(rs.commit_shas().len(), 4);
    assert_eq!(rs.query2("select sha from gha_commits").len(), 4);
}

#[test]
fn full_getreposskip() {
    // GETREPOSSKIP: the tool does nothing at all (no clone/pull, no backfill,
    // no commits processing), it only prints the final timing line.
    let case = Case::new("full_getreposskip")
        .process_repos()
        .mode("1")
        .process_commits()
        .debug()
        .env("GHA2DB_GETREPOSSKIP", "1");
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(0), Some(0));
    assert_eq!(
        rs.lines(0),
        vec![
            BANNER.to_string(),
            "All repos processed in: <dur>".to_string()
        ]
    );
    assert_eq!(rs.head(), Some(rs.shas[2].clone()));
    assert!(rs.commit_shas().is_empty());
    assert!(rs.files().is_empty());
}

#[test]
fn mode1_two_dbs_rerun() {
    // Two databases; an event added to the second one between the runs.
    let case = Case::new("mode1_two_dbs_rerun")
        .yaml(TWO_YAML)
        .db2(default_seed())
        .mode("1")
        .debug()
        .unordered()
        .steps(vec![
            Step::Run(Vec::new()),
            Step::Sql2("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values (1003, 'PushEvent', 2, 100, '2020-01-06 01:00:00', 10, 'bob', 'org/repo');
insert into gha_payloads(event_id, push_id, size, ref, head, befor, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values (1003, 3, 1, 'refs/heads/main', '{sha3}', '{sha2}', 'bob', 100, 'org/repo', 'PushEvent', '2020-01-06 01:00:00');"),
            Step::Run(Vec::new()),
        ]);
    let Some(rs) = both(&case) else {
        return;
    };
    assert_eq!(rs.code(1), Some(0));
    rs.expect_line(
        0,
        "Finished all DBs: backfilled 6 commits and 4 commit roles in: <dur>",
    );
    rs.expect_prefix(1, "<db>/org/repo: no need to backfill commits since ");
    rs.expect_line(
        1,
        "<db2>/org/repo: successfully backfilled 1 commits and 0 commit roles for 1 events",
    );
    rs.expect_line(
        1,
        "Finished all DBs: backfilled 1 commits and 0 commit roles in: <dur>",
    );
    assert_eq!(rs.count("select count(*) from gha_commits"), 3);
    assert_eq!(rs.query2("select sha from gha_commits").len(), 4);
}
