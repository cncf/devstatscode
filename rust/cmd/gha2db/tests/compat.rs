//! Go ⇄ Rust compatibility tests for `gha2db`.
//!
//! Every case runs each binary against its own scratch database
//! (`dbtest_gha2db_<case>_<go|rs>`, the full DevStats schema plus the
//! case's seed rows), its own scripted fake GH Archive
//! (`devstats_compat::gharchive::FakeGHArchive`, reached through
//! `GHA2DB_GHARCHIVE_URL`) and its own working directory (`jsons/`,
//! `skip_dates.yaml`, optionally `hide/hide.csv`). Compared per run: the
//! exit code, stdout (as lines, with the archive URL, the binary path, the
//! database names, durations, GC statistics, the `N remain:` lists and
//! now-derived dates masked; in order for single-threaded cases, as a sorted
//! multiset otherwise), the `Error: '…'` stderr lines; afterwards every table
//! of the database(s), the sorted log of the archive requests each binary
//! made and (when asked) the files left in `jsons/`.
//!
//! The archive hours are pseudonymised real GH Archive samples
//! (`rust/compat/fixtures/gha2db/*.json.gz`, see `gen_fixtures.py` there):
//! two pre-2015 (old format) hours, three new-format hours, one hour with
//! JSONs the tools cannot decode and the (real) empty 2012-03-10-15 hour.
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use devstats_compat::gharchive::{Archive, FakeGHArchive};
use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{fixture, go_binary, is_go_duration, run, rust_binary, Invocation, Outcome};
use devstatscode::hash::hash_strings;
use regex::Regex;
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("gha2db")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_gha2db"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// Fixture hours (`rust/compat/fixtures/gha2db/<key>.json.gz`).
const H2013: &str = "2013-06-01-10";
const H2014: &str = "2014-12-31-23";
const H2015: &str = "2015-01-01-15";
const H2020: &str = "2020-05-01-10";
const H2025: &str = "2025-11-20-12";
/// Three events whose `created_at` is not RFC3339 — every JSON fails to decode.
const H_BROKEN: &str = "2012-03-11-12";
/// The real empty hour (a 20-byte gzip of nothing).
const H_EMPTY: &str = "2012-03-10-15";

/// SHA-1s of the strings anonymised by the `hide/hide.csv` cases.
const RULLZER_SHA1: &str = "16f2d52edcaec44656b79b18c493fd900b9ac5db";
const BABOLIVIER_SHA1: &str = "cb305dd4a7ad43570b69f6347676311ce690e33c";
const USER126_EMAIL_SHA1: &str = "5d35ebb5f44f7491040b3950b2e9fc95b0d9e408";
const ROELAND_NAME_SHA1: &str = "5d82dafddc14fd1c98a1739e7e648ea3133127da";

fn fixture_hour(key: &str) -> Archive {
    Archive::gz(fs::read(fixture(&format!("gha2db/{key}.json.gz"))).unwrap())
}

fn fixture_lines(key: &str) -> Vec<String> {
    let mut gz = flate2::read::MultiGzDecoder::new(
        fs::File::open(fixture(&format!("gha2db/{key}.json.gz"))).unwrap(),
    );
    let mut s = String::new();
    std::io::Read::read_to_string(&mut gz, &mut s).unwrap();
    s.lines()
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Actor rows resolving the `Signed-off-by:` trailers of the nextcloud
/// commits of the 2020 hour: by email, by `gha_actors_names`, by
/// `gha_actors.name`; the remaining trailers stay unresolved.
const ROLES_SEED: &str = "
insert into gha_actors(id, login, name) values (1001, 'rullzer', 'Roeland Jago Douma');
insert into gha_actors_emails(actor_id, email) values (1001, 'user126@example.com');
insert into gha_actors(id, login, name) values (1002, 'mario', 'Mario D');
insert into gha_actors_names(actor_id, name) values (1002, 'Mario Danic');
insert into gha_actors(id, login, name) values (1003, 'nickvergessen', 'Joas Schilling');
";

// ---------------------------------------------------------------------------
// Masks
// ---------------------------------------------------------------------------

/// `All done: 1.234s`, `Time: 1.234s` — Go `time.Duration`s after a marker.
static DURATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(All done: |Time: )(-?[0-9][0-9.hmsµn]*)$").unwrap());
/// `3 remain: 2020-05-01 10, 2020-05-01 11` — the remaining hours of the
/// final join (Go map order, and in MT mode the set depends on timing).
static REMAIN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d+) remain: .*$").unwrap());
/// A `time.Now()`-derived date/hour/time stamp (the fixtures end in 2025).
static NOW: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"20(?:2[6-9]|[3-9]\d)-\d{2}-\d{2}(?:-\d{1,2}|T\d{2}| \d{2}:\d{2}:\d{2}(?:\.\d+)?(?: [+-]\d{4} \S+)?)?",
    )
    .unwrap()
});
/// A timestamp cell of a dump (`2026-09-12 01:02:03.123456`).
static TIMESTAMP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\d{4})-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:\d{2})?)?$").unwrap()
});

// ---------------------------------------------------------------------------
// Case / Side
// ---------------------------------------------------------------------------

struct Case {
    name: &'static str,
    /// Command line arguments of every run (one run per entry).
    runs: Vec<Vec<String>>,
    env: Vec<(&'static str, &'static str)>,
    seed: Vec<String>,
    affs: bool,
    affs_seed: Vec<String>,
    hours: Vec<(String, Vec<Archive>)>,
    /// Extra files of the working directory (relative path, content).
    files: Vec<(String, Vec<u8>)>,
    skip_dates: Option<String>,
    jsons_dir: bool,
    st: bool,
    ordered: bool,
    loose: Vec<String>,
    compare_data: bool,
    compare_errors: bool,
    compare_jsons: bool,
    tz: &'static str,
}

impl Case {
    fn new(name: &'static str) -> Self {
        Case {
            name,
            runs: Vec::new(),
            env: Vec::new(),
            seed: Vec::new(),
            affs: false,
            affs_seed: Vec::new(),
            hours: Vec::new(),
            files: Vec::new(),
            skip_dates: Some("skip_dates: []\n".to_string()),
            jsons_dir: true,
            st: true,
            ordered: true,
            loose: Vec::new(),
            compare_data: true,
            compare_errors: true,
            compare_jsons: false,
            tz: "UTC",
        }
    }

    /// Add a run with these arguments.
    fn args(mut self, args: &[&str]) -> Self {
        self.runs.push(args.iter().map(|a| a.to_string()).collect());
        self
    }

    fn env(mut self, k: &'static str, v: &'static str) -> Self {
        self.env.push((k, v));
        self
    }

    fn seed(mut self, sql: &str) -> Self {
        self.seed.push(sql.to_string());
        self
    }

    /// Use a second (shared affiliations) database (`GHA2DB_AFFILIATIONS_DB`).
    fn affs(mut self) -> Self {
        self.affs = true;
        self
    }

    fn affs_seed(mut self, sql: &str) -> Self {
        self.affs_seed.push(sql.to_string());
        self
    }

    /// Script the answers of an archive hour (`YYYY-MM-DD-H`).
    fn hour(mut self, key: &str, answers: Vec<Archive>) -> Self {
        self.hours.push((key.to_string(), answers));
        self
    }

    /// Serve the fixture of this hour.
    fn fixture(self, key: &str) -> Self {
        self.hour(key, vec![fixture_hour(key)])
    }

    fn file(mut self, name: &str, content: &[u8]) -> Self {
        self.files.push((name.to_string(), content.to_vec()));
        self
    }

    /// Write `hide/hide.csv`.
    fn hide(self, csv: &str) -> Self {
        self.file("hide/hide.csv", csv.as_bytes())
    }

    /// The `skip_dates.yaml` content (default: an empty list).
    fn skip_dates(mut self, yaml: &str) -> Self {
        self.skip_dates = Some(yaml.to_string());
        self
    }

    fn no_skip_dates_file(mut self) -> Self {
        self.skip_dates = None;
        self
    }

    fn no_jsons_dir(mut self) -> Self {
        self.jsons_dir = false;
        self
    }

    /// Multi-threaded run (no `GHA2DB_ST`); stdout compared as a multiset.
    fn mt(mut self) -> Self {
        self.st = false;
        self.ordered = false;
        self
    }

    /// Lines starting with `prefix` are compared by prefix only.
    fn loose(mut self, prefix: &str) -> Self {
        self.loose.push(prefix.to_string());
        self
    }

    /// Skip the `Error: '…'` lines compare (Go panics outside `FatalOnError`).
    fn no_errors_compare(mut self) -> Self {
        self.compare_errors = false;
        self
    }

    /// Also compare the files left in `jsons/`.
    fn compare_jsons(mut self) -> Self {
        self.compare_jsons = true;
        self
    }

    fn tz(mut self, tz: &'static str) -> Self {
        self.tz = tz;
        self
    }
}

type TableDump = (Vec<String>, Vec<Vec<String>>);

struct Side {
    db: TestDb,
    affs: Option<TestDb>,
    archive: FakeGHArchive,
    dir: TempDir,
    outs: Vec<Outcome>,
    bin_str: String,
}

fn mask_cell(v: &str) -> String {
    if let Some(c) = TIMESTAMP.captures(v) {
        if c[1].parse::<i32>().unwrap() >= 2026 {
            return "<now>".to_string();
        }
    }
    v.to_string()
}

fn dump_db(db: &TestDb) -> BTreeMap<String, TableDump> {
    let con = db.conn();
    let mut res = BTreeMap::new();
    for t in cpg::tables(&con) {
        let mut cols = cpg::table_columns(&con, &t);
        cols.sort();
        let names: Vec<String> = cols.iter().map(|c| format!("\"{}\"::text", c.0)).collect();
        let order: Vec<String> = (1..=names.len()).map(|i| i.to_string()).collect();
        let mut rows: Vec<Vec<String>> = cpg::snapshot(
            &con,
            &format!(
                "select {} from \"{t}\" order by {}",
                names.join(", "),
                order.join(", ")
            ),
            &[],
        )
        .rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| mask_cell(&v)).collect())
        .collect();
        rows.sort();
        if !rows.is_empty() {
            res.insert(t, (cols.iter().map(|c| c.0.clone()).collect(), rows));
        }
    }
    con.close();
    res
}

impl Side {
    fn mask(&self, l: &str) -> String {
        if l.starts_with("alloc:") {
            return "<gc>".to_string();
        }
        let l = l.replace(self.archive.base_url(), "<archive>/");
        let l = l.replace(&self.bin_str, "<bin>");
        let l = l.replace(&self.db.name, "<db>");
        let l = match &self.affs {
            Some(a) => l.replace(&a.name, "<affsdb>"),
            None => l,
        };
        let l = l.replace(&self.dir.path().to_string_lossy().to_string(), "<dir>");
        let l = DURATION.replace_all(&l, |c: &regex::Captures| {
            if is_go_duration(&c[2]) {
                format!("{}<dur>", &c[1])
            } else {
                c[0].to_string()
            }
        });
        let l = REMAIN.replace_all(&l, "$1 remain: <hours>");
        NOW.replace_all(&l, "<now>").to_string()
    }

    fn code(&self, i: usize) -> Option<i32> {
        self.outs[i].code
    }

    /// The masked stdout lines; the decoder error echoed after
    /// `<dt>: Cannot unmarshal:` + the JSON is replaced by `<decode error>`
    /// (jsoniter and serde word their errors differently).
    fn lines(&self, i: usize) -> Vec<String> {
        let mut v: Vec<String> = self.outs[i]
            .stdout_str()
            .lines()
            .map(|l| self.mask(l))
            .collect();
        let mut j = 0;
        while j < v.len() {
            if v[j].ends_with(": Cannot unmarshal:") && j + 2 < v.len() {
                v[j + 2] = "<decode error>".to_string();
                j += 3;
            } else {
                j += 1;
            }
        }
        v
    }

    fn sorted_lines(&self, i: usize) -> Vec<String> {
        let mut v = self.lines(i);
        v.sort();
        v
    }

    /// The `Error: '…'` lines of stderr (fatal errors).
    fn errors(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stderr_str()
            .lines()
            .filter(|l| l.starts_with("Error: '"))
            .map(|l| self.mask(l))
            .collect()
    }

    fn expect_line(&self, i: usize, line: &str) {
        assert!(
            self.lines(i).iter().any(|l| l == line),
            "run #{i} of {} lacks line {line:?}:\n{}",
            self.bin_str,
            self.outs[i].stdout_str()
        );
    }

    fn expect_prefix(&self, i: usize, prefix: &str) {
        assert!(
            self.lines(i).iter().any(|l| l.starts_with(prefix)),
            "run #{i} of {} lacks a line starting with {prefix:?}:\n{}",
            self.bin_str,
            self.outs[i].stdout_str()
        );
    }

    fn count_prefix(&self, i: usize, prefix: &str) -> usize {
        self.lines(i)
            .iter()
            .filter(|l| l.starts_with(prefix))
            .count()
    }

    fn count(&self, sql: &str) -> i64 {
        let con = self.db.conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows[0][0].parse().unwrap()
    }

    fn column(&self, sql: &str) -> Vec<String> {
        let con = self.db.conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.column(0)
    }

    fn data(&self) -> Vec<BTreeMap<String, TableDump>> {
        let mut v = vec![dump_db(&self.db)];
        if let Some(a) = &self.affs {
            v.push(dump_db(a));
        }
        v
    }

    fn requests(&self) -> Vec<String> {
        self.archive
            .sorted_requests()
            .iter()
            .map(|r| self.mask(r))
            .collect()
    }

    /// The files of `jsons/` (name → content).
    fn jsons(&self) -> BTreeMap<String, String> {
        let mut res = BTreeMap::new();
        let dir = self.dir.path().join("jsons");
        if let Ok(rd) = fs::read_dir(&dir) {
            for e in rd {
                let e = e.unwrap();
                let name = e.file_name().to_string_lossy().to_string();
                res.insert(name, fs::read_to_string(e.path()).unwrap());
            }
        }
        res
    }
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let schema = fs::read_to_string(fixture("structure/full_structure.sql")).unwrap();
    let db = TestDb::fresh(&format!("gha2db_{}_{}", case.name, suffix))?;
    db.exec(&schema);
    for s in &case.seed {
        db.exec(s);
    }
    let affs = if case.affs {
        let a = TestDb::fresh(&format!("gha2db_{}_{}_affs", case.name, suffix))?;
        a.exec(&schema);
        for s in &case.affs_seed {
            a.exec(s);
        }
        Some(a)
    } else {
        None
    };
    let archive = FakeGHArchive::start();
    for (k, answers) in &case.hours {
        archive.hour(k, answers.clone());
    }
    let dir = TempDir::new().unwrap();
    if case.jsons_dir {
        fs::create_dir(dir.path().join("jsons")).unwrap();
    }
    if let Some(y) = &case.skip_dates {
        fs::write(dir.path().join("skip_dates.yaml"), y).unwrap();
    }
    for (name, content) in &case.files {
        let p = dir.path().join(name);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, content).unwrap();
    }
    let data_dir = leak(&format!("{}/", dir.path().to_string_lossy()));
    let bin_str = bin.to_string_lossy().to_string();
    let mut outs = Vec::new();
    for args in &case.runs {
        let mut inv = Invocation::new().cwd(dir.path());
        for (k, v) in db.env() {
            inv = inv.env(k, v);
        }
        inv = inv
            .env("GHA2DB_GHARCHIVE_URL", archive.base_url())
            .env("GHA2DB_LOCAL", "1")
            .env("GHA2DB_DATADIR", data_dir)
            .env("GHA2DB_HTTP_RETRY", "1")
            .env("TZ", case.tz);
        if case.st {
            inv = inv.env("GHA2DB_ST", "1");
        }
        if let Some(a) = &affs {
            inv = inv.env("GHA2DB_AFFILIATIONS_DB", &a.name);
        }
        for (k, v) in &case.env {
            inv = inv.env(k, v);
        }
        for a in args {
            inv = inv.arg(a.clone());
        }
        outs.push(run(bin, &inv));
    }
    Some(Side {
        db,
        affs,
        archive,
        dir,
        outs,
        bin_str,
    })
}

fn check(case: Case) -> Option<(Side, Side)> {
    let rs = run_side(&rust_bin(), &case, "rs")?;
    let Some(go_bin) = go_bin() else {
        // Without the Go reference at least make sure the runs finished.
        return None;
    };
    let go = run_side(&go_bin, &case, "go").unwrap();
    for i in 0..case.runs.len() {
        let ctx = || {
            format!(
                "\ncase {} run #{i} {:?}\n--- go (code {:?}) stdout ---\n{}\n--- go stderr ---\n{}\n--- rust (code {:?}) stdout ---\n{}\n--- rust stderr ---\n{}\n",
                case.name,
                case.runs[i],
                go.code(i),
                go.outs[i].stdout_str(),
                go.outs[i].stderr_str(),
                rs.code(i),
                rs.outs[i].stdout_str(),
                rs.outs[i].stderr_str()
            )
        };
        if std::env::var_os("G2R_DUMP").is_some() {
            eprintln!("{}", ctx());
        }
        assert_eq!(go.code(i), rs.code(i), "exit code differs{}", ctx());
        let loosen = |lines: Vec<String>| -> Vec<String> {
            lines
                .into_iter()
                .map(
                    |l| match case.loose.iter().find(|p| l.starts_with(p.as_str())) {
                        Some(p) => format!("{p}<loose>"),
                        None => l,
                    },
                )
                .collect()
        };
        if case.ordered {
            assert_eq!(
                loosen(go.lines(i)),
                loosen(rs.lines(i)),
                "stdout differs{}",
                ctx()
            );
        } else {
            assert_eq!(
                loosen(go.sorted_lines(i)),
                loosen(rs.sorted_lines(i)),
                "stdout (multiset) differs{}",
                ctx()
            );
        }
        if case.compare_errors {
            assert_eq!(
                loosen(go.errors(i)),
                loosen(rs.errors(i)),
                "Error lines differ{}",
                ctx()
            );
        }
    }
    if case.compare_data {
        assert_eq!(
            go.data(),
            rs.data(),
            "database contents differ (case {})",
            case.name
        );
    }
    assert_eq!(
        go.requests(),
        rs.requests(),
        "archive requests differ (case {})",
        case.name
    );
    if case.compare_jsons {
        assert_eq!(
            go.jsons(),
            rs.jsons(),
            "jsons/ files differ (case {})",
            case.name
        );
    }
    Some((go, rs))
}

/// Run `f` on both sides.
fn both(sides: &Option<(Side, Side)>, f: impl Fn(&Side)) {
    if let Some((go, rs)) = sides {
        f(go);
        f(rs);
    }
}

const COMPILED: &str = "Compiled None, commit: None on None using None";
const USAGE: &str = "Arguments required: date_from_YYYY-MM-DD hour_from_HH date_to_YYYY-MM-DD hour_to_HH ['org1,org2,...,orgN' ['repo1,repo2,...,repoN']]";

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------

#[test]
fn usage_no_args() {
    let s = check(
        Case::new("usage0")
            .args(&[])
            .args(&["2015-01-01", "15", "2015-01-01"]),
    );
    both(&s, |s| {
        for i in 0..2 {
            assert_eq!(s.code(i), Some(1));
            assert_eq!(s.lines(i), vec![COMPILED.to_string(), USAGE.to_string()]);
        }
        assert!(s.requests().is_empty());
        assert!(s.data()[0].is_empty());
    });
}

#[test]
fn bad_hour() {
    let s = check(
        Case::new("badhour")
            .args(&["2015-01-01", "1x", "2015-01-01", "15"])
            .args(&["2015-01-01", "15", "2015-01-01", "now-ish"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        assert_eq!(
            s.errors(0),
            vec!["Error: 'strconv.Atoi: parsing \"1x\": invalid syntax'".to_string()]
        );
        // The deferred commit roles update still runs while unwinding.
        s.expect_line(0, "Processing 0 commit roles using 1 CPUs");
        s.expect_line(0, "Updated 0/0 roles using 1 CPUs");
        assert_eq!(s.code(1), Some(2));
        assert_eq!(
            s.errors(1),
            vec!["Error: 'strconv.Atoi: parsing \"now-ish\": invalid syntax'".to_string()]
        );
        assert!(s.requests().is_empty());
    });
}

#[test]
fn bad_date() {
    let s = check(
        Case::new("baddate")
            .args(&["2015-13-01", "15", "2015-13-01", "15"])
            .args(&["2015-02-30", "15", "2015-03-01", "15"])
            .args(&["2015-01-01", "15", "2015/01/02", "15"])
            .args(&["2015-01-01", "25", "2015-01-01", "25"])
            .args(&["2015-01-01", "-5", "2015-01-01", "15"])
            .args(&["2015-01-01", "100", "2015-01-01", "15"]),
    );
    both(&s, |s| {
        for i in 0..6 {
            assert_eq!(s.code(i), Some(2), "run {i}");
            assert_eq!(s.errors(i).len(), 1, "run {i}");
        }
        assert!(s.errors(0)[0].contains("month out of range"));
        assert!(s.errors(1)[0].contains("day out of range"));
        assert!(s.errors(2)[0].contains("cannot parse"));
        assert!(s.errors(3)[0].contains("hour out of range"));
        assert!(s.requests().is_empty());
    });
}

#[test]
fn reversed_range() {
    let s = check(
        Case::new("reversed")
            .args(&["2015-01-01", "16", "2015-01-01", "15", "rust-lang"])
            .args(&["2015-01-02", "0", "2015-01-01", "23"]),
    );
    both(&s, |s| {
        for i in 0..2 {
            assert_eq!(s.code(i), Some(0));
            assert_eq!(s.count_prefix(i, "Working on"), 0);
            s.expect_prefix(i, "All done: ");
            s.expect_prefix(i, "Time: ");
        }
        s.expect_line(
            0,
            "gha2db.go: Running (1 CPUs): 2015-01-01 16:00:00 +0000 UTC - 2015-01-01 15:00:00 +0000 UTC rust-lang ",
        );
        assert!(s.requests().is_empty());
        assert!(s.data()[0].is_empty());
    });
}

#[test]
fn bad_regexp() {
    let s = check(
        Case::new("badre")
            // Go panics in `regexp.MustCompile` (no `Error:` line, exit 2); the
            // port reports the (differently worded) error through `FatalOnError`.
            .no_errors_compare()
            .args(&["2015-01-01", "15", "2015-01-01", "15", "regexp:("])
            .args(&[
                "2015-01-01",
                "15",
                "2015-01-01",
                "15",
                "rust-lang",
                "regexp:[z-a]",
            ]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        assert!(s.outs[0].stderr_str().contains("regexp: Compile(`(`): "));
        assert_eq!(s.code(1), Some(2));
        assert!(s.outs[1]
            .stderr_str()
            .contains("regexp: Compile(`[z-a]`): "));
        for i in 0..2 {
            s.expect_line(i, "Processing 0 commit roles using 1 CPUs");
        }
        assert!(s.requests().is_empty());
    });
}

#[test]
fn missing_skip_dates_file() {
    let s = check(Case::new("noskipfile").no_skip_dates_file().args(&[
        "2015-01-01",
        "15",
        "2015-01-01",
        "15",
        "rust-lang",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        assert_eq!(
            s.errors(0),
            vec!["Error: 'open ./skip_dates.yaml: no such file or directory'".to_string()]
        );
        assert!(s.requests().is_empty());
    });
}

#[test]
fn skip_dates_file_from_data_dir() {
    // Without `GHA2DB_LOCAL` the file is read from `GHA2DB_DATADIR`.
    let s = check(
        Case::new("skipdatadir")
            .no_skip_dates_file()
            .file(
                "data/skip_dates.yaml",
                b"skip_dates:\n  - 2015-01-01T15:00:00Z\n",
            )
            .env("GHA2DB_LOCAL", "")
            .env("GHA2DB_DATADIR", "data/")
            .fixture(H2015)
            .args(&["2015-01-01", "15", "2015-01-01", "16", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Skipped 2015-01-01 15:00:00 +0000 UTC");
        s.expect_line(0, "Gave up on 2015-01-01 16:00:00 +0000 UTC");
        assert_eq!(s.requests(), vec!["/2015-01-01-16.json.gz".to_string()]);
    });
}

// ---------------------------------------------------------------------------
// New format ingestion
// ---------------------------------------------------------------------------

#[test]
fn rust_lang_2015() {
    let s = check(Case::new("rustlang").fixture(H2015).args(&[
        "2015-01-01",
        "15",
        "2015-01-01",
        "15",
        "rust-lang",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "gha2db.go: Running (1 CPUs): 2015-01-01 15:00:00 +0000 UTC - 2015-01-01 15:00:00 +0000 UTC rust-lang ",
        );
        s.expect_line(0, "Working on 2015-01-01 15:00:00 +0000 UTC");
        s.expect_line(0, "Opened <archive>/2015-01-01-15.json.gz");
        s.expect_line(0, "Decompressed <archive>/2015-01-01-15.json.gz");
        s.expect_line(0, "Split <archive>/2015-01-01-15.json.gz, 146 JSONs");
        s.expect_line(
            0,
            "Parsed: <archive>/2015-01-01-15.json.gz: 145 JSONs, found 18 matching, events 18",
        );
        s.expect_line(0, "All done: <dur>");
        s.expect_line(0, "Processing 0 commit roles using 1 CPUs");
        s.expect_line(0, "Updated 0/0 roles using 1 CPUs");
        s.expect_line(0, "Time: <dur>");
        assert_eq!(s.count("select count(*) from gha_events"), 18);
        assert_eq!(
            s.column("select distinct org_login from gha_repos order by 1"),
            vec!["rust-lang".to_string()]
        );
        assert_eq!(
            s.column("select dt::text from gha_parsed"),
            vec!["2015-01-01 15:00:00".to_string()]
        );
        assert_eq!(s.requests(), vec!["/2015-01-01-15.json.gz".to_string()]);
    });
}

#[test]
fn all_events_2015() {
    let s =
        check(
            Case::new("all2015")
                .fixture(H2015)
                .args(&["2015-01-01", "15", "2015-01-01", "15"]),
        );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Parsed: <archive>/2015-01-01-15.json.gz: 145 JSONs, found 145 matching, events 145",
        );
        assert_eq!(s.count("select count(*) from gha_events"), 145);
        assert!(s.count("select count(*) from gha_payloads") > 100);
        assert!(s.count("select count(*) from gha_actors") > 50);
    });
}

#[test]
fn all_event_types_2025() {
    let s =
        check(
            Case::new("all2025")
                .fixture(H2025)
                .args(&["2025-11-20", "12", "2025-11-20", "12"]),
        );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Parsed: <archive>/2025-11-20-12.json.gz: 191 JSONs, found 191 matching, events 191",
        );
        assert_eq!(s.count("select count(distinct type) from gha_events"), 16);
        assert!(s.count("select count(*) from gha_pull_requests") > 0);
        assert!(s.count("select count(*) from gha_reviews") > 0);
        assert!(s.count("select count(*) from gha_releases") > 0);
        assert!(s.count("select count(*) from gha_pages") > 0);
        // 2025 push payloads no longer carry commits.
        assert_eq!(s.count("select count(*) from gha_commits"), 0);
        assert!(s.count("select count(*) from gha_comments") > 0);
    });
}

#[test]
fn orgs_with_spaces_2020() {
    let s = check(Case::new("orgspaces").fixture(H2020).args(&[
        "2020-05-01",
        "10",
        "2020-05-01",
        "10",
        "dotnet, nextcloud,matrix-org , ",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "gha2db.go: Running (1 CPUs): 2020-05-01 10:00:00 +0000 UTC - 2020-05-01 10:00:00 +0000 UTC +dotnet+matrix-org+nextcloud ",
        );
        s.expect_line(
            0,
            "Parsed: <archive>/2020-05-01-10.json.gz: 210 JSONs, found 136 matching, events 136",
        );
        assert_eq!(
            s.column("select distinct org_login from gha_repos order by 1"),
            vec![
                "dotnet".to_string(),
                "matrix-org".to_string(),
                "nextcloud".to_string()
            ]
        );
    });
}

#[test]
fn repo_filter_2020() {
    let s = check(
        Case::new("repofilter")
            .fixture(H2020)
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "dotnet",
                "roslyn, aspnetcore",
            ])
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "dotnet,nextcloud",
                "server",
            ]),
    );
    both(&s, |s| {
        s.expect_line(
            0,
            "gha2db.go: Running (1 CPUs): 2020-05-01 10:00:00 +0000 UTC - 2020-05-01 10:00:00 +0000 UTC dotnet aspnetcore+roslyn",
        );
        s.expect_line(
            0,
            "Parsed: <archive>/2020-05-01-10.json.gz: 210 JSONs, found 26 matching, events 26",
        );
        s.expect_line(
            1,
            "Parsed: <archive>/2020-05-01-10.json.gz: 210 JSONs, found 13 matching, events 13",
        );
        assert_eq!(
            s.column("select distinct name from gha_repos order by 1"),
            vec![
                "dotnet/aspnetcore".to_string(),
                "dotnet/roslyn".to_string(),
                "nextcloud/server".to_string()
            ]
        );
    });
}

#[test]
fn full_names_2020() {
    let s = check(Case::new("fullnames").fixture(H2020).args(&[
        "2020-05-01",
        "10",
        "2020-05-01",
        "10",
        "dotnet/roslyn,nextcloud/server,aws",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(
            s.count("select count(*) from gha_events"),
            29 + s.count("select count(*) from gha_events where dup_repo_name like 'aws/%'")
        );
        assert_eq!(s.count("select count(*) from gha_events where dup_repo_name in ('dotnet/roslyn', 'nextcloud/server')"), 29);
        assert!(s.count("select count(*) from gha_events where dup_repo_name like 'aws/%'") > 0);
        assert_eq!(s.count("select count(*) from gha_events where dup_repo_name like 'dotnet/%' and dup_repo_name != 'dotnet/roslyn'"), 0);
    });
}

#[test]
fn exact_2020() {
    let s = check(
        Case::new("exact")
            .fixture(H2020)
            .env("GHA2DB_EXACT", "1")
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "dotnet/roslyn,nextcloud",
            ]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Parsed: <archive>/2020-05-01-10.json.gz: 210 JSONs, found 58 matching, events 58",
        );
        // `dotnet/roslyn` is a full name hit, `nextcloud` still matches as an org.
        assert_eq!(
            s.column("select distinct name from gha_repos where org_login = 'dotnet' order by 1"),
            vec!["dotnet/roslyn".to_string()]
        );
        assert_eq!(
            s.count("select count(*) from gha_events where dup_repo_name like 'nextcloud/%'"),
            42
        );
        assert_eq!(s.count("select count(*) from gha_events"), 58);
    });
}

#[test]
fn regexp_filters_2020() {
    let s = check(
        Case::new("regexps")
            .fixture(H2020)
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "regexp:^(dotnet|aws)$",
            ])
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "dotnet",
                "regexp:^(roslyn|aspnetcore)$",
            ])
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "regexp:^dotnet/(roslyn|runtime)$",
            ])
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "regexp:^nextcloud$",
                "regexp:^(server|spreed)$",
            ]),
    );
    both(&s, |s| {
        for i in 0..4 {
            assert_eq!(s.code(i), Some(0));
        }
        s.expect_line(
            0,
            "gha2db.go: Running (1 CPUs): 2020-05-01 10:00:00 +0000 UTC - 2020-05-01 10:00:00 +0000 UTC  ",
        );
        s.expect_line(
            0,
            "Parsed: <archive>/2020-05-01-10.json.gz: 210 JSONs, found 94 matching, events 94",
        );
        s.expect_line(
            1,
            "Parsed: <archive>/2020-05-01-10.json.gz: 210 JSONs, found 26 matching, events 0",
        );
        assert!(
            s.count("select count(*) from gha_events where dup_repo_name = 'dotnet/runtime'") > 0
        );
        assert!(
            s.count("select count(*) from gha_events where dup_repo_name = 'nextcloud/server'") > 0
        );
        assert_eq!(
            s.count("select count(*) from gha_events where dup_repo_name like 'matrix-org/%'"),
            0
        );
    });
}

#[test]
fn exclude_repos_2020() {
    let s = check(
        Case::new("exclude")
            .fixture(H2020)
            .env("GHA2DB_EXCLUDE_REPOS", "dotnet/roslyn,nextcloud/server")
            .args(&["2020-05-01", "10", "2020-05-01", "10", "dotnet,nextcloud"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.count("select count(*) from gha_events where dup_repo_name in ('dotnet/roslyn', 'nextcloud/server')"), 0);
        assert!(
            s.count("select count(*) from gha_events where dup_repo_name = 'dotnet/aspnetcore'")
                > 0
        );
        assert!(
            s.count("select count(*) from gha_events where dup_repo_name like 'nextcloud/%'") > 0
        );
    });
}

#[test]
fn actors_filter_2020() {
    let s = check(
        Case::new("actors")
            .fixture(H2020)
            .env("GHA2DB_ACTORS_FILTER", "1")
            .env("GHA2DB_ACTORS_ALLOW", "^(msftbot\\[bot\\]|rullzer)$")
            .args(&["2020-05-01", "10", "2020-05-01", "10"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Parsed: <archive>/2020-05-01-10.json.gz: 210 JSONs, found 23 matching, events 23",
        );
        assert_eq!(
            s.column("select distinct dup_actor_login from gha_events order by 1"),
            vec!["msftbot[bot]".to_string(), "rullzer".to_string()]
        );
    });
}

#[test]
fn actors_forbid_2020() {
    let s = check(
        Case::new("actorsforbid")
            .fixture(H2020)
            .env("GHA2DB_ACTORS_FILTER", "1")
            .env("GHA2DB_ACTORS_FORBID", "bot")
            .env("GHA2DB_ACTORS_ALLOW", "^[a-z]")
            .args(&["2020-05-01", "10", "2020-05-01", "10"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(
            s.count("select count(*) from gha_events where dup_actor_login like '%bot%'"),
            0
        );
        assert_eq!(
            s.count("select count(*) from gha_events where dup_actor_login !~ '^[a-z]'"),
            0
        );
        assert!(s.count("select count(*) from gha_events") > 100);
    });
}

#[test]
fn actors_filter_disabled_2020() {
    // Without `GHA2DB_ACTORS_FILTER` the allow/forbid regexps are ignored.
    let s = check(
        Case::new("actorsoff")
            .fixture(H2020)
            .env("GHA2DB_ACTORS_ALLOW", "^nobody$")
            .args(&["2020-05-01", "10", "2020-05-01", "10", "matrix-org"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert!(s.count("select count(*) from gha_events") > 10);
    });
}

#[test]
fn hidden_actors_2020() {
    let s = check(
        Case::new("hidden")
            .fixture(H2020)
            .hide(&format!("sha1\n{RULLZER_SHA1}\n{BABOLIVIER_SHA1}\n"))
            .args(&[
                "2020-05-01",
                "10",
                "2020-05-01",
                "10",
                "nextcloud,matrix-org",
            ]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.count("select count(*) from gha_events where dup_actor_login in ('rullzer', 'babolivier')"), 0);
        assert!(
            s.count(&format!(
                "select count(*) from gha_events where dup_actor_login = 'anon-{RULLZER_SHA1}'"
            )) > 0
        );
        assert!(
            s.count(&format!(
                "select count(*) from gha_actors where login = 'anon-{BABOLIVIER_SHA1}'"
            )) > 0
        );
    });
}

#[test]
fn debug_output_2015() {
    let s = check(
        Case::new("debug")
            .fixture(H2015)
            .env("GHA2DB_DEBUG", "1")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "lib.ReadFile('./skip_dates.yaml'): ok");
        assert_eq!(
            s.count_prefix(0, "Processed: '2015-01-01 15:00:00 +0000 UTC' event: "),
            18
        );
        s.expect_line(
            0,
            "Processed: '2015-01-01 15:00:00 +0000 UTC' event: 2489654310",
        );
    });
}

#[test]
fn json_out_2015() {
    let s = check(
        Case::new("jsonout")
            .fixture(H2015)
            .env("GHA2DB_JSON", "1")
            .compare_jsons()
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        let files = s.jsons();
        assert_eq!(files.len(), 18);
        assert!(files.contains_key("1420124400_2489654310.json"));
        assert!(files["1420124400_2489654310.json"].starts_with("{\n  \"actor\": {"));
        assert!(files["1420124400_2489654310.json"].contains("\n  \"id\": \"2489654310\","));
        assert_eq!(s.count("select count(*) from gha_events"), 18);
    });
}

#[test]
fn no_db_2015() {
    let s = check(
        Case::new("nodb")
            .fixture(H2015)
            .env("GHA2DB_JSON", "1")
            .env("GHA2DB_NODB", "1")
            .compare_jsons()
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Parsed: <archive>/2015-01-01-15.json.gz: 145 JSONs, found 18 matching, events 0",
        );
        assert_eq!(s.jsons().len(), 18);
        assert!(s.data()[0].is_empty());
    });
}

#[test]
fn json_out_without_jsons_dir() {
    let s = check(
        Case::new("nojsonsdir")
            .fixture(H2015)
            .env("GHA2DB_JSON", "1")
            .no_jsons_dir()
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        assert_eq!(
            s.errors(0),
            vec![
                "Error: 'open jsons/1420124400_2489654310.json: no such file or directory'"
                    .to_string()
            ]
        );
    });
}

#[test]
fn rerun_2015() {
    let s = check(
        Case::new("rerun")
            .fixture(H2015)
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"])
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang,kaltura"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.code(1), Some(0));
        assert_eq!(s.count_prefix(1, "event id collision"), 0);
        assert!(s.count("select count(*) from gha_events") > 18);
        assert_eq!(s.count("select count(*) from gha_parsed"), 1);
        assert_eq!(
            s.requests(),
            vec![
                "/2015-01-01-15.json.gz".to_string(),
                "/2015-01-01-15.json.gz".to_string()
            ]
        );
    });
}

#[test]
fn new_event_id_collision() {
    // A different event already stored under the same id is reported and the
    // new one skipped.
    let s = check(
        Case::new("collision")
            .fixture(H2015)
            .seed("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values (2489654310, 'WatchEvent', 7, 8, '2015-01-01 15:07:05', null, 'someone', 'some/repo')")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "event id collision: id 2489654310 already exists as (WatchEvent, some/repo, 2015-01-01 15:07:05 +0000 +0000), new event (IssueCommentEvent, rust-lang/cargo, 2015-01-01 15:07:05 +0000 UTC) skipped",
        );
        assert_eq!(
            s.count(
                "select count(*) from gha_events where id = 2489654310 and type = 'WatchEvent'"
            ),
            1
        );
    });
}

// ---------------------------------------------------------------------------
// Old (pre-2015) format
// ---------------------------------------------------------------------------

#[test]
fn old_format_2013() {
    let s = check(
        Case::new("old2013")
            .fixture(H2013)
            .env("GHA2DB_OLDFMT", "1")
            .args(&["2013-06-01", "10", "2013-06-01", "10"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(
            0,
            "Parsed: <archive>/2013-06-01-10.json.gz: 171 JSONs, found ",
        );
        assert!(s.count("select count(*) from gha_events") > 100);
        // Events without a repository (gists, follows) are not repo hits.
        assert_eq!(
            s.count("select count(*) from gha_events where type in ('GistEvent', 'FollowEvent')"),
            0
        );
        // Wall clocks of the `-07:00` stamps are kept (147 at 10:xx, 9 at 09:xx).
        assert_eq!(s.count("select count(*) from gha_events"), 156);
        assert_eq!(s.count("select count(*) from gha_events where created_at::date = '2013-06-01' and extract(hour from created_at) = 10"), 147);
        assert_eq!(s.count("select count(*) from gha_events where created_at::date = '2013-06-01' and extract(hour from created_at) = 9"), 9);
    });
}

#[test]
fn old_format_orgs_2013() {
    let s = check(
        Case::new("old2013orgs")
            .fixture(H2013)
            .env("GHA2DB_OLDFMT", "1")
            .args(&["2013-06-01", "10", "2013-06-01", "10", "mozilla,adobe"])
            .args(&[
                "2013-06-01",
                "10",
                "2013-06-01",
                "10",
                "mozilla",
                "regexp:^(rust|servo)$",
            ]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(
            0,
            "Parsed: <archive>/2013-06-01-10.json.gz: 171 JSONs, found ",
        );
        assert_eq!(
            s.column("select distinct org_login from gha_repos order by 1"),
            vec!["adobe".to_string(), "mozilla".to_string()]
        );
    });
}

#[test]
fn old_format_2014_rerun() {
    // Bug 41: re-running old-format hours must not report id collisions
    // (the -08:00 wall clocks are stored in a zone-less column).
    let s = check(
        Case::new("old2014")
            .fixture(H2014)
            .env("GHA2DB_OLDFMT", "1")
            .args(&["2014-12-31", "23", "2014-12-31", "23"])
            .args(&["2014-12-31", "23", "2014-12-31", "23"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.code(1), Some(0));
        s.expect_prefix(
            0,
            "Parsed: <archive>/2014-12-31-23.json.gz: 116 JSONs, found ",
        );
        assert_eq!(s.count_prefix(0, "event id collision"), 0);
        assert_eq!(s.count_prefix(1, "event id collision"), 0);
        assert!(s.count("select count(*) from gha_events") > 100);
        assert_eq!(s.count("select count(*) from gha_parsed"), 1);
    });
}

#[test]
fn old_format_collision() {
    let eid = hash_strings(&["PushEvent", "fujimura", "hi", "2014-12-31 23:08:06"]);
    let s = check(
        Case::new("oldcollision")
            .fixture(H2014)
            .env("GHA2DB_OLDFMT", "1")
            .seed(&format!("insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values ({eid}, 'WatchEvent', 7, 8, '2014-12-31 23:08:06', null, 'someone', 'some/repo')"))
            .args(&["2014-12-31", "23", "2014-12-31", "23", "fujimura"])
            .args(&["2014-12-31", "23", "2014-12-31", "23", "", "hi"]),
    );
    both(&s, |s| {
        // Old-format repos without an organization have no org part, so the
        // `fujimura` org filter hits nothing…
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Parsed: <archive>/2014-12-31-23.json.gz: 116 JSONs, found 0 matching, events 0",
        );
        // …while the `hi` repo filter does, and the seeded id collides.
        assert_eq!(s.code(1), Some(0));
        s.expect_line(
            1,
            &format!("event id collision: id {eid} already exists as (WatchEvent, some/repo, 2014-12-31 23:08:06 +0000 +0000), new event (PushEvent, hi, 2014-12-31 23:08:06 -0800 -0800) skipped"),
        );
        assert_eq!(s.count_prefix(1, "event id collision"), 1);
        // 12 hits: 1 collision + 2 identical `CreateEvent`s sharing an id.
        s.expect_line(
            1,
            "Parsed: <archive>/2014-12-31-23.json.gz: 116 JSONs, found 12 matching, events 10",
        );
        assert_eq!(
            s.count("select count(*) from gha_events where type = 'WatchEvent'"),
            1
        );
        assert_eq!(s.count("select count(*) from gha_events"), 11);
    });
}

#[test]
fn new_format_on_old_json() {
    // Decoding old-format JSONs as new-format ones fails (`actor` is a
    // string); with `GHA2DB_ALLOW_BROKEN_JSON` every JSON is dumped to
    // `jsons/error_*.json` and skipped.
    let s = check(
        Case::new("newonold")
            .fixture(H2014)
            .env("GHA2DB_ALLOW_BROKEN_JSON", "1")
            .loose("Error(2014-12-31-23): ")
            .compare_jsons()
            .args(&["2014-12-31", "23", "2014-12-31", "23"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Parsed: <archive>/2014-12-31-23.json.gz: 116 JSONs, found 0 matching, events 0",
        );
        assert_eq!(s.count_prefix(0, "Error(2014-12-31-23): "), 116);
        let files = s.jsons();
        assert_eq!(files.len(), 116);
        assert!(files.contains_key("error_2014-12-31-23-1-117.json"));
        assert_eq!(s.count("select count(*) from gha_events"), 0);
        assert_eq!(s.count("select count(*) from gha_parsed"), 1);
    });
}

// ---------------------------------------------------------------------------
// Broken JSONs, empty and missing hours, transport errors
// ---------------------------------------------------------------------------

#[test]
fn broken_json_fatal() {
    let s = check(
        Case::new("brokenfatal")
            .fixture(H_BROKEN)
            .loose("Error(2012-03-11-12): ")
            .loose("Error: '")
            .args(&["2012-03-11", "12", "2012-03-11", "12"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_line(
            0,
            "2012-03-11 12:00:00 +0000 UTC: JSON Unmarshal failed for:",
        );
        assert_eq!(s.errors(0).len(), 1);
        // The pretty-printed JSON is echoed before failing.
        assert!(s.outs[0]
            .stdout_str()
            .contains("\n  \"created_at\": \"2012/03/11 12:00:00 -0700\","));
        assert!(s.outs[0]
            .stdout_str()
            .contains("\n  \"actor\": \"izzm\",\n"));
        assert_eq!(
            s.jsons().keys().collect::<Vec<_>>(),
            vec!["error_2012-03-11-12-1-4.json"]
        );
        assert_eq!(s.count("select count(*) from gha_parsed"), 0);
    });
}

#[test]
fn broken_json_allowed() {
    let s = check(
        Case::new("brokenok")
            .fixture(H_BROKEN)
            .env("GHA2DB_ALLOW_BROKEN_JSON", "1")
            .loose("Error(2012-03-11-12): ")
            .compare_jsons()
            .args(&["2012-03-11", "12", "2012-03-11", "12"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Split <archive>/2012-03-11-12.json.gz, 4 JSONs");
        s.expect_line(
            0,
            "Parsed: <archive>/2012-03-11-12.json.gz: 3 JSONs, found 0 matching, events 0",
        );
        assert_eq!(s.count_prefix(0, "Error(2012-03-11-12): "), 3);
        assert_eq!(
            s.jsons().keys().cloned().collect::<Vec<_>>(),
            vec![
                "error_2012-03-11-12-1-4.json".to_string(),
                "error_2012-03-11-12-2-4.json".to_string(),
                "error_2012-03-11-12-3-4.json".to_string()
            ]
        );
        assert_eq!(s.count("select count(*) from gha_parsed"), 1);
    });
}

#[test]
fn empty_hour() {
    let s = check(Case::new("emptyhour").fixture(H_EMPTY).args(&[
        "2012-03-10",
        "15",
        "2012-03-10",
        "15",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Decompressed <archive>/2012-03-10-15.json.gz");
        s.expect_line(0, "Split <archive>/2012-03-10-15.json.gz, 1 JSONs");
        s.expect_line(
            0,
            "Parsed: <archive>/2012-03-10-15.json.gz: 0 JSONs, found 0 matching, events 0",
        );
        assert_eq!(
            s.column("select dt::text from gha_parsed"),
            vec!["2012-03-10 15:00:00".to_string()]
        );
    });
}

#[test]
fn hour_not_found() {
    let s = check(
        Case::new("notfound")
            .hour(
                "2015-01-01-16",
                vec![Archive::not_found("2015-01-01-16.json.gz")],
            )
            .args(&["2015-01-01", "16", "2015-01-01", "16", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "2015-01-01 16:00:00 +0000 UTC: No data yet, gzip reader:",
        );
        s.expect_line(0, "gzip: invalid header");
        s.expect_line(0, "Gave up on 2015-01-01 16:00:00 +0000 UTC");
        assert_eq!(s.count_prefix(0, "Opened"), 0);
        assert!(s.data()[0].is_empty());
        assert_eq!(s.requests(), vec!["/2015-01-01-16.json.gz".to_string()]);
    });
}

#[test]
fn hour_empty_body() {
    let s = check(
        Case::new("emptybody")
            .hour("2015-01-01-16", vec![Archive::empty()])
            .hour(
                "2015-01-01-17",
                vec![Archive::raw(
                    200,
                    "application/x-gzip",
                    vec![0x1f, 0x8b, 0x08],
                )],
            )
            .args(&["2015-01-01", "16", "2015-01-01", "17", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "2015-01-01 16:00:00 +0000 UTC: No data yet, gzip reader:",
        );
        s.expect_line(0, "EOF");
        s.expect_line(
            0,
            "2015-01-01 17:00:00 +0000 UTC: No data yet, gzip reader:",
        );
        s.expect_line(0, "unexpected EOF");
        assert_eq!(s.count_prefix(0, "Gave up on"), 2);
    });
}

#[test]
fn hour_truncated() {
    let lines = fixture_lines(H2015);
    let s = check(
        Case::new("truncated")
            .hour("2015-01-01-15", vec![Archive::truncated(&lines, 20000)])
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Opened <archive>/2015-01-01-15.json.gz");
        s.expect_line(
            0,
            "2015-01-01 15:00:00 +0000 UTC: Error (no data yet, ioutil readall):",
        );
        s.expect_line(0, "unexpected EOF");
        s.expect_line(0, "Gave up on 2015-01-01 15:00:00 +0000 UTC");
        assert_eq!(s.count_prefix(0, "Decompressed"), 0);
        assert!(s.data()[0].is_empty());
    });
}

#[test]
fn hour_corrupted() {
    let mut body = fs::read(fixture(&format!("gha2db/{H2015}.json.gz"))).unwrap();
    // Flip bits in the middle of the deflate stream.
    for b in body.iter_mut().skip(5000).take(64) {
        *b ^= 0x55;
    }
    let s = check(
        Case::new("corrupted")
            .hour("2015-01-01-15", vec![Archive::gz(body)])
            .loose("flate: ")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "2015-01-01 15:00:00 +0000 UTC: Error (no data yet, ioutil readall):",
        );
        s.expect_line(0, "Gave up on 2015-01-01 15:00:00 +0000 UTC");
        assert!(s.data()[0].is_empty());
    });
}

#[test]
fn retry_recovers() {
    let s = check(
        Case::new("retryok")
            .hour(
                H2015,
                vec![
                    Archive::not_found("2015-01-01-15.json.gz"),
                    fixture_hour(H2015),
                ],
            )
            .env("GHA2DB_HTTP_RETRY", "2")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "2015-01-01 15:00:00 +0000 UTC: No data yet, gzip reader:",
        );
        s.expect_line(0, "Retry(2) 2015-01-01 15:00:00 +0000 UTC");
        s.expect_line(
            0,
            "Recovered(2) & decompressed <archive>/2015-01-01-15.json.gz",
        );
        assert_eq!(s.count_prefix(0, "Gave up"), 0);
        assert_eq!(s.count("select count(*) from gha_events"), 18);
        assert_eq!(
            s.requests(),
            vec![
                "/2015-01-01-15.json.gz".to_string(),
                "/2015-01-01-15.json.gz".to_string()
            ]
        );
    });
}

#[test]
fn retry_gives_up() {
    let lines = fixture_lines(H2015);
    let s = check(
        Case::new("retryfail")
            .hour(
                H2015,
                vec![
                    Archive::not_found("2015-01-01-15.json.gz"),
                    Archive::truncated(&lines, 10000),
                    Archive::truncated(&lines, 10000),
                ],
            )
            .env("GHA2DB_HTTP_RETRY", "3")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Retry(2) 2015-01-01 15:00:00 +0000 UTC");
        s.expect_line(0, "Retry(3) 2015-01-01 15:00:00 +0000 UTC");
        assert_eq!(
            s.count_prefix(
                0,
                "2015-01-01 15:00:00 +0000 UTC: No data yet, gzip reader:"
            ),
            1
        );
        assert_eq!(
            s.count_prefix(
                0,
                "2015-01-01 15:00:00 +0000 UTC: Error (no data yet, ioutil readall):"
            ),
            2
        );
        s.expect_line(0, "Gave up on 2015-01-01 15:00:00 +0000 UTC");
        assert_eq!(s.requests().len(), 3);
        assert!(s.data()[0].is_empty());
    });
}

#[test]
fn dead_archive() {
    let s = check(
        Case::new("deadarchive")
            .env("GHA2DB_GHARCHIVE_URL", "http://127.0.0.1:1/")
            .loose("Get \"http://127.0.0.1:1/2015-01-01-15.json.gz\": ")
            .loose("Error: 'Get \"http://127.0.0.1:1/2015-01-01-15.json.gz\": ")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_line(0, "2015-01-01 15:00:00 +0000 UTC: Error http.Get:");
        assert_eq!(s.errors(0).len(), 1);
        assert!(s.outs[0]
            .stderr_str()
            .contains("2015-01-01 15:00:00 +0000 UTC: Error http.Get:"));
        assert!(s.requests().is_empty());
    });
}

#[test]
fn archive_hangup() {
    let s = check(
        Case::new("hangup")
            .hour(H2015, vec![Archive::hangup()])
            .loose("Get \"<archive>/2015-01-01-15.json.gz\": ")
            .loose("Error: 'Get \"<archive>/2015-01-01-15.json.gz\": ")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_line(0, "2015-01-01 15:00:00 +0000 UTC: Error http.Get:");
        assert_eq!(s.errors(0).len(), 1);
        assert_eq!(s.requests(), vec!["/2015-01-01-15.json.gz".to_string()]);
    });
}

#[test]
fn archive_hangup_retry() {
    let s = check(
        Case::new("hangupretry")
            .hour(H2015, vec![Archive::hangup(), fixture_hour(H2015)])
            .env("GHA2DB_HTTP_RETRY", "2")
            .loose("Get \"<archive>/2015-01-01-15.json.gz\": ")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "2015-01-01 15:00:00 +0000 UTC: Error http.Get:");
        s.expect_line(0, "Retry(2) 2015-01-01 15:00:00 +0000 UTC");
        s.expect_line(
            0,
            "Recovered(2) & decompressed <archive>/2015-01-01-15.json.gz",
        );
        assert_eq!(s.count("select count(*) from gha_events"), 18);
    });
}

// ---------------------------------------------------------------------------
// Ranges, skipping, threads, today/now, time zones
// ---------------------------------------------------------------------------

#[test]
fn skip_dates() {
    let s = check(
        Case::new("skipdates")
            .skip_dates("skip_dates:\n  - 2015-01-01T15:00:00Z\n  - 2015-01-01T17:30:00Z\n  - 2016-01-01T00:00:00Z\n")
            .fixture(H2015)
            .args(&["2015-01-01", "14", "2015-01-01", "17", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Working on 2015-01-01 15:00:00 +0000 UTC");
        s.expect_line(0, "Skipped 2015-01-01 15:00:00 +0000 UTC");
        s.expect_line(0, "Skipped 2015-01-01 17:00:00 +0000 UTC");
        s.expect_line(0, "Gave up on 2015-01-01 14:00:00 +0000 UTC");
        s.expect_line(0, "Gave up on 2015-01-01 16:00:00 +0000 UTC");
        assert_eq!(
            s.requests(),
            vec![
                "/2015-01-01-14.json.gz".to_string(),
                "/2015-01-01-16.json.gz".to_string()
            ]
        );
        // Skipped hours are marked as parsed, nothing else is written.
        assert_eq!(
            s.column("select dt::text from gha_parsed order by dt"),
            vec![
                "2015-01-01 15:00:00".to_string(),
                "2015-01-01 17:00:00".to_string()
            ]
        );
        assert_eq!(s.count("select count(*) from gha_events"), 0);
    });
}

#[test]
fn skip_dates_bad_yaml() {
    let s = check(
        Case::new("skipbadyaml")
            .skip_dates("skip_dates: [\n")
            .loose("Error: '")
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(2));
        assert_eq!(s.errors(0).len(), 1);
        assert!(s.errors(0)[0].starts_with("Error: 'yaml: "));
        assert!(s.requests().is_empty());
    });
}

#[test]
fn range_single_threaded() {
    let s = check(Case::new("rangest").fixture(H2020).args(&[
        "2020-05-01",
        "9",
        "2020-05-01",
        "11",
        "dotnet",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Using single threaded version");
        s.expect_line(
            0,
            "gha2db.go: Running (1 CPUs): 2020-05-01 09:00:00 +0000 UTC - 2020-05-01 11:00:00 +0000 UTC dotnet ",
        );
        assert_eq!(s.count_prefix(0, "Working on"), 3);
        assert_eq!(s.count_prefix(0, "Gave up on"), 2);
        assert_eq!(
            s.requests(),
            vec![
                "/2020-05-01-10.json.gz".to_string(),
                "/2020-05-01-11.json.gz".to_string(),
                "/2020-05-01-9.json.gz".to_string()
            ]
        );
        // Hours given up on are not marked as parsed.
        assert_eq!(
            s.column("select dt::text from gha_parsed order by dt"),
            vec!["2020-05-01 10:00:00".to_string()]
        );
    });
}

#[test]
fn range_multi_threaded() {
    let s = check(
        Case::new("rangemt")
            .mt()
            .env("GHA2DB_NCPUS", "4")
            .fixture(H2020)
            .fixture(H2015)
            .args(&["2020-05-01", "8", "2020-05-01", "12", "dotnet,nextcloud"])
            .args(&["2015-01-01", "13", "2015-01-01", "16", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.code(1), Some(0));
        s.expect_line(
            0,
            "gha2db.go: Running (4 CPUs): 2020-05-01 08:00:00 +0000 UTC - 2020-05-01 12:00:00 +0000 UTC dotnet+nextcloud ",
        );
        s.expect_line(0, "Final threads join (processed 2)");
        assert_eq!(s.count_prefix(0, "Working on"), 5);
        assert!(s.count_prefix(0, "1 remain: <hours>") >= 1);
        s.expect_line(1, "Final threads join (processed 1)");
        assert_eq!(s.count_prefix(1, "Working on"), 4);
        assert_eq!(s.requests().len(), 9);
        // Only the hours the archive served are marked as parsed.
        assert_eq!(
            s.column("select dt::text from gha_parsed order by dt"),
            vec![
                "2015-01-01 15:00:00".to_string(),
                "2020-05-01 10:00:00".to_string()
            ]
        );
        assert_eq!(
            s.count("select count(*) from gha_events where dup_repo_name like 'rust-lang/%'"),
            18
        );
    });
}

#[test]
fn single_hour_multi_threaded() {
    let s = check(
        Case::new("mt1")
            .mt()
            .env("GHA2DB_NCPUS", "2")
            .fixture(H2015)
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Final threads join (processed 0)");
        s.expect_line(0, "1 remain: <hours>");
        assert_eq!(s.count("select count(*) from gha_events"), 18);
    });
}

#[test]
fn gc_stats_single_threaded() {
    // Every 24 hours the tools print GC statistics (masked: Go's heap
    // numbers are not reproducible).
    let s = check(
        Case::new("gcst")
            .fixture(H2020)
            .args(&["2020-05-01", "0", "2020-05-02", "1"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.count_prefix(0, "Working on"), 26);
        assert_eq!(s.count_prefix(0, "Gave up on"), 25);
        assert_eq!(s.count_prefix(0, "<gc>"), 2);
        assert_eq!(s.requests().len(), 26);
        assert_eq!(s.count("select count(*) from gha_events"), 210);
    });
}

#[test]
fn gc_stats_multi_threaded() {
    let s = check(
        Case::new("gcmt")
            .mt()
            .env("GHA2DB_NCPUS", "3")
            .fixture(H2020)
            .args(&["2020-05-01", "0", "2020-05-02", "1", "aws"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.count_prefix(0, "Working on"), 26);
        s.expect_line(0, "Final threads join (processed 24)");
        assert_eq!(s.count_prefix(0, "<gc>"), 2);
        // The loop never leaves more than `threads - 1` hours in flight.
        assert_eq!(s.count_prefix(0, "3 remain: <hours>"), 0);
        assert_eq!(s.count_prefix(0, "2 remain: <hours>"), 1);
        assert_eq!(s.count_prefix(0, "1 remain: <hours>"), 1);
        assert_eq!(s.requests().len(), 26);
    });
}

#[test]
fn today_now() {
    let s = check(
        Case::new("todaynow")
            .args(&["today", "now", "today", "now", "rust-lang"])
            .args(&["TODAY", "NOW", "today", "23"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "gha2db.go: Running (1 CPUs): <now> - <now> rust-lang ");
        s.expect_line(0, "Working on <now>");
        s.expect_line(0, "Gave up on <now>");
        assert_eq!(s.count_prefix(0, "Working on"), 1);
        assert_eq!(s.code(1), Some(0));
        assert!(s.count_prefix(1, "Working on") >= 1);
        assert!(s.requests().iter().all(|r| r == "/<now>.json.gz"));
    });
}

#[test]
fn time_zone_warsaw() {
    // Under a non-UTC zone Go keeps the parsed `+00:00` as a nameless zone.
    let s = check(
        Case::new("tzwarsaw")
            .tz("Europe/Warsaw")
            .fixture(H2015)
            .args(&["2015-01-01", "15", "2015-01-01", "15", "rust-lang"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "gha2db.go: Running (1 CPUs): 2015-01-01 15:00:00 +0000 +0000 - 2015-01-01 15:00:00 +0000 +0000 rust-lang ",
        );
        s.expect_line(0, "Working on 2015-01-01 15:00:00 +0000 +0000");
        assert_eq!(s.count("select count(*) from gha_events"), 18);
        assert_eq!(
            s.column("select dt::text from gha_parsed"),
            vec!["2015-01-01 15:00:00".to_string()]
        );
    });
}

#[test]
fn time_zone_warsaw_today() {
    let s = check(Case::new("tzwarsawtoday").tz("Europe/Warsaw").args(&[
        "today",
        "0",
        "today",
        "0",
        "rust-lang",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Working on <now>");
        assert_eq!(s.requests().len(), 1);
    });
}

// ---------------------------------------------------------------------------
// Commit roles, affiliations database
// ---------------------------------------------------------------------------

#[test]
fn commit_roles_refresh() {
    let s = check(
        Case::new("roles")
            .fixture(H2020)
            .env("GHA2DB_REFRESH_COMMIT_ROLES", "1")
            .seed(ROLES_SEED)
            .args(&["2020-05-01", "10", "2020-05-01", "10", "nextcloud,dotnet"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(0, "Processing ");
        s.expect_prefix(0, "Processed ");
        s.expect_prefix(0, "Now updating/inserting ");
        assert_eq!(s.count_prefix(0, "Processing 0 commit roles"), 0);
        assert!(s.count("select count(*) from gha_commits_roles") >= 8);
        assert!(s.count("select count(*) from gha_commits_roles where actor_id = 1001 and actor_login = 'rullzer'") > 0);
        assert!(s.count("select count(*) from gha_commits_roles where actor_id = 1002 and actor_login = 'mario'") > 0);
        assert!(s.count("select count(*) from gha_commits_roles where actor_id = 1003 and actor_login = 'nickvergessen'") > 0);
        assert!(
            s.count(
                "select count(*) from gha_commits_roles where actor_id = 0 and actor_login = ''"
            ) > 0
        );
        assert_eq!(
            s.column("select distinct role from gha_commits_roles order by 1"),
            vec!["Signed-off-by".to_string()]
        );
    });
}

#[test]
fn commit_roles_refresh_hidden() {
    let s = check(
        Case::new("roleshidden")
            .fixture(H2020)
            .env("GHA2DB_REFRESH_COMMIT_ROLES", "1")
            .seed(ROLES_SEED)
            .seed(&format!("insert into gha_actors_emails(actor_id, email) values (1001, 'anon-{USER126_EMAIL_SHA1}')"))
            .hide(&format!("sha1\n{USER126_EMAIL_SHA1}\n{ROELAND_NAME_SHA1}\n{RULLZER_SHA1}\n"))
            .args(&["2020-05-01", "10", "2020-05-01", "10", "nextcloud"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(
            s.count(
                "select count(*) from gha_commits_roles where actor_email = 'user126@example.com'"
            ),
            0
        );
        assert!(s.count(&format!("select count(*) from gha_commits_roles where actor_email = 'anon-{USER126_EMAIL_SHA1}' and actor_name = 'anon-{ROELAND_NAME_SHA1}' and actor_login = 'anon-{RULLZER_SHA1}' and actor_id = 1001")) > 0);
    });
}

#[test]
fn commit_roles_refresh_empty() {
    let s = check(
        Case::new("rolesempty")
            .env("GHA2DB_REFRESH_COMMIT_ROLES", "1")
            .args(&["2015-01-02", "0", "2015-01-01", "23"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(0, "Processed 0 commits with at least 1 commit role");
        s.expect_line(0, "Now updating/inserting 0 commit roles");
        assert!(s.data()[0].is_empty());
    });
}

#[test]
fn commit_roles_update() {
    let s = check(
        Case::new("rolesupdate")
            .seed(ROLES_SEED)
            .seed("insert into gha_commits_roles(sha, event_id, role, actor_id, actor_login, actor_name, actor_email, dup_repo_id, dup_repo_name, dup_created_at) values
                ('aaaa', 1, 'Signed-off-by', 0, '', 'Roeland Jago Douma', 'user126@example.com', 10, 'nextcloud/server', '2020-05-01 10:00:00'),
                ('bbbb', 2, 'Co-authored-by', null, '', 'Mario Danic', 'nobody@example.com', 10, 'nextcloud/server', '2020-05-01 10:00:00'),
                ('cccc', 3, 'Reviewed-by', 0, '', 'Joas Schilling', 'other@example.com', 10, 'nextcloud/server', '2020-05-01 10:00:00'),
                ('dddd', 4, 'Signed-off-by', 0, '', 'Unknown Person', 'unknown@example.com', 10, 'nextcloud/server', '2020-05-01 10:00:00'),
                ('eeee', 5, 'Signed-off-by', 42, 'known', 'Known Person', 'known@example.com', 10, 'nextcloud/server', '2020-05-01 10:00:00')")
            .args(&["2015-01-02", "0", "2015-01-01", "23"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Processing 4 commit roles using 1 CPUs");
        s.expect_line(0, "Updated 3/4 roles using 1 CPUs");
        assert_eq!(
            s.column("select actor_id::text || ':' || coalesce(actor_login, '<null>') from gha_commits_roles order by sha"),
            vec![
                "1001:rullzer".to_string(),
                "1002:mario".to_string(),
                "1003:nickvergessen".to_string(),
                "0:".to_string(),
                "42:known".to_string()
            ]
        );
    });
}

#[test]
fn commit_roles_update_hidden() {
    let s = check(
        Case::new("rolesupdatehidden")
            .seed(ROLES_SEED)
            .seed(&format!("insert into gha_actors_emails(actor_id, email) values (1001, 'anon-{USER126_EMAIL_SHA1}')"))
            .seed(&format!("insert into gha_commits_roles(sha, event_id, role, actor_id, actor_login, actor_name, actor_email, dup_repo_id, dup_repo_name, dup_created_at) values ('aaaa', 1, 'Signed-off-by', 0, '', 'anon-{ROELAND_NAME_SHA1}', 'anon-{USER126_EMAIL_SHA1}', 10, 'nextcloud/server', '2020-05-01 10:00:00')"))
            .hide(&format!("sha1\n{USER126_EMAIL_SHA1}\n{ROELAND_NAME_SHA1}\n{RULLZER_SHA1}\n"))
            .args(&["2015-01-02", "0", "2015-01-01", "23"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Updated 1/1 roles using 1 CPUs");
        assert_eq!(
            s.column("select actor_id::text || ':' || actor_login from gha_commits_roles"),
            vec![format!("1001:anon-{RULLZER_SHA1}")]
        );
    });
}

#[test]
fn affiliations_db() {
    let s = check(Case::new("affs").affs().fixture(H2015).args(&[
        "2015-01-01",
        "15",
        "2015-01-01",
        "15",
        "rust-lang",
    ]));
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.count("select count(*) from gha_events"), 18);
        assert_eq!(s.count("select count(*) from gha_actors"), 0);
        let a = s.affs.as_ref().unwrap();
        let con = a.conn();
        let n = cpg::snapshot(&con, "select count(*) from gha_actors", &[]);
        con.close();
        assert!(n.rows[0][0].parse::<i64>().unwrap() > 5);
    });
}

#[test]
fn affiliations_db_commit_roles() {
    let s = check(
        Case::new("affsroles")
            .affs()
            .seed(ROLES_SEED)
            .affs_seed(ROLES_SEED)
            .fixture(H2020)
            .env("GHA2DB_REFRESH_COMMIT_ROLES", "1")
            .args(&["2020-05-01", "10", "2020-05-01", "10", "nextcloud"]),
    );
    both(&s, |s| {
        assert_eq!(s.code(0), Some(0));
        assert!(s.count("select count(*) from gha_commits_roles") >= 8);
        assert!(s.count("select count(*) from gha_commits_roles where actor_id = 1001 and actor_login = 'rullzer'") > 0);
    });
}
