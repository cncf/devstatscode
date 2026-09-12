//! Go ⇄ Rust compatibility tests for `sqlitedb`.
//!
//! Every case gets one scratch directory per side (`go`/`rs`) holding a Grafana
//! SQLite database built from `compat/fixtures/sqlitedb/schema.sql` (seeded
//! with the real Prometheus dashboards of `fixtures/sqlitedb/dashboards/`) and
//! the JSON files to import. Compared afterwards:
//!
//! * exit code,
//! * stdout — exactly (durations, backup names and `time.Now()` values
//!   masked) where the Go tool's output order is deterministic, as a sorted
//!   multiset of lines where Go iterates a map (`jsonMap` — the order of the
//!   `… updated dashboard …` lines; `allMap` in `updateTags` — the debug
//!   `inserted`/`deleted` tag lines),
//! * the `Error: '…'` lines of fatal errors (Go's `ErrorType:` lines and stack
//!   traces are not reproduced),
//! * the database afterwards: every `dashboard` row (all columns except the
//!   `created`/`updated` timestamps, whose go-sqlite3 format is checked
//!   separately) and the `(dashboard_id, term)` tag set (tag `id`s depend on
//!   Go's random map order),
//! * the side files: exported `sqlite/<slug>.json`, `<json>.was` snapshots and
//!   the `<db>.<UnixNano>` backup (which must equal the original database).
//!
//! The Go binary needs cgo (`mattn/go-sqlite3`), so a C compiler is required
//! to build it; `DEVSTATS_SKIP_GO_COMPAT=1` skips the Go side.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};
use rusqlite::Connection;
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("sqlitedb")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_sqlitedb"))
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The four real dashboards: (file, uid, title, tags).
const DASHBOARDS: &[(&str, &str, &str, &[&str])] = &[
    (
        "companies-table.json",
        "5",
        "Companies Table",
        &["dashboard", "prometheus", "companies", "table"],
    ),
    (
        "new-contributors-table.json",
        "52",
        "New Contributors Table",
        &["dashboard", "prometheus", "table"],
    ),
    (
        "pr-comments.json",
        "17",
        "PR Comments",
        &["dashboard", "prometheus"],
    ),
    (
        "repository-groups.json",
        "68",
        "Repository groups",
        &["dashboard", "prometheus", "table"],
    ),
];

fn dashboard_json(file: &str) -> serde_json::Value {
    let p = fixture(&format!("sqlitedb/dashboards/{file}"));
    let bytes = fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
    serde_json::from_slice(&bytes).unwrap()
}

fn schema_sql() -> String {
    fs::read_to_string(fixture("sqlitedb/schema.sql")).unwrap()
}

/// Go `lib.Slugify` (runs of non-word characters → `-`, lowercased).
fn slugify(s: &str) -> String {
    devstatscode::string::slugify(s)
}

/// A dashboard row to seed: Grafana stores compact JSON, tags in `dashboard_tag`.
struct Seed {
    uid: String,
    title: String,
    slug: Option<String>,
    data: String,
    tags: Vec<String>,
}

impl Seed {
    fn from_fixture(file: &str) -> Seed {
        let v = dashboard_json(file);
        Seed {
            uid: v["uid"].as_str().unwrap().to_string(),
            title: v["title"].as_str().unwrap().to_string(),
            slug: None,
            data: serde_json::to_string(&v).unwrap(),
            tags: v["tags"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t.as_str().unwrap().to_string())
                .collect(),
        }
    }

    fn all_fixtures() -> Vec<Seed> {
        DASHBOARDS.iter().map(|d| Seed::from_fixture(d.0)).collect()
    }
}

/// Create a Grafana database with the fixture schema and the given rows.
fn create_db(path: &Path, seeds: &[Seed]) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(&schema_sql()).unwrap();
    for s in seeds {
        let slug = s.slug.clone().unwrap_or_else(|| slugify(&s.title));
        conn.execute(
            "insert into dashboard(version, slug, title, data, org_id, created, updated, \
             created_by, updated_by, gnet_id, plugin_id, folder_id, is_folder, has_acl, uid) \
             values(3, ?, ?, ?, 1, '2026-01-02 03:04:05+00:00', '2026-01-02 03:04:05+00:00', \
             1, 1, 0, '', 0, 0, 0, ?)",
            rusqlite::params![slug, s.title, s.data, s.uid],
        )
        .unwrap();
        let id = conn.last_insert_rowid();
        for t in &s.tags {
            conn.execute(
                "insert into dashboard_tag(dashboard_id, term) values(?, ?)",
                rusqlite::params![id, t],
            )
            .unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Database state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct DashRow {
    id: i64,
    version: i64,
    slug: String,
    title: String,
    data: String,
    org_id: i64,
    created_by: Option<i64>,
    updated_by: Option<i64>,
    gnet_id: Option<i64>,
    plugin_id: Option<String>,
    folder_id: i64,
    is_folder: i64,
    has_acl: i64,
    uid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbState {
    dashboards: Vec<DashRow>,
    tags: BTreeSet<(i64, String)>,
    /// `uid -> (created, updated)`
    times: BTreeMap<String, (String, String)>,
}

fn db_state(path: &Path) -> DbState {
    let conn = Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare(
            "select id, version, slug, title, data, org_id, created_by, updated_by, gnet_id, \
             plugin_id, folder_id, is_folder, has_acl, uid, created, updated from dashboard order by id",
        )
        .unwrap();
    let mut dashboards = Vec::new();
    let mut times = BTreeMap::new();
    let rows = stmt
        .query_map([], |r| {
            Ok((
                DashRow {
                    id: r.get(0)?,
                    version: r.get(1)?,
                    slug: r.get(2)?,
                    title: r.get(3)?,
                    data: r.get(4)?,
                    org_id: r.get(5)?,
                    created_by: r.get(6)?,
                    updated_by: r.get(7)?,
                    gnet_id: r.get(8)?,
                    plugin_id: r.get(9)?,
                    folder_id: r.get(10)?,
                    is_folder: r.get(11)?,
                    has_acl: r.get(12)?,
                    uid: r.get(13)?,
                },
                r.get::<_, String>(14)?,
                r.get::<_, String>(15)?,
            ))
        })
        .unwrap();
    for row in rows {
        let (d, created, updated) = row.unwrap();
        times.insert(d.uid.clone().unwrap_or_default(), (created, updated));
        dashboards.push(d);
    }
    let mut stmt = conn
        .prepare("select dashboard_id, term from dashboard_tag")
        .unwrap();
    let tags = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    DbState {
        dashboards,
        tags,
        times,
    }
}

/// go-sqlite3 binds `time.Time` as `2006-01-02 15:04:05.999999999-07:00`.
fn is_go_sqlite3_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 25 {
        return false;
    }
    let date_ok = b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b' '
        && b[13] == b':'
        && b[16] == b':'
        && b[..19]
            .iter()
            .enumerate()
            .all(|(i, c)| matches!(i, 4 | 7 | 10 | 13 | 16) || c.is_ascii_digit());
    if !date_ok {
        return false;
    }
    let rest = &s[19..];
    let (frac, zone) = match rest.strip_prefix('.') {
        Some(r) => {
            let n = r.bytes().take_while(|c| c.is_ascii_digit()).count();
            (n, &r[n..])
        }
        None => (0, rest),
    };
    if rest.starts_with('.') && !(1..=9).contains(&frac) {
        return false;
    }
    let zb = zone.as_bytes();
    zb.len() == 6
        && (zb[0] == b'+' || zb[0] == b'-')
        && zb[3] == b':'
        && zb[1].is_ascii_digit()
        && zb[2].is_ascii_digit()
        && zb[4].is_ascii_digit()
        && zb[5].is_ascii_digit()
}

// ---------------------------------------------------------------------------
// Output normalisation
// ---------------------------------------------------------------------------

/// Mask everything run-time dependent: durations (`Time: …`), the backup name
/// `<db>.<UnixNano>`, `time.Now()` values echoed by `GHA2DB_QOUT` and the
/// program name in the log context.
fn mask(s: &str, side: &Side) -> String {
    let dur = mask_go_durations(s);
    let mut out = Vec::new();
    for line in dur.lines() {
        let mut l = line.to_string();
        if let Some(pos) = l.find("backed up as' ") {
            let start = pos + "backed up as' ".len();
            let end = l.rfind('\'').unwrap();
            let name = &l[start..end];
            let (base, nanos) = name.rsplit_once('.').unwrap();
            assert!(
                nanos.len() >= 18 && nanos.bytes().all(|c| c.is_ascii_digit()),
                "backup name {name:?} is not <db>.<UnixNano>"
            );
            l = format!(
                "{}<db>.<nanos>{}",
                l[..start].replace(base, "<db>"),
                &l[end..]
            );
            l = l.replace("<db><db>", "<db>");
        }
        l = mask_go_times(&l);
        l = l.replace(&format!("1:{}", side.program_name()), "1:<program>");
        out.push(l);
    }
    let mut joined = out.join("\n");
    if dur.ends_with('\n') {
        joined.push('\n');
    }
    joined
}

/// `2026-09-11 12:56:13.122082934 +0000 UTC m=+0.001145833` → `<time>`.
fn mask_go_times(line: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    while let Some(pos) = find_go_time(rest) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos..];
        let end = go_time_len(after);
        out.push_str("<time>");
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

fn find_go_time(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    (0..b.len().saturating_sub(20)).find(|&i| {
        b[i..i + 4].iter().all(|c| c.is_ascii_digit())
            && b[i + 4] == b'-'
            && b[i + 7] == b'-'
            && b[i + 10] == b' '
            && b[i + 13] == b':'
            && b[i + 16] == b':'
            && s[i + 19..].starts_with(['.', ' '])
    })
}

/// Length of the Go `time.Time` `%v` starting at the beginning of `s`:
/// `2006-01-02 15:04:05[.frac] -0700 MST[ m=+0.000000001]`.
fn go_time_len(s: &str) -> usize {
    let mut i = 19;
    let b = s.as_bytes();
    if b.get(i) == Some(&b'.') {
        i += 1;
        while b.get(i).is_some_and(|c| c.is_ascii_digit()) {
            i += 1;
        }
    }
    // " -0700 MST"
    i += 1;
    while b.get(i).is_some_and(|c| *c != b' ') {
        i += 1;
    }
    i += 1;
    while b
        .get(i)
        .is_some_and(|c| *c != b' ' && *c != b']' && *c != b',')
    {
        i += 1;
    }
    if s[i..].starts_with(" m=+") {
        i += 4;
        while b.get(i).is_some_and(|c| c.is_ascii_digit() || *c == b'.') {
            i += 1;
        }
    }
    i
}

/// The comparable stderr lines: `Error: '…'` of fatal errors.
fn error_lines(out: &Outcome) -> Vec<String> {
    out.stderr_str()
        .lines()
        .filter(|l| l.starts_with("Error: '"))
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// Running a case
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Go,
    Rs,
}

impl Side {
    fn program_name(&self) -> String {
        match self {
            Side::Go => go_bin()
                .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
                .unwrap_or_default(),
            Side::Rs => rust_bin()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        }
    }
}

/// How to compare stdout.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Order {
    /// The Go tool's output order is deterministic.
    Exact,
    /// Go iterates a map: compare as a sorted multiset of lines.
    Sorted,
}

struct Case {
    name: &'static str,
    /// database rows to seed (`None` → no database file is created)
    seeds: Option<Vec<Seed>>,
    /// files to create in the scratch dir: (name, content)
    files: Vec<(String, Vec<u8>)>,
    /// create the `sqlite/` export directory?
    sqlite_dir: bool,
    args: Vec<String>,
    env: Vec<(&'static str, &'static str)>,
    order: Order,
    /// stderr wording is expected to differ (jsoniter error messages)
    stderr_wording_differs: bool,
    /// make the database read-only before running
    readonly_db: bool,
}

impl Case {
    fn new(name: &'static str) -> Case {
        Case {
            name,
            seeds: Some(Seed::all_fixtures()),
            files: Vec::new(),
            sqlite_dir: true,
            args: vec!["g.db".to_string()],
            env: Vec::new(),
            order: Order::Exact,
            stderr_wording_differs: false,
            readonly_db: false,
        }
    }
    fn args(mut self, args: &[&str]) -> Case {
        self.args = args.iter().map(|s| s.to_string()).collect();
        self
    }
    fn file(mut self, name: &str, content: impl Into<Vec<u8>>) -> Case {
        self.files.push((name.to_string(), content.into()));
        self
    }
    fn json_file(self, name: &str, v: &serde_json::Value) -> Case {
        self.file(name, serde_json::to_vec_pretty(v).unwrap())
    }
    fn env(mut self, k: &'static str, v: &'static str) -> Case {
        self.env.push((k, v));
        self
    }
    fn seeds(mut self, seeds: Option<Vec<Seed>>) -> Case {
        self.seeds = seeds;
        self
    }
    fn sorted(mut self) -> Case {
        self.order = Order::Sorted;
        self
    }
}

struct Run {
    dir: TempDir,
    out: Outcome,
    /// bytes of the database before the run (when one was seeded)
    original_db: Option<Vec<u8>>,
}

impl Run {
    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }
    fn db(&self) -> DbState {
        db_state(&self.path("g.db"))
    }
    /// `<db>.<nanos>` backups in the scratch dir
    fn backups(&self) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = fs::read_dir(self.dir.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                let n = p.file_name().unwrap().to_string_lossy().into_owned();
                n.starts_with("g.db.") && n["g.db.".len()..].bytes().all(|c| c.is_ascii_digit())
            })
            .collect();
        v.sort();
        v
    }
    /// relative paths of all regular files under the scratch dir, sorted
    fn files(&self) -> Vec<String> {
        fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
            for e in fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    walk(root, &p, out);
                } else {
                    out.push(p.strip_prefix(root).unwrap().to_string_lossy().into_owned());
                }
            }
        }
        let mut v = Vec::new();
        walk(self.dir.path(), self.dir.path(), &mut v);
        v.sort();
        v
    }
}

fn run_side(bin: &Path, case: &Case, side: Side) -> Run {
    let dir = tempfile::Builder::new()
        .prefix(&format!(
            "g2r_sqlitedb_{}_{}_",
            case.name,
            match side {
                Side::Go => "go",
                Side::Rs => "rs",
            }
        ))
        .tempdir()
        .unwrap();
    let mut original_db = None;
    if let Some(seeds) = &case.seeds {
        let db = dir.path().join("g.db");
        create_db(&db, seeds);
        original_db = Some(fs::read(&db).unwrap());
        if case.readonly_db {
            let mut perm = fs::metadata(&db).unwrap().permissions();
            perm.set_readonly(true);
            fs::set_permissions(&db, perm).unwrap();
        }
    }
    if case.sqlite_dir {
        fs::create_dir(dir.path().join("sqlite")).unwrap();
    }
    for (name, content) in &case.files {
        let p = dir.path().join(name);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&p, content).unwrap();
    }
    let mut inv = Invocation::new()
        .cwd(dir.path().to_path_buf())
        .env("GHA2DB_SKIPLOG", "1")
        .env("GHA2DB_SKIPTIME", "1");
    for (k, v) in &case.env {
        inv = inv.env(k, v);
    }
    for a in &case.args {
        inv = inv.arg(a.clone());
    }
    let out = run(bin, &inv);
    Run {
        dir,
        out,
        original_db,
    }
}

/// Run the case on both sides, compare and return the Rust side (and Go's).
fn both(case: &Case) -> (Run, Option<Run>) {
    let rs = run_side(&rust_bin(), case, Side::Rs);
    let Some(go_bin) = go_bin() else {
        return (rs, None);
    };
    let go = run_side(&go_bin, case, Side::Go);
    let ctx = format!(
        "\ncase {:?} args {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
        case.name,
        case.args,
        case.env,
        go.out.code,
        go.out.stdout_str(),
        go.out.stderr_str(),
        rs.out.code,
        rs.out.stdout_str(),
        rs.out.stderr_str()
    );
    assert_eq!(go.out.code(), rs.out.code(), "exit code differs{ctx}");
    let go_stdout = mask(&go.out.stdout_str(), &Side::Go);
    let rs_stdout = mask(&rs.out.stdout_str(), &Side::Rs);
    match case.order {
        Order::Exact => assert_eq!(go_stdout, rs_stdout, "stdout differs{ctx}"),
        Order::Sorted => {
            let mut g: Vec<&str> = go_stdout.lines().collect();
            let mut r: Vec<&str> = rs_stdout.lines().collect();
            g.sort_unstable();
            r.sort_unstable();
            assert_eq!(g, r, "stdout (as a multiset of lines) differs{ctx}");
        }
    }
    let ge = error_lines(&go.out);
    let re = error_lines(&rs.out);
    if case.stderr_wording_differs {
        assert_eq!(ge.len(), re.len(), "number of Error lines differs{ctx}");
    } else {
        assert_eq!(ge, re, "Error lines differ{ctx}");
    }
    // Side files (sqlite/*.json, *.was, backups) — same names, same bytes,
    // except the database itself and the backup names.
    let strip = |files: Vec<String>| -> Vec<String> {
        files
            .into_iter()
            .filter(|f| f != "g.db")
            .map(|f| {
                if f.starts_with("g.db.") {
                    "g.db.<nanos>".to_string()
                } else {
                    f
                }
            })
            .collect()
    };
    let gf = strip(go.files());
    let rf = strip(rs.files());
    assert_eq!(gf, rf, "files in the scratch dir differ{ctx}");
    for f in &gf {
        if f == "g.db.<nanos>" {
            continue;
        }
        let a = fs::read(go.path(f)).unwrap();
        let b = fs::read(rs.path(f)).unwrap();
        assert!(
            a == b,
            "file {f} differs{ctx}\n--- go:\n{}\n--- rust:\n{}",
            String::from_utf8_lossy(&a),
            String::from_utf8_lossy(&b)
        );
    }
    let gb = go.backups();
    let rb = rs.backups();
    assert_eq!(gb.len(), rb.len(), "number of backups differs{ctx}");
    for (a, b) in gb.iter().zip(rb.iter()) {
        assert_eq!(
            fs::read(a).unwrap(),
            go.original_db.clone().unwrap(),
            "Go backup is not the original database{ctx}"
        );
        assert_eq!(
            fs::read(b).unwrap(),
            rs.original_db.clone().unwrap(),
            "Rust backup is not the original database{ctx}"
        );
    }
    // Database state.
    if case.seeds.is_some() && go.path("g.db").exists() {
        let gs = go.db();
        let rs_state = rs.db();
        assert_eq!(
            gs.dashboards, rs_state.dashboards,
            "dashboard rows differ{ctx}"
        );
        assert_eq!(gs.tags, rs_state.tags, "dashboard tags differ{ctx}");
        assert_eq!(
            gs.times.keys().collect::<Vec<_>>(),
            rs_state.times.keys().collect::<Vec<_>>()
        );
        for (uid, (c, u)) in gs.times.iter().chain(rs_state.times.iter()) {
            assert!(
                is_go_sqlite3_timestamp(c) && is_go_sqlite3_timestamp(u),
                "uid {uid}: timestamps {c:?} / {u:?} are not go-sqlite3 style"
            );
        }
    }
    (rs, Some(go))
}

fn stdout_lines(out: &Outcome) -> Vec<String> {
    mask(&out.stdout_str(), &Side::Rs)
        .lines()
        .filter(|l| *l != BANNER)
        .map(str::to_string)
        .collect()
}

/// [`stdout_lines`] without the lines `GHA2DB_QOUT` adds for the (skipped)
/// PostgreSQL logging of the banner: the connection string, the `gha_logs`
/// insert and its arguments.
fn stdout_lines_sans_pg_log(out: &Outcome) -> Vec<String> {
    stdout_lines(out)
        .into_iter()
        .filter(|l| {
            !(l.starts_with("PgConnectString: ")
                || l.starts_with("insert into gha_logs(")
                || l.starts_with("[1:<program> 2:"))
        })
        .collect()
}

fn pretty(v: &serde_json::Value) -> Vec<u8> {
    devstatscode::json::pretty_print_json(serde_json::to_vec(v).unwrap().as_slice())
}

/// Length of the pretty-printed form (what `data … bytes` reports).
fn plen(v: &serde_json::Value) -> usize {
    pretty(v).len()
}

// ---------------------------------------------------------------------------
// Usage / argument handling
// ---------------------------------------------------------------------------

#[test]
fn no_args_prints_usage_and_exits_1() {
    let case = Case::new("usage").args(&[]).seeds(None);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 1);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Required args: grafana.db file name and list(*) of jsons to import.",
            "If only db file name given, it will output all dashboards to jsons",
            "It will import JSONs by matching their internal uid with SQLite database",
            "If DB name given and single argument with comman separated uids - dashboards with those uids will be removed",
        ]
    );
    assert!(rs.out.stderr_str().is_empty());
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

#[test]
fn export_writes_pretty_sorted_json_per_dashboard() {
    let case = Case::new("export");
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let lines = stdout_lines(&rs.out);
    assert_eq!(
        lines,
        vec![
            "Written 'Companies Table' to sqlite/companies-table.json",
            "Written 'New Contributors Table' to sqlite/new-contributors-table.json",
            "Written 'PR Comments' to sqlite/pr-comments.json",
            "Written 'Repository groups' to sqlite/repository-groups.json",
            "Time: <duration>",
        ]
    );
    for (file, _, _, _) in DASHBOARDS {
        let exported = fs::read(rs.path(&format!("sqlite/{file}"))).unwrap();
        assert_eq!(exported, pretty(&dashboard_json(file)), "{file}");
        // keys sorted, 2-space indent, no trailing newline (Go MarshalIndent)
        let text = String::from_utf8(exported).unwrap();
        assert!(
            text.starts_with("{\n  \"annotations\": {"),
            "{file}: {}",
            &text[..40]
        );
        assert!(text.ends_with("\n}"), "{file}");
    }
}

#[test]
fn export_of_empty_db_writes_nothing() {
    let case = Case::new("export_empty").seeds(Some(Vec::new()));
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(stdout_lines(&rs.out), vec!["Time: <duration>"]);
    assert_eq!(rs.files(), vec!["g.db"]);
}

#[test]
fn export_without_sqlite_dir_is_fatal() {
    let mut case = Case::new("export_no_dir");
    case.sqlite_dir = false;
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'open sqlite/companies-table.json: no such file or directory'"]
    );
    assert!(rs.out.stdout_str().is_empty());
}

#[test]
fn export_of_missing_db_creates_empty_file_and_fails() {
    let case = Case::new("export_missing_db").seeds(None);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'no such table: dashboard'"]
    );
    // sqlite3 creates the database file on open
    assert_eq!(fs::metadata(rs.path("g.db")).unwrap().len(), 0);
}

#[test]
fn db_path_that_is_a_directory_is_fatal() {
    let case = Case::new("db_is_dir").seeds(None).args(&["sqlite"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'unable to open database file: is a directory'"]
    );
}

#[test]
fn db_in_missing_directory_is_fatal() {
    let case = Case::new("db_missing_dir")
        .seeds(None)
        .args(&["nodir/g.db"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'unable to open database file: no such file or directory'"]
    );
}

#[test]
fn db_that_is_not_sqlite_is_fatal() {
    let case = Case::new("db_garbage").seeds(None).file(
        "g.db",
        "this is not a database, just some bytes\n".repeat(3),
    );
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'file is not a database'"]
    );
}

// ---------------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------------

#[test]
fn import_into_empty_db_creates_dashboards_and_tags() {
    let case = Case::new("import_create")
        .seeds(Some(Vec::new()))
        .file(
            "in/companies.json",
            fs::read(fixture("sqlitedb/dashboards/companies-table.json")).unwrap(),
        )
        .file(
            "in/pr.json",
            fs::read(fixture("sqlitedb/dashboards/pr-comments.json")).unwrap(),
        )
        .args(&["g.db", "in/companies.json", "in/pr.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Processing 'in/companies.json'",
            "Inserted dashboard: id=1 (uid=5, title=Companies Table, slug=companies-table)",
            "Updated dashboard tags '5 Companies Table' id: 1, '' -> 'companies,dashboard,prometheus,table', added: 4, removed: 0",
            "Original db file backed up as' <db>.<nanos>'",
            "Processing 'in/pr.json'",
            "Inserted dashboard: id=2 (uid=17, title=PR Comments, slug=pr-comments)",
            "Updated dashboard tags '17 PR Comments' id: 2, '' -> 'dashboard,prometheus', added: 2, removed: 0",
            "SQLite DB has 0 dashboards, there were 2 JSONs to import, updated 0, created 2",
            "Time: <duration>",
        ]
    );
    let db = rs.db();
    assert_eq!(db.dashboards.len(), 2);
    let d = &db.dashboards[0];
    assert_eq!((d.id, d.version, d.org_id), (1, 1, 1));
    assert_eq!(
        (d.slug.as_str(), d.title.as_str()),
        ("companies-table", "Companies Table")
    );
    assert_eq!(d.uid.as_deref(), Some("5"));
    assert_eq!(
        (d.created_by, d.updated_by, d.gnet_id),
        (Some(1), Some(1), Some(0))
    );
    assert_eq!(d.plugin_id.as_deref(), Some(""));
    assert_eq!((d.folder_id, d.is_folder, d.has_acl), (0, 0, 0));
    // data is stored pretty-printed, key-sorted
    assert_eq!(
        d.data.as_bytes(),
        pretty(&dashboard_json("companies-table.json")).as_slice()
    );
    assert_eq!(
        db.tags,
        [
            (1, "companies"),
            (1, "dashboard"),
            (1, "prometheus"),
            (1, "table"),
            (2, "dashboard"),
            (2, "prometheus")
        ]
        .into_iter()
        .map(|(i, t)| (i, t.to_string()))
        .collect()
    );
    assert_eq!(rs.backups().len(), 1);
}

#[test]
fn reimporting_exported_dashboards_changes_nothing() {
    // The seeded rows hold compact JSON with the fixture's key order; the
    // pretty-printed forms of both sides are equal, so nothing is updated —
    // no backup, no `.was` files (Go bug 28 made every re-import an update).
    let mut case = Case::new("import_noop").args(&[
        "g.db",
        "in/companies-table.json",
        "in/new-contributors-table.json",
        "in/pr-comments.json",
        "in/repository-groups.json",
    ]);
    for (file, _, _, _) in DASHBOARDS {
        case = case.file(
            &format!("in/{file}"),
            fs::read(fixture(&format!("sqlitedb/dashboards/{file}"))).unwrap(),
        );
    }
    let before = Seed::all_fixtures();
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Processing 'in/companies-table.json'",
            "Processing 'in/new-contributors-table.json'",
            "Processing 'in/pr-comments.json'",
            "Processing 'in/repository-groups.json'",
            "SQLite DB has 4 dashboards, there were 4 JSONs to import, updated 0, created 0",
            "Time: <duration>",
        ]
    );
    assert!(rs.backups().is_empty());
    let db = rs.db();
    for (row, seed) in db.dashboards.iter().zip(before.iter()) {
        assert_eq!(row.data, seed.data, "data must stay compact/unchanged");
        assert_eq!(row.version, 3);
    }
    assert_eq!(rs.files().iter().filter(|f| f.ends_with(".was")).count(), 0);
}

/// The JSONs of [`update_case`].
struct UpdateJsons {
    renamed: serde_json::Value,
    retagged: serde_json::Value,
    panel: serde_json::Value,
    fresh: serde_json::Value,
}

fn update_jsons() -> UpdateJsons {
    let mut renamed = dashboard_json("pr-comments.json");
    renamed["title"] = "PR Comments (v2)".into();
    let mut retagged = dashboard_json("repository-groups.json");
    retagged["tags"] = serde_json::json!(["zeta", "alpha", "dashboard"]);
    let mut panel = dashboard_json("new-contributors-table.json");
    panel["refresh"] = "1h".into();
    panel["rows"] = serde_json::json!([]);
    let mut fresh = dashboard_json("companies-table.json");
    fresh["uid"] = "9999".into();
    fresh["title"] = "Brand New".into();
    fresh["tags"] = serde_json::json!(["new"]);
    UpdateJsons {
        renamed,
        retagged,
        panel,
        fresh,
    }
}

/// The scenario of a real `import_jsons_to_sqlite.sh` run: title change
/// (→ slug change), tags change, panel change, unchanged file, brand new file.
fn update_case(name: &'static str) -> Case {
    let j = update_jsons();
    Case::new(name)
        .json_file("in/renamed.json", &j.renamed)
        .json_file("in/retagged.json", &j.retagged)
        .json_file("in/panel.json", &j.panel)
        .json_file("in/same.json", &dashboard_json("companies-table.json"))
        .json_file("in/fresh.json", &j.fresh)
        .args(&[
            "g.db",
            "in/renamed.json",
            "in/retagged.json",
            "in/panel.json",
            "in/same.json",
            "in/fresh.json",
        ])
        .sorted()
}

#[test]
fn import_updates_title_slug_tags_and_data() {
    let case = update_case("import_update");
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let mut lines = stdout_lines(&rs.out);
    lines.sort();
    let j = update_jsons();
    let mut expected: Vec<String> = vec![
        "Processing 'in/renamed.json'".to_string(),
        "Processing 'in/retagged.json'".to_string(),
        "Processing 'in/panel.json'".to_string(),
        "Processing 'in/same.json'".to_string(),
        "Processing 'in/fresh.json'".to_string(),
        "Inserted dashboard: id=5 (uid=9999, title=Brand New, slug=brand-new)".to_string(),
        "Updated dashboard tags '9999 Brand New' id: 5, '' -> 'new', added: 1, removed: 0".to_string(),
        "Original db file backed up as' <db>.<nanos>'".to_string(),
        format!(
            "in/renamed.json: updated dashboard: uid: 17 title: 'PR Comments' -> 'PR Comments (v2)', slug: 'pr-comments' -> 'pr-comments-v2-', tags: false:[dashboard prometheus] (data {} -> {} bytes)",
            plen(&dashboard_json("pr-comments.json")),
            plen(&j.renamed)
        ),
        "Updated dashboard tags '68 Repository groups' id: 4, 'dashboard,prometheus,table' -> 'alpha,dashboard,zeta', added: 2, removed: 2".to_string(),
        format!(
            "in/retagged.json: updated dashboard: uid: 68 title: 'Repository groups' -> 'Repository groups', slug: 'repository-groups' -> 'repository-groups', tags: true:[alpha dashboard zeta] (data {} -> {} bytes)",
            plen(&dashboard_json("repository-groups.json")),
            plen(&j.retagged)
        ),
        format!(
            "in/panel.json: updated dashboard: uid: 52 title: 'New Contributors Table' -> 'New Contributors Table', slug: 'new-contributors-table' -> 'new-contributors-table', tags: false:[dashboard prometheus table] (data {} -> {} bytes)",
            plen(&dashboard_json("new-contributors-table.json")),
            plen(&j.panel)
        ),
        "SQLite DB has 4 dashboards, there were 5 JSONs to import, updated 3, created 1".to_string(),
        "Time: <duration>".to_string(),
    ];
    expected.sort();
    assert_eq!(lines, expected);

    // `.was` snapshots hold the pretty-printed previous data of the updated ones
    let mut was: Vec<String> = rs
        .files()
        .into_iter()
        .filter(|f| f.ends_with(".was"))
        .collect();
    was.sort();
    assert_eq!(
        was,
        vec![
            "in/panel.json.was",
            "in/renamed.json.was",
            "in/retagged.json.was"
        ]
    );
    assert_eq!(
        fs::read(rs.path("in/renamed.json.was")).unwrap(),
        pretty(&dashboard_json("pr-comments.json"))
    );
    assert_eq!(rs.backups().len(), 1);

    let db = rs.db();
    assert_eq!(db.dashboards.len(), 5);
    let by_uid = |uid: &str| {
        db.dashboards
            .iter()
            .find(|d| d.uid.as_deref() == Some(uid))
            .unwrap()
    };
    assert_eq!(by_uid("17").title, "PR Comments (v2)");
    assert_eq!(by_uid("17").slug, "pr-comments-v2-");
    assert_eq!(by_uid("68").title, "Repository groups");
    assert_eq!(by_uid("5").version, 3, "untouched row keeps its version");
    assert_eq!(
        by_uid("9999").version,
        1,
        "new rows are inserted with version 1"
    );
    let tags_of = |id: i64| -> Vec<String> {
        db.tags
            .iter()
            .filter(|(i, _)| *i == id)
            .map(|(_, t)| t.clone())
            .collect()
    };
    assert_eq!(tags_of(by_uid("68").id), vec!["alpha", "dashboard", "zeta"]);
    assert_eq!(tags_of(by_uid("9999").id), vec!["new"]);
    assert_eq!(
        tags_of(by_uid("5").id),
        vec!["companies", "dashboard", "prometheus", "table"]
    );
}

#[test]
fn import_update_with_debug_1_prints_struct_dumps() {
    let case = update_case("import_update_debug1").env("GHA2DB_DEBUG", "1");
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let out = rs.out.stdout_str();
    // `%+v` of dashboardData uses its String() method
    let j = update_jsons();
    assert!(out.contains(&format!(
        "in/renamed.json: updated uid: 17: tags updated: false\nnew: {{dash:'{{Title:PR Comments (v2) UID:17 Tags:[dashboard prometheus]}}', id:3, title:'PR Comments (v2)', slug:'pr-comments-v2-', data:len:{}, fn:'in/renamed.json'}}\nold: {{dash:'{{Title:PR Comments UID:17 Tags:[dashboard prometheus]}}', id:3, title:'PR Comments', slug:'pr-comments', data:len:{}, fn:'*pr-comments.json*'}}\n",
        plen(&j.renamed),
        plen(&dashboard_json("pr-comments.json"))
    )), "{out}");
    assert!(out.contains(
        "Updating dashboard '68 Repository groups' id: 4, 'dashboard,prometheus,table' -> 'alpha,dashboard,zeta', inserted 'alpha' tag"
    ));
    assert!(out.contains(
        "Updating dashboard '68 Repository groups' id: 4, 'dashboard,prometheus,table' -> 'alpha,dashboard,zeta', deleted 'table' tag"
    ));
    assert!(out.contains("lib.ReadFile('g.db'): ok\n"));
}

#[test]
fn import_update_with_debug_2_prints_string_forms() {
    let case = update_case("import_update_debug2").env("GHA2DB_DEBUG", "2");
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let out = rs.out.stdout_str();
    // printed before updateTags sorts the tags, so both keep the JSON order
    let n = plen(&dashboard_json("companies-table.json"));
    assert!(out.contains(&format!(
        "\n{{dash:'{{Title:Companies Table UID:5 Tags:[dashboard prometheus companies table]}}', id:1, title:'Companies Table', slug:'companies-table', data:len:{n}, fn:'in/same.json'}}\n{{dash:'{{Title:Companies Table UID:5 Tags:[dashboard prometheus companies table]}}', id:1, title:'Companies Table', slug:'companies-table', data:len:{n}, fn:'*companies-table.json*'}}\n\n"
    )), "{out}");
}

#[test]
fn qout_echoes_every_query_with_arguments() {
    let case = update_case("import_update_qout").env("GHA2DB_QOUT", "1");
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let out = rs.out.stdout_str();
    // argument-less queries are echoed without the `[…]` arguments line
    assert!(
        out.contains("\nselect id, data, title, slug, uid from dashboard\n"),
        "{out}"
    );
    assert!(!out.lines().any(|l| l == "[]"), "{out}");
    assert!(
        out.contains(
            "[4]\nselect term from dashboard_tag where dashboard_id = ? order by term asc\n"
        ),
        "{out}"
    );
    assert!(
        out.contains("[4 alpha]\ninsert into dashboard_tag(dashboard_id, term) values(?, ?)\n"),
        "{out}"
    );
    assert!(
        out.contains("[4 table]\ndelete from dashboard_tag where dashboard_id = ? and term = ?\n"),
        "{out}"
    );
    assert!(out.contains("\nselect max(id) from dashboard\n"), "{out}");
    assert!(
        out.contains("[PR Comments (v2) pr-comments-v2- {\n"),
        "{out}"
    );
    assert!(
        out.contains("\n} 3]\nupdate dashboard set title = ?, slug = ?, data = ? where id = ?\n"),
        "{out}"
    );
    // the insert echo spans the pretty-printed data: `[brand-new Brand New {` …
    // `} <now> <now> 9999]`, followed by the statement
    assert!(out.contains("\n[brand-new Brand New {\n"), "{out}");
    let insert_end = out
        .lines()
        .find(|l| l.starts_with("} ") && l.ends_with(" 9999]"))
        .unwrap_or_else(|| panic!("no insert echo in {out}"));
    assert_eq!(mask_go_times(insert_end), "} <time> <time> 9999]");
    assert!(out.contains(&format!(
        "{insert_end}\ninsert into dashboard(version, slug, title, data, org_id, created, updated, created_by, updated_by, gnet_id, plugin_id, folder_id, is_folder, has_acl, uid) values(1, ?, ?, ?, 1, ?, ?, 1, 1, 0, '', 0, 0, 0, ?)\n"
    )), "{out}");
}

#[test]
fn export_with_qout_is_exact() {
    let case = Case::new("export_qout").env("GHA2DB_QOUT", "1");
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        &stdout_lines_sans_pg_log(&rs.out)[..2],
        &[
            "select slug, title, data from dashboard",
            "Written 'Companies Table' to sqlite/companies-table.json"
        ]
    );
}

#[test]
fn json_with_case_insensitive_keys_and_nulls_decodes_like_jsoniter() {
    let mut fresh = dashboard_json("companies-table.json");
    fresh.as_object_mut().unwrap().remove("uid");
    fresh.as_object_mut().unwrap().remove("title");
    fresh.as_object_mut().unwrap().remove("tags");
    fresh["UID"] = "4242".into();
    fresh["Title"] = "Mixed Case Keys".into();
    fresh["TAGS"] = serde_json::json!(["x", null, "a"]);
    let case = Case::new("import_mixed_case")
        .json_file("in/mixed.json", &fresh)
        .args(&["g.db", "in/mixed.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let lines = stdout_lines(&rs.out);
    assert!(
        lines.contains(
            &"Inserted dashboard: id=5 (uid=4242, title=Mixed Case Keys, slug=mixed-case-keys)"
                .to_string()
        ),
        "{lines:?}"
    );
    assert!(lines.contains(&"Updated dashboard tags '4242 Mixed Case Keys' id: 5, '' -> ',a,x', added: 3, removed: 0".to_string()), "{lines:?}");
    let db = rs.db();
    let tags: Vec<String> = db
        .tags
        .iter()
        .filter(|(i, _)| *i == 5)
        .map(|(_, t)| t.clone())
        .collect();
    assert_eq!(tags, vec!["", "a", "x"]);
}

#[test]
fn top_level_null_json_imports_an_empty_dashboard() {
    let case = Case::new("import_null")
        .seeds(Some(Vec::new()))
        .file("in/null.json", "null")
        .args(&["g.db", "in/null.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Processing 'in/null.json'",
            "Inserted dashboard: id=1 (uid=, title=, slug=)",
            "Original db file backed up as' <db>.<nanos>'",
            "SQLite DB has 0 dashboards, there were 1 JSONs to import, updated 0, created 1",
            "Time: <duration>",
        ]
    );
    assert_eq!(rs.db().dashboards[0].data, "null");
}

#[test]
fn inconsistent_db_rows_are_reported_and_db_values_win() {
    // dashboard table says title/uid A, the stored JSON says B
    let mut seeds = Seed::all_fixtures();
    seeds[2].title = "Renamed In Table".to_string();
    seeds[2].slug = Some("renamed-in-table".to_string());
    seeds[3].uid = "6868".to_string();
    let mut reimport = dashboard_json("pr-comments.json");
    reimport["title"] = "Renamed In Table".into();
    let case = Case::new("import_inconsistent")
        .seeds(Some(seeds))
        .json_file("in/pr.json", &reimport)
        .args(&["g.db", "in/pr.json"])
        .sorted();
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let out = rs.out.stdout_str();
    // `%+v` of dashboardData is its String(); data is still the raw (compact) DB text here
    assert!(out.contains(&format!(
        "SQLite internal inconsistency (title): Renamed In Table != PR Comments: {{dash:'{{Title:PR Comments UID:17 Tags:[dashboard prometheus]}}', id:3, title:'Renamed In Table', slug:'renamed-in-table', data:len:{}, fn:''}}, using value from dashboard table, not from JSON\n",
        Seed::from_fixture("pr-comments.json").data.len()
    )), "{out}");
    assert!(out.contains(&format!(
        "SQLite internal inconsistency (uid): 6868 != 68: {{dash:'{{Title:Repository groups UID:68 Tags:[dashboard prometheus table]}}', id:4, title:'Repository groups', slug:'repository-groups', data:len:{}, fn:''}}, using value from dashboard table, not from JSON\n",
        Seed::from_fixture("repository-groups.json").data.len()
    )), "{out}");
    // the JSON's title now equals the table's title, but its data differs from the stored data
    assert!(out.contains(&format!(
        "in/pr.json: updated dashboard: uid: 17 title: 'Renamed In Table' -> 'Renamed In Table', slug: 'renamed-in-table' -> 'renamed-in-table', tags: false:[dashboard prometheus] (data {} -> {} bytes)",
        plen(&dashboard_json("pr-comments.json")),
        plen(&reimport)
    )), "{out}");
}

// ---------------------------------------------------------------------------
// Import failures
// ---------------------------------------------------------------------------

#[test]
fn import_into_missing_db_is_fatal_before_any_change() {
    let case = Case::new("import_missing_db")
        .seeds(None)
        .json_file("in/x.json", &dashboard_json("pr-comments.json"))
        .args(&["g.db", "in/x.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'open g.db: no such file or directory'"]
    );
    assert!(
        !rs.path("g.db").exists(),
        "ReadFile fails before sqlite3 could create it"
    );
}

#[test]
fn import_with_db_path_being_a_directory_is_fatal() {
    let case = Case::new("import_db_dir")
        .seeds(None)
        .json_file("in/x.json", &dashboard_json("pr-comments.json"))
        .args(&["in", "in/x.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'read in: is a directory'"]
    );
}

#[test]
fn missing_json_file_is_fatal_after_earlier_files_were_imported() {
    let mut fresh = dashboard_json("companies-table.json");
    fresh["uid"] = "9999".into();
    fresh["title"] = "Brand New".into();
    let case = Case::new("import_missing_json")
        .json_file("in/fresh.json", &fresh)
        .args(&["g.db", "in/fresh.json", "in/missing.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'open in/missing.json: no such file or directory'"]
    );
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Processing 'in/fresh.json'",
            "Inserted dashboard: id=5 (uid=9999, title=Brand New, slug=brand-new)",
            "Updated dashboard tags '9999 Brand New' id: 5, '' -> 'companies,dashboard,prometheus,table', added: 4, removed: 0",
            "Original db file backed up as' <db>.<nanos>'",
            "Processing 'in/missing.json'",
        ]
    );
    assert_eq!(
        rs.db().dashboards.len(),
        5,
        "the first file was already inserted"
    );
}

#[test]
fn invalid_json_is_fatal() {
    let mut case = Case::new("import_bad_json")
        .file("in/bad.json", "{\"uid\": \"1\", ")
        .file("in/text.json", "Compiled None")
        .args(&["g.db", "in/bad.json", "in/text.json"]);
    case.stderr_wording_differs = true; // jsoniter's error text is not reproduced
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(error_lines(&rs.out).len(), 1);
    assert_eq!(stdout_lines(&rs.out), vec!["Processing 'in/bad.json'"]);
}

#[test]
fn wrong_json_types_are_fatal() {
    let mut case = Case::new("import_wrong_types")
        .file("in/arr.json", "[1, 2, 3]")
        .args(&["g.db", "in/arr.json"]);
    case.stderr_wording_differs = true;
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    let mut case = Case::new("import_wrong_tags")
        .file(
            "in/tags.json",
            "{\"uid\": \"1\", \"title\": \"T\", \"tags\": \"not-an-array\"}",
        )
        .args(&["g.db", "in/tags.json"]);
    case.stderr_wording_differs = true;
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    let mut case = Case::new("import_wrong_title")
        .file("in/title.json", "{\"uid\": \"1\", \"title\": 12}")
        .args(&["g.db", "in/title.json"]);
    case.stderr_wording_differs = true;
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
}

#[test]
fn duplicate_uid_among_jsons_is_fatal() {
    let case = Case::new("import_dup_uid")
        .json_file("in/a.json", &dashboard_json("pr-comments.json"))
        .json_file("in/b.json", &dashboard_json("pr-comments.json"))
        .args(&["g.db", "in/a.json", "in/b.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'in/b.json: duplicate json uid, attempt to import {PR Comments 17 [dashboard prometheus]}, collision with {PR Comments 17 [dashboard prometheus]}'"]
    );
    assert!(rs.backups().is_empty());
}

#[test]
fn duplicate_new_uid_hits_the_unique_index() {
    // Both files carry a uid unknown to the database: the first is inserted,
    // the second insert violates UQE_dashboard_org_id_uid.
    let mut fresh = dashboard_json("companies-table.json");
    fresh["uid"] = "9999".into();
    fresh["title"] = "Brand New".into();
    let mut fresh2 = fresh.clone();
    fresh2["title"] = "Brand New 2".into();
    let case = Case::new("import_dup_new_uid")
        .json_file("in/a.json", &fresh)
        .json_file("in/b.json", &fresh2)
        .args(&["g.db", "in/a.json", "in/b.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'UNIQUE constraint failed: dashboard.org_id, dashboard.uid'"]
    );
    assert_eq!(rs.db().dashboards.len(), 5);
}

#[test]
fn new_uid_with_existing_title_hits_the_unique_index() {
    let mut dup = dashboard_json("companies-table.json");
    dup["uid"] = "9999".into();
    let case = Case::new("import_dup_title")
        .json_file("in/dup.json", &dup)
        .args(&["g.db", "in/dup.json"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'UNIQUE constraint failed: dashboard.org_id, dashboard.folder_id, dashboard.title'"]
    );
    assert!(rs.backups().is_empty(), "nothing was changed, so no backup");
}

#[test]
fn readonly_db_makes_inserts_fail() {
    // A read-only file cannot be tested as root (root ignores the mode bits).
    let probe = tempfile::NamedTempFile::new().unwrap();
    let mut perm = fs::metadata(probe.path()).unwrap().permissions();
    perm.set_readonly(true);
    fs::set_permissions(probe.path(), perm).unwrap();
    if fs::OpenOptions::new()
        .write(true)
        .open(probe.path())
        .is_ok()
    {
        eprintln!("running as root — skipping the read-only database case");
        return;
    }
    let mut fresh = dashboard_json("companies-table.json");
    fresh["uid"] = "9999".into();
    fresh["title"] = "Brand New".into();
    let mut case = Case::new("import_readonly")
        .json_file("in/fresh.json", &fresh)
        .args(&["g.db", "in/fresh.json"]);
    case.readonly_db = true;
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'attempt to write a readonly database'"]
    );
    assert!(rs.backups().is_empty());
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

#[test]
fn delete_by_comma_separated_uids() {
    let case = Case::new("delete").args(&["g.db", "17,12345,68"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Deleted dashboard with uid 17",
            "Dashboard with uid=12345 not found, skipping",
            "Deleted dashboard with uid 68",
            "Time: <duration>",
        ]
    );
    let db = rs.db();
    let uids: Vec<&str> = db
        .dashboards
        .iter()
        .map(|d| d.uid.as_deref().unwrap())
        .collect();
    assert_eq!(uids, vec!["5", "52"]);
    assert!(
        db.tags.iter().all(|(id, _)| *id == 1 || *id == 2),
        "{:?}",
        db.tags
    );
    assert_eq!(db.tags.len(), 7);
    assert!(rs.backups().is_empty(), "delete mode never backs up");
}

#[test]
fn delete_with_qout_is_exact() {
    let case = Case::new("delete_qout")
        .args(&["g.db", "5,x5"])
        .env("GHA2DB_QOUT", "1");
    // `5,x5` is not all-numeric → treated as a JSON path to import
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'open 5,x5: no such file or directory'"]
    );
    let case = Case::new("delete_qout2")
        .args(&["g.db", "5,52"])
        .env("GHA2DB_QOUT", "1");
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines_sans_pg_log(&rs.out),
        vec![
            "[5]",
            "select id from dashboard where uid = ?",
            "[1]",
            "delete from dashboard_tag where dashboard_id = ?",
            "[1]",
            "delete from dashboard where id = ?",
            "Deleted dashboard with uid 5",
            "[52]",
            "select id from dashboard where uid = ?",
            "[2]",
            "delete from dashboard_tag where dashboard_id = ?",
            "[2]",
            "delete from dashboard where id = ?",
            "Deleted dashboard with uid 52",
            "Time: <duration>",
        ]
    );
}

#[test]
fn single_numeric_argument_deletes_instead_of_importing() {
    let case = Case::new("delete_single").args(&["g.db", "5"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines(&rs.out),
        vec!["Deleted dashboard with uid 5", "Time: <duration>"]
    );
    assert_eq!(rs.db().dashboards.len(), 3);
}

#[test]
fn numeric_looking_lists_with_gaps_or_spaces_are_import_paths() {
    for arg in ["1,,2", " 5", "5 ", "5,", "0x5", "5.0", "٥"] {
        let case = Case::new("delete_not_numeric").args(&["g.db", arg]);
        let (rs, _) = both(&case);
        assert_eq!(rs.out.code(), 2, "{arg:?}");
        assert_eq!(
            error_lines(&rs.out),
            vec![format!("Error: 'open {arg}: no such file or directory'")],
            "{arg:?}"
        );
        assert_eq!(rs.db().dashboards.len(), 4, "{arg:?}");
    }
}

#[test]
fn signed_numbers_parse_like_strconv_atoi_and_delete() {
    // strconv.Atoi accepts a leading sign, so these are delete lists; the uid
    // strings are matched literally (`+68` ≠ `68`).
    let case = Case::new("delete_signed").args(&["g.db", "+68,-5,17"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Dashboard with uid=+68 not found, skipping",
            "Dashboard with uid=-5 not found, skipping",
            "Deleted dashboard with uid 17",
            "Time: <duration>",
        ]
    );
    assert_eq!(rs.db().dashboards.len(), 3);
    // out-of-range for Atoi (int64 overflow) → import path
    let case = Case::new("delete_overflow").args(&["g.db", "99999999999999999999"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'open 99999999999999999999: no such file or directory'"]
    );
    // but int64 range is fine
    let case = Case::new("delete_big").args(&["g.db", "9223372036854775807"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 0);
    assert_eq!(
        stdout_lines(&rs.out),
        vec![
            "Dashboard with uid=9223372036854775807 not found, skipping",
            "Time: <duration>",
        ]
    );
}

#[test]
fn two_numeric_arguments_are_import_paths() {
    // delete mode needs exactly one (comma separated) argument after the db
    let case = Case::new("delete_two_args").args(&["g.db", "5", "17"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'open 5: no such file or directory'"]
    );
    assert_eq!(rs.db().dashboards.len(), 4);
}

#[test]
fn delete_from_missing_db_fails_after_creating_it() {
    let case = Case::new("delete_missing_db")
        .seeds(None)
        .args(&["g.db", "5"]);
    let (rs, _) = both(&case);
    assert_eq!(rs.out.code(), 2);
    assert_eq!(
        error_lines(&rs.out),
        vec!["Error: 'no such table: dashboard'"]
    );
    assert_eq!(fs::metadata(rs.path("g.db")).unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

#[test]
fn export_then_import_round_trip_is_stable() {
    // export everything, re-import the exported files: nothing changes
    let case = Case::new("roundtrip_export");
    let (rs, go) = both(&case);
    assert_eq!(rs.out.code(), 0);
    let mut inv = Invocation::new()
        .cwd(rs.dir.path().to_path_buf())
        .env("GHA2DB_SKIPLOG", "1")
        .env("GHA2DB_SKIPTIME", "1")
        .arg("g.db");
    for (file, _, _, _) in DASHBOARDS {
        inv = inv.arg(format!("sqlite/{file}"));
    }
    let rs2 = run(&rust_bin(), &inv);
    assert_eq!(rs2.code(), 0, "{}", rs2.stderr_str());
    let lines = stdout_lines(&rs2);
    assert_eq!(lines.last().map(|s| s.as_str()), Some("Time: <duration>"));
    assert_eq!(
        lines[lines.len() - 2],
        "SQLite DB has 4 dashboards, there were 4 JSONs to import, updated 0, created 0"
    );
    if let Some(go) = go {
        let mut inv = inv.clone();
        inv.cwd = Some(go.dir.path().to_path_buf());
        let go2 = run(&go_bin().unwrap(), &inv);
        assert_eq!(go2.code(), 0, "{}", go2.stderr_str());
        assert_eq!(
            mask(&go2.stdout_str(), &Side::Go),
            mask(&rs2.stdout_str(), &Side::Rs)
        );
        assert_eq!(go.db().dashboards, rs.db().dashboards);
        assert!(go.backups().is_empty() && rs.backups().is_empty());
    }
}

#[test]
fn masking_helpers_work() {
    assert!(is_go_sqlite3_timestamp(
        "2026-09-08 14:25:05.700421301+00:00"
    ));
    assert!(is_go_sqlite3_timestamp("2026-09-08 14:25:05+02:00"));
    assert!(is_go_sqlite3_timestamp("2026-09-08 14:25:05.7-05:00"));
    assert!(!is_go_sqlite3_timestamp("2026-09-08T14:25:05Z"));
    assert!(!is_go_sqlite3_timestamp("2026-09-08 14:25:05.+00:00"));
    assert_eq!(
        mask_go_times("[x 2026-09-11 12:56:13.122082934 +0000 UTC m=+0.001145833 2026-09-11 12:56:13.12208949 +0200 CEST 9999]"),
        "[x <time> <time> 9999]"
    );
    assert_eq!(
        mask_go_times(
            "[x 2026-09-11 12:56:13.122082934 +0000 UTC 2026-09-11 12:56:13 +0000 UTC 9999]"
        ),
        "[x <time> <time> 9999]"
    );
}
