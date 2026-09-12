//! Go ⇄ Rust compatibility tests for `website_data`.
//!
//! Every case builds one set of scratch project databases
//! (`dbtest_wd_<case>_<project>`, schema from
//! `compat/fixtures/website_data/schema.sql`, deterministic seed rows relative
//! to `now()`), then runs the Go and the Rust binary — each in its own scratch
//! directory holding `projects.yaml`, `util_sql/exclude_bots.sql` (the real
//! DevStats file), the `jsons/` output directory, an optional fake
//! `last_tag.sh` and fake repository directories — against the **same**
//! databases (the tool only reads them).
//!
//! Compared: exit code, stdout as a multiset of lines (concurrent workers;
//! durations, scratch paths and the `GHA2DB_QOUT` log-insert time argument
//! masked) with the banner first and the `Generated website data in:` line
//! last, the `Error:`/`PqError:` stderr lines, and the generated JSON files:
//! the same file names, semantically equal contents (the `timestamp` values
//! — `time.Now()` — are checked for Go's RFC3339Nano format and for the same
//! UTC offset, then masked) and byte-identical pretty formatting once the Go
//! object keys are sorted (jsoniter writes them in random order).
//!
//! The cases need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};
use devstatscode::json::to_pretty_json;
use serde_json::Value;
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("website_data")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_website_data"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Seed exercising every query: distinct SHAs, bot exclusion (`dependabot`,
/// `k8s-ci-robot`, `fossabot`), hour/day/week buckets, star deltas per repo
/// (`dup_repo_name = full_name` filter, `fmin > 0`, `diff > 0`), the
/// 3-month star maximum and the reopen-aware open-issue window.
const DATA: &str = r"
insert into gha_commits values
 ('a1', 1, 'alice', now() - '30 minutes'::interval),
 ('a2', 2, 'bob', now() - '90 minutes'::interval),
 ('a2', 3, 'bob', now() - '95 minutes'::interval),
 ('a3', 4, 'dependabot', now() - '2 hours 30 minutes'::interval),
 ('a4', 5, 'alice', now() - '3 days 12 hours'::interval),
 ('a5', 6, 'carol', now() - '10 days'::interval),
 ('a6', 7, 'k8s-ci-robot', now() - '20 days'::interval),
 ('a7', 8, 'dave', now() - '25 days'::interval),
 ('a8', 9, 'dave', now() - '40 days'::interval),
 ('a9', 10, 'Alice', now() - '5 hours 30 minutes'::interval),
 ('b1', 11, 'eve', now() - '23 hours 30 minutes'::interval),
 ('b2', 12, 'eve', now() - '6 days 12 hours'::interval),
 ('b3', 13, 'eve', now() - '27 days 12 hours'::interval);
insert into gha_texts values
 (1, now() - '1 hour'::interval, 'alice'),
 (1, now() - '1 hour'::interval, 'alice'),
 (2, now() - '2 days'::interval, 'bob'),
 (3, now() - '20 days'::interval, 'fossabot'),
 (4, now() - '20 days'::interval, 'carol'),
 (5, now() - '50 days'::interval, 'carol'),
 (null, now() - '3 hours'::interval, 'nobody');
insert into gha_forkees values
 (1, 1, 'org/p1', 100, 'org/p1', now() - '12 hours'::interval),
 (1, 2, 'org/p1', 110, 'org/p1', now() - '2 hours'::interval),
 (1, 3, 'org/p1', 90, 'org/p1', now() - '3 days'::interval),
 (2, 4, 'org/other', 5, 'org/p1', now() - '2 hours'::interval),
 (3, 5, 'org/x', 7, 'org/x', now() - '20 days'::interval),
 (3, 6, 'org/x', 8, 'org/x', now() - '80 days'::interval),
 (4, 7, 'org/old', 1000, 'org/old', now() - '100 days'::interval),
 (5, 8, 'org/zero', 0, 'org/zero', now() - '1 hour'::interval),
 (5, 9, 'org/zero', 3, 'org/zero', now() - '2 hours'::interval);
insert into gha_issues values
 (1, 1, null, now() - '3 days'::interval, false),
 (1, 2, now() - '2 days'::interval, now() - '2 days'::interval, false),
 (2, 3, null, now() - '3 days'::interval, false),
 (3, 4, now() - '5 days'::interval, now() - '5 days'::interval, false),
 (3, 5, null, now() - '4 days'::interval, false),
 (4, 6, null, now() - '1 day'::interval, true),
 (5, 7, null, now() - '400 days'::interval, false);
";

/// Commits only, no forkees at all: `sum(max(stargazers_count))` is NULL
/// (Go bug 20 — the original tool died with a `Scan` error here).
const DATA_NO_FORKEES: &str = r"
insert into gha_commits values ('c1', 1, 'alice', now() - '30 minutes'::interval);
insert into gha_texts values (1, now() - '1 hour'::interval, 'alice');
insert into gha_issues values (1, 1, null, now() - '3 days'::interval, false);
";

/// Activity by bots only (everything filtered out) plus stale stars.
const DATA_BOTS: &str = r"
insert into gha_commits values
 ('d1', 1, 'dependabot', now() - '30 minutes'::interval),
 ('d2', 2, 'my-bot', now() - '2 days'::interval),
 ('d3', 3, 'k8s-merge-robot', now() - '20 days'::interval),
 ('d4', 4, 'travis-x-bot', now() - '2 hours'::interval),
 ('d5', 5, 'CNCF-CI', now() - '1 hour'::interval);
insert into gha_texts values
 (1, now() - '1 hour'::interval, 'fluxcdbot'),
 (2, now() - '2 days'::interval, 'thing[bot]');
insert into gha_forkees values
 (1, 1, 'org/p', 50, 'org/p', now() - '120 days'::interval);
";

/// Fake `last_tag.sh`: the tag with surrounding white space (trimmed by the
/// tool) when the repository directory exists.
const TAG_SCRIPT: &str = "#!/bin/bash\nif [ -z \"$1\" ]; then echo 'Argument required: repo path'; exit 1; fi\ncd \"$1\" || exit 2\necho \" v1.2.3 \"\n";
/// Prints a tag and fails: the tool keeps the raw (untrimmed) output.
const TAG_SCRIPT_FAIL: &str = "#!/bin/bash\necho 'v9.9.9'\necho 'boom' >&2\nexit 1\n";
/// Chatty on stderr but successful.
const TAG_SCRIPT_STDERR: &str = "#!/bin/bash\necho 'warning: something' >&2\necho 'v2.0.0'\n";
/// Repository-dependent output (the argument is echoed back).
const TAG_SCRIPT_ECHO: &str = "#!/bin/bash\necho \"tag-of-$(basename \"$1\")\"\n";

/// Three projects; `{db:p1}` etc. are replaced by the scratch database
/// names (the tool connects to the database **named after the project**).
const YAML: &str = "projects:
  {db:p1}:
    name: 'Project & <One>'
    psql_db: {db:p1}
    main_repo: org/p1
    status: Graduated
    order: 2
  {db:p2}:
    name: Żółw \"quoted\"
    psql_db: {db:p2}
    main_repo: ''
    status: Incubating
    order: 1
  {db:p3}:
    name: Third
    psql_db: {db:p3}
    main_repo: org/missing
    status: Sandbox
    disabled: true
    order: 3
";

const YAML_DUP_ORDER: &str = "projects:
  {db:p1}:
    name: One
    psql_db: {db:p1}
    main_repo: org/p1
    status: Graduated
    order: 5
  {db:p2}:
    name: Two
    psql_db: {db:p2}
    status: Incubating
    order: 5
";

const YAML_FIVE: &str = "projects:
  {db:p1}:
    name: One
    psql_db: {db:p1}
    main_repo: org/p1
    order: 1
  {db:p2}:
    name: Two
    psql_db: {db:p2}
    main_repo: org/p2
    order: 2
  {db:p3}:
    name: Three
    psql_db: {db:p3}
    main_repo: org/p3
    order: 3
  {db:p4}:
    name: Four
    psql_db: {db:p4}
    main_repo: org/p4
    order: 4
  {db:p5}:
    name: Five
    psql_db: {db:p5}
    main_repo: org/p5
    order: 5
";

/// The two projects with a fixed database name (`gha`, `allprj`).
const YAML_K8S_ALL: &str = "projects:
  kubernetes:
    name: Kubernetes
    psql_db: gha
    main_repo: kubernetes/kubernetes
    status: Graduated
    order: 1
  all:
    name: All CNCF
    psql_db: allprj
    order: 2
  {db:p1}:
    name: One
    psql_db: {db:p1}
    order: 3
";

#[derive(Clone)]
enum Yaml {
    Template(&'static str),
    Real,
    Missing,
}

#[derive(Clone)]
enum TagScript {
    /// No `last_tag.sh` anywhere.
    None,
    /// `./git/last_tag.sh` (needs `GHA2DB_LOCAL_CMD`).
    Local(&'static str),
    /// `bin/last_tag.sh` on `PATH`.
    Path(&'static str),
}

struct Case {
    name: &'static str,
    yaml: Yaml,
    /// Project keys whose databases are created (schema + seed).
    dbs: Vec<&'static str>,
    /// Per-project seed (default: `DATA`; `""` — schema only).
    seeds: BTreeMap<&'static str, &'static str>,
    env: Vec<(&'static str, String)>,
    /// `GHA2DB_DATADIR` mode (files in `datadir/`) instead of `GHA2DB_LOCAL`.
    datadir: bool,
    /// Write `util_sql/exclude_bots.sql` (the real file).
    exclude_bots: bool,
    /// Create the output directory (`jsons/` or the custom `GHA2DB_JSONS_DIR`).
    jsons_dir: Option<&'static str>,
    tag_script: TagScript,
    /// Set `GHA2DB_LOCAL_CMD` (default: with `TagScript::Local`).
    local_cmd: Option<bool>,
    /// Repository directories created under `repos/`.
    repos: Vec<&'static str>,
    /// Compare stderr line by line (not just the count).
    compare_errors: bool,
    /// Compare the per-project JSON files (not with Go's random processing
    /// order and a fatal error in the middle).
    compare_project_files: bool,
}

impl Case {
    fn new(name: &'static str) -> Self {
        Case {
            name,
            yaml: Yaml::Template(YAML),
            dbs: vec!["p1", "p2", "p3"],
            seeds: BTreeMap::from([("p2", ""), ("p3", "")]),
            env: Vec::new(),
            datadir: false,
            exclude_bots: true,
            jsons_dir: Some("jsons"),
            tag_script: TagScript::Local(TAG_SCRIPT),
            local_cmd: None,
            repos: vec!["org/p1"],
            compare_errors: true,
            compare_project_files: true,
        }
    }
    fn yaml(mut self, y: Yaml) -> Self {
        self.yaml = y;
        self
    }
    fn dbs(mut self, dbs: &[&'static str]) -> Self {
        self.dbs = dbs.to_vec();
        self
    }
    fn seed(mut self, proj: &'static str, sql: &'static str) -> Self {
        self.seeds.insert(proj, sql);
        self
    }
    fn env(mut self, k: &'static str, v: &str) -> Self {
        self.env.push((k, v.to_string()));
        self
    }
    fn datadir(mut self) -> Self {
        self.datadir = true;
        self
    }
    fn no_exclude_bots(mut self) -> Self {
        self.exclude_bots = false;
        self
    }
    fn jsons_dir(mut self, d: Option<&'static str>) -> Self {
        self.jsons_dir = d;
        self
    }
    fn tag_script(mut self, t: TagScript) -> Self {
        self.tag_script = t;
        self
    }
    fn local_cmd(mut self, v: bool) -> Self {
        self.local_cmd = Some(v);
        self
    }
    fn repos(mut self, r: &[&'static str]) -> Self {
        self.repos = r.to_vec();
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
    fn skip_project_files(mut self) -> Self {
        self.compare_project_files = false;
        self
    }
}

/// The shared scratch databases of a case.
struct Dbs {
    prefix: String,
    /// Kept alive until the case ends — dropping the handles removes the
    /// databases.
    _dbs: BTreeMap<&'static str, TestDb>,
}

impl Dbs {
    fn create(case: &Case) -> Option<Dbs> {
        let prefix = format!("dbtest_wd_{}_", case.name);
        let schema = fs::read_to_string(fixture("website_data/schema.sql")).unwrap();
        let mut dbs = BTreeMap::new();
        for proj in &case.dbs {
            let db = TestDb::fresh(&format!("wd_{}_{}", case.name, proj))?;
            db.exec(&schema);
            let seed = case.seeds.get(proj).copied().unwrap_or(DATA);
            if !seed.is_empty() {
                db.exec(seed);
            }
            dbs.insert(*proj, db);
        }
        Some(Dbs { prefix, _dbs: dbs })
    }
    fn name(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

struct Side {
    dir: TempDir,
    /// Directory holding the generated JSON files.
    jsons: PathBuf,
    out: Outcome,
    prefix: String,
}

/// Go's `time.Time` JSON form: RFC3339 with up to 9 fractional digits.
fn is_go_rfc3339nano(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return false;
    }
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    if !(digits(0..4)
        && digits(5..7)
        && digits(8..10)
        && digits(11..13)
        && digits(14..16)
        && digits(17..19))
    {
        return false;
    }
    let mut rest = &s[19..];
    if let Some(f) = rest.strip_prefix('.') {
        let n = f.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 || n > 9 || f.as_bytes()[n - 1] == b'0' {
            // Go trims trailing zeros of the fraction.
            return false;
        }
        rest = &f[n..];
    }
    rest == "Z"
        || (rest.len() == 6
            && (rest.starts_with('+') || rest.starts_with('-'))
            && rest.as_bytes()[3] == b':'
            && rest[1..3].bytes().all(|c| c.is_ascii_digit())
            && rest[4..6].bytes().all(|c| c.is_ascii_digit()))
}

/// The UTC offset suffix of a Go RFC3339 time stamp.
fn offset_of(s: &str) -> &str {
    if s.ends_with('Z') {
        "Z"
    } else {
        &s[s.len() - 6..]
    }
}

/// Replace every `"timestamp"` value by `<t>`, returning the originals.
fn mask_timestamps(v: &mut Value, found: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, val) in m.iter_mut() {
                if k == "timestamp" {
                    if let Value::String(s) = val {
                        found.push(s.clone());
                        *val = Value::String("<t>".into());
                        continue;
                    }
                }
                mask_timestamps(val, found);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|x| mask_timestamps(x, found)),
        _ => {}
    }
}

impl Side {
    fn mask(&self, s: &str) -> String {
        s.replace(&self.dir.path().to_string_lossy().to_string(), "<dir>")
            .replace(&self.prefix, "<dbs>_")
    }
    /// All stdout lines (durations, paths and database names masked) as a
    /// sorted multiset.
    fn all_set(&self) -> Vec<String> {
        let mut lines: Vec<String> = self.all_lines();
        lines.sort();
        lines
    }
    fn all_lines(&self) -> Vec<String> {
        self.mask(&mask_go_durations(&self.out.stdout_str()))
            .lines()
            .map(|l| {
                // `GHA2DB_QOUT`: the arguments of the banner's `gha_logs`
                // insert carry `time.Now()` (Go with the monotonic reading).
                if l.starts_with("[1:website_data 2:") {
                    if let (Some(a), Some(b)) = (l.find(" 3:"), l.find(" 4:")) {
                        if a < b {
                            return format!("{}<time>{}", &l[..a + 3], &l[b..]);
                        }
                    }
                }
                l.to_string()
            })
            .collect()
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
    /// Names of the generated JSON files.
    fn files(&self) -> BTreeSet<String> {
        match fs::read_dir(&self.jsons) {
            Ok(rd) => rd
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect(),
            Err(_) => BTreeSet::new(),
        }
    }
    fn text(&self, file: &str) -> String {
        fs::read_to_string(self.jsons.join(file))
            .unwrap_or_else(|e| panic!("{}: {e}", self.jsons.join(file).display()))
    }
    /// Parsed file with the time stamps masked, plus the time stamps.
    fn json(&self, file: &str) -> (Value, Vec<String>) {
        let mut v: Value = serde_json::from_str(&self.text(file))
            .unwrap_or_else(|e| panic!("{file}: invalid JSON: {e}"));
        let mut ts = Vec::new();
        mask_timestamps(&mut v, &mut ts);
        (v, ts)
    }
    /// Parsed file without masking (for value expectations).
    fn value(&self, file: &str) -> Value {
        self.json(file).0
    }
}

fn write_script(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn run_side(bin: &Path, case: &Case, dbs: &Dbs, suffix: &str) -> Side {
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_wd_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    let files = if case.datadir {
        dir.path().join("datadir")
    } else {
        dir.path().to_path_buf()
    };
    fs::create_dir_all(&files).unwrap();
    match &case.yaml {
        Yaml::Template(t) => {
            let mut text = t.to_string();
            while let Some(start) = text.find("{db:") {
                let end = text[start..].find('}').unwrap() + start;
                let key = text[start + 4..end].to_string();
                text.replace_range(start..=end, &dbs.name(&key));
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
    if case.exclude_bots {
        fs::create_dir_all(files.join("util_sql")).unwrap();
        fs::copy(
            fixture("website_data/util_sql/exclude_bots.sql"),
            files.join("util_sql/exclude_bots.sql"),
        )
        .unwrap();
    }
    let jsons = dir.path().join(case.jsons_dir.unwrap_or("jsons"));
    if case.jsons_dir.is_some() {
        fs::create_dir_all(&jsons).unwrap();
    }
    let repos = dir.path().join("repos");
    for r in &case.repos {
        fs::create_dir_all(repos.join(r)).unwrap();
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
        (
            "GHA2DB_REPOS_DIR".into(),
            format!("{}/", repos.to_string_lossy()),
        ),
    ];
    if case.datadir {
        env.push((
            "GHA2DB_DATADIR".into(),
            format!("{}/", files.to_string_lossy()),
        ));
    } else {
        env.push(("GHA2DB_LOCAL".into(), "1".into()));
    }
    let mut local_cmd = matches!(case.tag_script, TagScript::Local(_));
    match &case.tag_script {
        TagScript::None => {}
        TagScript::Local(body) => {
            fs::create_dir_all(dir.path().join("git")).unwrap();
            write_script(&dir.path().join("git/last_tag.sh"), body);
        }
        TagScript::Path(body) => {
            let bin_dir = dir.path().join("bin");
            fs::create_dir_all(&bin_dir).unwrap();
            write_script(&bin_dir.join("last_tag.sh"), body);
            let path = std::env::var("PATH").unwrap_or_default();
            env.push((
                "PATH".into(),
                format!("{}:{}", bin_dir.to_string_lossy(), path),
            ));
        }
    }
    if let Some(v) = case.local_cmd {
        local_cmd = v;
    }
    if local_cmd {
        env.push(("GHA2DB_LOCAL_CMD".into(), "1".into()));
    }
    for (k, v) in &case.env {
        env.retain(|(key, _)| key != k);
        env.push((k.to_string(), v.clone()));
    }
    let mut inv = Invocation::new().cwd(dir.path().to_path_buf());
    for (k, v) in &env {
        inv = inv.env(leak(k), leak(v));
    }
    let out = run(bin, &inv);
    Side {
        dir,
        jsons,
        out,
        prefix: dbs.prefix.clone(),
    }
}

/// Canonical text of a JSON value: the Rust port's pretty printer (sorted
/// keys).
fn canonical(v: &Value) -> String {
    String::from_utf8(to_pretty_json(v).unwrap()).unwrap()
}

/// Compare one JSON file of both sides; returns the masked Rust value.
fn compare_file(go: &Side, rust: &Side, file: &str, ctx: &str) {
    let (gv, gts) = go.json(file);
    let (rv, rts) = rust.json(file);
    assert_eq!(gv, rv, "{file} contents{ctx}");
    assert_eq!(gts.len(), rts.len(), "{file} time stamp count{ctx}");
    for (g, r) in gts.iter().zip(&rts) {
        assert!(is_go_rfc3339nano(g), "go time stamp {g:?} in {file}{ctx}");
        assert!(is_go_rfc3339nano(r), "rust time stamp {r:?} in {file}{ctx}");
        assert_eq!(
            offset_of(g),
            offset_of(r),
            "time stamp offsets in {file}{ctx}"
        );
    }
    // Formatting: identical once the (random) Go key order is normalised.
    let gtext = go.text(file);
    let rtext = rust.text(file);
    let gval: Value = serde_json::from_str(&gtext).unwrap();
    let rval: Value = serde_json::from_str(&rtext).unwrap();
    assert_eq!(
        canonical(&rval),
        rtext,
        "rust {file} is canonically formatted{ctx}"
    );
    assert_eq!(
        canonical(&gval).lines().count(),
        gtext.lines().count(),
        "go {file} line count{ctx}"
    );
    let strip = |t: &str| {
        let mut ls: Vec<String> = t
            .lines()
            .map(|l| {
                let l = if l.contains("\"timestamp\": \"") {
                    "<timestamp line>".to_string()
                } else {
                    l.to_string()
                };
                // `"key": value` lines differ only in position between the
                // sides; compare their multiset (with indentation).
                l.trim_end_matches(',').to_string()
            })
            .collect();
        ls.sort();
        ls
    };
    assert_eq!(strip(&gtext), strip(&rtext), "{file} lines{ctx}");
    assert!(
        !gtext.ends_with('\n'),
        "go {file} has no trailing newline{ctx}"
    );
    assert!(
        !rtext.ends_with('\n'),
        "rust {file} has no trailing newline{ctx}"
    );
}

/// Run both binaries and compare everything; returns the Rust side for
/// further assertions (`None` when the DB tests are skipped).
fn both(case: &Case) -> Option<Side> {
    let dbs = Dbs::create(case)?;
    let rust = run_side(&rust_bin(), case, &dbs, "rs");
    if let Some(go) = go_bin() {
        let go = run_side(&go, case, &dbs, "go");
        let ctx = format!(
            "\ncase {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- go files: {:?}\n--- rust code {:?} stdout:\n{}--- rust stderr:\n{}--- rust files: {:?}",
            case.name,
            case.env,
            go.out.code,
            go.out.stdout_str(),
            go.out.stderr_str(),
            go.files(),
            rust.out.code,
            rust.out.stdout_str(),
            rust.out.stderr_str(),
            rust.files(),
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
        // The banner comes first (when anything is printed), the summary
        // line last on success.
        let (gl, rl) = (go.all_lines(), rust.all_lines());
        assert_eq!(gl.first(), rl.first(), "first line{ctx}");
        if go.out.code == Some(0) {
            assert!(
                gl.last()
                    .is_some_and(|l| l.starts_with("Generated website data in: ")),
                "last go line{ctx}"
            );
            assert_eq!(gl.last(), rl.last(), "last line{ctx}");
        }
        // Files.
        let (gf, rf) = (go.files(), rust.files());
        if case.compare_project_files {
            assert_eq!(gf, rf, "generated files{ctx}");
            for f in &rf {
                compare_file(&go, &rust, f, &ctx);
            }
        } else {
            assert_eq!(
                gf.contains("projects.json"),
                rf.contains("projects.json"),
                "projects.json presence{ctx}"
            );
            if rf.contains("projects.json") {
                compare_file(&go, &rust, "projects.json", &ctx);
            }
        }
    }
    Some(rust)
}

/// `stderr` lines of the Rust side (for expectations).
fn errors(side: &Side) -> Vec<String> {
    side.stderr_set()
}

fn hostname() -> String {
    devstatscode::io::hostname().unwrap()
}

/// Integer at `path` (`a.b.c`) of a JSON value.
fn int(v: &Value, path: &str) -> i64 {
    let mut cur = v;
    for p in path.split('.') {
        cur = match p.parse::<usize>() {
            Ok(i) => &cur[i],
            Err(_) => &cur[p],
        };
    }
    cur.as_i64()
        .unwrap_or_else(|| panic!("{path}: not an integer: {cur}"))
}

fn str_at<'a>(v: &'a Value, path: &str) -> &'a str {
    let mut cur = v;
    for p in path.split('.') {
        cur = match p.parse::<usize>() {
            Ok(i) => &cur[i],
            Err(_) => &cur[p],
        };
    }
    cur.as_str()
        .unwrap_or_else(|| panic!("{path}: not a string: {cur}"))
}

/// The expected `commitGraph` of the `DATA` seed.
fn expected_graph(v: &Value) {
    // Hours: bucket i covers [now-(24-i)h, now-(23-i)h).
    let mut day = [0i64; 24];
    day[23] = 1; // a1 (30 min)
    day[22] = 1; // a2 (90 and 95 min, one sha)
    day[18] = 1; // a9 (5.5 h, `Alice` lower-cased)
    day[0] = 1; // b1 (23.5 h)
    for (i, n) in day.iter().enumerate() {
        assert_eq!(int(v, &format!("commitGraph.day.{i}.0")), i as i64);
        assert_eq!(
            int(v, &format!("commitGraph.day.{i}.1")),
            *n,
            "day bucket {i}"
        );
    }
    // Days: bucket i covers [now-(7-i)d, now-(6-i)d).
    let mut week = [0i64; 7];
    week[6] = 4; // a1, a2, a9, b1
    week[3] = 1; // a4 (3.5 days)
    week[0] = 1; // b2 (6.5 days)
    for (i, n) in week.iter().enumerate() {
        assert_eq!(int(v, &format!("commitGraph.week.{i}.0")), i as i64);
        assert_eq!(
            int(v, &format!("commitGraph.week.{i}.1")),
            *n,
            "week bucket {i}"
        );
    }
    // Weeks: bucket i covers [now-(4-i)w, now-(3-i)w): a7 (25 d) and b3
    // (27.5 d); the bot a6 (20 d); a5 (10 d); the last week.
    let month = [2i64, 0, 1, 6];
    for (i, n) in month.iter().enumerate() {
        assert_eq!(int(v, &format!("commitGraph.month.{i}.0")), i as i64);
        assert_eq!(
            int(v, &format!("commitGraph.month.{i}.1")),
            *n,
            "month bucket {i}"
        );
    }
}

/// Expectations for a project seeded with `DATA`.
fn expect_data_stats(v: &Value, latest_version: &str) {
    expected_graph(v);
    assert_eq!(int(v, "activityTotals.day.commits"), 4);
    assert_eq!(int(v, "activityTotals.week.commits"), 6);
    // Month: 6 + a5 (10 d) + a7 (25 d) + b3 (27.5 d); a8 (40 d) is out.
    assert_eq!(int(v, "activityTotals.month.commits"), 9);
    assert_eq!(int(v, "activityTotals.day.discussion"), 1);
    assert_eq!(int(v, "activityTotals.week.discussion"), 2);
    assert_eq!(int(v, "activityTotals.month.discussion"), 3);
    assert_eq!(int(v, "recentDiscussion"), 3);
    // Stars: org/p1 100→110 within a day (10); with the 3-day 90 row: 20;
    // org/other is not its own repo, org/zero has fmin = 0.
    assert_eq!(int(v, "activityTotals.day.stars"), 10);
    assert_eq!(int(v, "activityTotals.week.stars"), 20);
    assert_eq!(int(v, "activityTotals.month.stars"), 20);
    // 3-month maxima: org/p1 110 + org/x 8 + org/zero 3; org/old is older.
    assert_eq!(int(v, "stars"), 121);
    // Issues: 1 closed, 2 open, 3 reopened, 4 is a PR, 5 open (old).
    assert_eq!(int(v, "openIssues"), 3);
    assert_eq!(str_at(v, "latestVersion"), latest_version);
    assert_eq!(str_at(v, "timestamp"), "<t>");
}

fn expect_empty_stats(v: &Value, latest_version: &str) {
    for i in 0..24 {
        assert_eq!(int(v, &format!("commitGraph.day.{i}.0")), i);
        assert_eq!(int(v, &format!("commitGraph.day.{i}.1")), 0);
    }
    for i in 0..7 {
        assert_eq!(int(v, &format!("commitGraph.week.{i}.1")), 0);
    }
    for i in 0..4 {
        assert_eq!(int(v, &format!("commitGraph.month.{i}.1")), 0);
    }
    for p in ["day", "week", "month"] {
        for m in ["commits", "discussion", "stars"] {
            assert_eq!(int(v, &format!("activityTotals.{p}.{m}")), 0, "{p}.{m}");
        }
    }
    assert_eq!(int(v, "stars"), 0);
    assert_eq!(int(v, "openIssues"), 0);
    assert_eq!(int(v, "recentDiscussion"), 0);
    assert_eq!(str_at(v, "latestVersion"), latest_version);
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

#[test]
fn basic_two_projects() {
    let Some(rust) = both(&Case::new("basic")) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    let host = hostname();
    let dbs = "dbtest_wd_basic_";
    let files = rust.files();
    assert_eq!(
        files,
        BTreeSet::from([
            format!("{dbs}p1.json"),
            format!("{dbs}p2.json"),
            "projects.json".to_string()
        ])
    );
    // projects.json: ordered by `order` (p2 first), the disabled p3 absent.
    let (pj, ts) = rust.json("projects.json");
    assert_eq!(ts.len(), 1);
    assert_eq!(str_at(&pj, "summary"), "all");
    let projects = pj["projects"].as_array().unwrap();
    assert_eq!(projects.len(), 2);
    assert_eq!(str_at(&pj, "projects.0.name"), format!("{dbs}p2"));
    assert_eq!(str_at(&pj, "projects.0.title"), "Żółw \"quoted\"");
    assert_eq!(str_at(&pj, "projects.0.status"), "Incubating");
    assert_eq!(str_at(&pj, "projects.0.repo"), "");
    assert_eq!(
        str_at(&pj, "projects.0.dashboardUrl"),
        format!("https://{dbs}p2.{host}")
    );
    assert_eq!(
        str_at(&pj, "projects.0.dbDumpUrl"),
        format!("https://{host}/{dbs}p2.dump")
    );
    assert_eq!(str_at(&pj, "projects.1.name"), format!("{dbs}p1"));
    assert_eq!(str_at(&pj, "projects.1.title"), "Project & <One>");
    assert_eq!(str_at(&pj, "projects.1.repo"), "org/p1");
    // jsoniter escapes HTML characters.
    assert!(rust
        .text("projects.json")
        .contains("\"title\": \"Project \\u0026 \\u003cOne\\u003e\""));
    // Per-project stats.
    let p1 = rust.value(&format!("{dbs}p1.json"));
    expect_data_stats(&p1, "v1.2.3");
    let p2 = rust.value(&format!("{dbs}p2.json"));
    expect_empty_stats(&p2, "-");
    // Nothing but the banner and the summary line on stdout.
    assert_eq!(
        rust.stdout_set(),
        vec!["Generated website data in: <duration>".to_string()]
    );
    assert!(errors(&rust).is_empty());
}

#[test]
fn single_threaded() {
    let Some(rust) = both(&Case::new("st").env("GHA2DB_ST", "1")) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(
        rust.stdout_set(),
        vec![
            "Generated website data in: <duration>".to_string(),
            "Using single threaded version".to_string()
        ]
    );
    // Bug 21: the single-threaded path used to leave the zero time stamp.
    let (_, ts) = rust.json("dbtest_wd_st_p1.json");
    assert_eq!(ts.len(), 1);
    assert!(ts[0].starts_with("20"), "{ts:?}");
    let p1 = rust.value("dbtest_wd_st_p1.json");
    expect_data_stats(&p1, "v1.2.3");
}

#[test]
fn two_workers_five_projects() {
    let Some(rust) = both(
        &Case::new("ncpus2")
            .yaml(Yaml::Template(YAML_FIVE))
            .dbs(&["p1", "p2", "p3", "p4", "p5"])
            .seed("p2", "")
            .seed("p3", DATA_BOTS)
            .seed("p4", DATA_NO_FORKEES)
            .repos(&["org/p1", "org/p3", "org/p5"])
            .tag_script(TagScript::Local(TAG_SCRIPT_ECHO))
            .env("GHA2DB_NCPUS", "2"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(rust.files().len(), 6);
    let p = |k: &str| rust.value(&format!("dbtest_wd_ncpus2_{k}.json"));
    expect_data_stats(&p("p1"), "tag-of-p1");
    expect_empty_stats(&p("p2"), "tag-of-p2");
    let p3 = p("p3");
    expect_empty_stats(&p3, "tag-of-p3");
    let p4 = p("p4");
    assert_eq!(int(&p4, "activityTotals.day.commits"), 1);
    assert_eq!(int(&p4, "stars"), 0);
    assert_eq!(int(&p4, "openIssues"), 1);
    assert_eq!(int(&p4, "recentDiscussion"), 1);
    expect_data_stats(&p("p5"), "tag-of-p5");
}

#[test]
fn many_workers() {
    let Some(rust) = both(
        &Case::new("ncpus4096")
            .yaml(Yaml::Template(YAML_FIVE))
            .dbs(&["p1", "p2", "p3", "p4", "p5"])
            .seed("p2", DATA)
            .seed("p3", DATA)
            .repos(&["org/p1", "org/p2", "org/p3", "org/p4", "org/p5"])
            .env("GHA2DB_NCPUS", "4096"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(rust.files().len(), 6);
    for k in ["p1", "p2", "p3", "p4", "p5"] {
        expect_data_stats(
            &rust.value(&format!("dbtest_wd_ncpus4096_{k}.json")),
            "v1.2.3",
        );
    }
}

#[test]
fn bots_only_activity() {
    let Some(rust) = both(&Case::new("bots").seed("p1", DATA_BOTS)) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    expect_empty_stats(&rust.value("dbtest_wd_bots_p1.json"), "v1.2.3");
}

/// Go bug 20: `select sum(fmax) …` is NULL without forkees in the last three
/// months — the original tool died with `converting NULL to int`.
#[test]
fn no_forkees_null_stars_sum() {
    let Some(rust) = both(&Case::new("noforkees").seed("p1", DATA_NO_FORKEES)) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    let p1 = rust.value("dbtest_wd_noforkees_p1.json");
    assert_eq!(int(&p1, "stars"), 0);
    assert_eq!(int(&p1, "activityTotals.day.stars"), 0);
    assert_eq!(int(&p1, "activityTotals.day.commits"), 1);
    assert_eq!(int(&p1, "commitGraph.day.23.1"), 1);
    assert_eq!(int(&p1, "openIssues"), 1);
    assert!(errors(&rust).is_empty());
}

#[test]
fn datadir_mode() {
    let Some(rust) = both(&Case::new("datadir").datadir()) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(rust.files().len(), 3);
    expect_data_stats(&rust.value("dbtest_wd_datadir_p1.json"), "v1.2.3");
}

#[test]
fn custom_jsons_dir() {
    let Some(rust) = both(
        &Case::new("jsonsdir")
            .jsons_dir(Some("out/sub"))
            .env("GHA2DB_JSONS_DIR", "out/sub"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(rust.files().len(), 3);
    assert!(!rust.dir.path().join("jsons").exists());
}

#[test]
fn missing_jsons_dir_is_fatal_before_any_database_access() {
    let Some(rust) = both(&Case::new("nojsons").jsons_dir(None)) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        errors(&rust),
        vec!["Error: 'open ./jsons/projects.json: no such file or directory'".to_string()]
    );
    assert!(rust.files().is_empty());
}

#[test]
fn only_filter() {
    let Some(rust) = both(&Case::new("only").env("ONLY", "dbtest_wd_only_p1")) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(
        rust.files(),
        BTreeSet::from([
            "dbtest_wd_only_p1.json".to_string(),
            "projects.json".to_string()
        ])
    );
    let pj = rust.value("projects.json");
    assert_eq!(pj["projects"].as_array().unwrap().len(), 1);
    assert_eq!(str_at(&pj, "projects.0.name"), "dbtest_wd_only_p1");
}

#[test]
fn only_filter_matching_nothing_writes_null_projects() {
    let Some(rust) = both(&Case::new("onlynone").env("ONLY", "nope").dbs(&[])) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(rust.files(), BTreeSet::from(["projects.json".to_string()]));
    let pj = rust.value("projects.json");
    // Go marshals the nil slice as `null`.
    assert!(pj["projects"].is_null(), "{pj}");
    assert_eq!(str_at(&pj, "summary"), "all");
    assert_eq!(rust.text("projects.json").lines().next(), Some("{"));
    assert!(rust
        .text("projects.json")
        .contains("  \"projects\": null,\n  \"summary\": \"all\",\n  \"timestamp\": \""));
}

#[test]
fn projects_override() {
    let Some(rust) = both(
        &Case::new("override")
            .env(
                "GHA2DB_PROJECTS_OVERRIDE",
                "+dbtest_wd_override_p3,-dbtest_wd_override_p1",
            )
            .repos(&["org/p1", "org/missing"]),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(
        rust.files(),
        BTreeSet::from([
            "dbtest_wd_override_p2.json".to_string(),
            "dbtest_wd_override_p3.json".to_string(),
            "projects.json".to_string()
        ])
    );
    let pj = rust.value("projects.json");
    assert_eq!(str_at(&pj, "projects.0.name"), "dbtest_wd_override_p2");
    assert_eq!(str_at(&pj, "projects.1.name"), "dbtest_wd_override_p3");
    assert_eq!(str_at(&pj, "projects.1.status"), "Sandbox");
    expect_empty_stats(&rust.value("dbtest_wd_override_p3.json"), "v1.2.3");
}

/// Bug 19: projects sharing an `order` were dropped by the Go tool.
#[test]
fn duplicate_order_keeps_both_projects() {
    let Some(rust) = both(
        &Case::new("duporder")
            .yaml(Yaml::Template(YAML_DUP_ORDER))
            .dbs(&["p1", "p2"])
            .seed("p2", ""),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(rust.files().len(), 3);
    let pj = rust.value("projects.json");
    assert_eq!(str_at(&pj, "projects.0.name"), "dbtest_wd_duporder_p1");
    assert_eq!(str_at(&pj, "projects.1.name"), "dbtest_wd_duporder_p2");
    // (database names are masked in the compared output)
    assert!(
        rust.stdout_set().contains(
            &"Warning: projects '<dbs>_p1' and '<dbs>_p2' have the same order 5".to_string()
        ),
        "{:?}",
        rust.stdout_set()
    );
}

#[test]
fn missing_projects_yaml() {
    let Some(rust) = both(&Case::new("noyaml").yaml(Yaml::Missing).dbs(&[])) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        errors(&rust),
        vec!["Error: 'open ./projects.yaml: no such file or directory'".to_string()]
    );
    assert!(rust.files().is_empty());
}

#[test]
fn missing_projects_yaml_in_datadir() {
    let Some(rust) = both(&Case::new("noyamldd").yaml(Yaml::Missing).dbs(&[]).datadir()) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    let errs = errors(&rust);
    assert_eq!(errs.len(), 1);
    assert!(
        errs[0].starts_with("Error: 'open <dir>/datadir/projects.yaml: no such file or directory'"),
        "{errs:?}"
    );
}

#[test]
fn custom_projects_yaml_name() {
    let Some(rust) = both(
        &Case::new("customyaml")
            .env("GHA2DB_PROJECTS_YAML", "other.yaml")
            .dbs(&[]),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        errors(&rust),
        vec!["Error: 'open ./other.yaml: no such file or directory'".to_string()]
    );
}

#[test]
fn malformed_projects_yaml() {
    let Some(rust) = both(
        &Case::new("badyaml")
            .yaml(Yaml::Template("projects:\n  - not\n  a: map\n"))
            .dbs(&[])
            .code_only_errors(),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(errors(&rust).len(), 1);
    assert!(rust.files().is_empty());
}

#[test]
fn empty_projects_yaml() {
    let Some(rust) = both(&Case::new("emptyyaml").yaml(Yaml::Template("")).dbs(&[])) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert_eq!(rust.files(), BTreeSet::from(["projects.json".to_string()]));
    assert!(rust.value("projects.json")["projects"].is_null());
}

#[test]
fn missing_exclude_bots_sql_after_projects_json() {
    let Some(rust) = both(&Case::new("nobots").no_exclude_bots()) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        errors(&rust),
        vec!["Error: 'open ./util_sql/exclude_bots.sql: no such file or directory'".to_string()]
    );
    // Written before the failure; no `lib.ReadFile` line without a project.
    assert_eq!(rust.files(), BTreeSet::from(["projects.json".to_string()]));
    assert!(rust.stdout_set().is_empty(), "{:?}", rust.stdout_set());
}

#[test]
fn missing_exclude_bots_sql_with_project_reports_the_fallback() {
    let Some(rust) = both(
        &Case::new("nobotsproj")
            .no_exclude_bots()
            .env("GHA2DB_PROJECT", "dbtest_wd_nobotsproj_p1"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        rust.stdout_set(),
        vec![
            "lib.ReadFile('./util_sql/exclude_bots.sql'): error: open ./util_sql/exclude_bots.sql: no such file or directory"
                .to_string()
        ]
    );
}

#[test]
fn debug_mode_reports_read_files() {
    let Some(rust) = both(&Case::new("debug").env("GHA2DB_DEBUG", "1")) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    assert!(rust
        .stdout_set()
        .contains(&"lib.ReadFile('./util_sql/exclude_bots.sql'): ok".to_string()));
}

#[test]
fn missing_database_is_fatal() {
    let Some(rust) = both(
        &Case::new("nodb")
            .yaml(Yaml::Template(YAML))
            .dbs(&["p2", "p3"])
            .env("GHA2DB_ST", "1")
            .env("ONLY", "dbtest_wd_nodb_p1"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        errors(&rust),
        vec![
            "Error: 'pq: database \"<dbs>_p1\" does not exist'".to_string(),
            "PqError: code=3D000, name=invalid_catalog_name, detail=".to_string(),
        ]
    );
    assert_eq!(rust.files(), BTreeSet::from(["projects.json".to_string()]));
}

#[test]
fn missing_database_among_others() {
    let Some(rust) = both(
        &Case::new("nodbamong")
            .dbs(&["p2"])
            .env("GHA2DB_ST", "1")
            .skip_project_files(),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        errors(&rust),
        vec![
            "Error: 'pq: database \"<dbs>_p1\" does not exist'".to_string(),
            "PqError: code=3D000, name=invalid_catalog_name, detail=".to_string(),
        ]
    );
    assert!(rust.files().contains("projects.json"));
}

/// `kubernetes` → database `gha`, dashboard `k8s.<host>`; `all` → `allprj`.
#[test]
fn kubernetes_and_all_name_mapping() {
    let Some(probe) = TestDb::fresh("wd_k8sall_probe") else {
        return;
    };
    let con_exists = |db: &str| {
        let con = probe.conn();
        let snap = cpg::snapshot(
            &con,
            "select count(*) from pg_database where datname = $1",
            &[devstatscode::pg::SqlArg::from(db)],
        );
        con.close();
        snap.column(0) == ["1"]
    };
    let Some(rust) = both(
        &Case::new("k8sall")
            .yaml(Yaml::Template(YAML_K8S_ALL))
            .dbs(&["p1"])
            .env("GHA2DB_ST", "1")
            .env("ONLY", "kubernetes")
            .skip_project_files(),
    ) else {
        return;
    };
    let host = hostname();
    let pj = rust.value("projects.json");
    assert_eq!(pj["projects"].as_array().unwrap().len(), 1);
    assert_eq!(str_at(&pj, "projects.0.name"), "kubernetes");
    assert_eq!(
        str_at(&pj, "projects.0.dashboardUrl"),
        format!("https://k8s.{host}")
    );
    assert_eq!(
        str_at(&pj, "projects.0.dbDumpUrl"),
        format!("https://{host}/gha.dump")
    );
    if !con_exists("gha") {
        assert_eq!(rust.out.code, Some(2));
        assert!(
            errors(&rust).contains(&"Error: 'pq: database \"gha\" does not exist'".to_string()),
            "{:?}",
            errors(&rust)
        );
    }
    let Some(rust) = both(
        &Case::new("k8sall2")
            .yaml(Yaml::Template(YAML_K8S_ALL))
            .dbs(&["p1"])
            .env("GHA2DB_ST", "1")
            .env("ONLY", "all")
            .skip_project_files(),
    ) else {
        return;
    };
    let pj = rust.value("projects.json");
    assert_eq!(str_at(&pj, "projects.0.name"), "all");
    assert_eq!(
        str_at(&pj, "projects.0.dashboardUrl"),
        format!("https://all.{host}")
    );
    assert_eq!(
        str_at(&pj, "projects.0.dbDumpUrl"),
        format!("https://{host}/allprj.dump")
    );
    if !con_exists("allprj") {
        assert_eq!(rust.out.code, Some(2));
        assert!(
            errors(&rust).contains(&"Error: 'pq: database \"allprj\" does not exist'".to_string()),
            "{:?}",
            errors(&rust)
        );
    }
    // The full projects.json of the three (without the filter).
    let Some(rust) = both(
        &Case::new("k8sall3")
            .yaml(Yaml::Template(YAML_K8S_ALL))
            .dbs(&["p1"])
            .env("GHA2DB_ST", "1")
            .skip_project_files()
            .code_only_errors(),
    ) else {
        return;
    };
    let pj = rust.value("projects.json");
    assert_eq!(pj["projects"].as_array().unwrap().len(), 3);
    assert_eq!(str_at(&pj, "projects.2.name"), "dbtest_wd_k8sall3_p1");
}

#[test]
fn missing_repository_directory_reports_stderr_and_empty_tag() {
    let Some(rust) = both(&Case::new("norepo").repos(&[])) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    let p1 = rust.value("dbtest_wd_norepo_p1.json");
    expect_data_stats(&p1, "");
    let lines = rust.stdout_set();
    assert!(lines.contains(&"STDERR:".to_string()), "{lines:?}");
    assert!(
        lines.contains(
            &"./git/last_tag.sh: line 3: cd: <dir>/repos/org/p1: No such file or directory"
                .to_string()
        ),
        "{lines:?}"
    );
    assert!(lines.contains(&"Command, arguments, environment:".to_string()));
    assert!(lines.contains(&"[./git/last_tag.sh <dir>/repos/org/p1]".to_string()));
    assert!(lines.contains(&"map[GIT_TERMINAL_PROMPT:0]".to_string()));
    assert!(lines.contains(&"Command and arguments:".to_string()));
}

#[test]
fn tag_script_from_path() {
    let Some(rust) = both(&Case::new("pathtag").tag_script(TagScript::Path(TAG_SCRIPT))) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    expect_data_stats(&rust.value("dbtest_wd_pathtag_p1.json"), "v1.2.3");
    assert_eq!(
        rust.stdout_set(),
        vec!["Generated website data in: <duration>".to_string()]
    );
}

#[test]
fn tag_script_missing_locally() {
    let Some(rust) = both(
        &Case::new("nolocaltag")
            .tag_script(TagScript::None)
            .local_cmd(true),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    expect_data_stats(&rust.value("dbtest_wd_nolocaltag_p1.json"), "");
    let lines = rust.stdout_set();
    assert!(
        lines.contains(&"[./git/last_tag.sh <dir>/repos/org/p1]".to_string()),
        "{lines:?}"
    );
    assert!(!lines.iter().any(|l| l.starts_with("STDERR")), "{lines:?}");
}

#[test]
fn tag_script_missing_from_path() {
    let Some(rust) = both(
        &Case::new("nopathtag")
            .tag_script(TagScript::None)
            .env("PATH", "/nonexistent-g2r"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    expect_data_stats(&rust.value("dbtest_wd_nopathtag_p1.json"), "");
    let lines = rust.stdout_set();
    assert!(
        lines.contains(&"[last_tag.sh <dir>/repos/org/p1]".to_string()),
        "{lines:?}"
    );
}

/// A failing script's output is kept untrimmed (Go returns `stdOut.String()`
/// together with the error and only trims on success).
#[test]
fn failing_tag_script_keeps_raw_output() {
    let Some(rust) = both(&Case::new("failtag").tag_script(TagScript::Local(TAG_SCRIPT_FAIL)))
    else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    expect_data_stats(&rust.value("dbtest_wd_failtag_p1.json"), "v9.9.9\n");
    assert!(rust
        .text("dbtest_wd_failtag_p1.json")
        .contains("\"latestVersion\": \"v9.9.9\\n\""));
    let lines = rust.stdout_set();
    assert!(lines.contains(&"v9.9.9".to_string()), "{lines:?}");
    assert!(lines.contains(&"STDERR:".to_string()));
    assert!(lines.contains(&"boom".to_string()));
}

#[test]
fn chatty_stderr_of_a_successful_script_is_silent() {
    let Some(rust) = both(&Case::new("stderrtag").tag_script(TagScript::Local(TAG_SCRIPT_STDERR)))
    else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    expect_data_stats(&rust.value("dbtest_wd_stderrtag_p1.json"), "v2.0.0");
    assert_eq!(
        rust.stdout_set(),
        vec!["Generated website data in: <duration>".to_string()]
    );
}

#[test]
fn cmddebug_one_logs_commands() {
    let Some(rust) = both(&Case::new("cmddebug1").env("GHA2DB_CMDDEBUG", "1")) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    let lines = rust.stdout_set();
    assert!(
        lines.contains(&"./git/last_tag.sh <dir>/repos/org/p1".to_string()),
        "{lines:?}"
    );
    assert!(lines.contains(&"Environment Override: map[GIT_TERMINAL_PROMPT:0]".to_string()));
    assert!(
        lines.contains(&"./git/last_tag.sh <dir>/repos/org/p1 ... <duration>".to_string()),
        "{lines:?}"
    );
}

#[test]
fn cmddebug_two_streams_output() {
    let Some(rust) = both(
        &Case::new("cmddebug2")
            .tag_script(TagScript::Local(TAG_SCRIPT_STDERR))
            .env("GHA2DB_CMDDEBUG", "2")
            .env("GHA2DB_ST", "1"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    expect_data_stats(&rust.value("dbtest_wd_cmddebug2_p1.json"), "v2.0.0");
    let lines = rust.stdout_set();
    assert!(lines.contains(&"v2.0.0".to_string()), "{lines:?}");
    assert!(lines.contains(&"Errors:".to_string()), "{lines:?}");
    assert!(
        lines.contains(&"warning: something".to_string()),
        "{lines:?}"
    );
}

#[test]
fn failing_tag_script_in_streaming_mode_returns_empty_tag() {
    let Some(rust) = both(
        &Case::new("cmddebug2fail")
            .tag_script(TagScript::Local(TAG_SCRIPT_FAIL))
            .env("GHA2DB_CMDDEBUG", "2")
            .env("GHA2DB_ST", "1"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    // Go returns `stdOut.String()` — empty when stdout was streamed.
    expect_data_stats(&rust.value("dbtest_wd_cmddebug2fail_p1.json"), "");
}

#[test]
fn qout_prints_every_query_in_order() {
    let Some(rust) = both(
        &Case::new("qout")
            .env("GHA2DB_QOUT", "1")
            .env("GHA2DB_ST", "1")
            .env("ONLY", "dbtest_wd_qout_p1"),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(0));
    let lines = rust.all_lines();
    // 24 + 7 + 4 graph queries, month commits, 3 discussion, 3 star deltas
    // (the Go tool used to run the month one twice), stars, open issues = 44
    // project queries.
    let selects = lines.iter().filter(|l| l.starts_with("select ")).count();
    assert_eq!(selects, 44, "{lines:#?}");
    assert!(
        lines.iter().any(|l| l.starts_with("PgConnectString: ")),
        "{lines:#?}"
    );
    let commits: Vec<&String> = lines
        .iter()
        .filter(|l| {
            l.starts_with(
                "select count(distinct sha) from gha_commits where dup_created_at >= now() - '",
            )
        })
        .collect();
    assert_eq!(commits.len(), 36);
    assert!(commits[0].starts_with(
        "select count(distinct sha) from gha_commits where dup_created_at >= now() - '24 hours'::interval and dup_created_at < now() - '23 hours'::interval and (lower(dup_actor_login) not like all(array["
    ), "{}", commits[0]);
    assert!(commits[35].starts_with(
        "select count(distinct sha) from gha_commits where dup_created_at >= now() - '1 month'::interval and (lower(dup_actor_login) not like all(array["
    ));
    // Go ⇄ Rust order is compared too (single project, single thread).
    if let Some(go) = go_bin() {
        let dbs = Dbs::create(&Case::new("qout")).unwrap();
        let case = Case::new("qout")
            .env("GHA2DB_QOUT", "1")
            .env("GHA2DB_ST", "1")
            .env("ONLY", "dbtest_wd_qout_p1");
        let g = run_side(&go, &case, &dbs, "go2");
        let r = run_side(&rust_bin(), &case, &dbs, "rs2");
        assert_eq!(g.all_lines(), r.all_lines());
    }
}

#[test]
fn unreachable_server_is_fatal_after_projects_json() {
    let Some(rust) = both(
        &Case::new("noserver")
            .env("PG_PORT", "1")
            .env("PG_HOST", "127.0.0.1")
            .env("GHA2DB_ST", "1")
            .dbs(&[]),
    ) else {
        return;
    };
    assert_eq!(rust.out.code, Some(2));
    assert_eq!(
        errors(&rust),
        vec!["Error: 'dial tcp 127.0.0.1:1: connect: connection refused'".to_string()]
    );
    assert_eq!(rust.files(), BTreeSet::from(["projects.json".to_string()]));
}

#[test]
fn real_projects_yaml() {
    let Some(rust) = both(
        &Case::new("real")
            .yaml(Yaml::Real)
            .dbs(&[])
            .env("GHA2DB_ST", "1")
            .skip_project_files()
            .code_only_errors(),
    ) else {
        return;
    };
    let host = hostname();
    let pj = rust.value("projects.json");
    let projects = pj["projects"].as_array().unwrap();
    assert_eq!(projects.len(), 254);
    assert_eq!(str_at(&pj, "projects.0.name"), "kubernetes");
    assert_eq!(str_at(&pj, "projects.0.title"), "Kubernetes");
    assert_eq!(str_at(&pj, "projects.0.repo"), "kubernetes/kubernetes");
    assert_eq!(
        str_at(&pj, "projects.0.dashboardUrl"),
        format!("https://k8s.{host}")
    );
    assert_eq!(
        str_at(&pj, "projects.0.dbDumpUrl"),
        format!("https://{host}/gha.dump")
    );
    assert_eq!(str_at(&pj, "projects.1.name"), "prometheus");
    assert_eq!(str_at(&pj, "projects.253.name"), "all");
    assert_eq!(
        str_at(&pj, "projects.253.dbDumpUrl"),
        format!("https://{host}/allprj.dump")
    );
    assert!(
        projects
            .iter()
            .all(|p| !p["status"].as_str().unwrap().is_empty()
                || p["name"].as_str().unwrap() == "all")
    );
    let names: BTreeSet<&str> = projects
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 254);
    assert!(!names.contains("opentracing"));
    assert!(names.contains("agones") && names.contains("kaischeduler"));
    assert!(rust.stdout_set().contains(
        &"Warning: projects 'agones' and 'kaischeduler' have the same order 245".to_string()
    ));
    // The first database (`gha`) does not exist here → fatal.
    assert_eq!(rust.out.code, Some(2));
}
