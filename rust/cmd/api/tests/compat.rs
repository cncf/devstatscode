//! Go ⇄ Rust compatibility tests for `api`.
//!
//! Every test creates the scratch databases it needs on the test server —
//! `dbtest_api` (fixture schema + data), `dbtest_api_empty` (schema only) and,
//! through the marker-guarded [`TestDb::fresh_named`], the exact-name
//! databases the server hardcodes (`gha` for the Kubernetes project, `allprj`
//! for the GitHub ID lookups) — then runs each case against the Go server and
//! the Rust server (one after the other, each on a free port, with its own
//! scratch directory holding `projects.yaml` and a fake `calc_metric` on
//! `PATH` that records how it was called). Raw HTTP requests are sent over TCP
//! and compared byte for byte (the `Date` header masked); also compared: the
//! server's stdout (client ports, cache time stamps, the current hour passed
//! to `calc_metric` and the JSON decoder wording masked), the interesting
//! stderr lines, the recorded `calc_metric` invocations and the exit status.
//!
//! All cases are serialized: the fixture databases have fixed names.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use devstats_compat::pg::{db_tests_skipped, test_ctx, TestDb, PG_ENV_VARS};
use devstats_compat::{go_binary, rust_binary, LIB_ENV_PREFIX, TOOL_ENV_VARS};
use serde_json::Value;
use tempfile::TempDir;

static SERIAL: Mutex<()> = Mutex::new(());

fn go_bin() -> Option<PathBuf> {
    go_binary("api")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_api"))
}

// ---------------------------------------------------------------------------
// Fixtures

/// The dashboard tables the API reads (a subset of a real project database).
const SCHEMA: &str = r#"
create table tquick_ranges(quick_ranges_name text, quick_ranges_suffix text);
create table tall_repo_groups(all_repo_group_name text, all_repo_group_value text);
create table trepos(repo_name text, repo_value text);
create table tcountries(country_name text, country_value text);
create table tcompanies(companies_name text);
create table gha_countries(name text, code text);
create table shdev(time timestamp not null default '2020-01-01', series text not null, period text not null default '', name text not null default '', value double precision not null default 0.0);
create table shdev_repos(time timestamp not null default '2020-01-01', series text not null, period text not null default '', name text not null default '', value double precision not null default 0.0);
create table shcom(time timestamp not null default '2020-01-01', series text not null, period text not null default '', name text not null default '', value double precision not null default 0.0);
create table snum_stats(time timestamp not null, series text not null, period text not null default '', value double precision not null default 0.0);
create table scompany_activity(time timestamp not null, series text not null, period text not null default '', "Google" double precision not null default 0.0, "Red Hat" double precision not null default 0.0, "All" double precision not null default 0.0);
create table scntrs_and_orgs(time timestamp not null, series text not null, period text not null default '', value double precision not null default 0.0);
create table sevents_h(time timestamp not null, series text not null, period text not null default '', value double precision not null default 0.0);
create table spstat(time timestamp not null default '2020-01-01', series text not null, period text not null default '', name text not null default '', value double precision not null default 0.0);
create table gha_repos(id bigint not null, name text not null, alias text, repo_group text);
create table gha_repos_langs(repo_name text not null, lang_loc bigint not null default 0);
create table gha_events(id bigint not null, type text not null, actor_id bigint not null, created_at timestamp not null, dup_actor_login text not null);
create table gha_actors(id bigint not null, country_id text);
create table gha_actors_affiliations(actor_id bigint not null, company_name text not null, dt_from timestamp not null, dt_to timestamp not null);
create table gha_commits(event_id bigint not null, author_id bigint, committer_id bigint, dup_actor_login text not null, dup_author_login text not null default '', dup_committer_login text not null default '', dup_created_at timestamp not null);
create table gha_commits_roles(event_id bigint not null, actor_id bigint, actor_login text not null, role text not null, dup_created_at timestamp not null);
create table gha_issues(id bigint not null, event_id bigint not null, number int not null, dup_repo_id bigint not null, is_pull_request boolean not null, dup_actor_login text not null, dup_user_login text not null);
create table gha_pull_requests(event_id bigint not null, number int not null, dup_repo_id bigint not null, dup_actor_login text not null, dup_user_login text not null, dupn_merged_by_login text);
"#;

/// Fixture rows: the tag lists, histogram/time series data for every API and
/// the raw GHA tables the `GithubIDContributions` counts come from.
const DATA: &str = r#"
insert into tquick_ranges values ('Last day','d'),('Last week','w'),('Last month','m'),('Last quarter','q'),('Last year','y'),('Last decade','y10'),('Range v1.0 - v1.1','a_v1_0_v1_1'),('Empty suffix','');
insert into tall_repo_groups values ('All','all'),('Kubernetes','kubernetes'),('Other Group','othergroup'),('Blank','');
insert into trepos values ('org/repo1','org_repo1'),('org/repo2','org_repo2'),('org/blank','');
insert into tcountries values ('Poland','pl'),('United States','us'),('Blankia','');
insert into tcompanies values ('Google'),('Red Hat'),('Independent');
insert into gha_countries values ('Poland','PL'),('United States','US'),('Ünïcödé Land','UL');
insert into shdev(series, period, name, value) values
 ('hdev_commitsallall','d','alice$$$Google',10),('hdev_commitsallall','d','bob$$$Red Hat',7),('hdev_commitsallall','d','carol$$$Independent',6),('hdev_commitsallall','d','alice$$$Independent',2),
 ('hdev_commitsallall','w','alice$$$Google',30),('hdev_commitsallall','w','dave$$$Red Hat',12),
 ('hdev_commitsallpl','d','alice$$$Google',3),
 ('hdev_commitskubernetesall','d','erin$$$Google',5),
 ('hdev_prsallall','d','alice$$$Google',1),
 ('hdev_commitsallall','range:2020-01-01 00:00:00,2020-02-01 00:00:00','manual$$$Google',1);
insert into shdev_repos(series, period, name, value) values
 ('hdev_commitsorg_repo1all','d','alice$$$Google',4),('hdev_commitsorg_repo1all','d','bob$$$Red Hat',3),('hdev_commitsorg_repo1all','d','alice$$$Independent',1),
 ('hdev_commitsorg_repo1pl','d','alice$$$Google',2),
 ('hdev_commitsorg_repo2all','w','zed$$$Google',9),
 ('hdev_commitsorg_repo1all','range:2020-01-01 00:00:00,2020-02-01 00:00:00','manualr$$$Google',1);
insert into shcom(series, period, name, value) values ('hcomcommits','d','Google',100),('hcomcommits','d','Red Hat',50),('hcomcommits','d','Independent',50),('hcomcommits','w','Google',700),('hcomprs','d','Google',1);
insert into snum_stats values ('2020-01-01','nstatsallcomps','d7',5),('2020-01-02','nstatsallcomps','d7',6),('2020-01-03','nstatsallcomps','d7',7),
 ('2020-01-01','nstatsalldevs','d7',50),('2020-01-02','nstatsalldevs','d7',60),
 ('2020-01-01','nstatskubernetescomps','w',1),('2020-01-01','nstatskubernetesdevs','w',2),('2019-12-31','nstatsallcomps','d7',4),('2020-02-01','nstatsallcomps','d7',9);
insert into scompany_activity values ('2020-01-01','companyallcommits','d',10,5,15),('2020-01-02','companyallcommits','d',11,6,17),('2020-01-01','companyallcommits','w',70,35,105),('2020-01-01','companykubernetescommits','d',1,2,3),('2019-12-31','companyallcommits','d',9,4,13);
insert into scntrs_and_orgs values ('2020-01-01','contributors','',100),('2020-02-01','contributors','',150),('2020-01-01','organizations','',10),('2020-02-01','organizations','',12),('2020-03-01','fractional','',1.5);
insert into sevents_h values ('2020-01-01 00:00:00','events_h','',10),('2020-01-01 01:00:00','events_h','',20),('2020-01-02 00:00:00','events_h','',30),('2019-12-31 23:00:00','events_h','',5);
insert into spstat(series, period, name, value) values ('pstatall','y100','Contributors',1000),('pstatall','y100','Contributions',2000),('pstatall','y100','Code committers',300),('pstatall','y100','Commits',4000),('pstatall','y100','Events',50000),('pstatall','y100','Forkers',600),('pstatall','y100','Repositories',70),('pstatall','y100','Stargazers',8000),('pstatall','y100','Other',1),('pstatall','y10','Commits',1);
insert into gha_repos values (1,'org/repo1','org/repo1','Kubernetes'),(2,'org/repo2','org/repo2',''),(3,'org/repo3','org/repo3',null),(4,'org/repo1','org/repo1','Kubernetes'),(5,'solo','solo','Other Group'),(6,'a/b/c','a/b/c','Other Group'),(7,'org/repo4','org/repo4','Other Group');
insert into gha_repos_langs values ('org/repo1',1000),('org/repo1',500),('org/repo2',200),('solo',999),('org/repo4',1);
insert into gha_events values (1,'PushEvent',1,'2020-01-01','Alice'),(2,'IssuesEvent',2,'2020-01-02','bob'),(3,'WatchEvent',3,'2020-01-03','carol'),(4,'PullRequestEvent',1,'2020-01-04','alice'),(5,'IssueCommentEvent',4,'2020-01-05','dave');
insert into gha_actors values (1,'pl'),(2,'us'),(3,'de'),(4,null);
insert into gha_actors_affiliations values (1,'Google','1900-01-01','2100-01-01'),(2,'Red Hat','1900-01-01','2019-06-01'),(2,'Independent','2019-06-01','2100-01-01'),(3,'Unknown','1900-01-01','2100-01-01'),(4,'Acme','1900-01-01','2100-01-01');
insert into gha_commits values (1,1,2,'Alice','alice','bob','2020-01-01'),(6,3,null,'carol','carol','','2020-01-06'),(7,null,4,'dave','','dave','2020-01-07');
insert into gha_commits_roles values (1,2,'bob','Co-authored-by','2020-01-01'),(6,1,'alice','Signed-off-by','2020-01-06'),(8,4,'dave','Co-authored-by','2020-01-08');
insert into gha_issues values (10,2,1,1,false,'bob','bob'),(11,9,2,1,false,'alice','Alice'),(12,4,3,1,true,'alice','alice'),(13,10,3,1,true,'bob','alice'),(14,11,2,2,false,'carol','carol');
insert into gha_pull_requests values (4,3,1,'alice','alice',null),(12,4,1,'bob','bob','alice'),(13,5,2,'carol','carol','bob');
"#;

/// The `projects.yaml` every server reads (`GHA2DB_LOCAL=1` → `./projects.yaml`).
const PROJECTS_YAML: &str = "---
projects:
  kubernetes:
    name: Kubernetes
    psql_db: gha
    order: 1
    disabled: false
  test:
    name: Test Project
    psql_db: dbtest_api
    order: 2
  empty:
    name: Empty Project
    psql_db: dbtest_api_empty
    order: 3
  missing:
    name: Missing DB
    psql_db: dbtest_api_missing
    order: 4
  gone:
    name: Disabled Project
    psql_db: dbtest_api
    disabled: true
    order: 5
";

/// The fake `calc_metric`: records its arguments and the two environment
/// variables the server passes, then sleeps (`G2R_CALC_SLEEP` seconds) and
/// fails (`G2R_CALC_FAIL=1`) on demand.
const CALC_METRIC: &str = r#"#!/bin/sh
{
  printf 'calc_metric'
  for a in "$@"; do printf ' [%s]' "$a"; done
  printf ' PG_DB=%s GHA2DB_PROJECT=%s\n' "${PG_DB-unset}" "${GHA2DB_PROJECT-unset}"
} >> "$G2R_RECORD"
if [ -n "$G2R_CALC_SLEEP" ]; then sleep "$G2R_CALC_SLEEP"; fi
if [ "$G2R_CALC_FAIL" = "1" ]; then
  echo "calc_metric fake stdout"
  echo "calc_metric fake stderr" >&2
  exit 2
fi
echo "calc_metric fake output line 1"
echo "calc_metric fake output line 2"
exit 0
"#;

/// Which optional databases a test needs besides `dbtest_api`.
#[derive(Clone, Copy, Default)]
struct Needs {
    empty: bool,
    gha: bool,
    allprj: bool,
}

/// The scratch databases of one test (dropped when the test ends).
struct Fixtures {
    _main: TestDb,
    _empty: Option<TestDb>,
    _gha: Option<TestDb>,
    _allprj: Option<TestDb>,
}

impl Fixtures {
    /// `None` when the PostgreSQL tests are skipped.
    fn create(needs: Needs) -> Option<Fixtures> {
        let main = TestDb::fresh("api")?;
        main.exec(SCHEMA);
        main.exec(DATA);
        let empty = needs.empty.then(|| {
            let db = TestDb::fresh("api_empty").unwrap();
            db.exec(SCHEMA);
            db
        });
        let full_named = |name: &str| {
            let db = TestDb::fresh_named(name).unwrap();
            db.exec(SCHEMA);
            db.exec(DATA);
            db
        };
        let gha = needs.gha.then(|| full_named("gha"));
        let allprj = needs.allprj.then(|| full_named("allprj"));
        Some(Fixtures {
            _main: main,
            _empty: empty,
            _gha: gha,
            _allprj: allprj,
        })
    }
}

fn write_exec(dir: &Path, name: &str, content: &str) {
    let p = dir.join(name);
    fs::write(&p, content).unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
}

// ---------------------------------------------------------------------------
// Servers

/// What to put into the server's scratch directory as `projects.yaml`.
#[derive(Clone)]
enum Yaml {
    /// The standard [`PROJECTS_YAML`] as `projects.yaml`.
    Standard,
    /// No file at all.
    Missing,
    /// A file with this name and content.
    Custom(String, String),
}

/// How to start a server.
#[derive(Clone)]
struct Config {
    env: Vec<(String, String)>,
    /// Variables to leave unset (the startup checks).
    unset: Vec<String>,
    yaml: Yaml,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            env: Vec::new(),
            unset: Vec::new(),
            yaml: Yaml::Standard,
        }
    }
}

impl Config {
    fn env(mut self, k: &str, v: &str) -> Self {
        self.env.push((k.to_string(), v.to_string()));
        self
    }

    fn unset(mut self, k: &str) -> Self {
        self.unset.push(k.to_string());
        self
    }

    fn yaml(mut self, yaml: Yaml) -> Self {
        self.yaml = yaml;
        self
    }
}

struct Server {
    child: Child,
    port: u16,
    _dir: TempDir,
    record: PathBuf,
    out: PathBuf,
    err: PathBuf,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// `PG_*` connection variables for the servers (the test server, database
/// `dbtest_api` — every handler picks its own database anyway) plus the
/// `*_RO` copies `api` insists on.
fn pg_env() -> Vec<(String, String)> {
    let ctx = test_ctx();
    vec![
        ("PG_HOST".to_string(), ctx.pg_host.clone()),
        ("PG_PORT".to_string(), ctx.pg_port.clone()),
        ("PG_DB".to_string(), "dbtest_api".to_string()),
        ("PG_USER".to_string(), ctx.pg_user.clone()),
        ("PG_PASS".to_string(), ctx.pg_pass.clone()),
        ("PG_SSL".to_string(), ctx.pg_ssl.clone()),
        ("PG_HOST_RO".to_string(), ctx.pg_host.clone()),
        ("PG_USER_RO".to_string(), ctx.pg_user.clone()),
        ("PG_PASS_RO".to_string(), ctx.pg_pass),
    ]
}

fn spawn(bin: &Path, cfg: &Config, tag: &str, port: u16) -> Server {
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_api_{tag}_"))
        .tempdir()
        .unwrap();
    let fakes = dir.path().join("bin");
    fs::create_dir_all(&fakes).unwrap();
    write_exec(&fakes, "calc_metric", CALC_METRIC);
    match &cfg.yaml {
        Yaml::Standard => fs::write(dir.path().join("projects.yaml"), PROJECTS_YAML).unwrap(),
        Yaml::Missing => {}
        Yaml::Custom(name, content) => fs::write(dir.path().join(name), content).unwrap(),
    }
    let record = dir.path().join("record.txt");
    fs::write(&record, "").unwrap();
    let out = dir.path().join("stdout.txt");
    let err = dir.path().join("stderr.txt");

    let mut cmd = Command::new(bin);
    for v in TOOL_ENV_VARS {
        cmd.env_remove(v);
    }
    for v in PG_ENV_VARS {
        cmd.env_remove(v);
    }
    for (k, _) in std::env::vars_os() {
        let k = k.to_string_lossy();
        if k.starts_with(LIB_ENV_PREFIX) || k.starts_with("PG_") || k.starts_with("G2R_") {
            cmd.env_remove(k.as_ref());
        }
    }
    cmd.env(
        "PATH",
        format!(
            "{}:{}",
            fakes.display(),
            std::env::var("PATH").unwrap_or_default()
        ),
    )
    .env("TZ", "UTC")
    .env("G2R_RECORD", &record)
    .env("NO_FATAL_DELAY", "1")
    .env("GHA2DB_SKIPLOG", "1")
    .env("GHA2DB_SKIPTIME", "1")
    .env("GHA2DB_LOCAL", "1")
    .env("GHA2DB_API_HOST", "127.0.0.1")
    .env("GHA2DB_API_PORT", format!(":{port}"));
    for (k, v) in pg_env() {
        cmd.env(k, v);
    }
    for (k, v) in &cfg.env {
        cmd.env(k, v);
    }
    for k in &cfg.unset {
        cmd.env_remove(k);
    }
    cmd.current_dir(dir.path())
        .stdin(Stdio::null())
        .stdout(File::create(&out).unwrap())
        .stderr(File::create(&err).unwrap());
    let child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("cannot spawn {}: {e}", bin.display()));
    Server {
        child,
        port,
        _dir: dir,
        record,
        out,
        err,
    }
}

/// Start `bin` on a free port and wait until it accepts connections.
fn start(bin: &Path, cfg: &Config, tag: &str) -> Server {
    let mut srv = spawn(bin, cfg, tag, free_port());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", srv.port)).is_ok() {
            return srv;
        }
        if let Some(status) = srv.child.try_wait().unwrap() {
            panic!(
                "{} exited early with {status}: stdout={:?} stderr={:?}",
                bin.display(),
                fs::read_to_string(&srv.out).unwrap_or_default(),
                fs::read_to_string(&srv.err).unwrap_or_default()
            );
        }
        assert!(Instant::now() < deadline, "{} did not start", bin.display());
        thread::sleep(Duration::from_millis(20));
    }
}

/// What a case produced on one side.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Captured {
    responses: Vec<String>,
    stdout: String,
    stderr: String,
    record: String,
    /// `Some(code)` / `None` (killed by a signal → see `signal`).
    code: Option<i32>,
    signal: Option<i32>,
}

fn lossy(p: &Path) -> String {
    String::from_utf8_lossy(&fs::read(p).unwrap_or_default()).into_owned()
}

impl Server {
    /// Parse complete HTTP responses out of `buf`; `head` marks a HEAD request
    /// (responses without body). Returns how many complete non-1xx responses
    /// the buffer holds.
    fn complete_responses(buf: &[u8], head: bool) -> usize {
        let mut pos = 0;
        let mut n = 0;
        loop {
            let rest = &buf[pos..];
            let Some(hdr_end) = rest.windows(4).position(|w| w == b"\r\n\r\n") else {
                return n;
            };
            let header = String::from_utf8_lossy(&rest[..hdr_end]).into_owned();
            let mut lines = header.split("\r\n");
            let status: u16 = lines
                .next()
                .and_then(|l| l.split(' ').nth(1))
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            let content_length: usize = lines
                .filter_map(|l| l.split_once(':'))
                .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, v)| v.trim().parse().ok())
                .unwrap_or(0);
            pos += hdr_end + 4;
            if (100..200).contains(&status) {
                continue;
            }
            let body_len = if head || status == 204 || status == 304 {
                0
            } else {
                content_length
            };
            if buf.len() - pos < body_len {
                return n;
            }
            pos += body_len;
            n += 1;
        }
    }

    /// Send one raw request and read one response (or up to EOF).
    fn request(&self, raw: &[u8]) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
        s.write_all(raw).unwrap();
        let head = raw.starts_with(b"HEAD ");
        let mut buf = Vec::new();
        let mut chunk = [0u8; 65536];
        loop {
            if Self::complete_responses(&buf, head) >= 1 {
                break;
            }
            match s.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e) => panic!("read from api: {e}"),
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// Wait (up to 30 s) for the server to exit on its own.
    fn wait_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let status = self.child.wait().unwrap();
                panic!(
                    "server did not exit in time (killed: {status}): stdout={:?} stderr={:?}",
                    lossy(&self.out),
                    lossy(&self.err)
                );
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Collect everything the (already exited) server left behind.
    fn collect(self, responses: Vec<String>, status: ExitStatus) -> Captured {
        Captured {
            responses,
            stdout: lossy(&self.out),
            stderr: lossy(&self.err),
            record: lossy(&self.record),
            code: status.code(),
            signal: status.signal(),
        }
    }

    /// Kill the server and collect everything it left behind.
    fn stop(mut self, responses: Vec<String>) -> Captured {
        let _ = self.child.kill();
        let status = self.child.wait().unwrap();
        self.collect(responses, status)
    }
}

// ---------------------------------------------------------------------------
// Requests

/// A raw HTTP/1.1 request on a closing connection.
fn raw_request(method: &str, path: &str, headers: &[(&str, &str)], body: Option<&[u8]>) -> Vec<u8> {
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    let mut req = req.into_bytes();
    if let Some(b) = body {
        req.extend_from_slice(b);
    }
    req
}

/// `POST /api/v1` with a JSON body.
fn post(body: &str) -> Vec<u8> {
    raw_request(
        "POST",
        "/api/v1",
        &[
            ("Content-Type", "application/json"),
            ("User-Agent", "g2r-compat"),
        ],
        Some(body.as_bytes()),
    )
}

/// `{"api":"<api>","payload":<payload>}`.
fn call(api: &str, payload: &str) -> Vec<u8> {
    post(&format!(r#"{{"api":"{api}","payload":{payload}}}"#))
}

/// One scripted interaction with a running server.
#[derive(Clone)]
enum Step {
    /// Send the request, compare the response byte for byte.
    Req(Vec<u8>),
    /// Send the request, compare the response with its JSON body
    /// canonicalized (object keys sorted, the `projects` list sorted) — for
    /// what Go emits in random map order.
    ReqCanon(Vec<u8>),
    /// Wait (background calculations).
    Sleep(Duration),
}

fn req(api: &str, payload: &str) -> Step {
    Step::Req(call(api, payload))
}

/// Canonical JSON: object keys sorted recursively, a top-level `projects`
/// array sorted.
fn canon(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for k in keys {
                out.insert(k.clone(), canon(&m[k]));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(canon).collect()),
        _ => v.clone(),
    }
}

fn canonical_body(resp: &str) -> String {
    let Some((head, body)) = resp.split_once("\r\n\r\n") else {
        return resp.to_string();
    };
    match serde_json::from_str::<Value>(body.trim_end()) {
        Ok(v) => {
            let mut v = canon(&v);
            if let Some(Value::Array(p)) = v.get_mut("projects") {
                p.sort_by_key(|x| x.to_string());
            }
            format!("{head}\r\n\r\n{}\n", serde_json::to_string(&v).unwrap())
        }
        Err(_) => resp.to_string(),
    }
}

fn drive(srv: Server, steps: &[Step]) -> Captured {
    let mut responses = Vec::new();
    for step in steps {
        match step {
            Step::Req(raw) => responses.push(srv.request(raw)),
            Step::ReqCanon(raw) => responses.push(canonical_body(&srv.request(raw))),
            Step::Sleep(d) => thread::sleep(*d),
        }
    }
    // let late background goroutines finish their logging
    thread::sleep(Duration::from_millis(200));
    srv.stop(responses)
}

// ---------------------------------------------------------------------------
// Normalization

/// Go `ToYMDHDate(time.Now())` for the current, previous and next hour (UTC —
/// the servers run with `TZ=UTC`): the "now" argument `calc_metric` gets.
fn ymdh_candidates() -> Vec<String> {
    use devstatscode::chrono::{Duration as CDuration, Timelike, Utc};
    let now = Utc::now();
    [-1i64, 0, 1]
        .iter()
        .map(|h| {
            let t = now + CDuration::hours(*h);
            format!("{} {}", t.format("%Y-%m-%d"), t.hour())
        })
        .collect()
}

fn mask_ymdh(s: &str, candidates: &[String]) -> String {
    let mut out = s.to_string();
    for c in candidates {
        // a full match only: "2026-09-12 1" must not eat "2026-09-12 13"
        let mut res = String::with_capacity(out.len());
        let mut rest = out.as_str();
        while let Some(pos) = rest.find(c.as_str()) {
            let after = rest[pos + c.len()..].chars().next();
            res.push_str(&rest[..pos]);
            if after.is_some_and(|ch| ch.is_ascii_digit() || ch == ':') {
                res.push_str(c);
            } else {
                res.push_str("<ymdh>");
            }
            rest = &rest[pos + c.len()..];
        }
        res.push_str(rest);
        out = res;
    }
    out
}

/// Replace the digits following `prefix` (a client/server port) with `<port>`.
fn mask_port_after(line: &str, prefix: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    while let Some(pos) = rest.find(prefix) {
        out.push_str(&rest[..pos + prefix.len()]);
        rest = &rest[pos + prefix.len()..];
        let n = rest.chars().take_while(|c| c.is_ascii_digit()).count();
        if n > 0 {
            out.push_str("<port>");
        }
        rest = &rest[n..];
    }
    out.push_str(rest);
    out
}

fn mask_stdout_line(line: &str) -> String {
    let mut l = mask_port_after(line, "IP: 127.0.0.1:");
    // cache entries carry a `time.Now()` value Go prints as
    // `{wall:… ext:… loc:0x…}` (unexported fields) — unreproducible
    if let Some(start) = l.find("{dt:") {
        let tail = &l[start..];
        if let Some(end) = tail.find(" siteStats:").or_else(|| tail.find(" stats:")) {
            l = format!("{}{{dt:<dt>{}", &l[..start], &tail[end..]);
        }
    }
    if let Some(start) = l.find("(age is ") {
        let tail = &l[start + "(age is ".len()..];
        if let Some(end) = tail.find(" < ").or_else(|| tail.find(" >= ")) {
            l = format!("{}(age is <age>{}", &l[..start], &tail[end..]);
        }
    }
    // `returnError` logs the decoder's message too (jsoniter vs serde wording)
    if l.starts_with("API 'unknown': ") {
        l = "API 'unknown': <decode error>".to_string();
    }
    // the only errors on the request-exit line are the JSON decoder's (whose
    // wording is jsoniter's vs serde's) and `unknown API '…'`
    if l.starts_with("Request(exit") {
        if let Some(pos) = l.find(" err:") {
            let e = &l[pos + " err:".len()..];
            if e != "<nil>" && !e.starts_with("unknown API '") {
                l = format!("{} err:<decode error>", &l[..pos]);
            }
        }
    }
    l
}

fn normalize_response(r: &str) -> String {
    let (head, body) = r.split_once("\r\n\r\n").unwrap_or((r, ""));
    let decode_error = body.starts_with("{\"error\":\"API 'unknown': ");
    let mut out = String::new();
    for (i, line) in head.split("\r\n").enumerate() {
        if i > 0 {
            out.push_str("\r\n");
        }
        if line.starts_with("Date: ") {
            out.push_str("Date: <date>");
        } else if decode_error && line.starts_with("Content-Length: ") {
            out.push_str("Content-Length: <n>");
        } else {
            out.push_str(line);
        }
    }
    out.push_str("\r\n\r\n");
    if decode_error {
        out.push_str("{\"error\":\"API 'unknown': <decode error>\"}\n");
    } else {
        out.push_str(body);
    }
    out
}

/// Mask what legitimately differs between two runs/sides; `sorted` compares
/// stdout and the recorded invocations as sorted lines (parallel goroutines).
fn normalize(c: &Captured, sorted: bool) -> Captured {
    let ymdh = ymdh_candidates();
    let responses = c.responses.iter().map(|r| normalize_response(r)).collect();
    let mut stdout_lines: Vec<String> = c
        .stdout
        .split('\n')
        .map(|l| mask_ymdh(&mask_stdout_line(l), &ymdh))
        .collect();
    let mut record_lines: Vec<String> = c.record.split('\n').map(|l| mask_ymdh(l, &ymdh)).collect();
    if sorted {
        // the background runner count in the exit line races with the
        // runner's own start (the same race in both implementations)
        for l in stdout_lines.iter_mut() {
            if l.starts_with("Request(exit") {
                if let Some(pos) = l.find("): ") {
                    *l = format!("Request(exit<bg>{}", &l[pos..]);
                }
            }
        }
        stdout_lines.sort();
        record_lines.sort();
    }
    // stderr: keep the lines both implementations define (Go adds
    // `ErrorType:` lines and goroutine dumps, Rust `thread … panicked` lines).
    let mut stderr = String::new();
    for line in c.stderr.split('\n') {
        let mut l = if line.starts_with("Error(time=") {
            "Error(time=<time>):".to_string()
        } else {
            mask_port_after(line, "listen tcp 127.0.0.1:")
        };
        // the YAML parsers' wording differs (go-yaml vs serde_yaml); the
        // message starts with `yaml: ` (not to be confused with `x.yaml: …`)
        if let Some(pos) = l.find("'yaml: ").or_else(|| l.find(": yaml: ")) {
            let pos = pos + if l[pos..].starts_with('\'') { 1 } else { 2 };
            l = format!("{}yaml: <msg>", &l[..pos]);
        }
        if l.starts_with("Error(time=")
            || l.starts_with("Error: '")
            || l.starts_with("panic: stacktrace: ")
        {
            stderr.push_str(&l);
            stderr.push('\n');
        }
    }
    Captured {
        responses,
        stdout: stdout_lines.join("\n"),
        stderr,
        record: record_lines.join("\n"),
        code: c.code,
        signal: c.signal,
    }
}

fn dump(side: &str, c: &Captured) {
    if std::env::var_os("G2R_DUMP").is_some() {
        eprintln!("===== {side} responses:");
        for (i, r) in c.responses.iter().enumerate() {
            eprintln!("--- #{i}\n{r}");
        }
        eprintln!("===== {side} stdout:\n{}", c.stdout);
        eprintln!("===== {side} stderr:\n{}", c.stderr);
        eprintln!("===== {side} record:\n{}", c.record);
        eprintln!("===== {side} exit: code={:?} signal={:?}", c.code, c.signal);
    }
}

fn compare(name: &str, go: &Captured, rs: &Captured) {
    dump("go", go);
    dump("rust", rs);
    assert_eq!(
        go.responses.len(),
        rs.responses.len(),
        "{name}: number of responses"
    );
    for (i, (g, r)) in go.responses.iter().zip(rs.responses.iter()).enumerate() {
        assert_eq!(g, r, "{name}: response #{i}");
    }
    assert_eq!(go.stdout, rs.stdout, "{name}: stdout");
    assert_eq!(go.stderr, rs.stderr, "{name}: stderr");
    assert_eq!(go.record, rs.record, "{name}: recorded calc_metric calls");
    assert_eq!(go.code, rs.code, "{name}: exit code");
    assert_eq!(go.signal, rs.signal, "{name}: exit signal");
}

/// Run the steps against both servers and compare; returns the (normalized)
/// Rust capture. The caller holds [`SERIAL`] and the fixtures.
fn run_case(name: &str, cfg: &Config, steps: &[Step], sorted: bool) -> Captured {
    let go = go_bin().map(|bin| {
        normalize(
            &drive(start(&bin, cfg, &format!("{name}_go")), steps),
            sorted,
        )
    });
    let rs = normalize(
        &drive(start(&rust_bin(), cfg, &format!("{name}_rs")), steps),
        sorted,
    );
    if let Some(go) = go {
        compare(name, &go, &rs);
    }
    rs
}

/// Start both servers with a configuration that makes them exit at startup
/// (on `port`, or a free one) and compare what they leave behind.
fn run_startup(name: &str, cfg: &Config, port: Option<u16>) -> Captured {
    let one = |bin: &Path, tag: &str| {
        let mut srv = spawn(bin, cfg, tag, port.unwrap_or_else(free_port));
        let status = srv.wait_exit();
        normalize(&srv.collect(Vec::new(), status), false)
    };
    let go = go_bin().map(|bin| one(&bin, &format!("{name}_go")));
    let rs = one(&rust_bin(), &format!("{name}_rs"));
    if let Some(go) = go {
        compare(name, &go, &rs);
    }
    rs
}

/// Start both servers, send them `sig` (a `kill -<sig>` name) and compare how
/// they exit.
fn run_signal(name: &str, sig: &str) -> Captured {
    let cfg = Config::default();
    let one = |bin: &Path, tag: &str| {
        let mut srv = start(bin, &cfg, tag);
        // make sure the server has installed its handlers
        let _ = srv.request(&call("ListAPIs", "{}"));
        let status = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(srv.child.id().to_string())
            .status()
            .unwrap();
        assert!(status.success(), "kill -{sig} failed");
        let status = srv.wait_exit();
        normalize(&srv.collect(Vec::new(), status), false)
    };
    let go = go_bin().map(|bin| one(&bin, &format!("{name}_go")));
    let rs = one(&rust_bin(), &format!("{name}_rs"));
    if let Some(go) = go {
        compare(name, &go, &rs);
    }
    rs
}

fn status_line(resp: &str) -> &str {
    resp.split("\r\n").next().unwrap_or("")
}

fn body(resp: &str) -> &str {
    resp.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("")
}

fn header<'a>(resp: &'a str, name: &str) -> Option<&'a str> {
    resp.split("\r\n\r\n")
        .next()
        .unwrap_or("")
        .split("\r\n")
        .skip(1)
        .filter_map(|l| l.split_once(": "))
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
}

/// Lock the serial guard and create the fixtures; `None` when the DB tests
/// are skipped.
fn setup(needs: Needs) -> Option<(std::sync::MutexGuard<'static, ()>, Fixtures)> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let fx = Fixtures::create(needs)?;
    Some((guard, fx))
}

// ---------------------------------------------------------------------------
// Cases

#[test]
fn lists_health_and_project_lookup() {
    let Some((_g, _fx)) = setup(Needs {
        empty: true,
        gha: true,
        allprj: false,
    }) else {
        return;
    };
    let c = run_case(
        "lists_health",
        &Config::default(),
        &[
            req("ListAPIs", "null"),
            Step::ReqCanon(call("ListProjects", "{}")),
            req("Health", r#"{"project":"test"}"#),
            req("Health", r#"{"project":"Test Project"}"#),
            req("Health", r#"{"project":"dbtest_api"}"#),
            req("Health", r#"{"project":"kubernetes"}"#),
            req("Health", r#"{"project":"Kubernetes"}"#),
            req("Health", r#"{"project":"gha"}"#),
            req("Health", r#"{"project":"empty"}"#),
            req("Health", r#"{"project":"missing"}"#),
            req("Health", r#"{"project":"gone"}"#),
            req("Health", r#"{"project":"Disabled Project"}"#),
            req("Health", r#"{"project":"nope"}"#),
            req("Health", r#"{"project":"TEST"}"#),
            req("Health", r#"{"project":1}"#),
            req("Health", r#"{"project":null}"#),
            req("Health", r#"{"project":["test"]}"#),
            req("Health", "{}"),
            req("Health", "null"),
            Step::Req(post(r#"{"api":"Health"}"#)),
            req("Health", r#"{"projectx":"test"}"#),
            req("Nope", r#"{"project":"test"}"#),
            req("", r#"{"project":"test"}"#),
            req("health", r#"{"project":"test"}"#),
            req(
                "Health",
                r#"{"project":"test","x":[1,2,{"a":null,"b":[]}],"f":1e3,"g":-0.0,"h":12345678901234567890,"i":1.5,"j":"q\"uote","u":"\u2028","t":true,"n":null,"o":{}}"#,
            ),
            req("ListAPIs", r#"{"project":"ignored"}"#),
        ],
        false,
    );
    assert_eq!(status_line(&c.responses[0]), "HTTP/1.1 200 OK");
    assert_eq!(
        header(&c.responses[0], "Content-Type"),
        Some("application/json")
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"apis\":[\"Health\",\"ListAPIs\",\"ListProjects\",\"RepoGroups\",\"Ranges\",\"Countries\",\"Companies\",\"Events\",\"Repos\",\"CumulativeCounts\",\"CompaniesTable\",\"ComContribRepoGrp\",\"DevActCnt\",\"DevActCntComp\",\"ComStatsRepoGrp\",\"SiteStats\",\"GithubIDContributions\"]}\n"
    );
    assert_eq!(
        body(&c.responses[1]),
        "{\"projects\":[\"Empty Project\",\"Kubernetes\",\"Missing DB\",\"Test Project\"]}\n"
    );
    assert_eq!(
        body(&c.responses[2]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"events\":5}\n"
    );
    assert_eq!(
        body(&c.responses[5]),
        "{\"project\":\"kubernetes\",\"db_name\":\"gha\",\"events\":5}\n"
    );
    assert_eq!(status_line(&c.responses[9]), "HTTP/1.1 400 Bad Request");
    assert_eq!(
        body(&c.responses[9]),
        "{\"error\":\"API 'Health': pq: database \\\"dbtest_api_missing\\\" does not exist\"}\n"
    );
    assert_eq!(
        body(&c.responses[10]),
        "{\"error\":\"API 'Health': database not found for project 'gone'\"}\n"
    );
    assert_eq!(
        body(&c.responses[14]),
        "{\"error\":\"API 'Health': 'payload' 'project' field '1' is not a string\"}\n"
    );
    assert_eq!(
        body(&c.responses[17]),
        "{\"error\":\"API 'Health': 'payload' section empty or missing\"}\n"
    );
    assert_eq!(
        body(&c.responses[21]),
        "{\"error\":\"API 'unknown:Nope': unknown API 'Nope'\"}\n"
    );
    assert!(
        c.stdout.contains(
            "Health(exit): project:test db:dbtest_api payload: map[project:test] err:<nil>\n"
        ),
        "{}",
        c.stdout
    );
    assert!(c.stdout.contains("Payload: {API:Health Payload:map[f:1000 g:-0 h:1.2345678901234567e+19 i:1.5 j:q\"uote n:<nil> o:map[] project:test t:true u:\u{2028} x:[1 2 map[a:<nil> b:[]]]]}\n"), "{}", c.stdout);
    assert_eq!(c.stderr, "");

    // GHA2DB_PROJECTS_OVERRIDE enables/disables projects
    let c = run_case(
        "lists_health_override",
        &Config::default().env("GHA2DB_PROJECTS_OVERRIDE", "+gone,-empty"),
        &[
            Step::ReqCanon(call("ListProjects", "{}")),
            req("Health", r#"{"project":"gone"}"#),
            req("Health", r#"{"project":"Disabled Project"}"#),
            req("Health", r#"{"project":"empty"}"#),
            req("Health", r#"{"project":"dbtest_api_empty"}"#),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"projects\":[\"Disabled Project\",\"Kubernetes\",\"Missing DB\",\"Test Project\"]}\n"
    );
    assert_eq!(
        body(&c.responses[1]),
        "{\"project\":\"gone\",\"db_name\":\"dbtest_api\",\"events\":5}\n"
    );
    assert_eq!(
        body(&c.responses[3]),
        "{\"error\":\"API 'Health': database not found for project 'empty'\"}\n"
    );
}

#[test]
fn http_edge_cases_and_malformed_bodies() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let json = [
        ("Content-Type", "application/json"),
        ("User-Agent", "g2r-compat"),
    ];
    let list = br#"{"api":"ListAPIs"}"#;
    let odd_bodies = [
        "",
        " ",
        "null",
        "{}",
        "[]",
        r#"{"api":"Health",}"#,
        r#"{"api":1}"#,
        r#"{"api":"Health","payload":[]}"#,
        r#"{"api":"Health","payload":"x"}"#,
        r#"{"api":"Health","payload":{"project":"test"}} trailing"#,
        r#"{"api":"Health","payload":{"project":"test"}}{"api":"ListAPIs"}"#,
        r#"{"Api":"ListAPIs","PAYLOAD":null}"#,
        r#"{"API":"ListAPIs"}"#,
        r#"{"api":"\u0048ealth","payload":{"project":"test"}}"#,
        r#"{"api":"ListAPIs""#,
        r#""str""#,
        "42",
        r#"{"api":"ListAPIs","api":"Health"}"#,
        r#"{"api":"ListAPIs","extra":{"deep":[1,{"x":"y"}]}}"#,
        r#"{"api":null}"#,
        r#"{"payload":{"project":"test"}}"#,
        "{\"api\":\"ListAPIs\"}\n\n",
        r#"{"api":"Health","payload":{"project":"test\u0000x"}}"#,
        r#"{"api":"Health","payload":{"project":"tést"}}"#,
        "{\"api\":\"Health\",\"payload\":{\"project\":\"\u{FF}\"}}",
    ];
    let mut steps = vec![
        Step::Req(raw_request("GET", "/api/v1", &json, Some(list))),
        Step::Req(raw_request("GET", "/api/v1", &json, None)),
        Step::Req(raw_request("POST", "/api/v1?x=<y>&z=1", &json, Some(list))),
        Step::Req(raw_request("POST", "/api/v1/", &json, Some(list))),
        Step::Req(raw_request("GET", "/other/<x>", &json, None)),
        Step::Req(raw_request("GET", "/", &json, None)),
        Step::Req(raw_request("HEAD", "/api/v1", &json, None)),
        Step::Req(raw_request("PUT", "/api/v1", &json, Some(list))),
        Step::Req(raw_request("DELETE", "/api/v1", &json, None)),
        Step::Req(raw_request(
            "OPTIONS",
            "/api/v1",
            &[
                ("Origin", "http://x.y"),
                ("Access-Control-Request-Method", "POST"),
                ("Access-Control-Request-Headers", "content-type"),
            ],
            None,
        )),
        Step::Req(raw_request(
            "OPTIONS",
            "/api/v1",
            &[("Access-Control-Request-Method", "POST")],
            None,
        )),
        Step::Req(raw_request(
            "OPTIONS",
            "/api/v1",
            &[
                ("Origin", "http://x.y"),
                ("Access-Control-Request-Method", "FOO"),
            ],
            None,
        )),
        Step::Req(raw_request(
            "OPTIONS",
            "/api/v1",
            &[
                ("Origin", "http://x.y"),
                ("Content-Type", "application/json"),
            ],
            Some(list),
        )),
        Step::Req(raw_request(
            "OPTIONS",
            "/nope",
            &[
                ("Origin", "http://x.y"),
                ("Access-Control-Request-Method", "POST"),
            ],
            None,
        )),
        Step::Req(raw_request(
            "POST",
            "/api/v1",
            &[
                ("Origin", "http://x.y"),
                ("Content-Type", "application/json"),
                ("User-Agent", "g2r-compat"),
            ],
            Some(list),
        )),
        Step::Req(raw_request(
            "GET",
            "/zzz",
            &[("Origin", "http://x.y")],
            None,
        )),
        Step::Req(raw_request(
            "POST",
            "/api/v1",
            &[("X-Extra", "1")],
            Some(list),
        )),
        Step::Req(raw_request(
            "POST",
            "/api/v1",
            &[("User-Agent", "a"), ("User-Agent", "b")],
            Some(list),
        )),
        Step::Req(raw_request(
            "POST",
            "/api/v1",
            &[("User-Agent", ""), ("Content-Type", "text/plain")],
            Some(list),
        )),
        Step::Req(raw_request(
            "POST",
            "/api/v1",
            &[(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux) ünïcödé <b>&amp;</b>",
            )],
            Some(list),
        )),
    ];
    for b in odd_bodies {
        steps.push(Step::Req(post(b)));
    }
    let c = run_case("http_edge_cases", &Config::default(), &steps, false);
    let n = c.responses.len();
    let odd = &c.responses[n - odd_bodies.len()..];
    assert_eq!(status_line(&c.responses[0]), "HTTP/1.1 200 OK");
    assert_eq!(
        body(&c.responses[1]),
        "{\"error\":\"API 'unknown': <decode error>\"}\n"
    );
    assert_eq!(status_line(&c.responses[3]), "HTTP/1.1 404 Not Found");
    assert_eq!(body(&c.responses[3]), "404 page not found\n");
    // HEAD reaches the handler: the empty body fails to decode, no body is sent
    assert_eq!(status_line(&c.responses[6]), "HTTP/1.1 400 Bad Request");
    assert_eq!(body(&c.responses[6]), "");
    assert_eq!(status_line(&c.responses[9]), "HTTP/1.1 204 No Content");
    assert_eq!(
        header(&c.responses[9], "Access-Control-Allow-Origin"),
        Some("*")
    );
    assert_eq!(
        header(&c.responses[14], "Access-Control-Allow-Origin"),
        Some("*")
    );
    assert_eq!(header(&c.responses[0], "Access-Control-Allow-Origin"), None);
    assert_eq!(status_line(&odd[0]), "HTTP/1.1 400 Bad Request");
    assert_eq!(
        body(&odd[0]),
        "{\"error\":\"API 'unknown': <decode error>\"}\n"
    );
    // `null` decodes into the zero payload: unknown API ''
    assert_eq!(
        body(&odd[2]),
        "{\"error\":\"API 'unknown:': unknown API ''\"}\n"
    );
    // a trailing document is ignored by the streaming decoder
    assert_eq!(status_line(&odd[9]), "HTTP/1.1 200 OK");
    assert_eq!(status_line(&odd[13]), "HTTP/1.1 200 OK");
    assert!(
        c.stdout
            .contains("Request: IP: 127.0.0.1:<port>, method: POST, path: /api/v1\n"),
        "{}",
        c.stdout
    );
    assert!(
        c.stdout
            .contains("Request: IP: 127.0.0.1:<port>, agent: a, b, method: POST, path: /api/v1\n"),
        "{}",
        c.stdout
    );
    assert!(c.stdout.contains("Request: IP: 127.0.0.1:<port>, agent: Mozilla/5.0 (X11; Linux) ünïcödé <b>&amp;</b>, method: POST, path: /api/v1\n"), "{}", c.stdout);
    assert!(c.stdout.contains("Request(exit): IP: 127.0.0.1:<port>, agent: g2r-compat, method: GET, path: /api/v1 err:<decode error>\n"), "{}", c.stdout);
    assert_eq!(c.stderr, "");
}

#[test]
fn tag_lists() {
    let Some((_g, _fx)) = setup(Needs {
        empty: true,
        ..Needs::default()
    }) else {
        return;
    };
    let c = run_case(
        "tag_lists",
        &Config::default(),
        &[
            req("RepoGroups", r#"{"project":"test"}"#),
            req("RepoGroups", r#"{"project":"test","raw":"1"}"#),
            req("RepoGroups", r#"{"project":"test","raw":1}"#),
            req("RepoGroups", r#"{"project":"test","raw":true}"#),
            req("RepoGroups", r#"{"project":"test","raw":""}"#),
            req("RepoGroups", r#"{"project":"test","raw":null}"#),
            req("Ranges", r#"{"project":"test"}"#),
            req("Ranges", r#"{"project":"test","raw":"yes"}"#),
            req("Countries", r#"{"project":"test"}"#),
            req("Countries", r#"{"project":"test","raw":"y"}"#),
            req("Companies", r#"{"project":"test"}"#),
            req("Companies", r#"{"project":"test","raw":"1"}"#),
            req("RepoGroups", r#"{"project":"empty"}"#),
            req("RepoGroups", r#"{"project":"empty","raw":"1"}"#),
            req("Ranges", r#"{"project":"empty"}"#),
            req("Countries", r#"{"project":"empty"}"#),
            req("Companies", r#"{"project":"empty"}"#),
            req("RepoGroups", r#"{"project":"missing"}"#),
            req("Ranges", r#"{"project":"nope"}"#),
            req("Countries", "{}"),
            req("Companies", r#"{"project":7}"#),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"repo_groups\":[\"All\",\"Kubernetes\",\"Other Group\",\"Blank\"]}\n"
    );
    assert_eq!(
        body(&c.responses[1]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"repo_groups\":[\"all\",\"kubernetes\",\"othergroup\",\"\"]}\n"
    );
    assert_eq!(
        body(&c.responses[2]),
        "{\"error\":\"API 'RepoGroups': 'payload' 'raw' field '1'/float64 is not a string (optional true)\"}\n"
    );
    assert_eq!(
        body(&c.responses[8]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"countries\":[\"Poland\",\"United States\",\"Ünïcödé Land\"]}\n"
    );
    assert_eq!(
        body(&c.responses[10]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"companies\":[\"Google\",\"Red Hat\",\"Independent\"]}\n"
    );
    assert_eq!(
        body(&c.responses[12]),
        "{\"project\":\"empty\",\"db_name\":\"dbtest_api_empty\",\"repo_groups\":null}\n"
    );
    assert_eq!(status_line(&c.responses[17]), "HTTP/1.1 400 Bad Request");
    assert_eq!(c.stderr, "");
}

#[test]
fn repos_and_events() {
    let Some((_g, _fx)) = setup(Needs {
        empty: true,
        ..Needs::default()
    }) else {
        return;
    };
    let c = run_case(
        "repos_events",
        &Config::default(),
        &[
            req("Repos", r#"{"project":"test","repository_group":["All"]}"#),
            req(
                "Repos",
                r#"{"project":"test","repository_group":["Kubernetes","Not specified"]}"#,
            ),
            req(
                "Repos",
                r#"{"project":"test","repository_group":["Kubernetes"]}"#,
            ),
            req(
                "Repos",
                r#"{"project":"test","repository_group":["Other Group"]}"#,
            ),
            req(
                "Repos",
                r#"{"project":"test","repository_group":["Not specified"]}"#,
            ),
            req("Repos", r#"{"project":"test","repository_group":["Nope"]}"#),
            req(
                "Repos",
                r#"{"project":"test","repository_group":["Kubernetes","All"]}"#,
            ),
            req("Repos", r#"{"project":"test","repository_group":[""]}"#),
            req("Repos", r#"{"project":"test","repository_group":[]}"#),
            req("Repos", r#"{"project":"test","repository_group":"All"}"#),
            req(
                "Repos",
                r#"{"project":"test","repository_group":["All",1,null]}"#,
            ),
            req("Repos", r#"{"project":"test","repository_group":[true]}"#),
            req("Repos", r#"{"project":"test"}"#),
            req("Repos", r#"{"project":"empty","repository_group":["All"]}"#),
            req(
                "Repos",
                r#"{"project":"missing","repository_group":["All"]}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01","to":"2020-01-02"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2019-12-31","to":"2020-01-03"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01 00:00:00","to":"2020-01-01 01:00:00"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020","to":"2021-02"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01 5","to":"2021-02"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01T00:00:00Z","to":"2021-02-03 04:05:06"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01T00:00:00","to":"2021"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-1-01","to":"2021"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01","to":"2019-01-01"}"#,
            ),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01","to":"2020-01-01"}"#,
            ),
            req("Events", r#"{"project":"test","from":"2020-01-01"}"#),
            req("Events", r#"{"project":"test","to":"2020-01-01"}"#),
            req("Events", r#"{"project":"test","from":2020,"to":"2021"}"#),
            req("Events", r#"{"project":"test","from":"","to":"2021"}"#),
            req("Events", r#"{"project":"test","from":"now","to":"2021"}"#),
            req(
                "Events",
                r#"{"project":"test","from":"2020-01-01","to":"2021-01-01T00:00:00Z"}"#,
            ),
            req(
                "Events",
                r#"{"project":"empty","from":"2020-01-01","to":"2021-01-01"}"#,
            ),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"repo_groups\":[\"Kubernetes\",\"Not specified\",\"Not specified\",\"Other Group\"],\"repos\":[\"org/repo1\",\"org/repo2\",\"org/repo3\",\"org/repo4\"]}\n"
    );
    assert_eq!(
        body(&c.responses[8]),
        "{\"error\":\"API 'Repos': 'payload' 'repository_group' field '[]' cannot be empty (optional false, allow empty false)\"}\n"
    );
    assert_eq!(
        body(&c.responses[10]),
        "{\"error\":\"API 'Repos': 'payload' 'repository_group' field '[All 1 \\u003cnil\\u003e]' #3 item '\\u003cnil\\u003e'/\\u003cnil\\u003e is not a string (optional false, allow empty false)\"}\n"
    );
    assert_eq!(
        body(&c.responses[15]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"timestamps\":[\"2020-01-01T00:00:00Z\",\"2020-01-01T01:00:00Z\"],\"from\":\"2020-01-01\",\"to\":\"2020-01-02\",\"values\":[10,20]}\n"
    );
    assert_eq!(
        body(&c.responses[21]),
        "{\"error\":\"API 'Events': cannot parse datetime: '2020-01-01T00:00:00'\"}\n"
    );
    assert_eq!(c.stderr, "");
}

#[test]
fn cumulative_counts_and_site_stats() {
    let Some((_g, _fx)) = setup(Needs {
        empty: true,
        ..Needs::default()
    }) else {
        return;
    };
    let c = run_case(
        "cumulative_site_stats",
        &Config::default(),
        &[
            req(
                "CumulativeCounts",
                r#"{"project":"test","metric":"contributors"}"#,
            ),
            req(
                "CumulativeCounts",
                r#"{"project":"test","metric":"contributors"}"#,
            ),
            req(
                "CumulativeCounts",
                r#"{"project":"test","metric":"organizations"}"#,
            ),
            req(
                "CumulativeCounts",
                r#"{"project":"test","metric":"fractional"}"#,
            ),
            req(
                "CumulativeCounts",
                r#"{"project":"test","metric":"nothing"}"#,
            ),
            req(
                "CumulativeCounts",
                r#"{"project":"test","metric":"Contributors"}"#,
            ),
            req("CumulativeCounts", r#"{"project":"test","metric":""}"#),
            req(
                "CumulativeCounts",
                r#"{"project":"test","metric":["contributors"]}"#,
            ),
            req("CumulativeCounts", r#"{"project":"test"}"#),
            req(
                "CumulativeCounts",
                r#"{"project":"Test Project","metric":"contributors"}"#,
            ),
            req(
                "CumulativeCounts",
                r#"{"project":"empty","metric":"contributors"}"#,
            ),
            req(
                "CumulativeCounts",
                r#"{"project":"missing","metric":"contributors"}"#,
            ),
            req("SiteStats", r#"{"project":"test"}"#),
            req("SiteStats", r#"{"project":"test"}"#),
            req("SiteStats", r#"{"project":"dbtest_api"}"#),
            req("SiteStats", r#"{"project":"empty"}"#),
            req("SiteStats", r#"{"project":"nope"}"#),
            req("SiteStats", "{}"),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"project\":\"test\",\"metric\":\"contributors\",\"db_name\":\"dbtest_api\",\"timestamps\":[\"2020-01-01T00:00:00Z\",\"2020-02-01T00:00:00Z\"],\"values\":[100,150]}\n"
    );
    assert_eq!(body(&c.responses[0]), body(&c.responses[1]));
    assert!(
        c.stdout.contains("CumulativeCounts: using cached values for [test dbtest_api contributors] (age is <age> < 43200)\n"),
        "{}",
        c.stdout
    );
    assert_eq!(status_line(&c.responses[3]), "HTTP/1.1 400 Bad Request");
    assert_eq!(
        body(&c.responses[12]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"contributors\":1000,\"contributions\":2000,\"boc\":1701,\"committers\":300,\"commits\":4000,\"events\":50000,\"forkers\":600,\"repositories\":70,\"stargazers\":8000,\"countries\":3,\"companies\":2}\n"
    );
    assert_eq!(body(&c.responses[12]), body(&c.responses[13]));
    assert!(
        c.stdout.contains("SiteStats: using cached value for [test dbtest_api]: {dt:<dt> siteStats:{Project:test DB:dbtest_api Contributors:1000 Contributions:2000 BOC:1701 Committers:300 Commits:4000 Events:50000 Forkers:600 Repositories:70 Stargazers:8000 Countries:3 Companies:2}} (age is <age> < 43200)\n"),
        "{}",
        c.stdout
    );
    assert_eq!(status_line(&c.responses[15]), "HTTP/1.1 400 Bad Request");
    assert_eq!(c.stderr, "");

    // all four SiteStats queries fail in parallel on a missing database: the
    // SQL dumps come in random order
    let c = run_case(
        "site_stats_missing_db",
        &Config::default(),
        &[
            req("SiteStats", r#"{"project":"missing"}"#),
            req("SiteStats", r#"{"project":"missing"}"#),
        ],
        true,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"error\":\"API 'SiteStats': pq: database \\\"dbtest_api_missing\\\" does not exist\"}\n"
    );
}

#[test]
fn github_id_contributions() {
    let Some((_g, _fx)) = setup(Needs {
        allprj: true,
        ..Needs::default()
    }) else {
        return;
    };
    let c = run_case(
        "github_id",
        &Config::default(),
        &[
            req(
                "GithubIDContributions",
                r#"{"project":"test","github_id":"alice"}"#,
            ),
            req(
                "GithubIDContributions",
                r#"{"project":"test","github_id":"Alice"}"#,
            ),
            req(
                "GithubIDContributions",
                r#"{"project":"test","github_id":"alice"}"#,
            ),
            req("GithubIDContributions", r#"{"github_id":"bob"}"#),
            req("GithubIDContributions", r#"{"github_id":"carol"}"#),
            req("GithubIDContributions", r#"{"github_id":"dave"}"#),
            req("GithubIDContributions", r#"{"github_id":"nobody"}"#),
            req("GithubIDContributions", r#"{"github_id":""}"#),
            req("GithubIDContributions", r#"{"github_id":7}"#),
            req("GithubIDContributions", r#"{"github_id":null}"#),
            req("GithubIDContributions", r#"{"project":"test"}"#),
            req("GithubIDContributions", "{}"),
            Step::Req(post(r#"{"api":"GithubIDContributions"}"#)),
            req(
                "GithubIDContributions",
                r#"{"project":"empty","github_id":"alice"}"#,
            ),
            req(
                "GithubIDContributions",
                r#"{"project":"nope","github_id":"BOB"}"#,
            ),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"contributions\":6,\"issues\":1,\"prs\":2}\n"
    );
    assert_eq!(
        body(&c.responses[1]),
        "{\"contributions\":6,\"issues\":1,\"prs\":2}\n"
    );
    assert!(
        c.stdout.contains("GithubIDContributions: using cached value for alice: {dt:<dt> stats:[6 1 2]} (age is <age> < 86400)\n"),
        "{}",
        c.stdout
    );
    assert_eq!(
        body(&c.responses[6]),
        "{\"contributions\":0,\"issues\":0,\"prs\":0}\n"
    );
    assert_eq!(
        body(&c.responses[7]),
        "{\"error\":\"API 'GithubIDContributions': github_id parameter must be set\"}\n"
    );
    assert!(
        c.stdout.contains("GithubIDContributions(exit): project:all db:allprj payload: map[github_id:alice project:test] err:<nil>\n"),
        "{}",
        c.stdout
    );
    assert_eq!(c.stderr, "");
}

#[test]
fn github_id_contributions_without_allprj() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    // the three counting queries fail in parallel: SQL dumps in random order
    let c = run_case(
        "github_id_no_allprj",
        &Config::default(),
        &[
            req("GithubIDContributions", r#"{"github_id":"alice"}"#),
            req("GithubIDContributions", r#"{"github_id":"alice"}"#),
        ],
        true,
    );
    assert_eq!(status_line(&c.responses[0]), "HTTP/1.1 400 Bad Request");
    assert_eq!(
        body(&c.responses[0]),
        "{\"error\":\"API 'GithubIDContributions': pq: database \\\"allprj\\\" does not exist\"}\n"
    );
}

#[test]
fn companies_table_and_company_apis() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let c = run_case(
        "companies",
        &Config::default(),
        &[
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Last day","metric":"Commits"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Last week","metric":"Commits"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Last day","metric":"Pull requests"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Last month","metric":"Commits"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Last day","metric":"Nope"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Last day","metric":"commits"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Nope","metric":"Commits"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"Empty suffix","metric":"Commits"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":"range:2020,2021","metric":"Commits"}"#,
            ),
            req("CompaniesTable", r#"{"project":"test","range":"Last day"}"#),
            req("CompaniesTable", r#"{"project":"test","metric":"Commits"}"#),
            req(
                "CompaniesTable",
                r#"{"project":"test","range":1,"metric":"Commits"}"#,
            ),
            req(
                "CompaniesTable",
                r#"{"project":"missing","range":"Last day","metric":"Commits"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"7 Days MA","repository_group":"All"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"d7","repository_group":"All"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Week","repository_group":"Kubernetes"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","repository_group":"All"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-02","to":"2020-01-03","period":"7 Days MA","repository_group":"All"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Week","repository_group":"Nope"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Week","repository_group":"Blank"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"bad","to":"2020-02-01","period":"Week","repository_group":"All"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Nope","repository_group":"All"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Week"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","period":"Week","repository_group":"All"}"#,
            ),
            req(
                "ComContribRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Week","repository_group":["All"]}"#,
            ),
            Step::ReqCanon(call(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":["All"]}"#,
            )),
            Step::ReqCanon(call(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":["Google","Red Hat"]}"#,
            )),
            Step::ReqCanon(call(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":["Google"]}"#,
            )),
            Step::ReqCanon(call(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2019-12-01","to":"2020-02-01","period":"Week","metric":"Commits","repository_group":"All","companies":["All"]}"#,
            )),
            Step::ReqCanon(call(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"Kubernetes","companies":["All"]}"#,
            )),
            Step::ReqCanon(call(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":["Nope"]}"#,
            )),
            Step::ReqCanon(call(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":["Google","Nope"]}"#,
            )),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":[]}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":"All"}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":["All",2]}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All"}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Year","metric":"Nope","repository_group":"All","companies":["All"]}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Nope","metric":"Commits","repository_group":"All","companies":["All"]}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"Nope","companies":["All"]}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-01-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"Blank","companies":["All"]}"#,
            ),
            req(
                "ComStatsRepoGrp",
                r#"{"project":"test","from":"2020-13-01","to":"2020-02-01","period":"Day","metric":"Commits","repository_group":"All","companies":["All"]}"#,
            ),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"Last day\",\"metric\":\"Commits\",\"rank\":[0,1,2],\"company\":[\"Google\",\"Red Hat\",\"Independent\"],\"number\":[100,50,50]}\n"
    );
    assert_eq!(
        body(&c.responses[13]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"period\":\"7 Days MA\",\"repository_group\":\"All\",\"companies\":[5,6,7],\"developers\":[50,60],\"companies_timestamps\":[\"2020-01-01T00:00:00Z\",\"2020-01-02T00:00:00Z\",\"2020-01-03T00:00:00Z\"],\"developers_timestamps\":[\"2020-01-01T00:00:00Z\",\"2020-01-02T00:00:00Z\"]}\n"
    );
    assert_eq!(
        body(&c.responses[25]),
        "{\"companies\":[\"All\"],\"db_name\":\"dbtest_api\",\"from\":\"2020-01-01\",\"metric\":\"Commits\",\"period\":\"Day\",\"project\":\"test\",\"repository_group\":\"All\",\"timestamps\":[\"2020-01-01T00:00:00Z\",\"2020-01-02T00:00:00Z\"],\"to\":\"2020-02-01\",\"values\":[{\"All\":15,\"Google\":10,\"Red Hat\":5},{\"All\":17,\"Google\":11,\"Red Hat\":6}]}\n"
    );
    assert_eq!(status_line(&c.responses[32]), "HTTP/1.1 400 Bad Request");
    assert_eq!(c.stderr, "");
}

/// DevActCnt payload on the `test` project, `range` + `metric` + the rest.
fn dev_act(range: &str, metric: &str, rest: &str) -> Step {
    req(
        "DevActCnt",
        &format!(r#"{{"project":"test","range":"{range}","metric":"{metric}",{rest}}}"#),
    )
}

#[test]
fn dev_act_cnt() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let all = r#""repository_group":"All","github_id":"","country":"All""#;
    let c = run_case(
        "dev_act_cnt",
        &Config::default(),
        &[
            dev_act("Last day", "Commits", all),
            dev_act("Last week", "Commits", all),
            dev_act("Last month", "Commits", all),
            dev_act("Last quarter", "Commits", all),
            dev_act("Last day", "PRs", all),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","country":"All","github_id":"alice""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","country":"All","github_id":"bob""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","country":"All","github_id":"Alice""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","country":"All","github_id":"nobody""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"Poland""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"United States""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"Kubernetes","github_id":"","country":"All""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"Other Group","github_id":"","country":"All""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"Nope","github_id":"","country":"All""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"Blank","github_id":"","country":"All""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"all","github_id":"","country":"All""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"Nowhere""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"Blankia""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"all""#,
            ),
            dev_act("Last day", "Approves", all),
            dev_act("Last day", "Reviews", all),
            dev_act("Last day", "fractions", all),
            dev_act("Last day", "commits", all),
            dev_act("Nope", "Commits", all),
            dev_act("Empty suffix", "Commits", all),
            dev_act("range:2020-01-01,2020-02", "Commits", all),
            dev_act(
                "range:2020-01-01 00:00:00,2020-02-01 00:00:00",
                "Commits",
                all,
            ),
            dev_act("range:2020-03-01,2020-04", "Commits", all),
            dev_act("range:2020-03-01,2020-04", "Commits", all),
            dev_act("range:2020,2021", "Commits", all),
            dev_act("range:2020-03-01", "Commits", all),
            dev_act("range:2020-03-01,2020-04,2020-05", "Commits", all),
            dev_act("range:2020-03-01,bad", "Commits", all),
            dev_act("range:bad,2020-04", "Commits", all),
            dev_act("range:2030-03-01,2031", "Commits", all),
            dev_act("range:2020-03-01,2020-01", "Commits", all),
            dev_act("range:2020-03-01,2020-03-01", "Commits", all),
            dev_act("range:2020-03-01,2020-03-01 1", "Commits", all),
            dev_act("range:", "Commits", all),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"All","repository":"org/repo1""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"All","bg":"1""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"All","bg":1"#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","country":"All""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"""#,
            ),
            dev_act("Last day", "Commits", r#""github_id":"","country":"All""#),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":1,"country":"All""#,
            ),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":["All"],"github_id":"","country":"All""#,
            ),
            req(
                "DevActCnt",
                r#"{"project":"test","metric":"Commits","repository_group":"All","github_id":"","country":"All"}"#,
            ),
            req(
                "DevActCnt",
                r#"{"project":"test","range":"Last day","repository_group":"All","github_id":"","country":"All"}"#,
            ),
            req(
                "DevActCnt",
                r#"{"project":"missing","range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All"}"#,
            ),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"All\",\"github_id\":\"\",\"filter\":\"series:hdev_commitsallall period:d\",\"rank\":[1,2,3],\"login\":[\"alice\",\"bob\",\"carol\"],\"number\":[12,7,6]}\n"
    );
    assert_eq!(
        body(&c.responses[5]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"All\",\"github_id\":\"alice\",\"filter\":\"series:hdev_commitsallall period:d github_id:alice\",\"rank\":[1],\"login\":[\"alice\"],\"number\":[12]}\n"
    );
    assert_eq!(
        body(&c.responses[8]),
        "{\"error\":\"API 'DevActCnt': github_id 'nobody' not found in results\"}\n"
    );
    assert_eq!(
        body(&c.responses[9]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"Poland\",\"github_id\":\"\",\"filter\":\"series:hdev_commitsallpl period:d\",\"rank\":[1],\"login\":[\"alice\"],\"number\":[3]}\n"
    );
    assert_eq!(
        body(&c.responses[2]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"Last month\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"All\",\"github_id\":\"\",\"filter\":\"series:hdev_commitsallall period:m\",\"rank\":null,\"login\":null,\"number\":null}\n"
    );
    assert_eq!(
        body(&c.responses[19]),
        "{\"error\":\"API 'DevActCnt': invalid metric value: 'Approves'\"}\n"
    );
    assert_eq!(
        body(&c.responses[25]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"range:2020-01-01,2020-02\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"All\",\"github_id\":\"\",\"filter\":\"series:hdev_commitsallall period:range:2020-01-01 00:00:00,2020-02-01 00:00:00\",\"rank\":[1],\"login\":[\"manual\"],\"number\":[1]}\n"
    );
    assert_eq!(
        body(&c.responses[30]),
        "{\"error\":\"API 'DevActCnt': range should be specified as 'range:YYYY[-MM[-DD [HH[-MM[-SS]]]]],YYYY[-MM[-DD [HH[-MM[-SS]]]]]'\"}\n"
    );
    assert_eq!(
        body(&c.responses[32]),
        "{\"error\":\"API 'DevActCnt': cannot parse datetime: 'bad'\"}\n"
    );
    assert!(
        body(&c.responses[34]).starts_with("{\"error\":\"API 'DevActCnt': from (2030-03-01 00:00:00) and to (2031-01-01 00:00:00) dates must not be after "),
        "{}",
        body(&c.responses[34])
    );
    // manual ranges without data run calc_metric (the fake); repeats re-run it
    assert_eq!(
        c.record,
        "calc_metric [multi_row_single_column] [/etc/gha2db/metrics/test/project_developer_stats.sql] [<ymdh>] [<ymdh>] [range:2020-03-01 00:00:00,2020-04-01 00:00:00] [hist,merge_series:hdev] PG_DB=dbtest_api GHA2DB_PROJECT=test\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/test/project_developer_stats.sql] [<ymdh>] [<ymdh>] [range:2020-03-01 00:00:00,2020-04-01 00:00:00] [hist,merge_series:hdev] PG_DB=dbtest_api GHA2DB_PROJECT=test\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/test/project_developer_stats.sql] [<ymdh>] [<ymdh>] [range:2020-01-01 00:00:00,2021-01-01 00:00:00] [hist,merge_series:hdev] PG_DB=dbtest_api GHA2DB_PROJECT=test\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/test/project_developer_stats.sql] [<ymdh>] [<ymdh>] [range:2020-03-01 00:00:00,2020-03-01 01:00:00] [hist,merge_series:hdev] PG_DB=dbtest_api GHA2DB_PROJECT=test\n"
    );
    assert!(
        c.stdout.contains("Calculated manually:\ncalc_metric fake output line 1\ncalc_metric fake output line 2\n"),
        "{}",
        c.stdout
    );
    assert_eq!(c.stderr, "");
}

#[test]
fn dev_act_cnt_kubernetes_and_repositories() {
    let Some((_g, _fx)) = setup(Needs {
        gha: true,
        ..Needs::default()
    }) else {
        return;
    };
    let k8s = |api: &str, rest: &str| req(api, &format!(r#"{{"project":"kubernetes",{rest}}}"#));
    let c = run_case(
        "dev_act_cnt_k8s",
        &Config::default(),
        &[
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Approves","repository_group":"All","github_id":"","country":"All""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Reviews","repository_group":"All","github_id":"","country":"All""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"range:2020-03-01,2020-04","metric":"Approves","repository_group":"All","github_id":"","country":"All""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"range:2020-03-01,2020-04","metric":"Reviews","repository_group":"All","github_id":"","country":"All""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","repository":"org/repo1""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","repository":"org/nope""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","repository":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","repository":"org/blank""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","country":"All","repository":"org/repo1","github_id":"alice""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","country":"All","repository":"org/repo1","github_id":"nobody""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","country":"Poland","repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last week","metric":"Commits","country":"All","repository":"org/repo2","github_id":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","country":"All","repository":"org/repo1""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Commits","country":"All","repository":7,"github_id":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"Last day","metric":"Approves","country":"All","repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"range:2020-01-01,2020-02","metric":"Commits","country":"All","repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"range:2020-05-01,2020-06","metric":"Reviews","country":"All","repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCnt",
                r#""range":"range:2020-05-01,2020-06","metric":"Approves","country":"All","repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","github_id":"","country":"All","companies":["All"],"repository":"org/repo1""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","github_id":"","country":"All","companies":["Google","Red Hat"],"repository":"org/repo1""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","github_id":"","country":"All","companies":["Nope"],"repository":"org/repo1""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","country":"All","companies":["Google"],"repository":"org/repo1","github_id":"alice""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","country":"All","companies":["Red Hat"],"repository":"org/repo1","github_id":"alice""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","country":"All","companies":[],"repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","country":"All","repository":"org/repo1","github_id":"""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"Last day","metric":"Commits","country":"All","companies":["All"],"repository":"org/nope","github_id":"""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"range:2020-01-01,2020-02","metric":"Commits","github_id":"","country":"All","companies":["All"],"repository":"org/repo1""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"range:2020-05-01,2020-06","metric":"Reviews","github_id":"","country":"All","companies":["All"],"repository":"org/repo1""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"range:2020-05-01,2020-06","metric":"Approves","github_id":"","country":"All","companies":["All"],"repository":"org/repo1""#,
            ),
            k8s(
                "DevActCntComp",
                r#""range":"range:2020-05-01,2020-06","metric":"Approves","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[5]),
        "{\"project\":\"kubernetes\",\"db_name\":\"gha\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository\":\"org/repo1\",\"country\":\"All\",\"github_id\":\"\",\"filter\":\"series:hdev_commitsorg_repo1all period:d\",\"rank\":[1,2],\"login\":[\"alice\",\"bob\"],\"number\":[5,3]}\n"
    );
    assert_eq!(
        body(&c.responses[6]),
        "{\"error\":\"API 'DevActCnt': invalid repository name: 'org/nope'\"}\n"
    );
    assert_eq!(
        body(&c.responses[9]),
        "{\"project\":\"kubernetes\",\"db_name\":\"gha\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository\":\"org/repo1\",\"country\":\"All\",\"github_id\":\"alice\",\"filter\":\"series:hdev_commitsorg_repo1all period:d github_id:alice\",\"rank\":[1],\"login\":[\"alice\"],\"number\":[5]}\n"
    );
    assert_eq!(
        body(&c.responses[21]),
        "{\"project\":\"kubernetes\",\"db_name\":\"gha\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository\":\"org/repo1\",\"country\":\"All\",\"companies\":[\"All\"],\"github_id\":\"\",\"rank\":[1,2,3],\"login\":[\"alice\",\"bob\",\"alice\"],\"company\":[\"Google\",\"Red Hat\",\"Independent\"],\"number\":[4,3,1]}\n"
    );
    assert_eq!(
        body(&c.responses[29]),
        "{\"project\":\"kubernetes\",\"db_name\":\"gha\",\"range\":\"range:2020-01-01,2020-02\",\"metric\":\"Commits\",\"repository\":\"org/repo1\",\"country\":\"All\",\"companies\":[\"All\"],\"github_id\":\"\",\"rank\":[1],\"login\":[\"manualr\"],\"company\":[\"Google\"],\"number\":[1]}\n"
    );
    assert_eq!(
        c.record,
        "calc_metric [multi_row_single_column] [/etc/gha2db/metrics/kubernetes/hist_approvers.sql] [<ymdh>] [<ymdh>] [range:2020-03-01 00:00:00,2020-04-01 00:00:00] [hist,merge_series:hdev] PG_DB=gha GHA2DB_PROJECT=kubernetes\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/kubernetes/hist_reviewers.sql] [<ymdh>] [<ymdh>] [range:2020-03-01 00:00:00,2020-04-01 00:00:00] [hist,merge_series:hdev] PG_DB=gha GHA2DB_PROJECT=kubernetes\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/kubernetes/hist_reviewers_repos.sql] [<ymdh>] [<ymdh>] [range:2020-05-01 00:00:00,2020-06-01 00:00:00] [hist,merge_series:hdev_repos] PG_DB=gha GHA2DB_PROJECT=kubernetes\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/kubernetes/hist_approvers_repos.sql] [<ymdh>] [<ymdh>] [range:2020-05-01 00:00:00,2020-06-01 00:00:00] [hist,merge_series:hdev_repos] PG_DB=gha GHA2DB_PROJECT=kubernetes\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/kubernetes/hist_reviewers_repos.sql] [<ymdh>] [<ymdh>] [range:2020-05-01 00:00:00,2020-06-01 00:00:00] [hist,merge_series:hdev_repos] PG_DB=gha GHA2DB_PROJECT=kubernetes\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/kubernetes/hist_approvers_repos.sql] [<ymdh>] [<ymdh>] [range:2020-05-01 00:00:00,2020-06-01 00:00:00] [hist,merge_series:hdev_repos] PG_DB=gha GHA2DB_PROJECT=kubernetes\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/kubernetes/hist_approvers.sql] [<ymdh>] [<ymdh>] [range:2020-05-01 00:00:00,2020-06-01 00:00:00] [hist,merge_series:hdev] PG_DB=gha GHA2DB_PROJECT=kubernetes\n"
    );
    assert_eq!(c.stderr, "");
}

#[test]
fn dev_act_cnt_comp() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let comp = |rest: &str| req("DevActCntComp", &format!(r#"{{"project":"test",{rest}}}"#));
    let c = run_case(
        "dev_act_cnt_comp",
        &Config::default(),
        &[
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["Google","Independent"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["Red Hat"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["Nope"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All","Google"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["Google","All"]"#,
            ),
            comp(
                r#""range":"Last week","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"Last month","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","country":"All","companies":["All"],"github_id":"alice""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","country":"All","companies":["Google"],"github_id":"alice""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","country":"All","companies":["Red Hat"],"github_id":"alice""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","country":"Poland","companies":["All"],"github_id":"""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"Kubernetes","country":"All","companies":["All"],"github_id":"""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"Nope","country":"All","companies":["All"],"github_id":"""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","country":"Nowhere","companies":["All"],"github_id":"""#,
            ),
            comp(
                r#""range":"Last day","metric":"Nope","repository_group":"All","country":"All","companies":["All"],"github_id":"""#,
            ),
            comp(
                r#""range":"Last day","metric":"Approves","repository_group":"All","country":"All","companies":["All"],"github_id":"""#,
            ),
            comp(
                r#""range":"Nope","metric":"Commits","repository_group":"All","country":"All","companies":["All"],"github_id":"""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":[]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":"All""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All",null]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All""#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"range:2020-01-01,2020-02","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"range:2020-01-01,2020-02","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["Google"]"#,
            ),
            comp(
                r#""range":"range:2020-05-01,2020-06","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"range:2020-05-01,2020-06","metric":"Reviews","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"range:2020-05-01,bad","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]"#,
            ),
            comp(
                r#""range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"],"repository":"org/repo1""#,
            ),
            req(
                "DevActCntComp",
                r#"{"project":"missing","range":"Last day","metric":"Commits","repository_group":"All","github_id":"","country":"All","companies":["All"]}"#,
            ),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"All\",\"companies\":[\"All\"],\"github_id\":\"\",\"rank\":[1,2,3,4],\"login\":[\"alice\",\"bob\",\"carol\",\"alice\"],\"company\":[\"Google\",\"Red Hat\",\"Independent\",\"Independent\"],\"number\":[10,7,6,2]}\n"
    );
    assert_eq!(
        body(&c.responses[1]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"Last day\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"All\",\"companies\":[\"Google\",\"Independent\"],\"github_id\":\"\",\"rank\":[1,2,3],\"login\":[\"alice\",\"carol\",\"alice\"],\"company\":[\"Google\",\"Independent\",\"Independent\"],\"number\":[10,6,2]}\n"
    );
    assert_eq!(
        body(&c.responses[10]),
        "{\"error\":\"API 'DevActCntComp': github_id 'alice' not found in results\"}\n"
    );
    assert_eq!(
        body(&c.responses[18]),
        "{\"error\":\"API 'DevActCntComp': 'payload' 'companies' field '[]' cannot be empty (optional false, allow empty false)\"}\n"
    );
    assert_eq!(
        c.record,
        "calc_metric [multi_row_single_column] [/etc/gha2db/metrics/test/project_developer_stats.sql] [<ymdh>] [<ymdh>] [range:2020-05-01 00:00:00,2020-06-01 00:00:00] [hist,merge_series:hdev] PG_DB=dbtest_api GHA2DB_PROJECT=test\n"
    );
    assert_eq!(c.stderr, "");
}

#[test]
fn manual_calculation_failure() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let all = r#""repository_group":"All","github_id":"","country":"All""#;
    let c = run_case(
        "manual_calc_failure",
        &Config::default().env("G2R_CALC_FAIL", "1"),
        &[
            dev_act("range:2020-03-01,2020-04", "Commits", all),
            dev_act("range:2020-01-01,2020-02", "Commits", all),
            req(
                "DevActCntComp",
                r#"{"project":"test","range":"range:2020-06-01,2020-07","metric":"PRs","repository_group":"Kubernetes","github_id":"","country":"Poland","companies":["Google"]}"#,
            ),
            req("Health", r#"{"project":"test"}"#),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[0]),
        "{\"error\":\"API 'DevActCnt': exit status 2\"}\n"
    );
    assert_eq!(status_line(&c.responses[1]), "HTTP/1.1 200 OK");
    assert_eq!(
        body(&c.responses[2]),
        "{\"error\":\"API 'DevActCntComp': exit status 2\"}\n"
    );
    assert_eq!(status_line(&c.responses[3]), "HTTP/1.1 200 OK");
    assert!(
        c.stdout.contains("calc_metric fake stdout\n\nSTDERR:\ncalc_metric fake stderr\n\nCommand, arguments, environment:\n[calc_metric multi_row_single_column /etc/gha2db/metrics/test/project_developer_stats.sql <ymdh> <ymdh> range:2020-03-01 00:00:00,2020-04-01 00:00:00 hist,merge_series:hdev]\nmap[GHA2DB_PROJECT:test PG_DB:dbtest_api]\n"),
        "{}",
        c.stdout
    );
    assert_eq!(
        c.record,
        "calc_metric [multi_row_single_column] [/etc/gha2db/metrics/test/project_developer_stats.sql] [<ymdh>] [<ymdh>] [range:2020-03-01 00:00:00,2020-04-01 00:00:00] [hist,merge_series:hdev] PG_DB=dbtest_api GHA2DB_PROJECT=test\n\
         calc_metric [multi_row_single_column] [/etc/gha2db/metrics/test/project_developer_stats.sql] [<ymdh>] [<ymdh>] [range:2020-06-01 00:00:00,2020-07-01 00:00:00] [hist,merge_series:hdev] PG_DB=dbtest_api GHA2DB_PROJECT=test\n"
    );
    assert_eq!(c.stderr, "");
}

#[test]
fn background_calculations() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let bg = |range: &str| {
        dev_act(
            range,
            "Commits",
            r#""repository_group":"All","github_id":"","country":"All","bg":"yes""#,
        )
    };
    let pause = Step::Sleep(Duration::from_millis(400));
    let c = run_case(
        "background",
        &Config::default().env("G2R_CALC_SLEEP", "3"),
        &[
            bg("range:2020-03-01,2020-04"),
            pause.clone(),
            bg("range:2020-03-01,2020-04"),
            bg("range:2020-03-01 00:00:00,2020-04-01 00:00:00"),
            bg("range:2020-04-01,2020-05"),
            pause.clone(),
            bg("range:2020-05-01,2020-06"),
            pause.clone(),
            bg("range:2020-06-01,2020-07"),
            bg("range:2020-03-01,2020-04"),
            dev_act(
                "Last day",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"All""#,
            ),
            req("Health", r#"{"project":"test"}"#),
            Step::Sleep(Duration::from_secs(5)),
            bg("range:2020-06-01,2020-07"),
            pause.clone(),
            req("Health", r#"{"project":"test"}"#),
            Step::Sleep(Duration::from_secs(5)),
            req("Health", r#"{"project":"test"}"#),
        ],
        true,
    );
    // a background submission answers right away (with no data yet)
    assert_eq!(
        body(&c.responses[0]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"range\":\"range:2020-03-01,2020-04\",\"metric\":\"Commits\",\"repository_group\":\"All\",\"country\":\"All\",\"github_id\":\"\",\"filter\":\"series:hdev_commitsallall period:range:2020-03-01 00:00:00,2020-04-01 00:00:00\",\"rank\":null,\"login\":null,\"number\":null}\n"
    );
    let running = "{\"error\":\"API 'DevActCnt': configuration already running in background (test,dbtest_api,DevActCnt,commits,range:2020-03-01 00:00:00,2020-04-01 00:00:00,false)\"}\n";
    assert_eq!(body(&c.responses[1]), running);
    assert_eq!(body(&c.responses[2]), running);
    assert_eq!(status_line(&c.responses[3]), "HTTP/1.1 200 OK");
    assert_eq!(status_line(&c.responses[4]), "HTTP/1.1 200 OK");
    let too_many = "{\"error\":\"API 'DevActCnt': too many background calculations: 3\"}\n";
    assert_eq!(body(&c.responses[5]), too_many);
    assert_eq!(body(&c.responses[6]), running);
    assert_eq!(status_line(&c.responses[7]), "HTTP/1.1 200 OK");
    assert_eq!(status_line(&c.responses[8]), "HTTP/1.1 200 OK");
    // once the runners are done a new one can be started
    assert_eq!(status_line(&c.responses[9]), "HTTP/1.1 200 OK");
    assert!(
        c.stdout.contains("Request (1 bg runners): "),
        "{}",
        c.stdout
    );
    assert!(
        c.stdout.contains("Request (3 bg runners): "),
        "{}",
        c.stdout
    );
    assert!(
        !c.stdout.contains("Request (4 bg runners): "),
        "{}",
        c.stdout
    );
    assert_eq!(c.stdout.matches("Calculated manually:").count(), 4);
    assert_eq!(c.record.matches("calc_metric ").count(), 4);
    assert_eq!(c.stderr, "");
}

#[test]
fn background_calculation_failure() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let c = run_case(
        "background_failure",
        &Config::default()
            .env("G2R_CALC_SLEEP", "1")
            .env("G2R_CALC_FAIL", "1"),
        &[
            dev_act(
                "range:2020-03-01,2020-04",
                "Commits",
                r#""repository_group":"All","github_id":"","country":"All","bg":"1""#,
            ),
            Step::Sleep(Duration::from_secs(3)),
            req("Health", r#"{"project":"test"}"#),
        ],
        true,
    );
    // the submission itself succeeds, the failure is only logged
    assert_eq!(status_line(&c.responses[0]), "HTTP/1.1 200 OK");
    // (sorted lines)
    assert!(c.stdout.contains("\nSTDERR:\n"), "{}", c.stdout);
    assert!(
        c.stdout.contains("\ncalc_metric fake stderr\n"),
        "{}",
        c.stdout
    );
    assert!(!c.stdout.contains("Calculated manually:"), "{}", c.stdout);
    assert_eq!(c.record.matches("calc_metric ").count(), 1);
    assert_eq!(c.stderr, "");
}

#[test]
fn startup_failures() {
    if db_tests_skipped() {
        return;
    }
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for var in ["PG_PASS", "PG_PASS_RO", "PG_USER_RO", "PG_HOST_RO"] {
        let c = run_startup(
            &format!("startup_no_{var}"),
            &Config::default().unset(var),
            None,
        );
        assert_eq!(c.code, Some(2), "{var}: exit code");
        assert_eq!(
            c.stderr,
            format!(
                "Error(time=<time>):\nError: '{var} env variable must be set'\npanic: stacktrace: {var} env variable must be set\n"
            ),
            "{var}: stderr"
        );
        assert_eq!(
            c.stdout, "Compiled None, commit: None on None using None\nStarting API server\n",
            "{var}: stdout"
        );
    }
    let c = run_startup(
        "startup_no_yaml",
        &Config::default().yaml(Yaml::Missing),
        None,
    );
    assert_eq!(c.code, Some(2));
    assert!(
        c.stderr
            .contains("Error: 'open ./projects.yaml: no such file or directory'"),
        "{}",
        c.stderr
    );
    let c = run_startup(
        "startup_broken_yaml",
        &Config::default().yaml(Yaml::Custom(
            "projects.yaml".to_string(),
            "projects:\n  a: [1\n".to_string(),
        )),
        None,
    );
    assert_eq!(c.code, Some(2));
    assert!(c.stderr.contains("yaml: <msg>"), "{}", c.stderr);
    let c = run_startup(
        "startup_other_yaml_missing",
        &Config::default()
            .env("GHA2DB_PROJECTS_YAML", "other.yaml")
            .yaml(Yaml::Missing),
        None,
    );
    assert_eq!(c.code, Some(2));
    assert!(
        c.stderr
            .contains("Error: 'open ./other.yaml: no such file or directory'"),
        "{}",
        c.stderr
    );
    // a port that is already taken
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let c = run_startup("startup_port_taken", &Config::default(), Some(port));
    assert_eq!(c.code, Some(2));
    assert!(
        c.stderr
            .contains("listen tcp 127.0.0.1:<port>: bind: address already in use"),
        "{}",
        c.stderr
    );
    drop(taken);
    // a malformed port
    let c = run_startup(
        "startup_bad_port",
        &Config::default().env("GHA2DB_API_PORT", "notaport"),
        None,
    );
    assert_eq!(c.code, Some(2));
    assert!(
        c.stderr
            .contains("Error: 'listen tcp: lookup tcp/notaport: unknown port'"),
        "{}",
        c.stderr
    );
}

#[test]
fn alternative_projects_yaml() {
    let Some((_g, _fx)) = setup(Needs::default()) else {
        return;
    };
    let c = run_case(
        "other_yaml",
        &Config::default()
            .env("GHA2DB_PROJECTS_YAML", "other.yaml")
            .yaml(Yaml::Custom(
                "other.yaml".to_string(),
                PROJECTS_YAML.to_string(),
            )),
        &[
            Step::ReqCanon(call("ListProjects", "{}")),
            req("Health", r#"{"project":"test"}"#),
        ],
        false,
    );
    assert_eq!(
        body(&c.responses[1]),
        "{\"project\":\"test\",\"db_name\":\"dbtest_api\",\"events\":5}\n"
    );
}

#[test]
fn signals() {
    if db_tests_skipped() {
        return;
    }
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for (sig, name) in [
        ("INT", "interrupt"),
        ("USR1", "user defined signal 1"),
        ("ALRM", "alarm clock"),
    ] {
        let c = run_signal(&format!("signal_{sig}"), sig);
        assert_eq!(c.code, Some(1), "{sig}: exit code");
        assert!(
            c.stdout
                .ends_with(&format!("Exiting due to signal {name}\n")),
            "{sig}: {}",
            c.stdout
        );
    }
    let c = run_signal("signal_TERM", "TERM");
    assert_eq!(c.code, None);
    assert_eq!(c.signal, Some(15));
}
