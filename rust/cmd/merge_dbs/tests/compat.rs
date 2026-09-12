//! Go ⇄ Rust compatibility tests for `merge_dbs`.
//!
//! Every case gets its own scratch databases per side
//! (`dbtest_merge_<case>_<go|rs>_<i1|i2|…|out>`, schema from
//! `compat/fixtures/merge_dbs/schema.sql`, the same generated seed rows —
//! see [`seed_sql`]) and runs the binary in a scratch directory (holding
//! `projects.yaml` for the `-all-` cases). Compared: exit code, stdout
//! (durations, database names and the `GHA2DB_QOUT` banner time masked; as
//! a sorted multiset when `PARALLEL` > 1), the `Error: '…'` / `PqError:`
//! stderr lines and every table of the output database afterwards.
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise). The unit tests of the binary cover the pure helpers.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("merge_dbs")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_merge_dbs"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The build-information line every DevStats tool prints when it first logs.
const BANNER: &str = "Compiled None, commit: None on None using None";

/// The closing hint printed after a successful merge.
const HINT: &str =
    "Consider running './devel/remove_db_dups.sh' if you merged into existing database.";

/// Every table the tool merges (in its processing order, `gha_actors`
/// first), plus the never merged `gha_companies`.
const MERGED_TABLES: &[&str] = &[
    "gha_actors",
    "gha_assets",
    "gha_branches",
    "gha_comments",
    "gha_reviews",
    "gha_commits",
    "gha_commits_files",
    "gha_commits_roles",
    "gha_events",
    "gha_forkees",
    "gha_issues",
    "gha_issues_assignees",
    "gha_issues_events_labels",
    "gha_issues_labels",
    "gha_issues_pull_requests",
    "gha_labels",
    "gha_milestones",
    "gha_orgs",
    "gha_pages",
    "gha_payloads",
    "gha_pull_requests",
    "gha_pull_requests_assignees",
    "gha_pull_requests_requested_reviewers",
    "gha_releases",
    "gha_releases_assets",
    "gha_repos",
    "gha_repo_groups",
    "gha_repos_langs",
    "gha_skip_commits",
    "gha_teams",
    "gha_teams_repositories",
    "gha_texts",
];

// ---------------------------------------------------------------------------
// Seed data
// ---------------------------------------------------------------------------

/// Seed rows of input database `k` (1-based): keys `n` in `3k-2 ..= 3k+3`,
/// so neighbouring inputs share three keys (collisions; the `db{k}` markers
/// in non-key columns prove that the first database wins) and odd `n` are
/// also inserted as `-n` into the tables merged in two passes
/// (`gha_actors`, `gha_events`, `gha_issues`, `gha_labels`,
/// `gha_payloads`). Timestamps are `2024-01-<n> 10:00:00` (the date
/// filter cases cut at `2024-01-05`). `gha_issues_assignees` gets one row
/// referencing a non-existent event so the `event_id in (select …)` date
/// mapping drops it. `gha_texts` exercises `numeric`, `text[]` and `jsonb`
/// (raw bytes in Go's `database/sql`) with `NULL` variants. Nothing is
/// inserted into `gha_companies` by the tool: its rows must stay in the
/// inputs only.
fn seed_sql(k: i64) -> String {
    assert!(
        (1..=9).contains(&k),
        "seed_sql supports 1..=9 input databases"
    );
    let lo = 3 * k - 2;
    let hi = 3 * k + 3;
    let all: Vec<i64> = (lo..=hi).collect();
    let odd: Vec<i64> = all.iter().copied().filter(|n| n % 2 == 1).collect();
    let ts = |n: i64| format!("'2024-01-{n:02} 10:00:00'");
    let sha = |n: i64| format!("'sha{n:04}'");
    let null_if = |cond: bool, v: String| if cond { "null".to_string() } else { v };
    let q = |s: String| format!("'{s}'");
    let mut out = String::new();
    let mut ins = |t: &str, rows: Vec<String>| {
        if !rows.is_empty() {
            out.push_str(&format!("insert into {t} values {};\n", rows.join(", ")));
        }
    };
    let rows = |f: &dyn Fn(i64) -> String| all.iter().map(|&n| f(n)).collect::<Vec<_>>();
    let odd_rows = |f: &dyn Fn(i64) -> String| odd.iter().map(|&n| f(n)).collect::<Vec<_>>();

    let mut actors = rows(&|n| {
        format!(
            "({n}, 'actor{n}', 'Name {n} db{k}', 'PL', 'm', 0.{n}, 'Europe/Warsaw', 60, 'Poland', {})",
            30 + n
        )
    });
    actors.extend(odd_rows(&|n| {
        format!("(-{n}, 'anon{n}', null, null, null, null, null, null, null, null)")
    }));
    ins("gha_actors", actors);
    ins(
        "gha_assets",
        rows(&|n| format!("({n}, {n}, {}, 'asset-{n}.tgz', {})", ts(n), n * 1000)),
    );
    ins(
        "gha_branches",
        rows(&|n| {
            format!(
                "({}, {n}, {}, {})",
                sha(n),
                ts(n),
                null_if(n % 4 == 0, q(format!("branch-{n}")))
            )
        }),
    );
    ins(
        "gha_comments",
        rows(&|n| {
            format!(
                "({n}, {n}, 'comment {n} from db{k}', {}, {})",
                ts(n),
                null_if(n % 3 == 0, n.to_string())
            )
        }),
    );
    ins(
        "gha_reviews",
        rows(&|n| {
            format!(
                "({n}, {n}, {}, {})",
                null_if(n % 2 == 0, "'APPROVED'".to_string()),
                ts(n)
            )
        }),
    );
    ins(
        "gha_commits",
        rows(&|n| {
            format!(
                "({}, {n}, 'commit {n}', {}, {}, {n})",
                sha(n),
                ts(n),
                n * 10
            )
        }),
    );
    ins(
        "gha_commits_files",
        rows(&|n| format!("({}, 'path/{n}.go', {}, {})", sha(n), n * 100, ts(n))),
    );
    ins(
        "gha_commits_roles",
        rows(&|n| format!("({}, {n}, 'Author', 'actor{n}', {})", sha(n), ts(n))),
    );
    let mut events = rows(&|n| {
        format!(
            "({n}, 'PushEvent', {n}, {n}, {}, {}, {}, 'actor{n}', 'org/repo{n}')",
            ts(n),
            null_if(n % 2 == 0, n.to_string()),
            n % 2 == 0
        )
    });
    events.extend(odd_rows(&|n| {
        format!(
            "(-{n}, 'IssuesEvent', {n}, {n}, {}, null, true, 'actor{n}', 'org/repo{n}')",
            ts(n)
        )
    }));
    ins("gha_events", events);
    ins(
        "gha_forkees",
        rows(&|n| {
            format!(
                "({n}, {n}, 'repo{n}', {}, {}, {})",
                ts(n),
                n * 7,
                n % 3 == 0
            )
        }),
    );
    let mut issues = rows(&|n| {
        format!(
            "({n}, {n}, 'issue {n} db{k}', {}, {}, {})",
            ts(n),
            null_if(n % 2 == 0, ts(n)),
            n % 2 == 1
        )
    });
    issues.extend(odd_rows(&|n| {
        format!("(-{n}, {n}, 'neg issue {n}', {}, null, false)", ts(n))
    }));
    ins("gha_issues", issues);
    let mut assignees = rows(&|n| format!("({n}, {n}, {})", n + 100));
    assignees.push(format!("({lo}, 9999, 1)"));
    ins("gha_issues_assignees", assignees);
    ins(
        "gha_issues_events_labels",
        rows(&|n| format!("({n}, {n}, {n}, 'label{n}', {}, 'actor{n}')", ts(n))),
    );
    ins(
        "gha_issues_labels",
        rows(&|n| format!("({n}, {n}, {n}, {})", ts(n))),
    );
    ins(
        "gha_issues_pull_requests",
        rows(&|n| format!("({n}, {n}, {n}, {n}, 'org/repo{n}', {})", ts(n))),
    );
    let mut labels = rows(&|n| format!("({n}, 'label{n}', 'ff00{n:02x}', {})", n % 2 == 0));
    labels.extend(odd_rows(&|n| format!("(-{n}, 'neg{n}', '000000', null)")));
    ins("gha_labels", labels);
    ins(
        "gha_milestones",
        rows(&|n| format!("({n}, {n}, 'm{n}', {}, 'open')", ts(n))),
    );
    ins("gha_orgs", rows(&|n| format!("({n}, 'org{n}')")));
    ins(
        "gha_pages",
        rows(&|n| {
            let wide: Vec<String> = (1..=65)
                .map(|i| null_if((i + n) % 2 == 1, n.to_string()))
                .collect();
            format!(
                "({}, {n}, 'created', 'Page {n}', {}, {})",
                sha(n),
                ts(n),
                wide.join(", ")
            )
        }),
    );
    let mut payloads = rows(&|n| {
        format!(
            "({n}, {n}, {n}, 'refs/heads/main', {}, 'opened', {n}, null, {}, {n}, {})",
            sha(n),
            ts(n),
            0.5 * n as f64
        )
    });
    payloads.extend(odd_rows(&|n| {
        format!(
            "(-{n}, null, null, null, null, 'closed', null, null, {}, null, null)",
            ts(n)
        )
    }));
    ins("gha_payloads", payloads);
    ins(
        "gha_pull_requests",
        rows(&|n| format!("({n}, {n}, 'pr {n}', {}, {}, {})", ts(n), n % 2 == 0, n * 3)),
    );
    ins(
        "gha_pull_requests_assignees",
        rows(&|n| format!("({n}, {n}, {})", n + 200)),
    );
    ins(
        "gha_pull_requests_requested_reviewers",
        rows(&|n| format!("({n}, {n}, {})", n + 300)),
    );
    ins(
        "gha_releases",
        rows(&|n| format!("({n}, {n}, 'v{n}.0', {}, false)", ts(n))),
    );
    ins(
        "gha_releases_assets",
        rows(&|n| format!("({n}, {n}, {})", n + 400)),
    );
    ins(
        "gha_repos",
        rows(&|n| {
            format!(
                "({n}, 'org/repo{n}', {n}, 'org', {}, null)",
                null_if(n % 2 == 0, "'group'".to_string())
            )
        }),
    );
    ins(
        "gha_repo_groups",
        rows(&|n| format!("({n}, 'org/repo{n}', 'group{n}')")),
    );
    ins(
        "gha_repos_langs",
        rows(&|n| format!("({n}, 'org/repo{n}', 'Go', {}, 12.5, {})", n * 50, ts(n))),
    );
    ins(
        "gha_skip_commits",
        rows(&|n| format!("({}, {})", sha(n), ts(n))),
    );
    ins(
        "gha_teams",
        rows(&|n| format!("({n}, {n}, 'team{n}', {})", ts(n))),
    );
    ins(
        "gha_teams_repositories",
        rows(&|n| format!("({n}, {n}, {})", n + 500)),
    );
    ins(
        "gha_texts",
        rows(&|n| {
            format!(
                "({n}, 'text {n} db{k}', {}, {n}, 'actor{n}', 'PushEvent', {}, {}, {})",
                ts(n),
                null_if(n % 3 == 0, format!("{}", 1.5 * n as f64)),
                null_if(n % 2 == 0, format!("array['a', 'b{n}']")),
                null_if(n % 4 == 0, format!("'{{\"n\": {n}}}'::jsonb"))
            )
        }),
    );
    ins("gha_companies", rows(&|n| format!("('company{n}')")));
    out
}

/// Input `gha_orgs` of the column-mismatch cases: two extra columns
/// (`extra timestamp`, `score numeric`) the output table lacks. The physical
/// row order decides which row fails first: input #1 starts with a NULL
/// time, the other inputs with a real one (and a NULL numeric).
fn orgs_mismatch_sql(k: i64) -> String {
    let rows = [
        "(1, 'org1', null, 1.5)",
        "(2, 'org2', '2024-02-03 04:05:06', null)",
        "(3, 'org3', '2024-02-03 04:05:06', 2.25)",
    ];
    let order: Vec<&str> = if k == 1 {
        rows.to_vec()
    } else {
        vec![rows[1], rows[2], rows[0]]
    };
    format!(
        "drop table gha_orgs; create table gha_orgs(id bigint not null primary key, login varchar(100) not null, extra timestamp, score numeric(10, 3)); insert into gha_orgs values {};",
        order.join(", ")
    )
}

// ---------------------------------------------------------------------------
// Case / Side harness
// ---------------------------------------------------------------------------

/// What the output database looks like before the run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Output {
    /// Schema only.
    Empty,
    /// Schema plus the seed rows of input database #2 (collisions with both
    /// inputs).
    Preseeded,
    /// Schema, but `gha_orgs` lacks the `extra`/`score` columns the inputs
    /// get ([`orgs_mismatch_sql`]; positional inserts fail with
    /// `undefined_column`).
    OrgsMismatch,
}

struct Case {
    name: &'static str,
    /// Number of seeded input databases (`i1`, `i2`, …).
    inputs: usize,
    /// Extra database keys created with the schema only (no seed).
    empty_dbs: Vec<&'static str>,
    output: Output,
    /// `GHA2DB_INPUT_DBS` (`{db:key}` placeholders expand to the side's
    /// database names); `None` — the seeded inputs in order.
    input_dbs: Option<&'static str>,
    /// `GHA2DB_OUTPUT_DB` (`{db:key}` placeholders); `None` — the `out` DB.
    output_db: Option<&'static str>,
    /// `projects.yaml` written to the scratch directory (`{db:key}`
    /// placeholders).
    yaml: Option<&'static str>,
    /// Extra environment (`{db:key}` placeholders in the values).
    env: Vec<(&'static str, &'static str)>,
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
            inputs: 2,
            empty_dbs: Vec::new(),
            output: Output::Empty,
            input_dbs: None,
            output_db: None,
            yaml: None,
            env: Vec::new(),
            sorted: false,
            compare_errors: true,
        }
    }
    fn inputs(mut self, n: usize) -> Self {
        self.inputs = n;
        self
    }
    fn empty_dbs(mut self, dbs: &[&'static str]) -> Self {
        self.empty_dbs = dbs.to_vec();
        self
    }
    fn output(mut self, o: Output) -> Self {
        self.output = o;
        self
    }
    fn input_dbs(mut self, v: &'static str) -> Self {
        self.input_dbs = Some(v);
        self
    }
    fn output_db(mut self, v: &'static str) -> Self {
        self.output_db = Some(v);
        self
    }
    fn yaml(mut self, y: &'static str) -> Self {
        self.yaml = Some(y);
        self
    }
    fn env(mut self, k: &'static str, v: &'static str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self.env.push((k, v));
        self
    }
    fn sorted(mut self) -> Self {
        self.sorted = true;
        self
    }
    fn code_only_errors(mut self) -> Self {
        self.compare_errors = false;
        self
    }
}

struct Side {
    /// Databases by key (`i1`, `i2`, …, `out`, extra keys).
    dbs: BTreeMap<String, TestDb>,
    /// Database name prefix of the side (masked in the outputs).
    prefix: String,
    _dir: TempDir,
    out: Outcome,
}

impl Side {
    fn mask(&self, s: &str) -> String {
        s.replace(&self.prefix, "<dbs>_")
    }
    /// All stdout lines, durations, database names and the `GHA2DB_QOUT`
    /// banner time masked; sorted for the concurrent cases.
    fn stdout_lines(&self, sorted: bool) -> Vec<String> {
        let mut lines: Vec<String> = self
            .mask(&mask_go_durations(&self.out.stdout_str()))
            .lines()
            .map(|l| {
                // The arguments of the banner's `gha_logs` insert carry
                // `time.Now()` (Go with the monotonic reading).
                if l.starts_with("[1:merge_dbs 2:") {
                    if let (Some(a), Some(b)) = (l.find(" 3:"), l.find(" 4:")) {
                        if a < b {
                            return format!("{}<time>{}", &l[..a + 3], &l[b..]);
                        }
                    }
                }
                l.to_string()
            })
            .collect();
        if sorted {
            lines.sort();
        }
        lines
    }
    /// The stdout lines without the banner, in output order.
    fn lines(&self) -> Vec<String> {
        self.stdout_lines(false)
            .into_iter()
            .filter(|l| l != BANNER)
            .collect()
    }
    fn has_line(&self, line: &str) -> bool {
        self.lines().iter().any(|l| l == line)
    }
    fn stderr_lines(&self) -> Vec<String> {
        self.out
            .stderr_str()
            .lines()
            .filter(|l| l.starts_with("Error: '") || l.starts_with("PqError: "))
            .map(|l| self.mask(l))
            .collect()
    }
    /// The `Error: '…'` message of a fatal error.
    fn error(&self) -> Option<String> {
        self.stderr_lines().into_iter().find_map(|l| {
            l.strip_prefix("Error: '")
                .and_then(|r| r.strip_suffix('\''))
                .map(str::to_string)
        })
    }
    /// Every table of the output database (rows ordered by all columns).
    fn data(&self) -> BTreeMap<String, Vec<Vec<String>>> {
        let mut res = BTreeMap::new();
        if let Some(db) = self.dbs.get("out") {
            let con = db.conn();
            for t in cpg::tables(&con) {
                let snap = cpg::table_data(&con, &t);
                res.insert(t, snap.rows);
            }
            con.close();
        }
        res
    }
    /// Row count of a table of the output database.
    fn count(&self, table: &str) -> i64 {
        let con = self.dbs["out"].conn();
        let n = cpg::snapshot(&con, &format!("select count(*) from {table}"), &[]).rows[0][0]
            .parse()
            .unwrap();
        con.close();
        n
    }
    /// One column of the output database's `table`, ordered by `order`.
    fn column(&self, table: &str, column: &str, order: &str) -> Vec<String> {
        let con = self.dbs["out"].conn();
        let snap = cpg::snapshot(
            &con,
            &format!("select coalesce({column}::text, '<null>') from {table} order by {order}"),
            &[],
        );
        con.close();
        snap.column(0)
    }
}

/// Expand `{db:key}` placeholders to `<prefix><key>`.
fn expand(text: &str, prefix: &str) -> String {
    let mut text = text.to_string();
    while let Some(start) = text.find("{db:") {
        let end = text[start..].find('}').unwrap() + start;
        let key = text[start + 4..end].to_string();
        text.replace_range(start..=end, &format!("{prefix}{key}"));
    }
    text
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let short = format!("merge_{}_{}_", case.name, suffix);
    let prefix = format!("{}_{}", cpg::GUARD_DB, short);
    let schema = fs::read_to_string(fixture("merge_dbs/schema.sql")).unwrap();
    let mut dbs = BTreeMap::new();
    let mut input_keys = Vec::new();
    for k in 1..=case.inputs {
        let key = format!("i{k}");
        let db = TestDb::fresh(&format!("{short}{key}"))?;
        db.exec(&schema);
        db.exec(&seed_sql(k as i64));
        if case.output == Output::OrgsMismatch {
            db.exec(&orgs_mismatch_sql(k as i64));
        }
        dbs.insert(key.clone(), db);
        input_keys.push(key);
    }
    for key in &case.empty_dbs {
        let db = TestDb::fresh(&format!("{short}{key}"))?;
        db.exec(&schema);
        dbs.insert(key.to_string(), db);
    }
    let out = TestDb::fresh(&format!("{short}out"))?;
    out.exec(&schema);
    match case.output {
        Output::Empty => {}
        Output::Preseeded => out.exec(&seed_sql(2)),
        Output::OrgsMismatch => {
            out.exec("drop table gha_orgs; create table gha_orgs(id bigint not null primary key, login varchar(100) not null)");
        }
    }
    dbs.insert("out".to_string(), out);

    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_{short}"))
        .tempdir()
        .unwrap();
    if let Some(y) = case.yaml {
        fs::write(dir.path().join("projects.yaml"), expand(y, &prefix)).unwrap();
    }

    let ctx = cpg::test_ctx();
    let input_dbs = match case.input_dbs {
        Some(v) => expand(v, &prefix),
        None => input_keys
            .iter()
            .map(|k| format!("{prefix}{k}"))
            .collect::<Vec<_>>()
            .join(","),
    };
    let output_db = match case.output_db {
        Some(v) => expand(v, &prefix),
        None => format!("{prefix}out"),
    };
    let mut env: Vec<(String, String)> = vec![
        ("PG_HOST".into(), ctx.pg_host.clone()),
        ("PG_PORT".into(), ctx.pg_port.clone()),
        ("PG_USER".into(), ctx.pg_user.clone()),
        ("PG_PASS".into(), ctx.pg_pass.clone()),
        ("PG_SSL".into(), ctx.pg_ssl.clone()),
        ("PG_DB".into(), cpg::GUARD_DB.to_string()),
        ("GHA2DB_SKIPLOG".into(), "1".into()),
        ("GHA2DB_SKIPTIME".into(), "1".into()),
        ("GHA2DB_LOCAL".into(), "1".into()),
        ("GHA2DB_INPUT_DBS".into(), input_dbs),
        ("GHA2DB_OUTPUT_DB".into(), output_db),
    ];
    for (k, v) in &case.env {
        env.retain(|(key, _)| key != k);
        env.push((k.to_string(), expand(v, &prefix)));
    }
    let mut inv = Invocation::new().cwd(dir.path().to_path_buf());
    for (k, v) in &env {
        inv = inv.env(leak(k), leak(v));
    }
    let out = run(bin, &inv);
    Some(Side {
        dbs,
        prefix,
        _dir: dir,
        out,
    })
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
        assert_eq!(
            go.stdout_lines(case.sorted),
            rust.stdout_lines(case.sorted),
            "stdout{ctx}"
        );
        if case.compare_errors {
            assert_eq!(go.stderr_lines(), rust.stderr_lines(), "stderr{ctx}");
        } else {
            assert_eq!(
                go.stderr_lines().len(),
                rust.stderr_lines().len(),
                "stderr line count{ctx}"
            );
        }
        assert_eq!(go.data(), rust.data(), "output database contents{ctx}");
    }
    Some(rust)
}

/// The `done table … all rows` summary line of a pass.
fn all_rows_line(
    pass: &str,
    idx: usize,
    table: &str,
    rows: usize,
    inserted: usize,
    collisions: usize,
) -> String {
    let perc = if rows > 0 {
        collisions as f64 * 100.0 / rows as f64
    } else {
        0.0
    };
    format!(
        "{pass}: done table: #{idx}: {table}, all rows: {rows}, inserted: {inserted}, collisions: {collisions} ({perc:.3}%)"
    )
}

// ---------------------------------------------------------------------------
// Merging
// ---------------------------------------------------------------------------

#[test]
fn basic_two_inputs() {
    let Some(rs) = both(&Case::new("basic")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.lines();
    assert_eq!(
        lines[0],
        "merge_dbs: USE_BATCH=false, BATCH_SIZE=1000, PARALLEL=1 (max 65535 psql params per batch insert)"
    );
    // 6 rows per input, keys 1..=6 and 4..=9: 3 collide.
    assert!(rs.has_line("1st pass: start table: #0: gha_actors, DB #0: <dbs>_i1, rows: 6..."));
    assert!(rs.has_line(
        "1st pass: done table: #0: gha_actors, DB #1: <dbs>_i2, rows: 6, inserted: 3, collisions: 3 (50.000%)"
    ));
    assert!(rs.has_line(&all_rows_line("1st pass", 0, "gha_actors", 12, 9, 3)));
    // 2nd pass: the negative ids (odd n: 1,3,5 and 5,7,9 → 5 collides).
    assert!(rs.has_line(&all_rows_line("2nd pass", 0, "gha_actors", 6, 5, 1)));
    assert!(rs.has_line(&all_rows_line("2nd pass", 8, "gha_events", 6, 5, 1)));
    // No primary key: everything is inserted, duplicates included.
    assert!(rs.has_line(&all_rows_line(
        "1st pass",
        12,
        "gha_issues_events_labels",
        12,
        12,
        0
    )));
    assert!(rs.has_line(&all_rows_line("1st pass", 31, "gha_texts", 12, 12, 0)));
    // The 2nd pass only visits the two-pass tables.
    assert_eq!(
        lines
            .iter()
            .filter(|l| l.starts_with("2nd pass: done table") && l.contains("all rows"))
            .count(),
        5
    );
    assert_eq!(lines[lines.len() - 2], "Time: <duration>");
    assert_eq!(lines[lines.len() - 1], HINT);

    assert_eq!(rs.count("gha_actors"), 9 + 5);
    assert_eq!(rs.count("gha_events"), 9 + 5);
    assert_eq!(rs.count("gha_orgs"), 9);
    assert_eq!(rs.count("gha_texts"), 12);
    assert_eq!(rs.count("gha_issues_events_labels"), 12);
    assert_eq!(rs.count("gha_issues_assignees"), 9 + 2);
    assert_eq!(rs.count("gha_companies"), 0);
    // The first database wins on collisions.
    assert_eq!(
        rs.column("gha_actors", "name", "id"),
        [
            "<null>",
            "<null>",
            "<null>",
            "<null>",
            "<null>",
            "Name 1 db1",
            "Name 2 db1",
            "Name 3 db1",
            "Name 4 db1",
            "Name 5 db1",
            "Name 6 db1",
            "Name 7 db2",
            "Name 8 db2",
            "Name 9 db2"
        ]
    );
    // Raw-bytes columns (numeric / text[] / jsonb) survive the round trip.
    assert_eq!(
        rs.column("gha_texts", "score", "event_id, body"),
        [
            "1.500", "3.000", "<null>", "6.000", "6.000", "7.500", "7.500", "<null>", "<null>",
            "10.500", "12.000", "<null>"
        ]
    );
    assert_eq!(
        rs.column("gha_texts", "tags", "event_id, body")[0],
        "{a,b1}"
    );
    assert_eq!(
        rs.column("gha_texts", "meta", "event_id, body")[0],
        "{\"n\": 1}"
    );
}

#[test]
fn three_inputs() {
    let Some(rs) = both(&Case::new("three").inputs(3)) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line("1st pass: start table: #17: gha_orgs, DB #2: <dbs>_i3, rows: 6..."));
    assert!(rs.has_line(&all_rows_line("1st pass", 17, "gha_orgs", 18, 12, 6)));
    assert_eq!(rs.count("gha_orgs"), 12);
}

#[test]
fn affiliations_db_skips_actors() {
    let Some(rs) = both(&Case::new("affs").env("GHA2DB_AFFILIATIONS_DB", "{db:i1}")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(!rs.lines().iter().any(|l| l.contains("gha_actors")));
    assert!(rs.has_line("1st pass: start table: #0: gha_assets, DB #0: <dbs>_i1, rows: 6..."));
    assert_eq!(rs.count("gha_actors"), 0);
    assert_eq!(rs.count("gha_assets"), 9);
}

#[test]
fn existing_output_collides() {
    let Some(rs) = both(&Case::new("existing").output(Output::Preseeded)) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    // Output holds keys 4..=9 already: input 1 (1..=6) adds 3, input 2 nothing.
    assert!(rs.has_line(
        "1st pass: done table: #17: gha_orgs, DB #0: <dbs>_i1, rows: 6, inserted: 3, collisions: 3 (50.000%)"
    ));
    assert!(rs.has_line(
        "1st pass: done table: #17: gha_orgs, DB #1: <dbs>_i2, rows: 6, inserted: 0, collisions: 6 (100.000%)"
    ));
    assert_eq!(rs.count("gha_orgs"), 9);
    // The pre-existing rows win.
    assert_eq!(rs.column("gha_actors", "name", "id")[5 + 4], "Name 5 db2");
    // No key: the pre-seeded rows are duplicated by the input copies.
    assert_eq!(rs.count("gha_texts"), 18);
}

#[test]
fn empty_inputs_merge_nothing() {
    let Some(rs) = both(
        &Case::new("emptyin")
            .inputs(0)
            .empty_dbs(&["e1", "e2"])
            .input_dbs("{db:e1},{db:e2}"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line("1st pass: start table: #0: gha_actors, DB #0: <dbs>_e1, rows: 0..."));
    assert!(rs.has_line(&all_rows_line("1st pass", 0, "gha_actors", 0, 0, 0)));
    assert!(rs.data().values().all(|rows| rows.is_empty()));
}

#[test]
fn input_names_are_trimmed() {
    let Some(rs) = both(&Case::new("trim").input_dbs(" {db:i1} , {db:i2} ,, ")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line("1st pass: start table: #17: gha_orgs, DB #1: <dbs>_i2, rows: 6..."));
    assert_eq!(rs.count("gha_orgs"), 9);
}

// ---------------------------------------------------------------------------
// Table filters
// ---------------------------------------------------------------------------

#[test]
fn only_tables() {
    let Some(rs) =
        both(&Case::new("only").env("ONLY_TABLES", " gha_texts, gha_orgs ,gha_events,, "))
    else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line(
        "merge_dbs table filter: selected 3 table(s), ONLY_TABLES=\" gha_texts, gha_orgs ,gha_events,, \", SKIP_TABLES=\"\""
    ));
    // Indexes are renumbered within the selection, processing order kept.
    assert!(rs.has_line("1st pass: start table: #0: gha_events, DB #0: <dbs>_i1, rows: 6..."));
    assert!(rs.has_line("1st pass: start table: #1: gha_orgs, DB #0: <dbs>_i1, rows: 6..."));
    assert!(rs.has_line("1st pass: start table: #2: gha_texts, DB #0: <dbs>_i1, rows: 6..."));
    assert!(rs.has_line("2nd pass: start table: #0: gha_events, DB #0: <dbs>_i1, rows: 3..."));
    assert_eq!(rs.count("gha_events"), 14);
    assert_eq!(rs.count("gha_orgs"), 9);
    assert_eq!(rs.count("gha_texts"), 12);
    assert_eq!(rs.count("gha_actors"), 0);
    assert_eq!(rs.count("gha_labels"), 0);
}

#[test]
fn skip_tables() {
    let Some(rs) = both(&Case::new("skip").env("SKIP_TABLES", "gha_pages,gha_texts")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line(
        "merge_dbs table filter: selected 30 table(s), ONLY_TABLES=\"\", SKIP_TABLES=\"gha_pages,gha_texts\""
    ));
    assert!(!rs
        .lines()
        .iter()
        .any(|l| l.contains("table: ") && (l.contains("gha_pages") || l.contains("gha_texts"))));
    assert_eq!(rs.count("gha_pages"), 0);
    assert_eq!(rs.count("gha_texts"), 0);
    assert_eq!(rs.count("gha_orgs"), 9);
}

#[test]
fn only_tables_override_skip_tables() {
    let Some(rs) = both(
        &Case::new("onlyskip")
            .env("ONLY_TABLES", "gha_orgs")
            .env("SKIP_TABLES", "gha_orgs,nosuchtable"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    // SKIP_TABLES is neither validated nor applied when ONLY_TABLES is set.
    assert!(rs.has_line(
        "merge_dbs table filter: selected 1 table(s), ONLY_TABLES=\"gha_orgs\", SKIP_TABLES=\"gha_orgs,nosuchtable\""
    ));
    assert_eq!(rs.count("gha_orgs"), 9);
}

#[test]
fn only_tables_unknown_table_is_fatal() {
    let Some(rs) = both(&Case::new("onlybad").env("ONLY_TABLES", "gha_orgs,gha_nosuch")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("ONLY_TABLES contains unknown table 'gha_nosuch'")
    );
    assert_eq!(rs.count("gha_orgs"), 0);
}

#[test]
fn skip_tables_unknown_table_is_fatal() {
    let Some(rs) = both(&Case::new("skipbad").env("SKIP_TABLES", "gha_nosuch")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("SKIP_TABLES contains unknown table 'gha_nosuch'")
    );
}

#[test]
fn actors_unknown_when_affiliations_db_set() {
    let Some(rs) = both(
        &Case::new("affsonly")
            .env("GHA2DB_AFFILIATIONS_DB", "{db:i1}")
            .env("ONLY_TABLES", "gha_actors"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("ONLY_TABLES contains unknown table 'gha_actors'")
    );
}

// ---------------------------------------------------------------------------
// Date filter
// ---------------------------------------------------------------------------

#[test]
fn date_filter() {
    let Some(rs) = both(&Case::new("dtfrom").env("MERGE_DT_FROM", "2024-01-05")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.lines();
    assert_eq!(
        lines[0],
        "merge_dbs date filter: MERGE_DT_FROM=\"2024-01-05 00:00:00\"; tables without a merge date mapping are copied fully"
    );
    // Tables without a mapping are reported once per pass (first DB only);
    // gha_actors and gha_labels are merged in two passes.
    let no_map = |t: &str| {
        format!("merge_dbs date filter: table {t} has no merge date mapping, copying all rows")
    };
    for t in ["gha_reviews", "gha_orgs", "gha_repos", "gha_repo_groups"] {
        assert_eq!(lines.iter().filter(|l| **l == no_map(t)).count(), 1, "{t}");
    }
    for t in ["gha_actors", "gha_labels"] {
        assert_eq!(lines.iter().filter(|l| **l == no_map(t)).count(), 2, "{t}");
    }
    // Input 1 keeps 5, 6; input 2 keeps 5..=9 → 7 rows, 2 collisions.
    assert!(rs.has_line("1st pass: start table: #17: gha_orgs, DB #0: <dbs>_i1, rows: 6..."));
    assert!(rs.has_line("1st pass: start table: #1: gha_assets, DB #0: <dbs>_i1, rows: 2..."));
    assert!(rs.has_line(&all_rows_line("1st pass", 1, "gha_assets", 7, 5, 2)));
    assert!(rs.has_line(&all_rows_line("1st pass", 8, "gha_events", 7, 5, 2)));
    assert!(rs.has_line(&all_rows_line("2nd pass", 8, "gha_events", 4, 3, 1)));
    // `event_id in (select …)`: the row referencing event 9999 is dropped.
    assert!(rs.has_line(&all_rows_line(
        "1st pass",
        11,
        "gha_issues_assignees",
        7,
        5,
        2
    )));
    assert!(rs.has_line(&all_rows_line("1st pass", 6, "gha_commits_files", 7, 5, 2)));
    assert!(rs.has_line(&all_rows_line("1st pass", 31, "gha_texts", 7, 7, 0)));
    assert_eq!(rs.count("gha_orgs"), 9);
    assert_eq!(rs.count("gha_assets"), 5);
    assert_eq!(
        rs.column("gha_assets", "id", "id"),
        ["5", "6", "7", "8", "9"]
    );
}

#[test]
fn date_filter_formats() {
    // Events at `2024-01-<n> 10:00:00`: from midnight of the 5th, ids 5..=9
    // and -5, -7, -9 pass; from 11:00 on, 6..=9 and -7, -9.
    for (i, (value, printed, total)) in [
        ("2024-01-05T11:30:15Z", "2024-01-05 11:30:15", 6),
        ("2024-01-05 11:30:15", "2024-01-05 11:30:15", 6),
        ("2024-01-05 11:30", "2024-01-05 11:30:00", 6),
        ("2024-01-05 11", "2024-01-05 11:00:00", 6),
        (" 2024-01-05 ", "2024-01-05 00:00:00", 8),
    ]
    .iter()
    .enumerate()
    {
        let name = leak(&format!("dtfmt{i}"));
        let Some(rs) = both(
            &Case::new(name)
                .env("MERGE_DT_FROM", value)
                .env("ONLY_TABLES", "gha_events"),
        ) else {
            return;
        };
        assert_eq!(rs.out.code, Some(0), "{value}");
        assert_eq!(
            rs.lines()[0],
            format!(
                "merge_dbs date filter: MERGE_DT_FROM=\"{printed}\"; tables without a merge date mapping are copied fully"
            ),
            "{value}"
        );
        assert_eq!(rs.count("gha_events"), *total, "{value}");
    }
}

#[test]
fn date_filter_drom_alias() {
    let Some(rs) = both(
        &Case::new("dtdrom")
            .env("MERGE_DT_DROM", "2024-01-08")
            .env("ONLY_TABLES", "gha_events"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line(
        "merge_dbs date filter: MERGE_DT_FROM=\"2024-01-08 00:00:00\"; tables without a merge date mapping are copied fully"
    ));
    assert_eq!(rs.column("gha_events", "id", "id"), ["-9", "8", "9"]);
}

#[test]
fn date_filter_both_equal() {
    let Some(rs) = both(
        &Case::new("dtboth")
            .env("MERGE_DT_FROM", "2024-01-08")
            .env("MERGE_DT_DROM", "2024-01-08")
            .env("ONLY_TABLES", "gha_events"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(rs.count("gha_events"), 3);
}

#[test]
fn date_filter_mismatch_is_fatal() {
    let Some(rs) = both(
        &Case::new("dtmismatch")
            .env("MERGE_DT_FROM", "2024-01-08")
            .env("MERGE_DT_DROM", "2024-01-09"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("MERGE_DT_FROM and MERGE_DT_DROM are both set but differ: \"2024-01-08\" != \"2024-01-09\"")
    );
}

#[test]
fn date_filter_unparseable_is_fatal() {
    for (i, value) in [
        "2024/01/05",
        "yesterday",
        "2024-01-05 11:30:15.",
        "2024-13-01",
    ]
    .iter()
    .enumerate()
    {
        let name = leak(&format!("dtbad{i}"));
        let Some(rs) = both(&Case::new(name).env("MERGE_DT_FROM", value)) else {
            return;
        };
        assert_eq!(rs.out.code, Some(2), "{value}");
        assert_eq!(
            rs.error(),
            Some(format!(
                "MERGE_DT_FROM/MERGE_DT_DROM must be YYYY-MM-DD or parseable timestamp, got \"{value}\""
            )),
            "{value}"
        );
    }
}

// ---------------------------------------------------------------------------
// Batch mode / parallelism
// ---------------------------------------------------------------------------

#[test]
fn batch_mode_small_batches() {
    let Some(rs) = both(
        &Case::new("batch")
            .env("USE_BATCH", "1")
            .env("BATCH_SIZE", "4"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.lines()[0],
        "merge_dbs: USE_BATCH=true, BATCH_SIZE=4, PARALLEL=1 (max 65535 psql params per batch insert)"
    );
    // Same counts as the row-by-row mode (`on conflict do nothing`).
    assert!(rs.has_line(&all_rows_line("1st pass", 0, "gha_actors", 12, 9, 3)));
    assert!(rs.has_line(&all_rows_line("2nd pass", 0, "gha_actors", 6, 5, 1)));
    assert!(rs.has_line(&all_rows_line("1st pass", 31, "gha_texts", 12, 12, 0)));
    assert!(!rs.lines().iter().any(|l| l.contains("capped")));
    assert_eq!(rs.count("gha_actors"), 14);
    assert_eq!(rs.count("gha_texts"), 12);
}

#[test]
fn batch_mode_default_size_is_capped_for_wide_tables() {
    let Some(rs) = both(&Case::new("batchcap").env("USE_BATCH", "true")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line(
        "1st pass: table #18 gha_pages, DB #0 <dbs>_i1: batch size capped from 1000 to 936 (70 columns, max 65535 psql params)"
    ));
    assert!(rs.has_line(
        "1st pass: table #18 gha_pages, DB #1 <dbs>_i2: batch size capped from 1000 to 936 (70 columns, max 65535 psql params)"
    ));
    assert_eq!(
        rs.lines().iter().filter(|l| l.contains("capped")).count(),
        2
    );
    assert_eq!(rs.count("gha_pages"), 9);
}

#[test]
fn batch_size_clamps() {
    for (i, (value, msg, effective)) in [
        (
            "1",
            Some("merge_dbs: BATCH_SIZE=1 is below minimum, using 2"),
            2,
        ),
        (
            "0",
            Some("merge_dbs: BATCH_SIZE=0 is below minimum, using 2"),
            2,
        ),
        (
            "-7",
            Some("merge_dbs: BATCH_SIZE=-7 is below minimum, using 2"),
            2,
        ),
        (
            "5000",
            Some("merge_dbs: BATCH_SIZE=5000 is above maximum, using 1000"),
            1000,
        ),
        (" 2 ", None, 2),
        ("", None, 1000),
    ]
    .iter()
    .enumerate()
    {
        let name = leak(&format!("bsize{i}"));
        let Some(rs) = both(
            &Case::new(name)
                .env("USE_BATCH", "yes")
                .env("BATCH_SIZE", value)
                .env("ONLY_TABLES", "gha_orgs"),
        ) else {
            return;
        };
        assert_eq!(rs.out.code, Some(0), "{value:?}");
        match msg {
            Some(m) => assert!(rs.has_line(m), "{value:?}"),
            None => assert!(
                !rs.lines()
                    .iter()
                    .any(|l| l.contains("BATCH_SIZE=") && l.contains("using")),
                "{value:?}"
            ),
        }
        assert!(
            rs.has_line(&format!(
                "merge_dbs: USE_BATCH=true, BATCH_SIZE={effective}, PARALLEL=1 (max 65535 psql params per batch insert)"
            )),
            "{value:?}"
        );
        assert_eq!(rs.count("gha_orgs"), 9, "{value:?}");
    }
}

#[test]
fn batch_size_invalid_is_fatal() {
    let Some(rs) = both(
        &Case::new("bsizebad")
            .env("USE_BATCH", "1")
            .env("BATCH_SIZE", "abc"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("invalid BATCH_SIZE=\"abc\": strconv.Atoi: parsing \"abc\": invalid syntax")
    );
}

#[test]
fn batch_size_ignored_without_use_batch() {
    let Some(rs) = both(
        &Case::new("bsizeoff")
            .env("USE_BATCH", "0")
            .env("BATCH_SIZE", "abc")
            .env("ONLY_TABLES", "gha_orgs"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line(
        "merge_dbs: USE_BATCH=false, BATCH_SIZE=1000, PARALLEL=1 (max 65535 psql params per batch insert)"
    ));
}

#[test]
fn parallel_workers() {
    let Some(rs) = both(&Case::new("par").env("PARALLEL", "4").sorted()) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert!(rs.has_line(
        "merge_dbs: USE_BATCH=false, BATCH_SIZE=1000, PARALLEL=4 (max 65535 psql params per batch insert)"
    ));
    assert!(rs.has_line(&all_rows_line("1st pass", 0, "gha_actors", 12, 9, 3)));
    assert!(rs.has_line(&all_rows_line("2nd pass", 19, "gha_payloads", 6, 5, 1)));
    // Passes stay sequential: everything of the 1st pass precedes the 2nd.
    let lines = rs.lines();
    let last_first = lines
        .iter()
        .rposition(|l| l.starts_with("1st pass"))
        .unwrap();
    let first_second = lines
        .iter()
        .position(|l| l.starts_with("2nd pass"))
        .unwrap();
    assert!(last_first < first_second);
    assert_eq!(rs.count("gha_actors"), 14);
    assert_eq!(rs.count("gha_texts"), 12);
}

#[test]
fn parallel_batch_mode() {
    let Some(rs) = both(
        &Case::new("parbatch")
            .env("PARALLEL", "3")
            .env("USE_BATCH", "1")
            .env("BATCH_SIZE", "5")
            .sorted(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(rs.count("gha_actors"), 14);
    assert_eq!(rs.count("gha_pages"), 9);
}

#[test]
fn parallel_clamps() {
    for (i, (value, msg, effective)) in [
        (
            "0",
            Some("merge_dbs: PARALLEL=0 is below minimum, using 1"),
            1,
        ),
        (
            "-2",
            Some("merge_dbs: PARALLEL=-2 is below minimum, using 1"),
            1,
        ),
        (
            "100",
            Some("merge_dbs: PARALLEL=100 is above maximum, using 16"),
            16,
        ),
        ("1", None, 1),
    ]
    .iter()
    .enumerate()
    {
        let name = leak(&format!("parclamp{i}"));
        let mut case = Case::new(name)
            .env("PARALLEL", value)
            .env("ONLY_TABLES", "gha_orgs,gha_repos");
        if *effective > 1 {
            case = case.sorted();
        }
        let Some(rs) = both(&case) else {
            return;
        };
        assert_eq!(rs.out.code, Some(0), "{value}");
        if let Some(m) = msg {
            assert!(rs.has_line(m), "{value}");
        }
        assert!(
            rs.has_line(&format!(
                "merge_dbs: USE_BATCH=false, BATCH_SIZE=1000, PARALLEL={effective} (max 65535 psql params per batch insert)"
            )),
            "{value}"
        );
        assert_eq!(rs.count("gha_orgs"), 9, "{value}");
    }
}

#[test]
fn parallel_invalid_is_fatal() {
    let Some(rs) = both(&Case::new("parbad").env("PARALLEL", "many")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("invalid PARALLEL=\"many\": strconv.Atoi: parsing \"many\": invalid syntax")
    );
}

// ---------------------------------------------------------------------------
// `-all-` mode (projects.yaml)
// ---------------------------------------------------------------------------

/// `projects.yaml` of the `-all-` cases: alpha (order 1) and beta/beta2
/// (order 2, the same database → deduplicated) share the output database
/// (with surrounding blanks around alpha's `shared_db`), zeta (order 3)
/// too; gamma is disabled; ghost (order 0, disabled, non-existent database)
/// can be enabled with `GHA2DB_PROJECTS_OVERRIDE=+ghost`; other has a
/// different `shared_db`; selfref's database *is* the output; nodb has no
/// database.
const YAML: &str = r#"---
projects:
  alpha:
    name: Alpha
    psql_db: {db:i1}
    shared_db: "  {db:out}  "
    order: 1
  beta:
    name: Beta
    psql_db: {db:i2}
    shared_db: {db:out}
    order: 2
  beta2:
    name: Beta Two
    psql_db: {db:i2}
    shared_db: {db:out}
    order: 2
  zeta:
    name: Zeta
    psql_db: {db:i3}
    shared_db: {db:out}
    order: 3
  gamma:
    name: Gamma
    psql_db: {db:gamma}
    shared_db: {db:out}
    disabled: true
    order: 4
  ghost:
    name: Ghost
    psql_db: {db:ghost}
    shared_db: {db:out}
    disabled: true
    order: 0
  other:
    name: Other
    psql_db: {db:other}
    shared_db: elsewhere
    order: 5
  selfref:
    name: Self
    psql_db: {db:out}
    shared_db: {db:out}
    order: 6
  nodb:
    name: No DB
    psql_db: ""
    shared_db: {db:out}
    order: 7
"#;

fn all_case(name: &'static str) -> Case {
    Case::new(name)
        .inputs(3)
        .yaml(YAML)
        .input_dbs("-all-")
        .env("ONLY_TABLES", "gha_orgs,gha_events")
}

#[test]
fn all_mode_expands_shared_db_projects() {
    let Some(rs) = both(&all_case("all")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    assert_eq!(
        rs.lines()[0],
        "merge_dbs: expanded GHA2DB_INPUT_DBS=\"-all-\" to 3 DB(s) with shared_db=\"<dbs>_out\": [<dbs>_i1 <dbs>_i2 <dbs>_i3]"
    );
    assert!(rs.has_line("1st pass: start table: #1: gha_orgs, DB #2: <dbs>_i3, rows: 6..."));
    assert_eq!(rs.count("gha_orgs"), 12);
    assert_eq!(rs.count("gha_events"), 12 + 6);
}

#[test]
fn all_mode_skip_dbs() {
    let Some(rs) = both(&all_case("allskip").env("SKIP_DBS", " {db:i2}, nosuch ,,{db:i3}")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.lines();
    assert_eq!(
        lines[0],
        "merge_dbs: skipped 2 DB(s) using SKIP_DBS=\" <dbs>_i2, nosuch ,,<dbs>_i3\": [<dbs>_i2 <dbs>_i3]"
    );
    assert_eq!(
        lines[1],
        "merge_dbs: expanded GHA2DB_INPUT_DBS=\"-all-\" to 1 DB(s) with shared_db=\"<dbs>_out\": [<dbs>_i1]"
    );
    assert_eq!(rs.count("gha_orgs"), 6);
}

#[test]
fn all_mode_override_enables_project_with_missing_db() {
    let Some(rs) = both(&all_case("allghost").env("GHA2DB_PROJECTS_OVERRIDE", "+ghost,-zeta"))
    else {
        return;
    };
    // Lazily connected: the first query against the ghost database fails.
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.lines()[0],
        "merge_dbs: expanded GHA2DB_INPUT_DBS=\"-all-\" to 3 DB(s) with shared_db=\"<dbs>_out\": [<dbs>_ghost <dbs>_i1 <dbs>_i2]"
    );
    assert_eq!(
        rs.error(),
        Some("pq: database \"<dbs>_ghost\" does not exist".to_string())
    );
    assert!(rs
        .stderr_lines()
        .contains(&"PqError: code=3D000, name=invalid_catalog_name, detail=".to_string()));
    assert!(rs.has_line("PqError: code=3D000, name=invalid_catalog_name, detail="));
    assert_eq!(rs.count("gha_orgs"), 0);
}

#[test]
fn all_mode_ignore_no_db() {
    let Some(rs) = both(
        &all_case("allignore")
            .env("GHA2DB_PROJECTS_OVERRIDE", "+ghost")
            .env("IGNORE_NO_DB", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.lines();
    assert_eq!(
        lines[0],
        "merge_dbs: expanded GHA2DB_INPUT_DBS=\"-all-\" to 4 DB(s) with shared_db=\"<dbs>_out\": [<dbs>_ghost <dbs>_i1 <dbs>_i2 <dbs>_i3]"
    );
    assert_eq!(
        lines[1],
        "merge_dbs: skipping unavailable input DB \"<dbs>_ghost\" due to IGNORE_NO_DB=1: pq: database \"<dbs>_ghost\" does not exist"
    );
    // The remaining databases are renumbered.
    assert!(rs.has_line("1st pass: start table: #1: gha_orgs, DB #0: <dbs>_i1, rows: 6..."));
    assert!(rs.has_line("1st pass: start table: #1: gha_orgs, DB #2: <dbs>_i3, rows: 6..."));
    assert_eq!(rs.count("gha_orgs"), 12);
}

#[test]
fn all_mode_ignore_no_db_all_missing_is_fatal() {
    let Some(rs) = both(
        &all_case("allmissing")
            .env(
                "GHA2DB_PROJECTS_OVERRIDE",
                "+ghost,-alpha,-beta,-beta2,-zeta",
            )
            .env("IGNORE_NO_DB", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("required at least 1 available input database after filtering/connection, got 0 from [<dbs>_ghost]")
    );
}

#[test]
fn all_mode_no_matching_projects_is_fatal() {
    let Some(rs) = both(&all_case("allnone").output_db("{db:nowhere}")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("no enabled projects in projects.yaml have shared_db=\"<dbs>_nowhere\"")
    );
}

#[test]
fn all_mode_must_be_alone() {
    let Some(rs) = both(&all_case("allalone").input_dbs("-all-,{db:i1}")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("-all- must be used alone in GHA2DB_INPUT_DBS, got [-all- <dbs>_i1]")
    );
}

#[test]
fn skip_dbs_requires_all_mode() {
    let Some(rs) = both(&Case::new("skipdbsreq").env("SKIP_DBS", "{db:i2}")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("SKIP_DBS can only be used with GHA2DB_INPUT_DBS=\"-all-\"")
    );
}

#[test]
fn ignore_no_db_requires_all_mode() {
    let Some(rs) = both(&Case::new("ignorereq").env("IGNORE_NO_DB", "1")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("IGNORE_NO_DB=1 can only be used with GHA2DB_INPUT_DBS=\"-all-\"")
    );
}

#[test]
fn all_mode_without_projects_yaml_is_fatal() {
    let Some(rs) = both(
        &Case::new("allnoyaml")
            .inputs(0)
            .input_dbs("-all-")
            .env("ONLY_TABLES", "gha_orgs"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(
        rs.error().as_deref(),
        Some("open ./projects.yaml: no such file or directory")
    );
}

#[test]
fn all_mode_invalid_projects_yaml_is_fatal() {
    let Some(rs) = both(
        &Case::new("allbadyaml")
            .inputs(0)
            .input_dbs("-all-")
            .yaml("projects: [\n")
            .code_only_errors(),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert!(rs.error().is_some());
}

// ---------------------------------------------------------------------------
// Argument / connection errors
// ---------------------------------------------------------------------------

#[test]
fn output_db_required() {
    let Some(rs) = both(&Case::new("noout").output_db("")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert_eq!(rs.error().as_deref(), Some("output database required"));
}

#[test]
fn input_dbs_required() {
    for (i, value) in ["", " , ,"].iter().enumerate() {
        let name = leak(&format!("noin{i}"));
        let Some(rs) = both(&Case::new(name).inputs(0).input_dbs(value)) else {
            return;
        };
        assert_eq!(rs.out.code, Some(2), "{value:?}");
        assert_eq!(
            rs.error().as_deref(),
            Some("required at least 1 input database, got 0: []"),
            "{value:?}"
        );
    }
}

#[test]
fn missing_input_db_is_fatal() {
    let Some(rs) = both(&Case::new("noindb").input_dbs("{db:i1},{db:nosuch}")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    // Input 1 is merged before the second database is touched.
    assert!(rs.has_line("1st pass: start table: #0: gha_actors, DB #0: <dbs>_i1, rows: 6..."));
    assert!(rs.has_line("PqError: code=3D000, name=invalid_catalog_name, detail="));
    assert_eq!(
        rs.error(),
        Some("pq: database \"<dbs>_nosuch\" does not exist".to_string())
    );
    assert_eq!(rs.count("gha_actors"), 6);
}

#[test]
fn missing_output_db_is_fatal() {
    let Some(rs) = both(&Case::new("nooutdb").output_db("{db:nosuch}")) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert!(rs.has_line("1st pass: start table: #0: gha_actors, DB #0: <dbs>_i1, rows: 6..."));
    assert_eq!(
        rs.error(),
        Some("pq: database \"<dbs>_nosuch\" does not exist".to_string())
    );
}

#[test]
fn column_mismatch_row_mode_prints_failing_values() {
    let Some(rs) = both(
        &Case::new("mismatch")
            .output(Output::OrgsMismatch)
            .env("ONLY_TABLES", "gha_orgs"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    let lines = rs.lines();
    let at = lines.iter().position(|l| l == "Failing values:").unwrap();
    // Go's `%+v` of the scanned values: int64, string, time.Time from the
    // driver (nameless fixed zone), nil and raw numeric bytes.
    assert_eq!(
        &lines[at..at + 6],
        [
            "Failing values:",
            "0: 1",
            "1: org1",
            "2: <nil>",
            "3: [49 46 53 48 48]",
            "PqError: code=42703, name=undefined_column, detail=",
        ]
    );
    assert_eq!(
        rs.error().as_deref(),
        Some("pq: column \"extra\" of relation \"gha_orgs\" does not exist")
    );
    assert_eq!(rs.count("gha_orgs"), 0);
}

#[test]
fn column_mismatch_row_mode_time_value() {
    // Input #2 first: its first row carries a timestamp and a NULL numeric.
    let Some(rs) = both(
        &Case::new("mismatch2")
            .output(Output::OrgsMismatch)
            .input_dbs("{db:i2},{db:i1}")
            .env("ONLY_TABLES", "gha_orgs"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    let lines = rs.lines();
    let at = lines.iter().position(|l| l == "Failing values:").unwrap();
    assert_eq!(
        &lines[at..at + 5],
        [
            "Failing values:",
            "0: 2",
            "1: org2",
            "2: 2024-02-03 04:05:06 +0000 +0000",
            "3: <nil>",
        ]
    );
}

#[test]
fn column_mismatch_batch_mode() {
    let Some(rs) = both(
        &Case::new("mismatchb")
            .output(Output::OrgsMismatch)
            .env("ONLY_TABLES", "gha_orgs")
            .env("USE_BATCH", "1")
            .env("BATCH_SIZE", "2"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert!(rs.has_line("Failing batch insert into gha_orgs (rows: 2, columns: 4)"));
    assert!(rs.has_line("PqError: code=42703, name=undefined_column, detail="));
    assert_eq!(
        rs.error().as_deref(),
        Some("pq: column \"extra\" of relation \"gha_orgs\" does not exist")
    );
}

#[test]
fn column_mismatch_durable_pq_is_not_retried() {
    let Some(rs) = both(
        &Case::new("mismatchd")
            .output(Output::OrgsMismatch)
            .env("ONLY_TABLES", "gha_orgs")
            .env("DURABLE_PQ", "1"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(2));
    assert!(rs.has_line("undefined_column error is not retryable, even with DURABLE_PQ"));
}

// ---------------------------------------------------------------------------
// Query echo
// ---------------------------------------------------------------------------

#[test]
fn qout_echoes_queries_and_arguments() {
    let Some(rs) = both(
        &Case::new("qout")
            .env("GHA2DB_QOUT", "1")
            .env("ONLY_TABLES", "gha_texts,gha_orgs,gha_events")
            .env("MERGE_DT_FROM", "2024-01-05T10:00:00Z"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.lines();
    assert!(lines.contains(
        &"select count(*) from gha_events where id > 0 and created_at >= $1".to_string()
    ));
    assert!(lines.contains(&"[1:2024-01-05 10:00:00 +0000 UTC ]".to_string()));
    assert!(lines.contains(&"select * from gha_texts where created_at >= $1".to_string()));
    assert!(lines.contains(&"select count(*) from gha_orgs".to_string()));
    assert!(lines.contains(
        &"insert into gha_texts(\"event_id\", \"body\", \"created_at\", \"repo_id\", \"actor_login\", \"type\", \"score\", \"tags\", \"meta\") values($1, $2, $3, $4, $5, $6, $7, $8, $9)".to_string()
    ));
    // Driver timestamps print with the nameless zone, raw bytes like
    // `FormatRawBytes`, NULLs as `(null)`.
    assert!(lines.contains(
        &"[1:5 2:text 5 db1 3:2024-01-05 10:00:00 +0000 +0000 4:5 5:actor5 6:PushEvent 7:[]uint8(5):372e353030:[55 46 53 48 48] 8:[]uint8(6):7b612c62357d:[123 97 44 98 53 125] 9:[]uint8(8):7b226e223a20357d:[123 34 110 34 58 32 53 125] ]".to_string()
    ));
    assert!(lines.contains(
        &"[1:6 2:text 6 db1 3:2024-01-06 10:00:00 +0000 +0000 4:6 5:actor6 6:PushEvent 7:(null) 8:(null) 9:[]uint8(8):7b226e223a20367d:[123 34 110 34 58 32 54 125] ]".to_string()
    ));
    assert!(lines
        .iter()
        .any(|l| l.starts_with("[1:merge_dbs 2: 3:<time> 4:Compiled None")));
}

#[test]
fn qout_batch_mode() {
    let Some(rs) = both(
        &Case::new("qoutb")
            .env("GHA2DB_QOUT", "1")
            .env("ONLY_TABLES", "gha_orgs")
            .env("USE_BATCH", "1")
            .env("BATCH_SIZE", "4"),
    ) else {
        return;
    };
    assert_eq!(rs.out.code, Some(0));
    let lines = rs.lines();
    assert!(lines.contains(
        &"insert into gha_orgs(\"id\", \"login\") values ($1,$2),($3,$4),($5,$6),($7,$8) on conflict do nothing".to_string()
    ));
    assert!(lines.contains(
        &"insert into gha_orgs(\"id\", \"login\") values ($1,$2),($3,$4) on conflict do nothing"
            .to_string()
    ));
    assert!(lines.contains(&"[1:1 2:org1 3:2 4:org2 5:3 6:org3 7:4 8:org4 ]".to_string()));
    assert!(lines.contains(&"[1:5 2:org5 3:6 4:org6 ]".to_string()));
}

// ---------------------------------------------------------------------------
// Sanity of the harness
// ---------------------------------------------------------------------------

#[test]
fn seed_covers_every_merged_table() {
    let sql = seed_sql(1);
    for t in MERGED_TABLES {
        assert!(sql.contains(&format!("insert into {t} values")), "{t}");
    }
    assert!(sql.contains("insert into gha_companies values"));
    assert!(sql
        .contains("(1, 'org1'), (2, 'org2'), (3, 'org3'), (4, 'org4'), (5, 'org5'), (6, 'org6')"));
    assert!(seed_sql(2)
        .contains("(4, 'org4'), (5, 'org5'), (6, 'org6'), (7, 'org7'), (8, 'org8'), (9, 'org9')"));
}
