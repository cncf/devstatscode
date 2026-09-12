//! Go ⇄ Rust compatibility tests for `structure`.
//!
//! Every scenario runs the Go binary against its own scratch database
//! (`dbtest_structure_<name>_go`) and the Rust binary against another
//! (`dbtest_structure_<name>_rs`), with identical environment, stdin and
//! working directory — the invocations are the ones used by
//! `cncf/devstats/<project>/psql.sh` — then compares:
//!
//! * exit code, stdout (run-time durations masked) and, for fatal errors, the
//!   `Error: '…'` line of stderr;
//! * the complete resulting schema (tables, columns, constraints, indexes,
//!   views, functions, extensions) — both via catalog queries and, when
//!   `pg_dump` is installed, via `pg_dump --schema-only`;
//! * the data of the tables the tool fills (countries, bot logins, …) and the
//!   row counts of everything else.
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped otherwise).

use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{
    fixture, go_binary, mask_go_durations, run, rust_binary, Invocation, Outcome,
};

fn go_bin() -> Option<PathBuf> {
    go_binary("structure")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_structure"))
}

/// Directory holding the real `util_sql/*.sql` scripts copied from
/// `cncf/devstats` (what `GHA2DB_LOCAL=1 structure` reads from `./`).
fn scripts_dir() -> PathBuf {
    fixture("structure")
}

/// One side of a scenario: the database it worked on and its outcome.
struct Side {
    db: TestDb,
    out: Outcome,
}

/// How a scenario prepares its databases and runs the binaries.
struct Scenario<'a> {
    name: &'a str,
    /// The database does not exist before the run (the tool must create it).
    absent: bool,
    /// SQL run on the fresh database before the binary (ignored when absent).
    setup: Vec<String>,
    /// Extra environment on top of the `PG_*` connection variables.
    env: Vec<(&'a str, &'a str)>,
    stdin: Vec<u8>,
    cwd: PathBuf,
}

impl<'a> Scenario<'a> {
    fn new(name: &'a str) -> Self {
        Scenario {
            name,
            absent: false,
            setup: Vec::new(),
            env: vec![("GHA2DB_LOCAL", "1")],
            stdin: Vec::new(),
            cwd: scripts_dir(),
        }
    }
    fn absent(mut self) -> Self {
        self.absent = true;
        self
    }
    fn setup(mut self, sql: &str) -> Self {
        self.setup.push(sql.to_string());
        self
    }
    fn env(mut self, k: &'a str, v: &'a str) -> Self {
        self.env.push((k, v));
        self
    }
    fn no_env(mut self, k: &str) -> Self {
        self.env.retain(|(key, _)| *key != k);
        self
    }
    fn stdin(mut self, s: &str) -> Self {
        self.stdin = s.as_bytes().to_vec();
        self
    }
    fn cwd(mut self, p: impl Into<PathBuf>) -> Self {
        self.cwd = p.into();
        self
    }
}

/// Run `bin` on its own database per the scenario.
fn run_side(bin: &Path, sc: &Scenario<'_>, suffix: &str) -> Option<Side> {
    let name = format!("structure_{}_{}", sc.name, suffix);
    let db = if sc.absent {
        TestDb::absent(&name)?
    } else {
        let db = TestDb::fresh(&name)?;
        for sql in &sc.setup {
            db.exec(sql);
        }
        db
    };
    let env = db.env();
    let mut inv = Invocation::new()
        .cwd(sc.cwd.clone())
        .stdin(sc.stdin.clone());
    for (k, v) in &env {
        inv = inv.env(k, v);
    }
    for (k, v) in &sc.env {
        inv = inv.env(k, v);
    }
    let out = run(bin, &inv);
    Some(Side { db, out })
}

/// `Error: '…'` lines of a fatal error report (the rest of stderr is a Go
/// stack trace / Go type names that the port does not reproduce).
fn error_lines(stderr: &str) -> Vec<String> {
    stderr
        .lines()
        .filter(|l| l.starts_with("Error: '"))
        .map(str::to_string)
        .collect()
}

/// Make the stdout of the two sides comparable: run-time durations, the
/// (different) database names and the `GHA2DB_QOUT` echo of the `gha_logs`
/// insert arguments (Go prints `time.Now()` with the zone abbreviation and
/// its monotonic reading `m=+0.0123`, which nothing can reproduce) are masked.
fn normalize_stdout(stdout: &str, db_name: &str) -> String {
    let masked = mask_go_durations(stdout).replace(db_name, "<db>");
    masked
        .split('\n')
        .map(|l| {
            if l.starts_with("[1:")
                && l.contains(" 3:")
                && l.contains(" 4:Compiled ")
                && l.ends_with(" ]")
            {
                "[<gha_logs insert args>]".to_string()
            } else {
                l.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Everything we compare about a database after a run.
struct DbState {
    schema: String,
    pg_dump: Option<String>,
    counts: Vec<(String, i64)>,
    data: Vec<(String, cpg::Snapshot)>,
}

/// Tables whose full content is compared (the ones the tool itself fills,
/// plus whatever the scenario put in); everything else by row count.
const DATA_TABLES: &[&str] = &[
    "gha_countries",
    "gha_bot_logins",
    "gha_postprocess_scripts",
    "gha_events",
    "gha_actors",
    "custom_table",
    "marker",
];

fn db_state(db: &TestDb) -> DbState {
    let con = db.conn();
    let tables = cpg::tables(&con);
    let data = DATA_TABLES
        .iter()
        .filter(|t| tables.iter().any(|x| x == *t))
        .map(|t| (t.to_string(), cpg::table_data(&con, t)))
        .collect();
    let st = DbState {
        schema: cpg::schema_dump(&con),
        pg_dump: cpg::pg_dump_schema(&db.ctx),
        counts: cpg::table_counts(&con),
        data,
    };
    con.close();
    st
}

/// Run both binaries and assert they agree (outcome + database). Returns the
/// Rust side for scenario-specific assertions; `None` when DB tests are off.
fn both(sc: &Scenario<'_>) -> Option<Side> {
    let rust = run_side(&rust_bin(), sc, "rs")?;
    if let Some(go) = go_bin() {
        let go = run_side(&go, sc, "go").unwrap();
        let ctx = format!(
            "\nscenario {:?} env {:?} stdin {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
            sc.name,
            sc.env,
            String::from_utf8_lossy(&sc.stdin),
            go.out.code,
            go.out.stdout_str(),
            go.out.stderr_str(),
            rust.out.code,
            rust.out.stdout_str(),
            rust.out.stderr_str()
        );
        assert_eq!(go.out.code, rust.out.code, "exit code{ctx}");
        assert_eq!(
            normalize_stdout(&go.out.stdout_str(), &go.db.name),
            normalize_stdout(&rust.out.stdout_str(), &rust.db.name),
            "stdout{ctx}"
        );
        assert_eq!(
            error_lines(&go.out.stderr_str()),
            error_lines(&rust.out.stderr_str()),
            "fatal error lines{ctx}"
        );
        assert_eq!(go.db.exists(), rust.db.exists(), "database existence{ctx}");
        if go.db.exists() {
            let g = db_state(&go.db);
            let r = db_state(&rust.db);
            assert_eq!(g.schema, r.schema, "schema (catalog dump){ctx}");
            assert_eq!(g.counts, r.counts, "row counts{ctx}");
            assert_eq!(g.data, r.data, "table data{ctx}");
            if let (Some(gd), Some(rd)) = (&g.pg_dump, &r.pg_dump) {
                assert_eq!(gd, rd, "pg_dump --schema-only{ctx}");
            }
        }
    }
    Some(rust)
}

/// The stdout of a run that created the database and built everything.
const CREATED_STDOUT: &str = "Compiled None, commit: None on None using None\nTime: <duration>\n";
/// The stdout of a run on an existing database answered with `y`.
const RECREATE_STDOUT: &str = "Compiled None, commit: None on None using None\nThis program will recreate DB structure (dropping all existing data)\nContinue? (y/n) \nTime: <duration>\n";

fn tables_of(db: &TestDb) -> Vec<String> {
    let con = db.conn();
    let t = cpg::tables(&con);
    con.close();
    t
}

fn count(db: &TestDb, sql: &str) -> i64 {
    let con = db.conn();
    let n = cpg::snapshot(&con, sql, &[]).rows[0][0].parse().unwrap();
    con.close();
    n
}

/// Every table `structure.go` creates in legacy (project-local) mode.
const ALL_TABLES: &[&str] = &[
    "gha_actors",
    "gha_actors_affiliations",
    "gha_actors_emails",
    "gha_actors_names",
    "gha_assets",
    "gha_bot_logins",
    "gha_branches",
    "gha_comments",
    "gha_commits",
    "gha_commits_files",
    "gha_commits_roles",
    "gha_companies",
    "gha_computed",
    "gha_countries",
    "gha_events",
    "gha_events_commits_files",
    "gha_forkees",
    "gha_imported_shas",
    "gha_issues",
    "gha_issues_assignees",
    "gha_issues_events_labels",
    "gha_issues_labels",
    "gha_issues_pull_requests",
    "gha_labels",
    "gha_last_computed",
    "gha_logs",
    "gha_milestones",
    "gha_orgs",
    "gha_pages",
    "gha_parsed",
    "gha_payloads",
    "gha_postprocess_scripts",
    "gha_pull_requests",
    "gha_pull_requests_assignees",
    "gha_pull_requests_requested_reviewers",
    "gha_releases",
    "gha_releases_assets",
    "gha_repo_groups",
    "gha_repos",
    "gha_repos_langs",
    "gha_reviews",
    "gha_skip_commits",
    "gha_teams",
    "gha_teams_repositories",
    "gha_texts",
    "gha_vars",
];

/// Tables that live in the shared affiliations database when
/// `GHA2DB_AFFILIATIONS_DB` is set (not created project-locally then).
const SHARED_TABLES: &[&str] = &[
    "gha_actors",
    "gha_actors_affiliations",
    "gha_actors_emails",
    "gha_actors_names",
    "gha_bot_logins",
    "gha_companies",
    "gha_countries",
    "gha_imported_shas",
];

// ---------------------------------------------------------------------------
// The first `psql.sh` invocation: database missing, full structure + tools
// ---------------------------------------------------------------------------

#[test]
fn creates_missing_database_with_full_structure() {
    // devstats/<proj>/psql.sh: GHA2DB_PROJECT=<proj> PG_DB=<db> GHA2DB_LOCAL=1 structure
    let Some(r) = both(
        &Scenario::new("create")
            .absent()
            .env("GHA2DB_PROJECT", "kubernetes"),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    assert_eq!(mask_go_durations(&r.out.stdout_str()), CREATED_STDOUT);
    assert!(r.db.exists());
    let mut expected: Vec<String> = ALL_TABLES.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(tables_of(&r.db), expected);
    // tools ran: country codes and bot logins were loaded from ./util_sql/
    let countries = count(&r.db, "select count(*) from gha_countries");
    assert!(countries > 200, "gha_countries has {countries} rows");
    assert!(count(&r.db, "select count(*) from gha_bot_logins") > 50);
    assert_eq!(
        count(
            &r.db,
            "select count(*) from gha_countries where code = 'pl' and name = 'Poland'"
        ),
        1
    );
    // no indexes yet (GHA2DB_INDEX not set) except the primary keys
    assert_eq!(
        count(&r.db, "select count(*) from pg_indexes where schemaname = 'public' and indexname not like '%_pkey'"),
        0
    );
    // gha_postprocess_scripts starts empty; the shared-DB variants were not used
    assert_eq!(
        count(&r.db, "select count(*) from gha_postprocess_scripts"),
        0
    );
}

#[test]
fn skip_tools_creates_tables_only() {
    // GHA2DB_SKIPTOOLS=1: no scripts are read, so the working directory does not matter
    let dir = tempfile::tempdir().unwrap();
    let Some(r) = both(
        &Scenario::new("skiptools")
            .absent()
            .env("GHA2DB_SKIPTOOLS", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0);
    assert_eq!(mask_go_durations(&r.out.stdout_str()), CREATED_STDOUT);
    assert_eq!(count(&r.db, "select count(*) from gha_countries"), 0);
    assert_eq!(count(&r.db, "select count(*) from gha_bot_logins"), 0);
    assert_eq!(tables_of(&r.db).len(), ALL_TABLES.len());
}

// ---------------------------------------------------------------------------
// Existing database: the `Continue? (y/n)` prompt
// ---------------------------------------------------------------------------

#[test]
fn existing_database_recreated_after_mgetc_y() {
    // Pre-existing project tables with data are dropped and recreated; foreign
    // (non-DevStats) tables are left alone.
    let Some(r) = both(
        &Scenario::new("mgetc_y")
            .setup("create table gha_events(id bigint primary key, junk text)")
            .setup("insert into gha_events values (1, 'old'), (2, 'data')")
            .setup("create table custom_table(x int, y text)")
            .setup("insert into custom_table values (1, 'keep me')")
            .env("GHA2DB_MGETC", "y"),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0);
    assert_eq!(mask_go_durations(&r.out.stdout_str()), RECREATE_STDOUT);
    assert_eq!(count(&r.db, "select count(*) from gha_events"), 0);
    assert_eq!(count(&r.db, "select count(*) from custom_table"), 1);
    assert_eq!(
        count(&r.db, "select count(*) from information_schema.columns where table_name = 'gha_events' and column_name = 'junk'"),
        0
    );
    assert_eq!(tables_of(&r.db).len(), ALL_TABLES.len() + 1);
}

#[test]
fn existing_database_untouched_after_mgetc_n() {
    // GHA2DB_MGETC is truncated to its first character, so "no" behaves like "n"
    for (answer, tag) in [("n", "n"), ("Y", "upper_y"), ("no", "word"), (" ", "space")] {
        let Some(r) = both(
            &Scenario::new(&format!("mgetc_{tag}"))
                .setup("create table custom_table(x int)")
                .env("GHA2DB_MGETC", answer),
        ) else {
            return;
        };
        assert_eq!(r.out.code(), 0);
        assert_eq!(
            mask_go_durations(&r.out.stdout_str()),
            RECREATE_STDOUT,
            "answer {answer:?}"
        );
        // only "y" (exactly) proceeds
        assert_eq!(
            tables_of(&r.db),
            vec!["custom_table".to_string()],
            "answer {answer:?}"
        );
    }
    // "yes" is truncated to "y" and therefore recreates the structure
    let Some(r) = both(
        &Scenario::new("mgetc_yes")
            .setup("create table custom_table(x int)")
            .env("GHA2DB_MGETC", "yes"),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0);
    assert!(tables_of(&r.db).contains(&"gha_events".to_string()));
    assert!(
        tables_of(&r.db).contains(&"custom_table".to_string()),
        "existing tables are kept"
    );
}

#[test]
fn answer_read_from_stdin_when_mgetc_unset() {
    // interactive: one byte is read from stdin
    let Some(r) = both(&Scenario::new("stdin_y").stdin("y\n")) else {
        return;
    };
    assert_eq!(r.out.code(), 0);
    assert_eq!(mask_go_durations(&r.out.stdout_str()), RECREATE_STDOUT);
    assert_eq!(tables_of(&r.db).len(), ALL_TABLES.len());

    let Some(r) = both(&Scenario::new("stdin_n").stdin("no\n")) else {
        return;
    };
    assert_eq!(r.out.code(), 0);
    assert!(tables_of(&r.db).is_empty());

    // EOF on stdin is a fatal error (exit 2) before anything is touched
    let Some(r) = both(&Scenario::new("stdin_eof")) else {
        return;
    };
    assert_eq!(r.out.code(), 2);
    assert_eq!(
        error_lines(&r.out.stderr_str()),
        vec!["Error: 'EOF'".to_string()]
    );
    assert_eq!(
        r.out.stdout_str(),
        "Compiled None, commit: None on None using None\nThis program will recreate DB structure (dropping all existing data)\nContinue? (y/n) "
    );
    assert!(tables_of(&r.db).is_empty());
}

// ---------------------------------------------------------------------------
// The second `psql.sh` invocation: indexes only, tables kept
// ---------------------------------------------------------------------------

#[test]
fn second_pass_adds_indexes_and_keeps_data() {
    // psql.sh: GHA2DB_MGETC=y GHA2DB_SKIPTABLE=1 GHA2DB_INDEX=1 structure — after gha2db filled the tables.
    // The pre-state (all tables) comes from the Go-generated fixture, not from the binary under test.
    let sc =
        Scenario::new("indexes")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup("insert into gha_events(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name) values (42, 'PushEvent', 1, 2, '2020-01-02 03:04:05', 'someone', 'org/repo')")
            .setup("insert into gha_postprocess_scripts(ord, path) values (1, 'util_sql/postprocess_texts.sql'), (2, 'util_sql/postprocess_labels.sql'), (3, 'util_sql/postprocess_issues_prs.sql'), (6, 'util_sql/postprocess_commits.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_INDEX", "1");
    let Some(r) = both(&sc) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    // no "will recreate" warning without GHA2DB_SKIPTABLE unset
    assert_eq!(
        mask_go_durations(&r.out.stdout_str()),
        "Compiled None, commit: None on None using None\nContinue? (y/n) \nTime: <duration>\n"
    );
    assert_eq!(count(&r.db, "select count(*) from gha_events"), 1);
    let idx = count(&r.db, "select count(*) from pg_indexes where schemaname = 'public' and indexname not like '%_pkey'");
    assert!(idx > 200, "only {idx} indexes created");
    assert_eq!(
        count(&r.db, "select count(*) from pg_indexes where indexname = 'events_repo_name_created_at_idx' and indexdef like '%(repo_id, dup_repo_name, created_at)'"),
        1
    );
    // the real postprocess scripts ran (on the tiny data set) without error
    assert_eq!(
        count(&r.db, "select count(*) from gha_postprocess_scripts"),
        4
    );
}

#[test]
fn index_pass_is_idempotent() {
    // running the index pass twice must not fail (create index if not exists)
    let dir = tempfile::tempdir().unwrap();
    let Some(r) = both(
        &Scenario::new("reindex")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(&fs::read_to_string(fixture("structure/full_indexes.sql")).unwrap())
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_SKIPTOOLS", "1")
            .env("GHA2DB_INDEX", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
}

// ---------------------------------------------------------------------------
// Tools: postprocess scripts, shared affiliations mode, bounded range mode
// ---------------------------------------------------------------------------

/// A data directory with tiny marker scripts in place of the real ones, so
/// the script selection logic (order, shared-DB substitution, range mode) is
/// observable in table `marker`.
fn marker_data_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let us = dir.path().join("util_sql");
    fs::create_dir(&us).unwrap();
    let scripts = [
        ("postprocess_texts.sql", "texts"),
        ("postprocess_labels.sql", "labels"),
        ("postprocess_issues_prs.sql", "issues_prs"),
        ("postprocess_commits.sql", "commits"),
        ("postprocess_commits_shared.sql", "commits_shared"),
        ("country_codes.sql", "country_codes"),
        ("exclude_bots_table_insert.sql", "exclude_bots"),
        ("update_affiliations.sql", "update_affiliations"),
        ("custom_one.sql", "custom_one"),
        ("custom_two.sql", "custom_two"),
    ];
    for (file, tag) in scripts {
        fs::write(
            us.join(file),
            format!("insert into marker(tag, seq) values ('{tag}', (select coalesce(max(seq), 0) + 1 from marker));\n"),
        )
        .unwrap();
    }
    for (file, tag) in [
        ("postprocess_texts_range.sql", "texts_range"),
        ("postprocess_labels_range.sql", "labels_range"),
        ("postprocess_issues_prs_range.sql", "issues_prs_range"),
    ] {
        // the range scripts see the bounds through session settings set in the same batch
        fs::write(
            us.join(file),
            format!("insert into marker(tag, seq) values ('{tag} ' || current_setting('devstats.postprocess_from') || ' .. ' || current_setting('devstats.postprocess_to'), (select coalesce(max(seq), 0) + 1 from marker));\n"),
        )
        .unwrap();
    }
    fs::write(
        us.join("bad_sql.sql"),
        "insert into marker(tag, seq) values ('bad', 1);\nselect bogus from;\n",
    )
    .unwrap();
    dir
}

const MARKER_SETUP: &str = "create table marker(tag text, seq int)";

fn markers(db: &TestDb) -> Vec<String> {
    let con = db.conn();
    let m = cpg::snapshot(&con, "select tag from marker order by seq", &[]).column(0);
    con.close();
    m
}

#[test]
fn postprocess_scripts_run_in_order_then_tools() {
    let dir = marker_data_dir();
    let Some(r) = both(
        &Scenario::new("scripts")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (10, 'util_sql/custom_two.sql'), (6, 'util_sql/postprocess_commits.sql'), (1, 'util_sql/postprocess_texts.sql'), (5, 'util_sql/custom_one.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    assert_eq!(
        markers(&r.db),
        vec![
            "texts",
            "custom_one",
            "commits",
            "custom_two",
            "country_codes",
            "exclude_bots",
            "update_affiliations"
        ]
    );
}

#[test]
fn shared_affiliations_db_mode() {
    // GHA2DB_AFFILIATIONS_DB=<shared>: actor/company/country/bot tables are not
    // created locally, postprocess_commits.sql is swapped for the _shared
    // variant and the three affiliation-related tool scripts are skipped.
    let dir = marker_data_dir();
    let Some(r) = both(
        &Scenario::new("shared")
            .absent()
            .env("GHA2DB_AFFILIATIONS_DB", "sharedaffs")
            .env("GHA2DB_SKIPTOOLS", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    let tables = tables_of(&r.db);
    for t in SHARED_TABLES {
        assert!(
            !tables.contains(&t.to_string()),
            "{t} must not be created in shared mode"
        );
    }
    assert_eq!(tables.len(), ALL_TABLES.len() - SHARED_TABLES.len());

    let Some(r) = both(
        &Scenario::new("shared_scripts")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (1, 'util_sql/postprocess_texts.sql'), (6, 'util_sql/postprocess_commits.sql')")
            .env("GHA2DB_AFFILIATIONS_DB", "sharedaffs")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    assert_eq!(markers(&r.db), vec!["texts", "commits_shared"]);
}

#[test]
fn bounded_postprocess_range_mode() {
    let dir = marker_data_dir();
    let Some(r) = both(
        &Scenario::new("range")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (1, 'util_sql/postprocess_texts.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_POSTPROCESS_FROM", "2024-01-15")
            .env("GHA2DB_POSTPROCESS_TO", "2024-02-01T12:30:00Z")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    assert_eq!(
        mask_go_durations(&r.out.stdout_str()),
        "Compiled None, commit: None on None using None\nContinue? (y/n) \nPostprocess: bounded rebuild of generated tables for range [2024-01-15 00:00:00, 2024-02-01 12:30:00)\nTime: <duration>\n"
    );
    // only the three range scripts, with the canonicalized bounds visible in-session
    assert_eq!(
        markers(&r.db),
        vec![
            "texts_range 2024-01-15 00:00:00 .. 2024-02-01 12:30:00",
            "labels_range 2024-01-15 00:00:00 .. 2024-02-01 12:30:00",
            "issues_prs_range 2024-01-15 00:00:00 .. 2024-02-01 12:30:00",
        ]
    );
}

#[test]
fn bounded_range_with_real_scripts() {
    // the real *_range.sql scripts on the freshly built (empty) tables
    let Some(r) = both(
        &Scenario::new("range_real")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_POSTPROCESS_FROM", "2024-01-01 00:00:00")
            .env("GHA2DB_POSTPROCESS_TO", "2024-02-01 00:00:00"),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
}

#[test]
fn postprocess_range_validation_is_fatal() {
    // only one bound set / from >= to: Ctx.Init dies before touching the database
    for (from, to, msg) in [
        ("2024-01-01", "", "Error: 'GHA2DB_POSTPROCESS_FROM and GHA2DB_POSTPROCESS_TO must both be set (or both empty), got from='2024-01-01' to='''"),
        ("2024-02-01", "2024-01-01", "Error: 'GHA2DB_POSTPROCESS_FROM (2024-02-01) must be strictly before GHA2DB_POSTPROCESS_TO (2024-01-01)'"),
        ("2024-01-01", "2024-01-01", "Error: 'GHA2DB_POSTPROCESS_FROM (2024-01-01) must be strictly before GHA2DB_POSTPROCESS_TO (2024-01-01)'"),
    ] {
        let Some(r) = both(
            &Scenario::new(&format!("range_bad_{}", from.replace('-', "")))
                .absent()
                .env("GHA2DB_POSTPROCESS_FROM", from)
                .env("GHA2DB_POSTPROCESS_TO", to),
        ) else {
            return;
        };
        assert_eq!(r.out.code(), 2, "{from} {to}");
        assert_eq!(error_lines(&r.out.stderr_str()), vec![msg.to_string()], "{from} {to}");
        assert!(!r.db.exists(), "database must not be created");
    }
}

#[test]
fn data_dir_is_used_without_local_mode() {
    // cron mode: scripts come from GHA2DB_DATADIR (default /etc/gha2db/), not from ./
    let dir = marker_data_dir();
    let datadir = format!("{}/", dir.path().display());
    let Some(r) = both(
        &Scenario::new("datadir")
            .no_env("GHA2DB_LOCAL")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (1, 'util_sql/custom_one.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_DATADIR", &datadir)
            .cwd(tempfile::tempdir().unwrap().path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    assert_eq!(
        markers(&r.db),
        vec![
            "custom_one",
            "country_codes",
            "exclude_bots",
            "update_affiliations"
        ]
    );

    // GHA2DB_LOCAL wins over GHA2DB_DATADIR: scripts are read from ./ (which has none here)
    let Some(r) = both(
        &Scenario::new("datadir_local")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_DATADIR", &datadir)
            .cwd(tempfile::tempdir().unwrap().path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 2);
    assert_eq!(
        error_lines(&r.out.stderr_str()),
        vec!["Error: 'open ./util_sql/country_codes.sql: no such file or directory'".to_string()]
    );
}

#[test]
fn debug_output_lists_executed_scripts() {
    let dir = marker_data_dir();
    let Some(r) = both(
        &Scenario::new("debug")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (2, 'util_sql/custom_two.sql'), (1, 'util_sql/custom_one.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_DEBUG", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    let out = mask_go_durations(&r.out.stdout_str());
    for expected in [
        "Executed script: util_sql/custom_one.sql: took <duration>\n",
        "Executed script: util_sql/custom_two.sql: took <duration>\n",
        "Executed countries script: util_sql/country_codes.sql: took <duration>\n",
        "Executed bot logins table insert script: util_sql/exclude_bots_table_insert.sql: took <duration>\n",
        "Updated missing affiliations for multiple ID actors script: util_sql/update_affiliations.sql: took <duration>\n",
    ] {
        assert!(out.contains(expected), "missing {expected:?} in\n{out}");
    }
}

#[test]
fn qout_prints_every_statement() {
    // GHA2DB_QOUT=1 echoes every SQL statement: the strongest check that the
    // Rust port issues exactly the Go statements, in the Go order.
    let Some(r) = both(&Scenario::new("qout").absent().env("GHA2DB_QOUT", "1")) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    let out = r.out.stdout_str();
    assert!(out.contains("drop table if exists gha_events\n"));
    assert!(out.contains("create table gha_events(id bigint not null primary key, type varchar(40) not null, actor_id bigint not null, repo_id bigint not null, created_at timestamp not null, org_id bigint, dup_actor_login varchar(120) not null, dup_repo_name varchar(160) not null)\n"));
    assert!(out.contains("create table gha_imported_shas(sha text not null, dt timestamp default now() not null, primary key(sha))\n"));
    assert!(out.contains(
        "insert into gha_countries(code, name) values('pl', 'Poland') on conflict do nothing;\n"
    ));
    // 44 tables dropped+created, no indexes
    assert_eq!(
        out.matches("\ndrop table if exists ").count(),
        ALL_TABLES.len()
    );
    assert_eq!(out.matches("\ncreate table ").count(), ALL_TABLES.len());
    assert!(!out.contains("create index"));

    let Some(r) = both(
        &Scenario::new("qout_idx")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_SKIPTOOLS", "1")
            .env("GHA2DB_INDEX", "1")
            .env("GHA2DB_QOUT", "1"),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 0, "{}", r.out.stderr_str());
    let out = r.out.stdout_str();
    assert!(out.matches("\ncreate index if not exists ").count() > 200);
    assert!(!out.contains("drop table"));
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

#[test]
fn missing_script_is_fatal() {
    let dir = marker_data_dir();
    let Some(r) = both(
        &Scenario::new("noscript")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (1, 'util_sql/custom_one.sql'), (2, 'util_sql/nope.sql'), (3, 'util_sql/custom_two.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 2);
    assert_eq!(
        error_lines(&r.out.stderr_str()),
        vec!["Error: 'open ./util_sql/nope.sql: no such file or directory'".to_string()]
    );
    // scripts before the missing one ran, the ones after did not
    assert_eq!(markers(&r.db), vec!["custom_one"]);

    // with a project set, ReadFile retries the "shared" path and reports on stdout
    let Some(r) = both(
        &Scenario::new("noscript_proj")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (1, 'util_sql/nope.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .env("GHA2DB_PROJECT", "kubernetes")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 2);
    assert!(r
        .out
        .stdout_str()
        .ends_with("lib.ReadFile('./util_sql/nope.sql'): error: open ./util_sql/nope.sql: no such file or directory\n"));
}

#[test]
fn sql_error_in_script_is_fatal() {
    let dir = marker_data_dir();
    let Some(r) = both(
        &Scenario::new("badsql")
            .setup(&fs::read_to_string(fixture("structure/full_structure.sql")).unwrap())
            .setup(MARKER_SETUP)
            .setup("insert into gha_postprocess_scripts(ord, path) values (1, 'util_sql/bad_sql.sql'), (2, 'util_sql/custom_one.sql')")
            .env("GHA2DB_MGETC", "y")
            .env("GHA2DB_SKIPTABLE", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 2);
    assert_eq!(
        error_lines(&r.out.stderr_str()),
        vec!["Error: 'pq: syntax error at or near \";\"'".to_string()]
    );
    // the whole script is one statement batch => rolled back, and nothing after it ran
    assert!(markers(&r.db).is_empty());
    assert!(r
        .out
        .stdout_str()
        .contains("PqError: code=42601, name=syntax_error, detail=\n"));
}

#[test]
fn unreachable_server_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    // PG_PORT overrides the test server's port: nothing listens on 1
    let Some(r) = both(
        &Scenario::new("noserver")
            .absent()
            .env("PG_PORT", "1")
            .cwd(dir.path()),
    ) else {
        return;
    };
    assert_eq!(r.out.code(), 2);
    let errs = error_lines(&r.out.stderr_str());
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(errs[0].contains("connection refused"), "{errs:?}");
    assert!(!r.db.exists());
}
