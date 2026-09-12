//! PostgreSQL test support shared by the `lib-pg` tests and by the
//! compatibility tests of the DB-using binaries (`structure`, `runq`, ...).
//!
//! The server is taken from the very variables the tools read — `PG_HOST`,
//! `PG_PORT`, `PG_USER`, `PG_PASS`, `PG_SSL` (`test.sh` detects a local server
//! and exports them). Like the Go `TestPostgres`, DB tests only run when
//! `PG_DB=dbtest`; otherwise (or when `DEVSTATS_SKIP_DB_TESTS=1`) they skip
//! with a message. Each test works in its own scratch database
//! `dbtest_<name>` — dropped and re-created by [`TestDb::fresh`], dropped
//! again when the [`TestDb`] is dropped — so tests can run in parallel and a
//! crashed run leaves nothing but `dbtest_*` databases behind.

use std::process::Command;

use devstatscode::chrono::SecondsFormat;
use devstatscode::context::Ctx;
use devstatscode::pg::{self, DriverValue, PgConn, SqlArg};

/// Connection variables understood by the tools; [`crate::run`] removes them
/// from the child environment so a test always passes them explicitly.
pub const PG_ENV_VARS: &[&str] = &[
    "PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS", "PG_SSL",
];

/// The database name the DB tests require in `PG_DB` (Go `TestPostgres` guard).
pub const GUARD_DB: &str = "dbtest";

/// Marker table [`TestDb::fresh_named`] creates in databases it owns.
pub const OWNERSHIP_MARKER: &str = "devstats_compat_fixture";

/// True when the DB tests must be skipped (prints the reason).
pub fn db_tests_skipped() -> bool {
    if std::env::var_os("DEVSTATS_SKIP_DB_TESTS").is_some_and(|v| !v.is_empty() && v != "0") {
        eprintln!("[compat] DEVSTATS_SKIP_DB_TESTS set — skipping PostgreSQL tests");
        return true;
    }
    match std::env::var("PG_DB") {
        Ok(db) if db == GUARD_DB => false,
        other => {
            eprintln!(
                "[compat] PG_DB={:?} — PostgreSQL tests only run with PG_DB={GUARD_DB} (see test.sh); skipping",
                other.unwrap_or_default()
            );
            true
        }
    }
}

/// A DevStats context for the test server (`Ctx::init()` from the `PG_*`
/// variables, `test_mode` set, DB logging disabled).
pub fn test_ctx() -> Ctx {
    let mut ctx = Ctx::default();
    ctx.init();
    ctx.test_mode = true;
    ctx.log_to_db = false;
    ctx
}

/// A scratch database `dbtest_<name>` on the test server.
pub struct TestDb {
    /// Context pointing at the scratch database.
    pub ctx: Ctx,
    /// Database name (`dbtest_<name>`).
    pub name: String,
    env: Vec<(String, String)>,
    dropped: bool,
}

impl std::fmt::Debug for TestDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestDb").field("name", &self.name).finish()
    }
}

impl TestDb {
    /// Drop (if it exists) and create `dbtest_<name>`; `None` when the DB
    /// tests are skipped. `name` must be a lowercase identifier.
    pub fn fresh(name: &str) -> Option<TestDb> {
        if db_tests_skipped() {
            return None;
        }
        assert!(
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "test database suffix must be a lowercase identifier: {name:?}"
        );
        let mut ctx = test_ctx();
        ctx.pg_db = format!("{GUARD_DB}_{name}");
        pg::drop_database_if_exists(&mut ctx);
        assert!(
            pg::create_database_if_needed(&mut ctx),
            "cannot create database {}",
            ctx.pg_db
        );
        Some(TestDb {
            name: ctx.pg_db.clone(),
            env: Self::child_env(&ctx),
            ctx,
            dropped: false,
        })
    }

    /// Create a database with an **exact** name — for tools that hardcode
    /// database names (`api` insists on `gha` for Kubernetes and `allprj` for
    /// the GitHub ID lookups). Every database created this way gets the
    /// marker table [`OWNERSHIP_MARKER`]; an existing database of that name
    /// is only dropped and recreated when it carries the marker, otherwise
    /// the call panics so real data is never destroyed.
    pub fn fresh_named(name: &str) -> Option<TestDb> {
        if db_tests_skipped() {
            return None;
        }
        assert!(
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "test database name must be a lowercase identifier: {name:?}"
        );
        let mut ctx = test_ctx();
        ctx.pg_db = name.to_string();
        if pg::database_exists(&mut ctx, false).0 {
            let con = pg::pg_conn(&ctx);
            let owned = pg::table_exists(&con, &ctx, OWNERSHIP_MARKER);
            con.close();
            assert!(
                owned,
                "database {name:?} exists and was not created by the compat harness (no {OWNERSHIP_MARKER} table) — refusing to drop it"
            );
            pg::drop_database_if_exists(&mut ctx);
        }
        assert!(
            pg::create_database_if_needed(&mut ctx),
            "cannot create database {}",
            ctx.pg_db
        );
        let db = TestDb {
            name: ctx.pg_db.clone(),
            env: Self::child_env(&ctx),
            ctx,
            dropped: false,
        };
        db.exec(&format!(
            "create table \"{OWNERSHIP_MARKER}\"(created_at timestamp not null default now())"
        ));
        Some(db)
    }

    fn child_env(ctx: &Ctx) -> Vec<(String, String)> {
        vec![
            ("PG_HOST".to_string(), ctx.pg_host.clone()),
            ("PG_PORT".to_string(), ctx.pg_port.clone()),
            ("PG_DB".to_string(), ctx.pg_db.clone()),
            ("PG_USER".to_string(), ctx.pg_user.clone()),
            ("PG_PASS".to_string(), ctx.pg_pass.clone()),
            ("PG_SSL".to_string(), ctx.pg_ssl.clone()),
            // Never let a binary under test log into the `devstats` database
            // or prefix its output with the (unreproducible) time stamp.
            ("GHA2DB_SKIPLOG".to_string(), "1".to_string()),
            ("GHA2DB_SKIPTIME".to_string(), "1".to_string()),
        ]
    }

    /// Make sure `dbtest_<name>` does **not** exist and return a handle whose
    /// `Drop` removes whatever the binary under test created there — for
    /// tests of the "database is missing, the tool creates it" path.
    pub fn absent(name: &str) -> Option<TestDb> {
        let mut db = TestDb::fresh(name)?;
        pg::drop_database_if_exists(&mut db.ctx);
        Some(db)
    }

    /// Does the database exist right now?
    pub fn exists(&self) -> bool {
        let mut ctx = self.ctx.clone();
        pg::database_exists(&mut ctx, false).0
    }

    /// A connection pool to the scratch database.
    pub fn conn(&self) -> PgConn {
        pg::pg_conn(&self.ctx)
    }

    /// Environment for a binary under test: `PG_*` for this database plus
    /// `GHA2DB_SKIPLOG=1` and `GHA2DB_SKIPTIME=1`.
    pub fn env(&self) -> Vec<(&str, &str)> {
        self.env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Run `psql`-free SQL through the library: `exec` on a fresh pool.
    pub fn exec(&self, sql: &str) {
        let con = self.conn();
        pg::exec_sql_with_err(&con, &self.ctx, sql, &[]);
        con.close();
    }

    /// Drop the database now (also done on `Drop`).
    pub fn drop_db(&mut self) {
        if self.dropped {
            return;
        }
        self.dropped = true;
        // Kill stray sessions (a binary under test may have crashed while
        // connected) — otherwise `drop database` fails.
        let mut ctx = self.ctx.clone();
        ctx.pg_db = "postgres".to_string();
        let con = pg::pg_conn(&ctx);
        let _ = con.exec(
            "select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()",
            &[SqlArg::from(self.name.as_str())],
        );
        con.close();
        pg::drop_database_if_exists(&mut self.ctx);
    }
}

/// Make sure the `devstats` logs database with its `gha_logs` table exists on
/// the test server — the sync programs' `ClearDBLogs` deletes old rows from
/// it (fatally when it is missing). Created once, never dropped; `None` when
/// the DB tests are skipped. Safe to call from parallel tests.
pub fn ensure_logs_db() -> Option<()> {
    if db_tests_skipped() {
        return None;
    }
    static DONE: std::sync::Mutex<bool> = std::sync::Mutex::new(false);
    let mut done = DONE.lock().unwrap();
    if *done {
        return Some(());
    }
    let mut ctx = test_ctx();
    ctx.pg_db = devstatscode::consts::DEVSTATS.to_string();
    let (exists, c) = pg::database_exists(&mut ctx, false);
    let c = c.expect("connection to the postgres database");
    if !exists {
        // Another test process may create it at the same time: tolerate.
        let _ = c.exec(&format!("create database {}", ctx.pg_db), &[]);
    }
    c.close();
    assert!(
        pg::database_exists(&mut ctx, true).0,
        "cannot create the {} logs database",
        ctx.pg_db
    );
    let con = pg::pg_conn(&ctx);
    let _ = con.exec(
        "create table if not exists gha_logs(id integer not null default 0, dt timestamp without time zone default now(), prog character varying(32) not null default '', proj character varying(32) not null default '', run_dt timestamp without time zone not null default now(), msg text)",
        &[],
    );
    assert!(
        pg::table_exists(&con, &ctx, "gha_logs"),
        "cannot create gha_logs in {}",
        ctx.pg_db
    );
    con.close();
    *done = true;
    Some(())
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // Keep the database for inspection when the test failed.
            eprintln!("[compat] test failed — keeping database {}", self.name);
            return;
        }
        self.drop_db();
    }
}

/// `ctx.pg_db`'s connection parameters as `psql` arguments
/// (`-h host -p port -U user -d db`; `PGPASSWORD` must be set separately).
pub fn psql_args(ctx: &Ctx) -> Vec<String> {
    vec![
        "-h".into(),
        ctx.pg_host.clone(),
        "-p".into(),
        ctx.pg_port.clone(),
        "-U".into(),
        ctx.pg_user.clone(),
        "-d".into(),
        ctx.pg_db.clone(),
    ]
}

/// Run `psql -X -q -A -t -c <sql>` against `ctx.pg_db` (requires `psql` on
/// `PATH`); returns stdout. Used to cross-check the library against the
/// reference client.
pub fn psql(ctx: &Ctx, sql: &str) -> Option<String> {
    let out = Command::new("psql")
        .args(psql_args(ctx))
        .args(["-X", "-q", "-A", "-t", "-c", sql])
        .env("PGPASSWORD", &ctx.pg_pass)
        .output()
        .ok()?;
    if !out.status.success() {
        panic!(
            "psql failed ({:?}): {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Is `psql` available?
pub fn have_psql() -> bool {
    Command::new("psql")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Snapshot of a query result: column names and every row rendered the way
/// Go's `%v` renders the driver values (`<nil>` for NULL). Handy to compare a
/// database written by the Go binary with one written by the Rust binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl Snapshot {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Single-column results flattened.
    pub fn column(&self, i: usize) -> Vec<String> {
        self.rows.iter().map(|r| r[i].clone()).collect()
    }
}

/// Run `sql` and snapshot the result (Go `%v` rendering of every value).
pub fn snapshot(con: &PgConn, sql: &str, args: &[SqlArg]) -> Snapshot {
    let mut rows = con
        .query(sql, args)
        .unwrap_or_else(|e| panic!("query {sql:?}: {e}"));
    let columns = rows.column_names();
    let mut out = Vec::new();
    while rows.next() {
        out.push(rows.values().iter().map(render).collect());
    }
    rows.err().unwrap_or_else(|e| panic!("query {sql:?}: {e}"));
    Snapshot { columns, rows: out }
}

/// Go `%v` of a driver value (`<nil>` for NULL, RFC 3339 for times so the
/// rendering does not depend on the session time zone).
pub fn render(v: &DriverValue) -> String {
    match v {
        DriverValue::Null => "<nil>".to_string(),
        DriverValue::Time(t) => t.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        other => other.go_string().unwrap_or_default(),
    }
}

/// All user tables of the connected database, sorted.
pub fn tables(con: &PgConn) -> Vec<String> {
    snapshot(
        con,
        "select tablename from pg_tables where schemaname = 'public' order by tablename",
        &[],
    )
    .column(0)
}

/// Column names and types of `table`, in definition order:
/// `(name, data_type, is_nullable, default)`.
pub fn table_columns(con: &PgConn, table: &str) -> Vec<ColumnInfo> {
    snapshot(
        con,
        "select column_name, data_type, is_nullable, coalesce(column_default, '') \
         from information_schema.columns where table_schema = 'public' and table_name = $1 \
         order by ordinal_position",
        &[SqlArg::from(table)],
    )
    .rows
    .into_iter()
    .map(|r| (r[0].clone(), r[1].clone(), r[2].clone(), r[3].clone()))
    .collect()
}

/// Index names and definitions of `table`, sorted by name.
pub fn table_indexes(con: &PgConn, table: &str) -> Vec<IndexInfo> {
    snapshot(
        con,
        "select indexname, indexdef from pg_indexes where schemaname = 'public' and tablename = $1 order by indexname",
        &[SqlArg::from(table)],
    )
    .rows
    .into_iter()
    .map(|r| (r[0].clone(), r[1].clone()))
    .collect()
}

/// `(column, type, nullable, default)` as reported by `information_schema`.
pub type ColumnInfo = (String, String, String, String);
/// `(index name, index definition)`.
pub type IndexInfo = (String, String);
/// One table: name, columns, indexes.
pub type TableSchema = (String, Vec<ColumnInfo>, Vec<IndexInfo>);

/// Full structural description of the database (every table with its
/// columns and indexes) — what `structure`-like tools produce.
pub fn schema(con: &PgConn) -> Vec<TableSchema> {
    tables(con)
        .into_iter()
        .map(|t| {
            let cols = table_columns(con, &t);
            let idx = table_indexes(con, &t);
            (t, cols, idx)
        })
        .collect()
}

/// Is the `hll` extension installable on the test server?
pub fn hll_available(con: &PgConn) -> bool {
    !snapshot(
        con,
        "select 1 from pg_available_extensions where name = 'hll'",
        &[],
    )
    .is_empty()
}

/// Textual description of everything in schema `public` that a DDL tool can
/// create: tables (columns with `format_type`, NOT NULL, defaults;
/// constraints; indexes), views, materialized views, sequences, functions
/// and the installed extensions. Two databases built by the Go and the Rust
/// binary must produce the same text.
pub fn schema_dump(con: &PgConn) -> String {
    let mut out = String::new();
    let tables = snapshot(
        con,
        "select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace \
         where n.nspname = 'public' and c.relkind in ('r', 'p') order by c.relname",
        &[],
    )
    .column(0);
    for t in &tables {
        out.push_str(&format!("table {t}\n"));
        let cols = snapshot(
            con,
            "select a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, \
             coalesce(pg_get_expr(d.adbin, d.adrelid), '') \
             from pg_attribute a left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum \
             where a.attrelid = ('public.' || quote_ident($1))::regclass and a.attnum > 0 and not a.attisdropped \
             order by a.attnum",
            &[SqlArg::from(t.as_str())],
        );
        for r in &cols.rows {
            out.push_str(&format!(
                "  column {} {}{}{}\n",
                r[0],
                r[1],
                if r[2] == "true" { " not null" } else { "" },
                if r[3].is_empty() {
                    String::new()
                } else {
                    format!(" default {}", r[3])
                }
            ));
        }
        let cons = snapshot(
            con,
            "select conname, pg_get_constraintdef(oid) from pg_constraint \
             where conrelid = ('public.' || quote_ident($1))::regclass order by conname",
            &[SqlArg::from(t.as_str())],
        );
        for r in &cons.rows {
            out.push_str(&format!("  constraint {} {}\n", r[0], r[1]));
        }
        for (name, def) in table_indexes(con, t) {
            out.push_str(&format!("  index {name} {def}\n"));
        }
    }
    for r in &snapshot(
        con,
        "select viewname, definition from pg_views where schemaname = 'public' order by viewname",
        &[],
    )
    .rows
    {
        out.push_str(&format!("view {} {}\n", r[0], r[1].trim()));
    }
    for r in &snapshot(
        con,
        "select matviewname, definition from pg_matviews where schemaname = 'public' order by matviewname",
        &[],
    )
    .rows
    {
        out.push_str(&format!("matview {} {}\n", r[0], r[1].trim()));
    }
    for r in &snapshot(
        con,
        "select sequencename, data_type::text, start_value::text, increment_by::text \
         from pg_sequences where schemaname = 'public' order by sequencename",
        &[],
    )
    .rows
    {
        out.push_str(&format!(
            "sequence {} {} start {} by {}\n",
            r[0], r[1], r[2], r[3]
        ));
    }
    for r in &snapshot(
        con,
        "select p.proname, pg_get_function_identity_arguments(p.oid), pg_get_functiondef(p.oid) \
         from pg_proc p join pg_namespace n on n.oid = p.pronamespace \
         where n.nspname = 'public' and p.prokind in ('f', 'p') order by 1, 2",
        &[],
    )
    .rows
    {
        out.push_str(&format!("function {}({}) {}\n", r[0], r[1], r[2].trim()));
    }
    for r in &snapshot(
        con,
        "select extname, extversion from pg_extension order by extname",
        &[],
    )
    .rows
    {
        out.push_str(&format!("extension {} {}\n", r[0], r[1]));
    }
    out
}

/// Every row of `table` ordered by all of its columns (Go `%v` rendering).
pub fn table_data(con: &PgConn, table: &str) -> Snapshot {
    let n = table_columns(con, table).len();
    assert!(n > 0, "table {table} has no columns (does it exist?)");
    let order: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
    snapshot(
        con,
        &format!(
            "select * from \"{}\" order by {}",
            table.replace('"', "\"\""),
            order.join(", ")
        ),
        &[],
    )
}

/// Row counts of every user table, sorted by table name.
pub fn table_counts(con: &PgConn) -> Vec<(String, i64)> {
    tables(con)
        .into_iter()
        .map(|t| {
            let n = snapshot(
                con,
                &format!("select count(*) from \"{}\"", t.replace('"', "\"\"")),
                &[],
            )
            .rows[0][0]
                .parse::<i64>()
                .unwrap();
            (t, n)
        })
        .collect()
}

/// `pg_dump --schema-only` of `ctx.pg_db` with comments, blank lines and
/// `SET`/`SELECT pg_catalog.set_config` noise removed — a second, independent
/// opinion on the schema. `None` when `pg_dump` is not installed.
pub fn pg_dump_schema(ctx: &Ctx) -> Option<String> {
    let out = Command::new("pg_dump")
        .args(psql_args(ctx))
        .args(["--schema-only", "--no-owner", "--no-privileges"])
        .env("PGPASSWORD", &ctx.pg_pass)
        .output()
        .ok()?;
    if !out.status.success() {
        panic!(
            "pg_dump failed ({:?}): {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Some(
        text.lines()
            .filter(|l| {
                let l = l.trim();
                !(l.is_empty()
                    || l.starts_with("--")
                    || l.starts_with("\\")
                    || l.starts_with("SET ")
                    || l.starts_with("SELECT pg_catalog.set_config"))
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}
