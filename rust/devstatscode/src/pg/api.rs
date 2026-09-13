//! The DevStats database helper API — port of the functions of `pg_conn.go`
//! (`QuerySQL*`, `ExecSQL*`, `WriteTSPoints`, `DatabaseExists`, ...) and of
//! the PostgreSQL branches of `FatalOnError` (`error.go`).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::{go_quote, ExecResult, PgConn, PgError, PgTx, Row, Rows, ScanDest, SqlArg};
use crate::consts::{OK, RECONNECT, RETRY};
use crate::context::Ctx;
use crate::error::{fatal_on_error, now_string};
use crate::gofmt;
use crate::printf;
use crate::ts_points::{FieldValue, TSPoint};
use crate::unicode::strip_unicode;

// ---------------------------------------------------------------------------
// FatalOnError (PostgreSQL aware part of error.go)
// ---------------------------------------------------------------------------

fn durable_pq() -> bool {
    match std::env::var("DURABLE_PQ") {
        Ok(v) => !(v.is_empty() || v == "0" || v == "false"),
        Err(_) => false,
    }
}

/// Seconds the "DB shutting down" / "cannot assign requested address"
/// branches wait before asking for a reconnect (15 minutes, like Go).
/// `DEVSTATS_PG_SETTLE_SECONDS` overrides it (used by the tests).
fn settle_seconds() -> u64 {
    std::env::var("DEVSTATS_PG_SETTLE_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(900)
}

/// Go `FatalOnError(err)` for database errors: returns [`RETRY`] or
/// [`RECONNECT`] for the retryable conditions (after printing the Go
/// warnings) and terminates the process (via
/// [`fatal_on_error`]) for everything else.
pub fn fatal_on_pg_error(err: &PgError) -> &'static str {
    let tm = now_string();
    match err {
        PgError::Server(e) => {
            let name = e.name();
            if name == "too_many_connections" {
                eprintln!(
                    "PqError: code={}, name={}, detail={}",
                    e.code, name, e.detail
                );
                eprintln!("Warning: too many postgres connections: {}: '{}'", tm, err);
                return RETRY;
            } else if name == "cannot_connect_now" {
                eprintln!(
                    "PqError: code={}, name={}, detail={}",
                    e.code, name, e.detail
                );
                eprintln!(
                    "Warning: DB shutting down: {}: '{}', sleeping 15 minutes to settle",
                    tm, err
                );
                thread::sleep(Duration::from_secs(settle_seconds()));
                eprintln!(
                    "Warning: DB shutting down: {}: '{}', waited 15 minutes, retrying",
                    now_string(),
                    err
                );
                return RECONNECT;
            }
            printf!(
                "PqError: code={}, name={}, detail={}\n",
                e.code,
                name,
                e.detail
            );
            eprintln!(
                "PqError: code={}, name={}, detail={}",
                e.code, name, e.detail
            );
            if durable_pq() {
                match name {
                    "program_limit_exceeded"
                    | "undefined_column"
                    | "invalid_catalog_name"
                    | "character_not_in_repertoire" => {
                        printf!("{} error is not retryable, even with DURABLE_PQ\n", name);
                    }
                    _ => {
                        eprintln!("retrying with DURABLE_PQ");
                        return RECONNECT;
                    }
                }
            }
        }
        other => {
            eprintln!("ErrorType: {}, error: {}", other.go_type_name(), other);
            eprintln!("ErrorType: {}, error: {}", other.go_type_name(), other);
        }
    }
    let msg = err.to_string();
    if msg.contains("driver: bad connection") {
        eprintln!("Warning: bad driver, retrying");
        return RECONNECT;
    }
    if msg.contains("cannot assign requested address") {
        eprintln!("Warning: cannot assign requested address, retrying in 15 minutes");
        thread::sleep(Duration::from_secs(settle_seconds()));
        eprintln!("Warning: cannot assign requested address - waited 15 minutes, retrying");
        return RECONNECT;
    }
    fatal_on_error(err)
}

/// `FatalOnError` on a result: the value on success, `T::default()` when the
/// error was one of the retryable conditions (Go callers ignore the returned
/// status and go on with zero values), process termination otherwise.
pub fn fatal_on_pg_err<T: Default>(res: Result<T, PgError>) -> T {
    match res {
        Ok(v) => v,
        Err(e) => {
            fatal_on_pg_error(&e);
            T::default()
        }
    }
}

fn sleep_seconds(secs: i64) {
    if secs > 0 {
        thread::sleep(Duration::from_secs(secs as u64));
    }
}

// ---------------------------------------------------------------------------
// Query output / SQL text helpers
// ---------------------------------------------------------------------------

/// Go `queryOut`: print the query and its numbered arguments (plain stdout —
/// never the DB logger, which would recurse).
pub fn query_out(query: &str, args: &[SqlArg]) {
    println!("{}", query);
    if !args.is_empty() {
        let mut s = String::new();
        for (i, a) in args.iter().enumerate() {
            s.push_str(&format!("{}:{} ", i + 1, a.go_arg_string()));
        }
        println!("[{}]", s);
    }
}

/// Go `CreateTable`: expand the DB specific placeholders of a table definition.
pub fn create_table(tdef: &str) -> String {
    let tdef = tdef
        .replace("{{ts}}", "timestamp")
        .replace("{{tsnow}}", "timestamp default now()")
        .replace("{{pkauto}}", "bigserial");
    format!("create table {}", tdef)
}

/// Go `NValues(n)`: `values($1, $2, .., $n)`.
pub fn n_values(n: usize) -> String {
    let parts: Vec<String> = (1..=n).map(|i| format!("${}", i)).collect();
    format!("values({})", parts.join(", "))
}

/// Go `NArray(n, offset)`: `($offset+1, .., $offset+n)`.
pub fn n_array(n: usize, offset: usize) -> String {
    let parts: Vec<String> = (1 + offset..=n + offset)
        .map(|i| format!("${}", i))
        .collect();
    format!("({})", parts.join(", "))
}

/// Go `NValue(i)`: `$i`.
pub fn n_value(index: usize) -> String {
    format!("${}", index)
}

/// Go `InsertIgnore`: `insert <query> on conflict do nothing`.
pub fn insert_ignore(query: &str) -> String {
    format!("insert {} on conflict do nothing", query)
}

// ---------------------------------------------------------------------------
// Query / Exec wrappers
// ---------------------------------------------------------------------------

/// Go `QueryRowSQL`.
pub fn query_row_sql<'a>(con: &'a PgConn, ctx: &Ctx, query: &str, args: &[SqlArg]) -> Row<'a> {
    if ctx.q_out {
        query_out(query, args);
    }
    con.query_row(query, args)
}

/// Go `QueryRowSQLTx`.
pub fn query_row_sql_tx<'a>(
    tx: &'a mut PgTx<'_>,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Row<'a> {
    if ctx.q_out {
        query_out(query, args);
    }
    tx.query_row(query, args)
}

/// Go `QuerySQL`.
pub fn query_sql<'a>(
    con: &'a PgConn,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Result<Rows<'a>, PgError> {
    if ctx.q_out {
        query_out(query, args);
    }
    con.query(query, args)
}

/// Go `QuerySQLLogErr`: like [`query_sql`] but prints the query on error.
pub fn query_sql_log_err<'a>(
    con: &'a PgConn,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Result<Rows<'a>, PgError> {
    if ctx.q_out {
        query_out(query, args);
    }
    let res = con.query(query, args);
    if res.is_err() {
        query_out(query, args);
    }
    res
}

/// Handle a failed attempt of the `*WithErr` helpers on a pool: report,
/// wait `try_` seconds, and reconnect when requested.
fn retry_wait_pool(con: &PgConn, ctx: &Ctx, err: &PgError, try_: i64) {
    let status = fatal_on_pg_error(err);
    eprintln!("Will retry after {} seconds...", try_);
    sleep_seconds(try_);
    eprintln!("{} seconds passed, retrying...", try_);
    if status == RECONNECT {
        if ctx.can_reconnect {
            eprintln!("Reconnect request after {} seconds", try_);
            con.reset();
            eprintln!("Reconnected after {} seconds", try_);
        } else {
            crate::fatalf(format_args!(
                "returned reconnect request, but custom DB connect strings are in use"
            ));
        }
    }
}

/// Same for the transaction helpers: a reconnect cannot happen inside a
/// transaction.
fn retry_wait_tx(ctx: &Ctx, err: &PgError, try_: i64) {
    let status = fatal_on_pg_error(err);
    eprintln!("Will retry after {} seconds...", try_);
    sleep_seconds(try_);
    eprintln!("{} seconds passed, retrying...", try_);
    if status == RECONNECT {
        eprintln!(
            "Reconnect request after {} seconds, breaking transaction",
            try_
        );
        if ctx.can_reconnect {
            crate::fatalf(format_args!(
                "reconnect request from within the transaction is not supported"
            ));
        } else {
            crate::fatalf(format_args!(
                "returned reconnect request, but custom DB connect strings are in use"
            ));
        }
    }
}

fn too_many_attempts(ctx: &Ctx) -> ! {
    crate::fatalf(format_args!(
        "too many attempts, tried {} times",
        ctx.trials.len()
    ))
}

/// Go `QuerySQLWithErr`: [`query_sql`] retried over `ctx.trials` for the
/// retryable errors; any other error terminates the process.
pub fn query_sql_with_err<'a>(
    con: &'a PgConn,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Rows<'a> {
    for try_ in ctx.trials.iter().copied() {
        match query_sql(con, ctx, query, args) {
            Ok(rows) => return rows,
            Err(err) => {
                query_out(query, args);
                retry_wait_pool(con, ctx, &err, try_);
            }
        }
    }
    too_many_attempts(ctx)
}

/// Go `QuerySQLTx`.
pub fn query_sql_tx<'a>(
    tx: &'a mut PgTx<'_>,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Result<Rows<'a>, PgError> {
    if ctx.q_out {
        query_out(query, args);
    }
    tx.query(query, args)
}

/// Go `QuerySQLTxWithErr`.
pub fn query_sql_tx_with_err<'a>(
    tx: &'a mut PgTx<'_>,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Rows<'a> {
    for try_ in ctx.trials.iter().copied() {
        if ctx.q_out {
            query_out(query, args);
        }
        // Start and hand out the rows in two steps so that a failed attempt
        // does not keep `tx` borrowed for the caller's lifetime.
        match tx.start_query(query, args) {
            Ok(start) => return tx.rows_after_start(start),
            Err(err) => {
                query_out(query, args);
                retry_wait_tx(ctx, &err, try_);
            }
        }
    }
    too_many_attempts(ctx)
}

/// Go `ExecSQLLogErr`.
pub fn exec_sql_log_err(
    con: &PgConn,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Result<ExecResult, PgError> {
    if ctx.q_out {
        query_out(query, args);
    }
    let res = con.exec(query, args);
    if res.is_err() {
        query_out(query, args);
    }
    res
}

/// Go `ExecSQL`.
pub fn exec_sql(
    con: &PgConn,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Result<ExecResult, PgError> {
    if ctx.q_out {
        query_out(query, args);
    }
    con.exec(query, args)
}

/// Go `ExecSQLWithErr`: [`exec_sql`] retried over `ctx.trials`.
pub fn exec_sql_with_err(con: &PgConn, ctx: &Ctx, query: &str, args: &[SqlArg]) -> ExecResult {
    for try_ in ctx.trials.iter().copied() {
        match exec_sql(con, ctx, query, args) {
            Ok(res) => return res,
            Err(err) => {
                print!("Failed sql: ");
                query_out(query, args);
                let status = fatal_on_pg_error(&err);
                eprintln!("Will retry after {} seconds...", try_);
                sleep_seconds(try_);
                eprintln!("{} seconds passed, retrying...", try_);
                if status == RECONNECT {
                    eprintln!("Reconnect request after {} seconds", try_);
                    if ctx.can_reconnect {
                        con.reset();
                        eprintln!("Reconnected after {} seconds", try_);
                    } else {
                        crate::fatalf(format_args!(
                            "returned reconnect request, but custom DB connect strings are in use"
                        ));
                    }
                }
            }
        }
    }
    too_many_attempts(ctx)
}

/// Go `ExecSQLTx`.
pub fn exec_sql_tx(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> Result<ExecResult, PgError> {
    if ctx.q_out {
        query_out(query, args);
    }
    tx.exec(query, args)
}

/// Go `ExecSQLTxWithErr`.
pub fn exec_sql_tx_with_err(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> ExecResult {
    for try_ in ctx.trials.iter().copied() {
        match exec_sql_tx(tx, ctx, query, args) {
            Ok(res) => return res,
            Err(err) => {
                query_out(query, args);
                retry_wait_tx(ctx, &err, try_);
            }
        }
    }
    too_many_attempts(ctx)
}

/// Go `execSQLAutocommitTransactionRetry`: a single autocommit statement
/// retried on serialization failures / deadlocks (SQLSTATE 40001 / 40P01).
fn exec_sql_autocommit_transaction_retry(
    con: &PgConn,
    ctx: &Ctx,
    query: &str,
    args: &[SqlArg],
) -> ExecResult {
    let mut attempt = 0usize;
    let last_err: PgError = loop {
        let err = match exec_sql(con, ctx, query, args) {
            Ok(res) => return res,
            Err(err) => err,
        };
        let retry_transaction = err
            .server()
            .is_some_and(|e| e.code == "40001" || e.code == "40P01");
        if retry_transaction {
            let e = err.server().expect("server error");
            eprintln!(
                "PqError: code={}, name={}, detail={}; retrying complete autocommit transaction",
                e.code,
                e.name(),
                e.detail
            );
        } else {
            query_out(query, args);
            let status = fatal_on_pg_error(&err);
            if status != RETRY && status != RECONNECT {
                crate::fatalf(format_args!(
                    "unexpected shared actor insert status {}: {}",
                    go_quote(status),
                    err
                ));
            }
        }
        if attempt >= ctx.trials.len() {
            break err;
        }
        let delay = ctx.trials[attempt];
        eprintln!("Will retry shared actor insert after {} seconds...", delay);
        sleep_seconds(delay);
        attempt += 1;
    };
    print!("Failed sql: ");
    query_out(query, args);
    crate::fatalf(format_args!(
        "shared actor insert failed after {} attempts: {}",
        ctx.trials.len() + 1,
        last_err
    ))
}

// ---------------------------------------------------------------------------
// Shared affiliations database
// ---------------------------------------------------------------------------

struct SharedAffiliations {
    con: PgConn,
    ctx: Ctx,
}

fn shared_affiliations_slot() -> &'static Mutex<Option<Arc<SharedAffiliations>>> {
    static SLOT: OnceLock<Mutex<Option<Arc<SharedAffiliations>>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

/// Go `SetSharedAffiliationsDB`: set the process-wide connection used for
/// shared actor inserts.
pub fn set_shared_affiliations_db(con: PgConn, ctx: Ctx) {
    let mut slot = shared_affiliations_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *slot = Some(Arc::new(SharedAffiliations { con, ctx }));
}

/// Go's `gSharedAffsDB`/`gSharedAffsCtx` read-back (`ghapi2db`): run `f`
/// with the shared affiliations connection when one was set by
/// [`set_shared_affiliations_db`], `None` otherwise.
pub fn with_shared_affiliations_db<R>(f: impl FnOnce(&PgConn, &Ctx) -> R) -> Option<R> {
    let shared = shared_affiliations_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    shared.map(|s| f(&s.con, &s.ctx))
}

fn get_shared_affiliations_db(ctx: &Ctx) -> Arc<SharedAffiliations> {
    let mut slot = shared_affiliations_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(s) = slot.as_ref() {
        return Arc::clone(s);
    }
    let mut actx = ctx.copy_context();
    let con = pg_conn_db_impl(&mut actx, &ctx.affiliations_db);
    let s = Arc::new(SharedAffiliations { con, ctx: actx });
    *slot = Some(Arc::clone(&s));
    s
}

fn pg_conn_db_impl(ctx: &mut Ctx, db_name: &str) -> PgConn {
    super::pg_conn_db(ctx, db_name)
}

/// Go `InsertActorTx`: insert an actor inside the transaction (legacy mode)
/// or directly into the shared affiliations database as a retried autocommit
/// statement (when `ctx.affiliations_db` is set).
pub fn insert_actor_tx(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    id: SqlArg,
    login: &str,
    name: &str,
) -> ExecResult {
    let query = insert_ignore(&format!("into gha_actors(id, login, name) {}", n_values(3)));
    let args = [id, SqlArg::from(login), SqlArg::from(name)];
    if ctx.affiliations_db.is_empty() {
        return exec_sql_tx_with_err(tx, ctx, &query, &args);
    }
    let shared = get_shared_affiliations_db(ctx);
    exec_sql_autocommit_transaction_retry(&shared.con, &shared.ctx, &query, &args)
}

// ---------------------------------------------------------------------------
// Nullable argument helpers
// ---------------------------------------------------------------------------

/// Go `BoolOrNil`.
pub fn bool_or_nil(v: Option<bool>) -> SqlArg {
    v.into()
}

/// Go `NegatedBoolOrNil`.
pub fn negated_bool_or_nil(v: Option<bool>) -> SqlArg {
    v.map(|b| !b).into()
}

/// Go `TimeOrNil`.
pub fn time_or_nil(v: Option<DateTime<Utc>>) -> SqlArg {
    v.into()
}

/// Go `IntOrNil`.
pub fn int_or_nil(v: Option<i64>) -> SqlArg {
    v.into()
}

/// Go `FirstIntOrNil`: the first non-nil value.
pub fn first_int_or_nil(vals: &[Option<i64>]) -> SqlArg {
    vals.iter().flatten().next().copied().into()
}

/// Go `CleanUTF8`: drop NUL characters (PostgreSQL rejects them in text).
pub fn clean_utf8(s: &str) -> String {
    if s.contains('\0') {
        s.replace('\0', "")
    } else {
        s.to_string()
    }
}

/// Go `StringOrNil`.
pub fn string_or_nil(v: Option<&str>) -> SqlArg {
    match v {
        None => SqlArg::Null,
        Some(s) => SqlArg::Str(clean_utf8(s)),
    }
}

/// Go `TruncToBytes`: truncate to at most `size` bytes on a character
/// boundary (after [`clean_utf8`]).
pub fn trunc_to_bytes(s: &str, size: usize) -> String {
    let s = clean_utf8(s);
    if s.len() < size {
        return s;
    }
    let mut res = String::new();
    for ch in s.chars() {
        if res.len() + ch.len_utf8() > size {
            break;
        }
        res.push(ch);
    }
    res
}

/// Go `TruncStringOrNil`.
pub fn trunc_string_or_nil(v: Option<&str>, max_len: usize) -> SqlArg {
    match v {
        None => SqlArg::Null,
        Some(s) => SqlArg::Str(trunc_to_bytes(s, max_len)),
    }
}

// ---------------------------------------------------------------------------
// Databases
// ---------------------------------------------------------------------------

/// Go `DatabaseExists`: does `ctx.pg_db` exist? Checked through the default
/// `postgres` database; the pool to it is returned unless `close_conn`.
pub fn database_exists(ctx: &mut Ctx, close_conn: bool) -> (bool, Option<PgConn>) {
    let db = std::mem::replace(&mut ctx.pg_db, "postgres".to_string());
    let c = super::pg_conn(ctx);
    let mut exists = false;
    {
        let mut rows = query_sql_with_err(
            &c,
            ctx,
            "select 1 from pg_database where datname = $1",
            &[SqlArg::from(db.as_str())],
        );
        while rows.next() {
            exists = true;
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
    }
    ctx.pg_db = db;
    if close_conn {
        c.close();
        (exists, None)
    } else {
        (exists, Some(c))
    }
}

/// Go `DropDatabaseIfExists`: returns whether the database existed (and was
/// dropped).
pub fn drop_database_if_exists(ctx: &mut Ctx) -> bool {
    let (exists, c) = database_exists(ctx, false);
    let c = c.expect("connection to the postgres database");
    if exists {
        exec_sql_with_err(&c, ctx, &format!("drop database {}", ctx.pg_db), &[]);
    }
    c.close();
    exists
}

/// Go `CreateDatabaseIfNeeded`: returns whether the database was created.
pub fn create_database_if_needed(ctx: &mut Ctx) -> bool {
    let (exists, c) = database_exists(ctx, false);
    let c = c.expect("connection to the postgres database");
    if !exists {
        exec_sql_with_err(&c, ctx, &format!("create database {}", ctx.pg_db), &[]);
    }
    c.close();
    !exists
}

/// Go `CreateDatabaseIfNeededExtended`: like [`create_database_if_needed`]
/// with extra `create database` parameters.
pub fn create_database_if_needed_extended(ctx: &mut Ctx, extra_params: &str) -> bool {
    let (exists, c) = database_exists(ctx, false);
    let c = c.expect("connection to the postgres database");
    if !exists {
        exec_sql_with_err(
            &c,
            ctx,
            &format!("create database {} {}", ctx.pg_db, extra_params),
            &[],
        );
    }
    c.close();
    !exists
}

/// Go `ClearOrphanedLocks`: remove stale `affs_lock`/`giant_lock` markers
/// (older than `GHA2DB_MAX_AFFS_LOCK_AGE` / `GHA2DB_MAX_GIANT_LOCK_AGE`) from
/// the project database and from `devstats`. Errors are ignored.
pub fn clear_orphaned_locks() {
    let mut ctx = Ctx::default();
    ctx.init();
    if ctx.skip_pdb {
        return;
    }
    let c0 = super::pg_conn(&ctx);
    let _ = exec_sql(
        &c0,
        &ctx,
        &format!(
            "delete from gha_computed where metric like 'affs_lock%' and dt < now() - '{}'::interval",
            ctx.clear_affs_lock_period
        ),
        &[],
    );
    c0.close();
    ctx.pg_db = crate::consts::DEVSTATS.to_string();
    let c = super::pg_conn(&ctx);
    let _ = exec_sql(
        &c,
        &ctx,
        &format!(
            "delete from gha_computed where metric like 'affs_lock%' and dt < now() - '{}'::interval",
            ctx.clear_affs_lock_period
        ),
        &[],
    );
    let _ = exec_sql(
        &c,
        &ctx,
        &format!(
            "delete from gha_computed where metric like 'giant_lock%' and dt < now() - '{}'::interval",
            ctx.clear_giant_lock_period
        ),
        &[],
    );
    c.close();
}

// ---------------------------------------------------------------------------
// Tables / columns
// ---------------------------------------------------------------------------

/// Go `TableExists`.
pub fn table_exists(con: &PgConn, ctx: &Ctx, table_name: &str) -> bool {
    let mut s: Option<String> = None;
    fatal_on_pg_err(
        query_row_sql(
            con,
            ctx,
            &format!("select to_regclass({})", n_value(1)),
            &[SqlArg::from(table_name)],
        )
        .scan(&mut [&mut s]),
    );
    s.is_some()
}

/// Go `TableColumnExists`.
pub fn table_column_exists(con: &PgConn, ctx: &Ctx, table_name: &str, column_name: &str) -> bool {
    let mut s: Option<String> = None;
    fatal_on_pg_err(
        query_row_sql(
            con,
            ctx,
            &format!(
                "select column_name from information_schema.columns where table_name={} and column_name={} union select null limit 1",
                n_value(1),
                n_value(2)
            ),
            &[SqlArg::from(table_name), SqlArg::from(column_name)],
        )
        .scan(&mut [&mut s]),
    );
    s.is_some()
}

/// Go `GetTagValues`: `select <key> from t<name> order by time asc`.
pub fn get_tag_values(con: &PgConn, ctx: &Ctx, name: &str, key: &str) -> Vec<String> {
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!("select {} from t{} order by time asc", key, name),
        &[],
    );
    let mut ret = Vec::new();
    let mut s = String::new();
    while rows.next() {
        fatal_on_pg_err(rows.scan(&mut [&mut s]));
        ret.push(s.clone());
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    ret
}

/// Go `GetCurrentTableColumns`: the columns of a public table (list and set).
pub fn get_current_table_columns(
    con: &PgConn,
    ctx: &Ctx,
    table: &str,
) -> Result<(Vec<String>, HashSet<String>), PgError> {
    let mut rows = match query_sql(
        con,
        ctx,
        &format!(
            "select column_name from information_schema.columns where table_schema = 'public' and table_name = {}",
            n_value(1)
        ),
        &[SqlArg::from(table)],
    ) {
        Ok(rows) => rows,
        Err(err) => {
            printf!("Error select column_name {}: {}\n", table, err);
            return Err(err);
        }
    };
    let mut col_names = Vec::new();
    let mut col_names_set = HashSet::new();
    let mut col_name = String::new();
    while rows.next() {
        if let Err(er) = rows.scan(&mut [&mut col_name]) {
            printf!("Error scan column name {}: {}\n", table, er);
            // Go returns the (nil) query error here, not the scan error.
            return Ok((Vec::new(), HashSet::new()));
        }
        col_names.push(col_name.clone());
        col_names_set.insert(col_name.clone());
    }
    if let Err(err) = rows.err() {
        printf!("Error rows error {}: {}\n", table, err);
        return Err(err);
    }
    if let Err(err) = rows.close() {
        printf!("Error rows close {}: {}\n", table, err);
    }
    Ok((col_names, col_names_set))
}

/// Go `IdentifyColumnsToDelete`: columns present but not needed, except the
/// structural ones (`time`, `series`, `period`) and `all`/`none`.
pub fn identify_columns_to_delete(curr_cols: &[String], needed_cols: &[String]) -> Vec<String> {
    let needed: HashSet<&str> = needed_cols.iter().map(String::as_str).collect();
    let mut to_drop = Vec::new();
    for col in curr_cols {
        let lcol = col.to_lowercase();
        if col == "time" || col == "series" || col == "period" || lcol == "all" || lcol == "none" {
            continue;
        }
        if !needed.contains(col.as_str()) {
            to_drop.push(col.clone());
        }
    }
    to_drop
}

fn go_float_slice(vals: &[f64]) -> String {
    let strs: Vec<String> = vals.iter().map(|f| gofmt::float(*f)).collect();
    gofmt::slice(&strs)
}

/// Go `DropLeastUsedCol`: when `table` has at least 80 non-protected columns
/// drop the two with the lowest average value. Returns whether columns were
/// dropped (so the failed operation should be retried).
pub fn drop_least_used_col(
    con: &PgConn,
    ctx: &Ctx,
    table: &str,
    info: &str,
    protected_cols: &HashSet<String>,
) -> bool {
    let mut col_names: Vec<String> = Vec::new();
    {
        let mut rows = match query_sql(
            con,
            ctx,
            &format!(
                "select column_name from information_schema.columns where table_schema = 'public' and table_name = {}",
                n_value(1)
            ),
            &[SqlArg::from(table)],
        ) {
            Ok(rows) => rows,
            Err(err) => {
                printf!("Error select column_name (ignored) {}: {}\n", info, err);
                return false;
            }
        };
        let mut col_name = String::new();
        while rows.next() {
            if let Err(er) = rows.scan(&mut [&mut col_name]) {
                printf!("Error scan column name (ignored) {}: {}\n", info, er);
                return false;
            }
            if !protected_cols.contains(&col_name) {
                col_names.push(col_name.clone());
            }
        }
        if let Err(err) = rows.err() {
            printf!("Error rows error (ignored) {}: {}\n", info, err);
            return false;
        }
        if let Err(err) = rows.close() {
            printf!("Error rows close (ignored) {}: {}\n", info, err);
        }
    }
    col_names.sort();
    if ctx.debug > 0 {
        printf!(
            "Table '{}' has {} columns: {}\n",
            table,
            col_names.len(),
            gofmt::slice(&col_names)
        );
    } else {
        printf!("Table '{}' has {} column\n", table, col_names.len());
    }
    if col_names.len() < 80 {
        return false;
    }
    let mut query = String::from("select ");
    for col in &col_names {
        query.push_str(&format!("coalesce(avg(\"{}\"), 0.0), ", col));
    }
    query.truncate(query.len() - 2);
    query.push_str(&format!(" from \"{}\"", table));
    let mut col_avgs = vec![0f64; col_names.len()];
    {
        let mut rows = match query_sql(con, ctx, &query, &[]) {
            Ok(rows) => rows,
            Err(err) => {
                printf!("Error avg (ignored) {}: {}\n", info, err);
                return false;
            }
        };
        while rows.next() {
            let mut dests: Vec<&mut dyn ScanDest> = col_avgs
                .iter_mut()
                .map(|a| a as &mut dyn ScanDest)
                .collect();
            if let Err(er) = rows.scan(&mut dests) {
                printf!("Error avg scan (ignored) {}: {}\n", info, er);
                return false;
            }
        }
        if let Err(err) = rows.err() {
            printf!("Error avg rows (ignored) {}: {}\n", info, err);
            return false;
        }
        if let Err(err) = rows.close() {
            printf!("Error avg rows close (ignored) {}: {}\n", info, err);
        }
    }
    if ctx.debug > 0 {
        printf!(
            "Table '{}' columns averages: {}\n",
            table,
            go_float_slice(&col_avgs)
        );
    }
    if col_avgs.len() < 80 {
        return false;
    }
    let (mut min1, mut min2) = if col_avgs[1] < col_avgs[0] {
        (1, 0)
    } else {
        (0, 1)
    };
    for i in 2..col_avgs.len() {
        if col_avgs[i] < col_avgs[min1] {
            min2 = min1;
            min1 = i;
        } else if col_avgs[i] < col_avgs[min2] {
            min2 = i;
        }
    }
    printf!(
        "Two least used columns are: '{}' and '{}' with averages: {:.6}, {:.6}, indices: {}, {}\n",
        col_names[min1],
        col_names[min2],
        col_avgs[min1],
        col_avgs[min2],
        min1,
        min2
    );
    if let Err(err) = exec_sql(
        con,
        ctx,
        &format!(
            "alter table \"{}\" drop column if exists \"{}\"",
            table, col_names[min1]
        ),
        &[],
    ) {
        printf!("Error drop columns 1 (ignored) {}: {}\n", info, err);
        return false;
    }
    if let Err(err) = exec_sql(
        con,
        ctx,
        &format!(
            "alter table \"{}\" drop column if exists \"{}\"",
            table, col_names[min2]
        ),
        &[],
    ) {
        printf!("Error drop column 2 (ignored) {}: {}\n", info, err);
        return false;
    }
    printf!(
        "Dropped '{}' and '{}' from '{}' table\n",
        col_names[min1],
        col_names[min2],
        table
    );
    true
}

/// Columns already added per table by the `columns` program
/// (`map[table][column]`), shared between its threads.
pub type AddedColumns = Mutex<HashMap<String, HashSet<String>>>;

/// Go `HandleRowIsTooBig`: when `err` is `pq: row is too big` (SQLSTATE
/// class `program_limit_exceeded`) drop the two least used columns of
/// `table` (never `time`/`series`/`period` nor the columns in `added_cols`)
/// and return `true` so the caller retries; otherwise report the error
/// (unless it is an "already exists" one) and return `false`.
pub fn handle_row_is_too_big(
    con: &PgConn,
    ctx: &Ctx,
    table: &str,
    info: &str,
    added_cols: Option<&AddedColumns>,
    err: Option<&PgError>,
) -> bool {
    let Some(err) = err else {
        return false;
    };
    if let PgError::Server(e) = err {
        if e.name() == "program_limit_exceeded" && err.to_string().contains("pq: row is too big") {
            let mut cols_set: HashSet<String> = ["time", "series", "period"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            if let Some(added) = added_cols {
                let added = added.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(cols) = added.get(table) {
                    cols_set.extend(cols.iter().cloned());
                }
            }
            return drop_least_used_col(con, ctx, table, info, &cols_set);
        }
    }
    if !err.to_string().contains("already exists") {
        printf!("Error handle row is too big {}: {}\n", info, err);
    }
    false
}

// ---------------------------------------------------------------------------
// Time series points
// ---------------------------------------------------------------------------

/// Go `makePsqlName`: escape `"` and make sure the identifier fits in 63
/// bytes. Too long names are fatal when `fatal` (tables, columns) and
/// shortened otherwise (index names).
pub fn make_psql_name(name: &str, fatal: bool) -> String {
    let name = name.replace('"', "\"\"");
    let l = name.len();
    if l > 63 {
        if fatal {
            crate::fatalf(format_args!(
                "postgresql identifier name too long ({}, {})",
                l, name
            ));
        }
        printf!(
            "Notice: makePsqlName: postgresql identifier name too long ({}, {})\n",
            l,
            name
        );
        // Byte slicing like Go; the result is stripped of non-ASCII anyway.
        let bytes = name.as_bytes();
        let head = String::from_utf8_lossy(&bytes[..32]);
        let tail = String::from_utf8_lossy(&bytes[l - 31..]);
        return strip_unicode(&format!("{}{}", head, tail));
    }
    name
}

/// Go `escapeName`: `"` → `""`.
pub fn escape_name(name: &str) -> String {
    name.replace('"', "\"\"")
}

/// Go `checkPsqlName`: `true` when the identifier fits in 63 bytes, otherwise
/// a notice is printed and `false` returned.
pub fn check_psql_name(name: &str) -> bool {
    let name = name.replace('"', "\"\"");
    let l = name.len();
    if l > 63 {
        printf!(
            "Notice: checkPsqlName: postgresql identifier name too long ({}, {})\n",
            l,
            name
        );
        return false;
    }
    true
}

/// Go `WriteTSPoints`: write points in batches of up to 1000.
pub fn write_ts_points(
    ctx: &Ctx,
    con: &PgConn,
    pts: &[TSPoint],
    merge_series: &str,
    hll_empty: &[u8],
    mutex: Option<&Mutex<()>>,
) {
    let npts = pts.len();
    if npts == 0 {
        return;
    }
    printf!(
        "WriteTSPoints: writing {} points in batches of up to 1000\n",
        npts
    );
    for batch in pts.chunks(1000) {
        write_ts_points_batch(ctx, con, batch, merge_series, hll_empty, mutex);
    }
}

fn column_definition(col: &str, ty: i32) -> String {
    match ty {
        0 => format!("\"{}\" double precision not null default 0.0", col),
        1 => format!(
            "\"{}\" timestamp not null default '1900-01-01 00:00:00'",
            col
        ),
        3 => format!("\"{}\" hll not null default hll_empty()", col),
        _ => format!("\"{}\" text not null default ''", col),
    }
}

fn field_arg(value: &FieldValue, hll_empty: &[u8]) -> SqlArg {
    match value {
        FieldValue::Float(f) => SqlArg::Float(*f),
        FieldValue::Time(t) => SqlArg::from(*t),
        FieldValue::Str(s) => SqlArg::Str(s.clone()),
        FieldValue::Hll(b) => {
            if b.is_empty() {
                SqlArg::Bytes(hll_empty.to_vec())
            } else {
                SqlArg::Bytes(b.clone())
            }
        }
    }
}

/// Build the upsert for one point: `insert into "<table>"(<fixed>, cols)
/// values(...) on conflict(<conflict>) do update set ... where ...`.
fn upsert_query(
    table: &str,
    fixed_names: &[&str],
    fixed_args: Vec<SqlArg>,
    cols: &[(String, SqlArg)],
    conflict: &str,
) -> (String, Vec<SqlArg>) {
    let mut names_i: Vec<String> = fixed_names.iter().map(|s| s.to_string()).collect();
    let mut args_i: Vec<String> = (1..=fixed_names.len()).map(|i| format!("${}", i)).collect();
    let mut vals: Vec<SqlArg> = fixed_args.clone();
    let mut i = fixed_names.len() + 1;
    for (name, _) in cols {
        names_i.push(format!("\"{}\"", name));
        args_i.push(format!("${}", i));
        i += 1;
    }
    vals.extend(cols.iter().map(|(_, v)| v.clone()));
    let mut names_u: Vec<String> = Vec::new();
    let mut args_u: Vec<String> = Vec::new();
    for (name, value) in cols {
        names_u.push(format!("\"{}\"", name));
        args_u.push(format!("${}", i));
        vals.push(value.clone());
        i += 1;
    }
    let mut names_ua = names_u.join(", ");
    let mut args_ua = args_u.join(", ");
    if names_u.len() > 1 {
        names_ua = format!("({})", names_ua);
        args_ua = format!("({})", args_ua);
    }
    let names_ia = names_i.join(", ");
    let args_ia = args_i.join(", ");
    if names_u.is_empty() {
        let q = format!(
            "insert into \"{}\"({}) values({}) on conflict({}) do nothing",
            table, names_ia, args_ia, conflict
        );
        return (q, vals);
    }
    let mut wheres: Vec<String> = Vec::new();
    for (k, name) in fixed_names.iter().enumerate() {
        wheres.push(format!("\"{}\".{} = ${}", table, name, i + k));
    }
    vals.extend(fixed_args);
    let q = format!(
        "insert into \"{}\"({}) values({}) on conflict({}) do update set {} = {} where {}",
        table,
        names_ia,
        args_ia,
        conflict,
        names_ua,
        args_ua,
        wheres.join(" and ")
    );
    (q, vals)
}

/// Go `WriteTSPointsBatch`: create/extend the `t<series>` (tags) and
/// `s<series>` / `s<merge_series>` (fields) tables as needed and upsert the
/// points. Structural statement failures are reported and ignored (another
/// process may have created the object first); upsert failures are fatal
/// (after retries).
pub fn write_ts_points_batch(
    ctx: &Ctx,
    con: &PgConn,
    pts: &[TSPoint],
    merge_series: &str,
    hll_empty: &[u8],
    mutex: Option<&Mutex<()>>,
) {
    let npts = pts.len();
    if npts == 0 {
        return;
    }
    printf!("WriteTSPointsBatch: writing {} points\n", npts);
    if ctx.debug > 0 {
        printf!("Points:\n{}\n", crate::ts_points::ts_points_str(pts));
    }
    let merge = !merge_series.is_empty();
    let mut merge_s = String::new();
    if merge {
        if !check_psql_name(&format!("s{}", merge_series)) {
            printf!("WriteTSPointsBatch: writing {} points - finished\n", npts);
            return;
        }
        merge_s = format!("s{}", merge_series);
    }
    // table → tag names / field name → type id
    let mut tags: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut fields: BTreeMap<String, BTreeMap<String, i32>> = BTreeMap::new();
    for p in pts {
        if let Some(ptags) = &p.tags {
            let name = if merge {
                p.name.clone()
            } else {
                if !check_psql_name(&format!("t{}", p.name)) {
                    continue;
                }
                format!("t{}", p.name)
            };
            let entry = tags.entry(name).or_default();
            for tag_name in ptags.keys() {
                if !check_psql_name(tag_name) {
                    continue;
                }
                entry.insert(tag_name.clone());
            }
        }
        if let Some(pfields) = &p.fields {
            let name = if merge {
                p.name.clone()
            } else {
                if !check_psql_name(&format!("s{}", p.name)) {
                    continue;
                }
                format!("s{}", p.name)
            };
            let entry = fields.entry(name).or_default();
            for (field_name, field_value) in pfields {
                if !check_psql_name(field_name) {
                    continue;
                }
                let ty = field_value.type_id();
                if let Some(t) = entry.get(field_name) {
                    if *t != ty {
                        crate::fatalf(format_args!(
                            "Field {} has a value {},{}, previous values were different type {} != {}",
                            field_name,
                            field_value.go_string(),
                            field_value.go_type_name(),
                            ty,
                            t
                        ));
                    }
                }
                entry.insert(field_name.clone(), ty);
            }
        }
    }
    if ctx.debug > 0 {
        printf!("Merge: {},{}\n", merge, merge_series);
        let tags_str: BTreeMap<String, String> = tags
            .iter()
            .map(|(k, v)| {
                let inner: BTreeMap<&String, &str> = v.iter().map(|t| (t, "{}")).collect();
                (k.clone(), gofmt::map(&inner))
            })
            .collect();
        printf!("{} tags:\n{}\n", tags.len(), gofmt::map(&tags_str));
        let fields_str: BTreeMap<String, String> = fields
            .iter()
            .map(|(k, v)| (k.clone(), gofmt::map(v)))
            .collect();
        printf!("{} fields:\n{}\n", fields.len(), gofmt::map(&fields_str));
    }
    let mut sqls: Vec<String> = Vec::new();
    // Only used when multiple threads are writing the same series.
    let guard = mutex.map(|m| m.lock().unwrap_or_else(|e| e.into_inner()));
    for (name, data) in &tags {
        if data.is_empty() {
            continue;
        }
        if !table_exists(con, ctx, name) {
            let mut sq = format!(
                "create table if not exists \"{}\"(time timestamp primary key, ",
                name
            );
            let mut indices: Vec<String> = Vec::new();
            for col in data {
                sq.push_str(&format!("\"{}\" text, ", escape_name(col)));
                let iname = make_psql_name(&format!("i{}{}", &name[1..], col), false);
                indices.push(format!(
                    "create index if not exists \"{}\" on \"{}\"(\"{}\")",
                    iname,
                    name,
                    escape_name(col)
                ));
            }
            sq.truncate(sq.len() - 2);
            sq.push(')');
            sqls.push(sq);
            sqls.extend(indices);
            sqls.push(format!("grant select on \"{}\" to ro_user", name));
            sqls.push(format!("grant select on \"{}\" to devstats_team", name));
        } else {
            for col in data {
                let ecol = escape_name(col);
                if !table_column_exists(con, ctx, name, &ecol) {
                    sqls.push(format!(
                        "alter table \"{}\" add column if not exists \"{}\" text",
                        name, ecol
                    ));
                    let iname = make_psql_name(&format!("i{}{}", &name[1..], col), false);
                    sqls.push(format!(
                        "create index if not exists \"{}\" on \"{}\"(\"{}\")",
                        iname, name, ecol
                    ));
                }
            }
        }
    }
    if merge {
        let mut b_table = false;
        let mut col_set: HashSet<String> = HashSet::new();
        for data in fields.values() {
            if data.is_empty() {
                continue;
            }
            if !b_table {
                if !table_exists(con, ctx, &merge_s) {
                    let mut sq = format!(
                        "create table if not exists \"{}\"(time timestamp not null, series text not null, period text not null default '', ",
                        merge_s
                    );
                    let indices = vec![
                        format!(
                            "create index if not exists \"{}\" on \"{}\"(time)",
                            make_psql_name(&format!("i{}t", &merge_s[1..]), false),
                            merge_s
                        ),
                        format!(
                            "create index if not exists \"{}\" on \"{}\"(series)",
                            make_psql_name(&format!("i{}s", &merge_s[1..]), false),
                            merge_s
                        ),
                        format!(
                            "create index if not exists \"{}\" on \"{}\"(period)",
                            make_psql_name(&format!("i{}p", &merge_s[1..]), false),
                            merge_s
                        ),
                    ];
                    for (col, ty) in data {
                        let col = escape_name(col);
                        sq.push_str(&column_definition(&col, *ty));
                        sq.push_str(", ");
                        col_set.insert(col);
                    }
                    sq.push_str("primary key(time, series, period))");
                    sqls.push(sq);
                    sqls.extend(indices);
                    sqls.push(format!("grant select on \"{}\" to ro_user", merge_s));
                    sqls.push(format!("grant select on \"{}\" to devstats_team", merge_s));
                }
                b_table = true;
            }
            for (col, ty) in data {
                let col = escape_name(col);
                if !col_set.contains(&col) {
                    let col_exists = table_column_exists(con, ctx, &merge_s, &col);
                    col_set.insert(col.clone());
                    if !col_exists {
                        sqls.push(format!(
                            "alter table \"{}\" add column if not exists {}",
                            merge_s,
                            column_definition(&col, *ty)
                        ));
                    }
                }
            }
        }
    } else {
        for (name, data) in &fields {
            if data.is_empty() {
                continue;
            }
            if !table_exists(con, ctx, name) {
                let mut sq = format!(
                    "create table if not exists \"{}\"(time timestamp not null, period text not null default '', ",
                    name
                );
                let indices = vec![
                    format!(
                        "create index if not exists \"{}\" on \"{}\"(time)",
                        make_psql_name(&format!("i{}t", &name[1..]), false),
                        name
                    ),
                    format!(
                        "create index if not exists \"{}\" on \"{}\"(period)",
                        make_psql_name(&format!("i{}p", &name[1..]), false),
                        name
                    ),
                ];
                for (col, ty) in data {
                    sq.push_str(&column_definition(&escape_name(col), *ty));
                    sq.push_str(", ");
                }
                sq.push_str("primary key(time, period))");
                sqls.push(sq);
                sqls.extend(indices);
                sqls.push(format!("grant select on \"{}\" to ro_user", name));
                sqls.push(format!("grant select on \"{}\" to devstats_team", name));
            } else {
                for (col, ty) in data {
                    let col = escape_name(col);
                    if !table_column_exists(con, ctx, name, &col) {
                        sqls.push(format!(
                            "alter table \"{}\" add column if not exists {}",
                            name,
                            column_definition(&col, *ty)
                        ));
                    }
                }
            }
        }
    }
    if ctx.debug > 0 && !sqls.is_empty() {
        printf!("structural sqls:\n{}\n", sqls.join("\n"));
    }
    for q in &sqls {
        // These may fail when several processes create the same structures
        // concurrently — every such failure means somebody else already did
        // it, so they are only reported.
        if let Err(err) = exec_sql(con, ctx, q, &[]) {
            printf!("Ignored {}: {}\n", q, err);
        }
    }
    drop(guard);
    let mut ns = 0;
    for p in pts {
        if let Some(ptags) = &p.tags {
            if !check_psql_name(&format!("t{}", p.name)) {
                continue;
            }
            let name = format!("t{}", p.name);
            let cols: Vec<(String, SqlArg)> = ptags
                .iter()
                .filter(|(tag_name, _)| check_psql_name(tag_name))
                .map(|(tag_name, tag_value)| (escape_name(tag_name), SqlArg::from(tag_value)))
                .collect();
            if cols.is_empty() {
                if ctx.debug >= 0 {
                    printf!("tag {} has no values, skipping\n", name);
                }
                continue;
            }
            let (q, vals) = upsert_query(&name, &["time"], vec![SqlArg::from(p.t)], &cols, "time");
            exec_sql_with_err(con, ctx, &q, &vals);
            ns += 1;
        }
        if let Some(pfields) = &p.fields {
            if !merge {
                if !check_psql_name(&format!("s{}", p.name)) {
                    continue;
                }
                let name = format!("s{}", p.name);
                let cols: Vec<(String, SqlArg)> = pfields
                    .iter()
                    .filter(|(field_name, _)| check_psql_name(field_name))
                    .map(|(field_name, value)| {
                        (escape_name(field_name), field_arg(value, hll_empty))
                    })
                    .collect();
                if cols.is_empty() {
                    if ctx.debug >= 0 {
                        printf!(
                            "field {} has no values other than time and period, skipping\n",
                            name
                        );
                    }
                    continue;
                }
                let (q, vals) = upsert_query(
                    &name,
                    &["time", "period"],
                    vec![SqlArg::from(p.t), SqlArg::from(&p.period)],
                    &cols,
                    "time, period",
                );
                exec_sql_with_err(con, ctx, &q, &vals);
                ns += 1;
            } else {
                let cols: Vec<(String, SqlArg)> = pfields
                    .iter()
                    .filter(|(field_name, _)| check_psql_name(field_name))
                    .map(|(field_name, value)| {
                        (escape_name(field_name), field_arg(value, hll_empty))
                    })
                    .collect();
                if cols.is_empty() {
                    if ctx.debug >= 0 {
                        printf!(
                            "field {} has no values other than time, period and series, skipping\n",
                            merge_s
                        );
                    }
                    continue;
                }
                let (q, vals) = upsert_query(
                    &merge_s,
                    &["time", "period", "series"],
                    vec![
                        SqlArg::from(p.t),
                        SqlArg::from(&p.period),
                        SqlArg::from(&p.name),
                    ],
                    &cols,
                    "time, series, period",
                );
                exec_sql_with_err(con, ctx, &q, &vals);
                ns += 1;
            }
        }
    }
    if ctx.debug > 0 {
        printf!("upserts: {}\n", ns);
    }
    printf!("WriteTSPointsBatch: writing {} points - finished\n", npts);
}

#[allow(dead_code)]
fn _status_is_ok(status: &str) -> bool {
    status == OK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_text_helpers() {
        assert_eq!(n_values(1), "values($1)");
        assert_eq!(n_values(3), "values($1, $2, $3)");
        assert_eq!(n_array(2, 0), "($1, $2)");
        assert_eq!(n_array(2, 3), "($4, $5)");
        assert_eq!(n_value(7), "$7");
        assert_eq!(
            insert_ignore("into t(a) values($1)"),
            "insert into t(a) values($1) on conflict do nothing"
        );
        assert_eq!(
            create_table("t(id {{pkauto}}, dt {{ts}}, created {{tsnow}})"),
            "create table t(id bigserial, dt timestamp, created timestamp default now())"
        );
    }

    #[test]
    fn nullable_helpers() {
        assert_eq!(bool_or_nil(None), SqlArg::Null);
        assert_eq!(bool_or_nil(Some(true)), SqlArg::Bool(true));
        assert_eq!(negated_bool_or_nil(Some(true)), SqlArg::Bool(false));
        assert_eq!(negated_bool_or_nil(None), SqlArg::Null);
        assert_eq!(int_or_nil(Some(5)), SqlArg::Int(5));
        assert_eq!(int_or_nil(None), SqlArg::Null);
        assert_eq!(first_int_or_nil(&[None, Some(2), Some(3)]), SqlArg::Int(2));
        assert_eq!(first_int_or_nil(&[None, None]), SqlArg::Null);
        assert_eq!(time_or_nil(None), SqlArg::Null);
        assert_eq!(string_or_nil(None), SqlArg::Null);
        assert_eq!(string_or_nil(Some("a\0b")), SqlArg::Str("ab".into()));
        assert_eq!(clean_utf8("plain"), "plain");
        assert_eq!(clean_utf8("a\0\0b"), "ab");
    }

    #[test]
    fn trunc_to_bytes_respects_character_boundaries() {
        assert_eq!(trunc_to_bytes("abcdef", 10), "abcdef");
        assert_eq!(trunc_to_bytes("abcdef", 6), "abcdef");
        assert_eq!(trunc_to_bytes("abcdef", 3), "abc");
        // 'ł' is 2 bytes: "zażółć" = z(1) a(1) ż(2) ó(2) ł(2) ć(2) = 10 bytes
        assert_eq!(trunc_to_bytes("zażółć", 5), "zaż");
        assert_eq!(trunc_to_bytes("zażółć", 6), "zażó");
        assert_eq!(trunc_to_bytes("zażółć", 7), "zażó");
        assert_eq!(trunc_to_bytes("a\0bcd", 3), "abc");
        assert_eq!(trunc_to_bytes("abc", 0), "");
        assert_eq!(trunc_string_or_nil(None, 3), SqlArg::Null);
        assert_eq!(
            trunc_string_or_nil(Some("abcdef"), 3),
            SqlArg::Str("abc".into())
        );
    }

    // 1:1 ports of the Go `pg_test.go` helper tables (`TestCleanUTF8`,
    // `TestTruncToBytes`, `TestTruncStringOrNil`, `TestBoolOrNil`,
    // `TestNegatedBoolOrNil`, `TestTimeOrNil`, `TestIntOrNil`,
    // `TestFirstIntOrNil`, `TestStringOrNil`). Go's `\x00`, `\u0000` and
    // `\U00000000` all denote the NUL character.

    #[test]
    fn clean_utf8_go_table() {
        let test_cases: &[(&str, &str)] = &[
            ("value", "value"),
            ("val\0ue", "value"),
            ("val\u{0000}ue", "value"),
            ("v\0a\u{0000}l\u{0000}ue", "value"),
            ("平仮名, ひらがな", "平仮名, ひらがな"),
            ("\u{0000}平仮名\0ひらがな\u{0000}", "平仮名ひらがな"),
        ];
        for (index, (value, expected)) in test_cases.iter().enumerate() {
            let got = clean_utf8(value);
            assert_eq!(
                got,
                *expected,
                "test number {}, expected {expected}, got {got}",
                index + 1
            );
        }
    }

    #[test]
    fn trunc_to_bytes_go_table() {
        let test_cases: &[(&str, usize, &str, usize)] = &[
            ("value", 3, "val", 3),
            ("平仮名, ひらがな", 6, "平仮", 6),
            ("平仮名, ひらがな", 8, "平仮", 6),
            ("平仮名, ひらがな", 9, "平仮名", 9),
            ("\u{0000}平仮名, \0ひら\u{0000}がな", 9, "平仮名", 9),
        ];
        for (index, (value, n, expected_str, expected_len)) in test_cases.iter().enumerate() {
            let got_str = trunc_to_bytes(value, *n);
            assert_eq!(
                got_str,
                *expected_str,
                "test number {}, expected string {expected_str}, got {got_str}",
                index + 1
            );
            assert_eq!(
                got_str.len(),
                *expected_len,
                "test number {}, expected length {expected_len}, got {}",
                index + 1,
                got_str.len()
            );
        }
    }

    #[test]
    fn trunc_string_or_nil_go_table() {
        let s_values = [
            "value",
            "平仮名, ひらがな",
            "\u{0000}平仮名, \0ひら\u{0000}がな",
        ];
        let test_cases: &[(Option<&str>, usize, SqlArg)] = &[
            (None, 10, SqlArg::Null),
            (Some(s_values[0]), 3, SqlArg::Str("val".into())),
            (Some(s_values[1]), 6, SqlArg::Str("平仮".into())),
            (Some(s_values[2]), 9, SqlArg::Str("平仮名".into())),
        ];
        for (index, (value, n, expected)) in test_cases.iter().enumerate() {
            let got = trunc_string_or_nil(*value, *n);
            assert_eq!(
                got,
                *expected,
                "test number {}, expected {expected:?}, got {got:?}",
                index + 1
            );
        }
    }

    #[test]
    fn or_nil_scalars_go_cases() {
        // TestBoolOrNil
        assert_eq!(bool_or_nil(None), SqlArg::Null);
        assert_eq!(bool_or_nil(Some(true)), SqlArg::Bool(true));
        // TestNegatedBoolOrNil
        assert_eq!(negated_bool_or_nil(None), SqlArg::Null);
        assert_eq!(negated_bool_or_nil(Some(true)), SqlArg::Bool(!true));
        // TestTimeOrNil
        assert_eq!(time_or_nil(None), SqlArg::Null);
        let val = Utc::now();
        assert_eq!(time_or_nil(Some(val)), SqlArg::Time(val.fixed_offset()));
        // TestIntOrNil
        assert_eq!(int_or_nil(None), SqlArg::Null);
        assert_eq!(int_or_nil(Some(2)), SqlArg::Int(2));
        // TestStringOrNil
        assert_eq!(string_or_nil(None), SqlArg::Null);
        assert_eq!(
            string_or_nil(Some("hello\0 world")),
            SqlArg::Str("hello world".into())
        );
    }

    #[test]
    fn first_int_or_nil_go_table() {
        let nn1 = 1;
        let nn2 = 2;
        let test_cases: &[(&[Option<i64>], SqlArg)] = &[
            (&[], SqlArg::Null),
            (&[None], SqlArg::Null),
            (&[Some(nn1)], SqlArg::Int(nn1)),
            (&[None, None], SqlArg::Null),
            (&[None, Some(nn1)], SqlArg::Int(nn1)),
            (&[Some(nn1), None], SqlArg::Int(nn1)),
            (&[Some(nn1), Some(nn2)], SqlArg::Int(nn1)),
            (&[Some(nn2), Some(nn1)], SqlArg::Int(nn2)),
            (&[None, Some(nn2), Some(nn1)], SqlArg::Int(nn2)),
        ];
        for (index, (array, expected)) in test_cases.iter().enumerate() {
            let got = first_int_or_nil(array);
            assert_eq!(
                got,
                *expected,
                "test number {}, expected {expected:?}, got {got:?}",
                index + 1
            );
        }
    }

    #[test]
    fn identify_columns() {
        let curr: Vec<String> = ["time", "series", "period", "All", "none", "a", "b", "c"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let needed: Vec<String> = ["b"].iter().map(|s| s.to_string()).collect();
        assert_eq!(identify_columns_to_delete(&curr, &needed), vec!["a", "c"]);
        assert!(identify_columns_to_delete(&[], &needed).is_empty());
    }

    #[test]
    fn psql_names() {
        assert_eq!(escape_name("a\"b"), "a\"\"b");
        assert_eq!(make_psql_name("short", true), "short");
        assert_eq!(make_psql_name("q\"q", true), "q\"\"q");
        let long = "x".repeat(40) + &"y".repeat(40);
        let shortened = make_psql_name(&long, false);
        assert_eq!(shortened.len(), 63);
        assert!(shortened.starts_with(&"x".repeat(32)));
        assert!(shortened.ends_with(&"y".repeat(31)));
        assert!(check_psql_name("fine"));
        assert!(!check_psql_name(&long));
        assert!(check_psql_name(&"a".repeat(63)));
        assert!(!check_psql_name(&"a".repeat(64)));
        // Escaping counts towards the limit.
        assert!(!check_psql_name(&"\"".repeat(32)));
    }

    #[test]
    fn upsert_queries_match_go() {
        let t = Utc::now();
        // Tags table: single update column → no parentheses.
        let cols = vec![("repo".to_string(), SqlArg::from("r"))];
        let (q, vals) = upsert_query("tseries", &["time"], vec![SqlArg::from(t)], &cols, "time");
        assert_eq!(
            q,
            "insert into \"tseries\"(time, \"repo\") values($1, $2) on conflict(time) do update set \"repo\" = $3 where \"tseries\".time = $4"
        );
        assert_eq!(vals.len(), 4);
        assert_eq!(vals[3], SqlArg::from(t));
        // Fields table: two columns → parenthesised sets.
        let cols = vec![
            ("a".to_string(), SqlArg::Float(1.0)),
            ("b".to_string(), SqlArg::Float(2.0)),
        ];
        let (q, vals) = upsert_query(
            "sseries",
            &["time", "period"],
            vec![SqlArg::from(t), SqlArg::from("d")],
            &cols,
            "time, period",
        );
        assert_eq!(
            q,
            "insert into \"sseries\"(time, period, \"a\", \"b\") values($1, $2, $3, $4) on conflict(time, period) do update set (\"a\", \"b\") = ($5, $6) where \"sseries\".time = $7 and \"sseries\".period = $8"
        );
        assert_eq!(vals.len(), 8);
        assert_eq!(vals[7], SqlArg::from("d"));
        // Merge table.
        let (q, vals) = upsert_query(
            "smerged",
            &["time", "period", "series"],
            vec![SqlArg::from(t), SqlArg::from("d"), SqlArg::from("s")],
            &cols,
            "time, series, period",
        );
        assert_eq!(
            q,
            "insert into \"smerged\"(time, period, series, \"a\", \"b\") values($1, $2, $3, $4, $5) on conflict(time, series, period) do update set (\"a\", \"b\") = ($6, $7) where \"smerged\".time = $8 and \"smerged\".period = $9 and \"smerged\".series = $10"
        );
        assert_eq!(vals.len(), 10);
    }

    #[test]
    fn column_definitions() {
        assert_eq!(
            column_definition("c", 0),
            "\"c\" double precision not null default 0.0"
        );
        assert_eq!(
            column_definition("c", 1),
            "\"c\" timestamp not null default '1900-01-01 00:00:00'"
        );
        assert_eq!(column_definition("c", 2), "\"c\" text not null default ''");
        assert_eq!(
            column_definition("c", 3),
            "\"c\" hll not null default hll_empty()"
        );
        assert_eq!(
            field_arg(&FieldValue::Hll(vec![]), b"\\x128b7f"),
            SqlArg::Bytes(b"\\x128b7f".to_vec())
        );
        assert_eq!(
            field_arg(&FieldValue::Hll(vec![1]), b"e"),
            SqlArg::Bytes(vec![1])
        );
        assert_eq!(field_arg(&FieldValue::Float(1.5), b""), SqlArg::Float(1.5));
    }
}
