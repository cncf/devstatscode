//! PostgreSQL integration tests of `devstatscode::pg` against a real server.
//!
//! Like the Go `TestPostgres` (`pg_test.go`) these only run with
//! `PG_DB=dbtest` (and skip with a message otherwise or when
//! `DEVSTATS_SKIP_DB_TESTS=1`); `test.sh` detects a local server and sets the
//! `PG_*` variables. Every test uses its own scratch database
//! (`dbtest_<name>`), so they run in parallel.
//!
//! `go_probe_agrees_with_rust` additionally runs the *Go* reference program
//! `rust/compat/go/testdata/pgprobe` (real `lib.PgConn` + `database/sql` +
//! lib/pq) on the same statements and compares columns, driver value types
//! and renderings, scan conversions and error texts one by one.

use std::collections::{BTreeMap, HashSet};

use devstats_compat::pg::{self as tpg, TestDb};
use devstats_compat::{go_probe, run, Invocation};
use devstatscode::chrono::{DateTime, TimeZone, Utc};
use devstatscode::pg::{
    self, create_table, exec_sql, exec_sql_tx_with_err, exec_sql_with_err, insert_ignore, n_values,
    query_row_sql, query_sql, query_sql_with_err, DriverValue, PgConn, PgError, SqlArg,
};
use devstatscode::pg_scan;
use devstatscode::ts_points::{FieldValue, Fields, TSPoint, Tags};

/// Go `getInts` from `pg_test.go`.
fn get_ints(c: &PgConn, ctx: &devstatscode::Ctx) -> Vec<i64> {
    let mut rows = query_sql_with_err(c, ctx, "select an_int from test order by an_int asc", &[]);
    let mut arr = Vec::new();
    let mut i: i64 = 0;
    while rows.next() {
        pg_scan!(rows, i).unwrap();
        arr.push(i);
    }
    rows.err().unwrap();
    rows.close().unwrap();
    arr
}

/// Port of Go `TestPostgres`: database create/drop, table creation,
/// inserts, `QueryRowSQL`, transactions (rollback + commit), `InsertIgnore`.
#[test]
fn test_postgres_go_port() {
    let Some(db) = TestDb::fresh("go_port") else {
        return;
    };
    let ctx = &db.ctx;
    // Go: DropDatabaseIfExists + CreateDatabaseIfNeeded (done by TestDb) —
    // the database must now exist and creating it again is a no-op.
    let mut ctx2 = ctx.clone();
    assert!(!pg::create_database_if_needed(&mut ctx2));
    assert!(pg::database_exists(&mut ctx2, true).0);

    let c = db.conn();
    exec_sql_with_err(
        &c,
        ctx,
        &create_table("test(an_int int, a_string text, a_dt {{ts}}, primary key(an_int))"),
        &[],
    );
    exec_sql_with_err(
        &c,
        ctx,
        &format!("insert into test(an_int, a_string, a_dt) {}", n_values(3)),
        &[
            SqlArg::from(1),
            SqlArg::from("string"),
            SqlArg::from(Utc::now()),
        ],
    );
    let mut i: i64 = 0;
    pg_scan!(query_row_sql(&c, ctx, "select an_int from test", &[]), i).unwrap();
    assert_eq!(i, 1, "expected to insert 1");

    exec_sql_with_err(
        &c,
        ctx,
        &format!("insert into test(an_int, a_string, a_dt) {}", n_values(3)),
        &[
            SqlArg::from(11),
            SqlArg::from("another string"),
            SqlArg::from(Utc::now()),
        ],
    );
    assert_eq!(get_ints(&c, ctx), vec![1, 11], "after two inserts");

    // Transaction rolled back.
    {
        let mut tx = c.begin().unwrap();
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &format!("insert into test(an_int, a_string, a_dt) {}", n_values(3)),
            &[
                SqlArg::from(21),
                SqlArg::from("this will be rolled back"),
                SqlArg::from(Utc::now()),
            ],
        );
        tx.rollback().unwrap();
    }
    assert_eq!(get_ints(&c, ctx), vec![1, 11], "after rollback");

    // Transaction committed.
    {
        let mut tx = c.begin().unwrap();
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &format!("insert into test(an_int, a_string, a_dt) {}", n_values(3)),
            &[
                SqlArg::from(31),
                SqlArg::from("this will be committed"),
                SqlArg::from(Utc::now()),
            ],
        );
        tx.commit().unwrap();
    }
    assert_eq!(get_ints(&c, ctx), vec![1, 11, 31], "after commit");

    // Insert ignore (violates the primary key).
    exec_sql_with_err(
        &c,
        ctx,
        &insert_ignore(&format!(
            "into test(an_int, a_string, a_dt) {}",
            n_values(3)
        )),
        &[
            SqlArg::from(1),
            SqlArg::from("conflicting key"),
            SqlArg::from(Utc::now()),
        ],
    );
    assert_eq!(get_ints(&c, ctx), vec![1, 11, 31], "after insert ignore");

    // Dropped transaction = rollback (Go: a Tx that is never committed is
    // rolled back when the connection is closed).
    {
        let mut tx = c.begin().unwrap();
        tx.exec("insert into test(an_int) values(41)", &[]).unwrap();
    }
    assert_eq!(get_ints(&c, ctx), vec![1, 11, 31], "after dropped tx");
    c.close();
    assert!(c.is_closed());
    assert_eq!(
        c.exec("select 1", &[]).unwrap_err().to_string(),
        "sql: database is closed"
    );
}

/// Every DevStats-relevant column type round-trips through the driver
/// values and the typed scan destinations the tools use.
#[test]
fn typed_round_trips() {
    let Some(db) = TestDb::fresh("types") else {
        return;
    };
    let ctx = &db.ctx;
    let c = db.conn();
    exec_sql_with_err(
        &c,
        ctx,
        "create table t(i2 smallint, i4 int, i8 bigint, f4 real, f8 double precision, \
         b bool, s text, v varchar(10), ch char(3), by bytea, ts timestamp, tz timestamptz, \
         d date, n numeric(10,2), j jsonb, arr int[], u uuid, iv interval, nm name)",
        &[],
    );
    let ts = Utc.with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap()
        + devstatscode::chrono::Duration::nanoseconds(123456000);
    exec_sql_with_err(
        &c,
        ctx,
        &format!(
            "insert into t {}",
            n_values(19)
                .replace("$14", "$14::numeric")
                .replace("$15", "$15::jsonb")
        ),
        &[
            SqlArg::from(-7i16),
            SqlArg::from(123456i32),
            SqlArg::from(-9007199254740993i64),
            SqlArg::from(1.5f32),
            SqlArg::from(-0.1f64),
            SqlArg::from(true),
            SqlArg::from("héllo\twörld"),
            SqlArg::from("var"),
            SqlArg::from("ab"),
            SqlArg::from(vec![0u8, 1, 255, 10]),
            SqlArg::from(ts),
            SqlArg::from(ts),
            SqlArg::from(ts),
            SqlArg::from("1234.50"),
            SqlArg::from(r#"{"a": [1, 2], "b": null}"#),
            SqlArg::from("{1,2,3}"),
            SqlArg::from("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            SqlArg::from("1 day 02:03:04"),
            SqlArg::from("some_name"),
        ],
    );
    let mut rows = c.query("select * from t", &[]).unwrap();
    assert_eq!(
        rows.column_names(),
        vec![
            "i2", "i4", "i8", "f4", "f8", "b", "s", "v", "ch", "by", "ts", "tz", "d", "n", "j",
            "arr", "u", "iv", "nm"
        ]
    );
    assert!(rows.next());
    let vals: Vec<DriverValue> = rows.values().to_vec();
    assert!(!rows.next());
    rows.err().unwrap();
    // Driver value types = what lib/pq gives Go (`%T`).
    let types: Vec<&str> = vals.iter().map(|v| v.go_type_name()).collect();
    assert_eq!(
        types,
        vec![
            "int64",
            "int64",
            "int64",
            "float64",
            "float64",
            "bool",
            "string",
            "string",
            "[]uint8",
            "[]uint8",
            "time.Time",
            "time.Time",
            "time.Time",
            "[]uint8",
            "[]uint8",
            "[]uint8",
            "[]uint8",
            "[]uint8",
            "[]uint8"
        ]
    );
    assert_eq!(vals[0], DriverValue::Int(-7));
    assert_eq!(vals[1], DriverValue::Int(123456));
    assert_eq!(vals[2], DriverValue::Int(-9007199254740993));
    assert_eq!(vals[3], DriverValue::Float(1.5));
    assert_eq!(vals[4], DriverValue::Float(-0.1));
    assert_eq!(vals[5], DriverValue::Bool(true));
    assert_eq!(vals[6], DriverValue::Str("héllo\twörld".into()));
    assert_eq!(vals[7], DriverValue::Str("var".into()));
    // char(n) (bpchar) is not one of lib/pq's string OIDs → raw bytes, blank padded
    assert_eq!(vals[8], DriverValue::Bytes(b"ab ".to_vec()));
    assert_eq!(vals[9], DriverValue::Bytes(vec![0, 1, 255, 10]));
    // timestamp (no zone) is returned as UTC wall-clock time
    let want_ts = ts.fixed_offset();
    assert_eq!(vals[10], DriverValue::Time(want_ts));
    // timestamptz: same instant, the server's session offset — compare instants
    match &vals[11] {
        DriverValue::Time(t) => assert_eq!(t.with_timezone(&Utc), ts),
        other => panic!("tz: {other:?}"),
    }
    assert_eq!(
        vals[12],
        DriverValue::Time(
            Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0)
                .unwrap()
                .fixed_offset()
        )
    );
    assert_eq!(vals[13], DriverValue::Bytes(b"1234.50".to_vec()));
    assert_eq!(
        vals[14],
        DriverValue::Bytes(br#"{"a": [1, 2], "b": null}"#.to_vec())
    );
    assert_eq!(vals[15], DriverValue::Bytes(b"{1,2,3}".to_vec()));
    assert_eq!(
        vals[16],
        DriverValue::Bytes(b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".to_vec())
    );
    assert_eq!(vals[17], DriverValue::Bytes(b"1 day 02:03:04".to_vec()));
    assert_eq!(vals[18], DriverValue::Bytes(b"some_name".to_vec()));

    // Typed scans (the destinations DevStats uses).
    let (mut i2, mut i4, mut i8) = (0i16, 0i32, 0i64);
    let (mut f4, mut f8) = (0f64, 0f64);
    let mut b = false;
    let (mut s, mut v, mut ch) = (String::new(), String::new(), String::new());
    let mut by: Vec<u8> = Vec::new();
    let (mut ts_d, mut tz_d, mut d_d): (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) =
        (Utc::now(), Utc::now(), Utc::now());
    let mut n = String::new();
    let mut j: Vec<u8> = Vec::new();
    let mut arr = String::new();
    let mut u = String::new();
    let mut iv = String::new();
    let mut nm = String::new();
    let row = c.query_row("select * from t", &[]);
    pg_scan!(row, i2, i4, i8, f4, f8, b, s, v, ch, by, ts_d, tz_d, d_d, n, j, arr, u, iv, nm)
        .unwrap();
    assert_eq!(
        (i2, i4, i8, f4, f8, b),
        (-7, 123456, -9007199254740993, 1.5, -0.1, true)
    );
    assert_eq!(
        (s.as_str(), v.as_str(), ch.as_str()),
        ("héllo\twörld", "var", "ab ")
    );
    assert_eq!(by, vec![0, 1, 255, 10]);
    assert_eq!(ts_d, ts);
    assert_eq!(tz_d, ts);
    assert_eq!(d_d, Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0).unwrap());
    assert_eq!(n, "1234.50");
    assert_eq!(j, br#"{"a": [1, 2], "b": null}"#.to_vec());
    assert_eq!(arr, "{1,2,3}");
    assert_eq!(u, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert_eq!(iv, "1 day 02:03:04");
    assert_eq!(nm, "some_name");

    // Go-style string conversions of non-string values (database/sql
    // `convertAssign`): ints/floats/bools/times into *string and *[]byte.
    let (mut si, mut sf, mut sb, mut st) =
        (String::new(), String::new(), String::new(), String::new());
    let row = c.query_row("select i8, f8, b, ts from t", &[]);
    pg_scan!(row, si, sf, sb, st).unwrap();
    assert_eq!(
        (si.as_str(), sf.as_str(), sb.as_str(), st.as_str()),
        (
            "-9007199254740993",
            "-0.1",
            "true",
            "2020-01-02T03:04:05.123456Z"
        )
    );
    let mut bi: Vec<u8> = Vec::new();
    pg_scan!(c.query_row("select i4 from t", &[]), bi).unwrap();
    assert_eq!(bi, b"123456".to_vec());
    // ...and text into numbers the way `strconv` does.
    let (mut ti, mut tf) = (0i64, 0f64);
    pg_scan!(c.query_row("select '42'::text, '2.5'::text", &[]), ti, tf).unwrap();
    assert_eq!((ti, tf), (42, 2.5));

    // NULL handling: Option<T> destinations and the Go error texts.
    let (mut oi, mut os, mut ot): (Option<i64>, Option<String>, Option<DateTime<Utc>>) =
        (Some(1), Some("x".into()), Some(Utc::now()));
    pg_scan!(
        c.query_row("select null::int, null::text, null::timestamp", &[]),
        oi,
        os,
        ot
    )
    .unwrap();
    assert_eq!((oi, os, ot), (None, None, None));
    let mut plain = 0i64;
    let err = pg_scan!(c.query_row("select null::int as n", &[]), plain).unwrap_err();
    assert_eq!(
        err.to_string(),
        "sql: Scan error on column index 0, name \"n\": converting NULL to int64 is unsupported"
    );
    let mut plain_s = String::new();
    let err = pg_scan!(c.query_row("select null::text as n", &[]), plain_s).unwrap_err();
    assert_eq!(
        err.to_string(),
        "sql: Scan error on column index 0, name \"n\": converting NULL to string is unsupported"
    );
    // NULL into *[]byte is fine in Go (nil slice) — runq relies on it.
    let mut nb: Vec<u8> = vec![1];
    pg_scan!(c.query_row("select null::text", &[]), nb).unwrap();
    assert!(nb.is_empty());
    let mut bad = 0i64;
    let err = pg_scan!(c.query_row("select 'abc'::text as s", &[]), bad).unwrap_err();
    assert_eq!(
        err.to_string(),
        "sql: Scan error on column index 0, name \"s\": converting driver.Value type string (\"abc\") to a int64: invalid syntax"
    );
    c.close();
}

/// Parameters of every `SqlArg` kind reach the server with the right value
/// (text encoding like lib/pq), including NULL and bytea.
#[test]
fn parameters_of_every_kind() {
    let Some(db) = TestDb::fresh("params") else {
        return;
    };
    let c = db.conn();
    let t = Utc.with_ymd_and_hms(2021, 12, 31, 23, 59, 59).unwrap()
        + devstatscode::chrono::Duration::microseconds(999999);
    let mut rows = c
        .query(
            "select $1::bigint, $2::double precision, $3::bool, $4::text, $5::bytea, $6::timestamp, $7::text is null, pg_typeof($1)::text",
            &[
                SqlArg::from(i64::MIN),
                SqlArg::from(1e300),
                SqlArg::from(false),
                SqlArg::from("it's \"quoted\" \\ back"),
                SqlArg::from(vec![0u8, 255, b'\'', b'\\']),
                SqlArg::from(t),
                SqlArg::Null,
            ],
        )
        .unwrap();
    assert!(rows.next());
    let got = rows.values().to_vec();
    assert_eq!(got[0], DriverValue::Int(i64::MIN));
    assert_eq!(got[1], DriverValue::Float(1e300));
    assert_eq!(got[2], DriverValue::Bool(false));
    assert_eq!(got[3], DriverValue::Str("it's \"quoted\" \\ back".into()));
    assert_eq!(got[4], DriverValue::Bytes(vec![0, 255, b'\'', b'\\']));
    assert_eq!(got[5], DriverValue::Time(t.fixed_offset()));
    assert_eq!(got[6], DriverValue::Bool(true));
    assert_eq!(got[7], DriverValue::Str("bigint".into()));
    assert!(!rows.next());
    // Bytes sent for an hll/text parameter are passed as raw text (that is how
    // DevStats ships HLL sketches: hex text in a []byte).
    let mut s = String::new();
    pg_scan!(
        c.query_row("select $1::text", &[SqlArg::from(b"\\x1234".to_vec())]),
        s
    )
    .unwrap();
    assert_eq!(s, "\\x1234");
    // Floats: Go 'f' formatting (no exponent) survives.
    let mut f = 0f64;
    pg_scan!(
        c.query_row("select $1::double precision", &[SqlArg::from(0.000001234)]),
        f
    )
    .unwrap();
    assert_eq!(f, 0.000001234);
    c.close();
}

/// Error paths: server errors carry SQLSTATE + name, `NoRows`, bad database,
/// failed transactions, use after close.
#[test]
fn error_paths() {
    let Some(db) = TestDb::fresh("errors") else {
        return;
    };
    let ctx = &db.ctx;
    let c = db.conn();
    let err = c.query("selec 1", &[]).unwrap_err();
    assert_eq!(err.to_string(), "pq: syntax error at or near \"selec\"");
    assert_eq!(err.code(), "42601");
    assert_eq!(err.name(), "syntax_error");
    assert_eq!(err.go_type_name(), "*pq.Error");
    let se = err.server().unwrap();
    assert_eq!(se.severity, "ERROR");
    assert_eq!(se.position, "1");

    let err = c.exec("select * from no_such_table", &[]).unwrap_err();
    assert_eq!(err.name(), "undefined_table");
    assert_eq!(
        err.to_string(),
        "pq: relation \"no_such_table\" does not exist"
    );
    // The connection is still usable afterwards (error was not FATAL).
    let mut one = 0i64;
    pg_scan!(c.query_row("select 1", &[]), one).unwrap();
    assert_eq!(one, 1);
    assert_eq!(c.connections_opened(), 1, "errors do not burn connections");

    // Row.scan on an empty result → sql.ErrNoRows
    let err = pg_scan!(c.query_row("select 1 where false", &[]), one).unwrap_err();
    assert!(err.is_no_rows());
    assert_eq!(err.to_string(), "sql: no rows in result set");
    // query_sql (Go QuerySQL) returns the error instead of dying
    assert!(query_sql(&c, ctx, "select * from nope", &[]).is_err());
    // exec_sql on a statement with a parameter type mismatch
    let err = exec_sql(&c, ctx, "select $1::int", &[SqlArg::from("abc")]).unwrap_err();
    assert_eq!(err.name(), "invalid_text_representation");

    // Unknown database → invalid_catalog_name (3D000) at first use (like
    // Go, `sql.Open`/`pg_conn_err` do not dial yet)
    let mut bad = ctx.clone();
    bad.pg_db = "dbtest_definitely_missing_db".into();
    let pool = pg::pg_conn_err(&bad).unwrap();
    let err = pool.ping().unwrap_err();
    assert_eq!(err.code(), "3D000");
    assert_eq!(err.name(), "invalid_catalog_name");
    assert_eq!(
        err.to_string(),
        "pq: database \"dbtest_definitely_missing_db\" does not exist"
    );
    // Wrong port → Go *net.OpError text
    let mut bad = ctx.clone();
    bad.pg_port = "1".into();
    let err = pg::pg_conn_err(&bad).unwrap().ping().unwrap_err();
    assert_eq!(err.go_type_name(), "*net.OpError");
    assert!(
        err.to_string().starts_with("dial tcp ") && err.to_string().contains("connection refused"),
        "{err}"
    );

    // Failed transaction: statements after an error fail with 25P02 and
    // commit performs a rollback (lib/pq behaviour).
    {
        let mut tx = c.begin().unwrap();
        assert!(tx.exec("select 1/0", &[]).is_err());
        let err = tx.exec("select 1", &[]).unwrap_err();
        assert_eq!(err.name(), "in_failed_sql_transaction");
        let err = tx.commit().unwrap_err();
        assert_eq!(
            err.to_string(),
            "pq: Could not complete operation in a failed transaction"
        );
    }
    pg_scan!(c.query_row("select 1", &[]), one).unwrap();
    c.close();
    assert_eq!(
        c.query("select 1", &[]).unwrap_err().to_string(),
        "sql: database is closed"
    );
}

/// Pool behaviour: connections are reused, at most two are kept idle, a
/// reset drops them, statements retry on a killed connection.
#[test]
fn pool_behaviour() {
    let Some(db) = TestDb::fresh("pool") else {
        return;
    };
    let c = db.conn();
    assert_eq!(c.connections_opened(), 0, "lazy connect");
    for _ in 0..10 {
        let mut one = 0i64;
        pg_scan!(c.query_row("select 1", &[]), one).unwrap();
    }
    assert_eq!(c.connections_opened(), 1);
    assert_eq!(c.idle_connections(), 1);
    // Three open result sets need three connections; on close only two stay idle.
    {
        let r1 = c.query("select generate_series(1, 3)", &[]).unwrap();
        let r2 = c.query("select generate_series(1, 3)", &[]).unwrap();
        let r3 = c.query("select generate_series(1, 3)", &[]).unwrap();
        assert_eq!(c.connections_opened(), 3);
        drop((r1, r2, r3));
    }
    assert_eq!(c.idle_connections(), 2);
    c.reset();
    assert_eq!(c.idle_connections(), 0);
    let mut one = 0i64;
    pg_scan!(c.query_row("select 1", &[]), one).unwrap();
    assert_eq!(c.connections_opened(), 4);
    // Kill our own backend from another session: the next statement on the
    // dead connection is retried transparently (database/sql ErrBadConn).
    let mut pid = 0i64;
    pg_scan!(c.query_row("select pg_backend_pid()", &[]), pid).unwrap();
    let killer = db.conn();
    let mut killed = false;
    pg_scan!(
        killer.query_row("select pg_terminate_backend($1)", &[SqlArg::from(pid)]),
        killed
    )
    .unwrap();
    assert!(killed);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let mut pid2 = 0i64;
    pg_scan!(c.query_row("select pg_backend_pid()", &[]), pid2).unwrap();
    assert_ne!(pid, pid2);
    // Ping
    c.ping().unwrap();
    killer.close();
    c.close();
}

/// Multi-statement strings (simple protocol, no args) and rows-affected
/// counts of the command tags.
#[test]
fn exec_results_and_multi_statements() {
    let Some(db) = TestDb::fresh("exec") else {
        return;
    };
    let ctx = &db.ctx;
    let c = db.conn();
    let r = exec_sql_with_err(&c, ctx, "create table t(a int)", &[]);
    assert_eq!(r.rows_affected().unwrap(), 0);
    let r = exec_sql_with_err(&c, ctx, "insert into t values(1),(2),(3)", &[]);
    assert_eq!(r.rows_affected().unwrap(), 3);
    let r = exec_sql_with_err(&c, ctx, "update t set a = a + 1 where a > 1", &[]);
    assert_eq!(r.rows_affected().unwrap(), 2);
    let r = exec_sql_with_err(&c, ctx, "delete from t where a = 4", &[]);
    assert_eq!(r.rows_affected().unwrap(), 1);
    // lib/pq simple query: several statements in one string, last tag wins
    let r = exec_sql_with_err(
        &c,
        ctx,
        "insert into t values(10); insert into t values(11); insert into t values(12), (13)",
        &[],
    );
    assert_eq!(r.rows_affected().unwrap(), 2);
    let mut n = 0i64;
    pg_scan!(c.query_row("select count(*) from t", &[]), n).unwrap();
    assert_eq!(n, 6);
    // `select` through exec reports the row count too
    let r = exec_sql_with_err(&c, ctx, "select * from t", &[]);
    assert_eq!(r.rows_affected().unwrap(), 6);
    // Empty query
    let r = c.exec("", &[]).unwrap();
    assert!(r.rows_affected().is_err());
    // `{{ts}}` / `{{tsnow}}` in create_table
    exec_sql_with_err(
        &c,
        ctx,
        &create_table("t2(d {{tsnow}} not null, id {{pkauto}}, primary key(d))"),
        &[],
    );
    assert!(pg::table_exists(&c, ctx, "t2"));
    assert!(pg::table_column_exists(&c, ctx, "t2", "d"));
    assert!(!pg::table_column_exists(&c, ctx, "t2", "x"));
    assert!(!pg::table_exists(&c, ctx, "t3"));
    let (cols, set) = pg::get_current_table_columns(&c, ctx, "t2").unwrap();
    assert_eq!(cols, vec!["d".to_string(), "id".to_string()]);
    assert!(set.contains("d"));
    let r = exec_sql_with_err(&c, ctx, "insert into t2 default values", &[]);
    assert_eq!(r.rows_affected().unwrap(), 1);
    let mut id = 0i64;
    pg_scan!(c.query_row("select id from t2", &[]), id).unwrap();
    assert_eq!(id, 1);
    c.close();
}

fn tags(pairs: &[(&str, &str)]) -> Tags {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn fields(pairs: Vec<(&str, FieldValue)>) -> Fields {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// `write_ts_points`: series tables (`s<name>`), tag tables (`t<name>`) and
/// a merged series table are created with the expected columns/indexes,
/// points are upserted (second write updates), `get_tag_values` reads back.
#[test]
fn write_ts_points_plain_and_merged() {
    let Some(db) = TestDb::fresh("tspoints") else {
        return;
    };
    let ctx = &db.ctx;
    let c = db.conn();
    // The grants in WriteTSPointsBatch target these roles; they are reported
    // and ignored when missing, but create them so the schema is exact.
    for role in ["ro_user", "devstats_team"] {
        let _ = c.exec(&format!("create role {role}"), &[]);
    }
    let t1 = Utc.with_ymd_and_hms(2020, 3, 4, 5, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2020, 3, 4, 6, 0, 0).unwrap();
    let pts = vec![
        TSPoint {
            t: t1,
            added: Utc::now().fixed_offset(),
            period: "d".into(),
            name: "metric_a".into(),
            tags: None,
            fields: Some(fields(vec![
                ("value", FieldValue::Float(1.5)),
                ("descr", FieldValue::Str("first".into())),
                ("dt", FieldValue::Time(t2)),
            ])),
        },
        TSPoint {
            t: t2,
            added: Utc::now().fixed_offset(),
            period: "d".into(),
            name: "metric_a".into(),
            tags: None,
            fields: Some(fields(vec![
                ("value", FieldValue::Float(2.5)),
                ("descr", FieldValue::Str("second".into())),
                ("dt", FieldValue::Time(t1)),
            ])),
        },
        TSPoint {
            t: t1,
            added: Utc::now().fixed_offset(),
            period: String::new(),
            name: "tags_a".into(),
            tags: Some(tags(&[("name", "alpha"), ("value", "1")])),
            fields: None,
        },
        TSPoint {
            t: t2,
            added: Utc::now().fixed_offset(),
            period: String::new(),
            name: "tags_a".into(),
            tags: Some(tags(&[("name", "beta"), ("value", "2")])),
            fields: None,
        },
    ];
    pg::write_ts_points(ctx, &c, &pts, "", b"", None);
    assert_eq!(tpg::tables(&c), vec!["smetric_a", "ttags_a"]);
    let cols: Vec<(String, String)> = tpg::table_columns(&c, "smetric_a")
        .into_iter()
        .map(|(n, ty, _, _)| (n, ty))
        .collect();
    assert_eq!(
        cols,
        vec![
            (
                "time".to_string(),
                "timestamp without time zone".to_string()
            ),
            ("period".to_string(), "text".to_string()),
            ("descr".to_string(), "text".to_string()),
            ("dt".to_string(), "timestamp without time zone".to_string()),
            ("value".to_string(), "double precision".to_string()),
        ]
    );
    let idx: Vec<String> = tpg::table_indexes(&c, "smetric_a")
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(idx, vec!["imetric_ap", "imetric_at", "smetric_a_pkey"]);
    let idx: Vec<String> = tpg::table_indexes(&c, "ttags_a")
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(idx, vec!["itags_aname", "itags_avalue", "ttags_a_pkey"]);
    let snap = tpg::snapshot(
        &c,
        "select time, period, value, descr, dt from smetric_a order by time",
        &[],
    );
    assert_eq!(
        snap.rows,
        vec![
            vec![
                "2020-03-04T05:00:00Z",
                "d",
                "1.5",
                "first",
                "2020-03-04T06:00:00Z"
            ],
            vec![
                "2020-03-04T06:00:00Z",
                "d",
                "2.5",
                "second",
                "2020-03-04T05:00:00Z"
            ],
        ]
    );
    assert_eq!(
        pg::get_tag_values(&c, ctx, "tags_a", "name"),
        vec!["alpha", "beta"]
    );
    // Second write: upsert updates values, adds a new column, keeps rows unique.
    let pts2 = vec![TSPoint {
        t: t1,
        added: Utc::now().fixed_offset(),
        period: "d".into(),
        name: "metric_a".into(),
        tags: None,
        fields: Some(fields(vec![
            ("value", FieldValue::Float(9.0)),
            ("extra", FieldValue::Float(1.0)),
        ])),
    }];
    pg::write_ts_points(ctx, &c, &pts2, "", b"", None);
    let snap = tpg::snapshot(
        &c,
        "select value, descr, extra from smetric_a order by time",
        &[],
    );
    assert_eq!(
        snap.rows,
        vec![vec!["9", "first", "1"], vec!["2.5", "second", "0"]]
    );
    assert!(pg::table_column_exists(&c, ctx, "smetric_a", "extra"));

    // Merged series: one table `s<merge>` with a `series` column.
    let pts3 = vec![
        TSPoint {
            t: t1,
            added: Utc::now().fixed_offset(),
            period: "w".into(),
            name: "m1".into(),
            tags: None,
            fields: Some(fields(vec![("value", FieldValue::Float(10.0))])),
        },
        TSPoint {
            t: t1,
            added: Utc::now().fixed_offset(),
            period: "w".into(),
            name: "m2".into(),
            tags: None,
            fields: Some(fields(vec![
                ("value", FieldValue::Float(20.0)),
                ("other", FieldValue::Str("x".into())),
            ])),
        },
    ];
    pg::write_ts_points(ctx, &c, &pts3, "merged", b"", None);
    assert!(pg::table_exists(&c, ctx, "smerged"));
    let cols: Vec<String> = tpg::table_columns(&c, "smerged")
        .into_iter()
        .map(|(n, _, _, _)| n)
        .collect();
    assert_eq!(cols, vec!["time", "series", "period", "value", "other"]);
    let idx: Vec<String> = tpg::table_indexes(&c, "smerged")
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(
        idx,
        vec!["imergedp", "imergeds", "imergedt", "smerged_pkey"]
    );
    let snap = tpg::snapshot(
        &c,
        "select series, period, value, other from smerged order by series",
        &[],
    );
    assert_eq!(
        snap.rows,
        vec![vec!["m1", "w", "10", ""], vec!["m2", "w", "20", "x"]]
    );
    // Points whose names are not valid identifiers are skipped, not fatal.
    let bad = vec![TSPoint {
        t: t1,
        added: Utc::now().fixed_offset(),
        period: "d".into(),
        name: "x".repeat(70),
        tags: None,
        fields: Some(fields(vec![("value", FieldValue::Float(1.0))])),
    }];
    pg::write_ts_points(ctx, &c, &bad, "", b"", None);
    assert_eq!(tpg::tables(&c).len(), 3);
    c.close();
}

/// HLL fields (only when the `hll` extension exists on the server — the
/// FreeBSD host has none; the k8s DevStats databases do).
#[test]
fn write_ts_points_hll() {
    let Some(db) = TestDb::fresh("hll") else {
        return;
    };
    let ctx = &db.ctx;
    let c = db.conn();
    if !tpg::hll_available(&c) {
        eprintln!("[compat] hll extension not available on the test server — skipping");
        return;
    }
    exec_sql_with_err(&c, ctx, "create extension if not exists hll", &[]);
    let mut empty: Vec<u8> = Vec::new();
    pg_scan!(c.query_row("select hll_empty()", &[]), empty).unwrap();
    let mut sketch: Vec<u8> = Vec::new();
    pg_scan!(
        c.query_row("select hll_add(hll_empty(), hll_hash_text('a'))", &[]),
        sketch
    )
    .unwrap();
    let t1 = Utc.with_ymd_and_hms(2020, 3, 4, 5, 0, 0).unwrap();
    let pts = vec![TSPoint {
        t: t1,
        added: Utc::now().fixed_offset(),
        period: "d".into(),
        name: "h".into(),
        tags: None,
        fields: Some(fields(vec![
            ("sketch", FieldValue::Hll(sketch)),
            ("none", FieldValue::Hll(Vec::new())),
        ])),
    }];
    pg::write_ts_points(ctx, &c, &pts, "", &empty, None);
    let snap = tpg::snapshot(
        &c,
        "select hll_cardinality(sketch)::int, hll_cardinality(none)::int from sh",
        &[],
    );
    assert_eq!(snap.rows, vec![vec!["1", "0"]]);
    c.close();
}

/// Column bookkeeping helpers used by `columns`/`calc_metric`.
#[test]
fn column_helpers() {
    let Some(db) = TestDb::fresh("columns") else {
        return;
    };
    let ctx = &db.ctx;
    let c = db.conn();
    exec_sql_with_err(
        &c,
        ctx,
        "create table stab(time timestamp, period text, a double precision, b double precision, c text)",
        &[],
    );
    let (cols, set) = pg::get_current_table_columns(&c, ctx, "stab").unwrap();
    assert_eq!(cols, vec!["time", "period", "a", "b", "c"]);
    assert_eq!(set.len(), 5);
    let needed: Vec<String> = ["time", "period", "b"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let to_drop = pg::identify_columns_to_delete(&cols, &needed);
    assert_eq!(to_drop, vec!["a", "c"]);
    // fewer than 80 columns: nothing dropped
    let protected: HashSet<String> = ["time", "period"].iter().map(|s| s.to_string()).collect();
    assert!(!pg::drop_least_used_col(
        &c, ctx, "stab", "info", &protected
    ));
    // an unrelated error is not "row is too big"
    let err = c.exec("select * from nope", &[]).unwrap_err();
    assert!(!pg::handle_row_is_too_big(
        &c,
        ctx,
        "stab",
        "info",
        None,
        Some(&err)
    ));
    assert!(!pg::handle_row_is_too_big(
        &c, ctx, "stab", "info", None, None
    ));
    // a table with 90 columns and a "row is too big" error → two dropped
    let mut sq = String::from("create table wide(time timestamp, period text");
    for i in 0..90 {
        sq.push_str(&format!(", c{i:02} double precision default 0"));
    }
    sq.push(')');
    exec_sql_with_err(&c, ctx, &sq, &[]);
    exec_sql_with_err(
        &c,
        ctx,
        "insert into wide(time, period, c05, c07) values(now(), 'd', 1.0, 0.5), (now(), 'w', 3.0, 0.2)",
        &[],
    );
    let too_big = PgError::server_err(pg::ServerError {
        severity: "ERROR".into(),
        code: "54000".into(),
        message: "row is too big: size 8168, maximum size 8160".into(),
        ..pg::ServerError::default()
    });
    assert!(pg::handle_row_is_too_big(
        &c,
        ctx,
        "wide",
        "info",
        None,
        Some(&too_big)
    ));
    let (cols, _) = pg::get_current_table_columns(&c, ctx, "wide").unwrap();
    assert_eq!(cols.len(), 90);
    // c00 and c01 have the lowest (zero) averages and sort first
    assert!(!cols.contains(&"c00".to_string()));
    assert!(!cols.contains(&"c01".to_string()));
    assert!(cols.contains(&"c05".to_string()));
    c.close();
}

// ---------------------------------------------------------------------------
// Differential test against the Go reference program
// ---------------------------------------------------------------------------

/// Statements fed to both implementations (`\t`-separated probe arguments).
const PROBE_SCRIPT: &[&str] = &[
    "!create table t(i2 smallint, i4 int, i8 bigint, f4 real, f8 double precision, b bool, s text, v varchar(10), ch char(3), by bytea, ts timestamp, d date, n numeric(10,2), j jsonb, arr int[], u uuid, iv interval, nm name)",
    "!insert into t values(-7, 123456, -9007199254740993, 1.5, -0.1, true, E'h\\u00e9llo\\tw\\u00f6rld', 'var', 'ab', '\\x00017f0a'::bytea, '2020-01-02 03:04:05.123456', '2020-01-02', 1234.50, '{\"a\": [1, 2], \"b\": null}', '{1,2,3}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '1 day 02:03:04', 'some_name')",
    "select * from t",
    "select null::int as n, null::text as s, null::timestamp as t, null::bytea as b",
    "select 1 where false",
    "select $1::bigint as a, $2::double precision as b, $3::bool as c, $4::text as d, $5::bytea as e, $6::timestamp as f, $7::text is null as g\ti:-9223372036854775808\tf:1e300\tb:false\ts:it's \"quoted\" \\ back\tx:007f275c\tt:2021-12-31T23:59:59.999999Z\tn:",
    "select $1::text as raw_bytes\tx:5c7831323334",
    // a time argument with a non-UTC offset is sent in its own zone (lib/pq
    // FormatTimestamp): a `timestamp` column keeps the wall-clock time,
    // `timestamptz` the instant (rendered as text to stay zone-name independent).
    "select $1::timestamp as wall, ($1::timestamptz at time zone 'UTC')::text as instant, $1::text as raw\tt:2021-06-30T23:30:00.5+02:00",
    "select $1::double precision as f\tf:0.000001234",
    "select '42'::text as i, '2.5'::text as f, 'true'::text as b, '1'::text as one, 't'::text as t",
    "select 1::int as one, 0::int as zero, 2::int as two",
    "select 1.0::float8 as one, 1e10::float8 as big, 'NaN'::float8 as nan, '-Infinity'::float8 as ninf, 0.1::float8 as tenth",
    "select '2020-01-02 03:04:05'::timestamp as ts, '2020-01-02 03:04:05.5'::timestamp as half, '1900-01-01'::date as d, '0001-01-01 00:00:00'::timestamp as min",
    "select 'abc'::text as s, ''::text as empty, 'x'::char(3) as padded",
    // timestamptz values scanned into *string/*[]byte (RFC3339Nano, zone-name
    // independent); their %v carries the session zone's abbreviation, which
    // the Rust layer does not reproduce, so they are not selected as `V=`.
    "select ('2020-01-02 03:04:05.123456+00'::timestamptz)::text as tz_text, to_char('2020-01-02 03:04:05+00'::timestamptz at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS') as tz_utc",
    "select true as t, false as f",
    "selec 1",
    "select * from no_such_table",
    "!insert into t values(1)",
    "select $1::int as bad\ts:abc",
    "!update t set i4 = i4 + 1",
    "!delete from t where i4 = 123457",
    "!insert into t(i4) values(1),(2),(3)",
    "select count(*) as c from t",
    "select i4 from t order by i4",
    "!",
    "!select 1",
    "!create table t2(a int); insert into t2 values(1),(2)",
    "select a from t2 order by a",
    "select generate_series(1, 5) as g",
];

/// Render the probe's `R T=… | V=… | S=… | …` line for one Rust driver value:
/// Go `%T`, `%v`, and the outcome of scanning it into each destination type.
fn rust_probe_row(v: &DriverValue) -> String {
    fn scan<T: pg::ScanDest + Default>(v: &DriverValue, show: impl Fn(&T) -> String) -> String {
        let mut dest = T::default();
        match dest.scan_from(v) {
            Ok(()) => format!("ok:{}", show(&dest)),
            Err(e) => format!("err:{e}"),
        }
    }
    let bytes_show = |b: &Vec<u8>| {
        let parts: Vec<String> = b.iter().map(|x| x.to_string()).collect();
        format!("[{}]", parts.join(" "))
    };
    let i_show = |i: &i64| i.to_string();
    let f_show = |f: &f64| devstatscode::gofmt::float(*f);
    let b_show = |b: &bool| b.to_string();
    let t_show = |t: &DateTime<devstatscode::chrono::FixedOffset>| {
        devstatscode::pg::value::go_time_string(t)
    };
    format!(
        "R T={} | V={} | S={} | B={} | I={} | F={} | L={} | D={}",
        v.go_type_name(),
        v,
        scan::<String>(v, |s| pg::go_quote(s)),
        scan::<Vec<u8>>(v, bytes_show),
        scan::<i64>(v, i_show),
        scan::<f64>(v, f_show),
        scan::<bool>(v, b_show),
        scan_time(v, t_show),
    )
}

fn scan_time(
    v: &DriverValue,
    show: impl Fn(&DateTime<devstatscode::chrono::FixedOffset>) -> String,
) -> String {
    let mut dest: DateTime<devstatscode::chrono::FixedOffset> = Utc::now().fixed_offset();
    match pg::ScanDest::scan_from(&mut dest, v) {
        Ok(()) => format!("ok:{}", show(&dest)),
        Err(e) => format!("err:{e}"),
    }
}

fn scan_error_line(v: &DriverValue, idx: usize, name: &str, inner: &str) -> String {
    let _ = v;
    format!(
        "sql: Scan error on column index {idx}, name {}: {inner}",
        pg::go_quote(name)
    )
}

/// The Rust side of the probe: same protocol as `pgprobe/main.go`.
fn rust_probe(c: &PgConn, script: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    for line in script {
        let parts: Vec<&str> = line.split('\t').collect();
        let q = parts[0];
        let args: Vec<SqlArg> = parts[1..]
            .iter()
            .map(|a| {
                let (kind, v) = a.split_at(2);
                match kind {
                    "i:" => SqlArg::Int(v.parse().unwrap()),
                    "f:" => SqlArg::Float(v.parse().unwrap()),
                    "b:" => SqlArg::Bool(v.parse().unwrap()),
                    "s:" => SqlArg::Str(v.to_string()),
                    "x:" => SqlArg::Bytes(
                        (0..v.len())
                            .step_by(2)
                            .map(|i| u8::from_str_radix(&v[i..i + 2], 16).unwrap())
                            .collect(),
                    ),
                    "t:" => SqlArg::Time(DateTime::parse_from_rfc3339(v).unwrap()),
                    "n:" => SqlArg::Null,
                    _ => panic!("bad probe arg {a}"),
                }
            })
            .collect();
        out.push(format!("Q {q}"));
        let err_line = |e: &PgError| {
            format!(
                "E {} | {} | code={} | name={}",
                e.go_type_name(),
                e,
                e.code(),
                e.name()
            )
        };
        if let Some(sql) = q.strip_prefix('!') {
            match c.exec(sql, &args).and_then(|r| r.rows_affected()) {
                Ok(n) => out.push(format!("X {n}")),
                Err(e) => out.push(err_line(&e)),
            }
            continue;
        }
        let mut rows = match c.query(q, &args) {
            Ok(r) => r,
            Err(e) => {
                out.push(err_line(&e));
                continue;
            }
        };
        let names = rows.column_names();
        out.push(format!("C {}", names.join(" | ")));
        let mut n = 0;
        let mut first: Vec<DriverValue> = Vec::new();
        while rows.next() {
            if n == 0 {
                first = rows.values().to_vec();
            }
            n += 1;
        }
        if let Err(e) = rows.err() {
            out.push(err_line(&e));
            continue;
        }
        for (i, v) in first.iter().enumerate() {
            // The probe's per-destination scans go through `rows.Scan`, whose
            // errors are wrapped with the column index/name.
            let line = rust_probe_row(v);
            let wrapped = line
                .split(" | ")
                .map(|part| match part.split_once("err:") {
                    Some((prefix, inner)) if !inner.starts_with("sql: ") => {
                        format!("{prefix}err:{}", scan_error_line(v, i, &names[i], inner))
                    }
                    _ => part.to_string(),
                })
                .collect::<Vec<_>>()
                .join(" | ");
            out.push(wrapped);
        }
        out.push(format!("N {n}"));
    }
    out
}

/// Run the Go probe and the Rust layer on the same statements and compare
/// every output line.
#[test]
fn go_probe_agrees_with_rust() {
    let Some(db) = TestDb::fresh("probe") else {
        return;
    };
    let Some(probe) = go_probe("pgprobe") else {
        return;
    };
    let mut env = db.env();
    // The probe's `time.Time.String()` includes the local zone; pin it.
    env.push(("TZ", "UTC"));
    let script = PROBE_SCRIPT.join("\n") + "\n";
    let mut inv = Invocation::new().stdin(script.clone());
    for (k, v) in &env {
        inv = inv.env(k, v);
    }
    let go = run(&probe, &inv);
    assert_eq!(go.code(), 0, "probe failed: {}", go.stderr_str());
    let go_stdout = go.stdout_str();
    let go_lines: Vec<String> = go_stdout
        .lines()
        .map(|l| l.trim_end().to_string())
        .collect();

    // Rust side runs the script on a fresh copy of the schema (the Go run
    // created/modified table `t`; reset first so both start from nothing).
    let mut ctx = db.ctx.clone();
    ctx.pg_db = db.name.clone();
    let c = pg::pg_conn(&ctx);
    c.exec("drop table if exists t; drop table if exists t2", &[])
        .unwrap();
    let rust_lines = rust_probe(&c, PROBE_SCRIPT);
    c.close();

    let mut diffs = Vec::new();
    let max = go_lines.len().max(rust_lines.len());
    for i in 0..max {
        let g = go_lines.get(i).map(String::as_str).unwrap_or("<missing>");
        let r = rust_lines.get(i).map(String::as_str).unwrap_or("<missing>");
        if g != r {
            diffs.push(format!("line {}:\n  go:   {g}\n  rust: {r}", i + 1));
        }
    }
    assert!(
        diffs.is_empty(),
        "{} differing lines (of {}):\n{}",
        diffs.len(),
        max,
        diffs.join("\n")
    );
    assert!(go_lines.len() > 60, "probe output suspiciously short");
}

/// `pg_connection_string` + `pg_conn` from a real `PG_*` environment give a
/// working connection (what every binary does first).
#[test]
fn connection_from_environment() {
    let Some(db) = TestDb::fresh("env") else {
        return;
    };
    let ctx = &db.ctx;
    let cs = pg::pg_connection_string(ctx, &ctx.pg_db);
    assert!(cs.contains(&format!("dbname='{}'", db.name)));
    let c = pg::pg_conn(ctx);
    assert_eq!(c.connection_string, cs);
    let mut cur = String::new();
    pg_scan!(c.query_row("select current_database()", &[]), cur).unwrap();
    assert_eq!(cur, db.name);
    // pg_conn_db: another database through the same context
    let mut ctx2 = ctx.clone();
    let c2 = pg::pg_conn_db(&mut ctx2, "postgres");
    assert!(!ctx2.can_reconnect);
    pg_scan!(c2.query_row("select current_database()", &[]), cur).unwrap();
    assert_eq!(cur, "postgres");
    // shared affiliations DB: defaults to the project DB (ctx.affiliations_db
    // empty) — same current_database
    let mut ctx3 = ctx.clone();
    ctx3.affiliations_db = String::new();
    let cs3 = pg::pg_connection_string(&ctx3, &ctx3.pg_db);
    assert_eq!(cs3, cs);
    c2.close();
    c.close();
    let _: BTreeMap<String, String> = BTreeMap::new();
}
