//! `sqlitedb` — Rust port of `cmd/sqlitedb/sqlitedb.go`.
//!
//! Manipulates Grafana's SQLite database (`dashboard` / `dashboard_tag`
//! tables):
//!
//! * `sqlitedb grafana.db` — exports every dashboard as `sqlite/<slug>.json`
//!   (pretty printed, sorted keys);
//! * `sqlitedb grafana.db a.json b.json …` — imports dashboards, matching them
//!   with the database by their `uid`: new ones are inserted, changed ones
//!   updated (title, slug, data, tags) with the previous JSON saved as
//!   `<file>.was` and the original database file backed up as
//!   `grafana.db.<unix nanoseconds>` before the first modification;
//! * `sqlitedb grafana.db uid1,uid2,…` — deletes the dashboards (and their
//!   tags) with those uids; only taken as a deletion when every item is an
//!   integer, otherwise the argument is a JSON file to import.
//!
//! Output, environment handling, fatal conditions and exit codes follow the Go
//! tool; the intended differences are documented in `rust/README.md`
//! (dashboards and tags are processed in sorted order where Go iterates maps
//! randomly; JSON decoding error wording).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::process;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use devstatscode::chrono::{DateTime, Local, Timelike};
use devstatscode::error::go_io_error_string;
use devstatscode::{
    fatal_on_err, fatal_on_error, fatalf, gofmt, io as gio, json, printf, signal, string as gostr,
    time as gotime, Ctx,
};
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{params_from_iter, Connection};
use serde_json::Value;

/// Main dashboard keys: title, uid and tags (Go `dashboard`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Dashboard {
    title: String,
    uid: String,
    tags: Vec<String>,
}

impl Dashboard {
    /// Go `%+v`: `{Title:… UID:… Tags:[a b]}`.
    fn plus_v(&self) -> String {
        format!(
            "{{Title:{} UID:{} Tags:{}}}",
            self.title,
            self.uid,
            go_strings(&self.tags)
        )
    }

    /// Go `%v`: `{… … [a b]}`.
    fn v(&self) -> String {
        format!("{{{} {} {}}}", self.title, self.uid, go_strings(&self.tags))
    }
}

/// Go `%v` of a `[]string`.
fn go_strings(v: &[String]) -> String {
    format!("[{}]", v.join(" "))
}

/// All dashboard data & metadata (Go `dashboardData`).
#[derive(Debug, Default, Clone)]
struct DashboardData {
    dash: Dashboard,
    id: i64,
    title: String,
    slug: String,
    data: String,
    file: String,
    uid: String,
}

impl DashboardData {
    /// Go `String()` — skips the long JSON data.
    fn go_string(&self) -> String {
        format!(
            "{{dash:'{}', id:{}, title:'{}', slug:'{}', data:len:{}, fn:'{}'}}",
            self.dash.plus_v(),
            self.id,
            self.title,
            self.slug,
            self.data.len(),
            self.file
        )
    }
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// jsoniter field lookup: the exact key first, then a case-insensitive match.
fn field<'a>(obj: &'a serde_json::Map<String, Value>, name: &str) -> Option<&'a Value> {
    obj.get(name).or_else(|| {
        obj.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    })
}

fn string_field(obj: &serde_json::Map<String, Value>, name: &str) -> Result<String, String> {
    match field(obj, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) => Ok(s.clone()),
        Some(other) => Err(format!(
            "main.dashboard.{}: expected string, but found {}",
            name,
            json_type_name(other)
        )),
    }
}

/// Decode a dashboard JSON like `jsoniter.Unmarshal` into the Go struct:
/// unknown keys are ignored, `null` leaves the zero value (a `null` tag is an
/// empty string), key matching is case-insensitive and a value of the wrong
/// type is an error.
fn decode_dashboard(bytes: &[u8]) -> Result<Dashboard, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let obj = match value {
        Value::Null => return Ok(Dashboard::default()),
        Value::Object(obj) => obj,
        other => {
            return Err(format!(
                "main.dashboard: ReadObject: expect {{ or n, but found {}",
                json_type_name(&other)
            ))
        }
    };
    let mut dash = Dashboard {
        title: string_field(&obj, "title")?,
        uid: string_field(&obj, "uid")?,
        tags: Vec::new(),
    };
    match field(&obj, "tags") {
        None | Some(Value::Null) => {}
        Some(Value::Array(items)) => {
            for item in items {
                match item {
                    Value::Null => dash.tags.push(String::new()),
                    Value::String(s) => dash.tags.push(s.clone()),
                    other => {
                        return Err(format!(
                            "main.dashboard.tags: []string: expected string, but found {}",
                            json_type_name(other)
                        ))
                    }
                }
            }
        }
        Some(other) => {
            return Err(format!(
                "main.dashboard.tags: decode slice: expect [ or n, but found {}",
                json_type_name(other)
            ))
        }
    }
    Ok(dash)
}

/// A query argument: what is bound and how Go prints it with `%+v`.
enum Arg {
    Int(i64),
    Str(String),
    /// `time.Now()` — bound as go-sqlite3 does (`2006-01-02 15:04:05.999999999-07:00`),
    /// printed like Go's `time.Time.String()` (with the monotonic reading).
    Now(DateTime<Local>),
}

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// go-sqlite3 binds `time.Time` as `t.Format("2006-01-02 15:04:05.999999999-07:00")`.
fn sqlite_timestamp(t: &DateTime<Local>) -> String {
    let mut s = t.format("%Y-%m-%d %H:%M:%S").to_string();
    let nanos = t.nanosecond();
    if nanos != 0 {
        let mut frac = format!("{:09}", nanos);
        while frac.ends_with('0') {
            frac.pop();
        }
        s.push('.');
        s.push_str(&frac);
    }
    s.push_str(&t.format("%:z").to_string());
    s
}

impl Arg {
    fn to_sql(&self) -> SqlValue {
        match self {
            Arg::Int(i) => SqlValue::Integer(*i),
            Arg::Str(s) => SqlValue::Text(s.clone()),
            Arg::Now(t) => SqlValue::Text(sqlite_timestamp(t)),
        }
    }

    fn go_v(&self) -> String {
        match self {
            Arg::Int(i) => i.to_string(),
            Arg::Str(s) => s.clone(),
            Arg::Now(t) => {
                let el = PROCESS_START.get_or_init(Instant::now).elapsed();
                format!(
                    "{} m=+{}.{:09}",
                    gofmt::time(*t),
                    el.as_secs(),
                    el.subsec_nanos()
                )
            }
        }
    }
}

/// `sqliteQueryOut` — echo the query (and its arguments) with `GHA2DB_QOUT`.
fn sqlite_query_out(query: &str, args: &[Arg]) {
    if !args.is_empty() {
        let parts: Vec<String> = args.iter().map(Arg::go_v).collect();
        printf!("[{}]\n", parts.join(" "));
    }
    printf!("{}\n", query);
}

/// Go `database/sql` wording for the rows returned by go-sqlite3.
fn scan_error(idx: usize, name: &str, what: &str) -> String {
    format!(
        "sql: Scan error on column index {}, name \"{}\": {}",
        idx, name, what
    )
}

fn scan_string(row: &rusqlite::Row<'_>, idx: usize, name: &str) -> Result<String, String> {
    match row.get_ref(idx).map_err(|e| e.to_string())? {
        ValueRef::Null => Err(scan_error(
            idx,
            name,
            "converting NULL to string is unsupported",
        )),
        ValueRef::Integer(i) => Ok(i.to_string()),
        ValueRef::Real(f) => Ok(gofmt::float(f)),
        ValueRef::Text(t) | ValueRef::Blob(t) => Ok(String::from_utf8_lossy(t).into_owned()),
    }
}

fn scan_i64(row: &rusqlite::Row<'_>, idx: usize, name: &str) -> Result<i64, String> {
    match row.get_ref(idx).map_err(|e| e.to_string())? {
        ValueRef::Null => Err(scan_error(
            idx,
            name,
            "converting NULL to int is unsupported",
        )),
        ValueRef::Integer(i) => Ok(i),
        ValueRef::Real(f) => Err(scan_error(
            idx,
            name,
            &format!(
                "converting driver.Value type float64 (\"{}\") to a int: invalid syntax",
                gofmt::float(f)
            ),
        )),
        ValueRef::Text(t) | ValueRef::Blob(t) => {
            let s = String::from_utf8_lossy(t);
            s.trim().parse::<i64>().map_err(|_| {
                scan_error(
                    idx,
                    name,
                    &format!(
                        "converting driver.Value type string (\"{}\") to a int: invalid syntax",
                        s
                    ),
                )
            })
        }
    }
}

/// `sqliteQuery` + row iteration: runs `query` and maps every row.
fn sqlite_query<T>(
    db: &Connection,
    ctx: &Ctx,
    query: &str,
    args: &[Arg],
    mut f: impl FnMut(&rusqlite::Row<'_>) -> Result<T, String>,
) -> Vec<T> {
    if ctx.q_out {
        sqlite_query_out(query, args);
    }
    let mut stmt = fatal_on_err(db.prepare(query));
    let params: Vec<SqlValue> = args.iter().map(Arg::to_sql).collect();
    let mut rows = fatal_on_err(stmt.query(params_from_iter(params)));
    let mut out = Vec::new();
    loop {
        match rows.next() {
            Ok(Some(row)) => out.push(fatal_on_err(f(row))),
            Ok(None) => break,
            Err(e) => fatal_on_error(e),
        }
    }
    out
}

/// `sqliteExec` — execute a statement with eventual logging output.
fn sqlite_exec(db: &Connection, ctx: &Ctx, exec: &str, args: &[Arg]) {
    if ctx.q_out {
        sqlite_query_out(exec, args);
    }
    let params: Vec<SqlValue> = args.iter().map(Arg::to_sql).collect();
    fatal_on_err(db.execute(exec, params_from_iter(params)));
}

/// `sql.Open("sqlite3", dbFile)` — go-sqlite3 opens (and creates) the file
/// lazily on the first query; the same errors surface here.
fn open_db(db_file: &str) -> Connection {
    match Connection::open(db_file) {
        Ok(db) => db,
        Err(e) => fatal_on_error(go_sqlite3_open_error(db_file, &e)),
    }
}

/// Render a failed open like go-sqlite3: for `SQLITE_CANTOPEN` it appends the
/// OS error (`sqlite3_system_errno`), e.g. `unable to open database file: is a
/// directory`, whereas rusqlite appends the path. The handle is already closed
/// here, so the errno is recovered by replaying SQLite's `open(2)` sequence:
/// `O_RDWR|O_CREAT` first, then read-only unless the path is a directory.
fn go_sqlite3_open_error(path: &str, e: &rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqliteFailure(ffi_err, _)
            if ffi_err.code == rusqlite::ErrorCode::CannotOpen =>
        {
            let msg = "unable to open database file";
            let rw = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path);
            let os_err = match rw {
                Ok(_) => None,
                Err(err) if err.kind() == ErrorKind::IsADirectory => Some(err),
                Err(_) => OpenOptions::new().read(true).open(path).err(),
            };
            match os_err {
                Some(err) => format!("{msg}: {}", go_io_error_string(&err)),
                None => msg.to_string(),
            }
        }
        _ => e.to_string(),
    }
}

/// `updateTags` — make the JSON and SQLite tags match; returns whether an
/// update was needed.
fn update_tags(db: &Connection, ctx: &Ctx, did: i64, json_tags: &mut [String], info: &str) -> bool {
    let db_tags: Vec<String> = sqlite_query(
        db,
        ctx,
        "select term from dashboard_tag where dashboard_id = ? order by term asc",
        &[Arg::Int(did)],
        |row| scan_string(row, 0, "term"),
    );
    json_tags.sort();
    let s_json_tags = json_tags.join(",");
    let s_db_tags = db_tags.join(",");
    if s_json_tags == s_db_tags {
        return false;
    }
    let json_set: BTreeSet<&String> = json_tags.iter().collect();
    let db_set: BTreeSet<&String> = db_tags.iter().collect();
    let all: BTreeSet<&String> = json_set.union(&db_set).copied().collect();
    let mut n_i = 0;
    let mut n_d = 0;
    for tag in all {
        let j = json_set.contains(tag);
        let d = db_set.contains(tag);
        if j && !d {
            sqlite_exec(
                db,
                ctx,
                "insert into dashboard_tag(dashboard_id, term) values(?, ?)",
                &[Arg::Int(did), Arg::Str(tag.clone())],
            );
            if ctx.debug > 0 {
                printf!(
                    "Updating dashboard '{}' id: {}, '{}' -> '{}', inserted '{}' tag\n",
                    info,
                    did,
                    s_db_tags,
                    s_json_tags,
                    tag
                );
            }
            n_i += 1;
        }
        if !j && d {
            sqlite_exec(
                db,
                ctx,
                "delete from dashboard_tag where dashboard_id = ? and term = ?",
                &[Arg::Int(did), Arg::Str(tag.clone())],
            );
            if ctx.debug > 0 {
                printf!(
                    "Updating dashboard '{}' id: {}, '{}' -> '{}', deleted '{}' tag\n",
                    info,
                    did,
                    s_db_tags,
                    s_json_tags,
                    tag
                );
            }
            n_d += 1;
        }
    }
    printf!(
        "Updated dashboard tags '{}' id: {}, '{}' -> '{}', added: {}, removed: {}\n",
        info,
        did,
        s_db_tags,
        s_json_tags,
        n_i,
        n_d
    );
    true
}

/// `deleteUids` — delete all dashboards with the given uids.
fn delete_uids(ctx: &Ctx, db_file: &str, uids: &[String]) {
    let db = open_db(db_file);
    for uid in uids {
        let ids = sqlite_query(
            &db,
            ctx,
            "select id from dashboard where uid = ?",
            &[Arg::Str(uid.clone())],
            |row| scan_i64(row, 0, "id"),
        );
        let id = ids.last().copied().unwrap_or(-1);
        if id < 0 {
            printf!("Dashboard with uid={} not found, skipping\n", uid);
            continue;
        }
        sqlite_exec(
            &db,
            ctx,
            "delete from dashboard_tag where dashboard_id = ?",
            &[Arg::Int(id)],
        );
        sqlite_exec(
            &db,
            ctx,
            "delete from dashboard where id = ?",
            &[Arg::Int(id)],
        );
        printf!("Deleted dashboard with uid {}\n", uid);
    }
}

/// `exportJsons` — dump all dashboards as `sqlite/<slug>.json`.
fn export_jsons(ctx: &Ctx, db_file: &str) {
    let db = open_db(db_file);
    let rows = sqlite_query(
        &db,
        ctx,
        "select slug, title, data from dashboard",
        &[],
        |row| {
            Ok((
                scan_string(row, 0, "slug")?,
                scan_string(row, 1, "title")?,
                scan_string(row, 2, "data")?,
            ))
        },
    );
    for (slug, title, data) in rows {
        let file = format!("sqlite/{}.json", slug);
        json::write_file_0644(&file, &json::pretty_print_json(data.as_bytes()));
        printf!("Written '{}' to {}\n", title, file);
    }
}

/// `insertDashboard` — insert a new dashboard (and its tags).
fn insert_dashboard(db: &Connection, ctx: &Ctx, dd: &mut DashboardData) {
    dd.uid = dd.dash.uid.clone();
    dd.title = dd.dash.title.clone();
    dd.slug = gostr::slugify(&dd.title);
    sqlite_exec(
        db,
        ctx,
        "insert into dashboard(version, slug, title, data, \
         org_id, created, updated, created_by, updated_by, \
         gnet_id, plugin_id, folder_id, is_folder, has_acl, uid) \
         values(1, ?, ?, ?, 1, ?, ?, 1, 1, 0, '', 0, 0, 0, ?)",
        &[
            Arg::Str(dd.slug.clone()),
            Arg::Str(dd.title.clone()),
            Arg::Str(dd.data.clone()),
            Arg::Now(Local::now()),
            Arg::Now(Local::now()),
            Arg::Str(dd.uid.clone()),
        ],
    );
    let ids = sqlite_query(db, ctx, "select max(id) from dashboard", &[], |row| {
        scan_i64(row, 0, "max(id)")
    });
    if let Some(id) = ids.last() {
        dd.id = *id;
    }
    printf!(
        "Inserted dashboard: id={} (uid={}, title={}, slug={})\n",
        dd.id,
        dd.uid,
        dd.title,
        dd.slug
    );
    let info = format!("{} {}", dd.dash.uid, dd.dash.title);
    let mut tags = dd.dash.tags.clone();
    let updated = update_tags(db, ctx, dd.id, &mut tags, &info);
    dd.dash.tags = tags;
    if !dd.dash.tags.is_empty() && !updated {
        fatalf!("should add new tags for {}", dd.go_string());
    }
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// `importJsons` — update the database from the given dashboard JSONs
/// (matched by uid), inserting the unknown ones.
fn import_jsons(ctx: &Ctx, db_file: &str, jsons: &[String]) {
    let contents = fatal_on_err(gio::read_file(ctx, db_file));
    let mut backed_up = false;
    let backup = |backed_up: &mut bool| {
        if *backed_up {
            return;
        }
        let bfn = format!("{}.{}", db_file, unix_nanos());
        json::write_file_0644(&bfn, &contents);
        printf!("Original db file backed up as' {}'\n", bfn);
        *backed_up = true;
    };

    let db = open_db(db_file);

    // Load and parse all dashboards JSONs: uid -> sqlite dashboard data
    let mut db_map: BTreeMap<String, DashboardData> = BTreeMap::new();
    let rows = sqlite_query(
        &db,
        ctx,
        "select id, data, title, slug, uid from dashboard",
        &[],
        |row| {
            Ok(DashboardData {
                id: scan_i64(row, 0, "id")?,
                data: scan_string(row, 1, "data")?,
                title: scan_string(row, 2, "title")?,
                slug: scan_string(row, 3, "slug")?,
                uid: scan_string(row, 4, "uid")?,
                ..Default::default()
            })
        },
    );
    for mut dd in rows {
        dd.dash = fatal_on_err(decode_dashboard(dd.data.as_bytes()));
        if dd.title != dd.dash.title {
            printf!(
                "SQLite internal inconsistency (title): {} != {}: {}, using value from dashboard table, not from JSON\n",
                dd.title,
                dd.dash.title,
                dd.go_string()
            );
            dd.dash.title = dd.title.clone();
        }
        if dd.uid != dd.dash.uid {
            printf!(
                "SQLite internal inconsistency (uid): {} != {}: {}, using value from dashboard table, not from JSON\n",
                dd.uid,
                dd.dash.uid,
                dd.go_string()
            );
            dd.dash.uid = dd.uid.clone();
        }
        dd.data =
            String::from_utf8_lossy(&json::pretty_print_json(dd.data.as_bytes())).into_owned();
        dd.file = format!("*{}.json*", dd.slug);
        db_map.insert(dd.dash.uid.clone(), dd);
    }
    let n_db_map = db_map.len();

    // Now load & parse JSON arguments
    let mut json_map: BTreeMap<String, DashboardData> = BTreeMap::new();
    let mut n_ins = 0;
    for j in jsons {
        printf!("Processing '{}'\n", j);
        let bytes = fatal_on_err(gio::read_file(ctx, j));
        let mut dd = DashboardData {
            dash: fatal_on_err(decode_dashboard(&bytes)),
            ..Default::default()
        };
        let Some(db_dash) = db_map.get(&dd.dash.uid) else {
            dd.data = String::from_utf8_lossy(&json::pretty_print_json(&bytes)).into_owned();
            dd.file = j.clone();
            insert_dashboard(&db, ctx, &mut dd);
            backup(&mut backed_up);
            n_ins += 1;
            continue;
        };
        if let Some(json_dash) = json_map.get(&dd.dash.uid) {
            fatalf!(
                "{}: duplicate json uid, attempt to import {}, collision with {}",
                j,
                dd.dash.v(),
                json_dash.dash.v()
            );
        }
        dd.data = String::from_utf8_lossy(&json::pretty_print_json(&bytes)).into_owned();
        dd.id = db_dash.id;
        dd.uid = dd.dash.uid.clone();
        dd.title = dd.dash.title.clone();
        dd.slug = gostr::slugify(&dd.title);
        dd.file = j.clone();
        json_map.insert(dd.dash.uid.clone(), dd);
    }
    let n_json_map = json_map.len();

    // Now do updates
    let mut n_imp = 0;
    for (uid, dd) in json_map.iter_mut() {
        let dd_was = &db_map[uid];
        if ctx.debug > 1 {
            printf!("\n{}\n{}\n\n", dd.go_string(), dd_was.go_string());
        }
        // Update/check tags
        let info = format!("{} {}", dd.dash.uid, dd.dash.title);
        let mut tags = dd.dash.tags.clone();
        let updated = update_tags(&db, ctx, dd.id, &mut tags, &info);
        dd.dash.tags = tags;

        // Check if we actually need to update anything
        if dd_was.dash.title == dd.dash.title && dd_was.slug == dd.slug && dd_was.data == dd.data {
            if updated {
                backup(&mut backed_up);
                n_imp += 1;
            }
            continue;
        }
        // Update JSON inside database
        sqlite_exec(
            &db,
            ctx,
            "update dashboard set title = ?, slug = ?, data = ? where id = ?",
            &[
                Arg::Str(dd.dash.title.clone()),
                Arg::Str(dd.slug.clone()),
                Arg::Str(dd.data.clone()),
                Arg::Int(dd.id),
            ],
        );

        // Info
        if ctx.debug > 0 {
            printf!(
                "{}: updated uid: {}: tags updated: {}\nnew: {}\nold: {}\n",
                dd.file,
                uid,
                updated,
                dd.go_string(),
                dd_was.go_string()
            );
        } else {
            printf!(
                "{}: updated dashboard: uid: {} title: '{}' -> '{}', slug: '{}' -> '{}', tags: {}:{} (data {} -> {} bytes)\n",
                dd.file,
                uid,
                dd_was.dash.title,
                dd.dash.title,
                dd_was.slug,
                dd.slug,
                updated,
                go_strings(&dd.dash.tags),
                dd_was.data.len(),
                dd.data.len()
            );
        }

        // And save JSON from DB
        json::write_file_0644(&format!("{}.was", dd.file), dd_was.data.as_bytes());

        // Something changed, backup original db file
        backup(&mut backed_up);
        n_imp += 1;
    }
    printf!(
        "SQLite DB has {} dashboards, there were {} JSONs to import, updated {}, created {}\n",
        n_db_map,
        n_json_map + n_ins,
        n_imp,
        n_ins
    );
}

/// `len(os.Args) == 3` and every comma separated item is an integer
/// (`strconv.Atoi`) → the argument is a list of uids to delete.
fn uids_to_delete(arg: &str) -> Option<Vec<String>> {
    let mut uids = Vec::new();
    for item in arg.split(',') {
        item.parse::<i64>().ok()?;
        uids.push(item.to_string());
    }
    Some(uids)
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    PROCESS_START.get_or_init(|| dt_start);
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    let args: Vec<String> = std::env::args_os()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    if args.len() < 2 {
        printf!("Required args: grafana.db file name and list(*) of jsons to import.\n");
        printf!("If only db file name given, it will output all dashboards to jsons\n");
        printf!("It will import JSONs by matching their internal uid with SQLite database\n");
        printf!("If DB name given and single argument with comman separated uids - dashboards with those uids will be removed\n");
        process::exit(1);
    }
    let uids = if args.len() == 3 {
        uids_to_delete(&args[2])
    } else {
        None
    };
    match uids {
        Some(uids) => delete_uids(&ctx, &args[1], &uids),
        None => {
            if args.len() > 2 {
                import_jsons(&ctx, &args[1], &args[2..]);
            } else {
                export_jsons(&ctx, &args[1]);
            }
        }
    }
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use devstatscode::chrono::TimeZone;

    #[test]
    fn decode_dashboard_like_jsoniter() {
        let d = decode_dashboard(
            br#"{"title":"PR Comments","uid":"17","tags":["dashboard","prometheus"],"panels":[{"id":1}]}"#,
        )
        .unwrap();
        assert_eq!(
            d,
            Dashboard {
                title: "PR Comments".into(),
                uid: "17".into(),
                tags: vec!["dashboard".into(), "prometheus".into()],
            }
        );
        // case-insensitive keys, null tags / values, missing keys
        let d = decode_dashboard(br#"{"Title":"T","UID":null,"Tags":["a",null]}"#).unwrap();
        assert_eq!(d.title, "T");
        assert_eq!(d.uid, "");
        assert_eq!(d.tags, vec!["a".to_string(), String::new()]);
        assert_eq!(decode_dashboard(b"{}").unwrap(), Dashboard::default());
        assert_eq!(decode_dashboard(b"null").unwrap(), Dashboard::default());
        assert_eq!(
            decode_dashboard(br#"{"tags":null}"#).unwrap(),
            Dashboard::default()
        );
        // type errors
        assert!(decode_dashboard(br#"{"title":1}"#).is_err());
        assert!(decode_dashboard(br#"{"uid":["1"]}"#).is_err());
        assert!(decode_dashboard(br#"{"tags":"a"}"#).is_err());
        assert!(decode_dashboard(br#"{"tags":[1]}"#).is_err());
        assert!(decode_dashboard(b"[]").is_err());
        assert!(decode_dashboard(b"").is_err());
        assert!(decode_dashboard(b"{} x").is_err());
    }

    #[test]
    fn go_formatting_of_dashboard_data() {
        let dd = DashboardData {
            dash: Dashboard {
                title: "A B".into(),
                uid: "7".into(),
                tags: vec!["x".into(), "y".into()],
            },
            id: 3,
            title: "A B".into(),
            slug: "a-b".into(),
            data: "{\n}".into(),
            file: "*a-b.json*".into(),
            uid: "7".into(),
        };
        assert_eq!(
            dd.go_string(),
            "{dash:'{Title:A B UID:7 Tags:[x y]}', id:3, title:'A B', slug:'a-b', data:len:3, fn:'*a-b.json*'}"
        );
        assert_eq!(dd.dash.v(), "{A B 7 [x y]}");
        assert_eq!(Dashboard::default().plus_v(), "{Title: UID: Tags:[]}");
        assert_eq!(go_strings(&[]), "[]");
    }

    #[test]
    fn sqlite_timestamps_like_go_sqlite3() {
        let t = Local.with_ymd_and_hms(2026, 9, 8, 14, 25, 5).unwrap();
        let s = sqlite_timestamp(&t);
        assert!(s.starts_with("2026-09-08 14:25:05"), "{s}");
        assert!(!s.contains('.'), "{s}");
        let t = t.with_nanosecond(700_421_300).unwrap();
        let s = sqlite_timestamp(&t);
        assert!(s.starts_with("2026-09-08 14:25:05.7004213"), "{s}");
        let off = &s[s.len() - 6..];
        assert!(
            off.starts_with('+') || off.starts_with('-'),
            "offset with a colon expected: {s}"
        );
        assert_eq!(&off[3..4], ":");
    }

    #[test]
    fn delete_argument_detection() {
        assert_eq!(
            uids_to_delete("1,2,3"),
            Some(vec!["1".to_string(), "2".to_string(), "3".to_string()])
        );
        assert_eq!(uids_to_delete("+5"), Some(vec!["+5".to_string()]));
        assert_eq!(uids_to_delete("-3"), Some(vec!["-3".to_string()]));
        assert_eq!(uids_to_delete("1,,2"), None);
        assert_eq!(uids_to_delete("1,x"), None);
        assert_eq!(uids_to_delete("a.json"), None);
        assert_eq!(uids_to_delete(""), None);
        assert_eq!(uids_to_delete(" 1"), None);
        assert_eq!(uids_to_delete("99999999999999999999"), None);
    }

    #[test]
    fn query_out_arguments_format() {
        assert_eq!(Arg::Int(7).go_v(), "7");
        assert_eq!(Arg::Str("a b".into()).go_v(), "a b");
        let now = Arg::Now(Local::now()).go_v();
        assert!(now.contains(" m=+"), "{now}");
    }
}
