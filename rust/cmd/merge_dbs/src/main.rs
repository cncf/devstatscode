//! `merge_dbs` — merge several DevStats project databases into one (shared)
//! database; Rust port of `cmd/merge_dbs/merge_dbs.go`.
//!
//! Environment:
//! - `GHA2DB_INPUT_DBS` — comma separated input databases (order matters:
//!   later databases only add rows that are not there yet); the single value
//!   `-all-` expands to every enabled `projects.yaml` project whose
//!   `shared_db` is the output database.
//! - `GHA2DB_OUTPUT_DB` — the database merged into (required).
//! - `SKIP_DBS`, `IGNORE_NO_DB` — only with `-all-`: databases to leave out /
//!   skip input databases that do not exist.
//! - `MERGE_DT_FROM` (alias `MERGE_DT_DROM`) — only copy rows created at or
//!   after this date (tables without a date mapping are copied fully).
//! - `ONLY_TABLES`, `SKIP_TABLES` — comma separated table filters.
//! - `USE_BATCH`, `BATCH_SIZE`, `PARALLEL` — multi-row inserts (`on conflict
//!   do nothing`), rows per insert (2..1000, additionally capped by the 65535
//!   bind parameters limit) and the number of tables processed concurrently
//!   (1..16).
//!
//! Every table is copied in two passes (rows with positive ids first, then
//! the rest for `gha_actors`, `gha_events`, `gha_issues`, `gha_labels` and
//! `gha_payloads`; a single pass for the other tables). Rows already present
//! in the output database are counted as collisions.

use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use devstatscode::chrono::{DateTime, TimeZone, Utc};
use devstatscode::pg::{
    self, fatal_on_pg_err, fatal_on_pg_error, go_quote, ExecResult, PgConn, PgError, SqlArg,
};
use devstatscode::time as gotime;
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    fatal_on_err, fatal_on_error, fatalf, gofmt, io, printf, projects, signal, Ctx,
};

/// `GHA2DB_INPUT_DBS` value meaning "all projects sharing the output DB".
const ALL_INPUT_DBS: &str = "-all-";

/// Maximum number of bind parameters of a single PostgreSQL statement (the
/// wire protocol stores the parameter count in a 16-bit field).
const MAX_PARAMS: usize = 65535;

/// Progress info period (Go `time.Duration(10)*time.Second`).
const PROGRESS_PERIOD: Duration = Duration::from_secs(10);

/// One merged table: name, 1st pass condition, 2nd pass condition
/// (`""` = all rows, `"-"` = pass skipped).
type TableData = (&'static str, &'static str, &'static str);

/// Go `tableData` (without `gha_actors`, prepended in legacy mode only).
const TABLE_DATA: &[TableData] = &[
    ("gha_assets", "", "-"),
    ("gha_branches", "", "-"),
    ("gha_comments", "", "-"),
    ("gha_reviews", "", "-"),
    ("gha_commits", "", "-"),
    ("gha_commits_files", "", "-"),
    ("gha_commits_roles", "", "-"),
    ("gha_events", "id > 0", "id <= 0"),
    ("gha_forkees", "", "-"),
    ("gha_issues", "id > 0", "id <= 0"),
    ("gha_issues_assignees", "", "-"),
    ("gha_issues_events_labels", "", "-"),
    ("gha_issues_labels", "", "-"),
    ("gha_issues_pull_requests", "", "-"),
    ("gha_labels", "id > 0", "id <= 0"),
    ("gha_milestones", "", "-"),
    ("gha_orgs", "", "-"),
    ("gha_pages", "", "-"),
    ("gha_payloads", "event_id > 0", "event_id <= 0"),
    ("gha_pull_requests", "", "-"),
    ("gha_pull_requests_assignees", "", "-"),
    ("gha_pull_requests_requested_reviewers", "", "-"),
    ("gha_releases", "", "-"),
    ("gha_releases_assets", "", "-"),
    ("gha_repos", "", "-"),
    ("gha_repo_groups", "", "-"),
    ("gha_repos_langs", "", "-"),
    ("gha_skip_commits", "", "-"),
    ("gha_teams", "", "-"),
    ("gha_teams_repositories", "", "-"),
    ("gha_texts", "", "-"),
];

/// The table processed first when there is no shared affiliations DB.
const ACTORS_TABLE: TableData = ("gha_actors", "id > 0", "id <= 0");

/// Go `projectDBsForSharedDB`: the databases of the enabled `projects.yaml`
/// projects whose `shared_db` is the output database, ordered by project
/// `order`, name and database, without duplicates.
fn project_dbs_for_shared_db(ctx: &Ctx) -> Vec<String> {
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    // `ioutil.ReadFile` — no `/shared/` fallback.
    let data = fatal_on_err(io::read_file_raw(format!(
        "{data_prefix}{}",
        ctx.projects_yaml
    )));
    let all: projects::AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };

    let mut project_dbs: Vec<(i64, &str, String)> = Vec::new();
    for (name, proj) in &all.projects {
        if projects::is_project_disabled(ctx, name, proj.disabled) {
            continue;
        }
        if proj.shared_db.trim() != ctx.output_db {
            continue;
        }
        let db = proj.pdb.trim();
        if db.is_empty() || db == ctx.output_db {
            continue;
        }
        project_dbs.push((proj.order, name.as_str(), db.to_string()));
    }
    project_dbs.sort();

    let mut seen: HashSet<&str> = HashSet::new();
    let mut dbs = Vec::new();
    for (_, _, db) in &project_dbs {
        if seen.insert(db.as_str()) {
            dbs.push(db.clone());
        }
    }
    dbs
}

/// Go `envFlag`: `1`, `t`, `true`, `y`, `yes` (any case, trimmed).
fn env_flag(name: &str) -> bool {
    let v = std::env::var(name).unwrap_or_default();
    matches!(
        v.trim().to_lowercase().as_str(),
        "1" | "t" | "true" | "y" | "yes"
    )
}

/// Go `parseBatchSize`: `BATCH_SIZE` (default 1000) clamped to `[2, 1000]`.
fn parse_batch_size() -> usize {
    let raw = std::env::var("BATCH_SIZE").unwrap_or_default();
    let value = raw.trim();
    if value.is_empty() {
        return 1000;
    }
    let mut n = match gotime::parse_go_int(value) {
        Ok(n) => n,
        Err(e) => fatalf(format_args!(
            "invalid BATCH_SIZE={}: {}",
            go_quote(value),
            e
        )),
    };
    if n < 2 {
        printf!("merge_dbs: BATCH_SIZE={} is below minimum, using 2\n", n);
        n = 2;
    }
    if n > 1000 {
        printf!("merge_dbs: BATCH_SIZE={} is above maximum, using 1000\n", n);
        n = 1000;
    }
    n as usize
}

/// Go `parseParallel`: `PARALLEL` (default 1) clamped to `[1, 16]`.
fn parse_parallel() -> usize {
    let raw = std::env::var("PARALLEL").unwrap_or_default();
    let value = raw.trim();
    if value.is_empty() {
        return 1;
    }
    let mut n = match gotime::parse_go_int(value) {
        Ok(n) => n,
        Err(e) => fatalf(format_args!("invalid PARALLEL={}: {}", go_quote(value), e)),
    };
    if n < 1 {
        printf!("merge_dbs: PARALLEL={} is below minimum, using 1\n", n);
        n = 1;
    }
    if n > 16 {
        printf!("merge_dbs: PARALLEL={} is above maximum, using 16\n", n);
        n = 16;
    }
    n as usize
}

/// Go `batchValues`: `values ($1,..,$nCols),($nCols+1,..),..`.
fn batch_values(n_rows: usize, n_cols: usize) -> String {
    let mut sb = String::from("values ");
    let mut param = 1usize;
    for r in 0..n_rows {
        if r > 0 {
            sb.push(',');
        }
        sb.push('(');
        for c in 0..n_cols {
            if c > 0 {
                sb.push(',');
            }
            sb.push('$');
            sb.push_str(&param.to_string());
            param += 1;
        }
        sb.push(')');
    }
    sb
}

/// One element of a Go time layout, as far as the layouts used here go.
#[derive(Clone, Copy, PartialEq)]
enum LayoutItem {
    /// `2006` — exactly 4 digits.
    Year,
    /// `01` — exactly 2 digits, 1..=12.
    Month,
    /// `02` — exactly 2 digits, 1..=days in month.
    Day,
    /// `15` — 1 or 2 digits, 0..=23.
    Hour,
    /// `04` — exactly 2 digits, 0..=59.
    Minute,
    /// `05` — exactly 2 digits, 0..=59; the input may carry a fractional
    /// part (`.123` / `,123`) even though the layout has none.
    Second,
    /// A run of spaces in the layout matches one or more spaces.
    Space,
    /// Any other byte matches itself.
    Lit(u8),
}

/// The `MERGE_DT_FROM` layouts tried in order (Go `time.Parse` layouts).
const DT_LAYOUTS: &[&[LayoutItem]] = &[
    // "2006-01-02T15:04:05Z"
    &[
        LayoutItem::Year,
        LayoutItem::Lit(b'-'),
        LayoutItem::Month,
        LayoutItem::Lit(b'-'),
        LayoutItem::Day,
        LayoutItem::Lit(b'T'),
        LayoutItem::Hour,
        LayoutItem::Lit(b':'),
        LayoutItem::Minute,
        LayoutItem::Lit(b':'),
        LayoutItem::Second,
        LayoutItem::Lit(b'Z'),
    ],
    // "2006-01-02 15:04:05"
    &[
        LayoutItem::Year,
        LayoutItem::Lit(b'-'),
        LayoutItem::Month,
        LayoutItem::Lit(b'-'),
        LayoutItem::Day,
        LayoutItem::Space,
        LayoutItem::Hour,
        LayoutItem::Lit(b':'),
        LayoutItem::Minute,
        LayoutItem::Lit(b':'),
        LayoutItem::Second,
    ],
    // "2006-01-02 15:04"
    &[
        LayoutItem::Year,
        LayoutItem::Lit(b'-'),
        LayoutItem::Month,
        LayoutItem::Lit(b'-'),
        LayoutItem::Day,
        LayoutItem::Space,
        LayoutItem::Hour,
        LayoutItem::Lit(b':'),
        LayoutItem::Minute,
    ],
    // "2006-01-02 15"
    &[
        LayoutItem::Year,
        LayoutItem::Lit(b'-'),
        LayoutItem::Month,
        LayoutItem::Lit(b'-'),
        LayoutItem::Day,
        LayoutItem::Space,
        LayoutItem::Hour,
    ],
    // "2006-01-02"
    &[
        LayoutItem::Year,
        LayoutItem::Lit(b'-'),
        LayoutItem::Month,
        LayoutItem::Lit(b'-'),
        LayoutItem::Day,
    ],
];

/// Go `time.Parse(layout, value)` for the layouts of [`DT_LAYOUTS`]: `None`
/// when Go would return an error (wrong digits count, out of range fields,
/// trailing text, ...). Like Go, the result is in UTC.
fn go_time_parse(layout: &[LayoutItem], value: &str) -> Option<DateTime<Utc>> {
    let b = value.as_bytes();
    let mut pos = 0usize;
    // Go `getnum`: 2 digits (`fixed`) or 1..2 digits.
    let getnum = |pos: &mut usize, fixed: bool| -> Option<u32> {
        let d0 = *b.get(*pos)?;
        if !d0.is_ascii_digit() {
            return None;
        }
        match b.get(*pos + 1) {
            Some(d1) if d1.is_ascii_digit() => {
                *pos += 2;
                Some(u32::from(d0 - b'0') * 10 + u32::from(d1 - b'0'))
            }
            _ if fixed => None,
            _ => {
                *pos += 1;
                Some(u32::from(d0 - b'0'))
            }
        }
    };
    let (mut year, mut month, mut day) = (0i32, 1u32, 1u32);
    let (mut hour, mut min, mut sec, mut nsec) = (0u32, 0u32, 0u32, 0u32);
    for item in layout {
        match item {
            LayoutItem::Year => {
                let digits = b.get(pos..pos + 4)?;
                if !digits.iter().all(u8::is_ascii_digit) {
                    return None;
                }
                year = std::str::from_utf8(digits).ok()?.parse().ok()?;
                pos += 4;
            }
            LayoutItem::Month => {
                month = getnum(&mut pos, true)?;
                if !(1..=12).contains(&month) {
                    return None;
                }
            }
            LayoutItem::Day => {
                day = getnum(&mut pos, true)?;
            }
            LayoutItem::Hour => {
                hour = getnum(&mut pos, false)?;
                if hour >= 24 {
                    return None;
                }
            }
            LayoutItem::Minute => {
                min = getnum(&mut pos, true)?;
                if min >= 60 {
                    return None;
                }
            }
            LayoutItem::Second => {
                sec = getnum(&mut pos, true)?;
                if sec >= 60 {
                    return None;
                }
                // Fractional second present in the input but not in the
                // layout: consume every digit, keep at most 9 of them.
                if b.len() >= pos + 2
                    && (b[pos] == b'.' || b[pos] == b',')
                    && b[pos + 1].is_ascii_digit()
                {
                    let mut n = pos + 2;
                    while n < b.len() && b[n].is_ascii_digit() {
                        n += 1;
                    }
                    let digits = &b[pos + 1..(pos + 10).min(n)];
                    let mut ns: u32 = std::str::from_utf8(digits).ok()?.parse().ok()?;
                    for _ in digits.len()..9 {
                        ns *= 10;
                    }
                    nsec = ns;
                    pos = n;
                }
            }
            LayoutItem::Space => {
                if let Some(c) = b.get(pos) {
                    if *c != b' ' {
                        return None;
                    }
                }
                while b.get(pos) == Some(&b' ') {
                    pos += 1;
                }
            }
            LayoutItem::Lit(c) => {
                if b.get(pos) != Some(c) {
                    return None;
                }
                pos += 1;
            }
        }
    }
    if pos != b.len() {
        // Go: "extra text".
        return None;
    }
    // Go validates the day of the month after parsing ("day out of range").
    let date = devstatscode::chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    let time = devstatscode::chrono::NaiveTime::from_hms_nano_opt(hour, min, sec, nsec)?;
    Some(Utc.from_utc_datetime(&date.and_time(time)))
}

/// Go `parseMergeDtFrom`: `MERGE_DT_FROM` / `MERGE_DT_DROM` (both accepted,
/// must match when both set), `None` when unset.
fn parse_merge_dt_from() -> Option<DateTime<Utc>> {
    let from_raw = std::env::var("MERGE_DT_FROM").unwrap_or_default();
    let drom_raw = std::env::var("MERGE_DT_DROM").unwrap_or_default();
    let dt_from = from_raw.trim();
    let dt_drom = drom_raw.trim();
    if !dt_from.is_empty() && !dt_drom.is_empty() && dt_from != dt_drom {
        fatalf(format_args!(
            "MERGE_DT_FROM and MERGE_DT_DROM are both set but differ: {} != {}",
            go_quote(dt_from),
            go_quote(dt_drom)
        ));
    }
    let dt_from = if dt_from.is_empty() { dt_drom } else { dt_from };
    if dt_from.is_empty() {
        return None;
    }
    for layout in DT_LAYOUTS {
        if let Some(tm) = go_time_parse(layout, dt_from) {
            return Some(tm);
        }
    }
    fatalf(format_args!(
        "MERGE_DT_FROM/MERGE_DT_DROM must be YYYY-MM-DD or parseable timestamp, got {}",
        go_quote(dt_from)
    ))
}

/// Go `addWhereCondition`.
fn add_where_condition(query_root: &str, condition: &str) -> String {
    if condition.is_empty() {
        return query_root.to_string();
    }
    if query_root.contains(" where ") {
        return format!("{query_root} and {condition}");
    }
    format!("{query_root} where {condition}")
}

/// Go `mergeDateCondition`: the `MERGE_DT_FROM` filter of a table (`""` when
/// the table has no creation date column mapping).
fn merge_date_condition(table: &str) -> &'static str {
    match table {
        "gha_assets" | "gha_branches" | "gha_comments" | "gha_commits" | "gha_commits_roles"
        | "gha_forkees" | "gha_issues" | "gha_issues_labels" | "gha_milestones" | "gha_pages"
        | "gha_payloads" | "gha_pull_requests" | "gha_releases" | "gha_teams" => {
            "dup_created_at >= $1"
        }
        "gha_events" | "gha_issues_events_labels" | "gha_issues_pull_requests" | "gha_texts" => {
            "created_at >= $1"
        }
        "gha_commits_files" | "gha_repos_langs" | "gha_skip_commits" => "dt >= $1",
        "gha_issues_assignees"
        | "gha_pull_requests_assignees"
        | "gha_pull_requests_requested_reviewers"
        | "gha_releases_assets"
        | "gha_teams_repositories" => {
            "event_id in (select id from gha_events where created_at >= $1)"
        }
        _ => "",
    }
}

/// Go `parseTableList`: comma separated, trimmed, empty items dropped.
fn parse_table_list(env_name: &str) -> BTreeSet<String> {
    let raw = std::env::var(env_name).unwrap_or_default();
    raw.trim()
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

/// Go `resolveInputDBs`: trims `GHA2DB_INPUT_DBS`, expands `-all-` (applying
/// `SKIP_DBS`); returns whether the `-all-` mode is active.
fn resolve_input_dbs(ctx: &mut Ctx) -> bool {
    ctx.input_dbs = ctx
        .input_dbs
        .iter()
        .map(|db| db.trim().to_string())
        .filter(|db| !db.is_empty())
        .collect();

    if ctx.input_dbs.iter().any(|db| db == ALL_INPUT_DBS) && ctx.input_dbs.len() != 1 {
        fatalf(format_args!(
            "{} must be used alone in GHA2DB_INPUT_DBS, got {}",
            ALL_INPUT_DBS,
            gofmt::slice(&ctx.input_dbs)
        ));
    }

    let all_mode = ctx.input_dbs.len() == 1 && ctx.input_dbs[0] == ALL_INPUT_DBS;
    let skip_dbs = parse_table_list("SKIP_DBS");
    if !skip_dbs.is_empty() && !all_mode {
        fatalf(format_args!(
            "SKIP_DBS can only be used with GHA2DB_INPUT_DBS={}",
            go_quote(ALL_INPUT_DBS)
        ));
    }

    if all_mode {
        ctx.input_dbs = project_dbs_for_shared_db(ctx);
        if !skip_dbs.is_empty() {
            let (skipped, kept): (Vec<String>, Vec<String>) = ctx
                .input_dbs
                .iter()
                .cloned()
                .partition(|db| skip_dbs.contains(db));
            ctx.input_dbs = kept;
            printf!(
                "merge_dbs: skipped {} DB(s) using SKIP_DBS={}: {}\n",
                skipped.len(),
                go_quote(&std::env::var("SKIP_DBS").unwrap_or_default()),
                gofmt::slice(&skipped)
            );
        }
        if ctx.input_dbs.is_empty() {
            fatalf(format_args!(
                "no enabled projects in {} have shared_db={}",
                ctx.projects_yaml,
                go_quote(&ctx.output_db)
            ));
        }
        printf!(
            "merge_dbs: expanded GHA2DB_INPUT_DBS={} to {} DB(s) with shared_db={}: {}\n",
            go_quote(ALL_INPUT_DBS),
            ctx.input_dbs.len(),
            go_quote(&ctx.output_db),
            gofmt::slice(&ctx.input_dbs)
        );
    }

    all_mode
}

/// Go `isNoDBError`: `invalid_catalog_name` (SQLSTATE 3D000) or a message
/// saying that a database does not exist.
fn is_no_db_error(err: &PgError) -> bool {
    if let Some(e) = err.server() {
        return e.name() == "invalid_catalog_name" || e.code == "3D000";
    }
    let msg = err.to_string().to_lowercase();
    msg.contains("database") && msg.contains("does not exist")
}

/// Go `connectInputDB`: a lazily connecting pool, except in the
/// `-all-` + `IGNORE_NO_DB` mode where the database is pinged right away so
/// that missing databases can be skipped.
fn connect_input_db(
    ctx: &mut Ctx,
    db: &str,
    all_mode: bool,
    ignore_no_db: bool,
) -> Result<PgConn, PgError> {
    if !(all_mode && ignore_no_db) {
        return Ok(pg::pg_conn_db(ctx, db));
    }
    let mut lctx = ctx.clone();
    lctx.pg_db = db.to_string();
    lctx.exec_fatal = false;
    lctx.exec_output = true;
    let c = pg::pg_conn_err(&lctx)?;
    if let Err(e) = c.ping() {
        c.close();
        return Err(e);
    }
    Ok(c)
}

/// Everything the per-table workers share read-only.
struct Merge<'a> {
    ctx: &'a Ctx,
    co: &'a PgConn,
    ci: &'a [PgConn],
    i_names: &'a [String],
    merge_dt_from: Option<DateTime<Utc>>,
    use_batch: bool,
    batch_size: usize,
}

impl Merge<'_> {
    /// Go `processTable`: merge one table (one pass) from every input DB.
    fn process_table(&self, pass: usize, pass_info: &str, i: usize, data: &TableData) {
        let ctx = self.ctx;
        let table = data.0;
        let cond = if pass == 0 { data.1 } else { data.2 };
        if cond == "-" {
            return;
        }
        let mut all_rows = 0usize;
        let mut all_errs = 0usize;
        let mut all_ins = 0usize;
        for (dbi, c) in self.ci.iter().enumerate() {
            let db_name = &self.i_names[dbi];
            // First get the row count.
            let mut query_root = format!("from {table}");
            let mut query_args: Vec<SqlArg> = Vec::new();
            if !cond.is_empty() {
                query_root.push_str(" where ");
                query_root.push_str(cond);
            }
            if let Some(dt_from) = self.merge_dt_from {
                let dt_cond = merge_date_condition(table);
                if !dt_cond.is_empty() {
                    query_root = add_where_condition(&query_root, dt_cond);
                    query_args.push(SqlArg::Time(dt_from.fixed_offset()));
                } else if dbi == 0 {
                    printf!(
                        "merge_dbs date filter: table {} has no merge date mapping, copying all rows\n",
                        table
                    );
                }
            }
            let mut rc: i64 = 0;
            fatal_on_pg_err(
                pg::query_row_sql(
                    c,
                    ctx,
                    &format!("select count(*) {query_root}"),
                    &query_args,
                )
                .scan(&mut [&mut rc]),
            );
            let rc = usize::try_from(rc).unwrap_or(0);

            // Now get all the data.
            printf!(
                "{}: start table: #{}: {}, DB #{}: {}, rows: {}...\n",
                pass_info,
                i,
                table,
                dbi,
                db_name,
                rc
            );
            let mut rows =
                pg::query_sql_with_err(c, ctx, &format!("select * {query_root}"), &query_args);
            let columns = rows.column_names();
            let n_columns = columns.len();
            let cols = format!(
                "({})",
                columns
                    .iter()
                    .map(|col| format!("\"{col}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            let mut row_count = 0usize;
            let mut err_count = 0usize;
            let mut ins_count = 0usize;
            // For `progress_info`.
            let dt_start = Utc::now();
            let mut last_time = dt_start;
            let progress_msg = format!("{pass_info}: table #{i} {table}, DB #{dbi} {db_name}");
            if self.use_batch {
                // Batch mode: many rows per `insert ... on conflict do nothing`;
                // conflicts are not errors, the inserted count comes from the
                // rows affected and the rest of the batch are collisions.
                let max_rows_by_params = (MAX_PARAMS / n_columns.max(1)).max(1);
                let eff_batch = self.batch_size.min(max_rows_by_params);
                if eff_batch < self.batch_size {
                    printf!(
                        "{}: table #{} {}, DB #{} {}: batch size capped from {} to {} ({} columns, max {} psql params)\n",
                        pass_info,
                        i,
                        table,
                        dbi,
                        db_name,
                        self.batch_size,
                        eff_batch,
                        n_columns,
                        MAX_PARAMS
                    );
                }
                let insert_prefix = format!("insert into {table}{cols} ");
                let full_values = batch_values(eff_batch, n_columns);
                let mut batch_args: Vec<SqlArg> = Vec::with_capacity(eff_batch * n_columns);
                let mut rows_in_batch = 0usize;
                let mut flush = |batch_args: &mut Vec<SqlArg>, rows_in_batch: &mut usize| {
                    if *rows_in_batch == 0 {
                        return;
                    }
                    let values_clause = if *rows_in_batch == eff_batch {
                        full_values.clone()
                    } else {
                        batch_values(*rows_in_batch, n_columns)
                    };
                    let query = format!("{insert_prefix}{values_clause} on conflict do nothing");
                    let res = match pg::exec_sql(self.co, ctx, &query, batch_args) {
                        Ok(res) => res,
                        Err(e) => {
                            // `on conflict do nothing` never raises a unique
                            // violation, so this is a real problem (usually
                            // a different columns order).
                            printf!(
                                "Failing batch insert into {} (rows: {}, columns: {})\n",
                                table,
                                *rows_in_batch,
                                n_columns
                            );
                            // Go ignores the retry status `FatalOnError`
                            // returns for `DURABLE_PQ` and carries on with
                            // a zero result.
                            fatal_on_pg_error(&e);
                            ExecResult::RowsAffected(0)
                        }
                    };
                    let affected = fatal_on_pg_err(res.rows_affected());
                    let ins = usize::try_from(affected).unwrap_or(0).min(*rows_in_batch);
                    ins_count += ins;
                    err_count += *rows_in_batch - ins;
                    row_count += *rows_in_batch;
                    batch_args.clear();
                    *rows_in_batch = 0;
                    gotime::progress_info(
                        row_count,
                        rc,
                        dt_start,
                        &mut last_time,
                        PROGRESS_PERIOD,
                        &progress_msg,
                    );
                };
                while rows.next() {
                    batch_args.extend(rows.values().iter().map(SqlArg::from));
                    rows_in_batch += 1;
                    if rows_in_batch >= eff_batch {
                        flush(&mut batch_args, &mut rows_in_batch);
                    }
                }
                flush(&mut batch_args, &mut rows_in_batch);
            } else {
                let query = format!("insert into {table}{cols} {}", pg::n_values(n_columns));
                while rows.next() {
                    let vals = rows.values();
                    let args: Vec<SqlArg> = vals.iter().map(SqlArg::from).collect();
                    match pg::exec_sql(self.co, ctx, &query, &args) {
                        Ok(_) => ins_count += 1,
                        Err(e) => {
                            if e.server().is_none() {
                                fatal_on_pg_error(&e);
                            } else if e.name() != "unique_violation" {
                                // Usually a different columns order: the
                                // inserts are positional.
                                printf!("Failing values:\n");
                                // `DriverValue`'s `Display` is Go's `%+v`.
                                for (vi, vv) in vals.iter().enumerate() {
                                    printf!("{}: {}\n", vi, vv);
                                }
                                fatal_on_pg_error(&e);
                            }
                            // Reached only for unique violations, or when
                            // `FatalOnError` returned a `DURABLE_PQ` retry
                            // status (Go ignores it here too).
                            err_count += 1;
                        }
                    }
                    row_count += 1;
                    gotime::progress_info(
                        row_count,
                        rc,
                        dt_start,
                        &mut last_time,
                        PROGRESS_PERIOD,
                        &progress_msg,
                    );
                }
            }
            fatal_on_pg_err(rows.err());
            fatal_on_pg_err(rows.close());
            let perc = if row_count > 0 {
                err_count as f64 * 100.0 / row_count as f64
            } else {
                0.0
            };
            printf!(
                "{}: done table: #{}: {}, DB #{}: {}, rows: {}, inserted: {}, collisions: {} ({:.3}%)\n",
                pass_info,
                i,
                table,
                dbi,
                db_name,
                row_count,
                ins_count,
                err_count,
                perc
            );
            all_rows += row_count;
            all_errs += err_count;
            all_ins += ins_count;
        }
        let perc = if all_rows > 0 {
            all_errs as f64 * 100.0 / all_rows as f64
        } else {
            0.0
        };
        printf!(
            "{}: done table: #{}: {}, all rows: {}, inserted: {}, collisions: {} ({:.3}%)\n",
            pass_info,
            i,
            table,
            all_rows,
            all_ins,
            all_errs,
            perc
        );
    }
}

/// Go `mergePDBs`.
fn merge_pdbs() {
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    if ctx.output_db.is_empty() {
        fatalf(format_args!("output database required"));
    }
    let merge_dt_from = parse_merge_dt_from();
    if let Some(dt) = merge_dt_from {
        printf!(
            "merge_dbs date filter: MERGE_DT_FROM={}; tables without a merge date mapping are copied fully\n",
            go_quote(&gotime::to_ymdhms_date(dt))
        );
    }
    let all_mode = resolve_input_dbs(&mut ctx);
    let ignore_no_db = env_flag("IGNORE_NO_DB");
    if ignore_no_db && !all_mode {
        fatalf(format_args!(
            "IGNORE_NO_DB=1 can only be used with GHA2DB_INPUT_DBS={}",
            go_quote(ALL_INPUT_DBS)
        ));
    }
    if ctx.input_dbs.is_empty() {
        fatalf(format_args!(
            "required at least 1 input database, got {}: {}",
            ctx.input_dbs.len(),
            gofmt::slice(&ctx.input_dbs)
        ));
    }

    // Connect to the input databases.
    let mut ci: Vec<PgConn> = Vec::new();
    let mut i_names: Vec<String> = Vec::new();
    for i_name in ctx.input_dbs.clone() {
        match connect_input_db(&mut ctx, &i_name, all_mode, ignore_no_db) {
            Ok(c) => {
                ci.push(c);
                i_names.push(i_name);
            }
            Err(e) => {
                if all_mode && ignore_no_db && is_no_db_error(&e) {
                    printf!(
                        "merge_dbs: skipping unavailable input DB {} due to IGNORE_NO_DB=1: {}\n",
                        go_quote(&i_name),
                        e
                    );
                    continue;
                }
                fatal_on_pg_error(&e);
            }
        }
    }
    if ci.is_empty() {
        fatalf(format_args!(
            "required at least 1 available input database after filtering/connection, got {} from {}",
            ci.len(),
            gofmt::slice(&ctx.input_dbs)
        ));
    }

    // Connect to the output database.
    let output_db = ctx.output_db.clone();
    let co = pg::pg_conn_db(&mut ctx, &output_db);

    // Tables to process: the 1st pass uses the 1st condition, the 2nd pass
    // the 2nd one; "-" skips the pass. Some tables are left out because
    // other tools fill them on the merged database.
    let mut table_data: Vec<TableData> = Vec::with_capacity(TABLE_DATA.len() + 1);
    if ctx.affiliations_db.is_empty() {
        table_data.push(ACTORS_TABLE);
    }
    table_data.extend_from_slice(TABLE_DATA);

    let only_tables = parse_table_list("ONLY_TABLES");
    let skip_tables = parse_table_list("SKIP_TABLES");
    if !only_tables.is_empty() || !skip_tables.is_empty() {
        let known: HashSet<&str> = table_data.iter().map(|d| d.0).collect();
        for table in &only_tables {
            if !known.contains(table.as_str()) {
                fatalf(format_args!(
                    "ONLY_TABLES contains unknown table '{}'",
                    table
                ));
            }
        }
        if only_tables.is_empty() {
            for table in &skip_tables {
                if !known.contains(table.as_str()) {
                    fatalf(format_args!(
                        "SKIP_TABLES contains unknown table '{}'",
                        table
                    ));
                }
            }
        }
        table_data.retain(|d| {
            if !only_tables.is_empty() {
                only_tables.contains(d.0)
            } else {
                !skip_tables.contains(d.0)
            }
        });
        printf!(
            "merge_dbs table filter: selected {} table(s), ONLY_TABLES={}, SKIP_TABLES={}\n",
            table_data.len(),
            go_quote(&std::env::var("ONLY_TABLES").unwrap_or_default()),
            go_quote(&std::env::var("SKIP_TABLES").unwrap_or_default())
        );
    }

    // Batch / parallelism configuration (defaults: single-row inserts,
    // single-threaded).
    let use_batch = env_flag("USE_BATCH");
    let batch_size = if use_batch { parse_batch_size() } else { 1000 };
    let parallel = parse_parallel();
    printf!(
        "merge_dbs: USE_BATCH={}, BATCH_SIZE={}, PARALLEL={} (max {} psql params per batch insert)\n",
        use_batch,
        batch_size,
        parallel,
        MAX_PARAMS
    );

    let merge = Merge {
        ctx: &ctx,
        co: &co,
        ci: &ci,
        i_names: &i_names,
        merge_dt_from,
        use_batch,
        batch_size,
    };
    for (pass, pass_info) in ["1st pass", "2nd pass"].iter().enumerate() {
        if parallel > 1 {
            // Up to `parallel` tables of a pass are processed concurrently;
            // the passes stay sequential.
            let next = AtomicUsize::new(0);
            thread::scope(|s| {
                for _ in 0..parallel.min(table_data.len()) {
                    let (merge, next, table_data) = (&merge, &next, &table_data);
                    s.spawn(move || loop {
                        let i = next.fetch_add(1, Ordering::SeqCst);
                        let Some(data) = table_data.get(i) else {
                            break;
                        };
                        merge.process_table(pass, pass_info, i, data);
                    });
                }
            });
        } else {
            for (i, data) in table_data.iter().enumerate() {
                merge.process_table(pass, pass_info, i, data);
            }
        }
    }

    // `defer`s: the output pool is closed first, then the input pools.
    co.close();
    for c in &ci {
        c.close();
    }
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    merge_pdbs();
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
    // Plain `fmt.Printf` in Go — not logged to the database.
    println!("Consider running './devel/remove_db_dups.sh' if you merged into existing database.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use devstatscode::chrono::Timelike;

    fn parse(s: &str) -> Option<DateTime<Utc>> {
        DT_LAYOUTS.iter().find_map(|l| go_time_parse(l, s))
    }

    #[test]
    fn dt_from_layouts_like_go() {
        let ymdhms = |s: &str| parse(s).map(gotime::to_ymdhms_date);
        assert_eq!(
            ymdhms("2024-01-02T03:04:05Z").as_deref(),
            Some("2024-01-02 03:04:05")
        );
        assert_eq!(
            ymdhms("2024-01-02 03:04:05").as_deref(),
            Some("2024-01-02 03:04:05")
        );
        assert_eq!(
            ymdhms("2024-01-02 03:04").as_deref(),
            Some("2024-01-02 03:04:00")
        );
        assert_eq!(
            ymdhms("2024-01-02 03").as_deref(),
            Some("2024-01-02 03:00:00")
        );
        // Go's `15` accepts a single digit hour.
        assert_eq!(
            ymdhms("2024-01-02 3").as_deref(),
            Some("2024-01-02 03:00:00")
        );
        assert_eq!(ymdhms("2024-01-02").as_deref(), Some("2024-01-02 00:00:00"));
        // A layout space matches a run of spaces.
        assert_eq!(
            ymdhms("2024-01-02   03:04:05").as_deref(),
            Some("2024-01-02 03:04:05")
        );
        // Fractional seconds are accepted without being in the layout.
        let frac = parse("2024-01-02 03:04:05.123456789123").unwrap();
        assert_eq!(frac.nanosecond(), 123_456_789);
        let frac = parse("2024-01-02T03:04:05,5Z").unwrap();
        assert_eq!(frac.nanosecond(), 500_000_000);
        // Rejected like in Go.
        for bad in [
            "",
            "2024",
            "2024-1-02",
            "2024-01-2",
            "24-01-02",
            "2024-13-01",
            "2024-02-30",
            "2024-01-02 24:00",
            "2024-01-02 03:60",
            "2024-01-02 03:04:60",
            "2024-01-02T03:04:05",
            "2024-01-02 03:04:05Z",
            "2024-01-02 03:04:05 ",
            "2024-01-02x",
            "2024-01-02 03:04:05.",
            "2024-01-02T03:04Z",
        ] {
            assert!(parse(bad).is_none(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn batch_values_like_go() {
        assert_eq!(batch_values(1, 1), "values ($1)");
        assert_eq!(batch_values(2, 3), "values ($1,$2,$3),($4,$5,$6)");
        assert_eq!(batch_values(0, 3), "values ");
    }

    #[test]
    fn where_conditions() {
        assert_eq!(add_where_condition("from t", ""), "from t");
        assert_eq!(
            add_where_condition("from t", "a >= $1"),
            "from t where a >= $1"
        );
        assert_eq!(
            add_where_condition("from t where id > 0", "a >= $1"),
            "from t where id > 0 and a >= $1"
        );
        assert_eq!(merge_date_condition("gha_events"), "created_at >= $1");
        assert_eq!(merge_date_condition("gha_commits"), "dup_created_at >= $1");
        assert_eq!(merge_date_condition("gha_skip_commits"), "dt >= $1");
        assert_eq!(
            merge_date_condition("gha_releases_assets"),
            "event_id in (select id from gha_events where created_at >= $1)"
        );
        assert_eq!(merge_date_condition("gha_actors"), "");
        assert_eq!(merge_date_condition("gha_repos"), "");
    }

    #[test]
    fn table_list_matches_go() {
        assert_eq!(TABLE_DATA.len(), 31);
        assert_eq!(TABLE_DATA[0].0, "gha_assets");
        assert_eq!(TABLE_DATA[30].0, "gha_texts");
        let two_pass: Vec<&str> = TABLE_DATA
            .iter()
            .filter(|d| d.2 != "-")
            .map(|d| d.0)
            .collect();
        assert_eq!(
            two_pass,
            ["gha_events", "gha_issues", "gha_labels", "gha_payloads"]
        );
        assert_eq!(ACTORS_TABLE.1, "id > 0");
    }
}
