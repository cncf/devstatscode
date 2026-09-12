//! `calc_metric` — Rust port of `cmd/calc_metric/calc_metric.go`.
//!
//! Computes one metric from a metric SQL file into the DevStats time-series
//! tables (`s<series>` fields / `t<series>` tags, written through
//! `write_ts_points`): either a series of period ranges (`h`, `d7`, `m`, …,
//! `{{from}}`/`{{to}}`/`{{range}}`/… placeholders, one query per range, MT
//! over `GHA2DB_NCPUS` connections) or a histogram (`hist`: one query for the
//! last `{{period}}`, an annotations quick range or a `range:from,to`).
//! Arguments, options (`hist,desc:…,multivalue,escape_value_name,…`),
//! environment variables, messages, SQL statements and exit codes are those
//! of the Go program.
//!
//! Deliberate differences: maps are iterated in sorted order (Go: random map
//! order — the order of the multivalue series writes and of the per-series
//! `delete` statements); the numbers/dates parsers' error wording; a Go
//! runtime panic on malformed metric output (a row with too few
//! `;`-separated parts, too few columns) is a Rust index panic — both exit 2.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use devstatscode::chrono::{DateTime, Local, Utc};
use devstatscode::pg::{
    self, exec_sql, exec_sql_with_err, fatal_on_pg_err, get_tag_values, insert_ignore, n_value,
    n_values, query_sql_with_err, table_exists, write_ts_points, PgConn, SqlArg,
};
use devstatscode::ts_points::{
    add_ts_point, make_ts_points_unique_times, new_ts_point, FieldValue, Fields, TSPoints,
};
use devstatscode::{
    consts, error, fatal_on_err, fatalf, gofmt, io, map as gomap, printf, rng, signal,
    string as gostring, threads, time as gotime, unicode, Ctx,
};

/// Go `calcMetricData`: the parsed options (6th argument).
#[derive(Debug, Clone, Default, PartialEq)]
struct CalcMetricData {
    hist: bool,
    multivalue: bool,
    escape_value_name: bool,
    skip_escape_series_name: bool,
    annotations_ranges: bool,
    skip_past: bool,
    desc: String,
    merge_series: String,
    custom_data: bool,
    custom_data_unique_time: bool,
    series_name_map: Option<BTreeMap<String, String>>,
    drop: Vec<String>,
    project_scale: String,
    hll: bool,
}

impl CalcMetricData {
    /// Go `%+v` of the struct value.
    fn go_string(&self) -> String {
        format!(
            "{{hist:{} multivalue:{} escapeValueName:{} skipEscapeSeriesName:{} annotationsRanges:{} \
             skipPast:{} desc:{} mergeSeries:{} customData:{} customDataUniqueTime:{} \
             seriesNameMap:{} drop:{} projectScale:{} hll:{}}}",
            self.hist,
            self.multivalue,
            self.escape_value_name,
            self.skip_escape_series_name,
            self.annotations_ranges,
            self.skip_past,
            self.desc,
            self.merge_series,
            self.custom_data,
            self.custom_data_unique_time,
            match &self.series_name_map {
                Some(m) => gofmt::map(m),
                None => "map[]".to_string(),
            },
            gofmt::slice(&self.drop),
            self.project_scale,
            self.hll
        )
    }
}

/// Go globals: start date and command line stored into `gha_last_computed`.
#[derive(Clone)]
struct Globals {
    start_dt: DateTime<Local>,
    start: Instant,
    cmd: String,
}

/// Go `gDropped`: the `drop:` tables are handled once per process.
static DROPPED: Mutex<bool> = Mutex::new(false);

/// Go `mapName`: `series_name_map` lookup.
fn map_name(cfg: &CalcMetricData, name: &str) -> String {
    match &cfg.series_name_map {
        Some(m) => m.get(name).cloned().unwrap_or_else(|| name.to_string()),
        None => name.to_string(),
    }
}

/// Go `valueDescription`: currently only `time_diff_as_string`.
fn value_description(desc_func: &str, value: f64) -> String {
    match desc_func {
        "time_diff_as_string" => gotime::describe_period_in_hours(value),
        _ => {
            printf!(
                "Error\nUnknown value description function '{}'\n",
                desc_func
            );
            println!("Error\nUnknown value description function '{}'", desc_func);
            std::process::exit(1);
        }
    }
}

/// Go `multiRowMultiColumn`: `prefix;rowName;series1,series2,…` →
/// series names (see the Go comments for the multivalue variants).
fn multi_row_multi_column(
    cfg: &CalcMetricData,
    expr: &str,
    multivalue: bool,
    escape_value_name: bool,
    skip_escape_series_name: bool,
) -> Vec<String> {
    let ary: Vec<&str> = expr.split(';').collect();
    let pref = ary[0];
    if pref.is_empty() {
        printf!(
            "multiRowMultiColumn: Info: prefix '{}' (ary={},expr={},mv={},data={}) skipping\n",
            pref,
            gofmt::slice(&ary),
            expr,
            multivalue,
            cfg.go_string()
        );
        return Vec::new();
    }
    let split_columns: Vec<&str> = ary[2].split(',').collect();
    let mut result = Vec::new();
    if multivalue {
        let row_name_ary: Vec<&str> = ary[1].split('`').collect();
        let mut row_name = row_name_ary[0].to_string();
        if escape_value_name {
            row_name = unicode::normalize_name(&row_name);
        }
        row_name = map_name(cfg, &row_name);
        if row_name_ary.len() > 1 {
            let row_name_non_multi = if skip_escape_series_name {
                row_name_ary[1].to_string()
            } else {
                unicode::normalize_name(row_name_ary[1])
            };
            for series in &split_columns {
                result.push(format!("{pref}{row_name_non_multi}{series};{row_name}"));
            }
            return result;
        }
        for series in &split_columns {
            result.push(format!("{pref}{series};{row_name}"));
        }
        return result;
    }
    let row_name = if skip_escape_series_name {
        ary[1].to_string()
    } else {
        unicode::normalize_name(ary[1])
    };
    if row_name.is_empty() {
        printf!(
            "multiRowMultiColumn: Info: rowName '{}' ({}) maps to empty string, skipping\n",
            ary[1],
            gofmt::slice(&ary)
        );
        return Vec::new();
    }
    let row_name = map_name(cfg, &row_name);
    for series in &split_columns {
        result.push(format!("{pref}{row_name}{series}"));
    }
    result
}

/// Go `multiRowSingleColumn`: `prefix,rowName` → one series name.
fn multi_row_single_column(
    cfg: &CalcMetricData,
    col: &str,
    multivalue: bool,
    escape_value_name: bool,
    skip_escape_series_name: bool,
) -> Vec<String> {
    let ary: Vec<&str> = col.split(',').collect();
    let pref = ary[0];
    if pref.is_empty() {
        printf!(
            "multiRowSingleColumn: Info: prefix '{}' (ary={},col={},mv={},data={}) skipping\n",
            pref,
            gofmt::slice(&ary),
            col,
            multivalue,
            cfg.go_string()
        );
        return Vec::new();
    }
    if multivalue {
        let row_name_ary: Vec<&str> = ary[1].split('`').collect();
        let mut row_name = row_name_ary[0].to_string();
        if escape_value_name {
            row_name = unicode::normalize_name(&row_name);
        }
        row_name = map_name(cfg, &row_name);
        if row_name_ary.len() > 1 {
            let row_name_non_multi = if skip_escape_series_name {
                row_name_ary[1].to_string()
            } else {
                unicode::normalize_name(row_name_ary[1])
            };
            return vec![format!("{pref}{row_name_non_multi};{row_name}")];
        }
        return vec![format!("{pref};{row_name}")];
    }
    let row_name = if skip_escape_series_name {
        ary[1].to_string()
    } else {
        unicode::normalize_name(ary[1])
    };
    if row_name.is_empty() {
        printf!(
            "multiRowSingleColumn: Info: rowName '{}' ({}) maps to empty string, skipping\n",
            ary[1],
            gofmt::slice(&ary)
        );
        return Vec::new();
    }
    let row_name = map_name(cfg, &row_name);
    vec![format!("{pref}{row_name}")]
}

/// Go `nameForMetricsRow`: series names for a result row given the
/// `series_name_or_func` function name.
fn name_for_metrics_row(
    cfg: &CalcMetricData,
    metric: &str,
    name: &str,
    multivalue: bool,
    escape_value_name: bool,
    skip_escape_series_name: bool,
) -> Vec<String> {
    match metric {
        "single_row_multi_column" => name.split(',').map(str::to_string).collect(),
        "multi_row_single_column" => multi_row_single_column(
            cfg,
            name,
            multivalue,
            escape_value_name,
            skip_escape_series_name,
        ),
        "multi_row_multi_column" => multi_row_multi_column(
            cfg,
            name,
            multivalue,
            escape_value_name,
            skip_escape_series_name,
        ),
        _ => {
            printf!("Error\nUnknown metric '{}'\n", metric);
            println!("Error\nUnknown metric '{}'", metric);
            std::process::exit(1);
        }
    }
}

/// Go `getHLLDefault`: the text form of `hll_empty()` (`\x118b7f`).
fn get_hll_default() -> Vec<u8> {
    vec![92, 120, 49, 49, 56, 98, 55, 102]
}

/// `database/sql` scan of a column into `sql.RawBytes`/`[]uint8` then
/// `string(...)`: NULL is the empty string.
fn raw_string(v: &pg::DriverValue) -> String {
    v.go_string().unwrap_or_default()
}

/// Scan into `[]uint8`: NULL is an empty slice.
fn raw_bytes(v: &pg::DriverValue) -> Vec<u8> {
    v.go_bytes().unwrap_or_default()
}

/// Go `value, _ = strconv.ParseFloat(s, 64)`: 0 when unparsable.
fn float_or_zero(s: &str) -> f64 {
    gotime::parse_go_float(s).unwrap_or(0.0)
}

/// Go `calcSingleHLLRange`: one range of an `hll` metric.
#[allow(clippy::too_many_arguments)]
fn calc_single_hll_range(
    ctx: &Ctx,
    sqlc: &PgConn,
    cfg: &CalcMetricData,
    pts: &mut TSPoints,
    sql_query: &str,
    series_name_or_func: &str,
    period: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    dt: DateTime<Utc>,
    hll_empty: &[u8],
) {
    let mut rows = query_sql_with_err(sqlc, ctx, sql_query, &[]);
    let n_columns = rows.columns().len();
    if n_columns == 1 {
        let mut value: Vec<u8> = Vec::new();
        let mut row_count = 0;
        while rows.next() {
            value = raw_bytes(&rows.values()[0]);
            row_count += 1;
        }
        fatal_on_pg_err(rows.err());
        if row_count != 1 {
            printf!(
                "Error:\nQuery should return either single value or \
                 multiple rows, each containing string and numbers\n\
                 Got {} rows, each containing single number\nQuery:{}\n",
                row_count,
                sql_query
            );
        }
        let name = series_name_or_func;
        if ctx.debug > 0 {
            printf!(
                "{} - {} -> {}, {}\n",
                gofmt::time(from),
                gofmt::time(to),
                name,
                gostring::format_raw_bytes(&value)
            );
        }
        let mut fields = Fields::new();
        fields.insert("value".to_string(), FieldValue::Hll(value));
        add_ts_point(
            ctx,
            pts,
            new_ts_point(ctx, name, period, None, Some(&fields), dt, false),
        );
    } else if n_columns >= 2 {
        let mut all_fields: BTreeMap<String, Fields> = BTreeMap::new();
        let mut c_hll: Vec<u8> = Vec::new();
        let mut c_time: DateTime<Utc> = Utc::now();
        while rows.next() {
            let values = rows.values().to_vec();
            let name = raw_string(&values[0]);
            let names = name_for_metrics_row(
                cfg,
                series_name_or_func,
                &name,
                cfg.multivalue,
                cfg.escape_value_name,
                cfg.skip_escape_series_name,
            );
            if ctx.debug > 0 {
                printf!("nameForMetricsRow: {} -> {}\n", name, gofmt::slice(&names));
            }
            if names.is_empty() {
                continue;
            }
            if cfg.custom_data {
                // values triples (time, HLL, string)
                for (idx, p_val) in values[1..].iter().enumerate() {
                    let val_type = idx % 3;
                    let cidx = idx / 3;
                    if val_type == 0 {
                        c_time = gotime::time_parse_any(&raw_string(p_val));
                    } else if val_type == 1 {
                        c_hll = raw_bytes(p_val);
                    } else {
                        let c_string = raw_string(p_val);
                        if cfg.multivalue {
                            let name_arr: Vec<&str> = names[cidx].split(';').collect();
                            let series_name = name_arr[0];
                            let series_value_name = name_arr[1];
                            if ctx.debug > 0 {
                                printf!(
                                    "{} - {} -> ({}, {}): {}[{}], ({}, {}, {})\n",
                                    gofmt::time(from),
                                    gofmt::time(to),
                                    idx,
                                    cidx,
                                    series_name,
                                    series_value_name,
                                    gofmt::time(c_time),
                                    gostring::format_raw_bytes(&c_hll),
                                    c_string
                                );
                            }
                            let f = all_fields.entry(series_name.to_string()).or_default();
                            f.insert(format!("{series_value_name}_t"), FieldValue::Time(c_time));
                            f.insert(
                                format!("{series_value_name}_h"),
                                FieldValue::Hll(c_hll.clone()),
                            );
                            f.insert(
                                format!("{series_value_name}_s"),
                                FieldValue::Str(c_string.clone()),
                            );
                        } else {
                            let name = &names[cidx];
                            if ctx.debug > 0 {
                                printf!(
                                    "{} - {} -> ({}, {}): {}, ({}, {}, {})\n",
                                    gofmt::time(from),
                                    gofmt::time(to),
                                    idx,
                                    cidx,
                                    name,
                                    gofmt::time(c_time),
                                    gostring::format_raw_bytes(&c_hll),
                                    c_string
                                );
                            }
                            let mut fields = Fields::new();
                            fields.insert("value".to_string(), FieldValue::Hll(c_hll.clone()));
                            fields.insert("str".to_string(), FieldValue::Str(c_string.clone()));
                            fields.insert("dt".to_string(), FieldValue::Time(c_time));
                            add_ts_point(
                                ctx,
                                pts,
                                new_ts_point(ctx, name, period, None, Some(&fields), c_time, true),
                            );
                        }
                    }
                }
            } else {
                for (idx, p_val) in values[1..].iter().enumerate() {
                    let value = raw_bytes(p_val);
                    if cfg.multivalue {
                        let name_arr: Vec<&str> = names[idx].split(';').collect();
                        let series_name = name_arr[0];
                        let series_value_name = name_arr[1];
                        if ctx.debug > 0 {
                            printf!(
                                "{} - {} -> {}: {}[{}], {}\n",
                                gofmt::time(from),
                                gofmt::time(to),
                                idx,
                                series_name,
                                series_value_name,
                                gostring::format_raw_bytes(&value)
                            );
                        }
                        all_fields
                            .entry(series_name.to_string())
                            .or_default()
                            .insert(series_value_name.to_string(), FieldValue::Hll(value));
                    } else {
                        let name = &names[idx];
                        if ctx.debug > 0 {
                            printf!(
                                "{} - {} -> {}: {}, {}\n",
                                gofmt::time(from),
                                gofmt::time(to),
                                idx,
                                name,
                                gostring::format_raw_bytes(&value)
                            );
                        }
                        let mut fields = Fields::new();
                        fields.insert("value".to_string(), FieldValue::Hll(value));
                        add_ts_point(
                            ctx,
                            pts,
                            new_ts_point(ctx, name, period, None, Some(&fields), dt, false),
                        );
                    }
                }
            }
        }
        fatal_on_pg_err(rows.err());
        for (series_name, series_values) in &all_fields {
            add_ts_point(
                ctx,
                pts,
                new_ts_point(
                    ctx,
                    series_name,
                    period,
                    None,
                    Some(series_values),
                    dt,
                    cfg.custom_data,
                ),
            );
        }
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    let _ = hll_empty;
}

/// Go `calcSingleNumericRange`: one range of a numeric metric.
#[allow(clippy::too_many_arguments)]
fn calc_single_numeric_range(
    ctx: &Ctx,
    sqlc: &PgConn,
    cfg: &CalcMetricData,
    pts: &mut TSPoints,
    sql_query: &str,
    series_name_or_func: &str,
    period: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    dt: DateTime<Utc>,
) {
    let mut rows = query_sql_with_err(sqlc, ctx, sql_query, &[]);
    let n_columns = rows.columns().len();
    let use_desc = !cfg.desc.is_empty();
    if n_columns == 1 {
        let mut p_value: Option<f64> = None;
        let mut row_count = 0;
        while rows.next() {
            fatal_on_pg_err(rows.scan(&mut [&mut p_value]));
            row_count += 1;
        }
        fatal_on_pg_err(rows.err());
        if row_count != 1 {
            printf!(
                "Error:\nQuery should return either single value or \
                 multiple rows, each containing string and numbers\n\
                 Got {} rows, each containing single number\nQuery:{}\n",
                row_count,
                sql_query
            );
        }
        let value = p_value.unwrap_or(0.0);
        let name = series_name_or_func;
        if ctx.debug > 0 {
            printf!(
                "{} - {} -> {}, {}\n",
                gofmt::time(from),
                gofmt::time(to),
                name,
                gofmt::float(value)
            );
        }
        let mut fields = Fields::new();
        fields.insert("value".to_string(), FieldValue::Float(value));
        if use_desc {
            fields.insert(
                "descr".to_string(),
                FieldValue::Str(value_description(&cfg.desc, value)),
            );
        }
        add_ts_point(
            ctx,
            pts,
            new_ts_point(ctx, name, period, None, Some(&fields), dt, false),
        );
    } else if n_columns >= 2 {
        let mut all_fields: BTreeMap<String, Fields> = BTreeMap::new();
        let mut c_float = 0.0_f64;
        let mut c_time: DateTime<Utc> = Utc::now();
        while rows.next() {
            let values = rows.values().to_vec();
            let name = raw_string(&values[0]);
            let names = name_for_metrics_row(
                cfg,
                series_name_or_func,
                &name,
                cfg.multivalue,
                cfg.escape_value_name,
                cfg.skip_escape_series_name,
            );
            if ctx.debug > 0 {
                printf!("nameForMetricsRow: {} -> {}\n", name, gofmt::slice(&names));
            }
            if names.is_empty() {
                continue;
            }
            if cfg.custom_data {
                // values triples (time, float, string)
                for (idx, p_val) in values[1..].iter().enumerate() {
                    let val_type = idx % 3;
                    let cidx = idx / 3;
                    if val_type == 0 {
                        c_time = gotime::time_parse_any(&raw_string(p_val));
                    } else if val_type == 1 {
                        c_float = float_or_zero(&raw_string(p_val));
                    } else {
                        let c_string = raw_string(p_val);
                        if cfg.multivalue {
                            let name_arr: Vec<&str> = names[cidx].split(';').collect();
                            let series_name = name_arr[0];
                            let series_value_name = name_arr[1];
                            if ctx.debug > 0 {
                                printf!(
                                    "{} - {} -> ({}, {}): {}[{}], ({}, {}, {})\n",
                                    gofmt::time(from),
                                    gofmt::time(to),
                                    idx,
                                    cidx,
                                    series_name,
                                    series_value_name,
                                    gofmt::time(c_time),
                                    gofmt::float(c_float),
                                    c_string
                                );
                            }
                            let f = all_fields.entry(series_name.to_string()).or_default();
                            f.insert(format!("{series_value_name}_t"), FieldValue::Time(c_time));
                            f.insert(format!("{series_value_name}_v"), FieldValue::Float(c_float));
                            f.insert(
                                format!("{series_value_name}_s"),
                                FieldValue::Str(c_string.clone()),
                            );
                        } else {
                            let name = &names[cidx];
                            if ctx.debug > 0 {
                                printf!(
                                    "{} - {} -> ({}, {}): {}, ({}, {}, {})\n",
                                    gofmt::time(from),
                                    gofmt::time(to),
                                    idx,
                                    cidx,
                                    name,
                                    gofmt::time(c_time),
                                    gofmt::float(c_float),
                                    c_string
                                );
                            }
                            let mut fields = Fields::new();
                            fields.insert("value".to_string(), FieldValue::Float(c_float));
                            fields.insert("str".to_string(), FieldValue::Str(c_string.clone()));
                            fields.insert("dt".to_string(), FieldValue::Time(c_time));
                            if use_desc {
                                fields.insert(
                                    "descr".to_string(),
                                    FieldValue::Str(value_description(&cfg.desc, c_float)),
                                );
                            }
                            add_ts_point(
                                ctx,
                                pts,
                                new_ts_point(ctx, name, period, None, Some(&fields), c_time, true),
                            );
                        }
                    }
                }
            } else {
                for (idx, p_val) in values[1..].iter().enumerate() {
                    let value = float_or_zero(&raw_string(p_val));
                    if cfg.multivalue {
                        let name_arr: Vec<&str> = names[idx].split(';').collect();
                        let series_name = name_arr[0];
                        let series_value_name = name_arr[1];
                        if ctx.debug > 0 {
                            printf!(
                                "{} - {} -> {}: {}[{}], {}\n",
                                gofmt::time(from),
                                gofmt::time(to),
                                idx,
                                series_name,
                                series_value_name,
                                gofmt::float(value)
                            );
                        }
                        all_fields
                            .entry(series_name.to_string())
                            .or_default()
                            .insert(series_value_name.to_string(), FieldValue::Float(value));
                    } else {
                        let name = &names[idx];
                        if ctx.debug > 0 {
                            printf!(
                                "{} - {} -> {}: {}, {}\n",
                                gofmt::time(from),
                                gofmt::time(to),
                                idx,
                                name,
                                gofmt::float(value)
                            );
                        }
                        let mut fields = Fields::new();
                        fields.insert("value".to_string(), FieldValue::Float(value));
                        if use_desc {
                            fields.insert(
                                "descr".to_string(),
                                FieldValue::Str(value_description(&cfg.desc, value)),
                            );
                        }
                        add_ts_point(
                            ctx,
                            pts,
                            new_ts_point(ctx, name, period, None, Some(&fields), dt, false),
                        );
                    }
                }
            }
        }
        fatal_on_pg_err(rows.err());
        for (series_name, series_values) in &all_fields {
            add_ts_point(
                ctx,
                pts,
                new_ts_point(
                    ctx,
                    series_name,
                    period,
                    None,
                    Some(series_values),
                    dt,
                    cfg.custom_data,
                ),
            );
        }
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
}

/// One worker's share of the ranges: `(dt, from, to)` triples.
type Ranges = Vec<(DateTime<Utc>, DateTime<Utc>, DateTime<Utc>)>;

/// Go `calcRange`: compute the given ranges on a fresh connection and write
/// the points (the `drop:` handling and the write are serialised by `mutex`
/// in MT mode).
#[allow(clippy::too_many_arguments)]
fn calc_range(
    ctx: &Ctx,
    series_name_or_func: &str,
    sql_query_orig: &str,
    exclude_bots: &str,
    period: &str,
    cfg: &CalcMetricData,
    n_intervals: i64,
    ranges: &Ranges,
    hll_empty: &[u8],
    mutex: Option<&Mutex<()>>,
) {
    let sqlc = pg::pg_conn(ctx);
    let mut pts: TSPoints = Vec::new();
    let sql_query_orig = sql_query_orig
        .replace("{{n}}", &format!("{n_intervals}.0"))
        .replace("{{exclude_bots}}", exclude_bots);
    for (dt, from, to) in ranges {
        let s_from = gotime::to_ymdhms_date(*from);
        let s_to = gotime::to_ymdhms_date(*to);
        let s_hours = gotime::range_hours(*from, *to);
        let sql_query = sql_query_orig
            .replace("{{from}}", &s_from)
            .replace("{{to}}", &s_to)
            .replace("{{range}}", &s_hours)
            .replace("{{project_scale}}", &cfg.project_scale)
            .replace("{{rnd}}", &gostring::rand_string());
        if cfg.hll {
            calc_single_hll_range(
                ctx,
                &sqlc,
                cfg,
                &mut pts,
                &sql_query,
                series_name_or_func,
                period,
                *from,
                *to,
                *dt,
                hll_empty,
            );
        } else {
            calc_single_numeric_range(
                ctx,
                &sqlc,
                cfg,
                &mut pts,
                &sql_query,
                series_name_or_func,
                period,
                *from,
                *to,
                *dt,
            );
        }
    }
    if !ctx.skip_tsdb {
        {
            let _guard = mutex.map(|m| m.lock().unwrap_or_else(|e| e.into_inner()));
            let mut dropped = DROPPED.lock().unwrap_or_else(|e| e.into_inner());
            if !*dropped {
                handle_series_drop(ctx, &sqlc, cfg);
                *dropped = true;
            }
        }
        if cfg.custom_data_unique_time {
            make_ts_points_unique_times(ctx, &mut pts);
        }
        write_ts_points(ctx, &sqlc, &pts, &cfg.merge_series, hll_empty, mutex);
    } else if ctx.debug > 0 {
        printf!("Skipping series write\n");
    }
    sqlc.close();
}

/// Go `getPathIndependentKey`: `…/metrics/<proj>/key.sql` → `<proj>/key.sql`
/// (`with_proj`) or `key.sql`.
fn get_path_independent_key(key: &str, with_proj: bool) -> String {
    let key_ary: Vec<&str> = key.split('/').collect();
    let length = key_ary.len();
    if with_proj {
        if length < 3 {
            return key.to_string();
        }
        return format!("{}/{}", key_ary[length - 2], key_ary[length - 1]);
    }
    if length < 2 {
        return key.to_string();
    }
    key_ary[length - 1].to_string()
}

/// Go `isAlreadyComputed`: was this quick range period already computed?
fn is_already_computed(con: &PgConn, ctx: &Ctx, key: &str, sdt: &str) -> bool {
    let key = get_path_independent_key(key, true);
    let dt = gotime::time_parse_any(sdt);
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!(
            "select 1 from gha_computed where metric = {} and dt = {}",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::from(key), SqlArg::from(dt)],
    );
    let mut i: i64 = 0;
    while rows.next() {
        fatal_on_pg_err(rows.scan(&mut [&mut i]));
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    i > 0
}

/// Go `setAlreadyComputed`: mark a quick range period as computed.
fn set_already_computed(con: &PgConn, ctx: &Ctx, key: &str, sdt: &str) {
    let key = get_path_independent_key(key, true);
    let dt = gotime::time_parse_any(sdt);
    exec_sql_with_err(
        con,
        ctx,
        &insert_ignore(&format!("into gha_computed(metric, dt) {}", n_values(2))),
        &[SqlArg::from(key), SqlArg::from(dt)],
    );
}

/// Go `setLastComputed`: upsert the `gha_last_computed` row of this metric
/// and period.
fn set_last_computed(con: &PgConn, ctx: &Ctx, metric: &str, interval_abbr: &str, g: &Globals) {
    let key = format!(
        "{} {}",
        get_path_independent_key(metric, false).replace(".sql", ""),
        interval_abbr
    );
    let took_dur = g.start.elapsed();
    let now = g.start_dt + devstatscode::chrono::Duration::from_std(took_dur).unwrap_or_default();
    let took_ms = took_dur.as_millis() as i64;
    let took_str = gotime::format_go_duration(took_dur);
    exec_sql_with_err(
        con,
        ctx,
        "insert into gha_last_computed(metric, dt, start_dt, took, took_as_str, command) \
         values($1, $2, $3, $4, $5, $6) \
         on conflict(metric) do update set \
         dt = $7, start_dt = $8, took = $9, took_as_str = $10, command = $11 \
         where gha_last_computed.metric = $12",
        &[
            SqlArg::from(key.clone()),
            SqlArg::from(now),
            SqlArg::from(g.start_dt),
            SqlArg::Int(took_ms),
            SqlArg::from(took_str.clone()),
            SqlArg::from(g.cmd.clone()),
            SqlArg::from(now),
            SqlArg::from(g.start_dt),
            SqlArg::Int(took_ms),
            SqlArg::from(took_str),
            SqlArg::from(g.cmd.clone()),
            SqlArg::from(key),
        ],
    );
}

/// Go `handleSeriesDrop`: drop the `drop:` tables (once) when
/// `GHA2DB_ENABLE_METRICS_DROP` is set.
fn handle_series_drop(ctx: &Ctx, con: &PgConn, cfg: &CalcMetricData) {
    if cfg.hist && !cfg.drop.is_empty() {
        fatalf!(
            "you cannot use drop series property on histogram metrics: &{}",
            cfg.go_string()
        );
    }
    if !ctx.enable_metrics_drop {
        return;
    }
    for table in &cfg.drop {
        if !ctx.skip_tsdb && table_exists(con, ctx, table) {
            if ctx.debug >= 0 {
                printf!("Truncating table {}\n", table);
            }
            // Go `%+v` of an error value is its `Error()` text
            if let Err(err) = exec_sql(con, ctx, &format!("drop table {table}"), &[]) {
                printf!("warning: failed dropping table '{}': {}\n", table, err);
            }
        }
    }
}

/// Deferred `setLastComputed` (Go `defer`): runs when the guard is dropped
/// and also when the thread dies through a fatal error. The interval
/// abbreviation is shared because Go's closure sees the `range:` value
/// normalised later by `calcHistogram`.
fn defer_last_computed(
    ctx: &Ctx,
    sqlc: &Arc<PgConn>,
    sql_file: &str,
    interval_abbr: &Arc<Mutex<String>>,
    g: &Globals,
) -> Option<error::Defer> {
    if ctx.skip_tsdb {
        return None;
    }
    let sqlc = Arc::clone(sqlc);
    let ctx = ctx.copy_context();
    let sql_file = sql_file.to_string();
    let interval_abbr = Arc::clone(interval_abbr);
    let g = g.clone();
    Some(error::defer(move || {
        let abbr = interval_abbr.lock().unwrap().clone();
        set_last_computed(&sqlc, &ctx, &sql_file, &abbr, &g)
    }))
}

/// Clear the existing histogram data of one series (`s<series>` table or
/// the `s<merge_series>` table's `series`) for the period.
fn clear_histogram(
    ctx: &Ctx,
    sqlc: &PgConn,
    cfg: &CalcMetricData,
    series: &str,
    interval_abbr: &str,
    first_wording: bool,
) {
    let (verb, prep) = if first_wording {
        ("Dropped data from", "table")
    } else {
        ("Dropped from", "table")
    };
    if cfg.merge_series.is_empty() {
        let table = format!("s{series}");
        if table_exists(sqlc, ctx, &table) {
            exec_sql_with_err(
                sqlc,
                ctx,
                &format!("delete from \"{}\" where period = {}", table, n_value(1)),
                &[SqlArg::from(interval_abbr)],
            );
            if ctx.debug > 0 {
                printf!("{verb} {prep} {} with {} period\n", table, interval_abbr);
            }
        }
    } else {
        let table = format!("s{}", cfg.merge_series);
        if table_exists(sqlc, ctx, &table) {
            exec_sql_with_err(
                sqlc,
                ctx,
                &format!(
                    "delete from \"{}\" where series = {} and period = {}",
                    table,
                    n_value(1),
                    n_value(2)
                ),
                &[SqlArg::from(series), SqlArg::from(interval_abbr)],
            );
            if ctx.debug > 0 {
                printf!(
                    "{verb} {prep} {} with {} series and {} period\n",
                    table,
                    series,
                    interval_abbr
                );
            }
        }
    }
}

/// Go `calcHistogram`: one query for the histogram period, written as
/// points with decreasing fake times starting at 2012-07-01.
#[allow(clippy::too_many_arguments)]
fn calc_histogram(
    ctx: &Ctx,
    series_name_or_func: &str,
    sql_file: &str,
    sql_query: &str,
    exclude_bots: &str,
    interval: &str,
    interval_abbr: &str,
    n_intervals: i64,
    cfg: &CalcMetricData,
    g: &Globals,
) {
    let sqlc = Arc::new(pg::pg_conn(ctx));
    let abbr_cell = Arc::new(Mutex::new(interval_abbr.to_string()));
    let _deferred = defer_last_computed(ctx, &sqlc, sql_file, &abbr_cell, g);
    let mut pts: TSPoints = Vec::new();
    let mut sql_query = sql_query.to_string();
    let mut interval_abbr = interval_abbr.to_string();

    printf!(
        "calc_metric.go: Histogram running interval '{},{}' n:{} anno:{} past:{} multi:{}\n",
        interval,
        interval_abbr,
        n_intervals,
        cfg.annotations_ranges,
        cfg.skip_past,
        cfg.multivalue
    );

    let mut qr_dt: Option<String> = None;
    if cfg.annotations_ranges {
        let quick_ranges = get_tag_values(&sqlc, ctx, "quick_ranges", "quick_ranges_data");
        if ctx.debug > 0 {
            printf!("Quick ranges: {}\n", gofmt::slice(&quick_ranges));
        }
        let mut found = false;
        for data in &quick_ranges {
            let ary: Vec<&str> = data.split(';').collect();
            let sfx = ary[0];
            if interval_abbr == sfx {
                found = true;
                printf!("Found quick range: {}\n", gofmt::slice(&ary));
                let period = ary[1];
                let from = ary[2];
                let to = ary[3];
                if cfg.skip_past && period.is_empty() {
                    let dt_to = gotime::time_parse_any(to);
                    let prev_hour = gotime::prev_hour_start(Utc::now());
                    if dt_to < prev_hour && is_already_computed(&sqlc, ctx, sql_file, to) {
                        printf!(
                            "Skipping past quick range: {}-{} (already computed)\n",
                            from,
                            to
                        );
                        // Go's defer: bookkeeping first, then the connection is closed
                        drop(_deferred);
                        sqlc.close();
                        return;
                    }
                }
                let (q, s_hours) =
                    gostring::prepare_quick_range_query(&sql_query, period, from, to);
                sql_query = q
                    .replace("{{exclude_bots}}", exclude_bots)
                    .replace("{{range}}", &s_hours)
                    .replace("{{project_scale}}", &cfg.project_scale)
                    .replace("{{rnd}}", &gostring::rand_string());
                if period.is_empty() {
                    let dt_to = gotime::time_parse_any(to);
                    let prev_hour = gotime::prev_hour_start(Utc::now());
                    if dt_to < prev_hour {
                        qr_dt = Some(to.to_string());
                    }
                }
                break;
            }
        }
        if !found {
            fatalf!(
                "quick range not found: '{}' known quick ranges: {}",
                interval_abbr,
                gofmt::slice(&quick_ranges)
            );
        }
    } else if let Some(spec) = interval_abbr.strip_prefix("range:") {
        let ary: Vec<&str> = spec.split(',').collect();
        if ary.len() != 2 {
            fatalf!("range should be specified as 'range:YYYY-MM-DD,YYYY-MM-DD'\n");
        }
        let (from, to, period) = (ary[0], ary[1], "");
        let (q, s_hours) = gostring::prepare_quick_range_query(&sql_query, period, from, to);
        let from = gotime::to_ymdhms_date(gotime::time_parse_any(from));
        let to = gotime::to_ymdhms_date(gotime::time_parse_any(to));
        interval_abbr = format!("range:{from},{to}");
        *abbr_cell.lock().unwrap() = interval_abbr.clone();
        sql_query = q
            .replace("{{exclude_bots}}", exclude_bots)
            .replace("{{range}}", &s_hours)
            .replace("{{project_scale}}", &cfg.project_scale)
            .replace("{{rnd}}", &gostring::rand_string());
    } else {
        let mut db_interval = format!("{n_intervals} {interval}");
        if interval == consts::QUARTER {
            db_interval = format!("{} month", n_intervals * 3);
        }
        let s_hours = gotime::interval_hours(&db_interval);
        sql_query = sql_query
            .replace("{{period}}", &db_interval)
            .replace("{{n}}", &format!("{n_intervals}.0"))
            .replace("{{rnd}}", &gostring::rand_string())
            .replace("{{exclude_bots}}", exclude_bots)
            .replace("{{range}}", &s_hours)
            .replace("{{project_scale}}", &cfg.project_scale);
    }

    let mut rows = query_sql_with_err(&sqlc, ctx, &sql_query, &[]);
    let n_columns = rows.columns().len();
    if n_columns == 2 {
        if !ctx.skip_tsdb {
            clear_histogram(ctx, &sqlc, cfg, series_name_or_func, &interval_abbr, true);
        }
        let mut tm = gotime::time_parse_any("2012-07-01");
        let mut row_count = 0;
        let mut name = String::new();
        let mut value = 0.0_f64;
        while rows.next() {
            fatal_on_pg_err(rows.scan(&mut [&mut name, &mut value]));
            if ctx.debug > 0 {
                printf!(
                    "hist {}, {} {} -> {}, {}\n",
                    series_name_or_func,
                    n_intervals,
                    interval,
                    name,
                    gofmt::float(value)
                );
            }
            let mut fields = Fields::new();
            fields.insert("name".to_string(), FieldValue::Str(name.clone()));
            fields.insert("value".to_string(), FieldValue::Float(value));
            add_ts_point(
                ctx,
                &mut pts,
                new_ts_point(
                    ctx,
                    series_name_or_func,
                    &interval_abbr,
                    None,
                    Some(&fields),
                    tm,
                    false,
                ),
            );
            row_count += 1;
            tm -= devstatscode::chrono::Duration::hours(1);
        }
        if ctx.debug > 0 {
            printf!(
                "hist {}, {} {}: {} rows\n",
                series_name_or_func,
                n_intervals,
                interval,
                row_count
            );
        }
        fatal_on_pg_err(rows.err());
    } else if n_columns >= 3 {
        let mut series_to_clear: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
        let start_tm = gotime::time_parse_any("2012-07-01");
        let mut next_tm = |name: &str| -> DateTime<Utc> {
            let tm = match series_to_clear.get(name) {
                Some(tm) => *tm - devstatscode::chrono::Duration::hours(1),
                None => start_tm,
            };
            series_to_clear.insert(name.to_string(), tm);
            tm
        };
        while rows.next() {
            let values = rows.values().to_vec();
            let name = raw_string(&values[0]);
            let mut names = name_for_metrics_row(
                cfg,
                series_name_or_func,
                &name,
                cfg.multivalue,
                false,
                false,
            );
            if ctx.debug > 0 {
                printf!("nameForMetricsRow: {} -> {}\n", name, gofmt::slice(&names));
            }
            // multivalue will return names as [ser_name1;a,b,c]
            let mut value_names: Vec<String> = Vec::new();
            if cfg.multivalue {
                if names.len() > 1 {
                    fatalf!(
                        "should return only one series name when using multi value, got: {}",
                        gofmt::slice(&names)
                    );
                }
                let names_ary: Vec<&str> = names[0].split(';').collect();
                if names_ary.len() > 1 {
                    value_names = names_ary[1].split(',').map(str::to_string).collect();
                }
                names = vec![names_ary[0].to_string()];
            }
            let n_names = names.len();
            if cfg.multivalue {
                let mut fields = Fields::new();
                let name = names[0].clone();
                for (i, value_data) in value_names.iter().enumerate() {
                    let va: Vec<&str> = value_data.split(':').collect();
                    let value_name = va[0];
                    let value_type = va[1];
                    let raw = raw_string(&values[i + 1]);
                    match value_type {
                        "s" => {
                            fields.insert(value_name.to_string(), FieldValue::Str(raw));
                        }
                        "f" => {
                            let v = fatal_on_err(gotime::parse_go_float(&raw));
                            fields.insert(value_name.to_string(), FieldValue::Float(v));
                        }
                        _ => fatalf!(
                            "unknown data type: {} ({}), i: {}, valuedata: {}",
                            value_type,
                            value_data,
                            i,
                            value_data
                        ),
                    }
                }
                let tm = next_tm(&name);
                add_ts_point(
                    ctx,
                    &mut pts,
                    new_ts_point(ctx, &name, &interval_abbr, None, Some(&fields), tm, false),
                );
            } else if n_names > 0 {
                if cfg.custom_data {
                    // seriesName + N * (name, dt_value, f_value, s_value) 4-tuples
                    for (i, name) in names.iter().enumerate() {
                        let s_value = raw_string(&values[4 * i + 1]);
                        let dt_value = gotime::time_parse_any(&raw_string(&values[4 * i + 2]));
                        let f_value = float_or_zero(&raw_string(&values[4 * i + 3]));
                        let s2_value = raw_string(&values[4 * i + 4]);
                        if ctx.debug > 0 {
                            printf!(
                                "hist {}, {} {} -> {}, {}, {}, {}\n",
                                name,
                                n_intervals,
                                interval,
                                s_value,
                                gofmt::time(dt_value),
                                gofmt::float(f_value),
                                s2_value
                            );
                        }
                        let tm = next_tm(name);
                        let mut fields = Fields::new();
                        fields.insert("name".to_string(), FieldValue::Str(s_value));
                        fields.insert("value".to_string(), FieldValue::Float(f_value));
                        fields.insert("str".to_string(), FieldValue::Str(s2_value));
                        fields.insert("dt".to_string(), FieldValue::Time(dt_value));
                        add_ts_point(
                            ctx,
                            &mut pts,
                            new_ts_point(ctx, name, &interval_abbr, None, Some(&fields), tm, false),
                        );
                    }
                } else {
                    // seriesName + N * (name, value) pairs
                    for (i, name) in names.iter().enumerate() {
                        let s_value = raw_string(&values[2 * i + 1]);
                        let f_value = float_or_zero(&raw_string(&values[2 * i + 2]));
                        if ctx.debug > 0 {
                            printf!(
                                "hist {}, {} {} -> {}, {}\n",
                                name,
                                n_intervals,
                                interval,
                                s_value,
                                gofmt::float(f_value)
                            );
                        }
                        let tm = next_tm(name);
                        let mut fields = Fields::new();
                        fields.insert("name".to_string(), FieldValue::Str(s_value));
                        fields.insert("value".to_string(), FieldValue::Float(f_value));
                        add_ts_point(
                            ctx,
                            &mut pts,
                            new_ts_point(ctx, name, &interval_abbr, None, Some(&fields), tm, false),
                        );
                    }
                }
            }
        }
        fatal_on_pg_err(rows.err());
        if !series_to_clear.is_empty() && !ctx.skip_tsdb {
            for series in series_to_clear.keys() {
                clear_histogram(ctx, &sqlc, cfg, series, &interval_abbr, false);
            }
        }
    }
    fatal_on_pg_err(rows.close());
    if !ctx.skip_tsdb {
        if cfg.custom_data_unique_time {
            make_ts_points_unique_times(ctx, &mut pts);
        }
        write_ts_points(ctx, &sqlc, &pts, &cfg.merge_series, &[], None);
        if let Some(qr_dt) = &qr_dt {
            set_already_computed(&sqlc, ctx, sql_file, qr_dt);
        }
    } else if ctx.debug > 0 {
        printf!("Skipping series write\n");
    }
    drop(_deferred);
    sqlc.close();
}

/// Go `calcMetric`: the program body after argument parsing.
fn calc_metric(
    series_name_or_func: &str,
    sql_file: &str,
    from: &str,
    to: &str,
    interval_abbr: &str,
    cfg: &CalcMetricData,
    g: &Globals,
) {
    if interval_abbr.is_empty() {
        fatalf!("you need to define period");
    }
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    let bytes = fatal_on_err(io::read_file(&ctx, sql_file));
    let sql_query = String::from_utf8_lossy(&bytes).into_owned();
    let bytes = fatal_on_err(io::read_file(
        &ctx,
        &format!("{data_prefix}util_sql/exclude_bots.sql"),
    ));
    let exclude_bots = String::from_utf8_lossy(&bytes).into_owned();

    let mut allow_unknowns = cfg.annotations_ranges;
    if !allow_unknowns {
        allow_unknowns = interval_abbr.starts_with("range:");
    }
    let funcs = gotime::get_interval_functions(interval_abbr, allow_unknowns);
    let (interval, n_intervals) = (funcs.interval, funcs.n);

    if cfg.hist {
        calc_histogram(
            &ctx,
            series_name_or_func,
            sql_file,
            &sql_query,
            &exclude_bots,
            interval,
            interval_abbr,
            n_intervals,
            cfg,
            g,
        );
        return;
    }

    let sqlc = Arc::new(pg::pg_conn(&ctx));
    let abbr_cell = Arc::new(Mutex::new(interval_abbr.to_string()));
    let _deferred = defer_last_computed(&ctx, &sqlc, sql_file, &abbr_cell, g);

    let hll_empty: Vec<u8> = if cfg.hll {
        get_hll_default()
    } else {
        Vec::new()
    };

    // Unknown intervals only reach here with `annotations_ranges`/`range:`
    // (allowed above) — Go then calls nil function values and panics.
    let interval_start = funcs.start.unwrap_or_else(|| {
        panic!("runtime error: invalid memory address or nil pointer dereference")
    });
    let next_interval_start = funcs.next.unwrap_or_else(|| unreachable!());
    let prev_interval_start = funcs.prev.unwrap_or_else(|| unreachable!());

    let d_from = interval_start(gotime::time_parse_any(from));
    let d_to = next_interval_start(gotime::time_parse_any(to));

    let thr_n = threads::get_threads_num(&mut ctx);

    printf!(
        "calc_metric.go: {}: Running (on {} CPUs): {} - {} with interval {} {}, descriptions '{}', \
         multivalue: {}, escape_value_name: {}, skip_escape_series_name: {}, custom_data: {}, \
         custom_data_unique_time: {}\n",
        sql_file,
        thr_n,
        gofmt::time(d_from),
        gofmt::time(d_to),
        n_intervals,
        interval,
        cfg.desc,
        cfg.multivalue,
        cfg.escape_value_name,
        cfg.skip_escape_series_name,
        cfg.custom_data,
        cfg.custom_data_unique_time
    );

    // Distribute the ranges round-robin over the threads: (dt, from, to).
    let mut per_thread: Vec<Ranges> = Vec::new();
    let mut dt = d_from;
    let mut i = 0usize;
    while dt < d_to {
        let n_dt = next_interval_start(dt);
        let p_dt = if n_intervals <= 1 {
            dt
        } else {
            gotime::add_n_intervals(
                dt,
                1 - n_intervals,
                next_interval_start,
                prev_interval_start,
            )
        };
        let t = i % thr_n;
        if per_thread.len() < t + 1 {
            per_thread.push(Vec::new());
        }
        per_thread[t].push((dt, p_dt, n_dt));
        dt = n_dt;
        i += 1;
    }
    if n_intervals > 1 {
        rng::shuffle(&mut per_thread);
    }
    let ldt = per_thread.len();
    if thr_n > 1 {
        let mutex = Mutex::new(());
        thread::scope(|s| {
            for ranges in per_thread.iter().take(thr_n.min(ldt)) {
                let ctx = &ctx;
                let mutex = &mutex;
                let hll_empty = &hll_empty;
                let sql_query = &sql_query;
                let exclude_bots = &exclude_bots;
                s.spawn(move || {
                    calc_range(
                        ctx,
                        series_name_or_func,
                        sql_query,
                        exclude_bots,
                        interval_abbr,
                        cfg,
                        n_intervals,
                        ranges,
                        hll_empty,
                        Some(mutex),
                    );
                });
            }
        });
    } else {
        printf!("Using single threaded version\n");
        if ldt > 0 {
            calc_range(
                &ctx,
                series_name_or_func,
                &sql_query,
                &exclude_bots,
                interval_abbr,
                cfg,
                n_intervals,
                &per_thread[0],
                &hll_empty,
                None,
            );
        }
    }
    printf!("All done.\n");
    drop(_deferred);
    sqlc.close();
}

/// Go `main`: argument parsing (`series_name_or_func some.sql from to
/// period [options]`).
fn main() {
    error::exit_on_panic();
    let start = Instant::now();
    let start_dt = Local::now();
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        printf!(
            "Required series name, SQL file name, from, to, period \
             [series_name_or_func some.sql '2015-08-03' '2017-08-21' h|d|w|m|q|y \
             [hist,desc:time_diff_as_string,multivalue,escape_value_name,annotations_ranges,\
             skip_past,merge_series:name,custom_data,custom_data_unique_time,drop:table1;table2,\
             project_scale:float]]\n"
        );
        printf!(
            "Series name (series_name_or_func) will become exact series name if \
             query return just single numeric value\n"
        );
        printf!(
            "For queries returning multiple rows 'series_name_or_func' will be used as function that\n"
        );
        printf!("receives data row and period and returns name and value(s) for it\n");
        printf!(
            "Example run: GHA2DB_QOUT=1 GHA2DB_PROJECT=istio PG_DB=istio calc_metric \
             multi_row_single_column /etc/gha2db/metrics/istio/project_developer_stats.sql \
             '2026-02-12 7' '2026-02-12 19' d hist,merge_series:hdev,annotations_ranges,skip_past\n"
        );
        std::process::exit(1);
    }
    let mut cfg = CalcMetricData {
        project_scale: "1.0".to_string(),
        ..CalcMetricData::default()
    };
    if args.len() > 6 {
        let mut opt_map: BTreeMap<String, String> = BTreeMap::new();
        for opt in args[6].split(',') {
            let opt_arr: Vec<&str> = opt.split(':').collect();
            let opt_name = opt_arr[0];
            let opt_val = if opt_arr.len() > 1 { opt_arr[1] } else { "" };
            if opt_name == "series_name_map" {
                opt_map.insert(opt_name.to_string(), opt_arr[1..].join(":"));
            } else {
                opt_map.insert(opt_name.to_string(), opt_val.to_string());
            }
        }
        cfg.hist = opt_map.contains_key("hist");
        cfg.multivalue = opt_map.contains_key("multivalue");
        cfg.escape_value_name = opt_map.contains_key("escape_value_name");
        cfg.skip_escape_series_name = opt_map.contains_key("skip_escape_series_name");
        cfg.annotations_ranges = opt_map.contains_key("annotations_ranges");
        cfg.skip_past = opt_map.contains_key("skip_past");
        if let Some(d) = opt_map.get("desc") {
            cfg.desc = d.clone();
        }
        if let Some(d) = opt_map.get("drop") {
            cfg.drop = d.split(';').map(str::to_string).collect();
        }
        if let Some(ms) = opt_map.get("merge_series") {
            cfg.merge_series = ms.clone();
        }
        cfg.custom_data = opt_map.contains_key("custom_data");
        cfg.custom_data_unique_time = opt_map.contains_key("custom_data_unique_time");
        if let Some(snm) = opt_map.get("series_name_map") {
            cfg.series_name_map = gomap::map_from_string(snm);
        }
        if let Some(pss) = opt_map.get("project_scale") {
            if let Ok(ps) = gotime::parse_go_float(pss) {
                if ps >= 0.0 {
                    cfg.project_scale = go_percent_f(ps);
                }
            }
        }
        cfg.hll = opt_map.contains_key("hll");
    }
    if cfg.custom_data_unique_time && !cfg.custom_data {
        fatalf!("custom_data_unique_time requires custom_data");
    }
    let g = Globals {
        start_dt,
        start,
        cmd: args[1..].join(" "),
    };
    printf!("{}...\n", args[2]);
    printf!("Start({})\n", args[1..].join(" € "));
    calc_metric(&args[1], &args[2], &args[3], &args[4], &args[5], &cfg, &g);
    printf!(
        "Time({}): {}\n",
        args[1..].join(" € "),
        gotime::format_go_duration(start.elapsed())
    );
}

/// Go `fmt.Sprintf("%f", f)`: six decimals, `+Inf` for infinity.
fn go_percent_f(f: f64) -> String {
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    format!("{f:.6}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CalcMetricData {
        CalcMetricData {
            project_scale: "1.0".to_string(),
            ..CalcMetricData::default()
        }
    }

    #[test]
    fn names_multi_row_single_column() {
        let c = cfg();
        assert_eq!(
            multi_row_single_column(&c, "pref,Some Name!", false, false, false),
            vec!["prefsomename!".to_string()]
        );
        assert_eq!(
            multi_row_single_column(&c, "pref,Some Name!", false, false, true),
            vec!["prefSome Name!".to_string()]
        );
        assert_eq!(
            multi_row_single_column(&c, "pref,Some Name!", true, false, false),
            vec!["pref;Some Name!".to_string()]
        );
        assert_eq!(
            multi_row_single_column(&c, "pref,Some Name!", true, true, false),
            vec!["pref;somename!".to_string()]
        );
        assert_eq!(
            multi_row_single_column(&c, "pref,Multi`Ser Ies", true, false, false),
            vec!["prefseries;Multi".to_string()]
        );
        assert_eq!(
            multi_row_single_column(&c, "pref,Multi`Ser Ies", true, false, true),
            vec!["prefSer Ies;Multi".to_string()]
        );
        assert!(multi_row_single_column(&c, ",x", false, false, false).is_empty());
        assert!(multi_row_single_column(&c, "pref,-_.", false, false, false).is_empty());
        let mut m = cfg();
        m.series_name_map = Some(BTreeMap::from([("abc".to_string(), "xyz".to_string())]));
        assert_eq!(
            multi_row_single_column(&m, "p,ABC", false, false, false),
            vec!["pxyz".to_string()]
        );
    }

    #[test]
    fn names_multi_row_multi_column() {
        let c = cfg();
        assert_eq!(
            multi_row_multi_column(&c, "p;Row Name;a,b", false, false, false),
            vec!["prownamea".to_string(), "prownameb".to_string()]
        );
        assert_eq!(
            multi_row_multi_column(&c, "p;Row Name;a,b", true, false, false),
            vec!["pa;Row Name".to_string(), "pb;Row Name".to_string()]
        );
        assert_eq!(
            multi_row_multi_column(&c, "p;Row Name`Ser;a", true, true, false),
            vec!["psera;rowname".to_string()]
        );
        assert!(multi_row_multi_column(&c, ";x;a", false, false, false).is_empty());
        assert!(multi_row_multi_column(&c, "p;(-);a", false, false, false).is_empty());
        assert_eq!(
            name_for_metrics_row(&c, "single_row_multi_column", "a,b,c", false, false, false),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn go_struct_rendering() {
        let mut c = cfg();
        assert_eq!(
            c.go_string(),
            "{hist:false multivalue:false escapeValueName:false skipEscapeSeriesName:false \
             annotationsRanges:false skipPast:false desc: mergeSeries: customData:false \
             customDataUniqueTime:false seriesNameMap:map[] drop:[] projectScale:1.0 hll:false}"
        );
        c.drop = vec!["sa".to_string(), "sb".to_string()];
        c.series_name_map = Some(BTreeMap::from([("k".to_string(), "v".to_string())]));
        assert!(c
            .go_string()
            .contains("seriesNameMap:map[k:v] drop:[sa sb]"));
    }

    #[test]
    fn path_independent_keys() {
        assert_eq!(
            get_path_independent_key("/etc/gha2db/metrics/kubernetes/key.sql", true),
            "kubernetes/key.sql"
        );
        assert_eq!(
            get_path_independent_key("./metrics/kubernetes/key.sql", false),
            "key.sql"
        );
        assert_eq!(get_path_independent_key("a/b", true), "a/b");
        assert_eq!(get_path_independent_key("key.sql", false), "key.sql");
    }

    #[test]
    fn project_scale_formatting() {
        assert_eq!(go_percent_f(1.0), "1.000000");
        assert_eq!(go_percent_f(0.5), "0.500000");
        assert_eq!(go_percent_f(f64::INFINITY), "+Inf");
        assert_eq!(float_or_zero("1.5"), 1.5);
        assert_eq!(float_or_zero(""), 0.0);
        assert_eq!(float_or_zero("abc"), 0.0);
    }

    #[test]
    fn hll_default_is_hll_empty_text() {
        assert_eq!(get_hll_default(), b"\\x118b7f".to_vec());
    }
}
