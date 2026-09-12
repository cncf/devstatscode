//! `gha2db_sync` — Rust port of `cmd/gha2db_sync/gha2db_sync.go`.
//!
//! The hourly sync of one project (run by `devstats` for every project, or
//! by hand with `GHA2DB_PROJECT=<name>` / `gha2db_sync org[,org…] [repo…]`):
//!
//! 1. the start dates: the newest `gha_parsed` hour + 1 (or
//!    `GHA2DB_STARTDT` with `GHA2DB_STARTDT_FORCE`) for the GitHub archives and
//!    the newest point of the `s<GHA2DB_LASTSERIES>` series for the TSDB
//!    metrics;
//! 2. unless `GHA2DB_SKIPPDB`: old DB logs are cleared, then `gha2db`
//!    (the new GHA hours), `get_repos` (commits of the new hours), `ghapi2db`
//!    (non fatal) and `structure` (post-process SQLs) are run;
//! 3. unless `GHA2DB_SKIPTSDB`: `tags` (once a day, or with
//!    `GHA2DB_RESETTSDB`), `columns` after tags, `annotations` (once a day),
//!    then every metric of `metrics.yaml` for every period/aggregate that is
//!    due at this hour is calculated with `calc_metric` (histograms are
//!    collected and run at the end, possibly in parallel), then `columns`
//!    (once a day / `GHA2DB_RUN_COLUMNS`);
//! 4. unless `GHA2DB_SKIPPDB` / `GHA2DB_SKIPVARS`: `vars` with
//!    `GHA2DB_VARS_FN_YAML` (`sync_vars.yaml`).
//!
//! Environment, arguments passed to the sub-commands, output and exit codes
//! are those of the Go program; see `rust/README.md` for the few documented
//! differences (yaml error wording, Go's random map/metric orders).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use devstatscode::chrono::{DateTime, FixedOffset, Local, TimeZone, Timelike, Utc};
use devstatscode::exec::exec_command;
use devstatscode::pg::api::{fatal_on_pg_err, get_tag_values, query_row_sql, table_exists};
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    fatal_on_err, fatal_on_error, fatalf, gofmt, io, log, pg, printf, projects, rng, signal,
    threads, time as gotime, Ctx,
};
use serde::Deserialize;

/// `metrics.yaml`: the list of metrics to evaluate.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
struct Metrics {
    #[serde(deserialize_with = "yde::seq")]
    metrics: Vec<Metric>,
}

/// One metric of `metrics.yaml` (Go `metric`, yaml.v2 decoding rules).
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
struct Metric {
    #[serde(deserialize_with = "yde::string")]
    name: String,
    #[serde(deserialize_with = "yde::string")]
    periods: String,
    #[serde(rename = "series_name_or_func", deserialize_with = "yde::string")]
    series_name_or_func: String,
    #[serde(rename = "sql", deserialize_with = "yde::string")]
    metric_sql: String,
    #[serde(rename = "sqls", deserialize_with = "yde::opt_str_seq")]
    metric_sqls: Option<Vec<String>>,
    #[serde(rename = "add_period_to_name", deserialize_with = "yde::boolean")]
    add_period_to_name: bool,
    #[serde(deserialize_with = "yde::boolean")]
    histogram: bool,
    #[serde(deserialize_with = "yde::string")]
    aggregate: String,
    #[serde(deserialize_with = "yde::string")]
    skip: String,
    #[serde(deserialize_with = "yde::string")]
    desc: String,
    #[serde(rename = "multi_value", deserialize_with = "yde::boolean")]
    multi_value: bool,
    #[serde(rename = "escape_value_name", deserialize_with = "yde::boolean")]
    escape_value_name: bool,
    #[serde(rename = "skip_escape_series_name", deserialize_with = "yde::boolean")]
    skip_escape_series_name: bool,
    #[serde(rename = "annotations_ranges", deserialize_with = "yde::boolean")]
    annotations_ranges: bool,
    #[serde(rename = "merge_series", deserialize_with = "yde::string")]
    merge_series: String,
    #[serde(rename = "custom_data", deserialize_with = "yde::boolean")]
    custom_data: bool,
    #[serde(rename = "custom_data_unique_time", deserialize_with = "yde::boolean")]
    custom_data_unique_time: bool,
    #[serde(rename = "start_from", deserialize_with = "yde::opt_time")]
    start_from: Option<DateTime<FixedOffset>>,
    #[serde(rename = "last_hours", deserialize_with = "yde::int")]
    last_hours: i64,
    #[serde(rename = "series_name_map", deserialize_with = "yde::str_map")]
    series_name_map: BTreeMap<String, String>,
    #[serde(rename = "env", deserialize_with = "yde::str_map")]
    env_map: BTreeMap<String, String>,
    #[serde(deserialize_with = "yde::boolean")]
    disabled: bool,
    #[serde(deserialize_with = "yde::string")]
    drop: String,
    #[serde(deserialize_with = "yde::string")]
    project: String,
    #[serde(rename = "allow_fail", deserialize_with = "yde::boolean")]
    allow_fail: bool,
    #[serde(rename = "wait_after_fail", deserialize_with = "yde::int")]
    wait_after_fail: i64,
    #[serde(deserialize_with = "yde::boolean")]
    hll: bool,
    #[serde(rename = "always_recalc", deserialize_with = "yde::boolean")]
    always_recalc: bool,
}

impl Metric {
    /// Go `%+v` of the metric (the `String()` method of the Go program:
    /// field names and values, pointers printed as values / `<nil>`).
    fn go_string(&self) -> String {
        let sqls = match &self.metric_sqls {
            None => "<nil>".to_string(),
            Some(v) => gofmt::slice(v),
        };
        let start_from = match &self.start_from {
            None => "<nil>".to_string(),
            Some(t) => gofmt::time(*t),
        };
        format!(
            "{{Name:{} Periods:{} SeriesNameOrFunc:{} MetricSQL:{} MetricSQLs:{} AddPeriodToName:{} Histogram:{} \
             Aggregate:{} Skip:{} Desc:{} MultiValue:{} EscapeValueName:{} SkipEscapeSeriesName:{} \
             AnnotationsRanges:{} MergeSeries:{} CustomData:{} CustomDataUniqueTime:{} StartFrom:{} \
             LastHours:{} SeriesNameMap:{} EnvMap:{} Disabled:{} Drop:{} Project:{} AllowFail:{} \
             WaitAfterFail:{} HLL:{} AlwaysRecalc:{}}}",
            self.name,
            self.periods,
            self.series_name_or_func,
            self.metric_sql,
            sqls,
            self.add_period_to_name,
            self.histogram,
            self.aggregate,
            self.skip,
            self.desc,
            self.multi_value,
            self.escape_value_name,
            self.skip_escape_series_name,
            self.annotations_ranges,
            self.merge_series,
            self.custom_data,
            self.custom_data_unique_time,
            start_from,
            self.last_hours,
            gofmt::map(&self.series_name_map),
            gofmt::map(&self.env_map),
            self.disabled,
            self.drop,
            self.project,
            self.allow_fail,
            self.wait_after_fail,
            self.hll,
            self.always_recalc,
        )
    }
}

/// How Go would print the zone of a `time.Time` value with `%v`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Zone {
    /// A named zone: `UTC` for parsed / default dates and `time.Now()` in a
    /// UTC environment (`+0000 UTC`).
    Named,
    /// lib/pq's zone-less timestamps (`+0000 +0000`) and yaml offsets.
    Nameless,
}

/// A `time.Time` as the Go program holds it: the instant plus what is needed
/// to print it like Go's `%v` (zone rendering, monotonic clock reading for
/// `time.Now()`-derived values).
#[derive(Debug, Clone, Copy)]
struct GoTime {
    t: DateTime<FixedOffset>,
    zone: Zone,
    /// Nanoseconds of the monotonic reading (`m=+1.5` / `m=-3599.9`).
    mono: Option<i128>,
}

impl GoTime {
    /// A date from the context / yaml (UTC).
    fn utc(t: DateTime<Utc>) -> Self {
        GoTime {
            t: t.fixed_offset(),
            zone: Zone::Named,
            mono: None,
        }
    }

    /// A timestamp read from the database (zone-less).
    fn db(t: DateTime<FixedOffset>) -> Self {
        GoTime {
            t,
            zone: Zone::Nameless,
            mono: None,
        }
    }

    /// A yaml timestamp: UTC when it had no offset, a nameless fixed zone
    /// otherwise (both render identically through [`gofmt::time`]).
    fn parsed(t: DateTime<FixedOffset>) -> Self {
        GoTime {
            t,
            zone: if t.offset().local_minus_utc() == 0 {
                Zone::Named
            } else {
                Zone::Nameless
            },
            mono: None,
        }
    }

    /// `time.Now()` (local time with the monotonic reading).
    fn now() -> Self {
        let now = Local::now();
        GoTime {
            t: now.fixed_offset(),
            zone: Zone::Named,
            mono: Some(process_elapsed_nanos()),
        }
    }

    /// `time.Now().Add(-hours * time.Hour)`.
    fn now_minus_hours(hours: i64) -> Self {
        let mut t = GoTime::now();
        t.t -= devstatscode::chrono::Duration::hours(hours);
        t.mono = t.mono.map(|m| m - hours as i128 * 3_600_000_000_000);
        t
    }

    /// `lib.ToYMDHDate` (the time's own zone).
    fn ymdh(&self) -> String {
        gotime::to_ymdh_date(self.t)
    }

    /// Go `%v`.
    fn v(&self) -> String {
        let mut s = gofmt::time(self.t);
        if self.zone == Zone::Nameless && self.t.offset().local_minus_utc() == 0 {
            s = s.replace(" +0000 UTC", " +0000 +0000");
        }
        if let Some(m) = self.mono {
            let sign = if m < 0 { '-' } else { '+' };
            let a = m.unsigned_abs();
            s.push_str(&format!(
                " m={sign}{}.{:09}",
                a / 1_000_000_000,
                a % 1_000_000_000
            ));
        }
        s
    }

    /// The wall clock of this time as a UTC value — what Go's `HourStart` &
    /// co. produce from the local components (`time.Date(..., time.UTC)`).
    fn wall_as_utc(&self) -> DateTime<Utc> {
        Utc.from_utc_datetime(&self.t.naive_local())
    }
}

static PROCESS_START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn process_elapsed_nanos() -> i128 {
    PROCESS_START.get_or_init(Instant::now).elapsed().as_nanos() as i128
}

/// Go `%+v` of `ctx.ComputePeriods` (`map[string]map[bool]struct{}`).
fn compute_periods_string(cp: &Option<BTreeMap<String, BTreeSet<bool>>>) -> String {
    match cp {
        None => "map[]".to_string(),
        Some(m) => {
            let parts: Vec<String> = m
                .iter()
                .map(|(k, v)| {
                    let inner: Vec<String> = v.iter().map(|b| format!("{b}:{{}}")).collect();
                    format!("{k}:map[{}]", inner.join(" "))
                })
                .collect();
            format!("map[{}]", parts.join(" "))
        }
    }
}

/// Shuffle the metrics, keeping the one of `ctx.last_series` last (Go
/// `randomize`).
fn randomize(metrics: &mut [Metric], ctx: &Ctx) {
    printf!("Randomizing metrics calculation order\n");
    rng::shuffle(metrics);
    if metrics.is_empty() {
        return;
    }
    let last_i = metrics.len() - 1;
    if let Some(idx) = metrics
        .iter()
        .position(|m| m.series_name_or_func == ctx.last_series)
    {
        if idx != last_i {
            metrics.swap(idx, last_i);
        }
    }
}

/// Go `processEnvMap`: the environment a metric's `env:` gives to
/// `calc_metric` for `period`.
///
/// * `KEY@period` — only for that period, `KEY!period` — for every other one;
/// * `KEY?` — only when `KEY` is not set or empty in the environment;
/// * `KEY??` — only when `KEY` is not set at all.
fn process_env_map(input: &BTreeMap<String, String>, period: &str) -> BTreeMap<String, String> {
    let mut in_map: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in input {
        if k.contains('@') {
            let ary: Vec<&str> = k.split('@').collect();
            if ary[1] == period && !ary[0].is_empty() {
                in_map.insert(ary[0].to_string(), v.clone());
            }
            continue;
        }
        if k.contains('!') {
            let ary: Vec<&str> = k.split('!').collect();
            if ary[1] != period && !ary[0].is_empty() {
                in_map.insert(ary[0].to_string(), v.clone());
            }
            continue;
        }
        in_map.insert(k.clone(), v.clone());
    }
    if !in_map.keys().any(|k| k.ends_with('?')) {
        return in_map;
    }
    let mut out_map = BTreeMap::new();
    for (k, v) in in_map {
        if let Some(k2) = k.strip_suffix("??") {
            if std::env::var_os(k2).is_none() {
                out_map.insert(k2.to_string(), v);
            }
            continue;
        }
        if let Some(k2) = k.strip_suffix('?') {
            if std::env::var(k2).unwrap_or_default().is_empty() {
                out_map.insert(k2.to_string(), v);
            }
            continue;
        }
        out_map.insert(k, v);
    }
    out_map
}

/// A histogram `calc_metric` run collected for the end of the sync.
#[derive(Debug, Clone)]
struct HistJob {
    /// The 7 command line strings: `calc_metric series sql from to period params`.
    hist: Vec<String>,
    env_map: BTreeMap<String, String>,
    allow_fail: bool,
    wait_after_fail: i64,
}

/// Go `calcHistogram`: run one histogram `calc_metric`; returns the wait
/// requested after a (tolerated) failure. The result is sent to `ch` (when
/// given) before the closing line is printed, like the Go `defer`.
fn calc_histogram(ctx: &Ctx, job: &HistJob, ch: Option<&mpsc::Sender<i64>>) -> i64 {
    let hist = &job.hist;
    if hist.len() != 7 {
        fatalf!(
            "calcHistogram, expected 7 strings, got: {}: {}",
            hist.len(),
            gofmt::slice(hist)
        );
    }
    let dt_start = Instant::now();
    let desc = format!(
        "{},{},{},{},{},{},{},{}",
        hist[1], hist[2], hist[3], hist[4], hist[5], hist[6], job.allow_fail, job.wait_after_fail
    );
    printf!("Calculate histogram {} ...\n", desc);
    let calculated = |dt_start: Instant| {
        printf!(
            "Calculated histogram {} ... {}\n",
            desc,
            gotime::format_go_duration(dt_start.elapsed())
        );
    };
    let mut ch_res = 0;
    // Go runs the command with `ExecFatal` unless failures are tolerated; a
    // failure then panics inside `ExecCommand`, and the deferred "Calculated
    // histogram" line is still printed while the panic unwinds. `fatal_on_error`
    // exits directly, so run the command non-fatally and print that line
    // ourselves before dying.
    let mut exec_ctx = ctx.copy_context();
    exec_ctx.exec_fatal = false;
    let res = exec_command(&exec_ctx, hist, &job.env_map);
    if !ctx.allow_metric_fail && !job.allow_fail {
        if let Err(err) = res {
            calculated(dt_start);
            fatal_on_error(err);
        }
    } else if let Err(err) = res {
        printf!(
            "WARNING: histogram {} {} failed: {}\n",
            gofmt::map(&job.env_map),
            gofmt::slice(hist),
            err
        );
        if job.wait_after_fail > 0 {
            printf!(
                "WARNING: {} failed: waiting {} seconds\n",
                gofmt::slice(hist),
                job.wait_after_fail
            );
            std::thread::sleep(Duration::from_secs(job.wait_after_fail as u64));
            printf!(
                "WARNING: {} failed: waited {} seconds\n",
                gofmt::slice(hist),
                job.wait_after_fail
            );
            ch_res = job.wait_after_fail;
        }
    }
    if let Some(ch) = ch {
        let _ = ch.send(ch_res);
    }
    calculated(dt_start);
    ch_res
}

/// `GetThreadsNum` limited by `GHA2DB_MAX_HIST` (without the message).
fn histogram_threads(ctx: &Ctx) -> usize {
    let mut c = ctx.copy_context();
    let thr_n = threads::get_threads_num(&mut c);
    if ctx.max_histograms > 0 && thr_n as i64 > ctx.max_histograms {
        return ctx.max_histograms as usize;
    }
    thr_n
}

/// Run the collected histograms (Go: the "Process histograms" part of
/// `sync`), possibly `thr_n` at a time; returns the longest wait requested by
/// a tolerated failure.
fn run_histograms(ctx: &mut Ctx, hists: &[HistJob]) -> i64 {
    let mut thr_n = threads::get_threads_num(ctx);
    if ctx.max_histograms > 0 && thr_n as i64 > ctx.max_histograms {
        printf!(
            "Number of parallel histograms limited to {} -> {}\n",
            thr_n,
            ctx.max_histograms
        );
        thr_n = ctx.max_histograms as usize;
    }
    let ctx: &Ctx = ctx;
    let mut max_res = 0;
    if thr_n > 1 {
        printf!(
            "Now processing {} histograms using MT{} version\n",
            hists.len(),
            thr_n
        );
        let (tx, rx) = mpsc::channel::<i64>();
        let mut prc = 0usize;
        std::thread::scope(|scope| {
            let mut n_threads = 0usize;
            for job in hists {
                let tx = tx.clone();
                scope.spawn(move || {
                    calc_histogram(ctx, job, Some(&tx));
                });
                n_threads += 1;
                while n_threads >= thr_n {
                    let res = rx.recv().unwrap_or(0);
                    if res > max_res {
                        max_res = res;
                    }
                    n_threads -= 1;
                    prc += 1;
                    if prc.is_multiple_of(3) {
                        thr_n = histogram_threads(ctx);
                    }
                }
            }
            printf!("Final threads join (processed {})\n", prc);
            while n_threads > 0 {
                let res = rx.recv().unwrap_or(0);
                if res > max_res {
                    max_res = res;
                }
                n_threads -= 1;
            }
        });
    } else {
        printf!(
            "Now processing {} histograms using ST version\n",
            hists.len()
        );
        for job in hists {
            let res = calc_histogram(ctx, job, None);
            if res > max_res {
                max_res = res;
            }
        }
    }
    max_res
}

/// Expand the `sqls:` lists and drop the metrics excluded for the project
/// (Go: the first metrics loop of `sync`).
fn expand_metrics(ctx: &Ctx, all_metrics: Vec<Metric>) -> Vec<Metric> {
    let mut metrics_list = Vec::new();
    for metric in all_metrics {
        if projects::excluded_for_project(&ctx.project, &metric.project) {
            printf!(
                "Metric {} have project setting {} which is skipped for the current {} project\n",
                metric.name,
                metric.project,
                ctx.project
            );
            continue;
        }
        if metric.histogram && !metric.drop.is_empty() {
            fatalf!(
                "you cannot use drop series property on histogram metrics: {}",
                metric.go_string()
            );
        }
        if let Some(sqls) = &metric.metric_sqls {
            if !metric.metric_sql.is_empty() {
                fatalf!("you cannot use both 'sql' and 'sqls' fields'");
            }
            let mut drop_added = false;
            for sql in sqls {
                let mut new_metric = metric.clone();
                new_metric.metric_sqls = None;
                new_metric.metric_sql = sql.clone();
                if !drop_added {
                    drop_added = true;
                } else {
                    new_metric.drop = String::new();
                }
                metrics_list.push(new_metric);
            }
            continue;
        }
        metrics_list.push(metric);
    }
    metrics_list
}

fn sync(ctx: &mut Ctx, args: &[String]) {
    // Orgs & repos
    let s_org = args.first().cloned().unwrap_or_default();
    let s_repo = args.get(1).cloned().unwrap_or_default();
    let org: Vec<String> = s_org.split(',').map(|x| x.trim().to_string()).collect();
    let repo: Vec<String> = s_repo.split(',').map(|x| x.trim().to_string()).collect();
    printf!(
        "gha2db_sync.go: Running on: {}/{}\n",
        org.join("+"),
        repo.join("+")
    );

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    let cmd_prefix = if ctx.local_cmd { "./" } else { "" };

    // Connect to Postgres DB
    let con = pg::pg_conn(ctx);

    // Get max event date from Postgres database
    let mut max_dt_pg = GoTime::utc(ctx.default_start_date);
    if !ctx.force_start_date {
        let mut max_dt: Option<DateTime<FixedOffset>> = None;
        fatal_on_pg_err(
            query_row_sql(&con, ctx, "select max(dt) from gha_parsed", &[])
                .scan(&mut [&mut max_dt]),
        );
        if let Some(dt) = max_dt {
            max_dt_pg = GoTime::db(dt + devstatscode::chrono::Duration::hours(1));
        }
    }

    // Get max series date from TS database
    let mut max_dt_tsdb = GoTime::utc(ctx.default_start_date);
    if !ctx.force_start_date {
        let table = format!("s{}", ctx.last_series);
        if table_exists(&con, ctx, &table) {
            let mut max_dt: Option<DateTime<FixedOffset>> = None;
            fatal_on_pg_err(
                query_row_sql(&con, ctx, &format!("select max(time) from {table}"), &[])
                    .scan(&mut [&mut max_dt]),
            );
            if let Some(dt) = max_dt {
                max_dt_tsdb = GoTime::db(dt);
            }
        }
    }
    printf!(
        "Using start dates: pg: {}, tsdb: {}\n",
        max_dt_pg.ymdh(),
        max_dt_tsdb.ymdh()
    );

    // Create date range — just to get into the next GHA hour
    let mut from = max_dt_pg;
    let to = GoTime::now();
    let now_hour = Local::now().hour() as i64;
    let from_date = gotime::to_ymd_date(from.t);
    let from_hour = from.t.hour().to_string();
    let to_date = gotime::to_ymd_date(to.t);
    let to_hour = to.t.hour().to_string();

    // Get new GHAs
    if !ctx.skip_pdb {
        // Clear old DB logs
        log::clear_db_logs();

        // gha2db
        printf!(
            "GHA range: {} {} - {} {}\n",
            from_date,
            from_hour,
            to_date,
            to_hour
        );
        let res = exec_command(
            ctx,
            &[
                format!("{cmd_prefix}gha2db"),
                from_date,
                from_hour,
                to_date,
                to_hour,
                org.join(","),
                repo.join(","),
            ],
            &BTreeMap::new(),
        );
        fatal_on_err(res);

        // Only run commits analysis for the current DB here: repos were
        // updated by `devstats`, the new GHA hours were fetched by `gha2db`,
        // now update the new commits files.
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        env.insert("GHA2DB_FETCH_COMMITS_MODE".into(), "1".into());
        env.insert("GHA2DB_PROCESS_COMMITS".into(), "1".into());
        env.insert("GHA2DB_PROJECTS_COMMITS".into(), ctx.project.clone());
        env.insert("GHA2DB_PROJECT".into(), ctx.project.clone());
        if ctx.fetch_commits_mode != 1 {
            env.insert(
                "GHA2DB_FETCH_COMMITS_MODE".into(),
                ctx.fetch_commits_mode.to_string(),
            );
        }
        if !ctx.skip_get_repos {
            printf!("Update git commits\n");
            let res = exec_command(ctx, &[format!("{cmd_prefix}get_repos")], &env);
            fatal_on_err(res);
        }

        // GitHub API calls to get the open issues state (non fatal)
        if !ctx.skip_ghapi {
            printf!("Update data from GitHub API\n");
            ctx.exec_fatal = false;
            let res = exec_command(ctx, &[format!("{cmd_prefix}ghapi2db")], &BTreeMap::new());
            ctx.exec_fatal = true;
            if let Err(err) = res {
                printf!("Error executing ghapi2db: {}\n", err);
                eprintln!("Error executing ghapi2db: {}", err);
            }
        }

        // Eventual postprocess SQLs from the `structure` call
        printf!("Update structure\n");
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        env.insert("GHA2DB_SKIPTABLE".into(), "1".into());
        env.insert("GHA2DB_MGETC".into(), "y".into());
        let res = exec_command(ctx, &[format!("{cmd_prefix}structure")], &env);
        fatal_on_err(res);
    }

    // Calc metric
    // This is only correct when we are able to run all syncs every hour,
    // otherwise ctx.rand_compute_at_this_date is set.
    let mut daily_recalc_hour: i64 = 0;
    let mut ran_tags = false;
    if ctx.rand_compute_at_this_date {
        daily_recalc_hour = rng::intn(6) as i64;
    }
    // If set, tags and columns are only computed at a random 0-5 hour,
    // otherwise always when hour < 6.
    if !ctx.allow_rand_tags_cols_compute && now_hour < 6 {
        daily_recalc_hour = now_hour;
    }
    if !ctx.skip_tsdb {
        let mut metrics_dir = format!("{data_prefix}metrics");
        if !ctx.project.is_empty() {
            metrics_dir.push('/');
            metrics_dir.push_str(&ctx.project);
        }
        // Regenerate points from this date
        if ctx.reset_tsdb {
            from = GoTime::utc(ctx.default_start_date);
        } else {
            from = max_dt_tsdb;
        }
        printf!("TS range: {} - {}\n", from.ymdh(), to.ymdh());

        // TSDB tags (repo groups template variable currently)
        if !ctx.skip_tags {
            if ctx.reset_tsdb || now_hour == daily_recalc_hour {
                printf!("Run tags\n");
                let res = exec_command(ctx, &[format!("{cmd_prefix}tags")], &BTreeMap::new());
                fatal_on_err(res);
                ran_tags = true;
                printf!("Run tags finished, will also run columns later\n");
            } else {
                printf!(
                    "Skipping `tags` recalculation, it is only computed once per day hour={}\n",
                    daily_recalc_hour
                );
            }
        }
        // Run `columns` anytime tags were run (or when resetting the TSDB).
        if (ctx.reset_tsdb || ran_tags) && !ctx.skip_columns {
            printf!("Run columns\n");
            let res = exec_command(ctx, &[format!("{cmd_prefix}columns")], &BTreeMap::new());
            fatal_on_err(res);
            printf!("Run columns finished\n");
        }

        // Annotations
        if !ctx.skip_annotations {
            if !ctx.project.is_empty()
                && (ctx.reset_tsdb || now_hour == daily_recalc_hour || ran_tags)
            {
                printf!("Run annotations\n");
                let res =
                    exec_command(ctx, &[format!("{cmd_prefix}annotations")], &BTreeMap::new());
                fatal_on_err(res);
            } else {
                printf!(
                    "Skipping `annotations` recalculation, it is only computed once per day hour={} or if tags were ran during this sync\n",
                    daily_recalc_hour
                );
            }
        }

        // Get quick ranges from the TSDB (filled by the annotations command)
        let quick_ranges = get_tag_values(&con, ctx, "quick_ranges", "quick_ranges_suffix");
        printf!(
            "Quick ranges: {}, compute periods: {}\n",
            gofmt::slice(&quick_ranges),
            compute_periods_string(&ctx.compute_periods)
        );

        // Read metrics configuration
        let data = fatal_on_err(io::read_file(
            ctx,
            &format!("{data_prefix}{}", ctx.metrics_yaml),
        ));
        let mut all_metrics: Metrics = fatal_on_err(yde::unmarshal(&data));

        // randomize metrics order
        if !ctx.skip_rand {
            randomize(&mut all_metrics.metrics, ctx);
        }

        // Keep all histograms here
        let mut hists: Vec<HistJob> = Vec::new();
        let only_metrics = !ctx.only_metrics.is_empty();
        let skip_metrics = !ctx.skip_metrics.is_empty();
        let metrics_list = expand_metrics(ctx, all_metrics.metrics);

        // Iterate all metrics
        let mut max_wait: i64 = 0;
        for metric in &metrics_list {
            if metric.disabled {
                continue;
            }
            if only_metrics && !ctx.only_metrics.contains_key(&metric.metric_sql) {
                continue;
            }
            if skip_metrics && ctx.skip_metrics.contains_key(&metric.metric_sql) {
                continue;
            }
            let mut drop_processed = false;
            // handle start_from (datetime) or last_hours (from now - N hours)
            let mut from_date = from;
            let mut changed = false;
            if let Some(start_from) = metric.start_from {
                if metric.last_hours > 0 {
                    fatalf!(
                        "you cannot use both StartFrom {} and LastHours {}",
                        gofmt::time(start_from),
                        metric.last_hours
                    );
                }
                if from_date.t < start_from {
                    from_date = GoTime::parsed(start_from);
                    changed = true;
                }
            }
            if metric.last_hours > 0 {
                let dt = GoTime::now_minus_hours(metric.last_hours);
                if from_date.t < dt.t {
                    from_date = dt;
                    changed = true;
                }
            }
            if ctx.debug > 0 && changed {
                printf!(
                    "Using non-standard start date: {}, instead of {}\n",
                    from_date.v(),
                    from.v()
                );
            }
            if changed && from_date.t > to.t {
                if ctx.debug >= 0 {
                    printf!(
                        "Non-standard start date: {} (used instead of {}) is after end date {}, skipping\n",
                        from_date.v(),
                        from.v(),
                        to.v()
                    );
                }
                continue;
            }
            let mut extra_params: Vec<String> = Vec::new();
            if ctx.project_scale != 1.0 {
                extra_params.push(format!("project_scale:{:.6}", ctx.project_scale));
            }
            if metric.histogram {
                extra_params.push("hist".into());
            }
            if metric.multi_value {
                extra_params.push("multivalue".into());
            }
            if metric.escape_value_name {
                extra_params.push("escape_value_name".into());
            }
            if metric.skip_escape_series_name {
                extra_params.push("skip_escape_series_name".into());
            }
            if !metric.desc.is_empty() {
                extra_params.push(format!("desc:{}", metric.desc));
            }
            if !metric.merge_series.is_empty() {
                extra_params.push(format!("merge_series:{}", metric.merge_series));
            }
            if metric.custom_data {
                extra_params.push("custom_data".into());
                if metric.custom_data_unique_time {
                    extra_params.push("custom_data_unique_time".into());
                }
            }
            if !metric.series_name_map.is_empty() {
                extra_params.push(format!(
                    "series_name_map:{}",
                    gofmt::map(&metric.series_name_map)
                ));
            }
            if metric.hll {
                extra_params.push("hll".into());
            }
            let mut periods: Vec<String> = metric.periods.split(',').map(String::from).collect();
            let mut aggregate = metric.aggregate.clone();
            if aggregate.is_empty() {
                aggregate = "1".into();
            }
            if metric.annotations_ranges {
                extra_params.push("annotations_ranges".into());
                periods = quick_ranges.clone();
                aggregate = "1".into();
            }
            let aggregate_arr: Vec<&str> = aggregate.split(',').collect();
            let skip_map: BTreeSet<&str> = metric.skip.split(',').collect();
            if !ctx.reset_tsdb && !ctx.reset_ranges {
                extra_params.push("skip_past".into());
            }
            for aggr_str in aggregate_arr {
                fatal_on_err(gotime::parse_go_int(aggr_str));
                let aggr_suffix = if aggr_str == "1" { "" } else { aggr_str };
                for period in &periods {
                    let period_aggr = format!("{period}{aggr_suffix}");
                    if skip_map.contains(period_aggr.as_str()) {
                        if ctx.debug > 0 {
                            printf!("Skipped period {}\n", period_aggr);
                        }
                        continue;
                    }
                    let mut recalc = if metric.always_recalc {
                        true
                    } else {
                        gotime::compute_period_at_this_date(
                            ctx,
                            period,
                            to.wall_as_utc(),
                            metric.histogram,
                        )
                    };
                    // The sync probability can be less than 100% and that may
                    // cause gaps: eventually recalculate even if not due.
                    if !recalc && ctx.compute_periods.is_none() {
                        let val = rng::intn(ctx.recalc_reciprocal.max(1) as u64);
                        if val == 0 {
                            printf!(
                                "Recalculating period due to reciprocal \"{}{}\", hist {} for date to {}, computePeriods: {}, metric: {}\n",
                                period,
                                aggr_suffix,
                                metric.histogram,
                                to.v(),
                                compute_periods_string(&ctx.compute_periods),
                                metric.name
                            );
                            recalc = true;
                        }
                    }
                    if ctx.debug > 0 {
                        printf!(
                            "Recalculate period \"{}{}\", hist {} for date to {}: {}\n",
                            period,
                            aggr_suffix,
                            metric.histogram,
                            to.v(),
                            recalc
                        );
                    }
                    if (!ctx.reset_tsdb || ctx.compute_periods.is_some()) && !recalc {
                        printf!(
                            "Skipping recalculating period \"{}{}\", hist {} for date to {}, computePeriods: {}, metric: {}\n",
                            period,
                            aggr_suffix,
                            metric.histogram,
                            to.v(),
                            compute_periods_string(&ctx.compute_periods),
                            metric.name
                        );
                        continue;
                    }
                    let mut series_name_or_func = metric.series_name_or_func.clone();
                    if metric.add_period_to_name {
                        series_name_or_func.push('_');
                        series_name_or_func.push_str(&period_aggr);
                    }
                    // Histogram metrics usually take long but execute a single
                    // query, so they are collected and run at the end, each in
                    // its own thread.
                    let mut e_params = extra_params.clone();
                    if ctx.enable_metrics_drop && !drop_processed {
                        if !metric.drop.is_empty() {
                            e_params.push(format!("drop:{}", metric.drop));
                        }
                        drop_processed = true;
                    }
                    let env_map = process_env_map(&metric.env_map, &period_aggr);
                    let sql_file = format!("{metrics_dir}/{}.sql", metric.metric_sql);
                    if metric.histogram {
                        printf!(
                            "Scheduled histogram metric {}, period {}, desc: '{}', aggregate: '{}' ...\n",
                            metric.name,
                            period,
                            metric.desc,
                            aggr_suffix
                        );
                        hists.push(HistJob {
                            hist: vec![
                                format!("{cmd_prefix}calc_metric"),
                                series_name_or_func,
                                sql_file,
                                from_date.ymdh(),
                                to.ymdh(),
                                period_aggr,
                                extra_params.join(","),
                            ],
                            env_map,
                            allow_fail: metric.allow_fail,
                            wait_after_fail: metric.wait_after_fail,
                        });
                    } else {
                        let dt_start = Instant::now();
                        printf!(
                            "Calculate metric {}, period {}, desc: '{}', aggregate: '{}' ...\n",
                            metric.name,
                            period,
                            metric.desc,
                            aggr_suffix
                        );
                        let cmd = [
                            format!("{cmd_prefix}calc_metric"),
                            series_name_or_func,
                            sql_file,
                            from_date.ymdh(),
                            to.ymdh(),
                            period_aggr,
                            e_params.join(","),
                        ];
                        let res = if ctx.allow_metric_fail || metric.allow_fail {
                            let mut exec_ctx = ctx.copy_context();
                            exec_ctx.exec_fatal = false;
                            exec_command(&exec_ctx, &cmd, &env_map)
                        } else {
                            exec_command(ctx, &cmd, &env_map)
                        };
                        if !ctx.allow_metric_fail && !metric.allow_fail {
                            fatal_on_err(res);
                        } else if let Err(err) = res {
                            printf!("WARNING: {} failed: {}\n", metric.go_string(), err);
                            if metric.wait_after_fail > 0 {
                                printf!(
                                    "WARNING: {} failed: waiting {} seconds\n",
                                    metric.go_string(),
                                    metric.wait_after_fail
                                );
                                std::thread::sleep(Duration::from_secs(
                                    metric.wait_after_fail as u64,
                                ));
                                printf!(
                                    "WARNING: {} failed: waited {} seconds\n",
                                    metric.go_string(),
                                    metric.wait_after_fail
                                );
                                if metric.wait_after_fail > max_wait {
                                    max_wait = metric.wait_after_fail;
                                }
                            }
                        }
                        printf!(
                            "Calculated metric {}, period {}, desc: '{}', aggregate: '{}' ... {}\n",
                            metric.name,
                            period,
                            metric.desc,
                            aggr_suffix,
                            gotime::format_go_duration(dt_start.elapsed())
                        );
                    }
                }
            }
        }
        if max_wait > 0 {
            printf!(
                "There was at least one failure that requested wait (non-hist), waiting: {} seconds\n",
                max_wait
            );
            std::thread::sleep(Duration::from_secs(max_wait as u64));
            printf!(
                "There was at least one failure that requested wait (non-hist), waited: {} seconds\n",
                max_wait
            );
        }
        // randomize histograms
        if !ctx.skip_rand {
            printf!("Randomizing histogram metrics calculation order\n");
            rng::shuffle(&mut hists);
        }
        // Process histograms (possibly MT)
        let max_res = run_histograms(ctx, &hists);
        if max_res > 0 {
            printf!(
                "There was at least one failure that requested wait (hist), waiting: {} seconds\n",
                max_res
            );
            std::thread::sleep(Duration::from_secs(max_res as u64));
            printf!(
                "There was at least one failure that requested wait (hist), waited: {} seconds\n",
                max_res
            );
        }

        // TSDB: ensure that the calculated metrics have all columns from tags
        if !ctx.skip_columns {
            if ctx.run_columns || ctx.reset_tsdb || ran_tags || now_hour == daily_recalc_hour {
                printf!("Run columns\n");
                let res = exec_command(ctx, &[format!("{cmd_prefix}columns")], &BTreeMap::new());
                fatal_on_err(res);
            } else {
                printf!(
                    "Skipping `columns` recalculation, it is only computed once per day, hour={} or if tags were ran during this sync\n",
                    daily_recalc_hour
                );
            }
        }
    }

    // Vars (some tables/dashboards require vars calculation)
    if !ctx.skip_pdb && !ctx.skip_vars {
        let mut vars_fn = std::env::var("GHA2DB_VARS_FN_YAML").unwrap_or_default();
        if vars_fn.is_empty() {
            vars_fn = "sync_vars.yaml".into();
        }
        printf!("Run vars\n");
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        env.insert("GHA2DB_VARS_FN_YAML".into(), vars_fn);
        let res = exec_command(ctx, &[format!("{cmd_prefix}vars")], &env);
        fatal_on_err(res);
    }
    printf!("Sync success\n");
    con.close();
}

/// Go `os.Setenv` failure conditions (`setenv: invalid argument`).
fn setenv(key: &str, value: &str) {
    if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
        fatal_on_error("setenv: invalid argument");
    }
    std::env::set_var(key, value);
}

/// Go `getSyncArgs`: the command line arguments (without the program name)
/// when given, otherwise the `command_line` of `GHA2DB_PROJECT` in
/// `projects.yaml` — also applying the project's `start_date` (unless
/// `GHA2DB_STARTDT_FORCE`), `env` (unless `ENV_SET`, i.e. when started by
/// `devstats`) and `project_scale`.
fn get_sync_args(ctx: &mut Ctx, os_args: &[String]) -> Vec<String> {
    // User commandline override
    if os_args.len() > 1 {
        return os_args[1..].to_vec();
    }

    // No user commandline, get args specific to project GHA2DB_PROJECT
    if ctx.project.is_empty() {
        fatalf!(
            "you have to set project via GHA2DB_PROJECT environment variable if you provide no commandline arguments"
        );
    }
    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Are we running from "devstats" which already sets ENV from projects.yaml?
    let env_set = !std::env::var("ENV_SET").unwrap_or_default().is_empty();

    // Read defined projects
    let data = fatal_on_err(io::read_file(
        ctx,
        &format!("{data_prefix}{}", ctx.projects_yaml),
    ));
    let all: projects::AllProjects = fatal_on_err(yde::unmarshal(&data));
    match all.projects.get(&ctx.project) {
        Some(proj) => {
            if let Some(start_date) = proj.start_date {
                if !ctx.force_start_date {
                    ctx.default_start_date = yde::to_utc(start_date);
                }
            }
            if !env_set {
                for (env_k, env_v) in &proj.env {
                    setenv(env_k, env_v);
                }
            }
            if let Some(scale) = proj.project_scale {
                if scale >= 0.0 {
                    ctx.project_scale = scale;
                }
            }
            proj.command_line.clone()
        }
        // No user commandline and project not found
        None => fatalf!(
            "project '{}' is not defined in '{}'",
            ctx.project,
            ctx.projects_yaml
        ),
    }
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    let _ = PROCESS_START.set(dt_start);
    gofmt::mark_process_start();
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    let os_args: Vec<String> = std::env::args().collect();
    let args = get_sync_args(&mut ctx, &os_args);
    sync(&mut ctx, &args);
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn env_map_period_conditions() {
        let input = m(&[
            ("A", "1"),
            ("B@d7", "2"),
            ("C!d7", "3"),
            ("@d7", "x"),
            ("!d7", "y"),
        ]);
        assert_eq!(process_env_map(&input, "d7"), m(&[("A", "1"), ("B", "2")]));
        assert_eq!(process_env_map(&input, "h"), m(&[("A", "1"), ("C", "3")]));
        assert!(process_env_map(&BTreeMap::new(), "h").is_empty());
    }

    #[test]
    fn env_map_conditional_keys_look_at_the_environment() {
        std::env::set_var("G2R_SYNC_SET", "v");
        std::env::set_var("G2R_SYNC_EMPTY", "");
        std::env::remove_var("G2R_SYNC_UNSET");
        let input = m(&[
            ("G2R_SYNC_SET?", "1"),
            ("G2R_SYNC_EMPTY?", "2"),
            ("G2R_SYNC_UNSET?", "3"),
            ("G2R_SYNC_SET??", "4"),
            ("G2R_SYNC_EMPTY??", "5"),
            ("G2R_SYNC_UNSET??", "6"),
            ("PLAIN", "7"),
            ("G2R_SYNC_SET?@d", "8"),
        ]);
        assert_eq!(
            process_env_map(&input, "d"),
            m(&[
                ("G2R_SYNC_EMPTY", "2"),
                ("G2R_SYNC_UNSET", "6"),
                ("PLAIN", "7"),
            ])
        );
        // no conditional key at all: the map is passed through untouched
        assert_eq!(
            process_env_map(&m(&[("G2R_SYNC_SET", "1")]), "d"),
            m(&[("G2R_SYNC_SET", "1")])
        );
        std::env::remove_var("G2R_SYNC_SET");
        std::env::remove_var("G2R_SYNC_EMPTY");
    }

    #[test]
    fn metric_prints_like_go_plus_v() {
        let mut metric = Metric {
            name: "GitHub Stats".into(),
            periods: "h,d".into(),
            series_name_or_func: "multi_row_single_column".into(),
            metric_sql: "github_stats".into(),
            allow_fail: true,
            ..Metric::default()
        };
        assert_eq!(
            metric.go_string(),
            "{Name:GitHub Stats Periods:h,d SeriesNameOrFunc:multi_row_single_column MetricSQL:github_stats \
             MetricSQLs:<nil> AddPeriodToName:false Histogram:false Aggregate: Skip: Desc: MultiValue:false \
             EscapeValueName:false SkipEscapeSeriesName:false AnnotationsRanges:false MergeSeries: \
             CustomData:false CustomDataUniqueTime:false StartFrom:<nil> LastHours:0 SeriesNameMap:map[] \
             EnvMap:map[] Disabled:false Drop: Project: AllowFail:true WaitAfterFail:0 HLL:false AlwaysRecalc:false}"
        );
        metric.metric_sqls = Some(vec!["a".into(), "b c".into()]);
        metric.start_from = Some(
            Utc.with_ymd_and_hms(2018, 1, 1, 0, 0, 0)
                .unwrap()
                .fixed_offset(),
        );
        metric.series_name_map = m(&[("b", "2"), ("a", "1")]);
        metric.env_map = m(&[("GHA2DB_NCPUS?", "8")]);
        let s = metric.go_string();
        assert!(s.contains(" MetricSQLs:[a b c] "), "{s}");
        assert!(
            s.contains(" StartFrom:2018-01-01 00:00:00 +0000 UTC "),
            "{s}"
        );
        assert!(
            s.contains(" SeriesNameMap:map[a:1 b:2] EnvMap:map[GHA2DB_NCPUS?:8] "),
            "{s}"
        );
    }

    #[test]
    fn go_time_renderings() {
        let db = GoTime::db(
            Utc.with_ymd_and_hms(2020, 3, 4, 5, 0, 0)
                .unwrap()
                .fixed_offset(),
        );
        assert_eq!(db.v(), "2020-03-04 05:00:00 +0000 +0000");
        assert_eq!(db.ymdh(), "2020-03-04 5");
        let utc = GoTime::utc(Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap());
        assert_eq!(utc.v(), "2012-07-01 00:00:00 +0000 UTC");
        let with_offset = GoTime::parsed(
            devstatscode::chrono::FixedOffset::east_opt(2 * 3600)
                .unwrap()
                .with_ymd_and_hms(2018, 1, 1, 0, 0, 0)
                .unwrap(),
        );
        assert_eq!(with_offset.v(), "2018-01-01 00:00:00 +0200 +0200");
        let now = GoTime::now();
        let s = now.v();
        assert!(s.contains(" m=+"), "{s}");
        let earlier = GoTime::now_minus_hours(2);
        let s = earlier.v();
        assert!(s.contains(" m=-7199."), "{s}");
        assert!(earlier.t < now.t);
        let mono = GoTime {
            t: db.t,
            zone: Zone::Named,
            mono: Some(1_500_000_000),
        };
        assert_eq!(mono.v(), "2020-03-04 05:00:00 +0000 UTC m=+1.500000000");
    }

    #[test]
    fn compute_periods_render_like_go_maps() {
        assert_eq!(compute_periods_string(&None), "map[]");
        let mut cp: BTreeMap<String, BTreeSet<bool>> = BTreeMap::new();
        cp.insert("h".into(), [true, false].into_iter().collect());
        cp.insert("d".into(), [false].into_iter().collect());
        assert_eq!(
            compute_periods_string(&Some(cp)),
            "map[d:map[false:{}] h:map[false:{} true:{}]]"
        );
    }

    #[test]
    fn sqls_are_expanded_and_drop_kept_once() {
        let ctx = Ctx {
            project: "kubernetes".into(),
            ..Ctx::default()
        };
        let metrics = vec![
            Metric {
                name: "multi".into(),
                metric_sqls: Some(vec!["a".into(), "b".into(), "c".into()]),
                drop: "sdrop".into(),
                ..Metric::default()
            },
            Metric {
                name: "not for k8s".into(),
                project: "!kubernetes".into(),
                metric_sql: "x".into(),
                ..Metric::default()
            },
            Metric {
                name: "only k8s".into(),
                project: "kubernetes,other".into(),
                metric_sql: "y".into(),
                ..Metric::default()
            },
            Metric {
                name: "empty sqls vanishes".into(),
                metric_sqls: Some(Vec::new()),
                ..Metric::default()
            },
        ];
        let list = expand_metrics(&ctx, metrics);
        let got: Vec<(String, String, String)> = list
            .iter()
            .map(|m| (m.name.clone(), m.metric_sql.clone(), m.drop.clone()))
            .collect();
        assert_eq!(
            got,
            [
                ("multi".to_string(), "a".to_string(), "sdrop".to_string()),
                ("multi".to_string(), "b".to_string(), String::new()),
                ("multi".to_string(), "c".to_string(), String::new()),
                ("only k8s".to_string(), "y".to_string(), String::new()),
            ]
        );
        assert!(list.iter().all(|m| m.metric_sqls.is_none()));
    }

    #[test]
    fn metrics_yaml_decodes_with_yaml_v2_rules() {
        let yaml = b"---\nmetrics:\n  - name: A\n    sql: a\n    periods: h,d\n    aggregate: 1,7\n    skip: h7\n    histogram: yes\n    last_hours: 24\n    env:\n      GHA2DB_NCPUS?: 8\n    series_name_map:\n      x: 1\n  - name: B\n    sqls: [b1, b2]\n    start_from: 2018-01-01T00:00:00Z\n    wait_after_fail: 3\n    unknown_field: ignored\n";
        let m: Metrics = yde::unmarshal(yaml).unwrap();
        assert_eq!(m.metrics.len(), 2);
        let a = &m.metrics[0];
        assert!(a.histogram);
        assert_eq!(a.last_hours, 24);
        assert_eq!(
            a.env_map.get("GHA2DB_NCPUS?").map(String::as_str),
            Some("8")
        );
        assert_eq!(a.series_name_map.get("x").map(String::as_str), Some("1"));
        assert_eq!(a.metric_sqls, None);
        let b = &m.metrics[1];
        assert_eq!(
            b.metric_sqls,
            Some(vec!["b1".to_string(), "b2".to_string()])
        );
        assert_eq!(
            gofmt::time(b.start_from.unwrap()),
            "2018-01-01 00:00:00 +0000 UTC"
        );
        assert_eq!(b.wait_after_fail, 3);
        let empty: Metrics = yde::unmarshal(b"").unwrap();
        assert!(empty.metrics.is_empty());
    }
}
