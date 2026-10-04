//! `gha2db` — Rust port of `cmd/gha2db/gha2db.go`: downloads the hourly
//! GH Archive JSON dumps for the requested `[from, to]` hour range, keeps the
//! events of the project's organisations/repositories and writes them (and
//! all their nested objects) into the PostgreSQL database. The old (2012–2014)
//! and the current GH Archive formats are both supported (`GHA2DB_OLDFMT`).

mod gz;
mod roles;

use std::collections::{BTreeSet, HashMap};
use std::io::Read;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, TimeZone, Timelike, Utc};
use devstatscode::broken_json::{is_plain_line, recover_json_chunks, JsonChunk};
use devstatscode::consts::{GHARCHIVE_URL, HIDE_CFG_FILE, NOW, TODAY};
use devstatscode::context::GoRegex;
use devstatscode::error::{defer, exit_on_panic, fatal_on_error};
use devstatscode::gha::{
    actor_hit, make_old_repo_name, parse_go_rfc3339, repo_hit, Event, EventOld, SkipDatesList,
};
use devstatscode::hash::hash_strings;
use devstatscode::json::{pretty_print_json, try_write_file_0644, write_file_0644};
use devstatscode::map::{strings_map_to_set, strings_set_keys};
use devstatscode::pg::api::{exec_sql_with_err, insert_ignore, n_value};
use devstatscode::pg::{pg_conn, PgConn, SqlArg};
use devstatscode::string::{get_hidden, maybe_hide_func, safe_utf8_bytes};
use devstatscode::threads::get_threads_num;
use devstatscode::time::{
    day_start, format_go_duration, to_gha_date, to_ymdh_date, to_ymdhms_date, wall_as_utc,
};
use devstatscode::{fatal_on_err, gofmt, printf, rng, signal, yamlv2, Ctx};

use devstatscode::ghawriter::{write_to_db, write_to_db_old_fmt, MaybeHide};

/// One processed hour with Go's rendering of it. `time.Parse(RFC3339,
/// "…+00:00")` yields a `time.Local` value (printed `+0000 UTC`) only when
/// the local zone offset is 0 at that instant, a nameless fixed zone (printed
/// `+0000 +0000`) otherwise; the `today`/`now` keywords go through `DayStart`
/// and are plain UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct HourDt {
    dt: DateTime<Utc>,
    named: bool,
}

impl HourDt {
    /// `time.Parse(time.RFC3339, fmt.Sprintf("%sT%02d:00:00+00:00", date, hour))`.
    fn parse(date: &str, hour: i64) -> Result<Self, String> {
        let t = parse_go_rfc3339(&format!("{}T{:02}:00:00+00:00", date, hour))?;
        let dt = t.with_timezone(&Utc);
        let named = Local
            .offset_from_utc_datetime(&dt.naive_utc())
            .local_minus_utc()
            == 0;
        Ok(HourDt { dt, named })
    }

    /// `lib.DayStart(now).Add(time.Duration(hour) * time.Hour)` on a local
    /// `time.Now()` (Go's `DayStart` re-labels the local date as UTC).
    fn today(now: DateTime<Local>, hour: i64) -> Self {
        HourDt {
            dt: day_start(wall_as_utc(&now)) + chrono::Duration::hours(hour),
            named: true,
        }
    }

    fn add_hour(self) -> Self {
        HourDt {
            dt: self.dt + chrono::Duration::hours(1),
            named: self.named,
        }
    }

    /// Go `%v`/`%+v` (`Time.String()` without a monotonic reading).
    fn go_string(&self) -> String {
        let s = gofmt::time(self.dt);
        if self.named {
            s
        } else {
            s.replace(" +0000 UTC", " +0000 +0000")
        }
    }
}

impl std::fmt::Display for HourDt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.go_string())
    }
}

/// Go `getMemUsage`: the memory line printed around a forced GC. There is no
/// tracing GC here, so the resident set size stands in for every counter and
/// `#gc` is always 0 (documented deviation, diagnostics only).
fn get_mem_usage() -> String {
    let rss_mb = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .nth(1)
                .and_then(|pages| pages.parse::<u64>().ok())
        })
        .map(|pages| (pages * 4096) >> 20)
        .unwrap_or(0);
    format!(
        "alloc:{0}M heap-alloc:{0}M(0k objs) total:{0}M sys:{0}M #gc:0",
        rss_mb
    )
}

/// Go `runGC`: memory line, `runtime.GC()`, memory line.
pub fn run_gc() {
    printf!("{}\n", get_mem_usage());
    printf!("{}\n", get_mem_usage());
}

/// Go `markAsProcessed`: remember that the hour was fetched (`gha_parsed`),
/// so that hours without events for the project are not fetched again.
fn mark_as_processed(con: &PgConn, ctx: &Ctx, dt: &HourDt) {
    if !ctx.db_out {
        return;
    }
    exec_sql_with_err(
        con,
        ctx,
        &insert_ignore(&format!("into gha_parsed(dt) values({})", n_value(1))),
        &[SqlArg::Time(dt.dt.fixed_offset())],
    );
}

/// Filters of one run: organisations/repositories as sets or regular
/// expressions (Go passes them separately to `getGHAJSON`).
struct Filters<'a> {
    forg: &'a BTreeSet<String>,
    frepo: &'a BTreeSet<String>,
    org_re: Option<&'a GoRegex>,
    repo_re: Option<&'a GoRegex>,
    maybe_hide: MaybeHide<'a>,
    skip_dates: &'a BTreeSet<String>,
}

/// Counters of one GH Archive line: events found/written and, for damaged
/// lines, broken chunks skipped and events recovered from the pieces.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LineStats {
    found: i64,
    events: i64,
    broken: i64,
    recovered: i64,
}

/// One decoded event in either GH Archive format.
enum Decoded {
    New(Box<Event>),
    Old(Box<EventOld>),
}

/// Strict decode of one JSON value in the run's format (`GHA2DB_OLDFMT`).
fn decode_event(ctx: &Ctx, json: &[u8]) -> Result<Decoded, serde_json::Error> {
    if ctx.old_format {
        serde_json::from_slice::<EventOld>(json).map(|e| Decoded::Old(Box::new(e)))
    } else {
        serde_json::from_slice::<Event>(json).map(|e| Decoded::New(Box::new(e)))
    }
}

/// Go `parseJSON`: process one GH Archive line. A line that is a single
/// well-formed event is decoded directly; anything else (NUL padding, glued
/// events, garbage) is split by `recover_json_chunks` and every piece that
/// still decodes is processed while the broken ones are logged and skipped —
/// a damaged line never stops the run.
fn parse_json(
    con: &PgConn,
    ctx: &Ctx,
    idx: usize,
    njsons: usize,
    json: &[u8],
    dt: &HourDt,
    flt: &Filters<'_>,
) -> LineStats {
    let mut st = LineStats::default();
    if is_plain_line(json) {
        if let Ok(ev) = decode_event(ctx, json) {
            let (f, e) = process_event(con, ctx, json, &ev, dt, flt);
            st.found += f;
            st.events += e;
            return st;
        }
    }
    for chunk in recover_json_chunks(json) {
        let raw = chunk.bytes();
        // A broken chunk is never an event, even when the decoder takes it
        // (Go's jsoniter accepts invalid UTF-8 that Postgres rejects).
        match decode_event(ctx, raw) {
            Ok(ev) if matches!(chunk, JsonChunk::Json(_)) => {
                st.recovered += 1;
                let (f, e) = process_event(con, ctx, raw, &ev, dt, flt);
                st.found += f;
                st.events += e;
            }
            Ok(_) => {
                st.broken += 1;
                log_broken_json(ctx, idx, njsons, raw, dt, "broken JSON chunk", st.broken);
            }
            Err(err) => {
                st.broken += 1;
                log_broken_json(ctx, idx, njsons, raw, dt, &err.to_string(), st.broken);
            }
        }
    }
    st
}

/// Logs one undecodable chunk the way Go does: an `Error(<hour>)` line, the
/// chunk saved (best effort, only with `GHA2DB_JSON`) as
/// `jsons/error_<hour>-<line>-<lines>[-<n>].json` (`n` > 1 for further
/// chunks of the same line) and the `Cannot unmarshal` report on stdout and
/// stderr.
fn log_broken_json(
    ctx: &Ctx,
    idx: usize,
    njsons: usize,
    raw: &[u8],
    dt: &HourDt,
    err: &str,
    nth: i64,
) {
    let json_str = safe_utf8_bytes(raw);
    printf!("Error({}): {}\n", to_gha_date(dt.dt), err);
    if ctx.json_out {
        let mut ofn = format!("jsons/error_{}-{}-{}", to_gha_date(dt.dt), idx + 1, njsons);
        if nth > 1 {
            ofn = format!("{}-{}", ofn, nth);
        }
        ofn.push_str(".json");
        if let Err(e) = try_write_file_0644(&ofn, raw) {
            printf!("{}: cannot save broken JSON: {}\n", to_gha_date(dt.dt), e);
        }
    }
    printf!("{}: Cannot unmarshal:\n{}\n{}\n", dt, json_str, err);
    eprint!("{}: Cannot unmarshal:\n{}\n{}\n", dt, json_str, err);
}

/// The filtering/writing half of Go `parseJSON` for one decoded event:
/// `(found, events written)`.
fn process_event(
    con: &PgConn,
    ctx: &Ctx,
    json: &[u8],
    ev: &Decoded,
    dt: &HourDt,
    flt: &Filters<'_>,
) -> (i64, i64) {
    let (full_name, actor_name) = match ev {
        Decoded::Old(o) => (make_old_repo_name(&o.repository), o.actor.clone()),
        Decoded::New(n) => (n.repo.name.clone(), n.actor.login.clone()),
    };
    let mut f = 0;
    let mut e = 0;
    if repo_hit(
        ctx,
        &full_name,
        flt.forg,
        flt.frepo,
        flt.org_re,
        flt.repo_re,
    ) && actor_hit(ctx, &actor_name)
    {
        let eid = match ev {
            Decoded::Old(o) => hash_strings(&[
                &o.type_,
                &o.actor,
                &o.repository.name,
                &to_ymdhms_date(*o.created_at),
            ])
            .to_string(),
            Decoded::New(n) => n.id.clone(),
        };
        if ctx.json_out {
            // We want to Unmarshal/Marshall ALL JSON data, regardless of what is defined in lib.Event
            let pretty = pretty_print_json(json);
            let ofn = format!("jsons/{}_{}.json", dt.dt.timestamp(), eid);
            write_file_0644(&ofn, &pretty);
        }
        if ctx.db_out {
            e = match ev {
                Decoded::Old(o) => write_to_db_old_fmt(con, ctx, &eid, o, flt.maybe_hide),
                Decoded::New(n) => write_to_db(con, ctx, n, flt.maybe_hide),
            };
        }
        if ctx.debug >= 1 {
            printf!("Processed: '{}' event: {}\n", dt, eid);
        }
        f = 1;
    }
    (f, e)
}

/// Go `getGHAJSON`: the work for one hour of GH Archive data — download the
/// gzipped JSON lines (with retries), split and process them, and mark the
/// hour as parsed.
fn get_gha_json(ctx: &Ctx, dt: HourDt, flt: &Filters<'_>) {
    printf!("Working on {}\n", dt);

    // Connect to Postgres DB
    let con = pg_conn(ctx);

    // Check skip GHA date config
    if flt.skip_dates.contains(&to_ymdh_date(dt.dt)) {
        printf!("Skipped {}\n", dt);
        mark_as_processed(&con, ctx, &dt);
        con.close();
        return;
    }

    let gha_url = if ctx.gharchive_url.is_empty() {
        GHARCHIVE_URL
    } else {
        ctx.gharchive_url.as_str()
    };
    let fname = format!("{}{}.json.gz", gha_url, to_gha_date(dt.dt));

    // Get gzipped JSON array via HTTP
    let mut trials: i64 = 0;
    let jsons_bytes: Vec<u8>;
    loop {
        trials += 1;
        if trials > 1 {
            printf!("Retry({}) {}\n", trials, dt);
        }
        let timeout = Duration::from_secs(60 * (trials * ctx.http_timeout).max(0) as u64);
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(10)
            .timeout_global(Some(timeout))
            .build();
        let agent: ureq::Agent = config.into();
        let body = agent
            .get(&fname)
            .call()
            .map_err(|e| format!("Get {:?}: {}", fname, e))
            .and_then(|mut resp| {
                let mut body = Vec::new();
                match resp.body_mut().as_reader().read_to_end(&mut body) {
                    Ok(_) => Ok(body),
                    Err(e) => Err(format!("Get {:?}: {}", fname, e)),
                }
            });
        let body = match body {
            Ok(b) => b,
            Err(err) => {
                printf!("{}: Error http.Get:\n{}\n", dt, err);
                if trials < ctx.http_retry {
                    std::thread::sleep(Duration::from_secs((1 + rng::intn(20)) * trials as u64));
                    continue;
                }
                eprint!("{}: Error http.Get:\n{}\n", dt, err);
                fatal_on_error(err);
            }
        };

        // Decompress Gzipped response
        if let Some(err) = gz::header_error(&body) {
            printf!("{}: No data yet, gzip reader:\n{}\n", dt, err);
            if trials < ctx.http_retry {
                std::thread::sleep(Duration::from_secs((1 + rng::intn(3)) * trials as u64));
                continue;
            }
            eprint!("{}: No data yet, gzip reader:\n{}\n", dt, err);
            printf!("Gave up on {}\n", dt);
            con.close();
            return;
        }
        printf!("Opened {}\n", fname);

        match gz::read_all(&body) {
            Ok(b) => jsons_bytes = b,
            Err((partial, err)) => {
                printf!("{}: Error (no data yet, ioutil readall):\n{}\n", dt, err);
                if trials < ctx.http_retry {
                    std::thread::sleep(Duration::from_secs((1 + rng::intn(20)) * trials as u64));
                    continue;
                }
                eprint!("{}: Error (no data yet, ioutil readall):\n{}\n", dt, err);
                if partial.is_empty() {
                    printf!("Gave up on {}\n", dt);
                    con.close();
                    return;
                }
                // The archive keeps serving a truncated/corrupted hour: use
                // whatever decompressed instead of losing the whole hour.
                printf!(
                    "{}: Recovered {} bytes from broken archive after {} attempt(s), continuing\n",
                    dt,
                    partial.len(),
                    trials
                );
                jsons_bytes = partial;
            }
        }
        if trials > 1 {
            printf!("Recovered({}) & decompressed {}\n", trials, fname);
        } else {
            printf!("Decompressed {}\n", fname);
        }
        break;
    }

    // Split JSON array into separate JSONs
    let jsons_array: Vec<&[u8]> = jsons_bytes.split(|b| *b == b'\n').collect();
    printf!("Split {}, {} JSONs\n", fname, jsons_array.len());

    // Process JSONs one by one
    let (mut n, mut f, mut e) = (0i64, 0i64, 0i64);
    let (mut broken, mut recovered) = (0i64, 0i64);
    let njsons = jsons_array.len();
    for (i, json) in jsons_array.iter().enumerate() {
        if json.is_empty() {
            continue;
        }
        let st = parse_json(&con, ctx, i, njsons, json, &dt, flt);
        n += 1;
        f += st.found;
        e += st.events;
        broken += st.broken;
        recovered += st.recovered;
    }
    printf!(
        "Parsed: {}: {} JSONs, found {} matching, events {}\n",
        fname,
        n,
        f,
        e
    );
    if broken > 0 || recovered > 0 {
        printf!(
            "Recovered {}: {} broken JSON chunk(s) skipped, {} event(s) recovered\n",
            fname,
            broken,
            recovered
        );
    }
    // Mark date as computed, to skip fetching this JSON again when it contains no events for a current project
    mark_as_processed(&con, ctx, &dt);
    con.close();
}

/// Go `strconv.Atoi(hour)` (fatal on error) or the current local hour for
/// the `now` keyword.
fn parse_hour(arg: &str, now: DateTime<Local>) -> i64 {
    if arg.to_lowercase() == NOW {
        now.hour() as i64
    } else {
        fatal_on_err(devstatscode::time::parse_go_int(arg))
    }
}

/// Go `gha2db`: main work horse.
fn gha2db(args: &[String]) {
    // Current date
    let now = Local::now();
    let started = Instant::now();
    // Init stuff (Go also tunes the GC here — `debug.SetGCPercent(25)`)
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    let dctx = ctx.clone();
    let _deferred = if ctx.refresh_commit_roles {
        defer(move || roles::refresh_commit_roles(&dctx))
    } else {
        defer(move || roles::update_commit_roles(&dctx))
    };

    let (start_d, start_h, end_d, end_h) = (&args[0], &args[1], &args[2], &args[3]);

    // Parse from day & hour
    let hour_from = parse_hour(start_h, now);
    let d_from = if start_d.to_lowercase() == TODAY {
        HourDt::today(now, hour_from)
    } else {
        fatal_on_err(HourDt::parse(start_d, hour_from))
    };

    // Parse to day & hour (re-evaluated after every processed hour, so a
    // `today now` end moves along with the wall clock)
    let mut curr_now = now;
    let mut d_to = d_from;
    let date_to_func = |d_to: &mut HourDt, curr_now: &mut DateTime<Local>| {
        *curr_now = Local::now();
        let hour_to = parse_hour(end_h, *curr_now);
        *d_to = if end_d.to_lowercase() == TODAY {
            HourDt::today(*curr_now, hour_to)
        } else {
            fatal_on_err(HourDt::parse(end_d, hour_to))
        };
    };
    date_to_func(&mut d_to, &mut curr_now);

    // Stripping whitespace from org and repo params
    let strip = |x: &str| x.trim().to_string();
    let mut org: BTreeSet<String> = BTreeSet::new();
    let mut org_re: Option<GoRegex> = None;
    if args.len() >= 5 {
        if let Some(re) = args[4].strip_prefix("regexp:") {
            org_re = Some(GoRegex::must(re));
        } else {
            org = strings_map_to_set(strip, args[4].split(',').map(str::to_string).collect());
        }
    }
    let mut repo: BTreeSet<String> = BTreeSet::new();
    let mut repo_re: Option<GoRegex> = None;
    if args.len() >= 6 {
        if let Some(re) = args[5].strip_prefix("regexp:") {
            repo_re = Some(GoRegex::must(re));
        } else {
            repo = strings_map_to_set(strip, args[5].split(',').map(str::to_string).collect());
        }
    }

    // Get number of CPUs available
    let mut thr_n = get_threads_num(&mut ctx);
    printf!(
        "gha2db.go: Running ({} CPUs): {} - {} {} {}\n",
        thr_n,
        d_from,
        d_to,
        strings_set_keys(&org).join("+"),
        strings_set_keys(&repo).join("+")
    );

    // GDPR data hiding
    let sha_map = get_hidden(&ctx, HIDE_CFG_FILE);

    // Skipping JSON dates
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read GHA dates to skip
    let data = fatal_on_err(devstatscode::io::read_file(
        &ctx,
        &format!("{}{}", data_prefix, ctx.skip_dates_yaml),
    ));

    // Read list and convert it to set
    let skip_dates_list: SkipDatesList = fatal_on_err(yamlv2::de::unmarshal(&data));
    let skip_dates: BTreeSet<String> = skip_dates_list
        .dates
        .iter()
        .map(|date| to_ymdh_date(*date))
        .collect();

    let hider = maybe_hide_func(sha_map);
    let flt = Filters {
        forg: &org,
        frepo: &repo,
        org_re: org_re.as_ref(),
        repo_re: repo_re.as_ref(),
        maybe_hide: &hider,
        skip_dates: &skip_dates,
    };
    let flt = &flt;

    let mut igc = 0usize;
    let mut maybe_gc = || {
        igc += 1;
        if igc.is_multiple_of(24) {
            run_gc();
        }
    };

    let mut dt = d_from;
    let mut prc = 0usize;
    let ctx_ref = &ctx;
    if thr_n > 1 {
        let (tx, rx) = mpsc::channel::<HourDt>();
        let mut mp: HashMap<HourDt, ()> = HashMap::new();
        let mut n_threads = 0usize;
        std::thread::scope(|scope| {
            while dt <= d_to {
                date_to_func(&mut d_to, &mut curr_now);
                let tx = tx.clone();
                scope.spawn(move || {
                    get_gha_json(ctx_ref, dt, flt);
                    let _ = tx.send(dt);
                });
                mp.insert(dt, ());
                dt = dt.add_hour();
                n_threads += 1;
                while n_threads >= thr_n {
                    let prcdt = rx.recv().expect("worker sends its hour");
                    mp.remove(&prcdt);
                    n_threads -= 1;
                    date_to_func(&mut d_to, &mut curr_now);
                    maybe_gc();
                    prc += 1;
                    if prc.is_multiple_of(10) {
                        thr_n = get_threads_num(&mut ctx_ref.copy_context());
                    }
                }
            }
            printf!("Final threads join (processed {})\n", prc);
            while n_threads > 0 {
                if ctx_ref.debug >= 0 {
                    // Go iterates the map (random order); sorted here.
                    let mut dta: Vec<String> = mp.keys().map(|k| to_ymdh_date(k.dt)).collect();
                    dta.sort();
                    printf!("{} remain: {}\n", n_threads, dta.join(", "));
                }
                let prcdt = rx.recv().expect("worker sends its hour");
                mp.remove(&prcdt);
                n_threads -= 1;
                date_to_func(&mut d_to, &mut curr_now);
                maybe_gc();
            }
        });
    } else {
        printf!("Using single threaded version\n");
        while dt <= d_to {
            date_to_func(&mut d_to, &mut curr_now);
            get_gha_json(ctx_ref, dt, flt);
            dt = dt.add_hour();
            maybe_gc();
        }
    }
    // Finished (Go: `currNow.Sub(now)` — the last `dateToFunc` reading)
    let elapsed = (curr_now - now).to_std().unwrap_or_default();
    let _ = started;
    printf!("All done: {}\n", format_go_duration(elapsed));
}

fn main() {
    exit_on_panic();
    gofmt::mark_process_start();
    let dt_start = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    // Required args
    if args.len() < 5 {
        printf!(
            "Arguments required: date_from_YYYY-MM-DD hour_from_HH date_to_YYYY-MM-DD hour_to_HH \
             ['org1,org2,...,orgN' ['repo1,repo2,...,repoN']]\n"
        );
        std::process::exit(1);
    }
    gha2db(&args[1..]);
    printf!("Time: {}\n", format_go_duration(dt_start.elapsed()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn hour_dt_rendering() {
        let h = HourDt::parse("2015-01-01", 15).unwrap();
        assert_eq!(to_gha_date(h.dt), "2015-01-01-15");
        let utc = HourDt { named: true, ..h };
        assert_eq!(utc.go_string(), "2015-01-01 15:00:00 +0000 UTC");
        let nameless = HourDt { named: false, ..h };
        assert_eq!(nameless.go_string(), "2015-01-01 15:00:00 +0000 +0000");
        assert_eq!(utc.add_hour().go_string(), "2015-01-01 16:00:00 +0000 UTC");
        assert_eq!(
            HourDt::parse("2015-13-01", 0).unwrap_err(),
            "parsing time \"2015-13-01T00:00:00+00:00\": month out of range"
        );
        assert_eq!(
            HourDt::parse("2015-01-01", 24).unwrap_err(),
            "parsing time \"2015-01-01T24:00:00+00:00\": hour out of range"
        );
        assert_eq!(
            HourDt::parse("2015-02-30", 1).unwrap_err(),
            "parsing time \"2015-02-30T01:00:00+00:00\": day out of range"
        );
        assert_eq!(
            HourDt::parse("2015-01-01", 100).unwrap_err(),
            "parsing time \"2015-01-01T100:00:00+00:00\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"0:00:00+00:00\" as \":\""
        );
        assert_eq!(
            HourDt::parse("2015-01-01", -5).unwrap_err(),
            "parsing time \"2015-01-01T-5:00:00+00:00\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"-5:00:00+00:00\" as \"15\""
        );
    }

    #[test]
    fn today_uses_local_wall_clock_relabelled_as_utc() {
        let now = Local.with_ymd_and_hms(2024, 5, 6, 7, 8, 9).unwrap();
        let h = HourDt::today(now, 3);
        assert_eq!(h.go_string(), "2024-05-06 03:00:00 +0000 UTC");
        assert_eq!(h.dt.year(), 2024);
    }

    #[test]
    fn log_broken_json_saves_only_with_json_out_best_effort() {
        // Relative `jsons/` paths: run in a scratch working directory
        // (nothing else in this binary depends on the cwd).
        std::env::set_var("GHA2DB_SKIPLOG", "1");
        let dir = tempfile::tempdir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let dt = HourDt::parse("2015-01-01", 15).unwrap();
        let chunk: &[u8] = b"{\"id\":\"1\",\"repo\":{\"name\":\"a/b\xff\"}}";
        let mut ctx = Ctx::default();

        // Without GHA2DB_JSON nothing is saved (no jsons/ directory is needed).
        log_broken_json(&ctx, 48, 144, chunk, &dt, "broken JSON chunk", 1);
        assert!(!dir.path().join("jsons").exists());

        // With GHA2DB_JSON the chunk is saved as is, further chunks of the
        // same line get a -n suffix.
        ctx.json_out = true;
        std::fs::create_dir(dir.path().join("jsons")).unwrap();
        log_broken_json(&ctx, 48, 144, chunk, &dt, "broken JSON chunk", 1);
        log_broken_json(&ctx, 48, 144, b"garbage", &dt, "broken JSON chunk", 2);
        assert_eq!(
            std::fs::read(dir.path().join("jsons/error_2015-01-01-15-49-144.json")).unwrap(),
            chunk
        );
        assert_eq!(
            std::fs::read(dir.path().join("jsons/error_2015-01-01-15-49-144-2.json")).unwrap(),
            b"garbage"
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("jsons")).unwrap().count(),
            2
        );

        // Saving is best effort: a missing jsons/ directory never panics or exits.
        std::fs::remove_dir_all(dir.path().join("jsons")).unwrap();
        log_broken_json(&ctx, 0, 4, b"x", &dt, "broken JSON chunk", 1);
        assert!(!dir.path().join("jsons").exists());
    }
}
