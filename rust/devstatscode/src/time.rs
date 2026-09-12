//! Time helpers — port of `time.go` (plus Go `time.Duration` parsing/formatting
//! which several `Ctx` fields depend on).
//!
//! All calendar arithmetic is done in UTC, exactly like the Go code.

use std::process;
use std::time::Duration;

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc, Weekday};

use crate::consts;
use crate::context::Ctx;
use crate::error::fatalf;
use crate::rng;

/// A `time.Time -> time.Time` function (interval start/next/prev).
pub type TimeFn = fn(DateTime<Utc>) -> DateTime<Utc>;

/// Number of hours in a period like `"1 week"`, `"3 months"`, `"h"`, formatted
/// with Go's `%f` (6 decimals). Empty/blank input → `"0"`. Negative counts are
/// clamped to zero. Unknown interval names are fatal.
pub fn interval_hours(period: &str) -> String {
    let tokens: Vec<&str> = period.split(' ').filter(|t| !t.is_empty()).collect();
    if tokens.is_empty() {
        return "0".to_string();
    }
    let mut n = 1.0f64;
    let mut interval = tokens[0];
    if tokens.len() > 1 {
        n = crate::error::fatal_on_err(parse_go_float(tokens[0]));
        if n < 0.0 {
            n = 0.0;
        }
        interval = tokens[1];
    }
    let mul = match interval.to_lowercase().as_str() {
        "s" | "sec" | "second" | "secs" | "seconds" => 1.0 / 3600.0,
        "min" | "minute" | "mins" | "minutes" => 1.0 / 60.0,
        "h" | "hr" | "hour" | "hrs" | "hours" => 1.0,
        "d" | "day" | "days" => 24.0,
        "w" | "week" | "weeks" => 168.0,
        "month" | "months" => 730.5,
        "q" | "quarter" | "quarters" => 2191.5,
        "y" | "year" | "years" => 8766.0,
        _ => fatalf(format_args!("unknown interval '{}'\n", interval)),
    };
    format!("{:.6}", n * mul)
}

/// Go's `strconv.ParseFloat(s, 64)` (accepts things like `1`, `1.00`, `1e3`,
/// `inf`, `+Inf`, `nan`; rejects blanks, underscores-only, etc.).
pub fn parse_go_float(s: &str) -> Result<f64, String> {
    let err = || format!("strconv.ParseFloat: parsing \"{}\": invalid syntax", s);
    if s.is_empty() || s != s.trim() {
        return Err(err());
    }
    let lower = s.to_ascii_lowercase();
    let body = lower.trim_start_matches(['+', '-']);
    if body == "inf" || body == "infinity" || body == "nan" {
        return lower.parse::<f64>().map_err(|_| err());
    }
    if s.starts_with('.') || s.ends_with('.') || s.contains("e.") || s.contains(".e") {
        // Go accepts ".5" and "5." — Rust does too; keep them.
        return s.parse::<f64>().map_err(|_| err());
    }
    s.parse::<f64>().map_err(|_| err())
}

/// Hours between `from` and `to` as Go `%f`; `"0"` when `to` is not after `from`.
pub fn range_hours(from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    if to <= from {
        return "0".to_string();
    }
    let d = to - from;
    let hours = d
        .num_nanoseconds()
        .map(|n| n as f64 / 3.6e12)
        .unwrap_or_else(|| d.num_milliseconds() as f64 / 3.6e6);
    format!("{:.6}", hours)
}

/// Return true with `percent` % probability.
pub fn probab(percent: i64) -> bool {
    ((rng::next_u64() % 100) as i64) < percent
}

/// For some longer periods, only recalculate them on specific dates/times.
/// See `time_test.go` / the unit tests for the schedule.
pub fn compute_period_at_this_date(
    ctx: &Ctx,
    period: &str,
    idt: DateTime<Utc>,
    hist: bool,
) -> bool {
    if ctx.compute_all {
        return true;
    }
    if let Some(cp) = &ctx.compute_periods {
        return match cp.get(period) {
            None => false,
            Some(data) => data.contains(&hist),
        };
    }
    let dt = hour_start(idt);
    // dtc: date with current hour start, dtn: tomorrow with current hour start,
    // dth: current date with tz offset
    let dtc = dt;
    let dtn = dt + chrono::Duration::days(1);
    let dth = dt + chrono::Duration::hours(ctx.tm_offset);
    let h = dth.hour() as i64;
    let ch = dtc.hour() as i64;
    let period_start = &period[0..1.min(period.len())];
    if period_start == "h" {
        return true;
    } else if period_start == "d" {
        if period.len() == 1 {
            return true;
        }
        if ctx.rand_compute_at_this_date {
            return probab(25);
        }
        return h == 1 || h == 6 || h == 9 || h == 13 || h == 18 || h == 21;
    } else if hist && period_start == "a" {
        // histograms between annotations or the final one "a_num_n"
        let period_end = tail2(period);
        if period_end == "_n" {
            if ctx.rand_compute_at_this_date {
                return probab(25);
            }
            return h == 1 || h == 8 || h == 15 || h == 13 || h == 20;
        }
        if ctx.rand_compute_at_this_date {
            return probab(15);
        }
        return h == 2 || h == 3;
    } else if hist && period_start == "c" {
        // histograms between maturity levels: "c_n", "c_g_n", "c_i_n"
        if ctx.rand_compute_at_this_date {
            if tail2(period) == "_n" {
                return probab(25);
            }
            return probab(15);
        }
        return h == 3 || h == 4;
    }
    if hist {
        if period_start == "w" {
            if ctx.rand_compute_at_this_date {
                return probab(30);
            }
            return h % 7 == 0;
        } else if period_start == "m" || period_start == "q" || period_start == "y" {
            if ctx.rand_compute_at_this_date {
                return probab(15);
            }
            return h == 23 || h == 18;
        }
    } else {
        let wday = go_weekday(dtc);
        if period_start == "w" {
            if ctx.rand_compute_at_this_date {
                return probab(60) && h >= 12 && (0..=2).contains(&wday);
            }
            return ch == 23 && wday == 0;
        } else if period_start == "m" {
            if ctx.rand_compute_at_this_date {
                let dom = dtn.day();
                return probab(80) && h < 12 && (1..=4).contains(&dom);
            }
            return ch == 23 && dtn.day() == 1;
        } else if period_start == "q" {
            if ctx.rand_compute_at_this_date {
                let dom = dtn.day();
                return h > 12 && (1..=4).contains(&dom) && dtn.month() % 3 == 1;
            }
            return ch == 23 && dtn.day() == 1 && dtn.month() % 3 == 1;
        } else if period_start == "y" {
            if ctx.rand_compute_at_this_date {
                let dom = dtn.day();
                return h < 12 && (1..=4).contains(&dom) && dtn.month() == 1;
            }
            return ch == 23 && dtn.day() == 1 && dtn.month() == 1;
        }
    }
    fatalf(format_args!(
        "ComputePeriodAtThisDate: unknown period: '{}', hist: {}",
        period, hist
    ))
}

fn tail2(s: &str) -> &str {
    if s.len() >= 2 {
        &s[s.len() - 2..]
    } else {
        s
    }
}

/// Go's `time.Weekday()`: Sunday = 0 … Saturday = 6.
pub fn go_weekday(dt: DateTime<Utc>) -> i64 {
    match dt.weekday() {
        Weekday::Sun => 0,
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
    }
}

/// Build a UTC date-time; like Go's `time.Date` it normalizes out-of-range
/// months/days (e.g. month 13 → January of the next year).
pub fn ymd_hms(year: i32, month: i32, day: i32, hour: u32, min: u32, sec: u32) -> DateTime<Utc> {
    // normalize month
    let mut y = year;
    let mut m = month - 1;
    y += m.div_euclid(12);
    m = m.rem_euclid(12) + 1;
    let base = NaiveDate::from_ymd_opt(y, m as u32, 1).expect("valid year/month");
    let date = base + chrono::Duration::days(i64::from(day) - 1);
    Utc.from_utc_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .expect("midnight")
            .checked_add_signed(chrono::Duration::seconds(
                i64::from(hour) * 3600 + i64::from(min) * 60 + i64::from(sec),
            ))
            .expect("in range"),
    )
}

/// The wall clock of a (possibly non-UTC) time re-labelled as UTC — what
/// Go's `HourStart`/`DayStart`/… produce from a local `time.Time`
/// (`time.Date(dt.Year(), dt.Month(), …, time.UTC)` on the local components).
pub fn wall_as_utc<Tz: TimeZone>(dt: &DateTime<Tz>) -> DateTime<Utc> {
    Utc.from_utc_datetime(&dt.naive_local())
}

/// Time rounded down to the current hour start.
pub fn hour_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(
        dt.year(),
        dt.month() as i32,
        dt.day() as i32,
        dt.hour(),
        0,
        0,
    )
}
/// Next hour start.
pub fn next_hour_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    hour_start(dt) + chrono::Duration::hours(1)
}
/// Previous hour start.
pub fn prev_hour_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    hour_start(dt) - chrono::Duration::hours(1)
}
/// Current day start.
pub fn day_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(dt.year(), dt.month() as i32, dt.day() as i32, 0, 0, 0)
}
/// Next day start.
pub fn next_day_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    day_start(dt) + chrono::Duration::days(1)
}
/// Previous day start.
pub fn prev_day_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    day_start(dt) - chrono::Duration::days(1)
}
/// Current week start (weeks start on Monday, as in the Go code).
pub fn week_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    let sub_days = (go_weekday(dt) + 6) % 7;
    day_start(dt) - chrono::Duration::days(sub_days)
}
/// Next week start.
pub fn next_week_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    week_start(dt) + chrono::Duration::days(7)
}
/// Previous week start.
pub fn prev_week_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    week_start(dt) - chrono::Duration::days(7)
}
/// Current month start.
pub fn month_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(dt.year(), dt.month() as i32, 1, 0, 0, 0)
}
/// Next month start.
pub fn next_month_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(dt.year(), dt.month() as i32 + 1, 1, 0, 0, 0)
}
/// Previous month start.
pub fn prev_month_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(dt.year(), dt.month() as i32 - 1, 1, 0, 0, 0)
}
/// Current quarter start.
pub fn quarter_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    let month = ((dt.month() as i32 - 1) / 3) * 3 + 1;
    ymd_hms(dt.year(), month, 1, 0, 0, 0)
}
/// Next quarter start.
pub fn next_quarter_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    let q = quarter_start(dt);
    ymd_hms(q.year(), q.month() as i32 + 3, 1, 0, 0, 0)
}
/// Previous quarter start.
pub fn prev_quarter_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    let q = quarter_start(dt);
    ymd_hms(q.year(), q.month() as i32 - 3, 1, 0, 0, 0)
}
/// Current year start.
pub fn year_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(dt.year(), 1, 1, 0, 0, 0)
}
/// Next year start.
pub fn next_year_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(dt.year() + 1, 1, 1, 0, 0, 0)
}
/// Previous year start.
pub fn prev_year_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    ymd_hms(dt.year() - 1, 1, 1, 0, 0, 0)
}

/// Extract the duration from a GitHub API message containing
/// `[rate reset in 2m31s]`. Returns `None` when absent or unparsable.
pub fn period_parse(per_str: &str) -> Option<Duration> {
    let idx = per_str.find("[rate reset in ")?;
    // fmt.Sscanf("[rate reset in %s") reads one whitespace-delimited token
    let rest = &per_str[idx + "[rate reset in ".len()..];
    let token = rest.split_whitespace().next().unwrap_or("");
    if token.len() < 2 {
        return None;
    }
    // drop the closing ']' (Go drops the last byte whatever it is)
    let rate = &token[..token.len() - 1];
    if rate.is_empty() {
        return None;
    }
    parse_go_duration(rate).ok()
}

/// Parse a date/time in one of the formats accepted by Go's `TimeParseAny`:
/// `YYYY-MM-DDTHH:MI:SSZ`, `YYYY-MM-DD HH:MI:SS`, `YYYY-MM-DD HH:MI`,
/// `YYYY-MM-DD HH`, `YYYY-MM-DD`, `YYYY-MM`, `YYYY` (UTC). Like Go, the hour may
/// be 1 or 2 digits and an optional fractional seconds part is accepted after
/// the seconds. Returns `None` when nothing matches.
pub fn try_time_parse_any(dt_str: &str) -> Option<DateTime<Utc>> {
    let b = dt_str.as_bytes();
    let mut pos = 0usize;
    let fixed = |n: usize, pos: &mut usize| -> Option<u32> {
        if b.len() < *pos + n || !b[*pos..*pos + n].iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let v = std::str::from_utf8(&b[*pos..*pos + n])
            .ok()?
            .parse::<u32>()
            .ok()?;
        *pos += n;
        Some(v)
    };
    let year = fixed(4, &mut pos)?;
    let mut month = 1u32;
    let mut day = 1u32;
    let (mut hour, mut min, mut sec, mut nanos) = (0u32, 0u32, 0u32, 0u32);
    if pos < b.len() {
        if b[pos] != b'-' {
            return None;
        }
        pos += 1;
        month = fixed(2, &mut pos)?;
        if pos < b.len() {
            if b[pos] != b'-' {
                return None;
            }
            pos += 1;
            day = fixed(2, &mut pos)?;
            if pos < b.len() {
                let sep = b[pos];
                if sep != b' ' && sep != b'T' {
                    return None;
                }
                pos += 1;
                // hour: 1 or 2 digits (Go "15")
                let start = pos;
                while pos < b.len() && pos - start < 2 && b[pos].is_ascii_digit() {
                    pos += 1;
                }
                if pos == start {
                    return None;
                }
                hour = std::str::from_utf8(&b[start..pos]).ok()?.parse().ok()?;
                if sep == b'T' {
                    // only the full "2006-01-02T15:04:05Z" layout uses 'T'
                    if pos >= b.len() || b[pos] != b':' {
                        return None;
                    }
                }
                if pos < b.len() {
                    if b[pos] != b':' {
                        return None;
                    }
                    pos += 1;
                    min = fixed(2, &mut pos)?;
                    if sep == b'T' && (pos >= b.len() || b[pos] != b':') {
                        return None;
                    }
                    if pos < b.len() {
                        if b[pos] != b':' {
                            return None;
                        }
                        pos += 1;
                        sec = fixed(2, &mut pos)?;
                        // optional fractional seconds
                        if pos + 1 < b.len()
                            && (b[pos] == b'.' || b[pos] == b',')
                            && b[pos + 1].is_ascii_digit()
                        {
                            pos += 1;
                            let start = pos;
                            while pos < b.len() && b[pos].is_ascii_digit() {
                                pos += 1;
                            }
                            let frac = &dt_str[start..pos];
                            let mut f = frac.to_string();
                            f.truncate(9);
                            while f.len() < 9 {
                                f.push('0');
                            }
                            nanos = f.parse().ok()?;
                        }
                        if sep == b'T' {
                            if pos >= b.len() || b[pos] != b'Z' {
                                return None;
                            }
                            pos += 1;
                        }
                    }
                }
            }
        }
    }
    if pos != b.len() {
        return None;
    }
    if !(1..=12).contains(&month) || hour > 23 || min > 59 || sec > 59 {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(year as i32, month, day)?;
    let t = date.and_hms_nano_opt(hour, min, sec, nanos)?;
    Some(Utc.from_utc_datetime(&t))
}

/// Parse a date/time (see [`try_time_parse_any`]); on failure prints
/// `Error:\nCannot parse date: '<s>'` to stdout and exits with status 1
/// (this is what the Go tool does).
pub fn time_parse_any(dt_str: &str) -> DateTime<Utc> {
    match try_time_parse_any(dt_str) {
        Some(t) => t,
        None => {
            // Only log when the logger already exists: initializing it here
            // would recurse into `Ctx::init()` (see the Go fix for the deadlock).
            if crate::log::is_log_initialized() {
                crate::log::printf(&format!("Error:\nCannot parse date: '{}'\n", dt_str));
            }
            println!("Error:\nCannot parse date: '{}'", dt_str);
            process::exit(1)
        }
    }
}

// The `to_*_date` formatters print the wall-clock fields in the value's own
// zone, like the Go `ToXxxDate(t)` helpers (`t.Year()`, `t.Hour()`, …), so
// `to_ymdhms_date(Local::now())` matches Go's `ToYMDHMSDate(time.Now())`.

/// `YYYY-MM-DD-H` (GH Archive file name style).
pub fn to_gha_date<Tz: TimeZone>(dt: DateTime<Tz>) -> String {
    format!(
        "{:04}-{:02}-{:02}-{}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour()
    )
}
/// `YYYY-MM-DD`.
pub fn to_ymd_date<Tz: TimeZone>(dt: DateTime<Tz>) -> String {
    format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day())
}
/// `YYYY-MM-DD HH:MI:SS`.
pub fn to_ymdhms_date<Tz: TimeZone>(dt: DateTime<Tz>) -> String {
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}
/// `YYYY-MM-DD H`.
pub fn to_ymdh_date<Tz: TimeZone>(dt: DateTime<Tz>) -> String {
    format!(
        "{:04}-{:02}-{:02} {}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour()
    )
}

/// Human description of a period given in hours (`336` → `2 weeks`,
/// `167.9` → `6 days 23 hours 54 minutes`, `0` → `zero`, negative → `- ...`).
/// (Go name: `DescriblePeriodInHours`.)
pub fn describe_period_in_hours(hrs: f64) -> String {
    let mut secs = (hrs * 3600.0 + 0.5) as i64;
    if secs < 0 {
        return format!("- {}", describe_period_in_hours(-hrs));
    }
    if secs == 0 {
        return "zero".to_string();
    }
    let mut desc = String::new();
    let unit = |n: i64, one: &str, many: &str, desc: &mut String| {
        if n > 0 {
            if n > 1 {
                desc.push_str(&format!("{} {} ", n, many));
            } else {
                desc.push_str(&format!("1 {} ", one));
            }
        }
    };
    let weeks = secs / 604800;
    unit(weeks, "week", "weeks", &mut desc);
    secs -= weeks * 604800;
    let days = secs / 86400;
    unit(days, "day", "days", &mut desc);
    secs -= days * 86400;
    let hours = secs / 3600;
    unit(hours, "hour", "hours", &mut desc);
    secs -= hours * 3600;
    let minutes = secs / 60;
    unit(minutes, "minute", "minutes", &mut desc);
    secs -= minutes * 60;
    unit(secs, "second", "seconds", &mut desc);
    desc.trim().to_string()
}

/// Add (`n > 0`, using `next`) or subtract (`n < 0`, using `prev`) `|n|` intervals.
pub fn add_n_intervals(mut dt: DateTime<Utc>, n: i64, next: TimeFn, prev: TimeFn) -> DateTime<Utc> {
    if n == 0 {
        return dt;
    }
    let (times, fun) = if n < 0 { (-n, prev) } else { (n, next) };
    for _ in 0..times {
        dt = fun(dt);
    }
    dt
}

/// Interval description returned by [`get_interval_functions`].
#[derive(Debug, Clone, Copy)]
pub struct IntervalFunctions {
    /// `hour`, `day`, `week`, `month`, `quarter`, `year` or `""` for unknown.
    pub interval: &'static str,
    /// Number of intervals (`d7` → 7); at least 1.
    pub n: i64,
    pub start: Option<TimeFn>,
    pub next: Option<TimeFn>,
    pub prev: Option<TimeFn>,
}

/// Map an interval abbreviation (`h`, `d2`, `w3`, `m4`, `q`, `y10`) to its
/// name, count and start/next/prev functions. Unknown abbreviations are fatal
/// (stdout `Error:\nUnknown interval '<abbr>'`, exit 1) unless `allow_unknown`.
pub fn get_interval_functions(interval_abbr: &str, allow_unknown: bool) -> IntervalFunctions {
    let mut res = IntervalFunctions {
        interval: "",
        n: 1,
        start: None,
        next: None,
        prev: None,
    };
    let first = interval_abbr.get(0..1).unwrap_or("").to_lowercase();
    let (interval, start, next, prev): (&'static str, TimeFn, TimeFn, TimeFn) = match first.as_str()
    {
        "h" => (consts::HOUR, hour_start, next_hour_start, prev_hour_start),
        "d" => (consts::DAY, day_start, next_day_start, prev_day_start),
        "w" => (consts::WEEK, week_start, next_week_start, prev_week_start),
        "m" => (
            consts::MONTH,
            month_start,
            next_month_start,
            prev_month_start,
        ),
        "q" => (
            consts::QUARTER,
            quarter_start,
            next_quarter_start,
            prev_quarter_start,
        ),
        "y" => (consts::YEAR, year_start, next_year_start, prev_year_start),
        _ => {
            if !allow_unknown {
                crate::log::printf(&format!("Error:\nUnknown interval '{}'\n", interval_abbr));
                println!("Error:\nUnknown interval '{}'", interval_abbr);
                process::exit(1);
            }
            return res;
        }
    };
    res.interval = interval;
    res.start = Some(start);
    res.next = Some(next);
    res.prev = Some(prev);
    if interval_abbr.len() > 1 {
        let n_str = &interval_abbr[1..];
        let n = crate::error::fatal_on_err(parse_go_int(n_str));
        if n > 1 {
            res.n = n;
        }
    }
    res
}

/// Go's `strconv.Atoi` (optional sign, decimal digits only, no blanks).
pub fn parse_go_int(s: &str) -> Result<i64, String> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    let ok = !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit());
    if ok {
        if let Ok(v) = s.parse::<i64>() {
            return Ok(v);
        }
        return Err(format!(
            "strconv.Atoi: parsing {}: value out of range",
            crate::pg::go_quote(s)
        ));
    }
    Err(format!(
        "strconv.Atoi: parsing {}: invalid syntax",
        crate::pg::go_quote(s)
    ))
}

/// Go's `time.ParseDuration`: `[-+]?([0-9]*(\.[0-9]*)?(ns|us|µs|ms|s|m|h))+`, or `0`.
pub fn parse_go_duration(s: &str) -> Result<Duration, String> {
    let orig = s;
    let invalid = || format!("time: invalid duration \"{}\"", orig);
    let mut rest = s;
    let mut neg = false;
    if let Some(r) = rest.strip_prefix('-') {
        neg = true;
        rest = r;
    } else if let Some(r) = rest.strip_prefix('+') {
        rest = r;
    }
    if rest == "0" {
        return Ok(Duration::ZERO);
    }
    if rest.is_empty() {
        return Err(invalid());
    }
    let mut total_ns: f64 = 0.0;
    while !rest.is_empty() {
        // number part: digits, optionally a single '.' followed by digits
        let int_len = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
        let mut num_len = int_len;
        let mut frac_len = 0;
        if rest[num_len..].starts_with('.') {
            frac_len = rest[num_len + 1..]
                .bytes()
                .take_while(|b| b.is_ascii_digit())
                .count();
            num_len += 1 + frac_len;
        }
        if int_len == 0 && frac_len == 0 {
            return Err(invalid());
        }
        let num = &rest[..num_len];
        let value: f64 = if num.starts_with('.') {
            format!("0{}", num).parse().map_err(|_| invalid())?
        } else if let Some(int_part) = num.strip_suffix('.') {
            int_part.parse().map_err(|_| invalid())?
        } else {
            num.parse().map_err(|_| invalid())?
        };
        rest = &rest[num_len..];
        let unit_len = rest
            .char_indices()
            .take_while(|(_, c)| !c.is_ascii_digit() && *c != '.')
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        let unit = &rest[..unit_len];
        if unit.is_empty() {
            return Err(format!("time: missing unit in duration \"{}\"", orig));
        }
        let mul: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            _ => {
                return Err(format!(
                    "time: unknown unit \"{}\" in duration \"{}\"",
                    unit, orig
                ))
            }
        };
        total_ns += value * mul;
        rest = &rest[unit_len..];
    }
    if total_ns > i64::MAX as f64 {
        return Err(invalid());
    }
    if neg {
        return Err(format!("negative duration not representable: \"{}\"", orig));
    }
    Ok(Duration::from_nanos(total_ns.round() as u64))
}

/// Go's `time.Duration.String()`: `9h0m0s`, `2m31s`, `1.5s`, `150ms`, `0s`.
pub fn format_go_duration(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns == 0 {
        return "0s".to_string();
    }
    if ns < 1_000_000_000 {
        // sub-second: use ns / µs / ms with fractional part
        let (val, unit): (f64, &str) = if ns < 1_000 {
            return format!("{}ns", ns);
        } else if ns < 1_000_000 {
            (ns as f64 / 1e3, "µs")
        } else {
            (ns as f64 / 1e6, "ms")
        };
        return format!("{}{}", trim_float(val), unit);
    }
    let total_secs = ns / 1_000_000_000;
    let frac_ns = (ns % 1_000_000_000) as u64;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    let mut out = String::new();
    if hours > 0 {
        out.push_str(&format!("{}h", hours));
    }
    if hours > 0 || mins > 0 {
        out.push_str(&format!("{}m", mins));
    }
    if frac_ns == 0 {
        out.push_str(&format!("{}s", secs));
    } else {
        let mut frac = format!("{:09}", frac_ns);
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push_str(&format!("{}.{}s", secs, frac));
    }
    out
}

fn trim_float(v: f64) -> String {
    let s = format!("{}", v);
    s
}

/// Go `GetDateAgo`: `from - ago` computed by PostgreSQL
/// (`select $1::timestamp - $2::interval`); `from` is passed as its
/// `YYYY-MM-DD HH:MM:SS` wall clock in its own zone (Go `ToYMDHMSDate`).
pub fn get_date_ago<Tz: TimeZone>(
    con: &crate::pg::PgConn,
    ctx: &Ctx,
    from: DateTime<Tz>,
    ago: &str,
) -> DateTime<Utc> {
    let mut rows = crate::pg::api::query_sql_with_err(
        con,
        ctx,
        &format!(
            "select {}::timestamp - {}::interval",
            crate::pg::api::n_value(1),
            crate::pg::api::n_value(2)
        ),
        &[to_ymdhms_date(from).into(), ago.into()],
    );
    let mut tm = DateTime::<Utc>::default();
    while rows.next() {
        crate::pg::api::fatal_on_pg_err(rows.scan(&mut [&mut tm]));
    }
    crate::pg::api::fatal_on_pg_err(rows.err());
    crate::pg::api::fatal_on_pg_err(rows.close());
    tm
}

/// Display info about progress `i/n` if now >= `last + period`; updates `last`.
pub fn progress_info(
    i: usize,
    n: usize,
    start: DateTime<Utc>,
    last: &mut DateTime<Utc>,
    period: Duration,
    msg: &str,
) {
    let now = Utc::now();
    let period = chrono::Duration::from_std(period).unwrap_or(chrono::Duration::MAX);
    if *last + period < now {
        let perc = if n > 0 {
            (i as f64 * 100.0) / n as f64
        } else {
            0.0
        };
        if i > 0 && n > 0 {
            let elapsed_ns = (now - start).num_nanoseconds().unwrap_or(i64::MAX) as f64;
            let eta_ns = elapsed_ns * (n as f64 / i as f64);
            let eta = start + chrono::Duration::nanoseconds(eta_ns as i64);
            if !msg.is_empty() {
                crate::log::printf(&format!(
                    "{}/{} ({:.3}%), ETA: {}: {}\n",
                    i,
                    n,
                    perc,
                    crate::gofmt::time(eta),
                    msg
                ));
            } else {
                crate::log::printf(&format!(
                    "{}/{} ({:.3}%), ETA: {}\n",
                    i,
                    n,
                    perc,
                    crate::gofmt::time(eta)
                ));
            }
        } else {
            crate::log::printf(&format!("{}\n", msg));
        }
        *last = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// `testlib.YMDHMS` equivalent: missing parts default to month/day 1, time 0.
    fn ft(parts: &[i32]) -> DateTime<Utc> {
        let g = |i: usize, d: i32| parts.get(i).copied().unwrap_or(d);
        ymd_hms(
            g(0, 1970),
            g(1, 1),
            g(2, 1),
            g(3, 0) as u32,
            g(4, 0) as u32,
            g(5, 0) as u32,
        )
    }

    #[test]
    fn interval_hours_table() {
        let cases = [
            ("", "0"),
            ("h", "1.000000"),
            (" 1 h ", "1.000000"),
            ("1.00 h and whatever else", "1.000000"),
            ("2 hrs", "2.000000"),
            ("3 hour", "3.000000"),
            ("4.5 hours", "4.500000"),
            ("1 day", "24.000000"),
            ("1 week", "168.000000"),
            ("10 days", "240.000000"),
            ("1 month", "730.500000"),
            ("3 months", "2191.500000"),
            ("1 quarter", "2191.500000"),
            ("1 year", "8766.000000"),
            ("10 years", "87660.000000"),
            ("100 years", "876600.000000"),
            ("15 minutes", "0.250000"),
            ("20 mins", "0.333333"),
            ("180 sec", "0.050000"),
            ("-10 days", "0.000000"),
        ];
        for (period, want) in cases {
            assert_eq!(interval_hours(period), want, "period {period:?}");
        }
    }

    #[test]
    fn range_hours_table() {
        assert_eq!(
            range_hours(ft(&[2017, 8, 29, 12, 29, 3]), ft(&[2017, 8, 29, 14, 29, 3])),
            "2.000000"
        );
        assert_eq!(
            range_hours(ft(&[2017, 8, 29, 14, 29, 3]), ft(&[2017, 8, 29, 12, 29, 3])),
            "0"
        );
        assert_eq!(
            range_hours(ft(&[2020, 3, 13, 12, 0, 0]), ft(&[2020, 3, 13, 12, 0, 1])),
            "0.000278"
        );
    }

    #[test]
    fn compute_period_at_this_date_table() {
        // (tm_offset, period, dt parts, expected)
        let cases: &[(i64, &str, &[i32], bool)] = &[
            (0, "h", &[2017, 12, 19], true),
            (0, "h", &[2017, 12, 19, 3], true),
            (0, "h", &[2017, 12, 19, 5, 45, 17], true),
            (0, "h2", &[2017, 12, 19], true),
            (0, "h12", &[2017, 12, 19, 3], true),
            (0, "h240", &[2017, 12, 19, 5, 45, 17], true),
            (0, "d", &[2017, 12, 19], true),
            (0, "d", &[2017, 12, 19, 3], true),
            (0, "d", &[2017, 12, 19, 5, 45, 17], true),
            (0, "d2", &[2017, 12, 19], false),
            (0, "d3", &[2017, 12, 19, 3], false),
            (0, "d7", &[2017, 12, 19, 6, 45, 17], true),
            (0, "d7", &[2017, 12, 19, 9, 45, 17], true),
            (0, "d7", &[2017, 12, 19, 13, 45, 17], true),
            (0, "d14", &[2017, 12, 19, 13, 45, 17], true),
            (0, "d14", &[2017, 12, 19, 12, 45, 17], false),
            (0, "a_13_n", &[2017, 12, 19], false),
            (0, "a_13_n", &[2017, 12, 19, 1], true),
            (0, "a_13_n", &[2017, 12, 19, 2, 11], false),
            (0, "a_13_n", &[2017, 12, 19, 4, 11], false),
            (0, "a_12_13", &[2017, 12, 19], false),
            (0, "a_0_1", &[2017, 12, 19, 1], false),
            (0, "a_10_11", &[2017, 12, 19, 2, 11], true),
            (0, "a_10_11", &[2017, 12, 19, 4, 11], false),
            (0, "w", &[2017, 12, 19], true),
            (0, "w", &[2017, 12, 19, 1], false),
            (0, "w", &[2017, 12, 19, 20, 13], false),
            (0, "w3", &[2017, 12, 19, 20, 13], false),
            (0, "w3", &[2017, 12, 19, 14, 13], true),
            (0, "m", &[2017, 12, 19, 23], true),
            (0, "q", &[2017, 12, 19, 23], true),
            (0, "y", &[2017, 12, 19, 23], true),
            (0, "y2", &[2017, 12, 19, 23], true),
            (0, "y3", &[2017, 12, 19, 23], true),
            (0, "y5", &[2017, 12, 19, 23], true),
            (0, "m2", &[2017, 12, 19, 23], true),
            (0, "m6", &[2017, 12, 19, 23], true),
            (0, "q3", &[2017, 12, 19, 23], true),
            (0, "y10", &[2017, 12, 19, 23], true),
            (0, "m", &[2017, 12, 19, 1], false),
            (0, "q", &[2017, 12, 19, 2], false),
            (0, "y", &[2017, 12, 19, 3], false),
            (0, "y2", &[2017, 12, 19, 3], false),
            (0, "y3", &[2017, 12, 19, 3], false),
            (0, "y4", &[2017, 12, 19, 3], false),
            (0, "m2", &[2017, 12, 19, 4], false),
            (0, "m6", &[2017, 12, 19, 4], false),
            (0, "q3", &[2017, 12, 19, 5], false),
            (0, "y10", &[2017, 12, 19, 5], false),
            (5, "h", &[2017, 12, 19, 19], true),
            (5, "h", &[2017, 12, 19, 22], true),
            (5, "h", &[2017, 12, 19, 0, 45, 17], true),
            (5, "h2", &[2017, 12, 19, 19], true),
            (5, "h12", &[2017, 12, 19, 22], true),
            (5, "h240", &[2017, 12, 19, 2, 45, 17], true),
            (5, "d", &[2017, 12, 19, 19], true),
            (5, "d", &[2017, 12, 19, 22], true),
            (5, "d", &[2017, 12, 19, 0, 45, 17], true),
            (5, "d2", &[2017, 12, 19, 19], false),
            (5, "d3", &[2017, 12, 19, 22], false),
            (5, "d7", &[2017, 12, 19, 1, 45, 17], true),
            (5, "d14", &[2017, 12, 19, 8, 45, 17], true),
            (5, "d14", &[2017, 12, 19, 7, 45, 17], false),
            (5, "a_13_n", &[2017, 12, 19, 19], false),
            (5, "a_13_n", &[2017, 12, 19, 20], true),
            (5, "a_13_n", &[2017, 12, 19, 21, 11], false),
            (5, "a_13_n", &[2017, 12, 19, 23, 11], false),
            (5, "a_12_13", &[2017, 12, 19, 19], false),
            (5, "a_0_1", &[2017, 12, 19, 20], false),
            (5, "a_10_11", &[2017, 12, 19, 21, 11], true),
            (5, "a_10_11", &[2017, 12, 19, 23, 11], false),
            (5, "w", &[2017, 12, 19, 19], true),
            (5, "w", &[2017, 12, 19, 20], false),
            (5, "w", &[2017, 12, 19, 15, 13], false),
            (5, "w3", &[2017, 12, 19, 15, 13], false),
            (5, "w3", &[2017, 12, 19, 2, 13], true),
            (5, "w3", &[2017, 12, 19, 9, 13], true),
            (5, "m", &[2017, 12, 19, 18], true),
            (5, "q", &[2017, 12, 19, 18], true),
            (5, "y", &[2017, 12, 19, 18], true),
            (5, "y5", &[2017, 12, 19, 18], true),
            (5, "m2", &[2017, 12, 19, 18], true),
            (5, "m6", &[2017, 12, 19, 18], true),
            (5, "q3", &[2017, 12, 19, 18], true),
            (5, "y10", &[2017, 12, 19, 18], true),
            (5, "m", &[2017, 12, 19, 20], false),
            (5, "q", &[2017, 12, 19, 21], false),
            (5, "y", &[2017, 12, 19, 22], false),
            (5, "y3", &[2017, 12, 19, 22], false),
            (5, "m2", &[2017, 12, 19, 23], false),
            (5, "m6", &[2017, 12, 19, 23], false),
            (5, "q3", &[2017, 12, 19], false),
            (5, "y10", &[2017, 12, 19], false),
            (-10, "h", &[2017, 12, 19, 10], true),
            (-10, "h", &[2017, 12, 19, 13], true),
            (-10, "h", &[2017, 12, 19, 15, 45, 17], true),
            (-10, "h2", &[2017, 12, 19, 10], true),
            (-10, "h12", &[2017, 12, 19, 3], true),
            (-10, "h240", &[2017, 12, 19, 15, 45, 17], true),
            (-10, "d", &[2017, 12, 19, 10], true),
            (-10, "d", &[2017, 12, 19, 13], true),
            (-10, "d", &[2017, 12, 19, 15, 45, 17], true),
            (-10, "d2", &[2017, 12, 19, 10], false),
            (-10, "d3", &[2017, 12, 19, 13], false),
            (-10, "d7", &[2017, 12, 19, 4, 45, 17], true),
            (-10, "d7", &[2017, 12, 19, 23, 45, 17], true),
            (-10, "d7", &[2017, 12, 19, 7, 45, 17], true),
            (-10, "d14", &[2017, 12, 19, 23, 45, 17], true),
            (-10, "d14", &[2017, 12, 19, 22, 45, 17], false),
            (-10, "a_13_n", &[2017, 12, 19, 10], false),
            (-10, "a_13_n", &[2017, 12, 19, 11], true),
            (-10, "a_13_n", &[2017, 12, 19, 12, 11], false),
            (-10, "a_13_n", &[2017, 12, 19, 14, 11], false),
            (-10, "a_12_13", &[2017, 12, 19, 10], false),
            (-10, "a_0_1", &[2017, 12, 19, 11], false),
            (-10, "a_10_11", &[2017, 12, 19, 12, 11], true),
            (-10, "a_10_11", &[2017, 12, 19, 14, 11], false),
            (-10, "w", &[2017, 12, 19, 10], true),
            (-10, "w", &[2017, 12, 19, 11], false),
            (-10, "w", &[2017, 12, 19, 6, 13], false),
            (-10, "w3", &[2017, 12, 19, 6, 13], false),
            (-10, "w3", &[2017, 12, 19, 7, 13], true),
            (-10, "w3", &[2017, 12, 19, 8, 13], false),
            (-10, "m", &[2017, 12, 19, 9], true),
            (-10, "q", &[2017, 12, 19, 9], true),
            (-10, "y", &[2017, 12, 19, 9], true),
            (-10, "y2", &[2017, 12, 19, 9], true),
            (-10, "m2", &[2017, 12, 19, 9], true),
            (-10, "m6", &[2017, 12, 19, 9], true),
            (-10, "q3", &[2017, 12, 19, 9], true),
            (-10, "y10", &[2017, 12, 19, 9], true),
            (-10, "m", &[2017, 12, 19, 11], false),
            (-10, "q", &[2017, 12, 19, 12], false),
            (-10, "y", &[2017, 12, 19, 13], false),
            (-10, "m2", &[2017, 12, 19, 14], false),
            (-10, "m6", &[2017, 12, 19, 14], false),
            (-10, "q3", &[2017, 12, 19, 15], false),
            (-10, "y10", &[2017, 12, 19, 15], false),
        ];
        let mut ctx = Ctx {
            rand_compute_at_this_date: false,
            ..Ctx::default()
        };
        for (i, (off, period, dt, want)) in cases.iter().enumerate() {
            ctx.tm_offset = *off;
            let got = compute_period_at_this_date(&ctx, period, ft(dt), true);
            assert_eq!(
                got,
                *want,
                "case {} period {period} offset {off} dt {dt:?}",
                i + 1
            );
        }
        // compute_all forces everything, compute_periods restricts
        ctx.compute_all = true;
        assert!(compute_period_at_this_date(
            &ctx,
            "y10",
            ft(&[2017, 12, 19, 11, 12, 13]),
            true
        ));
        ctx.compute_all = false;
        let mut cp = BTreeMap::new();
        cp.insert("y10".to_string(), BTreeSet::from([true]));
        ctx.compute_periods = Some(cp);
        assert!(compute_period_at_this_date(
            &ctx,
            "y10",
            ft(&[2017, 12, 19, 11]),
            true
        ));
        assert!(!compute_period_at_this_date(
            &ctx,
            "y10",
            ft(&[2017, 12, 19, 11]),
            false
        ));
        assert!(!compute_period_at_this_date(
            &ctx,
            "w",
            ft(&[2017, 12, 19, 11]),
            true
        ));
    }

    #[test]
    fn non_hist_charts_schedule() {
        let ctx = Ctx {
            rand_compute_at_this_date: false,
            ..Ctx::default()
        };
        // weekly chart: Sunday 23:00 (2017-12-17 is a Sunday)
        assert!(compute_period_at_this_date(
            &ctx,
            "w",
            ft(&[2017, 12, 17, 23]),
            false
        ));
        assert!(!compute_period_at_this_date(
            &ctx,
            "w",
            ft(&[2017, 12, 18, 23]),
            false
        ));
        // monthly chart: last hour of the month
        assert!(compute_period_at_this_date(
            &ctx,
            "m",
            ft(&[2017, 11, 30, 23]),
            false
        ));
        assert!(!compute_period_at_this_date(
            &ctx,
            "m",
            ft(&[2017, 11, 29, 23]),
            false
        ));
        // quarterly: last hour of a quarter
        assert!(compute_period_at_this_date(
            &ctx,
            "q",
            ft(&[2017, 12, 31, 23]),
            false
        ));
        assert!(!compute_period_at_this_date(
            &ctx,
            "q",
            ft(&[2017, 11, 30, 23]),
            false
        ));
        // yearly
        assert!(compute_period_at_this_date(
            &ctx,
            "y",
            ft(&[2017, 12, 31, 23]),
            false
        ));
        assert!(!compute_period_at_this_date(
            &ctx,
            "y",
            ft(&[2017, 9, 30, 23]),
            false
        ));
    }

    #[test]
    fn describe_period_table() {
        let cases: &[(f64, &str)] = &[
            (-337.0, "- 2 weeks 1 hour"),
            (0.0, "zero"),
            (336.0, "2 weeks"),
            (360.0, "2 weeks 1 day"),
            (337.0, "2 weeks 1 hour"),
            (338.0, "2 weeks 2 hours"),
            (335.0, "1 week 6 days 23 hours"),
            (168.0, "1 week"),
            (216.0, "1 week 2 days"),
            (169.0, "1 week 1 hour"),
            (170.0, "1 week 2 hours"),
            (167.0, "6 days 23 hours"),
            (167.9, "6 days 23 hours 54 minutes"),
            (168.2, "1 week 12 minutes"),
            (335.99, "1 week 6 days 23 hours 59 minutes 24 seconds"),
            (100.0, "4 days 4 hours"),
            (1000.0, "5 weeks 6 days 16 hours"),
            (0.3, "18 minutes"),
        ];
        for (h, want) in cases {
            assert_eq!(describe_period_in_hours(*h), *want, "hours {h}");
        }
    }

    #[test]
    fn period_parse_table() {
        let d = Some(Duration::from_secs(151));
        assert_eq!(
            period_parse("blah blah blah [rate reset in 2m31s] no more calls"),
            d
        );
        assert_eq!(period_parse("blah blah blah [rate reset in 2m31s]"), d);
        assert_eq!(period_parse("[rate reset in 2m31s] no more calls"), d);
        assert_eq!(period_parse("[rate reset in 2m31s]"), d);
        assert_eq!(period_parse("[rate reset in xxx]"), None);
        assert_eq!(period_parse("[rate reset in ]"), None);
        assert_eq!(period_parse("[rate reset in]"), None);
        assert_eq!(period_parse("blah blah blah"), None);
    }

    #[test]
    fn hour_day_starts() {
        assert_eq!(
            hour_start(ft(&[2017, 8, 29, 12, 29, 3])),
            ft(&[2017, 8, 29, 12])
        );
        assert_eq!(hour_start(ft(&[2017, 8, 29, 13])), ft(&[2017, 8, 29, 13]));
        assert_eq!(hour_start(ft(&[2018])), ft(&[2018]));
        assert_eq!(
            next_hour_start(ft(&[2017, 8, 29, 12, 29, 3])),
            ft(&[2017, 8, 29, 13])
        );
        assert_eq!(next_hour_start(ft(&[2018])), ft(&[2018, 1, 1, 1]));
        assert_eq!(
            next_hour_start(ft(&[2017, 12, 31, 23, 59, 59])),
            ft(&[2018])
        );
        assert_eq!(
            prev_hour_start(ft(&[2017, 8, 29, 12, 29, 3])),
            ft(&[2017, 8, 29, 11])
        );
        assert_eq!(prev_hour_start(ft(&[2018])), ft(&[2017, 12, 31, 23]));
        assert_eq!(
            prev_hour_start(ft(&[2017, 12, 31, 23, 59, 59])),
            ft(&[2017, 12, 31, 22])
        );
        assert_eq!(day_start(ft(&[2017, 8, 29, 12, 29, 3])), ft(&[2017, 8, 29]));
        assert_eq!(next_day_start(ft(&[2017, 8, 31, 13])), ft(&[2017, 9, 1]));
        assert_eq!(next_day_start(ft(&[2018])), ft(&[2018, 1, 2]));
        assert_eq!(next_day_start(ft(&[2017, 12, 31, 23, 59, 59])), ft(&[2018]));
        assert_eq!(
            prev_day_start(ft(&[2017, 8, 29, 12, 29, 3])),
            ft(&[2017, 8, 28])
        );
        assert_eq!(prev_day_start(ft(&[2018])), ft(&[2017, 12, 31]));
        assert_eq!(
            prev_day_start(ft(&[2017, 12, 31, 23, 59, 59])),
            ft(&[2017, 12, 30])
        );
    }

    #[test]
    fn week_starts() {
        assert_eq!(
            week_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 8, 21])
        );
        assert_eq!(week_start(ft(&[2017, 8, 23, 13])), ft(&[2017, 8, 21]));
        assert_eq!(week_start(ft(&[2017, 8, 13])), ft(&[2017, 8, 7]));
        assert_eq!(week_start(ft(&[2017, 8, 14])), ft(&[2017, 8, 14]));
        assert_eq!(week_start(ft(&[2017, 8, 15])), ft(&[2017, 8, 14]));
        assert_eq!(week_start(ft(&[2017])), ft(&[2016, 12, 26]));
        assert_eq!(
            next_week_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 8, 28])
        );
        assert_eq!(next_week_start(ft(&[2017, 8, 13])), ft(&[2017, 8, 14]));
        assert_eq!(next_week_start(ft(&[2017, 8, 14])), ft(&[2017, 8, 21]));
        assert_eq!(next_week_start(ft(&[2017, 12, 31])), ft(&[2018]));
        assert_eq!(
            prev_week_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 8, 14])
        );
        assert_eq!(prev_week_start(ft(&[2017, 8, 13])), ft(&[2017, 7, 31]));
        assert_eq!(prev_week_start(ft(&[2017, 8, 14])), ft(&[2017, 8, 7]));
        assert_eq!(prev_week_start(ft(&[2017, 12, 31])), ft(&[2017, 12, 18]));
    }

    #[test]
    fn month_quarter_year_starts() {
        assert_eq!(
            month_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 8, 1])
        );
        assert_eq!(month_start(ft(&[2017])), ft(&[2017]));
        assert_eq!(month_start(ft(&[2017, 12, 10])), ft(&[2017, 12]));
        assert_eq!(
            next_month_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 9, 1])
        );
        assert_eq!(next_month_start(ft(&[2017])), ft(&[2017, 2]));
        assert_eq!(next_month_start(ft(&[2017, 12, 10])), ft(&[2018]));
        assert_eq!(
            prev_month_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 7, 1])
        );
        assert_eq!(prev_month_start(ft(&[2017])), ft(&[2016, 12]));
        assert_eq!(prev_month_start(ft(&[2017, 12, 10])), ft(&[2017, 11]));
        assert_eq!(
            quarter_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 7, 1])
        );
        assert_eq!(quarter_start(ft(&[2017])), ft(&[2017]));
        assert_eq!(quarter_start(ft(&[2017, 12, 10])), ft(&[2017, 10]));
        assert_eq!(quarter_start(ft(&[2017, 10, 12])), ft(&[2017, 10]));
        assert_eq!(
            next_quarter_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 10])
        );
        assert_eq!(next_quarter_start(ft(&[2017])), ft(&[2017, 4]));
        assert_eq!(next_quarter_start(ft(&[2017, 12, 10])), ft(&[2018]));
        assert_eq!(next_quarter_start(ft(&[2017, 10, 12])), ft(&[2018]));
        assert_eq!(
            prev_quarter_start(ft(&[2017, 8, 26, 12, 29, 3])),
            ft(&[2017, 4])
        );
        assert_eq!(prev_quarter_start(ft(&[2017])), ft(&[2016, 10]));
        assert_eq!(prev_quarter_start(ft(&[2017, 12, 10])), ft(&[2017, 7]));
        assert_eq!(prev_quarter_start(ft(&[2017, 10, 12])), ft(&[2017, 7]));
        assert_eq!(year_start(ft(&[2017, 8, 26, 12, 29, 3])), ft(&[2017]));
        assert_eq!(year_start(ft(&[2017])), ft(&[2017]));
        assert_eq!(next_year_start(ft(&[2017, 8, 26, 12, 29, 3])), ft(&[2018]));
        assert_eq!(next_year_start(ft(&[2017])), ft(&[2018]));
        assert_eq!(prev_year_start(ft(&[2017, 8, 26, 12, 29, 3])), ft(&[2016]));
        assert_eq!(prev_year_start(ft(&[2017])), ft(&[2016]));
    }

    #[test]
    fn add_n_intervals_table() {
        assert_eq!(
            add_n_intervals(
                ft(&[2017, 1, 1, 13, 15]),
                3,
                next_hour_start,
                prev_hour_start
            ),
            ft(&[2017, 1, 1, 16])
        );
        assert_eq!(
            add_n_intervals(
                ft(&[2017, 1, 1, 13, 15]),
                -3,
                next_hour_start,
                prev_hour_start
            ),
            ft(&[2017, 1, 1, 10])
        );
        assert_eq!(
            add_n_intervals(
                ft(&[2017, 1, 1, 13, 15]),
                0,
                next_day_start,
                prev_quarter_start
            ),
            ft(&[2017, 1, 1, 13, 15])
        );
        assert_eq!(
            add_n_intervals(ft(&[2017, 9, 27]), -7, next_day_start, prev_day_start),
            ft(&[2017, 9, 20])
        );
    }

    #[test]
    fn get_interval_functions_table() {
        type Row = (
            &'static str,
            bool,
            &'static str,
            i64,
            Option<TimeFn>,
            Option<TimeFn>,
            Option<TimeFn>,
        );
        let cases: &[Row] = &[
            (
                "h",
                false,
                "hour",
                1,
                Some(hour_start),
                Some(next_hour_start),
                Some(prev_hour_start),
            ),
            (
                "d",
                false,
                "day",
                1,
                Some(day_start),
                Some(next_day_start),
                Some(prev_day_start),
            ),
            (
                "w",
                false,
                "week",
                1,
                Some(week_start),
                Some(next_week_start),
                Some(prev_week_start),
            ),
            (
                "m",
                false,
                "month",
                1,
                Some(month_start),
                Some(next_month_start),
                Some(prev_month_start),
            ),
            (
                "q",
                false,
                "quarter",
                1,
                Some(quarter_start),
                Some(next_quarter_start),
                Some(prev_quarter_start),
            ),
            (
                "y",
                false,
                "year",
                1,
                Some(year_start),
                Some(next_year_start),
                Some(prev_year_start),
            ),
            (
                "y2",
                false,
                "year",
                2,
                Some(year_start),
                Some(next_year_start),
                Some(prev_year_start),
            ),
            (
                "d7",
                false,
                "day",
                7,
                Some(day_start),
                Some(next_day_start),
                Some(prev_day_start),
            ),
            (
                "q0",
                false,
                "quarter",
                1,
                Some(quarter_start),
                Some(next_quarter_start),
                Some(prev_quarter_start),
            ),
            (
                "m-2",
                false,
                "month",
                1,
                Some(month_start),
                Some(next_month_start),
                Some(prev_month_start),
            ),
            ("a_0_1", true, "", 1, None, None, None),
            ("c_n", true, "", 1, None, None, None),
        ];
        for (abbr, allow, interval, n, s, nx, pv) in cases {
            let got = get_interval_functions(abbr, *allow);
            assert_eq!(got.interval, *interval, "{abbr}");
            assert_eq!(got.n, *n, "{abbr}");
            assert_eq!(
                got.start.map(|f| f as usize),
                s.map(|f| f as usize),
                "{abbr} start"
            );
            assert_eq!(
                got.next.map(|f| f as usize),
                nx.map(|f| f as usize),
                "{abbr} next"
            );
            assert_eq!(
                got.prev.map(|f| f as usize),
                pv.map(|f| f as usize),
                "{abbr} prev"
            );
        }
    }

    #[test]
    fn time_parse_any_formats() {
        assert_eq!(try_time_parse_any("2017"), Some(ft(&[2017])));
        assert_eq!(try_time_parse_any("2017-12"), Some(ft(&[2017, 12])));
        assert_eq!(try_time_parse_any("1982-07-16"), Some(ft(&[1982, 7, 16])));
        assert_eq!(
            try_time_parse_any("2010-01-01 12"),
            Some(ft(&[2010, 1, 1, 12]))
        );
        assert_eq!(
            try_time_parse_any("2010-01-01 5"),
            Some(ft(&[2010, 1, 1, 5]))
        );
        assert_eq!(
            try_time_parse_any("2010-01-01 12:30"),
            Some(ft(&[2010, 1, 1, 12, 30]))
        );
        assert_eq!(
            try_time_parse_any("1982-07-16 10:15:45"),
            Some(ft(&[1982, 7, 16, 10, 15, 45]))
        );
        assert_eq!(
            try_time_parse_any("2017-08-29T12:29:03Z"),
            Some(ft(&[2017, 8, 29, 12, 29, 3]))
        );
        assert_eq!(
            try_time_parse_any("2017-08-29 12:29:03.5"),
            Some(ft(&[2017, 8, 29, 12, 29, 3]) + chrono::Duration::milliseconds(500))
        );
        for bad in [
            "",
            "17",
            "2017-1-01",
            "2017-01-01T12",
            "2017-01-01T12:00:00",
            "2017-13-01",
            "2017-02-30",
            "2017-01-01 24",
            "2017-01-01 12:60",
            "2017-01-01 12:00:00 extra",
            "abcd",
            "2017-01-01x",
        ] {
            assert_eq!(try_time_parse_any(bad), None, "{bad:?} should not parse");
        }
    }

    #[test]
    fn date_formatting() {
        let t = ft(&[2017, 8, 9, 5, 4, 3]);
        assert_eq!(to_gha_date(t), "2017-08-09-5");
        assert_eq!(to_ymd_date(t), "2017-08-09");
        assert_eq!(to_ymdhms_date(t), "2017-08-09 05:04:03");
        assert_eq!(to_ymdh_date(t), "2017-08-09 5");
    }

    #[test]
    fn go_duration_roundtrip() {
        let cases = [
            ("9h", 32400.0),
            ("1h45m", 6300.0),
            ("2m31s", 151.0),
            ("12h", 43200.0),
            ("48h", 172800.0),
            ("1.5s", 1.5),
            ("150ms", 0.15),
            ("0", 0.0),
            ("1h1m1s1ms", 3661.001),
            ("+1s", 1.0),
            ("1.h", 3600.0),
            (".5h", 1800.0),
            ("1.5h30m", 7200.0),
        ];
        for (s, secs) in cases {
            let d = parse_go_duration(s).unwrap_or_else(|e| panic!("{s}: {e}"));
            assert!((d.as_secs_f64() - secs).abs() < 1e-9, "{s}");
        }
        for bad in [
            "", "1", "xxx", "1d", "1.", "1.5.h", "1h.", "h", "s", ".", "1..2s", "5m3", "1hh",
        ] {
            assert!(parse_go_duration(bad).is_err(), "{bad:?} should fail");
        }
        assert_eq!(
            parse_go_duration("1d").unwrap_err(),
            "time: unknown unit \"d\" in duration \"1d\""
        );
        assert_eq!(
            parse_go_duration("1").unwrap_err(),
            "time: missing unit in duration \"1\""
        );
        assert_eq!(
            parse_go_duration("1hh").unwrap_err(),
            "time: unknown unit \"hh\" in duration \"1hh\""
        );
        assert_eq!(
            parse_go_duration("1h.").unwrap_err(),
            "time: invalid duration \"1h.\""
        );
        assert_eq!(format_go_duration(Duration::from_secs(9 * 3600)), "9h0m0s");
        assert_eq!(format_go_duration(Duration::from_secs(6300)), "1h45m0s");
        assert_eq!(format_go_duration(Duration::from_secs(151)), "2m31s");
        assert_eq!(format_go_duration(Duration::from_millis(1500)), "1.5s");
        assert_eq!(format_go_duration(Duration::from_millis(150)), "150ms");
        assert_eq!(format_go_duration(Duration::from_micros(1500)), "1.5ms");
        assert_eq!(format_go_duration(Duration::from_nanos(1500)), "1.5µs");
        assert_eq!(format_go_duration(Duration::from_nanos(15)), "15ns");
        assert_eq!(format_go_duration(Duration::ZERO), "0s");
        assert_eq!(
            format_go_duration(Duration::from_secs(3661) + Duration::from_millis(1)),
            "1h1m1.001s"
        );
    }

    #[test]
    fn go_int_and_float_parsing() {
        assert_eq!(parse_go_int("42"), Ok(42));
        assert_eq!(parse_go_int("-1"), Ok(-1));
        assert_eq!(parse_go_int("+7"), Ok(7));
        assert!(parse_go_int("").is_err());
        assert!(parse_go_int(" 1").is_err());
        assert!(parse_go_int("1.5").is_err());
        assert!(parse_go_int("abc").is_err());
        // A lone sign is a syntax error (not "out of range"); Go quotes the
        // offending input like `%q`.
        assert_eq!(
            parse_go_int("-"),
            Err("strconv.Atoi: parsing \"-\": invalid syntax".to_string())
        );
        assert_eq!(
            parse_go_int("+"),
            Err("strconv.Atoi: parsing \"+\": invalid syntax".to_string())
        );
        assert_eq!(
            parse_go_int("--5"),
            Err("strconv.Atoi: parsing \"--5\": invalid syntax".to_string())
        );
        assert_eq!(
            parse_go_int("99999999999999999999"),
            Err("strconv.Atoi: parsing \"99999999999999999999\": value out of range".to_string())
        );
        assert_eq!(
            parse_go_int("a\"b"),
            Err("strconv.Atoi: parsing \"a\\\"b\": invalid syntax".to_string())
        );
        assert_eq!(parse_go_float("3.75"), Ok(3.75));
        assert_eq!(parse_go_float("1"), Ok(1.0));
        assert_eq!(parse_go_float("1.00"), Ok(1.0));
        assert!(parse_go_float("").is_err());
        assert!(parse_go_float("x").is_err());
    }

    #[test]
    fn probab_bounds() {
        assert!(!probab(0));
        assert!(probab(100));
    }
}
