//! Go `fmt` `%v` / `%+v` style value formatting.
//!
//! Several DevStats outputs are literally Go `fmt.Sprintf("%v", ...)` results
//! (the `GHA2DB_CTXOUT` context dump, command/environment echo in `exec.go`,
//! and — importantly — the strings that get hashed into artificial event ids
//! in `hash.go`). This module reproduces the relevant subset of Go's rules so
//! the Rust port prints and hashes the same bytes.

use std::collections::BTreeMap;
use std::fmt::Display;

use chrono::{DateTime, Datelike, FixedOffset, TimeZone, Timelike};

/// Go `%v` of a `float64` (`strconv.FormatFloat(v, 'g', -1, 64)`): shortest
/// round-trip digits, exponent form when the decimal exponent is `< -4` or
/// `>= 6` (the "shortest" precision rule): `1e6` → `1e+06`, `123456` →
/// `123456`, `0.0001` → `0.0001`, `1e-5` → `1e-05`.
pub fn float(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        };
    }
    let (neg, digits, exp10) = shortest_digits(v);
    // `exp` is the decimal exponent of the first digit (d.ddd × 10^exp).
    let exp = exp10 + digits.len() as i32 - 1;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if !(-4..6).contains(&exp) {
        // %e with shortest digits: d[.ddd]e±XX
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", exp.abs()));
    } else {
        // %f with as many decimals as needed
        let point = exp + 1; // number of integer digits
        if point <= 0 {
            out.push_str("0.");
            for _ in 0..(-point) {
                out.push('0');
            }
            out.push_str(&digits);
        } else if (point as usize) >= digits.len() {
            out.push_str(&digits);
            for _ in 0..(point as usize - digits.len()) {
                out.push('0');
            }
        } else {
            out.push_str(&digits[..point as usize]);
            out.push('.');
            out.push_str(&digits[point as usize..]);
        }
    }
    out
}

/// JSON (encoding/json & jsoniter) formatting of a `float64`: `'f'` format
/// unless `abs < 1e-6 || abs >= 1e21`, in which case `'e'` with the exponent
/// cleaned up (`e-07` → `e-7`).
pub fn json_float(v: f64) -> String {
    if !v.is_finite() {
        return "null".to_string();
    }
    if v == 0.0 {
        return "0".to_string();
    }
    let abs = v.abs();
    let (neg, digits, exp10) = shortest_digits(v);
    let exp = exp10 + digits.len() as i32 - 1;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if !(1e-6..1e21).contains(&abs) {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        let e = exp.abs();
        if e < 10 {
            out.push_str(&e.to_string());
        } else {
            out.push_str(&format!("{:02}", e));
        }
    } else {
        let point = exp + 1;
        if point <= 0 {
            out.push_str("0.");
            for _ in 0..(-point) {
                out.push('0');
            }
            out.push_str(&digits);
        } else if (point as usize) >= digits.len() {
            out.push_str(&digits);
            for _ in 0..(point as usize - digits.len()) {
                out.push('0');
            }
        } else {
            out.push_str(&digits[..point as usize]);
            out.push('.');
            out.push_str(&digits[point as usize..]);
        }
    }
    out
}

/// Shortest round-trip decimal digits of `v` (non-zero, finite):
/// returns (negative, digits without dot or leading zeros, exponent of the last digit).
fn shortest_digits(v: f64) -> (bool, String, i32) {
    // Rust's `{:e}` prints the shortest round-trip mantissa, e.g. "1.2345e2", "-5e-7".
    let s = format!("{:e}", v);
    let (mant, exp) = s.split_once('e').expect("exponent form");
    let exp: i32 = exp.parse().expect("exponent");
    let neg = mant.starts_with('-');
    let mant = mant.trim_start_matches('-');
    let (int_part, frac_part) = match mant.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mant, ""),
    };
    let mut digits = format!("{}{}", int_part, frac_part);
    let mut last_exp = exp - frac_part.len() as i32;
    // strip trailing zeros (should not happen with {:e}, but be safe)
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
        last_exp += 1;
    }
    (neg, digits, last_exp)
}

/// Go `%v` of a slice: `[a b c]`.
pub fn slice<T: Display>(items: &[T]) -> String {
    let parts: Vec<String> = items.iter().map(|i| i.to_string()).collect();
    format!("[{}]", parts.join(" "))
}

/// Go `%v` of a `map[K]V` (keys sorted, as Go does since 1.12): `map[a:1 b:2]`.
pub fn map<K: Display, V: Display>(m: &BTreeMap<K, V>) -> String {
    let parts: Vec<String> = m.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
    format!("map[{}]", parts.join(" "))
}

/// Go `%v` of a `time.Time`: `2012-07-01 00:00:00 +0000 UTC` (fractional
/// seconds only when non-zero, trailing zeros trimmed). Times with another
/// offset print it twice (`+0200 +0200`) — the form Go uses for a zone without
/// a name; the abbreviation Go would print for a local time (`CEST`) is not
/// reproduced.
pub fn time<Tz: TimeZone>(t: DateTime<Tz>) -> String {
    let t: DateTime<FixedOffset> = t.fixed_offset();
    let nanos = t.nanosecond();
    let mut s = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        t.year(),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    if nanos != 0 {
        let mut frac = format!("{:09}", nanos);
        while frac.ends_with('0') {
            frac.pop();
        }
        s.push('.');
        s.push_str(&frac);
    }
    let off = t.offset().local_minus_utc();
    if off == 0 {
        s.push_str(" +0000 UTC");
    } else {
        let a = off.unsigned_abs();
        let z = format!(
            "{}{:02}{:02}",
            if off < 0 { '-' } else { '+' },
            a / 3600,
            (a % 3600) / 60
        );
        s.push_str(&format!(" {z} {z}"));
    }
    s
}

static PROCESS_START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// Record the process start for [`time_now`]'s monotonic reading (call
/// first thing in `main`; otherwise the first use counts as the start).
pub fn mark_process_start() {
    PROCESS_START.get_or_init(std::time::Instant::now);
}

/// Go `%v` of `time.Now()`: the wall clock like [`time`] followed by the
/// monotonic reading `m=+<seconds since process start>`.
pub fn time_now() -> String {
    let start = *PROCESS_START.get_or_init(std::time::Instant::now);
    let el = start.elapsed();
    format!(
        "{} m=+{}.{:09}",
        time(chrono::Local::now()),
        el.as_secs(),
        el.subsec_nanos()
    )
}

/// Go `%v` of a value decoded from JSON into `interface{}`
/// (`nil` → `<nil>`, numbers are `float64`, maps print with sorted keys).
pub fn json_value(v: &serde_json::Value) -> String {
    use serde_json::Value;
    match v {
        Value::Null => "<nil>".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => float(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) => s.clone(),
        Value::Array(a) => {
            let parts: Vec<String> = a.iter().map(json_value).collect();
            format!("[{}]", parts.join(" "))
        }
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .iter()
                .map(|k| format!("{}:{}", k, json_value(&o[*k])))
                .collect();
            format!("map[{}]", parts.join(" "))
        }
    }
}

/// Go `fmt` scanning `isSpace` (the `space` table of `fmt/scan.go`).
pub fn scan_is_space(r: char) -> bool {
    let r = r as u32;
    matches!(
        r,
        0x0009..=0x000d
            | 0x0020
            | 0x0085
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028..=0x2029
            | 0x202f
            | 0x205f
            | 0x3000
    )
}

/// Go `n, err := fmt.Sscanf(input, "%d"+rest, &v)` for a format starting
/// with `%d` and continuing with literal text (spaces allowed, no newlines,
/// no further verbs): `Some(v)` when `n == 1 && err == nil`. As in Go the
/// integer may carry a sign, a run of spaces in the format needs one or more
/// spaces (or the end) in the input, literals must match exactly and any
/// input after the format is ignored.
pub fn sscanf_int_prefix(input: &str, rest: &str) -> Option<i64> {
    let chars: Vec<char> = input.chars().collect();
    let mut pos = 0usize;
    // SkipSpace (newlines are not spaces for Sscanf)
    loop {
        let Some(&r) = chars.get(pos) else {
            return None; // notEOF: unexpected EOF
        };
        if r == '\r' && chars.get(pos + 1) == Some(&'\n') {
            pos += 1;
            continue;
        }
        if r == '\n' {
            return None; // unexpected newline
        }
        if !scan_is_space(r) {
            break;
        }
        pos += 1;
    }
    let mut tok = String::new();
    if matches!(chars.get(pos), Some('+') | Some('-')) {
        tok.push(chars[pos]);
        pos += 1;
    }
    let digits_start = tok.len();
    while let Some(&d) = chars.get(pos) {
        if d.is_ascii_digit() {
            tok.push(d);
            pos += 1;
        } else {
            break;
        }
    }
    if tok.len() == digits_start {
        return None; // expected integer
    }
    let v: i64 = tok.parse().ok()?;
    // advance(rest)
    let fmt: Vec<char> = rest.chars().collect();
    let mut i = 0usize;
    while i < fmt.len() {
        let fmtc = fmt[i];
        if scan_is_space(fmtc) {
            while i < fmt.len() && scan_is_space(fmt[i]) {
                i += 1;
            }
            match chars.get(pos) {
                None => {}
                Some(&c) if !scan_is_space(c) || c == '\n' => return None,
                Some(_) => {
                    while let Some(&c) = chars.get(pos) {
                        if scan_is_space(c) && c != '\n' {
                            pos += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            continue;
        }
        match chars.get(pos) {
            Some(&c) if c == fmtc => pos += 1,
            _ => return None,
        }
        i += 1;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sscanf_int_prefix_follows_go() {
        let f = |inp: &str, rest: &str| sscanf_int_prefix(inp, rest);
        assert_eq!(f("3 files changed", " files changed"), Some(3));
        assert_eq!(f("3 files changed", " file changed"), None);
        assert_eq!(f("1 file changed", " file changed"), Some(1));
        assert_eq!(f("  12   insertions(+)", " insertions(+)"), Some(12));
        assert_eq!(f("+7 deletions(-)", " deletions(-)"), Some(7));
        assert_eq!(f("-7 deletions(-)", " deletions(-)"), Some(-7));
        assert_eq!(f("3files changed", " files changed"), None);
        assert_eq!(f("3 files changed extra", " files changed"), Some(3));
        assert_eq!(f("3 files changedX", " files changed"), Some(3));
        assert_eq!(f("3", " files changed"), None);
        assert_eq!(f("", " files changed"), None);
        assert_eq!(f("abc", " files changed"), None);
        assert_eq!(f("\n3 files changed", " files changed"), None);
        assert_eq!(f("\t3\tfiles changed", " files changed"), Some(3));
        assert_eq!(
            f("99999999999999999999 files changed", " files changed"),
            None
        );
        assert_eq!(f("0x10 files changed", " files changed"), None);
        assert_eq!(f("10 insertion(+)", " insertions(+)"), None);
        assert_eq!(f("10 insertions(+)", " insertion(+)"), None);
        assert_eq!(f("\r\n3 files changed", " files changed"), None);
        assert_eq!(f("3\u{a0}files changed", " files changed"), Some(3));
        assert_eq!(f("3 files  changed", " files changed"), Some(3));
    }
    use chrono::TimeZone;
    use chrono::Utc;

    #[test]
    fn float_matches_go_percent_v() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (1.0, "1"),
            (-1.5, "-1.5"),
            (3.75, "3.75"),
            (730.5, "730.5"),
            (123456.0, "123456"),
            (1e6, "1e+06"),
            (12345678.0, "1.2345678e+07"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (0.1 + 0.2, "0.30000000000000004"),
            (100.0, "100"),
            (1e21, "1e+21"),
            (2.5e-10, "2.5e-10"),
        ];
        for (v, want) in cases {
            assert_eq!(float(*v), *want, "value {v}");
        }
    }

    #[test]
    fn json_float_matches_encoding_json() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (1.0, "1"),
            (1e6, "1000000"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (1.5e-10, "1.5e-10"),
            (-2.5, "-2.5"),
            (12345678901234567.0, "12345678901234568"),
        ];
        for (v, want) in cases {
            assert_eq!(json_float(*v), *want, "value {v}");
        }
    }

    #[test]
    fn slices_and_maps() {
        assert_eq!(slice(&["a", "b", "c"]), "[a b c]");
        assert_eq!(slice::<i64>(&[]), "[]");
        assert_eq!(slice(&[10i64, 30, 60]), "[10 30 60]");
        let mut m = BTreeMap::new();
        m.insert("b".to_string(), true);
        m.insert("a".to_string(), false);
        assert_eq!(map(&m), "map[a:false b:true]");
        assert_eq!(map::<String, String>(&BTreeMap::new()), "map[]");
    }

    #[test]
    fn time_formatting() {
        let t = Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap();
        assert_eq!(time(t), "2012-07-01 00:00:00 +0000 UTC");
        let t = Utc
            .with_ymd_and_hms(2017, 8, 29, 12, 29, 3)
            .unwrap()
            .with_nanosecond(120_000_000)
            .unwrap();
        assert_eq!(time(t), "2017-08-29 12:29:03.12 +0000 UTC");
        // Other offsets: Go's rendering of a zone without a name.
        let cest = FixedOffset::east_opt(2 * 3600).unwrap();
        assert_eq!(
            time(t.with_timezone(&cest)),
            "2017-08-29 14:29:03.12 +0200 +0200"
        );
        let nst = FixedOffset::west_opt(3 * 3600 + 30 * 60).unwrap();
        assert_eq!(
            time(t.with_timezone(&nst)),
            "2017-08-29 08:59:03.12 -0330 -0330"
        );
    }

    #[test]
    fn json_values() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"b":[1,2.5,"x",null,true],"a":{"z":1,"y":"q"}}"#).unwrap();
        assert_eq!(json_value(&v), "map[a:map[y:q z:1] b:[1 2.5 x <nil> true]]");
    }
}
