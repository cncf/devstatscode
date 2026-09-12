//! Driver values and Go-compatible conversions.
//!
//! lib/pq turns every result column into one of a handful of Go
//! `driver.Value` types (`textDecode`) and `database/sql` then converts those
//! into the caller's `Scan` targets (`convertAssign`). DevStats depends on the
//! observable results of both steps — it scans arbitrary columns into
//! `*[]byte`/`*string` and prints or re-parses them — so both are mirrored
//! here: [`decode_text`] is `textDecode`, [`ScanDest`] is `convertAssign`.

use std::fmt;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

use super::SqlArg;
use crate::gofmt;

pub const OID_BOOL: u32 = 16;
pub const OID_BYTEA: u32 = 17;
pub const OID_CHAR: u32 = 18;
pub const OID_INT8: u32 = 20;
pub const OID_INT2: u32 = 21;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_FLOAT4: u32 = 700;
pub const OID_FLOAT8: u32 = 701;
pub const OID_VARCHAR: u32 = 1043;
pub const OID_DATE: u32 = 1082;
pub const OID_TIME: u32 = 1083;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_TIMESTAMPTZ: u32 = 1184;
pub const OID_TIMETZ: u32 = 1266;

/// A result column (name and type from `RowDescription`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    /// Wire format code (always 0 = text here).
    pub format: i16,
}

/// A decoded column value — the Go `driver.Value` lib/pq produces.
#[derive(Debug, Clone, PartialEq)]
pub enum DriverValue {
    /// SQL NULL (Go `nil`).
    Null,
    /// int2/int4/int8 (Go `int64`).
    Int(i64),
    /// float4/float8 (Go `float64`).
    Float(f64),
    /// bool (Go `bool`).
    Bool(bool),
    /// char/varchar/text (Go `string`).
    Str(String),
    /// bytea and every type lib/pq does not decode (numeric, arrays, json,
    /// uuid, interval, name, hll, ...) — the raw text (Go `[]byte`).
    Bytes(Vec<u8>),
    /// timestamp/timestamptz/date/time/timetz (Go `time.Time`). The offset is
    /// the one PostgreSQL sent (session time zone for `timestamptz`, UTC for
    /// the zone-less types).
    Time(DateTime<FixedOffset>),
}

impl DriverValue {
    /// Go `%T` of the driver value.
    pub fn go_type_name(&self) -> &'static str {
        match self {
            DriverValue::Null => "<nil>",
            DriverValue::Int(_) => "int64",
            DriverValue::Float(_) => "float64",
            DriverValue::Bool(_) => "bool",
            DriverValue::Str(_) => "string",
            DriverValue::Bytes(_) => "[]uint8",
            DriverValue::Time(_) => "time.Time",
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, DriverValue::Null)
    }

    /// The string `database/sql` stores into `*string` / `*[]byte`
    /// destinations (`nil` has no string form). Times use RFC3339Nano —
    /// `convertAssign`'s dedicated `time.Time` → `*string` case.
    pub fn go_string(&self) -> Option<String> {
        match self {
            DriverValue::Time(t) => Some(go_rfc3339nano(t)),
            other => other.as_string(),
        }
    }

    /// `database/sql` `asString`: the intermediate representation of the
    /// numeric conversions (and of their error messages). Unlike the
    /// `*string` destination case it has no `time.Time` special case, so a
    /// time renders as Go `%v` (`time.Time.String()`).
    pub fn as_string(&self) -> Option<String> {
        match self {
            DriverValue::Null => None,
            DriverValue::Int(i) => Some(i.to_string()),
            DriverValue::Float(f) => Some(gofmt::float(*f)),
            DriverValue::Bool(b) => Some(b.to_string()),
            DriverValue::Str(s) => Some(s.clone()),
            DriverValue::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
            DriverValue::Time(t) => Some(go_time_string(t)),
        }
    }

    /// Scan into `*[]byte`: `None` for NULL, otherwise the same text as
    /// [`go_string`](Self::go_string) (raw bytes for byte values).
    pub fn go_bytes(&self) -> Option<Vec<u8>> {
        match self {
            DriverValue::Null => None,
            DriverValue::Bytes(b) => Some(b.clone()),
            other => other.go_string().map(String::into_bytes),
        }
    }

    /// Scan into `*string`.
    pub fn scan_string(&self) -> Result<String, String> {
        self.go_string()
            .ok_or_else(|| "converting NULL to string is unsupported".to_string())
    }

    /// Go `%v` of the value (used in a few error messages).
    fn go_v(&self) -> String {
        match self {
            DriverValue::Null => "<nil>".to_string(),
            DriverValue::Time(t) => go_time_string(t),
            DriverValue::Bytes(b) => {
                let parts: Vec<String> = b.iter().map(|x| x.to_string()).collect();
                format!("[{}]", parts.join(" "))
            }
            other => other.go_string().unwrap_or_default(),
        }
    }
}

impl fmt::Display for DriverValue {
    /// Go `%v`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.go_v())
    }
}

/// Go `strconv.Quote`-style rendering (`%q`) sufficient for error messages.
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\u{b}' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn go_strconv_int_err(e: &std::num::ParseIntError) -> &'static str {
    match e.kind() {
        std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
            "value out of range"
        }
        _ => "invalid syntax",
    }
}

/// Go `strconv.ParseInt(s, 10, bits)` semantics (no `_`, optional sign).
fn go_parse_int(s: &str, bits: u32) -> Result<i64, &'static str> {
    if s.is_empty() {
        return Err("invalid syntax");
    }
    let (neg, digits) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid syntax");
    }
    let v = match s.parse::<i128>() {
        Ok(v) => v,
        Err(e) => return Err(go_strconv_int_err(&e)),
    };
    let _ = neg;
    let (min, max) = match bits {
        16 => (i16::MIN as i128, i16::MAX as i128),
        32 => (i32::MIN as i128, i32::MAX as i128),
        _ => (i64::MIN as i128, i64::MAX as i128),
    };
    if v < min || v > max {
        return Err("value out of range");
    }
    Ok(v as i64)
}

/// Go `strconv.ParseFloat(s, 64)` on the intermediate string.
fn go_parse_float(s: &str) -> Result<f64, &'static str> {
    if s.is_empty() || s != s.trim() || s.contains('_') {
        return Err("invalid syntax");
    }
    match s.parse::<f64>() {
        Ok(v) => Ok(v),
        Err(_) => Err("invalid syntax"),
    }
}

fn int_conversion_error(v: &DriverValue, s: &str, kind: &str, err: &str) -> String {
    format!(
        "converting driver.Value type {} ({}) to a {}: {}",
        v.go_type_name(),
        go_quote(s),
        kind,
        err
    )
}

fn scan_int(v: &DriverValue, bits: u32, kind: &str) -> Result<i64, String> {
    match v {
        DriverValue::Null => Err(format!("converting NULL to {} is unsupported", kind)),
        DriverValue::Int(i) => {
            // reflect conversion between integer kinds (Go int64 → int/int32:
            // `dv.Kind() == sv.Kind()` only for int64; smaller kinds go through
            // ParseInt with the target width and report overflow).
            if bits == 64 {
                return Ok(*i);
            }
            let s = i.to_string();
            go_parse_int(&s, bits).map_err(|e| int_conversion_error(v, &s, kind, e))
        }
        other => {
            let s = other.as_string().unwrap_or_default();
            go_parse_int(&s, bits).map_err(|e| int_conversion_error(other, &s, kind, e))
        }
    }
}

fn scan_float(v: &DriverValue) -> Result<f64, String> {
    match v {
        DriverValue::Null => Err("converting NULL to float64 is unsupported".to_string()),
        DriverValue::Float(f) => Ok(*f),
        other => {
            let s = other.as_string().unwrap_or_default();
            go_parse_float(&s).map_err(|e| int_conversion_error(other, &s, "float64", e))
        }
    }
}

/// `driver.Bool.ConvertValue`.
fn scan_bool(v: &DriverValue) -> Result<bool, String> {
    match v {
        DriverValue::Bool(b) => Ok(*b),
        DriverValue::Str(_) | DriverValue::Bytes(_) => {
            let s = v.go_string().unwrap_or_default();
            match s.as_str() {
                "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
                "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
                _ => Err(format!(
                    "sql/driver: couldn't convert {} into type bool",
                    go_quote(&s)
                )),
            }
        }
        DriverValue::Int(i) => match i {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(format!("sql/driver: couldn't convert {} into type bool", i)),
        },
        other => Err(format!(
            "sql/driver: couldn't convert {} ({}) into type bool",
            other.go_v(),
            other.go_type_name()
        )),
    }
}

fn scan_time(v: &DriverValue, dest_type: &str) -> Result<DateTime<FixedOffset>, String> {
    match v {
        DriverValue::Time(t) => Ok(*t),
        other => Err(format!(
            "unsupported Scan, storing driver.Value type {} into type {}",
            other.go_type_name(),
            dest_type
        )),
    }
}

/// A `Scan` destination (Go `convertAssign` target). Errors are the
/// `database/sql` messages; the caller wraps them with the column context.
pub trait ScanDest {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String>;
}

impl ScanDest for String {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = v.scan_string()?;
        Ok(())
    }
}

impl ScanDest for Vec<u8> {
    /// `*[]byte`: NULL yields an empty vector (Go sets the slice to nil).
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = v.go_bytes().unwrap_or_default();
        Ok(())
    }
}

impl ScanDest for i64 {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = scan_int(v, 64, "int64")?;
        Ok(())
    }
}

impl ScanDest for i32 {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = scan_int(v, 32, "int32")? as i32;
        Ok(())
    }
}

impl ScanDest for i16 {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = scan_int(v, 16, "int16")? as i16;
        Ok(())
    }
}

impl ScanDest for f64 {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = scan_float(v)?;
        Ok(())
    }
}

impl ScanDest for bool {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = scan_bool(v)?;
        Ok(())
    }
}

impl ScanDest for DateTime<Utc> {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = scan_time(v, "*time.Time")?.with_timezone(&Utc);
        Ok(())
    }
}

impl ScanDest for DateTime<FixedOffset> {
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = scan_time(v, "*time.Time")?;
        Ok(())
    }
}

impl ScanDest for DriverValue {
    /// `*interface{}`: the driver value itself.
    fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
        *self = v.clone();
        Ok(())
    }
}

/// Pointer destinations (`**T`, `sql.Null*`): NULL → `None`.
macro_rules! scan_option {
    ($($t:ty => $init:expr),* $(,)?) => {
        $(impl ScanDest for Option<$t> {
            fn scan_from(&mut self, v: &DriverValue) -> Result<(), String> {
                if v.is_null() {
                    *self = None;
                    return Ok(());
                }
                let mut t: $t = $init;
                t.scan_from(v)?;
                *self = Some(t);
                Ok(())
            }
        })*
    };
}
scan_option!(
    String => String::new(),
    Vec<u8> => Vec::new(),
    i64 => 0,
    i32 => 0,
    i16 => 0,
    f64 => 0.0,
    bool => false,
    DriverValue => DriverValue::Null,
    DateTime<Utc> => DateTime::<Utc>::UNIX_EPOCH,
    DateTime<FixedOffset> => DateTime::<Utc>::UNIX_EPOCH.fixed_offset(),
);

/// Go `time.Time.Format(time.RFC3339Nano)`.
pub fn go_rfc3339nano(t: &DateTime<FixedOffset>) -> String {
    let mut s = format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        go_year(t.year()),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    push_frac(&mut s, t.nanosecond());
    let off = t.offset().local_minus_utc();
    if off == 0 {
        s.push('Z');
    } else {
        push_offset(&mut s, off, true);
    }
    s
}

/// Go `time.Time.String()` of a value lib/pq produced:
/// `2006-01-02 15:04:05.999999999 -0700 -0700`.
///
/// lib/pq parses `timestamp`/`date`/`time` values into a nameless
/// `time.FixedZone` (printed `+0000 +0000`), which is what every DevStats
/// `{{ts}}` column is. Only `timestamptz` values carry the session's named
/// `TimeZone` location (`+0000 UTC` in production, `+0100 CET` elsewhere);
/// zone abbreviations are not reproduced here — those print their numeric
/// offset twice as well.
pub fn go_time_string(t: &DateTime<FixedOffset>) -> String {
    let mut s = format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        go_year(t.year()),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    push_frac(&mut s, t.nanosecond());
    let off = t.offset().local_minus_utc();
    s.push(' ');
    push_offset(&mut s, off, false);
    s.push(' ');
    push_offset(&mut s, off, false);
    s
}

fn go_year(y: i32) -> String {
    if y < 0 {
        format!("-{:04}", -(y as i64))
    } else {
        format!("{:04}", y)
    }
}

fn push_frac(s: &mut String, nanos: u32) {
    if nanos != 0 {
        let mut frac = format!("{:09}", nanos);
        while frac.ends_with('0') {
            frac.pop();
        }
        s.push('.');
        s.push_str(&frac);
    }
}

fn push_offset(s: &mut String, off: i32, colon: bool) {
    let sign = if off < 0 { '-' } else { '+' };
    let a = off.abs();
    let (h, m) = (a / 3600, (a % 3600) / 60);
    if colon {
        s.push_str(&format!("{}{:02}:{:02}", sign, h, m));
    } else {
        s.push_str(&format!("{}{:02}{:02}", sign, h, m));
    }
}

/// Go `strconv.FormatFloat(f, 'f', -1, 64)` (lib/pq parameter encoding).
pub fn go_float_f(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    // Rust's Display for f64 is the shortest round-trip representation in
    // plain (non-exponent) notation — exactly Go's 'f' with precision -1.
    format!("{}", f)
}

/// lib/pq `FormatTimestamp` for a UTC value:
/// `2006-01-02 15:04:05.999999999Z` (+ ` BC` for years ≤ 0).
pub fn format_timestamp(t: DateTime<FixedOffset>) -> String {
    let mut year = t.year();
    let bc = year <= 0;
    if bc {
        // Go: t.AddDate(-year*2+1, 0, 0) → year becomes 1-year.
        year = 1 - year;
    }
    let mut s = format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        go_year(year),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    push_frac(&mut s, t.nanosecond());
    // Go layout `Z07:00`: `Z` for UTC, otherwise ±hh:mm; lib/pq then appends
    // the seconds of the offset (`:ss`) when they are not zero.
    let off = t.offset().local_minus_utc();
    if off == 0 {
        s.push('Z');
    } else {
        let a = off.unsigned_abs();
        s.push_str(&format!(
            "{}{:02}:{:02}",
            if off < 0 { '-' } else { '+' },
            a / 3600,
            (a % 3600) / 60
        ));
        if !a.is_multiple_of(60) {
            s.push_str(&format!(":{:02}", a % 60));
        }
    }
    if bc {
        s.push_str(" BC");
    }
    s
}

/// lib/pq `encode`: the text sent for a bound parameter (`None` = NULL).
/// `bytea` tells whether the parameter's declared type is `bytea` (hex
/// encoding applies only then).
pub fn encode_arg(arg: &SqlArg, bytea: bool) -> Option<Vec<u8>> {
    match arg {
        SqlArg::Null => None,
        SqlArg::Int(i) => Some(i.to_string().into_bytes()),
        SqlArg::Float(f) => Some(go_float_f(*f).into_bytes()),
        SqlArg::Bool(b) => Some(b.to_string().into_bytes()),
        SqlArg::Str(s) => {
            if bytea {
                Some(encode_bytea(s.as_bytes()))
            } else {
                Some(s.clone().into_bytes())
            }
        }
        SqlArg::Bytes(b) => {
            if bytea {
                Some(encode_bytea(b))
            } else {
                Some(b.clone())
            }
        }
        SqlArg::Time(t) | SqlArg::DbTime(t) => Some(format_timestamp(*t).into_bytes()),
    }
}

/// lib/pq `encodeBytea` (hex format, servers ≥ 9.0).
pub fn encode_bytea(v: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + v.len() * 2);
    out.extend_from_slice(b"\\x");
    for b in v {
        out.extend_from_slice(format!("{:02x}", b).as_bytes());
    }
    out
}

/// lib/pq `parseBytea`: hex (`\x...`) or legacy escape format.
pub fn parse_bytea(s: &[u8]) -> Result<Vec<u8>, String> {
    if s.len() >= 2 && &s[..2] == b"\\x" {
        let hex = &s[2..];
        if !hex.len().is_multiple_of(2) {
            return Err("encoding/hex: odd length hex string".to_string());
        }
        let mut out = Vec::with_capacity(hex.len() / 2);
        for pair in hex.chunks(2) {
            let h = std::str::from_utf8(pair).map_err(|_| "encoding/hex: invalid byte")?;
            out.push(
                u8::from_str_radix(h, 16)
                    .map_err(|_| format!("encoding/hex: invalid byte: U+00{:02X}", pair[0]))?,
            );
        }
        return Ok(out);
    }
    let mut out = Vec::with_capacity(s.len());
    let mut rest = s;
    while !rest.is_empty() {
        if rest[0] == b'\\' {
            if rest.len() >= 2 && rest[1] == b'\\' {
                out.push(b'\\');
                rest = &rest[2..];
                continue;
            }
            if rest.len() < 4 {
                return Err(format!("invalid bytea sequence {:?}", rest));
            }
            let oct = std::str::from_utf8(&rest[1..4]).map_err(|_| "invalid bytea sequence")?;
            let r = u8::from_str_radix(oct, 8).map_err(|e| {
                format!(
                    "could not parse bytea value: strconv.ParseUint: parsing {}: {}",
                    go_quote(oct),
                    match e.kind() {
                        std::num::IntErrorKind::PosOverflow => "value out of range",
                        _ => "invalid syntax",
                    }
                )
            })?;
            out.push(r);
            rest = &rest[4..];
        } else {
            match rest.iter().position(|b| *b == b'\\') {
                None => {
                    out.extend_from_slice(rest);
                    break;
                }
                Some(i) => {
                    out.extend_from_slice(&rest[..i]);
                    rest = &rest[i..];
                }
            }
        }
    }
    Ok(out)
}

struct TsParser {
    err: Option<String>,
}

impl TsParser {
    fn must_atoi(&mut self, s: &str, begin: usize, end: usize) -> i64 {
        if begin > end || end > s.len() {
            self.err = Some("expected number; got end of input".to_string());
            return 0;
        }
        let part = &s[begin..end];
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            if self.err.is_none() {
                self.err = Some(format!(
                    "expected number; got '{}'",
                    part.chars()
                        .next()
                        .map(|c| c.to_string())
                        .unwrap_or_default()
                ));
            }
            return 0;
        }
        match part.parse::<i64>() {
            Ok(v) => v,
            Err(_) => {
                self.err = Some(format!("expected number; got '{}'", part));
                0
            }
        }
    }

    fn expect(&mut self, s: &str, want: u8, pos: usize) {
        if pos >= s.len() {
            self.err = Some(format!(
                "expected '{}' at position {}; got end of input",
                want as char, pos
            ));
            return;
        }
        let got = s.as_bytes()[pos];
        if got != want && self.err.is_none() {
            self.err = Some(format!(
                "expected '{}' at position {}; got '{}'",
                want as char, pos, got as char
            ));
        }
    }
}

/// lib/pq `ParseTimestamp`: PostgreSQL ISO text form of timestamp[tz]/date
/// (`YYYY-MM-DD[ HH:MM:SS[.f]][±HH[:MM[:SS]]|Z][ BC]`) to a fixed-offset time.
pub fn parse_timestamp(s: &str) -> Result<DateTime<FixedOffset>, String> {
    let mut p = TsParser { err: None };
    let mon_sep = s
        .find('-')
        .ok_or_else(|| "expected '-' in timestamp".to_string())?;
    let year = p.must_atoi(s, 0, mon_sep);
    let day_sep = mon_sep + 3;
    let month = p.must_atoi(s, mon_sep + 1, day_sep);
    p.expect(s, b'-', day_sep);
    let time_sep = day_sep + 3;
    let day = p.must_atoi(s, day_sep + 1, time_sep);
    let mut min_len = mon_sep + "01-01".len() + 1;
    let is_bc = s.ends_with(" BC");
    if is_bc {
        min_len += 3;
    }
    let (mut hour, mut minute, mut second) = (0i64, 0i64, 0i64);
    if s.len() > min_len {
        p.expect(s, b' ', time_sep);
        let min_sep = time_sep + 3;
        p.expect(s, b':', min_sep);
        hour = p.must_atoi(s, time_sep + 1, min_sep);
        let sec_sep = min_sep + 3;
        p.expect(s, b':', sec_sep);
        minute = p.must_atoi(s, min_sep + 1, sec_sep);
        let sec_end = sec_sep + 3;
        second = p.must_atoi(s, sec_sep + 1, sec_end);
    }
    let mut remainder_idx = mon_sep + "01-01 00:00:00".len() + 1;
    let mut nanos: i64 = 0;
    let mut tz_off: i64 = 0;
    let bytes = s.as_bytes();
    if remainder_idx < s.len() && bytes[remainder_idx] == b'.' {
        let frac_start = remainder_idx + 1;
        let frac_off = s[frac_start..]
            .find(['-', '+', 'Z', ' '])
            .unwrap_or(s.len() - frac_start);
        let frac_sec = p.must_atoi(s, frac_start, frac_start + frac_off);
        let scale = 10i64.pow(frac_off.min(18) as u32);
        nanos = frac_sec * (1_000_000_000 / scale.max(1));
        if frac_off > 9 {
            nanos = 0;
        }
        remainder_idx += frac_off + 1;
    }
    let tz_start = remainder_idx;
    if tz_start < s.len() && (bytes[tz_start] == b'-' || bytes[tz_start] == b'+') {
        let tz_sign: i64 = if bytes[tz_start] == b'-' { -1 } else { 1 };
        let tz_hours = p.must_atoi(s, tz_start + 1, tz_start + 3);
        remainder_idx += 3;
        let (mut tz_min, mut tz_sec) = (0i64, 0i64);
        if remainder_idx < s.len() && bytes[remainder_idx] == b':' {
            tz_min = p.must_atoi(s, remainder_idx + 1, remainder_idx + 3);
            remainder_idx += 3;
        }
        if remainder_idx < s.len() && bytes[remainder_idx] == b':' {
            tz_sec = p.must_atoi(s, remainder_idx + 1, remainder_idx + 3);
            remainder_idx += 3;
        }
        tz_off = tz_sign * (tz_hours * 3600 + tz_min * 60 + tz_sec);
    } else if tz_start < s.len() && bytes[tz_start] == b'Z' {
        remainder_idx += 1;
    }
    let iso_year = if is_bc {
        remainder_idx += 3;
        1 - year
    } else {
        year
    };
    if remainder_idx < s.len() {
        return Err(format!(
            "expected end of input, got {}",
            &s[remainder_idx.min(s.len())..]
        ));
    }
    if let Some(e) = p.err {
        return Err(e);
    }
    go_time_date(iso_year, month, day, hour, minute, second, nanos, tz_off)
}

/// Go `time.Date` with a fixed zone: out-of-range fields are normalised
/// (e.g. second 60 rolls over) like Go does.
#[allow(clippy::too_many_arguments)]
pub fn go_time_date(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    nanos: i64,
    tz_off: i64,
) -> Result<DateTime<FixedOffset>, String> {
    // Normalise month into [1,12] adjusting the year.
    let m0 = month - 1;
    let year = year + m0.div_euclid(12);
    let month = m0.rem_euclid(12) + 1;
    let base = NaiveDate::from_ymd_opt(
        i32::try_from(year).map_err(|_| "year out of range".to_string())?,
        month as u32,
        1,
    )
    .ok_or_else(|| "date out of range".to_string())?;
    let total_secs = (day - 1) * 86_400 + hour * 3600 + minute * 60 + second;
    let extra_secs = nanos.div_euclid(1_000_000_000);
    let nanos = nanos.rem_euclid(1_000_000_000) as u32;
    let naive = base
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("midnight"))
        .checked_add_signed(chrono::Duration::seconds(total_secs + extra_secs))
        .ok_or_else(|| "time out of range".to_string())?;
    let naive = NaiveDateTime::new(
        naive.date(),
        NaiveTime::from_hms_nano_opt(naive.hour(), naive.minute(), naive.second(), nanos)
            .ok_or_else(|| "time out of range".to_string())?,
    );
    let off = FixedOffset::east_opt(
        i32::try_from(tz_off).map_err(|_| "time zone offset out of range".to_string())?,
    )
    .ok_or_else(|| "time zone offset out of range".to_string())?;
    match naive.and_local_timezone(off) {
        chrono::LocalResult::Single(t) => Ok(t),
        _ => Err("time out of range".to_string()),
    }
}

/// Go `time.Parse("15:04:05", s)` / `time.Parse("15:04:05-07", s)` as used by
/// lib/pq for `time`/`timetz` (year 0, January 1st).
fn parse_time_of_day(s: &str, with_zone: bool) -> Result<DateTime<FixedOffset>, String> {
    let bad = || format!("parsing time {}: cannot parse", go_quote(s));
    let b = s.as_bytes();
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return Err(bad());
    }
    let num = |from: usize, to: usize| -> Result<i64, String> {
        let part = &s[from..to];
        if !part.bytes().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        part.parse::<i64>().map_err(|_| bad())
    };
    let hour = num(0, 2)?;
    let minute = num(3, 5)?;
    let second = num(6, 8)?;
    if hour > 23 {
        return Err(format!("parsing time {}: hour out of range", go_quote(s)));
    }
    if minute > 59 {
        return Err(format!("parsing time {}: minute out of range", go_quote(s)));
    }
    if second > 59 {
        return Err(format!("parsing time {}: second out of range", go_quote(s)));
    }
    let mut rest = &s[8..];
    let mut nanos: i64 = 0;
    if let Some(r) = rest.strip_prefix('.') {
        let n = r.bytes().take_while(|c| c.is_ascii_digit()).count();
        if n == 0 {
            return Err(bad());
        }
        let digits = &r[..n.min(9)];
        nanos = digits.parse::<i64>().map_err(|_| bad())? * 10i64.pow(9 - digits.len() as u32);
        rest = &r[n..];
    }
    let mut tz_off = 0i64;
    if with_zone {
        let rb = rest.as_bytes();
        if rb.len() < 3 || (rb[0] != b'+' && rb[0] != b'-') {
            return Err(bad());
        }
        let sign: i64 = if rb[0] == b'-' { -1 } else { 1 };
        let hh = rest[1..3].parse::<i64>().map_err(|_| bad())?;
        rest = &rest[3..];
        let mut mm = 0i64;
        let mut ss = 0i64;
        if let Some(r) = rest.strip_prefix(':') {
            mm = r
                .get(..2)
                .and_then(|v| v.parse::<i64>().ok())
                .ok_or_else(bad)?;
            rest = &r[2..];
            if let Some(r) = rest.strip_prefix(':') {
                ss = r
                    .get(..2)
                    .and_then(|v| v.parse::<i64>().ok())
                    .ok_or_else(bad)?;
                rest = &r[2..];
            }
        }
        tz_off = sign * (hh * 3600 + mm * 60 + ss);
    }
    if !rest.is_empty() {
        return Err(format!(
            "parsing time {}: extra text: {}",
            go_quote(s),
            go_quote(rest)
        ));
    }
    go_time_date(0, 1, 1, hour, minute, second, nanos, tz_off)
}

/// lib/pq `textDecode`: a text-format column value to its driver value.
pub fn decode_text(bytes: &[u8], oid: u32) -> Result<DriverValue, String> {
    match oid {
        OID_CHAR | OID_VARCHAR | OID_TEXT => Ok(DriverValue::Str(
            String::from_utf8_lossy(bytes).into_owned(),
        )),
        OID_BYTEA => parse_bytea(bytes).map(DriverValue::Bytes),
        OID_TIMESTAMPTZ | OID_TIMESTAMP | OID_DATE => {
            let s = String::from_utf8_lossy(bytes);
            if s == "infinity" || s == "-infinity" {
                return Ok(DriverValue::Bytes(bytes.to_vec()));
            }
            parse_timestamp(&s).map(DriverValue::Time)
        }
        OID_TIME => {
            parse_time_of_day(&String::from_utf8_lossy(bytes), false).map(DriverValue::Time)
        }
        OID_TIMETZ => {
            parse_time_of_day(&String::from_utf8_lossy(bytes), true).map(DriverValue::Time)
        }
        OID_BOOL => Ok(DriverValue::Bool(bytes.first() == Some(&b't'))),
        OID_INT8 | OID_INT4 | OID_INT2 => {
            let s = String::from_utf8_lossy(bytes);
            go_parse_int(&s, 64)
                .map(DriverValue::Int)
                .map_err(|e| format!("strconv.ParseInt: parsing {}: {}", go_quote(&s), e))
        }
        OID_FLOAT4 | OID_FLOAT8 => {
            let s = String::from_utf8_lossy(bytes);
            go_parse_float(&s)
                .map(DriverValue::Float)
                .map_err(|e| format!("strconv.ParseFloat: parsing {}: {}", go_quote(&s), e))
        }
        _ => Ok(DriverValue::Bytes(bytes.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn fo(secs: i32) -> FixedOffset {
        FixedOffset::east_opt(secs).unwrap()
    }

    #[test]
    fn rfc3339nano() {
        let t = fo(0).with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(go_rfc3339nano(&t), "2020-01-02T03:04:05Z");
        let t = t.with_nanosecond(120_000_000).unwrap();
        assert_eq!(go_rfc3339nano(&t), "2020-01-02T03:04:05.12Z");
        let t = fo(3600)
            .with_ymd_and_hms(2020, 1, 2, 3, 4, 5)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();
        assert_eq!(go_rfc3339nano(&t), "2020-01-02T03:04:05.123456789+01:00");
        let t = fo(-19800)
            .with_ymd_and_hms(1999, 12, 31, 23, 59, 59)
            .unwrap();
        assert_eq!(go_rfc3339nano(&t), "1999-12-31T23:59:59-05:30");
        let t = fo(0).with_ymd_and_hms(0, 1, 1, 15, 4, 5).unwrap();
        assert_eq!(go_rfc3339nano(&t), "0000-01-01T15:04:05Z");
        let t = fo(0).with_ymd_and_hms(-5, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(go_rfc3339nano(&t), "-0005-01-01T00:00:00Z");
        let t = fo(0).with_ymd_and_hms(12345, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(go_rfc3339nano(&t), "12345-01-01T00:00:00Z");
        assert_eq!(
            go_time_string(&fo(0).with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap()),
            "2020-01-02 03:04:05 +0000 +0000"
        );
        assert_eq!(
            go_time_string(&fo(7200).with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap()),
            "2020-01-02 03:04:05 +0200 +0200"
        );
    }

    #[test]
    fn float_f() {
        assert_eq!(go_float_f(1.0), "1");
        assert_eq!(go_float_f(0.1), "0.1");
        assert_eq!(go_float_f(-2.5), "-2.5");
        assert_eq!(go_float_f(1e21), "1000000000000000000000");
        assert_eq!(go_float_f(1e-7), "0.0000001");
        assert_eq!(go_float_f(123456789.125), "123456789.125");
        assert_eq!(go_float_f(f64::NAN), "NaN");
        assert_eq!(go_float_f(f64::INFINITY), "+Inf");
        assert_eq!(go_float_f(f64::NEG_INFINITY), "-Inf");
        assert_eq!(go_float_f(-0.0), "-0");
        assert_eq!(go_float_f(0.30000000000000004), "0.30000000000000004");
    }

    #[test]
    fn timestamps() {
        let t = Utc.with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(format_timestamp(t.fixed_offset()), "2020-01-02 03:04:05Z");
        // Non-UTC offsets are sent as lib/pq does: ±hh:mm (+ :ss when needed).
        let cest = FixedOffset::east_opt(2 * 3600).unwrap();
        assert_eq!(
            format_timestamp(t.with_timezone(&cest)),
            "2020-01-02 05:04:05+02:00"
        );
        let odd = FixedOffset::west_opt(3 * 3600 + 30 * 60 + 7).unwrap();
        assert_eq!(
            format_timestamp(t.with_timezone(&odd)),
            "2020-01-01 23:33:58-03:30:07"
        );
        let t = t.with_nanosecond(500_000).unwrap();
        assert_eq!(
            format_timestamp(t.fixed_offset()),
            "2020-01-02 03:04:05.0005Z"
        );
        let t = Utc.with_ymd_and_hms(0, 3, 4, 0, 0, 0).unwrap();
        assert_eq!(
            format_timestamp(t.fixed_offset()),
            "0001-03-04 00:00:00Z BC"
        );
        let t = Utc.with_ymd_and_hms(-1, 3, 4, 0, 0, 0).unwrap();
        assert_eq!(
            format_timestamp(t.fixed_offset()),
            "0002-03-04 00:00:00Z BC"
        );

        let p = |s: &str| parse_timestamp(s).unwrap();
        assert_eq!(
            go_rfc3339nano(&p("2020-01-02 03:04:05")),
            "2020-01-02T03:04:05Z"
        );
        assert_eq!(
            go_rfc3339nano(&p("2020-01-02 03:04:05.5")),
            "2020-01-02T03:04:05.5Z"
        );
        assert_eq!(
            go_rfc3339nano(&p("2020-01-02 03:04:05.123456+02")),
            "2020-01-02T03:04:05.123456+02:00"
        );
        assert_eq!(
            go_rfc3339nano(&p("2020-01-02 03:04:05-05:30")),
            "2020-01-02T03:04:05-05:30"
        );
        assert_eq!(
            go_rfc3339nano(&p("2020-01-02 03:04:05+00")),
            "2020-01-02T03:04:05Z"
        );
        assert_eq!(go_rfc3339nano(&p("2020-01-02")), "2020-01-02T00:00:00Z");
        assert_eq!(
            go_rfc3339nano(&p("0001-01-01 00:00:00 BC")),
            "0000-01-01T00:00:00Z"
        );
        assert_eq!(go_rfc3339nano(&p("0002-01-01 BC")), "-0001-01-01T00:00:00Z");
        assert_eq!(
            go_rfc3339nano(&p("2020-01-02 03:04:05+05:45:30")),
            "2020-01-02T03:04:05+05:45"
        );
        assert_eq!(
            go_rfc3339nano(&p("9999-12-31 23:59:59.999999")),
            "9999-12-31T23:59:59.999999Z"
        );
        // chrono dates are limited to ±262143 years (PostgreSQL allows up to
        // 294276 AD) — such values are reported as a decode error.
        assert!(parse_timestamp("294276-12-31 23:59:59.999999").is_err());
        assert!(parse_timestamp("2020-01-02 03:04:05 junk").is_err());
        assert!(parse_timestamp("nope").is_err());
        assert!(parse_timestamp("2020-0x-02").is_err());
    }

    #[test]
    fn time_of_day() {
        let t = parse_time_of_day("15:04:05", false).unwrap();
        assert_eq!(go_rfc3339nano(&t), "0000-01-01T15:04:05Z");
        let t = parse_time_of_day("15:04:05.25", false).unwrap();
        assert_eq!(go_rfc3339nano(&t), "0000-01-01T15:04:05.25Z");
        let t = parse_time_of_day("15:04:05+02", true).unwrap();
        assert_eq!(go_rfc3339nano(&t), "0000-01-01T15:04:05+02:00");
        let t = parse_time_of_day("15:04:05.5-05:30", true).unwrap();
        assert_eq!(go_rfc3339nano(&t), "0000-01-01T15:04:05.5-05:30");
        assert_eq!(
            parse_time_of_day("24:00:00", false).unwrap_err(),
            "parsing time \"24:00:00\": hour out of range"
        );
        assert!(parse_time_of_day("15:04", false).is_err());
        assert!(parse_time_of_day("15:04:05", true).is_err());
    }

    #[test]
    fn bytea() {
        assert_eq!(parse_bytea(b"\\x01ff").unwrap(), vec![1, 255]);
        assert_eq!(parse_bytea(b"\\x").unwrap(), Vec::<u8>::new());
        assert_eq!(parse_bytea(b"abc").unwrap(), b"abc".to_vec());
        assert_eq!(parse_bytea(b"a\\\\b").unwrap(), b"a\\b".to_vec());
        assert_eq!(parse_bytea(b"a\\001b").unwrap(), vec![b'a', 1, b'b']);
        assert!(parse_bytea(b"\\x0").is_err());
        assert!(parse_bytea(b"a\\01").is_err());
        assert_eq!(encode_bytea(&[1, 255, 16]), b"\\x01ff10".to_vec());
        assert_eq!(encode_bytea(&[]), b"\\x".to_vec());
    }

    #[test]
    fn encode_args() {
        assert_eq!(encode_arg(&SqlArg::Null, false), None);
        assert_eq!(encode_arg(&SqlArg::Int(-7), false).unwrap(), b"-7".to_vec());
        assert_eq!(
            encode_arg(&SqlArg::Float(2.5), false).unwrap(),
            b"2.5".to_vec()
        );
        assert_eq!(
            encode_arg(&SqlArg::Float(1e21), false).unwrap(),
            b"1000000000000000000000".to_vec()
        );
        assert_eq!(
            encode_arg(&SqlArg::Bool(true), false).unwrap(),
            b"true".to_vec()
        );
        assert_eq!(
            encode_arg(&SqlArg::Str("x'y".into()), false).unwrap(),
            b"x'y".to_vec()
        );
        assert_eq!(
            encode_arg(&SqlArg::Str("ab".into()), true).unwrap(),
            b"\\x6162".to_vec()
        );
        assert_eq!(
            encode_arg(&SqlArg::Bytes(vec![1, 2]), false).unwrap(),
            vec![1, 2]
        );
        assert_eq!(
            encode_arg(&SqlArg::Bytes(vec![1, 2]), true).unwrap(),
            b"\\x0102".to_vec()
        );
        let t = Utc.with_ymd_and_hms(2021, 6, 7, 8, 9, 10).unwrap();
        assert_eq!(
            encode_arg(&SqlArg::Time(t.fixed_offset()), false).unwrap(),
            b"2021-06-07 08:09:10Z".to_vec()
        );
    }

    #[test]
    fn decode_values() {
        assert_eq!(
            decode_text(b"abc", OID_TEXT).unwrap(),
            DriverValue::Str("abc".into())
        );
        assert_eq!(
            decode_text(b"abc", OID_VARCHAR).unwrap(),
            DriverValue::Str("abc".into())
        );
        assert_eq!(
            decode_text(b"a", OID_CHAR).unwrap(),
            DriverValue::Str("a".into())
        );
        assert_eq!(
            decode_text(b"\\x00ff", OID_BYTEA).unwrap(),
            DriverValue::Bytes(vec![0, 255])
        );
        assert_eq!(
            decode_text(b"t", OID_BOOL).unwrap(),
            DriverValue::Bool(true)
        );
        assert_eq!(
            decode_text(b"f", OID_BOOL).unwrap(),
            DriverValue::Bool(false)
        );
        assert_eq!(decode_text(b"42", OID_INT4).unwrap(), DriverValue::Int(42));
        assert_eq!(decode_text(b"-1", OID_INT2).unwrap(), DriverValue::Int(-1));
        assert_eq!(
            decode_text(b"9223372036854775807", OID_INT8).unwrap(),
            DriverValue::Int(i64::MAX)
        );
        assert_eq!(
            decode_text(b"x", OID_INT8).unwrap_err(),
            "strconv.ParseInt: parsing \"x\": invalid syntax"
        );
        assert_eq!(
            decode_text(b"1.5", OID_FLOAT8).unwrap(),
            DriverValue::Float(1.5)
        );
        assert_eq!(
            decode_text(b"0.1", OID_FLOAT4).unwrap(),
            DriverValue::Float(0.1)
        );
        assert!(
            matches!(decode_text(b"NaN", OID_FLOAT8).unwrap(), DriverValue::Float(f) if f.is_nan())
        );
        assert_eq!(
            decode_text(b"-Infinity", OID_FLOAT8).unwrap(),
            DriverValue::Float(f64::NEG_INFINITY)
        );
        assert_eq!(
            decode_text(b"infinity", OID_TIMESTAMP).unwrap(),
            DriverValue::Bytes(b"infinity".to_vec())
        );
        assert_eq!(
            decode_text(b"12.50", 1700).unwrap(),
            DriverValue::Bytes(b"12.50".to_vec())
        );
        match decode_text(b"2020-01-02 03:04:05+01", OID_TIMESTAMPTZ).unwrap() {
            DriverValue::Time(t) => assert_eq!(go_rfc3339nano(&t), "2020-01-02T03:04:05+01:00"),
            other => panic!("{:?}", other),
        }
        match decode_text(b"2020-01-02", OID_DATE).unwrap() {
            DriverValue::Time(t) => assert_eq!(go_rfc3339nano(&t), "2020-01-02T00:00:00Z"),
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn scan_conversions() {
        let mut s = String::new();
        s.scan_from(&DriverValue::Int(5)).unwrap();
        assert_eq!(s, "5");
        s.scan_from(&DriverValue::Float(1234567.0)).unwrap();
        assert_eq!(s, "1.234567e+06");
        s.scan_from(&DriverValue::Float(0.5)).unwrap();
        assert_eq!(s, "0.5");
        s.scan_from(&DriverValue::Bool(true)).unwrap();
        assert_eq!(s, "true");
        s.scan_from(&DriverValue::Bytes(b"raw".to_vec())).unwrap();
        assert_eq!(s, "raw");
        let t = fo(0).with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap();
        s.scan_from(&DriverValue::Time(t)).unwrap();
        assert_eq!(s, "2020-01-02T03:04:05Z");
        assert_eq!(
            s.scan_from(&DriverValue::Null).unwrap_err(),
            "converting NULL to string is unsupported"
        );

        let mut b = Vec::<u8>::new();
        b.scan_from(&DriverValue::Null).unwrap();
        assert!(b.is_empty());
        b.scan_from(&DriverValue::Int(-3)).unwrap();
        assert_eq!(b, b"-3".to_vec());
        let mut ob: Option<Vec<u8>> = Some(vec![]);
        ob.scan_from(&DriverValue::Null).unwrap();
        assert_eq!(ob, None);
        ob.scan_from(&DriverValue::Time(t)).unwrap();
        assert_eq!(ob, Some(b"2020-01-02T03:04:05Z".to_vec()));

        let mut i = 0i64;
        i.scan_from(&DriverValue::Int(7)).unwrap();
        assert_eq!(i, 7);
        i.scan_from(&DriverValue::Str("12".into())).unwrap();
        assert_eq!(i, 12);
        i.scan_from(&DriverValue::Bytes(b"13".to_vec())).unwrap();
        assert_eq!(i, 13);
        assert_eq!(
            i.scan_from(&DriverValue::Str("abc".into())).unwrap_err(),
            "converting driver.Value type string (\"abc\") to a int64: invalid syntax"
        );
        assert_eq!(
            i.scan_from(&DriverValue::Float(1.5)).unwrap_err(),
            "converting driver.Value type float64 (\"1.5\") to a int64: invalid syntax"
        );
        assert_eq!(
            i.scan_from(&DriverValue::Bool(true)).unwrap_err(),
            "converting driver.Value type bool (\"true\") to a int64: invalid syntax"
        );
        assert_eq!(
            i.scan_from(&DriverValue::Null).unwrap_err(),
            "converting NULL to int64 is unsupported"
        );
        assert_eq!(
            i.scan_from(&DriverValue::Str("99999999999999999999".into()))
                .unwrap_err(),
            "converting driver.Value type string (\"99999999999999999999\") to a int64: value out of range"
        );
        let mut i32v = 0i32;
        assert_eq!(
            i32v.scan_from(&DriverValue::Int(1 << 40)).unwrap_err(),
            "converting driver.Value type int64 (\"1099511627776\") to a int32: value out of range"
        );
        i32v.scan_from(&DriverValue::Int(-5)).unwrap();
        assert_eq!(i32v, -5);

        let mut f = 0f64;
        f.scan_from(&DriverValue::Int(3)).unwrap();
        assert_eq!(f, 3.0);
        f.scan_from(&DriverValue::Bytes(b"12.50".to_vec())).unwrap();
        assert_eq!(f, 12.5);
        f.scan_from(&DriverValue::Float(0.25)).unwrap();
        assert_eq!(f, 0.25);
        assert_eq!(
            f.scan_from(&DriverValue::Str("x".into())).unwrap_err(),
            "converting driver.Value type string (\"x\") to a float64: invalid syntax"
        );
        assert_eq!(
            f.scan_from(&DriverValue::Null).unwrap_err(),
            "converting NULL to float64 is unsupported"
        );

        let mut bo = false;
        bo.scan_from(&DriverValue::Bool(true)).unwrap();
        assert!(bo);
        bo.scan_from(&DriverValue::Str("f".into())).unwrap();
        assert!(!bo);
        bo.scan_from(&DriverValue::Int(1)).unwrap();
        assert!(bo);
        assert_eq!(
            bo.scan_from(&DriverValue::Int(2)).unwrap_err(),
            "sql/driver: couldn't convert 2 into type bool"
        );
        assert_eq!(
            bo.scan_from(&DriverValue::Str("yes".into())).unwrap_err(),
            "sql/driver: couldn't convert \"yes\" into type bool"
        );
        assert_eq!(
            bo.scan_from(&DriverValue::Null).unwrap_err(),
            "sql/driver: couldn't convert <nil> (<nil>) into type bool"
        );
        assert_eq!(
            bo.scan_from(&DriverValue::Float(1.5)).unwrap_err(),
            "sql/driver: couldn't convert 1.5 (float64) into type bool"
        );

        let mut dt = Utc::now();
        dt.scan_from(&DriverValue::Time(
            fo(3600).with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap(),
        ))
        .unwrap();
        assert_eq!(dt, Utc.with_ymd_and_hms(2020, 1, 2, 2, 4, 5).unwrap());
        assert_eq!(
            dt.scan_from(&DriverValue::Str("2020".into())).unwrap_err(),
            "unsupported Scan, storing driver.Value type string into type *time.Time"
        );
        assert_eq!(
            dt.scan_from(&DriverValue::Null).unwrap_err(),
            "unsupported Scan, storing driver.Value type <nil> into type *time.Time"
        );
        let mut odt: Option<DateTime<Utc>> = None;
        odt.scan_from(&DriverValue::Null).unwrap();
        assert_eq!(odt, None);
        odt.scan_from(&DriverValue::Time(t)).unwrap();
        assert_eq!(
            odt,
            Some(Utc.with_ymd_and_hms(2020, 1, 2, 3, 4, 5).unwrap())
        );

        let mut os: Option<String> = Some("x".into());
        os.scan_from(&DriverValue::Null).unwrap();
        assert_eq!(os, None);
        os.scan_from(&DriverValue::Bytes(b"pg_class".to_vec()))
            .unwrap();
        assert_eq!(os.as_deref(), Some("pg_class"));

        let mut dv = DriverValue::Null;
        dv.scan_from(&DriverValue::Int(1)).unwrap();
        assert_eq!(dv, DriverValue::Int(1));
    }

    #[test]
    fn quoting() {
        assert_eq!(go_quote("abc"), "\"abc\"");
        assert_eq!(go_quote("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
        assert_eq!(go_quote("\u{1}"), "\"\\x01\"");
        assert_eq!(go_quote("平仮名"), "\"平仮名\"");
    }
}
