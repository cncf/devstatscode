//! String helpers — port of `string.go`.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use sha1::{Digest, Sha1};

use crate::context::Ctx;
use crate::error::{fatal_on_error, go_io_error_string};
use crate::goregex;
use crate::rng;
use crate::time::{interval_hours, range_hours, time_parse_any, to_ymdhms_date};

/// Error text returned by [`prepare_quick_range_query`] when neither a period
/// nor a from/to range is given.
pub const QUICK_RANGE_QUERY_ERROR: &str =
    "You need to provide either non-empty `period` or non empty `from` and `to`";

/// Prepare a query using either a ready `period` string or `from`/`to`
/// strings. Placeholders `{{period:alias.column}}` become either
/// `(alias.column >= now() - 'period'::interval)` or
/// `(alias.column >= 'from' and alias.column < 'to')`; `{{from}}`/`{{to}}` are
/// replaced too. Returns the query and the period length in hours.
pub fn prepare_quick_range_query(
    sql: &str,
    period: &str,
    from: &str,
    to: &str,
) -> (String, String) {
    const START_PATT: &str = "{{period:";
    const END_PATT: &str = "}}";
    let mut from = from.to_string();
    let mut to = to.to_string();
    let mut s_hours = "0".to_string();
    let period_mode = !period.is_empty();
    if period_mode {
        s_hours = interval_hours(period);
    } else if !from.is_empty() && !to.is_empty() {
        let t_from = time_parse_any(&from);
        let t_to = time_parse_any(&to);
        from = to_ymdhms_date(t_from);
        to = to_ymdhms_date(t_to);
        s_hours = range_hours(t_from, t_to);
    }
    let mut res = String::new();
    let mut start = 0usize;
    while let Some(idx1) = sql[start..].find(START_PATT) {
        // Go indexes into the string without checking for a missing `}}`;
        // treat an unterminated placeholder as running to the end of the SQL.
        let idx2 = sql[start + idx1..]
            .find(END_PATT)
            .unwrap_or(sql.len() - start - idx1);
        let col = &sql[start + idx1 + START_PATT.len()..start + idx1 + idx2];
        res.push_str(&sql[start..start + idx1]);
        if period_mode {
            res.push_str(&format!(" ({} >= now() - '{}'::interval) ", col, period));
        } else {
            if from.is_empty() || to.is_empty() {
                return (QUICK_RANGE_QUERY_ERROR.to_string(), s_hours);
            }
            res.push_str(&format!(" ({} >= '{}' and {} < '{}') ", col, from, col, to));
        }
        start += idx1 + idx2 + END_PATT.len();
        if start > sql.len() {
            start = sql.len();
        }
    }
    res.push_str(&sql[start..]);
    if period_mode {
        res = res.replace("{{from}}", &format!("(now() -'{}'::interval)", period));
        res = res.replace("{{to}}", "(now())");
    } else {
        res = res.replace("{{from}}", &format!("'{}'", from));
        res = res.replace("{{to}}", &format!("'{}'", to));
    }
    (res, s_hours)
}

/// Make raw bytes a valid UTF-8 string: NUL bytes and invalid sequences are
/// dropped (Go `SafeUTF8String`).
pub fn safe_utf8_bytes(input: &[u8]) -> String {
    let no_nul: Vec<u8> = input.iter().copied().filter(|b| *b != 0).collect();
    let mut out = String::with_capacity(no_nul.len());
    let mut rest: &[u8] = &no_nul;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                out.push_str(s);
                break;
            }
            Err(e) => {
                let valid = e.valid_up_to();
                out.push_str(std::str::from_utf8(&rest[..valid]).expect("validated prefix"));
                match e.error_len() {
                    Some(n) => rest = &rest[valid + n..],
                    None => break,
                }
            }
        }
    }
    out
}

/// [`safe_utf8_bytes`] for an already valid string (only NULs can be removed).
pub fn safe_utf8_string(input: &str) -> String {
    input.replace('\0', "")
}

/// Replace every run of non-word characters (other than `-`) with `-` and
/// lowercase the result.
pub fn slugify(arg: &str) -> String {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE.get_or_init(|| goregex::compile(r"[^\w-]+").expect("valid regex"));
    re.replace_all(arg, "-").to_lowercase()
}

/// Load the map of SHA1s to anonymize from a CSV file (`sha1` header skipped);
/// falls back to `<data_dir>/<config_file>`. Missing file → empty map.
pub fn get_hidden(ctx: &Ctx, config_file: &str) -> BTreeMap<String, String> {
    let mut sha_map = BTreeMap::new();
    let file = match std::fs::File::open(config_file) {
        Ok(f) => Some(f),
        Err(_) => std::fs::File::open(format!("{}/{}", ctx.data_dir, config_file)).ok(),
    };
    if let Some(f) = file {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(false)
            .from_reader(f);
        for row in reader.records() {
            let row = match row {
                Ok(r) => r,
                Err(e) => fatal_on_error(go_csv_error_string(&e)),
            };
            let sha = row.get(0).unwrap_or("");
            if sha == "sha1" {
                continue;
            }
            sha_map.insert(sha.to_string(), format!("anon-{}", sha));
        }
    }
    sha_map
}

fn go_csv_error_string(e: &csv::Error) -> String {
    match e.kind() {
        // Go `csv.ParseError` with `ErrFieldCount`: `record on line N: wrong
        // number of fields`.
        csv::ErrorKind::UnequalLengths { pos, .. } => format!(
            "record on line {}: wrong number of fields",
            pos.as_ref().map(|p| p.line()).unwrap_or(0)
        ),
        csv::ErrorKind::Io(io) => go_io_error_string(io),
        _ => e.to_string(),
    }
}

/// Hex SHA1 of a string.
pub fn sha1_hex(s: &str) -> String {
    let digest = Sha1::digest(s.as_bytes());
    let mut out = String::with_capacity(40);
    for b in digest {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Anonymizer built from a SHA1 → replacement map; caches SHA1s of seen
/// arguments. Thread safe (covers both Go `MaybeHideFunc` and `MaybeHideFuncTS`).
pub struct MaybeHide {
    shas: BTreeMap<String, String>,
    cache: Mutex<HashMap<String, String>>,
}

impl MaybeHide {
    pub fn new(shas: BTreeMap<String, String>) -> Self {
        MaybeHide {
            shas,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Return the anonymized replacement of `arg` if its SHA1 is in the map,
    /// otherwise `arg` itself.
    pub fn hide(&self, arg: &str) -> String {
        let sha = {
            let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
            cache
                .entry(arg.to_string())
                .or_insert_with(|| sha1_hex(arg))
                .clone()
        };
        match self.shas.get(&sha) {
            Some(anon) => anon.clone(),
            None => arg.to_string(),
        }
    }
}

/// Closure form of [`MaybeHide`] mirroring Go's `MaybeHideFunc`.
pub fn maybe_hide_func(shas: BTreeMap<String, String>) -> impl Fn(&str) -> String + Send + Sync {
    let hider = MaybeHide::new(shas);
    move |arg: &str| hider.hide(arg)
}

/// Go `MakeUniqueSort`: unique, sorted copy of the strings.
pub fn make_unique_sort(ary: &[String]) -> Vec<String> {
    let set: std::collections::BTreeSet<&String> = ary.iter().collect();
    set.into_iter().cloned().collect()
}

/// Random hex string (Go `fmt.Sprintf("%x", rand.Uint64())`).
pub fn rand_string() -> String {
    format!("{:x}", rng::next_u64())
}

fn hex_and_list(raw: &[u8]) -> (String, String) {
    let hex: String = raw.iter().map(|b| format!("{:02x}", b)).collect();
    let list: Vec<String> = raw.iter().map(|b| b.to_string()).collect();
    (hex, format!("[{}]", list.join(" ")))
}

/// Debug format of a byte slice, Go style: `[]uint8(3):616263:[97 98 99]`.
pub fn format_raw_bytes(raw: &[u8]) -> String {
    let (hex, list) = hex_and_list(raw);
    format!("[]uint8({}):{}:{}", raw.len(), hex, list)
}

/// Debug format of a raw value of the given Go type name (`[]uint8`,
/// `sql.RawBytes`, ...): `<type>:<hex>:[b b b]`.
pub fn format_raw_interface(type_name: &str, raw: &[u8]) -> String {
    let (hex, list) = hex_and_list(raw);
    format!("{}:{}:{}", type_name, hex, list)
}

/// Does `path` exist (file or directory)?
pub fn path_exists<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_utf8_table() {
        assert_eq!(safe_utf8_bytes(b"A b\x00C"), "A bC");
        assert_eq!(safe_utf8_bytes(&[0x41, 0x42, 0xff, 0xfe, 0x43]), "ABC");
        assert_eq!(
            safe_utf8_bytes(&[0x00, 0x41, 0x42, 0xff, 0xff, 0x43, 0x00]),
            "ABC"
        );
        assert_eq!(safe_utf8_bytes("gżegżółką".as_bytes()), "gżegżółką");
        assert_eq!(safe_utf8_bytes(&[0xe4, 0xbd]), "");
        assert_eq!(safe_utf8_string("A b\0C"), "A bC");
    }

    #[test]
    fn make_unique_sort_go_table() {
        // 1:1 port of Go `gha_test.go::TestMakeUniqueSort`.
        let test_cases: &[(&[&str], &[&str])] = &[
            (&[], &[]),
            (&["a", "b", "cde"], &["a", "b", "cde"]),
            (&["cde", "a", "b"], &["a", "b", "cde"]),
            (&["a", "a", "b", "cde"], &["a", "b", "cde"]),
            (
                &["a", "b", "b", "a", "cde", "a", "cde", "b"],
                &["a", "b", "cde"],
            ),
            (&["a", "a", "b", "b", "b", "cde", "cde"], &["a", "b", "cde"]),
        ];
        for (index, (input, expected)) in test_cases.iter().enumerate() {
            let input: Vec<String> = input.iter().map(|s| s.to_string()).collect();
            let got = make_unique_sort(&input);
            let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(
                got,
                expected,
                "test number {}, expected {expected:?}, got {got:?}",
                index + 1
            );
        }
    }

    #[test]
    fn slugify_table() {
        assert_eq!(slugify("A b C"), "a-b-c");
        assert_eq!(slugify("Hello, world\t   bye"), "hello-world-bye");
        assert_eq!(slugify("Activity Repo Groups"), "activity-repo-groups");
        assert_eq!(slugify("Open issues/PRs"), "open-issues-prs");
        // Go `\w` is ASCII-only, so every non-ASCII letter becomes a dash.
        assert_eq!(slugify("gżegżółką"), "g-eg-k-");
    }

    #[test]
    fn maybe_hide_table() {
        let shas: BTreeMap<String, String> = [
            "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8",
            "e9d71f5ee7c92d6dc9e92ffdad17b8bd49418f98",
            "84a516841ba77a5b4648de2cd0dfcb30ea46dbb4",
        ]
        .iter()
        .map(|s| (s.to_string(), format!("anon-{}", s)))
        .collect();
        let f = maybe_hide_func(shas);
        let args = ["a", "a", "b", "d", "c", "e", "a", "x"];
        let results = [
            "anon-86f7e437faa5a7fce15d1ddcb9eaeaea377667b8",
            "anon-86f7e437faa5a7fce15d1ddcb9eaeaea377667b8",
            "anon-e9d71f5ee7c92d6dc9e92ffdad17b8bd49418f98",
            "d",
            "anon-84a516841ba77a5b4648de2cd0dfcb30ea46dbb4",
            "e",
            "anon-86f7e437faa5a7fce15d1ddcb9eaeaea377667b8",
            "x",
        ];
        for (arg, expected) in args.iter().zip(results.iter()) {
            assert_eq!(f(arg), *expected);
        }
        let none = maybe_hide_func(BTreeMap::new());
        for arg in ["a", "b", "c"] {
            assert_eq!(none(arg), arg);
        }
    }

    #[test]
    fn sha1_hex_known() {
        assert_eq!(sha1_hex("a"), "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8");
        assert_eq!(sha1_hex(""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn get_hidden_reads_csv_with_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hide.csv");
        std::fs::write(&path, "sha1\nabc\ndef\n").unwrap();
        let ctx = Ctx {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..Ctx::default()
        };
        let expected: BTreeMap<String, String> = [("abc", "anon-abc"), ("def", "anon-def")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(get_hidden(&ctx, path.to_str().unwrap()), expected);
        assert_eq!(get_hidden(&ctx, "hide.csv"), expected);
        assert!(get_hidden(&ctx, "missing.csv").is_empty());
    }

    #[test]
    fn raw_bytes_formats() {
        assert_eq!(format_raw_bytes(b"abc"), "[]uint8(3):616263:[97 98 99]");
        assert_eq!(format_raw_bytes(b""), "[]uint8(0)::[]");
        assert_eq!(
            format_raw_interface("sql.RawBytes", &[0, 255]),
            "sql.RawBytes:00ff:[0 255]"
        );
    }

    #[test]
    fn rand_string_is_hex() {
        let s = rand_string();
        assert!(!s.is_empty() && s.len() <= 16);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn quick_range_query_table() {
        let cases: &[(&str, &str, &str, &str, &str, &str)] = &[
            ("simplest period {{period:a}} case", "", "", "", QUICK_RANGE_QUERY_ERROR, "0"),
            ("simplest no-period case", "", "", "", "simplest no-period case", "0"),
            ("simplest no-period case", "1 month", "", "", "simplest no-period case", "730.500000"),
            ("simplest no-period case", "0 month", "", "", "simplest no-period case", "0.000000"),
            ("simplest no-period case", "-3 days", "", "", "simplest no-period case", "0.000000"),
            ("simplest no-period case", "", "2010-01-01 12:00:00", "2010-01-01 12:00:00", "simplest no-period case", "0"),
            ("simplest no-period case", "", "2010-01-01 12:00:00", "2010-01-01 13:00:00", "simplest no-period case", "1.000000"),
            (
                "simplest period {{period:a}} case",
                "1 day",
                "",
                "",
                "simplest period  (a >= now() - '1 day'::interval)  case",
                "24.000000",
            ),
            (
                "simplest period {{period:a}} case",
                "",
                "2010-01-01 12:00:00",
                "2015-02-02 13:00:00",
                "simplest period  (a >= '2010-01-01 12:00:00' and a < '2015-02-02 13:00:00')  case",
                "44593.000000",
            ),
            (
                "simplest period {{period:a}} case",
                "1 week",
                "2010-01-01 12:00:00",
                "2015-02-02 13:00:00",
                "simplest period  (a >= now() - '1 week'::interval)  case",
                "168.000000",
            ),
            (
                "{{period:a.b.c}}{{period:c.d.e}}",
                "1 day",
                "",
                "",
                " (a.b.c >= now() - '1 day'::interval)  (c.d.e >= now() - '1 day'::interval) ",
                "24.000000",
            ),
            (
                "{{period:a.b.c}}{{period:c.d.e}}",
                "10 days",
                "",
                "",
                " (a.b.c >= now() - '10 days'::interval)  (c.d.e >= now() - '10 days'::interval) ",
                "240.000000",
            ),
            (
                "{{period:a.b.c}}{{period:c.d.e}}",
                "",
                "2015",
                "2016",
                " (a.b.c >= '2015-01-01 00:00:00' and a.b.c < '2016-01-01 00:00:00')  (c.d.e >= '2015-01-01 00:00:00' and c.d.e < '2016-01-01 00:00:00') ",
                "8760.000000",
            ),
            (
                "and ({{period:a.b.c}} and x is null) or {{period:c.d.e}}",
                "3 months",
                "",
                "",
                "and ( (a.b.c >= now() - '3 months'::interval)  and x is null) or  (c.d.e >= now() - '3 months'::interval) ",
                "2191.500000",
            ),
            (
                "and ({{period:a.b.c}} and x is null) or {{period:c.d.e}}",
                "",
                "1982-07-16",
                "2017-12",
                "and ( (a.b.c >= '1982-07-16 00:00:00' and a.b.c < '2017-12-01 00:00:00')  and x is null) or  (c.d.e >= '1982-07-16 00:00:00' and c.d.e < '2017-12-01 00:00:00') ",
                "310128.000000",
            ),
            (
                "and ({{period:a.b.c}} and x is null) or {{period:c.d.e}} and {{from}} - {{to}}",
                "",
                "1982-07-16",
                "2017-12",
                "and ( (a.b.c >= '1982-07-16 00:00:00' and a.b.c < '2017-12-01 00:00:00')  and x is null) or  (c.d.e >= '1982-07-16 00:00:00' and c.d.e < '2017-12-01 00:00:00')  and '1982-07-16 00:00:00' - '2017-12-01 00:00:00'",
                "310128.000000",
            ),
            (
                "and ({{period:a.b.c}} and x is null) or {{period:c.d.e}} and {{from}} or {{to}}",
                "3 months",
                "",
                "",
                "and ( (a.b.c >= now() - '3 months'::interval)  and x is null) or  (c.d.e >= now() - '3 months'::interval)  and (now() -'3 months'::interval) or (now())",
                "2191.500000",
            ),
        ];
        for (i, (sql, period, from, to, expected, hours)) in cases.iter().enumerate() {
            let (got, got_hours) = prepare_quick_range_query(sql, period, from, to);
            assert_eq!(got, *expected, "test number {}", i + 1);
            assert_eq!(got_hours, *hours, "test number {} hours", i + 1);
        }
    }
}
