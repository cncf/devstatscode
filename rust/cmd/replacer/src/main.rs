//! `replacer` — replace a regexp or a string inside a file, in place.
//!
//! Environment:
//! * `FROM`  — pattern (regexp or literal string, depending on `MODE`); `-` means empty
//! * `TO`    — replacement; `-` means empty (delete). Empty `TO` requires `NO_TO` to be set
//! * `NO_TO` — set to anything to allow an empty/unset `TO`
//! * `MODE`  — one of:
//!   * `rr`, `rr0` — regexp → regexp (`$1`, `${name}` expansions in `TO`)
//!   * `rs`, `rs0` — regexp → literal string (no `$` expansion in `TO`)
//!   * `ss`, `ss0` — literal string → literal string
//!
//!   A trailing `0` means "no hits is fine" (exit 0); without it, "nothing replaced"
//!   is an error (exit 1). Typical: ``MODE=ss FROM=`cat in` TO=`cat out` replacer file``.
//! * `NREPLACES`   — (`ss*` only) replace at most N occurrences (default: all)
//! * `REPLACEFROM` — (`ss*` only) only replace after byte offset N of the file
//!
//! Arguments: `replacer <file>`.
//!
//! Output (stdout): `Hits: <file>` when the file was modified,
//! `Nothing replaced in: <file>` otherwise. Exit codes: 0 ok, 1 usage/file
//! error or "nothing replaced" in non-`0` modes, 2 fatal (bad `NREPLACES`/`REPLACEFROM`,
//! invalid regexp).

use std::borrow::Cow;
use std::env;
use std::fs;
use std::process::ExitCode;

use devstatscode::goregex;
use devstatscode::{fatal_on_err, fatalf};
use regex::bytes::NoExpand;

/// Replacement mode, parsed from `MODE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    RegexpToRegexp,
    RegexpToString,
    StringToString,
}

impl Mode {
    /// Parse `MODE`; returns the mode and whether "no hits" is allowed (trailing `0`).
    pub fn parse(mode: &str) -> Option<(Mode, bool)> {
        match mode {
            "rr" => Some((Mode::RegexpToRegexp, false)),
            "rr0" => Some((Mode::RegexpToRegexp, true)),
            "rs" => Some((Mode::RegexpToString, false)),
            "rs0" => Some((Mode::RegexpToString, true)),
            "ss" => Some((Mode::StringToString, false)),
            "ss0" => Some((Mode::StringToString, true)),
            _ => None,
        }
    }
}

/// Literal-string replacement of up to `limit` occurrences (`None` = all).
///
/// An empty `from` matches at every UTF-8 character boundary (start, between
/// characters, end), like Go's `strings.Replace`.
pub fn replace_bytes(hay: &[u8], from: &[u8], to: &[u8], limit: Option<usize>) -> Vec<u8> {
    let max = limit.unwrap_or(usize::MAX);
    if max == 0 || from == to {
        return hay.to_vec();
    }
    let mut out = Vec::with_capacity(hay.len());
    if from.is_empty() {
        let mut done = 0;
        for &b in hay {
            let boundary = (b & 0xC0) != 0x80;
            if boundary && done < max {
                out.extend_from_slice(to);
                done += 1;
            }
            out.push(b);
        }
        if done < max {
            out.extend_from_slice(to);
        }
        return out;
    }
    let mut last = 0;
    for pos in memchr::memmem::find_iter(hay, from).take(max) {
        out.extend_from_slice(&hay[last..pos]);
        out.extend_from_slice(to);
        last = pos + from.len();
    }
    out.extend_from_slice(&hay[last..]);
    out
}

/// Apply `mode` to `contents`; `replace_from` restricts `ss*` replacements to
/// the tail of the file starting at that byte offset.
pub fn apply(
    mode: Mode,
    contents: &[u8],
    from: &str,
    to: &str,
    n_replaces: Option<usize>,
    replace_from: Option<usize>,
) -> Result<Vec<u8>, regex::Error> {
    Ok(match mode {
        Mode::RegexpToRegexp | Mode::RegexpToString => {
            let re = goregex::compile_bytes(from)?;
            let replaced: Cow<[u8]> = if mode == Mode::RegexpToString {
                re.replace_all(contents, NoExpand(to.as_bytes()))
            } else {
                re.replace_all(contents, to.as_bytes())
            };
            replaced.into_owned()
        }
        Mode::StringToString => {
            let start = replace_from.unwrap_or(0);
            let (head, tail) = contents.split_at(start);
            let mut out = head.to_vec();
            out.extend(replace_bytes(
                tail,
                from.as_bytes(),
                to.as_bytes(),
                n_replaces,
            ));
            out
        }
    })
}

/// Parse a positive integer env variable (`NREPLACES`, `REPLACEFROM`); dies on error.
fn positive_env(name: &str) -> Option<usize> {
    let raw = env::var(name).ok().filter(|v| !v.is_empty())?;
    let value: i64 = fatal_on_err(
        raw.parse::<i64>()
            .map_err(|e| format!("invalid {name} value {raw:?}: {e}")),
    );
    if value < 1 {
        fatalf(format_args!("{name} must be positive"));
    }
    Some(value as usize)
}

fn replacer(from: &str, to: &str, fname: &str, mode: &str) -> ExitCode {
    let from = if from == "-" { "" } else { from };
    let to = if to == "-" { "" } else { to };

    let contents = match fs::read(fname) {
        Ok(c) => c,
        Err(e) => {
            println!("Error: {fname}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let n_replaces = positive_env("NREPLACES");
    let replace_from = positive_env("REPLACEFROM");
    if let Some(rf) = replace_from {
        if rf >= contents.len() {
            fatalf(format_args!(
                "REPLACEFROM must be less than file length {}",
                contents.len()
            ));
        }
    }

    let Some((parsed_mode, allow_no_hits)) = Mode::parse(mode) else {
        println!("Unknown mode '{mode}'");
        return ExitCode::FAILURE;
    };
    let new_contents = fatal_on_err(
        apply(parsed_mode, &contents, from, to, n_replaces, replace_from)
            .map_err(|e| format!("regexp: Compile({from:?}): {e}")),
    );
    if new_contents == contents {
        println!("Nothing replaced in: {fname}");
        return if allow_no_hits {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }
    println!("Hits: {fname}");
    if let Err(e) = fs::write(fname, &new_contents) {
        println!("Error: {fname}: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    devstatscode::error::exit_on_panic();
    let from = env::var("FROM").unwrap_or_default();
    if from.is_empty() {
        println!("You need to set 'FROM' env variable");
        return ExitCode::FAILURE;
    }
    let to = env::var("TO").unwrap_or_default();
    let no_to = env::var("NO_TO").unwrap_or_default();
    if to.is_empty() && no_to.is_empty() {
        println!("You need to set 'TO' env variable or specify NO_TO");
        return ExitCode::FAILURE;
    }
    let mode = env::var("MODE").unwrap_or_default();
    if mode.is_empty() {
        println!("You need to set 'MODE' env variable");
        return ExitCode::FAILURE;
    }
    let Some(fname) = env::args().nth(1) else {
        println!("You need to provide a file name");
        return ExitCode::FAILURE;
    };
    replacer(&from, &to, &fname, &mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: Vec<u8>) -> String {
        String::from_utf8(v).unwrap()
    }

    #[test]
    fn mode_parsing() {
        assert_eq!(Mode::parse("rr"), Some((Mode::RegexpToRegexp, false)));
        assert_eq!(Mode::parse("rr0"), Some((Mode::RegexpToRegexp, true)));
        assert_eq!(Mode::parse("rs"), Some((Mode::RegexpToString, false)));
        assert_eq!(Mode::parse("rs0"), Some((Mode::RegexpToString, true)));
        assert_eq!(Mode::parse("ss"), Some((Mode::StringToString, false)));
        assert_eq!(Mode::parse("ss0"), Some((Mode::StringToString, true)));
        for bad in ["", "RR", "ss00", "sr", "xx", "0"] {
            assert_eq!(Mode::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn string_replace_all_and_limited() {
        assert_eq!(
            s(replace_bytes(b"a.b.c.d", b".", b"::", None)),
            "a::b::c::d"
        );
        assert_eq!(
            s(replace_bytes(b"a.b.c.d", b".", b"::", Some(2))),
            "a::b::c.d"
        );
        assert_eq!(s(replace_bytes(b"a.b.c.d", b".", b"", None)), "abcd");
        assert_eq!(s(replace_bytes(b"aaaa", b"aa", b"b", None)), "bb");
        assert_eq!(s(replace_bytes(b"nothing", b"zzz", b"y", None)), "nothing");
        assert_eq!(s(replace_bytes(b"", b"a", b"b", None)), "");
    }

    #[test]
    fn string_replace_empty_pattern_inserts_at_char_boundaries() {
        assert_eq!(s(replace_bytes("ab".as_bytes(), b"", b"-", None)), "-a-b-");
        assert_eq!(s(replace_bytes("Łu".as_bytes(), b"", b"-", None)), "-Ł-u-");
        assert_eq!(
            s(replace_bytes("ab".as_bytes(), b"", b"-", Some(2))),
            "-a-b"
        );
        assert_eq!(s(replace_bytes(b"", b"", b"-", None)), "-");
    }

    #[test]
    fn string_replace_multiline_blob() {
        let hay = b"line1\n  \"uid\": \"abc\",\nline3\n";
        let from = b"  \"uid\": \"abc\",\n";
        let to = b"  \"uid\": \"xyz\",\n  \"new\": 1,\n";
        assert_eq!(
            s(replace_bytes(hay, from, to, None)),
            "line1\n  \"uid\": \"xyz\",\n  \"new\": 1,\nline3\n"
        );
    }

    #[test]
    fn apply_ss_with_replace_from_offset() {
        let out = apply(Mode::StringToString, b"xx-xx-xx", "xx", "y", None, Some(3)).unwrap();
        assert_eq!(s(out), "xx-y-y");
        let out = apply(
            Mode::StringToString,
            b"xx-xx-xx",
            "xx",
            "y",
            Some(1),
            Some(3),
        )
        .unwrap();
        assert_eq!(s(out), "xx-y-xx");
    }

    #[test]
    fn apply_rr_expands_groups() {
        // Real pattern from devstats/devel/update_dashboards_labels.sh style usage.
        let out = apply(
            Mode::RegexpToRegexp,
            b"where (author {{exclude_bots}}) and x",
            r"\((.*)\s+{{exclude_bots}}\)",
            "(lower($1) {{exclude_bots}})",
            None,
            None,
        )
        .unwrap();
        assert_eq!(s(out), "where (lower(author) {{exclude_bots}}) and x");
    }

    #[test]
    fn apply_rr_multiline_crontab_toggle() {
        // devstats/devel/cronctl.sh: comment out / uncomment crontab lines.
        let crontab = b"# header\n*/5 * * * * devstats_sync k8s\n#0 * * * * other_job k8s\n";
        let out = apply(
            Mode::RegexpToRegexp,
            crontab,
            r"(?m)^([^#].*\s+devstats_sync\s+.*)$",
            "#$1",
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            s(out),
            "# header\n#*/5 * * * * devstats_sync k8s\n#0 * * * * other_job k8s\n"
        );
        let out = apply(
            Mode::RegexpToRegexp,
            crontab,
            r"(?m)^#(.*\s+other_job\s+.*)$",
            "$1",
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            s(out),
            "# header\n*/5 * * * * devstats_sync k8s\n0 * * * * other_job k8s\n"
        );
    }

    #[test]
    fn apply_rs_is_literal() {
        // devstats: MODE=rs0 FROM='(?m)^.*"uid": "\w+",\n' TO='-'
        let json = b"{\n  \"uid\": \"abc_123\",\n  \"title\": \"$1\"\n}\n";
        let out = apply(
            Mode::RegexpToString,
            json,
            r#"(?m)^.*"uid": "\w+",\n"#,
            "$1-",
            None,
            None,
        )
        .unwrap();
        assert_eq!(s(out), "{\n$1-  \"title\": \"$1\"\n}\n");
    }

    #[test]
    fn apply_invalid_regexp_is_an_error() {
        assert!(apply(Mode::RegexpToRegexp, b"x", "(unclosed", "y", None, None).is_err());
        assert!(apply(Mode::RegexpToString, b"x", "[z-a]", "y", None, None).is_err());
    }

    #[test]
    fn apply_regexp_modes_work_on_non_utf8_content() {
        let bytes = b"a\xFFb a\xFEb";
        let out = apply(Mode::RegexpToRegexp, bytes, "a(.)b", "<$1>", None, None).unwrap();
        // Invalid bytes are neither matched by `.` nor corrupted.
        assert_eq!(out, bytes.to_vec());
        let out = apply(Mode::RegexpToRegexp, bytes, "a", "A", None, None).unwrap();
        assert_eq!(out, b"A\xFFb A\xFEb".to_vec());
    }
}
