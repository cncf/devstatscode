//! JSON helpers — port of `json.go`.
//!
//! The Go code uses `json-iterator` (`jsoniter.ConfigDefault`): 2-space
//! indentation, HTML-safe escaping (`<`, `>`, `&`, U+2028, U+2029 → `\u00XX`),
//! `\b`/`\f` written as `\u0008`/`\u000c`, floats in shortest `%f` form unless
//! `|x| < 1e-6 || |x| >= 1e21`. This module reproduces that formatting.
//!
//! Object keys are emitted **sorted** (`serde_json::Value` is a `BTreeMap`).
//! `jsoniter.MarshalIndent` of a *struct* writes the fields in declaration
//! order, and DevStats' structs happen to be declared alphabetically where it
//! matters; of a *map* (`PrettyPrintJSON`, `website_data`'s file) the Go code
//! used to iterate randomly — `PrettyPrintJSON` now sorts (Go bug 28, fixed
//! with `encoding/json`, whose only formatting difference is the exponent of
//! tiny/huge floats: `1e-7` instead of jsoniter's `1e-07` — see
//! [`GoJsonFormatter::stdlib`]) and keeps integers exact (`json.Number`),
//! like `serde_json` does here.

use std::io;

use serde::Serialize;
use serde_json::ser::{CharEscape, CompactFormatter, Formatter, PrettyFormatter};

use crate::error::{fatal_on_error, go_io_error_string};

/// Pretty printer producing jsoniter-compatible output.
pub struct GoJsonFormatter<'a> {
    inner: PrettyFormatter<'a>,
    /// `encoding/json` exponent style (`1e-7`) instead of jsoniter's (`1e-07`).
    stdlib: bool,
}

impl Default for GoJsonFormatter<'_> {
    fn default() -> Self {
        GoJsonFormatter {
            inner: PrettyFormatter::with_indent(b"  "),
            stdlib: false,
        }
    }
}

impl GoJsonFormatter<'_> {
    /// Formatter matching `encoding/json`'s `MarshalIndent` (used by Go's
    /// `PrettyPrintJSON`): identical to jsoniter's output except that the
    /// exponent of tiny/huge floats is not zero-padded (`1e-7`, `1e+21`).
    pub fn stdlib() -> Self {
        GoJsonFormatter {
            inner: PrettyFormatter::with_indent(b"  "),
            stdlib: true,
        }
    }
}

/// Format a float the way jsoniter's `WriteFloat64` does.
pub fn go_json_float(v: f64) -> String {
    go_float(v, false)
}

/// Format a float the way `encoding/json` does (`1e-7`, not `1e-07`).
pub fn go_stdlib_json_float(v: f64) -> String {
    go_float(v, true)
}

fn go_float(v: f64, stdlib: bool) -> String {
    let abs = v.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        // strconv 'e' with shortest precision, Go pads the exponent to 2 digits
        let s = format!("{:e}", v);
        let (mantissa, exp) = s.split_once('e').unwrap_or((s.as_str(), "0"));
        let (sign, digits) = match exp.strip_prefix('-') {
            Some(d) => ("-", d),
            None => ("+", exp),
        };
        let digits = if digits.len() < 2 && !(stdlib && sign == "-") {
            // encoding/json cleans `e-07` up to `e-7` (only negative exponents)
            format!("0{}", digits)
        } else {
            digits.to_string()
        };
        format!("{}e{}{}", mantissa, sign, digits)
    } else {
        // strconv 'f' with shortest precision: no exponent, no trailing ".0"
        let s = format!("{}", v);
        match s.split_once('e') {
            None => s,
            Some(_) => {
                // Rust switches to exponent form for large magnitudes; expand it.
                let mut out = format!("{:.0}", v);
                if v.fract() != 0.0 {
                    out = format!("{}", v);
                }
                out
            }
        }
    }
}

impl Formatter for GoJsonFormatter<'_> {
    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(go_float(value, self.stdlib).as_bytes())
    }

    fn write_f32<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f32) -> io::Result<()> {
        writer.write_all(go_float(f64::from(value), self.stdlib).as_bytes())
    }

    fn write_string_fragment<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        let mut start = 0;
        for (i, c) in fragment.char_indices() {
            let esc = match c {
                '<' => "\\u003c",
                '>' => "\\u003e",
                '&' => "\\u0026",
                '\u{2028}' => "\\u2028",
                '\u{2029}' => "\\u2029",
                _ => continue,
            };
            writer.write_all(&fragment.as_bytes()[start..i])?;
            writer.write_all(esc.as_bytes())?;
            start = i + c.len_utf8();
        }
        writer.write_all(&fragment.as_bytes()[start..])
    }

    fn write_char_escape<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        char_escape: CharEscape,
    ) -> io::Result<()> {
        match char_escape {
            CharEscape::Backspace => writer.write_all(b"\\u0008"),
            CharEscape::FormFeed => writer.write_all(b"\\u000c"),
            other => self.inner.write_char_escape(writer, other),
        }
    }

    fn begin_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.inner.begin_array(writer)
    }

    fn end_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.inner.end_array(writer)
    }

    fn begin_array_value<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.inner.begin_array_value(writer, first)
    }

    fn end_array_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.inner.end_array_value(writer)
    }

    fn begin_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.inner.begin_object(writer)
    }

    fn end_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.inner.end_object(writer)
    }

    fn begin_object_key<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.inner.begin_object_key(writer, first)
    }

    fn begin_object_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.inner.begin_object_value(writer)
    }

    fn end_object_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.inner.end_object_value(writer)
    }
}

/// Serialize `value` as pretty JSON (Go/jsoniter formatting, sorted object keys
/// via `serde_json::Value`'s `BTreeMap` — the crate is built with
/// `preserve_order` disabled).
pub fn to_pretty_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, GoJsonFormatter::default());
    value.serialize(&mut ser)?;
    Ok(out)
}

/// Serialize `value` like Go's `PrettyPrintJSON` output (`encoding/json`
/// `MarshalIndent`: 2-space indent, sorted keys, `1e-7` exponents).
pub fn to_pretty_json_stdlib<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, GoJsonFormatter::stdlib());
    value.serialize(&mut ser)?;
    Ok(out)
}

/// Compact (single line) jsoniter-compatible formatter: what
/// `jsoniter.Marshal` / `jsoniter.NewEncoder(w).Encode` write (HTML-safe
/// escaping, jsoniter float formatting, no whitespace).
#[derive(Default)]
pub struct GoJsonCompactFormatter {
    inner: CompactFormatter,
}

impl Formatter for GoJsonCompactFormatter {
    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(go_float(value, false).as_bytes())
    }

    fn write_f32<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f32) -> io::Result<()> {
        writer.write_all(go_float(f64::from(value), false).as_bytes())
    }

    fn write_string_fragment<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        GoJsonFormatter::default().write_string_fragment(writer, fragment)
    }

    fn write_char_escape<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        char_escape: CharEscape,
    ) -> io::Result<()> {
        match char_escape {
            CharEscape::Backspace => writer.write_all(b"\\u0008"),
            CharEscape::FormFeed => writer.write_all(b"\\u000c"),
            other => self.inner.write_char_escape(writer, other),
        }
    }
}

/// Serialize `value` as compact JSON the way `jsoniter.Marshal` does (struct
/// fields in declaration order, `serde_json::Value` maps sorted).
pub fn to_compact_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::new();
    let mut ser =
        serde_json::Serializer::with_formatter(&mut out, GoJsonCompactFormatter::default());
    value.serialize(&mut ser)?;
    Ok(out)
}

/// Go `jsoniter.NewEncoder(w).Encode(value)`: [`to_compact_json`] followed by
/// a newline.
pub fn encode_json_line<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = to_compact_json(value)?;
    out.push(b'\n');
    Ok(out)
}

/// Pretty format raw JSON bytes (2-space indent, sorted keys); invalid JSON is
/// fatal, like Go's `PrettyPrintJSON`.
pub fn pretty_print_json(json_bytes: &[u8]) -> Vec<u8> {
    let value: serde_json::Value = match serde_json::from_slice(json_bytes) {
        Ok(v) => v,
        Err(e) => fatal_on_error(e),
    };
    match to_pretty_json_stdlib(&value) {
        Ok(v) => v,
        Err(e) => fatal_on_error(e),
    }
}

/// Serialize `obj` as pretty JSON into file `path` (mode 0644) — Go's
/// `ObjectToJSON` (`jsoniter.Marshal` + `PrettyPrintJSON`).
pub fn object_to_json<T: Serialize>(obj: &T, path: &str) {
    let value = match serde_json::to_value(obj) {
        Ok(v) => v,
        Err(e) => fatal_on_error(e),
    };
    let pretty = match to_pretty_json_stdlib(&value) {
        Ok(v) => v,
        Err(e) => fatal_on_error(e),
    };
    write_file_0644(path, &pretty);
}

/// `ioutil.WriteFile(fn, data, 0644)` with Go-style fatal error text.
pub fn write_file_0644(path: &str, data: &[u8]) {
    if let Err(e) = std::fs::write(path, data) {
        fatal_on_error(format!("open {}: {}", path, go_io_error_string(&e)));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mut perm = meta.permissions();
            if perm.mode() & 0o777 != 0o644 {
                perm.set_mode(0o644);
                let _ = std::fs::set_permissions(path, perm);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pp(s: &str) -> String {
        String::from_utf8(pretty_print_json(s.as_bytes())).unwrap()
    }

    #[test]
    fn compact_like_jsoniter_encoder() {
        #[derive(Serialize)]
        struct P {
            project: String,
            values: Option<Vec<i64>>,
            f: f64,
        }
        let p = P {
            project: "a<b>&c\u{2028}\u{8}".to_string(),
            values: None,
            f: 1e21,
        };
        assert_eq!(
            String::from_utf8(encode_json_line(&p).unwrap()).unwrap(),
            "{\"project\":\"a\\u003cb\\u003e\\u0026c\\u2028\\u0008\",\"values\":null,\"f\":1e+21}\n"
        );
        assert_eq!(
            String::from_utf8(to_compact_json(&json!({"b": [1.5, 2], "a": {}})).unwrap()).unwrap(),
            "{\"a\":{},\"b\":[1.5,2]}"
        );
    }

    #[test]
    fn floats_like_jsoniter() {
        assert_eq!(go_json_float(1.0), "1");
        assert_eq!(go_json_float(100.0), "100");
        assert_eq!(go_json_float(1.5), "1.5");
        assert_eq!(go_json_float(-2.25), "-2.25");
        assert_eq!(go_json_float(0.0), "0");
        assert_eq!(go_json_float(1e20), "100000000000000000000");
        assert_eq!(go_json_float(1e21), "1e+21");
        assert_eq!(go_json_float(1.5e-7), "1.5e-07");
        assert_eq!(go_json_float(0.000001), "0.000001");
        assert_eq!(go_json_float(12345678901.0), "12345678901");
        // encoding/json (Go's PrettyPrintJSON) only differs in the exponent padding
        assert_eq!(go_stdlib_json_float(1.5e-7), "1.5e-7");
        assert_eq!(go_stdlib_json_float(1e-10), "1e-10");
        assert_eq!(go_stdlib_json_float(1e21), "1e+21");
        assert_eq!(go_stdlib_json_float(-0.0), "-0");
        assert_eq!(go_stdlib_json_float(1.5), "1.5");
    }

    #[test]
    fn pretty_print_like_go_pretty_print_json() {
        // verified against the (fixed) Go PrettyPrintJSON on the same input
        let input = concat!(
            r#"{"z":1,"a":{"y":2,"b":[1,2,{"q":1,"p":2}]},"m":"x","k":null,"f":1.5,"e":1e3,"#,
            r#""u":"\u003c&\u00e9","big":12345678901234567890,"neg":-0.0,"exp":1.0e-7}"#
        );
        let expected = concat!(
            "{\n  \"a\": {\n    \"b\": [\n      1,\n      2,\n      {\n        \"p\": 2,\n",
            "        \"q\": 1\n      }\n    ],\n    \"y\": 2\n  },\n  \"big\": 12345678901234567890,\n",
            "  \"e\": 1000,\n  \"exp\": 1e-7,\n  \"f\": 1.5,\n  \"k\": null,\n  \"m\": \"x\",\n",
            "  \"neg\": -0,\n  \"u\": \"\\u003c\\u0026é\",\n  \"z\": 1\n}"
        );
        assert_eq!(pp(input), expected);
    }

    #[test]
    fn pretty_print_shape() {
        assert_eq!(pp("{}"), "{}");
        assert_eq!(pp("[]"), "[]");
        assert_eq!(pp(r#"{"b":1,"a":[1,2.5,{"x":null}],"c":"<&>"}"#), "{\n  \"a\": [\n    1,\n    2.5,\n    {\n      \"x\": null\n    }\n  ],\n  \"b\": 1,\n  \"c\": \"\\u003c\\u0026\\u003e\"\n}");
        assert_eq!(
            pp(r#""a\b\f\n\t\u2028""#),
            "\"a\\u0008\\u000c\\n\\t\\u2028\""
        );
        assert_eq!(
            pp("[1.0, 2.50, 1e2, true]"),
            "[\n  1,\n  2.5,\n  100,\n  true\n]"
        );
    }

    #[test]
    fn object_to_json_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.json");
        let p = path.to_str().unwrap();
        object_to_json(&json!({"z": 1, "a": "b"}), p);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\n  \"a\": \"b\",\n  \"z\": 1\n}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o644
            );
        }
    }
}
