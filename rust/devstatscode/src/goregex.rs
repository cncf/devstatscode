//! Go (`regexp`, RE2 syntax) → Rust (`regex` crate) pattern adapter.
//!
//! DevStats programs receive regular expressions from shell scripts, YAML
//! configuration and environment variables that were written for Go's `regexp`
//! package. Go and the `regex` crate share the RE2 dialect, but differ in a few
//! places that matter for real DevStats patterns:
//!
//! | construct | Go `regexp` | Rust `regex` | adapter |
//! |---|---|---|---|
//! | `{` not starting a valid `{n}`, `{n,}`, `{n,m}` (e.g. `{{exclude_bots}}`) | literal | syntax error | escaped |
//! | lone `}` | literal | literal | escaped (harmless) |
//! | `\d \w \s \b` (+ negations) | ASCII only | Unicode | rewritten to ASCII classes |
//! | `[` inside a class (not `[:posix:]`) | literal | nested class | escaped |
//! | `&&` `~~` inside a class | literal | set operators | escaped |
//!
//! Not translated (unused in DevStats, documented deviation): `\Q..\E` literal
//! quoting and octal escapes (`\123`) are Go-only; Rust-only syntax (`(?x)`,
//! `\p{Letter}`-style long names, nested classes) is accepted by Rust but would
//! be an error in Go.

use std::borrow::Cow;

/// Translate a Go regexp pattern into an equivalent `regex` crate pattern.
pub fn go_to_rust(pat: &str) -> Cow<'_, str> {
    if !pat.bytes().any(|b| matches!(b, b'{' | b'}' | b'\\' | b'[')) {
        return Cow::Borrowed(pat);
    }
    let chars: Vec<char> = pat.chars().collect();
    let mut out = String::with_capacity(pat.len() + 16);
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' => {
                i = emit_escape(&chars, i, &mut out, false);
                continue;
            }
            '[' => {
                i = emit_class(&chars, i, &mut out);
                continue;
            }
            '{' => {
                if let Some(end) = valid_repeat_end(&chars, i) {
                    out.extend(&chars[i..=end]);
                    i = end + 1;
                    continue;
                }
                out.push_str("\\{");
            }
            '}' => out.push_str("\\}"),
            _ => out.push(c),
        }
        i += 1;
    }
    Cow::Owned(out)
}

/// If `chars[i] == '{'` starts a Go repetition (`{n}`, `{n,}`, `{n,m}`), return
/// the index of the closing `}`.
fn valid_repeat_end(chars: &[char], i: usize) -> Option<usize> {
    let mut j = i + 1;
    let digits = |j: &mut usize| {
        let start = *j;
        while *j < chars.len() && chars[*j].is_ascii_digit() {
            *j += 1;
        }
        *j > start
    };
    if !digits(&mut j) {
        return None;
    }
    if j < chars.len() && chars[j] == ',' {
        j += 1;
        digits(&mut j);
    }
    (j < chars.len() && chars[j] == '}').then_some(j)
}

/// Emit the escape sequence starting at `chars[i] == '\\'`; returns next index.
fn emit_escape(chars: &[char], i: usize, out: &mut String, in_class: bool) -> usize {
    let Some(&c) = chars.get(i + 1) else {
        out.push('\\');
        return i + 1;
    };
    let ascii = match (c, in_class) {
        ('d', false) => Some("(?-u:\\d)"),
        // Negations as explicit (Unicode) classes: `(?-u:\D)` can match
        // invalid UTF-8, which the `str` regex flavour rejects at compile time
        ('D', false) => Some("[^0-9]"),
        ('w', false) => Some("(?-u:\\w)"),
        ('W', false) => Some("[^0-9A-Za-z_]"),
        ('s', false) => Some("(?-u:\\s)"),
        ('S', false) => Some("[^\\t\\n\\x0C\\r ]"),
        ('b', false) => Some("(?-u:\\b)"),
        ('B', false) => Some("(?-u:\\B)"),
        ('d', true) => Some("0-9"),
        ('D', true) => Some("[^0-9]"),
        ('w', true) => Some("0-9A-Za-z_"),
        ('W', true) => Some("[^0-9A-Za-z_]"),
        ('s', true) => Some("\\t\\n\\x0C\\r "),
        ('S', true) => Some("[^\\t\\n\\x0C\\r ]"),
        _ => None,
    };
    match ascii {
        Some(s) => out.push_str(s),
        None => {
            out.push('\\');
            out.push(c);
            // `\p{Greek}`, `\P{Lu}`, `\x{41}`: copy the braced argument verbatim.
            if matches!(c, 'p' | 'P' | 'x') && chars.get(i + 2) == Some(&'{') {
                if let Some(close) = chars[i + 2..].iter().position(|&ch| ch == '}') {
                    out.extend(&chars[i + 2..=i + 2 + close]);
                    return i + 3 + close;
                }
            }
        }
    }
    i + 2
}

/// Emit a bracket class starting at `chars[i] == '['`; returns next index.
fn emit_class(chars: &[char], i: usize, out: &mut String) -> usize {
    out.push('[');
    let mut j = i + 1;
    if j < chars.len() && chars[j] == '^' {
        out.push('^');
        j += 1;
    }
    // A `]` right after `[` or `[^` is a literal in Go (and in Rust).
    if j < chars.len() && chars[j] == ']' {
        out.push_str("\\]");
        j += 1;
    }
    while j < chars.len() {
        let c = chars[j];
        match c {
            ']' => {
                out.push(']');
                return j + 1;
            }
            '\\' => {
                j = emit_escape(chars, j, out, true);
                continue;
            }
            '[' => {
                if let Some(end) = posix_class_end(chars, j) {
                    out.extend(&chars[j..=end]);
                    j = end + 1;
                    continue;
                }
                out.push_str("\\[");
            }
            '&' | '~' if chars.get(j + 1) == Some(&c) => {
                out.push('\\');
                out.push(c);
                out.push('\\');
                out.push(c);
                j += 2;
                continue;
            }
            _ => out.push(c),
        }
        j += 1;
    }
    // Unterminated class: leave as is, the regex compiler reports the error.
    j
}

/// If `chars[i..]` starts a POSIX class `[:name:]` / `[:^name:]`, return the index of its final `]`.
fn posix_class_end(chars: &[char], i: usize) -> Option<usize> {
    if chars.get(i + 1) != Some(&':') {
        return None;
    }
    let mut j = i + 2;
    if chars.get(j) == Some(&'^') {
        j += 1;
    }
    let start = j;
    while j < chars.len() && chars[j].is_ascii_alphabetic() {
        j += 1;
    }
    (j > start && chars.get(j) == Some(&':') && chars.get(j + 1) == Some(&']')).then_some(j + 1)
}

/// Compile a Go regexp pattern as a Unicode `regex::Regex`.
pub fn compile(pat: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&go_to_rust(pat))
}

/// Compile a Go regexp pattern as a `regex::bytes::Regex` (works on arbitrary bytes).
pub fn compile_bytes(pat: &str) -> Result<regex::bytes::Regex, regex::Error> {
    regex::bytes::Regex::new(&go_to_rust(pat))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(pat: &str) -> String {
        go_to_rust(pat).into_owned()
    }

    #[test]
    fn plain_patterns_pass_through_untouched() {
        for p in [
            "abc",
            "^a.*b$",
            "(?m)^#(.*)$",
            "a|b",
            "(?P<name>x+)?",
            "x{2,3}y{4}z{5,}",
        ] {
            assert_eq!(t(p), p);
        }
    }

    #[test]
    fn literal_braces_are_escaped() {
        assert_eq!(t("{{exclude_bots}}"), r"\{\{exclude_bots\}\}");
        assert_eq!(
            t(r"\((.*)\s+{{exclude_bots}}\)"),
            r"\((.*)(?-u:\s)+\{\{exclude_bots\}\}\)"
        );
        assert_eq!(t("a{,5}"), r"a\{,5\}");
        assert_eq!(t("a{x}"), r"a\{x\}");
        assert_eq!(t("a{2"), r"a\{2");
        assert_eq!(t("a{2}b{3,}c{4,5}"), "a{2}b{3,}c{4,5}");
        assert!(compile("[[project]] {{exclude_bots}}").is_ok());
    }

    #[test]
    fn perl_classes_become_ascii() {
        assert_eq!(t(r"\d+\s*\w"), r"(?-u:\d)+(?-u:\s)*(?-u:\w)");
        assert_eq!(t(r"[\d\s]"), r"[0-9\t\n\x0C\r ]");
        assert_eq!(t(r"[^\w-]"), r"[^0-9A-Za-z_-]");
        assert_eq!(t(r"[\W]"), r"[[^0-9A-Za-z_]]");
        let re = compile(r"^\w+$").unwrap();
        assert!(re.is_match("kubernetes_1"));
        assert!(!re.is_match("Łukasz")); // Go: \w is ASCII
        let re = compile(r"\bfoo\b").unwrap();
        assert!(re.is_match("a foo b"));
        assert!(re.is_match("éfoo")); // ASCII boundary: é is a non-word char in Go
        let re = compile(r"\d").unwrap();
        assert!(!re.is_match("٣")); // Arabic-Indic digit: not a Go \d
        let re = compile(r"[\d]").unwrap();
        assert!(re.is_match("7") && !re.is_match("٣"));
        // Negated classes outside brackets (real companies.yaml pattern)
        assert_eq!(t(r"\D\W\S"), r"[^0-9][^0-9A-Za-z_][^\t\n\x0C\r ]");
        let re = compile(r"(?i)^mastercard(\s*\S*)?$").unwrap();
        assert!(re.is_match("MasterCard Inc.") && re.is_match("mastercard Łódź"));
        assert!(!re.is_match("mastercard a b"));
        let re = compile(r"^\S+$").unwrap();
        assert!(re.is_match("Łukasz") && !re.is_match("a b") && !re.is_match("a\tb"));
        assert!(compile(r"^\D+\W+$").unwrap().is_match("ąę!!"));
    }

    #[test]
    fn other_escapes_and_classes_are_kept() {
        assert_eq!(t(r"\.\*\{\}\[\]\\"), r"\.\*\{\}\[\]\\");
        assert_eq!(t(r"\p{Greek}\pL\x{41}\n\t"), r"\p{Greek}\pL\x{41}\n\t");
        assert_eq!(t("[a-z0-9_.-]+"), "[a-z0-9_.-]+");
        assert_eq!(t("[[:alpha:]][[:^digit:]]"), "[[:alpha:]][[:^digit:]]");
        assert_eq!(t("[]a]"), r"[\]a]");
        assert_eq!(t("[^]a]"), r"[^\]a]");
    }

    #[test]
    fn class_metacharacters_that_are_literal_in_go_are_escaped() {
        assert_eq!(t("[[a]"), r"[\[a]");
        assert_eq!(t("[a&&b]"), r"[a\&\&b]");
        assert_eq!(t("[a~~b]"), r"[a\~\~b]");
        assert_eq!(t("[a&b]"), "[a&b]");
        let re = compile("[[a]+").unwrap();
        assert_eq!(re.find("x[[aa]y").unwrap().as_str(), "[[aa");
        let re = compile("[a&&b]+").unwrap();
        assert_eq!(re.find("x&&ab").unwrap().as_str(), "&&ab");
    }

    #[test]
    fn trailing_backslash_and_unterminated_class_are_left_to_the_compiler() {
        assert_eq!(t(r"abc\"), r"abc\");
        assert!(compile(r"abc\").is_err());
        assert_eq!(t("[abc"), "[abc");
        assert!(compile("[abc").is_err());
    }

    #[test]
    fn real_devstats_patterns_compile_and_match() {
        // devstats/devel/update_dashboards_labels.sh
        let re = compile(r"\((.*)\s+{{exclude_bots}}\)").unwrap();
        let s = "where (author {{exclude_bots}})";
        assert_eq!(
            re.replace_all(s, "(lower($1) {{exclude_bots}})"),
            "where (lower(author) {{exclude_bots}})"
        );
        // devstats/devel/cronctl.sh
        let re = compile(r"(?m)^#(.*\s+devstats_sync\s+.*)$").unwrap();
        assert_eq!(
            re.replace_all("#* * * * * devstats_sync x\n", "$1"),
            "* * * * * devstats_sync x\n"
        );
        // devstats: grafana uid stripping
        let re = compile(r#"(?m)^.*"uid": "\w+",\n"#).unwrap();
        assert_eq!(
            re.replace_all("{\n  \"uid\": \"abc_1\",\n  \"x\": 1\n}", "-"),
            "{\n-  \"x\": 1\n}"
        );
        // projects.yaml annotation regexps
        for p in [
            r"^v?\d+\.\d+\.\d+$",
            r"^(v\d+\.\d+\.0|\d+\.\d+\.0)$",
            r"^release-\d+\.\d+$",
        ] {
            let re = compile(p).unwrap();
            assert!(re.is_match("v1.2.0") || re.is_match("release-1.2"), "{p}");
        }
    }
}
