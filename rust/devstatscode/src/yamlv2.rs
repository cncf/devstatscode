//! Byte-exact port of the `gopkg.in/yaml.v2` **encoder** (`yaml.Marshal`).
//!
//! Several DevStats tools write YAML files that are committed to other
//! repositories (`splitcrons` rewrites `devstats-helm/values.yaml`), so the
//! Rust port has to produce the very same bytes yaml.v2 does, not merely
//! equivalent YAML. Generic Rust YAML crates differ from yaml.v2 in quoting
//! rules, indentation of sequences, flow style, line wrapping at 80 columns,
//! key ordering, ... — hence this dedicated emitter.
//!
//! What is reproduced (all of it verified against Go, see the unit tests):
//!
//! * yaml.v2 `stringv` — a string is emitted *plain* only when it would not be
//!   resolved to another type when read back (`"1"`, `"yes"`, `"null"`,
//!   `"1e3"`, `"2001-12-14"`, base-60 `"12:30"`, ... get double quotes);
//!   strings containing `\n` use the literal block style (`|`, `|-`, `|+`, `|2-`),
//! * libyaml `yaml_emitter_analyze_scalar` — plain style is refused for
//!   indicators (`- a`, `#x`, `a: b`, `[`, `'`, leading/trailing spaces, ...)
//!   and the emitter then falls back to single quotes, or double quotes when
//!   the value has tabs / non-printable / non-BMP characters (`\t`, `\U0001F600`),
//! * block sequences nested in mappings are *indentless* (`projects:\n- proj: x`),
//! * `flow` sequences / mappings (`[0, 1, 0, 0]`, `{a: b}`), empty ones (`[]`, `{}`),
//! * wrapping of long plain / quoted scalars and flow collections at column 80,
//! * long (> 128 bytes) or multi-line keys in `? key\n: value` form,
//! * yaml.v2 map key ordering ([`key_less`]: digit-aware natural sort),
//! * numbers as yaml.v2 prints them (`%g`-style floats, `.inf`, `.nan`).
//!
//! Values are described with the [`Node`] tree; [`marshal`] renders it. The
//! [`MapBuilder`] helper mirrors struct marshalling with `omitempty`.
//!
//! The matching **decoder** side (yaml.v2 `Unmarshal` scalar coercions for
//! serde structs) lives in [`de`]; [`dedup`] adds yaml.v2's tolerance of
//! duplicate keys and multi-document input to it.

use crate::gofmt;

pub mod de;
pub mod dedup;

/// A YAML value in yaml.v2 terms.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    /// Go `string` — quoting decided by yaml.v2 rules.
    Str(String),
    /// Go signed integers.
    Int(i64),
    /// Go unsigned integers.
    Uint(u64),
    /// Go `float64` (`strconv.FormatFloat(v, 'g', -1, 64)`, `.inf`/`-.inf`/`.nan`).
    Float(f64),
    /// Go `bool`.
    Bool(bool),
    /// Go nil pointer/interface → `null`.
    Null,
    /// Block sequence (`- item`).
    Seq(Vec<Node>),
    /// Flow sequence (`[a, b]`) — struct tag `,flow`.
    FlowSeq(Vec<Node>),
    /// Block mapping in the given key order (structs: field order; Go maps: sort with [`key_less`]).
    Map(Vec<(Node, Node)>),
    /// Flow mapping (`{a: b}`) — struct tag `,flow`.
    FlowMap(Vec<(Node, Node)>),
}

impl Node {
    /// Convenience constructor for [`Node::Str`].
    pub fn str(s: impl Into<String>) -> Node {
        Node::Str(s.into())
    }

    /// Convenience constructor for [`Node::Int`].
    pub fn int(v: impl Into<i64>) -> Node {
        Node::Int(v.into())
    }

    /// yaml.v2 `isZero` — decides `omitempty` omission.
    pub fn is_zero(&self) -> bool {
        match self {
            Node::Str(s) => s.is_empty(),
            Node::Int(v) => *v == 0,
            Node::Uint(v) => *v == 0,
            Node::Float(v) => *v == 0.0,
            Node::Bool(b) => !*b,
            Node::Null => true,
            Node::Seq(v) | Node::FlowSeq(v) => v.is_empty(),
            Node::Map(v) | Node::FlowMap(v) => v.is_empty(),
        }
    }

    /// Sequence built from Go strings.
    pub fn str_seq<I, S>(items: I) -> Node
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Node::Seq(items.into_iter().map(|s| Node::Str(s.into())).collect())
    }
}

/// Builder mirroring the marshalling of a Go struct: fields in declaration
/// order, optional `omitempty`.
#[derive(Debug, Default, Clone)]
pub struct MapBuilder {
    entries: Vec<(Node, Node)>,
}

impl MapBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Always emitted field.
    pub fn field(mut self, key: &str, value: Node) -> Self {
        self.entries.push((Node::str(key), value));
        self
    }

    /// `yaml:"key,omitempty"` field — skipped when the value is zero.
    pub fn field_omitempty(mut self, key: &str, value: Node) -> Self {
        if !value.is_zero() {
            self.entries.push((Node::str(key), value));
        }
        self
    }

    /// Finish as a block mapping.
    pub fn build(self) -> Node {
        Node::Map(self.entries)
    }

    /// Finish as a flow mapping.
    pub fn build_flow(self) -> Node {
        Node::FlowMap(self.entries)
    }
}

/// Build a block mapping from Go-map–like string keyed entries, in yaml.v2's
/// key order.
pub fn sorted_map<S: AsRef<str>>(mut entries: Vec<(S, Node)>) -> Node {
    // Go's `sort.Sort` is an insertion sort for up to 12 keys and pdqsort
    // above that; `key_less` is not a strict weak ordering for exotic key sets
    // (`a1a` < `a2` < `a10` < `a1a`), for which even Go's output depends on the
    // random map iteration order. Insertion sort (Go's own variant) is used for
    // any size: identical results for every consistently ordered key set.
    for i in 1..entries.len() {
        let mut j = i;
        while j > 0 && key_less(entries[j].0.as_ref(), entries[j - 1].0.as_ref()) {
            entries.swap(j, j - 1);
            j -= 1;
        }
    }
    Node::Map(
        entries
            .into_iter()
            .map(|(k, v)| (Node::str(k.as_ref()), v))
            .collect(),
    )
}

/// yaml.v2 `keyList.Less` for string keys: letters sort before non-letters,
/// runs of digits compare numerically (`a2` < `a10`, `x09y` after `x9y`).
pub fn key_less(a: &str, b: &str) -> bool {
    let ar: Vec<char> = a.chars().collect();
    let br: Vec<char> = b.chars().collect();
    let mut i = 0;
    while i < ar.len() && i < br.len() {
        if ar[i] == br[i] {
            i += 1;
            continue;
        }
        let al = ar[i].is_alphabetic();
        let bl = br[i].is_alphabetic();
        if al && bl {
            return ar[i] < br[i];
        }
        if al || bl {
            return bl;
        }
        let (mut an, mut bn): (i64, i64) = (0, 0);
        if ar[i] == '0' || br[i] == '0' {
            let mut j = i as isize - 1;
            while j >= 0 && ar[j as usize].is_numeric() {
                if ar[j as usize] != '0' {
                    an = 1;
                    bn = 1;
                    break;
                }
                j -= 1;
            }
        }
        let mut ai = i;
        while ai < ar.len() && ar[ai].is_numeric() {
            an = an.wrapping_mul(10).wrapping_add(ar[ai] as i64 - '0' as i64);
            ai += 1;
        }
        let mut bi = i;
        while bi < br.len() && br[bi].is_numeric() {
            bn = bn.wrapping_mul(10).wrapping_add(br[bi] as i64 - '0' as i64);
            bi += 1;
        }
        if an != bn {
            return an < bn;
        }
        if ai != bi {
            return ai < bi;
        }
        return ar[i] < br[i];
    }
    ar.len() < br.len()
}

// ---------------------------------------------------------------------------
// yaml.v2 resolve(): which tag would a plain scalar get when parsed back?
// ---------------------------------------------------------------------------

/// Tag yaml.v2 `resolve("", s)` assigns to the plain scalar `s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tag {
    Str,
    Bool,
    Null,
    Int,
    Float,
    Timestamp,
}

/// yaml.v2 `resolve("", s).tag`.
pub fn resolve(s: &str) -> Tag {
    // resolveTable hint from the first byte.
    let hint = match s.as_bytes().first() {
        None => b'N',
        Some(b'+') | Some(b'-') => b'S',
        Some(c) if c.is_ascii_digit() => b'D',
        Some(c) if b"yYnNtTfFoO~".contains(c) => b'M',
        Some(b'.') => b'.',
        Some(_) => 0,
    };
    if hint == 0 {
        return Tag::Str;
    }
    match s {
        "y" | "Y" | "yes" | "Yes" | "YES" | "true" | "True" | "TRUE" | "on" | "On" | "ON" | "n"
        | "N" | "no" | "No" | "NO" | "false" | "False" | "FALSE" | "off" | "Off" | "OFF" => {
            return Tag::Bool
        }
        "" | "~" | "null" | "Null" | "NULL" => return Tag::Null,
        ".nan" | ".NaN" | ".NAN" | ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF"
        | "-.inf" | "-.Inf" | "-.INF" => return Tag::Float,
        _ => {}
    }
    match hint {
        b'M' => Tag::Str,
        b'.' => {
            if go_parse_float_ok(s) {
                Tag::Float
            } else {
                Tag::Str
            }
        }
        b'D' | b'S' => {
            if is_timestamp(s) {
                return Tag::Timestamp;
            }
            let plain: String = s.chars().filter(|c| *c != '_').collect();
            if go_parse_int_base0(&plain).is_some() || go_parse_uint_base0(&plain).is_some() {
                return Tag::Int;
            }
            if yaml_style_float(&plain) && go_parse_float_ok(&plain) {
                return Tag::Float;
            }
            if let Some(rest) = plain.strip_prefix("0b") {
                if !rest.is_empty()
                    && rest.bytes().all(|b| b == b'0' || b == b'1')
                    && (i64::from_str_radix(rest, 2).is_ok()
                        || u64::from_str_radix(rest, 2).is_ok())
                {
                    return Tag::Int;
                }
            } else if let Some(rest) = plain.strip_prefix("-0b") {
                if !rest.is_empty()
                    && rest.bytes().all(|b| b == b'0' || b == b'1')
                    && i64::from_str_radix(&format!("-{rest}"), 2).is_ok()
                {
                    return Tag::Int;
                }
            }
            Tag::Str
        }
        _ => Tag::Str,
    }
}

/// yaml.v2 `yamlStyleFloat` regexp: `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`.
fn yaml_style_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        i += 1;
    }
    let digits = |b: &[u8], mut i: usize| -> usize {
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        i
    };
    if i < b.len() && b[i] == b'.' {
        let j = digits(b, i + 1);
        if j == i + 1 {
            return false;
        }
        i = j;
    } else {
        let j = digits(b, i);
        if j == i {
            return false;
        }
        i = j;
        if i < b.len() && b[i] == b'.' {
            i = digits(b, i + 1);
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
            i += 1;
        }
        let j = digits(b, i);
        if j == i {
            return false;
        }
        i = j;
    }
    i == b.len()
}

/// Go `strconv.ParseFloat(s, 64) == nil` (decimal syntax only, no
/// underscores, `inf`/`infinity`/`nan` accepted, out of range → error).
fn go_parse_float_ok(s: &str) -> bool {
    let body = s.strip_prefix(['+', '-']).unwrap_or(s);
    let lower = body.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" || lower == "nan" {
        return true;
    }
    if let Some(rest) = lower.strip_prefix("0x") {
        // hexadecimal floats: mantissa with optional '.', mandatory 'p' exponent
        let (mant, exp) = match rest.split_once('p') {
            Some(x) => x,
            None => return false,
        };
        let mant_ok = !mant.trim_matches('.').is_empty()
            && mant.matches('.').count() <= 1
            && mant.chars().all(|c| c.is_ascii_hexdigit() || c == '.');
        let exp_body = exp.strip_prefix(['+', '-']).unwrap_or(exp);
        return mant_ok && !exp_body.is_empty() && exp_body.bytes().all(|b| b.is_ascii_digit());
    }
    // decimal: digits [. digits] [e [+-] digits], at least one mantissa digit
    let b = lower.as_bytes();
    let mut i = 0;
    let mut nd = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        nd += 1;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            nd += 1;
        }
    }
    if nd == 0 {
        return false;
    }
    if i < b.len() && b[i] == b'e' {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    if i != b.len() {
        return false;
    }
    match body.parse::<f64>() {
        Ok(v) => v.is_finite(), // Go reports ErrRange for ±Inf results
        Err(_) => false,
    }
}

/// Go `strconv.ParseInt(s, 0, 64)`: optional sign, base prefix `0x`/`0o`/`0b`
/// or leading `0` (octal), underscores allowed only between digits (they were
/// already stripped by the caller, so any left is a syntax error).
fn go_parse_int_base0(s: &str) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let mag = parse_uint_base0_magnitude(body)?;
    if neg {
        if mag > (i64::MAX as u64) + 1 {
            return None;
        }
        Some((mag as i64).wrapping_neg())
    } else {
        i64::try_from(mag).ok()
    }
}

/// Go `strconv.ParseUint(s, 0, 64)`.
fn go_parse_uint_base0(s: &str) -> Option<u64> {
    parse_uint_base0_magnitude(s)
}

fn parse_uint_base0_magnitude(s: &str) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let lower = s.to_ascii_lowercase();
    let (radix, digits) = if let Some(r) = lower.strip_prefix("0x") {
        (16, r)
    } else if let Some(r) = lower.strip_prefix("0b") {
        (2, r)
    } else if let Some(r) = lower.strip_prefix("0o") {
        (8, r)
    } else if lower.len() > 1 && lower.starts_with('0') {
        (8, &lower[1..])
    } else {
        (10, lower.as_str())
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return None;
    }
    u64::from_str_radix(digits, radix).ok()
}

/// yaml.v2 `parseTimestamp`: `YYYY-M-D`, optionally `[Tt ]H:M:S[.frac]` and,
/// for the `T` forms, a `Z` / `±HH:MM` zone (Go `time.Parse` semantics).
fn is_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 5 || !b[..4].iter().all(|c| c.is_ascii_digit()) || b[4] != b'-' {
        return false;
    }
    let year: i32 = s[..4].parse().unwrap_or(0);
    // date part: 1-2 digit month and day
    let mut i = 5;
    let (month, ni) = match take_num(b, i, 1, 2) {
        Some(x) => x,
        None => return false,
    };
    i = ni;
    if i >= b.len() || b[i] != b'-' {
        return false;
    }
    let (day, ni) = match take_num(b, i + 1, 1, 2) {
        Some(x) => x,
        None => return false,
    };
    i = ni;
    if !(1..=12).contains(&month) || day < 1 || day > days_in(year, month) {
        return false;
    }
    if i == b.len() {
        return true;
    }
    let sep = b[i];
    if sep != b'T' && sep != b't' && sep != b' ' {
        return false;
    }
    i += 1;
    // Go `time.Parse`: a space in the layout matches a run of spaces
    while sep == b' ' && i < b.len() && b[i] == b' ' {
        i += 1;
    }
    let (hour, ni) = match take_num(b, i, 1, 2) {
        Some(x) => x,
        None => return false,
    };
    i = ni;
    if i >= b.len() || b[i] != b':' {
        return false;
    }
    let (min, ni) = match take_num(b, i + 1, 1, 2) {
        Some(x) => x,
        None => return false,
    };
    i = ni;
    if i >= b.len() || b[i] != b':' {
        return false;
    }
    let (sec, ni) = match take_num(b, i + 1, 1, 2) {
        Some(x) => x,
        None => return false,
    };
    i = ni;
    if hour > 23 || min > 59 || sec > 59 {
        return false;
    }
    // optional fractional seconds
    if i < b.len() && (b[i] == b'.' || b[i] == b',') {
        let start = i + 1;
        let mut j = start;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j == start {
            return false;
        }
        i = j;
    }
    if sep == b' ' {
        return i == b.len();
    }
    // zone: Z or ±HH:MM
    if i >= b.len() {
        return false;
    }
    if b[i] == b'Z' {
        return i + 1 == b.len();
    }
    if b[i] != b'+' && b[i] != b'-' {
        return false;
    }
    let rest = &b[i + 1..];
    if rest.len() != 5 || rest[2] != b':' {
        return false;
    }
    if !rest[..2].iter().all(|c| c.is_ascii_digit())
        || !rest[3..].iter().all(|c| c.is_ascii_digit())
    {
        return false;
    }
    let hh = (rest[0] - b'0') as i32 * 10 + (rest[1] - b'0') as i32;
    let mm = (rest[3] - b'0') as i32 * 10 + (rest[4] - b'0') as i32;
    // Go accepts offsets of exactly 24 hours / 60 minutes (`>` range checks)
    hh <= 24 && mm <= 60
}

fn take_num(b: &[u8], start: usize, min_digits: usize, max_digits: usize) -> Option<(i32, usize)> {
    let mut i = start;
    while i < b.len() && i - start < max_digits && b[i].is_ascii_digit() {
        i += 1;
    }
    if i - start < min_digits {
        return None;
    }
    let v = std::str::from_utf8(&b[start..i]).ok()?.parse().ok()?;
    Some((v, i))
}

fn days_in(year: i32, month: i32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// yaml.v2 `isBase60Float`: `^[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+(?:\.[0-9_]*)?$`.
pub fn is_base60_float(s: &str) -> bool {
    let b = s.as_bytes();
    if b.is_empty() {
        return false;
    }
    let mut i = 0;
    if b[i] == b'+' || b[i] == b'-' {
        i += 1;
    }
    if i >= b.len() || !b[i].is_ascii_digit() {
        return false;
    }
    i += 1;
    while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
        i += 1;
    }
    let mut groups = 0;
    while i < b.len() && b[i] == b':' {
        let mut j = i + 1;
        if j < b.len() && (b'0'..=b'5').contains(&b[j]) {
            if j + 1 < b.len() && b[j + 1].is_ascii_digit() {
                j += 2;
            } else {
                j += 1;
            }
        } else if j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        } else {
            return false;
        }
        groups += 1;
        i = j;
    }
    if groups == 0 {
        return false;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
            i += 1;
        }
    }
    i == b.len()
}

// ---------------------------------------------------------------------------
// libyaml emitter (the subset yaml.v2 exercises)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Style {
    Plain,
    SingleQuoted,
    DoubleQuoted,
    Literal,
}

#[derive(Debug, Default, Clone, Copy)]
struct ScalarData {
    multiline: bool,
    flow_plain_allowed: bool,
    block_plain_allowed: bool,
    single_quoted_allowed: bool,
    block_allowed: bool,
}

struct Emitter {
    out: Vec<u8>,
    column: i32,
    indent: i32,
    indents: Vec<i32>,
    whitespace: bool,
    indention: bool,
    open_ended: bool,
    flow_level: i32,
    best_indent: i32,
    best_width: i32,
    root_context: bool,
    mapping_context: bool,
    simple_key_context: bool,
    sd: ScalarData,
}

// --- byte classification helpers (yamlprivateh.go) ---

fn width(b: u8) -> usize {
    if b & 0x80 == 0x00 {
        1
    } else if b & 0xE0 == 0xC0 {
        2
    } else if b & 0xF0 == 0xE0 {
        3
    } else if b & 0xF8 == 0xF0 {
        4
    } else {
        0
    }
}

fn at(b: &[u8], i: usize) -> u8 {
    b.get(i).copied().unwrap_or(0)
}

fn is_space(b: &[u8], i: usize) -> bool {
    at(b, i) == b' '
}

fn is_blank(b: &[u8], i: usize) -> bool {
    matches!(at(b, i), b' ' | b'\t')
}

fn is_break(b: &[u8], i: usize) -> bool {
    let c = at(b, i);
    c == b'\r'
        || c == b'\n'
        || (c == 0xC2 && at(b, i + 1) == 0x85)
        || (c == 0xE2 && at(b, i + 1) == 0x80 && at(b, i + 2) == 0xA8)
        || (c == 0xE2 && at(b, i + 1) == 0x80 && at(b, i + 2) == 0xA9)
}

fn is_z(b: &[u8], i: usize) -> bool {
    at(b, i) == 0x00
}

fn is_blankz(b: &[u8], i: usize) -> bool {
    is_blank(b, i) || is_break(b, i) || is_z(b, i)
}

/// libyaml quirk preserved: checks the *start* of the value, not position `i`.
fn is_bom(b: &[u8], _i: usize) -> bool {
    at(b, 0) == 0xEF && at(b, 1) == 0xBB && at(b, 2) == 0xBF
}

fn is_printable(b: &[u8], i: usize) -> bool {
    let c = at(b, i);
    c == 0x0A
        || (0x20..=0x7E).contains(&c)
        || (c == 0xC2 && at(b, i + 1) >= 0xA0)
        || (c > 0xC2 && c < 0xED)
        || (c == 0xED && at(b, i + 1) < 0xA0)
        || c == 0xEE
        || (c == 0xEF
            && !(at(b, i + 1) == 0xBB && at(b, i + 2) == 0xBF)
            && !(at(b, i + 1) == 0xBF && (at(b, i + 2) == 0xBE || at(b, i + 2) == 0xBF)))
}

impl Emitter {
    fn new() -> Self {
        Emitter {
            out: Vec::new(),
            column: 0,
            indent: -1,
            indents: Vec::new(),
            whitespace: true,
            indention: true,
            open_ended: false,
            flow_level: 0,
            best_indent: 2,
            best_width: 80,
            root_context: false,
            mapping_context: false,
            simple_key_context: false,
            sd: ScalarData::default(),
        }
    }

    // --- low level writers ---

    fn put(&mut self, c: u8) {
        self.out.push(c);
        self.column += 1;
    }

    fn put_break(&mut self) {
        self.out.push(b'\n');
        self.column = 0;
    }

    /// Copy one (UTF-8) character at `*i`.
    fn write_char(&mut self, b: &[u8], i: &mut usize) {
        let w = width(b[*i]).max(1);
        self.out.extend_from_slice(&b[*i..(*i + w).min(b.len())]);
        self.column += 1;
        *i += w;
    }

    fn write_break(&mut self, b: &[u8], i: &mut usize) {
        if b[*i] == b'\n' {
            self.put_break();
            *i += 1;
        } else {
            let w = width(b[*i]).max(1);
            self.out.extend_from_slice(&b[*i..(*i + w).min(b.len())]);
            self.column = 0;
            *i += w;
        }
    }

    fn write_indent(&mut self) {
        let indent = self.indent.max(0);
        if !self.indention || self.column > indent || (self.column == indent && !self.whitespace) {
            self.put_break();
        }
        while self.column < indent {
            self.put(b' ');
        }
        self.whitespace = true;
        self.indention = true;
    }

    fn write_indicator(
        &mut self,
        indicator: &[u8],
        need_whitespace: bool,
        is_whitespace: bool,
        is_indention: bool,
    ) {
        if need_whitespace && !self.whitespace {
            self.put(b' ');
        }
        self.out.extend_from_slice(indicator);
        self.column += indicator.len() as i32;
        self.whitespace = is_whitespace;
        self.indention = self.indention && is_indention;
        self.open_ended = false;
    }

    fn increase_indent(&mut self, flow: bool, indentless: bool) {
        self.indents.push(self.indent);
        if self.indent < 0 {
            self.indent = if flow { self.best_indent } else { 0 };
        } else if !indentless {
            self.indent += self.best_indent;
        }
    }

    fn pop_indent(&mut self) {
        self.indent = self.indents.pop().unwrap_or(-1);
    }

    // --- scalar analysis / style selection ---

    fn analyze_scalar(&mut self, value: &[u8]) {
        let sd = &mut self.sd;
        if value.is_empty() {
            sd.multiline = false;
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = true;
            sd.single_quoted_allowed = true;
            sd.block_allowed = false;
            return;
        }
        let mut block_indicators = false;
        let mut flow_indicators = false;
        let mut line_breaks = false;
        let mut special_characters = false;
        let mut leading_space = false;
        let mut leading_break = false;
        let mut trailing_space = false;
        let mut trailing_break = false;
        let mut break_space = false;
        let mut space_break = false;
        let mut previous_space = false;
        let mut previous_break = false;

        if value.len() >= 3
            && ((value[0] == b'-' && value[1] == b'-' && value[2] == b'-')
                || (value[0] == b'.' && value[1] == b'.' && value[2] == b'.'))
        {
            block_indicators = true;
            flow_indicators = true;
        }
        let mut preceded_by_whitespace = true;
        let mut i = 0;
        while i < value.len() {
            let w = width(value[i]).max(1);
            let followed_by_whitespace = i + w >= value.len() || is_blank(value, i + w);
            if i == 0 {
                match value[i] {
                    b'#' | b',' | b'[' | b']' | b'{' | b'}' | b'&' | b'*' | b'!' | b'|' | b'>'
                    | b'\'' | b'"' | b'%' | b'@' | b'`' => {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                    b'?' | b':' => {
                        flow_indicators = true;
                        if followed_by_whitespace {
                            block_indicators = true;
                        }
                    }
                    b'-' if followed_by_whitespace => {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                    _ => {}
                }
            } else {
                match value[i] {
                    b',' | b'?' | b'[' | b']' | b'{' | b'}' => flow_indicators = true,
                    b':' => {
                        flow_indicators = true;
                        if followed_by_whitespace {
                            block_indicators = true;
                        }
                    }
                    b'#' if preceded_by_whitespace => {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                    _ => {}
                }
            }
            if !is_printable(value, i) {
                special_characters = true;
            }
            if is_space(value, i) {
                if i == 0 {
                    leading_space = true;
                }
                if i + w == value.len() {
                    trailing_space = true;
                }
                if previous_break {
                    break_space = true;
                }
                previous_space = true;
                previous_break = false;
            } else if is_break(value, i) {
                line_breaks = true;
                if i == 0 {
                    leading_break = true;
                }
                if i + w == value.len() {
                    trailing_break = true;
                }
                if previous_space {
                    space_break = true;
                }
                previous_space = false;
                previous_break = true;
            } else {
                previous_space = false;
                previous_break = false;
            }
            preceded_by_whitespace = is_blankz(value, i);
            i += w;
        }
        sd.multiline = line_breaks;
        sd.flow_plain_allowed = true;
        sd.block_plain_allowed = true;
        sd.single_quoted_allowed = true;
        sd.block_allowed = true;
        if leading_space || leading_break || trailing_space || trailing_break {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
        }
        if trailing_space {
            sd.block_allowed = false;
        }
        if break_space {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
            sd.single_quoted_allowed = false;
        }
        if space_break || special_characters {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
            sd.single_quoted_allowed = false;
            sd.block_allowed = false;
        }
        if line_breaks {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
        }
        if flow_indicators {
            sd.flow_plain_allowed = false;
        }
        if block_indicators {
            sd.block_plain_allowed = false;
        }
    }

    fn select_scalar_style(&self, value: &[u8], requested: Style) -> Style {
        let mut style = requested;
        if self.simple_key_context && self.sd.multiline {
            style = Style::DoubleQuoted;
        }
        if style == Style::Plain {
            if (self.flow_level > 0 && !self.sd.flow_plain_allowed)
                || (self.flow_level == 0 && !self.sd.block_plain_allowed)
            {
                style = Style::SingleQuoted;
            }
            if value.is_empty() && (self.flow_level > 0 || self.simple_key_context) {
                style = Style::SingleQuoted;
            }
        }
        if style == Style::SingleQuoted && !self.sd.single_quoted_allowed {
            style = Style::DoubleQuoted;
        }
        if style == Style::Literal
            && (!self.sd.block_allowed || self.flow_level > 0 || self.simple_key_context)
        {
            style = Style::DoubleQuoted;
        }
        style
    }

    // --- scalar writers ---

    fn write_plain_scalar(&mut self, value: &[u8], allow_breaks: bool) {
        if !self.whitespace {
            self.put(b' ');
        }
        let mut spaces = false;
        let mut breaks = false;
        let mut i = 0;
        while i < value.len() {
            if is_space(value, i) {
                if allow_breaks
                    && !spaces
                    && self.column > self.best_width
                    && !is_space(value, i + 1)
                {
                    self.write_indent();
                    i += width(value[i]).max(1);
                } else {
                    self.write_char(value, &mut i);
                }
                spaces = true;
            } else if is_break(value, i) {
                if !breaks && value[i] == b'\n' {
                    self.put_break();
                }
                self.write_break(value, &mut i);
                self.indention = true;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                self.write_char(value, &mut i);
                self.indention = false;
                spaces = false;
                breaks = false;
            }
        }
        self.whitespace = false;
        self.indention = false;
        if self.root_context {
            self.open_ended = true;
        }
    }

    fn write_single_quoted_scalar(&mut self, value: &[u8], allow_breaks: bool) {
        self.write_indicator(b"'", true, false, false);
        let mut spaces = false;
        let mut breaks = false;
        let mut i = 0;
        while i < value.len() {
            if is_space(value, i) {
                if allow_breaks
                    && !spaces
                    && self.column > self.best_width
                    && i > 0
                    && i < value.len() - 1
                    && !is_space(value, i + 1)
                {
                    self.write_indent();
                    i += width(value[i]).max(1);
                } else {
                    self.write_char(value, &mut i);
                }
                spaces = true;
            } else if is_break(value, i) {
                if !breaks && value[i] == b'\n' {
                    self.put_break();
                }
                self.write_break(value, &mut i);
                self.indention = true;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                if value[i] == b'\'' {
                    self.put(b'\'');
                }
                self.write_char(value, &mut i);
                self.indention = false;
                spaces = false;
                breaks = false;
            }
        }
        self.write_indicator(b"'", false, false, false);
        self.whitespace = false;
        self.indention = false;
    }

    fn write_double_quoted_scalar(&mut self, value: &[u8], allow_breaks: bool) {
        self.write_indicator(b"\"", true, false, false);
        let mut spaces = false;
        let mut i = 0;
        while i < value.len() {
            if !is_printable(value, i)
                || is_bom(value, i)
                || is_break(value, i)
                || value[i] == b'"'
                || value[i] == b'\\'
            {
                let octet = value[i];
                let (mut w, mut v): (usize, u32) = match octet {
                    o if o & 0x80 == 0x00 => (1, (o & 0x7F) as u32),
                    o if o & 0xE0 == 0xC0 => (2, (o & 0x1F) as u32),
                    o if o & 0xF0 == 0xE0 => (3, (o & 0x0F) as u32),
                    o if o & 0xF8 == 0xF0 => (4, (o & 0x07) as u32),
                    _ => (1, 0),
                };
                for k in 1..w {
                    v = (v << 6) + (at(value, i + k) & 0x3F) as u32;
                }
                i += w;
                self.put(b'\\');
                match v {
                    0x00 => self.put(b'0'),
                    0x07 => self.put(b'a'),
                    0x08 => self.put(b'b'),
                    0x09 => self.put(b't'),
                    0x0A => self.put(b'n'),
                    0x0B => self.put(b'v'),
                    0x0C => self.put(b'f'),
                    0x0D => self.put(b'r'),
                    0x1B => self.put(b'e'),
                    0x22 => self.put(b'"'),
                    0x5C => self.put(b'\\'),
                    0x85 => self.put(b'N'),
                    0xA0 => self.put(b'_'),
                    0x2028 => self.put(b'L'),
                    0x2029 => self.put(b'P'),
                    _ => {
                        if v <= 0xFF {
                            self.put(b'x');
                            w = 2;
                        } else if v <= 0xFFFF {
                            self.put(b'u');
                            w = 4;
                        } else {
                            self.put(b'U');
                            w = 8;
                        }
                        let mut k = (w as i32 - 1) * 4;
                        while k >= 0 {
                            let digit = ((v >> k) & 0x0F) as u8;
                            self.put(if digit < 10 {
                                digit + b'0'
                            } else {
                                digit + b'A' - 10
                            });
                            k -= 4;
                        }
                    }
                }
                spaces = false;
            } else if is_space(value, i) {
                if allow_breaks
                    && !spaces
                    && self.column > self.best_width
                    && i > 0
                    && i < value.len() - 1
                {
                    self.write_indent();
                    if is_space(value, i + 1) {
                        self.put(b'\\');
                    }
                    i += width(value[i]).max(1);
                } else {
                    self.write_char(value, &mut i);
                }
                spaces = true;
            } else {
                self.write_char(value, &mut i);
                spaces = false;
            }
        }
        self.write_indicator(b"\"", false, false, false);
        self.whitespace = false;
        self.indention = false;
    }

    fn write_block_scalar_hints(&mut self, value: &[u8]) {
        if is_space(value, 0) || is_break(value, 0) {
            let hint = [b'0' + self.best_indent as u8];
            self.write_indicator(&hint, false, false, false);
        }
        self.open_ended = false;
        let mut chomp_hint: u8 = 0;
        if value.is_empty() {
            chomp_hint = b'-';
        } else {
            let mut i = value.len() - 1;
            while value[i] & 0xC0 == 0x80 {
                i -= 1;
            }
            if !is_break(value, i) {
                chomp_hint = b'-';
            } else if i == 0 {
                chomp_hint = b'+';
                self.open_ended = true;
            } else {
                i -= 1;
                while value[i] & 0xC0 == 0x80 {
                    i -= 1;
                }
                if is_break(value, i) {
                    chomp_hint = b'+';
                    self.open_ended = true;
                }
            }
        }
        if chomp_hint != 0 {
            self.write_indicator(&[chomp_hint], false, false, false);
        }
    }

    fn write_literal_scalar(&mut self, value: &[u8]) {
        self.write_indicator(b"|", true, false, false);
        self.write_block_scalar_hints(value);
        self.put_break();
        self.indention = true;
        self.whitespace = true;
        let mut breaks = true;
        let mut i = 0;
        while i < value.len() {
            if is_break(value, i) {
                self.write_break(value, &mut i);
                self.indention = true;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                self.write_char(value, &mut i);
                self.indention = false;
                breaks = false;
            }
        }
    }

    // --- node emission (state machine flattened over the tree) ---

    fn scalar_repr(node: &Node) -> Option<(String, Style)> {
        match node {
            Node::Str(s) => {
                let can_use_plain = resolve(s) == Tag::Str && !is_base60_float(s);
                let style = if s.contains('\n') {
                    Style::Literal
                } else if can_use_plain {
                    Style::Plain
                } else {
                    Style::DoubleQuoted
                };
                Some((s.clone(), style))
            }
            Node::Int(v) => Some((v.to_string(), Style::Plain)),
            Node::Uint(v) => Some((v.to_string(), Style::Plain)),
            Node::Float(v) => {
                let s = if v.is_nan() {
                    ".nan".to_string()
                } else if v.is_infinite() {
                    if *v > 0.0 {
                        ".inf".to_string()
                    } else {
                        "-.inf".to_string()
                    }
                } else {
                    gofmt::float(*v)
                };
                Some((s, Style::Plain))
            }
            Node::Bool(b) => Some((if *b { "true" } else { "false" }.to_string(), Style::Plain)),
            Node::Null => Some(("null".to_string(), Style::Plain)),
            _ => None,
        }
    }

    fn check_simple_key(&mut self, node: &Node) -> bool {
        match node {
            Node::Seq(v) | Node::FlowSeq(v) => v.is_empty(),
            Node::Map(v) | Node::FlowMap(v) => v.is_empty(),
            _ => {
                let (value, _) = Self::scalar_repr(node).expect("scalar");
                self.analyze_scalar(value.as_bytes());
                !self.sd.multiline && value.len() <= 128
            }
        }
    }

    fn emit_node(&mut self, node: &Node, root: bool, mapping: bool, simple_key: bool) {
        self.root_context = root;
        self.mapping_context = mapping;
        self.simple_key_context = simple_key;
        match node {
            Node::Seq(items) => self.emit_sequence(items, false),
            Node::FlowSeq(items) => self.emit_sequence(items, true),
            Node::Map(entries) => self.emit_mapping(entries, false),
            Node::FlowMap(entries) => self.emit_mapping(entries, true),
            _ => {
                let (value, style) = Self::scalar_repr(node).expect("scalar");
                self.emit_scalar(value.as_bytes(), style);
            }
        }
    }

    fn emit_scalar(&mut self, value: &[u8], requested: Style) {
        self.analyze_scalar(value);
        let style = self.select_scalar_style(value, requested);
        self.increase_indent(true, false);
        let allow_breaks = !self.simple_key_context;
        match style {
            Style::Plain => self.write_plain_scalar(value, allow_breaks),
            Style::SingleQuoted => self.write_single_quoted_scalar(value, allow_breaks),
            Style::DoubleQuoted => self.write_double_quoted_scalar(value, allow_breaks),
            Style::Literal => self.write_literal_scalar(value),
        }
        self.pop_indent();
    }

    fn emit_sequence(&mut self, items: &[Node], flow: bool) {
        if self.flow_level > 0 || flow || items.is_empty() {
            self.write_indicator(b"[", true, true, false);
            self.increase_indent(true, false);
            self.flow_level += 1;
            for (n, item) in items.iter().enumerate() {
                if n > 0 {
                    self.write_indicator(b",", false, false, false);
                }
                if self.column > self.best_width {
                    self.write_indent();
                }
                self.emit_node(item, false, false, false);
            }
            self.flow_level -= 1;
            self.pop_indent();
            self.write_indicator(b"]", false, false, false);
        } else {
            let indentless = self.mapping_context && !self.indention;
            self.increase_indent(false, indentless);
            for item in items {
                self.write_indent();
                self.write_indicator(b"-", true, false, true);
                self.emit_node(item, false, false, false);
            }
            self.pop_indent();
        }
    }

    fn emit_mapping(&mut self, entries: &[(Node, Node)], flow: bool) {
        if self.flow_level > 0 || flow || entries.is_empty() {
            self.write_indicator(b"{", true, true, false);
            self.increase_indent(true, false);
            self.flow_level += 1;
            for (n, (k, v)) in entries.iter().enumerate() {
                if n > 0 {
                    self.write_indicator(b",", false, false, false);
                }
                if self.column > self.best_width {
                    self.write_indent();
                }
                if self.check_simple_key(k) {
                    self.emit_node(k, false, true, true);
                    self.write_indicator(b":", false, false, false);
                    self.emit_node(v, false, true, false);
                } else {
                    self.write_indicator(b"?", true, false, false);
                    self.emit_node(k, false, true, false);
                    if self.column > self.best_width {
                        self.write_indent();
                    }
                    self.write_indicator(b":", true, false, false);
                    self.emit_node(v, false, true, false);
                }
            }
            self.flow_level -= 1;
            self.pop_indent();
            self.write_indicator(b"}", false, false, false);
        } else {
            self.increase_indent(false, false);
            for (k, v) in entries {
                self.write_indent();
                if self.check_simple_key(k) {
                    self.emit_node(k, false, true, true);
                    self.write_indicator(b":", false, false, false);
                    self.emit_node(v, false, true, false);
                } else {
                    self.write_indicator(b"?", true, false, true);
                    self.emit_node(k, false, true, false);
                    self.write_indent();
                    self.write_indicator(b":", true, false, true);
                    self.emit_node(v, false, true, false);
                }
            }
            self.pop_indent();
        }
    }

    fn document(mut self, root: &Node) -> Vec<u8> {
        // stream start: nothing written; implicit document start: nothing written
        self.emit_node(root, true, false, false);
        // implicit document end; yaml.v2 resets `open_ended` before the stream
        // end, so a plain root scalar never gets the `...` terminator
        self.write_indent();
        self.out
    }
}

/// yaml.v2 `yaml.Marshal` of the value described by `node`.
pub fn marshal(node: &Node) -> Vec<u8> {
    Emitter::new().document(node)
}

/// [`marshal`] as a `String` (the output is always valid UTF-8).
pub fn marshal_string(node: &Node) -> String {
    String::from_utf8(marshal(node)).expect("yaml is utf-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(s: &str) -> String {
        marshal_string(&Node::Map(vec![(Node::str("k"), Node::str(s))]))
    }

    #[test]
    fn resolve_tags() {
        assert_eq!(resolve("a"), Tag::Str);
        assert_eq!(resolve(""), Tag::Null);
        assert_eq!(resolve("~"), Tag::Null);
        assert_eq!(resolve("yes"), Tag::Bool);
        assert_eq!(resolve("OFF"), Tag::Bool);
        assert_eq!(resolve("y"), Tag::Bool);
        assert_eq!(resolve("1"), Tag::Int);
        assert_eq!(resolve("010"), Tag::Int);
        assert_eq!(resolve("08"), Tag::Float);
        assert_eq!(resolve("0x1F"), Tag::Int);
        assert_eq!(resolve("1_000"), Tag::Int);
        assert_eq!(resolve("+5"), Tag::Int);
        assert_eq!(resolve("1.0"), Tag::Float);
        assert_eq!(resolve("1e3"), Tag::Float);
        assert_eq!(resolve(".5"), Tag::Float);
        assert_eq!(resolve(".e5"), Tag::Str);
        assert_eq!(resolve(".inf"), Tag::Float);
        assert_eq!(resolve("1e400"), Tag::Str);
        assert_eq!(resolve("1.5.6"), Tag::Str);
        assert_eq!(resolve("2001-12-14"), Tag::Timestamp);
        assert_eq!(resolve("2020-02-30"), Tag::Str);
        assert_eq!(resolve("2001-12-14t21:59:43.10-05:00"), Tag::Timestamp);
        assert_eq!(resolve("<<"), Tag::Str); // '<' has no resolve hint → never looked up
        assert_eq!(resolve("12:30"), Tag::Str); // quoted through is_base60_float instead
        assert!(is_base60_float("12:30"));
        assert!(is_base60_float("190:20:30.15"));
        assert!(!is_base60_float("1:60"));
        assert!(!is_base60_float("a:1"));
    }

    #[test]
    fn scalar_styles() {
        assert_eq!(kv(""), "k: \"\"\n");
        assert_eq!(kv(" "), "k: ' '\n");
        assert_eq!(kv("a"), "k: a\n");
        assert_eq!(kv("1"), "k: \"1\"\n");
        assert_eq!(kv("yes"), "k: \"yes\"\n");
        assert_eq!(kv("-"), "k: '-'\n");
        assert_eq!(kv("- a"), "k: '- a'\n");
        assert_eq!(kv("-a"), "k: -a\n");
        assert_eq!(kv("a #x"), "k: 'a #x'\n");
        assert_eq!(kv("a# x"), "k: a# x\n");
        assert_eq!(kv("a: b"), "k: 'a: b'\n");
        assert_eq!(kv("a:b"), "k: a:b\n");
        assert_eq!(kv("a:"), "k: 'a:'\n");
        assert_eq!(kv("'"), "k: ''''\n");
        assert_eq!(kv("---"), "k: '---'\n");
        assert_eq!(kv("a\tb"), "k: \"a\\tb\"\n");
        assert_eq!(kv("😀"), "k: \"\\U0001F600\"\n");
        assert_eq!(kv("ż"), "k: ż\n");
        assert_eq!(kv("12:30"), "k: \"12:30\"\n");
        assert_eq!(kv("1:60"), "k: 1:60\n");
        assert_eq!(kv("2001-12-14"), "k: \"2001-12-14\"\n");
        assert_eq!(kv("line1\nline2\n"), "k: |\n  line1\n  line2\n");
        assert_eq!(kv("line1\nline2"), "k: |-\n  line1\n  line2\n");
        assert_eq!(kv("\nx"), "k: |2-\n\n  x\n");
        assert_eq!(kv(" a\nb"), "k: |2-\n   a\n  b\n");
        assert_eq!(kv("a\n\n"), "k: |+\n  a\n\n");
    }

    #[test]
    fn line_wrapping() {
        let long = format!("{}end", "word ".repeat(30));
        assert_eq!(
            marshal_string(&Node::Map(vec![
                (Node::str("k"), Node::str(&long)),
                (Node::str("kk"), Node::str(format!("a: b {long}"))),
            ])),
            "k: word word word word word word word word word word word word word word word word\n  word word word word word word word word word word word word word word end\nkk: 'a: b word word word word word word word word word word word word word word word\n  word word word word word word word word word word word word word word word end'\n"
        );
        assert_eq!(
            kv(&format!("\t{long}")),
            "k: \"\\tword word word word word word word word word word word word word word word word\n  word word word word word word word word word word word word word word end\"\n"
        );
    }

    #[test]
    fn collections() {
        let nested = Node::Map(vec![(
            Node::str("a"),
            Node::Seq(vec![
                Node::str_seq(["x", "y"]),
                Node::Map(vec![
                    (Node::str("e"), Node::Seq(vec![])),
                    (Node::str("em"), Node::Map(vec![])),
                    (Node::str("k"), Node::str("v")),
                ]),
            ]),
        )]);
        assert_eq!(
            marshal_string(&nested),
            "a:\n- - x\n  - \"y\"\n- e: []\n  em: {}\n  k: v\n"
        );
        let st = MapBuilder::new()
            .field("s", Node::str("hello world"))
            .field_omitempty("o", Node::str(""))
            .field("i", Node::Int(3))
            .field_omitempty("b", Node::Bool(false))
            .field("f", Node::Float(1.5))
            .field(
                "a",
                Node::FlowSeq(vec![Node::Int(1), Node::Int(2), Node::Int(3), Node::Int(4)]),
            )
            .field("l", Node::str_seq(["a", "b c"]))
            .field("fl", Node::FlowSeq(vec![Node::str("x"), Node::str("y z")]))
            .field(
                "m",
                sorted_map(vec![("z", Node::Int(1)), ("a", Node::Int(2))]),
            )
            .build();
        assert_eq!(
            marshal_string(&st),
            "s: hello world\ni: 3\nf: 1.5\na: [1, 2, 3, 4]\nl:\n- a\n- b c\nfl: [x, y z]\nm:\n  a: 2\n  z: 1\n"
        );
        let flow = Node::Map(vec![
            (
                Node::str("x"),
                Node::FlowSeq(vec![
                    Node::FlowSeq(vec![Node::Int(1), Node::Int(2)]),
                    Node::FlowSeq(vec![Node::Int(3)]),
                ]),
            ),
            (
                Node::str("y"),
                Node::FlowSeq(vec![
                    Node::Map(vec![
                        (Node::str("a"), Node::str("b")),
                        (Node::str("c"), Node::str("d e")),
                    ]),
                    Node::Map(vec![]),
                ]),
            ),
            (
                Node::str("z"),
                Node::FlowSeq(
                    ["", "a b", "x:y", "1", "k: v", "#", "a,b", "a]", "[x]"]
                        .iter()
                        .map(|s| Node::str(*s))
                        .collect(),
                ),
            ),
        ]);
        assert_eq!(
            marshal_string(&flow),
            "x: [[1, 2], [3]]\n\"y\": [{a: b, c: d e}, {}]\nz: [\"\", a b, 'x:y', \"1\", 'k: v', '#', 'a,b', 'a]', '[x]']\n"
        );
        let long_flow = Node::Map(vec![(
            Node::str("z"),
            Node::FlowSeq("abcdefghij ".repeat(12).split(' ').map(Node::str).collect()),
        )]);
        assert_eq!(
            marshal_string(&long_flow),
            "z: [abcdefghij, abcdefghij, abcdefghij, abcdefghij, abcdefghij, abcdefghij, abcdefghij,\n  abcdefghij, abcdefghij, abcdefghij, abcdefghij, abcdefghij, \"\"]\n"
        );
        let multi_seq = Node::str_seq(["a\nb", "c\n"]);
        assert_eq!(marshal_string(&multi_seq), "- |-\n  a\n  b\n- |\n  c\n");
    }

    #[test]
    fn keys_and_roots() {
        assert_eq!(
            marshal_string(&Node::Map(vec![(Node::str(""), Node::str("v"))])),
            "\"\": v\n"
        );
        let long_k = "k".repeat(129);
        let ok_k = "j".repeat(128);
        assert_eq!(
            marshal_string(&Node::Map(vec![
                (Node::str(&ok_k), Node::str("w")),
                (Node::str(&long_k), Node::str("v")),
                (Node::str("multi\nkey"), Node::str("x")),
            ])),
            format!("{ok_k}: w\n? {long_k}\n: v\n? |-\n  multi\n  key\n: x\n")
        );
        assert_eq!(marshal_string(&Node::str("foo")), "foo\n");
        assert_eq!(marshal_string(&Node::Int(42)), "42\n");
        assert_eq!(marshal_string(&Node::str("")), "\"\"\n");
        assert_eq!(marshal_string(&Node::str("a\nb")), "|-\n  a\n  b\n");
        assert_eq!(marshal_string(&Node::str("1")), "\"1\"\n");
        let nulls = Node::Map(vec![
            (Node::str("a"), Node::Null),
            (Node::str("b"), Node::Seq(vec![])),
            (Node::str("c"), Node::Map(vec![])),
        ]);
        assert_eq!(marshal_string(&nulls), "a: null\nb: []\nc: {}\n");
    }

    #[test]
    fn floats() {
        let m = sorted_map(vec![
            ("a", Node::Float(1.0)),
            ("b", Node::Float(0.1)),
            ("c", Node::Float(1e21)),
            ("d", Node::Float(1e-7)),
            ("e", Node::Float(100000.0)),
            ("f", Node::Float(1234567.0)),
            ("g", Node::Float(1e20)),
            ("h", Node::Float(123456789.0)),
            ("i", Node::Float(0.000001)),
        ]);
        assert_eq!(
            marshal_string(&m),
            "a: 1\nb: 0.1\nc: 1e+21\nd: 1e-07\ne: 100000\nf: 1.234567e+06\ng: 1e+20\nh: 1.23456789e+08\ni: 1e-06\n"
        );
        let sp = sorted_map(vec![
            ("a", Node::Float(f64::INFINITY)),
            ("b", Node::Float(f64::NEG_INFINITY)),
            ("c", Node::Float(f64::NAN)),
            ("d", Node::Float(-0.0)),
        ]);
        assert_eq!(marshal_string(&sp), "a: .inf\nb: -.inf\nc: .nan\nd: -0\n");
        assert_eq!(
            marshal_string(&Node::Map(vec![(Node::str("a"), Node::Uint(u64::MAX))])),
            "a: 18446744073709551615\n"
        );
        assert_eq!(
            marshal_string(&Node::Map(vec![(Node::str("a"), Node::Int(i64::MIN))])),
            "a: -9223372036854775808\n"
        );
    }

    fn map_keys(m: &Node) -> Vec<String> {
        match m {
            Node::Map(e) => e
                .iter()
                .map(|(k, _)| match k {
                    Node::Str(s) => s.clone(),
                    _ => unreachable!(),
                })
                .collect(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn key_order() {
        // both expectations are Go yaml.v2 outputs
        let keys = [
            "a10", "a2", "a1", "b", "A", "10", "9", "a0", "a00", "a01", "a", "", "x9y", "x10y",
            "x09y", "Z", "z", "1.5", "1.25", "-1", "0", "a-1", "a-2", "ab", "a b", "a_b", "abc",
            "ab1", "ab01", "ab001", "ab10",
        ];
        let m = sorted_map(keys.iter().map(|k| (*k, Node::str(""))).collect());
        let expected = [
            "", "-1", "0", "1.5", "1.25", "9", "10", "A", "Z", "a", "a b", "a-1", "a-2", "a_b",
            "a0", "a00", "a1", "a01", "a2", "a10", "ab", "ab1", "ab01", "ab001", "ab10", "abc",
            "b", "x9y", "x09y", "x10y", "z",
        ];
        assert_eq!(map_keys(&m), expected);
        let keys2 = [
            "cronTest",
            "proj",
            "url",
            "db",
            "i",
            "certNum",
            "domains",
            "ga",
            "kubernetes",
            "k8s",
            "Kubernetes",
            "prometheus",
            "opentracing",
            "all",
            "allcdf",
            "cdf",
            "gha",
            "gha2",
            "gha10",
        ];
        let m2 = sorted_map(keys2.iter().map(|k| (*k, Node::Int(1))).collect());
        let expected2 = [
            "Kubernetes",
            "all",
            "allcdf",
            "cdf",
            "certNum",
            "cronTest",
            "db",
            "domains",
            "ga",
            "gha",
            "gha2",
            "gha10",
            "i",
            "k8s",
            "kubernetes",
            "opentracing",
            "proj",
            "prometheus",
            "url",
        ];
        assert_eq!(map_keys(&m2), expected2);
    }
}
