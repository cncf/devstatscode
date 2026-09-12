//! `gopkg.in/yaml.v2` **decoder** semantics for serde (`yaml.Unmarshal` into Go structs).
//!
//! DevStats reads its configuration (`values.yaml`, `metrics.yaml`, `tags.yaml`,
//! ...) into Go structs with yaml.v2, which coerces scalars in ways the plain
//! `String` / `i64` / `bool` serde impls do not:
//!
//! * a Go `string` field takes the *raw text* of any scalar
//!   (`affSkipTemp: 1` → `"1"`, `x: yes` → `"yes"`) and `null` / `~` / empty → `""`,
//! * a Go `int` field accepts YAML 1.1 integers (`0x1F`, `0o17`, `017`, `0b101`,
//!   `1_000`), truncates floats (`1.5` → `1`), `null` → `0`,
//! * a Go `bool` field accepts YAML 1.1 booleans (`yes`/`no`/`on`/`off`/`y`/`n`
//!   and their case variants), `null` → `false`,
//! * a Go `[N]int` / `[N]string` array must have exactly `N` elements
//!   (`invalid array: want N elements but got M`), `null` → all zero values,
//! * unknown keys are ignored (serde's default), missing keys keep zero values
//!   (use `#[serde(default)]` on the struct).
//!
//! Wrap the struct fields in [`Str`], [`Int`], [`Bool`], [`IntArray`], [`StrArray`] to get
//! those rules with `serde_yaml_ng`, and decode with [`unmarshal`], which also
//! mirrors `yaml.Unmarshal` on an empty document (zero value, no error).
//!
//! Known, deliberate differences to yaml.v2 (all on malformed / exotic input):
//! duplicate keys are an error (yaml.v2: last one wins), quoted numerics /
//! booleans in `int` / `bool` fields are accepted (yaml.v2 rejects them),
//! a multi-document stream is rejected (yaml.v2 silently takes the first
//! document); error texts follow serde, not yaml.v2.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, FixedOffset, NaiveDate, Offset, TimeZone, Utc};
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::Deserialize;

/// Go `string` field: raw scalar text, `null` → empty.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Str(pub String);

impl From<Str> for String {
    fn from(s: Str) -> String {
        s.0
    }
}

impl From<&str> for Str {
    fn from(s: &str) -> Str {
        Str(s.to_string())
    }
}

impl std::ops::Deref for Str {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

/// Go `int` field (64-bit), yaml.v2 coercions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Int(pub i64);

impl From<Int> for i64 {
    fn from(v: Int) -> i64 {
        v.0
    }
}

/// Go `bool` field, YAML 1.1 booleans.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Bool(pub bool);

impl From<Bool> for bool {
    fn from(v: Bool) -> bool {
        v.0
    }
}

/// Go `[N]int` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IntArray<const N: usize>(pub [i64; N]);

impl<const N: usize> Default for IntArray<N> {
    fn default() -> Self {
        IntArray([0; N])
    }
}

impl<const N: usize> From<IntArray<N>> for [i64; N] {
    fn from(v: IntArray<N>) -> [i64; N] {
        v.0
    }
}

/// Go `[N]string` field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StrArray<const N: usize>(pub [String; N]);

impl<const N: usize> Default for StrArray<N> {
    fn default() -> Self {
        StrArray(std::array::from_fn(|_| String::new()))
    }
}

impl<const N: usize> From<StrArray<N>> for [String; N] {
    fn from(v: StrArray<N>) -> [String; N] {
        v.0
    }
}

/// Visitor that handles the `null` question first (yaml.v2 sets the zero value
/// for `null` regardless of the field type) and then delegates.
struct Nullable<V>(V);

impl<'de, V> Visitor<'de> for Nullable<V>
where
    V: Visitor<'de>,
    V::Value: Default,
{
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(V::Value::default())
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(V::Value::default())
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_any(self.0)
    }
}

struct StrVisitor;

impl<'de> Visitor<'de> for StrVisitor {
    type Value = Str;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a YAML scalar")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Str, E> {
        Ok(Str(v.to_string()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Str, E> {
        Ok(Str(v))
    }

    // `serde_yaml_ng` hands over the raw text for every scalar when asked for a
    // string, but a generic deserializer may pass resolved values instead.
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Str, E> {
        Ok(Str(v.to_string()))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Str, E> {
        Ok(Str(v.to_string()))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Str, E> {
        Ok(Str(v.to_string()))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Str, E> {
        Ok(Str(crate::gofmt::float(v)))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Str, E> {
        Ok(Str::default())
    }

    fn visit_none<E: de::Error>(self) -> Result<Str, E> {
        Ok(Str::default())
    }
}

impl<'de> Deserialize<'de> for Str {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Str, D::Error> {
        struct Raw;
        impl<'de> Visitor<'de> for Raw {
            type Value = Str;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a YAML scalar")
            }
            fn visit_none<E: de::Error>(self) -> Result<Str, E> {
                Ok(Str::default())
            }
            fn visit_unit<E: de::Error>(self) -> Result<Str, E> {
                Ok(Str::default())
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Str, D::Error> {
                // asking for a str makes serde_yaml_ng return the scalar's text verbatim
                d.deserialize_str(StrVisitor)
            }
        }
        d.deserialize_option(Raw)
    }
}

struct IntVisitor;

impl<'de> Visitor<'de> for IntVisitor {
    type Value = Int;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a YAML integer")
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Int, E> {
        Ok(Int(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Int, E> {
        i64::try_from(v)
            .map(Int)
            .map_err(|_| E::custom(format!("cannot unmarshal !!int `{v}` into int")))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Int, E> {
        // yaml.v2: `if resolved <= math.MaxInt64 { out.SetInt(int64(resolved)) }`
        if v <= i64::MAX as f64 {
            Ok(Int(v as i64))
        } else {
            Err(E::custom(format!(
                "cannot unmarshal !!float `{}` into int",
                crate::gofmt::float(v)
            )))
        }
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Int, E> {
        Err(E::custom(format!("cannot unmarshal !!bool `{v}` into int")))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Int, E> {
        resolve_int(v).map(Int).ok_or_else(|| {
            E::custom(format!(
                "cannot unmarshal !!str `{}` into int",
                v.replace('\n', "\\n")
            ))
        })
    }

    fn visit_unit<E: de::Error>(self) -> Result<Int, E> {
        Ok(Int(0))
    }

    fn visit_none<E: de::Error>(self) -> Result<Int, E> {
        Ok(Int(0))
    }
}

impl<'de> Deserialize<'de> for Int {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Int, D::Error> {
        d.deserialize_option(Nullable(IntVisitor))
    }
}

struct BoolVisitor;

impl<'de> Visitor<'de> for BoolVisitor {
    type Value = Bool;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a YAML boolean")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Bool, E> {
        Ok(Bool(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Bool, E> {
        resolve_bool(v)
            .map(Bool)
            .ok_or_else(|| E::custom(format!("cannot unmarshal !!str `{v}` into bool")))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Bool, E> {
        Err(E::custom(format!("cannot unmarshal !!int `{v}` into bool")))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Bool, E> {
        Err(E::custom(format!("cannot unmarshal !!int `{v}` into bool")))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Bool, E> {
        Err(E::custom(format!(
            "cannot unmarshal !!float `{}` into bool",
            crate::gofmt::float(v)
        )))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Bool, E> {
        Ok(Bool(false))
    }

    fn visit_none<E: de::Error>(self) -> Result<Bool, E> {
        Ok(Bool(false))
    }
}

impl<'de> Deserialize<'de> for Bool {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Bool, D::Error> {
        d.deserialize_option(Nullable(BoolVisitor))
    }
}

struct ArrayVisitor<const N: usize>;

impl<'de, const N: usize> Visitor<'de> for ArrayVisitor<N> {
    type Value = IntArray<N>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a YAML sequence of {N} integers")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<IntArray<N>, A::Error> {
        let mut items: Vec<i64> = Vec::new();
        while let Some(Int(v)) = seq.next_element::<Int>()? {
            items.push(v);
        }
        if items.len() != N {
            return Err(de::Error::custom(format!(
                "invalid array: want {N} elements but got {}",
                items.len()
            )));
        }
        let mut out = [0i64; N];
        out.copy_from_slice(&items);
        Ok(IntArray(out))
    }

    fn visit_unit<E: de::Error>(self) -> Result<IntArray<N>, E> {
        Ok(IntArray::default())
    }

    fn visit_none<E: de::Error>(self) -> Result<IntArray<N>, E> {
        Ok(IntArray::default())
    }
}

impl<'de, const N: usize> Deserialize<'de> for IntArray<N> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<IntArray<N>, D::Error> {
        struct Opt<const N: usize>;
        impl<'de, const N: usize> Visitor<'de> for Opt<N> {
            type Value = IntArray<N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a YAML sequence of {N} integers")
            }
            fn visit_none<E: de::Error>(self) -> Result<IntArray<N>, E> {
                Ok(IntArray::default())
            }
            fn visit_unit<E: de::Error>(self) -> Result<IntArray<N>, E> {
                Ok(IntArray::default())
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<IntArray<N>, D::Error> {
                d.deserialize_seq(ArrayVisitor::<N>)
            }
        }
        d.deserialize_option(Opt::<N>)
    }
}

/// yaml.v2 boolean resolution (YAML 1.1 `resolveMap`).
pub fn resolve_bool(s: &str) -> Option<bool> {
    match s {
        "y" | "Y" | "yes" | "Yes" | "YES" | "true" | "True" | "TRUE" | "on" | "On" | "ON" => {
            Some(true)
        }
        "n" | "N" | "no" | "No" | "NO" | "false" | "False" | "FALSE" | "off" | "Off" | "OFF" => {
            Some(false)
        }
        _ => None,
    }
}

/// yaml.v2 integer resolution of a plain scalar that `serde_yaml_ng` left as
/// text: `_` separators are dropped, then Go `strconv.ParseInt(s, 0, 64)`
/// (`0x`, `0o`, `0b`, leading-zero octal), `ParseUint`, and finally yaml.v2's
/// float syntax truncated to an integer.
pub fn resolve_int(s: &str) -> Option<i64> {
    let plain: String = s.chars().filter(|c| *c != '_').collect();
    if let Some(v) = go_parse_int_base0(&plain) {
        return Some(v);
    }
    if let Some(v) = go_parse_uint_base0(&plain) {
        // yaml.v2: uint64 values above MaxInt64 do not fit an int
        return i64::try_from(v).ok();
    }
    if is_yaml_style_float(&plain) {
        if let Ok(f) = plain.parse::<f64>() {
            if f <= i64::MAX as f64 {
                return Some(f as i64);
            }
        }
    }
    None
}

/// yaml.v2 `yamlStyleFloat`: `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`.
fn is_yaml_style_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
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
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
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

/// Base prefix handling of Go `strconv.ParseInt/ParseUint` with `base == 0`.
fn split_base0(s: &str) -> Option<(&str, u32)> {
    let b = s.as_bytes();
    if b.is_empty() {
        return None;
    }
    if b.len() > 1 && b[0] == b'0' {
        match b[1] {
            b'x' | b'X' => return Some((&s[2..], 16)),
            b'o' | b'O' => return Some((&s[2..], 8)),
            b'b' | b'B' => return Some((&s[2..], 2)),
            _ => return Some((&s[1..], 8)),
        }
    }
    Some((s, 10))
}

fn parse_digits_u64(digits: &str, base: u32) -> Option<u64> {
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(base)) {
        return None;
    }
    u64::from_str_radix(digits, base).ok()
}

/// Go `strconv.ParseInt(s, 0, 64)`.
pub fn go_parse_int_base0(s: &str) -> Option<i64> {
    let (neg, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let (digits, base) = split_base0(body)?;
    let v = parse_digits_u64(digits, base)?;
    if neg {
        if v > i64::MAX as u64 + 1 {
            return None;
        }
        Some((v as i64).wrapping_neg())
    } else {
        i64::try_from(v).ok()
    }
}

/// Go `strconv.ParseUint(s, 0, 64)`.
pub fn go_parse_uint_base0(s: &str) -> Option<u64> {
    let (digits, base) = split_base0(s)?;
    parse_digits_u64(digits, base)
}

/// `#[serde(deserialize_with = "de::string")]` — Go `string` field as a plain `String`.
pub fn string<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Str::deserialize(d).map(|s| s.0)
}

/// `#[serde(deserialize_with = "de::int")]` — Go `int` field as a plain `i64`.
pub fn int<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    Int::deserialize(d).map(|v| v.0)
}

/// `#[serde(deserialize_with = "de::boolean")]` — Go `bool` field as a plain `bool`.
pub fn boolean<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    Bool::deserialize(d).map(|v| v.0)
}

/// `#[serde(deserialize_with = "de::int_array::<_, 4>")]` — Go `[N]int` field.
pub fn int_array<'de, D: Deserializer<'de>, const N: usize>(d: D) -> Result<[i64; N], D::Error> {
    IntArray::<N>::deserialize(d).map(|v| v.0)
}

struct StrArrayVisitor<const N: usize>;

impl<'de, const N: usize> Visitor<'de> for StrArrayVisitor<N> {
    type Value = StrArray<N>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a YAML sequence of {N} scalars")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<StrArray<N>, A::Error> {
        let mut items: Vec<String> = Vec::new();
        while let Some(Str(v)) = seq.next_element::<Str>()? {
            items.push(v);
        }
        if items.len() != N {
            return Err(de::Error::custom(format!(
                "invalid array: want {N} elements but got {}",
                items.len()
            )));
        }
        let mut it = items.into_iter();
        Ok(StrArray(std::array::from_fn(|_| {
            it.next().unwrap_or_default()
        })))
    }

    fn visit_unit<E: de::Error>(self) -> Result<StrArray<N>, E> {
        Ok(StrArray::default())
    }

    fn visit_none<E: de::Error>(self) -> Result<StrArray<N>, E> {
        Ok(StrArray::default())
    }
}

impl<'de, const N: usize> Deserialize<'de> for StrArray<N> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<StrArray<N>, D::Error> {
        struct Opt<const N: usize>;
        impl<'de, const N: usize> Visitor<'de> for Opt<N> {
            type Value = StrArray<N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a YAML sequence of {N} scalars")
            }
            fn visit_none<E: de::Error>(self) -> Result<StrArray<N>, E> {
                Ok(StrArray::default())
            }
            fn visit_unit<E: de::Error>(self) -> Result<StrArray<N>, E> {
                Ok(StrArray::default())
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<StrArray<N>, D::Error> {
                d.deserialize_seq(StrArrayVisitor::<N>)
            }
        }
        d.deserialize_option(Opt::<N>)
    }
}

/// `#[serde(deserialize_with = "de::str_array_map::<_, N>")]` — Go
/// `map[string][N]string` field: `null` → empty map.
pub fn str_array_map<'de, D: Deserializer<'de>, const N: usize>(
    d: D,
) -> Result<std::collections::BTreeMap<String, [String; N]>, D::Error> {
    let m = Option::<std::collections::BTreeMap<Str, StrArray<N>>>::deserialize(d)?;
    Ok(m.unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k.0, v.0))
        .collect())
}

/// `#[serde(deserialize_with = "de::seq")]` — Go slice field: `null` → empty
/// (yaml.v2 leaves the slice nil, which marshals back as `[]`).
pub fn seq<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Vec<T>, D::Error> {
    Option::<Vec<T>>::deserialize(d).map(|v| v.unwrap_or_default())
}

/// `#[serde(deserialize_with = "de::str_seq")]` — Go `[]string` field: every
/// element takes the raw scalar text, `null` → empty.
pub fn str_seq<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    seq::<D, Str>(d).map(|v| v.into_iter().map(String::from).collect())
}

/// `#[serde(deserialize_with = "de::opt_str_seq")]` — Go `*[]string` field:
/// `null` / missing → `None` (nil pointer), a sequence (even an empty one)
/// → `Some` (yaml.v2 allocates the slice for `[]`).
pub fn opt_str_seq<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<String>>, D::Error> {
    Option::<Vec<Str>>::deserialize(d)
        .map(|v| v.map(|items| items.into_iter().map(String::from).collect()))
}

/// `#[serde(deserialize_with = "de::str_key_map")]` — Go `map[string]T`
/// field: `null` → empty map (keys take the raw scalar text).
pub fn str_key_map<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<BTreeMap<String, T>, D::Error> {
    let m = Option::<BTreeMap<Str, T>>::deserialize(d)?;
    Ok(m.unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k.0, v))
        .collect())
}

/// `#[serde(deserialize_with = "de::str_map")]` — Go `map[string]string`
/// field: keys and values take the raw scalar text, `null` → empty map.
pub fn str_map<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<String, String>, D::Error> {
    str_key_map::<D, Str>(d).map(|m| m.into_iter().map(|(k, v)| (k, v.0)).collect())
}

/// yaml.v2 float resolution (`yamlStyleFloat` plus the YAML 1.1 `.inf` /
/// `.nan` spellings and integers).
pub fn resolve_float(s: &str) -> Option<f64> {
    match s {
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => return Some(f64::INFINITY),
        "-.inf" | "-.Inf" | "-.INF" => return Some(f64::NEG_INFINITY),
        ".nan" | ".NaN" | ".NAN" => return Some(f64::NAN),
        _ => {}
    }
    // yaml.v2 drops the `_` separators first, then matches
    // ^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$
    let plain = s.replace('_', "");
    let b = plain.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        i += 1;
    }
    let digits = |b: &[u8], mut j: usize| -> usize {
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        j
    };
    let mut j = digits(b, i);
    if j == i {
        // `.5`
        if j < b.len() && b[j] == b'.' {
            let k = digits(b, j + 1);
            if k == j + 1 {
                return None;
            }
            j = k;
        } else {
            return None;
        }
    } else if j < b.len() && b[j] == b'.' {
        j = digits(b, j + 1);
    }
    if j < b.len() && (b[j] == b'e' || b[j] == b'E') {
        let mut k = j + 1;
        if k < b.len() && (b[k] == b'-' || b[k] == b'+') {
            k += 1;
        }
        let m = digits(b, k);
        if m == k {
            return None;
        }
        j = m;
    }
    if j != b.len() {
        // not a float: YAML 1.1 integers (`0x1F`, `0o17`, `1_000`) are floats too
        return resolve_int(s).map(|i| i as f64);
    }
    plain.parse::<f64>().ok()
}

/// Go `float64` field, yaml.v2 coercions (integers are accepted, `null` → 0).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Float(pub f64);

impl From<Float> for f64 {
    fn from(v: Float) -> f64 {
        v.0
    }
}

struct FloatVisitor;

impl<'de> Visitor<'de> for FloatVisitor {
    type Value = Float;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a YAML float")
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Float, E> {
        Ok(Float(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Float, E> {
        Ok(Float(v as f64))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Float, E> {
        Ok(Float(v as f64))
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Float, E> {
        Err(E::custom(format!(
            "cannot unmarshal !!bool `{v}` into float64"
        )))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Float, E> {
        resolve_float(v).map(Float).ok_or_else(|| {
            E::custom(format!(
                "cannot unmarshal !!str `{}` into float64",
                v.replace('\n', "\\n")
            ))
        })
    }

    fn visit_unit<E: de::Error>(self) -> Result<Float, E> {
        Ok(Float(0.0))
    }

    fn visit_none<E: de::Error>(self) -> Result<Float, E> {
        Ok(Float(0.0))
    }
}

impl<'de> Deserialize<'de> for Float {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Float, D::Error> {
        d.deserialize_option(Nullable(FloatVisitor))
    }
}

/// `#[serde(deserialize_with = "de::float")]` — Go `float64` field as a plain `f64`.
pub fn float<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    Float::deserialize(d).map(|v| v.0)
}

/// `#[serde(deserialize_with = "de::opt_float")]` — Go `*float64` field:
/// `null` / missing → `None`.
pub fn opt_float<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    struct Opt;
    impl<'de> Visitor<'de> for Opt {
        type Value = Option<f64>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a YAML float or null")
        }
        fn visit_none<E: de::Error>(self) -> Result<Option<f64>, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Option<f64>, E> {
            Ok(None)
        }
        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Option<f64>, D::Error> {
            d.deserialize_any(FloatVisitor).map(|v| Some(v.0))
        }
    }
    d.deserialize_option(Opt)
}

fn take_digits(b: &[u8], i: usize, min: usize, max: usize) -> Option<(u32, usize)> {
    let mut j = i;
    while j < b.len() && j - i < max && b[j].is_ascii_digit() {
        j += 1;
    }
    if j - i < min {
        return None;
    }
    std::str::from_utf8(&b[i..j])
        .ok()?
        .parse::<u32>()
        .ok()
        .map(|v| (v, j))
}

fn expect_byte(b: &[u8], i: usize, c: u8) -> Option<usize> {
    (i < b.len() && b[i] == c).then_some(i + 1)
}

/// Go `time.Parse` of one of yaml.v2's `allowedTimestampFormats`
/// (`2006-1-2T15:4:5.999999999Z07:00`, the lower-case `t` variant,
/// `2006-1-2 15:4:5.999999999` and `2006-1-2`): one- or two-digit month, day,
/// hour, minute and second, any number of fractional digits (`.` or `,`,
/// truncated to nanoseconds), `Z` or `±hh:mm`; no zone means UTC.
pub fn parse_yaml_timestamp(s: &str) -> Option<DateTime<FixedOffset>> {
    let b = s.as_bytes();
    // Quick check (yaml.v2 `parseTimestamp`): all date formats start with YYYY-.
    if b.len() < 5 || !b[..4].iter().all(|c| c.is_ascii_digit()) || b[4] != b'-' {
        return None;
    }
    let (year, i) = take_digits(b, 0, 4, 4)?;
    let i = expect_byte(b, i, b'-')?;
    let (month, i) = take_digits(b, i, 1, 2)?;
    let i = expect_byte(b, i, b'-')?;
    let (day, mut i) = take_digits(b, i, 1, 2)?;
    let date = NaiveDate::from_ymd_opt(year as i32, month, day)?;
    let (mut hour, mut min, mut sec, mut nanos, mut offset) = (0, 0, 0, 0u32, 0i32);
    if i < b.len() {
        let with_zone = match b[i] {
            b'T' | b't' => true,
            b' ' => false,
            _ => return None,
        };
        i += 1;
        let (h, j) = take_digits(b, i, 1, 2)?;
        let j = expect_byte(b, j, b':')?;
        let (m, j) = take_digits(b, j, 1, 2)?;
        let j = expect_byte(b, j, b':')?;
        let (sc, mut j) = take_digits(b, j, 1, 2)?;
        if h > 23 || m > 59 || sc > 59 {
            return None;
        }
        (hour, min, sec) = (h, m, sc);
        if j + 1 < b.len() && (b[j] == b'.' || b[j] == b',') && b[j + 1].is_ascii_digit() {
            let mut k = j + 1;
            while k < b.len() && b[k].is_ascii_digit() {
                k += 1;
            }
            let frac = &s[j + 1..k];
            let frac = if frac.len() > 9 { &frac[..9] } else { frac };
            nanos = frac.parse::<u32>().ok()? * 10u32.pow(9 - frac.len() as u32);
            j = k;
        }
        if with_zone {
            if j < b.len() && b[j] == b'Z' {
                j += 1;
            } else {
                let sign = match b.get(j) {
                    Some(b'+') => 1,
                    Some(b'-') => -1,
                    _ => return None,
                };
                let (zh, k) = take_digits(b, j + 1, 2, 2)?;
                let k = expect_byte(b, k, b':')?;
                let (zm, k) = take_digits(b, k, 2, 2)?;
                offset = sign * (zh as i32 * 3600 + zm as i32 * 60);
                j = k;
            }
        }
        i = j;
    }
    if i != b.len() {
        return None;
    }
    let tz = FixedOffset::east_opt(offset)?;
    let naive = date.and_hms_nano_opt(hour, min, sec, nanos)?;
    tz.from_local_datetime(&naive).single()
}

/// Go `time.Time.UnmarshalText`: strict RFC 3339 (`2006-01-02T15:04:05Z07:00`
/// with optional fractional seconds).
fn parse_rfc3339_text(s: &str) -> Option<DateTime<FixedOffset>> {
    let b = s.as_bytes();
    // Go's layout wants two-digit fields and an upper-case `T`.
    if b.len() < 20 || b[10] != b'T' || !b[..4].iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    for &(pos, len) in &[(5usize, 2usize), (8, 2), (11, 2), (14, 2), (17, 2)] {
        if !b[pos..pos + len].iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
    }
    DateTime::parse_from_rfc3339(s).ok()
}

/// `#[serde(deserialize_with = "de::opt_time")]` — Go `*time.Time` field:
/// a scalar in one of yaml.v2's timestamp formats (see
/// [`parse_yaml_timestamp`]) or in RFC 3339 (Go's `time.Time.UnmarshalText`,
/// which yaml.v2 falls back to for quoted values); `null` / missing → `None`.
/// The zone offset of the value is kept (`Z` and no zone are UTC), like Go's
/// `time.Time` does.
pub fn opt_time<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<DateTime<FixedOffset>>, D::Error> {
    struct Opt;
    impl<'de> Visitor<'de> for Opt {
        type Value = Option<DateTime<FixedOffset>>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a YAML timestamp or null")
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            let Str(text) = Str::deserialize(d)?;
            if text.is_empty() {
                return Ok(None);
            }
            parse_yaml_timestamp(&text)
                .or_else(|| parse_rfc3339_text(&text))
                .map(Some)
                .ok_or_else(|| {
                    de::Error::custom(format!(
                        "parsing time \"{}\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"{}\" as \"2006\"",
                        text, text
                    ))
                })
        }
    }
    d.deserialize_option(Opt)
}

/// `#[serde(deserialize_with = "de::time_seq")]` — Go `[]time.Time` field:
/// every element decodes like [`opt_time`] (a `null` element is Go's zero
/// time), `null` / missing → empty.
pub fn time_seq<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<DateTime<FixedOffset>>, D::Error> {
    struct Elem(Option<DateTime<FixedOffset>>);
    impl<'de> Deserialize<'de> for Elem {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            opt_time(d).map(Elem)
        }
    }
    let zero = || {
        NaiveDate::from_ymd_opt(1, 1, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc().fixed_offset())
            .expect("year 1 is representable")
    };
    Ok(Option::<Vec<Elem>>::deserialize(d)?
        .unwrap_or_default()
        .into_iter()
        .map(|Elem(t)| t.unwrap_or_else(zero))
        .collect())
}

/// A UTC view of a decoded timestamp (Go code mostly ignores the zone).
pub fn to_utc(t: DateTime<FixedOffset>) -> DateTime<Utc> {
    t.with_timezone(&Utc)
}

/// Is the decoded timestamp in UTC (`Z` or no zone)?
pub fn is_utc(t: &DateTime<FixedOffset>) -> bool {
    t.offset().fix().local_minus_utc() == 0
}

/// `yaml.Unmarshal(data, &out)` — decode a YAML document into a struct made of
/// the wrappers above (`#[derive(Deserialize, Default)] #[serde(default)]`).
/// An empty document yields the zero value like yaml.v2 does; the error text
/// is prefixed with `yaml: ` like yaml.v2's errors.
pub fn unmarshal<T>(data: &[u8]) -> Result<T, String>
where
    T: for<'de> Deserialize<'de> + Default,
{
    match serde_yaml_ng::from_slice::<Option<T>>(data) {
        Ok(Some(v)) => Ok(v),
        Ok(None) => Ok(T::default()),
        Err(e) => Err(format!("yaml: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, Deserialize, PartialEq)]
    #[serde(default)]
    struct S {
        s: Str,
        i: Int,
        b: Bool,
        a: IntArray<4>,
        #[serde(rename = "camelCase")]
        camel: Str,
        list: Option<Vec<Inner>>,
    }

    #[derive(Debug, Default, Deserialize, PartialEq)]
    #[serde(default)]
    struct Inner {
        x: Int,
    }

    #[derive(Debug, Default, Deserialize, PartialEq)]
    #[serde(default)]
    struct WithMap {
        #[serde(deserialize_with = "str_array_map::<_, 2>")]
        other: std::collections::BTreeMap<String, [String; 2]>,
        pair: StrArray<2>,
    }

    #[test]
    fn string_arrays_and_maps_of_them() {
        // Go: OtherTags map[string][2]string — raw scalar text, exact length.
        let v: WithMap = unmarshal(
            b"other:\n  alias: [full_command, 1]\n  b:\n    - yes\n    - ~\npair: [a, 0x10]\n",
        )
        .unwrap();
        assert_eq!(
            v.other["alias"],
            ["full_command".to_string(), "1".to_string()]
        );
        assert_eq!(v.other["b"], ["yes".to_string(), String::new()]);
        assert_eq!(v.pair, StrArray(["a".to_string(), "0x10".to_string()]));
        // null → zero values
        let v: WithMap = unmarshal(b"other: ~\npair:\n").unwrap();
        assert!(v.other.is_empty());
        assert_eq!(v.pair, StrArray::default());
        // wrong length is an error like yaml.v2's
        let e = unmarshal::<WithMap>(b"other:\n  a: [x]\n").unwrap_err();
        assert!(
            e.contains("invalid array: want 2 elements but got 1"),
            "{e}"
        );
        let e = unmarshal::<WithMap>(b"pair: [x, y, z]\n").unwrap_err();
        assert!(
            e.contains("invalid array: want 2 elements but got 3"),
            "{e}"
        );
    }

    fn dec(y: &str) -> Result<S, String> {
        unmarshal::<S>(y.as_bytes())
    }

    #[test]
    fn strings_take_raw_text() {
        assert_eq!(dec("s: 1").unwrap().s.0, "1");
        assert_eq!(dec("s: '1'").unwrap().s.0, "1");
        assert_eq!(dec("s: yes").unwrap().s.0, "yes");
        assert_eq!(dec("s: 1.50").unwrap().s.0, "1.50");
        assert_eq!(dec("s: 0x10").unwrap().s.0, "0x10");
        assert_eq!(dec("s: 10 2 * * *").unwrap().s.0, "10 2 * * *");
        assert_eq!(dec("s: \"a\\tb\"").unwrap().s.0, "a\tb");
        assert_eq!(dec("s: |\n  x\n  y\n").unwrap().s.0, "x\ny\n");
        assert_eq!(dec("s:").unwrap().s.0, "");
        assert_eq!(dec("s: ~").unwrap().s.0, "");
        assert_eq!(dec("s: null").unwrap().s.0, "");
        assert_eq!(dec("s: \"~\"").unwrap().s.0, "~");
        assert_eq!(dec("s: \"\"").unwrap().s.0, "");
        assert!(dec("s: [1]").is_err());
        assert!(dec("s: {a: 1}").is_err());
    }

    #[test]
    fn ints_follow_yaml_1_1() {
        assert_eq!(dec("i: 42").unwrap().i.0, 42);
        assert_eq!(dec("i: -42").unwrap().i.0, -42);
        assert_eq!(dec("i: +7").unwrap().i.0, 7);
        assert_eq!(dec("i: 0x1F").unwrap().i.0, 31);
        assert_eq!(dec("i: 0o17").unwrap().i.0, 15);
        assert_eq!(dec("i: 017").unwrap().i.0, 15);
        assert_eq!(dec("i: 0b101").unwrap().i.0, 5);
        assert_eq!(dec("i: 1_000").unwrap().i.0, 1000);
        assert_eq!(dec("i: 1.9").unwrap().i.0, 1);
        assert_eq!(dec("i: -1.9").unwrap().i.0, -1);
        assert_eq!(dec("i: 1e3").unwrap().i.0, 1000);
        assert_eq!(dec("i:").unwrap().i.0, 0);
        assert_eq!(dec("i: ~").unwrap().i.0, 0);
        assert_eq!(dec("i: 9223372036854775807").unwrap().i.0, i64::MAX);
        assert_eq!(dec("i: -9223372036854775808").unwrap().i.0, i64::MIN);
        assert!(dec("i: 9223372036854775808").is_err());
        assert!(dec("i: abc").is_err());
        assert!(dec("i: true").is_err());
        assert!(dec("i: 2001-12-14").is_err());
        assert!(dec("i: [1]").is_err());
        assert!(dec("i: 1 2").is_err());
    }

    #[test]
    fn bools_follow_yaml_1_1() {
        for t in [
            "true", "True", "TRUE", "yes", "Yes", "YES", "y", "Y", "on", "On", "ON",
        ] {
            assert!(dec(&format!("b: {t}")).unwrap().b.0, "{t}");
        }
        for f in [
            "false", "False", "FALSE", "no", "No", "NO", "n", "N", "off", "Off", "OFF",
        ] {
            assert!(!dec(&format!("b: {f}")).unwrap().b.0, "{f}");
        }
        assert!(!dec("b:").unwrap().b.0);
        assert!(!dec("b: ~").unwrap().b.0);
        assert!(dec("b: 1").is_err());
        assert!(dec("b: maybe").is_err());
        assert!(dec("b: [true]").is_err());
    }

    #[test]
    fn fixed_arrays() {
        assert_eq!(dec("a: [1, 0, 0x2, 3]").unwrap().a.0, [1, 0, 2, 3]);
        assert_eq!(dec("a:\n- 1\n- 2\n- 3\n- 4\n").unwrap().a.0, [1, 2, 3, 4]);
        assert_eq!(dec("a:").unwrap().a.0, [0, 0, 0, 0]);
        assert_eq!(dec("").unwrap().a.0, [0, 0, 0, 0]);
        let err = dec("a: [1, 2, 3]").unwrap_err();
        assert!(
            err.contains("invalid array: want 4 elements but got 3"),
            "{err}"
        );
        assert!(dec("a: 1").is_err());
        assert!(dec("a: [1, x, 3, 4]").is_err());
    }

    #[test]
    fn documents() {
        // empty / comment-only documents decode to the zero value
        assert_eq!(dec("").unwrap(), S::default());
        assert_eq!(dec("# nothing\n").unwrap(), S::default());
        assert_eq!(dec("---\n").unwrap(), S::default());
        // unknown keys are ignored, renamed keys honoured
        let v = dec("other: 1\ncamelCase: v\nlist:\n- x: 1\n- {x: 2}\n").unwrap();
        assert_eq!(v.camel.0, "v");
        assert_eq!(v.list, Some(vec![Inner { x: Int(1) }, Inner { x: Int(2) }]));
        assert_eq!(dec("list: ~").unwrap().list, None);
        assert_eq!(dec("list: []").unwrap().list, Some(vec![]));
        // errors carry the yaml.v2 prefix
        let err = dec("i: x").unwrap_err();
        assert!(err.starts_with("yaml: "), "{err}");
        assert!(dec("- a\n").is_err());
        assert!(dec("just a scalar").is_err());
    }

    #[derive(Debug, Default, Deserialize, PartialEq)]
    #[serde(default)]
    struct Plain {
        #[serde(deserialize_with = "string")]
        s: String,
        #[serde(deserialize_with = "int")]
        i: i64,
        #[serde(deserialize_with = "boolean")]
        b: bool,
        #[serde(deserialize_with = "int_array::<_, 4>")]
        a: [i64; 4],
        #[serde(deserialize_with = "seq")]
        v: Vec<Inner>,
    }

    #[test]
    fn deserialize_with_adapters() {
        let p: Plain = unmarshal(b"s: 5\ni: 0x10\nb: yes\na: [1,2,3,4]\nv:\n- x: 3\n").unwrap();
        assert_eq!(p.s, "5");
        assert_eq!(p.i, 16);
        assert!(p.b);
        assert_eq!(p.a, [1, 2, 3, 4]);
        assert_eq!(p.v, vec![Inner { x: Int(3) }]);
        let p: Plain = unmarshal(b"v: ~\ns: ~\n").unwrap();
        assert_eq!(p, Plain::default());
        let p: Plain = unmarshal(b"").unwrap();
        assert_eq!(p, Plain::default());
    }

    #[test]
    fn go_base0_parsing() {
        assert_eq!(go_parse_int_base0("0"), Some(0));
        assert_eq!(go_parse_int_base0("-0"), Some(0));
        assert_eq!(go_parse_int_base0("0x"), None);
        assert_eq!(go_parse_int_base0("08"), None);
        assert_eq!(go_parse_int_base0("0B11"), Some(3));
        assert_eq!(go_parse_int_base0("-0x10"), Some(-16));
        assert_eq!(go_parse_int_base0("9223372036854775808"), None);
        assert_eq!(go_parse_uint_base0("9223372036854775808"), Some(1 << 63));
        assert_eq!(go_parse_int_base0("-9223372036854775808"), Some(i64::MIN));
        assert_eq!(go_parse_int_base0("-9223372036854775809"), None);
        assert_eq!(go_parse_int_base0(""), None);
        assert_eq!(go_parse_int_base0("-"), None);
        assert_eq!(go_parse_int_base0("1a"), None);
        assert!(is_yaml_style_float("1."));
        assert!(is_yaml_style_float(".5"));
        assert!(is_yaml_style_float("-1.5e+3"));
        assert!(!is_yaml_style_float("."));
        assert!(!is_yaml_style_float("1e"));
        assert!(!is_yaml_style_float("e5"));
        assert_eq!(resolve_int("1_0.9"), Some(10));
        assert_eq!(resolve_int(".5"), Some(0));
        assert_eq!(resolve_int("18446744073709551615"), None);
    }

    #[test]
    fn floats_follow_yaml_v2() {
        #[derive(Debug, Default, Deserialize, PartialEq)]
        #[serde(default)]
        struct F {
            #[serde(deserialize_with = "float")]
            f: f64,
            #[serde(deserialize_with = "opt_float")]
            o: Option<f64>,
        }
        let v: F = unmarshal(b"f: 0.99\no: 3.0\n").unwrap();
        assert_eq!(
            v,
            F {
                f: 0.99,
                o: Some(3.0)
            }
        );
        let v: F = unmarshal(b"f: 2\no: 1_000\n").unwrap();
        assert_eq!(
            v,
            F {
                f: 2.0,
                o: Some(1000.0)
            }
        );
        let v: F = unmarshal(b"f: ~\no: null\n").unwrap();
        assert_eq!(v, F { f: 0.0, o: None });
        let v: F = unmarshal(b"f: .5\n").unwrap();
        assert_eq!(v, F { f: 0.5, o: None });
        let v: F = unmarshal(b"f: -.inf\no: .inf\n").unwrap();
        assert_eq!(v.f, f64::NEG_INFINITY);
        assert_eq!(v.o, Some(f64::INFINITY));
        let v: F = unmarshal(b"f: 1e3\no: '2.5'\n").unwrap();
        assert_eq!(
            v,
            F {
                f: 1000.0,
                o: Some(2.5)
            }
        );
        assert!(unmarshal::<F>(b"f: abc\n").is_err());
        assert!(unmarshal::<F>(b"o: true\n").is_err());
        assert_eq!(resolve_float("1."), Some(1.0));
        assert_eq!(resolve_float("+1.5e-3"), Some(0.0015));
        assert_eq!(resolve_float("."), None);
        assert_eq!(resolve_float("1e"), None);
        assert_eq!(resolve_float("0x10"), Some(16.0));
        assert_eq!(resolve_float("1_000.5"), Some(1000.5));
    }

    #[test]
    fn timestamps_follow_yaml_v2() {
        #[derive(Debug, Default, Deserialize, PartialEq)]
        #[serde(default)]
        struct T {
            #[serde(deserialize_with = "opt_time")]
            t: Option<DateTime<FixedOffset>>,
        }
        let utc = |y, mo, d, h, mi, s, n| {
            Some(
                Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
                    .unwrap()
                    .fixed_offset()
                    .with_nanosecond(n)
                    .unwrap(),
            )
        };
        use chrono::Timelike;
        let t = |y: &[u8]| unmarshal::<T>(y).map(|v| v.t);
        assert_eq!(
            t(b"t: 2014-06-01T00:00:00Z\n").unwrap(),
            utc(2014, 6, 1, 0, 0, 0, 0)
        );
        assert_eq!(
            t(b"t: 2014-6-1t1:2:3.5Z\n").unwrap(),
            utc(2014, 6, 1, 1, 2, 3, 500_000_000)
        );
        assert_eq!(
            t(b"t: 2014-06-01 12:30:45\n").unwrap(),
            utc(2014, 6, 1, 12, 30, 45, 0)
        );
        assert_eq!(t(b"t: 2014-06-01\n").unwrap(), utc(2014, 6, 1, 0, 0, 0, 0));
        assert_eq!(
            t(b"t: 2014-06-01T00:00:00.1234567891Z\n").unwrap(),
            utc(2014, 6, 1, 0, 0, 0, 123_456_789)
        );
        let plus2 = t(b"t: 2014-06-01T10:00:00+02:00\n").unwrap().unwrap();
        assert_eq!(plus2.offset().local_minus_utc(), 7200);
        assert_eq!(
            to_utc(plus2),
            Utc.with_ymd_and_hms(2014, 6, 1, 8, 0, 0).unwrap()
        );
        assert!(!is_utc(&plus2));
        assert!(is_utc(&t(b"t: 2014-06-01\n").unwrap().unwrap()));
        // quoted RFC 3339 goes through time.Time.UnmarshalText
        assert_eq!(
            t(b"t: '2014-06-01T00:00:00Z'\n").unwrap(),
            utc(2014, 6, 1, 0, 0, 0, 0)
        );
        assert_eq!(t(b"t: ~\n").unwrap(), None);
        assert_eq!(t(b"t:\n").unwrap(), None);
        assert_eq!(t(b"x: 1\n").unwrap(), None);
        for bad in [
            &b"t: 2014\n"[..],
            b"t: 2014-13-01\n",
            b"t: 2014-06-31\n",
            b"t: 2014-06-01T24:00:00Z\n",
            b"t: 2014-06-01T00:00:00\n",
            b"t: 2014-06-01T00:00:00+0200\n",
            b"t: 2014-06-01 00:00:00Z\n",
            b"t: 2014-06-01x\n",
            b"t: true\n",
            b"t: 1.5\n",
        ] {
            assert!(t(bad).is_err(), "{}", String::from_utf8_lossy(bad));
        }
        assert_eq!(parse_yaml_timestamp("2001-12-14 21:59:43.10 -5"), None);
    }

    #[test]
    fn string_sequences_and_maps() {
        #[derive(Debug, Default, Deserialize, PartialEq)]
        #[serde(default)]
        struct M {
            #[serde(deserialize_with = "str_seq")]
            l: Vec<String>,
            #[serde(deserialize_with = "str_map")]
            m: BTreeMap<String, String>,
            #[serde(deserialize_with = "str_key_map")]
            k: BTreeMap<String, Int>,
        }
        let v: M = unmarshal(b"l: [a, 1, yes]\nm:\n  A: x\n  2: 3\nk:\n  a: 1\n").unwrap();
        assert_eq!(v.l, ["a", "1", "yes"]);
        assert_eq!(v.m.get("A").map(String::as_str), Some("x"));
        assert_eq!(v.m.get("2").map(String::as_str), Some("3"));
        assert_eq!(v.k.get("a"), Some(&Int(1)));
        let v: M = unmarshal(b"l:\nm: ~\nk:\n").unwrap();
        assert!(v.l.is_empty() && v.m.is_empty() && v.k.is_empty());
    }

    #[test]
    fn optional_string_sequence_is_nil_only_for_null() {
        #[derive(Debug, Default, Deserialize, PartialEq)]
        #[serde(default)]
        struct M {
            #[serde(deserialize_with = "opt_str_seq")]
            sqls: Option<Vec<String>>,
        }
        let v: M = unmarshal(b"sqls: [a, 1]\n").unwrap();
        assert_eq!(v.sqls, Some(vec!["a".to_string(), "1".to_string()]));
        let v: M = unmarshal(b"sqls: []\n").unwrap();
        assert_eq!(v.sqls, Some(Vec::new()));
        let v: M = unmarshal(b"sqls:\n").unwrap();
        assert_eq!(v.sqls, None);
        let v: M = unmarshal(b"sqls: ~\n").unwrap();
        assert_eq!(v.sqls, None);
        let v: M = unmarshal(b"other: 1\n").unwrap();
        assert_eq!(v.sqls, None);
    }
}
