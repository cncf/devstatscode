//! FNV-1a based hashing — port of `hash.go`.
//!
//! These hashes end up as *artificial event ids* and object keys in the
//! database, so they must be bit-identical to the Go implementation
//! (verified by the tests below against values produced by Go).

use std::collections::BTreeMap;

use crate::error::fatalf;
use crate::gofmt;

const FNV64_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// 64-bit FNV-1a of `data`.
pub fn fnv64a(data: &[u8]) -> u64 {
    let mut h = FNV64_OFFSET;
    for b in data {
        h ^= u64::from(*b);
        h = h.wrapping_mul(FNV64_PRIME);
    }
    h
}

/// Unique (negative) hash for a list of strings, used as an artificially
/// generated id. `i64::MIN` is avoided by re-hashing with an extra `"a"`.
pub fn hash_strings(strs: &[&str]) -> i64 {
    let joined: String = strs.concat();
    let mut res = fnv64a(joined.as_bytes()) as i64;
    if res > 0 {
        res = -res;
    }
    if res == i64::MIN {
        let mut more: Vec<&str> = strs.to_vec();
        more.push("a");
        return hash_strings(&more);
    }
    res
}

/// Go `NegativeArtificialID`: deterministic negative event id for
/// API/git-restored objects with no natural id.
pub fn negative_artificial_id(parts: &[&str]) -> i64 {
    let mut id = hash_strings(parts);
    if id > 0 {
        id = -id;
    }
    if id == 0 {
        id = -1;
    }
    id
}

/// Base-36 string of a `u64` (Go `strconv.FormatUint(v, 36)`).
pub fn format_base36(mut v: u64) -> String {
    if v == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::new();
    while v > 0 {
        buf.push(DIGITS[(v % 36) as usize]);
        v /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).expect("ascii")
}

/// Hash (base 36) of the Go-`%v`-formatted values of `keys` taken from a JSON
/// object; a missing key is fatal.
pub fn hash_object(iv: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> String {
    let mut s = String::new();
    for key in keys {
        match iv.get(*key) {
            Some(v) => s.push_str(&gofmt::json_value(v)),
            None => {
                let sorted: BTreeMap<&String, &serde_json::Value> = iv.iter().collect();
                let parts: Vec<String> = sorted
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, gofmt::json_value(v)))
                    .collect();
                fatalf(format_args!(
                    "HashObject: map[{}] missing {} key",
                    parts.join(" "),
                    key
                ))
            }
        }
    }
    format_base36(fnv64a(s.as_bytes()))
}

/// Hash (base 36) of the Go-`%v`-formatted values of an array.
pub fn hash_array(ia: &[serde_json::Value]) -> String {
    let s: String = ia.iter().map(gofmt::json_value).collect();
    format_base36(fnv64a(s.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn negative_artificial_id_is_negative_hash() {
        let id = negative_artificial_id(&["PushEvent", "org/repo", "abc"]);
        assert!(id < 0);
        assert_eq!(id, hash_strings(&["PushEvent", "org/repo", "abc"]));
        assert_eq!(id, negative_artificial_id(&["PushEvent", "org/repoabc"]));
    }

    #[test]
    fn hash_strings_matches_go() {
        assert_eq!(hash_strings(&[]), -3750763034362895579);
        assert_eq!(hash_strings(&["a", "b"]), -620445648566982762);
        assert_eq!(
            hash_strings(&["kubernetes/kubernetes", "2017-01-01 12:00:00", "PushEvent"]),
            -3426352738442265477
        );
        assert_eq!(hash_strings(&["xyz"]), -4614876691646974944);
    }

    #[test]
    fn hash_array_matches_go() {
        assert_eq!(hash_array(&[]), "33niihzj4ux45");
        assert_eq!(
            hash_array(&[json!("a"), json!(1.0), json!(2.5), json!(true), json!(null)]),
            "10995ajzeyx09"
        );
        assert_eq!(
            hash_array(&[
                json!("kubernetes"),
                json!(1234567.0),
                json!("2017-01-01 12:00:00")
            ]),
            "c6tuy8ekec1w"
        );
        assert_eq!(
            hash_array(&[json!({"b": 1.0, "a": ["x", 2.0]})]),
            "3qz52gt3weza3"
        );
    }

    #[test]
    fn hash_object_uses_keys_in_order() {
        let obj = json!({"id": 12.0, "login": "someone", "extra": true});
        let m = obj.as_object().unwrap();
        assert_eq!(
            hash_object(m, &["id", "login"]),
            hash_array(&[json!(12.0), json!("someone")])
        );
        assert_ne!(
            hash_object(m, &["login", "id"]),
            hash_object(m, &["id", "login"])
        );
    }

    #[test]
    fn base36() {
        assert_eq!(format_base36(0), "0");
        assert_eq!(format_base36(35), "z");
        assert_eq!(format_base36(36), "10");
        assert_eq!(format_base36(u64::MAX), "3w5e11264sgsf");
    }
}
