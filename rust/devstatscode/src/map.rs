//! String slice/set helpers — port of `map.go`.

use std::collections::{BTreeMap, BTreeSet};

/// Go's `strings.Split("", ",")` yields `[""]`; DevStats wants an empty list
/// in that case. Any other input is returned unchanged.
pub fn skip_empty(arr: Vec<String>) -> Vec<String> {
    if arr.len() == 1 && arr[0].is_empty() {
        return Vec::new();
    }
    arr
}

/// Apply `f` to every element (after [`skip_empty`]).
pub fn strings_map_to_array<F: Fn(&str) -> String>(f: F, arr: Vec<String>) -> Vec<String> {
    skip_empty(arr).iter().map(|s| f(s)).collect()
}

/// Apply `f` to every element (after [`skip_empty`]) and collect into a set.
pub fn strings_map_to_set<F: Fn(&str) -> String>(f: F, arr: Vec<String>) -> BTreeSet<String> {
    skip_empty(arr).iter().map(|s| f(s)).collect()
}

/// Sorted keys of a string set.
pub fn strings_set_keys(set: &BTreeSet<String>) -> Vec<String> {
    set.iter().cloned().collect()
}

/// Parse a Go-formatted map `map[k1:v1 k2:v2]` back into a map. Returns `None`
/// for malformed input or when no `k:v` pair is present (Go returns `nil`).
pub fn map_from_string(s: &str) -> Option<BTreeMap<String, String>> {
    if s.len() < 6 || !s.starts_with("map[") || !s.ends_with(']') {
        return None;
    }
    let inner = &s[4..s.len() - 1];
    let mut res: Option<BTreeMap<String, String>> = None;
    for data in inner.split(' ') {
        let parts: Vec<&str> = data.split(':').collect();
        if parts.len() == 2 {
            res.get_or_insert_with(BTreeMap::new)
                .insert(parts[0].to_string(), parts[1].to_string());
        }
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn map_from_string_table() {
        let m = |pairs: &[(&str, &str)]| -> Option<BTreeMap<String, String>> {
            Some(
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
        };
        assert_eq!(map_from_string(""), None);
        assert_eq!(map_from_string("map[]"), None);
        assert_eq!(map_from_string("map[:]"), m(&[("", "")]));
        assert_eq!(map_from_string("map[a:]"), m(&[("a", "")]));
        assert_eq!(map_from_string("map[:b]"), m(&[("", "b")]));
        assert_eq!(map_from_string("map[a:b]"), m(&[("a", "b")]));
        assert_eq!(map_from_string("map[a:b c:d"), None);
        assert_eq!(map_from_string("map a:b c:d]"), None);
        assert_eq!(
            map_from_string("map[a:b c:d]"),
            m(&[("a", "b"), ("c", "d")])
        );
    }

    #[test]
    fn skip_empty_table() {
        assert_eq!(skip_empty(v(&[])), v(&[]));
        assert_eq!(skip_empty(v(&[""])), v(&[]));
        assert_eq!(skip_empty(v(&[" "])), v(&[" "]));
        assert_eq!(skip_empty(v(&["a"])), v(&["a"]));
        assert_eq!(skip_empty(v(&["", ""])), v(&["", ""]));
        assert_eq!(skip_empty(v(&["", "a"])), v(&["", "a"]));
        assert_eq!(skip_empty(v(&["a", "b"])), v(&["a", "b"]));
    }

    #[test]
    fn map_to_array_and_set() {
        let lower = |s: &str| s.to_lowercase();
        assert_eq!(strings_map_to_array(lower, v(&[])), v(&[]));
        assert_eq!(strings_map_to_array(lower, v(&["A"])), v(&["a"]));
        assert_eq!(
            strings_map_to_array(lower, v(&["A", "b", "Cd"])),
            v(&["a", "b", "cd"])
        );
        let strip = |s: &str| s.trim().to_string();
        assert!(strings_map_to_set(strip, v(&[])).is_empty());
        assert_eq!(
            strings_map_to_set(strip, v(&[" a\n\t"])),
            BTreeSet::from(["a".to_string()])
        );
        assert_eq!(
            strings_map_to_set(strip, v(&["a ", " b", "\tc\t", "d e"])),
            ["a", "b", "c", "d e"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    #[test]
    fn set_keys_sorted() {
        assert_eq!(strings_set_keys(&BTreeSet::new()), v(&[]));
        assert_eq!(
            strings_set_keys(&BTreeSet::from(["xyz".to_string()])),
            v(&["xyz"])
        );
        let set: BTreeSet<String> = ["b", "a", "c"].iter().map(|s| s.to_string()).collect();
        assert_eq!(strings_set_keys(&set), v(&["a", "b", "c"]));
    }
}
