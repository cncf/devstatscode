//! The subset of Go's `net/url` used by the tools: query-component escaping
//! and `url.Values`-style query parsing with Go's exact error wording.

/// Go `url.EscapeError` text: `invalid URL escape "%zz"`.
fn escape_error(s: &[u8]) -> String {
    format!("invalid URL escape {:?}", String::from_utf8_lossy(s))
}

fn unhex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn unescape_bytes(s: &[u8], plus_is_space: bool) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'%' => {
                if i + 2 >= s.len() {
                    return Err(escape_error(&s[i..]));
                }
                match (unhex(s[i + 1]), unhex(s[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push(h << 4 | l);
                        i += 3;
                    }
                    _ => return Err(escape_error(&s[i..i + 3])),
                }
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(out)
}

/// Go `url.QueryUnescape` on raw bytes: `%XX` sequences are decoded and `+`
/// becomes a space; a malformed escape is reported like Go
/// (`invalid URL escape "%zz"`, showing at most the `%` and two more bytes).
pub fn query_unescape_bytes(s: &[u8]) -> Result<Vec<u8>, String> {
    unescape_bytes(s, true)
}

/// Go `url.PathUnescape` on raw bytes: like [`query_unescape_bytes`] but `+`
/// is kept.
pub fn path_unescape_bytes(s: &[u8]) -> Result<Vec<u8>, String> {
    unescape_bytes(s, false)
}

/// Go `url.QueryUnescape` for a string (the result is converted lossily when
/// the escapes do not form valid UTF-8 — Go strings carry raw bytes).
pub fn query_unescape(s: &str) -> Result<String, String> {
    query_unescape_bytes(s.as_bytes()).map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn should_escape(c: u8) -> bool {
    !(c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~'))
}

/// Go `url.QueryEscape`: everything except `A-Z a-z 0-9 - _ . ~` is
/// percent-encoded, spaces become `+`.
pub fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &c in s.as_bytes() {
        if c == b' ' {
            out.push('+');
        } else if should_escape(c) {
            out.push_str(&format!("%{:02X}", c));
        } else {
            out.push(c as char);
        }
    }
    out
}

/// One `key=value` pair of a query string (raw bytes, already unescaped).
pub type QueryPair = (Vec<u8>, Vec<u8>);

/// Go `url.ParseQuery`: `key=value` pairs separated by `&`, in order. Pairs
/// containing `;` and pairs with a malformed escape are skipped (Go records
/// the first such error, which callers like `Request.FormValue` ignore); the
/// error is returned alongside the successfully parsed pairs.
pub fn parse_query(query: &[u8]) -> (Vec<QueryPair>, Option<String>) {
    let mut pairs = Vec::new();
    let mut first_err = None;
    for raw in query.split(|&c| c == b'&') {
        if raw.is_empty() {
            continue;
        }
        if raw.contains(&b';') {
            if first_err.is_none() {
                first_err = Some("invalid semicolon separator in query".to_string());
            }
            continue;
        }
        let (k, v) = match raw.iter().position(|&c| c == b'=') {
            Some(p) => (&raw[..p], &raw[p + 1..]),
            None => (raw, &raw[raw.len()..]),
        };
        let key = match query_unescape_bytes(k) {
            Ok(k) => k,
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
                continue;
            }
        };
        let value = match query_unescape_bytes(v) {
            Ok(v) => v,
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
                continue;
            }
        };
        pairs.push((key, value));
    }
    (pairs, first_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unescape_basic() {
        assert_eq!(
            query_unescape("a+b%20c%7B%22x%22%7D").unwrap(),
            "a b c{\"x\"}"
        );
        assert_eq!(query_unescape("").unwrap(), "");
        assert_eq!(query_unescape("plain").unwrap(), "plain");
        assert_eq!(query_unescape("%41%6a").unwrap(), "Aj");
    }

    #[test]
    fn unescape_errors_like_go() {
        assert_eq!(
            query_unescape("abc%zzdef").unwrap_err(),
            "invalid URL escape \"%zz\""
        );
        assert_eq!(
            query_unescape("abc%").unwrap_err(),
            "invalid URL escape \"%\""
        );
        assert_eq!(
            query_unescape("abc%4").unwrap_err(),
            "invalid URL escape \"%4\""
        );
        assert_eq!(
            query_unescape("%4g").unwrap_err(),
            "invalid URL escape \"%4g\""
        );
    }

    #[test]
    fn path_unescape_keeps_plus() {
        assert_eq!(path_unescape_bytes(b"a+b%20c").unwrap(), b"a+b c");
        assert_eq!(
            path_unescape_bytes(b"%q").unwrap_err(),
            "invalid URL escape \"%q\""
        );
    }

    #[test]
    fn escape_like_go() {
        assert_eq!(query_escape("a b&c=d/e~f_g-h.i"), "a+b%26c%3Dd%2Fe~f_g-h.i");
        assert_eq!(query_escape("Żółw"), "%C5%BB%C3%B3%C5%82w");
        assert_eq!(query_unescape(&query_escape("x y+z%")).unwrap(), "x y+z%");
    }

    #[test]
    fn parse_query_pairs() {
        let (pairs, err) = parse_query(b"a=1&b=x+y&&c&d=%7B%7D&a=2");
        assert!(err.is_none());
        let strs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| {
                (
                    String::from_utf8(k.clone()).unwrap(),
                    String::from_utf8(v.clone()).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            strs,
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "x y".into()),
                ("c".into(), "".into()),
                ("d".into(), "{}".into()),
                ("a".into(), "2".into()),
            ]
        );
    }

    #[test]
    fn parse_query_skips_bad_pairs() {
        let (pairs, err) = parse_query(b"a=%zz&b=2;c=3&d=4");
        assert_eq!(err.unwrap(), "invalid URL escape \"%zz\"");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, b"d");
        assert_eq!(pairs[0].1, b"4");
        let (_, err) = parse_query(b"b=2;c=3&a=%zz");
        assert_eq!(err.unwrap(), "invalid semicolon separator in query");
    }
}
