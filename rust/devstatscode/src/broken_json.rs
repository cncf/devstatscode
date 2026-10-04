//! Recovery of damaged GH Archive lines (Go `broken_json.go`).
//!
//! A GH Archive hour is a gzipped file of newline separated JSON events. A
//! corrupted hour (seen in `2023-05-14-19.json.gz`) can glue several events
//! into one line, separated/padded by NUL bytes, or contain undecodable
//! bytes. [`recover_json_chunks`] splits such a line into the well-formed
//! top-level JSON values it still contains plus the broken remainders, so a
//! single damaged event never costs the rest of the line (or the hour). Both
//! ports use exactly the same rules, so they store exactly the same events.

use serde::de::IgnoredAny;

/// A piece of a damaged line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonChunk<'a> {
    /// A syntactically well-formed top-level JSON value (it may still fail to
    /// decode as an event).
    Json(&'a [u8]),
    /// An undecodable remainder: not valid UTF-8, or a syntax error up to the
    /// end of its NUL-delimited segment.
    Broken(&'a [u8]),
}

impl<'a> JsonChunk<'a> {
    /// The chunk's bytes.
    pub fn bytes(&self) -> &'a [u8] {
        match self {
            JsonChunk::Json(b) | JsonChunk::Broken(b) => b,
        }
    }
}

/// True when `line` may take the fast path: it has no NUL byte and is valid
/// UTF-8, so a single strict decode decides about it. Go's `jsoniter` would
/// silently accept `<json>\0<anything>` (a literal NUL ends its token
/// scanning), dropping whatever follows the NUL — such lines must always go
/// through [`recover_json_chunks`].
pub fn is_plain_line(line: &[u8]) -> bool {
    !line.contains(&0) && std::str::from_utf8(line).is_ok()
}

/// Go `bytes.Trim(b, " \t\r\n")` (JSON whitespace only).
pub fn trim_json_ws(b: &[u8]) -> &[u8] {
    let is_ws = |c: &u8| matches!(*c, b' ' | b'\t' | b'\r' | b'\n');
    let start = b.iter().position(|c| !is_ws(c)).unwrap_or(b.len());
    let end = b.iter().rposition(|c| !is_ws(c)).map_or(start, |p| p + 1);
    &b[start..end.max(start)]
}

/// Splits `line` on runs of NUL bytes into trimmed, non-empty segments.
pub fn split_nul_segments(line: &[u8]) -> Vec<&[u8]> {
    line.split(|b| *b == 0)
        .map(trim_json_ws)
        .filter(|s| !s.is_empty())
        .collect()
}

/// Scans one NUL-free segment for consecutive top-level JSON values. Like
/// Go's `json.Decoder`, an object or array ends at its closing bracket while
/// a string, number or literal must be followed by whitespace or the end of
/// the segment. A syntax error ends the scan: the (trimmed) rest of the
/// segment, from the end of the last good value, is one broken chunk.
fn scan_json_values(seg: &[u8]) -> Vec<JsonChunk<'_>> {
    let mut chunks = Vec::new();
    let mut stream = serde_json::Deserializer::from_slice(seg).into_iter::<IgnoredAny>();
    loop {
        let start = stream.byte_offset().min(seg.len());
        match stream.next() {
            None => break,
            Some(Ok(_)) => {
                let end = stream.byte_offset().min(seg.len());
                let raw = trim_json_ws(&seg[start..end]);
                let self_delimited = matches!(raw.first(), Some(b'{') | Some(b'['));
                let glued = matches!(seg.get(end), Some(c) if !is_json_ws(*c));
                if raw.is_empty() || (!self_delimited && glued) {
                    push_broken(&mut chunks, &seg[start..]);
                    break;
                }
                chunks.push(JsonChunk::Json(raw));
            }
            Some(Err(_)) => {
                push_broken(&mut chunks, &seg[start..]);
                break;
            }
        }
    }
    chunks
}

fn is_json_ws(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\r' | b'\n')
}

fn push_broken<'a>(chunks: &mut Vec<JsonChunk<'a>>, rest: &'a [u8]) {
    let rest = trim_json_ws(rest);
    if !rest.is_empty() {
        chunks.push(JsonChunk::Broken(rest));
    }
}

/// Splits a damaged line into chunks: NUL runs separate segments, each
/// segment is trimmed of JSON whitespace, a segment that is not valid UTF-8
/// is one broken chunk, otherwise its consecutive top-level JSON values are
/// returned in order and a syntax error turns the rest of the segment into
/// one broken chunk. Nothing is ever merged across NULs or repaired: only
/// well-formed, unmodified values are offered for decoding.
pub fn recover_json_chunks(line: &[u8]) -> Vec<JsonChunk<'_>> {
    let mut chunks = Vec::new();
    for seg in split_nul_segments(line) {
        if std::str::from_utf8(seg).is_err() {
            chunks.push(JsonChunk::Broken(seg));
            continue;
        }
        chunks.extend(scan_json_values(seg));
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> &[u8] {
        s.as_bytes()
    }

    #[test]
    fn plain_line_detection() {
        assert!(is_plain_line(b(r#"{"id":"1"}"#)));
        assert!(is_plain_line(b("")));
        assert!(is_plain_line("{\"a\":\"zażółć\"}".as_bytes()));
        assert!(!is_plain_line(b"{\"id\":\"1\"}\0"));
        assert!(!is_plain_line(b"\0"));
        assert!(!is_plain_line(b"{\"a\":\"\xff\"}"));
    }

    #[test]
    fn trim_only_json_whitespace() {
        assert_eq!(trim_json_ws(b(" \t\r\n{}\n\r\t ")), b("{}"));
        assert_eq!(trim_json_ws(b("   ")), b(""));
        assert_eq!(trim_json_ws(b("")), b(""));
        assert_eq!(trim_json_ws(b("{}")), b("{}"));
        // Vertical tab, form feed and NBSP are not JSON whitespace.
        assert_eq!(trim_json_ws(b("\x0b{}\x0c")), b("\x0b{}\x0c"));
        assert_eq!(trim_json_ws("\u{a0}{}".as_bytes()), "\u{a0}{}".as_bytes());
    }

    #[test]
    fn nul_segments() {
        assert_eq!(split_nul_segments(b"a\0\0\0b"), vec![b("a"), b("b")]);
        assert_eq!(split_nul_segments(b"\0\0"), Vec::<&[u8]>::new());
        assert_eq!(split_nul_segments(b" \0 x \0\n"), vec![b("x")]);
        assert_eq!(split_nul_segments(b("abc")), vec![b("abc")]);
    }

    #[test]
    fn clean_line_is_one_json_chunk() {
        let line = br#"{"id":"1","type":"PushEvent"}"#;
        assert_eq!(recover_json_chunks(line), vec![JsonChunk::Json(&line[..])]);
        assert_eq!(
            recover_json_chunks(b"  {\"id\":\"1\"}\r\n"),
            vec![JsonChunk::Json(b("{\"id\":\"1\"}"))]
        );
    }

    #[test]
    fn empty_and_nul_only_lines() {
        assert!(recover_json_chunks(b"").is_empty());
        assert!(recover_json_chunks(b"\0\0\0").is_empty());
        assert!(recover_json_chunks(b" \t\0 \n").is_empty());
    }

    #[test]
    fn trailing_nuls_after_event() {
        let ev = br#"{"id":"29055979701","type":"IssueCommentEvent"}"#;
        let mut line = ev.to_vec();
        line.extend(std::iter::repeat_n(0u8, 1409));
        assert_eq!(recover_json_chunks(&line), vec![JsonChunk::Json(&ev[..])]);
    }

    #[test]
    fn event_nuls_event_is_recovered() {
        // The real 2023-05-14-19 damage: event + 1409 NULs + glued event.
        let e1 = br#"{"id":"29055979701","type":"IssueCommentEvent"}"#;
        let e2 =
            br#"{"id":"29056015615","type":"PushEvent","repo":{"name":"yowmamasita/anmeldung"}}"#;
        let mut line = e1.to_vec();
        line.extend(std::iter::repeat_n(0u8, 1409));
        line.extend_from_slice(e2);
        assert_eq!(
            recover_json_chunks(&line),
            vec![JsonChunk::Json(&e1[..]), JsonChunk::Json(&e2[..])]
        );
    }

    #[test]
    fn three_chunks_and_nuls_inside_event() {
        let e1 = br#"{"id":"1"}"#;
        let e3 = br#"{"id":"3"}"#;
        // The middle event is cut by NULs: both halves are broken, never merged.
        let line = b"{\"id\":\"1\"}\0\0{\"id\":\0\"2\"}\0{\"id\":\"3\"}";
        assert_eq!(
            recover_json_chunks(line),
            vec![
                JsonChunk::Json(&e1[..]),
                JsonChunk::Broken(b("{\"id\":")),
                JsonChunk::Broken(b("\"2\"}")),
                JsonChunk::Json(&e3[..]),
            ]
        );
    }

    #[test]
    fn glued_without_nuls() {
        let line = br#"{"id":"1"}{"id":"2"} {"id":"3"}"#;
        assert_eq!(
            recover_json_chunks(line),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Json(b("{\"id\":\"2\"}")),
                JsonChunk::Json(b("{\"id\":\"3\"}")),
            ]
        );
    }

    #[test]
    fn garbage_tail_and_head() {
        assert_eq!(
            recover_json_chunks(b("{\"id\":\"1\"}garbage")),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Broken(b("garbage"))
            ]
        );
        assert_eq!(
            recover_json_chunks(b("garbage{\"id\":\"1\"}")),
            vec![JsonChunk::Broken(b("garbage{\"id\":\"1\"}"))]
        );
        assert_eq!(
            recover_json_chunks(b("{\"id\":\"1\"}}")),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Broken(b("}"))
            ]
        );
    }

    #[test]
    fn truncated_event() {
        assert_eq!(
            recover_json_chunks(b("{\"id\":\"1\",\"type\":\"Pu")),
            vec![JsonChunk::Broken(b("{\"id\":\"1\",\"type\":\"Pu"))]
        );
        assert_eq!(
            recover_json_chunks(b("{\"id\":\"1\"}{\"id\":\"2\" ")),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Broken(b("{\"id\":\"2\""))
            ]
        );
        assert_eq!(
            recover_json_chunks(b("{\"id\":\"1\"} {")),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Broken(b("{"))
            ]
        );
    }

    #[test]
    fn invalid_utf8_segment_is_broken_whole() {
        let line = b"{\"id\":\"1\"}\0{\"a\":\"\xff\xfe\"}{\"id\":\"3\"}\0{\"id\":\"4\"}";
        assert_eq!(
            recover_json_chunks(line),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Broken(b"{\"a\":\"\xff\xfe\"}{\"id\":\"3\"}"),
                JsonChunk::Json(b("{\"id\":\"4\"}")),
            ]
        );
    }

    #[test]
    fn non_object_values_are_chunks_too() {
        assert_eq!(
            recover_json_chunks(b("[1,2] \"s\" 7 null {\"id\":\"1\"}")),
            vec![
                JsonChunk::Json(b("[1,2]")),
                JsonChunk::Json(b("\"s\"")),
                JsonChunk::Json(b("7")),
                JsonChunk::Json(b("null")),
                JsonChunk::Json(b("{\"id\":\"1\"}")),
            ]
        );
        assert_eq!(
            recover_json_chunks(b("123abc")),
            vec![JsonChunk::Broken(b("123abc"))]
        );
        // Go's json.Decoder: a string/number/literal must be followed by
        // whitespace or EOF, an object/array is self-delimiting.
        assert_eq!(
            recover_json_chunks(b("\"s\"{\"id\":\"1\"}")),
            vec![JsonChunk::Broken(b("\"s\"{\"id\":\"1\"}"))]
        );
        assert_eq!(
            recover_json_chunks(b("7{\"id\":\"1\"}")),
            vec![JsonChunk::Broken(b("7{\"id\":\"1\"}"))]
        );
        assert_eq!(
            recover_json_chunks(b("{\"id\":\"1\"}\"s\"{\"id\":\"2\"}")),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Broken(b("\"s\"{\"id\":\"2\"}")),
            ]
        );
        assert_eq!(
            recover_json_chunks(b("[1]2")),
            vec![JsonChunk::Json(b("[1]")), JsonChunk::Json(b("2"))]
        );
        assert_eq!(
            recover_json_chunks(b("{\"id\":\"1\"}\"s\"")),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Json(b("\"s\""))
            ]
        );
    }

    #[test]
    fn whitespace_around_nuls() {
        let line = b" \n{\"id\":\"1\"}\r\n\0\0 \t{\"id\":\"2\"}\n \0";
        assert_eq!(
            recover_json_chunks(line),
            vec![
                JsonChunk::Json(b("{\"id\":\"1\"}")),
                JsonChunk::Json(b("{\"id\":\"2\"}"))
            ]
        );
    }

    #[test]
    fn old_format_event_is_a_json_chunk() {
        let line = br#"{"repository":{"name":"rust","owner":"rust-lang"},"actor":"bors","type":"PushEvent","created_at":"2012/03/11 12:00:00 -0700"}"#;
        assert_eq!(recover_json_chunks(line), vec![JsonChunk::Json(&line[..])]);
    }

    #[test]
    fn chunk_bytes_accessor() {
        assert_eq!(JsonChunk::Json(b("a")).bytes(), b("a"));
        assert_eq!(JsonChunk::Broken(b("b")).bytes(), b("b"));
    }
}
