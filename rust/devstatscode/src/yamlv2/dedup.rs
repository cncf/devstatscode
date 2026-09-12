//! Two `gopkg.in/yaml.v2` leniencies that `serde_yaml_ng` does not have:
//!
//! * **duplicate mapping keys** — yaml.v2 silently keeps the *last* value
//!   (`annotation_regexp: a` followed by `annotation_regexp: b` gives `b`; a
//!   repeated `projects.kpt:` mapping replaces the earlier one wholesale),
//!   serde rejects them (`duplicate field`/`duplicate entry`),
//! * **multi-document streams** — yaml.v2 `Unmarshal` decodes the first
//!   document and ignores the rest, `serde_yaml_ng` refuses the input.
//!
//! [`normalize`] re-parses the text with `saphyr-parser`, keeps the first
//! document only, expands aliases, drops all but the last value of every
//! repeated key (the key keeps its first position) and re-emits the result as
//! plain block YAML that `serde_yaml_ng` decodes as usual. Every scalar keeps its
//! exact text and its plain/quoted distinction, so the yaml.v2 coercions in
//! [`super::de`] see the same scalar kinds as in the original; tags are
//! re-emitted verbatim. [`super::de::unmarshal`] uses it only as a fallback
//! after `serde_yaml_ng` failed for exactly one of those two reasons
//! ([`applies`]), so every other error keeps its original text and position.

use std::collections::HashMap;
use std::fmt::Write as _;

use saphyr_parser::{Event, Parser, ScalarStyle, Tag};

#[derive(Debug, Clone)]
enum Node {
    Scalar {
        text: String,
        plain: bool,
        tag: Option<String>,
    },
    Seq {
        items: Vec<Node>,
        tag: Option<String>,
    },
    Map {
        entries: Vec<(Node, Node)>,
        tag: Option<String>,
    },
}

enum Frame {
    Seq {
        items: Vec<Node>,
        tag: Option<String>,
        anchor: usize,
    },
    Map {
        entries: Vec<(Node, Node)>,
        key: Option<Node>,
        tag: Option<String>,
        anchor: usize,
    },
}

/// Does this `serde_yaml_ng` error describe one of the two yaml.v2 leniencies?
pub fn applies(err: &str) -> bool {
    err.contains("duplicate field `")
        || err.contains("duplicate entry ")
        || err.contains("more than one document")
}

/// First document of `data`, aliases expanded, duplicate keys collapsed to their
/// last value, as block-style YAML. Errors for input this pass cannot handle
/// (invalid YAML, non-scalar mapping keys); the caller then keeps the original
/// `serde_yaml_ng` error.
pub fn normalize(data: &[u8]) -> Result<String, String> {
    let text = std::str::from_utf8(data).map_err(|e| e.to_string())?;
    let mut parser = Parser::new_from_str(text);
    let mut stack: Vec<Frame> = Vec::new();
    let mut anchors: HashMap<usize, Node> = HashMap::new();
    let mut root: Option<Node> = None;
    while let Some(item) = parser.next_event() {
        let (event, _span) = item.map_err(|e| e.to_string())?;
        match event {
            Event::Nothing | Event::StreamStart | Event::DocumentStart(_) => {}
            // yaml.v2 decodes the first document only
            Event::StreamEnd | Event::DocumentEnd => break,
            Event::Alias(id) => {
                let node = anchors
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| format!("alias to unknown anchor {id}"))?;
                attach(&mut stack, &mut anchors, &mut root, node, 0)?;
            }
            Event::Scalar(value, style, anchor, tag) => {
                let node = Node::Scalar {
                    text: value.into_owned(),
                    plain: style == ScalarStyle::Plain,
                    tag: tag.map(|t| render_tag(&t)),
                };
                attach(&mut stack, &mut anchors, &mut root, node, anchor)?;
            }
            Event::SequenceStart(anchor, tag) => stack.push(Frame::Seq {
                items: Vec::new(),
                tag: tag.map(|t| render_tag(&t)),
                anchor,
            }),
            Event::SequenceEnd => match stack.pop() {
                Some(Frame::Seq { items, tag, anchor }) => {
                    attach(
                        &mut stack,
                        &mut anchors,
                        &mut root,
                        Node::Seq { items, tag },
                        anchor,
                    )?;
                }
                _ => return Err("unexpected end of sequence".to_string()),
            },
            Event::MappingStart(anchor, tag) => stack.push(Frame::Map {
                entries: Vec::new(),
                key: None,
                tag: tag.map(|t| render_tag(&t)),
                anchor,
            }),
            Event::MappingEnd => match stack.pop() {
                Some(Frame::Map {
                    entries,
                    key: None,
                    tag,
                    anchor,
                }) => {
                    let node = Node::Map {
                        entries: dedup(entries)?,
                        tag,
                    };
                    attach(&mut stack, &mut anchors, &mut root, node, anchor)?;
                }
                _ => return Err("unexpected end of mapping".to_string()),
            },
        }
    }
    if !stack.is_empty() {
        return Err("unterminated collection".to_string());
    }
    let mut out = String::new();
    if let Some(node) = root {
        emit_root(&mut out, &node);
    }
    Ok(out)
}

fn attach(
    stack: &mut [Frame],
    anchors: &mut HashMap<usize, Node>,
    root: &mut Option<Node>,
    node: Node,
    anchor: usize,
) -> Result<(), String> {
    if anchor != 0 {
        anchors.insert(anchor, node.clone());
    }
    match stack.last_mut() {
        None => {
            if root.is_some() {
                return Err("more than one root node".to_string());
            }
            *root = Some(node);
        }
        Some(Frame::Seq { items, .. }) => items.push(node),
        Some(Frame::Map { entries, key, .. }) => match key.take() {
            None => *key = Some(node),
            Some(k) => entries.push((k, node)),
        },
    }
    Ok(())
}

/// yaml.v2: for a repeated key the last value wins.
fn dedup(entries: Vec<(Node, Node)>) -> Result<Vec<(Node, Node)>, String> {
    let mut out: Vec<(Node, Node)> = Vec::with_capacity(entries.len());
    let mut index: HashMap<String, usize> = HashMap::new();
    for (key, value) in entries {
        let Node::Scalar { text, .. } = &key else {
            return Err("non-scalar mapping key".to_string());
        };
        match index.get(text) {
            Some(&i) => out[i].1 = value,
            None => {
                index.insert(text.clone(), out.len());
                out.push((key, value));
            }
        }
    }
    Ok(out)
}

fn render_tag(tag: &Tag) -> String {
    if tag.handle == "tag:yaml.org,2002:" {
        format!("!!{}", tag.suffix)
    } else if tag.handle == "!" {
        format!("!{}", tag.suffix)
    } else if tag.handle.is_empty() && tag.suffix == "!" {
        "!".to_string()
    } else {
        format!("!<{}{}>", tag.handle, tag.suffix)
    }
}

fn emit_root(out: &mut String, node: &Node) {
    match node {
        Node::Scalar { text, plain, tag } => {
            if let Some(t) = tag {
                out.push_str(t);
                out.push(' ');
            }
            if !(*plain && text.is_empty() && tag.is_none()) {
                emit_scalar(out, text, *plain);
            }
            out.push('\n');
        }
        Node::Seq { items, tag } => {
            if let Some(t) = tag {
                out.push_str(t);
                out.push(if items.is_empty() { ' ' } else { '\n' });
            }
            if items.is_empty() {
                out.push_str("[]\n");
            } else {
                emit_block(out, node, 0);
            }
        }
        Node::Map { entries, tag } => {
            if let Some(t) = tag {
                out.push_str(t);
                out.push(if entries.is_empty() { ' ' } else { '\n' });
            }
            if entries.is_empty() {
                out.push_str("{}\n");
            } else {
                emit_block(out, node, 0);
            }
        }
    }
}

/// Non-empty collection as an indented block, one entry per line.
fn emit_block(out: &mut String, node: &Node, indent: usize) {
    match node {
        Node::Seq { items, .. } => {
            for item in items {
                pad(out, indent);
                out.push('-');
                emit_value(out, item, indent + 2);
            }
        }
        Node::Map { entries, .. } => {
            for (key, value) in entries {
                pad(out, indent);
                emit_key(out, key);
                out.push(':');
                emit_value(out, value, indent + 2);
            }
        }
        Node::Scalar { .. } => {
            pad(out, indent);
            emit_value(out, node, indent);
        }
    }
}

/// What follows a `-` or a `key:` — the value and the line break, or a nested
/// block on the following lines.
fn emit_value(out: &mut String, node: &Node, child_indent: usize) {
    match node {
        Node::Scalar { text, plain, tag } => {
            if let Some(t) = tag {
                out.push(' ');
                out.push_str(t);
            }
            if !(*plain && text.is_empty()) {
                out.push(' ');
                emit_scalar(out, text, *plain);
            }
            out.push('\n');
        }
        Node::Seq { items, tag } => {
            if let Some(t) = tag {
                out.push(' ');
                out.push_str(t);
            }
            if items.is_empty() {
                out.push_str(" []\n");
            } else {
                out.push('\n');
                emit_block(out, node, child_indent);
            }
        }
        Node::Map { entries, tag } => {
            if let Some(t) = tag {
                out.push(' ');
                out.push_str(t);
            }
            if entries.is_empty() {
                out.push_str(" {}\n");
            } else {
                out.push('\n');
                emit_block(out, node, child_indent);
            }
        }
    }
}

fn emit_key(out: &mut String, key: &Node) {
    if let Node::Scalar { text, plain, tag } = key {
        if let Some(t) = tag {
            out.push_str(t);
            out.push(' ');
        }
        if *plain && text.is_empty() {
            out.push('~');
        } else {
            emit_scalar(out, text, *plain);
        }
    }
}

/// A plain scalar is re-emitted verbatim (it was valid plain text in the
/// original, block context is at least as permissive as flow context); values
/// that were quoted or block scalars, and plain scalars with line breaks or
/// control characters, are written double-quoted.
fn emit_scalar(out: &mut String, text: &str, plain: bool) {
    if plain && !text.is_empty() && !text.chars().any(|c| c.is_control()) {
        out.push_str(text);
    } else {
        double_quoted(out, text);
    }
}

fn double_quoted(out: &mut String, text: &str) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\u{85}' => out.push_str("\\N"),
            '\u{a0}' => out.push_str("\\_"),
            '\u{2028}' => out.push_str("\\L"),
            '\u{2029}' => out.push_str("\\P"),
            '\u{feff}' => out.push_str("\\uFEFF"),
            c if (c as u32) < 0x20 || (0x7f..=0x9f).contains(&(c as u32)) => {
                let _ = write!(out, "\\x{:02X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn pad(out: &mut String, n: usize) {
    for _ in 0..n {
        out.push(' ');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(s: &str) -> String {
        normalize(s.as_bytes()).unwrap()
    }

    #[test]
    fn last_duplicate_wins_keeping_first_position() {
        assert_eq!(
            norm("a: 1\nb: 2\na: 3\n"),
            "a: 3\nb: 2\n",
            "scalar duplicates"
        );
        assert_eq!(
            norm("projects:\n  kpt:\n    order: 1\n    name: a\n  other:\n    order: 2\n  kpt:\n    name: b\n"),
            "projects:\n  kpt:\n    name: b\n  other:\n    order: 2\n",
            "a repeated mapping replaces the earlier one, no merge"
        );
        assert_eq!(
            norm("x:\n  a: 1\n  a: 2\ny: [1, 2, 2]\n"),
            "x:\n  a: 2\ny:\n  - 1\n  - 2\n  - 2\n",
            "nested mappings are deduplicated, sequences are not"
        );
    }

    #[test]
    fn first_document_only() {
        assert_eq!(norm("---\na: 1\n---\na: 2\n"), "a: 1\n");
        assert_eq!(
            norm("a: 1\n...\n---\nb: [\n"),
            "a: 1\n",
            "the rest is not even parsed"
        );
        assert_eq!(norm(""), "");
        // saphyr reports an empty document as a plain `~` — null either way
        assert_eq!(norm("---\n"), "~\n");
    }

    #[test]
    fn scalar_kinds_survive() {
        // plain stays plain (null, ints, hex, floats, yes/no keep their text;
        // an empty value is reported by saphyr as the plain `~`, null as well)
        assert_eq!(
            norm("a:\nb: ~\nc: 0x1F\nd: 1_000\ne: yes\nf: 1e3\ng: -.inf\nh: 2001-12-14\n"),
            "a: ~\nb: ~\nc: 0x1F\nd: 1_000\ne: yes\nf: 1e3\ng: -.inf\nh: 2001-12-14\n"
        );
        // quoted / block scalars become double-quoted with the same content
        assert_eq!(
            norm("a: '1'\nb: \"x\\ty\"\nc: |\n  l1\n  l2\nd: >-\n  f1\n  f2\ne: ''\nf: 'it''s'\n"),
            "a: \"1\"\nb: \"x\\ty\"\nc: \"l1\\nl2\\n\"\nd: \"f1 f2\"\ne: \"\"\nf: \"it's\"\n"
        );
        // multi-line plain scalars (blank line = newline) get quoted too
        assert_eq!(norm("a: x\n\n  y\nb: p\n  q\n"), "a: \"x\\ny\"\nb: p q\n");
        // tags are kept
        assert_eq!(
            norm("a: !!str 1\nb: !foo bar\nc: !!binary |\n  aGk=\n"),
            "a: !!str 1\nb: !foo bar\nc: !!binary \"aGk=\\n\"\n"
        );
        // flow collections, nesting, empties
        assert_eq!(
            norm("a: [x, {b: c, b: d}, [], {}]\ne: {f: [1, 2]}\n"),
            "a:\n  - x\n  -\n    b: d\n  - []\n  - {}\ne:\n  f:\n    - 1\n    - 2\n"
        );
        // keys: quoted, numeric, null
        assert_eq!(
            norm("'a b': 1\n2: two\n~: n\n\"\": e\n"),
            "\"a b\": 1\n2: two\n~: n\n\"\": e\n"
        );
        // root scalar / root sequence
        assert_eq!(norm("just text\n"), "just text\n");
        assert_eq!(norm("- 1\n- a: b\n  a: c\n"), "- 1\n-\n  a: c\n");
    }

    #[test]
    fn aliases_are_expanded() {
        assert_eq!(
            norm("base: &b\n  x: 1\n  y: 2\nother: *b\nlist: [&s 5, *s]\n"),
            "base:\n  x: 1\n  y: 2\nother:\n  x: 1\n  y: 2\nlist:\n  - 5\n  - 5\n"
        );
    }

    #[test]
    fn unsupported_input_is_an_error() {
        assert!(normalize(b"? [a, b]\n: c\n").is_err(), "complex keys");
        assert!(normalize(b"a: [\n").is_err(), "invalid YAML");
        assert!(normalize(b"a: *nope\n").is_err(), "unknown alias");
        assert!(normalize(&[0xff, 0xfe]).is_err(), "not UTF-8");
    }

    #[test]
    fn applies_only_to_the_two_leniencies() {
        assert!(applies(
            "yaml: duplicate field `annotation_regexp` at line 2 column 5"
        ));
        assert!(applies("duplicate entry with key \"a\""));
        assert!(applies(
            "deserializing from YAML containing more than one document is not supported"
        ));
        assert!(!applies(
            "invalid type: string \"a\", expected a YAML integer at line 1 column 4"
        ));
        assert!(!applies("did not find expected key at line 3 column 1"));
    }
}
