//! `tsplit` — re-flow a CNCF projects HTML table into sections of `SIZE` items.
//!
//! Input (stdin) is a fragment of `partials/projects.html` (see
//! `cncf/devstats` GRADUATING.md / ADDING_NEW_PROJECTS.md): a `<tr>` with one
//! `<td>` link per project, followed by a `<tr>` with one `<td class="cncf-bb">`
//! logo (`<img class="cncf-proj">`) per project, possibly repeated and with
//! `colspan` separator rows in between. Output (stdout) is the same set of
//! projects re-split into sections of `SIZE` columns, each section preceded by
//! a `<td colspan=N class="cncf-sep">KIND</td>` separator row, with the
//! left/right border classes (`cncf-bl` / `cncf-br`) recomputed.
//!
//! Environment:
//! * `KIND` — section label, e.g. `Graduated|Incubating|Sandbox` (required)
//! * `SIZE` — number of columns per section, a positive integer (required)
//! * `DEBUG` — when non-empty, print section/link/image diagnostics on stderr
//!
//! Usage: `KIND=Graduated SIZE=12 tsplit < graduated.txt > new_graduated.txt`

use std::env;
use std::fmt;
use std::io::{self, Read, Write};
use std::process::ExitCode;

/// Lines containing any of these are structural and never carry a project.
const SKIP_MARKERS: [&str; 3] = ["<tr>", "</tr>", "colspan"];
/// Marker distinguishing a logo line from a link line.
const IMAGE_MARKER: &str = r#"class="cncf-proj""#;

/// Error returned when the input is not a well-formed links+logos table.
#[derive(Debug, PartialEq, Eq)]
pub enum TsplitError {
    /// Every project needs exactly one link line and one logo line.
    LinkImageMismatch { links: usize, images: usize },
}

impl fmt::Display for TsplitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TsplitError::LinkImageMismatch { links, images } => write!(
                f,
                "number of link lines ({links}) differs from number of image lines ({images})"
            ),
        }
    }
}

impl std::error::Error for TsplitError {}

/// Replace, in order, each `(from, to)` pair — the link line normaliser strips
/// any previously computed border classes from `<td>`.
fn strip_link_borders(line: &str) -> String {
    let mut s = line.to_string();
    for from in [
        r#" class="cncf-bl""#,
        r#" class="cncf-br""#,
        r#" class="cncf-bl cncf-br""#,
        r#" class="cncf-br cncf-bl""#,
    ] {
        s = s.replace(from, "");
    }
    s
}

/// Reset a logo `<td>` to the bare bottom-border class.
fn strip_image_borders(line: &str) -> String {
    let mut s = line.to_string();
    for from in [
        r#"class="cncf-bb cncf-bl""#,
        r#"class="cncf-bb cncf-br""#,
        r#"class="cncf-bb cncf-bl cncf-br""#,
        r#"class="cncf-bb cncf-br cncf-bl""#,
    ] {
        s = s.replace(from, r#"class="cncf-bb""#);
    }
    s
}

/// Border class(es) for column `i` of a section spanning `from..to`.
fn border_class(i: usize, from: usize, last: usize) -> &'static str {
    match (i == from, i == last) {
        (true, true) => "cncf-bl cncf-br",
        (true, false) => "cncf-bl",
        (false, true) => "cncf-br",
        (false, false) => "",
    }
}

/// Core transformation. `size` must be >= 1 (validated by `main`).
pub fn tsplit(size: usize, kind: &str, input: &str, dbg: bool) -> Result<String, TsplitError> {
    assert!(size >= 1, "size must be positive");
    let lines: Vec<&str> = input.split('\n').filter(|l| !l.trim().is_empty()).collect();

    // Indentation of the first `<tr>` line is reused for generated rows.
    let offset = lines
        .iter()
        .find(|l| l.trim() == "<tr>")
        .and_then(|l| l.find("<tr>").filter(|&off| off > 0).map(|off| &l[..off]))
        .unwrap_or("");

    let (image_lines, link_lines): (Vec<&str>, Vec<&str>) = lines
        .iter()
        .copied()
        .filter(|l| !SKIP_MARKERS.iter().any(|m| l.contains(m)))
        .partition(|l| l.contains(IMAGE_MARKER));

    let link_lines: Vec<String> = link_lines.iter().map(|l| strip_link_borders(l)).collect();
    let image_lines: Vec<String> = image_lines.iter().map(|l| strip_image_borders(l)).collect();

    let n_items = image_lines.len();
    if link_lines.len() != n_items {
        return Err(TsplitError::LinkImageMismatch {
            links: link_lines.len(),
            images: n_items,
        });
    }

    let mut out: Vec<String> = Vec::new();
    for (section, chunk) in image_lines.chunks(size).enumerate() {
        let from = section * size;
        let to = from + chunk.len();
        let n = chunk.len();
        if dbg {
            eprintln!("section {section}: {from}-{to} ({n} items)");
        }
        let last = to - 1;
        out.push(format!("{offset}<tr>"));
        out.push(format!(
            r#"{offset}  <td colspan="{n}" class="cncf-sep">{kind}</td>"#
        ));
        out.push(format!("{offset}</tr>"));
        out.push(format!("{offset}<tr>"));
        for (i, line) in link_lines.iter().enumerate().take(to).skip(from) {
            let cls = border_class(i, from, last);
            out.push(if cls.is_empty() {
                line.clone()
            } else {
                line.replace("<td>", &format!(r#"<td class="{cls}">"#))
            });
        }
        out.push(format!("{offset}</tr>"));
        out.push(format!("{offset}<tr>"));
        for (i, line) in image_lines.iter().enumerate().take(to).skip(from) {
            let cls = border_class(i, from, last);
            out.push(if cls.is_empty() {
                line.clone()
            } else {
                line.replace(
                    r#"<td class="cncf-bb">"#,
                    &format!(r#"<td class="cncf-bb {cls}">"#),
                )
            });
        }
        out.push(format!("{offset}</tr>"));
    }
    if dbg {
        eprintln!("Links {}:\n{}", link_lines.len(), link_lines.join("\n"));
        eprintln!("Images {}:\n{}", image_lines.len(), image_lines.join("\n"));
    }
    Ok(out.join("\n"))
}

fn run() -> Result<(), String> {
    let kind = env::var("KIND").unwrap_or_default();
    if kind.is_empty() {
        return Err("You need to specify kind via KIND=Graduated|Incubating|Sandbox".into());
    }
    let ssize = env::var("SIZE").unwrap_or_default();
    if ssize.is_empty() {
        return Err("You need to specify size via SIZE=n (usually 9, 10, 11, 12)".into());
    }
    let size: i64 = ssize
        .parse()
        .map_err(|e| format!("error: invalid SIZE {ssize:?}: {e}"))?;
    if size < 1 {
        return Err(format!("error: SIZE must be positive, got {size}"));
    }
    let mut data = Vec::new();
    io::stdin()
        .read_to_end(&mut data)
        .map_err(|e| format!("error: reading stdin: {e}"))?;
    let input = String::from_utf8_lossy(&data);
    let dbg = env::var_os("DEBUG").is_some_and(|v| !v.is_empty());
    let out = tsplit(size as usize, &kind, &input, dbg).map_err(|e| format!("error: {e}"))?;
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(out.as_bytes())
        .and_then(|_| stdout.write_all(b"\n"))
        .map_err(|e| format!("error: writing stdout: {e}"))
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("{msg}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(name: &str, cls: &str) -> String {
        let td = if cls.is_empty() {
            "<td>".to_string()
        } else {
            format!(r#"<td class="{cls}">"#)
        };
        format!(
            r#"        {td}<a href="https://{name}.[[hostname]]" target="_blank">{name}</a></td>"#
        )
    }

    fn img(name: &str, cls: &str) -> String {
        format!(
            r#"        <td class="{cls}"><a href="https://{name}.[[hostname]]" target="_blank"><img class="cncf-proj" src="public/img/projects/{name}.svg" alt="{name}" height="80" width="80" /></a></td>"#
        )
    }

    /// Build a realistic input table: one section of `names` with borders already set.
    fn table(names: &[&str]) -> String {
        let n = names.len();
        let mut s = String::new();
        s += "    <tr>\n";
        s += &format!(r#"      <td colspan="{n}" class="cncf-sep">Graduated</td>"#);
        s += "\n    </tr>\n    <tr>\n";
        for (i, name) in names.iter().enumerate() {
            let cls = border_class(i, 0, n - 1);
            s += &link(name, cls);
            s += "\n";
        }
        s += "    </tr>\n    <tr>\n";
        for (i, name) in names.iter().enumerate() {
            let cls = border_class(i, 0, n - 1);
            let cls = if cls.is_empty() {
                "cncf-bb".to_string()
            } else {
                format!("cncf-bb {cls}")
            };
            s += &img(name, &cls);
            s += "\n";
        }
        s += "    </tr>\n";
        s
    }

    #[test]
    fn splits_into_sections_with_borders() {
        let input = table(&["a", "b", "c", "d", "e"]);
        let out = tsplit(2, "Graduated", &input, false).unwrap();
        let expected = [
            "    <tr>",
            r#"      <td colspan="2" class="cncf-sep">Graduated</td>"#,
            "    </tr>",
            "    <tr>",
            &link("a", "cncf-bl"),
            &link("b", "cncf-br"),
            "    </tr>",
            "    <tr>",
            &img("a", "cncf-bb cncf-bl"),
            &img("b", "cncf-bb cncf-br"),
            "    </tr>",
            "    <tr>",
            r#"      <td colspan="2" class="cncf-sep">Graduated</td>"#,
            "    </tr>",
            "    <tr>",
            &link("c", "cncf-bl"),
            &link("d", "cncf-br"),
            "    </tr>",
            "    <tr>",
            &img("c", "cncf-bb cncf-bl"),
            &img("d", "cncf-bb cncf-br"),
            "    </tr>",
            "    <tr>",
            r#"      <td colspan="1" class="cncf-sep">Graduated</td>"#,
            "    </tr>",
            "    <tr>",
            &link("e", "cncf-bl cncf-br"),
            "    </tr>",
            "    <tr>",
            &img("e", "cncf-bb cncf-bl cncf-br"),
            "    </tr>",
        ]
        .join("\n");
        assert_eq!(out, expected);
    }

    #[test]
    fn is_idempotent_for_same_size() {
        let input = table(&["a", "b", "c", "d", "e", "f", "g"]);
        let once = tsplit(3, "Sandbox", &input, false).unwrap();
        let twice = tsplit(3, "Sandbox", &once, false).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn resplitting_with_a_larger_size_merges_sections() {
        let names = ["a", "b", "c", "d", "e", "f"];
        let input = table(&names);
        let small = tsplit(2, "Incubating", &input, false).unwrap();
        let merged = tsplit(6, "Incubating", &small, false).unwrap();
        let direct = tsplit(6, "Incubating", &input, false).unwrap();
        assert_eq!(merged, direct);
        assert!(merged.contains(r#"<td colspan="6" class="cncf-sep">Incubating</td>"#));
        assert_eq!(merged.matches("cncf-sep").count(), 1);
    }

    #[test]
    fn middle_columns_have_no_border_classes() {
        let input = table(&["a", "b", "c"]);
        let out = tsplit(3, "K", &input, false).unwrap();
        assert!(out.contains(&link("b", "")));
        assert!(out.contains(&img("b", "cncf-bb")));
    }

    #[test]
    fn keeps_indentation_of_first_tr() {
        let input = table(&["a", "b"]).replace("    <tr>", "\t<tr>");
        let out = tsplit(1, "K", &input, false).unwrap();
        assert!(out.starts_with("\t<tr>\n\t  <td colspan=\"1\""));
    }

    #[test]
    fn blank_lines_and_windows_line_endings_are_tolerated() {
        let input = table(&["a", "b"]).replace('\n', "\r\n\r\n");
        let out = tsplit(2, "K", &input, false).unwrap();
        // "\r" stays glued to the line content; nothing is lost.
        assert_eq!(out.matches("cncf-proj").count(), 2);
        assert_eq!(out.matches("<td").count(), 2 + 2 + 1);
    }

    #[test]
    fn empty_input_yields_empty_output() {
        assert_eq!(tsplit(5, "K", "", false).unwrap(), "");
        assert_eq!(tsplit(5, "K", "\n  \n\t\n", false).unwrap(), "");
    }

    #[test]
    fn separator_rows_from_input_are_dropped() {
        let input = table(&["a"]);
        let out = tsplit(1, "New", &input, false).unwrap();
        assert!(!out.contains("Graduated"));
        assert_eq!(out.matches("cncf-sep").count(), 1);
        assert!(out.contains(r#"<td colspan="1" class="cncf-sep">New</td>"#));
    }

    #[test]
    fn mismatched_link_and_image_counts_is_an_error() {
        let mut input = table(&["a", "b"]);
        input += &link("orphan", "");
        input += "\n";
        let err = tsplit(2, "K", &input, false).unwrap_err();
        assert_eq!(
            err,
            TsplitError::LinkImageMismatch {
                links: 3,
                images: 2
            }
        );
        assert_eq!(
            err.to_string(),
            "number of link lines (3) differs from number of image lines (2)"
        );
    }

    #[test]
    fn kind_is_inserted_verbatim() {
        let input = table(&["a"]);
        let out = tsplit(1, "Sandbox & <Friends>", &input, false).unwrap();
        assert!(out.contains(r#"class="cncf-sep">Sandbox & <Friends></td>"#));
    }
}
