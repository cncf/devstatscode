//! Go ⇄ Rust compatibility tests for `tsplit`.
//!
//! The Go reference binary is built from `../../cmd/tsplit/tsplit.go`; both
//! binaries get identical env/stdin and must produce identical stdout and exit
//! codes (and identical stderr where the message is part of the tool's contract).

use std::path::PathBuf;

use devstats_compat::{fixture_bytes, go_binary, run_both, rust_binary, Compare, Invocation};

fn bins() -> (Option<PathBuf>, PathBuf) {
    (
        go_binary("tsplit"),
        rust_binary(env!("CARGO_BIN_EXE_tsplit")),
    )
}

fn same(inv: Invocation<'_>, what: Compare) -> devstats_compat::Outcome {
    let (go, rust) = bins();
    run_both(go.as_deref(), &rust, &inv, what).1
}

fn base(kind: &'static str, size: &'static str, input: Vec<u8>) -> Invocation<'static> {
    Invocation::new()
        .env("KIND", kind)
        .env("SIZE", size)
        .stdin(input)
}

const FIXTURES: &[&str] = &[
    "graduated.html",
    "incubating.html",
    "sandbox.html",
    "full_table.html",
];
const SIZES: &[&str] = &["1", "2", "3", "7", "9", "10", "11", "12", "13", "500"];

#[test]
fn real_sections_all_sizes_identical_output() {
    for fx in FIXTURES {
        let input = fixture_bytes(&format!("tsplit/{fx}"));
        for size in SIZES {
            let out = same(base("Graduated", size, input.clone()), Compare::All);
            assert_eq!(out.code(), 0, "{fx} SIZE={size}");
            assert!(out.stdout.ends_with(b"</tr>\n"), "{fx} SIZE={size}");
        }
    }
}

#[test]
fn kinds_are_labels_only() {
    let input = fixture_bytes("tsplit/incubating.html");
    for kind in [
        "Graduated",
        "Incubating",
        "Sandbox",
        "Archived",
        "Some Thing",
    ] {
        let out = same(base(kind, "10", input.clone()), Compare::All);
        assert_eq!(out.code(), 0);
        let label = format!(r#"class="cncf-sep">{kind}</td>"#);
        assert_eq!(out.stdout_str().matches(&label).count(), 4, "{kind}");
    }
}

#[test]
fn output_is_stable_when_reprocessed() {
    // Re-splitting the tool's own output with the same SIZE is a no-op (both impls).
    let input = fixture_bytes("tsplit/sandbox.html");
    let first = same(base("Sandbox", "12", input), Compare::All);
    let second = same(base("Sandbox", "12", first.stdout.clone()), Compare::All);
    assert_eq!(first.stdout, second.stdout);
}

#[test]
fn debug_diagnostics_on_stderr() {
    let input = fixture_bytes("tsplit/graduated.html");
    let out = same(
        base("Graduated", "12", input).env("DEBUG", "1"),
        Compare::All,
    );
    assert_eq!(out.code(), 0);
    let err = out.stderr_str();
    assert!(err.starts_with("section 0: 0-12 (12 items)\n"), "{err}");
    assert!(err.contains("\nsection 3: 36-39 (3 items)\n"), "{err}");
    assert!(err.contains("\nLinks 39:\n"), "{err}");
    assert!(err.contains("\nImages 39:\n"), "{err}");
}

#[test]
fn empty_and_blank_input() {
    for input in ["", "\n", "\n\n   \n\t\n"] {
        let out = same(base("Graduated", "5", input.into()), Compare::All);
        assert_eq!(out.code(), 0);
        assert_eq!(out.stdout, b"\n");
    }
}

#[test]
fn structural_only_input_yields_empty_table() {
    let input = "    <tr>\n      <td colspan=\"3\" class=\"cncf-sep\">Graduated</td>\n    </tr>\n";
    let out = same(base("Graduated", "5", input.into()), Compare::All);
    assert_eq!(out.code(), 0);
    assert_eq!(out.stdout, b"\n");
}

#[test]
fn missing_kind_or_size_is_a_usage_error() {
    let input = fixture_bytes("tsplit/graduated.html");
    let out = same(
        Invocation::new().env("SIZE", "12").stdin(input.clone()),
        Compare::All,
    );
    assert_eq!(out.code(), 1);
    assert_eq!(
        out.stderr_str(),
        "You need to specify kind via KIND=Graduated|Incubating|Sandbox\n"
    );
    assert!(out.stdout.is_empty());

    let out = same(
        Invocation::new().env("KIND", "Graduated").stdin(input),
        Compare::All,
    );
    assert_eq!(out.code(), 1);
    assert_eq!(
        out.stderr_str(),
        "You need to specify size via SIZE=n (usually 9, 10, 11, 12)\n"
    );
    assert!(out.stdout.is_empty());
}

#[test]
fn invalid_size_values_are_rejected() {
    let input = fixture_bytes("tsplit/graduated.html");
    // Wording of the parse error differs between Go and Rust; the contract is:
    // exit 1, nothing on stdout, an `error: ...` line on stderr.
    for size in ["abc", "12x", "1.5", " 12", "0x10"] {
        let out = same(
            base("Graduated", size, input.clone()),
            Compare::CodeAndStdout,
        );
        assert_eq!(out.code(), 1, "SIZE={size:?}");
        assert!(out.stdout.is_empty(), "SIZE={size:?}");
        assert!(
            out.stderr_str().starts_with("error: "),
            "SIZE={size:?}: {}",
            out.stderr_str()
        );
    }
    // Non-positive sizes: identical message (bug fix applied to both implementations).
    for size in ["0", "-1", "-12"] {
        let out = same(base("Graduated", size, input.clone()), Compare::All);
        assert_eq!(out.code(), 1, "SIZE={size:?}");
        assert_eq!(
            out.stderr_str(),
            format!("error: SIZE must be positive, got {size}\n")
        );
    }
}

#[test]
fn mismatched_links_and_images_is_an_error() {
    // Feeding the whole partial including its `testsrv=[[hostname]]` marker
    // lines used to silently misalign links vs. logos; both implementations now
    // refuse with the same message.
    let mut input = b"    testsrv=[[hostname]] \n".to_vec();
    input.extend(fixture_bytes("tsplit/graduated.html"));
    input.extend(b"    [[hostname]]=testsrv\n");
    let out = same(base("Graduated", "12", input), Compare::All);
    assert_eq!(out.code(), 1);
    assert_eq!(
        out.stderr_str(),
        "error: number of link lines (41) differs from number of image lines (39)\n"
    );
    assert!(out.stdout.is_empty());
}

#[test]
fn indentation_and_crlf_are_preserved() {
    let input = String::from_utf8(fixture_bytes("tsplit/graduated.html")).unwrap();
    let tabbed = input.replace("    <tr>", "\t<tr>");
    let out = same(base("Graduated", "9", tabbed.into_bytes()), Compare::All);
    assert!(out
        .stdout_str()
        .starts_with("\t<tr>\n\t  <td colspan=\"9\""));

    let crlf = input.replace('\n', "\r\n");
    let out = same(base("Graduated", "9", crlf.into_bytes()), Compare::All);
    assert_eq!(out.code(), 0);
}

/// `tsplit … | head -1` after `head` exited: Go's `fmt.Printf` dies from
/// `SIGPIPE` (`os.epipecheck`), printing nothing — so must the Rust binary
/// (which would otherwise report `error: writing stdout: Broken pipe`).
#[test]
fn closed_stdout_pipe_dies_from_sigpipe_like_go() {
    let input = fixture_bytes("tsplit/graduated.html");
    let out = same(base("Graduated", "10", input).closed_stdout(), Compare::All);
    assert_eq!(out.code, None);
    assert_eq!(out.signal, Some(devstats_compat::SIGPIPE));
    assert!(out.stderr.is_empty(), "{}", out.stderr_str());
}

/// Same for diagnostics on a closed stderr (`fmt.Fprintf(os.Stderr, …)`): the
/// usage error and the `DEBUG` section dump both die silently from `SIGPIPE`.
#[test]
fn closed_stderr_pipe_dies_from_sigpipe_like_go() {
    let out = same(
        base("Graduated", "0", b"".to_vec()).closed_stderr(),
        Compare::All,
    );
    assert_eq!(
        (out.code, out.signal),
        (None, Some(devstats_compat::SIGPIPE))
    );
    assert!(out.stdout.is_empty());

    let input = fixture_bytes("tsplit/graduated.html");
    let out = same(
        base("Graduated", "10", input)
            .env("DEBUG", "1")
            .closed_stderr(),
        Compare::All,
    );
    assert_eq!(
        (out.code, out.signal),
        (None, Some(devstats_compat::SIGPIPE))
    );
    assert!(out.stdout.is_empty(), "died before the table was printed");
}
