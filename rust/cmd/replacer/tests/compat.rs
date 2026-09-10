//! Go ⇄ Rust compatibility tests for `replacer`.
//!
//! Every scenario runs both binaries on their own copy of the same file, then
//! compares exit code, stdout and the resulting file contents. Scenarios are
//! taken from real call sites in `cncf/devstats` (`devel/*.sh`, `util_sh/*.sh`,
//! `devstats-docker-images`, `devstats-helm`).

use std::fs;
use std::path::{Path, PathBuf};

use devstats_compat::{go_binary, run, rust_binary, Invocation, Outcome};

struct Case<'a> {
    name: &'a str,
    content: &'a [u8],
    env: Vec<(&'a str, &'a str)>,
}

struct Result_ {
    out: Outcome,
    file: Vec<u8>,
}

fn go_bin() -> Option<PathBuf> {
    go_binary("replacer")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_replacer"))
}

/// Run `bin` inside a fresh temp dir holding `case.content` as file `case.name`.
fn run_case(bin: &Path, case: &Case<'_>, file_arg: Option<&str>) -> Result_ {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(case.name);
    fs::write(&path, case.content).unwrap();
    let mut inv = Invocation::new().cwd(dir.path());
    for (k, v) in &case.env {
        inv = inv.env(k, v);
    }
    if let Some(a) = file_arg {
        inv = inv.arg(a);
    } else {
        inv = inv.arg(case.name);
    }
    let out = run(bin, &inv);
    let file = fs::read(&path).unwrap();
    Result_ { out, file }
}

/// Run both implementations and assert identical exit code, stdout and file result.
/// Returns the Rust result for further assertions.
fn both(case: &Case<'_>) -> Result_ {
    let rust = run_case(&rust_bin(), case, None);
    if let Some(go) = go_bin() {
        let go = run_case(&go, case, None);
        let ctx = format!(
            "\ncase {:?} env {:?}\n--- go code {:?} stdout:\n{}--- go stderr:\n{}--- rust code {:?} stdout:\n{}--- rust stderr:\n{}",
            case.name,
            case.env,
            go.out.code,
            go.out.stdout_str(),
            go.out.stderr_str(),
            rust.out.code,
            rust.out.stdout_str(),
            rust.out.stderr_str()
        );
        assert_eq!(go.out.code, rust.out.code, "exit code{ctx}");
        assert_eq!(go.out.stdout_str(), rust.out.stdout_str(), "stdout{ctx}");
        assert!(go.file == rust.file, "resulting file differs{ctx}");
    }
    rust
}

fn case<'a>(name: &'a str, content: &'a [u8], env: &[(&'a str, &'a str)]) -> Case<'a> {
    Case {
        name,
        content,
        env: env.to_vec(),
    }
}

// ---------------------------------------------------------------------------
// ss / ss0 — literal replacements (the overwhelmingly common usage)
// ---------------------------------------------------------------------------

#[test]
fn ss_template_substitution_in_sql() {
    // devstats/util_sh/repo_data.sh: FROM="{{org_repo}}" TO="$1" MODE=ss replacer /tmp/repo_data.sql
    let sql = b"select * from gha_repos where name = '{{org_repo}}' or alias = '{{org_repo}}';\n";
    let r = both(&case(
        "repo_data.sql",
        sql,
        &[
            ("MODE", "ss"),
            ("FROM", "{{org_repo}}"),
            ("TO", "kubernetes/kubernetes"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.out.stdout_str(), "Hits: repo_data.sql\n");
    assert_eq!(
        r.file,
        b"select * from gha_repos where name = 'kubernetes/kubernetes' or alias = 'kubernetes/kubernetes';\n"
    );
}

#[test]
fn ss_nothing_replaced_is_an_error_but_ss0_is_not() {
    let content = b"nothing to see here\n";
    let r = both(&case(
        "f.txt",
        content,
        &[("MODE", "ss"), ("FROM", "{{missing}}"), ("TO", "x")],
    ));
    assert_eq!(r.out.code(), 1);
    assert_eq!(r.out.stdout_str(), "Nothing replaced in: f.txt\n");
    assert_eq!(r.file, content);

    let r = both(&case(
        "f.txt",
        content,
        &[("MODE", "ss0"), ("FROM", "{{missing}}"), ("TO", "x")],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.out.stdout_str(), "Nothing replaced in: f.txt\n");
    assert_eq!(r.file, content);
}

#[test]
fn ss0_grafana_dashboard_rename() {
    // devstats/devel/*: FROM="\"$SRC\"" TO="\"$proj\"" MODE=ss0 replacer grafana/dashboards/$proj/$FILE
    let json =
        br#"{"title": "kubernetes", "tags": ["kubernetes", "dashboard"], "uid": "kubernetes"}"#;
    let r = both(&case(
        "dashboard.json",
        json,
        &[
            ("MODE", "ss0"),
            ("FROM", "\"kubernetes\""),
            ("TO", "\"prometheus\""),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(
        r.file,
        br#"{"title": "prometheus", "tags": ["prometheus", "dashboard"], "uid": "prometheus"}"#
    );
}

#[test]
fn ss_deleting_text_with_dash_to() {
    // devstats-docker-images: MODE=ss FROM=';google_analytics_ua_id =' TO="-" replacer "$cfile"
    let cfg = b"[analytics]\n;google_analytics_ua_id = UA-123\n";
    let r = both(&case(
        "grafana.ini",
        cfg,
        &[
            ("MODE", "ss"),
            ("FROM", ";google_analytics_ua_id ="),
            ("TO", "-"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"[analytics]\n UA-123\n");
}

#[test]
fn ss_deleting_text_with_no_to() {
    // devstats/util_sh/update_bots_exclusions.sh style: NO_TO=1 allows an empty TO
    let r = both(&case(
        "f.txt",
        b"keep DROP keep DROP\n",
        &[("MODE", "ss"), ("FROM", " DROP"), ("NO_TO", "1")],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"keep keep\n");

    let r = both(&case(
        "f.txt",
        b"keep DROP\n",
        &[
            ("MODE", "ss"),
            ("FROM", " DROP"),
            ("TO", ""),
            ("NO_TO", "yes"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"keep\n");
}

#[test]
fn ss_multiline_blob_from_files() {
    // devel/mass_replace.sh: MODE=ss FROM=`cat in` TO=`cat out` replacer file
    let content = b"a\n  \"x\": 1,\n  \"y\": 2,\nb\n";
    let r = both(&case(
        "f.json",
        content,
        &[
            ("MODE", "ss"),
            ("FROM", "  \"x\": 1,\n  \"y\": 2,\n"),
            ("TO", "  \"x\": 10,\n  \"y\": 20,\n  \"z\": 30,\n"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"a\n  \"x\": 10,\n  \"y\": 20,\n  \"z\": 30,\nb\n");
}

#[test]
fn ss_unicode_content() {
    let content = "Łukasz Gryglicki <lgryglicki@cncf.io> — CNCF\n".as_bytes();
    let r = both(&case(
        "authors.txt",
        content,
        &[("MODE", "ss"), ("FROM", "Łukasz"), ("TO", "Justyna")],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(
        r.file,
        "Justyna Gryglicki <lgryglicki@cncf.io> — CNCF\n".as_bytes()
    );
}

#[test]
fn ss_binary_safe_content() {
    let content = b"\x00\x01\xFF{{x}}\xFE\x00";
    let r = both(&case(
        "blob.bin",
        content,
        &[("MODE", "ss"), ("FROM", "{{x}}"), ("TO", "ok")],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"\x00\x01\xFFok\xFE\x00");
}

#[test]
fn ss_with_nreplaces_limit() {
    let r = both(&case(
        "f.txt",
        b"x x x x\n",
        &[
            ("MODE", "ss"),
            ("FROM", "x"),
            ("TO", "y"),
            ("NREPLACES", "2"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"y y x x\n");
}

#[test]
fn ss_with_replacefrom_offset() {
    let r = both(&case(
        "f.txt",
        b"x x x x\n",
        &[
            ("MODE", "ss"),
            ("FROM", "x"),
            ("TO", "y"),
            ("REPLACEFROM", "3"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    // Offset 3 is inside the 2nd `x`; only occurrences at/after the offset change.
    assert_eq!(r.file, b"x x y y\n");
}

#[test]
fn ss_replacefrom_and_nreplaces_together() {
    let r = both(&case(
        "f.txt",
        b"x x x x\n",
        &[
            ("MODE", "ss"),
            ("FROM", "x"),
            ("TO", "y"),
            ("REPLACEFROM", "3"),
            ("NREPLACES", "1"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"x x y x\n");
}

#[test]
fn ss_empty_from_via_dash_inserts_between_characters() {
    let r = both(&case(
        "f.txt",
        b"ab",
        &[("MODE", "ss"), ("FROM", "-"), ("TO", "-")],
    ));
    // FROM=- and TO=- both mean "empty" -> nothing changes.
    assert_eq!(r.out.code(), 1);
    assert_eq!(r.file, b"ab");

    let r = both(&case(
        "f.txt",
        b"ab",
        &[("MODE", "ss"), ("FROM", "-"), ("TO", "|")],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"|a|b|");
}

#[test]
fn ss_file_mode_is_preserved() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("script.sh");
        fs::write(&path, b"#!/bin/bash\necho {{x}}\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        let inv = Invocation::new()
            .cwd(dir.path())
            .env("MODE", "ss")
            .env("FROM", "{{x}}")
            .env("TO", "hi")
            .arg("script.sh");
        let out = run(&rust_bin(), &inv);
        assert_eq!(out.code(), 0);
        assert_eq!(fs::read(&path).unwrap(), b"#!/bin/bash\necho hi\n");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}

// ---------------------------------------------------------------------------
// rr / rr0 / rs / rs0 — regexp modes, patterns copied from devstats scripts
// ---------------------------------------------------------------------------

#[test]
fn rr_group_expansion_with_literal_braces_in_pattern() {
    // devstats/devel/update_dashboards_labels.sh:
    // MODE=rr FROM='\((.*)\s+{{exclude_bots}}\)' TO='(lower($1) {{exclude_bots}})' replacer $f
    let sql = b"select 1 from t where (dup_actor_login {{exclude_bots}})\n";
    let r = both(&case(
        "metric.sql",
        sql,
        &[
            ("MODE", "rr"),
            ("FROM", r"\((.*)\s+{{exclude_bots}}\)"),
            ("TO", "(lower($1) {{exclude_bots}})"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.out.stdout_str(), "Hits: metric.sql\n");
    assert_eq!(
        r.file,
        b"select 1 from t where (lower(dup_actor_login) {{exclude_bots}})\n"
    );
}

#[test]
fn rr0_crontab_comment_and_uncomment() {
    // devstats/devel/cronctl.sh
    let crontab = b"MAILTO=\"\"\n*/10 * * * * devstats_sync k8s\n#0 4 * * * devstats_backup k8s\n5 * * * * other prometheus\n";
    let r = both(&case(
        "crontab.tmp",
        crontab,
        &[
            ("MODE", "rr0"),
            ("FROM", r"(?m)^([^#].*\s+k8s\s+.*)$"),
            ("TO", "#$1"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(
        r.file,
        b"MAILTO=\"\"\n#*/10 * * * * devstats_sync k8s\n#0 4 * * * devstats_backup k8s\n5 * * * * other prometheus\n"
    );
    let r = both(&case(
        "crontab.tmp",
        crontab,
        &[
            ("MODE", "rr0"),
            ("FROM", r"(?m)^#(.*\s+k8s\s+.*)$"),
            ("TO", "$1"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(
        r.file,
        b"MAILTO=\"\"\n*/10 * * * * devstats_sync k8s\n0 4 * * * devstats_backup k8s\n5 * * * * other prometheus\n"
    );
    // No matching line: rr0 is fine with zero hits.
    let r = both(&case(
        "crontab.tmp",
        crontab,
        &[
            ("MODE", "rr0"),
            ("FROM", r"(?m)^#(.*\s+nosuch\s+.*)$"),
            ("TO", "$1"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.out.stdout_str(), "Nothing replaced in: crontab.tmp\n");
    assert_eq!(r.file, crontab);
}

#[test]
fn rr_nothing_replaced_is_an_error() {
    let r = both(&case(
        "f.txt",
        b"abc\n",
        &[("MODE", "rr"), ("FROM", "x+"), ("TO", "y")],
    ));
    assert_eq!(r.out.code(), 1);
    assert_eq!(r.out.stdout_str(), "Nothing replaced in: f.txt\n");
}

#[test]
fn rs0_literal_replacement_ignores_dollar_in_to() {
    // devstats: MODE=rs0 FROM='(?m)^.*"uid": "\w+",\n' TO='-' replacer $f
    let json = b"{\n  \"uid\": \"abc_1\",\n  \"title\": \"x\"\n}\n";
    let r = both(&case(
        "dash.json",
        json,
        &[
            ("MODE", "rs0"),
            ("FROM", r#"(?m)^.*"uid": "\w+",\n"#),
            ("TO", "-"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    // TO=- means "empty": the whole uid line is removed.
    assert_eq!(r.file, b"{\n  \"title\": \"x\"\n}\n");

    let r = both(&case(
        "f.txt",
        b"a1 b2\n",
        &[("MODE", "rs"), ("FROM", r"([a-z])(\d)"), ("TO", "$2$1")],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"$2$1 $2$1\n");
}

#[test]
fn rr_named_groups_and_flags() {
    let r = both(&case(
        "f.txt",
        b"Key=Value\nkey=other\n",
        &[
            ("MODE", "rr"),
            ("FROM", r"(?im)^(?P<k>key)=(?P<v>\w+)$"),
            ("TO", "${v}=${k}"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"Value=Key\nother=key\n");
}

#[test]
fn rr_dollar_escaping_and_unknown_groups() {
    let r = both(&case(
        "f.txt",
        b"price 10\n",
        &[("MODE", "rr"), ("FROM", r"(\d+)"), ("TO", "$$$1 ($2)")],
    ));
    assert_eq!(r.out.code(), 0);
    // `$$` -> `$`, `$2` does not exist -> empty, in both implementations.
    assert_eq!(r.file, b"price $10 ()\n");
}

#[test]
fn rr_anchors_and_dotall() {
    let r = both(&case(
        "f.txt",
        b"first\nsecond\nthird\n",
        &[
            ("MODE", "rr"),
            ("FROM", r"(?s)^first.*second\n"),
            ("TO", "-"),
        ],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, b"third\n");

    let r = both(&case(
        "f.txt",
        b"a.b.c\n",
        &[("MODE", "rr"), ("FROM", r"\."), ("TO", "_")],
    ));
    assert_eq!(r.file, b"a_b_c\n");
}

#[test]
fn rr_ascii_classes_like_go() {
    // Go's \w \d \s are ASCII-only; the Rust port keeps that semantics.
    let r = both(&case(
        "f.txt",
        "id=abc_1 name=Łukasz digit=٣\n".as_bytes(),
        &[("MODE", "rr"), ("FROM", r"=(\w+)"), ("TO", "=<$1>")],
    ));
    assert_eq!(r.out.code(), 0);
    assert_eq!(r.file, "id=<abc_1> name=Łukasz digit=٣\n".as_bytes());
}

#[test]
fn invalid_regexp_is_fatal_exit_2() {
    for pat in ["(unclosed", "[z-a]", "*x", r"a\"] {
        let r = both(&case(
            "f.txt",
            b"abc\n",
            &[("MODE", "rr"), ("FROM", pat), ("TO", "y")],
        ));
        assert_eq!(r.out.code(), 2, "FROM={pat:?}");
        assert!(r.out.stdout.is_empty(), "FROM={pat:?}");
        assert!(!r.out.stderr.is_empty(), "FROM={pat:?}");
        assert_eq!(r.file, b"abc\n");
    }
}

// ---------------------------------------------------------------------------
// usage / file errors
// ---------------------------------------------------------------------------

#[test]
fn usage_errors() {
    let content = b"abc\n";
    let checks: &[(&[(&str, &str)], &str)] = &[
        (
            &[("MODE", "ss"), ("TO", "y")],
            "You need to set 'FROM' env variable\n",
        ),
        (
            &[("MODE", "ss"), ("FROM", "a")],
            "You need to set 'TO' env variable or specify NO_TO\n",
        ),
        (
            &[("MODE", "ss"), ("FROM", "a"), ("TO", "")],
            "You need to set 'TO' env variable or specify NO_TO\n",
        ),
        (
            &[("FROM", "a"), ("TO", "b")],
            "You need to set 'MODE' env variable\n",
        ),
        (
            &[("MODE", "xx"), ("FROM", "a"), ("TO", "b")],
            "Unknown mode 'xx'\n",
        ),
        (
            &[("MODE", "SS"), ("FROM", "a"), ("TO", "b")],
            "Unknown mode 'SS'\n",
        ),
    ];
    for (env, msg) in checks {
        let r = both(&case("f.txt", content, env));
        assert_eq!(r.out.code(), 1, "env {env:?}");
        assert_eq!(r.out.stdout_str(), *msg, "env {env:?}");
        assert_eq!(r.file, content);
    }
}

#[test]
fn missing_file_argument() {
    let c = case(
        "f.txt",
        b"abc\n",
        &[("MODE", "ss"), ("FROM", "a"), ("TO", "b")],
    );
    let rust = run_case(&rust_bin(), &c, Some(""));
    // An empty argument is still an argument (os.Args has it) -> file "" cannot be read.
    assert_eq!(rust.out.code(), 1);
    assert!(rust.out.stdout_str().starts_with("Error: "));

    let dir = tempfile::tempdir().unwrap();
    let inv = Invocation::new()
        .cwd(dir.path())
        .env("MODE", "ss")
        .env("FROM", "a")
        .env("TO", "b");
    let rust = run(&rust_bin(), &inv);
    assert_eq!(rust.code(), 1);
    assert_eq!(rust.stdout_str(), "You need to provide a file name\n");
    if let Some(go) = go_bin() {
        let go = run(&go, &inv);
        assert_eq!(go.code, rust.code);
        assert_eq!(go.stdout, rust.stdout);
    }
}

#[test]
fn nonexistent_file_and_directory() {
    let c = case(
        "f.txt",
        b"abc\n",
        &[("MODE", "ss"), ("FROM", "a"), ("TO", "b")],
    );
    for target in ["does_not_exist.txt", "."] {
        let rust = run_case(&rust_bin(), &c, Some(target));
        assert_eq!(rust.out.code(), 1, "{target}");
        assert!(
            rust.out.stdout_str().starts_with("Error: "),
            "{target}: {}",
            rust.out.stdout_str()
        );
        if let Some(go) = go_bin() {
            let go = run_case(&go, &c, Some(target));
            assert_eq!(go.out.code, rust.out.code, "{target}");
            assert!(go.out.stdout_str().starts_with("Error: "), "{target}");
        }
    }
}

#[test]
fn bad_nreplaces_and_replacefrom_are_fatal_exit_2() {
    let content = b"x x x x\n";
    let bad: &[(&str, &str)] = &[
        ("NREPLACES", "0"),
        ("NREPLACES", "-1"),
        ("NREPLACES", "abc"),
        ("REPLACEFROM", "0"),
        ("REPLACEFROM", "-3"),
        ("REPLACEFROM", "1x"),
        ("REPLACEFROM", "8"),  // == len
        ("REPLACEFROM", "99"), // > len
    ];
    for (k, v) in bad {
        let r = both(&case(
            "f.txt",
            content,
            &[("MODE", "ss"), ("FROM", "x"), ("TO", "y"), (k, v)],
        ));
        assert_eq!(r.out.code(), 2, "{k}={v}");
        assert!(r.out.stdout.is_empty(), "{k}={v}");
        assert!(!r.out.stderr.is_empty(), "{k}={v}");
        assert_eq!(r.file, content, "{k}={v}");
    }
    // Messages for the value checks are shared by both implementations.
    let r = both(&case(
        "f.txt",
        content,
        &[
            ("MODE", "ss"),
            ("FROM", "x"),
            ("TO", "y"),
            ("NREPLACES", "0"),
        ],
    ));
    assert!(r.out.stderr_str().contains("NREPLACES must be positive"));
    let r = both(&case(
        "f.txt",
        content,
        &[
            ("MODE", "ss"),
            ("FROM", "x"),
            ("TO", "y"),
            ("REPLACEFROM", "8"),
        ],
    ));
    assert!(r
        .out
        .stderr_str()
        .contains("REPLACEFROM must be less than file length 8"));
}

#[test]
fn fatal_errors_wait_unless_no_fatal_delay() {
    // Without NO_FATAL_DELAY a fatal error sleeps 60s before exiting (both impls);
    // prove the process is still alive after a moment, then kill it.
    use std::process::{Command, Stdio};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("f.txt"), b"x\n").unwrap();
    let mut bins = vec![rust_bin()];
    bins.extend(go_bin());
    for bin in bins {
        let mut child = Command::new(&bin)
            .current_dir(dir.path())
            .env_remove("NO_FATAL_DELAY")
            .env("MODE", "ss")
            .env("FROM", "x")
            .env("TO", "y")
            .env("NREPLACES", "0")
            .arg("f.txt")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(1500));
        assert!(
            child.try_wait().unwrap().is_none(),
            "{} exited without the fatal delay",
            bin.display()
        );
        child.kill().unwrap();
        child.wait().unwrap();
    }
}
