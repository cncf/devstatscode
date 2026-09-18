//! `devstatscode::eventid` against the Go reference (`eventid.go`), live: the Go
//! probe `rust/compat/go/testdata/eventidprobe` computes `NativeIDBand`,
//! `NativeEventIDString` and `SplitNativeEventID` for every vector and prints its
//! `NativeIDBandRules`; the Rust side must agree line by line — this is what keeps
//! the two rule lists (epochs, default bands, per-type bands) identical, which is
//! what makes both implementations store one event under one id.

use devstats_compat::{go_probe, run, Invocation};
use devstatscode::chrono::{DateTime, Duration, FixedOffset, TimeZone, Utc};
use devstatscode::consts::{ARTIFICIAL_ID_BASE, NATIVE_ID_BAND_BASE};
use devstatscode::eventid::{
    native_event_id_string, native_id_band, split_native_event_id, NATIVE_ID_BAND_RULES,
};

const TYPES: &[&str] = &[
    "CommitCommentEvent",
    "CreateEvent",
    "DeleteEvent",
    "DiscussionCommentEvent",
    "ForkEvent",
    "GollumEvent",
    "IssueCommentEvent",
    "IssuesEvent",
    "MemberEvent",
    "PublicEvent",
    "PullRequestEvent",
    "PullRequestReviewCommentEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewThreadEvent",
    "PushEvent",
    "ReleaseEvent",
    "SponsorshipEvent",
    "TeamAddEvent",
    "WatchEvent",
    "SomethingNewEvent",
    "",
];

const RAW_IDS: &[&str] = &[
    "1",
    "2489654310",
    "15167597171",
    "21500000000",
    "55800000000",
    "999999999999",
    "1000000000000",
    "1000000000001",
    "281474976710655",
    "281474976710656",
    "281474976710657",
    "0",
    "-1",
    "-1234567890123",
    "007",
    "+7",
    "",
    "abc",
    "1.5",
    "99999999999999999999",
];

/// Every rule epoch ± 1 s, the same instants written with other offsets, and a
/// few fixed dates around the whole history.
fn instants() -> Vec<DateTime<FixedOffset>> {
    let mut v: Vec<DateTime<FixedOffset>> = Vec::new();
    let utc = FixedOffset::east_opt(0).unwrap();
    let cest = FixedOffset::east_opt(2 * 3600).unwrap();
    let pst = FixedOffset::west_opt(8 * 3600).unwrap();
    for rule in NATIVE_ID_BAND_RULES {
        let e = DateTime::parse_from_rfc3339(rule.since).unwrap();
        for d in [-86400, -1, 0, 1, 3600, 86400 * 30] {
            let t = e + Duration::seconds(d);
            v.push(t.with_timezone(&utc));
            v.push(t.with_timezone(&cest));
            v.push(t.with_timezone(&pst));
        }
    }
    for (y, m, d) in [
        (2011, 2, 12),
        (2014, 12, 31),
        (2015, 1, 1),
        (2020, 5, 1),
        (2025, 10, 9),
        (2025, 11, 20),
        (2026, 9, 18),
        (2030, 1, 1),
        (2100, 1, 1),
    ] {
        v.push(
            Utc.with_ymd_and_hms(y, m, d, 12, 0, 0)
                .unwrap()
                .with_timezone(&utc),
        );
    }
    v
}

fn rust_line(at: DateTime<FixedOffset>, t: &str, raw: &str) -> String {
    let band = native_id_band(t, at);
    let id = native_event_id_string(raw, t, at);
    let split = match id.parse::<i64>() {
        Ok(n) => {
            let (b, r) = split_native_event_id(n);
            format!("{b}, {r}")
        }
        Err(_) => "-".to_string(),
    };
    format!("{band} | {id} | {split}")
}

fn rust_rules() -> String {
    let rules: Vec<String> = NATIVE_ID_BAND_RULES
        .iter()
        .map(|rule| {
            let mut bands: Vec<(&str, i64)> = rule.bands.to_vec();
            bands.sort();
            let bands: Vec<String> = bands.iter().map(|(t, b)| format!("{t}:{b}")).collect();
            let since = DateTime::parse_from_rfc3339(rule.since)
                .unwrap()
                .with_timezone(&Utc)
                .to_rfc3339_opts(devstatscode::chrono::SecondsFormat::Secs, true);
            format!(
                "since={since} default={} bands={}",
                rule.default_band,
                bands.join(",")
            )
        })
        .collect();
    format!("RULES {}", rules.join(" ; "))
}

#[test]
fn go_probe_agrees_with_rust() {
    let Some(probe) = go_probe("eventidprobe") else {
        return;
    };
    let mut vectors: Vec<(DateTime<FixedOffset>, &str, &str)> = Vec::new();
    for at in instants() {
        for t in TYPES {
            for raw in RAW_IDS {
                vectors.push((at, t, raw));
            }
        }
    }
    let script: String = vectors
        .iter()
        .map(|(at, t, raw)| format!("{}\t{t}\t{raw}\n", at.to_rfc3339()))
        .collect();
    let inv = Invocation::new().stdin(script).env("TZ", "UTC");
    let go = run(&probe, &inv);
    assert_eq!(go.code(), 0, "probe failed: {}", go.stderr_str());
    let go_stdout = go.stdout_str();
    let go_lines: Vec<&str> = go_stdout.lines().collect();
    assert_eq!(
        go_lines.len(),
        vectors.len() + 1,
        "probe printed {} lines for {} vectors",
        go_lines.len(),
        vectors.len()
    );
    let mut diffs = Vec::new();
    for (i, (at, t, raw)) in vectors.iter().enumerate() {
        let r = rust_line(*at, t, raw);
        if go_lines[i] != r {
            diffs.push(format!(
                "({}, {t:?}, {raw:?}): Go `{}` Rust `{r}`",
                at.to_rfc3339(),
                go_lines[i]
            ));
        }
    }
    assert!(
        diffs.is_empty(),
        "{} of {} vectors differ:\n{}",
        diffs.len(),
        vectors.len(),
        diffs
            .iter()
            .take(50)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        go_lines[vectors.len()],
        rust_rules(),
        "NativeIDBandRules differ"
    );
    // and the vectors did exercise every band of every rule
    let mut bands_seen: Vec<i64> = vectors
        .iter()
        .map(|(at, t, _)| native_id_band(t, *at))
        .collect();
    bands_seen.sort();
    bands_seen.dedup();
    let mut bands_defined: Vec<i64> = vec![0];
    for rule in NATIVE_ID_BAND_RULES {
        bands_defined.push(rule.default_band);
        bands_defined.extend(rule.bands.iter().map(|(_, b)| *b));
    }
    bands_defined.sort();
    bands_defined.dedup();
    assert_eq!(bands_seen, bands_defined);
    assert!(bands_defined
        .iter()
        .all(|b| *b * NATIVE_ID_BAND_BASE < ARTIFICIAL_ID_BASE));
}
