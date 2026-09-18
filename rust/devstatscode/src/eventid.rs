//! Port of `eventid.go`: native (GitHub sourced) event id bands.
//!
//! GitHub restarted its event id sequence on 2025-10-09 and allocates ids from two
//! independent sequences since, so the raw id alone no longer identifies an event.
//! Native events created at/after the epoch of a rule are stored (in `gha_events.id`
//! and in every `*.event_id`) as `raw id + band * NATIVE_ID_BAND_BASE`; events older
//! than every rule keep the raw id (band 0), so history is untouched. This is the ONLY
//! transformation applied to native ids and it happens in the one place native ids
//! are created: [`crate::ghawriter::write_to_db`] (gha2db archives and the ghapi2db
//! `repo events` feed).

use chrono::{DateTime, TimeZone, Utc};
use std::sync::OnceLock;

use crate::consts::{ARTIFICIAL_ID_BASE, NATIVE_ID_BAND_BASE};

/// Go `NativeIDBandRule`: one generation of GitHub event id sequences — native events
/// created at/after `since` get the band of their type (`bands`) or `default_band`.
pub struct NativeIdBandRule {
    /// Inclusive epoch, RFC 3339 UTC.
    pub since: &'static str,
    /// Band for every event type not listed in `bands`.
    pub default_band: i64,
    /// `(event type, band)` pairs.
    pub bands: &'static [(&'static str, i64)],
}

/// Go `NativeIDBandRules`: newest first, the first rule whose `since <= created_at`
/// decides the band, older events keep their raw id (band 0).
///
/// To handle another GitHub sequence reset (or an event type moving to another
/// sequence) PREPEND a rule with a `since` a few days in the future (every running
/// image must carry the rule before it activates, otherwise old and new binaries store
/// the same event under two ids) and with band numbers never used by any other rule
/// (bands must stay < 281), then mirror it in Go `eventid.go` (the compat tests compare
/// both). 2026-09-20 rule: band 1 = the issues/PRs/comments/reviews/stars/forks/
/// releases/... sequence, band 2 = the git reference sequence (`PushEvent`,
/// `CreateEvent`, `DeleteEvent`).
pub const NATIVE_ID_BAND_RULES: &[NativeIdBandRule] = &[NativeIdBandRule {
    since: "2026-09-20T00:00:00Z",
    default_band: 1,
    bands: &[("PushEvent", 2), ("CreateEvent", 2), ("DeleteEvent", 2)],
}];

fn rule_epochs() -> &'static [DateTime<Utc>] {
    static EPOCHS: OnceLock<Vec<DateTime<Utc>>> = OnceLock::new();
    EPOCHS.get_or_init(|| {
        NATIVE_ID_BAND_RULES
            .iter()
            .map(|rule| {
                DateTime::parse_from_rfc3339(rule.since)
                    .unwrap_or_else(|e| {
                        panic!("NATIVE_ID_BAND_RULES: bad since {:?}: {}", rule.since, e)
                    })
                    .with_timezone(&Utc)
            })
            .collect()
    })
}

/// Go `NativeIDBand`: band of a native event given its type and creation time, 0 = raw id.
pub fn native_id_band<Tz: TimeZone>(e_type: &str, created_at: DateTime<Tz>) -> i64 {
    for (rule, since) in NATIVE_ID_BAND_RULES.iter().zip(rule_epochs()) {
        if created_at < *since {
            continue;
        }
        return rule
            .bands
            .iter()
            .find(|(t, _)| *t == e_type)
            .map(|(_, band)| *band)
            .unwrap_or(rule.default_band);
    }
    0
}

/// Go `NativeEventID`: id stored in `gha_events.id` (and every `*.event_id`) for a
/// native GitHub event — raw GitHub id + band * [`NATIVE_ID_BAND_BASE`]; ids outside
/// `(0, NATIVE_ID_BAND_BASE)` are not GitHub sequence ids and are returned unchanged.
pub fn native_event_id<Tz: TimeZone>(raw_id: i64, e_type: &str, created_at: DateTime<Tz>) -> i64 {
    if raw_id <= 0 || raw_id >= NATIVE_ID_BAND_BASE {
        return raw_id;
    }
    raw_id + native_id_band(e_type, created_at) * NATIVE_ID_BAND_BASE
}

/// Go `NativeEventIDString`: [`native_event_id`] for the JSON string id, a non-numeric
/// id is returned unchanged.
pub fn native_event_id_string<Tz: TimeZone>(
    raw_id: &str,
    e_type: &str,
    created_at: DateTime<Tz>,
) -> String {
    match raw_id.parse::<i64>() {
        Ok(raw) => native_event_id(raw, e_type, created_at).to_string(),
        Err(_) => raw_id.to_string(),
    }
}

/// Go `SplitNativeEventID`: `(band, raw GitHub id)` of a stored native event id,
/// `(0, id)` for non-native ids.
pub fn split_native_event_id(id: i64) -> (i64, i64) {
    if id <= 0 || id >= ARTIFICIAL_ID_BASE {
        return (0, id);
    }
    (id / NATIVE_ID_BAND_BASE, id % NATIVE_ID_BAND_BASE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, FixedOffset, TimeZone};

    fn epoch() -> DateTime<Utc> {
        rule_epochs()[0]
    }

    #[test]
    fn rules_invariants() {
        assert!(!NATIVE_ID_BAND_RULES.is_empty());
        assert_eq!(NATIVE_ID_BAND_BASE, 1_000_000_000_000);
        let max_band = ARTIFICIAL_ID_BASE / NATIVE_ID_BAND_BASE;
        let epochs = rule_epochs();
        assert_eq!(epochs.len(), NATIVE_ID_BAND_RULES.len());
        let mut seen: std::collections::HashMap<i64, usize> = Default::default();
        for (i, rule) in NATIVE_ID_BAND_RULES.iter().enumerate() {
            // every `since` parses, is UTC ("Z") and the list is newest first, strictly
            assert!(
                rule.since.ends_with('Z'),
                "rule {i}: since {:?} is not UTC",
                rule.since
            );
            if i > 0 {
                assert!(
                    epochs[i] < epochs[i - 1],
                    "rule {i}: since {} is not before rule {}",
                    rule.since,
                    i - 1
                );
            }
            let mut bands: Vec<i64> = vec![rule.default_band];
            bands.extend(rule.bands.iter().map(|(_, b)| *b));
            for b in &bands {
                assert!(
                    *b > 0,
                    "rule {i}: band {b} is not positive (0 is the raw id)"
                );
                assert!(*b < max_band, "rule {i}: band {b} does not fit below 2^48");
            }
            bands.sort();
            bands.dedup();
            for b in bands {
                if let Some(prev) = seen.insert(b, i) {
                    panic!("band {b} used by rules {prev} and {i}: bands must never be reused");
                }
            }
            // no duplicated type
            let mut types: Vec<&str> = rule.bands.iter().map(|(t, _)| *t).collect();
            types.sort();
            let n = types.len();
            types.dedup();
            assert_eq!(n, types.len(), "rule {i}: duplicated type");
        }
        let newest = &NATIVE_ID_BAND_RULES[0];
        for t in ["PushEvent", "CreateEvent", "DeleteEvent"] {
            let b = newest
                .bands
                .iter()
                .find(|(x, _)| *x == t)
                .map(|(_, b)| *b)
                .unwrap_or_else(|| panic!("newest rule: {t} has no explicit band"));
            assert_ne!(b, newest.default_band, "{t} shares the default band");
        }
    }

    #[test]
    fn band_by_type_and_time() {
        let e = epoch();
        let sec = Duration::seconds(1);
        let cases: Vec<(&str, DateTime<Utc>, i64)> = vec![
            ("IssuesEvent", e - sec, 0),
            ("PushEvent", e - sec, 0),
            (
                "IssuesEvent",
                Utc.with_ymd_and_hms(2015, 1, 1, 15, 0, 0).unwrap(),
                0,
            ),
            (
                "PushEvent",
                Utc.with_ymd_and_hms(2025, 11, 20, 12, 0, 0).unwrap(),
                0,
            ),
            ("IssuesEvent", e, 1),
            ("IssuesEvent", e + sec, 1),
            ("IssueCommentEvent", e + sec, 1),
            ("PullRequestEvent", e + sec, 1),
            ("PullRequestReviewEvent", e + sec, 1),
            ("PullRequestReviewCommentEvent", e + sec, 1),
            ("CommitCommentEvent", e + sec, 1),
            ("WatchEvent", e + sec, 1),
            ("ForkEvent", e + sec, 1),
            ("ReleaseEvent", e + sec, 1),
            ("MemberEvent", e + sec, 1),
            ("PublicEvent", e + sec, 1),
            ("GollumEvent", e + sec, 1),
            ("TeamAddEvent", e + sec, 1),
            ("PullRequestReviewThreadEvent", e + sec, 1),
            ("SponsorshipEvent", e + sec, 1),
            ("DiscussionCommentEvent", e + sec, 1),
            ("SomethingNewEvent", e + sec, 1),
            ("", e + sec, 1),
            ("PushEvent", e, 2),
            ("CreateEvent", e + sec, 2),
            ("DeleteEvent", e + sec, 2),
            (
                "PushEvent",
                Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap(),
                2,
            ),
            (
                "IssuesEvent",
                Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap(),
                1,
            ),
        ];
        for (t, at, band) in cases {
            assert_eq!(native_id_band(t, at), band, "native_id_band({t:?}, {at})");
        }
        // instants are compared, whatever the offset
        let cest = FixedOffset::east_opt(2 * 3600).unwrap();
        let pst = FixedOffset::west_opt(8 * 3600).unwrap();
        assert_eq!(native_id_band("IssuesEvent", e.with_timezone(&cest)), 1);
        assert_eq!(
            native_id_band("PushEvent", (e - sec).with_timezone(&pst)),
            0
        );
        assert_eq!(native_id_band("IssuesEvent", DateTime::<Utc>::MIN_UTC), 0);
    }

    #[test]
    fn event_id_and_string() {
        let e = epoch();
        let before = e - Duration::seconds(1);
        let after = e + Duration::hours(1);
        let base = NATIVE_ID_BAND_BASE;
        assert_eq!(
            native_event_id(15167597171, "IssuesEvent", before),
            15167597171
        );
        assert_eq!(
            native_event_id(21500000000, "PushEvent", before),
            21500000000
        );
        assert_eq!(
            native_event_id(15167597171, "IssuesEvent", after),
            base + 15167597171
        );
        assert_eq!(
            native_event_id(21500000000, "PushEvent", after),
            2 * base + 21500000000
        );
        assert_eq!(
            native_event_id(21500000001, "CreateEvent", after),
            2 * base + 21500000001
        );
        assert_eq!(
            native_event_id(21500000002, "DeleteEvent", after),
            2 * base + 21500000002
        );
        assert_eq!(native_event_id(1, "WatchEvent", after), base + 1);
        assert_eq!(native_event_id(7, "FooEvent", after), base + 7);
        assert_eq!(
            native_event_id(base - 1, "IssuesEvent", after),
            2 * base - 1
        );
        // not GitHub sequence ids: unchanged
        assert_eq!(native_event_id(0, "IssuesEvent", after), 0);
        assert_eq!(
            native_event_id(-1234567890123, "IssuesEvent", after),
            -1234567890123
        );
        assert_eq!(native_event_id(base + 5, "IssuesEvent", after), base + 5);
        assert_eq!(
            native_event_id(ARTIFICIAL_ID_BASE + 5, "IssuesEvent", after),
            ARTIFICIAL_ID_BASE + 5
        );
        // strings (Go strconv.ParseInt semantics for what the feed can carry)
        assert_eq!(
            native_event_id_string("15167597171", "IssuesEvent", before),
            "15167597171"
        );
        assert_eq!(
            native_event_id_string("15167597171", "IssuesEvent", after),
            "1015167597171"
        );
        assert_eq!(
            native_event_id_string("21500000000", "PushEvent", after),
            "2021500000000"
        );
        assert_eq!(
            native_event_id_string("1", "CreateEvent", after),
            "2000000000001"
        );
        assert_eq!(
            native_event_id_string("007", "WatchEvent", after),
            "1000000000007"
        );
        assert_eq!(
            native_event_id_string("+7", "WatchEvent", after),
            "1000000000007"
        );
        assert_eq!(native_event_id_string("", "WatchEvent", after), "");
        assert_eq!(native_event_id_string("abc", "WatchEvent", after), "abc");
        assert_eq!(native_event_id_string("1.5", "WatchEvent", after), "1.5");
        assert_eq!(native_event_id_string("-5", "WatchEvent", after), "-5");
        assert_eq!(native_event_id_string("0", "WatchEvent", after), "0");
        assert_eq!(
            native_event_id_string("99999999999999999999", "WatchEvent", after),
            "99999999999999999999"
        );
    }

    #[test]
    fn split_and_round_trip() {
        let base = NATIVE_ID_BAND_BASE;
        assert_eq!(split_native_event_id(15167597171), (0, 15167597171));
        assert_eq!(split_native_event_id(base + 15167597171), (1, 15167597171));
        assert_eq!(
            split_native_event_id(2 * base + 21500000000),
            (2, 21500000000)
        );
        assert_eq!(split_native_event_id(base), (1, 0));
        assert_eq!(split_native_event_id(2 * base - 1), (1, base - 1));
        assert_eq!(split_native_event_id(280 * base), (280, 0));
        assert_eq!(split_native_event_id(0), (0, 0));
        assert_eq!(split_native_event_id(-1234567890123), (0, -1234567890123));
        assert_eq!(
            split_native_event_id(ARTIFICIAL_ID_BASE),
            (0, ARTIFICIAL_ID_BASE)
        );
        assert_eq!(
            split_native_event_id(ARTIFICIAL_ID_BASE + 4_000_000_000_000),
            (0, ARTIFICIAL_ID_BASE + 4_000_000_000_000)
        );
        let after = epoch() + Duration::hours(1);
        for t in [
            "IssuesEvent",
            "PushEvent",
            "CreateEvent",
            "DeleteEvent",
            "WatchEvent",
            "XEvent",
        ] {
            let raw = 21500000000;
            let id = native_event_id(raw, t, after);
            let (band, r) = split_native_event_id(id);
            assert_eq!(r, raw, "{t}");
            assert_eq!(band, native_id_band(t, after), "{t}");
            assert_ne!(band, 0, "{t}");
            assert!(
                id > 0 && id < ARTIFICIAL_ID_BASE,
                "{t}: {id} outside the native class"
            );
        }
    }
}
