//! GH Archive event structures and helpers — port of the event part of
//! `gha.go` (`Event`, `EventOld`, `Payload`, … , `ActorHit`, `RepoHit`,
//! `MakeOldRepoName`, the `*OrNil` SQL argument helpers and the
//! `Compare*Ptr` functions).
//!
//! The structures decode GH Archive JSON like Go's `encoding/json`/jsoniter
//! decode the Go structs: unknown keys are ignored, a JSON `null` leaves the
//! zero value (`Option` fields for Go pointers, the zero value for the plain
//! fields), missing keys do the same, and `time.Time` values must be RFC 3339
//! strings — their zone offset is kept (`DateTime<FixedOffset>`), because
//! lib/pq sends the wall clock *and* the offset to PostgreSQL and a
//! `timestamp` column keeps only the wall clock (old GH Archive hours carry
//! `-08:00` offsets whose wall clock is the UTC hour).

use std::collections::BTreeSet;

use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::de::{self, Deserializer, Visitor};
use serde::Deserialize;

use crate::context::{Ctx, GoRegex};
use crate::pg::SqlArg;

/// A GH Archive timestamp: Go `time.Time` decoded from RFC 3339 with the
/// offset kept. Its `Default` is Go's zero time (`0001-01-01 00:00:00 UTC`),
/// so missing / `null` JSON values behave like Go's zero value; `Option<GhaTime>`
/// is a Go `*time.Time`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GhaTime(pub DateTime<FixedOffset>);

impl Default for GhaTime {
    fn default() -> Self {
        zero_time()
    }
}

impl std::ops::Deref for GhaTime {
    type Target = DateTime<FixedOffset>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DateTime<FixedOffset>> for GhaTime {
    fn from(t: DateTime<FixedOffset>) -> Self {
        GhaTime(t)
    }
}

impl From<GhaTime> for SqlArg {
    fn from(t: GhaTime) -> Self {
        SqlArg::Time(t.0)
    }
}

impl From<&GhaTime> for SqlArg {
    fn from(t: &GhaTime) -> Self {
        SqlArg::Time(t.0)
    }
}

impl std::fmt::Display for GhaTime {
    /// Go `%v` of a `time.Time` decoded from JSON.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&crate::gofmt::time(self.0))
    }
}

impl<'de> Deserialize<'de> for GhaTime {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        go_time(d).map(GhaTime)
    }
}

/// Go's zero `time.Time` (`0001-01-01 00:00:00 +0000 UTC`).
pub fn zero_time() -> GhaTime {
    GhaTime(
        NaiveDate::from_ymd_opt(1, 1, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc().fixed_offset())
            .expect("year 1 is representable"),
    )
}

// ---------------------------------------------------------------------------
// Go encoding/json semantics for serde
// ---------------------------------------------------------------------------

/// `null` (or a missing key, together with `#[serde(default)]`) leaves the
/// zero value like Go does for non-pointer fields.
fn nd<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// Go `time.Time.UnmarshalJSON`: the value must be a JSON string in RFC 3339
/// (`2006-01-02T15:04:05Z07:00`, optional fractional seconds); `null` is a
/// no-op. Returns the Go `time.Parse` error text on failure.
pub fn parse_go_rfc3339(s: &str) -> Result<DateTime<FixedOffset>, String> {
    let layout = "2006-01-02T15:04:05Z07:00";
    let b = s.as_bytes();
    // Go's RFC 3339 parser wants exactly two-digit fields, an upper-case `T`
    // (Go ≥ 1.20 is strict here) and `Z` or `±hh:mm`.
    let bad = |rest: &str, elem: &str| {
        format!(
            "parsing time {:?} as {:?}: cannot parse {:?} as {:?}",
            s, layout, rest, elem
        )
    };
    let digits = |from: usize, n: usize| -> bool {
        b.len() >= from + n && b[from..from + n].iter().all(|c| c.is_ascii_digit())
    };
    if !digits(0, 4) {
        return Err(bad(s, "2006"));
    }
    if b.get(4) != Some(&b'-') {
        return Err(bad(&s[4..], "-"));
    }
    if !digits(5, 2) {
        return Err(bad(&s[5..], "01"));
    }
    if b.get(7) != Some(&b'-') {
        return Err(bad(&s[7..], "-"));
    }
    if !digits(8, 2) {
        return Err(bad(&s[8..], "02"));
    }
    if b.get(10) != Some(&b'T') {
        return Err(bad(&s[10..], "T"));
    }
    if !digits(11, 2) {
        return Err(bad(&s[11..], "15"));
    }
    if b.get(13) != Some(&b':') {
        return Err(bad(&s[13..], ":"));
    }
    if !digits(14, 2) {
        return Err(bad(&s[14..], "04"));
    }
    if b.get(16) != Some(&b':') {
        return Err(bad(&s[16..], ":"));
    }
    if !digits(17, 2) {
        return Err(bad(&s[17..], "05"));
    }
    let mut i = 19;
    if b.get(i) == Some(&b'.') || b.get(i) == Some(&b',') {
        let start = i + 1;
        let mut j = start;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j == start {
            return Err(bad(&s[i..], "Z07:00"));
        }
        i = j;
    }
    match b.get(i) {
        Some(b'Z') if i + 1 == b.len() => {}
        Some(b'+') | Some(b'-') => {
            if !(digits(i + 1, 2) && b.get(i + 3) == Some(&b':') && digits(i + 4, 2))
                || i + 6 != b.len()
            {
                return Err(bad(&s[i..], "Z07:00"));
            }
        }
        _ => return Err(bad(&s[i..], "Z07:00")),
    }
    let mut norm = s.replace(',', ".");
    if norm.len() > 19 && norm.as_bytes()[19] == b'.' {
        // Go accepts any number of fractional digits (extra ones are dropped);
        // chrono needs at most nine.
        let frac_end = norm[20..]
            .find(|c: char| !c.is_ascii_digit())
            .map(|p| 20 + p)
            .unwrap_or(norm.len());
        if frac_end - 20 > 9 {
            norm.replace_range(29..frac_end, "");
        }
    }
    DateTime::parse_from_rfc3339(&norm).map_err(|_| {
        // A well formed but impossible date (e.g. month 13), Go's wording and
        // order: the month/hour/minute/second ranges are checked while the
        // elements are read, the day of the month (`daysIn(month, year)`)
        // once the whole value is parsed.
        let num = |from: usize, to: usize| s[from..to].parse::<u32>().unwrap_or(0);
        let (year, month, day) = (num(0, 4), num(5, 7), num(8, 10));
        format!(
            "parsing time {:?}: {}",
            s,
            if !(1..=12).contains(&month) {
                "month out of range"
            } else if num(11, 13) > 23 {
                "hour out of range"
            } else if num(14, 16) > 59 {
                "minute out of range"
            } else if num(17, 19) > 59 {
                "second out of range"
            } else if NaiveDate::from_ymd_opt(year as i32, month, day).is_none() {
                "day out of range"
            } else {
                "time out of range"
            }
        )
    })
}

/// Go `time.Time` JSON decoding: an RFC 3339 string, or `null` → zero time
/// (`time.Time.UnmarshalJSON` is a no-op for `null`); anything else is an
/// error like Go's `Time.UnmarshalJSON: input is not a JSON string`.
pub fn go_time<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<FixedOffset>, D::Error> {
    struct S;
    impl<'de> Visitor<'de> for S {
        type Value = DateTime<FixedOffset>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an RFC 3339 time string or null")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            parse_go_rfc3339(v).map_err(|e| E::custom(format!("time.Time.UnmarshalJSON: {e}")))
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(zero_time().0)
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(zero_time().0)
        }
        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            d.deserialize_any(S)
        }
        fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
            Err(E::custom("Time.UnmarshalJSON: input is not a JSON string"))
        }
        fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
            Err(E::custom("Time.UnmarshalJSON: input is not a JSON string"))
        }
        fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
            Err(E::custom("Time.UnmarshalJSON: input is not a JSON string"))
        }
        fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
            Err(E::custom("Time.UnmarshalJSON: input is not a JSON string"))
        }
    }
    d.deserialize_any(S)
}

// ---------------------------------------------------------------------------
// Structures
// ---------------------------------------------------------------------------

/// Go `Dummy`: a structure with no data — an `Option<Dummy>` tells whether a
/// key was present (and not `null`) in the JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Dummy {}

/// Go `Event`: a full GH Archive event (2015+ format).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Event {
    #[serde(deserialize_with = "nd")]
    pub id: String,
    #[serde(rename = "type", deserialize_with = "nd")]
    pub type_: String,
    #[serde(deserialize_with = "nd")]
    pub public: bool,
    pub created_at: GhaTime,
    #[serde(deserialize_with = "nd")]
    pub actor: Actor,
    #[serde(deserialize_with = "nd")]
    pub repo: Repo,
    pub org: Option<Org>,
    #[serde(deserialize_with = "nd")]
    pub payload: Payload,
}

/// Go `EventOld`: a GH Archive event in the pre-2015 format.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct EventOld {
    /// Go `json:"-"`: never decoded.
    #[serde(skip)]
    pub id: String,
    #[serde(rename = "type", deserialize_with = "nd")]
    pub type_: String,
    #[serde(deserialize_with = "nd")]
    pub public: bool,
    pub created_at: GhaTime,
    #[serde(deserialize_with = "nd")]
    pub actor: String,
    #[serde(deserialize_with = "nd")]
    pub repository: ForkeeOld,
    pub payload: Option<PayloadOld>,
}

/// Go `Payload`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Payload {
    pub push_id: Option<i64>,
    pub size: Option<i64>,
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub head: Option<String>,
    pub before: Option<String>,
    pub action: Option<String>,
    pub ref_type: Option<String>,
    pub master_branch: Option<String>,
    pub description: Option<String>,
    pub number: Option<i64>,
    pub forkee: Option<Forkee>,
    pub release: Option<Release>,
    pub member: Option<Actor>,
    pub issue: Option<Issue>,
    pub comment: Option<Comment>,
    pub review: Option<Review>,
    pub commits: Option<Vec<Commit>>,
    pub pages: Option<Vec<Page>>,
    pub pull_request: Option<PullRequest>,
}

/// Go `PayloadOld` (pre-2015).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct PayloadOld {
    pub issue: Option<i64>,
    pub issue_id: Option<i64>,
    pub comment: Option<Comment>,
    pub comment_id: Option<i64>,
    pub description: Option<String>,
    pub master_branch: Option<String>,
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub action: Option<String>,
    pub ref_type: Option<String>,
    pub head: Option<String>,
    pub size: Option<i64>,
    pub number: Option<i64>,
    pub pull_request: Option<PullRequest>,
    pub member: Option<Actor>,
    pub release: Option<Release>,
    pub pages: Option<Vec<Page>>,
    pub commit: Option<String>,
    /// `[[sha, email, message, author name, distinct], …]` — kept as raw JSON
    /// values like Go's `*[]interface{}`.
    pub shas: Option<Vec<serde_json::Value>>,
    pub repository: Option<Forkee>,
    pub team: Option<Team>,
}

/// Go `ForkeeOld` (pre-2015 repository).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct ForkeeOld {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    pub created_at: GhaTime,
    pub description: Option<String>,
    #[serde(deserialize_with = "nd")]
    pub fork: bool,
    #[serde(deserialize_with = "nd")]
    pub forks: i64,
    #[serde(deserialize_with = "nd")]
    pub has_downloads: bool,
    #[serde(deserialize_with = "nd")]
    pub has_issues: bool,
    #[serde(deserialize_with = "nd")]
    pub has_wiki: bool,
    pub homepage: Option<String>,
    pub language: Option<String>,
    /// Go `DefaultBranch json:"master_branch"`.
    #[serde(rename = "master_branch", deserialize_with = "nd")]
    pub default_branch: String,
    #[serde(deserialize_with = "nd")]
    pub name: String,
    #[serde(deserialize_with = "nd")]
    pub open_issues: i64,
    pub organization: Option<String>,
    #[serde(deserialize_with = "nd")]
    pub owner: String,
    pub private: Option<bool>,
    pub pushed_at: Option<GhaTime>,
    #[serde(deserialize_with = "nd")]
    pub size: i64,
    #[serde(deserialize_with = "nd")]
    pub stargazers: i64,
    #[serde(deserialize_with = "nd")]
    pub watchers: i64,
}

/// Go `Repo`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Repo {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub name: String,
}

/// Go `Actor` (`Name` is `json:"-"`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Actor {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub login: String,
    #[serde(skip)]
    pub name: String,
}

/// Go `Org`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Org {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub login: String,
}

/// Go `Forkee`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Forkee {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub name: String,
    #[serde(deserialize_with = "nd")]
    pub full_name: String,
    #[serde(deserialize_with = "nd")]
    pub owner: Actor,
    pub description: Option<String>,
    pub public: Option<bool>,
    #[serde(deserialize_with = "nd")]
    pub fork: bool,
    pub created_at: GhaTime,
    pub updated_at: GhaTime,
    pub pushed_at: Option<GhaTime>,
    pub homepage: Option<String>,
    #[serde(deserialize_with = "nd")]
    pub size: i64,
    #[serde(deserialize_with = "nd")]
    pub stargazers_count: i64,
    #[serde(deserialize_with = "nd")]
    pub has_issues: bool,
    pub has_projects: Option<bool>,
    #[serde(deserialize_with = "nd")]
    pub has_downloads: bool,
    #[serde(deserialize_with = "nd")]
    pub has_wiki: bool,
    pub has_pages: Option<bool>,
    #[serde(deserialize_with = "nd")]
    pub forks: i64,
    #[serde(deserialize_with = "nd")]
    pub open_issues: i64,
    #[serde(deserialize_with = "nd")]
    pub watchers: i64,
    #[serde(deserialize_with = "nd")]
    pub default_branch: String,
}

/// Go `Release`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Release {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub tag_name: String,
    #[serde(deserialize_with = "nd")]
    pub target_commitish: String,
    pub name: Option<String>,
    #[serde(deserialize_with = "nd")]
    pub draft: bool,
    #[serde(deserialize_with = "nd")]
    pub author: Actor,
    #[serde(deserialize_with = "nd")]
    pub prerelease: bool,
    pub created_at: GhaTime,
    pub published_at: Option<GhaTime>,
    pub body: Option<String>,
    #[serde(deserialize_with = "nd")]
    pub assets: Vec<Asset>,
}

/// Go `Asset`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Asset {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    pub created_at: GhaTime,
    pub updated_at: GhaTime,
    #[serde(deserialize_with = "nd")]
    pub name: String,
    pub label: Option<String>,
    #[serde(deserialize_with = "nd")]
    pub uploader: Actor,
    #[serde(deserialize_with = "nd")]
    pub content_type: String,
    #[serde(deserialize_with = "nd")]
    pub state: String,
    #[serde(deserialize_with = "nd")]
    pub size: i64,
    #[serde(deserialize_with = "nd")]
    pub download_count: i64,
}

/// Go `PullRequest`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct PullRequest {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub base: Branch,
    #[serde(deserialize_with = "nd")]
    pub head: Branch,
    #[serde(deserialize_with = "nd")]
    pub user: Actor,
    #[serde(deserialize_with = "nd")]
    pub number: i64,
    #[serde(deserialize_with = "nd")]
    pub state: String,
    pub locked: Option<bool>,
    #[serde(deserialize_with = "nd")]
    pub title: String,
    pub body: Option<String>,
    pub created_at: GhaTime,
    pub updated_at: GhaTime,
    pub closed_at: Option<GhaTime>,
    pub merged_at: Option<GhaTime>,
    pub merge_commit_sha: Option<String>,
    pub assignee: Option<Actor>,
    pub assignees: Option<Vec<Actor>>,
    pub requested_reviewers: Option<Vec<Actor>>,
    pub milestone: Option<Milestone>,
    pub merged: Option<bool>,
    pub mergeable: Option<bool>,
    pub merged_by: Option<Actor>,
    pub mergeable_state: Option<String>,
    pub rebaseable: Option<bool>,
    pub comments: Option<i64>,
    pub review_comments: Option<i64>,
    pub maintainer_can_modify: Option<bool>,
    pub commits: Option<i64>,
    pub additions: Option<i64>,
    pub deletions: Option<i64>,
    pub changed_files: Option<i64>,
}

/// Go `Branch` (its `repo` holds a `Forkee`).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Branch {
    #[serde(deserialize_with = "nd")]
    pub sha: String,
    pub user: Option<Actor>,
    pub repo: Option<Forkee>,
    #[serde(deserialize_with = "nd")]
    pub label: String,
    #[serde(rename = "ref", deserialize_with = "nd")]
    pub ref_: String,
}

/// Go `Issue`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Issue {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub number: i64,
    #[serde(deserialize_with = "nd")]
    pub comments: i64,
    #[serde(deserialize_with = "nd")]
    pub title: String,
    #[serde(deserialize_with = "nd")]
    pub state: String,
    #[serde(deserialize_with = "nd")]
    pub locked: bool,
    pub body: Option<String>,
    #[serde(deserialize_with = "nd")]
    pub user: Actor,
    pub assignee: Option<Actor>,
    #[serde(deserialize_with = "nd")]
    pub labels: Vec<Label>,
    #[serde(deserialize_with = "nd")]
    pub assignees: Vec<Actor>,
    pub milestone: Option<Milestone>,
    pub created_at: GhaTime,
    pub updated_at: GhaTime,
    pub closed_at: Option<GhaTime>,
    /// Presence marker: the issue is a pull request.
    pub pull_request: Option<Dummy>,
}

/// Go `Label`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Label {
    pub id: Option<i64>,
    #[serde(deserialize_with = "nd")]
    pub name: String,
    #[serde(deserialize_with = "nd")]
    pub color: String,
    pub default: Option<bool>,
}

/// Go `Milestone`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Milestone {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub name: String,
    #[serde(deserialize_with = "nd")]
    pub number: i64,
    #[serde(deserialize_with = "nd")]
    pub title: String,
    pub description: Option<String>,
    pub creator: Option<Actor>,
    #[serde(deserialize_with = "nd")]
    pub open_issues: i64,
    #[serde(deserialize_with = "nd")]
    pub closed_issues: i64,
    #[serde(deserialize_with = "nd")]
    pub state: String,
    pub created_at: GhaTime,
    pub updated_at: GhaTime,
    pub closed_at: Option<GhaTime>,
    pub due_on: Option<GhaTime>,
}

/// Go `Comment`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Comment {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub body: String,
    pub created_at: GhaTime,
    pub updated_at: GhaTime,
    #[serde(deserialize_with = "nd")]
    pub user: Actor,
    pub commit_id: Option<String>,
    pub original_commit_id: Option<String>,
    pub diff_hunk: Option<String>,
    pub position: Option<i64>,
    pub original_position: Option<i64>,
    pub path: Option<String>,
    pub pull_request_review_id: Option<i64>,
    pub line: Option<i64>,
}

/// Go `Review`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Review {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub user: Actor,
    #[serde(deserialize_with = "nd")]
    pub commit_id: String,
    pub submitted_at: GhaTime,
    #[serde(deserialize_with = "nd")]
    pub author_association: String,
    #[serde(deserialize_with = "nd")]
    pub state: String,
    pub body: Option<String>,
}

/// Go `Commit`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Commit {
    #[serde(deserialize_with = "nd")]
    pub sha: String,
    #[serde(deserialize_with = "nd")]
    pub author: Author,
    #[serde(deserialize_with = "nd")]
    pub message: String,
    #[serde(deserialize_with = "nd")]
    pub distinct: bool,
}

/// Go `Author` (git commit author).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Author {
    #[serde(deserialize_with = "nd")]
    pub name: String,
    #[serde(deserialize_with = "nd")]
    pub email: String,
}

/// Go `Page`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Page {
    #[serde(deserialize_with = "nd")]
    pub sha: String,
    #[serde(deserialize_with = "nd")]
    pub action: String,
    #[serde(deserialize_with = "nd")]
    pub title: String,
}

/// Go `Team` (pre-2015 only).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Team {
    #[serde(deserialize_with = "nd")]
    pub id: i64,
    #[serde(deserialize_with = "nd")]
    pub name: String,
    #[serde(deserialize_with = "nd")]
    pub slug: String,
    #[serde(deserialize_with = "nd")]
    pub permission: String,
}

/// Go `SkipDatesList`: the `skip_dates.yaml` structure (GH Archive hours to
/// skip).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct SkipDatesList {
    #[serde(
        rename = "skip_dates",
        deserialize_with = "crate::yamlv2::de::time_seq"
    )]
    pub dates: Vec<DateTime<FixedOffset>>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Go `MakeOldRepoName`: before 2015 the repository name is
/// `Organization/Name` (when the organization is present) or just `Name`.
pub fn make_old_repo_name(repo: &ForkeeOld) -> String {
    match &repo.organization {
        Some(org) if !org.is_empty() => format!("{}/{}", org, repo.name),
        _ => repo.name.clone(),
    }
}

/// Go `ActorHit`: are we interested in this actor?
pub fn actor_hit(ctx: &Ctx, actor_name: &str) -> bool {
    if !ctx.actors_filter {
        return true;
    }
    let allowed = ctx
        .actors_allow
        .as_ref()
        .is_none_or(|re| re.is_match(actor_name));
    let forbidden = ctx
        .actors_forbid
        .as_ref()
        .is_some_and(|re| re.is_match(actor_name));
    allowed && !forbidden
}

/// Go `RepoHit`: are we interested in this `org/repo`? `forg`/`frepo` are
/// the org/repo name sets (an empty set is Go's nil map), `org_re`/`repo_re`
/// the `regexp:` alternatives.
pub fn repo_hit(
    ctx: &Ctx,
    full_name: &str,
    forg: &BTreeSet<String>,
    frepo: &BTreeSet<String>,
    org_re: Option<&GoRegex>,
    repo_re: Option<&GoRegex>,
) -> bool {
    // Return false if no repo name
    if full_name.is_empty() {
        return false;
    }
    // If given repo full name is in the exclude list, signal no hit
    if ctx.exclude_repos.contains_key(full_name) {
        return false;
    }
    let exact = ctx.exact;
    // If repo name in old format (no org name) then assume org = ""
    let res: Vec<&str> = full_name.split('/').collect();
    let (mut org, mut repo) = ("", res[0]);
    // New repo name format org/repo
    if res.len() > 1 {
        org = res[0];
        repo = res[1];
    }
    // Now check for full name hit in org (one can provide full repo name org/repo)
    let ok = match org_re {
        Some(re) => re.is_match(full_name),
        None => forg.contains(full_name),
    };
    // If we hit then we can have two cases
    // We hit a full name with "/" - this is a direct hit, return true
    // We hit old repo name format but special flag GHA2DB_EXACT is used
    // Only return hit when this flag is set
    if ok && (exact || res.len() > 1) {
        return ok;
    }
    // Now if org list given and different org, return false
    if let Some(re) = org_re {
        if !re.is_match(org) {
            return false;
        }
    }
    if !forg.is_empty() && !forg.contains(org) {
        return false;
    }
    // Now if repo list given and different repo, return false
    if let Some(re) = repo_re {
        if !re.is_match(repo) {
            return false;
        }
    }
    if !frepo.is_empty() && !frepo.contains(repo) {
        return false;
    }
    // Either org matches given list or no org given
    // and repo name matches given or no repo given
    true
}

/// Go `OrgIDOrNil`.
pub fn org_id_or_nil(org: Option<&Org>) -> SqlArg {
    org.map(|o| o.id).into()
}

/// Go `OrgLoginOrNil`.
pub fn org_login_or_nil(org: Option<&Org>) -> SqlArg {
    org.map(|o| o.login.as_str()).into()
}

/// Go `RepoIDOrNil`.
pub fn repo_id_or_nil(repo: Option<&Repo>) -> SqlArg {
    repo.map(|r| r.id).into()
}

/// Go `RepoNameOrNil`.
pub fn repo_name_or_nil(repo: Option<&Repo>) -> SqlArg {
    repo.map(|r| r.name.as_str()).into()
}

/// Go `IssueIDOrNil`.
pub fn issue_id_or_nil(issue: Option<&Issue>) -> SqlArg {
    issue.map(|i| i.id).into()
}

/// Go `PullRequestIDOrNil`.
pub fn pull_request_id_or_nil(pr: Option<&PullRequest>) -> SqlArg {
    pr.map(|p| p.id).into()
}

/// Go `CommentIDOrNil`.
pub fn comment_id_or_nil(comment: Option<&Comment>) -> SqlArg {
    comment.map(|c| c.id).into()
}

/// Go `ForkeeIDOrNil`.
pub fn forkee_id_or_nil(forkee: Option<&Forkee>) -> SqlArg {
    forkee.map(|f| f.id).into()
}

/// Go `ForkeeOldIDOrNil`.
pub fn forkee_old_id_or_nil(forkee: Option<&ForkeeOld>) -> SqlArg {
    forkee.map(|f| f.id).into()
}

/// Go `ForkeeNameOrNil`.
pub fn forkee_name_or_nil(forkee: Option<&Forkee>) -> SqlArg {
    forkee.map(|f| f.name.as_str()).into()
}

/// Go `ActorIDOrNil`.
pub fn actor_id_or_nil(actor: Option<&Actor>) -> SqlArg {
    actor.map(|a| a.id).into()
}

/// Go `ActorLoginOrNil`: the (possibly anonymized) login.
pub fn actor_login_or_nil(actor: Option<&Actor>, maybe_hide: &dyn Fn(&str) -> String) -> SqlArg {
    actor.map(|a| maybe_hide(&a.login)).into()
}

/// Go `ReleaseIDOrNil`.
pub fn release_id_or_nil(release: Option<&Release>) -> SqlArg {
    release.map(|r| r.id).into()
}

/// Go `MilestoneIDOrNil`.
pub fn milestone_id_or_nil(milestone: Option<&Milestone>) -> SqlArg {
    milestone.map(|m| m.id).into()
}

/// Go `CompareStringPtr`: both nil, or both set and equal.
pub fn compare_string_ptr(p1: Option<&str>, p2: Option<&str>) -> bool {
    match (p1, p2) {
        (None, None) => true,
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Go `CompareIntPtr`.
pub fn compare_int_ptr(p1: Option<i64>, p2: Option<i64>) -> bool {
    match (p1, p2) {
        (None, None) => true,
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Go `CompareFloat64Ptr`: equal within `1e-10`.
pub fn compare_float64_ptr(p1: Option<f64>, p2: Option<f64>) -> bool {
    match (p1, p2) {
        (None, None) => true,
        (Some(a), Some(b)) => (a - b).abs() < 1e-10,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Ctx;
    use std::collections::BTreeMap;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn re(pat: &str) -> Option<GoRegex> {
        Some(GoRegex::must(pat))
    }

    // gha_test.go: TestActorHit
    #[test]
    fn actor_hit_table() {
        struct T(
            bool,
            Option<&'static str>,
            Option<&'static str>,
            &'static str,
            bool,
        );
        let cases = [
            T(false, None, None, "actor", true),
            T(false, Some("^a"), Some("z$"), "actor", true),
            T(true, None, None, "", true),
            T(true, None, None, "arbuz", true),
            T(true, Some("^a"), None, "arbuz", true),
            T(true, Some("^a"), None, "rbuz", false),
            T(true, None, Some("z$"), "arbuz", false),
            T(true, None, Some("z$"), "arbu", true),
            T(true, Some("^a"), Some("z$"), "arbuz", false),
            T(true, Some("^a"), Some("z$"), "rbuz", false),
            T(true, Some("^a"), Some("z$"), "arbu", true),
            T(true, Some("^a"), Some("z$"), "rbu", false),
        ];
        let mut ctx = Ctx::default();
        for (i, t) in cases.iter().enumerate() {
            ctx.actors_filter = t.0;
            ctx.actors_allow = t.1.and_then(re);
            ctx.actors_forbid = t.2.and_then(re);
            assert_eq!(actor_hit(&ctx, t.3), t.4, "test number {}", i + 1);
        }
    }

    // gha_test.go: TestRepoHit
    #[test]
    fn repo_hit_table() {
        #[derive(Default)]
        struct T {
            excludes: &'static [&'static str],
            exact: bool,
            full_name: &'static str,
            forg: &'static [&'static str],
            frepo: &'static [&'static str],
            org_re: Option<&'static str>,
            repo_re: Option<&'static str>,
            hit: bool,
        }
        const FLUENT: &str =
            r"^(fluent|fluent-plugins-nursery\/.*fluent.*|.+\/fluentd?-plugin-.+)$";
        let cases = [
            T {
                exact: true,
                full_name: "abc/def",
                forg: &["a/b", "abc/def", "x/y/z"],
                hit: true,
                ..T::default()
            },
            T {
                exact: true,
                full_name: "a/b",
                forg: &["a/b", "abc/def", "x/y/z"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                forg: &["a/b", "abc/def", "x/y/z"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                forg: &["abc"],
                frepo: &["def"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "",
                forg: &["abc"],
                frepo: &["def"],
                ..T::default()
            },
            T {
                full_name: "abc",
                forg: &["abc"],
                ..T::default()
            },
            T {
                full_name: "abc",
                frepo: &["abc"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abcd",
                forg: &["abc"],
                ..T::default()
            },
            T {
                full_name: "abcd",
                frepo: &["abc"],
                ..T::default()
            },
            T {
                full_name: "abc",
                forg: &["abcd"],
                ..T::default()
            },
            T {
                full_name: "abc",
                frepo: &["abcd"],
                ..T::default()
            },
            T {
                full_name: "abc/def",
                forg: &["abc"],
                frepo: &["def"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                forg: &["abc"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                frepo: &["def"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/xyz",
                forg: &["abc", "def/ghi", "j/l"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/ghi",
                forg: &["abc", "def/ghi", "j/l"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "j/l",
                forg: &["abc", "def/ghi", "j/l"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "j/l",
                forg: &["abc", "def/ghi", "j/l"],
                frepo: &["l", "klm"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "def/ghi",
                forg: &["abc", "def/ghi", "j/l"],
                frepo: &["l", "klm"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc",
                forg: &["abc", "def/ghi", "j/l"],
                frepo: &["l", "klm"],
                ..T::default()
            },
            T {
                exact: true,
                full_name: "abc",
                forg: &["abc", "def/ghi", "j/l"],
                frepo: &["l", "klm"],
                hit: true,
                ..T::default()
            },
            T {
                exact: true,
                full_name: "j/l",
                forg: &["abc", "def/ghi", "j/l"],
                frepo: &["l", "klm"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                forg: &["abc"],
                frepo: &["def"],
                excludes: &["abc/def"],
                hit: false,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                forg: &["abc"],
                frepo: &["def"],
                excludes: &["abc/ghi"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                forg: &["abc"],
                frepo: &[],
                excludes: &["abc/def"],
                hit: false,
                ..T::default()
            },
            T {
                full_name: "abc/ghi",
                forg: &["abc"],
                excludes: &["abc/def"],
                hit: true,
                ..T::default()
            },
            T {
                exact: true,
                full_name: "abc/def",
                org_re: Some(r"^(a\/b|abc\/def|x\/y\/z)$"),
                hit: true,
                ..T::default()
            },
            T {
                exact: true,
                full_name: "a/b",
                org_re: Some(r"^(a\/b|abc\/def|x\/y\/z)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                org_re: Some(r"^(a\/b|abc\/def|x\/y\/z)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "XabcX/XdefX",
                org_re: Some("abc"),
                repo_re: Some("def"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "XabcX/XdefX",
                org_re: Some("^abc"),
                repo_re: Some("^def"),
                ..T::default()
            },
            T {
                full_name: "abc/def",
                org_re: Some("^abc$"),
                repo_re: Some("^def$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "",
                org_re: Some("abc"),
                repo_re: Some("def"),
                ..T::default()
            },
            T {
                full_name: "abc",
                org_re: Some("abc"),
                ..T::default()
            },
            T {
                full_name: "abc",
                repo_re: Some("abc"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abcd",
                org_re: Some("abc"),
                ..T::default()
            },
            T {
                full_name: "abcd",
                repo_re: Some("abc"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abcd",
                repo_re: Some("abc$"),
                ..T::default()
            },
            T {
                full_name: "abc",
                org_re: Some("abcd"),
                ..T::default()
            },
            T {
                full_name: "abc",
                repo_re: Some("abcd"),
                ..T::default()
            },
            T {
                full_name: "abc/def",
                org_re: Some("^abc$"),
                repo_re: Some("^def$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                org_re: Some("abc"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                repo_re: Some("def"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/xyz",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/ghi",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "j/l",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "j/l",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "def/ghi",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                ..T::default()
            },
            T {
                exact: true,
                full_name: "abc",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                hit: true,
                ..T::default()
            },
            T {
                exact: true,
                full_name: "j/l",
                org_re: Some(r"^(abc|def\/ghi|j\/l)$"),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                org_re: Some("abc"),
                repo_re: Some("def"),
                excludes: &["abc/def"],
                hit: false,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                org_re: Some("abc"),
                repo_re: Some("def"),
                excludes: &["abc/ghi"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "abc/def",
                org_re: Some("abc"),
                excludes: &["abc/def"],
                hit: false,
                ..T::default()
            },
            T {
                full_name: "abc/ghi",
                org_re: Some("abc"),
                excludes: &["abc/def"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "unknown/some-fluentd-plugin-v2",
                repo_re: Some("fluentd"),
                excludes: &["abc/def"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "Fluentd-Org/some-FLuentd-plugin-v2",
                org_re: Some("(?i)fluentd"),
                repo_re: Some("(?i)fluentd"),
                excludes: &["abc/def"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "fluent-plugins-nursery/fluent-plugin-cloudwatch-logs",
                org_re: Some(FLUENT),
                excludes: &["fluent-plugins-nursery/this-fluent-is-excluded"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "fluent-plugins-nursery/api",
                org_re: Some(FLUENT),
                excludes: &["fluent-plugins-nursery/this-fluent-is-excluded"],
                ..T::default()
            },
            T {
                full_name: "fluent-plugins-nursery/this-fluent-is-excluded",
                org_re: Some(FLUENT),
                excludes: &["fluent-plugins-nursery/this-fluent-is-excluded"],
                ..T::default()
            },
            T {
                full_name: "fluent/client",
                org_re: Some(FLUENT),
                excludes: &["fluent-plugins-nursery/this-fluent-is-excluded"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "fluent-plugins-nursery/excluded",
                org_re: Some(FLUENT),
                excludes: &["fluent-plugins-nursery/excluded"],
                ..T::default()
            },
            T {
                full_name: "excluded/fluent-plugin-a",
                org_re: Some(FLUENT),
                excludes: &["excluded/fluent-plugin-a", "excluded2/fluentd-plugin-b"],
                ..T::default()
            },
            T {
                full_name: "excluded2/fluentd-plugin-b",
                org_re: Some(FLUENT),
                excludes: &["excluded/fluent-plugin-a", "excluded2/fluentd-plugin-b"],
                ..T::default()
            },
            T {
                full_name: "any-org/fluent-plugin-",
                org_re: Some(FLUENT),
                excludes: &["excluded/fluent-plugin-a", "excluded2/fluentd-plugin-b"],
                ..T::default()
            },
            T {
                full_name: "any-org/fluentd-plugin-",
                org_re: Some(FLUENT),
                excludes: &["excluded/fluent-plugin-a", "excluded2/fluentd-plugin-b"],
                ..T::default()
            },
            T {
                full_name: "any-org/fluentd-plugin-x",
                org_re: Some(FLUENT),
                excludes: &["excluded/fluent-plugin-a", "excluded2/fluentd-plugin-b"],
                hit: true,
                ..T::default()
            },
            T {
                full_name: "x/a-fluentd-plugin-x",
                org_re: Some(FLUENT),
                excludes: &["excluded/fluent-plugin-a", "excluded2/fluentd-plugin-b"],
                ..T::default()
            },
            T {
                full_name: "WallyNegima/scenario-manager-plugin",
                org_re: Some(
                    r"(?i)^(fluent|fluent-plugins-nursery\/.*fluent.*|.+\/fluentd?-plugin-.+|wallynegima\/scenario-manager-plugin)$",
                ),
                hit: true,
                ..T::default()
            },
            T {
                full_name: "WallyNegima/scenario-manager-plugin",
                org_re: Some(
                    r"(?i)^(fluent|fluent-plugins-nursery\/.*fluent.*|.+\/fluentd?-plugin-.+|baritolog\/barito-fluent-plugin|blacknight95\/aws-fluent-plugin-kinesis|sumologic\/fluentd-kubernetes-sumologic|sumologic\/fluentd-output-sumologic|wallynegima\/scenario-manager-plugin|aliyun\/aliyun-odps-fluentd-plugin|awslabs\/aws-fluent-plugin-kinesis|campanja\/fluent-output-router|grafana\/loki\/|jdoconnor\/fluentd_https_out|newrelic\/newrelic-fluentd-output|roma42427\/filter_wms_auth|scalyr\/scalyr-fluentd|sebryu\/fluent_plugin_in_websocket|tagomoris\/fluent-helper-plugin-spec|y-ken\/fluent-mixin-rewrite-tag-name|y-ken\/fluent-mixin-type-converter)$",
                ),
                hit: true,
                ..T::default()
            },
        ];
        let mut ctx = Ctx::default();
        for (i, t) in cases.iter().enumerate() {
            ctx.exclude_repos = t
                .excludes
                .iter()
                .map(|s| (s.to_string(), true))
                .collect::<BTreeMap<_, _>>();
            ctx.exact = t.exact;
            let org_re = t.org_re.and_then(re);
            let repo_re = t.repo_re.and_then(re);
            let got = repo_hit(
                &ctx,
                t.full_name,
                &set(t.forg),
                &set(t.frepo),
                org_re.as_ref(),
                repo_re.as_ref(),
            );
            assert_eq!(got, t.hit, "test number {}: {}", i + 1, t.full_name);
        }
    }

    // gha_test.go: Test*OrNil
    #[test]
    fn or_nil_helpers() {
        let id = |s: SqlArg| s;
        assert_eq!(org_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            org_id_or_nil(Some(&Org {
                id: 2,
                ..Org::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(org_login_or_nil(None), SqlArg::Null);
        assert_eq!(
            org_login_or_nil(Some(&Org {
                login: "cncf".into(),
                ..Org::default()
            })),
            SqlArg::Str("cncf".into())
        );
        assert_eq!(repo_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            repo_id_or_nil(Some(&Repo {
                id: 2,
                ..Repo::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(repo_name_or_nil(None), SqlArg::Null);
        assert_eq!(
            repo_name_or_nil(Some(&Repo {
                name: "kubernetes".into(),
                ..Repo::default()
            })),
            SqlArg::Str("kubernetes".into())
        );
        assert_eq!(issue_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            issue_id_or_nil(Some(&Issue {
                id: 2,
                ..Issue::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(pull_request_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            pull_request_id_or_nil(Some(&PullRequest {
                id: 2,
                ..PullRequest::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(comment_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            comment_id_or_nil(Some(&Comment {
                id: 2,
                ..Comment::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(forkee_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            forkee_id_or_nil(Some(&Forkee {
                id: 2,
                ..Forkee::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(forkee_old_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            forkee_old_id_or_nil(Some(&ForkeeOld {
                id: 2,
                ..ForkeeOld::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(forkee_name_or_nil(None), SqlArg::Null);
        assert_eq!(
            forkee_name_or_nil(Some(&Forkee {
                name: "kubernetes".into(),
                ..Forkee::default()
            })),
            SqlArg::Str("kubernetes".into())
        );
        assert_eq!(actor_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            actor_id_or_nil(Some(&Actor {
                id: 2,
                ..Actor::default()
            })),
            SqlArg::Int(2)
        );
        let ident = |a: &str| a.to_string();
        assert_eq!(actor_login_or_nil(None, &ident), SqlArg::Null);
        assert_eq!(
            actor_login_or_nil(
                Some(&Actor {
                    login: "lukaszgryglicki".into(),
                    ..Actor::default()
                }),
                &ident
            ),
            SqlArg::Str("lukaszgryglicki".into())
        );
        let anon = |_: &str| "anon-1".to_string();
        assert_eq!(
            actor_login_or_nil(
                Some(&Actor {
                    login: "forbidden".into(),
                    ..Actor::default()
                }),
                &anon
            ),
            id(SqlArg::Str("anon-1".into()))
        );
        assert_eq!(release_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            release_id_or_nil(Some(&Release {
                id: 2,
                ..Release::default()
            })),
            SqlArg::Int(2)
        );
        assert_eq!(milestone_id_or_nil(None), SqlArg::Null);
        assert_eq!(
            milestone_id_or_nil(Some(&Milestone {
                id: 2,
                ..Milestone::default()
            })),
            SqlArg::Int(2)
        );
    }

    // gha_test.go: TestCompare*Ptr
    #[test]
    fn compare_ptr_helpers() {
        assert!(compare_string_ptr(None, None));
        assert!(!compare_string_ptr(None, Some("string1")));
        assert!(!compare_string_ptr(Some("string2"), None));
        assert!(!compare_string_ptr(Some("string1"), Some("string2")));
        assert!(compare_string_ptr(Some("string1"), Some("string1")));
        assert!(compare_int_ptr(None, None));
        assert!(!compare_int_ptr(None, Some(1)));
        assert!(!compare_int_ptr(Some(2), None));
        assert!(!compare_int_ptr(Some(1), Some(2)));
        assert!(compare_int_ptr(Some(1), Some(1)));
        assert!(compare_float64_ptr(None, None));
        assert!(!compare_float64_ptr(None, Some(1.1)));
        assert!(!compare_float64_ptr(Some(1.2), None));
        assert!(!compare_float64_ptr(Some(1.1), Some(1.2)));
        assert!(compare_float64_ptr(Some(1.1), Some(1.1)));
        assert!(compare_float64_ptr(Some(1.1), Some(1.10000000001)));
        // Go `TestCompare*Ptr` "&x1, &x3" cases: two distinct variables holding
        // equal values compare equal (by value, not by pointer identity).
        let (s1, s3) = (String::from("string1"), String::from("string1"));
        assert!(compare_string_ptr(Some(s1.as_str()), Some(s3.as_str())));
        let (i1, i3) = (1i64, 1i64);
        assert!(compare_int_ptr(Some(i1), Some(i3)));
        let (f1, f3) = (1.1f64, 1.1f64);
        assert!(compare_float64_ptr(Some(f1), Some(f3)));
    }

    #[test]
    fn make_old_repo_name_cases() {
        let mut f = ForkeeOld {
            name: "kubernetes".into(),
            ..ForkeeOld::default()
        };
        assert_eq!(make_old_repo_name(&f), "kubernetes");
        f.organization = Some(String::new());
        assert_eq!(make_old_repo_name(&f), "kubernetes");
        f.organization = Some("kubernetes".into());
        assert_eq!(make_old_repo_name(&f), "kubernetes/kubernetes");
        f.name.clear();
        assert_eq!(make_old_repo_name(&f), "kubernetes/");
    }

    #[test]
    fn go_rfc3339_parsing() {
        let t = parse_go_rfc3339("2015-01-01T15:00:00Z").unwrap();
        assert_eq!(t.to_rfc3339(), "2015-01-01T15:00:00+00:00");
        let t = parse_go_rfc3339("2014-12-31T23:06:01-08:00").unwrap();
        assert_eq!(t.offset().local_minus_utc(), -8 * 3600);
        assert_eq!(crate::time::to_ymdhms_date(t), "2014-12-31 23:06:01");
        let t = parse_go_rfc3339("2020-05-01T10:00:00.123456789Z").unwrap();
        assert_eq!(t.timestamp_subsec_nanos(), 123_456_789);
        let t = parse_go_rfc3339("2020-05-01T10:00:00.1234567891234Z").unwrap();
        assert_eq!(t.timestamp_subsec_nanos(), 123_456_789);
        assert_eq!(
            parse_go_rfc3339("2012/03/11 12:00:00 -0700").unwrap_err(),
            "parsing time \"2012/03/11 12:00:00 -0700\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"/03/11 12:00:00 -0700\" as \"-\""
        );
        assert_eq!(
            parse_go_rfc3339("2015-01-01 15:00:00").unwrap_err(),
            "parsing time \"2015-01-01 15:00:00\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \" 15:00:00\" as \"T\""
        );
        assert_eq!(
            parse_go_rfc3339("2015-01-01T15:00:00").unwrap_err(),
            "parsing time \"2015-01-01T15:00:00\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"\" as \"Z07:00\""
        );
        assert_eq!(
            parse_go_rfc3339("2015-13-01T15:00:00Z").unwrap_err(),
            "parsing time \"2015-13-01T15:00:00Z\": month out of range"
        );
        assert_eq!(
            parse_go_rfc3339("2015-02-30T01:00:00Z").unwrap_err(),
            "parsing time \"2015-02-30T01:00:00Z\": day out of range"
        );
        assert_eq!(
            parse_go_rfc3339("2015-02-00T01:00:00Z").unwrap_err(),
            "parsing time \"2015-02-00T01:00:00Z\": day out of range"
        );
        assert_eq!(
            parse_go_rfc3339("2016-02-29T01:00:00Z")
                .unwrap()
                .to_rfc3339(),
            "2016-02-29T01:00:00+00:00"
        );
        assert_eq!(
            parse_go_rfc3339("2015-13-40T25:00:00Z").unwrap_err(),
            "parsing time \"2015-13-40T25:00:00Z\": month out of range"
        );
        assert_eq!(
            parse_go_rfc3339("2015-02-30T24:00:00Z").unwrap_err(),
            "parsing time \"2015-02-30T24:00:00Z\": hour out of range"
        );
        assert_eq!(
            parse_go_rfc3339("2015-02-30T23:60:00Z").unwrap_err(),
            "parsing time \"2015-02-30T23:60:00Z\": minute out of range"
        );
        assert_eq!(
            parse_go_rfc3339("2015-02-30T23:59:60Z").unwrap_err(),
            "parsing time \"2015-02-30T23:59:60Z\": second out of range"
        );
        assert!(parse_go_rfc3339("").is_err());
        assert!(parse_go_rfc3339("2015-01-01t15:00:00Z").is_err());
        assert!(parse_go_rfc3339("2015-01-01T15:00:00+0100").is_err());
    }

    #[test]
    fn event_decoding_follows_encoding_json() {
        let js = r#"{"id":"2489651045","type":"CreateEvent","actor":{"id":665991,"login":"petroav","gravatar_id":"","url":"https://api.github.com/users/petroav"},"repo":{"id":28688495,"name":"petroav/6.828"},"payload":{"ref":"master","ref_type":"branch","master_branch":"master","description":null,"pusher_type":"user","commits":null,"pages":[]},"public":true,"created_at":"2015-01-01T15:00:00Z","org":null,"unknown":{"x":[1,2]}}"#;
        let ev: Event = serde_json::from_str(js).unwrap();
        assert_eq!(ev.id, "2489651045");
        assert_eq!(ev.type_, "CreateEvent");
        assert!(ev.public);
        assert_eq!(
            ev.actor,
            Actor {
                id: 665991,
                login: "petroav".into(),
                name: String::new()
            }
        );
        assert_eq!(ev.repo.name, "petroav/6.828");
        assert!(ev.org.is_none());
        assert_eq!(ev.payload.ref_.as_deref(), Some("master"));
        assert_eq!(ev.payload.description, None);
        assert_eq!(ev.payload.commits, None);
        assert_eq!(ev.payload.pages, Some(vec![]));
        assert_eq!(
            crate::time::to_ymdhms_date(*ev.created_at),
            "2015-01-01 15:00:00"
        );

        // null / missing plain fields keep the zero value, `pull_request: {}` is a presence marker
        let js = r#"{"id":null,"type":"IssuesEvent","payload":{"issue":{"id":7,"number":null,"comments":null,"title":null,"locked":null,"labels":null,"assignees":null,"pull_request":{"url":"x"},"created_at":null,"closed_at":null,"user":null}},"created_at":"2016-02-03T04:05:06+02:00"}"#;
        let ev: Event = serde_json::from_str(js).unwrap();
        assert_eq!(ev.id, "");
        let issue = ev.payload.issue.unwrap();
        assert_eq!(issue.id, 7);
        assert_eq!(issue.number, 0);
        assert_eq!(issue.comments, 0);
        assert_eq!(issue.title, "");
        assert!(!issue.locked);
        assert!(issue.labels.is_empty());
        assert!(issue.assignees.is_empty());
        assert_eq!(issue.pull_request, Some(Dummy {}));
        assert_eq!(issue.created_at, zero_time());
        assert_eq!(issue.closed_at, None);
        assert_eq!(issue.user, Actor::default());
        assert_eq!(ev.created_at.offset().local_minus_utc(), 7200);
        let js = r#"{"payload":{"issue":{"id":7,"pull_request":null}}}"#;
        let ev: Event = serde_json::from_str(js).unwrap();
        assert_eq!(ev.payload.issue.unwrap().pull_request, None);
        assert_eq!(ev.created_at, zero_time());

        // type errors are errors (Go: cannot unmarshal … into Go struct field)
        assert!(serde_json::from_str::<Event>(r#"{"id":123}"#).is_err());
        assert!(serde_json::from_str::<Event>(r#"{"created_at":"2015-01-01 15:00:00"}"#).is_err());
        assert!(serde_json::from_str::<Event>(r#"{"created_at":123}"#).is_err());
        assert!(
            serde_json::from_str::<Event>(r#"{"payload":{"issue":{"pull_request":"yes"}}}"#)
                .is_err()
        );
    }

    #[test]
    fn old_event_decoding() {
        let js = r#"{"created_at":"2014-12-31T23:06:01-08:00","payload":{"shas":[["298cdbc7","ef60@gmail.com","Updating data","Kin Lane",true]],"size":1,"ref":"refs/heads/gh-pages","head":"298cdbc7"},"public":true,"type":"PushEvent","url":"https://github.com/kinlane/api-stack/compare/792bb9995b...298cdbc7fe","actor":"kinlane","actor_attributes":{"login":"kinlane"},"repository":{"id":10,"name":"api-stack","owner":"kinlane","organization":"kin-org","private":false,"master_branch":"gh-pages","pushed_at":"2014-12-31T23:06:00-08:00","stargazers":3,"watchers":3,"forks":1,"open_issues":0,"created_at":"2014-01-01T00:00:00-08:00","language":null}}"#;
        let ev: EventOld = serde_json::from_str(js).unwrap();
        assert_eq!(ev.id, "");
        assert_eq!(ev.type_, "PushEvent");
        assert_eq!(ev.actor, "kinlane");
        assert_eq!(ev.repository.id, 10);
        assert_eq!(ev.repository.owner, "kinlane");
        assert_eq!(ev.repository.organization.as_deref(), Some("kin-org"));
        assert_eq!(ev.repository.default_branch, "gh-pages");
        assert_eq!(ev.repository.private, Some(false));
        assert_eq!(ev.repository.language, None);
        assert_eq!(make_old_repo_name(&ev.repository), "kin-org/api-stack");
        let pl = ev.payload.unwrap();
        let shas = pl.shas.unwrap();
        assert_eq!(shas.len(), 1);
        assert_eq!(shas[0][0], "298cdbc7");
        assert_eq!(shas[0][4], true);
        assert_eq!(pl.size, Some(1));
        assert_eq!(
            crate::time::to_ymdhms_date(*ev.created_at),
            "2014-12-31 23:06:01"
        );

        // no payload, no repository
        let ev: EventOld = serde_json::from_str(
            r#"{"type":"PublicEvent","actor":"x","created_at":"2014-12-31T23:06:01-08:00"}"#,
        )
        .unwrap();
        assert!(ev.payload.is_none());
        assert_eq!(ev.repository, ForkeeOld::default());
        assert_eq!(make_old_repo_name(&ev.repository), "");
        // `id` is `json:"-"`
        let ev: EventOld = serde_json::from_str(r#"{"id":"abc","type":"PublicEvent"}"#).unwrap();
        assert_eq!(ev.id, "");
        // the 2012 time format is rejected like Go does
        let err = serde_json::from_str::<EventOld>(r#"{"created_at":"2012/03/11 12:00:00 -0700"}"#)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot parse \"/03/11 12:00:00 -0700\" as \"-\""),
            "{err}"
        );
    }

    #[test]
    fn skip_dates_yaml() {
        let data = b"---\nskip_dates:\n  - 2016-10-21 18:00:00\n  - 2016-10-21T19:00:00Z\n  - '2016-10-21T20:00:00Z'\n  - 2016-10-22 5:00:00\n";
        let list: SkipDatesList = crate::yamlv2::de::unmarshal(data).unwrap();
        let hours: Vec<String> = list
            .dates
            .iter()
            .map(|d| crate::time::to_ymdh_date(*d))
            .collect();
        assert_eq!(
            hours,
            [
                "2016-10-21 18",
                "2016-10-21 19",
                "2016-10-21 20",
                "2016-10-22 5"
            ]
        );
        let list: SkipDatesList = crate::yamlv2::de::unmarshal(b"---\nskip_dates: []\n").unwrap();
        assert!(list.dates.is_empty());
        let list: SkipDatesList = crate::yamlv2::de::unmarshal(b"---\nother: 1\n").unwrap();
        assert!(list.dates.is_empty());
        let list: SkipDatesList = crate::yamlv2::de::unmarshal(b"").unwrap();
        assert!(list.dates.is_empty());
        assert!(crate::yamlv2::de::unmarshal::<SkipDatesList>(
            b"---\nskip_dates:\n  - not a date\n"
        )
        .is_err());
    }
}
