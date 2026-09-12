//! The subset of `github.com/google/go-github/v38` the tools use: a REST
//! client with go-github's rate-limit bookkeeping (a request is not even
//! sent while the last known rate limit of its category is exhausted),
//! `Link` header pagination, its error types with their exact wording
//! (`ErrorResponse`, `RateLimitError`, `AbuseRateLimitError`,
//! `AcceptedError`, `*url.Error`) and the resource structs (`Issue`,
//! `PullRequest`, `IssueEvent`, …) with Go's pointer semantics (`Option`).
//!
//! The base URL is `https://api.github.com/` unless `GHA2DB_GITHUB_API_URL`
//! is set (GitHub Enterprise / the Go⇄Rust test servers) — the same knob the
//! Go `GHClient` honours.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use serde::de::{self, Deserializer};
use serde::Deserialize;

use crate::error::go_io_error_string;
use crate::gofmt;

/// go-github `mediaTypeV3`.
pub const MEDIA_TYPE_V3: &str = "application/vnd.github.v3+json";
/// go-github `mediaTypeReactionsPreview` (`Issues.Get`, comments).
pub const MEDIA_TYPE_REACTIONS_PREVIEW: &str = "application/vnd.github.squirrel-girl-preview";
/// go-github `mediaTypeMultiLineCommentsPreview` (`PullRequests.ListComments`).
pub const MEDIA_TYPE_MULTI_LINE_COMMENTS_PREVIEW: &str =
    "application/vnd.github.comfort-fade-preview+json";
/// go-github `mediaTypeTopicsPreview` (`Repositories.ListForks`).
pub const MEDIA_TYPE_TOPICS_PREVIEW: &str = "application/vnd.github.mercy-preview+json";
/// The `User-Agent` sent (go-github sends `go-github/38.1.0`).
pub const USER_AGENT: &str = "devstatscode-rust";
/// The default API endpoint.
pub const DEFAULT_BASE_URL: &str = "https://api.github.com/";
/// Environment variable overriding the API endpoint (Go and Rust alike).
pub const BASE_URL_ENV: &str = "GHA2DB_GITHUB_API_URL";

// ---------------------------------------------------------------------------
// Times
// ---------------------------------------------------------------------------

/// A Go `time.Time` decoded from JSON (RFC 3339, the offset is kept).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GoTime(pub DateTime<FixedOffset>);

impl GoTime {
    /// The instant in UTC.
    pub fn utc(&self) -> DateTime<Utc> {
        self.0.with_timezone(&Utc)
    }
}

impl<'de> Deserialize<'de> for GoTime {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_rfc3339(&s).map(GoTime).map_err(de::Error::custom)
    }
}

/// Go `time.Time.UnmarshalJSON` (RFC 3339 with optional fractional seconds).
pub fn parse_rfc3339(s: &str) -> Result<DateTime<FixedOffset>, String> {
    DateTime::parse_from_rfc3339(s).map_err(|_| {
        format!(
            "parsing time {:?} as \"2006-01-02T15:04:05Z07:00\": cannot parse {:?} as \"2006\"",
            s, s
        )
    })
}

/// go-github `Timestamp`: a unix time (seconds, or milliseconds when the
/// seconds reading would be past the year 3000) or an RFC 3339 string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timestamp(pub DateTime<Utc>);

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Int(i64),
            Str(String),
        }
        match Raw::deserialize(d)? {
            Raw::Int(i) => {
                let mut t = Utc.timestamp_opt(i, 0).single();
                if let Some(tt) = t {
                    if chrono::Datelike::year(&tt) > 3000 {
                        t = Utc.timestamp_millis_opt(i).single();
                    }
                }
                t.map(Timestamp)
                    .ok_or_else(|| de::Error::custom("timestamp out of range"))
            }
            Raw::Str(s) => parse_rfc3339(&s)
                .map(|t| Timestamp(t.with_timezone(&Utc)))
                .map_err(de::Error::custom),
        }
    }
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// go-github `User` (the fields the tools read).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct User {
    pub login: Option<String>,
    pub id: Option<i64>,
    pub name: Option<String>,
    pub email: Option<String>,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub site_admin: Option<bool>,
}

impl User {
    /// A user with just an id and a login (Go `&github.User{ID: &id, Login: &login}`).
    pub fn id_login(id: i64, login: &str) -> Self {
        User {
            id: Some(id),
            login: Some(login.to_string()),
            ..Default::default()
        }
    }
    /// Go `GetName()`.
    pub fn get_name(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }
    /// Go `GetLogin()`.
    pub fn get_login(&self) -> &str {
        self.login.as_deref().unwrap_or("")
    }
}

/// go-github `Label`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Label {
    pub id: Option<i64>,
    pub url: Option<String>,
    pub name: Option<String>,
    pub color: Option<String>,
    pub description: Option<String>,
    pub default: Option<bool>,
    pub node_id: Option<String>,
}

/// go-github `Milestone`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Milestone {
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub labels_url: Option<String>,
    pub id: Option<i64>,
    pub number: Option<i64>,
    pub state: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub creator: Option<User>,
    pub open_issues: Option<i64>,
    pub closed_issues: Option<i64>,
    pub created_at: Option<GoTime>,
    pub updated_at: Option<GoTime>,
    pub closed_at: Option<GoTime>,
    pub due_on: Option<GoTime>,
    pub node_id: Option<String>,
}

/// go-github `PullRequestLinks` (the presence marks an issue as a PR).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PullRequestLinks {
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub diff_url: Option<String>,
    pub patch_url: Option<String>,
}

/// go-github `Repository` (the fields the tools read).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Repository {
    pub id: Option<i64>,
    pub node_id: Option<String>,
    pub owner: Option<User>,
    pub name: Option<String>,
    pub full_name: Option<String>,
    pub description: Option<String>,
    pub html_url: Option<String>,
    pub url: Option<String>,
    pub fork: Option<bool>,
    pub created_at: Option<Timestamp>,
    pub pushed_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub language: Option<String>,
    pub forks_count: Option<i64>,
    pub stargazers_count: Option<i64>,
    pub watchers_count: Option<i64>,
    pub open_issues_count: Option<i64>,
    pub default_branch: Option<String>,
    pub license: Option<License>,
    pub private: Option<bool>,
    pub homepage: Option<String>,
    pub size: Option<i64>,
    pub has_issues: Option<bool>,
    pub has_wiki: Option<bool>,
    pub has_pages: Option<bool>,
    pub has_projects: Option<bool>,
    pub has_downloads: Option<bool>,
    pub organization: Option<User>,
}

impl Repository {
    /// Go `GetOwner()` (a nil owner reads as an empty user).
    pub fn get_owner(&self) -> User {
        self.owner.clone().unwrap_or_default()
    }
    /// Go `GetName()`.
    pub fn get_name(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }
}

/// go-github `License`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct License {
    pub key: Option<String>,
    pub name: Option<String>,
    pub url: Option<String>,
    pub spdx_id: Option<String>,
    pub html_url: Option<String>,
    pub featured: Option<bool>,
    pub description: Option<String>,
    pub implementation: Option<String>,
    pub permissions: Option<Vec<String>>,
    pub conditions: Option<Vec<String>>,
    pub limitations: Option<Vec<String>>,
    pub body: Option<String>,
}

/// go-github `Issue`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Issue {
    pub id: Option<i64>,
    pub number: Option<i64>,
    pub state: Option<String>,
    pub locked: Option<bool>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub author_association: Option<String>,
    pub user: Option<User>,
    pub labels: Vec<Label>,
    pub assignee: Option<User>,
    pub comments: Option<i64>,
    pub closed_at: Option<GoTime>,
    pub created_at: Option<GoTime>,
    pub updated_at: Option<GoTime>,
    pub closed_by: Option<User>,
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub milestone: Option<Milestone>,
    #[serde(rename = "pull_request")]
    pub pull_request_links: Option<PullRequestLinks>,
    pub repository: Option<Repository>,
    pub assignees: Vec<User>,
    pub node_id: Option<String>,
    pub active_lock_reason: Option<String>,
}

impl Issue {
    /// Go `IsPullRequest()`.
    pub fn is_pull_request(&self) -> bool {
        self.pull_request_links.is_some()
    }
    /// Go `GetNumber()`.
    pub fn get_number(&self) -> i64 {
        self.number.unwrap_or(0)
    }
    /// Go `GetRepository()` (a nil repository reads as an empty one).
    pub fn get_repository(&self) -> Repository {
        self.repository.clone().unwrap_or_default()
    }
}

/// go-github `PullRequestBranch`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PullRequestBranch {
    pub label: Option<String>,
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub sha: Option<String>,
    pub repo: Option<Repository>,
    pub user: Option<User>,
}

/// go-github `PullRequest`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PullRequest {
    pub id: Option<i64>,
    pub number: Option<i64>,
    pub state: Option<String>,
    pub locked: Option<bool>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub created_at: Option<GoTime>,
    pub updated_at: Option<GoTime>,
    pub closed_at: Option<GoTime>,
    pub merged_at: Option<GoTime>,
    pub labels: Vec<Label>,
    pub user: Option<User>,
    pub draft: Option<bool>,
    pub merged: Option<bool>,
    pub mergeable: Option<bool>,
    pub mergeable_state: Option<String>,
    pub merged_by: Option<User>,
    pub merge_commit_sha: Option<String>,
    pub rebaseable: Option<bool>,
    pub comments: Option<i64>,
    pub commits: Option<i64>,
    pub additions: Option<i64>,
    pub deletions: Option<i64>,
    pub changed_files: Option<i64>,
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub review_comments: Option<i64>,
    pub assignee: Option<User>,
    pub assignees: Vec<User>,
    pub milestone: Option<Milestone>,
    pub maintainer_can_modify: Option<bool>,
    pub author_association: Option<String>,
    pub node_id: Option<String>,
    pub requested_reviewers: Vec<User>,
    pub head: Option<PullRequestBranch>,
    pub base: Option<PullRequestBranch>,
    pub active_lock_reason: Option<String>,
}

/// go-github `Rename` (issue `renamed` events).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Rename {
    pub from: Option<String>,
    pub to: Option<String>,
}

/// go-github `IssueEvent`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct IssueEvent {
    pub id: Option<i64>,
    pub url: Option<String>,
    pub actor: Option<User>,
    pub event: Option<String>,
    pub created_at: Option<GoTime>,
    pub issue: Option<Issue>,
    pub assignee: Option<User>,
    pub assigner: Option<User>,
    pub commit_id: Option<String>,
    pub milestone: Option<Milestone>,
    pub label: Option<Label>,
    pub rename: Option<Rename>,
    pub lock_reason: Option<String>,
}

/// go-github `CommitAuthor` (the git author/committer of a commit).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct CommitAuthor {
    pub date: Option<GoTime>,
    pub name: Option<String>,
    pub email: Option<String>,
    #[serde(rename = "username")]
    pub login: Option<String>,
}

/// go-github `Commit` (the git-level part of a `RepositoryCommit`).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Commit {
    pub sha: Option<String>,
    pub author: Option<CommitAuthor>,
    pub committer: Option<CommitAuthor>,
    pub message: Option<String>,
    pub html_url: Option<String>,
    pub url: Option<String>,
    pub node_id: Option<String>,
    pub comment_count: Option<i64>,
}

/// go-github `RepositoryCommit` (`Repositories.ListCommits`).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RepositoryCommit {
    pub node_id: Option<String>,
    pub sha: Option<String>,
    pub commit: Option<Commit>,
    pub author: Option<User>,
    pub committer: Option<User>,
    pub html_url: Option<String>,
    pub url: Option<String>,
    pub comments_url: Option<String>,
}

/// go-github `RepositoryLicense` (`Repositories.License`).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RepositoryLicense {
    pub name: Option<String>,
    pub path: Option<String>,
    pub sha: Option<String>,
    pub size: Option<i64>,
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub git_url: Option<String>,
    pub download_url: Option<String>,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub content: Option<String>,
    pub encoding: Option<String>,
    pub license: Option<License>,
}

/// go-github `IssueComment`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct IssueComment {
    pub id: Option<i64>,
    pub node_id: Option<String>,
    pub body: Option<String>,
    pub user: Option<User>,
    pub created_at: Option<GoTime>,
    pub updated_at: Option<GoTime>,
    pub author_association: Option<String>,
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub issue_url: Option<String>,
}

/// go-github `PullRequestComment` (a review comment).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PullRequestComment {
    pub id: Option<i64>,
    pub node_id: Option<String>,
    #[serde(rename = "in_reply_to_id")]
    pub in_reply_to: Option<i64>,
    pub body: Option<String>,
    pub path: Option<String>,
    pub diff_hunk: Option<String>,
    pub pull_request_review_id: Option<i64>,
    pub position: Option<i64>,
    pub original_position: Option<i64>,
    pub start_line: Option<i64>,
    pub line: Option<i64>,
    pub original_line: Option<i64>,
    pub original_start_line: Option<i64>,
    pub side: Option<String>,
    pub start_side: Option<String>,
    pub commit_id: Option<String>,
    pub original_commit_id: Option<String>,
    pub user: Option<User>,
    pub created_at: Option<GoTime>,
    pub updated_at: Option<GoTime>,
    pub author_association: Option<String>,
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub pull_request_url: Option<String>,
}

/// go-github `RepositoryComment` (a commit comment).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RepositoryComment {
    pub html_url: Option<String>,
    pub url: Option<String>,
    pub id: Option<i64>,
    pub node_id: Option<String>,
    pub commit_id: Option<String>,
    pub user: Option<User>,
    pub created_at: Option<GoTime>,
    pub updated_at: Option<GoTime>,
    pub body: Option<String>,
    pub path: Option<String>,
    pub position: Option<i64>,
}

/// go-github `PullRequestReview`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PullRequestReview {
    pub id: Option<i64>,
    pub node_id: Option<String>,
    pub user: Option<User>,
    pub body: Option<String>,
    pub submitted_at: Option<GoTime>,
    pub commit_id: Option<String>,
    pub html_url: Option<String>,
    pub pull_request_url: Option<String>,
    pub state: Option<String>,
    pub author_association: Option<String>,
}

/// go-github `ReleaseAsset`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ReleaseAsset {
    pub id: Option<i64>,
    pub url: Option<String>,
    pub name: Option<String>,
    pub label: Option<String>,
    pub state: Option<String>,
    pub content_type: Option<String>,
    pub size: Option<i64>,
    pub download_count: Option<i64>,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub browser_download_url: Option<String>,
    pub uploader: Option<User>,
    pub node_id: Option<String>,
}

/// go-github `RepositoryRelease`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RepositoryRelease {
    pub tag_name: Option<String>,
    pub target_commitish: Option<String>,
    pub name: Option<String>,
    pub body: Option<String>,
    pub draft: Option<bool>,
    pub prerelease: Option<bool>,
    pub discussion_category_name: Option<String>,
    pub id: Option<i64>,
    pub created_at: Option<Timestamp>,
    pub published_at: Option<Timestamp>,
    pub url: Option<String>,
    pub html_url: Option<String>,
    pub assets_url: Option<String>,
    pub assets: Vec<ReleaseAsset>,
    pub upload_url: Option<String>,
    pub zipball_url: Option<String>,
    pub tarball_url: Option<String>,
    pub author: Option<User>,
    pub node_id: Option<String>,
}

/// go-github `Stargazer`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Stargazer {
    pub starred_at: Option<Timestamp>,
    pub user: Option<User>,
}

// ---------------------------------------------------------------------------
// Rate limits, responses, errors
// ---------------------------------------------------------------------------

/// go-github `Rate`.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Rate {
    pub limit: i64,
    pub remaining: i64,
    /// The reset time; `None` is Go's zero `Timestamp`.
    pub reset: Option<Timestamp>,
}

impl Rate {
    /// Go `Rate.Reset.Time` (the zero time when unset).
    pub fn reset_time(&self) -> DateTime<Utc> {
        self.reset
            .map(|t| t.0)
            .unwrap_or_else(|| Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap())
    }
}

/// go-github `RateLimits` (`/rate_limit` → `resources`).
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RateLimits {
    pub core: Option<Rate>,
    pub search: Option<Rate>,
}

/// go-github `Response`: the HTTP status, the rate headers and the `Link`
/// pagination values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub rate: Rate,
    pub next_page: i64,
    pub prev_page: i64,
    pub first_page: i64,
    pub last_page: i64,
    pub next_page_token: String,
    pub cursor: String,
}

/// go-github `Error` (one entry of an `ErrorResponse`'s `errors`).
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ApiError {
    pub resource: String,
    pub field: String,
    pub code: String,
    pub message: String,
}

impl fmt::Display for ApiError {
    /// Go `%+v` of the struct.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{Resource:{} Field:{} Code:{} Message:{}}}",
            self.resource, self.field, self.code, self.message
        )
    }
}

/// The errors go-github returns, worded like Go.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// `*github.ErrorResponse` (any non-2xx response not covered below).
    Api {
        method: String,
        url: String,
        status: u16,
        message: String,
        errors: Vec<ApiError>,
        documentation_url: String,
    },
    /// `*github.RateLimitError`: a 403 with `X-RateLimit-Remaining: 0`, or
    /// the client refusing to send while its last known limit is exhausted.
    RateLimit {
        method: String,
        url: String,
        status: u16,
        message: String,
        rate: Rate,
    },
    /// `*github.AbuseRateLimitError`: a 403 whose `documentation_url` ends
    /// with `#abuse-rate-limits`.
    Abuse {
        method: String,
        url: String,
        status: u16,
        message: String,
        retry_after: Option<Duration>,
    },
    /// `*github.AcceptedError` (a 202).
    Accepted,
    /// `*url.Error`: the request could not be performed.
    Url {
        op: String,
        url: String,
        cause: String,
    },
    /// A JSON decoding error (`*json.SyntaxError` / `*json.UnmarshalTypeError`).
    Json {
        go_type: &'static str,
        message: String,
    },
}

impl Error {
    /// Go `%T` of the error.
    pub fn go_type(&self) -> &'static str {
        match self {
            Error::Api { .. } => "*github.ErrorResponse",
            Error::RateLimit { .. } => "*github.RateLimitError",
            Error::Abuse { .. } => "*github.AbuseRateLimitError",
            Error::Accepted => "*github.AcceptedError",
            Error::Url { .. } => "*url.Error",
            Error::Json { go_type, .. } => go_type,
        }
    }
    pub fn is_rate_limit(&self) -> bool {
        matches!(self, Error::RateLimit { .. })
    }
    pub fn is_abuse(&self) -> bool {
        matches!(self, Error::Abuse { .. })
    }
    /// The HTTP status of an API error (0 for the others).
    pub fn status(&self) -> u16 {
        match self {
            Error::Api { status, .. }
            | Error::RateLimit { status, .. }
            | Error::Abuse { status, .. } => *status,
            _ => 0,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Api {
                method,
                url,
                status,
                message,
                errors,
                ..
            } => {
                write!(f, "{method} {url}: {status} {message} [")?;
                for (i, e) in errors.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{e}")?;
                }
                write!(f, "]")
            }
            Error::RateLimit {
                method,
                url,
                status,
                message,
                rate,
            } => {
                let until = rate.reset_time().signed_duration_since(Utc::now());
                write!(
                    f,
                    "{method} {url}: {status} {message} {}",
                    format_rate_reset(until)
                )
            }
            Error::Abuse {
                method,
                url,
                status,
                message,
                ..
            } => write!(f, "{method} {url}: {status} {message}"),
            Error::Accepted => write!(f, "job scheduled on GitHub side; try again later"),
            Error::Url { op, url, cause } => write!(f, "{op} {url:?}: {cause}"),
            Error::Json { message, .. } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {}

/// go-github `formatRateReset`.
pub fn format_rate_reset(d: chrono::Duration) -> String {
    let negative = d < chrono::Duration::zero();
    let d = if negative { -d } else { d };
    let seconds_total = (0.5 + d.num_milliseconds() as f64 / 1000.0) as i64;
    let minutes = seconds_total / 60;
    let seconds = seconds_total - minutes * 60;
    let time_string = if minutes > 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{seconds}s")
    };
    if negative {
        format!("[rate limit was reset {time_string} ago]")
    } else {
        format!("[rate reset in {time_string}]")
    }
}

/// go-github `sanitizeURL`: `client_secret` query values are redacted.
fn sanitize_url(url: &str) -> String {
    if !url.contains("client_secret=") {
        return url.to_string();
    }
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, q),
        None => return url.to_string(),
    };
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (p.to_string(), String::new()),
        })
        .collect();
    for (k, v) in pairs.iter_mut() {
        if k == "client_secret" {
            *v = "REDACTED".to_string();
        }
    }
    // Go's `url.Values.Encode()` sorts by key.
    pairs.sort();
    let q: Vec<String> = pairs.into_iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{base}?{}", q.join("&"))
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// go-github `ListOptions`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ListOptions {
    pub page: i64,
    pub per_page: i64,
}

impl ListOptions {
    fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        if self.page != 0 {
            q.push(("page".to_string(), self.page.to_string()));
        }
        if self.per_page != 0 {
            q.push(("per_page".to_string(), self.per_page.to_string()));
        }
        q
    }
}

/// Go `time.Time.Format(time.RFC3339)` of a query parameter (go-querystring).
fn rfc3339_param(t: &DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Go `url.Values.Encode()`: sorted by key (then value).
fn sorted_query(mut q: Vec<(String, String)>) -> Vec<(String, String)> {
    q.sort();
    q
}

fn push_str(q: &mut Vec<(String, String)>, key: &str, v: &str) {
    if !v.is_empty() {
        q.push((key.to_string(), v.to_string()));
    }
}

fn push_time(q: &mut Vec<(String, String)>, key: &str, v: Option<&DateTime<Utc>>) {
    if let Some(t) = v {
        q.push((key.to_string(), rfc3339_param(t)));
    }
}

/// go-github `CommitsListOptions` (`None` times are Go's omitted zero times).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitsListOptions {
    pub sha: String,
    pub path: String,
    pub author: String,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub list: ListOptions,
}

impl CommitsListOptions {
    fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        push_str(&mut q, "sha", &self.sha);
        push_str(&mut q, "path", &self.path);
        push_str(&mut q, "author", &self.author);
        push_time(&mut q, "since", self.since.as_ref());
        push_time(&mut q, "until", self.until.as_ref());
        q.extend(self.list.query());
        sorted_query(q)
    }
}

/// go-github `IssueListCommentsOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IssueListCommentsOptions {
    pub sort: Option<String>,
    pub direction: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub list: ListOptions,
}

impl IssueListCommentsOptions {
    fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        if let Some(v) = &self.sort {
            q.push(("sort".to_string(), v.clone()));
        }
        if let Some(v) = &self.direction {
            q.push(("direction".to_string(), v.clone()));
        }
        push_time(&mut q, "since", self.since.as_ref());
        q.extend(self.list.query());
        sorted_query(q)
    }
}

/// go-github `PullRequestListCommentsOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PullRequestListCommentsOptions {
    pub sort: String,
    pub direction: String,
    pub since: Option<DateTime<Utc>>,
    pub list: ListOptions,
}

impl PullRequestListCommentsOptions {
    fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        push_str(&mut q, "sort", &self.sort);
        push_str(&mut q, "direction", &self.direction);
        push_time(&mut q, "since", self.since.as_ref());
        q.extend(self.list.query());
        sorted_query(q)
    }
}

/// go-github `PullRequestListOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PullRequestListOptions {
    pub state: String,
    pub head: String,
    pub base: String,
    pub sort: String,
    pub direction: String,
    pub list: ListOptions,
}

impl PullRequestListOptions {
    fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        push_str(&mut q, "state", &self.state);
        push_str(&mut q, "head", &self.head);
        push_str(&mut q, "base", &self.base);
        push_str(&mut q, "sort", &self.sort);
        push_str(&mut q, "direction", &self.direction);
        q.extend(self.list.query());
        sorted_query(q)
    }
}

/// go-github `RepositoryListForksOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RepositoryListForksOptions {
    pub sort: String,
    pub list: ListOptions,
}

impl RepositoryListForksOptions {
    fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        push_str(&mut q, "sort", &self.sort);
        q.extend(self.list.query());
        sorted_query(q)
    }
}

/// Go's `(value, *Response, error)` triple of a go-github call: `response`
/// is `None` only when no HTTP exchange happened (a transport error),
/// `value` is `Some` only without an error.
#[derive(Clone, Debug, PartialEq)]
pub struct ApiResult<T> {
    pub value: Option<T>,
    pub response: Option<Response>,
    pub error: Option<Error>,
}

impl<T> ApiResult<T> {
    /// The HTTP status (0 without a response).
    pub fn status(&self) -> u16 {
        self.response.as_ref().map(|r| r.status).unwrap_or(0)
    }
    /// `Ok((value, response))` or the error.
    pub fn into_result(self) -> Result<(T, Response), Error> {
        match (self.value, self.response, self.error) {
            (Some(v), Some(r), None) => Ok((v, r)),
            (_, _, Some(e)) => Err(e),
            (_, _, None) => Err(Error::Json {
                go_type: "*json.SyntaxError",
                message: "unexpected end of JSON input".to_string(),
            }),
        }
    }
}

const CATEGORY_CORE: usize = 0;
const CATEGORY_SEARCH: usize = 1;

/// A go-github `Client`: one token (or none), its own rate-limit cache.
pub struct Client {
    base_url: String,
    token: Option<String>,
    agent: ureq::Agent,
    rate_limits: Mutex<[Rate; 2]>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("base_url", &self.base_url)
            .field("token", &self.token.as_ref().map(|_| "***"))
            .finish()
    }
}

/// The API base URL: `GHA2DB_GITHUB_API_URL` or `https://api.github.com/`
/// (a trailing slash is appended when missing).
pub fn base_url() -> String {
    let mut u = std::env::var(BASE_URL_ENV).unwrap_or_default();
    if u.is_empty() {
        u = DEFAULT_BASE_URL.to_string();
    }
    if !u.ends_with('/') {
        u.push('/');
    }
    u
}

impl Client {
    /// Go `github.NewClient(nil)` / `github.NewClient(oauth2.NewClient(...))`
    /// with the default (or `GHA2DB_GITHUB_API_URL`) endpoint.
    pub fn new(token: Option<&str>) -> Client {
        Self::with_base_url(token, &base_url())
    }

    /// A client for another endpoint (`client.BaseURL = …`); a trailing
    /// slash is appended when missing, an empty URL means the default.
    pub fn with_base_url(token: Option<&str>, base: &str) -> Client {
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .timeout_global(Some(Duration::from_secs(300)))
            .build();
        let mut base = base.to_string();
        if base.is_empty() {
            base = DEFAULT_BASE_URL.to_string();
        }
        if !base.ends_with('/') {
            base.push('/');
        }
        Client {
            base_url: base,
            token: token.map(|t| t.to_string()),
            agent: config.into(),
            rate_limits: Mutex::new([Rate::default(); 2]),
        }
    }

    /// The configured base URL (with its trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The URL a relative API path resolves to.
    pub fn url(&self, path: &str, query: &[(String, String)]) -> String {
        let mut u = format!("{}{}", self.base_url, path);
        if !query.is_empty() {
            let q: Vec<String> = query
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        crate::gourl::query_escape(k),
                        crate::gourl::query_escape(v)
                    )
                })
                .collect();
            u.push('?');
            u.push_str(&q.join("&"));
        }
        u
    }

    fn category(path: &str) -> usize {
        if path.starts_with("search/") {
            CATEGORY_SEARCH
        } else {
            CATEGORY_CORE
        }
    }

    fn cached_rate(&self, category: usize) -> Rate {
        self.rate_limits.lock().unwrap_or_else(|p| p.into_inner())[category]
    }

    fn store_rate(&self, category: usize, rate: Rate) {
        self.rate_limits.lock().unwrap_or_else(|p| p.into_inner())[category] = rate;
    }

    /// go-github `checkRateLimitBeforeDo`.
    fn check_rate_limit_before_do(
        &self,
        method: &str,
        url: &str,
        category: usize,
    ) -> Option<Error> {
        let rate = self.cached_rate(category);
        if let Some(reset) = rate.reset {
            if rate.remaining == 0 && Utc::now() < reset.0 {
                return Some(Error::RateLimit {
                    method: method.to_string(),
                    url: sanitize_url(url),
                    status: 403,
                    message: format!(
                        "API rate limit of {} still exceeded until {}, not making remote request.",
                        rate.limit,
                        gofmt::time(reset.0.with_timezone(&chrono::Local))
                    ),
                    rate,
                });
            }
        }
        None
    }

    /// go-github `Do`: perform a `GET` of a relative path and decode the
    /// body. `accept` overrides the default `Accept` media type.
    pub fn get_json<T: for<'de> Deserialize<'de> + Default>(
        &self,
        path: &str,
        query: &[(String, String)],
        accept: Option<&str>,
    ) -> Result<(T, Response), Error> {
        self.do_json("GET", path, query, accept, None, false)
    }

    /// go-github `BareDo` + JSON decoding (`bypass` skips the pre-flight
    /// rate-limit check like `RateLimits` does).
    fn do_json<T: for<'de> Deserialize<'de> + Default>(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        accept: Option<&str>,
        body: Option<&[u8]>,
        bypass: bool,
    ) -> Result<(T, Response), Error> {
        self.do_json_full(method, path, query, accept, body, bypass)
            .into_result()
    }

    /// go-github `Do` with Go's full `(value, response, error)` outcome.
    fn do_json_full<T: for<'de> Deserialize<'de> + Default>(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        accept: Option<&str>,
        body: Option<&[u8]>,
        bypass: bool,
    ) -> ApiResult<T> {
        let (response, data, error) = self.bare_do_full(method, path, query, accept, body, bypass);
        if let Some(e) = error {
            return ApiResult {
                value: None,
                response,
                error: Some(e),
            };
        }
        // Go: an `io.EOF` decode error of an empty body is ignored, and a JSON
        // `null` leaves the (zero) target untouched without an error.
        let trimmed: Vec<u8> = data
            .iter()
            .copied()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        if trimmed.is_empty() || trimmed == b"null" {
            return ApiResult {
                value: Some(T::default()),
                response,
                error: None,
            };
        }
        match serde_json::from_slice::<T>(&data) {
            Ok(v) => ApiResult {
                value: Some(v),
                response,
                error: None,
            },
            Err(e) => ApiResult {
                value: None,
                response,
                error: Some(json_error(&e)),
            },
        }
    }

    /// go-github `BareDo`: the request, rate bookkeeping, `CheckResponse`.
    /// Returns the response and the raw body.
    pub fn bare_do(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        accept: Option<&str>,
        body: Option<&[u8]>,
        bypass: bool,
    ) -> Result<(Response, Vec<u8>), Error> {
        match self.bare_do_full(method, path, query, accept, body, bypass) {
            (_, _, Some(e)) => Err(e),
            (Some(r), data, None) => Ok((r, data)),
            (None, _, None) => unreachable!("a response or an error"),
        }
    }

    /// go-github `BareDo` returning Go's `(response, error)` pair plus the
    /// body: the response is present with the error for a failed HTTP
    /// exchange (`CheckResponse`) and absent for a transport failure; the
    /// pre-flight rate-limit refusal comes with a synthetic 403 response.
    pub fn bare_do_full(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        accept: Option<&str>,
        body: Option<&[u8]>,
        bypass: bool,
    ) -> (Option<Response>, Vec<u8>, Option<Error>) {
        let url = self.url(path, query);
        let category = Self::category(path);
        if !bypass {
            if let Some(err) = self.check_rate_limit_before_do(method, &url, category) {
                let rate = match &err {
                    Error::RateLimit { rate, .. } => *rate,
                    _ => Rate::default(),
                };
                let response = Response {
                    status: 403,
                    rate,
                    ..Default::default()
                };
                return (Some(response), Vec::new(), Some(err));
            }
        }
        let op = go_title(method);
        // Go's `http.Client` follows up to 10 redirects itself: 301/302/303
        // turn a body-carrying request into a GET, 307/308 keep everything,
        // a 3xx without `Location` is returned as-is (go-github then reports
        // e.g. `301 Moved Permanently`) and `Authorization` travels only to
        // the same host (or a subdomain of it).
        let initial_host = url_host(&url);
        let mut cur_url = url.clone();
        let mut cur_method = method.to_string();
        let mut cur_body: Option<&[u8]> = body;
        let mut hops = 0;
        let mut http_resp = loop {
            let mut req = ureq::http::Request::builder()
                .method(cur_method.as_str())
                .uri(&cur_url)
                .header("Accept", accept.unwrap_or(MEDIA_TYPE_V3))
                .header("User-Agent", USER_AGENT);
            if let Some(t) = &self.token {
                if is_domain_or_subdomain(&url_host(&cur_url), &initial_host) {
                    req = req.header("Authorization", format!("Bearer {t}"));
                }
            }
            if cur_body.is_some() {
                req = req.header("Content-Type", "application/json");
            }
            let request = match req.body(cur_body.unwrap_or(&[]).to_vec()) {
                Ok(r) => r,
                Err(e) => {
                    return (
                        None,
                        Vec::new(),
                        Some(Error::Url {
                            op: op.clone(),
                            url: sanitize_url(&cur_url),
                            cause: e.to_string(),
                        }),
                    )
                }
            };
            let resp = match self.agent.run(request) {
                Ok(r) => r,
                Err(e) => {
                    return (
                        None,
                        Vec::new(),
                        Some(Error::Url {
                            op: op.clone(),
                            url: sanitize_url(&cur_url),
                            cause: go_transport_error(&e, &cur_url),
                        }),
                    )
                }
            };
            let status = resp.status().as_u16();
            if !matches!(status, 301 | 302 | 303 | 307 | 308) {
                break resp;
            }
            let location = resp
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            if location.is_empty() {
                break resp;
            }
            let next = resolve_location(&cur_url, &location);
            if hops >= 10 {
                return (
                    None,
                    Vec::new(),
                    Some(Error::Url {
                        op: op.clone(),
                        url: sanitize_url(&next),
                        cause: "stopped after 10 redirects".to_string(),
                    }),
                );
            }
            hops += 1;
            if (301..=303).contains(&status) && cur_method != "GET" && cur_method != "HEAD" {
                cur_method = "GET".to_string();
                cur_body = None;
            }
            cur_url = next;
        };
        let url = cur_url;
        let method = cur_method.as_str();
        let status = http_resp.status().as_u16();
        let headers: Vec<(String, String)> = http_resp
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    v.to_str().unwrap_or("").to_string(),
                )
            })
            .collect();
        let mut data = Vec::new();
        if let Err(e) = http_resp.body_mut().as_reader().read_to_end(&mut data) {
            return (
                None,
                Vec::new(),
                Some(Error::Url {
                    op,
                    url: sanitize_url(&url),
                    cause: go_io_error_string(&e),
                }),
            );
        }
        let header = |name: &str| -> Option<String> { header_value(&headers, name) };
        let rate = parse_rate(&headers);
        let mut response = Response {
            status,
            rate,
            ..Default::default()
        };
        if let Some(link) = header("link") {
            populate_page_values(&mut response, &link);
        }
        self.store_rate(category, rate);
        // CheckResponse
        if status == 202 {
            return (Some(response), data, Some(Error::Accepted));
        }
        if (200..=299).contains(&status) {
            return (Some(response), data, None);
        }
        #[derive(Deserialize, Default)]
        #[serde(default)]
        struct ErrBody {
            message: String,
            errors: Vec<ApiError>,
            documentation_url: String,
        }
        let eb: ErrBody = serde_json::from_slice(&data).unwrap_or_default();
        let surl = sanitize_url(&url);
        let err = if status == 403 && header("x-ratelimit-remaining").as_deref() == Some("0") {
            Error::RateLimit {
                method: method.to_string(),
                url: surl,
                status,
                message: eb.message,
                rate,
            }
        } else if status == 403 && eb.documentation_url.ends_with("#abuse-rate-limits") {
            let retry_after = header("retry-after")
                .map(|v| Duration::from_secs(v.trim().parse::<u64>().unwrap_or(0)));
            Error::Abuse {
                method: method.to_string(),
                url: surl,
                status,
                message: eb.message,
                retry_after,
            }
        } else {
            Error::Api {
                method: method.to_string(),
                url: surl,
                status,
                message: eb.message,
                errors: eb.errors,
                documentation_url: eb.documentation_url,
            }
        };
        (Some(response), data, Some(err))
    }

    /// go-github `RateLimits`: `GET rate_limit` (never rate-limit checked),
    /// the result refreshes the client's cache.
    pub fn rate_limits(&self) -> Result<(Option<RateLimits>, Response), Error> {
        #[derive(Deserialize, Default)]
        #[serde(default)]
        struct Body {
            resources: Option<RateLimits>,
        }
        let (body, resp): (Body, Response) =
            self.do_json("GET", "rate_limit", &[], None, None, true)?;
        if let Some(rl) = &body.resources {
            if let Some(core) = rl.core {
                self.store_rate(CATEGORY_CORE, core);
            }
            if let Some(search) = rl.search {
                self.store_rate(CATEGORY_SEARCH, search);
            }
        }
        Ok((body.resources, resp))
    }

    /// go-github `Issues.Get`.
    pub fn issues_get(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
    ) -> Result<(Issue, Response), Error> {
        self.get_json(
            &format!("repos/{owner}/{repo}/issues/{number}"),
            &[],
            Some(MEDIA_TYPE_REACTIONS_PREVIEW),
        )
    }

    /// go-github `PullRequests.Get`.
    pub fn pull_requests_get(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
    ) -> Result<(PullRequest, Response), Error> {
        self.get_json(&format!("repos/{owner}/{repo}/pulls/{number}"), &[], None)
    }

    /// go-github `Issues.ListRepositoryEvents`.
    pub fn issues_list_repository_events(
        &self,
        owner: &str,
        repo: &str,
        opts: ListOptions,
    ) -> Result<(Vec<IssueEvent>, Response), Error> {
        self.get_json(
            &format!("repos/{owner}/{repo}/issues/events"),
            &opts.query(),
            None,
        )
    }

    /// go-github `Repositories.ListLanguages`.
    pub fn repositories_list_languages(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<(BTreeMap<String, i64>, Response), Error> {
        self.repositories_list_languages_full(owner, repo)
            .into_result()
    }

    /// go-github `Repositories.ListLanguages` with the full outcome.
    pub fn repositories_list_languages_full(
        &self,
        owner: &str,
        repo: &str,
    ) -> ApiResult<BTreeMap<String, i64>> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/languages"),
            &[],
            None,
            None,
            false,
        )
    }

    /// go-github `Repositories.License`.
    pub fn repositories_license(&self, owner: &str, repo: &str) -> ApiResult<RepositoryLicense> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/license"),
            &[],
            None,
            None,
            false,
        )
    }

    /// go-github `Repositories.ListCommits`.
    pub fn repositories_list_commits(
        &self,
        owner: &str,
        repo: &str,
        opts: &CommitsListOptions,
    ) -> ApiResult<Vec<RepositoryCommit>> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/commits"),
            &opts.query(),
            None,
            None,
            false,
        )
    }

    /// go-github `Issues.ListComments` (`number` 0 lists the whole repository).
    pub fn issues_list_comments(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
        opts: &IssueListCommentsOptions,
    ) -> ApiResult<Vec<IssueComment>> {
        let path = if number == 0 {
            format!("repos/{owner}/{repo}/issues/comments")
        } else {
            format!("repos/{owner}/{repo}/issues/{number}/comments")
        };
        self.do_json_full(
            "GET",
            &path,
            &opts.query(),
            Some(MEDIA_TYPE_REACTIONS_PREVIEW),
            None,
            false,
        )
    }

    /// go-github `PullRequests.List`.
    pub fn pull_requests_list(
        &self,
        owner: &str,
        repo: &str,
        opts: &PullRequestListOptions,
    ) -> ApiResult<Vec<PullRequest>> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/pulls"),
            &opts.query(),
            None,
            None,
            false,
        )
    }

    /// go-github `PullRequests.ListComments` (`number` 0 lists the whole
    /// repository).
    pub fn pull_requests_list_comments(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
        opts: &PullRequestListCommentsOptions,
    ) -> ApiResult<Vec<PullRequestComment>> {
        let path = if number == 0 {
            format!("repos/{owner}/{repo}/pulls/comments")
        } else {
            format!("repos/{owner}/{repo}/pulls/{number}/comments")
        };
        let accept = format!(
            "{}, {}",
            MEDIA_TYPE_REACTIONS_PREVIEW, MEDIA_TYPE_MULTI_LINE_COMMENTS_PREVIEW
        );
        self.do_json_full("GET", &path, &opts.query(), Some(&accept), None, false)
    }

    /// go-github `PullRequests.ListReviews`.
    pub fn pull_requests_list_reviews(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
        opts: ListOptions,
    ) -> ApiResult<Vec<PullRequestReview>> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/pulls/{number}/reviews"),
            &opts.query(),
            None,
            None,
            false,
        )
    }

    /// go-github `Repositories.ListComments` (commit comments).
    pub fn repositories_list_comments(
        &self,
        owner: &str,
        repo: &str,
        opts: ListOptions,
    ) -> ApiResult<Vec<RepositoryComment>> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/comments"),
            &opts.query(),
            Some(MEDIA_TYPE_REACTIONS_PREVIEW),
            None,
            false,
        )
    }

    /// go-github `Repositories.ListForks`.
    pub fn repositories_list_forks(
        &self,
        owner: &str,
        repo: &str,
        opts: &RepositoryListForksOptions,
    ) -> ApiResult<Vec<Repository>> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/forks"),
            &opts.query(),
            Some(MEDIA_TYPE_TOPICS_PREVIEW),
            None,
            false,
        )
    }

    /// go-github `Repositories.ListReleases`.
    pub fn repositories_list_releases(
        &self,
        owner: &str,
        repo: &str,
        opts: ListOptions,
    ) -> ApiResult<Vec<RepositoryRelease>> {
        self.do_json_full(
            "GET",
            &format!("repos/{owner}/{repo}/releases"),
            &opts.query(),
            None,
            None,
            false,
        )
    }
}

/// Go `strings.Title` of a method: the `url.Error` operation (`Get`, `Post`).
fn go_title(method: &str) -> String {
    let mut c = method.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + &c.as_str().to_lowercase(),
        None => String::new(),
    }
}

/// The first header of that (lower-case) name.
fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
}

/// go-github `parseRate`.
fn parse_rate(headers: &[(String, String)]) -> Rate {
    let mut rate = Rate::default();
    if let Some(v) = header_value(headers, "x-ratelimit-limit") {
        rate.limit = v.trim().parse().unwrap_or(0);
    }
    if let Some(v) = header_value(headers, "x-ratelimit-remaining") {
        rate.remaining = v.trim().parse().unwrap_or(0);
    }
    if let Some(v) = header_value(headers, "x-ratelimit-reset") {
        if let Ok(secs) = v.trim().parse::<i64>() {
            if secs != 0 {
                rate.reset = Utc.timestamp_opt(secs, 0).single().map(Timestamp);
            }
        }
    }
    rate
}

/// go-github `populatePageValues`: the `Link` header's `rel` pages.
fn populate_page_values(r: &mut Response, links: &str) {
    for link in links.split(',') {
        let segments: Vec<&str> = link.trim().split(';').collect();
        if segments.len() < 2 {
            continue;
        }
        let href = segments[0];
        if !href.starts_with('<') || !href.ends_with('>') {
            continue;
        }
        let u = &href[1..href.len() - 1];
        let query = match u.split_once('?') {
            Some((_, q)) => q,
            None => "",
        };
        let get = |key: &str| -> String {
            query
                .split('&')
                .filter_map(|p| p.split_once('='))
                .find(|(k, _)| *k == key)
                .map(|(_, v)| crate::gourl::query_unescape(v).unwrap_or_else(|_| v.to_string()))
                .unwrap_or_default()
        };
        let cursor = get("cursor");
        if !cursor.is_empty() {
            for seg in &segments[1..] {
                if seg.trim() == "rel=\"next\"" {
                    r.cursor = cursor.clone();
                }
            }
            continue;
        }
        let page = get("page");
        if page.is_empty() {
            continue;
        }
        for seg in &segments[1..] {
            match seg.trim() {
                "rel=\"next\"" => match page.parse::<i64>() {
                    Ok(p) => r.next_page = p,
                    Err(_) => {
                        r.next_page = 0;
                        r.next_page_token = page.clone();
                    }
                },
                "rel=\"prev\"" => r.prev_page = page.parse().unwrap_or(0),
                "rel=\"first\"" => r.first_page = page.parse().unwrap_or(0),
                "rel=\"last\"" => r.last_page = page.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
}

/// `host[:port]` of an absolute URL (`""` when there is none).
fn url_host(url: &str) -> String {
    url.split("://")
        .nth(1)
        .unwrap_or("")
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .to_string()
}

/// Go `isDomainOrSubdomain` (ports stripped): may `Authorization` follow a
/// redirect from `parent` to `sub`?
fn is_domain_or_subdomain(sub: &str, parent: &str) -> bool {
    let strip = |h: &str| -> String {
        let h = h.to_ascii_lowercase();
        match h.rsplit_once(':') {
            Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
                host.to_string()
            }
            _ => h,
        }
    };
    let (sub, parent) = (strip(sub), strip(parent));
    if sub == parent {
        return true;
    }
    if sub.len() <= parent.len() || !sub.ends_with(&parent) {
        return false;
    }
    sub.as_bytes()[sub.len() - parent.len() - 1] == b'.'
}

/// Go `req.URL.Parse(location)`: resolve a `Location` header against the
/// request URL (absolute, scheme-relative, root-relative or relative).
fn resolve_location(base: &str, location: &str) -> String {
    if location.contains("://") {
        return location.to_string();
    }
    let (scheme, rest) = match base.split_once("://") {
        Some((s, r)) => (s, r),
        None => return location.to_string(),
    };
    let (authority, path_and_more) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if let Some(l) = location.strip_prefix("//") {
        return format!("{scheme}://{l}");
    }
    if location.starts_with('/') {
        return format!("{scheme}://{authority}{location}");
    }
    let path = path_and_more.split(['?', '#']).next().unwrap_or("/");
    let (rel_path, tail) = match location.find(['?', '#']) {
        Some(i) => (&location[..i], &location[i..]),
        None => (location, ""),
    };
    let dir = match path.rfind('/') {
        Some(i) => &path[..=i],
        None => "/",
    };
    let joined = crate::http::path_clean(&format!("{dir}{rel_path}"));
    let joined = if rel_path.ends_with('/') && !joined.ends_with('/') {
        format!("{joined}/")
    } else {
        joined
    };
    format!("{scheme}://{authority}{joined}{tail}")
}

/// Go's `net/http` transport error for a failed request, worded like
/// `dial tcp 127.0.0.1:1: connect: connection refused`.
/// A plain HTTP response (`ghapi2db`'s GraphQL calls made with a bare
/// `http.Client`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawResponse {
    pub status: u16,
    /// Header names lower-cased.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RawResponse {
    /// Go `resp.Header.Get(name)` (case-insensitive, first value).
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Go `(&http.Client{Timeout: timeout}).Do(POST url)` + `io.ReadAll`: the
/// status, headers and body, or Go's `Post "url": …` transport error text.
pub fn raw_post(
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    timeout: Duration,
) -> Result<RawResponse, String> {
    // Go's net/http adds no `Accept` header and identifies as
    // `Go-http-client/1.1`; mirror both.
    let config = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(timeout))
        .accept(ureq::config::AutoHeaderValue::None)
        .user_agent("Go-http-client/1.1")
        .build();
    let agent: ureq::Agent = config.into();
    let mut req = ureq::http::Request::builder().method("POST").uri(url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let request = req
        .body(body.to_vec())
        .map_err(|e| format!("Post {:?}: {}", url, e))?;
    let mut resp = agent
        .run(request)
        .map_err(|e| format!("Post {:?}: {}", url, go_transport_error(&e, url)))?;
    let status = resp.status().as_u16();
    let hdrs: Vec<(String, String)> = resp
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_ascii_lowercase(),
                v.to_str().unwrap_or("").to_string(),
            )
        })
        .collect();
    let mut data = Vec::new();
    resp.body_mut()
        .as_reader()
        .read_to_end(&mut data)
        .map_err(|e| go_io_error_string(&e))?;
    Ok(RawResponse {
        status,
        headers: hdrs,
        body: data,
    })
}

fn go_transport_error(e: &ureq::Error, url: &str) -> String {
    let host_port = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("")
        .to_string();
    match e {
        ureq::Error::Io(io) => {
            let s = go_io_error_string(io);
            match io.kind() {
                std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::TimedOut => format!("dial tcp {host_port}: connect: {s}"),
                _ => s,
            }
        }
        ureq::Error::ConnectionFailed => {
            format!("dial tcp {host_port}: connect: connection refused")
        }
        ureq::Error::Timeout(_) => format!("dial tcp {host_port}: i/o timeout"),
        ureq::Error::HostNotFound => format!(
            "dial tcp: lookup {}: no such host",
            host_port.split(':').next().unwrap_or("")
        ),
        other => other.to_string(),
    }
}

/// Go `encoding/json` decode errors (the type names `HandlePossibleError`
/// prints with `%T`).
fn json_error(e: &serde_json::Error) -> Error {
    let go_type = if e.is_syntax() || e.is_eof() {
        "*json.SyntaxError"
    } else {
        "*json.UnmarshalTypeError"
    };
    Error::Json {
        go_type,
        message: e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;

    fn serve_once(status: &str, headers: &str, body: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (status, headers, body) = (status.to_string(), headers.to_string(), body.to_string());
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 8192];
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                format!(
                    "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        });
        format!("http://{addr}/")
    }

    fn client_for(base: &str) -> Client {
        Client::with_base_url(None, base)
    }

    #[test]
    fn issue_decoding_and_pointer_semantics() {
        let j = r#"{"id": 5, "number": 7, "state": "open", "title": "t", "locked": false,
            "closed_at": null, "created_at": "2020-01-02T03:04:05Z", "labels": [{"id": 1, "name": "bug"}],
            "assignees": [], "pull_request": {"url": "x"}, "milestone": {"id": 9, "number": 1, "due_on": "2021-01-01T00:00:00+02:00"}}"#;
        let i: Issue = serde_json::from_str(j).unwrap();
        assert_eq!(i.id, Some(5));
        assert!(i.is_pull_request());
        assert_eq!(i.closed_at, None);
        assert_eq!(
            i.created_at.unwrap().utc().to_rfc3339(),
            "2020-01-02T03:04:05+00:00"
        );
        assert_eq!(i.labels[0].name.as_deref(), Some("bug"));
        assert_eq!(
            i.milestone.as_ref().unwrap().due_on.unwrap().0.to_rfc3339(),
            "2021-01-01T00:00:00+02:00"
        );
        assert_eq!(i.get_repository().get_owner().get_name(), "");
        // A float into an integer field is an error (Go: UnmarshalTypeError).
        assert!(serde_json::from_str::<Issue>(r#"{"id": 1.5}"#).is_err());
    }

    #[test]
    fn timestamp_forms() {
        #[derive(Deserialize)]
        struct T {
            t: Timestamp,
        }
        let a: T = serde_json::from_str(r#"{"t": 1600000000}"#).unwrap();
        assert_eq!(a.t.0.timestamp(), 1_600_000_000);
        let b: T = serde_json::from_str(r#"{"t": 1600000000000}"#).unwrap();
        assert_eq!(b.t.0.timestamp(), 1_600_000_000);
        let c: T = serde_json::from_str(r#"{"t": "2020-09-13T12:26:40Z"}"#).unwrap();
        assert_eq!(c.t.0.timestamp(), 1_600_000_000);
    }

    #[test]
    fn error_wording() {
        let base = serve_once(
            "404 Not Found",
            "Content-Type: application/json\r\n",
            r#"{"message": "Not Found", "documentation_url": "https://docs.github.com/rest"}"#,
        );
        let c = client_for(&base);
        let err = c.issues_get("o", "r", 1).unwrap_err();
        assert_eq!(err.go_type(), "*github.ErrorResponse");
        assert_eq!(
            err.to_string(),
            format!("GET {base}repos/o/r/issues/1: 404 Not Found []")
        );

        let base = serve_once(
            "422 Unprocessable Entity",
            "Content-Type: application/json\r\n",
            r#"{"message": "Validation Failed", "errors": [{"resource": "Issue", "field": "title", "code": "missing_field"}]}"#,
        );
        let err = client_for(&base).issues_get("o", "r", 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("GET {base}repos/o/r/issues/1: 422 Validation Failed [{{Resource:Issue Field:title Code:missing_field Message:}}]")
        );

        let base = serve_once(
            "403 Forbidden",
            "X-RateLimit-Limit: 60\r\nX-RateLimit-Remaining: 0\r\nX-RateLimit-Reset: 4102444800\r\n",
            r#"{"message": "API rate limit exceeded"}"#,
        );
        let c = client_for(&base);
        let err = c.issues_get("o", "r", 1).unwrap_err();
        assert!(err.is_rate_limit());
        assert!(err.to_string().starts_with(&format!(
            "GET {base}repos/o/r/issues/1: 403 API rate limit exceeded [rate reset in "
        )));
        // The client now refuses to send until the reset.
        let err2 = c.pull_requests_get("o", "r", 2).unwrap_err();
        assert!(err2.is_rate_limit());
        assert!(
            err2.to_string()
                .contains("403 API rate limit of 60 still exceeded until 2100-01-01 "),
            "{err2}"
        );

        let base = serve_once(
            "403 Forbidden",
            "Retry-After: 60\r\n",
            r#"{"message": "You have triggered an abuse detection mechanism.", "documentation_url": "https://docs.github.com/en/rest/overview/resources-in-the-rest-api#abuse-rate-limits"}"#,
        );
        let err = client_for(&base).issues_get("o", "r", 1).unwrap_err();
        assert!(err.is_abuse());
        assert_eq!(err.to_string(), format!("GET {base}repos/o/r/issues/1: 403 You have triggered an abuse detection mechanism."));

        let base = serve_once("202 Accepted", "", "{}");
        let err = client_for(&base).issues_get("o", "r", 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            "job scheduled on GitHub side; try again later"
        );

        let err = client_for("http://127.0.0.1:1/")
            .issues_get("o", "r", 1)
            .unwrap_err();
        assert_eq!(err.go_type(), "*url.Error");
        assert_eq!(
            err.to_string(),
            "Get \"http://127.0.0.1:1/repos/o/r/issues/1\": dial tcp 127.0.0.1:1: connect: connection refused"
        );
    }

    #[test]
    fn rate_limits_and_pages() {
        let base = serve_once(
            "200 OK",
            "Link: <https://api.github.com/x?page=3>; rel=\"next\", <https://api.github.com/x?page=7>; rel=\"last\"\r\nX-RateLimit-Limit: 5000\r\nX-RateLimit-Remaining: 4999\r\nX-RateLimit-Reset: 1600000000\r\n",
            r#"{"resources": {"core": {"limit": 5000, "remaining": 4999, "reset": 1600000000}, "search": {"limit": 30, "remaining": 30, "reset": 1600000001}}}"#,
        );
        let c = client_for(&base);
        let (rl, resp) = c.rate_limits().unwrap();
        let rl = rl.unwrap();
        assert_eq!(rl.core.unwrap().remaining, 4999);
        assert_eq!(rl.search.unwrap().limit, 30);
        assert_eq!(resp.next_page, 3);
        assert_eq!(resp.last_page, 7);
        assert_eq!(resp.rate.remaining, 4999);
        assert_eq!(c.cached_rate(CATEGORY_SEARCH).remaining, 30);
    }

    #[test]
    fn rate_reset_wording() {
        assert_eq!(
            format_rate_reset(chrono::Duration::seconds(59)),
            "[rate reset in 59s]"
        );
        assert_eq!(
            format_rate_reset(chrono::Duration::milliseconds(3_599_600)),
            "[rate reset in 60m00s]"
        );
        assert_eq!(
            format_rate_reset(chrono::Duration::seconds(-61)),
            "[rate limit was reset 1m01s ago]"
        );
    }

    #[test]
    fn sanitize() {
        assert_eq!(
            sanitize_url("http://x/y?b=1&client_secret=abc&a=2"),
            "http://x/y?a=2&b=1&client_secret=REDACTED"
        );
        assert_eq!(sanitize_url("http://x/y?a=2"), "http://x/y?a=2");
    }

    #[test]
    fn redirect_helpers() {
        assert!(is_domain_or_subdomain("api.github.com", "api.github.com"));
        assert!(is_domain_or_subdomain(
            "API.github.com:443",
            "api.github.com"
        ));
        assert!(is_domain_or_subdomain("a.api.github.com", "api.github.com"));
        assert!(!is_domain_or_subdomain("github.com", "api.github.com"));
        assert!(!is_domain_or_subdomain("xapi.github.com", "api.github.com"));
        assert!(is_domain_or_subdomain("127.0.0.1:8080", "127.0.0.1:9090"));
        assert_eq!(
            url_host("http://127.0.0.1:12/repos/o/r?x=1"),
            "127.0.0.1:12"
        );
        assert_eq!(url_host("nourl"), "");
        let base = "http://h:1/repos/o/r/issues/5?a=1";
        assert_eq!(resolve_location(base, "https://x.y/z"), "https://x.y/z");
        assert_eq!(resolve_location(base, "//x.y/z"), "http://x.y/z");
        assert_eq!(
            resolve_location(base, "/repos/o/r2/issues/5"),
            "http://h:1/repos/o/r2/issues/5"
        );
        assert_eq!(
            resolve_location(base, "6?b=2"),
            "http://h:1/repos/o/r/issues/6?b=2"
        );
        assert_eq!(
            resolve_location(base, "../pulls/7"),
            "http://h:1/repos/o/r/pulls/7"
        );
        assert_eq!(resolve_location("http://h", "x/"), "http://h/x/");
    }
}
