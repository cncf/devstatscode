//! Go ⇄ Rust compatibility tests for `ghapi2db`.
//!
//! Every case runs each binary against its own scratch database
//! (`dbtest_ghapi2db_<case>_<go|rs>`, the full DevStats schema plus the
//! case's seed rows) and its own scripted fake GitHub API
//! (`devstats_compat::github::FakeGitHub`, reached through
//! `GHA2DB_GITHUB_API_URL`; GraphQL included), usually with all but one of
//! the tool's passes skipped. Compared per run: the exit code, stdout (as
//! lines, with the API URL, the binary path, durations, now-derived event
//! ids / time stamps / `recent` dates masked; in order for single-threaded
//! cases, as a sorted multiset otherwise), the `Error: '…'` stderr lines;
//! afterwards every table of the database(s) (rows with the now-derived ids
//! and time stamps masked) and the sorted log of the API requests each
//! binary made (method, path, query, token, `Accept`, GraphQL bodies).
//!
//! The fixtures use 2020 dates; `GHA2DB_RECENT_RANGE` /
//! `GHA2DB_RECENT_REPOS_RANGE` are set to `10 years` so they count as
//! recent (dates in 2010 are "older than the recent range").
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use devstats_compat::github::{FakeGitHub, Logged, Scripted};
use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{fixture, go_binary, is_go_duration, run, rust_binary, Invocation, Outcome};
use regex::Regex;
use serde_json::{json, Value};
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("ghapi2db")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_ghapi2db"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

const REPO: &str = "org/repo";
const REPO_ID: i64 = 500;
const ORG_ID: i64 = 1;
const V3_ACCEPT: &str = "application/vnd.github.v3+json";
const SQUIRREL_ACCEPT: &str = "application/vnd.github.squirrel-girl-preview";
const REVIEW_COMMENTS_ACCEPT: &str =
    "application/vnd.github.squirrel-girl-preview, application/vnd.github.comfort-fade-preview+json";
const MERCY_ACCEPT: &str = "application/vnd.github.mercy-preview+json";
/// SHA-1 of `alice`, the login anonymised by the `hide.csv` cases.
const ALICE_SHA1: &str = "522b276a356bdf39013dfabea2cd43e141ecc9e8";
const OLD: &str = "2010-01-01T00:00:00Z";

/// `Time: 1.234s`, `waiting 5s (0)`, `wait 8s`, `reset in 5s` — Go
/// `time.Duration`s after a marker.
static DURATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(Time: |waiting |wait |message: |don't want to wait |reset in |difference for sha [0-9a-f]+: )(-?[0-9][0-9.hmsµn]*)",
    )
    .unwrap()
});
/// Durations inside `%+v` slices: `waitPeriod: [59m59.999s]`, `resets in: [...]`.
static DURATION_LIST: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(waitPeriod: |reset: |resets in:? )\[([^\]]*)\]").unwrap());
/// go-github's `[rate reset in 59m59s]` / `[rate limit was reset 1s ago]`.
static RATE_RESET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[rate (?:reset in|limit was reset) [^\]]*\]").unwrap());
/// `EventID: 56612345678901` inside an `IssueConfig` string (now-derived).
static EVENT_ID: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"EventID: \d+").unwrap());
/// A `time.Now()`-derived time stamp (the fixtures use 2020 dates).
static NOW: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"20(?:2[5-9]|[3-9]\d)-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?: [+-]\d{4} \S+(?: m=[+-][\d.]+)?)?")
        .unwrap()
});
/// The `recent` dates (`HourStart(now) - 10 years`, so 2016) after their
/// markers: `recent date: …`, `Repos to process from …:`, `] < …:`.
static RECENT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(recent date: |Repos to process from |\] < )\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)? [+-]\d{4} \S+")
        .unwrap()
});
/// `since=2016-…Z` query parameters derived from the recent date.
static RECENT_SINCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"since=20[01]\d-\d\d-\d\dT\d\d%3A\d\d%3A\d\dZ").unwrap());
/// `lib.ProgressInfo` lines (only after 10 seconds — dropped).
static PROGRESS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\d+/\d+ \(\d+\.\d{3}%\), ETA: ").unwrap());
/// A timestamp cell of a dump (`2026-09-12 01:02:03.123456`).
static TIMESTAMP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\d{4})-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:\d{2})?)?$").unwrap()
});
/// A value of the `now()`-derived event id family (`2^48 + UnixNano/31622`).
static BIG_ID: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d{13,}$").unwrap());

// ---------------------------------------------------------------------------
// GitHub JSON builders
// ---------------------------------------------------------------------------

fn user(id: i64, login: &str) -> Value {
    json!({
        "login": login,
        "id": id,
        "node_id": format!("MDQ6VXNlcj{id}"),
        "avatar_url": format!("https://avatars.githubusercontent.com/u/{id}?v=4"),
        "url": format!("https://api.github.com/users/{login}"),
        "html_url": format!("https://github.com/{login}"),
        "type": "User",
        "site_admin": false
    })
}

fn label(id: i64, name: &str) -> Value {
    json!({
        "id": id,
        "node_id": format!("MDU6TGFiZWw{id}"),
        "url": format!("https://api.github.com/repos/{REPO}/labels/{name}"),
        "name": name,
        "color": "d73a4a",
        "default": false,
        "description": format!("label {name}")
    })
}

fn milestone(id: i64, number: i64, title: &str) -> Value {
    json!({
        "url": format!("https://api.github.com/repos/{REPO}/milestones/{number}"),
        "html_url": format!("https://github.com/{REPO}/milestone/{number}"),
        "id": id,
        "node_id": format!("MDk6TWlsZXN0b25l{id}"),
        "number": number,
        "title": title,
        "description": format!("milestone {title}"),
        "creator": user(11, "alice"),
        "open_issues": 2,
        "closed_issues": 1,
        "state": "open",
        "created_at": "2020-01-10T10:00:00Z",
        "updated_at": "2020-01-11T10:00:00Z",
        "due_on": "2020-06-30T07:00:00Z",
        "closed_at": null
    })
}

/// An issue (`pull_request` present when `pr`) of `repo`.
#[derive(Clone)]
struct IssueSpec {
    id: i64,
    number: i64,
    title: String,
    body: Option<String>,
    state: &'static str,
    user: (i64, &'static str),
    assignee: Option<(i64, &'static str)>,
    assignees: Vec<(i64, &'static str)>,
    labels: Vec<(i64, &'static str)>,
    milestone: Option<Value>,
    pr: bool,
    repo: String,
}

impl IssueSpec {
    fn new(id: i64, number: i64) -> IssueSpec {
        IssueSpec {
            id,
            number,
            title: format!("Issue {number}"),
            body: Some(format!("Body of issue {number}")),
            state: "open",
            user: (11, "alice"),
            assignee: None,
            assignees: Vec::new(),
            labels: Vec::new(),
            milestone: None,
            pr: false,
            repo: REPO.to_string(),
        }
    }
    fn pr(mut self) -> Self {
        self.pr = true;
        self.title = format!("PR {}", self.number);
        self
    }
    fn title(mut self, t: &str) -> Self {
        self.title = t.to_string();
        self
    }
    fn state(mut self, s: &'static str) -> Self {
        self.state = s;
        self
    }
    fn labels(mut self, l: Vec<(i64, &'static str)>) -> Self {
        self.labels = l;
        self
    }
    fn assignee(mut self, a: (i64, &'static str)) -> Self {
        self.assignee = Some(a);
        self.assignees = vec![a];
        self
    }
    fn milestone(mut self, m: Value) -> Self {
        self.milestone = Some(m);
        self
    }
    fn repo(mut self, r: &str) -> Self {
        self.repo = r.to_string();
        self
    }
    fn json(&self) -> Value {
        let mut v = json!({
            "url": format!("https://api.github.com/repos/{}/issues/{}", self.repo, self.number),
            "repository_url": format!("https://api.github.com/repos/{}", self.repo),
            "html_url": format!("https://github.com/{}/issues/{}", self.repo, self.number),
            "id": self.id,
            "node_id": format!("MDU6SXNzdWU{}", self.id),
            "number": self.number,
            "title": self.title,
            "user": user(self.user.0, self.user.1),
            "labels": self.labels.iter().map(|(id, n)| label(*id, n)).collect::<Vec<_>>(),
            "state": self.state,
            "locked": false,
            "assignee": self.assignee.map(|(id, l)| user(id, l)),
            "assignees": self.assignees.iter().map(|(id, l)| user(*id, l)).collect::<Vec<_>>(),
            "milestone": self.milestone,
            "comments": 0,
            "created_at": "2020-03-01T12:00:00Z",
            "updated_at": "2020-03-02T13:30:00Z",
            "closed_at": if self.state == "closed" { json!("2020-03-03T14:00:00Z") } else { Value::Null },
            "author_association": "CONTRIBUTOR",
            "body": self.body,
        });
        if self.pr {
            v["pull_request"] = json!({
                "url": format!("https://api.github.com/repos/{}/pulls/{}", self.repo, self.number),
                "html_url": format!("https://github.com/{}/pull/{}", self.repo, self.number),
                "diff_url": format!("https://github.com/{}/pull/{}.diff", self.repo, self.number),
                "patch_url": format!("https://github.com/{}/pull/{}.patch", self.repo, self.number),
                "merged_at": null
            });
        }
        v
    }
}

/// A `GET /repos/{owner}/{repo}/pulls/{number}` body for `issue` (a PR).
fn pr_json(issue: &IssueSpec) -> Value {
    let branch = |label: &str, r: &str, sha: &str| {
        json!({
            "label": format!("org:{label}"),
            "ref": r,
            "sha": sha,
            "user": user(1, "org"),
            "repo": {"id": REPO_ID, "name": "repo", "full_name": issue.repo, "owner": user(1, "org")}
        })
    };
    json!({
        "url": format!("https://api.github.com/repos/{}/pulls/{}", issue.repo, issue.number),
        "id": issue.id + 100,
        "node_id": format!("MDExOlB1bGxSZXF1ZXN0{}", issue.id),
        "html_url": format!("https://github.com/{}/pull/{}", issue.repo, issue.number),
        "number": issue.number,
        "state": issue.state,
        "locked": false,
        "title": issue.title,
        "user": user(issue.user.0, issue.user.1),
        "body": issue.body,
        "created_at": "2020-04-01T08:00:00Z",
        "updated_at": "2020-04-02T09:00:00Z",
        "closed_at": if issue.state == "closed" { json!("2020-04-03T10:00:00Z") } else { Value::Null },
        "merged_at": null,
        "merge_commit_sha": "1111111111111111111111111111111111111111",
        "assignee": issue.assignee.map(|(id, l)| user(id, l)),
        "assignees": issue.assignees.iter().map(|(id, l)| user(*id, l)).collect::<Vec<_>>(),
        "requested_reviewers": [],
        "requested_teams": [],
        "labels": issue.labels.iter().map(|(id, n)| label(*id, n)).collect::<Vec<_>>(),
        "milestone": issue.milestone,
        "draft": false,
        "head": branch("feature", "feature", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        "base": branch("main", "main", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        "author_association": "MEMBER",
        "merged": false,
        "mergeable": true,
        "rebaseable": true,
        "mergeable_state": "clean",
        "merged_by": null,
        "comments": 1,
        "review_comments": 2,
        "maintainer_can_modify": true,
        "commits": 3,
        "additions": 40,
        "deletions": 5,
        "changed_files": 2
    })
}

/// A repository issue event (`GET /repos/{owner}/{repo}/issues/events`).
fn issue_event(
    id: i64,
    event: Option<&str>,
    actor: (i64, &str),
    created_at: &str,
    issue: Option<&IssueSpec>,
) -> Value {
    let mut v = json!({
        "id": id,
        "node_id": format!("MDEwOklzc3VlRXZlbnQ{id}"),
        "url": format!("https://api.github.com/repos/{REPO}/issues/events/{id}"),
        "actor": user(actor.0, actor.1),
        "event": event,
        "commit_id": null,
        "commit_url": null,
        "created_at": created_at,
        "issue": issue.map(|i| i.json())
    });
    if event == Some("renamed") {
        v["rename"] = json!({"from": "Old title", "to": "Renamed title"});
    }
    if event == Some("labeled") {
        v["label"] = json!({"name": "bug", "color": "d73a4a"});
    }
    v
}

/// A repository commit (`GET /repos/{owner}/{repo}/commits`).
#[derive(Clone)]
struct CommitSpec {
    sha: String,
    author: Option<(i64, &'static str)>,
    committer: Option<(i64, &'static str)>,
    author_name: &'static str,
    author_email: &'static str,
    committer_name: &'static str,
    committer_email: &'static str,
    date: &'static str,
    message: &'static str,
}

impl CommitSpec {
    fn new(sha: &str) -> CommitSpec {
        CommitSpec {
            sha: sha.to_string(),
            author: Some((11, "alice")),
            committer: Some((11, "alice")),
            author_name: "Alice A",
            author_email: "alice@example.com",
            committer_name: "Alice A",
            committer_email: "alice@example.com",
            date: "2020-03-01T10:00:00Z",
            message: "Fix the thing",
        }
    }
    fn author(
        mut self,
        a: Option<(i64, &'static str)>,
        name: &'static str,
        email: &'static str,
    ) -> Self {
        self.author = a;
        self.author_name = name;
        self.author_email = email;
        self
    }
    fn committer(
        mut self,
        c: Option<(i64, &'static str)>,
        name: &'static str,
        email: &'static str,
    ) -> Self {
        self.committer = c;
        self.committer_name = name;
        self.committer_email = email;
        self
    }
    fn date(mut self, d: &'static str) -> Self {
        self.date = d;
        self
    }
    fn json(&self) -> Value {
        json!({
            "sha": self.sha,
            "node_id": format!("MDY6Q29tbWl0{}", self.sha),
            "commit": {
                "author": {"name": self.author_name, "email": self.author_email, "date": self.date},
                "committer": {"name": self.committer_name, "email": self.committer_email, "date": self.date},
                "message": self.message,
                "tree": {"sha": "cccccccccccccccccccccccccccccccccccccccc", "url": ""},
                "url": format!("https://api.github.com/repos/{REPO}/git/commits/{}", self.sha),
                "comment_count": 0,
                "verification": {"verified": false, "reason": "unsigned", "signature": null, "payload": null}
            },
            "url": format!("https://api.github.com/repos/{REPO}/commits/{}", self.sha),
            "html_url": format!("https://github.com/{REPO}/commit/{}", self.sha),
            "comments_url": format!("https://api.github.com/repos/{REPO}/commits/{}/comments", self.sha),
            "author": self.author.map(|(id, l)| user(id, l)),
            "committer": self.committer.map(|(id, l)| user(id, l)),
            "parents": [{"sha": "dddddddddddddddddddddddddddddddddddddddd", "url": "", "html_url": ""}]
        })
    }
}

fn license_json(key: &str, name: &str) -> Value {
    json!({
        "name": "LICENSE",
        "path": "LICENSE",
        "sha": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        "size": 1071,
        "url": format!("https://api.github.com/repos/{REPO}/contents/LICENSE?ref=main"),
        "html_url": format!("https://github.com/{REPO}/blob/main/LICENSE"),
        "git_url": format!("https://api.github.com/repos/{REPO}/git/blobs/eeee"),
        "download_url": format!("https://raw.githubusercontent.com/{REPO}/main/LICENSE"),
        "type": "file",
        "content": "TUlUIExpY2Vuc2U=\n",
        "encoding": "base64",
        "_links": {"self": "", "git": "", "html": ""},
        "license": {
            "key": key,
            "name": name,
            "spdx_id": key.to_uppercase(),
            "url": format!("https://api.github.com/licenses/{key}"),
            "node_id": "MDc6TGljZW5zZTEz"
        }
    })
}

fn issue_comment_json(
    id: i64,
    issue_number: i64,
    user_: (i64, &str),
    created_at: &str,
    updated_at: Option<&str>,
    body: &str,
) -> Value {
    json!({
        "url": format!("https://api.github.com/repos/{REPO}/issues/comments/{id}"),
        "html_url": format!("https://github.com/{REPO}/issues/{issue_number}#issuecomment-{id}"),
        "issue_url": format!("https://api.github.com/repos/{REPO}/issues/{issue_number}"),
        "id": id,
        "node_id": format!("MDEyOklzc3VlQ29tbWVudD{id}"),
        "user": user(user_.0, user_.1),
        "created_at": created_at,
        "updated_at": updated_at,
        "author_association": "CONTRIBUTOR",
        "body": body,
        "reactions": {"url": "", "total_count": 0}
    })
}

fn review_comment_json(
    id: i64,
    pr_number: i64,
    user_: (i64, &str),
    created_at: &str,
    body: &str,
    review_id: Option<i64>,
) -> Value {
    json!({
        "url": format!("https://api.github.com/repos/{REPO}/pulls/comments/{id}"),
        "pull_request_review_id": review_id,
        "id": id,
        "node_id": format!("MDI0OlB1bGxSZXF1ZXN0UmV2aWV3Q29tbWVudD{id}"),
        "diff_hunk": "@@ -1,3 +1,4 @@",
        "path": "src/main.go",
        "position": 4,
        "original_position": 4,
        "commit_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "original_commit_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "user": user(user_.0, user_.1),
        "body": body,
        "created_at": created_at,
        "updated_at": created_at,
        "html_url": format!("https://github.com/{REPO}/pull/{pr_number}#discussion_r{id}"),
        "pull_request_url": format!("https://api.github.com/repos/{REPO}/pulls/{pr_number}"),
        "author_association": "MEMBER",
        "line": 5,
        "original_line": 5,
        "side": "RIGHT"
    })
}

fn commit_comment_json(id: i64, user_: (i64, &str), created_at: &str, body: &str) -> Value {
    json!({
        "url": format!("https://api.github.com/repos/{REPO}/comments/{id}"),
        "html_url": format!("https://github.com/{REPO}/commit/aaaa#commitcomment-{id}"),
        "id": id,
        "node_id": format!("MDEzOkNvbW1pdENvbW1lbnQ{id}"),
        "user": user(user_.0, user_.1),
        "position": 7,
        "line": 7,
        "path": "README.md",
        "commit_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "created_at": created_at,
        "updated_at": created_at,
        "author_association": "MEMBER",
        "body": body
    })
}

fn review_json(
    id: i64,
    user_: (i64, &str),
    submitted_at: &str,
    state: &str,
    body: Option<&str>,
) -> Value {
    json!({
        "id": id,
        "node_id": format!("MDE3OlB1bGxSZXF1ZXN0UmV2aWV3{id}"),
        "user": user(user_.0, user_.1),
        "body": body,
        "state": state,
        "html_url": format!("https://github.com/{REPO}/pull/2#pullrequestreview-{id}"),
        "pull_request_url": format!("https://api.github.com/repos/{REPO}/pulls/2"),
        "author_association": "COLLABORATOR",
        "submitted_at": submitted_at,
        "commit_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    })
}

/// A pull request list entry (`GET /repos/{owner}/{repo}/pulls`).
fn pr_list_json(number: i64, updated_at: &str) -> Value {
    json!({
        "url": format!("https://api.github.com/repos/{REPO}/pulls/{number}"),
        "id": 200 + number,
        "number": number,
        "state": "open",
        "title": format!("PR {number}"),
        "user": user(12, "bob"),
        "created_at": "2020-04-01T08:00:00Z",
        "updated_at": updated_at,
        "head": {"sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "ref": "feature"},
        "base": {"sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ref": "main"}
    })
}

fn fork_json(id: i64, owner: (i64, &str), created_at: &str, stars: i64) -> Value {
    json!({
        "id": id,
        "node_id": format!("MDEwOlJlcG9zaXRvcnk{id}"),
        "name": "repo",
        "full_name": format!("{}/repo", owner.1),
        "private": false,
        "owner": user(owner.0, owner.1),
        "html_url": format!("https://github.com/{}/repo", owner.1),
        "description": "A fork",
        "fork": true,
        "created_at": created_at,
        "updated_at": "2020-05-02T10:00:00Z",
        "pushed_at": "2020-05-02T11:00:00Z",
        "homepage": null,
        "size": 100,
        "stargazers_count": stars,
        "watchers_count": stars,
        "language": "Go",
        "forks_count": 2,
        "open_issues_count": 1,
        "default_branch": "main",
        "topics": []
    })
}

fn asset_json(id: i64, name: &str, uploader: Option<(i64, &str)>) -> Value {
    json!({
        "url": format!("https://api.github.com/repos/{REPO}/releases/assets/{id}"),
        "id": id,
        "node_id": format!("MDEyOlJlbGVhc2VBc3NldD{id}"),
        "name": name,
        "label": "",
        "uploader": uploader.map(|(id, l)| user(id, l)),
        "content_type": "application/gzip",
        "state": "uploaded",
        "size": 12345,
        "download_count": 7,
        "created_at": "2020-06-01T10:05:00Z",
        "updated_at": "2020-06-01T10:06:00Z",
        "browser_download_url": format!("https://github.com/{REPO}/releases/download/v1/{name}")
    })
}

fn release_json(
    id: i64,
    tag: &str,
    created_at: &str,
    published_at: Option<&str>,
    assets: Vec<Value>,
) -> Value {
    json!({
        "url": format!("https://api.github.com/repos/{REPO}/releases/{id}"),
        "html_url": format!("https://github.com/{REPO}/releases/tag/{tag}"),
        "id": id,
        "node_id": format!("MDc6UmVsZWFzZT{id}"),
        "tag_name": tag,
        "target_commitish": "main",
        "name": format!("Release {tag}"),
        "draft": false,
        "author": user(12, "bob"),
        "prerelease": tag.contains("rc"),
        "created_at": created_at,
        "published_at": published_at,
        "assets": assets,
        "tarball_url": format!("https://api.github.com/repos/{REPO}/tarball/{tag}"),
        "zipball_url": format!("https://api.github.com/repos/{REPO}/zipball/{tag}"),
        "body": format!("Notes for {tag}")
    })
}

/// A GraphQL stargazers page: `edges` = (starredAt, login, databaseId).
fn gql_page(edges: &[(&str, &str, i64)], has_prev: bool, start_cursor: &str) -> Value {
    json!({
        "data": {
            "repository": {
                "stargazers": {
                    "pageInfo": {"hasPreviousPage": has_prev, "startCursor": start_cursor},
                    "edges": edges.iter().map(|(at, login, id)| json!({
                        "starredAt": at,
                        "node": {"login": login, "databaseId": id}
                    })).collect::<Vec<_>>()
                }
            }
        }
    })
}

/// `GET /rate_limit` body with the given core state.
fn rate_json(limit: i64, remaining: i64, reset_in: i64) -> Value {
    let reset = devstats_compat::github::now_unix() + reset_in;
    let core =
        json!({"limit": limit, "used": limit - remaining, "remaining": remaining, "reset": reset});
    json!({
        "resources": {
            "core": core,
            "search": {"limit": 30, "used": 0, "remaining": 30, "reset": reset},
            "graphql": {"limit": 5000, "used": 0, "remaining": 5000, "reset": reset}
        },
        "rate": core
    })
}

fn events_path(repo: &str) -> String {
    format!("/repos/{repo}/issues/events")
}

fn pr_path(repo: &str, number: i64) -> String {
    format!("/repos/{repo}/pulls/{number}")
}

// ---------------------------------------------------------------------------
// Database seeds
// ---------------------------------------------------------------------------

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn sql_ts(s: &str) -> String {
    sql_str(&s.replace('T', " ").replace('Z', ""))
}

/// A GHA event of `repo` (makes it a recent repository).
fn seed_event(
    id: i64,
    e_type: &str,
    repo: &str,
    repo_id: i64,
    actor: (i64, &str),
    created_at: &str,
) -> String {
    format!(
        "insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) \
         values({id}, {}, {}, {repo_id}, {}, {ORG_ID}, {}, {});",
        sql_str(e_type),
        actor.0,
        sql_ts(created_at),
        sql_str(actor.1),
        sql_str(repo)
    )
}

fn seed_repo(id: i64, name: &str, license_key: Option<&str>) -> String {
    format!(
        "insert into gha_repos(id, name, org_id, org_login, license_key, license_name, license_prob, created_at, updated_at) \
         values({id}, {}, {ORG_ID}, 'org', {}, {}, {}, '2020-01-01 00:00:00', '2020-01-01 00:00:00');",
        sql_str(name),
        license_key.map(sql_str).unwrap_or_else(|| "null".to_string()),
        license_key
            .map(|k| sql_str(&format!("{k} license")))
            .unwrap_or_else(|| "null".to_string()),
        if license_key.is_some() { "100" } else { "null" }
    )
}

fn seed_actor(id: i64, login: &str, name: &str) -> String {
    format!(
        "insert into gha_actors(id, login, name) values({id}, {}, {});",
        sql_str(login),
        sql_str(name)
    )
}

fn seed_commit(
    sha: &str,
    event_id: i64,
    author_name: &str,
    author_email: &str,
    created_at: &str,
) -> String {
    format!(
        "insert into gha_commits(sha, event_id, author_name, message, is_distinct, dup_actor_id, dup_actor_login, \
         dup_repo_id, dup_repo_name, dup_type, dup_created_at, author_email) values({}, {event_id}, {}, 'msg', true, 11, \
         'alice', {REPO_ID}, {}, 'PushEvent', {}, {});",
        sql_str(sha),
        sql_str(author_name),
        sql_str(REPO),
        sql_ts(created_at),
        sql_str(author_email)
    )
}

fn seed_issue_row(id: i64, event_id: i64, number: i64, pr: bool) -> String {
    format!(
        "insert into gha_issues(id, event_id, body, comments, created_at, locked, number, state, title, updated_at, \
         user_id, is_pull_request, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
         dup_user_login) values({id}, {event_id}, 'b', 0, '2020-03-01 12:00:00', false, {number}, 'open', 'Issue {number}', \
         '2020-03-01 12:00:00', 11, {pr}, 11, 'alice', {REPO_ID}, {}, 'IssuesEvent', '2020-03-01 12:00:00', 'alice');",
        sql_str(REPO)
    )
}

fn seed_pr_row(id: i64, event_id: i64, number: i64) -> String {
    format!(
        "insert into gha_pull_requests(id, event_id, user_id, base_sha, head_sha, number, state, title, created_at, \
         updated_at, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) \
         values({id}, {event_id}, 12, 'aaaa', 'bbbb', {number}, 'open', 'PR {number}', '2020-04-01 08:00:00', \
         '2020-04-02 09:00:00', 12, 'bob', {REPO_ID}, {}, 'PullRequestEvent', '2020-04-01 08:00:00', 'bob');",
        sql_str(REPO)
    )
}

fn base_seed() -> String {
    let mut s = seed_event(
        1000,
        "IssuesEvent",
        REPO,
        REPO_ID,
        (11, "alice"),
        "2020-02-01T10:00:00Z",
    );
    s.push_str(&seed_repo(REPO_ID, REPO, None));
    s.push_str(&seed_actor(11, "alice", "Alice A"));
    s.push_str(&seed_actor(12, "bob", "Bob B"));
    s
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// The pass of the tool a case exercises (the others are skipped through
/// the `GHA2DB_GHAPISKIP*` variables).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pass {
    None,
    Licenses,
    Langs,
    Events,
    Commits,
    Comments,
    Reviews,
    Forks,
    Releases,
    Stars,
    All,
}

const SKIP_VARS: &[(Pass, &str)] = &[
    (Pass::Licenses, "GHA2DB_GHAPISKIPLICENSES"),
    (Pass::Langs, "GHA2DB_GHAPISKIPLANGS"),
    (Pass::Events, "GHA2DB_GHAPISKIPEVENTS"),
    (Pass::Commits, "GHA2DB_GHAPISKIPCOMMITS"),
    (Pass::Comments, "GHA2DB_GHAPISKIPCOMMENTS"),
    (Pass::Reviews, "GHA2DB_GHAPISKIPREVIEWS"),
    (Pass::Forks, "GHA2DB_GHAPISKIPFORKS"),
    (Pass::Releases, "GHA2DB_GHAPISKIPRELEASES"),
    (Pass::Stars, "GHA2DB_GHAPISKIPSTARS"),
];

type Setup = Box<dyn Fn(&FakeGitHub) + Send + Sync>;

struct Case {
    name: &'static str,
    pass: Pass,
    /// Extra environment (on top of the database, API URL and OAuth ones).
    env: Vec<(String, String)>,
    /// `GHA2DB_GITHUB_OAUTH` (`None`: not set → `/etc/github/oauths` lookup
    /// → anonymous); `@file` writes the token(s) to a file and passes its path.
    oauth: Option<String>,
    /// SQL run on the fresh database before the first run.
    seed: String,
    setup: Setup,
    /// Consecutive runs of the binary against the same database and API.
    runs: usize,
    /// `hide/hide.csv` content in the working directory.
    hide: Option<String>,
    /// Point the binary at a closed port instead of the fake API.
    dead_api: bool,
    /// Compare stdout lines in order (single-threaded runs) or as a sorted
    /// multiset.
    ordered: bool,
    /// Lines starting with one of these prefixes are compared by prefix
    /// only (documented wording deviations).
    loose: Vec<String>,
    /// Compare the final database contents (off for multi-threaded runs of
    /// the events pass: artificial event ids are `UnixNano / 31622` and
    /// collide at random between workers).
    compare_data: bool,
    /// Copy the targeted postprocess SQL scripts into `util_sql/` of the
    /// working directory (with `GHA2DB_LOCAL=1`).
    util_sql: bool,
    /// Create a second database and pass it as `GHA2DB_AFFILIATIONS_DB`.
    affs_db: bool,
}

impl Case {
    fn new(name: &'static str, pass: Pass) -> Case {
        let mut env = vec![
            ("GHA2DB_ST".to_string(), "1".to_string()),
            ("GHA2DB_RECENT_RANGE".to_string(), "10 years".to_string()),
            (
                "GHA2DB_RECENT_REPOS_RANGE".to_string(),
                "10 years".to_string(),
            ),
            ("GHA2DB_MAX_GHAPI_RETRY".to_string(), "2".to_string()),
        ];
        if pass == Pass::None {
            env.push(("GHA2DB_GHAPISKIP".to_string(), "1".to_string()));
        }
        for (p, var) in SKIP_VARS {
            if *p != pass && pass != Pass::All {
                env.push((var.to_string(), "1".to_string()));
            }
        }
        Case {
            name,
            pass,
            env,
            oauth: Some("tok1".to_string()),
            seed: base_seed(),
            setup: Box::new(|_| {}),
            runs: 1,
            hide: None,
            dead_api: false,
            ordered: true,
            loose: Vec::new(),
            compare_data: true,
            util_sql: false,
            affs_db: false,
        }
    }
    fn loose(mut self, prefix: &str) -> Self {
        self.loose.push(prefix.to_string());
        self
    }
    fn env(mut self, k: &str, v: &str) -> Self {
        self.env.retain(|(ek, _)| ek != k);
        self.env.push((k.to_string(), v.to_string()));
        self
    }
    /// Single-threaded, but Go's per-repo output order is random (map
    /// iteration): stdout compared as a multiset.
    fn unordered(mut self) -> Self {
        self.ordered = false;
        self
    }
    /// Multi-threaded (drop `GHA2DB_ST`): stdout compared as a multiset.
    fn mt(mut self) -> Self {
        self.env.retain(|(k, _)| k != "GHA2DB_ST");
        self.ordered = false;
        self
    }
    fn no_data_compare(mut self) -> Self {
        self.compare_data = false;
        self
    }
    fn oauth(mut self, o: Option<&str>) -> Self {
        self.oauth = o.map(str::to_string);
        self
    }
    /// Replace the default seed.
    fn seed_only(mut self, seed: &str) -> Self {
        self.seed = seed.to_string();
        self
    }
    fn seed(mut self, extra: &str) -> Self {
        self.seed.push_str(extra);
        self
    }
    fn setup(mut self, f: impl Fn(&FakeGitHub) + Send + Sync + 'static) -> Self {
        self.setup = Box::new(f);
        self
    }
    fn runs(mut self, n: usize) -> Self {
        self.runs = n;
        self
    }
    fn hide(mut self, csv: &str) -> Self {
        self.hide = Some(csv.to_string());
        self
    }
    fn dead_api(mut self) -> Self {
        self.dead_api = true;
        self
    }
    fn util_sql(mut self) -> Self {
        self.util_sql = true;
        self.env.push(("GHA2DB_LOCAL".to_string(), "1".to_string()));
        self
    }
    fn affs_db(mut self) -> Self {
        self.affs_db = true;
        self
    }
}

type TableDump = (Vec<String>, Vec<Vec<String>>);

/// One binary's runs of a case.
struct Side {
    db: TestDb,
    affs: Option<TestDb>,
    api: FakeGitHub,
    dir: TempDir,
    outs: Vec<Outcome>,
    bin_str: String,
}

fn mask_cell(v: &str) -> String {
    if BIG_ID.is_match(v) {
        return "<evid>".to_string();
    }
    if let Some(c) = TIMESTAMP.captures(v) {
        if c[1].parse::<i32>().unwrap() >= 2025 {
            return "<now>".to_string();
        }
    }
    v.to_string()
}

fn dump_db(db: &TestDb) -> BTreeMap<String, TableDump> {
    let con = db.conn();
    let mut res = BTreeMap::new();
    for t in cpg::tables(&con) {
        let mut cols = cpg::table_columns(&con, &t);
        cols.sort();
        let names: Vec<String> = cols.iter().map(|c| format!("\"{}\"::text", c.0)).collect();
        let order: Vec<String> = (1..=names.len()).map(|i| i.to_string()).collect();
        let mut rows: Vec<Vec<String>> = cpg::snapshot(
            &con,
            &format!(
                "select {} from \"{t}\" order by {}",
                names.join(", "),
                order.join(", ")
            ),
            &[],
        )
        .rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| mask_cell(&v)).collect())
        .collect();
        rows.sort();
        if !rows.is_empty() {
            res.insert(t, (cols.iter().map(|c| c.0.clone()).collect(), rows));
        }
    }
    con.close();
    res
}

impl Side {
    fn mask(&self, l: &str) -> String {
        let l = l.replace(self.api.base_url(), "<api>/");
        let l = l.replace(&self.bin_str, "<bin>");
        let l = l.replace(&self.db.name, "<db>");
        let l = match &self.affs {
            Some(a) => l.replace(&a.name, "<affsdb>"),
            None => l,
        };
        let l = DURATION.replace_all(&l, |c: &regex::Captures| {
            if is_go_duration(&c[2]) {
                format!("{}<dur>", &c[1])
            } else {
                c[0].to_string()
            }
        });
        let l = DURATION_LIST.replace_all(&l, |c: &regex::Captures| {
            let items: Vec<String> = c[2]
                .split(' ')
                .map(|d| {
                    if is_go_duration(d) {
                        "<dur>".to_string()
                    } else {
                        d.to_string()
                    }
                })
                .collect();
            format!("{}[{}]", &c[1], items.join(" "))
        });
        let l = RATE_RESET.replace_all(&l, "[rate reset in <dur>]");
        let l = EVENT_ID.replace_all(&l, "EventID: <evid>");
        let l = RECENT.replace_all(&l, "${1}<recent>");
        NOW.replace_all(&l, "<now>").into_owned()
    }
    /// All stdout lines of run `i`, masked. Progress lines and the
    /// `Warning: Duplicate event N(2): […]` lines (two workers computing
    /// `UnixNano / 31622` in the same 31.6 µs window — timing dependent) are
    /// dropped.
    fn lines(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stdout_str()
            .lines()
            .filter(|l| !PROGRESS.is_match(l) && !l.starts_with("Warning: Duplicate event "))
            .map(|l| self.mask(l))
            .collect()
    }
    fn sorted_lines(&self, i: usize) -> Vec<String> {
        let mut v = self.lines(i);
        v.sort();
        v
    }
    /// The `Error: '…'` stderr lines of run `i`, masked.
    fn errors(&self, i: usize) -> Vec<String> {
        self.outs[i]
            .stderr_str()
            .lines()
            .filter(|l| l.starts_with("Error: '"))
            .map(|l| self.mask(l))
            .collect()
    }
    fn code(&self, i: usize) -> Option<i32> {
        self.outs[i].code
    }
    fn expect_line(&self, i: usize, line: &str) {
        let lines = self.lines(i);
        assert!(
            lines.iter().any(|l| l == line),
            "missing {line:?} in run #{i}: {lines:#?}"
        );
    }
    fn expect_no_line(&self, i: usize, line: &str) {
        let lines = self.lines(i);
        assert!(
            !lines.iter().any(|l| l == line),
            "unexpected {line:?} in run #{i}: {lines:#?}"
        );
    }
    fn expect_prefix(&self, i: usize, prefix: &str) {
        let lines = self.lines(i);
        assert!(
            lines.iter().any(|l| l.starts_with(prefix)),
            "no line starting with {prefix:?} in run #{i}: {lines:#?}"
        );
    }
    fn expect_no_prefix(&self, i: usize, prefix: &str) {
        let lines = self.lines(i);
        assert!(
            !lines.iter().any(|l| l.starts_with(prefix)),
            "unexpected line starting with {prefix:?} in run #{i}: {lines:#?}"
        );
    }
    fn count_prefix(&self, i: usize, prefix: &str) -> usize {
        self.lines(i)
            .iter()
            .filter(|l| l.starts_with(prefix))
            .count()
    }
    fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let con = self.db.conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows
    }
    fn count(&self, sql: &str) -> i64 {
        self.query(sql)[0][0].parse().unwrap()
    }
    fn column(&self, sql: &str) -> Vec<String> {
        self.query(sql).into_iter().map(|r| r[0].clone()).collect()
    }
    fn affs_query(&self, sql: &str) -> Vec<Vec<String>> {
        let con = self.affs.as_ref().expect("affs db").conn();
        let snap = cpg::snapshot(&con, sql, &[]);
        con.close();
        snap.rows
    }
    /// Every non-empty table of the main (and the affiliations) database.
    fn data(&self) -> BTreeMap<String, TableDump> {
        let mut d = dump_db(&self.db);
        if let Some(a) = &self.affs {
            for (t, v) in dump_db(a) {
                d.insert(format!("affs.{t}"), v);
            }
        }
        d
    }
    /// The API requests, as sorted summaries (GraphQL bodies appended).
    fn requests(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .api
            .requests()
            .iter()
            .map(|r: &Logged| {
                let mut s = RECENT_SINCE
                    .replace_all(&r.summary(), "since=<recent>")
                    .into_owned();
                if r.method == "POST" {
                    s.push_str(" body=");
                    s.push_str(&String::from_utf8_lossy(&r.body));
                }
                s
            })
            .collect();
        v.sort();
        v
    }
    /// The GraphQL request bodies in arrival order.
    fn graphql_bodies(&self) -> Vec<String> {
        self.api
            .requests()
            .iter()
            .filter(|r| r.path == "/graphql")
            .map(|r| String::from_utf8_lossy(&r.body).into_owned())
            .collect()
    }
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let schema = fs::read_to_string(fixture("structure/full_structure.sql")).unwrap();
    let db = TestDb::fresh(&format!("ghapi2db_{}_{}", case.name, suffix))?;
    db.exec(&schema);
    if !case.seed.is_empty() {
        db.exec(&case.seed);
    }
    let affs = if case.affs_db {
        let a = TestDb::fresh(&format!("ghapi2db_{}_{}_affs", case.name, suffix))?;
        a.exec(&schema);
        Some(a)
    } else {
        None
    };
    let api = FakeGitHub::start();
    (case.setup)(&api);
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_ghapi2db_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    if let Some(csv) = &case.hide {
        fs::create_dir_all(dir.path().join("hide")).unwrap();
        fs::write(dir.path().join("hide").join("hide.csv"), csv).unwrap();
    }
    if case.util_sql {
        fs::create_dir_all(dir.path().join("util_sql")).unwrap();
        for f in [
            "postprocess_texts_ids.sql",
            "postprocess_labels_ids.sql",
            "postprocess_issues_prs_ids.sql",
        ] {
            fs::copy(
                fixture(&format!("ghapi2db/util_sql/{f}")),
                dir.path().join("util_sql").join(f),
            )
            .unwrap();
        }
    }
    let api_url = if case.dead_api {
        "http://127.0.0.1:1/".to_string()
    } else {
        api.base_url().to_string()
    };
    let mut env: Vec<(&str, &str)> = db.env();
    env.push(("GHA2DB_GITHUB_API_URL", leak(&api_url)));
    env.push(("GHA2DB_PROJECT", "test"));
    if let Some(a) = &affs {
        env.push(("GHA2DB_AFFILIATIONS_DB", leak(&a.name)));
    }
    if let Some(o) = &case.oauth {
        if let Some(tokens) = o.strip_prefix('@') {
            let p = dir.path().join("oauth.txt");
            fs::write(&p, format!("{tokens}\n")).unwrap();
            env.push(("GHA2DB_GITHUB_OAUTH", leak(p.to_str().unwrap())));
        } else {
            env.push(("GHA2DB_GITHUB_OAUTH", leak(o)));
        }
    }
    for (k, v) in &case.env {
        env.push((leak(k), leak(v)));
    }
    let mut inv = Invocation::new().cwd(dir.path());
    for (k, v) in &env {
        inv = inv.env(k, v);
    }
    let mut outs = Vec::new();
    for _ in 0..case.runs {
        outs.push(run(bin, &inv));
    }
    Some(Side {
        db,
        affs,
        api,
        dir,
        outs,
        bin_str: bin.to_str().unwrap().to_string(),
    })
}

/// Run the case with both binaries and assert that they agree; returns the
/// two sides (Go first) for case-specific assertions, `None` when the DB
/// tests are skipped.
fn check(case: Case) -> Option<(Side, Side)> {
    let rs = run_side(&rust_bin(), &case, "rs")?;
    let _ = &rs.dir;
    let Some(go_bin) = go_bin() else {
        // Without the Go reference at least make sure the runs finished.
        return None;
    };
    let go = run_side(&go_bin, &case, "go").unwrap();
    for i in 0..case.runs {
        let ctx = || {
            format!(
                "\ncase {} ({:?}) run #{i}\n--- go (code {:?}) stdout ---\n{}\n--- go stderr ---\n{}\n--- rust (code {:?}) stdout ---\n{}\n--- rust stderr ---\n{}\n",
                case.name,
                case.pass,
                go.code(i),
                go.outs[i].stdout_str(),
                go.outs[i].stderr_str(),
                rs.code(i),
                rs.outs[i].stdout_str(),
                rs.outs[i].stderr_str()
            )
        };
        if std::env::var_os("G2R_DUMP").is_some() {
            eprintln!("{}", ctx());
        }
        assert_eq!(go.code(i), rs.code(i), "exit code differs{}", ctx());
        let loosen = |lines: Vec<String>| -> Vec<String> {
            lines
                .into_iter()
                .map(
                    |l| match case.loose.iter().find(|p| l.starts_with(p.as_str())) {
                        Some(p) => format!("{p}<loose>"),
                        None => l,
                    },
                )
                .collect()
        };
        if case.ordered {
            assert_eq!(
                loosen(go.lines(i)),
                loosen(rs.lines(i)),
                "stdout differs{}",
                ctx()
            );
        } else {
            assert_eq!(
                loosen(go.sorted_lines(i)),
                loosen(rs.sorted_lines(i)),
                "stdout (multiset) differs{}",
                ctx()
            );
        }
        assert_eq!(go.errors(i), rs.errors(i), "Error lines differ{}", ctx());
    }
    if case.compare_data {
        assert_eq!(
            go.data(),
            rs.data(),
            "database contents differ (case {})",
            case.name
        );
    }
    assert_eq!(
        go.requests(),
        rs.requests(),
        "API requests differ (case {})",
        case.name
    );
    Some((go, rs))
}

/// Run `f` on both sides.
fn both(sides: &Option<(Side, Side)>, f: impl Fn(&Side)) {
    if let Some((go, rs)) = sides {
        f(go);
        f(rs);
    }
}

// ---------------------------------------------------------------------------
// Scenarios: general
// ---------------------------------------------------------------------------

#[test]
fn skip_ghapi_prints_only_the_time() {
    let sides = check(Case::new("skip_all", Pass::None));
    both(&sides, |s| {
        let lines = s.lines(0);
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(lines[0].starts_with("Compiled "), "{lines:?}");
        assert_eq!(lines[1], "Time: <dur>");
        assert!(s.requests().is_empty());
    });
}

// ---------------------------------------------------------------------------
// Scenarios: licenses
// ---------------------------------------------------------------------------

#[test]
fn licenses_found_not_found_and_already_set() {
    let sides = check(
        Case::new("lic_basic", Pass::Licenses)
            .seed(&seed_repo(501, "org/other", None))
            .seed(&seed_repo(502, "org/licensed", Some("mit")))
            .setup(|gh| {
                gh.get_ok(
                    "/repos/org/repo/license",
                    &license_json("apache-2.0", "Apache License 2.0"),
                );
                gh.get("/repos/org/other/license", vec![Scripted::not_found()]);
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Checking license on 2 repos");
        s.expect_line(0, "No license found for: org/other (404)");
        s.expect_line(0, "Processed 2, found 1 licenses, 1 not found, abuses 0");
        assert_eq!(
            s.query(
                "select name, license_key, license_name, license_prob from gha_repos order by name"
            ),
            vec![
                vec![
                    "org/licensed".to_string(),
                    "mit".to_string(),
                    "mit license".to_string(),
                    "100".to_string()
                ],
                vec![
                    "org/other".to_string(),
                    "not_found".to_string(),
                    "Not found".to_string(),
                    "0".to_string()
                ],
                vec![
                    REPO.to_string(),
                    "apache-2.0".to_string(),
                    "Apache License 2.0".to_string(),
                    "100".to_string()
                ],
            ]
        );
        assert_eq!(
            s.column("select updated_at::text from gha_repos where name = 'org/licensed'"),
            vec!["2020-01-01 00:00:00".to_string()]
        );
        assert_eq!(
            s.count("select count(*) from gha_repos where name <> 'org/licensed' and updated_at > '2025-01-01'"),
            2
        );
        assert_eq!(
            s.requests(),
            vec![
                format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"),
                format!("GET /repos/org/other/license accept={V3_ACCEPT} auth=tok1"),
                format!("GET /repos/org/repo/license accept={V3_ACCEPT} auth=tok1"),
            ]
        );
    });
}

#[test]
fn licenses_force_rechecks_licensed_repos() {
    let sides = check(
        Case::new("lic_force", Pass::Licenses)
            .env("GHA2DB_GHAPIFORCELICENSES", "1")
            .seed(&seed_repo(502, "org/licensed", Some("mit")))
            .setup(|gh| {
                gh.get_ok(
                    "/repos/org/repo/license",
                    &license_json("apache-2.0", "Apache License 2.0"),
                );
                gh.get_ok(
                    "/repos/org/licensed/license",
                    &license_json(
                        "bsd-3-clause",
                        "BSD 3-Clause \"New\" or \"Revised\" License",
                    ),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Checking license on 2 repos");
        s.expect_line(0, "Processed 2, found 2 licenses, 0 not found, abuses 0");
        assert_eq!(
            s.column("select license_key from gha_repos order by name"),
            vec!["bsd-3-clause".to_string(), "apache-2.0".to_string()]
        );
    });
}

#[test]
fn licenses_debug_prints_the_license() {
    let sides = check(
        Case::new("lic_debug", Pass::Licenses)
            .env("GHA2DB_DEBUG", "1")
            .seed(&seed_repo(501, "org/full", None))
            .setup(|gh| {
                gh.get_ok(
                    "/repos/org/repo/license",
                    &license_json("mit", "MIT License"),
                );
                gh.get_ok(
                    "/repos/org/full/license",
                    &json!({"name": "LICENSE", "license": {
                        "key": "apache-2.0", "name": "Apache License 2.0", "spdx_id": "Apache-2.0",
                        "url": "https://api.github.com/licenses/apache-2.0",
                        "html_url": "http://choosealicense.com/licenses/apache-2.0/",
                        "featured": true, "description": "A permissive license",
                        "implementation": "Create a text file",
                        "permissions": ["commercial-use", "modifications"], "conditions": [],
                        "body": "Apache License, Version 2.0"}}),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "org/repo license:github.License{Key:\"mit\", Name:\"MIT License\", URL:\"https://api.github.com/licenses/mit\", SPDXID:\"MIT\"}",
        );
        s.expect_line(
            0,
            "org/full license:github.License{Key:\"apache-2.0\", Name:\"Apache License 2.0\", URL:\"https://api.github.com/licenses/apache-2.0\", SPDXID:\"Apache-2.0\", HTMLURL:\"http://choosealicense.com/licenses/apache-2.0/\", Featured:true, Description:\"A permissive license\", Implementation:\"Create a text file\", Permissions:[\"commercial-use\" \"modifications\"], Conditions:[], Body:\"Apache License, Version 2.0\"}",
        );
    });
}

#[test]
fn licenses_abuse_403_is_retried() {
    let sides = check(
        Case::new("lic_abuse", Pass::Licenses)
            .seed(&seed_repo(501, "org/other", None))
            .setup(|gh| {
                gh.get(
                    "/repos/org/repo/license",
                    vec![
                        Scripted::abuse(None),
                        Scripted::ok(&license_json("mit", "MIT License")),
                    ],
                );
                gh.get(
                    "/repos/org/other/license",
                    vec![Scripted::error(403, "Forbidden")],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Licenses abuse detected on org/repo, retrying");
        // org/other is always 403: GHA2DB_MAX_GHAPI_RETRY=2 retries, then give up (Go bug 40)
        assert_eq!(
            s.count_prefix(0, "Licenses abuse detected on org/other, retrying"),
            2
        );
        s.expect_line(
            0,
            "Licenses abuse detected on org/other, giving up after 2 retries",
        );
        s.expect_line(0, "Processed 2, found 1 licenses, 0 not found, abuses 3");
        assert_eq!(
            s.column("select coalesce(license_key, '-') from gha_repos order by name"),
            vec!["-".to_string(), "mit".to_string()]
        );
        // every abuse refreshes the rate limits
        assert!(
            s.requests()
                .iter()
                .filter(|r| r.starts_with("GET /rate_limit "))
                .count()
                >= 2
        );
    });
}

#[test]
fn licenses_other_errors_and_null_bodies_skip() {
    let sides = check(
        Case::new("lic_errors", Pass::Licenses)
            .seed(&seed_repo(501, "org/five", None))
            .seed(&seed_repo(502, "org/nolic", None))
            .seed(&seed_repo(503, "org/null", None))
            .seed(&seed_repo(504, "org/gone", None))
            .setup(|gh| {
                gh.get(
                    "/repos/org/five/license",
                    vec![Scripted::error(500, "Server Error")],
                );
                gh.get_ok(
                    "/repos/org/nolic/license",
                    &json!({"name": "LICENSE", "license": null}),
                );
                gh.get(
                    "/repos/org/null/license",
                    vec![Scripted::raw(200, "application/json", "null")],
                );
                gh.get("/repos/org/gone/license", vec![Scripted::hangup()]);
                gh.get_ok(
                    "/repos/org/repo/license",
                    &license_json("mit", "MIT License"),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "No license found for: org/five, skipping (500)");
        s.expect_line(0, "No license found for: org/nolic (nil)");
        s.expect_line(0, "No license found for: org/null (nil)");
        s.expect_line(0, "License API response is null for org/gone, skipping");
        s.expect_line(0, "Processed 5, found 1 licenses, 0 not found, abuses 0");
    });
}

#[test]
fn licenses_low_points_wait_for_the_reset() {
    let sides = check(
        Case::new("lic_wait", Pass::Licenses)
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .setup(|gh| {
                gh.get(
                    "/rate_limit",
                    vec![
                        Scripted::ok(&rate_json(5000, 1, 1)),
                        Scripted::ok(&rate_json(5000, 4000, 3600)),
                    ],
                );
                gh.get_ok(
                    "/repos/org/repo/license",
                    &license_json("mit", "MIT License"),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "API limit reached while getting licenses data, waiting <dur>",
        );
        s.expect_line(0, "Processed 1, found 1 licenses, 0 not found, abuses 0");
        assert_eq!(
            s.requests()
                .iter()
                .filter(|r| r.starts_with("GET /rate_limit "))
                .count(),
            2
        );
    });
}

#[test]
fn licenses_low_points_long_reset_aborts() {
    let sides = check(Case::new("lic_abort", Pass::Licenses).setup(|gh| {
        gh.set_default_rate(5000, 1, 3600);
        gh.get_ok(
            "/repos/org/repo/license",
            &license_json("mit", "MIT License"),
        );
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "Error: API limit reached while getting licenses data, aborting, don't want to wait <dur>",
        );
        s.expect_no_prefix(0, "Processed ");
        assert_eq!(s.requests().len(), 1);
    });
}

#[test]
fn licenses_low_points_long_reset_is_fatal_when_asked() {
    let sides = check(
        Case::new("lic_fatal", Pass::Licenses)
            .env("GHA2DB_GHAPI_ERROR_FATAL", "1")
            .setup(|gh| {
                gh.set_default_rate(5000, 1, 3600);
            }),
    );
    both(&sides, |s| {
        assert_ne!(s.code(0), Some(0));
        assert_eq!(s.errors(0).len(), 1, "{:?}", s.errors(0));
        assert!(s.errors(0)[0].contains(
            "API limit reached while getting licenses data, aborting, don't want to wait "
        ));
    });
}

#[test]
fn licenses_refresh_the_rate_when_the_budget_runs_out() {
    let sides = check(
        Case::new("lic_budget", Pass::Licenses)
            .seed(&seed_repo(501, "org/a", None))
            .seed(&seed_repo(502, "org/b", None))
            .setup(|gh| {
                // allowed = rem / 10 = 1 → a rate refresh after every repo
                gh.set_default_rate(5000, 15, 3600);
                gh.get_ok(
                    "/repos/org/repo/license",
                    &license_json("mit", "MIT License"),
                );
                gh.get_ok("/repos/org/a/license", &license_json("mit", "MIT License"));
                gh.get("/repos/org/b/license", vec![Scripted::not_found()]);
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Processed 3, found 2 licenses, 1 not found, abuses 0");
        assert_eq!(
            s.requests()
                .iter()
                .filter(|r| r.starts_with("GET /rate_limit "))
                .count(),
            4
        );
    });
}

#[test]
fn licenses_multi_threaded() {
    let mut case = Case::new("lic_mt", Pass::Licenses).mt();
    for i in 0..25 {
        case = case.seed(&seed_repo(600 + i, &format!("org/r{i}"), None));
    }
    let sides = check(case.setup(|gh| {
        gh.get_ok(
            "/repos/org/repo/license",
            &license_json("mit", "MIT License"),
        );
        for i in 0..25 {
            if i % 3 == 0 {
                gh.get(
                    &format!("/repos/org/r{i}/license"),
                    vec![Scripted::not_found()],
                );
            } else {
                gh.get_ok(
                    &format!("/repos/org/r{i}/license"),
                    &license_json("apache-2.0", "Apache License 2.0"),
                );
            }
        }
    }));
    both(&sides, |s| {
        s.expect_line(0, "Checking license on 26 repos");
        s.expect_line(0, "Processed 26, found 17 licenses, 9 not found, abuses 0");
        assert_eq!(
            s.count("select count(*) from gha_repos where license_key = 'apache-2.0'"),
            16
        );
        assert_eq!(
            s.count("select count(*) from gha_repos where license_key = 'not_found'"),
            9
        );
    });
}

// ---------------------------------------------------------------------------
// Scenarios: programming languages
// ---------------------------------------------------------------------------

#[test]
fn langs_found_empty_and_not_found() {
    let sides = check(
        Case::new("langs_basic", Pass::Langs)
            .seed(&seed_repo(501, "org/empty", None))
            .seed(&seed_repo(502, "org/missing", None))
            .seed("insert into gha_repos_langs(repo_name, lang_name, lang_loc, lang_perc, dt) values('org/done', 'Go', 10, 100, '2020-01-01');")
            .seed(&seed_repo(503, "org/done", None))
            .setup(|gh| {
                gh.get_ok("/repos/org/repo/languages", &json!({"Go": 1500, "Shell": 400, "Makefile": 100}));
                gh.get_ok("/repos/org/empty/languages", &json!({}));
                gh.get("/repos/org/missing/languages", vec![Scripted::not_found()]);
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Checking programming languages on 3 repos");
        s.expect_line(0, "No programming languages found for: org/empty (0)");
        s.expect_line(0, "No programming languages found for: org/missing (404)");
        s.expect_line(
            0,
            "Processed 3, found languages on 1 repos, on 2 not found, abuses: 0",
        );
        assert_eq!(
            s.query("select repo_name, lang_name, lang_loc, round(lang_perc::numeric, 3)::text from gha_repos_langs order by 1, 2"),
            vec![
                vec!["org/done".to_string(), "Go".to_string(), "10".to_string(), "100.000".to_string()],
                vec!["org/empty".to_string(), "unknown".to_string(), "0".to_string(), "0.000".to_string()],
                vec!["org/missing".to_string(), "unknown".to_string(), "0".to_string(), "0.000".to_string()],
                vec![REPO.to_string(), "Go".to_string(), "1500".to_string(), "75.000".to_string()],
                vec![REPO.to_string(), "Makefile".to_string(), "100".to_string(), "5.000".to_string()],
                vec![REPO.to_string(), "Shell".to_string(), "400".to_string(), "20.000".to_string()],
            ]
        );
        // the "unknown" marker rows get the column default (now()) for dt
        assert_eq!(
            s.count("select count(*) from gha_repos_langs where dt > '2025-01-01'"),
            5
        );
        assert_eq!(
            s.count("select count(*) from gha_repos_langs where lang_name = 'unknown'"),
            2
        );
    });
}

#[test]
fn langs_force_replaces_and_zero_sum_is_reported() {
    let sides = check(
        Case::new("langs_force", Pass::Langs)
            .env("GHA2DB_GHAPIFORCELANGS", "1")
            .env("GHA2DB_DEBUG", "1")
            .seed("insert into gha_repos_langs(repo_name, lang_name, lang_loc, lang_perc, dt) values('org/repo', 'Perl', 10, 100, '2020-01-01');")
            .seed(&seed_repo(501, "org/zero", None))
            .setup(|gh| {
                gh.get_ok("/repos/org/repo/languages", &json!({"Go": 3, "Rust": 1}));
                gh.get_ok("/repos/org/zero/languages", &json!({"Go": 0, "C": 0}));
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "org/repo languages: map[Go:3 Rust:1]");
        s.expect_line(0, "All BOC sum to 0 for: org/zero");
        s.expect_line(
            0,
            "Processed 2, found languages on 1 repos, on 1 not found, abuses: 0",
        );
        assert_eq!(
            s.query("select lang_name, lang_loc, lang_perc::text from gha_repos_langs where repo_name = 'org/repo' order by 1"),
            vec![
                vec!["Go".to_string(), "3".to_string(), "75".to_string()],
                vec!["Rust".to_string(), "1".to_string(), "25".to_string()],
            ]
        );
    });
}

#[test]
fn langs_errors_abuse_and_hangup() {
    let sides = check(
        Case::new("langs_errors", Pass::Langs)
            .seed(&seed_repo(501, "org/five", None))
            .seed(&seed_repo(502, "org/gone", None))
            .setup(|gh| {
                gh.get(
                    "/repos/org/repo/languages",
                    vec![
                        Scripted::error(403, "Forbidden"),
                        Scripted::ok(&json!({"Go": 10})),
                    ],
                );
                gh.get(
                    "/repos/org/five/languages",
                    vec![Scripted::error(502, "Bad Gateway")],
                );
                gh.get("/repos/org/gone/languages", vec![Scripted::hangup()]);
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Languages abuse detected on org/repo, retrying");
        s.expect_line(0, "No languages found for: org/five, skipping (502)");
        s.expect_line(0, "Languages API response is null for org/gone, skipping");
        s.expect_line(
            0,
            "Processed 3, found languages on 1 repos, on 0 not found, abuses: 1",
        );
    });
}

#[test]
fn langs_low_points_long_reset_aborts() {
    let sides = check(Case::new("langs_abort", Pass::Langs).setup(|gh| {
        gh.set_default_rate(5000, 1, 3600);
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "Error: API limit reached while getting programming languages data, aborting, don't want to wait <dur>",
        );
        s.expect_no_prefix(0, "Processed ");
    });
}

#[test]
fn langs_multi_threaded() {
    let mut case = Case::new("langs_mt", Pass::Langs).mt();
    for i in 0..25 {
        case = case.seed(&seed_repo(600 + i, &format!("org/r{i}"), None));
    }
    let sides = check(case.setup(|gh| {
        gh.get_ok("/repos/org/repo/languages", &json!({"Go": 10}));
        for i in 0..25 {
            gh.get_ok(
                &format!("/repos/org/r{i}/languages"),
                &json!({"Go": 10 * (i + 1), "Shell": i}),
            );
        }
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "Processed 26, found languages on 26 repos, on 0 not found, abuses: 0",
        );
        // zero-byte languages are stored too (only an all-zero sum is skipped)
        assert_eq!(s.count("select count(*) from gha_repos_langs"), 1 + 25 + 25);
    });
}

// ---------------------------------------------------------------------------
// Scenarios: issue events
// ---------------------------------------------------------------------------

const EVENTS_PART: &str = "ghapi2db.go: Processing 1 repos - GHAPI Events part";

#[test]
fn events_new_issue_and_pr_are_added() {
    let issue = IssueSpec::new(7001, 1)
        .labels(vec![(31, "bug"), (30, "area/api")])
        .assignee((12, "bob"));
    let pr = IssueSpec::new(7002, 2)
        .pr()
        .state("closed")
        .milestone(milestone(41, 1, "v1.0"));
    let pr2 = pr.clone();
    let sides = check(Case::new("ev_new", Pass::Events).setup(move |gh| {
        gh.get_ok(
            &events_path(REPO),
            &json!([
                issue_event(
                    5001,
                    Some("labeled"),
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    Some(&issue)
                ),
                issue_event(
                    5002,
                    Some("closed"),
                    (12, "bob"),
                    "2020-05-02T11:00:00Z",
                    Some(&pr)
                ),
                issue_event(
                    5003,
                    Some("milestoned"),
                    (12, "bob"),
                    "2020-05-02T11:00:00Z",
                    Some(&pr)
                ),
            ]),
        );
        gh.get_ok(&pr_path(REPO, 2), &pr_json(&pr2));
    }));
    both(&sides, |s| {
        s.expect_line(0, EVENTS_PART);
        s.expect_line(0, "GH Repo Events/PRs API calls: 2");
        s.expect_line(
            0,
            "ghapi2db.go: Processing 1 PRs, 3 issues (3 with date collisions), manual mode: false - GHA part",
        );
        // one PR fetch per issue id, even with two events of the PR
        assert_eq!(
            s.requests()
                .iter()
                .filter(|r| r.contains("/pulls/2 "))
                .count(),
            1
        );
        assert_eq!(
            s.query("select id, event_id, number, state, title, is_pull_request, coalesce(milestone_id::text, '-'), coalesce(assignee_id::text, '-') from gha_issues order by event_id"),
            vec![
                vec!["7001".to_string(), (281474976710656i64 + 5001).to_string(), "1".to_string(), "open".to_string(), "Issue 1".to_string(), "false".to_string(), "-".to_string(), "12".to_string()],
                vec!["7002".to_string(), (281474976710656i64 + 5002).to_string(), "2".to_string(), "closed".to_string(), "PR 2".to_string(), "true".to_string(), "41".to_string(), "-".to_string()],
                vec!["7002".to_string(), (281474976710656i64 + 5003).to_string(), "2".to_string(), "closed".to_string(), "PR 2".to_string(), "true".to_string(), "41".to_string(), "-".to_string()],
            ]
        );
        assert_eq!(
            s.column("select label_id::text from gha_issues_labels order by label_id"),
            vec!["30".to_string(), "31".to_string()]
        );
        assert_eq!(
            s.count("select count(*) from gha_issues_assignees where assignee_id = 12"),
            1
        );
        // the PR itself is stored once (for the latest event of the PR)
        assert_eq!(
            s.query("select event_id::text from gha_pull_requests where id = 7102"),
            vec![vec![(281474976710656i64 + 5003).to_string()]]
        );
        assert_eq!(
            s.count("select count(*) from gha_milestones where id = 41"),
            2
        );
        assert_eq!(
            s.count("select count(*) from gha_events where id > 281474976710656"),
            3
        );
        assert_eq!(s.count("select count(*) from gha_payloads"), 3);
    });
}

#[test]
fn events_existing_state_is_kept_and_changes_reported() {
    let same = IssueSpec::new(7001, 1);
    let changed = IssueSpec::new(7003, 3).state("closed").title("New title");
    let sides = check(
        Case::new("ev_existing", Pass::Events)
            .seed(&seed_issue_row(7001, 281474976710656 + 5001, 1, false))
            .seed(&seed_issue_row(7003, 281474976710656 + 5003, 3, false))
            .setup(move |gh| {
                gh.get_ok(
                    &events_path(REPO),
                    &json!([
                        issue_event(
                            5001,
                            Some("reopened"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&same)
                        ),
                        issue_event(
                            5003,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-03T10:00:00Z",
                            Some(&changed)
                        ),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db.go: Processing 0 PRs, 2 issues (2 with date collisions), manual mode: false - GHA part",
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 2);
        assert_eq!(
            s.query("select state, title from gha_issues where id = 7003"),
            vec![vec!["closed".to_string(), "New title".to_string()]]
        );
        // the changed issue's row is updated in place and its (missing) event row added
        assert_eq!(
            s.column("select id::text from gha_events where id > 1000"),
            vec![(281474976710656i64 + 5003).to_string()]
        );
    });
}

#[test]
fn events_renamed_unknown_type_and_missing_fields() {
    let issue = IssueSpec::new(7001, 1);
    let other = IssueSpec::new(7004, 4);
    let sides = check(Case::new("ev_filters", Pass::Events).setup(move |gh| {
        gh.get_ok(
            &events_path(REPO),
            &json!([
                issue_event(
                    5001,
                    Some("renamed"),
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    Some(&issue)
                ),
                issue_event(
                    5002,
                    Some("some_new_type"),
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    Some(&other)
                ),
                issue_event(
                    5003,
                    None,
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    Some(&other)
                ),
                issue_event(
                    5004,
                    Some("closed"),
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    None
                ),
                issue_event(5005, Some("closed"), (11, "alice"), OLD, Some(&other)),
            ]),
        );
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "Warning: skipping event type some_new_type for issue org/repo 4",
        );
        s.expect_line(0, "Warning: Skipping event without type");
        s.expect_line(0, "Warning: Skipping event without issue");
        s.expect_line(
            0,
            "ghapi2db.go: Processing 0 PRs, 1 issues (1 with date collisions), manual mode: false - GHA part",
        );
        assert_eq!(
            s.query("select title, event_id from gha_issues"),
            vec![vec![
                "Renamed title".to_string(),
                (281474976710656i64 + 5001).to_string()
            ]]
        );
        // the old event stopped nothing: all events came in one page
        assert_eq!(s.count("select count(*) from gha_events"), 2);
    });
}

#[test]
fn events_duplicate_id_aborts_the_repo_page() {
    let issue = IssueSpec::new(7001, 1);
    let other = IssueSpec::new(7004, 4);
    let sides = check(
        Case::new("ev_dup", Pass::Events)
            .env("GHA2DB_DEBUG", "1")
            .setup(move |gh| {
                gh.get_ok(
                    &events_path(REPO),
                    &json!([
                        issue_event(
                            5001,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&issue)
                        ),
                        issue_event(
                            5001,
                            Some("reopened"),
                            (11, "alice"),
                            "2020-05-01T11:00:00Z",
                            Some(&issue)
                        ),
                        issue_event(
                            5002,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T12:00:00Z",
                            Some(&other)
                        ),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "Processing org/repo issue number 1, event: closed, date: 2020-05-01 10:00:00",
        );
        s.expect_line(
            0,
            "Note: duplicate GH event 5001, [7001 2], [org/repo org/repo]",
        );
        // the goroutine returns on the duplicate: the third event is never processed
        s.expect_no_line(
            0,
            "Processing org/repo issue number 4, event: closed, date: 2020-05-01 12:00:00",
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
    });
}

#[test]
fn events_paging_stops_at_old_events() {
    let issue = IssueSpec::new(7001, 1);
    let old = IssueSpec::new(7005, 5);
    let base = format!("{}?per_page=100", events_path(REPO));
    let sides = check(
        Case::new("ev_paging", Pass::Events)
            .env("GHA2DB_DEBUG", "1")
            .setup(move |gh| {
                gh.get(
                    &format!("{}?per_page=100", events_path(REPO)),
                    vec![Scripted::ok(&json!([issue_event(
                        5001,
                        Some("closed"),
                        (11, "alice"),
                        "2020-05-03T10:00:00Z",
                        Some(&issue)
                    ),]))
                    .paged(&base, 1, 3)],
                );
                gh.get(
                    &format!("{}?page=2&per_page=100", events_path(REPO)),
                    vec![Scripted::ok(&json!([
                        issue_event(
                            5002,
                            Some("reopened"),
                            (11, "alice"),
                            "2020-05-02T10:00:00Z",
                            Some(&issue)
                        ),
                        issue_event(5003, Some("closed"), (11, "alice"), OLD, Some(&old)),
                    ]))
                    .paged(&base, 2, 3)],
                );
                gh.get(
                    &format!("{}?page=3&per_page=100", events_path(REPO)),
                    vec![Scripted::ok(&json!([issue_event(
                        5004,
                        Some("closed"),
                        (11, "alice"),
                        OLD,
                        Some(&old)
                    ),]))
                    .paged(&base, 3, 3)],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "GH Repo Events/PRs API calls: 2");
        s.expect_line(0, "org/repo: [2020-05-03 10:00:00 +0000 UTC - 2020-05-03 10:00:00 +0000 UTC] < <recent> false");
        s.expect_line(0, "org/repo: [2010-01-01 00:00:00 +0000 UTC - 2020-05-02 10:00:00 +0000 UTC] < <recent> true");
        assert!(s
            .requests()
            .iter()
            .any(|r| r.starts_with("GET /repos/org/repo/issues/events?page=2&per_page=100 ")));
        assert!(!s.requests().iter().any(|r| r.contains("page=3")));
        assert_eq!(s.count("select count(*) from gha_issues"), 2);
    });
}

#[test]
fn events_two_pages_when_all_recent() {
    let issue = IssueSpec::new(7001, 1);
    let base = format!("{}?per_page=100", events_path(REPO));
    let sides = check(Case::new("ev_two_pages", Pass::Events).setup(move |gh| {
        gh.get(
            &format!("{}?per_page=100", events_path(REPO)),
            vec![Scripted::ok(&json!([issue_event(
                5001,
                Some("closed"),
                (11, "alice"),
                "2020-05-03T10:00:00Z",
                Some(&issue)
            ),]))
            .paged(&base, 1, 2)],
        );
        gh.get(
            &format!("{}?page=2&per_page=100", events_path(REPO)),
            vec![Scripted::ok(&json!([issue_event(
                5002,
                Some("reopened"),
                (11, "alice"),
                "2020-05-02T10:00:00Z",
                Some(&issue)
            ),]))
            .paged(&base, 2, 2)],
        );
    }));
    both(&sides, |s| {
        s.expect_line(0, "GH Repo Events/PRs API calls: 2");
        assert_eq!(s.count("select count(*) from gha_issues"), 2);
    });
}

#[test]
fn events_empty_page_debug_uses_now() {
    let sides = check(
        Case::new("ev_empty", Pass::Events)
            .env("GHA2DB_DEBUG", "1")
            .setup(|gh| {
                gh.get_ok(&events_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        // min = time.Now() (no events), max = the recent date
        let lines = s.lines(0);
        assert!(
            lines.iter().any(
                |l| l.starts_with("org/repo: [<now> - 20") && l.ends_with("] < <recent> false")
            ),
            "{lines:#?}"
        );
        s.expect_line(0, "GH Repo Events/PRs API calls: 1");
        s.expect_line(
            0,
            "ghapi2db.go: Processing 0 PRs, 0 issues (0 with date collisions), manual mode: false - GHA part",
        );
    });
}

#[test]
fn events_date_range_filters() {
    let issue = IssueSpec::new(7001, 1);
    let sides = check(
        Case::new("ev_dtrange", Pass::Events)
            .env("DTFROM", "2020-05-02")
            .env("DTTO", "2020-05-03")
            .setup(move |gh| {
                gh.get_ok(
                    &events_path(REPO),
                    &json!([
                        issue_event(
                            5001,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&issue)
                        ),
                        issue_event(
                            5002,
                            Some("reopened"),
                            (11, "alice"),
                            "2020-05-02T10:00:00Z",
                            Some(&issue)
                        ),
                        issue_event(
                            5003,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-04T10:00:00Z",
                            Some(&issue)
                        ),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        assert_eq!(
            s.column("select event_id::text from gha_issues"),
            vec![(281474976710656i64 + 5002).to_string()]
        );
    });
}

#[test]
fn events_single_issue_milestone_and_repo_filters() {
    let i1 = IssueSpec::new(7001, 1).milestone(milestone(41, 1, "v1.0"));
    let i2 = IssueSpec::new(7002, 2).milestone(milestone(42, 2, "v2.0"));
    let i3 = IssueSpec::new(7003, 3);
    let sides = check(
        Case::new("ev_single", Pass::Events)
            .env("MILESTONE", "v1.0")
            .env("ISSUE", "1")
            .env("REPO", REPO)
            .seed(&seed_event(
                1001,
                "PushEvent",
                "org/other",
                501,
                (12, "bob"),
                "2020-02-02T10:00:00Z",
            ))
            .setup(move |gh| {
                gh.get_ok(
                    &events_path(REPO),
                    &json!([
                        issue_event(
                            5001,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&i1)
                        ),
                        issue_event(
                            5002,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&i2)
                        ),
                        issue_event(
                            5003,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&i3)
                        ),
                    ]),
                );
                gh.get_ok(&events_path("org/other"), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "ghapi2db.go: Processing 2 repos - GHAPI Events part");
        s.expect_line(0, "GH Repo Events/PRs API calls: 1");
        assert!(!s.requests().iter().any(|r| r.contains("org/other")));
        assert_eq!(
            s.column("select id::text from gha_issues"),
            vec!["7001".to_string()]
        );
    });
}

#[test]
fn events_skip_issues_or_prs() {
    let issue = IssueSpec::new(7001, 1);
    let pr = IssueSpec::new(7002, 2).pr();
    let pr2 = pr.clone();
    let mk = |name: &'static str, var: &'static str| {
        let issue = issue.clone();
        let pr = pr.clone();
        let pr2 = pr2.clone();
        Case::new(name, Pass::Events)
            .env(var, "1")
            .setup(move |gh| {
                gh.get_ok(
                    &events_path(REPO),
                    &json!([
                        issue_event(
                            5001,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&issue)
                        ),
                        issue_event(
                            5002,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&pr)
                        ),
                    ]),
                );
                gh.get_ok(&pr_path(REPO, 2), &pr_json(&pr2));
            })
    };
    let sides = check(mk("ev_skip_issues", "GHA2DB_GHAPISKIPISSUES"));
    both(&sides, |s| {
        // the PR is still fetched, but SyncIssuesState skips everything with SkipAPIIssues
        s.expect_line(0, "GH Repo Events/PRs API calls: 2");
        s.expect_line(
            0,
            "ghapi2db.go: Processing 1 PRs, 1 issues (1 with date collisions), manual mode: false - GHA part",
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
        // …but not the PR part
        assert_eq!(s.count("select count(*) from gha_pull_requests"), 1);
    });
    let sides = check(mk("ev_skip_prs", "GHA2DB_GHAPISKIPPRS"));
    both(&sides, |s| {
        s.expect_line(0, "GH Repo Events/PRs API calls: 1");
        assert_eq!(
            s.column("select id::text from gha_issues"),
            vec!["7001".to_string()]
        );
        assert!(!s.requests().iter().any(|r| r.contains("/pulls/")));
    });
}

#[test]
fn events_not_found_repo_warns() {
    let sides = check(Case::new("ev_404", Pass::Events).setup(|gh| {
        gh.get(&events_path(REPO), vec![Scripted::not_found()]);
    }));
    both(&sides, |s| {
        s.expect_prefix(0, "Not found (Issues.ListRepositoryEvents) for ");
        s.expect_line(0, "Warning: not found: org/repo");
        s.expect_line(0, "GH Repo Events/PRs API calls: 1");
    });
}

#[test]
fn events_abuse_then_ok() {
    let issue = IssueSpec::new(7001, 1);
    let sides = check(
        Case::new("ev_abuse", Pass::Events)
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .setup(move |gh| {
                gh.get(
                    &events_path(REPO),
                    vec![
                        Scripted::abuse(Some(1)),
                        Scripted::ok(&json!([issue_event(
                            5001,
                            Some("closed"),
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some(&issue)
                        )])),
                    ],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_prefix(0, "Abuse detected (Issues.ListRepositoryEvents) for ");
        s.expect_line(0, "GitHub API abuse detected (issues events), wait <dur>");
        s.expect_line(0, "GH Repo Events/PRs API calls: 2");
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
    });
}

#[test]
fn events_server_errors_exhaust_retries() {
    let sides = check(Case::new("ev_502", Pass::Events).setup(|gh| {
        gh.get(
            &events_path(REPO),
            vec![Scripted::error(502, "Server Error")],
        );
    }));
    both(&sides, |s| {
        assert_eq!(
            s.count_prefix(0, "Server Error (Issues.ListRepositoryEvents) for "),
            2
        );
        s.expect_line(
            0,
            "Error: GetRateLimit call failed 2 times while getting events, aborting",
        );
        s.expect_line(0, "GH Repo Events/PRs API calls: 2");
    });
}

#[test]
fn events_server_errors_fatal_when_asked() {
    let sides = check(
        Case::new("ev_502_fatal", Pass::Events)
            .env("GHA2DB_GHAPI_ERROR_FATAL", "1")
            .setup(|gh| {
                gh.get(
                    &events_path(REPO),
                    vec![Scripted::error(502, "Server Error")],
                );
            }),
    );
    both(&sides, |s| {
        assert_ne!(s.code(0), Some(0));
        assert!(
            s.errors(0)
                .iter()
                .any(|e| e
                    .contains("GetRateLimit call failed 2 times while getting events, aborting")),
            "{:?}",
            s.errors(0)
        );
    });
}

#[test]
fn events_unknown_error_exits_zero() {
    let sides = check(Case::new("ev_500", Pass::Events).setup(|gh| {
        gh.get(
            &events_path(REPO),
            vec![Scripted::error(500, "Internal Server Error")],
        );
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "<bin> error: *github.ErrorResponse:GET <api>/repos/org/repo/issues/events?per_page=100: 500 Internal Server Error [], non fatal, exiting 0 status",
        );
        s.expect_no_prefix(0, "GH Repo Events/PRs API calls: ");
    });
}

#[test]
fn events_low_points_wait_then_abort() {
    let sides = check(
        Case::new("ev_low_points", Pass::Events)
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .setup(|gh| {
                gh.get(
                    "/rate_limit",
                    vec![
                        Scripted::ok(&rate_json(5000, 1, 1)),
                        Scripted::ok(&rate_json(5000, 1, 3600)),
                    ],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "API limit reached while getting events data, waiting <dur> (0)",
        );
        s.expect_line(0, "Error: API limit reached while getting issues events data, aborting, don't want to wait <dur>");
        s.expect_line(0, "GH Repo Events/PRs API calls: 0");
    });
}

#[test]
fn events_pr_fetch_errors() {
    let pr = IssueSpec::new(7002, 2).pr();
    let pr_b = pr.clone();
    let sides = check(Case::new("ev_pr_404", Pass::Events).setup(move |gh| {
        gh.get_ok(
            &events_path(REPO),
            &json!([issue_event(
                5002,
                Some("closed"),
                (11, "alice"),
                "2020-05-01T10:00:00Z",
                Some(&pr)
            )]),
        );
        gh.get(&pr_path(REPO, 2), vec![Scripted::not_found()]);
    }));
    both(&sides, |s| {
        // a 404 of the PR is retried like any other error, then the repo is given up
        assert_eq!(s.count_prefix(0, "Not found (PullRequests.Get) for "), 2);
        s.expect_line(
            0,
            "Error: GetRateLimit call failed 2 times while getting PR, aborting",
        );
        s.expect_line(0, "GH Repo Events/PRs API calls: 3");
        // the issue is still synced, without PR data
        assert_eq!(
            s.count("select count(*) from gha_issues where id = 7002"),
            1
        );
        assert_eq!(s.count("select count(*) from gha_pull_requests"), 0);
    });
    let sides = check(Case::new("ev_pr_502", Pass::Events).setup(move |gh| {
        gh.get_ok(
            &events_path(REPO),
            &json!([issue_event(
                5002,
                Some("closed"),
                (11, "alice"),
                "2020-05-01T10:00:00Z",
                Some(&pr_b)
            )]),
        );
        gh.get(
            &pr_path(REPO, 2),
            vec![Scripted::error(502, "Server Error")],
        );
    }));
    both(&sides, |s| {
        assert_eq!(s.count_prefix(0, "Server Error (PullRequests.Get) for "), 2);
        s.expect_line(
            0,
            "Error: GetRateLimit call failed 2 times while getting PR, aborting",
        );
        s.expect_line(0, "GH Repo Events/PRs API calls: 3");
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
    });
}

#[test]
fn events_github_debug_output() {
    let pr = IssueSpec::new(7002, 2).pr();
    let pr2 = pr.clone();
    let sides = check(
        Case::new("ev_ghdebug", Pass::Events)
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .env("GHA2DB_DEBUG", "2")
            .setup(move |gh| {
                gh.get_ok(
                    &events_path(REPO),
                    &json!([issue_event(
                        5002,
                        Some("closed"),
                        (11, "alice"),
                        "2020-05-01T10:00:00Z",
                        Some(&pr)
                    )]),
                );
                gh.get_ok(&pr_path(REPO, 2), &pr_json(&pr2));
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "Issues Repo Events Try: 0, rem: [4999], waitPeriod: [<dur>], hint: 0",
        );
        s.expect_line(
            0,
            "API call for issues events org/repo (1), remaining GHAPI points [4999], hint: 0",
        );
        s.expect_line(
            0,
            "Get PR Try: 0, rem: [4999], waitPeriod: [<dur>], hint: 0",
        );
        s.expect_line(
            0,
            "API call for org/repo PR: 2, remaining GHAPI points [4999], hint: 0",
        );
        s.expect_prefix(0, "Processing {Repo: org/repo, Number: 2, IssueID: 7002, EventID: <evid>, EventType: closed, Pr: true");
        s.expect_line(
            0,
            "GHA Issue ID '7002' --> PR ID 7102, updated 2020-04-02 09:00:00 +0000 UTC",
        );
        s.expect_line(0, "Final GHAPI threads join");
    });
}

#[test]
fn events_multi_threaded_many_repos() {
    let mut case = Case::new("ev_mt", Pass::Events).mt().no_data_compare();
    for i in 0..12 {
        case = case.seed(&seed_event(
            2000 + i,
            "PushEvent",
            &format!("org/r{i}"),
            600 + i,
            (11, "alice"),
            "2020-02-02T10:00:00Z",
        ));
    }
    let sides = check(case.setup(|gh| {
        gh.get_ok(&events_path(REPO), &json!([]));
        for i in 0..12 {
            let issue = IssueSpec::new(8000 + i, i + 1).repo(&format!("org/r{i}"));
            gh.get_ok(
                &events_path(&format!("org/r{i}")),
                &json!([issue_event(
                    6000 + i,
                    Some("closed"),
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    Some(&issue)
                )]),
            );
        }
    }));
    both(&sides, |s| {
        s.expect_line(0, "ghapi2db.go: Processing 13 repos - GHAPI Events part");
        s.expect_line(0, "GH Repo Events/PRs API calls: 13");
        assert_eq!(s.count("select count(*) from gha_issues"), 12);
        assert_eq!(
            s.count("select count(distinct dup_repo_name) from gha_issues"),
            12
        );
    });
}

// ---------------------------------------------------------------------------
// Scenarios: commits enrichment
// ---------------------------------------------------------------------------

const SHA1: &str = "1111111111111111111111111111111111111111";
const SHA2: &str = "2222222222222222222222222222222222222222";
const SHA3: &str = "3333333333333333333333333333333333333333";

fn commits_path(repo: &str) -> String {
    format!("/repos/{repo}/commits")
}

fn commits_seed() -> String {
    // two commits with an author email (the autofetch range starts 2 minutes
    // before the newest of them) and a newer one without
    let mut s = seed_commit(
        SHA1,
        1000,
        "Alice A",
        "alice@example.com",
        "2020-03-01T10:00:00Z",
    );
    s.push_str(&seed_commit(
        SHA2,
        1000,
        "Bob B",
        "bob@example.com",
        "2020-03-01T11:00:00Z",
    ));
    s.push_str(&seed_commit(
        SHA3,
        1000,
        "Carol C",
        "",
        "2020-03-01T12:00:00Z",
    ));
    s
}

#[test]
fn commits_enrich_existing_with_autofetch_range() {
    let sides = check(
        Case::new("cm_enrich", Pass::Commits)
            .env("GHA2DB_DEBUG", "1")
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get_ok(
                    &commits_path(REPO),
                    &json!([
                        CommitSpec::new(SHA3)
                            .author(Some((13, "carol")), "Carol C", "carol@example.com")
                            .committer(Some((11, "alice")), "Alice A", "alice@example.com")
                            .date("2020-03-01T12:00:00Z")
                            .json(),
                        CommitSpec::new(SHA2)
                            .author(Some((12, "bob")), "Bob B", "bob@example.com")
                            .committer(Some((12, "bob")), "Bob B", "bob@example.com")
                            .date("2020-03-01T11:00:00Z")
                            .json(),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "ghapi2db.go: Processing 1 repos - GHAPI commits part");
        s.expect_line(0, "org/repo: 2020-03-01 10:58:00 - 2020-03-01 12:02:00");
        s.expect_line(0, "org/repo: processing 2 commits, page 1");
        // carol is unknown in gha_actors → hashed id ≠ API id → inserted
        s.expect_line(0, "DB Author ID: -5783588138700960654 != API Author ID: 13, SHA: 3333333333333333333333333333333333333333, login: carol");
        s.expect_line(0, "GH Commits API calls: 1");
        assert!(s.requests().iter().any(|r| r.starts_with(
            "GET /repos/org/repo/commits?per_page=100&since=2020-03-01T10%3A58%3A00Z&until=2020-03-01T12%3A02%3A00Z "
        )), "{:?}", s.requests());
        assert_eq!(
            s.query("select sha, author_name, author_email, committer_name, committer_email, coalesce(author_id::text, '-'), coalesce(committer_id::text, '-'), coalesce(dup_author_login, '-'), coalesce(dup_committer_login, '-') from gha_commits order by sha"),
            vec![
                vec![SHA1.to_string(), "Alice A".to_string(), "alice@example.com".to_string(), "".to_string(), "".to_string(), "-".to_string(), "-".to_string(), "".to_string(), "".to_string()],
                vec![SHA2.to_string(), "Bob B".to_string(), "bob@example.com".to_string(), "Bob B".to_string(), "bob@example.com".to_string(), "12".to_string(), "12".to_string(), "bob".to_string(), "bob".to_string()],
                vec![SHA3.to_string(), "Carol C".to_string(), "carol@example.com".to_string(), "Alice A".to_string(), "alice@example.com".to_string(), "13".to_string(), "11".to_string(), "carol".to_string(), "alice".to_string()],
            ]
        );
        assert_eq!(
            s.query("select id, login, name from gha_actors where id = 13"),
            vec![vec![
                "13".to_string(),
                "carol".to_string(),
                "Carol C".to_string()
            ]]
        );
        assert_eq!(
            s.query("select actor_id, email, origin from gha_actors_emails order by 1, 2"),
            vec![
                vec![
                    "11".to_string(),
                    "alice@example.com".to_string(),
                    "1".to_string()
                ],
                vec![
                    "12".to_string(),
                    "bob@example.com".to_string(),
                    "1".to_string()
                ],
                vec![
                    "13".to_string(),
                    "carol@example.com".to_string(),
                    "1".to_string()
                ],
            ]
        );
        assert_eq!(
            s.query("select actor_id, name, origin from gha_actors_names order by 1, 2"),
            vec![
                vec!["11".to_string(), "Alice A".to_string(), "1".to_string()],
                vec!["12".to_string(), "Bob B".to_string(), "1".to_string()],
                vec!["13".to_string(), "Carol C".to_string(), "1".to_string()],
            ]
        );
    });
}

#[test]
fn commits_without_gha_commits_skip_the_repo() {
    let sides = check(
        Case::new("cm_norange", Pass::Commits)
            .env("GHA2DB_DEBUG", "1")
            .setup(|gh| {
                gh.get_ok(&commits_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "org/repo: no date from");
        s.expect_line(0, "GH Commits API calls: 0");
        assert!(
            !s.requests().iter().any(|r| r.contains("/repos/")),
            "{:?}",
            s.requests()
        );
    });
}

#[test]
fn commits_no_autofetch_uses_the_recent_date() {
    let sides = check(
        Case::new("cm_noauto", Pass::Commits)
            .env("GHA2DB_NO_AUTOFETCHCOMMITS", "1")
            .env("GHA2DB_DEBUG", "2")
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get_ok(
                    &commits_path(REPO),
                    &json!([
                        CommitSpec::new(SHA1).date("2020-03-01T10:00:30Z").json(),
                        CommitSpec::new("9999999999999999999999999999999999999999").json(),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "GHA GHAPI time difference for sha 1111111111111111111111111111111111111111: <dur>",
        );
        s.expect_line(0, "SHA 9999999999999999999999999999999999999999 not found");
        s.expect_line(0, "GH Commits API calls: 1");
        assert!(
            s.requests()
                .iter()
                .any(|r| r.starts_with("GET /repos/org/repo/commits?per_page=100&since=<recent> ")),
            "{:?}",
            s.requests()
        );
        // the unknown sha still records the author's email/name
        assert_eq!(
            s.count("select count(*) from gha_actors_emails where actor_id = 11"),
            1
        );
        assert_eq!(s.count("select count(*) from gha_commits"), 3);
    });
}

#[test]
fn commits_date_range_from_env() {
    let sides = check(
        Case::new("cm_dtrange", Pass::Commits)
            .env("DTFROM", "2020-03-01 09:00:00")
            .env("DTTO", "2020-03-01T13:00:00Z")
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get_ok(&commits_path(REPO), &json!([CommitSpec::new(SHA1).json()]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "GH Commits API calls: 1");
        assert!(s.requests().iter().any(|r| r.starts_with(
            "GET /repos/org/repo/commits?per_page=100&since=2020-03-01T09%3A00%3A00Z&until=2020-03-01T13%3A00%3A00Z "
        )), "{:?}", s.requests());
    });
}

#[test]
fn commits_author_name_mismatch_and_missing_users() {
    let sides = check(
        Case::new("cm_mismatch", Pass::Commits)
            .env("GHA2DB_DEBUG", "1")
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get_ok(
                    &commits_path(REPO),
                    &json!([
                        // no GitHub users attached (unknown emails)
                        CommitSpec::new(SHA1)
                            .author(None, "Alicia", "alice@example.com")
                            .committer(None, "GitHub", "noreply@github.com")
                            .json(),
                        // committer differs from the DB actor id
                        CommitSpec::new(SHA2)
                            .author(Some((12, "bob")), "Bob B", "bob@example.com")
                            .committer(Some((99, "bob")), "Bob B", "bob@example.com")
                            .json(),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Author name mismatch API: Alicia, DB: Alice A, SHA: 1111111111111111111111111111111111111111");
        s.expect_line(0, "DB Committer ID: 12 != API Committer ID: 99, sha: 2222222222222222222222222222222222222222, login: bob");
        assert_eq!(
            s.query(&format!("select author_name, committer_name, committer_email, coalesce(author_id::text, '-'), coalesce(committer_id::text, '-') from gha_commits where sha in ('{SHA1}', '{SHA2}') order by sha")),
            vec![
                vec!["Alicia".to_string(), "GitHub".to_string(), "noreply@github.com".to_string(), "-".to_string(), "-".to_string()],
                vec!["Bob B".to_string(), "Bob B".to_string(), "bob@example.com".to_string(), "12".to_string(), "99".to_string()],
            ]
        );
        // actor 99 (bob) inserted; emails for actor 0 (no user) recorded too
        assert_eq!(
            s.query("select login, name from gha_actors where id = 99"),
            vec![vec!["bob".to_string(), "Bob B".to_string()]]
        );
        assert_eq!(
            s.column("select email from gha_actors_emails where actor_id = 0 order by 1"),
            vec![
                "alice@example.com".to_string(),
                "noreply@github.com".to_string()
            ]
        );
    });
}

#[test]
fn commits_shared_affiliations_db() {
    let sides = check(
        Case::new("cm_affs", Pass::Commits)
            .affs_db()
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get_ok(&commits_path(REPO), &json!([CommitSpec::new(SHA1).json()]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "GH Commits API calls: 1");
        assert_eq!(s.count("select count(*) from gha_actors_emails"), 0);
        assert_eq!(s.count("select count(*) from gha_actors_names"), 0);
        assert_eq!(
            s.affs_query("select actor_id, email from gha_actors_emails"),
            vec![vec!["11".to_string(), "alice@example.com".to_string()]]
        );
        assert_eq!(
            s.affs_query("select actor_id, name from gha_actors_names"),
            vec![vec!["11".to_string(), "Alice A".to_string()]]
        );
        // the commit itself is enriched in the project database
        assert_eq!(s.count(&format!("select count(*) from gha_commits where sha = '{SHA1}' and committer_email = 'alice@example.com'")), 1);
    });
}

#[test]
fn commits_hidden_actors() {
    let sides = check(
        Case::new("cm_hide", Pass::Commits)
            .hide(&format!("sha1\n{ALICE_SHA1}\n"))
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get_ok(&commits_path(REPO), &json!([CommitSpec::new(SHA1).json()]));
            }),
    );
    both(&sides, |s| {
        // only the login `alice` is listed: names/emails hash differently and stay
        assert_eq!(
            s.query(&format!("select author_name, author_email, dup_author_login, dup_committer_login from gha_commits where sha = '{SHA1}'")),
            vec![vec![
                "Alice A".to_string(),
                "alice@example.com".to_string(),
                format!("anon-{ALICE_SHA1}"),
                format!("anon-{ALICE_SHA1}"),
            ]]
        );
        assert_eq!(
            s.column("select email from gha_actors_emails"),
            vec!["alice@example.com".to_string()]
        );
    });
}

#[test]
fn commits_paging_and_progress() {
    let base = format!(
        "{}?per_page=100&since=2020-03-01T10%3A58%3A00Z&until=2020-03-01T12%3A02%3A00Z",
        commits_path(REPO)
    );
    let sides = check(
        Case::new("cm_paging", Pass::Commits)
            .env("GHA2DB_DEBUG", "1")
            .seed(&commits_seed())
            .setup(move |gh| {
                gh.get(
                    &format!("{}?per_page=100", commits_path(REPO)),
                    vec![Scripted::ok(&json!([CommitSpec::new(SHA3).json()])).paged(&base, 1, 2)],
                );
                gh.get(
                    &format!("{}?page=2&per_page=100", commits_path(REPO)),
                    vec![Scripted::ok(&json!([CommitSpec::new(SHA2).json()])).paged(&base, 2, 2)],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "org/repo: processing 1 commits, page 1");
        s.expect_line(0, "org/repo: processing 1 commits, page 2");
        s.expect_line(0, "GH Commits API calls: 2");
        assert!(
            s.requests()
                .iter()
                .any(|r| r.contains("page=2&per_page=100&since=")),
            "{:?}",
            s.requests()
        );
    });
}

#[test]
fn commits_error_paths() {
    let sides = check(
        Case::new("cm_404", Pass::Commits)
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get(&commits_path(REPO), vec![Scripted::not_found()]);
            }),
    );
    both(&sides, |s| {
        s.expect_prefix(0, "Not found (Repositories.ListCommits) for org/repo: ");
        s.expect_line(0, "Warning: not found: org/repo");
        s.expect_line(0, "GH Commits API calls: 1");
    });
    let sides = check(
        Case::new("cm_abuse", Pass::Commits)
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get(
                    &commits_path(REPO),
                    vec![
                        Scripted::abuse(Some(1)),
                        Scripted::ok(&json!([CommitSpec::new(SHA1).json()])),
                    ],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Abuse detected (Repositories.ListCommits) for org/repo");
        s.expect_line(0, "GitHub API abuse detected (issues events), wait <dur>");
        s.expect_line(
            0,
            "Repo commits Try: 1, rem: [4999], waitPeriod: [<dur>], hint: 0",
        );
        s.expect_line(0, "GH Commits API calls: 2");
    });
    let sides = check(
        Case::new("cm_502", Pass::Commits)
            .seed(&commits_seed())
            .setup(|gh| {
                gh.get(
                    &commits_path(REPO),
                    vec![Scripted::error(502, "Server Error")],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "Error: GetRateLimit call failed 2 times while getting events, aborting",
        );
        s.expect_line(0, "GH Commits API calls: 2");
    });
    let sides = check(
        Case::new("cm_limit", Pass::Commits)
            .seed(&commits_seed())
            .setup(|gh| {
                gh.set_default_rate(5000, 0, 3600);
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "Error: API limit reached while getting commits data, aborting, don't want to wait <dur>");
        s.expect_line(0, "GH Commits API calls: 0");
    });
}

#[test]
fn commits_multi_threaded() {
    let mut case = Case::new("cm_mt", Pass::Commits).mt();
    for i in 0..10 {
        let repo = format!("org/r{i}");
        case = case.seed(&seed_event(
            2000 + i,
            "PushEvent",
            &repo,
            600 + i,
            (11, "alice"),
            "2020-02-02T10:00:00Z",
        ));
        case = case.seed(&format!(
            "insert into gha_commits(sha, event_id, author_name, message, is_distinct, dup_actor_id, dup_actor_login, \
             dup_repo_id, dup_repo_name, dup_type, dup_created_at, author_email) values('{i}{i}{i}', {}, 'X', 'm', true, 11, \
             'alice', {}, '{repo}', 'PushEvent', '2020-03-01 10:00:00', 'x@example.com');",
            2000 + i,
            600 + i
        ));
    }
    let sides = check(case.seed(&commits_seed()).setup(|gh| {
        gh.get_ok(&commits_path(REPO), &json!([CommitSpec::new(SHA1).json()]));
        for i in 0..10 {
            gh.get_ok(
                &commits_path(&format!("org/r{i}")),
                &json!([CommitSpec::new(&format!("{i}{i}{i}"))
                    .author(Some((12, "bob")), "Bob B", "bob@example.com")
                    .json()]),
            );
        }
    }));
    both(&sides, |s| {
        s.expect_line(0, "ghapi2db.go: Processing 11 repos - GHAPI commits part");
        s.expect_line(0, "GH Commits API calls: 11");
        assert_eq!(
            s.count("select count(*) from gha_commits where author_id = 12"),
            10
        );
    });
}

// ---------------------------------------------------------------------------
// Scenarios: API restores (comments, reviews, forks, releases, stars)
// ---------------------------------------------------------------------------

const ARTIFICIAL_BASE: i64 = 281474976710656;
const COMMENT_BASE: i64 = ARTIFICIAL_BASE + 4_000_000_000_000;
const REVIEW_COMMENT_BASE: i64 = ARTIFICIAL_BASE + 8_000_000_000_000;
const COMMIT_COMMENT_BASE: i64 = ARTIFICIAL_BASE + 12_000_000_000_000;
const REVIEW_BASE: i64 = ARTIFICIAL_BASE + 16_000_000_000_000;
const FORK_BASE: i64 = ARTIFICIAL_BASE + 20_000_000_000_000;
const RELEASE_BASE: i64 = ARTIFICIAL_BASE + 24_000_000_000_000;

fn issue_comments_path(repo: &str) -> String {
    format!("/repos/{repo}/issues/comments")
}
fn review_comments_path(repo: &str) -> String {
    format!("/repos/{repo}/pulls/comments")
}
fn commit_comments_path(repo: &str) -> String {
    format!("/repos/{repo}/comments")
}

/// Issue #1 and PR #2 rows, so the restored payloads can reference them.
fn issue_and_pr_seed() -> String {
    let mut s = seed_issue_row(7001, 1000, 1, false);
    s.push_str(&seed_pr_row(7102, 1000, 2));
    s
}

/// A pre-existing restored/archived comment row of the given kind.
fn seed_comment(id: i64, event_id: i64, e_type: &str) -> String {
    format!(
        "insert into gha_comments(id, event_id, body, created_at, updated_at, user_id, dup_actor_id, dup_actor_login, \
         dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) values({id}, {event_id}, 'old', \
         '2020-05-01 09:00:00', '2020-05-01 09:00:00', 12, 12, 'bob', {REPO_ID}, {}, {}, '2020-05-01 09:00:00', 'bob');",
        sql_str(REPO),
        sql_str(e_type)
    )
}

/// Empty `[]` answers for every restore endpoint of `repo` (the commit
/// comments probe included).
fn empty_restores(gh: &FakeGitHub, repo: &str) {
    gh.get_ok(&issue_comments_path(repo), &json!([]));
    gh.get_ok(&review_comments_path(repo), &json!([]));
    gh.get_ok(&commit_comments_path(repo), &json!([]));
    gh.get_ok(&format!("/repos/{repo}/pulls"), &json!([]));
    gh.get_ok(&format!("/repos/{repo}/forks"), &json!([]));
    gh.get_ok(&format!("/repos/{repo}/releases"), &json!([]));
}

#[test]
fn comments_restore_all_three_kinds() {
    let sides = check(
        Case::new("rc_all", Pass::Comments)
            .seed(&issue_and_pr_seed())
            .seed(&seed_comment(
                90002,
                COMMENT_BASE + 90002,
                "IssueCommentEvent",
            ))
            .setup(|gh| {
                let mut no_user = issue_comment_json(
                    90003,
                    1,
                    (14, "dave"),
                    "2020-05-01T12:00:00Z",
                    None,
                    "No user",
                );
                no_user["user"] = Value::Null;
                gh.get_ok(
                    &issue_comments_path(REPO),
                    &json!([
                        issue_comment_json(
                            90001,
                            1,
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            Some("2020-05-01T10:30:00Z"),
                            "First!"
                        ),
                        issue_comment_json(
                            90002,
                            1,
                            (12, "bob"),
                            "2020-05-01T11:00:00Z",
                            None,
                            "Already there"
                        ),
                        no_user,
                        issue_comment_json(
                            90004,
                            77,
                            (14, "dave"),
                            "2020-05-01T13:00:00Z",
                            None,
                            "Unknown issue"
                        ),
                    ]),
                );
                gh.get_ok(
                    &review_comments_path(REPO),
                    &json!([review_comment_json(
                        91001,
                        2,
                        (12, "bob"),
                        "2020-05-02T10:00:00Z",
                        "Nit",
                        Some(95001)
                    )]),
                );
                gh.get_ok(
                    &commit_comments_path(REPO),
                    &json!([
                        commit_comment_json(92001, (11, "alice"), OLD, "Ancient"),
                        commit_comment_json(92002, (11, "alice"), "2020-05-03T10:00:00Z", "LGTM"),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processing 1 repos, recent date: <recent>",
        );
        s.expect_line(
            0,
            "RestoreIssueComment: org/repo: skipping comment with missing id/user/created_at",
        );
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 3 pages, checked 6, restored 4",
        );
        s.expect_line(
            0,
            "targeted postprocess skipped: gha_texts is empty, full structure rebuild pending",
        );
        assert_eq!(
            s.query("select id, event_id, body, created_at, updated_at, user_id, dup_type, dup_actor_login, coalesce(commit_id, '-'), coalesce(position::text, '-'), coalesce(path, '-'), coalesce(pull_request_review_id::text, '-') from gha_comments order by id"),
            vec![
                vec!["90001".to_string(), (COMMENT_BASE + 90001).to_string(), "First!".to_string(), "2020-05-01T10:00:00Z".to_string(), "2020-05-01T10:30:00Z".to_string(), "11".to_string(), "IssueCommentEvent".to_string(), "alice".to_string(), "-".to_string(), "-".to_string(), "-".to_string(), "-".to_string()],
                vec!["90002".to_string(), (COMMENT_BASE + 90002).to_string(), "old".to_string(), "2020-05-01T09:00:00Z".to_string(), "2020-05-01T09:00:00Z".to_string(), "12".to_string(), "IssueCommentEvent".to_string(), "bob".to_string(), "-".to_string(), "-".to_string(), "-".to_string(), "-".to_string()],
                vec!["90004".to_string(), (COMMENT_BASE + 90004).to_string(), "Unknown issue".to_string(), "2020-05-01T13:00:00Z".to_string(), "2020-05-01T13:00:00Z".to_string(), "14".to_string(), "IssueCommentEvent".to_string(), "dave".to_string(), "-".to_string(), "-".to_string(), "-".to_string(), "-".to_string()],
                vec!["91001".to_string(), (REVIEW_COMMENT_BASE + 91001).to_string(), "Nit".to_string(), "2020-05-02T10:00:00Z".to_string(), "2020-05-02T10:00:00Z".to_string(), "12".to_string(), "PullRequestReviewCommentEvent".to_string(), "bob".to_string(), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(), "4".to_string(), "src/main.go".to_string(), "95001".to_string()],
                vec!["92002".to_string(), (COMMIT_COMMENT_BASE + 92002).to_string(), "LGTM".to_string(), "2020-05-03T10:00:00Z".to_string(), "2020-05-03T10:00:00Z".to_string(), "11".to_string(), "CommitCommentEvent".to_string(), "alice".to_string(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(), "7".to_string(), "README.md".to_string(), "-".to_string()],
            ]
        );
        assert_eq!(
            s.query("select id, type, actor_id, repo_id, org_id, created_at, dup_actor_login, dup_repo_name from gha_events where id > 1000 order by id"),
            vec![
                vec![(COMMENT_BASE + 90001).to_string(), "IssueCommentEvent".to_string(), "11".to_string(), "500".to_string(), "1".to_string(), "2020-05-01T10:00:00Z".to_string(), "alice".to_string(), REPO.to_string()],
                vec![(COMMENT_BASE + 90004).to_string(), "IssueCommentEvent".to_string(), "14".to_string(), "500".to_string(), "1".to_string(), "2020-05-01T13:00:00Z".to_string(), "dave".to_string(), REPO.to_string()],
                vec![(REVIEW_COMMENT_BASE + 91001).to_string(), "PullRequestReviewCommentEvent".to_string(), "12".to_string(), "500".to_string(), "1".to_string(), "2020-05-02T10:00:00Z".to_string(), "bob".to_string(), REPO.to_string()],
                vec![(COMMIT_COMMENT_BASE + 92002).to_string(), "CommitCommentEvent".to_string(), "11".to_string(), "500".to_string(), "1".to_string(), "2020-05-03T10:00:00Z".to_string(), "alice".to_string(), REPO.to_string()],
            ]
        );
        assert_eq!(
            s.query("select event_id, action, coalesce(number::text, '-'), coalesce(issue_id::text, '-'), coalesce(pull_request_id::text, '-'), comment_id, coalesce(commit, '-'), dup_type from gha_payloads order by event_id"),
            vec![
                vec![(COMMENT_BASE + 90001).to_string(), "created".to_string(), "1".to_string(), "7001".to_string(), "-".to_string(), "90001".to_string(), "-".to_string(), "IssueCommentEvent".to_string()],
                vec![(COMMENT_BASE + 90004).to_string(), "created".to_string(), "77".to_string(), "-".to_string(), "-".to_string(), "90004".to_string(), "-".to_string(), "IssueCommentEvent".to_string()],
                vec![(REVIEW_COMMENT_BASE + 91001).to_string(), "created".to_string(), "2".to_string(), "-".to_string(), "7102".to_string(), "91001".to_string(), "-".to_string(), "PullRequestReviewCommentEvent".to_string()],
                vec![(COMMIT_COMMENT_BASE + 92002).to_string(), "created".to_string(), "-".to_string(), "-".to_string(), "-".to_string(), "92002".to_string(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(), "CommitCommentEvent".to_string()],
            ]
        );
        // dave was unknown: added by ghActor
        assert_eq!(
            s.query("select login from gha_actors where id = 14"),
            vec![vec!["dave".to_string()]]
        );
        let reqs = s.requests();
        assert!(reqs.iter().any(|r| r.starts_with(&format!("GET {}?direction=asc&page=1&per_page=100&since=<recent>&sort=updated accept={SQUIRREL_ACCEPT} auth=tok1", issue_comments_path(REPO)))), "{reqs:#?}");
        assert!(reqs.iter().any(|r| r.starts_with(&format!("GET {}?direction=asc&page=1&per_page=100&since=<recent>&sort=updated accept={REVIEW_COMMENTS_ACCEPT} auth=tok1", review_comments_path(REPO)))), "{reqs:#?}");
        // the commit comments page 1 is fetched twice: the last-page probe and the walk
        assert_eq!(
            reqs.iter()
                .filter(|r| r.starts_with(&format!(
                    "GET {}?page=1&per_page=100 accept={SQUIRREL_ACCEPT}",
                    commit_comments_path(REPO)
                )))
                .count(),
            2,
            "{reqs:#?}"
        );
    });
}

#[test]
fn comments_restore_runs_the_targeted_postprocess() {
    let sides = check(
        Case::new("rc_pp", Pass::Comments)
            .util_sql()
            .seed(&issue_and_pr_seed())
            .seed("insert into gha_texts(event_id, body, created_at, repo_id, repo_name, actor_id, actor_login, type) values(1000, 'seed', '2020-02-01 10:00:00', 500, 'org/repo', 11, 'alice', 'IssuesEvent');")
            .setup(|gh| {
                gh.get_ok(
                    &issue_comments_path(REPO),
                    &json!([
                        issue_comment_json(90001, 1, (11, "alice"), "2020-05-01T10:00:00Z", None, "First!"),
                        issue_comment_json(90005, 1, (11, "alice"), "2020-05-01T10:05:00Z", None, ""),
                    ]),
                );
                gh.get_ok(&review_comments_path(REPO), &json!([]));
                gh.get_ok(&commit_comments_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 3 pages, checked 2, restored 2",
        );
        s.expect_line(
            0,
            "targeted postprocess executed for 2 restored event id(s)",
        );
        // the non-empty body made it into gha_texts (plus the seed row)
        assert_eq!(
            s.query("select event_id, body, type from gha_texts order by event_id"),
            vec![
                vec![
                    "1000".to_string(),
                    "seed".to_string(),
                    "IssuesEvent".to_string()
                ],
                vec![
                    (COMMENT_BASE + 90001).to_string(),
                    "First!".to_string(),
                    "IssueCommentEvent".to_string()
                ],
            ]
        );
    });
}

#[test]
fn comments_restore_reuses_raw_events() {
    let sides = check(
        Case::new("rc_raw", Pass::Comments)
            .seed(&issue_and_pr_seed())
            // a raw archived event for comment 90001 (verified via its payload)
            .seed(&seed_event(1500, "IssueCommentEvent", REPO, REPO_ID, (11, "alice"), "2020-05-01T10:00:00Z"))
            .seed("insert into gha_payloads(event_id, comment_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values(1500, 90001, 'alice', 500, 'org/repo', 'IssueCommentEvent', '2020-05-01 10:00:00');")
            // two candidates without payload values for comment 90006: cannot verify
            .seed(&seed_event(1501, "IssueCommentEvent", REPO, REPO_ID, (12, "bob"), "2020-05-01T11:00:00Z"))
            .seed(&seed_event(1502, "IssueCommentEvent", REPO, REPO_ID, (12, "bob"), "2020-05-01T11:00:00Z"))
            // one candidate with a different payload comment id for 90007: not reused
            .seed(&seed_event(1503, "IssueCommentEvent", REPO, REPO_ID, (11, "alice"), "2020-05-01T12:00:00Z"))
            .seed("insert into gha_payloads(event_id, comment_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values(1503, 1, 'alice', 500, 'org/repo', 'IssueCommentEvent', '2020-05-01 12:00:00');")
            .setup(|gh| {
                gh.get_ok(
                    &issue_comments_path(REPO),
                    &json!([
                        issue_comment_json(90001, 1, (11, "alice"), "2020-05-01T10:00:00Z", None, "First!"),
                        issue_comment_json(90006, 1, (12, "bob"), "2020-05-01T11:00:00Z", None, "Second"),
                        issue_comment_json(90007, 1, (11, "alice"), "2020-05-01T12:00:00Z", None, "Third"),
                    ]),
                );
                gh.get_ok(&review_comments_path(REPO), &json!([]));
                gh.get_ok(&commit_comments_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "findRawEventID: cannot verify raw event for (IssueCommentEvent, org/repo, 12, 2020-05-01 11:00:00 +0000 UTC, comment_id=90006), creating artificial event");
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 3 pages, checked 3, restored 3",
        );
        assert_eq!(
            s.query("select id, event_id from gha_comments order by id"),
            vec![
                vec!["90001".to_string(), "1500".to_string()],
                vec!["90006".to_string(), (COMMENT_BASE + 90006).to_string()],
                vec!["90007".to_string(), (COMMENT_BASE + 90007).to_string()],
            ]
        );
        // the reused event's payload is enriched (coalesce), not duplicated
        assert_eq!(
            s.query("select event_id, coalesce(number::text, '-'), coalesce(issue_id::text, '-'), comment_id from gha_payloads where event_id = 1500"),
            vec![vec!["1500".to_string(), "1".to_string(), "7001".to_string(), "90001".to_string()]]
        );
        assert_eq!(
            s.count("select count(*) from gha_events where id = 1500"),
            1
        );
    });
}

#[test]
fn comments_restore_walks_commit_comment_pages_backwards() {
    let base = format!("{}?per_page=100", commit_comments_path(REPO));
    let sides = check(Case::new("rc_walk", Pass::Comments).setup(move |gh| {
        gh.get_ok(&issue_comments_path(REPO), &json!([]));
        gh.get_ok(&review_comments_path(REPO), &json!([]));
        gh.get(
            &format!("{}?page=1", commit_comments_path(REPO)),
            vec![Scripted::ok(&json!([commit_comment_json(
                92001,
                (11, "alice"),
                OLD,
                "Ancient"
            )]))
            .paged(&base, 1, 3)],
        );
        gh.get(
            &format!("{}?page=2", commit_comments_path(REPO)),
            vec![Scripted::ok(&json!([
                commit_comment_json(92002, (11, "alice"), OLD, "Old"),
                commit_comment_json(92003, (12, "bob"), "2020-05-01T10:00:00Z", "Recent enough"),
            ]))
            .paged(&base, 2, 3)],
        );
        gh.get(
            &format!("{}?page=3", commit_comments_path(REPO)),
            vec![Scripted::ok(&json!([
                commit_comment_json(92004, (12, "bob"), "2020-05-02T10:00:00Z", "New"),
                commit_comment_json(92005, (12, "bob"), "2020-05-03T10:00:00Z", "Newer"),
            ]))
            .paged(&base, 3, 3)],
        );
    }));
    both(&sides, |s| {
        // pages: issue 1 + review 1 + commit comments 3, 2, 1 (page 1 has nothing recent → stop)
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 5 pages, checked 3, restored 3",
        );
        let reqs = s.requests();
        let commit_pages: Vec<&String> = reqs
            .iter()
            .filter(|r| r.contains("/repos/org/repo/comments?"))
            .collect();
        // probe page 1, then 3, 2, 1
        assert_eq!(commit_pages.len(), 4, "{commit_pages:#?}");
        assert_eq!(
            s.column("select id::text from gha_comments order by id"),
            vec![
                "92003".to_string(),
                "92004".to_string(),
                "92005".to_string()
            ]
        );
    });
}

#[test]
fn comments_restore_pages_and_stops_without_next() {
    let sides = check(
        Case::new("rc_pages", Pass::Comments)
            .seed(&issue_and_pr_seed())
            .setup(|gh| {
                let base = format!(
                    "{}?direction=asc&per_page=100&sort=updated",
                    issue_comments_path(REPO)
                );
                gh.get(
                    &format!("{}?page=1", issue_comments_path(REPO)),
                    vec![Scripted::ok(&json!([issue_comment_json(
                        90001,
                        1,
                        (11, "alice"),
                        "2020-05-01T10:00:00Z",
                        None,
                        "p1"
                    )]))
                    .paged(&base, 1, 2)],
                );
                gh.get(
                    &format!("{}?page=2", issue_comments_path(REPO)),
                    vec![Scripted::ok(&json!([issue_comment_json(
                        90002,
                        1,
                        (11, "alice"),
                        "2020-05-01T11:00:00Z",
                        None,
                        "p2"
                    )]))
                    .paged(&base, 2, 2)],
                );
                gh.get_ok(&review_comments_path(REPO), &json!([]));
                gh.get_ok(&commit_comments_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 4 pages, checked 2, restored 2",
        );
        assert!(
            s.requests()
                .iter()
                .any(|r| r.contains("/issues/comments?direction=asc&page=2&per_page=100")),
            "{:?}",
            s.requests()
        );
    });
}

#[test]
fn restore_skips_repos_without_id_or_malformed_names() {
    let sides = check(
        // Go builds the repo list from a map: per-repo lines come in random
        // order even single-threaded → multiset comparison.
        Case::new("rc_badrepos", Pass::Comments)
            .unordered()
            .seed(&seed_event(
                1001,
                "PushEvent",
                "noslash",
                701,
                (11, "alice"),
                "2020-02-02T10:00:00Z",
            ))
            .seed(&seed_event(
                1002,
                "PushEvent",
                "org/zero",
                0,
                (11, "alice"),
                "2020-02-02T10:00:00Z",
            ))
            .setup(|gh| {
                empty_restores(gh, REPO);
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processing 3 repos, recent date: <recent>",
        );
        s.expect_line(
            0,
            "WARNING: ghapi2db comments restore: malformed repo name: 'noslash'",
        );
        s.expect_line(
            0,
            "ghapi2db comments restore: org/zero: no existing repo_id, skipping restore",
        );
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 3 repos, 3 pages, checked 0, restored 0",
        );
        assert!(!s
            .requests()
            .iter()
            .any(|r| r.contains("org/zero") || r.contains("noslash")));
    });
}

#[test]
fn restore_api_error_paths() {
    let mut case = Case::new("rc_errors", Pass::Comments)
        .unordered()
        .loose("org/gone issue comments: error: ");
    for (i, r) in ["r404", "r410", "r500", "abuse", "abuse2", "gone"]
        .iter()
        .enumerate()
    {
        case = case.seed(&seed_event(
            1100 + i as i64,
            "PushEvent",
            &format!("org/{r}"),
            700 + i as i64,
            (11, "alice"),
            "2020-02-02T10:00:00Z",
        ));
    }
    let sides = check(case.setup(|gh| {
        empty_restores(gh, REPO);
        for r in ["r404", "r410", "r500", "abuse", "abuse2", "gone"] {
            let repo = format!("org/{r}");
            gh.get_ok(&review_comments_path(&repo), &json!([]));
            gh.get_ok(&commit_comments_path(&repo), &json!([]));
        }
        gh.get(
            &issue_comments_path("org/r404"),
            vec![Scripted::not_found()],
        );
        gh.get(
            &issue_comments_path("org/r410"),
            vec![Scripted::error(410, "This issue was deleted")],
        );
        gh.get(
            &issue_comments_path("org/r500"),
            vec![Scripted::error(500, "Internal Server Error")],
        );
        gh.get(
            &issue_comments_path("org/abuse"),
            vec![
                Scripted::abuse(Some(1)),
                Scripted::ok(&json!([issue_comment_json(
                    90001,
                    1,
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    None,
                    "after abuse"
                )])),
            ],
        );
        gh.get(
            &issue_comments_path("org/abuse2"),
            vec![Scripted::abuse(Some(3600))],
        );
        gh.get(&issue_comments_path("org/gone"), vec![Scripted::hangup()]);
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processing 7 repos, recent date: <recent>",
        );
        s.expect_line(0, "org/r500 issue comments: status 500, skipping");
        s.expect_line(
            0,
            "org/abuse issue comments: abuse detected, waiting <dur>, retry 1/2",
        );
        s.expect_line(
            0,
            "org/abuse2 issue comments: abuse detected, don't want to wait <dur>, skipping",
        );
        s.expect_prefix(0, "org/gone issue comments: error: ");
        // 404/410 are silent
        s.expect_no_prefix(0, "org/r404 ");
        s.expect_no_prefix(0, "org/r410 ");
        // the retried repo's comment got restored after the abuse wait
        assert_eq!(
            s.column("select dup_repo_name from gha_comments"),
            vec!["org/abuse".to_string()]
        );
        // 7 repos × (review + commit comments pages) + the two issue comments pages that worked
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 7 repos, 16 pages, checked 1, restored 1",
        );
    });
}

#[test]
fn restore_rate_limited_response_skips_the_rest() {
    // once a 403 with X-RateLimit-Remaining: 0 arrives, the client refuses
    // further calls until the reset: every later page is "rate limited"
    let sides = check(
        Case::new("rc_rl", Pass::Comments)
            .seed_only(&seed_event(
                1000,
                "PushEvent",
                REPO,
                REPO_ID,
                (11, "alice"),
                "2020-02-01T10:00:00Z",
            ))
            .setup(|gh| {
                gh.get(
                    &issue_comments_path(REPO),
                    vec![Scripted::rate_limited(5000, 3600)],
                );
                gh.get_ok(&review_comments_path(REPO), &json!([]));
                gh.get_ok(&commit_comments_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "org/repo issue comments: rate limited, reset in <dur>, skipping",
        );
        s.expect_line(
            0,
            "org/repo review comments: rate limited, reset in <dur>, skipping",
        );
        s.expect_line(
            0,
            "org/repo commit comments last page: rate limited, reset in <dur>, skipping",
        );
        s.expect_line(
            0,
            "org/repo commit comments: rate limited, reset in <dur>, skipping",
        );
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 0 pages, checked 0, restored 0",
        );
        // only the first call reached the API
        assert_eq!(
            s.requests()
                .iter()
                .filter(|r| r.contains("/repos/"))
                .count(),
            1,
            "{:?}",
            s.requests()
        );
    });
}

#[test]
fn restore_plain_403_gives_up_after_retries() {
    let sides = check(
        Case::new("rc_403", Pass::Comments)
            .env("GHA2DB_MAX_GHAPI_RETRY", "1")
            .setup(|gh| {
                gh.get(
                    &issue_comments_path(REPO),
                    vec![Scripted::error(403, "Forbidden")],
                );
                gh.get_ok(&review_comments_path(REPO), &json!([]));
                gh.get_ok(&commit_comments_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, "org/repo issue comments: abuse detected, retry 1/1");
        s.expect_line(0, "org/repo issue comments: giving up after 1 retries");
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 2 pages, checked 0, restored 0",
        );
    });
}

#[test]
fn restore_fatal_errors_when_asked() {
    let sides = check(
        Case::new("rc_fatal", Pass::Comments)
            .env("GHA2DB_GHAPI_ERROR_FATAL", "1")
            .setup(|gh| {
                gh.get(
                    &issue_comments_path(REPO),
                    vec![Scripted::rate_limited(5000, 3600)],
                );
            }),
    );
    both(&sides, |s| {
        assert_ne!(s.code(0), Some(0));
        assert_eq!(s.errors(0).len(), 1, "{:?}", s.errors(0));
        assert!(
            s.errors(0)[0].contains("org/repo issue comments: rate limited, don't want to wait "),
            "{:?}",
            s.errors(0)
        );
    });
}

#[test]
fn restore_skip_pdb_restores_nothing() {
    let sides = check(
        Case::new("rc_skippdb", Pass::Comments)
            .env("GHA2DB_SKIPPDB", "1")
            .setup(|gh| {
                gh.get_ok(
                    &issue_comments_path(REPO),
                    &json!([issue_comment_json(
                        90001,
                        1,
                        (11, "alice"),
                        "2020-05-01T10:00:00Z",
                        None,
                        "First!"
                    )]),
                );
                gh.get_ok(&review_comments_path(REPO), &json!([]));
                gh.get_ok(&commit_comments_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 3 pages, checked 1, restored 0",
        );
        assert_eq!(s.count("select count(*) from gha_comments"), 0);
    });
}

#[test]
fn restore_hidden_actors() {
    let sides = check(
        Case::new("rc_hide", Pass::Comments)
            .hide(&format!("sha1\n{ALICE_SHA1}\n"))
            .seed_only(&seed_event(
                1000,
                "IssuesEvent",
                REPO,
                REPO_ID,
                (12, "bob"),
                "2020-02-01T10:00:00Z",
            ))
            .setup(|gh| {
                gh.get_ok(
                    &issue_comments_path(REPO),
                    &json!([issue_comment_json(
                        90001,
                        1,
                        (11, "alice"),
                        "2020-05-01T10:00:00Z",
                        None,
                        "by alice"
                    )]),
                );
                gh.get_ok(&review_comments_path(REPO), &json!([]));
                gh.get_ok(&commit_comments_path(REPO), &json!([]));
            }),
    );
    both(&sides, |s| {
        let anon = format!("anon-{ALICE_SHA1}");
        assert_eq!(
            s.query("select dup_actor_login, dup_user_login from gha_comments"),
            vec![vec![anon.clone(), anon.clone()]]
        );
        assert_eq!(
            s.column("select dup_actor_login from gha_events where id > 1000"),
            vec![anon.clone()]
        );
        assert_eq!(
            s.column("select dup_actor_login from gha_payloads"),
            vec![anon.clone()]
        );
        assert_eq!(
            s.query("select login from gha_actors where id = 11"),
            vec![vec![anon]]
        );
    });
}

#[test]
fn restore_pass_refreshes_the_rate_every_20_repos() {
    let mk = |name: &'static str, reset_in: i64| {
        let mut case = Case::new(name, Pass::Comments);
        for i in 0..19 {
            case = case.seed(&seed_event(
                1100 + i,
                "PushEvent",
                &format!("org/r{i:02}"),
                700 + i,
                (11, "alice"),
                "2020-02-02T10:00:00Z",
            ));
        }
        case.setup(move |gh| {
            gh.get(
                "/rate_limit",
                vec![
                    Scripted::ok(&rate_json(5000, 4000, 3600)),
                    Scripted::ok(&rate_json(5000, 0, reset_in)),
                ],
            );
            empty_restores(gh, REPO);
            for i in 0..19 {
                empty_restores(gh, &format!("org/r{i:02}"));
            }
        })
    };
    let sides = check(mk("rc_rate_abort", 3600));
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processing 20 repos, recent date: <recent>",
        );
        s.expect_line(
            0,
            "ghapi2db comments restore: API limit reached, don't want to wait <dur>",
        );
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 20 repos, 60 pages, checked 0, restored 0",
        );
        assert_eq!(
            s.requests()
                .iter()
                .filter(|r| r.starts_with("GET /rate_limit "))
                .count(),
            3
        );
    });
    let sides = check(mk("rc_rate_wait", 1));
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: API limit reached, waiting <dur>",
        );
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 20 repos, 60 pages, checked 0, restored 0",
        );
    });
}

#[test]
fn restore_multi_threaded() {
    let mut case = Case::new("rc_mt", Pass::Comments).mt();
    for i in 0..10 {
        case = case.seed(&seed_event(
            1100 + i,
            "PushEvent",
            &format!("org/r{i}"),
            700 + i,
            (11, "alice"),
            "2020-02-02T10:00:00Z",
        ));
    }
    let sides = check(case.setup(|gh| {
        empty_restores(gh, REPO);
        for i in 0..10 {
            let repo = format!("org/r{i}");
            gh.get_ok(
                &issue_comments_path(&repo),
                &json!([issue_comment_json(
                    90000 + i,
                    1,
                    (11, "alice"),
                    "2020-05-01T10:00:00Z",
                    None,
                    "hello"
                )]),
            );
            gh.get_ok(&review_comments_path(&repo), &json!([]));
            gh.get_ok(&commit_comments_path(&repo), &json!([]));
        }
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 11 repos, 33 pages, checked 10, restored 10",
        );
        assert_eq!(s.count("select count(*) from gha_comments"), 10);
    });
}

#[test]
fn reviews_restore() {
    let sides = check(
        Case::new("rr_basic", Pass::Reviews)
            .seed(&issue_and_pr_seed())
            .seed("insert into gha_reviews(id, user_id, commit_id, submitted_at, author_association, state, body, event_id, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) values(95002, 12, 'bbbb', '2020-05-04 10:00:00', 'MEMBER', 'COMMENTED', 'x', 1000, 12, 'bob', 500, 'org/repo', 'PullRequestReviewEvent', '2020-05-04 10:00:00', 'bob');")
            // two raw candidates for review 95001 → ambiguous
            .seed(&seed_event(1501, "PullRequestReviewEvent", REPO, REPO_ID, (11, "alice"), "2020-05-05T10:00:00Z"))
            .seed(&seed_event(1502, "PullRequestReviewEvent", REPO, REPO_ID, (11, "alice"), "2020-05-05T10:00:00Z"))
            .setup(|gh| {
                let mut pending = review_json(95003, (12, "bob"), "2020-05-04T11:00:00Z", "PENDING", None);
                pending["submitted_at"] = Value::Null;
                gh.get_ok(
                    "/repos/org/repo/pulls",
                    &json!([
                        pr_list_json(2, "2020-05-05T12:00:00Z"),
                        pr_list_json(3, "2020-05-04T12:00:00Z"),
                        pr_list_json(4, OLD),
                        pr_list_json(5, "2020-05-03T12:00:00Z"),
                    ]),
                );
                gh.get_ok(
                    "/repos/org/repo/pulls/2/reviews",
                    &json!([
                        review_json(95001, (11, "alice"), "2020-05-05T10:00:00Z", "APPROVED", Some("Ship it")),
                        review_json(95002, (12, "bob"), "2020-05-04T10:00:00Z", "COMMENTED", Some("x")),
                        pending,
                    ]),
                );
                gh.get_ok(
                    "/repos/org/repo/pulls/3/reviews",
                    &json!([review_json(95004, (14, "dave"), "2020-05-04T09:00:00Z", "CHANGES_REQUESTED", None)]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db reviews restore: processing 1 repos, recent date: <recent>",
        );
        s.expect_line(0, "findRawEventID: ambiguous raw events for (PullRequestReviewEvent, org/repo, 11, 2020-05-05 10:00:00 +0000 UTC), creating artificial event");
        // PR 4 is older than the recent date: the list stops there (PR 5 is never looked at)
        s.expect_line(
            0,
            "ghapi2db reviews restore: processed 1 repos, 3 pages, checked 3, restored 2",
        );
        s.expect_line(
            0,
            "targeted postprocess skipped: gha_texts is empty, full structure rebuild pending",
        );
        assert!(!s.requests().iter().any(|r| r.contains("/pulls/5/")));
        assert!(s.requests().iter().any(|r| r.starts_with("GET /repos/org/repo/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all ")), "{:?}", s.requests());
        assert_eq!(
            s.query("select id, user_id, commit_id, submitted_at, author_association, state, coalesce(body, '-'), event_id, dup_actor_login from gha_reviews order by id"),
            vec![
                vec!["95001".to_string(), "11".to_string(), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(), "2020-05-05T10:00:00Z".to_string(), "COLLABORATOR".to_string(), "APPROVED".to_string(), "Ship it".to_string(), (REVIEW_BASE + 95001).to_string(), "alice".to_string()],
                vec!["95002".to_string(), "12".to_string(), "bbbb".to_string(), "2020-05-04T10:00:00Z".to_string(), "MEMBER".to_string(), "COMMENTED".to_string(), "x".to_string(), "1000".to_string(), "bob".to_string()],
                vec!["95004".to_string(), "14".to_string(), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(), "2020-05-04T09:00:00Z".to_string(), "COLLABORATOR".to_string(), "CHANGES_REQUESTED".to_string(), "-".to_string(), (REVIEW_BASE + 95004).to_string(), "dave".to_string()],
            ]
        );
        assert_eq!(
            s.query("select event_id, action, number, coalesce(pull_request_id::text, '-'), dup_type from gha_payloads order by event_id"),
            vec![
                vec![(REVIEW_BASE + 95001).to_string(), "created".to_string(), "2".to_string(), "7102".to_string(), "PullRequestReviewEvent".to_string()],
                vec![(REVIEW_BASE + 95004).to_string(), "created".to_string(), "3".to_string(), "-".to_string(), "PullRequestReviewEvent".to_string()],
            ]
        );
    });
}

#[test]
fn reviews_restore_paging() {
    let prs_base =
        "/repos/org/repo/pulls?direction=desc&per_page=100&sort=updated&state=all".to_string();
    let rev_base = "/repos/org/repo/pulls/2/reviews?per_page=100".to_string();
    let sides = check(Case::new("rr_pages", Pass::Reviews).setup(move |gh| {
        gh.get(
            "/repos/org/repo/pulls?page=1",
            vec![
                Scripted::ok(&json!([pr_list_json(2, "2020-05-05T12:00:00Z")]))
                    .paged(&prs_base, 1, 2),
            ],
        );
        gh.get(
            "/repos/org/repo/pulls?page=2",
            vec![
                Scripted::ok(&json!([pr_list_json(3, "2020-05-04T12:00:00Z")]))
                    .paged(&prs_base, 2, 2),
            ],
        );
        gh.get(
            "/repos/org/repo/pulls/2/reviews?page=1",
            vec![Scripted::ok(&json!([review_json(
                95001,
                (11, "alice"),
                "2020-05-05T10:00:00Z",
                "APPROVED",
                None
            )]))
            .paged(&rev_base, 1, 2)],
        );
        gh.get(
            "/repos/org/repo/pulls/2/reviews?page=2",
            vec![Scripted::ok(&json!([review_json(
                95005,
                (12, "bob"),
                "2020-05-05T11:00:00Z",
                "APPROVED",
                None
            )]))
            .paged(&rev_base, 2, 2)],
        );
        gh.get_ok("/repos/org/repo/pulls/3/reviews", &json!([]));
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db reviews restore: processed 1 repos, 5 pages, checked 2, restored 2",
        );
        assert!(
            s.requests()
                .iter()
                .any(|r| r.starts_with("GET /repos/org/repo/pulls/2/reviews?page=2&per_page=100 ")),
            "{:?}",
            s.requests()
        );
    });
}

#[test]
fn forks_restore() {
    let sides = check(
        Case::new("rf_basic", Pass::Forks)
            // fork 8002 already recorded (forkee + ForkEvent payload)
            .seed(&seed_event(1600, "ForkEvent", REPO, REPO_ID, (12, "bob"), "2020-05-02T09:00:00Z"))
            .seed("insert into gha_forkees(id, event_id, name, full_name, owner_id, updated_at, stargazers_count, forks, open_issues, watchers, dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at) values(8002, 1600, 'repo', 'bob/repo', 12, '2020-05-02 09:00:00', 0, 0, 0, 0, 12, 500, 'org/repo', '2020-05-02 09:00:00');")
            .seed("insert into gha_payloads(event_id, forkee_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values(1600, 8002, 'bob', 500, 'org/repo', 'ForkEvent', '2020-05-02 09:00:00');")
            .setup(|gh| {
                gh.get_ok(
                    "/repos/org/repo/forks",
                    &json!([
                        fork_json(8001, (14, "dave"), "2020-05-03T10:00:00Z", 5),
                        fork_json(8002, (12, "bob"), "2020-05-02T09:00:00Z", 0),
                        fork_json(8003, (15, "erin"), OLD, 1),
                        fork_json(8004, (15, "erin"), "2020-05-01T09:00:00Z", 1),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db forks restore: processing 1 repos, recent date: <recent>",
        );
        s.expect_line(
            0,
            "ghapi2db forks restore: processed 1 repos, 1 pages, checked 2, restored 1",
        );
        s.expect_no_prefix(0, "targeted postprocess ");
        assert!(s.requests().iter().any(|r| r.starts_with(&format!("GET /repos/org/repo/forks?page=1&per_page=100&sort=newest accept={MERCY_ACCEPT} auth=tok1"))), "{:?}", s.requests());
        assert_eq!(
            s.query("select id, event_id, name, full_name, owner_id, updated_at, stargazers_count, forks, open_issues, watchers, dup_actor_id, dup_created_at from gha_forkees order by id"),
            vec![
                vec!["8001".to_string(), (FORK_BASE + 8001).to_string(), "repo".to_string(), "dave/repo".to_string(), "14".to_string(), "2020-05-02T10:00:00Z".to_string(), "5".to_string(), "2".to_string(), "1".to_string(), "5".to_string(), "14".to_string(), "2020-05-03T10:00:00Z".to_string()],
                vec!["8002".to_string(), "1600".to_string(), "repo".to_string(), "bob/repo".to_string(), "12".to_string(), "2020-05-02T09:00:00Z".to_string(), "0".to_string(), "0".to_string(), "0".to_string(), "0".to_string(), "12".to_string(), "2020-05-02T09:00:00Z".to_string()],
            ]
        );
        assert_eq!(
            s.query("select event_id, coalesce(action, '-'), forkee_id, dup_type from gha_payloads where event_id > 1600"),
            vec![vec![(FORK_BASE + 8001).to_string(), "-".to_string(), "8001".to_string(), "ForkEvent".to_string()]]
        );
        assert_eq!(
            s.query("select login from gha_actors where id = 14"),
            vec![vec!["dave".to_string()]]
        );
    });
}

#[test]
fn forks_restore_paging() {
    let base = "/repos/org/repo/forks?per_page=100&sort=newest".to_string();
    let sides = check(Case::new("rf_pages", Pass::Forks).setup(move |gh| {
        gh.get(
            "/repos/org/repo/forks?page=1",
            vec![Scripted::ok(&json!([fork_json(
                8001,
                (14, "dave"),
                "2020-05-03T10:00:00Z",
                5
            )]))
            .paged(&base, 1, 2)],
        );
        gh.get(
            "/repos/org/repo/forks?page=2",
            vec![Scripted::ok(&json!([fork_json(
                8005,
                (15, "erin"),
                "2020-05-02T10:00:00Z",
                0
            )]))
            .paged(&base, 2, 2)],
        );
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db forks restore: processed 1 repos, 2 pages, checked 2, restored 2",
        );
        assert_eq!(s.count("select count(*) from gha_forkees"), 2);
    });
}

#[test]
fn releases_restore() {
    let sides = check(
        Case::new("rl_basic", Pass::Releases)
            .seed("insert into gha_releases(id, event_id, tag_name, target_commitish, name, draft, author_id, prerelease, created_at, published_at, body, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_author_login) values(9003, 1000, 'v0.9', 'main', 'old', false, 12, false, '2020-05-10 10:00:00', '2020-05-10 10:00:00', null, 12, 'bob', 500, 'org/repo', 'ReleaseEvent', '2020-05-10 10:00:00', 'bob');")
            .setup(|gh| {
                gh.get_ok(
                    "/repos/org/repo/releases",
                    &json!([
                        release_json(
                            9001,
                            "v1.0",
                            "2020-06-01T09:00:00Z",
                            Some("2020-06-01T10:00:00Z"),
                            vec![
                                asset_json(9101, "tool-linux.tar.gz", Some((14, "dave"))),
                                asset_json(9102, "tool-darwin.tar.gz", None),
                            ],
                        ),
                        release_json(9002, "v1.0-rc1", "2020-05-20T10:00:00Z", None, vec![]),
                        release_json(9003, "v0.9", "2020-05-10T10:00:00Z", Some("2020-05-10T10:00:00Z"), vec![]),
                        release_json(9004, "v0.1", OLD, Some(OLD), vec![]),
                        release_json(9005, "v0.5", "2020-05-01T10:00:00Z", Some("2020-05-01T10:00:00Z"), vec![]),
                    ]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db releases restore: processing 1 repos, recent date: <recent>",
        );
        s.expect_line(
            0,
            "ghapi2db releases restore: processed 1 repos, 1 pages, checked 3, restored 2",
        );
        assert_eq!(
            s.query("select id, event_id, tag_name, target_commitish, coalesce(name, '-'), draft, author_id, prerelease, created_at, coalesce(published_at::text, '-'), coalesce(body, '-'), dup_actor_login, dup_created_at from gha_releases order by id"),
            vec![
                vec!["9001".to_string(), (RELEASE_BASE + 9001).to_string(), "v1.0".to_string(), "main".to_string(), "Release v1.0".to_string(), "false".to_string(), "12".to_string(), "false".to_string(), "2020-06-01T10:00:00Z".to_string(), "2020-06-01 10:00:00".to_string(), "Notes for v1.0".to_string(), "bob".to_string(), "2020-06-01T10:00:00Z".to_string()],
                vec!["9002".to_string(), (RELEASE_BASE + 9002).to_string(), "v1.0-rc1".to_string(), "main".to_string(), "Release v1.0-rc1".to_string(), "false".to_string(), "12".to_string(), "true".to_string(), "2020-05-20T10:00:00Z".to_string(), "-".to_string(), "Notes for v1.0-rc1".to_string(), "bob".to_string(), "2020-05-20T10:00:00Z".to_string()],
                vec!["9003".to_string(), "1000".to_string(), "v0.9".to_string(), "main".to_string(), "old".to_string(), "false".to_string(), "12".to_string(), "false".to_string(), "2020-05-10T10:00:00Z".to_string(), "2020-05-10 10:00:00".to_string(), "-".to_string(), "bob".to_string(), "2020-05-10T10:00:00Z".to_string()],
            ]
        );
        assert_eq!(
            s.query("select id, event_id, name, coalesce(label, '-'), uploader_id, content_type, state, size, download_count, created_at, updated_at, dup_actor_login, dup_uploader_login from gha_assets order by id"),
            vec![
                vec!["9101".to_string(), (RELEASE_BASE + 9001).to_string(), "tool-linux.tar.gz".to_string(), "".to_string(), "14".to_string(), "application/gzip".to_string(), "uploaded".to_string(), "12345".to_string(), "7".to_string(), "2020-06-01T10:05:00Z".to_string(), "2020-06-01T10:06:00Z".to_string(), "bob".to_string(), "dave".to_string()],
                vec!["9102".to_string(), (RELEASE_BASE + 9001).to_string(), "tool-darwin.tar.gz".to_string(), "".to_string(), "12".to_string(), "application/gzip".to_string(), "uploaded".to_string(), "12345".to_string(), "7".to_string(), "2020-06-01T10:05:00Z".to_string(), "2020-06-01T10:06:00Z".to_string(), "bob".to_string(), "bob".to_string()],
            ]
        );
        assert_eq!(
            s.query(
                "select release_id, event_id, asset_id from gha_releases_assets order by asset_id"
            ),
            vec![
                vec![
                    "9001".to_string(),
                    (RELEASE_BASE + 9001).to_string(),
                    "9101".to_string()
                ],
                vec![
                    "9001".to_string(),
                    (RELEASE_BASE + 9001).to_string(),
                    "9102".to_string()
                ],
            ]
        );
        assert_eq!(
            s.query("select event_id, action, release_id, dup_type, dup_created_at from gha_payloads order by event_id"),
            vec![
                vec![(RELEASE_BASE + 9001).to_string(), "published".to_string(), "9001".to_string(), "ReleaseEvent".to_string(), "2020-06-01T10:00:00Z".to_string()],
                vec![(RELEASE_BASE + 9002).to_string(), "published".to_string(), "9002".to_string(), "ReleaseEvent".to_string(), "2020-05-20T10:00:00Z".to_string()],
            ]
        );
    });
}

fn gql_route(gh: &FakeGitHub, pages: Vec<Scripted>) {
    gh.route("POST", "/graphql", pages);
}

#[test]
fn stars_restore_over_graphql() {
    let sides = check(
        Case::new("rs_basic", Pass::Stars)
            .seed(&seed_event(
                1700,
                "WatchEvent",
                REPO,
                REPO_ID,
                (11, "alice"),
                "2020-05-01T10:00:00Z",
            ))
            .setup(|gh| {
                gql_route(
                    gh,
                    vec![
                        Scripted::ok(&gql_page(
                            &[
                                ("2020-05-01T10:00:00Z", "alice", 11),
                                ("2020-05-02T10:00:00Z", "dave", 14),
                                ("2020-05-03T10:00:00Z", "", 16),
                                ("2020-05-03T10:00:00Z", "zero", 0),
                            ],
                            true,
                            "cursor-1",
                        )),
                        Scripted::ok(&gql_page(&[(OLD, "old", 15)], true, "cursor-2")),
                    ],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db stars restore: processing 1 repos, recent date: <recent>",
        );
        // page 2 has nothing recent: stop even though hasPreviousPage
        s.expect_line(
            0,
            "ghapi2db stars restore: processed 1 repos, 2 pages, checked 2, restored 1",
        );
        s.expect_no_prefix(0, "targeted postprocess ");
        let expected_id = devstatscode::restore::negative_artificial_id(&[
            "WatchEvent",
            "14",
            REPO,
            "2020-05-02 10:00:00",
        ]);
        assert_eq!(
            s.query("select id, type, actor_id, repo_id, org_id, created_at, dup_actor_login from gha_events where id < 0"),
            vec![vec![expected_id.to_string(), "WatchEvent".to_string(), "14".to_string(), "500".to_string(), "1".to_string(), "2020-05-02T10:00:00Z".to_string(), "dave".to_string()]]
        );
        assert_eq!(
            s.query("select event_id, action, dup_type from gha_payloads"),
            vec![vec![
                expected_id.to_string(),
                "started".to_string(),
                "WatchEvent".to_string()
            ]]
        );
        let bodies = s.graphql_bodies();
        assert_eq!(bodies.len(), 2);
        assert_eq!(
            bodies[0],
            r#"{"query":"query($o: String!, $r: String!, $b: String) { repository(owner: $o, name: $r) { stargazers(last: 100, before: $b, orderBy: {field: STARRED_AT, direction: ASC}) { pageInfo { hasPreviousPage startCursor } edges { starredAt node { login databaseId } } } } }","variables":{"o":"org","r":"repo"}}"#
        );
        assert!(
            bodies[1].ends_with(r#""variables":{"b":"cursor-1","o":"org","r":"repo"}}"#),
            "{}",
            bodies[1]
        );
        assert!(
            s.requests()
                .iter()
                .all(|r| !r.starts_with("POST") || r.contains(" auth=tok1 ")),
            "{:?}",
            s.requests()
        );
    });
}

#[test]
fn stars_restore_needs_a_token() {
    let sides = check(
        Case::new("rs_notoken", Pass::Stars)
            .oauth(Some("-"))
            .setup(|_| {}),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "org/repo: stars restore needs GHA2DB_GITHUB_OAUTH token(s), skipping",
        );
        s.expect_line(
            0,
            "ghapi2db stars restore: processed 1 repos, 0 pages, checked 0, restored 0",
        );
        assert!(!s.requests().iter().any(|r| r.starts_with("POST")));
    });
}

#[test]
fn stars_restore_graphql_errors() {
    let sides = check(Case::new("rs_500", Pass::Stars).setup(|gh| {
        gql_route(gh, vec![Scripted::raw(500, "text/plain", "boom")]);
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "org/repo: stargazers graphql: graphql status 500 (token 1/1): boom, skipping",
        );
        s.expect_line(
            0,
            "ghapi2db stars restore: processed 1 repos, 0 pages, checked 0, restored 0",
        );
    });
    let sides = check(Case::new("rs_gqlerr", Pass::Stars).setup(|gh| {
        gql_route(
            gh,
            vec![Scripted::ok(
                &json!({"errors": [{"message": "Could not resolve to a Repository"}]}),
            )],
        );
    }));
    both(&sides, |s| {
        s.expect_line(0, "org/repo: stargazers graphql: graphql (token 1/1): Could not resolve to a Repository, skipping");
    });
    let sides = check(Case::new("rs_ratelimit", Pass::Stars).setup(|gh| {
        gql_route(
            gh,
            vec![
                Scripted::raw(429, "application/json", r#"{"message":"slow down"}"#)
                    .header("Retry-After", "3600"),
            ],
        );
    }));
    both(&sides, |s| {
        s.expect_line(0, r#"org/repo: stargazers graphql: graphql rate limited (token 1/1), reset in <dur>: {"message":"slow down"}, skipping"#);
    });
    let sides = check(Case::new("rs_retry", Pass::Stars).setup(|gh| {
        gql_route(
            gh,
            vec![
                Scripted::raw(403, "application/json", r#"{"message":"abuse"}"#)
                    .header("Retry-After", "1"),
                Scripted::ok(&gql_page(
                    &[("2020-05-02T10:00:00Z", "dave", 14)],
                    false,
                    "",
                )),
            ],
        );
    }));
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db stars restore: processed 1 repos, 1 pages, checked 1, restored 1",
        );
        assert_eq!(s.graphql_bodies().len(), 2);
    });
    let reset = (devstats_compat::github::now_unix() + 3600).to_string();
    let sides = check(Case::new("rs_xreset", Pass::Stars).setup(move |gh| {
        gql_route(
            gh,
            vec![Scripted::raw(403, "application/json", "{}").header("X-RateLimit-Reset", &reset)],
        );
    }));
    both(&sides, |s| {
        s.expect_line(0, "org/repo: stargazers graphql: graphql rate limited (token 1/1), reset in <dur>: {}, skipping");
    });
}

#[test]
fn stars_restore_falls_back_to_the_next_token() {
    let sides = check(
        Case::new("rs_tokens", Pass::Stars)
            .oauth(Some("@tok1,tok2"))
            .setup(|gh| {
                gql_route(
                    gh,
                    vec![
                        Scripted::raw(403, "application/json", r#"{"message":"forbidden"}"#)
                            .header("Retry-After", "3600"),
                        Scripted::ok(&gql_page(
                            &[("2020-05-02T10:00:00Z", "dave", 14)],
                            false,
                            "",
                        )),
                    ],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db stars restore: processed 1 repos, 1 pages, checked 1, restored 1",
        );
        let posts: Vec<String> = s
            .requests()
            .into_iter()
            .filter(|r| r.starts_with("POST"))
            .collect();
        assert_eq!(posts.len(), 2, "{posts:#?}");
        assert!(
            posts.iter().any(|r| r.contains(" auth=tok1 "))
                && posts.iter().any(|r| r.contains(" auth=tok2 ")),
            "{posts:#?}"
        );
    });
}

#[test]
fn stars_restore_hash_conflict() {
    let id = devstatscode::restore::negative_artificial_id(&[
        "WatchEvent",
        "14",
        REPO,
        "2020-05-02 10:00:00",
    ]);
    let sides = check(
        Case::new("rs_conflict", Pass::Stars)
            .seed(&seed_event(
                id,
                "PushEvent",
                REPO,
                REPO_ID,
                (12, "bob"),
                "2020-05-02T11:00:00Z",
            ))
            .setup(|gh| {
                gql_route(
                    gh,
                    vec![Scripted::ok(&gql_page(
                        &[("2020-05-02T10:00:00Z", "dave", 14)],
                        false,
                        "",
                    ))],
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(0, &format!("hash id {id} conflict: existing (PushEvent, org/repo, 12, 2020-05-02 11:00:00 +0000 +0000) vs new (WatchEvent, org/repo, 14, 2020-05-02 10:00:00 +0000 UTC), skipping"));
        s.expect_line(
            0,
            "ghapi2db stars restore: processed 1 repos, 1 pages, checked 1, restored 0",
        );
    });
}

#[test]
fn all_passes_run_in_order() {
    let sides = check(Case::new("all", Pass::All).setup(|gh| {
        gh.get_ok(
            "/repos/org/repo/license",
            &license_json("mit", "MIT License"),
        );
        gh.get_ok("/repos/org/repo/languages", &json!({"Go": 10}));
        gh.get_ok(&events_path(REPO), &json!([]));
        gh.get_ok(&commits_path(REPO), &json!([]));
        empty_restores(gh, REPO);
        gql_route(gh, vec![Scripted::ok(&gql_page(&[], false, ""))]);
    }));
    both(&sides, |s| {
        let lines = s.lines(0);
        let pos = |prefix: &str| {
            lines
                .iter()
                .position(|l| l.starts_with(prefix))
                .unwrap_or_else(|| panic!("no {prefix:?} in {lines:#?}"))
        };
        let order = [
            pos("Checking license on 1 repos"),
            pos("Checking programming languages on 1 repos"),
            pos("ghapi2db.go: Processing 1 repos - GHAPI Events part"),
            pos("ghapi2db.go: Processing 1 repos - GHAPI commits part"),
            pos("ghapi2db comments restore: processing 1 repos"),
            pos("ghapi2db reviews restore: processing 1 repos"),
            pos("ghapi2db forks restore: processing 1 repos"),
            pos("ghapi2db releases restore: processing 1 repos"),
            pos("ghapi2db stars restore: processing 1 repos"),
            pos("Time: "),
        ];
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{lines:#?}");
        // no gha_commits rows: the commits pass skips the repo silently
        s.expect_line(0, "GH Commits API calls: 0");
    });
}

#[test]
fn comments_restore_is_idempotent() {
    let sides = check(
        Case::new("rc_twice", Pass::Comments)
            .runs(2)
            .seed(&issue_and_pr_seed())
            .setup(|gh| {
                gh.get_ok(
                    &issue_comments_path(REPO),
                    &json!([
                        issue_comment_json(
                            90001,
                            1,
                            (11, "alice"),
                            "2020-05-01T10:00:00Z",
                            None,
                            "First!"
                        ),
                        issue_comment_json(
                            90002,
                            1,
                            (12, "bob"),
                            "2020-05-01T11:00:00Z",
                            None,
                            "Second"
                        ),
                    ]),
                );
                gh.get_ok(
                    &review_comments_path(REPO),
                    &json!([review_comment_json(
                        91001,
                        2,
                        (12, "bob"),
                        "2020-05-02T10:00:00Z",
                        "Nit",
                        None
                    )]),
                );
                gh.get_ok(
                    &commit_comments_path(REPO),
                    &json!([commit_comment_json(
                        92002,
                        (11, "alice"),
                        "2020-05-03T10:00:00Z",
                        "LGTM"
                    )]),
                );
            }),
    );
    both(&sides, |s| {
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 3 pages, checked 4, restored 4",
        );
        s.expect_line(
            0,
            "targeted postprocess skipped: gha_texts is empty, full structure rebuild pending",
        );
        // the second run finds everything present
        s.expect_line(
            1,
            "ghapi2db comments restore: processed 1 repos, 3 pages, checked 4, restored 0",
        );
        s.expect_no_prefix(1, "targeted postprocess ");
        assert_eq!(s.count("select count(*) from gha_comments"), 4);
        assert_eq!(s.count("select count(*) from gha_events"), 5);
    });
}

#[test]
fn restore_unreachable_api() {
    let sides = check(
        Case::new("rc_dead", Pass::Comments)
            .dead_api()
            .loose("GetRateLimit(0): ")
            .loose("org/repo issue comments: error: ")
            .loose("org/repo review comments: error: ")
            .loose("org/repo commit comments last page: error: ")
            .loose("org/repo commit comments: error: ")
            .setup(|_| {}),
    );
    both(&sides, |s| {
        s.expect_prefix(0, "GetRateLimit(0): ");
        s.expect_prefix(0, "org/repo issue comments: error: ");
        s.expect_prefix(0, "org/repo review comments: error: ");
        // the failed probe leaves last=1, so the walk still tries page 1
        s.expect_prefix(0, "org/repo commit comments last page: error: ");
        s.expect_prefix(0, "org/repo commit comments: error: ");
        s.expect_line(
            0,
            "ghapi2db comments restore: processed 1 repos, 0 pages, checked 0, restored 0",
        );
        assert_eq!(s.code(0), Some(0));
    });
}
