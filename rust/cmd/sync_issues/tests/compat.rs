//! Go ⇄ Rust compatibility tests for `sync_issues`.
//!
//! Every case runs each binary against its own scratch database
//! (`dbtest_sync_issues_<case>_<go|rs>`, the full DevStats schema plus the
//! case's seed rows) and its own scripted fake GitHub API
//! (`devstats_compat::github::FakeGitHub`, reached through
//! `GHA2DB_GITHUB_API_URL`), optionally several runs in a row. Compared per
//! run: the exit code, stdout (as lines, with the API URL, the binary path,
//! durations, now-derived event ids / time stamps masked; in order for
//! single-threaded cases, as a sorted multiset otherwise), the `Error: '…'`
//! stderr lines; afterwards every table of the database (rows with the
//! now-derived ids and time stamps masked) and the sorted log of the API
//! requests each binary made (method, path, query, token, `Accept`).
//!
//! The tests need a PostgreSQL server (`test.sh` finds one; skipped
//! otherwise).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use devstats_compat::github::{FakeGitHub, Scripted};
use devstats_compat::pg::{self as cpg, TestDb};
use devstats_compat::{fixture, go_binary, is_go_duration, run, rust_binary, Invocation, Outcome};
use regex::Regex;
use serde_json::{json, Value};
use tempfile::TempDir;

fn go_bin() -> Option<PathBuf> {
    go_binary("sync_issues")
}

fn rust_bin() -> PathBuf {
    rust_binary(env!("CARGO_BIN_EXE_sync_issues"))
}

/// `Invocation` borrows its environment; the handful of strings per case are
/// simply leaked for the life of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

const REPO: &str = "org/repo";
const ISSUES_ACCEPT: &str = "application/vnd.github.squirrel-girl-preview";
const V3_ACCEPT: &str = "application/vnd.github.v3+json";
/// SHA-1 of `alice`, the login anonymised by the `hide.csv` case.
const ALICE_SHA1: &str = "522b276a356bdf39013dfabea2cd43e141ecc9e8";

/// `Time: 1.234s`, `waiting 5s (0)`, `wait 8s` — Go `time.Duration`s after
/// a marker.
static DURATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(Time: |waiting |wait |message: |don't want to wait )(-?[0-9][0-9.hmsµn]*)")
        .unwrap()
});
/// Durations inside `%+v` slices: `waitPeriod: [59m59.999s]`, `reset: [1h0m0.5s 2s]`,
/// `resets in: [...]`.
static DURATION_LIST: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(waitPeriod: |reset: |resets in:? )\[([^\]]*)\]").unwrap());
/// go-github's `[rate reset in 59m59s]` / `[rate limit was reset 1s ago]`.
static RATE_RESET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[rate (?:reset in|limit was reset) [^\]]*\]").unwrap());
/// `EventID: 56612345678901` inside an `IssueConfig` string (now-derived).
static EVENT_ID: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"EventID: \d+").unwrap());
/// A `time.Now()`-derived time stamp (the fixtures use 2020 dates):
/// `2026-09-12 01:02:03`, `… 01:02:03.123456789 +0200 CEST m=+0.01`,
/// `… +0000 +0000`.
static NOW: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"20(?:2[5-9]|[3-9]\d)-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?: [+-]\d{4} \S+(?: m=[+-][\d.]+)?)?")
        .unwrap()
});
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

#[derive(Clone)]
struct MilestoneSpec {
    id: i64,
    number: i64,
    title: &'static str,
    state: &'static str,
    description: Option<&'static str>,
    creator: Option<(i64, &'static str)>,
    due_on: Option<&'static str>,
    closed_at: Option<&'static str>,
    open_issues: i64,
    closed_issues: i64,
}

impl MilestoneSpec {
    fn v1() -> MilestoneSpec {
        MilestoneSpec {
            id: 3001,
            number: 1,
            title: "v1.0",
            state: "open",
            description: Some("first release"),
            creator: Some((11, "alice")),
            due_on: Some("2020-06-30T07:00:00Z"),
            closed_at: None,
            open_issues: 4,
            closed_issues: 2,
        }
    }
    fn v2() -> MilestoneSpec {
        MilestoneSpec {
            id: 3002,
            number: 2,
            title: "v2.0",
            state: "open",
            description: None,
            creator: Some((12, "bob")),
            due_on: None,
            closed_at: None,
            open_issues: 1,
            closed_issues: 0,
        }
    }
    fn json(&self) -> Value {
        json!({
            "url": format!("https://api.github.com/repos/{REPO}/milestones/{}", self.number),
            "id": self.id,
            "node_id": format!("MDk6TWlsZXN0b25l{}", self.id),
            "number": self.number,
            "title": self.title,
            "description": self.description,
            "creator": self.creator.map(|(id, login)| user(id, login)),
            "open_issues": self.open_issues,
            "closed_issues": self.closed_issues,
            "state": self.state,
            "created_at": "2020-01-10T10:00:00Z",
            "updated_at": "2020-02-11T11:00:00Z",
            "due_on": self.due_on,
            "closed_at": self.closed_at
        })
    }
}

/// An issue as `GET /repos/{owner}/{repo}/issues/{number}` returns it.
#[derive(Clone)]
struct IssueSpec {
    id: i64,
    number: i64,
    title: String,
    body: Option<String>,
    state: &'static str,
    locked: bool,
    user: (i64, &'static str),
    assignee: Option<(i64, &'static str)>,
    assignees: Vec<(i64, &'static str)>,
    labels: Vec<(i64, &'static str)>,
    milestone: Option<MilestoneSpec>,
    comments: i64,
    created_at: &'static str,
    updated_at: &'static str,
    closed_at: Option<&'static str>,
    pull_request: bool,
    repo: &'static str,
}

impl IssueSpec {
    fn new(id: i64, number: i64) -> IssueSpec {
        IssueSpec {
            id,
            number,
            title: format!("Issue {number}"),
            body: Some(format!("Body of issue {number}")),
            state: "open",
            locked: false,
            user: (11, "alice"),
            assignee: None,
            assignees: Vec::new(),
            labels: Vec::new(),
            milestone: None,
            comments: 0,
            created_at: "2020-03-01T12:00:00Z",
            updated_at: "2020-03-02T13:30:00Z",
            closed_at: None,
            pull_request: false,
            repo: REPO,
        }
    }
    fn title(mut self, t: &str) -> Self {
        self.title = t.to_string();
        self
    }
    fn body(mut self, b: Option<&str>) -> Self {
        self.body = b.map(str::to_string);
        self
    }
    fn state(mut self, s: &'static str) -> Self {
        self.state = s;
        self
    }
    fn locked(mut self, l: bool) -> Self {
        self.locked = l;
        self
    }
    fn user(mut self, id: i64, login: &'static str) -> Self {
        self.user = (id, login);
        self
    }
    fn assignee(mut self, a: Option<(i64, &'static str)>) -> Self {
        self.assignee = a;
        self
    }
    fn assignees(mut self, a: Vec<(i64, &'static str)>) -> Self {
        self.assignees = a;
        self
    }
    fn labels(mut self, l: Vec<(i64, &'static str)>) -> Self {
        self.labels = l;
        self
    }
    fn milestone(mut self, m: Option<MilestoneSpec>) -> Self {
        self.milestone = m;
        self
    }
    fn comments(mut self, c: i64) -> Self {
        self.comments = c;
        self
    }
    fn closed_at(mut self, c: Option<&'static str>) -> Self {
        self.closed_at = c;
        self
    }
    fn pull_request(mut self, pr: bool) -> Self {
        self.pull_request = pr;
        self
    }
    fn json(&self) -> Value {
        let mut v = json!({
            "url": format!("https://api.github.com/repos/{}/issues/{}", self.repo, self.number),
            "repository_url": format!("https://api.github.com/repos/{}", self.repo),
            "labels_url": format!("https://api.github.com/repos/{}/issues/{}/labels{{/name}}", self.repo, self.number),
            "comments_url": format!("https://api.github.com/repos/{}/issues/{}/comments", self.repo, self.number),
            "events_url": format!("https://api.github.com/repos/{}/issues/{}/events", self.repo, self.number),
            "html_url": format!("https://github.com/{}/issues/{}", self.repo, self.number),
            "id": self.id,
            "node_id": format!("MDU6SXNzdWU{}", self.id),
            "number": self.number,
            "title": self.title,
            "user": user(self.user.0, self.user.1),
            "labels": self.labels.iter().map(|(id, n)| label(*id, n)).collect::<Vec<_>>(),
            "state": self.state,
            "locked": self.locked,
            "assignee": self.assignee.map(|(id, l)| user(id, l)),
            "assignees": self.assignees.iter().map(|(id, l)| user(*id, l)).collect::<Vec<_>>(),
            "milestone": self.milestone.as_ref().map(|m| m.json()),
            "comments": self.comments,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "closed_at": self.closed_at,
            "author_association": "CONTRIBUTOR",
            "active_lock_reason": null,
            "body": self.body,
            "reactions": {"url": "", "total_count": 0, "+1": 0, "-1": 0},
            "timeline_url": format!("https://api.github.com/repos/{}/issues/{}/timeline", self.repo, self.number),
            "performed_via_github_app": null,
            "state_reason": null
        });
        if self.pull_request {
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

/// A pull request as `GET /repos/{owner}/{repo}/pulls/{number}` returns it.
#[derive(Clone)]
struct PrSpec {
    id: i64,
    number: i64,
    title: String,
    body: Option<String>,
    state: &'static str,
    locked: bool,
    user: (i64, &'static str),
    merged_by: Option<(i64, &'static str)>,
    assignee: Option<(i64, &'static str)>,
    assignees: Vec<(i64, &'static str)>,
    requested_reviewers: Vec<(i64, &'static str)>,
    milestone: Option<MilestoneSpec>,
    closed_at: Option<&'static str>,
    merged_at: Option<&'static str>,
    merge_commit_sha: Option<&'static str>,
    merged: bool,
    mergeable: Option<bool>,
    mergeable_state: &'static str,
    comments: i64,
    review_comments: i64,
    maintainer_can_modify: bool,
    commits: i64,
    additions: i64,
    deletions: i64,
    changed_files: i64,
    base_sha: &'static str,
    head_sha: &'static str,
}

impl PrSpec {
    fn new(id: i64, number: i64) -> PrSpec {
        PrSpec {
            id,
            number,
            title: format!("PR {number}"),
            body: Some(format!("Body of PR {number}")),
            state: "open",
            locked: false,
            user: (12, "bob"),
            merged_by: None,
            assignee: None,
            assignees: Vec::new(),
            requested_reviewers: Vec::new(),
            milestone: None,
            closed_at: None,
            merged_at: None,
            merge_commit_sha: Some("1111111111111111111111111111111111111111"),
            merged: false,
            mergeable: Some(true),
            mergeable_state: "clean",
            comments: 1,
            review_comments: 2,
            maintainer_can_modify: true,
            commits: 3,
            additions: 40,
            deletions: 5,
            changed_files: 2,
            base_sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            head_sha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        }
    }
    fn title(mut self, t: &str) -> Self {
        self.title = t.to_string();
        self
    }
    fn merged(mut self, by: (i64, &'static str), at: &'static str) -> Self {
        self.merged = true;
        self.merged_by = Some(by);
        self.merged_at = Some(at);
        self.closed_at = Some(at);
        self.state = "closed";
        self
    }
    fn assignee(mut self, a: Option<(i64, &'static str)>) -> Self {
        self.assignee = a;
        self
    }
    fn assignees(mut self, a: Vec<(i64, &'static str)>) -> Self {
        self.assignees = a;
        self
    }
    fn reviewers(mut self, r: Vec<(i64, &'static str)>) -> Self {
        self.requested_reviewers = r;
        self
    }
    fn milestone(mut self, m: Option<MilestoneSpec>) -> Self {
        self.milestone = m;
        self
    }
    fn json(&self) -> Value {
        let branch = |label: &str, r: &str, sha: &str| {
            json!({
                "label": format!("org:{label}"),
                "ref": r,
                "sha": sha,
                "user": user(1, "org"),
                "repo": {"id": 500, "name": "repo", "full_name": REPO, "owner": user(1, "org")}
            })
        };
        json!({
            "url": format!("https://api.github.com/repos/{REPO}/pulls/{}", self.number),
            "id": self.id,
            "node_id": format!("MDExOlB1bGxSZXF1ZXN0{}", self.id),
            "html_url": format!("https://github.com/{REPO}/pull/{}", self.number),
            "number": self.number,
            "state": self.state,
            "locked": self.locked,
            "title": self.title,
            "user": user(self.user.0, self.user.1),
            "body": self.body,
            "created_at": "2020-04-01T08:00:00Z",
            "updated_at": "2020-04-02T09:00:00Z",
            "closed_at": self.closed_at,
            "merged_at": self.merged_at,
            "merge_commit_sha": self.merge_commit_sha,
            "assignee": self.assignee.map(|(id, l)| user(id, l)),
            "assignees": self.assignees.iter().map(|(id, l)| user(*id, l)).collect::<Vec<_>>(),
            "requested_reviewers": self.requested_reviewers.iter().map(|(id, l)| user(*id, l)).collect::<Vec<_>>(),
            "requested_teams": [],
            "labels": [],
            "milestone": self.milestone.as_ref().map(|m| m.json()),
            "draft": false,
            "head": branch("feature", "feature", self.head_sha),
            "base": branch("main", "main", self.base_sha),
            "author_association": "MEMBER",
            "auto_merge": null,
            "active_lock_reason": null,
            "merged": self.merged,
            "mergeable": self.mergeable,
            "rebaseable": self.mergeable,
            "mergeable_state": self.mergeable_state,
            "merged_by": self.merged_by.map(|(id, l)| user(id, l)),
            "comments": self.comments,
            "review_comments": self.review_comments,
            "maintainer_can_modify": self.maintainer_can_modify,
            "commits": self.commits,
            "additions": self.additions,
            "deletions": self.deletions,
            "changed_files": self.changed_files
        })
    }
}

fn issue_path(repo: &str, number: i64) -> String {
    format!("/repos/{repo}/issues/{number}")
}

fn pr_path(repo: &str, number: i64) -> String {
    format!("/repos/{repo}/pulls/{number}")
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

// ---------------------------------------------------------------------------
// Database seeds
// ---------------------------------------------------------------------------

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn sql_opt_str(s: Option<&str>) -> String {
    s.map(sql_str).unwrap_or_else(|| "null".to_string())
}

fn sql_opt_i64(v: Option<i64>) -> String {
    v.map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string())
}

/// A GHA event of `repo` (so `dup_repo_id` lookups find `repo_id`).
fn seed_event(id: i64, repo: &str, repo_id: i64, actor: (i64, &str), created_at: &str) -> String {
    format!(
        "insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) \
         values({id}, 'IssuesEvent', {}, {repo_id}, {}, 1, {}, {});",
        actor.0,
        sql_str(created_at),
        sql_str(actor.1),
        sql_str(repo)
    )
}

/// A previous state of `issue` as `gha_issues` (+ labels, assignees) rows
/// of event `event_id`.
fn seed_issue(issue: &IssueSpec, event_id: i64, updated_at: &str) -> String {
    let mut s = format!(
        "insert into gha_issues(id, event_id, assignee_id, body, closed_at, comments, created_at, locked, \
         milestone_id, number, state, title, updated_at, user_id, is_pull_request, dup_actor_id, dup_actor_login, \
         dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) values({}, {event_id}, {}, {}, {}, {}, \
         {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 500, {}, 'IssuesEvent', {}, {});",
        issue.id,
        sql_opt_i64(issue.assignee.map(|a| a.0)),
        sql_opt_str(issue.body.as_deref()),
        sql_opt_str(issue.closed_at).replace('T', " ").replace('Z', ""),
        issue.comments,
        sql_str(issue.created_at).replace('T', " ").replace('Z', ""),
        issue.locked,
        sql_opt_i64(issue.milestone.as_ref().map(|m| m.id)),
        issue.number,
        sql_str(issue.state),
        sql_str(&issue.title),
        sql_str(updated_at),
        issue.user.0,
        issue.pull_request,
        issue.user.0,
        sql_str(issue.user.1),
        sql_str(issue.repo),
        sql_str(updated_at),
        sql_str(issue.user.1),
    );
    for (lid, lname) in &issue.labels {
        s.push_str(&format!(
            "insert into gha_issues_labels(issue_id, event_id, label_id, dup_actor_id, dup_actor_login, dup_repo_id, \
             dup_repo_name, dup_type, dup_created_at, dup_issue_number, dup_label_name) values({}, {event_id}, {lid}, {}, {}, \
             500, {}, 'IssuesEvent', {}, {}, {});",
            issue.id,
            issue.user.0,
            sql_str(issue.user.1),
            sql_str(issue.repo),
            sql_str(updated_at),
            issue.number,
            sql_str(lname)
        ));
    }
    for (aid, _) in &issue.assignees {
        s.push_str(&format!(
            "insert into gha_issues_assignees(issue_id, event_id, assignee_id) values({}, {event_id}, {aid});",
            issue.id
        ));
    }
    s
}

/// A previous state of `pr` as a `gha_pull_requests` row of event `event_id`.
fn seed_pr(pr: &PrSpec, event_id: i64, updated_at: &str) -> String {
    let mut s = format!(
        "insert into gha_pull_requests(id, event_id, user_id, base_sha, head_sha, merged_by_id, assignee_id, milestone_id, \
         number, state, title, body, created_at, updated_at, closed_at, merged_at, merge_commit_sha, merged, mergeable, \
         mergeable_state, comments, maintainer_can_modify, commits, additions, deletions, changed_files, dup_actor_id, \
         dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login, dupn_merged_by_login) \
         values({}, {event_id}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '2020-04-01 08:00:00', {}, {}, {}, {}, {}, {}, {}, \
         {}, {}, {}, {}, {}, {}, {}, {}, 500, {}, 'PullRequestEvent', {}, {}, {});",
        pr.id,
        pr.user.0,
        sql_str(pr.base_sha),
        sql_str(pr.head_sha),
        sql_opt_i64(pr.merged_by.map(|m| m.0)),
        sql_opt_i64(pr.assignee.map(|a| a.0)),
        sql_opt_i64(pr.milestone.as_ref().map(|m| m.id)),
        pr.number,
        sql_str(pr.state),
        sql_str(&pr.title),
        sql_opt_str(pr.body.as_deref()),
        sql_str(updated_at),
        sql_opt_str(pr.closed_at).replace('T', " ").replace('Z', ""),
        sql_opt_str(pr.merged_at).replace('T', " ").replace('Z', ""),
        sql_opt_str(pr.merge_commit_sha),
        pr.merged,
        pr.mergeable
            .map(|m| m.to_string())
            .unwrap_or_else(|| "null".to_string()),
        sql_str(pr.mergeable_state),
        pr.comments,
        pr.maintainer_can_modify,
        pr.commits,
        pr.additions,
        pr.deletions,
        pr.changed_files,
        pr.user.0,
        sql_str(pr.user.1),
        sql_str(REPO),
        sql_str(updated_at),
        sql_str(pr.user.1),
        sql_opt_str(pr.merged_by.map(|m| m.1)),
    );
    for (aid, _) in &pr.assignees {
        s.push_str(&format!(
            "insert into gha_pull_requests_assignees(pull_request_id, event_id, assignee_id) values({}, {event_id}, {aid});",
            pr.id
        ));
    }
    for (rid, _) in &pr.requested_reviewers {
        s.push_str(&format!(
            "insert into gha_pull_requests_requested_reviewers(pull_request_id, event_id, requested_reviewer_id) \
             values({}, {event_id}, {rid});",
            pr.id
        ));
    }
    s
}

/// The standard seed: repo 500 known through one GHA event, actors alice/bob.
fn base_seed() -> String {
    let mut s = seed_event(1000, REPO, 500, (11, "alice"), "2020-02-01 10:00:00");
    s.push_str(
        "insert into gha_actors(id, login, name) values(11, 'alice', 'Alice A'), (12, 'bob', 'Bob B');",
    );
    s
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

type Setup = Box<dyn Fn(&FakeGitHub) + Send + Sync>;

struct Case {
    name: &'static str,
    /// `GHA2DB_ISSUES_SYNC_SQL` (`None`: not set).
    sql: Option<String>,
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
    /// only (documented wording deviations, e.g. JSON decoder messages).
    loose: Vec<String>,
    /// Compare the final database contents (off for multi-threaded runs:
    /// artificial event ids are `UnixNano / 31622` and collide at random
    /// between workers, which drops `gha_events` rows via insert-ignore).
    compare_data: bool,
}

impl Case {
    fn new(name: &'static str) -> Case {
        Case {
            name,
            sql: Some(format!("select '{REPO}', 1")),
            env: vec![("GHA2DB_ST".to_string(), "1".to_string())],
            oauth: Some("tok1".to_string()),
            seed: base_seed(),
            setup: Box::new(|_| {}),
            runs: 1,
            hide: None,
            dead_api: false,
            ordered: true,
            loose: Vec::new(),
            compare_data: true,
        }
    }
    fn loose(mut self, prefix: &str) -> Self {
        self.loose.push(prefix.to_string());
        self
    }
    fn sql(mut self, sql: &str) -> Self {
        self.sql = Some(sql.to_string());
        self
    }
    fn no_sql(mut self) -> Self {
        self.sql = None;
        self
    }
    /// `select 'repo', n union all …` for the given (repo, number) pairs.
    fn issues(self, list: &[(&str, i64)]) -> Self {
        let parts: Vec<String> = list
            .iter()
            .map(|(r, n)| format!("select '{r}'::text, {n}::int"))
            .collect();
        self.sql(&parts.join(" union all "))
    }
    fn env(mut self, k: &str, v: &str) -> Self {
        self.env.retain(|(ek, _)| ek != k);
        self.env.push((k.to_string(), v.to_string()));
        self
    }
    /// Multi-threaded (drop `GHA2DB_ST`): stdout compared as a multiset,
    /// database contents checked by the scenario itself.
    fn mt(mut self) -> Self {
        self.env.retain(|(k, _)| k != "GHA2DB_ST");
        self.ordered = false;
        self.compare_data = false;
        self
    }
    fn oauth(mut self, o: Option<&str>) -> Self {
        self.oauth = o.map(str::to_string);
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
}

type TableDump = (Vec<String>, Vec<Vec<String>>);

/// One binary's runs of a case.
struct Side {
    db: TestDb,
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

impl Side {
    fn mask(&self, l: &str) -> String {
        let l = l.replace(self.api.base_url(), "<api>/");
        let l = l.replace(&self.bin_str, "<bin>");
        let l = l.replace(&self.db.name, "<db>");
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
    fn expect_contains(&self, i: usize, needle: &str) {
        let lines = self.lines(i);
        assert!(
            lines.iter().any(|l| l.contains(needle)),
            "no line containing {needle:?} in run #{i}: {lines:#?}"
        );
    }
    /// Assert run `i` has the `SyncIssuesState` info block `header:` with
    /// the entry `\t<entry>` right below it.
    fn expect_info(&self, i: usize, header: &str, entry: &str) {
        let lines = self.lines(i);
        let pos = lines
            .iter()
            .position(|l| l == &format!("{header}:"))
            .unwrap_or_else(|| panic!("missing info block {header:?} in run #{i}: {lines:#?}"));
        let block: Vec<&String> = lines[pos + 1..]
            .iter()
            .take_while(|l| l.starts_with('\t'))
            .collect();
        assert!(
            block.iter().any(|l| l.as_str() == format!("\t{entry}")),
            "missing entry {entry:?} under {header:?} in run #{i}: {block:#?}"
        );
    }
    /// The `Manually processed … issues/PRs (…)` summary line of run `i`
    /// without the rate limit tail.
    fn processed(&self, i: usize) -> Vec<String> {
        self.lines(i)
            .iter()
            .filter(|l| l.starts_with("ghapi2db.go: Manually processed "))
            .map(|l| l.split("): ").next().unwrap().to_string() + ")")
            .collect()
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
    /// Every non-empty table with its rows (masked, sorted).
    fn data(&self) -> BTreeMap<String, TableDump> {
        let con = self.db.conn();
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
    /// The API requests, as sorted summaries.
    fn requests(&self) -> Vec<String> {
        self.api.summaries()
    }
}

fn run_side(bin: &Path, case: &Case, suffix: &str) -> Option<Side> {
    let schema = fs::read_to_string(fixture("structure/full_structure.sql")).unwrap();
    let db = TestDb::fresh(&format!("sync_issues_{}_{}", case.name, suffix))?;
    db.exec(&schema);
    if !case.seed.is_empty() {
        db.exec(&case.seed);
    }
    let api = FakeGitHub::start();
    (case.setup)(&api);
    let dir = tempfile::Builder::new()
        .prefix(&format!("g2r_sync_issues_{}_{}_", case.name, suffix))
        .tempdir()
        .unwrap();
    if let Some(csv) = &case.hide {
        fs::create_dir_all(dir.path().join("hide")).unwrap();
        fs::write(dir.path().join("hide").join("hide.csv"), csv).unwrap();
    }
    let api_url = if case.dead_api {
        "http://127.0.0.1:1/".to_string()
    } else {
        api.base_url().to_string()
    };
    let mut env: Vec<(&str, &str)> = db.env();
    env.push(("GHA2DB_GITHUB_API_URL", leak(&api_url)));
    env.push(("GHA2DB_PROJECT", "test"));
    if let Some(o) = &case.oauth {
        if let Some(tokens) = o.strip_prefix('@') {
            let p = dir.path().join("oauth.txt");
            fs::write(&p, format!("{tokens}\n")).unwrap();
            env.push(("GHA2DB_GITHUB_OAUTH", leak(p.to_str().unwrap())));
        } else {
            env.push(("GHA2DB_GITHUB_OAUTH", leak(o)));
        }
    }
    if let Some(sql) = &case.sql {
        env.push(("GHA2DB_ISSUES_SYNC_SQL", leak(sql)));
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
                "\ncase {} run #{i}\n--- go (code {:?}) stdout ---\n{}\n--- go stderr ---\n{}\n--- rust (code {:?}) stdout ---\n{}\n--- rust stderr ---\n{}\n",
                case.name,
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
// Scenarios: input handling
// ---------------------------------------------------------------------------

#[test]
fn no_sql_is_fatal() {
    let sides = check(Case::new("no_sql").no_sql());
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_prefix(
            0,
            "You have to provide a SQL query to get a list of issue numbers to sync.",
        );
        assert_eq!(
            s.errors(0),
            vec!["Error: 'no sync issues sql query provided'".to_string()]
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn bad_sql_is_fatal() {
    let sides = check(Case::new("bad_sql").sql("selec 1"));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        assert!(!s.errors(0).is_empty(), "{:?}", s.outs[0].stderr_str());
    });
}

#[test]
fn empty_result_syncs_nothing() {
    let sides = check(Case::new("empty").sql(&format!("select '{REPO}', 1 where false")));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "sync_issues.go: Processing 0 issues - GHAPI part");
        s.expect_line(
            0,
            "ghapi2db.go: Processing 0 PRs, 0 issues (0 with date collisions), manual mode: true - GHA part",
        );
        s.expect_prefix(0, "Time: <dur>");
        s.expect_line(0, "Issues to process:");
        s.expect_line(0, "PRs to process:");
        s.expect_line(0, "PRs: ");
        assert_eq!(
            s.processed(0),
            vec![
                "ghapi2db.go: Manually processed 0 issues/PRs (0 new issues, existing: 0 not needed, 0 added)".to_string(),
                "ghapi2db.go: Manually processed 0 PRs (0 new PRs, existing: 0 not needed, 0 added)".to_string(),
            ]
        );
        assert_eq!(
            s.requests(),
            vec![
                format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"),
                format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"),
            ]
        );
    });
}

#[test]
fn from_to_replacements_are_applied() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("from_to")
            .sql("select '{{repo}}', {{num}} where '{{x}}' = 'x'")
            .env("FROM1", "{{repo}}")
            .env("TO1", REPO)
            .env("FROM2", "{{num}}")
            .env("TO2", "1")
            .env("FROM3", "{{x}}")
            .env("TO3", "x")
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "sync_issues.go: Processing 1 issues - GHAPI part");
        assert_eq!(s.count("select count(*) from gha_issues where id = 101"), 1);
    });
}

#[test]
fn from_to_stops_at_first_gap() {
    // FROM2 is missing → FROM3/TO3 are ignored → the query keeps `{{x}}`.
    let sides = check(
        Case::new("from_to_gap")
            .sql("select '{{repo}}', 1 where '{{x}}' = 'x'")
            .env("FROM1", "{{repo}}")
            .env("TO1", REPO)
            .env("FROM3", "{{x}}")
            .env("TO3", "x"),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "sync_issues.go: Processing 0 issues - GHAPI part");
    });
}

#[test]
fn duplicates_are_reported_in_debug_mode() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("duplicates")
            .issues(&[(REPO, 1), (REPO, 1), (REPO, 1)])
            .env("GHA2DB_DEBUG", "1")
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(s.count_prefix(0, &format!("Duplicated issue: {REPO}:1")), 2);
        s.expect_line(0, "sync_issues.go: Processing 1 issues - GHAPI part");
        // `else if ctx.Debug == 1 { "Processing issue number" }` is dead code
        // in Go (`ctx.Debug > 0` wins) — both print the full config.
        s.expect_line(0, &format!("Processing {{Repo: {REPO}, Number: 1, IssueID: 101, EventID: <evid>, EventType: sync, Pr: false, MilestoneID: 0, AssigneeID: 0, CreatedAt: <now>, Labels: , LabelsMap: map[], Assignees: , AssigneesMap: map[]}}"));
        s.expect_no_prefix(0, "Processing issue number");
        s.expect_line(0, "Final GHAPI threads join");
        assert_eq!(
            s.requests()
                .iter()
                .filter(|r| r.contains("/issues/1 "))
                .count(),
            1
        );
    });
}

#[test]
fn duplicates_are_silent_without_debug() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("duplicates_quiet")
            .issues(&[(REPO, 1), (REPO, 1)])
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        s.expect_no_prefix(0, "Duplicated issue:");
        s.expect_no_line(0, "Final GHAPI threads join");
    });
}

#[test]
fn invalid_repo_names_are_skipped() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("bad_repos")
            .issues(&[("norepo", 1), ("/x", 2), ("x/", 3), ("", 4), (REPO, 1)])
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "sync_issues.go: Processing 5 issues - GHAPI part");
        s.expect_line(
            0,
            "ghapi2db.go: Processing 0 PRs, 1 issues (1 with date collisions), manual mode: true - GHA part",
        );
        // Every SQL row (even an invalid repo name) is handed to a worker and
        // the main loop asks for the rate limits after each join: 5 + 1
        // (retry loop of the valid issue) + 2 (SyncIssuesState) calls.
        let reqs = s.requests();
        assert_eq!(reqs.len(), 9, "{reqs:?}");
        assert_eq!(
            reqs[8],
            format!("GET /repos/{REPO}/issues/1 accept={ISSUES_ACCEPT} auth=tok1")
        );
        assert!(reqs[..8]
            .iter()
            .all(|r| r == &format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1")));
    });
}

// ---------------------------------------------------------------------------
// Scenarios: issue states
// ---------------------------------------------------------------------------

#[test]
fn new_issue_gets_an_artificial_event() {
    let issue = IssueSpec::new(101, 1)
        .labels(vec![(7, "bug"), (3, "help wanted")])
        .assignee(Some((12, "bob")))
        .assignees(vec![(12, "bob"), (13, "carol")])
        .milestone(Some(MilestoneSpec::v1()))
        .comments(4);
    let sides = check(
        Case::new("new_issue").setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Issues to process:");
        s.expect_line(0, &format!("{REPO} 1: [<now> sync]"));
        s.expect_line(
            0,
            "ghapi2db.go: Processing 0 PRs, 1 issues (1 with date collisions), manual mode: true - GHA part",
        );
        assert_eq!(
            s.processed(0),
            vec![
                "ghapi2db.go: Manually processed 1 issues/PRs (1 new issues, existing: 0 not needed, 0 added)".to_string(),
                "ghapi2db.go: Manually processed 0 PRs (0 new PRs, existing: 0 not needed, 0 added)".to_string(),
            ]
        );
        s.expect_info(0, "no previous issue state", &format!("{REPO} 1"));
        assert_eq!(s.count("select count(*) from gha_issues where id = 101"), 1);
        assert_eq!(
            s.query("select state, title, locked::text, comments::text, milestone_id::text, assignee_id::text, is_pull_request::text, dup_type, dup_actor_id::text, dup_actor_login, dup_repo_id::text, dup_repo_name, dup_user_login from gha_issues where id = 101"),
            vec![vec![
                "open".to_string(), "Issue 1".to_string(), "false".to_string(), "4".to_string(), "3001".to_string(),
                "12".to_string(), "false".to_string(), "sync".to_string(), "-1".to_string(), "devstats-sync".to_string(),
                "500".to_string(), REPO.to_string(), "alice".to_string(),
            ]]
        );
        assert_eq!(
            s.column("select label_id::text from gha_issues_labels where issue_id = 101 order by label_id"),
            vec!["3", "7"]
        );
        assert_eq!(
            s.column("select assignee_id::text from gha_issues_assignees where issue_id = 101 order by assignee_id"),
            vec!["12", "13"]
        );
        assert_eq!(
            s.query("select id::text, number::text, title, state, creator_id::text, open_issues::text, closed_issues::text, dupn_creator_login from gha_milestones"),
            vec![vec!["3001".to_string(), "1".to_string(), "v1.0".to_string(), "open".to_string(), "11".to_string(), "4".to_string(), "2".to_string(), "alice".to_string()]]
        );
        assert_eq!(
            s.query("select type, actor_id::text, repo_id::text, dup_actor_login, dup_repo_name from gha_events where id > 1000"),
            vec![vec!["sync".to_string(), "-1".to_string(), "500".to_string(), "devstats-sync".to_string(), REPO.to_string()]]
        );
        assert_eq!(
            s.query("select action, issue_id::text, number::text, pull_request_id::text, dup_type from gha_payloads"),
            vec![vec!["sync".to_string(), "101".to_string(), "1".to_string(), "<nil>".to_string(), "sync".to_string()]]
        );
        assert_eq!(
            s.column("select login from gha_actors order by id"),
            vec!["devstats-sync", "alice", "bob", "carol"]
        );
        // retry loop + main loop join + 2× SyncIssuesState = 4 rate limit
        // calls, then (sorted) the issue itself.
        assert_eq!(
            s.requests(),
            vec![
                format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"),
                format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"),
                format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"),
                format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"),
                format!("GET /repos/{REPO}/issues/1 accept={ISSUES_ACCEPT} auth=tok1"),
            ]
        );
    });
}

#[test]
fn unchanged_issue_is_skipped() {
    let issue = IssueSpec::new(101, 1)
        .labels(vec![(7, "bug")])
        .assignee(Some((12, "bob")))
        .assignees(vec![(12, "bob")])
        .milestone(Some(MilestoneSpec::v1()));
    let seed = seed_issue(&issue, 1000, "2020-03-02 13:30:00");
    let sides = check(
        Case::new("same_issue")
            .seed(&seed)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(
            s.processed(0)[0],
            "ghapi2db.go: Manually processed 1 issues/PRs (0 new issues, existing: 1 not needed, 0 added)"
        );
        s.expect_info(0, "previous issue state the same", &format!("{REPO} 1"));
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
        assert_eq!(s.count("select count(*) from gha_events"), 1);
    });
}

#[test]
fn rerun_sees_the_same_state() {
    let issue = IssueSpec::new(101, 1).labels(vec![(7, "bug")]);
    let sides = check(
        Case::new("rerun")
            .runs(2)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(
            s.processed(0)[0],
            "ghapi2db.go: Manually processed 1 issues/PRs (1 new issues, existing: 0 not needed, 0 added)"
        );
        assert_eq!(
            s.processed(1)[0],
            "ghapi2db.go: Manually processed 1 issues/PRs (0 new issues, existing: 1 not needed, 0 added)"
        );
        s.expect_info(1, "previous issue state the same", &format!("{REPO} 1"));
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
        assert_eq!(s.count("select count(*) from gha_issues_labels"), 1);
    });
}

#[test]
fn changed_state_title_and_lock() {
    let old = IssueSpec::new(101, 1);
    let new = IssueSpec::new(101, 1)
        .state("closed")
        .title("Issue 1 (renamed)")
        .locked(true)
        .closed_at(Some("2020-05-05T05:05:05Z"));
    let seed = seed_issue(&old, 1000, "2020-03-02 13:30:00");
    let sides = check(
        Case::new("changed_state")
            .seed(&seed)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &new.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(
            s.processed(0)[0],
            "ghapi2db.go: Manually processed 1 issues/PRs (0 new issues, existing: 0 not needed, 1 added)"
        );
        s.expect_info(
            0,
            "changed issue state",
            &format!("{REPO} 1: open -> closed"),
        );
        s.expect_info(
            0,
            "changed issue title",
            &format!("{REPO} 1: Issue 1 -> Issue 1 (renamed)"),
        );
        s.expect_info(
            0,
            "changed issue locked state",
            &format!("{REPO} 1: false -> true"),
        );
        s.expect_info(
            0,
            "changed issue closed at",
            &format!("{REPO} 1: null -> 2020-05-05 05:05:05"),
        );
        s.expect_info(0, "previous issue state different", &format!("{REPO} 1"));
        assert_eq!(s.count("select count(*) from gha_issues where id = 101"), 2);
        assert_eq!(
            s.column("select state from gha_issues where id = 101 order by event_id"),
            vec!["open", "closed"]
        );
    });
}

#[test]
fn changed_milestone_and_assignee() {
    let old = IssueSpec::new(101, 1)
        .assignee(Some((12, "bob")))
        .milestone(Some(MilestoneSpec::v1()));
    let mut m2 = MilestoneSpec::v1();
    m2.id = 3002;
    m2.number = 2;
    m2.title = "v2.0";
    m2.state = "closed";
    m2.closed_at = Some("2020-07-01T00:00:00Z");
    m2.creator = None;
    m2.description = None;
    m2.due_on = None;
    let new = IssueSpec::new(101, 1)
        .assignee(Some((13, "carol")))
        .milestone(Some(m2));
    let seed = seed_issue(&old, 1000, "2020-03-02 13:30:00");
    let sides = check(
        Case::new("changed_milestone")
            .seed(&seed)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &new.json())),
    );
    both(&sides, |s| {
        s.expect_info(
            0,
            "changed issue milestone",
            &format!("{REPO} 1: 3001 -> 3002"),
        );
        s.expect_info(0, "changed issue assignee", &format!("{REPO} 1: 12 -> 13"));
        assert_eq!(
            s.query("select id::text, state, closed_at::text, creator_id::text, description, due_on::text, dupn_creator_login from gha_milestones"),
            vec![vec!["3002".to_string(), "closed".to_string(), "2020-07-01 00:00:00".to_string(), "<nil>".to_string(), "<nil>".to_string(), "<nil>".to_string(), "<nil>".to_string()]]
        );
    });
}

#[test]
fn milestone_and_assignee_removed() {
    let old = IssueSpec::new(101, 1)
        .assignee(Some((12, "bob")))
        .assignees(vec![(12, "bob")])
        .milestone(Some(MilestoneSpec::v1()));
    let new = IssueSpec::new(101, 1);
    let seed = seed_issue(&old, 1000, "2020-03-02 13:30:00");
    let sides = check(
        Case::new("removed_milestone")
            .seed(&seed)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &new.json())),
    );
    both(&sides, |s| {
        s.expect_info(
            0,
            "changed issue milestone",
            &format!("{REPO} 1: 3001 -> null"),
        );
        s.expect_info(
            0,
            "changed issue assignee",
            &format!("{REPO} 1: 12 -> null"),
        );
        s.expect_info(0, "changed issue assignees", &format!("{REPO} 1: 12 -> "));
    });
}

#[test]
fn changed_labels_and_assignees() {
    let old = IssueSpec::new(101, 1)
        .labels(vec![(7, "bug"), (3, "help wanted")])
        .assignees(vec![(12, "bob")]);
    let new = IssueSpec::new(101, 1)
        .labels(vec![(7, "bug"), (9, "kind/feature")])
        .assignees(vec![(12, "bob"), (13, "carol")]);
    let seed = seed_issue(&old, 1000, "2020-03-02 13:30:00");
    let sides = check(
        Case::new("changed_labels")
            .seed(&seed)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &new.json())),
    );
    both(&sides, |s| {
        s.expect_info(0, "changed issue labels", &format!("{REPO} 1: 3,7 -> 7,9"));
        s.expect_info(
            0,
            "changed issue assignees",
            &format!("{REPO} 1: 12 -> 12,13"),
        );
        assert_eq!(s.count("select count(*) from gha_issues_labels"), 4);
        assert_eq!(
            s.column("select dup_label_name from gha_issues_labels where event_id > 1000 order by label_id"),
            vec!["bug", "kind/feature"]
        );
    });
}

#[test]
fn latest_state_wins_when_several_events_exist() {
    // Two previous states: the newer one (by updated_at) matches the API.
    let older = IssueSpec::new(101, 1).title("Old title");
    let newer = IssueSpec::new(101, 1);
    let mut seed = seed_issue(&older, 1000, "2020-03-01 00:00:00");
    seed.push_str(&seed_event(
        1001,
        REPO,
        500,
        (11, "alice"),
        "2020-03-02 10:00:00",
    ));
    seed.push_str(&seed_issue(&newer, 1001, "2020-03-02 13:30:00"));
    let api_issue = newer.clone();
    let sides = check(
        Case::new("latest_state")
            .seed(&seed)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &api_issue.json())),
    );
    both(&sides, |s| {
        s.expect_info(0, "previous issue state the same", &format!("{REPO} 1"));
        assert_eq!(s.count("select count(*) from gha_issues"), 2);
    });
}

#[test]
fn issue_body_null_and_unicode_title() {
    let issue = IssueSpec::new(101, 1)
        .body(None)
        .title("Zażółć gęślą jaźń — 日本語 \u{1F600} 'quote' \"dq\"");
    let sides = check(
        Case::new("unicode").setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(
            s.query("select body, title from gha_issues where id = 101"),
            vec![vec![
                "<nil>".to_string(),
                "Zażółć gęślą jaźń — 日本語 \u{1F600} 'quote' \"dq\"".to_string()
            ]]
        );
    });
}

#[test]
fn long_body_is_truncated() {
    let long: String = "x".repeat(70000);
    let issue = IssueSpec::new(101, 1).body(Some(&long));
    let sides = check(
        Case::new("long_body").setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(
            s.column("select length(body)::text from gha_issues where id = 101"),
            vec!["65535"]
        );
    });
}

// ---------------------------------------------------------------------------
// Scenarios: pull requests
// ---------------------------------------------------------------------------

/// The empty `IssueConfig{Repo: …}` passed to `HandlePossibleError`.
fn empty_cfg() -> String {
    format!("{{Repo: {REPO}, Number: 0, IssueID: 0, EventID: <evid>, EventType: , Pr: false, MilestoneID: 0, AssigneeID: 0, CreatedAt: 0001-01-01 00:00:00, Labels: , LabelsMap: map[], Assignees: , AssigneesMap: map[]}}")
}

#[test]
fn new_pr_gets_artificial_issue_and_pr_events() {
    let issue = IssueSpec::new(201, 2).pull_request(true).user(12, "bob");
    let pr = PrSpec::new(9201, 2)
        .assignee(Some((11, "alice")))
        .assignees(vec![(11, "alice")])
        .reviewers(vec![(13, "carol")])
        .milestone(Some(MilestoneSpec::v1()));
    let sides = check(Case::new("new_pr").issues(&[(REPO, 2)]).setup(move |api| {
        api.get_ok(&issue_path(REPO, 2), &issue.json());
        api.get_ok(&pr_path(REPO, 2), &pr.json());
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "ghapi2db.go: Processing 1 PRs, 1 issues (1 with date collisions), manual mode: true - GHA part",
        );
        assert_eq!(
            s.processed(0),
            vec![
                "ghapi2db.go: Manually processed 1 issues/PRs (1 new issues, existing: 0 not needed, 0 added)".to_string(),
                "ghapi2db.go: Manually processed 1 PRs (1 new PRs, existing: 0 not needed, 0 added)".to_string(),
            ]
        );
        s.expect_info(0, "no previous issue state", &format!("{REPO} 2"));
        assert_eq!(
            s.query("select id::text, number::text, is_pull_request::text from gha_issues"),
            vec![vec!["201".to_string(), "2".to_string(), "true".to_string()]]
        );
        assert_eq!(
            s.query(
                "select id::text, number::text, state, assignee_id::text, milestone_id::text, merged::text, \
                 dup_actor_login, dup_user_login, dupn_merged_by_login from gha_pull_requests"
            ),
            vec![vec![
                "9201".to_string(),
                "2".to_string(),
                "open".to_string(),
                "11".to_string(),
                "3001".to_string(),
                "false".to_string(),
                "devstats-sync".to_string(),
                "bob".to_string(),
                "<nil>".to_string(),
            ]]
        );
        assert_eq!(
            s.column("select assignee_id::text from gha_pull_requests_assignees"),
            vec!["11"]
        );
        assert_eq!(
            s.column(
                "select requested_reviewer_id::text from gha_pull_requests_requested_reviewers"
            ),
            vec!["13"]
        );
        assert_eq!(s.count("select count(*) from gha_milestones"), 1);
        assert_eq!(
            s.column("select login from gha_actors order by id"),
            vec!["devstats-sync", "alice", "bob", "carol"]
        );
        let reqs = s.requests();
        assert!(
            reqs.contains(&format!(
                "GET /repos/{REPO}/pulls/2 accept={V3_ACCEPT} auth=tok1"
            )),
            "{reqs:?}"
        );
        assert!(
            reqs.contains(&format!(
                "GET /repos/{REPO}/issues/2 accept={ISSUES_ACCEPT} auth=tok1"
            )),
            "{reqs:?}"
        );
        // issue retry loop + PR retry loop + join + 2× issues state + 1× PRs state.
        assert_eq!(
            reqs.iter()
                .filter(|r| r.starts_with("GET /rate_limit "))
                .count(),
            5,
            "{reqs:?}"
        );
    });
}

#[test]
fn unchanged_pr_is_skipped() {
    let issue = IssueSpec::new(201, 2).pull_request(true).user(12, "bob");
    let pr = PrSpec::new(9201, 2)
        .assignees(vec![(11, "alice")])
        .reviewers(vec![(13, "carol")]);
    let mut seed = seed_event(1001, REPO, 500, (12, "bob"), "2020-04-02 09:00:00");
    seed.push_str(&seed_issue(&issue, 1001, "2020-04-02 09:00:00"));
    seed.push_str(&seed_pr(&pr, 1001, "2020-04-02 09:00:00"));
    seed.push_str("insert into gha_actors(id, login, name) values(13, 'carol', 'Carol C');");
    let sides = check(
        Case::new("same_pr")
            .issues(&[(REPO, 2)])
            .seed(&seed)
            .setup(move |api| {
                api.get_ok(&issue_path(REPO, 2), &issue.json());
                api.get_ok(&pr_path(REPO, 2), &pr.json());
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(
            s.processed(0),
            vec![
                "ghapi2db.go: Manually processed 1 issues/PRs (0 new issues, existing: 1 not needed, 0 added)".to_string(),
                "ghapi2db.go: Manually processed 1 PRs (0 new PRs, existing: 1 not needed, 0 added)".to_string(),
            ]
        );
        assert_eq!(s.count("select count(*) from gha_pull_requests"), 1);
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
        assert_eq!(s.count("select count(*) from gha_events"), 2);
    });
}

#[test]
fn changed_pr_state_and_merge() {
    let issue_before = IssueSpec::new(201, 2).pull_request(true).user(12, "bob");
    let pr_before = PrSpec::new(9201, 2).assignees(vec![(11, "alice")]);
    let issue_after = issue_before
        .clone()
        .state("closed")
        .closed_at(Some("2020-05-05T05:05:05Z"));
    let pr_after = pr_before
        .clone()
        .merged((11, "alice"), "2020-05-05T05:05:05Z")
        .title("PR 2 (merged)")
        .assignee(Some((13, "carol")))
        .assignees(vec![(13, "carol")])
        .reviewers(vec![(11, "alice")])
        .milestone(Some(MilestoneSpec::v2()));
    let mut seed = seed_event(1001, REPO, 500, (12, "bob"), "2020-04-02 09:00:00");
    seed.push_str(&seed_issue(&issue_before, 1001, "2020-04-02 09:00:00"));
    seed.push_str(&seed_pr(&pr_before, 1001, "2020-04-02 09:00:00"));
    let sides = check(
        Case::new("changed_pr")
            .issues(&[(REPO, 2)])
            .seed(&seed)
            .setup(move |api| {
                api.get_ok(&issue_path(REPO, 2), &issue_after.json());
                api.get_ok(&pr_path(REPO, 2), &pr_after.json());
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        assert_eq!(
            s.processed(0),
            vec![
                "ghapi2db.go: Manually processed 1 issues/PRs (0 new issues, existing: 0 not needed, 1 added)".to_string(),
                "ghapi2db.go: Manually processed 1 PRs (0 new PRs, existing: 0 not needed, 1 added)".to_string(),
            ]
        );
        s.expect_info(
            0,
            "changed issue state",
            &format!("{REPO} 2: open -> closed"),
        );
        assert_eq!(s.count("select count(*) from gha_pull_requests"), 2);
        assert_eq!(
            s.query(
                "select state, title, merged::text, merged_by_id::text, assignee_id::text, milestone_id::text, \
                 dupn_merged_by_login from gha_pull_requests order by event_id desc limit 1"
            ),
            vec![vec![
                "closed".to_string(),
                "PR 2 (merged)".to_string(),
                "true".to_string(),
                "11".to_string(),
                "13".to_string(),
                "3002".to_string(),
                "alice".to_string(),
            ]]
        );
        assert_eq!(
            s.column("select assignee_id::text from gha_pull_requests_assignees order by event_id, assignee_id"),
            vec!["11", "13"]
        );
        assert_eq!(
            s.column(
                "select requested_reviewer_id::text from gha_pull_requests_requested_reviewers"
            ),
            vec!["11"]
        );
    });
}

// ---------------------------------------------------------------------------
// Scenarios: API error handling
// ---------------------------------------------------------------------------

#[test]
fn not_found_404_is_a_warning() {
    let sides = check(
        Case::new("not_found")
            .setup(|api| api.get(&issue_path(REPO, 1), vec![Scripted::not_found()])),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            &format!(
                "Not found (Issues.Get) for {}: GET <api>/repos/{REPO}/issues/1: 404 Not Found []",
                empty_cfg()
            ),
        );
        s.expect_line(0, &format!("Warning: not found: {REPO} 1"));
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
        // one rate limit call before the GET, one after the join, two in SyncIssuesState
        assert_eq!(s.requests().len(), 5);
    });
}

#[test]
fn deleted_410_is_a_warning() {
    let sides = check(Case::new("deleted").setup(|api| {
        api.get(
            &issue_path(REPO, 1),
            vec![Scripted::error(410, "This issue was deleted")],
        )
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(
            0,
            &format!("Issue was deleted (Issues.Get) for {}: ", empty_cfg()),
        );
        s.expect_line(0, &format!("Warning: issue is deleted: {REPO} 1"));
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn empty_repository_409_is_not_found() {
    let sides = check(Case::new("empty_repo").setup(|api| {
        api.get(
            &issue_path(REPO, 1),
            vec![Scripted::error(409, "Git Repository is empty")],
        )
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(
            0,
            &format!("Git repository empty (Issues.Get) for {}: ", empty_cfg()),
        );
        s.expect_line(0, &format!("Warning: not found: {REPO} 1"));
    });
}

#[test]
fn server_error_502_is_retried() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(Case::new("server_error").setup(move |api| {
        api.get(
            &issue_path(REPO, 1),
            vec![
                Scripted::error(502, "Server Error"),
                Scripted::ok(&issue.json()),
            ],
        )
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            &format!("Server Error (Issues.Get) for {}: GET <api>/repos/{REPO}/issues/1: 502 Server Error []", empty_cfg()),
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
        let reqs = s.requests();
        assert_eq!(
            reqs.iter()
                .filter(|r| r.starts_with(&format!("GET /repos/{REPO}/issues/1 ")))
                .count(),
            2
        );
        assert_eq!(
            reqs.iter()
                .filter(|r| r.starts_with("GET /rate_limit "))
                .count(),
            5
        );
    });
}

#[test]
fn retries_are_limited() {
    let sides = check(
        Case::new("retry_limit")
            .env("GHA2DB_MAX_GHAPI_RETRY", "2")
            .setup(|api| {
                api.get(
                    &issue_path(REPO, 1),
                    vec![Scripted::error(502, "Server Error")],
                )
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        assert_eq!(s.count_prefix(0, "Server Error (Issues.Get) for "), 2);
        assert_eq!(
            s.errors(0),
            vec![
                "Error: 'GetRateLimit call failed 2 times while getting issue, aborting'"
                    .to_string()
            ]
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn moved_301_without_location_is_a_warning() {
    let sides = check(
        Case::new("moved_no_loc")
            .setup(|api| api.get(&issue_path(REPO, 1), vec![Scripted::redirect(301, "")])),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            &format!(
                "Moved Permanently (Issues.Get) for {}: GET <api>/repos/{REPO}/issues/1: 301 Moved Permanently []",
                empty_cfg()
            ),
        );
        s.expect_line(
            0,
            &format!("Warning: This issue has been transferred: {REPO} 1"),
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn moved_301_with_location_is_followed() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(Case::new("moved_followed").setup(move |api| {
        let target = format!("{}repos/neworg/newrepo/issues/1", api.base_url());
        api.get(&issue_path(REPO, 1), vec![Scripted::redirect(301, &target)]);
        api.get_ok("/repos/neworg/newrepo/issues/1", &issue.json());
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_no_prefix(0, "Warning:");
        // The issue is stored under the repo name given in the SQL.
        assert_eq!(s.column("select dup_repo_name from gha_issues"), vec![REPO]);
        let reqs = s.requests();
        // Same host: the token is kept on the redirected request.
        assert!(
            reqs.contains(&format!(
                "GET /repos/neworg/newrepo/issues/1 accept={ISSUES_ACCEPT} auth=tok1"
            )),
            "{reqs:?}"
        );
        assert!(
            reqs.contains(&format!(
                "GET /repos/{REPO}/issues/1 accept={ISSUES_ACCEPT} auth=tok1"
            )),
            "{reqs:?}"
        );
    });
}

#[test]
fn transferred_issue_number_mismatch_is_a_warning() {
    let issue = IssueSpec::new(107, 7);
    let sides = check(Case::new("transferred").setup(move |api| {
        let target = format!("{}repos/{REPO}/issues/7", api.base_url());
        api.get(&issue_path(REPO, 1), vec![Scripted::redirect(301, &target)]);
        api.get_ok(&issue_path(REPO, 7), &issue.json());
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        // `issue.Repository` is not part of an Issues.Get payload → `/#7`.
        s.expect_line(
            0,
            &format!("Warning: This issue has been transferred from {REPO}#1 to /#7"),
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn number_mismatch_without_redirect_is_a_warning() {
    let issue = IssueSpec::new(107, 7);
    let sides = check(
        Case::new("mismatch").setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            &format!("Warning: This issue has been transferred from {REPO}#1 to /#7"),
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn other_api_error_exits_0() {
    let sides = check(
        Case::new("error_500")
            .setup(|api| api.get(&issue_path(REPO, 1), vec![Scripted::error(500, "boom")])),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            &format!("<bin> error: *github.ErrorResponse:GET <api>/repos/{REPO}/issues/1: 500 boom [], non fatal, exiting 0 status"),
        );
        s.expect_no_prefix(0, "Time: ");
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn accepted_202_exits_0() {
    let sides = check(
        Case::new("accepted")
            .setup(|api| api.get(&issue_path(REPO, 1), vec![Scripted::accepted()])),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "<bin> error: *github.AcceptedError:job scheduled on GitHub side; try again later, non fatal, exiting 0 status",
        );
    });
}

#[test]
fn malformed_json_exits_0() {
    // The decoder messages differ (Go `encoding/json` vs `serde_json`), the
    // error type and the exit path are the same.
    let sides = check(
        Case::new("bad_json")
            .loose("<bin> error: *json.SyntaxError:")
            .setup(|api| {
                api.get(
                    &issue_path(REPO, 1),
                    vec![Scripted::raw(
                        200,
                        "application/json; charset=utf-8",
                        "{not json",
                    )],
                )
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(0, "<bin> error: *json.SyntaxError:");
        s.expect_contains(0, ", non fatal, exiting 0 status");
    });
}

#[test]
fn dead_api_aborts_when_wait_is_too_long() {
    let sides = check(
        Case::new("dead_api")
            .dead_api()
            .env("GHA2DB_MAX_GHAPI_WAIT", "3"),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_line(
            0,
            "GetRateLimit(0): Get \"http://127.0.0.1:1/rate_limit\": dial tcp 127.0.0.1:1: connect: connection refused",
        );
        assert_eq!(
            s.errors(0),
            vec!["Error: 'API limit reached while getting issue data, aborting, don't want to wait <dur>'".to_string()]
        );
        assert_eq!(s.requests(), Vec::<String>::new());
    });
}

#[test]
fn dead_api_waits_then_gives_up() {
    // -1 points → wait 5s (+1s) once, then the single retry is exhausted.
    let sides = check(
        Case::new("dead_api_wait")
            .dead_api()
            .env("GHA2DB_MAX_GHAPI_RETRY", "1"),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_line(
            0,
            "API limit reached while getting issue data, waiting <dur> (0)",
        );
        assert_eq!(
            s.errors(0),
            vec![
                "Error: 'GetRateLimit call failed 1 times while getting issue, aborting'"
                    .to_string()
            ]
        );
    });
}

// ---------------------------------------------------------------------------
// Scenarios: rate limits and tokens
// ---------------------------------------------------------------------------

#[test]
fn rate_limit_endpoint_403_is_waited_out() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(Case::new("rl_403").setup(move |api| {
        api.get(
            "/rate_limit",
            vec![
                Scripted::rate_limited(5000, 2),
                Scripted::ok(&rate_json(5000, 4999, 3600)),
            ],
        );
        api.get_ok(&issue_path(REPO, 1), &issue.json());
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "Parsed wait time from error message: <dur>");
        s.expect_line(
            0,
            "API limit reached while getting issue data, waiting <dur> (0)",
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
    });
}

#[test]
fn rate_limit_endpoint_without_resources_aborts() {
    let sides = check(
        Case::new("rl_empty")
            .env("GHA2DB_MAX_GHAPI_WAIT", "3")
            .setup(|api| {
                api.get("/rate_limit", vec![Scripted::ok(&json!({}))]);
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_no_prefix(0, "GetRateLimit(");
        assert_eq!(
            s.errors(0),
            vec!["Error: 'API limit reached while getting issue data, aborting, don't want to wait <dur>'".to_string()]
        );
    });
}

#[test]
fn rate_limit_endpoint_500_aborts() {
    let sides = check(
        Case::new("rl_500")
            .env("GHA2DB_MAX_GHAPI_WAIT", "3")
            .setup(|api| {
                api.get("/rate_limit", vec![Scripted::error(500, "boom")]);
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        s.expect_line(0, "GetRateLimit(0): GET <api>/rate_limit: 500 boom []");
        assert_eq!(
            s.errors(0),
            vec!["Error: 'API limit reached while getting issue data, aborting, don't want to wait <dur>'".to_string()]
        );
    });
}

#[test]
fn issue_get_rate_limited_is_retried() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(Case::new("issue_rl").setup(move |api| {
        api.get(
            &issue_path(REPO, 1),
            vec![
                Scripted::rate_limited(5000, 3600),
                Scripted::ok(&issue.json()),
            ],
        )
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, &format!("Rate limit (Issues.Get) for {}", empty_cfg()));
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
        let reqs = s.requests();
        assert_eq!(
            reqs.iter()
                .filter(|r| r.starts_with(&format!("GET /repos/{REPO}/issues/1 ")))
                .count(),
            2
        );
    });
}

#[test]
fn issue_get_always_rate_limited_aborts() {
    let sides = check(Case::new("issue_rl_always").setup(|api| {
        api.get(
            &issue_path(REPO, 1),
            vec![Scripted::rate_limited(5000, 3600)],
        )
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        assert_eq!(s.count_prefix(0, "Rate limit (Issues.Get) for "), 6);
        assert_eq!(
            s.errors(0),
            vec![
                "Error: 'GetRateLimit call failed 6 times while getting issue, aborting'"
                    .to_string()
            ]
        );
        let reqs = s.requests();
        assert_eq!(
            reqs.iter()
                .filter(|r| r.starts_with(&format!("GET /repos/{REPO}/issues/1 ")))
                .count(),
            6
        );
        assert_eq!(
            reqs.iter()
                .filter(|r| r.starts_with("GET /rate_limit "))
                .count(),
            6
        );
    });
}

#[test]
fn abuse_is_waited_out_with_github_debug() {
    let issue = IssueSpec::new(101, 1).labels(vec![(7, "bug")]);
    let sides = check(
        Case::new("abuse")
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .setup(move |api| {
                api.get(
                    &issue_path(REPO, 1),
                    vec![Scripted::abuse(None), Scripted::ok(&issue.json())],
                )
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            &format!("Abuse detected (Issues.Get) for {}", empty_cfg()),
        );
        s.expect_line(0, "GitHub API abuse detected (issue), wait <dur>");
        assert!(s.outs[0]
            .stdout_str()
            .contains("GitHub API abuse detected (issue), wait 8s\n"));
        // single-threaded: the threads limit is already 1/1
        s.expect_no_prefix(0, "Lower threads limit");
        s.expect_no_prefix(0, "Rise threads limit");
        s.expect_line(
            0,
            "Get Issue Try: 0, rem: [4999], waitPeriod: [<dur>], hint: 0",
        );
        s.expect_line(
            0,
            "Get Issue Try: 1, rem: [4999], waitPeriod: [<dur>], hint: 0",
        );
        s.expect_line(
            0,
            &format!("API call for Issue {REPO} 1, remaining GHAPI points [4999], hint: 0"),
        );
        s.expect_line(
            0,
            "GetRateLimits: hint: 0, limits: [5000], remaining: [4999], reset: [<dur>]",
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
    });
}

#[test]
fn min_points_threshold_waits_for_reset() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("min_points_wait")
            .env("GHA2DB_MIN_GHAPI_POINTS", "100")
            .setup(move |api| {
                api.get(
                    "/rate_limit",
                    vec![
                        Scripted::ok(&rate_json(5000, 50, 2)),
                        Scripted::ok(&rate_json(5000, 4999, 3600)),
                    ],
                );
                api.get_ok(&issue_path(REPO, 1), &issue.json());
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            "API limit reached while getting issue data, waiting <dur> (0)",
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
    });
}

#[test]
fn min_points_threshold_aborts_on_long_wait() {
    let sides = check(
        Case::new("min_points_abort")
            .env("GHA2DB_MIN_GHAPI_POINTS", "100")
            .setup(|api| {
                api.get(
                    "/rate_limit",
                    vec![Scripted::ok(&rate_json(5000, 50, 3600))],
                );
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(2));
        assert_eq!(
            s.errors(0),
            vec!["Error: 'API limit reached while getting issue data, aborting, don't want to wait <dur>'".to_string()]
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
    });
}

#[test]
fn hint_prefers_the_token_with_more_points() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("two_tokens")
            .oauth(Some("tok1,tok2"))
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .setup(move |api| {
                api.set_rate(Some("tok1"), 5000, 100, 3600);
                api.set_rate(Some("tok2"), 5000, 4000, 3600);
                api.get_ok(&issue_path(REPO, 1), &issue.json());
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "GetRateLimits: hint: 1, limits: [5000 5000], remaining: [100 4000], reset: [<dur> <dur>]");
        s.expect_line(
            0,
            &format!("API call for Issue {REPO} 1, remaining GHAPI points [100 4000], hint: 1"),
        );
        let reqs = s.requests();
        assert!(
            reqs.contains(&format!(
                "GET /repos/{REPO}/issues/1 accept={ISSUES_ACCEPT} auth=tok2"
            )),
            "{reqs:?}"
        );
        assert!(
            !reqs
                .iter()
                .any(|r| r.starts_with("GET /repos/") && r.ends_with("auth=tok1")),
            "{reqs:?}"
        );
        assert_eq!(
            reqs.iter()
                .filter(|r| r == &&format!("GET /rate_limit accept={V3_ACCEPT} auth=tok1"))
                .count(),
            4
        );
        assert_eq!(
            reqs.iter()
                .filter(|r| r == &&format!("GET /rate_limit accept={V3_ACCEPT} auth=tok2"))
                .count(),
            4
        );
    });
}

#[test]
fn hint_tie_prefers_the_shorter_reset() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("two_tokens_tie")
            .oauth(Some("tok1,tok2"))
            .env("GHA2DB_GITHUB_DEBUG", "1")
            .setup(move |api| {
                api.set_rate(Some("tok1"), 5000, 4000, 3600);
                api.set_rate(Some("tok2"), 5000, 4000, 600);
                api.get_ok(&issue_path(REPO, 1), &issue.json());
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(
            0,
            "GetRateLimits: hint: 1, limits: [5000 5000], remaining: [4000 4000], reset: ",
        );
        let reqs = s.requests();
        assert!(
            reqs.contains(&format!(
                "GET /repos/{REPO}/issues/1 accept={ISSUES_ACCEPT} auth=tok2"
            )),
            "{reqs:?}"
        );
    });
}

#[test]
fn oauth_tokens_can_come_from_a_file() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("oauth_file")
            .oauth(Some("@filetok"))
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        let reqs = s.requests();
        assert!(reqs.iter().all(|r| r.ends_with("auth=filetok")), "{reqs:?}");
        assert_eq!(reqs.len(), 5);
    });
}

#[test]
fn oauth_dash_is_anonymous() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(Case::new("oauth_dash").oauth(Some("-")).setup(move |api| {
        api.set_rate(None, 60, 59, 3600);
        api.get_ok(&issue_path(REPO, 1), &issue.json())
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        let reqs = s.requests();
        assert!(reqs.iter().all(|r| r.ends_with("auth=-")), "{reqs:?}");
        assert_eq!(s.count("select count(*) from gha_issues"), 1);
    });
}

#[test]
fn oauth_unset_falls_back_to_anonymous() {
    // Only meaningful when the machine has no `/etc/github/oauth(s)` files
    // (the binaries would read real tokens from them otherwise).
    if Path::new("/etc/github/oauths").exists() || Path::new("/etc/github/oauth").exists() {
        eprintln!("skipping: /etc/github/oauth(s) exists on this machine");
        return;
    }
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("oauth_unset")
            .oauth(None)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        let reqs = s.requests();
        assert!(reqs.iter().all(|r| r.ends_with("auth=-")), "{reqs:?}");
    });
}

// ---------------------------------------------------------------------------
// Scenarios: flags, hiding, several repos, multi-threading
// ---------------------------------------------------------------------------

#[test]
fn skip_pdb_writes_nothing() {
    let issue = IssueSpec::new(101, 1);
    let sides = check(
        Case::new("skip_pdb")
            .env("GHA2DB_SKIPPDB", "1")
            .env("GHA2DB_DEBUG", "1")
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(
            0,
            &format!("No DB write: Issue '{{Repo: {REPO}, Number: 1, IssueID: 101, "),
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 0);
        assert_eq!(s.count("select count(*) from gha_events"), 1);
    });
}

#[test]
fn debug_level_2_prints_state_details() {
    let before = IssueSpec::new(101, 1);
    let after = before
        .clone()
        .state("closed")
        .closed_at(Some("2020-05-05T05:05:05Z"))
        .labels(vec![(7, "bug")]);
    let mut seed = seed_event(1001, REPO, 500, (11, "alice"), "2020-03-02 13:30:00");
    seed.push_str(&seed_issue(&before, 1001, "2020-03-02 13:30:00"));
    let api_issue = after.clone();
    let sides = check(
        Case::new("debug2")
            .env("GHA2DB_DEBUG", "2")
            .seed(&seed)
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &api_issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_prefix(
            0,
            &format!("Sorted: [{{Repo: {REPO}, Number: 1, IssueID: 101, "),
        );
        s.expect_prefix(
            0,
            &format!("GHA Issue ID '101' --> '{{Repo: {REPO}, Number: 1, "),
        );
        s.expect_contains(0, "' state open -> closed");
        s.expect_contains(0, "' closed_at null -> 2020-05-05 05:05:05");
        s.expect_contains(0, "' labels to '7', they were: '' (event_id 1001)");
        s.expect_prefix(
            0,
            "Previous event (event_id: 1001), added artificial: true: '",
        );
        assert_eq!(s.count("select count(*) from gha_issues"), 2);
    });
}

#[test]
fn hidden_actors_are_anonymised() {
    let issue = IssueSpec::new(101, 1)
        .assignee(Some((11, "alice")))
        .assignees(vec![(11, "alice"), (12, "bob")]);
    let sides = check(
        Case::new("hide")
            .hide(&format!("sha1\n{ALICE_SHA1}\n"))
            .setup(move |api| api.get_ok(&issue_path(REPO, 1), &issue.json())),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        let anon = format!("anon-{ALICE_SHA1}");
        assert_eq!(
            s.column("select dup_user_login from gha_issues"),
            vec![anon.clone()]
        );
        // (11, 'alice') is seeded; the hidden login is a second row of id 11.
        assert_eq!(
            s.column("select login from gha_actors order by id, login"),
            vec![
                "devstats-sync".to_string(),
                "alice".to_string(),
                anon.clone(),
                "bob".to_string()
            ]
        );
    });
}

#[test]
fn several_repos_without_events_get_repo_id_minus_one() {
    let a = IssueSpec::new(101, 1);
    let mut b = IssueSpec::new(301, 3);
    b.repo = "other/thing";
    let sides = check(
        Case::new("two_repos")
            .issues(&[(REPO, 1), ("other/thing", 3)])
            .setup(move |api| {
                api.get_ok(&issue_path(REPO, 1), &a.json());
                api.get_ok(&issue_path("other/thing", 3), &b.json());
            }),
    );
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(0, "sync_issues.go: Processing 2 issues - GHAPI part");
        assert_eq!(
            s.query("select dup_repo_name, dup_repo_id::text from gha_issues order by id"),
            vec![
                vec![REPO.to_string(), "500".to_string()],
                vec!["other/thing".to_string(), "-1".to_string()],
            ]
        );
        assert_eq!(
            s.query("select dup_repo_name, repo_id::text from gha_events where id > 1000 order by dup_repo_name"),
            vec![
                vec![REPO.to_string(), "500".to_string()],
                vec!["other/thing".to_string(), "-1".to_string()],
            ]
        );
    });
}

#[test]
fn multi_threaded_run_processes_all_issues() {
    let n = 12;
    let mut list: Vec<(&str, i64)> = Vec::new();
    let mut specs = Vec::new();
    for i in 1..=n {
        list.push((REPO, i));
        let mut spec = IssueSpec::new(100 + i, i);
        if i % 3 == 0 {
            spec = spec.pull_request(true);
        }
        if i % 4 == 0 {
            spec = spec.labels(vec![(7, "bug"), (i, "num")]);
        }
        specs.push(spec);
    }
    let prs: Vec<PrSpec> = (1..=n)
        .filter(|i| i % 3 == 0)
        .map(|i| PrSpec::new(9000 + i, i))
        .collect();
    let sides = check(Case::new("mt").mt().issues(&list).setup(move |api| {
        for spec in &specs {
            api.get_ok(&issue_path(REPO, spec.number), &spec.json());
        }
        for pr in &prs {
            api.get_ok(&pr_path(REPO, pr.number), &pr.json());
        }
    }));
    both(&sides, |s| {
        assert_eq!(s.code(0), Some(0));
        s.expect_line(
            0,
            &format!("sync_issues.go: Processing {n} issues - GHAPI part"),
        );
        assert_eq!(s.count("select count(*) from gha_issues"), n);
        assert_eq!(s.count("select count(*) from gha_pull_requests"), n / 3);
        assert_eq!(
            s.count("select count(*) from gha_issues_labels"),
            2 * (n / 4)
        );
        // Colliding artificial event ids (see `Case::compare_data`) may
        // reduce the number of distinct events/payloads (both keyed by the
        // event id); issues and PRs are keyed by (id, event_id).
        let events = s.count("select count(*) from gha_events");
        assert!(events > 1 && events <= n + 1, "{events}");
        assert_eq!(s.count("select count(*) from gha_payloads"), events - 1);
    });
}
