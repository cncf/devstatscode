//! Port of `cmd/ghapi2db/heartbeat.go`: the repository scope of the API
//! passes (every tracked repository of `gha_repos`, one current name per id)
//! and the GraphQL heartbeat that tells, per pass, which repositories had
//! matching activity since the recent date.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{mpsc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Local, Timelike, Utc};
use devstatscode::ghapi::{fmt_slice, get_rate_limits, get_tracked_repos};
use devstatscode::github::{GoTime, Repository};
use devstatscode::pg::api::{
    exec_sql_with_err, insert_ignore, n_value, n_values, query_sql_with_err, trunc_to_bytes,
};
use devstatscode::pg::{PgConn, SqlArg};
use devstatscode::project_filter::{new_project_filter, read_projects_if_present, ProjectFilter};
use devstatscode::threads::get_threads_num;
use devstatscode::time::{progress_info, wall_as_utc};
use devstatscode::{fatal_on_err, fatalf, printf, Ctx};
use serde::Deserialize;

use crate::restore::{
    api_page, gh_graphql_post, gh_tokens, go_json_error, page_failed, repo_ids, GRAPHQL_NO_RETRY,
};
use crate::{db_time, get_api_params};

/// Go `apiPass`: the ghapi2db passes that work on a list of repositories.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ApiPass {
    Events,
    Commits,
    Comments,
    Reviews,
    Forks,
    Releases,
    Stars,
    RepoStats,
    RepoEvents,
    IssuesPrs,
}

impl ApiPass {
    /// Pass name used in the log lines (the restore passes use the same names).
    pub fn label(self) -> &'static str {
        match self {
            ApiPass::Events => "ghapi2db events",
            ApiPass::Commits => "ghapi2db commits",
            ApiPass::Comments => "ghapi2db comments restore",
            ApiPass::Reviews => "ghapi2db reviews restore",
            ApiPass::Forks => "ghapi2db forks restore",
            ApiPass::Releases => "ghapi2db releases restore",
            ApiPass::Stars => "ghapi2db stars restore",
            ApiPass::RepoStats => "ghapi2db repo stats",
            ApiPass::RepoEvents => "ghapi2db repo events",
            ApiPass::IssuesPrs => "ghapi2db issues prs",
        }
    }

    /// What the heartbeat must show since the recent date for the pass to
    /// process a repository.
    fn gate(self) -> &'static str {
        match self {
            ApiPass::Events => "issue or PR updates",
            ApiPass::Commits => "pushes",
            ApiPass::Comments => "issue or PR updates or pushes",
            ApiPass::Reviews => "PR updates",
            ApiPass::Forks => "forks",
            ApiPass::Releases => "releases",
            ApiPass::Stars => "star changes",
            ApiPass::RepoStats => "repository data",
            ApiPass::RepoEvents => "activity",
            ApiPass::IssuesPrs => "repository data",
        }
    }
}

/// Repositories per GraphQL heartbeat query: 100 exceeds GitHub's per-query
/// node limit for this selection (`RESOURCE_LIMITS_EXCEEDED`), 50 costs 2
/// rate-limit points.
const HEARTBEAT_BATCH: usize = 50;

const HEARTBEAT_FRAGMENT: &str = "fragment F on Repository { databaseId nameWithOwner isArchived pushedAt stargazerCount forkCount watchers { totalCount } \
owner { __typename login ... on User { databaseId } ... on Organization { databaseId } } \
openIssues: issues(states: OPEN) { totalCount } openPRs: pullRequests(states: OPEN) { totalCount } \
issues(last: 1, orderBy: {field: UPDATED_AT, direction: ASC}) { nodes { updatedAt } } \
pullRequests(last: 1, orderBy: {field: UPDATED_AT, direction: ASC}) { nodes { updatedAt } } \
releases(last: 1, orderBy: {field: CREATED_AT, direction: ASC}) { nodes { createdAt publishedAt } } \
forks(last: 1, orderBy: {field: CREATED_AT, direction: ASC}) { nodes { createdAt } } }";

/// Go `repoHeartbeat`: what one GraphQL heartbeat query told about a repository.
#[derive(Clone, Debug, Default)]
pub struct RepoHeartbeat {
    pub found: bool,
    pub not_found: bool,
    /// resolves to a different repository id than the tracked one
    pub moved: bool,
    /// heartbeat failed: treated as active in every pass
    pub unknown: bool,
    pub archived: bool,
    /// GitHub's current name is in the project's `GHA2DB_EXCLUDE_REPOS`: no pass processes it
    pub excluded: bool,
    /// not found: the GraphQL error type (NOT_FOUND, FORBIDDEN, ...)
    pub reason: String,
    pub database_id: i64,
    pub name_with_owner: String,
    pub owner_id: i64,
    pub owner_login: String,
    pub owner_is_org: bool,
    pub pushed_at: Option<DateTime<Utc>>,
    pub issue_at: Option<DateTime<Utc>>,
    pub pr_at: Option<DateTime<Utc>>,
    pub release_at: Option<DateTime<Utc>>,
    pub fork_at: Option<DateTime<Utc>>,
    pub stargazer_count: i64,
    pub fork_count: i64,
    pub watchers: i64,
    pub open_issues: i64,
    pub open_prs: i64,
    /// newest `gha_forkees.stargazers_count` at or before the recent date
    pub star_snapshot: Option<i64>,
}

fn since(t: Option<DateTime<Utc>>, dt: DateTime<Utc>) -> bool {
    matches!(t, Some(t) if t >= dt)
}

impl RepoHeartbeat {
    fn unknown() -> Self {
        RepoHeartbeat {
            unknown: true,
            ..Default::default()
        }
    }

    /// Should the pass process the repository.
    pub fn active(&self, pass: ApiPass, recent_dt: DateTime<Utc>) -> bool {
        if self.unknown {
            return true;
        }
        if self.not_found || self.moved || self.excluded {
            return false;
        }
        match pass {
            ApiPass::Events => since(self.issue_at, recent_dt) || since(self.pr_at, recent_dt),
            ApiPass::Commits => since(self.pushed_at, recent_dt),
            ApiPass::Comments => {
                since(self.issue_at, recent_dt)
                    || since(self.pr_at, recent_dt)
                    || since(self.pushed_at, recent_dt)
            }
            ApiPass::Reviews => since(self.pr_at, recent_dt),
            ApiPass::Forks => since(self.fork_at, recent_dt),
            ApiPass::Releases => since(self.release_at, recent_dt),
            ApiPass::Stars => {
                self.stargazer_count > 0 && self.star_snapshot != Some(self.stargazer_count)
            }
            ApiPass::RepoStats => true,
            // the events feed carries every event type: any signal the heartbeat has
            ApiPass::RepoEvents => {
                since(self.issue_at, recent_dt)
                    || since(self.pr_at, recent_dt)
                    || since(self.pushed_at, recent_dt)
                    || since(self.fork_at, recent_dt)
                    || since(self.release_at, recent_dt)
                    || self.active(ApiPass::Stars, recent_dt)
            }
            // the stub sweep is database-driven (a repository without stub rows costs one query),
            // the listing part gates itself on issue or PR updates
            ApiPass::IssuesPrs => true,
        }
    }
}

/// Go `repoScope`: the repositories ghapi2db works on (computed once per process).
#[derive(Default)]
struct RepoScope {
    /// current names, sorted
    repos: Vec<String>,
    /// tracked ids per current name, ascending
    ids: BTreeMap<String, Vec<i64>>,
    /// `None` until the first gated pass
    heartbeat: Option<BTreeMap<String, RepoHeartbeat>>,
    /// recent date the heartbeat was evaluated against
    recent_dt: DateTime<Utc>,
    /// renamed repositories: GitHub's current name -> the database name it replaced
    db_names: BTreeMap<String, String>,
    /// repositories renamed out of the project's rules (warned about once per run)
    untracked: BTreeSet<String>,
}

static SCOPE: OnceLock<Mutex<Option<RepoScope>>> = OnceLock::new();

fn scope_cell() -> &'static Mutex<Option<RepoScope>> {
    SCOPE.get_or_init(|| Mutex::new(None))
}

impl RepoScope {
    /// The id a name is reported under when it resolves elsewhere (the newest one).
    fn tracked_id(&self, name: &str) -> i64 {
        self.ids
            .get(name)
            .and_then(|ids| ids.last().copied())
            .unwrap_or(0)
    }

    fn tracks(&self, name: &str, id: i64) -> bool {
        self.ids
            .get(name)
            .map(|ids| ids.contains(&id))
            .unwrap_or(false)
    }

    /// Go `getRepoScope`: all tracked repositories (one current name per id).
    fn load(c: &PgConn, ctx: &Ctx) -> RepoScope {
        let (repos, ids, historical) = get_tracked_repos(c, ctx);
        let n_ids = ids
            .values()
            .flatten()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        printf!(
            "ghapi2db scope: {} repos from gha_repos ({} ids), {} historical names skipped\n",
            repos.len(),
            n_ids,
            historical.len()
        );
        if ctx.debug > 0 {
            printf!("Repos to process (all tracked): {}\n", fmt_slice(&repos));
            printf!("Historical names skipped: {}\n", fmt_slice(&historical));
        }
        RepoScope {
            repos,
            ids,
            heartbeat: None,
            recent_dt: DateTime::<Utc>::default(),
            db_names: BTreeMap::new(),
            untracked: BTreeSet::new(),
        }
    }
}

/// Go `wellFormed`: `org/repo` with the characters GitHub allows, safe to
/// embed in a GraphQL query.
fn well_formed(org_repo: &str) -> Option<(&str, &str)> {
    let (org, repo) = org_repo.split_once('/')?;
    if org.is_empty() || repo.is_empty() || repo.contains('/') {
        return None;
    }
    let ok = |s: &str| {
        s.chars()
            .all(|r| r.is_ascii_alphanumeric() || r == '-' || r == '_' || r == '.')
    };
    if ok(org) && ok(repo) {
        Some((org, repo))
    } else {
        None
    }
}

/// Go `heartbeatQuery`: one GraphQL query for a batch of repositories aliased `r0..rN`.
fn heartbeat_query(batch: &[String]) -> Vec<u8> {
    let mut q = String::from("query { rateLimit { cost remaining }");
    for (i, org_repo) in batch.iter().enumerate() {
        let (org, repo) = well_formed(org_repo).unwrap_or(("", ""));
        q.push_str(&format!(
            " r{i}: repository(owner: \"{org}\", name: \"{repo}\") {{ ...F }}"
        ));
    }
    q.push_str(" } ");
    q.push_str(HEARTBEAT_FRAGMENT);
    let mut payload: BTreeMap<&str, String> = BTreeMap::new();
    payload.insert("query", q);
    fatal_on_err(serde_json::to_vec(&payload).map_err(|e| e.to_string()))
}

/// Go `encoding/json` semantics: `null` leaves the field at its zero value.
fn nd<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HbCount {
    #[serde(rename = "totalCount", deserialize_with = "nd")]
    total_count: i64,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HbUpdated {
    #[serde(rename = "updatedAt")]
    updated_at: Option<GoTime>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HbCreated {
    #[serde(rename = "createdAt")]
    created_at: Option<GoTime>,
    #[serde(rename = "publishedAt")]
    published_at: Option<GoTime>,
}

#[derive(Deserialize)]
#[serde(default, bound(deserialize = "T: Deserialize<'de>"))]
struct HbNodes<T> {
    #[serde(deserialize_with = "nd")]
    nodes: Vec<T>,
}

impl<T> Default for HbNodes<T> {
    fn default() -> Self {
        HbNodes { nodes: Vec::new() }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HbOwner {
    #[serde(rename = "__typename", deserialize_with = "nd")]
    type_name: String,
    #[serde(deserialize_with = "nd")]
    login: String,
    #[serde(rename = "databaseId", deserialize_with = "nd")]
    database_id: i64,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HeartbeatNode {
    #[serde(rename = "databaseId", deserialize_with = "nd")]
    database_id: i64,
    #[serde(rename = "nameWithOwner", deserialize_with = "nd")]
    name_with_owner: String,
    #[serde(rename = "isArchived", deserialize_with = "nd")]
    is_archived: bool,
    #[serde(rename = "pushedAt")]
    pushed_at: Option<GoTime>,
    #[serde(rename = "stargazerCount", deserialize_with = "nd")]
    stargazer_count: i64,
    #[serde(rename = "forkCount", deserialize_with = "nd")]
    fork_count: i64,
    #[serde(deserialize_with = "nd")]
    watchers: HbCount,
    #[serde(deserialize_with = "nd")]
    owner: HbOwner,
    #[serde(rename = "openIssues", deserialize_with = "nd")]
    open_issues: HbCount,
    #[serde(rename = "openPRs", deserialize_with = "nd")]
    open_prs: HbCount,
    #[serde(deserialize_with = "nd")]
    issues: HbNodes<HbUpdated>,
    #[serde(rename = "pullRequests", deserialize_with = "nd")]
    pull_requests: HbNodes<HbUpdated>,
    #[serde(deserialize_with = "nd")]
    releases: HbNodes<HbCreated>,
    #[serde(deserialize_with = "nd")]
    forks: HbNodes<HbCreated>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HeartbeatError {
    #[serde(rename = "type", deserialize_with = "nd")]
    type_: String,
    #[serde(deserialize_with = "nd")]
    message: String,
    #[serde(deserialize_with = "nd")]
    path: Vec<serde_json::Value>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HeartbeatResponse {
    data: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(deserialize_with = "nd")]
    errors: Vec<HeartbeatError>,
}

/// Go `decodeHeartbeat`: the heartbeat of every repository in the batch
/// (`Err`: batch failed, retry with another token unless the query itself was rejected).
fn decode_heartbeat(
    body: &[u8],
    batch: &[String],
    token_idx: usize,
    n_tokens: usize,
) -> Result<Vec<RepoHeartbeat>, String> {
    let out: HeartbeatResponse = serde_json::from_slice(body).map_err(|e| go_json_error(&e))?;
    let mut per_alias: BTreeMap<String, String> = BTreeMap::new();
    for e in &out.errors {
        if e.type_ == "RESOURCE_LIMITS_EXCEEDED"
            || e.message.contains("exceeds the maximum node limit")
        {
            return Err(format!("{GRAPHQL_NO_RETRY}: {}", e.message));
        }
        if e.path.len() == 1 {
            if let Some(alias) = e.path[0].as_str() {
                per_alias.insert(
                    alias.to_string(),
                    if e.type_.is_empty() {
                        e.message.clone()
                    } else {
                        e.type_.clone()
                    },
                );
                continue;
            }
        }
        return Err(format!(
            "graphql (token {}/{}): {}",
            token_idx + 1,
            n_tokens,
            e.message
        ));
    }
    let data = match out.data {
        Some(d) => d,
        None => {
            return Err(format!(
                "graphql (token {}/{}): no data",
                token_idx + 1,
                n_tokens
            ))
        }
    };
    let mut hbs = Vec::with_capacity(batch.len());
    for i in 0..batch.len() {
        let alias = format!("r{i}");
        let mut hb = RepoHeartbeat::default();
        let raw = match data.get(&alias) {
            Some(v) if !v.is_null() => v,
            _ => {
                hb.not_found = true;
                hb.reason = per_alias
                    .get(&alias)
                    .cloned()
                    .unwrap_or_else(|| "null".to_string());
                hbs.push(hb);
                continue;
            }
        };
        let node: HeartbeatNode =
            serde_json::from_value(raw.clone()).map_err(|e| go_json_error(&e))?;
        hb.found = true;
        hb.database_id = node.database_id;
        hb.name_with_owner = node.name_with_owner;
        hb.owner_id = node.owner.database_id;
        hb.owner_is_org = node.owner.type_name == "Organization";
        hb.owner_login = node.owner.login;
        hb.archived = node.is_archived;
        hb.pushed_at = node.pushed_at.map(|t| t.utc());
        hb.stargazer_count = node.stargazer_count;
        hb.fork_count = node.fork_count;
        hb.watchers = node.watchers.total_count;
        hb.open_issues = node.open_issues.total_count;
        hb.open_prs = node.open_prs.total_count;
        let utc = |t: &Option<GoTime>| t.map(|t| t.utc());
        hb.issue_at = node.issues.nodes.first().and_then(|n| utc(&n.updated_at));
        hb.pr_at = node
            .pull_requests
            .nodes
            .first()
            .and_then(|n| utc(&n.updated_at));
        if let Some(rel) = node.releases.nodes.first() {
            hb.release_at = utc(&rel.created_at);
            if let Some(p) = utc(&rel.published_at) {
                if hb.release_at.map(|c| p > c).unwrap_or(true) {
                    hb.release_at = Some(p);
                }
            }
        }
        hb.fork_at = node.forks.nodes.first().and_then(|n| utc(&n.created_at));
        hbs.push(hb);
    }
    Ok(hbs)
}

/// Go `heartbeatBatchQuery`: heartbeat of one batch, halving it when GitHub
/// rejects the query size. Returns the heartbeats (`None`: GitHub could not
/// tell) and the number of GraphQL queries made.
fn heartbeat_batch_query(
    ctx: &Ctx,
    tokens: &[String],
    batch: &[String],
    start: usize,
    warnings: &Mutex<Vec<String>>,
) -> (Vec<Option<RepoHeartbeat>>, usize) {
    if batch.is_empty() {
        return (Vec::new(), 0);
    }
    let got: Mutex<Vec<RepoHeartbeat>> = Mutex::new(Vec::new());
    let err = gh_graphql_post(
        ctx,
        tokens,
        start,
        "ghapi2db heartbeat",
        &heartbeat_query(batch),
        &|body, token_idx| {
            let hbs = decode_heartbeat(body, batch, token_idx, tokens.len())?;
            *got.lock().unwrap_or_else(|p| p.into_inner()) = hbs;
            Ok(())
        },
    );
    let n_queries = 1;
    let err = match err {
        Ok(()) => {
            let hbs = got.into_inner().unwrap_or_else(|p| p.into_inner());
            return (hbs.into_iter().map(Some).collect(), n_queries);
        }
        Err(e) => e,
    };
    if err.starts_with(GRAPHQL_NO_RETRY) && batch.len() > 1 {
        let half = batch.len() / 2;
        let (mut left, nl) = heartbeat_batch_query(ctx, tokens, &batch[..half], start, warnings);
        let (right, nr) = heartbeat_batch_query(ctx, tokens, &batch[half..], start + 1, warnings);
        left.extend(right);
        return (left, n_queries + nl + nr);
    }
    warnings
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(format!(
            "WARNING: ghapi2db heartbeat: {} repos unknown ({}), processing them in every pass\n",
            batch.len(),
            err
        ));
    (vec![None; batch.len()], n_queries)
}

/// Go `starSnapshot`: newest `gha_forkees.stargazers_count` of the
/// repository at or before the recent date. The snapshot time is
/// `updated_at` (the repository data time, what `watchers_by_alias.sql`
/// uses too): the counters-less rows GH Archive writes since 2024-09
/// (`updated_at` 0001-01-01) sort last.
fn star_snapshot(c: &PgConn, ctx: &Ctx, repo_id: i64, recent_dt: DateTime<Utc>) -> Option<i64> {
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!(
            "select stargazers_count from gha_forkees where id = {} and updated_at <= {} \
             order by updated_at desc, event_id desc limit 1",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::Int(repo_id), SqlArg::from(recent_dt)],
    );
    let mut snapshot: Option<i64> = None;
    while rows.next() {
        let mut cnt: Option<i64> = None;
        fatal_on_err(rows.scan(&mut [&mut cnt]));
        if cnt.is_some() {
            snapshot = cnt;
        }
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    snapshot
}

/// Go `repoScope.runHeartbeat`: ask GitHub (GraphQL, batches of
/// repositories) what happened in every tracked repository since the recent
/// date; evaluated once per process by the first gated pass.
fn run_heartbeat(scope: &mut RepoScope, ctx: &mut Ctx, c: &PgConn, recent_dt: DateTime<Utc>) {
    if scope.heartbeat.is_some() {
        return;
    }
    let mut heartbeat: BTreeMap<String, RepoHeartbeat> = BTreeMap::new();
    scope.recent_dt = recent_dt;
    let tokens = gh_tokens(ctx);
    let mut queried: Vec<String> = Vec::new();
    let mut malformed: Vec<String> = Vec::new();
    for org_repo in &scope.repos {
        if well_formed(org_repo).is_some() {
            queried.push(org_repo.clone());
        } else {
            malformed.push(org_repo.clone());
            heartbeat.insert(
                org_repo.clone(),
                RepoHeartbeat {
                    not_found: true,
                    reason: "malformed name".to_string(),
                    ..Default::default()
                },
            );
        }
    }
    if tokens.is_empty() {
        printf!("WARNING: ghapi2db heartbeat needs GHA2DB_GITHUB_OAUTH token(s), processing every repo in every pass\n");
        for org_repo in &queried {
            heartbeat.insert(org_repo.clone(), RepoHeartbeat::unknown());
        }
        scope.heartbeat = Some(heartbeat);
        return;
    }
    let batches: Vec<&[String]> = queried.chunks(HEARTBEAT_BATCH).collect();
    let thr_n = get_threads_num(ctx);
    let ctx: &Ctx = ctx;
    let warnings: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let mut results: Vec<Vec<Option<RepoHeartbeat>>> = vec![Vec::new(); batches.len()];
    let mut n_queries = 0usize;
    if thr_n > 1 {
        std::thread::scope(|s| {
            let (tx, rx) = mpsc::channel::<(usize, Vec<Option<RepoHeartbeat>>, usize)>();
            let mut n_threads = 0usize;
            for (i, batch) in batches.iter().enumerate() {
                let tx = tx.clone();
                let tokens = &tokens;
                let warnings = &warnings;
                s.spawn(move || {
                    let (hbs, n) = heartbeat_batch_query(ctx, tokens, batch, i, warnings);
                    let _ = tx.send((i, hbs, n));
                });
                n_threads += 1;
                if n_threads >= thr_n {
                    let (j, hbs, n) = rx.recv().expect("heartbeat worker");
                    results[j] = hbs;
                    n_queries += n;
                    n_threads -= 1;
                }
            }
            while n_threads > 0 {
                let (j, hbs, n) = rx.recv().expect("heartbeat worker");
                results[j] = hbs;
                n_queries += n;
                n_threads -= 1;
            }
        });
    } else {
        for (i, batch) in batches.iter().enumerate() {
            let (hbs, n) = heartbeat_batch_query(ctx, &tokens, batch, i, &warnings);
            results[i] = hbs;
            n_queries += n;
        }
    }
    let mut warnings = warnings.into_inner().unwrap_or_else(|p| p.into_inner());
    warnings.sort();
    for msg in &warnings {
        printf!("{}", msg);
    }
    let (mut found, mut not_found, mut moved, mut unknown, mut archived) = (0, 0, 0, 0, 0);
    let (mut pushes, mut issues, mut prs, mut forks, mut releases, mut stars) = (0, 0, 0, 0, 0, 0);
    let mut lines: Vec<String> = Vec::new();
    not_found += malformed.len();
    if ctx.debug > 0 {
        for org_repo in &malformed {
            lines.push(format!(
                "{}: not found on GitHub (malformed name), skipping\n",
                org_repo
            ));
        }
    }
    for (i, batch) in batches.iter().enumerate() {
        for (j, org_repo) in batch.iter().enumerate() {
            let mut hb = results[i]
                .get(j)
                .cloned()
                .flatten()
                .unwrap_or_else(RepoHeartbeat::unknown);
            if hb.unknown {
                unknown += 1;
            } else if hb.not_found {
                not_found += 1;
                if ctx.debug > 0 {
                    lines.push(format!(
                        "{}: not found on GitHub ({}), skipping\n",
                        org_repo, hb.reason
                    ));
                }
            } else if !scope.tracks(org_repo, hb.database_id) {
                hb.moved = true;
                moved += 1;
                lines.push(format!(
                    "WARNING: {}: resolves to {} (id {}) but is tracked as id {}, skipping\n",
                    org_repo,
                    hb.name_with_owner,
                    hb.database_id,
                    scope.tracked_id(org_repo)
                ));
            } else {
                found += 1;
                if hb.archived {
                    archived += 1;
                }
                if hb.name_with_owner != *org_repo && ctx.debug > 0 {
                    lines.push(format!(
                        "{}: renamed to {} on GitHub\n",
                        org_repo, hb.name_with_owner
                    ));
                }
                if since(hb.pushed_at, recent_dt) {
                    pushes += 1;
                }
                if since(hb.issue_at, recent_dt) {
                    issues += 1;
                }
                if since(hb.pr_at, recent_dt) {
                    prs += 1;
                }
                if since(hb.fork_at, recent_dt) {
                    forks += 1;
                }
                if since(hb.release_at, recent_dt) {
                    releases += 1;
                }
                if hb.stargazer_count > 0 {
                    hb.star_snapshot = star_snapshot(c, ctx, hb.database_id, recent_dt);
                }
                if hb.active(ApiPass::Stars, recent_dt) {
                    stars += 1;
                }
            }
            heartbeat.insert(org_repo.clone(), hb);
        }
    }
    scope.heartbeat = Some(heartbeat);
    lines.extend(follow_renames(scope, ctx, c));
    lines.sort();
    for line in &lines {
        printf!("{}", line);
    }
    printf!(
        "ghapi2db heartbeat: {} repos in {} GraphQL queries: {} found, {} not found, {} moved, {} unknown, {} archived; active since {}: pushes {}, issues {}, PRs {}, forks {}, releases {}, stars {}\n",
        scope.repos.len(),
        n_queries,
        found,
        not_found,
        moved,
        unknown,
        archived,
        db_time(recent_dt),
        pushes,
        issues,
        prs,
        forks,
        releases,
        stars
    );
}

/// Go `projectRules`: the ingestion rules of the database's project, read
/// once per process: the repository events feed and the heartbeat's rename
/// handling apply the same rules.
pub struct ProjectRules {
    /// `command_line`: what the project's gha2db ingests today
    pub filter: ProjectFilter,
    /// `hist_command_line`: the biggest scope the project ever had
    pub hist: ProjectFilter,
}

static RULES: OnceLock<ProjectRules> = OnceLock::new();

/// Go `getProjectRules`: the project's rules (none when there is no
/// projects.yaml or no enabled project uses the database).
pub fn get_project_rules(ctx: &Ctx) -> &'static ProjectRules {
    RULES.get_or_init(|| {
        let (projects, path) = read_projects_if_present(ctx);
        ProjectRules {
            filter: new_project_filter(ctx, projects.as_ref(), &ctx.pg_db, &path, false),
            hist: new_project_filter(ctx, projects.as_ref(), &ctx.pg_db, &path, true),
        }
    })
}

impl ProjectRules {
    /// Go `projectRules.covers`: does the project track the name: today or in
    /// its historical scope.
    fn covers(&self, name: &str) -> bool {
        self.filter.repo_hit(name) || self.hist.repo_hit(name)
    }

    /// Go `projectRules.formerName`: a former name of the repository that the
    /// project tracks ("" when none): the database name when it is one, else
    /// the alphabetically first such name among the repository id's names.
    fn former_name(&self, org_repo: &str, current: &str, names: &[String]) -> String {
        if org_repo != current && self.covers(org_repo) {
            return org_repo.to_string();
        }
        let mut former = "";
        for name in names {
            if name != current && self.covers(name) && (former.is_empty() || name.as_str() < former)
            {
                former = name;
            }
        }
        former.to_string()
    }
}

/// Go `repoScope.followRenames`: GitHub's API answers always carry the
/// current name of a repository, so the repositories GitHub reports under
/// another name than the database (same repository id) are processed under
/// their current names by every pass: the `gha_repos` (and `gha_orgs`) rows
/// of the current name are added the way the event writer adds them and the
/// name replaces the database one in the scope (each stored event then gets
/// the name of its time, see `repo_name_at`). A repository whose current name
/// is in the project's `GHA2DB_EXCLUDE_REPOS` is skipped by every pass. A
/// current name outside the project's rules while a former name of the
/// repository is inside gets the warning: once per run, because the
/// heartbeat is evaluated once per process. Returns the log lines.
fn follow_renames(scope: &mut RepoScope, ctx: &Ctx, c: &PgConn) -> Vec<String> {
    let rules = get_project_rules(ctx);
    let mut lines = Vec::new();
    scope.db_names = BTreeMap::new();
    scope.untracked = BTreeSet::new();
    for org_repo in scope.repos.clone() {
        let Some(hb) = scope
            .heartbeat
            .as_ref()
            .and_then(|heartbeat| heartbeat.get(&org_repo))
            .cloned()
        else {
            continue;
        };
        if !hb.found || hb.moved {
            continue;
        }
        let current = hb.name_with_owner.clone();
        if rules.filter.repo_excluded(&current) {
            if let Some(entry) = scope
                .heartbeat
                .as_mut()
                .and_then(|heartbeat| heartbeat.get_mut(&org_repo))
            {
                entry.excluded = true;
            }
            if ctx.debug > 0 {
                lines.push(format!(
                    "{}: {} is excluded by project '{}' (GHA2DB_EXCLUDE_REPOS), skipping\n",
                    org_repo, current, rules.filter.name
                ));
            }
            continue;
        }
        let mut name = org_repo.clone();
        if current != org_repo {
            let reason = follow_rename(scope, ctx, c, &org_repo, &hb);
            if reason.is_empty() {
                name = current.clone();
            } else if ctx.debug > 0 {
                lines.push(format!(
                    "{}: {}, still processed as {}\n",
                    current, reason, org_repo
                ));
            }
        }
        if !rules.filter.active() || rules.filter.repo_hit(&current) {
            continue;
        }
        let former = tracked_former_name(ctx, c, rules, &org_repo, &hb);
        if former.is_empty() {
            continue;
        }
        lines.push(format!(
            "WARNING: repo {} was renamed to {} (id {}) which is not tracked by project '{}': data is stored under the new name, update the project configuration\n",
            former, current, hb.database_id, rules.filter.name
        ));
        scope.untracked.insert(name);
    }
    lines
}

/// Go `repoScope.followRename`: put the repository into the scope under
/// GitHub's current name, "" when done, else why not.
fn follow_rename(
    scope: &mut RepoScope,
    ctx: &Ctx,
    c: &PgConn,
    org_repo: &str,
    hb: &RepoHeartbeat,
) -> &'static str {
    let current = hb.name_with_owner.as_str();
    if well_formed(current).is_none() {
        return "malformed name";
    }
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!("select id from gha_repos where name = {}", n_value(1)),
        &[SqlArg::from(current)],
    );
    let (mut known, mut taken) = (false, false);
    while rows.next() {
        let mut id: i64 = 0;
        fatal_on_err(rows.scan(&mut [&mut id]));
        if id == hb.database_id {
            known = true;
        } else {
            taken = true;
        }
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    if taken {
        // the passes resolve the repository id from the name: it would be ambiguous
        return "known under another repository id";
    }
    if !known {
        if ctx.skip_pdb {
            return "not in gha_repos (no database writes)";
        }
        let (mut org_id, mut org_login) = (SqlArg::Null, SqlArg::Null);
        if hb.owner_is_org && hb.owner_id > 0 && !hb.owner_login.is_empty() {
            exec_sql_with_err(
                c,
                ctx,
                &insert_ignore(&format!("into gha_orgs(id, login) {}", n_values(2))),
                &[
                    SqlArg::Int(hb.owner_id),
                    SqlArg::from(hb.owner_login.as_str()),
                ],
            );
            org_id = SqlArg::Int(hb.owner_id);
            org_login = SqlArg::from(hb.owner_login.as_str());
        }
        exec_sql_with_err(
            c,
            ctx,
            &insert_ignore(&format!(
                "into gha_repos(id, name, org_id, org_login) {}",
                n_values(4)
            )),
            &[
                SqlArg::Int(hb.database_id),
                SqlArg::from(current),
                org_id,
                org_login,
            ],
        );
    }
    scope.swap_name(org_repo, current);
    ""
}

impl RepoScope {
    /// Go `repoScope.swapName`: the scope entry of the database name becomes
    /// the entry of GitHub's current name (ids and heartbeat move with it,
    /// the list stays sorted) and the database name is remembered.
    fn swap_name(&mut self, org_repo: &str, current: &str) {
        if let Some(name) = self.repos.iter_mut().find(|name| name.as_str() == org_repo) {
            *name = current.to_string();
        }
        self.repos.sort();
        let ids = self.ids.remove(org_repo).unwrap_or_default();
        self.ids.insert(current.to_string(), ids);
        if let Some(heartbeat) = self.heartbeat.as_mut() {
            if let Some(hb) = heartbeat.remove(org_repo) {
                heartbeat.insert(current.to_string(), hb);
            }
        }
        self.db_names
            .insert(current.to_string(), org_repo.to_string());
    }
}

/// Go `trackedFormerName`: `former_name` over the names `gha_repos` has for
/// the repository id.
fn tracked_former_name(
    ctx: &Ctx,
    c: &PgConn,
    rules: &ProjectRules,
    org_repo: &str,
    hb: &RepoHeartbeat,
) -> String {
    let current = hb.name_with_owner.as_str();
    if org_repo != current && rules.covers(org_repo) {
        return org_repo.to_string();
    }
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!("select name from gha_repos where id = {}", n_value(1)),
        &[SqlArg::Int(hb.database_id)],
    );
    let mut names = Vec::new();
    while rows.next() {
        let mut name = String::new();
        fatal_on_err(rows.scan(&mut [&mut name]));
        names.push(name);
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    rules.former_name(org_repo, current, &names)
}

/// Go `scopeDBName`: the name the database knows a repository by: differs
/// from the scope's name for the repositories the heartbeat found renamed on
/// GitHub (their rows still carry the former name).
pub fn scope_db_name(org_repo: &str) -> String {
    let guard = scope_cell().lock().unwrap_or_else(|p| p.into_inner());
    guard
        .as_ref()
        .and_then(|s| s.db_names.get(org_repo).cloned())
        .unwrap_or_else(|| org_repo.to_string())
}

/// Go `scopeUntracked`: was the repository renamed out of the project's rules
/// (the heartbeat warned about it).
pub fn scope_untracked(org_repo: &str) -> bool {
    let guard = scope_cell().lock().unwrap_or_else(|p| p.into_inner());
    guard
        .as_ref()
        .map(|s| s.untracked.contains(org_repo))
        .unwrap_or(false)
}

/// Go `scopeRepos`: the repositories a pass should process and how many the
/// heartbeat skipped (`None`: not gated). Gates are bypassed in single
/// repository mode (`REPO`) and in date range mode (`DTFROM`/`DTTO`, events
/// and commits passes).
pub fn scope_repos(
    ctx: &mut Ctx,
    c: &PgConn,
    pass: ApiPass,
    recent_dt: DateTime<Utc>,
) -> (Vec<String>, Option<usize>) {
    let mut guard = scope_cell().lock().unwrap_or_else(|p| p.into_inner());
    if guard.is_none() {
        *guard = Some(RepoScope::load(c, ctx));
    }
    let scope = guard.as_mut().expect("scope loaded");
    let env_set = |k: &str| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false);
    let mut gated = !env_set("REPO");
    if gated
        && matches!(pass, ApiPass::Events | ApiPass::Commits)
        && (env_set("DTFROM") || env_set("DTTO"))
    {
        gated = false;
    }
    if !gated {
        let Some(heartbeat) = scope.heartbeat.as_ref() else {
            return (scope.repos.clone(), None);
        };
        // not gated, but the heartbeat of an earlier pass is known: the excluded repositories stay out
        let repos = scope
            .repos
            .iter()
            .filter(|org_repo| !heartbeat.get(*org_repo).is_some_and(|hb| hb.excluded))
            .cloned()
            .collect();
        return (repos, None);
    }
    run_heartbeat(scope, ctx, c, recent_dt);
    let heartbeat = scope.heartbeat.as_ref().expect("heartbeat evaluated");
    let mut repos = Vec::new();
    let mut skipped = 0usize;
    for org_repo in &scope.repos {
        let hb = heartbeat
            .get(org_repo)
            .cloned()
            .unwrap_or_else(RepoHeartbeat::unknown);
        if hb.active(pass, scope.recent_dt) {
            repos.push(org_repo.clone());
            continue;
        }
        skipped += 1;
        if ctx.debug > 0 && hb.found && !hb.moved {
            printf!(
                "{}: {}: skipped by heartbeat (no {} since {})\n",
                pass.label(),
                org_repo,
                pass.gate(),
                db_time(scope.recent_dt)
            );
        }
    }
    (repos, Some(skipped))
}

/// Go `scopeSuffix`: ` (heartbeat: N skipped)` for gated passes, `` otherwise.
pub fn scope_suffix(skipped: Option<usize>) -> String {
    match skipped {
        Some(n) => format!(" (heartbeat: {n} skipped)"),
        None => String::new(),
    }
}

// Repository counters pass (`sync_repo_stats`): one `gha_forkees` snapshot
// per repository per run. GH Archive used to deliver the repository counters
// with every PR event (base/head repository objects), since 2024-09 those
// objects carry only ids and names and since 2025-10 nothing at all, so
// `watchers_by_alias.sql` ("Community stats": stars, forks, open issues per
// repository group) starves. The snapshot is attached to the newest
// `gha_events` row of the repository (`gha_forkees` is keyed `(id, event_id)`)
// like the PR events' snapshots were, with GH Archive semantics: watchers =
// stargazers, open issues include PRs, `updated_at` = the snapshot time,
// `dup_*` = the event's columns. Counters come from the heartbeat (no extra
// API call); `GET /repos/{owner}/{repo}` is used when the heartbeat did not
// tell about the repository (single repository mode, legacy scope, heartbeat
// failure).

/// Go `repoCounters`: what the snapshot row carries.
struct RepoCounters {
    id: i64,
    name: String,
    full_name: String,
    owner_id: i64,
    stars: i64,
    forks: i64,
    open_issues: i64,
    /// `heartbeat` or `API`
    source: &'static str,
}

impl RepoHeartbeat {
    /// Go `repoHeartbeat.counters`: the snapshot counters from the heartbeat
    /// (`None`: the heartbeat did not tell about the repository).
    fn counters(&self) -> Option<RepoCounters> {
        if !self.found || self.moved {
            return None;
        }
        Some(RepoCounters {
            id: self.database_id,
            name: short_repo_name(&self.name_with_owner).to_string(),
            full_name: self.name_with_owner.clone(),
            owner_id: self.owner_id,
            stars: self.stargazer_count,
            forks: self.fork_count,
            open_issues: self.open_issues + self.open_prs,
            source: "heartbeat",
        })
    }
}

fn short_repo_name(full_name: &str) -> &str {
    full_name
        .rsplit_once('/')
        .map(|(_, n)| n)
        .unwrap_or(full_name)
}

/// Go `repoOwner`: the owner part of `owner/repo`.
pub fn repo_owner(full_name: &str) -> &str {
    full_name
        .split_once('/')
        .map(|(owner, _)| owner)
        .unwrap_or(full_name)
}

/// Go `apiCounters`: the snapshot counters from `GET /repos/{owner}/{repo}`
/// (REST reports open issues including PRs).
fn api_counters(repo: &Repository) -> Option<RepoCounters> {
    Some(RepoCounters {
        id: repo.id?,
        name: repo.name.clone().unwrap_or_default(),
        full_name: repo.full_name.clone().unwrap_or_default(),
        owner_id: repo.owner.as_ref().and_then(|o| o.id).unwrap_or(0),
        stars: repo.stargazers_count.unwrap_or(0),
        forks: repo.forks_count.unwrap_or(0),
        open_issues: repo.open_issues_count.unwrap_or(0),
        source: "API",
    })
}

/// Go `heartbeatOf`: the heartbeat of a repository, `None` when no heartbeat
/// was evaluated (single repository mode, legacy scope).
pub fn heartbeat_of(org_repo: &str) -> Option<RepoHeartbeat> {
    let guard = scope_cell().lock().unwrap_or_else(|p| p.into_inner());
    guard
        .as_ref()
        .and_then(|s| s.heartbeat.as_ref())
        .and_then(|hb| hb.get(org_repo).cloned())
}

/// Go `trackedRepo`: is the repository id the one tracked under the name
/// (scope ids, else the events/`gha_repos` id).
pub fn tracked_repo(c: &PgConn, ctx: &Ctx, org_repo: &str, id: i64) -> bool {
    let tracks = {
        let guard = scope_cell().lock().unwrap_or_else(|p| p.into_inner());
        guard.as_ref().map(|s| s.tracks(org_repo, id))
    };
    match tracks {
        Some(t) => t,
        None => repo_ids(c, ctx, org_repo).0 == id,
    }
}

/// The newest `gha_events` row of a repository.
struct LastEvent {
    id: i64,
    created_at: DateTime<Utc>,
    actor_id: i64,
}

/// Go `lastEvent`: the newest `gha_events` row of the repository: under its
/// current name, else under any name.
fn last_event(c: &PgConn, ctx: &Ctx, repo_id: i64, org_repo: &str) -> Option<LastEvent> {
    let queries: [(String, Vec<SqlArg>); 2] = [
        (
            format!(
                "select id, created_at, actor_id from gha_events where repo_id = {} and dup_repo_name = {} \
                 order by created_at desc, id desc limit 1",
                n_value(1),
                n_value(2)
            ),
            vec![SqlArg::Int(repo_id), SqlArg::from(org_repo)],
        ),
        (
            format!(
                "select id, created_at, actor_id from gha_events where repo_id = {} \
                 order by created_at desc, id desc limit 1",
                n_value(1)
            ),
            vec![SqlArg::Int(repo_id)],
        ),
    ];
    for (query, args) in &queries {
        let mut rows = query_sql_with_err(c, ctx, query, args);
        let mut found = None;
        while rows.next() {
            let mut ev = LastEvent {
                id: 0,
                created_at: DateTime::<Utc>::default(),
                actor_id: 0,
            };
            fatal_on_err(rows.scan(&mut [&mut ev.id, &mut ev.created_at, &mut ev.actor_id]));
            found = Some(ev);
        }
        fatal_on_err(rows.err());
        fatal_on_err(rows.close());
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Go `writeRepoStats`: upserts the snapshot row, `true` when inserted
/// (`false`: the `(id, event_id)` row existed and was refreshed).
fn write_repo_stats(
    c: &PgConn,
    ctx: &Ctx,
    cnt: &RepoCounters,
    org_repo: &str,
    ev: &LastEvent,
    now: DateTime<Utc>,
) -> bool {
    let values: Vec<String> = (1..=14).map(n_value).collect();
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!(
            "insert into gha_forkees(id, event_id, name, full_name, owner_id, updated_at, stargazers_count, forks, open_issues, watchers, \
             dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at) values({}) \
             on conflict (id, event_id) do update set name = excluded.name, full_name = excluded.full_name, owner_id = excluded.owner_id, \
             updated_at = excluded.updated_at, stargazers_count = excluded.stargazers_count, forks = excluded.forks, \
             open_issues = excluded.open_issues, watchers = excluded.watchers returning xmax = 0",
            values.join(", ")
        ),
        &[
            SqlArg::Int(cnt.id),
            SqlArg::Int(ev.id),
            SqlArg::from(trunc_to_bytes(&cnt.name, 80)),
            SqlArg::from(trunc_to_bytes(&cnt.full_name, 200)),
            SqlArg::Int(cnt.owner_id),
            SqlArg::from(now),
            SqlArg::Int(cnt.stars),
            SqlArg::Int(cnt.forks),
            SqlArg::Int(cnt.open_issues),
            SqlArg::Int(cnt.stars),
            SqlArg::Int(ev.actor_id),
            SqlArg::Int(cnt.id),
            SqlArg::from(org_repo),
            SqlArg::from(ev.created_at),
        ],
    );
    let mut inserted = false;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut inserted]));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    inserted
}

/// Go `repoStatsCounts`: the pass summary counters (and the GitHub client hint).
#[derive(Default)]
struct RepoStatsCounts {
    inserted: usize,
    updated: usize,
    no_events: usize,
    unavailable: usize,
    api_calls: usize,
    /// `None` until the first API call
    hint: Option<usize>,
}

/// Go `syncRepoStats`: write the repository counters snapshots (`sync_repo_stats`).
pub fn sync_repo_stats(ctx: &mut Ctx) {
    let name = ApiPass::RepoStats.label();
    let params = get_api_params(ctx, ApiPass::RepoStats);
    let n_repos = params.repos.len();
    printf!(
        "{}: processing {} repos{}\n",
        name,
        n_repos,
        scope_suffix(params.heartbeat_skipped)
    );
    let now = wall_as_utc(&Local::now());
    let now = now.with_nanosecond(0).unwrap_or(now);
    let thr_n = get_threads_num(ctx);
    let ctx: &Ctx = ctx;
    let gcs = &params.gcs;
    let c = &params.c;
    let counts = Mutex::new(RepoStatsCounts::default());
    let mut processed = 0usize;
    let dt_start = Utc::now();
    let mut last_time = dt_start;
    let freq = Duration::from_secs(30);
    // the GitHub client to use, rate limits (re)checked every 20 API calls
    let client_idx = || -> usize {
        let mut cn = counts.lock().unwrap_or_else(|p| p.into_inner());
        if cn.hint.is_none() || cn.api_calls.is_multiple_of(20) {
            let (mut h, _, r, w) = get_rate_limits(ctx, gcs, true);
            if r[h] <= ctx.min_ghapi_points {
                if w[h].seconds() <= ctx.max_ghapi_wait_seconds as f64 {
                    printf!("{}: API limit reached, waiting {}\n", name, w[h]);
                    w[h].sleep();
                } else if ctx.ghapi_error_is_fatal {
                    fatalf!("{}: API limit reached, don't want to wait {}", name, w[h]);
                } else {
                    printf!("{}: API limit reached, don't want to wait {}\n", name, w[h]);
                }
                h = get_rate_limits(ctx, gcs, true).0;
            }
            cn.hint = Some(h);
        }
        cn.api_calls += 1;
        cn.hint.expect("set above")
    };
    let fetch_counters = |org_repo: &str| -> Option<RepoCounters> {
        let Some((org, repo)) = well_formed(org_repo) else {
            printf!("WARNING: {}: malformed repo name: '{}'\n", name, org_repo);
            return None;
        };
        let cl = &gcs[client_idx()];
        let mut cnt = None;
        api_page(ctx, &format!("{org_repo} repository"), &mut || {
            let r = cl.repositories_get(org, repo);
            if page_failed(&r) {
                return (r.response, false, r.error);
            }
            cnt = r.value.as_ref().and_then(api_counters);
            (r.response, false, None)
        });
        cnt
    };
    let bump = |f: fn(&mut RepoStatsCounts)| {
        f(&mut counts.lock().unwrap_or_else(|p| p.into_inner()));
    };
    let process_repo = |org_repo: &str| {
        let hb = heartbeat_of(org_repo);
        let cnt = match hb.as_ref().and_then(|h| h.counters()) {
            Some(cnt) => cnt,
            None => {
                if matches!(&hb, Some(h) if !h.unknown) {
                    return;
                }
                let Some(cnt) = fetch_counters(org_repo) else {
                    bump(|cn| cn.unavailable += 1);
                    return;
                };
                if !tracked_repo(c, ctx, org_repo, cnt.id) {
                    printf!(
                        "WARNING: {}: {}: resolves to {} (id {}) which is not tracked, skipping\n",
                        name,
                        org_repo,
                        cnt.full_name,
                        cnt.id
                    );
                    bump(|cn| cn.unavailable += 1);
                    return;
                }
                cnt
            }
        };
        let Some(ev) = last_event(c, ctx, cnt.id, org_repo) else {
            if ctx.debug > 0 {
                printf!("{}: {}: no events, skipping\n", name, org_repo);
            }
            bump(|cn| cn.no_events += 1);
            return;
        };
        let inserted = write_repo_stats(c, ctx, &cnt, org_repo, &ev, now);
        let verb = if inserted {
            bump(|cn| cn.inserted += 1);
            "inserted"
        } else {
            bump(|cn| cn.updated += 1);
            "refreshed"
        };
        if ctx.debug > 0 {
            printf!(
                "{}: {}: {} stars, {} forks, {} open issues from the {}, snapshot {} (event {})\n",
                name,
                org_repo,
                cnt.stars,
                cnt.forks,
                cnt.open_issues,
                cnt.source,
                verb,
                ev.id
            );
        }
    };
    let mut iter = |processed: &mut usize| {
        *processed += 1;
        let msg = format!(
            "{}: API calls: {}",
            name,
            counts.lock().unwrap_or_else(|p| p.into_inner()).api_calls
        );
        progress_info(*processed, n_repos, dt_start, &mut last_time, freq, &msg);
    };
    let selected: Vec<&String> = params
        .repos
        .iter()
        .filter(|r| !params.is_single_repo || **r == params.single_repo)
        .collect();
    if thr_n > 1 {
        let process_repo = &process_repo;
        let (tx, rx) = mpsc::channel::<()>();
        std::thread::scope(|scope| {
            let mut n_threads = 0usize;
            for repo in &selected {
                let tx = tx.clone();
                scope.spawn(move || {
                    process_repo(repo);
                    let _ = tx.send(());
                });
                n_threads += 1;
                while n_threads >= thr_n {
                    let _ = rx.recv();
                    n_threads -= 1;
                    iter(&mut processed);
                }
            }
            while n_threads > 0 {
                let _ = rx.recv();
                n_threads -= 1;
                iter(&mut processed);
            }
        });
    } else {
        for repo in &selected {
            process_repo(repo);
            iter(&mut processed);
        }
    }
    let cn = counts.into_inner().unwrap_or_else(|p| p.into_inner());
    printf!(
        "{}: processed {} repos, snapshots: {} inserted, {} refreshed; skipped: {} without events, {} unavailable; GH API calls: {}\n",
        name,
        processed,
        cn.inserted,
        cn.updated,
        cn.no_events,
        cn.unavailable,
        cn.api_calls
    );
    params.c.close();
}

#[cfg(test)]
mod tests {
    use super::*;
    use devstatscode::context::test_support::{env_lock, set_or_unset};
    use devstatscode::projects::AllProjects;
    use devstatscode::yamlv2::de as yde;

    /// The project environment the rules read, restored when the test ends.
    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn new() -> EnvGuard {
            let lock = env_lock();
            let names = [
                "ENV_SET",
                "GHA2DB_EXCLUDE_REPOS",
                "GHA2DB_EXACT",
                "GHA2DB_PROJECT",
            ];
            let saved = names
                .iter()
                .map(|n| (*n, std::env::var(n).ok()))
                .collect::<Vec<_>>();
            for n in names {
                devstatscode::env::remove_var(n);
            }
            EnvGuard { saved, _lock: lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (n, v) in &self.saved {
                set_or_unset(n, v.as_deref());
            }
        }
    }

    /// Go `testRules`: project rules like `get_project_rules` builds them
    /// (`command_line` and `hist_command_line` filters of project `proj`).
    fn test_rules(project_yaml: &str) -> ProjectRules {
        let yaml = format!("projects:\n  proj:\n    psql_db: proj\n{project_yaml}");
        let projects: AllProjects = yde::unmarshal(yaml.as_bytes()).expect("test projects.yaml");
        let ctx = Ctx::default();
        ProjectRules {
            filter: new_project_filter(&ctx, Some(&projects), "proj", "p.yaml", false),
            hist: new_project_filter(&ctx, Some(&projects), "proj", "p.yaml", true),
        }
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    /// Go `TestDecodeHeartbeatOwner`.
    #[test]
    fn decode_heartbeat_owner() {
        let body = br#"{"data": {
            "r0": {"databaseId": 1, "nameWithOwner": "neworg/repo", "owner": {"__typename": "Organization", "login": "neworg", "databaseId": 11}},
            "r1": {"databaseId": 2, "nameWithOwner": "someone/repo", "owner": {"__typename": "User", "login": "someone", "databaseId": 12}},
            "r2": {"databaseId": 3, "nameWithOwner": "plain/repo", "owner": {"login": "plain", "databaseId": 13}},
            "r3": {"databaseId": 4, "nameWithOwner": "none/repo"},
            "r4": null
        }}"#;
        let batch = strings(&["a/a", "b/b", "c/c", "d/d", "e/e"]);
        let hbs = decode_heartbeat(body, &batch, 0, 1).expect("decoded");
        assert_eq!(hbs.len(), 5);
        let expected = [
            (true, 11, "neworg", true),
            (true, 12, "someone", false),
            (true, 13, "plain", false),
            (true, 0, "", false),
            (false, 0, "", false),
        ];
        for (i, (found, id, login, is_org)) in expected.into_iter().enumerate() {
            let hb = &hbs[i];
            assert_eq!(
                (
                    hb.found,
                    hb.owner_id,
                    hb.owner_login.as_str(),
                    hb.owner_is_org,
                    hb.excluded
                ),
                (found, id, login, is_org, false),
                "heartbeat {i}"
            );
        }
    }

    /// Go `TestHeartbeatQueryAsksTheOwnerType`.
    #[test]
    fn heartbeat_query_asks_the_owner_type() {
        let query = String::from_utf8(heartbeat_query(&strings(&["org/repo"]))).unwrap();
        // the owner type decides whether the gha_repos row of a renamed repository gets an organization
        for part in [
            "owner { __typename login ... on User { databaseId } ... on Organization { databaseId } }",
            r#"r0: repository(owner: \"org\", name: \"repo\") { ...F }"#,
        ] {
            assert!(query.contains(part), "heartbeat query lacks {part:?}: {query}");
        }
    }

    /// Go `TestExcludedHeartbeatIsNeverActive`.
    #[test]
    fn excluded_heartbeat_is_never_active() {
        let now = Utc::now();
        let recent = now - chrono::Duration::hours(1);
        let mut hb = RepoHeartbeat {
            found: true,
            pushed_at: Some(now),
            issue_at: Some(now),
            pr_at: Some(now),
            release_at: Some(now),
            fork_at: Some(now),
            stargazer_count: 5,
            ..Default::default()
        };
        let passes = [
            ApiPass::Events,
            ApiPass::Commits,
            ApiPass::Comments,
            ApiPass::Reviews,
            ApiPass::Forks,
            ApiPass::Releases,
            ApiPass::Stars,
            ApiPass::RepoStats,
            ApiPass::RepoEvents,
            ApiPass::IssuesPrs,
        ];
        for pass in passes {
            assert!(
                hb.active(pass, recent),
                "{}: an active repository must be processed",
                pass.label()
            );
        }
        hb.excluded = true;
        for pass in passes {
            assert!(
                !hb.active(pass, recent),
                "{}: an excluded repository must not be processed",
                pass.label()
            );
        }
        // an excluded repository still has counters (no pass asks for them)
        assert!(hb.counters().is_some());
    }

    /// Go `TestSwapName`.
    #[test]
    fn swap_name() {
        let hb = |id: i64| RepoHeartbeat {
            database_id: id,
            ..Default::default()
        };
        let mut s = RepoScope {
            repos: strings(&["a/old", "c/keep", "z/old"]),
            ids: BTreeMap::from([
                ("a/old".to_string(), vec![1, 7]),
                ("c/keep".to_string(), vec![3]),
                ("z/old".to_string(), vec![26]),
            ]),
            heartbeat: Some(BTreeMap::from([
                ("a/old".to_string(), hb(1)),
                ("c/keep".to_string(), hb(3)),
                ("z/old".to_string(), hb(26)),
            ])),
            ..Default::default()
        };
        let hb_id = |s: &RepoScope, name: &str| {
            s.heartbeat
                .as_ref()
                .and_then(|h| h.get(name))
                .map(|h| h.database_id)
        };
        // the new name sorts last
        s.swap_name("a/old", "x/new");
        assert_eq!(s.repos, strings(&["c/keep", "x/new", "z/old"]));
        assert_eq!(
            s.ids,
            BTreeMap::from([
                ("c/keep".to_string(), vec![3]),
                ("x/new".to_string(), vec![1, 7]),
                ("z/old".to_string(), vec![26]),
            ])
        );
        assert_eq!(hb_id(&s, "x/new"), Some(1));
        assert_eq!(hb_id(&s, "a/old"), None);
        assert_eq!(s.heartbeat.as_ref().map(|h| h.len()), Some(3));
        // the new name sorts first
        s.swap_name("z/old", "b/new");
        assert_eq!(s.repos, strings(&["b/new", "c/keep", "x/new"]));
        assert_eq!(hb_id(&s, "b/new"), Some(26));
        assert_eq!(hb_id(&s, "c/keep"), Some(3));
        assert!(s.tracks("b/new", 26) && !s.tracks("z/old", 26));
        assert_eq!(s.tracked_id("x/new"), 7);
        assert_eq!(
            s.db_names,
            BTreeMap::from([
                ("b/new".to_string(), "z/old".to_string()),
                ("x/new".to_string(), "a/old".to_string()),
            ])
        );
        // the accessors the passes use
        s.untracked = BTreeSet::from(["b/new".to_string()]);
        let saved = scope_cell()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace(s);
        assert_eq!(scope_db_name("x/new"), "a/old");
        assert_eq!(scope_db_name("b/new"), "z/old");
        assert_eq!(scope_db_name("c/keep"), "c/keep");
        assert_eq!(scope_db_name("not/there"), "not/there");
        assert!(scope_untracked("b/new"));
        assert!(
            !scope_untracked("x/new") && !scope_untracked("c/keep") && !scope_untracked("z/old")
        );
        // no scope: the name itself, nothing untracked
        *scope_cell().lock().unwrap_or_else(|p| p.into_inner()) = None;
        assert_eq!(scope_db_name("x/new"), "x/new");
        assert!(!scope_untracked("b/new"));
        *scope_cell().lock().unwrap_or_else(|p| p.into_inner()) = saved;
    }

    /// Go `TestProjectRulesFormerName`.
    #[test]
    fn project_rules_former_name() {
        let _guard = EnvGuard::new();
        // today's rules: the new organization; historical rules: also the names the repositories had before
        let rules = test_rules(
            "    command_line: ['neworg']\n    hist_command_line: ['neworg,oldorg/repo,oldorg/other']\n    env: {GHA2DB_EXCLUDE_REPOS: 'neworg/excluded,oldorg/other'}\n",
        );
        assert!(rules.filter.active() && rules.hist.active() && rules.hist.hist_rules);
        for (name, exp) in [
            ("neworg/repo", true),     // today's rules
            ("oldorg/repo", true),     // historical rules only
            ("oldorg/unknown", false), // neither
            ("elsewhere/repo", false),
            ("neworg/excluded", false), // excluded names are never tracked
            ("oldorg/other", false),
            ("", false),
        ] {
            assert_eq!(rules.covers(name), exp, "covers({name:?})");
        }
        let cases: [(&str, &str, &str, &[&str], &str); 9] = [
            (
                "renamed now, the database name is tracked",
                "oldorg/repo",
                "elsewhere/repo",
                &["oldorg/repo"],
                "oldorg/repo",
            ),
            (
                "renamed now, the database name is tracked (no gha_repos names needed)",
                "neworg/repo",
                "elsewhere/repo",
                &[],
                "neworg/repo",
            ),
            (
                "renamed now, another former name is tracked",
                "third/repo",
                "elsewhere/repo",
                &["third/repo", "oldorg/repo", "elsewhere/repo"],
                "oldorg/repo",
            ),
            (
                "renamed earlier (same name now), a former name is tracked",
                "elsewhere/repo",
                "elsewhere/repo",
                &["elsewhere/repo", "oldorg/repo"],
                "oldorg/repo",
            ),
            (
                "several tracked former names: the alphabetically first",
                "elsewhere/repo",
                "elsewhere/repo",
                &["oldorg/repo", "neworg/zzz", "neworg/aaa"],
                "neworg/aaa",
            ),
            (
                "no tracked name at all",
                "third/repo",
                "elsewhere/repo",
                &["third/repo", "fourth/repo"],
                "",
            ),
            (
                "only an excluded former name",
                "oldorg/other",
                "elsewhere/repo",
                &["oldorg/other", "neworg/excluded"],
                "",
            ),
            (
                "the current name does not count as a former name",
                "neworg/repo",
                "neworg/repo",
                &["neworg/repo"],
                "",
            ),
            ("no names", "elsewhere/repo", "elsewhere/repo", &[], ""),
        ];
        for (what, org_repo, current, names, expected) in cases {
            assert_eq!(
                rules.former_name(org_repo, current, &strings(names)),
                expected,
                "{what}"
            );
        }

        // without `hist_command_line` the historical rules are today's rules
        devstatscode::env::remove_var("GHA2DB_EXCLUDE_REPOS");
        let rules = test_rules("    command_line: ['neworg']\n");
        assert!(
            !rules.hist.hist_rules && rules.covers("neworg/repo") && !rules.covers("oldorg/repo")
        );
        // a name outside the only rules is not tracked
        assert_eq!(
            rules.former_name("oldorg/repo", "elsewhere/repo", &strings(&["oldorg/repo"])),
            ""
        );

        // no project: nothing is filtered, nothing is excluded (the warning is never evaluated then)
        let ctx = Ctx::default();
        let none = ProjectRules {
            filter: new_project_filter(&ctx, None, "proj", "p.yaml", false),
            hist: new_project_filter(&ctx, None, "proj", "p.yaml", true),
        };
        assert!(
            !none.filter.active()
                && !none.filter.repo_excluded("any/repo")
                && none.filter.repo_hit("any/repo")
        );
    }

    /// Go `TestRepoOwner`.
    #[test]
    fn repo_owner_cases() {
        for (name, exp) in [
            ("org/repo", "org"),
            ("org/repo/more", "org"),
            ("norepo", "norepo"),
            ("", ""),
            ("/repo", ""),
        ] {
            assert_eq!(repo_owner(name), exp, "repo_owner({name:?})");
        }
    }
}
