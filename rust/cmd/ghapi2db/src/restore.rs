//! Port of `cmd/ghapi2db/restore.go`: the restore passes for comments,
//! reviews, forks, releases and stars missed by GH Archive.

use std::collections::BTreeMap;
use std::sync::{mpsc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use devstatscode::consts::HIDE_CFG_FILE;
use devstatscode::ghapi::{fmt_slice, get_rate_limits, GoDuration};
use devstatscode::github::{
    self, Client, Error, GoTime, IssueListCommentsOptions, ListOptions,
    PullRequestListCommentsOptions, PullRequestListOptions, RepositoryListForksOptions, Response,
    Stargazer, Timestamp, User,
};
use devstatscode::io::read_file;
use devstatscode::pg::api::{n_value, query_sql_with_err};
use devstatscode::pg::{PgConn, SqlArg};
use devstatscode::restore::{
    restore_commit_comment, restore_fork, restore_issue_comment, restore_release, restore_review,
    restore_review_comment, restore_star,
};
use devstatscode::string::{get_hidden, maybe_hide_func};
use devstatscode::threads::get_threads_num;
use devstatscode::time::progress_info;
use devstatscode::{fatal_on_err, fatalf, printf, Ctx};
use serde::Deserialize;

use crate::{db_time, get_api_params, MaybeHide};

const RESTORE_PAGE_CAP: i64 = 2000;

/// Go `restoreStats`.
#[derive(Clone, Debug, Default)]
pub struct RestoreStats {
    pub checked: usize,
    pub restored: usize,
    pub pages: usize,
    /// repos whose stargazer list GitHub refuses to return (restricted to repository admins
    /// since 2026-06-30) although the repository has stars - stars restore only
    pub unavailable: usize,
    pub min_dt: Option<DateTime<Utc>>,
    pub max_dt: Option<DateTime<Utc>>,
    /// event ids of restored rows that produce postprocessed data (comments/reviews - text
    /// sources); forks/releases/stars restores add no gha_texts/labels/issue-PR-link rows,
    /// so they are counted but never collected here (they must not trigger a postprocess)
    pub eids: Vec<i64>,
}

impl RestoreStats {
    fn mark(&mut self, dt: DateTime<Utc>) {
        if self.min_dt.map(|m| dt < m).unwrap_or(true) {
            self.min_dt = Some(dt);
        }
        if self.max_dt.map(|m| dt > m).unwrap_or(true) {
            self.max_dt = Some(dt);
        }
    }

    pub fn merge(&mut self, o: RestoreStats) {
        self.checked += o.checked;
        self.restored += o.restored;
        self.pages += o.pages;
        self.unavailable += o.unavailable;
        if let Some(m) = o.min_dt {
            self.mark(m);
        }
        if let Some(m) = o.max_dt {
            self.mark(m);
        }
        self.eids.extend(o.eids);
    }
}

/// The per-repository arguments of a restore function (Go `restoreRepoFunc`).
struct RepoJob<'a> {
    gc: &'a Client,
    c: &'a PgConn,
    ctx: &'a Ctx,
    org: &'a str,
    repo: &'a str,
    org_repo: &'a str,
    repo_id: i64,
    org_id: SqlArg,
    recent_dt: DateTime<Utc>,
    maybe_hide: MaybeHide<'a>,
}

type RestoreRepoFunc = fn(&RepoJob<'_>, &mut RestoreStats);

/// Go `numberFromURL`: the trailing number of an API URL (0 when absent).
fn number_from_url(url: Option<&str>) -> i64 {
    let url = match url {
        None => return 0,
        Some(u) => u,
    };
    url.rsplit('/').next().and_then(go_atoi).unwrap_or(0)
}

/// Go `strconv.Atoi` (an optional sign, decimal digits only).
fn go_atoi(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<i64>().ok()
}

fn one_row_present(c: &PgConn, ctx: &Ctx, query: &str, args: &[SqlArg]) -> bool {
    let mut rows = query_sql_with_err(c, ctx, query, args);
    let mut present = false;
    while rows.next() {
        present = true;
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    present
}

fn id_present(c: &PgConn, ctx: &Ctx, table: &str, e_type: &str, id: i64) -> bool {
    one_row_present(
        c,
        ctx,
        &format!(
            "select 1 from {} where id = {} and dup_type = {} limit 1",
            table,
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::Int(id), SqlArg::from(e_type)],
    )
}

fn fork_present(c: &PgConn, ctx: &Ctx, forkee_id: i64) -> bool {
    one_row_present(
        c,
        ctx,
        &format!(
            "select 1 from gha_forkees f, gha_payloads p where f.id = {} and p.event_id = f.event_id and p.forkee_id = f.id and p.dup_type = 'ForkEvent' limit 1",
            n_value(1)
        ),
        &[SqlArg::Int(forkee_id)],
    )
}

fn star_present(c: &PgConn, ctx: &Ctx, actor_id: i64, org_repo: &str, starred_at: GoTime) -> bool {
    one_row_present(
        c,
        ctx,
        &format!(
            "select 1 from gha_events where type = 'WatchEvent' and actor_id = {} and dup_repo_name = {} and created_at = {} limit 1",
            n_value(1),
            n_value(2),
            n_value(3)
        ),
        &[
            SqlArg::Int(actor_id),
            SqlArg::from(org_repo),
            SqlArg::Time(starred_at.0),
        ],
    )
}

/// Go `repoIDs`: the repository id (0 when unknown) and organization id
/// (NULL when none) seen in `gha_events`.
fn repo_ids(c: &PgConn, ctx: &Ctx, org_repo: &str) -> (i64, SqlArg) {
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!(
            "select coalesce(max(repo_id), 0), max(org_id) from gha_events where dup_repo_name = {}",
            n_value(1)
        ),
        &[SqlArg::from(org_repo)],
    );
    let mut repo_id: i64 = 0;
    let mut oid: Option<i64> = None;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut repo_id, &mut oid]));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    let org_id = match oid {
        None => SqlArg::Null,
        Some(o) => SqlArg::Int(o),
    };
    (repo_id, org_id)
}

/// The result of one API page call: Go's `(*github.Response, more, error)`.
type PageResult = (Option<Response>, bool, Option<Error>);

/// Go `time.Until(t)` as a Go duration.
fn time_until(t: DateTime<Utc>) -> GoDuration {
    GoDuration((t - Utc::now()).num_nanoseconds().unwrap_or(i64::MAX))
}

/// Go `apiPage` - true: process next page, false: skip repo; retries 403
/// abuse with backoff.
fn api_page(ctx: &Ctx, info: &str, call: &mut dyn FnMut() -> PageResult) -> bool {
    for try_ in 1..=ctx.max_ghapi_retry {
        let (resp, more, err) = call();
        let status = resp.as_ref().map(|r| r.status).unwrap_or(0);
        if resp.is_some() && (status == 404 || status == 410) {
            return false;
        }
        if let Some(e) = &err {
            match e {
                Error::RateLimit { rate, .. } => {
                    let wait = time_until(rate.reset_time());
                    if wait.seconds() <= ctx.max_ghapi_wait_seconds as f64 {
                        if wait.0 > 0 {
                            GoDuration(wait.0 + 1_000_000_000).sleep();
                        }
                        continue;
                    }
                    if ctx.ghapi_error_is_fatal {
                        fatalf!("{}: rate limited, don't want to wait {}", info, wait);
                    }
                    printf!("{}: rate limited, reset in {}, skipping\n", info, wait);
                    return false;
                }
                Error::Abuse { retry_after, .. } => {
                    let mut wait = GoDuration::from_secs(10 * try_);
                    if let Some(ra) = retry_after {
                        wait = GoDuration(ra.as_nanos() as i64);
                    }
                    if wait.seconds() <= ctx.max_ghapi_wait_seconds as f64 {
                        printf!(
                            "{}: abuse detected, waiting {}, retry {}/{}\n",
                            info,
                            wait,
                            try_,
                            ctx.max_ghapi_retry
                        );
                        wait.sleep();
                        continue;
                    }
                    if ctx.ghapi_error_is_fatal {
                        fatalf!("{}: abuse detected, don't want to wait {}", info, wait);
                    }
                    printf!(
                        "{}: abuse detected, don't want to wait {}, skipping\n",
                        info,
                        wait
                    );
                    return false;
                }
                _ => {}
            }
        }
        if resp.is_some() && status == 403 {
            printf!(
                "{}: abuse detected, retry {}/{}\n",
                info,
                try_,
                ctx.max_ghapi_retry
            );
            std::thread::sleep(Duration::from_secs(10 * try_ as u64));
            continue;
        }
        if resp.is_some() && status >= 400 {
            printf!("{}: status {}, skipping\n", info, status);
            return false;
        }
        if let Some(e) = err {
            if ctx.ghapi_error_is_fatal {
                fatal_on_err::<(), _>(Err(&e));
            }
            printf!("{}: error: {}, skipping\n", info, e);
            return false;
        }
        return more;
    }
    printf!(
        "{}: giving up after {} retries\n",
        info,
        ctx.max_ghapi_retry
    );
    false
}

/// The `hint`/`rem` shared between the pass loop and its workers.
struct PassRate {
    hint: usize,
    rem: Vec<i64>,
}

/// Go `restorePass`: run `process` for every recent repository (the Go
/// goroutine pool), summing the statistics.
fn restore_pass(ctx: &mut Ctx, name: &str, process: RestoreRepoFunc) -> RestoreStats {
    let params = get_api_params(ctx);
    let maybe_hide = maybe_hide_func(get_hidden(ctx, HIDE_CFG_FILE));
    let n_repos = params.repos.len();
    printf!(
        "{}: processing {} repos, recent date: {}\n",
        name,
        n_repos,
        db_time(params.recent_dt)
    );
    let (hint, _, rem, _) = get_rate_limits(ctx, &params.gcs, true);
    let thr_n = get_threads_num(ctx);
    let ctx: &Ctx = ctx;
    let rate = Mutex::new(PassRate { hint, rem });
    let total = Mutex::new(RestoreStats::default());
    let mut processed = 0usize;
    let dt_start = Utc::now();
    let mut last_time = dt_start;
    let freq = Duration::from_secs(30);
    let gcs = &params.gcs;
    let c = &params.c;
    let recent_dt = params.recent_dt;
    let maybe_hide: MaybeHide<'_> = &maybe_hide;
    let mut iter = |processed: &mut usize| {
        *processed += 1;
        if (*processed).is_multiple_of(20) {
            let (mut h, _, mut r, mut w) = get_rate_limits(ctx, gcs, true);
            if r[h] <= ctx.min_ghapi_points {
                if w[h].seconds() <= ctx.max_ghapi_wait_seconds as f64 {
                    printf!("{}: API limit reached, waiting {}\n", name, w[h]);
                    w[h].sleep();
                } else if ctx.ghapi_error_is_fatal {
                    fatalf!("{}: API limit reached, don't want to wait {}", name, w[h]);
                } else {
                    printf!("{}: API limit reached, don't want to wait {}\n", name, w[h]);
                }
                (h, _, r, w) = get_rate_limits(ctx, gcs, true);
                let _ = &w;
            }
            let mut rt = rate.lock().unwrap_or_else(|p| p.into_inner());
            rt.hint = h;
            rt.rem = r;
        }
        let msg = {
            let rt = rate.lock().unwrap_or_else(|p| p.into_inner());
            format!(
                "{}: API points: {}, hint: {}",
                name,
                fmt_slice(&rt.rem),
                rt.hint
            )
        };
        progress_info(*processed, n_repos, dt_start, &mut last_time, freq, &msg);
    };
    let process_repo = |org_repo: &str| {
        let ary: Vec<&str> = org_repo.split('/').collect();
        if ary.len() < 2 {
            printf!("WARNING: {}: malformed repo name: '{}'\n", name, org_repo);
            return;
        }
        let (repo_id, org_id) = repo_ids(c, ctx, org_repo);
        if repo_id <= 0 {
            printf!(
                "{}: {}: no existing repo_id, skipping restore\n",
                name,
                org_repo
            );
            return;
        }
        let mut stats = RestoreStats::default();
        let cl = {
            let rt = rate.lock().unwrap_or_else(|p| p.into_inner());
            &gcs[rt.hint]
        };
        let job = RepoJob {
            gc: cl,
            c,
            ctx,
            org: ary[0],
            repo: ary[1],
            org_repo,
            repo_id,
            org_id,
            recent_dt,
            maybe_hide,
        };
        process(&job, &mut stats);
        total.lock().unwrap_or_else(|p| p.into_inner()).merge(stats);
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
    let total = total.into_inner().unwrap_or_else(|p| p.into_inner());
    printf!(
        "{}: processed {} repos, {} pages, checked {}, restored {}\n",
        name,
        processed,
        total.pages,
        total.checked,
        total.restored
    );
    if total.unavailable > 0 {
        printf!(
            "{}: stargazer lists unavailable for {}/{} repos (GitHub restricted stargazer/watcher lists to repository admins on 2026-06-30), star events cannot be restored\n",
            name,
            total.unavailable,
            processed
        );
    }
    params.c.close();
    total
}

/// Go's `if err != nil || resp == nil || resp.StatusCode >= 400 { return resp, false, err }`.
fn page_failed<T>(r: &github::ApiResult<T>) -> bool {
    r.error.is_some() || r.response.is_none() || r.status() >= 400
}

fn restore_comments_repo(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    let (gc, c, ctx) = (job.gc, job.c, job.ctx);
    let mut opt = IssueListCommentsOptions {
        sort: Some("updated".to_string()),
        direction: Some("asc".to_string()),
        since: Some(job.recent_dt),
        list: ListOptions {
            per_page: 100,
            page: 0,
        },
    };
    for page in 1..=RESTORE_PAGE_CAP {
        opt.list.page = page;
        let more = api_page(
            ctx,
            &format!("{} issue comments", job.org_repo),
            &mut || {
                let r = gc.issues_list_comments(job.org, job.repo, 0, &opt);
                if page_failed(&r) {
                    return (r.response, false, r.error);
                }
                stats.pages += 1;
                for cmt in r.value.unwrap_or_default() {
                    let cid = match cmt.id {
                        Some(id) => id,
                        None => continue,
                    };
                    stats.checked += 1;
                    if id_present(c, ctx, "gha_comments", "IssueCommentEvent", cid) {
                        continue;
                    }
                    let (eid, ok) = restore_issue_comment(
                        c,
                        ctx,
                        job.org_repo,
                        job.repo_id,
                        &job.org_id,
                        number_from_url(cmt.issue_url.as_deref()),
                        Some(&cmt),
                        job.maybe_hide,
                    );
                    if ok {
                        stats.restored += 1;
                        stats.mark(cmt.created_at.expect("checked by the restore").utc());
                        stats.eids.push(eid);
                    }
                }
                let resp = r.response.expect("checked above");
                let more = resp.next_page != 0;
                (Some(resp), more, None)
            },
        );
        if !more {
            break;
        }
    }
    let mut popt = PullRequestListCommentsOptions {
        sort: "updated".to_string(),
        direction: "asc".to_string(),
        since: Some(job.recent_dt),
        list: ListOptions {
            per_page: 100,
            page: 0,
        },
    };
    for page in 1..=RESTORE_PAGE_CAP {
        popt.list.page = page;
        let more = api_page(
            ctx,
            &format!("{} review comments", job.org_repo),
            &mut || {
                let r = gc.pull_requests_list_comments(job.org, job.repo, 0, &popt);
                if page_failed(&r) {
                    return (r.response, false, r.error);
                }
                stats.pages += 1;
                for cmt in r.value.unwrap_or_default() {
                    let cid = match cmt.id {
                        Some(id) => id,
                        None => continue,
                    };
                    stats.checked += 1;
                    if id_present(c, ctx, "gha_comments", "PullRequestReviewCommentEvent", cid) {
                        continue;
                    }
                    let (eid, ok) = restore_review_comment(
                        c,
                        ctx,
                        job.org_repo,
                        job.repo_id,
                        &job.org_id,
                        number_from_url(cmt.pull_request_url.as_deref()),
                        Some(&cmt),
                        job.maybe_hide,
                    );
                    if ok {
                        stats.restored += 1;
                        stats.mark(cmt.created_at.expect("checked by the restore").utc());
                        stats.eids.push(eid);
                    }
                }
                let resp = r.response.expect("checked above");
                let more = resp.next_page != 0;
                (Some(resp), more, None)
            },
        );
        if !more {
            break;
        }
    }
    // commit comments API has no since filter and lists ascending - walk from the last page down
    let mut copt = ListOptions {
        per_page: 100,
        page: 1,
    };
    let mut last = 1i64;
    api_page(
        ctx,
        &format!("{} commit comments last page", job.org_repo),
        &mut || {
            let r = gc.repositories_list_comments(job.org, job.repo, copt);
            if page_failed(&r) {
                return (r.response, false, r.error);
            }
            let resp = r.response.expect("checked above");
            if resp.last_page > 1 {
                last = resp.last_page;
            }
            (Some(resp), false, None)
        },
    );
    for page in (1..=last).rev() {
        copt.page = page;
        let mut any_recent = false;
        let ok = api_page(
            ctx,
            &format!("{} commit comments", job.org_repo),
            &mut || {
                let r = gc.repositories_list_comments(job.org, job.repo, copt);
                if page_failed(&r) {
                    return (r.response, false, r.error);
                }
                stats.pages += 1;
                for cmt in r.value.unwrap_or_default() {
                    let (cid, created_at) = match (cmt.id, cmt.created_at) {
                        (Some(id), Some(t)) if t.utc() >= job.recent_dt => (id, t),
                        _ => continue,
                    };
                    any_recent = true;
                    stats.checked += 1;
                    if id_present(c, ctx, "gha_comments", "CommitCommentEvent", cid) {
                        continue;
                    }
                    let (eid, ok) = restore_commit_comment(
                        c,
                        ctx,
                        job.org_repo,
                        job.repo_id,
                        &job.org_id,
                        Some(&cmt),
                        job.maybe_hide,
                    );
                    if ok {
                        stats.restored += 1;
                        stats.mark(created_at.utc());
                        stats.eids.push(eid);
                    }
                }
                (r.response, true, None)
            },
        );
        if !ok || !any_recent {
            break;
        }
    }
}

/// Go `ghTokens`: the OAuth tokens of `GHA2DB_GITHUB_OAUTH` (a file path
/// when it contains `/`), separated by commas/whitespace; `-` means none.
fn gh_tokens(ctx: &Ctx) -> Vec<String> {
    let mut oauth = ctx.github_oauth.trim().to_string();
    if oauth.contains('/') {
        let bytes = fatal_on_err(read_file(ctx, &oauth));
        oauth = String::from_utf8_lossy(&bytes).to_string();
    }
    oauth
        .split([',', '\n', '\r', '\t', ' '])
        .filter(|p| !p.is_empty() && *p != "-")
        .map(|p| p.to_string())
        .collect()
}

struct GqlStargazer {
    starred_at: GoTime,
    login: String,
    id: i64,
}

/// Go `encoding/json` semantics for the GraphQL response structs: a JSON
/// `null` leaves the field at its zero value (the API sends
/// `"startCursor": null` for an empty page and `"data": null` with
/// top-level errors) instead of being a decoding error.
fn nd<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlPageInfo {
    #[serde(rename = "hasPreviousPage", deserialize_with = "nd")]
    has_previous_page: bool,
    #[serde(rename = "startCursor", deserialize_with = "nd")]
    start_cursor: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlNode {
    #[serde(deserialize_with = "nd")]
    login: String,
    #[serde(rename = "databaseId", deserialize_with = "nd")]
    database_id: i64,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlEdge {
    #[serde(rename = "starredAt")]
    starred_at: Option<GoTime>,
    #[serde(deserialize_with = "nd")]
    node: GqlNode,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlStargazers {
    #[serde(rename = "pageInfo", deserialize_with = "nd")]
    page_info: GqlPageInfo,
    #[serde(deserialize_with = "nd")]
    edges: Vec<GqlEdge>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlRepository {
    #[serde(rename = "stargazerCount", deserialize_with = "nd")]
    stargazer_count: i64,
    #[serde(deserialize_with = "nd")]
    stargazers: GqlStargazers,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlData {
    #[serde(deserialize_with = "nd")]
    repository: GqlRepository,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlError {
    #[serde(deserialize_with = "nd")]
    message: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GqlOut {
    #[serde(deserialize_with = "nd")]
    data: GqlData,
    #[serde(deserialize_with = "nd")]
    errors: Vec<GqlError>,
}

/// One page of Go `ghGraphQLStargazers`: the usable stargazers, the
/// previous-page cursor and whether one exists, the number of raw edges on
/// the page and the repository's `stargazerCount`.
struct GqlStargazersPage {
    gazers: Vec<GqlStargazer>,
    prev_cursor: String,
    has_prev: bool,
    n_edges: usize,
    star_count: i64,
}

/// Go `ghGraphQLStargazers` - stars restore uses GraphQL: the REST
/// stargazers path returns 404 / no usable starred_at data on prod as of
/// 2026-07; GraphQL exposes starredAt directly, ordered by STARRED_AT.
/// Since 2026-06-30 GitHub returns an empty stargazers connection for
/// non-admins while `stargazerCount` still works, which is how an
/// unavailable list is told apart from a repository nobody starred.
fn gh_graphql_stargazers(
    ctx: &Ctx,
    tokens: &[String],
    org: &str,
    repo: &str,
    before: &str,
) -> Result<GqlStargazersPage, String> {
    let mut vars: BTreeMap<&str, &str> = BTreeMap::new();
    vars.insert("o", org);
    vars.insert("r", repo);
    if !before.is_empty() {
        vars.insert("b", before);
    }
    let mut payload_map: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
    payload_map.insert(
        "query",
        serde_json::Value::String(
            "query($o: String!, $r: String!, $b: String) { repository(owner: $o, name: $r) { stargazerCount stargazers(last: 100, before: $b, orderBy: {field: STARRED_AT, direction: ASC}) { pageInfo { hasPreviousPage startCursor } edges { starredAt node { login databaseId } } } } }".to_string(),
        ),
    );
    payload_map.insert(
        "variables",
        serde_json::to_value(&vars).map_err(|e| e.to_string())?,
    );
    let payload = serde_json::to_vec(&payload_map).map_err(|e| e.to_string())?;
    let graphql_url = if ctx.github_api_url.is_empty() {
        "https://api.github.com/graphql".to_string()
    } else {
        format!("{}graphql", ctx.github_api_url)
    };
    let mut err: Option<String> = None;
    let n_tokens = tokens.len();
    for (i, token) in tokens.iter().enumerate() {
        for try_ in 1..=ctx.max_ghapi_retry {
            let auth = format!("bearer {token}");
            let resp = match github::raw_post(
                &graphql_url,
                &[
                    ("Authorization", auth.as_str()),
                    ("Content-Type", "application/json"),
                ],
                &payload,
                Duration::from_secs(60),
            ) {
                Ok(r) => r,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            };
            let snippet_len = resp.body.len().min(200);
            let snippet = String::from_utf8_lossy(&resp.body[..snippet_len]).to_string();
            if resp.status == 403 || resp.status == 429 {
                let mut wait = GoDuration::from_secs(10 * try_);
                if let Some(ra) = resp.header("Retry-After") {
                    if let Some(secs) = go_atoi(ra) {
                        wait = GoDuration::from_secs(secs);
                    }
                } else if let Some(xr) = resp.header("X-RateLimit-Reset") {
                    if let Some(epoch) = go_atoi(xr) {
                        let reset = DateTime::<Utc>::from_timestamp(epoch, 0).unwrap_or_default();
                        wait = time_until(reset);
                    }
                }
                if wait.0 > 0 && wait.seconds() <= ctx.max_ghapi_wait_seconds as f64 {
                    wait.sleep();
                    continue;
                }
                if ctx.ghapi_error_is_fatal {
                    fatalf!(
                        "{}/{}: graphql rate limited, don't want to wait {}: {}",
                        org,
                        repo,
                        wait,
                        snippet
                    );
                }
                err = Some(format!(
                    "graphql rate limited (token {}/{}), reset in {}: {}",
                    i + 1,
                    n_tokens,
                    wait,
                    snippet
                ));
                break;
            }
            if resp.status != 200 {
                err = Some(format!(
                    "graphql status {} (token {}/{}): {}",
                    resp.status,
                    i + 1,
                    n_tokens,
                    snippet
                ));
                break;
            }
            let out: GqlOut = match serde_json::from_slice(&resp.body) {
                Ok(o) => o,
                Err(e) => {
                    err = Some(go_json_error(&e));
                    break;
                }
            };
            if let Some(first) = out.errors.first() {
                err = Some(format!(
                    "graphql (token {}/{}): {}",
                    i + 1,
                    n_tokens,
                    first.message
                ));
                break;
            }
            let star_count = out.data.repository.stargazer_count;
            let sg = out.data.repository.stargazers;
            let n_edges = sg.edges.len();
            let mut gazers = Vec::new();
            for edge in sg.edges {
                let starred_at = match edge.starred_at {
                    Some(t) => t,
                    None => continue,
                };
                if edge.node.database_id <= 0 || edge.node.login.is_empty() {
                    continue;
                }
                gazers.push(GqlStargazer {
                    starred_at,
                    login: edge.node.login,
                    id: edge.node.database_id,
                });
            }
            return Ok(GqlStargazersPage {
                gazers,
                prev_cursor: sg.page_info.start_cursor,
                has_prev: sg.page_info.has_previous_page,
                n_edges,
                star_count,
            });
        }
    }
    match err {
        Some(e) => Err(e),
        None => Ok(GqlStargazersPage {
            gazers: Vec::new(),
            prev_cursor: String::new(),
            has_prev: false,
            n_edges: 0,
            star_count: 0,
        }),
    }
}

/// Go `encoding/json` unmarshal error text for a serde error.
fn go_json_error(e: &serde_json::Error) -> String {
    if e.is_eof() {
        "unexpected end of JSON input".to_string()
    } else {
        format!("invalid character in JSON input: {e}")
    }
}

fn restore_stars_repo(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    let (c, ctx) = (job.c, job.ctx);
    let tokens = gh_tokens(ctx);
    if tokens.is_empty() {
        printf!(
            "{}: stars restore needs GHA2DB_GITHUB_OAUTH token(s), skipping\n",
            job.org_repo
        );
        return;
    }
    let mut before = String::new();
    for page in 1..=RESTORE_PAGE_CAP {
        let GqlStargazersPage {
            gazers,
            prev_cursor: prev,
            has_prev,
            n_edges,
            star_count,
        } = match gh_graphql_stargazers(ctx, &tokens, job.org, job.repo, &before) {
            Ok(v) => v,
            Err(e) => {
                printf!("{}: stargazers graphql: {}, skipping\n", job.org_repo, e);
                return;
            }
        };
        stats.pages += 1;
        if page == 1 && n_edges == 0 && star_count > 0 {
            // the repository has stars but GitHub returns none: the list is restricted, not empty
            stats.unavailable += 1;
            if ctx.debug > 0 {
                printf!(
                    "{}: stargazer list unavailable ({} stars), skipping\n",
                    job.org_repo,
                    star_count
                );
            }
            return;
        }
        let mut any_recent = false;
        for g in gazers {
            if g.starred_at.utc() < job.recent_dt {
                continue;
            }
            any_recent = true;
            stats.checked += 1;
            if star_present(c, ctx, g.id, job.org_repo, g.starred_at) {
                continue;
            }
            let star = Stargazer {
                starred_at: Some(Timestamp(g.starred_at.utc())),
                user: Some(User::id_login(g.id, &g.login)),
            };
            let (_, ok) = restore_star(
                c,
                ctx,
                job.org_repo,
                job.repo_id,
                &job.org_id,
                Some(&star),
                job.maybe_hide,
            );
            if ok {
                stats.restored += 1;
                stats.mark(g.starred_at.utc());
            }
        }
        if !any_recent || !has_prev {
            break;
        }
        before = prev;
    }
}

fn restore_reviews_repo(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    let (gc, c, ctx) = (job.gc, job.c, job.ctx);
    let mut pr_numbers: Vec<i64> = Vec::new();
    let mut opt = PullRequestListOptions {
        state: "all".to_string(),
        sort: "updated".to_string(),
        direction: "desc".to_string(),
        list: ListOptions {
            per_page: 100,
            page: 0,
        },
        ..Default::default()
    };
    for page in 1..=RESTORE_PAGE_CAP {
        opt.list.page = page;
        let mut older = false;
        let more = api_page(ctx, &format!("{} PRs", job.org_repo), &mut || {
            let r = gc.pull_requests_list(job.org, job.repo, &opt);
            if page_failed(&r) {
                return (r.response, false, r.error);
            }
            stats.pages += 1;
            for pr in r.value.unwrap_or_default() {
                let number = match pr.number {
                    Some(n) => n,
                    None => continue,
                };
                if pr
                    .updated_at
                    .map(|t| t.utc() < job.recent_dt)
                    .unwrap_or(false)
                {
                    older = true;
                    break;
                }
                pr_numbers.push(number);
            }
            let resp = r.response.expect("checked above");
            let more = resp.next_page != 0;
            (Some(resp), more, None)
        });
        if !more || older {
            break;
        }
    }
    for number in pr_numbers {
        let mut ropt = ListOptions {
            per_page: 100,
            page: 0,
        };
        for page in 1..=RESTORE_PAGE_CAP {
            ropt.page = page;
            let more = api_page(
                ctx,
                &format!("{}#{} reviews", job.org_repo, number),
                &mut || {
                    let r = gc.pull_requests_list_reviews(job.org, job.repo, number, ropt);
                    if page_failed(&r) {
                        return (r.response, false, r.error);
                    }
                    stats.pages += 1;
                    for rev in r.value.unwrap_or_default() {
                        let (rid, submitted_at) = match (rev.id, rev.submitted_at) {
                            (Some(id), Some(t)) => (id, t),
                            _ => continue,
                        };
                        stats.checked += 1;
                        if id_present(c, ctx, "gha_reviews", "PullRequestReviewEvent", rid) {
                            continue;
                        }
                        let (eid, ok) = restore_review(
                            c,
                            ctx,
                            job.org_repo,
                            job.repo_id,
                            &job.org_id,
                            number,
                            Some(&rev),
                            job.maybe_hide,
                        );
                        if ok {
                            stats.restored += 1;
                            stats.mark(submitted_at.utc());
                            stats.eids.push(eid);
                        }
                    }
                    let resp = r.response.expect("checked above");
                    let more = resp.next_page != 0;
                    (Some(resp), more, None)
                },
            );
            if !more {
                break;
            }
        }
    }
}

fn restore_forks_repo(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    let (gc, c, ctx) = (job.gc, job.c, job.ctx);
    let mut opt = RepositoryListForksOptions {
        sort: "newest".to_string(),
        list: ListOptions {
            per_page: 100,
            page: 0,
        },
    };
    for page in 1..=RESTORE_PAGE_CAP {
        opt.list.page = page;
        let mut older = false;
        let more = api_page(ctx, &format!("{} forks", job.org_repo), &mut || {
            let r = gc.repositories_list_forks(job.org, job.repo, &opt);
            if page_failed(&r) {
                return (r.response, false, r.error);
            }
            stats.pages += 1;
            for fork in r.value.unwrap_or_default() {
                let fid = match fork.id {
                    Some(id) => id,
                    None => continue,
                };
                if fork
                    .created_at
                    .map(|t| t.0 < job.recent_dt)
                    .unwrap_or(false)
                {
                    older = true;
                    break;
                }
                stats.checked += 1;
                if fork_present(c, ctx, fid) {
                    continue;
                }
                let (_, ok) = restore_fork(
                    c,
                    ctx,
                    job.org_repo,
                    job.repo_id,
                    &job.org_id,
                    Some(&fork),
                    job.maybe_hide,
                );
                if ok {
                    stats.restored += 1;
                    stats.mark(fork.created_at.expect("checked by the restore").0);
                }
            }
            let resp = r.response.expect("checked above");
            let more = resp.next_page != 0;
            (Some(resp), more, None)
        });
        if !more || older {
            break;
        }
    }
}

fn restore_releases_repo(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    let (gc, c, ctx) = (job.gc, job.c, job.ctx);
    let mut opt = ListOptions {
        per_page: 100,
        page: 0,
    };
    for page in 1..=RESTORE_PAGE_CAP {
        opt.page = page;
        let mut older = false;
        let more = api_page(ctx, &format!("{} releases", job.org_repo), &mut || {
            let r = gc.repositories_list_releases(job.org, job.repo, opt);
            if page_failed(&r) {
                return (r.response, false, r.error);
            }
            stats.pages += 1;
            for rel in r.value.unwrap_or_default() {
                let (rid, created_at) = match (rel.id, rel.created_at) {
                    (Some(id), Some(t)) => (id, t),
                    _ => continue,
                };
                let rel_dt = rel.published_at.unwrap_or(created_at).0;
                if rel_dt < job.recent_dt {
                    older = true;
                    break;
                }
                stats.checked += 1;
                if id_present(c, ctx, "gha_releases", "ReleaseEvent", rid) {
                    continue;
                }
                let (_, ok) = restore_release(
                    c,
                    ctx,
                    job.org_repo,
                    job.repo_id,
                    &job.org_id,
                    Some(&rel),
                    job.maybe_hide,
                );
                if ok {
                    stats.restored += 1;
                    stats.mark(rel_dt);
                }
            }
            let resp = r.response.expect("checked above");
            let more = resp.next_page != 0;
            (Some(resp), more, None)
        });
        if !more || older {
            break;
        }
    }
}

pub fn sync_comments(ctx: &mut Ctx) -> RestoreStats {
    restore_pass(ctx, "ghapi2db comments restore", restore_comments_repo)
}

pub fn sync_reviews(ctx: &mut Ctx) -> RestoreStats {
    restore_pass(ctx, "ghapi2db reviews restore", restore_reviews_repo)
}

pub fn sync_forks(ctx: &mut Ctx) -> RestoreStats {
    restore_pass(ctx, "ghapi2db forks restore", restore_forks_repo)
}

pub fn sync_stars(ctx: &mut Ctx) -> RestoreStats {
    restore_pass(ctx, "ghapi2db stars restore", restore_stars_repo)
}

pub fn sync_releases(ctx: &mut Ctx) -> RestoreStats {
    restore_pass(ctx, "ghapi2db releases restore", restore_releases_repo)
}
