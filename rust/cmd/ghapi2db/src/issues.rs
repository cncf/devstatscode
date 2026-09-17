//! Issues and pull requests sweep pass (Go `cmd/ghapi2db/issues.go`): GH
//! Archive's `PullRequestEvent` payloads carry stubbed pull request objects
//! since 2024-10-17 (`PullRequestReview{,Comment}Event` since 2025-10-09):
//! only url/id/number/base/head, so `gha_pull_requests` gets rows with the
//! zero created_at, no user, title or state. Objects GH Archive missed
//! altogether have no rows at all. Per repository:
//!
//! 1. stub sweep (DB-driven): every pull request with stub rows is fetched
//!    (`GET /pulls/{n}`) and its stub rows are filled with the current
//!    object; a pull request without any `gha_issues` row gets one attached
//!    (`GET /issues/{n}`) to its newest upgraded event;
//! 2. listing (API-driven, repositories with issue or PR updates since the
//!    recent date): `GET /issues?state=all&since=<recent date>&sort=updated&direction=asc`,
//!    an object without a good (non-stub) row in its primary table gets
//!    GHA-shaped synthetic `opened` (at created_at) and, when closed,
//!    `closed` (at closed_at) events written with the gha2db writer under
//!    deterministic ids (`ARTIFICIAL_ISSUE_ID_BASE` / `ARTIFICIAL_PR_ID_BASE`
//!    plus `2*id + 0/1`), so the pass is idempotent and a later close of a
//!    synthesized open object adds its `closed` event. Reopens are not
//!    synthesized (the object carries no reopen time) - the issue events pass
//!    handles state changes of objects the database already knows.

use chrono::{DateTime, SecondsFormat, Utc};
use devstatscode::consts::{ARTIFICIAL_ISSUE_ID_BASE, ARTIFICIAL_PR_ID_BASE};
use devstatscode::gha::{actor_hit, Actor, Event, GhaTime, Issue, Org, Payload, PullRequest, Repo};
use devstatscode::ghawriter::{upgrade_pull_request_stubs, write_to_db, STUB_CREATED_AT_CUT};
use devstatscode::pg::api::{n_value, query_sql_with_err};
use devstatscode::pg::{PgConn, SqlArg};
use devstatscode::{fatal_on_err, printf, Ctx};
use serde::Deserialize;

use crate::db_time;
use crate::heartbeat::{heartbeat_of, ApiPass};
use crate::restore::{
    api_page, page_failed, restore_pass, GhClients, RepoJob, RestoreStats, RESTORE_PAGE_CAP,
};

const ISSUES_PER_PAGE: i64 = 100;

/// Go `apiIssue`: the REST issue object - the GHA issue plus who closed it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct ApiIssue {
    #[serde(flatten)]
    issue: Issue,
    closed_by: Option<Actor>,
}

impl ApiIssue {
    /// `closed_by` when it is a real actor (Go: `ClosedBy != nil && ClosedBy.ID != 0`).
    fn closed_by(&self) -> Option<&Actor> {
        self.closed_by.as_ref().filter(|a| a.id != 0)
    }
}

/// Go `fetchPullRequest`: the full pull request object, `None` when unavailable.
fn fetch_pull_request(
    gc: &GhClients<'_>,
    ctx: &Ctx,
    info: &str,
    org: &str,
    repo: &str,
    number: i64,
) -> Option<PullRequest> {
    let mut raw: Option<Box<serde_json::value::RawValue>> = None;
    api_page(ctx, info, &mut || {
        let r = gc.call(|cl| cl.get_raw(&format!("repos/{org}/{repo}/pulls/{number}")));
        if page_failed(&r) {
            return (r.response, false, r.error);
        }
        raw = r.value;
        (r.response, false, None)
    });
    let raw = raw?;
    let pr: PullRequest = match serde_json::from_str(raw.get()) {
        Ok(pr) => pr,
        Err(e) => {
            printf!(
                "WARNING: {}: cannot unmarshal the pull request: {}, skipping\n",
                info,
                e
            );
            return None;
        }
    };
    if pr.id == 0 {
        printf!(
            "WARNING: {}: pull request object without an id, skipping\n",
            info
        );
        return None;
    }
    Some(pr)
}

/// Go `fetchIssue`: the full issue object, `None` when unavailable.
fn fetch_issue(
    gc: &GhClients<'_>,
    ctx: &Ctx,
    info: &str,
    org: &str,
    repo: &str,
    number: i64,
) -> Option<ApiIssue> {
    let mut raw: Option<Box<serde_json::value::RawValue>> = None;
    api_page(ctx, info, &mut || {
        let r = gc.call(|cl| cl.get_raw(&format!("repos/{org}/{repo}/issues/{number}")));
        if page_failed(&r) {
            return (r.response, false, r.error);
        }
        raw = r.value;
        (r.response, false, None)
    });
    let raw = raw?;
    let issue: ApiIssue = match serde_json::from_str(raw.get()) {
        Ok(issue) => issue,
        Err(e) => {
            printf!(
                "WARNING: {}: cannot unmarshal the issue: {}, skipping\n",
                info,
                e
            );
            return None;
        }
    };
    if issue.issue.id == 0 {
        printf!("WARNING: {}: issue object without an id, skipping\n", info);
        return None;
    }
    Some(issue)
}

/// Go `stubPullRequests`: (id, number) of the repository's pull requests that have stub rows
/// (by repository id: the rows written under the historical names of a renamed repository are included).
fn stub_pull_requests(c: &PgConn, ctx: &Ctx, repo_id: i64) -> Vec<(i64, i64)> {
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!(
            "select id, number from gha_pull_requests where dup_repo_id = {} and created_at < '{}' \
             group by id, number order by id, number",
            n_value(1),
            STUB_CREATED_AT_CUT
        ),
        &[SqlArg::Int(repo_id)],
    );
    let mut out = Vec::new();
    while rows.next() {
        let (mut id, mut number) = (0i64, 0i64);
        fatal_on_err(rows.scan(&mut [&mut id, &mut number]));
        out.push((id, number));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    out
}

fn row_present(c: &PgConn, ctx: &Ctx, query: &str, args: &[SqlArg]) -> bool {
    let mut rows = query_sql_with_err(c, ctx, query, args);
    let mut present = false;
    while rows.next() {
        present = true;
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    present
}

/// Go `issueRowPresent`: does the issue (by id) have a gha_issues row.
fn issue_row_present(c: &PgConn, ctx: &Ctx, issue_id: i64) -> bool {
    row_present(
        c,
        ctx,
        &format!("select 1 from gha_issues where id = {} limit 1", n_value(1)),
        &[SqlArg::Int(issue_id)],
    )
}

/// Go `issueRowPresentByNumber`: does the repository's issue/PR number have a
/// gha_issues row (under any of the repository's names).
fn issue_row_present_by_number(c: &PgConn, ctx: &Ctx, repo_id: i64, number: i64) -> bool {
    row_present(
        c,
        ctx,
        &format!(
            "select 1 from gha_issues where dup_repo_id = {} and number = {} limit 1",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::Int(repo_id), SqlArg::Int(number)],
    )
}

/// Go `goodPullRequestRowPresent`: does the repository's pull request number
/// have a non-stub gha_pull_requests row (under any of the repository's names).
fn good_pull_request_row_present(c: &PgConn, ctx: &Ctx, repo_id: i64, number: i64) -> bool {
    row_present(
        c,
        ctx,
        &format!(
            "select 1 from gha_pull_requests where dup_repo_id = {} and number = {} and created_at >= '{}' limit 1",
            n_value(1),
            n_value(2),
            STUB_CREATED_AT_CUT
        ),
        &[SqlArg::Int(repo_id), SqlArg::Int(number)],
    )
}

/// Go `syntheticEvent`: a GHA-shaped IssuesEvent/PullRequestEvent for the object's lifecycle step.
#[allow(clippy::too_many_arguments)]
fn synthetic_event(
    id: i64,
    e_type: &str,
    action: &str,
    created_at: GhaTime,
    actor: Actor,
    job: &RepoJob<'_>,
    number: i64,
    issue: Option<&Issue>,
    pr: Option<&PullRequest>,
) -> Event {
    let org = match job.org_id {
        SqlArg::Int(oid) => Some(Org {
            id: oid,
            login: job.org.to_string(),
        }),
        _ => None,
    };
    Event {
        id: id.to_string(),
        type_: e_type.to_string(),
        public: true,
        created_at,
        actor,
        repo: Repo {
            id: job.repo_id,
            name: job.org_repo.to_string(),
        },
        org,
        payload: Payload {
            action: Some(action.to_string()),
            number: Some(number),
            issue: issue.cloned(),
            pull_request: pr.cloned(),
            ..Payload::default()
        },
    }
}

/// Go `writeSynthetic`: write one synthetic event with the gha2db writer (actor filters, exists check).
fn write_synthetic(job: &RepoJob<'_>, name: &str, ev: &Event, stats: &mut RestoreStats) {
    let (c, ctx) = (job.c, job.ctx);
    // the same actor filters gha2db applies to the archives
    if !actor_hit(ctx, &ev.actor.login) {
        return;
    }
    if write_to_db(c, ctx, ev, job.maybe_hide) == 0 {
        return;
    }
    stats.restored += 1;
    stats.add_type(&ev.type_);
    stats.mark(ev.created_at.with_timezone(&Utc));
    if let Ok(eid) = ev.id.parse::<i64>() {
        stats.eids.push(eid);
    }
    if ctx.debug > 0 {
        printf!(
            "{}: {}: synthesized {} {} {} ({})\n",
            name,
            job.org_repo,
            ev.type_,
            ev.payload.action.as_deref().unwrap_or_default(),
            ev.id,
            ev.created_at
        );
    }
}

/// Go `synthesizeIssue`: opened (+ closed) IssuesEvents of an issue the database does not know.
fn synthesize_issue(job: &RepoJob<'_>, name: &str, issue: &ApiIssue, stats: &mut RestoreStats) {
    let base = ARTIFICIAL_ISSUE_ID_BASE + 2 * issue.issue.id;
    let ev = synthetic_event(
        base,
        "IssuesEvent",
        "opened",
        issue.issue.created_at,
        issue.issue.user.clone(),
        job,
        issue.issue.number,
        Some(&issue.issue),
        None,
    );
    write_synthetic(job, name, &ev, stats);
    if issue.issue.state == "closed" {
        if let Some(closed_at) = issue.issue.closed_at {
            let actor = issue.closed_by().unwrap_or(&issue.issue.user).clone();
            let ev = synthetic_event(
                base + 1,
                "IssuesEvent",
                "closed",
                closed_at,
                actor,
                job,
                issue.issue.number,
                Some(&issue.issue),
                None,
            );
            write_synthetic(job, name, &ev, stats);
        }
    }
}

/// Go `synthesizePullRequest`: opened (+ closed) PullRequestEvents of a pull
/// request the database does not know; the payload carries both the pull
/// request and its issue object (gha_pull_requests and gha_issues rows).
fn synthesize_pull_request(
    job: &RepoJob<'_>,
    name: &str,
    pr: &PullRequest,
    issue: &ApiIssue,
    stats: &mut RestoreStats,
) {
    let base = ARTIFICIAL_PR_ID_BASE + 2 * pr.id;
    let ev = synthetic_event(
        base,
        "PullRequestEvent",
        "opened",
        pr.created_at,
        pr.user.clone(),
        job,
        pr.number,
        Some(&issue.issue),
        Some(pr),
    );
    write_synthetic(job, name, &ev, stats);
    if pr.state == "closed" {
        if let Some(closed_at) = pr.closed_at {
            let actor = pr
                .merged_by
                .as_ref()
                .filter(|a| a.id != 0)
                .or_else(|| issue.closed_by())
                .unwrap_or(&pr.user)
                .clone();
            let ev = synthetic_event(
                base + 1,
                "PullRequestEvent",
                "closed",
                closed_at,
                actor,
                job,
                pr.number,
                Some(&issue.issue),
                Some(pr),
            );
            write_synthetic(job, name, &ev, stats);
        }
    }
}

/// Go `sweepStubs`: fill the stub gha_pull_requests rows of the repository with the current API objects.
fn sweep_stubs(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    let (gc, c, ctx) = (job.gc, job.c, job.ctx);
    let name = ApiPass::IssuesPrs.label();
    let stubs = stub_pull_requests(c, ctx, job.repo_id);
    if ctx.debug > 0 {
        printf!(
            "{}: {}: {} pull requests with stub rows\n",
            name,
            job.org_repo,
            stubs.len()
        );
    }
    for (id, number) in stubs {
        stats.checked += 1;
        let info = format!("{}: {} pull request {}", name, job.org_repo, number);
        let pr = match fetch_pull_request(gc, ctx, &info, job.org, job.repo, number) {
            Some(pr) => pr,
            None => {
                // deleted on GitHub (404 is silent in api_page) or not fetched: the stub rows stay
                if ctx.debug > 0 {
                    printf!(
                        "{}: {}: pull request {} ({}): not available on GitHub, stub rows kept\n",
                        name,
                        job.org_repo,
                        number,
                        id
                    );
                }
                continue;
            }
        };
        if pr.id != id {
            printf!(
                "WARNING: {}: {}: pull request {} is {} on GitHub, {} in the database, skipping\n",
                name,
                job.org_repo,
                number,
                pr.id,
                id
            );
            continue;
        }
        let mut issue: Option<Issue> = None;
        if !issue_row_present_by_number(c, ctx, job.repo_id, number) {
            let info = format!("{}: {} issue {}", name, job.org_repo, number);
            if let Some(ai) = fetch_issue(gc, ctx, &info, job.org, job.repo, number) {
                issue = Some(ai.issue);
            }
        }
        let (eids, attached) =
            upgrade_pull_request_stubs(c, ctx, &pr, issue.as_ref(), job.maybe_hide);
        if eids.is_empty() {
            continue;
        }
        stats.stub_prs += 1;
        stats.stub_rows += eids.len();
        stats.eids.extend(eids.iter().copied());
        if attached {
            stats.issue_rows += 1;
        }
        if ctx.debug > 0 {
            printf!(
                "{}: {}: pull request {} ({}): upgraded {} stub rows, issue row attached: {}\n",
                name,
                job.org_repo,
                number,
                pr.id,
                eids.len(),
                attached
            );
        }
    }
}

/// Go `time.RFC3339` of a UTC time (`2006-01-02T15:04:05Z`).
fn rfc3339_utc(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Go `sweepListing`: synthesize the lifecycle events of the updated objects the database does not know.
fn sweep_listing(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    let (gc, c, ctx) = (job.gc, job.c, job.ctx);
    let name = ApiPass::IssuesPrs.label();
    let since = rfc3339_utc(job.recent_dt);
    for page in 1..=RESTORE_PAGE_CAP {
        let mut issues: Option<Vec<Box<serde_json::value::RawValue>>> = None;
        let info = format!("{}: {} issues page {}", name, job.org_repo, page);
        let more = api_page(ctx, &info, &mut || {
            let r = gc.call(|cl| {
                cl.issues_list_by_repo_raw(job.org, job.repo, &since, ISSUES_PER_PAGE, page)
            });
            if page_failed(&r) {
                return (r.response, false, r.error);
            }
            issues = Some(r.value.unwrap_or_default());
            let resp = r.response.expect("checked above");
            let more = resp.next_page > 0;
            (Some(resp), more, None)
        });
        let issues = match issues {
            Some(objs) => objs,
            None => return,
        };
        stats.pages += 1;
        for raw in &issues {
            let issue: ApiIssue = match serde_json::from_str(raw.get()) {
                Ok(issue) => issue,
                Err(e) => {
                    printf!(
                        "WARNING: {}: {}: cannot unmarshal an issue: {}, skipping the listing\n",
                        name,
                        job.org_repo,
                        e
                    );
                    return;
                }
            };
            if issue.issue.id == 0 {
                continue;
            }
            stats.checked += 1;
            if issue.issue.pull_request.is_none() {
                if issue_row_present(c, ctx, issue.issue.id) {
                    continue;
                }
                synthesize_issue(job, name, &issue, stats);
                continue;
            }
            if good_pull_request_row_present(c, ctx, job.repo_id, issue.issue.number) {
                continue;
            }
            let info = format!(
                "{}: {} pull request {}",
                name, job.org_repo, issue.issue.number
            );
            let pr = match fetch_pull_request(gc, ctx, &info, job.org, job.repo, issue.issue.number)
            {
                Some(pr) => pr,
                None => continue,
            };
            synthesize_pull_request(job, name, &pr, &issue, stats);
        }
        if ctx.debug > 0 {
            printf!(
                "{}: {}: page {}: {} objects, synthesized so far {}\n",
                name,
                job.org_repo,
                page,
                issues.len(),
                stats.restored
            );
        }
        if !more {
            return;
        }
    }
}

/// Go `restoreIssuesPRsRepo`: the issues and pull requests sweep of one repository.
fn restore_issues_prs_repo(job: &RepoJob<'_>, stats: &mut RestoreStats) {
    sweep_stubs(job, stats);
    // the listing only for repositories with issue or PR updates since the recent date (when the heartbeat knows)
    if let Some(hb) = heartbeat_of(job.org_repo) {
        if !hb.active(ApiPass::Events, job.recent_dt) {
            if job.ctx.debug > 0 {
                printf!(
                    "{}: {}: no issue or PR updates since {}, listing skipped\n",
                    ApiPass::IssuesPrs.label(),
                    job.org_repo,
                    db_time(job.recent_dt)
                );
            }
            return;
        }
    }
    sweep_listing(job, stats);
}

/// Go `syncIssuesPRs`: issues and pull requests sweep pass.
pub fn sync_issues_prs(ctx: &mut Ctx) -> RestoreStats {
    let stats = restore_pass(ctx, ApiPass::IssuesPrs, &restore_issues_prs_repo);
    printf!(
        "{}: upgraded {} stub rows of {} pull requests, attached {} issue rows\n",
        ApiPass::IssuesPrs.label(),
        stats.stub_rows,
        stats.stub_prs,
        stats.issue_rows
    );
    stats
}
