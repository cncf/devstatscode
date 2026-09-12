//! Go `restore.go`: API-restored objects (comments, reviews, forks, releases,
//! stars missed by GH Archive) get artificial event ids in per-class
//! sub-bands (see `consts`) so ids from different REST namespaces can never
//! collide; plus the bounded/targeted post-processing of restored events.

use chrono::{DateTime, FixedOffset, Utc};

use crate::consts::{
    ARTIFICIAL_COMMENT_ID_BASE, ARTIFICIAL_COMMIT_COMMENT_ID_BASE, ARTIFICIAL_FORK_ID_BASE,
    ARTIFICIAL_RELEASE_ID_BASE, ARTIFICIAL_REVIEW_COMMENT_ID_BASE, ARTIFICIAL_REVIEW_ID_BASE,
    SYNC_EVENT_ID_THRESHOLD,
};
use crate::context::Ctx;
use crate::error::fatal_on_err;
use crate::ghapi::{gh_actor, gh_actor_id_or_nil, gh_actor_login_or_nil};
use crate::github::{
    GoTime, IssueComment, PullRequestComment, PullRequestReview, Repository, RepositoryComment,
    RepositoryRelease, Stargazer, Timestamp, User,
};
use crate::gofmt;
use crate::hash::hash_strings;
use crate::io::read_file;
use crate::pg::api::{
    exec_sql_tx_with_err, fatal_on_pg_err, fatal_on_pg_error, insert_ignore, int_or_nil, n_value,
    n_values, query_sql_tx_with_err, query_sql_with_err, string_or_nil, trunc_to_bytes,
};
use crate::pg::value::go_time_string;
use crate::pg::{pg_conn, pg_conn_db_shared, ExecResult, PgConn, PgTx, SqlArg};
use crate::printf;
use crate::time::to_ymdhms_date;

/// Go `func(string) string` hiding function shared between threads.
pub type MaybeHide<'a> = &'a (dyn Fn(&str) -> String + Sync);

fn ts_or_nil(t: Option<&Timestamp>) -> SqlArg {
    match t {
        None => SqlArg::Null,
        Some(t) => SqlArg::Time(t.0.fixed_offset()),
    }
}

fn ts_or(t: Option<&Timestamp>, def: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    match t {
        None => def,
        Some(t) => t.0.fixed_offset(),
    }
}

fn time_or(t: Option<&GoTime>, def: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    match t {
        None => def,
        Some(t) => t.0,
    }
}

fn str_or(s: Option<&str>, def: &str) -> String {
    s.unwrap_or(def).to_string()
}

fn int_or(i: Option<i64>, def: i64) -> i64 {
    i.unwrap_or(def)
}

fn bool_or(b: Option<bool>, def: bool) -> bool {
    b.unwrap_or(def)
}

/// Go `lookupID`: the single nullable id returned by `query` (nil otherwise).
fn lookup_id(tc: &mut PgTx<'_>, ctx: &Ctx, query: &str, args: &[SqlArg]) -> SqlArg {
    let mut rows = query_sql_tx_with_err(tc, ctx, query, args);
    let mut id: Option<i64> = None;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut id]));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    match id {
        None => SqlArg::Null,
        Some(id) => SqlArg::Int(id),
    }
}

/// Go `NegativeArtificialID`: deterministic negative event id for
/// API/git-restored objects with no natural id (like pre-2015 events).
pub fn negative_artificial_id(parts: &[&str]) -> i64 {
    let mut id = hash_strings(parts);
    if id > 0 {
        id = -id;
    }
    if id == 0 {
        id = -1;
    }
    id
}

fn trunc_string_or_nil_hidden(s: Option<&str>, max_len: usize, maybe_hide: MaybeHide) -> SqlArg {
    match s {
        None => SqlArg::Null,
        Some(s) => SqlArg::Str(trunc_to_bytes(&maybe_hide(s), max_len)),
    }
}

/// Go `%v` of a JSON-decoded (`time.Parse`) time.
fn json_time(t: DateTime<FixedOffset>) -> String {
    gofmt::time(t)
}

/// Go `findRawEventID`: the id of the raw GH Archive event matching the
/// restored object (verified through `payload_col` when given), or `None`
/// when there is none or it is ambiguous (an artificial event is created then).
#[allow(clippy::too_many_arguments)]
fn find_raw_event_id(
    tc: &mut PgTx<'_>,
    ctx: &Ctx,
    e_type: &str,
    repo: &str,
    actor: Option<&User>,
    created_at: DateTime<FixedOffset>,
    payload_col: &str,
    obj_id: Option<i64>,
) -> Option<i64> {
    let actor_id = actor.and_then(|a| a.id)?;
    let mut rows = query_sql_tx_with_err(
        tc,
        ctx,
        &format!(
            "select id from gha_events where id < 281474976710657 and type = {} and dup_repo_name = {} and actor_id = {} and created_at = {} order by id limit 3",
            n_value(1),
            n_value(2),
            n_value(3),
            n_value(4)
        ),
        &[
            SqlArg::from(e_type),
            SqlArg::from(repo),
            SqlArg::Int(actor_id),
            SqlArg::Time(created_at),
        ],
    );
    let mut ids: Vec<i64> = Vec::new();
    let mut id: i64 = 0;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut id]));
        ids.push(id);
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    if ids.is_empty() {
        return None;
    }
    let oid = match obj_id {
        Some(oid) if !payload_col.is_empty() => oid,
        _ => {
            if ids.len() == 1 {
                return Some(ids[0]);
            }
            printf!(
                "findRawEventID: ambiguous raw events for ({}, {}, {}, {}), creating artificial event\n",
                e_type,
                repo,
                actor_id,
                json_time(created_at)
            );
            return None;
        }
    };
    // verify the object id via gha_payloads; NULL/missing payload value is acceptable only
    // for a single candidate (payload gets enriched on reuse), a different value is not
    let mut matched: Vec<i64> = Vec::new();
    let mut weak: Vec<i64> = Vec::new();
    for cid in &ids {
        let mut prows = query_sql_tx_with_err(
            tc,
            ctx,
            &format!(
                "select {} from gha_payloads where event_id = {}",
                payload_col,
                n_value(1)
            ),
            &[SqlArg::Int(*cid)],
        );
        let mut pv: Option<i64> = None;
        let mut found = false;
        while prows.next() {
            fatal_on_err(prows.scan(&mut [&mut pv]));
            found = true;
        }
        fatal_on_err(prows.err());
        fatal_on_err(prows.close());
        match pv {
            Some(pv) if found => {
                if pv == oid {
                    matched.push(*cid);
                }
            }
            _ => weak.push(*cid),
        }
    }
    if matched.len() == 1 {
        return Some(matched[0]);
    }
    if matched.is_empty() && weak.len() == 1 && ids.len() == 1 {
        return Some(weak[0]);
    }
    if matched.len() > 1 || !weak.is_empty() {
        printf!(
            "findRawEventID: cannot verify raw event for ({}, {}, {}, {}, {}={}), creating artificial event\n",
            e_type,
            repo,
            actor_id,
            json_time(created_at),
            payload_col,
            oid
        );
    }
    None
}

/// Go `hashIDConflict`: is the hash-based `eid` already taken by a different
/// (type, repo, actor, created_at) event?
fn hash_id_conflict(
    tc: &mut PgTx<'_>,
    ctx: &Ctx,
    eid: i64,
    e_type: &str,
    repo: &str,
    actor_id: i64,
    created_at: DateTime<FixedOffset>,
) -> bool {
    let mut rows = query_sql_tx_with_err(
        tc,
        ctx,
        &format!(
            "select type, dup_repo_name, actor_id, created_at from gha_events where id = {}",
            n_value(1)
        ),
        &[SqlArg::Int(eid)],
    );
    let mut conflict = false;
    let mut e_t = String::new();
    let mut e_r = String::new();
    let mut e_a: i64 = 0;
    let mut e_d: Option<DateTime<FixedOffset>> = None;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut e_t, &mut e_r, &mut e_a, &mut e_d]));
        let same_time = e_d.map(|d| d == created_at).unwrap_or(false);
        if e_t != e_type || e_r != repo || e_a != actor_id || !same_time {
            conflict = true;
        }
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    if conflict {
        // Go prints the zero `time.Time{}` when the row had no created_at.
        let e_d_str = match e_d {
            Some(d) => go_time_string(&d),
            None => "0001-01-01 00:00:00 +0000 UTC".to_string(),
        };
        printf!(
            "hash id {} conflict: existing ({}, {}, {}, {}) vs new ({}, {}, {}, {}), skipping\n",
            eid,
            e_t,
            e_r,
            e_a,
            e_d_str,
            e_type,
            repo,
            actor_id,
            json_time(created_at)
        );
    }
    conflict
}

fn artificial_id_ok(eid: i64, what: &str, repo: &str) -> bool {
    if eid >= SYNC_EVENT_ID_THRESHOLD {
        printf!(
            "{}: {}: artificial event id {} reached the sync events range, skipping\n",
            what,
            repo,
            eid
        );
        return false;
    }
    true
}

/// The `gha_payloads` columns of a restored event (Go's nil-able
/// `interface{}` arguments of `restoreEventPayload`).
#[derive(Clone, Debug, Default)]
struct Payload {
    action: SqlArg,
    number: SqlArg,
    issue_id: SqlArg,
    pr_id: SqlArg,
    comment_id: SqlArg,
    forkee_id: SqlArg,
    release_id: SqlArg,
    commit_sha: SqlArg,
}

/// Go `restoreEventPayload`: the `gha_events` row (insert-ignore, its
/// result is returned) and the `gha_payloads` row (upsert filling NULLs).
#[allow(clippy::too_many_arguments)]
fn restore_event_payload(
    tc: &mut PgTx<'_>,
    ctx: &Ctx,
    eid: i64,
    e_type: &str,
    actor: Option<&User>,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    created_at: DateTime<FixedOffset>,
    payload: Payload,
    maybe_hide: MaybeHide,
) -> ExecResult {
    let res = exec_sql_tx_with_err(
        tc,
        ctx,
        &insert_ignore(&format!(
            "into gha_events(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name, org_id) {}",
            n_values(8)
        )),
        &[
            SqlArg::Int(eid),
            SqlArg::from(e_type),
            gh_actor_id_or_nil(actor),
            SqlArg::Int(repo_id),
            SqlArg::Time(created_at),
            gh_actor_login_or_nil(actor, maybe_hide),
            SqlArg::from(repo),
            org_id.clone(),
        ],
    );
    exec_sql_tx_with_err(
        tc,
        ctx,
        &format!(
            "insert into gha_payloads(event_id, action, number, issue_id, pull_request_id, comment_id, forkee_id, release_id, commit, \
             dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) {} \
             on conflict(event_id) do update set \
             action = coalesce(gha_payloads.action, excluded.action), \
             number = coalesce(gha_payloads.number, excluded.number), \
             issue_id = coalesce(gha_payloads.issue_id, excluded.issue_id), \
             pull_request_id = coalesce(gha_payloads.pull_request_id, excluded.pull_request_id), \
             comment_id = coalesce(gha_payloads.comment_id, excluded.comment_id), \
             forkee_id = coalesce(gha_payloads.forkee_id, excluded.forkee_id), \
             release_id = coalesce(gha_payloads.release_id, excluded.release_id), \
             commit = coalesce(gha_payloads.commit, excluded.commit)",
            n_values(14)
        ),
        &[
            SqlArg::Int(eid),
            payload.action,
            payload.number,
            payload.issue_id,
            payload.pr_id,
            payload.comment_id,
            payload.forkee_id,
            payload.release_id,
            payload.commit_sha,
            gh_actor_login_or_nil(actor, maybe_hide),
            SqlArg::Int(repo_id),
            SqlArg::from(repo),
            SqlArg::from(e_type),
            SqlArg::Time(created_at),
        ],
    );
    res
}

fn rows_restored(res: &ExecResult) -> bool {
    res.rows_affected().map(|n| n > 0).unwrap_or(false)
}

fn user_ok(user: Option<&User>) -> bool {
    matches!(user, Some(u) if u.login.is_some())
}

/// Go `RestoreIssueComment`: restores a comment missed by GH Archive —
/// artificial `IssueCommentEvent` + `gha_comments` row.
#[allow(clippy::too_many_arguments)]
pub fn restore_issue_comment(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    issue_number: i64,
    cmt: Option<&IssueComment>,
    maybe_hide: MaybeHide,
) -> (i64, bool) {
    if ctx.skip_pdb {
        return (0, false);
    }
    let cmt = match cmt {
        Some(c) if c.id.is_some() && user_ok(c.user.as_ref()) && c.created_at.is_some() => c,
        _ => {
            printf!(
                "RestoreIssueComment: {}: skipping comment with missing id/user/created_at\n",
                repo
            );
            return (0, false);
        }
    };
    let cid = cmt.id.unwrap_or(0);
    let mut eid = ARTIFICIAL_COMMENT_ID_BASE + cid;
    if !artificial_id_ok(eid, "RestoreIssueComment", repo) {
        return (0, false);
    }
    let created_at = cmt.created_at.map(|t| t.0).unwrap_or_default();
    let e_type = "IssueCommentEvent";
    let mut tc = begin_tx(c);
    if let Some(raw) = find_raw_event_id(
        &mut tc,
        ctx,
        e_type,
        repo,
        cmt.user.as_ref(),
        created_at,
        "comment_id",
        Some(cid),
    ) {
        eid = raw;
    }
    gh_actor(&mut tc, ctx, cmt.user.as_ref(), maybe_hide);
    let issue_id = lookup_id(
        &mut tc,
        ctx,
        &format!(
            "select max(id) from gha_issues where number = {} and dup_repo_name = {}",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::Int(issue_number), SqlArg::from(repo)],
    );
    restore_event_payload(
        &mut tc,
        ctx,
        eid,
        e_type,
        cmt.user.as_ref(),
        repo,
        repo_id,
        org_id,
        created_at,
        Payload {
            action: SqlArg::from("created"),
            number: SqlArg::Int(issue_number),
            issue_id,
            comment_id: SqlArg::Int(cid),
            ..Default::default()
        },
        maybe_hide,
    );
    let res = exec_sql_tx_with_err(
        &mut tc,
        ctx,
        &insert_ignore(&format!(
            "into gha_comments(id, event_id, body, created_at, updated_at, user_id, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) \
             values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            n_value(1),
            n_value(2),
            n_value(3),
            n_value(4),
            n_value(5),
            n_value(6),
            n_value(7),
            n_value(8),
            n_value(9),
            n_value(10),
            n_value(11),
            n_value(12),
            n_value(13),
        )),
        &[
            SqlArg::Int(cid),
            SqlArg::Int(eid),
            SqlArg::Str(trunc_to_bytes(
                &maybe_hide(&str_or(cmt.body.as_deref(), "")),
                0xffff,
            )),
            SqlArg::Time(created_at),
            SqlArg::Time(time_or(cmt.updated_at.as_ref(), created_at)),
            gh_actor_id_or_nil(cmt.user.as_ref()),
            gh_actor_id_or_nil(cmt.user.as_ref()),
            gh_actor_login_or_nil(cmt.user.as_ref(), maybe_hide),
            SqlArg::Int(repo_id),
            SqlArg::from(repo),
            SqlArg::from(e_type),
            SqlArg::Time(created_at),
            gh_actor_login_or_nil(cmt.user.as_ref(), maybe_hide),
        ],
    );
    fatal_on_pg_err(tc.commit());
    (eid, rows_restored(&res))
}

/// Go `RestoreReviewComment`: restores a PR review comment missed by GH
/// Archive — artificial `PullRequestReviewCommentEvent` + `gha_comments` row.
#[allow(clippy::too_many_arguments)]
pub fn restore_review_comment(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    pr_number: i64,
    cmt: Option<&PullRequestComment>,
    maybe_hide: MaybeHide,
) -> (i64, bool) {
    if ctx.skip_pdb {
        return (0, false);
    }
    let cmt = match cmt {
        Some(c) if c.id.is_some() && user_ok(c.user.as_ref()) && c.created_at.is_some() => c,
        _ => {
            printf!(
                "RestoreReviewComment: {}: skipping comment with missing id/user/created_at\n",
                repo
            );
            return (0, false);
        }
    };
    let cid = cmt.id.unwrap_or(0);
    let mut eid = ARTIFICIAL_REVIEW_COMMENT_ID_BASE + cid;
    if !artificial_id_ok(eid, "RestoreReviewComment", repo) {
        return (0, false);
    }
    let created_at = cmt.created_at.map(|t| t.0).unwrap_or_default();
    let e_type = "PullRequestReviewCommentEvent";
    let mut tc = begin_tx(c);
    if let Some(raw) = find_raw_event_id(
        &mut tc,
        ctx,
        e_type,
        repo,
        cmt.user.as_ref(),
        created_at,
        "comment_id",
        Some(cid),
    ) {
        eid = raw;
    }
    gh_actor(&mut tc, ctx, cmt.user.as_ref(), maybe_hide);
    let pr_id = lookup_id(
        &mut tc,
        ctx,
        &format!(
            "select max(id) from gha_pull_requests where number = {} and dup_repo_name = {}",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::Int(pr_number), SqlArg::from(repo)],
    );
    restore_event_payload(
        &mut tc,
        ctx,
        eid,
        e_type,
        cmt.user.as_ref(),
        repo,
        repo_id,
        org_id,
        created_at,
        Payload {
            action: SqlArg::from("created"),
            number: SqlArg::Int(pr_number),
            pr_id,
            comment_id: SqlArg::Int(cid),
            ..Default::default()
        },
        maybe_hide,
    );
    let res = exec_sql_tx_with_err(
        &mut tc,
        ctx,
        &insert_ignore(&format!(
            "into gha_comments(id, event_id, body, created_at, updated_at, user_id, \
             commit_id, original_commit_id, position, original_position, path, pull_request_review_id, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) \
             values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            n_value(1),
            n_value(2),
            n_value(3),
            n_value(4),
            n_value(5),
            n_value(6),
            n_value(7),
            n_value(8),
            n_value(9),
            n_value(10),
            n_value(11),
            n_value(12),
            n_value(13),
            n_value(14),
            n_value(15),
            n_value(16),
            n_value(17),
            n_value(18),
            n_value(19),
        )),
        &[
            SqlArg::Int(cid),
            SqlArg::Int(eid),
            SqlArg::Str(trunc_to_bytes(
                &maybe_hide(&str_or(cmt.body.as_deref(), "")),
                0xffff,
            )),
            SqlArg::Time(created_at),
            SqlArg::Time(time_or(cmt.updated_at.as_ref(), created_at)),
            gh_actor_id_or_nil(cmt.user.as_ref()),
            string_or_nil(cmt.commit_id.as_deref()),
            string_or_nil(cmt.original_commit_id.as_deref()),
            int_or_nil(cmt.position),
            int_or_nil(cmt.original_position),
            string_or_nil(cmt.path.as_deref()),
            int_or_nil(cmt.pull_request_review_id),
            gh_actor_id_or_nil(cmt.user.as_ref()),
            gh_actor_login_or_nil(cmt.user.as_ref(), maybe_hide),
            SqlArg::Int(repo_id),
            SqlArg::from(repo),
            SqlArg::from(e_type),
            SqlArg::Time(created_at),
            gh_actor_login_or_nil(cmt.user.as_ref(), maybe_hide),
        ],
    );
    fatal_on_pg_err(tc.commit());
    (eid, rows_restored(&res))
}

/// Go `RestoreReview`: restores a PR review missed by GH Archive —
/// artificial `PullRequestReviewEvent` + `gha_reviews` row.
#[allow(clippy::too_many_arguments)]
pub fn restore_review(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    pr_number: i64,
    rev: Option<&PullRequestReview>,
    maybe_hide: MaybeHide,
) -> (i64, bool) {
    if ctx.skip_pdb {
        return (0, false);
    }
    let rev = match rev {
        Some(r) if r.id.is_some() && user_ok(r.user.as_ref()) && r.submitted_at.is_some() => r,
        _ => return (0, false),
    };
    let rid = rev.id.unwrap_or(0);
    let mut eid = ARTIFICIAL_REVIEW_ID_BASE + rid;
    if !artificial_id_ok(eid, "RestoreReview", repo) {
        return (0, false);
    }
    let created_at = rev.submitted_at.map(|t| t.0).unwrap_or_default();
    let e_type = "PullRequestReviewEvent";
    let mut tc = begin_tx(c);
    if let Some(raw) = find_raw_event_id(
        &mut tc,
        ctx,
        e_type,
        repo,
        rev.user.as_ref(),
        created_at,
        "",
        None,
    ) {
        eid = raw;
    }
    gh_actor(&mut tc, ctx, rev.user.as_ref(), maybe_hide);
    let pr_id = lookup_id(
        &mut tc,
        ctx,
        &format!(
            "select max(id) from gha_pull_requests where number = {} and dup_repo_name = {}",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::Int(pr_number), SqlArg::from(repo)],
    );
    restore_event_payload(
        &mut tc,
        ctx,
        eid,
        e_type,
        rev.user.as_ref(),
        repo,
        repo_id,
        org_id,
        created_at,
        Payload {
            action: SqlArg::from("created"),
            number: SqlArg::Int(pr_number),
            pr_id,
            ..Default::default()
        },
        maybe_hide,
    );
    let res = exec_sql_tx_with_err(
        &mut tc,
        ctx,
        &insert_ignore(&format!(
            "into gha_reviews(id, user_id, commit_id, submitted_at, author_association, state, body, event_id, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) \
             values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            n_value(1),
            n_value(2),
            n_value(3),
            n_value(4),
            n_value(5),
            n_value(6),
            n_value(7),
            n_value(8),
            n_value(9),
            n_value(10),
            n_value(11),
            n_value(12),
            n_value(13),
            n_value(14),
            n_value(15),
        )),
        &[
            SqlArg::Int(rid),
            gh_actor_id_or_nil(rev.user.as_ref()),
            SqlArg::Str(str_or(rev.commit_id.as_deref(), "")),
            SqlArg::Time(created_at),
            SqlArg::Str(str_or(rev.author_association.as_deref(), "NONE")),
            SqlArg::Str(str_or(rev.state.as_deref(), "")),
            trunc_string_or_nil_hidden(rev.body.as_deref(), 0xffff, maybe_hide),
            SqlArg::Int(eid),
            gh_actor_id_or_nil(rev.user.as_ref()),
            gh_actor_login_or_nil(rev.user.as_ref(), maybe_hide),
            SqlArg::Int(repo_id),
            SqlArg::from(repo),
            SqlArg::from(e_type),
            SqlArg::Time(created_at),
            gh_actor_login_or_nil(rev.user.as_ref(), maybe_hide),
        ],
    );
    fatal_on_pg_err(tc.commit());
    (eid, rows_restored(&res))
}

/// Go `RestoreFork`: restores a fork missed by GH Archive — artificial
/// `ForkEvent` + `gha_forkees` row.
pub fn restore_fork(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    fork: Option<&Repository>,
    maybe_hide: MaybeHide,
) -> (i64, bool) {
    if ctx.skip_pdb {
        return (0, false);
    }
    let fork = match fork {
        Some(f) if f.id.is_some() && user_ok(f.owner.as_ref()) && f.created_at.is_some() => f,
        _ => return (0, false),
    };
    let fid = fork.id.unwrap_or(0);
    let mut eid = ARTIFICIAL_FORK_ID_BASE + fid;
    if !artificial_id_ok(eid, "RestoreFork", repo) {
        return (0, false);
    }
    let created_at = fork
        .created_at
        .map(|t| t.0.fixed_offset())
        .unwrap_or_default();
    let e_type = "ForkEvent";
    let mut tc = begin_tx(c);
    if let Some(raw) = find_raw_event_id(
        &mut tc,
        ctx,
        e_type,
        repo,
        fork.owner.as_ref(),
        created_at,
        "forkee_id",
        Some(fid),
    ) {
        eid = raw;
    }
    gh_actor(&mut tc, ctx, fork.owner.as_ref(), maybe_hide);
    restore_event_payload(
        &mut tc,
        ctx,
        eid,
        e_type,
        fork.owner.as_ref(),
        repo,
        repo_id,
        org_id,
        created_at,
        Payload {
            forkee_id: SqlArg::Int(fid),
            ..Default::default()
        },
        maybe_hide,
    );
    let res = exec_sql_tx_with_err(
        &mut tc,
        ctx,
        &insert_ignore(&format!(
            "into gha_forkees(id, event_id, name, full_name, owner_id, updated_at, \
             stargazers_count, forks, \
             open_issues, watchers, \
             dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at) \
             values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            n_value(1),
            n_value(2),
            n_value(3),
            n_value(4),
            n_value(5),
            n_value(6),
            n_value(7),
            n_value(8),
            n_value(9),
            n_value(10),
            n_value(11),
            n_value(12),
            n_value(13),
            n_value(14),
        )),
        &[
            SqlArg::Int(fid),
            SqlArg::Int(eid),
            SqlArg::Str(trunc_to_bytes(&str_or(fork.name.as_deref(), ""), 80)),
            SqlArg::Str(trunc_to_bytes(&str_or(fork.full_name.as_deref(), ""), 200)),
            gh_actor_id_or_nil(fork.owner.as_ref()),
            SqlArg::Time(ts_or(fork.updated_at.as_ref(), created_at)),
            SqlArg::Int(int_or(fork.stargazers_count, 0)),
            SqlArg::Int(int_or(fork.forks_count, 0)),
            SqlArg::Int(int_or(fork.open_issues_count, 0)),
            SqlArg::Int(int_or(fork.watchers_count, 0)),
            gh_actor_id_or_nil(fork.owner.as_ref()),
            SqlArg::Int(repo_id),
            SqlArg::from(repo),
            SqlArg::Time(created_at),
        ],
    );
    fatal_on_pg_err(tc.commit());
    (eid, rows_restored(&res))
}

/// Go `RestoreRelease`: restores a release missed by GH Archive — artificial
/// `ReleaseEvent` + `gha_releases` (+ assets) rows.
pub fn restore_release(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    rel: Option<&RepositoryRelease>,
    maybe_hide: MaybeHide,
) -> (i64, bool) {
    if ctx.skip_pdb {
        return (0, false);
    }
    let rel = match rel {
        Some(r) if r.id.is_some() && user_ok(r.author.as_ref()) && r.created_at.is_some() => r,
        _ => return (0, false),
    };
    let rid = rel.id.unwrap_or(0);
    let mut eid = ARTIFICIAL_RELEASE_ID_BASE + rid;
    if !artificial_id_ok(eid, "RestoreRelease", repo) {
        return (0, false);
    }
    let rel_created = rel
        .created_at
        .map(|t| t.0.fixed_offset())
        .unwrap_or_default();
    let created_at = ts_or(rel.published_at.as_ref(), rel_created);
    let e_type = "ReleaseEvent";
    let mut tc = begin_tx(c);
    if let Some(raw) = find_raw_event_id(
        &mut tc,
        ctx,
        e_type,
        repo,
        rel.author.as_ref(),
        created_at,
        "release_id",
        Some(rid),
    ) {
        eid = raw;
    }
    gh_actor(&mut tc, ctx, rel.author.as_ref(), maybe_hide);
    restore_event_payload(
        &mut tc,
        ctx,
        eid,
        e_type,
        rel.author.as_ref(),
        repo,
        repo_id,
        org_id,
        created_at,
        Payload {
            action: SqlArg::from("published"),
            release_id: SqlArg::Int(rid),
            ..Default::default()
        },
        maybe_hide,
    );
    let res = exec_sql_tx_with_err(
        &mut tc,
        ctx,
        &insert_ignore(&format!(
            "into gha_releases(id, event_id, tag_name, target_commitish, name, draft, author_id, prerelease, \
             created_at, published_at, body, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_author_login) \
             values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            n_value(1),
            n_value(2),
            n_value(3),
            n_value(4),
            n_value(5),
            n_value(6),
            n_value(7),
            n_value(8),
            n_value(9),
            n_value(10),
            n_value(11),
            n_value(12),
            n_value(13),
            n_value(14),
            n_value(15),
            n_value(16),
            n_value(17),
            n_value(18),
        )),
        &[
            SqlArg::Int(rid),
            SqlArg::Int(eid),
            SqlArg::Str(trunc_to_bytes(&str_or(rel.tag_name.as_deref(), ""), 200)),
            SqlArg::Str(trunc_to_bytes(
                &str_or(rel.target_commitish.as_deref(), ""),
                200,
            )),
            trunc_string_or_nil_hidden(rel.name.as_deref(), 200, maybe_hide),
            SqlArg::Bool(bool_or(rel.draft, false)),
            gh_actor_id_or_nil(rel.author.as_ref()),
            SqlArg::Bool(bool_or(rel.prerelease, false)),
            SqlArg::Time(created_at),
            ts_or_nil(rel.published_at.as_ref()),
            trunc_string_or_nil_hidden(rel.body.as_deref(), 0xffff, maybe_hide),
            gh_actor_id_or_nil(rel.author.as_ref()),
            gh_actor_login_or_nil(rel.author.as_ref(), maybe_hide),
            SqlArg::Int(repo_id),
            SqlArg::from(repo),
            SqlArg::from(e_type),
            SqlArg::Time(created_at),
            gh_actor_login_or_nil(rel.author.as_ref(), maybe_hide),
        ],
    );
    for asset in &rel.assets {
        let aid = match asset.id {
            Some(aid) => aid,
            None => continue,
        };
        let uploader = asset.uploader.as_ref().or(rel.author.as_ref());
        gh_actor(&mut tc, ctx, uploader, maybe_hide);
        exec_sql_tx_with_err(
            &mut tc,
            ctx,
            &insert_ignore(&format!(
                "into gha_assets(id, event_id, name, label, uploader_id, content_type, state, size, download_count, \
                 created_at, updated_at, \
                 dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_uploader_login) \
                 values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                n_value(1),
                n_value(2),
                n_value(3),
                n_value(4),
                n_value(5),
                n_value(6),
                n_value(7),
                n_value(8),
                n_value(9),
                n_value(10),
                n_value(11),
                n_value(12),
                n_value(13),
                n_value(14),
                n_value(15),
                n_value(16),
                n_value(17),
                n_value(18),
            )),
            &[
                SqlArg::Int(aid),
                SqlArg::Int(eid),
                SqlArg::Str(trunc_to_bytes(
                    &maybe_hide(&str_or(asset.name.as_deref(), "")),
                    200,
                )),
                trunc_string_or_nil_hidden(asset.label.as_deref(), 120, maybe_hide),
                gh_actor_id_or_nil(uploader),
                SqlArg::Str(trunc_to_bytes(
                    &str_or(asset.content_type.as_deref(), ""),
                    80,
                )),
                SqlArg::Str(trunc_to_bytes(&str_or(asset.state.as_deref(), ""), 20)),
                SqlArg::Int(int_or(asset.size, 0)),
                SqlArg::Int(int_or(asset.download_count, 0)),
                SqlArg::Time(ts_or(asset.created_at.as_ref(), created_at)),
                SqlArg::Time(ts_or(asset.updated_at.as_ref(), created_at)),
                gh_actor_id_or_nil(rel.author.as_ref()),
                gh_actor_login_or_nil(rel.author.as_ref(), maybe_hide),
                SqlArg::Int(repo_id),
                SqlArg::from(repo),
                SqlArg::from(e_type),
                SqlArg::Time(created_at),
                gh_actor_login_or_nil(uploader, maybe_hide),
            ],
        );
        exec_sql_tx_with_err(
            &mut tc,
            ctx,
            &insert_ignore(&format!(
                "into gha_releases_assets(release_id, event_id, asset_id) {}",
                n_values(3)
            )),
            &[SqlArg::Int(rid), SqlArg::Int(eid), SqlArg::Int(aid)],
        );
    }
    fatal_on_pg_err(tc.commit());
    (eid, rows_restored(&res))
}

/// Go `RestoreCommitComment`: restores a commit comment missed by GH
/// Archive — artificial `CommitCommentEvent` + `gha_comments` row.
pub fn restore_commit_comment(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    cmt: Option<&RepositoryComment>,
    maybe_hide: MaybeHide,
) -> (i64, bool) {
    if ctx.skip_pdb {
        return (0, false);
    }
    let cmt = match cmt {
        Some(c) if c.id.is_some() && user_ok(c.user.as_ref()) && c.created_at.is_some() => c,
        _ => {
            printf!(
                "RestoreCommitComment: {}: skipping comment with missing id/user/created_at\n",
                repo
            );
            return (0, false);
        }
    };
    let cid = cmt.id.unwrap_or(0);
    let mut eid = ARTIFICIAL_COMMIT_COMMENT_ID_BASE + cid;
    if !artificial_id_ok(eid, "RestoreCommitComment", repo) {
        return (0, false);
    }
    let created_at = cmt.created_at.map(|t| t.0).unwrap_or_default();
    let e_type = "CommitCommentEvent";
    let mut tc = begin_tx(c);
    if let Some(raw) = find_raw_event_id(
        &mut tc,
        ctx,
        e_type,
        repo,
        cmt.user.as_ref(),
        created_at,
        "comment_id",
        Some(cid),
    ) {
        eid = raw;
    }
    gh_actor(&mut tc, ctx, cmt.user.as_ref(), maybe_hide);
    restore_event_payload(
        &mut tc,
        ctx,
        eid,
        e_type,
        cmt.user.as_ref(),
        repo,
        repo_id,
        org_id,
        created_at,
        Payload {
            action: SqlArg::from("created"),
            comment_id: SqlArg::Int(cid),
            commit_sha: string_or_nil(cmt.commit_id.as_deref()),
            ..Default::default()
        },
        maybe_hide,
    );
    let res = exec_sql_tx_with_err(
        &mut tc,
        ctx,
        &insert_ignore(&format!(
            "into gha_comments(id, event_id, body, created_at, updated_at, user_id, commit_id, position, path, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) \
             values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            n_value(1),
            n_value(2),
            n_value(3),
            n_value(4),
            n_value(5),
            n_value(6),
            n_value(7),
            n_value(8),
            n_value(9),
            n_value(10),
            n_value(11),
            n_value(12),
            n_value(13),
            n_value(14),
            n_value(15),
            n_value(16),
        )),
        &[
            SqlArg::Int(cid),
            SqlArg::Int(eid),
            SqlArg::Str(trunc_to_bytes(
                &maybe_hide(&str_or(cmt.body.as_deref(), "")),
                0xffff,
            )),
            SqlArg::Time(created_at),
            SqlArg::Time(time_or(cmt.updated_at.as_ref(), created_at)),
            gh_actor_id_or_nil(cmt.user.as_ref()),
            string_or_nil(cmt.commit_id.as_deref()),
            int_or_nil(cmt.position),
            string_or_nil(cmt.path.as_deref()),
            gh_actor_id_or_nil(cmt.user.as_ref()),
            gh_actor_login_or_nil(cmt.user.as_ref(), maybe_hide),
            SqlArg::Int(repo_id),
            SqlArg::from(repo),
            SqlArg::from(e_type),
            SqlArg::Time(created_at),
            gh_actor_login_or_nil(cmt.user.as_ref(), maybe_hide),
        ],
    );
    fatal_on_pg_err(tc.commit());
    (eid, rows_restored(&res))
}

/// Go `RestoreStar`: restores a star missed by GH Archive as an artificial
/// `WatchEvent` (hash-based negative id, like pre-2015 events).
pub fn restore_star(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
    repo_id: i64,
    org_id: &SqlArg,
    star: Option<&Stargazer>,
    maybe_hide: MaybeHide,
) -> (i64, bool) {
    if ctx.skip_pdb {
        return (0, false);
    }
    let star = match star {
        Some(s) if user_ok(s.user.as_ref()) && s.starred_at.is_some() => s,
        _ => return (0, false),
    };
    let created_at = star
        .starred_at
        .map(|t| t.0.fixed_offset())
        .unwrap_or_default();
    let e_type = "WatchEvent";
    let user_id = match star.user.as_ref().and_then(|u| u.id) {
        Some(id) => id,
        None => return (0, false),
    };
    let eid = negative_artificial_id(&[
        e_type,
        &user_id.to_string(),
        repo,
        &to_ymdhms_date(created_at),
    ]);
    let mut tc = begin_tx(c);
    if hash_id_conflict(&mut tc, ctx, eid, e_type, repo, user_id, created_at) {
        fatal_on_pg_err(tc.rollback());
        return (0, false);
    }
    gh_actor(&mut tc, ctx, star.user.as_ref(), maybe_hide);
    let res = restore_event_payload(
        &mut tc,
        ctx,
        eid,
        e_type,
        star.user.as_ref(),
        repo,
        repo_id,
        org_id,
        created_at,
        Payload {
            action: SqlArg::from("started"),
            ..Default::default()
        },
        maybe_hide,
    );
    fatal_on_pg_err(tc.commit());
    (eid, rows_restored(&res))
}

/// Go `RunRangePostprocess`: [`run_range_postprocess_db`] on `ctx.pg_db`.
pub fn run_range_postprocess(ctx: &Ctx, from: DateTime<Utc>, to: DateTime<Utc>) {
    run_range_postprocess_db(ctx, "", from, to)
}

/// Go `RunRangePostprocessDB`: run the bounded `util_sql/postprocess_*_range.sql`
/// scripts for `[from, to)` (`db` empty = `ctx.pg_db`), each in its own
/// transaction. Nothing happens with `GHA2DB_SKIPPDB` or an empty `gha_texts`.
pub fn run_range_postprocess_db(ctx: &Ctx, db: &str, from: DateTime<Utc>, to: DateTime<Utc>) {
    if ctx.skip_pdb {
        return;
    }
    let mut c = pg_conn(ctx);
    if !db.is_empty() {
        c.close();
        c = pg_conn_db_shared(ctx, db);
    }
    if gha_texts_empty(&c, ctx) {
        printf!(
            "bounded postprocess skipped: gha_texts is empty, full structure rebuild pending\n"
        );
        c.close();
        return;
    }
    let span_days = (to - from).num_seconds() as f64 / 3600.0 / 24.0;
    if span_days > 62.0 {
        printf!(
            "WARNING: bounded postprocess window [{}, {}) spans {} days - this rebuilds every row in the window and can take hours on big databases, consider the targeted event-ids mode or a full truncate+structure rebuild instead\n",
            to_ymdhms_date(from),
            to_ymdhms_date(to),
            format!("{:.1}", span_days)
        );
    }
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    // each script is one delete+reinsert of a single table: run each in its own transaction
    // (set_config with is_local=true is transaction-scoped, so the window is set per script)
    for script in [
        "util_sql/postprocess_texts_range.sql",
        "util_sql/postprocess_labels_range.sql",
        "util_sql/postprocess_issues_prs_range.sql",
    ] {
        let bytes = fatal_on_err(read_file(ctx, &format!("{}{}", data_prefix, script)));
        let mut tc = begin_tx(&c);
        exec_sql_tx_with_err(
            &mut tc,
            ctx,
            &format!(
                "select set_config('devstats.postprocess_from', {}, true)",
                n_value(1)
            ),
            &[SqlArg::Str(to_ymdhms_date(from))],
        );
        exec_sql_tx_with_err(
            &mut tc,
            ctx,
            &format!(
                "select set_config('devstats.postprocess_to', {}, true)",
                n_value(1)
            ),
            &[SqlArg::Str(to_ymdhms_date(to))],
        );
        exec_sql_tx_with_err(&mut tc, ctx, &String::from_utf8_lossy(&bytes), &[]);
        fatal_on_pg_err(tc.commit());
    }
    c.close();
    printf!(
        "bounded postprocess executed for [{}, {})\n",
        to_ymdhms_date(from),
        to_ymdhms_date(to)
    );
}

/// Is `gha_texts` empty (a truncated target whose full structure rebuild is
/// pending)?
fn gha_texts_empty(c: &PgConn, ctx: &Ctx) -> bool {
    let mut rows = query_sql_with_err(c, ctx, "select 1 from gha_texts limit 1", &[]);
    let mut empty = true;
    while rows.next() {
        empty = false;
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    empty
}

/// Go `RunEventIDsPostprocess`: [`run_event_ids_postprocess_db`] on `ctx.pg_db`.
pub fn run_event_ids_postprocess(ctx: &Ctx, eids: &[i64]) {
    run_event_ids_postprocess_db(ctx, "", eids)
}

/// `con.Begin()` + `FatalOnError`: on a retryable condition the begin is
/// retried (Go would go on with a nil transaction).
fn begin_tx(con: &PgConn) -> PgTx<'_> {
    loop {
        match con.begin() {
            Ok(tx) => return tx,
            Err(e) => {
                fatal_on_pg_error(&e);
            }
        }
    }
}

/// Go `RunEventIDsPostprocessDB`: run the targeted `util_sql/postprocess_*_ids.sql`
/// scripts for exactly the given event ids (`db` empty = `ctx.pg_db`). Nothing
/// happens with `GHA2DB_SKIPPDB`, an empty id list or an empty `gha_texts`
/// table (the full structure rebuild is pending then).
pub fn run_event_ids_postprocess_db(ctx: &Ctx, db: &str, eids: &[i64]) {
    if ctx.skip_pdb || eids.is_empty() {
        return;
    }
    let mut c = pg_conn(ctx);
    if !db.is_empty() {
        c.close();
        // Go's PgConnDB also clears ctx.CanReconnect; callers of this
        // function have already run with a shared, immutable context.
        c = pg_conn_db_shared(ctx, db);
    }
    if gha_texts_empty(&c, ctx) {
        printf!(
            "targeted postprocess skipped: gha_texts is empty, full structure rebuild pending\n"
        );
        c.close();
        return;
    }
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    let mut tc = begin_tx(&c);
    exec_sql_tx_with_err(
        &mut tc,
        ctx,
        "create temp table pp_event_ids(event_id bigint not null) on commit drop",
        &[],
    );
    let batch = 10000;
    for chunk in eids.chunks(batch) {
        let mut sb = String::from("insert into pp_event_ids(event_id) values ");
        for (k, eid) in chunk.iter().enumerate() {
            if k > 0 {
                sb.push(',');
            }
            sb.push('(');
            sb.push_str(&eid.to_string());
            sb.push(')');
        }
        exec_sql_tx_with_err(&mut tc, ctx, &sb, &[]);
    }
    exec_sql_tx_with_err(&mut tc, ctx, "analyze pp_event_ids", &[]);
    for script in [
        "util_sql/postprocess_texts_ids.sql",
        "util_sql/postprocess_labels_ids.sql",
        "util_sql/postprocess_issues_prs_ids.sql",
    ] {
        let bytes = fatal_on_err(read_file(ctx, &format!("{}{}", data_prefix, script)));
        exec_sql_tx_with_err(&mut tc, ctx, &String::from_utf8_lossy(&bytes), &[]);
    }
    fatal_on_pg_err(tc.commit());
    c.close();
    printf!(
        "targeted postprocess executed for {} restored event id(s)\n",
        eids.len()
    );
}
