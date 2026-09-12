//! Whole-event writers — port of `ghaPullRequest`, `ghaTeam`,
//! `writeToDBOldFmt` and `writeToDB` of `cmd/gha2db/gha2db.go`.

use devstatscode::gha::{
    actor_id_or_nil, actor_login_or_nil, comment_id_or_nil, forkee_id_or_nil, issue_id_or_nil,
    milestone_id_or_nil, org_id_or_nil, org_login_or_nil, pull_request_id_or_nil,
    release_id_or_nil, Actor, Event, EventOld, Forkee, GhaTime, Org, PullRequest, Repo, Team,
};
use devstatscode::hash::hash_strings;
use devstatscode::pg::api::{
    bool_or_nil, clean_utf8, exec_sql_tx_with_err, exec_sql_with_err, first_int_or_nil,
    insert_ignore, int_or_nil, n_values, string_or_nil, trunc_string_or_nil, trunc_to_bytes,
};
use devstatscode::pg::{PgConn, PgTx, SqlArg};
use devstatscode::{fatal_on_err, fatalf, Ctx};

use crate::db::{
    event_exists_collision, find_org_id_or_nil, find_repo_from_name_and_org, gha_actor, gha_branch,
    gha_comment, gha_commits_roles, gha_forkee, gha_forkee_old, gha_milestone, gha_org, gha_pages,
    gha_release, gha_repo, gha_review, lookup_actor, lookup_label, Db, MaybeHide,
};

/// Go `ghaPullRequest` (PR, its branches/forkees, milestone, assignees and
/// requested reviewers).
#[allow(clippy::too_many_arguments)]
pub fn gha_pull_request(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    payload_pull_request: Option<&PullRequest>,
    event_id: &str,
    actor: &Actor,
    repo: &Repo,
    e_type: &str,
    e_created_at: GhaTime,
    forkee_ids_to_skip: &[i64],
    maybe_hide: MaybeHide<'_>,
) {
    let Some(pr) = payload_pull_request else {
        return;
    };

    // user
    gha_actor(tx, ctx, &pr.user, maybe_hide);

    let base_sha = &pr.base.sha;
    let head_sha = &pr.head.sha;
    let base_repo_id = pr.base.repo.as_ref().map(|r| r.id);

    // Create Event
    let ev = Event {
        actor: actor.clone(),
        repo: repo.clone(),
        type_: e_type.to_string(),
        created_at: e_created_at,
        ..Event::default()
    };

    // base
    gha_branch(
        tx,
        ctx,
        event_id,
        &pr.base,
        &ev,
        forkee_ids_to_skip,
        maybe_hide,
    );

    // head (if different, and skip its repo if defined and the same as base repo)
    if base_sha != head_sha {
        let mut skip = forkee_ids_to_skip.to_vec();
        if let Some(id) = base_repo_id {
            skip.push(id);
        }
        gha_branch(tx, ctx, event_id, &pr.head, &ev, &skip, maybe_hide);
    }

    // merged_by
    if let Some(merged_by) = &pr.merged_by {
        gha_actor(tx, ctx, merged_by, maybe_hide);
    }

    // assignee
    if let Some(assignee) = &pr.assignee {
        gha_actor(tx, ctx, assignee, maybe_hide);
    }

    // milestone
    if let Some(milestone) = &pr.milestone {
        gha_milestone(tx, ctx, event_id, milestone, &ev, maybe_hide);
    }

    // pull_request
    let prid = pr.id;
    exec_sql_tx_with_err(
        tx,
        ctx,
        &format!(
            "insert into gha_pull_requests(\
             id, event_id, user_id, base_sha, head_sha, merged_by_id, assignee_id, milestone_id, \
             number, state, locked, title, body, created_at, updated_at, closed_at, merged_at, \
             merge_commit_sha, merged, mergeable, rebaseable, mergeable_state, comments, \
             review_comments, maintainer_can_modify, commits, additions, deletions, changed_files, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
             dup_user_login, dupn_merged_by_login) {}",
            n_values(37)
        ),
        &[
            SqlArg::Int(prid),
            SqlArg::from(event_id),
            SqlArg::Int(pr.user.id),
            SqlArg::from(base_sha),
            SqlArg::from(head_sha),
            actor_id_or_nil(pr.merged_by.as_ref()),
            actor_id_or_nil(pr.assignee.as_ref()),
            milestone_id_or_nil(pr.milestone.as_ref()),
            SqlArg::Int(pr.number),
            SqlArg::from(&pr.state),
            bool_or_nil(pr.locked),
            SqlArg::Str(clean_utf8(&pr.title)),
            trunc_string_or_nil(pr.body.as_deref(), 0xffff),
            SqlArg::from(pr.created_at),
            SqlArg::from(pr.updated_at),
            SqlArg::from(pr.closed_at),
            SqlArg::from(pr.merged_at),
            string_or_nil(pr.merge_commit_sha.as_deref()),
            bool_or_nil(pr.merged),
            bool_or_nil(pr.mergeable),
            bool_or_nil(pr.rebaseable),
            string_or_nil(pr.mergeable_state.as_deref()),
            int_or_nil(pr.comments),
            int_or_nil(pr.review_comments),
            bool_or_nil(pr.maintainer_can_modify),
            int_or_nil(pr.commits),
            int_or_nil(pr.additions),
            int_or_nil(pr.deletions),
            int_or_nil(pr.changed_files),
            SqlArg::Int(actor.id),
            SqlArg::Str(maybe_hide(&actor.login)),
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            SqlArg::from(e_type),
            SqlArg::from(e_created_at),
            SqlArg::Str(maybe_hide(&pr.user.login)),
            actor_login_or_nil(pr.merged_by.as_ref(), &maybe_hide),
        ],
    );

    // Arrays: actors: assignees, requested_reviewers
    for assignee in pr_assignees(pr) {
        // assignee
        gha_actor(tx, ctx, assignee, maybe_hide);

        // pull_request-assignee connection
        exec_sql_tx_with_err(
            tx,
            ctx,
            &format!(
                "insert into gha_pull_requests_assignees(pull_request_id, event_id, assignee_id) {}",
                n_values(3)
            ),
            &[SqlArg::Int(prid), SqlArg::from(event_id), SqlArg::Int(assignee.id)],
        );
    }

    // requested_reviewers
    if let Some(reviewers) = &pr.requested_reviewers {
        for reviewer in reviewers {
            // reviewer
            gha_actor(tx, ctx, reviewer, maybe_hide);

            // pull_request-requested_reviewer connection
            exec_sql_tx_with_err(
                tx,
                ctx,
                &format!(
                    "insert into gha_pull_requests_requested_reviewers(pull_request_id, event_id, requested_reviewer_id) {}",
                    n_values(3)
                ),
                &[SqlArg::Int(prid), SqlArg::from(event_id), SqlArg::Int(reviewer.id)],
            );
        }
    }
}

/// The PR's assignee followed by its `assignees` (skipping the one equal to
/// the assignee), as both Go loops build them.
fn pr_assignees(pr: &PullRequest) -> Vec<&Actor> {
    let mut assignees = Vec::new();
    let pr_aid = pr.assignee.as_ref().map(|a| a.id);
    if let Some(a) = &pr.assignee {
        assignees.push(a);
    }
    if let Some(list) = &pr.assignees {
        for assignee in list {
            if Some(assignee.id) == pr_aid {
                continue;
            }
            assignees.push(assignee);
        }
    }
    assignees
}

/// Go `ghaTeam` (pre-2015 team events).
#[allow(clippy::too_many_arguments)]
pub fn gha_team(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    payload_team: Option<&Team>,
    payload_repo: Option<&Forkee>,
    event_id: &str,
    actor: &Actor,
    repo: &Repo,
    e_type: &str,
    e_created_at: GhaTime,
    maybe_hide: MaybeHide<'_>,
) {
    let Some(team) = payload_team else {
        return;
    };

    // team
    let tid = team.id;
    exec_sql_tx_with_err(
        tx,
        ctx,
        &format!(
            "insert into gha_teams(\
             id, event_id, name, slug, permission, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at\
             ) {}",
            n_values(11)
        ),
        &[
            SqlArg::Int(tid),
            SqlArg::from(event_id),
            SqlArg::Str(trunc_to_bytes(&team.name, 120)),
            SqlArg::Str(trunc_to_bytes(&team.slug, 100)),
            SqlArg::Str(trunc_to_bytes(&team.permission, 20)),
            SqlArg::Int(actor.id),
            SqlArg::Str(maybe_hide(&actor.login)),
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            SqlArg::from(e_type),
            SqlArg::from(e_created_at),
        ],
    );

    // team-repository connection
    if let Some(prepo) = payload_repo {
        exec_sql_tx_with_err(
            tx,
            ctx,
            &format!(
                "insert into gha_teams_repositories(team_id, event_id, repository_id) {}",
                n_values(3)
            ),
            &[
                SqlArg::Int(tid),
                SqlArg::from(event_id),
                SqlArg::Int(prepo.id),
            ],
        );
    }
}

/// One `[sha, email, message, author name, distinct]` entry of the old
/// format's `payload.shas`; Go type-asserts each element and panics on a
/// mismatch — reproduced as a fatal error.
struct OldSha<'a> {
    sha: &'a str,
    message: &'a str,
    name: &'a str,
    distinct: bool,
}

/// Go's dynamic type name of a decoded `interface{}` JSON value.
fn go_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "nil",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "float64",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "[]interface {}",
        serde_json::Value::Object(_) => "map[string]interface {}",
    }
}

fn old_sha(comm: &serde_json::Value) -> OldSha<'_> {
    let Some(commit) = comm.as_array() else {
        fatalf!("comm is not []interface{{}}: {}", comm);
    };
    let elem = |i: usize| -> &serde_json::Value {
        match commit.get(i) {
            Some(v) => v,
            None => fatalf!(
                "runtime error: index out of range [{}] with length {}",
                i,
                commit.len()
            ),
        }
    };
    let str_at = |i: usize| -> &str {
        match elem(i).as_str() {
            Some(s) => s,
            None => fatalf!(
                "interface conversion: interface {{}} is {}, not string",
                go_type_name(elem(i))
            ),
        }
    };
    let sha = str_at(0);
    let message = str_at(2);
    let name = str_at(3);
    let distinct = match elem(4).as_bool() {
        Some(b) => b,
        None => fatalf!(
            "interface conversion: interface {{}} is {}, not bool",
            go_type_name(elem(4))
        ),
    };
    OldSha {
        sha,
        message,
        name,
        distinct,
    }
}

/// Go `writeToDBOldFmt`: write an entire event in the old pre-2015 format;
/// `1` when written, `0` when it already existed or had no payload. Go
/// builds the GDPR `maybeHide` closure per event from the SHA map; the
/// caller passes an equivalent (thread safe) one here.
pub fn write_to_db_old_fmt(
    db: &PgConn,
    ctx: &Ctx,
    event_id: &str,
    ev: &EventOld,
    maybe_hide: MaybeHide<'_>,
) -> i64 {
    if event_exists_collision(
        db,
        ctx,
        event_id,
        &ev.type_,
        &ev.repository.name,
        ev.created_at,
    ) {
        return 0;
    }

    // Lookup author by GitHub login
    let aid = lookup_actor(&mut Db::Con(db), ctx, &ev.actor, maybe_hide);
    let actor = Actor {
        id: aid,
        login: ev.actor.clone(),
        name: String::new(),
    };

    // Repository
    let repository = &ev.repository;

    // Find Org ID from Repository.Organization
    let mut oid = find_org_id_or_nil(db, ctx, repository.organization.as_deref());

    // Find Repo ID from Repository (this is a ForkeeOld before 2015).
    let (mut rid, ok) = find_repo_from_name_and_org(db, ctx, &repository.name, oid);
    if !ok {
        rid = repository.id;
    }

    // We defer transaction create until we're inserting data that can be shared between different events
    exec_sql_with_err(
        db,
        ctx,
        &format!(
            "insert into gha_events(\
             id, type, actor_id, repo_id, created_at, \
             dup_actor_login, dup_repo_name, org_id) {}",
            n_values(8)
        ),
        &[
            SqlArg::from(event_id),
            SqlArg::from(&ev.type_),
            SqlArg::Int(aid),
            SqlArg::Int(rid),
            SqlArg::from(ev.created_at),
            SqlArg::Str(maybe_hide(&ev.actor)),
            SqlArg::from(&ev.repository.name),
            SqlArg::from(oid),
        ],
    );

    // Organization
    if let Some(org_login) = &repository.organization {
        if oid.is_none() {
            oid = Some(hash_strings(&[org_login.as_str()]));
        }
        gha_org(
            db,
            ctx,
            Some(&Org {
                id: oid.unwrap_or_default(),
                login: org_login.clone(),
            }),
        );
    }

    // Add Repository
    let repo = Repo {
        id: rid,
        name: repository.name.clone(),
    };
    gha_repo(
        db,
        ctx,
        &repo,
        SqlArg::from(oid),
        SqlArg::from(repository.organization.as_deref()),
    );

    // Pre 2015 Payload
    let Some(pl) = &ev.payload else {
        return 0;
    };

    let mut iid = first_int_or_nil(&[pl.issue, pl.issue_id]);
    let mut cid = comment_id_or_nil(pl.comment.as_ref());
    if cid.is_null() {
        cid = int_or_nil(pl.comment_id);
    }

    exec_sql_with_err(
        db,
        ctx,
        &format!(
            "insert into gha_payloads(\
             event_id, push_id, size, ref, head, befor, action, \
             issue_id, pull_request_id, comment_id, commit, \
             number, forkee_id, release_id, member_id, \
             dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at\
             ) {}",
            n_values(20)
        ),
        &[
            SqlArg::from(event_id),
            SqlArg::Null,
            int_or_nil(pl.size),
            trunc_string_or_nil(pl.ref_.as_deref(), 200),
            string_or_nil(pl.head.as_deref()),
            SqlArg::Null,
            string_or_nil(pl.action.as_deref()),
            iid.clone(),
            pull_request_id_or_nil(pl.pull_request.as_ref()),
            cid,
            string_or_nil(pl.commit.as_deref()),
            int_or_nil(pl.number),
            forkee_id_or_nil(pl.repository.as_ref()),
            release_id_or_nil(pl.release.as_ref()),
            actor_id_or_nil(pl.member.as_ref()),
            SqlArg::Str(maybe_hide(&actor.login)),
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            SqlArg::from(&ev.type_),
            SqlArg::from(ev.created_at),
        ],
    );

    // Start transaction for data possibly shared between events
    let mut tx = fatal_on_err(db.begin());

    // gha_actors
    gha_actor(&mut tx, ctx, &actor, maybe_hide);

    // Payload's Forkee (it uses new structure, so I'm giving it precedence over
    // Event's Forkee (which uses older structure)
    if let Some(prepo) = &pl.repository {
        // Repository is actually a Forkee (non old in this case!)
        // Artificial event is only used to allow duplicating EventOld's data
        // (passed as Event to avoid code duplication)
        let artificial_ev = Event {
            actor: actor.clone(),
            repo: repo.clone(),
            type_: ev.type_.clone(),
            created_at: ev.created_at,
            ..Event::default()
        };
        gha_forkee(&mut tx, ctx, event_id, prepo, &artificial_ev, maybe_hide);
    }

    // Add Forkee in old mode if we didn't added it from payload or if it is a different Forkee
    if pl
        .repository
        .as_ref()
        .is_none_or(|r| r.id != ev.repository.id)
    {
        gha_forkee_old(
            &mut tx,
            ctx,
            event_id,
            &ev.repository,
            &actor,
            &repo,
            ev,
            maybe_hide,
        );
    }

    // SHAs - commits
    if let Some(commits) = &pl.shas {
        for comm in commits {
            let commit = old_sha(comm);
            exec_sql_tx_with_err(
                &mut tx,
                ctx,
                &format!(
                    "insert into gha_commits(\
                     sha, event_id, author_name, message, is_distinct, \
                     dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, origin\
                     ) {}",
                    n_values(12)
                ),
                &[
                    SqlArg::from(commit.sha),
                    SqlArg::from(event_id),
                    SqlArg::Str(maybe_hide(&trunc_to_bytes(commit.name, 160))),
                    SqlArg::Str(trunc_to_bytes(commit.message, 0xffff)),
                    SqlArg::Bool(commit.distinct),
                    SqlArg::Int(actor.id),
                    SqlArg::Str(maybe_hide(&actor.login)),
                    SqlArg::Int(repo.id),
                    SqlArg::from(&repo.name),
                    SqlArg::from(&ev.type_),
                    SqlArg::from(ev.created_at),
                    SqlArg::Int(0),
                ],
            );
            // Commit Roles
            gha_commits_roles(
                &mut tx,
                ctx,
                commit.message,
                commit.sha,
                event_id,
                repo.id,
                &repo.name,
                ev.created_at,
                maybe_hide,
            );
        }
    }

    // Pages
    gha_pages(
        &mut tx,
        ctx,
        pl.pages.as_ref(),
        event_id,
        &actor,
        &repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // Member
    if let Some(member) = &pl.member {
        gha_actor(&mut tx, ctx, member, maybe_hide);
    }

    // Comment
    gha_comment(
        &mut tx,
        ctx,
        pl.comment.as_ref(),
        event_id,
        &actor,
        &repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // Release & assets
    gha_release(
        &mut tx,
        ctx,
        pl.release.as_ref(),
        event_id,
        &actor,
        &repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // Team & Repo connection
    gha_team(
        &mut tx,
        ctx,
        pl.team.as_ref(),
        pl.repository.as_ref(),
        event_id,
        &actor,
        &repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // Pull Request
    let mut forkee_ids_to_skip = vec![ev.repository.id];
    if let Some(prepo) = &pl.repository {
        forkee_ids_to_skip.push(prepo.id);
    }
    gha_pull_request(
        &mut tx,
        ctx,
        pl.pull_request.as_ref(),
        event_id,
        &actor,
        &repo,
        &ev.type_,
        ev.created_at,
        &forkee_ids_to_skip,
        maybe_hide,
    );

    // We need artificial issue
    if let Some(pr) = &pl.pull_request {
        // issue
        iid = SqlArg::Int(-pr.id);
        let is_pr = true;
        let comments = pr.comments.unwrap_or(0);
        let locked = pr.locked.unwrap_or(false);
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &format!(
                "insert into gha_issues(\
                 id, event_id, assignee_id, body, closed_at, comments, created_at, \
                 locked, milestone_id, number, state, title, updated_at, user_id, \
                 dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
                 dup_user_login, is_pull_request) {}",
                n_values(22)
            ),
            &[
                iid.clone(),
                SqlArg::from(event_id),
                actor_id_or_nil(pr.assignee.as_ref()),
                trunc_string_or_nil(pr.body.as_deref(), 0xffff),
                SqlArg::from(pr.closed_at),
                SqlArg::Int(comments),
                SqlArg::from(pr.created_at),
                SqlArg::Bool(locked),
                milestone_id_or_nil(pr.milestone.as_ref()),
                SqlArg::Int(pr.number),
                SqlArg::from(&pr.state),
                SqlArg::Str(clean_utf8(&pr.title)),
                SqlArg::from(pr.updated_at),
                SqlArg::Int(pr.user.id),
                SqlArg::Int(actor.id),
                SqlArg::Str(maybe_hide(&actor.login)),
                SqlArg::Int(repo.id),
                SqlArg::from(&repo.name),
                SqlArg::from(&ev.type_),
                SqlArg::from(ev.created_at),
                SqlArg::Str(maybe_hide(&pr.user.login)),
                SqlArg::Bool(is_pr),
            ],
        );

        for assignee in pr_assignees(pr) {
            // pull_request-assignee connection
            exec_sql_tx_with_err(
                &mut tx,
                ctx,
                &format!(
                    "insert into gha_issues_assignees(issue_id, event_id, assignee_id) {}",
                    n_values(3)
                ),
                &[
                    iid.clone(),
                    SqlArg::from(event_id),
                    SqlArg::Int(assignee.id),
                ],
            );
        }
    }

    // Final commit
    fatal_on_err(tx.commit());
    1
}

/// Go `writeToDB`: write an entire event in the 2015+ format; `1` when
/// written, `0` when it already existed.
pub fn write_to_db(db: &PgConn, ctx: &Ctx, ev: &Event, maybe_hide: MaybeHide<'_>) -> i64 {
    let event_id = ev.id.as_str();
    if event_exists_collision(db, ctx, event_id, &ev.type_, &ev.repo.name, ev.created_at) {
        return 0;
    }

    // gha_events
    exec_sql_with_err(
        db,
        ctx,
        &format!(
            "insert into gha_events(\
             id, type, actor_id, repo_id, created_at, \
             dup_actor_login, dup_repo_name, org_id) {}",
            n_values(8)
        ),
        &[
            SqlArg::from(event_id),
            SqlArg::from(&ev.type_),
            SqlArg::Int(ev.actor.id),
            SqlArg::Int(ev.repo.id),
            SqlArg::from(ev.created_at),
            SqlArg::Str(maybe_hide(&ev.actor.login)),
            SqlArg::from(&ev.repo.name),
            org_id_or_nil(ev.org.as_ref()),
        ],
    );

    // Repository
    let repo = &ev.repo;
    let org = ev.org.as_ref();
    gha_repo(db, ctx, repo, org_id_or_nil(org), org_login_or_nil(org));

    // Organization
    if org.is_some() {
        gha_org(db, ctx, org);
    }

    // gha_payloads
    let pl = &ev.payload;
    exec_sql_with_err(
        db,
        ctx,
        &format!(
            "insert into gha_payloads(\
             event_id, push_id, size, ref, head, befor, action, \
             issue_id, pull_request_id, comment_id, commit, \
             number, forkee_id, release_id, member_id, \
             dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at\
             ) {}",
            n_values(20)
        ),
        &[
            SqlArg::from(event_id),
            int_or_nil(pl.push_id),
            int_or_nil(pl.size),
            trunc_string_or_nil(pl.ref_.as_deref(), 200),
            string_or_nil(pl.head.as_deref()),
            string_or_nil(pl.before.as_deref()),
            string_or_nil(pl.action.as_deref()),
            issue_id_or_nil(pl.issue.as_ref()),
            pull_request_id_or_nil(pl.pull_request.as_ref()),
            comment_id_or_nil(pl.comment.as_ref()),
            SqlArg::Null,
            int_or_nil(pl.number),
            forkee_id_or_nil(pl.forkee.as_ref()),
            release_id_or_nil(pl.release.as_ref()),
            actor_id_or_nil(pl.member.as_ref()),
            SqlArg::Str(maybe_hide(&ev.actor.login)),
            SqlArg::Int(ev.repo.id),
            SqlArg::from(&ev.repo.name),
            SqlArg::from(&ev.type_),
            SqlArg::from(ev.created_at),
        ],
    );

    // Start transaction for data possibly shared between events
    let mut tx = fatal_on_err(db.begin());

    // gha_actors
    gha_actor(&mut tx, ctx, &ev.actor, maybe_hide);

    // gha_commits
    let empty = Vec::new();
    for commit in pl.commits.as_ref().unwrap_or(&empty) {
        let sha = &commit.sha;
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &format!(
                "insert into gha_commits(\
                 sha, event_id, author_name, message, is_distinct, \
                 dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, origin\
                 ) {}",
                n_values(12)
            ),
            &[
                SqlArg::from(sha),
                SqlArg::from(event_id),
                SqlArg::Str(maybe_hide(&trunc_to_bytes(&commit.author.name, 160))),
                SqlArg::Str(trunc_to_bytes(&commit.message, 0xffff)),
                SqlArg::Bool(commit.distinct),
                SqlArg::Int(ev.actor.id),
                SqlArg::Str(maybe_hide(&ev.actor.login)),
                SqlArg::Int(ev.repo.id),
                SqlArg::from(&ev.repo.name),
                SqlArg::from(&ev.type_),
                SqlArg::from(ev.created_at),
                SqlArg::Int(0),
            ],
        );
        // Commit Roles
        gha_commits_roles(
            &mut tx,
            ctx,
            &commit.message,
            sha,
            event_id,
            ev.repo.id,
            &ev.repo.name,
            ev.created_at,
            maybe_hide,
        );
    }

    // Pages
    gha_pages(
        &mut tx,
        ctx,
        pl.pages.as_ref(),
        event_id,
        &ev.actor,
        &ev.repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // Member
    if let Some(member) = &pl.member {
        gha_actor(&mut tx, ctx, member, maybe_hide);
    }

    // Comment
    gha_comment(
        &mut tx,
        ctx,
        pl.comment.as_ref(),
        event_id,
        &ev.actor,
        &ev.repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // gha_issues
    if let Some(issue) = &pl.issue {
        // user, assignee
        gha_actor(&mut tx, ctx, &issue.user, maybe_hide);
        if let Some(assignee) = &issue.assignee {
            gha_actor(&mut tx, ctx, assignee, maybe_hide);
        }

        // issue
        let iid = issue.id;
        let is_pr = issue.pull_request.is_some();
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &format!(
                "insert into gha_issues(\
                 id, event_id, assignee_id, body, closed_at, comments, created_at, \
                 locked, milestone_id, number, state, title, updated_at, user_id, \
                 dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
                 dup_user_login, is_pull_request) {}",
                n_values(22)
            ),
            &[
                SqlArg::Int(iid),
                SqlArg::from(event_id),
                actor_id_or_nil(issue.assignee.as_ref()),
                trunc_string_or_nil(issue.body.as_deref(), 0xffff),
                SqlArg::from(issue.closed_at),
                SqlArg::Int(issue.comments),
                SqlArg::from(issue.created_at),
                SqlArg::Bool(issue.locked),
                milestone_id_or_nil(issue.milestone.as_ref()),
                SqlArg::Int(issue.number),
                SqlArg::from(&issue.state),
                SqlArg::Str(clean_utf8(&issue.title)),
                SqlArg::from(issue.updated_at),
                SqlArg::Int(issue.user.id),
                SqlArg::Int(ev.actor.id),
                SqlArg::Str(maybe_hide(&ev.actor.login)),
                SqlArg::Int(ev.repo.id),
                SqlArg::from(&ev.repo.name),
                SqlArg::from(&ev.type_),
                SqlArg::from(ev.created_at),
                SqlArg::Str(maybe_hide(&issue.user.login)),
                SqlArg::Bool(is_pr),
            ],
        );

        // milestone
        if let Some(milestone) = &issue.milestone {
            gha_milestone(&mut tx, ctx, event_id, milestone, ev, maybe_hide);
        }

        let p_aid = issue.assignee.as_ref().map(|a| a.id);
        for assignee in &issue.assignees {
            let aid = assignee.id;
            if Some(aid) == p_aid {
                continue;
            }

            // assignee
            gha_actor(&mut tx, ctx, assignee, maybe_hide);

            // issue-assignee connection
            exec_sql_tx_with_err(
                &mut tx,
                ctx,
                &format!(
                    "insert into gha_issues_assignees(issue_id, event_id, assignee_id) {}",
                    n_values(3)
                ),
                &[SqlArg::Int(iid), SqlArg::from(event_id), SqlArg::Int(aid)],
            );
        }

        // labels
        for label in &issue.labels {
            let lid = match label.id {
                Some(id) => id,
                None => lookup_label(
                    &mut tx,
                    ctx,
                    &trunc_to_bytes(&label.name, 160),
                    &label.color,
                ),
            };

            // label
            exec_sql_tx_with_err(
                &mut tx,
                ctx,
                &insert_ignore(&format!(
                    "into gha_labels(id, name, color, is_default) {}",
                    n_values(4)
                )),
                &[
                    SqlArg::Int(lid),
                    SqlArg::Str(trunc_to_bytes(&label.name, 160)),
                    SqlArg::from(&label.color),
                    bool_or_nil(label.default),
                ],
            );

            // issue-label connection
            exec_sql_tx_with_err(
                &mut tx,
                ctx,
                &insert_ignore(&format!(
                    "into gha_issues_labels(issue_id, event_id, label_id, \
                     dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
                     dup_issue_number, dup_label_name\
                     ) {}",
                    n_values(11)
                )),
                &[
                    SqlArg::Int(iid),
                    SqlArg::from(event_id),
                    SqlArg::Int(lid),
                    SqlArg::Int(ev.actor.id),
                    SqlArg::Str(maybe_hide(&ev.actor.login)),
                    SqlArg::Int(ev.repo.id),
                    SqlArg::from(&ev.repo.name),
                    SqlArg::from(&ev.type_),
                    SqlArg::from(ev.created_at),
                    SqlArg::Int(issue.number),
                    SqlArg::from(&label.name),
                ],
            );
        }
    }

    // gha_forkees
    if let Some(forkee) = &pl.forkee {
        gha_forkee(&mut tx, ctx, event_id, forkee, ev, maybe_hide);
    }

    // Release & assets
    gha_release(
        &mut tx,
        ctx,
        pl.release.as_ref(),
        event_id,
        &ev.actor,
        &ev.repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // Pull Request
    gha_pull_request(
        &mut tx,
        ctx,
        pl.pull_request.as_ref(),
        event_id,
        &ev.actor,
        &ev.repo,
        &ev.type_,
        ev.created_at,
        &[],
        maybe_hide,
    );

    // Review
    gha_review(
        &mut tx,
        ctx,
        pl.review.as_ref(),
        event_id,
        &ev.actor,
        &ev.repo,
        &ev.type_,
        ev.created_at,
        maybe_hide,
    );

    // Final commit
    fatal_on_err(tx.commit());
    1
}
