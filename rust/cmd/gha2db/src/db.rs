//! Row writers and lookups — port of the `gha*`/`lookup*`/`find*`/
//! `eventExists*` functions of `cmd/gha2db/gha2db.go`.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use devstatscode::gha::{
    actor_id_or_nil, actor_login_or_nil, forkee_id_or_nil, Actor, Branch, Comment, Event, EventOld,
    Forkee, ForkeeOld, GhaTime, Milestone, Org, Page, Release, Repo, Review,
};
use devstatscode::hash::hash_strings;
use devstatscode::pg::api::{
    exec_sql_tx_with_err, exec_sql_with_err, insert_actor_tx, insert_ignore, int_or_nil, n_value,
    n_values, query_sql_tx_with_err, query_sql_with_err, string_or_nil, trunc_string_or_nil,
    trunc_to_bytes,
};
use devstatscode::pg::{PgConn, PgTx, SqlArg};
use devstatscode::time::to_ymdhms_date;
use devstatscode::trailers::{GIT_ALLOWED_TRAILERS, GIT_TRAILER_PATTERN};
use devstatscode::{fatal_on_err, printf, Ctx};

/// Go `func(string) string` hiding function shared between threads.
pub type MaybeHide<'a> = &'a (dyn Fn(&str) -> String + Sync);

/// Go `gEmailName2LoginIDCache`: found `(login, ID)` pairs for
/// `(email, name)` pairs, shared by all threads (`gUseCache` is always on).
type EmailNameCache = HashMap<(String, String), (i64, String)>;
static EMAIL_NAME_2_LOGIN_ID_CACHE: LazyLock<RwLock<EmailNameCache>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Cached `(id, login)` for `(email, name)`.
fn cache_get(email: &str, name: &str) -> Option<(i64, String)> {
    EMAIL_NAME_2_LOGIN_ID_CACHE
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .get(&(email.to_string(), name.to_string()))
        .cloned()
}

/// Number of cached (email, name) → (id, login) pairs (Go
/// `len(gEmailName2LoginIDCache)`).
pub fn cache_len() -> usize {
    EMAIL_NAME_2_LOGIN_ID_CACHE
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .len()
}

fn cache_put(email: &str, name: &str, id: i64, login: &str) {
    EMAIL_NAME_2_LOGIN_ID_CACHE
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            (email.to_string(), name.to_string()),
            (id, login.to_string()),
        );
}

/// Either side of the `lookup*`/`lookup*Tx` pairs: Go has one flavour taking
/// a `*sql.DB` and one taking a `*sql.Tx`; the queries are identical.
pub enum Db<'a, 'b> {
    Con(&'a PgConn),
    Tx(&'a mut PgTx<'b>),
}

impl Db<'_, '_> {
    /// Run a query and collect the rows as vectors of `(i64, String)` pairs
    /// scanned from the requested columns (`n_cols` is 1 or 2).
    fn query_id_login(
        &mut self,
        ctx: &Ctx,
        query: &str,
        args: &[SqlArg],
        n_cols: usize,
    ) -> Vec<(i64, String)> {
        let mut out = Vec::new();
        macro_rules! collect {
            ($rows:expr) => {{
                let mut rows = $rows;
                while rows.next() {
                    let mut id = 0i64;
                    let mut login = String::new();
                    if n_cols == 1 {
                        fatal_on_err(rows.scan(&mut [&mut id]));
                    } else {
                        fatal_on_err(rows.scan(&mut [&mut id, &mut login]));
                    }
                    out.push((id, login));
                }
                fatal_on_err(rows.err());
                fatal_on_err(rows.close());
            }};
        }
        match self {
            Db::Con(con) => collect!(query_sql_with_err(con, ctx, query, args)),
            Db::Tx(tx) => collect!(query_sql_tx_with_err(tx, ctx, query, args)),
        }
        out
    }
}

/// Go `ghaActor`: insert a single GHA actor.
pub fn gha_actor(tx: &mut PgTx<'_>, ctx: &Ctx, actor: &Actor, maybe_hide: MaybeHide<'_>) {
    insert_actor_tx(
        tx,
        ctx,
        SqlArg::Int(actor.id),
        &maybe_hide(&actor.login),
        "",
    );
}

/// Go `ghaRepo`: insert a single GHA repo (autocommit).
pub fn gha_repo(db: &PgConn, ctx: &Ctx, repo: &Repo, org_id: SqlArg, org_login: SqlArg) {
    exec_sql_with_err(
        db,
        ctx,
        &insert_ignore(&format!(
            "into gha_repos(id, name, org_id, org_login) {}",
            n_values(4)
        )),
        &[
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            org_id,
            org_login,
        ],
    );
}

/// Go `ghaOrg`: insert a single GHA org (autocommit).
pub fn gha_org(db: &PgConn, ctx: &Ctx, org: Option<&Org>) {
    if let Some(org) = org {
        exec_sql_with_err(
            db,
            ctx,
            &insert_ignore(&format!("into gha_orgs(id, login) {}", n_values(2))),
            &[SqlArg::Int(org.id), SqlArg::from(&org.login)],
        );
    }
}

/// Go `ghaMilestone`.
pub fn gha_milestone(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    eid: &str,
    milestone: &Milestone,
    ev: &Event,
    maybe_hide: MaybeHide<'_>,
) {
    // creator
    if let Some(creator) = &milestone.creator {
        gha_actor(tx, ctx, creator, maybe_hide);
    }

    // gha_milestones
    exec_sql_tx_with_err(
        tx,
        ctx,
        &format!(
            "insert into gha_milestones(\
             id, event_id, closed_at, closed_issues, created_at, creator_id, \
             description, due_on, number, open_issues, state, title, updated_at, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
             dupn_creator_login) {}",
            n_values(20)
        ),
        &[
            SqlArg::Int(milestone.id),
            SqlArg::from(eid),
            SqlArg::from(milestone.closed_at),
            SqlArg::Int(milestone.closed_issues),
            SqlArg::from(milestone.created_at),
            actor_id_or_nil(milestone.creator.as_ref()),
            trunc_string_or_nil(milestone.description.as_deref(), 0xffff),
            SqlArg::from(milestone.due_on),
            SqlArg::Int(milestone.number),
            SqlArg::Int(milestone.open_issues),
            SqlArg::from(&milestone.state),
            SqlArg::Str(trunc_to_bytes(&milestone.title, 200)),
            SqlArg::from(milestone.updated_at),
            SqlArg::Int(ev.actor.id),
            SqlArg::Str(maybe_hide(&ev.actor.login)),
            SqlArg::Int(ev.repo.id),
            SqlArg::from(&ev.repo.name),
            SqlArg::from(&ev.type_),
            SqlArg::from(ev.created_at),
            actor_login_or_nil(milestone.creator.as_ref(), &maybe_hide),
        ],
    );
}

/// Go `ghaForkeeOld`: insert a single GHA forkee (old format < 2015).
#[allow(clippy::too_many_arguments)]
pub fn gha_forkee_old(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    eid: &str,
    forkee: &ForkeeOld,
    actor: &Actor,
    repo: &Repo,
    ev: &EventOld,
    maybe_hide: MaybeHide<'_>,
) {
    // Lookup author by GitHub login
    let aid = lookup_actor(&mut Db::Tx(tx), ctx, &forkee.owner, maybe_hide);

    // Owner
    let owner = Actor {
        id: aid,
        login: forkee.owner.clone(),
        name: String::new(),
    };
    gha_actor(tx, ctx, &owner, maybe_hide);

    // gha_forkees
    exec_sql_tx_with_err(
        tx,
        ctx,
        &format!(
            "insert into gha_forkees(\
             id, event_id, name, full_name, owner_id, \
             updated_at, \
             stargazers_count, \
             forks, open_issues, watchers, \
             dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at\
             ) {}",
            n_values(14)
        ),
        &[
            SqlArg::Int(forkee.id),
            SqlArg::from(eid),
            SqlArg::Str(trunc_to_bytes(&forkee.name, 80)),
            SqlArg::Str(trunc_to_bytes(&forkee.name, 200)), // ForkeeOld has no FullName
            SqlArg::Int(owner.id),
            SqlArg::from(forkee.created_at), // ForkeeOld has no UpdatedAt
            SqlArg::Int(forkee.stargazers),
            SqlArg::Int(forkee.forks),
            SqlArg::Int(forkee.open_issues),
            SqlArg::Int(forkee.watchers),
            SqlArg::Int(actor.id),
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            SqlArg::from(ev.created_at),
        ],
    );
}

/// Go `ghaForkee`: insert a single GHA forkee.
pub fn gha_forkee(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    eid: &str,
    forkee: &Forkee,
    ev: &Event,
    maybe_hide: MaybeHide<'_>,
) {
    // owner
    gha_actor(tx, ctx, &forkee.owner, maybe_hide);

    // gha_forkees
    exec_sql_tx_with_err(
        tx,
        ctx,
        &format!(
            "insert into gha_forkees(\
             id, event_id, name, full_name, owner_id, \
             updated_at, \
             stargazers_count, \
             forks, open_issues, watchers, \
             dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at\
             ) {}",
            n_values(14)
        ),
        &[
            SqlArg::Int(forkee.id),
            SqlArg::from(eid),
            SqlArg::Str(trunc_to_bytes(&forkee.name, 80)),
            SqlArg::Str(trunc_to_bytes(&forkee.full_name, 200)),
            SqlArg::Int(forkee.owner.id),
            SqlArg::from(forkee.updated_at),
            SqlArg::Int(forkee.stargazers_count),
            SqlArg::Int(forkee.forks),
            SqlArg::Int(forkee.open_issues),
            SqlArg::Int(forkee.watchers),
            SqlArg::Int(ev.actor.id),
            SqlArg::Int(ev.repo.id),
            SqlArg::from(&ev.repo.name),
            SqlArg::from(ev.created_at),
        ],
    );
}

/// Go `ghaBranch`: insert a single GHA branch (and its user/forkee unless
/// the forkee id is in `skip_ids`).
pub fn gha_branch(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    eid: &str,
    branch: &Branch,
    ev: &Event,
    skip_ids: &[i64],
    maybe_hide: MaybeHide<'_>,
) {
    // user
    if let Some(user) = &branch.user {
        gha_actor(tx, ctx, user, maybe_hide);
    }

    // repo
    if let Some(repo) = &branch.repo {
        if !skip_ids.contains(&repo.id) {
            gha_forkee(tx, ctx, eid, repo, ev, maybe_hide);
        }
    }

    // gha_branches
    exec_sql_tx_with_err(
        tx,
        ctx,
        &format!(
            "insert into gha_branches(\
             sha, event_id, user_id, repo_id, \
             dup_created_at\
             ) {}",
            n_values(5)
        ),
        &[
            SqlArg::from(&branch.sha),
            SqlArg::from(eid),
            actor_id_or_nil(branch.user.as_ref()),
            forkee_id_or_nil(branch.repo.as_ref()), // GitHub uses JSON "repo" but it contains Forkee
            SqlArg::from(ev.created_at),
        ],
    );
}

/// Go `lookupLabel`: the label id by name & color, its hash when unknown.
pub fn lookup_label(tx: &mut PgTx<'_>, ctx: &Ctx, name: &str, color: &str) -> i64 {
    let rows = Db::Tx(tx).query_id_login(
        ctx,
        &format!(
            "select id from gha_labels where name={} and color={}",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::from(name), SqlArg::from(color)],
        1,
    );
    let mut lid = 0i64;
    for (id, _) in rows {
        lid = id;
    }
    if lid == 0 {
        lid = hash_strings(&[name, color]);
    }
    lid
}

/// Go `lookupActor`/`lookupActorTx`: the actor id by login, its hash when
/// unknown.
pub fn lookup_actor(db: &mut Db<'_, '_>, ctx: &Ctx, login: &str, maybe_hide: MaybeHide<'_>) -> i64 {
    let hlogin = maybe_hide(login);
    let rows = db.query_id_login(
        ctx,
        &format!(
            "select id from gha_actors where login={} order by id desc limit 1",
            n_value(1)
        ),
        &[SqlArg::Str(hlogin)],
        1,
    );
    let mut aid = 0i64;
    for (id, _) in rows {
        aid = id;
    }
    if aid == 0 {
        aid = hash_strings(&[login]);
    }
    aid
}

/// Go `lookupActorNameEmail`/`lookupActorNameEmailTx`: `(id, login)` of the
/// actor with the given name & email — by email (`gha_actors_emails`), by
/// name (`gha_actors_names`, then `gha_actors.name`), finally by login
/// (`gha_actors.login` = name); `(0, "")` when not found. Results are cached
/// per `(email, name)`.
pub fn lookup_actor_name_email(
    db: &mut Db<'_, '_>,
    ctx: &Ctx,
    name: &str,
    email: &str,
    maybe_hide: MaybeHide<'_>,
) -> (i64, String) {
    if let Some((id, login)) = cache_get(email, name) {
        return (id, login);
    }
    let last = |rows: Vec<(i64, String)>| -> (i64, String) {
        let mut res = (0i64, String::new());
        for r in rows {
            res = r;
        }
        res
    };
    // By email
    let hemail = maybe_hide(email);
    let (eaid, elogin) = last(db.query_id_login(
        ctx,
        &format!(
            "select a.id, a.login from gha_actors a, gha_actors_emails ae where a.id = ae.actor_id and ae.email={} order by a.id desc limit 1",
            n_value(1)
        ),
        &[SqlArg::Str(hemail)],
        2,
    ));
    if eaid != 0 {
        cache_put(email, name, eaid, &elogin);
        return (eaid, elogin);
    }

    // By name from actors names table
    let hname = maybe_hide(name);
    let (naid, nlogin) = last(db.query_id_login(
        ctx,
        &format!(
            "select a.id, a.login from gha_actors a, gha_actors_names an where a.id = an.actor_id and an.name={} order by a.id desc limit 1",
            n_value(1)
        ),
        &[SqlArg::Str(hname.clone())],
        2,
    ));
    if naid != 0 {
        cache_put(email, name, naid, &nlogin);
        return (naid, nlogin);
    }

    // By name from actors table
    let (n2aid, n2login) = last(db.query_id_login(
        ctx,
        &format!(
            "select id, login from gha_actors where name={} order by id desc limit 1",
            n_value(1)
        ),
        &[SqlArg::Str(hname.clone())],
        2,
    ));
    if n2aid != 0 {
        cache_put(email, name, n2aid, &n2login);
        return (n2aid, n2login);
    }

    // By login from actors table
    let (laid, llogin) = last(db.query_id_login(
        ctx,
        &format!(
            "select id, login from gha_actors where login={} order by id desc limit 1",
            n_value(1)
        ),
        &[SqlArg::Str(hname)],
        2,
    ));
    if laid != 0 {
        cache_put(email, name, laid, &llogin);
        return (laid, llogin);
    }
    (0, String::new())
}

/// Go `findRepoFromNameAndOrg`: `(id, exists)` of the repo with the given
/// name and org id (`None` → `org_id is null`).
pub fn find_repo_from_name_and_org(
    db: &PgConn,
    ctx: &Ctx,
    repo_name: &str,
    org_id: Option<i64>,
) -> (i64, bool) {
    let rows = match org_id {
        Some(oid) => Db::Con(db).query_id_login(
            ctx,
            &format!(
                "select id from gha_repos where name={} and org_id={}",
                n_value(1),
                n_value(2)
            ),
            &[SqlArg::from(repo_name), SqlArg::Int(oid)],
            1,
        ),
        None => Db::Con(db).query_id_login(
            ctx,
            &format!(
                "select id from gha_repos where name={} and org_id is null",
                n_value(1)
            ),
            &[SqlArg::from(repo_name)],
            1,
        ),
    };
    let mut exists = false;
    let mut rid = 0i64;
    for (id, _) in rows {
        rid = id;
        exists = true;
    }
    (rid, exists)
}

/// Go `findOrgIDOrNil`: the org id for the login (`None` for `None`/unknown).
pub fn find_org_id_or_nil(db: &PgConn, ctx: &Ctx, org_login: Option<&str>) -> Option<i64> {
    let login = org_login?;
    let rows = Db::Con(db).query_id_login(
        ctx,
        &format!("select id from gha_orgs where login={}", n_value(1)),
        &[SqlArg::from(login)],
        1,
    );
    rows.first().map(|(id, _)| *id)
}

/// Go `eventExistsCollision`: like Go's (unused) `eventExists`, but logs when the
/// existing row is a DIFFERENT event (GitHub reset the event id sequence on
/// 2025-10-08; new real ids can reuse 2016-2020 ids). Times are compared as
/// the wall clock stored in the `timestamp` column.
pub fn event_exists_collision(
    db: &PgConn,
    ctx: &Ctx,
    event_id: &str,
    e_type: &str,
    repo_name: &str,
    created_at: GhaTime,
) -> bool {
    let mut rows = query_sql_with_err(
        db,
        ctx,
        &format!(
            "select type, dup_repo_name, created_at from gha_events where id={}",
            n_value(1)
        ),
        &[SqlArg::from(event_id)],
    );
    let mut exists = false;
    let (mut e_t, mut e_r) = (String::new(), String::new());
    let mut e_d: chrono::DateTime<chrono::Utc> = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut e_t, &mut e_r, &mut e_d]));
        exists = true;
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    if exists
        && (e_t != e_type || e_r != repo_name || to_ymdhms_date(e_d) != to_ymdhms_date(*created_at))
    {
        printf!(
            "event id collision: id {} already exists as ({}, {}, {}), new event ({}, {}, {}) skipped\n",
            event_id,
            e_t,
            e_r,
            devstatscode::pg::value::go_time_string(&e_d.fixed_offset()),
            e_type,
            repo_name,
            created_at
        );
    }
    exists
}

/// Go `ghaCommitsRoles`: process the commit message trailers into
/// `gha_commits_roles`.
#[allow(clippy::too_many_arguments)]
pub fn gha_commits_roles(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    msg: &str,
    sha: &str,
    event_id: &str,
    repo_id: i64,
    repo_name: &str,
    ev_created_at: GhaTime,
    maybe_hide: MaybeHide<'_>,
) {
    let msg = msg.replace('\r', "\n");
    for line in msg.split('\n') {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(m) = GIT_TRAILER_PATTERN.captures(line) else {
            continue;
        };
        let o_trailer = m.name("name").map(|v| v.as_str()).unwrap_or("");
        let l_trailer = o_trailer.to_lowercase();
        let Some(trailers) = GIT_ALLOWED_TRAILERS.get(l_trailer.as_str()) else {
            continue;
        };
        let value = m.name("value").map(|v| v.as_str()).unwrap_or("");
        let mut fields = value.split('<');
        let name = fields.next().unwrap_or("").trim();
        let email = fields
            .next()
            .map(|f| f.split('>').next().unwrap_or("").trim())
            .unwrap_or("");
        if name.is_empty() || email.is_empty() {
            continue;
        }
        let (id, login) = lookup_actor_name_email(&mut Db::Tx(tx), ctx, name, email, maybe_hide);
        for role in trailers.iter() {
            exec_sql_tx_with_err(
                tx,
                ctx,
                &insert_ignore(&format!(
                    "into gha_commits_roles(\
                     sha, event_id, role, actor_id, actor_login, actor_name, actor_email, \
                     dup_repo_id, dup_repo_name, dup_created_at\
                     ) {}",
                    n_values(10)
                )),
                &[
                    SqlArg::from(sha),
                    SqlArg::from(event_id),
                    SqlArg::from(*role),
                    SqlArg::Int(id),
                    SqlArg::Str(maybe_hide(&trunc_to_bytes(&login, 120))),
                    SqlArg::Str(maybe_hide(&trunc_to_bytes(name, 160))),
                    SqlArg::Str(maybe_hide(&trunc_to_bytes(email, 160))),
                    SqlArg::Int(repo_id),
                    SqlArg::from(repo_name),
                    SqlArg::from(ev_created_at),
                ],
            );
        }
    }
}

/// Go `ghaPages`.
#[allow(clippy::too_many_arguments)]
pub fn gha_pages(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    payload_pages: Option<&Vec<Page>>,
    event_id: &str,
    actor: &Actor,
    repo: &Repo,
    e_type: &str,
    e_created_at: GhaTime,
    maybe_hide: MaybeHide<'_>,
) {
    let empty = Vec::new();
    for page in payload_pages.unwrap_or(&empty) {
        exec_sql_tx_with_err(
            tx,
            ctx,
            &insert_ignore(&format!(
                "into gha_pages(sha, event_id, action, title, \
                 dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at\
                 ) {}",
                n_values(10)
            )),
            &[
                SqlArg::from(&page.sha),
                SqlArg::from(event_id),
                SqlArg::from(&page.action),
                SqlArg::Str(trunc_to_bytes(&page.title, 300)),
                SqlArg::Int(actor.id),
                SqlArg::Str(maybe_hide(&actor.login)),
                SqlArg::Int(repo.id),
                SqlArg::from(&repo.name),
                SqlArg::from(e_type),
                SqlArg::from(e_created_at),
            ],
        );
    }
}

/// Go `ghaComment`.
#[allow(clippy::too_many_arguments)]
pub fn gha_comment(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    payload_comment: Option<&Comment>,
    event_id: &str,
    actor: &Actor,
    repo: &Repo,
    e_type: &str,
    e_created_at: GhaTime,
    maybe_hide: MaybeHide<'_>,
) {
    let Some(comment) = payload_comment else {
        return;
    };

    // user
    gha_actor(tx, ctx, &comment.user, maybe_hide);

    // comment
    exec_sql_tx_with_err(
        tx,
        ctx,
        &insert_ignore(&format!(
            "into gha_comments(\
             id, event_id, body, created_at, updated_at, user_id, \
             commit_id, original_commit_id, position, \
             original_position, path, pull_request_review_id, line, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
             dup_user_login) {}",
            n_values(20)
        )),
        &[
            SqlArg::Int(comment.id),
            SqlArg::from(event_id),
            SqlArg::Str(trunc_to_bytes(&comment.body, 0xffff)),
            SqlArg::from(comment.created_at),
            SqlArg::from(comment.updated_at),
            SqlArg::Int(comment.user.id),
            string_or_nil(comment.commit_id.as_deref()),
            string_or_nil(comment.original_commit_id.as_deref()),
            int_or_nil(comment.position),
            int_or_nil(comment.original_position),
            string_or_nil(comment.path.as_deref()),
            int_or_nil(comment.pull_request_review_id),
            int_or_nil(comment.line),
            SqlArg::Int(actor.id),
            SqlArg::Str(maybe_hide(&actor.login)),
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            SqlArg::from(e_type),
            SqlArg::from(e_created_at),
            SqlArg::Str(maybe_hide(&comment.user.login)),
        ],
    );
}

/// Go `ghaReview`.
#[allow(clippy::too_many_arguments)]
pub fn gha_review(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    payload_review: Option<&Review>,
    event_id: &str,
    actor: &Actor,
    repo: &Repo,
    e_type: &str,
    e_created_at: GhaTime,
    maybe_hide: MaybeHide<'_>,
) {
    let Some(review) = payload_review else {
        return;
    };

    // user
    gha_actor(tx, ctx, &review.user, maybe_hide);

    // review
    exec_sql_tx_with_err(
        tx,
        ctx,
        &insert_ignore(&format!(
            "into gha_reviews(\
             id, event_id, state, author_association, submitted_at, user_id, commit_id, body, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
             dup_user_login) {}",
            n_values(15)
        )),
        &[
            SqlArg::Int(review.id),
            SqlArg::from(event_id),
            SqlArg::from(&review.state),
            SqlArg::from(&review.author_association),
            SqlArg::from(review.submitted_at),
            SqlArg::Int(review.user.id),
            SqlArg::from(&review.commit_id),
            trunc_string_or_nil(review.body.as_deref(), 0xffff),
            SqlArg::Int(actor.id),
            SqlArg::Str(maybe_hide(&actor.login)),
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            SqlArg::from(e_type),
            SqlArg::from(e_created_at),
            SqlArg::Str(maybe_hide(&review.user.login)),
        ],
    );
}

/// Go `ghaRelease` (release, its assets and the release-asset connections).
#[allow(clippy::too_many_arguments)]
pub fn gha_release(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    payload_release: Option<&Release>,
    event_id: &str,
    actor: &Actor,
    repo: &Repo,
    e_type: &str,
    e_created_at: GhaTime,
    maybe_hide: MaybeHide<'_>,
) {
    let Some(release) = payload_release else {
        return;
    };

    // author
    gha_actor(tx, ctx, &release.author, maybe_hide);

    // release
    let rid = release.id;
    exec_sql_tx_with_err(
        tx,
        ctx,
        &format!(
            "insert into gha_releases(\
             id, event_id, tag_name, target_commitish, name, draft, \
             author_id, prerelease, created_at, published_at, body, \
             dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
             dup_author_login) {}",
            n_values(18)
        ),
        &[
            SqlArg::Int(rid),
            SqlArg::from(event_id),
            SqlArg::Str(trunc_to_bytes(&release.tag_name, 200)),
            SqlArg::Str(trunc_to_bytes(&release.target_commitish, 200)),
            trunc_string_or_nil(release.name.as_deref(), 200),
            SqlArg::Bool(release.draft),
            SqlArg::Int(release.author.id),
            SqlArg::Bool(release.prerelease),
            SqlArg::from(release.created_at),
            SqlArg::from(release.published_at),
            trunc_string_or_nil(release.body.as_deref(), 0xffff),
            SqlArg::Int(actor.id),
            SqlArg::Str(maybe_hide(&actor.login)),
            SqlArg::Int(repo.id),
            SqlArg::from(&repo.name),
            SqlArg::from(e_type),
            SqlArg::from(e_created_at),
            SqlArg::Str(maybe_hide(&release.author.login)),
        ],
    );

    // Assets
    for asset in &release.assets {
        // uploader
        gha_actor(tx, ctx, &asset.uploader, maybe_hide);

        // asset
        let aid = asset.id;
        exec_sql_tx_with_err(
            tx,
            ctx,
            &format!(
                "insert into gha_assets(\
                 id, event_id, name, label, uploader_id, content_type, \
                 state, size, download_count, created_at, updated_at, \
                 dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
                 dup_uploader_login) {}",
                n_values(18)
            ),
            &[
                SqlArg::Int(aid),
                SqlArg::from(event_id),
                SqlArg::Str(trunc_to_bytes(&asset.name, 200)),
                trunc_string_or_nil(asset.label.as_deref(), 120),
                SqlArg::Int(asset.uploader.id),
                SqlArg::from(&asset.content_type),
                SqlArg::from(&asset.state),
                SqlArg::Int(asset.size),
                SqlArg::Int(asset.download_count),
                SqlArg::from(asset.created_at),
                SqlArg::from(asset.updated_at),
                SqlArg::Int(actor.id),
                SqlArg::Str(maybe_hide(&actor.login)),
                SqlArg::Int(repo.id),
                SqlArg::from(&repo.name),
                SqlArg::from(e_type),
                SqlArg::from(e_created_at),
                SqlArg::Str(maybe_hide(&asset.uploader.login)),
            ],
        );

        // release-asset connection
        exec_sql_tx_with_err(
            tx,
            ctx,
            &format!(
                "insert into gha_releases_assets(release_id, event_id, asset_id) {}",
                n_values(3)
            ),
            &[SqlArg::Int(rid), SqlArg::from(event_id), SqlArg::Int(aid)],
        );
    }
}
