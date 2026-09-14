//! `ghapi2db` — Rust port of `cmd/ghapi2db/ghapi2db.go` (+ `restore.go`).
//!
//! Enriches the GH Archive data with the GitHub API: repository licenses and
//! programming languages, issue/PR events (stored as artificial events by
//! `SyncIssuesState`), commits (author/committer identities), then restores
//! comments, reviews, forks, releases and stars missed by GH Archive.
//! Environment, output and exit codes are those of the Go program.

mod restore;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, Local, Utc};
use devstatscode::consts::{ABUSE, HIDE_CFG_FILE, NOT_FOUND, REPO_NAMES_QUERY};
use devstatscode::ghapi::{
    fmt_slice, get_rate_limits, get_recent_repos, gh_client, handle_possible_error,
    sync_issues_state, GoDuration, IssueConfig, IssuesMap, PrsMap,
};
use devstatscode::github::{
    Client, CommitsListOptions, License, ListOptions, PullRequest, RepositoryCommit,
};
use devstatscode::gofmt;
use devstatscode::hash::hash_strings;
use devstatscode::pg::api::{
    exec_sql_tx_with_err, exec_sql_with_err, insert_actor_tx as lib_insert_actor_tx, insert_ignore,
    n_value, n_values, query_sql_tx_with_err, query_sql_with_err, set_shared_affiliations_db,
    trunc_to_bytes, with_shared_affiliations_db,
};
use devstatscode::pg::value::go_time_string;
use devstatscode::pg::{pg_conn, pg_conn_db, PgConn, PgTx, SqlArg};
use devstatscode::restore::run_event_ids_postprocess;
use devstatscode::string::{get_hidden, maybe_hide_func};
use devstatscode::threads::get_threads_num;
use devstatscode::time::{
    format_go_duration, get_date_ago, hour_start, progress_info, time_parse_any, to_ymdhms_date,
    wall_as_utc,
};
use devstatscode::{fatal_on_err, fatalf, printf, signal, Ctx};

use restore::RestoreStats;

/// Go `func(string) string` hiding function shared between threads.
pub type MaybeHide<'a> = &'a (dyn Fn(&str) -> String + Sync);

/// Go `%v` of a time read from the database (lib/pq's nameless zone).
pub fn db_time(t: DateTime<Utc>) -> String {
    go_time_string(&t.fixed_offset())
}

/// Go `execAffsUpsert`: run the upsert on the shared affiliations database
/// (`GHA2DB_AFFILIATIONS_DB`) when connected, inside `tx` otherwise.
fn exec_affs_upsert(tx: &mut PgTx<'_>, ctx: &Ctx, query: &str, args: &[SqlArg]) {
    let done = with_shared_affiliations_db(|con, actx| {
        exec_sql_with_err(con, actx, query, args);
    });
    if done.is_some() {
        return;
    }
    exec_sql_tx_with_err(tx, ctx, query, args);
}

/// Go `getAPIParams` results: the GitHub clients, the database connection,
/// the recent repositories and the recent date.
pub struct ApiParams {
    pub repos: Vec<String>,
    pub is_single_repo: bool,
    pub single_repo: String,
    pub gcs: Vec<Client>,
    pub c: PgConn,
    pub recent_dt: DateTime<Utc>,
}

/// Go `getAPIParams`: connects to GitHub and Postgres, returns the list of
/// recent repositories (unique by id, then by name) and the recent date.
pub fn get_api_params(ctx: &Ctx) -> ApiParams {
    // Connect to GitHub API
    let gcs = gh_client(ctx);

    // Connect to Postgres DB
    let c = pg_conn(ctx);

    // Get list of repositories to process
    let now_hour = hour_start(wall_as_utc(&Local::now()));
    let recent_repos_dt = get_date_ago(&c, ctx, now_hour, &ctx.recent_repos_range);
    let (repos_a, rids) = get_recent_repos(&c, ctx, recent_repos_dt);
    if ctx.debug > 0 {
        printf!(
            "Repos to process from {}: {}\n",
            db_time(recent_repos_dt),
            fmt_slice(&repos_a)
        );
    }
    // Repos can have the same ID with different names
    // But they also have the same name with different IDs
    // We first need to put all repo names with unique IDs
    // And then make this names list unique as well
    let mut rids_m: BTreeSet<i64> = BTreeSet::new();
    let mut repos_m: BTreeSet<String> = BTreeSet::new();
    for (i, rid) in rids.iter().enumerate() {
        if rids_m.insert(*rid) {
            repos_m.insert(repos_a[i].clone());
        }
    }
    // Go iterates a map here (random order) — sorted in the port.
    let repos: Vec<String> = repos_m.into_iter().collect();
    if ctx.debug > 0 {
        printf!("Unique repos: {}\n", fmt_slice(&repos));
    }
    let recent_dt = get_date_ago(&c, ctx, now_hour, &ctx.recent_range);

    // Single repo mode
    let single_repo = std::env::var("REPO").unwrap_or_default();
    let is_single_repo = !single_repo.is_empty();

    ApiParams {
        repos,
        is_single_repo,
        single_repo,
        gcs,
        c,
        recent_dt,
    }
}

/// Go `getEnrichCommitsDateRange`: the last enriched commits date range
/// (`[max enriched - 2min, max commit + 2min]`) of a repository.
fn get_enrich_commits_date_range(
    c: &PgConn,
    ctx: &Ctx,
    repo: &str,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!(
            "select coalesce(max(dup_created_at), \
             (select min(dup_created_at) from gha_commits where dup_repo_name = {})) \
             from gha_commits where author_email != '' and dup_repo_name = {}",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::from(repo), SqlArg::from(repo)],
    );
    let mut dtf: Option<DateTime<Utc>> = None;
    while rows.next() {
        let mut pdt: Option<DateTime<Utc>> = None;
        fatal_on_err(rows.scan(&mut [&mut pdt]));
        match pdt {
            None => {
                if ctx.debug > 0 {
                    printf!("{}: no date from\n", repo);
                }
                fatal_on_err(rows.close());
                return None;
            }
            Some(t) => dtf = Some(t - chrono::Duration::minutes(2)),
        }
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    let mut rows = query_sql_with_err(
        c,
        ctx,
        &format!(
            "select max(dup_created_at) from gha_commits where dup_repo_name = {}",
            n_value(1)
        ),
        &[SqlArg::from(repo)],
    );
    let mut dtt: Option<DateTime<Utc>> = None;
    while rows.next() {
        let mut pdt: Option<DateTime<Utc>> = None;
        fatal_on_err(rows.scan(&mut [&mut pdt]));
        match pdt {
            None => {
                if ctx.debug > 0 {
                    printf!("{}: no date to\n", repo);
                }
                fatal_on_err(rows.close());
                return None;
            }
            Some(t) => dtt = Some(t + chrono::Duration::minutes(2)),
        }
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    // Go's zero time when a query returned no row (cannot happen for aggregates).
    let dtf = dtf.unwrap_or_default();
    let dtt = dtt.unwrap_or_default();
    if ctx.debug > 0 {
        printf!(
            "{}: {} - {}\n",
            repo,
            to_ymdhms_date(dtf),
            to_ymdhms_date(dtt)
        );
    }
    Some((dtf, dtt))
}

/// Go `lookupActorTx`: search for the actor by login (exact, then
/// case-insensitive); the login's hash when not found.
fn lookup_actor_tx(con: &mut PgTx<'_>, ctx: &Ctx, login: &str, maybe_hide: MaybeHide) -> i64 {
    let hlogin = maybe_hide(login);
    let mut rows = query_sql_tx_with_err(
        con,
        ctx,
        &format!(
            "select id from gha_actors where login={} union select id from \
             gha_actors where lower(login)={} order by id desc limit 1",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::from(&hlogin), SqlArg::from(hlogin.to_lowercase())],
    );
    let mut aid: i64 = 0;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut aid]));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    if aid == 0 {
        aid = hash_strings(&[login]);
    }
    aid
}

/// Go `insertActorTx`: insert a single GHA actor (hidden login/name).
fn insert_actor_tx(
    con: &mut PgTx<'_>,
    ctx: &Ctx,
    aid: i64,
    login: &str,
    name: &str,
    maybe_hide: MaybeHide,
) {
    lib_insert_actor_tx(
        con,
        ctx,
        SqlArg::Int(aid),
        &maybe_hide(login),
        &maybe_hide(&trunc_to_bytes(name, 120)),
    );
}

/// Go `processCommit`: enrich a `gha_commits` row with the API commit's
/// author/committer identities and record their emails/names.
fn process_commit(c: &PgConn, ctx: &Ctx, commit: &RepositoryCommit, maybe_hide: MaybeHide) {
    // Check required fields
    let cmt = match commit.commit.as_ref() {
        Some(cmt) => cmt,
        None => {
            fatalf!("Nil Commit: {:?}\n", commit);
        }
    };

    // Start transaction for data possibly shared between events
    let mut tx = fatal_on_err(c.begin());

    // Shortcuts
    // SHA
    let c_sha = commit.sha.clone().expect("commit without sha");

    // Committer
    let committer_id = commit.committer.as_ref().and_then(|u| u.id).unwrap_or(0);
    let committer_login = commit
        .committer
        .as_ref()
        .and_then(|u| u.login.clone())
        .unwrap_or_default();
    let git_committer = cmt.committer.as_ref().expect("commit without committer");
    let committer_name = git_committer.name.clone().expect("committer without name");
    let committer_email = git_committer
        .email
        .clone()
        .expect("committer without email");

    // Author
    let author_id = commit.author.as_ref().and_then(|u| u.id).unwrap_or(0);
    let author_login = commit
        .author
        .as_ref()
        .and_then(|u| u.login.clone())
        .unwrap_or_default();
    let git_author = cmt.author.as_ref().expect("commit without author");
    let author_name = git_author.name.clone().expect("author without name");
    let author_email = git_author.email.clone().expect("author without email");
    let author_date = git_author.date.expect("author without date").0;

    // Check if we already have this commit
    let str_author_date = to_ymdhms_date(author_date);
    let mut rows = query_sql_tx_with_err(
        &mut tx,
        ctx,
        &format!(
            "select sha, author_name, dup_created_at \
             from gha_commits where sha = {} \
             order by abs(extract(epoch from {} - dup_created_at)) \
             limit 1",
            n_value(1),
            n_value(2)
        ),
        &[SqlArg::from(&c_sha), SqlArg::from(&str_author_date)],
    );
    let mut sha = String::new();
    let mut current_author_name = String::new();
    let mut created_at: DateTime<FixedOffset> = DateTime::<Utc>::default().fixed_offset();
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut sha, &mut current_author_name, &mut created_at]));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    if !sha.is_empty() && ctx.debug > 1 {
        let diff = (created_at - author_date)
            .num_nanoseconds()
            .unwrap_or(i64::MAX);
        printf!(
            "GHA GHAPI time difference for sha {}: {}\n",
            c_sha,
            GoDuration(diff)
        );
    }

    // Get existing committer & author, it is possible that we don't have them yet
    let mut new_committer_id = 0i64;
    if !committer_login.is_empty() {
        new_committer_id = lookup_actor_tx(&mut tx, ctx, &committer_login, maybe_hide);
    }
    let mut new_author_id = 0i64;
    if !author_login.is_empty() {
        new_author_id = committer_id;
        if author_login != committer_login {
            new_author_id = lookup_actor_tx(&mut tx, ctx, &author_login, maybe_hide);
        }
    }

    // Compare to what we currently have, eventually warn and insert new
    // The actor rows are ensured for every commit whose author/committer emails and names are
    // recorded below (also for commits not present in gha_commits), otherwise those identity rows
    // reference a missing actor and are unusable (gha_actors_emails/names → gha_actors)
    if !committer_login.is_empty() && new_committer_id != committer_id {
        if ctx.debug > 0 {
            printf!(
                "DB Committer ID: {} != API Committer ID: {}, sha: {}, login: {}\n",
                new_committer_id,
                committer_id,
                c_sha,
                committer_login
            );
        }
        insert_actor_tx(
            &mut tx,
            ctx,
            committer_id,
            &committer_login,
            &committer_name,
            maybe_hide,
        );
    }
    if !author_login.is_empty() && author_login != committer_login && new_author_id != author_id {
        if ctx.debug > 0 {
            printf!(
                "DB Author ID: {} != API Author ID: {}, SHA: {}, login: {}\n",
                new_author_id,
                author_id,
                c_sha,
                author_login
            );
        }
        insert_actor_tx(
            &mut tx,
            ctx,
            author_id,
            &author_login,
            &author_name,
            maybe_hide,
        );
    }

    // Same author?
    if !sha.is_empty() && current_author_name != author_name {
        printf!(
            "Author name mismatch API: {}, DB: {}, SHA: {}\n",
            author_name,
            current_author_name,
            c_sha
        );
    }

    // If we have that commit, update (enrich) it.
    if sha.is_empty() {
        sha = c_sha.clone();
        if ctx.debug > 1 {
            printf!("SHA {} not found\n", sha);
        }
    } else {
        let mut cols = vec![
            format!("author_name={}", n_value(1)),
            format!("author_email={}", n_value(2)),
            format!("committer_name={}", n_value(3)),
            format!("committer_email={}", n_value(4)),
        ];
        let mut vals = vec![
            SqlArg::Str(maybe_hide(&trunc_to_bytes(&author_name, 160))),
            SqlArg::Str(maybe_hide(&trunc_to_bytes(&author_email, 160))),
            SqlArg::Str(maybe_hide(&trunc_to_bytes(&committer_name, 160))),
            SqlArg::Str(maybe_hide(&trunc_to_bytes(&committer_email, 160))),
        ];
        let mut n_val = 5;
        if !committer_login.is_empty() {
            cols.push(format!("committer_id={}", n_value(n_val)));
            vals.push(SqlArg::Int(committer_id));
            n_val += 1;
            cols.push(format!("dup_committer_login={}", n_value(n_val)));
            vals.push(SqlArg::Str(maybe_hide(&trunc_to_bytes(
                &committer_login,
                160,
            ))));
            n_val += 1;
        }
        if !author_login.is_empty() {
            cols.push(format!("author_id={}", n_value(n_val)));
            vals.push(SqlArg::Int(author_id));
            n_val += 1;
            cols.push(format!("dup_author_login={}", n_value(n_val)));
            vals.push(SqlArg::Str(maybe_hide(&trunc_to_bytes(&author_login, 160))));
            n_val += 1;
        }
        vals.push(SqlArg::from(&sha));
        vals.push(SqlArg::DbTime(created_at));
        let mut query = format!("update gha_commits set {}", cols.join(", "));
        query.push_str(&format!(
            " where sha={} and dup_created_at={}",
            n_value(n_val),
            n_value(n_val + 1)
        ));
        exec_sql_tx_with_err(&mut tx, ctx, &query, &vals);
    }

    // Author email
    let emails_query = format!(
        "insert into gha_actors_emails(actor_id, email, origin) {} on conflict(actor_id, email) \
         do update set origin = 1 where gha_actors_emails.actor_id = {} \
         and gha_actors_emails.email = {}",
        n_values(3),
        n_value(4),
        n_value(5)
    );
    let m_email = maybe_hide(&trunc_to_bytes(&author_email, 120));
    exec_affs_upsert(
        &mut tx,
        ctx,
        &emails_query,
        &[
            SqlArg::Int(author_id),
            SqlArg::from(&m_email),
            SqlArg::Int(1),
            SqlArg::Int(author_id),
            SqlArg::from(&m_email),
        ],
    );
    // Committer email
    if committer_email != author_email {
        let m_email = maybe_hide(&trunc_to_bytes(&committer_email, 120));
        exec_affs_upsert(
            &mut tx,
            ctx,
            &emails_query,
            &[
                SqlArg::Int(committer_id),
                SqlArg::from(&m_email),
                SqlArg::Int(1),
                SqlArg::Int(committer_id),
                SqlArg::from(&m_email),
            ],
        );
    }
    // Author name
    let names_query = format!(
        "insert into gha_actors_names(actor_id, name, origin) {} on conflict(actor_id, name) \
         do update set origin = 1 where gha_actors_names.actor_id = {} \
         and gha_actors_names.name = {}",
        n_values(3),
        n_value(4),
        n_value(5)
    );
    let m_name = maybe_hide(&trunc_to_bytes(&author_name, 120));
    exec_affs_upsert(
        &mut tx,
        ctx,
        &names_query,
        &[
            SqlArg::Int(author_id),
            SqlArg::from(&m_name),
            SqlArg::Int(1),
            SqlArg::Int(author_id),
            SqlArg::from(&m_name),
        ],
    );
    // Committer name
    if committer_name != author_name {
        let m_name = maybe_hide(&trunc_to_bytes(&committer_name, 120));
        exec_affs_upsert(
            &mut tx,
            ctx,
            &names_query,
            &[
                SqlArg::Int(committer_id),
                SqlArg::from(&m_name),
                SqlArg::Int(1),
                SqlArg::Int(committer_id),
                SqlArg::from(&m_name),
            ],
        );
    }

    // Final commit
    fatal_on_err(tx.commit());
}

/// The `DTFROM`/`DTTO` date range mode of the events and commits passes.
struct DateRange {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    is_range: bool,
}

fn date_range_from_env() -> DateRange {
    let from_s = std::env::var("DTFROM").unwrap_or_default();
    let to_s = std::env::var("DTTO").unwrap_or_default();
    let mut dr = DateRange {
        from: None,
        to: None,
        is_range: false,
    };
    if !from_s.is_empty() {
        dr.from = Some(time_parse_any(&from_s));
        dr.is_range = true;
    }
    if !to_s.is_empty() {
        dr.to = Some(time_parse_any(&to_s));
        dr.is_range = true;
    }
    dr
}

/// The shared state of the fetching goroutines (events and commits passes).
struct Shared<'a> {
    ctx: &'a Ctx,
    gcs: &'a [Client],
    c: &'a PgConn,
    recent_dt: DateTime<Utc>,
    is_single_repo: bool,
    single_repo: &'a str,
    date_range: &'a DateRange,
    max_threads: usize,
    allowed_thr_n: AtomicUsize,
    /// The main loop's `nThreads` (only printed by the debug lines).
    n_threads: AtomicUsize,
    api_calls: AtomicUsize,
}

/// Go `time.Duration(int(math.Pow(2.0, float64(tr+3)))) * time.Second`.
fn abuse_wait(tr: i64) -> Duration {
    Duration::from_secs(1u64 << (tr + 3).clamp(0, 62))
}

/// Lower the thread limit after an abuse detection (Go's `thrMutex` block).
fn abuse_backoff(sh: &Shared<'_>, tr: i64, what: &str) {
    let wait = abuse_wait(tr);
    if sh.ctx.github_debug > 0 {
        printf!(
            "GitHub API abuse detected ({}), wait {}\n",
            what,
            format_go_duration(wait)
        );
    }
    let allowed = sh.allowed_thr_n.load(Ordering::SeqCst);
    if allowed > 1 {
        sh.allowed_thr_n.store(allowed - 1, Ordering::SeqCst);
        if sh.ctx.github_debug > 0 {
            printf!(
                "Lower threads limit ({}): {}/{}\n",
                what,
                sh.n_threads.load(Ordering::SeqCst),
                allowed - 1
            );
        }
    }
    std::thread::sleep(wait);
}

/// Raise the thread limit after a successful call.
fn success_raise(sh: &Shared<'_>, what: &str) {
    let allowed = sh.allowed_thr_n.load(Ordering::SeqCst);
    if allowed < sh.max_threads {
        sh.allowed_thr_n.store(allowed + 1, Ordering::SeqCst);
        if sh.ctx.github_debug > 0 {
            printf!(
                "Rise threads limit ({}): {}/{}\n",
                what,
                sh.n_threads.load(Ordering::SeqCst),
                allowed + 1
            );
        }
    }
}

/// The outcome of the rate-limit gate before an API call.
enum Gate {
    /// Proceed with the client `hint` (and the remaining points to print).
    Proceed(usize, Vec<i64>),
    /// Waited for the reset, retry.
    Retry,
    /// Not worth waiting: the goroutine gives up (`ch <- false`).
    Abort,
}

/// The rate-limit gate of the events/commits/PR fetches: `try_label` is the
/// debug line prefix (`Repo commits`, `Issues Repo Events`, `Get PR`),
/// `what` the data name of the limit messages (`commits`, `events`, `PR`,
/// `issues events`).
fn rate_gate(sh: &Shared<'_>, tr: i64, try_label: &str, what: &str, what_abort: &str) -> Gate {
    let ctx = sh.ctx;
    let (hint, _, rem, wait_period) = get_rate_limits(ctx, sh.gcs, true);
    if ctx.github_debug > 0 {
        printf!(
            "{} Try: {}, rem: {}, waitPeriod: {}, hint: {}\n",
            try_label,
            tr,
            fmt_slice(&rem),
            fmt_slice(&wait_period),
            hint
        );
    }
    if rem[hint] <= ctx.min_ghapi_points {
        if wait_period[hint].seconds() <= ctx.max_ghapi_wait_seconds as f64 {
            if ctx.github_debug > 0 {
                printf!(
                    "API limit reached while getting {} data, waiting {} ({})\n",
                    what,
                    wait_period[hint],
                    tr
                );
            }
            std::thread::sleep(Duration::from_secs(1));
            wait_period[hint].sleep();
            return Gate::Retry;
        }
        if ctx.ghapi_error_is_fatal {
            fatalf!(
                "API limit reached while getting {} data, aborting, don't want to wait {}",
                what_abort,
                wait_period[hint]
            );
        }
        printf!(
            "Error: API limit reached while getting {} data, aborting, don't want to wait {}\n",
            what_abort,
            wait_period[hint]
        );
        return Gate::Abort;
    }
    Gate::Proceed(hint, rem)
}

/// Go's `if !got { … }` after the retries: fatal or give up.
fn retries_exhausted(ctx: &Ctx, what: &str) -> bool {
    if ctx.ghapi_error_is_fatal {
        fatalf!(
            "GetRateLimit call failed {} times while getting {}, aborting",
            ctx.max_ghapi_retry,
            what
        );
    }
    printf!(
        "Error: GetRateLimit call failed {} times while getting {}, aborting\n",
        ctx.max_ghapi_retry,
        what
    );
    false
}

/// Split `org/repo`; `None` when malformed (the goroutine returns `false`).
fn split_org_repo(org_repo: &str) -> Option<(&str, &str)> {
    let ary: Vec<&str> = org_repo.split('/').collect();
    if ary.len() < 2 {
        return None;
    }
    let (org, repo) = (ary[0], ary[1]);
    if org.is_empty() || repo.is_empty() {
        return None;
    }
    Some((org, repo))
}

/// The per-repository commits goroutine of `syncCommits`.
fn fetch_commits(
    sh: &Shared<'_>,
    org_repo: &str,
    opt: &CommitsListOptions,
    dt_start: DateTime<Utc>,
) -> bool {
    let ctx = sh.ctx;
    if sh.is_single_repo && org_repo != sh.single_repo {
        return false;
    }
    let (org, repo) = match split_org_repo(org_repo) {
        Some(v) => v,
        None => return false,
    };
    let th_dt_start = Utc::now();
    let mut th_last_time = dt_start;
    // To handle GDPR
    let maybe_hide = maybe_hide_func(get_hidden(ctx, HIDE_CFG_FILE));
    // Need deep copy - threads
    let mut copt = opt.clone();
    // No DTFROM/DTTO set and no GHA2DB_NO_AUTOFETCHCOMMITS
    if !sh.date_range.is_range && ctx.auto_fetch_commits {
        let (dtf, dtt) = match get_enrich_commits_date_range(sh.c, ctx, org_repo) {
            Some(v) => v,
            None => return false,
        };
        copt = CommitsListOptions {
            since: Some(dtf),
            until: Some(dtt),
            sha: opt.sha.clone(),
            path: opt.path.clone(),
            author: opt.author.clone(),
            list: ListOptions {
                per_page: opt.list.per_page,
                page: 0,
            },
        };
    }
    let mut n_pages = 0i64;
    // start infinite for (paging)
    loop {
        let mut got = false;
        let mut commits: Vec<RepositoryCommit> = Vec::new();
        let mut next_page = 0i64;
        // start trials
        for tr in 0..ctx.max_ghapi_retry {
            let (hint, rem) = match rate_gate(sh, tr, "Repo commits", "commits", "commits") {
                Gate::Proceed(h, r) => (h, r),
                Gate::Retry => continue,
                Gate::Abort => return false,
            };
            n_pages += 1;
            if ctx.github_debug > 0 {
                printf!(
                    "API call for commits {} ({}), remaining GHAPI points {}, hint: {}\n",
                    org_repo,
                    n_pages,
                    fmt_slice(&rem),
                    hint
                );
            }
            sh.api_calls.fetch_add(1, Ordering::SeqCst);
            let r = sh.gcs[hint].repositories_list_commits(org, repo, &copt);
            let res = handle_possible_error(r.error.as_ref(), org_repo, "Repositories.ListCommits");
            if !res.is_empty() {
                if res == ABUSE {
                    abuse_backoff(sh, tr, "issues events");
                }
                if res == NOT_FOUND {
                    printf!("Warning: not found: {}/{}\n", org, repo);
                    return false;
                }
                continue;
            }
            success_raise(sh, "issues events");
            commits = r.value.unwrap_or_default();
            next_page = r.response.map(|resp| resp.next_page).unwrap_or(0);
            got = true;
            break;
        }
        // end trials
        if !got {
            return retries_exhausted(ctx, "events");
        }
        // Process commits
        if ctx.debug > 0 {
            printf!(
                "{}: processing {} commits, page {}\n",
                org_repo,
                commits.len(),
                n_pages
            );
        }
        for commit in &commits {
            process_commit(sh.c, ctx, commit, &maybe_hide);
        }
        let (hint, _, th_rem, th_wait) = get_rate_limits(ctx, sh.gcs, true);
        progress_info(
            0,
            0,
            th_dt_start,
            &mut th_last_time,
            Duration::from_secs(10),
            &format!(
                "{} page {}, API points: {}, resets in: {}, hint: {}",
                org_repo,
                n_pages,
                fmt_slice(&th_rem),
                fmt_slice(&th_wait),
                hint
            ),
        );
        // Handle paging
        if next_page == 0 {
            break;
        }
        copt.list.page = next_page;
    }
    // end infinite for (paging)
    true
}

fn progress_line(ctx: &Ctx, gcs: &[Client]) -> String {
    let (hint, _, rem, wait) = get_rate_limits(ctx, gcs, true);
    format!(
        "API points: {}, resets in: {}, hint: {}",
        fmt_slice(&rem),
        fmt_slice(&wait),
        hint
    )
}

/// Runs `worker` for every repository as the Go goroutine pool with the
/// adaptive `allowedThrN` limit; returns the number of finished workers.
fn run_pool<'a>(
    sh: &Shared<'a>,
    repos: &[String],
    worker: impl Fn(&Shared<'a>, &str) -> bool + Sync,
) {
    let n_repos = repos.len();
    let dt_start = Utc::now();
    let mut last_time = dt_start;
    let mut checked = 0usize;
    let period = Duration::from_secs(10);
    let worker = &worker;
    let (tx, rx) = mpsc::channel::<bool>();
    std::thread::scope(|scope| {
        let mut n_threads = 0usize;
        for org_repo in repos {
            let tx = tx.clone();
            scope.spawn(move || {
                let res = worker(sh, org_repo);
                let _ = tx.send(res);
            });
            n_threads += 1;
            sh.n_threads.store(n_threads, Ordering::SeqCst);
            while n_threads >= sh.allowed_thr_n.load(Ordering::SeqCst) {
                let _ = rx.recv();
                n_threads -= 1;
                sh.n_threads.store(n_threads, Ordering::SeqCst);
                checked += 1;
                // Get RateLimits info
                let msg = progress_line(sh.ctx, sh.gcs);
                progress_info(checked, n_repos, dt_start, &mut last_time, period, &msg);
            }
        }
        // Usually all work happens on '<-ch'
        if sh.ctx.debug > 0 {
            printf!("Final GHAPI threads join\n");
        }
        while n_threads > 0 {
            let _ = rx.recv();
            n_threads -= 1;
            sh.n_threads.store(n_threads, Ordering::SeqCst);
            checked += 1;
            // Get RateLimits info
            let msg = progress_line(sh.ctx, sh.gcs);
            progress_info(checked, n_repos, dt_start, &mut last_time, period, &msg);
        }
    });
}

/// Go `syncCommits`: enrich the recent commits of the recent repositories.
/// Debugging options (environment variables): `REPO=full_repo_name`,
/// `DTFROM`/`DTTO` datetimes (set `GHA2DB_RECENT_RANGE` to cover them).
fn sync_commits(ctx: &mut Ctx) {
    // Get common params
    let params = get_api_params(ctx);

    // Date range mode
    let date_range = date_range_from_env();

    // Process commits in parallel
    let thr_n = get_threads_num(ctx);
    let max_threads = 16.min(thr_n).max(1);
    let dt_start = Utc::now();
    let n_repos = params.repos.len();
    printf!(
        "ghapi2db.go: Processing {} repos - GHAPI commits part\n",
        n_repos
    );

    let mut opt = CommitsListOptions {
        since: Some(params.recent_dt),
        list: ListOptions {
            per_page: 100,
            page: 0,
        },
        ..Default::default()
    };
    if date_range.is_range {
        if let Some(from) = date_range.from {
            opt.since = Some(from);
        }
        if let Some(to) = date_range.to {
            opt.until = Some(to);
        }
    }
    let sh = Shared {
        ctx,
        gcs: &params.gcs,
        c: &params.c,
        recent_dt: params.recent_dt,
        is_single_repo: params.is_single_repo,
        single_repo: &params.single_repo,
        date_range: &date_range,
        max_threads,
        allowed_thr_n: AtomicUsize::new(max_threads),
        n_threads: AtomicUsize::new(0),
        api_calls: AtomicUsize::new(0),
    };
    let opt = &opt;
    run_pool(&sh, &params.repos, |sh, org_repo| {
        fetch_commits(sh, org_repo, opt, dt_start)
    });
    printf!(
        "GH Commits API calls: {}\n",
        sh.api_calls.load(Ordering::SeqCst)
    );
    params.c.close();
}

/// The event types processed by `syncEvents` (the rest is skipped with a warning).
const EVENT_TYPES: &[&str] = &[
    "closed",
    "merged",
    "referenced",
    "reopened",
    "locked",
    "unlocked",
    "renamed",
    "mentioned",
    "assigned",
    "unassigned",
    "labeled",
    "unlabeled",
    "milestoned",
    "demilestoned",
    "subscribed",
    "unsubscribed",
    "head_ref_deleted",
    "head_ref_restored",
    "review_requested",
    "review_dismissed",
    "review_request_removed",
    "added_to_project",
    "removed_from_project",
    "moved_columns_in_project",
    "marked_as_duplicate",
    "unmarked_as_duplicate",
    "converted_note_to_issue",
    // Non specified in GH API but happening
    "base_ref_changed",
    "comment_deleted",
    "deployed",
    "transferred",
    "head_ref_force_pushed",
    "pinned",
    "unpinned",
    "ready_for_review",
    "base_ref_force_pushed",
    "connected",
    "disconnected",
    "convert_to_draft",
    "base_ref_deleted",
    "automatic_base_change_succeeded",
    "automatic_base_change_failed",
    "auto_merge_enabled",
    "auto_merge_disabled",
    "auto_squash_enabled",
    "auto_squash_disabled",
    "auto_rebase_enabled",
    "auto_rebase_disabled",
    "user_blocked",
    "sync",
    "converted_to_discussion",
    "added_to_merge_queue",
    "added_to_project_v2",
    "converted_from_draft",
    "copilot_work_finished_failure",
    "copilot_work_finished",
    "copilot_work_started",
    "issue_type_added",
    "issue_type_changed",
    "parent_issue_added",
    "parent_issue_removed",
    "project_v2_item_status_changed",
    "removed_from_merge_queue",
    "removed_from_project_v2",
    "sub_issue_added",
    "sub_issue_removed",
];

/// The single-milestone / single-issue debugging modes of `syncEvents`.
struct EventsFilter {
    single_milestone: Option<String>,
    single_issue: Option<i64>,
}

/// Go `eids` (issue id, count) and `eidRepos` keyed by event id.
type EidsMap = std::collections::BTreeMap<i64, ([i64; 2], Vec<String>)>;

/// The state collected by the events goroutines.
struct EventsShared {
    issues: Mutex<IssuesMap>,
    prs: Mutex<PrsMap>,
    eids: Mutex<EidsMap>,
}

/// One of the `%v` renderings of the `[min - max] < recent` debug line.
enum GoTimeValue {
    /// `time.Now()` (with its monotonic reading).
    Now(String),
    /// A time decoded from the API JSON.
    Json(DateTime<FixedOffset>),
    /// A time read from the database.
    Db(DateTime<Utc>),
}

impl GoTimeValue {
    fn instant(&self) -> DateTime<Utc> {
        match self {
            GoTimeValue::Now(_) => Utc::now(),
            GoTimeValue::Json(t) => t.with_timezone(&Utc),
            GoTimeValue::Db(t) => *t,
        }
    }
    fn render(&self) -> String {
        match self {
            GoTimeValue::Now(s) => s.clone(),
            GoTimeValue::Json(t) => gofmt::time(*t),
            GoTimeValue::Db(t) => db_time(*t),
        }
    }
}

/// The per-repository events goroutine of `syncEvents`.
fn fetch_events(sh: &Shared<'_>, es: &EventsShared, filter: &EventsFilter, org_repo: &str) -> bool {
    let ctx = sh.ctx;
    if sh.is_single_repo && org_repo != sh.single_repo {
        return false;
    }
    let (org, repo) = match split_org_repo(org_repo) {
        Some(v) => v,
        None => return false,
    };
    let gcfg = IssueConfig {
        repo: org_repo.to_string(),
        ..Default::default()
    };
    let gcfg_str = gcfg.to_string();
    // Go shares one `opt` between all goroutines (see bug 39) — one per goroutine here.
    let mut opt = ListOptions {
        per_page: 100,
        page: 0,
    };
    let mut n_pages = 0i64;
    loop {
        let mut got = false;
        let mut events = Vec::new();
        let mut next_page = 0i64;
        for tr in 0..ctx.max_ghapi_retry {
            let (hint, rem) =
                match rate_gate(sh, tr, "Issues Repo Events", "events", "issues events") {
                    Gate::Proceed(h, r) => (h, r),
                    Gate::Retry => continue,
                    Gate::Abort => return false,
                };
            n_pages += 1;
            if ctx.github_debug > 0 {
                printf!(
                    "API call for issues events {} ({}), remaining GHAPI points {}, hint: {}\n",
                    org_repo,
                    n_pages,
                    fmt_slice(&rem),
                    hint
                );
            }
            sh.api_calls.fetch_add(1, Ordering::SeqCst);
            // Returns events in Issue Event format (UI events)
            let r = sh.gcs[hint].issues_list_repository_events(org, repo, opt);
            let (value, response, err) = match r {
                Ok((v, resp)) => (Some(v), Some(resp), None),
                Err(e) => (None, None, Some(e)),
            };
            let res = handle_possible_error(err.as_ref(), &gcfg_str, "Issues.ListRepositoryEvents");
            if !res.is_empty() {
                if res == ABUSE {
                    abuse_backoff(sh, tr, "issues events");
                }
                if res == NOT_FOUND {
                    printf!("Warning: not found: {}/{}\n", org, repo);
                    return false;
                }
                continue;
            }
            success_raise(sh, "issues events");
            events = value.unwrap_or_default();
            next_page = response.map(|resp| resp.next_page).unwrap_or(0);
            got = true;
            break;
        }
        if !got {
            return retries_exhausted(ctx, "events");
        }
        let mut min_created_at = GoTimeValue::Now(gofmt::time_now());
        let mut max_created_at = GoTimeValue::Db(sh.recent_dt);
        for event in events {
            let created_at = event.created_at.expect("event without created_at").0;
            if created_at < min_created_at.instant() {
                min_created_at = GoTimeValue::Json(created_at);
            }
            if created_at > max_created_at.instant() {
                max_created_at = GoTimeValue::Json(created_at);
            }
            if sh.date_range.is_range {
                if let Some(from) = sh.date_range.from {
                    if created_at < from {
                        continue;
                    }
                }
                if let Some(to) = sh.date_range.to {
                    if created_at > to {
                        continue;
                    }
                }
            }
            let event_type = match event.event.clone() {
                Some(t) => t,
                None => {
                    printf!("Warning: Skipping event without type\n");
                    continue;
                }
            };
            let mut issue = match event.issue.clone() {
                Some(i) => i,
                None => {
                    printf!("Warning: Skipping event without issue\n");
                    continue;
                }
            };
            if !EVENT_TYPES.contains(&event_type.as_str()) {
                printf!(
                    "Warning: skipping event type {} for issue {} {}\n",
                    event_type,
                    org_repo,
                    issue.number.expect("issue without number")
                );
                continue;
            }
            let is_pr = issue.is_pull_request();
            if ctx.skip_api_issues && !is_pr {
                continue;
            }
            if ctx.skip_api_prs && is_pr {
                continue;
            }
            if let Some(single_issue) = filter.single_issue {
                if issue.number != Some(single_issue) {
                    continue;
                }
            }
            if let Some(single_milestone) = &filter.single_milestone {
                let title = issue.milestone.as_ref().and_then(|m| m.title.as_deref());
                if title != Some(single_milestone.as_str()) {
                    continue;
                }
            }
            if created_at < sh.recent_dt {
                continue;
            }
            let mut cfg = IssueConfig {
                repo: org_repo.to_string(),
                ..Default::default()
            };
            let eid = event.id.expect("event without id");
            let iid = issue.id.expect("issue without id");
            // Check for duplicate events
            let duplicate_info = {
                let mut eids = es.eids.lock().unwrap_or_else(|p| p.into_inner());
                match eids.get_mut(&eid) {
                    Some((counts, repos)) => {
                        *counts = [iid, counts[1] + 1];
                        repos.push(org_repo.to_string());
                        Some((*counts, repos.clone()))
                    }
                    None => {
                        eids.insert(eid, ([iid, 1], vec![org_repo.to_string()]));
                        None
                    }
                }
            };
            if let Some((counts, repos)) = duplicate_info {
                if ctx.debug > 0 {
                    printf!(
                        "Note: duplicate GH event {}, {}, {}\n",
                        eid,
                        fmt_slice(&counts),
                        fmt_slice(&repos)
                    );
                }
                return false;
            }
            cfg.milestone_id = issue.milestone.as_ref().and_then(|m| m.id);
            cfg.assignee_id = issue.assignee.as_ref().and_then(|a| a.id);
            if event_type == "renamed" {
                issue.title = event.rename.as_ref().and_then(|r| r.to.clone());
            }
            cfg.event_id = eid;
            cfg.issue_id = iid;
            cfg.event_type = event_type.clone();
            cfg.created_at = created_at;
            cfg.number = issue.number.expect("issue without number");
            cfg.pr = is_pr;
            // Labels
            cfg.set_labels_from(&issue.labels);
            // Assignees
            cfg.set_assignees_from(&issue.assignees);
            cfg.gh_issue = Some(issue);
            cfg.gh_event = Some(event);
            let cfg_str = cfg.to_string();
            let (cfg_repo, cfg_number, cfg_event_type, cfg_created_at) = (
                cfg.repo.clone(),
                cfg.number,
                cfg.event_type.clone(),
                cfg.created_at,
            );
            {
                let mut issues = es.issues.lock().unwrap_or_else(|p| p.into_inner());
                issues.entry(iid).or_default().push(cfg);
            }
            if ctx.debug > 1 {
                printf!("Processing {}\n", cfg_str);
            } else if ctx.debug == 1 {
                printf!(
                    "Processing {} issue number {}, event: {}, date: {}\n",
                    cfg_repo,
                    cfg_number,
                    cfg_event_type,
                    to_ymdhms_date(cfg_created_at)
                );
            }
            // Handle PR
            if is_pr {
                let found_pr = es
                    .prs
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .contains_key(&iid);
                if !found_pr {
                    let pr_num = cfg_number;
                    let mut pr: Option<PullRequest> = None;
                    got = false;
                    for tr in 0..ctx.max_ghapi_retry {
                        let (hint, rem) = match rate_gate(sh, tr, "Get PR", "PR", "PR") {
                            Gate::Proceed(h, r) => (h, r),
                            Gate::Retry => continue,
                            Gate::Abort => return false,
                        };
                        if ctx.github_debug > 0 {
                            printf!(
                                "API call for {} PR: {}, remaining GHAPI points {}, hint: {}\n",
                                org_repo,
                                pr_num,
                                fmt_slice(&rem),
                                hint
                            );
                        }
                        sh.api_calls.fetch_add(1, Ordering::SeqCst);
                        let res = sh.gcs[hint].pull_requests_get(org, repo, pr_num);
                        let (got_pr, err) = match res {
                            Ok((p, _)) => (Some(p), None),
                            Err(e) => (None, Some(e)),
                        };
                        pr = got_pr;
                        let res =
                            handle_possible_error(err.as_ref(), &gcfg_str, "PullRequests.Get");
                        if !res.is_empty() {
                            if res == ABUSE {
                                abuse_backoff(sh, tr, "get PR");
                            }
                            continue;
                        }
                        success_raise(sh, "get PR");
                        got = true;
                        break;
                    }
                    if !got {
                        return retries_exhausted(ctx, "PR");
                    }
                    if let Some(pr) = pr {
                        es.prs
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .insert(iid, pr);
                    }
                }
            }
        }
        let stop = min_created_at.instant() < sh.recent_dt;
        if ctx.debug > 0 {
            printf!(
                "{}: [{} - {}] < {}: {}\n",
                org_repo,
                min_created_at.render(),
                max_created_at.render(),
                db_time(sh.recent_dt),
                stop
            );
        }
        if stop {
            break;
        }
        // Handle paging
        if next_page == 0 {
            break;
        }
        opt.page = next_page;
    }
    true
}

/// Go `syncEvents`: the recent issue/PR events of the recent repositories
/// as artificial events. Debugging options (environment variables):
/// `REPO`, `DTFROM`/`DTTO`, `MILESTONE=milestone name`, `ISSUE=number`.
fn sync_events(ctx: &mut Ctx) {
    // Get common params
    let params = get_api_params(ctx);

    // Date range mode
    let date_range = date_range_from_env();

    // Single milestone mode
    let single_milestone = std::env::var("MILESTONE").ok().filter(|s| !s.is_empty());

    // Single issue mode
    let single_issue = std::env::var("ISSUE")
        .ok()
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<i64>().ok());
    let filter = EventsFilter {
        single_milestone,
        single_issue,
    };

    // Get number of CPUs available
    let thr_n = get_threads_num(ctx);
    // GitHub is not detecting abuse when using 16 threads, but it detects when using 32.
    let max_threads = 16.min(thr_n).max(1);
    let n_repos = params.repos.len();
    printf!(
        "ghapi2db.go: Processing {} repos - GHAPI Events part\n",
        n_repos
    );

    let sh = Shared {
        ctx,
        gcs: &params.gcs,
        c: &params.c,
        recent_dt: params.recent_dt,
        is_single_repo: params.is_single_repo,
        single_repo: &params.single_repo,
        date_range: &date_range,
        max_threads,
        allowed_thr_n: AtomicUsize::new(max_threads),
        n_threads: AtomicUsize::new(0),
        api_calls: AtomicUsize::new(0),
    };
    let es = EventsShared {
        issues: Mutex::new(IssuesMap::new()),
        prs: Mutex::new(PrsMap::new()),
        eids: Mutex::new(std::collections::BTreeMap::new()),
    };
    {
        let es = &es;
        let filter = &filter;
        run_pool(&sh, &params.repos, |sh, org_repo| {
            fetch_events(sh, es, filter, org_repo)
        });
    }

    // API calls
    printf!(
        "GH Repo Events/PRs API calls: {}\n",
        sh.api_calls.load(Ordering::SeqCst)
    );
    let mut issues = es.issues.into_inner().unwrap_or_else(|p| p.into_inner());
    let prs = es.prs.into_inner().unwrap_or_else(|p| p.into_inner());

    // Do final corrections
    // manual sync: false
    sync_issues_state(&params.gcs, ctx, &params.c, &mut issues, &prs, false);
    params.c.close();
}

/// The rate budget shared by the licenses/languages pass and its workers
/// (Go's captured `hint, rem, wait, allowed, processed, …`).
struct Budget {
    hint: usize,
    rem: Vec<i64>,
    wait: Vec<GoDuration>,
    allowed: i64,
    processed: usize,
    found: usize,
    not_found: usize,
    abuses: usize,
    last_time: DateTime<Utc>,
    dt_start: DateTime<Utc>,
}

/// The shared state of a licenses/languages pass.
struct RepoPass<'a> {
    ctx: &'a Ctx,
    gcs: &'a [Client],
    n_repos: usize,
    /// `licenses` / `programming languages` (the rate-limit messages).
    what: &'a str,
    budget: Mutex<Budget>,
}

impl RepoPass<'_> {
    /// Go `handleRate`: wait for the reset (or abort) when out of points,
    /// then set the allowed calls budget. `false` aborts the pass.
    fn handle_rate(&self, b: &mut Budget) -> bool {
        let ctx = self.ctx;
        if b.rem[b.hint] <= ctx.min_ghapi_points {
            if b.wait[b.hint].seconds() <= ctx.max_ghapi_wait_seconds as f64 {
                if ctx.github_debug > 0 {
                    printf!(
                        "API limit reached while getting {} data, waiting {}\n",
                        self.what,
                        b.wait[b.hint]
                    );
                }
                std::thread::sleep(Duration::from_secs(1));
                b.wait[b.hint].sleep();
            } else {
                if ctx.ghapi_error_is_fatal {
                    fatalf!(
                        "API limit reached while getting {} data, aborting, don't want to wait {}",
                        self.what,
                        b.wait[b.hint]
                    );
                }
                printf!(
                    "Error: API limit reached while getting {} data, aborting, don't want to wait {}\n",
                    self.what,
                    b.wait[b.hint]
                );
                return false;
            }
            let (hint, _, rem, wait) = get_rate_limits(ctx, self.gcs, true);
            b.hint = hint;
            b.rem = rem;
            b.wait = wait;
        }
        b.allowed = b.rem[b.hint] / 10;
        true
    }

    /// Go `iter`: account one processed repository (or one abuse), refresh
    /// the budget when exhausted, print the progress. `false` aborts.
    fn iter(&self, b: &mut Budget, abused: bool) -> bool {
        if !abused {
            b.processed += 1;
            b.allowed -= 1;
        } else {
            b.allowed = 0;
            b.abuses += 1;
        }
        if b.allowed <= 0 {
            let (hint, _, rem, wait) = get_rate_limits(self.ctx, self.gcs, true);
            b.hint = hint;
            b.rem = rem;
            b.wait = wait;
            if !self.handle_rate(b) {
                return false;
            }
        }
        let msg = format!(
            "API points: {}, resets in: {}, hint: {}",
            fmt_slice(&b.rem),
            fmt_slice(&b.wait),
            b.hint
        );
        let (processed, dt_start) = (b.processed, b.dt_start);
        progress_info(
            processed,
            self.n_repos,
            dt_start,
            &mut b.last_time,
            Duration::from_secs(30),
            &msg,
        );
        true
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Budget> {
        self.budget.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The Go goroutine pool of `syncLicenses`/`syncLangs` (`thrN > 1`) or
    /// the sequential loop; `false` when the pass was aborted (`return`).
    fn run(&self, ctx: &Ctx, thr_n: usize, repos: &[String], worker: impl Fn(&str) + Sync) -> bool {
        let worker = &worker;
        let mut thr_n = thr_n;
        let mut aborted = false;
        if thr_n > 1 {
            let (tx, rx) = mpsc::channel::<()>();
            std::thread::scope(|scope| {
                let mut n_threads = 0usize;
                let mut prc = 0usize;
                'repos: for repo in repos {
                    let tx = tx.clone();
                    scope.spawn(move || {
                        worker(repo);
                        let _ = tx.send(());
                    });
                    n_threads += 1;
                    while n_threads >= thr_n {
                        let _ = rx.recv();
                        n_threads -= 1;
                        prc += 1;
                        if prc.is_multiple_of(20) {
                            thr_n = get_threads_num(&mut ctx.copy_context());
                        }
                        let mut b = self.lock();
                        if !self.iter(&mut b, false) {
                            aborted = true;
                            break 'repos;
                        }
                    }
                }
                if !aborted {
                    while n_threads > 0 {
                        let _ = rx.recv();
                        n_threads -= 1;
                        let mut b = self.lock();
                        if !self.iter(&mut b, false) {
                            aborted = true;
                            break;
                        }
                    }
                }
            });
        } else {
            for repo in repos {
                worker(repo);
                let mut b = self.lock();
                if !self.iter(&mut b, false) {
                    aborted = true;
                    break;
                }
            }
        }
        !aborted
    }
}

/// The repositories of `RepoNamesQuery` (+ `extra` condition).
fn repo_names(c: &PgConn, ctx: &Ctx, extra: &str) -> Vec<String> {
    let query = format!("{}{}", REPO_NAMES_QUERY, extra);
    let mut repos = Vec::new();
    let mut rows = query_sql_with_err(c, ctx, &query, &[]);
    while rows.next() {
        let mut repo = String::new();
        fatal_on_err(rows.scan(&mut [&mut repo]));
        repos.push(repo);
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    repos
}

/// Start a licenses/languages pass: the rate limits and the budget.
fn new_budget(ctx: &Ctx, gcs: &[Client]) -> Budget {
    let (hint, _, rem, wait) = get_rate_limits(ctx, gcs, true);
    let now = Utc::now();
    Budget {
        hint,
        rem,
        wait,
        allowed: 0,
        processed: 0,
        found: 0,
        not_found: 0,
        abuses: 0,
        last_time: now,
        dt_start: now,
    }
}

/// Go `%+v` of a `*github.License` — Go prints the addresses of the pointer
/// fields there; the values are printed instead.
/// go-github `License.String()` (`Stringify`): `github.License{Key:"mit", ...}`
/// listing only the non-nil fields, strings quoted, `*[]string` as `["a" "b"]`.
fn license_string(l: &License) -> String {
    fn p(parts: &mut Vec<String>, name: &str, v: &Option<String>) {
        if let Some(v) = v {
            parts.push(format!("{name}:\"{v}\""));
        }
    }
    fn a(parts: &mut Vec<String>, name: &str, v: &Option<Vec<String>>) {
        if let Some(v) = v {
            let items: Vec<String> = v.iter().map(|s| format!("\"{s}\"")).collect();
            parts.push(format!("{name}:[{}]", items.join(" ")));
        }
    }
    let mut parts: Vec<String> = Vec::new();
    p(&mut parts, "Key", &l.key);
    p(&mut parts, "Name", &l.name);
    p(&mut parts, "URL", &l.url);
    p(&mut parts, "SPDXID", &l.spdx_id);
    p(&mut parts, "HTMLURL", &l.html_url);
    if let Some(b) = l.featured {
        parts.push(format!("Featured:{b}"));
    }
    p(&mut parts, "Description", &l.description);
    p(&mut parts, "Implementation", &l.implementation);
    a(&mut parts, "Permissions", &l.permissions);
    a(&mut parts, "Conditions", &l.conditions);
    a(&mut parts, "Limitations", &l.limitations);
    p(&mut parts, "Body", &l.body);
    format!("github.License{{{}}}", parts.join(", "))
}

/// A `*string` passed straight to `database/sql`.
fn opt_str(v: &Option<String>) -> SqlArg {
    match v {
        None => SqlArg::Null,
        Some(s) => SqlArg::Str(s.clone()),
    }
}

/// Go `syncLicenses`: fetch the license of every repository without one
/// (all of them with `GHA2DB_FORCE_API_LICENSES`).
fn sync_licenses(ctx: &mut Ctx) {
    let gcs = gh_client(ctx);
    let c = pg_conn(ctx);
    let extra = if ctx.force_api_licenses {
        ""
    } else {
        " and (license_key is null or license_key = '')"
    };
    let repos = repo_names(&c, ctx, extra);
    let n_repos = repos.len();
    printf!("Checking license on {} repos\n", n_repos);
    let thr_n = get_threads_num(ctx);
    let pass = RepoPass {
        ctx,
        gcs: &gcs,
        n_repos,
        what: "licenses",
        budget: Mutex::new(new_budget(ctx, &gcs)),
    };
    {
        let mut b = pass.lock();
        if !pass.handle_rate(&mut b) {
            drop(b);
            c.close();
            return;
        }
    }
    let update_query = format!(
        "update gha_repos set license_key = {}, license_name = {}, license_prob = {}, updated_at = {} where name = {}",
        n_value(1),
        n_value(2),
        n_value(3),
        n_value(4),
        n_value(5)
    );
    let get_license = |org_repo: &str| {
        let ctx = pass.ctx;
        let no_license = || {
            exec_sql_with_err(
                &c,
                ctx,
                &update_query,
                &[
                    SqlArg::from("not_found"),
                    SqlArg::from("Not found"),
                    SqlArg::Float(0.0),
                    SqlArg::from(Local::now()),
                    SqlArg::from(org_repo),
                ],
            );
            pass.lock().not_found += 1;
        };
        let cl = &gcs[pass.lock().hint];
        let ary: Vec<&str> = org_repo.split('/').collect();
        if ary.len() < 2 {
            printf!("WARNING: malformed repo name: '{}'\n", org_repo);
            return;
        }
        let org = ary[0];
        let repo = ary[1];
        let license;
        let mut retries = 0;
        loop {
            let r = cl.repositories_license(org, repo);
            let resp = match &r.response {
                None => {
                    printf!(
                        "License API response is null for {}/{}, skipping\n",
                        org,
                        repo
                    );
                    return;
                }
                Some(resp) => resp,
            };
            if resp.status == 404 {
                printf!("No license found for: {}/{} (404)\n", org, repo);
                no_license();
                return;
            }
            if resp.status >= 400 {
                if resp.status == 403 {
                    retries += 1;
                    if retries > ctx.max_ghapi_retry {
                        printf!(
                            "Licenses abuse detected on {}/{}, giving up after {} retries\n",
                            org,
                            repo,
                            ctx.max_ghapi_retry
                        );
                        return;
                    }
                    printf!("Licenses abuse detected on {}/{}, retrying\n", org, repo);
                    let mut b = pass.lock();
                    if !pass.iter(&mut b, true) {
                        return;
                    }
                    continue;
                }
                printf!(
                    "No license found for: {}/{}, skipping ({})\n",
                    org,
                    repo,
                    resp.status
                );
                return;
            }
            if let Some(e) = r.error {
                fatal_on_err::<(), _>(Err(e));
            }
            let lic = match r.value {
                None => {
                    printf!("License is null for {}/{}, skipping\n", org, repo);
                    return;
                }
                Some(lic) => lic,
            };
            if lic.license.is_none() {
                printf!("No license found for: {}/{} (nil)\n", org, repo);
                return;
            }
            license = lic;
            break;
        }
        let lic = license.license.expect("license checked above");
        if ctx.debug > 0 {
            printf!("{} license:{}\n", org_repo, license_string(&lic));
        }
        exec_sql_with_err(
            &c,
            ctx,
            &update_query,
            &[
                opt_str(&lic.key),
                opt_str(&lic.name),
                SqlArg::Float(100.0),
                SqlArg::from(Local::now()),
                SqlArg::from(org_repo),
            ],
        );
        pass.lock().found += 1;
    };
    if !pass.run(ctx, thr_n, &repos, get_license) {
        c.close();
        return;
    }
    let b = pass.lock();
    printf!(
        "Processed {}, found {} licenses, {} not found, abuses {}\n",
        b.processed,
        b.found,
        b.not_found,
        b.abuses
    );
    drop(b);
    c.close();
}

/// Go `syncLangs`: fetch the programming languages of every repository
/// without them (all of them with `GHA2DB_FORCE_API_LANGS`).
fn sync_langs(ctx: &mut Ctx) {
    let gcs = gh_client(ctx);
    let c = pg_conn(ctx);
    let extra = if ctx.force_api_langs {
        ""
    } else {
        " and name not in (select distinct repo_name from gha_repos_langs)"
    };
    let repos = repo_names(&c, ctx, extra);
    let n_repos = repos.len();
    printf!("Checking programming languages on {} repos\n", n_repos);
    let thr_n = get_threads_num(ctx);
    let pass = RepoPass {
        ctx,
        gcs: &gcs,
        n_repos,
        what: "programming languages",
        budget: Mutex::new(new_budget(ctx, &gcs)),
    };
    {
        let mut b = pass.lock();
        if !pass.handle_rate(&mut b) {
            drop(b);
            c.close();
            return;
        }
    }
    let get_langs = |org_repo: &str| {
        let ctx = pass.ctx;
        let no_langs = || {
            exec_sql_with_err(
                &c,
                ctx,
                &insert_ignore(&format!(
                    "into gha_repos_langs(repo_name, lang_name, lang_loc, lang_perc) {}",
                    n_values(4)
                )),
                &[
                    SqlArg::from(org_repo),
                    SqlArg::from("unknown"),
                    SqlArg::Int(0),
                    SqlArg::Float(0.0),
                ],
            );
            pass.lock().not_found += 1;
        };
        let cl = &gcs[pass.lock().hint];
        let ary: Vec<&str> = org_repo.split('/').collect();
        if ary.len() < 2 {
            printf!("WARNING: malformed repo name: '{}'\n", org_repo);
            return;
        }
        let org = ary[0];
        let repo = ary[1];
        let langs;
        let when = Local::now();
        let mut retries = 0;
        loop {
            let r = cl.repositories_list_languages_full(org, repo);
            let resp = match &r.response {
                None => {
                    printf!(
                        "Languages API response is null for {}/{}, skipping\n",
                        org,
                        repo
                    );
                    return;
                }
                Some(resp) => resp,
            };
            if resp.status == 404 {
                printf!(
                    "No programming languages found for: {}/{} (404)\n",
                    org,
                    repo
                );
                no_langs();
                return;
            }
            if resp.status >= 400 {
                if resp.status == 403 {
                    retries += 1;
                    if retries > ctx.max_ghapi_retry {
                        printf!(
                            "Languages abuse detected on {}/{}, giving up after {} retries\n",
                            org,
                            repo,
                            ctx.max_ghapi_retry
                        );
                        return;
                    }
                    printf!("Languages abuse detected on {}/{}, retrying\n", org, repo);
                    let mut b = pass.lock();
                    if !pass.iter(&mut b, true) {
                        return;
                    }
                    continue;
                }
                printf!(
                    "No languages found for: {}/{}, skipping ({})\n",
                    org,
                    repo,
                    resp.status
                );
                return;
            }
            if let Some(e) = r.error {
                fatal_on_err::<(), _>(Err(e));
            }
            let ls = r.value.unwrap_or_default();
            if ls.is_empty() {
                printf!("No programming languages found for: {}/{} (0)\n", org, repo);
                no_langs();
                return;
            }
            langs = ls;
            break;
        }
        if ctx.debug > 0 {
            printf!("{} languages: {}\n", org_repo, gofmt::map(&langs));
        }
        let all_loc: i64 = langs.values().sum();
        if all_loc == 0 {
            printf!("All BOC sum to 0 for: {}/{}\n", org, repo);
            no_langs();
            return;
        }
        exec_sql_with_err(
            &c,
            ctx,
            &format!(
                "delete from gha_repos_langs where repo_name = {}",
                n_value(1)
            ),
            &[SqlArg::from(org_repo)],
        );
        for (lang, loc) in &langs {
            let perc = (*loc as f64 * 100.0) / all_loc as f64;
            exec_sql_with_err(
                &c,
                ctx,
                &format!(
                    "insert into gha_repos_langs(repo_name, lang_name, lang_loc, lang_perc, dt) {}",
                    n_values(5)
                ),
                &[
                    SqlArg::from(org_repo),
                    SqlArg::from(lang),
                    SqlArg::Int(*loc),
                    SqlArg::Float(perc),
                    SqlArg::from(when),
                ],
            );
        }
        pass.lock().found += 1;
    };
    if !pass.run(ctx, thr_n, &repos, get_langs) {
        c.close();
        return;
    }
    let b = pass.lock();
    printf!(
        "Processed {}, found languages on {} repos, on {} not found, abuses: {}\n",
        b.processed,
        b.found,
        b.not_found,
        b.abuses
    );
    drop(b);
    c.close();
}

fn main() {
    devstatscode::error::exit_on_panic();
    gofmt::mark_process_start();
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    if !ctx.affiliations_db.is_empty() {
        let mut actx = ctx.copy_context();
        let con = pg_conn_db(&mut actx, &ctx.affiliations_db);
        set_shared_affiliations_db(con, actx);
    }
    // Go tunes the GC (`debug.SetGCPercent(25)`) and returns freed memory to
    // the OS every minute here — nothing to do without a GC.

    let dt_start = Instant::now();
    // Create artificial events
    if !ctx.skip_ghapi {
        if !ctx.skip_api_licenses {
            sync_licenses(&mut ctx);
        }
        if !ctx.skip_api_langs {
            sync_langs(&mut ctx);
        }
        if !ctx.skip_api_events {
            sync_events(&mut ctx);
        }
        if !ctx.skip_api_commits {
            sync_commits(&mut ctx);
        }
        let mut restored = RestoreStats::default();
        if !ctx.skip_api_comments {
            restored.merge(restore::sync_comments(&mut ctx));
        }
        if !ctx.skip_api_reviews {
            restored.merge(restore::sync_reviews(&mut ctx));
        }
        if !ctx.skip_api_forks {
            restored.merge(restore::sync_forks(&mut ctx));
        }
        if !ctx.skip_api_releases {
            restored.merge(restore::sync_releases(&mut ctx));
        }
        if !ctx.skip_api_stars {
            restored.merge(restore::sync_stars(&mut ctx));
        }
        if !restored.eids.is_empty() {
            run_event_ids_postprocess(&ctx, &restored.eids);
        }
    }
    printf!("Time: {}\n", format_go_duration(dt_start.elapsed()));
}
