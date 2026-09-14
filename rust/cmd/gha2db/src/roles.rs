//! Commit roles maintenance — port of `refreshCommitRoles` and
//! `updateCommitRoles` of `cmd/gha2db/gha2db.go` (run deferred at the end of
//! every `gha2db` run: the former with `GHA2DB_REFRESH_COMMIT_ROLES`, the
//! latter otherwise).

use std::collections::HashMap;
use std::sync::{mpsc, Mutex, RwLock};

use chrono::{DateTime, Utc};
use devstatscode::consts::HIDE_CFG_FILE;
use devstatscode::pg::api::{
    exec_sql_with_err, insert_ignore, n_value, n_values, query_sql_with_err, trunc_to_bytes,
};
use devstatscode::pg::{pg_conn, PgConn, SqlArg};
use devstatscode::string::{get_hidden, maybe_hide_func};
use devstatscode::threads::get_threads_num;
use devstatscode::trailers::{GIT_ALLOWED_TRAILERS, GIT_TRAILER_PATTERN};
use devstatscode::{fatal_on_err, printf, Ctx};

use crate::run_gc;
use devstatscode::ghawriter::{cache_len, lookup_actor_name_email, Db, MaybeHide};

/// One `gha_commits` row of the refresh query.
struct CommitRow {
    sha: String,
    event_id: i64,
    repo_id: i64,
    repo_name: String,
    ev_created_at: DateTime<Utc>,
    msg: String,
}

/// Go `refreshCommitRoles`: (re)create `gha_commits_roles` rows for all the
/// commits in the database that have none yet.
pub fn refresh_commit_roles(ctx: &Ctx) {
    // GDPR data hiding
    let sha_map = get_hidden(ctx, HIDE_CFG_FILE);
    let hider = maybe_hide_func(sha_map);
    let maybe_hide: MaybeHide<'_> = &hider;
    let igc = Mutex::new(0usize);
    let maybe_gc = |val: usize| {
        let mut g = igc.lock().unwrap_or_else(|p| p.into_inner());
        *g += 1;
        if g.is_multiple_of(val) {
            run_gc();
        }
    };
    // Connect to Postgres DB
    let con = pg_conn(ctx);
    let mut offset = 0usize;
    let limit = 1000usize;
    // Get number of CPUs available
    let mut thr_n = get_threads_num(&mut ctx.copy_context());
    let updated = Mutex::new(0i64);
    let mut grand_updated = 0i64;
    let roles_map: RwLock<HashMap<String, Vec<SqlArg>>> = RwLock::new(HashMap::new());
    let add_mapping_func = |row: &CommitRow| {
        let ky_root = format!("{}-{}-", row.sha, row.event_id);
        let mut role_added = false;
        let msg = row.msg.replace('\r', "\n");
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
            let (id, login) =
                lookup_actor_name_email(&mut Db::Con(&con), ctx, name, email, maybe_hide);
            for role in trailers.iter() {
                let ky = format!("{}{}-{}", ky_root, role, email.to_lowercase());
                if roles_map
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .contains_key(&ky)
                {
                    continue;
                }
                roles_map.write().unwrap_or_else(|p| p.into_inner()).insert(
                    ky,
                    vec![
                        SqlArg::from(&row.sha),
                        SqlArg::Int(row.event_id),
                        SqlArg::from(*role),
                        SqlArg::Int(id),
                        SqlArg::Str(maybe_hide(&trunc_to_bytes(&login, 120))),
                        SqlArg::Str(maybe_hide(&trunc_to_bytes(name, 160))),
                        SqlArg::Str(maybe_hide(&trunc_to_bytes(email, 160))),
                        SqlArg::Int(row.repo_id),
                        SqlArg::from(&row.repo_name),
                        SqlArg::DbTime(row.ev_created_at.fixed_offset()),
                    ],
                );
                role_added = true;
            }
        }
        if role_added {
            *updated.lock().unwrap_or_else(|p| p.into_inner()) += 1;
        }
    };
    let add_mapping_func = &add_mapping_func;
    let mut first_loop = true;
    let mut all_commits = 0i64;
    loop {
        let mut rows = query_sql_with_err(
            &con,
            ctx,
            &format!(
                "select distinct sha, event_id, dup_repo_id, dup_repo_name, dup_created_at, message \
                 from gha_commits where (sha, event_id) not in (select sha, event_id from gha_commits_roles) \
                 order by sha, event_id limit {} offset {}",
                limit, offset
            ),
            &[],
        );
        let mut commits: Vec<CommitRow> = Vec::new();
        while rows.next() {
            let mut row = CommitRow {
                sha: String::new(),
                event_id: 0,
                repo_id: 0,
                repo_name: String::new(),
                ev_created_at: DateTime::<Utc>::UNIX_EPOCH,
                msg: String::new(),
            };
            fatal_on_err(rows.scan(&mut [
                &mut row.sha,
                &mut row.event_id,
                &mut row.repo_id,
                &mut row.repo_name,
                &mut row.ev_created_at,
                &mut row.msg,
            ]));
            commits.push(row);
        }
        fatal_on_err(rows.err());
        fatal_on_err(rows.close());
        let n_commits = commits.len();
        if first_loop {
            all_commits = n_commits as i64;
        }
        if n_commits == 0 {
            break;
        }
        if n_commits == limit && first_loop {
            first_loop = false;
            let mut arows = query_sql_with_err(
                &con,
                ctx,
                "select count(distinct sha || event_id) from gha_commits \
                 where (sha, event_id) not in (select sha, event_id from gha_commits_roles)",
                &[],
            );
            if arows.next() {
                fatal_on_err(arows.scan(&mut [&mut all_commits]));
            }
            fatal_on_err(arows.err());
            fatal_on_err(arows.close());
        }
        let n_cache = cache_len();
        printf!(
            "Processing {} commits (all: {}) using {} CPUs, cached: {}\n",
            n_commits,
            all_commits,
            thr_n,
            n_cache
        );
        maybe_gc(10);
        *updated.lock().unwrap_or_else(|p| p.into_inner()) = 0;
        // MT or ST
        let mut prc = 0usize;
        if thr_n > 1 {
            let (tx, rx) = mpsc::channel::<()>();
            std::thread::scope(|scope| {
                let mut n_threads = 0usize;
                for row in &commits {
                    let tx = tx.clone();
                    scope.spawn(move || {
                        add_mapping_func(row);
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
                    }
                }
                while n_threads > 0 {
                    let _ = rx.recv();
                    n_threads -= 1;
                }
            });
        } else {
            for row in &commits {
                add_mapping_func(row);
            }
        }
        let upd = *updated.lock().unwrap_or_else(|p| p.into_inner());
        grand_updated += upd;
        printf!(
            "Processed {}/{} commits using {} CPUs ({} so far, offset {})\n",
            upd,
            n_commits,
            thr_n,
            grand_updated,
            offset
        );
        offset += limit;
    }
    let roles_map = roles_map.into_inner().unwrap_or_else(|p| p.into_inner());
    let n_rols = roles_map.len();
    printf!(
        "Processed {} commits with at least 1 commit role\n",
        grand_updated
    );
    printf!("Now updating/inserting {} commit roles\n", n_rols);
    let update_func = |data: &Vec<SqlArg>| {
        exec_sql_with_err(
            &con,
            ctx,
            &insert_ignore(&format!(
                "into gha_commits_roles(\
                 sha, event_id, role, actor_id, actor_login, actor_name, actor_email, \
                 dup_repo_id, dup_repo_name, dup_created_at\
                 ) {}",
                n_values(10)
            )),
            data,
        );
    };
    let update_func = &update_func;
    let mut idx = 0usize;
    if thr_n > 8 {
        thr_n = 8;
    }
    let mut prc = 0usize;
    if thr_n > 1 {
        let (tx, rx) = mpsc::channel::<()>();
        std::thread::scope(|scope| {
            let mut n_threads = 0usize;
            for data in roles_map.values() {
                idx += 1;
                if idx.is_multiple_of(limit) {
                    printf!("Updating/inserting commit roles: {}/{}\n", idx, n_rols);
                    maybe_gc(20);
                }
                let tx = tx.clone();
                scope.spawn(move || {
                    update_func(data);
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
                }
            }
            while n_threads > 0 {
                let _ = rx.recv();
                n_threads -= 1;
            }
        });
    } else {
        for data in roles_map.values() {
            idx += 1;
            if idx.is_multiple_of(limit) {
                printf!("Updating/inserting commit roles: {}/{}\n", idx, n_rols);
                maybe_gc(20);
            }
            update_func(data);
        }
    }
    con.close();
}

/// Go `updateCommitRoles`: try to find the missing actor IDs/logins in the
/// `gha_commits_roles` table.
pub fn update_commit_roles(ctx: &Ctx) {
    // GDPR data hiding
    let sha_map = get_hidden(ctx, HIDE_CFG_FILE);
    let hider = maybe_hide_func(sha_map);
    let maybe_hide: MaybeHide<'_> = &hider;
    // Connect to Postgres DB
    let con = pg_conn(ctx);
    let mut rows = query_sql_with_err(
        &con,
        ctx,
        "select distinct actor_email, actor_name from gha_commits_roles where actor_id = 0 or actor_login = '' or actor_id is null or actor_login is null",
        &[],
    );
    let mut pairs: Vec<(String, String)> = Vec::new();
    while rows.next() {
        let (mut email, mut name) = (String::new(), String::new());
        fatal_on_err(rows.scan(&mut [&mut email, &mut name]));
        pairs.push((name, email));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    // Get number of CPUs available
    let mut thr_n = get_threads_num(&mut ctx.copy_context());
    let n_roles = pairs.len();
    printf!("Processing {} commit roles using {} CPUs\n", n_roles, thr_n);
    let updated = Mutex::new(0i64);
    let update_func = |name: &str, email: &str| {
        let (id, login) = lookup_actor_name_email(&mut Db::Con(&con), ctx, name, email, maybe_hide);
        if id != 0 {
            exec_sql_with_err(
                &con,
                ctx,
                &format!(
                    "update gha_commits_roles set actor_id={}, actor_login={} where actor_name={} and actor_email={}",
                    n_value(1),
                    n_value(2),
                    n_value(3),
                    n_value(4)
                ),
                &[
                    SqlArg::Int(id),
                    SqlArg::Str(maybe_hide(&login)),
                    SqlArg::Str(maybe_hide(name)),
                    SqlArg::Str(maybe_hide(email)),
                ],
            );
            *updated.lock().unwrap_or_else(|p| p.into_inner()) += 1;
        }
    };
    let update_func = &update_func;
    // MT or ST
    let mut prc = 0usize;
    if thr_n > 1 {
        let (tx, rx) = mpsc::channel::<()>();
        std::thread::scope(|scope| {
            let mut n_threads = 0usize;
            for (name, email) in &pairs {
                let tx = tx.clone();
                scope.spawn(move || {
                    update_func(name, email);
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
                }
            }
            while n_threads > 0 {
                let _ = rx.recv();
                n_threads -= 1;
            }
        });
    } else {
        for (name, email) in &pairs {
            update_func(name, email);
        }
    }
    let upd = *updated.lock().unwrap_or_else(|p| p.into_inner());
    printf!("Updated {}/{} roles using {} CPUs\n", upd, n_roles, thr_n);
    con.close();
}

/// Keep the unused-import lint quiet for the pooled connection type used in
/// the closures above.
#[allow(dead_code)]
fn _con_type(_: &PgConn) {}
