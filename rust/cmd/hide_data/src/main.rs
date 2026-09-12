//! `hide_data` — Rust port of `cmd/hide_data/hide_data.go`.
//!
//! * Without arguments: anonymize every actor whose SHA1 is listed in
//!   `hide/hide.csv` in all (enabled) project databases from `projects.yaml`
//!   — every login/name/email column that can carry an actor identity is
//!   `update`d to `anon-<sha1>` where `sha1(column) = <sha1>` (pgcrypto
//!   `digest`), one worker per (database, SHA1) pair.
//! * With arguments: add the SHA1s of the (trimmed) arguments to
//!   `hide/hide.csv` (skipping the ones already there).
//!
//! Output, environment handling, fatal conditions and exit codes follow the Go
//! tool; the only intended differences are documented in `rust/README.md`
//! (the Go tool walks its SHA1 map in random order, this port in sorted order).

use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use devstatscode::pg::api::exec_sql_with_err;
use devstatscode::pg::SqlArg;
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    consts, fatal_on_err, fatal_on_error, gocsv, gofmt, io, pg, printf, projects, signal, string,
    threads, time as gotime, Ctx,
};

/// One `(table, column)` pair to anonymize.
struct ReplaceConfig {
    table: &'static str,
    column: &'static str,
}

/// Every column that may hold an actor's login, name or e-mail (Go order,
/// including the duplicated `gha_issues.dup_actor_login` entry).
const REPLACES: &[ReplaceConfig] = &[
    ReplaceConfig {
        table: "gha_actors",
        column: "login",
    },
    ReplaceConfig {
        table: "gha_actors",
        column: "name",
    },
    ReplaceConfig {
        table: "gha_actors_emails",
        column: "email",
    },
    ReplaceConfig {
        table: "gha_actors_names",
        column: "name",
    },
    ReplaceConfig {
        table: "gha_actors_affiliations",
        column: "company_name",
    },
    ReplaceConfig {
        table: "gha_actors_affiliations",
        column: "original_company_name",
    },
    ReplaceConfig {
        table: "gha_companies",
        column: "name",
    },
    ReplaceConfig {
        table: "gha_events",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_payloads",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_commits",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_commits",
        column: "dup_author_login",
    },
    ReplaceConfig {
        table: "gha_commits",
        column: "dup_committer_login",
    },
    ReplaceConfig {
        table: "gha_commits",
        column: "author_name",
    },
    ReplaceConfig {
        table: "gha_commits",
        column: "author_email",
    },
    ReplaceConfig {
        table: "gha_commits",
        column: "committer_name",
    },
    ReplaceConfig {
        table: "gha_commits",
        column: "committer_email",
    },
    ReplaceConfig {
        table: "gha_commits_roles",
        column: "actor_login",
    },
    ReplaceConfig {
        table: "gha_commits_roles",
        column: "actor_name",
    },
    ReplaceConfig {
        table: "gha_commits_roles",
        column: "actor_email",
    },
    ReplaceConfig {
        table: "gha_pages",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_comments",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_comments",
        column: "dup_user_login",
    },
    ReplaceConfig {
        table: "gha_reviews",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_reviews",
        column: "dup_user_login",
    },
    ReplaceConfig {
        table: "gha_issues",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_issues",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_issues",
        column: "dup_user_login",
    },
    ReplaceConfig {
        table: "gha_milestones",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_milestones",
        column: "dupn_creator_login",
    },
    ReplaceConfig {
        table: "gha_issues_labels",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_releases",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_releases",
        column: "dup_author_login",
    },
    ReplaceConfig {
        table: "gha_assets",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_assets",
        column: "dup_uploader_login",
    },
    ReplaceConfig {
        table: "gha_pull_requests",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_pull_requests",
        column: "dup_user_login",
    },
    ReplaceConfig {
        table: "gha_teams",
        column: "dup_actor_login",
    },
    ReplaceConfig {
        table: "gha_texts",
        column: "actor_login",
    },
    ReplaceConfig {
        table: "gha_issues_events_labels",
        column: "actor_login",
    },
];

/// One unit of work: anonymize `sha` as `anon` in database `db`.
struct Task {
    db: String,
    sha: String,
    anon: String,
}

/// Run all `REPLACES` updates of one task on its own connection.
fn process_task(ctx: &Ctx, task: &Task) {
    let con = pg::pg_conn_db_shared(ctx, &task.db);
    for replace in REPLACES {
        let res = exec_sql_with_err(
            &con,
            ctx,
            &format!(
                "update {} set {} = {} where encode(digest({}, 'sha1'), 'hex') = {}",
                replace.table,
                replace.column,
                pg::api::n_value(1),
                replace.column,
                pg::api::n_value(2),
            ),
            &[
                SqlArg::from(task.anon.as_str()),
                SqlArg::from(task.sha.as_str()),
            ],
        );
        let rows = fatal_on_err(res.rows_affected());
        if rows > 0 {
            printf!(
                "DB: {}, table: {}, column: {}, sha: {}, updated {} rows\n",
                task.db,
                replace.table,
                replace.column,
                task.sha,
                rows
            );
        }
    }
    // `defer func() { lib.FatalOnError(con.Close()) }()` — closing the pool
    // cannot fail here.
    con.close();
}

/// Anonymize every hidden SHA1 in every enabled project database.
fn process_hidden(ctx: &mut Ctx) {
    let sha_map = string::get_hidden(ctx, consts::HIDE_CFG_FILE);

    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // `ioutil.ReadFile` — no `/shared/` fallback.
    let data = fatal_on_err(io::read_file_raw(format!(
        "{data_prefix}{}",
        ctx.projects_yaml
    )));
    let all: projects::AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };
    // Enabled projects by order (+ `ONLY`), see `projects::get_projects_list`.
    let (_names, projs) = projects::get_projects_list(ctx, &all);

    let mut tasks: Vec<Task> = Vec::new();
    let mut dbs: Vec<String> = Vec::new();
    for proj in &projs {
        for (sha, anon) in &sha_map {
            tasks.push(Task {
                db: proj.pdb.clone(),
                sha: sha.clone(),
                anon: anon.clone(),
            });
        }
        dbs.push(proj.pdb.clone());
    }
    printf!("Processing databases: {}\n", gofmt::slice(&dbs));
    let thr_n = threads::get_threads_num(ctx);
    // Go `PgConnDB` clears the reconnect flag from every worker; the workers
    // here share the context read-only, so clear it up front.
    if !tasks.is_empty() {
        ctx.can_reconnect = false;
    }
    let ctx = &*ctx;
    // One worker per task, at most `thr_n` running at a time, synchronised
    // through an unbuffered channel like the Go goroutines.
    thread::scope(|s| {
        let (tx, rx) = mpsc::sync_channel::<bool>(0);
        let mut n_threads = 0usize;
        for task in &tasks {
            let tx = tx.clone();
            s.spawn(move || {
                process_task(ctx, task);
                let _ = tx.send(true);
            });
            n_threads += 1;
            if n_threads >= thr_n {
                let _ = rx.recv();
                n_threads -= 1;
            }
        }
        while n_threads > 0 {
            let _ = rx.recv();
            n_threads -= 1;
        }
    });
}

/// Add the SHA1s of the arguments to `hide/hide.csv`.
fn hide_data(ctx: &Ctx, args: &[String]) {
    let mut sha_map = string::get_hidden(ctx, consts::HIDE_CFG_FILE);
    let mut added = false;
    for argo in args {
        let arg = argo.trim();
        let sha = string::sha1_hex(arg);
        if sha_map.contains_key(&sha) {
            printf!("Skipping '{}', SHA1 '{}' - already added\n", arg, sha);
            continue;
        }
        sha_map.insert(sha, String::new());
        added = true;
    }
    if !added {
        return;
    }
    // `os.Create` — always the file relative to the current directory, even
    // when the SHA1s were read from the `GHA2DB_DATADIR` copy.
    let file = match std::fs::File::create(consts::HIDE_CFG_FILE) {
        Ok(f) => f,
        Err(e) => fatal_on_error(io::FileError {
            op: "open",
            path: consts::HIDE_CFG_FILE.to_string(),
            source: e,
        }),
    };
    let mut writer = gocsv::Writer::new(file);
    fatal_on_err(writer.write(&["sha1"]));
    for sha in sha_map.keys() {
        fatal_on_err(writer.write(&[sha.as_str()]));
    }
    // `defer writer.Flush()` — errors ignored.
    let _ = writer.flush();
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        process_hidden(&mut ctx);
    } else {
        hide_data(&ctx, &args[1..]);
    }
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_match_the_go_table() {
        // 39 entries, `gha_issues.dup_actor_login` listed twice like in Go.
        assert_eq!(REPLACES.len(), 39);
        let dup = REPLACES
            .iter()
            .filter(|r| r.table == "gha_issues" && r.column == "dup_actor_login")
            .count();
        assert_eq!(dup, 2);
        assert_eq!(REPLACES[0].table, "gha_actors");
        assert_eq!(REPLACES[0].column, "login");
        assert_eq!(REPLACES[38].table, "gha_issues_events_labels");
        assert_eq!(REPLACES[38].column, "actor_login");
    }

    #[test]
    fn update_statement_shape() {
        let r = &REPLACES[2];
        let q = format!(
            "update {} set {} = {} where encode(digest({}, 'sha1'), 'hex') = {}",
            r.table,
            r.column,
            pg::api::n_value(1),
            r.column,
            pg::api::n_value(2),
        );
        assert_eq!(
            q,
            "update gha_actors_emails set email = $1 where encode(digest(email, 'sha1'), 'hex') = $2"
        );
    }
}
