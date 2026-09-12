//! `get_repos` — clone or pull all repositories of the enabled DevStats
//! projects, map new commits to the files they touch (`gha_commits_files`)
//! and their LOC stats (`gha_commits.loc_added/loc_removed/files_changed`),
//! backfill PushEvent commits and restore orphan commits straight from git.
//!
//! Rust port of `cmd/get_repos/get_repos.go` (+ `fetch_commits.go`, see the
//! [`fetch_commits`] module). Behaviour, messages and exit codes follow the
//! Go program; where Go iterates its maps in random order this port iterates
//! in sorted order (documented deviation).

mod fetch_commits;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Local, TimeZone, Utc};

use devstatscode::consts::LOCAL_GIT_SCRIPTS;
use devstatscode::error::{self, go_io_error_string};
use devstatscode::pg::api::{exec_sql_tx_with_err, exec_sql_with_err, insert_ignore, n_values};
use devstatscode::pg::{pg_conn_db_shared, PgConn, SqlArg};
use devstatscode::projects::{is_project_disabled, AllProjects};
use devstatscode::time::{format_go_duration, progress_info};
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    exec, fatal_on_err, fatal_on_error, fatalf, gofmt, io, printf, signal, string, threads, Ctx,
};

/// Go `dbCommits`: all commits (and their repos) still to process for one
/// project database (connection).
struct DbCommits {
    shas: Vec<String>,
    repos: Vec<String>,
    con: PgConn,
    files_skip_pattern: String,
}

/// Go `%+v` of a `map[string]map[string]struct{}` (keys sorted, as `fmt`
/// prints maps): `map[org:map[org/repo:{} …] …]`.
fn nested_set_map_string(m: &BTreeMap<String, BTreeSet<String>>) -> String {
    let parts: Vec<String> = m
        .iter()
        .map(|(k, v)| {
            let inner: Vec<String> = v.iter().map(|r| format!("{r}:{{}}")).collect();
            format!("{k}:map[{}]", inner.join(" "))
        })
        .collect();
    format!("map[{}]", parts.join(" "))
}

/// Go `dirExists`: does `path` exist and is it a directory? `Ok(false)` when
/// it does not exist, an error when it exists but is not a directory (or
/// `stat` fails for another reason).
fn dir_exists(path: &str) -> Result<bool, String> {
    let path = path.strip_suffix('/').unwrap_or(path);
    match std::fs::metadata(path) {
        Ok(meta) => {
            if meta.is_dir() {
                Ok(true)
            } else {
                Err(format!("{path}: exists, but is not a directory"))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!("stat {path}: {}", go_io_error_string(&e))),
    }
}

/// Go `os.Mkdir(path, 0755)` with a `*PathError` style message.
fn mkdir(path: &str) -> Result<(), String> {
    std::fs::create_dir(path).map_err(|e| format!("mkdir {path}: {}", go_io_error_string(&e)))
}

/// Go `getRepos`: returns `PDB -> files skip pattern` for all enabled (and,
/// when `GHA2DB_PROJECTS_COMMITS` is set, selected) projects, `org -> set of
/// org/repo` for all of them and `PDB -> set of org/repo`.
#[allow(clippy::type_complexity)]
fn get_repos(
    ctx: &mut Ctx,
) -> (
    BTreeMap<String, String>,
    BTreeMap<String, BTreeSet<String>>,
    BTreeMap<String, BTreeSet<String>>,
) {
    // Process all projects, or restrict from environment variable?
    let mut only_projects: BTreeSet<String> = BTreeSet::new();
    let mut selected_projects = false;
    if !ctx.projects_commits.is_empty() {
        selected_projects = true;
        for proj in ctx.projects_commits.split(',') {
            only_projects.insert(proj.trim().to_string());
        }
    }

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read defined projects
    let data = fatal_on_err(io::read_file(
        ctx,
        &format!("{data_prefix}{}", ctx.projects_yaml),
    ));
    let projects: AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };
    let mut dbs: BTreeMap<String, String> = BTreeMap::new();
    for (name, proj) in &projects.projects {
        if is_project_disabled(ctx, name, proj.disabled)
            || (selected_projects && !only_projects.contains(name))
        {
            continue;
        }
        dbs.insert(proj.pdb.clone(), proj.files_skip_pattern.clone());
    }

    let mut all_repos: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut db_repos: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for db in dbs.keys() {
        // Connect to Postgres `db` database.
        ctx.can_reconnect = false;
        let con = pg_conn_db_shared(ctx, db);

        // Get list of orgs in a given database
        let mut rows = fatal_on_err(con.query(
            "select distinct name from gha_repos where name like '%_/_%' and name not like '%/%/%'",
            &[],
        ));
        let mut repos: Vec<String> = Vec::new();
        while rows.next() {
            let mut repo = String::new();
            fatal_on_err(rows.scan(&mut [&mut repo]));
            repos.push(repo);
        }
        fatal_on_err(rows.err());
        fatal_on_err(rows.close());

        // Create map of distinct "org" --> list of repos
        for repo in repos {
            let ary: Vec<&str> = repo.split('/').collect();
            if ary.len() != 2 {
                printf!("{db}: invalid repo name: {repo}");
                continue;
            }
            let org = ary[0].to_string();
            all_repos.entry(org).or_default().insert(repo.clone());
            db_repos.entry(db.clone()).or_default().insert(repo);
        }
        con.close();
    }

    // return final map
    (dbs, all_repos, db_repos)
}

/// Go `processRepo`: clone (when the working directory is missing) or
/// reset+pull a single repository; returns `org/repo` on success and an
/// empty string on failure.
fn process_repo(ctx: &Ctx, org_repo: &str, rwd: &str) -> String {
    // Local or cron mode?
    let cmd_prefix = if ctx.local_cmd { LOCAL_GIT_SCRIPTS } else { "" };
    let env: BTreeMap<String, String> =
        BTreeMap::from([("GIT_TERMINAL_PROMPT".to_string(), "0".to_string())]);

    // Clone or reset+pull repo
    let exists = fatal_on_err(dir_exists(rwd));
    if !exists {
        // We need to clone repo
        if ctx.debug > 0 {
            printf!("Cloning {org_repo}\n");
        }
        let dt_start = Instant::now();
        // Clone repo into given directory (from command line)
        // We cannot chdir because this is a multithreaded app
        // And all threads share CWD (current working directory)
        let res = exec::exec_command(
            ctx,
            &[
                "git".to_string(),
                "clone".to_string(),
                format!("https://github.com/{org_repo}.git"),
                rwd.to_string(),
            ],
            &env,
        );
        let took = format_go_duration(dt_start.elapsed());
        if let Err(err) = res {
            if ctx.debug > 0 {
                printf!("Warning git-clone failed: {org_repo} (took {took}): {err}\n");
            }
            eprintln!("Warning git-clone failed: {org_repo} (took {took}): {err}");
            return String::new();
        }
        if ctx.debug > 0 {
            printf!("Cloned {org_repo}: took {took}\n");
        }
    } else {
        // We *may* need to pull repo
        if ctx.debug > 0 {
            printf!("Pulling {org_repo}\n");
        }
        let dt_start = Instant::now();
        // Update repo using shell script that uses 'chdir'
        // We cannot chdir because this is a multithreaded app
        // And all threads share CWD (current working directory)
        let res = exec::exec_command(
            ctx,
            &[format!("{cmd_prefix}git_reset_pull.sh"), rwd.to_string()],
            &env,
        );
        let took = format_go_duration(dt_start.elapsed());
        if let Err(err) = res {
            if ctx.debug > 0 {
                printf!("Warning git_reset_pull.sh failed: {org_repo} (took {took}): {err}\n");
            }
            eprintln!("Warning git_reset_pull.sh failed: {org_repo} (took {took}): {err}");
            return String::new();
        }
        if ctx.debug > 0 {
            printf!("Pulled {org_repo}: took {took}\n");
        }
    }
    org_repo.to_string()
}

/// Go `processRepos`: clone or pull every repository (up to `thrN` at a
/// time); with `GHA2DB_EXTERNAL_INFO` also print the cncf/gitdm helper data.
fn process_repos(ctx: &mut Ctx, all_repos: &BTreeMap<String, BTreeSet<String>>) {
    // Set non-fatal exec mode, we want to run sync for next project(s) if current fails
    // Also set quite mode, many git-pulls or git-clones can fail and this is not needed to log it to DB
    // User can set higher debug level and run manually to debug this
    ctx.exec_fatal = false;
    ctx.exec_quiet = true;

    // Go to main repos directory
    let wd = ctx.repos_dir.clone();
    let exists = fatal_on_err(dir_exists(&wd));
    if !exists {
        // Try to Mkdir it if not exists
        fatal_on_err(mkdir(&wd));
        let exists = fatal_on_err(dir_exists(&wd));
        if !exists {
            fatalf!("failed to create directory: {wd}");
        }
    }

    // Process all orgs & repos
    let thr_n = threads::get_threads_num(ctx);
    let ctx: &Ctx = ctx;
    let mut all_ok_repos: Vec<String> = Vec::new();
    // Count all data
    let mut checked = 0usize;
    let all_n: usize = all_repos.values().map(|repos| repos.len()).sum();
    let dt_start = Utc::now();
    let mut last_time = dt_start;
    let period = Duration::from_secs(10);
    // Process each repo only once
    let mut seen: BTreeSet<String> = BTreeSet::new();
    thread::scope(|s| {
        let (tx, rx) = mpsc::channel::<String>();
        let mut n_threads = 0usize;
        // Iterate orgs
        for (org, repos) in all_repos {
            // Go to current 'org' subdirectory
            let owd = format!("{wd}{org}");
            let exists = fatal_on_err(dir_exists(&owd));
            if !exists {
                // Try to Mkdir it if not exists
                fatal_on_err(mkdir(&owd));
                let exists = fatal_on_err(dir_exists(&owd));
                if !exists {
                    fatalf!("failed to create directory: {owd}");
                }
            }
            // Iterate org's repositories
            for org_repo in repos {
                // Check if we already processed that repo
                if !seen.insert(org_repo.clone()) {
                    continue;
                }
                // repository's working dir (if present we only need to do git reset --hard; git pull)
                let repo = org_repo.split('/').nth(1).unwrap_or("");
                let rwd = format!("{owd}/{repo}");
                let tx = tx.clone();
                let org_repo_owned = org_repo.clone();
                s.spawn(move || {
                    let _ = tx.send(process_repo(ctx, &org_repo_owned, &rwd));
                });
                n_threads += 1;
                if n_threads >= thr_n {
                    let res = rx.recv().unwrap_or_default();
                    n_threads -= 1;
                    if !res.is_empty() {
                        all_ok_repos.push(res);
                    }
                    checked += 1;
                    progress_info(checked, all_n, dt_start, &mut last_time, period, org_repo);
                }
            }
        }
        while n_threads > 0 {
            let res = rx.recv().unwrap_or_default();
            n_threads -= 1;
            if !res.is_empty() {
                all_ok_repos.push(res);
            }
            checked += 1;
            progress_info(
                checked,
                all_n,
                dt_start,
                &mut last_time,
                period,
                "final join...",
            );
        }
    });

    // Output all repos as ruby object & Final cncf/gitdm command to generate concatenated git.log
    // Only output when GHA2DB_EXTERNAL_INFO env variable is set
    // Only output to stdout - not standard logs via lib.Printf(...)
    if ctx.external_info {
        // Sort list of repos and made them unique
        all_ok_repos = string::make_unique_sort(&all_ok_repos);

        // Create Ruby-like string with all repos array
        let mut all_ok_repos_str = String::from("[\n");
        for ok_repo in &all_ok_repos {
            all_ok_repos_str.push_str(&format!("  '{ok_repo}',\n"));
        }
        all_ok_repos_str.push(']');

        // Create list of orgs, sorted and unique
        let orgs: Vec<String> = all_repos.keys().cloned().collect();
        let orgs = string::make_unique_sort(&orgs);

        // Output shell command sorted
        let mut final_cmd = String::from("./all_repos_log.sh ");
        for org in &orgs {
            final_cmd.push_str(&format!("{}{org}/* \\\n", ctx.repos_dir));
        }
        let final_cmd = final_cmd
            .strip_suffix(" \\\n")
            .unwrap_or(&final_cmd)
            .to_string();

        // Output cncf/gitdm related data
        print!("AllRepos:\n{all_ok_repos_str}\n");
        print!("Final command:\n{final_cmd}\n");
    }
    printf!(
        "Successfully processed {}/{} repos\n",
        all_ok_repos.len(),
        checked
    );
}

/// Shared body of Go `processCommitsDB` / `processCommitsLOC`: list the
/// unprocessed commits of database `db` using `query`.
fn list_db_commits(
    ctx: &Ctx,
    db: &str,
    files_skip_pattern: &str,
    query: &str,
    boc: bool,
) -> DbCommits {
    // Get list of unprocessed commits for current DB
    if boc {
        printf!("BOC stats running on database: {db}\n");
    } else {
        printf!("Running on database: {db}\n");
    }
    let dt_start = Instant::now();
    // Connect to Postgres `db` database.
    let con = pg_conn_db_shared(ctx, db);

    let mut shas: Vec<String> = Vec::new();
    let mut repos: Vec<String> = Vec::new();
    let mut rows = fatal_on_err(con.query(query, &[]));
    while rows.next() {
        let mut sha = String::new();
        let mut repo = String::new();
        fatal_on_err(rows.scan(&mut [&mut sha, &mut repo]));
        shas.push(sha);
        repos.push(repo);
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    let took = format_go_duration(dt_start.elapsed());
    if boc {
        printf!(
            "BOC stats database '{db}' processed took {took}, new commits: {}\n",
            shas.len()
        );
    } else {
        printf!(
            "Database '{db}' processed took {took}, new commits: {}\n",
            shas.len()
        );
    }
    DbCommits {
        shas,
        repos,
        con,
        files_skip_pattern: files_skip_pattern.to_string(),
    }
}

/// Go `time.Now()` passed to lib/pq: the local wall clock time.
fn now_arg() -> SqlArg {
    SqlArg::from(Local::now())
}

/// Go `getCommitLOC`: run `git_loc.sh` for one commit and store its LOC stats
/// in `gha_commits`. Returns 1 (stats stored), 0 (no stats / no row updated)
/// or -1 (script failed).
fn get_commit_loc(ctx: &Ctx, con: &PgConn, repo: &str, sha: &str) -> i32 {
    // Local or cron mode?
    let cmd_prefix = if ctx.local_cmd { LOCAL_GIT_SCRIPTS } else { "" };

    // Get LOC stats using shell script that does 'chdir'
    // We cannot chdir because this is a multithreaded app
    // And all threads share CWD (current working directory)
    if ctx.debug > 1 {
        printf!("Getting BOC stats for commit {repo}:{sha}\n");
    }
    let dt_start = Instant::now();
    let rwd = format!("{}{repo}", ctx.repos_dir);
    let env: BTreeMap<String, String> =
        BTreeMap::from([("GIT_TERMINAL_PROMPT".to_string(), "0".to_string())]);
    let res = exec::exec_command(
        ctx,
        &[format!("{cmd_prefix}git_loc.sh"), rwd, sha.to_string()],
        &env,
    );
    let took = format_go_duration(dt_start.elapsed());
    let loc_str = match res {
        Ok(out) => out,
        Err(err) => {
            if ctx.debug > 0 {
                printf!("Warning git_loc.sh failed: {repo}:{sha} (took {took}): {err}\n");
                eprintln!("Warning git_loc.sh failed: {repo}:{sha} (took {took}): {err}");
            }
            exec_sql_with_err(
                con,
                ctx,
                &insert_ignore(&format!(
                    "into gha_skip_commits(sha, dt, reason) {}",
                    n_values(3)
                )),
                &[SqlArg::from(sha), now_arg(), SqlArg::from(2i64)],
            );
            return -1;
        }
    };
    let mut changed = 0i64;
    let mut added = 0i64;
    let mut removed = 0i64;
    let mut status = 0;
    for part in loc_str.trim().split(',') {
        let part = part.trim();
        if let Some(v) = gofmt::sscanf_int_prefix(part, " file changed") {
            changed = v;
            status = 1;
            continue;
        }
        if let Some(v) = gofmt::sscanf_int_prefix(part, " files changed") {
            changed = v;
            status = 1;
            continue;
        }
        if let Some(v) = gofmt::sscanf_int_prefix(part, " insertion(+)") {
            added = v;
            status = 1;
            continue;
        }
        if let Some(v) = gofmt::sscanf_int_prefix(part, " insertions(+)") {
            added = v;
            status = 1;
            continue;
        }
        if let Some(v) = gofmt::sscanf_int_prefix(part, " deletion(-)") {
            removed = v;
            status = 1;
            continue;
        }
        if let Some(v) = gofmt::sscanf_int_prefix(part, " deletions(-)") {
            removed = v;
            status = 1;
            continue;
        }
    }
    let res = exec_sql_with_err(
        con,
        ctx,
        "update gha_commits set loc_added = $1, loc_removed = $2, files_changed = $3 where sha = $4 and dup_repo_name = $5",
        &[
            SqlArg::from(added),
            SqlArg::from(removed),
            SqlArg::from(changed),
            SqlArg::from(sha),
            SqlArg::from(repo),
        ],
    );
    let rows = fatal_on_err(res.rows_affected());
    if rows == 0 {
        if ctx.debug > 0 {
            printf!("No rows updated for SHA {sha}\n");
        }
        exec_sql_with_err(
            con,
            ctx,
            &insert_ignore(&format!(
                "into gha_skip_commits(sha, dt, reason) {}",
                n_values(3)
            )),
            &[SqlArg::from(sha), now_arg(), SqlArg::from(2i64)],
        );
        status = 0;
    }
    status
}

/// Go `getCommitFiles`: run `git_files.sh` for one commit and store the files
/// it touches in `gha_commits_files`. Returns 1 (files stored), 0 (commit
/// without files) or -1 (script failed).
fn get_commit_files(
    ctx: &Ctx,
    con: &PgConn,
    files_skip_pattern: Option<&regex::Regex>,
    repo: &str,
    sha: &str,
) -> i32 {
    // Local or cron mode?
    let cmd_prefix = if ctx.local_cmd { LOCAL_GIT_SCRIPTS } else { "" };

    // Get files using shell script that does 'chdir'
    // We cannot chdir because this is a multithreaded app
    // And all threads share CWD (current working directory)
    if ctx.debug > 1 {
        printf!("Getting files for commit {repo}:{sha}\n");
    }
    let dt_start = Instant::now();
    let rwd = format!("{}{repo}", ctx.repos_dir);
    let env: BTreeMap<String, String> =
        BTreeMap::from([("GIT_TERMINAL_PROMPT".to_string(), "0".to_string())]);
    let res = exec::exec_command(
        ctx,
        &[format!("{cmd_prefix}git_files.sh"), rwd, sha.to_string()],
        &env,
    );
    let took = format_go_duration(dt_start.elapsed());
    let files_str = match res {
        Ok(out) => out,
        Err(err) => {
            if ctx.debug > 1 {
                printf!("Warning git_files.sh failed: {repo}:{sha} (took {took}): {err}\n");
                eprintln!("Warning git_files.sh failed: {repo}:{sha} (took {took}): {err}");
            }
            exec_sql_with_err(
                con,
                ctx,
                &insert_ignore(&format!(
                    "into gha_skip_commits(sha, dt, reason) {}",
                    n_values(3)
                )),
                &[SqlArg::from(sha), now_arg(), SqlArg::from(1i64)],
            );
            return -1;
        }
    };
    let files: Vec<&str> = files_str.split('\n').collect();
    let mut n_files = 0i64;
    // Go's zero `time.Time`
    let mut commit_date = Utc
        .with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .map(SqlArg::from)
        .unwrap_or_else(|| SqlArg::from(Utc::now()));

    // Insert files in transaction: all or none
    let mut tx = fatal_on_err(con.begin());
    for (i, data) in files.iter().enumerate() {
        if i == 0 {
            if data.is_empty() {
                if ctx.debug > 0 {
                    printf!("Empty time returned for repo: {repo}, sha: {sha}\n");
                }
                continue;
            }
            let Ok(unix_time_stamp) = data.parse::<i64>() else {
                printf!("Invalid time returned for repo: {repo}, sha: {sha}: '{data}'\n");
                break;
            };
            if let Some(dt) = Local.timestamp_opt(unix_time_stamp, 0).single() {
                commit_date = SqlArg::from(dt);
            }
            continue;
        }
        let file_data = data.trim();
        if file_data.is_empty() {
            continue;
        }
        // Use '♂♀' separator to avoid any character that can appear inside file name
        let file_data_ary: Vec<&str> = file_data.split("♂♀").collect();
        if file_data_ary.len() != 2 {
            fatalf!("invalid fileData returned for repo: {repo}, sha: {sha}: '{file_data}'");
        }
        let file_name = file_data_ary[0];
        // If file matches exclude pattern, skip it
        if file_name.is_empty() || files_skip_pattern.is_some_and(|re| re.is_match(file_name)) {
            continue;
        }
        // fileSize can be:
        // > 0 - normal file size
        // 0 - file created - no contenets
        // -1 - file referenced in the commit SHA but not found in this commit (means deleted)
        // -2 - file size returned as "-" from git ls-tree - means some special file, directory
        let file_size: i64 = file_data_ary[1].parse().unwrap_or(-2);
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &insert_ignore(
                "into gha_commits_files(sha, dt, path, size, ext) \
                 values($1, $2, $3, $4, regexp_replace(lower($3), '^.*\\.', ''))",
            ),
            &[
                SqlArg::from(sha),
                commit_date.clone(),
                SqlArg::from(file_name),
                SqlArg::from(file_size),
            ],
        );
        n_files += 1;
    }
    // Some commits have no files (for example only renames)
    // Mark them as skipped not to process again
    if n_files == 0 {
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &insert_ignore(&format!(
                "into gha_skip_commits(sha, dt, reason) {}",
                n_values(3)
            )),
            &[SqlArg::from(sha), now_arg(), SqlArg::from(1i64)],
        );
        // Commit transaction
        fatal_on_err(tx.commit());
        return 0;
    }
    // Commit transaction
    fatal_on_err(tx.commit());
    if ctx.debug > 1 {
        printf!("Got {repo}:{sha} commit: {n_files} files: took {took}\n");
    }
    1
}

/// Go `postprocessCommitsDB`: run `query` on the database and close the
/// connection.
fn postprocess_commits_db(con: &PgConn, query: &str) {
    let rows = fatal_on_err(con.query(query, &[]));
    drop(rows);
    // Close connection
    con.close();
}

/// Collect the unprocessed commits of all databases, up to `thr_n` databases
/// at a time (Go's `processCommitsDB`/`processCommitsLOC` goroutines).
fn collect_db_commits(
    ctx: &Ctx,
    dbs: &BTreeMap<String, String>,
    query: &str,
    thr_n: usize,
    boc: bool,
) -> Vec<DbCommits> {
    let mut all_commits: Vec<DbCommits> = Vec::new();
    thread::scope(|s| {
        let (tx, rx) = mpsc::channel::<DbCommits>();
        let mut n_threads = 0usize;
        for (db, files_skip_pattern) in dbs {
            let tx = tx.clone();
            s.spawn(move || {
                let _ = tx.send(list_db_commits(ctx, db, files_skip_pattern, query, boc));
            });
            n_threads += 1;
            if n_threads >= thr_n {
                if let Ok(commits) = rx.recv() {
                    all_commits.push(commits);
                }
                n_threads -= 1;
            }
        }
        while n_threads > 0 {
            if let Ok(commits) = rx.recv() {
                all_commits.push(commits);
            }
            n_threads -= 1;
        }
    });
    all_commits
}

/// Go `processCommits`: for every database create the commit → files mapping
/// (`GHA2DB_SKIP_COMMITS_FILES` unset) and the commit LOC stats
/// (`GHA2DB_SKIP_COMMITS_LOC` unset), up to `thrN` commits at a time.
fn process_commits(ctx: &mut Ctx, dbs: &BTreeMap<String, String>) {
    // Read SQL to get commits to sync from 'util_sql/list_unprocessed_commits_files.sql' file.
    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    let period = Duration::from_secs(10);

    if ctx.commits_files_stats_enabled {
        let bytes = fatal_on_err(io::read_file(
            ctx,
            &format!("{data_prefix}util_sql/list_unprocessed_commits_files.sql"),
        ));
        let sql_query = String::from_utf8_lossy(&bytes).to_string();

        // Process all DBs in a separate threads to get all commits
        let dt_start = Instant::now();
        let thr_n = threads::get_threads_num(ctx);
        ctx.can_reconnect = false;
        let all_commits = collect_db_commits(ctx, dbs, &sql_query, thr_n, false);
        printf!(
            "Got {} DBs new commits list: took {}\n",
            all_commits.len(),
            format_go_duration(dt_start.elapsed())
        );

        // Set non-fatal exec mode, we want to run sync for next project(s) if current fails
        // Also set quite mode, many git-pulls or git-clones can fail and this is not needed to log it to DB
        // User can set higher debug level and run manually to debug this
        // Also set capture command's stdout mode
        ctx.exec_fatal = false;
        ctx.exec_quiet = true;
        ctx.exec_output = true;
        let ctx: &Ctx = ctx;

        // Create final 'commits - file list' associations
        let dt_start = Utc::now();
        let mut last_time = dt_start;
        // statuses:
        // -1: error
        // 0: commit without files
        // 1: commit with files
        let mut statuses: BTreeMap<i32, i64> = BTreeMap::from([(-1, 0), (0, 0), (1, 0)]);
        let all_n: usize = all_commits.iter().map(|c| c.shas.len()).sum();
        let mut checked = 0usize;
        // process all commits
        let regexps: Vec<Option<regex::Regex>> = all_commits
            .iter()
            .map(|commits| {
                if commits.files_skip_pattern.is_empty() {
                    None
                } else {
                    match devstatscode::goregex::compile(&commits.files_skip_pattern) {
                        Ok(re) => Some(re),
                        Err(e) => {
                            panic!("regexp: Compile(`{}`): {}", commits.files_skip_pattern, e)
                        }
                    }
                }
            })
            .collect();
        thread::scope(|s| {
            let (tx, rx) = mpsc::channel::<i32>();
            let mut n_threads = 0usize;
            for (commits, re) in all_commits.iter().zip(regexps.iter()) {
                let con = &commits.con;
                for (i, sha) in commits.shas.iter().enumerate() {
                    let repo = &commits.repos[i];
                    let tx = tx.clone();
                    let re = re.as_ref();
                    s.spawn(move || {
                        let _ = tx.send(get_commit_files(ctx, con, re, repo, sha));
                    });
                    n_threads += 1;
                    if n_threads >= thr_n {
                        *statuses.entry(rx.recv().unwrap_or(-1)).or_insert(0) += 1;
                        n_threads -= 1;
                        checked += 1;
                        progress_info(checked, all_n, dt_start, &mut last_time, period, repo);
                    }
                }
            }
            while n_threads > 0 {
                *statuses.entry(rx.recv().unwrap_or(-1)).or_insert(0) += 1;
                n_threads -= 1;
                checked += 1;
                progress_info(
                    checked,
                    all_n,
                    dt_start,
                    &mut last_time,
                    period,
                    "final join...",
                );
            }
        });
        let took = Utc::now() - dt_start;
        let all = statuses[&-1] + statuses[&0] + statuses[&1];
        let perc = if all > 0 {
            statuses[&1] as f64 * 100.0 / all as f64
        } else {
            0.0
        };
        printf!(
            "Got {} ({:.2}%) new commit's files, {} without files, {} failed, all {}, took {}\n",
            statuses[&1],
            perc,
            statuses[&0],
            statuses[&-1],
            all,
            format_go_duration(took.to_std().unwrap_or_default())
        );

        // Post execute SQL 'util_sql/create_events_commits.sql' on each database
        // This SQL updates 'gha_events_commits_files' table that
        // holds connections between commits SHA and events that refer to it
        // So we can query for files modified in the given events (via commits)
        let dt_start = Instant::now();
        let bytes = fatal_on_err(io::read_file(
            ctx,
            &format!("{data_prefix}util_sql/create_events_commits.sql"),
        ));
        let sql_query = String::from_utf8_lossy(&bytes).to_string();
        thread::scope(|s| {
            let (tx, rx) = mpsc::channel::<i32>();
            let mut n_threads = 0usize;
            for commits in &all_commits {
                let con = &commits.con;
                let tx = tx.clone();
                let sql_query = &sql_query;
                s.spawn(move || {
                    postprocess_commits_db(con, sql_query);
                    let _ = tx.send(1);
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
        printf!(
            "Postprocessed all new commits, took {}\n",
            format_go_duration(dt_start.elapsed())
        );
    }

    if !ctx.commits_loc_stats_enabled {
        return;
    }

    // Commits LOC analysis (lines of code added, removed and changed files counts)
    let bytes = fatal_on_err(io::read_file(
        ctx,
        &format!("{data_prefix}util_sql/list_unprocessed_commits_loc.sql"),
    ));
    let sql_query = String::from_utf8_lossy(&bytes).to_string();

    // Process all DBs in a separate threads to get all commits LOC stats
    let dt_start = Instant::now();
    let thr_n = threads::get_threads_num(ctx);
    ctx.can_reconnect = false;
    let ctx: &Ctx = ctx;
    let all_commits = collect_db_commits(ctx, dbs, &sql_query, thr_n, true);
    printf!(
        "Got {} DBs new commits BOC stats: took {}\n",
        all_commits.len(),
        format_go_duration(dt_start.elapsed())
    );

    // Create final commits LOC stats
    let dt_start = Utc::now();
    let mut last_time = dt_start;
    // statuses:
    // -1: error
    // 0: commit without LOC stats
    // 1: commit with LOC stats
    let mut statuses: BTreeMap<i32, i64> = BTreeMap::from([(-1, 0), (0, 0), (1, 0)]);
    let all_n: usize = all_commits.iter().map(|c| c.shas.len()).sum();
    let mut checked = 0usize;
    // process all commits
    thread::scope(|s| {
        let (tx, rx) = mpsc::channel::<i32>();
        let mut n_threads = 0usize;
        for commits in &all_commits {
            let con = &commits.con;
            for (i, sha) in commits.shas.iter().enumerate() {
                let repo = &commits.repos[i];
                let tx = tx.clone();
                s.spawn(move || {
                    let _ = tx.send(get_commit_loc(ctx, con, repo, sha));
                });
                n_threads += 1;
                if n_threads >= thr_n {
                    *statuses.entry(rx.recv().unwrap_or(-1)).or_insert(0) += 1;
                    n_threads -= 1;
                    checked += 1;
                    progress_info(checked, all_n, dt_start, &mut last_time, period, repo);
                }
            }
        }
        while n_threads > 0 {
            *statuses.entry(rx.recv().unwrap_or(-1)).or_insert(0) += 1;
            n_threads -= 1;
            checked += 1;
            progress_info(
                checked,
                all_n,
                dt_start,
                &mut last_time,
                period,
                "final join...",
            );
        }
    });
    let took = Utc::now() - dt_start;
    let all = statuses[&-1] + statuses[&0] + statuses[&1];
    let perc = if all > 0 {
        statuses[&1] as f64 * 100.0 / all as f64
    } else {
        0.0
    };
    printf!(
        "Got {} ({:.2}%) new commit's BOC stats, {} without stats, {} failed, all {}, took {}\n",
        statuses[&1],
        perc,
        statuses[&0],
        statuses[&-1],
        all,
        format_go_duration(took.to_std().unwrap_or_default())
    );
    for commits in &all_commits {
        // Close connection
        commits.con.close();
    }
}

fn main() {
    error::exit_on_panic();
    let dt_start = Instant::now();
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    if !ctx.skip_get_repos {
        let (dbs, repos, repo_dbs) = get_repos(&mut ctx);
        if ctx.debug > 0 {
            printf!("dbs: {}\n", gofmt::map(&dbs));
            printf!("repos: {}\n", nested_set_map_string(&repos));
            printf!("repoDBs: {}\n", nested_set_map_string(&repo_dbs));
        }
        if dbs.is_empty() {
            fatalf!("No databases to process");
        }
        if repos.is_empty() {
            fatalf!("No repos to process");
        }
        if ctx.process_repos {
            process_repos(&mut ctx, &repos);
        }
        if ctx.fetch_commits_mode != 0 {
            fetch_commits::backfill_push_event_commits(&mut ctx, &dbs, &repo_dbs);
        }
        if ctx.restore_orphan_commits {
            fetch_commits::restore_orphan_commits(&mut ctx, &dbs, &repo_dbs);
        }
        if ctx.process_commits {
            process_commits(&mut ctx, &dbs);
        }
    }
    printf!(
        "All repos processed in: {}\n",
        format_go_duration(dt_start.elapsed())
    );
}
