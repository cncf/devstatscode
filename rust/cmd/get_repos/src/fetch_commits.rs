//! Port of `cmd/get_repos/fetch_commits.go`: backfill `gha_commits` (and
//! `gha_commits_roles`) for PushEvent payloads from the local git clones
//! (`GHA2DB_FETCH_COMMITS_MODE`) and restore commits present in git but
//! missing from the database (`GHA2DB_RESTORE_ORPHAN_COMMITS`).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::{mpsc, Mutex};
use std::thread;
use std::time::Instant;

use chrono::{DateTime, FixedOffset, TimeZone, Utc};

use devstatscode::consts::{HIDE_CFG_FILE, LOCAL_GIT_SCRIPTS};
use devstatscode::error::go_io_error_string;
use devstatscode::pg::api::trunc_to_bytes;
use devstatscode::pg::value::{go_quote, go_time_string};
use devstatscode::pg::{pg_conn_db_shared, PgConn, PgError, PgTx, SqlArg};
use devstatscode::string::{get_hidden, MaybeHide};
use devstatscode::time::{format_go_duration, get_date_ago, to_ymd_date};
use devstatscode::{exec, gobase64, gofmt, hash, printf, restore, threads, trailers, Ctx};

const ZERO_SHA40: &str = "0000000000000000000000000000000000000000";

const INS_COMMIT_ROLE_SQL: &str = "
insert into gha_commits_roles(
  sha, event_id, role, actor_id, actor_login, actor_name, actor_email, dup_repo_id, dup_repo_name, dup_created_at
) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
on conflict do nothing
";

// InsertCommitterRole / InsertAuthorRole - we don't add those roles
const INSERT_COMMITTER_ROLE: bool = false;
const INSERT_AUTHOR_ROLE: bool = false;

/// LegacyUnsafeBackfill controls how aggressively we backfill commits for PushEvents.
///
/// When false (default): ONLY backfill events that are "100% deterministic" from payload:
///   - head and before must both be present and be valid non-zero 40-hex SHAs.
///   - no guessing for before==0/empty/null
///   - no fallback to inserting head-only on git range errors
///
/// When true: keep legacy/best-effort behavior (process before==0/empty/null, missing size, etc),
/// while still avoiding the known footgun: before==0 with size that looks like a cap/truncation marker.
const LEGACY_UNSAFE_BACKFILL: bool = false;

/// LegacyCappedSizes - historical "cap/truncation marker" sizes observed in GHA payloads.
/// Using these as exact commit counts for before==0 caused huge over-backfills (10k explosions).
const LEGACY_CAPPED_SIZES: [i64; 2] = [1000, 10000];

/// AllowNonFastForwardPushes - default=true. Ignored when LegacyUnsafeBackfill=true.
const ALLOW_NON_FAST_FORWARD_PUSHES: bool = true;

/// Go `commitInfo`: commit metadata extracted from local git history.
/// git/git_commits.sh output provides: sha,b64(author_name),b64(author_email),b64(committer_name),b64(committer_email),b64(message);
#[derive(Clone, Debug, Default)]
struct CommitInfo {
    sha: String,
    author_name: String,
    author_email: String,
    committer_name: String,
    committer_email: String,
    message: String,
}

impl fmt::Display for CommitInfo {
    /// Go `%+v` of a `commitInfo` (the never-set `AuthorDate` prints as the zero time).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{Sha:{} AuthorName:{} AuthorEmail:{} CommitterName:{} CommitterEmail:{} Message:{} AuthorDate:0001-01-01 00:00:00 +0000 UTC}}",
            self.sha,
            self.author_name,
            self.author_email,
            self.committer_name,
            self.committer_email,
            self.message
        )
    }
}

/// Go `pushEvent`.
#[derive(Clone, Debug)]
struct PushEvent {
    event_id: i64,
    actor_id: i64,
    actor_login: String,
    repo_id: i64,
    repo_name: String,
    created_at: DateTime<FixedOffset>,
    head: String,
    before: String,
    #[allow(dead_code)]
    ref_: String,
    #[allow(dead_code)]
    push_id: Option<i64>,
    size: Option<i64>,
    #[allow(dead_code)]
    cnt: i64,
}

/// Go `actorCache`: (lower(email), lower(name)) → (actor id, login); shared
/// across the repo threads of one database.
struct ActorCache {
    m: Mutex<HashMap<(String, String), (i64, String)>>,
}

impl ActorCache {
    fn new() -> Self {
        ActorCache {
            m: Mutex::new(HashMap::new()),
        }
    }
}

/// Go `time.Time` as printed with `%s`/`%v`: either the UTC
/// `GHA2DB_STARTDT` (`+0000 UTC`) or a value read from the database
/// (`+0000 +0000`, lib/pq's nameless zone).
#[derive(Clone, Copy)]
enum GoTime {
    Utc(DateTime<Utc>),
    Db(DateTime<FixedOffset>),
}

impl GoTime {
    fn utc(&self) -> DateTime<Utc> {
        match self {
            GoTime::Utc(t) => *t,
            GoTime::Db(t) => t.with_timezone(&Utc),
        }
    }
}

impl fmt::Display for GoTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GoTime::Utc(t) => f.write_str(&gofmt::time(*t)),
            GoTime::Db(t) => f.write_str(&go_time_string(t)),
        }
    }
}

/// Go `isZeroSHA`: true for empty strings and all-zero 40-hex-like SHAs.
fn is_zero_sha(sha: &str) -> bool {
    let sha = sha.trim();
    sha.is_empty() || sha.chars().all(|c| c == '0')
}

fn normalize_sha(sha: &str) -> String {
    sha.trim().to_lowercase()
}

/// Go `isValidHexSHA40`: exactly 40 hex chars (case-insensitive).
fn is_valid_hex_sha40(sha: &str) -> bool {
    sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit())
}

/// Go `isValidNonZeroSHA40`: a 40-hex SHA that is not all-zero.
fn is_valid_non_zero_sha40(sha: &str) -> bool {
    let sha = sha.trim();
    is_valid_hex_sha40(sha) && !is_zero_sha(sha)
}

fn no_env() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// Go `gitIsAncestor`: is `ancestor` an ancestor (or equal) of `commit`?
/// Uses `git merge-base ancestor commit` and compares the result to `ancestor`.
#[allow(dead_code)]
fn git_is_ancestor(
    ctx: &Ctx,
    repo_path: &str,
    ancestor: &str,
    commit: &str,
) -> Result<bool, String> {
    let ancestor = normalize_sha(ancestor);
    let commit = normalize_sha(commit);
    if ancestor.is_empty() || commit.is_empty() {
        return Err(format!(
            "empty sha in ancestor check: ancestor={} commit={}",
            go_quote(&ancestor),
            go_quote(&commit)
        ));
    }
    let out = exec::exec_command(
        ctx,
        &[
            "git".to_string(),
            "-C".to_string(),
            repo_path.to_string(),
            "merge-base".to_string(),
            ancestor.clone(),
            commit,
        ],
        &no_env(),
    )
    .map_err(|e| e.to_string())?;
    let mut mb = normalize_sha(&out);
    if let Some(idx) = mb.find(['\n', '\t', ' ']) {
        mb.truncate(idx);
    }
    if !is_valid_non_zero_sha40(&mb) {
        return Err(format!(
            "git merge-base returned unexpected output {}",
            go_quote(&out)
        ));
    }
    Ok(mb == ancestor)
}

/// Go `isExitStatus128`: best-effort detector for "unknown revision / bad
/// object" style git failures.
fn is_exit_status_128(err: &str) -> bool {
    err.contains("exit status 128")
}

/// Go `gitFetchGitHubPullRefs`: best-effort fetch of GitHub PR refs. This can
/// recover historical commits that were only reachable via `refs/pull/*`.
fn git_fetch_github_pull_refs(ctx: &Ctx, repo_path: &str) -> Result<(), String> {
    exec::exec_command(
        ctx,
        &[
            "git".to_string(),
            "-C".to_string(),
            repo_path.to_string(),
            "fetch".to_string(),
            "origin".to_string(),
            "+refs/pull/*/head:refs/pull/*/head".to_string(),
            "+refs/pull/*/merge:refs/pull/*/merge".to_string(),
        ],
        &no_env(),
    )
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// Go `os.Stat(repoPath)` error mapping shared by `backfillRepo` and
/// `restoreOrphanRepo`.
fn stat_repo_path(db: &str, repo_path: &str) -> Result<(), String> {
    match std::fs::metadata(repo_path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Do not silently skip: user explicitly requested tracking.
            Err(format!("{db}: repo not cloned: {repo_path}"))
        }
        Err(e) => Err(format!(
            "{db}: cannot stat repo path {repo_path}: stat {repo_path}: {}",
            go_io_error_string(&e)
        )),
    }
}

/// Run `f(item)` for every item, at most `thr_n` at a time, in the given
/// order (Go's `thr` semaphore channel of goroutines).
fn for_each_repo_limited<T, F>(repos: &[T], thr_n: usize, f: F)
where
    T: Sync,
    F: Fn(&T) + Sync,
{
    let thr_n = thr_n.max(1);
    thread::scope(|s| {
        let (tok_tx, tok_rx) = mpsc::channel::<()>();
        for _ in 0..thr_n {
            let _ = tok_tx.send(());
        }
        let f = &f;
        for repo in repos {
            let _ = tok_rx.recv();
            let tok_tx = tok_tx.clone();
            s.spawn(move || {
                f(repo);
                let _ = tok_tx.send(());
            });
        }
    });
}

/// Go `backfillPushEventCommits`: reconstruct `gha_commits` (and
/// `gha_commits_roles`) for PushEvent payloads. DBs are processed
/// sequentially; repos inside a DB in parallel up to NCPUs.
pub fn backfill_push_event_commits(
    ctx: &mut Ctx,
    dbs: &BTreeMap<String, String>,
    repo_dbs: &BTreeMap<String, BTreeSet<String>>,
) {
    if ctx.fetch_commits_mode == 0 {
        return;
    }
    let dt_start = Instant::now();

    // Ensure git commands return output and don't abort the whole process from worker threads.
    let prev_exec_output = ctx.exec_output;
    let prev_exec_fatal = ctx.exec_fatal;
    let prev_exec_quiet = ctx.exec_quiet;
    ctx.exec_output = true;
    ctx.exec_fatal = false;
    ctx.exec_quiet = true;
    ctx.can_reconnect = false;

    let hide_cfg = get_hidden(ctx, HIDE_CFG_FILE);
    let maybe_hide = MaybeHide::new(hide_cfg);

    let thr_n = threads::get_threads_num(ctx);
    let ctx_ro: &Ctx = ctx;

    let mut all_commits = 0i64;
    let mut all_roles = 0i64;
    // DBs sequentially (deterministic order).
    for db in dbs.keys() {
        let Some(repos_set) = repo_dbs.get(db) else {
            continue;
        };
        if repos_set.is_empty() {
            continue;
        }
        let repos: Vec<String> = repos_set.iter().cloned().collect();

        printf!(
            "FetchCommitsMode={}: processing DB '{}' ({} repos, threads {}, batch {})\n",
            ctx_ro.fetch_commits_mode,
            db,
            repos.len(),
            thr_n,
            ctx_ro.git_commits_batch
        );

        let con = pg_conn_db_shared(ctx_ro, db);
        // Actor cache shared across repos processed for this DB (thread-safe).
        let acache = ActorCache::new();
        let counters = Mutex::new((0i64, 0i64));

        for_each_repo_limited(&repos, thr_n, |repo| {
            let (commits, roles) = match backfill_repo(ctx_ro, &con, db, repo, &maybe_hide, &acache)
            {
                Ok(v) => v,
                Err((commits, roles, err)) => {
                    printf!("backfillRepo(DB={db}, repo={repo}) error: {err}\n");
                    (commits, roles)
                }
            };
            let mut c = counters.lock().unwrap_or_else(|p| p.into_inner());
            c.0 += commits;
            c.1 += roles;
        });

        con.close();
        let (n_commits, n_roles) = *counters.lock().unwrap_or_else(|p| p.into_inner());
        printf!(
            "Finished DB '{db}': backfilled {n_commits} commits and {n_roles} commit roles for {} repos\n",
            repos.len()
        );
        all_commits += n_commits;
        all_roles += n_roles;
    }
    printf!(
        "Finished all DBs: backfilled {all_commits} commits and {all_roles} commit roles in: {}\n",
        format_go_duration(dt_start.elapsed())
    );

    ctx.exec_output = prev_exec_output;
    ctx.exec_fatal = prev_exec_fatal;
    ctx.exec_quiet = prev_exec_quiet;
}

/// Go `backfillRepo`: returns `(commits, roles)` inserted, or the counts so
/// far plus the error message.
fn backfill_repo(
    ctx: &Ctx,
    con: &PgConn,
    db: &str,
    repo: &str,
    maybe_hide: &MaybeHide,
    acache: &ActorCache,
) -> Result<(i64, i64), (i64, i64, String)> {
    let repo_path = format!("{}{repo}", ctx.repos_dir);
    stat_repo_path(db, &repo_path).map_err(|e| (0, 0, e))?;

    // For mode=1 (missing only) we can limit scanning by last commit time already inserted.
    let mut dt_from = GoTime::Utc(ctx.default_start_date);
    if ctx.fetch_commits_mode == 1 {
        let mut max_dt: Option<DateTime<FixedOffset>> = None;
        let res = con
            .query_row(
                "select max(dup_created_at) from gha_commits where dup_repo_name = $1",
                &[SqlArg::from(repo)],
            )
            .scan(&mut [&mut max_dt]);
        if let Err(err) = res {
            return Err((
                0,
                0,
                format!(
                    "select max(dup_created_at) from gha_commits failed (db={db}, repo={repo}): {err}"
                ),
            ));
        }
        if let Some(max_dt) = max_dt {
            if max_dt.with_timezone(&Utc) > dt_from.utc() {
                dt_from = GoTime::Db(max_dt);
            }
        }
    }

    let events =
        select_push_events_needing_commits(ctx, con, repo, dt_from).map_err(|e| (0, 0, e))?;
    if events.is_empty() {
        if ctx.debug > 0 {
            printf!("{db}/{repo}: no need to backfill commits since {dt_from}\n");
        }
        return Ok((0, 0));
    }
    printf!(
        "{db}/{repo}: need to backfill {} events since {dt_from}\n",
        events.len()
    );

    let mut pull_refs_fetched = false;

    // Build: event -> shas, plus global sha set.
    let mut event_shas: HashMap<i64, Vec<String>> = HashMap::with_capacity(events.len());
    let mut sha_set: HashSet<String> = HashSet::new();

    let page_size = if ctx.git_commits_batch <= 0 {
        1000usize
    } else {
        ctx.git_commits_batch as usize
    };

    for ev in &events {
        let head = normalize_sha(&ev.head);
        let mut before = normalize_sha(&ev.before);

        // Always require a sane HEAD; otherwise we can't safely insert into gha_commits.sha (varchar(40)).
        if !is_valid_non_zero_sha40(&head) {
            if ctx.debug > 0 {
                printf!(
                    "Warning: skipping PushEvent {} in {db}/{repo}: invalid/empty/zero head SHA {}\n",
                    ev.event_id,
                    go_quote(&ev.head)
                );
            }
            continue;
        }

        let shas: Vec<String>;

        if !LEGACY_UNSAFE_BACKFILL {
            // STRICT mode ("100% sure"):
            // only process events where BOTH BEFORE and HEAD are valid non-zero SHAs.
            if !is_valid_non_zero_sha40(&before) {
                if ctx.debug > 0 {
                    printf!(
                        "Warning: strict mode: skipping PushEvent {} in {db}/{repo}: invalid/empty/zero before SHA {}\n",
                        ev.event_id,
                        go_quote(&ev.before)
                    );
                }
                continue;
            }
            let mut res = git_range_commits(ctx, &repo_path, &before, &head, page_size, 0);
            if let Err((_, gerr)) = &res {
                let gerr = gerr.clone();
                // If the repo clone doesn't have the objects, try fetching GitHub PR refs once.
                if !pull_refs_fetched && is_exit_status_128(&gerr) {
                    printf!(
                        "Warning: git range failed for {db}/{repo} event {} ({before}..{head}): {gerr}, trying to fetch GitHub PR refs and retry\n",
                        ev.event_id
                    );
                    if let Err(ferr) = git_fetch_github_pull_refs(ctx, &repo_path) {
                        printf!(
                            "Warning: git fetch GitHub PR refs failed for {db}/{repo} (event {}): {ferr}\n",
                            ev.event_id
                        );
                    }
                    pull_refs_fetched = true;
                    res = git_range_commits(ctx, &repo_path, &before, &head, page_size, 0);
                    if let Err((_, gerr)) = &res {
                        printf!(
                            "Error listing commits range for {db}/{repo} after fetching PR refs (strict mode, event {}, before {}, head {}): {gerr}\n",
                            ev.event_id,
                            ev.before,
                            ev.head
                        );
                    }
                } else {
                    printf!(
                        "Error listing commits range for {db}/{repo} (strict mode, event {}, before {}, head {}): {gerr}\n",
                        ev.event_id,
                        ev.before,
                        ev.head
                    );
                }
                if res.is_err() {
                    // No fallback in strict mode.
                    continue;
                }
            }
            shas = res.unwrap_or_default();
            // No-op push; nothing to backfill.
            if before == head {
                continue;
            }
            // Optionally skip non-fast-forward / force pushes in strict mode.
            if !ALLOW_NON_FAST_FORWARD_PUSHES {
                match git_is_ancestor(ctx, &repo_path, &before, &head) {
                    Err(err) => {
                        if ctx.debug > 0 {
                            printf!(
                                "Warning: skipping PushEvent {} for {repo} (cannot determine ancestry {before}..{head}: {err})\n",
                                ev.event_id
                            );
                        }
                        continue;
                    }
                    Ok(false) => {
                        if ctx.debug > 0 {
                            printf!(
                                "Warning: skipping PushEvent {} for {repo} (non-fast-forward {before}..{head})\n",
                                ev.event_id
                            );
                        }
                        continue;
                    }
                    Ok(true) => {}
                }
            }
        } else {
            // Legacy/best-effort mode (compatible with previous behavior).
            // BEFORE=0/empty is ambiguous. Historically we used payload.size to limit the scan.
            let mut max_needed = 0usize;
            if before.is_empty() || is_zero_sha(&before) || !is_valid_hex_sha40(&before) {
                // Treat missing/zero/invalid BEFORE as 000..0 sentinel for the git range script.
                before = ZERO_SHA40.to_string();

                match ev.size {
                    Some(size) if size > 0 => {
                        // Avoid the known "cap size" footgun for before==0.
                        if LEGACY_CAPPED_SIZES.contains(&size) {
                            // Treat as unknown; safest legacy fallback is head-only.
                            max_needed = 1;
                        } else {
                            max_needed = size as usize;
                        }
                    }
                    _ => {
                        // No size available (post-2025 GHA); safest fallback is head-only.
                        max_needed = 1;
                    }
                }
            }

            let mut res = git_range_commits(ctx, &repo_path, &before, &head, page_size, max_needed);
            if let Err((_, gerr)) = &res {
                let gerr = gerr.clone();
                // Best-effort: try fetching PR refs once before falling back to head-only.
                if !pull_refs_fetched && is_exit_status_128(&gerr) {
                    if ctx.debug > 0 {
                        printf!(
                            "Warning: git range failed for {db}/{repo} event {} ({before}..{head}): {gerr}, trying to fetch GitHub PR refs and retry\n",
                            ev.event_id
                        );
                    }
                    if let Err(ferr) = git_fetch_github_pull_refs(ctx, &repo_path) {
                        printf!(
                            "Warning: git fetch GitHub PR refs failed for {db}/{repo} (event {}): {ferr}\n",
                            ev.event_id
                        );
                    }
                    pull_refs_fetched = true;
                    res = git_range_commits(ctx, &repo_path, &before, &head, page_size, max_needed);
                    if let Err((_, gerr)) = &res {
                        printf!(
                            "Error listing commits range for {db}/{repo} after fetching PR refs (legacy mode, event {}, before {}, head {}): {gerr}\n",
                            ev.event_id,
                            ev.before,
                            ev.head
                        );
                    }
                } else {
                    printf!(
                        "Error listing commits range for {db}/{repo} (legacy mode, event {}, before {}, head {}): {gerr}\n",
                        ev.event_id,
                        ev.before,
                        ev.head
                    );
                }
                // Legacy fallback: at least head commit.
                shas = match res {
                    Ok(v) if !(v.is_empty() && is_valid_non_zero_sha40(&head)) => v,
                    _ => vec![head.clone()],
                };
            } else {
                shas = res.unwrap_or_default();
            }
        }
        if shas.is_empty() {
            if ctx.debug > 0 {
                printf!(
                    "Warning: no commits found for {db}/{repo} PushEvent {} (before {}, head {})\n",
                    ev.event_id,
                    ev.before,
                    ev.head
                );
            }
            continue;
        }
        if ctx.debug > 1 {
            printf!(
                "{db}/{repo} PushEvent {}: found {} commits (before {}, head {}): {}\n",
                ev.event_id,
                shas.len(),
                ev.before,
                ev.head,
                gofmt::slice(&shas)
            );
        } else if ctx.debug == 1 {
            printf!(
                "{db}/{repo} PushEvent {}: found {} commits (before {}, head {})\n",
                ev.event_id,
                shas.len(),
                ev.before,
                ev.head
            );
        }
        for s in &shas {
            let s = normalize_sha(s);
            if !is_valid_non_zero_sha40(&s) {
                if ctx.debug > 0 {
                    printf!(
                        "Warning: skipping invalid/empty/zero SHA for {db}/{repo} PushEvent {}: {}\n",
                        ev.event_id,
                        go_quote(&s)
                    );
                }
                continue;
            }
            sha_set.insert(s);
        }
        event_shas.insert(ev.event_id, shas);
    }

    if event_shas.is_empty() || sha_set.is_empty() {
        printf!(
            "{db}/{repo}: no commits to backfill after processing {} events\n",
            events.len()
        );
        return Ok((0, 0));
    }
    printf!(
        "{db}/{repo}: need to backfill {} commits for {} events\n",
        sha_set.len(),
        events.len()
    );

    // Fetch commit metadata for all SHAs in batches.
    let mut sha_list: Vec<String> = sha_set.iter().cloned().collect();
    sha_list.sort();

    let mut info_map: HashMap<String, CommitInfo> = HashMap::with_capacity(sha_set.len());
    let mut i = 0usize;
    while i < sha_list.len() {
        let j = (i + page_size).min(sha_list.len());
        let (batch_infos, ierr) = git_commit_info_batch(ctx, &repo_path, &sha_list[i..j]);
        for (sha, info) in batch_infos {
            info_map.insert(normalize_sha(&sha), info);
        }
        if let Some(ierr) = ierr {
            printf!(
                "Warning: git_commits.sh error for {db}/{repo} batch {i}-{j}/{}: {ierr}\n",
                sha_list.len()
            );
        }
        i += page_size;
    }
    if info_map.is_empty() {
        return Err((
            0,
            0,
            format!(
                "git_commits.sh returned no commit metadata for db={db}, repo={repo} (shas={})",
                sha_set.len()
            ),
        ));
    }
    if ctx.debug > 0 {
        printf!(
            "Fetched commit metadata for {db}/{repo}: {} SHAs, {} records so far\n",
            sha_set.len(),
            info_map.len()
        );
    }

    let mut tx = con.begin().map_err(|e| (0, 0, e.to_string()))?;

    let ins_commit_sql = "
insert into gha_commits(
  sha, event_id, author_name, message,
  is_distinct, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at,
  author_id, committer_id, dup_author_login, dup_committer_login,
  author_email, committer_name, committer_email, origin
)
select
  $1::varchar(40),$2,$3,$4,
  not exists(select 1 from gha_commits c2 where c2.sha = $1::varchar(40) limit 1),
  $5,$6,$7,$8,$9,$10,
  $11,$12,$13,$14,
  $15,$16,$17,1
on conflict do nothing
";
    // Only fill missing payload size (NULL) with the computed count.
    let upd_payload_sql = "update gha_payloads set size = $2 where event_id = $1 and (size is null or size <= 1) and (size is null or size <> $2)";

    printf!(
        "{db}/{repo}: inserting commits for {} events\n",
        events.len()
    );
    let mut n_commits = 0i64;
    let mut n_roles = 0i64;
    for ev in &events {
        let shas = event_shas.get(&ev.event_id).cloned().unwrap_or_default();
        if ctx.debug > 0 {
            printf!(
                "{db}/{repo} PushEvent {}: inserting {} commits (before {}, head {})\n",
                ev.event_id,
                shas.len(),
                ev.before,
                ev.head
            );
        }
        if shas.is_empty() {
            if ctx.debug > 0 {
                printf!(
                    "Warning: no commits to insert for {db}/{repo} PushEvent {} (before {}, head {})\n",
                    ev.event_id,
                    ev.before,
                    ev.head
                );
            }
            continue;
        }

        // Legacy mode: optionally update payload.size when missing/<=1.
        if LEGACY_UNSAFE_BACKFILL {
            if let Err(uerr) = tx.exec(
                upd_payload_sql,
                &[SqlArg::from(ev.event_id), SqlArg::from(shas.len() as i64)],
            ) {
                return Err((
                    0,
                    0,
                    format!(
                        "update gha_payloads.size (db={db}, repo={repo}, event={}): error: {uerr}",
                        ev.event_id
                    ),
                ));
            }
        }

        if let Some(size) = ev.size {
            if shas.len() as i64 != size {
                printf!(
                    "Warning: {db}/{repo} PushEvent {} payload size={size}, computed commits={} (before {}, head {})\n",
                    ev.event_id,
                    shas.len(),
                    ev.before,
                    ev.head
                );
            }
        }

        for sha in &shas {
            let sha = normalize_sha(sha);
            if !is_valid_non_zero_sha40(&sha) {
                if ctx.debug > 0 {
                    printf!(
                        "Warning: skipping empty/zero SHA for {db}/{repo} PushEvent {}\n",
                        ev.event_id
                    );
                }
                continue;
            }

            let Some(ci) = info_map.get(&sha) else {
                if ctx.debug > 0 {
                    printf!(
                        "Warning: missing git metadata for {db}/{repo} sha {sha} (event {})\n",
                        ev.event_id
                    );
                }
                continue;
            };

            // Commit table fields.
            let author_name_raw = ci.author_name.replace('\0', "");
            let author_email_raw = ci.author_email.replace('\0', "");
            let comm_name_raw = ci.committer_name.replace('\0', "");
            let comm_email_raw = ci.committer_email.replace('\0', "");

            let author_name = trunc_to_bytes(&maybe_hide.hide(&author_name_raw), 120);
            let author_email = trunc_to_bytes(&maybe_hide.hide(&author_email_raw), 160);
            let msg = trunc_to_bytes(&ci.message.replace('\0', ""), 0xffff);

            // Roles fields (longer allowed).
            let author_role_name = trunc_to_bytes(&maybe_hide.hide(&author_name_raw), 160);
            let author_role_email = trunc_to_bytes(&maybe_hide.hide(&author_email_raw), 160);
            let comm_role_name = trunc_to_bytes(&maybe_hide.hide(&comm_name_raw), 160);
            let comm_role_email = trunc_to_bytes(&maybe_hide.hide(&comm_email_raw), 160);

            let (author_id, author_login) = lookup_actor_name_email_cached_tx(
                ctx,
                &mut tx,
                acache,
                maybe_hide,
                &author_name_raw,
                &author_email_raw,
            );
            let (comm_id, comm_login) = lookup_actor_name_email_cached_tx(
                ctx,
                &mut tx,
                acache,
                maybe_hide,
                &comm_name_raw,
                &comm_email_raw,
            );
            if ctx.debug > 0 && author_id == 0 {
                printf!(
                    "Warning: could not find actor for author of {db}/{repo} sha {sha} (event {}): name={}, email={}\n",
                    ev.event_id,
                    go_quote(&author_name_raw),
                    go_quote(&author_email_raw)
                );
            }
            if ctx.debug > 0 && comm_id == 0 {
                printf!(
                    "Warning: could not find actor for committer of {db}/{repo} sha {sha} (event {}): name={}, email={}\n",
                    ev.event_id,
                    go_quote(&comm_name_raw),
                    go_quote(&comm_email_raw)
                );
            }

            let dup_actor_login = trunc_to_bytes(&maybe_hide.hide(&ev.actor_login), 120);

            let dup_author_login = if author_login.is_empty() {
                String::new()
            } else {
                trunc_to_bytes(&maybe_hide.hide(&author_login), 120)
            };
            let dup_comm_login = if comm_login.is_empty() {
                String::new()
            } else {
                trunc_to_bytes(&maybe_hide.hide(&comm_login), 120)
            };

            // Insert commit.
            if let Err(err) = tx.exec(
                ins_commit_sql,
                &[
                    SqlArg::from(sha.as_str()),
                    SqlArg::from(ev.event_id),
                    SqlArg::from(author_name.as_str()),
                    SqlArg::from(msg.as_str()),
                    SqlArg::from(ev.actor_id),
                    SqlArg::from(dup_actor_login.as_str()),
                    SqlArg::from(ev.repo_id),
                    SqlArg::from(ev.repo_name.as_str()),
                    SqlArg::from("PushEvent"),
                    SqlArg::from(ev.created_at),
                    SqlArg::from(author_id),
                    SqlArg::from(comm_id),
                    SqlArg::from(dup_author_login.as_str()),
                    SqlArg::from(dup_comm_login.as_str()),
                    SqlArg::from(author_email.as_str()),
                    SqlArg::from(comm_role_name.as_str()),
                    SqlArg::from(comm_role_email.as_str()),
                ],
            ) {
                return Err((
                    0,
                    0,
                    format!(
                        "insert gha_commits (db={db}, repo={repo}, event={}, sha={sha}): error: {err}",
                        ev.event_id
                    ),
                ));
            }
            n_commits += 1;

            // Insert roles: Author + Committer + trailers.
            if INSERT_AUTHOR_ROLE {
                if let Err(err) = insert_roles(
                    &mut tx,
                    &sha,
                    ev,
                    "Author",
                    author_id,
                    &author_login,
                    &author_role_name,
                    &author_role_email,
                    maybe_hide,
                ) {
                    return Err((
                        0,
                        0,
                        format!(
                            "insert Author role (db={db}, repo={repo}, event={}, sha={sha}): error: {err}",
                            ev.event_id
                        ),
                    ));
                }
                n_roles += 1;
            }
            if INSERT_COMMITTER_ROLE {
                if let Err(err) = insert_roles(
                    &mut tx,
                    &sha,
                    ev,
                    "Committer",
                    comm_id,
                    &comm_login,
                    &comm_role_name,
                    &comm_role_email,
                    maybe_hide,
                ) {
                    return Err((
                        0,
                        0,
                        format!(
                            "insert Committer role (db={db}, repo={repo}, event={}, sha={sha}): error: {err}",
                            ev.event_id
                        ),
                    ));
                }
                n_roles += 1;
            }

            let trailer_roles = parse_trailers(ctx, &ci.message);
            for tr in &trailer_roles {
                let name = trunc_to_bytes(&maybe_hide.hide(&tr.name), 160);
                let email = trunc_to_bytes(&maybe_hide.hide(&tr.email), 160);

                let (t_id, t_login) = lookup_actor_name_email_cached_tx(
                    ctx, &mut tx, acache, maybe_hide, &tr.name, &tr.email,
                );
                if ctx.debug > 0 && t_id == 0 {
                    printf!(
                        "Warning: could not find actor for trailer role of {db}/{repo} sha {sha} (event {}): name={}, email={}\n",
                        ev.event_id,
                        go_quote(&tr.name),
                        go_quote(&tr.email)
                    );
                }
                if let Err(err) = insert_roles(
                    &mut tx, &sha, ev, &tr.role, t_id, &t_login, &name, &email, maybe_hide,
                ) {
                    return Err((
                        0,
                        0,
                        format!(
                            "insert trailer role (db={db}, repo={repo}, event={}, sha={sha}, role={}): error: {err}",
                            ev.event_id, tr.role
                        ),
                    ));
                }
                n_roles += 1;
            }
        }
    }

    if let Err(err) = tx.commit() {
        printf!("Error committing transaction for {db}/{repo}: {err}\n");
        return Err((0, 0, err.to_string()));
    }
    printf!(
        "{db}/{repo}: successfully backfilled {n_commits} commits and {n_roles} commit roles for {} events\n",
        events.len()
    );
    Ok((n_commits, n_roles))
}

/// Go `insertRoles`: insert one `gha_commits_roles` row.
#[allow(clippy::too_many_arguments)]
fn insert_roles(
    tx: &mut PgTx<'_>,
    sha: &str,
    ev: &PushEvent,
    role: &str,
    actor_id: i64,
    actor_login: &str,
    actor_name: &str,
    actor_email: &str,
    maybe_hide: &MaybeHide,
) -> Result<(), PgError> {
    // gha_commits_roles columns are NOT NULL (defaults: actor_id=0, actor_login/name/email='').
    let actor_id = actor_id.max(0);
    let actor_login = if actor_login.is_empty() {
        String::new()
    } else {
        trunc_to_bytes(&maybe_hide.hide(actor_login), 120)
    };
    let actor_name = if actor_name.is_empty() {
        String::new()
    } else {
        trunc_to_bytes(actor_name, 160)
    };
    let actor_email = if actor_email.is_empty() {
        String::new()
    } else {
        trunc_to_bytes(actor_email, 160)
    };
    let role = if role.is_empty() {
        String::new()
    } else {
        trunc_to_bytes(role, 60)
    };

    tx.exec(
        INS_COMMIT_ROLE_SQL,
        &[
            SqlArg::from(sha),
            SqlArg::from(ev.event_id),
            SqlArg::from(role.as_str()),
            SqlArg::from(actor_id),
            SqlArg::from(actor_login.as_str()),
            SqlArg::from(actor_name.as_str()),
            SqlArg::from(actor_email.as_str()),
            SqlArg::from(ev.repo_id),
            SqlArg::from(ev.repo_name.as_str()),
            SqlArg::from(ev.created_at),
        ],
    )
    .map(|_| ())
}

fn b64_field(field: &str, what: &str, sha: &str) -> Result<String, String> {
    match gobase64::std_decode(field) {
        Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).to_string()),
        Err((_, e)) => Err(format!("base64 decode {what} for {sha}: {e}")),
    }
}

/// Go `parseGitCommitsOutput`: parse git/git_commits.sh output.
/// Record separator: `;`
/// Fields: sha,b64(author_name),b64(author_email),b64(committer_name),b64(committer_email),b64(message)
fn parse_git_commits_output(
    out: &str,
    out_map: &mut HashMap<String, CommitInfo>,
) -> Result<(), String> {
    let s = out.trim();
    if s.is_empty() {
        return Ok(());
    }

    for rec in s.split(';') {
        let rec = rec.trim();
        if rec.is_empty() {
            continue;
        }
        let parts: Vec<&str> = rec.split(',').collect();
        if parts.len() != 6 {
            return Err(format!(
                "invalid git_commits.sh record (expected 6 fields): {}",
                go_quote(rec)
            ));
        }
        let sha = parts[0].trim();
        if sha.is_empty() {
            return Err(format!(
                "empty sha in git_commits.sh record: {}",
                go_quote(rec)
            ));
        }

        let an = b64_field(parts[1], "author_name", sha)?;
        let ae = b64_field(parts[2], "author_email", sha)?;
        let cn = b64_field(parts[3], "committer_name", sha)?;
        let ce = b64_field(parts[4], "committer_email", sha)?;
        let msg = b64_field(parts[5], "message", sha)?;

        // PostgreSQL text cannot contain NUL bytes; strip defensively.
        out_map.insert(
            sha.to_string(),
            CommitInfo {
                sha: sha.to_string(),
                author_name: an.replace('\0', ""),
                author_email: ae.replace('\0', ""),
                committer_name: cn.replace('\0', ""),
                committer_email: ce.replace('\0', ""),
                message: msg.replace('\0', ""),
            },
        );
    }
    Ok(())
}

/// Go `%+v` of a `map[string]commitInfo` (sorted keys).
fn commit_info_map_string(m: &HashMap<String, CommitInfo>) -> String {
    let sorted: BTreeMap<&String, &CommitInfo> = m.iter().collect();
    let parts: Vec<String> = sorted.iter().map(|(k, v)| format!("{k}:{v}")).collect();
    format!("map[{}]", parts.join(" "))
}

/// Go `gitCommitInfoBatch`: run git/git_commits.sh for a batch. If it fails,
/// bisect to salvage partial results. Returns the records parsed so far and
/// the error (if any).
fn git_commit_info_batch(
    ctx: &Ctx,
    repo_path: &str,
    shas: &[String],
) -> (HashMap<String, CommitInfo>, Option<String>) {
    let mut out_map: HashMap<String, CommitInfo> = HashMap::with_capacity(shas.len());
    if shas.is_empty() {
        if ctx.debug > 0 {
            printf!("Warning: empty SHA batch for repo {repo_path}\n");
        }
        return (out_map, None);
    }

    let cmd_prefix = if ctx.local_cmd { LOCAL_GIT_SCRIPTS } else { "" };
    let mut args: Vec<String> = vec![format!("{cmd_prefix}git_commits.sh"), repo_path.to_string()];
    args.extend(shas.iter().cloned());
    let err = match exec::exec_command(ctx, &args, &no_env()) {
        Ok(out) => {
            let perr = parse_git_commits_output(&out, &mut out_map);
            match &perr {
                Err(perr) => {
                    printf!(
                        "Parsed git_commits.sh output for repo {repo_path}, batch size {}: {} records, parse error: {perr}\n",
                        shas.len(),
                        out_map.len()
                    );
                }
                Ok(()) => {
                    if ctx.debug > 1 {
                        printf!(
                            "Parsed git_commits.sh output for repo {repo_path}, batch size {}: {} records: {}\n",
                            shas.len(),
                            out_map.len(),
                            commit_info_map_string(&out_map)
                        );
                    }
                }
            }
            return (out_map, perr.err());
        }
        Err(e) => e.to_string(),
    };

    if ctx.debug > 0 {
        printf!(
            "Error running git_commits.sh for repo {repo_path}, batch size {}: {err}\n",
            shas.len()
        );
    }
    // If a batch fails, split to isolate bad SHAs but keep partial output.
    if shas.len() == 1 {
        return (out_map, Some(err));
    }
    let mid = shas.len() / 2;
    let (mut left, err_l) = git_commit_info_batch(ctx, repo_path, &shas[..mid]);
    let (right, err_r) = git_commit_info_batch(ctx, repo_path, &shas[mid..]);
    left.extend(right);

    match (err_l, err_r) {
        (Some(l), Some(r)) => (
            left,
            Some(format!(
                "git_commits.sh error for both halves: ({l}) and ({r})"
            )),
        ),
        (Some(l), None) => (left, Some(l)),
        (None, Some(r)) => (left, Some(r)),
        (None, None) => (left, None),
    }
}

/// Go `gitRangeCommits`: list commits between BEFORE..HEAD using
/// git/git_commits_range.sh paging.
///
/// Script output is newest->oldest for stable paging with --skip/--max-count;
/// the returned list is oldest->newest. On failure returns the commits
/// collected so far and the error message.
fn git_range_commits(
    ctx: &Ctx,
    repo_path: &str,
    before: &str,
    head: &str,
    page_size: usize,
    max_needed: usize,
) -> Result<Vec<String>, (Vec<String>, String)> {
    let before = before.trim();
    let head = head.trim();

    if head.is_empty() || is_zero_sha(head) {
        return Ok(Vec::new());
    }

    let before = if before.is_empty() {
        ZERO_SHA40
    } else {
        before
    };

    let limit = if page_size == 0 { 1000 } else { page_size };

    let cmd_prefix = if ctx.local_cmd { LOCAL_GIT_SCRIPTS } else { "" };

    let mut all: Vec<String> = Vec::with_capacity(limit);
    let mut skip = 0usize;
    loop {
        let args = vec![
            format!("{cmd_prefix}git_commits_range.sh"),
            repo_path.to_string(),
            before.to_string(),
            head.to_string(),
            format!("{skip}"),
            format!("{limit}"),
        ];
        let out = match exec::exec_command(ctx, &args, &no_env()) {
            Ok(out) => out,
            Err(e) => return Err((all, e.to_string())),
        };

        let mut chunk: Vec<String> = Vec::new();
        for line in out.split('\n') {
            let sha = line.trim();
            if sha.is_empty() {
                continue;
            }
            chunk.push(sha.to_string());
            // For BEFORE=0 case we may only need the newest maxNeeded commits.
            if max_needed > 0 && all.len() + chunk.len() >= max_needed {
                break;
            }
        }
        if chunk.is_empty() {
            break;
        }

        // If maxNeeded is set and we overshot by parsing a bigger page, trim.
        if max_needed > 0 && all.len() + chunk.len() > max_needed {
            chunk.truncate(max_needed - all.len());
        }

        let chunk_len = chunk.len();
        all.extend(chunk);

        if max_needed > 0 && all.len() >= max_needed {
            break;
        }
        if chunk_len < limit {
            break;
        }
        skip += limit;
    }

    // Reverse newest->oldest to oldest->newest.
    all.reverse();
    Ok(all)
}

/// Go `selectPushEventsNeedingCommits`: mode=1: missing only; mode>=2:
/// missing + truncated (cnt < payload.size).
fn select_push_events_needing_commits(
    ctx: &Ctx,
    con: &PgConn,
    repo: &str,
    dt_from: GoTime,
) -> Result<Vec<PushEvent>, String> {
    let q = "
select
  e.id,
  e.actor_id,
  e.dup_actor_login,
  e.repo_id,
  e.dup_repo_name,
  e.created_at,
  p.head,
  p.befor,
  p.ref,
  p.push_id,
  p.size,
  coalesce(c.cnt,0) as cnt
from gha_events e
join gha_payloads p on p.event_id = e.id
left join (
  select event_id, count(*) as cnt
  from gha_commits
	where dup_repo_name = $1
  and dup_created_at >= $2
  group by event_id
) c on c.event_id = e.id
where e.type = 'PushEvent'
  and e.dup_repo_name = $1
  and e.created_at >= $2
  and (
    p.size is null
    or p.size > 0
    or (
      p.size = 0
      and p.befor is not null
      and p.befor <> ''
      and p.befor <> '0000000000000000000000000000000000000000'
    )
  )
  and (
    c.cnt is null
    or c.cnt = 0
    or (
      $3 >= 2
      and p.size is not null
      and c.cnt < p.size
    )
  )
order by e.created_at, e.id
";
    let dt_arg = match dt_from {
        GoTime::Utc(t) => SqlArg::from(t),
        GoTime::Db(t) => SqlArg::from(t),
    };
    let mut rows = con
        .query(
            q,
            &[
                SqlArg::from(repo),
                dt_arg,
                SqlArg::from(ctx.fetch_commits_mode),
            ],
        )
        .map_err(|e| e.to_string())?;

    let mut out: Vec<PushEvent> = Vec::new();
    while rows.next() {
        let mut event_id = 0i64;
        let mut actor_id = 0i64;
        let mut actor_login = String::new();
        let mut repo_id = 0i64;
        let mut repo_name = String::new();
        let mut created_at = DateTime::<FixedOffset>::default();
        let mut head: Option<String> = None;
        let mut bef: Option<String> = None;
        let mut ref_: Option<String> = None;
        let mut push_id: Option<i64> = None;
        let mut size: Option<i64> = None;
        let mut cnt = 0i64;
        rows.scan(&mut [
            &mut event_id,
            &mut actor_id,
            &mut actor_login,
            &mut repo_id,
            &mut repo_name,
            &mut created_at,
            &mut head,
            &mut bef,
            &mut ref_,
            &mut push_id,
            &mut size,
            &mut cnt,
        ])
        .map_err(|e| e.to_string())?;
        out.push(PushEvent {
            event_id,
            actor_id,
            actor_login,
            repo_id,
            repo_name,
            created_at,
            head: head.unwrap_or_default(),
            before: bef.unwrap_or_default(),
            ref_: ref_.unwrap_or_default(),
            push_id,
            size,
            cnt,
        });
    }
    rows.err().map_err(|e| e.to_string())?;
    let _ = rows.close();
    Ok(out)
}

/// One `tx.QueryRow(...).Scan(&id, &login)` of the actor lookup: `Ok(true)`
/// when a row was found, `Ok(false)` for `sql.ErrNoRows`.
fn lookup_row(
    tx: &mut PgTx<'_>,
    sql: &str,
    arg: &str,
    id: &mut i64,
    login: &mut String,
) -> Result<bool, PgError> {
    let mut rid = 0i64;
    let mut rlogin = String::new();
    match tx
        .query_row(sql, &[SqlArg::from(arg)])
        .scan(&mut [&mut rid, &mut rlogin])
    {
        Ok(()) => {
            *id = rid;
            *login = rlogin;
            Ok(true)
        }
        Err(PgError::NoRows) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Go `lookupActorNameEmailCachedTx`: map (name, email) to (actor_id,
/// actor_login) using the same tables as gha2db:
/// - gha_actors_emails (email -> actor)
/// - gha_actors_names  (name  -> actor)
/// - gha_actors        (name  -> actor)
///
/// Cache key uses (lower(email), lower(name)).
fn lookup_actor_name_email_cached_tx(
    ctx: &Ctx,
    tx: &mut PgTx<'_>,
    cache: &ActorCache,
    maybe_hide: &MaybeHide,
    name: &str,
    email: &str,
) -> (i64, String) {
    let key = (email.trim().to_lowercase(), name.trim().to_lowercase());

    {
        let m = cache.m.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((id, login)) = m.get(&key) {
            return (*id, login.clone());
        }
    }

    let a_name = maybe_hide.hide(name).trim().to_string();
    let a_email = maybe_hide.hide(email).trim().to_string();

    let mut id = 0i64;
    let mut login = String::new();

    if !a_email.is_empty() {
        if let Err(err) = lookup_row(
            tx,
            "select a.id, a.login from gha_actors a, gha_actors_emails ae where a.id = ae.actor_id and lower(ae.email) = lower($1) order by a.id desc limit 1",
            &a_email,
            &mut id,
            &mut login,
        ) {
            printf!(
                "Warning: lookup actor by email failed (email={}): error: {err}\n",
                go_quote(&a_email)
            );
        }
    }

    if id == 0 && !a_name.is_empty() {
        if let Err(err) = lookup_row(
            tx,
            "select a.id, a.login from gha_actors a, gha_actors_names an where a.id = an.actor_id and lower(an.name) = lower($1) order by a.id desc limit 1",
            &a_name,
            &mut id,
            &mut login,
        ) {
            printf!(
                "Warning: lookup actor by gha_actors_names failed (name={}): error: {err}\n",
                go_quote(&a_name)
            );
        }
    }

    if id == 0 && !a_name.is_empty() {
        if let Err(err) = lookup_row(
            tx,
            "select id, login from gha_actors where lower(name) = lower($1) order by id desc limit 1",
            &a_name,
            &mut id,
            &mut login,
        ) {
            printf!(
                "Warning: lookup actor by gha_actors.name failed (name={}): error: {err}\n",
                go_quote(&a_name)
            );
        }
    }

    if id == 0 && !a_name.is_empty() {
        if let Err(err) = lookup_row(
            tx,
            "select id, login from gha_actors where lower(login) = lower($1) order by id desc limit 1",
            &a_name,
            &mut id,
            &mut login,
        ) {
            printf!(
                "Warning: lookup actor by gha_actors.login failed (name={}): error: {err}\n",
                go_quote(&a_name)
            );
        }
    }

    {
        let mut m = cache.m.lock().unwrap_or_else(|p| p.into_inner());
        m.insert(key, (id, login.clone()));
    }

    if ctx.debug > 0 {
        printf!(
            "lookupActorNameEmailCachedTx: name={}, email={} -> id={id}, login={}\n",
            go_quote(name),
            go_quote(email),
            go_quote(&login)
        );
    }
    (id, login)
}

/// Go `parseTrailers`: extract commit roles from message trailers (shared
/// table with gha2db via `lib.GitTrailerPattern` / `lib.GitAllowedTrailers`).
fn parse_trailers(ctx: &Ctx, msg: &str) -> Vec<trailers::Trailer> {
    let out = trailers::parse_trailers(msg);
    if ctx.debug > 1 {
        let parts: Vec<String> = out
            .iter()
            .map(|t| format!("{{Role:{} Name:{} Email:{}}}", t.role, t.name, t.email))
            .collect();
        printf!("parse trailers: '{msg}' -> [{}]\n", parts.join(" "));
    }
    out
}

/// Go `restoreOrphanCommits`: restore commits present in git but with no
/// `gha_commits` or `gha_skip_commits` row. DBs are processed sequentially;
/// repos inside a DB in parallel up to NCPUs.
pub fn restore_orphan_commits(
    ctx: &mut Ctx,
    dbs: &BTreeMap<String, String>,
    repo_dbs: &BTreeMap<String, BTreeSet<String>>,
) {
    if !ctx.restore_orphan_commits {
        return;
    }
    let dt_start = Instant::now();

    let prev_exec_output = ctx.exec_output;
    let prev_exec_fatal = ctx.exec_fatal;
    let prev_exec_quiet = ctx.exec_quiet;
    ctx.exec_output = true;
    ctx.exec_fatal = false;
    ctx.exec_quiet = true;
    ctx.can_reconnect = false;

    let hide_cfg = get_hidden(ctx, HIDE_CFG_FILE);
    let maybe_hide = MaybeHide::new(hide_cfg);

    let thr_n = threads::get_threads_num(ctx);
    let ctx_ro: &Ctx = ctx;

    let mut all_repos_processed = 0i64;
    let mut all_commits_checked = 0i64;
    let mut all_commits_restored = 0i64;

    for db in dbs.keys() {
        let Some(repos_set) = repo_dbs.get(db) else {
            continue;
        };
        if repos_set.is_empty() {
            continue;
        }
        let repos: Vec<String> = repos_set.iter().cloned().collect();

        printf!(
            "Restoring orphan commits: processing DB '{db}' ({} repos, threads {thr_n})\n",
            repos.len()
        );

        let con = pg_conn_db_shared(ctx_ro, db);
        let acache = ActorCache::new();

        let skip_set = match select_skip_commits(&con) {
            Ok(s) => s,
            Err(err) => {
                printf!("selectSkipCommits(DB={db}) error: {err}\n");
                con.close();
                continue;
            }
        };
        let aliases = match select_repo_aliases(&con) {
            Ok(a) => a,
            Err(err) => {
                printf!("selectRepoAliases(DB={db}) error: {err}\n");
                con.close();
                continue;
            }
        };
        let claimed_shas: Mutex<HashSet<String>> = Mutex::new(HashSet::new());

        // Renamed repos are cloned under every historical name (gha_repos keeps them all); their
        // current name is gha_repos.alias. A historical clone is skipped when the current-name
        // clone exists (it holds the same history), otherwise its commits are attributed to the
        // current name - never to the alias (bug 61).
        let mut work: Vec<(String, String)> = Vec::with_capacity(repos.len());
        let mut n_aliases_skipped = 0usize;
        for repo in &repos {
            let name = aliases.get(repo).cloned().unwrap_or_else(|| repo.clone());
            if name != *repo && Path::new(&format!("{}{name}", ctx_ro.repos_dir)).exists() {
                n_aliases_skipped += 1;
                if ctx_ro.debug > 0 {
                    printf!("{db}/{repo}: historical alias of {name}, skipping\n");
                }
                continue;
            }
            work.push((repo.clone(), name));
        }
        if n_aliases_skipped > 0 {
            printf!(
                "Restoring orphan commits: DB '{db}': skipped {n_aliases_skipped} historical alias clone(s)\n"
            );
        }

        // (repos processed, commits checked, commits restored, restored event ids)
        let totals: Mutex<(i64, i64, i64, Vec<i64>)> = Mutex::new((0, 0, 0, Vec::new()));

        for_each_repo_limited(&work, thr_n, |(repo, name)| {
            let (rp, cc, cr, reids) = match restore_orphan_repo(
                ctx_ro,
                &con,
                db,
                repo,
                name,
                &maybe_hide,
                &acache,
                &skip_set,
                &claimed_shas,
            ) {
                Ok(v) => v,
                Err((rp, cc, cr, err)) => {
                    printf!("restoreOrphanRepo(DB={db}, repo={repo}) error: {err}\n");
                    (rp, cc, cr, Vec::new())
                }
            };
            let mut t = totals.lock().unwrap_or_else(|p| p.into_inner());
            t.0 += rp;
            t.1 += cc;
            t.2 += cr;
            t.3.extend(reids);
        });

        con.close();
        let (n_repos_processed, n_commits_checked, n_commits_restored, db_eids) =
            totals.into_inner().unwrap_or_else(|p| p.into_inner());
        printf!(
            "Finished DB '{db}': processed {n_repos_processed} repos, checked {n_commits_checked} commits, restored {n_commits_restored}\n"
        );
        if !db_eids.is_empty() {
            restore::run_event_ids_postprocess_db(ctx_ro, db, &db_eids);
        }
        all_repos_processed += n_repos_processed;
        all_commits_checked += n_commits_checked;
        all_commits_restored += n_commits_restored;
    }

    printf!(
        "Finished orphan commit restore: processed {all_repos_processed} repos, checked {all_commits_checked} commits, restored {all_commits_restored} in: {}\n",
        format_go_duration(dt_start.elapsed())
    );

    ctx.exec_output = prev_exec_output;
    ctx.exec_fatal = prev_exec_fatal;
    ctx.exec_quiet = prev_exec_quiet;
}

type RestoreResult = Result<(i64, i64, i64, Vec<i64>), (i64, i64, i64, String)>;

/// Go `orphanBranch`: one `refs/remotes/origin/<name>` branch scanned for orphan commits.
struct OrphanBranch {
    ref_: String,
    name: String,
}

/// Go `orphanPush`: commits that became reachable from a branch in one first-parent step, i.e.
/// the GHA PushEvent shape: head = the step commit, before = its first parent ("" for a root
/// commit), ref = refs/heads/<branch>, created = the step's committer date (landing time),
/// size = number of commits that landed (payload size, also counting commits that are already
/// in the database), commits = the ones handled by this clone (newest first).
/// In the legacy one-event-per-commit mode every commit is its own push: head = the commit,
/// before = "", ref = the remote ref, created = its author date, size = 1.
struct OrphanPush {
    head: String,
    before: String,
    ref_: String,
    created: DateTime<Utc>,
    size: usize,
    commits: Vec<String>,
}

/// Go `branchName`: refs/remotes/origin/<name> or refs/heads/<name> -> <name> (anything else,
/// e.g. HEAD, is returned unchanged).
fn branch_name(ref_: &str) -> &str {
    let name = ref_.strip_prefix("refs/remotes/origin/").unwrap_or(ref_);
    name.strip_prefix("refs/heads/").unwrap_or(name)
}

/// Go `pushRef`: the payload ref of pushes scanned from `ref_`: refs/heads/<name> for a remote
/// branch, the ref itself otherwise (the HEAD fallback).
fn push_ref(ref_: &str) -> String {
    if ref_.starts_with("refs/remotes/origin/") {
        format!("refs/heads/{}", branch_name(ref_))
    } else {
        ref_.to_string()
    }
}

/// Go `restoreOrphanRepo`: returns (repos processed, commits checked,
/// commits restored, restored event ids) or the counts so far plus the error.
/// `repo` is the clone directory name, `name` the repo name the restored rows
/// are attributed to (the current name; differs for historical aliases).
#[allow(clippy::too_many_arguments)]
fn restore_orphan_repo(
    ctx: &Ctx,
    con: &PgConn,
    db: &str,
    repo: &str,
    name: &str,
    maybe_hide: &MaybeHide,
    acache: &ActorCache,
    skip_set: &HashSet<String>,
    claimed_shas: &Mutex<HashSet<String>>,
) -> RestoreResult {
    let mut eids: Vec<i64> = Vec::new();
    let repo_path = format!("{}{repo}", ctx.repos_dir);
    stat_repo_path(db, &repo_path).map_err(|e| (0, 0, 0, e))?;

    let mut dt_from = GoTime::Utc(ctx.default_start_date);
    if !ctx.orphan_commits_range.is_empty() {
        let mut dt_to: Option<DateTime<FixedOffset>> = None;
        if let Err(err) = con.query_row("select now()", &[]).scan(&mut [&mut dt_to]) {
            return Err((
                0,
                0,
                0,
                format!("select now() failed (db={db}, repo={repo}): {err}"),
            ));
        }
        if let Some(dt_to) = dt_to {
            let ago = get_date_ago(con, ctx, dt_to, &ctx.orphan_commits_range);
            dt_from = GoTime::Db(ago.fixed_offset());
        }
    }
    let since = dt_from.utc();

    // The default ref first; --all would also pick upstream history reachable in fork clones,
    // so only `origin/*` branches whose tip moved inside the window are added.
    let default_ref = match git_default_ref(ctx, &repo_path) {
        Ok(r) if !r.is_empty() => r,
        Ok(_) => {
            printf!(
                "Warning: could not determine default ref for {db}/{repo}: <nil>, using HEAD\n"
            );
            "HEAD".to_string()
        }
        Err(err) => {
            printf!(
                "Warning: could not determine default ref for {db}/{repo}: {err}, using HEAD\n"
            );
            "HEAD".to_string()
        }
    };
    let mut branches = vec![OrphanBranch {
        ref_: default_ref.clone(),
        name: branch_name(&default_ref).to_string(),
    }];
    if ctx.orphan_commits_all_branches {
        match git_origin_branches(ctx, &repo_path, since) {
            Ok(more) => {
                for b in more {
                    if b.ref_ != default_ref {
                        branches.push(b);
                    }
                }
            }
            Err(berr) => {
                printf!(
                    "Warning: cannot list branches of {db}/{repo}: {berr}, scanning {default_ref} only\n"
                );
            }
        }
        if branches.len() > 1 && ctx.debug > 0 {
            printf!(
                "{db}/{repo}: scanning {} branches updated since {dt_from}\n",
                branches.len()
            );
        }
    }

    // Every commit is handled once per clone: the default branch claims it first.
    let mut pushes: Vec<OrphanPush> = Vec::new();
    let mut shas: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for b in &branches {
        let listed = if ctx.orphan_commits_group {
            git_landed_commits(ctx, &repo_path, b, since)
        } else {
            git_listed_commits(ctx, &repo_path, &b.ref_, since)
        };
        let bp = match listed {
            Ok(bp) => bp,
            Err(lerr) => {
                if b.ref_ == default_ref {
                    return Err((
                        0,
                        0,
                        0,
                        format!("gitListCommits failed for {db}/{repo}: {lerr}"),
                    ));
                }
                printf!(
                    "Warning: listing commits of {db}/{repo} branch {} failed: {lerr}, skipping it\n",
                    b.name
                );
                continue;
            }
        };
        let (mut n_commits, mut n_pushes) = (0usize, 0usize);
        for mut p in bp {
            let mut kept: Vec<String> = Vec::with_capacity(p.commits.len());
            for sha in p.commits.drain(..) {
                if !seen.insert(sha.clone()) {
                    continue;
                }
                shas.push(sha.clone());
                kept.push(sha);
            }
            if kept.is_empty() {
                continue;
            }
            n_commits += kept.len();
            n_pushes += 1;
            p.commits = kept;
            pushes.push(p);
        }
        if ctx.orphan_commits_group && n_commits > 0 && ctx.debug > 0 {
            printf!(
                "{db}/{repo}: {}: {n_commits} commits in {n_pushes} pushes landed since {dt_from}\n",
                b.name
            );
        }
    }

    if shas.is_empty() {
        if ctx.debug > 0 {
            printf!("{db}/{repo}: no commits found since {dt_from}\n");
        }
        return Ok((0, 0, 0, Vec::new()));
    }

    if ctx.debug > 0 {
        printf!(
            "{db}/{repo}: found {} commits since {dt_from}\n",
            shas.len()
        );
    }
    let n_shas = shas.len() as i64;

    let candidates: Vec<String> = shas
        .iter()
        .filter(|sha| !skip_set.contains(*sha))
        .cloned()
        .collect();

    // Existence check is DB-wide (not per dup_repo_name): renamed repos are cloned under
    // every historical name and per-name checks restored the same commits once per alias.
    let batch = if ctx.git_commits_batch <= 0 {
        1000usize
    } else {
        ctx.git_commits_batch as usize
    };
    let mut existing_set: HashSet<String> = HashSet::new();
    let mut i = 0usize;
    while i < candidates.len() {
        let j = (i + batch).min(candidates.len());
        let args: Vec<SqlArg> = candidates[i..j]
            .iter()
            .map(|s| SqlArg::from(s.as_str()))
            .collect();
        let ph: Vec<String> = (1..=j - i).map(|k| format!("${k}")).collect();
        let mut rows = con
            .query(
                &format!(
                    "select sha from gha_commits where sha in ({})",
                    ph.join(",")
                ),
                &args,
            )
            .map_err(|e| {
                (
                    0,
                    0,
                    0,
                    format!("select gha_commits shas failed (db={db}, repo={repo}): {e}"),
                )
            })?;
        while rows.next() {
            let mut sha = String::new();
            if let Err(e) = rows.scan(&mut [&mut sha]) {
                let _ = rows.close();
                return Err((0, 0, 0, e.to_string()));
            }
            existing_set.insert(normalize_sha(&sha));
        }
        let err = rows.err();
        let _ = rows.close();
        err.map_err(|e| (0, 0, 0, e.to_string()))?;
        i += batch;
    }

    let to_restore: Vec<String> = candidates
        .iter()
        .filter(|sha| !existing_set.contains(*sha))
        .cloned()
        .collect();
    let restore_set: HashSet<&str> = to_restore.iter().map(String::as_str).collect();

    if to_restore.is_empty() {
        if ctx.debug > 0 {
            printf!("{db}/{repo}: no orphan commits to restore\n");
        }
        return Ok((1, n_shas, 0, Vec::new()));
    }

    if ctx.debug > 0 {
        printf!(
            "{db}/{repo}: need to restore {} orphan commits\n",
            to_restore.len()
        );
    }

    // Metadata of the commits to restore plus the heads of their pushes (the event actor
    // is the head's committer, and a head may itself be in the database already).
    let mut meta_shas: Vec<String> = to_restore.clone();
    let mut meta_set: HashSet<String> = to_restore.iter().cloned().collect();
    for p in &pushes {
        if meta_set.contains(&p.head) {
            continue;
        }
        if p.commits
            .iter()
            .any(|sha| restore_set.contains(sha.as_str()))
        {
            meta_set.insert(p.head.clone());
            meta_shas.push(p.head.clone());
        }
    }

    let page_size = batch;

    let mut info_map: HashMap<String, CommitInfo> = HashMap::new();
    let mut i = 0usize;
    while i < meta_shas.len() {
        let j = (i + page_size).min(meta_shas.len());
        let (batch_infos, ierr) = git_commit_info_batch(ctx, &repo_path, &meta_shas[i..j]);
        for (sha, info) in batch_infos {
            info_map.insert(normalize_sha(&sha), info);
        }
        if let Some(ierr) = ierr {
            printf!(
                "Warning: git_commits.sh error for {db}/{repo} batch {i}-{j}/{}: {ierr}\n",
                meta_shas.len()
            );
        }
        i += page_size;
    }

    if info_map.is_empty() {
        return Err((
            1,
            n_shas,
            0,
            format!(
                "git_commits.sh returned no commit metadata for db={db}, repo={repo} (shas={})",
                to_restore.len()
            ),
        ));
    }

    if ctx.debug > 0 {
        printf!(
            "Fetched commit metadata for {db}/{repo}: {} SHAs, {} records\n",
            meta_shas.len(),
            info_map.len()
        );
    }

    if name != repo && ctx.debug > 0 {
        printf!("{db}/{repo}: attributing restored commits to {name} (current name)\n");
    }
    let mut repo_id = get_repo_id(con, name).map_err(|e| (1, n_shas, 0, e))?;
    if repo_id == 0 && name != repo {
        repo_id = get_repo_id(con, repo).map_err(|e| (1, n_shas, 0, e))?;
    }
    if repo_id == 0 {
        if ctx.debug > 0 {
            printf!(
                "{db}/{repo}: no gha_events rows for this repo, skipping orphan commits restore\n"
            );
        }
        return Ok((1, n_shas, 0, Vec::new()));
    }

    let mut tx = con.begin().map_err(|e| (1, n_shas, 0, e.to_string()))?;

    let ins_event_sql = "
insert into gha_events(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name)
values($1,$2,$3,$4,$5,$6,$7)
on conflict do nothing
";
    let ins_payload_sql = "
insert into gha_payloads(event_id, size, ref, head, befor, action, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at)
values($1,$2,$3,$4,$5,'restored_orphan_commit',$6,$7,$8,$9,$10)
on conflict do nothing
";
    let ins_commit_sql = "
insert into gha_commits(
  sha, event_id, author_name, message,
  is_distinct, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at,
  author_id, committer_id, dup_author_login, dup_committer_login,
  author_email, committer_name, committer_email, origin
)
values($1,$2,$3,$4,true,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,2)
on conflict do nothing
";

    let mut n_restored = 0i64;
    for push in &pushes {
        // Commits of this push that can be restored: not in the database, with git metadata,
        // not restored by another clone meanwhile (claim late: an un-restorable alias must not
        // block a valid one).
        let mut todo: Vec<&str> = Vec::with_capacity(push.commits.len());
        for sha in &push.commits {
            if !restore_set.contains(sha.as_str()) {
                continue;
            }
            if !info_map.contains_key(sha) {
                if ctx.debug > 0 {
                    printf!("Warning: missing git metadata for {db}/{repo} sha {sha}\n");
                }
                continue;
            }
            todo.push(sha);
        }
        if todo.is_empty() {
            continue;
        }
        let Some(hi) = info_map.get(&push.head) else {
            if ctx.debug > 0 {
                printf!(
                    "Warning: missing git metadata for {db}/{repo} push head {}, skipping {} commits\n",
                    push.head,
                    todo.len()
                );
            }
            continue;
        };
        let claimed: Vec<&str> = {
            let mut guard = claimed_shas.lock().unwrap_or_else(|p| p.into_inner());
            todo.iter()
                .copied()
                .filter(|sha| guard.insert((*sha).to_string()))
                .collect()
        };
        if claimed.is_empty() {
            continue;
        }

        // The event actor: GHA's pusher - the committer of the step commit, or its author when
        // GitHub's web-flow identity committed it (UI merges); the legacy shape uses the author.
        let mut actor_name_raw = hi.committer_name.replace('\0', "");
        let mut actor_email_raw = hi.committer_email.replace('\0', "");
        if !ctx.orphan_commits_group
            || actor_email_raw
                .trim()
                .eq_ignore_ascii_case("noreply@github.com")
        {
            actor_name_raw = hi.author_name.replace('\0', "");
            actor_email_raw = hi.author_email.replace('\0', "");
        }
        let (actor_id, actor_login) = lookup_actor_name_email_cached_tx(
            ctx,
            &mut tx,
            acache,
            maybe_hide,
            &actor_name_raw,
            &actor_email_raw,
        );
        let dup_actor_login = if actor_login.is_empty() {
            actor_name_raw.clone()
        } else {
            actor_login.clone()
        };
        let dup_actor_login = trunc_to_bytes(&maybe_hide.hide(&dup_actor_login), 120);

        let mut created_at = push.created;
        let event_id = hash::negative_artificial_id(&["PushEvent", name, &push.head]);
        match orphan_event_check(&mut tx, event_id, name, &push.head) {
            Ok(EventCheck::Conflict) => continue,
            Ok(EventCheck::Exists(existing_dt)) => {
                // The same push is already there (a legacy one-event-per-commit row or an
                // earlier run) - its remaining commits join the existing event.
                created_at = existing_dt;
            }
            Ok(EventCheck::Free) => {
                if let Err(err) = tx.exec(
                    ins_event_sql,
                    &[
                        SqlArg::from(event_id),
                        SqlArg::from("PushEvent"),
                        SqlArg::from(actor_id),
                        SqlArg::from(repo_id),
                        SqlArg::from(created_at),
                        SqlArg::from(dup_actor_login.as_str()),
                        SqlArg::from(name),
                    ],
                ) {
                    printf!(
                        "Warning: insert gha_events failed (db={db}, repo={repo}, sha={}): {err}\n",
                        push.head
                    );
                    continue;
                }
                if let Err(err) = tx.exec(
                    ins_payload_sql,
                    &[
                        SqlArg::from(event_id),
                        SqlArg::from(push.size as i64),
                        SqlArg::from(push.ref_.as_str()),
                        SqlArg::from(push.head.as_str()),
                        SqlArg::from(push.before.as_str()),
                        SqlArg::from(dup_actor_login.as_str()),
                        SqlArg::from(repo_id),
                        SqlArg::from(name),
                        SqlArg::from("PushEvent"),
                        SqlArg::from(created_at),
                    ],
                ) {
                    printf!(
                        "Warning: insert gha_payloads failed (db={db}, repo={repo}, sha={}): {err}\n",
                        push.head
                    );
                    continue;
                }
            }
            Err(cerr) => {
                printf!(
                    "Warning: event id check failed (db={db}, repo={repo}, sha={}): {cerr}\n",
                    push.head
                );
                continue;
            }
        }
        if ctx.orphan_commits_group && ctx.debug > 0 {
            printf!(
                "{db}/{repo}: {} push {}: restoring {} of {} commits\n",
                branch_name(&push.ref_),
                push.head,
                claimed.len(),
                push.size
            );
        }

        let mut n_push = 0i64;
        for sha_norm in claimed {
            let ci = &info_map[sha_norm];

            let author_name_raw = ci.author_name.replace('\0', "");
            let author_email_raw = ci.author_email.replace('\0', "");
            let comm_name_raw = ci.committer_name.replace('\0', "");
            let comm_email_raw = ci.committer_email.replace('\0', "");
            let msg_raw = ci.message.replace('\0', "");

            let author_name = trunc_to_bytes(&maybe_hide.hide(&author_name_raw), 120);
            let author_email = trunc_to_bytes(&maybe_hide.hide(&author_email_raw), 160);
            let msg = trunc_to_bytes(&maybe_hide.hide(&msg_raw), 0xffff);

            let (author_id, author_login) = lookup_actor_name_email_cached_tx(
                ctx,
                &mut tx,
                acache,
                maybe_hide,
                &author_name_raw,
                &author_email_raw,
            );
            let (comm_id, comm_login) = lookup_actor_name_email_cached_tx(
                ctx,
                &mut tx,
                acache,
                maybe_hide,
                &comm_name_raw,
                &comm_email_raw,
            );

            let dup_author_login = if author_login.is_empty() {
                author_name_raw.clone()
            } else {
                author_login.clone()
            };
            let dup_author_login = trunc_to_bytes(&maybe_hide.hide(&dup_author_login), 120);

            let dup_comm_login = if comm_login.is_empty() {
                String::new()
            } else {
                trunc_to_bytes(&maybe_hide.hide(&comm_login), 120)
            };

            let comm_role_name = trunc_to_bytes(&maybe_hide.hide(&comm_name_raw), 160);
            let comm_role_email = trunc_to_bytes(&maybe_hide.hide(&comm_email_raw), 160);

            if let Err(err) = tx.exec(
                ins_commit_sql,
                &[
                    SqlArg::from(sha_norm),
                    SqlArg::from(event_id),
                    SqlArg::from(author_name.as_str()),
                    SqlArg::from(msg.as_str()),
                    SqlArg::from(actor_id),
                    SqlArg::from(dup_actor_login.as_str()),
                    SqlArg::from(repo_id),
                    SqlArg::from(name),
                    SqlArg::from("PushEvent"),
                    SqlArg::from(created_at),
                    SqlArg::from(author_id),
                    SqlArg::from(comm_id),
                    SqlArg::from(dup_author_login.as_str()),
                    SqlArg::from(dup_comm_login.as_str()),
                    SqlArg::from(author_email.as_str()),
                    SqlArg::from(comm_role_name.as_str()),
                    SqlArg::from(comm_role_email.as_str()),
                ],
            ) {
                printf!(
                    "Warning: insert gha_commits failed (db={db}, repo={repo}, sha={sha_norm}): {err}\n"
                );
                continue;
            }

            let ev = PushEvent {
                event_id,
                actor_id: 0,
                actor_login: String::new(),
                repo_id,
                repo_name: name.to_string(),
                created_at: created_at.fixed_offset(),
                head: String::new(),
                before: String::new(),
                ref_: String::new(),
                push_id: None,
                size: None,
                cnt: 0,
            };
            if INSERT_AUTHOR_ROLE {
                if let Err(err) = insert_roles(
                    &mut tx,
                    sha_norm,
                    &ev,
                    "Author",
                    author_id,
                    &author_login,
                    &trunc_to_bytes(&maybe_hide.hide(&author_name_raw), 160),
                    &trunc_to_bytes(&maybe_hide.hide(&author_email_raw), 160),
                    maybe_hide,
                ) {
                    printf!(
                        "Warning: insert Author role failed (db={db}, repo={repo}, sha={sha_norm}): {err}\n"
                    );
                }
            }
            if INSERT_COMMITTER_ROLE {
                if let Err(err) = insert_roles(
                    &mut tx,
                    sha_norm,
                    &ev,
                    "Committer",
                    comm_id,
                    &comm_login,
                    &comm_role_name,
                    &comm_role_email,
                    maybe_hide,
                ) {
                    printf!(
                        "Warning: insert Committer role failed (db={db}, repo={repo}, sha={sha_norm}): {err}\n"
                    );
                }
            }
            for tr in parse_trailers(ctx, &msg_raw) {
                let (t_id, t_login) = lookup_actor_name_email_cached_tx(
                    ctx, &mut tx, acache, maybe_hide, &tr.name, &tr.email,
                );
                if let Err(err) = insert_roles(
                    &mut tx,
                    sha_norm,
                    &ev,
                    &tr.role,
                    t_id,
                    &t_login,
                    &trunc_to_bytes(&maybe_hide.hide(&tr.name), 160),
                    &trunc_to_bytes(&maybe_hide.hide(&tr.email), 160),
                    maybe_hide,
                ) {
                    printf!(
                        "Warning: insert trailer role failed (db={db}, repo={repo}, sha={sha_norm}, role={}): {err}\n",
                        tr.role
                    );
                }
            }
            n_push += 1;
        }
        if n_push > 0 {
            n_restored += n_push;
            eids.push(event_id);
        }
    }

    if let Err(err) = tx.commit() {
        printf!("Error committing transaction for {db}/{repo}: {err}\n");
        // rolled back - none of the restored rows persisted, so no event ids to postprocess
        return Err((1, n_shas, 0, err.to_string()));
    }

    if ctx.debug > 0 {
        printf!("{db}/{repo}: successfully restored {n_restored} orphan commits\n");
    }

    Ok((1, n_shas, n_restored, eids))
}

/// Go `selectSkipCommits`: all `gha_skip_commits.sha` (normalized).
fn select_skip_commits(con: &PgConn) -> Result<HashSet<String>, PgError> {
    let mut out: HashSet<String> = HashSet::new();
    let mut rows = con.query("select sha from gha_skip_commits", &[])?;
    while rows.next() {
        let mut sha = String::new();
        rows.scan(&mut [&mut sha])?;
        out.insert(normalize_sha(&sha));
    }
    rows.err()?;
    let _ = rows.close();
    Ok(out)
}

/// Go `selectRepoAliases`: historical repo name -> current name for
/// repositories that were renamed.
///
/// Derived from data, not from `gha_repos.alias` (set once at project setup,
/// it goes stale after later renames): for every `gha_repos` (id, name) row
/// the newest native GHA event (0 < id < 2^48, so artificial rows written by
/// get_repos/ghapi2db never influence the choice; index
/// (repo_id, dup_repo_name, created_at)) is looked up, the current name of a
/// repository id is the name with the newest such event and every other name
/// of that id maps to it. A name that is current for one id and historical
/// for another one is ambiguous and never mapped.
fn select_repo_aliases(con: &PgConn) -> Result<HashMap<String, String>, PgError> {
    struct Named {
        name: String,
        last: Option<(DateTime<FixedOffset>, i64)>,
    }
    let mut out: HashMap<String, String> = HashMap::new();
    let mut rows = con.query(
        "select r.id, r.name, n.created_at, n.id from gha_repos r left join lateral (\
         select e.created_at, e.id from gha_events e where e.repo_id = r.id and e.dup_repo_name = r.name \
         and e.id > 0 and e.id < 281474976710656 order by e.created_at desc, e.id desc limit 1) n on true",
        &[],
    )?;
    let mut by_id: HashMap<i64, Vec<Named>> = HashMap::new();
    while rows.next() {
        let mut repo_id = 0i64;
        let mut name = String::new();
        let mut created_at: Option<DateTime<FixedOffset>> = None;
        let mut event_id: Option<i64> = None;
        rows.scan(&mut [&mut repo_id, &mut name, &mut created_at, &mut event_id])?;
        by_id.entry(repo_id).or_default().push(Named {
            name,
            last: created_at.map(|t| (t, event_id.unwrap_or(0))),
        });
    }
    rows.err()?;
    let _ = rows.close();

    let mut ambiguous: HashSet<String> = HashSet::new();
    for names in by_id.values() {
        if names.len() < 2 {
            continue;
        }
        let Some(current) = names
            .iter()
            .filter(|n| n.last.is_some())
            .max_by_key(|n| n.last)
        else {
            continue;
        };
        for n in names.iter().filter(|n| n.name != current.name) {
            if let Some(prev) = out.get(&n.name) {
                if prev != &current.name {
                    ambiguous.insert(n.name.clone());
                }
            }
            out.insert(n.name.clone(), current.name.clone());
        }
    }
    for (name, current) in &out {
        if out.contains_key(current) {
            // the current name of one id is a historical name of another one
            ambiguous.insert(name.clone());
            ambiguous.insert(current.clone());
        }
    }
    for name in &ambiguous {
        out.remove(name);
    }
    Ok(out)
}

/// Go `getRepoID`: `coalesce(max(repo_id), 0)` of the repo's events.
fn get_repo_id(con: &PgConn, repo_name: &str) -> Result<i64, String> {
    let mut repo_id = 0i64;
    con.query_row(
        "select coalesce(max(repo_id), 0) from gha_events where dup_repo_name = $1",
        &[SqlArg::from(repo_name)],
    )
    .scan(&mut [&mut repo_id])
    .map_err(|e| e.to_string())?;
    Ok(repo_id)
}

/// Result of [`git_list_commits`]: shas newest first, sha → author date (UTC).
type ListedCommits = (Vec<String>, HashMap<String, DateTime<Utc>>);

/// Go `gitListCommits`: `git log <ref> --format=%H %at --since=YYYY-MM-DD`.
fn git_list_commits(
    ctx: &Ctx,
    repo_path: &str,
    ref_: &str,
    since: DateTime<Utc>,
) -> Result<ListedCommits, String> {
    let args = vec![
        "git".to_string(),
        "-C".to_string(),
        repo_path.to_string(),
        "log".to_string(),
        ref_.to_string(),
        "--format=%H %at".to_string(),
        format!("--since={}", to_ymd_date(since)),
    ];

    let out = exec::exec_command(ctx, &args, &no_env()).map_err(|e| e.to_string())?;

    let mut shas: Vec<String> = Vec::new();
    let mut dates: HashMap<String, DateTime<Utc>> = HashMap::new();
    for line in out.split('\n') {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.is_empty() || !is_valid_non_zero_sha40(fields[0]) {
            continue;
        }
        shas.push(fields[0].to_string());
        if fields.len() > 1 {
            if let Ok(ts) = fields[1].parse::<i64>() {
                if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
                    dates.insert(normalize_sha(fields[0]), dt);
                }
            }
        }
    }

    Ok((shas, dates))
}

/// Go `gitListedCommits`: the legacy listing ([`git_list_commits`]: commits whose committer
/// date is inside the window, day granularity) as one-commit pushes: head = the commit,
/// created = its author date.
fn git_listed_commits(
    ctx: &Ctx,
    repo_path: &str,
    ref_: &str,
    since: DateTime<Utc>,
) -> Result<Vec<OrphanPush>, String> {
    let (shas, dates) = git_list_commits(ctx, repo_path, ref_, since)?;
    let mut pushes: Vec<OrphanPush> = Vec::with_capacity(shas.len());
    for sha in shas {
        let sha_norm = normalize_sha(&sha);
        let Some(&dt) = dates.get(&sha_norm) else {
            printf!("Warning: missing commit date for {repo_path} sha {sha_norm}, skipping\n");
            continue;
        };
        pushes.push(OrphanPush {
            head: sha_norm.clone(),
            before: String::new(),
            ref_: ref_.to_string(),
            created: dt,
            size: 1,
            commits: vec![sha_norm],
        });
    }
    Ok(pushes)
}

/// Go `gitOriginBranches`: `refs/remotes/origin/*` branches (sorted by name, `origin/HEAD`
/// excluded) whose tip's committer date is after `since`.
fn git_origin_branches(
    ctx: &Ctx,
    repo_path: &str,
    since: DateTime<Utc>,
) -> Result<Vec<OrphanBranch>, String> {
    let out = exec::exec_command(
        ctx,
        &[
            "git".to_string(),
            "-C".to_string(),
            repo_path.to_string(),
            "for-each-ref".to_string(),
            "--format=%(refname) %(committerdate:unix)".to_string(),
            "refs/remotes/origin/".to_string(),
        ],
        &no_env(),
    )
    .map_err(|e| e.to_string())?;
    let mut branches: Vec<OrphanBranch> = Vec::new();
    for line in out.split('\n') {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 2
            || fields[0] == "refs/remotes/origin/HEAD"
            || !fields[0].starts_with("refs/remotes/origin/")
        {
            continue;
        }
        let Ok(ts) = fields[1].parse::<i64>() else {
            continue;
        };
        if ts <= since.timestamp() {
            continue;
        }
        branches.push(OrphanBranch {
            ref_: fields[0].to_string(),
            name: branch_name(fields[0]).to_string(),
        });
    }
    Ok(branches)
}

/// Go `gitLandedCommits`: commits that became reachable from `branch` after `since`, grouped
/// by the first-parent step that brought them in (the GHA PushEvent shape). The boundary is
/// the branch's first-parent tip as of `since` (`git rev-list -1 --first-parent --before=…`);
/// when there is none (young repository) everything reachable from the branch landed inside
/// the window. Pushes are returned oldest first, commits inside a push in `git log` order
/// (newest first).
fn git_landed_commits(
    ctx: &Ctx,
    repo_path: &str,
    branch: &OrphanBranch,
    since: DateTime<Utc>,
) -> Result<Vec<OrphanPush>, String> {
    let git = |args: &[&str]| -> Result<String, String> {
        let mut argv = vec!["git".to_string(), "-C".to_string(), repo_path.to_string()];
        argv.extend(args.iter().map(|a| a.to_string()));
        exec::exec_command(ctx, &argv, &no_env()).map_err(|e| e.to_string())
    };

    let out = git(&[
        "rev-parse",
        "--verify",
        "--quiet",
        &format!("{}^{{commit}}", branch.ref_),
    ])?;
    let tip = normalize_sha(out.trim());
    if !is_valid_non_zero_sha40(&tip) {
        return Err(format!("cannot resolve {}: {:?}", branch.ref_, out.trim()));
    }

    let out = git(&[
        "rev-list",
        "-1",
        "--first-parent",
        &format!("--before=@{}", since.timestamp()),
        &branch.ref_,
    ])?;
    let boundary = normalize_sha(out.trim());
    let range_spec = if is_valid_non_zero_sha40(&boundary) {
        if boundary == tip {
            return Ok(Vec::new());
        }
        format!("{boundary}..{}", branch.ref_)
    } else {
        branch.ref_.clone()
    };

    let out = git(&["log", "--format=%H %P %ct", &range_spec])?;
    struct Landed {
        parents: Vec<String>,
        created: DateTime<Utc>,
        order: usize,
    }
    let mut commits: HashMap<String, Landed> = HashMap::new();
    for line in out.split('\n') {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 || !is_valid_non_zero_sha40(fields[0]) {
            continue;
        }
        let Ok(ts) = fields[fields.len() - 1].parse::<i64>() else {
            continue;
        };
        let Some(created) = Utc.timestamp_opt(ts, 0).single() else {
            continue;
        };
        let parents: Vec<String> = fields[1..fields.len() - 1]
            .iter()
            .filter(|p| is_valid_non_zero_sha40(p))
            .map(|p| normalize_sha(p))
            .collect();
        let order = commits.len();
        commits.insert(
            normalize_sha(fields[0]),
            Landed {
                parents,
                created,
                order,
            },
        );
    }
    if !commits.contains_key(&tip) {
        return Ok(Vec::new());
    }

    // First-parent steps from the tip down to the boundary, oldest first.
    let mut steps: Vec<String> = Vec::new();
    let mut sha = tip.clone();
    while let Some(c) = commits.get(&sha) {
        steps.push(sha.clone());
        match c.parents.first() {
            Some(p) => sha = p.clone(),
            None => break,
        }
    }
    steps.reverse();

    // A commit belongs to the oldest step it is reachable from: `<previous step>..<step>`.
    let mut assigned: HashSet<String> = HashSet::with_capacity(commits.len());
    let mut pushes: Vec<OrphanPush> = Vec::with_capacity(steps.len());
    for step in &steps {
        let mut group: Vec<String> = Vec::new();
        let mut stack: Vec<String> = vec![step.clone()];
        while let Some(sha) = stack.pop() {
            let Some(c) = commits.get(&sha) else {
                continue;
            };
            if !assigned.insert(sha.clone()) {
                continue;
            }
            group.push(sha);
            stack.extend(c.parents.iter().cloned());
        }
        group.sort_by_key(|sha| commits[sha].order);
        let step_info = &commits[step];
        pushes.push(OrphanPush {
            head: step.clone(),
            before: step_info.parents.first().cloned().unwrap_or_default(),
            ref_: push_ref(&branch.ref_),
            created: step_info.created,
            size: group.len(),
            commits: group,
        });
    }
    Ok(pushes)
}

/// Go `gitDefaultRef`: `git symbolic-ref refs/remotes/origin/HEAD`.
fn git_default_ref(ctx: &Ctx, repo_path: &str) -> Result<String, String> {
    let out = exec::exec_command(
        ctx,
        &[
            "git".to_string(),
            "-C".to_string(),
            repo_path.to_string(),
            "symbolic-ref".to_string(),
            "refs/remotes/origin/HEAD".to_string(),
        ],
        &no_env(),
    )
    .map_err(|e| e.to_string())?;

    let ref_ = out.trim();
    let ref_ = ref_.strip_prefix("ref: ").unwrap_or(ref_);
    Ok(ref_.to_string())
}

/// Result of [`orphan_event_check`].
enum EventCheck {
    /// No row with this id yet.
    Free,
    /// The same push already has a row (its `created_at`).
    Exists(DateTime<Utc>),
    /// A different event owns the id (hash collision).
    Conflict,
}

/// Go `orphanEventCheck`: how `event_id` relates to the push (`repo`, `sha`): the same push
/// already has a row (an earlier run or a legacy one-event-per-commit row; its created_at is
/// returned so the remaining commits join it), or a different event owns the id (type, repo or
/// payload head differ). created_at is not part of the identity: the legacy shape stamped the
/// author date, the push shape stamps the landing time.
fn orphan_event_check(
    tx: &mut PgTx<'_>,
    event_id: i64,
    repo: &str,
    sha: &str,
) -> Result<EventCheck, PgError> {
    let mut e_type = String::new();
    let mut e_repo = String::new();
    let mut e_dt = DateTime::<FixedOffset>::default();
    let mut head: Option<String> = None;
    match tx
        .query_row(
            "select e.type, e.dup_repo_name, e.created_at, p.head from gha_events e left join gha_payloads p on p.event_id = e.id where e.id = $1",
            &[SqlArg::from(event_id)],
        )
        .scan(&mut [&mut e_type, &mut e_repo, &mut e_dt, &mut head])
    {
        Ok(()) => {}
        Err(PgError::NoRows) => return Ok(EventCheck::Free),
        Err(e) => return Err(e),
    }
    if e_type != "PushEvent" || e_repo != repo || head.as_deref() != Some(sha) {
        printf!(
            "orphan event id {event_id} conflict: existing ({e_type}, {e_repo}, {}), skipping\n",
            go_time_string(&e_dt)
        );
        return Ok(EventCheck::Conflict);
    }
    Ok(EventCheck::Exists(e_dt.with_timezone(&Utc)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_names_and_push_refs() {
        assert_eq!(branch_name("refs/remotes/origin/main"), "main");
        assert_eq!(
            branch_name("refs/remotes/origin/release-1.2"),
            "release-1.2"
        );
        assert_eq!(branch_name("refs/heads/main"), "main");
        assert_eq!(branch_name("HEAD"), "HEAD");
        assert_eq!(push_ref("refs/remotes/origin/main"), "refs/heads/main");
        assert_eq!(push_ref("HEAD"), "HEAD");
        assert_eq!(push_ref("refs/heads/x"), "refs/heads/x");
    }

    #[test]
    fn sha_helpers() {
        assert!(is_zero_sha(""));
        assert!(is_zero_sha("  "));
        assert!(is_zero_sha(ZERO_SHA40));
        assert!(is_zero_sha("000"));
        assert!(!is_zero_sha("0001"));
        assert!(is_valid_hex_sha40(&"a".repeat(40)));
        assert!(is_valid_hex_sha40(&"F".repeat(40)));
        assert!(!is_valid_hex_sha40(&"g".repeat(40)));
        assert!(!is_valid_hex_sha40(&"a".repeat(39)));
        assert!(is_valid_non_zero_sha40(&format!(" {} ", "b".repeat(40))));
        assert!(!is_valid_non_zero_sha40(ZERO_SHA40));
        assert_eq!(normalize_sha("  ABC "), "abc");
    }

    #[test]
    fn parse_git_commits_output_records() {
        let mut m = HashMap::new();
        // "John,Doe" <j@d>, committer "C" <c@d>, message "Fix\n\nSigned-off-by: A <a@b>"
        let out = format!(
            "{},{},{},{},{},{};\n{},{},{},{},{},{};",
            "a".repeat(40),
            "Sm9obixEb2U=",
            "akBk",
            "Qw==",
            "Y0Bk",
            "Rml4CgpTaWduZWQtb2ZmLWJ5OiBBIDxhQGI+",
            "b".repeat(40),
            "",
            "",
            "",
            "",
            ""
        );
        parse_git_commits_output(&out, &mut m).unwrap();
        assert_eq!(m.len(), 2);
        let a = &m["a".repeat(40).as_str()];
        assert_eq!(a.author_name, "John,Doe");
        assert_eq!(a.author_email, "j@d");
        assert_eq!(a.committer_name, "C");
        assert_eq!(a.committer_email, "c@d");
        assert_eq!(a.message, "Fix\n\nSigned-off-by: A <a@b>");
        let b = &m["b".repeat(40).as_str()];
        assert_eq!(b.author_name, "");
        assert_eq!(b.message, "");

        let mut m = HashMap::new();
        let err = parse_git_commits_output("abc,def", &mut m).unwrap_err();
        assert_eq!(
            err,
            "invalid git_commits.sh record (expected 6 fields): \"abc,def\""
        );
        let err = parse_git_commits_output(" ,a,b,c,d,e", &mut m).unwrap_err();
        assert_eq!(err, "empty sha in git_commits.sh record: \",a,b,c,d,e\"");
        let err = parse_git_commits_output("sha,!!,b,c,d,e", &mut m).unwrap_err();
        assert_eq!(
            err,
            "base64 decode author_name for sha: illegal base64 data at input byte 0"
        );
        assert!(parse_git_commits_output("  \n ", &mut m).is_ok());
    }

    #[test]
    fn commit_info_display_is_go_plus_v() {
        let ci = CommitInfo {
            sha: "s".into(),
            author_name: "a n".into(),
            author_email: "a@e".into(),
            committer_name: "c n".into(),
            committer_email: "c@e".into(),
            message: "m".into(),
        };
        assert_eq!(
            ci.to_string(),
            "{Sha:s AuthorName:a n AuthorEmail:a@e CommitterName:c n CommitterEmail:c@e Message:m AuthorDate:0001-01-01 00:00:00 +0000 UTC}"
        );
    }
}
