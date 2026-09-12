//! `sync_issues` — Rust port of `cmd/sync_issues/sync_issues.go`.
//!
//! Runs the SQL of `GHA2DB_ISSUES_SYNC_SQL` (after the `FROM<n>`/`TO<n>`
//! replacements) to get `repo, number` pairs, fetches each issue (and PR)
//! from the GitHub API with the rate-limit/abuse handling of the Go tool and
//! stores the current states as artificial `sync` events
//! (`SyncIssuesState`, manual mode). Environment, output and exit codes are
//! those of the Go program.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Mutex};
use std::time::{Duration, Instant};

use chrono::{Local, Utc};
use devstatscode::consts::{ABUSE, ISSUE_IS_DELETED, MOVED_PERMANENTLY, NOT_FOUND};
use devstatscode::ghapi::{
    fmt_slice, get_rate_limits, gh_client, handle_possible_error, sync_issues_state, IssueConfig,
    IssuesMap, PrsMap,
};
use devstatscode::github::{Client, IssueEvent, PullRequest, User};
use devstatscode::pg::api::query_sql_with_err;
use devstatscode::pg::PgConn;
use devstatscode::threads::get_threads_num;
use devstatscode::time::{format_go_duration, progress_info};
use devstatscode::{fatal_on_err, fatalf, printf, signal, Ctx};

/// The shared state of the fetching goroutines.
struct Shared<'a> {
    ctx: &'a Ctx,
    gcs: &'a [Client],
    issues: Mutex<IssuesMap>,
    prs: Mutex<PrsMap>,
    max_threads: usize,
    allowed_thr_n: AtomicUsize,
    /// The main loop's `nThreads` (only printed by the debug lines).
    n_threads: AtomicUsize,
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

/// The rate-limit gate before an API call: `Some(hint)` to proceed, `None`
/// to retry (after waiting); aborts when the wait would be too long.
fn rate_gate(sh: &Shared<'_>, tr: i64, what: &str, try_label: &str) -> Option<(usize, Vec<i64>)> {
    let ctx = sh.ctx;
    let (hint, _, rem, wait_period) = get_rate_limits(ctx, sh.gcs, true);
    if ctx.github_debug > 0 {
        printf!(
            "Get {} Try: {}, rem: {}, waitPeriod: {}, hint: {}\n",
            try_label,
            tr,
            fmt_slice(&rem),
            fmt_slice(&wait_period),
            hint
        );
    }
    if rem[hint] <= ctx.min_ghapi_points {
        if wait_period[hint].seconds() <= ctx.max_ghapi_wait_seconds as f64 {
            printf!(
                "API limit reached while getting {} data, waiting {} ({})\n",
                what,
                wait_period[hint],
                tr
            );
            std::thread::sleep(Duration::from_secs(1));
            wait_period[hint].sleep();
            return None;
        }
        fatalf!(
            "API limit reached while getting {} data, aborting, don't want to wait {}",
            what,
            wait_period[hint]
        );
    }
    Some((hint, rem))
}

/// The per-issue goroutine: fetch the issue (and its PR) and record them.
fn fetch_issue(sh: &Shared<'_>, org_repo: &str, number: i64) -> bool {
    let ctx = sh.ctx;
    let mut artificial_event = IssueEvent {
        actor: Some(User::id_login(-1, "devstats-sync")),
        ..Default::default()
    };
    let ary: Vec<&str> = org_repo.split('/').collect();
    if ary.len() < 2 {
        return false;
    }
    let org = ary[0];
    let repo = ary[1];
    if org.is_empty() || repo.is_empty() {
        return false;
    }
    let gcfg = IssueConfig {
        repo: org_repo.to_string(),
        ..Default::default()
    };
    let mut issue = None;
    let mut got = false;
    for tr in 0..ctx.max_ghapi_retry {
        let (hint, rem) = match rate_gate(sh, tr, "issue", "Issue") {
            Some(v) => v,
            None => continue,
        };
        if ctx.github_debug > 0 {
            printf!(
                "API call for Issue {} {}, remaining GHAPI points {}, hint: {}\n",
                org_repo,
                number,
                fmt_slice(&rem),
                hint
            );
        }
        let res = sh.gcs[hint].issues_get(org, repo, number);
        let (got_issue, err) = match res {
            Ok((i, _)) => (Some(i), None),
            Err(e) => (None, Some(e)),
        };
        issue = got_issue;
        let res = handle_possible_error(err.as_ref(), &gcfg.to_string(), "Issues.Get");
        if !res.is_empty() {
            if res == ABUSE {
                abuse_backoff(sh, tr, "issue");
            }
            if res == NOT_FOUND {
                printf!("Warning: not found: {}/{} {}\n", org, repo, number);
                return false;
            }
            if res == ISSUE_IS_DELETED {
                printf!("Warning: issue is deleted: {}/{} {}\n", org, repo, number);
                return false;
            }
            if res == MOVED_PERMANENTLY {
                printf!(
                    "Warning: This issue has been transferred: {}/{} {}\n",
                    org,
                    repo,
                    number
                );
                return false;
            }
            continue;
        }
        success_raise(sh, "issue");
        got = true;
        break;
    }
    if !got {
        fatalf!(
            "GetRateLimit call failed {} times while getting issue, aborting",
            ctx.max_ghapi_retry
        );
    }
    let issue = issue.expect("issue fetched");

    // Notice: If the issue number changes, it means that the issue has been transferred to a new repository.
    let curr_number = issue.get_number();
    let curr_repo = issue.get_repository();
    if curr_number != 0 && curr_number != number {
        printf!(
            "Warning: This issue has been transferred from {}/{}#{} to {}/{}#{}\n",
            org,
            repo,
            number,
            curr_repo.get_owner().get_name(),
            curr_repo.get_name(),
            curr_number
        );
        return false;
    }

    let mut cfg = IssueConfig {
        repo: org_repo.to_string(),
        ..Default::default()
    };
    cfg.milestone_id = issue.milestone.as_ref().and_then(|m| m.id);
    cfg.assignee_id = issue.assignee.as_ref().and_then(|a| a.id);
    cfg.event_type = "sync".to_string();
    cfg.created_at = Local::now().fixed_offset();
    cfg.number = issue.number.expect("issue without number");
    cfg.issue_id = issue.id.expect("issue without id");
    cfg.event_id = Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX) / 31622;
    artificial_event.id = Some(cfg.event_id);
    cfg.pr = issue.is_pull_request();
    cfg.set_labels_from(&issue.labels);
    cfg.set_assignees_from(&issue.assignees);
    let is_pr = issue.is_pull_request();
    cfg.gh_issue = Some(issue);
    cfg.gh_event = Some(artificial_event);
    let issue_id = cfg.issue_id;
    let cfg_str = cfg.to_string();
    let pr_num = cfg.number;
    {
        let mut issues = sh.issues.lock().unwrap_or_else(|p| p.into_inner());
        issues.entry(issue_id).or_default().push(cfg);
    }
    if ctx.debug > 0 {
        printf!("Processing {}\n", cfg_str);
    }

    // Handle PR
    if is_pr {
        let found_pr = sh
            .prs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&issue_id);
        if !found_pr {
            let mut pr: Option<PullRequest> = None;
            got = false;
            for tr in 0..ctx.max_ghapi_retry {
                let (hint, rem) = match rate_gate(sh, tr, "PR", "PR") {
                    Some(v) => v,
                    None => continue,
                };
                if ctx.github_debug > 0 {
                    printf!(
                        "API call for PR {} {}, remaining GHAPI points {}, hint: {}\n",
                        org_repo,
                        pr_num,
                        fmt_slice(&rem),
                        hint
                    );
                }
                let res = sh.gcs[hint].pull_requests_get(org, repo, pr_num);
                let (got_pr, err) = match res {
                    Ok((p, _)) => (Some(p), None),
                    Err(e) => (None, Some(e)),
                };
                pr = got_pr;
                let res =
                    handle_possible_error(err.as_ref(), &gcfg.to_string(), "PullRequests.Get");
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
                fatalf!(
                    "GetRateLimit call failed {} times while getting PR, aborting",
                    ctx.max_ghapi_retry
                );
            }
            if let Some(pr) = pr {
                sh.prs
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(issue_id, pr);
            }
        }
    }
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

/// Go `syncIssues`.
fn sync_issues(ctx: &mut Ctx) {
    // Connect to GitHub API
    let gcs = gh_client(ctx);

    // Connect to Postgres DB
    let con: PgConn = devstatscode::pg::pg_conn(ctx);

    // Get SQL that will return list of issue numbers to sync
    let mut sql = std::env::var("GHA2DB_ISSUES_SYNC_SQL").unwrap_or_default();
    if sql.is_empty() {
        printf!("You have to provide a SQL query to get a list of issue numbers to sync. Use GHA2DB_ISSUES_SYNC_SQL environment variable for this");
        fatalf!("no sync issues sql query provided");
    }
    let mut i = 1;
    loop {
        let from = std::env::var(format!("FROM{i}")).unwrap_or_default();
        let to = std::env::var(format!("TO{i}")).unwrap_or_default();
        if from.is_empty() {
            break;
        }
        sql = sql.replace(&from, &to);
        i += 1;
    }

    // Execute SQL
    let mut rows = query_sql_with_err(&con, ctx, &sql, &[]);
    let mut numbers: Vec<i64> = Vec::new();
    let mut repos: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    while rows.next() {
        let mut repo = String::new();
        let mut number: i64 = 0;
        fatal_on_err(rows.scan(&mut [&mut repo, &mut number]));
        let key = format!("{repo}:{number}");
        if seen.insert(key.clone()) {
            numbers.push(number);
            repos.push(repo);
        } else if ctx.debug > 0 {
            printf!("Duplicated issue: {}\n", key);
        }
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    let n_numbers = numbers.len();
    printf!(
        "sync_issues.go: Processing {} issues - GHAPI part\n",
        n_numbers
    );

    // Get number of CPUs available
    let thr_n = get_threads_num(ctx);
    // GitHub is not detecting abuse when using 16 threads, but it detects when using 32.
    let max_threads = 16.min(thr_n).max(1);
    let sh = Shared {
        ctx,
        gcs: &gcs,
        issues: Mutex::new(BTreeMap::new()),
        prs: Mutex::new(BTreeMap::new()),
        max_threads,
        allowed_thr_n: AtomicUsize::new(max_threads),
        n_threads: AtomicUsize::new(0),
    };
    let dt_start = Utc::now();
    let mut last_time = dt_start;
    let mut checked = 0usize;
    let period = Duration::from_secs(10);
    {
        let sh = &sh;
        let (tx, rx) = mpsc::channel::<bool>();
        std::thread::scope(|scope| {
            let mut n_threads = 0usize;
            for idx in 0..n_numbers {
                let tx = tx.clone();
                let repo = repos[idx].clone();
                let number = numbers[idx];
                scope.spawn(move || {
                    let res = fetch_issue(sh, &repo, number);
                    let _ = tx.send(res);
                });
                n_threads += 1;
                sh.n_threads.store(n_threads, Ordering::SeqCst);
                while n_threads >= sh.allowed_thr_n.load(Ordering::SeqCst) {
                    let _ = rx.recv();
                    n_threads -= 1;
                    sh.n_threads.store(n_threads, Ordering::SeqCst);
                    checked += 1;
                    let msg = progress_line(sh.ctx, sh.gcs);
                    progress_info(checked, n_numbers, dt_start, &mut last_time, period, &msg);
                }
            }
            if sh.ctx.debug > 0 {
                printf!("Final GHAPI threads join\n");
            }
            while n_threads > 0 {
                let _ = rx.recv();
                n_threads -= 1;
                sh.n_threads.store(n_threads, Ordering::SeqCst);
                checked += 1;
                let msg = progress_line(sh.ctx, sh.gcs);
                progress_info(checked, n_numbers, dt_start, &mut last_time, period, &msg);
            }
        });
    }
    let mut issues = sh.issues.into_inner().unwrap_or_else(|p| p.into_inner());
    let prs = sh.prs.into_inner().unwrap_or_else(|p| p.into_inner());

    // Do final corrections, manual sync: true
    sync_issues_state(&gcs, ctx, &con, &mut issues, &prs, true);
    con.close();
}

fn main() {
    devstatscode::error::exit_on_panic();
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    let dt_start = Instant::now();
    sync_issues(&mut ctx);
    printf!("Time: {}\n", format_go_duration(dt_start.elapsed()));
}
