//! Go `ghapi.go`: the GitHub API side of `sync_issues`/`ghapi2db` — issue
//! configurations, rate-limit probing across several clients, the error
//! classification, and the "artificial" API events (event id = 2^48 +
//! `EventID`) written into `gha_*` tables.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, Utc};

use crate::consts::{
    ABUSE, GHOST_ACTOR_ID, GHOST_ACTOR_LOGIN, HIDE_CFG_FILE, ISSUE_IS_DELETED, MOVED_PERMANENTLY,
    NOT_FOUND, NULL,
};
use crate::context::Ctx;
use crate::error::fatal_on_err;
use crate::github::{self, Client, Issue, IssueEvent, PullRequest, User};
use crate::gofmt;
use crate::log::printf;
use crate::pg::api::{
    bool_or_nil, exec_sql_tx_with_err, insert_actor_tx, insert_ignore, int_or_nil, n_value,
    n_values, query_sql_with_err, string_or_nil, trunc_string_or_nil,
};
use crate::pg::{PgConn, PgTx, SqlArg};
use crate::string::{get_hidden, maybe_hide_func};
use crate::threads::get_threads_num;
use crate::time::{period_parse, progress_info, to_ymdhms_date};

/// Artificial events live above 2^48 (Go `281474976710656 + cfg.EventID`).
pub const ARTIFICIAL_EVENT_BASE: i64 = 281_474_976_710_656;

/// A Go `time.Duration` (signed nanoseconds) — the rate-limit waits may be
/// negative when a reset time lies in the past.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct GoDuration(pub i64);

impl GoDuration {
    pub fn from_secs(s: i64) -> Self {
        GoDuration(s * 1_000_000_000)
    }
    /// Go `Duration.Seconds()`.
    pub fn seconds(&self) -> f64 {
        self.0 as f64 / 1e9
    }
    /// The `time.Sleep` of this duration (negative → returns at once).
    pub fn sleep(&self) {
        if self.0 > 0 {
            std::thread::sleep(Duration::from_nanos(self.0 as u64));
        }
    }
}

impl fmt::Display for GoDuration {
    /// Go `Duration.String()`, including negatives (`-1.5s`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = crate::time::format_go_duration(Duration::from_nanos(self.0.unsigned_abs()));
        if self.0 < 0 {
            write!(f, "-{s}")
        } else {
            write!(f, "{s}")
        }
    }
}

/// Go `%+v` of a slice: `[a b c]`.
pub fn fmt_slice<T: fmt::Display>(items: &[T]) -> String {
    gofmt::slice(items)
}

// ---------------------------------------------------------------------------
// IssueConfig
// ---------------------------------------------------------------------------

/// Go `IssueConfig` — an issue state at one moment (from an API event or a
/// manual sync).
#[derive(Clone, Debug, PartialEq)]
pub struct IssueConfig {
    pub repo: String,
    pub number: i64,
    pub issue_id: i64,
    pub pr: bool,
    pub milestone_id: Option<i64>,
    pub labels: String,
    pub labels_map: BTreeMap<i64, String>,
    pub gh_issue: Option<Issue>,
    pub created_at: DateTime<FixedOffset>,
    pub event_id: i64,
    pub event_type: String,
    pub gh_event: Option<IssueEvent>,
    pub assignee_id: Option<i64>,
    pub assignees: String,
    pub assignees_map: BTreeMap<i64, String>,
}

/// Go's zero `time.Time` (`0001-01-01 00:00:00 UTC`) — what an
/// `IssueConfig{Repo: …}` literal prints as `CreatedAt`.
pub fn go_zero_time() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z").unwrap()
}

impl Default for IssueConfig {
    fn default() -> Self {
        IssueConfig {
            repo: String::new(),
            number: 0,
            issue_id: 0,
            pr: false,
            milestone_id: None,
            labels: String::new(),
            labels_map: BTreeMap::new(),
            gh_issue: None,
            created_at: go_zero_time(),
            event_id: 0,
            event_type: String::new(),
            gh_event: None,
            assignee_id: None,
            assignees: String::new(),
            assignees_map: BTreeMap::new(),
        }
    }
}

impl fmt::Display for IssueConfig {
    /// Go `IssueConfig.String()` (maps print sorted by key like Go's fmt).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{Repo: {}, Number: {}, IssueID: {}, EventID: {}, EventType: {}, Pr: {}, MilestoneID: {}, AssigneeID: {}, CreatedAt: {}, Labels: {}, LabelsMap: {}, Assignees: {}, AssigneesMap: {}}}",
            self.repo,
            self.number,
            self.issue_id,
            self.event_id,
            self.event_type,
            self.pr,
            self.milestone_id.unwrap_or(0),
            self.assignee_id.unwrap_or(0),
            to_ymdhms_date(self.created_at),
            self.labels,
            gofmt::map(&self.labels_map),
            self.assignees,
            gofmt::map(&self.assignees_map),
        )
    }
}

impl IssueConfig {
    /// Go `configStr()`: the state-only rendering used to spot collisions.
    pub fn config_str(&self) -> String {
        format!(
            "{{Repo: {}, Number: {}, IssueID: {}, MilestoneID: {}, AssigneeID: {}, Labels: {}, Assignees: {}}}",
            self.repo,
            self.number,
            self.issue_id,
            self.milestone_id.unwrap_or(0),
            self.assignee_id.unwrap_or(0),
            self.labels,
            self.assignees,
        )
    }

    /// The issue (Go dereferences `GhIssue` unconditionally).
    pub fn issue(&self) -> &Issue {
        self.gh_issue
            .as_ref()
            .expect("IssueConfig without a GitHub issue")
    }

    /// Fill `labels`/`labels_map` from the issue labels (sorted ids joined
    /// by commas) — the loop every caller of `IssueConfig` repeats.
    pub fn set_labels_from(&mut self, labels: &[github::Label]) {
        self.labels_map.clear();
        for label in labels {
            let id = label.id.expect("label without id");
            let name = label.name.clone().expect("label without name");
            self.labels_map.insert(id, name);
        }
        self.labels = join_ids(self.labels_map.keys().copied());
    }

    /// Fill `assignees`/`assignees_map` from the issue assignees.
    pub fn set_assignees_from(&mut self, assignees: &[User]) {
        self.assignees_map.clear();
        for a in assignees {
            let id = a.id.expect("assignee without id");
            let login = a.login.clone().expect("assignee without login");
            self.assignees_map.insert(id, login);
        }
        self.assignees = join_ids(self.assignees_map.keys().copied());
    }
}

/// Sorted ids joined by commas (`"1,5,9"`), as `sync_issues`/`ghapi2db` build
/// their label/assignee strings.
pub fn join_ids<I: Iterator<Item = i64>>(ids: I) -> String {
    let mut v: Vec<i64> = ids.collect();
    v.sort_unstable();
    v.iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Go `IssueConfigAry`.
pub type IssueConfigAry = Vec<IssueConfig>;

/// Go `sort.Sort(IssueConfigAry)`: by issue id, creation time, event id.
pub fn sort_issue_configs(ary: &mut IssueConfigAry) {
    ary.sort_by(|a, b| {
        a.issue_id
            .cmp(&b.issue_id)
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.event_id.cmp(&b.event_id))
    });
}

/// Go `%v` of an `IssueConfigAry`: `[{…} {…}]`.
pub fn fmt_issue_configs(ary: &[IssueConfig]) -> String {
    gofmt::slice(ary)
}

/// Issues keyed by issue id (Go `map[int64]IssueConfigAry`).
pub type IssuesMap = BTreeMap<i64, IssueConfigAry>;
/// PRs keyed by issue id (Go `map[int64]github.PullRequest`).
pub type PrsMap = BTreeMap<i64, PullRequest>;

/// Go `outputIssuesInfo`: the summary of the issue states to process.
pub fn output_issues_info(issues: &IssuesMap, info: &str) {
    printf(&format!("{info}:\n"));
    let mut eids: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    let mut data: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for cfg_ary in issues.values() {
        for cfg in cfg_ary {
            let eid = cfg.event_id;
            let iid = cfg.issue().id.expect("issue without id");
            let e = eids.entry(eid).or_insert((iid, 0));
            *e = (iid, e.1 + 1);
            let key = format!("{} {}", cfg.repo, cfg.number);
            let val = format!("{} {}", to_ymdhms_date(cfg.created_at), cfg.event_type);
            data.entry(key).or_default().push(val);
        }
    }
    for (key, values) in &data {
        let mut svalues = values.clone();
        svalues.sort();
        printf(&format!("{}: [{}]\n", key, svalues.join(", ")));
    }
    for (eid, (iid, n)) in &eids {
        if *n > 1 {
            printf(&format!(
                "Warning: Duplicate event {}({}): {}\n",
                eid,
                n,
                fmt_issue_configs(issues.get(iid).map(|v| v.as_slice()).unwrap_or(&[]))
            ));
        }
    }
    for cfg_ary in issues.values() {
        let l = cfg_ary.len();
        for i in 0..l {
            for j in i + 1..l {
                let state_a = cfg_ary[i].config_str();
                let state_b = cfg_ary[j].config_str();
                if state_a != state_b {
                    printf(&format!("StateA: {state_a}\n"));
                    printf(&format!("StateB: {state_b}\n\n"));
                }
            }
        }
    }
}

/// Go `outputPRsInfo`.
pub fn output_prs_info(prs: &PrsMap, info: &str) {
    printf(&format!("{info}:\n"));
    let mut infos: Vec<String> = Vec::new();
    for (prid, pr) in prs {
        let full_name = pr
            .base
            .as_ref()
            .and_then(|b| b.repo.as_ref())
            .and_then(|r| r.full_name.as_ref());
        match (pr.number, full_name) {
            (Some(n), Some(f)) => infos.push(format!("{f} {n}")),
            _ => infos.push(format!("<{prid}>")),
        }
    }
    infos.sort();
    printf(&format!("PRs: {}\n", infos.join(", ")));
}

/// Go `outputInfo`: the gathered messages, keys and messages sorted.
pub fn output_info(infos: &BTreeMap<String, Vec<String>>, info: &str) {
    printf(&format!("{info}:\n"));
    for (key, msgs) in infos {
        let mut msgs = msgs.clone();
        msgs.sort();
        printf(&format!("{}:\n\t{}\n", key, msgs.join("\n\t")));
    }
}

fn add_info(infos: &Mutex<BTreeMap<String, Vec<String>>>, why: &str, what: String) {
    infos
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(why.to_string())
        .or_default()
        .push(what);
}

// ---------------------------------------------------------------------------
// Rate limits, clients, errors
// ---------------------------------------------------------------------------

/// Go `GetRateLimits`: the limits of every client (core or search) and the
/// index of the best one to use (most remaining points, then the shortest
/// wait). A client whose probe fails contributes `-1, -1` and either the
/// wait parsed from the error message or 5 s.
pub fn get_rate_limits(
    ctx: &Ctx,
    gcs: &[Client],
    core: bool,
) -> (usize, Vec<i64>, Vec<i64>, Vec<GoDuration>) {
    let mut limits: Vec<i64> = Vec::new();
    let mut remainings: Vec<i64> = Vec::new();
    let mut durations: Vec<GoDuration> = Vec::new();
    for (idx, gc) in gcs.iter().enumerate() {
        let rl = match gc.rate_limits() {
            Ok((rl, _)) => rl,
            Err(err) => {
                if let Some(rem) = period_parse(&err.to_string()) {
                    let rem = GoDuration(rem.as_nanos() as i64);
                    printf(&format!("Parsed wait time from error message: {rem}\n"));
                    limits.push(-1);
                    remainings.push(-1);
                    durations.push(rem);
                    continue;
                }
                printf(&format!("GetRateLimit({idx}): {err}\n"));
                None
            }
        };
        let rl = match rl {
            Some(rl) => rl,
            None => {
                limits.push(-1);
                remainings.push(-1);
                durations.push(GoDuration::from_secs(5));
                continue;
            }
        };
        // Go dereferences `rl.Core`/`rl.Search`; a missing resource reads as zero.
        let rate = if core { rl.core } else { rl.search }.unwrap_or_default();
        limits.push(rate.limit);
        remainings.push(rate.remaining);
        let until = rate.reset_time().signed_duration_since(Utc::now());
        durations.push(GoDuration(
            until
                .num_nanoseconds()
                .unwrap_or(i64::MAX)
                .saturating_add(1_000_000_000),
        ));
    }
    let mut hint = 0usize;
    for idx in 0..limits.len() {
        // More points left wins; ties go to the client whose limit resets
        // sooner.
        let better = remainings[idx] > remainings[hint]
            || (idx != hint
                && remainings[idx] == remainings[hint]
                && durations[idx] < durations[hint]);
        if better {
            hint = idx;
        }
    }
    if ctx.github_debug > 0 {
        printf(&format!(
            "GetRateLimits: hint: {}, limits: {}, remaining: {}, reset: {}\n",
            hint,
            fmt_slice(&limits),
            fmt_slice(&remainings),
            fmt_slice(&durations)
        ));
    }
    (hint, limits, remainings, durations)
}

/// Go `GHClient`: one client per token of `GHA2DB_GITHUB_OAUTH` (a path
/// containing `/` names a file holding the tokens; `-` is anonymous).
pub fn gh_client(ctx: &Ctx) -> Vec<Client> {
    let mut oauth = ctx.github_oauth.clone();
    if ctx.github_oauth.contains('/') {
        let bytes = fatal_on_err(crate::io::read_file(ctx, &ctx.github_oauth));
        oauth = String::from_utf8_lossy(&bytes).trim().to_string();
    }
    if oauth == "-" {
        return vec![Client::with_base_url(None, &ctx.github_api_url)];
    }
    oauth
        .split(',')
        .map(|t| Client::with_base_url(Some(t), &ctx.github_api_url))
        .collect()
}

/// Go `HandlePossibleError`: classify an API error; unknown errors end the
/// process with status 0 after `<argv0> error: <type>:<error>, non fatal,
/// exiting 0 status`. Returns `""` when there is no error.
pub fn handle_possible_error(err: Option<&github::Error>, cfg: &str, info: &str) -> String {
    let err = match err {
        None => return String::new(),
        Some(e) => e,
    };
    if err.is_rate_limit() {
        printf(&format!("Rate limit ({info}) for {cfg}\n"));
        return "rate".to_string();
    }
    if err.is_abuse() {
        printf(&format!("Abuse detected ({info}) for {cfg}\n"));
        return ABUSE.to_string();
    }
    let err_str = err.to_string();
    if err_str.contains("410 This issue was deleted") {
        printf(&format!("Issue was deleted ({info}) for {cfg}: {err}\n"));
        return ISSUE_IS_DELETED.to_string();
    } else if err_str.contains("404 Not Found") {
        printf(&format!("Not found ({info}) for {cfg}: {err}\n"));
        return NOT_FOUND.to_string();
    } else if err_str.contains("502 Server Error") {
        printf(&format!("Server Error ({info}) for {cfg}: {err}\n"));
        return "server_error".to_string();
    } else if err_str.contains("409 Git Repository is empty") {
        printf(&format!("Git repository empty ({info}) for {cfg}: {err}\n"));
        return NOT_FOUND.to_string();
    } else if err_str.contains("301") {
        printf(&format!("Moved Permanently ({info}) for {cfg}: {err}\n"));
        return MOVED_PERMANENTLY.to_string();
    }
    let argv0 = std::env::args().next().unwrap_or_default();
    printf(&format!(
        "{} error: {}:{}, non fatal, exiting 0 status\n",
        argv0,
        err.go_type(),
        err
    ));
    std::process::exit(0);
}

// ---------------------------------------------------------------------------
// Actors, milestones
// ---------------------------------------------------------------------------

/// Go `ghActorIDOrNil`.
pub fn gh_actor_id_or_nil(actor: Option<&User>) -> SqlArg {
    match actor {
        None => SqlArg::Null,
        Some(a) => int_or_nil(a.id),
    }
}

/// Go `ghActorLoginOrNil`.
pub fn gh_actor_login_or_nil(actor: Option<&User>, maybe_hide: &dyn Fn(&str) -> String) -> SqlArg {
    match actor.and_then(|a| a.login.as_deref()) {
        None => SqlArg::Null,
        Some(login) => SqlArg::Str(maybe_hide(login)),
    }
}

/// Go `ghMilestoneIDOrNil`.
pub fn gh_milestone_id_or_nil(milestone: Option<&github::Milestone>) -> SqlArg {
    match milestone {
        None => SqlArg::Null,
        Some(m) => int_or_nil(m.id),
    }
}

/// `Option<&time>` → SQL argument (Go passes `*time.Time` straight through).
fn gotime_or_nil(t: Option<&github::GoTime>) -> SqlArg {
    match t {
        None => SqlArg::Null,
        Some(t) => SqlArg::Time(t.0),
    }
}

fn actor_complete(actor: Option<&User>) -> bool {
    matches!(actor, Some(a) if a.id.is_some() && a.login.is_some())
}

/// Go `ghEnsureEventActor`: events performed by deleted accounts come without
/// an actor — reassign them to GitHub's `ghost` placeholder (id 10137).
pub fn gh_ensure_event_actor(cfg: &mut IssueConfig) {
    let event = match cfg.gh_event.as_mut() {
        None => return,
        Some(e) => e,
    };
    if actor_complete(event.actor.as_ref()) {
        return;
    }
    printf(&format!(
        "Warning: event {} for {} #{} ({}, {}) has no actor (deleted account?), reassigning to '{}' (id {})\n",
        cfg.event_id,
        cfg.repo,
        cfg.number,
        cfg.event_type,
        to_ymdhms_date(cfg.created_at),
        GHOST_ACTOR_LOGIN,
        GHOST_ACTOR_ID
    ));
    event.actor = Some(User::id_login(GHOST_ACTOR_ID, GHOST_ACTOR_LOGIN));
}

/// Go `ghActor`: insert one GitHub user (skipped without a login).
pub fn gh_actor(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    actor: Option<&User>,
    maybe_hide: &dyn Fn(&str) -> String,
) {
    let actor = match actor {
        Some(a) if a.login.is_some() => a,
        _ => return,
    };
    insert_actor_tx(
        tx,
        ctx,
        int_or_nil(actor.id),
        &maybe_hide(actor.login.as_deref().unwrap_or("")),
        "",
    );
}

/// Go `ghMilestone`: insert `milestone` (the issue's one for artificial issue
/// events, the PR's one for artificial PR events — the two API payloads are
/// fetched separately and can differ) as the milestone state of the event.
pub fn gh_milestone(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    eid: i64,
    ic: &mut IssueConfig,
    milestone: &github::Milestone,
    maybe_hide: &dyn Fn(&str) -> String,
) {
    gh_ensure_event_actor(ic);
    let ev = ic.gh_event.as_ref().expect("ghMilestone without an event");
    let actor = ev.actor.as_ref().expect("event without actor");
    let query = insert_ignore(&format!(
        "into gha_milestones(id, event_id, closed_at, closed_issues, created_at, creator_id, \
         description, due_on, number, open_issues, state, title, updated_at, \
         dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
         dupn_creator_login) values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
         (select coalesce(max(repo_id), -1) from gha_events where dup_repo_name = {}), {}, {}, {}, {})",
        n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7),
        n_value(8), n_value(9), n_value(10), n_value(11), n_value(12), n_value(13), n_value(14),
        n_value(15), n_value(16), n_value(17), n_value(18), n_value(19), n_value(20),
    ));
    let args = [
        int_or_nil(milestone.id),
        SqlArg::Int(eid),
        gotime_or_nil(milestone.closed_at.as_ref()),
        int_or_nil(milestone.closed_issues),
        gotime_or_nil(milestone.created_at.as_ref()),
        gh_actor_id_or_nil(milestone.creator.as_ref()),
        trunc_string_or_nil(milestone.description.as_deref(), 0xffff),
        gotime_or_nil(milestone.due_on.as_ref()),
        int_or_nil(milestone.number),
        int_or_nil(milestone.open_issues),
        string_or_nil_raw(milestone.state.as_deref()),
        trunc_string_or_nil(milestone.title.as_deref(), 200),
        gotime_or_nil(milestone.updated_at.as_ref()),
        int_or_nil(actor.id),
        SqlArg::Str(maybe_hide(
            actor.login.as_deref().expect("actor without login"),
        )),
        SqlArg::from(&ic.repo),
        SqlArg::from(&ic.repo),
        SqlArg::from(&ic.event_type),
        SqlArg::Time(ic.created_at),
        gh_actor_login_or_nil(milestone.creator.as_ref(), maybe_hide),
    ];
    exec_sql_tx_with_err(tx, ctx, &query, &args);
}

/// A `*string` passed straight to `database/sql` (no NUL cleaning).
fn string_or_nil_raw(v: Option<&str>) -> SqlArg {
    match v {
        None => SqlArg::Null,
        Some(s) => SqlArg::Str(s.to_string()),
    }
}

/// Go `GetRecentRepos`: repos with events after `dt_from`.
pub fn get_recent_repos(
    con: &PgConn,
    ctx: &Ctx,
    dt_from: DateTime<Utc>,
) -> (Vec<String>, Vec<i64>) {
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!(
            "select distinct repo_id, dup_repo_name from gha_events where created_at > {}",
            n_value(1)
        ),
        &[SqlArg::from(dt_from)],
    );
    let mut repos = Vec::new();
    let mut rids = Vec::new();
    while rows.next() {
        let mut rid: i64 = 0;
        let mut repo = String::new();
        fatal_on_err(rows.scan(&mut [&mut rid, &mut repo]));
        repos.push(repo);
        rids.push(rid);
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    (repos, rids)
}

// ---------------------------------------------------------------------------
// Artificial events
// ---------------------------------------------------------------------------

/// Go `DeleteArtificialPREvent`: drop the PR rows of the artificial event.
pub fn delete_artificial_pr_event(con: &PgConn, ctx: &Ctx, cfg: &IssueConfig) {
    if ctx.skip_pdb {
        if ctx.debug > 0 {
            printf(&format!("No DB write: Delete PR '{cfg}'\n"));
        }
        return;
    }
    let eid = ARTIFICIAL_EVENT_BASE + cfg.event_id;
    let condition = format!(" where event_id = {eid}");
    let deletes = [
        format!("delete from gha_pull_requests{condition}"),
        format!("delete from gha_pull_requests_assignees{condition}"),
        format!("delete from gha_pull_requests_requested_reviewers{condition}"),
    ];
    let mut tx = fatal_on_err(con.begin());
    for del in &deletes {
        exec_sql_tx_with_err(&mut tx, ctx, del, &[]);
    }
    fatal_on_err(tx.commit());
}

/// Go `DeleteArtificialEvent`: drop every row of the artificial event.
pub fn delete_artificial_event(con: &PgConn, ctx: &Ctx, cfg: &IssueConfig) {
    if ctx.skip_pdb {
        if ctx.debug > 0 {
            printf(&format!("No DB write: Delete Issue '{cfg}'\n"));
        }
        return;
    }
    let eid = ARTIFICIAL_EVENT_BASE + cfg.event_id;
    let condition = format!(" where event_id = {eid}");
    let deletes = [
        format!("delete from gha_issues_labels{condition}"),
        format!("delete from gha_issues_assignees{condition}"),
        format!("delete from gha_issues{condition}"),
        format!("delete from gha_milestones{condition}"),
        format!("delete from gha_payloads{condition}"),
        format!("delete from gha_pull_requests{condition}"),
        format!("delete from gha_pull_requests_assignees{condition}"),
        format!("delete from gha_pull_requests_requested_reviewers{condition}"),
        format!("delete from gha_events where id = {eid}"),
    ];
    let mut tx = fatal_on_err(con.begin());
    for del in &deletes {
        exec_sql_tx_with_err(&mut tx, ctx, del, &[]);
    }
    fatal_on_err(tx.commit());
}

/// The `gha_events` insert shared by both artificial event kinds.
fn insert_artificial_gha_event(
    tx: &mut PgTx<'_>,
    ctx: &Ctx,
    event_id: i64,
    cfg: &IssueConfig,
    actor: Option<&User>,
    created_at: DateTime<FixedOffset>,
    maybe_hide: &dyn Fn(&str) -> String,
) {
    let query = insert_ignore(&format!(
        "into gha_events(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name, org_id) \
         values({}, {}, {}, (select coalesce(max(repo_id), -1) from gha_events where dup_repo_name = {}), {}, \
         {}, {}, (select max(org_id) from gha_events where dup_repo_name = {}))",
        n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7), n_value(8),
    ));
    let args = [
        SqlArg::Int(event_id),
        SqlArg::from(&cfg.event_type),
        gh_actor_id_or_nil(actor),
        SqlArg::from(&cfg.repo),
        SqlArg::Time(created_at),
        gh_actor_login_or_nil(actor, maybe_hide),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.repo),
    ];
    exec_sql_tx_with_err(tx, ctx, &query, &args);
}

/// Go `ArtificialPREvent`: store the PR state as an artificial event.
pub fn artificial_pr_event(con: &PgConn, ctx: &Ctx, cfg: &mut IssueConfig, pr: &PullRequest) {
    if ctx.skip_pdb {
        if ctx.debug > 0 {
            printf(&format!("No DB write: PR '{cfg}'\n"));
        }
        return;
    }
    let hidden = get_hidden(ctx, HIDE_CFG_FILE);
    let maybe_hide = maybe_hide_func(hidden);

    let event_id = ARTIFICIAL_EVENT_BASE + cfg.event_id;
    let e_type = cfg.event_type.clone();
    let e_created_at = cfg.created_at;
    let iid = cfg.issue().id.expect("issue without id");
    let event_actor = cfg.gh_event.as_ref().and_then(|e| e.actor.as_ref());
    if ctx.allow_ghapi_insert_fail && !actor_complete(event_actor) {
        printf(&format!(
            "Warning: GHA2DB_GHAPIALLOWINSERTFAIL: skipped artificial PR event for {} {} ({}, {}): event has no actor\n",
            cfg.repo,
            cfg.number,
            cfg.event_type,
            to_ymdhms_date(cfg.created_at)
        ));
        return;
    }
    gh_ensure_event_actor(cfg);
    let actor = cfg
        .gh_event
        .as_ref()
        .and_then(|e| e.actor.clone())
        .expect("event without actor");

    let mut tx = fatal_on_err(con.begin());

    gh_actor(&mut tx, ctx, Some(&actor), &maybe_hide);
    gh_actor(&mut tx, ctx, pr.user.as_ref(), &maybe_hide);

    let base_sha = pr
        .base
        .as_ref()
        .and_then(|b| b.sha.clone())
        .unwrap_or_default();
    let head_sha = pr
        .head
        .as_ref()
        .and_then(|b| b.sha.clone())
        .unwrap_or_default();

    if pr.merged_by.is_some() {
        gh_actor(&mut tx, ctx, pr.merged_by.as_ref(), &maybe_hide);
    }
    if pr.assignee.is_some() {
        gh_actor(&mut tx, ctx, pr.assignee.as_ref(), &maybe_hide);
    }
    if let Some(m) = pr.milestone.as_ref() {
        gh_milestone(&mut tx, ctx, event_id, cfg, m, &maybe_hide);
    }

    let prid = pr.id.expect("PR without id");
    let query = insert_ignore(&format!(
        "into gha_pull_requests(id, event_id, user_id, base_sha, head_sha, merged_by_id, assignee_id, milestone_id, \
         number, state, title, body, created_at, updated_at, closed_at, merged_at, \
         merge_commit_sha, merged, mergeable, mergeable_state, comments, \
         maintainer_can_modify, commits, additions, deletions, changed_files, \
         dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
         dup_user_login, dupn_merged_by_login) values({}, {}, {}, {}, {}, {}, {}, {}, \
         {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
         {}, {}, (select coalesce(max(repo_id), -1) from gha_events where dup_repo_name = {}), {}, {}, {}, {}, {})",
        n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7), n_value(8),
        n_value(9), n_value(10), n_value(11), n_value(12), n_value(13), n_value(14), n_value(15), n_value(16),
        n_value(17), n_value(18), n_value(19), n_value(20), n_value(21), n_value(22), n_value(23), n_value(24),
        n_value(25), n_value(26), n_value(27), n_value(28), n_value(29), n_value(30), n_value(31), n_value(32),
        n_value(33), n_value(34),
    ));
    let args = [
        SqlArg::Int(prid),
        SqlArg::Int(event_id),
        gh_actor_id_or_nil(pr.user.as_ref()),
        SqlArg::Str(base_sha),
        SqlArg::Str(head_sha),
        gh_actor_id_or_nil(pr.merged_by.as_ref()),
        gh_actor_id_or_nil(pr.assignee.as_ref()),
        gh_milestone_id_or_nil(pr.milestone.as_ref()),
        int_or_nil(pr.number),
        string_or_nil_raw(pr.state.as_deref()),
        string_or_nil_raw(pr.title.as_deref()),
        trunc_string_or_nil(pr.body.as_deref(), 0xffff),
        gotime_or_nil(pr.created_at.as_ref()),
        gotime_or_nil(pr.updated_at.as_ref()),
        gotime_or_nil(pr.closed_at.as_ref()),
        gotime_or_nil(pr.merged_at.as_ref()),
        string_or_nil(pr.merge_commit_sha.as_deref()),
        bool_or_nil(pr.merged),
        bool_or_nil(pr.mergeable),
        string_or_nil(pr.mergeable_state.as_deref()),
        int_or_nil(pr.comments),
        bool_or_nil(pr.maintainer_can_modify),
        int_or_nil(pr.commits),
        int_or_nil(pr.additions),
        int_or_nil(pr.deletions),
        int_or_nil(pr.changed_files),
        int_or_nil(actor.id),
        gh_actor_login_or_nil(Some(&actor), &maybe_hide),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&e_type),
        SqlArg::Time(e_created_at),
        gh_actor_login_or_nil(pr.user.as_ref(), &maybe_hide),
        gh_actor_login_or_nil(pr.merged_by.as_ref(), &maybe_hide),
    ];
    exec_sql_tx_with_err(&mut tx, ctx, &query, &args);

    insert_artificial_gha_event(
        &mut tx,
        ctx,
        event_id,
        cfg,
        Some(&actor),
        e_created_at,
        &maybe_hide,
    );

    let query = insert_ignore(&format!(
        "into gha_payloads(event_id, push_id, size, ref, head, befor, action, \
         issue_id, pull_request_id, comment_id, commit, number, forkee_id, release_id, member_id, \
         dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) \
         values({}, null, null, null, null, null, {}, {}, {}, null, null, {}, null, null, null, \
         {}, (select coalesce(max(repo_id), -1) from gha_events where dup_repo_name = {}), {}, {}, {})",
        n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7), n_value(8),
        n_value(9), n_value(10),
    ));
    let issue_number = int_or_nil(cfg.issue().number);
    let args = [
        SqlArg::Int(event_id),
        SqlArg::from(&cfg.event_type),
        SqlArg::Int(iid),
        SqlArg::Int(prid),
        issue_number,
        gh_actor_login_or_nil(Some(&actor), &maybe_hide),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.event_type),
        SqlArg::Time(e_created_at),
    ];
    exec_sql_tx_with_err(&mut tx, ctx, &query, &args);

    exec_sql_tx_with_err(
        &mut tx,
        ctx,
        &format!(
            "update gha_payloads set pull_request_id = {} where issue_id = {} and event_id = {}",
            n_value(1),
            n_value(2),
            n_value(3)
        ),
        &[SqlArg::Int(prid), SqlArg::Int(iid), SqlArg::Int(event_id)],
    );

    for assignee in &pr.assignees {
        gh_actor(&mut tx, ctx, Some(assignee), &maybe_hide);
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &insert_ignore(&format!(
                "into gha_pull_requests_assignees(pull_request_id, event_id, assignee_id) {}",
                n_values(3)
            )),
            &[
                SqlArg::Int(prid),
                SqlArg::Int(event_id),
                int_or_nil(assignee.id),
            ],
        );
    }
    for reviewer in &pr.requested_reviewers {
        gh_actor(&mut tx, ctx, Some(reviewer), &maybe_hide);
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &insert_ignore(&format!(
                "into gha_pull_requests_requested_reviewers(pull_request_id, event_id, requested_reviewer_id) {}",
                n_values(3)
            )),
            &[SqlArg::Int(prid), SqlArg::Int(event_id), int_or_nil(reviewer.id)],
        );
    }
    fatal_on_err(tx.commit());
}

/// Go `ArtificialEvent`: store the issue state as an artificial event.
pub fn artificial_event(con: &PgConn, ctx: &Ctx, cfg: &mut IssueConfig) {
    if ctx.skip_pdb {
        if ctx.debug > 0 {
            printf(&format!("No DB write: Issue '{cfg}'\n"));
        }
        return;
    }
    let eid = cfg.event_id;
    let iid = cfg.issue_id;
    let event_id = ARTIFICIAL_EVENT_BASE + eid;
    let now = cfg.created_at;

    let hidden = get_hidden(ctx, HIDE_CFG_FILE);
    let maybe_hide = maybe_hide_func(hidden);

    let event_actor = cfg.gh_event.as_ref().and_then(|e| e.actor.as_ref());
    if ctx.allow_ghapi_insert_fail && !actor_complete(event_actor) {
        printf(&format!(
            "Warning: GHA2DB_GHAPIALLOWINSERTFAIL: skipped artificial event for {} {} ({}, {}): event has no actor\n",
            cfg.repo,
            cfg.number,
            cfg.event_type,
            to_ymdhms_date(cfg.created_at)
        ));
        return;
    }
    gh_ensure_event_actor(cfg);
    let issue = cfg.issue().clone();
    let actor = cfg.gh_event.as_ref().and_then(|e| e.actor.clone());

    let mut tx = fatal_on_err(con.begin());

    gh_actor(&mut tx, ctx, actor.as_ref(), &maybe_hide);
    gh_actor(&mut tx, ctx, issue.assignee.as_ref(), &maybe_hide);
    gh_actor(&mut tx, ctx, issue.user.as_ref(), &maybe_hide);
    for assignee in &issue.assignees {
        gh_actor(&mut tx, ctx, Some(assignee), &maybe_hide);
    }
    if let Some(m) = &issue.milestone {
        gh_actor(&mut tx, ctx, m.creator.as_ref(), &maybe_hide);
    }

    let query = insert_ignore(&format!(
        "into gha_issues(id, event_id, assignee_id, body, closed_at, comments, created_at, \
         locked, milestone_id, number, state, title, updated_at, user_id, \
         dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, \
         dup_user_login, is_pull_request) values({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
         {}, {}, (select coalesce(max(repo_id), -1) from gha_events where dup_repo_name = {}), {}, {}, {}, {}, {}) ",
        n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7), n_value(8),
        n_value(9), n_value(10), n_value(11), n_value(12), n_value(13), n_value(14), n_value(15), n_value(16),
        n_value(17), n_value(18), n_value(19), n_value(20), n_value(21), n_value(22),
    ));
    let args = [
        SqlArg::Int(iid),
        SqlArg::Int(event_id),
        gh_actor_id_or_nil(issue.assignee.as_ref()),
        trunc_string_or_nil(issue.body.as_deref(), 0xffff),
        gotime_or_nil(issue.closed_at.as_ref()),
        int_or_nil(issue.comments),
        gotime_or_nil(issue.created_at.as_ref()),
        bool_or_nil(issue.locked),
        gh_milestone_id_or_nil(issue.milestone.as_ref()),
        int_or_nil(issue.number),
        string_or_nil_raw(issue.state.as_deref()),
        string_or_nil_raw(issue.title.as_deref()),
        SqlArg::Time(now),
        gh_actor_id_or_nil(issue.user.as_ref()),
        gh_actor_id_or_nil(actor.as_ref()),
        gh_actor_login_or_nil(actor.as_ref(), &maybe_hide),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.event_type),
        SqlArg::Time(now),
        gh_actor_login_or_nil(issue.user.as_ref(), &maybe_hide),
        SqlArg::Bool(issue.is_pull_request()),
    ];
    exec_sql_tx_with_err(&mut tx, ctx, &query, &args);

    if let Some(m) = issue.milestone.as_ref() {
        gh_milestone(&mut tx, ctx, event_id, cfg, m, &maybe_hide);
    }

    insert_artificial_gha_event(
        &mut tx,
        ctx,
        event_id,
        cfg,
        actor.as_ref(),
        now,
        &maybe_hide,
    );

    let query = insert_ignore(&format!(
        "into gha_payloads(event_id, push_id, size, ref, head, befor, action, \
         issue_id, pull_request_id, comment_id, commit, number, forkee_id, release_id, member_id, \
         dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) \
         values({}, null, null, null, null, null, {}, {}, null, null, null, {}, null, null, null, \
         {}, (select coalesce(max(repo_id), -1) from gha_events where dup_repo_name = {}), {}, {}, {})",
        n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7), n_value(8),
        n_value(9),
    ));
    let args = [
        SqlArg::Int(event_id),
        SqlArg::from(&cfg.event_type),
        SqlArg::Int(iid),
        int_or_nil(issue.number),
        gh_actor_login_or_nil(actor.as_ref(), &maybe_hide),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.repo),
        SqlArg::from(&cfg.event_type),
        SqlArg::Time(now),
    ];
    exec_sql_tx_with_err(&mut tx, ctx, &query, &args);

    for (label_id, label_name) in &cfg.labels_map {
        let query = insert_ignore(&format!(
            "into gha_issues_labels(issue_id, event_id, label_id, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, \
             dup_type, dup_created_at, dup_issue_number, dup_label_name) values({}, {}, {}, {}, {}, \
             (select coalesce(max(repo_id), -1) from gha_events where dup_repo_name = {}), {}, {}, {}, {}, {})",
            n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7), n_value(8),
            n_value(9), n_value(10), n_value(11),
        ));
        let args = [
            SqlArg::Int(iid),
            SqlArg::Int(event_id),
            SqlArg::Int(*label_id),
            gh_actor_id_or_nil(actor.as_ref()),
            gh_actor_login_or_nil(actor.as_ref(), &maybe_hide),
            SqlArg::from(&cfg.repo),
            SqlArg::from(&cfg.repo),
            SqlArg::from(&cfg.event_type),
            SqlArg::Time(now),
            int_or_nil(issue.number),
            SqlArg::from(label_name),
        ];
        exec_sql_tx_with_err(&mut tx, ctx, &query, &args);
    }

    for assignee_id in cfg.assignees_map.keys() {
        exec_sql_tx_with_err(
            &mut tx,
            ctx,
            &insert_ignore(&format!(
                "into gha_issues_assignees(issue_id, event_id, assignee_id) values({}, {}, {})",
                n_value(1),
                n_value(2),
                n_value(3)
            )),
            &[
                SqlArg::Int(iid),
                SqlArg::Int(event_id),
                SqlArg::Int(*assignee_id),
            ],
        );
    }
    fatal_on_err(tx.commit());
}

// ---------------------------------------------------------------------------
// SyncIssuesState
// ---------------------------------------------------------------------------

struct Counters {
    updates: Mutex<[i64; 3]>,
    infos: Mutex<BTreeMap<String, Vec<String>>>,
}

impl Counters {
    fn new() -> Self {
        Counters {
            updates: Mutex::new([0, 0, 0]),
            infos: Mutex::new(BTreeMap::new()),
        }
    }
    fn bump(&self, idx: usize) {
        self.updates.lock().unwrap_or_else(|p| p.into_inner())[idx] += 1;
    }
    fn info(&self, why: &str, what: String) {
        add_info(&self.infos, why, what);
    }
    fn take_infos(&self) -> BTreeMap<String, Vec<String>> {
        std::mem::take(&mut *self.infos.lock().unwrap_or_else(|p| p.into_inner()))
    }
    fn updates(&self) -> [i64; 3] {
        *self.updates.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// `"<what>"` for the manual mode, `"<what> <date> <type>"` otherwise.
fn what_for(cfg: &IssueConfig, manual: bool) -> String {
    if manual {
        format!("{} {}", cfg.repo, cfg.number)
    } else {
        format!(
            "{} {} {} {}",
            cfg.repo,
            cfg.number,
            to_ymdhms_date(cfg.created_at),
            cfg.event_type
        )
    }
}

fn opt_i64_str(v: Option<i64>) -> String {
    match v {
        None => NULL.to_string(),
        Some(i) => i.to_string(),
    }
}

/// One `string_agg` of ids for an event (labels/assignees/reviewers).
fn gha_id_list(con: &PgConn, ctx: &Ctx, table: &str, column: &str, event_id: i64) -> String {
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!(
            "select coalesce(string_agg(sub.{column}::text, ','), '') from \
             (select {column} from {table} where event_id = {} order by {column}) sub",
            n_value(1)
        ),
        &[SqlArg::Int(event_id)],
    );
    let mut res = String::new();
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut res]));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    res
}

/// The per-issue goroutine of `SyncIssuesState`; returns "changed anything".
fn sync_one_issue(
    ctx: &Ctx,
    con: &PgConn,
    mut cfg: IssueConfig,
    manual: bool,
    counters: &Counters,
) -> bool {
    let iid = cfg.issue_id;
    if ctx.debug > 1 {
        printf(&format!("GHA Issue ID '{iid}' --> '{cfg}'\n"));
    }
    let issue = cfg.issue().clone();
    let api_milestone_id = cfg.milestone_id;
    let api_closed_at = issue.closed_at;
    let api_state = issue.state.clone().expect("issue without state");
    let api_title = issue.title.clone().expect("issue without title");
    let api_locked = issue.locked.expect("issue without locked");
    let api_assignee_id = cfg.assignee_id;
    let event_id = ARTIFICIAL_EVENT_BASE + cfg.event_id;

    let mut gha_milestone_id: Option<i64> = None;
    let mut gha_event_id: i64 = 0;
    let mut gha_closed_at: Option<DateTime<Utc>> = None;
    let mut gha_state = String::new();
    let mut gha_title = String::new();
    let mut gha_locked = false;
    let mut gha_assignee_id: Option<i64> = None;

    let mut rows = if manual {
        query_sql_with_err(
            con,
            ctx,
            &format!(
                "select milestone_id, event_id, closed_at, state, title, locked, assignee_id \
                 from gha_issues where id = {} order by updated_at desc, event_id desc limit 1",
                n_value(1)
            ),
            &[SqlArg::Int(cfg.issue_id)],
        )
    } else {
        query_sql_with_err(
            con,
            ctx,
            &format!(
                "select milestone_id, event_id, closed_at, state, title, locked, assignee_id \
                 from gha_issues where id = {} and event_id = {}",
                n_value(1),
                n_value(2)
            ),
            &[SqlArg::Int(cfg.issue_id), SqlArg::Int(event_id)],
        )
    };
    let mut got = false;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [
            &mut gha_milestone_id,
            &mut gha_event_id,
            &mut gha_closed_at,
            &mut gha_state,
            &mut gha_title,
            &mut gha_locked,
            &mut gha_assignee_id,
        ]));
        got = true;
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());

    if !got {
        if ctx.debug > 1 {
            printf(&format!(
                "Adding missing ({}) event '{}'\n",
                gofmt::time(cfg.created_at),
                cfg
            ));
        }
        artificial_event(con, ctx, &mut cfg);
        let (why, what) = if manual {
            ("no previous issue state", what_for(&cfg, true))
        } else {
            ("no issue event", what_for(&cfg, false))
        };
        counters.bump(0);
        counters.info(why, what);
        return true;
    }

    let prefix = if manual {
        format!("{} {}: ", cfg.repo, cfg.number)
    } else {
        format!(
            "{} {} {} {}: ",
            cfg.repo,
            cfg.number,
            to_ymdhms_date(cfg.created_at),
            cfg.event_type
        )
    };

    let changed_state = api_state != gha_state;
    if changed_state {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating issue '{cfg}' state {gha_state} -> {api_state}\n"
            ));
        }
        counters.info(
            "changed issue state",
            format!("{prefix}{gha_state} -> {api_state}"),
        );
    }

    let changed_title = api_title != gha_title;
    if changed_title {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating issue '{cfg}' title {gha_title} -> {api_title}\n"
            ));
        }
        counters.info(
            "changed issue title",
            format!("{prefix}{gha_title} -> {api_title}"),
        );
    }

    let changed_locked = api_locked != gha_locked;
    if changed_locked {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating issue '{cfg}' locked {gha_locked} -> {api_locked}\n"
            ));
        }
        counters.info(
            "changed issue locked state",
            format!("{prefix}{gha_locked} -> {api_locked}"),
        );
    }

    let api_closed_str = api_closed_at.as_ref().map(|t| to_ymdhms_date(t.0));
    let gha_closed_str = gha_closed_at.as_ref().map(|t| to_ymdhms_date(*t));
    let changed_closed = api_closed_str != gha_closed_str;
    if changed_closed {
        let from = gha_closed_str.clone().unwrap_or_else(|| NULL.to_string());
        let to = api_closed_str.clone().unwrap_or_else(|| NULL.to_string());
        if ctx.debug > 1 {
            printf(&format!(
                "Updating issue '{cfg}' closed_at {from} -> {to}\n"
            ));
        }
        counters.info("changed issue closed at", format!("{prefix}{from} -> {to}"));
    }

    let changed_milestone = api_milestone_id != gha_milestone_id;
    if changed_milestone {
        let from = opt_i64_str(gha_milestone_id);
        let to = opt_i64_str(api_milestone_id);
        if ctx.debug > 1 {
            printf(&format!(
                "Updating issue '{cfg}' milestone {from} -> {to}\n"
            ));
        }
        counters.info("changed issue milestone", format!("{prefix}{from} -> {to}"));
    }

    let changed_assignee = api_assignee_id != gha_assignee_id;
    if changed_assignee {
        let from = opt_i64_str(gha_assignee_id);
        let to = opt_i64_str(api_assignee_id);
        if ctx.debug > 1 {
            printf(&format!("Updating issue '{cfg}' assignee {from} -> {to}\n"));
        }
        counters.info("changed issue assignee", format!("{prefix}{from} -> {to}"));
    }

    let gha_labels = gha_id_list(con, ctx, "gha_issues_labels", "label_id", gha_event_id);
    let changed_labels = gha_labels != cfg.labels;
    if changed_labels {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating issue '{}' labels to '{}', they were: '{}' (event_id {})\n",
                cfg, cfg.labels, gha_labels, gha_event_id
            ));
        }
        counters.info(
            "changed issue labels",
            format!("{prefix}{gha_labels} -> {}", cfg.labels),
        );
    }

    let gha_assignees = gha_id_list(
        con,
        ctx,
        "gha_issues_assignees",
        "assignee_id",
        gha_event_id,
    );
    let changed_assignees = gha_assignees != cfg.assignees;
    if changed_assignees {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating issue '{}' assignees to '{}', they were: '{}' (event_id {})\n",
                cfg, cfg.assignees, gha_assignees, gha_event_id
            ));
        }
        counters.info(
            "changed issue assignees",
            format!("{prefix}{gha_assignees} -> {}", cfg.assignees),
        );
    }

    let mut uidx = 1;
    let mut why = "previous issue state the same".to_string();
    let mut what = what_for(&cfg, manual);
    let changed_anything = changed_milestone
        || changed_state
        || changed_closed
        || changed_assignee
        || changed_title
        || changed_locked
        || changed_labels
        || changed_assignees;
    if changed_anything {
        uidx = 2;
        if manual {
            artificial_event(con, ctx, &mut cfg);
            why = "previous issue state different".to_string();
            what = format!("{} {}", cfg.repo, cfg.number);
        } else {
            if ctx.debug > 0 {
                printf(&format!(
                    "Debug: Exact artificial event ({}, {}) already exists with different state, skipping: '{}'\n",
                    gofmt::time(cfg.created_at),
                    event_id,
                    cfg
                ));
            }
            why = "collision and issue state differs".to_string();
            what = format!(
                "{} {} {} {}: {}",
                cfg.repo,
                cfg.number,
                to_ymdhms_date(cfg.created_at),
                cfg.event_type,
                event_id
            );
            if !ctx.skip_update_events {
                why = "updated existing issue state".to_string();
                delete_artificial_event(con, ctx, &cfg);
                artificial_event(con, ctx, &mut cfg);
            }
        }
    }
    if ctx.debug > 1 {
        if manual {
            printf(&format!(
                "Previous event (event_id: {gha_event_id}), added artificial: {changed_anything}: '{cfg}'\n"
            ));
        } else {
            printf(&format!(
                "Event for the same date ({}) exist (event_id: {}), added artificial: {}: '{}'\n",
                gofmt::time(cfg.created_at),
                gha_event_id,
                changed_anything,
                cfg
            ));
        }
    }
    counters.bump(uidx);
    counters.info(&why, what);
    changed_anything
}

/// The per-PR goroutine of `SyncIssuesState`; returns "changed anything".
fn sync_one_pr(
    ctx: &Ctx,
    con: &PgConn,
    iid: i64,
    mut ic: IssueConfig,
    pr: &PullRequest,
    manual: bool,
    counters: &Counters,
) -> bool {
    let prid = pr.id.expect("PR without id");
    let updated_at = pr.updated_at.expect("PR without updated_at");
    if ctx.debug > 1 {
        printf(&format!(
            "GHA Issue ID '{}' --> PR ID {}, updated {}\n",
            iid,
            prid,
            gofmt::time(updated_at.0)
        ));
    }
    let api_milestone_id = pr.milestone.as_ref().and_then(|m| m.id);
    let api_closed_at = pr.closed_at;
    let api_state = pr.state.clone().expect("PR without state");
    let api_title = pr.title.clone().expect("PR without title");
    let api_assignee_id = pr.assignee.as_ref().and_then(|a| a.id);
    let api_merged_by_id = pr.merged_by.as_ref().and_then(|a| a.id);
    let api_merged_at = pr.merged_at;
    let api_merged = pr.merged;
    let event_id = ARTIFICIAL_EVENT_BASE + ic.event_id;

    let mut gha_milestone_id: Option<i64> = None;
    let mut gha_event_id: i64 = 0;
    let mut gha_closed_at: Option<DateTime<Utc>> = None;
    let mut gha_state = String::new();
    let mut gha_title = String::new();
    let mut gha_merged_by_id: Option<i64> = None;
    let mut gha_merged_at: Option<DateTime<Utc>> = None;
    let mut gha_merged: Option<bool> = None;
    let mut gha_assignee_id: Option<i64> = None;

    let mut rows = if manual {
        query_sql_with_err(
            con,
            ctx,
            &format!(
                "select milestone_id, event_id, closed_at, state, title, assignee_id, \
                 merged_by_id, merged_at, merged from gha_pull_requests where id = {} \
                 order by updated_at desc, event_id desc limit 1",
                n_value(1)
            ),
            &[SqlArg::Int(prid)],
        )
    } else {
        query_sql_with_err(
            con,
            ctx,
            &format!(
                "select milestone_id, event_id, closed_at, state, title, assignee_id, \
                 merged_by_id, merged_at, merged from gha_pull_requests where id = {} and event_id = {}",
                n_value(1),
                n_value(2)
            ),
            &[SqlArg::Int(prid), SqlArg::Int(event_id)],
        )
    };
    let mut got = false;
    while rows.next() {
        fatal_on_err(rows.scan(&mut [
            &mut gha_milestone_id,
            &mut gha_event_id,
            &mut gha_closed_at,
            &mut gha_state,
            &mut gha_title,
            &mut gha_assignee_id,
            &mut gha_merged_by_id,
            &mut gha_merged_at,
            &mut gha_merged,
        ]));
        got = true;
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());

    if !got {
        if ctx.debug > 1 {
            printf(&format!(
                "Adding missing ({}) PR event '{}', PR ID: {}\n",
                gofmt::time(updated_at.0),
                ic,
                prid
            ));
        }
        artificial_pr_event(con, ctx, &mut ic, pr);
        let (why, what) = if manual {
            ("no previous pr state", what_for(&ic, true))
        } else {
            ("no pr event", what_for(&ic, false))
        };
        counters.bump(0);
        counters.info(why, what);
        return true;
    }

    let prefix = if manual {
        format!("{} {}: ", ic.repo, ic.number)
    } else {
        format!(
            "{} {} {} {}: ",
            ic.repo,
            ic.number,
            to_ymdhms_date(ic.created_at),
            ic.event_type
        )
    };

    let changed_state = api_state != gha_state;
    if changed_state {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating PR '{ic}' state {gha_state} -> {api_state}\n"
            ));
        }
        counters.info(
            "changed pr state",
            format!("{prefix}{gha_state} -> {api_state}"),
        );
    }

    let changed_title = api_title != gha_title;
    if changed_title {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating PR '{ic}' title {gha_title} -> {api_title}\n"
            ));
        }
        counters.info(
            "changed pr title",
            format!("{prefix}{gha_title} -> {api_title}"),
        );
    }

    let changed_merged = api_merged != gha_merged;
    if changed_merged {
        let from = gha_merged
            .map(|b| b.to_string())
            .unwrap_or_else(|| NULL.to_string());
        let to = api_merged
            .map(|b| b.to_string())
            .unwrap_or_else(|| NULL.to_string());
        if ctx.debug > 1 {
            printf(&format!("Updating PR '{ic}' merged {from} -> {to}\n"));
        }
        counters.info("changed pr merged", format!("{prefix}{from} -> {to}"));
    }

    let api_closed_str = api_closed_at.as_ref().map(|t| to_ymdhms_date(t.0));
    let gha_closed_str = gha_closed_at.as_ref().map(|t| to_ymdhms_date(*t));
    let changed_closed = api_closed_str != gha_closed_str;
    if changed_closed {
        let from = gha_closed_str.clone().unwrap_or_else(|| NULL.to_string());
        let to = api_closed_str.clone().unwrap_or_else(|| NULL.to_string());
        if ctx.debug > 1 {
            printf(&format!("Updating PR '{ic}' closed_at {from} -> {to}\n"));
        }
        counters.info("changed pr closed at", format!("{prefix}{from} -> {to}"));
    }

    let api_merged_str = api_merged_at.as_ref().map(|t| to_ymdhms_date(t.0));
    let gha_merged_str = gha_merged_at.as_ref().map(|t| to_ymdhms_date(*t));
    let changed_merged_at = api_merged_str != gha_merged_str;
    if changed_merged_at {
        let from = gha_merged_str.clone().unwrap_or_else(|| NULL.to_string());
        let to = api_merged_str.clone().unwrap_or_else(|| NULL.to_string());
        if ctx.debug > 1 {
            printf(&format!("Updating PR '{ic}' merged_at {from} -> {to}\n"));
        }
        counters.info("changed pr merged at", format!("{prefix}{from} -> {to}"));
    }

    let changed_milestone = api_milestone_id != gha_milestone_id;
    if changed_milestone {
        let from = opt_i64_str(gha_milestone_id);
        let to = opt_i64_str(api_milestone_id);
        if ctx.debug > 1 {
            printf(&format!("Updating PR '{ic}' milestone {from} -> {to}\n"));
        }
        counters.info("changed pr milestone", format!("{prefix}{from} -> {to}"));
    }

    let changed_assignee = api_assignee_id != gha_assignee_id;
    if changed_assignee {
        let from = opt_i64_str(gha_assignee_id);
        let to = opt_i64_str(api_assignee_id);
        if ctx.debug > 1 {
            printf(&format!("Updating PR '{ic}' assignee {from} -> {to}\n"));
        }
        counters.info("changed pr assignee", format!("{prefix}{from} -> {to}"));
    }

    let changed_merged_by = api_merged_by_id != gha_merged_by_id;
    if changed_merged_by {
        let from = opt_i64_str(gha_merged_by_id);
        let to = opt_i64_str(api_merged_by_id);
        if ctx.debug > 1 {
            printf(&format!("Updating PR '{ic}' merged by {from} -> {to}\n"));
        }
        counters.info("changed pr merged by", format!("{prefix}{from} -> {to}"));
    }

    let api_assignees = join_ids(
        pr.assignees
            .iter()
            .map(|a| a.id.expect("assignee without id")),
    );
    let gha_assignees = gha_id_list(
        con,
        ctx,
        "gha_pull_requests_assignees",
        "assignee_id",
        gha_event_id,
    );
    let changed_assignees = gha_assignees != api_assignees;
    if changed_assignees {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating PR '{ic}' assignees to '{api_assignees}', they were: '{gha_assignees}' (event_id {gha_event_id})\n"
            ));
        }
        counters.info(
            "changed pr assignees",
            format!("{prefix}{gha_assignees} -> {api_assignees}"),
        );
    }

    let api_reviewers = join_ids(
        pr.requested_reviewers
            .iter()
            .map(|a| a.id.expect("reviewer without id")),
    );
    let gha_reviewers = gha_id_list(
        con,
        ctx,
        "gha_pull_requests_requested_reviewers",
        "requested_reviewer_id",
        gha_event_id,
    );
    let changed_reviewers = gha_reviewers != api_reviewers;
    if changed_reviewers {
        if ctx.debug > 1 {
            printf(&format!(
                "Updating PR '{ic}' requested reviewers to '{api_reviewers}', they were: '{gha_reviewers}' (event_id {gha_event_id})\n"
            ));
        }
        counters.info(
            "changed pr reqested reviewers",
            format!("{prefix}{gha_reviewers} -> {api_reviewers}"),
        );
    }

    let mut uidx = 1;
    let mut why = "previous pr state the same".to_string();
    let mut what = what_for(&ic, manual);
    let changed_anything = changed_milestone
        || changed_state
        || changed_closed
        || changed_merged
        || changed_merged_at
        || changed_merged_by
        || changed_assignee
        || changed_title
        || changed_assignees
        || changed_reviewers;
    if changed_anything {
        uidx = 2;
        if manual {
            artificial_pr_event(con, ctx, &mut ic, pr);
            why = "previous pr state different".to_string();
            what = format!("{} {}", ic.repo, ic.number);
        } else {
            if ctx.debug > 0 {
                printf(&format!(
                    "Warning: Exact artificial PR event ({}, {}) already exists with different state, skipping: '{}'\n",
                    gofmt::time(ic.created_at),
                    event_id,
                    ic
                ));
            }
            why = "collision and pr state differs".to_string();
            what = format!(
                "{} {} {} {}: {}",
                ic.repo,
                ic.number,
                to_ymdhms_date(ic.created_at),
                ic.event_type,
                event_id
            );
            if !ctx.skip_update_events {
                why = "updated existing pr state".to_string();
                delete_artificial_pr_event(con, ctx, &ic);
                artificial_pr_event(con, ctx, &mut ic, pr);
            }
        }
    }
    if ctx.debug > 1 {
        if manual {
            printf(&format!(
                "PR Event exist (event_id: {gha_event_id}), added artificial: {changed_anything}: '{ic}'\n"
            ));
        } else {
            printf(&format!(
                "PR Event for the same date ({}) exist (event_id: {}), added artificial: {}: '{}'\n",
                gofmt::time(updated_at.0),
                gha_event_id,
                changed_anything,
                ic
            ));
        }
    }
    counters.bump(uidx);
    counters.info(&why, what);
    changed_anything
}

/// Go `SyncIssuesState`: write the collected issue (then PR) states as
/// artificial events — `manual` is the `sync_issues` mode (compare with the
/// latest stored state), otherwise the `ghapi2db` mode (compare with the
/// state stored for the very same second).
pub fn sync_issues_state(
    gcs: &[Client],
    ctx: &Ctx,
    con: &PgConn,
    issues: &mut IssuesMap,
    prs: &PrsMap,
    manual: bool,
) {
    let n_issues_before: usize = issues.values().map(|v| v.len()).sum();

    for ary in issues.values_mut() {
        sort_issue_configs(ary);
        if ctx.debug > 1 {
            printf(&format!("Sorted: {}\n", fmt_issue_configs(ary)));
        }
    }

    output_issues_info(issues, "Issues to process");

    let mut thr_ctx = ctx.copy_context();
    let mut thr_n = get_threads_num(&mut thr_ctx);
    let mut prc = 0usize;
    let n_issues: usize = issues.values().map(|v| v.len()).sum();
    let n_prs = prs.len();
    let counters = Counters::new();

    printf(&format!(
        "ghapi2db.go: Processing {} PRs, {} issues ({} with date collisions), manual mode: {} - GHA part\n",
        n_prs, n_issues, n_issues_before, manual
    ));

    let dt_start = Utc::now();
    let mut last_time = dt_start;
    let mut checked = 0usize;
    let period = Duration::from_secs(10);
    {
        let (tx, rx) = mpsc::channel::<bool>();
        let counters = &counters;
        std::thread::scope(|scope| {
            let mut n_threads = 0usize;
            for cfg_ary in issues.values() {
                for cfg in cfg_ary {
                    let tx = tx.clone();
                    let cfg = cfg.clone();
                    scope.spawn(move || {
                        if ctx.skip_api_issues || (ctx.skip_api_prs && cfg.pr) {
                            let _ = tx.send(false);
                            return;
                        }
                        let res = sync_one_issue(ctx, con, cfg, manual, counters);
                        let _ = tx.send(res);
                    });
                    n_threads += 1;
                    while n_threads >= thr_n {
                        let _ = rx.recv();
                        n_threads -= 1;
                        prc += 1;
                        if prc.is_multiple_of(20) {
                            thr_n = get_threads_num(&mut thr_ctx);
                        }
                        checked += 1;
                        progress_info(checked, n_issues, dt_start, &mut last_time, period, "");
                    }
                }
            }
            while n_threads > 0 {
                let _ = rx.recv();
                n_threads -= 1;
                checked += 1;
                progress_info(checked, n_issues, dt_start, &mut last_time, period, "");
            }
        });
    }
    let (hint, _, rem, wait) = get_rate_limits(ctx, gcs, true);
    let updates = counters.updates();
    if manual {
        printf(&format!(
            "ghapi2db.go: Manually processed {} issues/PRs ({} new issues, existing: {} not needed, {} added): {} API points remain, resets in {}, hint key: {}\n",
            checked, updates[0], updates[1], updates[2], fmt_slice(&rem), fmt_slice(&wait), hint
        ));
    } else {
        printf(&format!(
            "ghapi2db.go: Automatically processed {} issues/PRs ({} new, {} the same exists, {} incorrect state exists): {} API points remain, resets in {}, hint key: {}\n",
            checked, updates[0], updates[1], updates[2], fmt_slice(&rem), fmt_slice(&wait), hint
        ));
    }
    output_info(&counters.take_infos(), "Issues");

    if ctx.skip_api_prs {
        return;
    }
    output_prs_info(prs, "PRs to process");
    let counters = Counters::new();
    let dt_start = Utc::now();
    let mut last_time = dt_start;
    let mut checked = 0usize;
    {
        let (tx, rx) = mpsc::channel::<bool>();
        let counters = &counters;
        let issues: &IssuesMap = issues;
        std::thread::scope(|scope| {
            let mut n_threads = 0usize;
            for (iid, pr) in prs {
                let tx = tx.clone();
                let iid = *iid;
                scope.spawn(move || {
                    let ica = issues.get(&iid).expect("PR without its issue configs");
                    let ic = ica
                        .last()
                        .expect("PR with an empty issue config list")
                        .clone();
                    let res = sync_one_pr(ctx, con, iid, ic, pr, manual, counters);
                    let _ = tx.send(res);
                });
                n_threads += 1;
                while n_threads >= thr_n {
                    let _ = rx.recv();
                    n_threads -= 1;
                    prc += 1;
                    if prc.is_multiple_of(20) {
                        thr_n = get_threads_num(&mut thr_ctx);
                    }
                    checked += 1;
                    progress_info(checked, n_issues, dt_start, &mut last_time, period, "");
                }
            }
            while n_threads > 0 {
                let _ = rx.recv();
                n_threads -= 1;
                checked += 1;
                progress_info(checked, n_issues, dt_start, &mut last_time, period, "");
            }
        });
    }
    let (hint, _, rem, wait) = get_rate_limits(ctx, gcs, true);
    let updates = counters.updates();
    if manual {
        printf(&format!(
            "ghapi2db.go: Manually processed {} PRs ({} new PRs, existing: {} not needed, {} added): {} API points remain, resets in {}, hint key: {}\n",
            checked, updates[0], updates[1], updates[2], fmt_slice(&rem), fmt_slice(&wait), hint
        ));
    } else {
        printf(&format!(
            "ghapi2db.go: Automatically processed {} PRs ({} new PRs, existing: {} not needed, {} added): {} API points remain, resets in {}, hint key: {}\n",
            checked, updates[0], updates[1], updates[2], fmt_slice(&rem), fmt_slice(&wait), hint
        ));
    }
    output_info(&counters.take_infos(), "PRs");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(id: i64, eid: i64, secs: i64) -> IssueConfig {
        IssueConfig {
            repo: "o/r".to_string(),
            number: 1,
            issue_id: id,
            event_id: eid,
            created_at: chrono::TimeZone::timestamp_opt(&Utc, secs, 0)
                .unwrap()
                .fixed_offset(),
            ..Default::default()
        }
    }

    #[test]
    fn issue_config_strings() {
        let mut c = cfg(5, 7, 1_600_000_000);
        c.event_type = "sync".to_string();
        c.pr = true;
        c.milestone_id = Some(9);
        c.labels_map.insert(2, "b".to_string());
        c.labels_map.insert(1, "a".to_string());
        c.labels = "1,2".to_string();
        assert_eq!(
            c.to_string(),
            "{Repo: o/r, Number: 1, IssueID: 5, EventID: 7, EventType: sync, Pr: true, MilestoneID: 9, AssigneeID: 0, CreatedAt: 2020-09-13 12:26:40, Labels: 1,2, LabelsMap: map[1:a 2:b], Assignees: , AssigneesMap: map[]}"
        );
        assert_eq!(
            c.config_str(),
            "{Repo: o/r, Number: 1, IssueID: 5, MilestoneID: 9, AssigneeID: 0, Labels: 1,2, Assignees: }"
        );
        assert_eq!(join_ids([3, 1, 2].into_iter()), "1,2,3");
        assert_eq!(join_ids(std::iter::empty()), "");
    }

    #[test]
    fn sorting() {
        let mut ary = vec![cfg(2, 1, 10), cfg(1, 5, 20), cfg(1, 3, 20), cfg(1, 9, 5)];
        sort_issue_configs(&mut ary);
        let keys: Vec<(i64, i64)> = ary.iter().map(|c| (c.issue_id, c.event_id)).collect();
        assert_eq!(keys, vec![(1, 9), (1, 3), (1, 5), (2, 1)]);
    }

    #[test]
    fn go_durations() {
        assert_eq!(GoDuration::from_secs(5).to_string(), "5s");
        assert_eq!(GoDuration(-1_500_000_000).to_string(), "-1.5s");
        assert_eq!(GoDuration(3_599_500_000_000).to_string(), "59m59.5s");
        assert_eq!(
            fmt_slice(&[GoDuration::from_secs(5), GoDuration(0)]),
            "[5s 0s]"
        );
    }
}
