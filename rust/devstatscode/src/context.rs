//! Environment context — port of `context.go`.
//!
//! Every DevStats tool reads its configuration from `GHA2DB_*` / `PG_*`
//! environment variables into a [`Ctx`]; `Ctx::init` mirrors Go's `Ctx.Init()`
//! field by field (same defaults, same validation, same fatal errors).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::consts;
use crate::env::{start_env_syncer, update_env};
use crate::error::{fatal_no_log, fatalf, go_io_error_string};
use crate::gofmt;
use crate::goregex;
use crate::time::{
    format_go_duration, parse_go_duration, parse_go_float, parse_go_int, time_parse_any,
    to_ymdhms_date, ymd_hms,
};

/// A compiled regular expression that remembers its (Go syntax) pattern.
/// Equality and printing are pattern based, like Go's `*regexp.Regexp`.
#[derive(Clone)]
pub struct GoRegex {
    pattern: String,
    re: regex::Regex,
}

impl GoRegex {
    /// Compile a Go (RE2) pattern via [`goregex::compile`].
    pub fn new(pattern: &str) -> Result<Self, regex::Error> {
        Ok(GoRegex {
            pattern: pattern.to_string(),
            re: goregex::compile(pattern)?,
        })
    }

    /// Compile or die (Go `regexp.MustCompile`).
    pub fn must(pattern: &str) -> Self {
        match Self::new(pattern) {
            Ok(r) => r,
            Err(e) => fatal_no_log(format!("regexp: Compile(`{}`): {}", pattern, e)),
        }
    }

    /// The original pattern.
    pub fn as_str(&self) -> &str {
        &self.pattern
    }

    /// The compiled regex.
    pub fn regex(&self) -> &regex::Regex {
        &self.re
    }

    pub fn is_match(&self, s: &str) -> bool {
        self.re.is_match(s)
    }
}

impl PartialEq for GoRegex {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}

impl fmt::Debug for GoRegex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GoRegex({:?})", self.pattern)
    }
}

impl fmt::Display for GoRegex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pattern)
    }
}

fn getenv(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn env_set(name: &str) -> bool {
    !getenv(name).is_empty()
}

fn env_int(name: &str) -> Option<i64> {
    let v = getenv(name);
    if v.is_empty() {
        return None;
    }
    match parse_go_int(&v) {
        Ok(n) => Some(n),
        Err(e) => fatal_no_log(e),
    }
}

fn ensure_trailing_slash(mut s: String) -> String {
    if !s.ends_with('/') {
        s.push('/');
    }
    s
}

fn comma_set(name: &str) -> BTreeMap<String, bool> {
    let mut m = BTreeMap::new();
    let v = getenv(name);
    if !v.is_empty() {
        for item in v.split(',') {
            if !item.is_empty() {
                m.insert(item.to_string(), true);
            }
        }
    }
    m
}

fn env_list_or(name: &str, default: &[&str]) -> Vec<String> {
    let v = getenv(name);
    if v.is_empty() {
        default.iter().map(|s| s.to_string()).collect()
    } else {
        v.split(',').map(|s| s.to_string()).collect()
    }
}

fn env_int_list_or(name: &str, default: &[i64]) -> Vec<i64> {
    let v = getenv(name);
    if v.is_empty() {
        return default.to_vec();
    }
    v.split(',')
        .map(|s| match parse_go_int(s) {
            Ok(n) => n,
            Err(e) => fatal_no_log(e),
        })
        .collect()
}

fn env_string_or(name: &str, default: &str) -> String {
    let v = getenv(name);
    if v.is_empty() {
        default.to_string()
    } else {
        v
    }
}

fn opt_regex_string(r: Option<&GoRegex>) -> String {
    match r {
        Some(r) => r.as_str().to_string(),
        None => "<nil>".to_string(),
    }
}

fn compute_periods_string(cp: Option<&BTreeMap<String, BTreeSet<bool>>>) -> String {
    match cp {
        None => "map[]".to_string(),
        Some(m) => {
            let parts: Vec<String> = m
                .iter()
                .map(|(k, set)| {
                    let inner: Vec<String> = set.iter().map(|b| format!("{}:{{}}", b)).collect();
                    format!("{}:map[{}]", k, inner.join(" "))
                })
                .collect();
            format!("map[{}]", parts.join(" "))
        }
    }
}

fn map_arr2_string(m: &BTreeMap<String, [i64; 2]>) -> String {
    let parts: Vec<String> = m
        .iter()
        .map(|(k, v)| format!("{}:[{} {}]", k, v[0], v[1]))
        .collect();
    format!("map[{}]", parts.join(" "))
}

/// Environment context packed in a structure (see `context.go` for the
/// meaning of every field; the doc comments are copied from there).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Ctx {
    /// From GHA2DB_DATADIR, default /etc/gha2db/
    pub data_dir: String,
    /// From GHA2DB_DEBUG Debug level: 0-no, 1-info, 2-verbose, including SQLs, default 0
    pub debug: i64,
    /// From GHA2DB_CMDDEBUG Commands execution Debug level: 0-no, 1-only output commands, 2-output commands and their output, 3-output full environment as well, default 0
    pub cmd_debug: i64,
    /// From GHA2DB_GITHUB_DEBUG debug GitHub rate limits
    pub github_debug: i64,
    /// From GHA2DB_DRY_RUN, import_affs tool - stop before doing any updates
    pub dry_run: bool,
    /// From GHA2DB_JSON gha2db: write JSON files? default false
    pub json_out: bool,
    /// From GHA2DB_NODB gha2db: write to SQL database, default true
    pub db_out: bool,
    /// From GHA2DB_ST true: use single threaded version, false: use multi threaded version, default false
    pub st: bool,
    /// From GHA2DB_NCPUS, set to override number of CPUs to run, this overwrites GHA2DB_ST, default 0 (which means do not use it)
    pub ncpus: i64,
    /// From PG_HOST, default "localhost"
    pub pg_host: String,
    /// From PG_PORT, default "5432"
    pub pg_port: String,
    /// From PG_DB, default "gha"
    pub pg_db: String,
    /// From GHA2DB_AFFILIATIONS_DB, name of the shared affiliations database holding gha_actors* & related tables, empty (default) means legacy mode: those tables are project-local
    pub affiliations_db: String,
    /// From PG_USER, default "gha_admin"
    pub pg_user: String,
    /// From PG_PASS, default "password"
    pub pg_pass: String,
    /// From PG_SSL, default "disable"
    pub pg_ssl: String,
    /// From GHA2DB_INDEX Create DB index? default false
    pub index: bool,
    /// From GHA2DB_SKIPTABLE Create table structure? default true
    pub table: bool,
    /// From GHA2DB_SKIPTOOLS Create DB tools (like views, summary tables, materialized views etc)? default true
    pub tools: bool,
    /// From GHA2DB_MGETC Character returned by mgetc (if non empty), default ""
    pub mgetc: String,
    /// From GHA2DB_QOUT output all SQL queries?, default false
    pub q_out: bool,
    /// From GHA2DB_CTXOUT output all context data (this struct), default false
    pub ctx_out: bool,
    /// From GHA2DB_SKIPTIME, output time with all lib.Printf(...) calls, default true, use GHA2DB_SKIPTIME to disable
    pub log_time: bool,
    /// From GHA2DB_STARTDT, default `2012-07-01 00:00 UTC`, expects format "YYYY-MM-DD HH:MI:SS", can be set in `projects.yaml` via `start_date:`, value from projects.yaml (if set) has the highest priority.
    pub default_start_date: DateTime<Utc>,
    /// From GHA2DB_STARTDT_FORCE, default false
    pub force_start_date: bool,
    /// From GHA2DB_LASTSERIES, use this TSDB series to determine last timestamp date, default "events_h"
    pub last_series: String,
    /// From GHA2DB_SKIPTSDB gha2db_sync tool, skip TS DB processing? for calc_metric it skips final series write, default false
    pub skip_tsdb: bool,
    /// From GHA2DB_SKIPPDB gha2db_sync tool, skip Postgres DB processing (gha2db part) default false
    pub skip_pdb: bool,
    /// From GHA2DB_RESETTSDB sync tool, regenerate all TS points? default false
    pub reset_tsdb: bool,
    /// From GHA2DB_RESETRANGES sync tool, regenerate all past quick ranges? default false
    pub reset_ranges: bool,
    /// From GHA2DB_EXPLAIN runq tool, prefix query with "explain " - it will display query plan instead of executing real query, default false
    pub explain: bool,
    /// From GHA2DB_OLDFMT gha2db tool, if set then use pre 2015 GHA JSONs format
    pub old_format: bool,
    /// From GHA2DB_EXACT gha2db tool, if set then orgs list provided from commandline is used as a list of exact repository full names, like "a/b,c/d,e", if not only full names "a/b,x/y" can be treated like this, names without "/" are either orgs or repos.
    pub exact: bool,
    /// From GHA2DB_SKIPLOG all tools, if set, DB logging into Postgres table `gha_logs` in `devstats` database will be disabled
    pub log_to_db: bool,
    /// From GHA2DB_LOCAL many tools, if set it will use data files prefixed with "./" to use local ones. Otherwise it will search for data files in /etc/gha2db.
    pub local: bool,
    /// From GHA2DB_ABSOLUTE runq tool, if set it will use data files without any prefix (allowing absolute paths as well). Otherwise it will search for data files in /etc/gha2db.
    pub absolute: bool,
    /// From GHA2DB_LOCAL_CMD many tools, if set it will call other tools prefixed with "./" to use locally compiled ones. Otherwise it will call binaries without prefix (so it will use those in $PATH).
    pub local_cmd: bool,
    /// From GHA2DB_METRICS_YAML gha2db_sync tool, set other metrics.yaml file, default is "metrics/{{project}}metrics.yaml"
    pub metrics_yaml: String,
    /// From GHA2DB_TAGS_YAML tags tool, set other tags.yaml file, default is "metrics/{{project}}/tags.yaml"
    pub tags_yaml: String,
    /// From GHA2DB_COLUMNS_YAML tags tool, set other columns.yaml file, default is "metrics/{{project}}/columns.yaml"
    pub columns_yaml: String,
    /// From GHA2DB_VARS_YAML db_vars tool, set other vars.yaml file (full path), default is "metrics/{{project}}/vars.yaml"
    pub vars_yaml: String,
    /// From GHA2DB_VARS_FN_YAML db_vars tool, set other vars.yaml file (final file name without path), default is "vars.yaml"
    pub vars_fn_yaml: String,
    /// From GHA2DB_SKIP_DATES_YAML gha2db tool, set other skip_dates.yaml file, default is "skip_dates.yaml"
    pub skip_dates_yaml: String,
    /// From GHA2DB_GITHUB_OAUTH ghapi2db tool, if not set reads from /etc/github/oauth file, set to "-" to force public access.
    pub github_oauth: String,
    /// From GHA2DB_GITHUB_API_URL ghapi2db/sync_issues tools, override GitHub API base URL (GitHub Enterprise / testing), default "" = https://api.github.com/
    pub github_api_url: String,
    /// From GHA2DB_GHARCHIVE_URL gha2db tool, override GH Archive base URL (mirrors / testing), default "" = http://data.gharchive.org/
    pub gharchive_url: String,
    /// From GHA2DB_MAXLOGAGE gha2db_sync tool, maximum age of devstats.gha_logs entries, default "1 week"
    pub clear_db_period: String,
    /// From GHA2DB_MAX_AFFS_LOCK_AGE devstats tool, maximum age of devstats.gha_metrics "affs_lock" age, default "16 hours"
    pub clear_affs_lock_period: String,
    /// From GHA2DB_MAX_GIANT_LOCK_AGE devstats tool, maximum age of devstats.gha_metrics "giant_lock" age, default "40 hours"
    pub clear_giant_lock_period: String,
    /// From GHA2DB_TRIALS, all Postgres related tools, retry periods for some retryable errors
    pub trials: Vec<i64>,
    /// From GHA2DB_WHROOT, webhook tool, default "/hook", must match .travis.yml notifications webhooks
    pub web_hook_root: String,
    /// From GHA2DB_WHPORT, webhook tool, default ":1982", note that webhook listens using http:1982, but we use apache on https:2982 (to enable https protocol and proxy requests to http:1982)
    pub web_hook_port: String,
    /// From GHA2DB_WHHOST, webhook tool, default "127.0.0.1" (this can be localhost to disable access by IP, we use Apache proxy to enable https and then apache only need 127.0.0.1)
    pub web_hook_host: String,
    /// From GHA2DB_API_HOST, api tool, default "0.0.0.0", listen address of the API server
    pub api_host: String,
    /// From GHA2DB_API_PORT, api tool, default ":8080", listen port of the API server
    pub api_port: String,
    /// From GHA2DB_SKIP_VERIFY_PAYLOAD, webhook tool, default true, use GHA2DB_SKIP_VERIFY_PAYLOAD=1 to manually test payloads
    pub check_payload: bool,
    /// From GHA2DB_SKIP_FULL_DEPLOY, webhook tool, default true, use GHA2DB_SKIP_FULL_DEPLOY=1 to ignore "[deploy]" requests that call `./devel/deploy_all.sh`.
    pub full_deploy: bool,
    /// From GHA2DB_DEPLOY_BRANCHES, webhook tool, default "master" - comma separated list
    pub deploy_branches: Vec<String>,
    /// From GHA2DB_DEPLOY_STATUSES, webhook tool, default "Passed,Fixed", - comma separated list
    pub deploy_statuses: Vec<String>,
    /// From GHA2DB_DEPLOY_RESULTS, webhook tool, default "0", - comma separated list
    pub deploy_results: Vec<i64>,
    /// From GHA2DB_DEPLOY_TYPES, webhook tool, default "push", - comma separated list
    pub deploy_types: Vec<String>,
    /// From GHA2DB_PROJECT_ROOT, webhook tool, no default, must be specified to run webhook tool
    pub project_root: String,
    /// default true, set this manually to false to avoid lib.ExecCommand calling os.Exit() on failure and return error instead
    pub exec_fatal: bool,
    /// default false, set this manually to true to have quite exec failures (for example `get_repos` git-clones or git-pulls on errors).
    pub exec_quiet: bool,
    /// default false, set to true to capture commands STDOUT
    pub exec_output: bool,
    /// From GHA2DB_PROJECT, gha2db_sync default "", You should set it to something like "kubernetes", "prometheus" etc.
    pub project: String,
    /// From GHA2DB_TESTS_YAML ./dbtest.sh tool, set other tests.yaml file, default is "tests.yaml"
    pub tests_yaml: String,
    /// From GHA2DB_REPOS_DIR get_repos tool, default "~/devstats_repos/"
    pub repos_dir: String,
    /// From GHA2DB_PROCESS_REPOS get_repos tool, enable processing (cloning/pulling) all devstats repos, default false
    pub process_repos: bool,
    /// From GHA2DB_PROCESS_COMMITS get_repos tool, enable update/create mapping table: commit - list of file that commit refers to, default false
    pub process_commits: bool,
    /// From GHA2DB_EXTERNAL_INFO get_repos tool, enable outputing data needed by external tools (cncf/gitdm), default false
    pub external_info: bool,
    /// From GHA2DB_PROJECTS_COMMITS get_repos tool, set list of projects for commits analysis instead of analysing all, default "" - means all
    pub projects_commits: String,
    /// From GHA2DB_PROPAGATE_ONLY_VAR, if set the it will check ONLY="a b c" env variable and propagate it into other project filter variables if they're not set, for example GHA2DB_PROJECTS_COMMITS
    pub propagate_only_var: bool,
    /// From GHA2DB_PROJECTS_YAML, many tools - set main projects file, default "projects.yaml"
    pub projects_yaml: String,
    /// From GHA2DB_COMPANY_ACQ_YAML, import_affs tool, set non-standard "companies.yaml" file
    pub company_acq_yaml: String,
    /// From GHA2DB_PROJECTS_OVERRIDE, get_repos and ./devstats tools - for example "-pro1,+pro2" means never sync pro1 and always sync pro2 (even if disabled in `projects.yaml`).
    pub projects_override: BTreeMap<String, bool>,
    /// From GHA2DB_AFFILIATIONS_JSON, import_affs tool - set main affiliations file, default "github_users.json"
    pub affiliations_json: String,
    /// From GHA2DB_EXCLUDE_REPOS, gha2db tool, default "" - comma separated list of repos to exclude, example: "theupdateframework/notary,theupdateframework/other"
    pub exclude_repos: BTreeMap<String, bool>,
    /// From GHA2DB_INPUT_DBS, merge_dbs tool - list of input databases to merge, order matters - first one will insert on a clean DB, next will do insert ignore (to avoid constraints failure due to common data)
    pub input_dbs: Vec<String>,
    /// From GHA2DB_OUTPUT_DB, merge_dbs tool - output database to merge into
    pub output_db: String,
    /// From GHA2DB_TMOFFSET, gha2db_sync tool - uses time offset to decide when to calculate various metrics, default offset is 0 which means UTC, good offset for USA is -6, and for Poland is 1 or 2
    pub tm_offset: i64,
    /// "devstats.cncf.io"
    pub default_hostname: String,
    /// From GHA2DB_RECENT_RANGE, ghapi2db tool, default '8 hours' (6h sync cadence + 2h overlap). This is a recent period to check open issues/PR to fix their labels and milestones; also the API restore passes window.
    pub recent_range: String,
    /// From GHA2DB_RECENT_REPOS_RANGE, ghapi2db tool, default '1 day'. This is a recent period to check modified repositories.
    pub recent_repos_range: String,
    /// From GHA2DB_MIN_GHAPI_POINTS, ghapi2db tool, minimum GitHub API points, before waiting for reset.
    pub min_ghapi_points: i64,
    /// From GHA2DB_MAX_GHAPI_WAIT, ghapi2db tool, maximum wait time for GitHub API points reset (in seconds).
    pub max_ghapi_wait_seconds: i64,
    /// From GHA2DB_MAX_GHAPI_RETRY, ghapi2db tool, maximum wait retries
    pub max_ghapi_retry: i64,
    /// From GHA2DB_GHAPI_RATE_LIMITS_CACHE, ghapi2db/sync_issues tools, for how many seconds
    /// `get_rate_limits` results (one `/rate_limit` call per token) are cached, 0 disables
    /// caching (poll before every API call), default 5
    pub ghapi_rate_limits_cache: i64,
    /// From GHA2DB_GHAPI_ERROR_FATAL, ghapi2db tool, make any GH API error fatal, default false
    pub ghapi_error_is_fatal: bool,
    /// From GHA2DB_GHAPISKIP, ghapi2db tool, if set then tool is skipping GH API calls (all: events (artificial events to make sure we are in sync with GH) and commits (enriches obfuscated GHA commits data)
    pub skip_ghapi: bool,
    /// From GHA2DB_GHAPISKIPEVENTS, ghapi2db tool, if set then tool is skipping GH API events sync
    pub skip_api_events: bool,
    /// From GHA2DB_GHAPISKIPISSUES, ghapi2db tool, if set then tool is skipping GH API issues sync
    pub skip_api_issues: bool,
    /// From GHA2DB_GHAPISKIPPRS, ghapi2db tool, if set then tool is skipping GH API PRs sync
    pub skip_api_prs: bool,
    /// From GHA2DB_GHAPIALLOWINSERTFAIL, ghapi2db tool, if set then artificial events with no actor (deleted GitHub accounts) are reported and skipped, otherwise (default) they are reassigned to the 'ghost' placeholder actor
    pub allow_ghapi_insert_fail: bool,
    /// From GHA2DB_POSTPROCESS_FROM, structure tool: when set together with PostprocessTo, generated tables (gha_texts, gha_issues_events_labels, gha_issues_pull_requests) are rebuilt for rows in [from, to) instead of the default max(event_id) incremental append - makes historical backfills correct
    pub postprocess_from: String,
    /// From GHA2DB_POSTPROCESS_TO, structure tool: exclusive upper bound of the postprocess rebuild range, see PostprocessFrom
    pub postprocess_to: String,
    /// From GHA2DB_GHAPISKIPCOMMITS, ghapi2db tool, if set then tool is skipping GH API commits enrichment
    pub skip_api_commits: bool,
    /// From GHA2DB_GHAPISKIPLICENSES, ghapi2db tool, if set then tool is skipping GH API licenses enrichment
    pub skip_api_licenses: bool,
    /// From GHA2DB_GHAPIFORCELICENSES, ghapi2db tool, if set, recheck licenses on repos that already have licenses fetched
    pub force_api_licenses: bool,
    /// From GHA2DB_GHAPISKIPLANGS, ghapi2db tool, if set then tool is skipping GH API repos programming languages enrichment
    pub skip_api_langs: bool,
    /// From GHA2DB_GHAPIFORCELANGS, ghapi2db tool, if set, recheck programming languages on repos that already have them fetched
    pub force_api_langs: bool,
    /// From GHA2DB_GHAPISKIPCOMMENTS, ghapi2db tool, if set then tool is skipping GH API comments restore (issue + PR review comments missed by GH Archive)
    pub skip_api_comments: bool,
    /// From GHA2DB_GHAPISKIPREVIEWS, ghapi2db tool, if set then tool is skipping GH API PR reviews restore
    pub skip_api_reviews: bool,
    /// From GHA2DB_GHAPISKIPFORKS, ghapi2db tool, if set then tool is skipping GH API forks restore
    pub skip_api_forks: bool,
    /// From GHA2DB_GHAPISKIPRELEASES, ghapi2db tool, if set then tool is skipping GH API releases restore
    pub skip_api_releases: bool,
    /// From GHA2DB_GHAPISKIPSTARS, ghapi2db tool, if set then tool is skipping GH API stars (WatchEvent) restore
    pub skip_api_stars: bool,
    /// From GHA2DB_GHAPISKIPREPOSTATS, ghapi2db tool, if set then tool is skipping the repository counters snapshots (gha_forkees rows: stars, forks, open issues per tracked repository)
    pub skip_api_repo_stats: bool,
    /// From GHA2DB_GHAPISKIPREPOEVENTS, ghapi2db tool, if set then tool is skipping the repository events feed pass (GET /repos/{owner}/{repo}/events written with the gha2db writer: fills the events GH Archive missed)
    pub skip_api_repo_events: bool,
    /// From GHA2DB_GHAPI_RECENT_REPOS_ONLY, ghapi2db tool, when set the API passes only process repositories with gha_events rows in the recent repos range (legacy scope, no heartbeat), default: every gha_repos repository (one current name per id) gated per pass by a GraphQL heartbeat
    pub ghapi_all_repos: bool,
    /// From GHA2DB_GETREPOSSKIP, get_repos tool, if set then tool does nothing
    pub skip_get_repos: bool,
    /// From GHA2DB_CSVOUT, runq tool, if set, saves result in this file
    pub csv_file: String,
    /// From GHA2DB_COMPUTE_ALL, all tools, if set then no period decisions are taken based on time, but all possible periods are recalculated
    pub compute_all: bool,
    /// From GHA2DB_ACTORS_FILTER gha2db tool, if enabled then actor filterning will be added, default false
    pub actors_filter: bool,
    /// From GHA2DB_ACTORS_ALLOW, gha2db tool, process JSON if actor matches this regexp, default "" which means skip this check
    pub actors_allow: Option<GoRegex>,
    /// From GHA2DB_ACTORS_FORBID, gha2db tool, process JSON if actor doesn't match this regexp, default "" which means skip this check
    pub actors_forbid: Option<GoRegex>,
    /// From GHA2DB_SKIP_METRICS, gha2db_sync tool, default "" - comma separated list of metrics to skip, as given by "sql: name" in the "metrics.yaml" file. Those metrics will be skipped.
    pub skip_metrics: BTreeMap<String, bool>,
    /// From GHA2DB_ONLY_METRICS, gha2db_sync tool, default "" - comma separated list of metrics to process, as given by "sql: name" in the "metrics.yaml" file. Only those metrics will be calculated.
    pub only_metrics: BTreeMap<String, bool>,
    /// From GHA2DB_ALLOW_BROKEN_JSON, gha2db tool, default false. If set then gha2db skips broken jsons and saves them as jsons/error_YYYY-MM-DD-h-n-m.json (n is the JSON number (1-m) of m JSONS array)
    pub allow_broken_json: bool,
    /// From GHA2DB_JSONS_DIR, website_data tool, default "./jsons/"
    pub jsons_dir: String,
    /// From GHA2DB_WEBSITEDATA, devstats tool, run website_data just after sync is complete, default false.
    pub website_data: bool,
    /// From GHA2DB_SKIP_UPDATE_EVENTS, ghapi2db tool, drop and recreate artificial events if their state differs, default false
    pub skip_update_events: bool,
    /// From GHA2DB_FORCE_PERIODS, gha2db_sync tool, force recompute only given periods, "y10:t,m:f,...", default ""
    pub compute_periods: Option<BTreeMap<String, BTreeSet<bool>>>,
    /// From GHA2DB_NO_AUTOFETCHCOMMITS, ghapi2db, disable fetching from last enriched commit data, it will fetch from RecentRange instead, AutoFetchCommits is enabled by default
    pub auto_fetch_commits: bool,
    /// From GHA2DB_SKIP_TAGS, gha2db_sync tool, skip calling tags tool, default false
    pub skip_tags: bool,
    /// From GHA2DB_SKIP_ANNOTATIONS, gha2db_sync tool, skip calling annotations tool, default false
    pub skip_annotations: bool,
    /// From GHA2DB_SKIP_COLUMNS, gha2db_sync tool, skip calling columns tool, default false
    pub skip_columns: bool,
    /// From GHA2DB_RUN_COLUMNS, gha2db_sync tool, force calling columns tool, default false
    pub run_columns: bool,
    /// From GHA2DB_SKIP_VARS, gha2db_sync tool, skip calling vars tool, default false
    pub skip_vars: bool,
    /// From GHA2DB_SKIP_RAND, gha2db_sync tool, skip randomizing metrics calculation, default false
    pub skip_rand: bool,
    /// From GHA2DB_EXCLUDE_VARS, vars tool, default "" - comma separated list of variable names to exclude, example: "hostname,projects_health_partial_html"
    pub exclude_vars: BTreeMap<String, bool>,
    /// From GHA2DB_ONLY_VARS, vars tool, default "" - comma separated list of variable names to write (and skip all others): "hostname,projects_health_partial_html", not used if empty
    pub only_vars: BTreeMap<String, bool>,
    /// From GHA2DB_SKIP_SHAREDDB, annotations tool, default false, will skip writing to shared_db (from projects.yaml) if set
    pub skip_shared_db: bool,
    /// From GHA2DB_SKIP_PIDFILE, devstats tool, skip creating, checking and removing PID file
    pub skip_pid_file: bool,
    /// From GHA2DB_SKIP_COMPANY_ACQ, import_affs tool, skip processing company acquisitions from companies.yaml file
    pub skip_company_acq: bool,
    /// From GHA2DB_CHECK_PROVISION_FLAG, devstats tool - check if there is a 'provision' metric saved in 'gha_computed' table - if not, abort
    pub check_provision_flag: bool,
    /// From GHA2DB_CHECK_RUNNING_FLAG, devstats tool - check if there is a 'devstats_running' metric saved in 'gha_computed' table - if yes, abort
    pub check_running_flag: bool,
    /// From GHA2DB_SET_RUNNING_FLAG, devstats tool - set 'devstats_running' flag on 'gha_computed' table while devstats cronjob is running
    pub set_running_flag: bool,
    /// From GHA2DB_MAX_RUNNING_FLAG_AGE, how log "running_flag" can be present for next devstats sync to treat it as orphan, default "9h"
    pub max_running_flag_age: Duration,
    /// From GHA2DB_CHECK_IMPORTED_SHA, import_affs tool - check if given JSON was already imported using 'gha_imported_shas' table
    pub check_imported_sha: bool,
    /// From GHA2DB_ONLY_CHECK_IMPORTED_SHA, import_affs tool - check if given JSON was already imported using 'gha_imported_shas' table, do not attempt to import, only return status: 3=imported, 0=not imported
    pub only_check_imported_sha: bool,
    /// From GHA2DB_ENABLE_METRICS_DROP, if enabled will process each metric's 'drop:' property if present - use when regenerating affiliations data or reinitializing entire TSDB data
    pub enable_metrics_drop: bool,
    /// From GHA2DB_HTTP_TIMEOUT, gha2db - data.gharchive.org timeout value in minutes, default 2
    pub http_timeout: i64,
    /// From GHA2DB_HTTP_RETRY, gha2db - data.gharchive.org data fetch retries, default 4 (each retry takes 1*timeout*N), so in default config it will try timeouts: 1min, 2min, 3min, but if timeout is 3 and retry is 2, it will try 3min, 6min
    pub http_retry: i64,
    /// From GHA2DB_PROJECT_SCALE, calc_metric tool, project scale (default 1), some metrics can use this to adapt their SQLs to bigger/smaller projects
    pub project_scale: f64,
    /// From GHA2DB_PID_FILE_ROOT, devstats tool, use '/tmp/PidFileRoot.pid' as PID file, default 'devstats' -> '/tmp/devstats.pid'
    pub pid_file_root: String,
    /// Currently annotations tool read this from projects.yaml:shared_db and if set, outputs annotations data to the sharded DB in addition to the current DB
    pub shared_db: String,
    /// Used by annotations tool to store project's main repo name
    pub project_main_repo: String,
    /// True when running tests
    pub test_mode: bool,
    /// True, unless connecting to a custom database, in this case there can be multiple threads sharing context and we don't want to write to a random database
    pub can_reconnect: bool,
    /// True, can be disabled by GHA2DB_SKIP_COMMITS_FILES, get_repos tool
    pub commits_files_stats_enabled: bool,
    /// True, can be disabled by GHA2DB_SKIP_COMMITS_LOC, get_repos tool
    pub commits_loc_stats_enabled: bool,
    /// From GHA2DB_RECALC_RECIPROCAL: 1/RecalcReciprocal of recalc metric at given datetime, even if it should be calculated at this datetime, default 24 (means 4.1(6)%, or about once/day)
    pub recalc_reciprocal: i64,
    /// From GHA2DB_MAX_HIST: maximum histogram concurrency, default: 0 - means unlimited
    pub max_histograms: i64,
    /// From GHA2DB_MAX_RUN_DURATION, how log given programs can run and exist status after timeout, for example "tags:1h:0,calc_metric:12h:1"
    pub max_run_duration: BTreeMap<String, [i64; 2]>,
    /// Use rand to decide if a given date period must be calculated at this date or not.
    pub rand_compute_at_this_date: bool,
    /// From GHA2DB_REFRESH_COMMIT_ROLES - will process all commiths in DB and for every single one of them it will generate gha_commits_roles entries.
    pub refresh_commit_roles: bool,
    /// If set, then tags and columns will only be computed at random 0-5 hour, otherwise always when hour<6.
    pub allow_rand_tags_cols_compute: bool,
    /// From GHA2DB_ALLOW_METRIC_FAIL - if set, then calc_metric will not exit on first failed metric, but will try to compute all metrics.
    pub allow_metric_fail: bool,
    /// From GHA2DB_FETCH_COMMITS_MODE get_repos tool, mode to reconstruct gha_commits from git history for PushEvents: 0-disabled, 1-missing only (default), 2-missing+truncated
    pub fetch_commits_mode: i64,
    /// From GHA2DB_GIT_COMMITS_BATCH get_repos tool, max number of commit SHAs passed to git_commits.sh in one call, default 1000
    pub git_commits_batch: i64,
    /// From GHA2DB_RESTORE_ORPHAN_COMMITS, get_repos tool, enable restoring commits present in git but with no gha_commits row, binary default false (prod enables it via repos.sh/helm)
    pub restore_orphan_commits: bool,
    /// From GHA2DB_ORPHAN_COMMITS_RANGE, get_repos tool, orphan commits restore window, default '8 hours' (6h sync cadence + 2h overlap, keep equal to GHA2DB_RECENT_RANGE)
    pub orphan_commits_range: String,
    /// From GHA2DB_ORPHAN_COMMITS_DEFAULT_BRANCH_ONLY, get_repos tool, when set only the default branch is scanned for orphan commits, default: every `origin/*` branch whose tip moved inside the window
    pub orphan_commits_all_branches: bool,
    /// From GHA2DB_ORPHAN_COMMITS_NO_GROUPING, get_repos tool, when set restored commits keep the legacy shape (commit-date window, one artificial PushEvent per commit, author as actor), default: landing window + one GHA-shaped PushEvent per first-parent step (committer as actor)
    pub orphan_commits_group: bool,
}

static SYNCER_ONCE: OnceLock<()> = OnceLock::new();

impl Ctx {
    /// Read `GHA2DB_ST` / `GHA2DB_NCPUS` (Go `SetCPUs`).
    pub fn set_cpus(&mut self) {
        self.st = env_set("GHA2DB_ST");
        match env_int("GHA2DB_NCPUS") {
            None => self.ncpus = 0,
            Some(n) => {
                if n > 0 {
                    self.ncpus = n;
                    if self.ncpus == 1 {
                        self.st = true;
                    }
                }
            }
        }
    }

    /// Populate the context from environment variables (Go `Init`).
    pub fn init(&mut self) {
        SYNCER_ONCE.get_or_init(|| {
            update_env(false);
            start_env_syncer();
        });
        self.exec_fatal = true;
        self.exec_quiet = false;
        self.exec_output = false;
        self.can_reconnect = true;
        self.rand_compute_at_this_date = true;
        self.allow_rand_tags_cols_compute = false;

        // Commits analysis
        self.commits_files_stats_enabled = !env_set("GHA2DB_SKIP_COMMITS_FILES");
        self.commits_loc_stats_enabled = !env_set("GHA2DB_SKIP_COMMITS_LOC");

        // Data directory
        self.data_dir =
            ensure_trailing_slash(env_string_or("GHA2DB_DATADIR", consts::DEFAULT_DATA_DIR));

        // Outputs
        self.json_out = env_set("GHA2DB_JSON");
        self.db_out = !env_set("GHA2DB_NODB");

        // Dry run
        self.dry_run = env_set("GHA2DB_DRY_RUN");

        // GitHub API points and waiting for reset
        self.min_ghapi_points = 1;
        if let Some(pts) = env_int("GHA2DB_MIN_GHAPI_POINTS") {
            if pts >= 0 {
                self.min_ghapi_points = pts;
            }
        }
        self.max_ghapi_wait_seconds = 10;
        if let Some(secs) = env_int("GHA2DB_MAX_GHAPI_WAIT") {
            if secs >= 0 {
                self.max_ghapi_wait_seconds = secs;
            }
        }
        self.max_ghapi_retry = 6;
        if let Some(tr) = env_int("GHA2DB_MAX_GHAPI_RETRY") {
            if tr >= 1 {
                self.max_ghapi_retry = tr;
            }
        }
        self.ghapi_rate_limits_cache = 5;
        if let Some(secs) = env_int("GHA2DB_GHAPI_RATE_LIMITS_CACHE") {
            if secs >= 0 {
                self.ghapi_rate_limits_cache = secs;
            }
        }

        // Debug
        self.debug = 0;
        if let Some(level) = env_int("GHA2DB_DEBUG") {
            if level != 0 {
                self.debug = level;
            }
        }
        self.cmd_debug = env_int("GHA2DB_CMDDEBUG").unwrap_or(0);
        self.github_debug = env_int("GHA2DB_GITHUB_DEBUG").unwrap_or(0);
        self.q_out = env_set("GHA2DB_QOUT");
        self.ctx_out = env_set("GHA2DB_CTXOUT");

        // Threading
        self.set_cpus();

        // Postgres DB
        self.pg_host = env_string_or("PG_HOST", consts::LOCALHOST);
        self.pg_port = env_string_or("PG_PORT", "5432");
        self.pg_db = env_string_or("PG_DB", consts::GHA);
        self.affiliations_db = getenv("GHA2DB_AFFILIATIONS_DB");
        self.pg_user = env_string_or("PG_USER", consts::GHA_ADMIN);
        self.pg_pass = env_string_or("PG_PASS", consts::PASSWORD);
        self.pg_ssl = env_string_or("PG_SSL", "disable");

        // PID file
        self.pid_file_root = env_string_or("GHA2DB_PID_FILE_ROOT", consts::DEVSTATS);

        // Environment controlling index creation, table & tools
        self.index = env_set("GHA2DB_INDEX");
        self.table = !env_set("GHA2DB_SKIPTABLE");
        self.tools = !env_set("GHA2DB_SKIPTOOLS");
        self.mgetc = getenv("GHA2DB_MGETC");
        if self.mgetc.len() > 1 {
            // Go takes the first *byte*; keep the first character instead so
            // the result stays valid UTF-8.
            let first = self
                .mgetc
                .chars()
                .next()
                .map(|c| c.to_string())
                .unwrap_or_default();
            self.mgetc = first;
        }

        // Log Time
        self.log_time = !env_set("GHA2DB_SKIPTIME");

        // Time offset for gha2db_sync
        self.tm_offset = env_int("GHA2DB_TMOFFSET").unwrap_or(0);

        // Default start date
        let start_dt = getenv("GHA2DB_STARTDT");
        self.default_start_date = if start_dt.is_empty() {
            ymd_hms(2012, 7, 1, 0, 0, 0)
        } else {
            time_parse_any(&start_dt)
        };
        self.force_start_date = env_set("GHA2DB_STARTDT_FORCE");

        // Skip ghapi2db and/or get_repos
        self.skip_get_repos = env_set("GHA2DB_GETREPOSSKIP");
        self.skip_ghapi = env_set("GHA2DB_GHAPISKIP");
        self.skip_api_events = env_set("GHA2DB_GHAPISKIPEVENTS");
        self.skip_api_issues = env_set("GHA2DB_GHAPISKIPISSUES");
        self.skip_api_prs = env_set("GHA2DB_GHAPISKIPPRS");
        self.allow_ghapi_insert_fail = env_set("GHA2DB_GHAPIALLOWINSERTFAIL");
        self.postprocess_from = getenv("GHA2DB_POSTPROCESS_FROM");
        self.postprocess_to = getenv("GHA2DB_POSTPROCESS_TO");
        if !self.postprocess_from.is_empty() || !self.postprocess_to.is_empty() {
            if self.postprocess_from.is_empty() || self.postprocess_to.is_empty() {
                fatalf(format_args!(
                    "GHA2DB_POSTPROCESS_FROM and GHA2DB_POSTPROCESS_TO must both be set (or both empty), got from='{}' to='{}'",
                    self.postprocess_from, self.postprocess_to
                ));
            }
            let pp_from = time_parse_any(&self.postprocess_from);
            let pp_to = time_parse_any(&self.postprocess_to);
            if pp_from >= pp_to {
                fatalf(format_args!(
                    "GHA2DB_POSTPROCESS_FROM ({}) must be strictly before GHA2DB_POSTPROCESS_TO ({})",
                    self.postprocess_from, self.postprocess_to
                ));
            }
            self.postprocess_from = to_ymdhms_date(pp_from);
            self.postprocess_to = to_ymdhms_date(pp_to);
        }
        self.skip_api_commits = env_set("GHA2DB_GHAPISKIPCOMMITS");
        self.skip_api_licenses = env_set("GHA2DB_GHAPISKIPLICENSES");
        self.force_api_licenses = env_set("GHA2DB_GHAPIFORCELICENSES");
        self.skip_api_langs = env_set("GHA2DB_GHAPISKIPLANGS");
        self.force_api_langs = env_set("GHA2DB_GHAPIFORCELANGS");
        self.skip_api_comments = env_set("GHA2DB_GHAPISKIPCOMMENTS");
        self.skip_api_reviews = env_set("GHA2DB_GHAPISKIPREVIEWS");
        self.skip_api_forks = env_set("GHA2DB_GHAPISKIPFORKS");
        self.skip_api_releases = env_set("GHA2DB_GHAPISKIPRELEASES");
        self.skip_api_stars = env_set("GHA2DB_GHAPISKIPSTARS");
        self.skip_api_repo_stats = env_set("GHA2DB_GHAPISKIPREPOSTATS");
        self.skip_api_repo_events = env_set("GHA2DB_GHAPISKIPREPOEVENTS");
        self.ghapi_all_repos = !env_set("GHA2DB_GHAPI_RECENT_REPOS_ONLY");
        self.ghapi_error_is_fatal = env_set("GHA2DB_GHAPI_ERROR_FATAL");
        self.auto_fetch_commits = !env_set("GHA2DB_NO_AUTOFETCHCOMMITS");

        // Last TS series
        self.last_series = env_string_or("GHA2DB_LASTSERIES", "events_h");

        // Skip some tools
        self.skip_tags = env_set("GHA2DB_SKIP_TAGS");
        self.skip_annotations = env_set("GHA2DB_SKIP_ANNOTATIONS");
        self.skip_columns = env_set("GHA2DB_SKIP_COLUMNS");
        self.run_columns = env_set("GHA2DB_RUN_COLUMNS");
        self.skip_vars = env_set("GHA2DB_SKIP_VARS");

        // Skip randomizing task order
        self.skip_rand = env_set("GHA2DB_SKIP_RAND");

        // TS variables
        self.skip_tsdb = env_set("GHA2DB_SKIPTSDB");
        self.reset_tsdb = env_set("GHA2DB_RESETTSDB");
        self.reset_ranges = env_set("GHA2DB_RESETRANGES");

        // Allow broken JSON
        self.allow_broken_json = env_set("GHA2DB_ALLOW_BROKEN_JSON");

        // Allow metric fail
        self.allow_metric_fail = env_set("GHA2DB_ALLOW_METRIC_FAIL");

        // Fetch commits / gha_commits reconstruction mode (get_repos)
        self.fetch_commits_mode = 1;
        if let Some(mode) = env_int("GHA2DB_FETCH_COMMITS_MODE") {
            if mode >= 0 {
                self.fetch_commits_mode = mode;
            }
        }

        // Max number of commit SHAs passed to git_commits.sh in one call (get_repos)
        self.git_commits_batch = 1000;
        if let Some(b) = env_int("GHA2DB_GIT_COMMITS_BATCH") {
            if b > 0 {
                self.git_commits_batch = b;
            }
        }

        // Restore orphan commits (get_repos)
        self.restore_orphan_commits = env_set("GHA2DB_RESTORE_ORPHAN_COMMITS");
        self.orphan_commits_range = env_string_or("GHA2DB_ORPHAN_COMMITS_RANGE", "8 hours");
        self.orphan_commits_all_branches = !env_set("GHA2DB_ORPHAN_COMMITS_DEFAULT_BRANCH_ONLY");
        self.orphan_commits_group = !env_set("GHA2DB_ORPHAN_COMMITS_NO_GROUPING");

        // Run website_data tool after sync
        self.website_data = env_set("GHA2DB_WEBSITEDATA");

        // Disable delete & recreate past events
        self.skip_update_events = env_set("GHA2DB_SKIP_UPDATE_EVENTS");

        // Postgres DB variables
        self.skip_pdb = env_set("GHA2DB_SKIPPDB");

        // Explain
        self.explain = env_set("GHA2DB_EXPLAIN");

        // Old (pre 2015) GHA JSONs format
        self.old_format = env_set("GHA2DB_OLDFMT");

        // Exact repository full names to match
        self.exact = env_set("GHA2DB_EXACT");

        // Log to Postgres DB, table `devstats`.`gha_logs`
        self.log_to_db = !env_set("GHA2DB_SKIPLOG");

        // Local data files mode
        self.local = env_set("GHA2DB_LOCAL");

        // Local binary/shell files mode
        self.local_cmd = env_set("GHA2DB_LOCAL_CMD");

        // Absolute data files mode
        self.absolute = env_set("GHA2DB_ABSOLUTE");

        // Project
        self.project = getenv("GHA2DB_PROJECT");
        let proj = if self.project.is_empty() {
            String::new()
        } else {
            format!("{}/", self.project)
        };

        // YAML config files
        self.vars_fn_yaml = env_string_or("GHA2DB_VARS_FN_YAML", "vars.yaml");
        self.metrics_yaml = env_string_or(
            "GHA2DB_METRICS_YAML",
            &format!("metrics/{}metrics.yaml", proj),
        );
        self.tags_yaml = env_string_or("GHA2DB_TAGS_YAML", &format!("metrics/{}tags.yaml", proj));
        self.columns_yaml = env_string_or(
            "GHA2DB_COLUMNS_YAML",
            &format!("metrics/{}columns.yaml", proj),
        );
        self.vars_yaml = env_string_or(
            "GHA2DB_VARS_YAML",
            &format!("metrics/{}{}", proj, self.vars_fn_yaml),
        );

        // GitHub OAuth
        self.github_oauth = getenv("GHA2DB_GITHUB_OAUTH");
        if self.github_oauth.is_empty() {
            self.github_oauth = "-".to_string();
            for fn_ in ["/etc/github/oauths", "/etc/github/oauth"] {
                match std::fs::metadata(fn_) {
                    Ok(_) => {
                        self.github_oauth = fn_.to_string();
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => fatal_no_log(format!("stat {}: {}", fn_, go_io_error_string(&e))),
                }
            }
        }

        // GitHub API base URL override (GitHub Enterprise / testing)
        self.github_api_url = getenv("GHA2DB_GITHUB_API_URL");
        if !self.github_api_url.is_empty() && !self.github_api_url.ends_with('/') {
            self.github_api_url.push('/');
        }

        // GH Archive base URL override (mirrors / testing)
        self.gharchive_url = getenv("GHA2DB_GHARCHIVE_URL");
        if !self.gharchive_url.is_empty() && !self.gharchive_url.ends_with('/') {
            self.gharchive_url.push('/');
        }

        // Max DB logs age
        self.clear_db_period = env_string_or("GHA2DB_MAXLOGAGE", "1 week");

        // Max locks ages
        self.clear_affs_lock_period = env_string_or("GHA2DB_MAX_AFFS_LOCK_AGE", "16 hours");
        self.clear_giant_lock_period = env_string_or("GHA2DB_MAX_GIANT_LOCK_AGE", "40 hours");

        // Trials
        self.trials = env_int_list_or("GHA2DB_TRIALS", &[10, 30, 60, 120, 300, 600, 1200, 3600]);

        // Deploy statuses and branches
        self.deploy_branches = env_list_or("GHA2DB_DEPLOY_BRANCHES", &["master"]);
        self.deploy_statuses = env_list_or("GHA2DB_DEPLOY_STATUSES", &["Passed", "Fixed"]);
        self.deploy_types = env_list_or("GHA2DB_DEPLOY_TYPES", &["push"]);
        self.deploy_results = env_int_list_or("GHA2DB_DEPLOY_RESULTS", &[0]);
        self.project_root = getenv("GHA2DB_PROJECT_ROOT");

        // Projects sync override
        self.projects_override = BTreeMap::new();
        let overrides = getenv("GHA2DB_PROJECTS_OVERRIDE");
        if !overrides.is_empty() {
            for override_ in overrides.split(',') {
                if override_.is_empty() {
                    continue;
                }
                let mut chars = override_.chars();
                let mode = chars.next().unwrap_or_default();
                let project = chars.as_str();
                if project.is_empty() {
                    continue;
                }
                if mode == '-' {
                    self.projects_override.insert(project.to_string(), false);
                } else if mode == '+' {
                    self.projects_override.insert(project.to_string(), true);
                }
            }
        }

        // Exclude repos, exclude vars, only vars, only metrics, exclude metrics
        self.exclude_repos = comma_set("GHA2DB_EXCLUDE_REPOS");
        self.exclude_vars = comma_set("GHA2DB_EXCLUDE_VARS");
        self.only_vars = comma_set("GHA2DB_ONLY_VARS");
        self.only_metrics = comma_set("GHA2DB_ONLY_METRICS");
        self.skip_metrics = comma_set("GHA2DB_SKIP_METRICS");

        // WebHook Host, Port, Root
        self.web_hook_host = env_string_or("GHA2DB_WHHOST", "127.0.0.1");
        self.web_hook_port = getenv("GHA2DB_WHPORT");
        if self.web_hook_port.is_empty() {
            self.web_hook_port = ":1982".to_string();
        } else if !self.web_hook_port.starts_with(':') {
            self.web_hook_port = format!(":{}", self.web_hook_port);
        }
        // API Host, Port
        self.api_host = env_string_or("GHA2DB_API_HOST", "0.0.0.0");
        self.api_port = getenv("GHA2DB_API_PORT");
        if self.api_port.is_empty() {
            self.api_port = ":8080".to_string();
        } else if !self.api_port.starts_with(':') {
            self.api_port = format!(":{}", self.api_port);
        }
        self.web_hook_root = env_string_or("GHA2DB_WHROOT", "/hook");
        self.check_payload = !env_set("GHA2DB_SKIP_VERIFY_PAYLOAD");
        self.full_deploy = !env_set("GHA2DB_SKIP_FULL_DEPLOY");

        // Tests
        self.tests_yaml = env_string_or("GHA2DB_TESTS_YAML", "tests.yaml");

        // Skip dates
        self.skip_dates_yaml = env_string_or("GHA2DB_SKIP_DATES_YAML", "skip_dates.yaml");

        // Main projects file
        self.projects_yaml = env_string_or("GHA2DB_PROJECTS_YAML", "projects.yaml");

        // Main affiliations file
        self.affiliations_json = env_string_or("GHA2DB_AFFILIATIONS_JSON", "github_users.json");

        // Company acquisitions file
        self.company_acq_yaml = env_string_or("GHA2DB_COMPANY_ACQ_YAML", "companies.yaml");

        // `get_repos` repositories dir
        self.repos_dir = getenv("GHA2DB_REPOS_DIR");
        if self.repos_dir.is_empty() {
            self.repos_dir = format!("{}/devstats_repos/", getenv("HOME"));
        }
        self.repos_dir = ensure_trailing_slash(std::mem::take(&mut self.repos_dir));
        // `get_repos`: process repos, process commits, external info
        self.process_repos = env_set("GHA2DB_PROCESS_REPOS");
        self.process_commits = env_set("GHA2DB_PROCESS_COMMITS");
        self.external_info = env_set("GHA2DB_EXTERNAL_INFO");
        self.projects_commits = getenv("GHA2DB_PROJECTS_COMMITS");

        // PropagateOnlyVar
        self.propagate_only_var = env_set("GHA2DB_PROPAGATE_ONLY_VAR");
        if self.propagate_only_var {
            let only = getenv("ONLY");
            if !only.is_empty() && self.projects_commits.is_empty() {
                self.projects_commits = only.replace(' ', ",");
            }
        }

        // `website_data` JSONs dir
        self.jsons_dir = ensure_trailing_slash(env_string_or("GHA2DB_JSONS_DIR", "./jsons/"));

        // HTTP Timeout & retry
        self.http_timeout = env_int("GHA2DB_HTTP_TIMEOUT").unwrap_or(3);
        self.http_retry = env_int("GHA2DB_HTTP_RETRY").unwrap_or(5);

        // Skip writing to shared_db from projects.yaml
        self.skip_shared_db = env_set("GHA2DB_SKIP_SHAREDDB");

        // Skip PID file
        self.skip_pid_file = env_set("GHA2DB_SKIP_PIDFILE");

        // Skip company acquisitions file
        self.skip_company_acq = env_set("GHA2DB_SKIP_COMPANY_ACQ");

        // Provision / running flags
        self.check_provision_flag = env_set("GHA2DB_CHECK_PROVISION_FLAG");
        self.check_running_flag = env_set("GHA2DB_CHECK_RUNNING_FLAG");
        self.set_running_flag = env_set("GHA2DB_SET_RUNNING_FLAG");

        let mrfa = getenv("GHA2DB_MAX_RUNNING_FLAG_AGE");
        self.max_running_flag_age = if mrfa.is_empty() {
            Duration::from_secs(9 * 3600)
        } else {
            match parse_go_duration(&mrfa) {
                Ok(d) => d,
                Err(e) => fatal_no_log(e),
            }
        };

        // Check Imported SHAs
        self.check_imported_sha = env_set("GHA2DB_CHECK_IMPORTED_SHA");
        self.only_check_imported_sha = env_set("GHA2DB_ONLY_CHECK_IMPORTED_SHA");

        // Calculate all periods?
        self.compute_all = env_set("GHA2DB_COMPUTE_ALL");

        // Force compute periods
        self.compute_periods = None;
        let periods = getenv("GHA2DB_FORCE_PERIODS");
        if !periods.is_empty() {
            for data in periods.split(',') {
                let ary2: Vec<&str> = data.split(':').collect();
                if ary2.len() != 2 {
                    continue;
                }
                let period = ary2[0];
                let shist = ary2[1].trim();
                if shist != "t" && shist != "f" {
                    continue;
                }
                let hist = shist == "t";
                self.compute_periods
                    .get_or_insert_with(BTreeMap::new)
                    .entry(period.to_string())
                    .or_default()
                    .insert(hist);
            }
        }

        // Max run durations: "tags:1h:0,calc_metric:12h:1"
        self.max_run_duration = BTreeMap::new();
        let data = getenv("GHA2DB_MAX_RUN_DURATION");
        if !data.is_empty() {
            for item in data.split(',') {
                let ary2: Vec<&str> = item.split(':').collect();
                if ary2.len() != 3 {
                    continue;
                }
                let prog = ary2[0].trim();
                let dur_s = ary2[1].trim();
                let d = match parse_go_duration(dur_s) {
                    Ok(d) => d,
                    Err(e) => fatal_no_log(e),
                };
                let dur = d.as_secs_f64() as i64;
                let status = match parse_go_int(ary2[2].trim()) {
                    Ok(n) => n,
                    Err(e) => fatal_no_log(e),
                };
                if self.max_run_duration.contains_key(prog) {
                    fatal_no_log(format!(
                        "program '{}' already defined (in MaxRunDuration): {}",
                        prog,
                        map_arr2_string(&self.max_run_duration)
                    ));
                }
                self.max_run_duration
                    .insert(prog.to_string(), [dur, status]);
            }
        }

        // Actor filtering?
        self.actors_filter = env_set("GHA2DB_ACTORS_FILTER");
        self.actors_allow = None;
        self.actors_forbid = None;
        if self.actors_filter {
            let allow = getenv("GHA2DB_ACTORS_ALLOW");
            if !allow.is_empty() {
                self.actors_allow = Some(GoRegex::must(&allow));
            }
            let forbid = getenv("GHA2DB_ACTORS_FORBID");
            if !forbid.is_empty() {
                self.actors_forbid = Some(GoRegex::must(&forbid));
            }
        }

        // `merge_dbs` tool - input DBs and output DB
        let dbs = getenv("GHA2DB_INPUT_DBS");
        self.input_dbs = if dbs.is_empty() {
            Vec::new()
        } else {
            dbs.split(',').map(|s| s.to_string()).collect()
        };
        self.output_db = getenv("GHA2DB_OUTPUT_DB");

        // RecentRange - ghapi2db will check issues/PRs from now() - this range to now()
        self.recent_range = env_string_or("GHA2DB_RECENT_RANGE", "8 hours");
        self.recent_repos_range = env_string_or("GHA2DB_RECENT_REPOS_RANGE", "1 day");

        // Enable drop metrics support
        self.enable_metrics_drop = env_set("GHA2DB_ENABLE_METRICS_DROP");

        // Refresh commit roles
        self.refresh_commit_roles = env_set("GHA2DB_REFRESH_COMMIT_ROLES");

        // Project Scale
        let scale = getenv("GHA2DB_PROJECT_SCALE");
        self.project_scale = if scale.is_empty() {
            1.0
        } else {
            match parse_go_float(&scale) {
                Ok(f) => f,
                Err(e) => fatal_no_log(e),
            }
        };

        // CSV file
        self.csv_file = getenv("GHA2DB_CSVOUT");

        // RecalcReciprocal
        self.recalc_reciprocal = match env_int("GHA2DB_RECALC_RECIPROCAL") {
            Some(rr) if rr > 0 => rr,
            _ => 24,
        };

        // MaxHistograms
        self.max_histograms = 0;
        if let Some(mh) = env_int("GHA2DB_MAX_HIST") {
            if mh > 0 {
                self.max_histograms = mh;
            }
        }

        // Context out if requested
        if self.ctx_out {
            self.print();
        }
    }

    /// Go `%+v` rendering of the context: `&{Field:value Field2:value ...}`.
    pub fn go_string(&self) -> String {
        let parts: Vec<String> = self
            .go_fields()
            .into_iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect();
        format!("&{{{}}}", parts.join(" "))
    }

    /// Print the context contents (Go `Print`).
    pub fn print(&self) {
        println!("Environment Context Dump\n{}", self.go_string());
    }

    /// `(GoFieldName, Go %v value)` pairs in declaration order.
    pub fn go_fields(&self) -> Vec<(&'static str, String)> {
        vec![
            ("DataDir", self.data_dir.clone()),
            ("Debug", self.debug.to_string()),
            ("CmdDebug", self.cmd_debug.to_string()),
            ("GitHubDebug", self.github_debug.to_string()),
            ("DryRun", self.dry_run.to_string()),
            ("JSONOut", self.json_out.to_string()),
            ("DBOut", self.db_out.to_string()),
            ("ST", self.st.to_string()),
            ("NCPUs", self.ncpus.to_string()),
            ("PgHost", self.pg_host.clone()),
            ("PgPort", self.pg_port.clone()),
            ("PgDB", self.pg_db.clone()),
            ("AffiliationsDB", self.affiliations_db.clone()),
            ("PgUser", self.pg_user.clone()),
            ("PgPass", self.pg_pass.clone()),
            ("PgSSL", self.pg_ssl.clone()),
            ("Index", self.index.to_string()),
            ("Table", self.table.to_string()),
            ("Tools", self.tools.to_string()),
            ("Mgetc", self.mgetc.clone()),
            ("QOut", self.q_out.to_string()),
            ("CtxOut", self.ctx_out.to_string()),
            ("LogTime", self.log_time.to_string()),
            ("DefaultStartDate", gofmt::time(self.default_start_date)),
            ("ForceStartDate", self.force_start_date.to_string()),
            ("LastSeries", self.last_series.clone()),
            ("SkipTSDB", self.skip_tsdb.to_string()),
            ("SkipPDB", self.skip_pdb.to_string()),
            ("ResetTSDB", self.reset_tsdb.to_string()),
            ("ResetRanges", self.reset_ranges.to_string()),
            ("Explain", self.explain.to_string()),
            ("OldFormat", self.old_format.to_string()),
            ("Exact", self.exact.to_string()),
            ("LogToDB", self.log_to_db.to_string()),
            ("Local", self.local.to_string()),
            ("Absolute", self.absolute.to_string()),
            ("LocalCmd", self.local_cmd.to_string()),
            ("MetricsYaml", self.metrics_yaml.clone()),
            ("TagsYaml", self.tags_yaml.clone()),
            ("ColumnsYaml", self.columns_yaml.clone()),
            ("VarsYaml", self.vars_yaml.clone()),
            ("VarsFnYaml", self.vars_fn_yaml.clone()),
            ("SkipDatesYaml", self.skip_dates_yaml.clone()),
            ("GitHubOAuth", self.github_oauth.clone()),
            ("GitHubAPIURL", self.github_api_url.clone()),
            ("GHArchiveURL", self.gharchive_url.clone()),
            ("ClearDBPeriod", self.clear_db_period.clone()),
            ("ClearAffsLockPeriod", self.clear_affs_lock_period.clone()),
            ("ClearGiantLockPeriod", self.clear_giant_lock_period.clone()),
            ("Trials", gofmt::slice(&self.trials)),
            ("WebHookRoot", self.web_hook_root.clone()),
            ("WebHookPort", self.web_hook_port.clone()),
            ("WebHookHost", self.web_hook_host.clone()),
            ("APIHost", self.api_host.clone()),
            ("APIPort", self.api_port.clone()),
            ("CheckPayload", self.check_payload.to_string()),
            ("FullDeploy", self.full_deploy.to_string()),
            ("DeployBranches", gofmt::slice(&self.deploy_branches)),
            ("DeployStatuses", gofmt::slice(&self.deploy_statuses)),
            ("DeployResults", gofmt::slice(&self.deploy_results)),
            ("DeployTypes", gofmt::slice(&self.deploy_types)),
            ("ProjectRoot", self.project_root.clone()),
            ("ExecFatal", self.exec_fatal.to_string()),
            ("ExecQuiet", self.exec_quiet.to_string()),
            ("ExecOutput", self.exec_output.to_string()),
            ("Project", self.project.clone()),
            ("TestsYaml", self.tests_yaml.clone()),
            ("ReposDir", self.repos_dir.clone()),
            ("ProcessRepos", self.process_repos.to_string()),
            ("ProcessCommits", self.process_commits.to_string()),
            ("ExternalInfo", self.external_info.to_string()),
            ("ProjectsCommits", self.projects_commits.clone()),
            ("PropagateOnlyVar", self.propagate_only_var.to_string()),
            ("ProjectsYaml", self.projects_yaml.clone()),
            ("CompanyAcqYaml", self.company_acq_yaml.clone()),
            ("ProjectsOverride", gofmt::map(&self.projects_override)),
            ("AffiliationsJSON", self.affiliations_json.clone()),
            ("ExcludeRepos", gofmt::map(&self.exclude_repos)),
            ("InputDBs", gofmt::slice(&self.input_dbs)),
            ("OutputDB", self.output_db.clone()),
            ("TmOffset", self.tm_offset.to_string()),
            ("DefaultHostname", self.default_hostname.clone()),
            ("RecentRange", self.recent_range.clone()),
            ("RecentReposRange", self.recent_repos_range.clone()),
            ("MinGHAPIPoints", self.min_ghapi_points.to_string()),
            (
                "MaxGHAPIWaitSeconds",
                self.max_ghapi_wait_seconds.to_string(),
            ),
            ("MaxGHAPIRetry", self.max_ghapi_retry.to_string()),
            (
                "GHAPIRateLimitsCache",
                self.ghapi_rate_limits_cache.to_string(),
            ),
            ("GHAPIErrorIsFatal", self.ghapi_error_is_fatal.to_string()),
            ("SkipGHAPI", self.skip_ghapi.to_string()),
            ("SkipAPIEvents", self.skip_api_events.to_string()),
            ("SkipAPIIssues", self.skip_api_issues.to_string()),
            ("SkipAPIPRs", self.skip_api_prs.to_string()),
            (
                "AllowGHAPIInsertFail",
                self.allow_ghapi_insert_fail.to_string(),
            ),
            ("PostprocessFrom", self.postprocess_from.clone()),
            ("PostprocessTo", self.postprocess_to.clone()),
            ("SkipAPICommits", self.skip_api_commits.to_string()),
            ("SkipAPILicenses", self.skip_api_licenses.to_string()),
            ("ForceAPILicenses", self.force_api_licenses.to_string()),
            ("SkipAPILangs", self.skip_api_langs.to_string()),
            ("ForceAPILangs", self.force_api_langs.to_string()),
            ("SkipAPIComments", self.skip_api_comments.to_string()),
            ("SkipAPIReviews", self.skip_api_reviews.to_string()),
            ("SkipAPIForks", self.skip_api_forks.to_string()),
            ("SkipAPIReleases", self.skip_api_releases.to_string()),
            ("SkipAPIStars", self.skip_api_stars.to_string()),
            ("SkipAPIRepoStats", self.skip_api_repo_stats.to_string()),
            ("SkipAPIRepoEvents", self.skip_api_repo_events.to_string()),
            ("GHAPIAllRepos", self.ghapi_all_repos.to_string()),
            ("SkipGetRepos", self.skip_get_repos.to_string()),
            ("CSVFile", self.csv_file.clone()),
            ("ComputeAll", self.compute_all.to_string()),
            ("ActorsFilter", self.actors_filter.to_string()),
            ("ActorsAllow", opt_regex_string(self.actors_allow.as_ref())),
            (
                "ActorsForbid",
                opt_regex_string(self.actors_forbid.as_ref()),
            ),
            ("SkipMetrics", gofmt::map(&self.skip_metrics)),
            ("OnlyMetrics", gofmt::map(&self.only_metrics)),
            ("AllowBrokenJSON", self.allow_broken_json.to_string()),
            ("JSONsDir", self.jsons_dir.clone()),
            ("WebsiteData", self.website_data.to_string()),
            ("SkipUpdateEvents", self.skip_update_events.to_string()),
            (
                "ComputePeriods",
                compute_periods_string(self.compute_periods.as_ref()),
            ),
            ("AutoFetchCommits", self.auto_fetch_commits.to_string()),
            ("SkipTags", self.skip_tags.to_string()),
            ("SkipAnnotations", self.skip_annotations.to_string()),
            ("SkipColumns", self.skip_columns.to_string()),
            ("RunColumns", self.run_columns.to_string()),
            ("SkipVars", self.skip_vars.to_string()),
            ("SkipRand", self.skip_rand.to_string()),
            ("ExcludeVars", gofmt::map(&self.exclude_vars)),
            ("OnlyVars", gofmt::map(&self.only_vars)),
            ("SkipSharedDB", self.skip_shared_db.to_string()),
            ("SkipPIDFile", self.skip_pid_file.to_string()),
            ("SkipCompanyAcq", self.skip_company_acq.to_string()),
            ("CheckProvisionFlag", self.check_provision_flag.to_string()),
            ("CheckRunningFlag", self.check_running_flag.to_string()),
            ("SetRunningFlag", self.set_running_flag.to_string()),
            (
                "MaxRunningFlagAge",
                format_go_duration(self.max_running_flag_age),
            ),
            ("CheckImportedSHA", self.check_imported_sha.to_string()),
            (
                "OnlyCheckImportedSHA",
                self.only_check_imported_sha.to_string(),
            ),
            ("EnableMetricsDrop", self.enable_metrics_drop.to_string()),
            ("HTTPTimeout", self.http_timeout.to_string()),
            ("HTTPRetry", self.http_retry.to_string()),
            ("ProjectScale", gofmt::float(self.project_scale)),
            ("PidFileRoot", self.pid_file_root.clone()),
            ("SharedDB", self.shared_db.clone()),
            ("ProjectMainRepo", self.project_main_repo.clone()),
            ("TestMode", self.test_mode.to_string()),
            ("CanReconnect", self.can_reconnect.to_string()),
            (
                "CommitsFilesStatsEnabled",
                self.commits_files_stats_enabled.to_string(),
            ),
            (
                "CommitsLOCStatsEnabled",
                self.commits_loc_stats_enabled.to_string(),
            ),
            ("RecalcReciprocal", self.recalc_reciprocal.to_string()),
            ("MaxHistograms", self.max_histograms.to_string()),
            ("MaxRunDuration", map_arr2_string(&self.max_run_duration)),
            (
                "RandComputeAtThisDate",
                self.rand_compute_at_this_date.to_string(),
            ),
            ("RefreshCommitRoles", self.refresh_commit_roles.to_string()),
            (
                "AllowRandTagsColsCompute",
                self.allow_rand_tags_cols_compute.to_string(),
            ),
            ("AllowMetricFail", self.allow_metric_fail.to_string()),
            ("FetchCommitsMode", self.fetch_commits_mode.to_string()),
            ("GitCommitsBatch", self.git_commits_batch.to_string()),
            (
                "RestoreOrphanCommits",
                self.restore_orphan_commits.to_string(),
            ),
            ("OrphanCommitsRange", self.orphan_commits_range.clone()),
            (
                "OrphanCommitsAllBranches",
                self.orphan_commits_all_branches.to_string(),
            ),
            ("OrphanCommitsGroup", self.orphan_commits_group.to_string()),
        ]
    }

    /// Go `CopyContext`: a copy of the context without the runtime-only
    /// fields (`DefaultHostname`, `SharedDB`, `ProjectMainRepo`,
    /// `RandComputeAtThisDate`, `RefreshCommitRoles`,
    /// `AllowRandTagsColsCompute`), which stay at their zero values.
    pub fn copy_context(&self) -> Ctx {
        Ctx {
            default_hostname: String::new(),
            shared_db: String::new(),
            project_main_repo: String::new(),
            rand_compute_at_this_date: false,
            refresh_commit_roles: false,
            allow_rand_tags_cols_compute: false,
            ..self.clone()
        }
    }
}

/// Helpers shared by tests that mutate the process environment.
#[doc(hidden)]
pub mod test_support {
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Serialize tests touching environment variables.
    pub fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Set (`Some`) or remove (`None`) an environment variable.
    pub fn set_or_unset(name: &str, value: Option<&str>) {
        match value {
            Some(v) => crate::env::set_var(name, v),
            None => crate::env::remove_var(name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{env_lock, set_or_unset};
    use super::*;

    fn home() -> String {
        getenv("HOME")
    }

    fn svec(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn bmap(items: &[(&str, bool)]) -> BTreeMap<String, bool> {
        items.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn periods(items: &[(&str, &[bool])]) -> BTreeMap<String, BTreeSet<bool>> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().copied().collect()))
            .collect()
    }

    fn durs(items: &[(&str, [i64; 2])]) -> BTreeMap<String, [i64; 2]> {
        items.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    /// The expected default state (Go `defaultContext` in `context_test.go`).
    fn default_context() -> Ctx {
        let mut pass = getenv("PG_PASS");
        if pass.is_empty() {
            pass = consts::PASSWORD.to_string();
        }
        Ctx {
            data_dir: "/etc/gha2db/".to_string(),
            debug: 0,
            cmd_debug: 0,
            github_debug: 0,
            min_ghapi_points: 1,
            max_ghapi_wait_seconds: 10,
            max_ghapi_retry: 6,
            ghapi_rate_limits_cache: 5,
            json_out: false,
            db_out: true,
            dry_run: false,
            st: false,
            ncpus: 0,
            pg_host: "localhost".to_string(),
            pg_port: "5432".to_string(),
            pg_db: "gha".to_string(),
            affiliations_db: String::new(),
            pg_user: "gha_admin".to_string(),
            pg_pass: pass,
            pg_ssl: "disable".to_string(),
            index: false,
            table: true,
            tools: true,
            mgetc: String::new(),
            q_out: false,
            ctx_out: false,
            default_start_date: ymd_hms(2012, 7, 1, 0, 0, 0),
            force_start_date: false,
            last_series: "events_h".to_string(),
            auto_fetch_commits: true,
            log_to_db: true,
            metrics_yaml: "metrics/metrics.yaml".to_string(),
            tags_yaml: "metrics/tags.yaml".to_string(),
            columns_yaml: "metrics/columns.yaml".to_string(),
            vars_yaml: "metrics/vars.yaml".to_string(),
            vars_fn_yaml: "vars.yaml".to_string(),
            github_oauth: "not_use".to_string(),
            clear_db_period: "1 week".to_string(),
            clear_affs_lock_period: "16 hours".to_string(),
            clear_giant_lock_period: "40 hours".to_string(),
            trials: vec![10, 30, 60, 120, 300, 600, 1200, 3600],
            log_time: true,
            web_hook_root: "/hook".to_string(),
            web_hook_port: ":1982".to_string(),
            web_hook_host: "127.0.0.1".to_string(),
            api_host: "0.0.0.0".to_string(),
            api_port: ":8080".to_string(),
            check_payload: true,
            full_deploy: true,
            deploy_branches: svec(&["master"]),
            deploy_statuses: svec(&["Passed", "Fixed"]),
            deploy_results: vec![0],
            deploy_types: svec(&["push"]),
            project_root: String::new(),
            project: String::new(),
            tests_yaml: "tests.yaml".to_string(),
            skip_dates_yaml: "skip_dates.yaml".to_string(),
            repos_dir: home() + "/devstats_repos/",
            jsons_dir: "./jsons/".to_string(),
            exec_fatal: true,
            exec_quiet: false,
            exec_output: false,
            projects_yaml: "projects.yaml".to_string(),
            company_acq_yaml: "companies.yaml".to_string(),
            affiliations_json: "github_users.json".to_string(),
            recent_range: "8 hours".to_string(),
            recent_repos_range: "1 day".to_string(),
            max_running_flag_age: Duration::from_secs(9 * 3600),
            pid_file_root: "devstats".to_string(),
            test_mode: true,
            http_timeout: 3,
            http_retry: 5,
            project_scale: 1.0,
            can_reconnect: true,
            commits_files_stats_enabled: true,
            commits_loc_stats_enabled: true,
            rand_compute_at_this_date: true,
            recalc_reciprocal: 24,
            max_histograms: 0,
            fetch_commits_mode: 1,
            git_commits_batch: 1000,
            restore_orphan_commits: false,
            orphan_commits_range: "8 hours".to_string(),
            orphan_commits_all_branches: true,
            orphan_commits_group: true,
            ghapi_all_repos: true,
            ..Ctx::default()
        }
    }

    struct Case {
        name: &'static str,
        env: &'static [(&'static str, &'static str)],
        set: fn(&mut Ctx),
    }

    #[test]
    fn init_from_environment_table() {
        let _g = env_lock();
        // Like Go's `make test`, the table assumes a clean environment: scrub
        // any ambient `GHA2DB_*`/`PG_*` (e.g. the DB-test connection settings
        // exported by `test.sh`) for the duration of the test.
        let ambient: Vec<(String, String)> = std::env::vars()
            .filter(|(k, _)| k.starts_with("GHA2DB_") || k.starts_with("PG_"))
            .collect();
        for (k, _) in &ambient {
            set_or_unset(k, None);
        }
        struct Restore(Vec<(String, String)>);
        impl Drop for Restore {
            fn drop(&mut self) {
                for (k, v) in &self.0 {
                    set_or_unset(k, Some(v));
                }
            }
        }
        let _restore = Restore(ambient);
        let cases: Vec<Case> = vec![
        Case {
            name: "Default values",
            env: &[],
            set: |_c| {},
        },
        Case {
            name: "Setting debug level",
            env: &[("GHA2DB_DEBUG", "2")],
            set: |c| {
                c.debug = 2;
            },
        },
        Case {
            name: "Setting negative debug level",
            env: &[("GHA2DB_DEBUG", "-1")],
            set: |c| {
                c.debug = -1;
            },
        },
        Case {
            name: "Setting command debug level",
            env: &[("GHA2DB_CMDDEBUG", "3")],
            set: |c| {
                c.cmd_debug = 3;
            },
        },
        Case {
            name: "Setting GitHub debug level",
            env: &[("GHA2DB_GITHUB_DEBUG", "3")],
            set: |c| {
                c.github_debug = 3;
            },
        },
        Case {
            name: "Setting non-standard HTTP timeout/retry",
            env: &[("GHA2DB_HTTP_TIMEOUT", "5"), ("GHA2DB_HTTP_RETRY", "10")],
            set: |c| {
                c.http_timeout = 5;
                c.http_retry = 10;
            },
        },
        Case {
            name: "Setting project scale factor",
            env: &[("GHA2DB_PROJECT_SCALE", "3.14")],
            set: |c| {
                #[allow(clippy::approx_constant)]
                {
                    c.project_scale = 3.14;
                }
            },
        },
        Case {
            name: "Setting GitHub API Points 1",
            env: &[("GHA2DB_MIN_GHAPI_POINTS", "0")],
            set: |c| {
                c.min_ghapi_points = 0;
            },
        },
        Case {
            name: "Setting GitHub API Points 2",
            env: &[("GHA2DB_MIN_GHAPI_POINTS", "-1")],
            set: |c| {
                c.min_ghapi_points = 1;
            },
        },
        Case {
            name: "Setting GitHub API Points 3",
            env: &[("GHA2DB_MIN_GHAPI_POINTS", "1000")],
            set: |c| {
                c.min_ghapi_points = 1000;
            },
        },
        Case {
            name: "Setting GitHub API Wait 0",
            env: &[("GHA2DB_MAX_GHAPI_WAIT", "0")],
            set: |c| {
                c.max_ghapi_wait_seconds = 0;
            },
        },
        Case {
            name: "Setting GitHub API Wait -1",
            env: &[("GHA2DB_MAX_GHAPI_WAIT", "-1")],
            set: |c| {
                c.max_ghapi_wait_seconds = 10;
            },
        },
        Case {
            name: "Setting GitHub API Wait 1000",
            env: &[("GHA2DB_MAX_GHAPI_WAIT", "1000")],
            set: |c| {
                c.max_ghapi_wait_seconds = 1000;
            },
        },
        Case {
            name: "Setting GitHub API Retry 0",
            env: &[("GHA2DB_MAX_GHAPI_RETRY", "0")],
            set: |c| {
                c.max_ghapi_retry = 6;
            },
        },
        Case {
            name: "Setting GitHub API Retry 1",
            env: &[("GHA2DB_MAX_GHAPI_RETRY", "1")],
            set: |c| {
                c.max_ghapi_retry = 1;
            },
        },
        Case {
            name: "Setting GitHub API Retry 5",
            env: &[("GHA2DB_MAX_GHAPI_RETRY", "15")],
            set: |c| {
                c.max_ghapi_retry = 15;
            },
        },
        Case {
            name: "Setting GitHub API rate limits cache 0 (disabled)",
            env: &[("GHA2DB_GHAPI_RATE_LIMITS_CACHE", "0")],
            set: |c| {
                c.ghapi_rate_limits_cache = 0;
            },
        },
        Case {
            name: "Setting GitHub API rate limits cache 30",
            env: &[("GHA2DB_GHAPI_RATE_LIMITS_CACHE", "30")],
            set: |c| {
                c.ghapi_rate_limits_cache = 30;
            },
        },
        Case {
            name: "Setting GitHub API rate limits cache -1 (ignored)",
            env: &[("GHA2DB_GHAPI_RATE_LIMITS_CACHE", "-1")],
            set: |c| {
                c.ghapi_rate_limits_cache = 5;
            },
        },
        Case {
            name: "Setting dry run mode",
            env: &[("GHA2DB_DRY_RUN", "1")],
            set: |c| {
                c.dry_run = true;
            },
        },
        Case {
            name: "Setting JSON out and disabling DB out",
            env: &[("GHA2DB_JSON", "set"), ("GHA2DB_NODB", "1")],
            set: |c| {
                c.json_out = true;
                c.db_out = false;
            },
        },
        Case {
            name: "Setting ST (singlethreading) and NCPUs",
            env: &[("GHA2DB_ST", "1"), ("GHA2DB_NCPUS", "1")],
            set: |c| {
                c.st = true;
                c.ncpus = 1;
            },
        },
        Case {
            name: "Setting NCPUs to 2",
            env: &[("GHA2DB_NCPUS", "2")],
            set: |c| {
                c.st = false;
                c.ncpus = 2;
            },
        },
        Case {
            name: "Setting NCPUs to 1 should also set ST mode",
            env: &[("GHA2DB_NCPUS", "1")],
            set: |c| {
                c.st = true;
                c.ncpus = 1;
            },
        },
        Case {
            name: "Setting TmOffset",
            env: &[("GHA2DB_TMOFFSET", "5")],
            set: |c| {
                c.tm_offset = 5;
            },
        },
        Case {
            name: "Setting PID file",
            env: &[("GHA2DB_PID_FILE_ROOT", "kubernetes_devstats")],
            set: |c| {
                c.pid_file_root = "kubernetes_devstats".to_string();
            },
        },
        Case {
            name: "Setting Postgres parameters",
            env: &[("PG_HOST", "example.com"), ("PG_PORT", "1234"), ("PG_DB", "test"), ("PG_USER", "pgadm"), ("PG_PASS", "123!@#"), ("PG_SSL", "enable")],
            set: |c| {
                c.pg_host = "example.com".to_string();
                c.pg_port = "1234".to_string();
                c.pg_db = "test".to_string();
                c.pg_user = "pgadm".to_string();
                c.pg_pass = "123!@#".to_string();
                c.pg_ssl = "enable".to_string();
            },
        },
        Case {
            name: "Setting index, table, tools",
            env: &[("GHA2DB_INDEX", "1"), ("GHA2DB_SKIPTABLE", "yes"), ("GHA2DB_SKIPTOOLS", "Y")],
            set: |c| {
                c.index = true;
                c.table = false;
                c.tools = false;
            },
        },
        Case {
            name: "Setting data directory",
            env: &[("GHA2DB_DATADIR", "/path/to/dir")],
            set: |c| {
                c.data_dir = "/path/to/dir/".to_string();
            },
        },
        Case {
            name: "Setting skip log time",
            env: &[("GHA2DB_SKIPTIME", "Y")],
            set: |c| {
                c.log_time = false;
            },
        },
        Case {
            name: "Setting getchar default to string longer than 1 character",
            env: &[("GHA2DB_MGETC", "yes")],
            set: |c| {
                c.mgetc = "y".to_string();
            },
        },
        Case {
            name: "Setting query out & context out",
            env: &[("GHA2DB_QOUT", "1"), ("GHA2DB_CTXOUT", "1")],
            set: |c| {
                c.q_out = true;
                c.ctx_out = true;
            },
        },
        Case {
            name: "Setting skip TSDB, reset TSDB, reset quick ranges",
            env: &[("GHA2DB_SKIPTSDB", "1"), ("GHA2DB_RESETTSDB", "yes"), ("GHA2DB_RESETRANGES", "yeah")],
            set: |c| {
                c.skip_tsdb = true;
                c.reset_tsdb = true;
                c.reset_ranges = true;
            },
        },
        Case {
            name: "Setting skip PDB",
            env: &[("GHA2DB_SKIPPDB", "1")],
            set: |c| {
                c.skip_pdb = true;
            },
        },
        Case {
            name: "Setting affiliations DB",
            env: &[("GHA2DB_AFFILIATIONS_DB", "affiliations")],
            set: |c| {
                c.affiliations_db = "affiliations".to_string();
            },
        },
        Case {
            name: "Setting skip GHAPI and GetRepos",
            env: &[("GHA2DB_GETREPOSSKIP", "1"), ("GHA2DB_GHAPISKIP", "1"), ("GHA2DB_GHAPISKIPEVENTS", "1"), ("GHA2DB_GHAPISKIPISSUES", "1"), ("GHA2DB_GHAPISKIPPRS", "1"), ("GHA2DB_GHAPISKIPCOMMITS", "1"), ("GHA2DB_GHAPISKIPLICENSES", "1"), ("GHA2DB_GHAPIFORCELICENSES", "1"), ("GHA2DB_GHAPISKIPLANGS", "1"), ("GHA2DB_GHAPIFORCELANGS", "1"), ("GHA2DB_GHAPISKIPCOMMENTS", "1"), ("GHA2DB_GHAPISKIPREVIEWS", "1"), ("GHA2DB_GHAPISKIPFORKS", "1"), ("GHA2DB_GHAPISKIPRELEASES", "1"), ("GHA2DB_GHAPISKIPSTARS", "1"), ("GHA2DB_GHAPISKIPREPOSTATS", "1"), ("GHA2DB_GHAPISKIPREPOEVENTS", "1"), ("GHA2DB_GHAPI_ERROR_FATAL", "1"), ("GHA2DB_NO_AUTOFETCHCOMMITS", "1")],
            set: |c| {
                c.skip_get_repos = true;
                c.skip_ghapi = true;
                c.skip_api_events = true;
                c.skip_api_issues = true;
                c.skip_api_prs = true;
                c.skip_api_commits = true;
                c.skip_api_licenses = true;
                c.force_api_licenses = true;
                c.skip_api_langs = true;
                c.force_api_langs = true;
                c.skip_api_comments = true;
                c.skip_api_reviews = true;
                c.skip_api_forks = true;
                c.skip_api_releases = true;
                c.skip_api_stars = true;
                c.skip_api_repo_stats = true;
                c.skip_api_repo_events = true;
                c.ghapi_error_is_fatal = true;
                c.auto_fetch_commits = false;
            },
        },
        Case {
            name: "Setting skip tools",
            env: &[("GHA2DB_SKIP_TAGS", "1"), ("GHA2DB_SKIP_ANNOTATIONS", "1"), ("GHA2DB_SKIP_COLUMNS", "1"), ("GHA2DB_SKIP_VARS", "1")],
            set: |c| {
                c.skip_tags = true;
                c.skip_annotations = true;
                c.skip_columns = true;
                c.skip_vars = true;
            },
        },
        Case {
            name: "Setting skip tools",
            env: &[("GHA2DB_SKIP_RAND", "1")],
            set: |c| {
                c.skip_rand = true;
            },
        },
        Case {
            name: "Setting run columns",
            env: &[("GHA2DB_RUN_COLUMNS", "1")],
            set: |c| {
                c.run_columns = true;
            },
        },
        Case {
            name: "Allow broken JSON",
            env: &[("GHA2DB_ALLOW_BROKEN_JSON", "1")],
            set: |c| {
                c.allow_broken_json = true;
            },
        },
        Case {
            name: "Allow metric fail",
            env: &[("GHA2DB_ALLOW_METRIC_FAIL", "1")],
            set: |c| {
                c.allow_metric_fail = true;
            },
        },
        Case {
            name: "Run website_data just after sync",
            env: &[("GHA2DB_WEBSITEDATA", "y")],
            set: |c| {
                c.website_data = true;
            },
        },
        Case {
            name: "Drop and recreate artificial events mode",
            env: &[("GHA2DB_SKIP_UPDATE_EVENTS", "1")],
            set: |c| {
                c.skip_update_events = true;
            },
        },
        Case {
            name: "Setting explain query mode",
            env: &[("GHA2DB_EXPLAIN", "1")],
            set: |c| {
                c.explain = true;
            },
        },
        Case {
            name: "Setting last series",
            env: &[("GHA2DB_LASTSERIES", "reviewers_q")],
            set: |c| {
                c.last_series = "reviewers_q".to_string();
            },
        },
        Case {
            name: "Setting default start date to 2017",
            env: &[("GHA2DB_STARTDT", "2017")],
            set: |c| {
                c.default_start_date = ymd_hms(2017, 1, 1, 0, 0, 0);
            },
        },
        Case {
            name: "Setting default start date to 1982-07-16 10:15:45",
            env: &[("GHA2DB_STARTDT", "1982-07-16 10:15:45")],
            set: |c| {
                c.default_start_date = ymd_hms(1982, 7, 16, 10, 15, 45);
            },
        },
        Case {
            name: "Setting force start date",
            env: &[("GHA2DB_STARTDT_FORCE", "1")],
            set: |c| {
                c.force_start_date = true;
            },
        },
        Case {
            name: "Setting Old pre 2015 GHA JSONs format",
            env: &[("GHA2DB_OLDFMT", "1")],
            set: |c| {
                c.old_format = true;
            },
        },
        Case {
            name: "Setting exact repository names mode",
            env: &[("GHA2DB_EXACT", "1")],
            set: |c| {
                c.exact = true;
            },
        },
        Case {
            name: "Setting skip DB log mode mode",
            env: &[("GHA2DB_SKIPLOG", "1")],
            set: |c| {
                c.log_to_db = false;
            },
        },
        Case {
            name: "Setting local data mode",
            env: &[("GHA2DB_LOCAL", "yeah")],
            set: |c| {
                c.local = true;
            },
        },
        Case {
            name: "Setting local commands (binary and shell scripts) mode",
            env: &[("GHA2DB_LOCAL_CMD", "yeah")],
            set: |c| {
                c.local_cmd = true;
            },
        },
        Case {
            name: "Setting non standard YAML files",
            env: &[("GHA2DB_METRICS_YAML", "met.YAML"), ("GHA2DB_TAGS_YAML", "/t/g/s.yml"), ("GHA2DB_COLUMNS_YAML", "/t/cols.yml"), ("GHA2DB_VARS_YAML", "/vars.yml")],
            set: |c| {
                c.metrics_yaml = "met.YAML".to_string();
                c.tags_yaml = "/t/g/s.yml".to_string();
                c.columns_yaml = "/t/cols.yml".to_string();
                c.vars_yaml = "/vars.yml".to_string();
            },
        },
        Case {
            name: "Setting GitHub OAUth key",
            env: &[("GHA2DB_GITHUB_OAUTH", "1234567890123456789012345678901234567890")],
            set: |c| {
                c.github_oauth = "1234567890123456789012345678901234567890".to_string();
            },
        },
        Case {
            name: "Setting GitHub OAUth file",
            env: &[("GHA2DB_GITHUB_OAUTH", "/home/keogh/gh.key")],
            set: |c| {
                c.github_oauth = "/home/keogh/gh.key".to_string();
            },
        },
        Case {
            name: "Setting GitHub API URL",
            env: &[("GHA2DB_GITHUB_API_URL", "http://127.0.0.1:8080/api/v3/")],
            set: |c| {
                c.github_api_url = "http://127.0.0.1:8080/api/v3/".to_string();
            },
        },
        Case {
            name: "Setting GitHub API URL without trailing slash",
            env: &[("GHA2DB_GITHUB_API_URL", "http://127.0.0.1:8080")],
            set: |c| {
                c.github_api_url = "http://127.0.0.1:8080/".to_string();
            },
        },
        Case {
            name: "Setting GH Archive URL",
            env: &[("GHA2DB_GHARCHIVE_URL", "http://127.0.0.1:8081/archive/")],
            set: |c| {
                c.gharchive_url = "http://127.0.0.1:8081/archive/".to_string();
            },
        },
        Case {
            name: "Setting GH Archive URL without trailing slash",
            env: &[("GHA2DB_GHARCHIVE_URL", "http://127.0.0.1:8081")],
            set: |c| {
                c.gharchive_url = "http://127.0.0.1:8081/".to_string();
            },
        },
        Case {
            name: "Setting clear DB logs period",
            env: &[("GHA2DB_MAXLOGAGE", "3 days"), ("GHA2DB_MAX_AFFS_LOCK_AGE", "2 days"), ("GHA2DB_MAX_GIANT_LOCK_AGE", "4 days")],
            set: |c| {
                c.clear_db_period = "3 days".to_string();
                c.clear_affs_lock_period = "2 days".to_string();
                c.clear_giant_lock_period = "4 days".to_string();
            },
        },
        Case {
            name: "Setting webhook data",
            env: &[("GHA2DB_WHROOT", "/root"), ("GHA2DB_WHPORT", ":1666"), ("GHA2DB_WHHOST", "0.0.0.0")],
            set: |c| {
                c.web_hook_root = "/root".to_string();
                c.web_hook_port = ":1666".to_string();
                c.web_hook_host = "0.0.0.0".to_string();
            },
        },
        Case {
            name: "Setting webhook data missing ':'",
            env: &[("GHA2DB_WHPORT", "1986")],
            set: |c| {
                c.web_hook_port = ":1986".to_string();
            },
        },
        Case {
            name: "Setting API server data",
            env: &[("GHA2DB_API_PORT", ":8090"), ("GHA2DB_API_HOST", "127.0.0.1")],
            set: |c| {
                c.api_port = ":8090".to_string();
                c.api_host = "127.0.0.1".to_string();
            },
        },
        Case {
            name: "Setting API server data missing ':'",
            env: &[("GHA2DB_API_PORT", "8091")],
            set: |c| {
                c.api_port = ":8091".to_string();
            },
        },
        Case {
            name: "Setting skip check webhook payload",
            env: &[("GHA2DB_SKIP_VERIFY_PAYLOAD", "1")],
            set: |c| {
                c.check_payload = false;
            },
        },
        Case {
            name: "Setting skip full deploy",
            env: &[("GHA2DB_SKIP_FULL_DEPLOY", "1")],
            set: |c| {
                c.full_deploy = false;
            },
        },
        Case {
            name: "Setting trials",
            env: &[("GHA2DB_TRIALS", "1,2,3,4")],
            set: |c| {
                c.trials = vec![1, 2, 3, 4];
            },
        },
        Case {
            name: "Setting webhook params",
            env: &[("GHA2DB_DEPLOY_BRANCHES", "master,staging,production"), ("GHA2DB_DEPLOY_STATUSES", "ok,passed,fixed"), ("GHA2DB_DEPLOY_RESULTS", "-1,0,1"), ("GHA2DB_DEPLOY_TYPES", "push,pull_request"), ("GHA2DB_PROJECT_ROOT", "/home/lukaszgryglicki/dev/go/src/gha2db")],
            set: |c| {
                c.deploy_branches = svec(&["master", "staging", "production"]);
                c.deploy_statuses = svec(&["ok", "passed", "fixed"]);
                c.deploy_results = vec![-1, 0, 1];
                c.deploy_types = svec(&["push", "pull_request"]);
                c.project_root = "/home/lukaszgryglicki/dev/go/src/gha2db".to_string();
            },
        },
        Case {
            name: "Setting project",
            env: &[("GHA2DB_PROJECT", "prometheus")],
            set: |c| {
                c.project = "prometheus".to_string();
                c.metrics_yaml = "metrics/prometheus/metrics.yaml".to_string();
                c.tags_yaml = "metrics/prometheus/tags.yaml".to_string();
                c.columns_yaml = "metrics/prometheus/columns.yaml".to_string();
                c.vars_yaml = "metrics/prometheus/vars.yaml".to_string();
            },
        },
        Case {
            name: "Setting project and non standard yaml",
            env: &[("GHA2DB_PROJECT", "prometheus")],
            set: |c| {
                c.project = "prometheus".to_string();
                c.metrics_yaml = "metrics/prometheus/metrics.yaml".to_string();
                c.tags_yaml = "metrics/prometheus/tags.yaml".to_string();
                c.columns_yaml = "metrics/prometheus/columns.yaml".to_string();
                c.vars_yaml = "metrics/prometheus/vars.yaml".to_string();
            },
        },
        Case {
            name: "Setting project and non standard vars yaml",
            env: &[("GHA2DB_PROJECT", "cncf"), ("GHA2DB_VARS_FN_YAML", "sync_vars.yaml")],
            set: |c| {
                c.project = "cncf".to_string();
                c.metrics_yaml = "metrics/cncf/metrics.yaml".to_string();
                c.tags_yaml = "metrics/cncf/tags.yaml".to_string();
                c.columns_yaml = "metrics/cncf/columns.yaml".to_string();
                c.vars_yaml = "metrics/cncf/sync_vars.yaml".to_string();
                c.vars_fn_yaml = "sync_vars.yaml".to_string();
            },
        },
        Case {
            name: "Setting tests.yaml",
            env: &[("GHA2DB_TESTS_YAML", "foobar.yml")],
            set: |c| {
                c.tests_yaml = "foobar.yml".to_string();
            },
        },
        Case {
            name: "Setting skip_dates.yaml",
            env: &[("GHA2DB_SKIP_DATES_YAML", "bzz.yml")],
            set: |c| {
                c.skip_dates_yaml = "bzz.yml".to_string();
            },
        },
        Case {
            name: "Setting projects.yaml && github_users.json",
            env: &[("GHA2DB_PROJECTS_YAML", "baz.yml"), ("GHA2DB_AFFILIATIONS_JSON", "other.json"), ("GHA2DB_COMPANY_ACQ_YAML", "acq.yml")],
            set: |c| {
                c.projects_yaml = "baz.yml".to_string();
                c.affiliations_json = "other.json".to_string();
                c.company_acq_yaml = "acq.yml".to_string();
            },
        },
        Case {
            name: "Setting repos dir without ending '/'",
            env: &[("GHA2DB_REPOS_DIR", "/abc")],
            set: |c| {
                c.repos_dir = "/abc/".to_string();
            },
        },
        Case {
            name: "Setting repos dir with ending '/'",
            env: &[("GHA2DB_REPOS_DIR", "~/temp/")],
            set: |c| {
                c.repos_dir = "~/temp/".to_string();
            },
        },
        Case {
            name: "Setting JSONs dir without ending '/'",
            env: &[("GHA2DB_JSONS_DIR", "/abc")],
            set: |c| {
                c.jsons_dir = "/abc/".to_string();
            },
        },
        Case {
            name: "Setting JSONs dir with ending '/'",
            env: &[("GHA2DB_JSONS_DIR", "/def/ghi/")],
            set: |c| {
                c.jsons_dir = "/def/ghi/".to_string();
            },
        },
        Case {
            name: "Setting recent range",
            env: &[("GHA2DB_RECENT_RANGE", "6 hours"), ("GHA2DB_RECENT_REPOS_RANGE", "1 week")],
            set: |c| {
                c.recent_range = "6 hours".to_string();
                c.recent_repos_range = "1 week".to_string();
            },
        },
        Case {
            name: "Setting CSV output",
            env: &[("GHA2DB_CSVOUT", "report.csv")],
            set: |c| {
                c.csv_file = "report.csv".to_string();
            },
        },
        Case {
            name: "Set process repos & commits",
            env: &[("GHA2DB_PROCESS_REPOS", "1"), ("GHA2DB_PROCESS_COMMITS", "1")],
            set: |c| {
                c.process_repos = true;
                c.process_commits = true;
            },
        },
        Case {
            name: "Set get_repos external info for cncf/gitdm",
            env: &[("GHA2DB_EXTERNAL_INFO", "1")],
            set: |c| {
                c.external_info = true;
            },
        },
        Case {
            name: "Enable metrics drop",
            env: &[("GHA2DB_ENABLE_METRICS_DROP", "1")],
            set: |c| {
                c.enable_metrics_drop = true;
            },
        },
        Case {
            name: "Enable metrics drop",
            env: &[("GHA2DB_REFRESH_COMMIT_ROLES", "1")],
            set: |c| {
                c.refresh_commit_roles = true;
            },
        },
        Case {
            name: "Set compute all periods mode",
            env: &[("GHA2DB_COMPUTE_ALL", "1")],
            set: |c| {
                c.compute_all = true;
            },
        },
        Case {
            name: "Set disable commits stats mode",
            env: &[("GHA2DB_SKIP_COMMITS_FILES", "1"), ("GHA2DB_SKIP_COMMITS_LOC", "1")],
            set: |c| {
                c.commits_files_stats_enabled = false;
                c.commits_loc_stats_enabled = false;
            },
        },
        Case {
            name: "Set skip shared DB mode",
            env: &[("GHA2DB_SKIP_SHAREDDB", "1")],
            set: |c| {
                c.skip_shared_db = true;
            },
        },
        Case {
            name: "Set skip PID file mode",
            env: &[("GHA2DB_SKIP_PIDFILE", "1")],
            set: |c| {
                c.skip_pid_file = true;
            },
        },
        Case {
            name: "Set skip company acquisitions file mode",
            env: &[("GHA2DB_SKIP_COMPANY_ACQ", "1")],
            set: |c| {
                c.skip_company_acq = true;
            },
        },
        Case {
            name: "Set check provision flag",
            env: &[("GHA2DB_CHECK_PROVISION_FLAG", "1"), ("GHA2DB_CHECK_RUNNING_FLAG", "yes")],
            set: |c| {
                c.check_provision_flag = true;
                c.check_running_flag = true;
            },
        },
        Case {
            name: "Check imported SHA",
            env: &[("GHA2DB_CHECK_IMPORTED_SHA", "1"), ("GHA2DB_ONLY_CHECK_IMPORTED_SHA", "1")],
            set: |c| {
                c.check_imported_sha = true;
                c.only_check_imported_sha = true;
            },
        },
        Case {
            name: "Set devstats running flag",
            env: &[("GHA2DB_SET_RUNNING_FLAG", "1")],
            set: |c| {
                c.set_running_flag = true;
            },
        },
        Case {
            name: "Set max running flag age",
            env: &[("GHA2DB_MAX_RUNNING_FLAG_AGE", "1h45m")],
            set: |c| {
                c.max_running_flag_age = Duration::from_secs(105 * 60);
            },
        },
        Case {
            name: "Set compute periods mode",
            env: &[("GHA2DB_FORCE_PERIODS", "w:f")],
            set: |c| {
                c.compute_periods = Some(periods(&[("w", &[false])]));
            },
        },
        Case {
            name: "Set compute periods mode 2",
            env: &[("GHA2DB_FORCE_PERIODS", "w:t,w:f")],
            set: |c| {
                c.compute_periods = Some(periods(&[("w", &[false, true])]));
            },
        },
        Case {
            name: "Set compute periods mode 3",
            env: &[("GHA2DB_FORCE_PERIODS", "m:t,m:f,q2:t,y10:f,y3:t")],
            set: |c| {
                c.compute_periods = Some(periods(&[("m", &[false, true]), ("q2", &[true]), ("y10", &[false]), ("y3", &[true])]));
            },
        },
        Case {
            name: "Set max run durations and exit statuses after timeout",
            env: &[("GHA2DB_MAX_RUN_DURATION", "tags:1h:0,calc_metric:12h:1,devstats:48h:-1")],
            set: |c| {
                c.max_run_duration = durs(&[("calc_metric", [43200, 1]), ("devstats", [172800, -1]), ("tags", [3600, 0])]);
            },
        },
        Case {
            name: "Set actors filter",
            env: &[
                ("GHA2DB_ACTORS_FILTER", "1"),
                ("GHA2DB_ACTORS_ALLOW", r"lukasz\s+gryglicki"),
                ("GHA2DB_ACTORS_FORBID", "linus"),
            ],
            set: |c| {
                c.actors_filter = true;
                c.actors_allow = Some(GoRegex::new(r"lukasz\s+gryglicki").unwrap());
                c.actors_forbid = Some(GoRegex::new(r"linus").unwrap());
            },
        },
        Case {
            name: "Incorrectly set actors filter",
            env: &[
                ("GHA2DB_ACTORS_FILTER", ""),
                ("GHA2DB_ACTORS_ALLOW", r"lukasz\s+gryglicki"),
                ("GHA2DB_ACTORS_FORBID", "linus"),
            ],
            set: |c| {
                c.actors_filter = false;
                c.actors_allow = None;
                c.actors_forbid = None;
            },
        },
        Case {
            name: "Set actors filter allow",
            env: &[("GHA2DB_ACTORS_FILTER", "1"), ("GHA2DB_ACTORS_ALLOW", r"lukasz\s+gryglicki")],
            set: |c| {
                c.actors_filter = true;
                c.actors_allow = Some(GoRegex::new(r"lukasz\s+gryglicki").unwrap());
                c.actors_forbid = None;
            },
        },
        Case {
            name: "Set actors filter forbid",
            env: &[("GHA2DB_ACTORS_FILTER", "yes"), ("GHA2DB_ACTORS_FORBID", r"lukasz\s+gryglicki")],
            set: |c| {
                c.actors_filter = true;
                c.actors_allow = None;
                c.actors_forbid = Some(GoRegex::new(r"lukasz\s+gryglicki").unwrap());
            },
        },
        Case {
            name: "Setting projects commits",
            env: &[("GHA2DB_PROJECTS_COMMITS", "a,b,c")],
            set: |c| {
                c.projects_commits = "a,b,c".to_string();
            },
        },
        Case {
            name: "Setting projects override",
            env: &[("GHA2DB_PROJECTS_OVERRIDE", "a,,c,-,+,,")],
            set: |c| {
                c.projects_override = bmap(&[]);
            },
        },
        Case {
            name: "Setting projects override",
            env: &[("GHA2DB_PROJECTS_OVERRIDE", "nothing")],
            set: |c| {
                c.projects_override = bmap(&[]);
            },
        },
        Case {
            name: "Setting projects override",
            env: &[("GHA2DB_PROJECTS_OVERRIDE", "+pro1")],
            set: |c| {
                c.projects_override = bmap(&[("pro1", true)]);
            },
        },
        Case {
            name: "Setting projects override",
            env: &[("GHA2DB_PROJECTS_OVERRIDE", ",+pro1,-pro2,,pro3,,+-pro4,-+pro5,")],
            set: |c| {
                c.projects_override = bmap(&[("pro1", true), ("pro2", false), ("-pro4", true), ("+pro5", false)]);
            },
        },
        Case {
            name: "Setting exclude repos",
            env: &[("GHA2DB_EXCLUDE_REPOS", "repo1,org1/repo2,,abc")],
            set: |c| {
                c.exclude_repos = bmap(&[("repo1", true), ("org1/repo2", true), ("abc", true)]);
            },
        },
        Case {
            name: "Setting exclude variables",
            env: &[("GHA2DB_EXCLUDE_VARS", "hostname,projects_health_partial_html,,")],
            set: |c| {
                c.exclude_vars = bmap(&[("hostname", true), ("projects_health_partial_html", true)]);
            },
        },
        Case {
            name: "Setting only variables",
            env: &[("GHA2DB_ONLY_VARS", "hostname,projects_health_partial_html,,")],
            set: |c| {
                c.only_vars = bmap(&[("hostname", true), ("projects_health_partial_html", true)]);
            },
        },
        Case {
            name: "Setting propagate variables from ONLY, case without ONLY set",
            env: &[("GHA2DB_PROPAGATE_ONLY_VAR", "1")],
            set: |c| {
                c.propagate_only_var = true;
            },
        },
        Case {
            name: "Setting propagate variables from ONLY, case with ONLY set to a",
            env: &[("GHA2DB_PROPAGATE_ONLY_VAR", "1"), ("ONLY", "a")],
            set: |c| {
                c.propagate_only_var = true;
                c.projects_commits = "a".to_string();
            },
        },
        Case {
            name: "Setting propagate variables from ONLY, case with ONLY set to 'a b c'",
            env: &[("GHA2DB_PROPAGATE_ONLY_VAR", "1"), ("ONLY", "a b c")],
            set: |c| {
                c.propagate_only_var = true;
                c.projects_commits = "a,b,c".to_string();
            },
        },
        Case {
            name: "Setting propagate variables from ONLY, case with ONLY set to 'a b c' but with ProjectCommits also set",
            env: &[("GHA2DB_PROPAGATE_ONLY_VAR", "1"), ("GHA2DB_PROJECTS_COMMITS", "d,e,f"), ("ONLY", "a b c")],
            set: |c| {
                c.propagate_only_var = true;
                c.projects_commits = "d,e,f".to_string();
            },
        },
        Case {
            name: "Setting only metrics mode",
            env: &[("GHA2DB_ONLY_METRICS", "metric1,metric2,,metric3")],
            set: |c| {
                c.only_metrics = bmap(&[("metric1", true), ("metric2", true), ("metric3", true)]);
            },
        },
        Case {
            name: "Setting skip metrics mode",
            env: &[("GHA2DB_SKIP_METRICS", "metric1,metric2,,metric3")],
            set: |c| {
                c.skip_metrics = bmap(&[("metric1", true), ("metric2", true), ("metric3", true)]);
            },
        },
        Case {
            name: "Setting input & output DBs for 'merge_dbs' tool",
            env: &[("GHA2DB_INPUT_DBS", "db1,db2,db3"), ("GHA2DB_OUTPUT_DB", "db4")],
            set: |c| {
                c.input_dbs = svec(&["db1", "db2", "db3"]);
                c.output_db = "db4".to_string();
            },
        },
        Case {
            name: "Setting recalc reciprocal to 1",
            env: &[("GHA2DB_RECALC_RECIPROCAL", "1")],
            set: |c| {
                c.recalc_reciprocal = 1;
            },
        },
        Case {
            name: "Setting recalc reciprocal to 100",
            env: &[("GHA2DB_RECALC_RECIPROCAL", "100")],
            set: |c| {
                c.recalc_reciprocal = 100;
            },
        },
        Case {
            name: "Setting recalc reciprocal to 0",
            env: &[("GHA2DB_RECALC_RECIPROCAL", "0")],
            set: |c| {
                c.recalc_reciprocal = 24;
            },
        },
        Case {
            name: "Setting recalc reciprocal to -2",
            env: &[("GHA2DB_RECALC_RECIPROCAL", "-2")],
            set: |c| {
                c.recalc_reciprocal = 24;
            },
        },
        Case {
            name: "Setting max histograms to 16",
            env: &[("GHA2DB_MAX_HIST", "16")],
            set: |c| {
                c.max_histograms = 16;
            },
        },
        Case {
            name: "Setting fetch commits mode to 0",
            env: &[("GHA2DB_FETCH_COMMITS_MODE", "0")],
            set: |c| {
                c.fetch_commits_mode = 0;
            },
        },
        Case {
            name: "Setting git commits batch size",
            env: &[("GHA2DB_GIT_COMMITS_BATCH", "500")],
            set: |c| {
                c.git_commits_batch = 500;
            },
        },
        Case {
            name: "Setting restore orphan commits",
            env: &[("GHA2DB_RESTORE_ORPHAN_COMMITS", "1"), ("GHA2DB_ORPHAN_COMMITS_RANGE", "9 months")],
            set: |c| {
                c.restore_orphan_commits = true;
                c.orphan_commits_range = "9 months".to_string();
            },
        },
        Case {
            name: "Setting legacy ghapi2db recent repos scope",
            env: &[("GHA2DB_GHAPI_RECENT_REPOS_ONLY", "1")],
            set: |c| {
                c.ghapi_all_repos = false;
            },
        },
        Case {
            name: "Setting legacy orphan commits restore shape",
            env: &[
                ("GHA2DB_ORPHAN_COMMITS_DEFAULT_BRANCH_ONLY", "1"),
                ("GHA2DB_ORPHAN_COMMITS_NO_GROUPING", "yes"),
            ],
            set: |c| {
                c.orphan_commits_all_branches = false;
                c.orphan_commits_group = false;
            },
        },
        ];
        assert_eq!(cases.len(), 127);
        let default = default_context();
        for (index, case) in cases.iter().enumerate() {
            let mut expected = default.copy_context();
            expected.rand_compute_at_this_date = true;
            (case.set)(&mut expected);
            let mut env: Vec<(&str, &str)> = case.env.to_vec();
            // GitHubOAuth depends on /etc/github/oauth* files, force a known value
            if !env.iter().any(|(k, _)| *k == "GHA2DB_GITHUB_OAUTH") {
                env.push(("GHA2DB_GITHUB_OAUTH", "not_use"));
            }
            let saved: Vec<(&str, Option<String>)> = env
                .iter()
                .map(|(k, _)| (*k, std::env::var(k).ok()))
                .collect();
            for (k, v) in &env {
                set_or_unset(k, Some(v));
            }
            let mut got = Ctx::default();
            got.init();
            got.test_mode = true;
            for (k, v) in &saved {
                set_or_unset(k, v.as_deref());
            }
            assert_eq!(
                got.go_string(),
                expected.go_string(),
                "Test case number {} \"{}\"",
                index + 1,
                case.name
            );
            assert_eq!(
                got,
                expected,
                "Test case number {} \"{}\" (struct)",
                index + 1,
                case.name
            );
        }
    }

    #[test]
    fn copy_context_omits_runtime_only_fields() {
        let mut ctx = default_context();
        ctx.default_hostname = "devstats.cncf.io".to_string();
        ctx.shared_db = "shared".to_string();
        ctx.project_main_repo = "k/k".to_string();
        ctx.rand_compute_at_this_date = true;
        ctx.refresh_commit_roles = true;
        ctx.allow_rand_tags_cols_compute = true;
        let copy = ctx.copy_context();
        assert_eq!(copy.default_hostname, "");
        assert_eq!(copy.shared_db, "");
        assert_eq!(copy.project_main_repo, "");
        assert!(!copy.rand_compute_at_this_date);
        assert!(!copy.refresh_commit_roles);
        assert!(!copy.allow_rand_tags_cols_compute);
        assert_eq!(copy.data_dir, ctx.data_dir);
        assert_eq!(copy.trials, ctx.trials);
        assert_eq!(copy.max_running_flag_age, ctx.max_running_flag_age);
    }

    #[test]
    fn go_string_shape() {
        let ctx = default_context();
        let s = ctx.go_string();
        assert!(
            s.starts_with("&{DataDir:/etc/gha2db/ Debug:0 CmdDebug:0 GitHubDebug:0 DryRun:false"),
            "{}",
            s
        );
        assert!(
            s.contains(" DefaultStartDate:2012-07-01 00:00:00 +0000 UTC "),
            "{}",
            s
        );
        assert!(
            s.contains(" Trials:[10 30 60 120 300 600 1200 3600] "),
            "{}",
            s
        );
        assert!(s.contains(" ProjectsOverride:map[] "), "{}", s);
        assert!(
            s.contains(" ActorsAllow:<nil> ActorsForbid:<nil> "),
            "{}",
            s
        );
        assert!(s.contains(" ComputePeriods:map[] "), "{}", s);
        assert!(s.contains(" MaxRunningFlagAge:9h0m0s "), "{}", s);
        assert!(s.contains(" ProjectScale:1 "), "{}", s);
        assert!(s.contains(" MaxRunDuration:map[] "), "{}", s);
        assert!(
            s.contains(
                " SkipAPIStars:false SkipAPIRepoStats:false SkipAPIRepoEvents:false GHAPIAllRepos:true SkipGetRepos:false "
            ),
            "{}",
            s
        );
        assert!(
            s.ends_with(
                " OrphanCommitsRange:8 hours OrphanCommitsAllBranches:true OrphanCommitsGroup:true}"
            ),
            "{}",
            s
        );
        let mut ctx2 = ctx.clone();
        ctx2.compute_periods = Some(periods(&[("m", &[false, true]), ("q2", &[true])]));
        ctx2.max_run_duration = durs(&[("tags", [3600, 0]), ("calc_metric", [43200, 1])]);
        ctx2.actors_allow = Some(GoRegex::new(r"lukasz\s+gryglicki").unwrap());
        ctx2.project_scale = 3.25;
        let s2 = ctx2.go_string();
        assert!(
            s2.contains(" ComputePeriods:map[m:map[false:{} true:{}] q2:map[true:{}]] "),
            "{}",
            s2
        );
        assert!(
            s2.contains(" MaxRunDuration:map[calc_metric:[43200 1] tags:[3600 0]] "),
            "{}",
            s2
        );
        assert!(s2.contains(r" ActorsAllow:lukasz\s+gryglicki "), "{}", s2);
        assert!(s2.contains(" ProjectScale:3.25 "), "{}", s2);
    }

    #[test]
    fn go_regex_semantics() {
        let re = GoRegex::new(r"lukasz\s+gryglicki").unwrap();
        assert!(re.is_match("lukasz   gryglicki"));
        assert!(!re.is_match("lukaszgryglicki"));
        assert_eq!(re.to_string(), r"lukasz\s+gryglicki");
        assert_eq!(re, GoRegex::new(r"lukasz\s+gryglicki").unwrap());
        assert!(GoRegex::new("(").is_err());
    }
}
