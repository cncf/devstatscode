use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// Environment context packed in structure
/// Translated from Go's Ctx struct in context.go
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {
    // From GHA2DB_DATADIR, default /etc/gha2db/
    pub data_dir: String,
    
    // From GHA2DB_DEBUG Debug level: 0-no, 1-info, 2-verbose, including SQLs, default 0
    pub debug: i32,
    
    // From GHA2DB_CMDDEBUG Commands execution Debug level: 0-no, 1-only output commands, 2-output commands and their output, 3-output full environment as well, default 0
    pub cmd_debug: i32,
    
    // From GHA2DB_GITHUB_DEBUG debug GitHub rate limits
    pub github_debug: i32,
    
    // From GHA2DB_DRY_RUN, import_affs tool - stop before doing any updates
    pub dry_run: bool,
    
    // From GHA2DB_JSON gha2db: write JSON files? default false
    pub json_out: bool,
    
    // From GHA2DB_NODB gha2db: write to SQL database, default true
    pub db_out: bool,
    
    // From GHA2DB_ST true: use single threaded version, false: use multi threaded version, default false
    pub st: bool,
    
    // From GHA2DB_NCPUS, set to override number of CPUs to run, this overwrites GHA2DB_ST, default 0 (which means do not use it)
    pub n_cpus: i32,
    
    // PostgreSQL connection parameters
    pub pg_host: String,     // From PG_HOST, default "localhost"
    pub pg_port: String,     // From PG_PORT, default "5432"
    pub pg_db: String,       // From PG_DB, default "gha"
    pub pg_user: String,     // From PG_USER, default "gha_admin"
    pub pg_pass: String,     // From PG_PASS, default "password"
    pub pg_ssl: String,      // From PG_SSL, default "disable"
    
    // From GHA2DB_INDEX Create DB index? default false
    pub index: bool,
    
    // From GHA2DB_SKIPTABLE Create table structure? default true
    pub table: bool,
    
    // From GHA2DB_SKIPTOOLS Create DB tools (like views, summary tables, materialized views etc)? default true
    pub tools: bool,
    
    // From GHA2DB_MGETC Character returned by mgetc (if non empty), default ""
    pub mgetc: String,
    
    // From GHA2DB_QOUT output all SQL queries?, default false
    pub q_out: bool,
    
    // From GHA2DB_CTXOUT output all context data (this struct), default false
    pub ctx_out: bool,
    
    // From GHA2DB_SKIPTIME, output time with all lib.Printf(...) calls, default true, use GHA2DB_SKIPTIME to disable
    pub log_time: bool,
    
    // From GHA2DB_STARTDT, default `2012-07-01 00:00 UTC`, expects format "YYYY-MM-DD HH:MI:SS", can be set in `projects.yaml` via `start_date:`
    pub default_start_date: DateTime<Utc>,
    
    // From GHA2DB_STARTDT_FORCE, default false
    pub force_start_date: bool,
    
    // From GHA2DB_LASTSERIES, use this TSDB series to determine last timestamp date, default "events_h"
    pub last_series: String,
    
    // From GHA2DB_SKIPTSDB gha2db_sync tool, skip TS DB processing? for calc_metric it skips final series write, default false
    pub skip_tsdb: bool,
    
    // From GHA2DB_SKIPPDB gha2db_sync tool, skip Postgres DB processing (gha2db part) default false
    pub skip_pdb: bool,
    
    // From GHA2DB_RESETTSDB sync tool, regenerate all TS points? default false
    pub reset_tsdb: bool,
    
    // From GHA2DB_RESETRANGES sync tool, regenerate all past quick ranges? default false
    pub reset_ranges: bool,
    
    // From GHA2DB_EXPLAIN runq tool, prefix query with "explain " - it will display query plan instead of executing real query, default false
    pub explain: bool,
    
    // From GHA2DB_OLDFMT gha2db tool, if set then use pre 2015 GHA JSONs format
    pub old_format: bool,
    
    // From GHA2DB_EXACT gha2db tool, if set then orgs list provided from commandline is used as a list of exact repository full names
    pub exact: bool,
    
    // From GHA2DB_SKIPLOG all tools, if set, DB logging into Postgres table `gha_logs` in `devstats` database will be disabled
    pub log_to_db: bool,
    
    // From GHA2DB_LOCAL many tools, if set it will use data files prefixed with "./" to use local ones
    pub local: bool,
    
    // From GHA2DB_ABSOLUTE runq tool, if set it will use data files without any prefix
    pub absolute: bool,
    
    // From GHA2DB_LOCAL_CMD many tools, if set it will call other tools prefixed with "./"
    pub local_cmd: bool,
    
    // YAML configuration file paths
    pub metrics_yaml: String,      // From GHA2DB_METRICS_YAML
    pub tags_yaml: String,         // From GHA2DB_TAGS_YAML
    pub columns_yaml: String,      // From GHA2DB_COLUMNS_YAML
    pub vars_yaml: String,         // From GHA2DB_VARS_YAML
    pub vars_fn_yaml: String,      // From GHA2DB_VARS_FN_YAML
    pub skip_dates_yaml: String,   // From GHA2DB_SKIP_DATES_YAML
    
    // From GHA2DB_GITHUB_OAUTH ghapi2db tool
    pub github_oauth: String,
    
    // Database cleanup periods
    pub clear_db_period: String,        // From GHA2DB_MAXLOGAGE, default "1 week"
    pub clear_affs_lock_period: String, // From GHA2DB_MAX_AFFS_LOCK_AGE, default "16 hours"
    pub clear_giant_lock_period: String,// From GHA2DB_MAX_GIANT_LOCK_AGE, default "40 hours"
    
    // From GHA2DB_TRIALS, retry periods for some retryable errors
    pub trials: Vec<i32>,
    
    // Webhook configuration
    pub webhook_root: String,    // From GHA2DB_WHROOT, default "/hook"
    pub webhook_port: String,    // From GHA2DB_WHPORT, default ":1982"
    pub webhook_host: String,    // From GHA2DB_WHHOST, default "127.0.0.1"
    pub check_payload: bool,     // From GHA2DB_SKIP_VERIFY_PAYLOAD, default true
    pub full_deploy: bool,       // From GHA2DB_SKIP_FULL_DEPLOY, default true
    pub deploy_branches: Vec<String>,  // From GHA2DB_DEPLOY_BRANCHES, default "master"
    pub deploy_statuses: Vec<String>,  // From GHA2DB_DEPLOY_STATUSES, default "Passed,Fixed"
    pub deploy_results: Vec<i32>,      // From GHA2DB_DEPLOY_RESULTS, default "0"
    pub deploy_types: Vec<String>,     // From GHA2DB_DEPLOY_TYPES, default "push"
    pub project_root: String,          // From GHA2DB_PROJECT_ROOT
    
    // Execution configuration
    pub exec_fatal: bool,   // default true, set to false to avoid os.Exit() on failure
    pub exec_quiet: bool,   // default false, set to true for quiet exec failures
    pub exec_output: bool,  // default false, set to true to capture commands STDOUT
    
    // From GHA2DB_PROJECT, default ""
    pub project: String,
    
    // From GHA2DB_TESTS_YAML, default "tests.yaml"
    pub tests_yaml: String,
    
    // Repository processing configuration
    pub repos_dir: String,         // From GHA2DB_REPOS_DIR, default "~/devstats_repos/"
    pub process_repos: bool,       // From GHA2DB_PROCESS_REPOS, default false
    pub process_commits: bool,     // From GHA2DB_PROCESS_COMMITS, default false
    pub external_info: bool,       // From GHA2DB_EXTERNAL_INFO, default false
    pub projects_commits: String,  // From GHA2DB_PROJECTS_COMMITS, default ""
    pub propagate_only_var: bool,  // From GHA2DB_PROPAGATE_ONLY_VAR, default false
    
    // Configuration files
    pub projects_yaml: String,     // From GHA2DB_PROJECTS_YAML, default "projects.yaml"
    pub company_acq_yaml: String,  // From GHA2DB_COMPANY_ACQ_YAML
    pub affiliations_json: String, // From GHA2DB_AFFILIATIONS_JSON, default "github_users.json"
    
    // Project and repository filters
    pub projects_override: HashMap<String, bool>, // From GHA2DB_PROJECTS_OVERRIDE
    pub exclude_repos: HashMap<String, bool>,     // From GHA2DB_EXCLUDE_REPOS
    
    // Database merge configuration
    pub input_dbs: Vec<String>, // From GHA2DB_INPUT_DBS
    pub output_db: String,      // From GHA2DB_OUTPUT_DB
    
    // Time configuration
    pub tm_offset: i32,         // From GHA2DB_TMOFFSET, default 0
    pub default_hostname: String, // "devstats.cncf.io"
    
    // GitHub API configuration
    pub recent_range: String,         // From GHA2DB_RECENT_RANGE, default '12 hours'
    pub recent_repos_range: String,   // From GHA2DB_RECENT_REPOS_RANGE, default '1 day'
    pub min_ghapi_points: i32,        // From GHA2DB_MIN_GHAPI_POINTS
    pub max_ghapi_wait_seconds: i32,  // From GHA2DB_MAX_GHAPI_WAIT
    pub max_ghapi_retry: i32,         // From GHA2DB_MAX_GHAPI_RETRY
}

impl Default for Context {
    fn default() -> Self {
        Context {
            data_dir: "/etc/gha2db/".to_string(),
            debug: 0,
            cmd_debug: 0,
            github_debug: 0,
            dry_run: false,
            json_out: false,
            db_out: true,
            st: false,
            n_cpus: 0,
            pg_host: "localhost".to_string(),
            pg_port: "5432".to_string(),
            pg_db: "gha".to_string(),
            pg_user: "gha_admin".to_string(),
            pg_pass: "password".to_string(),
            pg_ssl: "disable".to_string(),
            index: false,
            table: true,
            tools: true,
            mgetc: String::new(),
            q_out: false,
            ctx_out: false,
            log_time: true,
            default_start_date: DateTime::parse_from_str("2012-07-01 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap().with_timezone(&Utc),
            force_start_date: false,
            last_series: "events_h".to_string(),
            skip_tsdb: false,
            skip_pdb: false,
            reset_tsdb: false,
            reset_ranges: false,
            explain: false,
            old_format: false,
            exact: false,
            log_to_db: true,
            local: false,
            absolute: false,
            local_cmd: false,
            metrics_yaml: String::new(),
            tags_yaml: String::new(),
            columns_yaml: String::new(),
            vars_yaml: String::new(),
            vars_fn_yaml: "vars.yaml".to_string(),
            skip_dates_yaml: "skip_dates.yaml".to_string(),
            github_oauth: String::new(),
            clear_db_period: "1 week".to_string(),
            clear_affs_lock_period: "16 hours".to_string(),
            clear_giant_lock_period: "40 hours".to_string(),
            trials: vec![10, 30, 60, 120, 300, 600],
            webhook_root: "/hook".to_string(),
            webhook_port: ":1982".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            check_payload: true,
            full_deploy: true,
            deploy_branches: vec!["master".to_string()],
            deploy_statuses: vec!["Passed".to_string(), "Fixed".to_string()],
            deploy_results: vec![0],
            deploy_types: vec!["push".to_string()],
            project_root: String::new(),
            exec_fatal: true,
            exec_quiet: false,
            exec_output: false,
            project: String::new(),
            tests_yaml: "tests.yaml".to_string(),
            repos_dir: "~/devstats_repos/".to_string(),
            process_repos: false,
            process_commits: false,
            external_info: false,
            projects_commits: String::new(),
            propagate_only_var: false,
            projects_yaml: "projects.yaml".to_string(),
            company_acq_yaml: String::new(),
            affiliations_json: "github_users.json".to_string(),
            projects_override: HashMap::new(),
            exclude_repos: HashMap::new(),
            input_dbs: Vec::new(),
            output_db: String::new(),
            tm_offset: 0,
            default_hostname: "devstats.cncf.io".to_string(),
            recent_range: "12 hours".to_string(),
            recent_repos_range: "1 day".to_string(),
            min_ghapi_points: 1000,
            max_ghapi_wait_seconds: 7200,
            max_ghapi_retry: 6,
        }
    }
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Load context from environment variables
    pub fn from_env() -> anyhow::Result<Self> {
        let mut ctx = Self::default();
        
        // Load configuration from environment variables
        if let Ok(data_dir) = std::env::var("GHA2DB_DATADIR") {
            ctx.data_dir = data_dir;
        }
        
        if let Ok(debug) = std::env::var("GHA2DB_DEBUG") {
            ctx.debug = debug.parse().unwrap_or(0);
        }
        
        if let Ok(cmd_debug) = std::env::var("GHA2DB_CMDDEBUG") {
            ctx.cmd_debug = cmd_debug.parse().unwrap_or(0);
        }
        
        if let Ok(github_debug) = std::env::var("GHA2DB_GITHUB_DEBUG") {
            ctx.github_debug = github_debug.parse().unwrap_or(0);
        }
        
        // PostgreSQL configuration
        if let Ok(pg_host) = std::env::var("PG_HOST") {
            ctx.pg_host = pg_host;
        }
        
        if let Ok(pg_port) = std::env::var("PG_PORT") {
            ctx.pg_port = pg_port;
        }
        
        if let Ok(pg_db) = std::env::var("PG_DB") {
            ctx.pg_db = pg_db;
        }
        
        if let Ok(pg_user) = std::env::var("PG_USER") {
            ctx.pg_user = pg_user;
        }
        
        if let Ok(pg_pass) = std::env::var("PG_PASS") {
            ctx.pg_pass = pg_pass;
        }
        
        if let Ok(pg_ssl) = std::env::var("PG_SSL") {
            ctx.pg_ssl = pg_ssl;
        }
        
        // Project configuration
        if let Ok(project) = std::env::var("GHA2DB_PROJECT") {
            ctx.project = project;
        }
        
        // GitHub OAuth
        if let Ok(github_oauth) = std::env::var("GHA2DB_GITHUB_OAUTH") {
            ctx.github_oauth = github_oauth;
        }
        
        // Boolean flags from environment
        ctx.dry_run = std::env::var("GHA2DB_DRY_RUN").is_ok();
        ctx.json_out = std::env::var("GHA2DB_JSON").is_ok();
        ctx.db_out = std::env::var("GHA2DB_NODB").is_err(); // Inverted logic
        ctx.st = std::env::var("GHA2DB_ST").is_ok();
        ctx.index = std::env::var("GHA2DB_INDEX").is_ok();
        ctx.table = std::env::var("GHA2DB_SKIPTABLE").is_err(); // Inverted logic
        ctx.tools = std::env::var("GHA2DB_SKIPTOOLS").is_err(); // Inverted logic
        ctx.q_out = std::env::var("GHA2DB_QOUT").is_ok();
        ctx.ctx_out = std::env::var("GHA2DB_CTXOUT").is_ok();
        ctx.log_time = std::env::var("GHA2DB_SKIPTIME").is_err(); // Inverted logic
        ctx.skip_tsdb = std::env::var("GHA2DB_SKIPTSDB").is_ok();
        ctx.skip_pdb = std::env::var("GHA2DB_SKIPPDB").is_ok();
        ctx.reset_tsdb = std::env::var("GHA2DB_RESETTSDB").is_ok();
        ctx.reset_ranges = std::env::var("GHA2DB_RESETRANGES").is_ok();
        ctx.explain = std::env::var("GHA2DB_EXPLAIN").is_ok();
        ctx.old_format = std::env::var("GHA2DB_OLDFMT").is_ok();
        ctx.exact = std::env::var("GHA2DB_EXACT").is_ok();
        ctx.log_to_db = std::env::var("GHA2DB_SKIPLOG").is_err(); // Inverted logic
        ctx.local = std::env::var("GHA2DB_LOCAL").is_ok();
        ctx.absolute = std::env::var("GHA2DB_ABSOLUTE").is_ok();
        ctx.local_cmd = std::env::var("GHA2DB_LOCAL_CMD").is_ok();
        ctx.check_payload = std::env::var("GHA2DB_SKIP_VERIFY_PAYLOAD").is_err(); // Inverted logic
        ctx.full_deploy = std::env::var("GHA2DB_SKIP_FULL_DEPLOY").is_err(); // Inverted logic
        ctx.process_repos = std::env::var("GHA2DB_PROCESS_REPOS").is_ok();
        ctx.process_commits = std::env::var("GHA2DB_PROCESS_COMMITS").is_ok();
        ctx.external_info = std::env::var("GHA2DB_EXTERNAL_INFO").is_ok();
        ctx.propagate_only_var = std::env::var("GHA2DB_PROPAGATE_ONLY_VAR").is_ok();
        
        Ok(ctx)
    }
    
    /// Get PostgreSQL connection string
    pub fn pg_conn_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} sslmode={}",
            self.pg_host, self.pg_port, self.pg_db, self.pg_user, self.pg_pass, self.pg_ssl
        )
    }
}