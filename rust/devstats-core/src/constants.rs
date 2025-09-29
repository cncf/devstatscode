// Constants used throughout the DevStats system
// Translated from const.go

// Common constant strings
pub const TODAY: &str = "today";
pub const DEFAULT_DATA_DIR: &str = "/etc/gha2db/";
pub const RETRY: &str = "retry";
pub const PASSWORD: &str = "password";
pub const GHA_ADMIN: &str = "gha_admin";
pub const QUARTER: &str = "quarter";
pub const NOW: &str = "now";
pub const GHA: &str = "gha";
pub const LOCALHOST: &str = "localhost";
pub const DEVSTATS: &str = "devstats";
pub const DEVSTATS_CODE: &str = "devstatscode";
pub const TIMEOUT_ERROR: &str = r#"{"error":"timeout"}
"#;
pub const ENGINE_IS_CLOSED_ERROR: &str = "engine is closed";
pub const LOCAL_GIT_SCRIPTS: &str = "./git/";
pub const METRICS: &str = "metrics/";
pub const UNSET: &str = "{{unset}}";
pub const TIME_COL: &str = "time";
pub const SERIES_COL: &str = "series";
pub const PERIOD_COL: &str = "period";
pub const NULL: &str = "null";
pub const HIDE_CFG_FILE: &str = "hide/hide.csv";
pub const ALL: &str = "all";
pub const ALL_CAPS: &str = "All";
pub const KUBERNETES: &str = "kubernetes";
pub const ABUSE: &str = "abuse";
pub const NOT_FOUND: &str = "not_found";
pub const ISSUE_IS_DELETED: &str = "issue_is_deleted";
pub const MOVED_PERMANENTLY: &str = "moved_permanently";
pub const MERGED: &str = "merged";
pub const INVALID_CATALOG_NAME: &str = "invalid_catalog_name";
pub const NIL: &str = "(nil)";
pub const RECONNECT: &str = "reconnect";
pub const OK: &str = "ok";
pub const REPO_NAMES_QUERY: &str = "select distinct name from gha_repos where name like '%_/_%' and name not like '%/%/%'";

// API endpoint constants
pub const GITHUB_ID_CONTRIBUTIONS: &str = "GithubIDContributions";
pub const DEV_ACT_CNT: &str = "DevActCnt";
pub const DEV_ACT_CNT_COMP: &str = "DevActCntComp";
pub const COM_CONTRIB_REPO_GRP: &str = "ComContribRepoGrp";
pub const COMPANIES_TABLE: &str = "CompaniesTable";
pub const COM_STATS_REPO_GRP: &str = "ComStatsRepoGrp";
pub const HEALTH: &str = "Health";
pub const EVENTS: &str = "Events";
pub const LIST_APIS: &str = "ListAPIs";
pub const CUMULATIVE_COUNTS: &str = "CumulativeCounts";
pub const LIST_PROJECTS: &str = "ListProjects";
pub const REPO_GROUPS: &str = "RepoGroups";
pub const RANGES: &str = "Ranges";
pub const REPOS: &str = "Repos";
pub const COUNTRIES: &str = "Countries";
pub const COMPANIES: &str = "Companies";
pub const SITE_STATS: &str = "SiteStats";

// Time period constants
pub const DAY: &str = "day";
pub const WEEK: &str = "week";
pub const HOUR: &str = "hour";
pub const MONTH: &str = "month";
pub const YEAR: &str = "year";