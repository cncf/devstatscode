//! Common constants — port of `const.go`.

pub const TODAY: &str = "today";
pub const DEFAULT_DATA_DIR: &str = "/etc/gha2db/";
pub const RETRY: &str = "retry";
pub const PASSWORD: &str = "password";
pub const GHA_ADMIN: &str = "gha_admin";
pub const QUARTER: &str = "quarter";
pub const NOW: &str = "now";
pub const GHA: &str = "gha";
pub const LOCALHOST: &str = "localhost";
/// Go `GHArchiveURL`: default GH Archive base URL (gha2db downloads `<base>YYYY-MM-DD-H.json.gz`).
pub const GHARCHIVE_URL: &str = "http://data.gharchive.org/";
pub const DEVSTATS: &str = "devstats";
pub const DEVSTATS_CODE: &str = "devstatscode";
pub const TIMEOUT_ERROR: &str = "{\"error\":\"timeout\"}\n";
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
pub const ALL_CAP: &str = "All";
pub const KUBERNETES: &str = "kubernetes";

/// Artificial (GitHub API sourced) event id base: 2^48.
/// Issue/PR timeline events use `ARTIFICIAL_ID_BASE + REST event id`.
/// API-restored object classes below get uniform 4e12-wide sub-bands.
pub const ARTIFICIAL_ID_BASE: i64 = 281_474_976_710_656;
/// API-restored issue comments.
pub const ARTIFICIAL_COMMENT_ID_BASE: i64 = ARTIFICIAL_ID_BASE + 4_000_000_000_000;
/// API-restored PR review comments.
pub const ARTIFICIAL_REVIEW_COMMENT_ID_BASE: i64 = ARTIFICIAL_ID_BASE + 8_000_000_000_000;
/// API-restored commit comments.
pub const ARTIFICIAL_COMMIT_COMMENT_ID_BASE: i64 = ARTIFICIAL_ID_BASE + 12_000_000_000_000;
/// API-restored PR reviews.
pub const ARTIFICIAL_REVIEW_ID_BASE: i64 = ARTIFICIAL_ID_BASE + 16_000_000_000_000;
/// API-restored forks.
pub const ARTIFICIAL_FORK_ID_BASE: i64 = ARTIFICIAL_ID_BASE + 20_000_000_000_000;
/// API-restored releases.
pub const ARTIFICIAL_RELEASE_ID_BASE: i64 = ARTIFICIAL_ID_BASE + 24_000_000_000_000;
/// Event ids >= this are 'sync' events; artificial sub-bands must stay below.
pub const SYNC_EVENT_ID_THRESHOLD: i64 = 329_900_000_000_000;

pub const ABUSE: &str = "abuse";
pub const NOT_FOUND: &str = "not_found";
pub const ISSUE_IS_DELETED: &str = "issue_is_deleted";
pub const MOVED_PERMANENTLY: &str = "moved_permanently";
pub const MERGED: &str = "merged";
pub const INVALID_CATALOG_NAME: &str = "invalid_catalog_name";
pub const NIL: &str = "(nil)";
pub const RECONNECT: &str = "reconnect";
pub const OK: &str = "ok";
pub const REPO_NAMES_QUERY: &str =
    "select distinct name from gha_repos where name like '%_/_%' and name not like '%/%/%'";
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
pub const DAY: &str = "day";
pub const WEEK: &str = "week";
pub const HOUR: &str = "hour";
pub const MONTH: &str = "month";
pub const YEAR: &str = "year";
/// ID of the 'ghost' placeholder actor GitHub uses for deleted accounts.
pub const GHOST_ACTOR_ID: i64 = 10137;
/// Login of the 'ghost' placeholder actor GitHub uses for deleted accounts.
pub const GHOST_ACTOR_LOGIN: &str = "ghost";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artificial_bands_are_below_sync_threshold() {
        assert_eq!(ARTIFICIAL_ID_BASE, 1i64 << 48);
        for base in [
            ARTIFICIAL_COMMENT_ID_BASE,
            ARTIFICIAL_REVIEW_COMMENT_ID_BASE,
            ARTIFICIAL_COMMIT_COMMENT_ID_BASE,
            ARTIFICIAL_REVIEW_ID_BASE,
            ARTIFICIAL_FORK_ID_BASE,
            ARTIFICIAL_RELEASE_ID_BASE,
        ] {
            assert!(base > ARTIFICIAL_ID_BASE);
            assert!(base + 4_000_000_000_000 <= SYNC_EVENT_ID_THRESHOLD);
        }
    }
}
