package devstatscode

// Today - common constant string
const Today string = "today"

// DefaultDataDir - common constant string
const DefaultDataDir string = "/etc/gha2db/"

// Retry - common constant string
const Retry string = "retry"

// Password - common constant string
const Password string = "password"

// GHAAdmin - common constant string
const GHAAdmin string = "gha_admin"

// Quarter - common constant string
const Quarter string = "quarter"

// Now - common constant string
const Now string = "now"

// GHA - common constant string
const GHA string = "gha"

// Localhost - common constant string
const Localhost string = "localhost"

// GHArchiveURL - default GH Archive base URL (gha2db downloads <base>YYYY-MM-DD-H.json.gz from it)
const GHArchiveURL string = "http://data.gharchive.org/"

// Devstats - common constant string
const Devstats string = "devstats"

// DevstatsCode - common constant string
const DevstatsCode string = "devstatscode"

// TimeoutError - common constant string
const TimeoutError string = "{\"error\":\"timeout\"}\n"

// EngineIsClosedError - common constant string
const EngineIsClosedError string = "engine is closed"

// LocalGitScripts - common constant string
const LocalGitScripts string = "./git/"

// Metrics - common constant string
const Metrics string = "metrics/"

// Unset - common constant string
const Unset string = "{{unset}}"

// TimeCol - common constant string
const TimeCol string = "time"

// SeriesCol - common constant string
const SeriesCol string = "series"

// PeriodCol - common constant string
const PeriodCol string = "period"

// Null - common constant string
const Null string = "null"

// HideCfgFile - common constant string
const HideCfgFile string = "hide/hide.csv"

// All - common constant string
const All string = "all"

// ALL - common constant string
const ALL string = "All"

// Kubernetes - common constant string
const Kubernetes string = "kubernetes"

// ArtificialIDBase - artificial (GitHub API sourced) event id base: 2^48.
// Issue/PR timeline events use ArtificialIDBase + REST event id (offsets ~2.8e10 in 2026, growing ~1e10/year).
// API-restored object classes below get uniform 4e12-wide sub-bands (object ids are ~3e9 in 2026, growing <1e9/year),
// so classes cannot collide for centuries. Sync events (ArtificialIDBase + UnixNano/31622) stay >= 329900000000000
// (offset >= ~48.4e12 since 2018 and growing), above all sub-bands.
const ArtificialIDBase int64 = 281474976710656

// ArtificialCommentIDBase - API-restored issue comments
const ArtificialCommentIDBase int64 = ArtificialIDBase + 4000000000000

// ArtificialReviewCommentIDBase - API-restored PR review comments
const ArtificialReviewCommentIDBase int64 = ArtificialIDBase + 8000000000000

// ArtificialCommitCommentIDBase - API-restored commit comments
const ArtificialCommitCommentIDBase int64 = ArtificialIDBase + 12000000000000

// ArtificialReviewIDBase - API-restored PR reviews
const ArtificialReviewIDBase int64 = ArtificialIDBase + 16000000000000

// ArtificialForkIDBase - API-restored forks
const ArtificialForkIDBase int64 = ArtificialIDBase + 20000000000000

// ArtificialReleaseIDBase - API-restored releases
const ArtificialReleaseIDBase int64 = ArtificialIDBase + 24000000000000

// ArtificialIssueIDBase - API-synthesized issue lifecycle events (ghapi2db issues prs pass):
// ArtificialIssueIDBase + 2*issue_id + 0 (opened), + 1 (closed)
const ArtificialIssueIDBase int64 = ArtificialIDBase + 28000000000000

// ArtificialPRIDBase - API-synthesized pull request lifecycle events (ghapi2db issues prs pass):
// ArtificialPRIDBase + 2*pull_request_id + 0 (opened), + 1 (closed)
const ArtificialPRIDBase int64 = ArtificialIDBase + 32000000000000

// SyncEventIDThreshold - event ids >= this are 'sync' events; artificial sub-bands must stay below
const SyncEventIDThreshold int64 = 329900000000000

// NativeIDBandBase - width of one native (GitHub sourced) event id band: 10^12.
// GitHub restarted its event id sequence on 2025-10-09 (ids fell from ~5.6e10 to ~4e9) and since then allocates ids
// from two independent sequences (PushEvent/CreateEvent/DeleteEvent vs every other type) that reuse the 2016-2025 ids
// and each other's ids, so a raw id alone no longer identifies an event. Native events created at/after the epoch of a
// NativeIDBandRules rule are stored (gha_events.id and every *.event_id) as `raw id + band*NativeIDBandBase`, see
// eventid.go: band 0 (the raw id) for everything older than every rule, so history is untouched, bands 1, 2, ... for
// the current sequences. Raw ids stay below 10^12 for decades and 2^48/10^12 = 281 bands fit below ArtificialIDBase,
// so banded ids remain 'native' (0 < id < 2^48) for every consumer; `id / 10^12` is the band, `id % 10^12` the raw id.
const NativeIDBandBase int64 = 1000000000000

// Abuse - common constant string
const Abuse string = "abuse"

// NotFound - common constant string
const NotFound string = "not_found"

// IssueIsDeleted - common constant string
const IssueIsDeleted string = "issue_is_deleted"

// MovedPermanently - common constant string
const MovedPermanently string = "moved_permanently"

// Merged - common constant string
const Merged string = "merged"

// InvalidCatalogName - common constant string
const InvalidCatalogName string = "invalid_catalog_name"

// Nil - common constant string
const Nil string = "(nil)"

// Reconnect - common constant string
const Reconnect string = "reconnect"

// OK - common constant string
const OK string = "ok"

// RepoNamesQuery - common constant string
const RepoNamesQuery string = "select distinct name from gha_repos where name like '%_/_%' and name not like '%/%/%'"

// GithubIDContributions - common constant string
const GithubIDContributions string = "GithubIDContributions"

// DevActCnt - common constant string
const DevActCnt string = "DevActCnt"

// DevActCntComp - common constant string
const DevActCntComp string = "DevActCntComp"

// ComContribRepoGrp - common constant string
const ComContribRepoGrp string = "ComContribRepoGrp"

// CompaniesTable - common constant string
const CompaniesTable string = "CompaniesTable"

// ComStatsRepoGrp - common constant string
const ComStatsRepoGrp string = "ComStatsRepoGrp"

// Health - common constant string
const Health string = "Health"

// Events - common constant string
const Events string = "Events"

// ListAPIs - common constant string
const ListAPIs string = "ListAPIs"

// CumulativeCounts - common constant string
const CumulativeCounts string = "CumulativeCounts"

// ListProjects - common constant string
const ListProjects string = "ListProjects"

// RepoGroups - common constant string
const RepoGroups string = "RepoGroups"

// Ranges - common constant string
const Ranges string = "Ranges"

// Repos - common constant string
const Repos string = "Repos"

// Countries - common constant string
const Countries string = "Countries"

// Companies - common constant string
const Companies string = "Companies"

// SiteStats - common constant string
const SiteStats string = "SiteStats"

// Day - common constant string
const Day string = "day"

// Week - common constant string
const Week string = "week"

// Hour - common constant string
const Hour string = "hour"

// Month - common constant string
const Month string = "month"

// Year - common constant string
const Year string = "year"

// GhostActorID - ID of the 'ghost' placeholder actor GitHub uses for deleted accounts
const GhostActorID int64 = 10137

// GhostActorLogin - login of the 'ghost' placeholder actor GitHub uses for deleted accounts
const GhostActorLogin string = "ghost"
