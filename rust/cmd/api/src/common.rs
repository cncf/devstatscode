//! Shared pieces of the API server: payload types (the JSON documents the
//! endpoints return), the request payload helpers with their exact Go error
//! texts, the tag/period/metric lookups and `ensureManualData`.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock, RwLock};
use std::thread;

use devstatscode::chrono::{DateTime, Datelike, FixedOffset, Local, TimeZone, Timelike, Utc};
use devstatscode::consts::{ALL_CAP, GHA};
use devstatscode::exec::exec_command;
use devstatscode::pg::api::query_sql_log_err;
use devstatscode::pg::{PgConn, PgError, Rows, SqlArg};
use devstatscode::time::{day_start, to_ymdh_date, to_ymdhms_date};
use devstatscode::{gofmt, printf, Ctx};
use serde::Serialize;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Constants (api.go)
// ---------------------------------------------------------------------------

/// GithubIDContributionsCacheTTL - Github login contributions cache expiration 24 hours
pub const GITHUB_ID_CONTRIBUTIONS_CACHE_TTL: i64 = 86400;
/// CumulativeCountsCacheTTL - Cumulative counts cache expiration 12 hours
pub const CUMULATIVE_COUNTS_CACHE_TTL: i64 = 43200;
/// SiteStatsCacheTTL - Site stats cache expiration 12 hours
pub const SITE_STATS_CACHE_TTL: i64 = 43200;

/// Maximum number of background `calc_metric` runs (Go `gMaxBg`).
pub const MAX_BG: usize = 3;

// ---------------------------------------------------------------------------
// Global server state (the Go package level variables)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct BgState {
    /// Go `gNumBg`.
    pub num: usize,
    /// Go `gBgMap`.
    pub running: HashSet<String>,
}

/// Cached `CumulativeCounts` response (Go `cumulativeCountsCacheEntry`).
pub struct CumulativeCountsCacheEntry {
    pub dt: DateTime<Local>,
    pub cumulative_counts: CumulativeCountsPayload,
}

/// Cached `SiteStats` response (Go `siteStatsCacheEntry`).
pub struct SiteStatsCacheEntry {
    pub dt: DateTime<Local>,
    /// Go `%v` of `dt` (a `time.Now()` value with its monotonic reading).
    pub dt_str: String,
    pub site_stats: SiteStatsPayload,
}

/// Cached `GithubIDContributions` counts (Go `githubIDContributionsCacheEntry`).
pub struct GithubIDContributionsCacheEntry {
    pub dt: DateTime<Local>,
    pub dt_str: String,
    pub stats: [i64; 3],
}

pub struct State {
    /// Go `gNameToDB`: project name / full name / database → database.
    pub name_to_db: RwLock<BTreeMap<String, String>>,
    /// Go `gProjects`: full names of the enabled projects.
    pub projects: RwLock<Vec<String>>,
    pub bg: RwLock<BgState>,
    pub site_stats_cache: Mutex<BTreeMap<[String; 2], SiteStatsCacheEntry>>,
    pub cumulative_counts_cache: Mutex<BTreeMap<[String; 3], CumulativeCountsCacheEntry>>,
    pub github_id_contributions_cache: Mutex<BTreeMap<String, GithubIDContributionsCacheEntry>>,
}

static STATE: OnceLock<State> = OnceLock::new();

/// The process-wide server state.
pub fn state() -> &'static State {
    STATE.get_or_init(|| State {
        name_to_db: RwLock::new(BTreeMap::new()),
        projects: RwLock::new(Vec::new()),
        bg: RwLock::new(BgState::default()),
        site_stats_cache: Mutex::new(BTreeMap::new()),
        cumulative_counts_cache: Mutex::new(BTreeMap::new()),
        github_id_contributions_cache: Mutex::new(BTreeMap::new()),
    })
}

/// Go `gNumBg` (number of `calc_metric` runs in the background).
pub fn num_bg() -> usize {
    state().bg.read().map(|b| b.num).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// An API error: `msg` is the Go `err.Error()` text. `logged` tells whether
/// the Go handler stored the error in the `err` variable printed by its
/// deferred `<API>(exit): … err:<err>` log line — several early returns in
/// api.go use a shadowed `err` (or pass a fresh error straight to
/// `returnError`), so that line shows `err:<nil>` for them.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub msg: String,
    pub logged: bool,
}

impl ApiError {
    pub fn new(msg: impl Into<String>) -> Self {
        ApiError {
            msg: msg.into(),
            logged: true,
        }
    }

    /// An error the deferred exit log does not see (`err:<nil>`).
    pub fn quiet(msg: impl Into<String>) -> Self {
        ApiError {
            msg: msg.into(),
            logged: false,
        }
    }
}

impl From<PgError> for ApiError {
    fn from(e: PgError) -> Self {
        ApiError::new(e.to_string())
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

// ---------------------------------------------------------------------------
// Response payload types (Go structs, JSON field order preserved)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ErrorPayload {
    pub error: String,
}

#[derive(Serialize)]
pub struct HealthPayload {
    pub project: String,
    pub db_name: String,
    pub events: i64,
}

#[derive(Serialize)]
pub struct ListApisPayload {
    pub apis: Vec<&'static str>,
}

#[derive(Serialize)]
pub struct ListProjectsPayload {
    pub projects: Vec<String>,
}

#[derive(Serialize)]
pub struct EventsPayload {
    pub project: String,
    pub db_name: String,
    pub timestamps: Vec<String>,
    pub from: String,
    pub to: String,
    pub values: Vec<i64>,
}

#[derive(Serialize, Clone)]
pub struct CumulativeCountsPayload {
    pub project: String,
    pub metric: String,
    pub db_name: String,
    pub timestamps: Vec<String>,
    pub values: Vec<i64>,
}

#[derive(Serialize, Clone, Default)]
pub struct SiteStatsPayload {
    pub project: String,
    pub db_name: String,
    pub contributors: i64,
    pub contributions: i64,
    pub boc: i64,
    pub committers: i64,
    pub commits: i64,
    pub events: i64,
    pub forkers: i64,
    pub repositories: i64,
    pub stargazers: i64,
    pub countries: i64,
    pub companies: i64,
}

impl SiteStatsPayload {
    /// Go `%+v` of the struct.
    pub fn go_string(&self) -> String {
        format!(
            "{{Project:{} DB:{} Contributors:{} Contributions:{} BOC:{} Committers:{} Commits:{} Events:{} Forkers:{} Repositories:{} Stargazers:{} Countries:{} Companies:{}}}",
            self.project,
            self.db_name,
            self.contributors,
            self.contributions,
            self.boc,
            self.committers,
            self.commits,
            self.events,
            self.forkers,
            self.repositories,
            self.stargazers,
            self.countries,
            self.companies
        )
    }
}

/// Go nil slices encode as `null`, empty non-nil slices as `[]`: the
/// handlers building their result with `append` on a nil slice produce
/// `null` when there are no rows.
pub type NilVec<T> = Option<Vec<T>>;

/// `append`-built slice: `None` (nil → `null`) when nothing was appended.
pub fn nil_vec<T>(v: Vec<T>) -> NilVec<T> {
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}

#[derive(Serialize)]
pub struct CompaniesTablePayload {
    pub project: String,
    pub db_name: String,
    pub range: String,
    pub metric: String,
    pub rank: NilVec<i64>,
    pub company: NilVec<String>,
    pub number: NilVec<f64>,
}

#[derive(Serialize)]
pub struct ComContribRepoGrpPayload {
    pub project: String,
    pub db_name: String,
    pub period: String,
    pub repository_group: String,
    pub companies: NilVec<f64>,
    pub developers: NilVec<f64>,
    pub companies_timestamps: NilVec<String>,
    pub developers_timestamps: NilVec<String>,
}

#[derive(Serialize)]
pub struct GithubIdContributionsResponse {
    pub contributions: i64,
    pub issues: i64,
    pub prs: i64,
}

#[derive(Serialize)]
pub struct DevActCntPayload {
    pub project: String,
    pub db_name: String,
    pub range: String,
    pub metric: String,
    pub repository_group: String,
    pub country: String,
    pub github_id: String,
    pub filter: String,
    pub rank: NilVec<i64>,
    pub login: NilVec<String>,
    pub number: NilVec<i64>,
}

#[derive(Serialize)]
pub struct DevActCntReposPayload {
    pub project: String,
    pub db_name: String,
    pub range: String,
    pub metric: String,
    pub repository: String,
    pub country: String,
    pub github_id: String,
    pub filter: String,
    pub rank: NilVec<i64>,
    pub login: NilVec<String>,
    pub number: NilVec<i64>,
}

#[derive(Serialize)]
pub struct DevActCntCompPayload {
    pub project: String,
    pub db_name: String,
    pub range: String,
    pub metric: String,
    pub repository_group: String,
    pub country: String,
    pub companies: Vec<String>,
    pub github_id: String,
    pub rank: NilVec<i64>,
    pub login: NilVec<String>,
    pub company: NilVec<String>,
    pub number: NilVec<i64>,
}

#[derive(Serialize)]
pub struct DevActCntCompReposPayload {
    pub project: String,
    pub db_name: String,
    pub range: String,
    pub metric: String,
    pub repository: String,
    pub country: String,
    pub companies: Vec<String>,
    pub github_id: String,
    pub rank: NilVec<i64>,
    pub login: NilVec<String>,
    pub company: NilVec<String>,
    pub number: NilVec<i64>,
}

#[derive(Serialize)]
pub struct ComStatsRepoGrpPayload {
    pub project: String,
    pub db_name: String,
    pub period: String,
    pub metric: String,
    pub repository_group: String,
    pub companies: Vec<String>,
    pub from: String,
    pub to: String,
    pub values: Vec<BTreeMap<String, f64>>,
    pub timestamps: Vec<String>,
}

#[derive(Serialize)]
pub struct RepoGroupsPayload {
    pub project: String,
    pub db_name: String,
    pub repo_groups: NilVec<String>,
}

#[derive(Serialize)]
pub struct CompaniesPayload {
    pub project: String,
    pub db_name: String,
    pub companies: NilVec<String>,
}

#[derive(Serialize)]
pub struct RangesPayload {
    pub project: String,
    pub db_name: String,
    pub ranges: NilVec<String>,
}

#[derive(Serialize)]
pub struct CountriesPayload {
    pub project: String,
    pub db_name: String,
    pub countries: NilVec<String>,
}

#[derive(Serialize)]
pub struct ReposPayload {
    pub project: String,
    pub db_name: String,
    pub repo_groups: Vec<String>,
    pub repos: Vec<String>,
}

/// `jsoniter.NewEncoder(w).Encode(v)`: compact JSON plus a newline.
pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    devstatscode::json::encode_json_line(v).unwrap_or_default()
}

/// `time.Time.MarshalJSON` content (RFC3339Nano) of a value read from a
/// timestamp column.
pub fn json_time(t: &DateTime<FixedOffset>) -> String {
    let mut s = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        t.year(),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    let nanos = t.nanosecond();
    if nanos != 0 {
        let mut frac = format!("{:09}", nanos);
        while frac.ends_with('0') {
            frac.pop();
        }
        s.push('.');
        s.push_str(&frac);
    }
    let off = t.offset().local_minus_utc();
    if off == 0 {
        s.push('Z');
    } else {
        let a = off.unsigned_abs();
        s.push_str(&format!(
            "{}{:02}:{:02}",
            if off < 0 { '-' } else { '+' },
            a / 3600,
            (a % 3600) / 60
        ));
    }
    s
}

/// A zero `DateTime<FixedOffset>` scan target.
pub fn zero_time() -> DateTime<FixedOffset> {
    DateTime::<Utc>::from_timestamp(0, 0)
        .unwrap_or_default()
        .fixed_offset()
}

// ---------------------------------------------------------------------------
// Request payload (Go `map[string]interface{}`)
// ---------------------------------------------------------------------------

/// The decoded `payload` section: `None` is Go's nil map (section missing or
/// `null`).
pub type Payload = Option<serde_json::Map<String, Value>>;

/// Go `%+v` of the payload map (`map[k:v …]` with sorted keys, `map[]` when
/// nil/empty).
pub fn payload_string(p: &Payload) -> String {
    match p {
        Some(m) if !m.is_empty() => gofmt::json_value(&Value::Object(m.clone())),
        _ => "map[]".to_string(),
    }
}

/// Go `%T` of a value decoded from JSON into `interface{}`.
pub fn go_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "<nil>",
        Value::Bool(_) => "bool",
        Value::Number(_) => "float64",
        Value::String(_) => "string",
        Value::Array(_) => "[]interface {}",
        Value::Object(_) => "map[string]interface {}",
    }
}

/// Go `handleSharedPayload`: the `project` field and its database.
///
/// `project` is set as soon as it is known — like Go's named result it is
/// printed by the exit log line even when the database lookup fails.
pub fn handle_shared_payload(payload: &Payload, project: &mut String) -> ApiResult<String> {
    let Some(m) = payload.as_ref().filter(|m| !m.is_empty()) else {
        return Err(ApiError::new("'payload' section empty or missing"));
    };
    let Some(iproject) = m.get("project") else {
        return Err(ApiError::new(
            "missing 'project' field in 'payload' section",
        ));
    };
    let Value::String(name) = iproject else {
        return Err(ApiError::new(format!(
            "'payload' 'project' field '{}' is not a string",
            gofmt::json_value(iproject)
        )));
    };
    *project = name.clone();
    name_to_db(name)
}

/// Go `nameToDB`.
pub fn name_to_db(name: &str) -> ApiResult<String> {
    let found = state()
        .name_to_db
        .read()
        .ok()
        .and_then(|m| m.get(name).cloned());
    found.ok_or_else(|| ApiError::new(format!("database not found for project '{}'", name)))
}

/// Go `getPayloadStringParam`. Errors are "quiet" (see [`ApiError`]): the
/// callers' `paramValue, err := …` shadows the logged `err`.
pub fn get_payload_string_param(
    param_name: &str,
    payload: &Payload,
    optional: bool,
) -> ApiResult<String> {
    let value = payload.as_ref().and_then(|m| m.get(param_name));
    let Some(iparam) = value else {
        if optional {
            return Ok(String::new());
        }
        return Err(ApiError::quiet(format!(
            "missing '{}' field in 'payload' section (optional {})",
            param_name, optional
        )));
    };
    match iparam {
        Value::String(s) => Ok(s.clone()),
        other => Err(ApiError::quiet(format!(
            "'payload' '{}' field '{}'/{} is not a string (optional {})",
            param_name,
            gofmt::json_value(other),
            go_type_name(other),
            optional
        ))),
    }
}

/// Go `getPayloadStringArrayParam` (quiet errors, like the string variant).
pub fn get_payload_string_array_param(
    param_name: &str,
    payload: &Payload,
    optional: bool,
    allow_empty: bool,
) -> ApiResult<Vec<String>> {
    let value = payload.as_ref().and_then(|m| m.get(param_name));
    let Some(iparam) = value else {
        if optional {
            return Ok(Vec::new());
        }
        return Err(ApiError::quiet(format!(
            "missing '{}' field in 'payload' section (optional {}, allow empty {})",
            param_name, optional, allow_empty
        )));
    };
    let Value::Array(iary) = iparam else {
        return Err(ApiError::quiet(format!(
            "'payload' '{}' field '{}'/{} is not an array (optional {}, allow empty {})",
            param_name,
            gofmt::json_value(iparam),
            go_type_name(iparam),
            optional,
            allow_empty
        )));
    };
    let mut param = Vec::new();
    let mut err = None;
    for (idx, item) in iary.iter().enumerate() {
        match item {
            Value::String(s) => param.push(s.clone()),
            other => {
                // Go keeps looping (the last offending item wins) and appends
                // the zero string.
                err = Some(ApiError::quiet(format!(
                    "'payload' '{}' field '{}' #{} item '{}'/{} is not a string (optional {}, allow empty {})",
                    param_name,
                    gofmt::json_value(iparam),
                    idx + 1,
                    gofmt::json_value(other),
                    go_type_name(other),
                    optional,
                    allow_empty
                )));
                param.push(String::new());
            }
        }
    }
    if !allow_empty && param.is_empty() {
        return Err(ApiError::quiet(format!(
            "'payload' '{}' field '{}' cannot be empty (optional {}, allow empty {})",
            param_name,
            gofmt::slice(&param),
            optional,
            allow_empty
        )));
    }
    match err {
        Some(e) => Err(e),
        None => Ok(param),
    }
}

/// Go loop `for paramName := range params { … getPayloadStringParam … }`:
/// the values of the named parameters (the first failure is reported; Go
/// iterates the map randomly, here in the given order).
pub fn string_params(
    names: &[&str],
    payload: &Payload,
    optional: bool,
) -> ApiResult<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for name in names {
        let v = get_payload_string_param(name, payload, optional)?;
        out.insert(name.to_string(), v);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Time parsing / periods
// ---------------------------------------------------------------------------

/// Go `timeParseAny` of api.go (a stricter list than the library one):
/// `2006-01-02T15:04:05Z`, `2006-01-02 15:04:05`, `2006-01-02 15:04`,
/// `2006-01-02 15`, `2006-01-02`, `2006-01`, `2006`.
pub fn time_parse_any(dt_str: &str) -> ApiResult<DateTime<Utc>> {
    let formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H",
        "%Y-%m-%d",
        "%Y-%m",
        "%Y",
    ];
    for f in formats {
        if let Some(t) = parse_go_layout(dt_str, f) {
            return Ok(t);
        }
    }
    Err(ApiError::new(format!(
        "cannot parse datetime: '{}'",
        dt_str
    )))
}

/// `time.Parse(layout, s)` for the layouts above with Go's rules: `2006`
/// is exactly four digits, `01`/`02`/`04`/`05` exactly two, `15` one or two,
/// a fractional second (`.5`, `,123`) is accepted after `05` even though the
/// layouts have none, and nothing may follow.
fn parse_go_layout(s: &str, fmt: &str) -> Option<DateTime<Utc>> {
    use devstatscode::chrono::NaiveDate;
    let mut rest = s;
    let mut year = 0i32;
    let mut month = 1u32;
    let mut day = 1u32;
    let (mut hour, mut min, mut sec, mut nanos) = (0u32, 0u32, 0u32, 0u32);
    let digits = |s: &str| s.bytes().take_while(|b| b.is_ascii_digit()).count();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let spec = chars.next()?;
            let width = match spec {
                'Y' => 4,
                'H' => digits(rest).min(2),
                'm' | 'd' | 'M' | 'S' => 2,
                _ => return None,
            };
            if width == 0 || digits(rest) < width {
                return None;
            }
            let v: u32 = rest[..width].parse().ok()?;
            rest = &rest[width..];
            match spec {
                'Y' => year = v as i32,
                'm' => month = v,
                'd' => day = v,
                'H' => hour = v,
                'M' => min = v,
                'S' => {
                    sec = v;
                    if (rest.starts_with('.') || rest.starts_with(',')) && digits(&rest[1..]) > 0 {
                        let n = digits(&rest[1..]);
                        let frac = &rest[1..1 + n];
                        let mut f = frac[..n.min(9)].to_string();
                        while f.len() < 9 {
                            f.push('0');
                        }
                        nanos = f.parse().ok()?;
                        rest = &rest[1 + n..];
                    }
                }
                _ => return None,
            }
        } else {
            if !rest.starts_with(c) {
                return None;
            }
            rest = &rest[c.len_utf8()..];
        }
    }
    if !rest.is_empty() {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let dt = date.and_hms_nano_opt(hour, min, sec, nanos)?;
    Some(Utc.from_utc_datetime(&dt))
}

/// Go `periodNameToValue`: `range:from,to` manual periods (when allowed) or
/// the `tquick_ranges` suffix of a named range. Returns `(value, manual)`.
pub fn period_name_to_value(
    c: &PgConn,
    ctx: &Ctx,
    period_name: &str,
    allow_manual: bool,
) -> ApiResult<(String, bool)> {
    if allow_manual && period_name.starts_with("range:") {
        let ary: Vec<&str> = period_name[6..].split(',').collect();
        if ary.len() != 2 {
            return Err(ApiError::new(
                "range should be specified as 'range:YYYY[-MM[-DD [HH[-MM[-SS]]]]],YYYY[-MM[-DD [HH[-MM[-SS]]]]]'",
            ));
        }
        let from = time_parse_any(ary[0])?;
        let to = time_parse_any(ary[1])?;
        let (s_from, s_to) = (to_ymdhms_date(from), to_ymdhms_date(to));
        // Go: lib.DayStart(time.Now().AddDate(0, 0, -1)) — the UTC midnight
        // of yesterday's *local* date.
        let yesterday = Local::now() - devstatscode::chrono::Duration::days(1);
        let max_dt = day_start(devstatscode::time::wall_as_utc(&yesterday));
        if from > max_dt || to > max_dt || from >= to {
            return Err(ApiError::new(format!(
                "from ({}) and to ({}) dates must not be after {}, from date must be before to date",
                s_from,
                s_to,
                gofmt::time(max_dt)
            )));
        }
        return Ok((format!("range:{},{}", s_from, s_to), true));
    }
    let mut rows = query_sql_log_err(
        c,
        ctx,
        "select quick_ranges_suffix from tquick_ranges where quick_ranges_name = $1",
        &[SqlArg::from(period_name)],
    )?;
    let mut period_value = String::new();
    while rows.next() {
        rows.scan(&mut [&mut period_value])?;
    }
    rows.err()?;
    if period_value.is_empty() {
        return Err(ApiError::new(format!(
            "invalid period name: '{}'",
            period_name
        )));
    }
    Ok((period_value, false))
}

// ---------------------------------------------------------------------------
// ensureManualData
// ---------------------------------------------------------------------------

/// Go `ensureManualData`: make sure the `hdev`/`hdev_repos` series for a
/// manual `range:…` period exist, running `calc_metric` (in the background
/// when `bg`) when they do not.
#[allow(clippy::too_many_arguments)]
pub fn ensure_manual_data(
    c: &PgConn,
    ctx: &Ctx,
    project: &str,
    db: &str,
    api_name: &str,
    metric: &str,
    period: &str,
    repos_mode: bool,
    bg: bool,
) -> ApiResult<()> {
    let cfg = || {
        format!(
            "({},{},{},{},{},{})",
            project, db, api_name, metric, period, repos_mode
        )
    };
    let (mut file, mode) = match api_name {
        devstatscode::consts::DEV_ACT_CNT | devstatscode::consts::DEV_ACT_CNT_COMP => {
            let mut file = "project_developer_stats".to_string();
            if metric == "approves" {
                if db != GHA {
                    return Err(ApiError::new(format!(
                        "ensureManualData: approves mode only allowed for kubernetes project {}",
                        cfg()
                    )));
                }
                file = "hist_approvers".to_string();
            }
            if metric == "reviews" {
                if db != GHA {
                    return Err(ApiError::new(format!(
                        "ensureManualData: reviews mode only allowed for kubernetes project {}",
                        cfg()
                    )));
                }
                file = "hist_reviewers".to_string();
            }
            (file, "multi_row_single_column")
        }
        _ => {
            return Err(ApiError::new(format!(
                "ensureManualData: unknown API configuration {}",
                cfg()
            )))
        }
    };
    if file.is_empty() {
        return Err(ApiError::new(format!(
            "ensureManualData: cannot find manual SQL file for configuration {}",
            cfg()
        )));
    }
    if repos_mode {
        file.push_str("_repos");
    }
    let (extra, query) = match file.as_str() {
        "hist_reviewers" | "hist_approvers" | "project_developer_stats" => (
            "hist,merge_series:hdev",
            "select 1 from shdev where period = $1 and series like $2 limit 1",
        ),
        "hist_reviewers_repos" | "hist_approvers_repos" | "project_developer_stats_repos" => (
            "hist,merge_series:hdev_repos",
            "select 1 from shdev_repos where period = $1 and series like $2 limit 1",
        ),
        _ => {
            return Err(ApiError::new(format!(
                "ensureManualData: don't know how to check for existing data for configuration {}",
                cfg()
            )))
        }
    };
    file.push_str(".sql");
    let mut rows = query_sql_log_err(
        c,
        ctx,
        query,
        &[
            SqlArg::from(period),
            SqlArg::from(format!("hdev_{}%", metric)),
        ],
    )?;
    let mut dummy = 0i64;
    if rows.next() {
        rows.scan(&mut [&mut dummy])?;
    }
    drop(rows);
    if dummy != 0 {
        return Ok(());
    }
    let dt_now = to_ymdh_date(Local::now());
    let key = if bg {
        format!("{}{}{}{}{}", project, file, mode, period, extra)
    } else {
        String::new()
    };
    let cmd: Vec<String> = vec![
        "calc_metric".to_string(),
        mode.to_string(),
        format!("/etc/gha2db/metrics/{}/{}", project, file),
        dt_now.clone(),
        dt_now,
        period.to_string(),
        extra.to_string(),
    ];
    let mut env = BTreeMap::new();
    env.insert("PG_DB".to_string(), db.to_string());
    env.insert("GHA2DB_PROJECT".to_string(), project.to_string());
    let calc = move |ctx: &Ctx| -> Result<(), String> {
        let data = exec_command(ctx, &cmd, &env).map_err(|e| e.to_string())?;
        printf!("Calculated manually:\n");
        printf!("{}", data);
        Ok(())
    };
    if bg {
        {
            let st = state()
                .bg
                .read()
                .map_err(|_| ApiError::new("bg lock poisoned"))?;
            if st.running.contains(&key) {
                return Err(ApiError::new(format!(
                    "configuration already running in background {}",
                    cfg()
                )));
            }
            if st.num >= MAX_BG {
                return Err(ApiError::new(format!(
                    "too many background calculations: {}",
                    st.num
                )));
            }
        }
        if let Ok(mut st) = state().bg.write() {
            st.num += 1;
            st.running.insert(key.clone());
        }
        let ctx = ctx.clone();
        thread::spawn(move || {
            // Go: the error of a background run is not reported anywhere.
            let _ = calc(&ctx);
            if let Ok(mut st) = state().bg.write() {
                st.num -= 1;
                st.running.remove(&key);
            }
        });
        Ok(())
    } else {
        calc(ctx).map_err(ApiError::new)
    }
}

// ---------------------------------------------------------------------------
// Tag lookups
// ---------------------------------------------------------------------------

fn single_value_lookup(c: &PgConn, ctx: &Ctx, query: &str, arg: &str) -> ApiResult<String> {
    let mut rows = query_sql_log_err(c, ctx, query, &[SqlArg::from(arg)])?;
    let mut value = String::new();
    while rows.next() {
        rows.scan(&mut [&mut value])?;
    }
    rows.err()?;
    Ok(value)
}

/// Go `allRepoGroupNameToValue`.
pub fn all_repo_group_name_to_value(c: &PgConn, ctx: &Ctx, name: &str) -> ApiResult<String> {
    let value = single_value_lookup(
        c,
        ctx,
        "select all_repo_group_value from tall_repo_groups where all_repo_group_name = $1",
        name,
    )?;
    if value.is_empty() {
        return Err(ApiError::new(format!(
            "invalid repository_group name: '{}'",
            name
        )));
    }
    Ok(value)
}

/// Go `repoNameToValue`.
pub fn repo_name_to_value(c: &PgConn, ctx: &Ctx, name: &str) -> ApiResult<String> {
    let value = single_value_lookup(
        c,
        ctx,
        "select repo_value from trepos where repo_name = $1",
        name,
    )?;
    if value.is_empty() {
        return Err(ApiError::new(format!(
            "invalid repository name: '{}'",
            name
        )));
    }
    Ok(value)
}

/// Go `allCountryNameToValue` (`all` is only valid for the `All` name).
pub fn all_country_name_to_value(c: &PgConn, ctx: &Ctx, name: &str) -> ApiResult<String> {
    let value = single_value_lookup(
        c,
        ctx,
        "select sub.value from (select country_value as value, 0 as ord from tcountries \
         where country_name = $1 union select 'all', 1 as ord) sub order by sub.ord limit 1",
        name,
    )?;
    if value.is_empty() || (value == "all" && name != ALL_CAP) {
        return Err(ApiError::new(format!("invalid country name: '{}'", name)));
    }
    Ok(value)
}

/// Go `getStringTags`: `select <col> from <tag>` (nil when there are no rows).
pub fn get_string_tags(c: &PgConn, ctx: &Ctx, tag: &str, col: &str) -> ApiResult<NilVec<String>> {
    if col.is_empty() || tag.is_empty() {
        return Err(ApiError::new(format!(
            "tag and col must both be non-empty, got ({}, {})",
            tag, col
        )));
    }
    let mut rows = query_sql_log_err(c, ctx, &format!("select {} from {}", col, tag), &[])?;
    let mut values = Vec::new();
    let mut value = String::new();
    while rows.next() {
        rows.scan(&mut [&mut value])?;
        values.push(value.clone());
    }
    rows.err()?;
    Ok(nil_vec(values))
}

// ---------------------------------------------------------------------------
// Metric / period name maps
// ---------------------------------------------------------------------------

fn map_of(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Go `metricNameToValueMap` followed by the callers' `for _, v := range m {
/// m[v] = v }` (values are accepted as names too).
pub fn metric_name_to_value_map(db: &str, api_name: &str) -> ApiResult<BTreeMap<String, String>> {
    use devstatscode::consts::{
        COMPANIES_TABLE, COM_STATS_REPO_GRP, DEV_ACT_CNT, DEV_ACT_CNT_COMP,
    };
    let mut m = match api_name {
        COMPANIES_TABLE => map_of(&[
            ("Commenters", "commenters"),
            ("Comments", "comments"),
            ("Commit commenters", "commitcommenters"),
            ("Commits", "commits"),
            ("Committers", "committers"),
            ("Documentation commits", "documentationcommits"),
            ("Documentation committers", "documentationcommitters"),
            ("Pushers", "pushers"),
            ("GitHub Events", "events"),
            ("Forkers", "forkers"),
            ("Issue commenters", "issuecommenters"),
            ("Issuers", "issues"),
            ("PR authors", "prcreators"),
            ("PR reviews", "prreviewers"),
            ("Pull requests", "prs"),
            ("Contributing in repositories", "repositories"),
            ("Contributors", "contributors"),
            ("Contributions", "contributions"),
            ("Watchers", "watchers"),
        ]),
        COM_STATS_REPO_GRP => map_of(&[
            ("All activity", "activity"),
            ("Active authors", "authors"),
            ("Issues created", "issues"),
            ("Pull requests created", "prs"),
            ("Commits", "commits"),
            ("Committers", "committers"),
            ("Pushers", "pushers"),
            ("Pushes", "pushes"),
            ("Contributions", "contributions"),
            ("Contributors", "contributors"),
            ("Comments", "comments"),
        ]),
        DEV_ACT_CNT | DEV_ACT_CNT_COMP => {
            let mut m = map_of(&[
                ("Comments", "comments"),
                ("Commit comments", "commit_comments"),
                ("Commits", "commits"),
                ("GitHub Events", "events"),
                ("GitHub pushes", "pushes"),
                ("Issue comments", "issue_comments"),
                ("Issues", "issues"),
                ("PRs", "prs"),
                ("Merged PRs", "merged_prs"),
                ("Review comments", "review_comments"),
                ("Contributions", "contributions"),
                ("Active repositories", "active_repos"),
            ]);
            if db == GHA {
                m.insert("Approves".to_string(), "approves".to_string());
                m.insert("Reviews".to_string(), "reviews".to_string());
            }
            m
        }
        _ => {
            return Err(ApiError::new(format!(
                "metricNameToValueMap: unknown db/api pair: '{}'/'{}'",
                db, api_name
            )))
        }
    };
    let values: Vec<String> = m.values().cloned().collect();
    for v in values {
        m.insert(v.clone(), v);
    }
    Ok(m)
}

/// Go `periodNameToValueMap` (+ the values accepted as names).
pub fn period_name_to_value_map(db: &str, api_name: &str) -> ApiResult<BTreeMap<String, String>> {
    use devstatscode::consts::{COM_CONTRIB_REPO_GRP, COM_STATS_REPO_GRP};
    let mut m = match api_name {
        COM_CONTRIB_REPO_GRP => map_of(&[
            ("7 Days MA", "d7"),
            ("28 Days MA", "d28"),
            ("Week", "w"),
            ("Month", "m"),
            ("Quarter", "q"),
        ]),
        COM_STATS_REPO_GRP => map_of(&[
            ("Day", "d"),
            ("7 Days MA", "d7"),
            ("Week", "w"),
            ("Month", "m"),
            ("Quarter", "q"),
            ("Year", "y"),
        ]),
        _ => {
            return Err(ApiError::new(format!(
                "periodNameToValueMap: unknown db/api pair: '{}'/'{}'",
                db, api_name
            )))
        }
    };
    let values: Vec<String> = m.values().cloned().collect();
    for v in values {
        m.insert(v.clone(), v);
    }
    Ok(m)
}

/// Go `getContextAndDB`: a fresh context pointed at the read-only
/// credentials (`PG_HOST_RO`, `PG_USER_RO`, `PG_PASS_RO`) and database `db`.
pub fn get_context_and_db(db: &str) -> ApiResult<(Ctx, PgConn)> {
    let mut ctx = Ctx::default();
    ctx.init();
    ctx.pg_host = std::env::var("PG_HOST_RO").unwrap_or_default();
    ctx.pg_user = std::env::var("PG_USER_RO").unwrap_or_default();
    ctx.pg_pass = std::env::var("PG_PASS_RO").unwrap_or_default();
    ctx.pg_db = db.to_string();
    ctx.exec_fatal = false;
    ctx.exec_output = true;
    let c = devstatscode::pg::pg_conn_err(&ctx)?;
    Ok((ctx, c))
}

/// Scan `(time, value)` rows into parallel vectors (`f64` values).
pub fn scan_time_float(rows: &mut Rows<'_>) -> ApiResult<(Vec<String>, Vec<f64>)> {
    let mut times = Vec::new();
    let mut values = Vec::new();
    let mut t = zero_time();
    let mut v = 0f64;
    while rows.next() {
        rows.scan(&mut [&mut t, &mut v])?;
        times.push(json_time(&t));
        values.push(v);
    }
    rows.err()?;
    Ok((times, values))
}

/// Scan `(time, value)` rows into parallel vectors (`int64` values).
pub fn scan_time_int(rows: &mut Rows<'_>) -> ApiResult<(Vec<String>, Vec<i64>)> {
    let mut times = Vec::new();
    let mut values = Vec::new();
    let mut t = zero_time();
    let mut v = 0i64;
    while rows.next() {
        rows.scan(&mut [&mut t, &mut v])?;
        times.push(json_time(&t));
        values.push(v);
    }
    rows.err()?;
    Ok((times, values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn payload(v: Value) -> Payload {
        match v {
            Value::Object(m) => Some(m),
            _ => None,
        }
    }

    #[test]
    fn time_parse_any_layouts() {
        let ok = |s: &str, exp: &str| {
            let t = time_parse_any(s).unwrap_or_else(|e| panic!("{s}: {}", e.msg));
            assert_eq!(t.format("%Y-%m-%d %H:%M:%S%.f").to_string(), exp, "{s}");
        };
        ok("2020-03-01T15:04:05Z", "2020-03-01 15:04:05");
        ok("2020-03-01 15:04:05", "2020-03-01 15:04:05");
        ok("2020-03-01 15:04", "2020-03-01 15:04:00");
        ok("2020-03-01 15", "2020-03-01 15:00:00");
        ok("2020-03-01 1", "2020-03-01 01:00:00");
        ok("2020-03-01", "2020-03-01 00:00:00");
        ok("2020-03", "2020-03-01 00:00:00");
        ok("2020", "2020-01-01 00:00:00");
        // Go accepts a fractional second after `05` even without it in the layout
        ok("2020-03-01 15:04:05.5", "2020-03-01 15:04:05.500");
        ok("2020-03-01 15:04:05,25", "2020-03-01 15:04:05.250");
        for bad in [
            "bad",
            "",
            "20200301",
            "2020-3-01",
            "2020-03-01 15:4",
            "2020-03-01 15:04:05 ",
            " 2020",
            "2020-13-01",
            "2020-02-30",
            "2020-03-01T15:04:05",
            "2020-03-01 15:04:05Z",
            "2020-03-01 123",
            "202",
            "20201",
        ] {
            let err = time_parse_any(bad)
                .err()
                .unwrap_or_else(|| panic!("{bad} parsed"));
            assert_eq!(err.msg, format!("cannot parse datetime: '{bad}'"));
        }
    }

    #[test]
    fn json_time_is_rfc3339_nano() {
        let t = |s: &str| DateTime::parse_from_rfc3339(s).unwrap();
        assert_eq!(
            json_time(&t("2020-01-02T03:04:05Z")),
            "2020-01-02T03:04:05Z"
        );
        assert_eq!(
            json_time(&t("2020-01-02T03:04:05.120000Z")),
            "2020-01-02T03:04:05.12Z"
        );
        assert_eq!(
            json_time(&t("2020-01-02T03:04:05.000000001Z")),
            "2020-01-02T03:04:05.000000001Z"
        );
        assert_eq!(
            json_time(&t("2020-01-02T03:04:05+02:30")),
            "2020-01-02T03:04:05+02:30"
        );
        assert_eq!(
            json_time(&t("2020-01-02T03:04:05-07:00")),
            "2020-01-02T03:04:05-07:00"
        );
        assert_eq!(json_time(&zero_time()), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn nil_slices_encode_as_null() {
        let pl = CompaniesTablePayload {
            project: "p".into(),
            db_name: "d".into(),
            range: "Last day".into(),
            metric: "Commits".into(),
            rank: nil_vec(vec![]),
            company: nil_vec(vec!["A <b> & \"c\"".to_string()]),
            number: nil_vec(vec![1.0, 2.5, 1e21]),
        };
        assert_eq!(
            String::from_utf8(encode(&pl)).unwrap(),
            "{\"project\":\"p\",\"db_name\":\"d\",\"range\":\"Last day\",\"metric\":\"Commits\",\"rank\":null,\"company\":[\"A \\u003cb\\u003e \\u0026 \\\"c\\\"\"],\"number\":[1,2.5,1e+21]}\n"
        );
    }

    #[test]
    fn payload_string_is_go_map_format() {
        assert_eq!(payload_string(&None), "map[]");
        assert_eq!(payload_string(&payload(json!({}))), "map[]");
        assert_eq!(
            payload_string(&payload(
                json!({"z":"1","a":1000.0,"m":[1,"x",null],"n":null,"b":true})
            )),
            "map[a:1000 b:true m:[1 x <nil>] n:<nil> z:1]"
        );
    }

    #[test]
    fn go_type_names() {
        assert_eq!(go_type_name(&json!(null)), "<nil>");
        assert_eq!(go_type_name(&json!(true)), "bool");
        assert_eq!(go_type_name(&json!(1)), "float64");
        assert_eq!(go_type_name(&json!(1.5)), "float64");
        assert_eq!(go_type_name(&json!("s")), "string");
        assert_eq!(go_type_name(&json!([1])), "[]interface {}");
        assert_eq!(go_type_name(&json!({"a":1})), "map[string]interface {}");
    }

    #[test]
    fn string_param_errors() {
        let p = payload(json!({"s":"v","n":1,"e":"","o":{"a":1}}));
        assert_eq!(get_payload_string_param("s", &p, false).unwrap(), "v");
        assert_eq!(get_payload_string_param("e", &p, false).unwrap(), "");
        assert_eq!(get_payload_string_param("missing", &p, true).unwrap(), "");
        assert_eq!(
            get_payload_string_param("missing", &None, true).unwrap(),
            ""
        );
        let e = get_payload_string_param("missing", &p, false).unwrap_err();
        assert_eq!(
            e.msg,
            "missing 'missing' field in 'payload' section (optional false)"
        );
        assert!(!e.logged);
        assert_eq!(
            get_payload_string_param("n", &p, true).unwrap_err().msg,
            "'payload' 'n' field '1'/float64 is not a string (optional true)"
        );
        assert_eq!(
            get_payload_string_param("o", &p, false).unwrap_err().msg,
            "'payload' 'o' field 'map[a:1]'/map[string]interface {} is not a string (optional false)"
        );
        assert_eq!(
            get_payload_string_param("x", &None, false).unwrap_err().msg,
            "missing 'x' field in 'payload' section (optional false)"
        );
    }

    #[test]
    fn string_array_param_errors() {
        let p = payload(json!({"a":["x","y"],"e":[],"s":"All","m":["All",1,null]}));
        assert_eq!(
            get_payload_string_array_param("a", &p, false, false).unwrap(),
            vec!["x", "y"]
        );
        assert!(get_payload_string_array_param("e", &p, false, true)
            .unwrap()
            .is_empty());
        assert!(get_payload_string_array_param("missing", &p, true, false)
            .unwrap()
            .is_empty());
        assert_eq!(
            get_payload_string_array_param("missing", &p, false, false)
                .unwrap_err()
                .msg,
            "missing 'missing' field in 'payload' section (optional false, allow empty false)"
        );
        assert_eq!(
            get_payload_string_array_param("e", &p, false, false)
                .unwrap_err()
                .msg,
            "'payload' 'e' field '[]' cannot be empty (optional false, allow empty false)"
        );
        assert_eq!(
            get_payload_string_array_param("s", &p, false, false)
                .unwrap_err()
                .msg,
            "'payload' 's' field 'All'/string is not an array (optional false, allow empty false)"
        );
        // the last offending item is reported
        assert_eq!(
            get_payload_string_array_param("m", &p, false, false)
                .unwrap_err()
                .msg,
            "'payload' 'm' field '[All 1 <nil>]' #3 item '<nil>'/<nil> is not a string (optional false, allow empty false)"
        );
    }

    #[test]
    fn string_params_in_given_order() {
        let p = payload(json!({"a":"1","b":"2"}));
        let m = string_params(&["b", "a"], &p, false).unwrap();
        assert_eq!(m.get("a").unwrap(), "1");
        assert_eq!(m.get("b").unwrap(), "2");
        assert_eq!(
            string_params(&["a", "c", "d"], &p, false).unwrap_err().msg,
            "missing 'c' field in 'payload' section (optional false)"
        );
    }

    #[test]
    fn metric_and_period_maps() {
        let m = metric_name_to_value_map("gha", "DevActCnt").unwrap();
        assert_eq!(m.get("Approves").unwrap(), "approves");
        assert_eq!(m.get("Reviews").unwrap(), "reviews");
        assert_eq!(m.get("Commits").unwrap(), "commits");
        assert_eq!(m.get("Active repositories").unwrap(), "active_repos");
        // values are accepted as names too (the callers' `m[v] = v` loop)
        assert_eq!(m.get("commits").unwrap(), "commits");
        assert_eq!(m.len(), 28);
        let m = metric_name_to_value_map("other", "DevActCntComp").unwrap();
        assert!(!m.contains_key("Approves"));
        assert!(!m.contains_key("approves"));
        assert_eq!(m.len(), 24);
        assert_eq!(
            metric_name_to_value_map("gha", "CompaniesTable")
                .unwrap()
                .get("Contributing in repositories")
                .unwrap(),
            "repositories"
        );
        assert_eq!(
            metric_name_to_value_map("gha", "ComStatsRepoGrp")
                .unwrap()
                .get("All activity")
                .unwrap(),
            "activity"
        );
        assert_eq!(
            metric_name_to_value_map("db", "Nope").unwrap_err().msg,
            "metricNameToValueMap: unknown db/api pair: 'db'/'Nope'"
        );
        let p = period_name_to_value_map("gha", "ComContribRepoGrp").unwrap();
        assert_eq!(p.get("7 Days MA").unwrap(), "d7");
        assert_eq!(p.get("d7").unwrap(), "d7");
        assert_eq!(p.len(), 10);
        let p = period_name_to_value_map("gha", "ComStatsRepoGrp").unwrap();
        assert_eq!(p.get("Year").unwrap(), "y");
        assert_eq!(p.len(), 12);
        assert_eq!(
            period_name_to_value_map("db", "DevActCnt").unwrap_err().msg,
            "periodNameToValueMap: unknown db/api pair: 'db'/'DevActCnt'"
        );
    }

    #[test]
    fn site_stats_go_string() {
        let s = SiteStatsPayload {
            project: "p".into(),
            db_name: "d".into(),
            contributors: 1,
            contributions: 2,
            boc: 3,
            committers: 4,
            commits: 5,
            events: 6,
            forkers: 7,
            repositories: 8,
            stargazers: 9,
            countries: 10,
            companies: 11,
        };
        assert_eq!(
            s.go_string(),
            "{Project:p DB:d Contributors:1 Contributions:2 BOC:3 Committers:4 Commits:5 Events:6 Forkers:7 Repositories:8 Stargazers:9 Countries:10 Companies:11}"
        );
    }
}
