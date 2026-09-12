//! The 17 API endpoints (Go `api<Name>` functions of api.go).

use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use devstatscode::chrono::Local;
use devstatscode::consts::*;
use devstatscode::http::Response;
use devstatscode::pg::api::{n_array, query_sql_log_err};
use devstatscode::pg::{PgConn, ScanDest, SqlArg};
use devstatscode::{gofmt, printf, Ctx};

use crate::common::*;

/// Go `allAPIs`.
pub const ALL_APIS: [&str; 17] = [
    HEALTH,
    LIST_APIS,
    LIST_PROJECTS,
    REPO_GROUPS,
    RANGES,
    COUNTRIES,
    COMPANIES,
    EVENTS,
    REPOS,
    CUMULATIVE_COUNTS,
    COMPANIES_TABLE,
    COM_CONTRIB_REPO_GRP,
    DEV_ACT_CNT,
    DEV_ACT_CNT_COMP,
    COM_STATS_REPO_GRP,
    SITE_STATS,
    GITHUB_ID_CONTRIBUTIONS,
];

/// Go `returnError`: log the error, answer 400 with `{"error":"…"}`.
pub fn return_error(api_name: &str, w: &mut Response, err: &str) {
    let err_str = if err.starts_with("API '") {
        err.to_string()
    } else {
        format!("API '{}': {}", api_name, err)
    };
    printf!("{}\n", err_str);
    w.status = 400;
    w.body = encode(&ErrorPayload { error: err_str });
}

/// Write the handler result and print the Go deferred exit log line
/// `<API>(exit): project:<p> db:<db> payload: <payload> err:<err>`.
fn finish(
    w: &mut Response,
    api_name: &str,
    project: &str,
    db: &str,
    payload: &Payload,
    res: ApiResult<Vec<u8>>,
) {
    let err = match res {
        Ok(body) => {
            w.status = 200;
            w.body = body;
            "<nil>".to_string()
        }
        Err(e) => {
            return_error(api_name, w, &e.msg);
            if e.logged {
                e.msg
            } else {
                "<nil>".to_string()
            }
        }
    };
    printf!(
        "{}(exit): project:{} db:{} payload: {} err:{}\n",
        api_name,
        project,
        db,
        payload_string(payload),
        err
    );
}

/// `bg` payload flag (Go: any non-empty string value; errors ignored).
fn bg_flag(payload: &Payload) -> bool {
    !get_payload_string_param("bg", payload, true)
        .unwrap_or_default()
        .is_empty()
}

fn to_args(before: &[String], strs: &[String]) -> Vec<SqlArg> {
    before
        .iter()
        .chain(strs.iter())
        .map(|s| SqlArg::from(s.as_str()))
        .collect()
}

// ---------------------------------------------------------------------------
// ListAPIs / ListProjects
// ---------------------------------------------------------------------------

pub fn api_list_apis(w: &mut Response) {
    w.status = 200;
    w.body = encode(&ListApisPayload {
        apis: ALL_APIS.to_vec(),
    });
    printf!("{}(exit)\n", LIST_APIS);
}

pub fn api_list_projects(w: &mut Response) {
    let names = state()
        .projects
        .read()
        .map(|p| p.clone())
        .unwrap_or_default();
    w.status = 200;
    w.body = encode(&ListProjectsPayload { projects: names });
    printf!("{}(exit)\n", LIST_PROJECTS);
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

pub fn api_health(w: &mut Response, payload: &Payload) {
    let api_name = HEALTH;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let (ctx, c) = get_context_and_db(&db)?;
        let mut rows = query_sql_log_err(&c, &ctx, "select count(*) from gha_events", &[])?;
        let mut events = 0i64;
        while rows.next() {
            rows.scan(&mut [&mut events])?;
        }
        rows.err()?;
        Ok(encode(&HealthPayload {
            project: project.clone(),
            db_name: db.clone(),
            events,
        }))
    })();
    finish(w, api_name, &project, &db, payload, res);
}

// ---------------------------------------------------------------------------
// Tag lists: RepoGroups, Companies, Ranges, Countries
// ---------------------------------------------------------------------------

/// A `select <col> from <tag>` list API; `raw_col` is used instead of `col`
/// when the optional `raw` parameter is set (`None`: no `raw` parameter).
struct TagList<'a> {
    api_name: &'a str,
    tag: &'a str,
    col: &'a str,
    raw_col: Option<&'a str>,
}

fn tag_list_api(
    w: &mut Response,
    payload: &Payload,
    t: TagList<'_>,
    build: impl FnOnce(String, String, NilVec<String>) -> Vec<u8>,
) {
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let raw = if t.raw_col.is_some() {
            get_payload_string_param("raw", payload, true)?
        } else {
            String::new()
        };
        let (ctx, c) = get_context_and_db(&db)?;
        let col = match t.raw_col {
            Some(rc) if !raw.is_empty() => rc,
            _ => t.col,
        };
        let values = get_string_tags(&c, &ctx, t.tag, col)?;
        Ok(build(project.clone(), db.clone(), values))
    })();
    finish(w, t.api_name, &project, &db, payload, res);
}

pub fn api_repo_groups(w: &mut Response, payload: &Payload) {
    tag_list_api(
        w,
        payload,
        TagList {
            api_name: REPO_GROUPS,
            tag: "tall_repo_groups",
            col: "all_repo_group_name",
            raw_col: Some("all_repo_group_value"),
        },
        |project, db_name, repo_groups| {
            encode(&RepoGroupsPayload {
                project,
                db_name,
                repo_groups,
            })
        },
    );
}

pub fn api_companies(w: &mut Response, payload: &Payload) {
    tag_list_api(
        w,
        payload,
        TagList {
            api_name: COMPANIES,
            tag: "tcompanies",
            col: "companies_name",
            raw_col: None,
        },
        |project, db_name, companies| {
            encode(&CompaniesPayload {
                project,
                db_name,
                companies,
            })
        },
    );
}

pub fn api_ranges(w: &mut Response, payload: &Payload) {
    tag_list_api(
        w,
        payload,
        TagList {
            api_name: RANGES,
            tag: "tquick_ranges",
            col: "quick_ranges_name",
            raw_col: Some("quick_ranges_suffix"),
        },
        |project, db_name, ranges| {
            encode(&RangesPayload {
                project,
                db_name,
                ranges,
            })
        },
    );
}

pub fn api_countries(w: &mut Response, payload: &Payload) {
    tag_list_api(
        w,
        payload,
        TagList {
            api_name: COUNTRIES,
            tag: "gha_countries",
            col: "name",
            raw_col: Some("code"),
        },
        |project, db_name, countries| {
            encode(&CountriesPayload {
                project,
                db_name,
                countries,
            })
        },
    );
}

// ---------------------------------------------------------------------------
// Repos
// ---------------------------------------------------------------------------

pub fn api_repos(w: &mut Response, payload: &Payload) {
    let api_name = REPOS;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let repository_group_param =
            get_payload_string_array_param("repository_group", payload, false, false)?;
        let (ctx, c) = get_context_and_db(&db)?;
        // TODO: consider swiitching to gha_repo_groups
        let mut query = r#"
    select
      distinct coalesce(case repo_group when '' then 'Not specified' else repo_group end, 'Not specified') as "Repository group",
      name as "Repository"
    from
      gha_repos
    where
      name like '%_/_%'
      and name not like '%/%/%'
  "#
        .to_string();
        let mut rows = if repository_group_param.len() == 1 && repository_group_param[0] == ALL_CAP
        {
            query_sql_log_err(&c, &ctx, &query, &[])?
        } else {
            query.push_str(
                " and coalesce(case repo_group when '' then 'Not specified' else repo_group end, 'Not specified') in ",
            );
            query.push_str(&n_array(repository_group_param.len(), 0));
            query_sql_log_err(&c, &ctx, &query, &to_args(&[], &repository_group_param))?
        };
        let mut repo_groups = Vec::new();
        let mut repos = Vec::new();
        let mut repo_group = String::new();
        let mut repo = String::new();
        while rows.next() {
            rows.scan(&mut [&mut repo_group, &mut repo])?;
            repo_groups.push(repo_group.clone());
            repos.push(repo.clone());
        }
        rows.err()?;
        Ok(encode(&ReposPayload {
            project: project.clone(),
            db_name: db.clone(),
            repo_groups,
            repos,
        }))
    })();
    finish(w, api_name, &project, &db, payload, res);
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

pub fn api_events(w: &mut Response, payload: &Payload) {
    let api_name = EVENTS;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let params = string_params(&["from", "to"], payload, false)?;
        time_parse_any(&params["from"])?;
        time_parse_any(&params["to"])?;
        let (ctx, c) = get_context_and_db(&db)?;
        let query = r#"
  select
    time,
    value
  from
    sevents_h
  where
    time >= $1
    and time < $2
  order by
    time
  "#;
        let mut rows = query_sql_log_err(
            &c,
            &ctx,
            query,
            &[
                SqlArg::from(params["from"].as_str()),
                SqlArg::from(params["to"].as_str()),
            ],
        )?;
        let (timestamps, values) = scan_time_int(&mut rows)?;
        Ok(encode(&EventsPayload {
            project: project.clone(),
            db_name: db.clone(),
            timestamps,
            from: params["from"].clone(),
            to: params["to"].clone(),
            values,
        }))
    })();
    finish(w, api_name, &project, &db, payload, res);
}

// ---------------------------------------------------------------------------
// CumulativeCounts (cached)
// ---------------------------------------------------------------------------

pub fn api_cumulative_counts(w: &mut Response, payload: &Payload) {
    let api_name = CUMULATIVE_COUNTS;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let params = string_params(&["metric"], payload, false)?;
        let metric = params["metric"].to_lowercase();
        if metric != "contributors" && metric != "organizations" {
            return Err(ApiError::new(
                "metric value can only be 'contributors' or 'organizations'",
            ));
        }
        let key = [project.clone(), db.clone(), metric.clone()];
        let key_str = format!("[{} {} {}]", key[0], key[1], key[2]);
        let cached = state()
            .cumulative_counts_cache
            .lock()
            .ok()
            .and_then(|c| c.get(&key).map(|e| (e.dt, e.cumulative_counts.clone())));
        if let Some((dt, cc)) = cached {
            let age = age_seconds(dt);
            if age < CUMULATIVE_COUNTS_CACHE_TTL as f64 {
                printf!(
                    "{}: using cached values for {} (age is {:.0} < {})\n",
                    api_name,
                    key_str,
                    age,
                    CUMULATIVE_COUNTS_CACHE_TTL
                );
                return Ok(encode(&cc));
            }
            printf!(
                "{}: deleting cached values for {} (age is {:.0} >= {})\n",
                api_name,
                key_str,
                age,
                CUMULATIVE_COUNTS_CACHE_TTL
            );
            if let Ok(mut c) = state().cumulative_counts_cache.lock() {
                c.remove(&key);
            }
        }
        let (ctx, c) = get_context_and_db(&db)?;
        let query = r#"
  select
    time,
    value
  from
    scntrs_and_orgs
  where
    series = $1
  order by
    time
  "#;
        let mut rows = query_sql_log_err(&c, &ctx, query, &[SqlArg::from(metric.as_str())])?;
        let (timestamps, values) = scan_time_int(&mut rows)?;
        let epl = CumulativeCountsPayload {
            project: project.clone(),
            metric: params["metric"].clone(),
            db_name: db.clone(),
            timestamps,
            values,
        };
        let body = encode(&epl);
        let n = epl.values.len();
        if let Ok(mut c) = state().cumulative_counts_cache.lock() {
            c.insert(
                key,
                CumulativeCountsCacheEntry {
                    dt: Local::now(),
                    cumulative_counts: epl,
                },
            );
        }
        printf!("{}: stored {} {} cached values\n", api_name, key_str, n);
        Ok(body)
    })();
    finish(w, api_name, &project, &db, payload, res);
}

/// Go `time.Now().Sub(dt).Seconds()`.
fn age_seconds(dt: devstatscode::chrono::DateTime<Local>) -> f64 {
    let d = Local::now().signed_duration_since(dt);
    d.num_nanoseconds()
        .map(|n| n as f64 / 1e9)
        .unwrap_or_else(|| d.num_seconds() as f64)
}

// ---------------------------------------------------------------------------
// CompaniesTable
// ---------------------------------------------------------------------------

pub fn api_companies_table(w: &mut Response, payload: &Payload) {
    let api_name = COMPANIES_TABLE;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let params = string_params(&["range", "metric"], payload, false)?;
        let metric_map = metric_name_to_value_map(&db, api_name)?;
        let Some(metric) = metric_map.get(&params["metric"]) else {
            return Err(ApiError::new(format!(
                "invalid metric value: '{}'",
                params["metric"]
            )));
        };
        let (ctx, c) = get_context_and_db(&db)?;
        let (period, _) = period_name_to_value(&c, &ctx, &params["range"], false)?;
        let series = format!("hcom{}", metric);
        let query = r#"
    select (row_number() over (order by value desc) -1), name, value from shcom where series = $1 and period = $2
	"#;
        let mut rows = query_sql_log_err(
            &c,
            &ctx,
            query,
            &[SqlArg::from(series.as_str()), SqlArg::from(period.as_str())],
        )?;
        let (mut rank, mut company, mut number) = (0i64, String::new(), 0f64);
        let (mut ranks, mut companies, mut numbers) = (Vec::new(), Vec::new(), Vec::new());
        while rows.next() {
            rows.scan(&mut [&mut rank, &mut company, &mut number])?;
            ranks.push(rank);
            companies.push(company.clone());
            numbers.push(number);
        }
        rows.err()?;
        Ok(encode(&CompaniesTablePayload {
            project: project.clone(),
            db_name: db.clone(),
            range: params["range"].clone(),
            metric: params["metric"].clone(),
            rank: nil_vec(ranks),
            company: nil_vec(companies),
            number: nil_vec(numbers),
        }))
    })();
    finish(w, api_name, &project, &db, payload, res);
}

// ---------------------------------------------------------------------------
// ComContribRepoGrp
// ---------------------------------------------------------------------------

pub fn api_com_contrib_repo_grp(w: &mut Response, payload: &Payload) {
    let api_name = COM_CONTRIB_REPO_GRP;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let params = string_params(
            &["from", "to", "period", "repository_group"],
            payload,
            false,
        )?;
        time_parse_any(&params["from"])?;
        time_parse_any(&params["to"])?;
        let period_map = period_name_to_value_map(&db, api_name)?;
        let Some(period) = period_map.get(&params["period"]) else {
            return Err(ApiError::new(format!(
                "invalid period value: '{}'",
                params["period"]
            )));
        };
        let (ctx, c) = get_context_and_db(&db)?;
        let repogroup = all_repo_group_name_to_value(&c, &ctx, &params["repository_group"])?;
        let query = r#"
  select
    time,
    value
  from
    snum_stats
  where
    time >= $1
    and time < $2
    and period = $3
    and series = $4
  order by
    time
	"#;
        let series_comps = format!("nstats{}comps", repogroup);
        let series_devs = format!("nstats{}devs", repogroup);
        let mut rows = query_sql_log_err(
            &c,
            &ctx,
            query,
            &[
                SqlArg::from(params["from"].as_str()),
                SqlArg::from(params["to"].as_str()),
                SqlArg::from(period.as_str()),
                SqlArg::from(series_comps.as_str()),
            ],
        )?;
        let (companies_timestamps, companies) = scan_time_float(&mut rows)?;
        drop(rows);
        let mut rows = query_sql_log_err(
            &c,
            &ctx,
            query,
            &[
                SqlArg::from(params["from"].as_str()),
                SqlArg::from(params["to"].as_str()),
                SqlArg::from(period.as_str()),
                SqlArg::from(series_devs.as_str()),
            ],
        )?;
        let (developers_timestamps, developers) = scan_time_float(&mut rows)?;
        Ok(encode(&ComContribRepoGrpPayload {
            project: project.clone(),
            db_name: db.clone(),
            period: params["period"].clone(),
            repository_group: params["repository_group"].clone(),
            companies: nil_vec(companies),
            developers: nil_vec(developers),
            companies_timestamps: nil_vec(companies_timestamps),
            developers_timestamps: nil_vec(developers_timestamps),
        }))
    })();
    finish(w, api_name, &project, &db, payload, res);
}

// ---------------------------------------------------------------------------
// ComStatsRepoGrp
// ---------------------------------------------------------------------------

enum Cell {
    Time(devstatscode::chrono::DateTime<devstatscode::chrono::FixedOffset>),
    Str(String),
    Float(f64),
}

pub fn api_com_stats_repo_grp(w: &mut Response, payload: &Payload) {
    let api_name = COM_STATS_REPO_GRP;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let params = string_params(
            &["from", "to", "period", "metric", "repository_group"],
            payload,
            false,
        )?;
        time_parse_any(&params["from"])?;
        time_parse_any(&params["to"])?;
        let period_map = period_name_to_value_map(&db, api_name)?;
        let Some(period) = period_map.get(&params["period"]) else {
            return Err(ApiError::new(format!(
                "invalid period value: '{}'",
                params["period"]
            )));
        };
        let metric_map = metric_name_to_value_map(&db, api_name)?;
        let Some(metric) = metric_map.get(&params["metric"]) else {
            return Err(ApiError::new(format!(
                "invalid metric value: '{}'",
                params["metric"]
            )));
        };
        let companies_param = get_payload_string_array_param("companies", payload, false, false)?;
        if companies_param.is_empty() {
            return Err(ApiError::new(
                "you need to specify at least one company, for example 'All'",
            ));
        }
        let (ctx, c) = get_context_and_db(&db)?;
        let repogroup = all_repo_group_name_to_value(&c, &ctx, &params["repository_group"])?;
        let mut query = "select ".to_string();
        if companies_param.len() == 1 && companies_param[0] == ALL_CAP {
            query.push('*');
        } else {
            query.push_str("time, ");
            for company in &companies_param {
                query.push_str(&format!("\"{}\", ", company));
            }
            query.truncate(query.len() - 2);
        }
        query.push_str(
            " from scompany_activity where time >= $1 and time < $2 and period = $3 and series = $4 order by time",
        );
        let series = format!("company{}{}", repogroup, metric);
        let mut rows = query_sql_log_err(
            &c,
            &ctx,
            &query,
            &[
                SqlArg::from(params["from"].as_str()),
                SqlArg::from(params["to"].as_str()),
                SqlArg::from(period.as_str()),
                SqlArg::from(series.as_str()),
            ],
        )?;
        let columns = rows.column_names();
        let mut cells: Vec<Cell> = columns
            .iter()
            .map(|col| match col.as_str() {
                TIME_COL => Cell::Time(zero_time()),
                SERIES_COL | PERIOD_COL => Cell::Str(String::new()),
                _ => Cell::Float(0.0),
            })
            .collect();
        let mut times = Vec::new();
        let mut values: Vec<BTreeMap<String, f64>> = Vec::new();
        while rows.next() {
            {
                let mut dests: Vec<&mut dyn ScanDest> = cells
                    .iter_mut()
                    .map(|c| match c {
                        Cell::Time(t) => t as &mut dyn ScanDest,
                        Cell::Str(s) => s as &mut dyn ScanDest,
                        Cell::Float(f) => f as &mut dyn ScanDest,
                    })
                    .collect();
                rows.scan(&mut dests)?;
            }
            let mut v_map = BTreeMap::new();
            for (column, cell) in columns.iter().zip(cells.iter()) {
                match cell {
                    Cell::Time(t) => times.push(json_time(t)),
                    Cell::Str(_) => {}
                    Cell::Float(f) => {
                        v_map.insert(column.clone(), *f);
                    }
                }
            }
            values.push(v_map);
        }
        rows.err()?;
        Ok(encode(&ComStatsRepoGrpPayload {
            project: project.clone(),
            db_name: db.clone(),
            period: params["period"].clone(),
            metric: params["metric"].clone(),
            repository_group: params["repository_group"].clone(),
            companies: companies_param,
            from: params["from"].clone(),
            to: params["to"].clone(),
            values,
            timestamps: times,
        }))
    })();
    finish(w, api_name, &project, &db, payload, res);
}

// ---------------------------------------------------------------------------
// DevActCnt (+ repository mode)
// ---------------------------------------------------------------------------

/// The `hdev`/`hdev_repos` ranking query of `DevActCnt`.
fn dev_act_cnt_query(table: &str) -> String {
    format!(
        r#"
   select
     sub."Rank",
     sub.name,
     sub.value
   from (
     select row_number() over (order by sum(value) desc) as "Rank",
       split_part(name, '$$$', 1) as name,
       sum(value) as value
     from
       {}
     where
       series = $1
       and period = $2
     group by
       split_part(name, '$$$', 1)
   ) sub
	"#,
        table
    )
}

struct DevActRows {
    ranks: Vec<i64>,
    logins: Vec<String>,
    numbers: Vec<i64>,
}

fn dev_act_cnt_rows(
    c: &PgConn,
    ctx: &Ctx,
    table: &str,
    series: &str,
    period: &str,
    gh_id: &str,
) -> ApiResult<DevActRows> {
    let mut rows = query_sql_log_err(
        c,
        ctx,
        &dev_act_cnt_query(table),
        &[SqlArg::from(series), SqlArg::from(period)],
    )?;
    let (mut rank, mut login, mut number) = (0i64, String::new(), 0i64);
    let mut out = DevActRows {
        ranks: Vec::new(),
        logins: Vec::new(),
        numbers: Vec::new(),
    };
    while rows.next() {
        rows.scan(&mut [&mut rank, &mut login, &mut number])?;
        if !gh_id.is_empty() && login != gh_id {
            continue;
        }
        out.ranks.push(rank);
        out.logins.push(login.clone());
        out.numbers.push(number);
    }
    rows.err()?;
    if out.ranks.is_empty() && !gh_id.is_empty() {
        return Err(ApiError::quiet(format!(
            "github_id '{}' not found in results",
            gh_id
        )));
    }
    Ok(out)
}

fn dev_act_filter(series: &str, period: &str, gh_id: &str) -> String {
    let mut filter = format!("series:{} period:{}", series, period);
    if !gh_id.is_empty() {
        filter.push_str(" github_id:");
        filter.push_str(gh_id);
    }
    filter
}

/// Go `apiDevActCntRepos` (only for the `gha` database).
fn api_dev_act_cnt_repos(
    w: &mut Response,
    api_name: &str,
    project: &str,
    db: &str,
    payload: &Payload,
) {
    let res = (|| -> ApiResult<Vec<u8>> {
        let params = string_params(
            &["range", "metric", "repository", "country", "github_id"],
            payload,
            false,
        )?;
        let bg = bg_flag(payload);
        let metric_map = metric_name_to_value_map(db, api_name)?;
        let Some(metric) = metric_map.get(&params["metric"]) else {
            return Err(ApiError::new(format!(
                "invalid metric value: '{}'",
                params["metric"]
            )));
        };
        let (ctx, c) = get_context_and_db(db)?;
        let repo = repo_name_to_value(&c, &ctx, &params["repository"])?;
        let country = all_country_name_to_value(&c, &ctx, &params["country"])?;
        let (period, manual) = period_name_to_value(&c, &ctx, &params["range"], true)?;
        if manual {
            ensure_manual_data(&c, &ctx, project, db, api_name, metric, &period, true, bg)?;
        }
        let series = format!("hdev_{}{}{}", metric, repo, country);
        let gh_id = params["github_id"].clone();
        let r = dev_act_cnt_rows(&c, &ctx, "shdev_repos", &series, &period, &gh_id)?;
        Ok(encode(&DevActCntReposPayload {
            project: project.to_string(),
            db_name: db.to_string(),
            range: params["range"].clone(),
            metric: params["metric"].clone(),
            repository: params["repository"].clone(),
            country: params["country"].clone(),
            github_id: gh_id.clone(),
            filter: dev_act_filter(&series, &period, &gh_id),
            rank: nil_vec(r.ranks),
            login: nil_vec(r.logins),
            number: nil_vec(r.numbers),
        }))
    })();
    finish(w, api_name, project, db, payload, res);
}

pub fn api_dev_act_cnt(w: &mut Response, payload: &Payload) {
    let api_name = DEV_ACT_CNT;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Option<Vec<u8>>> {
        db = handle_shared_payload(payload, &mut project)?;
        if db == "gha" {
            let repository =
                get_payload_string_param("repository", payload, true).unwrap_or_default();
            if !repository.is_empty() {
                // Repository mode
                api_dev_act_cnt_repos(w, api_name, &project, &db, payload);
                return Ok(None);
            }
        }
        let params = string_params(
            &[
                "range",
                "metric",
                "repository_group",
                "country",
                "github_id",
            ],
            payload,
            false,
        )?;
        let bg = bg_flag(payload);
        let metric_map = metric_name_to_value_map(&db, api_name)?;
        let Some(metric) = metric_map.get(&params["metric"]) else {
            return Err(ApiError::new(format!(
                "invalid metric value: '{}'",
                params["metric"]
            )));
        };
        let (ctx, c) = get_context_and_db(&db)?;
        let repogroup = all_repo_group_name_to_value(&c, &ctx, &params["repository_group"])?;
        let country = all_country_name_to_value(&c, &ctx, &params["country"])?;
        let (period, manual) = period_name_to_value(&c, &ctx, &params["range"], true)?;
        if manual {
            ensure_manual_data(
                &c, &ctx, &project, &db, api_name, metric, &period, false, bg,
            )?;
        }
        let series = format!("hdev_{}{}{}", metric, repogroup, country);
        let gh_id = params["github_id"].clone();
        let r = dev_act_cnt_rows(&c, &ctx, "shdev", &series, &period, &gh_id)?;
        Ok(Some(encode(&DevActCntPayload {
            project: project.clone(),
            db_name: db.clone(),
            range: params["range"].clone(),
            metric: params["metric"].clone(),
            repository_group: params["repository_group"].clone(),
            country: params["country"].clone(),
            github_id: gh_id.clone(),
            filter: dev_act_filter(&series, &period, &gh_id),
            rank: nil_vec(r.ranks),
            login: nil_vec(r.logins),
            number: nil_vec(r.numbers),
        })))
    })();
    match res {
        // Repository mode already answered; Go's outer deferred log still runs.
        Ok(None) => {
            printf!(
                "{}(exit): project:{} db:{} payload: {} err:<nil>\n",
                api_name,
                project,
                db,
                payload_string(payload)
            );
        }
        Ok(Some(body)) => finish(w, api_name, &project, &db, payload, Ok(body)),
        Err(e) => finish(w, api_name, &project, &db, payload, Err(e)),
    }
}

// ---------------------------------------------------------------------------
// DevActCntComp (+ repository mode)
// ---------------------------------------------------------------------------

struct DevActCompRows {
    ranks: Vec<i64>,
    logins: Vec<String>,
    companies: Vec<String>,
    numbers: Vec<i64>,
}

fn dev_act_cnt_comp_rows(
    c: &PgConn,
    ctx: &Ctx,
    table: &str,
    series: &str,
    period: &str,
    companies_param: &[String],
    gh_id: &str,
) -> ApiResult<DevActCompRows> {
    let mut query = format!(
        r#"
  select
    sub."Rank",
    split_part(sub.name, '$$$', 1),
    split_part(sub.name, '$$$', 2),
    sub.value
  from (
    select row_number() over (order by value desc) as "Rank",
      name,
      value
    from
      {}
    where
      series = $1
      and period = $2
  "#,
        table
    );
    let mut rows = if companies_param.len() == 1 && companies_param[0] == ALL_CAP {
        query.push_str(") sub");
        query_sql_log_err(
            c,
            ctx,
            &query,
            &[SqlArg::from(series), SqlArg::from(period)],
        )?
    } else {
        query.push_str(" and split_part(name, '$$$', 2) in ");
        query.push_str(&n_array(companies_param.len(), 2));
        query.push_str(") sub");
        query_sql_log_err(
            c,
            ctx,
            &query,
            &to_args(&[series.to_string(), period.to_string()], companies_param),
        )?
    };
    let (mut rank, mut login, mut company, mut number) = (0i64, String::new(), String::new(), 0i64);
    let mut out = DevActCompRows {
        ranks: Vec::new(),
        logins: Vec::new(),
        companies: Vec::new(),
        numbers: Vec::new(),
    };
    while rows.next() {
        rows.scan(&mut [&mut rank, &mut login, &mut company, &mut number])?;
        if !gh_id.is_empty() && login != gh_id {
            continue;
        }
        out.ranks.push(rank);
        out.logins.push(login.clone());
        out.companies.push(company.clone());
        out.numbers.push(number);
    }
    rows.err()?;
    if out.ranks.is_empty() && !gh_id.is_empty() {
        return Err(ApiError::quiet(format!(
            "github_id '{}' not found in results",
            gh_id
        )));
    }
    Ok(out)
}

/// Go `apiDevActCntCompRepos` (only for the `gha` database).
fn api_dev_act_cnt_comp_repos(
    w: &mut Response,
    api_name: &str,
    project: &str,
    db: &str,
    payload: &Payload,
) {
    let res = (|| -> ApiResult<Vec<u8>> {
        let params = string_params(
            &["range", "metric", "repository", "country", "github_id"],
            payload,
            false,
        )?;
        let companies_param = get_payload_string_array_param("companies", payload, false, false)?;
        let bg = bg_flag(payload);
        let metric_map = metric_name_to_value_map(db, api_name)?;
        let Some(metric) = metric_map.get(&params["metric"]) else {
            return Err(ApiError::new(format!(
                "invalid metric value: '{}'",
                params["metric"]
            )));
        };
        let (ctx, c) = get_context_and_db(db)?;
        let repo = repo_name_to_value(&c, &ctx, &params["repository"])?;
        let country = all_country_name_to_value(&c, &ctx, &params["country"])?;
        let (period, manual) = period_name_to_value(&c, &ctx, &params["range"], true)?;
        if companies_param.is_empty() {
            return Err(ApiError::new(
                "you need to specify at least one company, for example 'All'",
            ));
        }
        if manual {
            ensure_manual_data(&c, &ctx, project, db, api_name, metric, &period, true, bg)?;
        }
        let series = format!("hdev_{}{}{}", metric, repo, country);
        let gh_id = params["github_id"].clone();
        let r = dev_act_cnt_comp_rows(
            &c,
            &ctx,
            "shdev_repos",
            &series,
            &period,
            &companies_param,
            &gh_id,
        )?;
        Ok(encode(&DevActCntCompReposPayload {
            project: project.to_string(),
            db_name: db.to_string(),
            range: params["range"].clone(),
            metric: params["metric"].clone(),
            repository: params["repository"].clone(),
            country: params["country"].clone(),
            companies: companies_param,
            github_id: gh_id,
            rank: nil_vec(r.ranks),
            login: nil_vec(r.logins),
            company: nil_vec(r.companies),
            number: nil_vec(r.numbers),
        }))
    })();
    finish(w, api_name, project, db, payload, res);
}

pub fn api_dev_act_cnt_comp(w: &mut Response, payload: &Payload) {
    let api_name = DEV_ACT_CNT_COMP;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Option<Vec<u8>>> {
        db = handle_shared_payload(payload, &mut project)?;
        if db == "gha" {
            let repository =
                get_payload_string_param("repository", payload, true).unwrap_or_default();
            if !repository.is_empty() {
                // Repository mode
                api_dev_act_cnt_comp_repos(w, api_name, &project, &db, payload);
                return Ok(None);
            }
        }
        let params = string_params(
            &[
                "range",
                "metric",
                "repository_group",
                "country",
                "github_id",
            ],
            payload,
            false,
        )?;
        let bg = bg_flag(payload);
        let companies_param = get_payload_string_array_param("companies", payload, false, false)?;
        let metric_map = metric_name_to_value_map(&db, api_name)?;
        let Some(metric) = metric_map.get(&params["metric"]) else {
            return Err(ApiError::new(format!(
                "invalid metric value: '{}'",
                params["metric"]
            )));
        };
        let (ctx, c) = get_context_and_db(&db)?;
        let repogroup = all_repo_group_name_to_value(&c, &ctx, &params["repository_group"])?;
        let country = all_country_name_to_value(&c, &ctx, &params["country"])?;
        let (period, manual) = period_name_to_value(&c, &ctx, &params["range"], true)?;
        if companies_param.is_empty() {
            return Err(ApiError::new(
                "you need to specify at least one company, for example 'All'",
            ));
        }
        if manual {
            ensure_manual_data(
                &c, &ctx, &project, &db, api_name, metric, &period, false, bg,
            )?;
        }
        let series = format!("hdev_{}{}{}", metric, repogroup, country);
        let gh_id = params["github_id"].clone();
        let r = dev_act_cnt_comp_rows(
            &c,
            &ctx,
            "shdev",
            &series,
            &period,
            &companies_param,
            &gh_id,
        )?;
        Ok(Some(encode(&DevActCntCompPayload {
            project: project.clone(),
            db_name: db.clone(),
            range: params["range"].clone(),
            metric: params["metric"].clone(),
            repository_group: params["repository_group"].clone(),
            country: params["country"].clone(),
            companies: companies_param,
            github_id: gh_id,
            rank: nil_vec(r.ranks),
            login: nil_vec(r.logins),
            company: nil_vec(r.companies),
            number: nil_vec(r.numbers),
        })))
    })();
    match res {
        Ok(None) => {
            printf!(
                "{}(exit): project:{} db:{} payload: {} err:<nil>\n",
                api_name,
                project,
                db,
                payload_string(payload)
            );
        }
        Ok(Some(body)) => finish(w, api_name, &project, &db, payload, Ok(body)),
        Err(e) => finish(w, api_name, &project, &db, payload, Err(e)),
    }
}

// ---------------------------------------------------------------------------
// GithubIDContributions (cached, `allprj` database)
// ---------------------------------------------------------------------------

const GH_CONTRIBUTIONS_QUERY: &str = r#"
  select
    count(distinct s.event_id) as contributions
  from (
    select
      event_id
    from
      gha_commits
    where
      (lower(dup_actor_login) = $1 or lower(dup_author_login) = $1 or lower(dup_committer_login) = $1)
    union select
      event_id
    from
      gha_issues
    where
      (lower(dup_actor_login) = $1 or lower(dup_user_login) = $1)
    union select
      event_id
    from
      gha_pull_requests
    where
      (lower(dup_actor_login) = $1 or lower(dup_user_login) = $1 or lower(dupn_merged_by_login) = $1)
    union select
      event_id
    from
      gha_commits_roles
    where
      lower(actor_login) = $1
    union select
      id as event_id
    from
      gha_events
    where
      lower(dup_actor_login) = $1
      and type in (
        'PushEvent', 'PullRequestEvent', 'IssuesEvent', 'PullRequestReviewEvent',
        'CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent'
      )
    ) s
  "#;

const GH_PRS_QUERY: &str = r#"
  select
    count(distinct (s.number, s.dup_repo_id)) as prs
  from (
    select
      number, dup_repo_id
    from
      gha_issues
    where
      is_pull_request = true
      and (lower(dup_actor_login) = $1 or lower(dup_user_login) = $1)
    union select
      number, dup_repo_id
    from
      gha_pull_requests
    where
      (lower(dup_actor_login) = $1 or lower(dup_user_login) = $1 or lower(dupn_merged_by_login) = $1)
    ) s
  "#;

const GH_ISSUES_QUERY: &str = r#"
  select
    count(distinct id) as issues
  from
    gha_issues
  where
    is_pull_request = false
    and (lower(dup_actor_login) = $1 or lower(dup_user_login) = $1)
  "#;

/// Run `query` with the single `arg`, scanning every row into an `int`.
fn count_query(c: &PgConn, ctx: &Ctx, query: &str, arg: &str) -> Result<i64, String> {
    let mut rows =
        query_sql_log_err(c, ctx, query, &[SqlArg::from(arg)]).map_err(|e| e.to_string())?;
    let mut dest = 0i64;
    while rows.next() {
        rows.scan(&mut [&mut dest]).map_err(|e| e.to_string())?;
    }
    rows.err().map_err(|e| e.to_string())?;
    Ok(dest)
}

pub fn api_github_id_contributions(w: &mut Response, payload: &Payload) {
    let api_name = GITHUB_ID_CONTRIBUTIONS;
    let (project, db) = ("all".to_string(), "allprj".to_string());
    let res = (|| -> ApiResult<Vec<u8>> {
        let params = string_params(&["github_id"], payload, false)?;
        let gh_id = params["github_id"].clone();
        if gh_id.is_empty() {
            return Err(ApiError::quiet("github_id parameter must be set"));
        }
        let gh_id = gh_id.to_lowercase();
        // Caching: start
        let key = gh_id.clone();
        let cached = state()
            .github_id_contributions_cache
            .lock()
            .ok()
            .and_then(|c| c.get(&key).map(|e| (e.dt, e.dt_str.clone(), e.stats)));
        if let Some((dt, dt_str, stats)) = cached {
            let age = age_seconds(dt);
            let data_str = format!("{{dt:{} stats:{}}}", dt_str, gofmt::slice(&stats));
            if age < GITHUB_ID_CONTRIBUTIONS_CACHE_TTL as f64 {
                printf!(
                    "{}: using cached value for {}: {} (age is {:.0} < {})\n",
                    api_name,
                    key,
                    data_str,
                    age,
                    GITHUB_ID_CONTRIBUTIONS_CACHE_TTL
                );
                return Ok(encode(&GithubIdContributionsResponse {
                    contributions: stats[0],
                    issues: stats[1],
                    prs: stats[2],
                }));
            }
            printf!(
                "{}: deleting cached values for {}: {} (age is {:.0} >= {})\n",
                api_name,
                key,
                data_str,
                age,
                GITHUB_ID_CONTRIBUTIONS_CACHE_TTL
            );
            if let Ok(mut c) = state().github_id_contributions_cache.lock() {
                c.remove(&key);
            }
        }
        // Caching end
        let (ctx, c) = get_context_and_db(&db)?;
        let ctx = Arc::new(ctx);
        let c = Arc::new(c);

        // Get contributions, issues and PRs in parallel
        let (tx, rx) = mpsc::channel::<(usize, Result<i64, String>)>();
        for (i, query) in [GH_CONTRIBUTIONS_QUERY, GH_PRS_QUERY, GH_ISSUES_QUERY]
            .into_iter()
            .enumerate()
        {
            let (tx, ctx, c, gh_id) = (tx.clone(), Arc::clone(&ctx), Arc::clone(&c), gh_id.clone());
            thread::spawn(move || {
                let r = count_query(&c, &ctx, query, &gh_id);
                let _ = tx.send((i, r));
            });
        }
        drop(tx);
        let mut stats = [0i64; 3];
        let mut first_error: Option<String> = None;
        for (i, r) in rx {
            match r {
                Ok(v) => {
                    // index 0: contributions, 1: prs, 2: issues
                    match i {
                        0 => stats[0] = v,
                        1 => stats[2] = v,
                        _ => stats[1] = v,
                    }
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_error {
            return Err(ApiError::quiet(e));
        }
        // final results
        let body = encode(&GithubIdContributionsResponse {
            contributions: stats[0],
            issues: stats[1],
            prs: stats[2],
        });
        // Write to cache: starts
        if let Ok(mut cache) = state().github_id_contributions_cache.lock() {
            cache.insert(
                key.clone(),
                GithubIDContributionsCacheEntry {
                    dt: Local::now(),
                    dt_str: gofmt::time_now(),
                    stats,
                },
            );
        }
        printf!(
            "{}: written value to cache for {}: {}\n",
            api_name,
            key,
            gofmt::slice(&stats)
        );
        // Write to cache: ends
        Ok(body)
    })();
    finish(w, api_name, &project, &db, payload, res);
}

// ---------------------------------------------------------------------------
// SiteStats (cached, 4 parallel queries)
// ---------------------------------------------------------------------------

const SITE_STATS_PSTAT_QUERY: &str = r#"
  select
    name,
    value
  from
    spstat
  where
    series = 'pstatall'
    and period = 'y100'
    and name in (
      'Contributors', 'Contributions', 'Code committers',
      'Commits', 'Events', 'Forkers',
      'Repositories', 'Stargazers'
    )
  "#;

const SITE_STATS_BOC_QUERY: &str = r#"
  select
    sum(rl.lang_loc)
  from
    gha_repos r,
    gha_repos_langs rl
  where
    r.name = rl.repo_name
    and (r.name, r.id) = (
      select i.name,
        i.id
      from
        gha_repos i
      where
        i.alias = r.alias
        and i.name like '%_/_%'
        and i.name not like '%/%/%'
      limit 1
    )
  "#;

const SITE_STATS_COUNTRIES_QUERY: &str = r#"
  select
    count(distinct sub.country_id) as num_countries
  from (
    select
      a.country_id
    from
      gha_events e,
      gha_actors a
    where
      e.actor_id = a.id
      and e.type in (
        'PushEvent', 'PullRequestEvent', 'IssuesEvent', 'PullRequestReviewEvent',
        'CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent'
      )
    union select
      a.country_id
    from
      gha_actors a,
      gha_commits c
    where
      (
        c.author_id = a.id
        or c.committer_id = a.id
      )
    union select
      a.country_id
    from
      gha_actors a,
      gha_commits_roles cr
    where
      cr.actor_id = a.id
      and cr.role = 'Co-authored-by'
  ) sub
  "#;

const SITE_STATS_COMPANIES_QUERY: &str = r#"
  select
    count(distinct sub.company_name) as num_companis
  from (
    select
      af.company_name
    from
      gha_events e,
      gha_actors_affiliations af
    where
      e.actor_id = af.actor_id
      and af.dt_from <= e.created_at
      and af.dt_to > e.created_at
      and af.company_name not in ('Independent', 'Unknown', 'NotFound', '')
      and e.type in (
        'PushEvent', 'PullRequestEvent', 'IssuesEvent', 'PullRequestReviewEvent',
        'CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent'
      )
    union select
      af.company_name
    from
      gha_actors_affiliations af,
      gha_commits c
    where
      (
        c.author_id = af.actor_id
        or c.committer_id = af.actor_id
      )
      and af.dt_from <= c.dup_created_at
      and af.dt_to > c.dup_created_at
      and af.company_name not in ('Independent', 'Unknown', 'NotFound', '')
    union select
      af.company_name
    from
      gha_actors_affiliations af,
      gha_commits_roles cr
    where
      cr.actor_id = af.actor_id
      and cr.role = 'Co-authored-by'
      and af.dt_from <= cr.dup_created_at
      and af.dt_to > cr.dup_created_at
      and af.company_name not in ('Independent', 'Unknown', 'NotFound', '')
  ) sub
  "#;

/// One `float64` aggregate scanned into an `int64` field of the payload.
fn site_stats_single(
    c: &PgConn,
    ctx: &Ctx,
    query: &str,
    sspl: &Mutex<SiteStatsPayload>,
    set: fn(&mut SiteStatsPayload, i64),
) -> Result<(), String> {
    let mut rows = query_sql_log_err(c, ctx, query, &[]).map_err(|e| e.to_string())?;
    let mut value = 0f64;
    while rows.next() {
        rows.scan(&mut [&mut value]).map_err(|e| e.to_string())?;
        if let Ok(mut s) = sspl.lock() {
            set(&mut s, value as i64);
        }
    }
    rows.err().map_err(|e| e.to_string())
}

fn site_stats_pstat(c: &PgConn, ctx: &Ctx, sspl: &Mutex<SiteStatsPayload>) -> Result<(), String> {
    let mut rows =
        query_sql_log_err(c, ctx, SITE_STATS_PSTAT_QUERY, &[]).map_err(|e| e.to_string())?;
    let (mut name, mut value) = (String::new(), 0f64);
    while rows.next() {
        rows.scan(&mut [&mut name, &mut value])
            .map_err(|e| e.to_string())?;
        if let Ok(mut s) = sspl.lock() {
            let v = value as i64;
            match name.as_str() {
                "Contributors" => s.contributors = v,
                "Contributions" => s.contributions = v,
                "Code committers" => s.committers = v,
                "Commits" => s.commits = v,
                "Events" => s.events = v,
                "Forkers" => s.forkers = v,
                "Repositories" => s.repositories = v,
                "Stargazers" => s.stargazers = v,
                _ => {}
            }
        }
    }
    rows.err().map_err(|e| e.to_string())
}

pub fn api_site_stats(w: &mut Response, payload: &Payload) {
    let api_name = SITE_STATS;
    let (mut project, mut db) = (String::new(), String::new());
    let res = (|| -> ApiResult<Vec<u8>> {
        db = handle_shared_payload(payload, &mut project)?;
        let key = [project.clone(), db.clone()];
        let key_str = format!("[{} {}]", key[0], key[1]);
        let cached = state().site_stats_cache.lock().ok().and_then(|c| {
            c.get(&key)
                .map(|e| (e.dt, e.dt_str.clone(), e.site_stats.clone()))
        });
        if let Some((dt, dt_str, ss)) = cached {
            let age = age_seconds(dt);
            if age < SITE_STATS_CACHE_TTL as f64 {
                printf!(
                    "{}: using cached value for {}: {{dt:{} siteStats:{}}} (age is {:.0} < {})\n",
                    api_name,
                    key_str,
                    dt_str,
                    ss.go_string(),
                    age,
                    SITE_STATS_CACHE_TTL
                );
                return Ok(encode(&ss));
            }
            printf!(
                "{}: deleting cached values for {} (age is {:.0} >= {})\n",
                api_name,
                key_str,
                age,
                SITE_STATS_CACHE_TTL
            );
            if let Ok(mut c) = state().site_stats_cache.lock() {
                c.remove(&key);
            }
        }
        let (ctx, c) = get_context_and_db(&db)?;
        let ctx = Arc::new(ctx);
        let c = Arc::new(c);
        let sspl = Arc::new(Mutex::new(SiteStatsPayload {
            project: project.clone(),
            db_name: db.clone(),
            ..Default::default()
        }));
        let (tx, rx) = mpsc::channel::<Result<(), String>>();
        type Job = fn(&PgConn, &Ctx, &Mutex<SiteStatsPayload>) -> Result<(), String>;
        let jobs: [Job; 4] = [
            site_stats_pstat,
            |c, ctx, s| site_stats_single(c, ctx, SITE_STATS_BOC_QUERY, s, |p, v| p.boc = v),
            |c, ctx, s| {
                site_stats_single(c, ctx, SITE_STATS_COUNTRIES_QUERY, s, |p, v| {
                    p.countries = v
                })
            },
            |c, ctx, s| {
                site_stats_single(c, ctx, SITE_STATS_COMPANIES_QUERY, s, |p, v| {
                    p.companies = v
                })
            },
        ];
        for job in jobs {
            let (tx, ctx, c, sspl) = (
                tx.clone(),
                Arc::clone(&ctx),
                Arc::clone(&c),
                Arc::clone(&sspl),
            );
            thread::spawn(move || {
                let _ = tx.send(job(&c, &ctx, &sspl));
            });
        }
        drop(tx);
        for r in rx {
            // Go: the first error received answers the request (the shadowed
            // `err` keeps the exit log at `<nil>`).
            if let Err(e) = r {
                return Err(ApiError::quiet(e));
            }
        }
        let ss = sspl.lock().map(|s| s.clone()).unwrap_or_default();
        let body = encode(&ss);
        if let Ok(mut cache) = state().site_stats_cache.lock() {
            cache.insert(
                key,
                SiteStatsCacheEntry {
                    dt: Local::now(),
                    dt_str: gofmt::time_now(),
                    site_stats: ss,
                },
            );
        }
        printf!("{}: written value to cache for {}\n", api_name, key_str);
        Ok(body)
    })();
    finish(w, api_name, &project, &db, payload, res);
}
