//! `website_data` — Rust port of `cmd/website_data/website_data.go`.
//!
//! Generates the JSON files consumed by the DevStats website:
//!
//! * `<JSONsDir>projects.json` — the ordered list of enabled projects from
//!   `projects.yaml` (name, title, status, main repo, dashboard and database
//!   dump URLs built from the host name);
//! * `<JSONsDir><project>.json` — per-project activity data computed from the
//!   project's PostgreSQL database (commit graphs for the last day / week /
//!   month, discussion and stars totals, open issues) plus the latest tag of
//!   the main repository (`last_tag.sh`).
//!
//! Output, environment handling, fatal conditions and exit codes follow the Go
//! tool; the only intended differences are documented in `rust/README.md`
//! (sorted JSON object keys — Go's are random — and the per-project files
//! being generated in `projects.yaml` order instead of Go's random map order).

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use devstatscode::chrono::{DateTime, FixedOffset, Local};
use devstatscode::pg::api::query_sql_with_err;
use devstatscode::pg::value::go_rfc3339nano;
use devstatscode::pg::PgConn;
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    consts, exec, fatal_on_err, fatal_on_error, io, json, pg, printf, projects, signal, threads,
    time as gotime, Ctx,
};
use serde::{Serialize, Serializer};

/// Go `time.Time` JSON encoding (RFC3339 with nanoseconds, local offset).
fn go_time<S: Serializer>(t: &DateTime<FixedOffset>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&go_rfc3339nano(t))
}

fn now() -> DateTime<FixedOffset> {
    Local::now().fixed_offset()
}

/// `projects.json` root object.
#[derive(Serialize)]
struct AllProjects {
    /// Go marshals a nil slice as `null`, hence the `Option`.
    projects: Option<Vec<Project>>,
    summary: String,
    #[serde(serialize_with = "go_time")]
    timestamp: DateTime<FixedOffset>,
}

/// One entry of `projects.json`.
#[derive(Serialize)]
struct Project {
    name: String,
    title: String,
    status: String,
    repo: String,
    #[serde(rename = "dashboardUrl")]
    dashboard_url: String,
    #[serde(rename = "dbDumpUrl")]
    db_dump_url: String,
}

/// `<project>.json` root object.
#[derive(Serialize)]
struct ProjectStats {
    #[serde(rename = "activityTotals")]
    totals: ActivityTotals,
    #[serde(rename = "latestVersion")]
    latest_version: String,
    #[serde(rename = "openIssues")]
    open_issues: i64,
    #[serde(rename = "recentDiscussion")]
    recent_discussion: i64,
    stars: i64,
    #[serde(rename = "commitGraph")]
    commit_graph: CommitGraph,
    #[serde(serialize_with = "go_time")]
    timestamp: DateTime<FixedOffset>,
}

impl Default for ProjectStats {
    fn default() -> Self {
        ProjectStats {
            totals: ActivityTotals::default(),
            latest_version: String::new(),
            open_issues: 0,
            recent_discussion: 0,
            stars: 0,
            commit_graph: CommitGraph::default(),
            // Go's zero `time.Time`; always overwritten before writing.
            timestamp: DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z").expect("valid"),
        }
    }
}

/// Commits per hour (last day), per day (last week) and per week (last
/// month): `[index, commits]` pairs.
#[derive(Serialize, Default)]
struct CommitGraph {
    day: [[i64; 2]; 24],
    week: [[i64; 2]; 7],
    month: [[i64; 2]; 4],
}

#[derive(Serialize, Default)]
struct ActivityTotals {
    day: ActivityTotal,
    week: ActivityTotal,
    month: ActivityTotal,
}

#[derive(Serialize, Default)]
struct ActivityTotal {
    commits: i64,
    discussion: i64,
    stars: i64,
}

/// Go `getIntValue`: the (single) integer result of `sql`, 0 without rows.
fn get_int_value(con: &PgConn, ctx: &Ctx, sql: &str) -> i64 {
    let mut ival: i64 = 0;
    let mut rows = query_sql_with_err(con, ctx, sql, &[]);
    while rows.next() {
        fatal_on_err(rows.scan(&mut [&mut ival]));
    }
    fatal_on_err(rows.err());
    fatal_on_err(rows.close());
    ival
}

/// Fill `stats` from the project database `name` and the main repository's
/// latest tag (Go `generateJSONData`).
fn generate_json_data(
    ctx: &Ctx,
    name: &str,
    exclude_bots: &str,
    last_tag_cmd: &str,
    repo: &str,
    stats: &mut ProjectStats,
) {
    let name = if name == consts::KUBERNETES {
        consts::GHA
    } else if name == consts::ALL {
        "allprj"
    } else {
        name
    };
    let con = pg::pg_conn_db_shared(ctx, name);
    let commits_between = |unit: &str, from: i64, to: i64| {
        get_int_value(
            &con,
            ctx,
            &format!(
                "select count(distinct sha) from gha_commits \
                 where dup_created_at >= now() - '{from} {unit}'::interval \
                 and dup_created_at < now() - '{to} {unit}'::interval \
                 and (lower(dup_actor_login) {exclude_bots})"
            ),
        )
    };
    for i in 0..24i64 {
        let to = 23 - i;
        let from = to + 1;
        stats.commit_graph.day[i as usize] = [i, commits_between("hours", from, to)];
    }
    for i in 0..7i64 {
        let to = 6 - i;
        let from = to + 1;
        stats.commit_graph.week[i as usize] = [i, commits_between("days", from, to)];
    }
    for i in 0..4i64 {
        let to = 3 - i;
        let from = to + 1;
        stats.commit_graph.month[i as usize] = [i, commits_between("weeks", from, to)];
    }
    stats.totals.day.commits = stats.commit_graph.week[6][1];
    stats.totals.week.commits = stats.commit_graph.month[3][1];
    stats.totals.month.commits = get_int_value(
        &con,
        ctx,
        &format!(
            "select count(distinct sha) from gha_commits \
             where dup_created_at >= now() - '1 month'::interval \
             and (lower(dup_actor_login) {exclude_bots})"
        ),
    );
    let discussion = |period: &str| {
        get_int_value(
            &con,
            ctx,
            &format!(
                "select count(distinct event_id) from gha_texts \
                 where created_at >= now() - '{period}'::interval \
                 and (lower(actor_login) {exclude_bots})"
            ),
        )
    };
    stats.totals.day.discussion = discussion("1 day");
    stats.totals.week.discussion = discussion("1 week");
    stats.totals.month.discussion = discussion("1 month");
    stats.recent_discussion = stats.totals.month.discussion;
    let stars_diff = |period: &str| {
        get_int_value(
            &con,
            ctx,
            &format!(
                "select coalesce(sum(sub.diff), 0) \
                 from (select min(stargazers_count) as fmin, \
                 max(stargazers_count) - min(stargazers_count) as diff \
                 from gha_forkees where dup_repo_name = full_name and \
                 dup_created_at >= now() - '{period}'::interval \
                 group by dup_repo_name) sub where fmin > 0 and diff > 0"
            ),
        )
    };
    stats.totals.day.stars = stars_diff("1 day");
    stats.totals.week.stars = stars_diff("1 week");
    stats.totals.month.stars = stars_diff("1 month");
    stats.stars = get_int_value(
        &con,
        ctx,
        "select coalesce(sum(fmax), 0) from (select max(stargazers_count) as fmax \
         from gha_forkees where dup_repo_name = full_name \
         and dup_created_at >= now() - '3 months'::interval \
         group by dup_repo_name) sub",
    );
    stats.open_issues = get_int_value(
        &con,
        ctx,
        "select count(sub.id) from (select distinct id, \
         last_value(closed_at) over update_date as closed_at \
         from gha_issues where is_pull_request = false \
         window update_date as (partition by id order by \
         updated_at asc, event_id asc range between current row \
         and unbounded following)) sub where sub.closed_at is null",
    );
    let mut tag = "-".to_string();
    if !repo.is_empty() {
        let rwd = format!("{}{}", ctx.repos_dir, repo);
        let env: BTreeMap<String, String> =
            BTreeMap::from([("GIT_TERMINAL_PROMPT".to_string(), "0".to_string())]);
        let (out, err) = exec::exec_command_go(ctx, &[last_tag_cmd.to_string(), rwd], &env);
        // Go keeps the raw (untrimmed) output when the command failed.
        tag = out;
        if err.is_none() {
            tag = tag.trim().to_string();
        }
    }
    stats.latest_version = tag;
    // `defer func() { lib.FatalOnError(con.Close()) }()` — closing the pool
    // cannot fail here.
    con.close();
}

/// Compute and write `<JSONsDir><name>.json`.
fn write_project_json(
    ctx: &Ctx,
    name: &str,
    exclude_bots: &str,
    last_tag_cmd: &str,
    main_repo: &str,
) {
    let mut stats = ProjectStats::default();
    generate_json_data(ctx, name, exclude_bots, last_tag_cmd, main_repo, &mut stats);
    stats.timestamp = now();
    json::object_to_json(&stats, &format!("{}{}.json", ctx.jsons_dir, name));
}

/// Go `generateWebsiteData`.
fn generate_website_data(ctx: &mut Ctx) {
    // We need this to capture 'last_tag.sh' output.
    ctx.exec_output = true;
    ctx.exec_fatal = false;

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    let cmd_prefix = if ctx.local_cmd {
        consts::LOCAL_GIT_SCRIPTS
    } else {
        ""
    };
    let last_tag_cmd = format!("{cmd_prefix}last_tag.sh");

    let hostname = fatal_on_err(io::hostname());
    let proto = "https://";
    let prefix = format!("{proto}{hostname}/");

    // `ioutil.ReadFile` — no `/shared/` fallback.
    let data = fatal_on_err(io::read_file_raw(format!(
        "{data_prefix}{}",
        ctx.projects_yaml
    )));
    let all: projects::AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };

    // Ordered & filtered projects (see `projects::get_projects_list`).
    let (names, projs) = projects::get_projects_list(ctx, &all);
    let mut jprojs: Vec<Project> = Vec::new();
    for (name, proj) in names.iter().zip(&projs) {
        let dash_url = if name == consts::KUBERNETES {
            format!("{proto}k8s.{hostname}")
        } else {
            format!("{proto}{name}.{hostname}")
        };
        jprojs.push(Project {
            name: name.clone(),
            title: proj.full_name.clone(),
            status: proj.status.clone(),
            repo: proj.main_repo.clone(),
            dashboard_url: dash_url,
            db_dump_url: format!("{prefix}{}.dump", proj.pdb),
        });
    }
    let jall = AllProjects {
        projects: if jprojs.is_empty() {
            None
        } else {
            Some(jprojs)
        },
        summary: consts::ALL.to_string(),
        timestamp: now(),
    };
    json::object_to_json(&jall, &format!("{}projects.json", ctx.jsons_dir));

    // Read bots exclusion partial SQL
    let bytes = fatal_on_err(io::read_file(
        ctx,
        &format!("{data_prefix}util_sql/exclude_bots.sql"),
    ));
    let exclude_bots = String::from_utf8_lossy(&bytes).into_owned();

    let thr_n = threads::get_threads_num(ctx);
    // Go `PgConnDB` clears the reconnect flag from every worker; the workers
    // here share the context read-only, so clear it up front.
    if !names.is_empty() {
        ctx.can_reconnect = false;
    }
    let ctx = &*ctx;
    let main_repo = |name: &str| {
        all.projects
            .get(name)
            .map(|p| p.main_repo.clone())
            .unwrap_or_default()
    };
    if thr_n > 1 {
        // One worker per project, at most `thr_n` running at a time,
        // synchronised through an unbuffered channel like the Go goroutines.
        thread::scope(|s| {
            let (tx, rx) = mpsc::sync_channel::<bool>(0);
            let mut n_threads = 0usize;
            for name in &names {
                let tx = tx.clone();
                let repo = main_repo(name);
                let (exclude_bots, last_tag_cmd) = (&exclude_bots, &last_tag_cmd);
                s.spawn(move || {
                    write_project_json(ctx, name, exclude_bots, last_tag_cmd, &repo);
                    let _ = tx.send(true);
                });
                n_threads += 1;
                if n_threads >= thr_n {
                    let _ = rx.recv();
                    n_threads -= 1;
                }
            }
            while n_threads > 0 {
                let _ = rx.recv();
                n_threads -= 1;
            }
        });
    } else {
        printf!("Using single threaded version\n");
        for name in &names {
            write_project_json(ctx, name, &exclude_bots, &last_tag_cmd, &main_repo(name));
        }
    }
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    generate_website_data(&mut ctx);
    printf!(
        "Generated website data in: {}\n",
        gotime::format_go_duration(dt_start.elapsed())
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projects_json_shape() {
        let jall = AllProjects {
            projects: Some(vec![Project {
                name: "p1".into(),
                title: "P & <One>".into(),
                status: "Graduated".into(),
                repo: "org/p1".into(),
                dashboard_url: "https://p1.host".into(),
                db_dump_url: "https://host/p1.dump".into(),
            }]),
            summary: consts::ALL.to_string(),
            timestamp: DateTime::parse_from_rfc3339("2026-09-11T20:13:45.5+02:00").unwrap(),
        };
        let value = serde_json::to_value(&jall).unwrap();
        let pretty = String::from_utf8(json::to_pretty_json(&value).unwrap()).unwrap();
        assert_eq!(
            pretty,
            "{\n  \"projects\": [\n    {\n      \"dashboardUrl\": \"https://p1.host\",\n      \
             \"dbDumpUrl\": \"https://host/p1.dump\",\n      \"name\": \"p1\",\n      \
             \"repo\": \"org/p1\",\n      \"status\": \"Graduated\",\n      \
             \"title\": \"P \\u0026 \\u003cOne\\u003e\"\n    }\n  ],\n  \"summary\": \"all\",\n  \
             \"timestamp\": \"2026-09-11T20:13:45.5+02:00\"\n}"
        );
    }

    #[test]
    fn empty_projects_marshal_as_null() {
        let jall = AllProjects {
            projects: None,
            summary: consts::ALL.to_string(),
            timestamp: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z").unwrap(),
        };
        let value = serde_json::to_value(&jall).unwrap();
        let pretty = String::from_utf8(json::to_pretty_json(&value).unwrap()).unwrap();
        assert_eq!(
            pretty,
            "{\n  \"projects\": null,\n  \"summary\": \"all\",\n  \"timestamp\": \"2026-01-01T00:00:00Z\"\n}"
        );
    }

    #[test]
    fn stats_default_and_graph_shape() {
        let mut stats = ProjectStats::default();
        stats.commit_graph.day[23] = [23, 7];
        stats.commit_graph.week[6] = [6, 9];
        stats.commit_graph.month[3] = [3, 11];
        let value = serde_json::to_value(&stats).unwrap();
        assert_eq!(value["commitGraph"]["day"].as_array().unwrap().len(), 24);
        assert_eq!(value["commitGraph"]["day"][23], serde_json::json!([23, 7]));
        assert_eq!(value["commitGraph"]["week"].as_array().unwrap().len(), 7);
        assert_eq!(value["commitGraph"]["week"][6], serde_json::json!([6, 9]));
        assert_eq!(value["commitGraph"]["month"].as_array().unwrap().len(), 4);
        assert_eq!(value["commitGraph"]["month"][3], serde_json::json!([3, 11]));
        assert_eq!(value["activityTotals"]["day"]["commits"], 0);
        assert_eq!(value["latestVersion"], "");
        assert_eq!(value["timestamp"], "0001-01-01T00:00:00Z");
        let keys: Vec<&String> = value.as_object().unwrap().keys().collect();
        assert_eq!(
            keys,
            [
                "activityTotals",
                "commitGraph",
                "latestVersion",
                "openIssues",
                "recentDiscussion",
                "stars",
                "timestamp"
            ]
        );
    }
}
