//! `reconcile_dbs` — pull the GitHub events (and their dependent rows) that
//! the peer DevStats database(s) have for the repositories this database
//! tracks but this database lacks; Rust port of
//! `cmd/reconcile_dbs/reconcile_dbs.go`.
//!
//! Every project database and the shared database of its `shared_db`
//! (projects.yaml, e.g. `allprj`) are fed from the same GH Archive files, but
//! the GitHub API restores differ per database (ghapi2db runs 4x/day for
//! projects and once a day for the shared one, the repo events feed is capped
//! at 300 events per pass, get_repos restores orphan commits per database), so
//! each side ends up with events the other one misses. gha2db_sync runs this
//! tool after ghapi2db and before the `structure` postprocess, so the copied
//! rows get their repo groups and derived tables in the same sync and
//! calc_metric sees them.
//!
//! Modes (a database is only ever written by its own sync — pull only):
//! - project: target = `PG_DB` (project database), source = its `shared_db`
//!   from projects.yaml (project found by `GHA2DB_PROJECT`, else by
//!   `psql_db` = `PG_DB`); no `shared_db` — nothing to do,
//! - shared: `PG_DB` is the `shared_db` of some enabled projects (e.g.
//!   allprj) — sources = all of their `psql_db` (ordered by project order,
//!   name), unavailable databases are skipped,
//! - explicit: `GHA2DB_RECONCILE_DBS=db1,db2` — those sources (no
//!   projects.yaml needed).
//!
//! Scope: repositories present in both `gha_repos` (ids). Window:
//! `GHA2DB_RECONCILE_RANGE` (PostgreSQL interval, default "90 days") before
//! now. Classes: native GitHub ids (0 < id < 2^48, GH Archive and events feed
//! restores share them) and synthetic orphan pushes (id < 0, deterministic
//! ids); artificial API-restored events (id >= 2^48) only with
//! `GHA2DB_RECONCILE_ARTIFICIAL=1` (each side regenerates them itself).
//!
//! Filter: only the events the target's own `gha2db` would ingest are copied:
//! the org/repo rules of the target project (`command_line` in
//! projects.yaml: orgs and repos lists or `regexp:` patterns, parsed like
//! gha2db_sync/gha2db do) applied to the event's `dup_repo_name` with
//! `repo_hit`, and the actor rules (`GHA2DB_ACTORS_FILTER/ALLOW/FORBID`) with
//! `actor_hit`; the project's `env` (e.g. `GHA2DB_EXCLUDE_REPOS`,
//! `GHA2DB_EXACT`) is applied like gha2db_sync does (unless `ENV_SET` is
//! set). A repository that moved to another org (it keeps its id, so it is in
//! both `gha_repos`) is therefore not pulled from a database that tracks the
//! new org. The target project is `GHA2DB_PROJECT` when its `psql_db` is
//! `PG_DB`, else the first enabled project with `psql_db` = `PG_DB`; when
//! there is none (or projects.yaml is absent in explicit mode) nothing is
//! filtered. `GHA2DB_RECONCILE_HIST=1` applies the project's historical rules
//! instead (`hist_command_line`: the biggest scope the project ever had, so
//! events of repositories it used to track are pulled too; `command_line`
//! when the project has none).
//!
//! Stateless idempotency: per (repo_id, day) digests (count, sum of ids) on
//! both sides; only the differing buckets are diffed by id; a second run
//! copies nothing.
//!
//! Copy rules (per batch of missing events, one transaction on the target):
//! - a synthetic orphan push whose commits already exist in the target (under
//!   any event) is skipped,
//! - `gha_events` and all event scoped tables are copied with
//!   `insert ... on conflict do nothing`, the referenced `gha_repos` (id,
//!   name, org_id, org_login — the repo groups come from the `structure`
//!   postprocess), `gha_orgs`, `gha_labels` and (only without a shared
//!   affiliations DB) `gha_actors` rows too,
//! - the commits of copied GHA pushes take over the rows the orphan restore
//!   wrote for the same SHAs under synthetic events (like get_repos does),
//!   emptied synthetic events are removed,
//! - `is_distinct` of the copied commits is recomputed in the target,
//! - the targeted postprocess (texts, labels, issues/PRs) runs for the copied
//!   and affected events.
//!
//! `GHA2DB_RECONCILE_DRY_RUN=1` computes and reports everything without
//! writing. `GHA2DB_RECONCILE_SKIP_DBS=a,b` skips the listed sources.
//! `GHA2DB_RECONCILESKIP` makes gha2db_sync skip the tool.
//! `GHA2DB_RECONCILE_HIST=1` uses the project's historical org/repo rules (see
//! above).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use devstatscode::chrono::Local;
use devstatscode::pg::{self, fatal_on_pg_err, fatal_on_pg_error, PgConn, PgError, PgTx, SqlArg};
use devstatscode::project_filter::{
    new_project_filter, project_for_db, read_projects, read_projects_if_present, ProjectFilter,
};
use devstatscode::time as gotime;
use devstatscode::{fatal_on_err, fatalf, printf, projects, restore, signal, Ctx};

/// Maximum number of bind parameters a single PostgreSQL statement can use.
const MAX_PARAMS: usize = 65535;

/// Number of missing events copied per target transaction (and ids per array literal).
const ID_BATCH: usize = 1000;

/// Default reconciliation window (PostgreSQL interval).
const DEFAULT_RANGE: &str = "90 days";

/// Artificial (GitHub API sourced) event id base: 2^48 (Go `lib.ArtificialIDBase`).
const ARTIFICIAL_ID_BASE: i64 = 281474976710656;

/// Copied tables: `gha_events` first (key `id`), the rest by `event_id`, in this order.
const EVENT_TABLES: &[(&str, &str)] = &[
    ("gha_events", "id"),
    ("gha_payloads", "event_id"),
    ("gha_commits", "event_id"),
    ("gha_commits_roles", "event_id"),
    ("gha_pages", "event_id"),
    ("gha_comments", "event_id"),
    ("gha_issues", "event_id"),
    ("gha_issues_assignees", "event_id"),
    ("gha_issues_labels", "event_id"),
    ("gha_milestones", "event_id"),
    ("gha_forkees", "event_id"),
    ("gha_releases", "event_id"),
    ("gha_releases_assets", "event_id"),
    ("gha_assets", "event_id"),
    ("gha_pull_requests", "event_id"),
    ("gha_pull_requests_assignees", "event_id"),
    ("gha_pull_requests_requested_reviewers", "event_id"),
    ("gha_branches", "event_id"),
    ("gha_teams", "event_id"),
    ("gha_teams_repositories", "event_id"),
    ("gha_reviews", "event_id"),
];

/// Dimension tables copied for the referenced ids (`gha_actors` only in legacy mode), in this order.
const DIM_TABLES: &[&str] = &["gha_repos", "gha_orgs", "gha_labels", "gha_actors"];

/// Digest key: repository and day (`YYYY-MM-DD`).
type Bucket = (i64, String);

/// Digest value: number of events and the sum of their ids (numeric text).
type Digest = (i64, String);

/// Per source result (Go `sourceStats`).
#[derive(Debug, Default)]
struct SourceStats {
    copied: usize,
    native: usize,
    orphan: usize,
    artificial: usize,
    filtered: usize,
    skipped: usize,
    inserted: usize,
    taken: usize,
    taken_from: usize,
    removed: usize,
    postprocess: usize,
    /// table -> (rows read from the source, rows inserted into the target)
    tables: BTreeMap<String, (usize, usize)>,
}

impl SourceStats {
    /// Go `addTableStats`: accumulate rows/inserted of a table.
    fn add_table_stats(&mut self, table: &str, rows: usize, inserted: usize) {
        let ts = self.tables.entry(table.to_string()).or_insert((0, 0));
        ts.0 += rows;
        ts.1 += inserted;
        self.inserted += inserted;
    }
}

/// Tool configuration from the environment (Go `config`).
#[derive(Debug, Default)]
struct Config {
    mode: String,
    detail: String,
    sources: Vec<String>,
    explicit: bool,
    range_str: String,
    dry_run: bool,
    artificial: bool,
    /// `GHA2DB_RECONCILE_HIST`: the target project's historical rules (`hist_command_line`).
    hist: bool,
    filter: ProjectFilter,
}

/// Go `envFlag`: `1`, `t`, `true`, `y`, `yes` (any case, trimmed).
fn env_flag(name: &str) -> bool {
    let v = std::env::var(name).unwrap_or_default();
    matches!(
        v.trim().to_lowercase().as_str(),
        "1" | "t" | "true" | "y" | "yes"
    )
}

/// Go `parseDBList`: comma separated list of database names: trimmed, empty
/// items dropped, duplicates removed (first wins).
fn parse_db_list(value: &str) -> Vec<String> {
    let mut dbs: Vec<String> = Vec::new();
    for db in value.split(',') {
        let db = db.trim();
        if db.is_empty() || dbs.iter().any(|d| d == db) {
            continue;
        }
        dbs.push(db.to_string());
    }
    dbs
}

/// Go `classOf`: event id class: "native" (0 < id < 2^48), "orphan" (id < 0),
/// "artificial" (id >= 2^48), "zero" (id = 0).
fn class_of(id: i64) -> &'static str {
    if id < 0 {
        "orphan"
    } else if id == 0 {
        "zero"
    } else if id >= ARTIFICIAL_ID_BASE {
        "artificial"
    } else {
        "native"
    }
}

/// Go `int64ArrayLiteral`: `'{1,2,3}'::bigint[]` (safe: integers only).
fn int64_array_literal(ids: &[i64]) -> String {
    let mut s = String::from("'{");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&id.to_string());
    }
    s.push_str("}'::bigint[]");
    s
}

/// Go `textArrayLiteral`: `'{"a","b"}'::text[]` with array element and SQL literal quoting.
fn text_array_literal(values: &[String]) -> String {
    let mut s = String::from("'{");
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('"');
        for r in value.chars() {
            match r {
                '\\' | '"' => {
                    s.push('\\');
                    s.push(r);
                }
                '\'' => s.push_str("''"),
                _ => s.push(r),
            }
        }
        s.push('"');
    }
    s.push_str("}'::text[]");
    s
}

/// Go `dateArrayLiteral`: `'{2024-01-02,2024-01-03}'::date[]` (values are
/// `YYYY-MM-DD` texts from the database).
fn date_array_literal(days: &[String]) -> String {
    format!("'{{{}}}'::date[]", days.join(","))
}

/// Go `batchValues`: `values ($1,$2),($3,$4),...` for `n_rows` rows of `n_cols` columns.
fn batch_values(n_rows: usize, n_cols: usize) -> String {
    let mut s = String::from("values ");
    let mut k = 1usize;
    for r in 0..n_rows {
        if r > 0 {
            s.push(',');
        }
        s.push('(');
        for c in 0..n_cols {
            if c > 0 {
                s.push(',');
            }
            s.push('$');
            s.push_str(&k.to_string());
            k += 1;
        }
        s.push(')');
    }
    s
}

/// Go `intersectSorted`: ids present in both sorted slices (sorted, unique).
fn intersect_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut res: Vec<i64> = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            i += 1;
        } else if a[i] > b[j] {
            j += 1;
        } else {
            if res.last() != Some(&a[i]) {
                res.push(a[i]);
            }
            i += 1;
            j += 1;
        }
    }
    res
}

/// Go `diffSorted`: ids of the sorted slice `a` that are not in the sorted slice `b` (sorted, unique).
fn diff_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut res: Vec<i64> = Vec::new();
    let mut j = 0usize;
    for &id in a {
        while j < b.len() && b[j] < id {
            j += 1;
        }
        if j < b.len() && b[j] == id {
            continue;
        }
        if res.last() != Some(&id) {
            res.push(id);
        }
    }
    res
}

/// Go `differingBuckets`: source buckets missing in the target or with a
/// different digest, sorted by (repo_id, day).
fn differing_buckets(
    source: &BTreeMap<Bucket, Digest>,
    target: &BTreeMap<Bucket, Digest>,
) -> Vec<Bucket> {
    source
        .iter()
        .filter(|(key, sd)| target.get(*key) != Some(sd))
        .map(|(key, _)| key.clone())
        .collect()
}

/// Go `chunkInt64`/`chunkStrings`: split into chunks of at most `size` items.
fn chunks<T: Clone>(items: &[T], size: usize) -> Vec<Vec<T>> {
    items.chunks(size.max(1)).map(|c| c.to_vec()).collect()
}

/// Go `classCondition`: SQL condition selecting the reconciled event id classes.
fn class_condition(artificial: bool) -> String {
    if artificial {
        String::new()
    } else {
        format!(" and id < {ARTIFICIAL_ID_BASE}")
    }
}

/// Go `classesInfo`: human readable classes list.
fn classes_info(artificial: bool) -> &'static str {
    if artificial {
        "native, orphan, artificial"
    } else {
        "native, orphan"
    }
}

/// Go `isNoDBError`: `invalid_catalog_name` (SQLSTATE 3D000) or a message
/// saying that a database does not exist.
fn is_no_db_error(err: &PgError) -> bool {
    if let Some(e) = err.server() {
        return e.name() == "invalid_catalog_name" || e.code == "3D000";
    }
    let msg = err.to_string().to_lowercase();
    msg.contains("database") && msg.contains("does not exist")
}

/// Go `sharedDBSources`: `psql_db` of the enabled projects with `shared_db` =
/// target (ordered by order, name, db; unique).
fn shared_db_sources(ctx: &Ctx, all: &projects::AllProjects, target: &str) -> Vec<String> {
    let mut project_dbs: Vec<(i64, &str, String)> = Vec::new();
    for (name, proj) in &all.projects {
        if projects::is_project_disabled(ctx, name, proj.disabled) {
            continue;
        }
        if proj.shared_db.trim() != target {
            continue;
        }
        let db = proj.pdb.trim();
        if db.is_empty() || db == target {
            continue;
        }
        project_dbs.push((proj.order, name.as_str(), db.to_string()));
    }
    project_dbs.sort();
    let mut dbs: Vec<String> = Vec::new();
    for (_, _, db) in project_dbs {
        if !dbs.contains(&db) {
            dbs.push(db);
        }
    }
    dbs
}

/// Go `resolveConfig`: mode, sources and knobs from the environment (and
/// projects.yaml when needed). `None` when there is nothing to reconcile
/// (message already printed).
fn resolve_config(ctx: &Ctx, target: &str) -> Option<Config> {
    let mut cfg = Config {
        range_str: std::env::var("GHA2DB_RECONCILE_RANGE")
            .unwrap_or_default()
            .trim()
            .to_string(),
        dry_run: env_flag("GHA2DB_RECONCILE_DRY_RUN"),
        artificial: env_flag("GHA2DB_RECONCILE_ARTIFICIAL"),
        hist: env_flag("GHA2DB_RECONCILE_HIST"),
        ..Config::default()
    };
    if cfg.range_str.is_empty() {
        cfg.range_str = DEFAULT_RANGE.to_string();
    }
    let explicit = parse_db_list(&std::env::var("GHA2DB_RECONCILE_DBS").unwrap_or_default());
    let (all, path): (Option<projects::AllProjects>, String);
    if !explicit.is_empty() {
        for db in &explicit {
            if db == target {
                fatalf!(
                    "reconcile_dbs: source database '{}' is the target database",
                    db
                );
            }
        }
        cfg.mode = "explicit".to_string();
        cfg.detail = "GHA2DB_RECONCILE_DBS".to_string();
        cfg.sources = explicit;
        cfg.explicit = true;
        (all, path) = read_projects_if_present(ctx);
    } else {
        let (read, read_path) = read_projects(ctx);
        let shared = shared_db_sources(ctx, &read, target);
        if !shared.is_empty() {
            cfg.mode = "shared".to_string();
            cfg.detail = format!("projects with shared_db '{target}' in {read_path}");
            cfg.sources = shared;
        } else {
            let Some((name, proj)) = project_for_db(ctx, &read, target) else {
                printf!(
                    "reconcile_dbs: {}: no enabled project uses this database in {}, nothing to reconcile\n",
                    target,
                    read_path
                );
                return None;
            };
            let shared_db = proj.shared_db.trim();
            if shared_db.is_empty() || shared_db == target {
                printf!(
                    "reconcile_dbs: {}: project '{}' has no shared database in {}, nothing to reconcile\n",
                    target,
                    name,
                    read_path
                );
                return None;
            }
            cfg.mode = "project".to_string();
            cfg.detail = format!("project '{name}' shared_db '{shared_db}' in {read_path}");
            cfg.sources = vec![shared_db.to_string()];
        }
        (all, path) = (Some(read), read_path);
    }
    cfg.filter = new_project_filter(ctx, all.as_ref(), target, &path, cfg.hist);
    let skip_dbs = parse_db_list(&std::env::var("GHA2DB_RECONCILE_SKIP_DBS").unwrap_or_default());
    if !skip_dbs.is_empty() {
        let mut sources: Vec<String> = Vec::new();
        let mut skipped: Vec<String> = Vec::new();
        for db in cfg.sources {
            if skip_dbs.contains(&db) {
                skipped.push(db);
            } else {
                sources.push(db);
            }
        }
        cfg.sources = sources;
        printf!(
            "reconcile_dbs: {}: skipped {} source(s) using GHA2DB_RECONCILE_SKIP_DBS: {}\n",
            target,
            skipped.len(),
            skipped.join(", ")
        );
    }
    if cfg.sources.is_empty() {
        printf!(
            "reconcile_dbs: {}: no source databases, nothing to reconcile\n",
            target
        );
        return None;
    }
    Some(cfg)
}

/// Go `connectSource`: connect to a source database: fatal when it is not
/// available, unless `non_fatal` (returns the error).
fn connect_source(ctx: &mut Ctx, db: &str, non_fatal: bool) -> Result<PgConn, PgError> {
    if !non_fatal {
        return Ok(pg::pg_conn_db(ctx, db));
    }
    let mut lctx = ctx.clone();
    lctx.pg_db = db.to_string();
    lctx.exec_fatal = false;
    lctx.exec_output = true;
    let c = pg::pg_conn_err(&lctx)?;
    if let Err(e) = c.ping() {
        c.close();
        return Err(e);
    }
    Ok(c)
}

/// Go `queryInt64s`: single bigint column query results (in query order).
fn query_i64s(con: &PgConn, ctx: &Ctx, query: &str, args: &[SqlArg]) -> Vec<i64> {
    let mut rows = pg::query_sql_with_err(con, ctx, query, args);
    let mut ids: Vec<i64> = Vec::new();
    while rows.next() {
        let mut id: i64 = 0;
        fatal_on_pg_err(rows.scan(&mut [&mut id]));
        ids.push(id);
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    ids
}

/// Go `queryStrings`: single text column query results (in query order).
fn query_strings(con: &PgConn, ctx: &Ctx, query: &str, args: &[SqlArg]) -> Vec<String> {
    let mut rows = pg::query_sql_with_err(con, ctx, query, args);
    let mut values: Vec<String> = Vec::new();
    while rows.next() {
        let mut value = String::new();
        fatal_on_pg_err(rows.scan(&mut [&mut value]));
        values.push(value);
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    values
}

/// Go `repoIDs`: distinct repository ids of `gha_repos` (sorted).
fn repo_ids(con: &PgConn, ctx: &Ctx) -> Vec<i64> {
    query_i64s(
        con,
        ctx,
        "select distinct id from gha_repos order by id",
        &[],
    )
}

/// Go `digestsSQL`: digest query for a chunk of scope repositories.
fn digests_sql(chunk: &[i64], class_cond: &str) -> String {
    format!(
        "select repo_id, date_trunc('day', created_at)::date::text, count(*), sum(id)::text from gha_events where created_at >= {}::timestamp and repo_id = any({}){} group by 1, 2",
        pg::n_value(1),
        int64_array_literal(chunk),
        class_cond
    )
}

/// Go `digests`: per (repo_id, day) event count and sum of ids for the scope
/// repositories within the window.
fn digests(
    con: &PgConn,
    ctx: &Ctx,
    scope: &[i64],
    dt_from: &str,
    class_cond: &str,
) -> BTreeMap<Bucket, Digest> {
    let mut res: BTreeMap<Bucket, Digest> = BTreeMap::new();
    for chunk in chunks(scope, ID_BATCH) {
        let mut rows = pg::query_sql_with_err(
            con,
            ctx,
            &digests_sql(&chunk, class_cond),
            &[dt_from.into()],
        );
        while rows.next() {
            let mut repo_id: i64 = 0;
            let mut day = String::new();
            let mut count: i64 = 0;
            let mut sum = String::new();
            fatal_on_pg_err(rows.scan(&mut [&mut repo_id, &mut day, &mut count, &mut sum]));
            res.insert((repo_id, day), (count, sum));
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
    }
    res
}

/// Go `eventIDsSQL`: event ids of one repository on the given days within the window.
fn event_ids_sql(repo_id: i64, days: &[String], class_cond: &str) -> String {
    format!(
        "select id from gha_events where repo_id = {} and created_at >= {}::timestamp and date_trunc('day', created_at)::date = any({}){} order by id",
        repo_id,
        pg::n_value(1),
        date_array_literal(days),
        class_cond
    )
}

/// Go `eventIDs`: event ids of one repository on the given days within the window (sorted).
fn event_ids(
    con: &PgConn,
    ctx: &Ctx,
    repo_id: i64,
    days: &[String],
    dt_from: &str,
    class_cond: &str,
) -> Vec<i64> {
    query_i64s(
        con,
        ctx,
        &event_ids_sql(repo_id, days, class_cond),
        &[dt_from.into()],
    )
}

/// Go `orphanEventsToSkip`: synthetic orphan push events (from the given
/// missing ones) with at least one commit SHA already present in the target
/// `gha_commits` (under any event) — copying them would count those commits twice.
fn orphan_events_to_skip(ctx: &Ctx, src: &PgConn, tgt: &PgConn, orphans: &[i64]) -> BTreeSet<i64> {
    let mut skip: BTreeSet<i64> = BTreeSet::new();
    if orphans.is_empty() {
        return skip;
    }
    let mut sha_events: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    let mut shas: Vec<String> = Vec::new();
    for chunk in chunks(orphans, ID_BATCH) {
        let mut rows = pg::query_sql_with_err(
            src,
            ctx,
            &format!(
                "select sha, event_id from gha_commits where event_id = any({}) order by sha, event_id",
                int64_array_literal(&chunk)
            ),
            &[],
        );
        while rows.next() {
            let mut sha = String::new();
            let mut eid: i64 = 0;
            fatal_on_pg_err(rows.scan(&mut [&mut sha, &mut eid]));
            let entry = sha_events.entry(sha.clone()).or_default();
            if entry.is_empty() {
                shas.push(sha);
            }
            entry.push(eid);
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
    }
    for chunk in chunks(&shas, ID_BATCH) {
        let present = query_strings(
            tgt,
            ctx,
            &format!(
                "select distinct sha from gha_commits where sha = any({})",
                text_array_literal(&chunk)
            ),
            &[],
        );
        for sha in present {
            if let Some(eids) = sha_events.get(&sha) {
                skip.extend(eids.iter().copied());
            }
        }
    }
    skip
}

/// Go `eventsToFilterOut`: the given source events the target project would
/// not ingest itself: their repository name (`dup_repo_name`) or actor
/// (`dup_actor_login`) fails the project's gha2db rules (`repo_hit`,
/// `actor_hit`).
fn events_to_filter_out(
    ctx: &Ctx,
    src: &PgConn,
    filter: &ProjectFilter,
    ids: &[i64],
) -> BTreeSet<i64> {
    let mut out: BTreeSet<i64> = BTreeSet::new();
    if filter.name.is_empty() || ids.is_empty() {
        return out;
    }
    for chunk in chunks(ids, ID_BATCH) {
        let mut rows = pg::query_sql_with_err(
            src,
            ctx,
            &format!(
                "select id, dup_repo_name, dup_actor_login from gha_events where id = any({}) order by id",
                int64_array_literal(&chunk)
            ),
            &[],
        );
        while rows.next() {
            let mut id: i64 = 0;
            let mut repo_name = String::new();
            let mut actor_login = String::new();
            fatal_on_pg_err(rows.scan(&mut [&mut id, &mut repo_name, &mut actor_login]));
            if !filter.hit(&repo_name, &actor_login) {
                out.insert(id);
            }
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
    }
    out
}

/// One `insert ... on conflict do nothing` of the buffered rows; returns the rows inserted.
fn flush_batch(
    ctx: &Ctx,
    tx: &mut PgTx<'_>,
    table: &str,
    insert_prefix: &str,
    n_columns: usize,
    args: &[SqlArg],
    rows_in_batch: usize,
) -> usize {
    let query = format!(
        "{insert_prefix}{} on conflict do nothing",
        batch_values(rows_in_batch, n_columns)
    );
    let res = match pg::exec_sql_tx(tx, ctx, &query, args) {
        Ok(res) => res,
        Err(e) => {
            // "on conflict do nothing" never raises a unique violation, so this is a real
            // problem (the transaction is aborted anyway, so no retry status applies)
            printf!(
                "reconcile_dbs: failing batch insert into {} (rows: {}, columns: {})\n",
                table,
                rows_in_batch,
                n_columns
            );
            fatal_on_pg_error(&e);
            fatalf!("reconcile_dbs: batch insert into {} failed: {}", table, e);
        }
    };
    let affected = fatal_on_pg_err(res.rows_affected());
    usize::try_from(affected).unwrap_or(0).min(rows_in_batch)
}

/// Go `copyRows`: copy the rows of `select_sql` (run on the source) into the
/// target table (same column names) with `on conflict do nothing`; returns
/// rows read and rows inserted.
fn copy_rows(
    ctx: &Ctx,
    src: &PgConn,
    tx: &mut PgTx<'_>,
    table: &str,
    select_sql: &str,
) -> (usize, usize) {
    let mut rows = pg::query_sql_with_err(src, ctx, select_sql, &[]);
    let columns = rows.column_names();
    let n_columns = columns.len();
    let cols = format!(
        "({})",
        columns
            .iter()
            .map(|col| format!("\"{col}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let eff_batch = (MAX_PARAMS / n_columns.max(1)).clamp(1, ID_BATCH);
    let insert_prefix = format!("insert into {table}{cols} ");
    let mut args: Vec<SqlArg> = Vec::with_capacity(eff_batch * n_columns);
    let mut rows_in_batch = 0usize;
    let mut n_rows = 0usize;
    let mut n_inserted = 0usize;
    while rows.next() {
        args.extend(rows.values().iter().map(SqlArg::from));
        rows_in_batch += 1;
        if rows_in_batch >= eff_batch {
            n_inserted += flush_batch(
                ctx,
                tx,
                table,
                &insert_prefix,
                n_columns,
                &args,
                rows_in_batch,
            );
            n_rows += rows_in_batch;
            args.clear();
            rows_in_batch = 0;
        }
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    if rows_in_batch > 0 {
        n_inserted += flush_batch(
            ctx,
            tx,
            table,
            &insert_prefix,
            n_columns,
            &args,
            rows_in_batch,
        );
        n_rows += rows_in_batch;
    }
    (n_rows, n_inserted)
}

/// Go `countRows`: `select count(*) ...` on the source (dry run).
fn count_rows(ctx: &Ctx, src: &PgConn, from_sql: &str) -> usize {
    let mut n: i64 = 0;
    fatal_on_pg_err(
        pg::query_row_sql(src, ctx, &format!("select count(*) {from_sql}"), &[])
            .scan(&mut [&mut n]),
    );
    usize::try_from(n).unwrap_or(0)
}

/// Go `dimensionSelect`: `from ...` part selecting the dimension rows
/// referenced by the events `ids_lit` (empty = the table is not copied:
/// `gha_actors` with a shared affiliations database).
fn dimension_select(ctx: &Ctx, table: &str, ids_lit: &str) -> String {
    match table {
        "gha_repos" => format!("from gha_repos where id in (select repo_id from gha_events where id = any({ids_lit}))"),
        "gha_orgs" => format!(
            "from gha_orgs where id in (select org_id from gha_events where id = any({ids_lit}) and org_id is not null)"
        ),
        "gha_labels" => format!(
            "from gha_labels where id in (select label_id from gha_issues_labels where event_id = any({ids_lit}))"
        ),
        "gha_actors" if ctx.affiliations_db.is_empty() => {
            format!("from gha_actors where id in (select actor_id from gha_events where id = any({ids_lit}))")
        }
        _ => String::new(),
    }
}

/// Go `dimensionColumns`: copied columns of a dimension table (`*` = all).
fn dimension_columns(table: &str) -> &'static str {
    if table == "gha_repos" {
        "id, name, org_id, org_login"
    } else {
        "*"
    }
}

/// Go `takeOver`: the commits of the copied GHA push events take over the rows
/// the orphan restore wrote for the same SHAs under synthetic (negative id)
/// events: those rows are deleted (roles first), emptied synthetic events are
/// removed with their payloads, texts and files, partially emptied ones are
/// returned for the postprocess. Returns: commits taken over, synthetic
/// events they were taken from, removed synthetic events, events to postprocess.
fn take_over(ctx: &Ctx, tx: &mut PgTx<'_>, native_lit: &str) -> (usize, usize, usize, Vec<i64>) {
    let mut shas: Vec<String> = Vec::new();
    {
        let mut rows = pg::query_sql_tx_with_err(
            tx,
            ctx,
            &format!("select distinct sha from gha_commits where event_id = any({native_lit}) order by sha"),
            &[],
        );
        while rows.next() {
            let mut sha = String::new();
            fatal_on_pg_err(rows.scan(&mut [&mut sha]));
            shas.push(sha);
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
    }
    if shas.is_empty() {
        return (0, 0, 0, Vec::new());
    }
    let mut n_taken = 0usize;
    let mut synth: BTreeSet<i64> = BTreeSet::new();
    for chunk in chunks(&shas, ID_BATCH) {
        let shas_lit = text_array_literal(&chunk);
        pg::exec_sql_tx_with_err(
            tx,
            ctx,
            &format!("delete from gha_commits_roles where event_id < 0 and sha = any({shas_lit})"),
            &[],
        );
        let mut rows = pg::query_sql_tx_with_err(
            tx,
            ctx,
            &format!("delete from gha_commits where event_id < 0 and sha = any({shas_lit}) returning event_id"),
            &[],
        );
        while rows.next() {
            let mut eid: i64 = 0;
            fatal_on_pg_err(rows.scan(&mut [&mut eid]));
            synth.insert(eid);
            n_taken += 1;
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
    }
    if n_taken == 0 {
        return (0, 0, 0, Vec::new());
    }
    let mut n_removed = 0usize;
    let mut pp_eids: Vec<i64> = Vec::new();
    for &sid in &synth {
        let mut left: i64 = 0;
        fatal_on_pg_err(
            pg::query_row_sql_tx(
                tx,
                ctx,
                &format!(
                    "select count(*) from gha_commits where event_id = {}",
                    pg::n_value(1)
                ),
                &[sid.into()],
            )
            .scan(&mut [&mut left]),
        );
        if left > 0 {
            pp_eids.push(sid);
            continue;
        }
        for q in [
            format!("delete from gha_texts where event_id = {}", pg::n_value(1)),
            format!(
                "delete from gha_events_commits_files where event_id = {}",
                pg::n_value(1)
            ),
            format!(
                "delete from gha_payloads where event_id = {}",
                pg::n_value(1)
            ),
            format!("delete from gha_events where id = {}", pg::n_value(1)),
        ] {
            pg::exec_sql_tx_with_err(tx, ctx, &q, &[sid.into()]);
        }
        n_removed += 1;
    }
    (n_taken, synth.len(), n_removed, pp_eids)
}

/// Go `isDistinctSQL`: `is_distinct` of the copied commits: true only when no
/// other pre-existing row has the same SHA and the row is the first (lowest
/// event id) among the copied rows of that SHA.
fn is_distinct_sql(ids_lit: &str) -> String {
    format!(
        "update gha_commits c set is_distinct = (not exists (select 1 from gha_commits o where o.sha = c.sha and o.event_id <> c.event_id and not (o.event_id = any({ids_lit}))) and c.event_id = (select min(n.event_id) from gha_commits n where n.sha = c.sha and n.event_id = any({ids_lit}))) where c.event_id = any({ids_lit})"
    )
}

/// Go `reconcileSource`: reconcile the target from one source database.
#[allow(clippy::too_many_arguments)]
fn reconcile_source(
    ctx: &Ctx,
    cfg: &Config,
    tgt: &PgConn,
    src: &PgConn,
    target: &str,
    source: &str,
    target_repos: &[i64],
    dt_from: &str,
) -> SourceStats {
    let mut stats = SourceStats::default();
    let prefix = format!("reconcile_dbs: {target} <- {source}");
    let source_repos = repo_ids(src, ctx);
    let scope = intersect_sorted(target_repos, &source_repos);
    printf!(
        "{}: scope {} repo(s) (target {}, source {})\n",
        prefix,
        scope.len(),
        target_repos.len(),
        source_repos.len()
    );
    if scope.is_empty() {
        return stats;
    }
    let class_cond = class_condition(cfg.artificial);
    let source_digests = digests(src, ctx, &scope, dt_from, &class_cond);
    let target_digests = digests(tgt, ctx, &scope, dt_from, &class_cond);
    let differing = differing_buckets(&source_digests, &target_digests);
    printf!(
        "{}: buckets: source {}, target {}, differing {}\n",
        prefix,
        source_digests.len(),
        target_digests.len(),
        differing.len()
    );
    if differing.is_empty() {
        return stats;
    }

    // Missing events: per repository with differing days
    let mut repo_days: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    let mut repo_order: Vec<i64> = Vec::new();
    for (repo_id, day) in differing {
        let entry = repo_days.entry(repo_id).or_default();
        if entry.is_empty() {
            repo_order.push(repo_id);
        }
        entry.push(day);
    }
    let mut missing: Vec<i64> = Vec::new();
    let mut target_only = 0usize;
    for repo_id in repo_order {
        let days = &repo_days[&repo_id];
        let source_ids = event_ids(src, ctx, repo_id, days, dt_from, &class_cond);
        let target_ids = event_ids(tgt, ctx, repo_id, days, dt_from, &class_cond);
        missing.extend(diff_sorted(&source_ids, &target_ids));
        target_only += diff_sorted(&target_ids, &source_ids).len();
    }
    missing.sort_unstable();
    for &id in &missing {
        match class_of(id) {
            "native" => stats.native += 1,
            "orphan" => stats.orphan += 1,
            "artificial" => stats.artificial += 1,
            _ => {}
        }
    }
    printf!(
        "{}: events: source-only {} (native {}, orphan {}, artificial {}), target-only {}\n",
        prefix,
        missing.len(),
        stats.native,
        stats.orphan,
        stats.artificial,
        target_only
    );
    if missing.is_empty() {
        return stats;
    }

    // Events the target project would not ingest itself (its gha2db org/repo/actor rules) are not copied
    let drop = events_to_filter_out(ctx, src, &cfg.filter, &missing);
    if !drop.is_empty() {
        let mut to_copy: Vec<i64> = Vec::with_capacity(missing.len());
        let (mut f_native, mut f_orphan, mut f_artificial) = (0usize, 0usize, 0usize);
        for id in missing {
            if drop.contains(&id) {
                stats.filtered += 1;
                match class_of(id) {
                    "native" => {
                        stats.native = stats.native.saturating_sub(1);
                        f_native += 1;
                    }
                    "orphan" => {
                        stats.orphan = stats.orphan.saturating_sub(1);
                        f_orphan += 1;
                    }
                    "artificial" => {
                        stats.artificial = stats.artificial.saturating_sub(1);
                        f_artificial += 1;
                    }
                    _ => {}
                }
                continue;
            }
            to_copy.push(id);
        }
        missing = to_copy;
        printf!(
            "{}: filtered out {} event(s) outside project '{}' org/repo/actor rules (native {}, orphan {}, artificial {})\n",
            prefix,
            stats.filtered,
            cfg.filter.name,
            f_native,
            f_orphan,
            f_artificial
        );
    }
    if missing.is_empty() {
        return stats;
    }
    let orphans: Vec<i64> = missing
        .iter()
        .copied()
        .filter(|&id| class_of(id) == "orphan")
        .collect();

    // Orphan pushes whose commits the target already has are not copied
    let skip = orphan_events_to_skip(ctx, src, tgt, &orphans);
    if !skip.is_empty() {
        let mut to_copy: Vec<i64> = Vec::with_capacity(missing.len());
        for id in missing {
            if skip.contains(&id) {
                stats.skipped += 1;
                stats.orphan = stats.orphan.saturating_sub(1);
                continue;
            }
            to_copy.push(id);
        }
        missing = to_copy;
        printf!(
            "{}: skipped {} orphan event(s) whose commits are already present\n",
            prefix,
            stats.skipped
        );
    }
    if missing.is_empty() {
        return stats;
    }

    // Copy in batches, one transaction per batch
    let verb = if cfg.dry_run { "would copy" } else { "copied" };
    let mut pp_eids: Vec<i64> = Vec::new();
    for chunk in chunks(&missing, ID_BATCH) {
        let ids_lit = int64_array_literal(&chunk);
        let native: Vec<i64> = chunk
            .iter()
            .copied()
            .filter(|&id| class_of(id) == "native")
            .collect();
        if cfg.dry_run {
            for (name, key) in EVENT_TABLES {
                let n = count_rows(
                    ctx,
                    src,
                    &format!("from {name} where {key} = any({ids_lit})"),
                );
                stats.add_table_stats(name, n, 0);
            }
            for table in DIM_TABLES {
                let sel = dimension_select(ctx, table, &ids_lit);
                if sel.is_empty() {
                    continue;
                }
                let n = count_rows(ctx, src, &sel);
                stats.add_table_stats(table, n, 0);
            }
            stats.copied += chunk.len();
            pp_eids.extend(chunk.iter().copied());
            continue;
        }
        let mut tx = fatal_on_err(tgt.begin());
        for (name, key) in EVENT_TABLES {
            let (n, ins) = copy_rows(
                ctx,
                src,
                &mut tx,
                name,
                &format!("select * from {name} where {key} = any({ids_lit})"),
            );
            stats.add_table_stats(name, n, ins);
        }
        for table in DIM_TABLES {
            let sel = dimension_select(ctx, table, &ids_lit);
            if sel.is_empty() {
                continue;
            }
            let (n, ins) = copy_rows(
                ctx,
                src,
                &mut tx,
                table,
                &format!("select {} {}", dimension_columns(table), sel),
            );
            stats.add_table_stats(table, n, ins);
        }
        if !native.is_empty() {
            let (n_taken, n_from, n_removed, partial) =
                take_over(ctx, &mut tx, &int64_array_literal(&native));
            stats.taken += n_taken;
            stats.taken_from += n_from;
            stats.removed += n_removed;
            pp_eids.extend(partial);
        }
        pg::exec_sql_tx_with_err(&mut tx, ctx, &is_distinct_sql(&ids_lit), &[]);
        fatal_on_err(tx.commit());
        stats.copied += chunk.len();
        pp_eids.extend(chunk.iter().copied());
    }
    for (name, _) in EVENT_TABLES {
        if let Some(&(rows, inserted)) = stats.tables.get(*name) {
            if rows > 0 {
                printf!(
                    "{}: table {}: rows {}, inserted {}\n",
                    prefix,
                    name,
                    rows,
                    inserted
                );
            }
        }
    }
    for table in DIM_TABLES {
        if let Some(&(rows, inserted)) = stats.tables.get(*table) {
            if rows > 0 {
                printf!(
                    "{}: table {}: rows {}, inserted {}\n",
                    prefix,
                    table,
                    rows,
                    inserted
                );
            }
        }
    }
    if stats.taken > 0 {
        printf!(
            "{}: taken over {} commit(s) from {} restored push event(s), removed {} emptied restored event(s)\n",
            prefix,
            stats.taken,
            stats.taken_from,
            stats.removed
        );
    }
    pp_eids.sort_unstable();
    stats.postprocess = pp_eids.len();
    printf!(
        "{}: {} {} event(s), inserted {} row(s), postprocess {} event id(s)\n",
        prefix,
        verb,
        stats.copied,
        stats.inserted,
        stats.postprocess
    );
    if !cfg.dry_run && !pp_eids.is_empty() {
        restore::run_event_ids_postprocess_db(ctx, "", &pp_eids);
    }
    stats
}

fn reconcile_dbs() {
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    let target = ctx.pg_db.clone();
    if target.is_empty() {
        fatalf!("reconcile_dbs: target database required (PG_DB)");
    }
    let Some(cfg) = resolve_config(&ctx, &target) else {
        return;
    };

    // Connect to the target database
    let tgt = pg::pg_conn_db(&mut ctx, &target);
    let dt_from = gotime::to_ymdhms_date(gotime::get_date_ago(
        &tgt,
        &ctx,
        Local::now(),
        &cfg.range_str,
    ));
    printf!(
        "reconcile_dbs: {}: mode {} ({}), {} source(s): {}, since {} (range '{}'), classes: {}, dry run: {}\n",
        target,
        cfg.mode,
        cfg.detail,
        cfg.sources.len(),
        cfg.sources.join(", "),
        dt_from,
        cfg.range_str,
        classes_info(cfg.artificial),
        cfg.dry_run
    );
    printf!("reconcile_dbs: {}: filter: {}\n", target, cfg.filter.info());
    let target_repos = repo_ids(&tgt, &ctx);

    let mut total = SourceStats::default();
    let mut n_sources = 0usize;
    for source in &cfg.sources {
        let src = match connect_source(&mut ctx, source, !cfg.explicit) {
            Ok(c) => c,
            Err(e) => {
                if is_no_db_error(&e) {
                    printf!(
                        "reconcile_dbs: {} <- {}: source database unavailable, skipping: {}\n",
                        target,
                        source,
                        e
                    );
                    continue;
                }
                fatal_on_pg_error(&e);
                fatalf!(
                    "reconcile_dbs: cannot connect to source database '{}': {}",
                    source,
                    e
                );
            }
        };
        let stats = reconcile_source(
            &ctx,
            &cfg,
            &tgt,
            &src,
            &target,
            source,
            &target_repos,
            &dt_from,
        );
        src.close();
        n_sources += 1;
        total.copied += stats.copied;
        total.inserted += stats.inserted;
        total.filtered += stats.filtered;
        total.skipped += stats.skipped;
        total.taken += stats.taken;
        total.postprocess += stats.postprocess;
    }
    let verb = if cfg.dry_run { "would copy" } else { "copied" };
    printf!(
        "reconcile_dbs: {}: {} source(s), {} {} event(s), inserted {} row(s), filtered out {} event(s), skipped {} orphan event(s), taken over {} commit(s)\n",
        target,
        n_sources,
        verb,
        total.copied,
        total.inserted,
        total.filtered,
        total.skipped,
        total.taken
    );
    tgt.close();
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    reconcile_dbs();
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use devstatscode::yamlv2::de as yde;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn env_flag_values() {
        let name = "GHA2DB_RECONCILE_TEST_FLAG";
        for (value, expected) in [
            ("", false),
            ("0", false),
            ("false", false),
            ("no", false),
            ("1", true),
            (" 1 ", true),
            ("t", true),
            ("TRUE", true),
            ("yes", true),
            ("Y", true),
        ] {
            std::env::set_var(name, value);
            assert_eq!(env_flag(name), expected, "env_flag({value:?})");
        }
        std::env::remove_var(name);
    }

    #[test]
    fn parse_db_list_cases() {
        assert_eq!(parse_db_list(""), Vec::<String>::new());
        assert_eq!(parse_db_list(" , ,"), Vec::<String>::new());
        assert_eq!(parse_db_list("a"), s(&["a"]));
        assert_eq!(parse_db_list("a,b"), s(&["a", "b"]));
        assert_eq!(parse_db_list(" b , a ,b,,a"), s(&["b", "a"]));
    }

    #[test]
    fn class_of_cases() {
        assert_eq!(class_of(-1), "orphan");
        assert_eq!(class_of(i64::MIN), "orphan");
        assert_eq!(class_of(0), "zero");
        assert_eq!(class_of(1), "native");
        assert_eq!(class_of(ARTIFICIAL_ID_BASE - 1), "native");
        assert_eq!(class_of(ARTIFICIAL_ID_BASE), "artificial");
        assert_eq!(class_of(ARTIFICIAL_ID_BASE + 4000000000000), "artificial");
        assert_eq!(class_of(i64::MAX), "artificial");
    }

    #[test]
    fn array_literals() {
        assert_eq!(int64_array_literal(&[]), "'{}'::bigint[]");
        assert_eq!(
            int64_array_literal(&[-5, 0, 7, ARTIFICIAL_ID_BASE]),
            "'{-5,0,7,281474976710656}'::bigint[]"
        );
        assert_eq!(text_array_literal(&[]), "'{}'::text[]");
        assert_eq!(
            text_array_literal(&s(&["abc", "a\"b", "a\\b", "a'b", "a,b", "a b", "żółw"])),
            r#"'{"abc","a\"b","a\\b","a''b","a,b","a b","żółw"}'::text[]"#
        );
        assert_eq!(
            date_array_literal(&s(&["2026-01-02", "2026-01-03"])),
            "'{2026-01-02,2026-01-03}'::date[]"
        );
    }

    #[test]
    fn batch_values_cases() {
        assert_eq!(batch_values(1, 1), "values ($1)");
        assert_eq!(batch_values(1, 3), "values ($1,$2,$3)");
        assert_eq!(batch_values(2, 2), "values ($1,$2),($3,$4)");
        assert_eq!(batch_values(3, 1), "values ($1),($2),($3)");
    }

    #[test]
    fn sorted_set_ops() {
        type Case = (
            &'static [i64],
            &'static [i64],
            &'static [i64],
            &'static [i64],
        );
        let cases: &[Case] = &[
            (&[], &[], &[], &[]),
            (&[1, 2, 3], &[], &[], &[1, 2, 3]),
            (&[], &[1, 2, 3], &[], &[]),
            (&[1, 2, 3], &[2, 3, 4], &[2, 3], &[1]),
            (&[-3, -1, 5, 9], &[-1, 9, 10], &[-1, 9], &[-3, 5]),
            (&[1, 1, 2, 2, 3], &[2, 2], &[2], &[1, 3]),
            (&[1, 2, 3], &[1, 2, 3], &[1, 2, 3], &[]),
        ];
        for (a, b, inter, diff) in cases {
            assert_eq!(intersect_sorted(a, b), *inter, "intersect {a:?} {b:?}");
            assert_eq!(diff_sorted(a, b), *diff, "diff {a:?} {b:?}");
        }
    }

    #[test]
    fn differing_buckets_cases() {
        type Item = ((i64, &'static str), (i64, &'static str));
        let d = |v: &[Item]| -> BTreeMap<Bucket, Digest> {
            v.iter()
                .map(|((r, day), (c, sum))| ((*r, day.to_string()), (*c, sum.to_string())))
                .collect()
        };
        let source = d(&[
            ((2, "2026-01-02"), (3, "30")),
            ((2, "2026-01-01"), (2, "20")),
            ((1, "2026-01-03"), (1, "10")),
            ((1, "2026-01-01"), (5, "50")),
            ((3, "2026-01-01"), (1, "-7")),
        ]);
        let target = d(&[
            ((2, "2026-01-02"), (3, "30")),
            ((2, "2026-01-01"), (2, "21")),
            ((1, "2026-01-03"), (2, "10")),
            ((3, "2026-01-01"), (1, "-7")),
            ((4, "2026-01-01"), (9, "99")),
        ]);
        assert_eq!(
            differing_buckets(&source, &target),
            vec![
                (1, "2026-01-01".to_string()),
                (1, "2026-01-03".to_string()),
                (2, "2026-01-01".to_string())
            ]
        );
        assert!(differing_buckets(&BTreeMap::new(), &target).is_empty());
        assert!(differing_buckets(&source, &source).is_empty());
    }

    #[test]
    fn chunks_cases() {
        assert!(chunks::<i64>(&[], 2).is_empty());
        assert_eq!(
            chunks(&[1i64, 2, 3, 4, 5], 2),
            vec![vec![1, 2], vec![3, 4], vec![5]]
        );
        assert_eq!(chunks(&[1i64, 2], 5), vec![vec![1, 2]]);
        assert_eq!(
            chunks(&s(&["a", "b", "c"]), 2),
            vec![s(&["a", "b"]), s(&["c"])]
        );
    }

    #[test]
    fn class_condition_and_info() {
        assert_eq!(class_condition(false), " and id < 281474976710656");
        assert_eq!(class_condition(true), "");
        assert_eq!(classes_info(false), "native, orphan");
        assert_eq!(classes_info(true), "native, orphan, artificial");
    }

    #[test]
    fn no_db_error_detection() {
        assert!(is_no_db_error(&PgError::Other(
            "pq: database \"nodb\" does not exist".to_string()
        )));
        assert!(!is_no_db_error(&PgError::Other(
            "connection refused".to_string()
        )));
        assert!(!is_no_db_error(&PgError::BadConn));
    }

    #[test]
    fn sql_builders() {
        assert_eq!(
            digests_sql(&[1, 2], &class_condition(false)),
            "select repo_id, date_trunc('day', created_at)::date::text, count(*), sum(id)::text from gha_events where created_at >= $1::timestamp and repo_id = any('{1,2}'::bigint[]) and id < 281474976710656 group by 1, 2"
        );
        assert_eq!(
            event_ids_sql(7, &s(&["2026-01-01", "2026-01-05"]), ""),
            "select id from gha_events where repo_id = 7 and created_at >= $1::timestamp and date_trunc('day', created_at)::date = any('{2026-01-01,2026-01-05}'::date[]) order by id"
        );
        let got = is_distinct_sql("'{1}'::bigint[]");
        for part in [
            "update gha_commits c set is_distinct = (",
            "not exists (select 1 from gha_commits o where o.sha = c.sha and o.event_id <> c.event_id and not (o.event_id = any('{1}'::bigint[])))",
            "min(n.event_id)",
            ") where c.event_id = any('{1}'::bigint[])",
        ] {
            assert!(got.contains(part), "{got} should contain {part}");
        }
    }

    #[test]
    fn dimension_selects() {
        let mut ctx = Ctx::default();
        let ids = "'{1,2}'::bigint[]";
        ctx.affiliations_db = String::new();
        assert_eq!(
            dimension_select(&ctx, "gha_actors", ids),
            "from gha_actors where id in (select actor_id from gha_events where id = any('{1,2}'::bigint[]))"
        );
        ctx.affiliations_db = "affiliations".to_string();
        assert_eq!(dimension_select(&ctx, "gha_actors", ids), "");
        assert_eq!(
            dimension_select(&ctx, "gha_repos", ids),
            "from gha_repos where id in (select repo_id from gha_events where id = any('{1,2}'::bigint[]))"
        );
        assert_eq!(
            dimension_select(&ctx, "gha_orgs", ids),
            "from gha_orgs where id in (select org_id from gha_events where id = any('{1,2}'::bigint[]) and org_id is not null)"
        );
        assert_eq!(
            dimension_select(&ctx, "gha_labels", ids),
            "from gha_labels where id in (select label_id from gha_issues_labels where event_id = any('{1,2}'::bigint[]))"
        );
        assert_eq!(dimension_select(&ctx, "gha_texts", ids), "");
        assert_eq!(
            dimension_columns("gha_repos"),
            "id, name, org_id, org_login"
        );
        assert_eq!(dimension_columns("gha_orgs"), "*");
    }

    #[test]
    fn event_tables_order() {
        assert_eq!(EVENT_TABLES[0], ("gha_events", "id"));
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for (i, (name, key)) in EVENT_TABLES.iter().enumerate() {
            if i > 0 {
                assert_eq!(*key, "event_id", "{name}");
            }
            assert!(seen.insert(name), "duplicate table {name}");
        }
        assert_eq!(EVENT_TABLES.len(), 21);
        assert_eq!(
            DIM_TABLES,
            &["gha_repos", "gha_orgs", "gha_labels", "gha_actors"]
        );
    }

    fn test_projects() -> projects::AllProjects {
        let yaml = r#"
projects:
  zeta: {psql_db: zeta, shared_db: allprj, order: 5}
  alpha: {psql_db: alpha, shared_db: allprj, order: 5}
  beta: {psql_db: beta, shared_db: allprj, order: 1}
  gamma: {psql_db: gamma, shared_db: allprj, order: 3, disabled: true}
  delta: {psql_db: beta, shared_db: allprj, order: 0}
  all: {psql_db: allprj, order: 2}
  kube: {psql_db: gha, order: 0}
  other: {psql_db: other, shared_db: otherprj, order: 9}
  selfref: {psql_db: allprj, shared_db: allprj, order: 9}
  noshared: {psql_db: solo, order: 9}
"#;
        yde::unmarshal(yaml.as_bytes()).expect("test projects.yaml")
    }

    #[test]
    fn shared_db_sources_cases() {
        let mut ctx = Ctx::default();
        let all = test_projects();
        // delta (order 0, db beta), beta (1, beta - dup), gamma disabled, alpha (5), zeta (5)
        assert_eq!(
            shared_db_sources(&ctx, &all, "allprj"),
            s(&["beta", "alpha", "zeta"])
        );
        ctx.projects_override = [("gamma".to_string(), true), ("zeta".to_string(), false)]
            .into_iter()
            .collect();
        assert_eq!(
            shared_db_sources(&ctx, &all, "allprj"),
            s(&["beta", "gamma", "alpha"])
        );
        assert!(shared_db_sources(&ctx, &all, "gha").is_empty());
        assert_eq!(shared_db_sources(&ctx, &all, "otherprj"), s(&["other"]));
    }
}
