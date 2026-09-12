//! `splitcrons` — Rust port of `cmd/splitcrons/splitcrons.go`.
//!
//! Reads `devstats-helm/values.yaml`, recomputes the sync (`cronTest` /
//! `cronProd`) and affiliations (`affCronTest` / `affCronProd`) cron schedules
//! of every project so they are spread over the sync period, pushes the new
//! schedules, suspend flags and (optionally) env values to the live cronjobs
//! with `kubectl patch`, and writes the updated values file.
//!
//! ```text
//! splitcrons path/to/devstats-helm/values.yaml new-values.yaml
//! ```
//!
//! All environment knobs of the Go program are supported with the same
//! semantics (`MONTHLY`, `SYNC_HOURS`, `GHA_OFFSET`, `NEVER_PATCH`,
//! `ALWAYS_PATCH`, `ONLY_ENV`, `ONLY_SUSPEND`, `SUSPEND_ALL`, `NO_SUSPEND_H`,
//! `NO_SUSPEND_A`, `SKIP_AFFS_ENV`, `SKIP_SYNC_ENV`, `ONLY_PROD`, `ONLY_TEST`,
//! `OLD_ALGORITHM`, `NO_DB_SIZES`, `SPLIT_ALGO`, `WEIGHT_POWER`, `DAILY_RANGE`,
//! `DAILY_REPOS_RANGE`, `DAILY_PROJECTS`, `NO_AFFS_ANCHOR`,
//! `DAILY_AFFS_OFFSET_HOURS`, `PATCH_ENV`, `SIZES_POD`, `CTX_TEST`, `CTX_PROD`,
//! `KUBERNETES_HOURS`, `ALL_HOURS`, `OFFSET_HOURS`, `DEBUG`), the stdout report
//! is identical, and the written YAML is byte-identical to `gopkg.in/yaml.v2`
//! output (see `devstatscode::yamlv2`).

use std::collections::{BTreeMap, HashMap, HashSet};

use devstatscode::yamlv2::de as yde;
use devstatscode::yamlv2::{marshal, MapBuilder, Node};
use devstatscode::{
    exec, fatal_on_err, fatalf, gofmt, gomath, io, json, signal, time as gotime, Ctx,
};
use serde::Deserialize;

/// One `projects:` entry of `values.yaml` (Go `devstatsProject`; yaml.v2 decoding rules).
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
struct Project {
    #[serde(deserialize_with = "yde::string")]
    proj: String,
    #[serde(deserialize_with = "yde::string")]
    url: String,
    #[serde(deserialize_with = "yde::string")]
    db: String,
    #[serde(deserialize_with = "yde::string")]
    icon: String,
    #[serde(deserialize_with = "yde::string")]
    org: String,
    #[serde(deserialize_with = "yde::string")]
    repo: String,
    #[serde(rename = "cronTest", deserialize_with = "yde::string")]
    cron_test: String,
    #[serde(rename = "cronProd", deserialize_with = "yde::string")]
    cron_prod: String,
    #[serde(rename = "affCronTest", deserialize_with = "yde::string")]
    aff_cron_test: String,
    #[serde(rename = "affCronProd", deserialize_with = "yde::string")]
    aff_cron_prod: String,
    #[serde(rename = "suspendCronTest", deserialize_with = "yde::boolean")]
    suspend_cron_test: bool,
    #[serde(rename = "suspendCronProd", deserialize_with = "yde::boolean")]
    suspend_cron_prod: bool,
    #[serde(rename = "affSkipTemp", deserialize_with = "yde::string")]
    aff_skip_temp: String,
    #[serde(deserialize_with = "yde::string")]
    disk: String,
    #[serde(deserialize_with = "yde::int_array::<_, 4>")]
    domains: [i64; 4],
    #[serde(deserialize_with = "yde::string")]
    ga: String,
    #[serde(deserialize_with = "yde::int")]
    i: i64,
    #[serde(rename = "certNum", deserialize_with = "yde::int")]
    cert_num: i64,
    #[serde(rename = "maxHist", deserialize_with = "yde::int")]
    max_hist: i64,
    #[serde(rename = "skipAffsLock", deserialize_with = "yde::int")]
    skip_affs_lock: i64,
    #[serde(rename = "affsLockDB", deserialize_with = "yde::string")]
    affs_lock_db: String,
    #[serde(rename = "noDurable", deserialize_with = "yde::int")]
    no_durable: i64,
    #[serde(rename = "durablePQ", deserialize_with = "yde::int")]
    durable_pq: i64,
    #[serde(rename = "maxRunDuration", deserialize_with = "yde::string")]
    max_run_duration: String,
    #[serde(rename = "skipGHAPI", deserialize_with = "yde::int")]
    skip_ghapi: i64,
    #[serde(rename = "skipGetRepos", deserialize_with = "yde::int")]
    skip_get_repos: i64,
    #[serde(rename = "skipUpdAffs", deserialize_with = "yde::int")]
    skip_upd_affs: i64,
    #[serde(rename = "skipImpAffs", deserialize_with = "yde::int")]
    skip_imp_affs: i64,
    #[serde(deserialize_with = "yde::boolean")]
    archived: bool,
    #[serde(rename = "recentRange", deserialize_with = "yde::string")]
    recent_range: String,
    #[serde(rename = "orphanCommitsRange", deserialize_with = "yde::string")]
    orphan_commits_range: String,
    #[serde(rename = "recentReposRange", deserialize_with = "yde::string")]
    recent_repos_range: String,
}

impl Project {
    /// yaml.v2 marshalling of `devstatsProject`: field order and `omitempty` as in the Go struct tags.
    fn to_node(&self) -> Node {
        MapBuilder::new()
            .field("proj", Node::str(&self.proj))
            .field("url", Node::str(&self.url))
            .field("db", Node::str(&self.db))
            .field("icon", Node::str(&self.icon))
            .field("org", Node::str(&self.org))
            .field("repo", Node::str(&self.repo))
            .field("cronTest", Node::str(&self.cron_test))
            .field("cronProd", Node::str(&self.cron_prod))
            .field("affCronTest", Node::str(&self.aff_cron_test))
            .field("affCronProd", Node::str(&self.aff_cron_prod))
            .field_omitempty("suspendCronTest", Node::Bool(self.suspend_cron_test))
            .field_omitempty("suspendCronProd", Node::Bool(self.suspend_cron_prod))
            .field("affSkipTemp", Node::str(&self.aff_skip_temp))
            .field("disk", Node::str(&self.disk))
            .field(
                "domains",
                Node::FlowSeq(self.domains.iter().map(|d| Node::Int(*d)).collect()),
            )
            .field("ga", Node::str(&self.ga))
            .field("i", Node::Int(self.i))
            .field("certNum", Node::Int(self.cert_num))
            .field_omitempty("maxHist", Node::Int(self.max_hist))
            .field_omitempty("skipAffsLock", Node::Int(self.skip_affs_lock))
            .field_omitempty("affsLockDB", Node::str(&self.affs_lock_db))
            .field_omitempty("noDurable", Node::Int(self.no_durable))
            .field_omitempty("durablePQ", Node::Int(self.durable_pq))
            .field_omitempty("maxRunDuration", Node::str(&self.max_run_duration))
            .field_omitempty("skipGHAPI", Node::Int(self.skip_ghapi))
            .field_omitempty("skipGetRepos", Node::Int(self.skip_get_repos))
            .field_omitempty("skipUpdAffs", Node::Int(self.skip_upd_affs))
            .field_omitempty("skipImpAffs", Node::Int(self.skip_imp_affs))
            .field_omitempty("archived", Node::Bool(self.archived))
            .field_omitempty("recentRange", Node::str(&self.recent_range))
            .field_omitempty("orphanCommitsRange", Node::str(&self.orphan_commits_range))
            .field_omitempty("recentReposRange", Node::str(&self.recent_repos_range))
            .build()
    }
}

/// The parts of `values.yaml` this tool reads and rewrites (Go `devstatsValues`).
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
struct Values {
    #[serde(rename = "nSyncCPUs", deserialize_with = "yde::int")]
    sync_cpus: i64,
    #[serde(rename = "nAffsCPUs", deserialize_with = "yde::int")]
    affs_cpus: i64,
    #[serde(rename = "affiliationsImportCron", deserialize_with = "yde::string")]
    import_cron: String,
    #[serde(
        rename = "affiliationsImportCronTest",
        deserialize_with = "yde::string"
    )]
    import_cron_test: String,
    #[serde(deserialize_with = "yde::seq")]
    projects: Vec<Project>,
}

impl Values {
    fn to_node(&self) -> Node {
        MapBuilder::new()
            .field("nSyncCPUs", Node::Int(self.sync_cpus))
            .field("nAffsCPUs", Node::Int(self.affs_cpus))
            .field_omitempty("affiliationsImportCron", Node::str(&self.import_cron))
            .field_omitempty(
                "affiliationsImportCronTest",
                Node::str(&self.import_cron_test),
            )
            .field(
                "projects",
                Node::Seq(self.projects.iter().map(Project::to_node).collect()),
            )
            .build()
    }
}

/// hours in week (Go `cWeekHours`)
const WEEK_HOURS: f64 = 24.0 * 7.0;
/// minutes in week (Go `cWeekMinutes`)
const WEEK_MINUTES: f64 = 60.0 * 24.0 * 7.0;

/// The Go program's package-level state (`gXxx` variables and `ctx`).
#[derive(Debug, Default)]
struct State {
    ctx: Ctx,
    debug: bool,
    patched: i64,
    attempted: i64,
    never: bool,
    always: bool,
    only_env: bool,
    only_suspend: bool,
    suspend_all: bool,
    no_suspend_a: bool,
    no_suspend_h: bool,
    monthly: bool,
    skip_affs_env: bool,
    skip_sync_env: bool,
    only_prod: bool,
    only_test: bool,
    old_algorithm: bool,
    no_db_sizes: bool,
    weight_power: f64,
    split_algo: String,
    daily_projs: HashSet<String>,
    daily_range: String,
    daily_repos: String,
    affs_anchor: bool,
    daily_affs_off: i64,
    /// `PATCH_ENV` names; `None` when the variable is unset (Go: nil map).
    patch_env: Option<HashSet<String>>,
}

/// `os.Getenv`
fn env_str(name: &str) -> String {
    std::env::var_os(name)
        .map(|v| v.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// `os.Getenv(name) != ""`
fn env_set(name: &str) -> bool {
    !env_str(name).is_empty()
}

/// Go `%.<prec>f` (`+Inf` / `-Inf` / `NaN` for the non-finite values).
fn gof(v: f64, prec: usize) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        format!("{v:.prec$}")
    }
}

/// Go `%<width>.<prec>f` (right aligned, spaces).
fn gofw(v: f64, prec: usize, width: usize) -> String {
    format!("{:>width$}", gof(v, prec))
}

/// Go `%0<width>.<prec>f` (zero padded; Go pads non-finite values with spaces).
fn gof0(v: f64, prec: usize, width: usize) -> String {
    if v.is_finite() {
        format!("{v:0width$.prec$}")
    } else {
        gofw(v, prec, width)
    }
}

/// kubectl `--context` args for a namespace: `CTX_TEST` / `CTX_PROD` override
/// the defaults `test` / `prod`; `-` means no `--context` at all.
fn ctx_args_for_namespace(namespace: &str) -> Vec<String> {
    let (mut kctx, def) = if namespace.ends_with("-test") {
        (env_str("CTX_TEST"), "test")
    } else if namespace.ends_with("-prod") {
        (env_str("CTX_PROD"), "prod")
    } else {
        (String::new(), "")
    };
    if kctx.is_empty() {
        kctx = def.to_string();
    }
    if kctx == "-" || kctx.is_empty() {
        return Vec::new();
    }
    vec!["--context".to_string(), kctx]
}

fn kubectl_args(verb: &[&str], namespace: &str, rest: &[&str]) -> Vec<String> {
    let mut cmd: Vec<String> = vec!["kubectl".to_string()];
    cmd.extend(verb.iter().map(|s| s.to_string()));
    cmd.extend(ctx_args_for_namespace(namespace));
    cmd.push("-n".to_string());
    cmd.push(namespace.to_string());
    cmd.extend(rest.iter().map(|s| s.to_string()));
    cmd
}

/// Cronjobs that actually exist in a namespace: (`devstats-<proj>` set,
/// `devstats-affiliations-<proj>` set); `(None, None)` on kubectl failure (the
/// caller then falls back to values.yaml eligibility).
fn get_alive_cronjobs(
    st: &State,
    namespace: &str,
) -> (Option<HashSet<String>>, Option<HashSet<String>>) {
    let cmd = kubectl_args(
        &["get", "cronjob"],
        namespace,
        &[
            "-o",
            r#"jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}"#,
        ],
    );
    let res = match exec::exec_command(&st.ctx, &cmd, &BTreeMap::new()) {
        Ok(r) => r,
        Err(e) => {
            println!(
                "getAliveCronjobs: -n {namespace}: error: {e} (falling back to values.yaml based eligibility)"
            );
            return (None, None);
        }
    };
    let mut sync = HashSet::new();
    let mut affs = HashSet::new();
    for line in res.split('\n') {
        let name = line.trim();
        if name.is_empty() {
            continue;
        }
        if let Some(p) = name.strip_prefix("devstats-affiliations-") {
            affs.insert(p.to_string());
            continue;
        }
        if let Some(p) = name.strip_prefix("devstats-") {
            sync.insert(p.to_string());
        }
    }
    if st.debug {
        println!(
            "getAliveCronjobs: -n {namespace}: {} sync, {} affs cronjobs found",
            sync.len(),
            affs.len()
        );
    }
    (Some(sync), Some(affs))
}

/// database name → size in bytes from the namespace's postgres pod; `None` on
/// failure or with `NO_DB_SIZES` (even weights).
fn get_db_sizes(st: &State, namespace: &str) -> Option<HashMap<String, f64>> {
    if st.no_db_sizes {
        return None;
    }
    let mut pod = env_str("SIZES_POD");
    if pod.is_empty() {
        pod = "devstats-postgres-0".to_string();
    }
    let cmd = kubectl_args(
        &["exec"],
        namespace,
        &[
            &pod,
            "--",
            "psql",
            "-U",
            "postgres",
            "-At",
            "-F",
            " ",
            "-c",
            "select datname, pg_database_size(datname) from pg_database where not datistemplate",
        ],
    );
    let res = match exec::exec_command(&st.ctx, &cmd, &BTreeMap::new()) {
        Ok(r) => r,
        Err(e) => {
            println!("getDBSizes: -n {namespace} {pod}: error: {e} (falling back to even weights)");
            return None;
        }
    };
    let mut sizes = HashMap::new();
    for line in res.split('\n') {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let ary: Vec<&str> = line.split(' ').collect();
        if ary.len() < 2 {
            continue;
        }
        let Ok(size) = gotime::parse_go_float(ary[1]) else {
            continue;
        };
        sizes.insert(ary[0].to_string(), size);
    }
    if st.debug {
        println!(
            "getDBSizes: -n {namespace}: got {} database sizes",
            sizes.len()
        );
    }
    Some(sizes)
}

/// Scheduling weight of a project: `size^power`; unknown / tiny DBs get the
/// smallest known size > 1 byte; no sizes at all → 1.0 (even split).
fn proj_weight(sizes: Option<&HashMap<String, f64>>, db: &str, power: f64) -> f64 {
    let Some(sizes) = sizes else {
        return 1.0;
    };
    let mut size = sizes.get(db).copied();
    if size.is_none_or(|s| s <= 1.0) {
        let mut smallest = f64::MAX;
        for &s in sizes.values() {
            if s > 1.0 && s < smallest {
                smallest = s;
            }
        }
        if smallest == f64::MAX {
            return 1.0;
        }
        size = Some(smallest);
    }
    gomath::pow(size.unwrap_or(1.0), power)
}

/// `kubectl patch cronjob -n NS CJ -p '{"spec":{"jobTemplate":...env:[...]}}'`
fn patch_env(st: &mut State, namespace: &str, cronjob: &str, fields: &[&str], patches: &[&str]) {
    if st.debug {
        println!(
            "patchEnv: -n {namespace} {cronjob}: {}={}",
            gofmt::slice(fields),
            gofmt::slice(patches)
        );
    }
    st.attempted += 1;
    let mut spec = format!(
        r#"{{"spec":{{"jobTemplate":{{"spec":{{"template":{{"spec":{{"containers":[{{"name":"{cronjob}","env":["#
    );
    let n = fields.len();
    for (i, (field, patch)) in fields.iter().zip(patches).enumerate() {
        spec.push_str(&format!(r#"{{"name":"{field}","value":"{patch}"}}"#));
        if i + 1 < n {
            spec.push(',');
        }
    }
    spec.push_str("]}]}}}}}}");
    let cmd = kubectl_args(&["patch", "cronjob"], namespace, &[cronjob, "-p", &spec]);
    if let Err(e) = exec::exec_command(&st.ctx, &cmd, &BTreeMap::new()) {
        println!("{}: error: {e}", gofmt::slice(&cmd));
        return;
    }
    st.patched += 1;
}

/// `kubectl patch cronjob -n NS CJ -p '{"spec":{"<field>":<patch>}}'` (no-op with `ONLY_ENV`).
fn patch(st: &mut State, namespace: &str, cronjob: &str, field: &str, patch: &str) {
    if st.only_env {
        return;
    }
    if st.debug {
        println!("patch: -n {namespace} {cronjob}: {field}={patch}");
    }
    st.attempted += 1;
    let spec = format!(r#"{{"spec":{{"{field}":{patch}}}}}"#);
    let cmd = kubectl_args(&["patch", "cronjob"], namespace, &[cronjob, "-p", &spec]);
    if let Err(e) = exec::exec_command(&st.ctx, &cmd, &BTreeMap::new()) {
        println!("{}: error: {e}", gofmt::slice(&cmd));
        return;
    }
    st.patched += 1;
}

/// Go `gName2Env`.
fn name_to_env(name: &str) -> &'static str {
    match name {
        "AffSkipTemp" => "SKIPTEMP",
        "MaxHist" => "GHA2DB_MAX_HIST",
        "SkipAffsLock" => "SKIP_AFFS_LOCK",
        "AffsLockDB" => "AFFS_LOCK_DB",
        "NoDurable" => "NO_DURABLE",
        "DurablePQ" => "DURABLE_PQ",
        "MaxRunDuration" => "GHA2DB_MAX_RUN_DURATION",
        "RecentRange" => "GHA2DB_RECENT_RANGE",
        "OrphanCommitsRange" => "GHA2DB_ORPHAN_COMMITS_RANGE",
        "RecentReposRange" => "GHA2DB_RECENT_REPOS_RANGE",
        "SkipGHAPI" => "GHA2DB_GHAPISKIP",
        "SkipGetRepos" => "GHA2DB_GETREPOSSKIP",
        "NCPUs" => "GHA2DB_NCPUS",
        "SkipImpAffs" => "SKIP_IMP_AFFS",
        "SkipUpdAffs" => "SKIP_UPD_AFFS",
        _ => "",
    }
}

/// `strconv.Itoa(v)` with `"0"` → `""` (Go: unset the env when the value is zero).
fn int_or_empty(v: i64) -> String {
    if v == 0 {
        String::new()
    } else {
        v.to_string()
    }
}

/// Push the `PATCH_ENV`-selected project settings as container env values to a cronjob.
fn consider_patch_env(
    st: &mut State,
    namespace: &str,
    cronjob: &str,
    project: &Project,
    n_cpus: i64,
    affs: bool,
) {
    let Some(patch_set) = st.patch_env.clone() else {
        return;
    };
    let envs: &[&str] = if affs {
        if st.skip_affs_env {
            return;
        }
        &[
            "AffSkipTemp",
            "MaxHist",
            "SkipAffsLock",
            "AffsLockDB",
            "NoDurable",
            "DurablePQ",
            "MaxRunDuration",
            "SkipGHAPI",
            "SkipGetRepos",
            "NCPUs",
            "SkipImpAffs",
            "SkipUpdAffs",
        ]
    } else {
        if st.skip_sync_env {
            return;
        }
        &[
            "MaxHist",
            "NoDurable",
            "DurablePQ",
            "MaxRunDuration",
            "NCPUs",
            "RecentRange",
            "OrphanCommitsRange",
            "RecentReposRange",
        ]
    };
    let mut fields: Vec<&str> = Vec::new();
    let mut patches: Vec<String> = Vec::new();
    for env in envs {
        if !patch_set.contains(*env) {
            continue;
        }
        fields.push(name_to_env(env));
        let patch = match *env {
            "NCPUs" => n_cpus.to_string(),
            "AffSkipTemp" => project.aff_skip_temp.clone(),
            "MaxHist" => int_or_empty(project.max_hist),
            "SkipAffsLock" => int_or_empty(project.skip_affs_lock),
            "AffsLockDB" => project.affs_lock_db.clone(),
            "NoDurable" => int_or_empty(project.no_durable),
            "DurablePQ" => int_or_empty(project.durable_pq),
            "MaxRunDuration" => project.max_run_duration.clone(),
            "RecentRange" => project.recent_range.clone(),
            "OrphanCommitsRange" => project.orphan_commits_range.clone(),
            "RecentReposRange" => project.recent_repos_range.clone(),
            "SkipGHAPI" => int_or_empty(project.skip_ghapi),
            "SkipGetRepos" => int_or_empty(project.skip_get_repos),
            "SkipUpdAffs" => int_or_empty(project.skip_upd_affs),
            "SkipImpAffs" => int_or_empty(project.skip_imp_affs),
            _ => String::new(),
        };
        patches.push(patch);
    }
    if !fields.is_empty() {
        let patches: Vec<&str> = patches.iter().map(String::as_str).collect();
        patch_env(st, namespace, cronjob, &fields, &patches);
    }
}

/// A single alive project entry in the weighted scheduler (Go `weightedEntry`).
#[derive(Debug, Clone)]
struct WeightedEntry {
    /// index in `values.projects`
    idx: usize,
    proj: String,
    db: String,
    /// `size^power`
    weight: f64,
    /// DB size in Gb (for reporting)
    size_gb: f64,
    /// hourly sync cronjob exists in the cluster
    sync_alive: bool,
    /// affiliations cronjob exists in the cluster
    affs_alive: bool,
}

/// Go `pos / almostHour` style integer arithmetic helpers on `i64`.
fn sync_hours_list(hour_s: i64, sync_hrs: i64) -> String {
    let hours: Vec<String> = (0..24)
        .filter(|h| h % sync_hrs == hour_s)
        .map(|h| h.to_string())
        .collect();
    hours.join(",")
}

/// New (default) algorithm for one env: weighted positions over the sync /
/// daily / affs spaces, anchored affs, report, values update and patches.
fn generate_weighted_cron_entries(
    st: &mut State,
    values: &mut Values,
    test: bool,
    entries: &[WeightedEntry],
    gha_offset: f64,
    sync_hours: f64,
) {
    let (env, namespace) = if test {
        ("test", "devstats-test")
    } else {
        ("prod", "devstats-prod")
    };
    let period_days: i64 = if st.monthly { 28 } else { 7 };
    let gha_off = gha_offset as i64;
    let sync_hrs = sync_hours as i64;
    let almost_hour: i64 = 60 - gha_off;
    let sync_space: i64 = sync_hrs * almost_hour;
    let affs_space: i64 = period_days * 24 * almost_hour;
    let total_weight: f64 = entries.iter().map(|e| e.weight).sum();
    if total_weight <= 0.0 {
        println!("{env}: no alive projects to schedule");
        return;
    }
    // daily projects (DAILY_PROJECTS) sync once per day after the affiliations import
    let (daily, regular): (Vec<&WeightedEntry>, Vec<&WeightedEntry>) = entries
        .iter()
        .partition(|e| st.daily_projs.contains(&e.proj));
    let import_cron = if test {
        values.import_cron_test.clone()
    } else {
        values.import_cron.clone()
    };
    let mut import_hour: i64 = 2;
    let ary: Vec<&str> = import_cron.split_whitespace().collect();
    if ary.len() >= 2 {
        if let Ok(h) = gotime::parse_go_int(ary[1]) {
            import_hour = h;
        }
    }
    let daily_start_hour = import_hour + 1;
    let daily_space = (24 - daily_start_hour) * almost_hour;
    // linear position -> sync cron: 'M h0,h0+syncHours,... * * *', minute M in [ghaOffset, 60)
    let pos_to_cron_sync = |pos: i64| -> String {
        let mut hour_s = pos / almost_hour;
        let minute_s = (pos % almost_hour) + gha_off;
        if hour_s >= sync_hrs {
            hour_s = 0;
        }
        format!("{minute_s} {} * * *", sync_hours_list(hour_s, sync_hrs))
    };
    // linear position -> daily sync cron: 'M H * * *', H in [dailyStartHour, 24)
    let pos_to_cron_daily = |pos: i64| -> String {
        let hour_d = daily_start_hour + pos / almost_hour;
        let minute_d = (pos % almost_hour) + gha_off;
        format!("{minute_d} {hour_d} * * *")
    };
    // affs anchoring (default): affs placed relative to the project's OWN sync cron
    let minutes_in_day: i64 = 24 * 60;
    let affs_anchor = st.affs_anchor;
    let daily_affs_off = st.daily_affs_off;
    let affs_time_of = |pos: i64, sync_pos_v: i64, daily_pos_v: i64, is_daily: bool| -> i64 {
        let day_a = (pos / (almost_hour * 24)) % period_days;
        if !affs_anchor {
            return day_a * minutes_in_day
                + ((pos / almost_hour) % 24) * 60
                + (pos % almost_hour)
                + gha_off;
        }
        let mut affs_min_of_day = 0;
        if is_daily {
            let sync_min_of_day = (daily_start_hour + daily_pos_v / almost_hour) * 60
                + (daily_pos_v % almost_hour)
                + gha_off;
            affs_min_of_day = (sync_min_of_day + daily_affs_off * 60) % minutes_in_day;
        } else {
            let sync_min_of_day =
                (sync_pos_v / almost_hour) * 60 + (sync_pos_v % almost_hour) + gha_off;
            let base = sync_min_of_day + sync_hrs * 30;
            let target = ((pos / almost_hour) % 24) * 60 + (pos % almost_hour) + gha_off;
            let slots = std::cmp::max(24 / sync_hrs, 1);
            let mut best_dist = minutes_in_day;
            for j in 0..slots {
                let cand = (base + j * sync_hrs * 60) % minutes_in_day;
                let mut dist = (cand - target + minutes_in_day) % minutes_in_day;
                if dist > minutes_in_day / 2 {
                    dist = minutes_in_day - dist;
                }
                if dist < best_dist {
                    affs_min_of_day = cand;
                    best_dist = dist;
                }
            }
        }
        day_a * minutes_in_day + affs_min_of_day
    };
    // absolute affs minute within the period -> affs cron: weekly 'M H * * D' or monthly 'M H D * *'
    let monthly = st.monthly;
    let time_to_cron_affs = |t: i64| -> String {
        let minute_a = t % 60;
        let hour_a = (t / 60) % 24;
        let day_a = t / minutes_in_day;
        if monthly {
            format!("{minute_a} {hour_a} {} * *", day_a + 1)
        } else {
            format!("{minute_a} {hour_a} * * {day_a}")
        }
    };
    // distribute cumulative weighted positions, bump on collisions
    let positions = |list: &[&WeightedEntry], space: i64| -> HashMap<usize, i64> {
        let total: f64 = list.iter().map(|e| e.weight).sum();
        let mut out = HashMap::new();
        if total <= 0.0 {
            return out;
        }
        let mut used: HashSet<i64> = HashSet::new();
        let mut cum = 0.0;
        for e in list {
            let mut pos = ((cum / total) * space as f64) as i64;
            if pos >= space {
                pos = space - 1;
            }
            while used.contains(&pos) {
                pos = (pos + 1) % space;
            }
            used.insert(pos);
            out.insert(e.idx, pos);
            cum += e.weight;
        }
        out
    };
    let all: Vec<&WeightedEntry> = entries.iter().collect();
    let sync_pos = positions(&regular, sync_space);
    let daily_pos = positions(&daily, daily_space);
    let affs_pos = positions(&all, affs_space);
    let at = |m: &HashMap<usize, i64>, idx: usize| m.get(&idx).copied().unwrap_or(0);
    // final affs times: weighted day + (anchored or legacy) time of day
    let mut affs_time: HashMap<usize, i64> = HashMap::new();
    for e in entries {
        affs_time.insert(
            e.idx,
            affs_time_of(
                at(&affs_pos, e.idx),
                at(&sync_pos, e.idx),
                at(&daily_pos, e.idx),
                st.daily_projs.contains(&e.proj),
            ),
        );
    }
    // gap = distance from a project's position to the next scheduled one (wraps around the space).
    // Go sorts by position only, so equal positions (possible for affs times) are ordered by
    // its random map iteration; here ties are broken by the project index (deterministic).
    let gaps = |pos: &HashMap<usize, i64>, space: i64| -> HashMap<usize, i64> {
        let mut list: Vec<(i64, usize)> = pos.iter().map(|(idx, p)| (*p, *idx)).collect();
        list.sort_unstable();
        let mut out = HashMap::new();
        for (i, (p, idx)) in list.iter().enumerate() {
            if i == list.len() - 1 {
                out.insert(*idx, space - p + list[0].0);
            } else {
                out.insert(*idx, list[i + 1].0 - p);
            }
        }
        out
    };
    let sync_gap = gaps(&sync_pos, sync_space);
    let daily_gap = gaps(&daily_pos, daily_space);
    let affs_gap = gaps(&affs_time, period_days * minutes_in_day);
    println!(
        "{env}: {} alive projects ({} daily), algo {}, sync space {sync_space} minutes (every {}h), daily space {daily_space} minutes (import at {import_cron}, dailies from {daily_start_hour}:{:02}), affs space {affs_space} minutes ({period_days} days):",
        entries.len(),
        daily.len(),
        st.split_algo,
        gof(sync_hours, 0),
        gha_off
    );
    if st.affs_anchor {
        println!(
            "{env}: affs anchored to own sync: regular projects mid-gap (sync slot +{}m), daily projects sync +{}h (NO_AFFS_ANCHOR=1 for legacy independent placement)",
            sync_hrs * 30,
            st.daily_affs_off
        );
    }
    for e in entries {
        let is_daily = st.daily_projs.contains(&e.proj);
        let (cron_s, gap_s) = if is_daily {
            (
                pos_to_cron_daily(at(&daily_pos, e.idx)),
                format!(
                    "DAILY gap={}h ranges='{}'",
                    gof(at(&daily_gap, e.idx) as f64 / 60.0, 1),
                    st.daily_range
                ),
            )
        } else {
            (
                pos_to_cron_sync(at(&sync_pos, e.idx)),
                format!("gap={}m", at(&sync_gap, e.idx)),
            )
        };
        let cron_a = time_to_cron_affs(at(&affs_time, e.idx));
        println!(
            "  {:<24} db={:<16} size={}Gb weight={} share={}% sync='{cron_s}' {gap_s} affs='{cron_a}' gap={}h",
            e.proj,
            e.db,
            gofw(e.size_gb, 2, 9),
            gofw(e.weight, 1, 9),
            gofw((e.weight / total_weight) * 100.0, 1, 5),
            gof(at(&affs_gap, e.idx) as f64 / 60.0, 1)
        );
        let sync_cj = format!("devstats-{}", e.proj);
        let affs_cj = format!("devstats-affiliations-{}", e.proj);
        let range_envs = [
            "GHA2DB_RECENT_RANGE",
            "GHA2DB_ORPHAN_COMMITS_RANGE",
            "GHA2DB_RECENT_REPOS_RANGE",
        ];
        if is_daily {
            // widen ghapi2db/orphan-commits/recent-repos lookbacks to cover the 24h cadence (+overlap)
            let p = &mut values.projects[e.idx];
            if p.recent_range != st.daily_range
                || p.orphan_commits_range != st.daily_range
                || p.recent_repos_range != st.daily_repos
            {
                p.recent_range = st.daily_range.clone();
                p.orphan_commits_range = st.daily_range.clone();
                p.recent_repos_range = st.daily_repos.clone();
            }
            if !st.never && e.sync_alive {
                let (range, repos) = (st.daily_range.clone(), st.daily_repos.clone());
                patch_env(
                    st,
                    namespace,
                    &sync_cj,
                    &range_envs,
                    &[&range, &range, &repos],
                );
            }
        } else {
            let p = &mut values.projects[e.idx];
            if !p.recent_range.is_empty()
                || !p.orphan_commits_range.is_empty()
                || !p.recent_repos_range.is_empty()
            {
                // project left the daily list - restore default lookback ranges
                p.recent_range.clear();
                p.orphan_commits_range.clear();
                p.recent_repos_range.clear();
                if !st.never && e.sync_alive {
                    patch_env(st, namespace, &sync_cj, &range_envs, &["", "", ""]);
                }
            }
        }
        let p = &mut values.projects[e.idx];
        let (aff_cron, cron) = if test {
            (&mut p.aff_cron_test, &mut p.cron_test)
        } else {
            (&mut p.aff_cron_prod, &mut p.cron_prod)
        };
        let mut patch_affs = false;
        let mut patch_sync = false;
        if st.always || *aff_cron != cron_a {
            *aff_cron = cron_a.clone();
            patch_affs = !st.never && e.affs_alive;
        }
        if st.always || *cron != cron_s {
            *cron = cron_s.clone();
            patch_sync = !st.never && e.sync_alive;
        }
        if patch_affs {
            patch(
                st,
                namespace,
                &affs_cj,
                "schedule",
                &format!("\"{cron_a}\""),
            );
        }
        if patch_sync {
            patch(
                st,
                namespace,
                &sync_cj,
                "schedule",
                &format!("\"{cron_s}\""),
            );
        }
        if !st.never {
            if e.sync_alive {
                consider_patch_env(
                    st,
                    namespace,
                    &sync_cj,
                    &values.projects[e.idx],
                    values.sync_cpus,
                    false,
                );
            }
            if e.affs_alive {
                consider_patch_env(
                    st,
                    namespace,
                    &affs_cj,
                    &values.projects[e.idx],
                    values.affs_cpus,
                    true,
                );
            }
        }
    }
}

/// Probe alive cronjobs and DB sizes for one env, push suspend states, and
/// build the weighted entries of the projects eligible for scheduling.
fn new_algorithm_for_env(st: &mut State, values: &Values, test: bool) -> Vec<WeightedEntry> {
    let namespace = if test {
        "devstats-test"
    } else {
        "devstats-prod"
    };
    let (alive_sync, alive_affs) = get_alive_cronjobs(st, namespace);
    let mut sizes = get_db_sizes(st, namespace);
    if let Some(sizes) = sizes.as_mut() {
        // drop non-project databases (devstats, postgres, ...) so they never influence weights
        let proj_dbs: HashSet<&str> = values.projects.iter().map(|p| p.db.as_str()).collect();
        sizes.retain(|db, _| proj_dbs.contains(db.as_str()));
    }
    let mut entries = Vec::new();
    for (i, project) in values.projects.iter().enumerate() {
        let (domain_ok, mut suspended) = if test {
            (project.domains[0] != 0, project.suspend_cron_test)
        } else {
            (
                project.domains[1] != 0 || project.domains[2] != 0 || project.domains[3] != 0,
                project.suspend_cron_prod,
            )
        };
        if !domain_ok {
            continue;
        }
        if project.archived {
            // archived projects don't count: suspend leftover cronjobs, never schedule
            suspended = true;
        }
        let sync_alive = alive_sync
            .as_ref()
            .is_none_or(|s| s.contains(&project.proj));
        let affs_alive = alive_affs
            .as_ref()
            .is_none_or(|s| s.contains(&project.proj));
        if !st.never {
            let suspend = if st.suspend_all {
                "true".to_string()
            } else {
                suspended.to_string()
            };
            if !st.no_suspend_h && sync_alive {
                patch(
                    st,
                    namespace,
                    &format!("devstats-{}", project.proj),
                    "suspend",
                    &suspend,
                );
            }
            if !st.no_suspend_a && affs_alive {
                patch(
                    st,
                    namespace,
                    &format!("devstats-affiliations-{}", project.proj),
                    "suspend",
                    &suspend,
                );
            }
        }
        if suspended || (!sync_alive && !affs_alive) {
            continue;
        }
        let size_gb = match &sizes {
            Some(s) => s.get(&project.db).copied().unwrap_or(0.0) / (1024.0 * 1024.0 * 1024.0),
            None => 0.0,
        };
        entries.push(WeightedEntry {
            idx: i,
            proj: project.proj.clone(),
            db: project.db.clone(),
            weight: proj_weight(sizes.as_ref(), &project.db, st.weight_power),
            size_gb,
            sync_alive,
            affs_alive,
        });
    }
    entries
}

/// Parameters of the legacy static split (Go `generateCronEntries` arguments).
#[derive(Debug, Clone, Copy)]
struct OldParams {
    offset_hours: f64,
    kubernetes_hours: f64,
    all_hours: f64,
    interval_t: f64,
    interval_p: f64,
    minutes: f64,
    gha_offset: f64,
    sync_hours: f64,
    nt: i64,
    np: i64,
}

/// Legacy algorithm (`OLD_ALGORITHM=1`): static values.yaml based split.
/// `idxt`/`idxp` == -1 → kubernetes, == -2 → all cncf.
#[allow(clippy::too_many_arguments)]
fn generate_cron_entries(
    st: &mut State,
    values: &mut Values,
    idx: usize,
    test: bool,
    prod: bool,
    idxt: i64,
    idxp: i64,
    p: OldParams,
) {
    let gha_off = p.gha_offset as i64;
    let sync_hrs = p.sync_hours as i64;
    let monthly = st.monthly;
    let minutes_to_cron = |min_a: i64, min_s: i64| -> (String, String) {
        let minutes_a = min_a % 60;
        let hours_a = (min_a / 60) % 24;
        let cron_a = if monthly {
            let day_a = ((min_a / (60 * 24)) % 28) + 1;
            format!("{minutes_a} {hours_a} {day_a} * *")
        } else {
            let day_a = (min_a / (60 * 24)) % 7;
            format!("{minutes_a} {hours_a} * * {day_a}")
        };
        let almost_hour = 60 - gha_off;
        let mut hour_s = min_s / almost_hour;
        let minute_s = (min_s % almost_hour) + gha_off;
        if hour_s >= sync_hrs {
            println!(
                "warning: (minA,minS) = ({min_a},{min_s}) generates hourS >= syncHrs: {hour_s} >= {sync_hrs}"
            );
            hour_s = 0;
        }
        let cron_s = format!("{minute_s} {} * * *", sync_hours_list(hour_s, sync_hrs));
        (cron_a, cron_s)
    };
    let mut period_hours = WEEK_HOURS as i64;
    let mut period_minutes = WEEK_MINUTES as i64;
    if monthly {
        period_hours <<= 2;
        period_minutes <<= 2;
    }
    if test {
        let (mut minute_a, mut minute_s) = if idxt == -1 {
            (0, 0)
        } else if idxt == -2 {
            (
                60 * (period_hours as f64 - p.all_hours) as i64,
                (p.minutes / 2.0) as i64,
            )
        } else {
            (
                ((p.kubernetes_hours + p.interval_t * idxt as f64) * 60.0) as i64,
                ((idxt as f64 * p.minutes) / p.nt as f64) as i64,
            )
        };
        minute_a += (p.offset_hours * 60.0) as i64;
        // Offset for test: affiliations cronjob offset
        minute_a += period_hours * 30;
        // hourly sync cronjob offset
        minute_s += (p.minutes / 2.0) as i64;
        if minute_s >= p.minutes as i64 {
            minute_s -= p.minutes as i64;
        }
        if minute_a < 0 {
            minute_a += period_minutes;
        }
        if minute_a >= period_minutes {
            minute_a -= period_minutes;
        }
        let (cron_a, cron_s) = minutes_to_cron(minute_a, minute_s);
        let proj = values.projects[idx].proj.clone();
        if !st.never && (st.always || values.projects[idx].aff_cron_test != cron_a) {
            values.projects[idx].aff_cron_test = cron_a.clone();
            patch(
                st,
                "devstats-test",
                &format!("devstats-affiliations-{proj}"),
                "schedule",
                &format!("\"{cron_a}\""),
            );
        }
        if !st.never && (st.always || values.projects[idx].cron_test != cron_s) {
            values.projects[idx].cron_test = cron_s.clone();
            patch(
                st,
                "devstats-test",
                &format!("devstats-{proj}"),
                "schedule",
                &format!("\"{cron_s}\""),
            );
        }
        if !st.never {
            consider_patch_env(
                st,
                "devstats-test",
                &format!("devstats-{proj}"),
                &values.projects[idx],
                values.sync_cpus,
                false,
            );
            consider_patch_env(
                st,
                "devstats-test",
                &format!("devstats-affiliations-{proj}"),
                &values.projects[idx],
                values.affs_cpus,
                true,
            );
        }
    }
    if prod {
        let (mut minute_a, minute_s) = if idxp == -1 {
            (0, 0)
        } else if idxp == -2 {
            (
                60 * (period_hours as f64 - p.all_hours) as i64,
                (p.minutes / 2.0) as i64,
            )
        } else {
            (
                ((p.kubernetes_hours + p.interval_p * idxp as f64) * 60.0) as i64,
                ((idxp as f64 * p.minutes) / p.np as f64) as i64,
            )
        };
        minute_a += (p.offset_hours * 60.0) as i64;
        if minute_a < 0 {
            minute_a += period_minutes;
        }
        if minute_a >= period_minutes {
            minute_a -= period_minutes;
        }
        let (cron_a, cron_s) = minutes_to_cron(minute_a, minute_s);
        let proj = values.projects[idx].proj.clone();
        if !st.never && (st.always || values.projects[idx].aff_cron_prod != cron_a) {
            values.projects[idx].aff_cron_prod = cron_a.clone();
            patch(
                st,
                "devstats-prod",
                &format!("devstats-affiliations-{proj}"),
                "schedule",
                &format!("\"{cron_a}\""),
            );
        }
        if !st.never && (st.always || values.projects[idx].cron_prod != cron_s) {
            values.projects[idx].cron_prod = cron_s.clone();
            patch(
                st,
                "devstats-prod",
                &format!("devstats-{proj}"),
                "schedule",
                &format!("\"{cron_s}\""),
            );
        }
        if !st.never {
            consider_patch_env(
                st,
                "devstats-prod",
                &format!("devstats-{proj}"),
                &values.projects[idx],
                values.sync_cpus,
                false,
            );
            consider_patch_env(
                st,
                "devstats-prod",
                &format!("devstats-affiliations-{proj}"),
                &values.projects[idx],
                values.affs_cpus,
                true,
            );
        }
    }
}

/// `PATCH_ENV` → set of project setting names to push (Go `setPatchEnvMap`).
fn set_patch_env_map(st: &mut State) {
    let data = env_str("PATCH_ENV");
    if data.is_empty() {
        return;
    }
    st.patch_env = Some(data.split(',').map(|s| s.trim().to_string()).collect());
}

/// Read an integer knob: unset → default, otherwise `strconv.Atoi` (fatal on
/// error) and a range check with the Go error message.
fn int_knob(name: &str, default: i64, min: i64, max: i64, msg: &str) -> i64 {
    let s = env_str(name);
    if s.is_empty() {
        return default;
    }
    let v = fatal_on_err(gotime::parse_go_int(&s));
    if v < min || v > max {
        fatalf(format_args!("{msg}"));
    }
    v
}

fn generate_cron_values(in_file: &str, out_file: &str) {
    let mut st = State {
        ctx: Ctx::default(),
        ..State::default()
    };
    st.ctx.init();
    signal::setup_timeout_signal(&st.ctx);
    st.ctx.exec_fatal = false;
    st.ctx.exec_output = true;

    let data = fatal_on_err(io::read_file_raw(in_file));
    let mut values: Values = fatal_on_err(yde::unmarshal(&data));
    println!("read {in_file}");

    st.debug = env_set("DEBUG");
    st.monthly = env_set("MONTHLY");
    let max_hours: i64 = if st.monthly { 48 } else { 30 };
    let kubernetes_hours_i = int_knob(
        "KUBERNETES_HOURS",
        if st.monthly { 36 } else { 24 },
        3,
        max_hours,
        &format!("KUBERNETES_HOURS must be from [3,{max_hours}]"),
    );
    let kubernetes_hours = kubernetes_hours_i as f64;
    let all_hours_i = int_knob(
        "ALL_HOURS",
        if st.monthly { 36 } else { 20 },
        3,
        max_hours,
        &format!("ALL_HOURS must be from [3,{max_hours}]"),
    );
    let all_hours = all_hours_i as f64;
    let gha_offset = int_knob("GHA_OFFSET", 4, 2, 10, "GHA_OFFSET must be from [2,10]") as f64;
    let sync_hours = int_knob("SYNC_HOURS", 6, 1, 6, "SYNC_HOURS must be from 1 to 6") as f64;
    let offset_hours = int_knob(
        "OFFSET_HOURS",
        -4,
        -84,
        84,
        "OFFSET_HOURS must be from [-84,84]",
    ) as f64;
    st.always = env_set("ALWAYS_PATCH");
    st.never = env_set("NEVER_PATCH");
    st.only_env = env_set("ONLY_ENV");
    st.only_suspend = env_set("ONLY_SUSPEND");
    st.suspend_all = env_set("SUSPEND_ALL");
    st.no_suspend_h = env_set("NO_SUSPEND_H");
    st.no_suspend_a = env_set("NO_SUSPEND_A");
    st.skip_affs_env = env_set("SKIP_AFFS_ENV");
    st.skip_sync_env = env_set("SKIP_SYNC_ENV");
    st.only_prod = env_set("ONLY_PROD");
    st.only_test = env_set("ONLY_TEST");
    st.old_algorithm = env_set("OLD_ALGORITHM");
    st.no_db_sizes = env_set("NO_DB_SIZES");
    st.split_algo = env_str("SPLIT_ALGO");
    if st.split_algo.is_empty() {
        st.split_algo = "geom".to_string();
    }
    st.weight_power = match st.split_algo.as_str() {
        "geom" => 0.5,
        "prop" => 1.0,
        "invgeom" => 1.5,
        _ => fatalf(format_args!(
            "SPLIT_ALGO must be one of: geom (sqrt(size), default), prop (size), invgeom (size^1.5)"
        )),
    };
    if env_set("WEIGHT_POWER") {
        st.weight_power = fatal_on_err(gotime::parse_go_float(&env_str("WEIGHT_POWER")));
        // NaN passes plain range checks and would crash the scheduler later — reject it
        if st.weight_power.is_nan() || st.weight_power < 0.0 || st.weight_power > 4.0 {
            fatalf(format_args!("WEIGHT_POWER must be from 0.0 to 4.0"));
        }
        st.split_algo = format!("power={}", gofmt::float(st.weight_power));
    }
    st.daily_range = env_str("DAILY_RANGE");
    if st.daily_range.is_empty() {
        st.daily_range = "26 hours".to_string();
    }
    st.daily_repos = env_str("DAILY_REPOS_RANGE");
    if st.daily_repos.is_empty() {
        st.daily_repos = "2 days".to_string();
    }
    let mut daily_list = env_str("DAILY_PROJECTS");
    if daily_list.is_empty() {
        daily_list = "kubernetes,all,jenkins,opentelemetry,allcdf,istio".to_string();
    }
    st.affs_anchor = !env_set("NO_AFFS_ANCHOR");
    st.daily_affs_off = int_knob(
        "DAILY_AFFS_OFFSET_HOURS",
        8,
        1,
        23,
        "DAILY_AFFS_OFFSET_HOURS must be from [1,23]",
    );
    if daily_list != "-" {
        st.daily_projs = daily_list
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect();
    }
    set_patch_env_map(&mut st);
    // New (default) algorithm: schedule only projects actually alive in each env's cluster,
    // give each time proportional to size^power (SPLIT_ALGO), spread evenly over the whole period.
    // OLD_ALGORITHM=1 switches back to the legacy values.yaml based static split below.
    if !st.old_algorithm {
        println!("new algorithm: probing alive cronjobs & DB sizes (OLD_ALGORITHM=1 for legacy mode, NO_DB_SIZES=1 for even weights)");
        println!(
            "weights: SPLIT_ALGO={}, weight = size^{}",
            st.split_algo,
            gofmt::float(st.weight_power)
        );
        println!(
            "sync happens from HH:{}, every {} hours; affs spread over {}",
            gof0(gha_offset, 0, 2),
            gof(sync_hours, 0),
            if st.monthly { "28 days" } else { "7 days" }
        );
        if !st.only_prod {
            let entries = new_algorithm_for_env(&mut st, &values, true);
            if !st.only_suspend {
                generate_weighted_cron_entries(
                    &mut st,
                    &mut values,
                    true,
                    &entries,
                    gha_offset,
                    sync_hours,
                );
            }
        }
        if !st.only_test {
            let entries = new_algorithm_for_env(&mut st, &values, false);
            if !st.only_suspend {
                generate_weighted_cron_entries(
                    &mut st,
                    &mut values,
                    false,
                    &entries,
                    gha_offset,
                    sync_hours,
                );
            }
        }
        println!("patched {}/{} cronjobs", st.patched, st.attempted);
        json::write_file_0644(out_file, &marshal(&values.to_node()));
        println!("written {out_file}");
        return;
    }
    let minutes = sync_hours * (60.0 - gha_offset);
    let mut hours = WEEK_HOURS;
    if st.monthly {
        hours *= 4.0;
    }
    hours -= kubernetes_hours + all_hours;
    let (mut kt, mut kp) = (0i64, 0i64);
    let mut kubernetes_idx: i64 = -1;
    let mut all_idx: i64 = -1;
    for (i, project) in values.projects.iter().enumerate() {
        if project.db == "gha" {
            kubernetes_idx = i as i64;
            continue;
        }
        if project.db == "allprj" {
            all_idx = i as i64;
            continue;
        }
        if !project.suspend_cron_test && !project.archived && project.domains[0] != 0 {
            kt += 1;
        }
        if !project.suspend_cron_prod
            && !project.archived
            && (project.domains[1] != 0 || project.domains[2] != 0 || project.domains[3] != 0)
        {
            kp += 1;
        }
    }
    let interval_t = hours / kt as f64;
    let interval_p = hours / kp as f64;
    let interval_st = (60.0 * minutes) / kt as f64;
    let interval_sp = (60.0 * minutes) / kp as f64;
    println!(
        "sync happens from HH:{}, every {} hours, which gives {}min for hourly syncs, middle of weekend offset is {}h",
        gof0(gha_offset, 0, 2),
        gof(sync_hours, 0),
        gof(minutes, 0),
        gof(offset_hours, 0)
    );
    println!(
        "test: Kubernetes(#{kubernetes_idx}) needs {}h, All(#{all_idx}) needs {}h, {kt} others all have {}h, intervals are {}min, {}s",
        gof(kubernetes_hours, 0),
        gof(all_hours, 0),
        gof(hours, 0),
        gof(interval_t * 60.0, 1),
        gof(interval_st, 1)
    );
    println!(
        "prod: Kubernetes(#{kubernetes_idx}) needs {}h, All(#{all_idx}) needs {}h, {kp} others all have {}h, intervals are {}min, {}s",
        gof(kubernetes_hours, 0),
        gof(all_hours, 0),
        gof(hours, 0),
        gof(interval_p * 60.0, 1),
        gof(interval_sp, 1)
    );
    let params = OldParams {
        offset_hours,
        kubernetes_hours,
        all_hours,
        interval_t,
        interval_p,
        minutes,
        gha_offset,
        sync_hours,
        nt: kt,
        np: kp,
    };
    let (mut it, mut ip) = (0i64, 0i64);
    let mut suspend = String::new();
    if st.suspend_all {
        suspend = "true".to_string();
    }
    for i in 0..values.projects.len() {
        let project = values.projects[i].clone();
        let mut t = !project.suspend_cron_test && !project.archived && project.domains[0] != 0;
        let mut p = !project.suspend_cron_prod
            && !project.archived
            && (project.domains[1] != 0 || project.domains[2] != 0 || project.domains[3] != 0);
        if st.only_prod {
            t = false;
        }
        if st.only_test {
            p = false;
        }
        if !st.only_suspend {
            match project.db.as_str() {
                "gha" => generate_cron_entries(&mut st, &mut values, i, t, p, -1, -1, params),
                "allprj" => generate_cron_entries(&mut st, &mut values, i, t, p, -2, -2, params),
                _ => {
                    generate_cron_entries(&mut st, &mut values, i, t, p, it, ip, params);
                    if t {
                        it += 1;
                    }
                    if p {
                        ip += 1;
                    }
                }
            }
        }
        if t && !st.never && project.domains[0] != 0 {
            if !st.suspend_all {
                suspend = values.projects[i].suspend_cron_test.to_string();
            }
            if !st.no_suspend_h {
                patch(
                    &mut st,
                    "devstats-test",
                    &format!("devstats-{}", values.projects[i].proj),
                    "suspend",
                    &suspend,
                );
            }
            if !st.no_suspend_a {
                patch(
                    &mut st,
                    "devstats-test",
                    &format!("devstats-affiliations-{}", values.projects[i].proj),
                    "suspend",
                    &suspend,
                );
            }
        }
        if p && !st.never
            && (project.domains[1] != 0 || project.domains[2] != 0 || project.domains[3] != 0)
        {
            if !st.suspend_all {
                suspend = values.projects[i].suspend_cron_prod.to_string();
            }
            if !st.no_suspend_h {
                patch(
                    &mut st,
                    "devstats-prod",
                    &format!("devstats-{}", values.projects[i].proj),
                    "suspend",
                    &suspend,
                );
            }
            if !st.no_suspend_a {
                patch(
                    &mut st,
                    "devstats-prod",
                    &format!("devstats-affiliations-{}", values.projects[i].proj),
                    "suspend",
                    &suspend,
                );
            }
        }
    }
    println!("patched {}/{} cronjobs", st.patched, st.attempted);
    json::write_file_0644(out_file, &marshal(&values.to_node()));
    println!("written {out_file}");
}

fn main() {
    devstatscode::error::exit_on_panic();
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        println!(
            "usage: {} path/to/devstats-helm/values.yaml new-values.yaml",
            args[0]
        );
        return;
    }
    generate_cron_values(&args[1], &args[2]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_float_formats() {
        assert_eq!(gof(2.5, 0), "2");
        assert_eq!(gof(3.5, 0), "4");
        assert_eq!(gof(0.05, 1), "0.1");
        assert_eq!(gof(f64::INFINITY, 1), "+Inf");
        assert_eq!(gof(f64::NEG_INFINITY, 0), "-Inf");
        assert_eq!(gof(f64::NAN, 2), "NaN");
        assert_eq!(gofw(1.5, 2, 9), "     1.50");
        assert_eq!(gofw(f64::INFINITY, 1, 9), "     +Inf");
        assert_eq!(gof0(4.0, 0, 2), "04");
        assert_eq!(gof0(10.0, 0, 2), "10");
        assert_eq!(gof0(f64::NAN, 0, 2), "NaN");
    }

    #[test]
    fn cron_helpers() {
        assert_eq!(sync_hours_list(0, 6), "0,6,12,18");
        assert_eq!(sync_hours_list(3, 6), "3,9,15,21");
        assert_eq!(
            sync_hours_list(0, 1),
            (0..24).map(|h| h.to_string()).collect::<Vec<_>>().join(",")
        );
        assert_eq!(sync_hours_list(4, 5), "4,9,14,19");
        assert_eq!(int_or_empty(0), "");
        assert_eq!(int_or_empty(12), "12");
        assert_eq!(name_to_env("NCPUs"), "GHA2DB_NCPUS");
        assert_eq!(name_to_env("Bogus"), "");
    }

    #[test]
    fn values_roundtrip_yaml() {
        let src = b"nSyncCPUs: 8\nnAffsCPUs: 8\naffiliationsImportCron: '10 2 * * *'\nprojects:\n- proj: kubernetes\n  url: k8s\n  db: gha\n  icon: k8s\n  org: Kubernetes\n  repo: kubernetes/kubernetes\n  cronTest: '4 3,9,15,21 * * *'\n  cronProd: '4 3 * * *'\n  affCronTest: '0 20 14 * *'\n  affCronProd: '4 11 1 * *'\n  affSkipTemp: '1'\n  disk: 52Gi\n  domains: [0, 1, 0, 0]\n  ga: ''\n  i: 0\n  certNum: 0\n  maxHist: 2\n  affsLockDB: gha\n  durablePQ: 1\n  skipGHAPI: 1\n  suspendCronTest: yes\n  archived: false\n  extra: ignored\n";
        let values: Values = yde::unmarshal(src).unwrap();
        assert_eq!(values.projects.len(), 1);
        assert!(values.projects[0].suspend_cron_test);
        assert_eq!(values.projects[0].aff_skip_temp, "1");
        let out = String::from_utf8(marshal(&values.to_node())).unwrap();
        assert_eq!(
            out,
            "nSyncCPUs: 8\nnAffsCPUs: 8\naffiliationsImportCron: 10 2 * * *\nprojects:\n- proj: kubernetes\n  url: k8s\n  db: gha\n  icon: k8s\n  org: Kubernetes\n  repo: kubernetes/kubernetes\n  cronTest: 4 3,9,15,21 * * *\n  cronProd: 4 3 * * *\n  affCronTest: 0 20 14 * *\n  affCronProd: 4 11 1 * *\n  suspendCronTest: true\n  affSkipTemp: \"1\"\n  disk: 52Gi\n  domains: [0, 1, 0, 0]\n  ga: \"\"\n  i: 0\n  certNum: 0\n  maxHist: 2\n  affsLockDB: gha\n  durablePQ: 1\n  skipGHAPI: 1\n"
        );
        let empty: Values = yde::unmarshal(b"").unwrap();
        assert_eq!(
            String::from_utf8(marshal(&empty.to_node())).unwrap(),
            "nSyncCPUs: 0\nnAffsCPUs: 0\nprojects: []\n"
        );
    }
}
