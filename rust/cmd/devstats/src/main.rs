//! `devstats` — Rust port of `cmd/devstats/devstats.go`.
//!
//! The top-level cron entry point: reads `projects.yaml`, optionally checks
//! the `provisioned` / `devstats_running` flags in every project's
//! `gha_computed` table (and sets the running flag for the duration of the
//! run), guards against concurrent runs with a PID file, refreshes the git
//! repositories (`get_repos`), clears orphaned locks and then runs
//! `gha2db_sync` for every enabled project in `order` (and `website_data` at
//! the end when `GHA2DB_WEBSITEDATA` is set). Environment, output and exit
//! codes are those of the Go program.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use devstatscode::chrono::{DateTime, Local, Utc};
use devstatscode::exec::exec_command;
use devstatscode::pg::api::{exec_sql, exec_sql_with_err, fatal_on_pg_error, query_sql};
use devstatscode::pg::PgError;
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    consts, fatal_on_err, fatal_on_error, fatalf, gofmt, io, pg, printf, projects, rng, signal,
    time as gotime, Ctx,
};

const PROVISION_FLAG: &str = "provisioned";
const RUNNING_FLAG: &str = "devstats_running";

/// Process start, for the `m=+…` monotonic reading Go appends to `%v` of a
/// `time.Now()` value.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Go `%v` of a `time.Now()` value: the local time plus the monotonic clock
/// reading (`2026-09-11 10:00:00.123456789 +0000 UTC m=+0.001234567`).
fn now_v() -> String {
    let now: DateTime<Local> = Local::now();
    let mono = PROCESS_START.get().map(|s| s.elapsed()).unwrap_or_default();
    format!(
        "{} m=+{}.{:09}",
        gofmt::time(now),
        mono.as_secs(),
        mono.subsec_nanos()
    )
}

/// Go `time.Duration.String()` of a possibly negative duration.
fn duration_string(nanos: i64) -> String {
    let s = gotime::format_go_duration(Duration::from_nanos(nanos.unsigned_abs()));
    if nanos < 0 {
        format!("-{s}")
    } else {
        s
    }
}

/// Go `lib.FatalOnError(err)` for a PostgreSQL error: the retryable
/// conditions make Go carry on with a nil result (and crash right after);
/// every other error is fatal.
fn fatal_pg(err: PgError) -> ! {
    fatal_on_pg_error(&err);
    fatal_on_error(err)
}

/// The `gha_computed` flag checks of the projects' databases.
struct Flags<'a> {
    ctx: &'a mut Ctx,
    projs: &'a [projects::Project],
}

impl Flags<'_> {
    /// `GHA2DB_CHECK_PROVISION_FLAG`: is every project database present and
    /// marked `provisioned`? Returns the number of databases that are not.
    fn check_provisioned(&mut self) -> usize {
        let mut missing = 0;
        for proj in self.projs {
            let db = proj.pdb.as_str();
            let con = pg::pg_conn_db(self.ctx, db);
            let rows = query_sql(
                &con,
                self.ctx,
                "select 1 from gha_computed where metric = $1 limit 1",
                &[PROVISION_FLAG.into()],
            );
            let mut rows = match rows {
                Ok(rows) => rows,
                Err(err) => {
                    if err.name() == consts::INVALID_CATALOG_NAME {
                        printf!("No '{}' database, missing provisioning flag\n", db);
                        missing += 1;
                        con.close();
                        continue;
                    }
                    fatal_pg(err)
                }
            };
            let mut provisioned: i64 = 0;
            while rows.next() {
                fatal_on_err(rows.scan(&mut [&mut provisioned]));
            }
            fatal_on_err(rows.err());
            fatal_on_err(rows.close());
            con.close();
            if provisioned != 1 {
                printf!(
                    "Missing provisioned flag on '{}' database and check provisioned flag is set\n",
                    db
                );
                missing += 1;
            }
        }
        missing
    }

    /// `GHA2DB_CHECK_RUNNING_FLAG`: is another run in progress (a
    /// `devstats_running` flag younger than `GHA2DB_MAX_RUNNING_FLAG_AGE`)?
    /// Older flags are treated as orphans and removed. Returns `false` when
    /// this instance must not run.
    fn check_running(&mut self) -> bool {
        for proj in self.projs {
            let db = proj.pdb.as_str();
            let con = pg::pg_conn_db(self.ctx, db);
            let rows = query_sql(
                &con,
                self.ctx,
                "select dt from gha_computed where metric = $1 order by dt desc limit 1",
                &[RUNNING_FLAG.into()],
            );
            let mut rows = match rows {
                Ok(rows) => rows,
                Err(err) => {
                    if err.name() == consts::INVALID_CATALOG_NAME {
                        printf!("No '{}' database, cannot check running flag\n", db);
                        con.close();
                        return false;
                    }
                    fatal_pg(err)
                }
            };
            let mut running: DateTime<Utc> = Utc::now();
            let mut running_set = false;
            while rows.next() {
                fatal_on_err(rows.scan(&mut [&mut running]));
                running_set = true;
            }
            fatal_on_err(rows.err());
            fatal_on_err(rows.close());
            if running_set {
                let age = (Utc::now() - running).num_nanoseconds().unwrap_or(i64::MAX);
                let max_age = self.ctx.max_running_flag_age;
                printf!(
                    "Running flag on '{}' set, age {}, maximum allowed age: {}\n",
                    db,
                    duration_string(age),
                    gotime::format_go_duration(max_age)
                );
                if age <= max_age.as_nanos() as i64 {
                    printf!("Running flag on '{}' set, exiting\n", db);
                    con.close();
                    return false;
                }
                printf!(
                    "Running flag on '{}' expired, removing (this may be due to some error)\n",
                    db
                );
                exec_sql_with_err(
                    &con,
                    self.ctx,
                    "delete from gha_computed where metric =$1",
                    &[RUNNING_FLAG.into()],
                );
                con.close();
                printf!("Running flag on '{}' force removed\n", db);
            }
        }
        true
    }

    /// `GHA2DB_SET_RUNNING_FLAG`: set the `devstats_running` flag in every
    /// project database. Returns the databases where it was set and the
    /// number of missing databases.
    fn set_running(&mut self) -> (Vec<String>, usize) {
        if self.ctx.debug > 0 {
            printf!("Setting running flag\n");
        }
        let mut set = Vec::new();
        let mut missing = 0;
        for proj in self.projs {
            let db = proj.pdb.as_str();
            let con = pg::pg_conn_db(self.ctx, db);
            let res = exec_sql(
                &con,
                self.ctx,
                "insert into gha_computed(metric, dt) select $1, now() where not exists(select 1 from gha_computed where metric = $2)",
                &[RUNNING_FLAG.into(), RUNNING_FLAG.into()],
            );
            if let Err(err) = res {
                if err.name() == consts::INVALID_CATALOG_NAME {
                    printf!("No '{}' database, cannot set running flag\n", db);
                    missing += 1;
                    con.close();
                    continue;
                }
                fatal_pg(err)
            }
            con.close();
            if self.ctx.debug > 0 {
                printf!("Set running flag on {}\n", db);
            }
            set.push(db.to_string());
        }
        (set, missing)
    }

    /// The deferred part of `GHA2DB_SET_RUNNING_FLAG`: clear the flag from the
    /// databases where it was set, retrying failures with a growing delay.
    fn clear_running(&mut self, dbs: &[String]) {
        if self.ctx.debug > 0 {
            printf!("Deleting running flag\n");
        }
        for db in dbs {
            let mut sleep_time = 1u64;
            loop {
                let con = pg::pg_conn_db(self.ctx, db);
                let res = exec_sql(
                    &con,
                    self.ctx,
                    "delete from gha_computed where metric = $1",
                    &[RUNNING_FLAG.into()],
                );
                con.close();
                match res {
                    Ok(_) => {
                        if self.ctx.debug > 0 || sleep_time > 1 {
                            printf!("Cleared running flag on {}\n", db);
                        }
                        break;
                    }
                    Err(err) => {
                        if sleep_time >= 90 {
                            fatalf!("something really bad happened, tried to clear running flag 90 times without success");
                        }
                        printf!(
                            "Failed to clear running flag on {}: {}, retrying after {} seconds\n",
                            db,
                            err,
                            sleep_time
                        );
                        std::thread::sleep(Duration::from_secs(sleep_time));
                        sleep_time += 1;
                    }
                }
            }
        }
    }
}

/// The sync itself (after the flag checks): PID file, `get_repos`, orphaned
/// locks, `gha2db_sync` per project, `website_data`. Returns Go's boolean
/// result, or the error of the deferred PID file removal (fatal in Go, after
/// the other deferred calls ran).
fn run_sync(
    ctx: &Ctx,
    cmd_prefix: &str,
    names: &[String],
    projs: &[projects::Project],
) -> Result<bool, String> {
    if ctx.skip_pid_file {
        return Ok(sync_projects(ctx, cmd_prefix, names, projs));
    }
    // Create the PID file; if it exists another instance is running.
    let pid_file = format!("/tmp/{}.pid", ctx.pid_file_root);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o700)
        .open(&pid_file);
    let mut file = match file {
        Ok(f) => f,
        Err(_) => {
            printf!(
                "Another `devstats` instance is running, PID file '{}' exists, exiting (not an error)\n",
                pid_file
            );
            return Ok(false);
        }
    };
    if let Err(e) = write!(file, "{}", std::process::id()) {
        fatal_on_error(format!(
            "write {}: {}",
            pid_file,
            devstatscode::error::go_io_error_string(&e)
        ));
    }
    if let Err(e) = file.sync_all().and(Ok(())) {
        fatal_on_error(format!(
            "close {}: {}",
            pid_file,
            devstatscode::error::go_io_error_string(&e)
        ));
    }
    drop(file);
    let result = sync_projects(ctx, cmd_prefix, names, projs);
    match std::fs::remove_file(&pid_file) {
        Ok(()) => Ok(result),
        Err(e) => Err(format!(
            "remove {}: {}",
            pid_file,
            devstatscode::error::go_io_error_string(&e)
        )),
    }
}

/// Runs `get_repos`, clears the orphaned locks, runs `gha2db_sync` for every
/// project and finally `website_data` (Go: the tail of `syncAllProjects`).
fn sync_projects(
    ctx: &Ctx,
    cmd_prefix: &str,
    names: &[String],
    projs: &[projects::Project],
) -> bool {
    // Only the clone/pull part runs here, the commit analysis is done by
    // `gha2db_sync` once the new commits are in the database.
    if !ctx.skip_get_repos {
        printf!("Updating git repos for all projects\n");
        let dt_start = Instant::now();
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        env.insert("GHA2DB_PROCESS_REPOS".into(), "1".into());
        env.insert("GHA2DB_FETCH_COMMITS_MODE".into(), "0".into());
        // the orphan commit restore is left to the per-project `gha2db_sync` step (get_repos with
        // GHA2DB_PROCESS_COMMITS), which runs it after the PushEvent commit backfill: restoring
        // first would claim the commits of pushes not yet backfilled under synthetic events
        env.insert("GHA2DB_RESTORE_ORPHAN_COMMITS".into(), String::new());
        if ctx.fetch_commits_mode == 2 {
            env.insert(
                "GHA2DB_FETCH_COMMITS_MODE".into(),
                ctx.fetch_commits_mode.to_string(),
            );
        }
        let res = exec_command(ctx, &[format!("{cmd_prefix}get_repos")], &env);
        let took = gotime::format_go_duration(dt_start.elapsed());
        if let Err(res) = res {
            printf!("Error updating git repos (took {}): {}\n", took, res);
            eprintln!(
                "{}: Error updating git repos (took {}): {}",
                now_v(),
                took,
                res
            );
            return false;
        }
        printf!("Updated git repos, took: {}\n", took);
    }

    // Remove `affs_lock` / `giant_lock` mutexes older than their maximum age.
    pg::api::clear_orphaned_locks();

    for (name, proj) in names.iter().zip(projs) {
        if let Some(probability) = proj.sync_probability {
            if rng::float64() >= probability {
                printf!("Skipping #{} {}\n", proj.order, name);
                continue;
            }
        }
        let mut proj_env: BTreeMap<String, String> = BTreeMap::new();
        proj_env.insert("GHA2DB_PROJECT".into(), name.clone());
        proj_env.insert("PG_DB".into(), proj.pdb.clone());
        proj_env.insert("ENV_SET".into(), "1".into());
        for (env_name, env_value) in &proj.env {
            proj_env.insert(env_name.clone(), env_value.clone());
        }
        printf!("Syncing #{} {}\n", proj.order, name);
        let dt_start = Instant::now();
        let res = exec_command(ctx, &[format!("{cmd_prefix}gha2db_sync")], &proj_env);
        let took = gotime::format_go_duration(dt_start.elapsed());
        if let Err(res) = res {
            printf!("Error result for {} (took {}): {}\n", name, took, res);
            eprintln!(
                "{}: Error result for {} (took {}): {}",
                now_v(),
                name,
                took,
                res
            );
            continue;
        }
        printf!("Synced {}, took: {}\n", name, took);
    }
    if ctx.website_data {
        printf!("Generating website data for all projects\n");
        let dt_start = Instant::now();
        let res = exec_command(
            ctx,
            &[format!("{cmd_prefix}website_data")],
            &BTreeMap::new(),
        );
        let took = gotime::format_go_duration(dt_start.elapsed());
        if let Err(res) = res {
            printf!("Error generating website data (took {}): {}\n", took, res);
            // sic: Go's stderr line says "website", not "website data"
            eprintln!(
                "{}: Error generating website (took {}): {}",
                now_v(),
                took,
                res
            );
            return false;
        }
        printf!("Generated website data, took: {}\n", took);
    }
    true
}

/// Go `syncAllProjects`: `Ok(true)` when everything was synced, `Ok(false)`
/// on a (non fatal) problem, `Err` when the deferred PID file removal failed.
fn sync_all_projects() -> Result<bool, String> {
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    let cmd_prefix = if ctx.local_cmd { "./" } else { "" };

    // Read the defined projects (no `/shared/` fallback here: `ioutil.ReadFile`).
    let data = fatal_on_err(io::read_file_raw(format!(
        "{data_prefix}{}",
        ctx.projects_yaml
    )));
    let all: projects::AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };
    let (names, projs) = projects::get_projects_list(&ctx, &all);

    if ctx.check_provision_flag {
        let missing = Flags {
            ctx: &mut ctx,
            projs: &projs,
        }
        .check_provisioned();
        if missing > 0 {
            printf!(
                "Not all databases provisioned, pending: {}, exiting\n",
                missing
            );
            return Ok(false);
        }
    }

    if ctx.check_running_flag {
        let mut flags = Flags {
            ctx: &mut ctx,
            projs: &projs,
        };
        if !flags.check_running() {
            return Ok(false);
        }
    }

    // Set the running flag now and clear it (Go: `defer`) before returning.
    let mut flag_dbs: Option<Vec<String>> = None;
    if ctx.set_running_flag {
        let mut flags = Flags {
            ctx: &mut ctx,
            projs: &projs,
        };
        let (set, missing) = flags.set_running();
        if missing > 0 {
            printf!("Not all databases present, missing: {}, exiting\n", missing);
            flags.clear_running(&set);
            return Ok(false);
        }
        flag_dbs = Some(set);
    }

    // Non-fatal exec mode: the sync of the next project(s) must run even if
    // the current one fails.
    ctx.exec_fatal = false;

    let result = run_sync(&ctx, cmd_prefix, &names, &projs);
    if let Some(dbs) = &flag_dbs {
        Flags {
            ctx: &mut ctx,
            projs: &projs,
        }
        .clear_running(dbs);
    }
    result
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    let _ = PROCESS_START.set(dt_start);
    let synced = sync_all_projects();
    let took = gotime::format_go_duration(dt_start.elapsed());
    match synced {
        Err(err) => fatal_on_error(err),
        Ok(true) => printf!("Synced all projects in: {}\n", took),
        Ok(false) => printf!("There were sync errors, took: {}\n", took),
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_print_like_go() {
        assert_eq!(duration_string(0), "0s");
        assert_eq!(duration_string(1_500_000_000), "1.5s");
        assert_eq!(duration_string(-90_000_000_000), "-1m30s");
        assert_eq!(
            gotime::format_go_duration(Duration::from_secs(9 * 3600)),
            "9h0m0s"
        );
    }

    #[test]
    fn now_v_has_the_monotonic_suffix() {
        let _ = PROCESS_START.set(Instant::now());
        let s = now_v();
        let (time, mono) = s.split_once(" m=+").unwrap();
        assert!(time.contains(" +") || time.contains(" -"), "{time}");
        let (secs, nanos) = mono.split_once('.').unwrap();
        assert!(secs.bytes().all(|b| b.is_ascii_digit()));
        assert_eq!(nanos.len(), 9);
    }
}
