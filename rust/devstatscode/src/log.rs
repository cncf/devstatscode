//! Logging — port of `log.go`.
//!
//! `printf` is the DevStats logger: it prints to stdout (prefixed with
//! `YYYY-MM-DD HH:MM:SS <project>/<program>: ` unless `GHA2DB_SKIPTIME` is set)
//! and, unless `GHA2DB_SKIPLOG` is set, records the message in the
//! `gha_logs` table of the `devstats` database.
//!
//! The first call initializes a private context (`Ctx::init()`), prints the
//! build information line (`Compiled ..., commit: ... on ... using ...`) when
//! `GHA2DB_DEBUG >= 0` and stores it in the log table too — exactly what the
//! Go library does.

use std::cell::Cell;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use chrono::{DateTime, FixedOffset, Local};

use crate::consts;
use crate::context::Ctx;
use crate::pg::{PgConn, SqlArg};
use crate::time::to_ymdhms_date;

/// Build stamp, e.g. `2026-09-10_05:00:00PM` (set at build time through the
/// `DEVSTATS_BUILD_STAMP` environment variable, see `compile.sh`).
pub const BUILD_STAMP: &str = match option_env!("DEVSTATS_BUILD_STAMP") {
    Some(v) => v,
    None => "None",
};
/// Git commit hash of the build (`DEVSTATS_GIT_HASH`).
pub const GIT_HASH: &str = match option_env!("DEVSTATS_GIT_HASH") {
    Some(v) => v,
    None => "None",
};
/// Host the binary was built on (`DEVSTATS_HOST_NAME`).
pub const HOST_NAME: &str = match option_env!("DEVSTATS_HOST_NAME") {
    Some(v) => v,
    None => "None",
};
/// Compiler version used for the build (`DEVSTATS_RUST_VERSION`).
pub const COMPILER_VERSION: &str = match option_env!("DEVSTATS_RUST_VERSION") {
    Some(v) => v,
    None => "None",
};

/// Data needed to make the DB log inserts.
pub struct LogContext {
    pub ctx: Ctx,
    pub con: PgConn,
    pub prog: String,
    pub proj: String,
    pub run_dt: DateTime<FixedOffset>,
}

static LOG_CTX: OnceLock<LogContext> = OnceLock::new();
static LOG_INITIALIZED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static INITIALIZING: Cell<bool> = const { Cell::new(false) };
}

/// Program name: last path component of `argv[0]`.
pub fn program_name() -> String {
    let arg0 = std::env::args().next().unwrap_or_default();
    arg0.rsplit('/').next().unwrap_or("").to_string()
}

fn new_log_context() -> LogContext {
    let mut ctx = Ctx::default();
    ctx.init();
    ctx.pg_db = consts::DEVSTATS.to_string();
    let con = crate::pg::pg_conn(&ctx);
    let prog = program_name();
    let proj = ctx.project.clone();
    // Go: `time.Now()` — the local zone; a `timestamp` column keeps the wall-clock time.
    let now = Local::now().fixed_offset();
    if ctx.debug >= 0 {
        let info = format!(
            "Compiled {}, commit: {} on {} using {}",
            BUILD_STAMP, GIT_HASH, HOST_NAME, COMPILER_VERSION
        );
        write_stdout(&format!("{}\n", info));
        let _ = crate::pg::exec_sql(
            &con,
            &ctx,
            &format!(
                "insert into gha_logs(prog, proj, run_dt, msg) {}",
                crate::pg::n_values(4)
            ),
            &[
                SqlArg::from(prog.as_str()),
                SqlArg::from(proj.as_str()),
                SqlArg::from(now),
                SqlArg::from(info.as_str()),
            ],
        );
    }
    // Go disables QOut around every log insert; the log context is private so
    // it can simply stay disabled.
    ctx.q_out = false;
    LogContext {
        ctx,
        con,
        prog,
        proj,
        run_dt: now,
    }
}

fn log_ctx() -> Option<&'static LogContext> {
    if let Some(lc) = LOG_CTX.get() {
        return Some(lc);
    }
    // A `printf` issued *while* the log context is being created (e.g. from
    // `Ctx::init()` reporting a bad variable) must not recurse into the
    // initialization: fall back to plain printing.
    if INITIALIZING.with(|f| f.get()) {
        return None;
    }
    INITIALIZING.with(|f| f.set(true));
    let lc = LOG_CTX.get_or_init(new_log_context);
    INITIALIZING.with(|f| f.set(false));
    LOG_INITIALIZED.store(true, Ordering::SeqCst);
    Some(lc)
}

fn write_stdout(s: &str) {
    write_stdout_bytes(s.as_bytes());
}

/// `fmt.Printf` to stdout: a write to a closed pipe kills the process with
/// `SIGPIPE` like Go's `os.epipecheck`, other write errors are ignored (Go's
/// `Printf` returns them and every caller drops the result).
fn write_stdout_bytes(s: &[u8]) {
    let mut out = std::io::stdout().lock();
    if let Err(e) = out.write_all(s).and_then(|()| out.flush()) {
        crate::error::die_on_stdio_epipe(&e);
    }
}

/// [`printf`] for a message that may not be valid UTF-8 (a Go string built
/// from raw column bytes): the bytes reach stdout unchanged, the `gha_logs`
/// copy is lossily converted.
pub fn printf_bytes(msg: &[u8]) -> usize {
    match std::str::from_utf8(msg) {
        Ok(s) => printf(s),
        Err(_) => {
            let Some(lc) = log_ctx() else {
                write_stdout_bytes(msg);
                return msg.len();
            };
            let mut out = Vec::new();
            if lc.ctx.log_time {
                out.extend_from_slice(
                    format!("{} {}/{}: ", to_ymdhms_date(Local::now()), lc.proj, lc.prog)
                        .as_bytes(),
                );
            }
            out.extend_from_slice(msg);
            write_stdout_bytes(&out);
            if lc.ctx.log_to_db {
                let lossy = String::from_utf8_lossy(msg).into_owned();
                let trimmed = lossy.trim_matches([' ', '\t', '\n', '\r']);
                let _ = crate::pg::exec_sql(
                    &lc.con,
                    &lc.ctx,
                    &format!(
                        "insert into gha_logs(prog, proj, run_dt, msg) {}",
                        crate::pg::n_values(4)
                    ),
                    &[
                        SqlArg::from(lc.prog.as_str()),
                        SqlArg::from(lc.proj.as_str()),
                        SqlArg::from(lc.run_dt),
                        SqlArg::from(trimmed),
                    ],
                );
            }
            out.len()
        }
    }
}

/// Print `msg` to stdout (with the time/project/program prefix when enabled)
/// and record it in the `gha_logs` table (Go `Printf`). Returns the number of
/// bytes written to stdout.
pub fn printf(msg: &str) -> usize {
    let Some(lc) = log_ctx() else {
        write_stdout(msg);
        return msg.len();
    };
    let out = if lc.ctx.log_time {
        format!(
            "{} {}/{}: {}",
            to_ymdhms_date(Local::now()),
            lc.proj,
            lc.prog,
            msg
        )
    } else {
        msg.to_string()
    };
    write_stdout(&out);
    if lc.ctx.log_to_db {
        let trimmed = msg.trim_matches([' ', '\t', '\n', '\r']);
        let _ = crate::pg::exec_sql(
            &lc.con,
            &lc.ctx,
            &format!(
                "insert into gha_logs(prog, proj, run_dt, msg) {}",
                crate::pg::n_values(4)
            ),
            &[
                SqlArg::from(lc.prog.as_str()),
                SqlArg::from(lc.proj.as_str()),
                SqlArg::from(lc.run_dt),
                SqlArg::from(trimmed),
            ],
        );
    }
    out.len()
}

/// `printf!` — formatted [`printf`].
#[macro_export]
macro_rules! printf {
    ($($arg:tt)*) => {
        $crate::log::printf(&format!($($arg)*))
    };
}

/// Go `ClearDBLogs`: delete `gha_logs` rows older than `GHA2DB_MAX_LOG_AGE`
/// from the `devstats` database (unless `GHA2DB_SKIP_PDB` is set).
pub fn clear_db_logs() {
    let mut ctx = Ctx::default();
    ctx.init();
    ctx.pg_db = consts::DEVSTATS.to_string();
    if ctx.skip_pdb {
        return;
    }
    let con = crate::pg::pg_conn(&ctx);
    println!("Clearing old DB logs.");
    crate::pg::exec_sql_with_err(
        &con,
        &ctx,
        &format!(
            "delete from gha_logs where dt < now() - '{}'::interval",
            ctx.clear_db_period
        ),
        &[],
    );
    con.close();
}

/// Has the logger been initialized (Go `IsLogInitialized`)?
pub fn is_log_initialized() -> bool {
    LOG_INITIALIZED.load(Ordering::SeqCst)
}

/// The initialized log context, if any.
pub fn log_context() -> Option<&'static LogContext> {
    LOG_CTX.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_defaults() {
        // Not set by `cargo test` (only compile.sh exports the variables).
        assert!(!BUILD_STAMP.is_empty());
        assert!(!GIT_HASH.is_empty());
        assert!(!HOST_NAME.is_empty());
        assert!(!COMPILER_VERSION.is_empty());
    }

    #[test]
    fn program_name_is_basename() {
        let p = program_name();
        assert!(!p.contains('/'));
        assert!(!p.is_empty());
    }
}
