//! Run-duration timeout via `SIGALRM` — port of `signal.go`.

use std::thread;
use std::time::Duration;

use crate::context::Ctx;
use crate::log::printf;

/// Program name used for `GHA2DB_MAX_RUN_DURATION` lookups: the basename of
/// `argv[0]` without its last extension (Go `SetupTimeoutSignal`).
pub fn timeout_program_name() -> String {
    let arg0 = std::env::args().next().unwrap_or_default();
    let base = std::path::Path::new(&arg0)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_default();
    strip_last_extension(&base)
}

fn strip_last_extension(prog: &str) -> String {
    let parts: Vec<&str> = prog.split('.').collect();
    if parts.len() > 1 {
        parts[..parts.len() - 1].join(".")
    } else {
        prog.to_string()
    }
}

/// Sleep `seconds`, then send `SIGALRM` to the current process (Go
/// `FinishAfterTimeout`).
pub fn finish_after_timeout(prog: &str, seconds: i64, status: i64) {
    thread::sleep(Duration::from_secs(seconds.max(0) as u64));
    printf(&format!(
        "Program '{}' reached timeout after {} seconds, sending signal to exit {}\n",
        prog, seconds, status
    ));
    if let Err(e) = signal_hook::low_level::raise(signal_hook::consts::SIGALRM) {
        printf(&format!(
            "Error: {} sending '{}' timeout signal after {} seconds, exiting {} status\n",
            crate::error::go_io_error_string(&e),
            prog,
            seconds,
            status
        ));
        std::process::exit(status as i32);
    }
    printf(&format!(
        "Program '{}': sent timeout signal after {} seconds, requesting {} exit status\n",
        prog, seconds, status
    ));
}

/// Install the run-duration watchdog when `GHA2DB_MAX_RUN_DURATION` has an
/// entry for the current program (Go `SetupTimeoutSignal`).
pub fn setup_timeout_signal(ctx: &Ctx) {
    let prog = timeout_program_name();
    let Some(data) = ctx.max_run_duration.get(&prog) else {
        return;
    };
    let (seconds, status) = (data[0], data[1]);
    if seconds <= 0 {
        return;
    }
    let allow_metric_fail = ctx.allow_metric_fail;
    {
        let prog = prog.clone();
        thread::spawn(move || finish_after_timeout(&prog, seconds, status));
    }
    let mut signals = match signal_hook::iterator::Signals::new([signal_hook::consts::SIGALRM]) {
        Ok(s) => s,
        Err(e) => crate::error::fatal_on_error(crate::error::go_io_error_string(&e)),
    };
    {
        let prog = prog.clone();
        thread::spawn(move || {
            for _sig in signals.forever() {
                if prog == "calc_metric" && allow_metric_fail {
                    printf(&format!(
                        "Program '{}': timeout alarm clock after {} seconds, will exit with {} code, but will not fail due to this\n",
                        prog, seconds, status
                    ));
                } else {
                    printf(&format!(
                        "Program '{}': timeout alarm clock after {} seconds, will exit with {} code\n",
                        prog, seconds, status
                    ));
                    std::process::exit(status as i32);
                }
            }
        });
    }
    printf(&format!(
        "Program '{}': timeout handler installed: exit {} after {} seconds\n",
        prog, status, seconds
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_stripping() {
        assert_eq!(strip_last_extension("calc_metric"), "calc_metric");
        assert_eq!(strip_last_extension("calc_metric.exe"), "calc_metric");
        assert_eq!(strip_last_extension("a.b.c"), "a.b");
        assert_eq!(strip_last_extension(""), "");
    }

    #[test]
    fn program_name_has_no_dir() {
        assert!(!timeout_program_name().contains('/'));
    }
}
