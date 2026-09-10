//! Fatal error handling — port of `error.go` (`FatalOnError`, `Fatalf`, `FatalNoLog`).
//!
//! Contract kept from the Go library (this is what the rest of the DevStats
//! system relies on):
//!
//! * the error is reported on **stderr**, prefixed with a timestamp,
//! * unless `NO_FATAL_DELAY` is set to a non-empty value the process sleeps
//!   60 seconds before dying (so crash loops in cron/k8s stay visible and slow),
//! * the process terminates with **exit code 2** (the Go version `panic`s, and a
//!   Go panic exits with status 2).
//!
//! The PostgreSQL-specific retry/reconnect branches of the Go function
//! (`too_many_connections`, `cannot_connect_now`, `DURABLE_PQ`, ...) will be
//! added together with the database layer.

use std::fmt::Display;
use std::process;
use std::thread;
use std::time::Duration;

/// Seconds to wait before terminating, unless `NO_FATAL_DELAY` is set.
pub const FATAL_DELAY_SECS: u64 = 60;

/// Exit status used for fatal errors (matches a Go `panic`).
pub const FATAL_EXIT_CODE: i32 = 2;

fn fatal_delay_disabled() -> bool {
    std::env::var_os("NO_FATAL_DELAY").is_some_and(|v| !v.is_empty())
}

fn now_string() -> String {
    chrono::Local::now()
        .format("%Y-%m-%d %H:%M:%S%.9f %z %Z")
        .to_string()
}

/// Report `err` on stderr, optionally wait [`FATAL_DELAY_SECS`], then exit with
/// [`FATAL_EXIT_CODE`]. Never returns.
pub fn fatal_on_error<E: Display>(err: E) -> ! {
    eprintln!("Error(time={}):\nError: '{}'", now_string(), err);
    if !fatal_delay_disabled() {
        thread::sleep(Duration::from_secs(FATAL_DELAY_SECS));
    }
    eprintln!("panic: stacktrace: {}", err);
    process::exit(FATAL_EXIT_CODE)
}

/// Unwrap `res` or die via [`fatal_on_error`].
pub fn fatal_on_err<T, E: Display>(res: Result<T, E>) -> T {
    match res {
        Ok(v) => v,
        Err(e) => fatal_on_error(e),
    }
}

/// Die with a formatted message — port of `lib.Fatalf`.
///
/// ```ignore
/// devstatscode::fatalf(format_args!("NREPLACES must be positive"));
/// ```
pub fn fatalf(args: std::fmt::Arguments<'_>) -> ! {
    fatal_on_error(args.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_has_expected_shape() {
        let s = now_string();
        // e.g. "2026-09-10 17:29:38.033112345 +0000 UTC"
        assert!(s.len() >= 30, "unexpected timestamp: {s}");
        assert_eq!(&s[4..5], "-");
        assert_eq!(&s[10..11], " ");
        assert_eq!(&s[19..20], ".");
    }

    #[test]
    fn delay_flag_parsing() {
        // Cannot mutate the process environment safely in parallel tests, so
        // only check the current (test runner) state is consistent.
        let v = std::env::var_os("NO_FATAL_DELAY");
        assert_eq!(fatal_delay_disabled(), v.is_some_and(|v| !v.is_empty()));
    }
}
