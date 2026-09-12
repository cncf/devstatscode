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
//! (`too_many_connections`, `cannot_connect_now`, `DURABLE_PQ`, ...) live in
//! [`crate::pg::fatal_on_pg_error`].

use std::cell::RefCell;
use std::fmt::Display;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

/// Seconds to wait before terminating, unless `NO_FATAL_DELAY` is set.
pub const FATAL_DELAY_SECS: u64 = 60;

/// Exit status used for fatal errors (matches a Go `panic`).
pub const FATAL_EXIT_CODE: i32 = 2;

/// A registered deferred call: id + closure.
type DeferEntry = (u64, Box<dyn FnOnce()>);

thread_local! {
    static DEFERS: RefCell<Vec<DeferEntry>> = const { RefCell::new(Vec::new()) };
}
static NEXT_DEFER_ID: AtomicU64 = AtomicU64::new(1);

/// A deferred call registered with [`defer`]; dropping it (the normal end of
/// the scope) runs the closure.
#[must_use = "the deferred call runs when the guard is dropped"]
pub struct Defer {
    id: u64,
}

/// Go `defer` for code that must also run when the thread dies through a
/// fatal error: a Go `panic` (which is what `FatalOnError` does) runs the
/// deferred calls of the panicking goroutine while unwinding, so e.g.
/// `calc_metric`'s deferred `setLastComputed` still writes its row when the
/// metric fails. The closure runs once: when the returned guard is dropped,
/// or — if the current thread first hits [`fatal_on_error`]/[`fatal_no_log`]
/// or a Rust panic under [`exit_on_panic`] — right before the process exits
/// (LIFO, only the defers of that thread, like Go). `os.Exit`-style exits
/// (`process::exit`) skip them, also like Go.
pub fn defer(f: impl FnOnce() + 'static) -> Defer {
    let id = NEXT_DEFER_ID.fetch_add(1, Ordering::SeqCst);
    DEFERS.with(|d| d.borrow_mut().push((id, Box::new(f))));
    Defer { id }
}

impl Drop for Defer {
    fn drop(&mut self) {
        let f = DEFERS.with(|d| {
            let mut v = d.borrow_mut();
            v.iter()
                .position(|(id, _)| *id == self.id)
                .map(|pos| v.remove(pos).1)
        });
        if let Some(f) = f {
            f();
        }
    }
}

/// Run (and forget) the pending deferred calls of this thread, last first.
/// A deferred call that is itself fatal re-enters here and continues with
/// the remaining ones.
pub fn run_defers() {
    loop {
        let next = DEFERS.with(|d| d.borrow_mut().pop());
        match next {
            Some((_, f)) => f(),
            None => break,
        }
    }
}

fn fatal_delay_disabled() -> bool {
    std::env::var_os("NO_FATAL_DELAY").is_some_and(|v| !v.is_empty())
}

/// Go `%+v` of `time.Now()` (without the monotonic clock suffix), used as the
/// timestamp of error reports.
pub fn now_string() -> String {
    let now = chrono::Local::now();
    // chrono has no zone *names*; Go prints e.g. `+0000 UTC` / `+0200 CEST`.
    // DevStats runs in UTC, so name that one and fall back to the numeric
    // offset (Go does the same for zones it cannot name).
    let zone = if now.offset().local_minus_utc() == 0 {
        "UTC".to_string()
    } else {
        now.format("%z").to_string()
    };
    format!("{} {}", now.format("%Y-%m-%d %H:%M:%S%.9f %z"), zone)
}

/// Report `err` on stderr, optionally wait [`FATAL_DELAY_SECS`], then exit with
/// [`FATAL_EXIT_CODE`]. Never returns.
pub fn fatal_on_error<E: Display>(err: E) -> ! {
    eprintln!("Error(time={}):\nError: '{}'", now_string(), err);
    if !fatal_delay_disabled() {
        thread::sleep(Duration::from_secs(FATAL_DELAY_SECS));
    }
    run_defers();
    eprintln!("panic: stacktrace: {}", err);
    process::exit(FATAL_EXIT_CODE)
}

/// Like [`fatal_on_error`] but ends in a `panic!` instead of exiting — for
/// code running inside an HTTP handler, where Go's `net/http` recovers the
/// panic (`http: panic serving <addr>: stacktrace: …` on stderr), closes the
/// connection and keeps serving; [`crate::http`] does the same.
pub fn fatal_on_error_in_handler<E: Display>(err: E) -> ! {
    eprintln!("Error(time={}):\nError: '{}'", now_string(), err);
    if !fatal_delay_disabled() {
        thread::sleep(Duration::from_secs(FATAL_DELAY_SECS));
    }
    panic!("stacktrace: {}", err)
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

/// `fatalf!("...", args)` — formatted fatal error (Go `lib.Fatalf`).
#[macro_export]
macro_rules! fatalf {
    ($($arg:tt)*) => {
        $crate::error::fatalf(format_args!($($arg)*))
    };
}

/// Fatal error for the very early init state (before logging is available) —
/// port of `lib.FatalNoLog`. Same stderr format, delay and exit code as
/// [`fatal_on_error`].
pub fn fatal_no_log<E: Display>(err: E) -> ! {
    eprintln!(
        "Error(time={}):\nError: '{}'\nStacktrace:",
        now_string(),
        err
    );
    if !fatal_delay_disabled() {
        thread::sleep(Duration::from_secs(FATAL_DELAY_SECS));
    }
    run_defers();
    eprintln!("panic: stacktrace: {}", err);
    process::exit(FATAL_EXIT_CODE)
}

/// Die from `SIGPIPE` the way a Go program does when a write to stdout or
/// stderr hits a closed pipe (`os.epipecheck` → `runtime.sigpipe` →
/// `dieFromSignal`): nothing is printed, no deferred functions run and the
/// parent sees the process killed by signal 13 (`141` in a shell). Rust
/// ignores `SIGPIPE` at start-up (so writes return `EPIPE` instead), hence the
/// explicit reset + raise. Falls back to exit status [`FATAL_EXIT_CODE`] like
/// Go if the signal does not terminate the process.
pub fn die_from_sigpipe() -> ! {
    // SAFETY: plain libc calls with constant arguments; resetting the
    // disposition and raising the signal has no memory-safety implications.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
        libc::raise(libc::SIGPIPE);
    }
    process::exit(FATAL_EXIT_CODE)
}

/// Port of Go's `os.epipecheck` for a failed write to **stdout or stderr**: an
/// `EPIPE` error kills the process with `SIGPIPE` ([`die_from_sigpipe`]), any
/// other error is left to the caller (Go returns it from `Write`).
pub fn die_on_stdio_epipe(err: &std::io::Error) {
    if err.kind() == std::io::ErrorKind::BrokenPipe {
        die_from_sigpipe();
    }
}

/// Is this the panic `print!`/`println!`/`eprint!`/`eprintln!` raise when
/// stdout/stderr is a closed pipe (`failed printing to stdout: Broken pipe (os
/// error 32)`)? Go's `fmt.Printf` dies from `SIGPIPE` in that situation.
pub fn is_stdio_broken_pipe_panic(info: &std::panic::PanicHookInfo<'_>) -> bool {
    let msg = panic_message(info);
    (msg.starts_with("failed printing to stdout") || msg.starts_with("failed printing to stderr"))
        && msg.contains("Broken pipe")
}

fn panic_message(info: &std::panic::PanicHookInfo<'_>) -> String {
    info.payload()
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| info.payload().downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// For binaries that keep Rust's default panic behaviour (the `api`/`webhook`
/// servers keep serving after a handler thread panics, like `net/http`
/// recovering a handler): only the closed-stdout/stderr case is turned into
/// the Go `SIGPIPE` death, every other panic goes to the previous hook.
pub fn sigpipe_like_go() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if is_stdio_broken_pipe_panic(info) {
            die_from_sigpipe();
        }
        previous(info);
    }));
}

/// Make an unexpected Rust panic terminate the process like a Go runtime panic:
/// `panic: <message>` on stderr and exit status [`FATAL_EXIT_CODE`] (2) instead
/// of Rust's default 101 — and a `print!` to a closed stdout/stderr pipe dies
/// from `SIGPIPE` like `fmt.Printf` does. Call once at the top of `main`.
pub fn exit_on_panic() {
    std::panic::set_hook(Box::new(|info| {
        if is_stdio_broken_pipe_panic(info) {
            die_from_sigpipe();
        }
        let msg = panic_message(info);
        match info.location() {
            Some(loc) => eprintln!("panic: {msg} [{}:{}]", loc.file(), loc.line()),
            None => eprintln!("panic: {msg}"),
        }
        run_defers();
        process::exit(FATAL_EXIT_CODE);
    }));
}

/// Render an I/O error the way Go's `syscall.Errno` does: lowercase, without
/// Rust's ` (os error N)` suffix, e.g. `no such file or directory`.
pub fn go_io_error_string(err: &std::io::Error) -> String {
    let s = err.to_string();
    let s = match s.rfind(" (os error ") {
        Some(pos) if s.ends_with(')') => &s[..pos],
        _ => s.as_str(),
    };
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_uppercase() && !s.starts_with("EOF") => {
            c.to_ascii_lowercase().to_string() + chars.as_str()
        }
        _ => s.to_string(),
    }
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

    #[test]
    fn io_error_strings_look_like_go() {
        let e = std::io::Error::from_raw_os_error(2);
        assert_eq!(go_io_error_string(&e), "no such file or directory");
        let e = std::io::Error::from_raw_os_error(13);
        assert_eq!(go_io_error_string(&e), "permission denied");
        let e = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF");
        assert_eq!(go_io_error_string(&e), "EOF");
    }

    #[test]
    fn defers_run_once_lifo_and_per_thread() {
        use std::rc::Rc;
        let log: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
        {
            let l = log.clone();
            let _a = defer(move || l.borrow_mut().push("a"));
            let l = log.clone();
            let b = defer(move || l.borrow_mut().push("b"));
            let l = log.clone();
            let _c = defer(move || l.borrow_mut().push("c"));
            drop(b);
            assert_eq!(*log.borrow(), vec!["b"]);
            // A fatal path runs the remaining ones (last first)…
            run_defers();
            assert_eq!(*log.borrow(), vec!["b", "c", "a"]);
            // …and the guards do not run them again.
        }
        assert_eq!(*log.borrow(), vec!["b", "c", "a"]);
        // Defers registered on another thread are not seen here.
        let (tx, rx) = std::sync::mpsc::channel();
        let t = std::thread::spawn(move || {
            let tx2 = tx.clone();
            let d = defer(move || tx2.send("other").unwrap());
            std::mem::forget(d);
            tx.send("registered").unwrap();
            std::thread::park_timeout(Duration::from_millis(200));
        });
        assert_eq!(rx.recv().unwrap(), "registered");
        run_defers();
        t.join().unwrap();
        assert!(rx.try_recv().is_err(), "other thread's defer ran here");
        assert!(log.borrow().len() == 3);
    }
}
