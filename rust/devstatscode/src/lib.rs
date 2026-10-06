//! DevStats shared library — Rust port of the `github.com/cncf/devstatscode`
//! root Go package (`lib`).
//!
//! The port is being done program by program; modules are added here as the
//! ported binaries need them. Behaviour is *functionally* equivalent to the Go
//! library at the level that matters to the DevStats system (environment
//! variables, exit codes, output formats consumed by scripts) — it is not a
//! byte-for-byte imitation of Go runtime internals.
//!
//! Module ↔ Go file map:
//!
//! | Rust module | Go source |
//! |-------------|-----------|
//! | [`annotations`] | `annotations.go` |
//! | [`consts`]  | `const.go` |
//! | [`context`] | `context.go` |
//! | [`convert`] | `convert.go` |
//! | [`env`]     | `env.go` |
//! | [`error`]   | `error.go` |
//! | [`eventid`] | `eventid.go` (native event id bands) |
//! | [`exec`]    | `exec.go` |
//! | [`gobase64`] | Go `encoding/base64` `StdEncoding` decoding (Go error positions) |
//! | [`gofmt`]   | Go `fmt` `%v` renderings used in outputs |
//! | [`gomath`]  | bit-exact Go `math.Pow`/`Exp`/`Log`/`Frexp`/`Ldexp`/`Modf` |
//! | [`goregex`] | Go RE2 → Rust `regex` adapter |
//! | [`gourl`]   | Go `net/url` escaping/query parsing |
//! | [`hash`]    | `hash.go` |
//! | [`http`]    | Go `net/http` server subset (`ServeMux`, `ListenAndServe`) with Go's wire format |
//! | [`httpclient`] | `http.Get` (HTTPS via `ureq`/rustls) |
//! | [`io`]      | `io.go` |
//! | [`json`]    | `json.go` |
//! | [`log`]     | `log.go` |
//! | [`map`]     | `map.go` |
//! | [`mgetc`]   | `mgetc.go` |
//! | [`pg`]      | `pg_conn.go` + the used parts of `database/sql`/`lib/pq` (pure-Rust protocol client) |
//! | [`restore`] | `restore.go` (targeted `*_ids.sql` post-processing only) |
//! | [`rng`]     | `math/rand` usage |
//! | [`signal`]  | `signal.go` |
//! | [`string`]  | `string.go` |
//! | [`threads`] | `threads.go` |
//! | [`time`]    | `time.go` |
//! | [`trailers`] | `trailers.go` |
//! | [`ts_points`] | `ts_points.go` |
//! | [`unicode`] | `unicode.go` |
//! | [`yaml`]    | `yaml.go` |
//! | [`yamlv2`]  | byte-exact `gopkg.in/yaml.v2` encoder (`yaml.Marshal`) |

pub mod annotations;
pub mod broken_json;
pub mod computed;
pub mod consts;
pub mod context;
pub mod convert;
pub mod env;
pub mod error;
pub mod eventid;
pub mod exec;
pub mod gha;
pub mod ghapi;
pub mod ghawriter;
pub mod github;
pub mod gobase64;
pub mod gocsv;
pub mod gofmt;
pub mod gomath;
pub mod goregex;
pub mod gourl;
pub mod hash;
pub mod http;
pub mod httpclient;
pub mod io;
pub mod json;
pub mod log;
pub mod map;
pub mod mgetc;
pub mod pg;
pub mod project_filter;
pub mod projects;
pub mod restore;
pub mod rng;
pub mod signal;
pub mod string;
pub mod structure;
pub mod tags;
pub mod threads;
pub mod time;
pub mod trailers;
pub mod ts_points;
pub mod unicode;
pub mod yaml;
pub mod yamlv2;

/// The `chrono` crate used by the library (so dependants use the same version).
pub use chrono;
pub use context::Ctx;
pub use error::{fatal_no_log, fatal_on_err, fatal_on_error, fatalf};
pub use log::{is_log_initialized, printf, printf_bytes};

/// Process-wide allocator of every DevStats binary (all of them link this
/// library): mimalloc instead of the libc one.
///
/// The shipped executables are static musl builds and musl's malloc takes one
/// global lock, so allocation-heavy parallel work — gha2db decoding ~250k
/// JSON events per hour on `GHA2DB_NCPUS` workers — got *slower* with more
/// threads (k2s provisioning, 2026-10-06: going 6 -> 12 workers raised the
/// per-hour parse time from 45 s to 166 s, with ~40% of samples in `futex`
/// waits).  Go's runtime allocator scales per thread; mimalloc restores that
/// for the Rust port while keeping the single static binary.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(test, feature = "mimalloc"))]
mod global_allocator_tests {
    use std::alloc::{GlobalAlloc, Layout};

    #[test]
    fn global_allocator_is_mimalloc() {
        assert!(std::any::type_name_of_val(&super::GLOBAL_ALLOCATOR).ends_with("MiMalloc"));
    }

    #[test]
    fn global_allocator_serves_concurrent_allocations() {
        // Many short-lived allocations from several threads (the gha2db
        // pattern), plus one direct call through the GlobalAlloc trait.
        let handles: Vec<_> = (0..8)
            .map(|t| {
                std::thread::spawn(move || {
                    let mut total = 0usize;
                    for i in 0..20_000usize {
                        let v: Vec<u8> = vec![(i % 251) as u8; 16 + (i * 7 + t) % 2048];
                        total += v.len();
                    }
                    total
                })
            })
            .collect();
        for h in handles {
            assert!(h.join().expect("allocation worker panicked") > 0);
        }
        let layout = Layout::from_size_align(4096, 64).unwrap();
        // SAFETY: valid non-zero layout; the pointer is checked and freed with the same layout.
        unsafe {
            let p = super::GLOBAL_ALLOCATOR.alloc(layout);
            assert!(!p.is_null());
            assert_eq!(p as usize % 64, 0);
            super::GLOBAL_ALLOCATOR.dealloc(p, layout);
        }
    }
}
