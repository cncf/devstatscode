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
pub mod consts;
pub mod context;
pub mod convert;
pub mod env;
pub mod error;
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
