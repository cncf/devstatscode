//! DevStats shared library — Rust port of the `github.com/cncf/devstatscode`
//! root Go package (`lib`).
//!
//! The port is being done program by program; modules are added here as the
//! ported binaries need them. Behaviour is *functionally* equivalent to the Go
//! library at the level that matters to the DevStats system (environment
//! variables, exit codes, output formats consumed by scripts) — it is not a
//! byte-for-byte imitation of Go runtime internals.

pub mod error;
pub mod goregex;

pub use error::{fatal_on_err, fatal_on_error, fatalf};
