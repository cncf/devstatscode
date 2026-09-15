//! GHA event writer shared by `gha2db` (hourly GH Archive files) and
//! `ghapi2db` (the per-repository events feed of the GitHub API, same JSON
//! shape) — port of the `gha*`/`lookup*`/`find*`/`eventExists*`/`writeToDB*`
//! functions of `cmd/gha2db/gha2db.go` (Go: root-package `ghawriter.go`).

pub mod db;
pub mod writer;

pub use db::{cache_len, lookup_actor_name_email, Db, MaybeHide};
pub use writer::{
    upgrade_pull_request_stubs, write_to_db, write_to_db_old_fmt, STUB_CREATED_AT_CUT,
};
