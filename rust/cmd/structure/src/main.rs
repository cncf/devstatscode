//! `structure` — Rust port of `cmd/structure/structure.go`.
//!
//! Creates the project database if needed and (re)creates the DevStats
//! database structure: tables (`GHA2DB_SKIPTABLE` disables), indexes
//! (`GHA2DB_INDEX` enables), and the "tools" — postprocess scripts, country
//! codes, bot logins, affiliations (`GHA2DB_SKIPTOOLS` disables). When the
//! database already exists the program asks `Continue? (y/n)` (answered
//! non-interactively through `GHA2DB_MGETC=y`). Environment, output and exit
//! codes are those of the Go program.

use std::time::Instant;

use devstatscode::{mgetc, pg, printf, signal, structure, time as gotime, Ctx};

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // Create database if needed
    let created_database = pg::create_database_if_needed(&mut ctx);

    // If we are using existing database, then display warnings
    // And ask for continue
    if !created_database {
        if ctx.table {
            printf!("This program will recreate DB structure (dropping all existing data)\n");
        }
        printf!("Continue? (y/n) ");
        let c = mgetc::mgetc(&ctx);
        printf!("\n");
        if c == "y" {
            structure::structure(&ctx);
        }
    } else {
        structure::structure(&ctx);
    }
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}
