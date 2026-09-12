//! `tags` — Rust port of `cmd/tags/tags.go`.
//!
//! Reads the tag definitions (`GHA2DB_TAGS_YAML`, default
//! `metrics/<project>/tags.yaml`) and, for every tag, runs its SQL and writes
//! the resulting values as a `t<series_name>` tag series into the project's
//! PostgreSQL TSDB (`lib.ProcessTag`). Tags are processed concurrently by up
//! to `GetThreadsNum` workers (`GHA2DB_ST`, `GHA2DB_NCPUS`). Environment,
//! output and exit codes are those of the Go program.

use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use devstatscode::{fatal_on_error, io, pg, printf, signal, tags, threads, time as gotime, Ctx};

/// Insert TSDB tags (Go `calcTags`).
fn calc_tags() {
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // Connect to Postgres DB
    let con = pg::pg_conn(&ctx);

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read tags to generate
    let data = match io::read_file(&ctx, &format!("{data_prefix}{}", ctx.tags_yaml)) {
        Ok(d) => d,
        Err(e) => fatal_on_error(e),
    };
    let all_tags = match tags::parse_tags(&data) {
        Ok(t) => t,
        Err(e) => fatal_on_error(e),
    };

    let thr_n = threads::get_threads_num(&mut ctx);
    let ctx = &ctx;
    let con = &con;
    // Iterate tags: one worker per tag, at most `thr_n` running at a time,
    // synchronised through a channel exactly like the Go goroutines.
    thread::scope(|s| {
        // Unbuffered like the Go `make(chan bool)`: a worker's send completes
        // only when the main thread receives it.
        let (tx, rx) = mpsc::sync_channel::<bool>(0);
        let mut n_threads = 0usize;
        for tg in &all_tags.tags {
            let tx = tx.clone();
            s.spawn(move || {
                if ctx.debug > 0 {
                    printf!("Start Tag '{}' --> '{}'\n", tg.name, tg.series_name);
                }

                // Process tag
                tags::process_tag(con, ctx, tg, &[]);

                if ctx.debug > 0 {
                    printf!("End Tag '{}' --> '{}'\n", tg.name, tg.series_name);
                }
                // Synchronize go routine
                let _ = tx.send(true);
                if ctx.debug > 0 {
                    printf!("Synced tag '{}' --> '{}'\n", tg.name, tg.series_name);
                }
            });
            n_threads += 1;
            if n_threads >= thr_n {
                if ctx.debug > 0 {
                    printf!(
                        "threading: {} >= {}, waiting on the channel\n",
                        n_threads,
                        thr_n
                    );
                }
                let _ = rx.recv();
                n_threads -= 1;
                if ctx.debug > 0 {
                    printf!("threading: thread joined, num threads: {}\n", n_threads);
                }
            }
        }
        // Usually all work happens on '<-ch'
        printf!("Final {} threads join\n", n_threads);
        while n_threads > 0 {
            let _ = rx.recv();
            n_threads -= 1;
            if ctx.debug > 0 {
                printf!(
                    "threading: fianl thread joined, num threads: {}\n",
                    n_threads
                );
            }
        }
    });
    // `defer func() { lib.FatalOnError(con.Close()) }()` — closing the pool
    // cannot fail here.
    con.close();
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    calc_tags();
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}
