//! Allocator scaling benchmark for the gha2db hot path: every line of one
//! (uncompressed) GH Archive hour file is decoded into [`gha::Event`] by each
//! of N threads, exactly like gha2db's workers do for N concurrent hours.
//!
//! Build it with and without the `mimalloc` feature (the library default) to
//! compare the allocators, in particular on static musl builds:
//!
//! ```text
//! cargo build --release --example gha_parse_bench -p devstatscode
//! cargo build --release --example gha_parse_bench -p devstatscode --no-default-features
//! gha_parse_bench 2025-02-25-10.json 12 [rounds]
//! ```
//!
//! `per-thread-hour` is the wall time one worker needs for one hour while
//! N workers run — the number to compare with gha2db's `Split` -> `Parsed`
//! log timestamps.

use devstatscode::gha::Event;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: {} <hour.json> <threads> [rounds]", args[0]);
        std::process::exit(1);
    }
    let data = std::fs::read(&args[1]).expect("cannot read the hour file");
    let threads: usize = args[2].parse().expect("threads must be a number");
    let rounds: usize = args
        .get(3)
        .map(|r| r.parse().expect("rounds must be a number"))
        .unwrap_or(1);
    let lines: Vec<&[u8]> = data
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    let start = Instant::now();
    let decoded: usize = std::thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let lines = &lines;
                s.spawn(move || {
                    let mut ok = 0usize;
                    for _ in 0..rounds {
                        for line in lines {
                            if serde_json::from_slice::<Event>(line).is_ok() {
                                ok += 1;
                            }
                        }
                    }
                    ok
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| w.join().expect("benchmark worker panicked"))
            .sum()
    });
    let secs = start.elapsed().as_secs_f64();
    println!(
        "allocator={} threads={} lines={} decoded={} wall={:.2}s per-thread-hour={:.2}s events/s={:.0}",
        if cfg!(feature = "mimalloc") { "mimalloc" } else { "libc" },
        threads,
        lines.len(),
        decoded,
        secs,
        secs / rounds as f64,
        decoded as f64 / secs
    );
}
