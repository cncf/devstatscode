//! Thread count selection — port of `threads.go`.

use crate::context::Ctx;

/// Number of logical CPUs available to this process (Go `runtime.NumCPU()`).
pub fn num_cpu() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Number of worker threads to use: `GHA2DB_NCPUS` (clamped to the machine's
/// CPU count) if set, `1` when `GHA2DB_ST` is set, otherwise all CPUs.
pub fn get_threads_num(ctx: &mut Ctx) -> usize {
    ctx.set_cpus();
    if ctx.ncpus > 0 {
        let n = num_cpu() as i64;
        if ctx.ncpus > n {
            ctx.ncpus = n;
        }
        return ctx.ncpus as usize;
    }
    if ctx.st {
        return 1;
    }
    num_cpu()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::test_support::{env_lock, set_or_unset};

    #[test]
    fn go_table() {
        let _g = env_lock();
        let mut ctx = Ctx::default();
        ctx.init();
        ctx.test_mode = true;
        set_or_unset("GHA2DB_ST", None);
        set_or_unset("GHA2DB_NCPUS", None);
        let n = get_threads_num(&mut ctx);
        let cases: [(bool, i64, usize); 9] = [
            (false, 0, n),
            (false, 1, 1),
            (false, -1, n),
            (false, 2, 2.min(n)),
            (true, 0, 1),
            (true, 1, 1),
            (true, -1, 1),
            (true, 2, 2.min(n)),
            (true, n as i64 + 1, n),
        ];
        for (i, (st, ncpus, expected)) in cases.iter().enumerate() {
            ctx.st = *st;
            ctx.ncpus = *ncpus;
            set_or_unset("GHA2DB_ST", if *st { Some("1") } else { None });
            let ncpus_s = ncpus.to_string();
            set_or_unset(
                "GHA2DB_NCPUS",
                if *ncpus > 0 { Some(&ncpus_s) } else { None },
            );
            let got = get_threads_num(&mut ctx);
            assert_eq!(got, *expected, "test number {}", i + 1);
        }
        set_or_unset("GHA2DB_ST", None);
        set_or_unset("GHA2DB_NCPUS", None);
    }
}
