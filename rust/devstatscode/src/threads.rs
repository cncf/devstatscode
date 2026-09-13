//! Thread count selection — port of `threads.go`.

use crate::context::Ctx;

/// Number of logical CPUs available to this process (Go `runtime.NumCPU()`).
///
/// Go's `NumCPU()` is the popcount of the process' `sched_getaffinity` mask
/// and deliberately ignores cgroup CPU quotas, whereas Rust's
/// `available_parallelism()` additionally caps the count to the cgroup
/// `cpu.max` bandwidth limit.  Inside a Kubernetes pod with e.g.
/// `GHA2DB_NCPUS=8` and `limits.cpu: 6` Go therefore runs 8 workers while the
/// std answer would clamp that to 6 — so mirror Go: affinity mask only, with
/// `available_parallelism()` as the fallback for other platforms / failures.
pub fn num_cpu() -> usize {
    affinity_cpu_count().unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn affinity_cpu_count() -> Option<usize> {
    // SAFETY: `cpu_set_t` is plain old data, a zeroed value is a valid empty
    // set, and `sched_getaffinity` only writes into the buffer we hand it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        let rc = libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set);
        if rc != 0 {
            return None;
        }
        let n = libc::CPU_COUNT(&set);
        if n > 0 {
            Some(n as usize)
        } else {
            None
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn affinity_cpu_count() -> Option<usize> {
    None
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

    /// Go `runtime.NumCPU()` never reports fewer CPUs than the (cgroup-aware)
    /// std answer: the affinity mask is exactly what std starts from before
    /// applying the `cpu.max` quota.
    #[test]
    fn num_cpu_is_at_least_available_parallelism() {
        let n = num_cpu();
        assert!(n >= 1);
        let std_n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert!(n >= std_n, "num_cpu {n} < available_parallelism {std_n}");
    }

    /// Independent cross-check against the kernel's own view of the affinity
    /// mask (`Cpus_allowed_list` in `/proc/self/status`, e.g. `0-3,8,10-11`).
    #[cfg(target_os = "linux")]
    #[test]
    fn num_cpu_matches_proc_self_status_affinity() {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let list = status
            .lines()
            .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
            .expect("Cpus_allowed_list in /proc/self/status")
            .trim();
        let mut expected = 0usize;
        for part in list.split(',') {
            let part = part.trim();
            if let Some((a, b)) = part.split_once('-') {
                let a: usize = a.parse().unwrap();
                let b: usize = b.parse().unwrap();
                expected += b - a + 1;
            } else {
                let _: usize = part.parse().unwrap();
                expected += 1;
            }
        }
        assert_eq!(num_cpu(), expected, "Cpus_allowed_list: {list}");
        assert_eq!(affinity_cpu_count(), Some(expected));
    }

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
