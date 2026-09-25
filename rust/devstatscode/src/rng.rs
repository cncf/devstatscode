//! Process-wide pseudo random numbers (replacement for Go's `math/rand`
//! global source, which DevStats seeds from the clock).
//!
//! xoshiro256** seeded from `std::hash::RandomState` (per-process random) and
//! the wall clock; no external dependency. Not cryptographic — none of the Go
//! uses (`RandString`, task shuffling, retry sleeps) need that.

use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

struct Xoshiro256 {
    s: [u64; 4],
}

impl Xoshiro256 {
    fn seeded() -> Self {
        let mut seed = [0u64; 4];
        let rs = RandomState::new();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15);
        for (i, slot) in seed.iter_mut().enumerate() {
            let mut h = rs.build_hasher();
            h.write_u64(nanos ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            h.write_u64(std::process::id() as u64);
            *slot = splitmix64(h.finish());
        }
        if seed.iter().all(|&x| x == 0) {
            seed[0] = 1;
        }
        Xoshiro256 { s: seed }
    }

    fn next_u64(&mut self) -> u64 {
        let s = &mut self.s;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

static RNG: Mutex<Option<Xoshiro256>> = Mutex::new(None);

/// Next pseudo random `u64`.
pub fn next_u64() -> u64 {
    let mut guard = RNG.lock().unwrap_or_else(|p| p.into_inner());
    guard.get_or_insert_with(Xoshiro256::seeded).next_u64()
}

/// Uniform integer in `0..n` (`n > 0`), like Go's `rand.Intn`.
pub fn intn(n: u64) -> u64 {
    assert!(n > 0, "intn: n must be positive");
    // rejection sampling to avoid modulo bias
    let zone = u64::MAX - (u64::MAX % n);
    loop {
        let v = next_u64();
        if v < zone {
            return v % n;
        }
    }
}

/// Uniform float in `[0.0, 1.0)`, like Go's `rand.Float64`.
pub fn float64() -> f64 {
    (next_u64() >> 11) as f64 / (1u64 << 53) as f64
}

/// Fisher–Yates shuffle (Go `rand.Shuffle` equivalent).
pub fn shuffle<T>(items: &mut [T]) {
    for i in (1..items.len()).rev() {
        let j = intn(i as u64 + 1) as usize;
        items.swap(i, j);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produces_varied_values() {
        let a: Vec<u64> = (0..8).map(|_| next_u64()).collect();
        assert!(a.iter().any(|&x| x != a[0]));
        for _ in 0..1000 {
            assert!(intn(7) < 7);
            let f = float64();
            assert!((0.0..1.0).contains(&f));
        }
        let mut v: Vec<u32> = (0..50).collect();
        shuffle(&mut v);
        let mut sorted = v.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..50).collect::<Vec<_>>());
    }
}
