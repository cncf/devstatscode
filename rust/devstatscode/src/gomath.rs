//! Bit-exact ports of the Go `math` functions DevStats uses for scheduling
//! weights: [`pow`], [`exp`], [`log`], [`frexp`], [`ldexp`], [`modf`].
//!
//! Go's `math.Pow` prints its results (`size^power` weights in `splitcrons`),
//! so an "almost equal" result from the platform libm would show up as a
//! different last digit and, in unlucky cases, a different cron minute. The
//! functions here follow the Go sources operation by operation:
//!
//! * `pow` — `src/math/pow.go` (portable implementation used on amd64/arm64):
//!   special cases, `y == ±0.5` → `Sqrt`, integer part by repeated squaring of
//!   the `Frexp` mantissa, fractional part via `Exp(yf * Log(x))`.
//! * `log` — `src/math/log_amd64.s` (the FreeBSD `e_log.c` algorithm; same
//!   arithmetic as the portable `log.go` for normal inputs, including the
//!   assembly's non-normalizing treatment of subnormal inputs).
//! * `exp` — `src/math/exp_amd64.s` (SLEEF-based Taylor evaluation with the
//!   argument reduced by `LN2U`/`LN2L`). The assembly has two code paths and
//!   picks one at runtime: fused multiply-adds when the CPU has AVX+FMA,
//!   separate multiply/add otherwise. [`exp`] does the same
//!   (`is_x86_feature_detected!`), so it matches the Go binary running on the
//!   same machine; on non-x86 targets the FMA variant is used (Go's arm64
//!   assembly uses fused operations as well).
//!
//! `f64::mul_add` is a correctly rounded fused multiply-add on every platform
//! (hardware or libm `fma`), hence identical to the `VFMADD` instructions.
//!
//! Verified against 800+ Go-generated vectors in `tests/gomath_vectors.rs`.

// Constants are copied verbatim from the Go sources (traceability beats prettiness).
#![allow(
    clippy::excessive_precision,
    clippy::approx_constant,
    clippy::unreadable_literal
)]

/// Go `math.Modf`: integer and fractional parts (both carry the sign of `f`).
pub fn modf(f: f64) -> (f64, f64) {
    if f < 1.0 {
        if f < 0.0 {
            let (int, frac) = modf(-f);
            return (-int, -frac);
        } else if f == 0.0 {
            return (f, f); // Return -0, -0 when f == -0
        }
        return (0.0, f);
    }
    let int = f.trunc();
    (int, f - int)
}

const SMALLEST_NORMAL: f64 = 2.2250738585072014e-308;

/// Go `math.normalize`: subnormals are scaled by 2⁵² (exponent adjusted by -52).
fn normalize(x: f64) -> (f64, i64) {
    if x.abs() < SMALLEST_NORMAL {
        return (x * (1u64 << 52) as f64, -52);
    }
    (x, 0)
}

/// Go `math.Frexp`: `f = frac × 2^exp` with `|frac|` in `[0.5, 1)`.
pub fn frexp(f: f64) -> (f64, i64) {
    if f == 0.0 || f.is_infinite() || f.is_nan() {
        return (f, 0);
    }
    let (f, e) = normalize(f);
    let mut bits = f.to_bits();
    let exp = ((bits >> 52) & 0x7ff) as i64 - 1022 + e;
    bits &= !(0x7ffu64 << 52);
    bits |= 1022u64 << 52;
    (f64::from_bits(bits), exp)
}

/// Go `math.Ldexp`: `frac × 2^exp`.
pub fn ldexp(frac: f64, exp: i64) -> f64 {
    if frac == 0.0 || frac.is_infinite() || frac.is_nan() {
        return frac;
    }
    let (frac, e) = normalize(frac);
    let mut exp = exp + e;
    let mut x = frac.to_bits();
    exp += ((x >> 52) & 0x7ff) as i64 - 1023;
    if exp < -1075 {
        return 0.0f64.copysign(frac); // underflow
    }
    if exp > 1023 {
        // overflow
        return if frac < 0.0 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    let mut m = 1.0f64;
    if exp < -1022 {
        // denormal
        exp += 53;
        m = 1.0 / (1u64 << 53) as f64;
    }
    x &= !(0x7ffu64 << 52);
    x |= ((exp + 1023) as u64) << 52;
    m * f64::from_bits(x)
}

/// Go `math.isOddInt`.
fn is_odd_int(x: f64) -> bool {
    if x.abs() >= (1u64 << 53) as f64 {
        // 1 << 53 is the largest exact integer in the float64 format
        // and every float64 past it is even
        return false;
    }
    let (xi, xf) = modf(x);
    xf == 0.0 && (xi as i64) & 1 == 1
}

// log_amd64.s constants
const HSQRT2: f64 = 7.07106781186547524401e-01; // sqrt(2)/2
const LN2HI: f64 = 6.93147180369123816490e-01; // 0x3fe62e42fee00000
const LN2LO: f64 = 1.90821492927058770002e-10; // 0x3dea39ef35793c76
const L1: f64 = 6.666666666666735130e-01; // 0x3FE5555555555593
const L2: f64 = 3.999999999940941908e-01; // 0x3FD999999997FA04
const L3: f64 = 2.857142874366239149e-01; // 0x3FD2492494229359
const L4: f64 = 2.222219843214978396e-01; // 0x3FCC71C51D8E78AF
const L5: f64 = 1.818357216161805012e-01; // 0x3FC7466496CB03DE
const L6: f64 = 1.531383769920937332e-01; // 0x3FC39A09D078C69F
const L7: f64 = 1.479819860511658591e-01; // 0x3FC2F112DF3E5244

/// Go `math.Log` (amd64 assembly algorithm).
pub fn log(x: f64) -> f64 {
    if x == 0.0 {
        return f64::NEG_INFINITY;
    }
    if x < 0.0 {
        return f64::NAN;
    }
    if x.is_nan() || x.is_infinite() {
        return x;
    }
    // f1, ki := math.Frexp(x); k := float64(ki) — done with the assembly's bit
    // tricks (mantissa | 0.5 exponent), which, unlike Frexp, do not normalize
    // subnormal inputs; kept as is to match Go on amd64 bit for bit.
    let bits = x.to_bits();
    let mut f1 = f64::from_bits((bits & 0x000F_FFFF_FFFF_FFFF) | 0.5f64.to_bits());
    let mut k = (((bits >> 52) & 0x7ff) as i32 - 0x3fe) as f64;
    // if f1 < math.Sqrt2/2 { k -= 1; f1 *= 2 }
    if f1 < HSQRT2 {
        k -= 1.0;
        f1 *= 2.0;
    }
    // f := f1 - 1
    let f = f1 - 1.0;
    // s := f / (2 + f)
    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    let t1 = s2 * (L1 + s4 * (L3 + s4 * (L5 + s4 * L7)));
    let t2 = s4 * (L2 + s4 * (L4 + s4 * L6));
    let r = t1 + t2;
    let hfsq = 0.5 * f * f;
    k * LN2HI - ((hfsq - (s * (hfsq + r) + k * LN2LO)) - f)
}

// exp_amd64.s constants
const LOG2E: f64 = 1.4426950408889634073599246810018920; // 1/ln(2)
const LN2U: f64 = 0.69314718055966295651160180568695068359375; // upper half of ln(2)
const LN2L: f64 = 0.28235290563031577122588448175013436025525412068e-12; // lower half of ln(2)
const EXP_OVERFLOW: f64 = 7.09782712893384e+02;
const C3: f64 = 1.6666666666666666667e-1;
const C4: f64 = 4.1666666666666666667e-2;
const C5: f64 = 8.3333333333333333333e-3;
const C6: f64 = 1.3888888888888888889e-3;
const C7: f64 = 1.9841269841269841270e-4;
const C8: f64 = 2.4801587301587301587e-5;

/// Whether the Go binary on this machine takes the AVX+FMA code path of `archExp`.
pub fn exp_uses_fma() -> bool {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        std::arch::is_x86_feature_detected!("avx") && std::arch::is_x86_feature_detected!("fma")
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        true
    }
}

/// Go `math.Exp` (amd64 assembly algorithm) with an explicit FMA/non-FMA choice.
pub fn exp_with(x: f64, fma: bool) -> f64 {
    if x.is_nan() || x == f64::INFINITY {
        return x;
    }
    if x == f64::NEG_INFINITY {
        return 0.0;
    }
    if x > EXP_OVERFLOW {
        return f64::INFINITY;
    }
    // CVTSD2SL: round to nearest even, saturating to the "integer indefinite" value on overflow
    let kf = LOG2E * x;
    let k: i32 = if kf.abs() >= 2147483648.0 {
        i32::MIN
    } else {
        kf.round_ties_even() as i32
    };
    let kd = k as f64;
    let mut x0 = x;
    let fr = if fma {
        x0 = (-LN2U).mul_add(kd, x0);
        x0 = (-LN2L).mul_add(kd, x0);
        x0 *= 0.0625;
        let mut x1 = C8;
        x1 = x1.mul_add(x0, C7);
        x1 = x1.mul_add(x0, C6);
        x1 = x1.mul_add(x0, C5);
        x1 = x1.mul_add(x0, C4);
        x1 = x1.mul_add(x0, C3);
        x1 = x1.mul_add(x0, 0.5);
        x1 = x1.mul_add(x0, 1.0);
        x0 *= x1;
        x1 = x0 + 2.0;
        x0 *= x1;
        x1 = x0 + 2.0;
        x0 *= x1;
        x1 = x0 + 2.0;
        x0 *= x1;
        x1 = x0 + 2.0;
        x1.mul_add(x0, 1.0)
    } else {
        x0 -= LN2U * kd;
        x0 -= LN2L * kd;
        x0 *= 0.0625;
        let mut x1 = C8;
        x1 = x1 * x0 + C7;
        x1 = x1 * x0 + C6;
        x1 = x1 * x0 + C5;
        x1 = x1 * x0 + C4;
        x1 = x1 * x0 + C3;
        x1 = x1 * x0 + 0.5;
        x1 = x1 * x0 + 1.0;
        x0 *= x1;
        x1 = x0 + 2.0;
        x0 *= x1;
        x1 = x0 + 2.0;
        x0 *= x1;
        x1 = x0 + 2.0;
        x0 *= x1;
        x1 = x0 + 2.0;
        x0 *= x1;
        x0 + 1.0
    };
    // return fr * 2**k (the assembly's own ldexp)
    let biased = k as i64 + 0x3ff;
    if biased <= 0 {
        if biased < -52 {
            return 0.0;
        }
        // denormal: scale in two steps
        let first = f64::from_bits(((biased + 0x3fe) as u64) << 52);
        return fr * first * f64::from_bits(1u64 << 52);
    }
    if biased >= 0x7ff {
        return f64::INFINITY;
    }
    fr * f64::from_bits((biased as u64) << 52)
}

/// Go `math.Exp` as computed by the Go binary on this machine.
pub fn exp(x: f64) -> f64 {
    exp_with(x, exp_uses_fma())
}

/// Go `math.Pow` (portable `pow.go` as used on amd64/arm64).
pub fn pow(x: f64, y: f64) -> f64 {
    if y == 0.0 || x == 1.0 {
        return 1.0;
    }
    if y == 1.0 {
        return x;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        if y < 0.0 {
            if x.is_sign_negative() && is_odd_int(y) {
                return f64::NEG_INFINITY;
            }
            return f64::INFINITY;
        }
        if y > 0.0 {
            if x.is_sign_negative() && is_odd_int(y) {
                return x;
            }
            return 0.0;
        }
    }
    if y.is_infinite() {
        if x == -1.0 {
            return 1.0;
        }
        if (x.abs() < 1.0) == (y > 0.0) {
            return 0.0;
        }
        return f64::INFINITY;
    }
    if x.is_infinite() {
        if x < 0.0 {
            return pow(1.0 / x, -y); // Pow(-0, -y)
        }
        if y < 0.0 {
            return 0.0;
        }
        return f64::INFINITY;
    }
    if y == 0.5 {
        return x.sqrt();
    }
    if y == -0.5 {
        return 1.0 / x.sqrt();
    }
    let (mut yi, mut yf) = modf(y.abs());
    if yf != 0.0 && x < 0.0 {
        return f64::NAN;
    }
    if yi >= (1u64 << 63) as f64 {
        // yi is a large even int that will lead to overflow (or underflow to 0)
        // for all x except -1 (x == 1 was handled earlier)
        if x == -1.0 {
            return 1.0;
        }
        if (x.abs() < 1.0) == (y > 0.0) {
            return 0.0;
        }
        return f64::INFINITY;
    }
    // ans = a1 * 2**ae (= 1 for now).
    let mut a1 = 1.0f64;
    let mut ae: i64 = 0;
    // ans *= x**yf
    if yf != 0.0 {
        if yf > 0.5 {
            yf -= 1.0;
            yi += 1.0;
        }
        a1 = exp(yf * log(x));
    }
    // ans *= x**yi by repeated squaring and multiplying
    let (mut x1, mut xe) = frexp(x);
    let mut i = yi as i64;
    while i != 0 {
        if !(-(1 << 12)..=(1 << 12)).contains(&xe) {
            // catastrophic overflow - avoid the loop
            ae += xe;
            break;
        }
        if i & 1 == 1 {
            a1 *= x1;
            ae += xe;
        }
        x1 *= x1;
        xe <<= 1;
        if x1 < 0.5 {
            x1 += x1;
            xe -= 1;
        }
        i >>= 1;
    }
    // ans = a1 * 2**ae; if y < 0 { ans = 1 / ans } but in the opposite order
    if y < 0.0 {
        a1 = 1.0 / a1;
        ae = -ae;
    }
    ldexp(a1, ae)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modf_frexp_ldexp() {
        assert_eq!(modf(3.75), (3.0, 0.75));
        assert_eq!(modf(-3.75), (-3.0, -0.75));
        assert_eq!(modf(0.25), (0.0, 0.25));
        let (i, f) = modf(-0.0);
        assert!(i.is_sign_negative() && f.is_sign_negative());
        for v in [1.0, 0.75, 3.0e10, 1e-310, 123456789.0, 5e-324, -2.5] {
            let (f, e) = frexp(v);
            assert!((0.5..1.0).contains(&f.abs()), "{v}: {f}");
            assert_eq!(ldexp(f, e), v);
        }
        assert_eq!(frexp(0.0), (0.0, 0));
        assert_eq!(ldexp(1.0, 1024), f64::INFINITY);
        assert_eq!(ldexp(-1.0, 1024), f64::NEG_INFINITY);
        assert_eq!(ldexp(1.0, -1080), 0.0);
        assert_eq!(ldexp(0.5, -1073), 5e-324);
    }

    #[test]
    fn special_cases() {
        assert_eq!(pow(2.0, 0.0), 1.0);
        assert_eq!(pow(1.0, f64::NAN), 1.0);
        assert_eq!(pow(7.5, 1.0), 7.5);
        assert_eq!(pow(16.0, 0.5), 4.0);
        assert_eq!(pow(16.0, -0.5), 0.25);
        assert_eq!(pow(2.0, 10.0), 1024.0);
        assert_eq!(pow(2.0, -2.0), 0.25);
        assert_eq!(pow(-2.0, 3.0), -8.0);
        assert_eq!(pow(-2.0, 2.0), 4.0);
        assert!(pow(-2.0, 0.5).is_nan());
        assert!(pow(f64::NAN, 2.0).is_nan());
        assert_eq!(pow(0.0, 3.0), 0.0);
        assert_eq!(pow(0.0, -1.0), f64::INFINITY);
        assert_eq!(pow(-0.0, -1.0), f64::NEG_INFINITY);
        assert_eq!(pow(-0.0, -2.0), f64::INFINITY);
        assert!(pow(-0.0, 3.0).is_sign_negative());
        assert_eq!(pow(0.5, f64::INFINITY), 0.0);
        assert_eq!(pow(2.0, f64::INFINITY), f64::INFINITY);
        assert_eq!(pow(2.0, f64::NEG_INFINITY), 0.0);
        assert_eq!(pow(-1.0, f64::INFINITY), 1.0);
        assert_eq!(pow(f64::INFINITY, 2.0), f64::INFINITY);
        assert_eq!(pow(f64::INFINITY, -2.0), 0.0);
        assert_eq!(pow(f64::NEG_INFINITY, 3.0), f64::NEG_INFINITY);
        assert_eq!(pow(f64::NEG_INFINITY, 2.0), f64::INFINITY);
        assert_eq!(pow(2.0, 1e19), f64::INFINITY);
        assert_eq!(pow(0.5, 1e19), 0.0);
        assert_eq!(pow(-1.0, 1e19), 1.0);
        assert_eq!(pow(10.0, 300.0), 1.0000000000000006e300); // Go gives the same (repeated squaring)
        assert_eq!(pow(10.0, 400.0), f64::INFINITY);
        assert_eq!(pow(10.0, -400.0), 0.0);
        assert_eq!(pow(4.0, 1.5), 8.0);
        assert_eq!(pow(4.0, 2.5), 32.0);

        assert_eq!(log(0.0), f64::NEG_INFINITY);
        assert_eq!(log(-0.0), f64::NEG_INFINITY);
        assert!(log(-1.0).is_nan());
        assert!(log(f64::NAN).is_nan());
        assert_eq!(log(f64::INFINITY), f64::INFINITY);
        assert_eq!(log(1.0), 0.0);

        for fma in [false, true] {
            assert_eq!(exp_with(0.0, fma), 1.0);
            assert_eq!(exp_with(f64::INFINITY, fma), f64::INFINITY);
            assert_eq!(exp_with(f64::NEG_INFINITY, fma), 0.0);
            assert!(exp_with(f64::NAN, fma).is_nan());
            assert_eq!(exp_with(710.0, fma), f64::INFINITY);
            assert_eq!(exp_with(-800.0, fma), 0.0);
            assert_eq!(exp_with(-1e10, fma), 0.0);
            assert!((exp_with(1.0, fma) - std::f64::consts::E).abs() < 1e-15);
            // denormal results
            let d = exp_with(-740.0, fma);
            assert!(d > 0.0 && d < SMALLEST_NORMAL, "{d}");
        }
    }
}
