//! `gomath` against Go-generated vectors (`tests/data/gomath_vectors.rs`, produced by
//! Go 1.27 `math.Pow`/`Exp`/`Log` on an amd64 CPU with AVX+FMA).

use devstatscode::gomath;

#[allow(
    clippy::approx_constant,
    clippy::excessive_precision,
    clippy::unreadable_literal
)]
mod data {
    include!("data/gomath_vectors.rs");
}

fn same(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

#[test]
fn log_matches_go() {
    let mut bad = Vec::new();
    for (x, want) in data::LOG {
        let got = gomath::log(*x);
        if !same(got, *want) {
            bad.push(format!("log({x:e}) = {got:e}, Go {want:e}"));
        }
    }
    assert!(
        bad.is_empty(),
        "{} mismatches:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

#[test]
fn exp_matches_go() {
    // The vectors were produced on an AVX+FMA machine: the fused variant must be bit-exact.
    let mut bad = Vec::new();
    for (x, want) in data::EXP {
        let got = gomath::exp_with(*x, true);
        if !same(got, *want) {
            bad.push(format!("exp({x:e}) = {got:e}, Go {want:e}"));
        }
    }
    assert!(
        bad.is_empty(),
        "{} mismatches:\n{}",
        bad.len(),
        bad.join("\n")
    );
    // The non-fused variant is a different algorithm: within an ulp or two.
    for (x, want) in data::EXP {
        let got = gomath::exp_with(*x, false);
        if want.is_finite() && *want != 0.0 {
            let ulps = (got.to_bits() as i64 - want.to_bits() as i64).abs();
            assert!(
                ulps <= 2,
                "exp_nofma({x:e}) = {got:e}, Go(fma) {want:e} ({ulps} ulps)"
            );
        } else {
            assert!(same(got, *want), "exp_nofma({x:e}) = {got:e}, Go {want:e}");
        }
    }
}

#[test]
fn pow_matches_go() {
    let fma = gomath::exp_uses_fma();
    let mut bad = Vec::new();
    for (x, y, want) in data::POW {
        let got = gomath::pow(*x, *y);
        if same(got, *want) {
            continue;
        }
        if !fma && want.is_finite() {
            // CPU without AVX+FMA: Go itself would compute a slightly different fractional part here
            let ulps = (got.to_bits() as i64 - want.to_bits() as i64).abs();
            if ulps <= 2 {
                continue;
            }
        }
        bad.push(format!("pow({x:e}, {y:e}) = {got:e}, Go {want:e}"));
    }
    assert!(
        bad.is_empty(),
        "{} mismatches:\n{}",
        bad.len(),
        bad.join("\n")
    );
}
