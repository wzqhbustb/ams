//! Distance functions (tech-selection §5): f32 elements, **f64 accumulator**,
//! scalar loops.
//!
//! The f64 accumulation chain is deliberately non-reassociable — LLVM does
//! not reorder floating-point adds without fast-math, and that is exactly
//! what makes the results bit-identical across platforms (§4.1 determinism
//! note). Do **not** add fast-math-style optimizations here; SIMD is a
//! frozen-out M4-late/Phase-7b optimization gated on recall + A/B evidence
//! (§5, §11 O4).
//!
//! Entry validation (§5 v1.3): every function rejects dimension mismatch,
//! `dim = 0`, and non-finite components. NaN makes every distance comparison
//! silently false — the graph would keep inserting while recall quietly
//! degrades, and the determinism-preserving A/B harness cannot catch it, so
//! loud entry rejection is the only line of defense. **±inf carries the same
//! semantics** (2026-08-31 review): cosine's `inf/inf` and L2's `inf−inf`
//! would silently produce NaN downstream — the exact failure mode this
//! validation exists to prevent. Stage A performs this validation at the
//! distance-function layer; the Stage B insert/search entry points inherit
//! it by calling these functions.

use crate::error::{HnswError, Result};

/// Shared entry validation (§5): equal, non-zero dimensions; no non-finite
/// (NaN or ±inf) components in either vector.
fn validate_pair(a: &[f32], b: &[f32]) -> Result<()> {
    if a.len() != b.len() {
        return Err(HnswError::InvalidArgument(format!(
            "dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }
    if a.is_empty() {
        return Err(HnswError::InvalidArgument("dim = 0 vector".to_string()));
    }
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if !x.is_finite() || !y.is_finite() {
            return Err(HnswError::InvalidArgument(format!(
                "non-finite component (NaN or ±inf) at index {i} (§5: entry rejection is the only defense)"
            )));
        }
    }
    Ok(())
}

/// Squared L2 distance `Σ(aᵢ−bᵢ)²` (§5: no square root — monotonicity is
/// preserved, ordering is equivalent, and the `sqrt` is saved).
pub fn l2_squared(a: &[f32], b: &[f32]) -> Result<f64> {
    validate_pair(a, b)?;
    let mut acc = 0.0f64;
    for i in 0..a.len() {
        // f32 -> f64 conversion is exact; the f64 accumulator keeps 960-dim
        // error far below sort noise (§5), where an f32 accumulator would
        // drift to ~1e-5 relative error.
        let d = f64::from(a[i]) - f64::from(b[i]);
        acc += d * d;
    }
    Ok(acc)
}

/// Cosine distance `1 − (a·b)/(|a||b|)` (§5: the embedding-scenario
/// workhorse; a zero vector is an error, not a silent 1.0).
pub fn cosine(a: &[f32], b: &[f32]) -> Result<f64> {
    validate_pair(a, b)?;
    let (mut ab, mut aa, mut bb) = (0.0f64, 0.0f64, 0.0f64);
    for i in 0..a.len() {
        let (x, y) = (f64::from(a[i]), f64::from(b[i]));
        ab += x * y;
        aa += x * x;
        bb += y * y;
    }
    // aa/bb are sums of squares of f64-promoted f32 values, so == 0.0 iff
    // every component is exactly zero — no epsilon games.
    if aa == 0.0 || bb == 0.0 {
        return Err(HnswError::ZeroVector);
    }
    Ok(1.0 - ab / (aa.sqrt() * bb.sqrt()))
}

/// Negative inner product `−a·b` (§5: negation turns max-IP retrieval into
/// min-distance search, so the search code needs zero branches for MIPS).
pub fn negative_inner_product(a: &[f32], b: &[f32]) -> Result<f64> {
    validate_pair(a, b)?;
    let mut acc = 0.0f64;
    for i in 0..a.len() {
        acc += f64::from(a[i]) * f64::from(b[i]);
    }
    Ok(-acc)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- known answers (hand-computed 3-d / 4-d vector groups) ----

    #[test]
    fn l2_squared_known_answers() {
        assert_eq!(
            l2_squared(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]).unwrap(),
            27.0
        );
        assert_eq!(
            l2_squared(&[1.0, 0.0, -1.0, 2.0], &[0.0, 1.0, 1.0, -2.0]).unwrap(),
            22.0
        );
        assert_eq!(l2_squared(&[1.5, -2.5], &[1.5, -2.5]).unwrap(), 0.0);
    }

    #[test]
    fn cosine_known_answers() {
        // orthogonal vectors -> distance 1
        assert_eq!(cosine(&[1.0, 0.0, 0.0], &[0.0, 1.0, 0.0]).unwrap(), 1.0);
        // identical vectors -> distance 0
        assert_eq!(cosine(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]).unwrap(), 0.0);
        // |a| = |b| = 5, a·b = 24 -> 1 - 24/25 = 0.04 (to the last ulp)
        let d = cosine(&[3.0, 4.0], &[4.0, 3.0]).unwrap();
        assert!((d - 0.04).abs() < 1e-15, "got {d}");
        // |a| = |b| = 3, a·b = 8 -> 1 - 8/9
        let d = cosine(&[1.0, 2.0, 2.0], &[2.0, 1.0, 2.0]).unwrap();
        assert!((d - (1.0 - 8.0 / 9.0)).abs() < 1e-15, "got {d}");
    }

    #[test]
    fn negative_inner_product_known_answers() {
        assert_eq!(
            negative_inner_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]).unwrap(),
            -32.0
        );
        assert_eq!(
            negative_inner_product(&[1.0, -1.0, 2.0, -2.0], &[3.0, 3.0, -1.0, -1.0]).unwrap(),
            0.0
        );
    }

    // ---- entry validation (§5) ----

    #[test]
    fn cosine_zero_vector_is_a_loud_error() {
        assert!(matches!(
            cosine(&[0.0, 0.0, 0.0], &[1.0, 2.0, 3.0]),
            Err(HnswError::ZeroVector)
        ));
        assert!(matches!(
            cosine(&[1.0, 2.0, 3.0], &[0.0, 0.0, 0.0]),
            Err(HnswError::ZeroVector)
        ));
        assert!(matches!(
            cosine(&[0.0, 0.0], &[0.0, 0.0]),
            Err(HnswError::ZeroVector)
        ));
    }

    #[test]
    fn nan_components_are_rejected() {
        let nan = f32::NAN;
        assert!(matches!(
            l2_squared(&[1.0, nan], &[1.0, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            l2_squared(&[1.0, 2.0], &[nan, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            cosine(&[1.0, nan], &[1.0, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            negative_inner_product(&[1.0, 2.0], &[1.0, nan]),
            Err(HnswError::InvalidArgument(_))
        ));
    }

    #[test]
    fn inf_components_are_rejected() {
        // ±inf must fail as loudly as NaN: cosine's inf/inf (and L2's
        // inf−inf) would otherwise silently produce NaN downstream.
        let inf = f32::INFINITY;
        assert!(matches!(
            l2_squared(&[1.0, inf], &[1.0, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            l2_squared(&[1.0, 2.0], &[f32::NEG_INFINITY, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            cosine(&[1.0, inf], &[1.0, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            negative_inner_product(&[1.0, 2.0], &[1.0, -inf]),
            Err(HnswError::InvalidArgument(_))
        ));
    }

    #[test]
    fn dim_zero_is_rejected() {
        assert!(matches!(
            l2_squared(&[], &[]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            cosine(&[], &[]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            negative_inner_product(&[], &[]),
            Err(HnswError::InvalidArgument(_))
        ));
    }

    #[test]
    fn dimension_mismatch_is_rejected() {
        assert!(matches!(
            l2_squared(&[1.0, 2.0], &[1.0, 2.0, 3.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            cosine(&[1.0], &[1.0, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            negative_inner_product(&[1.0, 2.0, 3.0], &[1.0]),
            Err(HnswError::InvalidArgument(_))
        ));
    }

    // ---- tolerance A/B against an f64 reference (§9) ----

    /// Vectors whose components are exactly representable in f32 (integer
    /// numerators over powers of two), so the f64 reference carries **no**
    /// representation error. Coding plan Stage A pins this: random f64 -> f32
    /// casts would inject ~1e-7 representation error and blow the 1e-12
    /// budget. The test's real value is proving the accumulator was not
    /// accidentally written as f32 — at 960 dims f32 accumulation drifts to
    /// ~1e-5 relative error, far outside the tolerance.
    #[test]
    fn matches_f64_reference_within_1e_minus_12_at_960_dim() {
        let a: Vec<f32> = (0..960u32)
            .map(|i| ((i * 37 + 11) % 251) as f32 / 64.0 - 2.0)
            .collect();
        let b: Vec<f32> = (0..960u32)
            .map(|i| ((i * 91 + 7) % 233) as f32 / 128.0 - 1.0)
            .collect();

        // Independent reference: f64 throughout, written in a different
        // style (iterator fold) than the implementation.
        let ref_l2 = a.iter().zip(&b).fold(0.0f64, |acc, (&x, &y)| {
            let d = f64::from(x) - f64::from(y);
            acc + d * d
        });
        let got_l2 = l2_squared(&a, &b).unwrap();
        assert!(
            (got_l2 - ref_l2).abs() <= 1e-12 * ref_l2.max(1.0),
            "l2: got {got_l2}, ref {ref_l2}"
        );

        let (ref_ab, ref_aa, ref_bb) =
            a.iter()
                .zip(&b)
                .fold((0.0f64, 0.0f64, 0.0f64), |(ab, aa, bb), (&x, &y)| {
                    let (x, y) = (f64::from(x), f64::from(y));
                    (ab + x * y, aa + x * x, bb + y * y)
                });
        let ref_cos = 1.0 - ref_ab / (ref_aa.sqrt() * ref_bb.sqrt());
        let got_cos = cosine(&a, &b).unwrap();
        assert!(
            (got_cos - ref_cos).abs() <= 1e-12 * ref_cos.abs().max(1.0),
            "cosine: got {got_cos}, ref {ref_cos}"
        );

        let ref_ip = -a
            .iter()
            .zip(&b)
            .fold(0.0f64, |acc, (&x, &y)| acc + f64::from(x) * f64::from(y));
        let got_ip = negative_inner_product(&a, &b).unwrap();
        assert!(
            (got_ip - ref_ip).abs() <= 1e-12 * ref_ip.abs().max(1.0),
            "ip: got {got_ip}, ref {ref_ip}"
        );
    }
}
