//! Explicitly-seeded PRNG: hand-written xoshiro256** (tech-selection §4.1,
//! §10 — the `rand` crate is deliberately not pulled in; all we need is a
//! uniform u64 stream).
//!
//! Determinism contract (§4.1): one PRNG instance per graph, seeded
//! explicitly — the same insert sequence with the same seed produces a
//! byte-identical graph. PRNG state does **not** enter snapshots (§4.1
//! implementation prerequisite ③).

/// xoshiro256** 1.0 (Blackman & Vigna, 2018), a verbatim port of the public
/// reference implementation (`https://prng.di.unimi.it/xoshiro256starstar.c`).
/// The 256-bit state must not be all zero; [`Xoshiro256StarStar::new`] seeds
/// it through splitmix64 expansion, the scheme the reference recommends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Xoshiro256StarStar {
    s: [u64; 4],
}

/// splitmix64 (Stafford / Vigna), used only to expand a 64-bit seed into
/// the 256-bit xoshiro state.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

impl Xoshiro256StarStar {
    /// Seed from a single u64 via splitmix64 expansion (§4.1: the seed is an
    /// explicit constructor argument, never global entropy).
    pub fn new(seed: u64) -> Self {
        let mut sm = SplitMix64(seed);
        Self {
            s: [sm.next_u64(), sm.next_u64(), sm.next_u64(), sm.next_u64()],
        }
    }

    /// Inject a raw 256-bit state. Test-only: known-answer vectors and the
    /// `u == 0.0` redraw path need exact state control.
    #[cfg(test)]
    fn from_state(s: [u64; 4]) -> Self {
        Self { s }
    }

    /// Next uniformly distributed u64 (the reference algorithm, verbatim).
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Draw a node's top level (§4.1): geometric distribution,
    /// `level = floor(-ln(u) * m_L)` with `m_L = 1/ln(M)`.
    ///
    /// The frozen u64→f64 conversion (§4.1 v1.2) takes the **top 53 bits**:
    /// `u = (r >> 11) as f64 * 2⁻⁵³`, giving `u ∈ [0, 1)`. `u == 0.0` is a
    /// legal sample (probability 2⁻⁵³) and is handled by **redraw**, not by
    /// assertion — redraw keeps the distribution unbiased. After redraw
    /// `u >= 2⁻⁵³`, so `level <= floor(53·ln2 / ln M)` (13 at M=16); the
    /// debug assertion below guards only genuine invariant violations
    /// (§11 R2).
    pub fn next_level(&mut self, m: u16) -> u8 {
        debug_assert!(
            m >= 2,
            "HnswParams construction validation guarantees M >= 2"
        );
        let m_l = 1.0 / f64::from(m).ln();
        loop {
            let r = self.next_u64();
            let u = (r >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0); // 2^53
            if u == 0.0 {
                continue; // legal sample — redraw (§4.1 v1.2), do not assert
            }
            let level = (-u.ln() * m_l).floor();
            debug_assert!(
                level <= 53.0 * std::f64::consts::LN_2 / f64::from(m).ln(),
                "level {level} exceeds the post-redraw bound for M = {m} (§11 R2)"
            );
            return level as u8;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known-answer vectors produced by the public reference C implementation
    // (xoshiro256starstar.c + splitmix64.c from prng.di.unimi.it, compiled
    // and executed on 2026-08-31; harness: splitmix64(42) fills s, then 16
    // xoshiro256** outputs; plus 8 outputs from raw state {1,2,3,4}).
    const REF_SEED42_STATE: [u64; 4] = [
        13679457532755275413,
        2949826092126892291,
        5139283748462763858,
        6349198060258255764,
    ];
    const REF_SEED42_OUTPUTS: [u64; 16] = [
        1546998764402558742,
        6990951692964543102,
        12544586762248559009,
        17057574109182124193,
        18295552978065317476,
        14199186830065750584,
        13267978908934200754,
        15679888225317814407,
        14044878350692344958,
        10760895422300929085,
        12589033428110817649,
        5362058279183681893,
        14776290213336893110,
        5928998142081247042,
        13118401031821625293,
        16191947441114085370,
    ];
    const REF_STATE_1_2_3_4_OUTPUTS: [u64; 8] = [
        11520,
        0,
        1509978240,
        1215971899390074240,
        1216172134540287360,
        607988272756665600,
        16172922978634559625,
        8476171486693032832,
    ];

    #[test]
    fn known_answer_seed_path() {
        let mut rng = Xoshiro256StarStar::new(42);
        assert_eq!(rng.s, REF_SEED42_STATE, "splitmix64 seed expansion drifted");
        for (i, &expected) in REF_SEED42_OUTPUTS.iter().enumerate() {
            assert_eq!(
                rng.next_u64(),
                expected,
                "output {i} diverged from reference"
            );
        }
    }

    #[test]
    fn known_answer_raw_state_path() {
        let mut rng = Xoshiro256StarStar::from_state([1, 2, 3, 4]);
        for (i, &expected) in REF_STATE_1_2_3_4_OUTPUTS.iter().enumerate() {
            assert_eq!(
                rng.next_u64(),
                expected,
                "output {i} diverged from reference"
            );
        }
    }

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Xoshiro256StarStar::new(1234);
        let mut b = Xoshiro256StarStar::new(1234);
        for _ in 0..256 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        // determinism of the level draw specifically (§4.1: same seed + same
        // insert sequence => byte-identical graph)
        let levels_a: Vec<u8> = (0..1000).map(|_| a.next_level(16)).collect();
        let levels_b: Vec<u8> = (0..1000).map(|_| b.next_level(16)).collect();
        assert_eq!(levels_a, levels_b);
    }

    #[test]
    fn u_zero_redraws_instead_of_asserting() {
        // State [1,0,0,1]: the first draw is rotl(0*5,7)*9 == 0, i.e. exactly
        // the u == 0.0 sample (probability 2⁻⁵³ in the wild).
        let mut probe = Xoshiro256StarStar::from_state([1, 0, 0, 1]);
        assert_eq!(
            probe.next_u64(),
            0,
            "test premise: this state must yield u == 0.0"
        );

        // next_level must redraw rather than panic; the redraw consumes
        // exactly one extra draw, so the state afterwards equals two plain
        // next_u64() calls from the same initial state.
        let mut via_level = Xoshiro256StarStar::from_state([1, 0, 0, 1]);
        let level = via_level.next_level(16);
        assert!(level <= 13, "post-redraw bound at M=16 (§11 R2)");
        let mut via_draws = Xoshiro256StarStar::from_state([1, 0, 0, 1]);
        via_draws.next_u64();
        via_draws.next_u64();
        assert_eq!(
            via_level, via_draws,
            "redraw must consume exactly the zero draw"
        );
    }

    #[test]
    fn level_respects_the_post_redraw_bound() {
        let mut rng = Xoshiro256StarStar::new(7);
        for _ in 0..100_000 {
            assert!(rng.next_level(16) <= 13);
        }
        let mut rng = Xoshiro256StarStar::new(7);
        for _ in 0..100_000 {
            // floor(53·ln2 / ln2) = 53 at M=2, the loosest legal bound
            assert!(rng.next_level(2) <= 53);
        }
    }
}
