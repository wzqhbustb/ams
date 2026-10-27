//! Construction parameters and node identity (tech-selection §3, §4.2, §4.4).

use crate::error::{HnswError, Result};

/// Node identifier (§3): densely increasing, never reused (M4 has no
/// deletes), and **stable across snapshot round-trips** — M5's WAL records
/// and node-page addressing key on it, so M4 must not compact or relabel.
///
/// In the frozen snapshot format "position is identity" (§3 v1.2): the i-th
/// node record in the stream *is* `NodeId(i)`, with no explicit id field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

impl NodeId {
    /// Sentinel for "no node" — the empty-graph entry-point encoding (§3
    /// v1.2). Never a valid node: ids are allocated from 0 upward.
    pub const INVALID: NodeId = NodeId(u32::MAX);

    /// Zero-based position of this node in the graph's SoA arenas (§6) and
    /// in the snapshot node-record stream.
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Construction parameters (§4.2).
///
/// `m` / `m_max0` / `ef_construction` determine the graph's shape and are
/// part of its state — they go into the snapshot header (§3).
/// `ef_search_default` is a query-time default (§4.4) and does **not** enter
/// the snapshot; overriding it is a per-query action.
///
/// The fields are **private** (2026-08-31 second review): with `pub` fields a
/// caller could build an unvalidated instance via struct literal or mutate
/// after `new`, bypassing the §4.2/§4.4 invariants entirely. Read through the
/// getters; construct through [`HnswParams::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswParams {
    m: u16,
    m_max0: u16,
    ef_construction: u32,
    ef_search_default: u32,
}

impl HnswParams {
    /// Maximum out-edges per node per non-zero layer (`M`, §4.2) — the main
    /// recall/memory knob.
    pub fn m(&self) -> u16 {
        self.m
    }

    /// Maximum out-edges per node on layer 0 (`M_max0 = 2M`, §4.2); layer 0
    /// holds every node, so its cap is relaxed to protect recall.
    pub fn m_max0(&self) -> u16 {
        self.m_max0
    }

    /// Construction-time candidate pool size (`ef_construction`, §4.2);
    /// affects recall more than `ef_search`.
    pub fn ef_construction(&self) -> u32 {
        self.ef_construction
    }

    /// Query-time default beam width (`ef_search`, §4.4). The `>= M`
    /// constraint applies to this **construction default** only — per-query
    /// `ef` may go below `M` (coding plan Stage B invariant: each query
    /// checks `ef >= k` and nothing else).
    pub fn ef_search_default(&self) -> u32 {
        self.ef_search_default
    }
}

impl HnswParams {
    /// Validate and construct (§4.2/§4.4):
    /// `M >= 2`, `M_max0 >= M`, `ef_construction >= M`, `ef_search_default >= M`.
    /// No runtime magic numbers — all parameters flow through here.
    pub fn new(m: u16, m_max0: u16, ef_construction: u32, ef_search_default: u32) -> Result<Self> {
        if m < 2 {
            return Err(HnswError::InvalidParams(format!(
                "M = {m} < 2 (§4.2: the geometric level distribution needs ln(M) > 0)"
            )));
        }
        if m_max0 < m {
            return Err(HnswError::InvalidParams(format!(
                "M_max0 = {m_max0} < M = {m} (§4.2: the layer-0 cap must fit one full neighbor set; v1.7 closes the zero-validation gap before Stage B's shrink consumes it)"
            )));
        }
        if ef_construction < u32::from(m) {
            return Err(HnswError::InvalidParams(format!(
                "ef_construction = {ef_construction} < M = {m} (§4.2: the candidate pool must fit one full neighbor set)"
            )));
        }
        if ef_search_default < u32::from(m) {
            return Err(HnswError::InvalidParams(format!(
                "ef_search_default = {ef_search_default} < M = {m} (§4.4: constrains the construction default, not per-query ef)"
            )));
        }
        Ok(Self {
            m,
            m_max0,
            ef_construction,
            ef_search_default,
        })
    }
}

impl Default for HnswParams {
    /// Frozen defaults (§4.2): `M = 16`, `M_max0 = 2M = 32`,
    /// `ef_construction = 200` (hnswlib's recall-first choice),
    /// `ef_search_default = 64` (the §12 acceptance beam width).
    fn default() -> Self {
        Self::new(16, 32, 200, 64).expect("the frozen §4.2 defaults pass construction validation")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_frozen_selection() {
        let p = HnswParams::default();
        assert_eq!(p.m, 16);
        assert_eq!(p.m_max0, 32);
        assert_eq!(p.ef_construction, 200);
        assert_eq!(p.ef_search_default, 64);
    }

    #[test]
    fn rejects_m_below_two() {
        assert!(HnswParams::new(1, 2, 200, 64).is_err());
        assert!(HnswParams::new(0, 0, 200, 64).is_err());
    }

    #[test]
    fn rejects_m_max0_below_m() {
        // m_max0 == 0 and m_max0 < m are both rejected (§4.2, v1.7).
        assert!(matches!(
            HnswParams::new(16, 0, 200, 64),
            Err(HnswError::InvalidParams(_))
        ));
        assert!(matches!(
            HnswParams::new(16, 15, 200, 64),
            Err(HnswError::InvalidParams(_))
        ));
        // boundary: m_max0 == M is legal
        assert!(HnswParams::new(16, 16, 200, 64).is_ok());
    }

    #[test]
    fn rejects_ef_construction_below_m() {
        assert!(HnswParams::new(16, 32, 15, 64).is_err());
        // boundary: ef_construction == M is legal
        assert!(HnswParams::new(16, 32, 16, 64).is_ok());
    }

    #[test]
    fn rejects_ef_search_default_below_m() {
        let err = HnswParams::new(16, 32, 200, 15).unwrap_err().to_string();
        assert!(
            err.contains("ef_search_default"),
            "unexpected message: {err}"
        );
        // boundary: ef_search_default == M is legal
        assert!(HnswParams::new(16, 32, 200, 16).is_ok());
    }

    #[test]
    fn node_id_sentinel_and_index() {
        assert_eq!(NodeId::INVALID.0, u32::MAX);
        assert_eq!(NodeId(7).index(), 7);
        // INVALID sorts after every real id — safe as an ordering sentinel.
        assert!(NodeId::INVALID > NodeId(u32::MAX - 1));
    }
}
