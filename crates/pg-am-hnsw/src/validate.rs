//! Validation funnel — Phase 2 M5 Stage A (tech-selection §10.1 frozen
//! checklist, §10.2 layering rule; coding plan Stage A deliverable ③).
//!
//! The SINGLE point where WAL records and normal-path inputs are validated:
//! the redo handlers and the normal write path both validate here and then
//! call the pure-application primitives in [`crate::apply`] (which never
//! re-validate). This Stage A subset covers the **same-page / same-record
//! decidable** items of the §10.1 frozen checklist; meta reads currently go
//! through the in-memory [`MetaView`] — Stage B's `meta.rs` swaps in the
//! page-backed implementation behind the same struct (2026-09-14 timing
//! note: meta.rs is a Stage B deliverable, so the funnel is parameterized
//! by value, not by page).
//!
//! **Evaluability constraint** (§10.1 v1.10, frozen here): checks that
//! depend on the chain-derived high-water mark or the directory mapping —
//! referenced-id < HWM, entry_point < HWM, PublishLive's directory
//! consistency, SetNeighbors' owner directory consistency — are NOT part of
//! the redo path. They belong to the Stage D open-time audit (§11.3).
//! Moving that line is a protocol revision, not an implementation detail.
//!
//! **State-record dim gate** (2026-09-15, review round 6 P1): the
//! PublishLive/Tombstone `dim == meta.dim` check is NOT one of the demoted
//! audit items — it is redo-evaluable and MUST run before apply. Redo-
//! evaluable because the payload carries `meta_page_id` (round 5) and the
//! meta page's init records precede every node record in LSN order while
//! `dim` is immutable (metric/dim mismatch is a hard open failure); must
//! run before apply because the open-time audit cannot recover historical
//! payloads — a wrong `dim` caught only there would already have rewritten
//! the wrong bytes during redo. [`validate_state_dim`] is the single
//! point; the Stage C handler reads meta through the payload's
//! `meta_page_id` and gates here before the flip.
//!
//! **Slot-state check ownership** (2026-09-14, Stage A review P3-3): the
//! frozen checklist's "target slot free-or-INITIALIZING / state ∈
//! {INITIALIZING, LIVE}" items need PAGE access, so they are NOT part of
//! this value-parameterized funnel — they belong to the Stage C redo
//! handler, which holds the pinned page and applies them (via
//! [`crate::apply`]'s read-side accessors) before calling the primitives.

use crate::distance;
use crate::error::{HnswError, Result};
use crate::graph::Metric;

/// In-memory view of the meta-page parameters the funnel validates against
/// (Stage B swaps in the page-backed reader behind the same shape).
#[derive(Debug, Clone, Copy)]
pub(crate) struct MetaView {
    /// Vector dimension (must equal the record's dim).
    pub dim: u16,
    /// Construction parameter M (level capacity of upper levels, L_max base).
    pub m: u16,
    /// Construction parameter M_max0 (level-0 capacity).
    pub m_max0: u16,
    /// Distance metric (gates the Cosine zero-vector check).
    pub metric: Metric,
}

impl MetaView {
    /// Maximum legal top level for this meta: `⌊53·ln2 / ln M⌋` — the
    /// redraw-bounded hard ceiling of the geometric level draw, delegated
    /// to the single source [`crate::rng::l_max`] (2026-09-15, review
    /// round 3 P3-2: the formula was duplicated here and at rng.rs).
    /// Precondition (2026-09-14, review nano): `m >= 2`, guaranteed by
    /// meta creation (HnswParams' validation, params.rs:77-82) — `m ∈
    /// {0, 1}` makes the ln degenerate and is not defended here because no
    /// legal meta can carry it.
    pub(crate) fn l_max(&self) -> u8 {
        crate::rng::l_max(self.m)
    }

    /// Reserved neighbor capacity of `level` (level 0 → m_max0, upper → m).
    pub(crate) fn level_capacity(&self, level: u8) -> usize {
        if level == 0 {
            usize::from(self.m_max0)
        } else {
            usize::from(self.m)
        }
    }
}

/// Page-backed meta → funnel view (2026-09-16, Stage B slice 2 wiring; the
/// slice-2 archive entry predated the code, adversarial review P3-1 caught
/// it — this impl is what makes the archive true). The Stage C redo handler
/// reads [`crate::meta::MetaParams`] off the meta page and enters the funnel
/// through this conversion — the in-memory shape is unchanged, so the
/// negative-example matrix is unaffected.
impl From<&crate::meta::MetaParams> for MetaView {
    fn from(p: &crate::meta::MetaParams) -> Self {
        MetaView {
            dim: p.dim,
            m: p.m,
            m_max0: p.m_max0,
            metric: p.metric,
        }
    }
}

/// §10.1 HnswNodeInit checklist subset: `dim == meta.dim`,
/// `level <= L_max(meta.m)`, every vector component finite, and — for
/// Cosine — no zero vector (same funnel as M4's insert entry validation,
/// distance.rs:76; L2/IP zero vectors are legal, M4 convention).
pub(crate) fn validate_node_init(
    meta: &MetaView,
    dim: u16,
    level: u8,
    vector: &[f32],
) -> Result<()> {
    if dim != meta.dim {
        return Err(HnswError::InvalidArgument(format!(
            "NodeInit dim {dim} != meta dim {} — metric/dim mismatch must fail loudly",
            meta.dim
        )));
    }
    if vector.len() != usize::from(dim) {
        return Err(HnswError::InvalidArgument(format!(
            "NodeInit vector has {} components, dim is {dim}",
            vector.len()
        )));
    }
    let l_max = meta.l_max();
    if level > l_max {
        return Err(HnswError::InvalidArgument(format!(
            "NodeInit level {level} > L_max {l_max} (⌊53·ln2/ln m⌋ recomputed from meta)"
        )));
    }
    for (i, &x) in vector.iter().enumerate() {
        if !x.is_finite() {
            return Err(HnswError::InvalidArgument(format!(
                "NodeInit non-finite vector component at index {i} (M4 §5/§7)"
            )));
        }
    }
    if meta.metric == Metric::Cosine {
        // Zero-vector funnel identical to M4's insert/search entry
        // validation (graph.rs:402, validate_entry_vector): cosine distance
        // is undefined for a zero vector.
        distance::cosine(vector, vector)?;
    }
    Ok(())
}

/// §10.1 HnswSetNeighbors checklist subset: `count == content.len()`
/// (caller supplies both, mirroring the payload), `count <= level capacity`,
/// `level <= entry top level`, strictly ascending, no duplicates, no
/// self-loop against the owner (v1.12 — the owner node_id is the judgment
/// basis).
pub(crate) fn validate_set_neighbors(
    meta: &MetaView,
    owner_node_id: u32,
    level: u8,
    count: u16,
    content: &[u32],
    entry_top_level: u8,
) -> Result<()> {
    if usize::from(count) != content.len() {
        return Err(HnswError::InvalidArgument(format!(
            "SetNeighbors count {count} != {} content ids",
            content.len()
        )));
    }
    let cap = meta.level_capacity(level);
    if content.len() > cap {
        return Err(HnswError::InvalidArgument(format!(
            "SetNeighbors {} ids exceed the capacity {cap} of level {level}",
            content.len()
        )));
    }
    if level > entry_top_level {
        return Err(HnswError::InvalidArgument(format!(
            "SetNeighbors level {level} exceeds the entry's top level {entry_top_level}"
        )));
    }
    if content.windows(2).any(|w| w[0] >= w[1]) {
        return Err(HnswError::InvalidArgument(
            "SetNeighbors neighbors must be strictly ascending (duplicates included)".to_string(),
        ));
    }
    if content.contains(&owner_node_id) {
        return Err(HnswError::InvalidArgument(format!(
            "SetNeighbors self-loop: owner node_id {owner_node_id} is its own neighbor"
        )));
    }
    Ok(())
}

/// §10.1 HnswPublishLive / HnswNodeTombstone checklist item: the payload
/// `dim` — which locates the entry's state byte at offset `4·dim` — must
/// equal `meta.dim`. This is a PRE-APPLY gate, not an audit item (see the
/// module doc's "State-record dim gate": a mismatch caught only at the
/// open-time audit would already have flipped a bit at the wrong offset
/// during redo, and the audit cannot recover the historical payloads to
/// attribute it). The Stage C redo handler reads meta through the
/// payload's `meta_page_id` and gates here before calling the primitive;
/// the normal path gates here against its in-memory meta.
pub(crate) fn validate_state_dim(meta: &MetaView, dim: u16, what: &str) -> Result<()> {
    if dim != meta.dim {
        return Err(HnswError::InvalidArgument(format!(
            "{what} dim {dim} != meta dim {} — the state-byte locator must match meta before apply",
            meta.dim
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIM: u16 = 4;
    const M: u16 = 4;
    const M_MAX0: u16 = 8;

    fn meta() -> MetaView {
        MetaView {
            dim: DIM,
            m: M,
            m_max0: M_MAX0,
            metric: Metric::L2,
        }
    }

    #[test]
    fn node_init_accepts_and_rejects() {
        let m = meta();
        validate_node_init(&m, DIM, 2, &[1.0, 2.0, 3.0, 4.0]).unwrap();
        // dim mismatch / level > L_max / NaN / wrong length.
        assert!(validate_node_init(&m, 8, 0, &[1.0; 8]).is_err());
        assert!(validate_node_init(&m, DIM, 99, &[1.0; 4]).is_err());
        assert!(validate_node_init(&m, DIM, 0, &[1.0, f32::NAN, 3.0, 4.0]).is_err());
        assert!(validate_node_init(&m, DIM, 0, &[1.0; 3]).is_err());
        // L_max exact boundary: floor(53·ln2/ln 4) = 26.
        assert_eq!(m.l_max(), 26);
        validate_node_init(&m, DIM, 26, &[1.0; 4]).unwrap();
        assert!(validate_node_init(&m, DIM, 27, &[1.0; 4]).is_err());
        // L_max at the smallest legal m (2026-09-14 review round 2 nano):
        // m=2 → ⌊53·ln2/ln 2⌋ = 53 — the formula's upper semantics pinned
        // at the boundary, matching rng.rs:93-96.
        let m2 = MetaView { m: 2, ..meta() };
        assert_eq!(m2.l_max(), 53);
        validate_node_init(&m2, DIM, 53, &[1.0; 4]).unwrap();
        assert!(validate_node_init(&m2, DIM, 54, &[1.0; 4]).is_err());
        // Cosine: zero vector rejected (M4 funnel), non-zero accepted; the
        // L2 zero vector is LEGAL (M4 convention).
        let cosine_meta = MetaView {
            metric: Metric::Cosine,
            ..meta()
        };
        assert!(matches!(
            validate_node_init(&cosine_meta, DIM, 0, &[0.0; 4]),
            Err(HnswError::ZeroVector)
        ));
        validate_node_init(&cosine_meta, DIM, 0, &[1.0; 4]).unwrap();
        validate_node_init(&m, DIM, 0, &[0.0; 4]).unwrap();
    }

    #[test]
    fn set_neighbors_accepts_and_rejects() {
        let m = meta();
        validate_set_neighbors(&m, 7, 0, 3, &[1, 5, 9], 0).unwrap();
        // count != content.len().
        assert!(validate_set_neighbors(&m, 7, 0, 2, &[1, 5, 9], 0).is_err());
        // over capacity (level 0 cap = 8, upper cap = 4).
        assert!(validate_set_neighbors(&m, 7, 0, 9, &[1, 2, 3, 4, 5, 6, 7, 8, 9], 0).is_err());
        assert!(validate_set_neighbors(&m, 7, 1, 5, &[1, 2, 3, 4, 5], 2).is_err());
        // level > entry top level.
        assert!(validate_set_neighbors(&m, 7, 3, 1, &[1], 2).is_err());
        // unsorted / duplicate / self-loop.
        assert!(validate_set_neighbors(&m, 7, 0, 2, &[5, 1], 0).is_err());
        assert!(validate_set_neighbors(&m, 7, 0, 2, &[5, 5], 0).is_err());
        assert!(validate_set_neighbors(&m, 7, 0, 2, &[1, 7], 0).is_err());
    }

    #[test]
    fn state_dim_gate_accepts_and_rejects() {
        let m = meta();
        // The pre-apply gate (2026-09-15, review round 6 P1): equal dims
        // pass; any mismatch is a loud InvalidArgument BEFORE the flip.
        validate_state_dim(&m, DIM, "PublishLive").unwrap();
        validate_state_dim(&m, DIM, "NodeTombstone").unwrap();
        for what in ["PublishLive", "NodeTombstone"] {
            let err = validate_state_dim(&m, DIM + 1, what).unwrap_err();
            assert!(err.to_string().contains(what), "{err}");
            assert!(err.to_string().contains("before apply"), "{err}");
        }
    }
}
