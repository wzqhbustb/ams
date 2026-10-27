//! Meta-page layout ownership — Phase 2 M5 Stage B slice 2
//! (tech-selection §6: the HNSW index meta page).
//!
//! The meta page makes the page-resident graph **self-describing**: every
//! parameter a load or replay must check lives here, pinned at index
//! creation and validated at open/redo (§6 — a mismatch fails loudly
//! instead of silently changing semantics). The M4 snapshot deliberately
//! carries no metric/selection (§3, geometry only); the page format MUST
//! carry them or a wrong runtime choice silently corrupts meaning.
//!
//! Frozen layout (format constants — any change is a format revision;
//! `entry_point` @32 / `max_level` @36 were frozen in Stage A and must not
//! move):
//!
//! ```text
//! 32 entry_point:u32 | 36 max_level:u8 | 37 meta_format_version:u8 = 1 |
//! 38 metric:u8 | 39 selection:u8 | 40 dim:u16 | 42 m:u16 | 44 m_max0:u16 |
//! 46 ef_construction:u32 | 50 ef_search_default:u32 | 54 reserved:u16 = 0 |
//! 56 rng_seed:u64 | 64 dir_head:PageId(u64) | 72 snapshot_format_version:u16 = 1
//! ```

use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::error::{HnswError, Result};
use crate::graph::{Metric, NeighborSelection};
use crate::page::{page_type, PAGE_TYPE_META};

/// `entry_point` offset — **frozen in Stage A, must not move**.
pub(crate) const META_OFF_ENTRY_POINT: usize = PAGE_HEADER_SIZE;
/// `max_level` offset — **frozen in Stage A, must not move**.
pub(crate) const META_OFF_MAX_LEVEL: usize = PAGE_HEADER_SIZE + 4;
/// `meta_format_version` offset (value = [`META_FORMAT_VERSION`]).
pub(crate) const META_OFF_FORMAT_VERSION: usize = PAGE_HEADER_SIZE + 5;
/// `metric` offset (discriminant, see [`Metric::discriminant`]).
pub(crate) const META_OFF_METRIC: usize = PAGE_HEADER_SIZE + 6;
/// `selection` offset (discriminant, see [`NeighborSelection::discriminant`]).
pub(crate) const META_OFF_SELECTION: usize = PAGE_HEADER_SIZE + 7;
/// `dim` offset.
pub(crate) const META_OFF_DIM: usize = PAGE_HEADER_SIZE + 8;
/// `m` offset.
pub(crate) const META_OFF_M: usize = PAGE_HEADER_SIZE + 10;
/// `m_max0` offset.
pub(crate) const META_OFF_M_MAX0: usize = PAGE_HEADER_SIZE + 12;
/// `ef_construction` offset.
pub(crate) const META_OFF_EF_CONSTRUCTION: usize = PAGE_HEADER_SIZE + 14;
/// `ef_search_default` offset.
pub(crate) const META_OFF_EF_SEARCH_DEFAULT: usize = PAGE_HEADER_SIZE + 18;
/// `reserved` offset (always 0 in v1).
pub(crate) const META_OFF_RESERVED: usize = PAGE_HEADER_SIZE + 22;
/// `rng_seed` offset (creation-time pinned, §5 v1.1 P2-2).
pub(crate) const META_OFF_RNG_SEED: usize = PAGE_HEADER_SIZE + 24;
/// `dir_head` offset (directory chain head, §7.1).
pub(crate) const META_OFF_DIR_HEAD: usize = PAGE_HEADER_SIZE + 32;
/// `snapshot_format_version` offset (compatibility declaration for the M4
/// logical-archive format — single source [`crate::encoding::FORMAT_VERSION`],
/// 2026-09-17 review round 3 P3-2: previously a literal that would silently
/// stay behind when the snapshot format bumps).
pub(crate) const META_OFF_SNAPSHOT_FORMAT_VERSION: usize = PAGE_HEADER_SIZE + 40;

/// The meta-page format version (v1; bumping it is a format revision).
pub(crate) const META_FORMAT_VERSION: u8 = 1;

/// The meta page's full parameter set (§6 field table, creation-pinned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaParams {
    /// Entry-point NodeId; `u32::MAX` (NodeId::INVALID) for an empty graph.
    pub entry_point: u32,
    /// Top level of the entry-point node (0 for an empty graph).
    pub max_level: u8,
    /// Distance metric (§5; first persistence — see [`Metric::discriminant`]).
    pub metric: Metric,
    /// Neighbor-selection mode (§4.3; first persistence — see
    /// [`NeighborSelection::discriminant`]).
    pub selection: NeighborSelection,
    /// Vector dimension.
    pub dim: u16,
    /// Construction parameter M.
    pub m: u16,
    /// Construction parameter M_max0.
    pub m_max0: u16,
    /// Construction parameter ef_construction.
    pub ef_construction: u32,
    /// Query-time default beam width (a PERFORMANCE knob, not a
    /// graph-destroying parameter — mismatches are warnings, v1.4 nano).
    pub ef_search_default: u32,
    /// PRNG seed pinned at index creation (§5 skip-ahead lineage).
    pub rng_seed: u64,
    /// Directory chain head (§7.1; never INVALID on a valid meta page).
    pub dir_head: PageId,
}

/// Write the full meta layout onto an already-initialized meta page
/// (`page.rs::init_meta_page` has zero-filled it). `snapshot_format_version`
/// is written from [`crate::encoding::FORMAT_VERSION`] (the single source —
/// the M4 logical-archive format this build reads) and `reserved` as 0.
pub(crate) fn write_meta(page: &mut [u8; PAGE_SIZE], p: &MetaParams) {
    page[META_OFF_ENTRY_POINT..META_OFF_ENTRY_POINT + 4]
        .copy_from_slice(&p.entry_point.to_le_bytes());
    page[META_OFF_MAX_LEVEL] = p.max_level;
    page[META_OFF_FORMAT_VERSION] = META_FORMAT_VERSION;
    page[META_OFF_METRIC] = p.metric.discriminant();
    page[META_OFF_SELECTION] = p.selection.discriminant();
    page[META_OFF_DIM..META_OFF_DIM + 2].copy_from_slice(&p.dim.to_le_bytes());
    page[META_OFF_M..META_OFF_M + 2].copy_from_slice(&p.m.to_le_bytes());
    page[META_OFF_M_MAX0..META_OFF_M_MAX0 + 2].copy_from_slice(&p.m_max0.to_le_bytes());
    page[META_OFF_EF_CONSTRUCTION..META_OFF_EF_CONSTRUCTION + 4]
        .copy_from_slice(&p.ef_construction.to_le_bytes());
    page[META_OFF_EF_SEARCH_DEFAULT..META_OFF_EF_SEARCH_DEFAULT + 4]
        .copy_from_slice(&p.ef_search_default.to_le_bytes());
    page[META_OFF_RESERVED..META_OFF_RESERVED + 2].copy_from_slice(&0u16.to_le_bytes());
    page[META_OFF_RNG_SEED..META_OFF_RNG_SEED + 8].copy_from_slice(&p.rng_seed.to_le_bytes());
    page[META_OFF_DIR_HEAD..META_OFF_DIR_HEAD + 8].copy_from_slice(&p.dir_head.0.to_le_bytes());
    page[META_OFF_SNAPSHOT_FORMAT_VERSION..META_OFF_SNAPSHOT_FORMAT_VERSION + 2]
        .copy_from_slice(&crate::encoding::FORMAT_VERSION.to_le_bytes());
}

/// Read and structurally validate a meta page (every violation is a loud
/// `Corrupted`). The runtime pair IS structurally checked: `max_level`
/// must fit the 6-bit format ceiling (<= 63) AND the semantic ceiling
/// `l_max(m)` (2026-09-17, review round 6 P3 — no level draw can exceed
/// it), and `entry_point == NodeId::INVALID` is only legal with
/// `max_level == 0` (the empty-graph state).
pub(crate) fn read_meta(page: &[u8; PAGE_SIZE]) -> Result<MetaParams> {
    if page_type(page) != PAGE_TYPE_META {
        return Err(HnswError::Corrupted(format!(
            "page is not a meta page (page_type {})",
            page_type(page)
        )));
    }
    let version = page[META_OFF_FORMAT_VERSION];
    if version != META_FORMAT_VERSION {
        return Err(HnswError::Corrupted(format!(
            "meta_format_version {version} != {META_FORMAT_VERSION}"
        )));
    }
    let metric = Metric::from_discriminant(page[META_OFF_METRIC])?;
    let selection = NeighborSelection::from_discriminant(page[META_OFF_SELECTION])?;
    let dim = u16::from_le_bytes(page[META_OFF_DIM..META_OFF_DIM + 2].try_into().unwrap());
    let m = u16::from_le_bytes(page[META_OFF_M..META_OFF_M + 2].try_into().unwrap());
    let m_max0 = u16::from_le_bytes(
        page[META_OFF_M_MAX0..META_OFF_M_MAX0 + 2]
            .try_into()
            .unwrap(),
    );
    let ef_construction = u32::from_le_bytes(
        page[META_OFF_EF_CONSTRUCTION..META_OFF_EF_CONSTRUCTION + 4]
            .try_into()
            .unwrap(),
    );
    let ef_search_default = u32::from_le_bytes(
        page[META_OFF_EF_SEARCH_DEFAULT..META_OFF_EF_SEARCH_DEFAULT + 4]
            .try_into()
            .unwrap(),
    );
    if dim == 0 {
        return Err(HnswError::Corrupted(
            "meta page has dim = 0 (M4 §5: rejected at every entry point)".to_string(),
        ));
    }
    if m < 2 {
        return Err(HnswError::Corrupted(format!("meta page has m = {m} < 2")));
    }
    if m_max0 < m {
        return Err(HnswError::Corrupted(format!(
            "meta page has m_max0 = {m_max0} < m = {m}"
        )));
    }
    if ef_construction < u32::from(m) {
        return Err(HnswError::Corrupted(format!(
            "meta page has ef_construction = {ef_construction} < m = {m}"
        )));
    }
    // ef_search_default shares the construction domain rule (params.rs:
    // `>= M`) — the caller-expectation mismatch is WARN-level (v1.4 nano),
    // but a structurally out-of-domain value on the page is Corrupted like
    // any other (2026-09-16, mainline Stage B review P3-1).
    if ef_search_default < u32::from(m) {
        return Err(HnswError::Corrupted(format!(
            "meta page has ef_search_default = {ef_search_default} < m = {m}"
        )));
    }
    // Product-linked creation geometry, ONE rule (node.rs) — never
    // re-derived here.
    crate::node::check_creation_geometry(dim, m, m_max0)?;
    let reserved = u16::from_le_bytes(
        page[META_OFF_RESERVED..META_OFF_RESERVED + 2]
            .try_into()
            .unwrap(),
    );
    if reserved != 0 {
        return Err(HnswError::Corrupted(format!(
            "meta page reserved field is {reserved}, must be 0"
        )));
    }
    let rng_seed = u64::from_le_bytes(
        page[META_OFF_RNG_SEED..META_OFF_RNG_SEED + 8]
            .try_into()
            .unwrap(),
    );
    let dir_head = PageId(u64::from_le_bytes(
        page[META_OFF_DIR_HEAD..META_OFF_DIR_HEAD + 8]
            .try_into()
            .unwrap(),
    ));
    if dir_head == PageId::INVALID {
        return Err(HnswError::Corrupted(
            "meta page has dir_head = PageId::INVALID".to_string(),
        ));
    }
    let snapshot_format_version = u16::from_le_bytes(
        page[META_OFF_SNAPSHOT_FORMAT_VERSION..META_OFF_SNAPSHOT_FORMAT_VERSION + 2]
            .try_into()
            .unwrap(),
    );
    if snapshot_format_version != crate::encoding::FORMAT_VERSION {
        return Err(HnswError::Corrupted(format!(
            "meta page snapshot_format_version {snapshot_format_version} != {} (the M4 logical-archive format this build reads)",
            crate::encoding::FORMAT_VERSION
        )));
    }
    let entry_point = u32::from_le_bytes(
        page[META_OFF_ENTRY_POINT..META_OFF_ENTRY_POINT + 4]
            .try_into()
            .unwrap(),
    );
    let max_level = page[META_OFF_MAX_LEVEL];
    // Domain checks on the runtime pair (2026-09-16, mainline Stage B
    // review P3-1 — the write side enforces the format-level rules at the
    // HnswMetaUpdate constructor, WalRecord::hnsw_meta_update; the read
    // side mirrors them): max_level is a 6-bit top_level (<= 63), and
    // NodeId::INVALID is only legal as the empty graph's entry point,
    // where max_level must be 0.
    if max_level > 63 {
        return Err(HnswError::Corrupted(format!(
            "meta page has max_level = {max_level} > 63 (6-bit top_level convention)"
        )));
    }
    // 2026-09-17, review round 6 P3: max_level is the entry point's
    // top_level, produced by next_level(m) — its SEMANTIC ceiling is
    // l_max(m), strictly below the 6-bit format ceiling (13 at m = 16).
    // A value in (l_max(m), 63] is definitionally corrupt: no level draw
    // can produce it. m is already validated (>= 2) above, so l_max's
    // precondition holds. (The HnswMetaUpdate constructor keeps the
    // format-level check only — it never sees m.)
    let l_max = crate::rng::l_max(m);
    if max_level > l_max {
        return Err(HnswError::Corrupted(format!(
            "meta page has max_level = {max_level} > l_max(m = {m}) = {l_max} (no level draw can produce it)"
        )));
    }
    if entry_point == u32::MAX && max_level != 0 {
        return Err(HnswError::Corrupted(format!(
            "meta page has entry_point = NodeId::INVALID but max_level = {max_level} (INVALID is only legal for the empty graph, max_level = 0)"
        )));
    }
    Ok(MetaParams {
        entry_point,
        max_level,
        metric,
        selection,
        dim,
        m,
        m_max0,
        ef_construction,
        ef_search_default,
        rng_seed,
        dir_head,
    })
}

/// The caller-supplied expectations for an open (what the graph was
/// believed to have been created with).
#[derive(Debug, Clone, Copy)]
pub struct ExpectedParams {
    /// Expected vector dimension.
    pub dim: u16,
    /// Expected M.
    pub m: u16,
    /// Expected M_max0.
    pub m_max0: u16,
    /// Expected ef_construction.
    pub ef_construction: u32,
    /// Expected ef_search_default (warning-level only).
    pub ef_search_default: u32,
    /// Expected metric.
    pub metric: Metric,
    /// Expected neighbor-selection mode.
    pub selection: NeighborSelection,
}

/// Check stored meta against caller expectations. The graph-destroying set
/// (dim/m/m_max0/ef_construction/metric/selection — "错配静默毁图" set,
/// §6) fails loudly with `InvalidArgument` naming the field and both sides;
/// `ef_search_default` mismatches are returned as a warning STRING in the
/// result vector instead (performance knob, v1.4 nano "WARN not fail" —
/// the crate's dependency freeze {thiserror, crc32fast, pg-storage} bans a
/// tracing dependency, so warnings travel as strings for the caller to
/// dispose of). The meta value wins for ef_search_default.
pub(crate) fn check_expected(meta: &MetaParams, expected: &ExpectedParams) -> Result<Vec<String>> {
    fn mismatch(
        field: &str,
        stored: impl std::fmt::Display,
        expected: impl std::fmt::Display,
    ) -> HnswError {
        HnswError::InvalidArgument(format!(
            "meta page {field} = {stored}, caller expected {expected} — creation-parameter mismatch must fail loudly (§6)"
        ))
    }
    if meta.dim != expected.dim {
        return Err(mismatch("dim", meta.dim, expected.dim));
    }
    if meta.m != expected.m {
        return Err(mismatch("m", meta.m, expected.m));
    }
    if meta.m_max0 != expected.m_max0 {
        return Err(mismatch("m_max0", meta.m_max0, expected.m_max0));
    }
    if meta.ef_construction != expected.ef_construction {
        return Err(mismatch(
            "ef_construction",
            meta.ef_construction,
            expected.ef_construction,
        ));
    }
    if meta.metric != expected.metric {
        return Err(mismatch(
            "metric",
            format!("{:?}", meta.metric),
            format!("{:?}", expected.metric),
        ));
    }
    if meta.selection != expected.selection {
        return Err(mismatch(
            "neighbor_selection",
            format!("{:?}", meta.selection),
            format!("{:?}", expected.selection),
        ));
    }
    let mut warnings = Vec::new();
    if meta.ef_search_default != expected.ef_search_default {
        warnings.push(format!(
            "ef_search_default: meta page says {}, caller expected {} — performance knob only, using the meta value (v1.4 nano WARN-not-fail)",
            meta.ef_search_default, expected.ef_search_default
        ));
    }
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{init_meta_page, init_node_page};
    use crate::params::NodeId;

    fn sample() -> MetaParams {
        MetaParams {
            entry_point: 7,
            max_level: 2,
            metric: Metric::L2,
            selection: NeighborSelection::Heuristic,
            dim: 128,
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search_default: 64,
            rng_seed: 0xDEAD_BEEF_1234_5678,
            dir_head: PageId(7),
        }
    }

    fn expected_of(p: &MetaParams) -> ExpectedParams {
        ExpectedParams {
            dim: p.dim,
            m: p.m,
            m_max0: p.m_max0,
            ef_construction: p.ef_construction,
            ef_search_default: p.ef_search_default,
            metric: p.metric,
            selection: p.selection,
        }
    }

    #[test]
    fn write_read_roundtrip_all_fields() {
        let mut page = [0u8; PAGE_SIZE];
        init_meta_page(&mut page);
        let p = sample();
        write_meta(&mut page, &p);
        let got = read_meta(&page).unwrap();
        assert_eq!(got, p);
        // Frozen offsets (Stage A contract): entry_point lives at 32,
        // max_level at 36 — position is format.
        assert_eq!(
            u32::from_le_bytes(page[32..36].try_into().unwrap()),
            p.entry_point
        );
        assert_eq!(page[36], p.max_level);
        // Empty-graph entry point is legal (INVALID + max_level 0).
        let mut page = [0u8; PAGE_SIZE];
        init_meta_page(&mut page);
        let p = MetaParams {
            entry_point: NodeId::INVALID.0,
            max_level: 0,
            ..sample()
        };
        write_meta(&mut page, &p);
        assert_eq!(read_meta(&page).unwrap(), p);
    }

    #[test]
    fn read_meta_rejects_each_structural_defect() {
        // Wrong page type.
        let mut page = [0u8; PAGE_SIZE];
        init_node_page(&mut page);
        assert!(read_meta(&page).is_err());

        let p = sample();
        let defect = |mutate: &dyn Fn(&mut [u8; PAGE_SIZE])| {
            let mut page = [0u8; PAGE_SIZE];
            init_meta_page(&mut page);
            write_meta(&mut page, &p);
            mutate(&mut page);
            read_meta(&page)
        };
        // meta_format_version = 2.
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_FORMAT_VERSION] = 2).is_err());
        // Unknown metric / selection discriminants.
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_METRIC] = 9).is_err());
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_SELECTION] = 9).is_err());
        // dim = 0 / m = 1 / m_max0 < m / ef_construction < m.
        assert!(defect(
            &|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_DIM..META_OFF_DIM + 2]
                .copy_from_slice(&0u16.to_le_bytes())
        )
        .is_err());
        assert!(
            defect(&|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_M..META_OFF_M + 2]
                .copy_from_slice(&1u16.to_le_bytes()))
            .is_err()
        );
        assert!(defect(
            &|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_M_MAX0..META_OFF_M_MAX0 + 2]
                .copy_from_slice(&15u16.to_le_bytes())
        )
        .is_err());
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg
            [META_OFF_EF_CONSTRUCTION..META_OFF_EF_CONSTRUCTION + 4]
            .copy_from_slice(&15u32.to_le_bytes()))
        .is_err());
        // Creation-geometry overflow (dim 1792 with the defaults — the
        // product-linked check is node.rs's single rule).
        assert!(defect(
            &|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_DIM..META_OFF_DIM + 2]
                .copy_from_slice(&1792u16.to_le_bytes())
        )
        .is_err());
        // reserved != 0.
        assert!(defect(
            &|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_RESERVED..META_OFF_RESERVED + 2]
                .copy_from_slice(&1u16.to_le_bytes())
        )
        .is_err());
        // dir_head = INVALID.
        assert!(defect(
            &|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_DIR_HEAD..META_OFF_DIR_HEAD + 8]
                .copy_from_slice(&PageId::INVALID.0.to_le_bytes())
        )
        .is_err());
        // snapshot_format_version = 0.
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg
            [META_OFF_SNAPSHOT_FORMAT_VERSION..META_OFF_SNAPSHOT_FORMAT_VERSION + 2]
            .copy_from_slice(&0u16.to_le_bytes()))
        .is_err());
        // 2026-09-16, mainline Stage B review P3-1 — three same-class
        // structural checks:
        // ef_search_default < m (domain rule, WARN only covers the
        // caller-expectation mismatch).
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg
            [META_OFF_EF_SEARCH_DEFAULT..META_OFF_EF_SEARCH_DEFAULT + 4]
            .copy_from_slice(&1u32.to_le_bytes()))
        .is_err());
        // max_level > 63 (6-bit top_level convention).
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_MAX_LEVEL] = 200).is_err());
        // 2026-09-17, review round 6 P3: max_level in (l_max(m), 63] —
        // format-legal but unreachable by any level draw (l_max(16) = 13).
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| pg[META_OFF_MAX_LEVEL] = 14).is_err());
        // Boundary: max_level == l_max(16) = 13 (the largest drawable
        // level) is LEGAL — the semantic ceiling is inclusive.
        let mut page = [0u8; PAGE_SIZE];
        init_meta_page(&mut page);
        let p = MetaParams {
            max_level: 13,
            ..sample()
        };
        write_meta(&mut page, &p);
        assert_eq!(read_meta(&page).unwrap(), p);
        // entry_point = INVALID with a nonzero max_level (contradiction).
        assert!(defect(&|pg: &mut [u8; PAGE_SIZE]| {
            pg[META_OFF_ENTRY_POINT..META_OFF_ENTRY_POINT + 4]
                .copy_from_slice(&u32::MAX.to_le_bytes());
            pg[META_OFF_MAX_LEVEL] = 5;
        })
        .is_err());
    }

    #[test]
    fn check_expected_hard_mismatches_and_ef_warn() {
        let p = sample();
        assert!(check_expected(&p, &expected_of(&p)).unwrap().is_empty());
        // Each hard mismatch fails loudly (spelled out one per line —
        // a boxed-closure table would be denser but trips clippy's
        // type_complexity gate).
        let mut e = expected_of(&p);
        e.dim = 64;
        assert!(matches!(
            check_expected(&p, &e),
            Err(HnswError::InvalidArgument(_))
        ));
        let mut e = expected_of(&p);
        e.m = 8;
        assert!(matches!(
            check_expected(&p, &e),
            Err(HnswError::InvalidArgument(_))
        ));
        let mut e = expected_of(&p);
        e.m_max0 = 16;
        assert!(matches!(
            check_expected(&p, &e),
            Err(HnswError::InvalidArgument(_))
        ));
        let mut e = expected_of(&p);
        e.ef_construction = 100;
        assert!(matches!(
            check_expected(&p, &e),
            Err(HnswError::InvalidArgument(_))
        ));
        let mut e = expected_of(&p);
        e.metric = Metric::Cosine;
        assert!(matches!(
            check_expected(&p, &e),
            Err(HnswError::InvalidArgument(_))
        ));
        let mut e = expected_of(&p);
        e.selection = NeighborSelection::Simple;
        assert!(matches!(
            check_expected(&p, &e),
            Err(HnswError::InvalidArgument(_))
        ));
        // ef_search_default mismatch: Ok with exactly one warning naming the
        // field.
        let mut e = expected_of(&p);
        e.ef_search_default = 128;
        let warnings = check_expected(&p, &e).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("ef_search_default"), "{}", warnings[0]);
    }
}
