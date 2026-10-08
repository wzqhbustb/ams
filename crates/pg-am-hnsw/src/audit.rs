//! Post-recovery audit — Phase 2 M5 Stage D slice 1 (tech-selection §11.3).
//!
//! §11.3's integrity defense has three layers; this module is the third:
//!
//! 1. **In-stream redo gates** (redo.rs): the pd_lsn guard, the funnel
//!    validation, and assertion ④'s txn_id gate (`require_utility_txn` —
//!    HNSW records are utility operations, §8.1, and must carry
//!    `txn_id = INVALID`; the redo-side rejection lives there, this module
//!    has no page-side content for ④).
//! 2. **The open protocol** (index.rs `open`): meta structural validation,
//!    the chain walk, open-repair.
//! 3. **This audit**: a full re-derivation that trusts no handle cache —
//!    it re-walks the directory chain and re-reads every node entry.
//!
//! The §11.3 assertion coverage:
//!
//! - **a (referential integrity)**: forward — every directory entry maps to
//!   an occupied slot on a node page (else `Corrupted`, "audit a");
//!   endpoint — every neighbor id is below the chain high-water mark.
//! - **b (level compatibility)**: a level-`l` edge may only point at a node
//!   whose `top_level >= l`.
//! - **c (entry point)**: empty graph ⇒ `entry_point = INVALID ∧
//!   max_level = 0`; otherwise `entry_point < hwm` AND the entry point's
//!   `top_level == max_level` — the WEAKENED §8.3① invariant (v1.8): the
//!   audit does NOT assert "no node stands above max_level"; such nodes
//!   are counted in `hidden_high_level_count`, not rejected.
//! - **d/e (NodeInit/DirAppend identity)**: the post-hoc payload-identity
//!   check is NOT evaluable at audit time — §7.2 node entries do not
//!   store their NodeId, so "this entry is the one record N described"
//!   cannot be re-derived from pages. The evaluable residue is asserted
//!   instead: **mapping uniqueness** (no two directory entries name the
//!   same `(page, slot)` — "audit d/e") and occupancy (the a-forward
//!   form). Payload-level identity is carried by the redo-time LSN order
//!   (NodeInit precedes DirAppend precedes every reference). This is an
//!   honest narrowing of the audit scope; mainline records it in the
//!   tech-selection.
//! - **f (page types)**: every directory entry's target page is a NODE
//!   page ("audit f"); the chain pages' DIR typing is already asserted by
//!   `check_dir_chain` (assertion 1–4 of §7.1/§11.3), which this audit
//!   reuses unchanged.
//! - **stored vectors (§5 stored-side mirror)**: every mapped entry's
//!   vector is re-validated — all components finite (pages carry no
//!   checksum; a bit-rotted NaN/±inf would otherwise reach `Cand::cmp`'s
//!   panic on the search path), and an all-zero vector is rejected under
//!   the cosine metric (L2/IP allow zeros, same as insert). This makes
//!   the `GraphAccess` trait contract's citation of this audit as a
//!   finiteness guarantor true (2026-09-21, mainline final review).
//!
//! **Non-blocking statistics** (§8.3⑤): INITIALIZING entries reachable
//! from the directory (the 4–5 crash-window ghost), orphan entries
//! occupying a slot no directory entry names (the 3–4 window orphan), and
//! tombstoned entries (M5 has no tombstone producer — format delivered,
//! semantics land in M6) are COUNTED, never failures: all three are legal
//! crash-window residues the redo/open layers already tolerate. One state
//! combination IS loud even here: `tombstoned ∧ ¬LIVE` is unreachable via
//! any legal record stream (the Tombstone funnel requires LIVE;
//! PublishLive only sets the live bit), so it fails as `Corrupted`
//! (2026-09-21, mainline final review round 2). The
//! orphan scan covers REFERENCED pages only (pages at least one directory
//! entry names): a 3–4-window orphan that is the sole occupant of a
//! freshly allocated page is invisible to it — a registered undercount
//! direction (2026-09-21, adversarial review P3-2); a full page-allocator
//! scan was evaluated and deliberately NOT done (audit walks the index,
//! not the storage file).
//!
//! **Consumption premise** (2026-09-21, adversarial review P3-1): the
//! audit runs AFTER `open` — the open protocol's repair closes the §8.2
//! step 6–7 window (fully connected node, meta never updated), which is a
//! LEGAL residue. Calling `audit_index` on a recovered-but-never-opened
//! index in that state is rejected by assertion c (`entry_point =
//! INVALID >= hwm`) — expected, not a false positive: the layer order is
//! redo → open (repair) → audit.
//!
//! **Reachability scan** (M6 Stage 0, M6 tech-selection §8.3 先导):
//! [`audit_reachability`] BFS-walks the graph from the entry point under
//! the search path's level discipline (a level-`l` edge may only be
//! followed to a node whose `top_level >= l`, descending within a node is
//! free) and reports the LIVE nodes no legal search can ever reach — the
//! input of §14.1's hanging-node verdict. State bits are NOT traversal
//! predicates (the search path never checks them; INITIALIZING/tombstoned
//! nodes route fine) — they only partition the UNREACHABLE set in the
//! report. The scan REPORTS, never rejects: unreachable nodes are a legal
//! residue shape (crash windows, and in M6 delete/vacuum interleavings);
//! structural corruption (bad page type, neighbor ≥ hwm, level-discipline
//! violation) still fails stop as `Corrupted`, mirroring assertions a/b/f.
//! Same consumption premise as [`audit_index`] — run after `open`, and
//! only at quiesce (M6 §12.5: under concurrent writes the scan would
//! misread legal intermediate states).
//!
//! **Degree caps** ride along for free: `apply::neighbor_iter`'s
//! `checked_count` rejects a stored count above the level's reserved
//! capacity as `Corrupted` before any id is read.

use std::collections::BTreeSet;

use pg_storage::buffer_pool::BufferPool;
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::apply;
use crate::dir;
use crate::error::{HnswError, Result};
use crate::meta;
use crate::node::NodeGeometry;
use crate::page::{page_type, storage_err, PAGE_TYPE_META, PAGE_TYPE_NODE};
use crate::paged::PagedGraph;
use crate::params::NodeId;

/// Outcome of a clean [`audit_index`] run: the graph's re-derived shape
/// plus the non-blocking residue counters (§8.3⑤ — see the module header).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditReport {
    /// Directory-mapped node count (the chain high-water mark).
    pub node_count: u64,
    /// The meta page's entry point (`u32::MAX` = `NodeId::INVALID` on an
    /// empty graph).
    pub entry_point: u32,
    /// The meta page's max level.
    pub max_level: u8,
    /// Mapped entries with the LIVE bit set.
    pub live_count: u64,
    /// Mapped entries still INITIALIZING (the 4–5 crash-window ghost —
    /// counted, legal).
    pub initializing_count: u64,
    /// Mapped entries with the tombstone bit set (M6 semantics; M5 only
    /// counts the format).
    pub tombstoned_count: u64,
    /// Mapped entries whose `top_level` exceeds the meta `max_level`
    /// (the weakened §8.3① invariant tolerates them — counted, legal).
    pub hidden_high_level_count: u64,
    /// Occupied node-entry slots no directory entry names (the 3–4
    /// crash-window orphan — counted, legal). Scope: REFERENCED pages
    /// only (see the module header — a sole-occupant orphan on an
    /// unreferenced fresh page is a registered undercount).
    pub orphan_entry_count: u64,
    /// Total directed edges across all levels.
    pub edge_count: u64,
}

/// Run the §11.3 post-recovery audit against the index rooted at
/// `meta_page_id`. Fail-stop on any assertion violation (`Corrupted`);
/// legal crash-window residues are counted in the report, never rejected.
///
/// **Premise: run AFTER `open`** (module header, "Consumption premise") —
/// a recovered-but-never-opened index may legitimately carry the §8.2
/// step 6–7 residue (entry_point = INVALID with a non-empty chain), which
/// assertion c rejects by design; `open`'s repair closes it first.
///
/// The audit trusts NOTHING cached: it re-reads the meta page, re-walks
/// the directory chain (`check_dir_chain`, reusing the open protocol's
/// single implementation), and re-reads every node entry. It is a cold
/// path — neighbors are re-pinned per node rather than cached (a 1M-node
/// neighbor materialization is not a viable memory shape).
pub fn audit_index(buffer_pool: &BufferPool, meta_page_id: PageId) -> Result<AuditReport> {
    // 1. Meta page: type tag + full structural validation.
    let params = {
        let guard = buffer_pool
            .pin(meta_page_id)
            .map_err(|e| storage_err("audit: pin meta page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        if page_type(page) != PAGE_TYPE_META {
            return Err(HnswError::Corrupted(format!(
                "audit: page {} is not a meta page (page_type {})",
                meta_page_id.0,
                page_type(page)
            )));
        }
        meta::read_meta(page)?
    };
    let geometry = NodeGeometry {
        dim: params.dim,
        m: params.m,
        m_max0: params.m_max0,
    };
    let l_max = crate::rng::l_max(params.m);

    // 2. Directory chain walk (assertions 1–4 + HWM), same fetch idiom as
    // the open protocol.
    let fetch = |page_id: PageId| -> Result<[u8; PAGE_SIZE]> {
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| storage_err("audit: pin directory page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        Ok(*page)
    };
    let chain = dir::check_dir_chain(params.dir_head, fetch)?;
    let hwm = chain.hwm;
    let graph = PagedGraph::new(
        buffer_pool,
        &chain.pages,
        hwm,
        params.dim,
        params.m,
        params.m_max0,
        params.metric,
    );

    // 3. Pass 1 (entries): f (target page type), a-forward (occupied
    // slot), d/e residue (mapping uniqueness), l_max bound, state counts.
    let mut top_levels: Vec<u8> = Vec::with_capacity(hwm as usize);
    let mut mappings: BTreeSet<(u64, u16)> = BTreeSet::new();
    let mut referenced_pages: BTreeSet<PageId> = BTreeSet::new();
    let (mut live_count, mut initializing_count, mut tombstoned_count) = (0, 0, 0);
    for id in 0..hwm {
        let (page_id, slot) = graph.resolve(NodeId(id as u32))?;
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| storage_err("audit: pin node page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        if page_type(page) != PAGE_TYPE_NODE {
            return Err(HnswError::Corrupted(format!(
                "audit f: directory entry {id} points at page {} of page_type {}, not a node page",
                page_id.0,
                page_type(page)
            )));
        }
        if slot >= apply::slot_count(page)? || !apply::slot_is_occupied(page, slot) {
            return Err(HnswError::Corrupted(format!(
                "audit a: directory entry {id} maps to empty/out-of-range slot {slot} on page {}",
                page_id.0
            )));
        }
        if !mappings.insert((page_id.0, slot)) {
            return Err(HnswError::Corrupted(format!(
                "audit d/e: directory entries map twice to ({}, {slot}) — the mapping must be unique",
                page_id.0
            )));
        }
        referenced_pages.insert(page_id);
        // Stored-vector validation (2026-09-21, mainline final review):
        // the GraphAccess trait contract (graph.rs) cites THIS audit as a
        // stored-vector finiteness guarantor — make the citation true.
        // Pages carry no checksum, so a bit-rotted NaN/±inf component
        // would otherwise reach `Cand::cmp`'s panic on the search path;
        // the page is already pinned, so the scan is memory-bandwidth
        // only. Cosine additionally rejects an all-zero stored vector
        // (the §5 entry rule mirrored on the stored side; L2/IP allow
        // zeros, same as insert).
        let mut has_nonzero = false;
        for x in apply::vector_iter(page, slot, params.dim)? {
            if !x.is_finite() {
                return Err(HnswError::Corrupted(format!(
                    "audit: node {id} has a non-finite vector component (§5 stored-side — the search path's finiteness premise)"
                )));
            }
            has_nonzero |= x != 0.0;
        }
        if params.metric == crate::graph::Metric::Cosine && !has_nonzero {
            return Err(HnswError::Corrupted(format!(
                "audit: node {id} has an all-zero vector under the cosine metric (§5 stored-side)"
            )));
        }
        let live = apply::entry_is_live(page, slot, params.dim)?;
        let tombstoned = apply::entry_is_tombstoned(page, slot, params.dim)?;
        // State-machine legality (2026-09-21, mainline final review
        // round 2): tombstone requires LIVE at every layer (the §10.1
        // funnel's Tombstone precondition; PublishLive only ever SETS the
        // live bit), so `tombstoned ∧ ¬live` is unreachable via ANY legal
        // record stream, M6's included — only raw page corruption
        // produces it. Loud, not counted.
        if tombstoned && !live {
            return Err(HnswError::Corrupted(format!(
                "audit: node {id} is tombstoned but not LIVE — the §8.1③ state machine (INITIALIZING → LIVE → tombstoned) makes this state unreachable by any legal record stream"
            )));
        }
        if live {
            live_count += 1;
        } else {
            initializing_count += 1;
        }
        if tombstoned {
            tombstoned_count += 1;
        }
        let top = apply::entry_top_level(page, slot, params.dim)?;
        if top > l_max {
            return Err(HnswError::Corrupted(format!(
                "audit: node {id} has top_level {top} > l_max(m = {}) = {l_max} (no level draw can produce it)",
                params.m
            )));
        }
        top_levels.push(top);
    }

    // 4. Assertion c + the weakened §8.3① max_level invariant.
    let mut hidden_high_level_count = 0;
    if hwm == 0 {
        if params.entry_point != NodeId::INVALID.0 || params.max_level != 0 {
            return Err(HnswError::Corrupted(format!(
                "audit c: empty graph must carry entry_point = INVALID and max_level = 0 (found entry_point {}, max_level {})",
                params.entry_point, params.max_level
            )));
        }
    } else {
        // Covers entry_point = INVALID too (u32::MAX >= hwm always).
        if u64::from(params.entry_point) >= hwm {
            return Err(HnswError::Corrupted(format!(
                "audit c: entry_point {} >= chain high-water mark {hwm}",
                params.entry_point
            )));
        }
        let ep_top = top_levels[params.entry_point as usize];
        if ep_top != params.max_level {
            return Err(HnswError::Corrupted(format!(
                "audit c: entry point {} has top_level {ep_top} != meta max_level {} (weakened §8.3①: the entry point must sit AT max_level)",
                params.entry_point, params.max_level
            )));
        }
        hidden_high_level_count =
            top_levels.iter().filter(|&&t| t > params.max_level).count() as u64;
    }

    // 5. Pass 2 (edges): strict ascending order (write-path invariant), no
    // self-loops, a-endpoint (nb < hwm), b (level compatibility).
    let mut edge_count = 0u64;
    for id in 0..hwm {
        let (page_id, slot) = graph.resolve(NodeId(id as u32))?;
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| storage_err("audit: pin node page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        for level in 0..=top_levels[id as usize] {
            let list: Vec<u32> = apply::neighbor_iter(page, slot, geometry, level)?.collect();
            if list.windows(2).any(|w| w[0] >= w[1]) {
                return Err(HnswError::Corrupted(format!(
                    "audit: node {id} level {level} neighbor list is not strictly ascending (write-path invariant)"
                )));
            }
            for &nb in &list {
                if nb == id as u32 {
                    return Err(HnswError::Corrupted(format!(
                        "audit: node {id} level {level} has a self-loop"
                    )));
                }
                if u64::from(nb) >= hwm {
                    return Err(HnswError::Corrupted(format!(
                        "audit a: node {id} level {level} references node {nb} >= high-water mark {hwm}"
                    )));
                }
                if top_levels[nb as usize] < level {
                    return Err(HnswError::Corrupted(format!(
                        "audit b: node {id} level {level} references node {nb} with top_level {} < {level}",
                        top_levels[nb as usize]
                    )));
                }
                edge_count += 1;
            }
        }
    }

    // 6. Orphan scan (§8.3⑤ statistic + the d/e reverse direction): every
    // occupied slot on a referenced page that no directory entry names.
    let mut orphan_entry_count = 0u64;
    for &page_id in &referenced_pages {
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| storage_err("audit: pin node page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        for slot in 0..apply::slot_count(page)? {
            if apply::slot_is_occupied(page, slot) && !mappings.contains(&(page_id.0, slot)) {
                orphan_entry_count += 1;
            }
        }
    }

    Ok(AuditReport {
        node_count: hwm,
        entry_point: params.entry_point,
        max_level: params.max_level,
        live_count,
        initializing_count,
        tombstoned_count,
        hidden_high_level_count,
        orphan_entry_count,
        edge_count,
    })
}

/// Outcome of [`audit_reachability`]: how many nodes the search path can
/// reach from the entry point, and the unreachable set partitioned by
/// state (M6 Stage 0, M6 tech-selection §8.3 先导 — the §14.1
/// hanging-node verdict reads `unreachable_live`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReachabilityReport {
    /// Directory-mapped node count (the chain high-water mark).
    pub node_count: u64,
    /// Nodes reachable from the entry point under the level discipline
    /// (any state — INITIALIZING/tombstoned nodes route; see the module
    /// header).
    pub reachable_count: u64,
    /// LIVE, non-tombstoned nodes no legal search can reach — the
    /// hanging-node set. Reported, never rejected (a legal residue shape;
    /// the §14.1 threshold verdict belongs to the caller).
    pub unreachable_live: Vec<u32>,
    /// Unreachable INITIALIZING nodes (crash-window residue) — counted.
    pub unreachable_initializing_count: u64,
    /// Unreachable tombstoned nodes (M6 delete residue) — counted. A
    /// tombstoned node that is NOT live is unreachable via any legal
    /// record stream (audit_index's loud combination) and fails stop.
    pub unreachable_tombstoned_count: u64,
}

/// BFS from the entry point under the search path's level discipline
/// (module header, "Reachability scan"). Reports the unreachable set by
/// state; fails stop (`Corrupted`) on structural violations — bad target
/// page type, a neighbor id ≥ hwm, or a level-`l` edge to a node whose
/// `top_level < l` (assertions f/a/b mirrored, since the BFS would
/// otherwise mis-walk a corrupt shape).
///
/// **Premise: quiesce, after `open`** (M6 §12.5) — under concurrent
/// writes the scan misreads legal intermediate states; a
/// recovered-but-never-opened index with the §8.2 step 6–7 residue is
/// rejected by the entry-point check, same as `audit_index`.
///
/// Cold path: neighbors are re-pinned per (node, level), same idiom as
/// [`audit_index`]'s edge pass.
pub fn audit_reachability(
    buffer_pool: &BufferPool,
    meta_page_id: PageId,
) -> Result<ReachabilityReport> {
    // 1. Meta page: type tag + full structural validation (audit_index's
    // idiom — trust nothing cached).
    let params = {
        let guard = buffer_pool
            .pin(meta_page_id)
            .map_err(|e| storage_err("reachability: pin meta page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        if page_type(page) != PAGE_TYPE_META {
            return Err(HnswError::Corrupted(format!(
                "reachability: page {} is not a meta page (page_type {})",
                meta_page_id.0,
                page_type(page)
            )));
        }
        meta::read_meta(page)?
    };
    let geometry = NodeGeometry {
        dim: params.dim,
        m: params.m,
        m_max0: params.m_max0,
    };
    let l_max = crate::rng::l_max(params.m);

    // 2. Directory chain walk (same fetch idiom as the open protocol).
    let fetch = |page_id: PageId| -> Result<[u8; PAGE_SIZE]> {
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| storage_err("reachability: pin directory page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        Ok(*page)
    };
    let chain = dir::check_dir_chain(params.dir_head, fetch)?;
    let hwm = chain.hwm;
    let graph = PagedGraph::new(
        buffer_pool,
        &chain.pages,
        hwm,
        params.dim,
        params.m,
        params.m_max0,
        params.metric,
    );

    if hwm == 0 {
        // Assertion c's empty-graph shape, mirrored from audit_index: an
        // empty chain must carry entry_point = INVALID ∧ max_level = 0 —
        // anything else is a corrupt meta, loud here too (the same shape
        // must not pass one entry point and fail the other).
        if params.entry_point != NodeId::INVALID.0 || params.max_level != 0 {
            return Err(HnswError::Corrupted(format!(
                "reachability c: empty graph must carry entry_point = INVALID and max_level = 0 (found entry_point {}, max_level {})",
                params.entry_point, params.max_level
            )));
        }
        return Ok(ReachabilityReport {
            node_count: 0,
            reachable_count: 0,
            unreachable_live: vec![],
            unreachable_initializing_count: 0,
            unreachable_tombstoned_count: 0,
        });
    }
    // The BFS needs a valid start; assertion c's non-empty shape lives in
    // audit_index, this is the same loud rejection.
    if u64::from(params.entry_point) >= hwm {
        return Err(HnswError::Corrupted(format!(
            "reachability: entry_point {} >= chain high-water mark {hwm} — no BFS start",
            params.entry_point
        )));
    }

    // 3. Per-node shape pass: target page type (f), top_level, state —
    // one pin per node. The scan walks edges later; only the BFS inputs
    // are materialized here.
    let mut top_levels: Vec<u8> = Vec::with_capacity(hwm as usize);
    let mut live: Vec<bool> = Vec::with_capacity(hwm as usize);
    let mut tombstoned: Vec<bool> = Vec::with_capacity(hwm as usize);
    for id in 0..hwm {
        let (page_id, slot) = graph.resolve(NodeId(id as u32))?;
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| storage_err("reachability: pin node page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        if page_type(page) != PAGE_TYPE_NODE {
            return Err(HnswError::Corrupted(format!(
                "reachability f: directory entry {id} points at page {} of page_type {}, not a node page",
                page_id.0,
                page_type(page)
            )));
        }
        let is_live = apply::entry_is_live(page, slot, params.dim)?;
        let is_tombstoned = apply::entry_is_tombstoned(page, slot, params.dim)?;
        // audit_index's loud combination: unreachable via any legal record
        // stream (the Tombstone funnel requires LIVE; PublishLive only
        // SETS the live bit) — only raw page corruption produces it.
        if is_tombstoned && !is_live {
            return Err(HnswError::Corrupted(format!(
                "reachability: node {id} is tombstoned but not LIVE — unreachable via any legal record stream"
            )));
        }
        let top = apply::entry_top_level(page, slot, params.dim)?;
        // audit_index's hard bound, mirrored (review round 3): no level
        // draw can produce top_level > l_max(m) — only corruption can.
        // (Distinct from the hidden-high-level SOFT residue — top >
        // max_level — which is legal and merely unrouted; only the hard
        // bound is mirrored.)
        if top > l_max {
            return Err(HnswError::Corrupted(format!(
                "reachability: node {id} has top_level {top} > l_max(m = {}) = {l_max} (no level draw can produce it)",
                params.m
            )));
        }
        top_levels.push(top);
        live.push(is_live);
        tombstoned.push(is_tombstoned);
    }

    // Assertion c's non-empty shape, mirrored from audit_index (review
    // round 2): the entry point must sit AT max_level. A corrupt meta
    // (top_level(entry) != max_level) would otherwise make the BFS
    // silently start at the entry's actual top level and report a
    // reachable set computed under a different discipline — loud instead.
    let ep_top = top_levels[params.entry_point as usize];
    if ep_top != params.max_level {
        return Err(HnswError::Corrupted(format!(
            "reachability c: entry point {} has top_level {ep_top} != meta max_level {} (the entry point must sit AT max_level)",
            params.entry_point, params.max_level
        )));
    }

    // 4. BFS, level by level from max_level down to 0. Seeds for level `l`
    // are every reachable node with top_level >= l (reaching a node at a
    // higher level descends within it for free — the search path's
    // discipline). A node reached mid-level is pushed immediately, so its
    // own level-l edges are walked in the same pass. The iteration never
    // revisits a HIGHER level for a newly reached node — faithful to the
    // search path, which descends greedily and never walks back up (so a
    // node first reached at level 0 contributes only its level-0 edges,
    // even if its top_level is higher).
    let mut reachable = vec![false; hwm as usize];
    reachable[params.entry_point as usize] = true;
    for level in (0..=params.max_level).rev() {
        let mut stack: Vec<u32> = (0..hwm as u32)
            .filter(|&n| reachable[n as usize] && top_levels[n as usize] >= level)
            .collect();
        while let Some(n) = stack.pop() {
            let (page_id, slot) = graph.resolve(NodeId(n))?;
            let guard = buffer_pool
                .pin(page_id)
                .map_err(|e| storage_err("reachability: pin node page", e))?;
            let page: &[u8; PAGE_SIZE] = guard
                .page()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            for nb in apply::neighbor_iter(page, slot, geometry, level)? {
                if u64::from(nb) >= hwm {
                    return Err(HnswError::Corrupted(format!(
                        "reachability a: node {n} level {level} references node {nb} >= high-water mark {hwm}"
                    )));
                }
                if top_levels[nb as usize] < level {
                    return Err(HnswError::Corrupted(format!(
                        "reachability b: node {n} level {level} references node {nb} with top_level {} < {level} (level discipline)",
                        top_levels[nb as usize]
                    )));
                }
                if !reachable[nb as usize] {
                    reachable[nb as usize] = true;
                    stack.push(nb);
                }
            }
        }
    }

    // 5. Partition the unreachable set by state. LIVE ∧ ¬tombstoned is the
    // hanging-node set; INITIALIZING and tombstoned are counted (legal
    // residues), never listed.
    let mut reachable_count = 0u64;
    let mut unreachable_live = Vec::new();
    let mut unreachable_initializing_count = 0u64;
    let mut unreachable_tombstoned_count = 0u64;
    for n in 0..hwm as usize {
        if reachable[n] {
            reachable_count += 1;
            continue;
        }
        if live[n] && !tombstoned[n] {
            unreachable_live.push(n as u32);
        }
        if !live[n] {
            unreachable_initializing_count += 1;
        }
        if tombstoned[n] {
            unreachable_tombstoned_count += 1;
        }
    }

    Ok(ReachabilityReport {
        node_count: hwm,
        reachable_count,
        unreachable_live,
        unreachable_initializing_count,
        unreachable_tombstoned_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Metric, NeighborSelection};
    use crate::index::create;
    use crate::page::{init_node_page, log_page_init};
    use crate::params::HnswParams;
    use pg_storage::config::StorageConfig;
    use pg_storage::engine::StorageEngine;
    use pg_storage::wal::record::WalRecord;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    const DIM: u16 = 128;
    const SEED: u64 = 0x5EED;
    const GEO: NodeGeometry = NodeGeometry {
        dim: DIM,
        m: 16,
        m_max0: 32,
    };

    fn fresh_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pg_am_hnsw_m5_audit-{}-{}-{tag}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// One node entry of a synthetic residue graph.
    struct NodeSpec {
        top_level: u8,
        /// Per-level neighbor lists (index = level; len must be
        /// top_level + 1 — unlisted content stays the NodeInit zeros).
        level_lists: Vec<Vec<u32>>,
        live: bool,
    }

    /// Build a synthetic graph residue through the FPI pattern (index.rs's
    /// open-repair tests): `specs[s]` becomes the entry at slot s of one
    /// fresh node page; the directory content comes from
    /// `dir_map(meta_page_id)` (position = node id; `(None, slot)` targets
    /// the node page, `(Some(page), _)` overrides the target page — the
    /// audit-f case); meta gets `(entry_point, max_level)`. All content
    /// lands as post-image FPIs + pd_lsn stamps, then a checkpointless
    /// crash (`mem::forget`) and a redo-equipped reopen. Returns
    /// `(engine, meta_page_id, dir)`.
    ///
    /// NOTE: the residue is audited by calling [`audit_index`] DIRECTLY —
    /// `open` would reject several of these shapes itself (they are two
    /// independent defense lines).
    fn build_residue_with(
        tag: &str,
        specs: &[NodeSpec],
        entry_point: u32,
        max_level: u8,
        dir_map: impl FnOnce(PageId) -> Vec<(Option<PageId>, u16)>,
    ) -> (StorageEngine, PageId, PathBuf) {
        let dir = fresh_dir(tag);
        let config = StorageConfig::new(&dir);
        let meta_page_id = {
            let engine = StorageEngine::open(&dir, &config).unwrap();
            let index = create(
                engine.buffer_pool(),
                engine.wal_writer(),
                HnswParams::default(),
                DIM,
                Metric::L2,
                NeighborSelection::Heuristic,
                SEED,
            )
            .unwrap();
            let node_page_id = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                let page_id = guard.page_id();
                let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
                init_node_page(page);
                log_page_init(engine.wal_writer(), page_id, page).unwrap();
                page_id
            };
            {
                let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
                let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
                for (i, spec) in specs.iter().enumerate() {
                    let vector = vec![(i + 1) as f32; DIM as usize];
                    let slot =
                        apply::append_node(page, i as u32, spec.top_level, GEO, &vector).unwrap();
                    for (l, list) in spec.level_lists.iter().enumerate() {
                        apply::set_neighbors(page, slot, GEO, l as u8, list).unwrap();
                    }
                    if spec.live {
                        apply::publish_live(page, slot, DIM).unwrap();
                    }
                }
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(node_page_id, page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            {
                let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
                let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
                for (i, (target, slot)) in dir_map(index.meta_page_id()).iter().enumerate() {
                    apply::dir_append(page, i as u32, target.unwrap_or(node_page_id), *slot)
                        .unwrap();
                }
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(index.dir_head(), page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            {
                let mut guard = engine.buffer_pool().pin_mut(index.meta_page_id()).unwrap();
                let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
                apply::apply_meta(page, entry_point, max_level);
                let lsn = engine
                    .wal_writer()
                    .append(
                        WalRecord::full_page_image(index.meta_page_id(), page.to_vec()).unwrap(),
                    )
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // crash: no checkpoint
            index.meta_page_id()
        };
        let engine = StorageEngine::open_with_redo_handlers(
            &dir,
            &config,
            crate::redo::hnsw_redo_handlers(),
            vec![],
        )
        .unwrap();
        (engine, meta_page_id, dir)
    }

    /// The common form: the directory maps every entry onto the one node
    /// page (`(None, slot)` rows).
    fn build_residue(
        tag: &str,
        specs: &[NodeSpec],
        dir_map: &[(Option<PageId>, u16)],
        entry_point: u32,
        max_level: u8,
    ) -> (StorageEngine, PageId, PathBuf) {
        let rows: Vec<(Option<PageId>, u16)> = dir_map.to_vec();
        build_residue_with(tag, specs, entry_point, max_level, |_| rows)
    }

    fn spec(top_level: u8, level_lists: Vec<Vec<u32>>, live: bool) -> NodeSpec {
        NodeSpec {
            top_level,
            level_lists,
            live,
        }
    }

    /// A clean 200-node graph built through the real insert path audits
    /// clean, with the exact expected report shape.
    #[test]
    fn audit_clean_graph() {
        let dir = fresh_dir("clean");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let params = HnswParams::new(4, 8, 16, 4).unwrap();
        let mut index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            params,
            4,
            Metric::L2,
            NeighborSelection::Heuristic,
            0xC0FFEE,
        )
        .unwrap();
        let mut rng = crate::rng::Xoshiro256StarStar::new(0xDEC0DE);
        for _ in 0..200 {
            let v: Vec<f32> = (0..4)
                .map(|_| (rng.next_u64() % 4096) as f32 / 64.0)
                .collect();
            index
                .insert(engine.buffer_pool(), engine.wal_writer(), &v)
                .unwrap();
        }

        let report = index.audit(engine.buffer_pool()).unwrap();
        assert_eq!(report.node_count, 200);
        assert_eq!(report.live_count, 200);
        assert_eq!(report.initializing_count, 0);
        assert_eq!(report.tombstoned_count, 0);
        assert_eq!(report.orphan_entry_count, 0);
        assert_eq!(report.hidden_high_level_count, 0);
        assert!(report.edge_count > 0);
        assert_eq!(report.entry_point, index.entry_point());
        assert_eq!(report.max_level, index.max_level());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An empty graph audits to an all-zero report (and the INVALID entry
    /// point).
    #[test]
    fn audit_empty_graph() {
        let dir = fresh_dir("empty");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        let report = index.audit(engine.buffer_pool()).unwrap();
        assert_eq!(
            report,
            AuditReport {
                node_count: 0,
                entry_point: NodeId::INVALID.0,
                max_level: 0,
                live_count: 0,
                initializing_count: 0,
                tombstoned_count: 0,
                hidden_high_level_count: 0,
                orphan_entry_count: 0,
                edge_count: 0,
            }
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Legal crash-window residues are COUNTED, never rejected (§8.3⑤):
    /// slot 1 is an orphan (occupied, unmapped — the 3–4 window); slot 2 is
    /// a mapped INITIALIZING ghost (the 4–5 window).
    #[test]
    fn audit_counts_initializing_and_orphans() {
        let (engine, meta_page_id, dir) = build_residue(
            "residue-counts",
            &[
                spec(0, vec![vec![]], true),  // slot 0: LIVE, mapped as node 0
                spec(0, vec![vec![]], false), // slot 1: orphan (unmapped)
                spec(0, vec![vec![]], false), // slot 2: INITIALIZING ghost, node 1
            ],
            &[(None, 0), (None, 2)],
            0,
            0,
        );
        let report = audit_index(engine.buffer_pool(), meta_page_id).unwrap();
        assert_eq!(report.node_count, 2);
        assert_eq!(report.live_count, 1);
        // The orphan (slot 1) is unmapped, so it lands in
        // orphan_entry_count, NOT in the mapped-entry state counters —
        // initializing counts only the mapped ghost (slot 2).
        assert_eq!(report.initializing_count, 1);
        assert_eq!(report.orphan_entry_count, 1);
        assert_eq!(report.tombstoned_count, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The negative matrix: every assertion fails loudly with its fragment
    /// (each residue is audited through `audit_index` directly — `open`
    /// would pre-empt several of these).
    #[test]
    fn audit_negative_matrix() {
        // f: directory entry 0 points at the META page.
        let (engine, meta, dir) =
            build_residue_with("neg-f", &[spec(0, vec![vec![]], true)], 0, 0, |meta| {
                vec![(Some(meta), 0)]
            });
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("audit f"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // a (forward): directory entry maps to an empty slot.
        let (engine, meta, dir) = build_residue(
            "neg-a-fwd",
            &[spec(0, vec![vec![]], true)],
            &[(None, 5)],
            0,
            0,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("audit a"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // a (endpoint): node 0's level-0 list references id 5 >= hwm = 2.
        let (engine, meta, dir) = build_residue(
            "neg-a-endpoint",
            &[spec(0, vec![vec![5]], true), spec(0, vec![vec![]], true)],
            &[(None, 0), (None, 1)],
            0,
            0,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("audit a"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // b: node 0 (top_level 1) has a level-1 edge to node 1 (top_level 0).
        let (engine, meta, dir) = build_residue(
            "neg-b",
            &[
                spec(1, vec![vec![], vec![1]], true),
                spec(0, vec![vec![]], true),
            ],
            &[(None, 0), (None, 1)],
            0,
            1,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("audit b"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // c: entry_point 5 with hwm = 2.
        let (engine, meta, dir) = build_residue(
            "neg-c",
            &[spec(0, vec![vec![]], true), spec(0, vec![vec![]], true)],
            &[(None, 0), (None, 1)],
            5,
            0,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("audit c"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // Weakened invariant: entry_point 0 but max_level 3 !=
        // top_level(0) = 0.
        let (engine, meta, dir) = build_residue(
            "neg-weak",
            &[spec(0, vec![vec![]], true)],
            &[(None, 0)],
            0,
            3,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("audit c"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // d/e: directory entries 0 and 1 map the same (page, slot).
        let (engine, meta, dir) = build_residue(
            "neg-de",
            &[spec(0, vec![vec![]], true)],
            &[(None, 0), (None, 0)],
            0,
            0,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("audit d/e"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // Unsorted neighbors: node 0's level-0 list is [2, 1] (the apply
        // primitive is pure application — no ordering check — so the
        // residue carries the violation the audit must catch).
        let (engine, meta, dir) = build_residue(
            "neg-unsorted",
            &[
                spec(0, vec![vec![2, 1]], true),
                spec(0, vec![vec![]], true),
                spec(0, vec![vec![]], true),
            ],
            &[(None, 0), (None, 1), (None, 2)],
            0,
            0,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("strictly ascending"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);

        // Self-loop: node 0's level-0 list is [0].
        let (engine, meta, dir) = build_residue(
            "neg-selfloop",
            &[spec(0, vec![vec![0]], true)],
            &[(None, 0)],
            0,
            0,
        );
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("self-loop"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------
    // 2026-09-21, mainline final review: stored-vector validation (the
    // GraphAccess trait contract cites this audit as a finiteness
    // guarantor) + acceptance/scope pins for the weakened invariant and
    // the referenced-pages-only orphan scope. These tests build on the
    // LIVE pool (no crash/reopen) — the audit is a read-only
    // re-derivation, page state is page state.
    // -----------------------------------------------------------------

    /// Build a synthetic graph on the live pool (no crash): `specs[s]`
    /// becomes the entry at slot s of one fresh node page; the directory
    /// and meta get the given content. Returns `(engine, meta_page_id,
    /// node_page_id, dir)`.
    fn build_live(
        tag: &str,
        metric: Metric,
        specs: &[NodeSpec],
        dir_map: &[(Option<PageId>, u16)],
        entry_point: u32,
        max_level: u8,
    ) -> (StorageEngine, PageId, PageId, PathBuf) {
        let dir = fresh_dir(tag);
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            metric,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        let node_page_id = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_node_page(page);
            log_page_init(engine.wal_writer(), page_id, page).unwrap();
            page_id
        };
        {
            let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            for (i, spec) in specs.iter().enumerate() {
                let vector = vec![(i + 1) as f32; DIM as usize];
                let slot =
                    apply::append_node(page, i as u32, spec.top_level, GEO, &vector).unwrap();
                for (l, list) in spec.level_lists.iter().enumerate() {
                    apply::set_neighbors(page, slot, GEO, l as u8, list).unwrap();
                }
                if spec.live {
                    apply::publish_live(page, slot, DIM).unwrap();
                }
            }
        }
        {
            let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            for (i, (target, slot)) in dir_map.iter().enumerate() {
                apply::dir_append(page, i as u32, target.unwrap_or(node_page_id), *slot).unwrap();
            }
        }
        {
            let mut guard = engine.buffer_pool().pin_mut(index.meta_page_id()).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::apply_meta(page, entry_point, max_level);
        }
        (engine, index.meta_page_id(), node_page_id, dir)
    }

    /// Bit-rot injection: overwrite one f32 component of the entry at
    /// `(page_id, slot)` with raw bits. The LP is decoded through
    /// pg-storage's layout owner (`decode_line_pointer`), never
    /// re-derived here.
    fn corrupt_vector_component(
        engine: &StorageEngine,
        page_id: PageId,
        slot: u16,
        component: usize,
        bits: u32,
    ) {
        use pg_storage::page::{LINE_POINTER_SIZE, PAGE_HEADER_SIZE};
        let mut guard = engine.buffer_pool().pin_mut(page_id).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        let lp_at = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
        let (off, _len, _) = pg_storage::page::decode_line_pointer(
            page[lp_at..lp_at + LINE_POINTER_SIZE].try_into().unwrap(),
        );
        let at = off as usize + component * 4;
        page[at..at + 4].copy_from_slice(&bits.to_le_bytes());
    }

    /// A bit-rotted NaN component in a stored vector must fail the audit
    /// loudly — the page carries no checksum, and the search path's
    /// `Cand::cmp` panics on NaN (the finiteness premise this audit backs).
    #[test]
    fn audit_rejects_nonfinite_stored_vector() {
        let (engine, meta, node_page, dir) = build_live(
            "nonfinite",
            Metric::L2,
            &[spec(0, vec![vec![]], true)],
            &[(None, 0)],
            0,
            0,
        );
        corrupt_vector_component(&engine, node_page, 0, 3, f32::NAN.to_bits());
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("non-finite"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An all-zero stored vector under the cosine metric must fail the
    /// audit (the §5 zero-vector rule mirrored on the stored side).
    #[test]
    fn audit_rejects_cosine_zero_stored_vector() {
        let dir = fresh_dir("cos-zero");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            Metric::Cosine,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        let node_page_id = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_node_page(page);
            log_page_init(engine.wal_writer(), page_id, page).unwrap();
            page_id
        };
        {
            let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            // The apply primitive is pure application — the §5 zero-vector
            // rejection lives in the funnel/entry layers — so a zero
            // vector lands here, standing in for bit-rot.
            let slot = apply::append_node(page, 0, 0, GEO, &vec![0.0; DIM as usize]).unwrap();
            apply::publish_live(page, slot, DIM).unwrap();
        }
        {
            let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::dir_append(page, 0, node_page_id, 0).unwrap();
        }
        {
            let mut guard = engine.buffer_pool().pin_mut(index.meta_page_id()).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::apply_meta(page, 0, 0);
        }
        let err = audit_index(engine.buffer_pool(), index.meta_page_id()).unwrap_err();
        assert!(err.to_string().contains("all-zero"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The weakened §8.3① invariant's ACCEPTANCE side (2026-09-21,
    /// mainline final review — previously only the rejection side was
    /// pinned): a legal §8.2 step 6–7 residue — node 1 fully connected
    /// but hidden (top_level 1 > max_level 0, its level-1 list empty) —
    /// audits clean and is counted, never rejected.
    #[test]
    fn audit_counts_hidden_high_level_nodes() {
        let (engine, meta, _node_page, dir) = build_live(
            "hidden",
            Metric::L2,
            &[
                spec(0, vec![vec![1]], true),          // node 0: entry, LIVE
                spec(1, vec![vec![0], vec![]], false), // node 1: hidden INITIALIZING
            ],
            &[(None, 0), (None, 1)],
            0,
            0,
        );
        let report = audit_index(engine.buffer_pool(), meta).unwrap();
        assert_eq!(report.node_count, 2);
        assert_eq!(report.hidden_high_level_count, 1);
        assert_eq!(report.initializing_count, 1);
        assert_eq!(report.live_count, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pins the documented orphan-scan scope (adversarial review P3-2):
    /// an orphan entry whose page NO directory entry references is
    /// invisible to the scan (referenced pages only) — a registered
    /// undercount, not a failure. Node 1 lives on a FRESH page that the
    /// directory never names.
    #[test]
    fn audit_orphan_scope_is_referenced_pages_only() {
        let dir = fresh_dir("orphan-scope");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        let node_page_a = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_node_page(page);
            log_page_init(engine.wal_writer(), page_id, page).unwrap();
            page_id
        };
        let node_page_b = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_node_page(page);
            log_page_init(engine.wal_writer(), page_id, page).unwrap();
            page_id
        };
        {
            // Page A: node 0, LIVE, mapped. Page B: one INITIALIZING
            // orphan the directory never names.
            let mut guard = engine.buffer_pool().pin_mut(node_page_a).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            let slot = apply::append_node(page, 0, 0, GEO, &[1.0; DIM as usize]).unwrap();
            apply::publish_live(page, slot, DIM).unwrap();
        }
        {
            let mut guard = engine.buffer_pool().pin_mut(node_page_b).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            let _ = apply::append_node(page, 1, 0, GEO, &[2.0; DIM as usize]).unwrap();
        }
        {
            let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::dir_append(page, 0, node_page_a, 0).unwrap();
        }
        {
            let mut guard = engine.buffer_pool().pin_mut(index.meta_page_id()).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::apply_meta(page, 0, 0);
        }
        let report = audit_index(engine.buffer_pool(), index.meta_page_id()).unwrap();
        assert_eq!(report.node_count, 1);
        assert_eq!(
            report.orphan_entry_count, 0,
            "the orphan sits on an unreferenced page — the documented (registered) undercount"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `tombstoned ∧ ¬LIVE` is unreachable via any legal record stream
    /// (the Tombstone funnel requires LIVE; PublishLive only SETS the live
    /// bit), so the audit rejects it as Corrupted — only raw page
    /// corruption produces it (2026-09-21, mainline final review round 2).
    #[test]
    fn audit_rejects_tombstoned_but_not_live() {
        let (engine, meta, node_page, dir) = build_live(
            "tomb-not-live",
            Metric::L2,
            &[spec(0, vec![vec![]], false)], // INITIALIZING
            &[(None, 0)],
            0,
            0,
        );
        {
            // The apply primitive is pure application (the LIVE
            // precondition lives in the funnel), so the bit lands —
            // standing in for page corruption.
            let mut guard = engine.buffer_pool().pin_mut(node_page).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::apply_tombstone(page, 0, DIM).unwrap();
        }
        let err = audit_index(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("tombstoned but not LIVE"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------
    // M6 Stage 0: audit_reachability (M6 tech-selection §8.3 先导 — the
    // §14.1 hanging-node verdict's input).
    // -----------------------------------------------------------------

    /// Empty graph: an all-zero report (no entry point to start from is
    /// the empty shape, not an error).
    #[test]
    fn reachability_empty_graph() {
        let dir = fresh_dir("reach-empty");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        let report = audit_reachability(engine.buffer_pool(), index.meta_page_id()).unwrap();
        assert_eq!(
            report,
            ReachabilityReport {
                node_count: 0,
                reachable_count: 0,
                unreachable_live: vec![],
                unreachable_initializing_count: 0,
                unreachable_tombstoned_count: 0,
            }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Single node: the entry point reaches itself.
    #[test]
    fn reachability_single_node() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-single",
            Metric::L2,
            &[spec(0, vec![vec![]], true)],
            &[(None, 0)],
            0,
            0,
        );
        let report = audit_reachability(engine.buffer_pool(), meta).unwrap();
        assert_eq!(report.node_count, 1);
        assert_eq!(report.reachable_count, 1);
        assert!(report.unreachable_live.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Multi-level descend: node 1 is reached at level 1 (its top level),
    /// then seeds the level-0 pass; node 2 is level-0-only and reached
    /// through node 0's level-0 list. All reachable.
    #[test]
    fn reachability_multi_level_descend() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-multi",
            Metric::L2,
            &[
                spec(1, vec![vec![1, 2], vec![1]], true), // node 0: entry, top 1
                spec(1, vec![vec![0, 2], vec![0]], true), // node 1: top 1
                spec(0, vec![vec![0]], true),             // node 2: level-0 only
            ],
            &[(None, 0), (None, 1), (None, 2)],
            0,
            1,
        );
        let report = audit_reachability(engine.buffer_pool(), meta).unwrap();
        assert_eq!(report.node_count, 3);
        assert_eq!(report.reachable_count, 3);
        assert!(report.unreachable_live.is_empty());
        assert_eq!(report.unreachable_initializing_count, 0);
        assert_eq!(report.unreachable_tombstoned_count, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The hanging-node negative (§14.1's input): node 2 is LIVE but no
    /// inbound edge exists at any level — reported in unreachable_live,
    /// never rejected.
    #[test]
    fn reachability_reports_unreachable_live() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-hanging",
            Metric::L2,
            &[
                spec(1, vec![vec![1], vec![1]], true),
                spec(1, vec![vec![0], vec![0]], true),
                spec(0, vec![vec![]], true), // node 2: LIVE, cut off
            ],
            &[(None, 0), (None, 1), (None, 2)],
            0,
            1,
        );
        let report = audit_reachability(engine.buffer_pool(), meta).unwrap();
        assert_eq!(report.node_count, 3);
        assert_eq!(report.reachable_count, 2);
        assert_eq!(report.unreachable_live, vec![2]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Residue counting: unreachable INITIALIZING (crash window) and
    /// tombstoned (M6 delete residue) nodes are COUNTED, not listed in
    /// unreachable_live — they still route as traversal intermediates
    /// (state bits are not traversal predicates), so node 4 reached
    /// THROUGH the tombstoned node 3 is reachable.
    #[test]
    fn reachability_counts_unreachable_residue() {
        let (engine, meta, node_page, dir) = build_live(
            "reach-residue",
            Metric::L2,
            &[
                spec(0, vec![vec![3]], true),    // node 0: entry
                spec(0, vec![vec![]], true),     // node 1: LIVE, cut off
                spec(0, vec![vec![]], false),    // node 2: INITIALIZING, cut off
                spec(0, vec![vec![0, 4]], true), // node 3: tombstoned router
                spec(0, vec![vec![3]], true),    // node 4: reached via node 3
            ],
            &[(None, 0), (None, 1), (None, 2), (None, 3), (None, 4)],
            0,
            0,
        );
        {
            let mut guard = engine.buffer_pool().pin_mut(node_page).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::apply_tombstone(page, 3, DIM).unwrap();
        }
        let report = audit_reachability(engine.buffer_pool(), meta).unwrap();
        assert_eq!(report.node_count, 5);
        // 0, 3 (tombstoned routes), 4 (via 3) are reachable.
        assert_eq!(report.reachable_count, 3);
        assert_eq!(report.unreachable_live, vec![1]);
        assert_eq!(report.unreachable_initializing_count, 1);
        assert_eq!(report.unreachable_tombstoned_count, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Level-discipline violation fails stop: a level-1 edge to a
    /// level-0-only node would make the BFS mis-walk (assertion b's
    /// mirror).
    #[test]
    fn reachability_rejects_level_discipline_violation() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-badlevel",
            Metric::L2,
            &[
                spec(1, vec![vec![], vec![1]], true), // node 0: L1 edge to node 1
                spec(0, vec![vec![]], true),          // node 1: top_level 0
            ],
            &[(None, 0), (None, 1)],
            0,
            1,
        );
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("reachability b"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No valid BFS start (assertion c's shape) fails stop.
    #[test]
    fn reachability_rejects_entry_point_out_of_range() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-badentry",
            Metric::L2,
            &[spec(0, vec![vec![]], true)],
            &[(None, 0)],
            5,
            0,
        );
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("no BFS start"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Scan-cost sample on a real-insert graph (feeds Stage F's 24h
    /// acceptance timing budget — M6 coding-plan Stage 0). Prints the wall
    /// time; asserts no timing (flake-free), only shape sanity. Whether a
    /// healthy graph has unreachable LIVE nodes is NOT pinned here (later
    /// inserts' pruning can in principle evict every inbound edge) — the
    /// strict set assertions live in the synthetic tests above.
    #[test]
    fn reachability_scan_cost_sample() {
        let dir = fresh_dir("reach-cost");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let mut index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        let mut rng = crate::rng::Xoshiro256StarStar::new(0xDEC0DE);
        // N is a cost-SAMPLE, not a scale gate: the scan is linear in
        // nodes + edges, so 500 nodes calibrate the per-node figure
        // without making the debug suite pay the insert cost of more.
        const N: usize = 500;
        for _ in 0..N {
            let v: Vec<f32> = (0..DIM)
                .map(|_| (rng.next_u64() % 4096) as f32 / 64.0)
                .collect();
            index
                .insert(engine.buffer_pool(), engine.wal_writer(), &v)
                .unwrap();
        }
        let start = std::time::Instant::now();
        let report = audit_reachability(engine.buffer_pool(), index.meta_page_id()).unwrap();
        let elapsed = start.elapsed();
        eprintln!(
            "reachability scan cost: {N} nodes dim={DIM} -> {elapsed:?} ({:.2} µs/node); reachable {}/{}, unreachable_live {}, initializing {}, tombstoned {}",
            elapsed.as_secs_f64() * 1e6 / N as f64,
            report.reachable_count,
            report.node_count,
            report.unreachable_live.len(),
            report.unreachable_initializing_count,
            report.unreachable_tombstoned_count,
        );
        assert_eq!(report.node_count, N as u64);
        assert!(report.reachable_count > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The meta page-type arm: pointing the scan at a NODE page fails
    /// stop (review P3-2 — every loud arm gets one negative pin).
    #[test]
    fn reachability_rejects_non_meta_page() {
        let (engine, _meta, node_page, dir) = build_live(
            "reach-nonmeta",
            Metric::L2,
            &[spec(0, vec![vec![]], true)],
            &[(None, 0)],
            0,
            0,
        );
        let err = audit_reachability(engine.buffer_pool(), node_page).unwrap_err();
        assert!(err.to_string().contains("is not a meta page"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The f arm: a directory entry naming the META page (not a NODE
    /// page) fails stop (audit_index's neg-f shape, mirrored).
    #[test]
    fn reachability_rejects_non_node_target() {
        let (engine, meta, dir) =
            build_residue_with("reach-negf", &[spec(0, vec![vec![]], true)], 0, 0, |meta| {
                vec![(Some(meta), 0)]
            });
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("reachability f"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The a arm: a neighbor id past the high-water mark fails stop
    /// (before it can index `top_levels` out of bounds).
    #[test]
    fn reachability_rejects_neighbor_past_hwm() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-nega",
            Metric::L2,
            &[
                spec(0, vec![vec![5]], true), // node 0 references id 5 >= hwm = 2
                spec(0, vec![vec![]], true),
            ],
            &[(None, 0), (None, 1)],
            0,
            0,
        );
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("reachability a"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The tombstoned ∧ ¬LIVE arm: unreachable via any legal record
    /// stream (audit_index's loud combination, mirrored) — injected
    /// through the pure apply primitive, standing in for page corruption.
    #[test]
    fn reachability_rejects_tombstoned_not_live() {
        let (engine, meta, node_page, dir) = build_live(
            "reach-tomb",
            Metric::L2,
            &[spec(0, vec![vec![]], false)], // INITIALIZING
            &[(None, 0)],
            0,
            0,
        );
        {
            let mut guard = engine.buffer_pool().pin_mut(node_page).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            apply::apply_tombstone(page, 0, DIM).unwrap();
        }
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("tombstoned but not LIVE"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The empty-graph c mirror (review P3-1): an empty chain with a
    /// non-INVALID entry point / nonzero max_level is a corrupt meta —
    /// loud here, exactly as in audit_index.
    #[test]
    fn reachability_rejects_corrupt_empty_meta() {
        let (engine, meta, _node_page, dir) =
            build_live("reach-emptyc", Metric::L2, &[], &[], 5, 3);
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("reachability c"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The non-empty c mirror (review round 2): an entry point whose
    /// top_level disagrees with meta max_level is a corrupt meta — loud,
    /// not a silent BFS from the entry's actual top level.
    #[test]
    fn reachability_rejects_entry_point_level_mismatch() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-badc",
            Metric::L2,
            &[spec(0, vec![vec![]], true)], // node 0: top_level 0
            &[(None, 0)],
            0,
            1, // meta max_level 1 != top_level(entry) 0
        );
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("reachability c"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The l_max hard-bound mirror (review round 3): top_level 20 with
    /// m = 16 (l_max = 13) is unproducible by any level draw — corruption,
    /// loud. (Distinct from the legal hidden-high-level soft residue:
    /// top > max_level but ≤ l_max stays merely unrouted.)
    #[test]
    fn reachability_rejects_top_level_above_l_max() {
        let (engine, meta, _node_page, dir) = build_live(
            "reach-lmax",
            Metric::L2,
            &[spec(20, vec![vec![]; 21], true)],
            &[(None, 0)],
            0,
            20,
        );
        let err = audit_reachability(engine.buffer_pool(), meta).unwrap_err();
        assert!(err.to_string().contains("l_max"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
