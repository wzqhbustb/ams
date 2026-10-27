//! Crash-recovery redo handlers for the seven HNSW WAL record kinds
//! (121–127) — Phase 2 M5 Stage C slice 1 (tech-selection §4.2/§10.1/§10.2;
//! coding plan Stage C).
//!
//! `Engine::open` registers these handlers alongside heap/txn/btree
//! (pg-engine/src/engine.rs extension point, tech-selection §2). This module
//! closes the Stage B interim limitation named by the old skeleton: an
//! open-repair `HnswMetaUpdate` (123) in the replay window no longer
//! hard-fails redo as `UnknownRecord` — it replays through
//! [`MetaUpdateRedo`] like any other record.
//!
//! ## Handler shape (stage_spec §4.2)
//!
//! Every handler follows the same sequence:
//!
//! 0. **txn_id gate** (§11.3 assertion ④, in-stream side; 2026-09-21,
//!    Stage D slice 1 — closes the implementation gap the coding plan
//!    registered as "already in Stage C"): HNSW records are utility
//!    operations (§8.1) and must carry `txn_id = INVALID`. Rejected before
//!    the decode and any page touch (`require_utility_txn`).
//! 1. **Bounded decode** (pg-storage's `wal::record` decoders — malformed
//!    payloads fail loudly per §8.1); the two state-bit kinds (124/127)
//!    additionally decode through the record's `flags` version nibble.
//! 2. **Pin + type-check** the touched page (`pd_flags` must match the
//!    record's page kind).
//! 3. **pd_lsn guard**: if the page's stamped LSN is already ≥ the record
//!    LSN, the post-image is durable on the page and the handler returns
//!    `Ok(())` WITHOUT re-validating — replay is idempotent and convergent
//!    by construction, so an already-applied record is skipped, never
//!    re-checked against a state it already produced.
//! 4. **Funnel validation** through `crate::validate`: only the checks
//!    that are locally evaluable during recovery. Checks that need the
//!    index directory or cross-page resolution — referenced `node_id < HWM`,
//!    `entry_point < HWM`, PublishLive directory consistency, SetNeighbors
//!    owner directory consistency — cannot be evaluated by stateless redo
//!    handlers and are audit items for the Stage D open-time audit
//!    (stage_spec §10.1/§11.3; `validate.rs` freezes the same line). Moving
//!    that line is a protocol revision, not an implementation detail.
//! 5. **Apply** through the `crate::apply` primitives and stamp `pd_lsn`
//!    to `max(current, record.lsn)`.
//!
//! Handlers are pure page-state functions: no I/O beyond the buffer pool,
//! no allocator, no in-memory caches, no chain traversal — identical
//! bytes-in produce identical bytes-out (§11.2 N=3 idempotence is pinned by
//! the tests below).

use pg_storage::buffer_pool::{BufferPool, PageGuardMut};
use pg_storage::error::{Result, StorageError};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::recovery::{RedoContext, RedoHandler};
use pg_storage::types::{Lsn, PageId, TxnId, PAGE_SIZE};
use pg_storage::wal::record::{
    HnswDirAppendRecord, HnswDirLinkRecord, HnswMetaUpdateRecord, HnswNodeInitRecord,
    HnswNodeTombstoneRecord, HnswPublishLiveRecord, HnswSetNeighborsRecord, WalRecord,
    WalRecordType,
};

use crate::apply;
use crate::dir::DIR_ENTRIES_PER_PAGE;
use crate::error::HnswError;
use crate::meta;
use crate::node::NodeGeometry;
use crate::page::{
    dir_count, dir_next, dir_ordinal, page_type, PAGE_TYPE_DIR, PAGE_TYPE_META, PAGE_TYPE_NODE,
};
use crate::validate::{self, MetaView};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Recovery always opens the buffer pool before replay (Stage I reorder),
/// so a missing pool is a programming error rather than a recoverable
/// condition (pg-am-btree `require_pool` precedent).
fn require_pool<'a>(ctx: &RedoContext<'a>) -> Result<&'a BufferPool> {
    ctx.buffer_pool.ok_or_else(|| {
        StorageError::InvalidOperation(
            "hnsw redo requires a buffer pool in RedoContext".to_string(),
        )
    })
}

/// §11.3 assertion ④, the in-stream side (2026-09-21, Stage D slice 1 —
/// this gate closes an implementation gap, not a protocol change: the
/// coding plan registered ④ as "already in Stage C" but no redo-side
/// check ever landed). HNSW records are utility operations (§8.1) and
/// must carry `txn_id = INVALID`. The gate runs BEFORE the decode and any
/// page touch, so a transactional record can never drive a page mutation.
fn require_utility_txn(record: &WalRecord) -> Result<()> {
    if record.txn_id != TxnId::INVALID {
        return Err(StorageError::MetadataCorrupted(format!(
            "hnsw redo: HNSW record carries txn_id {:?} — index records are utility operations (§8.1) and must be non-transactional (§11.3 assertion ④)",
            record.txn_id
        )));
    }
    Ok(())
}

/// Map an HNSW-layer failure into a storage error for the redo dispatch
/// (pg-am-btree `btree_to_storage` precedent): a validation or application
/// failure during redo indicates on-disk inconsistency, not a routine
/// condition. `InvalidOperation` keeps its variant (page-full class —
/// reachable when a record targets a page whose free space the pre-image
/// FPI replay left shorter than the record implies); everything else is
/// metadata corruption.
fn to_storage(e: HnswError) -> StorageError {
    match e {
        HnswError::InvalidOperation(m) => StorageError::InvalidOperation(m),
        other => StorageError::MetadataCorrupted(format!("hnsw redo: {other}")),
    }
}

fn page_mut<'g>(guard: &'g mut PageGuardMut<'_>) -> &'g mut [u8; PAGE_SIZE] {
    guard
        .page_mut()
        .try_into()
        .expect("a buffer frame is exactly PAGE_SIZE")
}

/// The touched page's type tag must match the record's page kind — a
/// mismatch means the record names a page that is not what the record
/// claims (on-disk inconsistency; loud, never a misapplied post-image).
fn require_type(page: &[u8; PAGE_SIZE], expected: u16, what: &str) -> Result<()> {
    let actual = page_type(page);
    if actual != expected {
        return Err(StorageError::MetadataCorrupted(format!(
            "hnsw redo: {what} record targets a page of type {actual}, expected {expected}"
        )));
    }
    Ok(())
}

/// Read the meta page into the funnel's parameter view (`read_meta` runs
/// its own structural checks, including the page-type tag).
fn meta_view(pool: &BufferPool, meta_page_id: PageId) -> Result<MetaView> {
    let guard = pool.pin(meta_page_id)?;
    let page: &[u8; PAGE_SIZE] = guard
        .page()
        .try_into()
        .expect("a buffer frame is exactly PAGE_SIZE");
    let params = meta::read_meta(page).map_err(to_storage)?;
    Ok(MetaView::from(&params))
}

/// 2026-09-18, mainline review round 2 P2: `meta_view`'s read-pin would
/// DEADLOCK if a corrupt record named the SAME page as its meta_page_id
/// and its mutation target — the non-reentrant frame RwLock read-pin on
/// a page the handler already write-holds never returns (the P2-1 class
/// from the adversarial review, extended to the meta read). The meta page
/// is never a node page, so equality is definitionally corrupt; reject
/// BEFORE the pin. (The constructors allow meta_page_id == page_id — only
/// PageId::INVALID is rejected there — so redo must re-check.)
fn require_distinct_meta(meta_page_id: PageId, page_id: PageId, what: &str) -> Result<()> {
    if meta_page_id == page_id {
        return Err(StorageError::MetadataCorrupted(format!(
            "hnsw redo: {what} meta_page_id == page_id {page_id} (a page cannot be both meta and node)"
        )));
    }
    Ok(())
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`
/// (pg-am-btree `stamp_pd_lsn` precedent).
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let current = page_pd_lsn(page);
    if current < lsn {
        set_page_pd_lsn(page, lsn);
    }
}

/// The pd_lsn guard shared by every handler: `true` when the record is
/// already durable on the page and must be skipped WITHOUT re-validation.
fn already_applied(page: &[u8; PAGE_SIZE], lsn: Lsn) -> bool {
    page_pd_lsn(page) >= lsn
}

// ---------------------------------------------------------------------------
// 121 HnswNodeInit
// ---------------------------------------------------------------------------

/// 121 HnswNodeInit: validate the vector against the meta-page parameters
/// (dim / L_max / finiteness / Cosine zero-vector), then create or
/// idempotently overwrite the INITIALIZING entry at the record's slot
/// (slot-state ownership lives in `apply::apply_node_at`, Stage A review
/// P3-3: the funnel is value-parameterized, the page-side checks need the
/// pinned page).
pub struct NodeInitRedo;

impl RedoHandler for NodeInitRedo {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HnswNodeInit
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        require_utility_txn(record)?;
        let rec = HnswNodeInitRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.head.page_id)?;
        let page = page_mut(&mut guard);
        require_type(page, PAGE_TYPE_NODE, "HnswNodeInit")?;
        if already_applied(page, record.lsn) {
            return Ok(());
        }

        require_distinct_meta(rec.head.meta_page_id, rec.head.page_id, "HnswNodeInit")?;
        let view = meta_view(pool, rec.head.meta_page_id)?;
        validate::validate_node_init(&view, rec.dim(), rec.head.level, &rec.vector)
            .map_err(to_storage)?;

        let geometry = NodeGeometry {
            dim: view.dim,
            m: view.m,
            m_max0: view.m_max0,
        };
        apply::apply_node_at(
            page,
            rec.head.slot_id,
            rec.head.node_id,
            rec.head.level,
            geometry,
            &rec.vector,
        )
        .map_err(to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 122 HnswSetNeighbors
// ---------------------------------------------------------------------------

/// 122 HnswSetNeighbors: prove the entry exists and is initialised (the
/// NodeInit-precedes-SetNeighbors protocol premise — enforced loudly via
/// the read accessor), validate the content against the funnel (count /
/// capacity / level <= top_level / ascending / no self-loop), then rewrite
/// the level's neighbor list in place.
pub struct SetNeighborsRedo;

impl RedoHandler for SetNeighborsRedo {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HnswSetNeighbors
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        require_utility_txn(record)?;
        let rec = HnswSetNeighborsRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.head.page_id)?;
        let page = page_mut(&mut guard);
        require_type(page, PAGE_TYPE_NODE, "HnswSetNeighbors")?;
        if already_applied(page, record.lsn) {
            return Ok(());
        }

        require_distinct_meta(rec.head.meta_page_id, rec.head.page_id, "HnswSetNeighbors")?;
        let view = meta_view(pool, rec.head.meta_page_id)?;
        let entry_top_level =
            apply::entry_top_level(page, rec.head.slot_id, view.dim).map_err(to_storage)?;
        validate::validate_set_neighbors(
            &view,
            rec.head.node_id,
            rec.head.level,
            rec.count(),
            &rec.neighbors,
            entry_top_level,
        )
        .map_err(to_storage)?;

        let geometry = NodeGeometry {
            dim: view.dim,
            m: view.m,
            m_max0: view.m_max0,
        };
        apply::set_neighbors(
            page,
            rec.head.slot_id,
            geometry,
            rec.head.level,
            &rec.neighbors,
        )
        .map_err(to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 123 HnswMetaUpdate
// ---------------------------------------------------------------------------

/// 123 HnswMetaUpdate: write the entry-point / max-level post-image.
///
/// The `max_level == top_level(entry_point)` check is NOT redo-evaluable:
/// resolving the entry point to its node page requires the directory
/// chain, which stateless redo handlers must not traverse — it is a
/// Stage D open-time audit item (stage_spec §10.1/§11.3). The semantic
/// ceiling `max_level <= l_max(m)` IS evaluable here (m is
/// creation-pinned on the very page this record targets) and IS enforced
/// (2026-09-18, slice-1 acceptance: redo is the third writer of the
/// runtime pair — besides the write path and open-repair — and must not
/// apply what the other two reject, review round 6 P3).
pub struct MetaUpdateRedo;

impl RedoHandler for MetaUpdateRedo {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HnswMetaUpdate
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        require_utility_txn(record)?;
        let rec = HnswMetaUpdateRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.meta_page_id)?;
        let page = page_mut(&mut guard);
        require_type(page, PAGE_TYPE_META, "HnswMetaUpdate")?;
        if already_applied(page, record.lsn) {
            return Ok(());
        }
        // The pinned meta page is also the validation input: read_meta
        // re-runs its structural checks (a corrupt pre-image fails loudly
        // here instead of being mutated further), then the record's
        // max_level is checked against the semantic ceiling.
        let current = meta::read_meta(page).map_err(to_storage)?;
        let l_max = crate::rng::l_max(current.m);
        if rec.max_level > l_max {
            return Err(StorageError::MetadataCorrupted(format!(
                "hnsw redo: HnswMetaUpdate max_level {} > l_max(m = {}) = {l_max} (no level draw can produce it)",
                rec.max_level, current.m
            )));
        }
        apply::apply_meta(page, rec.entry_point, rec.max_level);
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 124 HnswNodeTombstone
// ---------------------------------------------------------------------------

/// 124 HnswNodeTombstone: gate `dim == meta.dim` BEFORE the bit flip (the
/// redo pre-apply gate of review round 6 P1 — the open-time audit cannot
/// recover historical payloads, so a wrong `dim` caught only there would
/// already have flipped a bit at the wrong offset), require the entry to be
/// LIVE, then set the tombstone bit.
pub struct TombstoneRedo;

impl RedoHandler for TombstoneRedo {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HnswNodeTombstone
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        require_utility_txn(record)?;
        let rec = HnswNodeTombstoneRecord::decode(&record.payload, record.flags)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page = page_mut(&mut guard);
        require_type(page, PAGE_TYPE_NODE, "HnswNodeTombstone")?;
        if already_applied(page, record.lsn) {
            return Ok(());
        }

        require_distinct_meta(rec.meta_page_id, rec.page_id, "HnswNodeTombstone")?;
        let view = meta_view(pool, rec.meta_page_id)?;
        validate::validate_state_dim(&view, rec.dim, "NodeTombstone").map_err(to_storage)?;
        if !apply::entry_is_live(page, rec.slot_id, rec.dim).map_err(to_storage)? {
            return Err(StorageError::MetadataCorrupted(
                "hnsw redo: HnswNodeTombstone target entry is not LIVE".to_string(),
            ));
        }
        apply::apply_tombstone(page, rec.slot_id, rec.dim).map_err(to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 125 HnswDirAppend
// ---------------------------------------------------------------------------

/// 125 HnswDirAppend: enforce the NodeInit-must-precede premise (the
/// referenced node slot must be occupied — the v1.11 discipline asserts
/// only the {INITIALIZING, LIVE} membership, never a single state value),
/// then append the mapping at the tail page's high-water mark. Byte-exact
/// idempotent replay (hwm / post-image comparison) lives inside
/// `apply::dir_append`.
pub struct DirAppendRedo;

impl RedoHandler for DirAppendRedo {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HnswDirAppend
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        require_utility_txn(record)?;
        let rec = HnswDirAppendRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.dir_tail_page)?;
        let page = page_mut(&mut guard);
        require_type(page, PAGE_TYPE_DIR, "HnswDirAppend")?;
        if already_applied(page, record.lsn) {
            return Ok(());
        }

        // 2026-09-18, adversarial review P2-1: a corrupt record naming the
        // directory tail as its own target would DEADLOCK here — the
        // non-reentrant frame RwLock read-pin on a page this handler
        // already write-holds never returns (probe-confirmed hang), and
        // the hang would precede require_type with zero diagnostics. The
        // equality is definitionally corrupt (a page is either DIR or
        // NODE, never both), so reject BEFORE the pin.
        if rec.target_page == rec.dir_tail_page {
            return Err(StorageError::MetadataCorrupted(
                "hnsw redo: HnswDirAppend target_page == dir_tail_page (a page cannot be both directory and node)"
                    .to_string(),
            ));
        }

        // The record carries no `meta_page_id`, so the existence proof is
        // physical, not geometric: the slot's line pointer must name a live
        // entry (any occupied state — NodeInit has run). An empty slot means
        // the record's premise is broken: fail loudly.
        {
            let node_guard = pool.pin(rec.target_page)?;
            let node_page: &[u8; PAGE_SIZE] = node_guard
                .page()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            require_type(node_page, PAGE_TYPE_NODE, "HnswDirAppend target")?;
            if !apply::slot_is_occupied(node_page, rec.target_slot) {
                return Err(StorageError::MetadataCorrupted(
                    "hnsw redo: HnswDirAppend target slot is empty — NodeInit must precede"
                        .to_string(),
                ));
            }
        }

        apply::dir_append(page, rec.node_id, rec.target_page, rec.target_slot)
            .map_err(to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 126 HnswDirLink
// ---------------------------------------------------------------------------

/// 126 HnswDirLink: the old tail's `next` must still be `INVALID`
/// (otherwise the record is stale — an already-linked tail is skipped by
/// the pd_lsn guard, so reaching here with a set `next` is loud), and the
/// new page must be a directory page whose ordinal is exactly the old
/// tail's `ordinal + 1`.
pub struct DirLinkRedo;

impl RedoHandler for DirLinkRedo {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HnswDirLink
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        require_utility_txn(record)?;
        let rec = HnswDirLinkRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.old_tail_page)?;
        let page = page_mut(&mut guard);
        require_type(page, PAGE_TYPE_DIR, "HnswDirLink")?;
        if already_applied(page, record.lsn) {
            return Ok(());
        }
        if dir_next(page) != PageId::INVALID {
            return Err(StorageError::MetadataCorrupted(
                "hnsw redo: HnswDirLink on a tail whose next pointer is already set".to_string(),
            ));
        }
        // 2026-09-18, adversarial review P2-1: same self-target deadlock
        // class as DirAppend — a corrupt self-link record would hang the
        // read pin below on the page this handler write-holds. The
        // constructor refuses self-links; redo decodes raw bytes, so the
        // handler must re-check BEFORE the pin.
        if rec.next_page == rec.old_tail_page {
            return Err(StorageError::MetadataCorrupted(
                "hnsw redo: HnswDirLink next_page == old_tail_page (self-link)".to_string(),
            ));
        }
        // 2026-09-18, review round 3 P3-2: link only a FULL tail. A corrupt
        // record linking a partial tail would build a chain that violates
        // the middle-pages-exactly-full invariant (check_dir_chain
        // assertion 2, dir.rs) at the next open. Same-page evaluable, so
        // the check lives in redo (tech-selection §10.1 DirLink item 4,
        // v1.35).
        if dir_count(page) != DIR_ENTRIES_PER_PAGE {
            return Err(StorageError::MetadataCorrupted(format!(
                "hnsw redo: HnswDirLink old tail has count {}, expected exactly {DIR_ENTRIES_PER_PAGE} (link only when the tail is full)",
                dir_count(page)
            )));
        }

        {
            let new_guard = pool.pin(rec.next_page)?;
            let new_page: &[u8; PAGE_SIZE] = new_guard
                .page()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            require_type(new_page, PAGE_TYPE_DIR, "HnswDirLink target")?;
            // 2026-09-18, slice-1 acceptance: checked_add — a bit-rotted
            // header could carry ordinal = u64::MAX; a wrapping/panicking
            // +1 violates the no-panic-on-corrupt-input discipline.
            let Some(expected) = dir_ordinal(page).checked_add(1) else {
                return Err(StorageError::MetadataCorrupted(
                    "hnsw redo: HnswDirLink old-tail ordinal u64::MAX cannot have a successor"
                        .to_string(),
                ));
            };
            if dir_ordinal(new_page) != expected {
                return Err(StorageError::MetadataCorrupted(format!(
                    "hnsw redo: HnswDirLink target ordinal {} != old-tail ordinal + 1 ({expected})",
                    dir_ordinal(new_page)
                )));
            }
        }

        apply::dir_link(page, rec.next_page);
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 127 HnswPublishLive
// ---------------------------------------------------------------------------

/// 127 HnswPublishLive: same pre-apply dim gate as the tombstone (review
/// round 6 P1), prove the entry exists (NodeInit precedes PublishLive —
/// the read is loud on an empty slot), then flip the state bit to LIVE.
pub struct PublishLiveRedo;

impl RedoHandler for PublishLiveRedo {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HnswPublishLive
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        require_utility_txn(record)?;
        let rec = HnswPublishLiveRecord::decode(&record.payload, record.flags)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page = page_mut(&mut guard);
        require_type(page, PAGE_TYPE_NODE, "HnswPublishLive")?;
        if already_applied(page, record.lsn) {
            return Ok(());
        }

        require_distinct_meta(rec.meta_page_id, rec.page_id, "HnswPublishLive")?;
        let view = meta_view(pool, rec.meta_page_id)?;
        validate::validate_state_dim(&view, rec.dim, "PublishLive").map_err(to_storage)?;
        // Existence proof: the read fails loudly on an empty slot.
        apply::entry_top_level(page, rec.slot_id, rec.dim).map_err(to_storage)?;
        apply::publish_live(page, rec.slot_id, rec.dim).map_err(to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// All seven HNSW redo handlers (121–127), registered by `Engine::open`
/// alongside heap/txn/btree (tech-selection §2 extension point).
pub fn hnsw_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    vec![
        Box::new(NodeInitRedo),
        Box::new(SetNeighborsRedo),
        Box::new(MetaUpdateRedo),
        Box::new(TombstoneRedo),
        Box::new(DirAppendRedo),
        Box::new(DirLinkRedo),
        Box::new(PublishLiveRedo),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::dir_entry;
    use crate::graph::{Metric, NeighborSelection};
    use crate::index::create;
    use crate::page::{dir_count, init_dir_page, init_node_page, log_page_init};
    use crate::params::HnswParams;
    use pg_storage::clog::NoOpClogAccessor;
    use pg_storage::config::StorageConfig;
    use pg_storage::engine::StorageEngine;
    use pg_storage::recovery::{ActiveXactTable, DirtyPageTable, IncompleteSplitTracker};
    use std::collections::BTreeMap;
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

    /// Manual temp dir (no tempfile dev-dependency — the M4 dependency
    /// freeze; index.rs/page_init tests use the same pattern).
    fn fresh_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pg_am_hnsw_m5_redo-{}-{}-{tag}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Create a fresh index plus one initialised node page; return the
    /// locators and the node page's init-FPI LSN (the highest pd_lsn stamp
    /// in the environment — hand-built records must sit above it). Every
    /// page is made durable by its own post-image FPI (`log_page_init`, the
    /// A1 contract) before the logical records append.
    fn create_env(engine: &StorageEngine) -> (PageId, PageId, PageId, Lsn) {
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
        let (node_page_id, node_init_lsn) = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_node_page(page);
            let lsn = log_page_init(engine.wal_writer(), page_id, page).unwrap();
            (page_id, lsn)
        };
        (
            index.meta_page_id(),
            index.dir_head(),
            node_page_id,
            node_init_lsn,
        )
    }

    /// The standard §8.1 five-record insert sequence for node 0
    /// (`(node_page, slot 0)`, top_level 1, level-0 neighbors {2,5,9},
    /// entry_point 0 / max_level 1) — WAL-only; the pages are NOT touched,
    /// so crash-recovery replay is the only way the state can appear.
    fn append_standard_five(engine: &StorageEngine, meta: PageId, dir_head: PageId, node: PageId) {
        let wal = engine.wal_writer();
        wal.append(
            WalRecord::hnsw_node_init(meta, node, 0, 0, 1, DIM, vec![1.0; DIM as usize]).unwrap(),
        )
        .unwrap();
        wal.append(WalRecord::hnsw_dir_append(dir_head, 0, node, 0).unwrap())
            .unwrap();
        wal.append(WalRecord::hnsw_set_neighbors(meta, node, 0, 0, 0, vec![2, 5, 9]).unwrap())
            .unwrap();
        wal.append(WalRecord::hnsw_meta_update(meta, 0, 1).unwrap())
            .unwrap();
        wal.append(WalRecord::hnsw_publish_live(meta, node, 0, 0, DIM).unwrap())
            .unwrap();
        wal.flush().unwrap();
    }

    fn assert_replayed_state(pool: &BufferPool, meta: PageId, dir_head: PageId, node: PageId) {
        // Directory: exactly one entry, node 0 → (node_page, slot 0).
        {
            let guard = pool.pin(dir_head).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            assert_eq!(dir_count(page), 1);
            assert_eq!(dir_entry(page, 0).unwrap(), (node, 0));
        }
        // Node entry: LIVE, top_level 1, level-0 neighbors {2,5,9}.
        {
            let guard = pool.pin(node).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            assert!(apply::entry_is_live(page, 0, DIM).unwrap());
            assert_eq!(apply::entry_top_level(page, 0, DIM).unwrap(), 1);
            assert_eq!(
                apply::entry_neighbors(page, 0, GEO, 0).unwrap(),
                vec![2, 5, 9]
            );
        }
        // Meta: entry_point 0, max_level 1.
        {
            let guard = pool.pin(meta).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            let params = meta::read_meta(page).unwrap();
            assert_eq!(params.entry_point, 0);
            assert_eq!(params.max_level, 1);
        }
    }

    /// The Stage C slice-1 acceptance test: crash-reopen an index whose WAL
    /// holds one full HNSW insert sequence. Before this slice the reopen
    /// hard-failed as `UnknownRecord`; now the seven handlers must replay
    /// the records into exactly the state the writer would have produced.
    #[test]
    fn reopen_replays_hnsw_records() {
        let dir = fresh_dir("reopen");
        let config = StorageConfig::new(&dir);
        let (meta, dir_head, node) = {
            let engine = StorageEngine::open(&dir, &config).unwrap();
            let ids = create_env(&engine);
            append_standard_five(&engine, ids.0, ids.1, ids.2);
            std::mem::forget(engine); // crash: no checkpoint, dirty pool dropped
            (ids.0, ids.1, ids.2)
        };

        let engine =
            StorageEngine::open_with_redo_handlers(&dir, &config, hnsw_redo_handlers(), vec![])
                .expect("reopen must replay 121–127 instead of failing UnknownRecord");
        assert_replayed_state(engine.buffer_pool(), meta, dir_head, node);
    }

    /// Hand-constructed redo context (pg-am-btree redo Harness precedent).
    struct CtxParts {
        clog: NoOpClogAccessor,
        att: ActiveXactTable,
        dpt: DirtyPageTable,
        splits: IncompleteSplitTracker,
    }

    impl CtxParts {
        fn new() -> Self {
            Self {
                clog: NoOpClogAccessor,
                att: ActiveXactTable::new(),
                dpt: DirtyPageTable::new(),
                splits: IncompleteSplitTracker::new(),
            }
        }
    }

    /// §11.2: every handler applied N=3 times to the same record yields
    /// byte-identical pages after each application (the pd_lsn guard makes
    /// applications 2–3 pure no-ops).
    #[test]
    fn handlers_replay_idempotently_n3() {
        let dir = fresh_dir("idem");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let (meta, dir_head, node, node_init_lsn) = create_env(&engine);
        // A second directory page for the DirLink case (ordinal 1).
        let (dir_page_1, dir_1_init_lsn) = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_dir_page(page, 1);
            let lsn = log_page_init(engine.wal_writer(), page_id, page).unwrap();
            (page_id, lsn)
        };

        // All seven record kinds, LSNs ascending and above every init FPI
        // (WAL LSNs are byte offsets — an 8 KiB post-image FPI pushes them
        // far past small round numbers, so anchor on the real init LSNs).
        let mut lsn = node_init_lsn.0.max(dir_1_init_lsn.0);
        let mut next_lsn = || {
            lsn += 1;
            Lsn(lsn)
        };
        let mut records: Vec<(WalRecord, PageId)> = vec![
            (
                WalRecord::hnsw_node_init(meta, node, 0, 0, 1, DIM, vec![1.0; DIM as usize])
                    .unwrap(),
                node,
            ),
            (
                WalRecord::hnsw_dir_append(dir_head, 0, node, 0).unwrap(),
                dir_head,
            ),
            (
                WalRecord::hnsw_set_neighbors(meta, node, 0, 0, 0, vec![2, 5, 9]).unwrap(),
                node,
            ),
            (WalRecord::hnsw_meta_update(meta, 0, 1).unwrap(), meta),
            (
                WalRecord::hnsw_publish_live(meta, node, 0, 0, DIM).unwrap(),
                node,
            ),
            (
                WalRecord::hnsw_node_tombstone(meta, node, 0, 0, DIM).unwrap(),
                node,
            ),
            (
                WalRecord::hnsw_dir_link(dir_head, dir_page_1).unwrap(),
                dir_head,
            ),
        ];
        for (rec, _) in &mut records {
            rec.lsn = next_lsn();
        }

        let handlers = hnsw_redo_handlers();
        let mut parts = CtxParts::new();
        for (record, target_page) in &records {
            // 2026-09-18, review round 3 P3-2: DirLink redo now requires a
            // FULL old tail — the DirAppend above left count = 1, so pin
            // the tail's count field to capacity by hand before the link
            // (test scaffolding on the header field only; the 813 entries
            // themselves are never read by dir_link).
            if record.record_type == WalRecordType::HnswDirLink {
                let mut guard = engine.buffer_pool().pin_mut(dir_head).unwrap();
                let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
                page[crate::page::DIR_OFF_COUNT..crate::page::DIR_OFF_COUNT + 4]
                    .copy_from_slice(&DIR_ENTRIES_PER_PAGE.to_le_bytes());
            }
            let handler = handlers
                .iter()
                .find(|h| h.kind() == record.record_type)
                .expect("handler registered");
            let mut snapshot: Option<Vec<u8>> = None;
            for round in 0..3 {
                {
                    let mut ctx = RedoContext {
                        buffer_pool: Some(engine.buffer_pool()),
                        page_allocator: engine.page_allocator(),
                        clog: &parts.clog,
                        att: &mut parts.att,
                        dpt: &mut parts.dpt,
                        incomplete_splits: &mut parts.splits,
                    };
                    handler.apply(record, &mut ctx).expect("apply");
                }
                let bytes = {
                    let guard = engine.buffer_pool().pin(*target_page).unwrap();
                    guard.page().to_vec()
                };
                match &snapshot {
                    None => snapshot = Some(bytes),
                    Some(expected) => assert_eq!(
                        expected, &bytes,
                        "kind {:?} round {round} diverged",
                        record.record_type
                    ),
                }
            }
        }

        // The full sequence also lands the expected semantic state.
        assert!(apply::entry_is_tombstoned(
            engine
                .buffer_pool()
                .pin(node)
                .unwrap()
                .page()
                .try_into()
                .unwrap(),
            0,
            DIM
        )
        .unwrap());
        let guard = engine.buffer_pool().pin(dir_head).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(dir_next(page), dir_page_1);
    }

    /// §4.2: identical WALs replay to byte-identical pages. Runs the
    /// standard five-record sequence (and a three-record prefix of it) in
    /// two independent directories each and compares the full page bytes.
    #[test]
    fn replay_prefix_is_deterministic() {
        fn run(record_count: usize) -> BTreeMap<PageId, Vec<u8>> {
            let dir = fresh_dir(&format!("det{record_count}"));
            let config = StorageConfig::new(&dir);
            let (meta, dir_head, node) = {
                let engine = StorageEngine::open(&dir, &config).unwrap();
                let ids = create_env(&engine);
                let wal = engine.wal_writer();
                let mut records = vec![
                    WalRecord::hnsw_node_init(ids.0, ids.2, 0, 0, 1, DIM, vec![1.0; DIM as usize])
                        .unwrap(),
                    WalRecord::hnsw_dir_append(ids.1, 0, ids.2, 0).unwrap(),
                    WalRecord::hnsw_set_neighbors(ids.0, ids.2, 0, 0, 0, vec![2, 5, 9]).unwrap(),
                    WalRecord::hnsw_meta_update(ids.0, 0, 1).unwrap(),
                    WalRecord::hnsw_publish_live(ids.0, ids.2, 0, 0, DIM).unwrap(),
                ];
                records.truncate(record_count);
                for rec in records {
                    wal.append(rec).unwrap();
                }
                wal.flush().unwrap();
                std::mem::forget(engine); // crash: no checkpoint
                (ids.0, ids.1, ids.2)
            };
            let engine =
                StorageEngine::open_with_redo_handlers(&dir, &config, hnsw_redo_handlers(), vec![])
                    .unwrap();
            let mut state = BTreeMap::new();
            for page_id in [meta, dir_head, node] {
                let guard = engine.buffer_pool().pin(page_id).unwrap();
                state.insert(page_id, guard.page().to_vec());
            }
            state
        }

        assert_eq!(
            run(3),
            run(3),
            "3-record prefix replay must be deterministic"
        );
        assert_eq!(run(5), run(5), "5-record replay must be deterministic");
    }

    /// 2026-09-18, slice-1 acceptance: handler-level negative matrix — one
    /// loud rejection per handler, the two adversarial-review P2-1
    /// self-target deadlock negatives, the four mainline-round-2
    /// meta_page_id == page_id deadlock negatives (meta_view's read pin
    /// under the held write guard — same class), and the two
    /// review-round-4 pins for the branches the dim gate would otherwise
    /// always mask (124's LIVE requirement with a CORRECT dim on an
    /// INITIALIZING entry; 127's existence proof with a CORRECT dim on an
    /// empty slot), each proving the decode →
    /// validate → apply wiring rejects BEFORE mutating the page (page
    /// bytes pinned unchanged across the failed apply). The funnel's value-level
    /// negative matrix lives in validate.rs; every case here is
    /// constructible through the public record constructors (the classes
    /// the constructors themselves reject — unsorted/duplicate/self-loop
    /// neighbors, level > 63, INVALID ids — are pinned in pg-storage's
    /// record tests and need no handler-side re-proof).
    #[test]
    fn handlers_reject_bad_records_without_mutating() {
        let dir = fresh_dir("neg");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let (meta, dir_head, node, node_init_lsn) = create_env(&engine);
        // A second directory page (ordinal 1) for the DirLink cases.
        let (dir_page_1, dir_1_init_lsn) = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_dir_page(page, 1);
            let lsn = log_page_init(engine.wal_writer(), page_id, page).unwrap();
            (page_id, lsn)
        };

        let handlers = hnsw_redo_handlers();
        let mut parts = CtxParts::new();
        let mut lsn = node_init_lsn.0.max(dir_1_init_lsn.0);
        let mut next_lsn = || {
            lsn += 1;
            Lsn(lsn)
        };
        let mut apply_one = |rec: &mut WalRecord, parts: &mut CtxParts| {
            rec.lsn = next_lsn();
            let handler = handlers
                .iter()
                .find(|h| h.kind() == rec.record_type)
                .expect("handler registered");
            let mut ctx = RedoContext {
                buffer_pool: Some(engine.buffer_pool()),
                page_allocator: engine.page_allocator(),
                clog: &parts.clog,
                att: &mut parts.att,
                dpt: &mut parts.dpt,
                incomplete_splits: &mut parts.splits,
            };
            handler.apply(rec, &mut ctx)
        };
        let page_bytes = |page_id: PageId| {
            let guard = engine.buffer_pool().pin(page_id).unwrap();
            guard.page().to_vec()
        };

        // One good NodeInit (the 122/124/127 negatives need the node entry
        // to exist) and one good DirLink (the already-linked negative needs
        // the old tail linked).
        apply_one(
            &mut WalRecord::hnsw_node_init(meta, node, 0, 0, 1, DIM, vec![1.0; DIM as usize])
                .unwrap(),
            &mut parts,
        )
        .expect("good NodeInit applies");
        // 2026-09-18, review round 3 P3-2: the good DirLink now needs a
        // FULL old tail — pin dir_head's count to capacity by hand first
        // (header-field scaffolding only; see the idempotency test).
        {
            let mut guard = engine.buffer_pool().pin_mut(dir_head).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            page[crate::page::DIR_OFF_COUNT..crate::page::DIR_OFF_COUNT + 4]
                .copy_from_slice(&DIR_ENTRIES_PER_PAGE.to_le_bytes());
        }
        apply_one(
            &mut WalRecord::hnsw_dir_link(dir_head, dir_page_1).unwrap(),
            &mut parts,
        )
        .expect("good DirLink applies");

        // A self-link DirLink record targeting the (still unlinked) second
        // directory page — the constructor refuses next == old_tail, so
        // the raw payload is crafted from a valid record instead: two
        // one-byte varints (valid while page ids < 251, always true in
        // tests; a wire-format drift fails the fragment assertion below
        // LOUDLY, never silently).
        let self_link_err = WalRecord::hnsw_dir_link(dir_page_1, dir_page_1)
            .unwrap_err()
            .to_string();
        assert!(self_link_err.contains("itself"), "{self_link_err}"); // constructor pin
        let mut self_link = WalRecord::hnsw_dir_link(dir_page_1, dir_head).unwrap();
        let b1 = u8::try_from(dir_page_1.0).unwrap();
        self_link.payload = vec![b1, b1];

        // (record, target page, expected error fragment or "")
        let cases: Vec<(WalRecord, PageId, &str)> = vec![
            // 121: dim != meta.dim (funnel dim consistency).
            (
                WalRecord::hnsw_node_init(meta, node, 0, 1, 1, 64, vec![1.0; 64]).unwrap(),
                node,
                "",
            ),
            // 122: count 33 exceeds the level-0 capacity m_max0 = 32
            // (constructor-legal: ascending, no self-loop, fits u16).
            (
                WalRecord::hnsw_set_neighbors(meta, node, 0, 0, 0, (1..=33u32).collect()).unwrap(),
                node,
                "",
            ),
            // 123: max_level 14 > l_max(m = 16) = 13 — constructor-legal
            // (<= 63), rejected by the redo-side semantic ceiling.
            (
                WalRecord::hnsw_meta_update(meta, 0, 14).unwrap(),
                meta,
                "l_max",
            ),
            // 124: dim != meta.dim — the redo pre-apply gate fires before
            // the LIVE check.
            (
                WalRecord::hnsw_node_tombstone(meta, node, 0, 0, 64).unwrap(),
                node,
                "",
            ),
            // 124 (review round 4 nano 1): CORRECT dim, but the target
            // entry is not LIVE — slot 0 is INITIALIZING (the good
            // NodeInit above is never published in this test), so the dim
            // gate passes and the LIVE requirement itself is what fires.
            (
                WalRecord::hnsw_node_tombstone(meta, node, 0, 0, DIM).unwrap(),
                node,
                "not LIVE",
            ),
            // 125: target slot 7 is empty — the NodeInit-precedes premise
            // is broken (node_id 0 == the chain hwm 0, so the occupancy
            // check is what fires, not dir_append's gap guard).
            (
                WalRecord::hnsw_dir_append(dir_head, 0, node, 7).unwrap(),
                dir_head,
                "NodeInit must precede",
            ),
            // 126: the old tail is already linked (good DirLink above).
            (
                WalRecord::hnsw_dir_link(dir_head, dir_page_1).unwrap(),
                dir_head,
                "already set",
            ),
            // 125 (adversarial review P2-1): target_page == dir_tail_page —
            // must fail loudly BEFORE the read pin deadlocks on the
            // write-held frame (a hang here would time out the test, which
            // is the loudest possible failure).
            (
                WalRecord::hnsw_dir_append(dir_head, 0, dir_head, 0).unwrap(),
                dir_head,
                "cannot be both",
            ),
            // 126 (adversarial review P2-1): self-link on the unlinked
            // second directory page — same deadlock class, rejected before
            // the pin (crafted payload; the constructor refuses self-links).
            (self_link, dir_page_1, "self-link"),
            // 126 (review round 3 P3-2): link a PARTIAL tail — the
            // middle-pages-exactly-full chain invariant (dir.rs) would be
            // violated at the next open.
            (
                WalRecord::hnsw_dir_link(dir_page_1, dir_head).unwrap(),
                dir_page_1,
                "tail is full",
            ),
            // 127: dim != meta.dim — same pre-apply gate as 124.
            (
                WalRecord::hnsw_publish_live(meta, node, 0, 0, 64).unwrap(),
                node,
                "",
            ),
            // 127 (review round 4 nano 1): CORRECT dim, but slot 7 is
            // EMPTY (the 125 negative above was rejected, leaving it
            // untouched) — the dim gate passes and the existence proof
            // (entry_top_level's loud read) fires before any state-bit
            // write.
            (
                WalRecord::hnsw_publish_live(meta, node, 7, 0, DIM).unwrap(),
                node,
                "not a live entry",
            ),
            // Mainline review round 2 P2 (2026-09-18): meta_page_id ==
            // page_id on the four meta-reading handlers — the P2-1
            // deadlock class via meta_view's read pin under the held
            // write guard. Constructor-legal (only PageId::INVALID is
            // rejected there), so redo must re-check.
            (
                WalRecord::hnsw_node_init(node, node, 0, 1, 1, DIM, vec![1.0; DIM as usize])
                    .unwrap(),
                node,
                "both meta and node",
            ),
            (
                WalRecord::hnsw_set_neighbors(node, node, 0, 0, 0, vec![2, 5, 9]).unwrap(),
                node,
                "both meta and node",
            ),
            (
                WalRecord::hnsw_node_tombstone(node, node, 0, 0, DIM).unwrap(),
                node,
                "both meta and node",
            ),
            (
                WalRecord::hnsw_publish_live(node, node, 0, 0, DIM).unwrap(),
                node,
                "both meta and node",
            ),
        ];
        for (mut rec, target, fragment) in cases {
            let before = page_bytes(target);
            let err = apply_one(&mut rec, &mut parts);
            let Err(err) = err else {
                panic!("{:?} bad record must be rejected", rec.record_type);
            };
            assert!(
                fragment.is_empty() || err.to_string().contains(fragment),
                "{:?}: error must name the violated invariant ({fragment}): {err}",
                rec.record_type
            );
            assert_eq!(
                page_bytes(target),
                before,
                "{:?}: rejection must precede any page mutation",
                rec.record_type
            );
        }
    }

    /// 2026-09-21, Stage D slice 1 (§11.3 assertion ④, the in-stream side
    /// — closes the coding-plan's falsely registered "already in Stage C"
    /// gap): EVERY handler rejects a record whose `txn_id` is not INVALID,
    /// before the decode and any page touch (target page bytes pinned
    /// unchanged across the failed apply).
    #[test]
    fn handlers_reject_transactional_records() {
        let dir = fresh_dir("txn-gate");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let (meta, dir_head, node, node_init_lsn) = create_env(&engine);
        let (dir_page_1, dir_1_init_lsn) = {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let page_id = guard.page_id();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init_dir_page(page, 1);
            let lsn = log_page_init(engine.wal_writer(), page_id, page).unwrap();
            (page_id, lsn)
        };

        let mut lsn = node_init_lsn.0.max(dir_1_init_lsn.0);
        let mut next_lsn = || {
            lsn += 1;
            Lsn(lsn)
        };
        let mut records: Vec<(WalRecord, PageId)> = vec![
            (
                WalRecord::hnsw_node_init(meta, node, 0, 0, 1, DIM, vec![1.0; DIM as usize])
                    .unwrap(),
                node,
            ),
            (
                WalRecord::hnsw_set_neighbors(meta, node, 0, 0, 0, vec![2, 5, 9]).unwrap(),
                node,
            ),
            (WalRecord::hnsw_meta_update(meta, 0, 1).unwrap(), meta),
            (
                WalRecord::hnsw_node_tombstone(meta, node, 0, 0, DIM).unwrap(),
                node,
            ),
            (
                WalRecord::hnsw_dir_append(dir_head, 0, node, 0).unwrap(),
                dir_head,
            ),
            (
                WalRecord::hnsw_dir_link(dir_head, dir_page_1).unwrap(),
                dir_head,
            ),
            (
                WalRecord::hnsw_publish_live(meta, node, 0, 0, DIM).unwrap(),
                node,
            ),
        ];
        for (rec, _) in &mut records {
            rec.lsn = next_lsn();
            rec.txn_id = TxnId(7); // a transactional record — the ④ violation
        }

        let handlers = hnsw_redo_handlers();
        let mut parts = CtxParts::new();
        for (record, target_page) in &records {
            let before = {
                let guard = engine.buffer_pool().pin(*target_page).unwrap();
                guard.page().to_vec()
            };
            let handler = handlers
                .iter()
                .find(|h| h.kind() == record.record_type)
                .expect("handler registered");
            let mut ctx = RedoContext {
                buffer_pool: Some(engine.buffer_pool()),
                page_allocator: engine.page_allocator(),
                clog: &parts.clog,
                att: &mut parts.att,
                dpt: &mut parts.dpt,
                incomplete_splits: &mut parts.splits,
            };
            let err = handler
                .apply(record, &mut ctx)
                .expect_err("a transactional record must be rejected");
            assert!(err.to_string().contains("txn_id"), "{err}");
            assert!(err.to_string().contains("hnsw redo"), "{err}");
            let after = {
                let guard = engine.buffer_pool().pin(*target_page).unwrap();
                guard.page().to_vec()
            };
            assert_eq!(
                before, after,
                "{:?}: the ④ gate must fire before any page mutation",
                record.record_type
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
