//! HNSW index lifecycle: creation and open — Phase 2 M5 Stage B slice 3a
//! (tech-selection §10.3 creation/open protocol, §7.2 creation geometry,
//! §5 rng skip-ahead).
//!
//! [`HnswIndex`] is the handle pg-engine (slice 3b) and the Stage C write
//! path consume: it carries the meta-page locator, the directory chain
//! head, the validated [`crate::meta::MetaParams`], the chain-derived
//! high-water mark, and the graph's level-draw PRNG (with its lineage
//! preserved across restarts — see the `open` function below).
//!
//! Creation is a UTILITY operation (§8.1: no transactional DML; index
//! records carry `txn_id = INVALID` and there is no abort window).

use pg_storage::buffer_pool::BufferPool;
use pg_storage::types::{Lsn, PageId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;

use crate::error::Result;
use crate::graph::{Metric, NeighborSelection};
pub use crate::meta::{ExpectedParams, MetaParams};
use crate::params::{HnswParams, NodeId};
use crate::rng::Xoshiro256StarStar;

/// A live handle on one HNSW index (meta page + directory chain + runtime
/// graph state).
pub struct HnswIndex {
    meta_page_id: PageId,
    dir_head: PageId,
    params: MetaParams,
    hwm: u64,
    rng: Xoshiro256StarStar,
}

impl HnswIndex {
    /// The index's meta page.
    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    /// The directory chain's head page (§7.1).
    pub fn dir_head(&self) -> PageId {
        self.dir_head
    }

    /// The validated meta parameters (creation-pinned + runtime
    /// entry_point/max_level).
    pub fn params(&self) -> &MetaParams {
        &self.params
    }

    /// The chain-derived high-water mark: the next unallocated NodeId.
    pub fn hwm(&self) -> u64 {
        self.hwm
    }

    /// Entry-point NodeId (`NodeId::INVALID.0` for an empty graph).
    pub fn entry_point(&self) -> u32 {
        self.params.entry_point
    }

    /// Top level of the entry-point node (0 for an empty graph).
    pub fn max_level(&self) -> u8 {
        self.params.max_level
    }

    /// Draw the next node level from the graph's PRNG (controlled rng
    /// access — no mutable alias of the rng escapes; Stage C's insert path
    /// consumes this). The draw uses the meta's `m`, §4.1.
    pub fn next_level(&mut self) -> u8 {
        self.rng.next_level(self.params.m)
    }

    /// Controlled high-water-mark write (Stage C's insert advances it
    /// after each DirAppend; not exposed to pg-engine directly — the write
    /// path inside this crate drives it).
    #[expect(dead_code, reason = "consumed by the Stage C write path (insert)")]
    pub(crate) fn set_hwm(&mut self, hwm: u64) {
        self.hwm = hwm;
    }

    /// Controlled runtime meta update (open-repair and Stage C's
    /// entry-point promotion share this; mirrors an applied
    /// `HnswMetaUpdate`).
    #[expect(
        dead_code,
        reason = "consumed by the Stage C write path (entry-point promotion)"
    )]
    pub(crate) fn set_entry_point(&mut self, entry_point: u32, max_level: u8) {
        self.params.entry_point = entry_point;
        self.params.max_level = max_level;
    }
}

/// Create a brand-new index (§10.3 creation sequence, §7.2 creation
/// geometry hard check).
///
/// Sequence (the A1 contract — every freshly allocated page is made
/// durable by its own post-image FPI before any later record, see
/// `page::log_page_init`):
///
/// 1. creation-geometry hard check (product-linked, §7.2 — ONE rule,
///    `node::check_creation_geometry`; HnswParams already validated its
///    own domain: m >= 2, m_max0 >= m, ef_construction >= m,
///    ef_search_default >= m);
/// 2. directory chain head: `new_page` → `init_dir_page(ordinal = 0)` →
///    `log_page_init`;
/// 3. meta page: `new_page` → `init_meta_page` → `write_meta`
///    (entry_point = INVALID, max_level = 0, dir_head = step 2's page,
///    snapshot_format_version = 1) → `log_page_init`.
///
/// Crash windows (§10.3): after 1–2 the fresh directory page is an
/// accounted orphan (M6 cleanup); after 2–3 the index is invisible
/// (creation is a utility operation — fail and recreate). The engine-side
/// `pg_rust_relpages` first_page registration is slice 3b's job.
///
/// Durability boundary (2026-09-16, adversarial review nano): `create`
/// returning Ok does NOT mean durable — this function appends WAL records
/// but never flushes; the caller owns the success boundary (§8.1 boundary
/// ① — `flush_to` / auto-commit covers every record above in WAL order).
pub fn create(
    buffer_pool: &BufferPool,
    wal_writer: &WalWriter,
    params: HnswParams,
    dim: u16,
    metric: Metric,
    selection: NeighborSelection,
    rng_seed: u64,
) -> Result<HnswIndex> {
    crate::node::check_creation_geometry(dim, params.m(), params.m_max0())?;

    // Step 2: directory chain head.
    let dir_head = {
        let mut guard = buffer_pool
            .new_page()
            .map_err(|e| crate::page::storage_err("create: allocate directory head", e))?;
        let page_id = guard.page_id();
        let page: &mut [u8; PAGE_SIZE] = guard
            .page_mut()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        crate::page::init_dir_page(&mut *page, 0);
        crate::page::log_page_init(wal_writer, page_id, page)?;
        page_id
    };

    let params = MetaParams {
        entry_point: NodeId::INVALID.0,
        max_level: 0,
        metric,
        selection,
        dim,
        m: params.m(),
        m_max0: params.m_max0(),
        ef_construction: params.ef_construction(),
        ef_search_default: params.ef_search_default(),
        rng_seed,
        dir_head,
    };

    // Step 3: meta page with the full creation-pinned parameter set.
    let meta_page_id = {
        let mut guard = buffer_pool
            .new_page()
            .map_err(|e| crate::page::storage_err("create: allocate meta page", e))?;
        let page_id = guard.page_id();
        let page: &mut [u8; PAGE_SIZE] = guard
            .page_mut()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        crate::page::init_meta_page(&mut *page);
        crate::meta::write_meta(&mut *page, &params);
        crate::page::log_page_init(wal_writer, page_id, page)?;
        page_id
    };
    Ok(HnswIndex {
        meta_page_id,
        dir_head,
        params,
        hwm: 0,
        rng: Xoshiro256StarStar::new(rng_seed),
    })
}

/// The outcome of opening an index: the handle plus non-fatal warnings
/// (currently only the ef_search_default WARN case, v1.4 nano).
pub struct OpenOutcome {
    /// The opened index handle.
    pub index: HnswIndex,
    /// Non-fatal warnings (ef_search_default mismatch travels here as a
    /// string — the crate's dependency freeze bans a tracing dependency).
    pub warnings: Vec<String>,
}

/// Open an existing index: read + validate the meta page, walk the
/// directory chain, repair the entry point if the crash window left it
/// unpublished, and fast-forward the level-draw PRNG (§5 skip-ahead).
///
/// Sequence: `read_meta` (structural checks) → `check_expected` (the six
/// graph-destroying mismatches fail loudly; ef_search_default mismatch
/// collects into `warnings`) → `check_dir_chain` (the §11.3 four chain
/// assertions, HWM derived by the same function) → open-repair →
/// skip-ahead.
///
/// **Open-time meta repair** (§10.3, v1.7 P1-4): when the chain is
/// non-empty (`hwm > 0`) but the meta's entry_point is INVALID — the
/// §8.2 step 6–7 crash window (node fully connected, meta update never
/// written) — repair by reading directory entry 0's target node and
/// publishing `entry_point = 0, max_level = its top_level` through a
/// NORMAL `HnswMetaUpdate` WAL record + `apply_meta` + pd_lsn stamp. The
/// record is appended UNDER the meta page's write guard (pin_mut →
/// append → apply → stamp, the pg-am-btree write_meta_record order), so
/// a due pre-image FPI always precedes it in the WAL (2026-09-17,
/// review round 6 P1). The repair is **idempotent** (post-image
/// overwrite + pd_lsn guard),
/// **deterministic** (the directory ordinal is the only evidence needed),
/// and **WAL-recorded** (a normal record, not a recovery special case).
/// The rejected alternative — writing the meta update BEFORE connecting
/// the node — would point the entry point at an empty-list dead end and
/// lose every search answer; this window is strictly safer to repair at
/// open.
///
/// **Skip-ahead** (§5 case (c)): the level-draw PRNG is re-seeded from the
/// meta's `rng_seed` and advanced by exactly `hwm` `next_level` draws —
/// replay never consumes the rng (levels travel in records), so a
/// continuation insert continues the exact stream of the never-crashed
/// run (cross-crash reproducibility, §11.1).
pub fn open(
    buffer_pool: &BufferPool,
    wal_writer: &WalWriter,
    meta_page_id: PageId,
    expected: &ExpectedParams,
) -> Result<OpenOutcome> {
    // 1. Meta page: structural validation + expectation check.
    let mut params = {
        let guard = buffer_pool
            .pin(meta_page_id)
            .map_err(|e| crate::page::storage_err("open: pin meta page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is PAGE_SIZE");
        crate::meta::read_meta(page)?
    };
    let warnings = crate::meta::check_expected(&params, expected)?;

    // 2. Directory chain walk (fetch through the read-only pin — the
    // closure pins per page and copies the frame out; no mutation, so the
    // read guard is the right tool, buffer_pool.rs:272).
    let dir_head = params.dir_head;
    let fetch = |page_id: PageId| -> Result<[u8; PAGE_SIZE]> {
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| crate::page::storage_err("open: pin directory page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is PAGE_SIZE");
        Ok(*page)
    };
    let chain = crate::dir::check_dir_chain(dir_head, fetch)?;

    // 3a. Published entry point must name an allocated node (2026-09-17,
    // review round 3 P3-1): INVALID is the empty graph (hwm == 0); anything
    // else must be below the chain-derived HWM. The HWM is already computed
    // by the walk above, so this natural open-time boundary check is free —
    // a corrupt meta (entry_point = 100, hwm = 2) previously passed open
    // and failed only at Stage C's first search. (This is the OPEN-side
    // instance of §11.3 audit item c; the redo path stays free of it per
    // the evaluability constraint.)
    if params.entry_point != NodeId::INVALID.0 && u64::from(params.entry_point) >= chain.hwm {
        return Err(crate::error::HnswError::Corrupted(format!(
            "meta page entry_point {} >= chain high-water mark {} (the entry point must name an allocated node)",
            params.entry_point, chain.hwm
        )));
    }

    // 3b. Open-time meta repair (see the fn-level contract above).
    if chain.hwm > 0 && params.entry_point == NodeId::INVALID.0 {
        let head_page = fetch(dir_head)?;
        let (node_page_id, slot) = crate::dir::dir_entry(&head_page, 0)?;
        let top_level = {
            let guard = buffer_pool
                .pin(node_page_id)
                .map_err(|e| crate::page::storage_err("open-repair: pin first node page", e))?;
            let page: &[u8; PAGE_SIZE] = guard
                .page()
                .try_into()
                .expect("a buffer frame is PAGE_SIZE");
            // 2026-09-17, review round 4 P3-1: this repair read path was
            // the ONLY page-type-unchecked read entry (check_dir_chain
            // checks PAGE_TYPE_DIR, read_meta checks PAGE_TYPE_META;
            // apply's entry_at checks LP bounds only). A corrupted
            // directory entry pointing at another structurally-legal
            // slotted page would read a garbage state byte — masked to
            // <= 63, it passes every domain check and gets published as
            // the entry point through a legal WAL record. Check the tag.
            let page_type = crate::page::page_type(page);
            if page_type != crate::page::PAGE_TYPE_NODE {
                return Err(crate::error::HnswError::Corrupted(format!(
                    "open-repair: directory entry 0 points at page {} with page_type {page_type}, not a node page",
                    node_page_id.0
                )));
            }
            crate::apply::entry_top_level(page, slot, params.dim)?
        };
        // 2026-09-17, review round 6 P3: top_level is a 6-bit mask of a
        // possibly-corrupt node entry — check it against the SEMANTIC
        // ceiling l_max(m) (no next_level draw can exceed it, rng.rs)
        // before publishing it as max_level through a legal WAL record.
        // The HnswMetaUpdate constructor only knows the 6-bit format
        // ceiling (it never sees m); read_meta owns the same check for
        // the meta page's stored max_level.
        let l_max = crate::rng::l_max(params.m);
        if top_level > l_max {
            return Err(crate::error::HnswError::Corrupted(format!(
                "open-repair: node 0's top_level {top_level} > l_max(m = {}) = {l_max} (corrupt node entry — no level draw can produce it)",
                params.m
            )));
        }
        // 2026-09-17, review round 6 P1: pin_mut BEFORE the WAL append.
        // A pin_mut that owes this checkpoint cycle's FPI emits the
        // pre-image FullPageImage, and its contract requires the FPI to
        // precede the modification's own record (buffer_pool.rs
        // ensure_fpi). Appending first would give the FPI a HIGHER LSN
        // than the MetaUpdate: redo would replay the MetaUpdate and then
        // restore the FPI's pre-repair image, wiping the repair — and
        // the stamped pd_lsn would lag the FPI LSN, breaking pd_lsn
        // authority (page.rs). Reference order: pg-am-btree's
        // write_meta_record — pin_mut → append → apply → stamp.
        let mut guard = buffer_pool
            .pin_mut(meta_page_id)
            .map_err(|e| crate::page::storage_err("open-repair: pin_mut meta page", e))?;
        let record = WalRecord::hnsw_meta_update(meta_page_id, 0, top_level)
            .map_err(|e| crate::page::storage_err("open-repair: encode MetaUpdate", e))?;
        let lsn: Lsn = wal_writer
            .append(record)
            .map_err(|e| crate::page::storage_err("open-repair: WAL append", e))?;
        let page: &mut [u8; PAGE_SIZE] = guard
            .page_mut()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        crate::apply::apply_meta(page, 0, top_level);
        pg_storage::page::set_page_pd_lsn(page, lsn);
        params.entry_point = 0;
        params.max_level = top_level;
    }

    // 4. rng skip-ahead (§5 case (c)).
    let mut rng = Xoshiro256StarStar::new(params.rng_seed);
    for _ in 0..chain.hwm {
        let _ = rng.next_level(params.m);
    }

    Ok(OpenOutcome {
        index: HnswIndex {
            meta_page_id,
            dir_head,
            params,
            hwm: chain.hwm,
            rng,
        },
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply;
    use crate::node::NodeGeometry;
    use crate::page::{init_node_page, log_page_init};
    use pg_storage::config::StorageConfig;
    use pg_storage::engine::StorageEngine;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Manual temp dir (no tempfile dev-dependency — M4's dependency
    /// freeze; page_init.rs uses the same pattern).
    fn fresh_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pg_am_hnsw_m5_index-{}-{}-{tag}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn page_mut<'g>(
        guard: &'g mut pg_storage::buffer_pool::PageGuardMut<'_>,
    ) -> &'g mut [u8; PAGE_SIZE] {
        guard
            .page_mut()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE")
    }

    const DIM: u16 = 128;
    const SEED: u64 = 0x5EED;

    fn make_expected() -> ExpectedParams {
        ExpectedParams {
            dim: DIM,
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search_default: 64,
            metric: Metric::L2,
            selection: NeighborSelection::Heuristic,
        }
    }

    /// Covers the open-repair path (hand-constructed §8.2 step 6–7
    /// crash-window residue — a fully connected node whose MetaUpdate
    /// never landed — after which `open` must publish entry_point = 0
    /// with the node's top_level) and skip-ahead (the opened rng must
    /// continue the exact stream).
    #[test]
    fn open_repairs_entry_point_and_skips_ahead() {
        let dir = fresh_dir("repair");
        let config = StorageConfig::new(&dir);
        let geo = NodeGeometry {
            dim: DIM,
            m: 16,
            m_max0: 32,
        };
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

            // Residue: node entry (top_level = 3) + directory entry 0, but
            // NO MetaUpdate — the step 6–7 crash window.
            let node_page_id = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                let page_id = guard.page_id();
                let page = page_mut(&mut guard);
                init_node_page(page);
                log_page_init(engine.wal_writer(), page_id, page).unwrap();
                page_id
            };
            // Make the residue WAL-durable in IMAGE form (the A1/FPI
            // mechanism): a full-page-image record of the post-apply page
            // plus a pd_lsn stamp at that record's LSN. Without the stamp,
            // the pin_mut pre-image FPI would replay over the unlogged
            // content and wipe the residue (pd_lsn is only advanced by WAL
            // records). Logical-record replay of 121–127 now exists (Stage
            // C slice 1, redo.rs); the image form keeps this Stage B test
            // independent of the handler wiring it predates.
            let slot = {
                let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
                let page = page_mut(&mut guard);
                let slot = apply::append_node(page, 0, 3, geo, &[1.0; DIM as usize]).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(node_page_id, page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
                slot
            };
            {
                let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
                let page = page_mut(&mut guard);
                apply::dir_append(page, 0, node_page_id, slot).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(index.dir_head(), page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            engine.buffer_pool().flush(node_page_id).unwrap();
            engine.buffer_pool().flush(index.dir_head()).unwrap();
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // crash: no checkpoint, no MetaUpdate
            index.meta_page_id()
        };

        // Open: repair must publish entry_point = 0 / max_level = 3, and
        // the rng must be advanced by hwm = 1 draw.
        let engine = StorageEngine::open_with_redo_handlers(
            &dir,
            &config,
            crate::redo::hnsw_redo_handlers(),
            vec![],
        )
        .unwrap();
        let mut outcome = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &make_expected(),
        )
        .unwrap();
        assert_eq!(outcome.index.entry_point(), 0);
        assert_eq!(outcome.index.max_level(), 3);
        assert_eq!(outcome.index.hwm(), 1);

        // Skip-ahead: next_level == the reference stream's second draw.
        let mut reference = Xoshiro256StarStar::new(SEED);
        let _ = reference.next_level(16); // node 0's draw
        assert_eq!(outcome.index.next_level(), reference.next_level(16));

        // Second open: idempotent — entry point already published, no new
        // repair, meta page bytes unchanged.
        let meta_bytes_before = {
            let guard = engine.buffer_pool().pin(meta_page_id).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            *page
        };
        let outcome2 = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &make_expected(),
        )
        .unwrap();
        assert_eq!(outcome2.index.entry_point(), 0);
        assert_eq!(outcome2.index.max_level(), 3);
        let meta_bytes_after = {
            let guard = engine.buffer_pool().pin(meta_page_id).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            *page
        };
        assert_eq!(meta_bytes_before, meta_bytes_after);

        // The repair is WAL-RECORDED: scan the WAL for the HnswMetaUpdate
        // record carrying entry_point = 0 / max_level = 3. The WAL fsync
        // above plus this record's presence is the durability claim;
        // engine-reopen replay of 121–127 (the stronger end-to-end proof)
        // is pinned by Stage C slice 1's `redo::tests::reopen_replays_hnsw_records`.
        engine.wal_writer().flush().unwrap();
        let wal_dir = dir.join("wal");
        let mut reader =
            pg_storage::wal::reader::WalReader::open(&wal_dir, config.wal_segment_size).unwrap();
        let mut found = false;
        while let Some(record) = reader.next_record().unwrap() {
            if record.record_type == pg_storage::wal::record::WalRecordType::HnswMetaUpdate {
                // 2026-09-16, adversarial review P3-3: decode through the
                // record's OWN decoder and assert fields — the earlier raw
                // byte assertion hard-coded a one-byte varint assumption
                // (breaks at meta_page_id >= 251 or a bincode config change).
                let decoded =
                    pg_storage::wal::record::HnswMetaUpdateRecord::decode(&record.payload).unwrap();
                assert_eq!(decoded.meta_page_id, meta_page_id);
                assert_eq!(decoded.entry_point, 0);
                assert_eq!(decoded.max_level, 3);
                found = true;
            }
        }
        assert!(
            found,
            "the open-repair must be WAL-recorded (HnswMetaUpdate)"
        );
        drop(engine);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 2026-09-17, review round 3 P3-1: a published entry point that names
    /// no allocated node (corrupt meta: entry_point = 100, hwm = 0) is
    /// rejected AT OPEN, not deferred to Stage C's first search.
    #[test]
    fn open_rejects_entry_point_beyond_hwm() {
        let dir = fresh_dir("ep-beyond-hwm");
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
        // Corrupt the meta page: a published entry point with an empty
        // chain (hwm = 0).
        let mut guard = engine.buffer_pool().pin_mut(index.meta_page_id()).unwrap();
        let page = page_mut(&mut guard);
        apply::apply_meta(page, 100, 0);
        drop(guard);
        let err = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            index.meta_page_id(),
            &make_expected(),
        );
        let Err(err) = err else {
            panic!("open must reject an entry point beyond the chain HWM");
        };
        assert!(err.to_string().contains("high-water mark"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 2026-09-17, review round 4 P3-1: the repair's target page must be a
    /// NODE page — a corrupted directory entry pointing at another
    /// structurally-legal slotted page (valid LP, wrong type tag) must fail
    /// loudly instead of publishing a garbage top_level as the entry point.
    #[test]
    fn open_repair_rejects_a_non_node_target_page() {
        let dir = fresh_dir("repair-type");
        let config = StorageConfig::new(&dir);
        let geo = NodeGeometry {
            dim: DIM,
            m: 16,
            m_max0: 32,
        };
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
            // Same residue shape as the repair test — but the "node" page's
            // type tag is corrupted (a heap page is slotted-legal with a
            // valid LP, so LP checks alone would not catch this).
            let node_page_id = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                let page_id = guard.page_id();
                let page = page_mut(&mut guard);
                init_node_page(page);
                log_page_init(engine.wal_writer(), page_id, page).unwrap();
                page_id
            };
            let slot = {
                let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
                let page = page_mut(&mut guard);
                let slot = apply::append_node(page, 0, 3, geo, &[1.0; DIM as usize]).unwrap();
                // Corrupt ONLY the type tag after the entry is in place.
                pg_storage::page::set_page_pd_flags(page, 7);
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(node_page_id, page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
                slot
            };
            {
                let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
                let page = page_mut(&mut guard);
                apply::dir_append(page, 0, node_page_id, slot).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(index.dir_head(), page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            engine.buffer_pool().flush(node_page_id).unwrap();
            engine.buffer_pool().flush(index.dir_head()).unwrap();
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine);
            index.meta_page_id()
        };
        let engine = StorageEngine::open_with_redo_handlers(
            &dir,
            &config,
            crate::redo::hnsw_redo_handlers(),
            vec![],
        )
        .unwrap();
        let err = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &make_expected(),
        );
        let Err(err) = err else {
            panic!("open-repair must reject a non-node target page");
        };
        assert!(err.to_string().contains("not a node page"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 2026-09-17, review round 6 P1: the repair's WAL order must be
    /// pin_mut (pre-image FPI, when one is due) → MetaUpdate append, so
    /// redo replays FPI-then-MetaUpdate and the repair survives replay.
    /// The reverse order gives the FPI a HIGHER LSN and lets recovery
    /// restore the pre-repair image over the fix. Force the FPI to be
    /// due (a checkpoint between reopen and open) and assert the stream
    /// order + pd_lsn authority numerically (record.lsn is pub).
    #[test]
    fn open_repair_emits_fpi_before_meta_update() {
        let dir = fresh_dir("repair-fpi-order");
        let config = StorageConfig::new(&dir);
        let geo = NodeGeometry {
            dim: DIM,
            m: 16,
            m_max0: 32,
        };
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
            // Same residue shape as open_repairs_entry_point_and_skips_ahead.
            let node_page_id = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                let page_id = guard.page_id();
                let page = page_mut(&mut guard);
                init_node_page(page);
                log_page_init(engine.wal_writer(), page_id, page).unwrap();
                page_id
            };
            let slot = {
                let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
                let page = page_mut(&mut guard);
                let slot = apply::append_node(page, 0, 3, geo, &[1.0; DIM as usize]).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(node_page_id, page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
                slot
            };
            {
                let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
                let page = page_mut(&mut guard);
                apply::dir_append(page, 0, node_page_id, slot).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(index.dir_head(), page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            engine.buffer_pool().flush(node_page_id).unwrap();
            engine.buffer_pool().flush(index.dir_head()).unwrap();
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // crash: no checkpoint, no MetaUpdate
            index.meta_page_id()
        };

        let engine = StorageEngine::open_with_redo_handlers(
            &dir,
            &config,
            crate::redo::hnsw_redo_handlers(),
            vec![],
        )
        .unwrap();
        // Publish a checkpoint BEFORE the open: the meta page's pd_lsn
        // (its creation FPI's LSN) is now < checkpoint_lsn, so the
        // repair's pin_mut owes a pre-image FPI.
        let checkpoint_lsn = engine.trigger_checkpoint().unwrap();
        let outcome = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &make_expected(),
        )
        .unwrap();
        assert_eq!(outcome.index.entry_point(), 0);
        assert_eq!(outcome.index.max_level(), 3);

        // WAL stream assertions: every post-checkpoint FullPageImage must
        // PRECEDE the repair's HnswMetaUpdate (else redo restores the
        // pre-repair image over the fix), and the meta page's stamped
        // pd_lsn must be the MetaUpdate's LSN (not the FPI's).
        engine.wal_writer().flush().unwrap();
        let wal_dir = dir.join("wal");
        let mut reader =
            pg_storage::wal::reader::WalReader::open(&wal_dir, config.wal_segment_size).unwrap();
        let mut mu_lsn = None;
        let mut fpi_lsns = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            match record.record_type {
                pg_storage::wal::record::WalRecordType::HnswMetaUpdate
                    if record.lsn > checkpoint_lsn =>
                {
                    let decoded =
                        pg_storage::wal::record::HnswMetaUpdateRecord::decode(&record.payload)
                            .unwrap();
                    assert_eq!(decoded.meta_page_id, meta_page_id);
                    assert_eq!(decoded.entry_point, 0);
                    assert_eq!(decoded.max_level, 3);
                    mu_lsn = Some(record.lsn);
                }
                pg_storage::wal::record::WalRecordType::FullPageImage
                    if record.lsn > checkpoint_lsn =>
                {
                    fpi_lsns.push(record.lsn);
                }
                _ => {}
            }
        }
        let mu_lsn = mu_lsn.expect("the repair must append its HnswMetaUpdate");
        assert!(
            !fpi_lsns.is_empty(),
            "a pre-image FPI must be due for the meta page (checkpoint published before open)"
        );
        for fpi_lsn in &fpi_lsns {
            assert!(
                *fpi_lsn < mu_lsn,
                "FPI @{} must precede the repair's MetaUpdate @{} (else redo restores the pre-repair image over the fix)",
                fpi_lsn.0,
                mu_lsn.0
            );
        }
        {
            let guard = engine.buffer_pool().pin(meta_page_id).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            assert_eq!(
                pg_storage::page::page_pd_lsn(page),
                mu_lsn,
                "the meta page's authoritative pd_lsn must be the MetaUpdate LSN, not the FPI's"
            );
        }
        drop(engine);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 2026-09-17, review round 6 P3: the repair reads node 0's top_level
    /// from a possibly-corrupt entry (6-bit mask, <= 63) — a value above
    /// l_max(m) is unreachable by any level draw and must be rejected
    /// BEFORE it is published as max_level through a legal WAL record
    /// (the HnswMetaUpdate constructor only knows the format ceiling).
    #[test]
    fn open_repair_rejects_top_level_beyond_l_max() {
        let dir = fresh_dir("repair-lmax");
        let config = StorageConfig::new(&dir);
        let geo = NodeGeometry {
            dim: DIM,
            m: 16,
            m_max0: 32,
        };
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
            // Residue with a format-legal (<= 63) but semantically
            // unreachable top_level = 14 (> l_max(16) = 13).
            let node_page_id = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                let page_id = guard.page_id();
                let page = page_mut(&mut guard);
                init_node_page(page);
                log_page_init(engine.wal_writer(), page_id, page).unwrap();
                page_id
            };
            let slot = {
                let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
                let page = page_mut(&mut guard);
                let slot = apply::append_node(page, 0, 14, geo, &[1.0; DIM as usize]).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(node_page_id, page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
                slot
            };
            {
                let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
                let page = page_mut(&mut guard);
                apply::dir_append(page, 0, node_page_id, slot).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(index.dir_head(), page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            engine.buffer_pool().flush(node_page_id).unwrap();
            engine.buffer_pool().flush(index.dir_head()).unwrap();
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine);
            index.meta_page_id()
        };
        let engine = StorageEngine::open_with_redo_handlers(
            &dir,
            &config,
            crate::redo::hnsw_redo_handlers(),
            vec![],
        )
        .unwrap();
        let err = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &make_expected(),
        );
        let Err(err) = err else {
            panic!("open-repair must reject a top_level beyond l_max(m)");
        };
        assert!(err.to_string().contains("l_max"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
