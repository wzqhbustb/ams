//! HNSW index lifecycle: creation, open, and the insert write path —
//! Phase 2 M5 Stage B slice 3a (tech-selection §10.3 creation/open
//! protocol, §7.2 creation geometry, §5 rng skip-ahead) and Stage C slice 3
//! (§8.1 eight-step insert, §10.2 write path).
//!
//! [`HnswIndex`] is the handle pg-engine (slice 3b) and the Stage C write
//! path consume: it carries the meta-page locator, the directory chain
//! head, the validated [`crate::meta::MetaParams`], the chain-derived
//! high-water mark, the graph's level-draw PRNG (with its lineage
//! preserved across restarts — see the `open` function below), and the
//! slice-3 insert-resolution caches (`dir_pages`, `current_node_page`).
//!
//! Creation is a UTILITY operation (§8.1: no transactional DML; index
//! records carry `txn_id = INVALID` and there is no abort window).

use pg_storage::buffer_pool::BufferPool;
use pg_storage::types::{Lsn, PageId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;

use crate::error::{HnswError, Result};
use crate::graph::{GraphAccess, Metric, NeighborSelection};
pub use crate::meta::{ExpectedParams, MetaParams};
use crate::node::NodeGeometry;
use crate::paged::PagedGraph;
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
    /// The directory chain's pages in ordinal order (2026-09-20, Stage C
    /// slice 3 — from `DirChainInfo::pages` at open, `vec![dir_head]` at
    /// create): the NodeId → `(page, slot)` resolution cache; insert/search
    /// never re-walk the chain.
    dir_pages: Vec<PageId>,
    /// The node page insert appends to (2026-09-20, Stage C slice 3): the
    /// page holding the most recent node entry at open/create (`None` on an
    /// empty graph); insert re-selects when it has no room for the next
    /// entry.
    current_node_page: Option<PageId>,
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
    pub(crate) fn set_hwm(&mut self, hwm: u64) {
        self.hwm = hwm;
    }

    /// Controlled runtime meta update (open-repair and Stage C's
    /// entry-point promotion share this; mirrors an applied
    /// `HnswMetaUpdate`).
    pub(crate) fn set_entry_point(&mut self, entry_point: u32, max_level: u8) {
        self.params.entry_point = entry_point;
        self.params.max_level = max_level;
    }

    /// Insert a vector into the page-resident graph (tech-selection §8.1
    /// eight-step sequence; Stage C slice 3). Returns the freshly allocated
    /// [`NodeId`] (dense, never reused — §3).
    ///
    /// **WAL-first discipline** (the open-repair round-6 P1 ordering): every
    /// page mutation is `pin_mut` (which may emit the due pre-image FPI) →
    /// WAL append → apply → `set_page_pd_lsn`, one guard per page touch.
    ///
    /// **Entry validation** (§5 funnel, same shape as graph.rs's
    /// `validate_entry_vector`): exact dimension match, no non-finite
    /// components, and (cosine) no zero vector — a failure changes NOTHING
    /// (no level drawn, no page touched, hwm unmoved).
    ///
    /// **Crash windows** (§8.2): an `Err` return or a crash mid-sequence
    /// leaves a residue of the §8.2 window shapes (INITIALIZING node,
    /// published directory mapping without MetaUpdate, …) — all of them
    /// replayable (slice-1 redo) and open-repairable; the graph is
    /// observably consistent only after the success boundary.
    ///
    /// **Success boundary** (§8.1 boundary ①): `flush_to` the last record's
    /// LSN (the PublishLive) before returning `Ok` — every record of the
    /// sequence precedes it in WAL order.
    ///
    /// **Single-threaded premise**: one writer per index handle; the buffer
    /// pool's latches make the page touches safe, but the eight steps are
    /// not isolated against a concurrent inserter.
    pub fn insert(
        &mut self,
        buffer_pool: &BufferPool,
        wal_writer: &WalWriter,
        vector: &[f32],
    ) -> Result<NodeId> {
        // Step 0: entry validation (§5) + NodeId space (mirrors
        // graph.rs:472 — the INVALID sentinel stays unallocated).
        if vector.len() != usize::from(self.params.dim) {
            return Err(HnswError::InvalidArgument(format!(
                "insert: vector has {} components, index dim is {} (§3: same index never mixes dimensions)",
                vector.len(),
                self.params.dim
            )));
        }
        self.params.metric.distance(vector, vector)?;
        if self.hwm == u64::from(u32::MAX) {
            return Err(HnswError::InvalidOperation(
                "NodeId space (u32) exhausted; NodeId::INVALID must stay unallocated (§3)"
                    .to_string(),
            ));
        }

        // Step 1 (level draw) — the rng's stream position is the hwm, so
        // the draw must happen exactly once per successful validation.
        let level = self.next_level();
        let node_id = NodeId(self.hwm as u32);
        let geometry = NodeGeometry {
            dim: self.params.dim,
            m: self.params.m,
            m_max0: self.params.m_max0,
        };
        let entry_len = geometry.entry_size(level);

        // Step 2 (§8.1 step 1): the current node page must have room for
        // this entry — otherwise allocate a fresh one through the init
        // chain (new_page → init → log_page_init; no exceptions, page.rs).
        let node_page_id = match self.current_node_page {
            Some(p) => {
                let fits = {
                    let guard = buffer_pool
                        .pin(p)
                        .map_err(|e| crate::page::storage_err("insert: pin node page", e))?;
                    let page: &[u8; PAGE_SIZE] = guard
                        .page()
                        .try_into()
                        .expect("a buffer frame is exactly PAGE_SIZE");
                    crate::apply::select_slot(page, entry_len).is_ok()
                };
                if fits {
                    p
                } else {
                    let fresh = alloc_node_page(buffer_pool, wal_writer)?;
                    self.current_node_page = Some(fresh);
                    fresh
                }
            }
            None => {
                let fresh = alloc_node_page(buffer_pool, wal_writer)?;
                self.current_node_page = Some(fresh);
                fresh
            }
        };

        // Step 3 (§8.1 step 2): the directory tail must have room for the
        // mapping — otherwise link a fresh chain page (its init FPI lands
        // BEFORE the DirLink record, so redo's ordinal/type checks see a
        // real directory page).
        let mut dir_tail = *self
            .dir_pages
            .last()
            .expect("the chain always has a head page");
        {
            let tail_full = {
                let guard = buffer_pool
                    .pin(dir_tail)
                    .map_err(|e| crate::page::storage_err("insert: pin directory tail", e))?;
                let page: &[u8; PAGE_SIZE] = guard
                    .page()
                    .try_into()
                    .expect("a buffer frame is exactly PAGE_SIZE");
                crate::page::dir_count(page) == crate::dir::DIR_ENTRIES_PER_PAGE
            };
            if tail_full {
                let new_dir = alloc_dir_page(buffer_pool, wal_writer, self.dir_pages.len() as u64)?;
                let mut guard = buffer_pool
                    .pin_mut(dir_tail)
                    .map_err(|e| crate::page::storage_err("insert: pin_mut directory tail", e))?;
                let record = WalRecord::hnsw_dir_link(dir_tail, new_dir)
                    .map_err(|e| crate::page::storage_err("insert: encode DirLink", e))?;
                let lsn = wal_writer
                    .append(record)
                    .map_err(|e| crate::page::storage_err("insert: WAL append DirLink", e))?;
                let page: &mut [u8; PAGE_SIZE] = guard
                    .page_mut()
                    .try_into()
                    .expect("a buffer frame is exactly PAGE_SIZE");
                crate::apply::dir_link(page, new_dir);
                pg_storage::page::set_page_pd_lsn(page, lsn);
                drop(guard);
                self.dir_pages.push(new_dir);
                dir_tail = new_dir;
            }
        }

        // Step 4 (§8.1 step 3): NodeInit — the slot is selected on the same
        // guard that applies the entry (single-threaded premise: the
        // selection cannot go stale between the two).
        let slot = {
            let mut guard = buffer_pool
                .pin_mut(node_page_id)
                .map_err(|e| crate::page::storage_err("insert: pin_mut node page", e))?;
            let page: &mut [u8; PAGE_SIZE] = guard
                .page_mut()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            let slot = crate::apply::select_slot(page, entry_len)?;
            let record = WalRecord::hnsw_node_init(
                self.meta_page_id,
                node_page_id,
                slot,
                node_id.0,
                level,
                self.params.dim,
                vector.to_vec(),
            )
            .map_err(|e| crate::page::storage_err("insert: encode NodeInit", e))?;
            let lsn = wal_writer
                .append(record)
                .map_err(|e| crate::page::storage_err("insert: WAL append NodeInit", e))?;
            crate::apply::apply_node_at(page, slot, node_id.0, level, geometry, vector)?;
            pg_storage::page::set_page_pd_lsn(page, lsn);
            slot
        };

        // Step 5 (§8.1 step 4): DirAppend — this record IS the NodeId
        // allocation (v1.2: allocation == mapping publication).
        {
            let mut guard = buffer_pool
                .pin_mut(dir_tail)
                .map_err(|e| crate::page::storage_err("insert: pin_mut directory tail", e))?;
            let record = WalRecord::hnsw_dir_append(dir_tail, node_id.0, node_page_id, slot)
                .map_err(|e| crate::page::storage_err("insert: encode DirAppend", e))?;
            let lsn = wal_writer
                .append(record)
                .map_err(|e| crate::page::storage_err("insert: WAL append DirAppend", e))?;
            let page: &mut [u8; PAGE_SIZE] = guard
                .page_mut()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            crate::apply::dir_append(page, node_id.0, node_page_id, slot)?;
            pg_storage::page::set_page_pd_lsn(page, lsn);
        }
        self.set_hwm(self.hwm + 1);

        // Step 6 (§8.1 step 5): search + connect, only when the graph was
        // non-empty BEFORE this insert (an empty graph has nothing to
        // connect to — the first node skips the whole phase, like
        // graph.rs's early return).
        let was_empty = self.params.entry_point == NodeId::INVALID.0;
        let old_max_level = self.params.max_level;
        // The new node's own per-level selections, kept for step 7. Levels
        // above the searched range stay empty and need NO record — NodeInit
        // already reserved them with count = 0, so an empty list is the
        // zero content (2026-09-20, slice 3 note: no record for empties).
        let mut own_lists: Vec<Vec<u32>> = vec![Vec::new(); usize::from(level) + 1];
        if !was_empty {
            let searched_top = level.min(old_max_level);
            {
                let g = PagedGraph::new(
                    buffer_pool,
                    &self.dir_pages,
                    self.hwm,
                    self.params.dim,
                    self.params.m,
                    self.params.m_max0,
                    self.params.metric,
                );
                let selection = self.params.selection;
                let ef_construction = self.params.ef_construction as usize;
                let m = usize::from(self.params.m);
                let m_max = |l: u8| {
                    if l == 0 {
                        usize::from(self.params.m_max0)
                    } else {
                        m
                    }
                };

                // Phase 1: greedy descent (ef = 1) from the entry point
                // down to the new node's own top level (Algorithm 1).
                let mut ep = NodeId(self.params.entry_point);
                for l in ((level + 1)..=old_max_level).rev() {
                    ep = crate::graph::search_layer(&g, vector, &[ep], 1, l)[0].id;
                }

                // Phase 2: beam-search, select, connect both sides, shrink
                // over-capacity lists through the same heuristic (§4.3).
                for l in (0..=searched_top).rev() {
                    let w = crate::graph::search_layer(&g, vector, &[ep], ef_construction, l);
                    let neighbors = crate::graph::select_neighbors(&g, selection, &w, m);
                    for &nb in &neighbors {
                        let (nb_page, nb_slot) = g.resolve(nb)?;
                        // Collect-then-pin_mut (paged.rs deadlock
                        // discipline): the read guard is dropped before the
                        // write guard exists.
                        let mut list: Vec<u32> = {
                            let guard = buffer_pool.pin(nb_page).map_err(|e| {
                                crate::page::storage_err("insert: pin neighbor page", e)
                            })?;
                            let page: &[u8; PAGE_SIZE] = guard
                                .page()
                                .try_into()
                                .expect("a buffer frame is exactly PAGE_SIZE");
                            crate::apply::neighbor_iter(page, nb_slot, geometry, l)?.collect()
                        };
                        // push_edge semantics: sorted insertion, a duplicate
                        // would mean algorithm divergence — loud, never
                        // silently absorbed (graph.rs debug_asserts the
                        // same invariant on the in-memory side).
                        match list.binary_search(&node_id.0) {
                            Ok(_) => {
                                return Err(HnswError::Corrupted(format!(
                                    "insert: duplicate edge {} -> {} on level {l}",
                                    nb.0, node_id.0
                                )))
                            }
                            Err(pos) => list.insert(pos, node_id.0),
                        }
                        // Shrink through the SAME heuristic (§4.3 "both
                        // sides"): the neighbor's own vector is the
                        // reference, distances recomputed against it.
                        let final_list = if list.len() > m_max(l) {
                            let cands: Vec<crate::graph::Cand> = list
                                .iter()
                                .map(|&c| crate::graph::Cand {
                                    dist: g.dist_between(nb, NodeId(c)),
                                    id: NodeId(c),
                                })
                                .collect();
                            crate::graph::select_neighbors(&g, selection, &cands, m_max(l))
                                .iter()
                                .map(|n| n.0)
                                .collect()
                        } else {
                            list
                        };
                        // ONE SetNeighbors record settles the layer's
                        // post-image (collect → pin_mut → append → apply →
                        // stamp).
                        let mut guard = buffer_pool.pin_mut(nb_page).map_err(|e| {
                            crate::page::storage_err("insert: pin_mut neighbor page", e)
                        })?;
                        let record = WalRecord::hnsw_set_neighbors(
                            self.meta_page_id,
                            nb_page,
                            nb_slot,
                            nb.0,
                            l,
                            final_list.clone(),
                        )
                        .map_err(|e| crate::page::storage_err("insert: encode SetNeighbors", e))?;
                        let lsn = wal_writer.append(record).map_err(|e| {
                            crate::page::storage_err("insert: WAL append SetNeighbors", e)
                        })?;
                        let page: &mut [u8; PAGE_SIZE] = guard
                            .page_mut()
                            .try_into()
                            .expect("a buffer frame is exactly PAGE_SIZE");
                        crate::apply::set_neighbors(page, nb_slot, geometry, l, &final_list)?;
                        pg_storage::page::set_page_pd_lsn(page, lsn);
                    }
                    own_lists[usize::from(l)] = neighbors.iter().map(|n| n.0).collect();
                    ep = w[0].id; // nearest result carries the descent
                }
            }

            // Step 7 (§8.1 step 6): the new node's own lists, AFTER every
            // backward edge is in place (the §8.1 literal order — nothing
            // in the loop above reads them).
            for (l, list) in own_lists
                .iter()
                .enumerate()
                .take(usize::from(searched_top) + 1)
            {
                if list.is_empty() {
                    continue; // empty list == the NodeInit zero content
                }
                let mut guard = buffer_pool
                    .pin_mut(node_page_id)
                    .map_err(|e| crate::page::storage_err("insert: pin_mut own node page", e))?;
                let record = WalRecord::hnsw_set_neighbors(
                    self.meta_page_id,
                    node_page_id,
                    slot,
                    node_id.0,
                    l as u8,
                    list.clone(),
                )
                .map_err(|e| crate::page::storage_err("insert: encode own SetNeighbors", e))?;
                let lsn = wal_writer.append(record).map_err(|e| {
                    crate::page::storage_err("insert: WAL append own SetNeighbors", e)
                })?;
                let page: &mut [u8; PAGE_SIZE] = guard
                    .page_mut()
                    .try_into()
                    .expect("a buffer frame is exactly PAGE_SIZE");
                crate::apply::set_neighbors(page, slot, geometry, l as u8, list)?;
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
        }

        // Step 8 (§8.1 step 7): MetaUpdate — a new top-level node becomes
        // the entry point; the first node of an empty graph always does.
        // (Non-empty graph with level <= max_level: no record at all.)
        if was_empty || level > old_max_level {
            let mut guard = buffer_pool
                .pin_mut(self.meta_page_id)
                .map_err(|e| crate::page::storage_err("insert: pin_mut meta page", e))?;
            let record = WalRecord::hnsw_meta_update(self.meta_page_id, node_id.0, level)
                .map_err(|e| crate::page::storage_err("insert: encode MetaUpdate", e))?;
            let lsn = wal_writer
                .append(record)
                .map_err(|e| crate::page::storage_err("insert: WAL append MetaUpdate", e))?;
            let page: &mut [u8; PAGE_SIZE] = guard
                .page_mut()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            crate::apply::apply_meta(page, node_id.0, level);
            pg_storage::page::set_page_pd_lsn(page, lsn);
            drop(guard);
            self.set_entry_point(node_id.0, level);
        }

        // Step 9 (§8.1 step 8): PublishLive — the state post-image lands
        // only after all of the node's own level lists are written (v1.7).
        let last_lsn = {
            let mut guard = buffer_pool
                .pin_mut(node_page_id)
                .map_err(|e| crate::page::storage_err("insert: pin_mut node page", e))?;
            let record = WalRecord::hnsw_publish_live(
                self.meta_page_id,
                node_page_id,
                slot,
                node_id.0,
                self.params.dim,
            )
            .map_err(|e| crate::page::storage_err("insert: encode PublishLive", e))?;
            let lsn = wal_writer
                .append(record)
                .map_err(|e| crate::page::storage_err("insert: WAL append PublishLive", e))?;
            let page: &mut [u8; PAGE_SIZE] = guard
                .page_mut()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            crate::apply::publish_live(page, slot, self.params.dim)?;
            pg_storage::page::set_page_pd_lsn(page, lsn);
            lsn
        };

        // Step 10 (§8.1 boundary ①): the success boundary — every record
        // of the sequence is durable before Ok escapes.
        wal_writer
            .flush_to(last_lsn)
            .map_err(|e| crate::page::storage_err("insert: success-boundary flush", e))?;
        Ok(node_id)
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
        dir_pages: vec![dir_head],
        current_node_page: None,
    })
}

/// Allocate + initialise a fresh node page through the §8.1 step-1 chain
/// (`new_page` → `init_node_page` → `log_page_init` post-image FPI — no
/// exceptions, page.rs's A1 contract).
fn alloc_node_page(buffer_pool: &BufferPool, wal_writer: &WalWriter) -> Result<PageId> {
    let mut guard = buffer_pool
        .new_page()
        .map_err(|e| crate::page::storage_err("alloc_node_page: allocate", e))?;
    let page_id = guard.page_id();
    let page: &mut [u8; PAGE_SIZE] = guard
        .page_mut()
        .try_into()
        .expect("a buffer frame is exactly PAGE_SIZE");
    crate::page::init_node_page(page);
    crate::page::log_page_init(wal_writer, page_id, page)?;
    Ok(page_id)
}

/// Allocate + initialise a fresh directory page with chain ordinal
/// `ordinal` through the same init chain (§8.1 step 2).
fn alloc_dir_page(
    buffer_pool: &BufferPool,
    wal_writer: &WalWriter,
    ordinal: u64,
) -> Result<PageId> {
    let mut guard = buffer_pool
        .new_page()
        .map_err(|e| crate::page::storage_err("alloc_dir_page: allocate", e))?;
    let page_id = guard.page_id();
    let page: &mut [u8; PAGE_SIZE] = guard
        .page_mut()
        .try_into()
        .expect("a buffer frame is exactly PAGE_SIZE");
    crate::page::init_dir_page(page, ordinal);
    crate::page::log_page_init(wal_writer, page_id, page)?;
    Ok(page_id)
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
    // read guard is the right tool, buffer_pool.rs:272). The head page's
    // bytes are cached for the open-repair below (2026-09-20, Stage C
    // slice 3: the walk already fetched them — a second pin would be a
    // redundant round-trip).
    let dir_head = params.dir_head;
    let mut head_cache: Option<[u8; PAGE_SIZE]> = None;
    let mut fetch = |page_id: PageId| -> Result<[u8; PAGE_SIZE]> {
        let guard = buffer_pool
            .pin(page_id)
            .map_err(|e| crate::page::storage_err("open: pin directory page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is PAGE_SIZE");
        if page_id == dir_head {
            head_cache = Some(*page);
        }
        Ok(*page)
    };
    let chain = crate::dir::check_dir_chain(dir_head, &mut fetch)?;

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
        let head_page = head_cache.expect("the chain walk fetches the head page first");
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

    // 5. Insert-resolution caches (2026-09-20, Stage C slice 3): the
    // ordinal-ordered chain pages come from the walk; the current node page
    // is the page of the most recent directory entry (`None` when the graph
    // is empty).
    let current_node_page = if chain.hwm > 0 {
        let tail_page_id = *chain.pages.last().expect("the chain is non-empty");
        let idx = ((chain.hwm - 1) % u64::from(crate::dir::DIR_ENTRIES_PER_PAGE)) as u32;
        let guard = buffer_pool
            .pin(tail_page_id)
            .map_err(|e| crate::page::storage_err("open: pin directory tail", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is PAGE_SIZE");
        Some(crate::dir::dir_entry(page, idx)?.0)
    } else {
        None
    };

    Ok(OpenOutcome {
        index: HnswIndex {
            meta_page_id,
            dir_head,
            params,
            hwm: chain.hwm,
            rng,
            dir_pages: chain.pages,
            current_node_page,
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

    // -----------------------------------------------------------------
    // 2026-09-20, Stage C slice 3: the insert write path (§8.1 eight-step
    // sequence). The load-bearing pin is the topology parity test against
    // the in-memory `Hnsw` — same seed, same vectors, byte-identical graph
    // semantics — plus the WAL ordering, page-overflow, crash-continuation,
    // and negative-validation pins.
    // -----------------------------------------------------------------

    /// Deterministic pseudo-random vectors from the crate's own PRNG (the
    /// M4 dependency freeze bans `rand`).
    fn det_vectors(seed: u64, n: usize, dim: usize) -> Vec<Vec<f32>> {
        let mut rng = Xoshiro256StarStar::new(seed);
        (0..n)
            .map(|_| {
                (0..dim)
                    .map(|_| (rng.next_u64() % 4096) as f32 / 64.0)
                    .collect()
            })
            .collect()
    }

    /// Full page-resident topology (per node: top_level + per-level
    /// neighbor ids) read through the slice-3 resolution cache.
    fn paged_topology(pool: &BufferPool, index: &HnswIndex) -> Vec<(u8, Vec<Vec<u32>>)> {
        let geo = NodeGeometry {
            dim: index.params.dim,
            m: index.params.m,
            m_max0: index.params.m_max0,
        };
        let g = PagedGraph::new(
            pool,
            &index.dir_pages,
            index.hwm,
            index.params.dim,
            index.params.m,
            index.params.m_max0,
            index.params.metric,
        );
        (0..index.hwm)
            .map(|i| {
                let (page_id, slot) = g.resolve(NodeId(i as u32)).unwrap();
                let guard = pool.pin(page_id).unwrap();
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
                let top = apply::entry_top_level(page, slot, geo.dim).unwrap();
                let lists = (0..=top)
                    .map(|l| {
                        apply::neighbor_iter(page, slot, geo, l)
                            .unwrap()
                            .collect::<Vec<u32>>()
                    })
                    .collect();
                (top, lists)
            })
            .collect()
    }

    /// The in-memory twin's topology in the same shape.
    fn memory_topology(g: &crate::graph::Hnsw) -> Vec<(u8, Vec<Vec<u32>>)> {
        (0..g.node_count())
            .map(|i| {
                let id = NodeId(i as u32);
                let top = g.level(id);
                let lists = (0..=top)
                    .map(|l| g.neighbors(id, l).iter().map(|n| n.0).collect::<Vec<u32>>())
                    .collect();
                (top, lists)
            })
            .collect()
    }

    /// The slice-3 load-bearing pin: the page-resident insert path must
    /// produce the graph the in-memory reference produces — same seed (the
    /// level draws align), same vectors, byte-identical topology.
    #[test]
    fn paged_insert_matches_in_memory_topology() {
        let dir = fresh_dir("paged-parity");
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
        let mut mem = crate::graph::Hnsw::new(4, Metric::L2, params, 0xC0FFEE).unwrap();
        for v in det_vectors(0xDEC0DE, 200, 4) {
            let m_id = mem.insert(&v).unwrap();
            let p_id = index
                .insert(engine.buffer_pool(), engine.wal_writer(), &v)
                .unwrap();
            assert_eq!(m_id, p_id, "NodeId streams must align");
        }
        assert_eq!(index.hwm(), 200);
        assert_eq!(index.entry_point(), mem.entry_point().unwrap().0);
        assert_eq!(index.max_level(), mem.max_level());
        assert_eq!(
            paged_topology(engine.buffer_pool(), &index),
            memory_topology(&mem),
            "page-resident and in-memory topologies must be identical"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// First insert into an empty graph: hwm = 1, entry_point = 0, the
    /// entry is LIVE with top_level == max_level, and the WAL carries the
    /// §8.1 prefix NodeInit → DirAppend → MetaUpdate → PublishLive in
    /// strictly ascending LSN order.
    #[test]
    fn first_insert_state_and_wal_order() {
        let dir = fresh_dir("first-insert");
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
        let id = index
            .insert(
                engine.buffer_pool(),
                engine.wal_writer(),
                &vec![1.0; DIM as usize],
            )
            .unwrap();
        assert_eq!(id, NodeId(0));
        assert_eq!(index.hwm(), 1);
        assert_eq!(index.entry_point(), 0);

        let g = PagedGraph::new(
            engine.buffer_pool(),
            &index.dir_pages,
            index.hwm,
            DIM,
            16,
            32,
            Metric::L2,
        );
        let (node_page, slot) = g.resolve(NodeId(0)).unwrap();
        let guard = engine.buffer_pool().pin(node_page).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert!(apply::entry_is_live(page, slot, DIM).unwrap());
        assert_eq!(
            apply::entry_top_level(page, slot, DIM).unwrap(),
            index.max_level()
        );
        drop(guard);

        // WAL scan: exactly the four records, LSN-ordered. The FullPageImage
        // records interleaved between them (page-init chain + pin_mut
        // pre-images) are intentionally filtered out — FPI-before-record
        // ordering is structurally guaranteed by the pin_mut → append
        // sequence, not something this assertion needs to see (2026-09-20,
        // slice-3 adversarial review nano ①). The exact-four cardinality
        // is a deliberate hard pin on the first-insert path: any future
        // LEGAL change to the record sequence turns this test red on
        // purpose (same nano ② — registered, not accidental).
        use pg_storage::wal::record::WalRecordType::*;
        let wal_dir = dir.join("wal");
        let mut reader =
            pg_storage::wal::reader::WalReader::open(&wal_dir, config.wal_segment_size).unwrap();
        let mut seq: Vec<(Lsn, pg_storage::wal::record::WalRecordType)> = Vec::new();
        while let Some(rec) = reader.next_record().unwrap() {
            if matches!(
                rec.record_type,
                HnswNodeInit
                    | HnswSetNeighbors
                    | HnswMetaUpdate
                    | HnswNodeTombstone
                    | HnswDirAppend
                    | HnswDirLink
                    | HnswPublishLive
            ) {
                seq.push((rec.lsn, rec.record_type));
            }
        }
        let types: Vec<_> = seq.iter().map(|(_, t)| *t).collect();
        assert_eq!(
            types,
            vec![HnswNodeInit, HnswDirAppend, HnswMetaUpdate, HnswPublishLive],
            "an empty-graph first insert is exactly the §8.1 prefix"
        );
        let lsns: Vec<_> = seq.iter().map(|(l, _)| *l).collect();
        assert!(
            lsns.windows(2).all(|w| w[0] < w[1]),
            "record LSNs must be strictly ascending"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Node-page AND directory-page overflow: dim = 1 with minimal
    /// parameters, N = 900 > 813 (DIR_ENTRIES_PER_PAGE) — the chain grows a
    /// second page mid-stream and the topology still matches the in-memory
    /// twin.
    #[test]
    fn insert_overflows_node_and_directory_pages() {
        let dir = fresh_dir("overflow");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let params = HnswParams::new(2, 2, 4, 2).unwrap();
        let mut index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            params,
            1,
            Metric::L2,
            NeighborSelection::Heuristic,
            0xBEEF,
        )
        .unwrap();
        let mut mem = crate::graph::Hnsw::new(1, Metric::L2, params, 0xBEEF).unwrap();
        for v in det_vectors(0xFACE, 900, 1) {
            let m_id = mem.insert(&v).unwrap();
            let p_id = index
                .insert(engine.buffer_pool(), engine.wal_writer(), &v)
                .unwrap();
            assert_eq!(m_id, p_id);
        }
        assert_eq!(index.hwm(), 900);
        assert_eq!(
            index.dir_pages.len(),
            2,
            "900 entries overflow the 813-entry head page"
        );
        assert_eq!(
            paged_topology(engine.buffer_pool(), &index),
            memory_topology(&mem)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Coding-plan Stage C row 4 acceptance: a checkpointless crash mid-run
    /// loses nothing — reopen replays the records, `open` re-derives hwm
    /// and skip-ahead restores the rng, and continuation inserts draw the
    /// exact level stream of the never-crashed run.
    #[test]
    fn reopen_continues_the_level_stream() {
        let dir = fresh_dir("level-stream");
        let config = StorageConfig::new(&dir);
        const K: u64 = 50;
        const J: u64 = 20;
        let vectors = det_vectors(0x1234, (K + J) as usize, DIM as usize);
        let meta_page_id = {
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
            for v in &vectors[..K as usize] {
                index
                    .insert(engine.buffer_pool(), engine.wal_writer(), v)
                    .unwrap();
            }
            let meta = index.meta_page_id();
            std::mem::forget(engine); // crash: no checkpoint
            meta
        };

        let engine = StorageEngine::open_with_redo_handlers(
            &dir,
            &config,
            crate::redo::hnsw_redo_handlers(),
            vec![],
        )
        .unwrap();
        let mut index = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &make_expected(),
        )
        .unwrap()
        .index;
        assert_eq!(index.hwm(), K, "reopen must re-derive the hwm");
        for v in &vectors[K as usize..] {
            index
                .insert(engine.buffer_pool(), engine.wal_writer(), v)
                .unwrap();
        }
        assert_eq!(index.hwm(), K + J);

        // The continuation's level stream, read back off the pages…
        let g = PagedGraph::new(
            engine.buffer_pool(),
            &index.dir_pages,
            index.hwm,
            DIM,
            16,
            32,
            Metric::L2,
        );
        let actual: Vec<u8> = (K..K + J)
            .map(|i| {
                let (p, s) = g.resolve(NodeId(i as u32)).unwrap();
                let guard = engine.buffer_pool().pin(p).unwrap();
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
                apply::entry_top_level(page, s, DIM).unwrap()
            })
            .collect();
        // …must equal the never-crashed reference stream.
        let mut reference = Xoshiro256StarStar::new(SEED);
        for _ in 0..K {
            reference.next_level(16);
        }
        let expected: Vec<u8> = (0..J).map(|_| reference.next_level(16)).collect();
        assert_eq!(
            actual, expected,
            "§5 skip-ahead: the level stream continues exactly"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// §5 entry validation on the write path: wrong dimension, NaN
    /// component, and the cosine zero vector are loud errors that change NO
    /// state (hwm unmoved, and a legal insert afterwards still works).
    #[test]
    fn insert_rejects_bad_vectors_without_state_change() {
        let dir = fresh_dir("bad-vectors");
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
            7,
        )
        .unwrap();
        assert!(matches!(
            index.insert(engine.buffer_pool(), engine.wal_writer(), &[1.0, 2.0]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            index.insert(engine.buffer_pool(), engine.wal_writer(), &[f32::NAN; 4]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert_eq!(index.hwm(), 0, "a rejected insert must not allocate");

        let mut cosine = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            params,
            4,
            Metric::Cosine,
            NeighborSelection::Heuristic,
            7,
        )
        .unwrap();
        assert!(matches!(
            cosine.insert(engine.buffer_pool(), engine.wal_writer(), &[0.0; 4]),
            Err(HnswError::ZeroVector)
        ));
        assert_eq!(cosine.hwm(), 0);

        // Validation changed nothing: a legal insert still succeeds.
        let id = index
            .insert(engine.buffer_pool(), engine.wal_writer(), &[1.0; 4])
            .unwrap();
        assert_eq!(id, NodeId(0));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
