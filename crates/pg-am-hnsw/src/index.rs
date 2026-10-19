//! HNSW index lifecycle: creation, open, and the insert write path —
//! Phase 2 M5 Stage B slice 3a (tech-selection §10.3 creation/open
//! protocol, §7.2 creation geometry, §5 rng skip-ahead) and Stage C slice 3
//! (§8.1 eight-step insert, §10.2 write path), with the M6 Stage A order
//! (M6 tech-selection §4.2/§8.2: the own level lists land BEFORE the
//! backward edges — the §8.1 steps 5↔6 swap; the connect-window crash
//! residue becomes an unreachable orphan, and reachable ⟹ own lists
//! complete).
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

// The lock/atomic primitives come from pg-storage's `sync` alias layer:
// parking_lot in production, loom's instrumented ones under `--cfg loom`
// (the hnsw_loom model tests). Same discipline as pg-storage/pg-am-btree.
use pg_storage::sync::atomic::{AtomicU64, Ordering};
use pg_storage::sync::{Mutex, RwLock};

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

/// WAL-append boundary marks for the Stage D slice-2 crash-window test
/// matrix (§8.1 eight-step sequence × §8.2 window table). TEST-ONLY
/// instrumentation: the production path never reads these; they exist so
/// the crash-matrix tests can drive the REAL `insert` to an exact record
/// boundary (zero state drift vs. hand-built residues).
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeMark {
    /// A fresh node page was allocated + init-FPI'd (§8.1 step 1).
    NodePageInit,
    /// A fresh directory page was allocated + init-FPI'd (§8.1 step 2).
    DirPageInit,
    /// The old directory tail was linked to the fresh page.
    DirLink,
    /// The node entry was created (INITIALIZING).
    NodeInit,
    /// The NodeId mapping was published at the directory tail.
    DirAppend,
    /// One backward edge's SetNeighbors post-image landed.
    NeighborEdge {
        /// The level whose list was rewritten.
        level: u8,
        /// Whether this edge triggered the shrink heuristic.
        shrank: bool,
    },
    /// The new node's own level list post-image landed.
    OwnList {
        /// The level whose list was written.
        level: u8,
    },
    /// The meta entry-point/max-level post-image landed.
    MetaUpdate,
    /// The entry's LIVE bit was set.
    PublishLive,
}

/// Test-only crash-probe state (Stage D slice 2): the append-boundary mark
/// log plus an optional crash point (`crash_after` = the number of marks
/// after which the next `barrier` fails the insert as a simulated crash).
/// A `Mutex` so test-harness builds keep the handle `Sync` (the Stage B
/// concurrent tests share it); production builds carry no probe field at
/// all and their `barrier` is a compile-time no-op.
///
/// NOTE: the mark log grows with the insert count (a few entries per
/// insert) — diagnostic plumbing for the test matrix. Production handles
/// never arm the probe and never enable logging, so their `barrier` calls
/// return before any push (the log stays empty); tests bound the log with
/// `probe_clear_marks`.
#[cfg(any(test, feature = "test-harness"))]
#[derive(Debug, Default)]
struct ProbeState {
    /// Append-boundary marks, in order. Filled only while `logging` is on
    /// or a crash point is armed (production: never — see the NOTE above).
    marks: Vec<ProbeMark>,
    /// The simulated crash point: `barrier` fails the insert once this many
    /// marks have been logged.
    crash_after: Option<usize>,
    /// Whether `barrier` records marks without an armed crash point (the
    /// discovery mode of the window-matrix tests). Default off: with no
    /// crash point armed either, `barrier` is a no-op branch.
    logging: bool,
}

/// A live handle on one HNSW index (meta page + directory chain + runtime
/// graph state).
///
/// **Shared-handle form (M6 Stage B, tech-selection §4.1)**: `insert` takes
/// `&self` — the handle is `Sync` and may be shared across writer threads.
/// The runtime state lives behind synchronization: `hwm` is an atomic and
/// the (`entry_point`, `max_level`) pair is ONE packed atomic (a torn read
/// is impossible, review P1-2), the allocation-critical state (rng stream,
/// current node page) sits in the `alloc` mutex (the §4.2 step 0–4 hwm
/// critical section), and `dir_pages` is an `RwLock` (readers clone a
/// snapshot; the DirLink push is the only writer). Latch order (§4.3):
/// alloc mutex → dir_pages → frame pins → WAL writer; the search path
/// never touches the alloc mutex. Declared exception (review P3-1): step
/// 6's resolve self-heal may take `dir_pages.read()` while holding a node
/// latch — no cycle, because dir_pages's only writer (the alloc section)
/// never takes a node latch. The single-writer path is bit-identical to
/// the M5/Stage-A one (the Stage B gate).
///
/// The crash probe exists only in `test-harness` builds (Stage 0's cfg
/// ruling — production builds carry zero probe field and a no-op barrier);
/// there it is a `Mutex` so the TEST build stays `Sync` too (the
/// concurrent smoke tests share `&HnswIndex` across threads).
pub struct HnswIndex {
    meta_page_id: PageId,
    dir_head: PageId,
    /// The creation-pinned parameters (dim/m/ef/metric/selection/rng_seed).
    /// The two RUNTIME fields (`entry_point`/`max_level`) are stale here —
    /// their authoritative values live in the atomics below; `params()`
    /// returns a synthesized copy with the live values filled in.
    params: MetaParams,
    /// The chain-derived high-water mark: the next unallocated NodeId.
    /// Revealed to readers only after the owning DirAppend landed
    /// (position-is-identity; single-store inside the alloc critical
    /// section, §4.2 step 1).
    hwm: AtomicU64,
    /// Runtime meta (mirrors an applied `HnswMetaUpdate`): entry point and
    /// max level PACKED into one AtomicU64 — a single load yields a
    /// consistent pair (Stage B review P1-2: two independent atomics could
    /// tear under Relaxed — a reader could see the OLD entry point with the
    /// NEW max level and drive the greedy descent above the entry's top
    /// level). Layout: high 32 bits = entry_point, low 8 = max_level.
    meta_rt: AtomicU64,
    /// The allocation critical section's state (§4.2 step 0–4): the
    /// level-draw PRNG (stream position == hwm) and the insert-side node
    /// page cache. Readers never touch this.
    alloc: Mutex<AllocInner>,
    /// The directory chain's pages in ordinal order (the NodeId →
    /// `(page, slot)` resolution cache): readers take a snapshot (Stage B
    /// slice 3 adds the self-healing refresh); the DirLink push is the
    /// only writer and happens inside the alloc critical section.
    dir_pages: RwLock<Vec<PageId>>,
    /// The node latch table (M6 §4.1): a process-lifetime, FIXED 4096-shard
    /// `Vec<RwLock<()>>`, NodeId modulo the shard count. The shard count
    /// must stay a compile-time constant — mutual exclusion requires every
    /// writer to map the same NodeId to the same latch, and a resizing
    /// table would break that mapping. One-shot preallocation (~KBs),
    /// zero resize; latches are never destroyed with their nodes (§4.1).
    /// Discipline (§4.3): at most ONE node latch held at a time, taken in
    /// ascending NodeId order — that is the structural deadlock-freedom
    /// argument, loom-verified (slice 4).
    latches: Vec<RwLock<()>>,
    /// Stage D slice-2 crash probe — test-harness builds only (Stage 0's
    /// cfg ruling). A Mutex (not RefCell) so the TEST build stays `Sync`
    /// too — the Stage B concurrent smoke tests share `&HnswIndex` across
    /// threads; the lock cost exists only in test builds (production has
    /// no probe field at all). See [`ProbeMark`]/`ProbeState`.
    #[cfg(any(test, feature = "test-harness"))]
    probe: Mutex<ProbeState>,
}

/// The node latch table's shard count (M6 §4.1): fixed, compile-time —
/// NEVER a function of hwm (a resizing shard count breaks the
/// same-NodeId-same-latch mapping and with it mutual exclusion).
pub(crate) const NODE_LATCH_SHARDS: usize = 4096;

/// The allocation critical section's shared state (§4.2 step 0–4).
struct AllocInner {
    /// The graph's level-draw PRNG (stream position = hwm; drawn exactly
    /// once per validated insert, inside the critical section).
    rng: Xoshiro256StarStar,
    /// The node page insert appends to (re-selected when full).
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

    /// The validated meta parameters (creation-pinned + the LIVE runtime
    /// entry_point/max_level read back from the atomics — a synthesized
    /// snapshot, same value semantics as the M5 plain-field version).
    pub fn params(&self) -> MetaParams {
        let mut p = self.params;
        let (ep, ml) = self.meta_pair();
        p.entry_point = ep;
        p.max_level = ml;
        p
    }

    /// The chain-derived high-water mark: the next unallocated NodeId.
    pub fn hwm(&self) -> u64 {
        self.hwm.load(Ordering::Relaxed)
    }

    /// The packed runtime meta — one load yields the consistent
    /// (entry_point, max_level) pair (P1-2).
    ///
    /// ACQUIRE (Stage B review round 3 P1-1): the writer publishes the
    /// entry point with a RELEASE store AFTER the hwm reveal
    /// (`set_hwm(hwm+1)` precedes `set_entry_point` in program order), so
    /// a reader that acquires ANY entry-point value is guaranteed to see
    /// hwm ≥ that entry point + 1 afterwards — the search/connect paths
    /// read hwm after meta_pair, hence `entry_point < hwm` always holds
    /// on the view they assemble (no torn "new entry point above the
    /// hwm" — that shape would index the visited bitset out of bounds).
    fn meta_pair(&self) -> (u32, u8) {
        let packed = self.meta_rt.load(Ordering::Acquire);
        ((packed >> 8) as u32, packed as u8)
    }

    /// Entry-point NodeId (`NodeId::INVALID.0` for an empty graph).
    pub fn entry_point(&self) -> u32 {
        self.meta_pair().0
    }

    /// Top level of the entry-point node (0 for an empty graph).
    pub fn max_level(&self) -> u8 {
        self.meta_pair().1
    }

    /// The latch serializing writers of one node's entries (M6 §4.1):
    /// `NodeId modulo 4096` — a compile-time-constant shard count, so every
    /// writer maps the same NodeId to the same latch. Readers never take
    /// this (the frame pin covers read exclusion; §4.3).
    fn node_latch(&self, node_id: u32) -> &RwLock<()> {
        &self.latches[(node_id as usize) & (NODE_LATCH_SHARDS - 1)]
    }

    /// TEST-HARNESS ONLY: draw the next node level from the graph's PRNG.
    ///
    /// **Footgun warning** (Stage B review): on a LIVE handle this advances
    /// the shared rng stream WITHOUT allocating a node, breaking the
    /// "stream position == hwm" invariant (§11.1's cross-restart
    /// reproducibility leans on it; reopen re-syncs by skip-ahead, so the
    /// damage heals, but the window in between is wrong). The insert path
    /// does NOT use this (it draws inside the alloc critical section
    /// directly); the only consumers are the rng-lineage verification
    /// tests. Hence the cfg gate.
    #[cfg(any(test, feature = "test-harness"))]
    #[doc(hidden)]
    pub fn next_level(&self) -> u8 {
        self.alloc.lock().rng.next_level(self.params.m)
    }

    /// Controlled high-water-mark write (Stage C's insert advances it
    /// after each DirAppend; not exposed to pg-engine directly — the write
    /// path inside this crate drives it). Single store: readers observe
    /// the increment only after the DirAppend landed (§4.2 step 1).
    pub(crate) fn set_hwm(&self, hwm: u64) {
        self.hwm.store(hwm, Ordering::Relaxed);
    }

    /// Controlled runtime meta update (open-repair and Stage C's
    /// entry-point promotion share this; mirrors an applied
    /// `HnswMetaUpdate`).
    pub(crate) fn set_entry_point(&self, entry_point: u32, max_level: u8) {
        // RELEASE (review round 3 P1-1): pairs with meta_pair's Acquire —
        // everything this insert wrote before the promotion (the hwm
        // reveal included) is visible to any reader that observes this
        // entry point.
        self.meta_rt.store(
            ((entry_point as u64) << 8) | max_level as u64,
            Ordering::Release,
        );
    }

    /// TEST-ONLY (Stage D slice-2 crash-window matrix): a copy of the
    /// probe mark log — every WAL-append boundary the inserts on this
    /// handle have passed, in order.
    #[cfg(any(test, feature = "test-harness"))]
    #[doc(hidden)]
    pub fn probe_marks(&self) -> Vec<ProbeMark> {
        self.probe.lock().marks.clone()
    }

    /// TEST-ONLY: clear the probe mark log (bounds it between the inserts
    /// a test dissects).
    #[cfg(any(test, feature = "test-harness"))]
    #[doc(hidden)]
    pub fn probe_clear_marks(&self) {
        self.probe.lock().marks.clear();
    }

    /// TEST-ONLY: set the simulated crash point — the `barrier` fails the
    /// insert with a fixed "crash probe" error once `crash_after` marks
    /// have been logged (the just-appended record is flushed first: a real
    /// crash's visibility is the durable-record prefix).
    #[cfg(any(test, feature = "test-harness"))]
    #[doc(hidden)]
    pub fn probe_set_crash_after(&self, crash_after: Option<usize>) {
        self.probe.lock().crash_after = crash_after;
    }

    /// TEST-ONLY: switch mark logging on/off (the discovery mode of the
    /// window-matrix tests). Off by default: with no crash point armed a
    /// production handle's `barrier` returns without touching the log.
    #[cfg(any(test, feature = "test-harness"))]
    #[doc(hidden)]
    pub fn probe_set_logging(&self, logging: bool) {
        self.probe.lock().logging = logging;
    }

    /// The crash-probe barrier (Stage D slice 2): log the append-boundary
    /// `mark`; when the log reaches the armed `crash_after` length, flush
    /// through the current END boundary — `current_lsn()` covers the
    /// just-appended record (flush_to's prefix semantics, its rustdoc) —
    /// and fail the insert as a simulated kill -9 (crash visibility = the
    /// durable prefix). Production builds (no probe field) get the no-op
    /// version below — zero branches, zero cost (Stage 0's cfg ruling).
    #[cfg(any(test, feature = "test-harness"))]
    fn barrier(&self, wal_writer: &WalWriter, mark: ProbeMark) -> Result<()> {
        let crash_after = {
            let mut probe = self.probe.lock();
            if probe.crash_after.is_none() && !probe.logging {
                return Ok(());
            }
            probe.marks.push(mark);
            probe.crash_after.filter(|&n| probe.marks.len() == n)
        };
        if let Some(n) = crash_after {
            wal_writer
                .flush_to(wal_writer.current_lsn())
                .map_err(|e| crate::page::storage_err("crash probe: flush", e))?;
            return Err(HnswError::InvalidOperation(format!(
                "stage-d crash probe: simulated crash after mark #{n} ({mark:?})"
            )));
        }
        Ok(())
    }

    /// The production barrier: no probe, no log — a compile-time no-op.
    #[cfg(not(any(test, feature = "test-harness")))]
    #[inline(always)]
    fn barrier(&self, _wal_writer: &WalWriter, _mark: ProbeMark) -> Result<()> {
        Ok(())
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
    /// **Success boundary** (§8.1 boundary ①): flush through the last
    /// record's END boundary (`current_lsn()` after the PublishLive
    /// append) before returning `Ok` — every record of the sequence
    /// precedes it in WAL order. (flush_to's prefix semantics early-exit
    /// on a record's own START LSN right after a group-commit wave —
    /// 2026-09-22 review P1; see its rustdoc.)
    ///
    /// **Err-after-draw semantics** (slice-3 leftover, slice-4 final review
    /// round 3 nano): entry-validation failures change nothing, but a
    /// failure AFTER the step-1 level draw (page alloc, WAL append, apply,
    /// flush) leaves the rng stream one draw ahead of `hwm` — a retry on
    /// the same handle draws the NEXT level, so the resulting level
    /// allocation is legal but not identical to a run where the failed
    /// attempt never happened (the in-memory twin has no post-validation
    /// failure points and never diverges this way). Reopening re-syncs the
    /// stream via skip-ahead (exactly `hwm` draws).
    ///
    /// **Concurrency** (M6 Stage B, tech-selection §4.2): `&self` shared
    /// handle. Steps 1–5 (level draw, page prep, NodeInit, DirAppend, hwm
    /// reveal) run inside the alloc critical section (`self.alloc`) —
    /// NodeId allocation is serialized globally (the known §13 serial
    /// point; batching relief is a Stage-F tuning item). The connect phase
    /// (steps 5–6 lists) runs OUTSIDE the section (node latches arrive
    /// with slice 2); the single-writer path is bit-identical to Stage A.
    pub fn insert(
        &self,
        buffer_pool: &BufferPool,
        wal_writer: &WalWriter,
        vector: &[f32],
    ) -> Result<NodeId> {
        self.insert_impl(buffer_pool, wal_writer, vector, true)
    }

    /// Insert WITHOUT the success-boundary flush (M6 Stage C, tech-selection
    /// §6.2/§10): the transactional form — the caller's commit flush covers
    /// every record of this sequence (WAL prefix property). Utility callers
    /// keep using [`Self::insert`] (the M5 boundary-① shape).
    pub fn insert_unflushed(
        &self,
        buffer_pool: &BufferPool,
        wal_writer: &WalWriter,
        vector: &[f32],
    ) -> Result<NodeId> {
        self.insert_impl(buffer_pool, wal_writer, vector, false)
    }

    fn insert_impl(
        &self,
        buffer_pool: &BufferPool,
        wal_writer: &WalWriter,
        vector: &[f32],
        flush: bool,
    ) -> Result<NodeId> {
        // Step 0: entry validation (§5) + NodeId space (mirrors
        // graph.rs:472 — the INVALID sentinel stays unallocated). The
        // vector checks touch no shared state; the hwm bound is re-checked
        // inside the critical section below (the allocation itself is
        // there).
        if vector.len() != usize::from(self.params.dim) {
            return Err(HnswError::InvalidArgument(format!(
                "insert: vector has {} components, index dim is {} (§3: same index never mixes dimensions)",
                vector.len(),
                self.params.dim
            )));
        }
        self.params.metric.distance(vector, vector)?;

        // Steps 1–5: the alloc critical section (§4.2 step 0–4) — one
        // insert at a time draws a level, prepares pages, initializes the
        // entry, and publishes the directory mapping; hwm is revealed to
        // readers only after the DirAppend landed (position-is-identity).
        let mut alloc = self.alloc.lock();
        let hwm = self.hwm.load(Ordering::Relaxed);
        if hwm == u64::from(u32::MAX) {
            return Err(HnswError::InvalidOperation(
                "NodeId space (u32) exhausted; NodeId::INVALID must stay unallocated (§3)"
                    .to_string(),
            ));
        }

        // Step 1 (level draw) — the rng's stream position is the hwm, so
        // the draw must happen exactly once per successful validation
        // (inside the critical section: the draw and the NodeId allocation
        // are one atomic decision).
        let level = alloc.rng.next_level(self.params.m);
        let node_id = NodeId(hwm as u32);
        let geometry = NodeGeometry {
            dim: self.params.dim,
            m: self.params.m,
            m_max0: self.params.m_max0,
        };
        let entry_len = geometry.entry_size(level);

        // Step 2 (§8.1 step 1): the current node page must have room for
        // this entry — otherwise allocate a fresh one through the init
        // chain (new_page → init → log_page_init; no exceptions, page.rs).
        let node_page_id = match alloc.current_node_page {
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
                    alloc.current_node_page = Some(fresh);
                    self.barrier(wal_writer, ProbeMark::NodePageInit)?;
                    fresh
                }
            }
            None => {
                let fresh = alloc_node_page(buffer_pool, wal_writer)?;
                alloc.current_node_page = Some(fresh);
                self.barrier(wal_writer, ProbeMark::NodePageInit)?;
                fresh
            }
        };

        // Step 3 (§8.1 step 2): the directory tail must have room for the
        // mapping — otherwise link a fresh chain page (its init FPI lands
        // BEFORE the DirLink record, so redo's ordinal/type checks see a
        // real directory page). Inside the alloc critical section, so the
        // DirLink push into the shared dir_pages cache is serialized with
        // every other allocation (latch order §4.3: alloc → dir_pages).
        let mut dir_tail = *self
            .dir_pages
            .read()
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
                let dir_pages_len = self.dir_pages.read().len() as u64;
                let new_dir = alloc_dir_page(buffer_pool, wal_writer, dir_pages_len)?;
                self.barrier(wal_writer, ProbeMark::DirPageInit)?;
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
                self.barrier(wal_writer, ProbeMark::DirLink)?;
                self.dir_pages.write().push(new_dir);
                dir_tail = new_dir;
            }
        }

        // Step 4 (§8.1 step 3): NodeInit — the slot is selected on the same
        // guard that applies the entry (the alloc critical section
        // guarantees the selection cannot go stale between the two: no
        // other inserter can be preparing an entry on this page).
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
            self.barrier(wal_writer, ProbeMark::NodeInit)?;
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
            self.barrier(wal_writer, ProbeMark::DirAppend)?;
        }
        // Reveal the allocation, then leave the critical section: readers
        // may resolve NodeIds below hwm only after the mapping landed.
        self.set_hwm(hwm + 1);
        drop(alloc);

        // Steps 5–6 (M6 §4.2 order — the §8.2 swap of the M5 §8.1 steps
        // 5↔6): search + connect, only when the graph was non-empty BEFORE
        // this insert (an empty graph has nothing to connect to — the
        // first node skips the whole phase, like graph.rs's early return).
        // The node's OWN level lists land FIRST (step 5), the backward
        // edges into OTHER nodes' lists SECOND (step 6). The swap turns
        // the connect-window crash residue from a ghost (inbound edges,
        // EMPTY own lists — a directed dead end that could trap a
        // descending query; M5 1000-round acceptance, round 521) into an
        // orphan (complete own lists, no inbound edges yet — unreachable,
        // hence harmless). Structural dividend (the M6 §4.2 invariant):
        // **reachable ⟹ own lists complete** — a node's first inbound edge
        // can only come from step 6 of its own insert, which runs strictly
        // after its step 5.
        // Steps 6–8 (M6 §4.2): the connect phase + the meta critical
        // section. Concurrency protocol (§4.2 step 4): the meta decision
        // re-reads the meta state INSIDE the critical section (the alloc
        // latch is reused — it already serializes every insert's steps
        // 0–4). A racing first insert on an EMPTY graph can lose the race
        // (it read `entry_point == INVALID` before the winner's MetaUpdate
        // landed); the loser BACKFILLS the connect phase so its node
        // becomes reachable, then re-enters the meta decision under the
        // non-empty rules. max_level only ever rises, never falls (two
        // concurrent promotions take the higher one).
        let mut ran_connect = false;
        if self.entry_point() != NodeId::INVALID.0 {
            self.connect_phases(
                buffer_pool,
                wal_writer,
                vector,
                node_id,
                level,
                node_page_id,
                slot,
                geometry,
            )?;
            ran_connect = true;
        }

        // Step 8 (M5 §8.1 step 7; unchanged by the 5↔6 swap — M6 §4.2
        // step 7): MetaUpdate — a new top-level node becomes the entry
        // point; the first node of an empty graph always does.
        // (Non-empty graph with level <= max_level: no record at all.)
        // The decision AND the record live inside the critical section.
        loop {
            enum MetaDecision {
                Done,
                Backfill,
            }
            let decision = {
                let _meta = self.alloc.lock();
                // One packed load — the pair must be consistent (P1-2).
                let (cur_ep, cur_max) = self.meta_pair();
                if cur_ep != NodeId::INVALID.0 && !ran_connect {
                    // Lost the first-insert race: the winner published the
                    // entry point while this insert was deciding — backfill
                    // the connect phase, then re-decide.
                    MetaDecision::Backfill
                } else {
                    if cur_ep == NodeId::INVALID.0 || level > cur_max {
                        let mut guard = buffer_pool.pin_mut(self.meta_page_id).map_err(|e| {
                            crate::page::storage_err("insert: pin_mut meta page", e)
                        })?;
                        let record =
                            WalRecord::hnsw_meta_update(self.meta_page_id, node_id.0, level)
                                .map_err(|e| {
                                    crate::page::storage_err("insert: encode MetaUpdate", e)
                                })?;
                        let lsn = wal_writer.append(record).map_err(|e| {
                            crate::page::storage_err("insert: WAL append MetaUpdate", e)
                        })?;
                        let page: &mut [u8; PAGE_SIZE] = guard
                            .page_mut()
                            .try_into()
                            .expect("a buffer frame is exactly PAGE_SIZE");
                        crate::apply::apply_meta(page, node_id.0, level);
                        pg_storage::page::set_page_pd_lsn(page, lsn);
                        drop(guard);
                        self.barrier(wal_writer, ProbeMark::MetaUpdate)?;
                        self.set_entry_point(node_id.0, level);
                    }
                    MetaDecision::Done
                }
            };
            match decision {
                MetaDecision::Done => break,
                MetaDecision::Backfill => {
                    self.connect_phases(
                        buffer_pool,
                        wal_writer,
                        vector,
                        node_id,
                        level,
                        node_page_id,
                        slot,
                        geometry,
                    )?;
                    ran_connect = true;
                }
            }
        }

        // Step 9 (M5 §8.1 step 8 = M6 §4.2 step 8): PublishLive — the
        // state post-image lands
        // only after all of the node's own level lists are written (v1.7).
        // Under the victim's node latch (§4.3): a concurrent delete's
        // tombstone on this entry serializes against this flip.
        {
            let _node_guard = self.node_latch(node_id.0).write();
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
            self.barrier(wal_writer, ProbeMark::PublishLive)?;
        }

        // Step 10 (§8.1 boundary ①): the success boundary — every record
        // of the sequence is durable before Ok escapes. Flush through the
        // current END boundary (`current_lsn()` covers the PublishLive
        // record): flush_to's prefix semantics early-exit on a record's
        // own start LSN right after a group-commit wave (its rustdoc).
        // Skipped in the transactional form (insert_unflushed): the
        // caller's commit flush covers the whole prefix (M6 §6.2/§10).
        if flush {
            wal_writer
                .flush_to(wal_writer.current_lsn())
                .map_err(|e| crate::page::storage_err("insert: success-boundary flush", e))?;
        }
        Ok(node_id)
    }

    /// The connect phase (M6 §4.2 steps 5–6), extracted so the
    /// first-insert backfill path can run it after the meta re-check:
    /// beam-search each level, write the node's OWN lists (step 5), then
    /// the backward edges into the neighbors' lists (step 6). Reads the
    /// CURRENT entry point / max level (the backfill caller invokes it
    /// after a meta race, when both may have moved). Runs OUTSIDE the
    /// alloc critical section; the node-latch discipline inside is §4.3.
    #[allow(clippy::too_many_arguments)]
    fn connect_phases(
        &self,
        buffer_pool: &BufferPool,
        wal_writer: &WalWriter,
        vector: &[f32],
        node_id: NodeId,
        level: u8,
        node_page_id: PageId,
        slot: u16,
        geometry: NodeGeometry,
    ) -> Result<()> {
        // The new node's own per-level selections, written in step 5. Levels
        // above the searched range stay empty and need NO record — NodeInit
        // already reserved them with count = 0, so an empty list is the
        // zero content (2026-09-20, slice 3 note: no record for empties).
        let mut own_lists: Vec<Vec<u32>> = vec![Vec::new(); usize::from(level) + 1];
        // One packed load — searched_top and the descent must read the
        // SAME meta generation (P1-2).
        let (connect_ep, connect_max) = self.meta_pair();
        let searched_top = level.min(connect_max);
        {
            // The connect phase runs OUTSIDE the alloc critical
            // section; the graph view uses a directory snapshot
            // (self-healing refresh is Stage B slice 3 — beneath the
            // swap the snapshot can only lag a DirLink, and the search
            // below touches only nodes < hwm whose entries predate it).
            let dir_snapshot = self.dir_pages.read().clone();
            let g = PagedGraph::new(
                buffer_pool,
                dir_snapshot,
                Some(&self.dir_pages),
                self.hwm(),
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
            // The entry point is RUNTIME state — read the atomic, not
            // the creation-time params snapshot.
            let mut ep = NodeId(connect_ep);
            for l in ((level + 1)..=connect_max).rev() {
                ep = crate::graph::search_layer(&g, vector, &[ep], 1, l)[0].id;
            }

            // Phase 2 (search half): beam-search + select per level
            // and STASH the selections — no page writes in this loop,
            // so every level's selection observes the identical
            // pre-insert graph. (The M5 order interleaved the
            // backward-edge writes here; they never influenced later
            // levels' searches — a level-l search walks level-l edges
            // only — so the selections are bit-identical across the
            // swap. Same for the shrink inputs in step 6: each
            // neighbor's list is read once and sees only pre-insert
            // content, and the step-5 own-list writes touch only the
            // NEW node's page.)
            for l in (0..=searched_top).rev() {
                let w = crate::graph::search_layer(&g, vector, &[ep], ef_construction, l);
                let neighbors = crate::graph::select_neighbors(&g, selection, &w, m);
                own_lists[usize::from(l)] = neighbors.iter().map(|n| n.0).collect();
                ep = w[0].id; // nearest result carries the descent
            }

            // Step 5 (M6 §4.2): the new node's own level lists —
            // BEFORE any backward edge lands (the swap, see the
            // steps-5–6 comment above). The victim's own node latch
            // serializes writers of THIS node (a concurrent insert
            // connecting TO it takes the same latch in step 6); at
            // most one node latch held at a time (§4.3).
            for (l, list) in own_lists
                .iter()
                .enumerate()
                .take(usize::from(searched_top) + 1)
            {
                if list.is_empty() {
                    continue; // empty list == the NodeInit zero content
                }
                let _node_guard = self.node_latch(node_id.0).write();
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
                self.barrier(wal_writer, ProbeMark::OwnList { level: l as u8 })?;
            }

            // Step 6 (M6 §4.2): backward edges into the neighbors'
            // lists. The per-level (descending) and per-neighbor order
            // matches the M5 interleaved loop exactly, so this half of
            // the record stream is unchanged by the swap.
            for l in (0..=searched_top).rev() {
                for &nb_raw in &own_lists[usize::from(l)] {
                    let nb = NodeId(nb_raw);
                    let (nb_page, nb_slot) = g.resolve(nb)?;
                    // The neighbor's node latch covers the whole
                    // read-modify-write of its level-l list (§4.3: one
                    // node latch at a time; readers are excluded by the
                    // frame pins inside).
                    let _node_guard = self.node_latch(nb.0).write();
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
                    let shrank = list.len() > m_max(l);
                    let final_list = if shrank {
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
                    self.barrier(wal_writer, ProbeMark::NeighborEdge { level: l, shrank })?;
                }
            }
        }
        Ok(())
    }

    /// Delete (tombstone) a LIVE node — M6 Stage B (tech-selection §8.1):
    /// the graph-side delete primitive, one `HnswNodeTombstone` record +
    /// the in-place state-bit flip under the node's latch (§4.3: same
    /// discipline as insert's step 5/6/9 — one node latch at a time).
    ///
    /// **Idempotent re-entry** (M6 §6.2's post-commit cleanup is retryable):
    /// deleting an already-tombstoned node is a no-op Ok (no record).
    /// Deleting an INITIALIZING node is loud — it is a crash residue (the
    /// vacuum channel owns it), never a user-visible row. A NodeId at or
    /// above the hwm is InvalidArgument.
    ///
    /// The success boundary mirrors insert's: the tombstone record is
    /// durable (flush through the end boundary) before Ok escapes. The
    /// tombstone bit is NOT a search-time visibility filter (§8.1③/M6 §4.2:
    /// state bits never gate traversal); row-level visibility filtering is
    /// the heap/MVCC layer's job (Stage C/D).
    pub fn delete(
        &self,
        buffer_pool: &BufferPool,
        wal_writer: &WalWriter,
        node_id: NodeId,
    ) -> Result<()> {
        self.delete_impl(buffer_pool, wal_writer, node_id, true)
    }

    fn delete_impl(
        &self,
        buffer_pool: &BufferPool,
        wal_writer: &WalWriter,
        node_id: NodeId,
        flush: bool,
    ) -> Result<()> {
        if u64::from(node_id.0) >= self.hwm() {
            return Err(HnswError::InvalidArgument(format!(
                "delete: node {} is at/above the high-water mark {} — never allocated",
                node_id.0,
                self.hwm()
            )));
        }
        let dir_snapshot = self.dir_pages.read().clone();
        let g = PagedGraph::new(
            buffer_pool,
            dir_snapshot,
            Some(&self.dir_pages),
            self.hwm(),
            self.params.dim,
            self.params.m,
            self.params.m_max0,
            self.params.metric,
        );
        let (page_id, slot) = g.resolve(node_id)?;

        // One node latch, one page touch (§4.3).
        let _node_guard = self.node_latch(node_id.0).write();
        let mut guard = buffer_pool
            .pin_mut(page_id)
            .map_err(|e| crate::page::storage_err("delete: pin_mut node page", e))?;
        {
            let page: &[u8; PAGE_SIZE] = guard
                .page()
                .try_into()
                .expect("a buffer frame is exactly PAGE_SIZE");
            if crate::apply::entry_is_tombstoned(page, slot, self.params.dim)? {
                return Ok(()); // already tombstoned — idempotent no-op
            }
            if !crate::apply::entry_is_live(page, slot, self.params.dim)? {
                return Err(HnswError::InvalidOperation(format!(
                    "delete: node {node_id:?} is INITIALIZING (a crash residue — the vacuum channel owns it, not the delete path)"
                )));
            }
        }
        let record = WalRecord::hnsw_node_tombstone(
            self.meta_page_id,
            page_id,
            slot,
            node_id.0,
            self.params.dim,
        )
        .map_err(|e| crate::page::storage_err("delete: encode Tombstone", e))?;
        let lsn = wal_writer
            .append(record)
            .map_err(|e| crate::page::storage_err("delete: WAL append Tombstone", e))?;
        let page: &mut [u8; PAGE_SIZE] = guard
            .page_mut()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        crate::apply::apply_tombstone(page, slot, self.params.dim)?;
        pg_storage::page::set_page_pd_lsn(page, lsn);
        drop(guard);
        drop(_node_guard);

        // Success boundary (same shape as insert's): durable before Ok.
        // Skipped in the transactional form (the commit flush covers it).
        if flush {
            wal_writer
                .flush_to(wal_writer.current_lsn())
                .map_err(|e| crate::page::storage_err("delete: success-boundary flush", e))?;
        }
        Ok(())
    }

    /// k-nearest-neighbor search on the page-resident graph (paper
    /// Algorithm 5 = greedy descent + Algorithm 2 level-0 beam) — Phase 2
    /// M5 Stage C slice 4. Mirrors `graph.rs`'s `Hnsw::search` semantics
    /// and error-priority order exactly (dim mismatch → §5 entry validation
    /// → ef >= k → empty graph → k == 0), so the cross-form parity tests
    /// pin identical outputs AND identical failure variants.
    ///
    /// Returns up to `k` hits as `(NodeId, distance)`, sorted ascending by
    /// `(distance, NodeId)` (§4.1 prerequisite ①). `ef` is the beam width;
    /// `None` uses the meta-pinned `ef_search_default` (creation-pinned,
    /// §4.4). The only per-query invariant is `ef >= k`.
    ///
    /// **INITIALIZING semantics** (§8.1③, frozen): the state bit is NOT
    /// checked anywhere on the search path — an INITIALIZING node has its
    /// vector, is recallable, and is a legal answer. That is deliberate
    /// (the bit is a recovery-progress marker, not a visibility filter);
    /// the `search_recalls_initializing_nodes` test pins it.
    ///
    /// **Premises** (paged.rs module header): directory corruption beneath
    /// the open protocol's validation is fail-stop (panic with the premise
    /// in the message), never a wrong answer. (M6 Stage B: the serving path
    /// is shared — `&self` readers and writers interleave freely; the
    /// crash-probe instrumentation and the single-writer bitwise pin are
    /// unaffected.)
    pub fn search(
        &self,
        buffer_pool: &BufferPool,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<(NodeId, f64)>> {
        // Entry validation, same order as the in-memory twin (graph.rs
        // Hnsw::search): dimension first, then the metric self-distance
        // funnel (finiteness + cosine zero vector, §5).
        if query.len() != usize::from(self.params.dim) {
            return Err(HnswError::InvalidArgument(format!(
                "search: query has {} components, index dim is {} (§3: same index never mixes dimensions)",
                query.len(),
                self.params.dim
            )));
        }
        self.params.metric.distance(query, query)?;
        let ef = ef.unwrap_or(self.params.ef_search_default as usize);
        if ef < k {
            return Err(HnswError::InvalidArgument(format!(
                "ef = {ef} < k = {k} (§4.4: the beam must fit the result set; this is the only per-query invariant)"
            )));
        }
        // One packed load for the whole descent — the entry point and
        // max level must come from the SAME meta generation (P1-2).
        let (search_ep, search_max) = self.meta_pair();
        if search_ep == NodeId::INVALID.0 {
            return Ok(Vec::new()); // empty graph
        }
        if k == 0 {
            return Ok(Vec::new());
        }

        // Directory snapshot + hwm: the reader's graph view (Stage B slice
        // 3 adds the self-healing refresh for a snapshot that lags a
        // concurrent DirLink — NodeIds below the hwm READ HERE resolve
        // within this snapshot, since their DirAppends precede the hwm
        // store; ids allocated concurrently are above this hwm and
        // invisible to this search).
        let dir_snapshot = self.dir_pages.read().clone();
        let g = PagedGraph::new(
            buffer_pool,
            dir_snapshot,
            Some(&self.dir_pages),
            self.hwm(),
            self.params.dim,
            self.params.m,
            self.params.m_max0,
            self.params.metric,
        );
        // Greedy descent (ef = 1) through the upper layers, then a beam
        // search of width ef on level 0 (Algorithm 5).
        let mut ep = NodeId(search_ep);
        for l in (1..=search_max).rev() {
            ep = crate::graph::search_layer(&g, query, &[ep], 1, l)[0].id;
        }
        let w = crate::graph::search_layer(&g, query, &[ep], ef, 0);
        Ok(w.into_iter().take(k).map(|c| (c.id, c.dist)).collect())
    }

    /// Run the §11.3 post-recovery audit against this index (Stage D
    /// slice 1, [`crate::audit`]). The audit re-walks the directory chain
    /// and re-reads every entry ITSELF — it deliberately trusts none of
    /// this handle's caches (`dir_pages`, `hwm`, …), so a broken cache
    /// cannot mask on-disk corruption.
    pub fn audit(&self, buffer_pool: &BufferPool) -> Result<crate::audit::AuditReport> {
        crate::audit::audit_index(buffer_pool, self.meta_page_id)
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
        hwm: AtomicU64::new(0),
        meta_rt: AtomicU64::new(((params.entry_point as u64) << 8) | params.max_level as u64),
        alloc: Mutex::new(AllocInner {
            rng: Xoshiro256StarStar::new(rng_seed),
            current_node_page: None,
        }),
        dir_pages: RwLock::new(vec![dir_head]),
        latches: (0..NODE_LATCH_SHARDS).map(|_| RwLock::new(())).collect(),
        #[cfg(any(test, feature = "test-harness"))]
        probe: Mutex::new(ProbeState::default()),
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
    //
    // 2026-09-22, Stage D slice 2 (the crash matrix's `empty_linked_tail`
    // window caught this): the last ENTRY lives on page
    // `pages[(hwm-1) / DIR_ENTRIES_PER_PAGE]` — NOT necessarily the chain
    // tail (a fresh tail linked by a crashed insert is empty, and
    // `dir_entry(tail, (hwm-1) % 813)` would read past its count = 0).
    let current_node_page = if chain.hwm > 0 {
        let last = chain.hwm - 1;
        let per_page = u64::from(crate::dir::DIR_ENTRIES_PER_PAGE);
        let entry_page_id = chain.pages[(last / per_page) as usize];
        let idx = (last % per_page) as u32;
        let guard = buffer_pool
            .pin(entry_page_id)
            .map_err(|e| crate::page::storage_err("open: pin directory page of last entry", e))?;
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
            hwm: AtomicU64::new(chain.hwm),
            meta_rt: AtomicU64::new(((params.entry_point as u64) << 8) | params.max_level as u64),
            alloc: Mutex::new(AllocInner {
                rng,
                current_node_page,
            }),
            dir_pages: RwLock::new(chain.pages),
            latches: (0..NODE_LATCH_SHARDS).map(|_| RwLock::new(())).collect(),
            #[cfg(any(test, feature = "test-harness"))]
            probe: Mutex::new(ProbeState::default()),
        },
        warnings,
    })
}

/// Read an index's creation-pinned parameters off its meta page into an
/// [`ExpectedParams`] (M6 Stage C): the self-evidencing form for engine
/// callers that need to open a handle without carrying the parameters
/// themselves (the DML hook's lazy open). The expectation check against
/// these params is trivially satisfied; structural validation still runs
/// inside [`open`].
pub fn expected_params_of(
    buffer_pool: &BufferPool,
    meta_page_id: PageId,
) -> Result<ExpectedParams> {
    let guard = buffer_pool
        .pin(meta_page_id)
        .map_err(|e| crate::page::storage_err("expected_params_of: pin meta page", e))?;
    let page: &[u8; PAGE_SIZE] = guard
        .page()
        .try_into()
        .expect("a buffer frame is exactly PAGE_SIZE");
    let p = crate::meta::read_meta(page)?;
    Ok(ExpectedParams {
        dim: p.dim,
        m: p.m,
        m_max0: p.m_max0,
        ef_construction: p.ef_construction,
        ef_search_default: p.ef_search_default,
        metric: p.metric,
        selection: p.selection,
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
        let outcome = open(
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
        let dir_snapshot = index.dir_pages.read().clone();
        let g = PagedGraph::new(
            pool,
            dir_snapshot,
            None,
            index.hwm(),
            index.params.dim,
            index.params.m,
            index.params.m_max0,
            index.params.metric,
        );
        (0..index.hwm())
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
        let index = create(
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

    /// M6 Stage B: the delete (tombstone) primitive — state flip, audit
    /// counters, idempotent re-entry, and the INITIALIZING/unallocated
    /// rejections. The tombstone bit is NOT a search filter (§8.1③/M6
    /// §4.2): a tombstoned node still recalls itself.
    #[test]
    fn delete_tombstones_live_nodes_and_is_idempotent() {
        let dir = fresh_dir("delete-basic");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::new(4, 8, 16, 4).unwrap(),
            4,
            Metric::L2,
            NeighborSelection::Heuristic,
            0xC0FFEE,
        )
        .unwrap();
        let vectors = det_vectors(0xDEC0DE, 10, 4);
        for v in &vectors {
            index
                .insert(engine.buffer_pool(), engine.wal_writer(), v)
                .unwrap();
        }

        index
            .delete(engine.buffer_pool(), engine.wal_writer(), NodeId(3))
            .unwrap();
        let report = index.audit(engine.buffer_pool()).unwrap();
        assert_eq!(report.tombstoned_count, 1, "one tombstone");
        // Idempotent re-entry (M6 §6.2's post-commit cleanup is retryable):
        // the second delete is a no-op Ok, no new tombstone.
        index
            .delete(engine.buffer_pool(), engine.wal_writer(), NodeId(3))
            .unwrap();
        let report2 = index.audit(engine.buffer_pool()).unwrap();
        assert_eq!(report2.tombstoned_count, 1, "idempotent: still one");

        // Unallocated NodeId is loud.
        let err = index
            .delete(engine.buffer_pool(), engine.wal_writer(), NodeId(10))
            .unwrap_err();
        assert!(err.to_string().contains("high-water mark"), "{err}");

        // The state bit is not a search predicate: node 3 still recalls
        // itself at distance 0.
        let hits = index
            .search(engine.buffer_pool(), &vectors[3], 1, Some(10))
            .unwrap();
        assert_eq!(hits[0].0, NodeId(3));
        assert_eq!(hits[0].1, 0.0);

        engine.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Deleting an INITIALIZING node (a crash residue) is loud — the
    /// vacuum channel owns it, never the delete path. The residue is built
    /// with the crash probe: the insert dies right before PublishLive.
    #[test]
    fn delete_rejects_initializing_nodes() {
        let dir = fresh_dir("delete-initializing");
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::new(4, 8, 16, 4).unwrap(),
            4,
            Metric::L2,
            NeighborSelection::Heuristic,
            0xBEEF,
        )
        .unwrap();
        let meta_page_id = index.meta_page_id();
        let vectors = det_vectors(0x1234, 1, 4);
        // Crash the FIRST insert right after DirAppend — the
        // probe_log_shape-pinned sequence is [NodePageInit, NodeInit,
        // DirAppend, MetaUpdate, PublishLive], so crash_after = 3 leaves
        // node 0 published on the page and INITIALIZING (no MetaUpdate,
        // no PublishLive). The in-memory hwm only advances AFTER the
        // DirAppend barrier, so the handle still reads hwm = 0 — reopen
        // re-derives it from the chain (hwm = 1, entry point repaired).
        index.probe_set_crash_after(Some(3));
        let r = index.insert(engine.buffer_pool(), engine.wal_writer(), &vectors[0]);
        assert!(r.is_err(), "the crash probe must fail the insert");
        drop(index);
        let outcome = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &ExpectedParams {
                dim: 4,
                m: 4,
                m_max0: 8,
                ef_construction: 16,
                ef_search_default: 4,
                metric: Metric::L2,
                selection: NeighborSelection::Heuristic,
            },
        )
        .unwrap();
        let index = outcome.index;
        assert_eq!(index.hwm(), 1, "reopen re-derives the published mapping");

        // The victim is INITIALIZING on the page; delete must refuse it.
        let err = index
            .delete(engine.buffer_pool(), engine.wal_writer(), NodeId(0))
            .unwrap_err();
        assert!(err.to_string().contains("INITIALIZING"), "{err}");

        engine.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// M6 Stage B review P1-2: the packed meta atomic — a reader must
    /// NEVER observe a torn (entry_point, max_level) pair. One writer
    /// stores strictly-matching pairs; concurrent readers must always read
    /// a matching pair. (Sentinel caveat: on x86/TSO this stays green even
    /// if the packing regresses — its discriminating power is on weakly
    /// ordered hardware; the real guarantee is the single-atomic
    /// structure.)
    #[test]
    fn meta_pair_never_reads_torn() {
        let dir = fresh_dir("meta-pair");
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
        let index = std::sync::Arc::new(index);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        std::thread::scope(|s| {
            let writer_index = std::sync::Arc::clone(&index);
            let writer_stop = std::sync::Arc::clone(&stop);
            s.spawn(move || {
                for i in 1..2000u32 {
                    writer_index.set_entry_point(i, (i % 60) as u8);
                }
                writer_stop.store(true, Ordering::Relaxed);
            });
            let reader_index = std::sync::Arc::clone(&index);
            s.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let (ep, ml) = reader_index.meta_pair();
                    assert_eq!(
                        ml,
                        (ep % 60) as u8,
                        "torn pair: entry_point {ep} with max_level {ml}"
                    );
                }
            });
        });
        engine.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Round-3 P1-1 extension: the writer follows the REAL publication
    /// order (hwm reveal first, entry point last — insert's program
    /// order); a reader acquiring the entry point must observe
    /// `hwm > entry_point` on the hwm read it does afterwards (the
    /// search/connect view assembly order). A torn view the other way
    /// (new entry point above the hwm) would index the search bitset out
    /// of bounds. (Sentinel caveat: x86/TSO keeps this green even without
    /// the Acquire/Release pair — its discriminating power is on weakly
    /// ordered hardware; the real guarantee is the release-sequence
    /// structure.)
    #[test]
    fn entry_point_never_ahead_of_hwm() {
        let dir = fresh_dir("ep-hwm-pair");
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
        let index = std::sync::Arc::new(index);
        std::thread::scope(|s| {
            let writer = std::sync::Arc::clone(&index);
            s.spawn(move || {
                for i in 0..2000u64 {
                    // The real publication order: hwm reveal, then the
                    // entry point (insert's step 5 boundary → step 8).
                    writer.set_hwm(i + 1);
                    writer.set_entry_point(i as u32, 0);
                }
            });
            let reader = std::sync::Arc::clone(&index);
            s.spawn(move || {
                for _ in 0..50_000 {
                    let (ep, _ml) = reader.meta_pair();
                    let hwm = reader.hwm();
                    if ep != NodeId::INVALID.0 {
                        assert!(
                            u64::from(ep) < hwm,
                            "torn view: entry_point {ep} at/above hwm {hwm}"
                        );
                    }
                }
            });
        });
        engine.shutdown();
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

        let dir_snapshot = index.dir_pages.read().clone();
        let g = PagedGraph::new(
            engine.buffer_pool(),
            dir_snapshot,
            None,
            index.hwm(),
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
        let index = create(
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
            index.dir_pages.read().len(),
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
        let index = open(
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
        let dir_snapshot = index.dir_pages.read().clone();
        let g = PagedGraph::new(
            engine.buffer_pool(),
            dir_snapshot,
            None,
            index.hwm(),
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
        let index = create(
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

        let cosine = create(
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

    // -----------------------------------------------------------------
    // 2026-09-21, Stage C slice 4: the page-resident search path. The
    // load-bearing pin is the cross-form parity test — every query/grid
    // point must match the in-memory twin to the last f64 bit, and the
    // error variants must match too.
    // -----------------------------------------------------------------

    /// Build the in-memory and page-resident twins over the same vectors.
    fn build_twins(
        tag: &str,
        dim: u16,
        params: HnswParams,
        metric: Metric,
        seed: u64,
        vectors: &[Vec<f32>],
    ) -> (crate::graph::Hnsw, HnswIndex, StorageEngine, PathBuf) {
        let dir = fresh_dir(tag);
        let config = StorageConfig::new(&dir);
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = create(
            engine.buffer_pool(),
            engine.wal_writer(),
            params,
            dim,
            metric,
            NeighborSelection::Heuristic,
            seed,
        )
        .unwrap();
        let mut mem = crate::graph::Hnsw::new(dim, metric, params, seed).unwrap();
        for v in vectors {
            let m_id = mem.insert(v).unwrap();
            let p_id = index
                .insert(engine.buffer_pool(), engine.wal_writer(), v)
                .unwrap();
            assert_eq!(m_id, p_id);
        }
        (mem, index, engine, dir)
    }

    /// `(NodeId, to_bits)` pairs — the exact comparison currency.
    fn bits(hits: &[(NodeId, f64)]) -> Vec<(u32, u64)> {
        hits.iter().map(|(id, d)| (id.0, d.to_bits())).collect()
    }

    /// One parity configuration: a query grid (queries × k × ef) must
    /// produce the same OUTCOME on both forms — bitwise-identical hits, or
    /// the same error variant (ef < k is a legal grid point: both sides
    /// must fail alike; the ef = None column is only satisfiable when the
    /// meta default reaches k).
    fn assert_search_parity(tag: &str, dim: u16, params: HnswParams, metric: Metric, n: usize) {
        let vectors = det_vectors(0xDEC0DE, n, dim as usize);
        let queries = det_vectors(0x0B5E55, 4, dim as usize);
        let (mem, index, engine, dir) = build_twins(tag, dim, params, metric, 0xC0FFEE, &vectors);
        let pool = engine.buffer_pool();
        for (qi, q) in queries.iter().enumerate() {
            for k in [1usize, 3, 10] {
                // ef grid: default / exactly k / one above / generous.
                for ef in [None, Some(k), Some(k + 1), Some(500)] {
                    let ctx = format!("{tag} query {qi} k={k} ef={ef:?}");
                    match (mem.search(q, k, ef), index.search(pool, q, k, ef)) {
                        (Ok(m_hits), Ok(p_hits)) => assert_eq!(
                            bits(&p_hits),
                            bits(&m_hits),
                            "{ctx}: page-resident must equal in-memory bitwise"
                        ),
                        (Err(m_err), Err(p_err)) => assert_eq!(
                            std::mem::discriminant(&m_err),
                            std::mem::discriminant(&p_err),
                            "{ctx}: error variant parity: in-memory {m_err}, paged {p_err}"
                        ),
                        (m, p) => panic!(
                            "{ctx}: outcome mismatch — in-memory ok = {}, paged ok = {}",
                            m.is_ok(),
                            p.is_ok()
                        ),
                    }
                }
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn paged_search_matches_in_memory_bitwise() {
        // dim=4 / N=200 at m=4 (the slice-3 parity shape), all three
        // metrics…
        for metric in [Metric::L2, Metric::Cosine, Metric::InnerProduct] {
            let params = HnswParams::new(4, 8, 16, 4).unwrap();
            assert_search_parity("parity-m4", 4, params, metric, 200);
        }
        // …and the dim=1 / N=900 double-overflow shape (node pages AND the
        // directory chain grow past one page).
        let params = HnswParams::new(2, 2, 4, 2).unwrap();
        assert_search_parity("parity-overflow", 1, params, Metric::L2, 900);
    }

    /// Error parity: the same bad input fails with the same error variant
    /// on both forms (the error-priority order is part of the contract).
    #[test]
    fn paged_search_error_parity() {
        let params = HnswParams::new(4, 8, 16, 4).unwrap();
        let vectors = det_vectors(0xDEC0DE, 50, 4);
        let (mem, index, engine, dir) =
            build_twins("parity-err", 4, params, Metric::L2, 0xC0FFEE, &vectors);
        let pool = engine.buffer_pool();
        let cases: Vec<(Vec<f32>, usize, Option<usize>)> = vec![
            (vec![1.0, 2.0], 1, None),         // dim mismatch
            (vec![f32::NAN; 4], 1, None),      // non-finite
            (vec![f32::INFINITY; 4], 1, None), // ±inf
            (vec![1.0; 4], 3, Some(2)),        // ef < k
        ];
        for (q, k, ef) in &cases {
            let m = mem.search(q, *k, *ef).unwrap_err();
            let p = index.search(pool, q, *k, *ef).unwrap_err();
            assert_eq!(
                std::mem::discriminant(&m),
                std::mem::discriminant(&p),
                "error variant parity: in-memory {m}, paged {p}"
            );
        }

        // Compound faults (2026-09-21, slice-4 adversarial review P3-1):
        // a discriminant-only comparison cannot see the error-PRIORITY
        // order — two faults at once must fail on the FIRST check of the
        // frozen order (dim → §5 finiteness → ef >= k) on BOTH forms, and
        // the message fragment pins which check fired (a same-variant
        // different-message divergence would pass the loop above).
        let compound: Vec<(Vec<f32>, usize, Option<usize>, &str)> = vec![
            (vec![1.0, 2.0], 3, Some(2), "components"), // dim + ef<k: dim wins
            (vec![f32::NAN; 4], 3, Some(2), "non-finite"), // NaN + ef<k: §5 wins
            (vec![f32::NAN, 7.0], 3, Some(2), "components"), // NaN + dim: dim wins
        ];
        for (q, k, ef, fragment) in &compound {
            let m = mem.search(q, *k, *ef).unwrap_err();
            let p = index.search(pool, q, *k, *ef).unwrap_err();
            assert_eq!(std::mem::discriminant(&m), std::mem::discriminant(&p));
            assert!(
                m.to_string().contains(fragment),
                "in-memory error must name the first-failed check ({fragment}): {m}"
            );
            assert!(
                p.to_string().contains(fragment),
                "paged error must name the first-failed check ({fragment}): {p}"
            );
        }

        // Cosine zero query: ZeroVector on both forms.
        let (mem_c, index_c, engine_c, dir_c) = build_twins(
            "parity-err-cos",
            4,
            params,
            Metric::Cosine,
            0xC0FFEE,
            &vectors,
        );
        let m = mem_c.search(&[0.0; 4], 1, None).unwrap_err();
        let p = index_c
            .search(engine_c.buffer_pool(), &[0.0; 4], 1, None)
            .unwrap_err();
        assert!(matches!(m, HnswError::ZeroVector));
        assert!(matches!(p, HnswError::ZeroVector));

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir_c);
    }

    /// §8.1③ frozen semantics: the state bit is NOT a search filter — an
    /// INITIALIZING node (the §8.2 crash-window residue: fully written,
    /// never PublishLive'd) has its vector, is recallable, and is a legal
    /// answer. Hand-built crash residue in image form (the
    /// open_repairs_entry_point_and_skips_ahead pattern).
    #[test]
    fn search_recalls_initializing_nodes() {
        let dir = fresh_dir("initializing-recall");
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
            // Residue: node 0 LIVE with level-0 neighbor [1]; node 1
            // INITIALIZING (crash between SetNeighbors and PublishLive);
            // directory maps both; meta entry_point = 0.
            let node_page_id = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                let page_id = guard.page_id();
                let page = page_mut(&mut guard);
                init_node_page(page);
                log_page_init(engine.wal_writer(), page_id, page).unwrap();
                page_id
            };
            {
                let mut guard = engine.buffer_pool().pin_mut(node_page_id).unwrap();
                let page = page_mut(&mut guard);
                let s0 = apply::append_node(page, 0, 0, geo, &[1.0; DIM as usize]).unwrap();
                let _s1 = apply::append_node(page, 1, 0, geo, &[2.0; DIM as usize]).unwrap();
                apply::set_neighbors(page, s0, geo, 0, &[1]).unwrap();
                apply::publish_live(page, s0, DIM).unwrap();
                // node 1 deliberately stays INITIALIZING.
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(node_page_id, page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            {
                let mut guard = engine.buffer_pool().pin_mut(index.dir_head()).unwrap();
                let page = page_mut(&mut guard);
                apply::dir_append(page, 0, node_page_id, 0).unwrap();
                apply::dir_append(page, 1, node_page_id, 1).unwrap();
                let lsn = engine
                    .wal_writer()
                    .append(WalRecord::full_page_image(index.dir_head(), page.to_vec()).unwrap())
                    .unwrap();
                pg_storage::page::set_page_pd_lsn(page, lsn);
            }
            {
                let mut guard = engine.buffer_pool().pin_mut(index.meta_page_id()).unwrap();
                let page = page_mut(&mut guard);
                apply::apply_meta(page, 0, 0);
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
        let index = open(
            engine.buffer_pool(),
            engine.wal_writer(),
            meta_page_id,
            &make_expected(),
        )
        .unwrap()
        .index;
        assert_eq!(index.hwm(), 2);

        // Premise: node 1 is INITIALIZING on disk.
        let dir_snapshot = index.dir_pages.read().clone();
        let g = PagedGraph::new(
            engine.buffer_pool(),
            dir_snapshot,
            None,
            index.hwm(),
            DIM,
            16,
            32,
            Metric::L2,
        );
        let (npage, nslot) = g.resolve(NodeId(1)).unwrap();
        let guard = engine.buffer_pool().pin(npage).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert!(!apply::entry_is_live(page, nslot, DIM).unwrap());
        drop(guard);

        // The frozen semantics: search for node 1's own vector must recall
        // node 1 — as the nearest hit (L2 self-distance 0) — with no state
        // check anywhere on the path.
        let hits = index
            .search(engine.buffer_pool(), &vec![2.0; DIM as usize], 2, Some(10))
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0, NodeId(1));
        assert_eq!(hits[0].1, 0.0);
        assert_eq!(hits[1].0, NodeId(0));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Early-exit pins: an empty graph answers `[]` (any ef), and k = 0
    /// answers `[]` on a non-empty graph.
    #[test]
    fn search_early_exits() {
        let dir = fresh_dir("search-early-exit");
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
        // Empty graph.
        assert_eq!(
            index
                .search(engine.buffer_pool(), &vec![1.0; DIM as usize], 5, None)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            index
                .search(engine.buffer_pool(), &vec![1.0; DIM as usize], 5, Some(5))
                .unwrap(),
            vec![]
        );
        // k = 0 on a non-empty graph.
        index
            .insert(
                engine.buffer_pool(),
                engine.wal_writer(),
                &vec![1.0; DIM as usize],
            )
            .unwrap();
        assert_eq!(
            index
                .search(engine.buffer_pool(), &vec![1.0; DIM as usize], 0, Some(0))
                .unwrap(),
            vec![]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
