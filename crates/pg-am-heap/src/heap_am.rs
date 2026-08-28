//! Heap access method: single-threaded CRUD over slotted pages (M2a Stage I),
//! with the Stage K page chain and AM-internal `t_xmin` stamping.
//!
//! [`HeapAM`] wires the in-memory page/tuple primitives to the storage engine's
//! buffer pool and WAL. Every page mutation follows the same discipline:
//!
//! 1. pin the page for write (`pin_mut` / `new_page`) — the content write latch
//!    serializes all mutators of that page and blocks flushes;
//! 2. append the WAL record and obtain its LSN;
//! 3. apply the change to the page;
//! 4. stamp the page's `pd_lsn` (authoritative, `page[0..8]`) to `record.lsn`;
//! 5. drop the guard, which marks the frame dirty.
//!
//! Because `flush_frame` takes a `content.read()` lock and fsyncs the WAL up to
//! the page's `pd_lsn` before writing, holding the write latch across steps 2–4
//! guarantees WAL-before-data: the record is durable before the dirty page can
//! reach disk.
//!
//! # The page chain (Stage K)
//!
//! Every user heap page carries [`HEAP_SPECIAL_SIZE`] bytes of special space
//! holding a forward pointer (`SlottedPage::set_next_page` / `next_page`), so
//! a relation's pages form a singly linked chain headed at
//! `RelationDesc::first_page`. The per-relation page list kept in memory is
//! only a **cache**: it is rebuilt by walking the chain
//! (`seed_from_chain`) the first time a relation is touched after open, then
//! grows as inserts extend the chain. Chain extension appends a freshly
//! allocated page at the tail and rewrites the old tail's next pointer.
//!
//! ## Durability of chain links
//!
//! A link write follows the M2a simplification of introducing no dedicated
//! WAL record type. Instead, the extension path logs a **post-image
//! `FullPageImage` record of the old tail page** (image captured after
//! `set_next_page`) and stamps the tail's `pd_lsn` with that record's LSN —
//! the same durability pattern catalog DDL already uses. On recovery the
//! default `FullPageImageRedoHandler` restores the link unconditionally, so
//! the chain is complete again before any `HeapInsert` redo runs; the heap
//! redo handlers stay stateless and never walk chains (they pin
//! `record.page_id` directly). WAL ordering makes the link consistent with
//! the new page's content for free: any durable `HeapInsert` into the new
//! page (higher LSN, same WAL stream) implies the tail's FPI is durable too;
//! if neither survived, the new page is simply unreachable and empty.
//!
//! # `t_xmin` stamping (Stage K, coding-plan Stage K row 3)
//!
//! `insert` / `update` overwrite the tuple header's fixed `t_xmin` field
//! (offset 0..8, §三) with `snapshot.current_xid()` before the tuple bytes
//! reach the WAL record or the page. This is the one sanctioned exception to
//! "the AM treats tuples as opaque bytes" — it touches only the fixed header
//! field, never column data — and it closes the Stage J P2 #2 hole where a
//! caller could encode `t_xmin = 99` while writing as `current_xid = 5`,
//! making scans judge visibility by `CLOG[99]`. (`delete` / `update` already
//! stamp `t_xmax` the same way.)
//!
//! # Slot stability and logical delete
//!
//! Delete is *logical*: it stamps `t_xmax` on the tuple header and leaves the
//! line pointer `Normal`. It never calls [`SlottedPage::delete_tuple`] (which
//! recycles the slot as `Unused`), because MVCC still needs the physical row
//! and recycling would break TID stability. `Unused` slots appear only when
//! vacuum's [`SlottedPage::compact`] (M3 Stage B) kills dead tuples, so
//! first-fit recycling IS reachable on the online paths. Slot assignment is
//! therefore explicit (M3 tech-selection §4.6): every online writer picks the
//! slot with [`SlottedPage::first_fit_slot`] (falling back to `slot_count`),
//! carries it in the WAL record, and places the tuple with
//! [`SlottedPage::add_tuple_at`]; redo places at the recorded slot directly.
//! Slot allocation is a WAL-carried fact, not an online-vs-redo coincidence.
//!
//! # Row-lock `t_xmax` protocol (M2c Stage P, tech-selection §9.1)
//!
//! Write-write arbitration on a row lives in its `t_xmax`: any non-INVALID
//! `t_xmax` — a real delete/update stamp OR a [`HEAP_XMAX_LOCK_ONLY`] stamp
//! (`SELECT ... FOR UPDATE`) — means "row locked". A writer reaching a row
//! runs the 5-step protocol ([`HeapAM::row_lock_gate`] + the restart loops
//! in `delete` / `update` / [`HeapAM::lock_tuple`]):
//!
//! 1. under the page write latch, read `t_xmax`;
//! 2. `t_xmax == INVALID` or `== self` → stamp immediately (the latch
//!    serializes check and stamp, so the pair IS the "CAS" of §9.1);
//! 3. `t_xmax` of a COMMITTED real deleter →
//!    [`HeapError::TupleConcurrentlyUpdated`] (the addressed version is
//!    dead; distinct from "row does not exist"). A committed LOCK_ONLY
//!    stamp is NOT a delete: the row stays modifiable and the stamp is
//!    simply overwritten;
//! 4. `t_xmax` of an ABORTED stamper → overwrite, same as step 2;
//! 5. `t_xmax` of a still-active OTHER transaction → register the wait edge
//!    in the `TxnManager`'s `row_wait_registry` WHILE STILL HOLDING THE
//!    LATCH (step 5a), then release the latch (5b), block in
//!    `TxnManager::wait_for` (5c) until the holder's commit/abort broadcast
//!    (5d), and restart from step 1 (5e).
//!
//! Registration strictly precedes latch release, so the wakeup can never be
//! missed: the holder's `end_txn` broadcast is serialized against the
//! registry by its mutex, and the latch serializes the stamper against any
//! state change of `t_xmax`.
//!
//! ## Backward compatibility (no waiter installed)
//!
//! The wait capability arrives via [`HeapAM::set_row_waiter`] (the engine
//! installs the `TxnManager` at open). A `HeapAM` WITHOUT a waiter — every
//! pre-Stage-P construction site — keeps the old "first-writer-wins +
//! second-writer-errors" behavior: any non-INVALID, non-ABORTED `t_xmax`
//! (committed OR in-progress) is rejected with [`HeapError::TupleNotFound`]
//! instead of waiting, and no `TupleConcurrentlyUpdated` is produced.
//!
//! ## Lock-only stamps and visibility
//!
//! A [`HEAP_XMAX_LOCK_ONLY`] stamp is a lock, not a delete: scan/visibility
//! paths mask it to INVALID before judging ([`visibility_xmax`]), so a
//! locked row reads as live for everyone. Lock-only stamps are NOT
//! WAL-logged (PostgreSQL does not log row locks either): they are
//! transient concurrency markers whose meaning ends with the stamper's
//! transaction, and a stamp that survives a crash reads as an in-progress
//! or aborted XID — never hiding the row.
//!
//! ## Shared locks (Stage S multixact lite; H5)
//!
//! A stamp with [`HEAP_XMAX_IS_SHARE`] additionally set is a FOR SHARE
//! lock. Because `t_xmax` names only one transaction, the full holder set
//! lives in the HeapAM's in-memory `share_locks` registry (see the field
//! docs): additional share lockers are registered without touching the
//! stamp, and a writer/exclusive requester waits for every live holder.
//! The registry is as transient as the stamps themselves — consistent with
//! locks being WAL-less.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use pg_storage::buffer_pool::{BufferPool, PageGuardMut};
use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn, PAGE_HEADER_SIZE};
use pg_storage::page_allocator::PageAllocator;
use pg_storage::recovery::RedoHandler;
use pg_storage::sync::Mutex as StorageMutex;
use pg_storage::types::{Lsn, Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::WalWriter;

use pg_txn::{is_visible, RowWaiter, Snapshot};

use crate::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext, UpdatableAM,
    UpdateContext, Vacuumable,
};
use crate::error::{HeapError, Result};
use crate::line_pointer::{LpFlags, LINE_POINTER_SIZE};
use crate::redo::heap_redo_handlers;
use crate::slotted_page::{SlottedPage, HEAP_SPECIAL_SIZE};
use crate::tuple::{
    decode_tuple, Datum, TupleHeader, HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE, HEAP_UPDATED,
    HEAP_XMAX_IS_SHARE, HEAP_XMAX_LOCK_ONLY, TUPLE_HEADER_SIZE,
};

/// Largest tuple that can ever fit on a heap page (page minus special space,
/// header, and one LP).
const MAX_TUPLE_BYTES: usize = PAGE_SIZE - HEAP_SPECIAL_SIZE - PAGE_HEADER_SIZE - LINE_POINTER_SIZE;

/// Heap access method over the shared data file.
///
/// Per relation OID, `HeapAM` caches the list of pages that hold its tuples.
/// The cache is seeded lazily by walking the on-disk page chain from
/// [`RelationDesc::first_page`] (`seed_from_chain`) the first time a relation
/// is touched, then grows as inserts extend the chain — see the module docs.
pub struct HeapAM {
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
    /// Per-relation page lists (see the struct docs).
    pages: Mutex<HashMap<Oid, Vec<PageId>>>,
    /// Serializes chain extension: without it, two threads that both find no
    /// page with room would fork the chain by linking two different pages
    /// from the same tail. Only extenders take this lock, and never while
    /// holding a page latch, so it cannot deadlock with the update path
    /// (which takes page latches but never this lock).
    extend_lock: Mutex<()>,
    /// Row-lock wait capability for the §9.1 5-step protocol (M2c Stage P),
    /// installed by the engine via [`Self::set_row_waiter`]. `None` keeps
    /// the pre-Stage-P "second-writer-errors" behavior — see the module
    /// docs' backward-compatibility section.
    row_waiter: Option<Arc<dyn RowWaiter>>,
    /// Stage S multixact lite (post-Stage-S review H5): the in-memory
    /// shared-row-lock holder registry, `(page_id, slot) → set of holder
    /// XIDs`. A `HEAP_XMAX_IS_SHARE` stamp on a tuple names only ONE holder
    /// in `t_xmax`; this set tracks the rest, which is what real
    /// share/share coexistence needs. Row locks are WAL-less by design (see
    /// the module docs), so an in-memory registry is consistent with that:
    /// on crash it vanishes exactly like the lock stamps' meaning does (a
    /// dead stamper's stamp is treated as aborted by the gate). Entries are
    /// written under the page's write latch by the gate and pruned lazily —
    /// a holder's transaction end does not remove it, the next gate pass on
    /// that tuple does.
    share_locks: Mutex<HashMap<(PageId, u16), std::collections::BTreeSet<TxnId>>>,
    /// Page allocator handle for vacuum's page release (M3 Stage C),
    /// installed by the engine via [`Self::set_page_allocator`]. `reclaim`
    /// needs it only when a compacted page turns fully empty and is returned
    /// to the allocator (`free_page`); compaction alone does not touch it.
    /// The lock type is pg-storage's aliased `Mutex` (the crate-boundary
    /// rule in `pg_storage::sync`): the exact `Arc<Mutex<PageAllocator>>`
    /// the engine hands out.
    page_allocator: Option<Arc<StorageMutex<PageAllocator>>>,
}

impl HeapAM {
    /// Create a heap AM bound to the engine's buffer pool and WAL writer.
    pub fn new(buffer_pool: Arc<BufferPool>, wal_writer: Arc<WalWriter>) -> Self {
        HeapAM {
            buffer_pool,
            wal_writer,
            pages: Mutex::new(HashMap::new()),
            extend_lock: Mutex::new(()),
            row_waiter: None,
            share_locks: Mutex::new(HashMap::new()),
            page_allocator: None,
        }
    }

    /// Install the row-lock wait capability (M2c Stage P). Called once by
    /// the engine at open time, before the AM is shared: the field is a
    /// plain `Option`, so installing requires `&mut self` and cannot race
    /// concurrent use.
    pub fn set_row_waiter(&mut self, waiter: Arc<dyn RowWaiter>) {
        self.row_waiter = Some(waiter);
    }

    /// Install the page allocator used by vacuum page release (M3 Stage C).
    /// Same install-once-before-sharing shape as [`Self::set_row_waiter`].
    pub fn set_page_allocator(&mut self, allocator: Arc<StorageMutex<PageAllocator>>) {
        self.page_allocator = Some(allocator);
    }

    /// Allocate and initialize a relation's first heap page, tracking it as a
    /// one-page chain (`next = None`).
    ///
    /// Convenience for callers/tests that need to materialize a brand-new,
    /// empty heap before inserting. The `PageAlloc` record written by
    /// `new_page` extends the data file, so recovery can pin the page even if
    /// it was never flushed. The page's `init` is made durable with a
    /// post-image `FullPageImage` record (see [`Self::extend_chain`]): the
    /// page may have come from the freelist, where a previous tenant's
    /// content still sits on disk, and "fresh page" detection on the
    /// recovery side keys off an all-zero page — replaying the init image is
    /// what guarantees a reused page is seen as freshly initialized rather
    /// than as its previous tenant's data.
    pub fn create_heap(&self, rel_oid: Oid) -> Result<PageId> {
        let mut guard = self.buffer_pool.new_page()?;
        let page_id = guard.page_id();
        {
            let page = as_page_mut(&mut guard);
            SlottedPage::init_with_special(page, HEAP_SPECIAL_SIZE);
            self.log_page_init(page_id, page)?;
        }
        self.pages
            .lock()
            .expect("heap page map poisoned")
            .insert(rel_oid, vec![page_id]);
        Ok(page_id)
    }

    /// Append a post-image `FullPageImage` of a freshly initialized page and
    /// stamp its `pd_lsn` — the durability anchor for page initialization.
    /// Without it, a freelist-reused page whose previous tenant's bytes are
    /// still on disk would be read back as that tenant's data on recovery
    /// (redo and `seed_from_chain` both key "freshness" off page content).
    fn log_page_init(&self, page_id: PageId, page: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let image = page.to_vec();
        let lsn = self
            .wal_writer
            .append(WalRecord::full_page_image(page_id, image)?)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    /// Return a snapshot of the pages tracked for `rel`, seeding the cache
    /// from the on-disk chain on first touch.
    ///
    /// Pub for pg-engine's recovery-time loser compensation (it maps heap
    /// pages back to their owning table); ordinary callers should go
    /// through the AM operations.
    pub fn relation_pages(&self, rel: &RelationDesc<'_>) -> Result<Vec<PageId>> {
        if let Some(pages) = self
            .pages
            .lock()
            .expect("heap page map poisoned")
            .get(&rel.rel_oid)
        {
            return Ok(pages.clone());
        }
        let seeded = self.seed_from_chain(rel.first_page)?;
        // Another thread may have seeded (and even extended) the same
        // relation concurrently; the existing cache entry wins because it is
        // at least as fresh as anything readable from disk.
        let mut map = self.pages.lock().expect("heap page map poisoned");
        Ok(map.entry(rel.rel_oid).or_insert(seeded).clone())
    }

    /// Walk the on-disk page chain from `first_page`, collecting the
    /// relation's pages in chain order.
    ///
    /// A fresh (all-zero) page ends the walk: it is a page whose allocation
    /// (`PageAlloc`) and incoming link survived a crash but whose first
    /// `HeapInsert` did not — keeping it in the list lets a later insert
    /// initialize and reuse it instead of leaking it. A cycle or an
    /// unreadable chain pointer is catalog-level corruption and fails loudly.
    fn seed_from_chain(&self, first_page: PageId) -> Result<Vec<PageId>> {
        let mut pages = vec![first_page];
        let mut seen = HashSet::from([first_page]);
        loop {
            let current = *pages.last().expect("pages starts non-empty");
            let guard = self.buffer_pool.pin(current)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            // A fresh page has no header yet, so there is no special space to
            // read a next pointer from.
            if SlottedPage::header(page).pd_upper == 0 {
                break;
            }
            let Some(next) = SlottedPage::next_page(page)? else {
                break;
            };
            if !seen.insert(next) {
                return Err(HeapError::Corrupted(format!(
                    "page chain cycle detected at page {next} (head {first_page})"
                )));
            }
            pages.push(next);
        }
        Ok(pages)
    }

    /// Record that `page_id` now belongs to `rel_oid` (idempotent).
    fn track_page(&self, rel_oid: Oid, page_id: PageId) {
        let mut map = self.pages.lock().expect("heap page map poisoned");
        let list = map.entry(rel_oid).or_default();
        if !list.contains(&page_id) {
            list.push(page_id);
        }
    }

    /// Drop the cached page list of `rel_oid` (Stage K engine DDL).
    ///
    /// Called by `drop_table` after the relation's pages have been freed:
    /// freed page IDs can be handed out again, and a stale cache entry would
    /// route a future relation's reads/writes into pages it does not own.
    /// The on-disk chain is untouched (the pages themselves are freed by the
    /// caller); this only clears the in-memory cache. Dropping an unknown
    /// relation is a no-op.
    pub fn drop_relation(&self, rel_oid: Oid) {
        self.pages
            .lock()
            .expect("heap page map poisoned")
            .remove(&rel_oid);
    }

    /// Remove `page_id` from the cached page list of `rel_oid` (single-page
    /// granularity, M3 Stage C). After vacuum unlinks an empty page from the
    /// on-disk chain, the in-memory cache must forget it in the same breath:
    /// otherwise `acquire_page_with_room` can still pick the stale entry and
    /// insert new rows into a page no longer reachable from the chain —
    /// physically consistent rows that are logically unreachable.
    fn evict_page(&self, rel_oid: Oid, page_id: PageId) {
        if let Some(list) = self
            .pages
            .lock()
            .expect("heap page map poisoned")
            .get_mut(&rel_oid)
        {
            list.retain(|&p| p != page_id);
        }
    }

    /// The ONE online compaction template (M3 Stage C, tech-selection
    /// §4.5): kill `dead_slots` on `page_id` via the shared
    /// [`SlottedPage::compact`] primitive under a `HeapCleanup` WAL record,
    /// optionally splicing the page out of the chain (`unlink_prev_page` /
    /// `unlink_next_page`, both `PageId::INVALID` for compaction only).
    /// Returns the record's LSN. Vacuum's `reclaim` and the Stage B/C crash
    /// tests both go through here — the ordering below must exist in exactly
    /// this one place.
    ///
    /// ORDERING (load-bearing, review F1): pin for write FIRST — `pin_mut`
    /// may emit the per-checkpoint-cycle FPI of the PRE-compact image, and
    /// the `HeapCleanup` record must sort after it in the WAL, or recovery's
    /// unconditional FPI replay rolls the page back past the compact and the
    /// next slot-addressed redo hard-fails on an occupied slot. Then append
    /// the record, run `compact()`, stamp `pd_lsn` — all under the page's
    /// write latch (WAL-before-data: no flush can slip between mutation and
    /// stamp).
    ///
    /// The chain unlink (relink the predecessor's `next_page` past the
    /// spliced page) rides in the SAME record and is applied to the
    /// predecessor under its own latch, guarded by the predecessor's own
    /// `pd_lsn` on the redo side — the two pages may reach disk at different
    /// times before a crash (same policy as a cross-page `HeapUpdate`).
    ///
    /// The predecessor is pinned BEFORE the record is appended (post-Stage-C
    /// review R2): its `pin_mut` may emit the per-checkpoint-cycle FPI of the
    /// PRE-unlink image (vacuum reclaiming cold, disk-resident pages is the
    /// typical workload), and that FPI must also sort before the
    /// `HeapCleanup` record. The reverse order lets the FPI take a larger
    /// LSN; recovery replays FPIs unconditionally, so it would roll the
    /// predecessor back past the relink while the `PageFree` for the spliced
    /// page still replays — the chain then points at a page the allocator
    /// can rehand to another relation: structural corruption. Holding both
    /// write latches at once is deadlock-free here: vacuum runs under the
    /// caller's `AccessExclusive` table lock (no concurrent writers; the
    /// lock-free readers only block on the latch), and redo is
    /// single-threaded — no AB/BA pairing exists.
    ///
    /// Vacuum is not transactional: the record carries `txn_id = INVALID`
    /// (`WalRecord::heap_cleanup`), so replay never depends on a transaction
    /// outcome and the analysis phase never puts the page in an ATT.
    pub fn compact_page(
        &self,
        page_id: PageId,
        dead_slots: &[u16],
        unlink_prev_page: PageId,
        unlink_next_page: PageId,
    ) -> Result<Lsn> {
        // Predecessor FIRST (R2, see the fn docs): its pre-unlink FPI must
        // reach the WAL before the HeapCleanup record.
        let mut prev_guard = if unlink_prev_page != PageId::INVALID {
            debug_assert!(
                unlink_prev_page != page_id,
                "a page is never its own chain predecessor"
            );
            Some(self.buffer_pool.pin_mut(unlink_prev_page)?)
        } else {
            None
        };
        let mut guard = self.buffer_pool.pin_mut(page_id)?;
        // The constructor hard-validates the ascending kill list BEFORE the
        // record can reach the WAL (F3): a poison record would brick every
        // subsequent recovery.
        let rec = WalRecord::heap_cleanup(
            page_id,
            dead_slots.to_vec(),
            unlink_prev_page,
            unlink_next_page,
        )?;
        let lsn = self.wal_writer.append(rec)?;
        {
            let page = as_page_mut(&mut guard);
            SlottedPage::compact(page, dead_slots)?;
            stamp_pd_lsn(page, lsn);
        }
        drop(guard);

        if let Some(prev_guard) = prev_guard.as_mut() {
            let prev = as_page_mut(prev_guard);
            let next = if unlink_next_page == PageId::INVALID {
                None
            } else {
                Some(unlink_next_page)
            };
            SlottedPage::set_next_page(prev, next)?;
            stamp_pd_lsn(prev, lsn);
        }
        Ok(lsn)
    }

    /// Group a dead-tuple list (from `scan_dead_tuples`) by HOT chain and
    /// split it into reclaimable vs retained (M3 Stage C, §4.2/§4.4). This
    /// is the SINGLE implementation of chain grouping: both
    /// `collect_index_keys` (its `index_keys`) and `reclaim` (its `kills`)
    /// consume this one's output, so the read-only and the physical half of
    /// vacuum can never disagree about which slots die.
    ///
    /// Deadness itself is NOT re-judged here — membership in the caller's
    /// `dead` set IS the per-member verdict (`scan_dead_tuples` already
    /// applied the horizon rules: aborted `t_xmin` → dead; committed,
    /// non-LOCK_ONLY `t_xmax < horizon` → dead). What this helper decides is
    /// purely structural: a chain is FULLY dead only when every member
    /// reachable from its root via `t_ctid` is in the dead set.
    ///
    /// - Standalone dead tuple (no chain): reclaimable; index key = itself.
    /// - Fully-dead chain: EVERY member slot is killable; the index key is
    ///   the chain ROOT's (tid, decoded column values) — the root owns the
    ///   chain's index entries (Stage S; `HEAP_ONLY_TUPLE` members never got
    ///   entries of their own).
    /// - Partially-dead chain: contributes NOTHING — no prune, no redirect
    ///   (§4.2: LP redirection is an on-disk format change, out of scope);
    ///   its dead members stay in place.
    fn classify_dead_tuples(
        &self,
        rel: &RelationDesc<'_>,
        dead: &[Tid],
    ) -> Result<DeadClassification> {
        let dead_set: HashSet<Tid> = dead.iter().copied().collect();
        // Group the input by page so each page is pinned exactly once.
        let mut by_page: BTreeMap<PageId, Vec<u16>> = BTreeMap::new();
        for tid in dead {
            by_page.entry(tid.page_id).or_default().push(tid.slot_id);
        }

        let mut out = DeadClassification {
            index_keys: Vec::new(),
            kills: BTreeMap::new(),
        };
        for (page_id, mut dead_slots) in by_page {
            dead_slots.sort_unstable();
            let guard = self.buffer_pool.pin(page_id)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            // A fresh (never-initialized) page holds no tuples at all.
            if SlottedPage::header(page).pd_upper == 0 {
                continue;
            }
            let slot_count = SlottedPage::slot_count(page) as u16;

            // Read every slot's header once (headers only — the full decode
            // happens for chain roots selected below).
            let mut headers: Vec<Option<TupleHeader>> = Vec::with_capacity(slot_count as usize);
            for slot in 0..slot_count {
                let header = match SlottedPage::tuple(page, slot)? {
                    Some(bytes) => match TupleHeader::read_from(bytes) {
                        Ok(h) => Some(h),
                        // Mirror scan_dead_tuples: an undecodable tuple must
                        // not abort the pass. The slot is treated as
                        // non-killable — the SAFE direction (leak, never
                        // corrupt) — and any chain containing it can no
                        // longer be confirmed fully dead.
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                %page_id,
                                slot,
                                "vacuum classify: skipping undecodable tuple"
                            );
                            None
                        }
                    },
                    // Non-Normal LP (already Unused/Dead): dead space, not a
                    // kill candidate — `compact()` hard-errors on killing an
                    // Unused slot, so such input must never reach the list.
                    None => None,
                };
                headers.push(header);
            }

            let mut visited: HashSet<u16> = HashSet::new();
            for &slot in &dead_slots {
                if slot >= slot_count || !visited.insert(slot) {
                    continue;
                }
                if headers[slot as usize].is_none() {
                    continue;
                }
                let tid = Tid {
                    page_id,
                    slot_id: slot,
                };
                // Chain root: a non-HEAP_ONLY tuple is its own root; a
                // HEAP_ONLY member walks the page's t_ctid links backwards
                // (chains never leave their page; the pin above is exactly
                // the pin hot_chain_root requires).
                let root = if headers[slot as usize].expect("checked above").t_infomask2
                    & HEAP_ONLY_TUPLE
                    != 0
                {
                    hot_chain_root(page, page_id, tid)?
                } else {
                    tid
                };

                // Walk FORWARD from the root, collecting the whole chain.
                // Same termination contract as follow_hot_chain: a
                // well-formed chain ends (self-t_ctid, no HOT_UPDATED bit,
                // or an off-page/undecodable link) within slot_count hops;
                // exhausting the bound means a cycle — corruption, loudly.
                let mut chain: Vec<Tid> = Vec::new();
                let mut cur = root;
                let mut terminated = false;
                for _ in 0..slot_count {
                    chain.push(cur);
                    if cur.slot_id >= slot_count {
                        terminated = true;
                        break;
                    }
                    match &headers[cur.slot_id as usize] {
                        Some(h)
                            if h.t_infomask2 & HEAP_HOT_UPDATED != 0
                                && h.t_ctid != cur
                                && h.t_ctid.page_id == page_id =>
                        {
                            cur = h.t_ctid;
                        }
                        _ => {
                            terminated = true;
                            break;
                        }
                    }
                }
                if !terminated {
                    return Err(HeapError::Corrupted(format!(
                        "HOT chain on page {page_id} exceeds {slot_count} hops (cycle?)"
                    )));
                }
                for t in &chain {
                    visited.insert(t.slot_id);
                }

                // FULLY dead iff every chain member is in the dead set and
                // decodable. Partially-dead chains fall through untouched.
                let fully_dead = chain
                    .iter()
                    .all(|t| t.slot_id < slot_count && headers[t.slot_id as usize].is_some())
                    && chain.iter().all(|t| dead_set.contains(t));
                if !fully_dead {
                    continue;
                }

                // Index key: the chain ROOT's (for a standalone tuple the
                // root IS the tuple). Decode from the tuple bytes via the
                // same path Engine::delete_inner uses (read tuple +
                // decode_tuple over the relation schema); NULL columns stay
                // None in the vector — the engine skips them when building
                // keys, its existing convention.
                let root_bytes = SlottedPage::tuple(page, root.slot_id)?
                    .expect("fully-dead chain root passed the decodable check");
                let (_, values) = decode_tuple(root_bytes, rel.columns)?;
                out.index_keys.push((root, values));

                let kills = out.kills.entry(page_id).or_default();
                for t in &chain {
                    kills.push(t.slot_id);
                }
            }
            // compact()'s WAL payload contract: strictly ascending kill list.
            if let Some(kills) = out.kills.get_mut(&page_id) {
                kills.sort_unstable();
                kills.dedup();
            }
        }
        Ok(out)
    }

    /// Reject tuples that are empty or can never fit on a page, matching
    /// [`SlottedPage::add_tuple`]'s own guards but *before* any WAL is written.
    fn validate_tuple_len(bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Err(HeapError::InvalidArgument(
                "cannot insert an empty tuple".to_string(),
            ));
        }
        if bytes.len() > MAX_TUPLE_BYTES {
            return Err(HeapError::TupleTooLarge(bytes.len()));
        }
        Ok(())
    }

    /// Overwrite `t_xmin` (tuple-header fixed field, offset 0..8, §三) with
    /// the writer's own XID, returning the stamped tuple bytes.
    ///
    /// This is the one place the AM breaks "tuples are opaque bytes": it
    /// touches only the fixed header field, never column data (see the
    /// module docs). Stamping happens before the WAL record is built, so the
    /// logged bytes — and therefore any tuple reconstructed by redo — carry
    /// the writer's XID, not whatever the caller encoded.
    fn stamp_xmin(tuple: &[u8], xid: TxnId) -> Result<Vec<u8>> {
        if tuple.len() < TUPLE_HEADER_SIZE {
            return Err(HeapError::InvalidArgument(format!(
                "tuple of {} bytes is shorter than the {}-byte header",
                tuple.len(),
                TUPLE_HEADER_SIZE
            )));
        }
        let mut owned = tuple.to_vec();
        owned[0..8].copy_from_slice(&xid.0.to_le_bytes());
        Ok(owned)
    }

    /// Like [`stamp_xmin`], but additionally sets the `HEAP_ONLY_TUPLE` bit
    /// in `t_infomask2`, marking the new tuple as a HOT chain member
    /// reachable only via `t_ctid` (not via index entries).
    fn stamp_hot_only(tuple: &[u8], xid: TxnId) -> Result<Vec<u8>> {
        if tuple.len() < TUPLE_HEADER_SIZE {
            return Err(HeapError::InvalidArgument(format!(
                "tuple of {} bytes is shorter than the {}-byte header",
                tuple.len(),
                TUPLE_HEADER_SIZE
            )));
        }
        let mut owned = tuple.to_vec();
        owned[0..8].copy_from_slice(&xid.0.to_le_bytes());
        let infomask2 = u16::from_le_bytes([owned[54], owned[55]]);
        owned[54..56].copy_from_slice(&(infomask2 | HEAP_ONLY_TUPLE).to_le_bytes());
        Ok(owned)
    }

    /// Pin a page (initialized) that has room for `needed` bytes, excluding
    /// `exclude`, extending the chain with a fresh page if none of the
    /// relation's existing pages qualify. The returned guard is held for the
    /// caller's mutation.
    fn acquire_page_with_room(
        &self,
        rel: &RelationDesc<'_>,
        needed: usize,
        exclude: PageId,
    ) -> Result<PageGuardMut<'_>> {
        // Scan newest-first: a pure-append heap fills pages in allocation order,
        // so only the most recently allocated (tail) page still has room. A
        // front-to-back scan would re-pin every full page on each insert (O(n)
        // locks per insert, O(n^2) overall); reverse order finds room in O(1)
        // for the common append case.
        for page_id in self.relation_pages(rel)?.into_iter().rev() {
            if page_id == exclude {
                continue;
            }
            let mut guard = self.buffer_pool.pin_mut(page_id)?;
            {
                let page = as_page_mut(&mut guard);
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                if SlottedPage::free_space(page) >= needed {
                    return Ok(guard);
                }
            }
        }
        self.extend_chain(rel, needed, exclude)
    }

    /// Append a freshly allocated page to the relation's chain and return it
    /// pinned for write.
    ///
    /// Serialized by `extend_lock` (see the struct docs). After taking the
    /// lock the current tail is re-checked: a concurrent extender may have
    /// just appended a page that still has room, in which case no new page is
    /// allocated at all.
    ///
    /// The link from the old tail is made durable with a post-image
    /// `FullPageImage` record of the tail page (see the module docs'
    /// "Durability of chain links"), not with a new WAL record type.
    fn extend_chain(
        &self,
        rel: &RelationDesc<'_>,
        needed: usize,
        exclude: PageId,
    ) -> Result<PageGuardMut<'_>> {
        let _serialize = self.extend_lock.lock().expect("heap extend lock poisoned");

        let pages = self.relation_pages(rel)?;
        let tail = *pages.last().expect("seeded chain is non-empty");
        if tail != exclude {
            let mut guard = self.buffer_pool.pin_mut(tail)?;
            {
                let page = as_page_mut(&mut guard);
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                if SlottedPage::free_space(page) >= needed {
                    // A concurrent extender already added room.
                    return Ok(guard);
                }
            }
        }

        // Allocate and initialize the new tail. A valid tuple always fits on
        // a freshly initialized page (validate_tuple_len bounds it), so the
        // caller can add_tuple unconditionally. The init is WAL-logged via
        // `log_page_init` so a freelist-reused page recovers as freshly
        // initialized, not as its previous tenant's bytes.
        let mut new_guard = self.buffer_pool.new_page()?;
        let new_page_id = new_guard.page_id();
        {
            let page = as_page_mut(&mut new_guard);
            SlottedPage::init_with_special(page, HEAP_SPECIAL_SIZE);
            self.log_page_init(new_page_id, page)?;
        }

        // Link the old tail to the new page, then log the tail's post-image
        // FPI and stamp its pd_lsn — all while holding the tail's write
        // latch, so no flush can slip between the link write and the pd_lsn
        // stamp (WAL-before-data for the link).
        {
            let mut tail_guard = self.buffer_pool.pin_mut(tail)?;
            let page = as_page_mut(&mut tail_guard);
            SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
            SlottedPage::set_next_page(page, Some(new_page_id))?;
            let image = page.to_vec();
            let lsn = self
                .wal_writer
                .append(WalRecord::full_page_image(tail, image)?)?;
            stamp_pd_lsn(page, lsn);
        }

        self.track_page(rel.rel_oid, new_page_id);
        Ok(new_guard)
    }

    /// Stamp `t_xmax`, `t_cid`, and optionally the `HEAP_UPDATED` infomask bit
    /// onto the live tuple at `tid`'s slot, in place. TID stability is
    /// preserved: the line pointer stays `Normal`.
    fn stamp_deleted(
        page: &mut [u8; PAGE_SIZE],
        tid: Tid,
        xmax: TxnId,
        curcid: u32,
        updated: bool,
    ) -> Result<()> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        header.t_xmax = xmax;
        header.t_cid = curcid;
        if updated {
            header.t_infomask |= HEAP_UPDATED;
        }
        // A real delete/update supersedes any lock-only stamp on the row
        // (e.g. `SELECT ... FOR UPDATE` followed by DELETE in the same
        // transaction): leaving LOCK_ONLY set would mask the delete from
        // visibility checks and resurrect the row for scans. IS_SHARE (H5)
        // goes with it — the holder registry entry was already dropped by
        // the gate's `note_stamp_overwrite`.
        header.t_infomask &= !(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_IS_SHARE);
        header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
        Ok(())
    }

    /// HOT-update stamp on the OLD tuple: like [`stamp_deleted`] with
    /// `updated=true`, but additionally sets `t_ctid` to the new version's
    /// TID and `HEAP_HOT_UPDATED` in `t_infomask2`, forming the chain link
    /// that `scan` follows when this old version is no longer visible.
    fn stamp_hot_update(
        page: &mut [u8; PAGE_SIZE],
        tid: Tid,
        xmax: TxnId,
        curcid: u32,
        new_tid: Tid,
    ) -> Result<()> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        header.t_xmax = xmax;
        header.t_cid = curcid;
        header.t_infomask |= HEAP_UPDATED;
        header.t_infomask &= !(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_IS_SHARE);
        header.t_ctid = new_tid;
        header.t_infomask2 |= HEAP_HOT_UPDATED;
        header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
        Ok(())
    }

    /// §9.1 steps 1–5a of the row-lock protocol: read the tuple header at
    /// `tid`'s slot (under the page write latch the caller holds) and decide
    /// whether the caller may stamp the tuple.
    ///
    /// `self_xid` is the writer's own XID (`snapshot.current_xid()`) — the
    /// row-lock identity. A `t_xmax` naming `self_xid` is a self-conflict:
    /// the caller already locked/deleted/updated this row version inside its
    /// own transaction and simply proceeds (never waits on itself).
    ///
    /// `request` says what the caller wants to stamp (post-Stage-S review
    /// H5 — replaces the boolean `for_lock`):
    ///
    /// - [`LockRequest::Writer`] (delete/update): a REAL `t_xmax` stamp,
    ///   which supersedes any lock. Overwriting my own stamp is the normal
    ///   same-transaction re-write.
    /// - [`LockRequest::Exclusive`] ([`Self::lock_tuple`], FOR UPDATE): an
    ///   exclusive lock-only stamp. Re-stamping a row whose existing stamp
    ///   is my own REAL delete/update (not [`HEAP_XMAX_LOCK_ONLY`]) would
    ///   re-add the lock-only bit on top of a delete stamp, and the
    ///   visibility mask would then resurrect the row — so that combination
    ///   is rejected and only idempotent re-locking of my own LOCK_ONLY
    ///   stamp proceeds.
    /// - [`LockRequest::Shared`] ([`Self::lock_tuple_shared`], FOR SHARE): a
    ///   shared lock-only stamp. Shared holders COEXIST (H5 multixact lite):
    ///   the first holder stamps `t_xmax` and every holder (including the
    ///   first) is tracked in the `share_locks` registry; additional share
    ///   lockers are registered without touching the stamp
    ///   ([`RowLockGate::ProceedNoStamp`]). A writer or exclusive requester
    ///   must wait for ALL live holders — one at a time, restarting the
    ///   protocol after each wait. A FOR SHARE → FOR UPDATE/write upgrade
    ///   inside one transaction waits for every OTHER holder; a shared
    ///   request on a row I already hold EXCLUSIVELY does not downgrade the
    ///   stamp (PG keeps the stronger lock).
    ///
    /// When the verdict is [`RowLockGate::Wait`] the wait edge
    /// (`self_xid → blocker`) is registered BEFORE this function returns —
    /// i.e. still under the latch — so the caller releasing the latch and
    /// sleeping cannot miss the blocker's commit/abort broadcast (the
    /// step-5a-before-5b ordering, see the module docs).
    ///
    /// # Errors
    ///
    /// - [`HeapError::TupleNotFound`]: the slot does not hold a live
    ///   (`Normal`) tuple. ALSO the legacy no-waiter behavior for a
    ///   committed or in-progress `t_xmax` (module docs, backward
    ///   compatibility).
    /// - [`HeapError::TupleConcurrentlyUpdated`] (§9.1 step 3): `t_xmax` is
    ///   a REAL delete/update stamp (not [`HEAP_XMAX_LOCK_ONLY`]) whose
    ///   transaction COMMITTED — the addressed row version is dead. A
    ///   committed LOCK_ONLY stamp is not a delete: the row stays
    ///   modifiable and the stamp is overwritten (Proceed).
    /// - [`HeapError::InvalidArgument`] (lock requests only): the row
    ///   already carries MY real delete/update stamp (see above).
    fn row_lock_gate(
        &self,
        page: &[u8; PAGE_SIZE],
        tid: Tid,
        self_xid: TxnId,
        clog: &dyn ClogAccessor,
        request: LockRequest,
    ) -> Result<RowLockGate> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        let xmax = header.t_xmax;
        let lock_only = header.t_infomask & HEAP_XMAX_LOCK_ONLY != 0;
        let is_share = header.t_infomask & HEAP_XMAX_IS_SHARE != 0;

        // Step 2 (no stamp yet): proceed. A shared request's fresh stamp
        // starts a new singleton holder set; any other stamp supersedes
        // whatever the registry held (bookkeeping in `note_stamp_overwrite`).
        if xmax == TxnId::INVALID {
            self.note_stamp_overwrite(tid, self_xid, request);
            return Ok(RowLockGate::Proceed);
        }

        if xmax == self_xid {
            // Self-conflict (my own stamp): never wait on myself.
            if !lock_only {
                // My own REAL delete/update stamp: re-stamping a lock on top
                // would resurrect the row via the visibility mask (see the
                // doc above); a writer overwriting its own stamp is the
                // normal same-transaction re-write.
                if request != LockRequest::Writer {
                    return Err(HeapError::InvalidArgument(format!(
                        "cannot lock {tid:?}: row version already deleted or updated by this transaction"
                    )));
                }
                return Ok(RowLockGate::Proceed);
            }
            if is_share {
                // My own SHARE stamp (H5): other holders may be registered
                // alongside me.
                return match request {
                    LockRequest::Shared => {
                        // Idempotent re-lock: make sure I am registered; the
                        // stamp is already mine and stays unchanged.
                        self.register_share_holder(tid, self_xid);
                        Ok(RowLockGate::ProceedNoStamp)
                    }
                    LockRequest::Exclusive | LockRequest::Writer => {
                        // FOR SHARE → FOR UPDATE / write upgrade (same txn):
                        // wait for every OTHER live holder, one at a time.
                        let live = self.live_share_holders(tid, xmax, clog)?;
                        if let Some(&blocking) = live.iter().find(|&&h| h != self_xid) {
                            self.register_wait_edge(self_xid, blocking);
                            return Ok(RowLockGate::Wait(blocking));
                        }
                        // Sole holder: the upgrade proceeds; the caller's
                        // stamp supersedes the shared one.
                        self.note_stamp_overwrite(tid, self_xid, request);
                        Ok(RowLockGate::Proceed)
                    }
                };
            }
            // My own EXCLUSIVE lock-only stamp. A shared request does not
            // downgrade it (PG keeps the stronger lock); anything else is an
            // idempotent re-stamp.
            if request == LockRequest::Shared {
                return Ok(RowLockGate::ProceedNoStamp);
            }
            return Ok(RowLockGate::Proceed);
        }

        // Someone else's stamp.
        if is_share {
            // H5 multixact lite: a share stamp's `t_xmax` names only one
            // holder; the registry holds the full set.
            return self.share_stamp_gate(tid, self_xid, xmax, clog, request);
        }

        // Exclusive lock-only stamp or a real delete/update stamp.
        let mut state = clog.get_state(xmax);
        if matches!(state, TxnState::InProgress | TxnState::SubCommitted) {
            // Step 5a: the holder LOOKS active. `SubCommitted` (M3-reserved,
            // never produced in M2) folds in here: a sub-committed stamper's
            // parent may still abort, so it is "not terminally committed",
            // matching the visibility oracle's `!= Committed` treatment.
            match &self.row_waiter {
                Some(waiter) => {
                    if waiter.is_active(xmax) {
                        // Genuinely active holder: register the wait edge
                        // UNDER THE LATCH; the caller releases latches and
                        // blocks (steps 5b/5c).
                        waiter.register_row_wait(self_xid, xmax);
                        return Ok(RowLockGate::Wait(xmax));
                    }
                    // Not active despite the InProgress CLOG read. Two
                    // cases:
                    //
                    // - The stamper ENDED between our CLOG read and the
                    //   active-set check (normal race): `end_txn` flips the
                    //   CLOG bit BEFORE removing the XID from the active
                    //   set, so observing not-active orders us after the
                    //   terminal write — re-reading the CLOG now yields the
                    //   terminal state, which the match below handles.
                    // - The stamper CRASHED: recovery undo (Stage S
                    //   `HeapUndoHandler`) stamps every ATT member
                    //   Aborted in the CLOG, so post-recovery this branch
                    //   is defensive only — the re-read normally yields
                    //   Aborted. If an InProgress stamp survives anyway,
                    //   WAL replay has rebuilt every durable commit's
                    //   bit, so it means "never committed" — treat it as
                    //   aborted (Proceed). Waiting would spin forever on
                    //   a transaction that can never end.
                    state = clog.get_state(xmax);
                }
                None => return Err(HeapError::TupleNotFound(tid)), // legacy mode
            }
        }
        match state {
            // Step 4: the stamp never took effect (or its stamper crashed);
            // overwrite it. A terminal LOCK_ONLY stamp (committed or
            // aborted) lands in the Proceed arms too — a lock is not a
            // delete, so the row stays modifiable.
            TxnState::Aborted => {}
            // InProgress/SubCommitted here is only reachable via the
            // crashed-stamper re-read above (a live holder took the `Wait`
            // early return; legacy mode returned already).
            TxnState::InProgress | TxnState::SubCommitted => {}
            TxnState::Committed if lock_only => {}
            // Step 3: a committed real delete/update owns this version.
            TxnState::Committed => match &self.row_waiter {
                Some(_) => return Err(HeapError::TupleConcurrentlyUpdated(tid)),
                None => return Err(HeapError::TupleNotFound(tid)), // legacy mode
            },
        }
        self.note_stamp_overwrite(tid, self_xid, request);
        Ok(RowLockGate::Proceed)
    }

    /// H5 gate for a tuple carrying SOMEONE ELSE's share stamp
    /// ([`HEAP_XMAX_IS_SHARE`]): the `share_locks` registry is authoritative
    /// for the holder set; the stamp's `t_xmax` is always folded in as a
    /// holder (it is the first stamper), so a registry that lost entries —
    /// the registry is per-`HeapAM`, and nothing stops a second instance
    /// from opening the same pages — degrades to the pre-H5 single-holder
    /// behavior instead of falsely proceeding.
    fn share_stamp_gate(
        &self,
        tid: Tid,
        self_xid: TxnId,
        stamp_xmax: TxnId,
        clog: &dyn ClogAccessor,
        request: LockRequest,
    ) -> Result<RowLockGate> {
        let live = self.live_share_holders(tid, stamp_xmax, clog)?;
        if live.contains(&self_xid) {
            // I already hold a share on this row (a later holder's fresh
            // stamp moved `t_xmax` on): same rules as the "my own share
            // stamp" case in `row_lock_gate`.
            return match request {
                LockRequest::Shared => {
                    self.register_share_holder(tid, self_xid);
                    Ok(RowLockGate::ProceedNoStamp)
                }
                LockRequest::Exclusive | LockRequest::Writer => {
                    if let Some(&blocking) = live.iter().find(|&&h| h != self_xid) {
                        self.register_wait_edge(self_xid, blocking);
                        return Ok(RowLockGate::Wait(blocking));
                    }
                    self.note_stamp_overwrite(tid, self_xid, request);
                    Ok(RowLockGate::Proceed)
                }
            };
        }
        match request {
            LockRequest::Shared => {
                if live.is_empty() {
                    // Every previous holder ended: take a FRESH stamp (the
                    // caller re-stamps `t_xmax` with its own XID).
                    self.note_stamp_overwrite(tid, self_xid, request);
                    return Ok(RowLockGate::Proceed);
                }
                // share/share coexistence (H5): register as an additional
                // holder and leave the stamp unchanged — the row's `t_xmax`
                // keeps naming the first holder, the registry now names both.
                self.register_share_holder(tid, self_xid);
                Ok(RowLockGate::ProceedNoStamp)
            }
            LockRequest::Exclusive | LockRequest::Writer => {
                // FOR UPDATE excludes share lockers and vice versa: wait for
                // ALL registered holders — one at a time, restarting the
                // protocol after each wait (the next gate pass re-evaluates
                // the remaining set).
                if let Some(&blocking) = live.iter().next() {
                    self.register_wait_edge(self_xid, blocking);
                    return Ok(RowLockGate::Wait(blocking));
                }
                // No live holders: overwrite the stale stamp.
                self.note_stamp_overwrite(tid, self_xid, request);
                Ok(RowLockGate::Proceed)
            }
        }
    }

    /// The LIVE holders of the share lock on `tid` (H5): the registry set
    /// ∪ {`stamp_xmax`}, pruned of ended or crashed transactions. The pruned
    /// set is written back to the registry so dead holders cannot
    /// accumulate (a holder's transaction end never removes it — entries
    /// are only reconciled here, under the page latch, or dropped by
    /// [`Self::note_stamp_overwrite`]).
    ///
    /// A holder is LIVE iff its CLOG entry is non-terminal AND it is still
    /// in the active set — the same crashed-stamper rule as the main gate
    /// (an `InProgress` CLOG read with no active-set membership means the
    /// holder ended in the race window or crashed; either way it must never
    /// be waited on).
    ///
    /// Legacy no-waiter mode keeps the pre-H5 behavior: a holder whose CLOG
    /// entry reads `InProgress` is [`HeapError::TupleNotFound`].
    fn live_share_holders(
        &self,
        tid: Tid,
        stamp_xmax: TxnId,
        clog: &dyn ClogAccessor,
    ) -> Result<std::collections::BTreeSet<TxnId>> {
        let key = (tid.page_id, tid.slot_id);
        let mut holders = self
            .share_locks
            .lock()
            .expect("share lock registry poisoned")
            .get(&key)
            .cloned()
            .unwrap_or_default();
        holders.insert(stamp_xmax);
        let mut live = std::collections::BTreeSet::new();
        for h in holders {
            match clog.get_state(h) {
                TxnState::Committed | TxnState::Aborted => {}
                TxnState::InProgress | TxnState::SubCommitted => match &self.row_waiter {
                    Some(waiter) if waiter.is_active(h) => {
                        live.insert(h);
                    }
                    // Ended in the race window, or crashed: not a blocker.
                    Some(_) => {}
                    None => return Err(HeapError::TupleNotFound(tid)), // legacy mode
                },
            }
        }
        let mut map = self
            .share_locks
            .lock()
            .expect("share lock registry poisoned");
        if live.is_empty() {
            map.remove(&key);
        } else {
            map.insert(key, live.clone());
        }
        Ok(live)
    }

    /// Registry bookkeeping for a gate [`RowLockGate::Proceed`] that the
    /// caller will follow by OVERWRITING the tuple's stamp (H5): a fresh
    /// shared stamp starts a new singleton holder set; an exclusive lock or
    /// a real delete/update retires the set (the stamp alone is
    /// authoritative again).
    fn note_stamp_overwrite(&self, tid: Tid, self_xid: TxnId, request: LockRequest) {
        let key = (tid.page_id, tid.slot_id);
        let mut map = self
            .share_locks
            .lock()
            .expect("share lock registry poisoned");
        match request {
            LockRequest::Shared => {
                map.insert(key, std::collections::BTreeSet::from([self_xid]));
            }
            LockRequest::Exclusive | LockRequest::Writer => {
                map.remove(&key);
            }
        }
    }

    /// Register `xid` as an additional share holder of `tid` (H5). Runs
    /// under the caller's page latch, so it cannot interleave with another
    /// gate pass on the same tuple.
    fn register_share_holder(&self, tid: Tid, xid: TxnId) {
        self.share_locks
            .lock()
            .expect("share lock registry poisoned")
            .entry((tid.page_id, tid.slot_id))
            .or_default()
            .insert(xid);
    }

    /// Register the §9.1 step-5a wait edge `self_xid → blocking` (still
    /// under the caller's page latch). Only called on paths where a waiter
    /// is installed — H5 share waits are unreachable in legacy mode because
    /// `live_share_holders` errors out first.
    fn register_wait_edge(&self, self_xid: TxnId, blocking: TxnId) {
        let waiter = self
            .row_waiter
            .as_ref()
            .expect("H5 share-lock wait requires a row waiter");
        waiter.register_row_wait(self_xid, blocking);
    }

    /// §9.1 steps 5b–5c: block until `blocking_xid` ends. The caller must
    /// have dropped every page latch already; the wait edge was registered
    /// by [`Self::row_lock_gate`] while the latch was still held.
    ///
    /// A [`TxnError::DeadlockVictim`] interruption (M2c Stage R) maps to
    /// [`HeapError::DeadlockVictim`] so the SQL layer can tell a
    /// detector-chosen abort from an internal wait failure.
    fn wait_row_lock(&self, self_xid: TxnId, blocking_xid: TxnId) -> Result<()> {
        let waiter = self
            .row_waiter
            .as_ref()
            .expect("row_lock_gate only returns Wait with a waiter installed");
        waiter.wait_for(self_xid, blocking_xid).map_err(|e| {
            // Unreachable through the gate (it never returns
            // `Wait(self_xid)`), but a failed wait must not leak the
            // registered edge — Stage R's deadlock detector reads the
            // registry as the wait-for graph. (Idempotent: `wait_for`
            // already cleared the edge on its own error paths.)
            waiter.unregister_row_wait(self_xid);
            match e {
                pg_txn::TxnError::DeadlockVictim(_) => HeapError::DeadlockVictim,
                other => HeapError::InvalidArgument(format!("row-lock wait failed: {other}")),
            }
        })
    }

    /// Acquire the §9.1 row lock on the tuple at `tid` WITHOUT deleting it
    /// (M2c Stage P: `SELECT ... FOR UPDATE`): stamps
    /// `t_xmax = snapshot.current_xid()` with [`HEAP_XMAX_LOCK_ONLY`] set and
    /// `t_cid = snapshot.curcid()`.
    ///
    /// Same 5-step protocol as delete/update: an INVALID/self/terminal
    /// stamp is (re)acquired immediately under the page write latch; a
    /// stamp by a still-active OTHER transaction registers the wait edge
    /// under the latch, releases it, blocks in `wait_for`, and restarts.
    ///
    /// The lock-only stamp is NOT WAL-logged (see the module docs): it is a
    /// transient concurrency marker, not a visibility fact. The lock is held
    /// until the stamper's transaction ends — which is exactly what the next
    /// locker's gate consults via the CLOG/active set.
    ///
    /// # Errors
    ///
    /// [`HeapError::TupleConcurrentlyUpdated`] if the row version was
    /// deleted or updated by a transaction that has since committed; in
    /// legacy no-waiter mode that condition (and any in-progress holder) is
    /// [`HeapError::TupleNotFound`] instead — see [`Self::row_lock_gate`].
    pub fn lock_tuple(&self, tid: Tid, snapshot: &Snapshot, clog: &dyn ClogAccessor) -> Result<()> {
        let self_xid = snapshot.current_xid();
        debug_assert!(
            self_xid != TxnId::INVALID,
            "lock_tuple with INVALID current_xid would stamp a no-op lock"
        );
        // §9.1 restart loop (steps 5d→1): identical shape to delete/update.
        // Every wait implies the counterparty ended (progress), so the loop
        // converges in practice; the counter turns a hypothetical livelock
        // into a debug-build panic instead of a silent spin (P2-2).
        let mut restarts = 0u32;
        loop {
            restarts += 1;
            debug_assert!(
                restarts < 10_000,
                "lock_tuple restart loop failed to converge (xid {self_xid})"
            );
            let mut guard = self.buffer_pool.pin_mut(tid.page_id)?;
            let gate = {
                let page = as_page_mut(&mut guard);
                self.row_lock_gate(page, tid, self_xid, clog, LockRequest::Exclusive)?
            };
            match gate {
                RowLockGate::Proceed => {
                    let page = as_page_mut(&mut guard);
                    Self::stamp_lock_only(page, tid, self_xid, snapshot.curcid(), false)?;
                    return Ok(());
                }
                RowLockGate::ProceedNoStamp => {
                    unreachable!("exclusive lock requests never coalesce with share holders")
                }
                RowLockGate::Wait(blocking) => {
                    // Step 5b: release the latch BEFORE sleeping (the edge
                    // is already registered, so no wakeup can be missed);
                    // 5c: block; the loop restarts at step 1.
                    drop(guard);
                    self.wait_row_lock(self_xid, blocking)?;
                }
            }
        }
    }

    /// Stamp a shared row lock (FOR SHARE, Stage S multixact lite; real
    /// share/share coexistence since post-Stage-S review H5). Like
    /// [`lock_tuple`](fn.lock_tuple.html) but the stamp carries
    /// [`HEAP_XMAX_IS_SHARE`] alongside [`HEAP_XMAX_LOCK_ONLY`], and — the
    /// H5 difference — additional share lockers COEXIST: the first holder's
    /// XID stays in `t_xmax` while every holder is tracked in the
    /// `share_locks` registry. A writer or FOR UPDATE requester waits for
    /// ALL live holders; a same-transaction FOR SHARE → FOR UPDATE/write
    /// upgrade waits for every OTHER holder. The row stays visible to all
    /// snapshots — a shared lock is not a delete.
    pub fn lock_tuple_shared(
        &self,
        tid: Tid,
        snapshot: &Snapshot,
        clog: &dyn ClogAccessor,
    ) -> Result<()> {
        let self_xid = snapshot.current_xid();
        debug_assert!(
            self_xid != TxnId::INVALID,
            "lock_tuple_shared with INVALID current_xid would stamp a no-op lock"
        );
        let mut restarts = 0u32;
        loop {
            restarts += 1;
            debug_assert!(
                restarts < 10_000,
                "lock_tuple_shared restart loop failed to converge (xid {self_xid})"
            );
            let mut guard = self.buffer_pool.pin_mut(tid.page_id)?;
            let gate = {
                let page = as_page_mut(&mut guard);
                self.row_lock_gate(page, tid, self_xid, clog, LockRequest::Shared)?
            };
            match gate {
                RowLockGate::Proceed => {
                    // Fresh stamp: no live holders remain, so `t_xmax`
                    // becomes mine and the gate reset the holder registry
                    // to just me.
                    let page = as_page_mut(&mut guard);
                    Self::stamp_lock_only(page, tid, self_xid, snapshot.curcid(), true)?;
                    return Ok(());
                }
                RowLockGate::ProceedNoStamp => {
                    // share/share coexistence (H5): registered as an
                    // additional holder; the stamp names the first holder
                    // and stays unchanged. Also the no-downgrade path for a
                    // row I already hold exclusively.
                    return Ok(());
                }
                RowLockGate::Wait(blocking) => {
                    drop(guard);
                    self.wait_row_lock(self_xid, blocking)?;
                }
            }
        }
    }

    /// Stamp the §9.1 lock-only mark (`t_xmax` + [`HEAP_XMAX_LOCK_ONLY`] +
    /// `t_cid`) in place. The tuple stays visible to every snapshot — a
    /// lock is not a delete — but the row-lock protocol treats the
    /// non-INVALID `t_xmax` as "row locked" until the stamper ends.
    ///
    /// The `t_cid` overwrite is LOSSY: if the same statement first inserted
    /// this row and then locked it (self-insert at `t_cid == curcid`,
    /// re-locked at the same curcid), the row would read as written-by-
    /// current-command and become invisible to the statement's own re-scan.
    /// Unreachable today: the executor never locks a row it wrote in the
    /// same statement (FOR UPDATE scans see only earlier-command rows), so
    /// the overwritten `t_cid` is always from a completed command.
    ///
    /// TODO: revisit when subtransactions or EvalPlanQual land — both make
    /// same-command lock-after-write reachable and need a non-lossy
    /// cmin/cmax representation (see the Stage O trade-off entry in
    /// docs/stage_spec.md).
    fn stamp_lock_only(
        page: &mut [u8; PAGE_SIZE],
        tid: Tid,
        locker: TxnId,
        curcid: u32,
        shared: bool,
    ) -> Result<()> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        header.t_xmax = locker;
        header.t_cid = curcid;
        header.t_infomask |= HEAP_XMAX_LOCK_ONLY;
        if shared {
            header.t_infomask |= HEAP_XMAX_IS_SHARE;
        } else {
            // An exclusive stamp supersedes a shared one (same-transaction
            // FOR SHARE → FOR UPDATE upgrade, H5): leave no stale IS_SHARE
            // bit behind, or the gate would keep consulting the holder
            // registry the upgrade already discarded.
            header.t_infomask &= !HEAP_XMAX_IS_SHARE;
        }
        header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
        Ok(())
    }
}

/// The §9.1 gate's verdict for one tuple (see [`HeapAM::row_lock_gate`]).
enum RowLockGate {
    /// The caller may stamp the tuple now, still under the page latch.
    Proceed,
    /// Shared-lock coexistence (post-Stage-S review H5): the caller is
    /// registered as an additional share holder and the tuple stamp stays
    /// unchanged, so the caller must NOT re-stamp. Only produced for
    /// [`LockRequest::Shared`]; writers and exclusive lockers never see it.
    ProceedNoStamp,
    /// `t_xmax` (or a registered share holder) names a still-active OTHER
    /// transaction; the wait edge is registered. The caller drops every
    /// latch, blocks in `wait_for`, and restarts the protocol from step 1.
    Wait(TxnId),
}

/// What a [`HeapAM::row_lock_gate`] caller wants to do with the tuple once
/// the gate lets it through (post-Stage-S review H5 — replaces the Stage P
/// boolean `for_lock`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LockRequest {
    /// Delete or update: stamps a REAL `t_xmax` (supersedes any lock stamp).
    Writer,
    /// `SELECT ... FOR UPDATE` ([`HeapAM::lock_tuple`]): exclusive lock-only
    /// stamp. Excludes share holders and is excluded by them.
    Exclusive,
    /// `SELECT ... FOR SHARE` ([`HeapAM::lock_tuple_shared`]): shared
    /// lock-only stamp. Coexists with other share holders (H5 multixact
    /// lite); excluded by writers/exclusive lockers.
    Shared,
}

/// The `t_xmax` a visibility judgment should see: a [`HEAP_XMAX_LOCK_ONLY`]
/// stamp is a row lock, NOT a delete, so it is masked to INVALID — a locked
/// row stays live for everyone, subject to the normal `t_xmin` rules.
/// Real delete/update stamps pass through unchanged.
fn visibility_xmax(header: &TupleHeader) -> TxnId {
    if header.t_infomask & HEAP_XMAX_LOCK_ONLY != 0 {
        TxnId::INVALID
    } else {
        header.t_xmax
    }
}

/// Follow a HOT chain forward from an invisible version (Stage S; the single
/// shared forward follower, post-Stage-S review H1): walk `t_ctid` links
/// starting at `start` until a version visible under `snapshot` is found and
/// return its TID, or `Ok(None)` when the chain ends without one.
///
/// The caller MUST already hold the chain page's pin (read or write): a HOT
/// chain never leaves its page (`HEAP_HOT_UPDATED` is only stamped by the
/// same-page fast path), and re-pinning here would be a recursive latch
/// acquisition, which deadlocks as soon as a writer queues (see
/// docs/stage_spec.md Stage S, 设计理由 6). A `t_ctid` pointing off-page is
/// corruption and ends the walk.
///
/// The walk runs to the chain END, bounded by the page's slot count as the
/// cycle guard: a well-formed chain terminates (`t_ctid == self`, or a
/// version without `HEAP_HOT_UPDATED`) within that many hops, so exhausting
/// the bound means a cycle — reported loudly as [`HeapError::Corrupted`].
/// (The pre-H1 hardcoded 8-hop cap silently dropped every version beyond the
/// eighth from scans and index lookups; in a vacuum-less system chains only
/// grow.)
pub fn follow_hot_chain(
    page: &[u8; PAGE_SIZE],
    page_id: PageId,
    start: Tid,
    snapshot: &Snapshot,
    clog: &dyn ClogAccessor,
) -> Result<Option<Tid>> {
    let slot_count = SlottedPage::slot_count(page) as u16;
    let mut chain_tid = start;
    for _ in 0..slot_count {
        if chain_tid.page_id != page_id {
            return Ok(None);
        }
        let Some(bytes) = SlottedPage::tuple(page, chain_tid.slot_id)? else {
            return Ok(None);
        };
        let header = TupleHeader::read_from(&bytes[..TUPLE_HEADER_SIZE])?;
        if is_visible(
            header.t_xmin,
            visibility_xmax(&header),
            header.t_cid,
            snapshot,
            clog,
        ) {
            return Ok(Some(chain_tid));
        }
        if header.t_infomask2 & HEAP_HOT_UPDATED != 0 && header.t_ctid != chain_tid {
            chain_tid = header.t_ctid;
        } else {
            return Ok(None);
        }
    }
    Err(HeapError::Corrupted(format!(
        "HOT chain on page {page_id} exceeds {slot_count} hops (cycle?)"
    )))
}

/// The TID that owns `tid`'s index entries (Stage S; the single shared
/// reverse follower, post-Stage-S review H1/B6).
///
/// A `HEAP_ONLY_TUPLE` version was created without index entries of its own —
/// that is the whole point of HOT — so index maintenance for such a version
/// must act on its chain root: the nearest ancestor version that is not
/// `HEAP_ONLY_TUPLE`. HOT chains never leave their page, so the root is found
/// by walking `t_ctid` links backwards. Every version in a HOT chain shares
/// the same indexed columns, so the root's entry carries exactly the key the
/// caller read back from `tid`. The caller MUST hold the page's pin.
///
/// The walk builds the page's `t_ctid → slot` map ONCE (B6: the naive
/// per-hop full slot scan is O(slots²) on deep chains) and is bounded by the
/// slot count; exhausting the bound means a cycle — [`HeapError::Corrupted`].
/// A `HEAP_ONLY` tuple with no predecessor link on the page is itself the
/// root candidate (returned as-is), matching the pre-H1 engine behavior.
pub fn hot_chain_root(page: &[u8; PAGE_SIZE], page_id: PageId, tid: Tid) -> Result<Tid> {
    let slot_count = SlottedPage::slot_count(page) as u16;
    // Forward links of the whole page, built once: t_ctid → slot for every
    // HOT_UPDATED tuple (B6).
    let mut links: HashMap<Tid, u16> = HashMap::new();
    for slot in 0..slot_count {
        let Some(bytes) = SlottedPage::tuple(page, slot)? else {
            continue;
        };
        let header = TupleHeader::read_from(&bytes[..TUPLE_HEADER_SIZE])?;
        if header.t_infomask2 & HEAP_HOT_UPDATED != 0 {
            links.insert(header.t_ctid, slot);
        }
    }

    let mut cur = tid;
    for _ in 0..slot_count {
        let Some(bytes) = SlottedPage::tuple(page, cur.slot_id)? else {
            return Ok(cur);
        };
        let header = TupleHeader::read_from(&bytes[..TUPLE_HEADER_SIZE])?;
        if header.t_infomask2 & HEAP_ONLY_TUPLE == 0 {
            return Ok(cur);
        }
        match links.get(&cur) {
            Some(&slot) => {
                cur = Tid {
                    page_id,
                    slot_id: slot,
                };
            }
            None => return Ok(cur),
        }
    }
    Err(HeapError::Corrupted(format!(
        "HOT chain root search on page {page_id} exceeds {slot_count} hops (cycle?)"
    )))
}

impl AccessMethod for HeapAM {
    fn name(&self) -> &'static str {
        "heap"
    }

    fn insert(&self, ctx: InsertContext<'_>) -> Result<()> {
        let InsertContext {
            rel,
            snapshot,
            tuple,
            out_tid,
        } = ctx;
        Self::validate_tuple_len(tuple)?;
        // A tuple written with an INVALID writer XID would be invisible to
        // every scan forever (`is_effectively_committed` rejects INVALID on
        // sight) — a silent dead row. That is always a caller bug; catch it.
        debug_assert!(
            snapshot.current_xid() != pg_storage::types::TxnId::INVALID,
            "heap insert with INVALID current_xid produces an unreadable tuple"
        );
        // Stamp t_xmin with the writer's own XID before the bytes reach the
        // WAL record or the page (see the module docs).
        let tuple = Self::stamp_xmin(tuple, snapshot.current_xid())?;

        let needed = tuple.len() + LINE_POINTER_SIZE;
        let mut guard = self.acquire_page_with_room(&rel, needed, PageId::INVALID)?;
        let page_id = guard.page_id();
        let page = as_page_mut(&mut guard);

        // §4.6 explicit slot addressing: choose the slot FIRST (first-fit
        // recycling of an Unused slot, else append at slot_count), carry it
        // in the WAL record, then place the tuple at exactly that slot. Redo
        // replays `add_tuple_at(rec.slot_id)` and no longer depends on
        // `add_tuple` reproducing the online writer's choice.
        let slot =
            SlottedPage::first_fit_slot(page).unwrap_or(SlottedPage::slot_count(page) as u16);
        let rec = WalRecord::heap_insert(page_id, slot, tuple.clone(), snapshot.current_xid())?;
        let lsn = self.wal_writer.append(rec)?;
        SlottedPage::add_tuple_at(page, slot, &tuple)?;
        stamp_pd_lsn(page, lsn);

        if let Some(out) = out_tid {
            *out = Tid {
                page_id,
                slot_id: slot,
            };
        }
        Ok(())
    }

    fn scan(&self, ctx: ScanContext<'_>) -> Result<Vec<(Tid, Vec<Option<crate::tuple::Datum>>)>> {
        let clog = ctx.clog;
        let mut out = Vec::new();
        for page_id in self.relation_pages(&ctx.rel)? {
            let guard = self.buffer_pool.pin(page_id)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            // A fresh (never-inserted) page has no tuples to yield.
            if SlottedPage::header(page).pd_upper == 0 {
                continue;
            }
            let slot_count = SlottedPage::slot_count(page) as u16;
            for slot in 0..slot_count {
                let Some(bytes) = SlottedPage::tuple(page, slot)? else {
                    continue;
                };
                let (header, values) = decode_tuple(bytes, ctx.rel.columns)?;
                // Skip HOT chain members — only the chain root (not marked
                // HEAP_ONLY_TUPLE) is directly scannable; chain members are
                // reached via t_ctid when the root is invisible.
                if header.t_infomask2 & HEAP_ONLY_TUPLE != 0 {
                    continue;
                }
                let self_tid = Tid {
                    page_id,
                    slot_id: slot,
                };
                if is_visible(
                    header.t_xmin,
                    visibility_xmax(&header),
                    header.t_cid,
                    ctx.snapshot,
                    clog,
                ) {
                    out.push((self_tid, values));
                } else if header.t_infomask2 & HEAP_HOT_UPDATED != 0 && header.t_ctid != self_tid {
                    // HOT chain: old version is invisible but t_ctid may
                    // point to a newer version visible to this snapshot.
                    // The walk reads the page pinned above (HOT chains never
                    // leave their page; re-pinning here would be a recursive
                    // read latch, which deadlocks as soon as a writer queues
                    // between the two acquisitions) and runs to the chain
                    // end, slot-count bounded — see `follow_hot_chain`.
                    if let Some(visible_tid) =
                        follow_hot_chain(page, page_id, header.t_ctid, ctx.snapshot, clog)?
                    {
                        let chain_bytes = SlottedPage::tuple(page, visible_tid.slot_id)?
                            .expect("follow_hot_chain only returns readable slots");
                        let (_, chain_values) = decode_tuple(chain_bytes, ctx.rel.columns)?;
                        out.push((visible_tid, chain_values));
                    }
                }
            }
        }
        Ok(out)
    }

    fn delete(&self, ctx: DeleteContext<'_>) -> Result<()> {
        let tid = ctx.tid;
        let xmax = ctx.snapshot.current_xid();

        // §9.1 restart loop: each iteration re-pins the page and re-runs the
        // gate from step 1; only a `Proceed` verdict falls through to the
        // WAL + stamp, still under the latch (check+stamp is the protocol's
        // "CAS"). Every wait implies the counterparty ended (progress), so
        // the loop converges in practice; the counter turns a hypothetical
        // livelock into a debug-build panic instead of a silent spin (P2-2).
        let mut restarts = 0u32;
        loop {
            restarts += 1;
            debug_assert!(
                restarts < 10_000,
                "delete restart loop failed to converge (xid {xmax})"
            );
            let mut guard = self.buffer_pool.pin_mut(tid.page_id)?;
            let gate = {
                let page = as_page_mut(&mut guard);
                self.row_lock_gate(page, tid, xmax, ctx.clog, LockRequest::Writer)?
            };
            if let RowLockGate::Wait(blocking) = gate {
                // Step 5b/5c: release the latch BEFORE sleeping (the edge
                // is already registered, so the wakeup cannot be missed),
                // then block; the loop restarts at step 1.
                drop(guard);
                self.wait_row_lock(xmax, blocking)?;
                continue;
            }
            debug_assert!(
                matches!(gate, RowLockGate::Proceed),
                "writer gates never coalesce with share holders"
            );

            let page = as_page_mut(&mut guard);
            // Validate-then-WAL discipline is unchanged (the gate ran first):
            // a rejected delete leaves no HeapDelete record behind for
            // recovery to choke on.
            let rec = WalRecord::heap_delete(tid, xmax, xmax)?;
            let lsn = self.wal_writer.append(rec)?;
            Self::stamp_deleted(page, tid, xmax, ctx.snapshot.curcid(), false)?;
            stamp_pd_lsn(page, lsn);
            return Ok(());
        }
    }

    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>> {
        // Single source of truth (F4): the trait method delegates to the
        // canonical constructor so the two can never drift apart again (this
        // body previously lagged by two handlers — HeapHotUpdate and
        // HeapCleanup).
        heap_redo_handlers()
    }
}

impl UpdatableAM for HeapAM {
    fn update(&self, ctx: UpdateContext<'_>) -> Result<()> {
        let UpdateContext {
            rel,
            snapshot,
            old_tid,
            new_tuple,
            out_tid,
            clog,
            hot_eligible,
        } = ctx;
        Self::validate_tuple_len(new_tuple)?;
        let xmax = snapshot.current_xid();
        // Stamp the new version's t_xmin with the writer's own XID (module
        // docs); t_xmax of the old version is stamped by `stamp_deleted`.
        let new_tuple = Self::stamp_xmin(new_tuple, xmax)?;
        let needed = new_tuple.len() + LINE_POINTER_SIZE;

        // §9.1 restart loop: the gate (steps 1–5a) runs under the old page's
        // write latch; on `Wait` EVERY latch is dropped before sleeping, and
        // the whole path — including the room check and any chain extension
        // — restarts from step 1 (the tuple's state may have changed
        // arbitrarily while we slept). Convergence argument and the debug
        // counter: same as delete/lock_tuple (P2-2).
        let mut restarts = 0u32;
        loop {
            restarts += 1;
            debug_assert!(
                restarts < 10_000,
                "update restart loop failed to converge (xid {xmax})"
            );
            // Fast path: pin the old page, run the gate, and check whether
            // the new version fits alongside it (single latch, single page).
            // Slot selection is explicit (§4.6): stamping the old tuple is a
            // logical delete (LP stays Normal), so first-fit is unaffected by
            // the stamp and the chosen slot stays valid through placement.
            let mut old_guard = self.buffer_pool.pin_mut(old_tid.page_id)?;
            let gate = {
                let old_page = as_page_mut(&mut old_guard);
                self.row_lock_gate(old_page, old_tid, xmax, clog, LockRequest::Writer)?
            };
            if let RowLockGate::Wait(blocking) = gate {
                drop(old_guard);
                self.wait_row_lock(xmax, blocking)?;
                continue;
            }
            debug_assert!(
                matches!(gate, RowLockGate::Proceed),
                "writer gates never coalesce with share holders"
            );
            let old_has_room = {
                let old_page = as_page_mut(&mut old_guard);
                SlottedPage::free_space(old_page) >= needed
            };

            if old_has_room {
                let page_id = old_guard.page_id();
                let old_page = as_page_mut(&mut old_guard);
                // §4.6: pick the slot BEFORE writing WAL — first-fit recycles
                // an Unused slot left by compact(), else append at slot_count.
                let new_slot = SlottedPage::first_fit_slot(old_page)
                    .unwrap_or(SlottedPage::slot_count(old_page) as u16);
                let new_tid = Tid {
                    page_id,
                    slot_id: new_slot,
                };
                if hot_eligible {
                    let hot_tuple = Self::stamp_hot_only(&new_tuple, xmax)?;
                    let rec = WalRecord::heap_hot_update(
                        page_id,
                        old_tid.slot_id,
                        new_slot,
                        hot_tuple.clone(),
                        xmax,
                        xmax,
                    )?;
                    let lsn = self.wal_writer.append(rec)?;
                    Self::stamp_hot_update(old_page, old_tid, xmax, snapshot.curcid(), new_tid)?;
                    SlottedPage::add_tuple_at(old_page, new_slot, &hot_tuple)?;
                    stamp_pd_lsn(old_page, lsn);
                } else {
                    let rec =
                        WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.clone(), xmax)?;
                    let lsn = self.wal_writer.append(rec)?;
                    Self::stamp_deleted(old_page, old_tid, xmax, snapshot.curcid(), true)?;
                    SlottedPage::add_tuple_at(old_page, new_slot, &new_tuple)?;
                    stamp_pd_lsn(old_page, lsn);
                }
                if let Some(out) = out_tid {
                    *out = new_tid;
                }
                return Ok(());
            }

            // Cross-page: the old page has no room. Drop its latch BEFORE
            // acquiring the new page: chain extension pins the chain tail, and
            // holding the old page's latch across that would invert the lock
            // order (extend path: extend_lock → tail latch) and could deadlock
            // when the old page IS the tail. The gate is re-run below after
            // re-pinning, before any heap WAL record is written; a page
            // allocated on behalf of an update that loses that race is simply
            // left empty (still tracked in the page cache, reused by the next
            // insert) — never a poison WAL record.
            drop(old_guard);
            let new_guard = self.acquire_page_with_room(&rel, needed, old_tid.page_id)?;
            let new_page_id = new_guard.page_id();

            // Two-latch acquisition follows a GLOBAL order — smaller PageId
            // first (M2c Stage P review): two concurrent cross-page updates
            // can pick each other's old page as their new page, and an
            // unordered hold-and-wait is an AB/BA deadlock on buffer-pool
            // latches, which have no timeout and are invisible to Stage R's
            // (lock-manager-based) deadlock detector. When the old page is
            // the smaller one, the new guard is dropped and both pages are
            // re-latched in order; the new page's room is re-checked because
            // a filler may have taken it in between (restart the whole
            // protocol if so — the fast path above will re-evaluate).
            let (mut old_guard, mut new_guard) = if old_tid.page_id < new_page_id {
                drop(new_guard);
                let old_guard = self.buffer_pool.pin_mut(old_tid.page_id)?;
                let new_guard = self.buffer_pool.pin_mut(new_page_id)?;
                let new_has_room = {
                    let new_page: &[u8; PAGE_SIZE] =
                        new_guard.page().try_into().expect("frame is PAGE_SIZE");
                    SlottedPage::free_space(new_page) >= needed
                };
                if !new_has_room {
                    drop(new_guard);
                    drop(old_guard);
                    continue;
                }
                (old_guard, new_guard)
            } else {
                (self.buffer_pool.pin_mut(old_tid.page_id)?, new_guard)
            };

            // Re-run the gate under the old page's latch before writing WAL
            // (a rejected update must leave no HeapUpdate record behind for
            // recovery to choke on — same discipline as delete). The gate is
            // CLOG-aware: a tuple whose committed deleter stamped it while
            // this update dropped the latch is rejected, not overwritten; an
            // in-progress holder sends us to sleep; an aborted stamp does not
            // count.
            let gate = {
                let old_page = as_page_mut(&mut old_guard);
                self.row_lock_gate(old_page, old_tid, xmax, clog, LockRequest::Writer)?
            };
            if let RowLockGate::Wait(blocking) = gate {
                // Sleep holding NO latch: drop the new page's guard too — a
                // blocked waiter must never hold a write latch.
                drop(old_guard);
                drop(new_guard);
                self.wait_row_lock(xmax, blocking)?;
                continue;
            }
            debug_assert!(
                matches!(gate, RowLockGate::Proceed),
                "writer gates never coalesce with share holders"
            );

            // The new slot is chosen only now, under the final latching
            // (§4.6 explicit addressing): in the re-ordered acquisition above
            // the new page may have been dropped and re-pinned, so any
            // earlier slot choice is stale. First-fit recycles an Unused slot
            // left by compact() — acquire_page_with_room reverse-scans from
            // the tail, so a compacted middle page is a valid update target.
            let new_slot = {
                let new_page = as_page_mut(&mut new_guard);
                SlottedPage::first_fit_slot(new_page)
                    .unwrap_or(SlottedPage::slot_count(new_page) as u16)
            };
            let new_tid = Tid {
                page_id: new_page_id,
                slot_id: new_slot,
            };

            let rec = WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.clone(), xmax)?;
            let lsn = self.wal_writer.append(rec)?;

            {
                let old_page = as_page_mut(&mut old_guard);
                Self::stamp_deleted(old_page, old_tid, xmax, snapshot.curcid(), true)?;
                stamp_pd_lsn(old_page, lsn);
            }
            {
                let new_page = as_page_mut(&mut new_guard);
                SlottedPage::add_tuple_at(new_page, new_slot, &new_tuple)?;
                stamp_pd_lsn(new_page, lsn);
            }

            if let Some(out) = out_tid {
                *out = new_tid;
            }
            return Ok(());
        }
    }
}

/// The output of [`HeapAM::classify_dead_tuples`] (M3 Stage C): the dead
/// list grouped by HOT chain and split into the reclaimable part.
struct DeadClassification {
    /// Index-cleanup items `(tid, decoded column values)`: standalone dead
    /// tuples as themselves; fully-dead HOT chains as their ROOT (the root
    /// owns the chain's index entries). Consumed by `collect_index_keys`.
    index_keys: Vec<(Tid, Vec<Option<Datum>>)>,
    /// Physical kill list, page → ascending slot ids: standalone dead tuples
    /// plus EVERY member slot of each fully-dead chain. Consumed by
    /// `reclaim`. Deterministic page order (BTreeMap) so multi-page vacuums
    /// append their `HeapCleanup` records in a stable order.
    kills: BTreeMap<PageId, Vec<u16>>,
}

impl Vacuumable for HeapAM {
    /// Scan `rel` for dead tuples.
    ///
    /// # InProgress vs Aborted (建档 note)
    ///
    /// The "InProgress ≡ Aborted for visibility" equivalence that recovery
    /// relies on (pg-storage `analysis` module docs) holds ONLY for
    /// visibility, not for reclamation: a tuple inserted by a crashed
    /// transaction has `t_xmin` whose CLOG entry reads `InProgress` (no
    /// terminal record exists), so case 1 below would NOT collect it until
    /// the crashed XID is explicitly stamped ABORTED. That stamping is
    /// exactly what Stage S's `HeapUndoHandler` does during recovery undo
    /// (crates/pg-am-heap/src/undo.rs: every ATT member is marked Aborted
    /// in the CLOG), so by the time vacuum runs post-recovery, crashed
    /// inserters read Aborted and case 1 collects their orphans.
    fn scan_dead_tuples(
        &self,
        rel: RelationDesc<'_>,
        oldest_xmin: TxnId,
        clog: &dyn ClogAccessor,
    ) -> Result<Vec<Tid>> {
        // A dead tuple is one of:
        //
        // 1. **Aborted inserter** (`t_xmin` aborted): the row was never
        //    visible to anyone and never will be, so it is dead regardless of
        //    `oldest_xmin`. Stage J made this reachable — before real aborts
        //    existed, no such rows could be produced. PG's vacuum reclaims
        //    aborted-insert tuples by the same rule.
        // 2. **Committed deleter** (`t_xmax` committed and older than
        //    `oldest_xmin`): no live snapshot can still see the row.
        //
        // The caller-supplied `clog` decides committedness authoritatively: a
        // tuple whose deleter aborted is NOT dead (the delete never took
        // effect). Only the tuple header is needed (xmin/xmax live at fixed
        // offsets), so no schema is required.
        let page_ids = self.relation_pages(&rel)?;

        let mut dead = Vec::new();
        for page_id in page_ids {
            let guard = self.buffer_pool.pin(page_id)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            if SlottedPage::header(page).pd_upper == 0 {
                continue;
            }
            let slot_count = SlottedPage::slot_count(page) as u16;
            for slot in 0..slot_count {
                let Some(bytes) = SlottedPage::tuple(page, slot)? else {
                    continue;
                };
                let header = match TupleHeader::read_from(bytes) {
                    Ok(h) => h,
                    // A single corrupted tuple must not abort the whole scan:
                    // vacuum is a background maintenance pass, so skip the
                    // unreadable slot and keep going (the corruption itself is
                    // surfaced loudly via the log).
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            %page_id,
                            slot,
                            "scan_dead_tuples: skipping undecodable tuple"
                        );
                        continue;
                    }
                };
                if clog.get_state(header.t_xmin) == TxnState::Aborted {
                    dead.push(Tid {
                        page_id,
                        slot_id: slot,
                    });
                    continue;
                }
                let xmax = header.t_xmax;
                // A HEAP_XMAX_LOCK_ONLY stamp is a row lock, not a delete:
                // the tuple is never dead because of it (§9.1, M2c Stage P).
                if header.t_infomask & HEAP_XMAX_LOCK_ONLY == 0
                    && xmax != TxnId::INVALID
                    && xmax.0 < oldest_xmin.0
                    && clog.get_state(xmax) == TxnState::Committed
                {
                    dead.push(Tid {
                        page_id,
                        slot_id: slot,
                    });
                }
            }
        }
        Ok(dead)
    }

    /// M3 Stage C (READ-ONLY): the `(tid, column values)` pairs whose index
    /// entries need cleanup — standalone dead tuples as themselves,
    /// fully-dead HOT chains as their root, partially-dead chains not at
    /// all. See the trait docs and `HeapAM::classify_dead_tuples`. Runs
    /// entirely under read pins; MUST be called before `reclaim` (§4.1 stage
    /// 2: after compaction the keys are unreadable).
    fn collect_index_keys(
        &self,
        rel: RelationDesc<'_>,
        dead: &[Tid],
    ) -> Result<Vec<(Tid, Vec<Option<Datum>>)>> {
        Ok(self.classify_dead_tuples(&rel, dead)?.index_keys)
    }

    /// M3 Stage C (PURELY PHYSICAL): kill the reclaimable slots of `dead`
    /// via `compact()` under `HeapCleanup` WAL records; unlink pages that
    /// become fully empty (the unlink rides in the same record) and return
    /// them to the allocator (`free_page` / `PageFree`).
    ///
    /// The kill set is RE-DERIVED from `dead` by the same grouping helper
    /// `collect_index_keys` uses, so passing the raw `scan_dead_tuples`
    /// output is safe: members of partially-dead chains in the input are
    /// left untouched (§4.2), keeping the `compact()` kill-list contract
    /// (never kill a member still referenced by a predecessor's `t_ctid`,
    /// never kill a chain root while any member lives) structural rather
    /// than caller-discipline.
    ///
    /// ORDER IS IRREVERSIBLE for empty pages: unlink (inside the
    /// `HeapCleanup` record) THEN `free_page` (PageFree). The reverse —
    /// free-then-unlink — can leave the page on BOTH the chain and the
    /// freelist after a crash in the window between the two records; once
    /// the allocator rehands the page and a writer fills it, the chain
    /// walks into live tuples of another owner: structural corruption. This
    /// order's worst case is a single-page leak (unlinked but never freed,
    /// crash window ② of §12.1), the accepted trade-off.
    fn reclaim(&self, rel: RelationDesc<'_>, dead: &[Tid]) -> Result<()> {
        let classification = self.classify_dead_tuples(&rel, dead)?;
        if classification.kills.is_empty() {
            return Ok(());
        }
        // Chain-order snapshot of the relation's pages: the predecessor of a
        // spliced page is its left neighbor here.
        let chain_pages = self.relation_pages(&rel)?;
        // Pages already spliced out THIS pass. When several chain-adjacent
        // pages all become empty, each unlink must relink the nearest
        // STILL-CHAINED predecessor — never a page already removed (relinking
        // a removed page would leave the live chain pointing at the page now
        // being freed: structural corruption once the allocator rehands it).
        let mut removed: HashSet<PageId> = HashSet::new();

        for (page_id, kills) in &classification.kills {
            // Under a read pin: does killing `kills` empty the page (every
            // non-Unused slot is on the list), and who is the current
            // successor (the relink target)?
            let (becomes_empty, next_page) = {
                let guard = self.buffer_pool.pin(*page_id)?;
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
                let slot_count = SlottedPage::slot_count(page) as u16;
                let mut live = 0usize;
                for slot in 0..slot_count {
                    if SlottedPage::line_pointer(page, slot)?.flags() != LpFlags::Unused {
                        live += 1;
                    }
                }
                (live == kills.len(), SlottedPage::next_page(page)?)
            };

            // The chain HEAD is never unlinked or freed, even when empty: it
            // is the relation's anchor (`RelationDesc::first_page` is what
            // the catalog and `seed_from_chain` start from). An empty head
            // stays as the head of a one-page chain and is simply compacted.
            if becomes_empty && *page_id != rel.first_page {
                let pos = chain_pages
                    .iter()
                    .position(|p| p == page_id)
                    .ok_or_else(|| {
                        HeapError::Corrupted(format!(
                            "page {page_id} of rel {} missing from the chain-order page cache",
                            rel.rel_oid
                        ))
                    })?;
                // Nearest still-chained left neighbor (see `removed` above).
                // The head is never removed, so the scan always terminates.
                let prev = chain_pages[..pos]
                    .iter()
                    .rev()
                    .find(|p| !removed.contains(p))
                    .copied()
                    .ok_or_else(|| {
                        HeapError::Corrupted(format!(
                            "page {page_id} of rel {} has no live predecessor in the chain",
                            rel.rel_oid
                        ))
                    })?;
                let unlink_next = next_page.unwrap_or(PageId::INVALID);
                // Resolve the allocator BEFORE any page modification: a
                // missing allocator is a configuration error knowable without
                // touching the page. Failing AFTER the compact instead would
                // leave the release half-applied — the page compacted and
                // unlinked but neither cache-evicted nor freed (a stale cache
                // entry could still route inserts into it) — and a caller
                // retry cannot cleanly resume from that state: the killed
                // slots are already Unused, so re-classification finds
                // nothing to kill and never re-enters the unlink/free branch.
                // Checking upfront keeps the error atomic (nothing touched).
                let allocator = self.page_allocator.as_ref().ok_or_else(|| {
                    HeapError::InvalidArgument(
                        "reclaim: page release requires a page allocator \
                         (HeapAM::set_page_allocator)"
                            .to_string(),
                    )
                })?;
                self.compact_page(*page_id, kills, prev, unlink_next)?;
                // Evict BEFORE freeing: while the stale cache entry lives,
                // `acquire_page_with_room` could route a new insert into the
                // unlinked page (logically unreachable row). And freeing
                // must come AFTER the unlink record — see the fn docs.
                removed.insert(*page_id);
                self.evict_page(rel.rel_oid, *page_id);
                allocator.lock().free_page(*page_id)?;
            } else {
                self.compact_page(*page_id, kills, PageId::INVALID, PageId::INVALID)?;
            }
        }
        Ok(())
    }
}

/// Reinterpret a write guard's page bytes as a fixed-size page array.
fn as_page_mut<'g>(guard: &'g mut PageGuardMut<'_>) -> &'g mut [u8; PAGE_SIZE] {
    guard
        .page_mut()
        .try_into()
        .expect("buffer frame is exactly PAGE_SIZE")
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`.
///
/// A free function taking the value in a local first, so the mutable borrow for
/// the write does not overlap the immutable read of the current LSN.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}
