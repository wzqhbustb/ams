//! Concurrent Blink-tree core (Stage Q): latch-coupled reads, optimistic
//! leaf writes, pessimistic full-path write descents, physical delete, and
//! the 3-step split WAL protocol (tech-selection §13.2/§13.3).
//!
//! # Tree organization
//!
//! A relation's `first_page` is the **meta page**: a slotted page whose
//! tuples are 10-byte [`page::encode_meta_record`] records
//! `(root_page_id, tree_level)`. The *last* record is authoritative; a root
//! promotion appends a new record (WAL-logged as `BTreeInsert`, so redo
//! rebuilds it under the usual `pd_lsn` guard). The root starts as a single
//! leaf (`LEAF | ROOT`) allocated right after the meta page.
//!
//! # Latch choreography (§13.2, Stage Q)
//!
//! Deadlock freedom rests on a single global acquisition order: page
//! latches are only ever acquired **DOWN** (root → leaf) and **RIGHT**
//! (left sibling → right sibling). Never up; never left while holding any
//! latch.
//!
//! - **Reads crab down**: the child's read latch is acquired while the
//!   parent's is still held, then the parent is released. Right hops on the
//!   sibling chain are coupled the same way (the right sibling is latched
//!   before the current page is released), so a reader never observes a
//!   torn sibling link. Left hops are the one move that cannot be coupled —
//!   acquiring a left sibling while holding a page would violate the order
//!   — so they drop first, then re-acquire. That is safe: a left hop is
//!   only ever caused by a stale separator key, which can only send the
//!   walk *left of* the target, and the subsequent right walk over
//!   `btpo_next` (the ground truth) re-establishes the exact position.
//!   INSERT placement is the exception: a new entry must land on the page
//!   whose full `(key, tid)` chain position contains it, so when the
//!   candidate leaf is empty (a delete-drained split twin — M2b has no page
//!   merge) or its first entry sorts above the probe, the decider is the
//!   nearest non-empty LEFT sibling's LAST entry — only if it sorts above
//!   the probe does the walk move left. Without that, update churn (delete
//!   old entry + insert same key) migrates the boundary key's entry left of
//!   its separator on every pass, drains the owning leaf into an empty-page
//!   black hole, and eventually breaks the chain's key order; and a
//!   smaller/reused tid inserted into a boundary run would strand at the
//!   right leaf's slot 0, breaking chain order and the `DuplicateKey` check
//!   (Stage T deadlock-stress + final-review findings; see
//!   (`BTreeIndex::descend_to_leaf_for_insert`). Lookups and deletes keep
//!   the unrestricted left walk — they LOCATE an existing entry, which a
//!   split-boundary duplicate run can legitimately place on either side of
//!   a separator — plus one extension: an empty leaf with a left sibling
//!   hops left unconditionally (the right check cannot bounce back from an
//!   empty page, so this cannot cycle).
//! - **Optimistic writes** (`insert`/`delete`): the descent runs under read
//!   latches; only the target leaf is then taken exclusive, and its
//!   ownership of the probe is **re-validated under the write latch** — a
//!   concurrent split may have moved the key range right, in which case the
//!   right twin is latched in chain order (coupled, left before right) and
//!   the check repeats, bounded by `MAX_CHAIN_HOPS`. If the entry fits,
//!   it is WAL-logged and applied under that single leaf latch. There is
//!   deliberately **no latch-upgrade API**: the buffer pool does not expose
//!   `parking_lot` upgrades, and re-validation after a drop-and-re-pin is
//!   required anyway — drop-and-revalidate *is* the Blink-standard
//!   optimistic path.
//! - **Pessimistic writes**: a leaf without room drops every latch and
//!   restarts from the root with coupled **write** latches down the whole
//!   descent path (the parent write latch is held while the child write
//!   latch is acquired), re-validating ownership at every level. The path
//!   stays latched through the split, so the pages the Commit touches
//!   cannot change under us. The split's right page is **reserved**
//!   ([`BufferPool::new_page`]) before the split pair is touched; an
//!   allocation failure releases all latches and restarts the pass instead
//!   of bubbling a bare error.
//! - **Split protocol**: a split holds left + right in that order (Prepare,
//!   Copy). The Commit walks UP the already-latched path: the parent latch
//!   was acquired down-order during the descent, so it is never *newly*
//!   acquired while a child latch is held; child-level latches are released
//!   before the Commit touches the parent level, and the flag-clearing
//!   re-latch of `st.left` is a plain down-acquisition. A parent without
//!   room for the downlink is split recursively under the same discipline.
//!
//! # FPI-before-commit (Stage T P0)
//!
//! A `BTreeSplitCommit` record modifies two pages (parent downlink, left
//! flag clear). If a checkpoint opened a new FPI cycle between the split's
//! Copy and its Commit, the `pin_mut` that applies the Commit fires the
//! page's cycle FPI — and because the Commit record is appended FIRST, that
//! FPI lands AFTER the Commit while capturing a PRE-commit image
//! (`SPLIT_INCOMPLETE` still set / downlink still missing). Recovery
//! replays an FPI unconditionally and patches `pd_lsn` to the FPI's LSN, so
//! the Commit redo's `pd_lsn` guard then skips its page effects: the
//! already-committed split is rolled back to "incomplete", and the
//! undo-time page scan emits a spurious finishing CLR with a duplicate
//! parent downlink. (Stage T stress finding; forensic dump
//! /tmp/conc_repro_35: FPI @1209856 with pre-commit image for a Commit
//! @1209304.)
//!
//! The fix keeps the latch choreography exactly as above (holding the left
//! latch across the append/parent touch deadlocks against the optimistic
//! right-hop/coupling paths) and instead splits the acquisition in two:
//!
//! 1. **Pre-touch**: before the Commit record's WAL position is fixed, a
//!    scoped `pin_mut` of each page the Commit modifies (parent before
//!    left, down the tree) emits any due cycle FPI — necessarily at an LSN
//!    below the Commit's.
//! 2. **Apply**: after the append, the page is re-pinned with
//!    [`BufferPool::pin_mut_without_fpi`], which cannot fire a second FPI
//!    if a checkpoint published in the pre-touch → re-pin window. The
//!    suppressed FPI is redo-safe (the Commit's LSN exceeds that
//!    checkpoint's begin, so replay re-applies it); the residual exposure
//!    is torn-write-only — see the method's doc.
//!
//! # Third-party FPI interposition (Stage T P0 residual)
//!
//! The pre-touch/apply split leaves one window: between the Commit's append
//! and its flag-clearing apply, `st.left` is unlatched and still carries
//! `SPLIT_INCOMPLETE`. A THIRD PARTY write-latching the page in that window
//! with a plain `pin_mut` could fire the page's cycle FPI (if a checkpoint
//! published after the pre-touch) — again a post-Commit FPI with a
//! pre-Commit image. In-window writes on such a page are a DESIGNED Stage S
//! feature (the undo CLR's NoMove truncation accounts for them), so the
//! leaf write paths must neither refuse the page nor emit that FPI:
//!
//! - `pin_leaf_for_write` (optimistic insert + delete) and
//!   `descend_write_path` (pessimistic insert) acquire via
//!   `pin_mut_without_fpi`, then call [`BufferPool::ensure_fpi`] under the
//!   held latch ONLY when the page is not `SPLIT_INCOMPLETE`. The flag is
//!   stable under the held latch (Prepare sets it, the Commit's apply
//!   clears it — both need this latch), so the check is race-free; a
//!   flagged page proceeds FPI-free (the in-window record replays
//!   normally — redo-safe by the same argument as `pin_mut_without_fpi`,
//!   torn-write-only residual). Deliberately there is NO "is an FPI due?"
//!   probe feeding the decision: such a probe would TOCTOU against a
//!   checkpoint publishing between the probe and `ensure_fpi`'s own gate
//!   read (post-Stage-T-review item 2). The pessimistic descent still
//!   restarts on the flag as before (the path above a flagged page cannot
//!   be trusted) — it just no longer emits an FPI for it on the way out.
//!
//! The split protocol's own steps are unaffected: Prepare/Copy/parent
//! downlink FPIs all fire before their records' WAL positions are fixed
//! (guards held from before the append, or the step API's natural order).
//!
//! Every pessimistic pass holds the root write latch from its descent
//! through its Commit, so online splits are serialized against each other;
//! readers and optimistic leaf inserts still proceed concurrently.
//!
//! # Restart discipline and `SPLIT_INCOMPLETE`
//!
//! Any structural surprise on the write path — a stale root (its `ROOT`
//! flag was cleared by a concurrent promotion), a `SPLIT_INCOMPLETE` page,
//! a probe that sorts right of the latched page's range, an internal-level
//! left hop (see the known limitation below), or allocation pressure even
//! MID-cascade — drops all latches and restarts the whole insert, bounded
//! by `MAX_INSERT_RESTARTS`. In a live tree every restart is caused by a
//! concurrent split that completes shortly after, so a few attempts
//! suffice. A tree that still refuses after the budget carries a
//! *post-crash* incomplete split (a `SPLIT_INCOMPLETE` page whose Commit
//! was lost); finishing those (`BTreeSplitCLR`, undo phase) is M2c work,
//! and the insert fails with [`BTreeError::Unsupported`] — the same
//! severity as Stage M's `hopped` guard, which this replaces: the
//! completed-split case is now a transparent retry instead of an error,
//! while the genuinely incomplete case still hard-fails.
//!
//! ## Known limitations (Stage Q review)
//!
//! - **Stale internal separator gap**: physical deletes can raise an
//!   internal page's real low key above its recorded separator. A probe
//!   that falls into the gap would need an internal-level LEFT hop, which
//!   cannot be trusted on the write path (the walked-to page's parent may
//!   be the stack-top's left sibling, and a Commit would then write the
//!   twin downlink into the wrong parent). The write descent therefore
//!   restarts instead; a persistent gap exhausts the restart budget and
//!   the insert fails `Unsupported` (loud, not corrupt). As a backstop,
//!   `split_commit_guarded` verifies the popped parent actually holds a
//!   downlink to `st.left` before writing, and fails loudly if not.
//! - **Mid-cascade allocation failure**: if `new_page` fails after the
//!   leaf's Prepare/Copy are already WAL-logged (its `SPLIT_INCOMPLETE`
//!   set), the bubble is folded into the restart budget — the pressure may
//!   relieve. If it does not, the leaf's key range stays write-unavailable
//!   (reads still work via the chain) until M2c's CLR finishes the split.
//! - **(key, child) tie disorder**: internal entries order by
//!   `(key, child_page_id)`; freelist reuse can hand a split twin a page
//!   id that flips that tie among duplicate separators, so `find_child`
//!   can pick a page that no longer owns the probe. The write descent
//!   hops right when the stack-top parent provably holds the twin's
//!   downlink (one extra hop instead of a deterministic wedge);
//!   `validate` correspondingly tolerates order swaps among EQUAL
//!   separator keys only. The empty-key edge (an empty Text key separator
//!   ties the slot-0 -infinity marker) resolves one level later, at the
//!   leaf level's full `(key, tid)` ownership check.
//! - **Byte-skewed split wedge (eliminated)**: before round 3 the split
//!   point was the median slot regardless of entry sizes, which could
//!   leave the receiving half without room for the pending entry AFTER
//!   Copy was WAL-logged (a permanent `SPLIT_INCOMPLETE`). The split point
//!   is now byte/pending-aware (`choose_split_slot`), and the downlink
//!   side choices use the same full `(key, trailer)` comparison.
//!
//! # Descent and Blink right hops
//!
//! Internal entries are `(low_key, child_page_id)` in sorted order; the
//! descent picks the last entry with `key <= probe` (entry 0 when the probe
//! is smaller than every key — the leftmost child covers `-infinity`).
//!
//! A split whose Commit record was lost (crash between Copy and Commit)
//! leaves the right sibling without a parent downlink. Reads stay correct
//! via Blink right hops: before descending or searching, if the probe is
//! `>=` the right sibling's first key, the descent moves right (the sibling
//! chain is the authority on which page owns a key range). This makes
//! recovered incomplete splits fully readable without repair.
//!
//! `validate` is a **quiescent-state** check: it must not run concurrently
//! with writers. A split in flight legitimately violates its checks
//! transiently (a right twin reachable via the chain before its downlink
//! lands, `SPLIT_INCOMPLETE` set), which is not corruption.
//!
//! # Split: 3-step WAL (§13.3)
//!
//! The online path emits, in order, and applies each step under the affected
//! pages' write latches (WAL-before-data):
//!
//! 1. [`BTreeIndex::split_prepare`] — allocate the right sibling, emit
//!    `BTreeSplitPrepare`, link `left.next = right`, mark left
//!    `SPLIT_INCOMPLETE`, initialize the right page header.
//! 2. [`BTreeIndex::split_copy`] — emit `BTreeSplitCopy`
//!    (`copy_start_slot` + `left_page_pre_lsn` anchor), move
//!    `[copy_start_slot, slot_count)` to the right page, truncate the left LP
//!    array. The right page is then flushed **before** the left guard is
//!    released, so the left page's post-copy image can never reach disk
//!    without the right page's (redo recomputes the moved entries from the
//!    left page; that contract would break otherwise).
//! 3. [`BTreeIndex::split_commit`] — emit `BTreeSplitCommit`, insert the
//!    downlink `(separator_key, right_page)` into the parent (splitting the
//!    parent recursively first if it has no room; a root split allocates a
//!    new root and appends a meta record), clear `SPLIT_INCOMPLETE` (and
//!    `ROOT`, for root splits) on the left page.
//!
//! The downlink insert is logged **only** by the Commit record — never as a
//! separate `BTreeInsert` — so redo cannot apply it twice.
//!
//! # Delete
//!
//! Physical removal of the exact `(key, tid)` entry (`BTreeDelete` records
//! the slot; redo performs the same deterministic transformation). M2b has
//! no page merge (§13: deferred).

#[cfg(feature = "test-hooks")]
use std::cell::Cell;
use std::cmp::Ordering;
use std::sync::Arc;

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_storage::buffer_pool::{BufferPool, PageGuard, PageGuardMut};
use pg_storage::error::StorageError;
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::page_allocator::PageAllocator;
use pg_storage::sync::Mutex;
use pg_storage::types::{Lsn, Oid, PageId, Tid, PAGE_SIZE};
use pg_storage::wal::record::{BTreeSplitCLRRecord, WalRecord};
use pg_storage::wal::WalWriter;

use crate::error::{BTreeError, Result};
use crate::key::{is_supported_key_type, MAX_INDEX_KEY_BYTES};
use crate::page::{self, BtreePage, BTREE_FLAG_LEAF, BTREE_FLAG_ROOT, BTREE_FLAG_SPLIT_INCOMPLETE};

/// Bound on sibling hops per descent / chain walk; a longer chain means a
/// corrupted `btpo_next` cycle (or a pathological run of incomplete splits),
/// and must hard-fail rather than loop forever.
const MAX_CHAIN_HOPS: usize = 1 << 16;

/// Bound on whole-insert restarts (stale-root refreshes, concurrent-split
/// collisions, split-page allocation failures). In a live tree every restart
/// is caused by a concurrent split that completes shortly after, so a
/// handful of attempts suffices; a tree that still refuses after this many
/// carries a *post-crash* incomplete split, and the insert fails
/// [`BTreeError::Unsupported`] (see the module doc's restart section).
const MAX_INSERT_RESTARTS: usize = 1 << 8;

#[cfg(feature = "test-hooks")]
thread_local! {
    /// Test hook (Stage Q, `test-hooks` feature): while nonzero **in the
    /// current thread**, split-page reservations
    /// ([`BTreeIndex::reserve_split_page`]) fail artificially, one failure
    /// per count, exercising the allocation-failure release-and-restart
    /// path. Thread-local so parallel tests in the same process cannot
    /// consume each other's injected failures (a process-global counter
    /// made the injection test a no-op whenever a concurrent test's split
    /// ate the count). Compiled only under `test-hooks` — production
    /// builds carry no injection surface. Never set outside tests.
    #[doc(hidden)]
    pub static SPLIT_ALLOC_FAILURES: Cell<usize> = const { Cell::new(0) };

    /// Test hook (post-Stage-S review C2, `test-hooks` feature): while
    /// nonzero **in the current thread**, the undo cascade
    /// ([`ensure_downlink_slot`]) fails right after one parent-split CLR has
    /// completed — one failure per count — simulating a crash mid-cascade so
    /// tests can verify the next recovery re-derives and finishes the
    /// remaining work. Thread-local for the same reason as
    /// [`SPLIT_ALLOC_FAILURES`]; recovery runs on the thread that opened the
    /// engine, so arming it from a test thread works. Never set outside
    /// tests.
    #[doc(hidden)]
    pub static UNDO_CASCADE_FAILURES: Cell<usize> = const { Cell::new(0) };

    /// Test hook (post-Stage-T P0 review, `test-hooks` feature): when true
    /// **in the current thread**, the guarded root-split Commit simulates a
    /// checkpoint landing in the (create_new_root, Commit append) window —
    /// it flushes every dirty page (so the brand-new root gets an on-disk
    /// image, `needs_fpi = true`) and publishes the current LSN as the
    /// checkpoint LSN — right before the Commit's pre-touch. Lets tests
    /// force the exact FPI-before-commit interleaving on the online path.
    /// One-shot (auto-clears). Never set outside tests.
    #[doc(hidden)]
    pub static SPLIT_COMMIT_ROOT_CKPT_HOOK: Cell<bool> = const { Cell::new(false) };

    /// Test hook (slot-0 stale-verdict regression, `test-hooks` feature):
    /// while true **in the current thread**, `pin_leaf_for_insert`'s
    /// no-non-empty-left branch parks inside the drop-and-re-latch window
    /// (cur's latch dropped, not yet re-acquired) until
    /// [`SLOT0_WINDOW_RELEASE`] is set, so a paired thread can land an
    /// insert in the exact window the slot-0 re-validation covers.
    /// Thread-local so parallel tests cannot consume each other's arming.
    /// Never set outside tests.
    #[doc(hidden)]
    pub static SLOT0_WINDOW_PARK: Cell<bool> = const { Cell::new(false) };
}

/// Cross-thread signals for [`SLOT0_WINDOW_PARK`] (see there): `ENTERED`
/// counts parkings (the paired thread waits for the first one),
/// `RELEASE` ends the park. Global (not thread-local) because the two
/// parties are different threads; only the arming test touches them.
/// Compiled only under `test-hooks`.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub static SLOT0_WINDOW_ENTERED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub static SLOT0_WINDOW_RELEASE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Park inside the slot-0 re-latch window while [`SLOT0_WINDOW_PARK`] is
/// armed (see there). Bounded so a broken pairing cannot wedge the thread.
/// Compiled only under `test-hooks`.
#[cfg(feature = "test-hooks")]
fn slot0_window_hook() {
    use std::sync::atomic::Ordering as AtomicOrdering;
    if !SLOT0_WINDOW_PARK.with(|c| c.get()) {
        return;
    }
    SLOT0_WINDOW_ENTERED.fetch_add(1, AtomicOrdering::SeqCst);
    let mut spins = 0usize;
    while !SLOT0_WINDOW_RELEASE.load(AtomicOrdering::SeqCst) {
        std::thread::yield_now();
        spins += 1;
        if spins > (1usize << 28) {
            break;
        }
    }
}

/// Consume one injected undo-cascade failure (see [`UNDO_CASCADE_FAILURES`]);
/// returns an error when the hook fired. Compiled only under `test-hooks`.
#[cfg(feature = "test-hooks")]
fn undo_cascade_failure_hook() -> Result<()> {
    let injected = UNDO_CASCADE_FAILURES.with(|failures| {
        let remaining = failures.get();
        if remaining > 0 {
            failures.set(remaining - 1);
            true
        } else {
            false
        }
    });
    if injected {
        return Err(BTreeError::Storage(StorageError::InvalidOperation(
            "injected undo cascade failure (test hook)".to_string(),
        )));
    }
    Ok(())
}

/// Outcome of one pessimistic insert pass (see [`BTreeIndex::insert_pessimistic`]).
enum Pessimistic {
    /// The entry was inserted (with or without a split).
    Done,
    /// The tree moved under the pass; restart the whole insert.
    Retry,
}

/// State carried between the three split steps (§13.3).
///
/// The steps are separate entry points so crash tests can drive a split
/// one step at a time and abandon the engine mid-protocol; the online
/// insert path runs all three back to back.
#[derive(Debug, Clone)]
pub struct SplitState {
    /// The overflowing original page.
    pub left: PageId,
    /// The freshly allocated right sibling.
    pub right: PageId,
    /// `btpo_level` of both pages (0 = leaf).
    pub level: u8,
    /// Slots `[copy_start_slot, slot_count)` of the left page move right.
    pub copy_start_slot: u16,
    /// LSN of the emitted `BTreeSplitPrepare` record; also the left page's
    /// `pd_lsn` after Prepare — the Copy step's idempotency anchor.
    pub prepare_lsn: Lsn,
}

/// A handle on one B+Tree index rooted at a meta page.
///
/// Cheap to construct: [`BTreeIndex::open`] reads the current
/// `(root_page_id, tree_level)` from the meta page, so after a restart (or
/// for the transient handles the `AccessMethod` glue builds per call) the
/// handle picks up the on-disk root.
pub struct BTreeIndex {
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
    rel_oid: Oid,
    meta_page: PageId,
    root_page: PageId,
    tree_level: u8,
    key_type: ColumnType,
}

impl BTreeIndex {
    /// Assemble a handle from already-materialized on-disk state (bulk
    /// load). Does no I/O: the caller owns the meta/root pages and writes
    /// the meta record separately.
    pub(crate) fn from_parts(
        buffer_pool: Arc<BufferPool>,
        wal_writer: Arc<WalWriter>,
        rel_oid: Oid,
        meta_page: PageId,
        root_page: PageId,
        tree_level: u8,
        key_type: ColumnType,
    ) -> Self {
        Self {
            buffer_pool,
            wal_writer,
            rel_oid,
            meta_page,
            root_page,
            tree_level,
            key_type,
        }
    }

    /// Create a brand-new index: allocate the meta page and a root leaf,
    /// make both durable with post-image `FullPageImage` records (the same
    /// pattern the heap uses for page initialization — a freelist-reused
    /// page must recover as freshly initialized, not as its previous
    /// tenant's bytes), then log the first meta record `(root, 0)` as a
    /// `BTreeInsert`.
    pub fn create(
        buffer_pool: Arc<BufferPool>,
        wal_writer: Arc<WalWriter>,
        rel_oid: Oid,
        key_type: ColumnType,
    ) -> Result<Self> {
        if !is_supported_key_type(key_type) {
            return Err(BTreeError::InvalidArgument(format!(
                "unsupported index key type: {key_type:?}"
            )));
        }

        let meta_page = {
            let mut guard = buffer_pool.new_page()?;
            let page_id = guard.page_id();
            let page = as_page_mut(&mut guard);
            // The meta page is not a tree page: level 0, no LEAF/ROOT flags.
            BtreePage::init(page, 0, 0);
            log_page_init(&wal_writer, page_id, page)?;
            page_id
        };

        let root_page = {
            let mut guard = buffer_pool.new_page()?;
            let page_id = guard.page_id();
            let page = as_page_mut(&mut guard);
            BtreePage::init(page, 0, BTREE_FLAG_LEAF | BTREE_FLAG_ROOT);
            log_page_init(&wal_writer, page_id, page)?;
            page_id
        };

        let mut index = Self {
            buffer_pool,
            wal_writer,
            rel_oid,
            meta_page,
            root_page,
            tree_level: 0,
            key_type,
        };
        index.write_meta_record()?;
        Ok(index)
    }

    /// Open an existing index from its meta page, recovering the current
    /// root and tree level from the last meta record.
    pub fn open(
        buffer_pool: Arc<BufferPool>,
        wal_writer: Arc<WalWriter>,
        rel_oid: Oid,
        meta_page: PageId,
        key_type: ColumnType,
    ) -> Result<Self> {
        let (root_page, tree_level) = root_from_meta(&buffer_pool, meta_page)?;
        Ok(Self {
            buffer_pool,
            wal_writer,
            rel_oid,
            meta_page,
            root_page,
            tree_level,
            key_type,
        })
    }

    /// Re-read the meta page and refresh the cached `root_page` /
    /// `tree_level`, returning the current root. Used by `split_commit`'s
    /// generational check (another handle may have promoted the root since
    /// this handle cached it).
    fn refresh_root_from_meta(&mut self) -> Result<PageId> {
        let (root_page, tree_level) = root_from_meta(&self.buffer_pool, self.meta_page)?;
        self.root_page = root_page;
        self.tree_level = tree_level;
        Ok(root_page)
    }

    /// The meta page of this index (also the relation's `first_page`).
    pub fn meta_page(&self) -> PageId {
        self.meta_page
    }

    /// The current root page, as of handle construction or the last root
    /// split performed through this handle.
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    /// The current tree level (0 = root is a leaf).
    pub fn tree_level(&self) -> u8 {
        self.tree_level
    }

    /// The indexed column type.
    pub fn key_type(&self) -> ColumnType {
        self.key_type
    }

    /// The relation OID this index belongs to.
    pub fn rel_oid(&self) -> Oid {
        self.rel_oid
    }

    /// Free space on a page (test support: crash tests fill a page until its
    /// next insert would split, then drive the split steps manually).
    pub fn page_free_space(&self, page_id: PageId) -> Result<usize> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        BtreePage::level(page)?; // geometry check
        Ok(SlottedPage::free_space(page))
    }

    /// Append a meta record `(root_page, tree_level)` as the new
    /// authoritative root pointer, WAL-logged as `BTreeInsert` (meta tuple =
    /// 10-byte record, per the `BTreeInsertRecord` payload contract).
    pub(crate) fn write_meta_record(&mut self) -> Result<()> {
        let mut guard = self.buffer_pool.pin_mut(self.meta_page)?;
        let page = as_page_mut(&mut guard);
        let slot = SlottedPage::slot_count(page) as u16;
        let record_bytes = page::encode_meta_record(self.root_page, self.tree_level as u16);
        // level/flags 0/0: a fresh meta page initializes with no tree flags.
        let rec = WalRecord::btree_insert(self.meta_page, slot, 0, 0, record_bytes.clone())?;
        let lsn = self.wal_writer.append(rec)?;
        BtreePage::insert_entry_at(page, slot, &record_bytes)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------

    /// Point lookup: return the heap TID of the first entry with `key`.
    pub fn lookup(&self, key: &[u8]) -> Result<Option<Tid>> {
        let probe_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        let (mut guard, _, _) = self.descend_to_leaf_guard(key, &probe_tid, false)?;
        let mut slot = leaf_lower_bound(as_page(&guard), key, &probe_tid)? as u16;
        // The entry that lower_bound points at is the global first entry
        // `>= (key, -infinity)`; when the page is exhausted it is the first
        // entry of the next non-empty sibling.
        let mut hops = 0usize;
        loop {
            let next = {
                let page = as_page(&guard);
                let count = SlottedPage::slot_count(page) as u16;
                if slot < count {
                    let (k, tid) = page::decode_leaf_entry(entry_bytes(page, slot)?)?;
                    return Ok(if k == key { Some(tid) } else { None });
                }
                BtreePage::next(page)?
            };
            if next == PageId::INVALID {
                return Ok(None);
            }
            // Coupled right hop: the sibling's read latch is taken before
            // the current page's is released (module doc).
            let next_guard = self.buffer_pool.pin(next)?;
            drop(guard);
            guard = next_guard;
            slot = 0;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    /// Point lookup with duplicates: return the heap TIDs of **every**
    /// entry with `key`, in `(key, tid)` order.
    ///
    /// M2b indexes are non-unique, so a key can map to several heap versions
    /// (e.g. after an UPDATE leaves a dead version behind, or two rows share
    /// a key). Callers that judge heap visibility per TID (the engine's
    /// `index_lookup`) must see all of them, not just the first.
    pub fn lookup_all(&self, key: &[u8]) -> Result<Vec<Tid>> {
        let probe_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        let (mut guard, _, _) = self.descend_to_leaf_guard(key, &probe_tid, false)?;
        let mut slot = leaf_lower_bound(as_page(&guard), key, &probe_tid)? as u16;
        // Same walk as `lookup`, but collecting while the entry key matches
        // and stopping at the first greater key (duplicates are adjacent in
        // (key, tid) order, possibly spanning leaf siblings).
        let mut out = Vec::new();
        let mut hops = 0usize;
        loop {
            let next = {
                let page = as_page(&guard);
                let count = SlottedPage::slot_count(page) as u16;
                while slot < count {
                    let (k, tid) = page::decode_leaf_entry(entry_bytes(page, slot)?)?;
                    if k != key {
                        return Ok(out);
                    }
                    out.push(tid);
                    slot += 1;
                }
                BtreePage::next(page)?
            };
            if next == PageId::INVALID {
                return Ok(out);
            }
            // Coupled right hop (module doc).
            let next_guard = self.buffer_pool.pin(next)?;
            drop(guard);
            guard = next_guard;
            slot = 0;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    /// Range scan over the leaf chain: every entry with
    /// `start <= key < end` (an open side is unbounded), in key order.
    ///
    /// Walks `btpo_next`, so entries on right siblings whose downlink was
    /// lost to a crash are still reached (§13.2 Blink semantics).
    pub fn range_scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Tid)>> {
        let mut out = Vec::new();
        let probe_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        let (mut guard, _, _) =
            self.descend_to_leaf_guard(start.unwrap_or(&[]), &probe_tid, false)?;
        let mut slot = match start {
            Some(s) => leaf_lower_bound(as_page(&guard), s, &probe_tid)? as u16,
            None => 0,
        };
        let mut hops = 0usize;
        loop {
            let next = {
                let page = as_page(&guard);
                let count = SlottedPage::slot_count(page) as u16;
                while slot < count {
                    let (k, tid) = page::decode_leaf_entry(entry_bytes(page, slot)?)?;
                    if let Some(e) = end {
                        if k >= e {
                            return Ok(out);
                        }
                    }
                    out.push((k.to_vec(), tid));
                    slot += 1;
                }
                BtreePage::next(page)?
            };
            if next == PageId::INVALID {
                return Ok(out);
            }
            // Coupled right hop (module doc).
            let next_guard = self.buffer_pool.pin(next)?;
            drop(guard);
            guard = next_guard;
            slot = 0;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    /// Descend from the root to the leaf that owns the probe `(key, tid)`,
    /// returning `(leaf, path, hopped)`: `path` holds the internal pages from
    /// root to the leaf's parent (split Commit pops it), and `hopped` records
    /// whether any right hop was taken. A right hop means the descent
    /// undershot because a downlink is missing — under concurrency that is a
    /// split whose Commit has not landed yet (transient; the write path
    /// restarts and re-descends), post-crash it is a lost Commit (the
    /// bounded-restart guard in `insert` hard-fails, see the module doc).
    ///
    /// Lookups/range scans probe with `tid = Tid::INVALID` (the minimum);
    /// inserts/deletes probe with the entry's real TID.
    ///
    /// This is the PageId view of `BTreeIndex::descend_to_leaf_guard` for
    /// callers (delete, validate) that re-latch the leaf themselves.
    pub fn descend_to_leaf(&self, key: &[u8], tid: &Tid) -> Result<(PageId, Vec<PageId>, bool)> {
        let (guard, path, hopped) = self.descend_to_leaf_guard(key, tid, false)?;
        Ok((guard.page_id(), path, hopped))
    }

    /// The INSERT positioning descent (Stage T deadlock-stress fixes): like
    /// [`BTreeIndex::descend_to_leaf`], but left movement is decided by the
    /// FULL `(key, tid)` chain position, not by the current leaf's first
    /// entry alone. A new entry must land on the page whose chain position
    /// contains the probe: when the candidate leaf is empty (a
    /// delete-drained split twin — M2b has no page merge) or its first entry
    /// sorts above the probe, the nearest non-empty LEFT sibling's LAST
    /// entry decides — only if THAT sorts above the probe does the entry
    /// belong further left. This is the ground truth the separator keys
    /// cannot provide (separators carry no tid and can be stale).
    ///
    /// Two failure modes this prevents:
    ///
    /// - **Boundary-key migration** (the original deadlock-stress
    ///   `EntryNotFound`): the unrestricted `(key, tid)` left hop placed a
    ///   new entry LEFT of the separator that owns its key whenever an
    ///   update's delete-then-insert raised the right leaf's first entry
    ///   above the probe — the boundary key's entry migrated left on every
    ///   update, the owning leaf drained to EMPTY, and later probes
    ///   black-holed on the empty page (never dominated, nothing rightward),
    ///   so lookups missed the entry and deletes returned a spurious
    ///   `EntryNotFound`. With the left-sibling-last rule, that probe's left
    ///   neighbor's last entry sorts strictly below it, so the walk stays.
    /// - **Small-tid misplacement** (final review): with the run's right
    ///   side deleted (or a drained twin in between), inserting `(K, t_new)`
    ///   with `t_new` smaller than an existing `(K, t_old)` on the left leaf
    ///   must reach the left leaf — otherwise chain order breaks
    ///   (`L.last > R.first`) and an exact re-insert is silently duplicated
    ///   instead of failing with `DuplicateKey`.
    ///
    /// Locating an EXISTING entry (lookup/delete) keeps the unrestricted
    /// left walk — born-left split-boundary duplicates can sit on either
    /// side of a separator — plus one extension: an EMPTY leaf with a left
    /// sibling hops left unconditionally (it owns nothing, and the
    /// right-hop check can never bounce back from an empty page, so this
    /// cannot cycle).
    fn descend_to_leaf_for_insert(
        &self,
        key: &[u8],
        tid: &Tid,
    ) -> Result<(PageId, Vec<PageId>, bool)> {
        let (guard, path, hopped) = self.descend_to_leaf_guard(key, tid, true)?;
        Ok((guard.page_id(), path, hopped))
    }

    /// [`BTreeIndex::descend_to_leaf`] starting from an explicit root
    /// (see [`BTreeIndex::descend_to_leaf_guard_from`]).
    fn descend_to_leaf_from(
        &self,
        root: PageId,
        key: &[u8],
        tid: &Tid,
    ) -> Result<(PageId, Vec<PageId>, bool)> {
        let (guard, path, hopped) = self.descend_to_leaf_guard_from(root, key, tid, false)?;
        Ok((guard.page_id(), path, hopped))
    }

    /// Read-coupled descent (§13.2 latch crabbing): at every level the
    /// child page's read latch is acquired while the parent's is still
    /// held, then the parent is released. Returns the read guard on the
    /// owning leaf, the internal path, and the right-hop flag. The child
    /// page is picked from the walked-to page (the page that provably owns
    /// the probe at this level), so every page pushed to `path` is the true
    /// parent of the next one.
    fn descend_to_leaf_guard<'a>(
        &'a self,
        key: &[u8],
        tid: &Tid,
        position_for_insert: bool,
    ) -> Result<(PageGuard<'a>, Vec<PageId>, bool)> {
        self.descend_to_leaf_guard_from(self.root_page, key, tid, position_for_insert)
    }

    /// `BTreeIndex::descend_to_leaf_guard` starting from an explicit root
    /// (used by `validate`, which re-reads the authoritative root from the
    /// meta page instead of trusting the handle's cache — review M2).
    ///
    /// `position_for_insert` selects POSITION semantics (`true`: where a NEW
    /// entry must go — left hops stay inside the probe key's duplicate run;
    /// see [`BTreeIndex::descend_to_leaf_for_insert`]) versus LOCATE
    /// semantics (`false`: find where an existing entry may sit —
    /// split-boundary duplicates can live left of their separator, so the
    /// left walk is unrestricted).
    fn descend_to_leaf_guard_from<'a>(
        &'a self,
        root: PageId,
        key: &[u8],
        tid: &Tid,
        position_for_insert: bool,
    ) -> Result<(PageGuard<'a>, Vec<PageId>, bool)> {
        let mut path = Vec::new();
        let mut hopped = false;
        let mut guard = self.buffer_pool.pin(root)?;
        loop {
            let level = BtreePage::level(as_page(&guard))?;
            // Position `cur` on the chain: the internal descent navigates by
            // key only (separator keys can be stale — a leftmost child's low
            // key decreases without updating the parent — and duplicate keys
            // can span siblings), so the exact page is found by walking the
            // sibling chain both ways. Right hops are the Blink mechanism
            // (§13.2); left hops cover stale separators.
            guard = self.walk_to_position_guard(
                guard,
                key,
                tid,
                level,
                &mut hopped,
                position_for_insert,
            )?;
            if level == 0 {
                return Ok((guard, path, hopped));
            }
            let child = find_child(as_page(&guard), key)?;
            // Crabbing: pin the child while the parent's read latch is held.
            let child_guard = self.buffer_pool.pin(child)?;
            path.push(guard.page_id());
            drop(guard);
            guard = child_guard;
        }
    }

    /// Walk the sibling chain at one tree level until the guarded page owns
    /// the probe: `first_entry(cur) <= probe < first_entry(next)`. Takes
    /// ownership of the read guard on the entry page and returns the read
    /// guard on the final page.
    ///
    /// Leaf level compares the full `(key, tid)` order (duplicates are
    /// disambiguated by TID); internal levels compare keys strictly — equal
    /// keys neither dominate nor yield, so an internal page whose subtree
    /// may contain the probe by key is never hopped over (the leaf-level
    /// walk resolves the exact position; the sibling chains are contiguous
    /// across parents).
    ///
    /// Right hops are latch-coupled (the right sibling's read latch is taken
    /// before the current page's is released) and set `hopped`. Left hops
    /// cannot be coupled (module doc: never move left while holding a
    /// latch), so they drop first; that is safe because a stale separator
    /// can only send the walk left of the target, and the right walk is the
    /// ground-truth mechanism that re-establishes the position. An empty
    /// sibling (Prepare without Copy) owns no keys and is never hopped onto.
    ///
    /// `position_for_insert = true` (insert placement) decides left movement
    /// by the FULL `(key, tid)` chain position: when the current leaf is
    /// empty or its first entry sorts above the probe, the nearest non-empty
    /// left sibling's LAST entry decides — only if it sorts above the probe
    /// does the new entry belong further left (see
    /// [`BTreeIndex::descend_to_leaf_for_insert`]). `false` (locate an
    /// existing entry) walks left whenever the first entry sorts above the
    /// probe, and hops left from an EMPTY leaf unconditionally (a
    /// delete-drained twin owns nothing; the right check cannot bounce back
    /// from it, so this cannot cycle).
    fn walk_to_position_guard<'a>(
        &'a self,
        mut guard: PageGuard<'a>,
        key: &[u8],
        tid: &Tid,
        level: u8,
        hopped: &mut bool,
        position_for_insert: bool,
    ) -> Result<PageGuard<'a>> {
        let mut hops = 0usize;
        // Insert placement only: the page id whose left sibling was already
        // probed this walk and found NOT to sort above the probe. The probe
        // drops cur's latch to read the left sibling (never move left while
        // holding one), so without this cache a stable candidate page would
        // be re-probed forever.
        let mut placement_verified = PageId::INVALID;
        loop {
            let mut moved = false;

            // Left: if cur's first entry is greater than the probe, the
            // probe sorts before cur (stale separator or duplicate run).
            // Internal pages compare keys strictly. Leaf handling differs by
            // mode — see below.
            if level == 0 {
                let (count, first_above, prev_id) = {
                    let page = as_page(&guard);
                    let count = SlottedPage::slot_count(page);
                    let first_above = if count == 0 {
                        false
                    } else {
                        let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                        (fk, ft) > (key, *tid)
                    };
                    (count, first_above, BtreePage::prev(page)?)
                };
                if position_for_insert {
                    // POSITION (insert placement): cur is a left CANDIDATE
                    // when it is empty or its first entry sorts above the
                    // probe — but the decider is the TRUE nearest non-empty
                    // LEFT sibling's LAST entry in full `(key, tid)` order
                    // (the chain ground truth; separator keys never carry
                    // tids, and the prev link can skip freshly split twins).
                    // Only if that entry sorts above the probe does the new
                    // entry belong further left. This keeps an update churn's
                    // delete-then-insert from migrating a boundary key left
                    // of its separator (its left neighbor's last key is
                    // strictly smaller), while a genuinely smaller tid
                    // (freelist reuse) or an exact re-insert still reaches
                    // the run's left segment — preserving chain order and
                    // the `DuplicateKey` check there.
                    if (count == 0 || first_above)
                        && prev_id != PageId::INVALID
                        && placement_verified != guard.page_id()
                    {
                        let cur_id = guard.page_id();
                        // Never move left while holding a latch: drop cur,
                        // then find the true left neighbor by walking RIGHT
                        // from the (possibly stale) prev hint.
                        drop(guard);
                        match self.nearest_nonempty_left(cur_id)? {
                            Some(walk) => {
                                let go_left = {
                                    let pguard = self.buffer_pool.pin(walk)?;
                                    let page = as_page(&pguard);
                                    let pcount = SlottedPage::slot_count(page);
                                    let (lk, lt) = page::decode_leaf_entry(entry_bytes(
                                        page,
                                        pcount as u16 - 1,
                                    )?)?;
                                    (lk, lt) > (key, *tid)
                                };
                                hops += 1;
                                if go_left {
                                    // The position is at/left of that
                                    // sibling: continue the walk FROM it;
                                    // the loop top re-runs its own checks.
                                    guard = self.buffer_pool.pin(walk)?;
                                } else {
                                    // cur holds the probe's chain position.
                                    // Re-pin cur and re-validate it from the
                                    // loop top (it may have changed while
                                    // unlatched); the cached verdict keeps a
                                    // stable page from being re-probed.
                                    placement_verified = cur_id;
                                    guard = self.buffer_pool.pin(cur_id)?;
                                }
                                moved = true;
                                hops += 1;
                            }
                            // Everything left of cur is empty: no entry
                            // there sorts above the probe.
                            None => {
                                placement_verified = cur_id;
                                guard = self.buffer_pool.pin(cur_id)?;
                                moved = true;
                                hops += 1;
                            }
                        }
                    }
                } else {
                    // LOCATE (lookup/delete): an EXISTING entry may sit left
                    // of its separator (born-left split-boundary duplicate),
                    // so the walk moves left whenever cur's first entry
                    // sorts above the probe — and, for an EMPTY cur (a
                    // delete-drained split twin; M2b has no page merge),
                    // whenever a left sibling exists at all: the empty page
                    // owns nothing, and the right check below can never
                    // bounce back from it (first_entry_key = None), so this
                    // cannot cycle. An empty chain head (prev = INVALID)
                    // simply owns the probe: nothing further left exists.
                    let dominated = count == 0 || first_above;
                    if dominated && prev_id != PageId::INVALID {
                        // Drop-then-acquire: never move left while holding a
                        // latch (module doc).
                        drop(guard);
                        guard = self.buffer_pool.pin(prev_id)?;
                        moved = true;
                        hops += 1;
                    }
                }
            } else {
                // Internal level: keys compared strictly; equal keys neither
                // dominate nor yield, so an internal page whose subtree may
                // contain the probe by key is never hopped over.
                let prev = {
                    let page = as_page(&guard);
                    let dominated = if SlottedPage::slot_count(page) == 0 {
                        false
                    } else {
                        let (fk, _) = page::decode_internal_entry(entry_bytes(page, 0)?)?;
                        fk > key
                    };
                    if dominated {
                        Some(BtreePage::prev(page)?)
                    } else {
                        None
                    }
                };
                if let Some(prev) = prev {
                    if prev != PageId::INVALID {
                        // Drop-then-acquire: never move left while holding a
                        // latch (module doc).
                        drop(guard);
                        guard = self.buffer_pool.pin(prev)?;
                        moved = true;
                        hops += 1;
                    }
                }
            }

            // Right: if the next NON-EMPTY sibling's first entry sorts at
            // or below the probe, the probe belongs to it or further right.
            // Empty siblings are skipped THROUGH, never landed on: a page is
            // empty either because deletes drained it (M2b has no page merge
            // — it owns nothing) or because its split Prepare ran but the
            // Copy is in flight — and that Copy's entries all sort below the
            // next non-empty page's first entry (they are the left page's
            // upper half, and the chain was ordered), so a probe sorting
            // right of that first entry belongs right of the twin too. An
            // empty page's `next` is stable (empty pages never split: the
            // split point needs >= 2 entries), so the skip walk cannot tear.
            let mut next = BtreePage::next(as_page(&guard))?;
            let mut hop_guard = None;
            while next != PageId::INVALID {
                // Coupled: the sibling's read latch is taken while cur's is
                // still held, so the sibling link cannot tear under us.
                let next_guard = self.buffer_pool.pin(next)?;
                let page = as_page(&next_guard);
                if SlottedPage::slot_count(page) == 0 {
                    let n2 = BtreePage::next(page)?;
                    drop(next_guard);
                    next = n2;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                    continue;
                }
                let owns = if level == 0 {
                    let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                    (fk, ft) <= (key, *tid)
                } else {
                    // An empty first key is the -infinity marker of a
                    // parent's leftmost child: it owns the smallest keys of
                    // its parent's range, so a real probe never sorts to its
                    // right. Only a real first key (a not-yet-linked split
                    // twin's separator) can force a right hop.
                    let fk = first_entry_key(page)?.expect("non-empty page has a first key");
                    !fk.is_empty() && fk.as_slice() <= key
                };
                if owns {
                    hop_guard = Some(next_guard);
                }
                break;
            }
            if let Some(next_guard) = hop_guard {
                drop(guard);
                guard = next_guard;
                *hopped = true;
                moved = true;
                hops += 1;
            }

            if !moved {
                return Ok(guard);
            }
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    // ------------------------------------------------------------------
    // Insert
    // ------------------------------------------------------------------

    /// Insert `(key, tid)`. Duplicate keys are allowed; a duplicate
    /// `(key, tid)` pair is [`BTreeError::DuplicateKey`].
    ///
    /// Optimistic first (§13.2): read-coupled descent, exclusive latch on
    /// the re-validated owning leaf; if the entry fits it is WAL-logged and
    /// applied under that single latch. When the leaf has no room, the
    /// insert escalates to a pessimistic pass (full-path write latches,
    /// split with a pre-reserved right page). Structural surprises restart
    /// the whole insert, bounded by `MAX_INSERT_RESTARTS`.
    pub fn insert(&mut self, key: &[u8], tid: Tid) -> Result<()> {
        self.insert_with_budget(key, tid, MAX_INSERT_RESTARTS)
    }

    /// [`BTreeIndex::insert`] with a caller-chosen restart budget (Stage Q
    /// review M3). Reserved for the engine's abort-time index-undo
    /// re-insert: that is an offline path where a much larger budget is
    /// acceptable, because a spurious budget exhaustion would permanently
    /// drop a LIVE row's index entry.
    pub fn insert_with_budget(&mut self, key: &[u8], tid: Tid, max_restarts: usize) -> Result<()> {
        if key.len() > MAX_INDEX_KEY_BYTES {
            return Err(BTreeError::KeyTooLarge(key.len()));
        }
        let entry = page::encode_leaf_entry(key, tid);
        let mut restarts = 0usize;
        loop {
            if restarts > 0 {
                // A previous attempt hit a structural surprise. The handle's
                // cached root may also be stale (another thread promoted
                // it), which would make every descent start from a demoted
                // page — re-read the authoritative root first.
                self.refresh_root_from_meta()?;
            }
            // ---- optimistic attempt ----
            // POSITION semantics (left hops only within the probe key's own
            // duplicate run): the new entry must land on the leaf that owns
            // its key range — see `descend_to_leaf_for_insert`.
            // `pin_leaf_for_insert` additionally makes a slot-0 (left-edge)
            // landing atomic with the decisive neighbor (see its doc):
            // `side_guard` must stay held through `insert_into_page`.
            let (leaf, _path, _hopped) = self.descend_to_leaf_for_insert(key, &tid)?;
            let (mut guard, pos, side_guard) = self.pin_leaf_for_insert(leaf, key, &tid)?;
            let fits = {
                let page = as_page_mut(&mut guard);
                let needed = entry.len() + 4; // entry + one line pointer
                SlottedPage::free_space(page) >= needed
            };
            if fits {
                let result = self.insert_into_page(&mut guard, pos, entry);
                drop(side_guard);
                return result;
            }
            drop(side_guard);
            drop(guard);

            // ---- pessimistic pass ----
            // A full leaf escalates here regardless of whether the descent
            // right-hopped: the write descent distinguishes a COMPLETED
            // split (the twin has a parent downlink — it hops onto it and
            // proceeds, review M1) from an in-flight or lost Commit (no
            // downlink — Retry; the restart budget separates a transient
            // in-flight Commit from a post-crash lost one, which then fails
            // Unsupported at the Stage M `hopped` guard's severity).
            match self.insert_pessimistic(key, &tid, entry.clone()) {
                Ok(Pessimistic::Done) => return Ok(()),
                Ok(Pessimistic::Retry) => restart_or_fail(&mut restarts, max_restarts)?,
                // Allocation pressure escaping MID-cascade (the leaf's
                // Prepare/Copy are already WAL-logged, its SPLIT_INCOMPLETE
                // set): the next pass would refuse the incomplete page
                // anyway, so fold the bubble into the same bounded-restart
                // budget — the pressure may relieve. If the budget runs
                // out, the subtree stays write-unavailable until M2c's CLR
                // finishes the split (module doc).
                Err(BTreeError::Storage(StorageError::BufferPoolFull)) => {
                    restart_or_fail(&mut restarts, max_restarts)?
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Latch `leaf` for write and re-validate that it still owns the probe
    /// `(key, tid)`, hopping along the sibling chain when a concurrent split
    /// moved the ownership window right (or concurrent physical deletes
    /// raised the first key past the probe). Right hops are coupled under
    /// write latches (the same left→right order split Prepare/Copy uses);
    /// left hops drop before acquiring (never move left while holding a
    /// latch). Returns the guard on the owning leaf and whether any RIGHT
    /// hop was taken — informational only: a hop does NOT forbid splitting
    /// the leaf. A full leaf escalates to the pessimistic pass regardless,
    /// and the write descent there decides via the parent-downlink check
    /// whether a twin may be split (see [`BTreeIndex::insert`]).
    ///
    /// This path is DELETE-only (LOCATE semantics: find an existing entry —
    /// split-boundary duplicates can sit left of their separator, so the
    /// left walk is unrestricted, and an EMPTY leaf — a delete-drained split
    /// twin; M2b has no page merge — hops left whenever it has a left
    /// sibling: it owns nothing and the right check can never bounce back
    /// from it). INSERT placement uses [`BTreeIndex::pin_leaf_for_insert`],
    /// which additionally makes a slot-0 landing atomic with the decisive
    /// neighbor.
    ///
    /// # SPLIT_INCOMPLETE / FPI discipline (Stage T P0 residual)
    ///
    /// In-window writes on a page whose split Commit is in flight are a
    /// DESIGNED Stage S feature (the undo CLR's NoMove truncation accounts
    /// for them), so this path never refuses `SPLIT_INCOMPLETE` pages. The
    /// one thing it must never do is emit the page's cycle FPI while the
    /// Commit is in flight (record appended, flag clear not yet applied):
    /// that FPI would land after the Commit record with a pre-commit
    /// image, and recovery's unconditional FPI replay would roll the page
    /// back past the Commit (resurrected `SPLIT_INCOMPLETE` → spurious undo
    /// CLR → duplicate downlink). So acquisitions go through
    /// [`BufferPool::pin_mut_without_fpi`], and [`BufferPool::ensure_fpi`]
    /// is called ONLY for pages without a Commit in flight: the flag is
    /// stable under the held write latch (Prepare sets it and the Commit's
    /// apply clears it, both needing this latch), so checking it once per
    /// acquisition is race-free — and skipping the FPI for a flagged page
    /// is redo-safe by the same argument as `pin_mut_without_fpi` (the
    /// in-window modification's record LSN exceeds any intervening
    /// checkpoint's begin, so replay re-applies it; the residual exposure
    /// is torn-write-only). Skipping the gate evaluation entirely for
    /// flagged pages also removes the check-then-emit TOCTOU a two-step
    /// "is an FPI due?" probe would have against a concurrent checkpoint
    /// publication.
    fn pin_leaf_for_write(
        &self,
        leaf: PageId,
        key: &[u8],
        tid: &Tid,
    ) -> Result<(PageGuardMut<'_>, bool)> {
        let mut guard = self.buffer_pool.pin_mut_without_fpi(leaf)?;
        let mut hopped = false;
        let mut hops = 0usize;
        loop {
            // See the fn doc: emit the cycle FPI only when no split Commit
            // is in flight for this page.
            let split_incomplete =
                BtreePage::flags(as_page_mut(&mut guard))? & BTREE_FLAG_SPLIT_INCOMPLETE != 0;
            if !split_incomplete {
                self.buffer_pool.ensure_fpi(&mut guard)?;
            }
            // Left edge: the probe LOCATEs an existing entry, which a
            // split-boundary duplicate run can place left of its
            // separator — so the walk moves left whenever cur's first entry
            // sorts above the probe, and hops left from an EMPTY leaf
            // unconditionally (a delete-drained split twin owns nothing —
            // M2b has no page merge — and the right edge below can never
            // bounce back from an empty page, so this cannot cycle). An
            // empty chain head (prev = INVALID) owns the probe.
            let prev_id = {
                let page = as_page_mut(&mut guard);
                let count = SlottedPage::slot_count(page);
                let first_above = if count == 0 {
                    false
                } else {
                    let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                    (fk, ft) > (key, *tid)
                };
                let prev_id = BtreePage::prev(page)?;
                if count == 0 || first_above {
                    Some(prev_id)
                } else {
                    None
                }
            };
            if let Some(prev_id) = prev_id {
                if prev_id != PageId::INVALID {
                    // Drop-then-acquire: never move left while holding a
                    // latch (module doc).
                    drop(guard);
                    guard = self.buffer_pool.pin_mut_without_fpi(prev_id)?;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                    continue;
                }
            }

            // Right edge: the next NON-EMPTY sibling's first entry sorts at
            // or below the probe, so the probe belongs to it or further
            // right. Empty siblings are skipped THROUGH, never landed on
            // (see `walk_to_position_guard`): an empty page's `next` is
            // stable (empty pages never split), and under our write latch a
            // split cannot re-point cur's own `next`, so the skip walk
            // cannot tear.
            let mut next = BtreePage::next(as_page_mut(&mut guard))?;
            let mut hop_target = None;
            while next != PageId::INVALID {
                // Coupled: each step's read latch is taken while the
                // current page's (write) latch is held.
                let rguard = self.buffer_pool.pin(next)?;
                let rpage = as_page(&rguard);
                if SlottedPage::slot_count(rpage) == 0 {
                    let n2 = BtreePage::next(rpage)?;
                    drop(rguard);
                    next = n2;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                    continue;
                }
                let (fk, ft) = page::decode_leaf_entry(entry_bytes(rpage, 0)?)?;
                if (fk, ft) <= (key, *tid) {
                    hop_target = Some(next);
                }
                break;
            }
            match hop_target {
                None => return Ok((guard, hopped)),
                Some(target) => {
                    // Read-pin → write-pin transition without re-checking
                    // here: the loop top re-validates everything against the
                    // page actually latched.
                    drop(guard);
                    guard = self.buffer_pool.pin_mut_without_fpi(target)?;
                    hopped = true;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                }
            }
        }
    }

    /// The nearest NON-EMPTY leaf left of `page`, or `None` (Stage T final
    /// review).
    ///
    /// `page`'s `prev` link is only a HINT: split Prepare re-points
    /// `left.next` and writes the twin's `prev`, but never updates the OLD
    /// right sibling's `prev`, so the link can skip freshly split twins.
    /// The true left neighborhood is found by walking RIGHT from `prev`
    /// (the ground-truth direction) back to `page`, tracking the last
    /// non-empty page. `prev` always names a page that was `page`'s left
    /// neighbor when `page` was created, and splits only splice twins
    /// immediately right of the split page, so `prev` is still at-or-left
    /// of `page` and the right walk reaches `page`.
    ///
    /// Read-only and coupled rightward (each sibling's read latch is taken
    /// before the current one is released): a concurrent split can only
    /// splice a twin between two chain-adjacent pages, and splicing needs
    /// the split page's write latch — blocked by our read latch — so the
    /// walked segment cannot change under us.
    fn nearest_nonempty_left(&self, page: PageId) -> Result<Option<PageId>> {
        Self::nearest_nonempty_left_on(&self.buffer_pool, page)
    }

    /// [`BTreeIndex::nearest_nonempty_left`] on an explicit pool (the
    /// pessimistic write descent holds its path guards through a local
    /// `Arc` clone rather than `self`).
    fn nearest_nonempty_left_on(pool: &BufferPool, page: PageId) -> Result<Option<PageId>> {
        let start = {
            let guard = pool.pin(page)?;
            BtreePage::prev(as_page(&guard))?
        };
        if start == PageId::INVALID {
            return Ok(None);
        }
        let mut found = None;
        let mut guard = pool.pin(start)?;
        let mut hops = 0usize;
        loop {
            let (count, next) = {
                let p = as_page(&guard);
                (SlottedPage::slot_count(p), BtreePage::next(p)?)
            };
            if count > 0 {
                found = Some(guard.page_id());
            }
            if next == page {
                return Ok(found);
            }
            if next == PageId::INVALID {
                return Err(BTreeError::Corrupted(format!(
                    "stale-prev walk from {} never reached {page}",
                    guard.page_id()
                )));
            }
            let next_guard = pool.pin(next)?;
            drop(guard);
            guard = next_guard;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    /// Optimistic INSERT placement (Stage T final-review fix): latch the
    /// leaf the new entry lands on and return `(guard, pos, side_guard)` —
    /// the leaf's write guard, the insertion slot (from
    /// `leaf_insert_slot`, so [`BTreeError::DuplicateKey`] surfaces here),
    /// and, for a slot-0 (left-edge) landing, the neighbor whose write
    /// latch MUST stay held through the caller's
    /// [`BTreeIndex::insert_into_page`].
    ///
    /// # Why the side latch
    ///
    /// A slot-0 insert places the entry at the chain boundary between this
    /// leaf and its left neighbor. The placement rule ("the probe belongs
    /// left iff the nearest non-empty left sibling's LAST entry sorts above
    /// it") is only as good as its atomicity: decided on a dropped latch,
    /// a concurrent insert of a LARGER entry into that sibling's tail
    /// between the check and our apply lands the larger entry left of our
    /// smaller one — a silent chain-order inversion inside a duplicate run
    /// (the Stage T `concurrent_duplicate_keys_lookup_all` release-mode
    /// failure), which `validate`'s equal-separator relaxation does not
    /// catch. So when the insertion slot is 0 (the leaf is empty or its
    /// first entry sorts above the probe), the decisive neighbor's write
    /// latch is taken BEFORE the verdict and held through the apply:
    ///
    /// - a non-empty left sibling L exists: hold L. Under L's latch its
    ///   last entry is stable (inserts into L's tail, deletes from L, and
    ///   L's own split all need L's latch). If it sorts above the probe,
    ///   the placement moves LEFT (L becomes the new current page, its
    ///   latch carried over); otherwise the caller inserts at slot 0 with
    ///   L held. (A split of L in the drop window is detected by walking
    ///   L's next chain back to the landing page — it must cross only
    ///   empty pages.)
    /// - only empty pages left of the landing page: the mirror threat is a
    ///   LARGER-tid same-key entry landing in one of those empty pages via
    ///   a flipped equal-separator descent (review M1); that inserter's own
    ///   slot-0 protocol holds the nearest non-empty page to ITS right —
    ///   our right boundary — so we hold the nearest non-empty RIGHT
    ///   sibling's write latch through the apply (coupled rightward, which
    ///   needs no drop). If its first entry sorts at or below the probe,
    ///   the placement moves right instead. cur itself was unlatched while
    ///   the left neighborhood was probed, so its slot-0 position is
    ///   RE-VALIDATED after the re-latch (a concurrent insert may have
    ///   claimed cur's left edge in the window; inserting at the stale
    ///   slot 0 would break the page's `(key, tid)` order and mask a
    ///   `DuplicateKey`); a failed re-validation restarts the placement.
    /// - the chain holds no non-empty page at all: nothing exists to
    ///   invert with; no side latch is taken.
    ///
    /// Interior landings (pos > 0) need no side latch: both neighbors of
    /// the insertion position are the leaf's own entries, stable under its
    /// write latch, and any left-edge insert into the right sibling holds
    /// THIS page's latch by the same protocol.
    ///
    /// # Residual (documented, never silent)
    ///
    /// The "no non-empty left page" verdict is taken after dropping cur
    /// (`nearest_nonempty_left` must walk left, which no latch may be held
    /// across). A concurrent slot-0 insert into one of those empty pages —
    /// reachable only after DELETEs drained it (in-flight split twins are
    /// undescendable: no downlink) — could in principle land a larger
    /// same-key entry left of ours in the verdict→apply window, inverting
    /// chain order across the pair. `validate`'s chain-order check (the
    /// full `(key, tid)` walk) reports such an inversion loudly as
    /// `Corrupted`; eliminating the window needs either latch-ordered left
    /// reads or page merge, both out of scope for M2b/M2c.
    ///
    /// Right hops skip THROUGH empty pages (never landing on one) for the
    /// same reason as the descent walk — see [`BTreeIndex::walk_to_position_guard`].
    /// The FPI discipline is [`BTreeIndex::pin_leaf_for_write`]'s: the side
    /// guard's page is never modified, so it needs no FPI.
    fn pin_leaf_for_insert(
        &self,
        leaf: PageId,
        key: &[u8],
        tid: &Tid,
    ) -> Result<(PageGuardMut<'_>, u16, Option<PageGuardMut<'_>>)> {
        let mut guard = self.buffer_pool.pin_mut_without_fpi(leaf)?;
        let mut hops = 0usize;
        loop {
            // Same FPI discipline as `pin_leaf_for_write` (see there).
            let split_incomplete =
                BtreePage::flags(as_page_mut(&mut guard))? & BTREE_FLAG_SPLIT_INCOMPLETE != 0;
            if !split_incomplete {
                self.buffer_pool.ensure_fpi(&mut guard)?;
            }

            // Right: hop to the nearest NON-EMPTY right sibling if its
            // first entry sorts at or below the probe (skip-through
            // semantics as in `walk_to_position_guard`; empty pages own
            // nothing and are never landed on).
            let mut next = BtreePage::next(as_page_mut(&mut guard))?;
            let mut hop = None;
            while next != PageId::INVALID {
                // Coupled: each step's read latch is taken while the
                // current page's (write) latch is held.
                let rguard = self.buffer_pool.pin(next)?;
                let page = as_page(&rguard);
                if SlottedPage::slot_count(page) == 0 {
                    let n2 = BtreePage::next(page)?;
                    drop(rguard);
                    next = n2;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                    continue;
                }
                let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                if (fk, ft) <= (key, *tid) {
                    hop = Some(next);
                }
                break;
            }
            if let Some(target) = hop {
                drop(guard);
                guard = self.buffer_pool.pin_mut_without_fpi(target)?;
                hops += 1;
                if hops > MAX_CHAIN_HOPS {
                    return Err(BTreeError::Corrupted(
                        "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                    ));
                }
                continue;
            }

            // Landed. The insertion slot (and the DuplicateKey check) under
            // the leaf's write latch.
            let pos = {
                let page = as_page_mut(&mut guard);
                leaf_insert_slot(page, key, tid)?
            };
            if pos > 0 {
                return Ok((guard, pos, None));
            }

            // Slot-0 landing: the leaf is empty or its first entry sorts
            // above the probe — the chain boundary with the left neighbor
            // decides, and the verdict must be atomic with the caller's
            // apply (see the fn doc).
            let cur_id = guard.page_id();
            drop(guard);
            // Never move left while holding a latch: cur is dropped; the
            // true nearest non-empty left sibling is found by walking RIGHT
            // from the (possibly stale) prev hint.
            let left = self.nearest_nonempty_left(cur_id)?;
            if let Some(lid) = left {
                let mut lguard = self.buffer_pool.pin_mut_without_fpi(lid)?;
                if SlottedPage::slot_count(as_page_mut(&mut lguard)) == 0 {
                    // L drained to empty in the window: restart — the next
                    // probe walks further left.
                    drop(lguard);
                    guard = self.buffer_pool.pin_mut_without_fpi(cur_id)?;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                    continue;
                }
                // Coupled right: re-latch cur while holding L.
                guard = self.buffer_pool.pin_mut_without_fpi(cur_id)?;
                // The segment L..cur must cross only empty pages; a
                // non-empty page in between means L split in the window and
                // its twin is the true neighbor — restart and rediscover.
                let mut w = BtreePage::next(as_page_mut(&mut lguard))?;
                let mut clean = true;
                while w != cur_id {
                    if w == PageId::INVALID {
                        return Err(BTreeError::Corrupted(format!(
                            "left-neighbor walk from {lid} never reached {cur_id}"
                        )));
                    }
                    let wguard = self.buffer_pool.pin(w)?;
                    let (wempty, wnext) = {
                        let p = as_page(&wguard);
                        (SlottedPage::slot_count(p) == 0, BtreePage::next(p)?)
                    };
                    drop(wguard);
                    if !wempty {
                        clean = false;
                        break;
                    }
                    w = wnext;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                }
                if !clean {
                    drop(lguard);
                    hops += 1;
                    continue;
                }
                // Re-validate cur itself (it was unlatched in between): the
                // probe must still sort at slot 0.
                let still_left_edge = {
                    let page = as_page_mut(&mut guard);
                    let count = SlottedPage::slot_count(page);
                    if count == 0 {
                        true
                    } else {
                        let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                        (fk, ft) > (key, *tid)
                    }
                };
                if !still_left_edge {
                    // cur's first entry moved (a concurrent insert landed
                    // inside cur, or the old first was deleted): recompute.
                    drop(lguard);
                    continue;
                }
                let (lk, lt) = {
                    let page = as_page_mut(&mut lguard);
                    let lcount = SlottedPage::slot_count(page);
                    page::decode_leaf_entry(entry_bytes(page, lcount as u16 - 1)?)?
                };
                if (lk, lt) > (key, *tid) {
                    // The probe sorts left of L's last entry: the placement
                    // is L or further left. L's write latch becomes the new
                    // current page's (drop the right page first).
                    drop(guard);
                    guard = lguard;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                    continue;
                }
                // Verdict final under L's latch: the caller inserts at
                // cur's slot 0 while L is held.
                return Ok((guard, 0, Some(lguard)));
            }

            // No non-empty page left of cur: re-latch cur and hold the
            // nearest non-empty RIGHT sibling through the apply (see the fn
            // doc for the mirror race this closes).
            #[cfg(feature = "test-hooks")]
            slot0_window_hook();
            guard = self.buffer_pool.pin_mut_without_fpi(cur_id)?;
            // Re-validate cur itself, exactly as the left-neighbor branch
            // above does: cur was UNLATCHED between the slot computation
            // and this re-latch, so a concurrent insert may have claimed
            // its left edge in the window. Without this check the caller
            // would insert at the STALE slot 0, ahead of an entry that
            // sorts below the probe — breaking the page's (key, tid) order
            // (and silently duplicating an exact re-insert instead of
            // failing DuplicateKey). Restart the placement; the recomputed
            // slot lands interior (pos > 0) and returns immediately.
            let still_left_edge = {
                let page = as_page_mut(&mut guard);
                let count = SlottedPage::slot_count(page);
                if count == 0 {
                    true
                } else {
                    let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                    (fk, ft) > (key, *tid)
                }
            };
            if !still_left_edge {
                continue;
            }
            let mut rnext = BtreePage::next(as_page_mut(&mut guard))?;
            let mut right = None;
            while rnext != PageId::INVALID {
                // Coupled rightward, cur's write latch held throughout.
                let mut rguard = self.buffer_pool.pin_mut_without_fpi(rnext)?;
                let page = as_page_mut(&mut rguard);
                if SlottedPage::slot_count(page) != 0 {
                    right = Some(rguard);
                    break;
                }
                rnext = BtreePage::next(page)?;
                hops += 1;
                if hops > MAX_CHAIN_HOPS {
                    return Err(BTreeError::Corrupted(
                        "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                    ));
                }
            }
            let Some(mut rguard) = right else {
                // The whole chain is empty: nothing to invert with.
                return Ok((guard, 0, None));
            };
            let (rk, rt) = {
                let page = as_page_mut(&mut rguard);
                page::decode_leaf_entry(entry_bytes(page, 0)?)?
            };
            if (rk, rt) <= (key, *tid) {
                // The probe sorts at/right of R's first entry: it belongs
                // to R or further right. R's write latch becomes the new
                // current page's.
                drop(guard);
                guard = rguard;
                hops += 1;
                if hops > MAX_CHAIN_HOPS {
                    return Err(BTreeError::Corrupted(
                        "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                    ));
                }
                continue;
            }
            return Ok((guard, 0, Some(rguard)));
        }
    }

    /// One pessimistic pass (§13.2): refresh the root from the meta page,
    /// crab down the descent path under coupled write latches, then either
    /// insert directly (a concurrent split made room) or split the leaf —
    /// with the right page reserved before the split pair is touched — and
    /// commit up the latched path. Returns [`Pessimistic::Retry`] when the
    /// tree moved under the pass (stale root, `SPLIT_INCOMPLETE` page,
    /// shifted key-range boundary, reservation failure); genuine errors
    /// (WAL, corruption, a mid-protocol cascade allocation failure — the
    /// same severity Stage M had) bubble.
    fn insert_pessimistic(&mut self, key: &[u8], tid: &Tid, entry: Vec<u8>) -> Result<Pessimistic> {
        // Re-read the authoritative root: a previous pass (or another
        // thread) may have promoted it since this handle cached it.
        self.refresh_root_from_meta()?;
        // The guard stack borrows this local `Arc` clone rather than `self`,
        // so the Commit can take `&mut self` (root promotion, meta record)
        // while the path latches are held.
        let pool = Arc::clone(&self.buffer_pool);
        let Some(mut stack) = Self::descend_write_path(&pool, self.root_page, key, tid)? else {
            return Ok(Pessimistic::Retry);
        };
        let mut leaf_guard = stack.pop().expect("the descent path ends at the leaf");
        let (pos, fits) = {
            let page = as_page_mut(&mut leaf_guard);
            let pos = leaf_insert_slot(page, key, tid)?;
            let needed = entry.len() + 4;
            (pos, SlottedPage::free_space(page) >= needed)
        };
        // Stage T final review: a slot-0 insert (the entry sorts before the
        // leaf's first entry — the leaf's LEFT chain boundary) must be
        // serialized against the left neighbor's tail through the apply.
        // That protocol lives in the OPTIMISTIC pass (`pin_leaf_for_insert`);
        // this path holds the descent path and can never latch leftward.
        // When the entry fits, restart and let the optimistic pass place it
        // (a concurrent split may have made the room we see). When the leaf
        // is FULL, split it WITHOUT the pending entry (median split) and
        // restart: the next pass inserts into the roomier left half through
        // the placement protocol. Without this, a boundary insert could land
        // left of a larger same-key entry — a silent chain-order inversion
        // inside a duplicate run (`pin_leaf_for_insert`'s doc).
        let left_edge_with_left_sibling =
            pos == 0 && BtreePage::prev(as_page_mut(&mut leaf_guard))? != PageId::INVALID;
        if fits {
            if left_edge_with_left_sibling {
                return Ok(Pessimistic::Retry);
            }
            self.insert_into_page(&mut leaf_guard, pos, entry)?;
            return Ok(Pessimistic::Done);
        }

        // Space reservation: allocate the split's right page BEFORE the
        // split pair is touched. Allocation can fail under pressure; per
        // spec that releases everything and restarts, not a bare Err bubble.
        // (The leaf has been write-latched continuously since the descent
        // validated it, so `split_prepare_on_guards`'s SPLIT_INCOMPLETE /
        // count pre-checks cannot actually fail here; if a future change
        // breaks that continuity, a failed Prepare AFTER this point simply
        // leaks the reserved page id — one empty 8 KB page, reclaimed by no
        // one but corrupting nothing.)
        let mut right_guard = match Self::reserve_split_page(&pool) {
            Ok(guard) => guard,
            Err(_) => return Ok(Pessimistic::Retry),
        };

        let st = self.split_prepare_on_guards(
            &mut leaf_guard,
            &mut right_guard,
            // A left-edge entry (slot 0 with a left sibling) is NOT carried
            // into the split: the split runs median (pending=None), and the
            // entry is inserted by the next pass through the optimistic
            // placement protocol, which holds the left neighbor's latch.
            if left_edge_with_left_sibling {
                None
            } else {
                Some(&entry)
            },
        )?;
        // The right page's post-copy first entry is the left page's current
        // `copy_start_slot` entry (the right page starts empty and receives
        // the moved tail in slot order).
        let right_first = entry_bytes(as_page_mut(&mut leaf_guard), st.copy_start_slot)?.to_vec();
        self.split_copy_on_guards(&mut leaf_guard, right_guard, &st)?;

        if left_edge_with_left_sibling {
            // Commit the (entry-less) split up the path, then restart: the
            // entry sorts before the left half's first entry, and the left
            // half now has room.
            drop(leaf_guard);
            let separator = entry_key(&right_first, st.level)?.to_vec();
            self.split_commit_guarded(&st, separator, &mut stack)?;
            return Ok(Pessimistic::Retry);
        }

        // Insert the pending entry into the half it sorts into.
        if entry_cmp(&entry, &right_first, true)? == Ordering::Less {
            let pos = entry_lower_bound(as_page_mut(&mut leaf_guard), &entry, true)? as u16;
            self.insert_into_page(&mut leaf_guard, pos, entry)?;
        } else {
            // Right-order acquisition while the left (leaf) latch is held —
            // the same order split Copy uses.
            let mut rguard = self.buffer_pool.pin_mut(st.right)?;
            let pos = entry_lower_bound(as_page_mut(&mut rguard), &entry, true)? as u16;
            self.insert_into_page(&mut rguard, pos, entry)?;
        }

        // Release the child level before the Commit walks up the path
        // (module doc: child latches are never held while the parent level
        // is touched).
        drop(leaf_guard);
        let separator = entry_key(&right_first, st.level)?.to_vec();
        self.split_commit_guarded(&st, separator, &mut stack)?;
        Ok(Pessimistic::Done)
    }

    /// Pessimistic descent: latch the root for write and crab DOWN the
    /// descent path, keeping every page's write latch (root..leaf; the leaf
    /// is the stack's last entry). Every newly latched page is re-validated:
    /// the root must still carry `ROOT` (`apply_commit_left` clears it under
    /// the old root's write latch, so under our own latch the flag is
    /// authoritative); a dominated page is walked left (drop-then-acquire);
    /// a page whose right sibling owns the probe, or a `SPLIT_INCOMPLETE`
    /// page, aborts the pass with `Ok(None)` — the path above it can no
    /// longer be trusted to cover the probe (hopping right would orphan the
    /// path for the Commit), so the whole pass restarts from the root.
    ///
    /// Associated function taking the pool explicitly: the returned guard
    /// stack borrows the caller's `Arc<BufferPool>` clone, not `self`, so
    /// the caller can still take `&mut self` for the Commit.
    fn descend_write_path<'a>(
        pool: &'a BufferPool,
        root_page: PageId,
        key: &[u8],
        tid: &Tid,
    ) -> Result<Option<Vec<PageGuardMut<'a>>>> {
        let mut stack: Vec<PageGuardMut<'_>> = Vec::new();
        // Acquisitions go through `pin_mut_without_fpi`; the cycle FPI is
        // emitted by `ensure_fpi` only AFTER the per-page checks below pass
        // (see the split_incomplete arm for why a plain `pin_mut` here could
        // emit a stale post-Commit FPI — Stage T P0 residual).
        let mut guard = pool.pin_mut_without_fpi(root_page)?;
        {
            let page = as_page_mut(&mut guard);
            if BtreePage::flags(page)? & BTREE_FLAG_ROOT == 0 {
                // Stale root: another thread promoted it before we latched.
                return Ok(None);
            }
        }
        let mut hops = 0usize;
        // Leaf-level insert placement: the page id whose left sibling was
        // already probed this pass and found NOT to sort above the probe
        // (the probe drops cur's latch to read the sibling, so without this
        // cache a stable candidate page would be re-probed forever).
        let mut placement_verified = PageId::INVALID;
        loop {
            let (level, split_incomplete, prev, next, dominated) = {
                let page = as_page_mut(&mut guard);
                let dominated = if SlottedPage::slot_count(page) == 0 {
                    false
                } else if BtreePage::level(page)? == 0 {
                    let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                    (fk, ft) > (key, *tid)
                } else {
                    let (fk, _) = page::decode_internal_entry(entry_bytes(page, 0)?)?;
                    fk > key
                };
                (
                    BtreePage::level(page)?,
                    BtreePage::flags(page)? & BTREE_FLAG_SPLIT_INCOMPLETE != 0,
                    BtreePage::prev(page)?,
                    BtreePage::next(page)?,
                    dominated,
                )
            };
            if split_incomplete {
                // Another split's Commit is in flight (only observable in
                // the root-split commit window) or was lost (post-crash).
                // Restart; the budget distinguishes the two. The guard was
                // taken WITHOUT an FPI on purpose: firing the cycle FPI
                // here could land it after the in-flight Commit record with
                // a pre-Commit image (Stage T P0 residual — recovery's
                // unconditional FPI replay would resurrect the flag).
                return Ok(None);
            }
            // The page passed the abort checks and stays on the path: emit
            // its due cycle FPI before anything can modify it.
            pool.ensure_fpi(&mut guard)?;
            if dominated && prev != PageId::INVALID && level != 0 {
                // INTERNAL-level left hop = the probe fell into a stale
                // separator gap (physical deletes raised a child's low
                // key). Hopping left here would leave the stack's "parent"
                // untrustworthy — the walked-to page may belong to the
                // stack-top's LEFT SIBLING, and a later Commit would
                // insert the twin downlink into the wrong parent. Retry
                // instead of continuing on an untrusted stack. A
                // transient cause (a boundary moving under us) resolves
                // on the next pass; a persistent stale gap exhausts the
                // restart budget and fails Unsupported — a documented
                // Stage Q limitation (module doc), not silent corruption.
                return Ok(None);
            }
            // Leaf-level insert placement (Stage T deadlock-stress fixes):
            // cur is a left CANDIDATE when it is empty or its first entry
            // sorts above the probe in full `(key, tid)` order — but the
            // decider is the TRUE nearest non-empty LEFT sibling's LAST
            // entry (`nearest_nonempty_left_on`: the prev link can skip
            // freshly split twins). Only if THAT sorts above the probe does
            // the new entry belong further left. The sibling's key alone
            // (or cur's) cannot decide: churn deleting a boundary key's
            // entries from cur leaves cur's first KEY above the probe's,
            // yet the probe's chain position is still cur's slot 0 when the
            // left sibling's last entry sorts below it (the
            // separator-ownership case); a genuinely smaller tid (freelist
            // reuse) or an exact re-insert sorts left and must reach the
            // run's left segment — preserving chain order and the
            // `DuplicateKey` check there.
            //
            // Latch note (pre-existing pattern): the walk drops the LEAF
            // guard but the path stack stays latched while left pages are
            // read-pinned — a (read) left move while holding the path. The
            // read latches cannot deadlock against writers' coupled RIGHT
            // hops (read/read compatible); a writer holding a left page who
            // wants OUR leaf write latch, while we want that left page's
            // read latch, is the one theoretical cycle — bounded in
            // practice by the walk's brevity and flagged here honestly.
            let candidate = level == 0
                && prev != PageId::INVALID
                && placement_verified != guard.page_id()
                && (dominated || {
                    let page = as_page_mut(&mut guard);
                    SlottedPage::slot_count(page) == 0
                });
            if candidate {
                let cur_id = guard.page_id();
                drop(guard);
                let left = Self::nearest_nonempty_left_on(pool, cur_id)?;
                let (go_left, walk) = match left {
                    None => (false, PageId::INVALID),
                    Some(lid) => {
                        let lguard = pool.pin(lid)?;
                        let page = as_page(&lguard);
                        let lcount = SlottedPage::slot_count(page);
                        let (lk, lt) =
                            page::decode_leaf_entry(entry_bytes(page, lcount as u16 - 1)?)?;
                        ((lk, lt) > (key, *tid), lid)
                    }
                };
                if go_left {
                    // Continue the descent FROM that sibling: the loop top
                    // re-runs its checks. The stack is untouched — the leaf
                    // chain is contiguous across parents, so the walked-to
                    // leaf stays under the same parent (and the Commit
                    // re-verifies parentage before writing the downlink).
                    guard = pool.pin_mut_without_fpi(walk)?;
                } else {
                    // cur holds the probe's chain position: re-pin it and
                    // re-validate from the loop top; the cached verdict
                    // keeps a stable page from being re-probed.
                    placement_verified = cur_id;
                    guard = pool.pin_mut_without_fpi(cur_id)?;
                }
                hops += 1;
                if hops > MAX_CHAIN_HOPS {
                    return Err(BTreeError::Corrupted(
                        "sibling chain exceeds hop bound (cycle?)".to_string(),
                    ));
                }
                continue;
            }
            // Right: walk to the next NON-EMPTY sibling (empty pages own
            // nothing and are skipped THROUGH, never landed on — see
            // `walk_to_position_guard`; an empty page's `next` is stable
            // because empty pages never split, and the latched cur's own
            // `next` cannot be re-pointed under our write latch).
            let mut hop_target = None;
            let mut rnext = next;
            while rnext != PageId::INVALID {
                let rguard = pool.pin(rnext)?;
                let rpage = as_page(&rguard);
                if SlottedPage::slot_count(rpage) == 0 {
                    let n2 = BtreePage::next(rpage)?;
                    drop(rguard);
                    rnext = n2;
                    hops += 1;
                    if hops > MAX_CHAIN_HOPS {
                        return Err(BTreeError::Corrupted(
                            "sibling chain exceeds hop bound (cycle?)".to_string(),
                        ));
                    }
                    continue;
                }
                let owns = if level == 0 {
                    let (fk, ft) = page::decode_leaf_entry(entry_bytes(rpage, 0)?)?;
                    (fk, ft) <= (key, *tid)
                } else {
                    let fk = first_entry_key(rpage)?.expect("non-empty page has a first key");
                    !fk.is_empty() && fk.as_slice() <= key
                };
                if owns {
                    hop_target = Some(rnext);
                }
                break;
            }
            if let Some(target) = hop_target {
                // The probe sorts right of the latched page. That is
                // either (Stage Q review M1) a COMPLETED split whose
                // downlink `find_child` missed — internal entries are
                // ordered by (key, child_page_id), and freelist reuse
                // can hand a twin a page id that breaks the id-order
                // assumption of that tie-break — or an in-flight/lost
                // Commit. In the first case the stack-top parent
                // provably holds the downlink to the target: hop right
                // (coupled, right-order latch acquisition) and continue
                // the descent instead of restarting. In the second,
                // restart as before.
                let parent_has_downlink = match stack.last_mut() {
                    Some(parent_guard) => {
                        internal_page_points_at(as_page_mut(parent_guard), target)?
                    }
                    None => false, // the root has no parent to check
                };
                if !parent_has_downlink {
                    return Ok(None);
                }
                // Read-pin → write-pin transition: the target is re-pinned
                // for write WITHOUT re-checking here. That is safe
                // because the loop's next iteration re-validates
                // ownership (dominated / right-hop conditions) against
                // the NEW guard's page — entries may have shifted into
                // the target in the drop window, but the re-validation
                // always runs on the page actually latched.
                drop(guard);
                guard = pool.pin_mut_without_fpi(target)?;
                hops += 1;
                if hops > MAX_CHAIN_HOPS {
                    return Err(BTreeError::Corrupted(
                        "sibling chain exceeds hop bound (cycle?)".to_string(),
                    ));
                }
                continue;
            }
            if level == 0 {
                stack.push(guard);
                return Ok(Some(stack));
            }
            let child = find_child(as_page_mut(&mut guard), key)?;
            // Write crabbing: the child's write latch is acquired while the
            // parent's is still held.
            let child_guard = pool.pin_mut_without_fpi(child)?;
            stack.push(guard);
            guard = child_guard;
        }
    }

    /// Guarded variant of [`BTreeIndex::split_commit`] for the pessimistic
    /// online path: the descent path above `st.left` is already
    /// write-latched (`stack`, root..parent). Emits the same records in the
    /// same order; only the latch choreography differs — parent latches come
    /// from the descent instead of being re-pinned, child-level latches are
    /// released before the parent level is touched, and the flag-clearing
    /// latch on `st.left` is a plain down-acquisition below the held path.
    fn split_commit_guarded(
        &mut self,
        st: &SplitState,
        separator: Vec<u8>,
        stack: &mut Vec<PageGuardMut<'_>>,
    ) -> Result<()> {
        let downlink = page::encode_internal_entry(&separator, st.right);

        let Some(mut parent_guard) = stack.pop() else {
            // Root split: nothing is latched (the old root's guard was
            // released with the child level). Generational check, then the
            // same record sequence as `split_commit`'s root branch. The
            // check cannot fire online — `st.left` is `SPLIT_INCOMPLETE`, so
            // no concurrent pass can split (and thus promote) it — but it
            // stays as a backstop.
            let current_root = self.refresh_root_from_meta()?;
            if current_root != st.left {
                return Err(BTreeError::Unsupported(format!(
                    "root page {} is stale (meta now points at {current_root}); \
                     reopen the index handle and retry the insert",
                    st.left
                )));
            }
            let new_root = self.create_new_root(st)?;
            // Test hook (see SPLIT_COMMIT_ROOT_CKPT_HOOK): simulate a
            // checkpoint publishing + flushing the brand-new root in the
            // (create_new_root, Commit append) window.
            #[cfg(feature = "test-hooks")]
            if SPLIT_COMMIT_ROOT_CKPT_HOOK.with(|c| c.replace(false)) {
                for page in self.buffer_pool.dirty_page_ids() {
                    self.buffer_pool.flush(page)?;
                }
                let ckpt = self.wal_writer.current_lsn();
                self.buffer_pool.set_checkpoint_lsn(ckpt);
            }
            // FPI-before-commit pre-touch (module doc): emit the cycle FPI,
            // if due, of BOTH pages this Commit modifies — new_root first
            // (parent), then st.left — BEFORE the record's WAL position is
            // fixed. new_root is fresh (needs_fpi=false), so its pre-touch
            // is normally a no-op; it matters when a checkpoint flushed the
            // new root in the window above (needs_fpi=true, pd_lsn=seed <
            // begin) — the applies below re-pin with the FPI suppressed.
            drop(self.buffer_pool.pin_mut(new_root)?);
            drop(self.buffer_pool.pin_mut(st.left)?);
            let rec = WalRecord::btree_split_commit(st.left, st.right, new_root, separator, 1)?;
            let lsn = self.wal_writer.append(rec)?;
            {
                let mut guard = self.buffer_pool.pin_mut_without_fpi(new_root)?;
                let page = as_page_mut(&mut guard);
                BtreePage::insert_entry_at(page, 1, &downlink)?;
                stamp_pd_lsn(page, lsn);
            }
            {
                let mut guard = self.buffer_pool.pin_mut_without_fpi(st.left)?;
                let page = as_page_mut(&mut guard);
                BtreePage::apply_commit_left(page)?;
                stamp_pd_lsn(page, lsn);
            }
            return Ok(());
        };

        // Parentage verification (Stage Q review): the popped page must
        // provably be `st.left`'s parent — it must hold a downlink to
        // `st.left`. A leaf-level left hop during the write descent can, in
        // the stale-separator-gap case (module doc's known limitation),
        // land the leaf under a DIFFERENT page than the stack top; writing
        // the twin downlink here without the check would silently corrupt
        // the parent's key ranges. Fail loudly instead of writing wrong.
        if !internal_page_points_at(as_page_mut(&mut parent_guard), st.left)? {
            return Err(BTreeError::Unsupported(format!(
                "descent path page {} holds no downlink to split page {}; the probe \
                 fell into a stale internal separator gap (documented Stage Q \
                 limitation) — cannot safely commit the split",
                parent_guard.page_id(),
                st.left
            )));
        }

        if SlottedPage::free_space(as_page_mut(&mut parent_guard)) >= downlink.len() + 4 {
            let parent = parent_guard.page_id();
            let slot =
                internal_lower_bound(as_page_mut(&mut parent_guard), &separator, st.right)? as u16;
            // FPI-before-commit pre-touch (module doc): emit `st.left`'s
            // cycle FPI, if due, BEFORE the Commit record's WAL position is
            // fixed; the apply below re-pins with the FPI suppressed. A
            // plain down-acquisition below the held path.
            drop(self.buffer_pool.pin_mut(st.left)?);
            let rec = WalRecord::btree_split_commit(st.left, st.right, parent, separator, slot)?;
            let lsn = self.wal_writer.append(rec)?;
            {
                let page = as_page_mut(&mut parent_guard);
                BtreePage::insert_entry_at(page, slot, &downlink)?;
                stamp_pd_lsn(page, lsn);
            }
            drop(parent_guard);
            // Flag clear on `st.left`: a plain down re-acquisition below the
            // latched path (never held while the parent level was touched).
            let mut guard = self.buffer_pool.pin_mut_without_fpi(st.left)?;
            let page = as_page_mut(&mut guard);
            BtreePage::apply_commit_left(page)?;
            stamp_pd_lsn(page, lsn);
            return Ok(());
        }

        // The parent has no room: split it first, recursively — the same
        // record order as `split_commit` (the parent's Prepare/Copy/Commit
        // all precede this split's Commit). A mid-protocol allocation
        // failure here bubbles, exactly as in Stage M: the already-emitted
        // steps are redo-correct, and the tree stays readable via the chain.
        let parent = parent_guard.page_id();
        let mut p2_guard = self.buffer_pool.new_page()?;
        let pst =
            self.split_prepare_on_guards(&mut parent_guard, &mut p2_guard, Some(&downlink))?;
        let p2_first = entry_bytes(as_page_mut(&mut parent_guard), pst.copy_start_slot)?.to_vec();
        let p2_first_key = entry_key(&p2_first, pst.level)?.to_vec();
        self.split_copy_on_guards(&mut parent_guard, p2_guard, &pst)?;
        // Release the parent latch before recursing: the recursive Commit
        // re-acquires `pst.left` down-order for the flag clear.
        drop(parent_guard);
        self.split_commit_guarded(&pst, p2_first_key.clone(), stack)?;

        // Choose the side for THIS split's downlink, in full (key, child)
        // order — the SAME rule `choose_split_slot` used when reserving room
        // for the pending downlink (a bare key comparison would disagree on
        // the equal-separator tie-break and could place the downlink on the
        // half without room). `pst.right` is published now, but nothing
        // below the parent level is latched, so this is a plain down/right
        // acquisition (module doc).
        let target = if entry_cmp(&downlink, &p2_first, false)? != Ordering::Less {
            pst.right
        } else {
            parent
        };
        let mut target_guard = self.buffer_pool.pin_mut(target)?;
        let slot =
            internal_lower_bound(as_page_mut(&mut target_guard), &separator, st.right)? as u16;
        // FPI-before-commit pre-touch (module doc): emit `st.left`'s cycle
        // FPI, if due, BEFORE the Commit record's WAL position is fixed; the
        // apply below re-pins with the FPI suppressed.
        drop(self.buffer_pool.pin_mut(st.left)?);
        let rec = WalRecord::btree_split_commit(st.left, st.right, target, separator, slot)?;
        let lsn = self.wal_writer.append(rec)?;
        {
            let page = as_page_mut(&mut target_guard);
            BtreePage::insert_entry_at(page, slot, &downlink)?;
            stamp_pd_lsn(page, lsn);
        }
        drop(target_guard);
        let mut guard = self.buffer_pool.pin_mut_without_fpi(st.left)?;
        let page = as_page_mut(&mut guard);
        BtreePage::apply_commit_left(page)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    /// Allocate the right page for an upcoming split (space reservation,
    /// Stage Q). Test-hookable via [`SPLIT_ALLOC_FAILURES`] (`test-hooks`
    /// feature only — the injection check compiles to nothing in production
    /// builds). Associated function taking the pool explicitly so the
    /// reserved guard borrows the caller's `Arc<BufferPool>` clone, not
    /// `self`.
    fn reserve_split_page(pool: &BufferPool) -> Result<PageGuardMut<'_>> {
        #[cfg(feature = "test-hooks")]
        {
            let injected = SPLIT_ALLOC_FAILURES.with(|failures| {
                let remaining = failures.get();
                if remaining > 0 {
                    failures.set(remaining - 1);
                    true
                } else {
                    false
                }
            });
            if injected {
                return Err(BTreeError::Storage(StorageError::BufferPoolFull));
            }
        }
        Ok(pool.new_page()?)
    }

    /// WAL-log (`BTreeInsert`) and apply an entry insert at `slot` of an
    /// already-pinned page that has room. Shared by leaf inserts, new-root
    /// initialization and meta records.
    fn insert_into_page(
        &self,
        guard: &mut PageGuardMut<'_>,
        slot: u16,
        entry: Vec<u8>,
    ) -> Result<()> {
        let page_id = guard.page_id();
        let page = as_page_mut(guard);
        let level = BtreePage::level(page)?;
        let flags = BtreePage::flags(page)?;
        let rec = WalRecord::btree_insert(page_id, slot, level, flags, entry.clone())?;
        let lsn = self.wal_writer.append(rec)?;
        BtreePage::insert_entry_at(page, slot, &entry)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Split: the three WAL steps (§13.3), individually drivable for crash
    // tests. The online path runs them back to back via `insert_pessimistic`
    // (guard-based variants with pre-reserved right page); the public
    // PageId-based steps below pin/allocate themselves and are unchanged
    // from Stage M.
    // ------------------------------------------------------------------

    /// §13.3 step 1: allocate the right sibling and emit + apply
    /// `BTreeSplitPrepare` (link `left.next = right`, set
    /// `SPLIT_INCOMPLETE` on the left page, initialize the right page
    /// header). The split point is the median slot. The right page's
    /// initialization is FPI-logged before the Prepare record — the page
    /// may be freelist-recycled, with a previous tenant's image on disk
    /// (A1; see `split_prepare_on_guards`).
    ///
    /// Refuses to split a page that is itself `SPLIT_INCOMPLETE`
    /// ([`BTreeError::Unsupported`]): such a page's previous split lost its
    /// Commit, so its right twin T has no parent downlink. Splitting it
    /// again would re-point `left.next` to a *new* twin T2 and give T2 the
    /// downlink, permanently orphaning T — and M2c's incomplete-split
    /// finish (`BTreeSplitCLR`) relies on `left.next` to find T. Until M2c
    /// finishes incomplete splits, a second split of such a page is
    /// forbidden (same severity as the bounded-restart guard in `insert`).
    pub fn split_prepare(&self, left: PageId) -> Result<SplitState> {
        let mut left_guard = self.buffer_pool.pin_mut(left)?;
        let mut right_guard = self.buffer_pool.new_page()?;
        self.split_prepare_on_guards(&mut left_guard, &mut right_guard, None)
    }

    /// [`BTreeIndex::split_prepare`] with a pending entry that still has to
    /// land on one of the halves: the split point is chosen so the receiving
    /// half's bytes + `pending` fit a fresh page (Stage Q review H3; used
    /// by the online paths, never by crash tests).
    pub(crate) fn split_prepare_with_pending(
        &self,
        left: PageId,
        pending: &[u8],
    ) -> Result<SplitState> {
        let mut left_guard = self.buffer_pool.pin_mut(left)?;
        let mut right_guard = self.buffer_pool.new_page()?;
        self.split_prepare_on_guards(&mut left_guard, &mut right_guard, Some(pending))
    }

    /// Guard-based [`BTreeIndex::split_prepare`]: the caller already holds
    /// both write latches (the pessimistic online path reserves the right
    /// page before latching the split pair; the public step API pins `left`
    /// and allocates the right page itself, unchanged from Stage M).
    ///
    /// `pending` is the entry whose insert triggered the split (the leaf
    /// entry, or the downlink for a cascading parent split). With `None`
    /// the split point is the median slot (exact Stage M behavior); with
    /// `Some` it is chosen byte-aware by `choose_split_slot` so the
    /// pending entry provably fits its landing half.
    fn split_prepare_on_guards(
        &self,
        left_guard: &mut PageGuardMut<'_>,
        right_guard: &mut PageGuardMut<'_>,
        pending: Option<&[u8]>,
    ) -> Result<SplitState> {
        let left = left_guard.page_id();
        let (level, old_next, high_key, copy_start_slot) = {
            let page = as_page_mut(left_guard);
            let level = BtreePage::level(page)?;
            if BtreePage::flags(page)? & BTREE_FLAG_SPLIT_INCOMPLETE != 0 {
                return Err(BTreeError::Unsupported(format!(
                    "page {left} is SPLIT_INCOMPLETE; a second split would orphan its \
                     uncommitted right twin (finishing incomplete splits is M2c work)"
                )));
            }
            let old_next = BtreePage::next(page)?;
            let count = SlottedPage::slot_count(page);
            if count < 2 {
                return Err(BTreeError::Corrupted(format!(
                    "cannot split page {left} with {count} entries"
                )));
            }
            let high_key = entry_key(entry_bytes(page, (count - 1) as u16)?, level)?.to_vec();
            let copy_start_slot = choose_split_slot(page, level, pending)?;
            (level, old_next, high_key, copy_start_slot)
        };

        let right = right_guard.page_id();

        // A1 (docs/stage_spec.md:879, ROADMAP.md appendix A1): the allocator
        // may hand a freelist-RECYCLED page whose previous tenant's image is
        // still on disk, and `BufferPool::new_page` clears `needs_fpi`
        // unconditionally — its "new page has no old on-disk image"
        // assumption is false for a recycled page, so the page's first
        // modification in this checkpoint cycle owes an FPI the double gate
        // would never emit. A power loss tearing the recycled page's first
        // flush then leaves recovery a torn image whose sector-0 `pd_lsn`
        // can already be the Copy LSN: the pd_lsn-guarded Prepare/Copy redo
        // would skip, and the tree would keep a half-predecessor right page.
        // Fix here, at the single choke point every online split funnels
        // through (the public step API, the pessimistic insert path, and
        // cascading parent splits): initialize the right page and make the
        // initialization durable with a post-image FPI BEFORE the Prepare
        // record — the same pattern as `create`, `create_new_root` and
        // `split_page_in_undo`. Unconditional FPI replay then restores the
        // freshly initialized page no matter how the flush tore, and
        // Prepare/Copy re-apply on top under the usual pd_lsn guards.
        //
        // Why not inside `BufferPool::new_page` (the other recorded
        // candidate): the FPI gate also requires `pd_lsn < checkpoint_lsn`,
        // but the first `pin_mut` after `new_page` only runs after the AM
        // has already initialized the page and stamped a post-checkpoint
        // pd_lsn, so a `needs_fpi` flag raised there would never fire in the
        // cycle that matters. Nor can an FPI simply be forced past the gate:
        // a post-init image landing after its owning init record is harmless
        // in itself — the real hazard is that an FPI armed at `new_page`
        // time stays live for the rest of the checkpoint cycle, so a
        // third-party re-pin can fire it while the page sits in a
        // mid-protocol state (e.g. SPLIT_INCOMPLETE, after Prepare has
        // applied), capturing that intermediate image at a WAL position
        // after the records that own the state — the exact inversion Stage
        // T P0 forbids.
        {
            let page = as_page_mut(right_guard);
            BtreePage::init_right_page(page, left, old_next, level);
        }
        log_page_init(&self.wal_writer, right, as_page_mut(right_guard))?;

        let rec = WalRecord::btree_split_prepare(left, right, level, old_next, high_key)?;
        let lsn = self.wal_writer.append(rec)?;
        // The right page already matches the Prepare record's effects (the
        // init above is exactly what Prepare redo re-applies); only advance
        // its pd_lsn so redo recognizes the record as applied.
        stamp_pd_lsn(as_page_mut(right_guard), lsn);
        {
            let page = as_page_mut(left_guard);
            BtreePage::apply_prepare_left(page, right)?;
            stamp_pd_lsn(page, lsn);
        }
        Ok(SplitState {
            left,
            right,
            level,
            copy_start_slot,
            prepare_lsn: lsn,
        })
    }

    /// §13.3 step 2: emit + apply `BTreeSplitCopy` — move
    /// `[copy_start_slot, slot_count)` from the left page to the right page
    /// and truncate the left LP array. `left_page_pre_lsn` anchors redo
    /// idempotency.
    ///
    /// After applying, the right page is flushed before the left guard is
    /// released: redo's Copy recomputes the moved entries from the left
    /// page's pre-copy image, so the left page's post-copy image must never
    /// be durable while the right page's is not. Both pages are pinned
    /// (eviction-proof) until the right page's flush completes.
    pub fn split_copy(&self, st: &SplitState) -> Result<Lsn> {
        let mut left_guard = self.buffer_pool.pin_mut(st.left)?;
        let right_guard = self.buffer_pool.pin_mut(st.right)?;
        self.split_copy_on_guards(&mut left_guard, right_guard, st)
    }

    /// Guard-based [`BTreeIndex::split_copy`]: the caller keeps holding the
    /// left guard; the right guard is consumed so the flush-before-release
    /// discipline stays inside one function — the right page is flushed
    /// after the copy is applied and before this function returns, while the
    /// caller still holds the left latch (so the left page's post-copy image
    /// can never reach disk without the right page's).
    fn split_copy_on_guards(
        &self,
        left_guard: &mut PageGuardMut<'_>,
        mut right_guard: PageGuardMut<'_>,
        st: &SplitState,
    ) -> Result<Lsn> {
        // The anchor is read AFTER the write latch is held: if a checkpoint
        // landed between Prepare and Copy, the pool fires an FPI for the
        // left page on `pin_mut` and pushes its `pd_lsn` to the FPI's LSN.
        // That is fine — the anchor is "whatever `pd_lsn` is now" (Prepare
        // LSN or the FPI LSN covering the same content), the record carries
        // it, and redo compares equality: replaying the FPI restores exactly
        // this pre-copy image and stamps the same FPI LSN, so the anchor
        // holds either way.
        let pre_lsn = page_pd_lsn(as_page_mut(left_guard));

        let rec = WalRecord::btree_split_copy(st.left, st.right, st.copy_start_slot, pre_lsn)?;
        let lsn = self.wal_writer.append(rec)?;
        apply_split_copy(
            as_page_mut(left_guard),
            as_page_mut(&mut right_guard),
            st.copy_start_slot,
            true,
        )?;
        stamp_pd_lsn(as_page_mut(left_guard), lsn);
        stamp_pd_lsn(as_page_mut(&mut right_guard), lsn);

        // Flush the right page's post-copy image first (see the doc above).
        // Dropping the guard releases the write latch so `flush` can take a
        // read latch; the left guard stays held by the caller, so the left
        // page cannot be evicted/flushed before the right page is durable.
        drop(right_guard);
        match self.buffer_pool.flush(st.right) {
            Ok(()) => {}
            // The unpin → flush window lets CLOCK evict the right page
            // first. That is NOT a durability gap: `evict_frame` runs
            // `flush_frame` (WAL-before-data, fsync included) BEFORE
            // dropping the page-table mapping (Stage Q final review — the
            // order used to be reversed, which made PageNotFound observable
            // in the not-yet-durable window), and a mapping still present
            // means `flush` waits out the in-flight eviction flush via the
            // H1 flush_done handshake. So PageNotFound is only observable
            // after the post-copy image is durable.
            Err(StorageError::PageNotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(lsn)
    }

    /// §13.3 step 3: emit + apply `BTreeSplitCommit` — insert the downlink
    /// `(separator_key, right_page)` into the parent and clear
    /// `SPLIT_INCOMPLETE` (and `ROOT`, for a root split) on the left page.
    ///
    /// `path` is the descent path recorded when the split was triggered
    /// (root..parent of `left`); the parent is popped from it. A parent
    /// without room for the downlink is split first, recursively. When
    /// `left` is the root, a new root is allocated, seeded with
    /// `(-infinity -> left)`, the meta page is updated, and the downlink
    /// lands at slot 1.
    pub fn split_commit(&mut self, st: &SplitState, path: &mut Vec<PageId>) -> Result<()> {
        let separator = {
            let bytes = self.first_entry_bytes(st.right)?;
            entry_key(&bytes, st.level)?.to_vec()
        };
        let downlink = page::encode_internal_entry(&separator, st.right);

        let (parent, slot) = if st.left == self.root_page {
            // Generational check: this handle cached `root_page` at
            // open/last-root-split, but ANOTHER handle on the same index
            // may have promoted the root since. Re-read the meta page; if
            // it no longer points at `st.left`, creating a "new root" here
            // would fork the tree (two roots, meta overwritten, half the
            // tree unreachable). Refresh the handle from the meta page and
            // fail loudly instead — the caller must reopen the handle and
            // retry. (The alternative of continuing with the stale descent
            // path has no correct parent to attach to: the path was
            // recorded when `st.left` was the root, so it is empty.)
            let current_root = self.refresh_root_from_meta()?;
            if current_root != st.left {
                return Err(BTreeError::Unsupported(format!(
                    "root page {} is stale (meta now points at {current_root}); \
                     reopen the index handle and retry the insert",
                    st.left
                )));
            }
            let new_root = self.create_new_root(st)?;
            (new_root, 1u16)
        } else {
            let mut parent = path.pop().ok_or_else(|| {
                BTreeError::Corrupted(format!(
                    "split of non-root page {} with an empty descent path",
                    st.left
                ))
            })?;
            if !self.page_fits(parent, downlink.len())? {
                // Split the parent first; the pending downlink is applied by
                // THIS split's Commit afterwards (each downlink is logged by
                // exactly one Commit record).
                let pst = self.split_prepare_with_pending(parent, &downlink)?;
                self.split_copy(&pst)?;
                self.split_commit(&pst, path)?;
                let right_first = self.first_entry_bytes(pst.right)?;
                // Full (key, child) comparison — the same rule
                // `choose_split_slot` reserved room by (see
                // `split_commit_guarded`).
                if entry_cmp(&downlink, &right_first, false)? != Ordering::Less {
                    parent = pst.right;
                }
            }
            let slot = self.internal_insert_slot(parent, &separator, st.right)?;
            (parent, slot)
        };

        // FPI-before-commit pre-touch (module doc, Stage T P0): emit any due
        // cycle FPI for BOTH pages this record modifies — parent before
        // left, down the tree — BEFORE the record's WAL position is fixed.
        // An FPI landing after the Commit with a pre-commit image would be
        // replayed unconditionally by recovery and roll the page back past
        // this Commit (resurrected SPLIT_INCOMPLETE → spurious undo CLR with
        // a duplicate downlink). The applies below re-pin with the FPI
        // suppressed.
        drop(self.buffer_pool.pin_mut(parent)?);
        drop(self.buffer_pool.pin_mut(st.left)?);
        let rec = WalRecord::btree_split_commit(st.left, st.right, parent, separator, slot)?;
        let lsn = self.wal_writer.append(rec)?;
        {
            let mut guard = self.buffer_pool.pin_mut_without_fpi(parent)?;
            let page = as_page_mut(&mut guard);
            BtreePage::insert_entry_at(page, slot, &downlink)?;
            stamp_pd_lsn(page, lsn);
        }
        {
            let mut guard = self.buffer_pool.pin_mut_without_fpi(st.left)?;
            let page = as_page_mut(&mut guard);
            BtreePage::apply_commit_left(page)?;
            stamp_pd_lsn(page, lsn);
        }
        Ok(())
    }

    /// Allocate and seed the new root for a root split: a fresh page at
    /// `st.level + 1` holding `(-infinity -> left)` at slot 0, then a meta
    /// record pointing at it. The downlink to `st.right` is added by the
    /// caller's Commit apply at slot 1.
    ///
    /// Slot 0 of an internal page carries an **empty key as the -infinity
    /// marker** (PG's `P_HIKEY` convention): the leftmost child's low key
    /// can decrease over time (descending inserts), and a real key here
    /// would go stale and scramble the parent's key order against the
    /// sibling-chain order. Non-leftmost pages always carry a real
    /// separator at slot 0 (copied verbatim by splits), so parent markers
    /// can only go stale *low* (physical deletes), which the descent's
    /// left-walk absorbs.
    ///
    /// The new root's initialization is made durable with a post-image
    /// `FullPageImage` (same pattern as [`BTreeIndex::create`]): the
    /// allocator may hand us a freelist-RECYCLED page whose on-disk image
    /// is a previous tenant's bytes (`pd_upper != 0`), and then redo's
    /// `init_if_fresh` for the seed `BTreeInsert` would not fire — the
    /// insert would apply onto garbage geometry and silently corrupt the
    /// root. Root splits are rare, so the extra 8 KB FPI is cheap.
    fn create_new_root(&mut self, st: &SplitState) -> Result<PageId> {
        // `btpo_level` is a 4-bit field (§13.1): the 16th root promotion
        // would overflow `level` into the `btpo_flags` bits. Make the
        // implicit assumption an explicit contract instead of corrupting the
        // flags nibble (a 15-level tree of 8 KB pages is unreachable in
        // practice, so failing loudly here is always right).
        if st.level >= 0x0F {
            return Err(BTreeError::Corrupted(format!(
                "tree level {} already at the 4-bit maximum; cannot promote the root",
                st.level
            )));
        }
        let new_level = st.level + 1;

        let new_root = {
            let mut guard = self.buffer_pool.new_page()?;
            let page_id = guard.page_id();
            {
                let page = as_page_mut(&mut guard);
                BtreePage::init(page, new_level, BTREE_FLAG_ROOT);
            }
            // Post-image FPI for the initialization (doc above): redo then
            // restores the initialized page regardless of what the recycled
            // on-disk image held, and the seed insert applies on top under
            // the usual pd_lsn guard.
            log_page_init(&self.wal_writer, page_id, as_page_mut(&mut guard))?;
            // -infinity -> old root (see the doc above).
            let e0 = page::encode_internal_entry(&[], st.left);
            self.insert_into_page(&mut guard, 0, e0)?;
            page_id
        };

        self.root_page = new_root;
        self.tree_level = new_level;
        self.write_meta_record()?;
        Ok(new_root)
    }

    /// Does `page_id` have room for an entry of `entry_len` bytes?
    fn page_fits(&self, page_id: PageId, entry_len: usize) -> Result<bool> {
        Ok(self.page_free_space(page_id)? >= entry_len + 4)
    }

    /// Insertion slot for the downlink `(key, child)` on an internal page.
    fn internal_insert_slot(&self, page_id: PageId, key: &[u8], child: PageId) -> Result<u16> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        Ok(internal_lower_bound(page, key, child)? as u16)
    }

    /// Read the raw bytes of a page's first entry.
    fn first_entry_bytes(&self, page_id: PageId) -> Result<Vec<u8>> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        if SlottedPage::slot_count(page) == 0 {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} has no entries"
            )));
        }
        Ok(entry_bytes(page, 0)?.to_vec())
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    /// Physically remove the exact `(key, tid)` entry (`BTreeDelete`; M2b
    /// has no page merge).
    ///
    /// Under concurrency, [`BTreeError::EntryNotFound`] is a LEGITIMATE
    /// transient outcome: a concurrent transaction may have removed the
    /// same entry between the descent and the leaf latch (the re-validation
    /// only proves the entry is not on the owning leaf *now*). Callers that
    /// require existence (e.g. index-undo for a just-inserted entry) must
    /// treat EntryNotFound as "already gone", not as corruption.
    pub fn delete(&mut self, key: &[u8], tid: Tid) -> Result<()> {
        let (leaf, _, _) = self.descend_to_leaf(key, &tid)?;
        // Re-validate ownership under the write latch: a concurrent split
        // may have moved the entry to a right sibling since the descent.
        // A SPLIT_INCOMPLETE leaf (its Commit in flight) is deleted from
        // normally — an in-window write is a designed Stage S case; only
        // the page's cycle FPI is suppressed for this hold (see
        // `pin_leaf_for_write`). LOCATE semantics: left hops stay enabled —
        // the entry being deleted may be a split-boundary duplicate sitting
        // left of its separator.
        let (mut guard, _) = self.pin_leaf_for_write(leaf, key, &tid)?;
        // The WAL record must name the page the delete is APPLIED to — the
        // re-validated guard's page, which is not necessarily the descent's
        // `leaf` (a hop moved us to a right twin). Logging the stale id
        // would make redo replay the delete on the wrong page (silently
        // losing it, or removing an innocent entry at the same slot).
        let leaf = guard.page_id();
        let page = as_page_mut(&mut guard);
        let pos = leaf_lower_bound(page, key, &tid)?;
        let count = SlottedPage::slot_count(page);
        if pos >= count {
            return Err(BTreeError::EntryNotFound);
        }
        let (k, t) = page::decode_leaf_entry(entry_bytes(page, pos as u16)?)?;
        if k != key || t != tid {
            return Err(BTreeError::EntryNotFound);
        }
        let slot = pos as u16;
        let rec = WalRecord::btree_delete(leaf, slot)?;
        let lsn = self.wal_writer.append(rec)?;
        BtreePage::remove_entry_at(page, slot)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Structural validation (tests / diagnostics)
    // ------------------------------------------------------------------

    /// Strict structural validation, intended for tests and diagnostics.
    ///
    /// **Quiescent-state check**: this must not run concurrently with
    /// writers — a split in flight transiently violates the checks below
    /// (a right twin reachable via the chain before its downlink lands,
    /// `SPLIT_INCOMPLETE` set), which is not corruption.
    ///
    /// Checks, recursively from the root: page geometry, `btpo_level`
    /// consistency, entries strictly sorted in full `(key, trailer)` order,
    /// and **adjacent subtree ranges strictly increasing** — each child's
    /// last leaf entry must sort below the next child's first leaf entry.
    /// The boundary check compares full entries rather than parent
    /// separator keys: separator keys can legitimately go stale (physical
    /// deletes raise a page's first key; duplicate keys at a split point
    /// legitimately live on both sides), but the sibling-chain order is the
    /// ground truth the descent walk relies on. Finally, the leaf chain
    /// walked from the leftmost leaf must match the root-reachable leaves,
    /// in order, and no page may be `SPLIT_INCOMPLETE`.
    ///
    /// A delete-drained EMPTY leaf is a legitimate quiescent state (M2b has
    /// no page merge): it owns no keys, so it takes no part in the boundary
    /// comparisons, but it must still appear in the leaf chain.
    ///
    /// An index carrying a recovered incomplete split fails this check on
    /// purpose (its right twin is unreachable from the root); crash tests
    /// assert the weaker chain/lookup properties instead.
    pub fn validate(&self) -> Result<()> {
        // Re-read the authoritative root from the meta page (review M2):
        // the handle's cached root may have been promoted by ANOTHER
        // handle, and validating from a demoted root would check only a
        // subtree and misreport Corrupted on a healthy tree.
        let (root_page, tree_level) = root_from_meta(&self.buffer_pool, self.meta_page)?;
        // An empty index (root leaf with no entries, e.g. right after
        // `create` or a bulk load of zero rows) is trivially valid.
        let root_slot_count = {
            let guard = self.buffer_pool.pin(root_page)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            BtreePage::level(page)?;
            SlottedPage::slot_count(page)
        };
        if root_slot_count == 0 {
            if tree_level != 0 {
                return Err(BTreeError::Corrupted(format!(
                    "empty root page {root_page} at tree level {tree_level}"
                )));
            }
            return Ok(());
        }

        let mut leaves = Vec::new();
        self.validate_page(root_page, tree_level, &mut leaves)?;

        // The leaf chain from the leftmost leaf must visit exactly the
        // root-reachable leaves, in order.
        let (leftmost, _, _) = self.descend_to_leaf_from(
            root_page,
            &[],
            &Tid {
                page_id: PageId::INVALID,
                slot_id: 0,
            },
        )?;
        let mut chain = Vec::new();
        let mut cur = leftmost;
        let mut hops = 0usize;
        // Full `(key, tid)` chain order across consecutive NON-EMPTY leaves
        // (Stage T final review): the placement protocol keeps the chain
        // globally sorted, so a boundary inversion inside a duplicate run —
        // which the per-parent boundary check deliberately tolerates when
        // the two separators are EQUAL (review M1) — is caught here, loudly.
        let mut prev_last_entry: Option<Vec<u8>> = None;
        loop {
            chain.push(cur);
            let guard = self.buffer_pool.pin(cur)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            let count = SlottedPage::slot_count(page);
            if count > 0 {
                let first = entry_bytes(page, 0)?.to_vec();
                let last = entry_bytes(page, (count - 1) as u16)?.to_vec();
                if let Some(prev_last) = &prev_last_entry {
                    if entry_cmp(&first, prev_last, true)? != Ordering::Greater {
                        return Err(BTreeError::Corrupted(format!(
                            "leaf {cur} first entry does not sort above the previous \
                             non-empty leaf's last entry (chain order broken)"
                        )));
                    }
                }
                prev_last_entry = Some(last);
            }
            let next = BtreePage::next(page)?;
            drop(guard);
            if next == PageId::INVALID {
                break;
            }
            cur = next;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
        if chain != leaves {
            // Blink tolerates ORDER differences at duplicate separators:
            // freelist reuse can hand a split twin a page id that flips the
            // (key, child) tie-break, so the parent entry order and the
            // chain order legitimately disagree among equal separators
            // (review M1). The SET of root-reachable leaves must still
            // match the chain exactly; order among DISTINCT keys is
            // enforced by the per-parent range check below.
            let mut chain_sorted = chain.clone();
            let mut leaves_sorted = leaves.clone();
            chain_sorted.sort();
            leaves_sorted.sort();
            if chain_sorted != leaves_sorted {
                return Err(BTreeError::Corrupted(format!(
                    "leaf chain {chain:?} disagrees with root-reachable leaves {leaves:?}"
                )));
            }
        }
        Ok(())
    }

    /// Recursive helper for [`BTreeIndex::validate`]: check one subtree,
    /// append its leaves (in order) to `leaves`, and return the subtree's
    /// first and last **leaf** entry bytes (full `(key, tid)` order
    /// boundaries). Returns `None` for a subtree that holds NO entries at
    /// all (delete-drained leaves are legitimate — see the fn doc above).
    fn validate_page(
        &self,
        page_id: PageId,
        expect_level: u8,
        leaves: &mut Vec<PageId>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let level = BtreePage::level(page)?;
        let flags = BtreePage::flags(page)?;
        if level != expect_level {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} at level {level}, expected {expect_level}"
            )));
        }
        if flags & BTREE_FLAG_SPLIT_INCOMPLETE != 0 {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} still SPLIT_INCOMPLETE"
            )));
        }
        if (level == 0) != (flags & BTREE_FLAG_LEAF != 0) {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} level {level} disagrees with LEAF flag"
            )));
        }
        let count = SlottedPage::slot_count(page);
        if count == 0 {
            if level != 0 {
                // An internal page always carries at least its leftmost
                // (-infinity) downlink; empty means corruption.
                return Err(BTreeError::Corrupted(format!(
                    "internal page {page_id} is empty"
                )));
            }
            // Delete-drained leaf: owns no keys, contributes no boundary.
            leaves.push(page_id);
            return Ok(None);
        }

        // Entries must be strictly sorted in full `(key, trailer)` order
        // (duplicate keys are allowed; the trailer disambiguates).
        for slot in 1..count as u16 {
            let prev = entry_bytes(page, slot - 1)?;
            let cur = entry_bytes(page, slot)?;
            if entry_cmp(cur, prev, level == 0)? != Ordering::Greater {
                return Err(BTreeError::Corrupted(format!(
                    "page {page_id} entries out of order at slot {slot}"
                )));
            }
        }

        if level == 0 {
            leaves.push(page_id);
            return Ok(Some((
                entry_bytes(page, 0)?.to_vec(),
                entry_bytes(page, (count - 1) as u16)?.to_vec(),
            )));
        }

        // Internal page: recurse into every child and require adjacent
        // subtree ranges to strictly increase. Exception (review M1):
        // adjacent children whose PARENT SEPARATOR KEYS are equal may
        // appear in either order — duplicate separators tie in (key, child)
        // order, and freelist reuse can flip that tie — without any
        // corruption (the leaf chain is the ground truth and is checked
        // above). Distinct separators keep the strict requirement. Empty
        // subtrees own no keys and are skipped by the boundary check.
        let mut subtree_first: Option<Vec<u8>> = None;
        let mut prev_last: Option<Vec<u8>> = None;
        let mut prev_sep: Option<Vec<u8>> = None;
        for slot in 0..count as u16 {
            let (sep, child) = page::decode_internal_entry(entry_bytes(page, slot)?)?;
            let Some((child_first, child_last)) = self.validate_page(child, level - 1, leaves)?
            else {
                continue;
            };
            if let Some(prev) = &prev_last {
                if entry_cmp(&child_first, prev, true)? != Ordering::Greater
                    && prev_sep.as_deref() != Some(sep)
                {
                    return Err(BTreeError::Corrupted(format!(
                        "page {page_id} child {child} range overlaps or is out of order"
                    )));
                }
            }
            if subtree_first.is_none() {
                subtree_first = Some(child_first);
            }
            prev_last = Some(child_last);
            prev_sep = Some(sep.to_vec());
        }
        match prev_last {
            Some(last) => Ok(Some((
                subtree_first.expect("a non-empty child sets the subtree's first"),
                last,
            ))),
            // Every child was an empty leaf: this subtree holds no entries.
            None => Ok(None),
        }
    }
}

// ----------------------------------------------------------------------
// Free helpers
// ----------------------------------------------------------------------

/// Apply the Copy transformation (§13.3 step 2): move every entry of the
/// left page at `>= copy_start_slot` onto the right page (appended in slot
/// order), then **rebuild** the left page with the entries it keeps.
///
/// A bare LP-array truncation would leave the moved tuple bytes as dead
/// space (`pd_upper` never recovers), so the left page would still be
/// effectively full and the very insert that triggered the split would not
/// fit. Rebuilding compacts the kept entries back to a fresh page while
/// preserving `pd_lsn`, `btpo_prev`/`btpo_next`, level and flags. The
/// transformation is deterministic, so the online path and the
/// `BTreeSplitCopy` redo handler produce byte-identical pages.
///
/// `move_to_right` is `false` only for the redo interleaving where the
/// right page's post-copy image is already durable (it then holds the
/// entries, and only the left page's rebuild is missing).
pub(crate) fn apply_split_copy(
    left_page: &mut [u8; PAGE_SIZE],
    right_page: &mut [u8; PAGE_SIZE],
    copy_start_slot: u16,
    move_to_right: bool,
) -> Result<()> {
    let count = SlottedPage::slot_count(left_page) as u16;
    // `copy_start_slot == count` is the no-op case: the copy was already
    // applied (the left page holds exactly the kept entries), so the rebuild
    // below deterministically re-packs the same content. `0` or beyond the
    // slot count is genuine corruption.
    if copy_start_slot == 0 || copy_start_slot > count {
        return Err(BTreeError::Corrupted(format!(
            "copy_start_slot {copy_start_slot} outside slot count {count}"
        )));
    }
    // Collect first, so the borrow of the left page ends before the right
    // page is mutated and the rebuild starts from a clean slate.
    let mut kept: Vec<Vec<u8>> = Vec::new();
    let mut moved: Vec<Vec<u8>> = Vec::new();
    for slot in 0..count {
        let bytes = entry_bytes(left_page, slot)?.to_vec();
        if slot < copy_start_slot {
            kept.push(bytes);
        } else {
            moved.push(bytes);
        }
    }
    if move_to_right {
        for entry in &moved {
            // The right page starts empty (Prepare initialized it), so heap
            // append is slot-deterministic.
            SlottedPage::add_tuple(right_page, entry)?;
        }
    }

    // Rebuild the left page, preserving its identity fields.
    let pd_lsn = page_pd_lsn(left_page);
    let prev = BtreePage::prev(left_page)?;
    let next = BtreePage::next(left_page)?;
    let level = BtreePage::level(left_page)?;
    let flags = BtreePage::flags(left_page)?;
    BtreePage::init(left_page, level, flags);
    BtreePage::set_prev(left_page, prev);
    BtreePage::set_next(left_page, next);
    for entry in &kept {
        SlottedPage::add_tuple(left_page, entry)?;
    }
    set_page_pd_lsn(left_page, pd_lsn);
    Ok(())
}

/// Binary search: first slot whose entry is `>= (key, tid)` in full
/// `(key, tid)` order — the insertion point, or the exact match candidate.
fn leaf_lower_bound(page: &[u8; PAGE_SIZE], key: &[u8], tid: &Tid) -> Result<usize> {
    let count = SlottedPage::slot_count(page);
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (k, t) = page::decode_leaf_entry(entry_bytes(page, mid as u16)?)?;
        if k.cmp(key).then(t.cmp(tid)) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Insertion slot for `(key, tid)` on a leaf page, or
/// [`BTreeError::DuplicateKey`] when the exact pair is already present.
fn leaf_insert_slot(page: &[u8; PAGE_SIZE], key: &[u8], tid: &Tid) -> Result<u16> {
    let pos = leaf_lower_bound(page, key, tid)?;
    let count = SlottedPage::slot_count(page);
    if pos < count {
        let (k, t) = page::decode_leaf_entry(entry_bytes(page, pos as u16)?)?;
        if k == key && t == *tid {
            return Err(BTreeError::DuplicateKey);
        }
    }
    Ok(pos as u16)
}

/// Count one whole-insert restart; fail with [`BTreeError::Unsupported`]
/// once `max_restarts` is exhausted (see `MAX_INSERT_RESTARTS` and the
/// module doc's restart section).
fn restart_or_fail(restarts: &mut usize, max_restarts: usize) -> Result<()> {
    *restarts += 1;
    if *restarts > max_restarts {
        return Err(BTreeError::Unsupported(format!(
            "insert restarted {max_restarts} times without progress; either \
             a sustained concurrent split / allocation-pressure storm starved every \
             pass (transient — retrying later may succeed), or the tree carries an \
             incomplete split whose Commit was lost (post-crash state; finishing \
             incomplete splits is M2c undo work), or the probe fell into a stale \
             internal separator gap (documented Stage Q limitation)"
        )));
    }
    Ok(())
}

/// Binary search on an internal page: first slot whose entry is
/// `>= (key, child)` in full `(key, child_page_id)` order.
fn internal_lower_bound(page: &[u8; PAGE_SIZE], key: &[u8], child: PageId) -> Result<usize> {
    let count = SlottedPage::slot_count(page);
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (k, c) = page::decode_internal_entry(entry_bytes(page, mid as u16)?)?;
        if k.cmp(key).then(c.cmp(&child)) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Linear scan: does this internal page hold a downlink to `child`?
/// (Parentage verification for the guarded split Commit — a binary search
/// cannot answer this, since the child's separator key is not what we
/// know.)
fn internal_page_points_at(page: &[u8; PAGE_SIZE], child: PageId) -> Result<bool> {
    let count = SlottedPage::slot_count(page);
    for slot in 0..count as u16 {
        let (_, c) = page::decode_internal_entry(entry_bytes(page, slot)?)?;
        if c == child {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Binary search for the insertion point of an encoded `entry` on a page
/// whose entries use the same encoding (`leaf` selects the trailer size).
fn entry_lower_bound(page: &[u8; PAGE_SIZE], entry: &[u8], leaf: bool) -> Result<usize> {
    let count = SlottedPage::slot_count(page);
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if entry_cmp(entry_bytes(page, mid as u16)?, entry, leaf)? == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Usable data area of a fresh B+Tree page (`pd_lower..pd_upper`): page
/// size minus the header and the 16-byte special space.
const BTREE_PAGE_CAPACITY: usize =
    PAGE_SIZE - pg_storage::page::PAGE_HEADER_SIZE - page::BTREE_SPECIAL_SIZE;

/// Choose the split point (first slot moving right) for a page split
/// (Stage Q review H3).
///
/// With `pending = None` this is the exact Stage M behavior: the median
/// slot. With `Some` the split point additionally accounts for the pending
/// entry's BYTE size and its key-determined landing half, so the receiving
/// half's bytes + pending always fit a fresh page — a count-based median
/// can overload the receiving half when entry sizes are highly skewed
/// (tiny keys on one side, near-limit keys on the other), wedging the
/// insert with `PageFull` AFTER Copy was already WAL-logged.
///
/// A valid split point always exists: entries are capped at roughly 1/3 of
/// a page ([`MAX_INDEX_KEY_BYTES`]), so if the pending entry does not fit
/// alongside its left predecessors (`left_bytes(p) + pending > CAPACITY`),
/// the right suffix is smaller than `pending` and moving the split point
/// one slot left puts pending + at most one entry + the sub-`pending`
/// suffix on the right — under capacity. The scan below finds the valid
/// point closest to the median for balance; the fallback error is
/// defensive and unreachable under the key bound.
///
/// `copy_start_slot` is stored in [`SplitState`] and the Copy WAL record,
/// so redo recomputes the moved entries from the SAME split point — no
/// protocol change.
fn choose_split_slot(page: &[u8; PAGE_SIZE], level: u8, pending: Option<&[u8]>) -> Result<u16> {
    let count = SlottedPage::slot_count(page);
    if count < 2 {
        return Err(BTreeError::Corrupted(format!(
            "cannot split page with {count} entries"
        )));
    }
    let Some(pending) = pending else {
        return Ok((count / 2) as u16);
    };
    let leaf = level == 0;
    // Cost model: entry bytes + one line pointer (matches insert_entry_at).
    let mut costs = Vec::with_capacity(count);
    for slot in 0..count as u16 {
        costs.push(entry_bytes(page, slot)?.len() + 4);
    }
    let total: usize = costs.iter().sum();
    let left_bytes = |s: usize| costs[..s].iter().sum::<usize>();
    let pending_cost = pending.len() + 4;
    // p = pending's insertion position: it lands LEFT of the split point s
    // iff s >= p (entry[s] sorts at-or-above pending in full (key, trailer)
    // order — the same rule the side choices use via entry_cmp).
    let p = entry_lower_bound(page, pending, leaf)?;
    let valid = |s: usize| {
        let lands_left = s >= p;
        let right_need = total - left_bytes(s) + if lands_left { 0 } else { pending_cost };
        let left_need = left_bytes(s) + if lands_left { pending_cost } else { 0 };
        right_need <= BTREE_PAGE_CAPACITY && left_need <= BTREE_PAGE_CAPACITY
    };
    // The valid split point closest to the median, for balance.
    let mid = count / 2;
    for d in 0..=count / 2 + 1 {
        for s in [mid + d, mid.saturating_sub(d)] {
            if (1..count).contains(&s) && valid(s) {
                return Ok(s as u16);
            }
        }
    }
    Err(BTreeError::PageFull {
        needed: pending_cost,
        available: 0,
    })
}

/// Compare two encoded entries in full `(key, trailer)` order.
fn entry_cmp(a: &[u8], b: &[u8], leaf: bool) -> Result<Ordering> {
    if leaf {
        let (ka, ta) = page::decode_leaf_entry(a)?;
        let (kb, tb) = page::decode_leaf_entry(b)?;
        Ok(ka.cmp(kb).then(ta.cmp(&tb)))
    } else {
        let (ka, ca) = page::decode_internal_entry(a)?;
        let (kb, cb) = page::decode_internal_entry(b)?;
        Ok(ka.cmp(kb).then(ca.cmp(&cb)))
    }
}

/// Extract the key of an encoded entry (trailer size selected by `level`).
fn entry_key(bytes: &[u8], level: u8) -> Result<&[u8]> {
    if level == 0 {
        Ok(page::decode_leaf_entry(bytes)?.0)
    } else {
        Ok(page::decode_internal_entry(bytes)?.0)
    }
}

/// The first entry's key on a page, or `None` for an empty page.
fn first_entry_key(page: &[u8; PAGE_SIZE]) -> Result<Option<Vec<u8>>> {
    if SlottedPage::slot_count(page) == 0 {
        return Ok(None);
    }
    let level = BtreePage::level(page)?;
    Ok(Some(entry_key(entry_bytes(page, 0)?, level)?.to_vec()))
}

/// Descent rule on an internal page: the last entry with `key <= probe`,
/// or entry 0 when the probe is smaller than every key (the leftmost child
/// covers `-infinity`).
fn find_child(page: &[u8; PAGE_SIZE], key: &[u8]) -> Result<PageId> {
    let count = SlottedPage::slot_count(page);
    if count == 0 {
        return Err(BTreeError::Corrupted(
            "internal page with no entries".to_string(),
        ));
    }
    // First slot with entry.key > probe; the child is the slot before it.
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (k, _) = page::decode_internal_entry(entry_bytes(page, mid as u16)?)?;
        if k <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let slot = lo.saturating_sub(1) as u16;
    let (_, child) = page::decode_internal_entry(entry_bytes(page, slot)?)?;
    Ok(child)
}

/// Read the raw bytes of the entry at `slot` (geometry-checked).
fn entry_bytes(page: &[u8; PAGE_SIZE], slot: u16) -> Result<&[u8]> {
    SlottedPage::tuple(page, slot)?
        .ok_or_else(|| BTreeError::Corrupted(format!("slot {slot} does not hold a live entry")))
}

/// Reinterpret a read guard's page bytes as a fixed-size page array.
fn as_page<'g>(guard: &'g PageGuard<'_>) -> &'g [u8; PAGE_SIZE] {
    guard.page().try_into().expect("frame is PAGE_SIZE")
}

/// Read the current `(root_page, tree_level)` from the meta page's last
/// record (shared by `open`, `refresh_root_from_meta`, and `validate`).
fn root_from_meta(pool: &BufferPool, meta_page: PageId) -> Result<(PageId, u8)> {
    let guard = pool.pin(meta_page)?;
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
    let slot_count = SlottedPage::slot_count(page);
    if slot_count == 0 {
        // Not necessarily corruption: a bulk load publishes the meta record
        // LAST, so an index whose build crashed mid-way (or was never
        // created) looks exactly like this (F6 — the two cases are
        // indistinguishable from the meta page alone).
        return Err(BTreeError::Corrupted(format!(
            "meta page {meta_page} holds no root record (index never created, \
             or a bulk load crashed before publishing the root)"
        )));
    }
    let bytes = SlottedPage::tuple(page, (slot_count - 1) as u16)?
        .ok_or_else(|| BTreeError::Corrupted(format!("meta page {meta_page} slot unreadable")))?;
    let (root_page, tree_level) = page::decode_meta_record(bytes)?;
    if tree_level > 0x0F {
        return Err(BTreeError::Corrupted(format!(
            "meta page {meta_page} records tree level {tree_level}"
        )));
    }
    Ok((root_page, tree_level as u8))
}

/// Reinterpret a write guard's page bytes as a fixed-size page array.
fn as_page_mut<'g>(guard: &'g mut PageGuardMut<'_>) -> &'g mut [u8; PAGE_SIZE] {
    guard
        .page_mut()
        .try_into()
        .expect("buffer frame is exactly PAGE_SIZE")
}

/// Append a post-image `FullPageImage` of a freshly initialized page and
/// stamp its `pd_lsn` — the durability anchor for page initialization (same
/// pattern as the heap's `log_page_init`).
pub(crate) fn log_page_init(
    wal_writer: &WalWriter,
    page_id: PageId,
    page: &mut [u8; PAGE_SIZE],
) -> Result<()> {
    let image = page.to_vec();
    let lsn = wal_writer.append(WalRecord::full_page_image(page_id, image)?)?;
    stamp_pd_lsn(page, lsn);
    Ok(())
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}

/// The split an undo pass has to finish.
pub(crate) struct SplitToFinish {
    pub left_page: PageId,
    pub right_page: PageId,
    pub level: u8,
    /// First slot of the left page that belongs to the right half.
    pub copy_start_slot: u16,
    /// LSN of the Prepare record, carried into the CLR for diagnostics.
    pub prepare_lsn: Lsn,
}

/// How an incomplete split is finished, decided from the **current page
/// states** — never from the left page's slot count alone (post-Stage-S
/// review C1). The Copy→Commit window is observable online (the leaf latch
/// is dropped before the Commit walks the path), so by crash time the WAL
/// may carry, after the Copy record, the pending `BTreeInsert` (≈50% of
/// online splits land it in the LEFT half) and/or `BTreeDelete`s from
/// concurrent transactions (the delete path right-hops onto the uncommitted
/// twin and never checks `SPLIT_INCOMPLETE`). After redo converges, the
/// right page's content — not the left page's slot count — says which state
/// the split is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SplitFinishPlan {
    /// The right page never held entries (`pd_upper == pd_special`): Copy
    /// was never applied. Move `left[copy_start_slot..]` right, exactly like
    /// the online Copy. (Deletes cannot precede Copy online — one latch
    /// spans Prepare→Copy — so the left page still holds the moved set.)
    Move,
    /// The right page holds entries: Copy was applied (and redone) before
    /// the crash. Never move again — appending `left[copy_start_slot..]` to
    /// the right page's END would corrupt its sort order when a pending
    /// insert landed left, and resurrect entries deleted from the right
    /// page in-window. The separator is the right page's first entry key,
    /// the same key the online Commit computes (`first_entry_bytes(right)`).
    NoMove,
    /// The right page held entries but is empty now (tuple bytes carved,
    /// then all removed by in-window deletes). There is nothing to move and
    /// no first entry to anchor a separator on, so completing the split is
    /// impossible; instead the split is *abandoned*: splice the empty right
    /// page out of the sibling chain (`left.next = right.next`) and clear
    /// `SPLIT_INCOMPLETE`, keeping the left page (and its ROOT flag, for a
    /// root split) intact. The orphan page leaks — reclaimed by no one,
    /// corrupting nothing. No downlink is inserted; the CLR carries
    /// `INVALID` for parent/new_root/meta, which `apply_split_clr` reads as
    /// the unlink plan.
    Unlink,
}

/// True if the page never held a tuple since its (re-)initialization:
/// `pd_upper` still equals `pd_special` — no tuple bytes were ever carved
/// out of the data area. A page that held entries and lost them all to
/// physical deletes keeps `pd_upper < pd_special` (deletes shrink only the
/// LP array), which is how the undo path tells "Copy never ran" apart from
/// "Copy ran but the whole right half was deleted in-window" without needing
/// any LSN bookkeeping.
fn page_never_had_entries(page: &[u8; PAGE_SIZE]) -> bool {
    let header = SlottedPage::header(page);
    header.pd_upper == header.pd_special
}

/// A `BTreeSplitCLR` with no downlink target at all is the unlink plan (see
/// [`SplitFinishPlan::Unlink`]). Every other CLR has either a parent page
/// (non-root split) or a new root + meta page (root split).
fn clr_is_unlink(rec: &BTreeSplitCLRRecord) -> bool {
    rec.parent_page == PageId::INVALID
        && rec.new_root_page == PageId::INVALID
        && rec.meta_page == PageId::INVALID
}

/// Finish an incomplete B+Tree split during ARIES undo (Stage S, §11.3).
///
/// Called for each split that reached Prepare (and optionally Copy) but never
/// Commit. Applies the copy, installs the downlink, clears `SPLIT_INCOMPLETE`,
/// updates the meta page for root splits, and emits a `BTreeSplitCLR` so the
/// result survives a crash during or after undo.
///
/// If the parent has no room for the downlink, the parent is split first —
/// recursively up to and including the root — via [`split_page_in_undo`]
/// (post-Stage-S review C2: recovery is single-threaded, so each cascade
/// level is completed directly by its own CLR; every cascade prefix
/// re-converges on the next recovery).
pub(crate) fn finish_incomplete_split(
    pool: &BufferPool,
    wal_writer: &WalWriter,
    page_allocator: &Mutex<PageAllocator>,
    split: &SplitToFinish,
) -> Result<()> {
    let &SplitToFinish {
        left_page: left_page_id,
        right_page: right_page_id,
        level,
        copy_start_slot,
        prepare_lsn,
    } = split;
    // Phase 1: decide the finish plan and gather everything the CLR payload
    // needs without touching a single page byte. Logging before applying is
    // what makes a crash inside undo recoverable: a half-applied split with
    // no CLR in the WAL would be finished a second time by the next undo
    // pass.
    let (plan, separator_key, is_root_split) = {
        let left_guard = pool.pin(left_page_id)?;
        let left: &[u8; PAGE_SIZE] = left_guard.page().try_into().expect("frame is PAGE_SIZE");
        let right_guard = pool.pin(right_page_id)?;
        let right: &[u8; PAGE_SIZE] = right_guard.page().try_into().expect("frame is PAGE_SIZE");
        let is_root_split = BtreePage::flags(left)? & BTREE_FLAG_ROOT != 0;
        let (plan, key) = if SlottedPage::slot_count(right) > 0 {
            // Copy already applied: the separator is the right page's first
            // entry key (same as the online Commit). Note the left page may
            // hold MORE than `copy_start_slot` entries here (a pending
            // insert that landed left) or FEWER (in-window deletes) — both
            // are fine, the apply path never counts slots on the left page.
            let key = entry_key(entry_bytes(right, 0)?, level)?.to_vec();
            (SplitFinishPlan::NoMove, key)
        } else if page_never_had_entries(right) {
            // Copy never ran: the entries to move are still on the left
            // page, and the separator is the first of them.
            let count = SlottedPage::slot_count(left) as u16;
            if copy_start_slot >= count {
                return Err(BTreeError::Corrupted(format!(
                    "incomplete split of page {left_page_id}: copy_start_slot \
                     {copy_start_slot} outside slot count {count} with a \
                     never-written right page"
                )));
            }
            let key = entry_key(entry_bytes(left, copy_start_slot)?, level)?.to_vec();
            (SplitFinishPlan::Move, key)
        } else {
            // Right page held entries and is empty now: the whole right half
            // was deleted in the Copy→Commit window. Abandon the split.
            (SplitFinishPlan::Unlink, Vec::new())
        };
        (plan, key, is_root_split)
    };

    let (parent_page, new_root_page, meta_page, parent_insert_slot) = match plan {
        SplitFinishPlan::Unlink => (PageId::INVALID, PageId::INVALID, PageId::INVALID, 0),
        _ if is_root_split => {
            // 4-bit level bound before allocating the new root (post-Stage-S
            // C2 deep review; see ensure_root_promotion_fits).
            ensure_root_promotion_fits(level)?;
            // Reserve the new root page id; its content is written by the
            // apply. `new_page` logs its own PageAlloc, so recovery
            // re-reserves the id.
            let new_root_id = pool.new_page()?.page_id();
            let mp = find_meta_page_for_root(pool, page_allocator, left_page_id)?;
            (PageId::INVALID, new_root_id, mp, 1u16)
        }
        _ => {
            let parent = find_parent_page(pool, page_allocator, left_page_id, level)?;
            // C2: the parent may not have room for the downlink (the online
            // Commit would have cascaded, but the crash happened first).
            // Split the parent — recursively — before inserting.
            let (target, slot) = ensure_downlink_slot(
                pool,
                wal_writer,
                page_allocator,
                parent,
                level + 1,
                &separator_key,
                right_page_id,
            )?;
            (target, PageId::INVALID, PageId::INVALID, slot)
        }
    };

    let rec = BTreeSplitCLRRecord {
        left_page: left_page_id,
        right_page: right_page_id,
        level,
        copy_start_slot,
        redo_ref_lsn: prepare_lsn,
        parent_page,
        separator_key,
        parent_insert_slot,
        new_root_page,
        meta_page,
    };

    emit_and_apply_clr(pool, wal_writer, &rec)
}

/// The outcome of splitting a page during undo (C2 cascade): everything the
/// caller needs to place the downlink that triggered the cascade.
struct UndoSplitOutcome {
    /// The freshly created right half.
    right_page: PageId,
    /// Raw bytes of the right page's first entry; the landing-side choice
    /// compares the pending downlink against it in full `(key, child)`
    /// order — the same rule the online cascade uses.
    right_first: Vec<u8>,
}

/// Resolve where the downlink `(separator_key, child)` must go during undo:
/// `parent` itself when it has room, otherwise split `parent` first —
/// recursively up to and including the root (C2) — and pick the half the
/// downlink sorts into, in full `(key, child)` order like the online
/// cascade. Returns `(page, insertion_slot)`.
fn ensure_downlink_slot(
    pool: &BufferPool,
    wal_writer: &WalWriter,
    page_allocator: &Mutex<PageAllocator>,
    parent: PageId,
    parent_level: u8,
    separator_key: &[u8],
    child: PageId,
) -> Result<(PageId, u16)> {
    let downlink = page::encode_internal_entry(separator_key, child);
    let needed = downlink.len() + 4; // entry + one line pointer
    let fits = {
        let guard = pool.pin(parent)?;
        SlottedPage::free_space(as_page(&guard)) >= needed
    };
    let target = if fits {
        parent
    } else {
        let outcome = split_page_in_undo(
            pool,
            wal_writer,
            page_allocator,
            parent,
            parent_level,
            Some(&downlink),
        )?;
        // Crash-injection hook (`test-hooks` only): simulate a crash right
        // after a cascade level completed — its CLR is durable, the split
        // that triggered the cascade is not finished yet.
        #[cfg(feature = "test-hooks")]
        undo_cascade_failure_hook()?;
        if entry_cmp(&downlink, &outcome.right_first, false)? != Ordering::Less {
            outcome.right_page
        } else {
            parent
        }
    };
    let slot = {
        let guard = pool.pin(target)?;
        internal_lower_bound(as_page(&guard), separator_key, child)? as u16
    };
    Ok((target, slot))
}

/// Hard bound for root promotion during undo (post-Stage-S C2 deep review):
/// the same 4-bit `btpo_level` contract the online path enforces in
/// [`BTreeIndex::create_new_root`]. Promoting a level-15 root would write
/// `16 << 8` into `pd_flags` — overflowing `btpo_level` (bits 8..11) into
/// the `btpo_flags` nibble (bits 12..15): a flag bit is spuriously set while
/// `btpo_level` reads back 0, recovery "succeeds", and the next open reads a
/// corrupt root. Fail loudly instead; a 15-level tree of 8 KiB pages is
/// unreachable in practice, so refusing is always right.
fn ensure_root_promotion_fits(level: u8) -> Result<()> {
    if level >= 0x0F {
        return Err(BTreeError::Corrupted(format!(
            "tree level {level} already at the 4-bit maximum; cannot promote the root"
        )));
    }
    Ok(())
}

/// Complete a FRESH split of `left_page` during undo (C2 cascade): the
/// downlink insertion of another split's finish found `left_page` without
/// room. Unlike [`finish_incomplete_split`] there is no Prepare/Copy in the
/// WAL for this split — the entire split (right-page creation and sibling
/// link, entry move, downlink, root promotion) is captured by one
/// `BTreeSplitCLR`; the apply path establishes the sibling links itself, so
/// the record alone suffices to redo it.
///
/// Crash safety at every intermediate state: the right page's
/// initialization is FPI-logged before the CLR (the allocator may hand a
/// recycled page whose on-disk bytes are a previous tenant's — the same
/// hazard `create_new_root` guards against), the CLR is WAL-flushed before
/// any page it touches, and each completed cascade level is fully flushed
/// before the next starts. A crash mid-cascade replays the completed CLRs
/// as pd_lsn-guarded no-ops and re-runs the original incomplete split's
/// finish, which re-derives its downlink target from the post-cascade
/// pages.
fn split_page_in_undo(
    pool: &BufferPool,
    wal_writer: &WalWriter,
    page_allocator: &Mutex<PageAllocator>,
    left_page: PageId,
    level: u8,
    pending: Option<&[u8]>,
) -> Result<UndoSplitOutcome> {
    let (copy_start_slot, is_root_split, old_next) = {
        let guard = pool.pin(left_page)?;
        let page = as_page(&guard);
        let flags = BtreePage::flags(page)?;
        if flags & BTREE_FLAG_SPLIT_INCOMPLETE != 0 {
            // The undo pass finishes higher-level incomplete splits first,
            // so reaching a flagged page here means detection missed one —
            // splitting it again would orphan its uncommitted right twin
            // (the exact hazard `split_prepare` refuses online). Loud, never
            // silent.
            return Err(BTreeError::Corrupted(format!(
                "undo cascade: page {left_page} is itself SPLIT_INCOMPLETE"
            )));
        }
        (
            choose_split_slot(page, level, pending)?,
            flags & BTREE_FLAG_ROOT != 0,
            BtreePage::next(page)?,
        )
    };

    // Right page: allocate, initialize, and make the initialization durable
    // with a post-image FPI.
    let right_page = {
        let mut guard = pool.new_page()?;
        let page_id = guard.page_id();
        {
            let page = as_page_mut(&mut guard);
            BtreePage::init_right_page(page, left_page, old_next, level);
        }
        log_page_init(wal_writer, page_id, as_page_mut(&mut guard))?;
        page_id
    };

    let right_first = {
        let guard = pool.pin(left_page)?;
        entry_bytes(as_page(&guard), copy_start_slot)?.to_vec()
    };
    let separator_key = entry_key(&right_first, level)?.to_vec();

    let (parent_page, new_root_page, meta_page, parent_insert_slot) = if is_root_split {
        // 4-bit level bound before allocating the new root (post-Stage-S C2
        // deep review; see ensure_root_promotion_fits).
        ensure_root_promotion_fits(level)?;
        // Reserve the new root page id. Its crash safety does NOT rest on
        // "apply_split_clr fully re-initializes it, so no FPI is needed" —
        // that is the A1 fallacy: a torn flush of the post-CLR page could
        // leave a garbage `pd_lsn >= clr_lsn` that the next recovery reads
        // as "already applied", skipping the CLR. What actually makes the
        // new root safe is `emit_and_apply_clr` (below): it appends a
        // pre-image FullPageImage for every page the CLR touches — this one
        // included — BEFORE the CLR; unconditional FPI replay restores the
        // image and patches `pd_lsn` to the FPI's own LSN (still below the
        // CLR's), so the CLR re-applies cleanly on top.
        let new_root_id = pool.new_page()?.page_id();
        let mp = find_meta_page_for_root(pool, page_allocator, left_page)?;
        (PageId::INVALID, new_root_id, mp, 1u16)
    } else {
        let parent = find_parent_page(pool, page_allocator, left_page, level)?;
        let (target, slot) = ensure_downlink_slot(
            pool,
            wal_writer,
            page_allocator,
            parent,
            level + 1,
            &separator_key,
            right_page,
        )?;
        (target, PageId::INVALID, PageId::INVALID, slot)
    };

    let rec = BTreeSplitCLRRecord {
        left_page,
        right_page,
        level,
        copy_start_slot,
        // No Prepare record exists for this split; the field is diagnostic.
        redo_ref_lsn: Lsn::INVALID,
        parent_page,
        separator_key,
        parent_insert_slot,
        new_root_page,
        meta_page,
    };
    emit_and_apply_clr(pool, wal_writer, &rec)?;
    Ok(UndoSplitOutcome {
        right_page,
        right_first,
    })
}

/// Phases 2–4 of finishing a split during undo: log the CLR and flush the
/// WAL (the apply stamps pages with this LSN and `BufferPool::flush_frame`
/// checks `synced_lsn` against it), apply through the exact code path redo
/// takes, then make the result durable — the right page first (inside
/// [`apply_split_clr`]), the left page last so it is never durable ahead of
/// the pages that hold the entries it gave away.
///
/// # Torn-write protection during undo (post-Stage-S review H4)
///
/// The undo phase runs with the buffer pool's own FPI gate CLOSED
/// (`checkpoint_lsn` is seeded only after undo — see `pg-storage` engine
/// open; an earlier seed would make `pin_mut`'s FPI stamp `pd_lsn` past the
/// already-appended CLR and defeat the apply's per-page idempotency
/// guards). A crash tearing the post-apply page flush could therefore leave
/// a garbage `pd_lsn ≥ clr_lsn` that the next recovery would read as
/// "already applied", skipping the CLR — silent corruption. To close that
/// hole, every page the CLR modifies gets an explicit **pre-image**
/// `FullPageImage` record appended BEFORE the CLR: FPI replay restores the
/// image unconditionally (torn writes are exactly what it repairs) and
/// patches `pd_lsn` to the FPI's own LSN — still below the CLR's, so the
/// CLR re-applies cleanly on top. The image is a pure WAL artifact here:
/// the live page's `pd_lsn` is NOT bumped (that is the difference from
/// `pin_mut`'s FPI, and why the guards keep working).
fn emit_and_apply_clr(
    pool: &BufferPool,
    wal_writer: &WalWriter,
    rec: &BTreeSplitCLRRecord,
) -> Result<()> {
    for page_id in [
        rec.left_page,
        rec.right_page,
        rec.parent_page,
        rec.new_root_page,
        rec.meta_page,
    ] {
        if page_id == PageId::INVALID {
            continue;
        }
        let image = {
            let guard = pool.pin(page_id)?;
            guard.page().to_vec()
        };
        wal_writer.append(WalRecord::full_page_image(page_id, image)?)?;
    }

    let clr = WalRecord::btree_split_clr(rec)?;
    let lsn = wal_writer.append(clr)?;
    wal_writer.flush()?;

    apply_split_clr(pool, rec, lsn)?;

    if rec.new_root_page != PageId::INVALID {
        pool.flush(rec.new_root_page)?;
    }
    if rec.parent_page != PageId::INVALID {
        pool.flush(rec.parent_page)?;
    }
    if rec.meta_page != PageId::INVALID {
        pool.flush(rec.meta_page)?;
    }
    pool.flush(rec.left_page)?;
    Ok(())
}

/// Apply the page mutations of a `BTreeSplitCLR` at `lsn`.
///
/// Shared by the undo path (`finish_incomplete_split` / `split_page_in_undo`)
/// and the redo handler, so both converge on byte-identical pages by
/// construction. Every page is guarded by its own `pd_lsn`, which makes
/// repeated application a no-op.
///
/// The left/right pair branches on the RIGHT page's content, never on the
/// left page's slot count (review C1 — the left page may hold a pending
/// in-window insert or have lost entries to in-window deletes):
///
/// - **right non-empty** — Copy was already applied (redone from the WAL, or
///   applied by an earlier partial application of this very CLR): never move
///   entries; only the left page's finish is potentially missing. The left
///   page is truncated at the first entry sorting at-or-above the right
///   page's first entry (full `(key, trailer)` order, so duplicate keys
///   straddling the split point survive) — a no-op in every
///   online-reachable state, kept as the defensive bound.
/// - **right empty, never written** (`pd_upper == pd_special`) — Copy never
///   ran: move `left[copy_start_slot..]` right, like the online Copy.
/// - **right empty, previously written** — only reachable for an unlink CLR
///   (the right half was deleted in-window): splice the right page out of
///   the chain and clear `SPLIT_INCOMPLETE`, preserving `ROOT`.
///
/// The left page's `btpo_next` is (re-)written unconditionally in the
/// non-unlink branches: a no-op for incomplete-split finishes (Prepare
/// linked it), but the write that ESTABLISHES the link for cascade splits
/// created during undo (which have no Prepare record).
pub(crate) fn apply_split_clr(
    pool: &BufferPool,
    rec: &BTreeSplitCLRRecord,
    lsn: Lsn,
) -> Result<()> {
    // 1. Move the entries (when the plan calls for it) and finish the left
    //    page under one pd_lsn stamp, so the left page is never durable in a
    //    half-finished state that a later replay would skip. The RIGHT page
    //    is stamped to `lsn` in EVERY branch (post-Stage-S review H4): the
    //    undo path appends a pre-image FPI of every CLR-touched page before
    //    the CLR, and FPI replay patches pd_lsn to the FPI's own LSN — only
    //    re-stamping the page to the CLR's LSN here makes recovery converge
    //    round-over-round. This does NOT weaken the C1 idempotency logic:
    //    the plan is chosen by the right page's CONTENT (entries present /
    //    never-written), never by comparing right pd_lsn to the CLR's, and
    //    the "move still owed" state (right page empty AND never-written)
    //    remains the only one that is never stamped.
    {
        let mut left_guard = pool.pin_mut(rec.left_page)?;
        let mut right_guard = pool.pin_mut(rec.right_page)?;
        let left_lsn = page_pd_lsn(left_guard.page());
        if left_lsn < lsn {
            let left: &mut [u8; PAGE_SIZE] = as_page_mut(&mut left_guard);
            let right: &mut [u8; PAGE_SIZE] = as_page_mut(&mut right_guard);
            if clr_is_unlink(rec) {
                // Unlink: abandon the split. The right page's content stays
                // untouched (orphaned); only its pd_lsn is stamped (H4, see
                // the fn-level comment). The left page reabsorbs its
                // key range by re-pointing past the orphan. `ROOT` is
                // preserved — this record creates no new root.
                let right_next = BtreePage::next(right)?;
                BtreePage::set_next(left, right_next);
                BtreePage::set_flag(left, BTREE_FLAG_SPLIT_INCOMPLETE, false)?;
                stamp_pd_lsn(right, lsn);
                stamp_pd_lsn(left, lsn);
            } else if SlottedPage::slot_count(right) > 0 {
                // NoMove: the moved entries are already on the right page.
                // Re-appending `left[copy_start_slot..]` here is the C1 bug:
                // with a pending insert in the left half it copies the
                // pending entry onto the right page's END (out of order) and
                // picks the wrong separator.
                let right_first = entry_bytes(right, 0)?.to_vec();
                let left_count = SlottedPage::slot_count(left);
                if left_count > 0 {
                    let truncate_at = entry_lower_bound(left, &right_first, rec.level == 0)?;
                    if truncate_at == 0 {
                        return Err(BTreeError::Corrupted(format!(
                            "split CLR: every entry of left page {} sorts at-or-above the \
                             right page {}'s first entry — the split boundary is inverted",
                            rec.left_page, rec.right_page
                        )));
                    }
                    // Rebuild-only (no move): keeps `[0, truncate_at)`.
                    apply_split_copy(left, right, truncate_at as u16, false)?;
                }
                BtreePage::set_next(left, rec.right_page);
                BtreePage::apply_commit_left(left)?;
                // Content unchanged by this CLR; stamped for H4 convergence
                // (see the fn-level comment).
                stamp_pd_lsn(right, lsn);
                stamp_pd_lsn(left, lsn);
            } else {
                // Right page empty: the move is still owed. It is only safe
                // if the right page never held entries — otherwise the
                // entries were deleted in-window and moving would resurrect
                // them. That state is exactly what unlink CLRs cover, so a
                // non-unlink record meeting a previously-written empty right
                // page is corruption.
                if !page_never_had_entries(right) {
                    return Err(BTreeError::Corrupted(format!(
                        "split CLR: right page {} held entries and is empty now, but the \
                         record is not an unlink — moving would resurrect deleted entries",
                        rec.right_page
                    )));
                }
                apply_split_copy(left, right, rec.copy_start_slot, true)?;
                stamp_pd_lsn(right, lsn);
                BtreePage::set_next(left, rec.right_page);
                BtreePage::apply_commit_left(left)?;
                stamp_pd_lsn(left, lsn);
            }
        } else {
            // The left page is past the CLR. Guard the one state flush
            // ordering makes unreachable: a record that still owes the move
            // (right page never written) while the left page is already
            // finished — the moved entries would be durable nowhere. (A
            // non-empty or previously-written right page is legitimate: the
            // NoMove and unlink plans never MOVE entries, though they do
            // stamp the right page's pd_lsn — H4, see above.)
            let right: &[u8; PAGE_SIZE] =
                right_guard.page().try_into().expect("frame is PAGE_SIZE");
            if !clr_is_unlink(rec)
                && SlottedPage::slot_count(right) == 0
                && page_never_had_entries(right)
            {
                return Err(BTreeError::Corrupted(format!(
                    "split CLR: left page {} is past the CLR (pd_lsn {:?} >= {:?}) but right \
                     page {} never received the moved entries",
                    rec.left_page, left_lsn, lsn, rec.right_page
                )));
            }
        }
    }
    // The right page must reach disk before the left page: the left page has
    // given its entries away, so a durable left with a stale right loses
    // them. The flush is load-bearing in EVERY plan, never a no-op
    // (post-Stage-S review H4): the NoMove/unlink branches do not move
    // entries but DO stamp the right page's pd_lsn, and only flushing that
    // stamp makes recovery converge round-over-round (see the fn-level
    // comment).
    pool.flush(rec.right_page)?;

    // 2. Downlink: a root split creates a new root, a non-root split inserts
    //    into the existing parent.
    if rec.new_root_page != PageId::INVALID {
        let mut guard = pool.pin_mut(rec.new_root_page)?;
        let page: &mut [u8; PAGE_SIZE] = as_page_mut(&mut guard);
        if page_pd_lsn(page) < lsn {
            BtreePage::init(page, rec.level + 1, BTREE_FLAG_ROOT);
            let e0 = page::encode_internal_entry(&[], rec.left_page);
            SlottedPage::add_tuple(page, &e0).map_err(BTreeError::Heap)?;
            let e1 = page::encode_internal_entry(&rec.separator_key, rec.right_page);
            SlottedPage::add_tuple(page, &e1).map_err(BTreeError::Heap)?;
            stamp_pd_lsn(page, lsn);
        }
    } else if rec.parent_page != PageId::INVALID {
        let mut guard = pool.pin_mut(rec.parent_page)?;
        let page: &mut [u8; PAGE_SIZE] = as_page_mut(&mut guard);
        if page_pd_lsn(page) < lsn {
            let entry = page::encode_internal_entry(&rec.separator_key, rec.right_page);
            BtreePage::insert_entry_at(page, rec.parent_insert_slot, &entry)?;
            stamp_pd_lsn(page, lsn);
        }
    }

    // 3. Publish the new root through the meta page (root splits only).
    if rec.meta_page != PageId::INVALID {
        let mut guard = pool.pin_mut(rec.meta_page)?;
        let page: &mut [u8; PAGE_SIZE] = as_page_mut(&mut guard);
        if page_pd_lsn(page) < lsn {
            let slot = SlottedPage::slot_count(page) as u16;
            let meta_bytes = page::encode_meta_record(rec.new_root_page, (rec.level + 1) as u16);
            BtreePage::insert_entry_at(page, slot, &meta_bytes)?;
            stamp_pd_lsn(page, lsn);
        }
    }

    Ok(())
}

/// Scan allocated pages for the meta page whose root record points at
/// `root_page`. The meta page has `flags == 0` (no LEAF, no ROOT) and its
/// last slot decodes as a `(root_page, tree_level)` meta record.
fn find_meta_page_for_root(
    pool: &BufferPool,
    page_allocator: &Mutex<PageAllocator>,
    root_page: PageId,
) -> Result<PageId> {
    let max_pid = page_allocator.lock().next_page_id().0;
    for pid in 1..max_pid {
        let page_id = PageId(pid);
        let guard = match pool.pin(page_id) {
            Ok(g) => g,
            Err(_) => continue,
        };
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        // Skip non-BTree pages and pages with any tree flags set.
        let flags = match BtreePage::flags(page) {
            Ok(f) => f,
            Err(_) => continue,
        };
        if flags != 0 {
            continue;
        }
        let count = SlottedPage::slot_count(page);
        if count == 0 {
            continue;
        }
        if let Ok(bytes) = entry_bytes(page, (count - 1) as u16) {
            if let Ok((rp, _)) = page::decode_meta_record(bytes) {
                if rp == root_page {
                    return Ok(page_id);
                }
            }
        }
    }
    Err(BTreeError::Corrupted(format!(
        "no meta page found for root {root_page}"
    )))
}

/// Scan allocated pages for the parent of `child` at `level + 1`. The parent
/// is an internal page whose downlink set includes `child`.
///
/// Cost: O(allocated_pages) per call, so N incomplete splits cost
/// O(N × allocated_pages) — fine for the typical crash (N = 1–3, and the
/// 4-bit level bound caps the cascade N); a descending-from-root lookup is
/// possible but the tree above an incomplete split is exactly where trust
/// is thinnest during recovery, so the dumb scan is the robust choice.
fn find_parent_page(
    pool: &BufferPool,
    page_allocator: &Mutex<PageAllocator>,
    child: PageId,
    level: u8,
) -> Result<PageId> {
    let max_pid = page_allocator.lock().next_page_id().0;
    let target_level = level + 1;
    for pid in 1..max_pid {
        let page_id = PageId(pid);
        let guard = match pool.pin(page_id) {
            Ok(g) => g,
            Err(_) => continue,
        };
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        // Skip pages that aren't internal pages at the target level.
        let flags = match BtreePage::flags(page) {
            Ok(f) => f,
            Err(_) => continue,
        };
        if flags & BTREE_FLAG_LEAF != 0 {
            continue; // Leaf pages can't be parents.
        }
        let pl = match BtreePage::level(page) {
            Ok(l) => l,
            Err(_) => continue,
        };
        if pl != target_level {
            continue;
        }
        if internal_page_points_at(page, child)? {
            return Ok(page_id);
        }
    }
    Err(BTreeError::Corrupted(format!(
        "no parent page found for child {child} at level {level}"
    )))
}

/// Compute the split slot for a page whose Copy phase was never reached
/// (only Prepare). Uses the midpoint heuristic (no pending entry).
pub(crate) fn choose_split_slot_readonly(
    pool: &BufferPool,
    page_id: PageId,
    level: u8,
) -> Result<u16> {
    let guard = pool.pin(page_id)?;
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
    choose_split_slot(page, level, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Post-Stage-S C2 deep review: the undo root-promotion guard fails
    /// loudly at the 4-bit `btpo_level` bound instead of letting level 16
    /// overflow into the flags nibble (a 15-level tree is not constructible
    /// in a test, so the guard logic itself is pinned here).
    #[test]
    fn undo_root_promotion_refuses_level_overflow() {
        assert!(ensure_root_promotion_fits(0).is_ok());
        assert!(ensure_root_promotion_fits(0x0E).is_ok());
        let err = ensure_root_promotion_fits(0x0F).unwrap_err();
        assert!(
            matches!(err, BTreeError::Corrupted(_)),
            "level 15 must fail loudly, got {err:?}"
        );
    }
}
