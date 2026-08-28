//! Access method traits and operation contexts (tech-selection §14).
//!
//! Moved here from `pg-catalog` in Stage I so that [`crate::heap_am::HeapAM`]
//! and its trait impls live in the same crate as the tuple / slotted-page
//! primitives they build on. `pg-catalog` re-exports these traits unchanged to
//! keep its public API stable.
//!
//! The contexts group the per-operation inputs an access method needs. Because
//! `pg-am-heap` cannot depend on `pg-catalog` (that would be a cycle), a
//! relation's physical location and column schema are supplied explicitly via
//! [`RelationDesc`] rather than resolved from the catalog.

use pg_storage::clog::ClogAccessor;
use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Lsn, Oid, PageId, Tid, TxnId};
use pg_txn::Snapshot;

use crate::tuple::{ColumnType, Datum};
use crate::Result;

/// Physical + schema description of a relation, resolved by the caller.
///
/// `first_page` is the head of the relation's on-disk page chain (Stage K):
/// the AM rebuilds its in-memory page list by walking the chain from here
/// (see [`crate::heap_am`]), so no page count is needed or trusted.
/// `columns` drives tuple decode.
#[derive(Debug, Clone, Copy)]
pub struct RelationDesc<'a> {
    /// The relation's OID (identity / logging).
    pub rel_oid: Oid,
    /// The relation's first heap page in the shared data file (chain head).
    pub first_page: PageId,
    /// Column schema, in `attnum` order, for tuple encode/decode.
    pub columns: &'a [ColumnType],
}

/// Inputs to [`AccessMethod::insert`].
pub struct InsertContext<'a> {
    /// The target relation.
    pub rel: RelationDesc<'a>,
    /// Snapshot of the inserting transaction (supplies `current_xid`).
    pub snapshot: &'a Snapshot,
    /// Pre-encoded tuple bytes (header + null bitmap + attributes).
    pub tuple: &'a [u8],
    /// Filled with the new tuple's TID on success (§14 P0-2).
    pub out_tid: Option<&'a mut Tid>,
}

/// Inputs to [`AccessMethod::scan`].
pub struct ScanContext<'a> {
    /// The relation to scan.
    pub rel: RelationDesc<'a>,
    /// Snapshot controlling tuple visibility.
    pub snapshot: &'a Snapshot,
    /// Commit-status oracle. A tuple whose `t_xmin` aborted (or is otherwise
    /// not committed) must not be yielded; the real CLOG makes that decision
    /// observable. M1 callers may pass `&NoOpClogAccessor` (every XID reads as
    /// committed) to preserve legacy behavior.
    pub clog: &'a dyn ClogAccessor,
}

/// Inputs to [`UpdatableAM::update`].
pub struct UpdateContext<'a> {
    /// The target relation.
    pub rel: RelationDesc<'a>,
    /// Snapshot of the updating transaction (supplies `current_xid`).
    pub snapshot: &'a Snapshot,
    /// TID of the row version being replaced.
    pub old_tid: Tid,
    /// Pre-encoded bytes of the new row version.
    pub new_tuple: &'a [u8],
    /// Filled with the new version's TID on success.
    pub out_tid: Option<&'a mut Tid>,
    /// Commit-status oracle used by the liveness check: a tuple whose
    /// `t_xmax` is set is only dead if the deleter COMMITTED. An aborted
    /// deleter means the delete never took effect, so the tuple may be
    /// updated again (without this, such a tuple would be visible yet
    /// permanently unmodifiable).
    pub clog: &'a dyn ClogAccessor,
    /// Whether the update is HOT-eligible: the caller asserts that no
    /// indexed column changed. Whether the new version FITS on the old page
    /// is decided by the AM, not the caller (post-Stage-S review B7): when
    /// it does, the AM appends the new version same-page, chains it via
    /// `t_ctid` + `HEAP_HOT_UPDATED`, and skips index maintenance (Stage S);
    /// when it does not, the AM silently falls back to the cross-page
    /// non-HOT path and the caller's index maintenance proceeds — compare
    /// `out_tid.page_id` against `old_tid.page_id` to tell which happened
    /// (see `Engine::update_inner`'s `hot_applied`).
    pub hot_eligible: bool,
}

/// Inputs to [`AccessMethod::delete`].
pub struct DeleteContext<'a> {
    /// The target relation.
    pub rel: RelationDesc<'a>,
    /// Snapshot of the deleting transaction (supplies `current_xid`).
    pub snapshot: &'a Snapshot,
    /// TID of the row to delete.
    pub tid: Tid,
    /// Commit-status oracle for the liveness check (see
    /// [`UpdateContext::clog`]).
    pub clog: &'a dyn ClogAccessor,
}

/// Inputs to [`AccessMethod::build`] (M2a placeholder).
pub struct BuildContext<'a> {
    /// The relation being built.
    pub rel: RelationDesc<'a>,
}

/// Base trait for all access methods (heap, B+Tree, future HNSW/Inverted).
///
/// Stage A defined only the identity method; Stage I adds the CRUD surface and
/// the redo-handler hook.
pub trait AccessMethod: Send + Sync {
    /// AM name, corresponds to `pg_am.amname`.
    fn name(&self) -> &'static str;

    /// Build/initialize storage for a new relation (M2a: no-op default).
    fn build(&self, _ctx: BuildContext<'_>) -> Result<()> {
        Ok(())
    }

    /// Insert a tuple, filling `ctx.out_tid` with its TID.
    fn insert(&self, ctx: InsertContext<'_>) -> Result<()>;

    /// Return every visible tuple as `(tid, decoded columns)`.
    ///
    /// M2a materializes into a `Vec` to avoid iterator/lifetime plumbing; a
    /// streaming scan is future work.
    fn scan(&self, ctx: ScanContext<'_>) -> Result<Vec<(Tid, Vec<Option<Datum>>)>>;

    /// Delete the tuple at `ctx.tid` (logical delete: sets `t_xmax`).
    fn delete(&self, ctx: DeleteContext<'_>) -> Result<()>;

    /// Redo handlers this AM contributes to the recovery registry.
    ///
    /// Returned to an upper layer for registration because `pg-storage` (which
    /// owns the registry) cannot depend on this crate.
    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>>;

    /// M3 Stage G reservation (tech-selection §9): freshness watermark of
    /// this AM's contents — the WAL LSN up to which the AM reflects the base
    /// table. The planner/executor will use it to decide "index scan vs.
    /// full-table fallback" for asynchronously-maintained Tier 2 indexes.
    ///
    /// **Default `None` = "no freshness tracking"**, which is also the
    /// correct answer for every synchronous AM (heap, B+Tree): they are
    /// maintained in-transaction, so they are always fresh and never need a
    /// planner fallback decision. The default means this reservation changes
    /// NOTHING about the existing AMs. Tier 2 implementations are expected
    /// to answer from a `pg_storage::tier2::WatermarkRegistry`.
    fn freshness(&self) -> Option<Lsn> {
        None
    }
}

/// AMs that support tuple updates.
///
/// In M2 only the heap AM implements this. Index AMs (B+Tree) do not — index
/// updates are modeled as delete + insert.
pub trait UpdatableAM: AccessMethod {
    /// Update the tuple at `ctx.old_tid`, producing a new version.
    fn update(&self, ctx: UpdateContext<'_>) -> Result<()>;
}

/// AMs that support vacuum / garbage collection.
///
/// `scan_dead_tuples` is implemented by heap since Stage I; M3 Stage C adds
/// `collect_index_keys` (read-only) and `reclaim` (purely physical)
/// (tech-selection §4.4). The two are deliberately SEPARATE methods — never
/// merge them: the §4.1 ordering invariant requires index cleanup (driven by
/// the caller between the two calls) to be WAL-durable BEFORE any TID-
/// invalidating heap record (`HeapCleanup` / `PageFree`) is appended. A
/// single fused method would force compaction ahead of index cleanup and
/// reopen the dangling-TID window.
///
/// Index cleanup itself is NOT part of this trait (`notify_indexes` stays
/// out, §4.4): index knowledge lives at the engine layer.
///
/// TODO(M3): When autovacuum is introduced, consider changing the return type
/// from `Vec<Tid>` to an iterator or callback pattern to avoid materializing
/// all dead tuples on the heap for large tables.
pub trait Vacuumable {
    /// Scan `rel` for dead tuples whose `xmax` is committed and older than
    /// `oldest_xmin`. Scoped to a single relation so callers (e.g. an M3
    /// autovacuum worker) need not filter results by OID.
    ///
    /// `clog` decides whether a deleter committed: a tuple deleted by an
    /// aborted transaction is NOT dead (the delete never took effect), so the
    /// committed check is authoritative rather than assumed.
    fn scan_dead_tuples(
        &self,
        rel: RelationDesc<'_>,
        oldest_xmin: TxnId,
        clog: &dyn ClogAccessor,
    ) -> Result<Vec<Tid>>;

    /// M3 Stage C (READ-ONLY): derive the `(tid, column values)` pairs whose
    /// index entries need cleanup, from the dead-tuple list produced by
    /// [`Vacuumable::scan_dead_tuples`]. A standalone dead tuple maps to
    /// itself; a FULLY-dead HOT chain (every member in `dead`) maps to its
    /// chain ROOT (root tid + the root's decoded column values — the root
    /// owns the chain's index entries, Stage S); a PARTIALLY-dead chain
    /// contributes NOTHING (§4.2: no prune, no redirect — LP redirection is
    /// an on-disk format change, out of scope).
    ///
    /// The returned values are the tuple's full decoded row
    /// (`Vec<Option<Datum>>`, `rel.columns` order); the caller picks out its
    /// indexed columns and skips NULL keys (the `Engine::delete_inner`
    /// convention).
    ///
    /// MUST run before [`Vacuumable::reclaim`]: once a page is compacted the
    /// tuple bytes are gone and the keys are unreadable (§4.1 stage 2). This
    /// method never modifies any page.
    fn collect_index_keys(
        &self,
        rel: RelationDesc<'_>,
        dead: &[Tid],
    ) -> Result<Vec<(Tid, Vec<Option<Datum>>)>>;

    /// M3 Stage C (PURELY PHYSICAL): kill the listed dead slots via
    /// [`crate::slotted_page::SlottedPage::compact`] under `HeapCleanup` WAL
    /// records, unlink pages that become fully empty from the relation's page
    /// chain (the unlink rides in the same `HeapCleanup` payload), and return
    /// them to the allocator via `PageAllocator::free_page` (`PageFree` WAL).
    ///
    /// Kill-list contract (inherited from `compact()`): only standalone dead
    /// tuples and FULLY-dead HOT chains may be killed — never a chain member
    /// still referenced by a predecessor's `t_ctid`, never a chain root while
    /// any member lives. The implementation re-derives that classification
    /// from `dead` itself (same grouping helper as `collect_index_keys`), so
    /// passing the raw `scan_dead_tuples` output is safe: partially-dead
    /// chain members in the input are left untouched (§4.2).
    ///
    /// Call preconditions (§4.1 ordering invariant):
    ///
    /// 1. ORDERING: the index cleanup for every entry `collect_index_keys`
    ///    returned must already be WAL-durable — vacuum is not transactional,
    ///    and every record written here carries `txn_id = INVALID`, so replay
    ///    is unconditional.
    /// 2. MUTUAL EXCLUSION: no concurrent writer may touch the relation
    ///    between `scan_dead_tuples`, `collect_index_keys`, and `reclaim` —
    ///    the kill list `reclaim` re-derives must still match the tuples
    ///    `collect_index_keys` decoded, and a concurrent insert/update would
    ///    invalidate that correspondence (a fresh row could even recycle a
    ///    compacted `Unused` slot mid-pipeline). Stage D's `Engine::vacuum`
    ///    guarantees this with the table's `AccessExclusive` lock; driving
    ///    the trait without that lock is the caller's own responsibility.
    fn reclaim(&self, rel: RelationDesc<'_>, dead: &[Tid]) -> Result<()>;
}
