//! MVCC snapshot (tech-selection §7.1).
//!
//! A [`Snapshot`] is the set of transactions whose effects are visible to one
//! scan. It is fully XID-based: visibility never compares LSNs. The v1
//! `snapshot_lsn` field was removed (v2 revision P2-8) — hint bits cache CLOG
//! outcomes, which are idempotent states, so readers need no LSN boundary to
//! shield themselves from "future" transactions.
//!
//! Interpretation:
//! - every XID `< xmin` is complete (committed or aborted) — consult the CLOG;
//! - every XID `>= xmax` started after this snapshot and is invisible;
//! - `xip` lists the XIDs in `[xmin, xmax)` that were still in progress when
//!   the snapshot was taken; they are invisible even if they later commit.
//!
//! # Anti-enumeration guardrail (M3 Stage A, tech-selection §3.3 v1.3/v1.4)
//!
//! The fields are PRIVATE to pg-txn (Rust has no "constructor" visibility, so
//! keeping literal construction out requires private fields): outside this
//! crate a `Snapshot` can only come from an associated function. The only
//! **registered** construction point is [`crate::TxnManager::snapshot`], which
//! registers the snapshot's `xmin` in the vacuum-horizon registry in the same
//! critical section that reads the active set and XID clock — no future call
//! site can construct a snapshot and forget to register it. The only
//! **unregistered** escape hatches are [`Snapshot::everything`] (catalog
//! bootstrap / tests; its `xmin = 0` would pin the horizon at 0 forever if it
//! were ever registered — it MUST NOT be) and the `#[doc(hidden)]`
//! test-constructor [`Snapshot::new_unregistered`]. A CI grep guard (the loom
//! job in `.github/workflows/ci.yml`) rejects any new `Snapshot {` literal or
//! `impl Snapshot` block outside pg-txn.

use smallvec::SmallVec;

use pg_storage::types::TxnId;

/// An MVCC snapshot: the set of transactions whose effects are visible
/// (tech-selection §7.1).
///
/// `xip` is a [`SmallVec`] with 32 inline slots (§16 approved dependency): the
/// overwhelming majority of snapshots see far fewer than 32 concurrent
/// transactions, so the common case never touches the heap.
///
/// Fields are crate-private; see the module docs for the anti-enumeration
/// guardrail. Read them through the accessor methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Lowest XID that is still considered running (all `< xmin` are complete).
    pub(crate) xmin: TxnId,
    /// First XID not yet assigned when the snapshot was taken.
    pub(crate) xmax: TxnId,
    /// In-progress XIDs in `[xmin, xmax)` at snapshot time, sorted ascending.
    pub(crate) xip: SmallVec<[TxnId; 32]>,
    /// The XID of the transaction that took this snapshot (sees its own writes).
    pub(crate) current_xid: TxnId,
    /// Command counter within the current transaction (v2.3, §7.1 Q4).
    ///
    /// The executor increments `curcid` by one **before** each SQL statement
    /// starts executing (not at commit, not at statement end). Every tuple
    /// written during the statement carries `t_cid = curcid`; a self-scan
    /// inside the same statement shares that `curcid`, so `t_cid < curcid` is
    /// false for the statement's own writes and they are skipped (Halloween
    /// protection). When the next statement advances the counter, the previous
    /// statement's writes become `t_cid < curcid` and turn into visible
    /// "writes by an earlier command".
    ///
    /// M2a ran one command per auto-commit transaction, so this stayed `0`
    /// (dead code path); M2b Stage L activates the increment protocol via
    /// [`Snapshot::advance_curcid`].
    pub(crate) curcid: u32,
}

impl Snapshot {
    /// A snapshot that sees every committed transaction and no in-progress one.
    ///
    /// M2a compatibility path: `xmin = 0`, `xmax = u64::MAX`, empty `xip`.
    /// Combined with a real CLOG, aborted inserters drop out automatically.
    ///
    /// # NEVER registered (M3 Stage A, §3.3 v1.4)
    ///
    /// This is the explicit NON-registered special case: catalog bootstrap
    /// (`Engine::open`'s index-registry rebuild) and test paths use it. It
    /// must NEVER enter the vacuum-horizon registry — its `xmin = 0` would
    /// pin `oldest_snapshot_xmin()` at 0 forever, disabling all reclamation.
    /// [`crate::TxnManager::snapshot`] is the only registered construction
    /// point.
    pub fn everything() -> Self {
        Snapshot {
            xmin: TxnId(0),
            xmax: TxnId(u64::MAX),
            xip: SmallVec::new(),
            current_xid: TxnId::INVALID,
            curcid: 0,
        }
    }

    /// Unregistered, fully-specified constructor for pg-txn's own visibility
    /// tests (the §7.2 oracle case tables hand-pick every field).
    ///
    /// NOT for engine/production use — a snapshot built this way is invisible
    /// to the vacuum horizon registry (same hazard class as
    /// [`Snapshot::everything`], see its doc). The CI grep guard allows it
    /// because it lives inside pg-txn.
    #[doc(hidden)]
    pub fn new_unregistered(
        xmin: TxnId,
        xmax: TxnId,
        xip: SmallVec<[TxnId; 32]>,
        current_xid: TxnId,
        curcid: u32,
    ) -> Self {
        Snapshot {
            xmin,
            xmax,
            xip,
            current_xid,
            curcid,
        }
    }

    /// Lowest XID still considered running (all `< xmin` are complete).
    pub fn xmin(&self) -> TxnId {
        self.xmin
    }

    /// First XID not yet assigned when the snapshot was taken.
    pub fn xmax(&self) -> TxnId {
        self.xmax
    }

    /// In-progress XIDs in `[xmin, xmax)` at snapshot time, sorted ascending.
    pub fn xip(&self) -> &[TxnId] {
        &self.xip
    }

    /// The XID of the transaction that took this snapshot (sees its own
    /// writes).
    pub fn current_xid(&self) -> TxnId {
        self.current_xid
    }

    /// Command counter within the current transaction (§7.1 Q4; see the
    /// field doc).
    pub fn curcid(&self) -> u32 {
        self.curcid
    }

    /// Overwrite `current_xid`. Intended for tests/benches that hand-build
    /// writer scenarios on top of [`Snapshot::everything`]; engine code gets
    /// the XID stamped at construction by [`crate::TxnManager::snapshot`].
    /// Harmless to the horizon registry (registration keys on `xmin`, which
    /// has no setter).
    pub fn set_current_xid(&mut self, xid: TxnId) {
        self.current_xid = xid;
    }

    /// Overwrite `curcid`. Intended for tests that hand-build §7.2 case-table
    /// scenarios; engine code must use [`Snapshot::advance_curcid`] so the
    /// counter only moves forward.
    pub fn set_curcid(&mut self, curcid: u32) {
        self.curcid = curcid;
    }

    /// Advance the command counter by one and return the new value (§7.1 Q4).
    ///
    /// The executor calls this **before** each SQL statement of a
    /// multi-statement transaction begins; the returned value is stamped into
    /// every tuple the statement writes as `t_cid`. All `is_visible` calls
    /// within one statement observe the same `curcid` (the statement shares
    /// the counter), so the statement never re-scans its own writes.
    ///
    /// # Two couplings the caller must respect (Stage L review)
    ///
    /// - **RC isolation**: a per-statement `TxnManager::snapshot()` call
    ///   always returns `curcid = 0`, so an RC executor must carry the
    ///   command counter in the transaction/executor state and inject it
    ///   here (set `snapshot.curcid` after obtaining the snapshot) — the
    ///   counter is transaction-scoped, not snapshot-scoped. Stage O wires
    ///   this.
    /// - **`t_cid` stamping**: advancing the counter is meaningless unless
    ///   every tuple written by the statement is stamped with the new value
    ///   (tuple header offset 60..64). Migrating heap scans to the
    ///   `PgVisibilityOracle` without stamping `t_cid` silently disables the
    ///   Halloween protection (all writes read as `t_cid = 0 < curcid`).
    pub fn advance_curcid(&mut self) -> u32 {
        self.curcid += 1;
        self.curcid
    }
}
