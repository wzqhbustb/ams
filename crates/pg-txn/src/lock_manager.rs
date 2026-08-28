//! Table-level lock manager (M2c Stage P; tech-selection §9.2).
//!
//! Four standard lock modes with the §9.2 conflict matrix:
//!
//! | Mode              | Conflicts with                                  |
//! |-------------------|-------------------------------------------------|
//! | `AccessShare`     | `AccessExclusive`                               |
//! | `RowExclusive`    | `Exclusive`, `AccessExclusive`                  |
//! | `Exclusive`       | `RowExclusive`, `Exclusive`, `AccessExclusive`  |
//! | `AccessExclusive` | everything                                      |
//!
//! Intention locks (IS/IX) are deliberately absent (deferred to Phase 6 per
//! the ROADMAP), which keeps the matrix at 4×4 instead of PG's 8×8.
//!
//! # Fairness (FIFO, anti-starvation)
//!
//! A requester blocks when its mode conflicts with any *granted* mode OR any
//! waiter is already queued ahead of it — no barging. The second clause is
//! what keeps a queued `AccessExclusive` (a DDL) from starving under a stream
//! of mutually compatible `AccessShare` readers: once anyone queues, every
//! later arrival queues behind them. [`LockManager::release_all`] re-grants
//! the queue head(s) in order, walking the queue while consecutive head
//! waiters are compatible with the granted set.
//!
//! # 2PL: held to transaction end, no downgrade
//!
//! Locks are released only by [`LockManager::release_all`] at commit/abort
//! time. Re-acquiring a held lock with a stronger mode upgrades in place
//! (max strength); re-acquiring with the same or a weaker mode is a no-op.
//! An upgrade that must wait keeps the old grant while queued (same as PG):
//! the requester's old mode still counts as granted, so two transactions
//! upgrading the same table lock in opposite directions can deadlock; Stage
//! R's detector ([`crate::deadlock`]) consumes the wait state exposed here
//! ([`LockManager::table_lock_states`]) to break such cycles.
//!
//! # Deadlock scope
//!
//! Deadlock *detection* is Stage R ([`crate::deadlock`]): the detector
//! snapshots [`LockManager::table_lock_states`] together with the row-lock
//! registry, finds wait-for cycles, and marks a victim; a waiting
//! [`LockManager::acquire`] then observes its own victim flag, drops its
//! queue entry, and returns [`LockError::DeadlockVictim`]. What Stage P
//! established (and Stage R preserves) is that waiting never holds the
//! global lock-manager mutex while blocked: [`parking_lot::Condvar::wait`]
//! releases the mutex for the duration of the sleep, so a cycle wedges only
//! the participating transactions until the detector breaks it.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use thiserror::Error;

use pg_storage::types::{Oid, TxnId};

use crate::deadlock::DeadlockVictims;

/// Errors from the table lock manager.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LockError {
    /// The deadlock detector (M2c Stage R, §9.3) chose this transaction as
    /// the victim of a wait-for cycle and interrupted its table-lock wait.
    /// The waiter's queue entry is already removed; its GRANTED locks are
    /// kept (2PL) and are released by the caller's abort path
    /// ([`LockManager::release_all`]).
    #[error("deadlock detected: transaction {0} chosen as victim")]
    DeadlockVictim(TxnId),
}

/// A convenient alias for lock-manager results.
pub type LockResult<T> = std::result::Result<T, LockError>;

/// The four table-level lock modes (tech-selection §9.2).
///
/// Ordered by strength: `AccessShare < RowExclusive < Exclusive <
/// AccessExclusive`. Strength drives upgrade semantics: re-acquiring with a
/// stronger mode upgrades in place, with a same-or-weaker mode is a no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockMode {
    /// SELECT. Conflicts only with `AccessExclusive`.
    AccessShare,
    /// INSERT / UPDATE / DELETE. Conflicts with `Exclusive` and
    /// `AccessExclusive`.
    RowExclusive,
    /// Index creation (M2b/M3). Conflicts with `RowExclusive`, itself, and
    /// `AccessExclusive`.
    Exclusive,
    /// DDL (DROP, ALTER). Conflicts with every mode.
    AccessExclusive,
}

impl LockMode {
    /// The §9.2 conflict matrix, symmetric by construction:
    /// `AccessExclusive` conflicts with everything (including itself);
    /// `Exclusive` additionally conflicts with `RowExclusive` and itself.
    pub fn conflicts_with(self, other: LockMode) -> bool {
        use LockMode::{AccessExclusive, Exclusive, RowExclusive};
        matches!(
            (self, other),
            (AccessExclusive, _)
                | (_, AccessExclusive)
                | (RowExclusive, Exclusive)
                | (Exclusive, RowExclusive)
                | (Exclusive, Exclusive)
        )
    }
}

/// A queued lock request, FIFO order per table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Waiter {
    xid: TxnId,
    mode: LockMode,
}

/// Per-table lock state: the granted set plus the FIFO wait queue.
#[derive(Debug, Default)]
struct LockEntry {
    /// XID → held mode. Several XIDs may hold compatible modes concurrently;
    /// one XID holds exactly one (strongest) mode per table.
    granted: HashMap<TxnId, LockMode>,
    /// Blocked requesters in arrival order. A waiter for an *upgrade* keeps
    /// its old entry in `granted` while queued (see module docs).
    wait_queue: VecDeque<Waiter>,
}

impl LockEntry {
    /// Would `mode` for `xid` be grantable right now?
    ///
    /// Three gates, in order:
    ///
    /// 1. **Already held** at same-or-stronger mode → grant (no-op success).
    ///    This also covers waiters that [`LockManager::release_all`]
    ///    re-granted eagerly before they woke up.
    /// 2. **FIFO**: anyone queued ahead of `xid` blocks the grant, even when
    ///    the modes would be compatible — the anti-starvation rule.
    /// 3. **Conflict**: `mode` must not conflict with any granted mode held
    ///    by a *different* XID. `xid`'s own grant is ignored, which is what
    ///    makes upgrades possible at all.
    fn can_grant(&self, xid: TxnId, mode: LockMode) -> bool {
        if let Some(&held) = self.granted.get(&xid) {
            if held >= mode {
                return true;
            }
        }
        if let Some(head) = self.wait_queue.front() {
            if head.xid != xid {
                return false;
            }
        }
        self.granted
            .iter()
            .all(|(&holder, &held)| holder == xid || !held.conflicts_with(mode))
    }

    /// Record the grant, keeping the strongest mode (upgrade-in-place) and
    /// dropping `xid`'s queue entry if it has one (it can only be the head).
    fn grant(&mut self, xid: TxnId, mode: LockMode) {
        if self.wait_queue.front().is_some_and(|w| w.xid == xid) {
            self.wait_queue.pop_front();
        }
        let entry = self.granted.entry(xid).or_insert(mode);
        if mode > *entry {
            *entry = mode;
        }
    }

    /// Queue `xid` unless it already has a pending request on this table.
    fn enqueue_if_absent(&mut self, xid: TxnId, mode: LockMode) {
        if !self.wait_queue.iter().any(|w| w.xid == xid) {
            self.wait_queue.push_back(Waiter { xid, mode });
        }
    }

    /// Re-grant queue heads in FIFO order after a release: walk from the
    /// head, granting each waiter that is compatible with the granted set,
    /// stopping at the first that still conflicts. Because `can_grant`
    /// requires the head position, this naturally grants all compatible
    /// *consecutive* head waiters (e.g. two queued `AccessShare` readers
    /// behind a departing `AccessExclusive` both proceed; an `Exclusive`
    /// behind them does not).
    fn regrant_heads(&mut self) {
        while let Some(head) = self.wait_queue.front() {
            let Waiter { xid, mode } = *head;
            if !self.can_grant(xid, mode) {
                break;
            }
            self.grant(xid, mode);
        }
    }
}

/// Snapshot of one table's lock state, for tests and for Stage R's deadlock
/// detector (wait-for graph edges: each queued waiter → every conflicting
/// grant holder).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableLockState {
    /// Granted (XID, mode) pairs, unordered.
    pub granted: Vec<(TxnId, LockMode)>,
    /// Queued (XID, mode) pairs in FIFO order.
    pub waiters: Vec<(TxnId, LockMode)>,
}

/// Table-level lock manager: `HashMap<Oid, LockEntry>` behind a single
/// mutex, with one condvar shared by all waiters.
///
/// Depends only on `pg-storage` types (`Oid`, `TxnId`) and the Stage R
/// victim registry — never on `pg-catalog` (tech-selection §一 dependency
/// rule).
#[derive(Debug)]
pub struct LockManager {
    entries: Mutex<HashMap<Oid, LockEntry>>,
    /// Shared by waiters of every table. Wakeups are `notify_all`: a woken
    /// waiter re-checks `can_grant` and either proceeds (it was re-granted or
    /// reached a compatible head) or sleeps again. A per-table condvar would
    /// avoid cross-table wakeups but is not worth the extra map at M2c
    /// concurrency levels.
    condvar: Condvar,
    /// Deadlock-victim flags (M2c Stage R): shared with the `TxnManager`
    /// and the `DeadlockDetector` via [`Self::with_deadlock_victims`]. Leaf
    /// mutex — taken only while holding `entries` (entries → victims), and
    /// the detector never holds it while taking `entries` (mark first, then
    /// lock-and-notify), so no inversion is possible.
    deadlock_victims: Arc<DeadlockVictims>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    /// Create an empty lock manager with a private, never-marked victim
    /// registry (Stage P behavior: waits are never interrupted) until
    /// [`Self::with_deadlock_victims`] installs the shared one.
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            condvar: Condvar::new(),
            deadlock_victims: Arc::new(DeadlockVictims::new()),
        }
    }

    /// Install the shared deadlock-victim registry (M2c Stage R). Builder
    /// style: call before the manager is wrapped in an `Arc` and shared.
    /// The engine passes the SAME registry to the `TxnManager` and the
    /// `DeadlockDetector`, so a mark by the detector is visible to both
    /// wait loops.
    pub fn with_deadlock_victims(mut self, victims: Arc<DeadlockVictims>) -> Self {
        self.deadlock_victims = victims;
        self
    }

    /// The shared victim registry (identity-checked by
    /// `DeadlockDetector::start`'s debug assertion).
    pub fn deadlock_victims(&self) -> Arc<DeadlockVictims> {
        Arc::clone(&self.deadlock_victims)
    }

    /// Broadcast to ALL lock waiters without a grant change (M2c Stage R):
    /// the deadlock detector calls this after marking a victim so a waiter
    /// parked in [`Self::acquire`] re-checks its victim flag. Delivered
    /// under the entries mutex, matching `release_all`'s wakeup discipline
    /// — waiters check their predicates and sleep atomically with respect
    /// to this mutex, so the mark cannot be missed.
    pub fn notify_waiters(&self) {
        let _entries = self.entries.lock();
        self.condvar.notify_all();
    }

    /// Acquire `mode` on `table` for `xid`, blocking in FIFO order until the
    /// lock can be granted.
    ///
    /// Re-acquisition follows 2PL upgrade semantics: same-or-weaker mode is a
    /// no-op, a stronger mode upgrades in place (waiting, if necessary, with
    /// the old grant retained). The lock is held until
    /// [`Self::release_all`].
    ///
    /// Never blocks while holding the internal mutex — `Condvar::wait`
    /// releases it — so two transactions deadlocked against each other wedge
    /// only themselves until Stage R's detector breaks the cycle.
    ///
    /// # Victim interruption (M2c Stage R)
    ///
    /// The deadlock-victim flag is checked FIRST on every iteration, under
    /// the entries mutex: the detector marks the victim and then notifies
    /// under the same mutex ([`Self::notify_waiters`]), so a mark can never
    /// slip between the check and the sleep. On a hit the waiter consumes
    /// the flag, drops its queue entry, re-grants any newly compatible
    /// heads, and returns [`LockError::DeadlockVictim`]. A mark consumed on
    /// an acquisition that could have been granted immediately still fails:
    /// once chosen, the victim's current statement is dead — its granted
    /// locks remain held (2PL) until the caller's abort releases them.
    ///
    /// # Errors
    ///
    /// [`LockError::DeadlockVictim`] when the deadlock detector chose `xid`
    /// to break a wait-for cycle. Without a detector wired in, `acquire`
    /// never fails.
    pub fn acquire(&self, xid: TxnId, table: Oid, mode: LockMode) -> LockResult<()> {
        let mut entries = self.entries.lock();
        loop {
            if self.deadlock_victims.take(xid) {
                // Victim cleanup: drop the queued request, re-grant heads
                // that became compatible, and wake everyone so re-granted
                // waiters proceed. The victim's GRANTED locks stay (2PL);
                // the abort path's `release_all` drops them. `get_mut`, not
                // `entry().or_default()`: a victim interrupted before it ever
                // queued on this table has nothing to clean, and creating an
                // empty entry here would leak it (only `release_all` prunes
                // empty entries).
                let became_empty = if let Some(entry) = entries.get_mut(&table) {
                    entry.wait_queue.retain(|w| w.xid != xid);
                    entry.regrant_heads();
                    entry.granted.is_empty() && entry.wait_queue.is_empty()
                } else {
                    false
                };
                if became_empty {
                    entries.remove(&table);
                }
                self.condvar.notify_all();
                return Err(LockError::DeadlockVictim(xid));
            }
            let entry = entries.entry(table).or_default();
            if entry.can_grant(xid, mode) {
                entry.grant(xid, mode);
                return Ok(());
            }
            entry.enqueue_if_absent(xid, mode);
            self.condvar.wait(&mut entries);
        }
    }

    /// Non-blocking acquire: returns `Ok(true)` if the lock was granted
    /// (immediately, following the same FIFO and upgrade rules as
    /// [`Self::acquire`]), `Ok(false)` if it would have had to wait. Never
    /// enqueues: a failed `try_acquire` leaves no trace — in particular it
    /// must not insert an empty `LockEntry` (only `release_all` prunes
    /// them), so grantability is checked through `get` first and the entry
    /// is created only when the grant is actually recorded.
    pub fn try_acquire(&self, xid: TxnId, table: Oid, mode: LockMode) -> LockResult<bool> {
        let mut entries = self.entries.lock();
        let grantable = entries.get(&table).is_none_or(|e| e.can_grant(xid, mode));
        if grantable {
            entries.entry(table).or_default().grant(xid, mode);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Release every table lock held by `xid` (commit/abort, 2PL release
    /// point) and re-grant compatible queue heads in FIFO order, then wake
    /// all waiters so the re-granted heads proceed.
    ///
    /// Any *queued* request by `xid` is also dropped defensively: in Stage P
    /// a waiting `acquire` cannot be interrupted, so `xid` cannot be queued
    /// and releasing at once — but Stage R's victim abort will create
    /// exactly that state, and silently discarding the queue entry is the
    /// correct cleanup for it.
    pub fn release_all(&self, xid: TxnId) {
        let mut entries = self.entries.lock();
        entries.retain(|_, entry| {
            entry.granted.remove(&xid);
            entry.wait_queue.retain(|w| w.xid != xid);
            entry.regrant_heads();
            // Drop the entry once the table has neither holders nor waiters,
            // so long runs do not accumulate empty entries per table.
            !entry.granted.is_empty() || !entry.wait_queue.is_empty()
        });
        self.condvar.notify_all();
    }

    /// Is `xid` holding `table` at `mode` or stronger? (test/observability)
    pub fn is_granted(&self, xid: TxnId, table: Oid, mode: LockMode) -> bool {
        self.entries
            .lock()
            .get(&table)
            .is_some_and(|e| e.granted.get(&xid).is_some_and(|&held| held >= mode))
    }

    /// Every table lock `xid` currently holds: `(table, mode)` pairs
    /// (test/observability).
    pub fn held_by(&self, xid: TxnId) -> Vec<(Oid, LockMode)> {
        let entries = self.entries.lock();
        let mut held: Vec<(Oid, LockMode)> = entries
            .iter()
            .filter_map(|(&table, entry)| entry.granted.get(&xid).map(|&mode| (table, mode)))
            .collect();
        held.sort_unstable();
        held
    }

    /// Snapshot of one table's granted set and wait queue — the input Stage
    /// R's deadlock detector needs to build wait-for edges from table locks
    /// (row-lock edges come from `TxnManager::wait_edges`). Returns `None`
    /// when the table has no lock state at all.
    pub fn table_lock_state(&self, table: Oid) -> Option<TableLockState> {
        self.entries.lock().get(&table).map(|entry| {
            let mut granted: Vec<(TxnId, LockMode)> =
                entry.granted.iter().map(|(&x, &m)| (x, m)).collect();
            granted.sort_unstable();
            TableLockState {
                granted,
                waiters: entry.wait_queue.iter().map(|w| (w.xid, w.mode)).collect(),
            }
        })
    }

    /// Snapshot of every CONTENDED table's lock state (sorted by table
    /// OID) — the table-lock half of Stage R's wait-for graph. Tables whose
    /// wait queue is empty are skipped: they contribute no wait-for edges
    /// (edges go from waiters to conflicting holders), and the filter keeps
    /// the per-tick clone cost proportional to the number of contended
    /// tables rather than the total number of locked tables. The entries
    /// mutex is held only for the clone and released immediately, so the
    /// detector's tick never blocks `acquire` / `release_all` for more than
    /// one map scan.
    pub fn table_lock_states(&self) -> Vec<(Oid, TableLockState)> {
        let entries = self.entries.lock();
        let mut states: Vec<(Oid, TableLockState)> = entries
            .iter()
            .filter(|(_, entry)| !entry.wait_queue.is_empty())
            .map(|(&table, entry)| {
                let mut granted: Vec<(TxnId, LockMode)> =
                    entry.granted.iter().map(|(&x, &m)| (x, m)).collect();
                granted.sort_unstable();
                (
                    table,
                    TableLockState {
                        granted,
                        waiters: entry.wait_queue.iter().map(|w| (w.xid, w.mode)).collect(),
                    },
                )
            })
            .collect();
        states.sort_unstable_by_key(|(table, _)| *table);
        states
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use LockMode::{AccessExclusive, AccessShare, Exclusive, RowExclusive};

    const ALL: [LockMode; 4] = [AccessShare, RowExclusive, Exclusive, AccessExclusive];

    /// The §9.2 matrix, restated as data so the implementation cannot drift
    /// from the spec silently.
    const CONFLICTS: [[bool; 4]; 4] = [
        // held:    AS      RE      EX      AE      requested:
        [false, false, false, true], // AccessShare
        [false, false, true, true],  // RowExclusive
        [false, true, true, true],   // Exclusive
        [true, true, true, true],    // AccessExclusive
    ];

    #[test]
    fn conflict_matrix_matches_spec() {
        for (i, &a) in ALL.iter().enumerate() {
            for (j, &b) in ALL.iter().enumerate() {
                assert_eq!(
                    a.conflicts_with(b),
                    CONFLICTS[i][j],
                    "{a:?} vs {b:?} disagrees with §9.2"
                );
                assert_eq!(
                    a.conflicts_with(b),
                    b.conflicts_with(a),
                    "matrix must be symmetric"
                );
            }
        }
    }

    #[test]
    fn strength_ordering() {
        assert!(AccessShare < RowExclusive);
        assert!(RowExclusive < Exclusive);
        assert!(Exclusive < AccessExclusive);
    }
}
