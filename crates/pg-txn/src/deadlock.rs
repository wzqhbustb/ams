//! Deadlock detection (M2c Stage R; tech-selection §9.3).
//!
//! [`DeadlockDetector`] runs a background thread that wakes every `interval`
//! (default 100ms at the engine, shorter in tests), snapshots the wait-for
//! graph from its two sources, finds cycles, and aborts the youngest
//! transaction of each cycle (the maximum XID) by marking it in the shared
//! [`DeadlockVictims`] registry and broadcasting on BOTH wait condvars. The
//! wait loops themselves deliver the error: [`TxnManager::wait_for`] returns
//! [`crate::TxnError::DeadlockVictim`] and [`LockManager::acquire`] returns
//! [`crate::LockError::DeadlockVictim`] when the caller finds its own XID
//! marked.
//!
//! # Wait-for graph: hard edges only
//!
//! Nodes are XIDs; edges are:
//!
//! - **Row-lock edges** — [`TxnManager::wait_edges`]: waiter → the XID whose
//!   `t_xmax` stamp it is blocked on (§9.1 step 5a registry).
//! - **Table-lock edges** — for every table, each queued waiter → every
//!   granted holder whose mode conflicts with the waiter's mode
//!   ([`LockMode::conflicts_with`](crate::lock_manager::LockMode::conflicts_with)).
//!
//! A waiter blocked ONLY by FIFO queue position (its mode is compatible with
//! the granted set, but a conflicting request is queued ahead of it) gets NO
//! edge. Hard edges suffice for cycles of genuine conflicts: a FIFO-blocked
//! waiter's queue head is always conflict-blocked, so any wedge of pure
//! conflicts shows up as a hard-edge cycle. The known blind spot is PG's
//! "soft edge" case — a deadlock routed THROUGH queue order (a compatible
//! waiter queued behind a conflicting one that is itself part of a cycle) is
//! not detected; PG solves this with soft-edge queue reordering, which is
//! out of scope for M2c (documented Stage R limitation).
//!
//! # Performance envelope
//!
//! A tick's cost is proportional to the number of CONTENTED tables (those
//! with a non-empty wait queue — `LockManager::table_lock_states` filters
//! the rest out before cloning) plus the number of waiting edges, not to
//! the total lock count; each tick takes at most two double-source
//! snapshots (initial scan + one shared re-verification pass). The Stage R
//! acceptance (tick p99 ≤ 5ms, detector CPU < 1%) is measured at the
//! production 100ms tick against a populated acyclic wait graph, 120 ticks
//! (~12s) — see
//! `tests/deadlock_detection.rs::test_detector_tick_latency_and_cpu_budget`
//! for the exact workload. Measured on the dev machine: p99 ≈ 230µs,
//! busy/wall ≈ 0.05%.
//!
//! # Torn snapshots
//!
//! The row registry and the lock manager are snapshotted SEQUENTIALLY
//! (take-and-release, never nested — see the lock-order rules on
//! [`TxnManager::wait_for`]), so the graph can be torn: an edge may come from
//! a slightly different instant than another. Before marking a victim the
//! detector RE-READS both sources and verifies every edge of the candidate
//! cycle is still present and the victim is still active. This narrows the
//! false-positive window but does not close it:
//!
//! - a cycle can dissolve between the re-check and the mark;
//! - the re-verification snapshot is itself torn, so a "cycle" assembled
//!   from edges that NEVER coexisted at any real instant can still verify.
//!
//! The probability of a false mark is therefore nonzero — but the outcome
//! is always semantically safe: the victim's current statement fails with a
//! retryable deadlock error (exactly what PG allows a caller to see under
//! `deadlock_timeout` races), and the caller aborts and retries.
//!
//! # Victim flag lifecycle
//!
//! [`DeadlockVictims`] is a leaf-lock set shared by the detector, the
//! [`TxnManager`], and the [`LockManager`]. A flag is removed in exactly
//! three ways:
//!
//! 1. **Consumed** — the victim's own wait loop observes the mark (`take`),
//!    cleans up its wait state, and returns the deadlock error.
//! 2. **Cleared at `end_txn`** — a victim whose transaction ends without
//!    consuming the flag (marked after its last wait completed) has the
//!    stale flag dropped by `TxnManager::end_txn`.
//! 3. **Pruned by the detector** — each tick drops flags whose XID left the
//!    active set, covering the mark-after-`end_txn`-clear race (XIDs are
//!    never reused, so a leftover flag would be inert anyway; pruning keeps
//!    the set bounded).
//!
//! Marking is idempotent: if the victim's wait already dissolved, the flag
//! simply sits until (2) or (3) removes it, and the victim's NEXT wait (if
//! any) consumes it — the PG-like outcome that the current statement fails
//! and the caller must roll back.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use pg_storage::types::TxnId;

use crate::lock_manager::LockManager;
use crate::manager::TxnManager;

/// Default detection interval (tech-selection §9.3: 100ms background scan).
pub const DEFAULT_DEADLOCK_INTERVAL: Duration = Duration::from_millis(100);

/// Number of recent tick durations kept for observability/tests.
const TICK_DURATION_CAP: usize = 4096;

/// The set of XIDs chosen as deadlock victims, shared by the detector (which
/// marks) and the two wait loops (which consume).
///
/// This mutex is a LEAF in the lock order: it may be taken while holding the
/// row-wait registry mutex or the lock-manager entries mutex, but no other
/// lock is ever acquired while holding it.
#[derive(Debug, Default)]
pub struct DeadlockVictims {
    set: Mutex<HashSet<TxnId>>,
}

impl DeadlockVictims {
    /// Create an empty victim registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `xid` as a victim. Idempotent — the detector may mark the same
    /// XID on consecutive ticks while its abort is still propagating.
    pub fn mark(&self, xid: TxnId) {
        self.set.lock().insert(xid);
    }

    /// Consume the flag: returns true (and clears it) if `xid` is marked.
    /// Called by the wait loops; only ONE waiter consumes a given mark.
    pub fn take(&self, xid: TxnId) -> bool {
        self.set.lock().remove(&xid)
    }

    /// Drop a flag without consuming it (`end_txn` stale-flag cleanup and
    /// the detector's inactive-XID pruning).
    pub fn clear(&self, xid: TxnId) {
        self.set.lock().remove(&xid);
    }

    /// Is `xid` currently marked? (detector skip-check, tests)
    pub fn is_marked(&self, xid: TxnId) -> bool {
        self.set.lock().contains(&xid)
    }

    /// Snapshot of all marked XIDs (detector pruning, tests).
    pub fn marked(&self) -> Vec<TxnId> {
        let mut v: Vec<TxnId> = self.set.lock().iter().copied().collect();
        v.sort_unstable();
        v
    }
}

/// Detector run statistics (observability + the Stage R performance
/// acceptance: tick p99 ≤ 5ms, detector CPU < 1%).
#[derive(Debug, Default)]
struct DetectorStats {
    /// Completed ticks.
    ticks: u64,
    /// Ticks whose body PANICKED (caught; the thread keeps running). Must
    /// stay 0 — a non-zero count is a detector bug.
    panics: u64,
    /// Wall-clock duration of recent tick bodies (capped ring).
    durations: VecDeque<Duration>,
}

/// The background deadlock detector. One per engine.
///
/// Owns its thread: [`Self::stop`] (also run by `Drop`) sets the stop flag,
/// wakes the sleep, and joins, so a dropped detector never leaves an
/// orphaned thread behind. Stopping is idempotent.
#[derive(Debug)]
pub struct DeadlockDetector {
    /// Stop flag + condvar the thread sleeps on between ticks, so shutdown
    /// latency is one notify, not one full interval.
    stop: Arc<(Mutex<bool>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>,
    stats: Arc<Mutex<DetectorStats>>,
}

impl DeadlockDetector {
    /// Start the detector thread over the given managers.
    ///
    /// `victims` MUST be the same registry the `txn` and `lock_manager`
    /// were built with (`with_deadlock_victims`) — otherwise marks never
    /// reach the wait loops and detection silently no-ops. The engine wires
    /// all three from one Arc; unit tests must do the same.
    ///
    /// `interval` is clamped to a 1ms floor: a zero (or near-zero) interval
    /// would busy-loop the scan, burning a core on empty graphs for no
    /// detection-latency benefit anyone can observe.
    pub fn start(
        txn: Arc<TxnManager>,
        lock_manager: Arc<LockManager>,
        victims: Arc<DeadlockVictims>,
        interval: Duration,
    ) -> Self {
        // A mismatched registry makes detection silently no-op (marks never
        // reach the wait loops) — catch mis-wiring in debug builds.
        debug_assert!(Arc::ptr_eq(&victims, &txn.deadlock_victims()));
        debug_assert!(Arc::ptr_eq(&victims, &lock_manager.deadlock_victims()));
        let interval = interval.max(Duration::from_millis(1));
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let stats = Arc::new(Mutex::new(DetectorStats::default()));
        let handle = {
            let stop = Arc::clone(&stop);
            let stats = Arc::clone(&stats);
            std::thread::Builder::new()
                .name("deadlock-detector".to_string())
                .spawn(move || Self::run(&txn, &lock_manager, &victims, interval, &stop, &stats))
                .expect("failed to spawn deadlock detector thread")
        };
        Self {
            stop,
            handle: Mutex::new(Some(handle)),
            stats,
        }
    }

    /// Stop the thread and join it. Idempotent; also called by `Drop`.
    pub fn stop(&self) {
        {
            let (lock, cv) = &*self.stop;
            *lock.lock() = true;
            cv.notify_all();
        }
        if let Some(handle) = self.handle.lock().take() {
            // A detector tick is bounded (µs-scale graph walk), so the join
            // cannot wedge shutdown; a panicked thread is ignored here — it
            // is already dead, which is the state stop() wants.
            let _ = handle.join();
        }
    }

    /// Number of completed ticks (test/observability).
    pub fn tick_count(&self) -> u64 {
        self.stats.lock().ticks
    }

    /// Number of ticks whose body panicked (caught). Must be 0.
    pub fn panic_count(&self) -> u64 {
        self.stats.lock().panics
    }

    /// Wall-clock durations of the most recent ticks (oldest first), for the
    /// Stage R tick-latency acceptance.
    pub fn tick_durations(&self) -> Vec<Duration> {
        self.stats.lock().durations.iter().copied().collect()
    }

    /// The thread body: sleep `interval` (or until stopped), then one tick.
    fn run(
        txn: &TxnManager,
        lock_manager: &LockManager,
        victims: &DeadlockVictims,
        interval: Duration,
        stop: &(Mutex<bool>, Condvar),
        stats: &Mutex<DetectorStats>,
    ) {
        loop {
            {
                let (lock, cv) = stop;
                let mut stopped = lock.lock();
                if *stopped {
                    return;
                }
                let _timeout = cv.wait_for(&mut stopped, interval);
                if *stopped {
                    return;
                }
            }
            let started = Instant::now();
            // A tick must NEVER kill the thread: a panic (a detector bug)
            // is caught, counted, and the loop continues — deadlock
            // detection degrades to one missed tick instead of none.
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                Self::tick(txn, lock_manager, victims);
            }))
            .is_err();
            let elapsed = started.elapsed();
            let mut s = stats.lock();
            s.ticks += 1;
            if panicked {
                s.panics += 1;
            }
            if s.durations.len() == TICK_DURATION_CAP {
                s.durations.pop_front();
            }
            s.durations.push_back(elapsed);
        }
    }

    /// One detection round: snapshot, find cycles, re-verify, mark victims.
    fn tick(txn: &TxnManager, lock_manager: &LockManager, victims: &DeadlockVictims) {
        // Hygiene first: drop flags of XIDs that already ended (covers the
        // mark-after-`end_txn`-clear race; see the module docs).
        for xid in victims.marked() {
            if !txn.is_active(xid) {
                victims.clear(xid);
            }
        }

        let edges = wait_for_edges(txn, lock_manager);
        if edges.is_empty() {
            return;
        }
        let cycles = find_cycles(&edges);
        if cycles.is_empty() {
            return;
        }
        // Torn-snapshot re-verification (module docs): ONE fresh double
        // source snapshot per tick, shared by all candidate cycles — a
        // per-cycle snapshot costs an extra lock-manager map scan for each
        // candidate, and the single later snapshot provides the same
        // guarantee (every marked cycle was verified against a state read
        // strictly after the one that found it).
        let fresh: HashSet<(TxnId, TxnId)> =
            wait_for_edges(txn, lock_manager).into_iter().collect();
        Self::mark_victims(txn, lock_manager, victims, &cycles, &fresh);
    }

    /// Resolve candidate cycles against the re-verification snapshot: mark
    /// each surviving cycle's youngest XID and wake both wait surfaces.
    ///
    /// Skip branches, in order: (a) the victim is already marked (earlier
    /// cycle this tick, or an unconsumed mark from a previous one); (b) the
    /// victim ENDED between the initial snapshot and now — its death broke
    /// the cycle for real, so there is nothing to abort; (c) the cycle
    /// dissolved by re-verification time (an edge is gone from `fresh`).
    ///
    /// Split out of [`Self::tick`] so the safety-critical skip branches can
    /// be unit-tested with hand-crafted snapshot pairs — a deterministic
    /// "cycle dissolves between the two `wait_for_edges` calls"
    /// interleaving is not reachable from outside the tick.
    fn mark_victims(
        txn: &TxnManager,
        lock_manager: &LockManager,
        victims: &DeadlockVictims,
        cycles: &[Vec<TxnId>],
        fresh: &HashSet<(TxnId, TxnId)>,
    ) {
        for cycle in cycles {
            // Victim = the youngest transaction in the cycle (max XID):
            // it provably started after the others, so killing it costs
            // the least completed work (tech-selection §9.3).
            let victim = *cycle.iter().max().expect("a cycle is non-empty");
            if victims.is_marked(victim) {
                // (a) one mark is enough.
                continue;
            }
            // (b) victim already ended.
            if !txn.is_active(victim) {
                continue;
            }
            // (c) require every edge of the candidate cycle to still exist
            // in the fresh snapshot before marking.
            let still_there = cycle
                .iter()
                .zip(cycle.iter().cycle().skip(1))
                .all(|(&from, &to)| fresh.contains(&(from, to)));
            if !still_there {
                continue;
            }
            victims.mark(victim);
            // Wake BOTH wait surfaces: the victim may be parked on a row
            // lock (TxnManager) or a table lock (LockManager); each wait
            // loop re-checks the victim flag before sleeping again, so the
            // extra broadcast to the non-victim side is harmless.
            txn.notify_row_waiters();
            lock_manager.notify_waiters();
        }
    }
}

impl Drop for DeadlockDetector {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Snapshot both wait-edge sources and merge them into one edge list
/// (sorted, deduped). Each source is locked, cloned, and released in turn —
/// never held across the other — so the hot path (`end_txn`, `release_all`,
/// `wait_for`) is never blocked by the detector, and the detector can never
/// invert the registry→active / entries→victims lock orders.
///
/// Public since M3 Stage E (tech-selection §6.2): the engine's `wait_edges()`
/// introspection API exposes exactly the graph the detector consumes, so the
/// diagnostic surface can never drift from deadlock detection.
pub fn wait_for_edges(txn: &TxnManager, lock_manager: &LockManager) -> Vec<(TxnId, TxnId)> {
    let mut edges = txn.wait_edges();
    for (_table, state) in lock_manager.table_lock_states() {
        for &(waiter, waiter_mode) in &state.waiters {
            for &(holder, holder_mode) in &state.granted {
                // Hard edge only: the waiter genuinely conflicts with this
                // holder. FIFO queue ORDER creates no edges (module docs).
                if holder != waiter && holder_mode.conflicts_with(waiter_mode) {
                    edges.push((waiter, holder));
                }
            }
        }
    }
    edges.sort_unstable();
    edges.dedup();
    edges
}

/// Every elementary cycle reachable via DFS back edges, each returned as the
/// stack slice `v0 → v1 → … → vk` with an implied closing edge `vk → v0`.
///
/// Iterative DFS with white/gray/black coloring; a gray successor closes a
/// cycle. Deterministic: nodes and adjacency lists are visited in sorted
/// XID order. The same cycle can be reported more than once (different
/// entry rotations); callers dedupe by victim.
fn find_cycles(edges: &[(TxnId, TxnId)]) -> Vec<Vec<TxnId>> {
    let mut adjacency: HashMap<TxnId, Vec<TxnId>> = HashMap::new();
    for &(from, to) in edges {
        adjacency.entry(from).or_default().push(to);
    }
    for targets in adjacency.values_mut() {
        targets.sort_unstable();
        targets.dedup();
    }
    let mut nodes: Vec<TxnId> = adjacency.keys().copied().collect();
    nodes.sort_unstable();

    const WHITE: u8 = 0;
    const GRAY: u8 = 1;
    const BLACK: u8 = 2;
    let mut color: HashMap<TxnId, u8> = HashMap::new();
    let mut cycles = Vec::new();

    for start in nodes {
        if color.get(&start).copied().unwrap_or(WHITE) != WHITE {
            continue;
        }
        color.insert(start, GRAY);
        // Explicit DFS stack: `path` is the gray chain, `next_child[i]` the
        // adjacency cursor of `path[i]`.
        let mut path: Vec<TxnId> = vec![start];
        let mut next_child: Vec<usize> = vec![0];
        while let Some(&node) = path.last() {
            let children = adjacency.get(&node).map_or(&[][..], Vec::as_slice);
            let cursor = next_child.last_mut().expect("cursor matches path");
            if *cursor < children.len() {
                let next = children[*cursor];
                *cursor += 1;
                match color.get(&next).copied().unwrap_or(WHITE) {
                    GRAY => {
                        let pos = path
                            .iter()
                            .position(|&n| n == next)
                            .expect("a gray node is on the current path");
                        cycles.push(path[pos..].to_vec());
                    }
                    WHITE => {
                        color.insert(next, GRAY);
                        path.push(next);
                        next_child.push(0);
                    }
                    _ => {} // BLACK: fully explored, cannot close a new cycle
                }
            } else {
                color.insert(node, BLACK);
                path.pop();
                next_child.pop();
            }
        }
    }
    cycles
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xid(n: u64) -> TxnId {
        TxnId(n)
    }

    #[test]
    fn find_cycles_simple_ring() {
        let cycles = find_cycles(&[(xid(1), xid(2)), (xid(2), xid(1))]);
        assert_eq!(cycles, vec![vec![xid(1), xid(2)]]);
    }

    #[test]
    fn find_cycles_acyclic() {
        assert!(find_cycles(&[(xid(1), xid(2)), (xid(2), xid(3))]).is_empty());
        assert!(find_cycles(&[]).is_empty());
    }

    #[test]
    fn find_cycles_self_loop() {
        // A self-edge is a 1-node cycle (should never occur in practice —
        // both edge sources exclude self — but the walker must handle it).
        let cycles = find_cycles(&[(xid(1), xid(1))]);
        assert_eq!(cycles, vec![vec![xid(1)]]);
    }

    #[test]
    fn find_cycles_ring_with_tail() {
        // 1 → 2 → 3 → 2: the cycle is {2, 3}, the tail 1 is excluded.
        let cycles = find_cycles(&[(xid(1), xid(2)), (xid(2), xid(3)), (xid(3), xid(2))]);
        assert_eq!(cycles, vec![vec![xid(2), xid(3)]]);
    }

    #[test]
    fn find_cycles_two_disjoint_rings() {
        let cycles = find_cycles(&[
            (xid(1), xid(2)),
            (xid(2), xid(1)),
            (xid(3), xid(4)),
            (xid(4), xid(3)),
        ]);
        // Exact contents, not just the count (deterministic: nodes and
        // adjacency lists are visited in sorted XID order).
        assert_eq!(cycles, vec![vec![xid(1), xid(2)], vec![xid(3), xid(4)]]);
    }

    #[test]
    fn victims_mark_take_clear() {
        let victims = DeadlockVictims::new();
        victims.mark(xid(7));
        victims.mark(xid(7)); // idempotent
        assert!(victims.is_marked(xid(7)));
        assert_eq!(victims.marked(), vec![xid(7)]);
        assert!(victims.take(xid(7)));
        assert!(!victims.take(xid(7)), "a consumed flag stays consumed");
        victims.mark(xid(8));
        victims.clear(xid(8));
        assert!(victims.marked().is_empty());
    }

    /// The `mark_victims` skip branches need a real `TxnManager` (for
    /// `is_active`) but no disk: same no-op WAL pattern as the manager's
    /// own unit tests. A deterministic "cycle dissolves between the two
    /// `wait_for_edges` calls inside one tick" interleaving is not
    /// reachable from outside the tick, so these tests drive
    /// `mark_victims` directly with hand-crafted snapshot pairs (initial
    /// `cycles` vs. re-verification `fresh`).
    mod mark_victims_skips {
        use super::super::{DeadlockDetector, DeadlockVictims};
        use crate::{CommitWal, InMemoryClogAccessor, LockManager, TxnManager};
        use pg_storage::error::Result as StorageResult;
        use pg_storage::txn_id::TxnIdClock;
        use pg_storage::types::{Lsn, TxnId};
        use pg_storage::wal::record::WalRecord;
        use std::collections::HashSet;
        use std::sync::Arc;

        #[derive(Debug, Default)]
        struct OkWal;

        impl CommitWal for OkWal {
            fn append(&self, _record: WalRecord) -> StorageResult<Lsn> {
                Ok(Lsn::FIRST)
            }

            fn flush_to(&self, _lsn: Lsn) -> StorageResult<()> {
                Ok(())
            }
        }

        fn rig() -> (TxnManager, LockManager, Arc<DeadlockVictims>) {
            let victims = Arc::new(DeadlockVictims::new());
            let mgr = TxnManager::new(
                TxnIdClock::new(TxnId::FIRST),
                Arc::new(OkWal),
                Arc::new(InMemoryClogAccessor::new()),
            )
            .with_deadlock_victims(Arc::clone(&victims));
            let lm = LockManager::new().with_deadlock_victims(Arc::clone(&victims));
            (mgr, lm, victims)
        }

        /// Skip branch (b): the victim ENDED between the initial snapshot
        /// and the mark decision. Nothing may be marked — the victim's own
        /// death already broke the cycle.
        #[test]
        fn skips_victim_that_ended_before_reverify() {
            let (mgr, lm, victims) = rig();
            let x1 = mgr.begin_txn();
            let x2 = mgr.begin_txn();
            // The ring {x1, x2} was found by the initial snapshot, but x2
            // (the youngest, i.e. the victim candidate) has since ended.
            mgr.commit_txn(x2).unwrap();
            let cycles = vec![vec![x1, x2]];
            let fresh: HashSet<(TxnId, TxnId)> = [(x1, x2), (x2, x1)].into_iter().collect();
            DeadlockDetector::mark_victims(&mgr, &lm, &victims, &cycles, &fresh);
            assert!(
                victims.marked().is_empty(),
                "a victim that already ended must not be marked"
            );
            mgr.commit_txn(x1).unwrap();
        }

        /// Skip branch (c): the cycle dissolved by re-verification time —
        /// the closing edge is gone from the fresh snapshot. Nothing may be
        /// marked, so no DeadlockVictim can ever surface (a mark is the
        /// ONLY source of that error, and the registry stays empty).
        #[test]
        fn skips_cycle_dissolved_at_reverify() {
            let (mgr, lm, victims) = rig();
            let x1 = mgr.begin_txn();
            let x2 = mgr.begin_txn();
            let cycles = vec![vec![x1, x2]];
            // Fresh snapshot still has x1 → x2 but lost the closing edge.
            let fresh: HashSet<(TxnId, TxnId)> = [(x1, x2)].into_iter().collect();
            DeadlockDetector::mark_victims(&mgr, &lm, &victims, &cycles, &fresh);
            assert!(
                victims.marked().is_empty(),
                "a dissolved cycle must not be marked"
            );
            mgr.commit_txn(x1).unwrap();
            mgr.commit_txn(x2).unwrap();
        }

        /// Positive control: the same rig DOES mark a cycle that survives
        /// re-verification with an active victim — proving the two skip
        /// tests above are not vacuously passing.
        #[test]
        fn marks_cycle_that_survives_reverify() {
            let (mgr, lm, victims) = rig();
            let x1 = mgr.begin_txn();
            let x2 = mgr.begin_txn();
            let cycles = vec![vec![x1, x2]];
            let fresh: HashSet<(TxnId, TxnId)> = [(x1, x2), (x2, x1)].into_iter().collect();
            DeadlockDetector::mark_victims(&mgr, &lm, &victims, &cycles, &fresh);
            assert_eq!(
                victims.marked(),
                vec![x2],
                "the surviving cycle's youngest XID must be marked"
            );
            mgr.commit_txn(x1).unwrap();
            mgr.commit_txn(x2).unwrap();
        }
    }
}
