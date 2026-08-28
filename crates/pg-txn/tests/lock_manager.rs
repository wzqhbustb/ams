//! M2c Stage P acceptance: table-level [`LockManager`] (tech-selection §9.2).
//!
//! Covers the full 4×4 grant matrix, FIFO fairness (anti-starvation: a queued
//! waiter blocks later arrivals even when their modes would be compatible
//! with the current grants), upgrade-in-place, `release_all` re-granting
//! compatible consecutive head waiters, and multi-holder compatible modes.
//!
//! Acceptance: `cargo test -p pg-txn --test lock_manager`

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pg_storage::types::{Oid, TxnId};
use pg_txn::{LockManager, LockMode};
use LockMode::{AccessExclusive, AccessShare, Exclusive, RowExclusive};

const ALL_MODES: [LockMode; 4] = [AccessShare, RowExclusive, Exclusive, AccessExclusive];

/// The §9.2 conflict matrix as data: `CONFLICTS[held][requested]`.
const CONFLICTS: [[bool; 4]; 4] = [
    // requested:  AS      RE      EX      AE
    /* held AS */ [false, false, false, true],
    /* held RE */ [false, false, true, true],
    /* held EX */ [false, true, true, true],
    /* held AE */ [true, true, true, true],
];

fn xid(n: u64) -> TxnId {
    TxnId(n)
}

fn table(n: u64) -> Oid {
    Oid(n)
}

/// Poll `pred` every 5 ms until it holds; panic after 5 s. State-based
/// synchronization (no fixed sleeps): the assertions below observe the lock
/// manager's own wait queues, never wall-clock luck.
fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Spawn a blocking `acquire`; the returned handle finishes once the lock is
/// granted. State assertions go through `table_lock_state`, so the test never
/// depends on when exactly the thread is scheduled.
fn spawn_acquire(lm: &Arc<LockManager>, x: TxnId, t: Oid, m: LockMode) -> JoinHandle<()> {
    let lm = Arc::clone(lm);
    thread::spawn(move || lm.acquire(x, t, m).unwrap())
}

fn waiters_of(lm: &LockManager, t: Oid) -> Vec<(TxnId, LockMode)> {
    lm.table_lock_state(t)
        .map(|s| s.waiters)
        .unwrap_or_default()
}

fn granted_of(lm: &LockManager, t: Oid) -> Vec<(TxnId, LockMode)> {
    lm.table_lock_state(t)
        .map(|s| s.granted)
        .unwrap_or_default()
}

/// Full 4×4 grant matrix (§9.2): with `held` granted to another XID, a
/// `try_acquire` of `requested` succeeds iff the modes do not conflict.
#[test]
fn test_table_lock_conflict_matrix() {
    for (hi, &held) in ALL_MODES.iter().enumerate() {
        for (ri, &requested) in ALL_MODES.iter().enumerate() {
            let lm = LockManager::new();
            lm.acquire(xid(1), table(100), held).unwrap();
            let got = lm.try_acquire(xid(2), table(100), requested).unwrap();
            assert_eq!(
                got, !CONFLICTS[hi][ri],
                "held={held:?} requested={requested:?}: try_acquire={got}, \
                 expected {} per §9.2",
                !CONFLICTS[hi][ri]
            );
        }
    }
}

/// Two transactions hold AccessShare on the same table concurrently
/// (multi-holder compatible modes).
#[test]
fn test_multi_holder_compatible_modes() {
    let lm = LockManager::new();
    lm.acquire(xid(1), table(1), AccessShare).unwrap();
    lm.acquire(xid(2), table(1), AccessShare).unwrap();
    assert!(lm.is_granted(xid(1), table(1), AccessShare));
    assert!(lm.is_granted(xid(2), table(1), AccessShare));
    assert_eq!(granted_of(&lm, table(1)).len(), 2);
}

/// Re-acquiring with a stronger mode upgrades in place; re-acquiring with
/// the same-or-weaker mode is a no-op that never blocks — even when another
/// holder present means a *fresh* request for that mode would have to queue
/// behind a waiter.
#[test]
fn test_upgrade_in_place_and_weaker_reacquire_noop() {
    let lm = Arc::new(LockManager::new());

    // Upgrade path with no other holders: AS -> AE succeeds immediately.
    lm.acquire(xid(1), table(1), AccessShare).unwrap();
    lm.acquire(xid(1), table(1), AccessExclusive).unwrap();
    assert!(lm.is_granted(xid(1), table(1), AccessExclusive));
    // One grant entry, at the strongest mode.
    assert_eq!(lm.held_by(xid(1)), vec![(table(1), AccessExclusive)]);
    // Downgrade attempt is a no-op: still AccessExclusive.
    lm.acquire(xid(1), table(1), RowExclusive).unwrap();
    assert_eq!(lm.held_by(xid(1)), vec![(table(1), AccessExclusive)]);

    // Same-or-weaker re-acquire must not queue even when a waiter exists:
    // A holds AS on t2, B queues for AE, A re-acquires AS -> no-op success,
    // B still waiting, A untouched.
    lm.acquire(xid(10), table(2), AccessShare).unwrap();
    let b = spawn_acquire(&lm, xid(11), table(2), AccessExclusive);
    wait_until("B queued for AE", || waiters_of(&lm, table(2)).len() == 1);
    lm.acquire(xid(10), table(2), AccessShare).unwrap();
    assert_eq!(waiters_of(&lm, table(2)), vec![(xid(11), AccessExclusive)]);
    assert_eq!(granted_of(&lm, table(2)), vec![(xid(10), AccessShare)]);

    // Cleanup: release A so B's upgrade grant lands and the thread ends.
    lm.release_all(xid(10));
    b.join().unwrap();
    assert!(lm.is_granted(xid(11), table(2), AccessExclusive));
}

/// An upgrade that conflicts with another holder waits (keeping its old
/// grant) and completes when the other holder releases.
#[test]
fn test_upgrade_waits_then_completes() {
    let lm = Arc::new(LockManager::new());
    lm.acquire(xid(1), table(1), AccessShare).unwrap();
    lm.acquire(xid(2), table(1), AccessShare).unwrap();

    // XID 1 upgrades AS -> AE: conflicts with XID 2's AS, so it queues while
    // KEEPING its AccessShare grant (PG-style upgrade wait).
    let up = spawn_acquire(&lm, xid(1), table(1), AccessExclusive);
    wait_until("upgrade queued", || waiters_of(&lm, table(1)).len() == 1);
    assert!(
        lm.is_granted(xid(1), table(1), AccessShare),
        "old grant retained while waiting"
    );
    assert!(
        !lm.is_granted(xid(1), table(1), AccessExclusive),
        "upgrade not yet granted"
    );

    lm.release_all(xid(2));
    up.join().unwrap();
    assert_eq!(lm.held_by(xid(1)), vec![(table(1), AccessExclusive)]);
}

/// FIFO fairness / anti-starvation: while B is queued for AccessExclusive,
/// a later AccessShare arrival must NOT barge ahead even though AccessShare
/// is compatible with the current AccessShare grant. On release, the queued
/// head is granted first and the later reader waits behind it.
#[test]
fn test_fifo_fairness_no_barging() {
    let lm = Arc::new(LockManager::new());
    lm.acquire(xid(1), table(1), AccessShare).unwrap();

    // B queues for AccessExclusive (conflicts with A's AccessShare).
    let b = spawn_acquire(&lm, xid(2), table(1), AccessExclusive);
    wait_until("B queued", || waiters_of(&lm, table(1)).len() == 1);

    // C wants AccessShare: compatible with A's grant, but B is ahead — C
    // must queue behind B (this is the anti-starvation rule).
    let c = spawn_acquire(&lm, xid(3), table(1), AccessShare);
    wait_until("C queued behind B", || waiters_of(&lm, table(1)).len() == 2);
    assert_eq!(
        waiters_of(&lm, table(1)),
        vec![(xid(2), AccessExclusive), (xid(3), AccessShare)],
        "FIFO order: B ahead of C"
    );
    assert!(
        !lm.is_granted(xid(3), table(1), AccessShare),
        "C must not barge"
    );

    // A releases: B (head) is granted; C still waits because AccessShare
    // conflicts with B's fresh AccessExclusive.
    lm.release_all(xid(1));
    wait_until("B granted after A releases", || {
        lm.is_granted(xid(2), table(1), AccessExclusive)
    });
    assert!(
        !lm.is_granted(xid(3), table(1), AccessShare),
        "C waits behind B's grant"
    );

    // B releases: C finally proceeds.
    lm.release_all(xid(2));
    b.join().unwrap();
    c.join().unwrap();
    assert!(lm.is_granted(xid(3), table(1), AccessShare));
}

/// `release_all` re-grants all compatible CONSECUTIVE head waiters in FIFO
/// order: with the queue [RE, RE, AE] and an empty granted set after the
/// release, both RowExclusive readers proceed but the AccessExclusive behind
/// them does not.
#[test]
fn test_release_all_regrants_compatible_consecutive_heads() {
    let lm = Arc::new(LockManager::new());
    lm.acquire(xid(1), table(1), AccessExclusive).unwrap();

    let b = spawn_acquire(&lm, xid(2), table(1), RowExclusive);
    wait_until("B queued", || waiters_of(&lm, table(1)).len() == 1);
    let c = spawn_acquire(&lm, xid(3), table(1), RowExclusive);
    wait_until("C queued", || waiters_of(&lm, table(1)).len() == 2);
    let d = spawn_acquire(&lm, xid(4), table(1), AccessExclusive);
    wait_until("D queued", || waiters_of(&lm, table(1)).len() == 3);

    lm.release_all(xid(1));
    // B and C are granted (RE + RE compatible, consecutive heads); D stays
    // queued because AE conflicts with the RE grants in front of it.
    wait_until("B and C re-granted", || {
        lm.is_granted(xid(2), table(1), RowExclusive)
            && lm.is_granted(xid(3), table(1), RowExclusive)
    });
    assert_eq!(waiters_of(&lm, table(1)), vec![(xid(4), AccessExclusive)]);
    b.join().unwrap();
    c.join().unwrap();

    // Partial release does not unblock D while C's RE remains.
    lm.release_all(xid(2));
    thread::sleep(Duration::from_millis(50));
    assert!(!lm.is_granted(xid(4), table(1), AccessExclusive));

    lm.release_all(xid(3));
    d.join().unwrap();
    assert!(lm.is_granted(xid(4), table(1), AccessExclusive));
}

/// Locks are independent per table: a conflict on one table never blocks
/// acquisitions on another (single shared mutex, released during waits).
#[test]
fn test_tables_are_independent() {
    let lm = Arc::new(LockManager::new());
    lm.acquire(xid(1), table(1), AccessExclusive).unwrap();
    // Same XID, another table: granted immediately.
    lm.acquire(xid(1), table(2), AccessExclusive).unwrap();
    // Other XID, another table: granted immediately.
    assert!(lm.try_acquire(xid(2), table(3), AccessExclusive).unwrap());
    assert_eq!(lm.held_by(xid(1)).len(), 2);
}

/// `release_all` drops the whole per-xid footprint across tables, and empty
/// entries are removed from the map (no unbounded growth).
#[test]
fn test_release_all_covers_every_table() {
    let lm = LockManager::new();
    for t in 1..=5 {
        lm.acquire(xid(1), table(t), RowExclusive).unwrap();
    }
    assert_eq!(lm.held_by(xid(1)).len(), 5);
    lm.release_all(xid(1));
    assert!(lm.held_by(xid(1)).is_empty());
    for t in 1..=5 {
        assert_eq!(lm.table_lock_state(table(t)), None, "empty entry removed");
    }
}
