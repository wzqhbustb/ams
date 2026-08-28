//! M2c Stage P acceptance: row-lock wait/wake protocol (tech-selection §9.1
//! step 5): `row_wait_registry` edges, `wait_for` blocking, and the `end_txn`
//! broadcast on commit AND abort.
//!
//! Acceptance: `cargo test -p pg-txn --test row_lock_wait_wake`

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_storage::error::Result;
use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_txn::{CommitWal, InMemoryClogAccessor, TxnError, TxnManager};

/// A no-op WAL: append/flush always succeed, so the manager can be driven
/// without touching disk (same pattern as the manager's own unit tests).
#[derive(Debug, Default)]
struct OkWal;

impl CommitWal for OkWal {
    fn append(&self, _record: WalRecord) -> Result<Lsn> {
        Ok(Lsn::FIRST)
    }

    fn flush_to(&self, _lsn: Lsn) -> Result<()> {
        Ok(())
    }
}

fn manager() -> Arc<TxnManager> {
    Arc::new(TxnManager::new(
        TxnIdClock::new(TxnId::FIRST),
        Arc::new(OkWal),
        Arc::new(InMemoryClogAccessor::new()),
    ))
}

fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Commit of the blocking transaction wakes the registered waiter; the
/// waiter clears its own registry edge on wake (§9.1: waiters clear their
/// own entries).
#[test]
fn test_wait_wakes_on_commit() {
    let mgr = manager();
    let blocker = mgr.begin_txn();
    let waiter = mgr.begin_txn();

    let mgr2 = Arc::clone(&mgr);
    let handle = thread::spawn(move || {
        mgr2.register_row_wait(waiter, blocker);
        mgr2.wait_for(waiter, blocker)
    });

    wait_until("wait edge registered", || {
        mgr.wait_edges() == vec![(waiter, blocker)]
    });
    assert!(mgr.active_xids().contains(&blocker));

    mgr.commit_txn(blocker).unwrap();
    handle.join().unwrap().unwrap();

    // The waiter cleared its own edge; nothing stale remains for Stage R.
    assert!(mgr.wait_edges().is_empty());
}

/// Abort of the blocking transaction wakes the waiter exactly like commit
/// does — the protocol re-reads the CLOG after the wake to learn the
/// outcome (§9.1 step 5e → restart from step 1).
#[test]
fn test_wait_wakes_on_abort() {
    let mgr = manager();
    let blocker = mgr.begin_txn();
    let waiter = mgr.begin_txn();

    let mgr2 = Arc::clone(&mgr);
    let handle = thread::spawn(move || {
        mgr2.register_row_wait(waiter, blocker);
        mgr2.wait_for(waiter, blocker)
    });

    wait_until("wait edge registered", || {
        mgr.wait_edges() == vec![(waiter, blocker)]
    });
    mgr.abort_txn(blocker).unwrap();
    handle.join().unwrap().unwrap();
    assert!(mgr.wait_edges().is_empty());
}

/// Waiting on an XID that has already terminated returns immediately
/// (predicate true on first check — no sleep, no wakeup needed).
#[test]
fn test_wait_for_already_terminated_returns_immediately() {
    let mgr = manager();
    let blocker = mgr.begin_txn();
    mgr.commit_txn(blocker).unwrap();

    let waiter = mgr.begin_txn();
    // Drive it on a thread with a join: immediate return is proven by the
    // join completing without any commit/abort happening afterwards.
    let mgr2 = Arc::clone(&mgr);
    let handle = thread::spawn(move || mgr2.wait_for(waiter, blocker));
    handle.join().unwrap().unwrap();
}

/// A transaction waiting on itself is a caller bug, not a schedulable
/// state: `wait_for` rejects it instead of deadlocking silently.
#[test]
fn test_self_wait_errors() {
    let mgr = manager();
    let xid = mgr.begin_txn();
    assert_eq!(mgr.wait_for(xid, xid), Err(TxnError::SelfWait(xid)));
}

/// N waiters parked on the same blocking XID all wake on its commit.
#[test]
fn test_many_waiters_all_wake() {
    const N: u64 = 8;
    let mgr = manager();
    let blocker = mgr.begin_txn();

    let mut handles = Vec::new();
    for _ in 0..N {
        let waiter = mgr.begin_txn();
        let mgr2 = Arc::clone(&mgr);
        handles.push(thread::spawn(move || {
            mgr2.register_row_wait(waiter, blocker);
            mgr2.wait_for(waiter, blocker)
        }));
    }

    wait_until("all edges registered", || {
        mgr.wait_edges().len() == N as usize
    });
    mgr.commit_txn(blocker).unwrap();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert!(mgr.wait_edges().is_empty());
}

/// `wait_edges` is the Stage-R-facing snapshot of the row-lock half of the
/// wait-for graph: it reflects register/unregister exactly, sorted.
#[test]
fn test_wait_edges_snapshot() {
    let mgr = manager();
    let b1 = mgr.begin_txn();
    let b2 = mgr.begin_txn();
    let w1 = mgr.begin_txn();
    let w2 = mgr.begin_txn();

    mgr.register_row_wait(w2, b1);
    mgr.register_row_wait(w1, b2);
    assert_eq!(
        mgr.wait_edges(),
        vec![(w1, b2), (w2, b1)],
        "sorted by waiter"
    );

    // Re-registering moves the edge (the §9.1 restart loop can re-block on
    // a different XID).
    mgr.register_row_wait(w1, b1);
    assert_eq!(mgr.wait_edges(), vec![(w1, b1), (w2, b1)]);

    mgr.unregister_row_wait(w2);
    assert_eq!(mgr.wait_edges(), vec![(w1, b1)]);
}

/// The M2c Stage P barrier sink must not change the commit/abort contract:
/// begin/commit/abort still drive the active set and CLOG as before, now
/// under the manager-internal read guard.
#[test]
fn test_commit_abort_still_work_with_internal_barrier() {
    let mgr = manager();
    let t1 = mgr.begin_txn();
    let t2 = mgr.begin_txn();
    mgr.commit_txn(t1).unwrap();
    mgr.abort_txn(t2).unwrap();
    assert!(mgr.active_xids().is_empty());

    // And a fresh transaction can begin immediately after (no lingering
    // barrier state).
    let t3 = mgr.begin_txn();
    mgr.commit_txn(t3).unwrap();
    assert!(mgr.active_xids().is_empty());
}

/// Concurrent commits racing row-lock waits: many waiters on many blockers,
/// committers and waiters interleaved — everyone wakes, no edge leaks.
#[test]
fn test_concurrent_waits_and_commits() {
    const PAIRS: u64 = 6;
    let mgr = manager();

    let mut handles = Vec::new();
    for _ in 0..PAIRS {
        let blocker = mgr.begin_txn();
        let waiter = mgr.begin_txn();
        let mgr2 = Arc::clone(&mgr);
        handles.push(thread::spawn(move || {
            mgr2.register_row_wait(waiter, blocker);
            mgr2.wait_for(waiter, blocker).unwrap();
            // The waiter proceeds after the wake and eventually terminates
            // too, so the active set drains completely.
            mgr2.commit_txn(waiter).unwrap();
        }));
        // Commit from yet another thread so the wake races the wait.
        let mgr3 = Arc::clone(&mgr);
        handles.push(thread::spawn(move || {
            mgr3.commit_txn(blocker).unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert!(mgr.wait_edges().is_empty());
    assert!(mgr.active_xids().is_empty());
}
