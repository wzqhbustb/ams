//! M2c Stage R acceptance: deadlock detection (tech-selection §9.3).
//!
//! Real `TxnManager` + `LockManager` + `DeadlockDetector` over a shared
//! victim registry; injected 2/3/4-transaction cycles must be detected with
//! the YOUNGEST transaction (max XID) chosen as victim, within ≤200ms at
//! the default 100ms tick. Positive assertions poll state instead of
//! sleeping; every blocking call is wrapped in a channel with a hard
//! timeout so a regression FAILS the test instead of hanging `cargo test`.
//!
//! Acceptance: `cargo test -p pg-txn --test deadlock_detection`

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_storage::error::Result;
use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::{Lsn, Oid, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_txn::{
    CommitWal, DeadlockDetector, DeadlockVictims, InMemoryClogAccessor, LockError, LockManager,
    LockMode, TxnError, TxnManager, DEFAULT_DEADLOCK_INTERVAL,
};

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

/// One detector test rig: manager + lock manager + detector sharing ONE
/// victim registry (the same wiring `Engine::open` performs).
struct Rig {
    mgr: Arc<TxnManager>,
    lm: Arc<LockManager>,
    victims: Arc<DeadlockVictims>,
    detector: DeadlockDetector,
}

fn rig(interval: Duration) -> Rig {
    let victims = Arc::new(DeadlockVictims::new());
    let mgr = Arc::new(
        TxnManager::new(
            TxnIdClock::new(TxnId::FIRST),
            Arc::new(OkWal),
            Arc::new(InMemoryClogAccessor::new()),
        )
        .with_deadlock_victims(Arc::clone(&victims)),
    );
    let lm = Arc::new(LockManager::new().with_deadlock_victims(Arc::clone(&victims)));
    let detector = DeadlockDetector::start(
        Arc::clone(&mgr),
        Arc::clone(&lm),
        Arc::clone(&victims),
        interval,
    );
    Rig {
        mgr,
        lm,
        victims,
        detector,
    }
}

/// Poll `pred` until it holds, failing after a deadline (no fixed sleeps
/// for positive assertions).
fn wait_until(what: &str, pred: impl FnMut() -> bool) {
    wait_until_within(what, Duration::from_secs(10), pred);
}

/// [`wait_until`] with an explicit deadline, for waits whose expected
/// duration exceeds the 10s default (e.g. the 120-tick measurement window).
fn wait_until_within(what: &str, deadline_after: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + deadline_after;
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

/// Run `f` (expected to block until the detector or a release unblocks it)
/// on a worker; receive its result with a hard timeout.
fn run_blocking<T>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> T
where
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(15))
        .unwrap_or_else(|e| panic!("{what}: still blocked after 15s: {e}"))
}

/// Stage R acceptance, 2/3/4-transaction rings: transaction `i` holds
/// `Exclusive` on table `i` and wants table `(i+1) % n`. The YOUNGEST (max
/// XID) must be chosen as victim and interrupted with
/// `LockError::DeadlockVictim` within 200ms of closing the ring; after its
/// (simulated) abort the rest of the ring acquires successfully in order.
fn run_table_ring(n: usize, interval: Duration) -> Duration {
    assert!(n >= 2);
    let rig = rig(interval);
    let tables: Vec<Oid> = (0..n).map(|i| Oid(10_000 + i as u64)).collect();
    let xids: Vec<TxnId> = (0..n).map(|_| rig.mgr.begin_txn()).collect();
    for i in 0..n {
        rig.lm
            .acquire(xids[i], tables[i], LockMode::Exclusive)
            .unwrap();
    }

    // Waiters 0..n-2 park on threads; the youngest closes the ring below.
    let mut handles = Vec::new();
    for i in 0..n - 1 {
        let lm = Arc::clone(&rig.lm);
        let (xid, want) = (xids[i], tables[(i + 1) % n]);
        handles.push(thread::spawn(move || {
            lm.acquire(xid, want, LockMode::Exclusive)
        }));
    }
    wait_until("all non-youngest waiters queued", || {
        rig.lm
            .table_lock_states()
            .iter()
            .map(|(_, s)| s.waiters.len())
            .sum::<usize>()
            == n - 1
    });

    // Close the ring with the youngest transaction and time the detection.
    let closed_at = Instant::now();
    let lm = Arc::clone(&rig.lm);
    let (youngest, want) = (xids[n - 1], tables[0]);
    let victim_result = run_blocking("youngest's ring-closing acquire", move || {
        lm.acquire(youngest, want, LockMode::Exclusive)
    });
    let latency = closed_at.elapsed();
    assert_eq!(
        victim_result,
        Err(LockError::DeadlockVictim(youngest)),
        "the youngest transaction of a {n}-txn ring must be the victim"
    );
    assert!(
        latency <= Duration::from_millis(200),
        "detection latency {latency:?} exceeds the 200ms bound (interval {interval:?})"
    );

    // Simulate the victim's abort (2PL release): the ring unblocks in
    // reverse dependency order — the waiter on the victim's table first.
    rig.lm.release_all(youngest);
    for (i, h) in handles.into_iter().enumerate().rev() {
        assert_eq!(
            h.join().unwrap(),
            Ok(()),
            "txn {i} must acquire once the victim released"
        );
        rig.lm.release_all(xids[i]);
    }

    // Hygiene: end every transaction; the victim's flag was consumed by its
    // own acquire, and nothing stale may remain.
    for &xid in &xids {
        rig.mgr.abort_txn(xid).unwrap();
    }
    assert!(
        rig.victims.marked().is_empty(),
        "victim flag must be consumed"
    );
    assert!(
        rig.lm.table_lock_states().is_empty(),
        "all locks released at the end"
    );
    assert_eq!(rig.detector.panic_count(), 0, "detector tick panicked");
    latency
}

/// Acceptance gate: 2-transaction table-lock cycle at the DEFAULT 100ms
/// tick — A holds t1 wants t2, B holds t2 wants t1, youngest (B) aborted.
#[test]
fn test_deadlock_2_txn_cycle() {
    let latency = run_table_ring(2, DEFAULT_DEADLOCK_INTERVAL);
    eprintln!("2-txn cycle detected in {latency:?}");
}

/// 3-transaction ring: t1→t2→t3→t1, youngest aborted.
#[test]
fn test_deadlock_3_txn_cycle() {
    let latency = run_table_ring(3, DEFAULT_DEADLOCK_INTERVAL);
    eprintln!("3-txn cycle detected in {latency:?}");
}

/// 4-transaction ring: t1→t2→t3→t4→t1, youngest aborted.
#[test]
fn test_deadlock_4_txn_cycle() {
    let latency = run_table_ring(4, DEFAULT_DEADLOCK_INTERVAL);
    eprintln!("4-txn cycle detected in {latency:?}");
}

/// Row-lock cycle at the unit level: two transactions registered in the
/// `row_wait_registry` against each other. The detector must interrupt the
/// younger one's `wait_for` with `TxnError::DeadlockVictim`; aborting the
/// victim then releases the elder.
#[test]
fn test_deadlock_row_lock_cycle() {
    let rig = rig(DEFAULT_DEADLOCK_INTERVAL);
    let a = rig.mgr.begin_txn();
    let b = rig.mgr.begin_txn(); // younger → victim
                                 // Inject the cycle's edges directly (the §9.1 5-step protocol would
                                 // register these under page latches; the detector only reads the
                                 // registry, so direct injection is equivalent at this level).
    rig.mgr.register_row_wait(a, b);
    rig.mgr.register_row_wait(b, a);

    let mgr = Arc::clone(&rig.mgr);
    let elder = thread::spawn(move || mgr.wait_for(a, b));

    let closed_at = Instant::now();
    let mgr = Arc::clone(&rig.mgr);
    let victim_result = run_blocking("younger's row-lock wait", move || mgr.wait_for(b, a));
    let latency = closed_at.elapsed();
    assert_eq!(victim_result, Err(TxnError::DeadlockVictim(b)));
    assert!(
        latency <= Duration::from_millis(200),
        "row-cycle detection latency {latency:?} exceeds 200ms"
    );
    eprintln!("row-lock cycle detected in {latency:?}");

    // The victim's abort wakes the elder normally.
    rig.mgr.abort_txn(b).unwrap();
    assert_eq!(elder.join().unwrap(), Ok(()));
    assert!(rig.mgr.wait_edges().is_empty(), "both edges cleared");
    assert!(rig.victims.marked().is_empty());
    rig.mgr.abort_txn(a).unwrap();
}

/// Mixed cycle: a ROW edge (a → b) plus a TABLE edge (b → a) in one ring.
/// The graph builder merges both sources, so the cycle is found and the
/// younger (b, parked in `LockManager::acquire`) is the victim.
#[test]
fn test_deadlock_mixed_row_and_table_cycle() {
    const TABLE: Oid = Oid(20_000);
    let rig = rig(DEFAULT_DEADLOCK_INTERVAL);
    let a = rig.mgr.begin_txn();
    let b = rig.mgr.begin_txn(); // younger → victim
    rig.lm.acquire(a, TABLE, LockMode::Exclusive).unwrap();
    rig.mgr.register_row_wait(a, b); // row edge a → b

    let mgr = Arc::clone(&rig.mgr);
    let elder = thread::spawn(move || mgr.wait_for(a, b));

    // b queues on a's table: table edge b → a closes the ring.
    let closed_at = Instant::now();
    let lm = Arc::clone(&rig.lm);
    let victim_result = run_blocking("mixed-cycle table wait", move || {
        lm.acquire(b, TABLE, LockMode::Exclusive)
    });
    let latency = closed_at.elapsed();
    assert_eq!(victim_result, Err(LockError::DeadlockVictim(b)));
    assert!(
        latency <= Duration::from_millis(200),
        "mixed-cycle detection latency {latency:?} exceeds 200ms"
    );

    // The victim's abort ends the row wait too (b leaves the active set).
    rig.mgr.abort_txn(b).unwrap();
    assert_eq!(elder.join().unwrap(), Ok(()));
    assert!(rig.victims.marked().is_empty());
    rig.lm.release_all(a);
    rig.mgr.abort_txn(a).unwrap();
}

/// No-deadlock case: an acyclic wait chain (table chain x3 → x2 → x1 plus a
/// row waiter on x1) must NEVER trigger the detector across many ticks; the
/// waits then resolve normally in dependency order. Guards against false
/// positives in a benign concurrent workload.
#[test]
fn test_no_deadlock_acyclic_waits_do_not_fire() {
    let rig = rig(Duration::from_millis(10));
    let tables: Vec<Oid> = (0..3).map(|i| Oid(30_000 + i)).collect();
    let xids: Vec<TxnId> = (0..3).map(|_| rig.mgr.begin_txn()).collect();
    for i in 0..3 {
        rig.lm
            .acquire(xids[i], tables[i], LockMode::Exclusive)
            .unwrap();
    }
    let row_waiter = rig.mgr.begin_txn();
    rig.mgr.register_row_wait(row_waiter, xids[0]);

    let (x0, t0) = (xids[0], tables[0]);
    let (x1, t1) = (xids[1], tables[1]);
    let x2 = xids[2];
    let lm = Arc::clone(&rig.lm);
    let h2 = thread::spawn(move || lm.acquire(x1, t0, LockMode::Exclusive));
    let lm = Arc::clone(&rig.lm);
    let h3 = thread::spawn(move || lm.acquire(x2, t1, LockMode::Exclusive));
    let mgr = Arc::clone(&rig.mgr);
    let h_row = thread::spawn(move || mgr.wait_for(row_waiter, x0));

    wait_until("chain waits queued", || {
        rig.lm
            .table_lock_states()
            .iter()
            .map(|(_, s)| s.waiters.len())
            .sum::<usize>()
            == 2
            && rig.mgr.wait_edges() == vec![(row_waiter, xids[0])]
    });

    // Benign window: ~30 ticks against a populated but acyclic graph — no
    // victim may ever be marked.
    for _ in 0..30 {
        thread::sleep(Duration::from_millis(10));
        assert!(
            rig.victims.marked().is_empty(),
            "detector fired on an acyclic wait graph (false positive)"
        );
    }

    // Resolve in dependency order; everyone must succeed, nobody a victim.
    rig.mgr.commit_txn(xids[0]).unwrap();
    rig.lm.release_all(xids[0]);
    assert_eq!(h_row.join().unwrap(), Ok(()));
    assert_eq!(h2.join().unwrap(), Ok(()));
    rig.lm.release_all(xids[1]);
    assert_eq!(h3.join().unwrap(), Ok(()));
    rig.lm.release_all(xids[2]);
    rig.mgr.commit_txn(xids[1]).unwrap();
    rig.mgr.commit_txn(xids[2]).unwrap();
    rig.mgr.commit_txn(row_waiter).unwrap();
    assert_eq!(rig.detector.panic_count(), 0);
}

/// Victim-flag lifecycle races (module docs on `deadlock`): marking must be
/// idempotent and stale flags must never outlive their transaction.
#[test]
fn test_victim_flag_lifecycle_races() {
    let rig = rig(Duration::from_millis(10));

    // Case 1: marked, then the victim ends WITHOUT ever waiting — end_txn
    // clears the stale flag; no panic, no leak.
    let ended = rig.mgr.begin_txn();
    rig.victims.mark(ended);
    rig.mgr.abort_txn(ended).unwrap();
    assert!(
        !rig.victims.is_marked(ended),
        "end_txn must clear a stale victim flag"
    );

    // Case 2: the mark races the blocking transaction's commit — the wait
    // ends either with the victim error (flag consumed) or normally; never
    // a panic, never a hang, and the flag is gone once the waiter ends.
    let blocker = rig.mgr.begin_txn();
    let waiter = rig.mgr.begin_txn();
    rig.mgr.register_row_wait(waiter, blocker);
    let mgr = Arc::clone(&rig.mgr);
    let h = thread::spawn(move || mgr.wait_for(waiter, blocker));
    rig.victims.mark(waiter);
    rig.mgr.commit_txn(blocker).unwrap();
    match h.join().unwrap() {
        Ok(()) | Err(TxnError::DeadlockVictim(_)) => {}
        other => panic!("unexpected wait outcome: {other:?}"),
    }
    rig.mgr.commit_txn(waiter).unwrap();
    assert!(
        rig.victims.marked().is_empty(),
        "flag consumed or cleared at end_txn"
    );

    // Case 3: a flag whose XID is not active at all (the mark-after-clear
    // race from the module docs) is pruned by the detector's next tick.
    rig.victims.mark(TxnId(999_999));
    wait_until("detector prunes the inactive XID's flag", || {
        rig.victims.marked().is_empty()
    });
}

/// Stage R performance acceptance: tick p99 ≤ 5ms and detector CPU < 1%.
///
/// Method: run the detector at the PRODUCTION 100ms tick for 120 ticks
/// (~12s) against a populated (acyclic) wait graph, so each tick does real
/// graph work and the CPU budget is measured at the production rate
/// directly (an earlier revision measured at a 10ms tick with 10×
/// headroom, which left the 1% margin thin enough to flap under parallel
/// test load). The detector records the wall-clock duration of every tick
/// body; p99 uses the NEAREST-RANK formula `samples[ceil(N*99/100) - 1]`
/// over the sorted samples — with N = 120 that is the 119th sample, so a
/// single scheduling-outlier tick cannot flap the assertion (with N ≤ 100
/// nearest-rank p99 degenerates to the max, which a descheduled-mid-tick
/// outlier would break). The CPU proxy is `sum(tick durations) / wall
/// time` — the fraction of time the detector thread is NOT sleeping — with
/// the wall window starting BEFORE the detector is spawned, so every
/// recorded tick falls inside it.
#[test]
fn test_detector_tick_latency_and_cpu_budget() {
    let started = Instant::now();
    let rig = rig(DEFAULT_DEADLOCK_INTERVAL);

    // Populated acyclic graph: table chain + row edge (resolved at the end).
    let tables: Vec<Oid> = (0..2).map(|i| Oid(40_000 + i)).collect();
    let xids: Vec<TxnId> = (0..2).map(|_| rig.mgr.begin_txn()).collect();
    rig.lm
        .acquire(xids[0], tables[0], LockMode::Exclusive)
        .unwrap();
    rig.lm
        .acquire(xids[1], tables[1], LockMode::Exclusive)
        .unwrap();
    rig.mgr.register_row_wait(xids[1], xids[0]);
    let (x1, t0) = (xids[1], tables[0]);
    let lm = Arc::clone(&rig.lm);
    let blocked = thread::spawn(move || lm.acquire(x1, t0, LockMode::Exclusive));
    wait_until("load waits queued", || {
        !rig.mgr.wait_edges().is_empty()
            && rig
                .lm
                .table_lock_states()
                .iter()
                .map(|(_, s)| s.waiters.len())
                .sum::<usize>()
                == 1
    });

    const TICKS: u64 = 120;
    // ~12s of ticks at the 100ms interval; the deadline must clear that.
    wait_until_within("120 detector ticks", Duration::from_secs(30), || {
        rig.detector.tick_count() >= TICKS
    });
    let wall = started.elapsed();

    let mut durations = rig.detector.tick_durations();
    assert!(
        durations.len() >= TICKS as usize,
        "every completed tick records a duration"
    );
    durations.sort_unstable();
    // Nearest-rank p99 (see the fn doc for why N > 100 matters).
    let p99 = durations[(durations.len() * 99).div_ceil(100) - 1];
    assert!(
        p99 <= Duration::from_millis(5),
        "tick p99 {p99:?} exceeds 5ms"
    );
    let busy: Duration = durations.iter().sum();
    assert!(
        busy * 100 < wall,
        "detector CPU budget exceeded: busy {busy:?} of {wall:?} wall (>= 1%)"
    );
    assert_eq!(rig.detector.panic_count(), 0);
    eprintln!(
        "tick stats over {} ticks: p99 = {p99:?}, busy = {busy:?} of {wall:?} wall",
        durations.len()
    );

    // stop() joins the thread promptly; the tick count must freeze.
    rig.detector.stop();
    rig.detector.stop(); // idempotent
    let frozen = rig.detector.tick_count();
    thread::sleep(Duration::from_millis(150)); // > one 100ms interval
    assert_eq!(rig.detector.tick_count(), frozen, "ticks after stop()");

    // Resolve the load cleanly.
    rig.mgr.commit_txn(xids[0]).unwrap();
    rig.lm.release_all(xids[0]);
    assert_eq!(blocked.join().unwrap(), Ok(()));
    rig.lm.release_all(xids[1]);
    rig.mgr.commit_txn(xids[1]).unwrap();
}

/// F1 regression: `interval = 0` must NOT busy-loop — `DeadlockDetector::start`
/// clamps it to a 1ms floor. Verified by tick spacing (10 ticks need ≥ 9
/// clamped intervals; a free-running loop would finish them in microseconds),
/// a clean stop, and zero tick panics.
#[test]
fn test_zero_interval_is_clamped_not_busy_looping() {
    let rig = rig(Duration::ZERO);
    let started = Instant::now();
    wait_until("10 detector ticks", || rig.detector.tick_count() >= 10);
    let wall = started.elapsed();
    assert!(
        wall >= Duration::from_micros(8_500),
        "10 ticks in {wall:?} — interval not clamped to the 1ms floor?"
    );
    assert_eq!(rig.detector.panic_count(), 0);

    // stop() is prompt and clean even at the clamped 1ms cadence.
    rig.detector.stop();
    let frozen = rig.detector.tick_count();
    thread::sleep(Duration::from_millis(10)); // ≫ clamped 1ms interval
    assert_eq!(rig.detector.tick_count(), frozen, "ticks after stop()");
}

/// Shared-victim double ring: A↔C (pure table locks) and B↔C (table edge
/// B→C plus an injected row edge C→B) coexist; BOTH rings' youngest member
/// is C. The detector must mark C — and only C (the `is_marked` skip branch
/// keeps the second ring in the same tick from re-marking); A and B are
/// never victims and their acquires succeed once C's abort releases.
#[test]
fn test_shared_victim_double_ring() {
    const T_A: Oid = Oid(50_000);
    const T_B: Oid = Oid(50_001);
    const T_C: Oid = Oid(50_002);
    let rig = rig(Duration::from_millis(10));
    let a = rig.mgr.begin_txn();
    let b = rig.mgr.begin_txn();
    let c = rig.mgr.begin_txn(); // youngest → shared victim of both rings
    assert!(a < b && b < c);

    rig.lm.acquire(a, T_A, LockMode::Exclusive).unwrap();
    rig.lm.acquire(b, T_B, LockMode::Exclusive).unwrap();
    rig.lm.acquire(c, T_C, LockMode::Exclusive).unwrap();

    // A queues on T_C, then B — with the ORDER made deterministic (spawn
    // order is NOT queue order: B's thread can win the race and queue
    // first, which would put B at the FIFO head; after C's abort B would
    // be granted first and A would wedge behind it).
    let lm = Arc::clone(&rig.lm);
    let ha = thread::spawn(move || lm.acquire(a, T_C, LockMode::Exclusive));
    wait_until("A queued on T_C", || {
        rig.lm
            .table_lock_state(T_C)
            .is_some_and(|s| s.waiters.len() == 1)
    });
    let lm = Arc::clone(&rig.lm);
    let hb = thread::spawn(move || lm.acquire(b, T_C, LockMode::Exclusive));
    wait_until("B queued behind A on T_C", || {
        rig.lm
            .table_lock_state(T_C)
            .is_some_and(|s| s.waiters.len() == 2 && s.waiters[0].0 == a && s.waiters[1].0 == b)
    });

    // C queues on T_A: ring 1 (A→C, C→A) closes. Parked C via channel so we
    // can poll the victim registry while it is blocked.
    let (tx, rx) = mpsc::channel();
    let lm = Arc::clone(&rig.lm);
    thread::spawn(move || {
        let _ = tx.send(lm.acquire(c, T_A, LockMode::Exclusive));
    });
    wait_until("C queued on T_A", || {
        rig.lm
            .table_lock_state(T_A)
            .is_some_and(|s| s.waiters.iter().any(|(x, _)| *x == c))
    });
    // Ring 2 (B→C via the T_C queue, C→B via an injected row edge — C's
    // parked acquire cannot also sit in wait_for, so the edge is registered
    // directly; the detector reads only the registry). NOW both rings are
    // complete, sharing victim C.
    rig.mgr.register_row_wait(c, b);

    // While C is blocked: only C may ever be marked, never A or B.
    let deadline = Instant::now() + Duration::from_secs(15);
    let victim_result = loop {
        match rx.try_recv() {
            Ok(r) => break r,
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => panic!("C's acquire thread died"),
        }
        assert!(Instant::now() < deadline, "C was not interrupted in time");
        let marked = rig.victims.marked();
        assert!(
            marked.is_empty() || marked == vec![c],
            "only the shared victim C may be marked: {marked:?}"
        );
        thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(
        victim_result,
        Err(LockError::DeadlockVictim(c)),
        "C (youngest, shared by both rings) must be the victim"
    );

    // C's abort: drop the injected row edge (the heap AM's error path does
    // this for real row waits), release the table locks, end the txn.
    rig.mgr.unregister_row_wait(c);
    rig.lm.release_all(c);
    rig.mgr.abort_txn(c).unwrap();

    // A (FIFO head on T_C) acquires first, then B after A releases; NEITHER
    // was ever a victim — their acquires return Ok.
    assert_eq!(ha.join().unwrap(), Ok(()), "A must not be a victim");
    rig.lm.release_all(a);
    assert_eq!(hb.join().unwrap(), Ok(()), "B must not be a victim");
    rig.lm.release_all(b);
    rig.mgr.abort_txn(a).unwrap();
    rig.mgr.abort_txn(b).unwrap();

    wait_until("victim flags drained", || rig.victims.marked().is_empty());
    assert!(rig.mgr.wait_edges().is_empty());
    assert!(rig.lm.table_lock_states().is_empty());
    assert_eq!(rig.detector.panic_count(), 0);
}

/// Tiny deterministic PRNG (xorshift64) — no `rand` dependency in pg-txn.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Churn soak-lite: 8 threads hammer a 4-table set with conflicting
/// `Exclusive` acquire pairs (two distinct tables per iteration, random
/// commit/abort), the detector ticking at 5ms, and one thread parked in a
/// row wait on a long-lived anchor txn so the registry always has an edge
/// to snapshot. Real cycles form and are broken (victim iterations observe
/// `DeadlockVictim` and roll back). After ~3s everything must drain: no
/// tick panic, no victim flags, no wait edges, no lock state, no active
/// XIDs.
#[test]
fn test_detector_churn_soak() {
    const THREADS: usize = 8;
    const TABLES: u64 = 4;
    const SOAK: Duration = Duration::from_secs(3);
    let rig = rig(Duration::from_millis(5));
    let tables: Vec<Oid> = (0..TABLES).map(Oid).collect();

    // The anchor stays active for the whole soak; one waiter parks on it,
    // keeping a persistent row edge in the graph, and must wake cleanly
    // when the anchor commits at the end. Reported through a channel so a
    // lost wakeup fails the test on a timeout instead of hanging it (this
    // module's hard-timeout rule for blocking calls).
    let anchor = rig.mgr.begin_txn();
    let anchor_waiter = rig.mgr.begin_txn();
    rig.mgr.register_row_wait(anchor_waiter, anchor);
    let mgr = Arc::clone(&rig.mgr);
    let (anchor_tx, anchor_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = anchor_tx.send(mgr.wait_for(anchor_waiter, anchor));
    });

    let stop = Arc::new(AtomicBool::new(false));
    // Workers report their iteration counts through this channel on exit:
    // if a detector regression wedges a worker in `acquire` forever, the
    // missing report trips the recv_timeout below and FAILS the test —
    // a plain `join().unwrap()` would hang `cargo test` instead.
    let (done_tx, done_rx) = mpsc::channel();
    for t in 0..THREADS as u64 {
        let mgr = Arc::clone(&rig.mgr);
        let lm = Arc::clone(&rig.lm);
        let tables = tables.clone();
        let stop = Arc::clone(&stop);
        let done_tx = done_tx.clone();
        thread::spawn(move || {
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (t + 1));
            let mut victim_aborts = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let x = mgr.begin_txn();
                let i = (rng.next() % TABLES) as usize;
                // A second, DISTINCT table.
                let j = (i + 1 + (rng.next() % (TABLES - 1)) as usize) % TABLES as usize;
                let first = lm.acquire(x, tables[i], LockMode::Exclusive);
                let victimed = match first {
                    Err(LockError::DeadlockVictim(_)) => true,
                    Ok(()) => {
                        matches!(
                            lm.acquire(x, tables[j], LockMode::Exclusive),
                            Err(LockError::DeadlockVictim(_))
                        )
                    }
                };
                if victimed {
                    victim_aborts += 1;
                }
                // Random roll-forward/back; locks always released (engine
                // order: durable end first, then the 2PL release).
                if rng.next() % 2 == 0 {
                    mgr.commit_txn(x).unwrap();
                } else {
                    mgr.abort_txn(x).unwrap();
                }
                lm.release_all(x);
            }
            let _ = done_tx.send(victim_aborts);
        });
    }
    drop(done_tx);

    thread::sleep(SOAK);
    stop.store(true, Ordering::Relaxed);
    let mut total_victims = 0u64;
    for _ in 0..THREADS {
        total_victims += done_rx.recv_timeout(Duration::from_secs(15)).expect(
            "a churn worker did not exit within 15s of the stop flag — \
             blocked in acquire (detector regression?)",
        );
    }

    // The anchor ends last, waking the parked row waiter normally (not as a
    // victim: the anchor never waits on anything, so no cycle through it).
    rig.mgr.commit_txn(anchor).unwrap();
    let anchor_result = anchor_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("anchor's row waiter did not wake within 15s of the commit");
    assert_eq!(anchor_result, Ok(()));
    rig.mgr.commit_txn(anchor_waiter).unwrap();

    // The detector must actually have fired during the soak: with 8 threads
    // cross-acquiring Exclusive locks on 4 tables for 3s, cycles are
    // constant — zero victims would mean detection silently no-oped.
    assert!(
        total_victims > 0,
        "soak produced no deadlock victims at all"
    );
    assert_eq!(rig.detector.panic_count(), 0, "detector tick panicked");
    eprintln!("churn soak: {total_victims} victim aborts across {THREADS} threads in {SOAK:?}");

    // Everything drains: flags (consumed / cleared at end_txn / pruned),
    // edges, lock tables, active XIDs.
    wait_until("victim flags drained", || rig.victims.marked().is_empty());
    assert!(rig.mgr.wait_edges().is_empty(), "row wait edges leaked");
    assert!(
        rig.lm.table_lock_states().is_empty(),
        "table lock state leaked"
    );
    assert!(rig.mgr.active_xids().is_empty(), "active XIDs leaked");
}
