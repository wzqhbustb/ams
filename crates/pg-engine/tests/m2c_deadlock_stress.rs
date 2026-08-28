//! M2c Stage T acceptance (part 2): deadlock injection stress — randomly
//! constructed 2–4 transaction wait-for cycles, every one of which the
//! engine's deadlock detector (default 100ms tick) must break by aborting
//! exactly one victim, interleaved with acyclic control groups that must
//! NEVER produce a victim (no false positives).
//!
//! # Cycle construction (deterministic, not hoped-for)
//!
//! Each cycle iteration picks `k ∈ {2, 3, 4}` transactions and `k` distinct
//! rows of `dlock (id INT, v INT)` (indexed on `id`, so the victim's abort
//! also exercises the index-undo path). Transaction `j` UPDATEs row `j`
//! (acquires its `t_xmax` stamp), all `k` transactions rendezvous on a
//! barrier, then transaction `j` UPDATEs row `(j+1) mod k` — closing a
//! wait-for cycle of length exactly `k`. Because the barrier guarantees
//! every first update landed before any second update starts, the cycle is
//! certain; the only question the test leaves open is whether the detector
//! finds it.
//!
//! Assertions per cycle:
//!
//! - EXACTLY ONE transaction's second UPDATE fails with
//!   `EngineError::Heap(HeapError::DeadlockVictim)` within the bounded
//!   watchdog window (a missed detection hangs a worker → watchdog → test
//!   failure; a second victim means the detector over-kills → failure);
//! - the victim aborts cleanly; every other transaction's blocked UPDATE
//!   then proceeds and commits. NOTE (SI semantics): a participant that
//!   waited on a COMMITTER — not the victim — wakes to
//!   `TupleConcurrentlyUpdated` (§9.1 step 3, PG's "could not serialize
//!   access due to concurrent update"); it retries its row update in a
//!   fresh single-statement transaction, which cannot deadlock;
//! - no wait edges / active XIDs / victim flags leak out of the iteration.
//!
//! # Acyclic control groups (false-positive gate)
//!
//! Every odd iteration runs `k` transactions that all UPDATE the same two
//! rows in the SAME order (row a, then row b): the wait-for graph is a
//! chain, never a cycle, so a correct detector stays silent. Any victim in
//! a control group is a false positive and fails the test. This runs at
//! full concurrency (all k transactions racing), not serialized.
//!
//! # Iterations
//!
//! `M2C_DEADLOCK_ITERS` total iterations (half cycles, half controls).
//! CI default 50; the Stage T acceptance configuration is 1000 cycles, run
//! manually (~15-30 min at the default detector tick):
//!
//! ```sh
//! M2C_DEADLOCK_ITERS=2000 cargo test -p pg-engine --test m2c_deadlock_stress --release -- --nocapture
//! ```
//!
//! Acceptance: `cargo test -p pg-engine --test m2c_deadlock_stress`

use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{Datum, Engine, EngineConfig, EngineError, HeapError, QueryResult};
use tempfile::TempDir;

const ITERS_ENV: &str = "M2C_DEADLOCK_ITERS";

/// Rows in the `dlock` table; cycle rows are picked a stride apart so the
/// `k` rows of a cycle are always distinct.
const ROWS: i32 = 32;
/// Hard bound on one iteration (cycle or control). Detection is expected
/// within ~1-2 detector ticks (~200ms); 15s leaves generous CI headroom
/// while still failing — never hanging — on a detection regression.
const ITERATION_WATCHDOG: Duration = Duration::from_secs(15);
/// Loose upper bound on measured detection latency (cycle close → victim
/// interrupted). The acceptance figure is p99 ≤ 200ms; the stress gate
/// asserts a 2s worst case so a loaded CI runner does not flake, and
/// reports the observed max for the benchmark doc.
const LATENCY_BOUND: Duration = Duration::from_secs(2);

/// Outcome of one cycle participant's second UPDATE.
enum Outcome {
    /// The UPDATE went through; the transaction committed.
    Committed,
    /// The detector chose this transaction; it aborted cleanly.
    Victim,
}

fn is_deadlock_victim(r: &pg_engine::Result<QueryResult>) -> bool {
    matches!(r, Err(EngineError::Heap(HeapError::DeadlockVictim)))
}

/// Retry bound for the SI write-conflict path (see `run_cycle`); a cycle of
/// k resolves in at most k-1 retries, so 32 is unreachable for correct code.
const MAX_CONFLICT_RETRIES: usize = 32;

/// Is this the §9.1 step-3 "committed concurrent writer" error? Under SI a
/// waiter whose stamper COMMITTED (instead of aborting) wakes to
/// `TupleConcurrentlyUpdated` and must retry with a fresh snapshot — PG's
/// Repeatable Read reports the same condition as `could not serialize
/// access`. In a broken cycle this hits every participant that waited on a
/// COMMITTER (as opposed to the victim): its own txn is rolled back and
/// the row is re-updated in a fresh single-statement txn (which cannot
/// deadlock — it touches one row — so a `DeadlockVictim` here would be a
/// false positive).
fn update_with_conflict_retry(engine: &Engine, row: i32, v: i32) {
    let mut attempts = 0;
    loop {
        attempts += 1;
        assert!(
            attempts <= MAX_CONFLICT_RETRIES,
            "conflict retried too often"
        );
        let txn = engine.begin_txn().unwrap();
        let r = engine.exec(
            Some(&txn),
            &format!("UPDATE dlock SET v = {v} WHERE id = {row}"),
        );
        match r {
            Ok(QueryResult::Affected(1)) => {
                txn.commit().unwrap();
                return;
            }
            Err(EngineError::Heap(HeapError::TupleConcurrentlyUpdated(_))) => {
                txn.abort().unwrap();
            }
            other => panic!("retry UPDATE of row {row}: unexpected result: {other:?}"),
        }
    }
}

/// One deterministic wait-for cycle of `k` transactions over `rows`.
/// Returns the detection latency (cycle close → victim interrupted).
fn run_cycle(engine: &Arc<Engine>, rows: &[i32], iter: u64) -> Duration {
    let k = rows.len();
    let barrier = Arc::new(Barrier::new(k));
    let (tx, rx) = mpsc::channel();
    let mut closed_at = None;
    for (j, &row) in rows.iter().enumerate() {
        let engine = Arc::clone(engine);
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        let next = rows[(j + 1) % k];
        thread::spawn(move || {
            let txn = engine.begin_txn().unwrap();
            // First update: distinct row per transaction, never blocks.
            engine
                .exec(
                    Some(&txn),
                    &format!("UPDATE dlock SET v = {} WHERE id = {row}", iter as i32),
                )
                .unwrap();
            // Rendezvous: every first update has landed, so the second
            // updates close the cycle for certain.
            barrier.wait();
            let r = engine.exec(
                Some(&txn),
                &format!("UPDATE dlock SET v = {} WHERE id = {next}", iter as i32 + 1),
            );
            let outcome = if is_deadlock_victim(&r) {
                txn.abort().unwrap();
                Outcome::Victim
            } else {
                match r {
                    Ok(QueryResult::Affected(1)) => {
                        txn.commit().unwrap();
                        Outcome::Committed
                    }
                    // Waited on a COMMITTER (the chain left after the
                    // victim's abort): PG-at-SI write conflict — retry
                    // fresh.
                    Err(EngineError::Heap(HeapError::TupleConcurrentlyUpdated(_))) => {
                        txn.abort().unwrap();
                        update_with_conflict_retry(&engine, next, iter as i32 + 1);
                        Outcome::Committed
                    }
                    other => panic!("participant {j}: unexpected UPDATE result: {other:?}"),
                }
            };
            tx.send(outcome).unwrap();
        });
        // The cycle closes when the LAST participant starts its second
        // update — approximately when the barrier opens.
        if j == k - 1 {
            closed_at = Some(Instant::now());
        }
    }

    let mut victims = 0usize;
    let mut latency = Duration::ZERO;
    for _ in 0..k {
        match rx.recv_timeout(ITERATION_WATCHDOG) {
            Ok(Outcome::Victim) => {
                victims += 1;
                latency = closed_at.unwrap().elapsed();
            }
            Ok(Outcome::Committed) => {}
            Err(e) => panic!("cycle participant did not finish within {ITERATION_WATCHDOG:?} — deadlock detection regression?: {e}"),
        }
    }
    assert_eq!(
        victims, 1,
        "cycle of {k} must produce EXACTLY ONE victim, got {victims}"
    );
    assert!(
        latency <= LATENCY_BOUND,
        "detection latency {latency:?} exceeds the {LATENCY_BOUND:?} stress bound"
    );
    latency
}

/// Acyclic control: `k` transactions all UPDATE row `a` then row `b` (same
/// order) — a wait chain, never a cycle. Any victim is a false positive.
fn run_acyclic_control(engine: &Arc<Engine>, k: usize, a: i32, b: i32, iter: u64) {
    let barrier = Arc::new(Barrier::new(k));
    let (tx, rx) = mpsc::channel();
    for _ in 0..k {
        let engine = Arc::clone(engine);
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        thread::spawn(move || {
            // Race for row `a`, then `b` (same order for everyone — a wait
            // CHAIN, never a cycle). On an SI write conflict (waited on a
            // committer) retry the pair in a fresh txn; a DeadlockVictim
            // anywhere is a false positive and is reported as such.
            barrier.wait();
            let mut attempts = 0;
            let victim = loop {
                attempts += 1;
                assert!(
                    attempts <= MAX_CONFLICT_RETRIES,
                    "conflict retried too often"
                );
                let txn = engine.begin_txn().unwrap();
                let r1 = engine.exec(
                    Some(&txn),
                    &format!("UPDATE dlock SET v = {} WHERE id = {a}", iter as i32),
                );
                if is_deadlock_victim(&r1) {
                    txn.abort().unwrap();
                    break true;
                }
                let r2 = match r1 {
                    Ok(_) => engine.exec(
                        Some(&txn),
                        &format!("UPDATE dlock SET v = {} WHERE id = {b}", iter as i32 + 1),
                    ),
                    Err(e) => Err(e),
                };
                if is_deadlock_victim(&r2) {
                    txn.abort().unwrap();
                    break true;
                }
                match r2 {
                    Ok(QueryResult::Affected(1)) => {
                        txn.commit().unwrap();
                        break false;
                    }
                    Err(EngineError::Heap(HeapError::TupleConcurrentlyUpdated(_))) => {
                        txn.abort().unwrap();
                    }
                    other => panic!("control: unexpected UPDATE result: {other:?}"),
                }
            };
            tx.send(victim).unwrap();
        });
    }
    let mut victims = 0usize;
    for _ in 0..k {
        match rx.recv_timeout(ITERATION_WATCHDOG) {
            Ok(true) => victims += 1,
            Ok(false) => {}
            Err(e) => panic!("control participant stuck within {ITERATION_WATCHDOG:?}: {e}"),
        }
    }
    assert_eq!(
        victims, 0,
        "FALSE POSITIVE: acyclic control group produced {victims} deadlock victim(s)"
    );
}

/// The Stage T deadlock-injection acceptance: `M2C_DEADLOCK_ITERS`
/// iterations (default 50), alternating real cycles (must be detected,
/// exactly one victim each) and acyclic controls (must NOT be detected).
#[test]
fn m2c_deadlock_injection_stress() {
    let iters: u64 = std::env::var(ITERS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine
        .exec(None, "CREATE TABLE dlock (id INT, v INT)")
        .unwrap();
    let preload = engine.begin_txn().unwrap();
    for id in 0..ROWS {
        engine
            .exec(
                Some(&preload),
                &format!("INSERT INTO dlock VALUES ({id}, 0)"),
            )
            .unwrap();
    }
    preload.commit().unwrap();
    engine.create_index("dlock", "id").unwrap();

    let mut cycles = 0u64;
    let mut controls = 0u64;
    let mut max_latency = Duration::ZERO;
    for iter in 0..iters {
        let k = (2 + iter % 3) as usize; // 2, 3, 4
        if iter % 2 == 0 {
            // Distinct rows: base + j*stride (mod ROWS), stride = ROWS/4.
            let base = (iter as i32 * 7) % ROWS;
            let stride = ROWS / 4;
            let rows: Vec<i32> = (0..k as i32).map(|j| (base + j * stride) % ROWS).collect();
            let latency = run_cycle(&engine, &rows, iter);
            max_latency = max_latency.max(latency);
            cycles += 1;
        } else {
            let a = (iter as i32 * 5) % ROWS;
            let b = (a + 1) % ROWS;
            run_acyclic_control(&engine, k, a, b, iter);
            controls += 1;
        }
        // No leakage may accumulate across iterations.
        assert!(
            engine.txn_manager().wait_edges().is_empty(),
            "iter {iter}: leaked wait edges: {:?}",
            engine.txn_manager().wait_edges()
        );
        assert!(
            engine.txn_manager().active_xids().is_empty(),
            "iter {iter}: leaked active XIDs"
        );
        assert!(
            engine.txn_manager().deadlock_victims().marked().is_empty(),
            "iter {iter}: leaked victim flags"
        );
    }

    eprintln!(
        "m2c_deadlock_stress: {cycles} cycles injected and detected (exactly one victim each), \
         {controls} acyclic controls with zero false positives, max detection latency {max_latency:?}"
    );

    // The table is intact: every row still resolves through heap and index.
    let res = engine.exec(None, "SELECT * FROM dlock").unwrap();
    let QueryResult::Rows { rows, .. } = res else {
        panic!("final scan returned {res:?}");
    };
    assert_eq!(rows.len(), ROWS as usize);
    for id in [0i32, ROWS / 2, ROWS - 1] {
        assert!(
            engine
                .index_lookup("dlock", "id", &Datum::Int4(id))
                .unwrap()
                .is_some(),
            "row {id} not reachable through the index"
        );
    }
    engine.shutdown();
}
