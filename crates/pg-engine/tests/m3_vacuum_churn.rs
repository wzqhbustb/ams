//! M3 Stage D acceptance (part 3): churn — ROADMAP.md:216's core gate.
//!
//! A fixed-row-count table runs N rounds of "DELETE a batch + UPDATE a
//! batch + INSERT a batch", with `Engine::vacuum` every K rounds. The
//! assertions:
//!
//! - **bounded space**: the table's on-disk page count converges to a
//!   steady state (NOT linear in rounds), and the allocator's high-water
//!   mark grows at most at the btree's no-page-merge drift rate
//!   (measured ~0.18 page/round — a declared M3 residual) — heap freed
//!   pages and compacted space are REUSED by new inserts (a heap-reuse
//!   failure would add ~0.7 page/round of insert traffic and blow the
//!   rate bound immediately);
//! - **clean pass**: after every vacuum nothing RECLAIMABLE remains at
//!   the latest horizon — `collect_index_keys(scan_dead_tuples)` is empty.
//!   (The raw dead scan may still list partially-dead HOT chain prefixes:
//!   §4.2 deliberately never prunes them, so "scan_dead_tuples is empty"
//!   is not the achievable gate for a HOT-updating workload. The dead
//!   prefixes are bounded by the live-set size and are reclaimed whole
//!   once their row is deleted and the chain fully dies.);
//! - **crash-injected rounds**: two rounds replace the plain vacuum with
//!   crash injection — one lands in window ① (phase-④ index-cleanup WAL
//!   flushed, `reclaim` never ran, driven by hand exactly like
//!   `m3_vacuum_crash_windows.rs`), one crashes with an in-flight INSERT
//!   batch (crash-loser inserts: dangling index entries the post-recovery
//!   vacuum must actually remove — the Task-5 path inside churn). The
//!   churn then continues on the recovered engine.
//! - the final full scan equals the bookkeeping model exactly.
//!
//! The whole run executes inside ONE worker thread joined under a
//! watchdog: a regression FAILS, it never hangs (Stage T convention).
//!
//! Long soak (the steady-state growth-rate bound scales with the round
//! count, so the 200-round soak exercises the same gate):
//!
//! ```sh
//! M3_CHURN_ROUNDS=200 cargo test -p pg-engine --release --test m3_vacuum_churn -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use pg_am_btree::{encode_key, BTreeError};
use pg_am_heap::access_method::{RelationDesc, Vacuumable};
use pg_am_heap::tuple::ColumnType;
use pg_engine::{Datum, Engine, EngineConfig, TableEntry};
use pg_storage::types::Tid;
use tempfile::TempDir;

/// Watchdog for the whole churn run (debug-mode fsync latency included).
const WATCHDOG: Duration = Duration::from_secs(900);

const ROUNDS_ENV: &str = "M3_CHURN_ROUNDS";
const DEFAULT_ROUNDS: u32 = 30;
/// Vacuum cadence (every K rounds).
const VACUUM_EVERY: u32 = 5;
/// Fixed live-row count the churn converges around.
const LIVE_ROWS: i32 = 400;
/// Rows deleted / updated / inserted per round.
const BATCH: i32 = 40;
/// Row payload size: ~130B/row ⇒ ~60 rows/page, 400 live ≈ 7–8 pages.
const PAD: usize = 100;

fn open(dir: &Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn col_types_of(entry: &TableEntry) -> Vec<ColumnType> {
    entry.columns.iter().map(|c| c.col_type).collect()
}

fn relation_desc<'a>(entry: &TableEntry, col_types: &'a [ColumnType]) -> RelationDesc<'a> {
    RelationDesc {
        rel_oid: entry.oid,
        first_page: entry.first_page,
        columns: col_types,
    }
}

fn dead_now(engine: &Engine) -> Vec<Tid> {
    let entry = engine.describe_table("t").expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);
    engine
        .heap()
        .scan_dead_tuples(rel, engine.oldest_snapshot_xmin(), engine.clog().as_ref())
        .unwrap()
}

/// What a vacuum pass at the latest horizon could still reclaim:
/// `collect_index_keys`' output over the current dead set — standalone
/// dead tuples and fully-dead chain roots. Partially-dead HOT chain
/// members legitimately survive every vacuum (§4.2: no prune, no
/// redirect), so the post-vacuum gate is "nothing RECLAIMABLE remains",
/// NOT "the dead scan is empty" — this churn's HOT updates keep a bounded
/// set of dead chain prefixes resident until their rows are deleted.
fn reclaimable_now(engine: &Engine) -> usize {
    let entry = engine.describe_table("t").expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);
    engine
        .heap()
        .collect_index_keys(rel, &dead_now(engine))
        .unwrap()
        .len()
}

fn page_count(engine: &Engine) -> usize {
    let entry = engine.describe_table("t").expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);
    engine.heap().relation_pages(&rel).unwrap().len()
}

fn allocated_pages(engine: &Engine) -> u64 {
    engine.storage().page_allocator().lock().next_page_id().0
}

/// `t`'s live set as `k → v`, from a full scan.
fn live_set(engine: &Engine) -> BTreeMap<i32, i32> {
    engine
        .scan("t", None)
        .unwrap()
        .into_iter()
        .map(|(_, v)| match (&v[0], &v[1]) {
            (Some(Datum::Int4(k)), Some(Datum::Int4(v))) => (*k, *v),
            other => panic!("unexpected row: {other:?}"),
        })
        .collect()
}

/// One batched DELETE / UPDATE / INSERT group per transaction (one fsync
/// per batch, not per row — the churn is round-paced, not fsync-paced).
fn delete_batch(engine: &Engine, model: &mut BTreeMap<i32, i32>, batch: i32) {
    let keys: Vec<i32> = model.keys().take(batch as usize).copied().collect();
    if keys.is_empty() {
        return;
    }
    let txn = engine.begin_txn().unwrap();
    for k in &keys {
        let n = engine
            .exec(Some(&txn), &format!("DELETE FROM t WHERE k = {k}"))
            .unwrap();
        assert_eq!(n, pg_engine::QueryResult::Affected(1));
        model.remove(k);
    }
    txn.commit().unwrap();
}

fn update_batch(engine: &Engine, model: &mut BTreeMap<i32, i32>, batch: i32, round: u32) {
    // Update the `v` of every 7th live key (unindexed ⇒ HOT when it fits).
    let keys: Vec<i32> = model
        .keys()
        .skip(3)
        .step_by(7)
        .take(batch as usize)
        .copied()
        .collect();
    if keys.is_empty() {
        return;
    }
    let txn = engine.begin_txn().unwrap();
    for k in keys {
        let v = 1_000_000 + round as i32;
        let n = engine
            .exec(Some(&txn), &format!("UPDATE t SET v = {v} WHERE k = {k}"))
            .unwrap();
        assert_eq!(n, pg_engine::QueryResult::Affected(1));
        model.insert(k, v);
    }
    txn.commit().unwrap();
}

fn insert_batch(engine: &Engine, model: &mut BTreeMap<i32, i32>, next_key: &mut i32, batch: i32) {
    let txn = engine.begin_txn().unwrap();
    for _ in 0..batch {
        let k = *next_key;
        *next_key += 1;
        engine
            .exec(
                Some(&txn),
                &format!("INSERT INTO t VALUES ({k}, {k}, '{}')", "x".repeat(PAD)),
            )
            .unwrap();
        model.insert(k, k);
    }
    txn.commit().unwrap();
}

/// Crash-injection round, window ①: hand-drive `Engine::vacuum`'s phases
/// ②–④ (identical calls, identical order — see `m3_vacuum_crash_windows`),
/// flush, then kill -9 (`mem::forget`) BEFORE `reclaim` runs.
fn crash_in_window_1(engine: Engine) {
    let entry = engine.describe_table("t").expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);
    let dead = engine
        .heap()
        .scan_dead_tuples(rel, engine.oldest_snapshot_xmin(), engine.clog().as_ref())
        .unwrap();
    assert!(!dead.is_empty(), "crash round must have garbage to work on");
    let keys = engine.heap().collect_index_keys(rel, &dead).unwrap();
    for idx in engine
        .indexes()
        .into_iter()
        .filter(|e| e.table_oid == entry.oid)
    {
        let col_index = entry
            .columns
            .iter()
            .position(|c| c.name == idx.column)
            .expect("indexed column exists");
        let mut index = engine.btree_index("t", &idx.column).unwrap();
        for (tid, values) in &keys {
            let Some(datum) = &values[col_index] else {
                continue;
            };
            let key = encode_key(datum).unwrap();
            match index.delete(&key, *tid) {
                Ok(()) | Err(BTreeError::EntryNotFound) => {}
                Err(e) => panic!("phase-4 delete failed unexpectedly: {e}"),
            }
        }
    }
    engine.storage().wal_writer().flush().unwrap();
    std::mem::forget(engine); // kill -9: no HeapCleanup, no PageFree
}

/// Crash-injection round with an in-flight INSERT batch: the batch's heap
/// inserts and index entries are WAL-durable but the transaction never
/// commits — crash-loser inserts whose dangling index entries the
/// post-recovery vacuum must physically remove (Task 5 inside churn).
fn crash_with_inflight_inserts(engine: Engine, next_key: &mut i32, batch: i32) {
    let txn = engine.begin_txn().unwrap();
    for _ in 0..batch {
        let k = *next_key;
        *next_key += 1;
        engine
            .exec(
                Some(&txn),
                &format!("INSERT INTO t VALUES ({k}, {k}, '{}')", "L".repeat(PAD)),
            )
            .unwrap();
        // NOT recorded in the model: the batch never commits.
    }
    engine.storage().wal_writer().flush().unwrap();
    std::mem::forget(txn);
    std::mem::forget(engine); // kill -9 with the batch in flight
}

fn run_churn(dir: &Path, rounds: u32) {
    let mut engine = Some(open(dir));
    {
        let e = engine.as_ref().unwrap();
        e.exec(None, "CREATE TABLE t (k INT, v INT, pad TEXT)")
            .unwrap();
        e.create_index("t", "k").unwrap();
    }

    let mut model: BTreeMap<i32, i32> = BTreeMap::new();
    let mut next_key = 0i32;
    // Preload the fixed live set.
    insert_batch(
        engine.as_ref().unwrap(),
        &mut model,
        &mut next_key,
        LIVE_ROWS,
    );
    let alloc_after_preload = allocated_pages(engine.as_ref().unwrap());

    // The two crash rounds (deterministically placed inside the run).
    let crash_window_round = rounds / 3;
    let crash_inflight_round = 2 * rounds / 3;

    let mut pages_after_vacuum: Vec<usize> = Vec::new();
    let mut alloc_after_vacuum: Vec<u64> = Vec::new();
    let mut loser_entries_removed_total = 0usize;

    for round in 0..rounds {
        {
            let e = engine.as_ref().unwrap();
            delete_batch(e, &mut model, BATCH);
            update_batch(e, &mut model, BATCH, round);
            insert_batch(e, &mut model, &mut next_key, BATCH);
        }

        if round == crash_window_round {
            // Window ①: index cleanup durable, reclaim never ran.
            crash_in_window_1(engine.take().unwrap());
            engine = Some(open(dir));
            let e = engine.as_ref().unwrap();
            // Recovery replayed the phase-④ deletes; the heap is untouched.
            // The finishing vacuum re-collects and reclaims (EntryNotFound
            // tolerated), and the model is still exactly the live set.
            assert_eq!(&live_set(e), &model, "post-crash live set diverged");
            let stats = e.vacuum("t").unwrap();
            assert_eq!(
                reclaimable_now(e),
                0,
                "reclaimable garbage survives the window-① finish"
            );
            assert_eq!(
                stats.index_entries_removed, 0,
                "all pre-crash deletes replayed"
            );
            pages_after_vacuum.push(page_count(e));
            alloc_after_vacuum.push(allocated_pages(e));
            continue;
        }
        if round == crash_inflight_round {
            // In-flight INSERT batch crash: dangling loser entries.
            crash_with_inflight_inserts(engine.take().unwrap(), &mut next_key, BATCH);
            engine = Some(open(dir));
            let e = engine.as_ref().unwrap();
            // The losers are invisible; the committed model is intact.
            assert_eq!(&live_set(e), &model, "loser inserts leaked into the scan");
            let stats = e.vacuum("t").unwrap();
            // The aborted inserts are collected (rule 1) and their dangling
            // entries are the one thing phase ④ physically removes.
            assert_eq!(
                stats.index_entries_removed, BATCH as usize,
                "vacuum must remove the {BATCH} dangling loser entries: {stats:?}"
            );
            loser_entries_removed_total += stats.index_entries_removed;
            assert_eq!(reclaimable_now(e), 0);
            pages_after_vacuum.push(page_count(e));
            alloc_after_vacuum.push(allocated_pages(e));
            continue;
        }

        if (round + 1) % VACUUM_EVERY == 0 {
            let e = engine.as_ref().unwrap();
            e.vacuum("t").unwrap();
            assert_eq!(
                reclaimable_now(e),
                0,
                "round {round}: reclaimable garbage survives vacuum"
            );
            pages_after_vacuum.push(page_count(e));
            alloc_after_vacuum.push(allocated_pages(e));
        }
    }

    let e = engine.as_ref().unwrap();

    // Final exact-match: scan == model (count AND content).
    assert_eq!(&live_set(e), &model);
    assert!(loser_entries_removed_total > 0);

    // --- Bounded space (ROADMAP.md:216) ---
    //
    // Heap page count converges: the post-vacuum counts in the second half
    // of the run must not exceed the early steady state by more than a
    // small slack. Linear growth would produce ~1 page/round indefinitely.
    let half = pages_after_vacuum.len() / 2;
    let early_max = pages_after_vacuum[..half.max(1)]
        .iter()
        .max()
        .copied()
        .unwrap_or(0);
    let late_max = pages_after_vacuum[half..]
        .iter()
        .max()
        .copied()
        .unwrap_or(0);
    assert!(
        late_max <= early_max + 3,
        "heap page count did not converge: {pages_after_vacuum:?}"
    );
    assert!(
        late_max <= 30,
        "heap page count {late_max} far above the ~8-page live working set"
    );

    // Freelist/compaction reuse, data-file level: in the second half of
    // the run (steady state) the allocator high-water may only grow at
    // the index's split-drift rate (M3 has no btree page merge — a known,
    // documented residual; measured ~0.18 page/round). The bound is a
    // per-round RATE (≤ 0.25 page/round) plus a small absolute slack, so
    // it scales with the soak length instead of being calibrated to one.
    // Heap reuse failing would add ~0.7 page/round of insert traffic on
    // top and blow this bound immediately.
    let second_half_rounds = (rounds - rounds / 2) as u64;
    let steady_growth = alloc_after_vacuum.last().unwrap() - alloc_after_vacuum[half];
    assert!(
        steady_growth <= 8 + second_half_rounds / 4,
        "steady-state high-water grew {steady_growth} pages over {second_half_rounds} rounds \
         (> 0.25 page/round + slack; alloc series {alloc_after_vacuum:?}) — \
         heap space is not being reused"
    );
    let _ = alloc_after_preload;

    e.shutdown();
}

/// Extract the real message from a worker's panic payload (`panic!` with
/// a formatted message yields `String`/`&str`; anything else falls back
/// to the Debug form).
fn panic_msg(e: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = e.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else {
        format!("{e:?}")
    }
}

#[test]
fn churn_fixed_row_count_space_bounded() {
    let rounds = std::env::var(ROUNDS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ROUNDS);
    assert!(rounds >= 6, "need enough rounds for two crash injections");

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let worker = thread::spawn(move || run_churn(&dir, rounds));

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(worker.join());
    });
    match rx.recv_timeout(WATCHDOG) {
        Ok(joined) => joined.unwrap_or_else(|e| panic!("churn worker panicked: {}", panic_msg(&e))),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("churn watchdog tripped after {WATCHDOG:?} (hang regression)")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("churn watchdog channel broke"),
    }
}
