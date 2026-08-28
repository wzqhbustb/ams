//! Stage Q end-to-end seam test (review T1): an INDEXED table under mixed
//! concurrent DML — inserts, updates, deletes, random aborts — with
//! constant B+Tree splits and a background thread running periodic
//! checkpoints. Afterwards the index is validated structurally and fully
//! cross-checked against the heap: every visible row's key resolves via
//! `index_lookup`, and every lookup's TID points back at the visible row.
//!
//! Also covers review M3: a dedicated thread repeatedly deletes rows in an
//! explicit transaction and ABORTS it — the abort-time index-undo
//! re-insert (with its independent large restart budget) must restore
//! those rows' index entries even under split pressure.
//!
//! CI-friendly by default (~15s); scale with M2Q_E2E_* env vars for soak.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};

use tempfile::TempDir;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Full-name so the test file stays self-contained.
fn run_with_watchdog<F>(name: &str, timeout: Duration, f: F)
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let name = name.to_string();
    thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(timeout) {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{name}: deadlocked or ran too long"),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{name}: a worker thread panicked (see above)")
        }
    }
}

/// Deterministic per-thread PRNG (xorshift64) — no external deps.
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

#[test]
fn indexed_table_concurrent_dml_aborts_and_checkpoints() {
    let writers = env_usize("M2Q_E2E_WRITERS", 6);
    let ops = env_usize("M2Q_E2E_OPS", 120);
    let preloaded = env_usize("M2Q_E2E_PRELOAD", 600);

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine.exec(None, "CREATE TABLE t (id INT, v INT)").unwrap();
    engine.create_index("t", "id").unwrap();
    // Committed pre-existing rows (the M3 delete-abort thread's targets).
    for i in 0..preloaded as i32 {
        engine
            .exec(None, &format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let engine2 = Arc::clone(&engine);
    let stop2 = Arc::clone(&stop);
    run_with_watchdog("e2e indexed dml", Duration::from_secs(600), move || {
        // Checkpoint thread: keeps the fuzzy-checkpoint / split interleave
        // (and the A3 quiescent-crash-point gap) covered. Joined LAST, after
        // the stop flag is raised once the DML workers finish.
        let stop_flag = Arc::clone(&stop2);
        let checkpoint_handle = {
            let engine = Arc::clone(&engine2);
            let stop = Arc::clone(&stop2);
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    engine.checkpoint().unwrap();
                    thread::sleep(Duration::from_millis(25));
                }
            })
        };

        let mut handles = Vec::new();

        // Mixed-DML writers: disjoint key ranges; insert / update / delete
        // with occasional explicit-txn aborts.
        for w in 0..writers {
            let engine = Arc::clone(&engine2);
            handles.push(thread::spawn(move || {
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (w as u64 + 1));
                let base = (preloaded + w * ops * 4) as i32;
                let mut live: Vec<i32> = Vec::new();
                for i in 0..ops {
                    let roll = rng.next() % 100;
                    if roll < 55 || live.is_empty() {
                        // Insert (12% inside an aborted txn).
                        let id = base + (i * 4) as i32;
                        if rng.next() % 100 < 12 {
                            let txn = engine.begin_txn().unwrap();
                            engine
                                .exec(Some(&txn), &format!("INSERT INTO t VALUES ({id}, {id})"))
                                .unwrap();
                            txn.abort().unwrap();
                        } else {
                            engine
                                .exec(None, &format!("INSERT INTO t VALUES ({id}, {id})"))
                                .unwrap();
                            live.push(id);
                        }
                    } else if roll < 80 {
                        // Update a live key (index maintenance = delete+insert).
                        let idx = (rng.next() as usize) % live.len();
                        let id = live[idx];
                        engine
                            .exec(
                                None,
                                &format!("UPDATE t SET v = {} WHERE id = {id}", id + 1),
                            )
                            .unwrap();
                    } else {
                        // Delete a live key (committed).
                        let idx = (rng.next() as usize) % live.len();
                        let id = live.swap_remove(idx);
                        engine
                            .exec(None, &format!("DELETE FROM t WHERE id = {id}"))
                            .unwrap();
                    }
                }
            }));
        }

        // M3 thread: delete preloaded rows in an explicit txn, then ABORT —
        // the index-undo re-insert must restore the entries under splits.
        {
            let engine = Arc::clone(&engine2);
            handles.push(thread::spawn(move || {
                for round in 0..30 {
                    let id = (round * 7 % preloaded) as i32;
                    let txn = engine.begin_txn().unwrap();
                    engine
                        .exec(Some(&txn), &format!("DELETE FROM t WHERE id = {id}"))
                        .unwrap();
                    txn.abort().unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        // DML is done: stop the checkpoint loop and join it.
        stop_flag.store(true, Ordering::SeqCst);
        checkpoint_handle.join().unwrap();
    });
    stop.store(true, Ordering::SeqCst);
    // ---- final cross-check: heap scan vs index ----
    // B+Tree structural validation (quiescent now).
    engine.btree_index("t", "id").unwrap().validate().unwrap();

    let rows = engine.scan("t", None).unwrap();
    let mut by_id: HashMap<i32, HashSet<pg_engine::Tid>> = HashMap::new();
    for (tid, values) in &rows {
        let id = match &values[0] {
            Some(Datum::Int4(v)) => *v,
            other => panic!("unexpected id value: {other:?}"),
        };
        by_id.entry(id).or_default().insert(*tid);
    }
    // Every visible row's key resolves through the index to its own TID.
    for (id, tids) in &by_id {
        let found = engine
            .index_lookup("t", "id", &Datum::Int4(*id))
            .unwrap()
            .unwrap_or_else(|| panic!("visible row id={id} missing from the index"));
        assert!(
            tids.contains(&found),
            "index TID {found:?} for id={id} does not point at a visible row"
        );
    }
    // M3: the delete-aborted preloaded rows are visible and indexed.
    for round in 0..30usize {
        let id = (round * 7 % preloaded) as i32;
        assert!(
            by_id.contains_key(&id),
            "delete-aborted row {id} not visible"
        );
        assert!(
            engine
                .index_lookup("t", "id", &Datum::Int4(id))
                .unwrap()
                .is_some(),
            "delete-aborted row {id} lost its index entry"
        );
    }
    // Index scan (via the AM surface) agrees with the heap row count. Under
    // HOT an entry points at the chain ROOT, whose own version is invisible
    // once it has been updated, so entry TIDs are not compared against the
    // visible TIDs directly — the per-row `index_lookup` loop above already
    // checks that each entry resolves to its visible version. What must hold
    // here is the count: one live entry per visible row, no leaks.
    let index_rows = engine.btree_index("t", "id").unwrap();
    let all_entries = index_rows.range_scan(None, None).unwrap();
    let heap_tids: HashSet<pg_engine::Tid> = rows.iter().map(|(t, _)| *t).collect();
    assert_eq!(
        all_entries.len(),
        heap_tids.len(),
        "index and heap disagree on the visible row set"
    );

    // Silence unused-import warning for QueryResult in case exec arms change.
    let _ = QueryResult::Ok;
    engine.shutdown();
}
