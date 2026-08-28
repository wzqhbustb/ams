//! M2c Stage T acceptance (part 1): sustained N-connection mixed read/write
//! stress against an INDEXED table through the SQL `exec` API.
//!
//! # Workload
//!
//! `M2C_STRESS_CONNS` worker threads ("connections") run paced transactions
//! against `stress (id INT, v INT, pad TEXT)` with a B+Tree index on `id`, so
//! every insert/delete pays index maintenance (leaf splits included) and
//! updates of the unindexed `v` take the HOT path when they fit. Each
//! connection owns a disjoint `id` range, so the run is contention-free by
//! construction: a `DeadlockVictim` / `TupleConcurrentlyUpdated` here is a
//! bug, not an expected outcome. The mix per iteration:
//!
//! - 40% INSERT (auto-commit), 25% UPDATE, 10% DELETE, 25% point SELECT
//!   whose result is checked against the connection's own bookkeeping
//!   (a wrong value = a visibility anomaly = test failure);
//! - every 4th iteration is an explicit multi-statement transaction
//!   (INSERT + SELECT ... FOR UPDATE + UPDATE, then COMMIT).
//!
//! A background thread checkpoints every 2s so the commit/checkpoint
//! barrier and CLOG flush run continuously under write load.
//!
//! # Rate pacing
//!
//! Simple sleep pacing: each connection gets one transaction slot per
//! `1 / M2C_STRESS_TPS` seconds; after finishing a transaction the worker
//! sleeps the remainder of the slot. A transaction slower than its slot
//! simply consumes the next one (no catch-up bursts). This is a target
//! rate, not a guarantee: if the machine cannot sustain it (e.g. the
//! per-commit fsync bound documented in `m2c_btree_tps.rs`), the achieved
//! rate floats below the target and the run is still valid — the
//! assertions are about correctness, not throughput.
//!
//! # Assertions (the Stage T "无 crash / 无丢失 / 无可见性错乱 / 无泄漏" gate)
//!
//! - no worker error, no panic, no watchdog trip (a lock/deadlock
//!   regression FAILS the test, it never hangs `cargo test`);
//! - no lost tuples: a full `SELECT *` must equal the merged per-connection
//!   bookkeeping exactly (count AND content);
//! - heap↔index consistency: `validate()` on the B+Tree, plus spot-checks —
//!   sampled live ids must resolve through `index_lookup`, sampled deleted
//!   ids must NOT (deletes remove the entry physically);
//! - no leaks at the end: no active XIDs, no wait edges, no stale deadlock
//!   victim flags.
//!
//! # Configurations
//!
//! CI default (`M2C_STRESS_SECS=30`, `M2C_STRESS_CONNS=16`,
//! `M2C_STRESS_TPS=100`) finishes in well under 2 minutes including the
//! final consistency checks. The Stage T acceptance configurations are
//! manual runs:
//!
//! ```sh
//! # 保底: 50 conn x 100 txn/s x 30min
//! M2C_STRESS_CONNS=50 M2C_STRESS_TPS=100 M2C_STRESS_SECS=1800 \
//!   cargo test -p pg-engine --test m2c_stress --release -- --nocapture
//! # 挑战: 100 conn x 100 txn/s x 60min
//! M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100 M2C_STRESS_SECS=3600 \
//!   cargo test -p pg-engine --test m2c_stress --release -- --nocapture
//! ```
//!
//! Acceptance: `cargo test -p pg-engine --test m2c_stress`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

const SECS_ENV: &str = "M2C_STRESS_SECS";
const CONNS_ENV: &str = "M2C_STRESS_CONNS";
const TPS_ENV: &str = "M2C_STRESS_TPS";

/// `id` range size per connection; disjoint ranges make the run
/// contention-free by construction.
const RANGE: i32 = 1_000_000;
/// Rows preloaded per connection so the first UPDATE/DELETE/SELECT have
/// targets.
const PRELOAD: i32 = 8;
/// Background checkpoint cadence.
const CHECKPOINT_EVERY: Duration = Duration::from_secs(2);
/// Watchdog margin on top of the configured run duration.
const WATCHDOG_MARGIN_SECS: u64 = 120;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// xorshift64* — deterministic per-connection PRNG (same construct as
/// `m2b_crash_rounds.rs`).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Per-connection bookkeeping returned when the worker exits. `model` maps
/// live `id` → expected `v`; it is the ground truth for both the in-run
/// visibility checks and the final full-scan comparison. `deleted` lists
/// the ids this connection deleted (ids are never reused: `next_id` only
/// advances), for the final index-absence spot-check.
struct WorkerOutcome {
    model: HashMap<i32, i32>,
    deleted: Vec<i32>,
    txns: u64,
    selects_checked: u64,
}

struct Worker {
    engine: Arc<Engine>,
    conn: usize,
    deadline: Instant,
    slot: Duration,
    rng: Rng,
    model: HashMap<i32, i32>,
    deleted: Vec<i32>,
    next_id: i32,
    txns: u64,
    selects_checked: u64,
}

impl Worker {
    fn new(engine: Arc<Engine>, conn: usize, secs: u64, tps: usize) -> Self {
        let base = conn as i32 * RANGE;
        let model = (0..PRELOAD).map(|i| (base + i, 0)).collect();
        Worker {
            engine,
            conn,
            deadline: Instant::now() + Duration::from_secs(secs),
            slot: Duration::from_secs_f64(1.0 / tps.max(1) as f64),
            rng: Rng(0x9E37_79B9_7F4A_7C15 ^ (conn as u64 + 1)),
            model,
            deleted: Vec::new(),
            next_id: base + PRELOAD,
            txns: 0,
            selects_checked: 0,
        }
    }

    /// Current value of a live row of THIS connection, checked against the
    /// bookkeeping — a mismatch is a visibility anomaly.
    fn select_check(&self, id: i32) -> Result<(), String> {
        let expected = self.model[&id];
        let res = self
            .engine
            .exec(None, &format!("SELECT v FROM stress WHERE id = {id}"))
            .map_err(|e| format!("conn {}: SELECT id={id} failed: {e}", self.conn))?;
        let QueryResult::Rows { rows, .. } = res else {
            return Err(format!(
                "conn {}: SELECT id={id} returned {res:?}",
                self.conn
            ));
        };
        if rows.len() != 1 {
            return Err(format!(
                "conn {}: SELECT id={id} returned {} rows (lost tuple?)",
                self.conn,
                rows.len()
            ));
        }
        match rows[0][0] {
            Some(Datum::Int4(v)) if v == expected => Ok(()),
            ref other => Err(format!(
                "conn {}: visibility anomaly on id={id}: expected v={expected}, got {other:?}",
                self.conn
            )),
        }
    }

    fn exec(&self, txn: Option<&pg_engine::TxnHandle>, sql: &str) -> Result<(), String> {
        self.engine
            .exec(txn, sql)
            .map(|_| ())
            .map_err(|e| format!("conn {}: `{sql}` failed: {e}", self.conn))
    }

    /// One auto-commit INSERT. Returns the new row's id.
    fn do_insert(&mut self) -> Result<i32, String> {
        let id = self.next_id;
        self.next_id += 1;
        self.exec(
            None,
            &format!("INSERT INTO stress VALUES ({id}, 0, 'xxxxxxxxxxxxxxxxxxxxxxxx')"),
        )?;
        self.model.insert(id, 0);
        Ok(id)
    }

    /// One auto-commit UPDATE of a random live row (HOT: only `v` changes).
    fn do_update(&mut self) -> Result<(), String> {
        let Some(id) = self.random_live() else {
            return Ok(());
        };
        let v = self.model[&id] + 1;
        self.exec(None, &format!("UPDATE stress SET v = {v} WHERE id = {id}"))?;
        self.model.insert(id, v);
        Ok(())
    }

    /// One auto-commit DELETE of a random live row.
    fn do_delete(&mut self) -> Result<(), String> {
        let Some(id) = self.random_live() else {
            return Ok(());
        };
        self.exec(None, &format!("DELETE FROM stress WHERE id = {id}"))?;
        self.model.remove(&id);
        self.deleted.push(id);
        Ok(())
    }

    /// One auto-commit point SELECT checked against the bookkeeping.
    fn do_select(&mut self) -> Result<(), String> {
        let Some(id) = self.random_live() else {
            return Ok(());
        };
        self.select_check(id)?;
        self.selects_checked += 1;
        Ok(())
    }

    /// Every 4th iteration: an explicit multi-statement transaction —
    /// INSERT + SELECT ... FOR UPDATE + UPDATE on the connection's own
    /// rows, then COMMIT. Bookkeeping is applied only after the commit
    /// lands.
    fn do_explicit_txn(&mut self) -> Result<(), String> {
        let txn = self
            .engine
            .begin_txn()
            .map_err(|e| format!("conn {}: begin failed: {e}", self.conn))?;
        let result = (|| {
            let id = self.next_id;
            self.exec(
                Some(&txn),
                &format!("INSERT INTO stress VALUES ({id}, 0, 'xxxxxxxxxxxxxxxxxxxxxxxx')"),
            )?;
            let target = self.random_live();
            if let Some(t) = target {
                let expected = self.model[&t];
                let res = self
                    .engine
                    .exec(
                        Some(&txn),
                        &format!("SELECT v FROM stress WHERE id = {t} FOR UPDATE"),
                    )
                    .map_err(|e| format!("conn {}: FOR UPDATE id={t} failed: {e}", self.conn))?;
                let QueryResult::Rows { rows, .. } = res else {
                    return Err(format!("conn {}: FOR UPDATE returned {res:?}", self.conn));
                };
                if rows.len() != 1 || rows[0][0] != Some(Datum::Int4(expected)) {
                    return Err(format!(
                        "conn {}: visibility anomaly in FOR UPDATE on id={t}: {rows:?}",
                        self.conn
                    ));
                }
                self.exec(
                    Some(&txn),
                    &format!("UPDATE stress SET v = {} WHERE id = {t}", expected + 1),
                )?;
            }
            Ok((id, target))
        })();
        match result {
            Ok((id, target)) => {
                txn.commit()
                    .map_err(|e| format!("conn {}: commit failed: {e}", self.conn))?;
                self.next_id += 1;
                self.model.insert(id, 0);
                if let Some(t) = target {
                    *self.model.get_mut(&t).unwrap() += 1;
                }
                self.selects_checked += 1;
                Ok(())
            }
            Err(e) => {
                let _ = txn.abort();
                Err(e)
            }
        }
    }

    fn random_live(&mut self) -> Option<i32> {
        // The model is small (tens-hundreds of rows); a linear pick keeps
        // this dependency-free.
        if self.model.is_empty() {
            return None;
        }
        let idx = self.rng.below(self.model.len() as u64) as usize;
        self.model.keys().nth(idx).copied()
    }

    fn run(mut self) -> Result<WorkerOutcome, String> {
        let mut iteration = 0u64;
        while Instant::now() < self.deadline {
            let slot_start = Instant::now();
            if iteration % 4 == 3 {
                self.do_explicit_txn()?;
            } else {
                match self.rng.below(100) {
                    0..=39 => {
                        self.do_insert()?;
                    }
                    40..=64 => self.do_update()?,
                    65..=74 => self.do_delete()?,
                    _ => self.do_select()?,
                }
            }
            self.txns += 1;
            iteration += 1;
            // Simple sleep pacing: one transaction slot per 1/tps seconds;
            // no catch-up bursts after a slow slot.
            let elapsed = slot_start.elapsed();
            if elapsed < self.slot {
                thread::sleep(self.slot - elapsed);
            }
        }
        Ok(WorkerOutcome {
            model: self.model,
            deleted: self.deleted,
            txns: self.txns,
            selects_checked: self.selects_checked,
        })
    }
}

/// The Stage T mixed-workload acceptance at CI scale.
#[test]
fn m2c_mixed_stress() {
    let secs = env_usize(SECS_ENV, 30).max(1) as u64;
    let conns = env_usize(CONNS_ENV, 16).max(1);
    let tps = env_usize(TPS_ENV, 100).max(1);

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine
        .exec(None, "CREATE TABLE stress (id INT, v INT, pad TEXT)")
        .unwrap();
    // Preload every connection's range in ONE transaction (one fsync).
    let preload = engine.begin_txn().unwrap();
    for conn in 0..conns {
        let base = conn as i32 * RANGE;
        for i in 0..PRELOAD {
            engine
                .exec(
                    Some(&preload),
                    &format!(
                        "INSERT INTO stress VALUES ({}, 0, 'xxxxxxxxxxxxxxxxxxxxxxxx')",
                        base + i
                    ),
                )
                .unwrap();
        }
    }
    preload.commit().unwrap();
    engine.create_index("stress", "id").unwrap();

    // Background checkpoints under write load.
    let stop = Arc::new(AtomicBool::new(false));
    let ckpt = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(CHECKPOINT_EVERY);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = engine.checkpoint() {
                    return Err(format!("background checkpoint failed: {e}"));
                }
            }
            Ok(())
        })
    };

    // Supervisor: joins all workers and reports through a channel, so a
    // lock/deadlock regression fails the test on the watchdog instead of
    // hanging `cargo test` forever.
    let (tx, rx) = mpsc::channel();
    let engine2 = Arc::clone(&engine);
    let started = Instant::now();
    thread::spawn(move || {
        let mut handles = Vec::with_capacity(conns);
        for conn in 0..conns {
            let worker = Worker::new(Arc::clone(&engine2), conn, secs, tps);
            handles.push(thread::spawn(move || worker.run()));
        }
        let mut outcomes = Vec::with_capacity(conns);
        for h in handles {
            match h.join() {
                Ok(outcome) => outcomes.push(outcome),
                Err(_) => outcomes.push(Err("worker panicked".to_string())),
            }
        }
        let _ = tx.send(outcomes);
    });
    let outcomes = rx
        .recv_timeout(Duration::from_secs(secs + WATCHDOG_MARGIN_SECS))
        .unwrap_or_else(|e| {
            panic!(
                "stress watchdog tripped after {}s (+{WATCHDOG_MARGIN_SECS}s margin): {e}",
                secs
            )
        });
    stop.store(true, Ordering::Relaxed);
    match ckpt.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{e}"),
        Err(_) => panic!("checkpoint thread panicked"),
    }

    let mut expected: HashMap<i32, i32> = HashMap::new();
    let mut deleted_per_conn: Vec<Vec<i32>> = Vec::with_capacity(conns);
    let mut total_txns = 0u64;
    let mut total_selects = 0u64;
    for outcome in outcomes {
        let outcome = outcome.unwrap_or_else(|e| panic!("worker failed: {e}"));
        total_txns += outcome.txns;
        total_selects += outcome.selects_checked;
        deleted_per_conn.push(outcome.deleted);
        for (id, v) in outcome.model {
            assert!(
                expected.insert(id, v).is_none(),
                "id ranges must be disjoint"
            );
        }
    }
    let elapsed = started.elapsed();
    eprintln!(
        "m2c_stress: {conns} conns x target {tps} txn/s x {secs}s — \
         {total_txns} txns ({:.0} txn/s achieved), {total_selects} checked selects",
        total_txns as f64 / elapsed.as_secs_f64()
    );

    // No lost tuples: the full scan must equal the merged bookkeeping.
    let res = engine.exec(None, "SELECT * FROM stress").unwrap();
    let QueryResult::Rows { rows, .. } = res else {
        panic!("final scan returned {res:?}");
    };
    assert_eq!(
        rows.len(),
        expected.len(),
        "lost tuples: scan count {} != bookkeeping count {}",
        rows.len(),
        expected.len()
    );
    for row in &rows {
        match (&row[0], &row[1]) {
            (Some(Datum::Int4(id)), Some(Datum::Int4(v))) => {
                assert_eq!(
                    expected.get(id),
                    Some(v),
                    "content mismatch for id={id}: scan v={v}"
                );
            }
            other => panic!("unexpected row shape: {other:?}"),
        }
    }

    // Heap↔index consistency: structural validation + presence spot-checks
    // (first/last live id per connection) + absence spot-checks (an id
    // deleted mid-run and an id never inserted).
    let index = engine.btree_index("stress", "id").unwrap();
    index.validate().unwrap();
    for (conn, del) in deleted_per_conn.iter().enumerate() {
        let base = conn as i32 * RANGE;
        let mut live: Vec<i32> = expected
            .keys()
            .copied()
            .filter(|id| *id >= base && *id < base + RANGE)
            .collect();
        live.sort_unstable();
        for id in live.iter().take(2).chain(live.iter().rev().take(2)) {
            assert!(
                engine
                    .index_lookup("stress", "id", &Datum::Int4(*id))
                    .unwrap()
                    .is_some(),
                "live id {id} not reachable through the index"
            );
        }
        // `base - 1` (previous conn's range top) may be live; use an id
        // never inserted: the top of THIS conn's range.
        let never = base + RANGE - 1;
        if !expected.contains_key(&never) {
            assert!(
                engine
                    .index_lookup("stress", "id", &Datum::Int4(never))
                    .unwrap()
                    .is_none(),
                "never-inserted id {never} resolved through the index"
            );
        }
        // Absence spot-check for ids DELETED mid-run (first/last two per
        // conn): the delete removes the index entry physically, so they
        // must not resolve. Ids are never reused, so a deleted id stays
        // deleted.
        for id in del.iter().take(2).chain(del.iter().rev().take(2)) {
            assert!(
                engine
                    .index_lookup("stress", "id", &Datum::Int4(*id))
                    .unwrap()
                    .is_none(),
                "deleted id {id} still resolves through the index"
            );
        }
    }

    // No leaks: no active XIDs, no wait edges, no stale victim flags.
    assert!(
        engine.txn_manager().active_xids().is_empty(),
        "leaked active XIDs: {:?}",
        engine.txn_manager().active_xids()
    );
    assert!(
        engine.txn_manager().wait_edges().is_empty(),
        "leaked wait edges: {:?}",
        engine.txn_manager().wait_edges()
    );
    assert!(
        engine.txn_manager().deadlock_victims().marked().is_empty(),
        "leaked deadlock victim flags: {:?}",
        engine.txn_manager().deadlock_victims().marked()
    );
    engine.shutdown();
}
