//! M3 Stage D acceptance (part 1): `Engine::vacuum` end-to-end fixtures and
//! concurrency coexistence (tech-selection §4.1 five-phase pipeline).
//!
//! Fixtures (coding-plan Stage D acceptance):
//!   - empty table / no-dead-rows table / all-dead table / HOT-chain table
//!     (fully-dead chain + partially-dead chain) / multi-index table with
//!     exact per-index cleanup counts (NULL keys skipped);
//!   - no lock residue after vacuum (`release_all` verified through the
//!     lock manager's introspection).
//!
//! Concurrency coexistence (Task 4):
//!   - an explicit transaction's registered snapshot PINS the horizon:
//!     vacuum cannot reclaim what the pin can still see, and reclaims it
//!     once the pin is gone (§3.3 defense, end to end);
//!   - while vacuum waits on / holds `AccessExclusive`: lock-free pure
//!     SELECTs and new BEGINs proceed, explicit-txn DML blocks in FIFO
//!     order behind vacuum — no deadlock, watchdog-protected;
//!   - lock-free readers overlapping reclaim always observe a consistent
//!     visible set (never a half-reclaimed page, never a resurrected row).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pg_am_heap::access_method::{RelationDesc, Vacuumable};
use pg_am_heap::tuple::ColumnType;
use pg_engine::{Datum, Engine, EngineConfig, TableEntry, VacuumStats};
use pg_storage::types::{Oid, Tid, TxnId};
use pg_txn::LockMode;
use tempfile::TempDir;

/// Watchdog budget for every join in this file: a lock/ordering regression
/// FAILS the test, it never hangs `cargo test` (Stage T convention).
const WATCHDOG: Duration = Duration::from_secs(120);
const OBSERVE_DEADLINE: Duration = Duration::from_secs(30);

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
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

/// Join a worker under a watchdog (same shape as `m3_snapshot_coverage.rs`).
fn watch<T: Send + 'static>(handle: JoinHandle<T>, what: &str) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(handle.join());
    });
    match rx.recv_timeout(WATCHDOG) {
        Ok(joined) => {
            joined.unwrap_or_else(|e| panic!("{what}: worker panicked: {}", panic_msg(&e)))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("{what}: watchdog tripped after {WATCHDOG:?} (deadlock/hang regression)")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{what}: watchdog channel broke"),
    }
}

/// Poll `cond` at 1ms cadence until it holds or the deadline passes.
fn poll_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if cond() {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    cond()
}

fn create_kv(engine: &Engine, table: &str) {
    engine
        .exec(
            None,
            &format!("CREATE TABLE {table} (k INT, v INT, pad TEXT)"),
        )
        .unwrap();
}

fn insert_row(engine: &Engine, table: &str, k: i32, v: i32, pad_len: usize) -> Tid {
    engine
        .insert(
            table,
            &[
                Some(Datum::Int4(k)),
                Some(Datum::Int4(v)),
                Some(Datum::Text("x".repeat(pad_len))),
            ],
        )
        .unwrap()
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

/// `scan_dead_tuples` at an explicit horizon through the engine's public
/// testing surface (same call vacuum's phase 2 makes).
fn dead_tuples(engine: &Engine, table: &str, horizon: TxnId) -> Vec<Tid> {
    let entry = engine.describe_table(table).expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);
    let mut dead = engine
        .heap()
        .scan_dead_tuples(rel, horizon, engine.clog().as_ref())
        .unwrap();
    dead.sort_by_key(|t| (t.page_id.0, t.slot_id));
    dead
}

/// Dead tuples at the engine's CURRENT horizon (empty registry + empty
/// active set ⇒ the XID clock's current value: every committed delete
/// qualifies). After a successful vacuum on a quiescent engine this must
/// be empty.
fn dead_now(engine: &Engine, table: &str) -> Vec<Tid> {
    dead_tuples(engine, table, engine.oldest_snapshot_xmin())
}

/// The table's on-disk chain length (page count of the relation).
fn page_count(engine: &Engine, table: &str) -> usize {
    let entry = engine.describe_table(table).expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);
    engine.heap().relation_pages(&rel).unwrap().len()
}

/// Allocator high-water mark: the data file's page count proxy. Freelist
/// reuse keeps this flat; without reuse every new page grows it.
fn allocated_pages(engine: &Engine) -> u64 {
    engine.storage().page_allocator().lock().next_page_id().0
}

fn freelist_len(engine: &Engine) -> usize {
    engine.storage().page_allocator().lock().freelist().len()
}

/// Stage D acceptance: no lock residue after vacuum (`release_all` ran on
/// the maintenance XID — success AND failure paths).
fn assert_no_lock_residue(engine: &Engine, oid: Oid) {
    match engine.lock_manager().table_lock_state(oid) {
        None => {}
        Some(state) => assert!(
            state.granted.is_empty() && state.waiters.is_empty(),
            "lock residue after vacuum: granted={:?} waiters={:?}",
            state.granted,
            state.waiters
        ),
    }
}

fn table_oid(engine: &Engine, table: &str) -> Oid {
    engine.describe_table(table).expect("table exists").oid
}

/// Visible `k` values of a full scan, sorted.
fn visible_ks(engine: &Engine, table: &str) -> Vec<i32> {
    let mut ks: Vec<i32> = engine
        .scan(table, None)
        .unwrap()
        .into_iter()
        .map(|(_, v)| match v[0] {
            Some(Datum::Int4(k)) => k,
            ref other => panic!("unexpected k datum: {other:?}"),
        })
        .collect();
    ks.sort_unstable();
    ks
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Empty table: a no-op pass — zero dead, zero index work, no residue.
#[test]
fn vacuum_empty_table() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_kv(&engine, "t");

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats, VacuumStats::default());
    assert_no_lock_residue(&engine, table_oid(&engine, "t"));
}

/// Table with only live rows: phase 2 finds nothing, phases 3–5 skipped.
#[test]
fn vacuum_no_dead_tuples() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_kv(&engine, "t");
    for i in 0..10 {
        insert_row(&engine, "t", i, i * 10, 16);
    }

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats, VacuumStats::default());
    assert_eq!(visible_ks(&engine, "t"), (0..10).collect::<Vec<_>>());
    assert_no_lock_residue(&engine, table_oid(&engine, "t"));
}

/// All-dead table (no index): every slot killed, all non-head pages
/// unlinked + freed to the allocator, and later inserts REUSE the freed
/// pages (allocator high-water stays flat) and the compacted head.
#[test]
fn vacuum_all_dead_table_reclaims_and_reuses_pages() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_kv(&engine, "t");
    // ~230B rows ⇒ ~35/page ⇒ 120 rows span 4+ pages.
    let tids: Vec<Tid> = (0..120)
        .map(|i| insert_row(&engine, "t", i, i, 200))
        .collect();
    let pages_before = page_count(&engine, "t");
    assert!(pages_before >= 3, "fixture must span pages: {pages_before}");
    assert!(dead_now(&engine, "t").is_empty());

    for tid in &tids {
        engine.delete("t", *tid).unwrap();
    }
    assert_eq!(dead_now(&engine, "t").len(), 120);

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 120);
    // No index registered: keys are collected but phase 4 has no targets.
    assert_eq!(stats.index_keys, 120);
    assert_eq!(stats.index_entries_removed, 0);
    assert_eq!(stats.index_entries_already_gone, 0);

    // The chain head is never freed (catalog anchor); every other page is.
    assert_eq!(page_count(&engine, "t"), 1);
    assert_eq!(
        freelist_len(&engine),
        pages_before - 1,
        "all non-head pages must land on the freelist"
    );
    assert!(dead_now(&engine, "t").is_empty());
    assert!(engine.scan("t", None).unwrap().is_empty());
    assert_no_lock_residue(&engine, table_oid(&engine, "t"));

    // Re-insert the same volume: the freed pages and the compacted head
    // must absorb it WITHOUT growing the data file.
    let hi_water = allocated_pages(&engine);
    for i in 0..120 {
        insert_row(&engine, "t", 1000 + i, i, 200);
    }
    assert_eq!(
        allocated_pages(&engine),
        hi_water,
        "freelist reuse: no new page allocated"
    );
    assert_eq!(page_count(&engine, "t"), pages_before);
    assert_eq!(visible_ks(&engine, "t").len(), 120);
}

/// HOT chains (M3 §4.2): a FULLY-dead chain is reclaimed whole (root slot,
/// its HEAP_ONLY members, and the root's index entry); a PARTIALLY-dead
/// chain is structurally untouched — no prune, no redirect.
#[test]
fn vacuum_hot_chains() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_kv(&engine, "t");
    engine.create_index("t", "k").unwrap();

    // Row X: HOT-updated 3× (pad is unindexed and stays same-size, so the
    // new versions fit the same page), then deleted ⇒ the whole chain dies.
    let x0 = insert_row(&engine, "t", 1, 100, 16);
    let x1 = engine
        .update(
            "t",
            x0,
            &[
                Some(Datum::Int4(1)),
                Some(Datum::Int4(101)),
                Some(Datum::Text("y".repeat(16))),
            ],
        )
        .unwrap();
    let x2 = engine
        .update(
            "t",
            x1,
            &[
                Some(Datum::Int4(1)),
                Some(Datum::Int4(102)),
                Some(Datum::Text("z".repeat(16))),
            ],
        )
        .unwrap();
    assert_eq!(
        x0.page_id, x2.page_id,
        "fixture must be a same-page HOT chain"
    );
    engine.delete("t", x2).unwrap();

    // Row Y: HOT-updated twice, still LIVE at the tail ⇒ partially-dead
    // chain (root + middle member are dead, the tail is not).
    let y0 = insert_row(&engine, "t", 2, 200, 16);
    let y1 = engine
        .update(
            "t",
            y0,
            &[
                Some(Datum::Int4(2)),
                Some(Datum::Int4(201)),
                Some(Datum::Text("y".repeat(16))),
            ],
        )
        .unwrap();
    let y2 = engine
        .update(
            "t",
            y1,
            &[
                Some(Datum::Int4(2)),
                Some(Datum::Int4(202)),
                Some(Datum::Text("z".repeat(16))),
            ],
        )
        .unwrap();
    assert_eq!(y0.page_id, y2.page_id);

    // Pre-vacuum: X's whole chain (3 members) + Y's dead prefix (2
    // members) are collectable at the latest horizon.
    assert_eq!(dead_now(&engine, "t").len(), 5);

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 5);
    // Only X's chain root becomes an index-cleanup work item.
    assert_eq!(stats.index_keys, 1);
    // X's entry was eagerly removed by the online delete (chain-root
    // maintenance) ⇒ phase 4 tolerates EntryNotFound.
    assert_eq!(stats.index_entries_removed, 0);
    assert_eq!(stats.index_entries_already_gone, 1);

    // X's chain is gone entirely: nothing collectable remains for it.
    let remaining = dead_tuples(&engine, "t", TxnId(u64::MAX));
    assert_eq!(
        remaining.len(),
        2,
        "only Y's partially-dead prefix may survive: {remaining:?}"
    );
    assert!(
        remaining.contains(&y0) && remaining.contains(&y1),
        "partially-dead chain members must stay in place: {remaining:?}"
    );

    // Y is still fully reachable, through both access paths.
    assert_eq!(visible_ks(&engine, "t"), vec![2]);
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int4(2)).unwrap(),
        Some(y2)
    );
    assert!(engine
        .index_lookup("t", "k", &Datum::Int4(1))
        .unwrap()
        .is_none());
    // The index stays structurally valid.
    engine.btree_index("t", "k").unwrap().validate().unwrap();
    assert_no_lock_residue(&engine, table_oid(&engine, "t"));
}

/// Multi-index table: per-index cleanup counts are EXACT, NULL keys are
/// skipped (the online convention), and both indexes end up clean.
#[test]
fn vacuum_multi_index_exact_counts() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_kv(&engine, "t");
    engine.create_index("t", "k").unwrap();
    engine.create_index("t", "v").unwrap();

    // 10 rows: k unique and non-NULL in all; v NULL in 4 of them.
    let mut tids = Vec::new();
    for i in 0..10 {
        let v = if i < 4 {
            None
        } else {
            Some(Datum::Int4(10_000 + i))
        };
        tids.push(
            engine
                .insert(
                    "t",
                    &[Some(Datum::Int4(i)), v, Some(Datum::Text("p".repeat(16)))],
                )
                .unwrap(),
        );
    }
    for tid in &tids {
        engine.delete("t", *tid).unwrap();
    }

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 10);
    assert_eq!(stats.index_keys, 10);
    // 10 delete attempts on k + 6 on v (4 NULLs skipped) = 16, all already
    // gone (eager online maintenance removed them at DELETE time).
    assert_eq!(stats.index_entries_removed, 0);
    assert_eq!(stats.index_entries_already_gone, 16);

    // Both indexes are clean: no dead key resolves, structure validates.
    for i in 0..10 {
        let key = pg_am_btree::encode_key(&Datum::Int4(i)).unwrap();
        assert!(engine
            .btree_index("t", "k")
            .unwrap()
            .lookup_all(&key)
            .unwrap()
            .is_empty());
    }
    for i in 4..10 {
        let key = pg_am_btree::encode_key(&Datum::Int4(10_000 + i)).unwrap();
        assert!(engine
            .btree_index("t", "v")
            .unwrap()
            .lookup_all(&key)
            .unwrap()
            .is_empty());
    }
    engine.btree_index("t", "k").unwrap().validate().unwrap();
    engine.btree_index("t", "v").unwrap().validate().unwrap();
    assert!(dead_now(&engine, "t").is_empty());
    assert!(engine.scan("t", None).unwrap().is_empty());
    assert_no_lock_residue(&engine, table_oid(&engine, "t"));
}

/// Vacuuming an unknown table fails cleanly (and acquires nothing).
#[test]
fn vacuum_table_not_found() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let err = engine.vacuum("nope").unwrap_err();
    assert!(
        matches!(err, pg_engine::EngineError::TableNotFound(_)),
        "unexpected error: {err}"
    );
}

/// Failure path residue: a vacuum that fails INSIDE the pass must still
/// release the maintenance XID's locks. The deterministic way to fail a
/// queued vacuum: queue a `drop_table` AHEAD of it (FIFO — whoever queues
/// first is granted first). The drop completes, the vacuum is then granted
/// `AccessExclusive` on the dead OID, fails the `lock_table_entry`
/// post-lock registry re-check with `TableNotFound`, and must leave no
/// lock behind.
#[test]
fn vacuum_failure_path_releases_locks() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    create_kv(&engine, "t");
    insert_row(&engine, "t", 1, 1, 16);
    let oid = table_oid(&engine, "t");

    // Holder pins the table with AccessShare so both contenders queue.
    let holder = engine.begin_txn().unwrap();
    engine.exec(Some(&holder), "SELECT * FROM t").unwrap();

    // Drop queues FIRST (it is granted first when the holder commits).
    let dropper = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || engine.drop_table("t"))
    };
    assert!(
        poll_until(OBSERVE_DEADLINE, || {
            engine
                .lock_manager()
                .table_lock_state(oid)
                .is_some_and(|s| {
                    s.waiters
                        .iter()
                        .any(|(_, m)| *m == LockMode::AccessExclusive)
                })
        }),
        "drop never queued on AccessExclusive"
    );

    // Vacuum queues SECOND, behind the drop.
    let vac = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || engine.vacuum("t"))
    };
    assert!(
        poll_until(OBSERVE_DEADLINE, || {
            engine
                .lock_manager()
                .table_lock_state(oid)
                .is_some_and(|s| s.waiters.len() == 2)
        }),
        "vacuum never queued behind the drop"
    );

    // Drain in FIFO order: drop runs, then vacuum is granted the dead
    // OID's lock and fails the post-lock re-check.
    holder.commit().unwrap();
    watch(dropper, "queued drop").unwrap();
    let err = watch(vac, "queued vacuum").unwrap_err();
    assert!(
        matches!(err, pg_engine::EngineError::TableNotFound(_)),
        "unexpected error: {err}"
    );
    assert_no_lock_residue(&engine, oid);
}

// ---------------------------------------------------------------------------
// Concurrency coexistence (Task 4)
// ---------------------------------------------------------------------------

/// Horizon defense, end to end (§3.3): an explicit transaction's
/// REGISTERED snapshot pins the horizon — vacuum cannot reclaim the row
/// versions the pin can still see; once the pin goes away, the next
/// vacuum reclaims them.
#[test]
fn vacuum_respects_pinned_horizon() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_kv(&engine, "t");
    let victim = insert_row(&engine, "t", 42, 420, 32);
    let keeper = insert_row(&engine, "t", 7, 70, 32);

    // R begins BEFORE the delete commits: its snapshot (xmin = R's own
    // XID) is registered and pins the horizon below the deleter's XID.
    let r = engine.begin_txn().unwrap();
    assert_eq!(engine.oldest_snapshot_xmin(), r.xid());

    engine.delete("t", victim).unwrap();

    // Vacuum runs to completion (R holds no lock — begin takes none), but
    // its horizon is R's pinned xmin, so the deleted version survives.
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 0, "pinned horizon must hide the delete");
    assert_eq!(
        dead_tuples(&engine, "t", TxnId(u64::MAX)),
        vec![victim],
        "the dead version must still be physically present"
    );
    assert_no_lock_residue(&engine, table_oid(&engine, "t"));

    // Unpin: the next vacuum reclaims it.
    r.abort().unwrap();
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 1);
    assert!(dead_tuples(&engine, "t", TxnId(u64::MAX)).is_empty());
    assert_eq!(visible_ks(&engine, "t"), vec![7]);
    let _ = keeper;
}

/// While vacuum waits on / holds the table's `AccessExclusive`:
///   - lock-free pure SELECTs (`Engine::scan`, auto-commit SQL SELECT)
///     proceed — no lock involved;
///   - new BEGINs proceed (no lock at begin);
///   - an explicit transaction's DML blocks in FIFO order BEHIND the
///     queued vacuum and completes only after it — ordered blocking, not
///     deadlock (watchdog);
///   - no lock residue afterwards.
#[test]
fn vacuum_blocking_order_and_lock_free_coexistence() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    create_kv(&engine, "t");
    let mut dead = Vec::new();
    for i in 0..40 {
        let tid = insert_row(&engine, "t", i, i, 100);
        if i % 2 == 0 {
            dead.push(tid);
        }
    }
    for tid in &dead {
        engine.delete("t", *tid).unwrap();
    }
    let oid = table_oid(&engine, "t");

    // Holder takes AccessShare inside an explicit txn and stays open.
    let holder = engine.begin_txn().unwrap();
    engine.exec(Some(&holder), "SELECT * FROM t").unwrap();

    // Vacuum queues on AccessExclusive behind the holder.
    let vac = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || engine.vacuum("t"))
    };
    assert!(
        poll_until(OBSERVE_DEADLINE, || {
            engine
                .lock_manager()
                .table_lock_state(oid)
                .is_some_and(|s| {
                    s.waiters
                        .iter()
                        .any(|(_, m)| *m == LockMode::AccessExclusive)
                })
        }),
        "vacuum never queued on AccessExclusive"
    );

    // An explicit-txn INSERT queues BEHIND vacuum (FIFO): ordered blocking.
    let inserter_txn = engine.begin_txn().unwrap();
    let inserter = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            engine
                .exec(Some(&inserter_txn), "INSERT INTO t VALUES (999, 999, 'q')")
                .unwrap();
            inserter_txn.commit().unwrap();
        })
    };
    assert!(
        poll_until(OBSERVE_DEADLINE, || {
            engine
                .lock_manager()
                .table_lock_state(oid)
                .is_some_and(|s| {
                    s.waiters.len() == 2
                        && s.waiters[0].1 == LockMode::AccessExclusive
                        && s.waiters[1].1 == LockMode::RowExclusive
                })
        }),
        "expected FIFO wait queue [vacuum AE, inserter RE]: {:?}",
        engine.lock_manager().table_lock_state(oid)
    );

    // While both wait: lock-free pure SELECTs proceed and see the
    // pre-vacuum visible set (20 live rows), and new BEGINs proceed.
    for _ in 0..10 {
        assert_eq!(engine.scan("t", None).unwrap().len(), 20);
        let r = engine.exec(None, "SELECT * FROM t").unwrap();
        match r {
            pg_engine::QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 20),
            other => panic!("unexpected SELECT result: {other:?}"),
        }
    }
    let fresh = engine.begin_txn().unwrap();
    fresh.abort().unwrap();

    // Let the queue drain: holder commits ⇒ vacuum runs ⇒ inserter runs.
    holder.commit().unwrap();
    let stats = watch(vac, "vacuum").unwrap();
    assert_eq!(stats.dead_tuples, 20);
    watch(inserter, "queued inserter");

    // No residue; the final state is exactly right.
    assert_no_lock_residue(&engine, oid);
    assert!(dead_now(&engine, "t").is_empty());
    assert_eq!(engine.scan("t", None).unwrap().len(), 21);
}

/// Lock-free readers overlapping reclaim always observe a consistent
/// visible set: the never-deleted rows are fully present in EVERY scan
/// (a half-compacted page would drop or duplicate them), and no error or
/// resurrection ever surfaces. Runs several vacuums back to back to
/// maximize phase-5/read overlap. Watchdog-protected.
#[test]
fn vacuum_concurrent_pure_selects_stay_consistent() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    create_kv(&engine, "t");
    // 600 live rows k in [0, 600) + 600 doomed rows k in [600, 1200),
    // ~120B each ⇒ the table spans ~30 pages; reclaim touches many.
    let mut doomed = Vec::new();
    for i in 0..1200 {
        let tid = insert_row(&engine, "t", i, i, 100);
        if i >= 600 {
            doomed.push(tid);
        }
    }
    for tid in &doomed {
        engine.delete("t", *tid).unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for r in 0..4 {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        readers.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // Both lock-free read paths: the typed API and the SQL
                // auto-commit pure SELECT (neither takes a table lock).
                let ks: Vec<i32> = if r % 2 == 0 {
                    engine
                        .scan("t", None)
                        .unwrap()
                        .into_iter()
                        .map(|(_, v)| v)
                        .filter_map(|v| match v[0] {
                            Some(Datum::Int4(k)) => Some(k),
                            _ => None,
                        })
                        .collect()
                } else {
                    match engine.exec(None, "SELECT * FROM t").unwrap() {
                        pg_engine::QueryResult::Rows { rows, .. } => rows
                            .into_iter()
                            .filter_map(|v| match v[0] {
                                Some(Datum::Int4(k)) => Some(k),
                                _ => None,
                            })
                            .collect(),
                        other => panic!("unexpected SELECT result: {other:?}"),
                    }
                };
                assert_eq!(
                    ks.len(),
                    600,
                    "reader {r} observed a torn set: {} rows",
                    ks.len()
                );
                for k in &ks {
                    assert!((0..600).contains(k), "resurrected or foreign row k={k}");
                }
            }
        }));
    }

    // Three back-to-back passes: the first reclaims; the next two are
    // no-ops that still take and release the lock.
    for i in 0..3 {
        let stats = engine.vacuum("t").unwrap();
        if i == 0 {
            assert_eq!(stats.dead_tuples, 600);
        } else {
            assert_eq!(stats, VacuumStats::default());
        }
    }

    stop.store(true, Ordering::Relaxed);
    for (i, r) in readers.into_iter().enumerate() {
        watch(r, &format!("reader {i}"));
    }
    assert!(dead_now(&engine, "t").is_empty());
    assert_eq!(engine.scan("t", None).unwrap().len(), 600);
    assert_no_lock_residue(&engine, table_oid(&engine, "t"));
}

/// Raw index probe (no visibility mask): the TIDs the B+Tree physically
/// holds for `key` on `column`.
fn raw_index_tids(engine: &Engine, column: &str, key: i32) -> Vec<Tid> {
    let encoded = pg_am_btree::encode_key(&Datum::Int4(key)).unwrap();
    engine
        .btree_index("t", column)
        .unwrap()
        .lookup_all(&encoded)
        .unwrap()
}

/// A loser INSERT without crashing the engine (the Stage O "undo-skipped
/// abort" shape, constructed deliberately): the heap insert and both index
/// entries are written inside an explicit transaction, then the XID is
/// aborted through the back door (`TxnManager::abort_txn`) WITHOUT running
/// the per-transaction index undo log — the handle is forgotten so its
/// `Drop` cannot do it either. Result: the heap tuple is dead (aborted
/// `t_xmin`, `scan_dead_tuples` rule 1) while its index entries dangle.
fn dangling_loser_insert(engine: &Engine, k: i32, v: i32) {
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(
            Some(&txn),
            &format!("INSERT INTO t VALUES ({k}, {v}, 'loser')"),
        )
        .unwrap();
    let xid = txn.xid();
    std::mem::forget(txn);
    engine.txn_manager().abort_txn(xid).unwrap();
    // The back-door abort does not release table locks (the
    // `Engine::txn_manager` doc's standing warning).
    engine.lock_manager().release_all(xid);
}

/// Phase-④ mid-failure injection (review finding): with TWO indexes on
/// the table and a dangling loser entry on each, the second index's meta
/// page is corrupted so `open_btree` fails AFTER the first index's
/// cleanup already ran. Assert:
///
/// - the failed vacuum leaves NO residue: the maintenance XID is out of
///   the active set and `table_lock_state` is clean (the auto-commit
///   failure path's abort + `release_all`);
/// - the first index's deletes are durable anyway (physical xid=0 WAL —
///   vacuum is not transactional);
/// - the heap was never touched (reclaim never ran): the dead tuples are
///   still physically present;
/// - after repairing the meta page, the next vacuum finishes cleanly:
///   first index tolerated as `EntryNotFound`, second index's dangling
///   entries actually removed — the window-① semantics without a crash.
#[test]
fn vacuum_phase4_mid_failure_releases_and_next_pass_finishes() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_kv(&engine, "t");
    engine.create_index("t", "k").unwrap();
    engine.create_index("t", "v").unwrap();
    let oid = table_oid(&engine, "t");

    // 10 committed rows, then deleted (their index entries are eagerly
    // removed online), plus 2 loser rows with dangling entries on BOTH
    // indexes.
    let tids: Vec<Tid> = (0..10)
        .map(|i| insert_row(&engine, "t", i, i, 32))
        .collect();
    for tid in &tids {
        engine.delete("t", *tid).unwrap();
    }
    dangling_loser_insert(&engine, 900, 900);
    dangling_loser_insert(&engine, 901, 901);
    assert_eq!(raw_index_tids(&engine, "k", 900).len(), 1);
    assert_eq!(raw_index_tids(&engine, "v", 900).len(), 1);

    // Corrupt the SECOND index's meta page (zeroed ⇒ `root_from_meta`
    // fails with Corrupted on the next open).
    let meta_v = engine
        .indexes()
        .into_iter()
        .find(|e| e.column == "v")
        .unwrap()
        .meta_page;
    let saved_meta = {
        let guard = engine.storage().buffer_pool().pin(meta_v).unwrap();
        guard.page().to_vec()
    };
    {
        let mut guard = engine.storage().buffer_pool().pin_mut(meta_v).unwrap();
        guard.page_mut().fill(0);
    }

    // Vacuum #1 fails mid-phase-④: index k processed (2 real deletes),
    // index v fails to open.
    let err = engine.vacuum("t").unwrap_err();
    assert!(
        matches!(err, pg_engine::EngineError::BTree(_)),
        "expected the corrupted index to fail the vacuum, got: {err}"
    );

    // No residue: maintenance XID aborted and out of the active set, no
    // lock state left on the table.
    assert!(
        engine.txn_manager().active_xids().is_empty(),
        "maintenance XID leaked into the active set: {:?}",
        engine.txn_manager().active_xids()
    );
    assert_no_lock_residue(&engine, oid);

    // The failed pass is not rolled back (physical WAL): k's dangling
    // entries are gone; the heap is untouched — all 12 dead tuples are
    // still physically present; v's entries still dangle.
    assert!(raw_index_tids(&engine, "k", 900).is_empty());
    assert!(raw_index_tids(&engine, "k", 901).is_empty());
    assert_eq!(dead_tuples(&engine, "t", TxnId(u64::MAX)).len(), 12);

    // Repair the meta page byte-for-byte.
    {
        let mut guard = engine.storage().buffer_pool().pin_mut(meta_v).unwrap();
        guard.page_mut().copy_from_slice(&saved_meta);
    }
    assert_eq!(raw_index_tids(&engine, "v", 900).len(), 1);

    // Vacuum #2 finishes the pass: k tolerated as EntryNotFound
    // (12 attempts), v removes its 2 dangling entries for real (plus 10
    // EntryNotFound for the committed deletes).
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 12);
    assert_eq!(stats.index_keys, 12);
    assert_eq!(stats.index_entries_removed, 2);
    assert_eq!(stats.index_entries_already_gone, 22);
    assert!(dead_tuples(&engine, "t", TxnId(u64::MAX)).is_empty());
    assert!(raw_index_tids(&engine, "v", 900).is_empty());
    assert!(raw_index_tids(&engine, "v", 901).is_empty());
    engine.btree_index("t", "k").unwrap().validate().unwrap();
    engine.btree_index("t", "v").unwrap().validate().unwrap();
    assert!(engine.scan("t", None).unwrap().is_empty());
    assert_no_lock_residue(&engine, oid);
}
