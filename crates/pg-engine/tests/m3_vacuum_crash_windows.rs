//! M3 Stage D acceptance (part 2): crash window ① (tech-selection §12.1)
//! and crash-loser dangling index entries.
//!
//! **Window ①**: vacuum's phase-4 index-cleanup WAL (`BTreeDelete`) is
//! durable, but no `HeapCleanup` / `PageFree` was ever written — the crash
//! lands between index cleanup and reclaim. The §4.1 ordering invariant is
//! what makes this safe: the entries are gone, the tuples are not, so no
//! dangling TID can resolve to a wrong row (the slots were never recycled),
//! and the next vacuum pass re-collects the still-present dead tuples and
//! tolerates the missing entries as `EntryNotFound` (§4.3).
//!
//! The window is constructed by driving phases ②–④ by hand through the
//! same public surface `Engine::vacuum` uses (same order: horizon →
//! `scan_dead_tuples` → `collect_index_keys` → per-index `delete` with
//! `EntryNotFound` → Ok), flushing the WAL, and then simulating kill -9
//! (`mem::forget`, no checkpoint, no shutdown — the `pg-am-heap`
//! `vacuum_crash_windows.rs` precedent). Mutual exclusion, which
//! `Engine::vacuum` provides with `AccessExclusive`, is trivially satisfied
//! here: the driver is single-threaded.
//!
//! **Dangling loser entries** (Task 5): a crash-loser INSERT (index entry
//! written, the transaction never committed) leaves a physically present
//! index entry pointing at an invisible heap tuple. `index_lookup` masks it
//! via heap visibility; vacuum's phase 4 is the mechanism that actually
//! removes it — this file asserts both.

use std::path::Path;

use pg_am_btree::{encode_key, BTreeError};
use pg_am_heap::access_method::{RelationDesc, Vacuumable};
use pg_am_heap::tuple::ColumnType;
use pg_engine::{Datum, Engine, EngineConfig, TableEntry};
use pg_storage::types::{Tid, TxnId};
use tempfile::TempDir;

fn open(dir: &Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn create_indexed(engine: &Engine) {
    engine
        .exec(None, "CREATE TABLE t (k INT, v INT, pad TEXT)")
        .unwrap();
    engine.create_index("t", "k").unwrap();
}

fn insert_row(engine: &Engine, k: i32, v: i32, pad_len: usize) -> Tid {
    engine
        .insert(
            "t",
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

fn dead_tuples(engine: &Engine, horizon: TxnId) -> Vec<Tid> {
    let entry = engine.describe_table("t").expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);
    let mut dead = engine
        .heap()
        .scan_dead_tuples(rel, horizon, engine.clog().as_ref())
        .unwrap();
    dead.sort_by_key(|t| (t.page_id.0, t.slot_id));
    dead
}

/// Visible `k` values of a full heap scan, sorted.
fn visible_ks(engine: &Engine) -> Vec<i32> {
    let mut ks: Vec<i32> = engine
        .scan("t", None)
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

/// Raw index probe (no visibility mask): the TIDs the B+Tree physically
/// holds for `k`.
fn raw_index_tids(engine: &Engine, k: i32) -> Vec<Tid> {
    let key = encode_key(&Datum::Int4(k)).unwrap();
    engine
        .btree_index("t", "k")
        .unwrap()
        .lookup_all(&key)
        .unwrap()
}

/// A crash-loser INSERT: heap insert + index entry written and WAL-flushed
/// inside an explicit transaction that NEVER commits (the handle and the
/// engine are forgotten — kill -9 with the statement durable but no
/// `TxnCommit` record anywhere).
fn crash_loser_insert(engine: Engine, k: i32, v: i32) {
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(
            Some(&txn),
            &format!("INSERT INTO t VALUES ({k}, {v}, 'loser')"),
        )
        .unwrap();
    // Make the loser's writes durable, then die without commit/abort.
    engine.storage().wal_writer().flush().unwrap();
    std::mem::forget(txn);
    std::mem::forget(engine);
}

/// Drive `Engine::vacuum`'s phases ②–④ by hand (same public calls, same
/// order) and STOP before phase 5 (`reclaim`). Returns `(dead, removed)`.
/// This is how the window-① crash point is reached deterministically,
/// without instrumenting the engine.
fn drive_phases_2_to_4(engine: &Engine) -> (Vec<Tid>, usize) {
    let entry = engine.describe_table("t").expect("table exists");
    let col_types = col_types_of(&entry);
    let rel = relation_desc(&entry, &col_types);

    // Phase ①'s horizon (the lock is trivially satisfied: single-threaded).
    let horizon = engine.oldest_snapshot_xmin();
    // Phase ②.
    let dead = engine
        .heap()
        .scan_dead_tuples(rel, horizon, engine.clog().as_ref())
        .unwrap();
    // Phase ③ (read-only).
    let keys = engine.heap().collect_index_keys(rel, &dead).unwrap();
    // Phase ④ (push mode; EntryNotFound → Ok, the §4.3 vacuum-call-site rule).
    let mut removed = 0;
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
                continue; // NULL keys carry no entry
            };
            let key = encode_key(datum).unwrap();
            match index.delete(&key, *tid) {
                Ok(()) => removed += 1,
                Err(BTreeError::EntryNotFound) => {}
                Err(e) => panic!("phase-4 delete failed unexpectedly: {e}"),
            }
        }
    }
    (dead, removed)
}

/// Heap↔index consistency after recovery, the window-① contract:
/// - every VISIBLE row resolves through the index, to a row whose key
///   matches (no dangling TID resolves to a wrong row);
/// - none of the dead/absent keys physically remains in the index;
/// - the B+Tree validates structurally.
fn assert_heap_index_consistent(engine: &Engine, live: &[i32], gone: &[i32]) {
    for &k in live {
        let tid = engine
            .index_lookup("t", "k", &Datum::Int4(k))
            .unwrap()
            .unwrap_or_else(|| panic!("live key {k} unreachable through the index"));
        let row = engine
            .scan(
                "t",
                Some(pg_engine::Predicate::Eq {
                    col_index: 0,
                    value: Datum::Int4(k),
                }),
            )
            .unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row[0].0, tid, "index TID must address the scanned row");
    }
    for &k in gone {
        assert!(
            engine
                .index_lookup("t", "k", &Datum::Int4(k))
                .unwrap()
                .is_none(),
            "dead key {k} must not resolve"
        );
        assert!(
            raw_index_tids(engine, k).is_empty(),
            "dead key {k} must not physically remain in the index"
        );
    }
    engine.btree_index("t", "k").unwrap().validate().unwrap();
}

/// Window ① end to end: index-cleanup WAL durable, `HeapCleanup` never
/// written. The cleanup WAL has real content because the pass removes a
/// crash-loser's dangling entry (committed deletes' entries were already
/// eagerly removed online — phase 4 tolerates those as `EntryNotFound`).
#[test]
fn crash_window_1_index_cleanup_durable_heap_cleanup_not() {
    let tmp = TempDir::new().unwrap();

    // Session 1: committed rows 0..40 (multi-page), then a crash-loser
    // INSERT of k=1000 (index entry durable, txn never commits).
    {
        let engine = open(tmp.path());
        create_indexed(&engine);
        for i in 0..40 {
            insert_row(&engine, i, i * 10, 150);
        }
        crash_loser_insert(engine, 1000, 1000);
    }

    // Session 2: recovery marks the loser aborted (the entry dangles).
    // Delete rows 0..20 (committed; their index entries are eagerly
    // removed online), then drive phases ②–④ and crash BEFORE reclaim.
    let (dead_len, removed) = {
        let engine = open(tmp.path());
        // The dangling entry survived recovery, physically present.
        assert_eq!(raw_index_tids(&engine, 1000).len(), 1);
        assert!(engine
            .index_lookup("t", "k", &Datum::Int4(1000))
            .unwrap()
            .is_none());

        let doomed: Vec<Tid> = (0..20)
            .map(|k| {
                engine
                    .index_lookup("t", "k", &Datum::Int4(k))
                    .unwrap()
                    .expect("live before delete")
            })
            .collect();
        for tid in doomed {
            engine.delete("t", tid).unwrap();
        }

        let (dead, removed) = drive_phases_2_to_4(&engine);
        // 20 committed deletes + 1 aborted loser insert.
        assert_eq!(dead.len(), 21, "dead set: {dead:?}");
        // The only entry phase 4 actually removes is the loser's dangling
        // one; the 20 committed deletes hit EntryNotFound (already gone).
        assert_eq!(removed, 1);
        assert!(raw_index_tids(&engine, 1000).is_empty());

        // Crash: phase-4 WAL durable, reclaim never ran.
        engine.storage().wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (dead.len(), removed)
    };
    let _ = (dead_len, removed);

    // Session 3: recovery replays the `BTreeDelete` (the loser entry is
    // gone) but no `HeapCleanup` exists — every heap tuple is still
    // physically in place. Consistency must hold exactly as specified.
    let engine = open(tmp.path());
    let live: Vec<i32> = (20..40).collect();
    let gone_committed: Vec<i32> = (0..20).collect();
    assert_eq!(visible_ks(&engine), live);
    assert_heap_index_consistent(&engine, &live, &gone_committed);
    assert_heap_index_consistent(&engine, &[], &[1000]);

    // The still-present dead tuples are re-collected: the finishing pass
    // tolerates the already-removed entries and reclaims the space.
    assert_eq!(dead_tuples(&engine, TxnId(u64::MAX)).len(), 21);
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 21);
    assert_eq!(stats.index_keys, 21);
    assert_eq!(stats.index_entries_removed, 0);
    assert_eq!(stats.index_entries_already_gone, 21);
    assert!(dead_tuples(&engine, TxnId(u64::MAX)).is_empty());
    assert_eq!(visible_ks(&engine), live);
    assert_heap_index_consistent(&engine, &live, &gone_committed);

    // The table keeps working: a fresh insert resolves through the index.
    insert_row(&engine, 2000, 2000, 150);
    assert!(engine
        .index_lookup("t", "k", &Datum::Int4(2000))
        .unwrap()
        .is_some());
    engine.shutdown();
}

/// Task 5: a crash-loser INSERT leaves a dangling index entry (written,
/// never committed). After recovery + `Engine::vacuum`, the entry is
/// ACTUALLY gone: the raw index probe finds nothing, `index_lookup`
/// cannot resolve it, and the heap never had a visible row for it.
#[test]
fn crash_loser_insert_entry_removed_by_vacuum() {
    let tmp = TempDir::new().unwrap();

    // Session 1: committed baseline rows, then the loser.
    {
        let engine = open(tmp.path());
        create_indexed(&engine);
        for i in 0..10 {
            insert_row(&engine, i, i, 32);
        }
        crash_loser_insert(engine, 777, 777);
    }

    // Session 2: the entry dangles (physically present, MVCC-invisible).
    let engine = open(tmp.path());
    let dangling = raw_index_tids(&engine, 777);
    assert_eq!(
        dangling.len(),
        1,
        "the loser's index entry must survive recovery physically"
    );
    // The heap never had it (visibility): index_lookup masks the entry.
    assert!(engine
        .index_lookup("t", "k", &Datum::Int4(777))
        .unwrap()
        .is_none());
    assert_eq!(visible_ks(&engine), (0..10).collect::<Vec<_>>());

    // Vacuum: the aborted insert is collected by `scan_dead_tuples`
    // (rule 1), its key extracted, and the dangling entry is the one
    // thing phase 4 actually deletes (removed == 1, not EntryNotFound).
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.dead_tuples, 1);
    assert_eq!(stats.index_keys, 1);
    assert_eq!(stats.index_entries_removed, 1);
    assert_eq!(stats.index_entries_already_gone, 0);

    // Gone for real: raw probe empty, lookup unresolvable, heap clean.
    assert!(raw_index_tids(&engine, 777).is_empty());
    assert!(engine
        .index_lookup("t", "k", &Datum::Int4(777))
        .unwrap()
        .is_none());
    assert!(dead_tuples(&engine, TxnId(u64::MAX)).is_empty());
    assert_eq!(visible_ks(&engine), (0..10).collect::<Vec<_>>());
    engine.btree_index("t", "k").unwrap().validate().unwrap();
    engine.shutdown();
}
