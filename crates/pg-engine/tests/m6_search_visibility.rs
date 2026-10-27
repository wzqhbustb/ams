//! M6 Stage D: visibility-filtered ANN search (tech-selection §7) — the
//! five-step pipeline pins: committed rows visible, in-flight/aborted/
//! deleted rows hidden, slot-reuse mismatches discarded, ef-retry rounds.

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig};
use pg_storage::types::{PageId, Tid};
use pg_txn::Snapshot;
use tempfile::TempDir;

const DIM: u16 = 3;

fn schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "id".to_string(),
            col_type: ColumnType::Int4,
        },
        ColumnDef {
            name: "v".to_string(),
            col_type: ColumnType::vector(DIM as u32).unwrap(),
        },
    ]
}

fn setup(dir: &std::path::Path) -> (Engine, pg_storage::types::Oid) {
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    engine.create_table("t", &schema()).unwrap();
    let (oid, _meta) = engine
        .create_hnsw_index_on(
            "t",
            "v",
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            0x5EED,
        )
        .unwrap();
    (engine, oid)
}

fn insert(engine: &Engine, id: i32, v: Vec<f32>) -> Tid {
    engine
        .insert(
            "t",
            &[
                pg_engine::Value::from(Some(pg_am_heap::Datum::Int4(id))),
                pg_engine::Value::from(Some(pg_am_heap::Datum::Vector(v))),
            ],
        )
        .unwrap()
}

fn q(engine: &Engine, v: &[f32], k: usize) -> Vec<(Tid, f64)> {
    engine
        .hnsw_search_visible("t", v, k, None, &Snapshot::everything())
        .unwrap()
}

#[test]
fn committed_rows_are_visible() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    let t1 = insert(&engine, 1, vec![1.0, 0.0, 0.0]);
    let t2 = insert(&engine, 2, vec![0.0, 1.0, 0.0]);

    let hits = q(&engine, &[1.0, 0.0, 0.0], 2);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, t1, "nearest first");
    assert_eq!(hits[0].1, 0.0, "self recall at distance 0");
    assert_eq!(hits[1].0, t2);
    engine.shutdown();
}

#[test]
fn in_flight_rows_are_hidden() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    insert(&engine, 1, vec![1.0, 0.0, 0.0]);
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO t VALUES (2, [0.0, 1.0, 0.0])")
        .unwrap();
    // The in-flight row must not appear (its xmin is uncommitted).
    let hits = q(&engine, &[0.0, 1.0, 0.0], 2);
    assert_eq!(hits.len(), 1, "only the committed row is visible");
    std::mem::forget(txn);
    engine.shutdown();
}

#[test]
fn aborted_rows_are_hidden() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    insert(&engine, 1, vec![1.0, 0.0, 0.0]);
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO t VALUES (2, [0.0, 1.0, 0.0])")
        .unwrap();
    txn.abort().unwrap();
    let t1 = engine.scan("t", None).unwrap()[0].0;
    let hits = q(&engine, &[0.0, 1.0, 0.0], 2);
    assert_eq!(hits.len(), 1, "only the committed row is visible");
    assert_eq!(hits[0].0, t1, "it is the committed row");
    assert_eq!(hits[0].1, 2.0, "distance to [0,1,0] from [1,0,0]");
    engine.shutdown();
}

#[test]
fn deleted_rows_are_hidden() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    let t1 = insert(&engine, 1, vec![1.0, 0.0, 0.0]);
    let _t2 = insert(&engine, 2, vec![0.0, 1.0, 0.0]);
    engine.delete("t", t1).unwrap();
    let hits = q(&engine, &[1.0, 0.0, 0.0], 2);
    assert_eq!(hits.len(), 1, "the deleted row is tombstoned out");
    engine.shutdown();
}

#[test]
fn slot_reuse_mismatch_is_discarded() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let ta = insert(&engine, 1, vec![1.0, 0.0, 0.0]);
    let _tb = insert(&engine, 2, vec![0.0, 1.0, 0.0]);
    // Forge a WRONG map entry: node 0 → row 2's tid (row 2 carries
    // NodeId 1, not 0). The pipeline must discard it (never a wrong row).
    let rows = engine.scan("t", None).unwrap();
    let tid_b = rows
        .iter()
        .find(|(_, vals)| matches!(&vals[0], Some(pg_am_heap::Datum::Int4(2))))
        .unwrap()
        .0;
    // Make the forged entry the ONLY entry for node 0 (otherwise the map
    // holds both (0→ta) and (0→tid_b) and lookup finds the right one).
    engine.hnsw_map_remove(oid, 0, ta).unwrap();
    engine.hnsw_map_put(oid, 0, tid_b).unwrap();
    let hits = q(&engine, &[1.0, 0.0, 0.0], 2);
    assert_eq!(hits.len(), 1, "the forged entry's candidate is discarded");
    assert_eq!(
        hits[0].0, tid_b,
        "only row B survives (row A's node has no valid map entry)"
    );
    engine.shutdown();
}

#[test]
fn ef_retry_fills_k_after_filtering() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    // Review P3-1 (the retry-coverage fix): delete the NEAREST rows, keep
    // the farthest three — round 0's top-6 candidates are all deleted (0
    // survivors), so the fill MUST come from a later round. (The earlier
    // shape deleted the far rows and round 0 filled k immediately, never
    // exercising the retry.)
    let mut kept = Vec::new();
    for i in 0..12 {
        let t = insert(&engine, i, vec![i as f32, 0.0, 0.0]);
        if i < 9 {
            engine.delete("t", t).unwrap();
        } else {
            kept.push(t);
        }
    }
    let hits = q(&engine, &[0.0, 0.0, 0.0], 3);
    assert_eq!(hits.len(), 3, "the retry must find all visible rows");
    assert!(
        hits.iter().all(|(t, _)| kept.contains(t)),
        "the three survivors are exactly the kept (farthest) rows"
    );
    engine.shutdown();
}

// ---------------------------------------------------------------------
// D-2: ground truth = visible-row brute force; recall measured against it
// ---------------------------------------------------------------------

/// Brute-force top-k over the VISIBLE rows only (the §7 ground truth).
fn brute_force_visible(engine: &Engine, query: &[f32], k: usize) -> Vec<(Tid, f64)> {
    let rows = engine.scan("t", None).unwrap();
    let mut scored: Vec<(Tid, f64)> = rows
        .into_iter()
        .map(|(tid, vals)| {
            let Some(pg_am_heap::Datum::Vector(v)) = &vals[1] else {
                panic!("test rows always carry a vector");
            };
            let d = pg_am_hnsw::distance::l2_squared(query, v).unwrap();
            (tid, d)
        })
        .collect();
    scored.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap()
            .then(a.0.page_id.0.cmp(&b.0.page_id.0))
            .then(a.0.slot_id.cmp(&b.0.slot_id))
    });
    scored.truncate(k);
    scored
}

/// Ground truth definition pin: the brute force runs over visible rows
/// only — deleted/aborted rows are OUT of the truth set.
#[test]
fn ground_truth_excludes_invisible_rows() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    insert(&engine, 1, vec![1.0, 0.0, 0.0]);
    let dead = insert(&engine, 2, vec![0.0, 1.0, 0.0]);
    engine.delete("t", dead).unwrap();
    let truth = brute_force_visible(&engine, &[0.0, 1.0, 0.0], 2);
    assert_eq!(truth.len(), 1, "the deleted row is out of the truth set");
    assert_eq!(truth[0].1, 2.0, "only the surviving row remains");
    engine.shutdown();
}

/// recall@k against the visible-set ground truth on a small corpus (where
/// HNSW at ef >= n is effectively exact): the filtered search must return
/// EXACTLY the ground-truth top-k (ids + distances).
#[test]
fn filtered_search_matches_visible_ground_truth() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    // 20 committed rows; delete 7 of them.
    let mut deleted = Vec::new();
    for i in 0..20 {
        let t = insert(&engine, i, vec![i as f32, (i % 5) as f32, 0.0]);
        if i % 3 == 0 && i > 0 {
            engine.delete("t", t).unwrap();
            deleted.push(t);
        }
    }
    let k = 10;
    let truth = brute_force_visible(&engine, &[2.0, 1.0, 0.0], k);
    let hits = q(&engine, &[2.0, 1.0, 0.0], k);
    assert_eq!(hits.len(), k);
    assert_eq!(
        hits, truth,
        "filtered search must equal the visible-set brute force on a small corpus"
    );
    assert!(
        !hits.iter().any(|(t, _)| deleted.contains(t)),
        "no deleted row may appear"
    );
    engine.shutdown();
}

/// Pathological amplification: NO visible rows (all deleted) — the ef
/// escalation must cap at 3 rounds and return the empty visible subset,
/// never hang or over-report.
#[test]
fn no_visible_rows_returns_empty_bounded() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    for i in 0..8 {
        let t = insert(&engine, i, vec![i as f32, 0.0, 0.0]);
        engine.delete("t", t).unwrap();
    }
    let hits = q(&engine, &[0.0, 0.0, 0.0], 5);
    assert_eq!(
        hits,
        Vec::new(),
        "empty visible set → empty result, bounded"
    );
    engine.shutdown();
}

// ---------------------------------------------------------------------
// D-3: concurrent DML × concurrent filtered search
// ---------------------------------------------------------------------

use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Concurrent inserts and deletes race continuous filtered searches: every
/// returned hit must be a VISIBLE row whose in-row NodeId matches the
/// candidate (the pipeline's own re-check — so any hit passing it is by
/// construction a real, non-mismatched row), and no tid repeats within a
/// result set. Quiesce: full audit green.
#[test]
fn concurrent_dml_and_search_return_only_visible_rows() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let (engine, oid) = setup(&dir);
    for i in 0..30 {
        insert(&engine, i, vec![i as f32, 0.0, 0.0]);
    }
    let engine = Arc::new(engine);
    // Searchers exit when all FOUR writers are done (a flag set after
    // `scope` would deadlock — scope waits for the searchers).
    let writers_done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let violations = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    std::thread::scope(|scope| {
        // Two inserters (ids 100..150).
        for t in 0..2 {
            let engine = Arc::clone(&engine);
            let writers_done = Arc::clone(&writers_done);
            scope.spawn(move || {
                for i in 0..25 {
                    insert(
                        &engine,
                        100 + t * 25 + i,
                        vec![(100 + t * 25 + i) as f32, 0.0, 0.0],
                    );
                }
                writers_done.fetch_add(1, Ordering::Relaxed);
            });
        }
        // Two deleters (of the seeded rows).
        for t in 0..2 {
            let engine = Arc::clone(&engine);
            let writers_done = Arc::clone(&writers_done);
            let violations = Arc::clone(&violations);
            scope.spawn(move || {
                let rows = engine.scan("t", None).unwrap();
                for (tid, _) in rows.iter().skip(t * 7).take(7) {
                    if let Err(e) = engine.delete("t", *tid) {
                        violations
                            .lock()
                            .unwrap()
                            .push(format!("delete {tid:?} failed: {e}"));
                    }
                }
                writers_done.fetch_add(1, Ordering::Relaxed);
            });
        }
        // Two searchers.
        for s in 0..2 {
            let engine = Arc::clone(&engine);
            let writers_done = Arc::clone(&writers_done);
            let violations = Arc::clone(&violations);
            scope.spawn(move || {
                while writers_done.load(Ordering::Relaxed) < 4 {
                    let query = vec![s as f32, 0.0, 0.0];
                    let hits = engine
                        .hnsw_search_visible("t", &query, 5, None, &Snapshot::everything())
                        .unwrap();
                    let mut seen = std::collections::HashSet::new();
                    for (tid, _) in &hits {
                        if !seen.insert(*tid) {
                            violations
                                .lock()
                                .unwrap()
                                .push(format!("duplicate tid {tid:?} in one result set"));
                        }
                        // The hit must name a REAL row (slot occupied) —
                        // "no phantom rows". (The "visible at search time"
                        // property is pinned by the D-1/D-2 unit tests; a
                        // row may be legitimately deleted between the
                        // search and this re-check, so re-scanning for
                        // visibility would be a TOCTOU false positive.)
                        let guard = engine.storage().buffer_pool().pin(tid.page_id).unwrap();
                        let page: &[u8; pg_storage::types::PAGE_SIZE] =
                            guard.page().try_into().unwrap();
                        let exists = pg_am_heap::SlottedPage::tuple(page, tid.slot_id)
                            .unwrap()
                            .is_some();
                        if !exists {
                            violations
                                .lock()
                                .unwrap()
                                .push(format!("hit {tid:?} names a non-existent row"));
                        }
                    }
                }
            });
        }
    });

    assert!(
        violations.lock().unwrap().is_empty(),
        "visibility violations: {:?}",
        violations.lock().unwrap()
    );

    // Quiesce: full audit green (§12.5's quiesce-first premise).
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine
        .open_hnsw_index(
            meta,
            &pg_am_hnsw::ExpectedParams {
                dim: DIM,
                m: 16,
                m_max0: 32,
                ef_construction: 200,
                ef_search_default: 64,
                metric: Metric::L2,
                selection: NeighborSelection::Heuristic,
            },
        )
        .unwrap();
    let report = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    assert_eq!(report.initializing_count, 0);
    assert_eq!(report.orphan_entry_count, 0);

    engine.shutdown();
    let _ = dir;
}

/// Stage D review P2: the map entry's removal waits for the snapshot
/// watermark — a reader holding a snapshot OLDER than the delete must
/// still resolve the row through the map (repeatable-read on the ANN
/// path).
#[test]
fn old_snapshot_keeps_seeing_deleted_row_until_watermark() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let ta = insert(&engine, 1, vec![1.0, 0.0, 0.0]);

    // A reader transaction opens its snapshot BEFORE the delete commits.
    let reader = engine.begin_txn().unwrap();
    let snap = reader.snapshot();

    // Delete + commit: post-commit cleanup runs, but the watermark holds
    // the map entry back (the reader's snapshot is older than the delete).
    engine.delete("t", ta).unwrap();

    // The OLD snapshot must still see the row (repeatable read on ANN).
    let hits = engine
        .hnsw_search_visible("t", &[1.0, 0.0, 0.0], 1, None, &snap)
        .unwrap();
    assert_eq!(hits.len(), 1, "the old snapshot must still resolve the row");
    assert_eq!(hits[0].0, ta);

    // A FRESH (everything) snapshot must NOT see it.
    let fresh = engine
        .hnsw_search_visible("t", &[1.0, 0.0, 0.0], 1, None, &Snapshot::everything())
        .unwrap();
    assert!(
        fresh.is_empty(),
        "the delete is committed — fresh snapshots don't see it"
    );

    // After the reader ends, a later delete's cleanup runs under a clear
    // watermark (the deferred entry itself is reclaimed by the Stage E
    // dead-row channel — registered).
    reader.abort().unwrap();
    let t2 = insert(&engine, 2, vec![0.0, 1.0, 0.0]);
    engine.delete("t", t2).unwrap();
    assert_eq!(
        engine.hnsw_map_get(oid, 1).unwrap(),
        None,
        "watermark clear: this removal lands"
    );
    engine.shutdown();
}

/// Round-6 P3 regression: an EXPLICIT-transaction delete's post-commit map
/// removal must also land (the watermark read inside TxnHandle::commit
/// must not see the committing transaction's own snapshot registration —
/// otherwise every explicit delete's map entry is deferred forever).
#[test]
fn explicit_txn_delete_removes_map_entry() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    insert(&engine, 1, vec![1.0, 0.0, 0.0]);

    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "DELETE FROM t WHERE id = 1")
        .unwrap();
    txn.commit().unwrap();

    // The map entry must be GONE (no reader was holding a snapshot).
    assert_eq!(
        engine.hnsw_map_get(oid, 0).unwrap(),
        None,
        "explicit-txn delete: the map removal must land (not deferred by our own snapshot)"
    );
    engine.shutdown();
}

/// Round-7 P1 regression: a non-HOT, non-vector-column UPDATE with an old
/// snapshot alive leaves TWO map entries for the node (old tid waits out
/// the watermark). A FRESH snapshot must still find the row — the pipeline
/// judges each map entry for visibility, never just the first.
#[test]
fn update_with_old_snapshot_both_readers_see_the_row() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    // A secondary index on id forces the update non-HOT (an indexed column
    // changes).
    engine.create_index("t", "id").unwrap();
    insert(&engine, 1, vec![1.0, 0.0, 0.0]);

    // Old snapshot opens BEFORE the update.
    let reader = engine.begin_txn().unwrap();
    let old_snap = reader.snapshot();

    // Non-vector, non-HOT update (id is indexed → non-HOT by definition).
    engine
        .exec(None, "UPDATE t SET id = 100 WHERE id = 1")
        .unwrap();

    // Fresh snapshot: must see the row (the new version).
    let fresh = q(&engine, &[1.0, 0.0, 0.0], 1);
    assert_eq!(fresh.len(), 1, "fresh snapshot must find the updated row");
    // Old snapshot: must see it too (the old version).
    let old = engine
        .hnsw_search_visible("t", &[1.0, 0.0, 0.0], 1, None, &old_snap)
        .unwrap();
    assert_eq!(old.len(), 1, "old snapshot must still find the row");
    assert_ne!(
        fresh[0].0, old[0].0,
        "different versions for the two snapshots"
    );
    reader.abort().unwrap();
    engine.shutdown();
}

// ---------------------------------------------------------------------
// Round-8 P1: deferred map entries must not survive vacuum (the map
// obeys the §4.1 phase order like every other index).
// ---------------------------------------------------------------------

/// The direct pin: a watermark-deferred map entry is reclaimed by the
/// NEXT vacuum pass over the dead row — before the row's page can be
/// freed and rehanded.
#[test]
fn vacuum_removes_watermark_deferred_map_entries() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let t0 = insert(&engine, 1, vec![1.0, 0.0, 0.0]);
    let _t1 = insert(&engine, 2, vec![0.0, 1.0, 0.0]);

    // An old snapshot forces the delete's post-commit map removal to defer.
    let reader = engine.begin_txn().unwrap();
    let _old_snap = reader.snapshot();
    engine.delete("t", t0).unwrap();
    assert_eq!(
        engine.hnsw_map_get(oid, 0).unwrap(),
        Some(t0),
        "the watermark defers the removal while the reader is alive"
    );
    reader.abort().unwrap();

    engine.vacuum("t").unwrap();
    assert_eq!(
        engine.hnsw_map_get(oid, 0).unwrap(),
        None,
        "vacuum's dead-row channel removes the deferred entry (the dead row still carried its NodeId)"
    );
    // And the search never returns the deleted row.
    let hits = q(&engine, &[1.0, 0.0, 0.0], 2);
    assert_eq!(hits.len(), 1, "only the surviving row remains");
    engine.shutdown();
}

/// The discriminating pin for the same fix: vacuum must remove ONLY the
/// dead version's entry. A non-HOT non-vector UPDATE reuses the same
/// NodeId for the live new version; its map entry must survive vacuum
/// untouched (the map delete is keyed by the exact dead tid).
#[test]
fn vacuum_keeps_live_entry_of_reused_node_id() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    // A secondary index on id forces the update non-HOT.
    engine.create_index("t", "id").unwrap();
    let t_old = insert(&engine, 1, vec![1.0, 0.0, 0.0]);

    // Old snapshot alive ⟹ the update's old-version map removal defers.
    let reader = engine.begin_txn().unwrap();
    let _old_snap = reader.snapshot();
    engine
        .exec(None, "UPDATE t SET id = 100 WHERE id = 1")
        .unwrap();
    let t_new = engine
        .scan("t", None)
        .unwrap()
        .into_iter()
        .find(|(_, vals)| matches!(&vals[0], Some(pg_am_heap::Datum::Int4(100))))
        .unwrap()
        .0;
    assert_ne!(t_old, t_new, "non-HOT update lands a new version");
    assert_eq!(
        engine.hnsw_map_get(oid, 0).unwrap(),
        Some(t_old),
        "pre-vacuum: the deferred old-version entry sorts first"
    );
    reader.abort().unwrap();

    engine.vacuum("t").unwrap();
    assert_eq!(
        engine.hnsw_map_get(oid, 0).unwrap(),
        Some(t_new),
        "vacuum removed the dead version's entry and ONLY that one"
    );
    // The live row stays searchable through the same NodeId.
    let hits = q(&engine, &[1.0, 0.0, 0.0], 1);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, t_new);
    engine.shutdown();
}

/// Round-8 P1 regression (the probe-proven chain): a watermark-deferred
/// map entry that survives vacuum + the freed heap page handed to ANOTHER
/// table by the global freelist + a colliding node id made table a's ANN
/// search return table b's row. Vacuum must delete the map entries of
/// dead rows (phase 4, before phase 5 frees the page), closing the chain.
///
/// The construction is layout-exact. a's heap spans head + 7 non-head
/// pages; the 7 non-head pages all go fully dead, so vacuum frees them in
/// ascending order and the global LIFO freelist hands them out in
/// reverse: b's head, b's HNSW dir/meta/map pages, b's first node page,
/// one catalog spill page — and b's first heap SPILL page is a's SECOND
/// page (the first freed). Identical row sizes give both tables the same
/// rows-per-page, so b's spill rows carry exactly the node ids of a's
/// deleted rows at the exact same slots — the collision the pre-fix
/// pipeline could not tell apart.
#[test]
fn vacuum_closes_deferred_entries_before_cross_table_page_reuse() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    let wide_schema = || {
        vec![
            ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::Int4,
            },
            // The pad column keeps rows-per-page small so the layout below
            // needs only a handful of rows.
            ColumnDef {
                name: "pad".to_string(),
                col_type: ColumnType::Text,
            },
            ColumnDef {
                name: "v".to_string(),
                col_type: ColumnType::vector(u32::from(DIM)).unwrap(),
            },
        ]
    };
    let row = |id: i32, pad: &str| {
        vec![
            pg_engine::Value::from(Some(pg_am_heap::Datum::Int4(id))),
            pg_engine::Value::from(Some(pg_am_heap::Datum::Text(pad.to_string()))),
            pg_engine::Value::from(Some(pg_am_heap::Datum::Vector(vec![id as f32, 0.0, 0.0]))),
        ]
    };
    engine.create_table("a", &wide_schema()).unwrap();
    let (a_oid, _) = engine
        .create_hnsw_index_on(
            "a",
            "v",
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            0x5EED,
        )
        .unwrap();

    // Fill a until its heap spans head + 7 non-head pages.
    let pad = "x".repeat(2000);
    let mut a_rows: Vec<(u32, Tid)> = Vec::new();
    let mut chain: Vec<PageId> = Vec::new();
    for i in 0..10_000i32 {
        let tid = engine.insert("a", &row(i, &pad)).unwrap();
        a_rows.push((i as u32, tid));
        if !chain.contains(&tid.page_id) {
            chain.push(tid.page_id);
        }
        if chain.len() == 8 {
            break;
        }
    }
    assert_eq!(chain.len(), 8, "a must span head + 7 non-head pages");
    let head = chain[0];
    let mut freed = chain[1..].to_vec();
    freed.sort();
    let first_freed = freed[0];

    // Hold an old snapshot so every delete's map removal defers.
    let reader = engine.begin_txn().unwrap();
    let _old_snap = reader.snapshot();
    let deleted: Vec<(u32, Tid)> = a_rows
        .iter()
        .filter(|(_, t)| t.page_id != head)
        .copied()
        .collect();
    for (_, tid) in &deleted {
        engine.delete("a", *tid).unwrap();
    }
    for (node, tid) in &deleted {
        assert_eq!(
            engine.hnsw_map_get(a_oid, *node).unwrap(),
            Some(*tid),
            "node {node}'s entry must be watermark-deferred, not lost"
        );
    }
    reader.abort().unwrap();

    // THE FIX UNDER TEST: vacuum's phase 4 removes the deferred entries
    // (the dead rows still carry their NodeIds), BEFORE phase 5 frees the
    // pages.
    engine.vacuum("a").unwrap();
    for (node, _) in &deleted {
        assert_eq!(
            engine.hnsw_map_get(a_oid, *node).unwrap(),
            None,
            "vacuum must not let node {node}'s deferred entry outlive its page"
        );
    }

    // Cross-table reuse: b inherits a's freed pages via the global LIFO
    // freelist.
    engine.create_table("b", &wide_schema()).unwrap();
    engine
        .create_hnsw_index_on(
            "b",
            "v",
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            0xB,
        )
        .unwrap();
    let mut b_on_first_freed: Vec<(u32, Tid)> = Vec::new();
    for j in 0..12i32 {
        let tid = engine.insert("b", &row(1000 + j, &pad)).unwrap();
        if tid.page_id == first_freed {
            b_on_first_freed.push((j as u32, tid));
            if b_on_first_freed.len() == 3 {
                break;
            }
        }
    }
    assert_eq!(
        b_on_first_freed.len(),
        3,
        "b's spill rows must land on a's first-freed page {first_freed:?}"
    );

    // a's deleted rows on that page carried nodes `a_nodes`; b's rows now
    // occupying the same slots carry the same node ids (identical row
    // sizes ⟹ identical head capacities ⟹ identical numbering offsets).
    let a_on_first_freed: Vec<(u32, Tid)> = deleted
        .iter()
        .filter(|(_, t)| t.page_id == first_freed)
        .copied()
        .collect();
    assert_eq!(
        a_on_first_freed.len(),
        3,
        "a filled that page before it died"
    );
    for ((a_node, a_tid), (b_node, b_tid)) in a_on_first_freed.iter().zip(b_on_first_freed.iter()) {
        assert_eq!(
            a_tid, b_tid,
            "same slots: a's residual entries point exactly at b's rows"
        );
        assert_eq!(a_node, b_node, "the exact node-id collision at {a_tid:?}");
    }

    // a's ANN search for a deleted row's own vector: the tombstoned node
    // still self-recalls, but with the map entry gone the candidate is
    // discarded — pre-fix it resolved through the residual entry to b's
    // row and passed the in-row NodeId re-check (the leak).
    let (probe_node, _) = a_on_first_freed[0];
    let hits = engine
        .hnsw_search_visible(
            "a",
            &[probe_node as f32, 0.0, 0.0],
            5,
            None,
            &Snapshot::everything(),
        )
        .unwrap();
    let live_a: std::collections::HashSet<Tid> = engine
        .scan("a", None)
        .unwrap()
        .into_iter()
        .map(|(t, _)| t)
        .collect();
    assert!(
        !live_a.is_empty(),
        "a's surviving head rows stay searchable (sanity)"
    );
    assert!(
        !hits.is_empty(),
        "the fix must not over-discard: a's live rows still match the query"
    );
    for (tid, _) in &hits {
        assert!(
            live_a.contains(tid),
            "cross-table leak: a's search returned {tid:?}, which is not a live row of a"
        );
    }
    engine.shutdown();
}
