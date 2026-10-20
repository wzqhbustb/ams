//! M6 Stage D: visibility-filtered ANN search (tech-selection §7) — the
//! five-step pipeline pins: committed rows visible, in-flight/aborted/
//! deleted rows hidden, slot-reuse mismatches discarded, ef-retry rounds.

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig};
use pg_storage::types::Tid;
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
