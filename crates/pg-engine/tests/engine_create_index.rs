//! Stage M wave 2 acceptance: blocking `Engine::create_index` over a 100k-row
//! table — point lookups through the engine, native range scans, crash
//! recovery of a finished build, and a clean failure for a build that
//! crashes before its catalog rows commit.

use std::path::Path;
use std::sync::Arc;

use pg_am_btree::BTreeAM;
use pg_am_heap::access_method::InsertContext;
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::AccessMethod;
use pg_engine::{ColumnDef, Engine, EngineConfig, EngineError};
use pg_storage::types::{PageId, Tid, TxnId};
use pg_txn::Snapshot;

use tempfile::TempDir;

const ROWS: i64 = 100_000;

fn schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "k".to_string(),
            col_type: ColumnType::Int8,
        },
        ColumnDef {
            name: "v".to_string(),
            col_type: ColumnType::Int8,
        },
    ]
}

fn open(dir: &Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// Insert `n` rows `(i, i*10)` in ONE transaction through the heap AM
/// directly (a single commit = a single fsync; per-row auto-commit would
/// fsync per row and dominate the test).
fn insert_rows(engine: &Engine, n: i64) -> Vec<Tid> {
    let entry = engine.describe_table("t").unwrap();
    let col_types = [ColumnType::Int8, ColumnType::Int8];
    let xid = engine.txn_manager().begin_txn();
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xid);
    let mut tids = Vec::with_capacity(n as usize);
    for i in 0..n {
        let tuple = encode_tuple(
            TupleHeader::new(
                TxnId::INVALID,
                TxnId::INVALID,
                0,
                [0; 16],
                Tid {
                    page_id: PageId::INVALID,
                    slot_id: 0,
                },
                0,
            ),
            &col_types,
            &[Some(Datum::Int8(i)), Some(Datum::Int8(i * 10))],
        )
        .unwrap();
        let mut tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        engine
            .heap()
            .insert(InsertContext {
                rel: pg_am_heap::access_method::RelationDesc {
                    rel_oid: entry.oid,
                    first_page: entry.first_page,
                    columns: &col_types,
                },
                snapshot: &snap,
                tuple: &tuple,
                out_tid: Some(&mut tid),
            })
            .unwrap();
        tids.push(tid);
    }
    engine.txn_manager().commit_txn(xid).unwrap();
    tids
}

/// Deterministic sample of `count` distinct keys in `[0, ROWS)`.
fn sample_keys(count: u64) -> Vec<i64> {
    let mut keys = Vec::new();
    let mut state: u64 = 0xDEAD_BEEF_CAFE_F00D;
    while keys.len() < count as usize {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        keys.push((state >> 33) as i64 % ROWS);
    }
    keys
}

#[test]
fn create_index_point_lookup_and_range_scan() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_table("t", &schema()).unwrap();
    let tids = insert_rows(&engine, ROWS);

    let index_oid = engine.create_index("t", "k").unwrap();
    assert!(engine.indexes().iter().any(|e| e.index_oid == index_oid));

    // 1000 random keys must all hit, with the right heap TID.
    for k in sample_keys(1000) {
        let got = engine.index_lookup("t", "k", &Datum::Int8(k)).unwrap();
        assert_eq!(
            got,
            Some(tids[k as usize]),
            "index lookup for key {k} must return its heap TID"
        );
    }
    // Keys outside the range miss.
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(-1)).unwrap(),
        None
    );
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(ROWS)).unwrap(),
        None
    );

    // Range scan through the native API: [100, 200) yields exactly 100 rows.
    let index = engine.btree_index("t", "k").unwrap();
    let rows = index
        .range_scan(
            Some(pg_am_btree::encode_i64(100).as_slice()),
            Some(pg_am_btree::encode_i64(200).as_slice()),
        )
        .unwrap();
    assert_eq!(rows.len(), 100);
    for (i, (_, tid)) in rows.iter().enumerate() {
        assert_eq!(*tid, tids[100 + i]);
    }
    index.validate().unwrap();
}

#[test]
fn create_index_survives_crash() {
    let tmp = TempDir::new().unwrap();
    let tids;
    {
        let engine = open(tmp.path());
        engine.create_table("t", &schema()).unwrap();
        tids = insert_rows(&engine, ROWS);
        engine.create_index("t", "k").unwrap();
        engine.storage().wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: no checkpoint, no shutdown
    }

    // Reopen: WAL replay (heap + txn + btree handlers) must rebuild the
    // bulk-loaded tree and the catalog rows.
    let engine = open(tmp.path());
    assert_eq!(engine.indexes().len(), 1);
    for k in sample_keys(200) {
        assert_eq!(
            engine.index_lookup("t", "k", &Datum::Int8(k)).unwrap(),
            Some(tids[k as usize])
        );
    }
    engine.btree_index("t", "k").unwrap().validate().unwrap();
}

/// A build that completes its pages but crashes before the catalog commit
/// must fail clean: replay is happy, no `pg_index` row exists, the table is
/// intact, and a later `create_index` succeeds (the leaked pages are just
/// that — leaked).
#[test]
fn crash_after_build_before_catalog_is_clean() {
    let tmp = TempDir::new().unwrap();
    let meta_page;
    {
        let engine = open(tmp.path());
        engine.create_table("t", &schema()).unwrap();
        let tids = insert_rows(&engine, 10_000);
        // Drive the loader directly (no catalog rows), then crash.
        let btree = BTreeAM::new(
            Arc::clone(engine.storage().buffer_pool()),
            Arc::clone(engine.storage().wal_writer()),
        );
        let entries: Vec<(Vec<u8>, Tid)> = tids
            .iter()
            .enumerate()
            .map(|(i, t)| (pg_am_btree::encode_i64(i as i64).to_vec(), *t))
            .collect();
        let index = btree
            .build_index(pg_engine::Oid(16_500), ColumnType::Int8, entries)
            .unwrap();
        meta_page = index.meta_page();
        engine.storage().wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9 before any catalog row exists
    }

    let engine = open(tmp.path()); // replay must not choke on the orphan pages
    assert!(
        engine.indexes().is_empty(),
        "no catalog rows, no index entry"
    );
    assert!(matches!(
        engine.index_lookup("t", "k", &Datum::Int8(1)),
        Err(EngineError::IndexNotFound(_))
    ));
    // The table itself is intact.
    assert_eq!(engine.scan("t", None).unwrap().len(), 10_000);
    // And a real create_index on the recovered engine works.
    engine.create_index("t", "k").unwrap();
    assert!(engine
        .index_lookup("t", "k", &Datum::Int8(42))
        .unwrap()
        .is_some());
    let _ = meta_page; // (kept for clarity: the orphan tree's meta page)
}

#[test]
fn create_index_rejects_duplicates_and_unknown_columns() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_table("t", &schema()).unwrap();
    insert_rows(&engine, 100);

    engine.create_index("t", "k").unwrap();
    assert!(matches!(
        engine.create_index("t", "k"),
        Err(EngineError::IndexExists(_))
    ));
    assert!(matches!(
        engine.create_index("t", "nope"),
        Err(EngineError::InvalidArgument(_))
    ));
    assert!(matches!(
        engine.create_index("missing_table", "k"),
        Err(EngineError::TableNotFound(_))
    ));
    // Index relations must NOT appear as tables in the DML registry.
    let table_oid = engine.describe_table("t").unwrap().oid;
    let index_name = format!("{}_k_idx", table_oid.0);
    assert!(matches!(
        engine.scan(&index_name, None),
        Err(EngineError::TableNotFound(_))
    ));
}

/// P1-1: DML must maintain every index on the table, in the same
/// transaction as the heap mutation — covering insert, delete, update
/// (key-changing and key-preserving), multiple indexes on one table, and
/// NULL-key skipping.
#[test]
fn dml_maintains_indexes() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_table("t", &schema()).unwrap();
    engine.create_index("t", "k").unwrap();
    engine.create_index("t", "v").unwrap();

    // INSERT through the DML API: the row is indexed on both columns.
    let t1 = engine
        .insert("t", &[Some(Datum::Int8(1)), Some(Datum::Int8(10))])
        .unwrap();
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(1)).unwrap(),
        Some(t1)
    );
    assert_eq!(
        engine.index_lookup("t", "v", &Datum::Int8(10)).unwrap(),
        Some(t1)
    );

    // NULL key: indexed on v only; the k index must not gain an entry.
    let t_null = engine.insert("t", &[None, Some(Datum::Int8(11))]).unwrap();
    assert_eq!(
        engine.index_lookup("t", "v", &Datum::Int8(11)).unwrap(),
        Some(t_null)
    );
    assert_eq!(
        engine
            .btree_index("t", "k")
            .unwrap()
            .range_scan(None, None)
            .unwrap()
            .len(),
        1,
        "NULL keys must be skipped by index maintenance"
    );

    // UPDATE changing the key: old key misses, new key hits with the new
    // TID; the untouched column index now points at the new TID.
    let t2 = engine
        .update("t", t1, &[Some(Datum::Int8(2)), Some(Datum::Int8(10))])
        .unwrap();
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(1)).unwrap(),
        None
    );
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(2)).unwrap(),
        Some(t2)
    );
    assert_eq!(
        engine.index_lookup("t", "v", &Datum::Int8(10)).unwrap(),
        Some(t2)
    );

    // UPDATE not changing the key: still hits, with the new TID.
    let t3 = engine
        .update("t", t2, &[Some(Datum::Int8(2)), Some(Datum::Int8(20))])
        .unwrap();
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(2)).unwrap(),
        Some(t3)
    );
    assert_eq!(
        engine.index_lookup("t", "v", &Datum::Int8(20)).unwrap(),
        Some(t3)
    );
    assert_eq!(
        engine.index_lookup("t", "v", &Datum::Int8(10)).unwrap(),
        None
    );

    // DELETE: both indexes forget the row.
    engine.delete("t", t3).unwrap();
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(2)).unwrap(),
        None
    );
    assert_eq!(
        engine.index_lookup("t", "v", &Datum::Int8(20)).unwrap(),
        None
    );

    // Consistency sampling: every index entry resolves to a live heap row
    // carrying the indexed key. 200 rows through the DML path, half
    // deleted afterwards.
    let mut live = Vec::new();
    for i in 0..200i64 {
        let tid = engine
            .insert("t", &[Some(Datum::Int8(i)), Some(Datum::Int8(i * 7))])
            .unwrap();
        live.push((i, tid));
    }
    for (_k, tid) in live.iter().step_by(2) {
        engine.delete("t", *tid).unwrap();
    }
    let index = engine.btree_index("t", "k").unwrap();
    let entries = index.range_scan(None, None).unwrap();
    // 200 rows minus the 100 even-key deletes (the earlier (NULL, 11) row
    // has no k entry, and the (2, 20) row was deleted above).
    assert_eq!(entries.len(), 100);
    for (key_bytes, tid) in entries.iter().step_by(7) {
        let k = pg_am_btree::decode_i64(key_bytes.clone().try_into().unwrap());
        let rows = engine
            .scan(
                "t",
                Some(pg_engine::Predicate::Eq {
                    col_index: 0,
                    value: Datum::Int8(k),
                }),
            )
            .unwrap();
        assert!(
            rows.iter().any(|(row_tid, _)| row_tid == tid),
            "index entry (k={k}, {tid}) must resolve to a live heap row"
        );
    }
    index.validate().unwrap();
}

/// A `HEAP_ONLY_TUPLE` never got an index entry of its own, so index
/// maintenance for that version has to act on its HOT chain root. Acting on
/// the descendant's own TID instead fails with `EntryNotFound` and leaves the
/// index disagreeing with the heap.
#[test]
fn dml_on_a_hot_descendant_retires_the_chain_root_entry() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_table("t", &schema()).unwrap();
    // A single index, so an update that leaves `k` alone is HOT-eligible.
    engine.create_index("t", "k").unwrap();

    let root = engine
        .insert("t", &[Some(Datum::Int8(1)), Some(Datum::Int8(10))])
        .unwrap();
    let hot = engine
        .update("t", root, &[Some(Datum::Int8(1)), Some(Datum::Int8(11))])
        .unwrap();
    assert_eq!(
        hot.page_id, root.page_id,
        "the new version must have stayed on the page for the update to be HOT"
    );
    assert_ne!(hot, root);
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(1)).unwrap(),
        Some(hot),
        "the chain root's entry must resolve through t_ctid to the HOT version"
    );

    // Key-changing update of the HOT descendant.
    let moved = engine
        .update("t", hot, &[Some(Datum::Int8(2)), Some(Datum::Int8(11))])
        .unwrap();
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(1)).unwrap(),
        None,
        "the retired key must no longer resolve to a live row"
    );
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(2)).unwrap(),
        Some(moved)
    );

    // Same for a DELETE of a HOT descendant.
    let root2 = engine
        .insert("t", &[Some(Datum::Int8(3)), Some(Datum::Int8(30))])
        .unwrap();
    let hot2 = engine
        .update("t", root2, &[Some(Datum::Int8(3)), Some(Datum::Int8(31))])
        .unwrap();
    assert_eq!(hot2.page_id, root2.page_id);
    engine.delete("t", hot2).unwrap();
    assert_eq!(
        engine.index_lookup("t", "k", &Datum::Int8(3)).unwrap(),
        None
    );

    let index = engine.btree_index("t", "k").unwrap();
    assert_eq!(
        index.range_scan(None, None).unwrap().len(),
        1,
        "only the surviving (k=2) row may still own an entry"
    );
    index.validate().unwrap();
}

/// P3-1: when a system catalog page is full, `create_index` must fail with
/// `CatalogFull` from the pre-check — before running the heap scan and bulk
/// load (the failure is cheap and leaves the table fully usable).
#[test]
fn create_index_fails_fast_when_catalog_is_full() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut hit = None;
    for i in 0..500i64 {
        let name = format!("t{i}");
        match engine.create_table(&name, &schema()) {
            Ok(_) => {}
            Err(EngineError::CatalogFull(_)) => {
                hit = Some("create_table");
                break;
            }
            Err(e) => panic!("unexpected create_table error: {e}"),
        }
        match engine.create_index(&name, "k") {
            Ok(_) => {}
            Err(EngineError::CatalogFull(_)) => {
                hit = Some("create_index");
                break;
            }
            Err(e) => panic!("unexpected create_index error: {e}"),
        }
    }
    let which = hit.expect("500 indexed tables must overflow a catalog page");
    eprintln!("catalog full surfaced in {which}");
    // The engine is still fully functional for reads.
    assert!(!engine.indexes().is_empty());
}
