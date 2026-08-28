//! M2a Stage K acceptance: the programmatic `Engine` API end to end.
//!
//! Acceptance command: `cargo test -p pg-engine --test m2a_integration`

use std::collections::HashMap;
use std::sync::Arc;

use pg_am_heap::access_method::{AccessMethod, InsertContext, RelationDesc};
use pg_am_heap::tuple::{encode_tuple, TupleHeader};
use pg_engine::{ColumnDef, ColumnType, Datum, Engine, EngineConfig, EngineError, Predicate, Tid};
use pg_storage::types::{PageId, TxnId};
use pg_txn::{ClogAccessor, Snapshot, TxnState};

use tempfile::TempDir;

/// The schema every test table uses unless stated otherwise.
fn schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "id".to_string(),
            col_type: ColumnType::Int4,
        },
        ColumnDef {
            name: "name".to_string(),
            col_type: ColumnType::Text,
        },
    ]
}

fn row(id: i32, name: &str) -> Vec<Option<Datum>> {
    vec![Some(Datum::Int4(id)), Some(Datum::Text(name.to_string()))]
}

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// Rows as an id → name map, for order-independent comparison.
fn rows_by_id(rows: &[(Tid, Vec<Option<Datum>>)]) -> HashMap<i32, String> {
    rows.iter()
        .map(|(_, vals)| match (&vals[0], &vals[1]) {
            (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
            other => panic!("unexpected row shape: {other:?}"),
        })
        .collect()
}

#[test]
fn create_insert_scan_update_delete_drop_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());

    let oid = engine.create_table("t", &schema()).unwrap();
    assert!(oid.0 >= pg_engine::Oid::FIRST_USER.0);

    let tid_a = engine.insert("t", &row(1, "alice")).unwrap();
    let tid_b = engine.insert("t", &row(2, "bob")).unwrap();
    assert_ne!(tid_a, tid_b, "two inserts must not share a slot");

    // Full scan.
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows_by_id(&rows),
        HashMap::from([(1, "alice".into()), (2, "bob".into())])
    );

    // Predicate scan (single-column equality).
    let rows = engine
        .scan(
            "t",
            Some(Predicate::Eq {
                col_index: 0,
                value: Datum::Int4(2),
            }),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, tid_b);
    assert_eq!(rows[0].1[1], Some(Datum::Text("bob".to_string())));

    // Update replaces the row version (new TID, new value).
    let tid_b2 = engine.update("t", tid_b, &row(2, "bob2")).unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 2, "update must not change the row count");
    assert_eq!(
        rows_by_id(&rows),
        HashMap::from([(1, "alice".into()), (2, "bob2".into())])
    );
    // The old version is invisible: no row at the old TID.
    assert!(rows.iter().all(|(tid, _)| *tid != tid_b));

    // Delete removes the row from visibility.
    engine.delete("t", tid_a).unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, tid_b2);

    // Drop removes the table from the registry.
    engine.drop_table("t").unwrap();
    assert!(matches!(
        engine.scan("t", None),
        Err(EngineError::TableNotFound(_))
    ));

    engine.shutdown();
}

#[test]
fn reopen_persists_tables_and_rows() {
    let tmp = TempDir::new().unwrap();

    {
        let engine = open(tmp.path());
        engine.create_table("a", &schema()).unwrap();
        engine.create_table("b", &schema()).unwrap();
        for i in 0..50 {
            engine.insert("a", &row(i, &format!("a-{i}"))).unwrap();
        }
        for i in 0..30 {
            engine.insert("b", &row(i, &format!("b-{i}"))).unwrap();
        }
        // No explicit checkpoint: recovery must rebuild state from the WAL.
        engine.shutdown();
    }

    {
        let engine = open(tmp.path());
        // The registry was rebuilt from pg_class + pg_attribute + relpages.
        for table in ["a", "b"] {
            let entry = engine
                .describe_table(table)
                .unwrap_or_else(|| panic!("missing {table}"));
            assert_eq!(entry.columns, schema());
        }
        let rows_a = engine.scan("a", None).unwrap();
        assert_eq!(rows_a.len(), 50);
        let map_a = rows_by_id(&rows_a);
        for i in 0..50 {
            assert_eq!(map_a.get(&i).unwrap(), &format!("a-{i}"));
        }
        let rows_b = engine.scan("b", None).unwrap();
        assert_eq!(rows_b.len(), 30);
        // DML keeps working on the rebuilt registry.
        engine.insert("a", &row(100, "after-reopen")).unwrap();
        assert_eq!(engine.scan("a", None).unwrap().len(), 51);
        engine.shutdown();
    }
}

/// Engine-level proof that the real CLOG is wired into scans (Stage J's
/// `heap_abort_visibility` semantics through the assembled engine): a row
/// written and then aborted via the engine's own TxnManager + HeapAM must
/// be invisible to `Engine::scan`; a committed control row must be visible.
#[test]
fn abort_invisible() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_table("t", &schema()).unwrap();

    let entry = engine.describe_table("t").unwrap();
    let col_types = [ColumnType::Int4, ColumnType::Text];
    let rel = RelationDesc {
        rel_oid: entry.oid,
        first_page: entry.first_page,
        columns: &col_types,
    };

    // Transaction A: insert through the engine's heap AM, then ABORT.
    let xid_a = engine.txn_manager().begin_txn();
    let mut snap_a = Snapshot::everything();
    snap_a.set_current_xid(xid_a);
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
        &row(1, "aborted"),
    )
    .unwrap();
    engine
        .heap()
        .insert(InsertContext {
            rel,
            snapshot: &snap_a,
            tuple: &tuple,
            out_tid: None,
        })
        .unwrap();
    engine.txn_manager().abort_txn(xid_a).unwrap();

    // The engine's scan consults the shared CLOG: the aborted row is gone.
    assert!(
        engine.scan("t", None).unwrap().is_empty(),
        "aborted transaction's row must be invisible to Engine::scan"
    );

    // Control: a committed row through the normal API is visible.
    engine.insert("t", &row(2, "committed")).unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Some(Datum::Text("committed".to_string())));

    engine.shutdown();
}

/// The M2a crash-consistency gate: N committed rows across many pages, a
/// simulated kill -9 (`mem::forget` — no shutdown, no final checkpoint),
/// reopen, and every row must be back exactly as written.
///
/// N defaults to 2000 (spans multiple heap pages); set `M2A_CRASH_ROWS`
/// higher (e.g. 1_000_000) for the full-scale run. Loading is parallelized
/// across `LOAD_THREADS` threads (each owns a deterministic id stripe, so
/// the expected content is unchanged) — a single-threaded load would make
/// the 1M-row run take over an hour of pure fsync latency.
#[test]
fn crash_consistency_multi_page() {
    const LOAD_THREADS: usize = 32;
    let n: usize = std::env::var("M2A_CRASH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let tmp = TempDir::new().unwrap();

    {
        let engine = Arc::new(open(tmp.path()));
        engine.create_table("t", &schema()).unwrap();
        std::thread::scope(|s| {
            for t in 0..LOAD_THREADS {
                let engine = Arc::clone(&engine);
                s.spawn(move || {
                    let mut i = t;
                    while i < n {
                        engine
                            .insert("t", &row(i as i32, &format!("row-{i}")))
                            .unwrap();
                        i += LOAD_THREADS;
                    }
                });
            }
        });
        // kill -9: no checkpoint, no shutdown, no Drop.
        std::mem::forget(engine);
    }

    {
        let engine = open(tmp.path());
        let rows = engine.scan("t", None).unwrap();
        assert_eq!(rows.len(), n, "row count changed across crash recovery");
        let map = rows_by_id(&rows);
        assert_eq!(map.len(), n, "duplicate ids after recovery");
        for i in 0..n as i32 {
            assert_eq!(
                map.get(&i)
                    .unwrap_or_else(|| panic!("row {i} missing after recovery")),
                &format!("row-{i}"),
                "row {i} content changed across crash recovery"
            );
        }
        engine.shutdown();
    }
}

#[test]
fn duplicate_create_and_missing_drop_errors() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());

    engine.create_table("t", &schema()).unwrap();
    assert!(matches!(
        engine.create_table("t", &schema()),
        Err(EngineError::TableExists(name)) if name == "t"
    ));

    assert!(matches!(
        engine.drop_table("nope"),
        Err(EngineError::TableNotFound(name)) if name == "nope"
    ));
    assert!(matches!(
        engine.insert("nope", &row(1, "x")),
        Err(EngineError::TableNotFound(_))
    ));
    assert!(matches!(
        engine.scan(
            "t",
            Some(Predicate::Eq {
                col_index: 5,
                value: Datum::Int4(1)
            })
        ),
        Err(EngineError::InvalidPredicate(_))
    ));

    // Drop + recreate under the same name works (fresh OID, fresh chain).
    engine.insert("t", &row(1, "old")).unwrap();
    engine.drop_table("t").unwrap();
    let oid = engine.create_table("t", &schema()).unwrap();
    assert!(engine.scan("t", None).unwrap().is_empty());
    engine.insert("t", &row(2, "new")).unwrap();
    assert_eq!(engine.scan("t", None).unwrap().len(), 1);

    // The freed pages of the first "t" may be reused by the second — the
    // registry and AM cache must route only to the new chain.
    let entry = engine.describe_table("t").unwrap();
    assert_eq!(entry.oid, oid);

    engine.shutdown();
}

/// Regression guard for the CLOG x checkpoint gap, now closed natively by
/// the M2b disk CLOG (Stage L): recovery replays only from the checkpoint
/// redo point, so a commit recorded before the checkpoint is never
/// replayed. With the disk CLOG the checkpoint's `ClogFlush` hook has
/// already fsynced those bits to the segment files, so the committed AND
/// aborted states survive — including ones driven through the
/// `txn_manager` back door. No `clog-snapshot.bin` is involved anymore.
#[test]
fn clog_survives_checkpoint_and_crash() {
    let tmp = TempDir::new().unwrap();

    {
        let engine = open(tmp.path());
        engine.create_table("t", &schema()).unwrap();
        engine
            .insert("t", &row(1, "committed-before-checkpoint"))
            .unwrap();

        // An aborted row via the engine's own TxnManager (the back door);
        // its terminal state lands in the same disk CLOG.
        let entry = engine.describe_table("t").unwrap();
        let col_types = [ColumnType::Int4, ColumnType::Text];
        let xid = engine.txn_manager().begin_txn();
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
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
            &row(2, "aborted-before-checkpoint"),
        )
        .unwrap();
        engine
            .heap()
            .insert(InsertContext {
                rel: RelationDesc {
                    rel_oid: entry.oid,
                    first_page: entry.first_page,
                    columns: &col_types,
                },
                snapshot: &snap,
                tuple: &tuple,
                out_tid: None,
            })
            .unwrap();
        engine.txn_manager().abort_txn(xid).unwrap();

        engine.checkpoint().unwrap();
        // Crash AFTER the checkpoint: the WAL prefix holding both the
        // commit and the abort records may be recycled by the checkpoint.
        std::mem::forget(engine);
    }

    {
        let engine = open(tmp.path());
        let rows = engine.scan("t", None).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "committed row lost across checkpoint+crash (disk CLOG flush missing)"
        );
        assert_eq!(rows[0].1[0], Some(Datum::Int4(1)));

        // New work after the reopen keeps the full history: commit another
        // row, checkpoint again, crash again.
        engine
            .insert("t", &row(3, "committed-after-reopen"))
            .unwrap();
        engine.checkpoint().unwrap();
        std::mem::forget(engine);
    }

    {
        let engine = open(tmp.path());
        let rows = engine.scan("t", None).unwrap();
        assert_eq!(
            rows.len(),
            2,
            "disk CLOG must carry both sessions' terminal states across checkpoints"
        );
        let map = rows_by_id(&rows);
        assert_eq!(map.get(&1).unwrap(), "committed-before-checkpoint");
        assert_eq!(map.get(&3).unwrap(), "committed-after-reopen");
        engine.shutdown();
    }
}

#[test]
fn catalog_survives_reopen_after_ddl() {
    let tmp = TempDir::new().unwrap();

    {
        let engine = open(tmp.path());
        for (name, id) in [("t1", 1), ("t2", 2), ("t3", 3)] {
            engine.create_table(name, &schema()).unwrap();
            engine.insert(name, &row(id, name)).unwrap();
        }
        engine.checkpoint().unwrap();
        engine.shutdown();
    }

    {
        let engine = open(tmp.path());
        for (name, id) in [("t1", 1), ("t2", 2), ("t3", 3)] {
            let rows = engine.scan(name, None).unwrap();
            assert_eq!(rows.len(), 1, "{name} lost its row across reopen");
            assert_eq!(rows[0].1[0], Some(Datum::Int4(id)));
        }
        // A dropped table stays dropped across reopen (relkind = 'd').
        engine.drop_table("t2").unwrap();
        engine.checkpoint().unwrap();
        engine.shutdown();
    }

    {
        let engine = open(tmp.path());
        assert!(engine.describe_table("t2").is_none());
        assert!(matches!(
            engine.scan("t2", None),
            Err(EngineError::TableNotFound(_))
        ));
        assert_eq!(engine.scan("t1", None).unwrap().len(), 1);
        assert_eq!(engine.scan("t3", None).unwrap().len(), 1);
        engine.shutdown();
    }
}

/// 100 threads x 100 inserts through the shared engine: no slot conflicts
/// (TIDs unique), exact row count. The criterion bench
/// (`benches/m2a_100_threads.rs`) scales this to 100 x 1000 and measures
/// TPS; this test keeps the correctness gate cheap.
#[test]
fn concurrent_inserts_have_unique_tids_and_exact_count() {
    const THREADS: usize = 100;
    const OPS: usize = 100;

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.create_table("t", &schema()).unwrap();

    let tids: Arc<std::sync::Mutex<Vec<Tid>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let engine = Arc::clone(&engine);
            let tids = Arc::clone(&tids);
            s.spawn(move || {
                for i in 0..OPS {
                    let tid = engine
                        .insert("t", &row((t * OPS + i) as i32, "concurrent"))
                        .unwrap();
                    tids.lock().unwrap().push(tid);
                }
            });
        }
    });

    let tids = tids.lock().unwrap();
    assert_eq!(tids.len(), THREADS * OPS);
    let unique: std::collections::HashSet<_> = tids.iter().collect();
    assert_eq!(unique.len(), tids.len(), "slot conflict: duplicate TIDs");

    assert_eq!(engine.scan("t", None).unwrap().len(), THREADS * OPS);

    // plan §K 并发验收四项的另两项：xmin 单调与 CLOG 一致。
    // XID clock 必须越过每次 auto-commit insert 分配的 XID（单调无复用）。
    let next_xid = engine.storage().txn_id_clock().current();
    assert!(
        next_xid.0 > (THREADS * OPS) as u64,
        "xid clock must advance past every auto-commit insert: {next_xid:?}"
    );
    // 负载期间分配的每个 XID 都必须到达终态 Committed——没有
    // InProgress 残留（commit 丢失）也没有缺项（CLOG 写入丢失）。
    let clog = Arc::clone(engine.clog());
    for xid in 1..next_xid.0 {
        assert_eq!(
            clog.get_state(TxnId(xid)),
            TxnState::Committed,
            "xid {xid} left in a non-committed state after the concurrent load"
        );
    }

    engine.shutdown();
}

/// Regression for the checkpoint × in-flight commit race (Stage K review
/// P0-1, re-justified for the M2b disk CLOG): a commit whose WAL append
/// landed before the checkpoint's begin_lsn but whose `clog.set_state` ran
/// after the checkpoint's CLOG flush would be present in NEITHER the
/// fsynced CLOG NOR the replay — the row was committed, yet invisible after
/// restart. The commit barrier (statements hold a read guard,
/// `Engine::checkpoint` the write guard across the whole checkpoint)
/// closes the window; this test hammers it and checks visibility after a
/// reopen, which is where the window actually bites.
#[test]
fn commits_concurrent_with_checkpoint_all_survive_restart() {
    const COMMITTERS: usize = 8;
    const OPS_PER: usize = 50;
    const CHECKPOINTS: usize = 10;

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.create_table("t", &schema()).unwrap();

    std::thread::scope(|s| {
        for t in 0..COMMITTERS {
            let engine = Arc::clone(&engine);
            s.spawn(move || {
                for i in 0..OPS_PER {
                    engine
                        .insert("t", &row((t * OPS_PER + i) as i32, "race"))
                        .unwrap();
                }
            });
        }
        let engine = Arc::clone(&engine);
        s.spawn(move || {
            for _ in 0..CHECKPOINTS {
                engine.checkpoint().unwrap();
            }
        });
    });

    assert_eq!(engine.scan("t", None).unwrap().len(), COMMITTERS * OPS_PER);
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(
        engine.scan("t", None).unwrap().len(),
        COMMITTERS * OPS_PER,
        "a commit fell into the begin_lsn→CLOG-flush window"
    );
    engine.shutdown();
}

/// Regression for the page-reuse residual-content bug (Stage K review
/// P0-2): a freelist-reused page keeps its previous tenant's bytes on disk.
/// Without a WAL-logged init, recovery would read those bytes back — heap
/// redo then either hard-fails on slot divergence or, worse, `seed_from_chain`
/// surfaces the previous tenant's rows inside the new relation. The fix
/// (`log_page_init`'s post-image FPI) makes recovery restore the freshly
/// initialized page instead.
#[test]
fn reused_page_recovers_as_fresh_after_crash() {
    let tmp = TempDir::new().unwrap();

    // Table A: rows on disk (checkpoint), then dropped — its pages go to the
    // freelist with A's content still on disk.
    {
        let engine = open(tmp.path());
        engine.create_table("a", &schema()).unwrap();
        for i in 0..50 {
            engine.insert("a", &row(i, "tenant-a")).unwrap();
        }
        engine.checkpoint().unwrap();
        engine.drop_table("a").unwrap();
        // No shutdown of clean state — fall through with pages unflushed.
        std::mem::forget(engine);
    }

    // Table B reuses A's freed pages. Its inserts are committed (WAL
    // durable) but the page contents are NOT flushed before the "crash".
    {
        let engine = open(tmp.path());
        engine.create_table("b", &schema()).unwrap();
        for i in 0..50 {
            engine.insert("b", &row(1000 + i, "tenant-b")).unwrap();
        }
        std::mem::forget(engine);
    }

    // Reopen: recovery must rebuild B — and ONLY B — on the reused pages.
    let engine = open(tmp.path());
    let rows = engine.scan("b", None).unwrap();
    assert_eq!(rows.len(), 50, "B must recover its exact rows: {rows:?}");
    for (_tid, values) in &rows {
        let id = match &values[0] {
            Some(Datum::Int4(v)) => *v,
            other => panic!("unexpected id column: {other:?}"),
        };
        assert!(id >= 1000, "previous tenant A's row leaked into B: id={id}");
    }
    engine.shutdown();
}

// ---------------------------------------------------------------------------
// M2b Stage L: disk-backed CLOG end to end
// ---------------------------------------------------------------------------

/// Drive one explicit transaction through the engine's own TxnManager +
/// HeapAM (the back door), inserting `row(id, name)` into table `entry`'s
/// relation, then commit or abort. Returns the transaction's XID.
fn drive_explicit_txn(engine: &Engine, table: &str, id: i32, name: &str, commit: bool) -> TxnId {
    let entry = engine.describe_table(table).unwrap();
    let col_types = [ColumnType::Int4, ColumnType::Text];
    let xid = engine.txn_manager().begin_txn();
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xid);
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
        &row(id, name),
    )
    .unwrap();
    engine
        .heap()
        .insert(InsertContext {
            rel: RelationDesc {
                rel_oid: entry.oid,
                first_page: entry.first_page,
                columns: &col_types,
            },
            snapshot: &snap,
            tuple: &tuple,
            out_tid: None,
        })
        .unwrap();
    if commit {
        engine.txn_manager().commit_txn(xid).unwrap();
    } else {
        engine.txn_manager().abort_txn(xid).unwrap();
    }
    xid
}

/// Stage L acceptance: the engine's CLOG is the disk SLRU. After a
/// checkpoint, the terminal states must be physically present in the CLOG
/// segment file — verified by a raw positioned read of
/// `{data_dir}/clog/clog-00000000.log` that bypasses the `ClogBuffer`
/// entirely — and must survive a crash (`mem::forget`) + reopen.
#[test]
fn test_disk_clog_end_to_end() {
    use pg_txn::clog_file::{byte_offset_of_xid, get_nibble, txn_state_from_nibble};

    let tmp = TempDir::new().unwrap();

    let (committed_xid, aborted_xid) = {
        let engine = open(tmp.path());
        engine.create_table("t", &schema()).unwrap();
        let committed_xid = drive_explicit_txn(&engine, "t", 1, "committed", true);
        let aborted_xid = drive_explicit_txn(&engine, "t", 2, "aborted", false);

        // The checkpoint's ClogFlush hook (CheckpointBegin..End) is the only
        // fsync of the CLOG: after it returns, both nibbles are durable.
        engine.checkpoint().unwrap();
        // Crash AFTER the checkpoint: no shutdown, no Drop.
        std::mem::forget(engine);
        (committed_xid, aborted_xid)
    };

    // Raw pread of the segment file, no CLOG involved: the nibbles landed.
    let segment = tmp.path().join("clog").join("clog-00000000.log");
    let read_state = |xid: TxnId| {
        use std::os::unix::fs::FileExt;
        let file = std::fs::File::open(&segment).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact_at(&mut byte, byte_offset_of_xid(xid))
            .unwrap();
        txn_state_from_nibble(get_nibble(byte[0], xid))
    };
    assert_eq!(
        read_state(committed_xid),
        TxnState::Committed,
        "committed xid {committed_xid} not fsynced into the CLOG segment"
    );
    assert_eq!(
        read_state(aborted_xid),
        TxnState::Aborted,
        "aborted xid {aborted_xid} not fsynced into the CLOG segment"
    );
    assert!(
        !tmp.path().join("clog-snapshot.bin").exists(),
        "the M2a CLOG snapshot bridge must be gone"
    );

    // Reopen on a cold ClogBuffer: the states come from disk alone (the WAL
    // prefix with the commit/abort records is behind the redo point).
    let engine = open(tmp.path());
    assert_eq!(engine.clog().get_state(committed_xid), TxnState::Committed);
    assert_eq!(engine.clog().get_state(aborted_xid), TxnState::Aborted);
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1, "aborted row visible after reopen: {rows:?}");
    assert_eq!(rows[0].1[0], Some(Datum::Int4(1)));
    engine.shutdown();
}

/// Stage L acceptance: the exact gap the M2a `clog-snapshot.bin` bridge
/// existed for. Commits roll the WAL past segment 0, a checkpoint advances
/// the redo point AND physically recycles the segment holding the commit
/// records, then a crash + reopen must still see every committed row —
/// served by the disk CLOG alone. No `clog-snapshot.bin` is ever written.
#[test]
fn test_clog_survives_wal_recycle() {
    const ROWS: i32 = 200;

    let tmp = TempDir::new().unwrap();
    let mut config = EngineConfig::new(tmp.path());
    // Small WAL segments so the checkpoint actually recycles the segment
    // holding the commit records (the default 16 MiB segment would keep
    // everything in segment 0 and make the recycling assertion vacuous).
    // 16 KiB is the floor here: a full-page image is ~8 KiB + header, and a
    // record must fit inside one segment.
    config.storage.wal_segment_size = 16384;

    {
        let engine = Engine::open(tmp.path(), config.clone()).unwrap();
        engine.create_table("t", &schema()).unwrap();
        for i in 0..ROWS {
            engine.insert("t", &row(i, "durable")).unwrap();
        }
        engine.checkpoint().unwrap();
        // Segment 0 — which held every commit record — must be gone, or the
        // test exercises nothing.
        assert!(
            !tmp.path().join("wal").join("wal-00000001.log").exists(),
            "WAL segment 0 was not recycled; test is vacuous"
        );
        // Crash AFTER the checkpoint + recycle.
        std::mem::forget(engine);
    }

    {
        let engine = Engine::open(tmp.path(), config).unwrap();
        assert!(
            !tmp.path().join("clog-snapshot.bin").exists(),
            "the M2a CLOG snapshot bridge must be gone"
        );
        // Recovery replays only from the redo point, where none of the 200
        // commit records exist anymore; visibility comes from the fsynced
        // CLOG nibbles.
        let rows = engine.scan("t", None).unwrap();
        assert_eq!(
            rows.len(),
            ROWS as usize,
            "committed rows lost after checkpoint recycled their WAL segment"
        );
        let map = rows_by_id(&rows);
        for i in 0..ROWS {
            assert_eq!(map.get(&i).unwrap(), "durable");
        }
        engine.shutdown();
    }
}

/// M2a → M2b upgrade path: a directory last written by the M2a engine may
/// hold a `clog-snapshot.bin` bridge file with pre-checkpoint commit states
/// that exist nowhere else (the WAL prefix was recycled). Stage L replaced
/// the bridge with the disk CLOG; opening such a directory must migrate the
/// snapshot into the disk CLOG instead of silently ignoring it (which would
/// lose those states — and violate the on-disk compatibility promise).
#[test]
fn m2a_clog_snapshot_is_migrated_into_disk_clog() {
    let tmp = TempDir::new().unwrap();

    // Build a normal database with one committed row (so visibility of the
    // migrated state is checkable end to end), then hand-craft an M2a-era
    // clog-snapshot.bin holding the commit state of the row's XID.
    let xid;
    {
        let engine = open(tmp.path());
        engine.create_table("t", &schema()).unwrap();
        xid = {
            // Use the txn backdoor to learn the XID of an insert.
            let x = engine.txn_manager().begin_txn();
            engine.insert("t", &row(1, "legacy")).unwrap();
            engine.txn_manager().commit_txn(x).unwrap();
            x
        };
        engine.shutdown();

        // Write the legacy file in the exact M2a format (magic, version,
        // count, entries, fnv1a).
        let mut buf = Vec::new();
        buf.extend_from_slice(&0x5047_5255_5354_434Cu64.to_le_bytes()); // "PGRUSTCL"
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes()); // two entries
        buf.extend_from_slice(&xid.0.to_le_bytes());
        buf.push(1u8); // Committed
        buf.extend_from_slice(&999u64.to_le_bytes());
        buf.push(2u8); // Aborted (state-only entry)
        let checksum = {
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
            for &b in &buf {
                hash ^= u64::from(b);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            hash
        };
        buf.extend_from_slice(&checksum.to_le_bytes());
        std::fs::write(tmp.path().join("clog-snapshot.bin"), &buf).unwrap();
    }

    // M2b open: the legacy snapshot must be migrated, not ignored.
    let engine = open(tmp.path());
    assert!(
        !tmp.path().join("clog-snapshot.bin").exists(),
        "legacy file must be renamed after migration"
    );
    assert!(
        tmp.path().join("clog-snapshot.bin.migrated").exists(),
        "migrated copy must be kept for audit"
    );
    assert_eq!(engine.clog().get_state(xid), pg_txn::TxnState::Committed);
    assert_eq!(
        engine.clog().get_state(TxnId(999)),
        pg_txn::TxnState::Aborted
    );
    assert_eq!(engine.scan("t", None).unwrap().len(), 1);

    // The migration must be durable BEFORE any M2b checkpoint runs (the
    // legacy file was renamed away, and M2a already recycled the WAL prefix
    // — an un-flushed migration would be unrecoverable after a crash).
    // Verify the nibbles are on disk right now, without a checkpoint.
    {
        use pg_storage::positioned_file::PositionedFile;
        use pg_txn::clog_file::{byte_offset_of_xid, get_nibble, txn_state_from_nibble};
        let seg = PositionedFile::open(tmp.path().join("clog").join("clog-00000000.log")).unwrap();
        let mut byte = [0u8; 1];
        seg.read_exact_at(&mut byte, byte_offset_of_xid(xid))
            .unwrap();
        assert_eq!(
            txn_state_from_nibble(get_nibble(byte[0], xid)),
            pg_txn::TxnState::Committed,
            "migrated state must be fsynced before the legacy file is retired"
        );
    }
    engine.shutdown();
}
