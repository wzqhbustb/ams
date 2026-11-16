//! Phase 4a Stage F: sequence durability and session semantics.

use std::sync::Arc;

use pg_am_heap::access_method::{DeleteContext, InsertContext, RelationDesc};
use pg_am_heap::tuple::{encode_tuple, TupleHeader};
use pg_am_heap::AccessMethod;
use pg_catalog::system_tables::{HEAP_AM_OID, PG_CLASS, PG_RELPAGES};
use pg_engine::{Datum, Engine, EngineConfig, SqlSession};
use pg_storage::types::{PageId, Tid, TxnId};
use pg_txn::Snapshot;
use tempfile::TempDir;

fn writer_snapshot(engine: &Engine) -> (TxnId, Snapshot) {
    let xid = engine.txn_manager().begin_txn();
    let mut snapshot = Snapshot::everything();
    snapshot.set_current_xid(xid);
    (xid, snapshot)
}

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

#[test]
fn nextval_and_currval_survive_reopen_without_inserts() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 2);
    assert_eq!(engine.currval(&session, "s").unwrap(), 2);
    assert!(engine.currval(&SqlSession::new(2), "s").is_err());
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 3);
    engine.shutdown();
}

#[test]
fn nextval_only_crash_does_not_reissue_values() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    std::mem::forget(engine);

    let engine = open(tmp.path());
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 2);
    engine.checkpoint().unwrap();
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 3);
    engine.shutdown();
}

#[test]
fn outer_rollback_does_not_reclaim_sequence_values() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    let txn = engine.begin_txn().unwrap();
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    txn.abort().unwrap();
    assert_eq!(engine.currval(&session, "s").unwrap(), 1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 2);
    engine.shutdown();
}

#[test]
fn concurrent_nextval_is_unique_and_contiguous() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.create_sequence("s").unwrap();
    let mut workers = Vec::new();
    for id in 0..4 {
        let engine = Arc::clone(&engine);
        workers.push(std::thread::spawn(move || {
            let mut session = SqlSession::new(id);
            let mut values = Vec::new();
            for _ in 0..8 {
                values.push(engine.nextval(&mut session, "s").unwrap());
            }
            assert_eq!(
                engine.currval(&session, "s").unwrap(),
                *values.last().unwrap()
            );
            values
        }));
    }
    let mut values = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, (1..=32).collect::<Vec<_>>());
    engine.shutdown();
}

fn int8_rows(result: pg_engine::QueryResult) -> Vec<Vec<Option<pg_engine::Datum>>> {
    let pg_engine::QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows")
    };
    rows
}

#[test]
fn sql_sequence_functions_and_serial_defaults_work() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut session = SqlSession::new(1);
    engine
        .exec_in_session(&mut session, None, "CREATE SEQUENCE s")
        .unwrap();
    assert_eq!(
        int8_rows(
            engine
                .exec_in_session(&mut session, None, "SELECT nextval('s')")
                .unwrap()
        )[0][0],
        Some(pg_engine::Datum::Int8(1))
    );
    assert_eq!(
        int8_rows(
            engine
                .exec_in_session(&mut session, None, "SELECT currval('s')")
                .unwrap()
        )[0][0],
        Some(pg_engine::Datum::Int8(1))
    );

    engine
        .exec_in_session(
            &mut session,
            None,
            "CREATE TABLE items (id SERIAL, note TEXT)",
        )
        .unwrap();
    engine
        .exec_in_session(
            &mut session,
            None,
            "INSERT INTO items (note) VALUES ('a'), ('b')",
        )
        .unwrap();
    engine
        .exec_in_session(
            &mut session,
            None,
            "INSERT INTO items (id, note) VALUES (DEFAULT, 'c'), (99, 'd'), (NULL, 'e')",
        )
        .unwrap();
    let rows = engine.scan("items", None).unwrap();
    assert_eq!(
        rows.iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Some(pg_engine::Datum::Int4(1)),
            Some(pg_engine::Datum::Int4(2)),
            Some(pg_engine::Datum::Int4(3)),
            Some(pg_engine::Datum::Int4(99)),
            None,
        ]
    );
    assert_eq!(
        int8_rows(
            engine
                .exec_in_session(&mut session, None, "SELECT currval('items_id_seq')")
                .unwrap()
        )[0][0],
        Some(pg_engine::Datum::Int8(3))
    );
    engine.shutdown();

    let engine = open(tmp.path());
    engine
        .exec_in_session(&mut session, None, "INSERT INTO items (note) VALUES ('f')")
        .unwrap();
    let rows = engine.scan("items", None).unwrap();
    assert_eq!(rows.last().unwrap().1[0], Some(pg_engine::Datum::Int4(4)));
    engine.shutdown();
}

#[test]
fn serial_rollback_preserves_value_and_currval() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut session = SqlSession::new(1);
    engine
        .exec_in_session(
            &mut session,
            None,
            "CREATE TABLE items (id SERIAL, note TEXT)",
        )
        .unwrap();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec_in_session(
            &mut session,
            Some(&txn),
            "INSERT INTO items (note) VALUES ('rollback')",
        )
        .unwrap();
    txn.abort().unwrap();
    assert!(engine.scan("items", None).unwrap().is_empty());
    assert_eq!(
        int8_rows(
            engine
                .exec_in_session(&mut session, None, "SELECT currval('items_id_seq')")
                .unwrap()
        )[0][0],
        Some(pg_engine::Datum::Int8(1))
    );
    engine
        .exec_in_session(
            &mut session,
            None,
            "INSERT INTO items (note) VALUES ('next')",
        )
        .unwrap();
    assert_eq!(
        engine.scan("items", None).unwrap()[0].1[0],
        Some(pg_engine::Datum::Int4(2))
    );
    engine.shutdown();
}

#[test]
fn redo_only_high_water_is_repaired_before_checkpoint() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let sequence_oid = engine.create_sequence("s").unwrap();
    let xid = engine.txn_manager().begin_txn();
    let record =
        pg_storage::wal::record::WalRecord::sequence_advance(sequence_oid, 41, xid).unwrap();
    let record_len = record.record_size() as u64;
    let lsn = engine.storage().wal_writer().append(record).unwrap();
    engine
        .storage()
        .wal_writer()
        .flush_to(pg_storage::types::Lsn(lsn.0 + record_len))
        .unwrap();
    std::mem::forget(engine);

    let engine = open(tmp.path());
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 42);
    engine.checkpoint().unwrap();
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 43);
    engine.shutdown();
}

static COMMIT_FAIL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn create_sequence_and_serial_commit_failures_allow_retry() {
    let _serialization = COMMIT_FAIL_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    pg_txn::manager::test_hooks::set_commit_txn_force_fail(true);
    assert!(engine.create_sequence("s").is_err());
    pg_txn::manager::test_hooks::set_commit_txn_force_fail(false);
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    engine.shutdown();

    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    pg_txn::manager::test_hooks::set_commit_txn_force_fail(true);
    assert!(engine
        .exec(None, "CREATE TABLE items (id SERIAL, note TEXT)")
        .is_err());
    pg_txn::manager::test_hooks::set_commit_txn_force_fail(false);
    engine
        .exec(None, "CREATE TABLE items (id SERIAL, note TEXT)")
        .unwrap();
    let mut session = SqlSession::new(1);
    engine
        .exec_in_session(&mut session, None, "INSERT INTO items (note) VALUES ('ok')")
        .unwrap();
    assert_eq!(
        engine.scan("items", None).unwrap()[0].1[0],
        Some(pg_engine::Datum::Int4(1))
    );
    engine.shutdown();
}

#[test]
fn multiple_serial_columns_have_independent_defaults() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut session = SqlSession::new(1);
    engine
        .exec_in_session(&mut session, None, "CREATE TABLE t (a SERIAL, b SERIAL)")
        .unwrap();
    engine
        .exec_in_session(
            &mut session,
            None,
            "INSERT INTO t VALUES (DEFAULT, DEFAULT), (DEFAULT, DEFAULT)",
        )
        .unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows[0].1[0], Some(pg_engine::Datum::Int4(1)));
    assert_eq!(rows[0].1[1], Some(pg_engine::Datum::Int4(1)));
    assert_eq!(rows[1].1[0], Some(pg_engine::Datum::Int4(2)));
    assert_eq!(rows[1].1[1], Some(pg_engine::Datum::Int4(2)));
    assert_eq!(engine.currval(&session, "t_a_seq").unwrap(), 2);
    assert_eq!(engine.currval(&session, "t_b_seq").unwrap(), 2);
    engine.shutdown();
}

#[test]
fn sigkill_after_committed_serial_insert_does_not_reissue() {
    if let Ok(dir) = std::env::var("PG_RUST_SEQUENCE_CHILD_DIR") {
        let engine = open(std::path::Path::new(&dir));
        engine
            .exec(None, "CREATE TABLE items (id SERIAL, note TEXT)")
            .unwrap();
        engine
            .exec(None, "INSERT INTO items (note) VALUES ('committed')")
            .unwrap();
        std::fs::write(std::path::Path::new(&dir).join("ready"), b"1").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(60));
        return;
    }

    let tmp = TempDir::new().unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(exe)
        .args([
            "--exact",
            "sigkill_after_committed_serial_insert_does_not_reissue",
            "--nocapture",
        ])
        .env("PG_RUST_SEQUENCE_CHILD_DIR", tmp.path())
        .spawn()
        .unwrap();
    let ready = tmp.path().join("ready");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !ready.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "child never became ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let status = std::process::Command::new("kill")
        .args(["-9", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let _ = child.wait();
    std::fs::remove_file(tmp.path().join("lock")).unwrap();

    let engine = open(tmp.path());
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "items_id_seq").unwrap(), 2);
    let rows = engine.scan("items", None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Some(pg_engine::Datum::Int4(1)));
    engine.shutdown();
}

#[test]
fn nextval_remains_bounded_after_many_updates() {
    let _serialization = SEQUENCE_HOOK_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    for expected in 1..=1_000 {
        assert_eq!(engine.nextval(&mut session, "s").unwrap(), expected);
    }
    engine.checkpoint().unwrap();
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1_001);
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1_002);
    engine.shutdown();
}

fn delete_internal_row(
    engine: &Engine,
    def: pg_catalog::system_tables::SystemTableDef,
    oid: pg_storage::types::Oid,
) {
    let columns = def.column_types();
    let rows = engine
        .heap()
        .scan(pg_am_heap::access_method::ScanContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            snapshot: &Snapshot::everything(),
            clog: engine.clog().as_ref(),
        })
        .unwrap();
    let (xid, snapshot) = writer_snapshot(engine);
    for (tid, values) in rows {
        if values.first() == Some(&Some(Datum::Int8(oid.0 as i64))) {
            engine
                .heap()
                .delete(DeleteContext {
                    rel: RelationDesc {
                        rel_oid: def.oid.raw(),
                        first_page: def.first_page,
                        columns: &columns,
                    },
                    snapshot: &snapshot,
                    tid,
                    clog: engine.clog().as_ref(),
                })
                .unwrap();
        }
    }
    engine.txn_manager().commit_txn(xid).unwrap();
}

#[test]
fn deleted_and_aborted_internal_rows_are_rebuilt_from_mvcc() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    for oid in [pg_storage::types::Oid(9022), pg_storage::types::Oid(9023)] {
        delete_internal_row(&engine, PG_CLASS, oid);
        delete_internal_row(&engine, PG_RELPAGES, oid);
    }

    let (xid, snapshot) = writer_snapshot(&engine);
    let class_row = vec![
        Some(Datum::Int8(9022)),
        Some(Datum::Text("pg_rust_sequences".to_string())),
        Some(Datum::Text("c".to_string())),
        Some(Datum::Int4(3)),
        Some(Datum::Int8(0)),
        Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
    ];
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
        &PG_CLASS.column_types(),
        &class_row,
    )
    .unwrap();
    engine
        .heap()
        .insert(InsertContext {
            rel: RelationDesc {
                rel_oid: PG_CLASS.oid.raw(),
                first_page: PG_CLASS.first_page,
                columns: &PG_CLASS.column_types(),
            },
            snapshot: &snapshot,
            tuple: &tuple,
            out_tid: None,
        })
        .unwrap();
    engine.txn_manager().abort_txn(xid).unwrap();
    engine.shutdown();

    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    engine.shutdown();
}

struct ResetSequenceHooks;

impl Drop for ResetSequenceHooks {
    fn drop(&mut self) {
        pg_engine::sequence::test_hooks::set_fail_nextval_flush(false);
        pg_engine::sequence::test_hooks::set_fail_compaction(false);
        pg_engine::sequence::test_hooks::set_fail_internal_catalog_write(false);
    }
}

static SEQUENCE_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn nextval_flush_failure_does_not_publish_value() {
    let _serialization = SEQUENCE_HOOK_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let _reset = ResetSequenceHooks;
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    pg_engine::sequence::test_hooks::set_fail_nextval_flush(true);
    assert!(engine.nextval(&mut session, "s").is_err());
    assert!(engine.currval(&session, "s").is_err());
    pg_engine::sequence::test_hooks::set_fail_nextval_flush(false);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 2);
    engine.shutdown();
}

#[test]
fn nextval_flush_hook_is_thread_local() {
    let _reset = ResetSequenceHooks;
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    pg_engine::sequence::test_hooks::set_fail_nextval_flush(true);

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let mut session = SqlSession::new(2);
            engine.nextval(&mut session, "s").unwrap()
        });
        assert_eq!(worker.join().unwrap(), 1);
    });

    let mut session = SqlSession::new(1);
    assert!(engine.nextval(&mut session, "s").is_err());
    assert!(engine.currval(&session, "s").is_err());
    pg_engine::sequence::test_hooks::set_fail_nextval_flush(false);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 2);
    engine.shutdown();
}

#[test]
fn compaction_failure_is_retryable_without_duplicate_values() {
    let _serialization = SEQUENCE_HOOK_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let _reset = ResetSequenceHooks;
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    pg_engine::sequence::test_hooks::set_fail_compaction(true);
    let mut failed_at = None;
    for expected in 2..=200 {
        if engine.nextval(&mut session, "s").is_err() {
            failed_at = Some(expected);
            break;
        }
    }
    let failed_at = failed_at.expect("compaction failure was not injected");
    pg_engine::sequence::test_hooks::set_fail_compaction(false);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), failed_at);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), failed_at + 1);
    engine.shutdown();
}

#[test]
fn internal_catalog_write_failure_retries_on_next_open() {
    let _serialization = SEQUENCE_HOOK_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let _reset = ResetSequenceHooks;
    let tmp = TempDir::new().unwrap();
    pg_engine::sequence::test_hooks::set_fail_internal_catalog_write(true);
    assert!(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).is_err());
    pg_engine::sequence::test_hooks::set_fail_internal_catalog_write(false);
    let engine = open(tmp.path());
    engine.create_sequence("s").unwrap();
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "s").unwrap(), 1);
    engine.shutdown();
}

#[test]
fn very_long_sequence_names_do_not_brick_reopen() {
    let _serialization = SEQUENCE_HOOK_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let _reset = ResetSequenceHooks;
    let tmp = TempDir::new().unwrap();
    let name = "s".repeat(3_000);
    let engine = open(tmp.path());
    engine.create_sequence(&name).unwrap();
    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, &name).unwrap(), 1);
    assert_eq!(engine.nextval(&mut session, &name).unwrap(), 2);
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(engine.nextval(&mut session, &name).unwrap(), 3);
    assert_eq!(engine.nextval(&mut session, &name).unwrap(), 4);
    engine.shutdown();
}

#[test]
fn very_long_serial_sequence_names_also_recover() {
    let _serialization = SEQUENCE_HOOK_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let _reset = ResetSequenceHooks;
    let tmp = TempDir::new().unwrap();
    let table = "t".repeat(3_000);
    let engine = open(tmp.path());
    engine
        .exec(
            None,
            &format!("CREATE TABLE {table} (id SERIAL, note TEXT)"),
        )
        .unwrap();
    let mut session = SqlSession::new(1);
    engine
        .exec_in_session(
            &mut session,
            None,
            &format!("INSERT INTO {table} (note) VALUES ('a'), ('b')"),
        )
        .unwrap();
    engine.shutdown();

    let engine = open(tmp.path());
    engine
        .exec_in_session(
            &mut session,
            None,
            &format!("INSERT INTO {table} (note) VALUES ('c')"),
        )
        .unwrap();
    let rows = engine.scan(&table, None).unwrap();
    assert_eq!(rows.last().unwrap().1[0], Some(Datum::Int4(3)));
    engine.shutdown();
}

#[test]
fn generated_sequence_names_are_unique_under_ddl_lock() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    let mut workers = Vec::new();
    for sql in [
        "CREATE TABLE a_b (c SERIAL)".to_string(),
        "CREATE TABLE a (b_c SERIAL)".to_string(),
    ] {
        let engine = Arc::clone(&engine);
        workers.push(std::thread::spawn(move || engine.exec(None, &sql)));
    }
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);

    let mut session = SqlSession::new(1);
    assert_eq!(engine.nextval(&mut session, "a_b_c_seq").unwrap(), 1);
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(engine.nextval(&mut session, "a_b_c_seq").unwrap(), 2);
    engine.shutdown();
}

#[test]
fn sequence_internal_catalog_names_are_reserved() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    for name in ["pg_rust_sequences", "pg_rust_attrdefaults"] {
        assert!(engine
            .create_table(
                name,
                &[pg_engine::ColumnDef {
                    name: "v".to_string(),
                    col_type: pg_engine::ColumnType::Text,
                }],
            )
            .is_err());
    }
    engine.shutdown();
}
