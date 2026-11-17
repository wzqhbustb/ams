//! Phase 4a Stage D: TOAST chunk I/O, MVCC, and reclamation.

use std::path::Path;

use pg_am_heap::access_method::{DeleteContext, InsertContext, RelationDesc, UpdateContext};
use pg_am_heap::toast::ToastRelation;
use pg_am_heap::tuple::{decode_tuple, encode_tuple, TupleHeader};
use pg_am_heap::{AccessMethod, HeapError, SlottedPage, ToastPointer, UpdatableAM};
use pg_catalog::system_tables::{
    SystemTableDef, HEAP_AM_OID, PG_CLASS, PG_RELPAGES, RELKIND_TABLE,
};
use pg_engine::{
    ColumnDef, ColumnType, Datum, Engine, EngineConfig, EngineError, QueryResult, Value,
};
use pg_storage::types::{Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_txn::Snapshot;
use tempfile::TempDir;

fn open(dir: &Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

struct CommitFailGuard;

impl Drop for CommitFailGuard {
    fn drop(&mut self) {
        pg_txn::manager::test_hooks::set_commit_txn_force_fail(false);
    }
}

fn arm_commit_failure() -> CommitFailGuard {
    pg_txn::manager::test_hooks::set_commit_txn_force_fail(true);
    CommitFailGuard
}

fn column(name: &str, col_type: ColumnType) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        col_type,
    }
}

fn rows(result: QueryResult) -> Vec<Vec<Value>> {
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}")
    };
    rows
}

fn scan(engine: &Engine, table: &str) -> Vec<Vec<Value>> {
    engine
        .scan(table, None)
        .unwrap()
        .into_iter()
        .map(|(_tid, values)| values)
        .collect()
}

const CHUNK_COLUMNS: [ColumnType; 3] = [ColumnType::Int8, ColumnType::Int4, ColumnType::Bytea];

fn main_relation<'a>(
    entry: &'a pg_engine::TableEntry,
    columns: &'a [ColumnType],
) -> RelationDesc<'a> {
    RelationDesc {
        rel_oid: entry.oid,
        first_page: entry.first_page,
        columns,
    }
}

fn toast_relation(entry: &pg_engine::TableEntry) -> RelationDesc<'static> {
    let toast = entry.toast.unwrap();
    RelationDesc {
        rel_oid: toast.rel_oid,
        first_page: toast.first_page,
        columns: &CHUNK_COLUMNS,
    }
}

fn writer_snapshot(engine: &Engine) -> (TxnId, Snapshot) {
    let xid = engine.txn_manager().begin_txn();
    let mut snapshot = Snapshot::everything();
    snapshot.set_current_xid(xid);
    (xid, snapshot)
}

fn raw_external_pointer(engine: &Engine, entry: &pg_engine::TableEntry, tid: Tid) -> ToastPointer {
    let columns: Vec<ColumnType> = entry.columns.iter().map(|column| column.col_type).collect();
    let guard = engine.storage().buffer_pool().pin(tid.page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    let bytes = SlottedPage::tuple(page, tid.slot_id).unwrap().unwrap();
    let (_header, values) = decode_tuple(bytes, &columns).unwrap();
    match &values[1] {
        Some(Datum::External(pointer)) => *pointer,
        other => panic!("expected external pointer, got {other:?}"),
    }
}

fn insert_chunk(
    engine: &Engine,
    entry: &pg_engine::TableEntry,
    snapshot: &Snapshot,
    valueid: u32,
    chunk_seq: u32,
    payload: Vec<u8>,
) {
    let row = vec![
        Some(Datum::Int8(i64::from(valueid))),
        Some(Datum::Int4(chunk_seq as i32)),
        Some(Datum::Bytea(payload)),
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
        &CHUNK_COLUMNS,
        &row,
    )
    .unwrap();
    engine
        .heap()
        .insert(InsertContext {
            rel: toast_relation(entry),
            snapshot,
            tuple: &tuple,
            out_tid: None,
        })
        .unwrap();
}

fn chunk_tid(engine: &Engine, entry: &pg_engine::TableEntry, valueid: u32, wanted_seq: u32) -> Tid {
    for page_id in engine
        .heap()
        .relation_pages(&toast_relation(entry))
        .unwrap()
    {
        let guard = engine.storage().buffer_pool().pin(page_id).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
                continue;
            };
            let (_header, values) = decode_tuple(bytes, &CHUNK_COLUMNS).unwrap();
            if values[0] == Some(Datum::Int8(i64::from(valueid)))
                && values[1] == Some(Datum::Int4(wanted_seq as i32))
            {
                return Tid {
                    page_id,
                    slot_id: slot,
                };
            }
        }
    }
    panic!("chunk ({valueid}, {wanted_seq}) not found")
}

fn catalog_row_tid(engine: &Engine, def: &SystemTableDef, oid: u64) -> Tid {
    let guard = engine.storage().buffer_pool().pin(def.first_page).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    let columns = def.column_types();
    for slot in 0..SlottedPage::slot_count(page) as u16 {
        let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
            continue;
        };
        let (_header, values) = decode_tuple(bytes, &columns).unwrap();
        if matches!(values.first(), Some(Some(Datum::Int8(value))) if *value == oid as i64) {
            return Tid {
                page_id: def.first_page,
                slot_id: slot,
            };
        }
    }
    panic!("catalog row for OID {oid} not found")
}

fn update_catalog_row(
    engine: &Engine,
    snapshot: &Snapshot,
    def: &SystemTableDef,
    old_tid: Tid,
    row: &[Value],
) {
    let columns = def.column_types();
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
        &columns,
        row,
    )
    .unwrap();
    engine
        .heap()
        .update(UpdateContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            snapshot,
            old_tid,
            new_tuple: &tuple,
            out_tid: None,
            clog: engine.clog().as_ref(),
            hot_eligible: false,
        })
        .unwrap();
}

fn force_pre_toast_catalog(engine: &Engine) {
    let entry = engine.describe_table("t").unwrap();
    let (xid, snapshot) = writer_snapshot(engine);
    let class_tid = catalog_row_tid(engine, &PG_CLASS, entry.oid.0);
    update_catalog_row(
        engine,
        &snapshot,
        &PG_CLASS,
        class_tid,
        &[
            Some(Datum::Int8(entry.oid.0 as i64)),
            Some(Datum::Text("t".to_string())),
            Some(Datum::Text(RELKIND_TABLE.to_string())),
            Some(Datum::Int4(entry.columns.len() as i32)),
            Some(Datum::Int8(0)),
            Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
        ],
    );
    let relpages_tid = catalog_row_tid(engine, &PG_RELPAGES, entry.oid.0);
    update_catalog_row(
        engine,
        &snapshot,
        &PG_RELPAGES,
        relpages_tid,
        &[
            Some(Datum::Int8(entry.oid.0 as i64)),
            Some(Datum::Int8(entry.first_page.0 as i64)),
            Some(Datum::Int8(entry.first_page.0 as i64)),
            Some(Datum::Int8(1)),
        ],
    );
    engine.txn_manager().commit_txn(xid).unwrap();
}

fn crash_uncommitted_toast_row(dir: &Path, with_main_row: bool) {
    let engine = open(dir);
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let columns = [ColumnType::Int8, ColumnType::Text];
    let (xid, snapshot) = writer_snapshot(&engine);
    let external = engine
        .heap()
        .externalize_value(
            &main_relation(&entry, &columns),
            &snapshot,
            ColumnType::Text,
            &Datum::Text("x".repeat(4_500)),
        )
        .unwrap();
    if with_main_row {
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
            &columns,
            &[Some(Datum::Int8(1)), Some(external)],
        )
        .unwrap();
        engine
            .heap()
            .insert(InsertContext {
                rel: main_relation(&entry, &columns),
                snapshot: &snapshot,
                tuple: &tuple,
                out_tid: None,
            })
            .unwrap();
    }
    engine.storage().wal_writer().flush().unwrap();
    // Keep the XID unresolved and abandon the engine without commit/abort.
    let _ = xid;
    std::mem::forget(engine);
}

#[test]
fn large_text_roundtrip_reopen_and_oid_progress() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let first_toast_oid = entry.toast.unwrap().rel_oid;
    let large = "z".repeat(9_000);
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text(large.clone()))],
        )
        .unwrap();
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 5);
    assert_eq!(scan(&engine, "t")[0][1], Some(Datum::Text(large.clone())));
    drop(engine);

    let engine = open(tmp.path());
    assert_eq!(scan(&engine, "t")[0][1], Some(Datum::Text(large.clone())));
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(2)), Some(Datum::Text(large.clone()))],
        )
        .unwrap();
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 10);
    assert_eq!(scan(&engine, "t").len(), 2);

    engine
        .create_table("other", &[column("v", ColumnType::Text)])
        .unwrap();
    let other_toast_oid = engine
        .describe_table("other")
        .unwrap()
        .toast
        .unwrap()
        .rel_oid;
    assert_ne!(other_toast_oid, first_toast_oid);
}

#[test]
fn multi_attribute_and_vector_overflow_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let text_columns: Vec<ColumnDef> = std::iter::once(column("id", ColumnType::Int8))
        .chain((0..5).map(|i| column(&format!("v{i}"), ColumnType::Text)))
        .collect();
    engine.create_table("multi", &text_columns).unwrap();
    let values: Vec<Value> = std::iter::once(Some(Datum::Int8(1)))
        .chain((0..5).map(|i| Some(Datum::Text(char::from(b'a' + i).to_string().repeat(1_800)))))
        .collect();
    engine.insert("multi", &values).unwrap();
    let entry = engine.describe_table("multi").unwrap();
    // Largest-first externalization stops as soon as the other four fit.
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 1);
    assert_eq!(scan(&engine, "multi")[0], values);

    engine
        .create_table(
            "vectors",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Vector(2_000)),
                column("note", ColumnType::Text),
            ],
        )
        .unwrap();
    let vector: Vec<f32> = (0..2_000).map(|i| i as f32).collect();
    let vector_values = vec![
        Some(Datum::Int8(1)),
        Some(Datum::Vector(vector)),
        Some(Datum::Text("n".repeat(1_800))),
    ];
    engine.insert("vectors", &vector_values).unwrap();
    let vector_entry = engine.describe_table("vectors").unwrap();
    assert_eq!(
        engine.heap().toast_chunk_count(vector_entry.oid).unwrap(),
        4
    );
    assert_eq!(scan(&engine, "vectors")[0], vector_values);
}

#[test]
fn update_shares_unchanged_group_and_old_snapshot_pins_old_group() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("a", ColumnType::Text),
                column("b", ColumnType::Text),
            ],
        )
        .unwrap();
    let old_a = "a".repeat(4_500);
    let shared_b = "b".repeat(4_500);
    engine
        .insert(
            "t",
            &[
                Some(Datum::Int8(1)),
                Some(Datum::Text(old_a.clone())),
                Some(Datum::Text(shared_b.clone())),
            ],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 6);

    let reader = engine.begin_txn().unwrap();
    let new_a = "c".repeat(4_500);
    engine
        .update(
            "t",
            engine.scan("t", None).unwrap()[0].0,
            &[
                Some(Datum::Int8(1)),
                Some(Datum::Text(new_a.clone())),
                Some(Datum::Text(shared_b.clone())),
            ],
        )
        .unwrap();
    // Only the modified attribute received a fresh three-chunk group.
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 9);
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.toast_chunks_reclaimed, 0);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 9);

    let old_rows = rows(engine.exec(Some(&reader), "SELECT * FROM t").unwrap());
    assert_eq!(old_rows[0][1], Some(Datum::Text(old_a)));
    assert_eq!(old_rows[0][2], Some(Datum::Text(shared_b.clone())));
    reader.abort().unwrap();

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.toast_chunks_reclaimed, 3);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 6);
    let current = scan(&engine, "t");
    assert_eq!(current[0][1], Some(Datum::Text(new_a)));
    assert_eq!(current[0][2], Some(Datum::Text(shared_b)));
    assert_eq!(engine.vacuum("t").unwrap().toast_chunks_reclaimed, 0);
}

#[test]
fn aborted_insert_chunks_are_reclaimed() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(
            Some(&txn),
            &format!("INSERT INTO t VALUES (1, '{}')", "x".repeat(4_500)),
        )
        .unwrap();
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 3);
    txn.abort().unwrap();
    assert!(scan(&engine, "t").is_empty());

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.toast_chunks_reclaimed, 3);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 0);
    assert_eq!(engine.vacuum("t").unwrap().toast_chunks_reclaimed, 0);
}

#[test]
fn duplicate_visible_chunk_is_corrupted() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text("x".repeat(4_500)))],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let pointer = raw_external_pointer(&engine, &entry, engine.scan("t", None).unwrap()[0].0);
    let (xid, snapshot) = writer_snapshot(&engine);
    insert_chunk(
        &engine,
        &entry,
        &snapshot,
        pointer.va_valueid,
        0,
        vec![b'x'; 2_000],
    );
    engine.txn_manager().commit_txn(xid).unwrap();

    let error = engine.scan("t", None).unwrap_err();
    assert!(
        matches!(error, EngineError::Heap(HeapError::Corrupted(_))),
        "unexpected error: {error:?}"
    );
}

#[test]
fn missing_chunk_is_corrupted() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text("x".repeat(4_500)))],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let pointer = raw_external_pointer(&engine, &entry, engine.scan("t", None).unwrap()[0].0);
    let tid = chunk_tid(&engine, &entry, pointer.va_valueid, 1);
    let (xid, snapshot) = writer_snapshot(&engine);
    engine
        .heap()
        .delete(DeleteContext {
            rel: toast_relation(&entry),
            snapshot: &snapshot,
            tid,
            clog: engine.clog().as_ref(),
        })
        .unwrap();
    engine.txn_manager().commit_txn(xid).unwrap();

    let error = engine.scan("t", None).unwrap_err();
    assert!(
        matches!(error, EngineError::Heap(HeapError::Corrupted(_))),
        "unexpected error: {error:?}"
    );
}

#[test]
fn crash_after_chunk_wal_without_main_wal_is_reclaimed() {
    let tmp = TempDir::new().unwrap();
    crash_uncommitted_toast_row(tmp.path(), false);
    let engine = open(tmp.path());
    let entry = engine.describe_table("t").unwrap();
    assert!(scan(&engine, "t").is_empty());
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 3);
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.toast_chunks_reclaimed, 3);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 0);
}

#[test]
fn crash_after_main_wal_without_commit_is_reclaimed() {
    let tmp = TempDir::new().unwrap();
    crash_uncommitted_toast_row(tmp.path(), true);
    let engine = open(tmp.path());
    let entry = engine.describe_table("t").unwrap();
    assert!(scan(&engine, "t").is_empty());
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 3);
    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.toast_chunks_reclaimed, 3);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 0);
    assert_eq!(engine.vacuum("t").unwrap().toast_chunks_reclaimed, 0);
}

#[test]
fn pre_toast_catalog_is_migrated_on_open_and_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text("small".to_string()))],
        )
        .unwrap();
    force_pre_toast_catalog(&engine);
    drop(engine);

    let engine = open(tmp.path());
    let entry = engine.describe_table("t").unwrap();
    assert!(entry.toast.is_some());
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(2)), Some(Datum::Text("x".repeat(4_500)))],
        )
        .unwrap();
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 3);
    let rows = scan(&engine, "t");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][1], Some(Datum::Text("x".repeat(4_500))));
    drop(engine);

    let engine = open(tmp.path());
    assert_eq!(scan(&engine, "t").len(), 2);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 3);
}

#[test]
fn deleted_group_waits_for_old_snapshot_before_reclaim() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    let large = "d".repeat(4_500);
    let tid = engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text(large.clone()))],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let reader = engine.begin_txn().unwrap();
    engine.delete("t", tid).unwrap();
    assert!(scan(&engine, "t").is_empty());
    assert_eq!(engine.vacuum("t").unwrap().toast_chunks_reclaimed, 0);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 3);

    let old_rows = rows(engine.exec(Some(&reader), "SELECT * FROM t").unwrap());
    assert_eq!(old_rows[0][1], Some(Datum::Text(large)));
    reader.abort().unwrap();
    assert_eq!(engine.vacuum("t").unwrap().toast_chunks_reclaimed, 3);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 0);
    assert_eq!(engine.vacuum("t").unwrap().toast_chunks_reclaimed, 0);
}

#[test]
fn aborted_update_reclaims_only_the_new_group() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    let old = "o".repeat(4_500);
    engine
        .insert("t", &[Some(Datum::Int8(1)), Some(Datum::Text(old.clone()))])
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(
            Some(&txn),
            &format!("UPDATE t SET v = '{}' WHERE id = 1", "n".repeat(4_500)),
        )
        .unwrap();
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 6);
    txn.abort().unwrap();
    assert_eq!(scan(&engine, "t")[0][1], Some(Datum::Text(old)));

    let stats = engine.vacuum("t").unwrap();
    assert_eq!(stats.toast_chunks_reclaimed, 3);
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 3);
    assert_eq!(engine.vacuum("t").unwrap().toast_chunks_reclaimed, 0);
}

#[test]
fn bytea_multi_chunk_roundtrip_and_reopen() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Bytea),
            ],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let payload = vec![0xab; 9_000];
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Bytea(payload.clone()))],
        )
        .unwrap();
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 5);
    assert_eq!(
        scan(&engine, "t")[0][1],
        Some(Datum::Bytea(payload.clone()))
    );
    drop(engine);

    let engine = open(tmp.path());
    assert_eq!(scan(&engine, "t")[0][1], Some(Datum::Bytea(payload)));
}

#[test]
fn inline_vector_header_bits_survive_external_text() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Vector(1)),
                column("note", ColumnType::Text),
            ],
        )
        .unwrap();
    let values = vec![
        Some(Datum::Int8(1)),
        Some(Datum::Vector(vec![f32::from_bits(0x4000_0014)])),
        Some(Datum::Text("x".repeat(4_500))),
    ];
    engine.insert("t", &values).unwrap();
    assert_eq!(scan(&engine, "t")[0], values);
}

#[test]
fn all_vector_columns_are_externalized_together_and_reopen() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("large", ColumnType::Vector(2_000)),
                column("small", ColumnType::Vector(1)),
            ],
        )
        .unwrap();
    let values = vec![
        Some(Datum::Int8(1)),
        Some(Datum::Vector(vec![1.0; 2_000])),
        Some(Datum::Vector(vec![f32::from_bits(0x4000_0014)])),
    ];
    let tid = engine.insert("t", &values).unwrap();
    let entry = engine.describe_table("t").unwrap();
    // 8000 bytes in four chunks plus the small four-byte vector's own chunk.
    assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 5);
    assert_eq!(scan(&engine, "t")[0], values);

    let guard = engine.storage().buffer_pool().pin(tid.page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    let bytes = SlottedPage::tuple(page, tid.slot_id).unwrap().unwrap();
    let (header, _raw_values) = decode_tuple(
        bytes,
        &[
            ColumnType::Int8,
            ColumnType::Vector(2_000),
            ColumnType::Vector(1),
        ],
    )
    .unwrap();
    assert_ne!(
        header.t_infomask2 & pg_am_heap::tuple::HEAP_VECTOR_EXTERNAL,
        0
    );
    drop(guard);
    drop(engine);

    let engine = open(tmp.path());
    assert_eq!(scan(&engine, "t")[0], values);
}

#[test]
fn toast_registration_failure_aborts_table_and_allows_retry() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let main_oid = Oid::FIRST_USER;
    let fake_toast = ToastRelation {
        rel_oid: Oid(99_000),
        first_page: engine.heap().create_heap(Oid(99_000)).unwrap(),
    };
    engine
        .heap()
        .register_toast_relation(main_oid, fake_toast)
        .unwrap();

    let error = engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        EngineError::Heap(HeapError::InvalidArgument(_))
    ));
    assert!(engine.describe_table("t").is_none());
    drop(engine);

    let engine = open(tmp.path());
    assert!(engine.describe_table("t").is_none());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text("x".repeat(4_500)))],
        )
        .unwrap();
    assert_eq!(
        scan(&engine, "t")[0][1],
        Some(Datum::Text("x".repeat(4_500)))
    );
}

#[test]
fn zero_length_values_still_require_their_empty_chunk() {
    for (column_type, datum) in [
        (ColumnType::Text, Datum::Text(String::new())),
        (ColumnType::Bytea, Datum::Bytea(Vec::new())),
    ] {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .create_table(
                "t",
                &[column("id", ColumnType::Int8), column("v", column_type)],
            )
            .unwrap();
        let entry = engine.describe_table("t").unwrap();
        let columns = [ColumnType::Int8, column_type];
        let (xid, snapshot) = writer_snapshot(&engine);
        let external = engine
            .heap()
            .externalize_value(
                &main_relation(&entry, &columns),
                &snapshot,
                column_type,
                &datum,
            )
            .unwrap();
        let mut tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
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
            &columns,
            &[Some(Datum::Int8(1)), Some(external)],
        )
        .unwrap();
        engine
            .heap()
            .insert(InsertContext {
                rel: main_relation(&entry, &columns),
                snapshot: &snapshot,
                tuple: &tuple,
                out_tid: Some(&mut tid),
            })
            .unwrap();
        engine.txn_manager().commit_txn(xid).unwrap();
        assert_eq!(engine.heap().toast_chunk_count(entry.oid).unwrap(), 1);
        assert_eq!(scan(&engine, "t")[0][1], Some(datum));

        let pointer = raw_external_pointer(&engine, &entry, tid);
        let chunk_tid = chunk_tid(&engine, &entry, pointer.va_valueid, 0);
        let (delete_xid, delete_snapshot) = writer_snapshot(&engine);
        engine
            .heap()
            .delete(DeleteContext {
                rel: toast_relation(&entry),
                snapshot: &delete_snapshot,
                tid: chunk_tid,
                clog: engine.clog().as_ref(),
            })
            .unwrap();
        engine.txn_manager().commit_txn(delete_xid).unwrap();

        let error = engine.scan("t", None).unwrap_err();
        assert!(
            matches!(error, EngineError::Heap(HeapError::Corrupted(_))),
            "unexpected error for {column_type:?}: {error:?}"
        );
    }
}

#[test]
fn create_table_commit_failure_leaves_no_table_and_allows_retry() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let _guard = arm_commit_failure();
    let error = engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap_err();
    assert!(matches!(error, EngineError::Storage(_)));
    drop(_guard);
    assert!(engine.describe_table("t").is_none());
    drop(engine);

    let engine = open(tmp.path());
    assert!(engine.describe_table("t").is_none());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text("x".repeat(4_500)))],
        )
        .unwrap();
    assert_eq!(
        scan(&engine, "t")[0][1],
        Some(Datum::Text("x".repeat(4_500)))
    );
}
