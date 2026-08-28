//! Stage I concurrency: many threads inserting into one heap produce unique
//! TIDs (no slot collisions) and every row is scannable afterwards.

use std::sync::{Arc, Mutex};
use std::thread;

use pg_am_heap::access_method::{AccessMethod, InsertContext, RelationDesc, ScanContext};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::HeapAM;

use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};

use pg_txn::Snapshot;

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);
const THREADS: i32 = 100;
const PER_THREAD: i32 = 5;

fn encode_row(id: i32) -> Vec<u8> {
    let header = TupleHeader::new(
        TxnId(100),
        TxnId::INVALID,
        0,
        [0u8; 16],
        Tid {
            page_id: PageId(0),
            slot_id: 0,
        },
        0,
    );
    encode_tuple(
        header,
        &COLUMNS,
        &[
            Some(Datum::Int4(id)),
            Some(Datum::Text(format!("row-{id}"))),
        ],
    )
    .unwrap()
}

#[test]
fn concurrent_insert_unique_tids() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = Arc::new(HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    ));
    let first_page = heap.create_heap(REL_OID).unwrap();

    let tids: Arc<Mutex<Vec<Tid>>> = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let heap = Arc::clone(&heap);
        let tids = Arc::clone(&tids);
        handles.push(thread::spawn(move || {
            let mut snap = Snapshot::everything();
            snap.set_current_xid(TxnId(100));
            for i in 0..PER_THREAD {
                let id = t * PER_THREAD + i;
                let tuple = encode_row(id);
                let mut tid = Tid {
                    page_id: PageId(0),
                    slot_id: 0,
                };
                heap.insert(InsertContext {
                    // create_heap already seeded the chain-head page; the AM
                    // tracks/extends the chain from here.
                    rel: RelationDesc {
                        rel_oid: REL_OID,
                        first_page,
                        columns: &COLUMNS,
                    },
                    snapshot: &snap,
                    tuple: &tuple,
                    out_tid: Some(&mut tid),
                })
                .unwrap();
                tids.lock().unwrap().push(tid);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let total = (THREADS * PER_THREAD) as usize;
    let mut all = tids.lock().unwrap().clone();
    assert_eq!(all.len(), total);
    all.sort_unstable_by_key(|t| (t.page_id.0, t.slot_id));
    all.dedup();
    assert_eq!(all.len(), total, "duplicate TIDs: slot collision detected");

    // Every inserted row must be scannable. The inserts spilled onto new
    // pages, but the AM tracks the extended chain internally, so the scan
    // sees all of them from the chain head alone.
    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: RelationDesc {
                rel_oid: REL_OID,
                first_page,
                columns: &COLUMNS,
            },
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), total, "scan did not see every inserted row");
}
