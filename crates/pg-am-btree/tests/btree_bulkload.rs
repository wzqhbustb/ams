//! Stage M wave 2: bottom-up bulk load (`BTreeAM::build_index`) — packing,
//! lookups, range scans, structural validation, and crash recovery of a
//! bulk-loaded index.

use std::sync::Arc;

use pg_am_btree::key::{decode_i32, encode_i32};
use pg_am_btree::{btree_redo_handlers, BTreeAM, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_390);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(42_000 + i / 60_000),
        slot_id: (i % 60_000) as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    encode_i32(i).to_vec()
}

fn am(engine: &StorageEngine) -> BTreeAM {
    BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    )
}

/// Build an index over `n` keys, handed to the loader in *reverse* order to
/// prove the loader sorts.
fn build_index(engine: &StorageEngine, n: i32) -> BTreeIndex {
    let entries: Vec<(Vec<u8>, Tid)> = (0..n).rev().map(|i| (key(i), tid(i as u64))).collect();
    am(engine)
        .build_index(REL_OID, ColumnType::Int4, entries)
        .unwrap()
}

fn assert_all_present(index: &BTreeIndex, n: i32) {
    for i in 0..n {
        assert_eq!(
            index.lookup(&key(i)).unwrap(),
            Some(tid(i as u64)),
            "key {i}"
        );
    }
    assert_eq!(index.lookup(&key(-1)).unwrap(), None);
    assert_eq!(index.lookup(&key(n)).unwrap(), None);
}

#[test]
fn bulk_load_multi_level_validate_and_scan() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // ~452 leaf entries / ~509 downlinks per page: 300k entries overflow a
    // single internal root, producing a 3-level tree (level 2).
    let n = 300_000i32;
    let index = build_index(&engine, n);
    assert!(
        index.tree_level() >= 2,
        "300k entries must produce a multi-level tree, got level {}",
        index.tree_level()
    );
    index.validate().unwrap();
    assert_all_present(&index, n);

    // Range scan boundaries on the packed tree.
    let rows = index.range_scan(Some(&key(500)), Some(&key(510))).unwrap();
    let got: Vec<i32> = rows
        .iter()
        .map(|(k, _)| decode_i32(k.clone().try_into().unwrap()))
        .collect();
    assert_eq!(got, (500..510).collect::<Vec<_>>());
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(rows.len(), n as usize);
}

#[test]
fn bulk_load_empty_and_single_page() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // Empty entry set: one empty root leaf, like BTreeIndex::create.
    let index = am(&engine)
        .build_index(REL_OID, ColumnType::Int4, Vec::new())
        .unwrap();
    assert_eq!(index.tree_level(), 0);
    index.validate().unwrap();
    assert!(index.range_scan(None, None).unwrap().is_empty());

    // A few entries: single-page root leaf. And the tree is writable: an
    // insert landing on the packed page works, an overflowing insert splits.
    let index = build_index(&engine, 50);
    assert_eq!(index.tree_level(), 0);
    assert_all_present(&index, 50);
}

#[test]
fn bulk_load_is_writable_afterwards() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let n = 20_000i32;
    let mut index = build_index(&engine, n);
    // Packed-full pages split on the first inserts that overflow them.
    for i in n..n + 5_000 {
        index.insert(&key(i), tid(i as u64)).unwrap();
    }
    index.validate().unwrap();
    assert_all_present(&index, n + 5_000);
}

#[test]
fn bulk_load_survives_crash() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let n = 30_000i32;
    let meta_page;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let index = build_index(&engine, n);
        meta_page = index.meta_page();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: no checkpoint, no shutdown
    }
    let engine = StorageEngine::open_with_redo_handlers(
        tmp.path(),
        &config,
        btree_redo_handlers(),
        Vec::new(),
    )
    .unwrap();
    let index = am(&engine)
        .open_index(REL_OID, meta_page, ColumnType::Int4)
        .unwrap();
    assert!(index.tree_level() >= 1);
    index.validate().unwrap();
    assert_all_present(&index, n);
}
