//! Stage H acceptance tests: catalog bootstrap.
//!
//! Acceptance command: `cargo test -p pg-catalog --test bootstrap`

use pg_am_heap::tuple::{encode_tuple, Datum, TupleHeader};
use pg_am_heap::SlottedPage;
use pg_catalog::bootstrap::BOOTSTRAP_XID;
use pg_catalog::builtin_types::BUILTIN_TYPES;
use pg_catalog::system_tables::{
    PG_AM, PG_AM_OID, PG_ATTRIBUTE, PG_ATTRIBUTE_OID, PG_CLASS, PG_CLASS_OID, PG_INDEX_OID,
    PG_RELPAGES, PG_RELPAGES_OID, PG_TYPE_OID,
};
use pg_catalog::{Catalog, CatalogError, TableOid};
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::page::set_page_pd_lsn;
use pg_storage::types::{Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;

fn open_engine(dir: &std::path::Path) -> StorageEngine {
    let config = StorageConfig::new(dir);
    StorageEngine::open(dir, &config).unwrap()
}

/// Insert one `pg_class` row directly through the buffer pool, bypassing the
/// catalog's read-only API (catalog mutation is Stage I). Returns the OID
/// used. The caller is responsible for durability (flush / checkpoint).
fn insert_pg_class_row(engine: &StorageEngine, oid: Oid, name: &str) {
    let mut guard = engine.buffer_pool().pin_mut(PG_CLASS.first_page).unwrap();
    let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
    let slot = SlottedPage::slot_count(page) as u16;
    let ctid = Tid {
        page_id: PG_CLASS.first_page,
        slot_id: slot,
    };
    let header = TupleHeader::new(BOOTSTRAP_XID, TxnId::INVALID, 0, [0; 16], ctid, 0);
    let row = vec![
        Some(Datum::Int8(oid.0 as i64)),
        Some(Datum::Text(name.to_string())),
        Some(Datum::Text("r".to_string())),
        Some(Datum::Int4(1)),
        Some(Datum::Int8(0)),
        Some(Datum::Int8(2)),
    ];
    let bytes = encode_tuple(header, &PG_CLASS.column_types(), &row).unwrap();
    SlottedPage::add_tuple(page, &bytes).unwrap();

    // Durably log the write so it survives crash recovery. A raw, unlogged page
    // write is not recoverable once a prior checkpoint has armed full-page-image
    // torn-write protection: `pin_mut` emits a pre-image FPI that recovery would
    // replay, rolling the unlogged tuple back. Logging the post-insert page
    // image (replayed by the default FullPageImageRedoHandler) makes it durable.
    let image = page.to_vec();
    let lsn = engine
        .wal_writer()
        .append(WalRecord::full_page_image(PG_CLASS.first_page, image).unwrap())
        .unwrap();
    set_page_pd_lsn(page, lsn);
}

#[test]
fn test_bootstrap_from_empty_dir() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = open_engine(tmp.path());
    let catalog = Catalog::open(&engine).unwrap();

    assert!(catalog.was_bootstrapped());

    // All six system tables are present (five PG-conventional OIDs plus the
    // engine-private pg_rust_relpages directory, Stage K).
    let expected = [
        (PG_CLASS_OID, "pg_class"),
        (PG_ATTRIBUTE_OID, "pg_attribute"),
        (TableOid::new(Oid(1247)), "pg_type"),
        (PG_AM_OID, "pg_am"),
        (PG_INDEX_OID, "pg_index"),
        (PG_RELPAGES_OID, "pg_rust_relpages"),
    ];
    assert_eq!(catalog.relations().len(), 6);
    for (oid, name) in expected {
        let rel = catalog
            .relation(oid)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(rel.name, name);
        assert_eq!(rel.kind, "r");
        assert_eq!(catalog.relation_by_name(name).unwrap().oid, oid);
    }

    // pg_class schemas are self-consistent: relnatts matches the number of
    // pg_attribute rows, and the attribute rows match the hardcoded schema.
    for def in [PG_CLASS, PG_ATTRIBUTE, PG_AM, PG_RELPAGES] {
        let rel = catalog.relation(def.oid).unwrap();
        let attrs = catalog.attributes_of(def.oid);
        assert_eq!(rel.natts as usize, def.columns.len(), "{}", def.name);
        assert_eq!(attrs.len(), def.columns.len(), "{}", def.name);
        for (i, (attr, col)) in attrs.iter().zip(def.columns.iter()).enumerate() {
            assert_eq!(attr.rel, def.oid);
            assert_eq!(attr.name, col.name, "{}.col[{i}]", def.name);
            assert_eq!(attr.type_oid, col.type_oid, "{}.{}", def.name, col.name);
            assert_eq!(attr.len, col.len);
            assert_eq!(attr.num, i as i32 + 1, "attnum is 1-based");
            assert_eq!(attr.not_null, col.not_null);
            assert_eq!(attr.nullable, !col.not_null);
        }
    }

    // pg_type matches builtin_types exactly (OID, name, len).
    assert_eq!(catalog.types().len(), BUILTIN_TYPES.len() + 1); // + the vector row (M6 Stage C)
    for ty in BUILTIN_TYPES {
        let row = catalog.type_by_oid(ty.oid).unwrap();
        assert_eq!(row.name, ty.name);
        assert_eq!(row.len, ty.len);
    }

    // pg_am has exactly heap(2) and btree(403).
    assert_eq!(catalog.access_methods().len(), 3); // heap + btree + hnsw (M6 Stage C)
    assert_eq!(catalog.access_methods()[0].oid, Oid(2));
    assert_eq!(catalog.access_methods()[0].name, "heap");
    assert_eq!(catalog.access_methods()[1].oid, Oid(403));
    assert_eq!(catalog.access_methods()[1].name, "btree");

    // Fresh allocator starts at the first user OID.
    assert_eq!(catalog.oid_allocator().current(), Oid::FIRST_USER);
}

#[test]
fn test_catalog_self_describing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = open_engine(tmp.path());
    let catalog = Catalog::open(&engine).unwrap();

    // pg_class contains a row describing pg_class itself, and its relnatts
    // agrees with both the hardcoded schema and the pg_attribute content.
    let pg_class = catalog.relation(PG_CLASS_OID).unwrap();
    assert_eq!(pg_class.name, "pg_class");
    assert_eq!(pg_class.natts as usize, PG_CLASS.columns.len());
    assert_eq!(
        pg_class.natts as usize,
        catalog.attributes_of(PG_CLASS_OID).len()
    );

    // Every relation's relnatts agrees with its pg_attribute rows.
    for rel in catalog.relations() {
        assert_eq!(
            rel.natts as usize,
            catalog.attributes_of(rel.oid).len(),
            "relnatts mismatch for {}",
            rel.name
        );
    }

    // Type OIDs referenced by pg_attribute all exist in pg_type.
    for rel in catalog.relations() {
        for attr in catalog.attributes_of(rel.oid) {
            assert!(
                catalog.type_by_oid(attr.type_oid).is_some(),
                "{}.{} references unknown type {:?}",
                rel.name,
                attr.name,
                attr.type_oid
            );
        }
    }
}

#[test]
fn test_second_open_does_not_rebootstrap() {
    let tmp = tempfile::TempDir::new().unwrap();

    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        assert!(catalog.was_bootstrapped());
        assert_eq!(catalog.relations().len(), 6);
    }

    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        assert!(!catalog.was_bootstrapped());
        // Row counts did not double.
        assert_eq!(catalog.relations().len(), 6);
        assert_eq!(
            catalog.attributes_of(PG_CLASS_OID).len(),
            PG_CLASS.columns.len()
        );
        assert_eq!(catalog.types().len(), BUILTIN_TYPES.len() + 1); // + the vector row (M6 Stage C)
        assert_eq!(catalog.access_methods().len(), 3); // heap + btree + hnsw (M6 Stage C)
    }
}

#[test]
fn test_next_oid_persists_across_checkpoint() {
    let tmp = tempfile::TempDir::new().unwrap();

    const N: u64 = 10;
    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        let first = catalog.oid_allocator().alloc();
        assert_eq!(first, Oid::FIRST_USER);
        for expected in 1..N {
            assert_eq!(
                catalog.oid_allocator().alloc(),
                Oid(Oid::FIRST_USER.0 + expected)
            );
        }
        engine.trigger_checkpoint().unwrap();
    }

    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        // The allocator resumes past every OID handed out before the crash.
        assert_eq!(
            catalog.oid_allocator().current(),
            Oid(Oid::FIRST_USER.0 + N)
        );
        assert_eq!(catalog.oid_allocator().alloc(), Oid(Oid::FIRST_USER.0 + N));
    }
}

#[test]
fn test_crash_rollback_scans_catalog_oids() {
    let tmp = tempfile::TempDir::new().unwrap();

    let allocated;
    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();

        // Allocate an OID and write a catalog row carrying it.
        allocated = catalog.oid_allocator().alloc();
        assert_eq!(allocated, Oid::FIRST_USER);
        insert_pg_class_row(&engine, allocated, "t_user");

        // Make the catalog page durable WITHOUT a checkpoint: the superblock's
        // next_oid stays at FIRST_USER (the rollback window of the Stage H
        // warning), while the page itself survives.
        engine.buffer_pool().flush(PG_CLASS.first_page).unwrap();
        // Drop without trigger_checkpoint — the simulated crash.
    }

    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        assert!(!catalog.was_bootstrapped());

        // The row written before the "crash" survived.
        assert_eq!(catalog.relations().len(), 7);
        assert!(catalog.relation_by_name("t_user").is_some());

        // Startup correction: the allocator was scanned past the OID already
        // present in the catalog, even though superblock.next_oid rolled back.
        assert!(catalog.oid_allocator().current() > allocated);
        let fresh = catalog.oid_allocator().alloc();
        assert_ne!(fresh, allocated, "OID conflict after crash rollback");
        assert!(catalog.relation(TableOid::new(fresh)).is_none());
    }
}

#[test]
fn test_half_state_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();

    // Simulate a crash mid-bootstrap: pages 1..=6 allocated (PageAlloc WAL is
    // durable) but never initialized with slotted-page content.
    {
        let engine = open_engine(tmp.path());
        for expected in 1..=6u64 {
            let guard = engine.buffer_pool().new_page().unwrap();
            assert_eq!(guard.page_id(), PageId(expected));
        }
        // No content written, no checkpoint — straight to "crash".
    }

    // Reopen: recovery replays the PageAlloc records; bootstrap must detect
    // the uninitialized pages and complete.
    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        assert!(catalog.was_bootstrapped());
        assert_eq!(catalog.relations().len(), 6);
        assert_eq!(
            catalog.relation(PG_CLASS_OID).unwrap().natts as usize,
            PG_CLASS.columns.len()
        );
        assert_eq!(catalog.types().len(), BUILTIN_TYPES.len() + 1); // + the vector row (M6 Stage C)
        assert_eq!(catalog.access_methods().len(), 3); // heap + btree + hnsw (M6 Stage C)
                                                       // Bootstrap reused the pre-allocated pages rather than allocating new
                                                       // ones.
        assert_eq!(engine.page_allocator().lock().next_page_id(), PageId(7));
    }

    // A third open sees a fully valid catalog and does not re-bootstrap.
    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        assert!(!catalog.was_bootstrapped());
        assert_eq!(catalog.relations().len(), 6);
        assert!(catalog.relation(PG_TYPE_OID).is_some());
        assert!(catalog.relation(PG_ATTRIBUTE_OID).is_some());
    }
}

// ---------------------------------------------------------------------------
// Self-healing on corrupted catalog content (M2 has no page checksums; bit
// rot can pass the slotted-page validity check, so read-back is validated
// against the fixed bootstrap content and rewritten on failure).
// ---------------------------------------------------------------------------

/// Corrupt a byte range of page 1 (pg_class) directly in the data file.
fn corrupt_pg_class_page(dir: &std::path::Path, range: std::ops::Range<usize>) {
    let data_file = pg_storage::io::data_file_path(dir);
    let mut contents = std::fs::read(&data_file).unwrap();
    for b in &mut contents[range] {
        *b = 0xAB;
    }
    std::fs::write(&data_file, &contents).unwrap();
}

fn assert_full_system_catalog(catalog: &Catalog) {
    assert_eq!(catalog.relations().len(), 6);
    assert_eq!(
        catalog.attributes_of(PG_CLASS_OID).len(),
        PG_CLASS.columns.len()
    );
    assert_eq!(catalog.types().len(), BUILTIN_TYPES.len() + 1); // + the vector row (M6 Stage C)
    assert_eq!(catalog.access_methods().len(), 3); // heap + btree + hnsw (M6 Stage C)
}

/// Garbage line pointers that happen to decode as `Dead` would silently
/// hide every row (a page-1 full of 0xAB has lp_flags = 3). The old code
/// opened "successfully" with an empty catalog; now the content validation
/// must catch it and self-heal via force_rebootstrap.
#[test]
fn corrupt_lp_array_triggers_rebootstrap() {
    let tmp = tempfile::TempDir::new().unwrap();
    {
        let engine = open_engine(tmp.path());
        assert!(Catalog::open(&engine).unwrap().was_bootstrapped());
    }
    corrupt_pg_class_page(tmp.path(), 32..52); // LP array of page 1

    let engine = open_engine(tmp.path());
    let catalog = Catalog::open(&engine).unwrap();
    assert!(
        catalog.was_bootstrapped(),
        "corruption must trigger a rewrite"
    );
    assert_full_system_catalog(&catalog);
}

/// A torn tuple region fails decode outright (`t_ctid` pad check). The old
/// code hard-failed `Catalog::open`; now it must self-heal.
#[test]
fn corrupt_tuple_payload_triggers_rebootstrap() {
    let tmp = tempfile::TempDir::new().unwrap();
    {
        let engine = open_engine(tmp.path());
        assert!(Catalog::open(&engine).unwrap().was_bootstrapped());
    }
    corrupt_pg_class_page(tmp.path(), 6000..PAGE_SIZE); // tuple region

    let engine = open_engine(tmp.path());
    let catalog = Catalog::open(&engine).unwrap();
    assert!(
        catalog.was_bootstrapped(),
        "corruption must trigger a rewrite"
    );
    assert_full_system_catalog(&catalog);
}

/// Regression test for the partial-flush half-state: a background checkpoint
/// can flush a strict subset of the bootstrap pages mid-bootstrap. If a
/// crash follows, the directory can hold pg_class valid but pg_attribute /
/// pg_type / pg_am uninitialized. A page-1-only validity check would skip
/// bootstrap and load a catalog with relations but no columns.
#[test]
fn test_partial_flush_half_state_triggers_rebootstrap() {
    let tmp = tempfile::TempDir::new().unwrap();

    // Simulate the crashed state: pages 1..=6 allocated; page 1 is a
    // valid-looking slotted page with a pg_class row; pages 2..=6 are zeros.
    {
        let engine = open_engine(tmp.path());
        for expected in 1..=6u64 {
            assert_eq!(
                engine.buffer_pool().new_page().unwrap().page_id(),
                PageId(expected)
            );
        }
        let mut guard = engine.buffer_pool().pin_mut(PG_CLASS.first_page).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        SlottedPage::init(page);
        drop(guard);
        insert_pg_class_row(&engine, Oid(1259), "pg_class");
        // Flush only page 1 (the partial checkpoint); then "crash".
        engine.buffer_pool().flush(PG_CLASS.first_page).unwrap();
    }

    {
        let engine = open_engine(tmp.path());
        let catalog = Catalog::open(&engine).unwrap();
        // The old page-1-only check would have skipped bootstrap here and
        // loaded zero attributes/types.
        assert!(catalog.was_bootstrapped());
        assert_eq!(catalog.relations().len(), 6);
        assert_eq!(
            catalog.attributes_of(PG_CLASS_OID).len(),
            PG_CLASS.columns.len()
        );
        assert_eq!(catalog.types().len(), BUILTIN_TYPES.len() + 1); // + the vector row (M6 Stage C)
        assert_eq!(catalog.access_methods().len(), 3); // heap + btree + hnsw (M6 Stage C)
    }
}

// ---------------------------------------------------------------------------
// pg_rust_relpages directory (Stage K): bootstrap leaves it empty; rows
// written later (here: directly through the buffer pool) are readable via
// the catalog's relpages API.
// ---------------------------------------------------------------------------

/// Insert one `pg_rust_relpages` row directly through the buffer pool,
/// logging a post-image FPI for durability (same discipline as
/// `insert_pg_class_row`).
fn insert_relpages_row(
    engine: &StorageEngine,
    rel_oid: Oid,
    first_page: u64,
    last_page: u64,
    page_count: u64,
) {
    let mut guard = engine
        .buffer_pool()
        .pin_mut(PG_RELPAGES.first_page)
        .unwrap();
    let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
    let slot = SlottedPage::slot_count(page) as u16;
    let ctid = Tid {
        page_id: PG_RELPAGES.first_page,
        slot_id: slot,
    };
    let header = TupleHeader::new(BOOTSTRAP_XID, TxnId::INVALID, 0, [0; 16], ctid, 0);
    let row = vec![
        Some(Datum::Int8(rel_oid.0 as i64)),
        Some(Datum::Int8(first_page as i64)),
        Some(Datum::Int8(last_page as i64)),
        Some(Datum::Int8(page_count as i64)),
    ];
    let bytes = encode_tuple(header, &PG_RELPAGES.column_types(), &row).unwrap();
    SlottedPage::add_tuple(page, &bytes).unwrap();

    let image = page.to_vec();
    let lsn = engine
        .wal_writer()
        .append(WalRecord::full_page_image(PG_RELPAGES.first_page, image).unwrap())
        .unwrap();
    set_page_pd_lsn(page, lsn);
}

#[test]
fn test_relpages_api() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    // Freshly bootstrapped: the directory exists as a relation but holds no rows.
    let catalog = Catalog::open(&engine).unwrap();
    assert!(catalog.relation(PG_RELPAGES_OID).is_some());
    assert_eq!(catalog.relation(PG_RELPAGES_OID).unwrap().natts, 4);
    assert!(catalog.relpages().is_empty());
    assert!(catalog.relpages_of(TableOid::new(Oid(16_384))).is_none());

    // Write two directory rows, then re-read the catalog: the snapshot picks
    // them up (extra rows beyond the system content are tolerated).
    insert_relpages_row(&engine, Oid(16_384), 7, 9, 3);
    insert_relpages_row(&engine, Oid(16_385), 10, 10, 1);

    let catalog = Catalog::open(&engine).unwrap();
    assert_eq!(catalog.relpages().len(), 2);

    let row = catalog.relpages_of(TableOid::new(Oid(16_384))).unwrap();
    assert_eq!(row.first_page, PageId(7));
    assert_eq!(row.last_page, PageId(9));
    assert_eq!(row.page_count, 3);

    let row = catalog.relpages_of(TableOid::new(Oid(16_385))).unwrap();
    assert_eq!(row.first_page, PageId(10));
    assert_eq!(row.last_page, PageId(10));
    assert_eq!(row.page_count, 1);

    assert!(catalog.relpages_of(TableOid::new(Oid(99_999))).is_none());
}

/// Regression for the destructive-self-heal bug (Stage K review P1-2):
/// `force_rebootstrap` rewrites ALL six system pages from fixed bootstrap
/// content, which used to wipe user DDL rows (pg_class/pg_attribute/
/// pg_rust_relpages) and orphan every user table. The self-heal gate now
/// refuses to open when user DDL is present, preserving the data directory.
///
/// The corruption is made un-repairable on purpose: small WAL segments plus
/// a checkpoint after the user insert recycle every WAL record describing
/// page 1, so recovery cannot rebuild it (bootstrap rows are never in the
/// WAL anyway) and the validation genuinely fails.
#[test]
fn corruption_with_user_ddl_refuses_rebootstrap() {
    use pg_am_heap::SlottedPage;

    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = pg_storage::config::StorageConfig::new(tmp.path());
    config.wal_segment_size = 16384; // fits one FPI, still small enough to recycle

    // Bootstrap, add a user table's DDL row, push the WAL past one segment
    // (t_user's FPI lands in segment 0), checkpoint (page 1 durable; segment
    // 0 recycled), shut down cleanly.
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let catalog = Catalog::open(&engine).unwrap();
        assert!(catalog.was_bootstrapped());
        insert_pg_class_row(&engine, Oid(16_384), "t_user");
        for _ in 0..500 {
            drop(engine.buffer_pool().new_page().unwrap());
        }
        engine.trigger_checkpoint().unwrap();
        engine.shutdown();
    }

    // Corrupt the FIRST bootstrap row's tuple bytes on disk (row 0, at slot
    // 0): validation must fail, while the user row (slot 5) stays decodable
    // so the self-heal gate can see it. Corrupting via the LP keeps the
    // scenario precise.
    {
        let data_file = pg_storage::io::data_file_path(tmp.path());
        let mut raw = std::fs::read(&data_file).unwrap();
        let page: &[u8; pg_storage::types::PAGE_SIZE] =
            raw[0..pg_storage::types::PAGE_SIZE].try_into().unwrap();
        let lp = SlottedPage::line_pointer(page, 0).unwrap();
        let off = lp.off() as usize;
        let len = lp.len() as usize;
        for b in &mut raw[off..off + len] {
            *b = 0xAB;
        }
        std::fs::write(&data_file, &raw).unwrap();
    }

    // Open must REFUSE (not wipe): user DDL exists.
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let err = Catalog::open(&engine).unwrap_err();
    assert!(
        matches!(err, CatalogError::Corrupted(ref msg) if msg.contains("user DDL")),
        "expected user-DDL refusal, got {err:?}"
    );
    drop(engine);

    // The refusal must not have run a rebootstrap: the injected corruption
    // is still exactly where we put it (a rewrite would have re-initialized
    // page 1 and restored valid content).
    let raw = std::fs::read(pg_storage::io::data_file_path(tmp.path())).unwrap();
    let page: &[u8; pg_storage::types::PAGE_SIZE] =
        raw[0..pg_storage::types::PAGE_SIZE].try_into().unwrap();
    let lp = SlottedPage::line_pointer(page, 0).unwrap();
    assert!(
        raw[lp.off() as usize..][..4].iter().all(|&b| b == 0xAB),
        "page 1 must be untouched (no rebootstrap)"
    );
}

/// Companion to `corruption_with_user_ddl_refuses_rebootstrap`: header-level
/// damage (`system_pages_are_valid` fails) reaches `bootstrap_if_needed`
/// BEFORE `Catalog::open`'s validation gate, so it needs the same user-DDL
/// refusal (Stage K review P1-2 residual path).
#[test]
fn header_corruption_with_user_ddl_refuses_rebootstrap() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = pg_storage::config::StorageConfig::new(tmp.path());
    config.wal_segment_size = 16384; // fits one FPI, still small enough to recycle

    // Bootstrap, add a user table's DDL row, push the WAL past one segment
    // (t_user's FPI lands in segment 0), checkpoint (page durable; segment 0
    // recycled so recovery cannot repair the corruption below), shut down.
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        assert!(Catalog::open(&engine).unwrap().was_bootstrapped());
        insert_pg_class_row(&engine, Oid(16_384), "t_user");
        for _ in 0..500 {
            drop(engine.buffer_pool().new_page().unwrap());
        }
        engine.trigger_checkpoint().unwrap();
        engine.shutdown();
    }

    // Corrupt only `pd_pagesize_version` (offset 20..22): the page stops
    // being a "valid slotted page" for the half-state check, but its rows
    // stay readable — so the user-DDL gate can still see t_user.
    {
        let data_file = pg_storage::io::data_file_path(tmp.path());
        let mut raw = std::fs::read(&data_file).unwrap();
        raw[20..22].copy_from_slice(&0xFFFFu16.to_le_bytes());
        std::fs::write(&data_file, &raw).unwrap();
    }

    // Open must REFUSE at the bootstrap_if_needed level (not wipe).
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let err = Catalog::open(&engine).unwrap_err();
    assert!(
        matches!(err, CatalogError::Corrupted(ref msg) if msg.contains("user DDL")),
        "expected user-DDL refusal from bootstrap_if_needed, got {err:?}"
    );
    drop(engine);

    // No rebootstrap ran: the corrupted version field is still there.
    let raw = std::fs::read(pg_storage::io::data_file_path(tmp.path())).unwrap();
    assert_eq!(
        &raw[20..22],
        &0xFFFFu16.to_le_bytes(),
        "page 1 header must be untouched (no rebootstrap)"
    );
}

/// Same header corruption on a bootstrap-only catalog must still heal
/// (the gate only refuses when user DDL is present).
#[test]
fn header_corruption_without_user_ddl_still_rebootstraps() {
    let tmp = tempfile::TempDir::new().unwrap();
    let tmp = tmp.path();
    {
        let engine = open_engine(tmp);
        assert!(Catalog::open(&engine).unwrap().was_bootstrapped());
        engine.shutdown();
    }

    {
        let data_file = pg_storage::io::data_file_path(tmp);
        let mut raw = std::fs::read(&data_file).unwrap();
        raw[20..22].copy_from_slice(&0xFFFFu16.to_le_bytes());
        std::fs::write(&data_file, &raw).unwrap();
    }

    let engine = open_engine(tmp);
    let catalog = Catalog::open(&engine).unwrap();
    assert!(
        catalog.was_bootstrapped(),
        "bootstrap-only catalog must heal"
    );
    assert_eq!(catalog.relations().len(), 6);
    assert_eq!(catalog.types().len(), BUILTIN_TYPES.len() + 1); // + the vector row (M6 Stage C)
}
