//! Sequence catalog definitions, shared state, and WAL recovery.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use pg_am_heap::tuple::ColumnType;
use pg_catalog::builtin_types::{INT4_OID, INT8_OID, TEXT_OID};
use pg_catalog::system_tables::{SysColumn, SystemTableDef, HEAP_AM_OID};
use pg_catalog::{TableOid, TypeOid};
use pg_storage::error::Result as StorageResult;
use pg_storage::recovery::{RedoContext, RedoHandler};
use pg_storage::types::Oid;
use pg_storage::wal::record::{SequenceAdvanceRecord, WalRecord, WalRecordType};

#[doc(hidden)]
pub mod test_hooks {
    use std::cell::Cell;

    thread_local! {
        static FAIL_NEXTVAL_FLUSH: Cell<bool> = const { Cell::new(false) };
        static FAIL_COMPACTION: Cell<bool> = const { Cell::new(false) };
        static FAIL_INTERNAL_CATALOG_WRITE: Cell<bool> = const { Cell::new(false) };
    }

    pub fn set_fail_nextval_flush(on: bool) {
        FAIL_NEXTVAL_FLUSH.with(|flag| flag.set(on));
    }

    pub fn set_fail_compaction(on: bool) {
        FAIL_COMPACTION.with(|flag| flag.set(on));
    }

    pub fn set_fail_internal_catalog_write(on: bool) {
        FAIL_INTERNAL_CATALOG_WRITE.with(|flag| flag.set(on));
    }

    pub(crate) fn fail_nextval_flush() -> bool {
        FAIL_NEXTVAL_FLUSH.with(Cell::get)
    }

    pub(crate) fn fail_compaction() -> bool {
        FAIL_COMPACTION.with(Cell::get)
    }

    pub(crate) fn fail_internal_catalog_write() -> bool {
        FAIL_INTERNAL_CATALOG_WRITE.with(Cell::get)
    }
}

pub(crate) const RELKIND_SEQUENCE: &str = "s";
pub(crate) const RELKIND_INTERNAL: &str = "c";
pub(crate) const PG_RUST_SEQUENCES_OID: Oid = Oid(9022);
pub(crate) const PG_RUST_ATTRDEFAULTS_OID: Oid = Oid(9023);
pub(crate) const PG_RUST_SEQUENCES_NAME: &str = "pg_rust_sequences";
pub(crate) const PG_RUST_ATTRDEFAULTS_NAME: &str = "pg_rust_attrdefaults";

const fn col(
    name: &'static str,
    type_oid: TypeOid,
    column_type: ColumnType,
    len: i32,
) -> SysColumn {
    SysColumn {
        name,
        type_oid,
        column_type,
        len,
        not_null: true,
    }
}

const SEQUENCE_COLUMNS: [SysColumn; 3] = [
    col("oid", INT8_OID, ColumnType::Int8, 8),
    col("name", TEXT_OID, ColumnType::Text, -1),
    col("high_water", INT8_OID, ColumnType::Int8, 8),
];
const ATTRDEFAULT_COLUMNS: [SysColumn; 3] = [
    col("rel_oid", INT8_OID, ColumnType::Int8, 8),
    col("attnum", INT4_OID, ColumnType::Int4, 4),
    col("sequence_oid", INT8_OID, ColumnType::Int8, 8),
];

pub(crate) fn sequences_table(first_page: pg_storage::types::PageId) -> SystemTableDef {
    SystemTableDef {
        oid: TableOid(PG_RUST_SEQUENCES_OID),
        name: PG_RUST_SEQUENCES_NAME,
        first_page,
        columns: &SEQUENCE_COLUMNS,
    }
}

pub(crate) fn attrdefaults_table(first_page: pg_storage::types::PageId) -> SystemTableDef {
    SystemTableDef {
        oid: TableOid(PG_RUST_ATTRDEFAULTS_OID),
        name: PG_RUST_ATTRDEFAULTS_NAME,
        first_page,
        columns: &ATTRDEFAULT_COLUMNS,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SequenceState {
    pub(crate) name: String,
    pub(crate) high_water: i64,
}

#[derive(Debug, Default)]
pub(crate) struct SequenceManager {
    pub(crate) states: Mutex<HashMap<Oid, SequenceState>>,
    pub(crate) relation_pages: Mutex<HashMap<Oid, pg_storage::types::PageId>>,
    recovered: Arc<Mutex<HashMap<Oid, i64>>>,
}

impl SequenceManager {
    pub(crate) fn redo_handler(&self) -> SequenceAdvanceRedoHandler {
        SequenceAdvanceRedoHandler {
            recovered: Arc::clone(&self.recovered),
        }
    }

    pub(crate) fn recovered_high_water(&self) -> HashMap<Oid, i64> {
        self.recovered.lock().clone()
    }
}

pub(crate) struct SequenceAdvanceRedoHandler {
    recovered: Arc<Mutex<HashMap<Oid, i64>>>,
}

impl RedoHandler for SequenceAdvanceRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::SequenceAdvance
    }

    fn apply(&self, record: &WalRecord, _ctx: &mut RedoContext<'_>) -> StorageResult<()> {
        let decoded = SequenceAdvanceRecord::decode(&record.payload)?;
        self.recovered
            .lock()
            .entry(decoded.sequence_oid)
            .and_modify(|current| *current = (*current).max(decoded.high_water))
            .or_insert(decoded.high_water);
        Ok(())
    }
}

pub(crate) fn sequence_class_row(oid: Oid, name: &str) -> Vec<Option<pg_am_heap::tuple::Datum>> {
    vec![
        Some(pg_am_heap::tuple::Datum::Int8(oid.0 as i64)),
        Some(pg_am_heap::tuple::Datum::Text(name.to_string())),
        Some(pg_am_heap::tuple::Datum::Text(RELKIND_SEQUENCE.to_string())),
        Some(pg_am_heap::tuple::Datum::Int4(0)),
        Some(pg_am_heap::tuple::Datum::Int8(0)),
        Some(pg_am_heap::tuple::Datum::Int8(HEAP_AM_OID.0 as i64)),
    ]
}
