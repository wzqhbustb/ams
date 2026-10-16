//! System table schemas and physical layout (tech-selection §5.1).
//!
//! Six system tables exist in M2: `pg_class`, `pg_attribute`, `pg_type`,
//! `pg_am`, `pg_index` (the last is initialized but holds no rows until
//! M2b), and `pg_rust_relpages` (Stage K). Their column definitions are
//! hardcoded here — this is the "hardcoded bootstrap" choice of §5.2,
//! replacing PostgreSQL's initdb SQL scripts.
//!
//! `pg_rust_relpages` is **engine-private** (hence the `pg_rust_` prefix and
//! an OID outside the PostgreSQL-conventional range): it is the
//! relation → page-chain directory recording each heap relation's first/last
//! page and page count, so `pg_class` keeps its PostgreSQL-compatible shape.
//! Rows are written by DDL (`create_table`, Stage K engine waves), not by
//! bootstrap; the bootstrap only initializes its (empty) first page.
//!
//! # Physical layout (M2a simplification)
//!
//! With a single data file, every system table gets a **fixed first page**:
//! `pg_class` = page 1, `pg_attribute` = page 2, `pg_type` = page 3,
//! `pg_am` = page 4, `pg_index` = page 5, `pg_rust_relpages` = page 6. The
//! bootstrap content of each table fits in one page. User relations are
//! located through `pg_rust_relpages` plus the on-disk page chain (Stage K);
//! these constants are the mapping for system tables only.

use pg_am_heap::ColumnType;
use pg_storage::types::{Oid, PageId};

use crate::builtin_types::{INT4_OID, INT8_OID, TEXT_OID};
use crate::TableOid;

/// `pg_class` OID (PostgreSQL conventional value).
pub const PG_CLASS_OID: TableOid = TableOid(Oid(1259));
/// `pg_attribute` OID (PostgreSQL conventional value).
pub const PG_ATTRIBUTE_OID: TableOid = TableOid(Oid(1249));
/// `pg_type` OID (PostgreSQL conventional value).
pub const PG_TYPE_OID: TableOid = TableOid(Oid(1247));
/// `pg_am` OID (PostgreSQL conventional value).
pub const PG_AM_OID: TableOid = TableOid(Oid(2601));
/// `pg_index` OID (PostgreSQL conventional value).
pub const PG_INDEX_OID: TableOid = TableOid(Oid(2610));
/// `pg_rust_relpages` OID (engine-private directory table, Stage K; outside
/// the PostgreSQL-conventional system OID range).
pub const PG_RELPAGES_OID: TableOid = TableOid(Oid(9021));

/// `pg_am` OID of the heap access method (PostgreSQL conventional value).
pub const HEAP_AM_OID: Oid = Oid(2);
/// `pg_am` OID of the B+Tree access method (PostgreSQL conventional value).
pub const BTREE_AM_OID: Oid = Oid(403);
/// `pg_am` OID of the HNSW vector access method (M6 Stage C — engine-side
/// registration; the `CREATE INDEX ... USING hnsw` SQL form is Phase 4b).
pub const HNSW_AM_OID: Oid = Oid(9878);

/// `pg_class.relkind` for an ordinary table.
pub const RELKIND_TABLE: &str = "r";
/// `pg_class.relkind` for an index (unused in M2a; reserved for M2b).
pub const RELKIND_INDEX: &str = "i";

/// A column of a system table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SysColumn {
    /// `pg_attribute.attname`.
    pub name: &'static str,
    /// `pg_attribute.atttypid`: one of the built-in type OIDs.
    pub type_oid: crate::TypeOid,
    /// The heap tuple codec type for this column.
    pub column_type: ColumnType,
    /// `pg_attribute.attlen`: fixed width in bytes, `-1` for varlena.
    pub len: i32,
    /// `pg_attribute.attnotnull` / `attnullable` are derived from this flag.
    pub not_null: bool,
}

/// Static definition of one system table.
#[derive(Debug, Clone, Copy)]
pub struct SystemTableDef {
    /// The table's OID (PostgreSQL conventional value).
    pub oid: TableOid,
    /// `pg_class.relname`.
    pub name: &'static str,
    /// Fixed first page in the single data file (M2a simplification, see the
    /// module-level note).
    pub first_page: PageId,
    /// Column definitions in schema order.
    pub columns: &'static [SysColumn],
}

impl SystemTableDef {
    /// The tuple codec column types, for `encode_tuple` / `decode_tuple`.
    pub fn column_types(&self) -> Vec<ColumnType> {
        self.columns.iter().map(|c| c.column_type).collect()
    }
}

const PG_CLASS_COLUMNS: &[SysColumn] = &[
    SysColumn {
        name: "oid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "relname",
        type_oid: TEXT_OID,
        column_type: ColumnType::Text,
        len: -1,
        not_null: true,
    },
    SysColumn {
        name: "relkind",
        type_oid: TEXT_OID,
        column_type: ColumnType::Text,
        len: -1,
        not_null: true,
    },
    SysColumn {
        name: "relnatts",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
    // 0 means "no TOAST table"; kept nullable to mirror PostgreSQL, where
    // the column is 0-filled rather than NULL — here it is 0-filled too, the
    // flag only describes the schema.
    SysColumn {
        name: "reltoastrelid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: false,
    },
    SysColumn {
        name: "relam",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
];

const PG_ATTRIBUTE_COLUMNS: &[SysColumn] = &[
    SysColumn {
        name: "attrelid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "attname",
        type_oid: TEXT_OID,
        column_type: ColumnType::Text,
        len: -1,
        not_null: true,
    },
    SysColumn {
        name: "atttypid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "attlen",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
    SysColumn {
        name: "attnum",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
    // M2 has no bool type; attnotnull / attnullable are Int4 0/1 (§5.1).
    SysColumn {
        name: "attnotnull",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
    SysColumn {
        name: "attnullable",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
];

const PG_TYPE_COLUMNS: &[SysColumn] = &[
    SysColumn {
        name: "oid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "typname",
        type_oid: TEXT_OID,
        column_type: ColumnType::Text,
        len: -1,
        not_null: true,
    },
    SysColumn {
        name: "typlen",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
];

const PG_AM_COLUMNS: &[SysColumn] = &[
    SysColumn {
        name: "oid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "amname",
        type_oid: TEXT_OID,
        column_type: ColumnType::Text,
        len: -1,
        not_null: true,
    },
];

// pg_index is initialized as an empty page in M2a (rows arrive in M2b), but
// its schema is fixed now so pg_attribute can describe all five tables and
// M2b does not need a catalog format change. Column set is a PostgreSQL
// subset: identity (indexrelid, indrelid), width (indnatts), and the two
// flags a uniqueness-checking executor needs.
const PG_INDEX_COLUMNS: &[SysColumn] = &[
    SysColumn {
        name: "indexrelid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "indrelid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "indnatts",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
    SysColumn {
        name: "indisunique",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
    SysColumn {
        name: "indisprimary",
        type_oid: INT4_OID,
        column_type: ColumnType::Int4,
        len: 4,
        not_null: true,
    },
];

// pg_rust_relpages (Stage K): the engine-private relation → page-chain
// directory. One row per heap relation, maintained by DDL (create_table /
// drop_table and heap extension in later Stage K waves); bootstrap leaves it
// empty. It deliberately lives outside pg_class so the PG-compatible catalog
// shape is untouched (see the module-level note).
const PG_RELPAGES_COLUMNS: &[SysColumn] = &[
    SysColumn {
        name: "rel_oid",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "first_page",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "last_page",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
    SysColumn {
        name: "page_count",
        type_oid: INT8_OID,
        column_type: ColumnType::Int8,
        len: 8,
        not_null: true,
    },
];

/// `pg_class` definition: every relation (table, index, TOAST table).
pub const PG_CLASS: SystemTableDef = SystemTableDef {
    oid: PG_CLASS_OID,
    name: "pg_class",
    first_page: PageId(1),
    columns: PG_CLASS_COLUMNS,
};

/// `pg_attribute` definition: column definitions of all relations.
pub const PG_ATTRIBUTE: SystemTableDef = SystemTableDef {
    oid: PG_ATTRIBUTE_OID,
    name: "pg_attribute",
    first_page: PageId(2),
    columns: PG_ATTRIBUTE_COLUMNS,
};

/// `pg_type` definition: data types (M2: built-ins only).
pub const PG_TYPE: SystemTableDef = SystemTableDef {
    oid: PG_TYPE_OID,
    name: "pg_type",
    first_page: PageId(3),
    columns: PG_TYPE_COLUMNS,
};

/// `pg_am` definition: access methods (M2: heap and btree).
pub const PG_AM: SystemTableDef = SystemTableDef {
    oid: PG_AM_OID,
    name: "pg_am",
    first_page: PageId(4),
    columns: PG_AM_COLUMNS,
};

/// `pg_index` definition: index metadata (empty until M2b).
///
/// `Catalog::open` intentionally does **not** read this table (it reads the
/// four PG-conventional content-bearing tables plus `pg_rust_relpages` —
/// reading an always-empty `pg_index` page would be wasted I/O). M2b index
/// code is responsible for writing and reading `pg_index` rows directly
/// through the buffer pool.
pub const PG_INDEX: SystemTableDef = SystemTableDef {
    oid: PG_INDEX_OID,
    name: "pg_index",
    first_page: PageId(5),
    columns: PG_INDEX_COLUMNS,
};

/// `pg_rust_relpages` definition: engine-private relation → page-chain
/// directory (Stage K; empty until DDL writes rows).
///
/// Unlike `pg_index`, `Catalog::open` **does** read this table: the heap AM's
/// page-directory rebuild and later Stage K engine waves consult it.
pub const PG_RELPAGES: SystemTableDef = SystemTableDef {
    oid: PG_RELPAGES_OID,
    name: "pg_rust_relpages",
    first_page: PageId(6),
    columns: PG_RELPAGES_COLUMNS,
};

/// All system tables, in fixed-page order (pages 1..=6).
pub const SYSTEM_TABLES: [SystemTableDef; 6] = [
    PG_CLASS,
    PG_ATTRIBUTE,
    PG_TYPE,
    PG_AM,
    PG_INDEX,
    PG_RELPAGES,
];

/// The highest page ID reserved for system table bootstrap content.
pub const LAST_SYSTEM_PAGE: PageId = PageId(6);

/// All built-in type OIDs referenced by system table columns are defined in
/// [`crate::builtin_types`]; this helper keeps that invariant checked in
/// tests.
#[cfg(test)]
fn referenced_type_oids() -> Vec<crate::TypeOid> {
    let mut oids: Vec<_> = SYSTEM_TABLES
        .iter()
        .flat_map(|t| t.columns.iter().map(|c| c.type_oid))
        .collect();
    oids.sort_unstable();
    oids.dedup();
    oids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin_types::{builtin_type, BYTEA_OID, TIMESTAMPTZ_OID, UUID_OID};

    #[test]
    fn fixed_first_pages_are_sequential_from_one() {
        for (i, def) in SYSTEM_TABLES.iter().enumerate() {
            assert_eq!(def.first_page, PageId(i as u64 + 1));
        }
        assert_eq!(SYSTEM_TABLES[5].first_page, LAST_SYSTEM_PAGE);
    }

    #[test]
    fn system_table_oids_are_pg_conventional() {
        assert_eq!(PG_CLASS.oid.raw(), Oid(1259));
        assert_eq!(PG_ATTRIBUTE.oid.raw(), Oid(1249));
        assert_eq!(PG_TYPE.oid.raw(), Oid(1247));
        assert_eq!(PG_AM.oid.raw(), Oid(2601));
        assert_eq!(PG_INDEX.oid.raw(), Oid(2610));
        // pg_rust_relpages is engine-private: a fixed OID outside the
        // PostgreSQL-conventional range, distinct from every system table.
        assert_eq!(PG_RELPAGES.oid.raw(), Oid(9021));
        assert!(
            SYSTEM_TABLES
                .iter()
                .filter(|t| t.oid == PG_RELPAGES.oid)
                .count()
                == 1
        );
    }

    #[test]
    fn all_column_type_oids_are_builtin() {
        for oid in referenced_type_oids() {
            assert!(
                builtin_type(oid).is_some(),
                "system table column references non-builtin type {oid:?}"
            );
        }
    }

    #[test]
    fn codec_types_match_type_oids() {
        for def in SYSTEM_TABLES {
            for col in def.columns {
                assert_eq!(
                    builtin_type(col.type_oid).unwrap().column_type,
                    col.column_type,
                    "{}.{}",
                    def.name,
                    col.name
                );
            }
        }
    }

    #[test]
    fn expected_schemas() {
        assert_eq!(PG_CLASS.columns.len(), 6);
        assert_eq!(PG_ATTRIBUTE.columns.len(), 7);
        assert_eq!(PG_TYPE.columns.len(), 3);
        assert_eq!(PG_AM.columns.len(), 2);
        assert_eq!(PG_INDEX.columns.len(), 5);
        assert_eq!(PG_RELPAGES.columns.len(), 4);
        // Type OIDs actually used by the system columns.
        for oid in [INT8_OID, TEXT_OID, INT4_OID] {
            assert!(referenced_type_oids().contains(&oid));
        }
        assert!(!referenced_type_oids().contains(&BYTEA_OID));
        assert!(!referenced_type_oids().contains(&TIMESTAMPTZ_OID));
        assert!(!referenced_type_oids().contains(&UUID_OID));
    }
}
