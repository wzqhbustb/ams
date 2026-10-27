//! Built-in type definitions (tech-selection §5.2).
//!
//! M2 hardcodes the six built-in types here; there is no `CREATE TYPE` at
//! runtime. OIDs use the PostgreSQL conventional values so dumps and mental
//! models transfer directly. `timestamptz` is 1184 (1114 is `timestamp`
//! without time zone in PostgreSQL, which M2 does not have).

use pg_am_heap::ColumnType;
use pg_storage::types::Oid;

use crate::TypeOid;

/// PostgreSQL-conventional OID of `int4`.
pub const INT4_OID: TypeOid = TypeOid(Oid(23));
/// PostgreSQL-conventional OID of `int8`.
pub const INT8_OID: TypeOid = TypeOid(Oid(20));
/// PostgreSQL-conventional OID of `text`.
pub const TEXT_OID: TypeOid = TypeOid(Oid(25));
/// PostgreSQL-conventional OID of `bytea`.
pub const BYTEA_OID: TypeOid = TypeOid(Oid(17));
/// PostgreSQL-conventional OID of `timestamptz`.
pub const TIMESTAMPTZ_OID: TypeOid = TypeOid(Oid(1184));
/// PostgreSQL-conventional OID of `uuid`.
pub const UUID_OID: TypeOid = TypeOid(Oid(2950));
/// The HNSW vector column type's OID (M6 Stage C): a system OID, but NOT
/// a [`BUILTIN_TYPES`] entry — a vector's heap codec type carries its dim
/// (`ColumnType::Vector(dim)`), which a static table cannot express. The
/// dim travels in the `pg_attribute` row's `len` (= dim × 4, the inline
/// width); [`column_type_of_attr`] resolves the codec from that pair.
pub const VECTOR_OID: TypeOid = TypeOid(Oid(9877));

/// Resolve a column's heap codec type from its catalog `(type_oid, len)`
/// pair. VECTOR columns resolve from `len` (= dim × 4); everything else
/// delegates to the static [`BUILTIN_TYPES`] table. `None` for an unknown
/// OID or a malformed vector len.
pub fn column_type_of_attr(type_oid: TypeOid, len: i32) -> Option<ColumnType> {
    if type_oid == VECTOR_OID {
        if len <= 0 || len % 4 != 0 {
            return None;
        }
        return ColumnType::vector((len / 4) as u32).ok();
    }
    builtin_type(type_oid).map(|t| t.column_type)
}

/// A hardcoded built-in type definition, mirrored into `pg_type` at
/// bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinType {
    /// The type's OID (PostgreSQL conventional value).
    pub oid: TypeOid,
    /// `pg_type.typname`.
    pub name: &'static str,
    /// `pg_type.typlen`: fixed width in bytes, `-1` for varlena types.
    pub len: i32,
    /// The heap tuple codec type used for columns of this type.
    pub column_type: ColumnType,
}

/// All built-in types, in bootstrap write order.
pub const BUILTIN_TYPES: [BuiltinType; 6] = [
    BuiltinType {
        oid: INT4_OID,
        name: "int4",
        len: 4,
        column_type: ColumnType::Int4,
    },
    BuiltinType {
        oid: INT8_OID,
        name: "int8",
        len: 8,
        column_type: ColumnType::Int8,
    },
    BuiltinType {
        oid: TEXT_OID,
        name: "text",
        len: -1,
        column_type: ColumnType::Text,
    },
    BuiltinType {
        oid: BYTEA_OID,
        name: "bytea",
        len: -1,
        column_type: ColumnType::Bytea,
    },
    BuiltinType {
        oid: TIMESTAMPTZ_OID,
        name: "timestamptz",
        len: 8,
        column_type: ColumnType::Timestamptz,
    },
    BuiltinType {
        oid: UUID_OID,
        name: "uuid",
        len: 16,
        column_type: ColumnType::Uuid,
    },
];

/// Look up a built-in type by OID.
pub fn builtin_type(oid: TypeOid) -> Option<&'static BuiltinType> {
    BUILTIN_TYPES.iter().find(|t| t.oid == oid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_builtin_types_are_system_oids() {
        for ty in BUILTIN_TYPES {
            assert!(ty.oid.raw().is_system(), "{} has a non-system OID", ty.name);
        }
    }

    #[test]
    fn oids_are_unique() {
        for (i, a) in BUILTIN_TYPES.iter().enumerate() {
            for b in &BUILTIN_TYPES[i + 1..] {
                assert_ne!(a.oid, b.oid, "{} and {} share an OID", a.name, b.name);
            }
        }
    }

    #[test]
    fn lookup_by_oid() {
        assert_eq!(builtin_type(INT4_OID).unwrap().name, "int4");
        assert!(builtin_type(TypeOid(Oid(999_999))).is_none());
    }
}
