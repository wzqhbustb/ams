//! Text encoding of engine values and the type-OID mapping (§7.2).
//!
//! Encoding rules (the wire speaks text format only):
//!
//! | `ColumnType`   | text form                       | reported OID |
//! |----------------|---------------------------------|--------------|
//! | `Int4`         | decimal                         | 23 (`int4`)  |
//! | `Int8`         | decimal                         | 20 (`int8`)  |
//! | `Text`         | as-is                           | 25 (`text`)  |
//! | `Bytea`        | `\x` + lowercase hex (PG text)  | 17 (`bytea`) |
//! | `Timestamptz`  | µs-since-epoch integer          | 20 (`int8`)  |
//! | `Uuid`         | standard hyphenated string      | 25 (`text`)  |
//!
//! The last two rows **report a different OID than real PostgreSQL** on
//! purpose: the encoding must round-trip through a stock client, and no
//! stock client decodes a µs integer as `timestamptz` (1184) or accepts a
//! bare string for `uuid` (2950) without type-specific support. Reporting
//! the OID that matches the bytes on the wire keeps `psql`, psycopg2,
//! node-postgres and rust-postgres all consistent. True PG OIDs are a
//! Phase 4a concern (documented in stage_spec Stage F trade-offs).

use pg_engine::{ColumnType, Datum};

use crate::error::{Result, WireError};

/// Wire-level type descriptor: the OID and fixed width a RowDescription
/// reports for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireType {
    /// PostgreSQL type OID (see the table in the module docs).
    pub oid: u32,
    /// Fixed width in bytes, or -1 for varlena.
    pub len: i16,
}

/// Map a heap codec type to its wire descriptor (see module docs for the
/// full mapping table).
pub fn wire_type(ty: ColumnType) -> WireType {
    match ty {
        ColumnType::Int4 => WireType { oid: 23, len: 4 },
        ColumnType::Int8 => WireType { oid: 20, len: 8 },
        ColumnType::Text => WireType { oid: 25, len: -1 },
        ColumnType::Bytea => WireType { oid: 17, len: -1 },
        // Reported as INT8 / TEXT so the declared OID matches the bytes
        // (module docs). `typlen` 8 for the µs integer.
        ColumnType::Timestamptz => WireType { oid: 20, len: 8 },
        ColumnType::Uuid => WireType { oid: 25, len: -1 },
    }
}

/// Infer a column type from a value when the table schema is unavailable
/// (e.g. the table was dropped between parse and execution). Used only as
/// a fallback; schema-driven typing via [`wire_type`] is the primary path.
pub fn column_type_of(d: &Datum) -> ColumnType {
    match d {
        Datum::Int4(_) => ColumnType::Int4,
        Datum::Int8(_) => ColumnType::Int8,
        Datum::Text(_) => ColumnType::Text,
        Datum::Bytea(_) => ColumnType::Bytea,
        Datum::Timestamptz(_) => ColumnType::Timestamptz,
        Datum::Uuid(_) => ColumnType::Uuid,
        // The underlying type of a TOASTed value is unknowable here; TEXT
        // is the least-wrong placeholder since the encode path errors out
        // on `External` anyway.
        Datum::External(_) => ColumnType::Text,
    }
}

/// Text-encode one datum (module-docs table). Returns an error for
/// `Datum::External` — M3 never resolves TOAST pointers on the read path,
/// so this is a loud failure, not silent garbage.
pub fn encode_text(d: &Datum) -> Result<Vec<u8>> {
    let bytes = match d {
        Datum::Int4(v) => v.to_string().into_bytes(),
        Datum::Int8(v) => v.to_string().into_bytes(),
        Datum::Timestamptz(us) => us.to_string().into_bytes(),
        Datum::Uuid(u) => u.to_string().into_bytes(),
        Datum::Text(s) => s.as_bytes().to_vec(),
        Datum::Bytea(b) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut out = Vec::with_capacity(2 + b.len() * 2);
            out.extend_from_slice(b"\\x");
            for byte in b {
                out.push(HEX[(byte >> 4) as usize]);
                out.push(HEX[(byte & 0x0f) as usize]);
            }
            out
        }
        Datum::External(_) => {
            return Err(WireError::Encode(
                "TOASTed value cannot be resolved on the wire (M3)".to_string(),
            ))
        }
    };
    Ok(bytes)
}
