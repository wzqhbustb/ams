//! TOAST pointer encoding and hidden chunk-table I/O (Phase 4a Stage D).
//!
//! Only **EXTERNAL** out-of-line storage is supported; compression is deferred
//! to Phase 7b. An external attribute is stored in the main tuple as a 20-byte
//! TOAST pointer:
//!
//! ```text
//! vl_len_:       u32  (high 2 bits = 01 marks external)   offset  0..4
//! va_rawsize:    u32  (original uncompressed size)        offset  4..8
//! va_extsize:    u32  (stored size)                       offset  8..12
//! va_valueid:    u32  (chunk group id in the TOAST table) offset 12..16
//! va_toastrelid: u32  (TOAST table OID, low 32 bits)      offset 16..20
//! ```
//!
//! Chunk rows are ordinary heap tuples in an AM-internal relation. Query reads
//! detoast through the query snapshot; physical scans are reserved for valueid
//! recovery and vacuum's reference accounting, never for query results.

use std::collections::{BTreeMap, HashSet};

use pg_storage::clog::ClogAccessor;
use pg_storage::types::{Oid, PageId, Tid, TxnId};
use pg_txn::Snapshot;

use crate::access_method::{AccessMethod, InsertContext, RelationDesc, ScanContext, Vacuumable};
use crate::error::{HeapError, Result};
use crate::heap_am::HeapAM;
use crate::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};

/// Size of a TOAST pointer in bytes (5 × u32, §四).
pub const TOAST_POINTER_SIZE: usize = 20;

/// Serialized attribute size above which a value is moved out of line (§四).
pub const TOAST_TUPLE_THRESHOLD: usize = 2048;

/// Maximum payload bytes per TOAST chunk (§四).
pub const TOAST_MAX_CHUNK_SIZE: usize = 2000;

/// `vl_len_` bit pattern marking an external (out-of-line) attribute: the
/// high 2 bits are `01` (§四).
pub const VARLENA_EXTERNAL_FLAG: u32 = 0x4000_0000;

/// Mask covering the two high tag bits of a varlena header.
const VARLENA_TAG_MASK: u32 = 0xC000_0000;

/// Mask extracting the total-length field of a varlena header (tag bits
/// cleared).
pub const VARLENA_LEN_MASK: u32 = !VARLENA_TAG_MASK;

/// Return true if a varlena header marks the attribute as external
/// (high 2 bits = `01`, §四).
pub fn is_external(vl_len: u32) -> bool {
    vl_len & VARLENA_TAG_MASK == VARLENA_EXTERNAL_FLAG
}

/// A 20-byte TOAST pointer stored in place of an out-of-line attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastPointer {
    /// Original uncompressed size of the attribute.
    pub va_rawsize: u32,
    /// Size as stored in the TOAST table (M2: always == `va_rawsize`, no
    /// compression).
    pub va_extsize: u32,
    /// Chunk group id (`va_valueid`) inside the TOAST table.
    pub va_valueid: u32,
    /// TOAST table OID, low 32 bits (§四: OIDs are u64 but M2 relation
    /// counts stay below 2^32).
    pub va_toastrelid: u32,
}

impl ToastPointer {
    /// Encode to the 20-byte on-disk layout. `vl_len_` is written as
    /// `TOAST_POINTER_SIZE` with the external tag bits set.
    pub fn encode(&self) -> [u8; TOAST_POINTER_SIZE] {
        let vl_len = (TOAST_POINTER_SIZE as u32) | VARLENA_EXTERNAL_FLAG;
        let mut out = [0u8; TOAST_POINTER_SIZE];
        out[0..4].copy_from_slice(&vl_len.to_le_bytes());
        out[4..8].copy_from_slice(&self.va_rawsize.to_le_bytes());
        out[8..12].copy_from_slice(&self.va_extsize.to_le_bytes());
        out[12..16].copy_from_slice(&self.va_valueid.to_le_bytes());
        out[16..20].copy_from_slice(&self.va_toastrelid.to_le_bytes());
        out
    }

    /// Decode from the 20-byte on-disk layout.
    ///
    /// Returns [`HeapError::Corrupted`] if the slice is not exactly 20
    /// bytes, the external tag is missing, or the length field disagrees
    /// with [`TOAST_POINTER_SIZE`].
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != TOAST_POINTER_SIZE {
            return Err(HeapError::Corrupted(format!(
                "TOAST pointer must be {TOAST_POINTER_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let vl_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if !is_external(vl_len) {
            return Err(HeapError::Corrupted(format!(
                "TOAST pointer missing external tag: vl_len_ = {vl_len:#010x}"
            )));
        }
        if (vl_len & VARLENA_LEN_MASK) as usize != TOAST_POINTER_SIZE {
            return Err(HeapError::Corrupted(format!(
                "TOAST pointer length mismatch: vl_len_ = {:#010x}",
                vl_len & VARLENA_LEN_MASK
            )));
        }
        Ok(ToastPointer {
            va_rawsize: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            va_extsize: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            va_valueid: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            va_toastrelid: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
        })
    }
}

/// Physical location of a main relation's hidden TOAST chunk heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastRelation {
    /// Hidden relation OID, allocated by the catalog OID allocator.
    pub rel_oid: Oid,
    /// Head of the hidden relation's heap page chain.
    pub first_page: PageId,
}

impl ToastRelation {
    pub(crate) fn relation_desc(self) -> RelationDesc<'static> {
        RelationDesc {
            rel_oid: self.rel_oid,
            first_page: self.first_page,
            columns: &TOAST_CHUNK_COLUMNS,
        }
    }
}

/// `(valueid, chunk_seq, payload)` schema shared by every hidden chunk heap.
static TOAST_CHUNK_COLUMNS: [ColumnType; 3] =
    [ColumnType::Int8, ColumnType::Int4, ColumnType::Bytea];

/// Whether values of this column type may be moved out of line.
pub fn toastable(column: ColumnType) -> bool {
    matches!(
        column,
        ColumnType::Text | ColumnType::Bytea | ColumnType::Jsonb | ColumnType::Vector(_)
    )
}

/// Size of the inline representation used when choosing the next attribute to
/// externalize. External pointers already have their final 20-byte size.
pub fn inline_size(column: ColumnType, datum: &Datum) -> Result<usize> {
    match (column, datum) {
        (_, Datum::External(_)) if toastable(column) => Ok(TOAST_POINTER_SIZE),
        (ColumnType::Text, Datum::Text(value)) => value
            .len()
            .checked_add(4)
            .ok_or_else(|| HeapError::InvalidArgument("text size overflow".to_string())),
        (ColumnType::Bytea, Datum::Bytea(value)) => value
            .len()
            .checked_add(4)
            .ok_or_else(|| HeapError::InvalidArgument("bytea size overflow".to_string())),
        (ColumnType::Jsonb, Datum::Jsonb(value)) => value
            .len()
            .checked_add(4)
            .ok_or_else(|| HeapError::InvalidArgument("jsonb size overflow".to_string())),
        (ColumnType::Vector(dim), Datum::Vector(value)) if value.len() == dim as usize => value
            .len()
            .checked_mul(4)
            .ok_or_else(|| HeapError::InvalidArgument("vector size overflow".to_string())),
        _ => Err(HeapError::InvalidArgument(format!(
            "datum {datum:?} is not TOASTable as {column:?}"
        ))),
    }
}

fn encode_payload(column: ColumnType, datum: &Datum) -> Result<Vec<u8>> {
    match (column, datum) {
        (ColumnType::Text, Datum::Text(value)) => Ok(value.as_bytes().to_vec()),
        (ColumnType::Bytea, Datum::Bytea(value)) => Ok(value.clone()),
        (ColumnType::Jsonb, Datum::Jsonb(value)) => {
            serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| HeapError::InvalidArgument(format!("invalid JSONB: {error}")))?;
            Ok(value.as_bytes().to_vec())
        }
        (ColumnType::Vector(dim), Datum::Vector(value)) if value.len() == dim as usize => {
            let mut bytes = Vec::with_capacity(value.len() * 4);
            for component in value {
                bytes.extend_from_slice(&component.to_le_bytes());
            }
            Ok(bytes)
        }
        _ => Err(HeapError::InvalidArgument(format!(
            "datum {datum:?} cannot be externalized as {column:?}"
        ))),
    }
}

fn decode_payload(column: ColumnType, payload: Vec<u8>) -> Result<Datum> {
    match column {
        ColumnType::Text => String::from_utf8(payload)
            .map(Datum::Text)
            .map_err(|_| HeapError::Corrupted("TOAST text is not valid UTF-8".to_string())),
        ColumnType::Bytea => Ok(Datum::Bytea(payload)),
        ColumnType::Jsonb => {
            let value = String::from_utf8(payload)
                .map_err(|_| HeapError::Corrupted("TOAST JSONB is not valid UTF-8".to_string()))?;
            serde_json::from_str::<serde_json::Value>(&value)
                .map_err(|error| HeapError::Corrupted(format!("invalid TOAST JSONB: {error}")))?;
            Ok(Datum::Jsonb(value))
        }
        ColumnType::Vector(dim) => {
            let expected = dim as usize * 4;
            if payload.len() != expected {
                return Err(HeapError::Corrupted(format!(
                    "TOAST vector has {} bytes, expected {expected} for dim {dim}",
                    payload.len()
                )));
            }
            Ok(Datum::Vector(
                payload
                    .chunks_exact(4)
                    .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("four-byte chunk")))
                    .collect(),
            ))
        }
        _ => Err(HeapError::Corrupted(format!(
            "TOAST pointer found in non-TOASTable column {column:?}"
        ))),
    }
}

pub(crate) fn externalize_value(
    heap: &HeapAM,
    main: &RelationDesc<'_>,
    snapshot: &Snapshot,
    column: ColumnType,
    datum: &Datum,
) -> Result<Datum> {
    let payload = encode_payload(column, datum)?;
    let rawsize = u32::try_from(payload.len()).map_err(|_| {
        HeapError::InvalidArgument(format!("TOAST value is too large: {} bytes", payload.len()))
    })?;
    let toast = heap.toast_relation(main.rel_oid)?;
    let valueid = heap.allocate_toast_valueid(main.rel_oid)?;

    // Empty input is not a normal externalization candidate, but writing one
    // empty chunk keeps this low-level primitive total and unambiguous.
    if payload.is_empty() {
        insert_chunk(heap, toast, snapshot, valueid, 0, &payload)?;
    } else {
        for (seq, bytes) in payload.chunks(TOAST_MAX_CHUNK_SIZE).enumerate() {
            let seq = u32::try_from(seq).map_err(|_| {
                HeapError::InvalidArgument("TOAST chunk sequence overflow".to_string())
            })?;
            insert_chunk(heap, toast, snapshot, valueid, seq, bytes)?;
        }
    }

    Ok(Datum::External(ToastPointer {
        va_rawsize: rawsize,
        va_extsize: rawsize,
        va_valueid: valueid,
        va_toastrelid: toast.rel_oid.0 as u32,
    }))
}

fn insert_chunk(
    heap: &HeapAM,
    toast: ToastRelation,
    snapshot: &Snapshot,
    valueid: u32,
    chunk_seq: u32,
    payload: &[u8],
) -> Result<()> {
    if payload.len() > TOAST_MAX_CHUNK_SIZE {
        return Err(HeapError::InvalidArgument(format!(
            "TOAST chunk payload {} exceeds {TOAST_MAX_CHUNK_SIZE}",
            payload.len()
        )));
    }
    let row = vec![
        Some(Datum::Int8(i64::from(valueid))),
        Some(Datum::Int4(chunk_seq as i32)),
        Some(Datum::Bytea(payload.to_vec())),
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
            snapshot.curcid(),
        ),
        &TOAST_CHUNK_COLUMNS,
        &row,
    )?;
    // This is deliberately the ordinary HeapInsert path: the chunk tuple gets
    // the same current XID as the main row and its WAL precedes the main row's.
    heap.insert(InsertContext {
        rel: toast.relation_desc(),
        snapshot,
        tuple: &tuple,
        out_tid: None,
    })
}

pub(crate) fn detoast_values(
    heap: &HeapAM,
    main: &RelationDesc<'_>,
    snapshot: &Snapshot,
    clog: &dyn ClogAccessor,
    values: &[Option<Datum>],
) -> Result<Vec<Option<Datum>>> {
    let Some(pointer) = values.iter().find_map(|value| match value {
        Some(Datum::External(pointer)) => Some(pointer),
        _ => None,
    }) else {
        return Ok(values.to_vec());
    };
    let toast = checked_toast_relation(heap, main, pointer)?;
    // One visibility-filtered chunk scan per main row, not one per attribute.
    let rows = heap.scan(ScanContext {
        rel: toast.relation_desc(),
        snapshot,
        clog,
    })?;
    values
        .iter()
        .zip(main.columns.iter())
        .map(|(value, &column)| match value {
            Some(Datum::External(pointer)) => {
                checked_toast_relation(heap, main, pointer)?;
                assemble_value(column, pointer, rows.iter().cloned())
            }
            _ => Ok(value.clone()),
        })
        .collect()
}

/// Physical detoast for recovery-time index maintenance and vacuum. This never
/// feeds a query result: valueid uniqueness makes the addressed group
/// unambiguous even when its inserting transaction is a crashed loser whose
/// index entry must be compensated. Query reads use [`detoast_values`] instead.
pub(crate) fn detoast_values_physical(
    heap: &HeapAM,
    main: &RelationDesc<'_>,
    values: &[Option<Datum>],
) -> Result<Vec<Option<Datum>>> {
    let Some(pointer) = values.iter().find_map(|value| match value {
        Some(Datum::External(pointer)) => Some(pointer),
        _ => None,
    }) else {
        return Ok(values.to_vec());
    };
    let toast = checked_toast_relation(heap, main, pointer)?;
    let rows: Vec<(Tid, Vec<Option<Datum>>)> = heap
        .scan_physical(&toast.relation_desc())?
        .into_iter()
        .map(|(tid, _header, values)| (tid, values))
        .collect();
    values
        .iter()
        .zip(main.columns.iter())
        .map(|(value, &column)| match value {
            Some(Datum::External(pointer)) => {
                checked_toast_relation(heap, main, pointer)?;
                assemble_value(column, pointer, rows.iter().cloned())
            }
            _ => Ok(value.clone()),
        })
        .collect()
}

fn checked_toast_relation(
    heap: &HeapAM,
    main: &RelationDesc<'_>,
    pointer: &ToastPointer,
) -> Result<ToastRelation> {
    let toast = heap.toast_relation(main.rel_oid)?;
    if pointer.va_toastrelid != toast.rel_oid.0 as u32 {
        return Err(HeapError::Corrupted(format!(
            "TOAST pointer relation {} does not match table {} relation {}",
            pointer.va_toastrelid, main.rel_oid.0, toast.rel_oid.0
        )));
    }
    if pointer.va_rawsize != pointer.va_extsize {
        return Err(HeapError::Corrupted(
            "compressed TOAST values are not supported".to_string(),
        ));
    }
    Ok(toast)
}

fn assemble_value(
    column: ColumnType,
    pointer: &ToastPointer,
    rows: impl Iterator<Item = (Tid, Vec<Option<Datum>>)>,
) -> Result<Option<Datum>> {
    let mut chunks = BTreeMap::new();
    for (_tid, values) in rows {
        let Some((valueid, seq, payload)) = chunk_parts(&values)? else {
            continue;
        };
        if valueid != pointer.va_valueid {
            continue;
        }
        if chunks.insert(seq, payload).is_some() {
            return Err(HeapError::Corrupted(format!(
                "duplicate TOAST chunk ({}, {seq})",
                pointer.va_valueid
            )));
        }
    }

    let expected_size = pointer.va_extsize as usize;
    // Even a zero-length value has one seq=0 empty chunk. No visible chunks
    // therefore always means missing/corrupt data, never a valid empty value.
    if chunks.is_empty() {
        return Err(HeapError::Corrupted(format!(
            "no visible chunks for TOAST valueid {}",
            pointer.va_valueid
        )));
    }
    let assembled_size = chunks.values().try_fold(0usize, |total, bytes| {
        total.checked_add(bytes.len()).ok_or_else(|| {
            HeapError::Corrupted(format!(
                "TOAST valueid {} size overflow",
                pointer.va_valueid
            ))
        })
    })?;
    if assembled_size != expected_size || pointer.va_rawsize as usize != assembled_size {
        return Err(HeapError::Corrupted(format!(
            "TOAST valueid {} size mismatch: raw={}, ext={}, assembled={}",
            pointer.va_valueid, pointer.va_rawsize, pointer.va_extsize, assembled_size
        )));
    }
    let mut payload = Vec::new();
    payload.try_reserve(assembled_size).map_err(|error| {
        HeapError::Corrupted(format!(
            "TOAST valueid {} cannot allocate {assembled_size} bytes: {error}",
            pointer.va_valueid
        ))
    })?;
    for (expected_seq, (seq, bytes)) in chunks.into_iter().enumerate() {
        if seq != expected_seq as u32 {
            return Err(HeapError::Corrupted(format!(
                "missing or out-of-order TOAST chunk {} at sequence {expected_seq}: got {seq}",
                pointer.va_valueid
            )));
        }
        if bytes.len() > TOAST_MAX_CHUNK_SIZE {
            return Err(HeapError::Corrupted(format!(
                "TOAST chunk ({}, {seq}) has {} payload bytes",
                pointer.va_valueid,
                bytes.len()
            )));
        }
        payload.extend_from_slice(&bytes);
    }
    if payload.len() != expected_size || pointer.va_rawsize as usize != payload.len() {
        return Err(HeapError::Corrupted(format!(
            "TOAST valueid {} size mismatch: raw={}, ext={}, assembled={}",
            pointer.va_valueid,
            pointer.va_rawsize,
            pointer.va_extsize,
            payload.len()
        )));
    }
    decode_payload(column, payload).map(Some)
}

/// `None` means the row belongs to another valueid; malformed chunk schemas
/// are corruption rather than something a scan may silently skip.
fn chunk_parts(values: &[Option<Datum>]) -> Result<Option<(u32, u32, Vec<u8>)>> {
    let (
        Some(Some(Datum::Int8(valueid))),
        Some(Some(Datum::Int4(seq))),
        Some(Some(Datum::Bytea(payload))),
    ) = (values.first(), values.get(1), values.get(2))
    else {
        return Err(HeapError::Corrupted(
            "malformed TOAST chunk row schema".to_string(),
        ));
    };
    let valueid = u32::try_from(*valueid)
        .map_err(|_| HeapError::Corrupted(format!("invalid TOAST valueid {valueid}")))?;
    let seq = u32::try_from(*seq)
        .map_err(|_| HeapError::Corrupted(format!("invalid TOAST chunk sequence {seq}")))?;
    Ok(Some((valueid, seq, payload.clone())))
}

pub(crate) fn max_valueid(heap: &HeapAM, toast: &RelationDesc<'_>) -> Result<u64> {
    let mut max = 0u64;
    for (_tid, _header, values) in heap.scan_physical(toast)? {
        if let Some((valueid, _seq, _payload)) = chunk_parts(&values)? {
            max = max.max(u64::from(valueid));
        }
    }
    Ok(max)
}

/// Reclaim every chunk group no longer referenced by ANY physical main-row
/// version. Main-row vacuum runs first, so an old snapshot or a partially dead
/// HOT chain conservatively keeps the group alive. Aborted/loser groups with
/// no surviving pointer are collected even when the main relation had no dead
/// rows of its own.
pub(crate) fn vacuum_toast(heap: &HeapAM, main: &RelationDesc<'_>) -> Result<usize> {
    let Some(toast) = heap.toast_relation_optional(main.rel_oid) else {
        return Ok(0);
    };
    let toast_desc = toast.relation_desc();
    let mut referenced = HashSet::new();
    for (_tid, _header, values) in heap.scan_physical(main)? {
        for value in values.into_iter().flatten() {
            if let Datum::External(pointer) = value {
                checked_toast_relation(heap, main, &pointer)?;
                referenced.insert(pointer.va_valueid);
            }
        }
    }

    let mut dead = Vec::new();
    for (tid, _header, values) in heap.scan_physical(&toast_desc)? {
        let Some((valueid, _seq, _payload)) = chunk_parts(&values)? else {
            continue;
        };
        if !referenced.contains(&valueid) {
            dead.push(tid);
        }
    }
    if dead.is_empty() {
        return Ok(0);
    }
    let reclaimed = dead.len();
    heap.reclaim(toast_desc, &dead)?;
    Ok(reclaimed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let ptr = ToastPointer {
            va_rawsize: 5000,
            va_extsize: 5000,
            va_valueid: 42,
            va_toastrelid: 16399,
        };
        let bytes = ptr.encode();
        // External tag present in the high 2 bits (01).
        assert_eq!(bytes[3] & 0xC0, 0x40);
        assert_eq!(ToastPointer::decode(&bytes).unwrap(), ptr);
    }

    #[test]
    fn is_external_checks_tag_bits() {
        assert!(!is_external(100));
        assert!(is_external(100 | VARLENA_EXTERNAL_FLAG));
        // Tag 10 / 11 are not the external marker.
        assert!(!is_external(0x8000_0000 | 100));
        assert!(!is_external(0xC000_0000 | 100));
    }

    #[test]
    fn jsonb_toast_payload_preserves_and_validates_json() {
        let text = "{ \"large\": [1, 2, 3] }".to_string();
        let payload = encode_payload(ColumnType::Jsonb, &Datum::Jsonb(text.clone())).unwrap();
        assert_eq!(
            decode_payload(ColumnType::Jsonb, payload).unwrap(),
            Datum::Jsonb(text)
        );
        assert!(encode_payload(ColumnType::Jsonb, &Datum::Jsonb("not json".to_string())).is_err());
        assert!(decode_payload(ColumnType::Jsonb, b"not json".to_vec()).is_err());
        assert!(toastable(ColumnType::Jsonb));
    }

    #[test]
    fn decode_rejects_bad_input() {
        assert!(ToastPointer::decode(&[0u8; 19]).is_err());
        // Inline varlena header (no external tag) is rejected.
        let mut bytes = [0u8; TOAST_POINTER_SIZE];
        bytes[0..4].copy_from_slice(&20u32.to_le_bytes());
        assert!(matches!(
            ToastPointer::decode(&bytes),
            Err(HeapError::Corrupted(_))
        ));
    }
}
