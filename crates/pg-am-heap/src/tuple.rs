//! Tuple encoding/decoding (tech-selection §三).
//!
//! M2 tuple layout — every offset is 8-byte aligned and matches §三 exactly:
//!
//! ```text
//! ┌ TupleHeader (64 bytes) ─────────────────────────────┐
//! │ t_xmin:      TxnId  u64       offset  0..8          │
//! │ t_xmax:      TxnId  u64       offset  8..16         │
//! │ t_agent_id:  u64              offset 16..24         │
//! │ t_trace_id:  [u8; 16]         offset 24..40         │
//! │ t_ctid:      Tid    12 bytes  offset 40..52         │
//! │   (PageId u64 40..48; slot u16 48..50; pad u16 50..52, always 0)
//! │ t_infomask:  u16              offset 52..54         │
//! │ t_infomask2: u16              offset 54..56         │
//! │ t_hoff:      u16              offset 56..58         │
//! │ t_flags:     u16              offset 58..60         │
//! │ t_cid:       u32              offset 60..64         │
//! ├ NullBitmap (1 bit/attr, only when HEAP_HASNULL) ────┤
//! ├ Attribute data (starts at t_hoff, 8-byte aligned) ──┤
//! │  - fixed-width columns inline, in schema order       │
//! │  - varlena columns: 4-byte varlena header + payload  │
//! │    (external values store a 20-byte TOAST pointer,   │
//! │     pointer includes its own tagged header, §四)     │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! `t_hoff` = 64 + null-bitmap bytes, aligned up to 8. Encoding is
//! hand-written little-endian throughout.

use pg_storage::types::{PageId, Tid, TxnId};

use crate::error::{HeapError, Result};
use crate::toast::{is_external, ToastPointer, TOAST_POINTER_SIZE, VARLENA_LEN_MASK};

/// Size of the fixed tuple header in bytes (§三).
pub const TUPLE_HEADER_SIZE: usize = 64;

// `t_infomask` bits (§三).
/// At least one column is NULL (null bitmap present after the header).
pub const HEAP_HASNULL: u16 = 0x0001;
/// At least one column is variable-width.
pub const HEAP_HASVARWIDTH: u16 = 0x0002;
/// At least one column is stored out of line (TOAST).
pub const HEAP_HASEXTERNAL: u16 = 0x0004;
/// Hint: `t_xmin` is known committed.
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
/// Hint: `t_xmin` is known invalid (aborted).
pub const HEAP_XMIN_INVALID: u16 = 0x0200;
/// Hint: `t_xmax` is known committed.
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400;
/// Hint: `t_xmax` is known invalid (no live deletion).
pub const HEAP_XMAX_INVALID: u16 = 0x0800;
/// Tuple is an updated version.
pub const HEAP_UPDATED: u16 = 0x2000;
/// `t_xmax` holds a row LOCK, not a delete (M2c Stage P, tech-selection
/// §9.1): set by `SELECT ... FOR UPDATE` / `HeapAM::lock_tuple`. The row
/// stays visible to every snapshot — visibility masks a LOCK_ONLY `t_xmax`
/// to INVALID — but the row-lock protocol treats the non-INVALID `t_xmax`
/// as "row locked" until the stamper's transaction ends. A real delete or
/// update by the lock holder clears the bit when it overwrites the stamp.
///
/// Bit 0x1000 sits between PG's `HEAP_XMAX_INVALID` (0x0800) and
/// `HEAP_UPDATED` (0x2000), mirroring where PG keeps its own
/// `HEAP_XMAX_LOCK_ONLY`; this crate's infomask layout is §三, not PG's, so
/// only the relative gap matters.
pub const HEAP_XMAX_LOCK_ONLY: u16 = 0x1000;
/// `t_xmax` holds a shared row lock (FOR SHARE, Stage S multixact lite,
/// tech-selection §9.1). Always set together with [`HEAP_XMAX_LOCK_ONLY`].
/// Distinguishes a shared lock from an exclusive one without a separate
/// multixact struct — full multixact is deferred to Phase 6.
pub const HEAP_XMAX_IS_SHARE: u16 = 0x4000;

// `t_infomask2` bits (§三): natts occupies bits 0..=10 (up to 2047 columns;
// 11 bits represent 0..=2047).
/// Mask extracting the column count from `t_infomask2` (bits 0..=10).
pub const HEAP_NATTS_MASK: u16 = 0x07FF;
/// Key columns were updated (blocks HOT).
pub const HEAP_KEYS_UPDATED: u16 = 0x2000;
/// Tuple is a HOT-updated version.
pub const HEAP_HOT_UPDATED: u16 = 0x4000;
/// Tuple is the only version in its chain.
pub const HEAP_ONLY_TUPLE: u16 = 0x8000;

// `t_flags` (§三): low 4 bits hold the tuple encoding version.
/// Mask for the tuple encoding version in `t_flags` (low 4 bits).
pub const TUPLE_VERSION_MASK: u16 = 0x000F;
/// Tuple encoding version for M2.
pub const TUPLE_VERSION_M2: u16 = 0;

/// Maximum number of columns. natts is stored in bits 0..=10 of
/// `t_infomask2` (mask [`HEAP_NATTS_MASK`] = `0x07FF`), so the largest
/// encodable count is 2047. A larger count would overflow into the flag bits
/// and read back as a different (corrupt) natts.
pub const MAX_NATTS: usize = 2047;

/// The 64-byte M2 tuple header (field order and offsets per §三).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleHeader {
    /// Inserting transaction (offset 0..8).
    pub t_xmin: TxnId,
    /// Deleting transaction, or [`TxnId::INVALID`] (offset 8..16).
    pub t_xmax: TxnId,
    /// Agent identity that wrote this row (offset 16..24).
    pub t_agent_id: u64,
    /// Trace id at write time; all-zero means untraced (offset 24..40).
    pub t_trace_id: [u8; 16],
    /// Self-reference (or next version in an update chain); encoded as 12
    /// bytes with a zeroed pad u16 (offset 40..52).
    pub t_ctid: Tid,
    /// Infomask bits (offset 52..54).
    pub t_infomask: u16,
    /// Infomask2: natts in bits 0..=10 plus flag bits (offset 54..56).
    pub t_infomask2: u16,
    /// Offset of attribute data from the tuple start (offset 56..58).
    pub t_hoff: u16,
    /// Tuple flags; low 4 bits = encoding version (offset 58..60).
    pub t_flags: u16,
    /// Command id (offset 60..64).
    pub t_cid: u32,
}

impl TupleHeader {
    /// A header with caller-supplied identity fields; `encode_tuple` fills in
    /// `t_infomask`, `t_infomask2`, `t_hoff` and `t_flags`.
    pub fn new(
        t_xmin: TxnId,
        t_xmax: TxnId,
        t_agent_id: u64,
        t_trace_id: [u8; 16],
        t_ctid: Tid,
        t_cid: u32,
    ) -> Self {
        TupleHeader {
            t_xmin,
            t_xmax,
            t_agent_id,
            t_trace_id,
            t_ctid,
            t_infomask: 0,
            t_infomask2: 0,
            t_hoff: 0,
            t_flags: 0,
            t_cid,
        }
    }

    /// Number of columns, from `t_infomask2` bits 0..=10.
    pub fn natts(&self) -> usize {
        (self.t_infomask2 & HEAP_NATTS_MASK) as usize
    }

    /// Write the header into `buf[..64]` using the §三 offsets. The `t_ctid`
    /// pad u16 (offset 50..52) is always written as zero.
    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= TUPLE_HEADER_SIZE);
        buf[0..8].copy_from_slice(&self.t_xmin.0.to_le_bytes());
        buf[8..16].copy_from_slice(&self.t_xmax.0.to_le_bytes());
        buf[16..24].copy_from_slice(&self.t_agent_id.to_le_bytes());
        buf[24..40].copy_from_slice(&self.t_trace_id);
        buf[40..48].copy_from_slice(&self.t_ctid.page_id.0.to_le_bytes());
        buf[48..50].copy_from_slice(&self.t_ctid.slot_id.to_le_bytes());
        buf[50..52].copy_from_slice(&0u16.to_le_bytes()); // t_ctid pad, always 0
        buf[52..54].copy_from_slice(&self.t_infomask.to_le_bytes());
        buf[54..56].copy_from_slice(&self.t_infomask2.to_le_bytes());
        buf[56..58].copy_from_slice(&self.t_hoff.to_le_bytes());
        buf[58..60].copy_from_slice(&self.t_flags.to_le_bytes());
        buf[60..64].copy_from_slice(&self.t_cid.to_le_bytes());
    }

    /// Decode the header from `bytes[..64]`. Errors on short input.
    pub fn read_from(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < TUPLE_HEADER_SIZE {
            return Err(HeapError::Corrupted(format!(
                "tuple shorter than header: {} bytes",
                bytes.len()
            )));
        }
        let u16_at = |off: usize| u16::from_le_bytes(bytes[off..off + 2].try_into().unwrap());
        let pad = u16_at(50);
        if pad != 0 {
            return Err(HeapError::Corrupted(format!(
                "t_ctid pad u16 must be 0, got {pad:#06x}"
            )));
        }
        Ok(TupleHeader {
            t_xmin: TxnId(u64::from_le_bytes(bytes[0..8].try_into().unwrap())),
            t_xmax: TxnId(u64::from_le_bytes(bytes[8..16].try_into().unwrap())),
            t_agent_id: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            t_trace_id: bytes[24..40].try_into().unwrap(),
            t_ctid: Tid {
                page_id: PageId(u64::from_le_bytes(bytes[40..48].try_into().unwrap())),
                slot_id: u16_at(48),
            },
            t_infomask: u16_at(52),
            t_infomask2: u16_at(54),
            t_hoff: u16_at(56),
            t_flags: u16_at(58),
            t_cid: u32::from_le_bytes(bytes[60..64].try_into().unwrap()),
        })
    }
}

/// Maximum dimension of an inline vector column (M6 tech-selection §11).
/// 2000 keeps a single-column vector tuple inside an 8 KiB page:
/// 64-byte header + 2000 × 4 = 8064 B ≤ `MAX_TUPLE_BYTES` (8140 B on 8 KiB
/// pages). Larger dimensions belong to the M5 O1 out-of-line window, not
/// the inline tuple format.
pub const MAX_VECTOR_DIM: u32 = 2000;

/// Column type descriptor needed to split the attribute area at decode time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// 4-byte integer.
    Int4,
    /// 8-byte integer.
    Int8,
    /// 8-byte timestamp (microseconds since epoch).
    Timestamptz,
    /// 16-byte UUID.
    Uuid,
    /// Variable-length UTF-8 text.
    Text,
    /// Variable-length binary.
    Bytea,
    /// Fixed-width vector of `f32` (M6 Stage 0): `dim × 4` bytes inline,
    /// little-endian per component. `dim` is part of the type identity —
    /// two `Vector` columns with different dims are different types, which
    /// makes `encode_tuple`'s datum/column match reject a wrong-length
    /// `Datum::Vector` without a separate check. Valid range is
    /// `1..=MAX_VECTOR_DIM`; use [`ColumnType::vector`] to construct.
    Vector(u32),
}

impl ColumnType {
    /// Validated constructor for vector columns: `dim` must lie in
    /// `1..=MAX_VECTOR_DIM` (M6 tech-selection §11's hard limit; oversized
    /// dims are the M5 O1 window's concern, not the inline format's).
    /// `ColumnType` is a plain enum, so the codec re-validates the range at
    /// the encode/decode boundary — this constructor is the front door
    /// (Stage C's `VECTOR(n)` DDL parsing), not the only check.
    pub fn vector(dim: u32) -> Result<Self> {
        check_vector_dim(dim)?;
        Ok(ColumnType::Vector(dim))
    }

    /// Fixed width in bytes, or `None` for varlena columns.
    fn fixed_width(self) -> Option<usize> {
        match self {
            ColumnType::Int4 => Some(4),
            ColumnType::Int8 | ColumnType::Timestamptz => Some(8),
            ColumnType::Uuid => Some(16),
            ColumnType::Text | ColumnType::Bytea => None,
            ColumnType::Vector(dim) => Some(dim as usize * 4),
        }
    }
}

/// Reject a vector dimension outside `1..=MAX_VECTOR_DIM`.
fn check_vector_dim(dim: u32) -> Result<()> {
    if !(1..=MAX_VECTOR_DIM).contains(&dim) {
        return Err(HeapError::InvalidArgument(format!(
            "vector dimension {dim} out of range 1..={MAX_VECTOR_DIM}"
        )));
    }
    Ok(())
}

/// A single column value. `None` at the `values` level encodes SQL NULL.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    /// 4-byte integer.
    Int4(i32),
    /// 8-byte integer.
    Int8(i64),
    /// 8-byte timestamp (microseconds since epoch).
    Timestamptz(i64),
    /// 16-byte UUID.
    Uuid(uuid::Uuid),
    /// Variable-length UTF-8 text.
    Text(String),
    /// Variable-length binary.
    Bytea(Vec<u8>),
    /// Fixed-width `f32` vector (M6 Stage 0); the length must equal the
    /// column's `ColumnType::Vector(dim)` — enforced by the encode-side
    /// datum/column match.
    Vector(Vec<f32>),
    /// Out-of-line (TOASTed) value: the 20-byte pointer stored in the main
    /// tuple (§四). Resolving it is Stage I's job.
    External(ToastPointer),
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Datum::Int4(a), Datum::Int4(b)) => Some(a.cmp(b)),
            (Datum::Int8(a), Datum::Int8(b)) => Some(a.cmp(b)),
            (Datum::Timestamptz(a), Datum::Timestamptz(b)) => Some(a.cmp(b)),
            (Datum::Uuid(a), Datum::Uuid(b)) => Some(a.cmp(b)),
            (Datum::Text(a), Datum::Text(b)) => Some(a.cmp(b)),
            (Datum::Bytea(a), Datum::Bytea(b)) => Some(a.cmp(b)),
            // Cross-type comparisons return None: never triggered when
            // comparing values of ONE column (they share a variant by
            // construction); the engine's ORDER BY falls back to `Equal`
            // via `unwrap_or(Equal)`, so a type mix-up sorts silently
            // instead of failing — acceptable for the M2b subset, which
            // never compares across column types.
            _ => None,
        }
    }
}

impl Datum {
    /// The column type this datum is encoded for.
    fn column_type(&self) -> ColumnType {
        match self {
            Datum::Int4(_) => ColumnType::Int4,
            Datum::Int8(_) => ColumnType::Int8,
            Datum::Timestamptz(_) => ColumnType::Timestamptz,
            Datum::Uuid(_) => ColumnType::Uuid,
            Datum::Text(_) => ColumnType::Text,
            Datum::Bytea(_) => ColumnType::Bytea,
            Datum::Vector(v) => ColumnType::Vector(v.len() as u32),
            // An external pointer stands in for a varlena column; the schema
            // (Text or Bytea) decides how the detoasted bytes are read.
            Datum::External(_) => ColumnType::Bytea,
        }
    }
}

/// Encode a tuple: 64-byte header + optional null bitmap + attribute data.
///
/// `header` carries the identity fields; this function fills in
/// `t_infomask` (HASNULL/HASVARWIDTH/HASEXTERNAL), `t_infomask2` natts,
/// `t_hoff` (64 + bitmap, aligned up to 8) and `t_flags` (version M2 = 0),
/// then appends the null bitmap and column data. Varlena values are written
/// as a 4-byte inline varlena header (total length, tag bits 00) plus
/// payload; [`Datum::External`] writes its 20-byte TOAST pointer as-is.
pub fn encode_tuple(
    mut header: TupleHeader,
    columns: &[ColumnType],
    values: &[Option<Datum>],
) -> Result<Vec<u8>> {
    if columns.len() != values.len() {
        return Err(HeapError::InvalidArgument(format!(
            "schema has {} columns but {} values given",
            columns.len(),
            values.len()
        )));
    }
    let natts = columns.len();
    if natts > MAX_NATTS {
        return Err(HeapError::InvalidArgument(format!(
            "too many columns: {natts} > {MAX_NATTS}"
        )));
    }

    let mut has_null = false;
    let mut has_varwidth = false;
    let mut has_external = false;
    for (col, val) in columns.iter().zip(values.iter()) {
        // A Vector schema column carries its own validity domain. The
        // `ColumnType::vector` constructor enforces it, but the enum is
        // public and catalog read-back constructs variants directly —
        // re-validate at the codec boundary regardless of NULL-ness.
        if let ColumnType::Vector(dim) = col {
            check_vector_dim(*dim)?;
        }
        match val {
            None => has_null = true,
            Some(datum) => {
                // External pointers stand in for either varlena type; a
                // vector datum must match the column's dim exactly.
                let matches = match (col, datum) {
                    (_, Datum::External(_)) => col.fixed_width().is_none(),
                    (ColumnType::Vector(dim), Datum::Vector(v)) => v.len() == *dim as usize,
                    (col, datum) => *col == datum.column_type(),
                };
                if !matches {
                    return Err(HeapError::InvalidArgument(format!(
                        "datum {datum:?} does not match column type {col:?}"
                    )));
                }
                if col.fixed_width().is_none() {
                    has_varwidth = true;
                }
                if matches!(datum, Datum::External(_)) {
                    has_external = true;
                }
            }
        }
    }

    let bitmap_len = if has_null { natts.div_ceil(8) } else { 0 };
    let t_hoff = (TUPLE_HEADER_SIZE + bitmap_len).next_multiple_of(8);

    // Recompute the structural bits; caller-set hint bits (XMIN_COMMITTED,
    // UPDATED, ...) are preserved.
    header.t_infomask = (header.t_infomask & !(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL))
        | (if has_null { HEAP_HASNULL } else { 0 })
        | (if has_varwidth { HEAP_HASVARWIDTH } else { 0 })
        | (if has_external { HEAP_HASEXTERNAL } else { 0 });
    header.t_infomask2 = (header.t_infomask2 & !HEAP_NATTS_MASK) | natts as u16;
    header.t_hoff = t_hoff as u16;
    header.t_flags = (header.t_flags & !TUPLE_VERSION_MASK) | TUPLE_VERSION_M2;

    // Worst case: header + bitmap + 4-byte header per varlena + payload.
    let mut out = Vec::with_capacity(t_hoff + 256);
    out.resize(TUPLE_HEADER_SIZE, 0);
    header.write_to(&mut out);
    out.resize(t_hoff, 0);

    if has_null {
        // Null bitmap semantics: bit i set means column i IS NULL. This is
        // the inverse of PostgreSQL's convention (PG stores not-null bits);
        // it is self-consistent within this format and the two formats are
        // not dump-compatible anyway (64B vs 23B headers).
        let bitmap = &mut out[TUPLE_HEADER_SIZE..TUPLE_HEADER_SIZE + bitmap_len];
        for (i, val) in values.iter().enumerate() {
            if val.is_none() {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
    }

    for val in values.iter().flatten() {
        match val {
            Datum::Int4(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::Int8(v) | Datum::Timestamptz(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::Uuid(v) => out.extend_from_slice(v.as_bytes()),
            Datum::Text(s) => write_inline_varlena(&mut out, s.as_bytes())?,
            Datum::Bytea(b) => write_inline_varlena(&mut out, b)?,
            Datum::Vector(v) => {
                // Bit-exact little-endian per component; the length was
                // pinned to the column dim by the match above.
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Datum::External(ptr) => out.extend_from_slice(&ptr.encode()),
        }
    }
    Ok(out)
}

/// Append an inline varlena value: 4-byte total-length header (tag bits 00)
/// plus payload.
///
/// Hard-fails if the payload is large enough to collide with the 2 tag bits
/// (>= 2^30 - 4 bytes). Unreachable through the slotted page (max tuple is
/// under 16 KiB), but this is a public API and the failure must not be
/// silent in release builds.
fn write_inline_varlena(out: &mut Vec<u8>, payload: &[u8]) -> Result<()> {
    // Bound-check in usize *before* narrowing to u32: `payload.len() + 4`
    // could exceed u32::MAX and the `as u32` cast would silently truncate,
    // letting an oversized value pass the tag-bit check. Unreachable via the
    // slotted page (max tuple < 16 KiB) but encode_tuple is a public API.
    if payload.len() + 4 > VARLENA_LEN_MASK as usize {
        return Err(HeapError::InvalidArgument(format!(
            "varlena payload too large: {} bytes",
            payload.len()
        )));
    }
    let total = (payload.len() + 4) as u32;
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// Decode a tuple into its header and column values.
///
/// `columns` describes the schema so the attribute area can be split;
/// it must match the natts recorded in `t_infomask2`.
pub fn decode_tuple(
    bytes: &[u8],
    columns: &[ColumnType],
) -> Result<(TupleHeader, Vec<Option<Datum>>)> {
    let header = TupleHeader::read_from(bytes)?;
    if header.natts() != columns.len() {
        return Err(HeapError::Corrupted(format!(
            "tuple has {} columns, schema describes {}",
            header.natts(),
            columns.len()
        )));
    }
    // The schema is disk-derived (catalog read-back) in production, so a
    // Vector dim outside the constructor's domain is corruption, not a
    // caller bug — fail loud before any read is sized by it.
    for col in columns {
        if let ColumnType::Vector(dim) = col {
            if check_vector_dim(*dim).is_err() {
                return Err(HeapError::Corrupted(format!(
                    "vector column dimension {dim} out of range 1..={MAX_VECTOR_DIM}"
                )));
            }
        }
    }
    let t_hoff = header.t_hoff as usize;
    if t_hoff < TUPLE_HEADER_SIZE || t_hoff > bytes.len() || t_hoff % 8 != 0 {
        return Err(HeapError::Corrupted(format!(
            "t_hoff {t_hoff} out of range or not 8-byte aligned (tuple is {} bytes)",
            bytes.len()
        )));
    }

    let has_null = header.t_infomask & HEAP_HASNULL != 0;
    let bitmap_len = if has_null {
        columns.len().div_ceil(8)
    } else {
        0
    };
    if has_null && TUPLE_HEADER_SIZE + bitmap_len > t_hoff {
        return Err(HeapError::Corrupted(
            "null bitmap extends past t_hoff".to_string(),
        ));
    }
    let is_null = |i: usize| has_null && bytes[TUPLE_HEADER_SIZE + i / 8] & (1 << (i % 8)) != 0;

    let mut values = Vec::with_capacity(columns.len());
    let mut pos = t_hoff;
    for (i, col) in columns.iter().enumerate() {
        if is_null(i) {
            values.push(None);
            continue;
        }
        let datum = match col.fixed_width() {
            Some(width) => {
                let field = take(bytes, &mut pos, width)?;
                match col {
                    ColumnType::Int4 => Datum::Int4(i32::from_le_bytes(field.try_into().unwrap())),
                    ColumnType::Int8 => Datum::Int8(i64::from_le_bytes(field.try_into().unwrap())),
                    ColumnType::Timestamptz => {
                        Datum::Timestamptz(i64::from_le_bytes(field.try_into().unwrap()))
                    }
                    ColumnType::Uuid => {
                        Datum::Uuid(uuid::Uuid::from_bytes(field.try_into().unwrap()))
                    }
                    ColumnType::Vector(dim) => {
                        // `field` is exactly `dim × 4` bytes (the fixed
                        // width); dim was range-checked above, so this
                        // allocation is bounded by MAX_VECTOR_DIM.
                        let mut v = Vec::with_capacity(*dim as usize);
                        for chunk in field.chunks_exact(4) {
                            v.push(f32::from_le_bytes(chunk.try_into().unwrap()));
                        }
                        Datum::Vector(v)
                    }
                    ColumnType::Text | ColumnType::Bytea => unreachable!(),
                }
            }
            None => {
                let vl_len = u32::from_le_bytes(take(bytes, &mut pos, 4)?.try_into().unwrap());
                if is_external(vl_len) {
                    // The 4 tag/length bytes are the pointer's own vl_len_.
                    pos -= 4;
                    let raw = take(bytes, &mut pos, TOAST_POINTER_SIZE)?;
                    Datum::External(ToastPointer::decode(raw)?)
                } else {
                    let total = (vl_len & VARLENA_LEN_MASK) as usize;
                    if total < 4 {
                        return Err(HeapError::Corrupted(format!(
                            "varlena total length {total} < header size"
                        )));
                    }
                    let payload = take(bytes, &mut pos, total - 4)?;
                    match col {
                        ColumnType::Text => {
                            Datum::Text(String::from_utf8(payload.to_vec()).map_err(|_| {
                                HeapError::Corrupted("text column is not valid UTF-8".to_string())
                            })?)
                        }
                        ColumnType::Bytea => Datum::Bytea(payload.to_vec()),
                        _ => unreachable!(),
                    }
                }
            }
        };
        values.push(Some(datum));
    }
    if pos != bytes.len() {
        return Err(HeapError::Corrupted(format!(
            "tuple has {} trailing bytes after the last column",
            bytes.len() - pos
        )));
    }
    Ok((header, values))
}

/// Copy `n` bytes out of `bytes` at `pos`, advancing `pos`.
fn take<'a>(bytes: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    if *pos + n > bytes.len() {
        return Err(HeapError::Corrupted(format!(
            "tuple truncated: need {n} bytes at {pos}, have {}",
            bytes.len()
        )));
    }
    let out = &bytes[*pos..*pos + n];
    *pos += n;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> TupleHeader {
        TupleHeader::new(
            TxnId(7),
            TxnId::INVALID,
            99,
            [0xAB; 16],
            Tid {
                page_id: PageId(3),
                slot_id: 2,
            },
            0,
        )
    }

    #[test]
    fn header_write_read_round_trip() {
        let mut header = sample_header();
        header.t_infomask = HEAP_HASNULL | HEAP_XMIN_COMMITTED;
        header.t_infomask2 = 3 | HEAP_ONLY_TUPLE;
        header.t_hoff = 72;
        let mut buf = [0u8; TUPLE_HEADER_SIZE];
        header.write_to(&mut buf);
        assert_eq!(TupleHeader::read_from(&buf).unwrap(), header);
    }

    #[test]
    fn encode_decode_fixed_width_round_trip() {
        let columns = [ColumnType::Int4, ColumnType::Int8, ColumnType::Uuid];
        let values = vec![
            Some(Datum::Int4(-42)),
            Some(Datum::Int8(1 << 40)),
            Some(Datum::Uuid(uuid::Uuid::from_bytes([7; 16]))),
        ];
        let bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
        // No nulls: t_hoff = 64 exactly, HASNULL clear.
        assert_eq!(bytes[56], 64);
        assert_eq!(
            u16::from_le_bytes(bytes[52..54].try_into().unwrap()) & HEAP_HASNULL,
            0
        );
        let (header, decoded) = decode_tuple(&bytes, &columns).unwrap();
        assert_eq!(header.t_xmin, TxnId(7));
        assert_eq!(decoded, values);
    }

    #[test]
    fn encode_decode_varlena_and_nulls_round_trip() {
        let columns = [
            ColumnType::Int8,
            ColumnType::Text,
            ColumnType::Bytea,
            ColumnType::Int4,
        ];
        let values = vec![
            Some(Datum::Int8(5)),
            None,
            Some(Datum::Bytea(vec![0xDE, 0xAD])),
            None,
        ];
        let bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
        let (header, decoded) = decode_tuple(&bytes, &columns).unwrap();
        assert_ne!(header.t_infomask & HEAP_HASNULL, 0);
        assert_ne!(header.t_infomask & HEAP_HASVARWIDTH, 0);
        // bitmap: 4 natts → 1 byte; t_hoff = align8(64 + 1) = 72.
        assert_eq!(header.t_hoff, 72);
        assert_eq!(decoded, values);
    }

    #[test]
    fn type_mismatch_rejected() {
        let columns = [ColumnType::Int4];
        let values = vec![Some(Datum::Int8(1))];
        assert!(encode_tuple(sample_header(), &columns, &values).is_err());
    }

    #[test]
    fn decode_rejects_truncated_tuple() {
        let columns = [ColumnType::Int8];
        let values = vec![Some(Datum::Int8(5))];
        let bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
        assert!(decode_tuple(&bytes[..bytes.len() - 1], &columns).is_err());
    }

    #[test]
    fn decode_rejects_unaligned_t_hoff() {
        let columns = [ColumnType::Int8];
        let values = vec![Some(Datum::Int8(5))];
        let mut bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
        // Corrupt t_hoff (offset 56..58) to a non-8-aligned value.
        bytes[56..58].copy_from_slice(&65u16.to_le_bytes());
        assert!(matches!(
            decode_tuple(&bytes, &columns),
            Err(HeapError::Corrupted(_))
        ));
    }

    #[test]
    fn natts_bitfield_boundary() {
        // MAX_NATTS columns must encode and round-trip: natts is read back
        // exactly. All-NULL keeps the tuple to header + bitmap (no payload).
        let columns = vec![ColumnType::Int4; MAX_NATTS];
        let values = vec![None; MAX_NATTS];
        let bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
        let (header, decoded) = decode_tuple(&bytes, &columns).unwrap();
        assert_eq!(header.natts(), MAX_NATTS);
        assert_eq!(decoded.len(), MAX_NATTS);

        // MAX_NATTS + 1 does not fit in HEAP_NATTS_MASK; encode must reject it
        // rather than overflow the natts bitfield into the flag bits.
        let too_many = MAX_NATTS + 1;
        let columns = vec![ColumnType::Int4; too_many];
        let values = vec![None; too_many];
        assert!(matches!(
            encode_tuple(sample_header(), &columns, &values),
            Err(HeapError::InvalidArgument(_))
        ));
    }

    // -----------------------------------------------------------------
    // M6 Stage 0: ColumnType::Vector / Datum::Vector (tech-selection §11).
    // -----------------------------------------------------------------

    /// Mixed-schema round-trip with exact f32 bit patterns (every value is
    /// exactly representable, so PartialEq is bit equality here), plus a
    /// NULL vector column to exercise the bitmap interplay.
    #[test]
    fn vector_round_trip_mixed_and_null() {
        let columns = [
            ColumnType::Int4,
            ColumnType::vector(3).unwrap(),
            ColumnType::Text,
            ColumnType::vector(2).unwrap(),
        ];
        let values = vec![
            Some(Datum::Int4(-7)),
            Some(Datum::Vector(vec![1.5, -2.25, 3.75e-8])),
            Some(Datum::Text("v".to_string())),
            None,
        ];
        let bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
        let (header, decoded) = decode_tuple(&bytes, &columns).unwrap();
        assert_eq!(decoded, values);
        // Both vectors are fixed-width: no varwidth bit for them; the Text
        // column sets it.
        assert_ne!(header.t_infomask & HEAP_HASVARWIDTH, 0);
        assert_ne!(header.t_infomask & HEAP_HASNULL, 0);
    }

    /// Boundary dims: 1 and MAX_VECTOR_DIM round-trip; 0 and
    /// MAX_VECTOR_DIM + 1 are rejected by the constructor. The dim-2000
    /// tuple is 64 + 8000 = 8064 bytes — under the 8 KiB page's
    /// MAX_TUPLE_BYTES (8140), so the format cap is page-coherent.
    #[test]
    fn vector_boundary_dims() {
        for dim in [1u32, MAX_VECTOR_DIM] {
            let columns = [ColumnType::vector(dim).unwrap()];
            let values = vec![Some(Datum::Vector(vec![0.5; dim as usize]))];
            let bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
            assert_eq!(bytes.len(), 64 + 4 * dim as usize);
            let (_, decoded) = decode_tuple(&bytes, &columns).unwrap();
            assert_eq!(decoded, values);
        }
        assert!(matches!(
            ColumnType::vector(0),
            Err(HeapError::InvalidArgument(_))
        ));
        assert!(matches!(
            ColumnType::vector(MAX_VECTOR_DIM + 1),
            Err(HeapError::InvalidArgument(_))
        ));
    }

    /// A datum whose length disagrees with the column dim is rejected, in
    /// both directions (dim is part of the type identity).
    #[test]
    fn vector_dim_mismatch_rejected() {
        let short = vec![Some(Datum::Vector(vec![1.0, 2.0]))];
        assert!(matches!(
            encode_tuple(sample_header(), &[ColumnType::vector(3).unwrap()], &short),
            Err(HeapError::InvalidArgument(_))
        ));
        let long = vec![Some(Datum::Vector(vec![1.0, 2.0, 3.0, 4.0]))];
        assert!(matches!(
            encode_tuple(sample_header(), &[ColumnType::vector(3).unwrap()], &long),
            Err(HeapError::InvalidArgument(_))
        ));
        // A vector datum against a non-vector column is an ordinary type
        // mismatch.
        assert!(matches!(
            encode_tuple(sample_header(), &[ColumnType::Text], &short),
            Err(HeapError::InvalidArgument(_))
        ));
    }

    /// The enum is public, so the codec boundary re-validates the dim even
    /// when the constructor was bypassed: encode fails as InvalidArgument
    /// (caller bug), decode as Corrupted (disk-derived schema).
    #[test]
    fn vector_schema_dim_validated_at_codec() {
        let bad = ColumnType::Vector(MAX_VECTOR_DIM + 1);
        let values = vec![Some(Datum::Vector(vec![1.0; MAX_VECTOR_DIM as usize + 1]))];
        assert!(matches!(
            encode_tuple(sample_header(), &[bad], &values),
            Err(HeapError::InvalidArgument(_))
        ));
        // Encode a valid Int4 tuple, then decode it against a corrupt
        // vector schema (natts matches — the dim check must fire first).
        let bytes = encode_tuple(
            sample_header(),
            &[ColumnType::Int4],
            &[Some(Datum::Int4(1))],
        )
        .unwrap();
        assert!(matches!(
            decode_tuple(&bytes, &[bad]),
            Err(HeapError::Corrupted(_))
        ));
        assert!(matches!(
            decode_tuple(&bytes, &[ColumnType::Vector(0)]),
            Err(HeapError::Corrupted(_))
        ));
    }

    /// A tuple truncated inside the vector payload fails loudly.
    #[test]
    fn vector_decode_rejects_truncated() {
        let columns = [ColumnType::vector(3).unwrap()];
        let values = vec![Some(Datum::Vector(vec![1.0, 2.0, 3.0]))];
        let bytes = encode_tuple(sample_header(), &columns, &values).unwrap();
        assert!(matches!(
            decode_tuple(&bytes[..bytes.len() - 1], &columns),
            Err(HeapError::Corrupted(_))
        ));
    }
}
