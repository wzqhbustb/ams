//! WAL record format, types, and (de)serialization.
//!
//! A WAL record consists of a 32-byte fixed header (24 B header + 8 B meta),
//! followed by a variable-length payload and 0-7 bytes of padding so that the
//! total record length is a multiple of 8 bytes.

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};
use crate::types::{align_up, Lsn, PageId, Tid, TxnId};

/// Size of the fixed record header in bytes.
pub const WAL_RECORD_HEADER_SIZE: usize = 32;

/// WAL record type with explicit discriminants for on-disk compatibility.
///
/// Discriminants are part of the on-disk format and must never be renumbered.
/// Values marked "reserved" have no producer or replay logic yet; recovery
/// fails them as unknown until the corresponding stage registers a handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WalRecordType {
    /// Heap insert (M2 logic; value reserved).
    HeapInsert = 1,
    /// Heap update (M2 logic; value reserved).
    HeapUpdate = 2,
    /// Heap delete (M2 logic; value reserved).
    HeapDelete = 3,
    /// B+Tree insert (M2 logic; value reserved).
    BTreeInsert = 4,
    /// B+Tree split prepare (M2 logic; value reserved). Renamed from M1's
    /// `BTreeSplit`; the discriminant is unchanged (tech-selection v2.3-8).
    BTreeSplitPrepare = 5,
    /// B+Tree delete (M2 logic; value reserved).
    BTreeDelete = 6,
    /// Heap HOT update (M2 logic; value reserved).
    HeapHotUpdate = 7,
    /// Heap cleanup: vacuum page compaction + optional chain unlink (M3
    /// Stage B implements; the discriminant itself is the Stage-0-reserved
    /// value, not a new assignment).
    HeapCleanup = 8,

    /// Full page image written before the first modification of a page after a
    /// checkpoint (M1 implements).
    FullPageImage = 10,

    /// Transaction begin (M2 logic; value reserved).
    TxnBegin = 20,
    /// Transaction commit (M2 logic; value reserved).
    TxnCommit = 21,
    /// Transaction abort (M2 logic; value reserved).
    TxnAbort = 22,

    /// Checkpoint start marker (M1 implements).
    CheckpointBegin = 30,
    /// Checkpoint end marker (M1 implements).
    CheckpointEnd = 31,

    /// Page allocation (M1 implements).
    PageAlloc = 40,
    /// Page free (M2 Stage E implements).
    PageFree = 41,

    /// B+Tree split compensation log record (M2c undo; value reserved).
    BTreeSplitCLR = 50,
    /// B+Tree split copy: redo recomputes the moved content from the left
    /// page (M2 logic; value reserved).
    BTreeSplitCopy = 51,
    /// B+Tree split commit (M2 logic; value reserved).
    BTreeSplitCommit = 52,

    /// Logical HNSW operation (Phase 2+).
    LogicalHnsw = 100,
    /// Logical inverted-index operation (Phase 2+).
    LogicalInverted = 101,
    /// Logical graph operation (Phase 2+).
    LogicalGraph = 102,
    /// Logical time-series operation (Phase 2+).
    LogicalTimeSeries = 103,

    /// Segment seal operation (Phase 3+; reserved at Stage 0, the M1+M2 baseline).
    ///
    /// Payload contract (M3 Stage G, tech-selection §8 — no payload struct or
    /// redo handler yet; recovery hard-fails on this discriminant until one
    /// is registered): exactly one [`crate::segment::SegmentId`] — the
    /// segment being sealed. bincode-serialized like all M1–M3 payloads.
    SegmentSeal = 110,
    /// Segment merge operation (Phase 3+; reserved at Stage 0, the M1+M2 baseline).
    ///
    /// Payload contract (M3 Stage G, tech-selection §8 — no payload struct or
    /// redo handler yet; recovery hard-fails on this discriminant until one
    /// is registered): the input [`crate::segment::SegmentId`] list (merge
    /// sources, in merge order) followed by the target `SegmentId` (the
    /// merge output). Redo retires the inputs and installs the target;
    /// the record must be sufficient to reconstruct that outcome
    /// idempotently.
    SegmentMerge = 111,

    /// HNSW node initialization (Phase 2 M5 Stage 0; tech-selection §4.2):
    /// creates a fixed-size node entry on a node page, state INITIALIZING.
    HnswNodeInit = 121,
    /// HNSW neighbor-list update (Phase 2 M5): in-place count+content rewrite
    /// of one level of one node entry; carries the owner `node_id` (v1.12 —
    /// the redo-side no-self-loop check's judgment basis).
    HnswSetNeighbors = 122,
    /// HNSW meta-page field post-image (Phase 2 M5): entry point / max level.
    HnswMetaUpdate = 123,
    /// HNSW node tombstone (Phase 2 M5): format only — the delete SEMANTICS
    /// land in M6 (tech-selection §1 scope split).
    HnswNodeTombstone = 124,
    /// HNSW directory append (Phase 2 M5): publishes `node_id → (page, slot)`
    /// at the directory tail page — the NodeId allocation point.
    HnswDirAppend = 125,
    /// HNSW directory chain link (Phase 2 M5): points the old tail page at
    /// the freshly allocated next directory page.
    HnswDirLink = 126,
    /// HNSW publish-live (Phase 2 M5): flips a fully connected node from
    /// INITIALIZING to LIVE (v1.7 — the LIVE flip gets its own record).
    HnswPublishLive = 127,
}

impl WalRecordType {
    /// Convert the enum to its on-disk `u8` discriminant.
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Parse a `u8` discriminant back into a `WalRecordType`.
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(WalRecordType::HeapInsert),
            2 => Ok(WalRecordType::HeapUpdate),
            3 => Ok(WalRecordType::HeapDelete),
            4 => Ok(WalRecordType::BTreeInsert),
            5 => Ok(WalRecordType::BTreeSplitPrepare),
            6 => Ok(WalRecordType::BTreeDelete),
            7 => Ok(WalRecordType::HeapHotUpdate),
            8 => Ok(WalRecordType::HeapCleanup),
            10 => Ok(WalRecordType::FullPageImage),
            20 => Ok(WalRecordType::TxnBegin),
            21 => Ok(WalRecordType::TxnCommit),
            22 => Ok(WalRecordType::TxnAbort),
            30 => Ok(WalRecordType::CheckpointBegin),
            31 => Ok(WalRecordType::CheckpointEnd),
            40 => Ok(WalRecordType::PageAlloc),
            41 => Ok(WalRecordType::PageFree),
            50 => Ok(WalRecordType::BTreeSplitCLR),
            51 => Ok(WalRecordType::BTreeSplitCopy),
            52 => Ok(WalRecordType::BTreeSplitCommit),
            100 => Ok(WalRecordType::LogicalHnsw),
            101 => Ok(WalRecordType::LogicalInverted),
            102 => Ok(WalRecordType::LogicalGraph),
            103 => Ok(WalRecordType::LogicalTimeSeries),
            110 => Ok(WalRecordType::SegmentSeal),
            111 => Ok(WalRecordType::SegmentMerge),
            121 => Ok(WalRecordType::HnswNodeInit),
            122 => Ok(WalRecordType::HnswSetNeighbors),
            123 => Ok(WalRecordType::HnswMetaUpdate),
            124 => Ok(WalRecordType::HnswNodeTombstone),
            125 => Ok(WalRecordType::HnswDirAppend),
            126 => Ok(WalRecordType::HnswDirLink),
            127 => Ok(WalRecordType::HnswPublishLive),
            _ => Err(StorageError::WalReadFailed(format!(
                "unknown WAL record type discriminant {v}"
            ))),
        }
    }
}

/// Payload for a `PageAlloc` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageAllocRecord {
    /// The page that was allocated.
    pub page_id: PageId,
}

/// Payload for a `PageFree` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageFreeRecord {
    /// The page that was freed.
    pub page_id: PageId,
}

/// Payload for a `FullPageImage` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullPageImageRecord {
    /// The page whose image is being stored.
    pub page_id: PageId,
    /// The raw page image.
    pub image: Vec<u8>,
}

/// Payload for a `HeapInsert` record: a single tuple placed at a slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeapInsertRecord {
    /// The page the tuple was inserted into.
    pub page_id: PageId,
    /// The slot the tuple occupies.
    pub slot_id: u16,
    /// The encoded tuple bytes (header + null bitmap + attributes).
    pub tuple_bytes: Vec<u8>,
}

/// Payload for a `HeapUpdate` record: delete-old + insert-new in one record.
///
/// M2a has no in-place update; an update marks the old version deleted
/// (`xmax_old` on `old_tid`) and inserts a new version at `new_tid`. Redo
/// touches the old page then the new page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeapUpdateRecord {
    /// TID of the row version being superseded.
    pub old_tid: Tid,
    /// TID where the new version is written.
    pub new_tid: Tid,
    /// The `t_xmax` stamped onto the old version.
    pub xmax_old: TxnId,
    /// The encoded bytes of the new version.
    pub new_tuple_bytes: Vec<u8>,
}

/// Payload for a `HeapDelete` record: a logical delete stamping `t_xmax`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeapDeleteRecord {
    /// TID of the row being deleted.
    pub tid: Tid,
    /// The `t_xmax` stamped onto the deleted tuple.
    pub xmax: TxnId,
}

/// Payload for a `HeapHotUpdate` record: a page-local HOT update (Stage S).
///
/// The old tuple is stamped deleted (`xmax` + `HEAP_UPDATED` + `t_ctid` →
/// new version + `HEAP_HOT_UPDATED`) and the new version is inserted at
/// `new_slot` on the same page (carrying `HEAP_ONLY_TUPLE`). No index
/// maintenance occurs — the key columns are unchanged, so the B+Tree still
/// points to the old TID, and scans follow the `t_ctid` chain to the new
/// version. Redo is idempotent via `page.pd_lsn >= record.lsn`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeapHotUpdateRecord {
    /// Page containing both old and new versions (always same page for HOT).
    pub page_id: PageId,
    /// Slot of the old version (stamped deleted + t_ctid chain).
    pub old_slot: u16,
    /// Slot where the new version is inserted.
    pub new_slot: u16,
    /// The encoded bytes of the new version (HEAP_ONLY_TUPLE already set).
    pub new_tuple_bytes: Vec<u8>,
    /// The `t_xmax` stamped onto the old version.
    pub xmax: TxnId,
}

/// Payload for a `HeapCleanup` record (M3 Stage B, tech-selection §4.5):
/// physical compaction of one heap page plus an optional page-chain unlink.
///
/// Redo calls the SAME `SlottedPage::compact` (pg-am-heap) the online path
/// uses, with the same arguments — replay convergence is "replay =
/// re-execute the same physical operation" (§4.5 重放收敛性), never a
/// parallel reimplementation. `dead_slots` is therefore written in ascending
/// order: identical input yields identical output on both sides.
///
/// # Field-order invariant
///
/// `dead_slots` is deliberately the LAST field (same contract as
/// [`BTreeSplitCLRRecord::separator_key`], post-Stage-S fix B5): the analysis
/// phase prefix-decodes only the fixed-size leading page ids and never
/// touches the variable-length tail — bincode's standard config imposes no
/// size limit, so a full decode there would trust a corrupt length prefix on
/// a CRC-valid record with an unbounded allocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeapCleanupRecord {
    /// The page being compacted (dead slots killed, live bytes defragmented).
    pub page_id: PageId,
    /// Chain unlink: the predecessor page whose `next_page` pointer is
    /// relinked when the compacted page became empty and was spliced out of
    /// the relation's page chain. `PageId::INVALID` when no unlink happened
    /// (compaction only — the only shape Stage B produces online; the unlink
    /// fields exist so Stage C's page reclamation can log under the same
    /// record type).
    pub unlink_prev_page: PageId,
    /// Relink target written into the predecessor's `next_page`: the unlinked
    /// page's own successor at unlink time (`PageId::INVALID` = the unlinked
    /// page was the chain tail). Meaningless when `unlink_prev_page` is
    /// `INVALID`.
    pub unlink_next_page: PageId,
    /// Dead slots killed on `page_id`, ascending (see the struct docs).
    /// LAST field — see the field-order invariant.
    pub dead_slots: Vec<u16>,
}

/// Maximum accepted length of a [`HeapCleanupRecord::dead_slots`] (defense in
/// depth, mirroring [`MAX_CLR_SEPARATOR_KEY_BYTES`]): a page can never hold
/// more line pointers than the tuple area fits 4-byte entries, so a decoded
/// kill list longer than that trusts a corrupt length prefix. pg-storage
/// cannot depend on pg-am-heap, so the bound is re-derived from
/// [`PAGE_SIZE`](crate::types::PAGE_SIZE) here: `(PAGE_SIZE - 32 header - 16
/// special) / 4 per LP`.
pub const MAX_HEAP_CLEANUP_SLOTS: usize = (crate::types::PAGE_SIZE - 32 - 16) / 4;

/// Payload for a `BTreeInsert` record: one index entry placed at a slot.
///
/// Used for leaf inserts, internal-page downlink inserts, and appends to an
/// index's meta page (tech-selection §13). `level`/`flags` describe the page
/// the entry belongs to; redo uses them only when it must initialize a fresh
/// (all-zero) page before applying the insert, so a page whose initializing
/// record is lost (e.g. a `PageAlloc` that outlived the page-content records)
/// still recovers with the correct `btpo_level`/`btpo_flags` (§13.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeInsertRecord {
    /// The page the entry was inserted into.
    pub page_id: PageId,
    /// The slot the entry occupies.
    pub slot_id: u16,
    /// `btpo_level` of the page (0 = leaf), for fresh-page redo init.
    pub level: u8,
    /// `btpo_flags` of the page (LEAF/ROOT/...), for fresh-page redo init.
    pub flags: u8,
    /// The encoded entry bytes: leaf `key ++ tid(10B)`, internal
    /// `key ++ child_page_id(8B)`, meta `(root_page_id, tree_level)(10B)`.
    pub tuple_bytes: Vec<u8>,
}

/// Payload for a `BTreeDelete` record: physical removal of one index entry.
///
/// M2b has no page merge; the delete rebuilds the page without the slot, so
/// redo is the same deterministic transformation applied to the same
/// pre-image (no separate compaction record is needed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeDeleteRecord {
    /// The page the entry is removed from.
    pub page_id: PageId,
    /// The slot being removed.
    pub slot_id: u16,
}

/// The scalar head carried BY the two sequence-carrying HNSW records
/// ([`HnswNodeInitRecord`] / [`HnswSetNeighborsRecord`]). The pre-decode
/// bound gate decodes this exact type — the same type the full record
/// contains — so gate and decoder can never drift apart (2026-09-14,
/// review round 6 P2: the independent prefix struct is gone; a field
/// added to the head moves gate and record in lockstep, structurally).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswSeqHead {
    /// The index's meta page (validation context for redo).
    pub meta_page_id: PageId,
    /// The node page the entry lives on.
    pub page_id: PageId,
    /// The slot of the node entry.
    pub slot_id: u16,
    /// The entry's NodeId (NodeInit: the id being allocated; SetNeighbors:
    /// the owner — the no-self-loop check's judgment basis, v1.12).
    pub node_id: u32,
    /// NodeInit: the drawn top level (carried in the record — replay never
    /// redraws, tech-selection §5). SetNeighbors: the level whose list is
    /// rewritten (must be <= the entry's top level).
    pub level: u8,
    /// Declared sequence length: `dim` (NodeInit) / `count` (SetNeighbors).
    pub tail: u16,
}

/// Payload for an `HnswNodeInit` record (Phase 2 M5 Stage 0, tech-selection
/// §4.2): creates a node entry on a node page at `(page_id, slot_id)` —
/// fixed-size reserved by `level`, state INITIALIZING (v1.5), all level
/// lists empty. Self-contained per the §4.2 rule: `meta_page_id` lets redo
/// re-derive `L_max` and the metric/dim checks from the meta page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HnswNodeInitRecord {
    /// Scalar head: meta page / node page / slot / NodeId / top level /
    /// declared dim — see [`HnswSeqHead`].
    pub head: HnswSeqHead,
    /// The node's vector, `dim` f32 components.
    pub vector: Vec<f32>,
}

impl HnswNodeInitRecord {
    /// Vector dimension (= `head.tail`; checked against the meta page at
    /// redo).
    pub fn dim(&self) -> u16 {
        self.head.tail
    }
}

/// Payload for an `HnswSetNeighbors` record (Phase 2 M5): in-place rewrite of
/// one level's neighbor list of the node entry at `(page_id, slot_id)` —
/// the entry never moves or grows (v1.5 fixed-size reservation). Carries the
/// owner `node_id` (v1.12: the redo-side no-self-loop check's judgment
/// basis; the owner ↔ `(page, slot)` directory-mapping consistency is an
/// open-time audit item, §11.3, not a redo check).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswSetNeighborsRecord {
    /// Scalar head: meta page / node page / slot / owner NodeId / level /
    /// declared count — see [`HnswSeqHead`].
    pub head: HnswSeqHead,
    /// The full new neighbor list content (ascending, no duplicates).
    pub neighbors: Vec<u32>,
}

impl HnswSetNeighborsRecord {
    /// Neighbor count (= `head.tail`) — must equal `neighbors.len()`.
    pub fn count(&self) -> u16 {
        self.head.tail
    }
}

/// Payload for an `HnswMetaUpdate` record (Phase 2 M5): field-level
/// post-image of the index's meta page (entry point / max level).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswMetaUpdateRecord {
    /// The meta page itself (also the touched page).
    pub meta_page_id: PageId,
    /// New entry-point NodeId; `u32::MAX` (NodeId::INVALID) for an empty
    /// graph — the M4 encoding's empty-graph sentinel convention.
    pub entry_point: u32,
    /// New max level (== the entry-point node's top level).
    pub max_level: u8,
}

/// Payload version nibble for the two HNSW state-bit records
/// (HnswNodeTombstone = 124, HnswPublishLive = 127), stamped in the record
/// header's `flags` byte — same convention as
/// [`CHECKPOINT_END_VERSION_V2`]/[`CHECKPOINT_END_V2_FLAGS`]
/// (2026-09-15, M5 Stage A review round 5 P2). Version 0 means the
/// pre-versioning DEVELOPMENT format (Stage 0 / early Stage A, before
/// `dim`/`meta_page_id` entered the payloads); it never shipped in any
/// release, so decode rejects it loudly instead of offering a compatibility
/// path. The other five HNSW record types are format-unchanged since their
/// introduction: their `flags` stays 0, read as "implicit original
/// version" — no nibble needed (documented here as the umbrella note).
pub const HNSW_STATE_VERSION_V1: u8 = 1;

/// The `flags` byte stamped on emitted v1 state-bit records: version 1 in
/// the high nibble, no record-specific flags in the low nibble.
pub const HNSW_STATE_V1_FLAGS: u8 = HNSW_STATE_VERSION_V1 << 4;

/// Payload for an `HnswNodeTombstone` record (Phase 2 M5): format only —
/// the tombstone SEMANTICS (search filtering, space reclamation) land in M6
/// (tech-selection §1 scope split); M5 only writes/replays the bit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswNodeTombstoneRecord {
    /// The node page the entry lives on.
    pub page_id: PageId,
    /// The slot of the node entry.
    pub slot_id: u16,
    /// The NodeId being tombstoned.
    pub node_id: u32,
    /// The index's vector dimension — the entry's state byte sits at offset
    /// `4·dim`, and the redo handler is stateless (tech-selection §10.1):
    /// without `dim` in the payload the bit is physically unlocatable
    /// (2026-09-15, M5 Stage A review round 3 P1-2). `dim == meta.dim` is
    /// a REDO pre-apply gate, not an audit item (round 6 P1): the handler
    /// reads meta through `meta_page_id` and rejects a mismatch BEFORE the
    /// bit flip — the open-time audit cannot recover historical payloads,
    /// so a wrong `dim` caught only there would already have rewritten the
    /// wrong bytes during redo. Sound at redo time because the meta page's
    /// init records precede every node record in LSN order and `dim` is
    /// immutable (metric/dim mismatch is a hard open failure).
    pub dim: u16,
    /// The index's meta page — carried so the redo handler can locate meta
    /// from the payload alone and gate `dim == meta.dim` before apply
    /// (2026-09-15, M5 Stage A review round 5 P1 for localization; round 6
    /// P1 moved the check itself from the open-time audit to the redo
    /// pre-apply gate). The RedoContext has no index registry (§4.2
    /// self-containment), so the payload must name the page.
    pub meta_page_id: PageId,
}

/// Payload for an `HnswDirAppend` record (Phase 2 M5): publishes the
/// `node_id → (target_page, target_slot)` mapping at the directory chain's
/// tail page. This record IS the NodeId allocation (v1.2: allocation ==
/// mapping publication; the high-water mark is derived from the chain).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswDirAppendRecord {
    /// The directory chain's current tail page.
    pub dir_tail_page: PageId,
    /// The NodeId being published (== the entry's ordinal in the chain).
    pub node_id: u32,
    /// The node page the entry lives on.
    pub target_page: PageId,
    /// The slot of the node entry on `target_page`.
    pub target_slot: u16,
}

/// Payload for an `HnswDirLink` record (Phase 2 M5): directory-chain
/// expansion — points the old tail page at the freshly allocated next
/// directory page (single-page record, v1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswDirLinkRecord {
    /// The directory chain's old tail page.
    pub old_tail_page: PageId,
    /// The freshly allocated next directory page.
    pub next_page: PageId,
}

/// Payload for an `HnswPublishLive` record (Phase 2 M5): flips the node
/// entry at `(page_id, slot_id)` from INITIALIZING to LIVE — the state
/// post-image (v1.7: the LIVE flip gets its own record, applied after all
/// of the node's own level lists are written).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswPublishLiveRecord {
    /// The node page the entry lives on.
    pub page_id: PageId,
    /// The slot of the node entry.
    pub slot_id: u16,
    /// The NodeId being published as live.
    pub node_id: u32,
    /// The index's vector dimension — locates the state byte (`4·dim`)
    /// without a meta read, keeping the redo handler stateless (2026-09-15,
    /// M5 Stage A review round 3 P1-2; same contract and redo pre-apply
    /// gate note as [`HnswNodeTombstoneRecord::dim`]).
    pub dim: u16,
    /// The index's meta page — same redo-gate localization contract as
    /// [`HnswNodeTombstoneRecord::meta_page_id`] (2026-09-15, round 5 P1;
    /// the check itself moved to the redo pre-apply gate in round 6 P1).
    pub meta_page_id: PageId,
}

// ---------------------------------------------------------------------
// Bounded, fully-consuming HNSW payload decoders (2026-09-11, M5 Stage 0
// review round 2 P2-1; mechanism rewritten 2026-09-14 round 3 P2 and
// round 4 P2-1/P3-1): pg-waldump decodes WAL payloads without the
// recovery path's trust context. Two properties:
//
// - **bounded**: sequence-carrying payloads (NodeInit.vector,
//   SetNeighbors.neighbors) pass a PRE-DECODE gate (`bounded_seq_gate`):
//   the shared scalar prefix is decoded through `bincode_config()`, the
//   wire's sequence length is decoded as a `u64` through the SAME config
//   (no hand-rolled varint/layout rules that could drift from the real
//   decoder), and the claim is checked against BOTH the remaining bytes
//   (allocation capped by input size) and the record's declared
//   dim/count (semantic mismatch rejected pre-allocation) BEFORE the
//   real decode runs. (Round 2 relied on bincode's serde layer to reject
//   forged lengths "without allocating" — false in substance: serde's
//   cautious size_hint still pre-allocates up to 1 MiB before the input
//   runs out, and the old test pinned only the error, not the
//   allocation.)
// - **fully-consuming**: `read != payload.len()` after a successful decode
//   is a loud error (trailing bytes mean the record is not what its type
//   claims).
//
// An earlier draft of this block hand-rolled a fixed-width decoder — wrong
// on contact with reality: bincode's standard config is VARINT for all
// integers (probed 2026-09-11), not little-endian fixed width. The
// encoders stay bincode-only; the decoders stay bincode-only; the
// roundtrip tests pin them together.

/// Bincode decode + exact-consumption gate shared by all seven HNSW
/// payload decoders (single implementation, P2-1).
fn decode_hnsw_payload<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T> {
    let (rec, read) = bincode::serde::decode_from_slice::<T, _>(payload, bincode_config())
        .map_err(|e| StorageError::Serialize(format!("HNSW payload undecodable: {e}")))?;
    if read != payload.len() {
        return Err(StorageError::Serialize(format!(
            "HNSW payload has {} trailing bytes",
            payload.len() - read
        )));
    }
    Ok(rec)
}

/// Pre-decode bound gate (round 3 P2; round 4 P2-1/P3-1; round 6 P2
/// revision — the prefix is now the records' OWN [`HnswSeqHead`], so the
/// gate decodes the exact type the full record contains and drift between
/// gate and decoder is structurally impossible, not merely test-pinned):
///
/// 1. decode the scalar head through the SHARED [`bincode_config`] — the
///    record's declared sequence length (`tail` = dim / count) comes from
///    this decode;
/// 2. decode the wire's sequence length as a `u64` through the SAME
///    config — no hand-rolled varint rules, a `bincode_config()` change
///    moves both decoders together;
/// 3. reject BEFORE any Vec allocation when (a) the wire length exceeds
///    what the remaining bytes could hold (`elem_min` = each element's
///    minimum encoded width: f32 = 4 fixed LE, varint ints = 1), or
///    (b) the wire length disagrees with the declared dim/count — a
///    legal-size but semantically false payload is now also rejected
///    pre-allocation (round 4 P3-1), not after it.
fn bounded_seq_gate(payload: &[u8], elem_min: usize, what: &str) -> Result<()> {
    let (head, off) =
        bincode::serde::decode_from_slice::<HnswSeqHead, _>(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(format!("HNSW payload head undecodable: {e}")))?;
    let (wire_len, len_bytes) =
        bincode::serde::decode_from_slice::<u64, _>(&payload[off..], bincode_config()).map_err(
            |e| StorageError::Serialize(format!("HNSW payload {what} length undecodable: {e}")),
        )?;
    let remaining = payload.len() - off - len_bytes;
    if wire_len > (remaining / elem_min) as u64 {
        return Err(StorageError::Serialize(format!(
            "HNSW payload {what} claims {wire_len} elements but only {remaining} bytes remain (bounded decode: allocation is capped by input size)"
        )));
    }
    if wire_len != u64::from(head.tail) {
        return Err(StorageError::Serialize(format!(
            "HNSW payload {what} wire length {wire_len} != declared {} (payload contract, rejected pre-allocation)",
            head.tail
        )));
    }
    Ok(())
}

impl HnswNodeInitRecord {
    /// Bounded (pre-decode length gate), fully-consuming decode — see the
    /// section comment. The gate enforces the `vector.len() == dim`
    /// payload contract pre-allocation.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        bounded_seq_gate(payload, 4, "vector")?;
        decode_hnsw_payload::<Self>(payload)
    }
}

impl HnswSetNeighborsRecord {
    /// Bounded, fully-consuming decode; the gate enforces
    /// `neighbors.len() == count` pre-allocation (varint elements: minimum
    /// encoded width 1 byte).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        bounded_seq_gate(payload, 1, "neighbors")?;
        decode_hnsw_payload::<Self>(payload)
    }
}

impl HnswMetaUpdateRecord {
    /// Bounded, fully-consuming decode (P2-1 — see the section comment).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        decode_hnsw_payload(payload)
    }
}

impl HnswNodeTombstoneRecord {
    /// Bounded, fully-consuming decode, dispatched on the record's `flags`
    /// version nibble (2026-09-15, round 5 P2 — see [`HNSW_STATE_VERSION_V1`]):
    /// version 0 is the pre-versioning development format and is REJECTED
    /// (it never shipped in any release — regenerate the data directory);
    /// version 1 decodes the current layout; any other nibble is an unknown
    /// version from a newer binary and must never be silently mis-decoded.
    pub fn decode(payload: &[u8], flags: u8) -> Result<Self> {
        match flags >> 4 {
            0 => Err(StorageError::Serialize(
                "HnswNodeTombstone payload version 0: superseded pre-versioning development format (never shipped in any release; regenerate the data directory)"
                    .to_string(),
            )),
            HNSW_STATE_VERSION_V1 => decode_hnsw_payload(payload),
            v => Err(StorageError::WalReadFailed(format!(
                "unknown HnswNodeTombstone payload version {v}"
            ))),
        }
    }
}

impl HnswDirAppendRecord {
    /// Bounded, fully-consuming decode (P2-1 — see the section comment).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        decode_hnsw_payload(payload)
    }
}

impl HnswDirLinkRecord {
    /// Bounded, fully-consuming decode (P2-1 — see the section comment).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        decode_hnsw_payload(payload)
    }
}

impl HnswPublishLiveRecord {
    /// Bounded, fully-consuming decode, dispatched on the record's `flags`
    /// version nibble (2026-09-15, round 5 P2 — same contract as
    /// [`HnswNodeTombstoneRecord::decode`]).
    pub fn decode(payload: &[u8], flags: u8) -> Result<Self> {
        match flags >> 4 {
            0 => Err(StorageError::Serialize(
                "HnswPublishLive payload version 0: superseded pre-versioning development format (never shipped in any release; regenerate the data directory)"
                    .to_string(),
            )),
            HNSW_STATE_VERSION_V1 => decode_hnsw_payload(payload),
            v => Err(StorageError::WalReadFailed(format!(
                "unknown HnswPublishLive payload version {v}"
            ))),
        }
    }
}

/// Payload for a `BTreeSplitPrepare` record (tech-selection §13.3 step 1).
///
/// `left_old_next` is an addition to the §13.3 field list: Prepare touches
/// two pages that may reach disk independently, so redo guards each page by
/// its own `pd_lsn`. When the left page's post-Prepare image is durable but
/// the right page's is not, redo must re-initialize the right page and can no
/// longer read the left page's pre-Prepare `btpo_next` from the left page
/// itself (it now points at the right page); the value is therefore carried
/// in the payload. `PageId::INVALID` (0) means "no old right sibling".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeSplitPrepareRecord {
    /// The overflowing original page.
    pub left_page: PageId,
    /// The freshly allocated right sibling.
    pub new_right_page: PageId,
    /// `btpo_level` of both pages (0 = leaf).
    pub level: u8,
    /// `btpo_next` of `left_page` before the split (0 = none).
    pub left_old_next: PageId,
    /// The left page's maximum key before the split (redo validation marker).
    pub high_key_bytes: Vec<u8>,
}

/// Payload for a `BTreeSplitCopy` record (tech-selection §13.3 step 2).
///
/// Minimal by design (§13.3 P2-9): redo recomputes the moved tuples from the
/// left page instead of logging them, anchored by `left_page_pre_lsn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeSplitCopyRecord {
    /// The page being split.
    pub left_page: PageId,
    /// The right sibling receiving the upper half.
    pub right_page: PageId,
    /// Slots `[copy_start_slot, slot_count)` of the left page move right.
    pub copy_start_slot: u16,
    /// Idempotency anchor: redo applies only while
    /// `left_page.pd_lsn == left_page_pre_lsn` (the Prepare LSN).
    pub left_page_pre_lsn: Lsn,
}

/// Payload for a `BTreeSplitCommit` record (tech-selection §13.3 step 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeSplitCommitRecord {
    /// The page that was split.
    pub left_page: PageId,
    /// The right sibling created by the split.
    pub right_page: PageId,
    /// The parent page receiving the new downlink (a new root for root splits).
    pub parent_page: PageId,
    /// The separator key: the right page's low key, inserted into the parent
    /// together with `right_page` as the child pointer.
    pub separator_key: Vec<u8>,
    /// The slot at which the parent page gains the downlink.
    pub parent_insert_slot: u16,
}

/// Payload for a `BTreeSplitCLR` record (Stage S, §11.3): a compensation log
/// record emitted during undo to finish an incomplete B+Tree split.
///
/// Two shapes exist (post-Stage-S review C1/C2): a *finishing* CLR completes
/// the split (move owed entries, insert the downlink, clear the flag) and
/// always carries either a parent page or a new root + meta page; an
/// *unlink* CLR abandons the split (the whole right half was deleted in the
/// Copy→Commit window, so no separator exists) and carries `INVALID` for
/// parent/new_root/meta — `apply_split_clr` then splices the empty right
/// page out of the sibling chain and clears only `SPLIT_INCOMPLETE`.
///
/// # Field-order invariant (post-Stage-S fix B5)
///
/// `separator_key` is deliberately the LAST field. The analysis phase
/// prefix-decodes only the fixed-size leading fields (page ids, level,
/// slots, `redo_ref_lsn`) and never touches the variable-length tail:
/// bincode's standard config imposes no size limit, so a full decode would
/// trust a corrupt length prefix on a CRC-valid record with an unbounded
/// allocation. The layout changed after Stage S (the CLR discriminant is new
/// in Stage S and the project is pre-release, so no on-disk migration is
/// owed); all producers go through [`WalRecord::btree_split_clr`], and no
/// test fixture encodes CLR bytes by hand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeSplitCLRRecord {
    /// The left page of the incomplete split.
    pub left_page: PageId,
    /// The right page of the incomplete split.
    pub right_page: PageId,
    /// B+Tree level of the split page.
    pub level: u8,
    /// Slot where the Copy phase began. Only consulted when the right page
    /// never held entries; otherwise the apply keys off the right page's
    /// first entry.
    pub copy_start_slot: u16,
    /// LSN of the SplitPrepare record being compensated (idempotency anchor).
    pub redo_ref_lsn: Lsn,
    /// Parent page receiving the downlink (non-root splits); `INVALID` for
    /// root splits and unlink records.
    pub parent_page: PageId,
    /// Slot at which the parent gains the downlink.
    pub parent_insert_slot: u16,
    /// New root page for root splits; `PageId::INVALID` for non-root splits
    /// and unlink records.
    pub new_root_page: PageId,
    /// Meta page to update for root splits; `PageId::INVALID` for non-root.
    pub meta_page: PageId,
    /// Separator key inserted into the parent. Empty for unlink records.
    /// LAST field — see the struct-level field-order invariant.
    pub separator_key: Vec<u8>,
}

/// Maximum accepted length of a [`BTreeSplitCLRRecord::separator_key`]
/// (post-Stage-S fix B5, defense in depth for the remaining full decodes in
/// redo/undo). Mirrors `pg_am_btree::key::MAX_INDEX_KEY_BYTES`
/// (`(PAGE_SIZE - 32 - 16) / 3 - 16`, 2698 at 8 KiB pages) plus the 16-byte
/// index-entry trailer; pg-storage cannot depend on pg-am-btree, so the
/// formula is re-derived from [`PAGE_SIZE`](crate::types::PAGE_SIZE) here.
pub const MAX_CLR_SEPARATOR_KEY_BYTES: usize = (crate::types::PAGE_SIZE - 32 - 16) / 3;

/// Payload for a `TxnCommit` record: the transaction whose commit is durable.
///
/// Per the Commit hard-order rule (§3 P1-5), this record is fsynced *before*
/// the in-memory CLOG bit flips to `Committed`, so recovery can rebuild the
/// CLOG authoritatively from the WAL: a present `TxnCommit` means the XID is
/// committed regardless of any hint bits on data pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnCommitRecord {
    /// The transaction that committed.
    pub xid: TxnId,
}

/// Payload for a `TxnAbort` record: the transaction whose abort is durable.
///
/// ABORTED entries are never garbage-collected (v2.3-2): a missing CLOG entry
/// after recovery must never be silently treated as committed, so an explicit
/// `TxnAbort` record anchors the aborted state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnAbortRecord {
    /// The transaction that aborted.
    pub xid: TxnId,
}

impl HeapInsertRecord {
    /// Decode a `HeapInsert` payload. Exposed so out-of-crate redo handlers
    /// (`pg-am-heap`) can deserialize without the internal bincode config.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl HeapUpdateRecord {
    /// Decode a `HeapUpdate` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl HeapDeleteRecord {
    /// Decode a `HeapDelete` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl HeapHotUpdateRecord {
    /// Decode a `HeapHotUpdate` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl HeapCleanupRecord {
    /// Decode a `HeapCleanup` payload (see [`HeapInsertRecord::decode`]).
    ///
    /// Defense in depth (same policy as [`BTreeSplitCLRRecord::decode`]): the
    /// decoded `dead_slots` is rejected when it exceeds
    /// [`MAX_HEAP_CLEANUP_SLOTS`] — bincode's standard config has no size
    /// limit, so a corrupt length prefix on a CRC-valid record must not be
    /// trusted blindly; a page can never hold that many line pointers.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let rec: Self = bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0;
        if rec.dead_slots.len() > MAX_HEAP_CLEANUP_SLOTS {
            return Err(StorageError::Serialize(format!(
                "HeapCleanup dead_slots length {} exceeds maximum {}",
                rec.dead_slots.len(),
                MAX_HEAP_CLEANUP_SLOTS
            )));
        }
        Ok(rec)
    }
}

impl BTreeInsertRecord {
    /// Decode a `BTreeInsert` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl BTreeDeleteRecord {
    /// Decode a `BTreeDelete` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl BTreeSplitPrepareRecord {
    /// Decode a `BTreeSplitPrepare` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl BTreeSplitCopyRecord {
    /// Decode a `BTreeSplitCopy` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl BTreeSplitCommitRecord {
    /// Decode a `BTreeSplitCommit` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl BTreeSplitCLRRecord {
    /// Decode a `BTreeSplitCLR` payload (see [`HeapInsertRecord::decode`]).
    ///
    /// Defense in depth (post-Stage-S fix B5): the decoded `separator_key` is
    /// rejected when it exceeds [`MAX_CLR_SEPARATOR_KEY_BYTES`]. bincode's
    /// standard config has no size limit, so a corrupt length prefix on a
    /// CRC-valid record must not be trusted blindly; a separator key can
    /// never legitimately exceed the B+Tree's maximum index key size plus
    /// its trailer.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let rec: Self = bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0;
        if rec.separator_key.len() > MAX_CLR_SEPARATOR_KEY_BYTES {
            return Err(StorageError::Serialize(format!(
                "BTreeSplitCLR separator_key length {} exceeds maximum {}",
                rec.separator_key.len(),
                MAX_CLR_SEPARATOR_KEY_BYTES
            )));
        }
        Ok(rec)
    }
}

impl TxnCommitRecord {
    /// Decode a `TxnCommit` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

impl TxnAbortRecord {
    /// Decode a `TxnAbort` payload (see [`HeapInsertRecord::decode`]).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(bincode::serde::decode_from_slice(payload, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?
            .0)
    }
}

/// Payload for a `CheckpointEnd` record (v2 layout, M2b Stage N;
/// tech-selection §11.4).
///
/// v2 moves the ATT/DPT out of the record payload into external snapshot
/// files (a 100K-transaction ATT cannot fit the 64KB single-record payload
/// limit) and adds `next_oid`, so the record carries six fields. v1 (M1)
/// records carry only the first three; see [`CheckpointEndRecord::decode`]
/// for the versioned decoding contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointEndRecord {
    /// The LSN of the corresponding `CheckpointBegin` record (redo point).
    pub checkpoint_lsn: Lsn,
    /// The next page ID to allocate after the checkpoint.
    pub next_page_id: PageId,
    /// The next transaction ID to allocate after the checkpoint.
    pub next_txn_id: TxnId,
    /// The next OID to allocate after the checkpoint (v2; v1 decodes default
    /// this to [`crate::types::Oid::FIRST_USER`]).
    pub next_oid: u64,
    /// Path of the ATT snapshot file relative to the data directory, e.g.
    /// `meta/att-0000000000000128.snapshot` (v2; empty for v1, meaning "no
    /// snapshot — rebuild the ATT by a full WAL scan from `checkpoint_lsn`").
    pub att_file: String,
    /// Path of the DPT snapshot file relative to the data directory (v2;
    /// empty for v1, same semantics as `att_file`).
    pub dpt_file: String,
}

/// The M1 (v1) `CheckpointEnd` payload: three fields, no snapshot files.
///
/// Kept for decode-only migration of M1 data directories (tech-selection
/// §11.4, v2.3-17); M2 never emits this layout.
#[derive(Debug, Serialize, Deserialize)]
struct CheckpointEndRecordV1 {
    /// The LSN of the corresponding `CheckpointBegin` record (redo point).
    checkpoint_lsn: Lsn,
    /// The next page ID to allocate after the checkpoint.
    next_page_id: PageId,
    /// The next transaction ID to allocate after the checkpoint.
    next_txn_id: TxnId,
}

/// Payload version stamped on every `CheckpointEnd` record M2 emits.
///
/// # Version channel — deviation from tech-selection §11.4
///
/// §11.4 assigns the record payload version to the high 4 bits of a
/// `WalRecord.flags: u16` (`flags >> 12`). M1, however, froze the 32-byte
/// record header with `flags: u8` (`record.rs` header layout: bytes 24-27 =
/// `record_type, flags, payload_len`), and the header cannot be widened
/// without breaking every M1 segment on disk. The version therefore lives in
/// the **high 4 bits of the `u8` flags**: `version = flags >> 4`, with the
/// low 4 bits reserved for record-specific flags. All M1 records were written
/// with `flags = 0`, so they are implicitly v1 — exactly the §11.4 semantics,
/// shifted to the channel the frozen header actually provides.
pub const CHECKPOINT_END_VERSION_V2: u8 = 1;

/// The `flags` byte stamped on emitted v2 `CheckpointEnd` records: version 1
/// in the high nibble, no record-specific flags in the low nibble.
pub const CHECKPOINT_END_V2_FLAGS: u8 = CHECKPOINT_END_VERSION_V2 << 4;

impl CheckpointEndRecord {
    /// Decode a `CheckpointEnd` payload, dispatching on the record's `flags`
    /// version nibble (tech-selection §11.4 v1/v2 migration, v2.3-17; see
    /// [`CHECKPOINT_END_VERSION_V2`] for why the nibble is `flags >> 4`
    /// rather than the spec's `flags >> 12`).
    ///
    /// - version 0 (v1, all M1 records): decode the 3-field M1 layout and
    ///   fill the v2-only fields with defaults — `next_oid =
    ///   `[`crate::types::Oid::FIRST_USER`]` (16384, the PG reserved-OID
    ///   upper bound) and empty `att_file`/`dpt_file`. An empty `att_file`
    ///   tells the analysis phase there is no snapshot: it rebuilds the ATT
    ///   by a full WAL scan from `checkpoint_lsn` (Stage N wave 2 consumes
    ///   this).
    /// - version 1 (v2, emitted by M2): decode the full 6-field layout.
    ///
    /// Recovery never rewrites a v1 record as v2 (read-only recovery); the
    /// upgrade happens naturally when M2 emits its own `CheckpointEnd`.
    ///
    /// # Errors
    ///
    /// Returns an error on malformed payloads and on unknown version nibbles:
    /// a record from a newer binary must never be silently mis-decoded.
    pub fn decode(payload: &[u8], flags: u8) -> Result<Self> {
        match flags >> 4 {
            0 => {
                let v1: CheckpointEndRecordV1 =
                    bincode::serde::decode_from_slice(payload, bincode_config())
                        .map_err(|e| StorageError::Serialize(e.to_string()))?
                        .0;
                Ok(Self {
                    checkpoint_lsn: v1.checkpoint_lsn,
                    next_page_id: v1.next_page_id,
                    next_txn_id: v1.next_txn_id,
                    next_oid: crate::types::Oid::FIRST_USER.0,
                    att_file: String::new(),
                    dpt_file: String::new(),
                })
            }
            CHECKPOINT_END_VERSION_V2 => {
                Ok(bincode::serde::decode_from_slice(payload, bincode_config())
                    .map_err(|e| StorageError::Serialize(e.to_string()))?
                    .0)
            }
            v => Err(StorageError::WalReadFailed(format!(
                "unknown CheckpointEnd payload version {v}"
            ))),
        }
    }
}

/// A single WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// LSN at which this record begins.
    pub lsn: Lsn,
    /// LSN of the previous record from the same transaction (undo chain).
    pub prev_lsn: Lsn,
    /// Transaction ID, or 0 for non-transactional operations.
    pub txn_id: TxnId,
    /// Record type.
    pub record_type: WalRecordType,
    /// Flags (e.g. FPI marker).
    pub flags: u8,
    /// Variable-length payload.
    pub payload: Vec<u8>,
}

/// Reject `PageId::INVALID` for a payload page field (2026-09-11, M5 Stage 0
/// review P3-3(b)): every page-id field in the HNSW payload layouts must
/// name a real page at construction — the redo validation checklist rejects
/// such records, and the constructors' self-imposed discipline is to never
/// emit what redo would refuse. Shared by all seven HNSW constructors
/// (single implementation).
fn reject_invalid_page_id(record: &str, field: &str, page: PageId) -> Result<()> {
    if page == PageId::INVALID {
        return Err(StorageError::Serialize(format!(
            "{record} {field} must be a real page (PageId::INVALID)"
        )));
    }
    Ok(())
}

/// 2026-09-12, M5 Stage 0 round 2 P2-3: `u32::MAX` is `NodeId::INVALID`, the
/// "no node" sentinel (M4's `NodeId::INVALID`, pg-am-hnsw/src/graph.rs) —
/// legal **only** as an empty graph's `HnswMetaUpdate` entry point. A record
/// carrying it as a real node identity (node/owner/neighbor) is as malformed
/// as one carrying `PageId::INVALID`.
fn reject_invalid_node_id(record: &str, field: &str, node: u32) -> Result<()> {
    if node == u32::MAX {
        return Err(StorageError::Serialize(format!(
            "{record} {field} = u32::MAX (NodeId::INVALID is only legal as the empty-graph MetaUpdate entry point)"
        )));
    }
    Ok(())
}

impl WalRecord {
    /// Create a `PageAlloc` record.
    pub fn page_alloc(page_id: PageId) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(PageAllocRecord { page_id }, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::PageAlloc, payload))
    }

    /// Create a `PageFree` record.
    pub fn page_free(page_id: PageId) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(PageFreeRecord { page_id }, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::PageFree, payload))
    }

    /// Create a `FullPageImage` record.
    pub fn full_page_image(page_id: PageId, image: Vec<u8>) -> Result<Self> {
        let payload =
            bincode::serde::encode_to_vec(FullPageImageRecord { page_id, image }, bincode_config())
                .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::FullPageImage, payload))
    }

    /// Create a `CheckpointBegin` record.
    pub fn checkpoint_begin() -> Self {
        Self::new(WalRecordType::CheckpointBegin, Vec::new())
    }

    /// Create a `HeapInsert` record stamped with the inserting `xid`.
    ///
    /// `xid` is written to the record header (`txn_id`) so recovery's XID
    /// high-water scan counts heap mutations, not just Txn commit/abort
    /// records. Without this, an XID that inserted a tuple but crashed before
    /// committing could be reused after restart, and the reuser's commit would
    /// make the orphaned tuple (xmin = reused XID) phantom-visible.
    pub fn heap_insert(
        page_id: PageId,
        slot_id: u16,
        tuple_bytes: Vec<u8>,
        xid: TxnId,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            HeapInsertRecord {
                page_id,
                slot_id,
                tuple_bytes,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        let mut rec = Self::new(WalRecordType::HeapInsert, payload);
        rec.txn_id = xid;
        Ok(rec)
    }

    /// Create a `HeapUpdate` record (logical delete-old + insert-new) stamped
    /// with the updating `xid` (see [`Self::heap_insert`] for why).
    pub fn heap_update(
        old_tid: Tid,
        new_tid: Tid,
        xmax_old: TxnId,
        new_tuple_bytes: Vec<u8>,
        xid: TxnId,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            HeapUpdateRecord {
                old_tid,
                new_tid,
                xmax_old,
                new_tuple_bytes,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        let mut rec = Self::new(WalRecordType::HeapUpdate, payload);
        rec.txn_id = xid;
        Ok(rec)
    }

    /// Create a `HeapDelete` record (logical delete stamping `t_xmax`) stamped
    /// with the deleting `xid` (see [`Self::heap_insert`] for why).
    pub fn heap_delete(tid: Tid, xmax: TxnId, xid: TxnId) -> Result<Self> {
        let payload =
            bincode::serde::encode_to_vec(HeapDeleteRecord { tid, xmax }, bincode_config())
                .map_err(|e| StorageError::Serialize(e.to_string()))?;
        let mut rec = Self::new(WalRecordType::HeapDelete, payload);
        rec.txn_id = xid;
        Ok(rec)
    }

    /// Create a `HeapHotUpdate` record (page-local HOT update, Stage S):
    /// stamp old version deleted + t_ctid chain + HEAP_HOT_UPDATED, insert
    /// new version with HEAP_ONLY_TUPLE. Stamped with the updating `xid`
    /// (see [`Self::heap_insert`] for why).
    pub fn heap_hot_update(
        page_id: PageId,
        old_slot: u16,
        new_slot: u16,
        new_tuple_bytes: Vec<u8>,
        xmax: TxnId,
        xid: TxnId,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            HeapHotUpdateRecord {
                page_id,
                old_slot,
                new_slot,
                new_tuple_bytes,
                xmax,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        let mut rec = Self::new(WalRecordType::HeapHotUpdate, payload);
        rec.txn_id = xid;
        Ok(rec)
    }

    /// Create a `HeapCleanup` record (M3 Stage B, §4.5): physical compaction
    /// of `page_id` killing `dead_slots` (must be ascending — the redo
    /// handler re-runs the same `compact()` with these exact arguments), plus
    /// an optional page-chain unlink (`unlink_prev_page` /
    /// `unlink_next_page`; both `PageId::INVALID` when the page stays in the
    /// chain).
    ///
    /// Vacuum is not transactional, so the record's `txn_id` stays `INVALID`:
    /// the change is purely physical and must be replayed regardless of any
    /// transaction outcome.
    pub fn heap_cleanup(
        page_id: PageId,
        dead_slots: Vec<u16>,
        unlink_prev_page: PageId,
        unlink_next_page: PageId,
    ) -> Result<Self> {
        // Hard validation, not a debug assertion (F3): under the WAL-first
        // protocol a violating record would be appended before `compact()`
        // rejects it, and every subsequent recovery would hard-fail on the
        // poison record — a bricked data directory. Reject BEFORE encoding:
        // replay convergence (§4.5) rests on the strictly-ascending kill
        // list, and a list longer than a page's maximum LP count can never
        // be legitimate (same bound [`HeapCleanupRecord::decode`] enforces).
        if dead_slots.windows(2).any(|w| w[0] >= w[1]) {
            return Err(StorageError::Serialize(
                "HeapCleanup dead_slots must be strictly ascending".to_string(),
            ));
        }
        if dead_slots.len() > MAX_HEAP_CLEANUP_SLOTS {
            return Err(StorageError::Serialize(format!(
                "HeapCleanup dead_slots length {} exceeds maximum {}",
                dead_slots.len(),
                MAX_HEAP_CLEANUP_SLOTS
            )));
        }
        let payload = bincode::serde::encode_to_vec(
            HeapCleanupRecord {
                page_id,
                unlink_prev_page,
                unlink_next_page,
                dead_slots,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::HeapCleanup, payload))
    }

    /// Create a `BTreeInsert` record (leaf/internal entry or meta-page append).
    ///
    /// `level`/`flags` describe the target page so redo can initialize a
    /// fresh page correctly (see [`BTreeInsertRecord`]). Index entries carry
    /// no `t_xmin`, so the record's `txn_id` stays `INVALID`.
    pub fn btree_insert(
        page_id: PageId,
        slot_id: u16,
        level: u8,
        flags: u8,
        tuple_bytes: Vec<u8>,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            BTreeInsertRecord {
                page_id,
                slot_id,
                level,
                flags,
                tuple_bytes,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::BTreeInsert, payload))
    }

    /// Create a `BTreeDelete` record (physical removal of one index entry).
    pub fn btree_delete(page_id: PageId, slot_id: u16) -> Result<Self> {
        let payload =
            bincode::serde::encode_to_vec(BTreeDeleteRecord { page_id, slot_id }, bincode_config())
                .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::BTreeDelete, payload))
    }

    /// Create an `HnswNodeInit` record (Phase 2 M5 Stage 0; tech-selection
    /// §4.2). Loud argument validation: `dim > 0`, the vector length matches
    /// `dim`, and every component is finite (the M4 §5 entry-validation
    /// convention — a constructor must never emit a record the redo
    /// validation checklist would reject).
    pub fn hnsw_node_init(
        meta_page_id: PageId,
        page_id: PageId,
        slot_id: u16,
        node_id: u32,
        level: u8,
        dim: u16,
        vector: Vec<f32>,
    ) -> Result<Self> {
        reject_invalid_page_id("HnswNodeInit", "meta_page_id", meta_page_id)?;
        reject_invalid_page_id("HnswNodeInit", "page_id", page_id)?;
        reject_invalid_node_id("HnswNodeInit", "node_id", node_id)?;
        if dim == 0 {
            return Err(StorageError::Serialize(
                "HnswNodeInit dim = 0 (M4 §5: rejected at every entry point)".to_string(),
            ));
        }
        if vector.len() != usize::from(dim) {
            return Err(StorageError::Serialize(format!(
                "HnswNodeInit vector has {} components, dim is {dim}",
                vector.len()
            )));
        }
        if let Some((i, _)) = vector.iter().enumerate().find(|(_, x)| !x.is_finite()) {
            return Err(StorageError::Serialize(format!(
                "HnswNodeInit non-finite vector component at index {i} (M4 §5/§7)"
            )));
        }
        if level > 63 {
            return Err(StorageError::Serialize(format!(
                "HnswNodeInit level {level} > 63 (top_level is 6 bits in the node-entry state byte; M4's MAX_LEVEL_COUNT = 64 convention)"
            )));
        }
        let payload = bincode::serde::encode_to_vec(
            HnswNodeInitRecord {
                head: HnswSeqHead {
                    meta_page_id,
                    page_id,
                    slot_id,
                    node_id,
                    level,
                    tail: dim,
                },
                vector,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::HnswNodeInit, payload))
    }

    /// Create an `HnswSetNeighbors` record (Phase 2 M5). Loud argument
    /// validation: `count == neighbors.len()`, strictly ascending neighbors,
    /// no self-loop against the owner `node_id` (v1.12 — the owner is
    /// carried precisely so this check is implementable), and
    /// `level <= 63` (2026-09-11 Stage 0 review P3-3(a): NodeInit and
    /// MetaUpdate already reject out-of-range levels; this constructor
    /// missed the same gate).
    pub fn hnsw_set_neighbors(
        meta_page_id: PageId,
        page_id: PageId,
        slot_id: u16,
        node_id: u32,
        level: u8,
        neighbors: Vec<u32>,
    ) -> Result<Self> {
        reject_invalid_page_id("HnswSetNeighbors", "meta_page_id", meta_page_id)?;
        reject_invalid_page_id("HnswSetNeighbors", "page_id", page_id)?;
        reject_invalid_node_id("HnswSetNeighbors", "node_id", node_id)?;
        if neighbors.contains(&u32::MAX) {
            return Err(StorageError::Serialize(
                "HnswSetNeighbors neighbor = u32::MAX (NodeId::INVALID is not a real endpoint)"
                    .to_string(),
            ));
        }
        if level > 63 {
            return Err(StorageError::Serialize(format!(
                "HnswSetNeighbors level {level} > 63 (6-bit top_level convention)"
            )));
        }
        if neighbors.len() > usize::from(u16::MAX) {
            return Err(StorageError::Serialize(format!(
                "HnswSetNeighbors {} neighbors exceeds the u16 count field",
                neighbors.len()
            )));
        }
        if neighbors.windows(2).any(|w| w[0] >= w[1]) {
            return Err(StorageError::Serialize(
                "HnswSetNeighbors neighbors must be strictly ascending (duplicates included)"
                    .to_string(),
            ));
        }
        if neighbors.contains(&node_id) {
            return Err(StorageError::Serialize(format!(
                "HnswSetNeighbors self-loop: owner node_id {node_id} is its own neighbor (v1.12 owner check)"
            )));
        }
        let payload = bincode::serde::encode_to_vec(
            HnswSetNeighborsRecord {
                head: HnswSeqHead {
                    meta_page_id,
                    page_id,
                    slot_id,
                    node_id,
                    level,
                    tail: neighbors.len() as u16,
                },
                neighbors,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::HnswSetNeighbors, payload))
    }

    /// Create an `HnswMetaUpdate` record (Phase 2 M5).
    pub fn hnsw_meta_update(meta_page_id: PageId, entry_point: u32, max_level: u8) -> Result<Self> {
        reject_invalid_page_id("HnswMetaUpdate", "meta_page_id", meta_page_id)?;
        if max_level > 63 {
            return Err(StorageError::Serialize(format!(
                "HnswMetaUpdate max_level {max_level} > 63 (6-bit top_level convention)"
            )));
        }
        // 2026-09-12, round 2 P2-3: NodeId::INVALID is legal here ONLY for
        // the empty graph — an entry point of "no node" with a nonzero
        // max_level is a contradiction (no node can claim a top level).
        if entry_point == u32::MAX && max_level != 0 {
            return Err(StorageError::Serialize(format!(
                "HnswMetaUpdate entry_point = NodeId::INVALID but max_level = {max_level} (INVALID is only legal for the empty graph, max_level = 0)"
            )));
        }
        let payload = bincode::serde::encode_to_vec(
            HnswMetaUpdateRecord {
                meta_page_id,
                entry_point,
                max_level,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::HnswMetaUpdate, payload))
    }

    /// Create an `HnswNodeTombstone` record (Phase 2 M5 — format only; the
    /// semantics land in M6, tech-selection §1). `dim` locates the entry's
    /// state byte at redo (2026-09-15 round 3 P1-2); `dim = 0` is rejected
    /// like NodeInit's (M4 §5: rejected at every entry point).
    pub fn hnsw_node_tombstone(
        meta_page_id: PageId,
        page_id: PageId,
        slot_id: u16,
        node_id: u32,
        dim: u16,
    ) -> Result<Self> {
        reject_invalid_page_id("HnswNodeTombstone", "meta_page_id", meta_page_id)?;
        reject_invalid_page_id("HnswNodeTombstone", "page_id", page_id)?;
        reject_invalid_node_id("HnswNodeTombstone", "node_id", node_id)?;
        if dim == 0 {
            return Err(StorageError::Serialize(
                "HnswNodeTombstone dim = 0 (M4 §5: rejected at every entry point)".to_string(),
            ));
        }
        let payload = bincode::serde::encode_to_vec(
            HnswNodeTombstoneRecord {
                page_id,
                slot_id,
                node_id,
                dim,
                meta_page_id,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        // 2026-09-15, round 5 P2: stamp the v1 payload-version nibble (see
        // [`HNSW_STATE_VERSION_V1`]).
        let mut rec = Self::new(WalRecordType::HnswNodeTombstone, payload);
        rec.flags = HNSW_STATE_V1_FLAGS;
        Ok(rec)
    }

    /// Create an `HnswDirAppend` record (Phase 2 M5): publish a
    /// `node_id → (target_page, target_slot)` mapping at the directory tail.
    pub fn hnsw_dir_append(
        dir_tail_page: PageId,
        node_id: u32,
        target_page: PageId,
        target_slot: u16,
    ) -> Result<Self> {
        reject_invalid_page_id("HnswDirAppend", "dir_tail_page", dir_tail_page)?;
        reject_invalid_node_id("HnswDirAppend", "node_id", node_id)?;
        reject_invalid_page_id("HnswDirAppend", "target_page", target_page)?;
        let payload = bincode::serde::encode_to_vec(
            HnswDirAppendRecord {
                dir_tail_page,
                node_id,
                target_page,
                target_slot,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::HnswDirAppend, payload))
    }

    /// Create an `HnswDirLink` record (Phase 2 M5): point the old directory
    /// tail at the freshly allocated next directory page.
    pub fn hnsw_dir_link(old_tail_page: PageId, next_page: PageId) -> Result<Self> {
        reject_invalid_page_id("HnswDirLink", "old_tail_page", old_tail_page)?;
        reject_invalid_page_id("HnswDirLink", "next_page", next_page)?;
        if next_page == old_tail_page {
            return Err(StorageError::Serialize(
                "HnswDirLink must not link a page to itself".to_string(),
            ));
        }
        let payload = bincode::serde::encode_to_vec(
            HnswDirLinkRecord {
                old_tail_page,
                next_page,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::HnswDirLink, payload))
    }

    /// Create an `HnswPublishLive` record (Phase 2 M5): flip the node entry
    /// from INITIALIZING to LIVE. `dim` locates the entry's state byte at
    /// redo (2026-09-15 round 3 P1-2); `dim = 0` is rejected like
    /// NodeInit's (M4 §5: rejected at every entry point).
    pub fn hnsw_publish_live(
        meta_page_id: PageId,
        page_id: PageId,
        slot_id: u16,
        node_id: u32,
        dim: u16,
    ) -> Result<Self> {
        reject_invalid_page_id("HnswPublishLive", "meta_page_id", meta_page_id)?;
        reject_invalid_page_id("HnswPublishLive", "page_id", page_id)?;
        reject_invalid_node_id("HnswPublishLive", "node_id", node_id)?;
        if dim == 0 {
            return Err(StorageError::Serialize(
                "HnswPublishLive dim = 0 (M4 §5: rejected at every entry point)".to_string(),
            ));
        }
        let payload = bincode::serde::encode_to_vec(
            HnswPublishLiveRecord {
                page_id,
                slot_id,
                node_id,
                dim,
                meta_page_id,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        // 2026-09-15, round 5 P2: stamp the v1 payload-version nibble (see
        // [`HNSW_STATE_VERSION_V1`]).
        let mut rec = Self::new(WalRecordType::HnswPublishLive, payload);
        rec.flags = HNSW_STATE_V1_FLAGS;
        Ok(rec)
    }

    /// Create a `BTreeSplitPrepare` record (§13.3 step 1).
    pub fn btree_split_prepare(
        left_page: PageId,
        new_right_page: PageId,
        level: u8,
        left_old_next: PageId,
        high_key_bytes: Vec<u8>,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            BTreeSplitPrepareRecord {
                left_page,
                new_right_page,
                level,
                left_old_next,
                high_key_bytes,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::BTreeSplitPrepare, payload))
    }

    /// Create a `BTreeSplitCopy` record (§13.3 step 2, minimal payload).
    pub fn btree_split_copy(
        left_page: PageId,
        right_page: PageId,
        copy_start_slot: u16,
        left_page_pre_lsn: Lsn,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            BTreeSplitCopyRecord {
                left_page,
                right_page,
                copy_start_slot,
                left_page_pre_lsn,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::BTreeSplitCopy, payload))
    }

    /// Create a `BTreeSplitCommit` record (§13.3 step 3).
    pub fn btree_split_commit(
        left_page: PageId,
        right_page: PageId,
        parent_page: PageId,
        separator_key: Vec<u8>,
        parent_insert_slot: u16,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            BTreeSplitCommitRecord {
                left_page,
                right_page,
                parent_page,
                separator_key,
                parent_insert_slot,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::BTreeSplitCommit, payload))
    }

    /// Create a `BTreeSplitCLR` record (Stage S, §11.3 undo):
    /// a compensation log record that finishes an incomplete split.
    pub fn btree_split_clr(rec: &BTreeSplitCLRRecord) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(rec, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
        Ok(Self::new(WalRecordType::BTreeSplitCLR, payload))
    }

    /// Create a `TxnCommit` record for `xid`.
    ///
    /// The record's `txn_id` is stamped with `xid` so recovery's active-xact
    /// bookkeeping and the redo handler can identify the transaction without
    /// decoding the payload.
    pub fn txn_commit(xid: TxnId) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(TxnCommitRecord { xid }, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
        let mut rec = Self::new(WalRecordType::TxnCommit, payload);
        rec.txn_id = xid;
        Ok(rec)
    }

    /// Create a `TxnAbort` record for `xid` (see [`Self::txn_commit`]).
    pub fn txn_abort(xid: TxnId) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(TxnAbortRecord { xid }, bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
        let mut rec = Self::new(WalRecordType::TxnAbort, payload);
        rec.txn_id = xid;
        Ok(rec)
    }

    /// Create a v2 `CheckpointEnd` record (M2b Stage N; tech-selection §11.4).
    ///
    /// The record carries the six-field v2 payload and is stamped with
    /// [`CHECKPOINT_END_V2_FLAGS`] (version 1 in the flags high nibble) so
    /// readers can dispatch v1/v2 via [`CheckpointEndRecord::decode`].
    /// `att_file`/`dpt_file` are the ATT/DPT snapshot paths relative to the
    /// data directory; pass empty strings only when no snapshot was written
    /// (the reader then rebuilds from `checkpoint_lsn` by a full WAL scan).
    pub fn checkpoint_end(
        checkpoint_lsn: Lsn,
        next_page_id: PageId,
        next_txn_id: TxnId,
        next_oid: u64,
        att_file: String,
        dpt_file: String,
    ) -> Result<Self> {
        let payload = bincode::serde::encode_to_vec(
            CheckpointEndRecord {
                checkpoint_lsn,
                next_page_id,
                next_txn_id,
                next_oid,
                att_file,
                dpt_file,
            },
            bincode_config(),
        )
        .map_err(|e| StorageError::Serialize(e.to_string()))?;
        let mut rec = Self::new(WalRecordType::CheckpointEnd, payload);
        rec.flags = CHECKPOINT_END_V2_FLAGS;
        Ok(rec)
    }

    /// Return the total serialized size of this record, including padding.
    pub fn record_size(&self) -> usize {
        let raw = WAL_RECORD_HEADER_SIZE + self.payload.len();
        align_up(raw, 8)
    }

    /// Serialize the record into a byte vector.
    ///
    /// The caller should set `self.lsn` before calling this method.
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.payload.len() > u16::MAX as usize {
            return Err(StorageError::WalWriteFailed(format!(
                "payload length {} exceeds maximum {}",
                self.payload.len(),
                u16::MAX
            )));
        }

        let total = self.record_size();
        let mut buf = Vec::with_capacity(total);

        // Header (24 bytes).
        buf.extend_from_slice(&self.lsn.0.to_le_bytes());
        buf.extend_from_slice(&self.prev_lsn.0.to_le_bytes());
        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());

        // Meta (8 bytes): record_type, flags, payload_len, crc placeholder.
        buf.push(self.record_type.to_u8());
        buf.push(self.flags);
        buf.extend_from_slice(&(self.payload.len() as u16).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());

        // Payload.
        buf.extend_from_slice(&self.payload);

        // Padding to 8-byte alignment.
        buf.resize(total, 0);

        // Compute CRC over everything except the crc field itself (bytes 28-31).
        let mut hasher = Hasher::new();
        hasher.update(&buf[0..28]);
        hasher.update(&buf[32..total]);
        let crc = hasher.finalize();
        buf[28..32].copy_from_slice(&crc.to_le_bytes());

        Ok(buf)
    }

    /// Decode a record from its serialized form.
    ///
    /// Returns the record and the total number of bytes consumed (including
    /// padding).
    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < WAL_RECORD_HEADER_SIZE {
            return Err(StorageError::WalCorrupted(Lsn::INVALID));
        }

        let lsn = Lsn(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
        let prev_lsn = Lsn(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
        let txn_id = TxnId(u64::from_le_bytes(buf[16..24].try_into().unwrap()));
        let type_byte = buf[24];
        let flags = buf[25];
        let payload_len = u16::from_le_bytes(buf[26..28].try_into().unwrap()) as usize;
        let stored_crc = u32::from_le_bytes(buf[28..32].try_into().unwrap());

        let total = align_up(WAL_RECORD_HEADER_SIZE + payload_len, 8);
        if buf.len() < total {
            return Err(StorageError::WalCorrupted(lsn));
        }

        // Verify CRC BEFORE checking the discriminant (Stage N review, P1):
        // a valid CRC means the bytes on disk are intact; an unknown
        // discriminant then is a genuine "type not recognized" error, not a
        // bit-rot artifact that should be silently treated as end-of-WAL.
        let mut hasher = Hasher::new();
        hasher.update(&buf[0..28]);
        hasher.update(&buf[32..total]);
        if hasher.finalize() != stored_crc {
            return Err(StorageError::WalCorrupted(lsn));
        }

        let record_type = WalRecordType::from_u8(type_byte)?;

        let payload = buf[32..32 + payload_len].to_vec();
        let record = Self {
            lsn,
            prev_lsn,
            txn_id,
            record_type,
            flags,
            payload,
        };
        Ok((record, total))
    }

    fn new(record_type: WalRecordType, payload: Vec<u8>) -> Self {
        Self {
            lsn: Lsn::INVALID,
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type,
            flags: 0,
            payload,
        }
    }
}

/// Return the shared bincode configuration used across the storage crate.
/// `pub` (not `pub(crate)`) so binary targets under `src/bin/` (separate
/// crates linking this lib, e.g. pg-waldump) share the exact same config.
pub fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PAGE_SIZE;
    use proptest::prelude::*;

    #[test]
    fn page_alloc_roundtrip() {
        let mut record = WalRecord::page_alloc(PageId(42)).unwrap();
        record.lsn = Lsn(16);
        let buf = record.encode().unwrap();
        assert_eq!(buf.len() % 8, 0);

        let (decoded, consumed) = WalRecord::decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.lsn, Lsn(16));
        assert_eq!(decoded.record_type, WalRecordType::PageAlloc);
        assert_eq!(decoded.payload, record.payload);
    }

    #[test]
    fn checkpoint_begin_roundtrip() {
        let mut record = WalRecord::checkpoint_begin();
        record.lsn = Lsn(128);
        let buf = record.encode().unwrap();
        assert_eq!(buf.len() % 8, 0);

        let (decoded, consumed) = WalRecord::decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.lsn, Lsn(128));
        assert_eq!(decoded.record_type, WalRecordType::CheckpointBegin);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn checkpoint_end_v2_roundtrip() {
        let mut record = WalRecord::checkpoint_end(
            Lsn(128),
            PageId(99),
            TxnId(7),
            20_000,
            "meta/att-0000000000000128.snapshot".to_string(),
            "meta/dpt-0000000000000128.snapshot".to_string(),
        )
        .unwrap();
        // Emitted v2 records carry version 1 in the flags high nibble
        // (§11.4; the channel is `flags >> 4`, not the spec's `>> 12` — the
        // M1 32-byte header froze `flags` at u8).
        assert_eq!(record.flags, CHECKPOINT_END_V2_FLAGS);
        assert_eq!(record.flags >> 4, CHECKPOINT_END_VERSION_V2);
        record.lsn = Lsn(256);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.lsn, Lsn(256));
        assert_eq!(decoded.record_type, WalRecordType::CheckpointEnd);
        assert_eq!(decoded.flags, CHECKPOINT_END_V2_FLAGS);
        assert_eq!(decoded.payload, record.payload);

        let payload = CheckpointEndRecord::decode(&decoded.payload, decoded.flags).unwrap();
        assert_eq!(payload.checkpoint_lsn, Lsn(128));
        assert_eq!(payload.next_page_id, PageId(99));
        assert_eq!(payload.next_txn_id, TxnId(7));
        assert_eq!(payload.next_oid, 20_000);
        assert_eq!(payload.att_file, "meta/att-0000000000000128.snapshot");
        assert_eq!(payload.dpt_file, "meta/dpt-0000000000000128.snapshot");
    }

    /// v1/v2 migration (§11.4, v2.3-17): a hand-built M1 v1 payload
    /// (`flags = 0`, 3 fields) decodes with the v2-only fields defaulted —
    /// `next_oid = 16384` (PG reserved-OID bound) and empty snapshot paths,
    /// which analysis reads as "no snapshot: full rebuild from
    /// `checkpoint_lsn`".
    #[test]
    fn checkpoint_end_v1_decode_defaults() {
        let v1_payload = bincode::serde::encode_to_vec(
            CheckpointEndRecordV1 {
                checkpoint_lsn: Lsn(64),
                next_page_id: PageId(5),
                next_txn_id: TxnId(3),
            },
            bincode_config(),
        )
        .unwrap();

        let decoded = CheckpointEndRecord::decode(&v1_payload, 0).unwrap();
        assert_eq!(decoded.checkpoint_lsn, Lsn(64));
        assert_eq!(decoded.next_page_id, PageId(5));
        assert_eq!(decoded.next_txn_id, TxnId(3));
        assert_eq!(decoded.next_oid, crate::types::Oid::FIRST_USER.0);
        assert!(decoded.att_file.is_empty());
        assert!(decoded.dpt_file.is_empty());
    }

    /// The version nibble (high 4 bits) and the record-specific flag bits
    /// (low 4 bits) must not interfere: low bits set on a v1 record still
    /// dispatch to v1, and on a v2 record still dispatch to v2.
    #[test]
    fn checkpoint_end_version_nibble_ignores_low_flag_bits() {
        let v1_payload = bincode::serde::encode_to_vec(
            CheckpointEndRecordV1 {
                checkpoint_lsn: Lsn(64),
                next_page_id: PageId(5),
                next_txn_id: TxnId(3),
            },
            bincode_config(),
        )
        .unwrap();
        let v1 = CheckpointEndRecord::decode(&v1_payload, 0x0F).unwrap();
        assert_eq!(v1.next_oid, crate::types::Oid::FIRST_USER.0);
        assert!(v1.att_file.is_empty());

        let v2_record = WalRecord::checkpoint_end(
            Lsn(64),
            PageId(5),
            TxnId(3),
            20_000,
            "meta/att-x.snapshot".to_string(),
            "meta/dpt-x.snapshot".to_string(),
        )
        .unwrap();
        let v2 = CheckpointEndRecord::decode(&v2_record.payload, CHECKPOINT_END_V2_FLAGS | 0x0F)
            .unwrap();
        assert_eq!(v2.next_oid, 20_000);
        assert_eq!(v2.att_file, "meta/att-x.snapshot");
    }

    /// A record written by a newer binary (unknown version nibble) is a hard
    /// error, never a silent mis-decode.
    #[test]
    fn checkpoint_end_decode_rejects_unknown_version() {
        let record = WalRecord::checkpoint_end(
            Lsn(64),
            PageId(5),
            TxnId(3),
            1,
            String::new(),
            String::new(),
        )
        .unwrap();
        assert!(CheckpointEndRecord::decode(&record.payload, 2 << 4).is_err());
    }

    #[test]
    fn full_page_image_roundtrip() {
        let image = vec![0xAB; 8192];
        let mut record = WalRecord::full_page_image(PageId(3), image).unwrap();
        record.lsn = Lsn(64);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::FullPageImage);
        assert_eq!(decoded.payload, record.payload);
    }

    /// Phase 2 M5 Stage 0: the seven HNSW payload layouts round-trip through
    /// encode → bincode decode with every field asserted (tech-selection §4.2
    /// self-containment: NodeInit/SetNeighbors/MetaUpdate carry
    /// `meta_page_id`; SetNeighbors carries the owner `node_id`, v1.12).
    /// Tombstone is format-only (§1 scope split — semantics land in M6).
    #[test]
    fn hnsw_payloads_roundtrip_field_by_field() {
        // 121 NodeInit.
        let rec =
            WalRecord::hnsw_node_init(PageId(1), PageId(10), 3, 42, 2, 4, vec![1.0, 2.0, 3.0, 4.0])
                .unwrap();
        let r = HnswNodeInitRecord::decode(&rec.payload).unwrap();
        assert_eq!(
            (
                r.head.meta_page_id,
                r.head.page_id,
                r.head.slot_id,
                r.head.node_id,
                r.head.level,
                r.dim()
            ),
            (PageId(1), PageId(10), 3, 42, 2, 4)
        );
        assert_eq!(r.vector, vec![1.0, 2.0, 3.0, 4.0]);

        // 122 SetNeighbors (owner node_id present, v1.12).
        let rec =
            WalRecord::hnsw_set_neighbors(PageId(1), PageId(11), 4, 42, 0, vec![1, 5, 9]).unwrap();
        let r = HnswSetNeighborsRecord::decode(&rec.payload).unwrap();
        assert_eq!(
            (
                r.head.meta_page_id,
                r.head.page_id,
                r.head.slot_id,
                r.head.node_id,
                r.head.level,
                r.count()
            ),
            (PageId(1), PageId(11), 4, 42, 0, 3)
        );
        assert_eq!(r.neighbors, vec![1, 5, 9]);

        // 123 MetaUpdate.
        let rec = WalRecord::hnsw_meta_update(PageId(1), 42, 2).unwrap();
        let r = HnswMetaUpdateRecord::decode(&rec.payload).unwrap();
        assert_eq!(
            (r.meta_page_id, r.entry_point, r.max_level),
            (PageId(1), 42, 2)
        );

        // 124 NodeTombstone (format only; dim locates the state byte,
        // 2026-09-15 round 3 P1-2).
        let rec = WalRecord::hnsw_node_tombstone(PageId(1), PageId(11), 4, 42, 4).unwrap();
        let r = HnswNodeTombstoneRecord::decode(&rec.payload, rec.flags).unwrap();
        assert_eq!(
            (r.page_id, r.slot_id, r.node_id, r.dim, r.meta_page_id),
            (PageId(11), 4, 42, 4, PageId(1))
        );
        // 2026-09-15 round 5 P2: the state-bit records carry the v1
        // payload-version nibble in the header flags byte.
        assert_eq!(rec.flags, HNSW_STATE_V1_FLAGS);

        // 125 DirAppend.
        let rec = WalRecord::hnsw_dir_append(PageId(100), 42, PageId(11), 4).unwrap();
        let r = HnswDirAppendRecord::decode(&rec.payload).unwrap();
        assert_eq!(
            (r.dir_tail_page, r.node_id, r.target_page, r.target_slot),
            (PageId(100), 42, PageId(11), 4)
        );

        // 126 DirLink.
        let rec = WalRecord::hnsw_dir_link(PageId(100), PageId(101)).unwrap();
        let r = HnswDirLinkRecord::decode(&rec.payload).unwrap();
        assert_eq!((r.old_tail_page, r.next_page), (PageId(100), PageId(101)));

        // 127 PublishLive.
        let rec = WalRecord::hnsw_publish_live(PageId(1), PageId(11), 4, 42, 4).unwrap();
        let r = HnswPublishLiveRecord::decode(&rec.payload, rec.flags).unwrap();
        assert_eq!(
            (r.page_id, r.slot_id, r.node_id, r.dim, r.meta_page_id),
            (PageId(11), 4, 42, 4, PageId(1))
        );
        assert_eq!(rec.flags, HNSW_STATE_V1_FLAGS);
    }

    /// Golden bytes pin (2026-09-14, review round 7 P3-1): the nested
    /// `HnswSeqHead` record layout must stay byte-identical to the flat
    /// wire layout it replaced (bincode encodes fields positionally,
    /// varint standard config — every small value below is a one-byte
    /// varint; f32 is fixed 4B LE). If a future refactor changes the wire
    /// bytes, this test — not memory — says so.
    #[test]
    fn hnsw_payload_golden_bytes() {
        // 121 NodeInit: head (meta/page/slot/node/level/dim) + vec len + 4 f32.
        let rec =
            WalRecord::hnsw_node_init(PageId(1), PageId(10), 3, 42, 2, 4, vec![1.0, 2.0, 3.0, 4.0])
                .unwrap();
        let mut expect = vec![1u8, 10, 3, 42, 2, 4, 4];
        for f in [1.0f32, 2.0, 3.0, 4.0] {
            expect.extend_from_slice(&f.to_le_bytes());
        }
        assert_eq!(rec.payload, expect);

        // 122 SetNeighbors: head (…/count) + vec len + 3 one-byte varint ids.
        let rec =
            WalRecord::hnsw_set_neighbors(PageId(1), PageId(11), 4, 42, 0, vec![1, 5, 9]).unwrap();
        assert_eq!(rec.payload, vec![1u8, 11, 4, 42, 0, 3, 3, 1, 5, 9]);

        // 124/127 (2026-09-15, round 3 P1-2 + round 5 P1): page/slot/node +
        // dim + meta_page_id — trailing dim locates the state byte,
        // meta_page_id localizes the §11.3 audit; both POSITIONS are format.
        let rec = WalRecord::hnsw_node_tombstone(PageId(1), PageId(11), 4, 42, 4).unwrap();
        assert_eq!(rec.payload, vec![11u8, 4, 42, 4, 1]);
        let rec = WalRecord::hnsw_publish_live(PageId(1), PageId(11), 4, 42, 4).unwrap();
        assert_eq!(rec.payload, vec![11u8, 4, 42, 4, 1]);
    }

    /// Stage 0: constructor argument validation fails loudly (a constructor
    /// must never emit a record the redo validation checklist would reject).
    #[test]
    fn hnsw_constructors_reject_bad_arguments() {
        // dim = 0 / length mismatch / NaN / level > 63.
        assert!(WalRecord::hnsw_node_init(PageId(1), PageId(2), 0, 0, 0, 0, vec![]).is_err());
        assert!(WalRecord::hnsw_node_init(PageId(1), PageId(2), 0, 0, 0, 4, vec![1.0]).is_err());
        assert!(
            WalRecord::hnsw_node_init(PageId(1), PageId(2), 0, 0, 0, 1, vec![f32::NAN]).is_err()
        );
        assert!(WalRecord::hnsw_node_init(PageId(1), PageId(2), 0, 0, 64, 1, vec![1.0]).is_err());
        // SetNeighbors: unsorted/duplicate/self-loop.
        assert!(WalRecord::hnsw_set_neighbors(PageId(1), PageId(2), 0, 0, 0, vec![5, 3]).is_err());
        assert!(WalRecord::hnsw_set_neighbors(PageId(1), PageId(2), 0, 0, 0, vec![3, 3]).is_err());
        assert!(WalRecord::hnsw_set_neighbors(PageId(1), PageId(2), 0, 7, 0, vec![3, 7]).is_err());
        // MetaUpdate: max_level > 63.
        assert!(WalRecord::hnsw_meta_update(PageId(1), 0, 64).is_err());
        // SetNeighbors: level > 63 (2026-09-11 Stage 0 review P3-3(a)).
        assert!(WalRecord::hnsw_set_neighbors(PageId(1), PageId(2), 0, 0, 64, vec![3]).is_err());
        // PageId::INVALID rejected on every page field (P3-3(b)): the two
        // already-gated fields (DirAppend.target_page, DirLink.next_page)
        // plus the nine that were silently accepted.
        assert!(
            WalRecord::hnsw_node_init(PageId::INVALID, PageId(2), 0, 0, 0, 1, vec![1.0]).is_err()
        );
        assert!(
            WalRecord::hnsw_node_init(PageId(1), PageId::INVALID, 0, 0, 0, 1, vec![1.0]).is_err()
        );
        assert!(
            WalRecord::hnsw_set_neighbors(PageId::INVALID, PageId(2), 0, 0, 0, vec![3]).is_err()
        );
        assert!(
            WalRecord::hnsw_set_neighbors(PageId(1), PageId::INVALID, 0, 0, 0, vec![3]).is_err()
        );
        assert!(WalRecord::hnsw_meta_update(PageId::INVALID, 0, 0).is_err());
        assert!(WalRecord::hnsw_node_tombstone(PageId(1), PageId::INVALID, 0, 0, 1).is_err());
        assert!(WalRecord::hnsw_dir_append(PageId::INVALID, 0, PageId(2), 0).is_err());
        assert!(WalRecord::hnsw_dir_link(PageId::INVALID, PageId(2)).is_err());
        assert!(WalRecord::hnsw_publish_live(PageId(1), PageId::INVALID, 0, 0, 1).is_err());
        // meta_page_id = INVALID rejected on both state-bit constructors
        // (2026-09-15 round 5 P1).
        assert!(WalRecord::hnsw_node_tombstone(PageId::INVALID, PageId(2), 0, 0, 1).is_err());
        assert!(WalRecord::hnsw_publish_live(PageId::INVALID, PageId(2), 0, 0, 1).is_err());
        // Tombstone/PublishLive: dim = 0 rejected like NodeInit's (2026-09-15
        // round 3 P1-2 — the state byte sits at 4·dim, dim = 0 would target
        // the vector's first byte).
        assert!(WalRecord::hnsw_node_tombstone(PageId(1), PageId(2), 0, 0, 0).is_err());
        assert!(WalRecord::hnsw_publish_live(PageId(1), PageId(2), 0, 0, 0).is_err());
        // DirAppend: invalid target page. DirLink: invalid/self next page.
        assert!(WalRecord::hnsw_dir_append(PageId(100), 0, PageId::INVALID, 0).is_err());
        assert!(WalRecord::hnsw_dir_link(PageId(100), PageId::INVALID).is_err());
        assert!(WalRecord::hnsw_dir_link(PageId(100), PageId(100)).is_err());
        // NodeId::INVALID (u32::MAX) rejected as a real node identity
        // (2026-09-12 round 2 P2-3): node / owner / neighbor / tombstone /
        // dir-append / publish-live all gate it.
        assert!(
            WalRecord::hnsw_node_init(PageId(1), PageId(2), 0, u32::MAX, 0, 1, vec![1.0]).is_err()
        );
        assert!(
            WalRecord::hnsw_set_neighbors(PageId(1), PageId(2), 0, u32::MAX, 0, vec![3]).is_err()
        );
        assert!(
            WalRecord::hnsw_set_neighbors(PageId(1), PageId(2), 0, 0, 0, vec![3, u32::MAX])
                .is_err()
        );
        assert!(WalRecord::hnsw_node_tombstone(PageId(1), PageId(2), 0, u32::MAX, 1).is_err());
        assert!(WalRecord::hnsw_dir_append(PageId(100), u32::MAX, PageId(2), 0).is_err());
        assert!(WalRecord::hnsw_publish_live(PageId(1), PageId(2), 0, u32::MAX, 1).is_err());
        // MetaUpdate: NodeId::INVALID is legal ONLY for the empty graph
        // (max_level = 0); any nonzero level contradicts "no node".
        assert!(WalRecord::hnsw_meta_update(PageId(1), u32::MAX, 1).is_err());
        assert!(WalRecord::hnsw_meta_update(PageId(1), u32::MAX, 0).is_ok());
    }

    /// Stage 0 round 3 P2: the bounded decoders reject forged length
    /// prefixes at the PRE-DECODE gate (claimed length vs remaining bytes,
    /// checked before any Vec allocation) and reject any trailing byte.
    /// Round 2 pinned only "errors, eventually" — serde's cautious
    /// size_hint would still pre-allocate up to 1 MiB first; the gate makes
    /// the bound structural.
    #[test]
    fn hnsw_bounded_decoders_reject_forged_lengths_and_trailing_bytes() {
        // Forged huge vector count in bincode VARINT layout (standard
        // config: u64 counts > u32::MAX encode as marker 0xFD + 8B LE):
        // tiny fixed prefix (one-byte varints) + count = 2^40 + no data.
        let mut forged = vec![1u8, 10, 3, 42, 2, 4]; // meta/page/slot/node/level/dim
        forged.push(0xFD);
        forged.extend_from_slice(&(1u64 << 40).to_le_bytes());
        let err = HnswNodeInitRecord::decode(&forged).unwrap_err();
        assert!(
            err.to_string().contains("claims"),
            "forged length must fail at the pre-decode gate: {err}"
        );
        // Round 4 P2-2: the SetNeighbors arm gets the same gate-point
        // assertion — is_err() alone cannot prove the rejection happened
        // pre-allocation.
        let err = HnswSetNeighborsRecord::decode(&forged).unwrap_err();
        assert!(
            err.to_string().contains("claims"),
            "forged length must fail at the pre-decode gate: {err}"
        );

        // Round 4 P3-1: a legal-size but semantically false payload (wire
        // length disagrees with the declared dim/count) is rejected at the
        // gate, pre-allocation. Flip the declared dim byte (prefix field
        // #6, one-byte varint for small values) of an honest payload.
        let mut mismatched =
            WalRecord::hnsw_node_init(PageId(1), PageId(10), 3, 42, 2, 3, vec![1.0; 3])
                .unwrap()
                .payload;
        mismatched[5] = 4; // declared dim 3 -> 4, wire length stays 3
        let err = HnswNodeInitRecord::decode(&mismatched).unwrap_err();
        assert!(
            err.to_string().contains("!= declared"),
            "dim/wire mismatch must fail at the pre-decode gate: {err}"
        );
        let mut mismatched =
            WalRecord::hnsw_set_neighbors(PageId(1), PageId(11), 4, 42, 0, vec![1, 5, 9])
                .unwrap()
                .payload;
        mismatched[5] = 4; // declared count 3 -> 4, wire length stays 3
        let err = HnswSetNeighborsRecord::decode(&mismatched).unwrap_err();
        assert!(
            err.to_string().contains("!= declared"),
            "count/wire mismatch must fail at the pre-decode gate: {err}"
        );

        // Trailing byte: each decoder rejects its own honest payload + 1B.
        let with_tail = |mut p: Vec<u8>| {
            p.push(0);
            p
        };
        assert!(HnswNodeInitRecord::decode(&with_tail(
            WalRecord::hnsw_node_init(PageId(1), PageId(10), 3, 42, 2, 4, vec![1.0; 4])
                .unwrap()
                .payload
        ))
        .is_err());
        assert!(HnswSetNeighborsRecord::decode(&with_tail(
            WalRecord::hnsw_set_neighbors(PageId(1), PageId(11), 4, 42, 0, vec![1, 5, 9])
                .unwrap()
                .payload
        ))
        .is_err());
        assert!(HnswMetaUpdateRecord::decode(&with_tail(
            WalRecord::hnsw_meta_update(PageId(1), 42, 2)
                .unwrap()
                .payload
        ))
        .is_err());
        assert!(HnswNodeTombstoneRecord::decode(
            &with_tail(
                WalRecord::hnsw_node_tombstone(PageId(1), PageId(11), 4, 42, 4)
                    .unwrap()
                    .payload
            ),
            HNSW_STATE_V1_FLAGS
        )
        .is_err());
        assert!(HnswDirAppendRecord::decode(&with_tail(
            WalRecord::hnsw_dir_append(PageId(100), 42, PageId(11), 4)
                .unwrap()
                .payload
        ))
        .is_err());
        assert!(HnswDirLinkRecord::decode(&with_tail(
            WalRecord::hnsw_dir_link(PageId(100), PageId(101))
                .unwrap()
                .payload
        ))
        .is_err());
        assert!(HnswPublishLiveRecord::decode(
            &with_tail(
                WalRecord::hnsw_publish_live(PageId(1), PageId(11), 4, 42, 4)
                    .unwrap()
                    .payload
            ),
            HNSW_STATE_V1_FLAGS
        )
        .is_err());

        // Honest payloads still decode through the bounded API.
        let good = WalRecord::hnsw_publish_live(PageId(1), PageId(11), 4, 42, 4).unwrap();
        assert!(HnswPublishLiveRecord::decode(&good.payload, good.flags).is_ok());
    }

    /// Stage A review round 5 P2: the state-bit records' version nibble is
    /// enforced at decode — version 0 (pre-versioning development format,
    /// never shipped) and unknown future versions are both loud errors.
    #[test]
    fn hnsw_state_records_enforce_the_version_nibble() {
        let rec = WalRecord::hnsw_node_tombstone(PageId(1), PageId(11), 4, 42, 4).unwrap();
        // v1 decodes.
        assert!(HnswNodeTombstoneRecord::decode(&rec.payload, HNSW_STATE_V1_FLAGS).is_ok());
        // flags = 0: superseded development format — loud rejection.
        let err = HnswNodeTombstoneRecord::decode(&rec.payload, 0).unwrap_err();
        assert!(
            err.to_string()
                .contains("pre-versioning development format"),
            "{err}"
        );
        // Unknown future nibble: loud, never silently mis-decoded.
        assert!(HnswNodeTombstoneRecord::decode(&rec.payload, 2 << 4).is_err());

        let rec = WalRecord::hnsw_publish_live(PageId(1), PageId(11), 4, 42, 4).unwrap();
        assert!(HnswPublishLiveRecord::decode(&rec.payload, HNSW_STATE_V1_FLAGS).is_ok());
        assert!(HnswPublishLiveRecord::decode(&rec.payload, 0).is_err());
        assert!(HnswPublishLiveRecord::decode(&rec.payload, 2 << 4).is_err());
        // The other five HNSW types keep flags = 0 (implicit original
        // version, HNSW_STATE_VERSION_V1's umbrella note).
        let rec = WalRecord::hnsw_dir_append(PageId(100), 42, PageId(11), 4).unwrap();
        assert_eq!(rec.flags, 0);
    }

    #[test]
    fn heap_insert_roundtrip() {
        let mut record = WalRecord::heap_insert(PageId(7), 3, vec![1, 2, 3, 4], TxnId(42)).unwrap();
        record.lsn = Lsn(64);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::HeapInsert);
        assert_eq!(decoded.txn_id, TxnId(42));
        let payload: HeapInsertRecord =
            bincode::serde::decode_from_slice(&decoded.payload, bincode_config())
                .unwrap()
                .0;
        assert_eq!(payload.page_id, PageId(7));
        assert_eq!(payload.slot_id, 3);
        assert_eq!(payload.tuple_bytes, vec![1, 2, 3, 4]);
    }

    #[test]
    fn heap_update_roundtrip() {
        let old_tid = Tid {
            page_id: PageId(7),
            slot_id: 1,
        };
        let new_tid = Tid {
            page_id: PageId(7),
            slot_id: 2,
        };
        let mut record =
            WalRecord::heap_update(old_tid, new_tid, TxnId(9), vec![5, 6, 7], TxnId(9)).unwrap();
        record.lsn = Lsn(72);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::HeapUpdate);
        assert_eq!(decoded.txn_id, TxnId(9));
        let payload: HeapUpdateRecord =
            bincode::serde::decode_from_slice(&decoded.payload, bincode_config())
                .unwrap()
                .0;
        assert_eq!(payload.old_tid, old_tid);
        assert_eq!(payload.new_tid, new_tid);
        assert_eq!(payload.xmax_old, TxnId(9));
        assert_eq!(payload.new_tuple_bytes, vec![5, 6, 7]);
    }

    #[test]
    fn heap_delete_roundtrip() {
        let tid = Tid {
            page_id: PageId(7),
            slot_id: 4,
        };
        let mut record = WalRecord::heap_delete(tid, TxnId(11), TxnId(11)).unwrap();
        record.lsn = Lsn(80);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::HeapDelete);
        assert_eq!(decoded.txn_id, TxnId(11));
        let payload: HeapDeleteRecord =
            bincode::serde::decode_from_slice(&decoded.payload, bincode_config())
                .unwrap()
                .0;
        assert_eq!(payload.tid, tid);
        assert_eq!(payload.xmax, TxnId(11));
    }

    /// Post-Stage-S review B5: the Stage S record types roundtrip too.
    #[test]
    fn heap_hot_update_roundtrip() {
        let mut record =
            WalRecord::heap_hot_update(PageId(7), 3, 9, vec![8, 8, 8], TxnId(21), TxnId(21))
                .unwrap();
        record.lsn = Lsn(88);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::HeapHotUpdate);
        assert_eq!(decoded.txn_id, TxnId(21));
        let payload = HeapHotUpdateRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.page_id, PageId(7));
        assert_eq!(payload.old_slot, 3);
        assert_eq!(payload.new_slot, 9);
        assert_eq!(payload.new_tuple_bytes, vec![8, 8, 8]);
        assert_eq!(payload.xmax, TxnId(21));
    }

    /// Post-Stage-S review B5: both CLR shapes — a finishing CLR (parent
    /// downlink) and an unlink CLR (INVALID parent/new_root/meta) — encode
    /// and decode losslessly.
    #[test]
    fn btree_split_clr_roundtrip() {
        let finishing = BTreeSplitCLRRecord {
            left_page: PageId(10),
            right_page: PageId(11),
            level: 0,
            copy_start_slot: 113,
            redo_ref_lsn: Lsn(4_200),
            parent_page: PageId(12),
            separator_key: vec![1, 2, 3, 4],
            parent_insert_slot: 57,
            new_root_page: PageId::INVALID,
            meta_page: PageId::INVALID,
        };
        let unlink = BTreeSplitCLRRecord {
            left_page: PageId(10),
            right_page: PageId(11),
            level: 0,
            copy_start_slot: 113,
            redo_ref_lsn: Lsn::INVALID,
            parent_page: PageId::INVALID,
            separator_key: Vec::new(),
            parent_insert_slot: 0,
            new_root_page: PageId::INVALID,
            meta_page: PageId::INVALID,
        };
        for rec in [finishing, unlink] {
            let mut record = WalRecord::btree_split_clr(&rec).unwrap();
            record.lsn = Lsn(9_999);
            let buf = record.encode().unwrap();
            let (decoded, _) = WalRecord::decode(&buf).unwrap();
            assert_eq!(decoded.record_type, WalRecordType::BTreeSplitCLR);
            assert_eq!(decoded.lsn, Lsn(9_999));
            let payload = BTreeSplitCLRRecord::decode(&decoded.payload).unwrap();
            assert_eq!(payload, rec);
        }
    }

    /// Post-Stage-S fix B5: a full decode rejects a separator key beyond
    /// [`MAX_CLR_SEPARATOR_KEY_BYTES`] (defense in depth — a corrupt length
    /// prefix on a CRC-valid record must not be trusted), while a key at the
    /// bound still roundtrips.
    #[test]
    fn btree_split_clr_decode_bounds_separator_key() {
        let base = BTreeSplitCLRRecord {
            left_page: PageId(10),
            right_page: PageId(11),
            level: 0,
            copy_start_slot: 113,
            redo_ref_lsn: Lsn(4_200),
            parent_page: PageId(12),
            parent_insert_slot: 57,
            new_root_page: PageId::INVALID,
            meta_page: PageId::INVALID,
            separator_key: Vec::new(),
        };
        let at_bound = BTreeSplitCLRRecord {
            separator_key: vec![0xAA; MAX_CLR_SEPARATOR_KEY_BYTES],
            ..base.clone()
        };
        let payload = bincode::serde::encode_to_vec(&at_bound, bincode_config()).unwrap();
        assert_eq!(
            BTreeSplitCLRRecord::decode(&payload).unwrap(),
            at_bound,
            "a separator key at the bound must decode"
        );
        let over_bound = BTreeSplitCLRRecord {
            separator_key: vec![0xAA; MAX_CLR_SEPARATOR_KEY_BYTES + 1],
            ..base
        };
        let payload = bincode::serde::encode_to_vec(&over_bound, bincode_config()).unwrap();
        assert!(
            BTreeSplitCLRRecord::decode(&payload).is_err(),
            "a separator key past the bound must be rejected"
        );
    }

    #[test]
    fn txn_commit_roundtrip() {
        let mut record = WalRecord::txn_commit(TxnId(17)).unwrap();
        record.lsn = Lsn(88);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::TxnCommit);
        assert_eq!(decoded.txn_id, TxnId(17));
        let payload = TxnCommitRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.xid, TxnId(17));
    }

    #[test]
    fn txn_abort_roundtrip() {
        let mut record = WalRecord::txn_abort(TxnId(19)).unwrap();
        record.lsn = Lsn(96);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::TxnAbort);
        assert_eq!(decoded.txn_id, TxnId(19));
        let payload = TxnAbortRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.xid, TxnId(19));
    }

    #[test]
    fn btree_insert_roundtrip() {
        let mut record =
            WalRecord::btree_insert(PageId(7), 3, 0, 1, vec![1, 2, 3, 4, 9, 9]).unwrap();
        record.lsn = Lsn(64);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::BTreeInsert);
        let payload = BTreeInsertRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.page_id, PageId(7));
        assert_eq!(payload.slot_id, 3);
        assert_eq!(payload.level, 0);
        assert_eq!(payload.flags, 1);
        assert_eq!(payload.tuple_bytes, vec![1, 2, 3, 4, 9, 9]);
    }

    #[test]
    fn btree_delete_roundtrip() {
        let mut record = WalRecord::btree_delete(PageId(7), 5).unwrap();
        record.lsn = Lsn(72);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::BTreeDelete);
        let payload = BTreeDeleteRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.page_id, PageId(7));
        assert_eq!(payload.slot_id, 5);
    }

    #[test]
    fn btree_split_prepare_roundtrip() {
        let mut record =
            WalRecord::btree_split_prepare(PageId(7), PageId(8), 1, PageId(9), vec![0xAA, 0xBB])
                .unwrap();
        record.lsn = Lsn(80);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::BTreeSplitPrepare);
        let payload = BTreeSplitPrepareRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.left_page, PageId(7));
        assert_eq!(payload.new_right_page, PageId(8));
        assert_eq!(payload.level, 1);
        assert_eq!(payload.left_old_next, PageId(9));
        assert_eq!(payload.high_key_bytes, vec![0xAA, 0xBB]);
    }

    #[test]
    fn btree_split_copy_roundtrip() {
        let mut record = WalRecord::btree_split_copy(PageId(7), PageId(8), 42, Lsn(1_000)).unwrap();
        record.lsn = Lsn(88);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::BTreeSplitCopy);
        let payload = BTreeSplitCopyRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.left_page, PageId(7));
        assert_eq!(payload.right_page, PageId(8));
        assert_eq!(payload.copy_start_slot, 42);
        assert_eq!(payload.left_page_pre_lsn, Lsn(1_000));
    }

    #[test]
    fn btree_split_commit_roundtrip() {
        let mut record =
            WalRecord::btree_split_commit(PageId(7), PageId(8), PageId(9), vec![5, 6], 2).unwrap();
        record.lsn = Lsn(96);
        let buf = record.encode().unwrap();
        let (decoded, _) = WalRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::BTreeSplitCommit);
        let payload = BTreeSplitCommitRecord::decode(&decoded.payload).unwrap();
        assert_eq!(payload.left_page, PageId(7));
        assert_eq!(payload.right_page, PageId(8));
        assert_eq!(payload.parent_page, PageId(9));
        assert_eq!(payload.separator_key, vec![5, 6]);
        assert_eq!(payload.parent_insert_slot, 2);
    }

    #[test]
    fn decode_rejects_corrupted_crc() {
        let mut record = WalRecord::page_alloc(PageId(1)).unwrap();
        record.lsn = Lsn(16);
        let mut buf = record.encode().unwrap();
        buf[0] ^= 0xff; // corrupt the LSN
        assert!(WalRecord::decode(&buf).is_err());
    }

    #[test]
    fn decode_rejects_truncated_record() {
        let mut record = WalRecord::page_alloc(PageId(1)).unwrap();
        record.lsn = Lsn(16);
        let buf = record.encode().unwrap();
        assert!(WalRecord::decode(&buf[..buf.len() - 1]).is_err());
    }

    #[test]
    fn record_type_discriminants_are_stable() {
        assert_eq!(WalRecordType::CheckpointEnd.to_u8(), 31);
        assert_eq!(
            WalRecordType::from_u8(40).unwrap(),
            WalRecordType::PageAlloc
        );
    }

    proptest! {
        // Coding plan target is 10,000 cases. 1024 keeps normal CI fast while
        // still exercising the encoding paths thoroughly; set PROPTEST_CASES
        // environment variable to override.
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1024)
        ))]

        #[test]
        fn wal_record_roundtrip(
            lsn in 8u64..10_000u64,
            record_type in prop_oneof![
                Just(WalRecordType::PageAlloc),
                Just(WalRecordType::CheckpointBegin),
                Just(WalRecordType::CheckpointEnd),
                Just(WalRecordType::FullPageImage),
            ],
            page_id in 1u64..1000u64,
            checkpoint_lsn in 8u64..10_000u64,
            next_page_id in 1u64..1000u64,
            next_txn_id in 1u64..1000u64,
            image_seed in 0u8..=255u8,
        ) {
            let lsn = Lsn(lsn);
            let mut record = match record_type {
                WalRecordType::PageAlloc => WalRecord::page_alloc(PageId(page_id)).unwrap(),
                WalRecordType::CheckpointBegin => WalRecord::checkpoint_begin(),
                WalRecordType::CheckpointEnd => WalRecord::checkpoint_end(
                    Lsn(checkpoint_lsn),
                    PageId(next_page_id),
                    TxnId(next_txn_id),
                    crate::types::Oid::FIRST_USER.0,
                    String::new(),
                    String::new(),
                ).unwrap(),
                WalRecordType::FullPageImage => {
                    let image = vec![image_seed; PAGE_SIZE];
                    WalRecord::full_page_image(PageId(page_id), image).unwrap()
                }
                _ => unreachable!(),
            };
            record.lsn = lsn;

            let buf = record.encode().unwrap();
            prop_assert_eq!(buf.len() % 8, 0);

            let (decoded, consumed) = WalRecord::decode(&buf).unwrap();
            prop_assert_eq!(consumed, buf.len());
            prop_assert_eq!(decoded.lsn, lsn);
            prop_assert_eq!(decoded.record_type, record_type);
            prop_assert_eq!(decoded.payload, record.payload);
        }
    }
}
