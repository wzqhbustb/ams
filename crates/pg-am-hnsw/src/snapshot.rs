//! Snapshot `save`/`load` file API (tech-selection §7) — **Stage C
//! deliverable**.
//!
//! Stage A provides the byte-stream layer in [`crate::encoding`] (frozen
//! format §3, CRC32 prefix §7, full load-validation checklist); this module
//! will add the file-level API on top once Stage B freezes the graph
//! structure. Round-trip equivalence (`save → load → search` identical to
//! the in-memory original) is M4's crash-injection surrogate (§9).
//!
//! Naming note: types here deliberately avoid the bare name `Snapshot` — the
//! CI snapshot-construction guardrail greps every crate except pg-txn for
//! literal-construction and impl-block patterns on that name (coding plan
//! Stage A), so composite names ([`crate::encoding::SnapshotHeader`],
//! [`crate::encoding::SnapshotFileData`]) are used throughout.
//!
//! Conservative default already decided (coding plan Stage C): a
//! snapshot-loaded graph rejects `insert` with
//! [`crate::HnswError::InvalidOperation`] — continuation-insert semantics
//! are an open question punted to tech-selection v1.6.
