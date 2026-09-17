//! HNSW redo-handler registry — Phase 2 M5 Stage 0 skeleton.
//!
//! `Engine::open` registers these handlers alongside heap/txn/btree
//! (pg-engine/src/engine.rs:692-693 extension point, tech-selection §2).
//! Stage 0 returns an EMPTY vector: the seven handler bodies (one per
//! discriminant 121–127) land in Stage C. Registering an empty skeleton now
//! keeps the wiring — dependency edge, registration call site, and CI
//! compilation — real from day one, so Stage C only fills bodies (coding
//! plan v1.3 §阶段 0 依赖边落地行).

use pg_storage::recovery::RedoHandler;

/// All HNSW redo handlers (Stage 0: none yet — Stage C fills the seven
/// bodies). An HNSW record replayed before Stage C therefore fails as
/// unknown/unhandled, which is loud BY DESIGN — but the original premise
/// "no M5 data exists yet" no longer holds (2026-09-16, mainline Stage B
/// review round 2 P2-1): Stage B's open-time meta repair CAN write an
/// `HnswMetaUpdate` (123) into a live directory's WAL. Consequence,
/// registered as the Stage B interim limitation: once a repair has fired,
/// an engine reopen WITHOUT an intervening checkpoint hard-fails redo on
/// the handlerless 123 record (`UnknownRecord`) — fail-loud, never data
/// loss (the directory opens again as soon as Stage C's handlers land,
/// and a checkpoint after the repair moves the record out of the replay
/// window). Stage C's first task closes this.
pub fn hnsw_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    Vec::new()
}
