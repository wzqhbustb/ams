//! HNSW redo-handler registry — Phase 2 M5 Stage 0 skeleton.
//!
//! `Engine::open` registers these handlers alongside heap/txn/btree
//! (pg-engine/src/engine.rs:687-689 extension point, tech-selection §2).
//! Stage 0 returns an EMPTY vector: the seven handler bodies (one per
//! discriminant 121–127) land in Stage C. Registering an empty skeleton now
//! keeps the wiring — dependency edge, registration call site, and CI
//! compilation — real from day one, so Stage C only fills bodies (coding
//! plan v1.3 §阶段 0 依赖边落地行).

use pg_storage::recovery::RedoHandler;

/// All HNSW redo handlers (Stage 0: none yet — Stage C fills the seven
/// bodies). An HNSW record replayed before Stage C therefore fails as
/// unknown/unhandled, which is the intended loud behavior: no M5 data
/// exists yet.
pub fn hnsw_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    Vec::new()
}
