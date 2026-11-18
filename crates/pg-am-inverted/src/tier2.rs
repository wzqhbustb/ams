//! Tier 2 freshness and watermark contracts.

use pg_storage::types::Lsn;

/// Freshness state used by the inverted planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermark {
    /// In-process WAL prefix consumed by the index, including write buffer.
    pub consumed_lsn: Lsn,
    /// Durable WAL prefix recovered after segment metadata publication.
    pub durable_applied_lsn: Lsn,
}
