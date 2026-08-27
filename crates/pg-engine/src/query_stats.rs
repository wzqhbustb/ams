//! In-memory query statistics ring buffer (M3 Stage E, tech-selection §6.3).
//!
//! `QueryStats` is the M3 stand-in for `pg_stat_statements`: every
//! [`Engine::exec`](crate::Engine::exec) call records one entry — query
//! text, latency, rows affected/returned, execution path, timestamp — into
//! a fixed-capacity ring buffer. Overflow drops the OLDEST entry; process
//! restart loses everything. Both are accepted diagnostics semantics (§6.3
//! 代价): the ring exists to answer "which recent statements were slow",
//! not to persist history. Turning the stats into a system table is
//! Phase 6 — writing stats through heap/WAL/MVCC would be self-referential
//! (the stats table's own statements would produce stats) and would drag
//! M3 into catalog-machinery scope.
//!
//! Only the SQL text path is instrumented: the typed API
//! (`Engine::scan` / `insert` / `update` / `delete` / `index_lookup`) never
//! goes through `exec` and therefore produces NO entries (§6.3 另注 — the
//! acceptance criteria are phrased accordingly).

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;

use crate::sql::Statement;

/// Default [`EngineConfig::query_stats_capacity`](crate::EngineConfig::query_stats_capacity):
/// 1000 entries (tech-selection §6.3).
pub const DEFAULT_QUERY_STATS_CAPACITY: usize = 1000;

/// Maximum query-text bytes retained per entry (M3 Stage E review F3).
///
/// The ring stores the statement text verbatim; without a cap, a single
/// megabyte-long statement would be retained 1000 times over. Truncating
/// at 1 KiB bounds ring memory at roughly `capacity × 1 KiB` regardless of
/// statement length — plenty for "which recent statements were slow". The
/// cut falls back to a UTF-8 char boundary; truncation is not marked in
/// the stored text.
pub const MAX_QUERY_TEXT_BYTES: usize = 1024;

/// How a statement was executed (M3 Stage E, tech-selection §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPath {
    /// SELECT. The M3 SQL executor always seq-scans (`exec_select` →
    /// `scan_inner`); there is no index access path in the executor yet.
    SeqScan,
    /// Reserved for a future executor index path. The typed
    /// `Engine::index_lookup` is NOT instrumented (§6.3), so no entry can
    /// carry this variant today.
    IndexLookup,
    /// INSERT.
    Insert,
    /// UPDATE (its row lookup is a seq scan; the label reports the
    /// statement class, not the lookup method).
    Update,
    /// DELETE (same seq-scan-lookup note as `Update`).
    Delete,
    /// CREATE TABLE / CREATE INDEX.
    Ddl,
    /// BEGIN / COMMIT / ROLLBACK as SQL text. `exec` rejects these
    /// (transaction control is programmatic only), so entries with this
    /// path always record a failed statement (`rows` = 0).
    TxnControl,
}

impl ExecutionPath {
    /// Classify a parsed statement — the single classification source for
    /// the `exec` probe.
    pub(crate) fn of(stmt: &Statement) -> Self {
        match stmt {
            Statement::Select { .. } => Self::SeqScan,
            Statement::Insert { .. } => Self::Insert,
            Statement::Update { .. } => Self::Update,
            Statement::Delete { .. } => Self::Delete,
            Statement::CreateTable { .. } | Statement::CreateIndex { .. } => Self::Ddl,
            Statement::Begin | Statement::Commit | Statement::Rollback => Self::TxnControl,
        }
    }

    /// Stable lowercase label for diagnostics output.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SeqScan => "seq_scan",
            Self::IndexLookup => "index_lookup",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Ddl => "ddl",
            Self::TxnControl => "txn_control",
        }
    }
}

impl fmt::Display for ExecutionPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One recorded statement execution (M3 Stage E, tech-selection §6.3).
#[derive(Debug, Clone)]
pub struct QueryStatEntry {
    /// The SQL text as passed to `Engine::exec`, truncated to
    /// [`MAX_QUERY_TEXT_BYTES`] (F3 — bounds ring memory).
    pub query: String,
    /// Wall-clock latency of the whole `exec` call (parse + execution).
    pub latency: Duration,
    /// Rows returned (SELECT) or affected (INSERT/UPDATE/DELETE); 0 for
    /// DDL and failed statements.
    pub rows: usize,
    /// How the statement was executed.
    pub path: ExecutionPath,
    /// When the statement finished (wall clock; no monotonicity guarantee
    /// across entries).
    pub timestamp: SystemTime,
}

/// Fixed-capacity ring buffer of recent statement executions (M3 Stage E,
/// tech-selection §6.3).
///
/// Concurrency: one `parking_lot::Mutex` guards the ring — a single short
/// critical section per `exec` call (one push + at most one pop), trivial
/// against an execution path that does WAL appends and page I/O. §6.3
/// allows a sharded variant; the plain mutex is the M3-simple choice and
/// keeps the read API a consistent snapshot.
pub struct QueryStats {
    /// Configured capacity; immutable after construction.
    capacity: usize,
    /// Live entries, oldest first (overflow pops from the front).
    inner: Mutex<VecDeque<QueryStatEntry>>,
}

impl fmt::Debug for QueryStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryStats")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .finish()
    }
}

impl QueryStats {
    /// A ring holding at most `capacity` entries. Capacity 0 disables
    /// recording entirely (every `record` is dropped) — a legitimate
    /// "stats off" configuration, not an error.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Mutex::new(VecDeque::new()),
        }
    }

    /// Record one execution, dropping the OLDEST entry when the ring is
    /// full (§6.3 overflow semantics). No-op at capacity 0.
    ///
    /// The entry's query text is truncated to [`MAX_QUERY_TEXT_BYTES`]
    /// here — the single choke point, so no caller can bypass the F3
    /// memory bound.
    pub(crate) fn record(&self, mut entry: QueryStatEntry) {
        if self.capacity == 0 {
            return;
        }
        truncate_query_text(&mut entry.query);
        let mut entries = self.inner.lock();
        if entries.len() == self.capacity {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// All live entries, oldest → newest (the tests / diagnostics read
    /// API). The snapshot is internally consistent (taken under the ring
    /// lock); entries may be evicted immediately after it is taken.
    pub fn entries(&self) -> Vec<QueryStatEntry> {
        self.inner.lock().iter().cloned().collect()
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Whether the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// The configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Truncate `query` to at most [`MAX_QUERY_TEXT_BYTES`], backing off to a
/// UTF-8 char boundary (F3).
fn truncate_query_text(query: &mut String) {
    if query.len() > MAX_QUERY_TEXT_BYTES {
        let mut end = MAX_QUERY_TEXT_BYTES;
        while !query.is_char_boundary(end) {
            end -= 1;
        }
        query.truncate(end);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(query: &str) -> QueryStatEntry {
        QueryStatEntry {
            query: query.to_string(),
            latency: Duration::from_micros(1),
            rows: 0,
            path: ExecutionPath::SeqScan,
            timestamp: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn overflow_drops_oldest() {
        let stats = QueryStats::new(3);
        for i in 0..5 {
            stats.record(entry(&format!("q{i}")));
        }
        let entries = stats.entries();
        let queries: Vec<&str> = entries.iter().map(|e| e.query.as_str()).collect();
        assert_eq!(queries, ["q2", "q3", "q4"]);
    }

    #[test]
    fn capacity_zero_disables_recording() {
        let stats = QueryStats::new(0);
        stats.record(entry("q"));
        assert!(stats.is_empty());
        assert_eq!(stats.len(), 0);
    }

    /// F3 (Stage E review): over-long query text is truncated to the byte
    /// cap at a UTF-8 boundary, bounding ring memory.
    #[test]
    fn query_text_is_truncated_to_cap() {
        let stats = QueryStats::new(4);
        // 2 KiB of ASCII + a multi-byte tail straddling the cut point.
        let long = format!("{}中中中", "x".repeat(MAX_QUERY_TEXT_BYTES * 2));
        stats.record(entry(&long));
        let entries = stats.entries();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].query.len() <= MAX_QUERY_TEXT_BYTES);
        assert!(entries[0].query.is_char_boundary(entries[0].query.len()));
        // Short text is kept verbatim.
        stats.record(entry("SELECT 1"));
        assert_eq!(stats.entries()[1].query, "SELECT 1");
    }

    #[test]
    fn execution_path_labels_are_stable() {
        assert_eq!(ExecutionPath::SeqScan.as_str(), "seq_scan");
        assert_eq!(ExecutionPath::IndexLookup.to_string(), "index_lookup");
        assert_eq!(ExecutionPath::TxnControl.to_string(), "txn_control");
    }
}
