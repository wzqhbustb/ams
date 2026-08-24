//! Integration tests for the `RedoRegistry` dispatch protocol (Stage D).
//!
//! Guards the two tech-selection invariants (§11.6 / v2.3-24): duplicate
//! registration panics, and an unregistered record type is a hard failure
//! rather than a silent skip.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pg_storage::sync::Mutex;

use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::error::StorageError;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::recovery::{
    ActiveXactTable, DirtyPageTable, IncompleteSplitTracker, RedoContext, RedoHandler, RedoRegistry,
};
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::{WalRecord, WalRecordType};
use pg_storage::wal::writer::WalWriter;

/// Collaborators needed to build a `RedoContext` in an integration test.
struct CtxParts {
    _tmp: tempfile::TempDir,
    allocator: Arc<Mutex<PageAllocator>>,
    _wal: Arc<WalWriter>,
    clog: NoOpClogAccessor,
    att: ActiveXactTable,
    dpt: DirtyPageTable,
    incomplete_splits: IncompleteSplitTracker,
}

impl CtxParts {
    fn new() -> Self {
        let tmp = tempfile::TempDir::new().unwrap();
        let cfg = StorageConfig::new(tmp.path());
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
        ));
        Self {
            _tmp: tmp,
            allocator,
            _wal: wal,
            clog: NoOpClogAccessor,
            att: ActiveXactTable::new(),
            dpt: DirtyPageTable::new(),
            incomplete_splits: IncompleteSplitTracker::new(),
        }
    }

    fn ctx(&mut self) -> RedoContext<'_> {
        RedoContext {
            buffer_pool: None,
            page_allocator: &self.allocator,
            clog: &self.clog,
            att: &mut self.att,
            dpt: &mut self.dpt,
            incomplete_splits: &mut self.incomplete_splits,
        }
    }
}

/// A test handler that records how many times (and at which LSNs) it fired.
struct CountingHandler {
    kind: WalRecordType,
    applies: Arc<AtomicUsize>,
    seen_lsns: Arc<Mutex<Vec<Lsn>>>,
}

impl RedoHandler for CountingHandler {
    fn kind(&self) -> WalRecordType {
        self.kind
    }

    fn apply(
        &self,
        record: &WalRecord,
        _ctx: &mut RedoContext<'_>,
    ) -> pg_storage::error::Result<()> {
        self.applies.fetch_add(1, Ordering::Relaxed);
        self.seen_lsns.lock().push(record.lsn);
        Ok(())
    }
}

fn record(kind: WalRecordType, lsn: u64) -> WalRecord {
    WalRecord {
        lsn: Lsn(lsn),
        prev_lsn: Lsn::INVALID,
        txn_id: TxnId::INVALID,
        record_type: kind,
        flags: 0,
        payload: Vec::new(),
    }
}

#[test]
fn test_redo_registry_duplicate_panics() {
    let attempt = std::panic::catch_unwind(|| {
        let mut registry = RedoRegistry::new();
        let mk = || CountingHandler {
            kind: WalRecordType::PageAlloc,
            applies: Arc::new(AtomicUsize::new(0)),
            seen_lsns: Arc::new(Mutex::new(Vec::new())),
        };
        registry.register(Box::new(mk()));
        registry.register(Box::new(mk()));
    });
    let err = attempt.expect_err("duplicate registration must panic");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .unwrap_or("");
    assert!(
        msg.contains("duplicate redo handler"),
        "unexpected panic message: {msg}"
    );
}

#[test]
fn unregistered_record_type_is_hard_failure() {
    let mut parts = CtxParts::new();
    let registry = RedoRegistry::new();

    let err = registry
        .apply(&record(WalRecordType::HeapDelete, 64), &mut parts.ctx())
        .unwrap_err();
    assert!(
        matches!(
            err,
            StorageError::UnknownRecord {
                record_type: 3,
                lsn: Lsn(64)
            }
        ),
        "expected UnknownRecord for HeapDelete, got {err:?}"
    );
}

#[test]
fn registered_handler_receives_each_record() {
    let mut parts = CtxParts::new();
    let applies = Arc::new(AtomicUsize::new(0));
    let seen_lsns = Arc::new(Mutex::new(Vec::new()));

    let mut registry = RedoRegistry::new();
    registry.register(Box::new(CountingHandler {
        kind: WalRecordType::TxnCommit,
        applies: Arc::clone(&applies),
        seen_lsns: Arc::clone(&seen_lsns),
    }));

    registry
        .apply(&record(WalRecordType::TxnCommit, 8), &mut parts.ctx())
        .unwrap();
    registry
        .apply(&record(WalRecordType::TxnCommit, 16), &mut parts.ctx())
        .unwrap();

    assert_eq!(applies.load(Ordering::Relaxed), 2);
    assert_eq!(*seen_lsns.lock(), vec![Lsn(8), Lsn(16)]);

    // A different type with no registered handler still hard-fails.
    assert!(registry
        .apply(&record(WalRecordType::TxnAbort, 24), &mut parts.ctx())
        .is_err());
}
