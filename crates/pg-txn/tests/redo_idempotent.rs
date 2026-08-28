//! Txn redo handler idempotency (Stage J).
//!
//! Recovery may re-run any prefix of WAL records after a crash *during*
//! recovery, so every handler must be safe to apply repeatedly. For the txn
//! handlers idempotency means: applying the same `TxnCommit`/`TxnAbort`
//! record any number of times leaves the CLOG in exactly the same state.

use std::sync::Arc;

use parking_lot::Mutex;

use pg_storage::config::StorageConfig;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::recovery::{ActiveXactTable, DirtyPageTable, IncompleteSplitTracker, RedoContext};
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;
use pg_txn::{txn_redo_handlers, ClogAccessor, InMemoryClogAccessor, TxnState};

#[test]
fn txn_redo_handlers_are_idempotent_under_repeated_apply() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
    let allocator = Arc::new(Mutex::new(
        PageAllocator::open(tmp.path(), &config, Arc::clone(&wal)).unwrap(),
    ));

    let clog = InMemoryClogAccessor::new();
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    let mut incomplete_splits = IncompleteSplitTracker::new();
    let mut ctx = RedoContext {
        buffer_pool: None, // txn handlers only touch the CLOG
        page_allocator: &allocator,
        clog: &clog,
        att: &mut att,
        dpt: &mut dpt,
        incomplete_splits: &mut incomplete_splits,
    };

    let handlers = txn_redo_handlers();
    let commit = WalRecord::txn_commit(TxnId(7)).unwrap();
    let abort = WalRecord::txn_abort(TxnId(8)).unwrap();

    // Apply each record 10 times; the handlers must not error and the CLOG
    // must not change after the first apply.
    for _ in 0..10 {
        for h in &handlers {
            if h.kind() == commit.record_type {
                h.apply(&commit, &mut ctx).unwrap();
            }
            if h.kind() == abort.record_type {
                h.apply(&abort, &mut ctx).unwrap();
            }
        }
        assert_eq!(clog.get_state(TxnId(7)), TxnState::Committed);
        assert_eq!(clog.get_state(TxnId(8)), TxnState::Aborted);
    }

    // The LSN on the record is irrelevant to the txn handlers (CLOG state is
    // LSN-free); a second copy at a different LSN is equally idempotent.
    let mut commit_later = WalRecord::txn_commit(TxnId(7)).unwrap();
    commit_later.lsn = Lsn(9_999_999);
    for h in &handlers {
        if h.kind() == commit_later.record_type {
            h.apply(&commit_later, &mut ctx).unwrap();
        }
    }
    assert_eq!(clog.get_state(TxnId(7)), TxnState::Committed);
}
