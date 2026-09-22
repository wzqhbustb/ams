//! Stage J §3 P1-5: the commit hard-order guarantees that the in-memory CLOG
//! bit is flipped only *after* the `TxnCommit` record is durably fsynced. If
//! the WAL flush fails, the transaction must not be observable as committed.

use std::sync::Arc;

use parking_lot::Mutex;

use pg_storage::error::{Result, StorageError};
use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, TxnManager, TxnState};

/// A fake WAL that records calls and can be told to fail on `flush_to`.
/// Appends hand out strictly increasing LSNs so the hard-order tests exercise
/// real LSN propagation (append's LSN must reach flush_to verbatim).
#[derive(Debug, Default)]
struct FaultWal {
    fail_flush: bool,
    appended: Mutex<usize>,
    flushed: Mutex<usize>,
    next_lsn: Mutex<u64>,
    last_flush_lsn: Mutex<Option<Lsn>>,
}

impl FaultWal {
    fn new(fail_flush: bool) -> Self {
        Self {
            fail_flush,
            next_lsn: Mutex::new(Lsn::FIRST.0),
            ..Default::default()
        }
    }
}

impl CommitWal for FaultWal {
    fn append(&self, _record: WalRecord) -> Result<Lsn> {
        *self.appended.lock() += 1;
        let mut next = self.next_lsn.lock();
        let lsn = Lsn(*next);
        *next += 8; // LSN_ALIGNMENT-sized steps keep LSNs strictly increasing
        Ok(lsn)
    }

    fn flush_to(&self, lsn: Lsn) -> Result<()> {
        *self.flushed.lock() += 1;
        *self.last_flush_lsn.lock() = Some(lsn);
        if self.fail_flush {
            Err(StorageError::WalWriteFailed(
                "injected flush failure".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[test]
fn commit_flips_clog_only_after_durable_flush() {
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal_impl = Arc::new(FaultWal::new(false));
    let wal: Arc<dyn CommitWal> = wal_impl.clone();
    let mgr = TxnManager::new(
        TxnIdClock::new(TxnId::FIRST),
        Arc::clone(&wal),
        Arc::clone(&clog),
    );

    let xid = mgr.begin_txn();
    // Before commit the XID is in progress and active.
    assert_eq!(clog.get_state(xid), TxnState::InProgress);
    assert_eq!(mgr.active_xids(), vec![xid]);

    mgr.commit_txn(xid).expect("commit succeeds");
    assert_eq!(clog.get_state(xid), TxnState::Committed);
    assert!(mgr.active_xids().is_empty());

    // The flush must run through the commit record's END boundary (start +
    // record_size): flush_to's prefix semantics early-exit on a record's
    // own start LSN right after a wave/reopen (writer.rs flush_to rustdoc),
    // so the hard order depends on the end-boundary target being exact.
    let commit_len = WalRecord::txn_commit(xid).unwrap().record_size() as u64;
    assert_eq!(
        *wal_impl.last_flush_lsn.lock(),
        Some(Lsn(Lsn::FIRST.0 + commit_len))
    );
}

#[test]
fn commit_does_not_flip_clog_when_flush_fails() {
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal_impl = Arc::new(FaultWal::new(true));
    let wal: Arc<dyn CommitWal> = wal_impl.clone();
    let mgr = TxnManager::new(TxnIdClock::new(TxnId::FIRST), wal, Arc::clone(&clog));

    let xid = mgr.begin_txn();
    let err = mgr.commit_txn(xid).expect_err("flush failure propagates");
    assert!(matches!(err, StorageError::WalWriteFailed(_)));

    // Hard-order: record appended + flush attempted, but the CLOG bit was NOT
    // flipped and the XID stays active (never observable as committed).
    assert_eq!(*wal_impl.appended.lock(), 1);
    assert_eq!(*wal_impl.flushed.lock(), 1);
    assert_eq!(clog.get_state(xid), TxnState::InProgress);
    assert_eq!(mgr.active_xids(), vec![xid]);
}

#[test]
fn abort_does_not_flip_clog_when_flush_fails() {
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::new(FaultWal::new(true));
    let mgr = TxnManager::new(TxnIdClock::new(TxnId::FIRST), wal, Arc::clone(&clog));

    let xid = mgr.begin_txn();
    let err = mgr.abort_txn(xid).expect_err("flush failure propagates");
    assert!(matches!(err, StorageError::WalWriteFailed(_)));
    assert_eq!(clog.get_state(xid), TxnState::InProgress);
}

#[test]
fn abort_flips_clog_to_aborted_on_success() {
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::new(FaultWal::new(false));
    let mgr = TxnManager::new(TxnIdClock::new(TxnId::FIRST), wal, Arc::clone(&clog));

    let xid = mgr.begin_txn();
    mgr.abort_txn(xid).expect("abort succeeds");
    assert_eq!(clog.get_state(xid), TxnState::Aborted);
    assert!(mgr.active_xids().is_empty());
}
