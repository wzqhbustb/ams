//! M2b Stage L acceptance: the six §7.2 verification cases against the full
//! [`PgVisibilityOracle`], plus the curcid increment protocol test.
//!
//! Every case hand-builds the [`Snapshot`] (`xmin / xmax / xip / current_xid /
//! curcid`) and the tuple header fields (`xmin / xmax / t_cid`) exactly as the
//! §7.2 case table prescribes, over an [`InMemoryClogAccessor`]. Snapshots are
//! constructed per statement with the `curcid` the executor would have
//! advanced to (§7.1 Q4: +1 before each statement starts).

use std::sync::Arc;

use smallvec::SmallVec;

use pg_storage::clog::ClogAccessor;
use pg_storage::types::TxnId;
use pg_txn::{
    InMemoryClogAccessor, PgVisibilityOracle, Snapshot, TxnState, Visibility, VisibilityOracle,
};

/// Build an oracle over a fresh in-memory CLOG, returning both so the test can
/// record commit/abort states.
fn oracle_and_clog() -> (PgVisibilityOracle, Arc<InMemoryClogAccessor>) {
    let clog = Arc::new(InMemoryClogAccessor::new());
    let oracle = PgVisibilityOracle::new(clog.clone());
    (oracle, clog)
}

/// Hand-built snapshot: `xmin` is the smallest `xip` entry (or `xmax` when
/// empty), matching `TxnManager::snapshot`. Uses pg-txn's unregistered
/// test constructor — these snapshots exercise the oracle directly and
/// never touch a `TxnManager`, so the horizon registry is irrelevant here.
fn snap(current_xid: TxnId, xmax: TxnId, xip: &[TxnId], curcid: u32) -> Snapshot {
    let xmin = xip.iter().copied().min().unwrap_or(xmax);
    Snapshot::new_unregistered(
        xmin,
        xmax,
        xip.iter().copied().collect::<SmallVec<[TxnId; 32]>>(),
        current_xid,
        curcid,
    )
}

/// Case 1 (§7.2): `BEGIN(T1) → INSERT r1(cid=1) → curcid=2 →
/// DELETE r1(xmax=T1, cid=2) → curcid=3 → SELECT` must **not** return r1.
///
/// At the SELECT: `xmin=T1, t_cid=2 < curcid=3` → xmin 见 (earlier command);
/// `xmax=T1, t_cid=2 < curcid=3` → deleted by an earlier command → 不见.
#[test]
fn case1_self_delete_by_prior_command_is_invisible() {
    let (oracle, _clog) = oracle_and_clog();
    let t1 = TxnId(10);

    // Sanity: between INSERT and DELETE (curcid=2), the live tuple is visible.
    let during_insert = snap(t1, TxnId(11), &[t1], 2);
    assert_eq!(
        oracle.is_visible(t1, TxnId::INVALID, 1, &during_insert),
        Visibility::Visible,
        "own earlier-command insert is visible"
    );

    // After the DELETE, the tuple header is (xmin=T1, xmax=T1) and t_cid
    // carries the deleting command's cid (=2).
    let select = snap(t1, TxnId(11), &[t1], 3);
    assert_eq!(
        oracle.is_visible(t1, t1, 2, &select),
        Visibility::Invisible,
        "deleted by a prior command of the same transaction"
    );
}

/// Case 2 (§7.2): `INSERT r1(cid=1) → 同语句 RETURNING` — with `curcid` still
/// 1, the xmin branch takes the `t_cid < curcid == false` else path → 不见.
///
/// This is intentional and harmless: INSERT ... RETURNING emits the row
/// through the INSERT output channel, never through a scan `is_visible` call.
#[test]
fn case2_same_statement_insert_invisible_to_own_scan() {
    let (oracle, _clog) = oracle_and_clog();
    let t1 = TxnId(10);
    let same_statement = snap(t1, TxnId(11), &[t1], 1);
    assert_eq!(
        oracle.is_visible(t1, TxnId::INVALID, 1, &same_statement),
        Visibility::Invisible,
        "t_cid == curcid: own current-command write is hidden from its own scan"
    );
}

/// Case 3 (§7.2): `INSERT r1(cid=1) → DELETE r1(cid=1) → 同语句 RETURNING`.
///
/// Same-statement insert-then-delete: the tuple header is
/// `(xmin=T1, xmax=T1, t_cid=1)` with `curcid=1`, so the xmin branch's else
/// path already answers 不见 (the xmax branch is never reached). As in case 2
/// this does not affect correctness: DELETE ... RETURNING emits the row
/// through the DELETE output channel, not through a scan.
#[test]
fn case3_same_statement_insert_delete_invisible_to_own_scan() {
    let (oracle, _clog) = oracle_and_clog();
    let t1 = TxnId(10);
    let same_statement = snap(t1, TxnId(11), &[t1], 1);
    assert_eq!(
        oracle.is_visible(t1, t1, 1, &same_statement),
        Visibility::Invisible,
        "xmin branch preempts: current-command write never enters the scan"
    );
}

/// Case 4 (§7.2): `BEGIN(T1) → UPDATE r1 SET ...(cid=1)` — Halloween
/// protection. r1 was inserted earlier by T1 itself, so the old version is
/// `(xmin=T1, xmax=T1, t_cid=1)` and the new version is
/// `(xmin=T1, xmax=INVALID, t_cid=1)`. With `curcid=1`, **both** versions are
/// invisible to the same statement's scan: the xmin branch hides the new
/// version outright, and it also preempts on the old version, so the
/// statement can never re-update the row it just wrote.
#[test]
fn case4_update_halloween_protection_skips_both_versions() {
    let (oracle, _clog) = oracle_and_clog();
    let t1 = TxnId(10);
    let same_statement = snap(t1, TxnId(11), &[t1], 1);

    let old_version = oracle.is_visible(t1, t1, 1, &same_statement);
    let new_version = oracle.is_visible(t1, TxnId::INVALID, 1, &same_statement);
    assert_eq!(old_version, Visibility::Invisible, "old version skipped");
    assert_eq!(new_version, Visibility::Invisible, "new version skipped");
}

/// Case 5 (§7.2): `BEGIN(T1) → DELETE r1` (uncommitted); another transaction
/// T2's SELECT **returns** r1: `xmax=T1` is in T2's `xip` → 并发未提交删除 →
/// 见.
#[test]
fn case5_uncommitted_foreign_delete_stays_visible() {
    let (oracle, clog) = oracle_and_clog();
    let t0 = TxnId(5); // original inserter, long committed
    let t1 = TxnId(10); // uncommitted deleter
    let t2 = TxnId(11); // concurrent reader
    clog.set_state(t0, TxnState::Committed);

    let t2_snap = snap(t2, TxnId(12), &[t1, t2], 1);
    assert_eq!(
        oracle.is_visible(t0, t1, 0, &t2_snap),
        Visibility::Visible,
        "xmax in xip: uncommitted delete does not hide the row"
    );
}

/// Case 6 (§7.2): `BEGIN(T1) → DELETE r1 → COMMIT; BEGIN(T2) → SELECT` must
/// **not** return r1: `xmax=T1` is committed and no longer in `xip` → the
/// delete takes effect.
#[test]
fn case6_committed_foreign_delete_is_invisible() {
    let (oracle, clog) = oracle_and_clog();
    let t0 = TxnId(5);
    let t1 = TxnId(10);
    let t2 = TxnId(11);
    clog.set_state(t0, TxnState::Committed);
    clog.set_state(t1, TxnState::Committed);

    // T1 committed before T2 began: xip is empty, xmin collapses to xmax.
    let t2_snap = snap(t2, TxnId(12), &[t2], 1);
    assert_eq!(
        oracle.is_visible(t0, t1, 0, &t2_snap),
        Visibility::Invisible,
        "committed delete hides the row"
    );
}

/// curcid 递增协议 (§7.1 Q4, coding-plan Stage L acceptance
/// `test_curcid_advance_on_statement_start`): the executor advances the
/// command counter **before** each statement starts; the counter is monotone
/// across statements and shared by every judgment within one statement.
#[test]
fn test_curcid_advance_on_statement_start() {
    let t1 = TxnId(10);
    let mut snap = snap(t1, TxnId(11), &[t1], 0);
    assert_eq!(snap.curcid(), 0, "fresh snapshot starts at curcid 0");

    // Statement 1 begins: advance once, then every is_visible in the
    // statement observes the same curcid (no per-call advance).
    let cid_stmt1 = snap.advance_curcid();
    assert_eq!(cid_stmt1, 1);
    let observed_a = snap.curcid();
    let observed_b = snap.curcid(); // a second judgment in the same statement
    assert_eq!(observed_a, observed_b, "same statement shares one curcid");

    // Statement 2 begins: monotone +1; statement 1's writes (t_cid=1) now
    // satisfy t_cid < curcid and become visible as an earlier command's.
    let cid_stmt2 = snap.advance_curcid();
    assert_eq!(cid_stmt2, 2);
    assert!(
        cid_stmt2 > cid_stmt1,
        "curcid is monotone across statements"
    );

    let (oracle, _clog) = oracle_and_clog();
    assert_eq!(
        oracle.is_visible(t1, TxnId::INVALID, 1, &snap),
        Visibility::Visible,
        "statement 2 sees statement 1's own write (t_cid=1 < curcid=2)"
    );
}
