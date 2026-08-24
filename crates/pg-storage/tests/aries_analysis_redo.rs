//! Integration tests for the ARIES analysis phase and end-to-end
//! analysis + redo crash recovery (M2b Stage N acceptance; tech-selection
//! §11.1, §11.4).
//!
//! Covered here:
//!
//! - `test_analysis_redo_from_100k_wal` — the Stage N performance
//!   acceptance: analysis + redo over 100K WAL records completes well
//!   within 10s, with analysis timed separately from full recovery;
//! - ATT end-to-end: uncommitted XIDs land in the analysis ATT, committed
//!   and aborted ones do not, and a v2 checkpoint's ATT snapshot is
//!   consumed as the baseline;
//! - crash mid-checkpoint: a dangling `CheckpointBegin` (no `CheckpointEnd`)
//!   falls back to the previous completed checkpoint — or to `Lsn::FIRST`
//!   when no checkpoint ever completed.
//!
//! pg-storage tests cannot depend on pg-am-heap (it depends on pg-storage),
//! so `HeapInsert`/transaction redo is stubbed with counting/no-op
//! handlers; the heap AM's own crash-recovery tests cover real heap redo.

use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_storage::analysis;
use pg_storage::clog::ClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::recovery::{AttProvider, NoOpRedoHandler, RedoContext, RedoHandler};
use pg_storage::superblock::Superblock;
use pg_storage::types::{PageId, TxnId};
use pg_storage::wal::record::{WalRecord, WalRecordType};

/// Minimal in-memory `ClogAccessor` for recovery tests: every XID defaults
/// to `InProgress`; terminal states can be preset. Needed since Stage N
/// review P2-3, which filters the recovered ATT through the rebuilt CLOG —
/// under the `NoOp` CLOG every XID reads Committed and the ATT would
/// always come back empty.
#[derive(Debug, Default)]
struct MapClog {
    states: std::sync::Mutex<std::collections::HashMap<TxnId, pg_storage::clog::TxnState>>,
}

impl pg_storage::clog::ClogAccessor for MapClog {
    fn get_state(&self, xid: TxnId) -> pg_storage::clog::TxnState {
        self.states
            .lock()
            .unwrap()
            .get(&xid)
            .copied()
            .unwrap_or(pg_storage::clog::TxnState::InProgress)
    }

    fn set_state(&self, xid: TxnId, state: pg_storage::clog::TxnState) {
        self.states.lock().unwrap().insert(xid, state);
    }
}

/// No-op redo handler that counts how many records it applied — stands in
/// for the heap AM's handlers, which pg-storage tests cannot depend on.
struct CountingHandler {
    kind: WalRecordType,
    count: Arc<AtomicUsize>,
}

impl RedoHandler for CountingHandler {
    fn kind(&self) -> WalRecordType {
        self.kind
    }

    fn apply(
        &self,
        _record: &WalRecord,
        _ctx: &mut RedoContext<'_>,
    ) -> pg_storage::error::Result<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn counting_handler(kind: WalRecordType) -> (Box<dyn RedoHandler>, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    (
        Box::new(CountingHandler {
            kind,
            count: Arc::clone(&count),
        }),
        count,
    )
}

fn noop_handler(kind: WalRecordType) -> Box<dyn RedoHandler> {
    Box::new(NoOpRedoHandler::new(kind))
}

fn test_config(tmp: &tempfile::TempDir) -> StorageConfig {
    let mut config = StorageConfig::new(tmp.path());
    config.wal_group_commit_timeout_ms = 1;
    config.wal_group_commit_batch_size = 1;
    config
}

/// Stage N performance acceptance (coding plan: "Analysis + Redo 10 万
/// record ≤ 10s").
///
/// Layout: an initial checkpoint establishes the redo point, then 100K
/// PageAlloc/HeapInsert records plus one commit hit the WAL, then kill -9.
/// Recovery runs analysis (checkpoint scan + ATT/DPT rebuild) and redo over
/// all 100K records. Analysis is also timed on its own to demonstrate it
/// does not replay page contents (no handler dispatch, no payload
/// materialization) — it must be far cheaper than full recovery.
#[test]
fn test_analysis_redo_from_100k_wal() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);
    const RECORDS: u64 = 100_000;
    const PAGES: u64 = 1_000;

    let checkpoint_lsn;
    let content_page;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();

        // One real content page, made durable by the initial checkpoint.
        {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            content_page = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x5A;
        }
        checkpoint_lsn = engine.trigger_checkpoint().unwrap();

        // 100K records over a bounded page set (keeps the data file small:
        // PageAlloc redo extends the file to the highest page id). XID 1
        // stamps every HeapInsert and commits at the end, so the ATT must
        // come back empty.
        let wal = engine.wal_writer();
        for i in 0..RECORDS {
            let page_id = PageId(i / 2 % PAGES + 1);
            if i % 2 == 0 {
                wal.append(WalRecord::page_alloc(page_id).unwrap()).unwrap();
            } else {
                wal.append(WalRecord::heap_insert(page_id, 0, vec![0xAB; 32], TxnId(1)).unwrap())
                    .unwrap();
            }
        }
        wal.append(WalRecord::txn_commit(TxnId(1)).unwrap())
            .unwrap();
        wal.flush().unwrap();

        mem::forget(engine); // kill -9
    }

    // -- Analysis phase, timed on its own ---------------------------------
    let sb = Superblock::read(&Superblock::path(tmp.path())).unwrap();
    let analysis_start = Instant::now();
    let (end, _end_lsn) = analysis::find_latest_checkpoint_end(
        &tmp.path().join("wal"),
        config.wal_segment_size,
        sb.checkpoint_lsn,
    )
    .unwrap()
    .expect("the initial checkpoint's CheckpointEnd must be found");
    let result = analysis::run_analysis(tmp.path(), config.wal_segment_size, &end).unwrap();
    let analysis_elapsed = analysis_start.elapsed();

    assert_eq!(result.redo_start, checkpoint_lsn);
    assert!(result.att.is_empty(), "XID 1 committed before the crash");
    assert_eq!(result.dpt.len(), PAGES as usize);
    assert!(result.dpt.iter().all(|(_, lsn)| *lsn >= checkpoint_lsn));

    // -- Full recovery (analysis + redo), timed ---------------------------
    let recover_start = Instant::now();
    let (heap_handler, heap_count) = counting_handler(WalRecordType::HeapInsert);
    let engine = StorageEngine::open_with_redo_handlers(
        tmp.path(),
        &config,
        vec![heap_handler, noop_handler(WalRecordType::TxnCommit)],
        Vec::new(),
    )
    .unwrap();
    let recover_elapsed = recover_start.elapsed();

    println!(
        "Stage N perf ({} records): analysis = {:?}, full recovery (analysis + redo) = {:?}",
        RECORDS, analysis_elapsed, recover_elapsed
    );
    assert!(
        recover_elapsed <= Duration::from_secs(10),
        "analysis + redo over {RECORDS} records took {recover_elapsed:?} (> 10s budget)"
    );
    assert!(
        analysis_elapsed < recover_elapsed,
        "analysis ({analysis_elapsed:?}) must be far cheaper than redo-including \
         recovery ({recover_elapsed:?}) — it does not replay page contents"
    );

    // Correctness: every HeapInsert was redone exactly once, the allocator
    // covers the whole page set, and the checkpointed content survived.
    assert_eq!(heap_count.load(Ordering::Relaxed), (RECORDS / 2) as usize);
    assert!(engine.page_allocator().lock().next_page_id().0 > PAGES);
    let guard = engine.buffer_pool().pin(content_page).unwrap();
    assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0x5A);
    drop(guard);
    assert!(engine.recovered_active_xids().is_empty());

    // The task's other order — records → checkpoint → tail → crash — must
    // also recover cleanly (analysis starts at the new checkpoint and only
    // scans the tail).
    let checkpoint2 = engine.trigger_checkpoint().unwrap();
    assert!(checkpoint2 > checkpoint_lsn);
    engine
        .wal_writer()
        .append(WalRecord::heap_insert(PageId(1), 1, vec![0xCD; 16], TxnId(2)).unwrap())
        .unwrap();
    engine.wal_writer().flush().unwrap();
    mem::forget(engine);

    let (heap_handler2, _) = counting_handler(WalRecordType::HeapInsert);
    let restart = Instant::now();
    let engine = StorageEngine::open_with_redo_and_clog(
        tmp.path(),
        &config,
        vec![heap_handler2, noop_handler(WalRecordType::TxnCommit)],
        Vec::new(),
        Arc::new(MapClog::default()),
    )
    .unwrap();
    let restart_elapsed = restart.elapsed();
    println!("post-checkpoint tail recovery = {restart_elapsed:?}");
    assert!(restart_elapsed <= Duration::from_secs(10));
    // XID 2's insert never committed: it belongs to the ATT.
    assert_eq!(engine.recovered_active_xids(), &[TxnId(2)]);
    let guard = engine.buffer_pool().pin(content_page).unwrap();
    assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0x5A);
}

/// ATT end-to-end: XIDs with in-flight records land in the analysis ATT;
/// committing or aborting removes them.
#[test]
fn test_analysis_att_tracks_uncommitted_xids() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        engine.trigger_checkpoint().unwrap();
        let wal = engine.wal_writer();

        // Committed: XIDs 10..13 (records + TxnCommit).
        for xid in 10..13u64 {
            wal.append(WalRecord::heap_insert(PageId(1), 0, vec![1], TxnId(xid)).unwrap())
                .unwrap();
            wal.append(WalRecord::txn_commit(TxnId(xid)).unwrap())
                .unwrap();
        }
        // Aborted: XIDs 20..22 (records + TxnAbort).
        for xid in 20..22u64 {
            wal.append(WalRecord::heap_insert(PageId(2), 0, vec![2], TxnId(xid)).unwrap())
                .unwrap();
            wal.append(WalRecord::txn_abort(TxnId(xid)).unwrap())
                .unwrap();
        }
        // In-flight at the crash: XIDs 30, 31, 32 (records only).
        for xid in 30..33u64 {
            wal.append(WalRecord::heap_insert(PageId(3), 0, vec![3], TxnId(xid)).unwrap())
                .unwrap();
        }
        wal.flush().unwrap();
        mem::forget(engine); // kill -9
    }

    let (heap_handler, _) = counting_handler(WalRecordType::HeapInsert);
    let engine = StorageEngine::open_with_redo_and_clog(
        tmp.path(),
        &config,
        vec![
            heap_handler,
            noop_handler(WalRecordType::TxnCommit),
            noop_handler(WalRecordType::TxnAbort),
        ],
        Vec::new(),
        Arc::new(MapClog::default()),
    )
    .unwrap();

    assert_eq!(
        engine.recovered_active_xids(),
        &[TxnId(30), TxnId(31), TxnId(32)],
        "analysis ATT must hold exactly the XIDs that never reached a terminal record"
    );
}

/// The v2 checkpoint's ATT snapshot is consumed as the analysis baseline:
/// provider-reported in-flight XIDs survive a crash, minus the ones that
/// commit in the post-checkpoint tail.
#[test]
fn test_analysis_uses_att_snapshot_baseline() {
    #[derive(Debug)]
    struct StaticAttProvider(Vec<TxnId>);

    impl AttProvider for StaticAttProvider {
        fn active_xids(&self) -> Vec<TxnId> {
            self.0.clone()
        }
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        engine
            .checkpoint()
            .set_att_provider(Arc::new(StaticAttProvider(vec![TxnId(41), TxnId(42)])));
        engine.trigger_checkpoint().unwrap();
        // XID 41 commits after the checkpoint; XID 42 stays in flight.
        engine
            .wal_writer()
            .append(WalRecord::txn_commit(TxnId(41)).unwrap())
            .unwrap();
        engine.wal_writer().flush().unwrap();
        mem::forget(engine); // kill -9
    }

    let engine = StorageEngine::open_with_redo_and_clog(
        tmp.path(),
        &config,
        vec![noop_handler(WalRecordType::TxnCommit)],
        Vec::new(),
        Arc::new(MapClog::default()),
    )
    .unwrap();
    assert_eq!(
        engine.recovered_active_xids(),
        &[TxnId(42)],
        "ATT snapshot baseline (41, 42) minus the post-checkpoint commit (41)"
    );
}

/// ATT snapshot race (§11.4; Stage N review P2-3): XID 41's commit record
/// predates the checkpoint begin — invisible to the analysis scan — yet the
/// racy ATT snapshot still lists it. Recovery must drop it from the
/// recovered ATT via the rebuilt CLOG (its Committed bit is already there,
/// as the checkpoint's CLOG flush would have persisted it). Only the
/// genuinely in-flight XID 42 survives. No ABORTED is written for it
/// (filter-only; M2c work).
#[test]
fn test_recovered_att_is_filtered_through_rebuilt_clog() {
    #[derive(Debug)]
    struct StaticAttProvider(Vec<TxnId>);

    impl AttProvider for StaticAttProvider {
        fn active_xids(&self) -> Vec<TxnId> {
            self.0.clone()
        }
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let wal = engine.wal_writer();
        // XID 41 commits BEFORE the checkpoint begin: its terminal record
        // will predate the redo point.
        wal.append(WalRecord::heap_insert(PageId(1), 0, vec![1], TxnId(41)).unwrap())
            .unwrap();
        wal.append(WalRecord::txn_commit(TxnId(41)).unwrap())
            .unwrap();
        // XID 42 is still in flight.
        wal.append(WalRecord::heap_insert(PageId(2), 0, vec![2], TxnId(42)).unwrap())
            .unwrap();
        wal.flush().unwrap();

        // The racy provider still reports 41 as active (its commit landed
        // between the record append and the provider's active.remove).
        engine
            .checkpoint()
            .set_att_provider(Arc::new(StaticAttProvider(vec![TxnId(41), TxnId(42)])));
        engine.trigger_checkpoint().unwrap();
        mem::forget(engine); // kill -9
    }

    // The CLOG the recovery replays into already carries 41 = Committed
    // (the checkpoint's CLOG flush persisted it before the crash).
    let clog = Arc::new(MapClog::default());
    clog.set_state(TxnId(41), pg_storage::clog::TxnState::Committed);

    let (heap_handler, _) = counting_handler(WalRecordType::HeapInsert);
    let engine = StorageEngine::open_with_redo_and_clog(
        tmp.path(),
        &config,
        vec![heap_handler, noop_handler(WalRecordType::TxnCommit)],
        Vec::new(),
        clog,
    )
    .unwrap();

    assert_eq!(
        engine.recovered_active_xids(),
        &[TxnId(42)],
        "the CLOG filter must drop the racily snapshotted, already-committed XID 41"
    );
    // And the CLOG was NOT rewritten for the survivor (filter-only undo).
    assert_eq!(
        engine.clog().get_state(TxnId(42)),
        pg_storage::clog::TxnState::InProgress
    );
}

/// Crash mid-checkpoint (CheckpointBegin emitted, no CheckpointEnd):
/// recovery falls back to the previous completed CheckpointEnd and redoes
/// from its redo point.
///
/// Proof that redo actually ran from the previous redo point: a torn write
/// destroys the on-disk page after the crash, and only the post-checkpoint
/// FPI — replayed from `begin1` — can repair it. Note the FPI restores the
/// *pre-modification* image: raw `page_mut` bytes are not WAL-logged at the
/// storage layer (logging content is the AM's job), so the correct
/// post-recovery content is the checkpointed `0xA1`, not the unlogged
/// `0xA2`.
#[test]
fn test_crash_mid_checkpoint_recovers_from_previous_checkpoint_end() {
    use std::io::{Seek, SeekFrom, Write};

    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = test_config(&tmp);
    // Small pool so the eviction loop below forces the FPI that anchors the
    // page's post-checkpoint on-disk repair.
    config.buffer_pool_size = 256 * 1024; // 32 frames

    let page_id;
    let begin1;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();

        {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            page_id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE..].fill(0xA1);
        }
        begin1 = engine.trigger_checkpoint().unwrap();

        // Evict the page, then touch it mutably: the first post-checkpoint
        // pin_mut writes an FPI (page pd_lsn < checkpoint_lsn).
        let frame_count = engine.buffer_pool().frame_count();
        for _ in 0..frame_count + 4 {
            drop(engine.buffer_pool().new_page().unwrap());
        }
        let mut saw_fpi = false;
        {
            let mut guard = engine.buffer_pool().pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xA2; // unlogged, lost in the crash

            // Confirm the FPI for our page is in the WAL, or the repair
            // assertion below is vacuous.
            let mut reader = pg_storage::wal::reader::WalReader::open_at(
                tmp.path().join("wal"),
                config.wal_segment_size,
                begin1,
            )
            .unwrap();
            while let Some(rec) = reader.next_record().unwrap() {
                if rec.record_type == WalRecordType::FullPageImage {
                    let decoded: pg_storage::wal::record::FullPageImageRecord =
                        bincode::serde::decode_from_slice(
                            &rec.payload,
                            bincode::config::standard(),
                        )
                        .unwrap()
                        .0;
                    if decoded.page_id == page_id {
                        saw_fpi = true;
                    }
                }
            }
        }
        assert!(saw_fpi, "post-checkpoint pin_mut must have written an FPI");
        engine.wal_writer().flush().unwrap();

        // Crash mid-next-checkpoint: begin emitted, end never written.
        engine
            .wal_writer()
            .append(WalRecord::checkpoint_begin())
            .unwrap();
        engine.wal_writer().flush().unwrap();
        mem::forget(engine); // kill -9
    }

    // Torn write: destroy the on-disk page. Only redo of the post-begin1
    // FPI can bring it back.
    let data_file_path = pg_storage::io::data_file_path(tmp.path());
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&data_file_path)
        .unwrap();
    let offset = (page_id.0 - 1) * pg_storage::types::PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&vec![0xFFu8; pg_storage::types::PAGE_SIZE / 2])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);

    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // The superblock still anchors at the previous completed checkpoint...
    assert_eq!(engine.superblock().lock().checkpoint_lsn, begin1);
    // ...and redo from that redo point replayed the post-checkpoint FPI,
    // repairing the torn page to the checkpointed image. (Only the payload
    // region past the header is compared: FPI redo patches pd_lsn at
    // page[0..8] to the record's own LSN.)
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    assert!(
        guard.page()[PAGE_HEADER_SIZE..].iter().all(|&b| b == 0xA1),
        "redo from the previous CheckpointEnd must replay the post-checkpoint FPI"
    );
    drop(guard);

    // The engine is fully operational afterwards.
    let begin2 = engine.trigger_checkpoint().unwrap();
    assert!(begin2 > begin1);
}

/// Crash during the FIRST checkpoint (no completed checkpoint at all):
/// analysis finds no CheckpointEnd and recovery replays from `Lsn::FIRST`.
#[test]
fn test_crash_mid_first_checkpoint_replays_from_wal_start() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    let page_id;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            page_id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xB1;
        }
        // Dangling begin of the first-ever checkpoint.
        engine
            .wal_writer()
            .append(WalRecord::checkpoint_begin())
            .unwrap();
        engine.wal_writer().flush().unwrap();
        mem::forget(engine); // kill -9
    }

    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // No checkpoint ever completed: the superblock's redo point stays
    // invalid, and the PageAlloc record was replayed from Lsn::FIRST, so
    // the crashed page id is never handed out again.
    assert!(!engine.superblock().lock().checkpoint_lsn.is_valid());
    let guard = engine.buffer_pool().new_page().unwrap();
    assert!(guard.page_id() > page_id);
    assert!(engine.recovered_active_xids().is_empty());
}

/// Recovery branch: the WAL holds a CheckpointEnd NEWER than the
/// superblock — the instance crashed after `flush_to(end_lsn)` but before
/// the superblock write (checkpoint.rs steps 7→9). Recovery must warn and
/// rebuild with an empty baseline from the superblock's OLDER redo point,
/// so every record between the two redo points is replayed and neither
/// page ids nor XIDs are reused (engine.rs `run_analysis`, Stage N review
/// P1-2).
#[test]
fn test_recovery_with_checkpoint_end_newer_than_superblock() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    let page_a;
    let page_b;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            page_a = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x11;
        }
        engine.trigger_checkpoint().unwrap();

        // Snapshot the raw superblock FILE as of checkpoint 1 (the state a
        // crash between WAL flush and superblock write would leave behind).
        // Raw bytes, not Superblock::write: the A/B-copy writer refuses
        // non-increasing checkpoint_lsn values by design.
        let sb_bytes_before_checkpoint2 = std::fs::read(Superblock::path(tmp.path())).unwrap();

        {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            page_b = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x22;
        }
        engine.trigger_checkpoint().unwrap();

        // Roll the superblock back to the pre-checkpoint-2 state: the WAL
        // now holds a CheckpointEnd newer than the superblock's redo point.
        std::fs::write(Superblock::path(tmp.path()), sb_bytes_before_checkpoint2).unwrap();
        mem::forget(engine); // kill -9
    }

    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // The superblock still anchors at checkpoint 1...
    let sb = Superblock::read(&Superblock::path(tmp.path())).unwrap();
    assert!(sb.checkpoint_lsn.is_valid());
    // ...and redo started from that OLDER point, replaying the records
    // between the two checkpoints: page B's PageAlloc was replayed, so the
    // allocator cannot hand B out again.
    assert!(engine.page_allocator().lock().next_page_id() > page_b);
    let guard = engine.buffer_pool().new_page().unwrap();
    assert!(guard.page_id() > page_b, "no page id reuse across the gap");
    drop(guard);
    // Both pages' contents are intact (A via checkpoint 1, B via
    // checkpoint 2's flush, both durable).
    let guard = engine.buffer_pool().pin(page_a).unwrap();
    assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0x11);
    drop(guard);
    let guard = engine.buffer_pool().pin(page_b).unwrap();
    assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0x22);
}

/// Recovery branch: the superblock references a valid checkpoint but the
/// WAL contains NO CheckpointEnd at all (segments lost/corrupted after the
/// checkpoint). Recovery must warn and rebuild with an empty baseline from
/// the superblock's redo point instead of failing (engine.rs
/// `run_analysis`, Stage N review P1-2).
#[test]
fn test_recovery_with_superblock_checkpoint_but_no_checkpoint_end_in_wal() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    let page_a;
    let begin1;
    let next_page_id_at_checkpoint;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            page_a = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x33;
        }
        begin1 = engine.trigger_checkpoint().unwrap();
        next_page_id_at_checkpoint = engine.page_allocator().lock().next_page_id();
        engine.shutdown();
    }

    // Disaster: every WAL segment is gone. The data pages and the
    // superblock survive (checkpoint 1 flushed them).
    for entry in std::fs::read_dir(tmp.path().join("wal")).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|ext| ext == "log") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // Recovery fell back to the superblock anchor: checkpointed data is
    // intact and the allocator state comes from the superblock (no WAL
    // records left to replay), so no page id is reused.
    assert_eq!(engine.superblock().lock().checkpoint_lsn, begin1);
    let guard = engine.buffer_pool().pin(page_a).unwrap();
    assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0x33);
    drop(guard);
    assert_eq!(
        engine.page_allocator().lock().next_page_id(),
        next_page_id_at_checkpoint
    );
    let guard = engine.buffer_pool().new_page().unwrap();
    assert!(guard.page_id() > page_a, "no page id reuse after WAL loss");
}

/// A corrupted ATT snapshot file (CRC mismatch) degrades to an empty
/// baseline — analysis rebuilds the ATT by a full WAL scan from the
/// checkpoint LSN (§11.4; analysis.rs `load_att_snapshot` degradation
/// contract). Recovery must still succeed and the ATT must match the
/// WAL-rebuild result, not the (corrupted) snapshot.
///
/// Layout: a v2 checkpoint's ATT snapshot lists XIDs 51 and 52. XID 51
/// commits in the post-checkpoint tail; XID 52 writes a `HeapInsert` in
/// the tail and stays in flight. Corrupting the snapshot's CRC makes
/// analysis start from an empty ATT, but the WAL scan rediscovers 52
/// (insert) and drops 51 (commit) — the same result the snapshot
/// baseline plus WAL scan would produce. This proves degradation is
/// *correct*, not silently lossy.
#[test]
fn test_corrupted_snapshot_degrades_to_wal_scan() {
    #[derive(Debug)]
    struct StaticAttProvider(Vec<TxnId>);

    impl AttProvider for StaticAttProvider {
        fn active_xids(&self) -> Vec<TxnId> {
            self.0.clone()
        }
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    let checkpoint_lsn;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        engine
            .checkpoint()
            .set_att_provider(Arc::new(StaticAttProvider(vec![TxnId(51), TxnId(52)])));
        checkpoint_lsn = engine.trigger_checkpoint().unwrap();
        // XID 51 commits after the checkpoint; XID 52 writes a HeapInsert
        // and stays in flight — its WAL record lets the degraded WAL scan
        // rediscover it even without the snapshot.
        let wal = engine.wal_writer();
        wal.append(WalRecord::txn_commit(TxnId(51)).unwrap())
            .unwrap();
        wal.append(WalRecord::heap_insert(PageId(1), 0, vec![0xAB], TxnId(52)).unwrap())
            .unwrap();
        wal.flush().unwrap();
        mem::forget(engine); // kill -9
    }

    // Corrupt the ATT snapshot file: flip a body byte (after the 4-byte
    // CRC prefix) so the CRC check in `read_snapshot` fails.
    {
        let att_path = tmp
            .path()
            .join(format!("meta/att-{:016}.snapshot", checkpoint_lsn.0));
        assert!(
            att_path.exists(),
            "ATT snapshot must exist before corruption"
        );
        let mut bytes = std::fs::read(&att_path).unwrap();
        assert!(bytes.len() > 4);
        bytes[4] ^= 0xFF; // flip a body byte → CRC mismatch
        std::fs::write(&att_path, &bytes).unwrap();
    }

    let (heap_handler, _) = counting_handler(WalRecordType::HeapInsert);
    let engine = StorageEngine::open_with_redo_and_clog(
        tmp.path(),
        &config,
        vec![
            heap_handler,
            noop_handler(WalRecordType::TxnCommit),
            noop_handler(WalRecordType::TxnAbort),
        ],
        Vec::new(),
        Arc::new(MapClog::default()),
    )
    .unwrap();

    // The corrupted snapshot's baseline (51, 52) was discarded; the WAL
    // scan rebuilt the ATT from scratch: 52's HeapInsert added it, 51's
    // TxnCommit removed it. The CLOG filter (MapClog defaults to
    // InProgress) keeps 52. Without degradation the snapshot would have
    // seeded 51 and 52 — the same final result after the tail scan.
    assert_eq!(
        engine.recovered_active_xids(),
        &[TxnId(52)],
        "corrupted ATT snapshot degraded to empty baseline; WAL scan \
         rebuilt the ATT and found XID 52 in flight, dropped XID 51 (committed)"
    );
}

/// Under the `NoOpClogAccessor` (the M1/heap-only default), every XID
/// reads as `Committed` in the step-5b CLOG filter, so
/// `recovered_active_xids()` is always empty — even when transactions
/// were genuinely in-flight at the crash (engine.rs step 5b comment;
/// clog.rs `NoOpClogAccessor` docs). This test pins that behavior so a
/// future change to the filter logic cannot silently break M1
/// configurations.
#[test]
fn test_noop_clog_yields_empty_att_even_with_inflight_txns() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = test_config(&tmp);

    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        engine.trigger_checkpoint().unwrap();
        // Three in-flight XIDs with WAL records but no commit/abort.
        // Under a real CLOG these would land in the ATT.
        let wal = engine.wal_writer();
        for xid in 60..63u64 {
            wal.append(WalRecord::heap_insert(PageId(1), 0, vec![1], TxnId(xid)).unwrap())
                .unwrap();
        }
        wal.flush().unwrap();
        mem::forget(engine); // kill -9
    }

    // Recover with the default NoOp CLOG (open_with_redo_handlers, not
    // open_with_redo_and_clog).
    let (heap_handler, _) = counting_handler(WalRecordType::HeapInsert);
    let engine = StorageEngine::open_with_redo_handlers(
        tmp.path(),
        &config,
        vec![heap_handler, noop_handler(WalRecordType::TxnCommit)],
        Vec::new(),
    )
    .unwrap();

    // The analysis ATT held XIDs 60, 61, 62, but step 5b filters them
    // through the NoOp CLOG (Committed for all) → empty.
    assert_eq!(
        engine.recovered_active_xids(),
        &[],
        "NoOpClogAccessor reads every XID as Committed; the step-5b \
         filter must drop all ATT members — even genuinely in-flight ones"
    );
}
