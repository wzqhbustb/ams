//! M5 Stage 0: the page-initialization chain under a simulated crash
//! (tech-selection §8.1 step 1 / §10.3, v1.9 P1 — the A1 contract of
//! buffer_pool.rs:424-442).
//!
//! The core assertion: a freelist-RECYCLED page recovers with the freshly
//! initialized HNSW header — never with the previous tenant's bytes. This
//! is the test that turns red if `log_page_init` (post-image FPI) is dropped
//! from the chain.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use pg_am_hnsw::page::{
    dir_count, dir_next, dir_ordinal, init_dir_page, log_page_init, page_type, PAGE_TYPE_DIR,
};
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Lsn, PageId, PAGE_SIZE};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Manual temp dir (the crate carries no tempfile dev-dependency — M4's
/// dependency freeze discipline; dataset.rs's tests use the same pattern).
fn fresh_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pg_am_hnsw_m5_stage0-{}-{}-{tag}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn page_mut<'g>(
    guard: &'g mut pg_storage::buffer_pool::PageGuardMut<'_>,
) -> &'g mut [u8; PAGE_SIZE] {
    guard
        .page_mut()
        .try_into()
        .expect("a buffer frame is exactly PAGE_SIZE")
}

#[test]
fn recycled_page_recovers_as_hnsw_init_not_previous_tenant() {
    let dir = fresh_dir("recycle");
    let config = StorageConfig::new(&dir);

    let (page_b, init_lsn) = {
        let engine = StorageEngine::open(&dir, &config).unwrap();

        // Tenant A: allocate a page, fill it with junk, flush the junk to
        // disk (this is the previous tenant's on-disk image), then free it.
        let page_a = engine.buffer_pool().new_page().unwrap().page_id();
        {
            let mut guard = engine.buffer_pool().pin_mut(page_a).unwrap();
            let page = page_mut(&mut guard);
            page.fill(0xAB);
        }
        engine.buffer_pool().flush(page_a).unwrap();
        engine.page_allocator().lock().free_page(page_a).unwrap();

        // Recycle: with exactly one page on the freelist, the next
        // allocation must return the same page id.
        let page_b = engine.buffer_pool().new_page().unwrap().page_id();
        assert_eq!(page_a, page_b, "the freed page must be recycled");

        // The initialization chain (§8.1 step 1): init the HNSW header,
        // then post-image FPI + stamp pd_lsn.
        let lsn = {
            let mut guard = engine.buffer_pool().pin_mut(page_b).unwrap();
            let page = page_mut(&mut guard);
            init_dir_page(page, 0);
            let lsn = log_page_init(engine.wal_writer().as_ref(), page_b, page).unwrap();
            // 2026-09-11, Stage 0 review round 2 P2-2 (mutation-test style:
            // dropping the stamp inside log_page_init must turn this red):
            // the FPI's LSN must be stamped onto the page itself, and must
            // be a real LSN (Lsn::INVALID = 0 is "never touched by WAL",
            // pg-storage page.rs contract).
            assert_eq!(pg_storage::page::page_pd_lsn(page), lsn);
            assert_ne!(lsn, Lsn::INVALID);
            lsn
        };
        engine.wal_writer().flush_to(lsn).unwrap();

        // Simulated kill -9: no checkpoint, no clean shutdown.
        std::mem::forget(engine);
        (page_b, lsn)
    };

    // Recover: redo must replay the init FPI over the junk image.
    let engine = StorageEngine::open_with_redo_handlers(
        &dir,
        &config,
        pg_am_hnsw::redo::hnsw_redo_handlers(),
        vec![],
    )
    .unwrap();
    let guard = engine.buffer_pool().pin(page_b).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(
        page_type(page),
        PAGE_TYPE_DIR,
        "the recycled page must recover as an HNSW directory page, not the previous tenant's junk"
    );
    assert_eq!(dir_ordinal(page), 0);
    assert_eq!(dir_count(page), 0);
    assert_eq!(dir_next(page), PageId::INVALID);
    // 2026-09-14, M5 Stage 0 review round 5 P3-2 (mechanism correction —
    // the 2026-09-11 nano-3 comment invented a "tenant A pre-image FPI that
    // zeroes the page"; no such FPI exists, the junk was written without
    // WAL): WITHOUT log_page_init, redo replays no page content for page_b,
    // so the recovery end state is the ON-DISK JUNK flushed before free
    // (page_type would read 0xABAB), not a zero page. The load-bearing
    // assertion is therefore the equality ABOVE (page_type == PAGE_TYPE_DIR)
    // — it goes red whether the missing-FPI residue is junk or a zero page.
    // The != 0 check below documents one specific residue shape (the
    // "allocated but never initialized" zero page); it is not the detector
    // for this scenario (junk fails it too, but that is not what proves the
    // init chain).
    assert_ne!(
        page_type(page),
        0,
        "a zero page_type would mean 'allocated but never initialized'"
    );
    // And the init content must extend past the 32-byte header into the
    // directory self-describing header (bytes 32..56): version tag present.
    assert_eq!(
        page[32], 1,
        "directory format version must come from init_dir_page, not from junk or zeros"
    );
    // P2-2 (recovery side): the FPI replay must restore the page with the
    // FPI record's LSN as pd_lsn — the pd_lsn authority contract
    // (pg-storage page.rs:29-36) holds end-to-end across the crash.
    assert_eq!(
        pg_storage::page::page_pd_lsn(page),
        init_lsn,
        "recovered pd_lsn must equal the init FPI's LSN"
    );
    // P1 (crash side): no previous-tenant byte survives anywhere past the
    // headers — the whole page post-image is the initialized content.
    assert!(
        page[56..].iter().all(|&b| b == 0),
        "recovered page must be zero past the headers (FPI carries no tenant bytes)"
    );
    drop(guard);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
