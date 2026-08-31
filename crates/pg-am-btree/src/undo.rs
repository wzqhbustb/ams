//! B+Tree undo handler (Stage S, §11.3).
//!
//! Finishes incomplete B+Tree splits detected during redo. Each incomplete
//! split reached Prepare (and optionally Copy) but never Commit before the
//! crash. The handler calls `finish_incomplete_split` to complete the
//! split, which emits a `BTreeSplitCLR` so the result is durable.
//!
//! # Detection beyond the WAL tail (post-Stage-S review H3)
//!
//! Redo populates the incomplete-split tracker only from the records it
//! replays, and replay starts at the last checkpoint. A split whose Prepare
//! predates the checkpoint — both pages flushed clean, Commit never written
//! — leaves no trace in the replay window, so the tracker comes up empty
//! and the page's `SPLIT_INCOMPLETE` flag would stay set forever (writes to
//! that key range wedge on the restart budget; for a root split the whole
//! index). The undo pass therefore additionally **scans allocated pages for
//! the flag** (`scan_split_incomplete_pages`). A page scan is consistent
//! with the existing undo cost model (`find_parent_page` already scans
//! `1..next_page_id` per finished split) and recovery is rare and
//! single-threaded. The alternatives were rejected: persisting an
//! in-flight-split list into checkpoint snapshots needs an online registry
//! that does not exist (a much larger change), and withholding
//! `SPLIT_INCOMPLETE` pages from checkpoint flushes does nothing on its own
//! — the redo start stays at the checkpoint LSN, and pulling rec_lsn back
//! would conflict with Stage N's clamp.

use std::collections::HashMap;

use pg_storage::error::{Result, StorageError};
use pg_storage::recovery::{IncompleteSplit, UndoContext, UndoHandler};
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::error::BTreeError;
use crate::index::{choose_split_slot_readonly, finish_incomplete_split, SplitToFinish};
use crate::page::{BtreePage, BTREE_FLAG_SPLIT_INCOMPLETE};

fn btree_to_storage(e: BTreeError) -> StorageError {
    match e {
        BTreeError::Storage(s) => s,
        other => StorageError::MetadataCorrupted(format!("btree undo: {other}")),
    }
}

/// Finishes incomplete B+Tree splits after crash recovery redo.
pub struct BTreeUndoHandler;

impl UndoHandler for BTreeUndoHandler {
    fn undo(&self, ctx: &mut UndoContext<'_>) -> Result<()> {
        let mut splits = ctx.incomplete_splits.incomplete_splits().clone();
        // H3: merge in splits redo could not see (Prepare before the redo
        // start, pages flushed by a checkpoint, crash before Commit).
        scan_split_incomplete_pages(ctx, &mut splits)?;
        if splits.is_empty() {
            return Ok(());
        }

        // Sort by level descending: splits closer to the root first. A leaf
        // split's downlink insertion may need to split its parent (the C2
        // cascade), and finishing a parent's own incomplete split first both
        // frees space in it and guarantees `find_parent_page` never returns
        // a still-`SPLIT_INCOMPLETE` page. Same-level splits tie-break by
        // left page id (post-Stage-S fix B4): the tracker is a HashMap, so
        // without the tiebreaker CLR emission order — and thus the recovered
        // WAL bytes — would be nondeterministic across recoveries.
        let mut sorted: Vec<_> = splits.into_values().collect();
        sorted.sort_by_key(|split| (std::cmp::Reverse(split.level), split.left_page.0));

        for split in sorted {
            let copy_start_slot = match split.copy_start_slot {
                Some(slot) => slot,
                None => {
                    // Only Prepare was reached (or the Copy record predates
                    // the redo start) — compute the midpoint split slot. The
                    // value is only consulted when the right page never held
                    // entries; a right page with entries makes the finish
                    // key off its first entry instead.
                    choose_split_slot_readonly(ctx.buffer_pool, split.left_page, split.level)
                        .map_err(btree_to_storage)?
                }
            };

            finish_incomplete_split(
                ctx.buffer_pool,
                ctx.wal_writer,
                ctx.page_allocator,
                &SplitToFinish {
                    left_page: split.left_page,
                    right_page: split.right_page,
                    level: split.level,
                    copy_start_slot,
                    // B8: the Prepare record's LSN is the CLR's diagnostic
                    // redo reference point. INVALID when the split was found
                    // by the H3 page scan (its Prepare predates the replay
                    // window, so no LSN is knowable).
                    prepare_lsn: split.prepare_lsn,
                },
            )
            .map_err(btree_to_storage)?;
        }
        Ok(())
    }
}

/// H3: scan allocated pages for `SPLIT_INCOMPLETE` and add every flagged
/// page the tracker does not already know (redo saw its Prepare) to
/// `splits`.
///
/// Cost: always O(allocated_pages) — the scan runs even when the tracker
/// already holds every split (tracker-known pages are skipped but still
/// iterated). Acceptable because recovery is single-threaded and rare, and
/// the scan takes only read pins; revisit if multi-million-page databases
/// make recovery I/O audible.
///
/// False-positive analysis: `btpo_flags` lives in `pd_flags` bits 12..15 and
/// nothing outside the B+Tree AM ever writes those bits — the heap never
/// touches `pd_flags` at all, and heap pages are zero-initialised
/// (`init_with_special` fills the whole page), so a heap page always reads
/// flags == 0. (Note: heap pages DO pass the 16-byte special-space geometry
/// check — `HEAP_SPECIAL_SIZE == BTREE_SPECIAL_SIZE == 16` — so the flag
/// bits alone are the discriminator; a freed B+Tree page can only reach the
/// heap after a recovery pass that would have cleared any stale flag, and
/// heap page init re-zeroes the page anyway.) Never-written pages read as
/// geometry-invalid, so they are skipped by the `Err(_) => continue`
/// arms. A set bit therefore unambiguously marks a B+Tree page whose split
/// never committed: redo clears the flag for every Commit in the replay
/// window, and a Commit before the checkpoint made the cleared flag durable
/// (checkpoints flush every dirty page), so a still-set flag after redo
/// means genuinely incomplete.
fn scan_split_incomplete_pages(
    ctx: &UndoContext<'_>,
    splits: &mut HashMap<PageId, IncompleteSplit>,
) -> Result<()> {
    let max_pid = ctx.page_allocator.lock().next_page_id().0;
    for pid in 1..max_pid {
        let page_id = PageId(pid);
        if splits.contains_key(&page_id) {
            continue;
        }
        let guard = match ctx.buffer_pool.pin(page_id) {
            Ok(g) => g,
            Err(_) => continue,
        };
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let flags = match BtreePage::flags(page) {
            Ok(f) => f,
            Err(_) => continue, // not a B+Tree page (heap, free, never written)
        };
        if flags & BTREE_FLAG_SPLIT_INCOMPLETE == 0 {
            continue;
        }
        let level = BtreePage::level(page).map_err(btree_to_storage)?;
        let right = BtreePage::next(page).map_err(btree_to_storage)?;
        if right == PageId::INVALID {
            return Err(StorageError::MetadataCorrupted(format!(
                "page {page_id} is SPLIT_INCOMPLETE but has no right sibling"
            )));
        }
        splits.insert(
            page_id,
            IncompleteSplit {
                left_page: page_id,
                right_page: right,
                level,
                // Not knowable from the page alone and not needed: the finish
                // reads everything it uses (chain links, split decision)
                // from the current page states.
                left_old_next: PageId::INVALID,
                // The Copy record, if any, predates the redo start. `None`
                // makes the finish derive the split slot from the current
                // left page — only consulted when the right page is empty.
                copy_start_slot: None,
                // The Prepare record predates the replay window too, so its
                // LSN is unknowable — the CLR's diagnostic redo_ref_lsn
                // stays INVALID for scan-detected splits (B8).
                prepare_lsn: pg_storage::types::Lsn::INVALID,
            },
        );
    }
    Ok(())
}
