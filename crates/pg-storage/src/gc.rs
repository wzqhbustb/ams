//! AM-neutral garbage-collection contracts.
//!
//! Collection is read-only and produces a bounded batch. Reclamation consumes
//! that batch through the same ascending, deduplicated page order. AM-specific
//! schemas and index types deliberately stay out of this module.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::clog::ClogAccessor;
use crate::error::{Result, StorageError};
use crate::types::{Oid, PageId, Tid, TxnId};

/// Physical identity of one relation participating in GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GcRelation {
    /// Relation OID.
    pub rel_oid: Oid,
    /// First page of the relation's heap chain.
    pub first_page: PageId,
}

/// Stable identifier of one page-reclamation operation within a relation
/// generation. It is derived at collect time and needs no separate high-water.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GcOperationId {
    /// Owning relation OID.
    pub relation_oid: Oid,
    /// Relation generation when collection began.
    pub base_generation: u64,
    /// Page reclaimed by this operation.
    pub page_id: PageId,
}

/// Input to one incremental, read-only collection batch.
#[derive(Debug)]
pub struct GcRequest<'a> {
    /// Relation to inspect.
    pub relation: GcRelation,
    /// Oldest snapshot XID whose visibility must be preserved.
    pub horizon: TxnId,
    /// Relation generation captured before collection starts. This advances
    /// only for DROP and page-chain structure changes, never for ordinary DML.
    pub base_generation: u64,
    /// Maximum number of pages admitted to this batch.
    pub max_pages: usize,
    /// Maximum estimated bytes admitted to this batch.
    pub max_estimated_bytes: u64,
    /// Collect only pages strictly after this cursor; `None` starts at the
    /// relation's first page.
    pub start_after: Option<PageId>,
    /// Commit-status oracle used by the AM's existing dead-tuple rules.
    pub clog: &'a dyn ClogAccessor,
}

impl GcRequest<'_> {
    /// Validate the budget and cursor before an AM starts scanning.
    pub fn validate(&self) -> Result<()> {
        if self.horizon == TxnId::INVALID {
            return Err(StorageError::InvalidConfig(
                "GC horizon must be a real transaction ID".to_string(),
            ));
        }
        if self.max_pages == 0 || self.max_estimated_bytes == 0 {
            return Err(StorageError::InvalidConfig(
                "GC budgets must be positive to guarantee forward progress".to_string(),
            ));
        }
        if self.relation.rel_oid == Oid::INVALID || self.relation.first_page == PageId::INVALID {
            return Err(StorageError::InvalidConfig(
                "GC relation contains an invalid OID or first page".to_string(),
            ));
        }
        if self.start_after == Some(PageId::INVALID) {
            return Err(StorageError::InvalidConfig(
                "GC resume cursor must be a real page".to_string(),
            ));
        }
        Ok(())
    }
}

/// Work for exactly one page. `dead_tids` never contains another page's rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPageWork {
    /// Page owning this work item; must match its `GcWork.pages` key.
    pub page_id: PageId,
    /// Dead row versions on this page only.
    pub dead_tids: Vec<Tid>,
    /// Estimated bytes needed or reclaimed by this batch item.
    pub estimated_bytes: u64,
}

/// Maximum dead TIDs representable on one slotted page.
pub const MAX_GC_DEAD_TIDS_PER_PAGE: usize = (crate::types::PAGE_SIZE - 32 - 16) / 4;

impl GcPageWork {
    /// Validate page ownership, the slot-count bound, and strictly ascending
    /// TIDs at the contract boundary.
    pub fn validate(&self) -> Result<()> {
        if self.dead_tids.len() > MAX_GC_DEAD_TIDS_PER_PAGE {
            return Err(StorageError::InvalidConfig(format!(
                "GC page {:?} has {} dead TIDs, maximum {MAX_GC_DEAD_TIDS_PER_PAGE}",
                self.page_id,
                self.dead_tids.len()
            )));
        }
        let mut previous = None;
        for tid in &self.dead_tids {
            if tid.page_id != self.page_id {
                return Err(StorageError::InvalidConfig(format!(
                    "GC TID {:?} does not belong to page {:?}",
                    tid, self.page_id
                )));
            }
            if previous.is_some_and(|previous| *tid <= previous) {
                return Err(StorageError::InvalidConfig(format!(
                    "GC page {:?} dead TIDs are not strictly ascending at {:?}",
                    self.page_id, tid
                )));
            }
            previous = Some(*tid);
        }
        Ok(())
    }
}

/// A batch produced by [`GcParticipant::collect`] and consumed by
/// [`GcParticipant::reclaim`]. `BTreeMap` guarantees one item per page and a
/// single shared ascending execution order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcWork {
    /// Relation that was collected.
    pub relation: GcRelation,
    /// Horizon actually used for collection.
    pub horizon: TxnId,
    /// Relation generation captured before collection. It advances only for
    /// DROP and page-chain structure changes (link/unlink/free), not ordinary
    /// DML. Reclaim must abandon this batch if the current generation differs;
    /// page reuse is guarded separately by `page_generation`.
    pub base_generation: u64,
    /// Deduplicated per-page work.
    pub pages: BTreeMap<PageId, GcPageWork>,
    /// First page of the next batch, or `None` when collection is complete.
    /// The next batch must start strictly after this page.
    pub next_page: Option<PageId>,
}

impl GcWork {
    /// Construct an empty work batch for a relation, horizon, and generation.
    pub fn new(relation: GcRelation, horizon: TxnId, base_generation: u64) -> Self {
        Self {
            relation,
            horizon,
            base_generation,
            pages: BTreeMap::new(),
            next_page: None,
        }
    }

    /// Add one page, rejecting duplicate page work and cross-page TIDs rather
    /// than compacting or freeing anything twice.
    pub fn insert_page(&mut self, work: GcPageWork) -> Result<()> {
        work.validate()?;
        let page_id = work.page_id;
        if self.pages.contains_key(&page_id) {
            return Err(StorageError::InvalidConfig(format!(
                "duplicate GC work for page {page_id:?}"
            )));
        }
        self.pages.insert(page_id, work);
        Ok(())
    }

    /// Validate every page key and item before reclaim starts.
    pub fn validate(&self) -> Result<()> {
        if self.next_page == Some(PageId::INVALID) {
            return Err(StorageError::MetadataCorrupted(
                "GC next-page cursor must be a real page".to_string(),
            ));
        }
        if let (Some(next), Some(last)) = (self.next_page, self.pages.keys().next_back()) {
            if next <= *last {
                return Err(StorageError::MetadataCorrupted(format!(
                    "GC next-page cursor {next:?} does not advance past last work page {last:?}"
                )));
            }
        }
        for (page_id, work) in &self.pages {
            if page_id != &work.page_id {
                return Err(StorageError::MetadataCorrupted(format!(
                    "GC page key {page_id:?} disagrees with item {:?}",
                    work.page_id
                )));
            }
            work.validate()?;
        }
        Ok(())
    }
}

/// Counters reported by one reclaim batch. Missing AM-specific work stays zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Dead heap versions removed.
    pub dead_tuples: u64,
    /// Index entries removed before TID invalidation.
    pub index_entries_removed: u64,
    /// Heap pages compacted in place.
    pub heap_pages_compacted: u64,
    /// Empty heap/B+Tree pages unlinked by a merge.
    pub heap_pages_merged: u64,
    /// Pages returned to the allocator exactly once.
    pub heap_pages_freed: u64,
    /// TOAST chunks reclaimed.
    pub toast_chunks_reclaimed: u64,
    /// Retired segment files removed.
    pub segment_files_removed: u64,
    /// Number of coordinator work items completed.
    pub work_items: u64,
}

/// One AM's adapter into the engine-owned coordinator.
pub trait GcParticipant: Send + Sync {
    /// Collect one read-only, incrementally bounded work batch.
    fn collect(&self, request: GcRequest<'_>) -> Result<GcWork>;

    /// Apply a previously collected batch in the contract's durable order.
    fn reclaim(&self, work: GcWork) -> Result<GcStats>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation() -> GcRelation {
        GcRelation {
            rel_oid: Oid(20_000),
            first_page: PageId(1),
        }
    }

    fn tid(page: u64, slot: u16) -> Tid {
        Tid {
            page_id: PageId(page),
            slot_id: slot,
        }
    }

    #[test]
    fn work_pages_are_unique_and_share_ascending_order() {
        let mut work = GcWork::new(relation(), TxnId(9), 7);
        for page in [9, 3, 7] {
            work.insert_page(GcPageWork {
                page_id: PageId(page),
                dead_tids: vec![tid(page, 1)],
                estimated_bytes: 64,
            })
            .unwrap();
        }
        assert_eq!(
            work.pages.keys().map(|page| page.0).collect::<Vec<_>>(),
            vec![3, 7, 9]
        );
        assert!(work
            .insert_page(GcPageWork {
                page_id: PageId(3),
                dead_tids: Vec::new(),
                estimated_bytes: 0,
            })
            .is_err());
        assert_eq!(work.pages.get(&PageId(3)).unwrap().estimated_bytes, 64);
        assert!(work
            .insert_page(GcPageWork {
                page_id: PageId(4),
                dead_tids: vec![tid(5, 1)],
                estimated_bytes: 64,
            })
            .is_err());
    }

    #[test]
    fn page_work_rejects_non_ascending_duplicate_tids_and_slot_overflow() {
        let duplicate = GcPageWork {
            page_id: PageId(3),
            dead_tids: vec![tid(3, 1), tid(3, 1)],
            estimated_bytes: 128,
        };
        assert!(duplicate.validate().is_err());
        let unsorted = GcPageWork {
            page_id: PageId(3),
            dead_tids: vec![tid(3, 2), tid(3, 1)],
            estimated_bytes: 128,
        };
        assert!(unsorted.validate().is_err());
        let overflow = GcPageWork {
            page_id: PageId(3),
            dead_tids: (0..MAX_GC_DEAD_TIDS_PER_PAGE + 1)
                .map(|slot| tid(3, slot as u16))
                .collect(),
            estimated_bytes: 0,
        };
        assert!(overflow.validate().is_err());
    }

    #[test]
    fn request_validates_budget_cursor_and_generation() {
        let clog = crate::clog::NoOpClogAccessor;
        let mut request = GcRequest {
            relation: relation(),
            horizon: TxnId(9),
            base_generation: 7,
            max_pages: 2,
            max_estimated_bytes: 128,
            start_after: Some(PageId(3)),
            clog: &clog,
        };
        request.validate().unwrap();
        assert_eq!(request.base_generation, 7);
        request.max_pages = 0;
        assert!(request.validate().is_err());
        request.max_pages = 1;
        request.max_estimated_bytes = 0;
        assert!(request.validate().is_err());
        request.max_estimated_bytes = 128;
        request.horizon = TxnId::INVALID;
        assert!(request.validate().is_err());
        request.horizon = TxnId(9);
        request.start_after = Some(PageId::INVALID);
        assert!(request.validate().is_err());
    }

    #[test]
    fn work_next_page_must_advance_without_replaying_pages() {
        let mut work = GcWork::new(relation(), TxnId(9), 7);
        work.insert_page(GcPageWork {
            page_id: PageId(9),
            dead_tids: vec![tid(9, 1)],
            estimated_bytes: 64,
        })
        .unwrap();
        assert_eq!(work.base_generation, 7);
        assert_eq!(work.next_page, None);
        work.next_page = Some(PageId(9));
        assert!(work.validate().is_err());
        work.next_page = Some(PageId(10));
        work.validate().unwrap();
        work.next_page = Some(PageId::INVALID);
        assert!(work.validate().is_err());
    }

    #[test]
    fn key_item_mismatch_is_loud_before_reclaim() {
        let mut work = GcWork::new(relation(), TxnId(9), 7);
        work.pages.insert(
            PageId(3),
            GcPageWork {
                page_id: PageId(4),
                dead_tids: Vec::new(),
                estimated_bytes: 0,
            },
        );
        assert!(matches!(
            work.validate(),
            Err(StorageError::MetadataCorrupted(_))
        ));
    }
}
