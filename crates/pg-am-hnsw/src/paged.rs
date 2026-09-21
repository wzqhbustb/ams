//! Page-resident read-only graph view — Phase 2 M5 Stage C slice 3
//! (tech-selection §10.2 task 3 consumer, §8.1 step 5).
//!
//! [`PagedGraph`] is the [`GraphAccess`] implementation that lets the
//! generic algorithm cores ([`crate::graph::search_layer`] /
//! [`crate::graph::select_neighbors`]) run DIRECTLY against the buffer
//! pool: the in-memory `Hnsw` and the page-resident graph share one
//! algorithm implementation, so slice 3's insert path and the in-memory
//! reference cannot drift.
//!
//! **Resolution cache** (slice 3): NodeId → `(page, slot)` is
//! `dir_pages[id / DIR_ENTRIES_PER_PAGE]` + `dir_entry(page, id %
//! DIR_ENTRIES_PER_PAGE)` — the ordinal-ordered page list from
//! `DirChainInfo::pages`, never a chain re-walk.
//!
//! **Deadlock discipline**: every page access is a short-lived shared pin;
//! no read guard is ever held across another pin (dist_between resolves and
//! reads the two nodes in sequence, dropping each guard before the next),
//! and no read guard is held into a `pin_mut` (the insert path collects
//! into owned Vecs first).
//!
//! **Trait-contract premise** (slice 2 final-review nano-1; tightened
//! 2026-09-21, slice-3 final review P3-A): the `GraphAccess` methods return
//! bare values, not `Result`s. What the open protocol actually validates:
//! the chain STRUCTURE and the DIR/META page types — NOT each directory
//! entry's target page type. That gap is closed one boundary later, by the
//! §11.3 audit (assertion f: every entry resolves to a NODE page, Stage D).
//! A resolution failure on the search/insert path is therefore fail-stop
//! (panic with the premise spelled out in the message), never a wrong
//! answer — accepted for M5's single-threaded utility scope. Making the
//! serving path error-instead-of-panic means `GraphAccess` returning
//! `Result` (a slice-2 design rollback touching both algorithm cores and
//! the in-memory reference) — registered as a Phase-4 hardening option,
//! not an M5 defect.

use pg_storage::buffer_pool::BufferPool;
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::apply;
use crate::dir::{self, DIR_ENTRIES_PER_PAGE};
use crate::error::{HnswError, Result};
use crate::graph::{GraphAccess, Metric};
use crate::node::NodeGeometry;
use crate::page::{page_type, storage_err, PAGE_TYPE_DIR, PAGE_TYPE_NODE};
use crate::params::NodeId;

/// A read-only view of the page-resident graph behind the insert/search
/// algorithm cores. Borrows the pool and the index's resolution cache; the
/// geometry (`dim`/`m`/`m_max0`) and metric come from the validated meta
/// page (the struct carries m/m_max0 beyond the minimal slice-3 sketch
/// because `neighbor_iter` needs the full [`NodeGeometry`]).
pub(crate) struct PagedGraph<'a> {
    pool: &'a BufferPool,
    /// Directory chain pages in ordinal order (`DirChainInfo::pages`).
    dir_pages: &'a [PageId],
    /// Chain high-water mark == node count (dense NodeId space, §3).
    hwm: u64,
    /// Vector dimension (meta page, creation-pinned).
    dim: u16,
    /// Upper-level neighbor capacity (meta page).
    m: u16,
    /// Level-0 neighbor capacity (meta page).
    m_max0: u16,
    /// Distance metric (meta page).
    metric: Metric,
}

impl<'a> PagedGraph<'a> {
    /// Assemble the view from the index's open-time state.
    pub(crate) fn new(
        pool: &'a BufferPool,
        dir_pages: &'a [PageId],
        hwm: u64,
        dim: u16,
        m: u16,
        m_max0: u16,
        metric: Metric,
    ) -> Self {
        Self {
            pool,
            dir_pages,
            hwm,
            dim,
            m,
            m_max0,
            metric,
        }
    }

    /// The entry geometry derived from the meta-pinned parameters.
    fn geometry(&self) -> NodeGeometry {
        NodeGeometry {
            dim: self.dim,
            m: self.m,
            m_max0: self.m_max0,
        }
    }

    /// Resolve a NodeId to its `(node page, slot)` through the directory
    /// resolution cache. Loud `Corrupted` on an out-of-chain id or an
    /// out-of-range/empty directory slot (§8.1③: resolution past the
    /// published mapping must fail loudly, never fabricate).
    pub(crate) fn resolve(&self, node_id: NodeId) -> Result<(PageId, u16)> {
        let id = u64::from(node_id.0);
        let per_page = u64::from(DIR_ENTRIES_PER_PAGE);
        let page_idx = (id / per_page) as usize;
        let Some(&dir_page_id) = self.dir_pages.get(page_idx) else {
            return Err(HnswError::Corrupted(format!(
                "resolve: node {} maps past the directory chain ({} pages)",
                node_id.0,
                self.dir_pages.len()
            )));
        };
        let guard = self
            .pool
            .pin(dir_page_id)
            .map_err(|e| storage_err("resolve: pin directory page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        if page_type(page) != PAGE_TYPE_DIR {
            return Err(HnswError::Corrupted(format!(
                "resolve: page {} is not a directory page",
                dir_page_id.0
            )));
        }
        dir::dir_entry(page, (id % per_page) as u32)
    }

    /// Pin `node`'s page, check its type, and hand `f` the page bytes plus
    /// the entry's slot — one short-lived shared pin per call (the deadlock
    /// discipline above).
    fn with_node<R>(
        &self,
        node: NodeId,
        f: impl FnOnce(&[u8; PAGE_SIZE], u16) -> Result<R>,
    ) -> Result<R> {
        let (page_id, slot) = self.resolve(node)?;
        let guard = self
            .pool
            .pin(page_id)
            .map_err(|e| storage_err("paged graph: pin node page", e))?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("a buffer frame is exactly PAGE_SIZE");
        if page_type(page) != PAGE_TYPE_NODE {
            return Err(HnswError::Corrupted(format!(
                "paged graph: directory maps node {} to page {} of wrong type {}",
                node.0,
                page_id.0,
                page_type(page)
            )));
        }
        f(page, slot)
    }
}

impl GraphAccess for PagedGraph<'_> {
    fn node_count(&self) -> usize {
        self.hwm as usize
    }

    fn dist_to_query(&self, query: &[f32], node: NodeId) -> f64 {
        self.with_node(node, |page, slot| {
            let v = apply::entry_vector(page, slot, self.dim)?;
            self.metric.distance(query, &v)
        })
        .expect("PagedGraph::dist_to_query: nodes handed to the algorithm cores resolve through a directory the open protocol validated (chain structure + node page type), and all vectors passed §5 entry validation at insert — a failure here is on-disk corruption beneath that validation")
    }

    fn dist_between(&self, a: NodeId, b: NodeId) -> f64 {
        // Sequential short-lived pins: never hold one node's guard while
        // resolving/pinning the other (deadlock discipline, module header).
        let va = self
            .with_node(a, |page, slot| apply::entry_vector(page, slot, self.dim))
            .expect("PagedGraph::dist_between: same validated-resolution premise as dist_to_query");
        let vb = self
            .with_node(b, |page, slot| apply::entry_vector(page, slot, self.dim))
            .expect("PagedGraph::dist_between: same validated-resolution premise as dist_to_query");
        self.metric
            .distance(&va, &vb)
            .expect("PagedGraph::dist_between: both vectors passed §5 entry validation at insert, so the distance cannot fail")
    }

    fn for_each_neighbor(&self, node: NodeId, level: u8, mut f: impl FnMut(NodeId)) {
        self.with_node(node, |page, slot| {
            for id in apply::neighbor_iter(page, slot, self.geometry(), level)? {
                f(NodeId(id));
            }
            Ok(())
        })
        .expect("PagedGraph::for_each_neighbor: same validated-resolution premise as dist_to_query; a level above the entry's top level is an algorithm-core bug, not input")
    }
}
