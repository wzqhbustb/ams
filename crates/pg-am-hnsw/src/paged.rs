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
//!
//! **Allocation accounting** (2026-09-21, slice 4 P3-B closure — the
//! slice-3 final review's registered evaluation point; conclusion: the
//! `vector_iter` + iterator-distance path, zero unsafe):
//! `dist_to_query` is allocation-free (the stored vector streams off the
//! pinned page through `apply::vector_iter` into
//! `Metric::distance_iter`); `dist_between` keeps exactly ONE allocation
//! (node a's vector is materialized, its guard dropped, then node b
//! streams) because the deadlock discipline forbids holding two page pins
//! at once.

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
        // Allocation-free (P3-B): the stored vector streams off the pinned
        // page through `vector_iter` into the bit-identical iterator
        // distance — the computation completes inside the guard's lifetime.
        self.with_node(node, |page, slot| {
            let v = apply::vector_iter(page, slot, self.dim)?;
            self.metric.distance_iter(query, v)
        })
        .expect("PagedGraph::dist_to_query: nodes handed to the algorithm cores resolve through a directory the open protocol validated (chain structure + node page type), and all vectors passed §5 entry validation at insert — a failure here is on-disk corruption beneath that validation")
    }

    fn dist_between(&self, a: NodeId, b: NodeId) -> f64 {
        // Sequential short-lived pins: never hold one node's guard while
        // resolving/pinning the other (deadlock discipline, module header).
        // Exactly one allocation (P3-B): node a's vector is materialized
        // and its guard dropped; node b then streams through `vector_iter`.
        let va = self
            .with_node(a, |page, slot| apply::entry_vector(page, slot, self.dim))
            .expect("PagedGraph::dist_between: same validated-resolution premise as dist_to_query");
        self.with_node(b, |page, slot| {
            let vb = apply::vector_iter(page, slot, self.dim)?;
            self.metric.distance_iter(&va, vb)
        })
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

#[cfg(test)]
mod tests {
    //! Stage E coverage top-up (2026-09-29): the resolve/with_node loud-
    //! corruption arms — "resolution past the published mapping must fail
    //! loudly, never fabricate" is a CONTRACT (§8.1③) and gets pinned here
    //! directly, not just by coverage accounting.
    use super::*;
    use crate::page::{init_dir_page, init_meta_page, log_page_init};
    use pg_storage::config::StorageConfig;
    use pg_storage::engine::StorageEngine;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Manual temp dir (no tempfile dev-dependency — M4's dependency freeze).
    fn fresh_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pg_am_hnsw_paged-{}-{}-{tag}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_engine(tag: &str) -> (StorageEngine, PathBuf) {
        let dir = fresh_dir(tag);
        // A handful of frames is plenty for these page-type pins — the
        // 128 MiB default across three parallel tests was ~384 MiB of
        // waste (Stage E review).
        let mut cfg = StorageConfig::new(&dir);
        cfg.buffer_pool_size = 16 * cfg.page_size();
        let engine = StorageEngine::open(&dir, &cfg).unwrap();
        (engine, dir)
    }

    /// A freshly allocated + init'd page of the given type, WAL-logged.
    fn alloc_page(engine: &StorageEngine, init: impl FnOnce(&mut [u8; PAGE_SIZE])) -> PageId {
        let mut guard = engine.buffer_pool().new_page().unwrap();
        let page_id = guard.page_id();
        {
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            init(page);
            log_page_init(engine.wal_writer(), page_id, page).unwrap();
        }
        page_id
    }

    fn graph<'a>(pool: &'a BufferPool, dir_pages: &'a [PageId]) -> PagedGraph<'a> {
        PagedGraph::new(pool, dir_pages, 1, 4, 4, 8, Metric::L2)
    }

    #[test]
    fn resolve_past_the_chain_is_loud_corruption() {
        let (engine, dir) = test_engine("past-chain");
        {
            let g = graph(engine.buffer_pool(), &[]);
            let err = g.resolve(NodeId(0)).expect_err("out-of-chain id must fail");
            assert!(
                err.to_string().contains("maps past the directory chain"),
                "{err}"
            );
        }
        // Cleanup discipline: the graph borrows the pool, so its scope ends
        // first, and the dir is removed only AFTER the engine is down —
        // with a loud failure, not a swallowed `let _ =` (Stage E review).
        drop(engine);
        std::fs::remove_dir_all(&dir).expect("temp dir cleanup");
    }

    #[test]
    fn resolve_non_dir_page_is_loud_corruption() {
        let (engine, dir) = test_engine("non-dir");
        let meta = alloc_page(&engine, init_meta_page);
        {
            let dir_pages = [meta];
            let g = graph(engine.buffer_pool(), &dir_pages);
            let err = g.resolve(NodeId(0)).expect_err("non-dir page must fail");
            assert!(err.to_string().contains("is not a directory page"), "{err}");
        }
        drop(engine);
        std::fs::remove_dir_all(&dir).expect("temp dir cleanup");
    }

    #[test]
    fn with_node_wrong_target_type_is_loud_corruption() {
        let (engine, dir) = test_engine("wrong-type");
        let meta = alloc_page(&engine, init_meta_page);
        let dir_page = alloc_page(&engine, |p| init_dir_page(p, 0));
        // Directory entry 0 maps node 0 at the META page (wrong type).
        {
            let mut guard = engine.buffer_pool().pin_mut(dir_page).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            crate::apply::dir_append(page, 0, meta, 0).unwrap();
        }
        {
            let g_dir_pages = [dir_page];
            let g = graph(engine.buffer_pool(), &g_dir_pages);
            let err = g
                .with_node(NodeId(0), |_, _| Ok(()))
                .expect_err("wrong target page type must fail");
            assert!(err.to_string().contains("wrong type"), "{err}");
        }
        drop(engine);
        std::fs::remove_dir_all(&dir).expect("temp dir cleanup");
    }
}
