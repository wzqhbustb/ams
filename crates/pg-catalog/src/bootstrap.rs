//! Catalog bootstrap (coding-plan Stage H; tech-selection §5.2).
//!
//! On an empty data directory, [`bootstrap_if_needed`] writes the six system
//! tables directly as heap tuples at their fixed first pages
//! ([`crate::system_tables`]). `pg_class` is self-describing: it contains a
//! row for every system table, including itself, with `relnatts` matching the
//! hardcoded schema. `pg_rust_relpages` (Stage K) gets no bootstrap rows —
//! its rows are written by DDL — only an initialized empty page.
//!
//! # Write path
//!
//! All writes go through the `StorageEngine`'s buffer pool (`pin_mut` /
//! `new_page`) — never around it — and a checkpoint is triggered at the end
//! to make the catalog durable. There is intentionally **no** heap WAL
//! record for catalog content yet: `HeapInsert` redo records arrive in
//! Stage I. The buffer pool's own FPI machinery still applies as usual.
//!
//! # Idempotency and half-state detection
//!
//! Because catalog writes are not WAL-logged, a crash can leave a half-state:
//! pages 1..=6 allocated (their `PageAlloc` WAL records are durable) but
//! never initialized — or, when a background checkpoint flushes a strict
//! subset of the bootstrap pages mid-write, only some of them initialized.
//! The detection rule therefore checks **all six** system pages: each must
//! be a valid slotted page written by `SlottedPage::init_with_special`
//! (`pd_pagesize_version == PAGE_FORMAT_VERSION`, `pd_lower >= 32`, and not
//! all zeros). If any is invalid, pages 1..=6 are (re)initialized and the
//! full bootstrap content is rewritten — an overwrite, so it is idempotent.
//! If all are valid, bootstrap is skipped entirely (second `open` does not
//! re-bootstrap).
//!
//! **M2a limitation**: the rewrite is a full overwrite of fixed content,
//! which is only safe because M2a catalog content is immutable after
//! bootstrap. Once Stage I adds DDL (user rows in `pg_class`), the
//! validity rule must distinguish "bootstrap rows present" from "page
//! initialized" before overwriting; do not carry this overwrite strategy
//! into Stage I unchanged.
//!
//! Bootstrap tuple headers use `t_xmin = TxnId(1)` — a synthetic bootstrap
//! transaction; M2 has no FrozenXid concept. `t_agent_id` is 0 and
//! `t_trace_id` is all zeros (no agent runtime in M2a), `t_cid` is 0, and
//! `t_ctid` points at the tuple itself.

use pg_am_heap::slotted_page::HEAP_SPECIAL_SIZE;
use pg_am_heap::tuple::{
    encode_tuple, Datum, TupleHeader, HEAP_ONLY_TUPLE, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED,
};
use pg_am_heap::SlottedPage;
use pg_storage::engine::StorageEngine;
use pg_storage::page::{PAGE_FORMAT_VERSION, PAGE_HEADER_SIZE};
use pg_storage::types::{Tid, TxnId, PAGE_SIZE};

use crate::builtin_types::BUILTIN_TYPES;
use crate::system_tables::{
    SystemTableDef, BTREE_AM_OID, HEAP_AM_OID, HNSW_AM_OID, LAST_SYSTEM_PAGE, RELKIND_TABLE,
    SYSTEM_TABLES,
};
use crate::Result;

/// Transaction ID stamped on bootstrap tuples. M2 has no FrozenXid; this
/// synthetic "bootstrap transaction" is always considered committed (the
/// tuples also carry the `HEAP_XMIN_COMMITTED` hint bit).
pub const BOOTSTRAP_XID: TxnId = TxnId(1);

/// Run catalog bootstrap if the data directory needs it.
///
/// Returns `Ok(true)` if bootstrap content was written, `Ok(false)` if a
/// valid catalog was already present.
///
/// Header-level damage (a system page that is not even a well-formed slotted
/// page) triggers a full rewrite — but only for a bootstrap-only catalog.
/// When user DDL is present the rewrite would orphan every user table, so
/// this fails loudly instead, matching [`crate::catalog::Catalog::open`]'s
/// self-heal gate (Stage K review P1-2: this path runs *before* that gate
/// and must apply the same policy).
pub fn bootstrap_if_needed(engine: &StorageEngine) -> Result<bool> {
    // Allocate first: on a fresh directory the system pages do not exist
    // yet, and the validity check pins them.
    ensure_system_pages_allocated(engine)?;

    if system_pages_are_valid(engine)? {
        return Ok(false);
    }

    if crate::catalog::user_ddl_present(engine) {
        return Err(crate::CatalogError::Corrupted(
            "system catalog page header is corrupt and user DDL rows exist; \
             force_rebootstrap would orphan every user table — refusing to \
             open to preserve the data directory (M2b replaces this with \
             catalog WAL replay)"
                .to_string(),
        ));
    }

    write_all_system_tables(engine)
}

/// Force a full rewrite of the bootstrap content, skipping the validity
/// check, then checkpoint. Returns `Ok(true)` for symmetry with
/// [`bootstrap_if_needed`].
///
/// This is the self-healing path for damage that passes the slotted-page
/// validity check but does not decode into the expected system tables
/// (torn write / bit rot landing in valid-looking bytes — e.g. garbage
/// line pointers whose flags happen to decode as `Dead`, silently hiding
/// every row). `Catalog::open` calls this when read-back or content
/// validation fails, then retries once.
///
/// M2a-only escape hatch: a full overwrite is safe only because bootstrap
/// content is fixed (see the module-level "M2a limitation" note).
///
/// Note: the internal `trigger_checkpoint` intentionally runs before the
/// catalog's validated OID allocator is wired (see `Catalog::open` — wiring
/// happens only after content validation passes, so a garbage-derived value
/// can never be persisted). The checkpoint therefore persists the
/// coordinator's default counter, which is seeded from the superblock and
/// is always safe.
pub fn force_rebootstrap(engine: &StorageEngine) -> Result<bool> {
    write_all_system_tables(engine)
}

/// Initialize all six system pages, write the full bootstrap content, and
/// checkpoint it to disk.
fn write_all_system_tables(engine: &StorageEngine) -> Result<bool> {
    ensure_system_pages_allocated(engine)?;
    for def in SYSTEM_TABLES {
        write_system_table(engine, &def)?;
    }
    engine.trigger_checkpoint()?;
    Ok(true)
}

/// The half-state detection rule from the module docs: **every** system
/// table's first page must be a valid slotted page written by
/// `SlottedPage::init_with_special`. The rule is header-only and therefore
/// insensitive to the special-space size.
///
/// All six pages are checked, not just `pg_class`: a background checkpoint
/// can flush a strict subset of the bootstrap pages (`flush_frame` does not
/// consult pin counts). With a page-1-only check, a crash after such a
/// partial flush would leave `pg_class` valid but `pg_attribute` / `pg_type`
/// / `pg_am` zeroed — the half-state would go undetected and the catalog
/// would load with relations but no columns, types, or AMs.
fn system_pages_are_valid(engine: &StorageEngine) -> Result<bool> {
    for def in SYSTEM_TABLES {
        if !page_is_valid_slotted(engine, def.first_page)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// One page of the validity rule: `pd_pagesize_version` matches,
/// `pd_lower` covers the header, and the page is not all zeros.
fn page_is_valid_slotted(
    engine: &StorageEngine,
    page_id: pg_storage::types::PageId,
) -> Result<bool> {
    let guard = engine.buffer_pool().pin(page_id)?;
    let page: &[u8; PAGE_SIZE] = guard
        .page()
        .try_into()
        .expect("buffer pool pages are PAGE_SIZE bytes");
    let header = SlottedPage::header(page);
    Ok(header.pd_pagesize_version == PAGE_FORMAT_VERSION
        && header.pd_lower as usize >= PAGE_HEADER_SIZE
        && !page.iter().all(|&b| b == 0))
}

/// Allocate pages 1..=6 if the page allocator has not handed them out yet.
///
/// A fresh directory starts at `next_page_id = 1`, so this allocates all
/// six system pages. A half-state directory (pages allocated by a crashed
/// bootstrap, replayed from WAL during recovery) already has
/// `next_page_id > 6` and this is a no-op; a partially allocated directory
/// gets only the missing pages.
fn ensure_system_pages_allocated(engine: &StorageEngine) -> Result<()> {
    loop {
        let next = engine.page_allocator().lock().next_page_id();
        if next > LAST_SYSTEM_PAGE {
            return Ok(());
        }
        // `new_page` goes through the page allocator (sequential IDs from
        // `next_page_id` when the freelist is empty) and returns the page
        // pinned zero-filled; `SlottedPage::init_with_special` runs later in
        // `write_system_table`.
        drop(engine.buffer_pool().new_page()?);
    }
}

/// Initialize `def`'s first page and write all of its bootstrap rows.
///
/// The page is initialized with [`HEAP_SPECIAL_SIZE`] bytes of special space
/// (Stage K wave 2), exactly like a user heap page: every relation — system
/// catalogs included — is then chain-extensible, and the heap AM can write
/// catalog rows itself (its chain walk requires the 16-byte special
/// geometry and would fail on a `special = 0` page).
fn write_system_table(engine: &StorageEngine, def: &SystemTableDef) -> Result<()> {
    let mut guard = engine.buffer_pool().pin_mut(def.first_page)?;
    let page: &mut [u8; PAGE_SIZE] = guard
        .page_mut()
        .try_into()
        .expect("buffer pool pages are PAGE_SIZE bytes");
    SlottedPage::init_with_special(page, HEAP_SPECIAL_SIZE);

    let columns = def.column_types();
    for (slot, row) in bootstrap_rows(def).iter().enumerate() {
        // Slots are handed out sequentially from 0 on a fresh page, so the
        // self-referencing t_ctid is known before insert.
        let ctid = Tid {
            page_id: def.first_page,
            slot_id: slot as u16,
        };
        let mut header = TupleHeader::new(BOOTSTRAP_XID, TxnId::INVALID, 0, [0; 16], ctid, 0);
        // Bootstrap tuples are committed and are the only version.
        header.t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
        header.t_infomask2 = HEAP_ONLY_TUPLE;
        let bytes = encode_tuple(header, &columns, row)?;
        let inserted = SlottedPage::add_tuple(page, &bytes)?;
        debug_assert_eq!(inserted, slot as u16);
    }
    Ok(())
}

/// Build the bootstrap rows of one system table, in insert order.
///
/// `pg_class` is self-describing: it gets a row for every system table,
/// including itself. `pg_index` gets no rows in M2a (§5.1: indexes arrive in
/// M2b) and `pg_rust_relpages` gets none until DDL writes them (Stage K);
/// both pages are initialized empty.
fn bootstrap_rows(def: &SystemTableDef) -> Vec<Vec<Option<Datum>>> {
    match def.oid {
        crate::system_tables::PG_CLASS_OID => SYSTEM_TABLES
            .iter()
            .map(|t| {
                vec![
                    Some(Datum::Int8(t.oid.raw().0 as i64)),
                    Some(Datum::Text(t.name.to_string())),
                    Some(Datum::Text(RELKIND_TABLE.to_string())),
                    Some(Datum::Int4(t.columns.len() as i32)),
                    // No TOAST table for system catalogs.
                    Some(Datum::Int8(0)),
                    Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
                ]
            })
            .collect(),
        crate::system_tables::PG_ATTRIBUTE_OID => SYSTEM_TABLES
            .iter()
            .flat_map(|t| {
                t.columns.iter().enumerate().map(move |(i, col)| {
                    vec![
                        Some(Datum::Int8(t.oid.raw().0 as i64)),
                        Some(Datum::Text(col.name.to_string())),
                        Some(Datum::Int8(col.type_oid.raw().0 as i64)),
                        Some(Datum::Int4(col.len)),
                        Some(Datum::Int4(i as i32 + 1)),
                        // M2 has no bool type: 0/1 in an Int4 column (§5.1).
                        Some(Datum::Int4(col.not_null as i32)),
                        Some(Datum::Int4(!col.not_null as i32)),
                    ]
                })
            })
            .collect(),
        crate::system_tables::PG_TYPE_OID => {
            let mut rows: Vec<_> = BUILTIN_TYPES
                .iter()
                .map(|ty| {
                    vec![
                        Some(Datum::Int8(ty.oid.raw().0 as i64)),
                        Some(Datum::Text(ty.name.to_string())),
                        Some(Datum::Int4(ty.len)),
                    ]
                })
                .collect();
            // M6 Stage C: the vector type row (typlen -1 — the dim is
            // per-column and travels in pg_attribute.len, not here).
            rows.push(vec![
                Some(Datum::Int8(crate::builtin_types::VECTOR_OID.raw().0 as i64)),
                Some(Datum::Text("vector".to_string())),
                Some(Datum::Int4(-1)),
            ]);
            rows
        }
        crate::system_tables::PG_AM_OID => vec![
            vec![
                Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
                Some(Datum::Text("heap".to_string())),
            ],
            vec![
                Some(Datum::Int8(BTREE_AM_OID.0 as i64)),
                Some(Datum::Text("btree".to_string())),
            ],
            vec![
                Some(Datum::Int8(HNSW_AM_OID.0 as i64)),
                Some(Datum::Text("hnsw".to_string())),
            ],
        ],
        // pg_index / pg_rust_relpages (and any future table): schema only, no
        // bootstrap rows.
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_storage::config::StorageConfig;
    use pg_storage::types::PageId;

    #[test]
    fn bootstrap_row_counts() {
        assert_eq!(bootstrap_rows(&crate::system_tables::PG_CLASS).len(), 6);
        // 6 + 7 + 3 + 2 + 5 + 4 columns across the six system tables.
        assert_eq!(
            bootstrap_rows(&crate::system_tables::PG_ATTRIBUTE).len(),
            6 + 7 + 3 + 2 + 5 + 4
        );
        assert_eq!(bootstrap_rows(&crate::system_tables::PG_TYPE).len(), 7);
        assert_eq!(bootstrap_rows(&crate::system_tables::PG_AM).len(), 3);
        assert!(bootstrap_rows(&crate::system_tables::PG_INDEX).is_empty());
        assert!(bootstrap_rows(&crate::system_tables::PG_RELPAGES).is_empty());
    }

    #[test]
    fn bootstrap_is_idempotent_and_fills_fixed_pages() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        assert!(bootstrap_if_needed(&engine).unwrap());
        // Second call on the same engine: the catalog is already valid.
        assert!(!bootstrap_if_needed(&engine).unwrap());

        // Pages 1..=6 were allocated, in order.
        assert_eq!(engine.page_allocator().lock().next_page_id(), PageId(7));
    }
}
