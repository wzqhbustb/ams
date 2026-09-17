//! In-memory catalog snapshot (tech-selection §五).
//!
//! [`Catalog::open`] bootstraps the system tables if needed
//! ([`crate::bootstrap`]), then reads them back from their fixed first pages
//! into plain in-memory structures and offers read-only queries. It also owns
//! the [`OidAllocator`]: the allocator is loaded with the corrected starting
//! OID and wired into the checkpoint coordinator so `next_oid` is persisted
//! on every checkpoint.
//!
//! M2a loads the catalog once at open time. No `arc-swap` snapshot
//! replacement is introduced: nothing mutates the catalog after open in M2a
//! (DDL lands in Stage I+), so there is no atomic-swap requirement yet. The
//! coding plan lists `arc-swap` as "introduce when needed" — it is not needed
//! here.

use std::collections::BTreeMap;

use pg_am_heap::tuple::{decode_tuple, Datum};
use pg_am_heap::SlottedPage;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, PAGE_SIZE};

use crate::bootstrap;
use crate::oid::OidAllocator;
use crate::system_tables::{SystemTableDef, PG_AM, PG_ATTRIBUTE, PG_CLASS, PG_RELPAGES, PG_TYPE};
use crate::{CatalogError, Result, TableOid, TypeOid};

/// A row of `pg_class`: one relation.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationRow {
    /// The relation's OID.
    pub oid: TableOid,
    /// `relname`.
    pub name: String,
    /// `relkind` (`"r"` = table, `"i"` = index).
    pub kind: String,
    /// `relnatts`: number of columns.
    pub natts: i32,
    /// `reltoastrelid`: OID of the TOAST table, or [`Oid::INVALID`].
    pub toastrelid: Oid,
    /// `relam`: OID of the access method (`pg_am`).
    pub am: Oid,
}

/// A row of `pg_attribute`: one column of one relation.
#[derive(Debug, Clone, PartialEq)]
pub struct AttributeRow {
    /// `attrelid`: the relation this column belongs to.
    pub rel: TableOid,
    /// `attname`.
    pub name: String,
    /// `atttypid`.
    pub type_oid: TypeOid,
    /// `attlen`: fixed width in bytes, `-1` for varlena.
    pub len: i32,
    /// `attnum`: 1-based column position.
    pub num: i32,
    /// `attnotnull` (stored as Int4 0/1; M2 has no bool type).
    pub not_null: bool,
    /// `attnullable` (stored as Int4 0/1).
    pub nullable: bool,
}

/// A row of `pg_type`: one data type.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeRow {
    /// The type's OID.
    pub oid: TypeOid,
    /// `typname`.
    pub name: String,
    /// `typlen`: fixed width in bytes, `-1` for varlena.
    pub len: i32,
}

/// A row of `pg_am`: one access method.
#[derive(Debug, Clone, PartialEq)]
pub struct AmRow {
    /// The access method's OID.
    pub oid: Oid,
    /// `amname`.
    pub name: String,
}

/// A row of `pg_rust_relpages` (Stage K): the engine-private page-chain
/// directory entry of one heap relation.
#[derive(Debug, Clone, PartialEq)]
pub struct RelpagesRow {
    /// The relation this entry describes.
    pub rel_oid: TableOid,
    /// Head of the relation's on-disk page chain.
    pub first_page: PageId,
    /// Tail of the relation's on-disk page chain.
    pub last_page: PageId,
    /// Number of pages in the chain (`>= 1`).
    pub page_count: u64,
}

/// The in-memory system catalog.
///
/// Construct with [`Catalog::open`]; all queries are read-only lookups into
/// the snapshot loaded at open time.
#[derive(Debug)]
pub struct Catalog {
    relations: Vec<RelationRow>,
    attributes: BTreeMap<TableOid, Vec<AttributeRow>>,
    types: Vec<TypeRow>,
    access_methods: Vec<AmRow>,
    relpages: Vec<RelpagesRow>,
    oid_allocator: OidAllocator,
    bootstrapped: bool,
}

impl Catalog {
    /// Open the catalog on a storage engine, bootstrapping it first if the
    /// data directory is empty (or holds a crashed half-state).
    ///
    /// The OID allocator is loaded with
    /// `max(superblock.next_oid, max_oid_in_catalog + 1, Oid::FIRST_USER)`
    /// — the startup correction required by the coding plan's next_oid
    /// rollback-window warning — and registered with the checkpoint
    /// coordinator so every checkpoint persists the current value into the
    /// v2 superblock.
    ///
    /// # Self-healing on corrupted content
    ///
    /// The slotted-page validity check in [`crate::bootstrap`] only proves
    /// the system pages are well-formed pages, not that their content
    /// decodes into the expected system tables: bit rot whose line pointers
    /// happen to decode as `Dead` silently hides every row, and a torn
    /// tuple region fails decode outright. After read-back, the snapshot is
    /// therefore validated against the fixed bootstrap content
    /// (`Catalog::validate_content`).
    ///
    /// A validation failure splits two ways (Stage K):
    ///
    /// - **Bootstrap-only catalog** (no user tables): the content is fixed
    ///   and known, so one [`bootstrap::force_rebootstrap`] + retry repairs
    ///   it for free.
    /// - **User DDL present**: a full rewrite would wipe every user row in
    ///   `pg_class` / `pg_attribute` / `pg_rust_relpages`, orphaning every
    ///   user table. That is strictly worse than refusing to open, so a
    ///   decodable user row turns the failure into a hard error that
    ///   preserves the data directory for manual recovery (M2b replaces this
    ///   with catalog WAL replay).
    pub fn open(engine: &StorageEngine) -> Result<Self> {
        let bootstrapped = bootstrap::bootstrap_if_needed(engine)?;

        match Self::read_validated(engine) {
            Ok(mut catalog) => {
                catalog.bootstrapped = bootstrapped;
                Ok(catalog)
            }
            Err(first_err) => {
                if user_ddl_present(engine) {
                    return Err(CatalogError::Corrupted(format!(
                        "catalog corruption ({first_err}); user DDL rows exist, \
                         so force_rebootstrap would orphan every user table — \
                         refusing to open to preserve the data directory \
                         (M2b replaces this with catalog WAL replay)"
                    )));
                }
                tracing::warn!(
                    error = %first_err,
                    "catalog content failed validation; rewriting bootstrap content and retrying"
                );
                bootstrap::force_rebootstrap(engine)?;
                let mut catalog = Self::read_validated(engine)?;
                catalog.bootstrapped = true;
                Ok(catalog)
            }
        }
    }

    /// Read the system tables back and validate the snapshot against the
    /// fixed bootstrap content.
    fn read_validated(engine: &StorageEngine) -> Result<Self> {
        let relations = read_relations(engine)?;
        let attributes = read_attributes(engine)?;
        let types = read_types(engine)?;
        let access_methods = read_access_methods(engine)?;
        let relpages = read_relpages(engine)?;

        validate_content(&relations, &attributes, &types, &access_methods)?;

        // Startup correction for the next_oid crash-rollback window (see
        // crate::oid): OIDs written to catalog pages after the last
        // checkpoint survive in those pages while superblock.next_oid rolls
        // back, so the allocator must start past every OID already in use.
        //
        // The allocator is wired into the checkpoint coordinator only AFTER
        // validation passes: a garbage catalog can decode huge-but-"legal"
        // OID values (`oid_of` only rejects negatives), and wiring such a
        // value would let the checkpoint inside `force_rebootstrap` persist
        // it into the superblock — poisoning `next_oid` permanently, since
        // this same max() can never go back down.
        // 2026-09-17, review round 4 P3-2 + round 5 P3-1: relpages OIDs
        // feed `max_in_use` (round-3 P1 fix) but bypass `validate_content`,
        // and `oid_of` only rejects negatives. Garbage-huge values are
        // rejected against the sane ceiling (see `MAX_SANE_OID`) instead of
        // poisoning `next_oid`: the next checkpoint would persist the
        // garbage start permanently (the original threat of :190-195), a
        // start past i64::MAX would brick the next restart's catalog open
        // (`Oid.0 as i64` goes negative, engine.rs:1370), and
        // `OidCounter::alloc` wraps silently at u64::MAX
        // (pg-storage/src/oid.rs:32).
        check_relpages_oid_bounds(&relpages)?;
        let max_in_use = relations
            .iter()
            .map(|r| r.oid.raw())
            .chain(types.iter().map(|t| t.oid.raw()))
            .chain(access_methods.iter().map(|a| a.oid))
            // 2026-09-17, Stage B review round 3 P1: the HNSW minimal
            // registration (engine.rs create_hnsw_index) writes ONLY a
            // pg_rust_relpages row — the first "OID on a catalog page but
            // not in pg_class" path in the system. Without this chain the
            // startup correction is blind to such OIDs: after any restart
            // without a checkpoint (clean drops included — nothing
            // auto-checkpoints), next_oid rolls back and re-issues the
            // same OID to the next HNSW index, making the second index
            // unreachable by OID (reproduced live: two creates across a
            // reopen both got Oid(16384)).
            .chain(relpages.iter().map(|r| r.rel_oid.raw()))
            .max()
            .map(|m| Oid(m.0.saturating_add(1)));
        let start = [Some(engine.next_oid()), max_in_use, Some(Oid::FIRST_USER)]
            .into_iter()
            .flatten()
            .max()
            .expect("FIRST_USER is always present");

        let oid_allocator = OidAllocator::load(start);
        engine.set_next_oid_source(oid_allocator.shared_counter());

        Ok(Self {
            relations,
            attributes,
            types,
            access_methods,
            relpages,
            oid_allocator,
            bootstrapped: false,
        })
    }

    /// All relations (`pg_class` rows), in bootstrap order.
    pub fn relations(&self) -> &[RelationRow] {
        &self.relations
    }

    /// Look up a relation by OID.
    pub fn relation(&self, oid: TableOid) -> Option<&RelationRow> {
        self.relations.iter().find(|r| r.oid == oid)
    }

    /// Look up a relation by name.
    pub fn relation_by_name(&self, name: &str) -> Option<&RelationRow> {
        self.relations.iter().find(|r| r.name == name)
    }

    /// The columns of a relation (`pg_attribute` rows), in `attnum` order.
    /// Returns an empty slice for an unknown relation.
    pub fn attributes_of(&self, table: TableOid) -> &[AttributeRow] {
        self.attributes
            .get(&table)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// All types (`pg_type` rows).
    pub fn types(&self) -> &[TypeRow] {
        &self.types
    }

    /// Look up a type by OID.
    pub fn type_by_oid(&self, oid: TypeOid) -> Option<&TypeRow> {
        self.types.iter().find(|t| t.oid == oid)
    }

    /// All access methods (`pg_am` rows).
    pub fn access_methods(&self) -> &[AmRow] {
        &self.access_methods
    }

    /// All page-chain directory entries (`pg_rust_relpages` rows, Stage K).
    /// Empty until DDL creates user tables.
    pub fn relpages(&self) -> &[RelpagesRow] {
        &self.relpages
    }

    /// Look up the page-chain directory entry of a relation.
    pub fn relpages_of(&self, rel_oid: TableOid) -> Option<RelpagesRow> {
        self.relpages.iter().find(|r| r.rel_oid == rel_oid).cloned()
    }

    /// The OID allocator, shared with the checkpoint coordinator.
    pub fn oid_allocator(&self) -> &OidAllocator {
        &self.oid_allocator
    }

    /// Whether [`Catalog::open`] ran the bootstrap (fresh or half-state
    /// directory) or found an already-valid catalog.
    pub fn was_bootstrapped(&self) -> bool {
        self.bootstrapped
    }
}

/// Verify the snapshot contains the fixed bootstrap content: every
/// system relation by OID with attribute rows matching the hardcoded
/// schema, all built-in types, and both access methods.
///
/// Extra rows (e.g. user tables from Stage I+ DDL) are tolerated — only
/// the system content is checked.
fn validate_content(
    relations: &[RelationRow],
    attributes: &BTreeMap<TableOid, Vec<AttributeRow>>,
    types: &[TypeRow],
    access_methods: &[AmRow],
) -> Result<()> {
    for def in crate::system_tables::SYSTEM_TABLES {
        let rel = relations.iter().find(|r| r.oid == def.oid).ok_or_else(|| {
            CatalogError::Corrupted(format!("catalog is missing system relation {}", def.name))
        })?;
        let attrs: &[AttributeRow] = attributes.get(&def.oid).map(Vec::as_slice).unwrap_or(&[]);
        if rel.natts as usize != def.columns.len() || attrs.len() != def.columns.len() {
            return Err(CatalogError::Corrupted(format!(
                "{}: relnatts={} and {} attribute rows do not match schema ({} columns)",
                def.name,
                rel.natts,
                attrs.len(),
                def.columns.len()
            )));
        }
        for (attr, col) in attrs.iter().zip(def.columns.iter()) {
            if attr.name != col.name || attr.type_oid != col.type_oid {
                return Err(CatalogError::Corrupted(format!(
                    "{}.{}: schema mismatch (got {:?}/{:?})",
                    def.name, col.name, attr.name, attr.type_oid
                )));
            }
        }
    }
    for ty in crate::builtin_types::BUILTIN_TYPES {
        let row = types.iter().find(|t| t.oid == ty.oid).ok_or_else(|| {
            CatalogError::Corrupted(format!("catalog is missing builtin type {}", ty.name))
        })?;
        if row.name != ty.name || row.len != ty.len {
            return Err(CatalogError::Corrupted(format!(
                "pg_type row for {} does not match builtin definition",
                ty.name
            )));
        }
    }
    for am_oid in [
        crate::system_tables::HEAP_AM_OID,
        crate::system_tables::BTREE_AM_OID,
    ] {
        if !access_methods.iter().any(|a| a.oid == am_oid) {
            return Err(CatalogError::Corrupted(format!(
                "catalog is missing access method {am_oid}"
            )));
        }
    }
    Ok(())
}

/// Read all live tuples of a system table's first page and decode them with
/// the hardcoded schema.
fn read_tuples(engine: &StorageEngine, def: &SystemTableDef) -> Result<Vec<Vec<Option<Datum>>>> {
    let guard = engine.buffer_pool().pin(def.first_page)?;
    let page: &[u8; PAGE_SIZE] = guard
        .page()
        .try_into()
        .expect("buffer pool pages are PAGE_SIZE bytes");
    let columns = def.column_types();
    let mut rows = Vec::new();
    for slot in 0..SlottedPage::slot_count(page) {
        if let Some(bytes) = SlottedPage::tuple(page, slot as u16)? {
            let (_header, values) = decode_tuple(bytes, &columns)?;
            rows.push(values);
        }
    }
    Ok(rows)
}

/// Extract an `Int8` column value; anything else is catalog corruption.
fn int8_col(row: &[Option<Datum>], idx: usize, table: &str, col: &str) -> Result<i64> {
    match row.get(idx) {
        Some(Some(Datum::Int8(v))) => Ok(*v),
        other => Err(CatalogError::Corrupted(format!(
            "{table}.{col}: expected Int8, got {other:?}"
        ))),
    }
}

/// Extract an `Int4` column value; anything else is catalog corruption.
fn int4_col(row: &[Option<Datum>], idx: usize, table: &str, col: &str) -> Result<i32> {
    match row.get(idx) {
        Some(Some(Datum::Int4(v))) => Ok(*v),
        other => Err(CatalogError::Corrupted(format!(
            "{table}.{col}: expected Int4, got {other:?}"
        ))),
    }
}

/// Extract a `Text` column value; anything else is catalog corruption.
fn text_col(row: &[Option<Datum>], idx: usize, table: &str, col: &str) -> Result<String> {
    match row.get(idx) {
        Some(Some(Datum::Text(v))) => Ok(v.clone()),
        other => Err(CatalogError::Corrupted(format!(
            "{table}.{col}: expected Text, got {other:?}"
        ))),
    }
}

/// Tolerantly scan `pg_class` / `pg_attribute` / `pg_rust_relpages` for any
/// decodable row owned by a user relation (OID ≥ `FIRST_USER`).
///
/// Used by `Catalog::open`'s self-heal gate (see the `open` docs): a full
/// rebootstrap is only allowed when the catalog is provably bootstrap-only.
/// Slots that fail to decode are skipped with a warning — the corruption is
/// what brought us here — while every *readable* row still counts, so a
/// single undamaged system page is enough to detect existing user tables.
pub(crate) fn user_ddl_present(engine: &StorageEngine) -> bool {
    for def in [PG_CLASS, PG_ATTRIBUTE, PG_RELPAGES] {
        let guard = match engine.buffer_pool().pin(def.first_page) {
            Ok(g) => g,
            Err(_) => continue,
        };
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        if SlottedPage::header(page).pd_upper == 0 {
            continue;
        }
        let columns = def.column_types();
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Ok(Some(bytes)) = SlottedPage::tuple(page, slot) else {
                continue;
            };
            match decode_tuple(bytes, &columns) {
                Ok((_header, values)) => {
                    // All three tables carry the owner OID as the first Int8
                    // column (pg_class.oid / pg_attribute.attrelid /
                    // pg_rust_relpages.rel_oid).
                    if let Some(Some(Datum::Int8(v))) = values.first() {
                        if *v >= Oid::FIRST_USER.0 as i64 {
                            return true;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        table = def.name,
                        slot,
                        "user_ddl_present: skipping undecodable row"
                    );
                }
            }
        }
    }
    false
}

fn read_relations(engine: &StorageEngine) -> Result<Vec<RelationRow>> {
    read_tuples(engine, &PG_CLASS)?
        .iter()
        .map(|row| {
            Ok(RelationRow {
                oid: TableOid::new(oid_of(
                    int8_col(row, 0, "pg_class", "oid")?,
                    "pg_class",
                    "oid",
                )?),
                name: text_col(row, 1, "pg_class", "relname")?,
                kind: text_col(row, 2, "pg_class", "relkind")?,
                natts: int4_col(row, 3, "pg_class", "relnatts")?,
                toastrelid: oid_of(
                    int8_col(row, 4, "pg_class", "reltoastrelid")?,
                    "pg_class",
                    "reltoastrelid",
                )?,
                am: oid_of(int8_col(row, 5, "pg_class", "relam")?, "pg_class", "relam")?,
            })
        })
        .collect()
}

fn read_attributes(engine: &StorageEngine) -> Result<BTreeMap<TableOid, Vec<AttributeRow>>> {
    let mut map: BTreeMap<TableOid, Vec<AttributeRow>> = BTreeMap::new();
    for row in read_tuples(engine, &PG_ATTRIBUTE)? {
        let attr = AttributeRow {
            rel: TableOid::new(oid_of(
                int8_col(&row, 0, "pg_attribute", "attrelid")?,
                "pg_attribute",
                "attrelid",
            )?),
            name: text_col(&row, 1, "pg_attribute", "attname")?,
            type_oid: TypeOid::new(oid_of(
                int8_col(&row, 2, "pg_attribute", "atttypid")?,
                "pg_attribute",
                "atttypid",
            )?),
            len: int4_col(&row, 3, "pg_attribute", "attlen")?,
            num: int4_col(&row, 4, "pg_attribute", "attnum")?,
            not_null: int4_col(&row, 5, "pg_attribute", "attnotnull")? != 0,
            nullable: int4_col(&row, 6, "pg_attribute", "attnullable")? != 0,
        };
        map.entry(attr.rel).or_default().push(attr);
    }
    // Enforce the `attributes_of` "attnum order" contract explicitly. Slot
    // order happens to match attnum order for bootstrap-written rows, but
    // Stage I+ DDL may insert rows through other paths where it does not.
    for attrs in map.values_mut() {
        attrs.sort_by_key(|a| a.num);
    }
    Ok(map)
}

fn read_types(engine: &StorageEngine) -> Result<Vec<TypeRow>> {
    read_tuples(engine, &PG_TYPE)?
        .iter()
        .map(|row| {
            Ok(TypeRow {
                oid: TypeOid::new(oid_of(
                    int8_col(row, 0, "pg_type", "oid")?,
                    "pg_type",
                    "oid",
                )?),
                name: text_col(row, 1, "pg_type", "typname")?,
                len: int4_col(row, 2, "pg_type", "typlen")?,
            })
        })
        .collect()
}

fn read_access_methods(engine: &StorageEngine) -> Result<Vec<AmRow>> {
    read_tuples(engine, &PG_AM)?
        .iter()
        .map(|row| {
            Ok(AmRow {
                oid: oid_of(int8_col(row, 0, "pg_am", "oid")?, "pg_am", "oid")?,
                name: text_col(row, 1, "pg_am", "amname")?,
            })
        })
        .collect()
}

fn read_relpages(engine: &StorageEngine) -> Result<Vec<RelpagesRow>> {
    read_tuples(engine, &PG_RELPAGES)?
        .iter()
        .map(|row| {
            Ok(RelpagesRow {
                rel_oid: TableOid::new(oid_of(
                    int8_col(row, 0, "pg_rust_relpages", "rel_oid")?,
                    "pg_rust_relpages",
                    "rel_oid",
                )?),
                first_page: page_id_of(
                    int8_col(row, 1, "pg_rust_relpages", "first_page")?,
                    "first_page",
                )?,
                last_page: page_id_of(
                    int8_col(row, 2, "pg_rust_relpages", "last_page")?,
                    "last_page",
                )?,
                page_count: u64::try_from(int8_col(row, 3, "pg_rust_relpages", "page_count")?)
                    .map_err(|_| {
                        CatalogError::Corrupted(
                            "pg_rust_relpages.page_count: negative page count".to_string(),
                        )
                    })?,
            })
        })
        .collect()
}

/// Widen an `Int8` catalog value to an [`Oid`]. Negative values are
/// corruption — OIDs are unsigned.
fn oid_of(v: i64, table: &str, col: &str) -> Result<Oid> {
    u64::try_from(v)
        .map(Oid)
        .map_err(|_| CatalogError::Corrupted(format!("{table}.{col}: negative OID value {v}")))
}

/// Sane upper bound for a catalog OID (2026-09-17, review round 5 P3-1 —
/// replaces the round-4 Int8-bound guard, which was DEAD on the production
/// path and let the worst reachable value through). OIDs start at
/// `Oid::FIRST_USER` = 16384 and every allocation consumes exactly one —
/// even u32::MAX (~4.3e9, PostgreSQL's own OID width) is unreachable in
/// practice, so 2^48 ≈ 2.8e14 is beyond any legal value by a wide margin.
/// The bound must NOT be i64::MAX: `read_relpages` decodes through
/// `int8_col` (i64) → `oid_of`, so every reachable value is already
/// ≤ i64::MAX — a guard there rejects only unproducible values while
/// `i64::MAX` itself (the worst REACHABLE poison) passes: `max_in_use =
/// i64::MAX` → start = 2^63 → first `alloc` returns Oid(2^63) → the
/// engine writes `Oid.0 as i64` = i64::MIN (engine.rs:1370) → the next
/// restart's `oid_of` rejects it and the catalog cannot open.
const MAX_SANE_OID: u64 = 1 << 48;

/// Reject garbage-huge `pg_rust_relpages.rel_oid` values before they enter
/// the `next_oid` startup correction (round-3 P1 chained relpages into
/// `max_in_use`; without this bound the correction itself is a poisoning
/// channel — the next checkpoint persists whatever start it computes, and
/// `OidCounter::alloc` wraps silently at u64::MAX). Factored for direct
/// unit testing. The same exposure on pg_class user rows (extra rows carry
/// no OID upper bound either) is a pre-existing class, registered but out
/// of this fix's scope.
fn check_relpages_oid_bounds(relpages: &[RelpagesRow]) -> Result<()> {
    for r in relpages {
        if r.rel_oid.raw().0 > MAX_SANE_OID {
            return Err(CatalogError::Corrupted(format!(
                "pg_rust_relpages.rel_oid {} exceeds the sane OID ceiling {MAX_SANE_OID} — corrupt catalog row",
                r.rel_oid.raw().0
            )));
        }
    }
    Ok(())
}

/// Widen an `Int8` `pg_rust_relpages` value to a [`PageId`]. Negative values
/// are corruption — page ids are unsigned.
fn page_id_of(v: i64, col: &str) -> Result<PageId> {
    u64::try_from(v).map(PageId).map_err(|_| {
        CatalogError::Corrupted(format!("pg_rust_relpages.{col}: negative page id {v}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TableOid;
    use pg_storage::types::PageId;

    /// 2026-09-17, review round 5 P3-1: the sane-OID ceiling — values above
    /// 2^48 are definitionally garbage (OIDs start at 16384, one per alloc;
    /// even u32::MAX is unreachable). i64::MAX MUST be rejected: it is the
    /// worst REACHABLE poison (Int8 decode caps at i64::MAX) — accepting it
    /// pushes `next_oid` to 2^63, whose `as i64` write-back is negative and
    /// bricks the next restart's catalog open. (The round-4 Int8-bound
    /// guard pinned i64::MAX as Ok — exactly the value that must fail.)
    #[test]
    fn relpages_oid_bound_rejects_garbage_huge_values() {
        let row = |oid: u64| RelpagesRow {
            rel_oid: TableOid::new(Oid(oid)),
            first_page: PageId(1),
            last_page: PageId(1),
            page_count: 1,
        };
        check_relpages_oid_bounds(&[row(16_384)]).unwrap();
        check_relpages_oid_bounds(&[row(MAX_SANE_OID)]).unwrap();
        assert!(check_relpages_oid_bounds(&[row(MAX_SANE_OID + 1)]).is_err());
        // The worst reachable poison must fail (round-4's dead guard
        // accepted exactly this).
        let err = check_relpages_oid_bounds(&[row(i64::MAX as u64)]).unwrap_err();
        assert!(err.to_string().contains("sane OID ceiling"), "{err}");
        // A garbage row must not hide behind a legal predecessor.
        assert!(check_relpages_oid_bounds(&[row(16_384), row(u64::MAX)]).is_err());
    }
}
