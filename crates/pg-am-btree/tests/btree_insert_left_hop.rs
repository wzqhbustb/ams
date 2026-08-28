//! Regression test for the Stage T deadlock-stress `EntryNotFound` failure
//! (`m2c_deadlock_injection_stress`, control group): an INSERT that hops
//! LEFT past the separator owning its key.
//!
//! # The bug
//!
//! An update's index maintenance is delete-old-then-insert-new of the same
//! key. When the key is the separator of a split twin, the delete raises the
//! twin's first entry above the key; the follow-up insert's descent then
//! found the twin "dominated" (its first `(key, tid)` sorts above the probe)
//! and hopped LEFT, writing the new entry into the left sibling — LEFT of
//! the separator that owns the key. Every update of the boundary keys
//! repeated this, draining the twin until it was EMPTY. An empty leaf
//! terminates every ownership walk (never "dominated", and an empty right
//! sibling owns nothing), so later probes for the migrated keys black-holed
//! on the empty page: `lookup` missed entries sitting on the left leaf and
//! `delete` returned a spurious `EntryNotFound` — which is exactly what the
//! acyclic control group of the deadlock stress surfaced. (Once a leaf holds
//! keys from its right sibling's range, ITS next split also splices a twin
//! whose range overlaps the older twins — the chain falls out of key order
//! and the tree degrades beyond local repair.)
//!
//! # The fix
//!
//! Insert positioning never hops left (`descend_to_leaf_for_insert` /
//! `pin_leaf_for_write` / `descend_write_path` with `allow_left_hop =
//! false`): a new entry always lands on the leaf whose separator range
//! contains its key. Locating EXISTING entries (lookup/delete) keeps left
//! hops, because split-boundary duplicates can legitimately sit on either
//! side of a separator.
//!
//! # This test
//!
//! Deterministic, single-threaded reproduction of the exact interleaving:
//! build one leaf, force a median root split through the public
//! Prepare/Copy/Commit steps, then replay update-style delete+insert churn
//! on every key of the twin. Pre-fix the churn migrates every twin key left
//! and the final lookups/deletes fail; post-fix the twin keeps its entries
//! and the tree validates.

use std::sync::Arc;

use pg_am_btree::{BTreeAM, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_402);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(51_000 + i),
        slot_id: i as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    pg_am_btree::encode_i32(i).to_vec()
}

#[test]
fn insert_never_migrates_an_entry_left_of_its_separator() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index: BTreeIndex = am.create_index(REL_OID, ColumnType::Int4).unwrap();

    // Fill the single leaf to ~50% so the post-churn left leaf cannot fill
    // up and split mid-scenario (that would change the choreography, not the
    // point).
    const ENTRY_BYTES: usize = 4 + 10 + 4; // i32 key + tid trailer + line pointer
    let capacity = index.page_free_space(index.root_page()).unwrap() / ENTRY_BYTES;
    let n = (capacity / 2) as i32;
    assert!(n > 100, "the scenario needs a meaningful key count");
    for i in 0..n {
        index.insert(&key(i), tid(i as u64)).unwrap();
    }

    // Force a median split of the root leaf through the public crash-test
    // steps: L keeps keys [0, n/2), the twin R gets [n/2, n) with separator
    // key n/2.
    let boundary = n / 2;
    let st = index.split_prepare(index.root_page()).unwrap();
    index.split_copy(&st).unwrap();
    let mut path = Vec::new();
    index.split_commit(&st, &mut path).unwrap();
    assert_eq!(
        index.tree_level(),
        1,
        "the forced split must promote a root"
    );

    // Update-style churn on every key of the twin: delete the old entry,
    // insert the same key with a fresh tid — exactly what an UPDATE's index
    // maintenance does when the indexed column is unchanged but the update
    // falls off the HOT path. Forward order: pre-fix every key except the
    // twin's last migrates left of the separator (each insert finds the
    // twin dominated — its first entry was just raised by the delete — and
    // hops left); the last key's insert lands back on the by-then-empty
    // twin, so R ends holding exactly {n-1}. Post-fix every key stays on R.
    for i in boundary..n {
        index.delete(&key(i), tid(i as u64)).unwrap();
        index.insert(&key(i), tid(10_000 + i as u64)).unwrap();
    }

    // Remove the twin's one remaining entry. Pre-fix this leaves R EMPTY
    // with its downlink (boundary -> R) still in the root — the black hole:
    // probes for the migrated keys descend straight onto the empty page and
    // every ownership walk terminates there. Post-fix R still holds
    // [boundary, n-1), so nothing below is affected.
    index
        .delete(&key(n - 1), tid(10_000 + (n - 1) as u64))
        .unwrap();

    // Every surviving key must still resolve, with the latest tid. Pre-fix
    // the lookups for [boundary, n-1) returned None (the entries sit on the
    // left leaf, unreachable past the empty twin) — the exact
    // `EntryNotFound` / missing-entry failure the deadlock stress surfaced.
    for i in 0..n - 1 {
        let want = if i < boundary {
            tid(i as u64)
        } else {
            tid(10_000 + i as u64)
        };
        assert_eq!(
            index.lookup(&key(i)).unwrap(),
            Some(want),
            "key {i} must resolve after the churn"
        );
    }

    // The reported failure itself: deleting a migrated entry must not be a
    // spurious EntryNotFound.
    index
        .delete(&key(boundary), tid(10_000 + boundary as u64))
        .unwrap();
    index.insert(&key(boundary), tid(20_000)).unwrap();
    assert_eq!(index.lookup(&key(boundary)).unwrap(), Some(tid(20_000)));

    // Structural health: sibling ranges strictly increasing, sort order
    // intact (catches the chain-order degradation the migration causes).
    index.validate().unwrap();
}
