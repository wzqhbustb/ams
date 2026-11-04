//! Phase 4a Stage A: the self-owned transactional-processing executor.
//!
//! The caller owns the snapshot and its SnapshotGuard. This module only
//! chooses and executes a ScanPlan; it never creates a fresh snapshot and
//! never commits or aborts a transaction.

use pg_am_btree::encode_key;
use pg_am_heap::tuple::ColumnType;
use pg_storage::types::Tid;
use pg_txn::Snapshot;

use crate::engine::{column_types, filter_to_predicate, Engine, TableEntry, Value};
use crate::error::{EngineError, Result};
use crate::planner::{plan_for_predicate, AmKind, IndexMetadata, ScanPlan, ScanRequest, ScanSpec};
use crate::sql::{CmpOp, Filter, OrderByKind};

#[cfg(test)]
thread_local! {
    static POST_BTREE_SCAN_HOOK: std::cell::Cell<Option<fn(&Engine)>> =
        std::cell::Cell::new(None);
}

/// Materialized `(visible_tid, decoded_values)` rows.
type ScanRows = Vec<(Tid, Vec<Value>)>;

/// The materialized rows and the plan that produced them.
pub(crate) struct ScanOutput {
    /// Rows in the access method's natural candidate order. ORDER BY and
    /// LIMIT are applied by the shared outer SELECT path.
    pub(crate) rows: Vec<(Tid, Vec<Value>)>,
    /// The plan whose ScanSpec produced these rows.
    pub(crate) plan: ScanPlan,
}

/// Plan and execute one ordinary SELECT scan against the caller's snapshot.
pub(crate) fn execute(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
    order_by: &Option<OrderByKind>,
    limit: &Option<usize>,
) -> Result<ScanOutput> {
    let request = build_request(engine, table, entry, filter, order_by, limit);
    let mut plan = plan_for_predicate(&request)?;
    let mut fallback_reason = None;
    let rows = match &plan.spec {
        ScanSpec::BTree(spec) if snapshot_allows_btree(engine, snap) => {
            let btree_filter = spec
                .predicate
                .as_ref()
                .ok_or_else(|| EngineError::Corrupted("BTree plan has no predicate".to_string()))?;
            match execute_btree(engine, snap, table, entry, btree_filter)? {
                Some(rows) => recheck_btree(
                    engine,
                    snap,
                    table,
                    entry,
                    filter,
                    rows,
                    &mut fallback_reason,
                )?,
                // A query-literal size fallback must not be followed by a
                // second, unrelated snapshot recheck or a different reason.
                None => {
                    fallback_reason = Some(crate::planner::SeqScanReason::KeyTooLarge);
                    execute_seq(engine, snap, table, entry, filter)?
                }
            }
        }
        ScanSpec::BTree(_) => {
            fallback_reason = Some(crate::planner::SeqScanReason::SnapshotFallback);
            execute_seq(engine, snap, table, entry, filter)?
        }
        ScanSpec::Seq(spec) => execute_seq(engine, snap, table, entry, &spec.predicate)?,
        ScanSpec::Hnsw(_) => {
            return Err(EngineError::InvalidArgument(
                "HNSW execution is not yet supported in this select path (Stage B)".to_string(),
            ))
        }
    };
    if let Some(reason) = fallback_reason {
        plan.spec = ScanSpec::Seq(crate::planner::SeqScanSpec {
            predicate: filter.clone(),
            order_by: order_by.clone(),
            limit: *limit,
            est_rows: 0,
            est_pages: 0,
            reason,
        });
    }
    Ok(ScanOutput { rows, plan })
}

fn build_request(
    engine: &Engine,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
    order_by: &Option<OrderByKind>,
    limit: &Option<usize>,
) -> ScanRequest {
    let indexes = engine
        .indexes_of(entry)
        .into_iter()
        .map(|(idx, _)| IndexMetadata {
            kind: if matches!(idx.key_type, ColumnType::Vector(_)) {
                AmKind::Hnsw
            } else {
                AmKind::BTree
            },
            column: idx.column,
            // Stage B fills the HNSW opclass and ef metadata. Ordinary
            // B+Tree planning needs neither field.
            opclass: None,
            ef_search_default: None,
        })
        .collect();
    ScanRequest {
        table: table.to_string(),
        predicate: filter.clone(),
        order_by: order_by.clone(),
        limit: *limit,
        indexes,
    }
}

/// Whether this snapshot can use an eager-removal B+Tree as a complete
/// candidate source. Old snapshots and snapshots with other in-flight
/// transactions must use the heap: an index delete may already have removed
/// an entry that is still visible to the reader.
fn snapshot_allows_btree(engine: &Engine, snap: &Snapshot) -> bool {
    snap.xmax() == engine.txn_manager().current_xid()
        && snap.xip().iter().all(|xid| *xid == snap.current_xid())
}

fn execute_seq(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
) -> Result<Vec<(Tid, Vec<Value>)>> {
    let predicate = filter
        .as_ref()
        .map(|filter| filter_to_predicate(entry, filter))
        .transpose()?;
    engine.scan_inner(snap, table, predicate.as_ref())
}

/// Produce EXPLAIN output without executing the inner B+Tree/ANN plan.
/// Table row/page estimates are collected through the caller's snapshot; the
/// snapshot guard therefore has the same owner and lifetime as a real scan.
#[allow(clippy::too_many_arguments)] // mirrors the parsed Select AST fields
pub(crate) fn explain(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    columns: &crate::sql::SelectCols,
    filter: &Option<Filter>,
    order_by: &Option<OrderByKind>,
    limit: &Option<usize>,
) -> Result<crate::QueryResult> {
    // EXPLAIN shares normal SELECT's Stage A shape gate and schema checks.
    Engine::validate_select_shape(entry, columns, order_by)?;
    let predicate = filter
        .as_ref()
        .map(|filter| filter_to_predicate(entry, filter))
        .transpose()?;
    let request = build_request(engine, table, entry, filter, order_by, limit);
    let mut plan = plan_for_predicate(&request)?;
    let key_too_large = predicate.as_ref().is_some_and(|predicate| {
        matches!(
            encode_key(predicate.value()),
            Err(pg_am_btree::BTreeError::KeyTooLarge(_))
        )
    });
    if matches!(&plan.spec, ScanSpec::BTree(_)) && !snapshot_allows_btree(engine, snap) {
        plan.spec = ScanSpec::Seq(crate::planner::SeqScanSpec {
            predicate: filter.clone(),
            order_by: order_by.clone(),
            limit: *limit,
            est_rows: 0,
            est_pages: 0,
            reason: crate::planner::SeqScanReason::SnapshotFallback,
        });
    } else if key_too_large && matches!(&plan.spec, ScanSpec::BTree(_)) {
        plan.spec = ScanSpec::Seq(crate::planner::SeqScanSpec {
            predicate: filter.clone(),
            order_by: order_by.clone(),
            limit: *limit,
            est_rows: 0,
            est_pages: 0,
            reason: crate::planner::SeqScanReason::KeyTooLarge,
        });
    }

    let col_types = column_types(entry);
    let rel = pg_am_heap::access_method::RelationDesc {
        rel_oid: entry.oid,
        first_page: entry.first_page,
        columns: &col_types,
    };
    let est_rows = engine.scan_inner(snap, table, predicate.as_ref())?.len() as u64;
    let est_pages = engine.heap().relation_pages(&rel)?.len() as u64;
    match &mut plan.spec {
        ScanSpec::BTree(spec) => {
            spec.est_rows = est_rows;
            spec.est_pages = est_pages;
        }
        ScanSpec::Seq(spec) => {
            spec.est_rows = est_rows;
            spec.est_pages = est_pages;
        }
        ScanSpec::Hnsw(_) => {}
    }
    Ok(crate::QueryResult::Rows {
        columns: vec!["plan".to_string()],
        rows: vec![vec![Some(crate::Datum::Text(plan.explain()))]],
    })
}

/// Execute a B+Tree candidate scan, then perform the authoritative heap
/// visibility check and decode at the visible (possibly HOT-descendant) TID.
fn execute_btree(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    filter: &Filter,
) -> Result<Option<ScanRows>> {
    let predicate = filter_to_predicate(entry, filter)?;
    let key = match encode_key(predicate.value()) {
        Ok(key) => key,
        Err(pg_am_btree::BTreeError::KeyTooLarge(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let index = engine.btree_index(table, &filter.column)?;
    let candidates = match filter.op {
        CmpOp::Eq => index.lookup_all(&key)?,
        CmpOp::Lt => index
            .range_scan(None, Some(key.as_slice()))?
            .into_iter()
            .map(|(_, tid)| tid)
            .collect(),
        // range_scan's start is INCLUSIVE. Reapplying the predicate below
        // removes the equal-key entries, which is type-independent and also
        // protects against stale or unrelated candidates.
        CmpOp::Gt => index
            .range_scan(Some(key.as_slice()), None)?
            .into_iter()
            .map(|(_, tid)| tid)
            .collect(),
    };
    let col_types = column_types(entry);
    let mut rows = Vec::new();
    for candidate in candidates {
        let Some(visible_tid) = engine.heap_tuple_visible(snap, candidate)? else {
            continue;
        };
        let values = engine.read_row_by_tid(entry, &col_types, visible_tid)?;
        // Visibility does not prove key identity. Recheck the decoded row,
        // which also enforces strict `>` after the inclusive range start.
        if predicate.matches(&values[predicate.col_index()]) {
            rows.push((visible_tid, values));
        }
    }
    Ok(Some(rows))
}

/// Recheck only a completed B+Tree candidate scan. The KeyTooLarge fallback
/// never reaches this function, so it cannot be overwritten by a second reason.
fn recheck_btree(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
    rows: ScanRows,
    fallback_reason: &mut Option<crate::planner::SeqScanReason>,
) -> Result<ScanRows> {
    #[cfg(test)]
    POST_BTREE_SCAN_HOOK.with(|hook| {
        if let Some(hook) = hook.get() {
            hook(engine);
        }
    });
    if engine.txn_manager().current_xid() == snap.xmax() {
        return Ok(rows);
    }
    // A writer started after the snapshot and may have removed the very index
    // entries we need. Discard the partial result and re-read through the heap,
    // which is MVCC-complete for this snapshot. The plan is changed to match.
    *fallback_reason = Some(crate::planner::SeqScanReason::SnapshotFallback);
    execute_seq(engine, snap, table, entry, filter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::engine::EngineConfig;
    use crate::planner::SeqScanReason;
    use crate::query_stats::ExecutionPath;
    use crate::sql::Literal;
    use crate::{Datum, QueryResult};
    use tempfile::TempDir;

    fn setup(with_index: bool) -> (TempDir, Engine) {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
        engine
            .exec(None, "CREATE TABLE t (id INT8, v TEXT)")
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();
        if with_index {
            engine.create_index("t", "id").unwrap();
        }
        (tmp, engine)
    }

    fn id_eq(value: i64) -> Option<Filter> {
        Some(Filter {
            column: "id".to_string(),
            op: CmpOp::Eq,
            value: Literal::Int(value),
        })
    }

    fn execute_with(engine: &Engine, filter: Option<Filter>) -> ScanOutput {
        let entry = engine.describe_table("t").unwrap();
        let (snap, _guard) = engine
            .txn_manager()
            .snapshot(pg_storage::types::TxnId::INVALID);
        execute(engine, &snap, "t", &entry, &filter, &None, &None).unwrap()
    }

    #[test]
    fn equality_uses_btree_when_indexed_and_seqscan_otherwise() {
        let (_tmp, indexed) = setup(true);
        let output = execute_with(&indexed, id_eq(2));
        assert!(matches!(output.plan.spec, ScanSpec::BTree(_)));
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].1[0], Some(Datum::Int8(2)));

        let (_tmp, plain) = setup(false);
        let output = execute_with(&plain, id_eq(2));
        match output.plan.spec {
            ScanSpec::Seq(spec) => assert_eq!(spec.reason, SeqScanReason::NoIndex),
            _ => panic!("expected SeqScan without an index"),
        }
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].1[0], Some(Datum::Int8(2)));
    }

    #[test]
    fn auto_commit_and_explicit_transaction_read_consistently() {
        let (_tmp, engine) = setup(true);
        let result = engine.exec(None, "SELECT * FROM t WHERE id = 2").unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));

        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "INSERT INTO t VALUES (4, 'd')")
            .unwrap();
        let result = engine
            .exec(Some(&txn), "SELECT * FROM t WHERE id = 4")
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));
        txn.abort().unwrap();

        let result = engine.exec(None, "SELECT * FROM t WHERE id = 4").unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.is_empty()));
    }

    #[test]
    fn aborted_insert_is_invisible() {
        let (_tmp, engine) = setup(true);
        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "INSERT INTO t VALUES (9, 'aborted')")
            .unwrap();
        txn.abort().unwrap();
        let result = engine.exec(None, "SELECT * FROM t WHERE id = 9").unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.is_empty()));
    }

    fn execute_snapshot(engine: &Engine, snap: &Snapshot, filter: Option<Filter>) -> ScanOutput {
        let entry = engine.describe_table("t").unwrap();
        execute(engine, snap, "t", &entry, &filter, &None, &None).unwrap()
    }

    #[test]
    fn old_snapshot_survives_delete_and_vacuum() {
        let (_tmp, engine) = setup(true);
        let reader = engine.begin_txn().unwrap();
        engine.exec(None, "DELETE FROM t WHERE id = 1").unwrap();
        engine.vacuum("t").unwrap();

        let output = execute_snapshot(&engine, &reader.snapshot(), id_eq(1));
        match output.plan.spec {
            ScanSpec::Seq(spec) => assert_eq!(spec.reason, SeqScanReason::SnapshotFallback),
            _ => panic!("old snapshot must use the heap fallback"),
        }
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].1[0], Some(Datum::Int8(1)));

        reader.abort().unwrap();
        // The second pass must actually find and reclaim the old version;
        // otherwise the zero-dead third pass would be vacuous evidence.
        let reclaimed = engine.vacuum("t").unwrap();
        assert!(reclaimed.dead_tuples > 0);
        // A third pass must find no dead version left, proving the guarded
        // old version was physically reclaimed only after Guard drop.
        let stats = engine.vacuum("t").unwrap();
        assert_eq!(stats.dead_tuples, 0);
        let result = engine.exec(None, "SELECT * FROM t WHERE id = 1").unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.is_empty()));
    }

    #[test]
    fn old_snapshot_sees_old_key_after_non_hot_update() {
        let (_tmp, engine) = setup(true);
        let reader = engine.begin_txn().unwrap();
        engine
            .exec(None, "UPDATE t SET id = 2 WHERE id = 1")
            .unwrap();

        let output = execute_snapshot(&engine, &reader.snapshot(), id_eq(1));
        match output.plan.spec {
            ScanSpec::Seq(spec) => assert_eq!(spec.reason, SeqScanReason::SnapshotFallback),
            _ => panic!("old snapshot must use the heap fallback"),
        }
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].1[0], Some(Datum::Int8(1)));
        reader.abort().unwrap();

        let result = engine.exec(None, "SELECT * FROM t WHERE id = 2").unwrap();
        assert_eq!(result_ids(result), vec![2, 2]);
    }

    #[test]
    fn in_flight_delete_also_uses_heap_fallback() {
        let (_tmp, engine) = setup(true);
        let reader = engine.begin_txn().unwrap();
        let writer = engine.begin_txn().unwrap();
        engine
            .exec(Some(&writer), "DELETE FROM t WHERE id = 2")
            .unwrap();

        let output = execute_snapshot(&engine, &reader.snapshot(), id_eq(2));
        match output.plan.spec {
            ScanSpec::Seq(spec) => assert_eq!(spec.reason, SeqScanReason::SnapshotFallback),
            _ => panic!("in-flight delete must use the heap fallback"),
        }
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].1[0], Some(Datum::Int8(2)));
        writer.abort().unwrap();
        reader.abort().unwrap();
    }

    fn result_ids(result: QueryResult) -> Vec<i64> {
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected rows")
        };
        rows.into_iter()
            .map(|row| match &row[0] {
                Some(Datum::Int8(id)) => *id,
                other => panic!("expected INT8 id, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn equality_returns_every_visible_duplicate() {
        let (_tmp, engine) = setup(true);
        engine
            .exec(None, "INSERT INTO t VALUES (2, 'dup-1'), (2, 'dup-2')")
            .unwrap();
        let result = engine.exec(None, "SELECT * FROM t WHERE id = 2").unwrap();
        assert_eq!(result_ids(result).len(), 3);
    }

    #[test]
    fn range_scans_enforce_strict_bounds() {
        let (_tmp, engine) = setup(true);
        let output = execute_with(
            &engine,
            Some(Filter {
                column: "id".to_string(),
                op: CmpOp::Gt,
                value: Literal::Int(2),
            }),
        );
        assert!(matches!(output.plan.spec, ScanSpec::BTree(_)));
        assert_eq!(
            output
                .rows
                .into_iter()
                .map(|(_, values)| values[0].clone())
                .collect::<Vec<_>>(),
            vec![Some(Datum::Int8(3))]
        );

        let output = execute_with(
            &engine,
            Some(Filter {
                column: "id".to_string(),
                op: CmpOp::Lt,
                value: Literal::Int(2),
            }),
        );
        assert_eq!(
            output
                .rows
                .into_iter()
                .map(|(_, values)| values[0].clone())
                .collect::<Vec<_>>(),
            vec![Some(Datum::Int8(1))]
        );
    }

    fn setup_text_index() -> (TempDir, Engine) {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
        engine.exec(None, "CREATE TABLE t (v TEXT)").unwrap();
        engine.exec(None, "INSERT INTO t VALUES ('a')").unwrap();
        engine.create_index("t", "v").unwrap();
        (tmp, engine)
    }

    #[test]
    fn oversized_literals_fall_back_to_seqscan() {
        let (_tmp, engine) = setup_text_index();
        let oversized = "z".repeat(pg_am_btree::MAX_INDEX_KEY_BYTES + 1);
        for (op, expected) in [("=", 0), ("<", 1), (">", 0)] {
            let sql = format!("SELECT * FROM t WHERE v {op} '{oversized}'");
            let result = engine.exec(None, &sql).unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected rows")
            };
            assert_eq!(rows.len(), expected, "unexpected result for {op}");
            assert_eq!(
                engine.query_stats().entries().last().unwrap().path,
                ExecutionPath::SeqScan
            );
        }
    }

    #[test]
    fn oversized_explain_also_uses_seqscan() {
        let (_tmp, engine) = setup_text_index();
        let oversized = "z".repeat(pg_am_btree::MAX_INDEX_KEY_BYTES + 1);
        let text = plan_text(
            engine
                .exec(
                    None,
                    &format!("EXPLAIN SELECT * FROM t WHERE v < '{oversized}'"),
                )
                .unwrap(),
        );
        assert!(text.contains("SeqScan"), "{text}");
        assert!(text.contains("KeyTooLarge"), "{text}");
        assert!(!text.contains("BTreeScan"), "{text}");
        assert!(text.contains("est_rows=1"), "{text}");

        let text = plan_text(
            engine
                .exec(
                    None,
                    &format!("EXPLAIN SELECT * FROM t WHERE v = '{oversized}'"),
                )
                .unwrap(),
        );
        assert!(text.contains("SeqScan"), "{text}");
        assert!(text.contains("KeyTooLarge"), "{text}");
        assert!(text.contains("est_rows=0"), "{text}");
    }

    #[test]
    fn oversized_explain_respects_no_index_and_snapshot_precedence() {
        let tmp = TempDir::new().unwrap();
        let plain = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
        plain.exec(None, "CREATE TABLE t (v TEXT)").unwrap();
        plain.exec(None, "INSERT INTO t VALUES ('a')").unwrap();
        let oversized = "z".repeat(pg_am_btree::MAX_INDEX_KEY_BYTES + 1);
        let text = plan_text(
            plain
                .exec(
                    None,
                    &format!("EXPLAIN SELECT * FROM t WHERE v < '{oversized}'"),
                )
                .unwrap(),
        );
        assert!(text.contains("SeqScan"), "{text}");
        assert!(text.contains("NoIndex"), "{text}");
        assert!(!text.contains("KeyTooLarge"), "{text}");

        let (_tmp, indexed) = setup_text_index();
        let reader = indexed.begin_txn().unwrap();
        indexed.exec(None, "INSERT INTO t VALUES ('b')").unwrap();
        let text = plan_text(
            indexed
                .exec(
                    Some(&reader),
                    &format!("EXPLAIN SELECT * FROM t WHERE v < '{oversized}'"),
                )
                .unwrap(),
        );
        assert!(text.contains("SeqScan"), "{text}");
        assert!(text.contains("SnapshotFallback"), "{text}");
        assert!(!text.contains("KeyTooLarge"), "{text}");
        reader.abort().unwrap();
    }

    #[test]
    fn order_and_limit_apply_to_filtered_rows() {
        let (_tmp, engine) = setup(true);
        engine
            .exec(None, "INSERT INTO t VALUES (2, 'dup')")
            .unwrap();
        let result = engine
            .exec(
                None,
                "SELECT * FROM t WHERE id > 1 ORDER BY id DESC LIMIT 2",
            )
            .unwrap();
        assert_eq!(result_ids(result), vec![3, 2]);

        let result = engine
            .exec(None, "SELECT * FROM t WHERE id < 3 ORDER BY id ASC")
            .unwrap();
        assert_eq!(result_ids(result), vec![1, 2, 2]);

        let (_tmp, plain) = setup(false);
        plain.exec(None, "INSERT INTO t VALUES (2, 'dup')").unwrap();
        let result = plain
            .exec(None, "SELECT * FROM t WHERE id < 3 ORDER BY id ASC")
            .unwrap();
        assert_eq!(result_ids(result), vec![1, 2, 2]);
    }

    fn plan_text(result: QueryResult) -> String {
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected EXPLAIN rows")
        };
        assert_eq!(rows.len(), 1);
        match &rows[0][0] {
            Some(Datum::Text(text)) => text.clone(),
            other => panic!("expected EXPLAIN text, got {other:?}"),
        }
    }

    #[test]
    fn explain_reports_btree_plan_and_table_stats() {
        let (_tmp, engine) = setup(true);
        let text = plan_text(
            engine
                .exec(None, "EXPLAIN SELECT * FROM t WHERE id = 2")
                .unwrap(),
        );
        assert!(text.contains("BTreeScan"), "{text}");
        assert!(text.contains("est_rows=1"), "{text}");
        assert!(text.contains("est_pages="), "{text}");
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::Explain
        );

        let result = engine.exec(None, "SELECT * FROM t WHERE id = 2").unwrap();
        assert!(matches!(result, QueryResult::Rows { .. }));
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
    }

    #[test]
    fn explain_reports_seqscan_plan() {
        let (_tmp, engine) = setup(false);
        let text = plan_text(
            engine
                .exec(None, "EXPLAIN SELECT * FROM t WHERE id = 2")
                .unwrap(),
        );
        assert!(text.contains("SeqScan"), "{text}");
        assert!(text.contains("NoIndex"), "{text}");
        assert!(text.contains("est_rows=1"), "{text}");
    }

    #[test]
    fn explain_in_explicit_txn_does_not_stamp_row_locks() {
        let (_tmp, engine) = setup(true);
        let txn = engine.begin_txn().unwrap();
        let text = plan_text(
            engine
                .exec(
                    Some(&txn),
                    "EXPLAIN SELECT * FROM t WHERE id = 1 FOR UPDATE",
                )
                .unwrap(),
        );
        assert!(text.contains("BTreeScan"), "{text}");

        engine.lock_manager().release_all(txn.xid());
        engine
            .exec(None, "UPDATE t SET id = 2 WHERE id = 1")
            .unwrap();
        txn.abort().unwrap();
    }

    #[test]
    fn explain_rejects_invalid_projection_order_and_ann_shape() {
        let (_tmp, engine) = setup(true);
        for sql in [
            "EXPLAIN SELECT missing FROM t",
            "EXPLAIN SELECT * FROM t ORDER BY missing",
            "EXPLAIN SELECT * FROM t ORDER BY v <-> '[1]' LIMIT 1",
        ] {
            assert!(engine.exec(None, sql).is_err(), "must reject {sql}");
        }
    }

    #[test]
    fn mixed_case_indexed_column_executes_btree() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
        engine
            .create_table(
                "t",
                &[
                    crate::engine::ColumnDef {
                        name: "ID".to_string(),
                        col_type: ColumnType::Int8,
                    },
                    crate::engine::ColumnDef {
                        name: "v".to_string(),
                        col_type: ColumnType::Text,
                    },
                ],
            )
            .unwrap();
        engine.exec(None, "INSERT INTO t VALUES (1, 'a')").unwrap();
        engine.create_index("t", "ID").unwrap();

        let result = engine.exec(None, "SELECT * FROM t WHERE id = 1").unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
    }

    #[test]
    fn writer_already_in_xip_forces_heap_fallback() {
        let (_tmp, engine) = setup(true);
        let writer = engine.begin_txn().unwrap();
        let reader = engine.begin_txn().unwrap();
        let output = execute_snapshot(&engine, &reader.snapshot(), id_eq(1));
        match output.plan.spec {
            ScanSpec::Seq(spec) => assert_eq!(spec.reason, SeqScanReason::SnapshotFallback),
            _ => panic!("writer in xip must force the heap fallback"),
        }
        assert_eq!(output.rows.len(), 1);
        writer.abort().unwrap();
        reader.abort().unwrap();
    }

    fn delete_id_one(engine: &Engine) {
        engine
            .exec(None, "DELETE FROM hook_t WHERE id = 1")
            .unwrap();
    }

    #[test]
    fn post_scan_xid_recheck_falls_back_after_writer() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
        engine
            .exec(None, "CREATE TABLE hook_t (id INT8, v TEXT)")
            .unwrap();
        engine
            .exec(None, "INSERT INTO hook_t VALUES (1, 'a')")
            .unwrap();
        engine.create_index("hook_t", "id").unwrap();
        let entry = engine.describe_table("hook_t").unwrap();
        let (snap, _guard) = engine
            .txn_manager()
            .snapshot(pg_storage::types::TxnId::INVALID);
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                POST_BTREE_SCAN_HOOK.with(|hook| hook.set(None));
            }
        }
        let _reset = ResetHook;
        POST_BTREE_SCAN_HOOK.with(|hook| hook.set(Some(delete_id_one)));
        let output = execute(&engine, &snap, "hook_t", &entry, &id_eq(1), &None, &None).unwrap();
        match output.plan.spec {
            ScanSpec::Seq(spec) => assert_eq!(spec.reason, SeqScanReason::SnapshotFallback),
            _ => panic!("post-scan XID change must use the heap fallback"),
        }
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].1[0], Some(Datum::Int8(1)));
    }

    #[test]
    fn key_too_large_does_not_recheck_or_overwrite_reason() {
        let (_tmp, engine) = setup_text_index();
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                POST_BTREE_SCAN_HOOK.with(|hook| hook.set(None));
            }
        }
        let _reset = ResetHook;
        POST_BTREE_SCAN_HOOK.with(|hook| {
            hook.set(Some(|_| {
                panic!("post-fallback recheck must not run");
            }))
        });
        let oversized = "z".repeat(pg_am_btree::MAX_INDEX_KEY_BYTES + 1);
        let output = execute_with(
            &engine,
            Some(Filter {
                column: "v".to_string(),
                op: CmpOp::Lt,
                value: Literal::Str(oversized),
            }),
        );
        match output.plan.spec {
            ScanSpec::Seq(spec) => assert_eq!(spec.reason, SeqScanReason::KeyTooLarge),
            _ => panic!("oversized query must keep KeyTooLarge"),
        }
        assert_eq!(output.rows.len(), 1);
    }

    #[test]
    fn indexed_sql_follows_hot_chain_to_visible_descendant() {
        let (_tmp, engine) = setup(true);
        for i in 0..10 {
            engine
                .exec(None, &format!("UPDATE t SET v = 'v{i}' WHERE id = 1"))
                .unwrap();
        }
        let result = engine.exec(None, "SELECT * FROM t WHERE id = 1").unwrap();
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected rows")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1].clone(), Some(Datum::Text("v9".to_string())));
    }

    #[test]
    fn indexed_order_limit_for_update_and_for_share() {
        let (_tmp, engine) = setup(true);
        let engine = Arc::new(engine);
        for clause in ["FOR UPDATE", "FOR SHARE"] {
            let txn = engine.begin_txn().unwrap();
            let result = engine
                .exec(
                    Some(&txn),
                    &format!("SELECT * FROM t WHERE id > 0 ORDER BY id DESC LIMIT 1 {clause}"),
                )
                .unwrap();
            assert_eq!(result_ids(result), vec![3]);

            // The returned row (id=3) must be locked, while a row discarded
            // by LIMIT (id=1) must remain writable.
            let (tx, rx) = mpsc::channel();
            let engine2 = Arc::clone(&engine);
            let writer = thread::spawn(move || {
                let result = engine2.exec(None, "UPDATE t SET v = 'blocked' WHERE id = 3");
                tx.send(result).unwrap();
            });
            match rx.recv_timeout(Duration::from_millis(200)) {
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                other => panic!("id=3 must be locked after {clause}: {other:?}"),
            }
            engine
                .exec(None, "UPDATE t SET v = 'free' WHERE id = 1")
                .unwrap();

            txn.abort().unwrap();
            let result = rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(
                result.is_ok(),
                "writer must proceed after abort: {result:?}"
            );
            writer.join().unwrap();
        }
    }

    #[test]
    fn btree_point_lookup_mean_latency_is_below_5ms() {
        let (_tmp, engine) = setup(true);
        // Warm the index and heap pages so this measures the steady-state
        // executor path rather than first-open bootstrap cost.
        engine.exec(None, "SELECT * FROM t WHERE id = 2").unwrap();
        let started = Instant::now();
        for _ in 0..20 {
            let result = engine.exec(None, "SELECT * FROM t WHERE id = 2").unwrap();
            assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));
        }
        let mean = started.elapsed() / 20;
        eprintln!("Stage A B+Tree point lookup mean latency: {mean:?}");
        assert!(mean < Duration::from_millis(5), "mean latency {mean:?}");
    }
}
