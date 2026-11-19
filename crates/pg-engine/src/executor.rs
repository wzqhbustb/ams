//! Phase 4a Stages A/B: the self-owned transactional-processing executor.
//!
//! The caller owns the snapshot and its SnapshotGuard. This module only
//! chooses and executes a ScanPlan; it never creates a fresh snapshot and
//! never commits or aborts a transaction.

use pg_am_btree::encode_key;
use pg_am_heap::tuple::{ColumnType, Datum};
use pg_storage::types::Tid;
use pg_txn::Snapshot;

use crate::engine::{
    column_types, filter_to_predicate, filter_to_predicate_with_analyzer, Engine, TableEntry, Value,
};
use crate::error::{EngineError, Result};
use crate::planner::{
    plan_for_predicate, AmKind, HnswScanSpec, IndexMetadata, InvertedScanSpec, ScanPlan,
    ScanRequest, ScanSpec,
};
use crate::sql::{CmpOp, DistanceOp, Expr, Filter, Literal, OrderByExpr, OrderByKind};

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
#[allow(clippy::too_many_arguments)] // mirrors the parsed Select AST fields
pub(crate) fn execute(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
    extra_filter: &Option<Filter>,
    order_by: &Option<OrderByKind>,
    limit: &Option<usize>,
) -> Result<ScanOutput> {
    if let Some(OrderByKind::Expr(expr)) = order_by {
        // Reject malformed vectors and non-VECTOR columns before any heap
        // scan, regardless of whether the planner selects ANN or a fallback.
        prepare_distance(entry, expr)?;
    }
    let request =
        build_request_with_extra(engine, table, entry, filter, extra_filter, order_by, limit)?;
    let mut plan = plan_for_predicate(&request)?;
    let mut fallback_reason = None;
    let rows = match &mut plan.spec {
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
                    execute_seq_with_extra(engine, snap, table, entry, filter, extra_filter)?
                }
            }
        }
        ScanSpec::BTree(_) => {
            fallback_reason = Some(crate::planner::SeqScanReason::SnapshotFallback);
            execute_seq_with_extra(engine, snap, table, entry, filter, extra_filter)?
        }
        ScanSpec::Seq(spec) => execute_seq_with_extra(
            engine,
            snap,
            table,
            entry,
            &spec.predicate,
            &spec.extra_predicate,
        )?,
        ScanSpec::Hnsw(spec) => execute_hnsw(engine, snap, table, entry, spec)?,
        ScanSpec::Inverted(spec) if !engine.inverted_snapshot_allows(spec.index_oid, snap) => {
            fallback_reason = Some(crate::planner::SeqScanReason::SnapshotFallback);
            execute_seq_with_extra(engine, snap, table, entry, filter, extra_filter)?
        }
        ScanSpec::Inverted(spec) => {
            let (rows, candidate_count) = execute_inverted(engine, snap, entry, spec)?;
            spec.candidate_count = candidate_count;
            rows
        }
    };
    if let Some(reason) = fallback_reason {
        plan.spec = ScanSpec::Seq(crate::planner::SeqScanSpec {
            predicate: filter.clone(),
            extra_predicate: extra_filter.clone(),
            order_by: order_by.clone(),
            limit: *limit,
            est_rows: 0,
            est_pages: 0,
            reason,
        });
    }
    Ok(ScanOutput { rows, plan })
}

fn execute_inverted(
    engine: &Engine,
    snap: &Snapshot,
    entry: &TableEntry,
    spec: &InvertedScanSpec,
) -> Result<(ScanRows, u64)> {
    let Literal::Str(query) = &spec.query else {
        return Err(EngineError::InvalidArgument(
            "full-text query must be a bound text literal".to_string(),
        ));
    };
    let filter = Filter {
        column: spec.column.clone(),
        op: CmpOp::FullText,
        value: spec.query.clone(),
    };
    let predicate = filter_to_predicate_with_analyzer(entry, &filter, &spec.analyzer_config)?;
    let handle = engine.inverted_handle(spec.index_oid)?;
    let hits = handle
        .lock()
        .search_text(query, None)
        .map_err(|error| EngineError::Corrupted(error.to_string()))?;
    let candidate_count = hits.len() as u64;
    let col_types = column_types(entry);
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for hit in hits {
        let Some(visible_tid) = engine.heap_tuple_visible(snap, hit.tid)? else {
            continue;
        };
        if !seen.insert(visible_tid) {
            continue;
        }
        let values = engine.read_row_by_tid(snap, entry, &col_types, visible_tid)?;
        // Visibility alone is insufficient for stale candidates and recycled
        // slots: the visible document must still satisfy the analyzed query.
        if predicate.matches(&values[predicate.col_index()]) {
            rows.push((visible_tid, values));
        }
    }
    Ok((rows, candidate_count))
}

/// Validate a BM25 expression before any index or heap work.
pub(crate) fn validate_bm25(entry: &TableEntry, expr: &crate::sql::Bm25ScoreExpr) -> Result<()> {
    bm25_query(entry, expr).map(|_| ())
}

fn bm25_query(entry: &TableEntry, expr: &crate::sql::Bm25ScoreExpr) -> Result<String> {
    let column = entry
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&expr.column))
        .ok_or_else(|| {
            EngineError::InvalidPredicate(format!("no column {:?} in table", expr.column))
        })?;
    if column.col_type != ColumnType::Text {
        return Err(EngineError::InvalidArgument(format!(
            "bm25_score left operand {:?} is not a TEXT column",
            expr.column
        )));
    }
    match &expr.query {
        Literal::Str(query) => Ok(query.clone()),
        Literal::Param(index) => Err(EngineError::InvalidArgument(format!(
            "unbound parameter ${index}; use Engine::exec_with_params"
        ))),
        _ => Err(EngineError::InvalidArgument(
            "bm25_score query must be text or a typed text parameter".to_string(),
        )),
    }
}

/// Compute exact scores keyed by visible heap TID. A sequential fallback
/// builds its statistics from the visible heap, never from inverted candidates.
pub(crate) fn bm25_scores(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    expr: &crate::sql::Bm25ScoreExpr,
    use_inverted: bool,
) -> Result<std::collections::HashMap<Tid, f64>> {
    let query = bm25_query(entry, expr)?;
    let column = entry
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(&expr.column))
        .expect("validated BM25 column");
    let mut scores = std::collections::HashMap::new();
    if use_inverted {
        let (index_entry, _) = engine
            .indexes_of(entry)
            .into_iter()
            .find(|(index, _)| {
                index.kind == AmKind::Inverted && index.column.eq_ignore_ascii_case(&expr.column)
            })
            .ok_or_else(|| {
                EngineError::IndexNotFound(format!("inverted index on {:?}", expr.column))
            })?;
        let hits = engine
            .inverted_handle(index_entry.index_oid)?
            .lock()
            .search_text(&query, None)
            .map_err(|error| EngineError::Corrupted(error.to_string()))?;
        for hit in hits {
            if let Some(visible_tid) = engine.heap_tuple_visible(snap, hit.tid)? {
                scores.insert(visible_tid, hit.score);
            }
        }
        return Ok(scores);
    }

    let analyzer = engine.inverted_analyzer_for(entry.oid, &expr.column);
    let mut index =
        pg_am_inverted::InMemoryIndex::new(pg_am_inverted::DefaultAnalyzer::from_config(analyzer));
    let rows = engine.scan_inner(snap, table, None)?;
    let mut visible_tids = std::collections::HashMap::new();
    for (visible_tid, values) in rows {
        let index_tid = engine.hot_chain_root(visible_tid)?;
        visible_tids.insert(index_tid, visible_tid);
        if let Some(Datum::Text(text)) = &values[column] {
            index
                .insert_document(index_tid, text)
                .map_err(|error| EngineError::Corrupted(error.to_string()))?;
        }
    }
    let query = pg_am_inverted::parse_query(index.analyzer(), &query);
    for hit in index
        .search(&query, None)
        .map_err(|error| EngineError::Corrupted(error.to_string()))?
    {
        if let Some(visible_tid) = visible_tids.get(&hit.tid) {
            scores.insert(*visible_tid, hit.score);
        }
    }
    Ok(scores)
}

fn execute_hnsw(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    spec: &HnswScanSpec,
) -> Result<ScanRows> {
    let op = match spec.opclass {
        crate::sql::VectorOpclass::VectorL2Ops => DistanceOp::L2,
        crate::sql::VectorOpclass::VectorCosineOps => DistanceOp::Cosine,
        crate::sql::VectorOpclass::VectorIpOps => DistanceOp::InnerProduct,
    };
    let evaluator = prepare_distance_parts(entry, &spec.column, op, &spec.query)?;
    let hits = engine.hnsw_search_visible_with_policy(
        table,
        &evaluator.query,
        spec.k,
        Some(spec.ef),
        spec.candidates,
        spec.max_retry_rounds,
        snap,
    )?;
    let col_types = column_types(entry);
    let mut rows = hits
        .into_iter()
        .map(|(tid, _)| {
            // hnsw_search_visible already judged this exact TID against
            // `snap`; the caller's guard keeps that version reclaim-safe.
            let values = engine.read_row_by_tid(snap, entry, &col_types, tid)?;
            Ok((tid, values))
        })
        .collect::<Result<ScanRows>>()?;
    if rows.len() < spec.k {
        // NULL vectors have no graph node or sidecar entry. SQL ASC is
        // NULLS LAST, so they belong after the ANN hits and must still fill
        // LIMIT when fewer than k non-NULL rows are visible. This heap read
        // uses the same snapshot/guard and cannot duplicate indexed rows.
        let missing = spec.k - rows.len();
        let null_rows = execute_seq(engine, snap, table, entry, &None)?
            .into_iter()
            .filter(|(_, values)| matches!(values.get(evaluator.column), Some(None)))
            .take(missing)
            .collect::<ScanRows>();
        rows.extend(null_rows);
    }
    Ok(rows)
}

/// A validated vector-column/query pair plus its SQL distance operator.
pub(crate) struct DistanceEvaluator {
    column: usize,
    op: DistanceOp,
    query: Vec<f32>,
}

pub(crate) fn prepare_distance(
    entry: &TableEntry,
    expr: &OrderByExpr,
) -> Result<DistanceEvaluator> {
    let Expr::Column(column) = &expr.left else {
        return Err(EngineError::InvalidArgument(
            "distance left operand must be a column reference".to_string(),
        ));
    };
    let Expr::Literal(literal) = &expr.right else {
        return Err(EngineError::InvalidArgument(
            "distance right operand must be a vector literal".to_string(),
        ));
    };
    prepare_distance_parts(entry, column, expr.op, literal)
}

fn prepare_distance_parts(
    entry: &TableEntry,
    column: &str,
    op: DistanceOp,
    literal: &Literal,
) -> Result<DistanceEvaluator> {
    let (column, dim) = vector_column(entry, column)?;
    let query = decode_vector_literal(literal, dim)?;
    if op == DistanceOp::Cosine {
        // Match the HNSW entry-point contract even for an empty-table or
        // SeqScan query, and reject before any FOR UPDATE row locks.
        pg_am_hnsw::distance::cosine(&query, &query)?;
    }
    Ok(DistanceEvaluator { column, op, query })
}

impl DistanceEvaluator {
    /// Evaluate one row. SQL NULL distance stays NULL; ORDER BY places it
    /// last for ASC and first for DESC, matching PostgreSQL's defaults.
    pub(crate) fn evaluate(&self, values: &[Value]) -> Result<Option<f64>> {
        let Some(value) = values.get(self.column) else {
            return Err(EngineError::Corrupted(format!(
                "distance column index {} is outside a row of width {}",
                self.column,
                values.len()
            )));
        };
        let Some(datum) = value else {
            return Ok(None);
        };
        let Datum::Vector(vector) = datum else {
            return Err(EngineError::Corrupted(format!(
                "VECTOR column {} decoded as {datum:?}",
                self.column
            )));
        };
        let distance = match self.op {
            DistanceOp::L2 => pg_am_hnsw::distance::l2_squared(vector, &self.query)?.sqrt(),
            DistanceOp::Cosine => pg_am_hnsw::distance::cosine(vector, &self.query)?,
            DistanceOp::InnerProduct => {
                pg_am_hnsw::distance::negative_inner_product(vector, &self.query)?
            }
        };
        Ok(Some(distance))
    }
}

fn vector_column(entry: &TableEntry, column: &str) -> Result<(usize, u32)> {
    let index = entry
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(column))
        .ok_or_else(|| EngineError::InvalidPredicate(format!("no column {column:?} in table")))?;
    let ColumnType::Vector(dim) = entry.columns[index].col_type else {
        return Err(EngineError::InvalidArgument(format!(
            "distance left operand {column:?} is not a VECTOR column"
        )));
    };
    Ok((index, dim))
}

fn decode_vector_literal(literal: &Literal, dim: u32) -> Result<Vec<f32>> {
    let vector = match literal {
        Literal::Vector(vector) => vector.clone(),
        Literal::Str(text) => parse_vector_text(text)?,
        other => {
            return Err(EngineError::InvalidArgument(format!(
                "distance right operand must be a vector literal, got {other:?}"
            )))
        }
    };
    if vector.len() != dim as usize {
        return Err(EngineError::InvalidArgument(format!(
            "query vector has {} components, column is VECTOR({dim})",
            vector.len()
        )));
    }
    if let Some((index, _)) = vector
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(EngineError::InvalidArgument(format!(
            "query vector component {index} is not finite"
        )));
    }
    Ok(vector)
}

fn parse_vector_text(text: &str) -> Result<Vec<f32>> {
    let text = text.trim();
    let inner = text
        .strip_prefix('[')
        .and_then(|text| text.strip_suffix(']'))
        .ok_or_else(|| {
            EngineError::InvalidArgument(format!(
                "query vector must use pgvector text form '[...]', got {text:?}"
            ))
        })?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|component| {
            let component = component.trim();
            if component.is_empty() {
                return Err(EngineError::InvalidArgument(
                    "query vector contains an empty component".to_string(),
                ));
            }
            component.parse::<f32>().map_err(|_| {
                EngineError::InvalidArgument(format!(
                    "invalid query vector component {component:?}"
                ))
            })
        })
        .collect()
}

/// Apply an exact distance ORDER BY to a sequential-scan result. Distances
/// are evaluated once per row, both for error propagation and to avoid
/// re-decoding the query vector inside the comparator.
pub(crate) fn sort_by_distance(
    entry: &TableEntry,
    rows: ScanRows,
    expr: &OrderByExpr,
) -> Result<ScanRows> {
    let evaluator = prepare_distance(entry, expr)?;
    let mut keyed = Vec::with_capacity(rows.len());
    for (tid, values) in rows {
        let distance = evaluator.evaluate(&values)?;
        keyed.push((tid, values, distance));
    }
    keyed.sort_by(|a, b| {
        let cmp = match (&a.2, &b.2) {
            (Some(a), Some(b)) => a.total_cmp(b),
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
        };
        if expr.desc {
            cmp.reverse()
        } else {
            cmp
        }
    });
    Ok(keyed
        .into_iter()
        .map(|(tid, values, _)| (tid, values))
        .collect())
}

#[cfg(test)]
pub(crate) fn build_request(
    engine: &Engine,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
    order_by: &Option<OrderByKind>,
    limit: &Option<usize>,
) -> Result<ScanRequest> {
    build_request_with_extra(engine, table, entry, filter, &None, order_by, limit)
}

pub(crate) fn build_request_with_extra(
    engine: &Engine,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
    extra_filter: &Option<Filter>,
    order_by: &Option<OrderByKind>,
    limit: &Option<usize>,
) -> Result<ScanRequest> {
    let ann_column = match (filter, limit, order_by) {
        (None, Some(_), Some(OrderByKind::Expr(expr))) if !expr.desc => {
            match (&expr.left, &expr.right) {
                (Expr::Column(column), Expr::Literal(Literal::Vector(_) | Literal::Str(_))) => {
                    Some(column.as_str())
                }
                _ => None,
            }
        }
        _ => None,
    };
    let inverted_column = filter
        .as_ref()
        .filter(|filter| filter.op == CmpOp::FullText)
        .map(|filter| filter.column.as_str())
        .or(match order_by {
            Some(OrderByKind::Bm25Score(expr)) => Some(expr.column.as_str()),
            _ => None,
        });
    let indexes = engine
        .indexes_of(entry)
        .into_iter()
        .map(|(idx, _)| {
            let is_hnsw = matches!(idx.kind, AmKind::Hnsw);
            let (opclass, ef_search_default) = if is_hnsw
                && ann_column.is_some_and(|column| idx.column.eq_ignore_ascii_case(column))
            {
                // Only a usable ANN shape pays the lazy-open cost, and only
                // for the distance column's own index.
                let params = engine.hnsw_handle(&idx)?.params();
                let opclass = match params.metric {
                    pg_am_hnsw::Metric::L2 => crate::sql::VectorOpclass::VectorL2Ops,
                    pg_am_hnsw::Metric::Cosine => crate::sql::VectorOpclass::VectorCosineOps,
                    pg_am_hnsw::Metric::InnerProduct => crate::sql::VectorOpclass::VectorIpOps,
                };
                (Some(opclass), Some(params.ef_search_default as usize))
            } else {
                (None, None)
            };
            let inverted = if idx.kind == AmKind::Inverted
                && inverted_column.is_some_and(|column| idx.column.eq_ignore_ascii_case(column))
                && !engine.allow_stale_inverted()
            {
                // require_fresh catches up before planning. allow_stale uses
                // the current sealed generation and preserves the stale marker.
                Some(engine.refresh_inverted(&idx, entry)?)
            } else {
                idx.inverted
            };
            Ok(IndexMetadata {
                kind: idx.kind,
                index_oid: idx.index_oid,
                inverted,
                column: idx.column,
                opclass,
                ef_search_default,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ScanRequest {
        table: table.to_string(),
        predicate: filter.clone(),
        extra_predicate: extra_filter.clone(),
        order_by: order_by.clone(),
        limit: *limit,
        allow_stale_inverted: engine.allow_stale_inverted(),
        snapshot_frontier: engine.storage().wal_writer().synced_lsn(),
        indexes,
    })
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
    execute_seq_with_extra(engine, snap, table, entry, filter, &None)
}

fn execute_seq_with_extra(
    engine: &Engine,
    snap: &Snapshot,
    table: &str,
    entry: &TableEntry,
    filter: &Option<Filter>,
    extra_filter: &Option<Filter>,
) -> Result<Vec<(Tid, Vec<Value>)>> {
    let predicate = filter
        .as_ref()
        .map(|filter| {
            let analyzer = engine.inverted_analyzer_for(entry.oid, &filter.column);
            filter_to_predicate_with_analyzer(entry, filter, &analyzer)
        })
        .transpose()?;
    let mut rows = engine.scan_inner(snap, table, predicate.as_ref())?;
    if let Some(filter) = extra_filter {
        let analyzer = engine.inverted_analyzer_for(entry.oid, &filter.column);
        let predicate = filter_to_predicate_with_analyzer(entry, filter, &analyzer)?;
        rows.retain(|(_, values)| predicate.matches(&values[predicate.col_index()]));
    }
    Ok(rows)
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
    extra_filter: &Option<Filter>,
    order_by: &Option<OrderByKind>,
    limit: &Option<usize>,
) -> Result<crate::QueryResult> {
    // EXPLAIN shares normal SELECT's shape gate and vector validation but
    // never executes the inner B+Tree/ANN plan.
    Engine::validate_select_shape(entry, columns, order_by)?;
    if let Some(OrderByKind::Expr(expr)) = order_by {
        prepare_distance(entry, expr)?;
    }
    let predicate = filter
        .as_ref()
        .map(|filter| filter_to_predicate(entry, filter))
        .transpose()?;
    let request =
        build_request_with_extra(engine, table, entry, filter, extra_filter, order_by, limit)?;
    let mut plan = plan_for_predicate(&request)?;
    let key_too_large = predicate.as_ref().is_some_and(|predicate| {
        matches!(
            encode_key(predicate.value()),
            Err(pg_am_btree::BTreeError::KeyTooLarge(_))
        )
    });
    if (matches!(&plan.spec, ScanSpec::BTree(_)) && !snapshot_allows_btree(engine, snap))
        || matches!(&plan.spec, ScanSpec::Inverted(spec)
            if !engine.inverted_snapshot_allows(spec.index_oid, snap))
    {
        plan.spec = ScanSpec::Seq(crate::planner::SeqScanSpec {
            predicate: filter.clone(),
            extra_predicate: extra_filter.clone(),
            order_by: order_by.clone(),
            limit: *limit,
            est_rows: 0,
            est_pages: 0,
            reason: crate::planner::SeqScanReason::SnapshotFallback,
        });
    } else if key_too_large && matches!(&plan.spec, ScanSpec::BTree(_)) {
        plan.spec = ScanSpec::Seq(crate::planner::SeqScanSpec {
            predicate: filter.clone(),
            extra_predicate: extra_filter.clone(),
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
    let est_rows =
        execute_seq_with_extra(engine, snap, table, entry, filter, extra_filter)?.len() as u64;
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
        ScanSpec::Hnsw(_) | ScanSpec::Inverted(_) => {}
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
        CmpOp::FullText => {
            return Err(EngineError::Unsupported(
                "a full-text predicate cannot execute through a B+Tree".to_string(),
            ))
        }
    };
    let col_types = column_types(entry);
    let mut rows = Vec::new();
    for candidate in candidates {
        let Some(visible_tid) = engine.heap_tuple_visible(snap, candidate)? else {
            continue;
        };
        let values = engine.read_row_by_tid(snap, entry, &col_types, visible_tid)?;
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
        execute(engine, &snap, "t", &entry, &filter, &None, &None, &None).unwrap()
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
        execute(engine, snap, "t", &entry, &filter, &None, &None, &None).unwrap()
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
        let output = execute(
            &engine,
            &snap,
            "hook_t",
            &entry,
            &id_eq(1),
            &None,
            &None,
            &None,
        )
        .unwrap();
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
    fn ordinary_order_by_uses_pg_null_defaults() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
        engine.exec(None, "CREATE TABLE t (id INT4)").unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (NULL), (1)")
            .unwrap();

        for (direction, expected) in [("ASC", Some(Datum::Int4(1))), ("DESC", None)] {
            let result = engine
                .exec(
                    None,
                    &format!("SELECT * FROM t ORDER BY id {direction} LIMIT 1"),
                )
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected rows")
            };
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], expected, "unexpected {direction} NULL order");
        }
        engine.shutdown();
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
