//! Phase 4a: the AM registry and the single-path selector.
//!
//! The planner's shape is deliberately a REGISTRY + selector, never an
//! if/else on AM type (the Phase 4a pluggability constraint): a new AM
//! (Phase 3's inverted index) registers later by adding an `AmRegistration`
//! entry, without touching the selector's core dispatch structure.

use crate::error::{EngineError, Result};
use crate::sql::{DistanceOp, Expr, Filter, Literal, OrderByKind, VectorOpclass};

/// The access-method kind registered with the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmKind {
    /// The B+Tree access method.
    BTree,
    /// The HNSW vector access method.
    Hnsw,
}

/// A registered access method. The registration owns both shape matching
/// and planning, so the selector does not need to know AM-specific AST forms.
pub struct AmRegistration {
    /// The kind of access method being registered.
    pub kind: AmKind,
    /// Whether this access method recognizes the request's shape.
    pub matches: fn(&ScanRequest) -> bool,
    /// The scan planner entry point for this access method.
    pub scan: fn(&ScanRequest) -> Result<ScanPlan>,
}

/// The index metadata needed by the single-path planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    /// The access method that owns this index.
    pub kind: AmKind,
    /// The indexed column name.
    pub column: String,
    /// The vector opclass, for HNSW indexes.
    pub opclass: Option<VectorOpclass>,
    /// The index's configured HNSW search breadth, when applicable.
    pub ef_search_default: Option<usize>,
}

/// A single-path scan request (one predicate + optional ORDER BY/LIMIT and
/// the indexes available on the table).
pub struct ScanRequest {
    /// The table being scanned.
    pub table: String,
    /// The optional predicate applied by the scan.
    pub predicate: Option<Filter>,
    /// The optional ordering requested by the query.
    pub order_by: Option<OrderByKind>,
    /// The optional maximum number of result rows.
    pub limit: Option<usize>,
    /// Metadata for indexes available on this table.
    pub indexes: Vec<IndexMetadata>,
}

/// A single-path scan plan. `spec` is the sole source of execution-path truth.
pub struct ScanPlan {
    /// The concrete scan specification.
    pub spec: ScanSpec,
}

/// The concrete plan shape per AM.
pub enum ScanSpec {
    /// A B+Tree scan with its predicate and estimates.
    BTree(BTreeScanSpec),
    /// An HNSW approximate nearest-neighbor scan.
    Hnsw(HnswScanSpec),
    /// A sequential heap scan.
    Seq(SeqScanSpec),
}

/// The parameters and estimates for a B+Tree scan.
pub struct BTreeScanSpec {
    /// The predicate used for the index lookup.
    pub predicate: Option<Filter>,
    /// The optional ordering applied after the index scan.
    pub order_by: Option<OrderByKind>,
    /// The optional maximum number of result rows.
    pub limit: Option<usize>,
    /// The estimated number of result rows (Stage A/B placeholder).
    pub est_rows: u64,
    /// The estimated number of pages read (Stage A/B placeholder).
    pub est_pages: u64,
}

/// The parameters for an HNSW approximate nearest-neighbor scan.
pub struct HnswScanSpec {
    /// The HNSW search breadth.
    pub ef: usize,
    /// The vector opclass whose distance metric is used.
    pub opclass: VectorOpclass,
    /// The query vector literal (decoded by Stage B/C execution).
    pub query: Literal,
    /// The requested number of nearest neighbors.
    pub k: usize,
    /// The number of candidates requested before visibility filtering.
    pub candidates: usize,
    /// The maximum number of candidate retry rounds.
    pub max_retry_rounds: u8,
}

/// The parameters for a sequential heap scan, including the complete
/// filter/order/limit shape needed by the fallback executor.
pub struct SeqScanSpec {
    /// The optional predicate applied while scanning.
    pub predicate: Option<Filter>,
    /// The optional ordering applied after filtering.
    pub order_by: Option<OrderByKind>,
    /// The optional maximum number of result rows.
    pub limit: Option<usize>,
    /// Why the planner selected a sequential scan.
    pub reason: SeqScanReason,
}

/// Why a query fell back to SeqScan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqScanReason {
    /// No index or ordering directive was requested.
    Plain,
    /// No suitable index exists on the queried column.
    NoIndex,
    /// The distance operator does not match the index opclass.
    MetricMismatch,
    /// ANN selection requires a LIMIT.
    MissingLimit,
    /// ANN selection only supports ascending distance order.
    Desc,
    /// ANN selection cannot be combined with an additional WHERE predicate.
    ExtraWhere,
}

impl ScanPlan {
    /// EXPLAIN output: path type, predicate, AM parameters, cost estimate
    /// (cost is a Stage A/B placeholder, not a real CBO).
    pub fn explain(&self) -> String {
        match &self.spec {
            ScanSpec::BTree(s) => format!(
                "BTreeScan (predicate={:?}, order_by={:?}, limit={:?}, est_rows={}, est_pages={})",
                s.predicate, s.order_by, s.limit, s.est_rows, s.est_pages
            ),
            ScanSpec::Hnsw(s) => format!(
                "HnswScan (ef={}, opclass={:?}, query={:?}, k={}, candidates={}, max_retry_rounds={})",
                s.ef, s.opclass, s.query, s.k, s.candidates, s.max_retry_rounds
            ),
            ScanSpec::Seq(s) => format!(
                "SeqScan (reason={:?}, predicate={:?}, order_by={:?}, limit={:?})",
                s.reason, s.predicate, s.order_by, s.limit
            ),
        }
    }
}

/// The built-in access-method registry. A later AM adds a registration here
/// without changing the selector's dispatch structure.
pub const AM_REGISTRY: &[AmRegistration] = &[
    AmRegistration {
        kind: AmKind::BTree,
        matches: btree_matches,
        scan: plan_btree,
    },
    AmRegistration {
        kind: AmKind::Hnsw,
        matches: hnsw_matches,
        scan: plan_hnsw,
    },
];

/// The planner's single-path selection for one predicate. Every AM owns its
/// shape matcher; the selector only iterates registrations and dispatches.
pub fn plan_for_predicate(req: &ScanRequest) -> Result<ScanPlan> {
    plan_with_registry(req, AM_REGISTRY)
}

fn plan_with_registry(req: &ScanRequest, registry: &[AmRegistration]) -> Result<ScanPlan> {
    for registration in registry {
        if (registration.matches)(req) {
            return (registration.scan)(req);
        }
    }
    let reason = if req.predicate.is_some() {
        SeqScanReason::NoIndex
    } else {
        SeqScanReason::Plain
    };
    Ok(seq_plan(req, reason))
}

fn seq_plan(req: &ScanRequest, reason: SeqScanReason) -> ScanPlan {
    ScanPlan {
        spec: ScanSpec::Seq(SeqScanSpec {
            predicate: req.predicate.clone(),
            order_by: req.order_by.clone(),
            limit: req.limit,
            reason,
        }),
    }
}

fn index_on<'a>(req: &'a ScanRequest, kind: AmKind, column: &str) -> Option<&'a IndexMetadata> {
    req.indexes
        .iter()
        .find(|index| index.kind == kind && index.column.eq_ignore_ascii_case(column))
}

fn btree_matches(req: &ScanRequest) -> bool {
    // Distance expressions always belong to the HNSW shape handler, even
    // when an additional WHERE predicate has a usable B+Tree. The HNSW
    // handler is responsible for the required ExtraWhere SeqScan fallback.
    if matches!(&req.order_by, Some(OrderByKind::Expr(_))) {
        return false;
    }
    let Some(predicate) = &req.predicate else {
        return false;
    };
    index_on(req, AmKind::BTree, &predicate.column).is_some()
}

fn hnsw_matches(req: &ScanRequest) -> bool {
    matches!(&req.order_by, Some(OrderByKind::Expr(_)))
}

/// Plan an ANN-shaped request, including its explicit SeqScan fallbacks.
fn plan_hnsw(req: &ScanRequest) -> Result<ScanPlan> {
    let Some(OrderByKind::Expr(expr)) = &req.order_by else {
        return plan_btree(req);
    };
    if req.predicate.is_some() {
        return Ok(seq_plan(req, SeqScanReason::ExtraWhere));
    }
    if req.limit.is_none() {
        return Ok(seq_plan(req, SeqScanReason::MissingLimit));
    }
    if expr.desc {
        return Ok(seq_plan(req, SeqScanReason::Desc));
    }
    let Expr::Column(column) = &expr.left else {
        return Err(EngineError::InvalidArgument(
            "ANN left operand must be a column reference".to_string(),
        ));
    };
    let Expr::Literal(query) = &expr.right else {
        return Err(EngineError::InvalidArgument(
            "ANN right operand must be a vector literal".to_string(),
        ));
    };
    if !matches!(query, Literal::Vector(_) | Literal::Str(_)) {
        return Err(EngineError::InvalidArgument(
            "ANN right operand must be a vector literal".to_string(),
        ));
    }
    let required = match expr.op {
        DistanceOp::L2 => VectorOpclass::VectorL2Ops,
        DistanceOp::Cosine => VectorOpclass::VectorCosineOps,
        DistanceOp::InnerProduct => VectorOpclass::VectorIpOps,
    };
    let Some(index) = index_on(req, AmKind::Hnsw, column) else {
        return Ok(seq_plan(req, SeqScanReason::NoIndex));
    };
    if index.opclass != Some(required) {
        return Ok(seq_plan(req, SeqScanReason::MetricMismatch));
    }
    let ef = index.ef_search_default.ok_or_else(|| {
        EngineError::InvalidArgument("HNSW index metadata lacks ef_search_default".to_string())
    })?;
    let k = req.limit.expect("checked above");
    Ok(ScanPlan {
        spec: ScanSpec::Hnsw(HnswScanSpec {
            ef,
            opclass: required,
            query: query.clone(),
            k,
            candidates: k.saturating_mul(2),
            max_retry_rounds: 3,
        }),
    })
}

/// Plan an ordinary predicate through B+Tree, or a plain sequential scan.
fn plan_btree(req: &ScanRequest) -> Result<ScanPlan> {
    let Some(predicate) = &req.predicate else {
        return Ok(seq_plan(req, SeqScanReason::Plain));
    };
    if index_on(req, AmKind::BTree, &predicate.column).is_none() {
        return Ok(seq_plan(req, SeqScanReason::NoIndex));
    }
    Ok(ScanPlan {
        spec: ScanSpec::BTree(BTreeScanSpec {
            predicate: Some(predicate.clone()),
            order_by: req.order_by.clone(),
            limit: req.limit,
            est_rows: 0,
            est_pages: 0,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{CmpOp, OrderBy, OrderByExpr};

    fn hnsw_metadata(opclass: VectorOpclass, ef: usize) -> IndexMetadata {
        IndexMetadata {
            kind: AmKind::Hnsw,
            column: "v".to_string(),
            opclass: Some(opclass),
            ef_search_default: Some(ef),
        }
    }

    fn btree_metadata(column: &str) -> IndexMetadata {
        IndexMetadata {
            kind: AmKind::BTree,
            column: column.to_string(),
            opclass: None,
            ef_search_default: None,
        }
    }

    fn req_with_expr(
        op: DistanceOp,
        limit: Option<usize>,
        index_opclass: Option<VectorOpclass>,
        predicate: Option<Filter>,
        desc: bool,
    ) -> ScanRequest {
        let indexes: Vec<IndexMetadata> = index_opclass
            .map(|opclass| hnsw_metadata(opclass, 64))
            .into_iter()
            .collect();
        ScanRequest {
            table: "t".to_string(),
            predicate,
            order_by: Some(OrderByKind::Expr(OrderByExpr {
                op,
                left: Expr::Column("v".to_string()),
                right: Expr::Literal(Literal::Vector(vec![1.0, 2.0, 3.0])),
                desc,
            })),
            limit,
            indexes,
        }
    }

    fn filter() -> Filter {
        Filter {
            column: "id".to_string(),
            op: CmpOp::Eq,
            value: Literal::Int(1),
        }
    }

    fn ordinary_request(indexes: Vec<IndexMetadata>) -> ScanRequest {
        ScanRequest {
            table: "t".to_string(),
            predicate: Some(filter()),
            order_by: None,
            limit: None,
            indexes,
        }
    }

    #[test]
    fn registry_dispatches_builtin_access_methods() {
        assert_eq!(AM_REGISTRY.len(), 2);
        let ann = plan_for_predicate(&req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            None,
            false,
        ))
        .unwrap();
        assert!(matches!(ann.spec, ScanSpec::Hnsw(_)));

        let btree = plan_for_predicate(&ordinary_request(vec![btree_metadata("id")])).unwrap();
        assert!(matches!(btree.spec, ScanSpec::BTree(_)));
    }

    #[test]
    fn registry_can_match_a_new_shape_without_selector_changes() {
        fn custom_matches(req: &ScanRequest) -> bool {
            req.table == "custom"
        }
        fn custom_scan(req: &ScanRequest) -> Result<ScanPlan> {
            Ok(seq_plan(req, SeqScanReason::Plain))
        }

        let req = ScanRequest {
            table: "custom".to_string(),
            predicate: Some(filter()),
            order_by: None,
            limit: None,
            indexes: vec![],
        };
        let registration = AmRegistration {
            kind: AmKind::BTree,
            matches: custom_matches,
            scan: custom_scan,
        };
        let plan = plan_with_registry(&req, &[registration]).unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::Plain),
            _ => panic!("expected custom registration to run"),
        }
    }

    #[test]
    fn ann_path_when_everything_matches() {
        let plan = plan_for_predicate(&req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            None,
            false,
        ))
        .unwrap();
        match plan.spec {
            ScanSpec::Hnsw(ref s) => {
                assert_eq!(s.k, 10);
                assert_eq!(s.candidates, 20);
                assert_eq!(s.max_retry_rounds, 3);
                assert_eq!(s.opclass, VectorOpclass::VectorL2Ops);
                assert_eq!(s.query, Literal::Vector(vec![1.0, 2.0, 3.0]));
                assert_eq!(s.ef, 64);
            }
            _ => panic!("expected HnswScan"),
        }
        assert!(plan.explain().contains("HnswScan"));
    }

    #[test]
    fn ann_uses_index_ef_and_saturating_candidates() {
        let mut req = req_with_expr(
            DistanceOp::L2,
            Some(usize::MAX),
            Some(VectorOpclass::VectorL2Ops),
            None,
            false,
        );
        req.indexes[0].ef_search_default = Some(96);
        let plan = plan_for_predicate(&req).unwrap();
        match plan.spec {
            ScanSpec::Hnsw(s) => {
                assert_eq!(s.ef, 96);
                assert_eq!(s.candidates, usize::MAX);
            }
            _ => panic!("expected HnswScan"),
        }
    }

    #[test]
    fn ann_checks_the_indexed_column() {
        let mut req = req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            None,
            false,
        );
        let Some(OrderByKind::Expr(expr)) = &mut req.order_by else {
            unreachable!()
        };
        expr.left = Expr::Column("other".to_string());
        let plan = plan_for_predicate(&req).unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::NoIndex),
            _ => panic!("expected NoIndex fallback for a different column"),
        }
    }

    #[test]
    fn ann_rejects_invalid_operands() {
        let mut req = req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            None,
            false,
        );
        let Some(OrderByKind::Expr(expr)) = &mut req.order_by else {
            unreachable!()
        };
        expr.right = Expr::Literal(Literal::Int(1));
        assert!(plan_for_predicate(&req).is_err());

        let mut req = req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            None,
            false,
        );
        let Some(OrderByKind::Expr(expr)) = &mut req.order_by else {
            unreachable!()
        };
        expr.left = Expr::Literal(Literal::Vector(vec![1.0]));
        assert!(plan_for_predicate(&req).is_err());
    }

    #[test]
    fn missing_limit_falls_back() {
        let plan = plan_for_predicate(&req_with_expr(
            DistanceOp::L2,
            None,
            Some(VectorOpclass::VectorL2Ops),
            None,
            false,
        ))
        .unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::MissingLimit),
            _ => panic!("expected SeqScan"),
        }
    }

    #[test]
    fn desc_falls_back() {
        let plan = plan_for_predicate(&req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            None,
            true,
        ))
        .unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::Desc),
            _ => panic!("expected SeqScan"),
        }
    }

    #[test]
    fn no_index_falls_back() {
        let plan = plan_for_predicate(&req_with_expr(DistanceOp::L2, Some(10), None, None, false))
            .unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::NoIndex),
            _ => panic!("expected SeqScan"),
        }
    }

    #[test]
    fn metric_mismatch_falls_back() {
        let plan = plan_for_predicate(&req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorCosineOps),
            None,
            false,
        ))
        .unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::MetricMismatch),
            _ => panic!("expected SeqScan"),
        }
    }

    #[test]
    fn extra_where_falls_back() {
        let plan = plan_for_predicate(&req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            Some(filter()),
            false,
        ))
        .unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::ExtraWhere),
            _ => panic!("expected SeqScan"),
        }
    }

    #[test]
    fn btree_does_not_capture_ann_with_extra_where() {
        let mut req = req_with_expr(
            DistanceOp::L2,
            Some(10),
            Some(VectorOpclass::VectorL2Ops),
            Some(filter()),
            false,
        );
        req.indexes.push(btree_metadata("id"));
        let plan = plan_for_predicate(&req).unwrap();
        match &plan.spec {
            ScanSpec::Seq(s) => {
                assert_eq!(s.reason, SeqScanReason::ExtraWhere);
                assert_eq!(s.predicate, Some(filter()));
                assert_eq!(s.order_by, req.order_by);
                assert_eq!(s.limit, Some(10));
            }
            _ => panic!("B+Tree must not capture an ANN request"),
        }
    }

    #[test]
    fn plain_column_order_by_is_btree() {
        let mut req = ordinary_request(vec![btree_metadata("id")]);
        req.order_by = Some(OrderByKind::Column(OrderBy {
            column: "id".to_string(),
            desc: false,
        }));
        req.limit = Some(5);
        let plan = plan_for_predicate(&req).unwrap();
        match &plan.spec {
            ScanSpec::BTree(s) => {
                assert_eq!(s.predicate, Some(filter()));
                assert_eq!(
                    s.order_by,
                    Some(OrderByKind::Column(OrderBy {
                        column: "id".to_string(),
                        desc: false,
                    }))
                );
                assert_eq!(s.limit, Some(5));
            }
            _ => panic!("expected BTreeScan"),
        }
        assert!(plan.explain().contains("limit=Some(5)"));
    }

    #[test]
    fn ordinary_predicate_without_btree_index_falls_back() {
        let mut req = ordinary_request(vec![]);
        req.order_by = Some(OrderByKind::Column(OrderBy {
            column: "id".to_string(),
            desc: true,
        }));
        req.limit = Some(7);
        let plan = plan_for_predicate(&req).unwrap();
        match &plan.spec {
            ScanSpec::Seq(s) => {
                assert_eq!(s.reason, SeqScanReason::NoIndex);
                assert_eq!(s.predicate, Some(filter()));
                assert_eq!(s.order_by, req.order_by);
                assert_eq!(s.limit, Some(7));
            }
            _ => panic!("expected NoIndex SeqScan"),
        }
        assert!(plan.explain().contains("predicate=Some"));
    }

    #[test]
    fn plain_select_is_seqscan_plain() {
        let req = ScanRequest {
            table: "t".to_string(),
            predicate: None,
            order_by: None,
            limit: None,
            indexes: vec![],
        };
        let plan = plan_for_predicate(&req).unwrap();
        match plan.spec {
            ScanSpec::Seq(s) => assert_eq!(s.reason, SeqScanReason::Plain),
            _ => panic!("expected plain SeqScan"),
        }
    }
}
