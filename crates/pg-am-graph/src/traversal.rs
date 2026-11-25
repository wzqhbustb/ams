//! Traversal request and hard resource-budget contracts.

use crate::error::{GraphError, Result};
use crate::model::{GraphDirection, MAX_GRAPH_DEPTH};

/// Default maximum number of returned hits.
pub const DEFAULT_MAX_RESULTS: usize = 100_000;
/// Default maximum number of visited nodes.
pub const DEFAULT_MAX_VISITED_NODES: usize = 1_000_000;
/// Default maximum projected response size.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Traversal algorithm selected by SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphTraversalAlgorithm {
    /// Breadth-first search with shortest-path output.
    Bfs,
    /// Depth-first search with deterministic preorder output.
    Dfs,
}

/// Hard budgets supplied only by engine configuration, never SQL syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphTraversalOptions {
    /// Maximum number of hits before returning `Capacity`.
    pub max_results: usize,
    /// Maximum number of visited nodes before returning `Capacity`.
    pub max_visited_nodes: usize,
    /// Maximum response bytes before returning `Capacity`.
    pub max_response_bytes: usize,
}

impl Default for GraphTraversalOptions {
    fn default() -> Self {
        Self {
            max_results: DEFAULT_MAX_RESULTS,
            max_visited_nodes: DEFAULT_MAX_VISITED_NODES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }
}

/// Fully typed BFS/DFS request. Execution lands in Stage B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphTraversalRequest {
    /// BFS or DFS.
    pub algorithm: GraphTraversalAlgorithm,
    /// Existing node at which traversal starts.
    pub start_node_id: i64,
    /// Adjacency direction.
    pub direction: GraphDirection,
    /// Inclusive depth limit in `0..=3`.
    pub max_depth: u32,
    /// Hard resource budgets.
    pub options: GraphTraversalOptions,
}

impl GraphTraversalRequest {
    /// Construct a request after enforcing the frozen depth range.
    pub fn new(
        algorithm: GraphTraversalAlgorithm,
        start_node_id: i64,
        direction: GraphDirection,
        max_depth: u32,
        options: GraphTraversalOptions,
    ) -> Result<Self> {
        if max_depth > MAX_GRAPH_DEPTH {
            return Err(GraphError::InvalidArgument(format!(
                "graph depth {max_depth} exceeds {MAX_GRAPH_DEPTH}"
            )));
        }
        Ok(Self {
            algorithm,
            start_node_id,
            direction,
            max_depth,
            options,
        })
    }
}
