# Phase 4a Benchmarks

## Stage A: B+Tree point lookup

- Date: 2026-11-03 (UTC)
- Environment: Darwin 23.5.0 arm64, rustc 1.97.1 (8bab26f4f 2026-07-14), debug build
- Command: `cargo test -p pg-engine --lib executor::tests::btree_point_lookup_mean_latency_is_below_5ms -- --nocapture`
- Fixture: three heap rows, one B+Tree index on `id INT8`; one warm-up query followed by 20 equality SELECT statements.
- Result: **29.63µs mean per statement** (latest run; other local runs were 15.27–67.91µs, all wall clock including SQL parse, planning, B+Tree lookup, heap visibility check, row decode, and query-stat recording).
- Gate: **< 5ms — PASS**

This is a focused executor sanity measurement, not a general throughput benchmark. It does not measure cold-open cost, large tables, range scans, or release-mode performance.

## Stage B: SQL ANN on siftsmall

- Date: 2026-11-09 (UTC; latest post-review run)
- Environment: Darwin 23.5.0 arm64, rustc 1.97.1 (8bab26f4f 2026-07-14), release build
- Command: `cargo test -p pg-engine --test m6_benchmark --release phase4a_sql_ann_benchmark_siftsmall -- --ignored --nocapture`
- Fixture: full siftsmall corpus (10,000 base vectors × 128 dimensions, 100 queries, supplied ground truth); SQL `CREATE INDEX ... USING hnsw (v vector_l2_ops)` with default `ef_search=64` and seed 42; all inserts committed in one explicit transaction; SQL `ORDER BY v <-> ... ASC LIMIT 10` through the Phase 4a executor.
- Result: **recall@10 = 0.9990 — PASS**. Bulk insert took 13.91s. Query latency: **0.305ms mean, 0.297ms p50, 0.412ms p95, 0.645ms max**.
- Consistency: every benchmark query also asserts `ExecutionPath::IndexLookup`, so an exact SeqScan cannot satisfy the recall gate by masquerading as ANN. Separate integration tests compare SQL ANN rows and ordering with `Engine::hnsw_search_visible` on the same snapshot for all three metrics, including after reopen.

The benchmark is ignored by default so CI does not pay the 10k graph-build cost; the command above is the manual acceptance gate.
