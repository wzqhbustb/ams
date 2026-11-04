# Phase 4a Stage A Benchmarks

## B+Tree point lookup

- Date: 2026-11-03 (UTC)
- Environment: Darwin 23.5.0 arm64, rustc 1.97.1 (8bab26f4f 2026-07-14), debug build
- Command: `cargo test -p pg-engine --lib executor::tests::btree_point_lookup_mean_latency_is_below_5ms -- --nocapture`
- Fixture: three heap rows, one B+Tree index on `id INT8`; one warm-up query followed by 20 equality SELECT statements.
- Result: **29.63µs mean per statement** (latest run; other local runs were 15.27–67.91µs, all wall clock including SQL parse, planning, B+Tree lookup, heap visibility check, row decode, and query-stat recording).
- Gate: **< 5ms — PASS**

This is a focused executor sanity measurement, not a general throughput benchmark. It does not measure cold-open cost, large tables, range scans, or release-mode performance.
