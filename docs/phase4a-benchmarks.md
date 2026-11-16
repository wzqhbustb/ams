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

## Stage C: Extended Query and driver acceptance

- Date: 2026-11-10 (UTC)
- Protocol: `cargo test -p pg-wire --no-fail-fast` passed **79 tests**: 35 protocol/type tests, 23 Extended Query state/golden tests, 11 real-socket rust-postgres client tests, and 10 additional Stage C review probes. Engine parameter binding passed 10 focused tests; the pg-engine full suite also passed.
- psql: `scripts/phase4a_stagec_psql.sh` passed with exact golden-output diff for CRUD, transactions, and parameterized ANN.
- node-postgres: `NODE_PATH=target/tmp/stagec-node/node_modules node scripts/phase4a_stagec_node.js` — **PASS**, including server-side `$1..$n` CRUD and ANN parameters.
- psycopg2: `target/tmp/stagec-drivers-venv/bin/python scripts/phase4a_stagec_psycopg2.py` — **PASS**. psycopg2 composes ordinary cursor parameters client-side, so this is a CRUD/transaction/ANN driver check, not a claim of server-side Bind; raw VECTOR literals use `AsIs`.
- rust-postgres: real TCP typed CRUD, binary scalar results, NULL, EXPLAIN, error recovery, and parameterized ANN passed in `wire_clients.rs`; VECTOR binary layout and suspension/resume have exact golden coverage in `wire_extended.rs`.

These are correctness/protocol acceptance results, not latency benchmarks. Third-party drivers were installed only under `target/tmp` in isolated environments.

Final gates: workspace debug tests and release tests for all four changed runtime crates exited 0; all eight crate Clippy checks, rustdoc, fmt, and `git diff --check` passed. The final independent review found no remaining P1/P2/P3 issues.

## Stage D: TOAST correctness and crash acceptance

- Date: 2026-11-11 (UTC)
- `cargo test -p pg-engine --test phase4a_toast`（debug 与 release）：**17 tests passed**. Coverage includes >8KB multi-chunk TEXT/BYTEA, multi-attribute aggregate overflow, external VECTOR roundtrip, checkpointless reopen and OID/valueid progress, unchanged-group sharing, old-snapshot UPDATE/DELETE × vacuum, aborted INSERT/UPDATE groups, missing/duplicate chunks, legacy catalog migration, and two uncommitted WAL windows.
- Crash windows: chunk WAL durable without main-row WAL, and chunk + main-row WAL durable without commit. In both cases reopen exposes no row, and the following vacuum physically reclaims all three chunks; a subsequent pass reclaims zero.
- `cargo test -p pg-am-heap --lib`: **39 tests passed**, including the frozen 20-byte external pointer, `HEAP_VECTOR_EXTERNAL`, exact `0x4000_0014` inline/external disambiguation, and mixed-representation rejection.
- `cargo test -p pg-wire --test wire_protocol`: **36 tests passed**, including a 9000-byte TEXT value returned through Simple Query with exact DataRow length and payload.
- Static checks for the four changed runtime crates: Clippy `--all-targets --all-features -D warnings`, rustdoc `-D warnings`, fmt, and `git diff --check` all passed.
- Post-fix gates: `cargo test --workspace --no-fail-fast` exited 0 with no failures; release tests for `pg-am-heap`, `pg-catalog`, `pg-engine`, and `pg-wire` also exited 0. `cargo check --workspace --all-features`, all eight crate Clippy/rustdoc checks, fmt, and `git diff --check` passed.

These are correctness and recovery acceptance results, not throughput measurements. TOAST detoast currently favors a simple one-chunk-scan-per-main-row implementation; no performance claim is made for large TOAST relations. The current type system has no JSONB Datum/DDL type, so BYTEA covers the binary varlena path; JSONB-specific acceptance remains a follow-up when that type exists.

## Stage E: DataFusion AP integration

- Date: 2026-11-13 (UTC)
- DataFusion: **50.3.0** (actual resolved version), MSRV-compatible and optional. Default TP/ANN execution does not enable it.
- `cargo test -p pg-engine --features datafusion --test phase4a_datafusion`: **13 passed, 1 ignored** covering projection/order/limit, zero-column and zero-batch schemas, B+Tree and ordinary filters, in-flight/aborted visibility, old-snapshot UPDATE + vacuum, Arrow types including UUID aliases/JOIN/NULL vectors and string predicates, Guard release on collect/LIMIT/execution error, AccessShare on JOIN, current/multi-thread Tokio sync/async calls, and AND/aliases/aggregates/computed columns/JOIN result conversion.
- Manual release acceptance: `cargo test -p pg-engine --features datafusion --release --test phase4a_datafusion datafusion_point_lookup_mean_latency_is_below_5ms -- --ignored --nocapture` — **1 passed**. Point lookup mean **0.645ms** over 20 `SELECT id FROM t WHERE id = 2` calls, below the **5ms** gate. The default self-owned TP path remains in place regardless of this result.
- Feature-off behavior: `exec_datafusion` returns `Unsupported` explicitly, without changing existing TP/ANN semantics.
- Final gates: `cargo test --workspace --no-fail-fast` exited 0 with no failures; release tests for `pg-am-heap`, `pg-catalog`, `pg-engine`, and `pg-wire` also exited 0. `cargo check --workspace --all-features`, all eight crate Clippy/rustdoc checks, fmt, and `git diff --check` passed.

## Stage F: Sequence / SERIAL

- Date: 2026-11-13 (UTC)
- `cargo test -p pg-engine --test phase4a_sequence`: **20 passed**, and the same command with `--release`: **20 passed**. Coverage includes concurrent unique/contiguous nextval, per-session currval, no-INSERT crash/reopen, redo-only high-water repair followed by immediate checkpoint, rollback, CREATE/commit-failure retry, SequenceQuery, SERIAL DEFAULT/omitted/explicit/NULL values, multiple SERIAL columns, true SIGKILL after committed SERIAL INSERT, 1000 nextval calls with catalog compaction, deleted/aborted internal catalog recovery through fresh MVCC, deterministic WAL flush/compaction/internal catalog write failure retries, thread-local failure-hook isolation, 3000-character sequence and SERIAL names, and generated-name uniqueness under concurrent DDL.
- `cargo test -p pg-wire --test wire_sequence`: **4 passed**, and the same command with `--release`: **4 passed**. Simple/Extended currval survives Sync and rollback, connections are isolated, SERIAL defaults work, and SequenceQuery Describe reports INT8.
- `cargo test -p pg-storage --test wal_record_type_discriminant --test waldump`: **6 passed** total across the two files, including fixed `SequenceAdvance(128)` and `sequence_oid/high_water` dump fields.
- Final gates: all eight crate Clippy/rustdoc checks, workspace all-features check, fmt, and `git diff --check` passed; existing Wire/DataFusion regression suites also passed.
