# pg_rust Development Roadmap

## Design principles and discipline

1. **Build strictly in layers**: Layer 1 → Layer 2 → Layer 3. A shaky foundation guarantees the upper layers collapse.
2. **Deliverable at every phase**: by the end of each phase, the internal Agent team can connect and use it through psql/gRPC/MCP clients.
3. **AMs land in priority order**: HNSW → FT → TS → Graph → Columnar. Never attempt all six at once.
4. **Every new AM ships with Vacuum/GC**: no technical debt left to the end.
5. **Keep AM boundaries clean**: the kernel provides primitives (TTL/downsampling/delete); policy lives in the upper SDK. The LLM always stays external.
6. **Correctness > performance > features**: better to drop a feature than to ship a bug.
7. **No phase-skipping, no over-design**: enter the next phase only when every acceptance check of the current one passes. Interfaces may be extensible, but implementations never land early.
8. **One technical blog post per phase**: material for outward communication.

---

## Phase overview

```
Phase 1: storage foundation + row store + transactions + B+Tree
         ├── M1: physical layer (Page/WAL/BufferPool/LSN)
         ├── M2: row store + MVCC + B+Tree + crash recovery
         │    ├── M2a: single-statement auto-commit
         │    ├── M2b: multi-statement transactions + MVCC
         │    └── M2c: full lock management + concurrency
         └── M3: Vacuum (offline) + observability + minimal PG Wire
              (online Vacuum is explicitly assigned to the multi-AM GC
              coordinator in Phase 5b; see the Phase 5b section)

Phase 2: HNSW vector index
         ├── 2a: in-memory HNSW
         ├── 2b: WAL + persistence + crash recovery
         └── 2c: concurrency control + Tier 1 synchronization

Phase 3: full-text inverted index
         ├── inverted AM + BM25
         ├── Tier 2 fully landed
         └── Segment merge + GC

Phase 4a: SQL layer + PG Wire Protocol (basics)
Phase 4b: Multi-Path Fusion + cross-modal planner

Phase 5a: time series + column store + distillation stub
         (Phase 1 gate; may start alongside Phase 2/3/4a)
         ├── kernel: TimeSeries AM + TTL, columnar projection prototype
         └── SDK: memory distillation (stub may start in parallel early)

Phase 5b: graph + GC + forgetting/layering + Fusion integration
         (needs 4a / ≥2 AM / 4b landing progressively)
         ├── kernel: Graph AM (lightweight) + graph query syntax,
         │   time-range query routing, multi-AM GC coordinator
         ├── SDK: forgetting curve, memory layering (after the GC coordinator)
         └── Fusion integration: graph/time-series into Fusion (after 4b)

Phase 6: full protocol layer + multi-agent isolation

Phase 7: production hardening
         ├── 7a: observability + backup/restore
         ├── 7b: performance + compression
         ├── 7c: full CBO
         └── 7d: high availability + advanced features
```

---

## Overall schedule estimate

| Phase | Optimistic | P50 (typical pace) | Long tail (P0 blockers) | Risk |
|---|---|---|---|---|
| Phase 1 (M1+M2+M3) | 6 months | 9–12 months | 15+ months | 🔴 High (transactions/recovery is the core) |
| Phase 2 (HNSW) | 9 months | 12–15 months | 24+ months | 🔴 High (long pole) |
| Phase 3 (Inverted) | 4 months | 6–8 months | 10 months | 🟡 Medium (new segment model) |
| Phase 4a+4b (SQL+PG → Fusion) | 4 months | 6–8 months | 12 months | 🟡 Medium (DataFusion integration) |
| Phase 5a (TS+columnar+distill stub, parallel with 2/3/4a) | 1.5 months | 2–3 months | 4 months | 🟡 Medium (parallel with the main line) |
| Phase 5b (graph+GC+forgetting/layering+Fusion) | 2 months | 3–4 months | 6 months | 🟡 Medium (SDK/kernel boundary, many gates) |
| Phase 6 (protocol+isolation) | 6 months | 9–12 months | 18+ months | 🔴 High (full protocol) |
| Phase 7 (production) | ongoing | 12+ months | ongoing | 🟢 Low (continuous optimization) |

**Total P50: 55–75 months / about 5–6 years (1–2 senior Rust engineers)**

> Note: Phase 1 and Phase 2 carry the highest risk; actual duration may land at the upper end of the range.

---

## Current progress (2026-10-27)

| Phase | Status | Notes |
|---|---|---|
| Phase 1 (M1+M2+M3) | ✅ Complete | Page/WAL/BufferPool, MVCC transactions and locks, B+Tree, crash recovery, Vacuum, minimal PG Wire — all through multiple adversarial reviews and full regression |
| Phase 2a (In-memory HNSW) | ✅ Complete | Self-built HNSW (no third-party library), siftsmall recall@10 = 0.9990; L2/Cosine distance functions; snapshot loading |
| Phase 2b (WAL + persistence) | ✅ Complete | Page-resident graph + 8 WAL record types + crash recovery; 25/1000 SIGKILL crash rounds; post-recovery recall bit-identical to baseline |
| Phase 2c (concurrency + Tier 1) | 🚧 Closing | Concurrency protocol (shared handle + sharded latches + four loom models), Tier 1 transaction integration (in-row NodeId + sidecar map + IndexUndo), visibility-filtered search pipeline, vacuum extensions (leak accounting / local rebuild / dead-row backfill) — all delivered and reviewed; pgvector comparison landed (recall 0.9990 vs 0.9940, insert-throughput gap; see phase2-m6-benchmarks.md). **Still open**: first 24h stress run stalled at the watchdog (root cause under investigation), CI coverage report (needs a PR), the `phase2-m6` release tag (to be cut once CI is green, via branch + PR) |
| Phase 3 (full-text inverted) | ⏳ Next milestone | Tech selection starting: segment-based inverted AM + BM25/WAND, Tier 2 WAL catch-up + watermark, merge/vacuum, NDCG comparison |
| Phase 4a (SQL+PG Wire) | ⏳ Not started | Depends on the Phase 3 main line |
| Phase 4b (Fusion + Planner) | ⏳ Not started | Depends on Phase 4a |
| Phase 5a (TS+columnar+distill stub) | ⏳ Can start anytime | Phase 1 gate, parallel with Phase 2/3/4a; segment merge may reuse the Phase 3 architecture or a simple seal+TTL variant |
| Phase 5b (graph+GC+forgetting/layering+Fusion) | ⏳ Not started | Graph queries/time routing need Phase 4a; multi-AM GC coordinator needs ≥2 AM (already satisfied); Fusion tails need Phase 4b |
| Phase 6 (full protocol+isolation) | ⏳ Not started | Needs Phase 4a's PG Wire foundation and SQL layer |
| Phase 7 (production) | ⏳ Not started | After 4b + 5b + 6 |

**Near-term order**: main line Phase 3 → 4a → 4b; Phase 5a can be inserted in parallel anytime; Phase 5b/6 follow 4a, Fusion tails follow 4b; M6's three closing items (PR/CI report, 24h rerun, tag) proceed in parallel with Phase 3, not blocking each other.

---

## Phase × Layer mapping

| Phase | Layer 1 (physical) | Layer 2 (transactions/visibility) | Layer 3 (Access Methods) | Cross-layer capabilities |
|---|---|---|---|---|
| Phase 1 M1 | ✓ Page/WAL/BufferPool/LSN/Checkpoint | — | — | (file management spread across components, not exposed separately) |
| Phase 1 M2 | (ext) Full Page Image | ✓ MVCC / Lock / Snapshot / Visibility | ✓ B+Tree (with AccessMethod trait) | ARIES crash recovery |
| Phase 1 M3 | — | (ext) Vacuum | — | Minimal PG Wire + observability |
| Phase 2 | (Tier 1 async IO) | (ext) Per-tx delta | ✓ HNSW (Epoch + node locks) | HNSW Vacuum |
| Phase 3 | (Tier 2 async IO) | (ext) Watermark | ✓ Inverted Index (BM25) | Segment merge |
| Phase 4a | — | — | (single-path selection) | DataFusion + PG Wire Extended + EXPLAIN |
| Phase 4b | — | (ext) Cost hooks | (multi-path fusion) | MultiIndexScan + Fusion + Planner |
| Phase 5a | — | — | ✓ TimeSeries | Columnar projection prototype |
| Phase 5b | — | (ext) Multi-AM GC coordination | ✓ Graph (lightweight) | Fusion integration: time series + graph |
| Phase 6 | — | (ext) RLS predicate | (stable) | MCP Server + full PG Wire |
| Phase 7a | — | — | — | Backup/restore + monitoring |
| Phase 7b | (performance) | — | ✓ Columnar Projection | SIMD / io_uring / huge pages |
| Phase 7c | — | ✓ Full CBO + statistics | (compression) SQ/PQ | Cross-modal cost model |
| Phase 7d | — | ✓ SSI | — | High availability + Jepsen |

---

## Phase dependencies

```
Main line (must be linear):
Phase 1 ──→ Phase 2 ──→ Phase 3 ──→ Phase 4a ──→ Phase 4b
(foundation+row) (vector)   (full-text) (SQL+PG)   (Fusion)

Parallel tracks (do not block the main line; start early if capacity allows):

Phase 5a (Phase 1 gate, may start with Phase 2/3/4a):
  ├── TimeSeries AM + TTL ─────── parallel with Phase 2/3/4;
  │                               note: segment merge logic is reused after
  │                               Phase 3 completes, or implemented as a simple
  │                               variant (time-partition seal + TTL cleanup only)
  ├── Columnar projection proto ── parallel with Phase 2/3
  └── Distillation SDK ─────────── external LLM; the stub may start early

Phase 5b · 4a gate (needs the SQL layer, not 4b):
  ├── Graph AM (lightweight) + graph query syntax ── needs the SQL layer ready
  └── Time-range query routing ──────────────────── planner predicate routing = 4a's job

Phase 5b · ≥2 AM gate (orthogonal to 4b, a vacuum-era concern):
  └── Multi-AM GC coordinator ── can start once ≥2 AMs have landed;
                                  only then do the forgetting curve / memory-layering
                                  SDKs get a real mark_for_gc trait to call

Phase 5b · 4b gate (only the two Fusion integration tails):
  ├── Graph into Fusion ──── feed Graph output into 4b's MultiIndexScan operator
  └── Time series into Fusion ─ feed TimeSeries output into 4b's MultiIndexScan operator

Phase 6 (4a gate, starts in the same batch as Phase 5b · 4a):
  ├── Full PG Protocol ─── needs Phase 4a's PG Wire foundation
  └── MCP Server ──────── needs Phase 4a's SQL layer

Note: Phase 5b and 4b have a【partial, one-way】dependency: only the
"graph/time-series into Fusion" tails wait for 4b; every other Phase 5b
deliverable depends on Phase 4a or ≥2 AM and may start in parallel with 4b.
Conversely, 4b does not depend on Phase 5 (4b only needs HNSW + inverted + planner).
Graph AM as a whole is gated on 4a (the "interfaces may be extensible but
implementations never land early" principle); if the team accepts the SQL
integration rework risk, the Graph AM storage layer + BFS/DFS primitives may
start at 5a, with only the graph query syntax left to 5b.

Phase 4b + Phase 5b + Phase 6 → Phase 7
```

---

## Phase 1: Storage foundation + row store + transactions + B+Tree (✅ Complete)

**Goal: a minimal usable database so Agents can read and write structured metadata**

**Time estimate (1–2 senior Rust engineers):** optimistic 6 months / P50 9–12 months / long tail 15+ months
**Risk:** 🔴 High (transactions/recovery is the core and decides the project's success)

### Milestone 1: Physical layer

| Module | Notes |
|------|------|
| Page Allocator | Fixed-size page alloc/free (8KB/16KB configurable), freelist management, no assumptions about page contents |
| WAL Writer | Append-only log accepting (record_type, payload), fsync semantics, CRC32 checksums; full physical WAL (before/after images); logical WAL interface reserved (implemented when Phase 2 HNSW lands) |
| Buffer Pool | page_id → in-memory frame map, LRU/CLOCK replacement, pin/unpin protocol, WAL-before-data rule (related WAL must be durable before a page is flushed) |
| LSN Clock | Globally monotonically increasing, shared by all components |
| File management | Data-file/WAL/metadata operations live inside PageAllocator/WalWriter/Superblock, not exposed separately; O_DIRECT is a Phase 7b optimization |

**Acceptance:**
- **Correctness first (no hard performance targets)**:
  - WAL sequential-write correctness: every record fully recoverable after a crash
  - Buffer Pool correctness: no pin/unpin leaks, no lost pages on eviction
  - Unit-test coverage ≥ 90%
  - proptest validation of Page Allocator (no leaks, no overlaps)
- **Reference performance baseline (only enforced in Phase 7b)**:
  - WAL ≥ 200MB/s (sequential writes, 60% of local SSD physical ceiling)
  - Buffer Pool ≥ 50K ops/s (random 8KB page reads)
- **Crash testing**: random kill -9 × 1000 with no data loss

### Milestone 2: Row store + transactions + B+Tree + crash recovery

#### Internal M2 checkpoints

To avoid building full MVCC/deadlock detection/B+Tree in one shot before architecture problems surface, M2 splits into three internal checkpoints:

| Checkpoint | Goal | Verification |
|---|---|---|
| M2a | Single-statement auto-commit + heap table + non-concurrent B+Tree | `INSERT` / `SELECT` / `UPDATE` / `DELETE` correct single-threaded |
| M2b | Multi-statement transactions + MVCC snapshot + visibility judgment | No dirty reads under concurrent read/write; RC/SI snapshots correct |
| M2c | Full lock management + deadlock detection + concurrent B+Tree | 100 concurrent CRUD + random crash tests pass |

#### Deliverables

| Module | Notes |
|------|------|
| Heap Storage | Slotted-page row store with variable-length fields, TOAST overflow pages (large objects/vectors/JSONB stay out of the main row) |
| Tuple format | Fat header (xmin, xmax, agent_id, trace_id, flags) + fixed-width scalar columns + column pointers |
| Transaction Manager | begin/commit/abort, 64-bit transaction IDs (no wraparound) |
| Snapshot mechanism | **LSN-based snapshot** (consistent with the "single WAL + single LSN" root contract); snapshot = {xmin_lsn, xmax_lsn, active_xacts: ATT snapshot}; no XID wraparound. Default isolation: Snapshot Isolation (SI); RC (fresh snapshot per statement) optional; SSI deferred to Phase 7d |
| Visibility Oracle | is_visible(xmin, xmax, snapshot), one shared judgment used by every AM |
| Lock Manager | Row locks (tuple-header xmax field, S/X modes), table locks (4 standard modes), wait queue + deadlock detection (wait-for graph, 100ms cycle). IS/IX intention locks deferred to Phase 6 (ALTER TABLE / VACUUM FULL support) |
| B+Tree Index | Latch-coupled reads, optimistic/pessimistic inserts, leaf splits, AccessMethod trait. **Page merge explicitly out of scope** (split only, never merge; deletes reclaim entries, not pages): merging needs a left-sibling latch plus parent downlink deletion, which breaks every concurrency-correctness argument of the Blink one-way lock order (DOWN/RIGHT). Residual shape: bounded space growth from delete-drain empty pages (churn measured ~0.18 pages/round, acceptance bound 0.25 pages/round). Owned by Phase 5b (multi-AM GC coordinator — safe page release needs horizon advancement + online incremental reclamation + background scheduling) |
| Crash recovery | Full ARIES variant: Analysis → Redo → Undo; CLR guarantees safety under nested crashes |
| Checkpoint | Fuzzy Checkpoint: collect ATT+DPT, flush dirty pages in the background, update the superblock |
| Full Page Image | Full page copy recorded on the first modification of a page within each checkpoint interval, preventing torn pages |

**Acceptance:**
- ACID correctness: concurrent read/write + random process kills, data always consistent after restart
- Simple Agent metadata read/write works (session records, user profiles, configuration)
- Concurrency: TPS ≥ 10K with 100 concurrent connections (simple CRUD, single table, SSD, no protocol overhead)

### Milestone 3: Basic Vacuum + observability + minimal PG Wire

| Module | Notes |
|------|------|
| Vacuum | Scan dead tuples (xmax committed and not referenced by any active snapshot), reclaim space, notify B+Tree to remove matching entries |
| Observability | Human-readable WAL dump, active-transaction listing, lock-wait inspection, Buffer Pool hit-rate stats, query statistics (pg_stat_statements equivalent: per-query latency, rows, scan path) |
| Minimal PG Wire Protocol | Simple Query + text result format only, enough for psql / standard PG drivers to run CREATE/INSERT/SELECT/UPDATE/DELETE |
| SegmentedStorage interface reservation | Phase 3 (Inverted) and Phase 5 (TimeSeries) are both segment-based; reserve: SegmentedStorage trait (create_segment/freeze/seal/merge), segment lifecycle, WAL protocol extension (SEGMENT_SEAL/SEGMENT_MERGE record types) |
| Tier 2 interface reservation | WAL tail reader interface, watermark registry interface, planner hook for index freshness |
| ~~Online/incremental Vacuum~~ | **Out of M3 scope**. Explicitly assigned to Phase 5b (multi-AM GC coordinator): M3 delivers only offline `Engine::vacuum(table)` (one `AccessExclusive` pass) plus every reusable foundation (snapshot-registry watermark, in-page compaction primitives, index notification path, WAL record family). If the write-stop window becomes a real problem after M3 acceptance, mitigate with "segmented offline" (batch locks per page range) rather than building a separate online middle state |

**Acceptance:**
- Space reusable after Vacuum, no unbounded growth
- psql connects and runs basic SQL (CREATE TABLE / INSERT / SELECT / UPDATE / DELETE)
- Mainstream PG drivers (psycopg2, node-postgres, rust-postgres) connect successfully
- Transaction and lock problems diagnosable from the command line

### Value of Phase 1 for the Agent team

- Replaces SQLite/PG as the store for structured Agent metadata
- Native session_id, agent_id, timestamp fields
- Row-level provenance (who wrote it, when)
- psql / Python / TypeScript clients work out of the box

---

## Phase 2: HNSW vector index (🚧 Closing; open items in "Current progress")

**Goal: vector storage and nearest-neighbor search so Agents can do semantic memory recall**

**Time estimate (1–2 senior Rust engineers):** optimistic 9 months / P50 12–15 months / long tail 24+ months
**Risk:** 🔴 High (long pole of the entire roadmap)

### Why split it

HNSW is the largest engineering effort of the six AMs:
- Concurrency control (HNSW undo is a known hard problem)
- WAL integration (logical WAL + consistency repair)
- Crash recovery (Epoch snapshot + incremental replay)
- Visibility filtering (2x candidates + top-K)

Any one of them can stall the whole phase. Split into 3 sub-phases, each with its own demo.

### Phase 2a: In-memory HNSW (3 months)

**Goal:** build and query the graph in memory; no persistence required

| Module | Notes |
|------|------|
| VECTOR(n) type | First-class data type, DDL-level declaration, f32/f16/bf16 support |
| In-memory HNSW | Hierarchical random graph, M/M_max, neighbor selection (simple / robust prune), greedy search |
| Distance functions | L2 / Cosine / Inner Product, correct basic implementations; SIMD optimization reserved for the end of 2a or Phase 7b |
| Load API | Load a prebuilt graph from disk (hnswlib-format compatible) |

**Acceptance:**
- 1M 768d vectors recall@10 ≥ 95% (sift-128-euclidean, gist-960-euclidean)
- 1M-vector load time < 5 minutes
- Search latency P99 < 20ms (ef_search=64)
- Unit tests ≥ 90%

### Phase 2b: HNSW WAL + persistence (4 months)

**Goal:** HNSW changes go into the single WAL and fully recover after a crash

| Module | Notes |
|------|------|
| Logical WAL records | add_node(v, neighbors), connect(a, b), remove_node(tombstone) |
| On-disk neighbor lists | Node pages written through the Buffer Pool, WAL-before-data enforced |
| Checkpoint HNSW snapshot | Entry point, max level, node count |
| Crash recovery | Replay from checkpoint + WAL, verify graph consistency |
| Idempotent redo | Replaying the same WAL record N times yields the same result |

**Acceptance:**
- 1M vectors, random kill -9 × 1000, graph semantics stay consistent
- Restart recovery time < 30 seconds (incremental replay after checkpoint)

### Phase 2c: HNSW concurrency control + Tier 1 synchronization (5–7 months)

**Goal:** HNSW stays correct under high concurrency

| Module | Notes |
|------|------|
| Epoch-based reclamation | Safe reclamation that prevents use-after-free |
| Node-level fine-grained locks | Not page-level, avoiding overly coarse granularity |
| Tier 1 synchronization | Accumulate deltas inside a transaction, merge into the HNSW graph in bulk at commit |
| Visibility adaptation | Fetch 2x candidates, filter through heap visibility before returning top-K |
| Concurrent undo | Mark tombstone on abort + background repair of neighbor connectivity |
| Vacuum extensions | Tombstone-ratio monitoring + local rebuild |

**Acceptance:**
- 100 concurrent INSERT + DELETE for 24 hours with consistent graph semantics:
  - Bidirectional-edge asymmetry increment over the single-threaded baseline < 1% (baseline = M4 Stage B measured aggregate 15.35%, per-cell 8.9%–17.1% — the inherent asymmetry of HNSW's one-sided shrink removals, same order as hnswlib/pgvector; the <1% bound constrains the **increment** introduced by concurrency/crashes/repair, not the absolute rate. 2026-08-31 M4 Stage B adversarial review P2-1 revision: the original absolute reading was falsified by measurement — even a correct implementation could never pass it)
  - recall@10 ≥ 95% of the pre-crash value
  - No dangling nodes (every node reachable from the entry point)
- recall@10 after concurrent aborts ≥ 95% of the single-threaded baseline
- Comparison: pgvector (HNSW) and Qdrant on recall/latency/throughput

### Phase 2 SQL examples

**TOAST decision (Phase 2 prerequisite, decided 2026-08-31, removing the "invisible prerequisite blocker"):**
- **HNSW vectors do not go through heap TOAST**: vectors live in `pg-am-hnsw`'s own node storage (M4 contiguous in-memory arena, M5 node-page layout) — distance computation sits on the graph-traversal hot path, so vectors must stay co-resident with the graph; fetching them from the heap is not viable. M2 selection §4's assumption that "Phase 2 HNSW storing 4KB vectors must use TOAST" is superseded by the M4 design (`pg-am-hnsw` has zero dependency on `pg-am-heap`).
- **VECTOR(n) heap column values stored inline**: dim ≤ 2000 as the upper bound (aligned with M5's single-page node-layout feasibility); larger dims fail loudly; M5 open issue O1 (node-page overflow plan for >2000 dims) remains assigned to M5.
- **Heap TOAST chunk I/O itself belongs to Phase 4a** (see the Phase 4a deliverables table): Phase 1's status quo is inline storage up to single-page capacity with a loud `TupleTooLarge` error beyond it — Phase 2 in its entirety (2a/2b/2c) does not depend on heap TOAST, so the blocker is gone with no functional reduction.

```sql
-- Invoked through PG Wire Protocol (the Phase 1 M3 minimal subset suffices)
INSERT INTO memories (id, content, embedding) VALUES ('m1', 'hello', '[0.1, 0.2, ...]');

-- Pure vector search
SELECT id, content FROM memories
ORDER BY embedding <=> $1
LIMIT 10;

-- Vector + structured filter combo (Phase 1 B+Tree + Phase 2 HNSW)
SELECT id, content FROM memories
WHERE agent_id = 'a1'
ORDER BY embedding <=> $1
LIMIT 10;
```

### Value of Phase 2 for the Agent team

- Embedding storage and semantic search for Agent conversations/documents
- Combined with structured filters (WHERE team='sales' filters through B+Tree first, then vector ordering)
- Semantic dedup of memories (approximate-vector detection)
- Single-transaction guarantee: inserting a memory and its vector either both succeed or both fail

---

## Phase 3: Full-text inverted index (⏳ Next milestone: tech selection starting)

**Goal: BM25 full-text search so Agents can do keyword memory recall**

**Time estimate (1–2 senior Rust engineers):** optimistic 4 months / P50 6–8 months / long tail 10 months
**Risk:** 🟡 Medium (new segment model, but Tantivy/Lucene references exist)

### Deliverables

| Module | Notes |
|------|------|
| Full-text index declaration | WITH (fulltext_index = 'bm25') DDL-level declaration |
| Inverted Index AM | AccessMethod trait implementation, segment-based architecture |
| Tokenizers | Chinese (jieba-rs) + English (stemming + stop words), pluggable |
| Posting List | Ordered TID list per term + term frequency + position info |
| BM25 scoring | Standard BM25 formula (k1=1.2, b=0.75), WAND acceleration |
| BM25 global statistics | doc_count, avgdl, term_df maintained with atomic increments; persisted to the metadata file at checkpoint |
| Segment architecture | Write buffer (in-memory) → flush to immutable segments → background merge |
| Tier 2 fully landed | Background worker consumes the WAL stream, maintains applied_lsn, planner is watermark-aware |
| Watermark mechanism | At query time the planner checks whether the inverted index's applied_lsn covers the query snapshot |
| Watermark monitoring | watermark_lag_seconds metric exported so the planner can decide on fallback; when lag exceeds a configurable threshold, the background worker's consumption rate is automatically increased |
| Fallback strategy | When the watermark lags: 1) query only flushed immutable segments, ignoring the unflushed write buffer; 2) if the query needs fresher data and segments don't cover it, fall back to seqscan; 3) on large tables the seqscan fallback MUST be bounded by timeout/sampling limits to avoid hanging queries |
| Segment Merge | Background merge of small segments, cleaning up deleted posting entries |
| Vacuum extensions | Physically clean deleted TIDs' postings during segment merge |
| Crash recovery | Discard the unflushed write buffer and resume catch-up from applied_lsn |

### Acceptance

- Retrieval quality: NDCG@10 on standard IR datasets at Tantivy level
- Tier 2 latency: written rows searchable in < 1 second (normal load)
- Crash recovery: after random kills, catch up correctly from applied_lsn with no loss or duplication
- Comparison: Tantivy / Elasticsearch on retrieval quality and latency

### Value of Phase 3 for the Agent team

- Keyword search over Agent memory ("the contract dispute we discussed last time")
- Complements vector search (vectors find semantic similarity, full-text finds exact keywords)
- No external ES to operate, less operational complexity

---

## Phase 4a: SQL layer + PG Wire Protocol (basics) (⏳ Not started)

**Goal: make pg_rust fully accessible through standard SQL and PG drivers**

**Time estimate (1–2 senior Rust engineers):** optimistic 2 months / P50 3–4 months / long tail 6 months
**Risk:** 🟡 Medium (DataFusion integration carries TP adaptation risk)

### DataFusion TP adaptation checkpoint

The following must be verified at the START of Phase 4a:

1. DataFusion can serve simple point queries with latency < 5ms (single connection)
2. DataFusion can pass transaction context through to TableProvider correctly
3. If either check fails, the "bypass DataFusion for the TP path" fallback plan MUST be activated

Fallback plan outline:
- Simple point queries / small-range queries go through the custom executor
- Complex analytical queries go through DataFusion
- The SQL layer routes uniformly, transparently to upper layers

### Deliverables

| Module | Notes |
|------|------|
| DataFusion integration | SQL parsing, logical planning, physical planning; custom TableProvider wired to pg_rust storage |
| Single-path index scan | The optimizer can choose B+Tree / HNSW / inverted index for a single predicate |
| PG Wire Protocol (basic) | Simple Query + Extended Query (parameter binding), text/binary result formats |
| EXPLAIN | Single-path scan plan + cost estimation |
| heap TOAST chunk I/O | Phase 1 deferred item: overflow storage for >8KB text/JSONB/oversized vectors (chunk-table read/write + main-row 20B pointer resolution; WAL reuses `HeapInsert`/`HeapDelete`, pointer format frozen). Current status: inline up to single-page capacity with a loud error beyond it, no functional reduction. Links into TOAST compression (Phase 7b) |
| Sequence (SERIAL) | Phase 1 trimmed item (M2 §18 once listed it under M3, M3 O6 decided against): `nextval`/`currval` + catalog registration, landed once the DataFusion SQL layer is in place |

### Acceptance

- psql connects and runs CREATE/INSERT/SELECT/UPDATE/DELETE
- DataFusion correctly pushes filters down into pg_rust's TableProvider
- Mainstream PG drivers (psycopg2, node-postgres, rust-postgres) connect successfully
- Simple point-query latency < 5ms (or go through the fallback executor if the bypass is active)

### What Phase 4a does NOT do

- MultiIndexScan / Fusion (Phase 4b)
- Complete type mapping for Extended Query mode (Phase 6)
- Cross-modal Planner (Phase 4b)

---

## Phase 4b: Multi-Path Fusion + cross-modal Planner (⏳ Not started)

**Goal: a single SQL statement that walks multiple indexes and fuses their results — pg_rust's core differentiator**

**Time estimate (1–2 senior Rust engineers):** optimistic 2 months / P50 3–4 months / long tail 6 months
**Risk:** 🟡 Medium

### Deliverables

| Module | Notes |
|------|------|
| MultiIndexScan operator | Start several AM scans in parallel, merge TIDs per fusion strategy, unified visibility check, fetch rows from the heap |
| Fusion strategies | Filter-style (A∩B∩C), RRF ranking (Reciprocal Rank Fusion), hybrid (hard filter + soft ranking) |
| Cross-modal Planner (v1) | Recognizes multi-modal predicates, splits them into per-AM scan paths, picks the fusion strategy |
| Basic Cost Model | Selectivity estimation per AM: B+Tree histograms, HNSW distance distributions, inverted term frequencies |
| EXPLAIN enhancements | Multi-path scan plan + per-path cost + fusion strategy |

### Example query

```sql
SELECT * FROM agent_memory
WHERE team = 'sales'                    -- B+Tree path
  AND content @@ 'contract'             -- Inverted path
ORDER BY embedding <=> $vec             -- HNSW path
LIMIT 10;
```

Execution plan:
```
MultiIndexScan (fusion=hybrid, hard_filter=[btree, inverted], soft_rank=[hnsw])
├── BTreeScan (team = 'sales') → TID set A
├── InvertedScan (content @@ 'contract') → TID set B
├── HnswScan (embedding <=> $vec, ef=128) → TID set C (ranked)
├── Filter: A ∩ B
├── Rank: filtered set by HNSW score from C
├── VisibilityCheck
└── HeapFetch → Top 10 rows
```

### Acceptance

- Correctness: fused results match "query separately then merge in the application layer" 100%
- Performance: parallel multi-path < 60% of the sum of serial per-path time
- Quality: RRF fusion's NDCG beats the best single path
- Comparison: end-to-end latency against "PG + pgvector + pg_search, queried separately and merged in the application"

### End of Phase 4b: open-source Alpha preparation

**Why this milestone:**
- Phase 4b delivers "one SQL statement for hybrid recall" — the best outward demo
- Early user feedback can correct direction before Phase 5/6 development begins

**Checklist:**
- README + CONTRIBUTING + LICENSE (Apache 2.0 or MIT)
- CI/CD working (GitHub Actions: rust check + cargo test + benchmark)
- API docs generated automatically (cargo doc + mdbook)
- Reproducible public performance benchmarks (standard datasets + scripts)
- Audience statement (explicit scope: single-node, Agent-memory scenarios first)

**Explicitly NOT promised (avoid overcommitting):**
- No cross-platform promise (initially macOS + Linux)
- No distributed promise
- No full-PostgreSQL-compatibility promise

### Value of Phase 4 for the Agent team

- **Core value:** one "recall" call for an Agent = one SQL statement using semantics + keywords + structure at once
- Application-layer code for memory recall gets dramatically simpler (no more calling three services and merging results manually)
- Transactional guarantee: hybrid retrieval results are snapshot-consistent
- **This is the strongest outward demo scenario**

---

## Phase 5a: Time series + column store + distillation stub (parallel with Phase 2/3/4a) (⏳ Can start anytime)

**Goal: deliver the infrastructure for time-dimension memory management; can start independently once Phase 1 is done**

**Time estimate (1–2 senior Rust engineers):** optimistic 1.5 months / P50 2–3 months / long tail 4 months
**Risk:** 🟡 Parallel with the main line (Phase 1 gate; the segment-merge reuse depends on Phase 3's Tier-2 async pipeline and is incremental)

### 5a.1 Kernel deliverables (required)

| Module | Notes |
|------|------|
| TimeSeries AM | Time-partitioned storage (auto day/hour partitions), range scans, downsampling aggregation (segment-merge reuse depends on Phase 3's Tier-2 async pipeline) |
| TTL auto-expiry | Declarative TTL (WITH ts_partition = 'day', ttl = '90d'), automatic cleanup of expired partitions in the background |
| Columnar projection prototype | Lightweight columnar materialized views for time-series/memory analytics, maintained asynchronously by Tier 2, validating HTAP architecture feasibility; **prototype excludes compression encoding** (plain column layout is enough; encoding/compression belongs to the Phase 7b production version, registered 2026-09-02) |

### 5a.2 SDK deliverables (required, but not in the kernel)

| Module | Notes |
|------|------|
| Memory distillation SDK | Many detailed memories → one summary memory (via external LLM), executed as a background async job that never blocks write transactions; distilled results are written back as new tuples and re-indexed (the stub may start in parallel at Phase 5a) |

### Acceptance

- Time-series query performance: 100M time points, hot-partition range-query latency < 5ms (SSD, no aggregation); queries with downsampling aggregation < 50ms
- TTL correctness: expired data is not queryable after background cleanup, space is reclaimed
- Distillation correctness: distilled summary memories are retrievable by both vector and full-text indexes

---

## Phase 5b: Graph + GC + forgetting/layering + Fusion integration (⏳ Not started)

**Goal: complete the relationship dimension, unified GC, forgetting/layering, and wire time-series/graph into MultiIndexScan**

**Time estimate (1–2 senior Rust engineers):** optimistic 2 months / P50 3–4 months / long tail 6 months
**Risk:** 🟡 Many gates (4a / ≥2 AM / 4b landing progressively)

### 5b.1 Kernel deliverables (required)

| Module | Notes | Start gate |
|------|------|--------|
| Time-range queries | WHERE created_at BETWEEN ... AND ... routed automatically to the time-series index | Phase 4a (planner routing) |
| Graph AM (lightweight) | Adjacency-list storage, directed/undirected edges, edge properties (JSONB), BFS/DFS up to 3 hops | Phase 4a (SQL layer ready) |
| Graph query syntax | SQL extensions (LATERAL recursion or Cypher-like clauses) | Phase 4a |
| Multi-AM unified GC coordinator | Unified Vacuum coordination driven by oldest_active_snapshot; when dead tuples are reclaimed, every index referencing the TID is notified. **Includes online Vacuum**: online incremental vacuum for heap/B+Tree plus **B+Tree page merge + in-page dead-space compaction** (Phase 1 M2 deferred items: delete-drain empty-page merge + page release, leaf dead-space reclamation — requires breaking the one-way lock order and reusing the horizon safety judgment) landing as the coordinator's first consumers (page-level TOCTOU re-verification, throttling/backpressure, incremental progress tracking, background scheduling lifecycle). Also settled in the same window, the Phase 1 small items: batched/per-page index-cleanup interfaces for vacuum (replacing per-entry random descents), lock-free reader chain-generation checks for cross-relation page reuse, vacuum crash-window ② elimination (the single-page leak between unlink→free, needs PageFree payload versioning). All M3 offline-vacuum deliverables (watermark registry, compaction primitives, index notification path, `HeapCleanup`/`PageFree` WAL families) are reused directly as its underlying capabilities | ≥2 AM landed (orthogonal to 4b) |
| Graph into Fusion | Graph traversal results can be jointly retrieved with vector/full-text/structured | Phase 4b (MultiIndexScan operator) |
| Time series into Fusion | Time-series index joins MultiIndexScan, enabling "last 7 days + semantic similarity + keyword match" combinations | Phase 4b |

### 5b.2 SDK deliverables (required, but not in the kernel)

| Module | Notes | Start gate |
|------|------|--------|
| Forgetting-curve SDK | Importance scoring from access frequency + time decay, marking memories as evictable (application-layer scoring + kernel GC trigger) | GC coordinator landed (≥2 AM) |
| Memory-layering SDK | View abstractions for short-term (in-session) → working (task-level) → long-term (persistent) memory | GC coordinator landed (≥2 AM) |

**Layer-boundary notes:**
- Kernel traits expose only atomic capabilities like `mark_for_gc(tids) / vacuum_range()` (provided by Phase 5b's multi-AM GC coordinator)
- Scoring algorithms, LLM calls, and layering policy all live in the SDK, never in the kernel
- Graph AM stays wholly in 5b (avoid building the implementation before the SQL layer is settled); if parallel work is needed earlier, split "adjacency-list storage + traversal operators" from "SQL/query syntax" into storage/query halves — the storage half may drop to 5a, but the query half still needs 4a, and the storage-interface rework risk must be accepted

### Acceptance

- Graph traversal: million-edge scale, 3-hop traversal < 50ms
- GC coordination: no TID leaks and no dangling index entries across multiple AMs

### Value of Phase 5 (5a+5b) for the Agent team

- "Show every interaction from the last 7 days on a timeline" — time-series queries (5a)
- "User A complained about shipping last week → link to order X" — graph traversal (5b)
- Automatic forgetting of unimportant memories, keeping the memory store from growing without bound (5b)
- Memory distillation: Agents automatically consolidate fragmented memories into structured knowledge (5a stub → 5b full)

---

## Phase 6: Full protocol layer + multi-agent isolation (⏳ Not started)

**Goal: provide standard service capabilities externally**

**Time estimate (1–2 senior Rust engineers):** optimistic 6 months / P50 9–12 months / long tail 18+ months
**Risk:** 🔴 High (full PG protocol compatibility + RLS correctness)

### Deliverables

| Module | Notes |
|------|------|
| Full PG Wire Protocol | Full Extended Query support, prepared statements, parameter binding, type mapping, error-code compatibility |
| MCP Server | Native MCP tool interface exposing memory read/write/retrieval/management |
| RLS (Row Level Security) | Agent-level row-level security policies, data isolation across agents |
| Multi-tenant quotas | Per-agent storage/query quota limits |
| IS/IX intention locks | Coordination between table-level operations (ALTER TABLE / VACUUM FULL) and row-level operations |
| Full multixact | Phase 1 deferred item: durable segment storage for shared row locks, off-page member sets, exact recovery after crashes (currently a single-bit + in-memory holder registry simplified version, discarded on crash with the "dead XID stamp counts as aborted" fallback). lock_timeout / NOWAIT / SKIP LOCKED land in the same window |
| Subtransactions and statement-level rollback | Phase 1 trimmed item: SAVEPOINT / failed-txn state / statement failure must not destroy the transaction; on landing, re-review EPQ (EvalPlanQual) and the cmin/cmax combo CID, plus the three `lock_tuple` entries covering the lossy `t_cid` bookkeeping |
| System-table-ization and catalog expansion | pg_type-family catalog probes (psql `\d` works), QueryStats moved into system tables (replacing the in-memory ring buffer), multi-page catalog (removing the single-page system-table capacity cap, currently ~70–80 tables hits CatalogFull) |

### Acceptance

- PG protocol compatibility: full test matrix across mainstream PG drivers (psycopg2, node-postgres, rust-postgres, asyncpg)
- MCP integration: Claude Code / Dify can use memory capabilities directly through MCP
- RLS correctness: Agent A cannot read Agent B's data (also correct under hybrid retrieval)
- RLS predicates injected uniformly through the Fusion operator, per-tenant isolation tests pass

### Value of Phase 6 for the Agent team

- Multi-agent collaboration: shared memory with permission boundaries
- Outward adoption: standard interfaces, third-party Agent frameworks integrate directly
- Claude Code / Dify / LangChain and other mainstream frameworks can use it via MCP or the PG protocol

---

## Phase 7: Production hardening (⏳ Not started)

**Goal: reach externally deployable production quality**

**Time estimate:** ongoing / 12+ months
**Risk:** 🟢 Low (optimization phase, no architectural risk)

### Phase 7a: Observability + backup/restore (3–4 months)

| Module | Notes |
|------|------|
| Monitoring | Prometheus metrics export, slow-query log, query heatmaps |
| Physical backup | File-level snapshots (CoW minimal version, used only for snapshot backup) |
| Logical backup | Export/import tooling |
| PITR | WAL-based Point-in-Time Recovery |
| WAL Shipping | For hot standby (no automatic primary-replica failover) |
| Autovacuum in production form | Background automatic vacuum scheduling (built on Phase 5b's online vacuum capabilities: watermark monitoring, throttle parameters, trigger policy) + VACUUM FULL/CLUSTER |

**Outstanding technical debt (from Phase 1 Stage B):**
- **WAL LSN hole compatibility**: Phase 1's `LsnClock::reserve` placeholder mechanism (used to eliminate the checkpoint FPI race) allows legitimate zero-byte holes in the WAL stream (process crashes after `reserve` without executing `append_at`). The current recovery scanner treats zero bytes as end-of-WAL, which is correct locally. But in a WAL Shipping setup, a receiver hitting zero bytes cannot distinguish "sender has not finished sending" from "a legitimate hole meaning end of WAL". **Direction**: introduce a PostgreSQL-like WAL page header (with `xlp_pageaddr` continuity checking + page magic) so the receiver can tell whether a page was fully received. This MUST land before WAL Shipping is implemented.
- **WAL/Recovery hardening bundle (a Phase 1 leftover cluster; fixes are all recorded in stage_spec)**: ① ~~Torn-page exposure on reclaimed pages~~ **pulled forward and fixed in Phase 2 M4** (decision 2026-08-31: after the new AM lands, page recycling happens more often and exposure probability rises; see docs/phase2-m4-coding-plan.md); ② split-CLR redo isomorphic asymmetry window (the `force_reload_from_disk` repair mode is known); ③ WAL torn-tail header self-CRC (the PG xl_crc model, eliminating the silent truncation window from payload_len bit-rot amplification); ④ `reserve_and_append` >32B hole miss-detection (validate payload length before advancing + poison the writer on IO failure); ⑤ `open_at` missing-initial-segment warn + empty-baseline fallback is unimplemented; ⑥ analysis/replay catch-all still does warn+break on `MetadataCorrupted`/`WalReadFailed`, a silent truncation surface.
- **Catalog-corruption repair tooling**: with a corrupted system page header and existing user DDL, open is currently rejected with no repair path (self-healing rewrite would orphan user tables); delivered as part of 7a's backup/restore tool family.
- **Recovery performance**: merge the three full scans (find_latest can short-circuit) + the two full-page scans in undo (`scan_split_incomplete_pages`/`find_parent_page`) for large-database recovery I/O optimization.
- **Long-run stability bundle**: concurrent crash 1000 rounds and the 100-conn × 60min challenge configuration enter 7a acceptance (a Phase 1 unexecuted item; commands in docs/phase1-m2-benchmarks.md §reproduction); the WAL-segment recycling boundary of loser index compensation resolves naturally once WAL archiving lands.

### Phase 7b: Performance and compression (3–4 months)

| Module | Notes |
|------|------|
| Columnar projection (production) | Columnar materialized views for general AP workloads, maintained asynchronously by Tier 2, vectorized scans; **columnar encoding details (not previously listed, registered 2026-09-02)**: dictionary encoding / RLE / bit-packing selected automatically per column type + block-level compression (LZ4/zstd chosen at 7b kickoff, aligned with PG 14+ TOAST/backup-chain algorithms) |
| Vector compression | Scalar Quantization (SQ) / Product Quantization (PQ), automatic strategy selection |
| Full SIMD optimization | All distance functions, B+Tree comparisons, CRC checks |
| io_uring | Linux async I/O |
| Huge pages | Fewer TLB misses |
| Phase 1 performance-debt payoff | Batch commit to amortize fsync (the M1/M2 unmet-item root fix), commit/checkpoint barrier narrowed to the critical section, ClogBuffer sharding, moving fsync waits out of allocation_lock, early release of B+Tree pessimistic-write "safe nodes", hint-bit write-back wiring (replacing per-access CLOG lookups), CLOCK-Pro / freelist bitmap / WAL segment reuse and compression / multi-file data storage, TOAST compression (LZ4/PGLZ, contingent on Phase 4a's TOAST implementation) |
| B+Tree structural maintenance | tid separators (root fix for ~200k duplicate-key degradation across internal pages) + root fix for stale internal separator-key gaps wedging (separator-key maintenance / internal-level left-jump protocol, eliminating the durable loud Unsupported surface); before landing, the accepted position is probability argument + the REINDEX escape hatch |

### Phase 7c: Full CBO (4–6 months)

| Module | Notes |
|------|------|
| Cross-modal statistics collection | Per-AM statistics shapes: B+Tree histograms, HNSW distance distributions, Inverted term frequencies, TimeSeries bucket distributions |
| Cost-model calibration | Cost-parameter calibration tooling driven by real workloads |
| EXPLAIN cross-modal decomposition | Per-access-path cost breakdown, making plan choices explainable |

### Phase 7d: High availability + advanced features (ongoing)

| Module | Notes |
|------|------|
| Jepsen testing | Standardized testing of transaction isolation + crash recovery |
| SSI (Serializable) | Full SSI conflict detection (predicate locks + dangerous structures) |
| Primary-replica failover | WAL Shipping + Raft/Paxos leader election |
| Documentation and SDKs | Python/TypeScript/Go SDKs + full API docs + example projects |

### Phase 7 acceptance (overall)

- Jepsen tests pass (transaction isolation + crash recovery)
- 7×24 stable operation (72 hours of high-concurrency stress with no OOM/deadlock/data loss)
- Comprehensive benchmark comparison: end-to-end performance in the Agent-memory scenario ≥ 80% of the PG+pgvector+ES solution

---

## Milestones and Agent-team integration points

| Completed phase | Capabilities the Agent team can start using | External components replaced |
|----------|------------------------|--------------|
| Phase 1 M3 | Structured metadata read/write (psql / Python clients) | SQLite / PG (basic CRUD) |
| Phase 2a | Semantic memory retrieval (in-memory, lost on restart) | Prototyping/validation only |
| Phase 2c | Semantic memory retrieval (persistent + concurrency-safe) | pgvector / Qdrant |
| Phase 3 | Keyword memory retrieval (BM25 full-text) | Elasticsearch / Meilisearch |
| Phase 4b | **One SQL statement for hybrid recall** (the core demo) | Application-layer multi-service assembly |
| Phase 5a | Time-series memory + distillation stub (delivered in parallel with 2/3/4a) | — |
| Phase 5b | Full memory lifecycle management + graph relations + forgetting/layering | Custom forgetting/distillation logic |
| Phase 6 | Standard-protocol external service + multi-agent isolation | — |
| Phase 7 | Production-grade deployment | The whole multi-engine architecture |

---

## Benchmark strategy per phase

| Phase | Comparison target | Test scenario | Pass line |
|------|---------|---------|--------|
| Phase 1 | SQLite / PG | Pure TP (mixed CRUD workload) | ≥ 50% of SQLite performance |
| Phase 2 | pgvector / Qdrant | Pure vector retrieval (ANN benchmark) | recall@10 ≥ 95%, latency gap < 2x |
| Phase 3 | Tantivy / ES | Pure full-text retrieval (standard IR datasets) | NDCG@10 aligned, latency gap < 3x |
| Phase 4b | PG+pgvector+ES assembly | End-to-end hybrid retrieval | Total latency < 50% of the assembled solution |
| Phase 5 | TimescaleDB / Neo4j | Time-series ranges / graph traversal | Latency gap < 3x |

---

## Explicit strategic boundaries

| Capability | Decision | Phase | Notes |
|---|---|---|---|
| HTAP (unified row+column engine) | **Do (prototype + production)** | Phase 5 (columnar projection prototype) + Phase 7b (production) | Supports the "unified engine" differentiator |
| CoW snapshots (basics) | **Do (minimal)** | Phase 7a (inside backup/restore) | Used only for snapshot backup, not a general feature |
| CoW second-level branching (Agent sandbox) | **Defer** | Not in the 7-phase roadmap | Not required by current Agent scenarios |
| Serverless / compute-storage separation | **Do not** | — | Strategic choice: focus on kernel quality, do not compete with Neon |
| Cross-region distributed transactions | **Do not** | — | Single-node route |
| Multi-tenant RLS | **Do** | Phase 6 | Hard requirement for multi-agent isolation |
| Multi-tenant quotas | **Do** | Phase 6 | Same as above |
