# agent memory storage engine

A database kernel written from scratch in Rust — storage engine, MVCC
transactions, and concurrent B+Tree indexing — built toward a unified
multi-modal engine for **AI-agent memory** (vector + full-text + graph +
time-series, with structured metadata).

> **Status (build in public):** Phase 1 is complete (tag `phase1-m3`). What
> exists today is a working, crash-safe storage/transaction **library** plus a
> minimal PostgreSQL wire-protocol server — `psql` and standard PG drivers can
> connect and run basic CRUD. Phase 2 (HNSW vector index) is now underway:
> M4 Stages A–C (crate foundations, the core graph algorithm, and the
> snapshot save/load file API) are done. See [Roadmap](#roadmap).

---

## What this is

The long-term goal (see [ROADMAP.md](ROADMAP.md)) is a single kernel that
answers queries like:

```sql
SELECT * FROM agent_memory
WHERE team = 'sales'                    -- B+Tree
  AND content @@ 'contract'             -- inverted index (Phase 3)
ORDER BY embedding <=> $vec             -- HNSW (Phase 2)
LIMIT 10;
```

…with one transaction, one snapshot, and one durability path (a single WAL +
LSN). That's the differentiating bet: multi-modal recall as **one SQL**, not
three services glued together in the application.

What is implemented so far is the foundation that makes that possible — the
physical layer, transactions/MVCC, and the B+Tree access method — done to a
standard where the hard part (ACID + crash recovery + concurrency) is already
proven.

## Current status

| Milestone | Scope | Status |
|-----------|-------|--------|
| Phase 1 M1 | Page / WAL / BufferPool / LSN / Checkpoint | ✅ done |
| Phase 1 M2a | Single-statement auto-commit + heap + B+Tree | ✅ done |
| Phase 1 M2b | Multi-statement transactions + MVCC (SI) | ✅ done |
| Phase 1 M2c | Lock management + deadlock detection + concurrent B+Tree | ✅ done |
| Phase 1 M3 | Vacuum + observability + minimal PG Wire | ✅ done |
| Phase 2 M4 | In-memory HNSW foundations (`pg-am-hnsw`) | 🚧 in progress (Stages A–C done) |
| Phase 2 M5+ | HNSW WAL + persistence, concurrency, … | 📋 planned |

## Architecture

Seven library crates in the stack, strictly layered (a lower layer never
depends on a higher one), plus a wire-protocol server on top. `pg-am-hnsw`
(Phase 2 M4, in progress) is a standalone eighth crate that joins the AM layer
when persistence lands in M5:

```
┌─────────────────────────────────────────────────────────┐
│  pg-wire     PostgreSQL v3 wire protocol (Simple Query) │
│              psql / PG drivers can connect (pg-server)  │
├─────────────────────────────────────────────────────────┤
│  pg-engine   top-level assembly: catalog + AMs + txn    │
│              SQL-string execution, DDL/DML, checkpoint  │
├─────────────────────────────────────────────────────────┤
│  pg-am-heap  │  pg-am-btree          │  pg-catalog      │
│  slotted     │  latch-coupled Blink   │  system tables, │
│  page, HOT,  │  split WAL + CLR,      │  AccessMethod   │
│  FOR SHARE   │  loom model-checked    │  trait          │
├─────────────────────────────────────────────────────────┤
│  pg-txn      XID, disk CLOG, XID snapshot, visibility,  │
│              lock manager, deadlock detection           │
├─────────────────────────────────────────────────────────┤
│  pg-storage  Page, WAL, BufferPool, LSN, checkpoint,    │
│              ARIES recovery (Analysis/Redo/Undo)        │
└─────────────────────────────────────────────────────────┘
```

## What works today

- **Storage**: fixed-size slotted pages, append-only WAL with CRC32, a buffer
  pool with WAL-ahead flushing, fuzzy checkpoints with full-page images, and
  full ARIES crash recovery (Analysis → Redo → Undo).
- **Transactions**: 64-bit XIDs, disk-backed CLOG, pure-XID snapshots
  (xmin/xmax/xip + command id), Snapshot-Isolation MVCC, and a shared
  visibility oracle.
- **Locking**: row locks (via the tuple `t_xmax` protocol, `SELECT … FOR
  UPDATE` / `FOR SHARE`) and table locks, with a background deadlock detector
  (wait-for graph, youngest-victim selection).
- **Heap**: variable-length tuples, HOT (heap-only tuple) updates, and MVCC
  visibility.
- **B+Tree**: latch-coupling (crabbing) reads, optimistic + pessimistic writes,
  Blink sibling links, a crash-safe three-step split protocol, and
  CLR-based undo for incomplete splits.
- **Engine API**: `CREATE TABLE` / `CREATE INDEX`, `INSERT` / `SELECT` /
  `UPDATE` / `DELETE` — both as typed methods and via a SQL-string `exec(...)`;
  `DROP TABLE` as a typed method only (not yet in the SQL subset).
- **Observability (M3)**: the `QueryStats` ring buffer records only the SQL
  text path (`Engine::exec`); the typed API (`Engine::scan` / `insert` /
  `update` / `delete`, …) is deliberately NOT counted (tech-selection §6.3).
- **PG Wire (M3)**: a minimal PostgreSQL v3 wire-protocol server (`pg-server`)
  — Simple Query + text results, trust auth. `psql`, psycopg2, node-postgres
  and rust-postgres can connect and run CRUD + transactions. Extended Query /
  COPY / TLS land in Phase 4a/6.
- **Vacuum (M3)**: offline `Engine::vacuum(table)` — dead-tuple scan at a
  snapshot-registered horizon, index cleanup, in-page compaction and page
  freeing. Online/incremental vacuum is Phase 5b.

## Quick start

It's a library. Add it to your crate and drive it directly:

```rust
use std::path::Path;

use pg_engine::{Engine, EngineConfig};

fn main() -> pg_engine::Result<()> {
    let dir = Path::new("my_data_dir");
    let engine = Engine::open(dir, EngineConfig::new(dir))?;

    // DDL + DML through a minimal SQL string, or typed methods.
    engine.exec(None, "CREATE TABLE users (id INT, name TEXT)")?;
    engine.exec(None, "INSERT INTO users VALUES (1, 'alice')")?;

    // Typed API with index support.
    engine.create_index("users", "id")?;
    let rows = engine.scan("users", None)?; // Vec<(Tid, Vec<Option<Datum>>)>
    for (tid, values) in rows {
        println!("{tid:?} -> {values:?}");
    }

    // Multi-statement transaction with commit / abort.
    let txn = engine.begin_txn()?;
    engine.exec(Some(&txn), "UPDATE users SET name = 'bob' WHERE id = 1")?;
    txn.commit()?;

    Ok(())
}
```

Build and test the whole workspace:

```bash
cargo build --workspace
cargo test --workspace
```

**Requirements:** Rust 1.86+ (MSRV). The engine runs inside your process; an
optional `pg-server` binary (`cargo run -p pg-wire --bin pg-server -- <addr>
<data-dir>`) exposes the same engine over the PostgreSQL wire protocol for
`psql` / PG drivers.

## Roadmap & progress

The full plan (with rationale, time estimates, and risk register) is in
[ROADMAP.md](ROADMAP.md). Here it is at a glance, with where we are today:

| Phase | What it delivers | Status |
|-------|------------------|--------|
| **1 — Storage base + row store + tx + B+Tree** | Page / WAL / BufferPool (M1) · MVCC + crash recovery + locking (M2) · vacuum + observability + minimal PG Wire (M3) | ✅ done (tag `phase1-m3`) |
| **2 — HNSW vector index** | in-memory graph (2a) → WAL + persistence (2b) → concurrency control (2c) | 🚧 M4 (= 2a) underway: Stages A–C done, D closing, E in flight |
| **3 — Inverted index** | BM25 full-text, segment-based storage, merge | 📋 planned |
| **4 — SQL + multi-path fusion** | DataFusion + PG Wire extended (4a) → fusion planner (4b) | 📋 planned |
| **5a — Time-series + columnar** (parallel with 2/3/4a) | TTL partitions, columnar projection, distillation SDK stub | 📋 planned |
| **5b — Graph + GC + memory lifecycle** | lightweight graph AM, multi-AM GC, forgetting/layering SDK, fusion hooks | 📋 planned |
| **6 — Protocol + multi-agent isolation** | full PG protocol, MCP server, row-level security | 📋 planned |
| **7 — Production** | observability, compression, CBO, high availability | 📋 planned |

The current work is **Phase 2 M4**: the in-memory HNSW graph (insert /
search / neighborhood-selection heuristics), distance functions, and a
deterministic snapshot format — Stages A–C (crate foundations, the core
algorithm, snapshot save/load) are done. Stage D (siftsmall recall harness
+ A/B acceptance) is implemented and green locally but **not yet closed**:
two acceptance items remain externally gated (the CI ftp-reachability
probe verdict and the 1M sift/gist runs, both needing runner-side
network/data — see `docs/phase2-m4-benchmarks.md`). Stage E (criterion
benchmarks, coverage, M4 closeout) has started on local tasks and, per its
declared precondition, closes only after D does.

Per-stage design decisions and deviations from PostgreSQL are documented in
[docs/](docs/), particularly `docs/stage_spec.md` (what was actually built).

## Design principles

From [ROADMAP.md](ROADMAP.md):

1. Strict layering — build bottom-up, each stage independently deliverable.
2. **Correctness > performance > features.** Prefer one missing feature over
   one bug.
3. No skipped stages, no speculative generality — extensible interfaces,
   minimal implementations.
4. Every access method ships with its own vacuum/GC story.

## Testing & verification

Correctness is the priority, so the test surface is heavy:

- **884 tests** across the workspace, run in CI on both Linux and macOS.
- **Crash recovery**: `kill -9`-style round-trip tests that replay real WAL
  streams (checkpoint + split + HOT + lock combinations) and re-verify state.
- **`loom` model checking**: the B+Tree latch choreography is model-checked
  across interleavings (split model at full preemption bound; read
  linearizability model at bound 2), not just stress-tested.
- **`proptest`**: property-based tests for the page allocator and WAL record
  round-trips.
- **CI**: `rustfmt`, `clippy -D warnings`, MSRV check, per-crate test matrix,
  loom, and rustdoc-with-warnings-as-errors; plus a nightly criterion-bench
  job and a coverage job (tarpaulin).

## License

[Apache 2.0](LICENSE)
