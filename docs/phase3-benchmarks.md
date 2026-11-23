# Phase 3 benchmark evidence

## Environment

- Host: Apple M3 Max, aarch64-apple-darwin
- OS: Darwin 23.5.0
- Rust: 1.97.1 (8bab26f4f 2026-07-14)
- Tantivy: 0.25.0
- Measurement baseline: `2f9c939` (+ current worktree fixes)

## Assets

- jieba dictionary: `fxsjy/jieba@67fa2e36e72f69d9134b8a1037b83fbb070b9775`
- Dictionary SHA256: `7197c3211ddd98962b036cdf40324d1ea2bfaa12bd028e68faa70111a88e12a8`
- SciFact corpus SHA256: `f0d32db0d156b526d75921ed7a76f2cb912902631c87248c5c97c617bad0b60c`
- SciFact queries SHA256: `9db7df096f7414435d52bafcbacf814c30cea50eb565c1e8fa6d11440759bba8`
- SciFact qrels SHA256: `0864bb985e0ca2367ba217977e72004d549054b2b06666ed9d4825ac7c21284c`

Provenance is recorded in `crates/pg-am-inverted/dict/README.md` and `datasets/scifact/README.md`.

## WAND correctness and pruning

```bash
cargo test -p pg-am-inverted wand::tests
```

Result: 4 tests passed. WAND and DAAT matched on AND/OR, empty, duplicate, tied, high-df, and generated Boolean corpora. The upper-bound test evaluated only the winning document (1 scoring call), confirming pruning.

Engine integration regression:

```bash
cargo test -p pg-engine --lib inverted_wand_top_k_repairs_visibility_filtered_candidates
```

Result: passed. A deleted high-scoring leader is removed by MVCC, and a full DAAT repair restores the correct visible top-2 instead of returning a short page.

## SciFact quality

```bash
cargo test -p pg-am-inverted --test quality -- --ignored --nocapture
```

Result:

- 300 evaluated claims with qrels
- Mean absolute NDCG@10 difference: **0.00171049**
- Acceptance threshold: 0.005 — **PASS**
- Worst query difference: query `1359`, absolute difference `0.36907024642854247`; query `179`, difference `0.1440776612513717`

Both indexes used the same document text, `en_stem` / `mixed` analysis, and OR-of-AND query semantics.

This is a **retrieval-quality comparability** gate, not a scorer-equivalence gate. Our analyzer removes stopwords before computing `doc_len`, while Tantivy's `en_stem` path retains them in `fieldnorm`; Tantivy's length normalization therefore includes stopwords that ours excludes. This systematic asymmetry affects ranking: query `1359`, for example, scores 1.0 with our index versus 0.6309297535714575 with Tantivy. The ±0.005 result must be interpreted as comparable retrieval quality under this fieldnorm difference, not as identical BM25 scoring semantics.

## Latency against Tantivy

```bash
cargo test -p pg-am-inverted --release --test performance -- --ignored --nocapture
```

Result: **PASS** — WAND mean latency is below the 3× Tantivy budget.

| Engine | Mean | p95 |
|---|---:|---:|
| Tantivy | 36.340µs | 79.959µs |
| DAAT | 45.404µs | 120.000µs |
| WAND | 45.146µs | 122.500µs |

Ratios: WAND/Tantivy mean **1.242**, p95 **1.532**. The sample contains 100 qrel-covered claims, five iterations each, over the vendored 5,183-document corpus. These are post-cache-fix measurements.

## Tier 2 load

The Tier 2 workload is defined in `crates/pg-engine/tests/phase3_tier2.rs`:

- 10,000 documents, including a >8 KiB TOAST sample every 100 commits
- 16 writer clients, 6,000 measured commits
- 100 additional high/mid/low/empty-result queries
- merge enabled and disabled variants
- `commit_finished_at → visible_at` latency samples

Results after the analyzer-cache fix: **PASS — both variants meet the p95 < 1s target.**

The current runs use 25 batches of 15 commits per client. Each batch's last visible commit verifies the whole preceding prefix, avoiding an OR-of-15 query while preserving the commit-to-visible evidence.

| Variant | Elapsed | Effective commits/s | commit_to_visible p95 |
|---|---:|---:|---:|
| merge enabled | 60.005542625s | 99.99 | 496.013458ms |
| merge disabled | 60.005421500s | 99.99 | 270.778458ms |

Both variants have 6,000 valid visibility samples. The earlier 26.5177s/23.0327s p95 figures were polluted by repeated dictionary SHA256 computation and per-seal dictionary parsing, and are superseded by these runs. The 60-second elapsed floor is the workload's offered-rate schedule. These clean results support retaining the synchronous consumer; no background consumer is introduced.

## Current product decision (2026-11-23)

The primary deployment shape is **one agent with low-frequency writes**. The 16-client/high-rate workload is retained as a stress reference, not as a current product acceptance blocker. The synchronous consumer remains the production choice; `allow_stale` is not used as a performance switch. The cache-fix rerun also meets the stress workload's original p95 target, so this decision no longer relies on the polluted failure measurements.

A future high-concurrency option is recorded without implementation: one single-writer background consumer per index, Condvar-based freshness waits, bounded timeout with exact SeqScan fallback, DROP lifecycle handling, dynamic backoff, pending limits, and deterministic recovery tests. It will be implemented only if multi-agent, high-rate writes become a real requirement.
