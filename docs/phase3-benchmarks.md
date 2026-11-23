# Phase 3 benchmark evidence

## Environment

- Host: Apple M3 Max, aarch64-apple-darwin
- OS: Darwin 23.5.0
- Rust: 1.97.1 (8bab26f4f 2026-07-14)
- Tantivy: 0.25.0
- Stage D baseline commit: `f167f387eb01bf3b6525e2c97a66091aebfa95f8`

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

## Latency against Tantivy

```bash
cargo test -p pg-am-inverted --release --test performance -- --ignored --nocapture
```

Result: **PASS** — WAND mean latency is below the 3× Tantivy budget.

| Engine | Mean | p95 |
|---|---:|---:|
| Tantivy | 32.322µs | 64.792µs |
| DAAT | 29.641µs | 95.750µs |
| WAND | 32.281µs | 62.917µs |

Ratios: WAND/Tantivy mean **0.999**, p95 **0.971**. The sample contains 100 qrel-covered claims, five iterations each, over the vendored 5,183-document corpus.

## Tier 2 load

The Tier 2 workload is defined in `crates/pg-engine/tests/phase3_tier2.rs`:

- 10,000 documents, including a >8 KiB TOAST sample every 100 commits
- 16 writer clients, 6,000 measured commits
- 100 additional high/mid/low/empty-result queries
- merge enabled and disabled variants
- `commit_finished_at → visible_at` latency samples

Results: **FAIL — the p95 < 1s target is not met.**

The current runs use 25 batches of 15 commits per client. Each batch's last visible commit verifies the whole preceding prefix, avoiding an OR-of-15 query while preserving the commit-to-visible evidence.

| Variant | Elapsed | Effective commits/s | commit_to_visible p95 |
|---|---:|---:|---:|
| merge enabled | 413.842836291s | 14.50 | 26.517700958s |
| merge disabled | 805.423736959s | 7.45 | 23.032686584s |

Both variants have 6,000 valid visibility samples. The dominant cost is the concurrent refresh/visibility path, not merge itself: merge-disabled is slower, not faster. This remains an open performance blocker; Stage E is not accepted as complete.
