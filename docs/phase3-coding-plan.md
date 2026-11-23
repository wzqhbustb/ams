# Phase 3 Coding Plan：全文倒排索引（BM25 / Segment / Tier 2）

版本：v1.2（2026-11-17）
依据：`docs/phase3-tech-selection.md` v1.2（含 TOAST 消费、SegmentManifest、`ExtraWhere`、参数化查询与偏离登记）
前置：Phase 1/2 已交付；Phase 4a 已完成并合入。Phase 2 的 24h 压测、CI coverage、`phase2-m6` tag 并行推进，不阻塞本计划。

---

## 硬约束（贯穿所有 Stage，违反即返工）

1. **heap + WAL 是唯一事实源**：segment 是可重建的二级结构，不得成为独立写入真相；查询候选最终必须经 heap MVCC 可见性复核。
2. **AM 注册表可插拔**：新增 `AmKind::Inverted`、注册项、AST 与 `InvertedScanSpec` 即可，禁止修改 selector/executor 的核心分派结构。
3. **Commit 才能进入倒排**：WAL consumer 按 XID 暂存变更，Commit 按原顺序应用，Abort 丢弃；绝不允许 in-flight 版本进入 write buffer。`HeapUpdate` 必须**先删除 old_tid 的 posting，再插入 new_tid/new_tuple 的 posting**，不得只按新行追加；TOAST 复用旧 group 时删除旧 posting 不需要 detoast。
4. **快照与 Guard 不可分割**：所有读路径使用真实 `(Snapshot, SnapshotGuard)`，禁止 `Snapshot::everything()`；Guard 覆盖 WAND、merge stream、TOAST 组包、派生任务与 heap 回表。
5. **TOAST 语义唯一**：20B pointer、2000B chunk 上限、同 XID 且 chunk 先于主行写，全部复用 Stage D 的组包/校验 helper；新 group 缺块或共享旧 group 损坏均为 `Corrupted` 并停水位，不得跳过。
6. **backfill 使用 `AccessExclusive`**：锁覆盖快照、初始 segment、初始 manifest 和 consumer 起点，禁止 `AccessShare` 或按 entry 去重掩盖并发重复基线。
7. **merge 以 `SegmentManifest` 原子切换读集合**：target 与 inputs 不得同时可见；只允许完整旧 generation 或完整新 generation。
8. **回退必须精确**：`ExtraWhere` 一律 SeqScan + 普通过滤 + BM25 精确评分/排序，禁止倒排候选后 post-filter；无 LIMIT 时不静默截断，超时只报错、不采样。
9. **统计按 segment 保存并汇总**：checkpoint 只作优化，不作为全局统计事实源；merge 间历史统计窗口必须显式记录，不能宣称始终精确。
10. **WAL 与 SegmentId 契约冻结**：只复用 `SegmentSeal=110`、`SegmentMerge=111`；`LogicalInverted=101` 保留，SegmentId 在索引实例内不复用。
11. **参数化必须走类型化绑定**：`content @@ $1` 接受 `Literal::Param`，推断为 TEXT，经 `exec_with_params`/pg-wire Bind 后使用同一 Analyzer；禁止 SQL 文本替换。
12. **Tantivy 只作对照**：生产路径不嵌入 Tantivy，也不新增异步 runtime；基准版本与 lockfile 固定，不得无约束升级。
13. **WAND 不得牺牲正确性**：任何 upper-bound 剪枝必须与 DAAT 全量结果、分数及 tie-break 完全一致；同分不得跳过。
14. **测试先行**：Analyzer、posting、segment、WAL/Tier 2、`Bm25Score` AST、参数化与 `ExtraWhere` 的 golden tests 必须先于对应实现。

### 对 ROADMAP 原文的有意修订（偏离登记）

1. **全局统计持久化**：ROADMAP 原文要求 `doc_count/avgdl/term_df` 在 checkpoint metadata file 中维护；本计划改为**按 segment 保存并汇总**，checkpoint 只作恢复优化。理由是避免“统计已更新、行未提交”的分布式事务窗口，segment 统计与对应版本同生同灭，崩溃和 merge 均可重算验证。
2. **大表回退限制**：ROADMAP 原文为 `timeout/sampling limits`；本计划只保留 timeout，**拒绝 sampling**。采样会静默改变结果集与 BM25 排名，破坏与索引路径的语义一致性；超时后以结构化错误返回，不得用近似结果冒充精确回退。

---

## Stage 0：格式冻结、crate 骨架、AST 与 CI（1–2 天）

| 任务 | 交付物 |
|---|---|
| crate 与依赖注册 | 新建 `crates/pg-am-inverted`，加入 workspace members；模块骨架 `error.rs`、`analyzer.rs`、`posting.rs`、`segment.rs`、`bm25.rs`、`wand.rs`、`tier2.rs`、`index.rs`、`recovery.rs`。不依赖 `pg-engine` 或 DataFusion |
| 依赖锁定 | `jieba-rs = { version = "=0.10.1", default-features = false }`；build dependency `jieba-macros = "=0.10.1"`；`rust-stemmers = "1.2.0"`。Tantivy 仅 benchmark：`0.25.0`，并锁定 `time=0.3.45`、`uuid=1.24.0`、`bon=3.3.1`、`darling=0.20.11` |
| SQL AST | 新增 inverted `CreateIndex` 方法与可选 `fulltext_index='bm25'`；新增 `SelectItem::Bm25Score`、`OrderByKind::Bm25Score`、`InvertedScanSpec`、`ExtraWhere`；`Literal::Param` 对 `@@` 推断为 TEXT。`AS score` 只作输出标签，ORDER BY 直接重复评分表达式，不引入 alias 解析 |
| stale 配置 | 在 `EngineConfig` 增加 `allow_stale_inverted: bool`，默认 `false`（即 `require_fresh`）。只允许 engine 配置级开关，不引入 SQL 语法；`allow_stale` 时计划必须标注 `StaleIndex` |
| pg-wire 投影 | `SelectItem::Bm25Score` 的 statement/portal `RowDescription` 静态返回 FLOAT8（OID 701，与距离列同型）；Stage 0 以静态字段描述和手工结果形状覆盖 Simple/Extended 编码的空/非空形态，真实查询端到端空结果与非空结果验收归 Stage B（此时执行尚未实现） |
| catalog/AM 注册 | 固定 `INVERTED_AM_OID = 9879`（紧随 HNSW 的 9878）并注册 `pg_class.relam`；`build_index_registry` 支持 inverted 的 analyzer 配置、watermark 与 orphan 过滤，DROP TABLE 不重建。`Engine::vacuum` 对 inverted 只走 segment merge GC，禁止按 B+Tree 清理 posting；旧目录无该 OID 时按未知 AM 处理而非误认 B+Tree |
| 格式冻结 | `Token { text: String, position: u32 }`、posting、doc-length、`SegmentMeta`、`SegmentManifest { generation, active_segment_ids, applied_lsn }`、SegmentId 与 110/111 payload 契约；二进制 magic/version/CRC 与长度上限 |
| AM 方法契约 | `build` 初始化 SegmentStore；独立 `search` 查询；`insert/delete/scan` 显式返回 `Unsupported`，禁止空 Vec/空 Ok/伪造 heap tuple |
| 参数与回退 | `content @@ $1`、`content @@ 'x' AND id > 5` 的解析和 planner 负例；`ExtraWhere` 固定为 SeqScan，未知 AM/fulltext_index 响亮拒绝 |
| CI 注册 | 新 crate 加入 clippy/test/doc crate matrix、fmt、MSRV workspace check；`pg-am-inverted` 无 loom feature，归入 **all-features test 分支**，不使用 pg-storage/pg-am-btree/pg-am-hnsw 的 default-feature 例外；覆盖率若需要必须另建 pg-am-inverted 专用 job，不能沿用 pg-am-hnsw 数字 |
| Golden tests | Analyzer token、posting/segment bytes、SegmentManifest、AST、参数类型、ExtraWhere、未知 AM、非法版本/长度/CRC |

**验收**：
- `cargo test -p pg-am-inverted --lib`
- `cargo test -p pg-engine --lib`
- `cargo +1.86.0 check --workspace --all-features`

本阶段只冻结格式与 AST，不得提前实现倒排查询或让 `@@` 静默等价于字符串相等。

---

## Stage A：Analyzer、posting、BM25 与 DAAT（2–3 天）

| 任务 | 交付物 |
|---|---|
| 分析器 | `Analyzer` trait 与 `Token { text, position }`；英文 Snowball/stop-word、中文 jieba、混合文本处理；建索引与查询使用同一实例、词典和配置 hash |
| 中文词典 | `Jieba::empty()` + `load_dict(BufRead)`；词典作为带版本/SHA256 的 vendored 数据资产，禁止运行时下载 |
| Posting/统计 | 有序 `(term, tid, position)` posting、doc-length、segment-local `doc_count/total_tokens/term_df`；有界编码与损坏检测 |
| BM25 | `k1=1.2`、`b=0.75`、标准 idf；f64 计算，零文档/零 df/零 avgdl 安全处理；同分 `(score DESC, tid ASC)` |
| DAAT | 单 term scan、多 term AND 交集、OR 全量基线；top-k 有界堆，超大 k 不按输入直接预分配 |
| 测试 | 中英文/混合 token、position、词典 hash、BM25 对 Tantivy `bm25.rs` 已知答案、重复 term、长短文档、零文档/零 df、损坏 posting/segment、LIMIT 0/1/k/超大 k |

**验收**：内存模型的 Analyzer、posting、BM25 与 DAAT 结果正确；Tantivy 已知答案逐项一致；所有损坏输入结构化报错，无 panic。

**实现补充**：全局统计只在 segment 内维护，后续查询从 active manifest 的 segment 汇总；不创建全局计数文件。首版不实现 phrase、prefix、fuzzy、括号优先级或完整 query parser。

---

## Stage B：单 segment 持久化、backfill、查询与 EXPLAIN（2–3 天）

| 任务 | 交付物 |
|---|---|
| SegmentStore | write buffer → Frozen → Sealed 的单 segment 写路径；版本化二进制、metadata、doc-length、posting blocks、CRC；临时文件 fsync → rename → 父目录 fsync。`seal()` 同时发射 `SegmentSeal=110` 并注册幂等 redo handler，不能只做文件状态迁移 |
| 初始 manifest | `generation=1`、active IDs、初始 `durable_applied_lsn`；查询固定 generation，不遍历磁盘所有 sealed 文件 |
| backfill | **全程 `AccessExclusive`**：取锁后重新取快照与 `base_lsn=synced_lsn()`，扫描并构建初始 segment，发布初始 manifest 和 consumer 起点，再从 `successor(base_lsn)` 消费；大表增量 backfill 不在本阶段实现。**空 WAL 时从第一条 WAL 记录开始**，successor 必须按 LSN 对齐规则计算，不能产生非法 LSN |
| registry/回滚 | 镜像 Phase 4a `create_hnsw_index` 的审查结论：registry/handle 在 `auto_commit_with_hooks` 成功钩子里、commit 后且锁释放前发布；op Err 或 commit Err 在锁释放前回滚，禁止幽灵 registry/consumer。backfill 的 `AccessExclusive` 与 registry 回滚必须覆盖并发 waiter，commit failure 后可重试 |
| AM 与查询 | `build/search/freshness`；`insert/delete/scan` 显式 `Unsupported`；单 segment DAAT + BM25 + heap 回表，LIMIT/top-k 与无索引回退 |
| TOAST helper | 将 Stage D 的组包与校验提炼为 pg-am-heap 可复用 helper；backfill 以真实快照 detoast，不使用 physical detoast 或 `Snapshot::everything()` |
| EXPLAIN | `InvertedScan` 输出 index/analyzer/limit/consumed_lsn/snapshot_frontier/plan；回退输出 `NoIndex`、`ExtraWhere`、`UnsupportedQuery` 等 |
| 测试 | 空表/非空表 backfill、空表+空 WAL、首次 DML 与 reopen、并发 DML 锁互斥、结果不重复、单 segment 查询、TOAST 多 chunk、旧目录无索引、文件损坏/rename 窗口、重复 seal、参数化 `@@ $1`、额外谓词回退、backfill commit failure 后并发 waiter 无幽灵 registry/consumer 且可重试 |

**验收**：单 segment 可持久化并 reopen；backfill 与并发 DML 不产生重复行或重复文档统计；`ExtraWhere` 结果与精确 SeqScan 一致；坏 magic/version/长度/CRC 均为 `Corrupted`。

**实现补充**：初始 backfill 的写停止窗口是首版明确接受的限制，必须记录到基准；不得用 per-entry 去重掩盖 backfill 与 WAL 的竞态。

---

## Stage C：Tier 2 consumer、TOAST 消费与 watermark（3–4 天）

| 任务 | 交付物 |
|---|---|
| WAL consumer | 实现 `WalTailReader`：只读已 flush 记录、拉取式背压、跨段续传；per-XID pending 有界，Commit 按序应用，Abort 丢弃。**HeapUpdate 必须先删 old_tid posting 再插入 new_tid/new_tuple posting**，TOAST 复用旧 group 删除旧 posting 不需要 detoast |
| 水位 | 区分 `consumed_lsn`（进程内查询新鲜度）与 `durable_applied_lsn`（durable seal 后恢复起点）；`WatermarkRegistry` 单调更新，stale worker 不得回写 |
| TOAST 新 group | pending 中同 XID chunk 按 seq 组包，校验 relation/rawsize/extsize/连续 seq/≤2000B/总大小/UTF-8；任何损坏都 `Corrupted` 停水位 |
| TOAST 共享旧 group | pending 无该 valueid 时，用 commit 后新 snapshot + Guard 读取 committed chunk 并复用同一 helper；禁止 physical detoast 或 `Snapshot::everything()` |
| 新鲜度与回退 | `snapshot_frontier = wal_writer.synced_lsn()`；不足时默认 `require_fresh` 回退，允许显式 `allow_stale` 只读 sealed 并标注 `StaleIndex` |
| lag 与观测 | 暴露 `wal_lag_records/bytes`、`applied_lsn`、`synced_lsn`、`last_progress_at`、`watermark_lag_seconds=max(0, now-last_progress_at)`；落后时缩短退避，不 busy-loop |
| p95 观测 | 新增 doc-hidden `commit_finished_at` 测试 hook 记录 XID commit 完成时间；consumer 侧记录 `visible_at`，用二者差值衡量真实提交可见延迟 |
| 测试 | Commit/Abort/in-flight、跨 WAL 段、checkpoint 前后、pending 溢出、TOAST 新/共享 group、缺/重/乱序/尺寸错误/非法 UTF-8、stale worker、旧快照回退、Guard × vacuum；UPDATE 后仅一行、旧 term 消失、新 term 可检索且统计不重复；DELETE 后候选被剔除、tombstone 与统计窗口明确 |

**验收**：
- Abort 不入库、in-flight 不入库；重复消费不重复，崩溃恢复不丢失。
- TOAST 损坏均不推进水位；普通字段与 >8KiB 多 chunk 文本均可检索。
- 固定负载下已提交行 **p95 < 1s 可搜索**，使用 `commit_finished_at → visible_at` 样本，而不是 consumer 首次看到 WAL 的时间。

**实现补充**：大事务 pending 优先使用有界临时存储；未实现前必须暂停并响亮报错，不能把未提交版本提前发布。`allow_stale` 只能由调用方显式选择，默认始终是 `require_fresh`。

---

## Stage D：k-way merge、物理删除与 manifest 原子替换（2–3 天）

| 任务 | 交付物 |
|---|---|
| k-way merge | 按 `(term, tid, position)` 流式合并，不全量载入内存；重复 redo entry 按完整键去重，正常 UPDATE 保留不同 TID |
| 物理删除 | tombstone 从 merge 输出中丢弃；统计汇总与重算交叉校验；merge 间历史统计窗口在 EXPLAIN/文档中可观察 |
| SegmentManifest | target durable 后先不发布；以 `merged_applied_lsn = max(inputs.applied_lsn)` 计算水位，空 inputs 保持原值，禁止用 merge 完成时的 `synced_lsn()` 跳过新 DML。manifest 使用 `manifest.tmp → fsync → 原子 rename → 父目录 fsync`：一次替换安装 target、移除 inputs、`generation + 1`，替换前读者只见旧 generation，替换后只见新 generation；替换失败旧 manifest 继续有效 |
| 生命周期与 redo | 文件层输入保持 `Sealed`，manifest 原子切换时逻辑退休 inputs；`SegmentSeal=110` 在 seal 时发射且幂等，`SegmentMerge=111` 携带有序 inputs + target，在 target 与 manifest durable 后发射；redo 严格校验 payload 后为幂等 no-op，发布失败时 inputs 保持 Sealed 且仍在 active 读集合 |
| 并发与清理 | 单 consumer 与 merge worker 协调 manifest；禁止并发 seal 同 ID 或并发覆盖 generation/applied_lsn；未发布目标按临时文件清理 |
| 测试 | merge 前后结果和 tie-break 一致、并发 reader×merge×DML、统计一致、target durable 但 manifest 未替换、替换完成、manifest 撕裂/替换失败、重复 seal/merge、SegmentId 不复用；DELETE tombstone 在 merge 输出中物理清除且统计重算正确 |

**验收**：merge 不丢行、不重复、不混代；任何崩溃窗口只见完整旧或新 generation；失败 merge 不产生可见半成品。

**实现补充**：manifest 替换是唯一读集合切换点；target 与 manifest durable 后才追加 `SegmentMerge`，redo 不重复切换。不允许先改 manifest 再写 target，也不允许先 retire inputs 再 durable target。冻结的 111 payload 不含 index OID，不能独立定位 manifest，因此不得把 redo 设计成重新发布 generation。

---

## Stage E：WAND、全 crash matrix 与检索基准（3–4 天）

| 任务 | 交付物 |
|---|---|
| WAND | term upper-bound、pivot、候选维护与 top-k；上界只能使用安全 BM25 max_score；`score_upper_bound < 当前第 k 名` 才允许跳过，等于时保留并执行 tie-break |
| 正确性对拍 | WAND 与 DAAT 在单 term/AND/OR、空结果、重复 term、并列分数、LIMIT 0/1/k/超大 k 下逐项相同；无 LIMIT 时全量精确评估 |
| 全 crash matrix | WAL commit 前后、消费前后、seal rename 前后、manifest 替换前后、pending 溢出、TOAST 缺块、重复 open/reopen、DROP 后重建 |
| 并发验收 | reader×merge×DML、旧快照 vacuum、TOAST 共享旧 group、consumer 与 stale worker、merge 与查询 Guard 生命周期 |
| BEIR/中文质量 | BEIR SciFact + 冻结中文/混合文本集，同一 Analyzer 和固定 flush+merge 统计截面；NDCG@10 与 Tantivy 差异目标 **±0.005** |
| 性能基准 | 相对 Tantivy 延迟 **不超过 3×** 并记录绝对延迟；WAND/DAAT 结果一致后才比较性能；记录硬件、OS、Rust、数据/查询 hash、缓存口径 |
| Tier 2 负载 | release、10k docs、16 clients、100 commits/s、60s/≥1000 commit 样本、100 个高中低频/空结果查询；普通 1KiB 档与 TOAST 档（每批至少一个 >8KiB 样本）均覆盖 merge 开/关 |

**验收**：
- 全 crash matrix 无重复、无丢失、无半成品；WAND 与 DAAT 结果完全一致。
- NDCG@10 达目标，Tier 2 p95 < 1s，Tantivy 对比延迟 ≤3×；所有原始结果和命令写入 `docs/phase3-benchmarks.md`。

---

## Stage F：全量门禁、两轮对抗审查与归档（1–2 天）

| 任务 | 交付物 |
|---|---|
| 全量回归 | workspace 默认 feature debug + release；新 crate debug + release；保留既有 TP/ANN/Wire/TOAST/DataFusion 全部回归，不发明不受支持的 workspace all-features test 口径 |
| 静态门禁 | `cargo check --workspace --all-features`、MSRV 1.86 实编译、逐 crate Clippy/rustdoc、fmt、`git diff --check` |
| 对抗审查 ×2 | 第一轮实现审查 → 修复 → 第二轮复核；重点为 backfill、pending、TOAST、watermark、manifest、旧快照、WAND upper-bound 与 crash |
| 覆盖率 | 若新增覆盖率，必须另注册 pg-am-inverted 专用 job；不得沿用 pg-am-hnsw 的 90% 结果冒充本 crate 覆盖率 |
| 归档 | `docs/stage_spec.md` 增加 Phase 3 各节（交付内容/关键约束/审查回流/验证数字）+ `docs/phase3-benchmarks.md` + 本计划的最终实现补充 |

**验收命令（全阶段收口）**：

```bash
cargo test --workspace
cargo test --workspace --release
cargo test -p pg-am-inverted
cargo test -p pg-am-inverted --release
cargo test -p pg-engine --features datafusion
cargo test -p pg-engine --features datafusion --release
cargo check --workspace --all-features
for crate in pg-storage pg-txn pg-catalog pg-am-heap pg-am-btree pg-am-hnsw pg-am-inverted pg-engine pg-wire; do
  cargo clippy -p "$crate" --all-targets --all-features -- -D warnings || exit 1
done
cargo fmt --all -- --check
for crate in pg-storage pg-txn pg-catalog pg-am-heap pg-am-btree pg-am-hnsw pg-am-inverted pg-engine pg-wire; do
  RUSTDOCFLAGS="-D warnings" cargo doc -p "$crate" --all-features --no-deps || exit 1
done
CARGO_TARGET_DIR=target/msrv-phase3 cargo +1.86.0 check --workspace --all-features
```

---

## CI 注册清单

| # | 内容 | workflow/job | 运行条件 |
|---|------|-------------|---------|
| 1 | crate 骨架、AST、格式 golden tests（Stage 0） | ci.yml 新增 pg-am-inverted matrix + pg-engine 既有 matrix | 每 push |
| 2 | Analyzer/posting/BM25/DAAT（Stage A） | pg-am-inverted test matrix | 每 push |
| 3 | 单 segment、backfill、查询与 EXPLAIN（Stage B） | 同上 | 每 push |
| 4 | Tier 2/TOAST consumer、watermark、crash（Stage C） | 单元每 push + crash rounds 手动/nightly | 每 push + 手动 |
| 5 | k-way merge、manifest、物理删除（Stage D） | 单元每 push + 并发回归 | 每 push |
| 6 | WAND、全 crash matrix、BEIR/性能基准（Stage E） | 单元每 push + 手动 release benchmark/nightly | 每 push + 手动 |
| 7 | 全量回归与两轮对抗审查（Stage F） | 全 workspace debug/release + MSRV + 静态门禁 | Stage F 收口时 |
| 8 | 覆盖率 | 独立 pg-am-inverted coverage job（若注册） | 每 push；不得复用 pg-am-hnsw 结果 |

---

## 风险与开放问题（登记，不阻塞）

- **大事务 pending**：有界临时存储未落地前必须暂停消费并响亮报错，不能提前发布未提交版本。
- **backfill 写停止**：首版用 `AccessExclusive` 换取简单且可证明的基线一致性；后续在线 backfill 优先评估 per-TID upsert，而不是按 entry 去重。
- **统计截面**：merge 间历史统计会影响排序，必须可观察；若业务要求更强一致性，后续引入按 LSN 的统计增量。
- **旧快照**：首版只允许当前快照走倒排，历史 posting 版本化是独立工作。
- **WAND upper-bound**：错误上界会直接漏行，必须 DAAT 全量对拍，尤其同分与非有限值。
- **词典/分析器升级**：配置 hash 变化必须显式重建，不能在线混用。
- **英文 CJK 与日韩/西里尔文本**：`english` 将连续 CJK 段视为不透明 token；Kana/Hangul/西里尔进入 Latin 分支且 stemming 近似空转，日韩及混合语种召回属于当前非目标，不能按英文检索质量验收。
- **小词典与 OOV 回退**：`custom.dict` 当前仅覆盖 5 个词，词典外中文可能退回单字切分并放大 df；已补回退行为测试，Stage E 前必须替换为真实、带版本的词典资产并重跑中文质量基准。
- **position 语义**：Latin stop-word 留位置空洞，CJK 空 token 不占位；未来 phrase 查询必须显式登记并测试该差异。
- **Analyzer 失配 fail-stop**：任一索引的 analyzer hash 失配会阻止整个 Engine 打开，当前选择是显式重建；若未来需要可用性隔离，应另评估“隔离该索引 + require_fresh 回退”，不得静默换分析器。
- **DAAT 性能债**：当前实现包含 per-term `BTreeSet` 分配和候选×term map 查找，Stage E 需要真流式 DAAT 基线与 WAND 对照；性能优化只能在正确性证据之后进行。
- **Posting 写入校验**：`Posting::validate` 已抽出为公共函数，Stage B 持久化前必须调用，不能只依赖 decode 时发现损坏。
- **Tantivy 版本漂移**：基准版本和 lockfile 一并归档，禁止无约束升级造成 MSRV 或质量数字失真。
- **seal/merge 内存、容量与读者延迟**：posting 已按当前 term 流式合并，但 doc-length、term_df 和 TID 版本所有权表仍驻留内存，冻结的 16MiB frame 上限同时约束 delta seal 与单段 merge。seal 与 merge 分别触发进程级熔断：merge 失败后继续使用多段，seal 失败后以完整内存视图服务并保留 durable 水位差距，均记录警告；多 delta 拆分、多 target 输出或 per-term 帧格式根治归 Stage E+。`pending_documents` 尚无独立字节上限，大批量窗口可能持续增长，需后续在 apply 侧限流。服务与 reopen 仍是 O(corpus) 全量内存视图，“bounded memory”仅描述 merge 文件处理；大 segment 的 I/O、内存与页缓存污染归 Stage E 测量。
- **退休段与孤儿清理**：成功 merge 后清理为 best-effort；reopen 只回收“不在 active 且 ID 小于 active 高水位”的文件，更新 orphan 必须为 SegmentId 防复用而保留。I/O 失败或反复容量失败仍可能留下不可见垃圾，不影响正确性；严格磁盘配额需要后续独立 GC 指标与清理重试。
- **统计截面可观察性**：EXPLAIN 尚未暴露 generation/active 段集合，当前只通过文档登记 merge 间统计窗口，属于部分满足；后续应在 `InvertedScan` 增加 generation 与段数。
- **stale 读配置**：`allow_stale` 是有意放宽，默认 `require_fresh` 不得被配置静默覆盖。
- **TOAST 组包与复用 group**：新 group 缺块与共享旧 group 损坏只能 `Corrupted` 停水位，不得通过忽略 chunk 提高新鲜度。
- **M6 收口项并行**：24h 复跑/CI 报告/tag 与本计划无依赖，不阻塞各 Stage。

---

## 修订记录

| 版本 | 日期 | 内容 |
|---|---|---|
| v1.0 | 2026-11-17 | 初稿：依据技术选型 v1.2，拆出 Stage 0–F 的交付物、验收、CI、依赖锁定和两轮审查；覆盖 backfill、manifest、TOAST、Tier 2、参数化、WAND、merge/crash 与性能基准。 |
| v1.1 | 2026-11-17 | 审查修复：`HeapUpdate` 明确先删旧 posting 再插新 posting；补 `INVERTED_AM_OID`、catalog/reopen/vacuum 路径；merge 水位改为 `max(inputs.applied_lsn)`，manifest 增加 tmp+fsync+rename 持久化；补 `SegmentSeal=110` 发射/redo、空 WAL 起点和 DataFusion debug/release 验收命令。 |
| v1.2 | 2026-11-17 | 二审修复：`EngineConfig.allow_stale_inverted` 默认 false 且只作 engine 配置；补 pg-wire `Bm25Score` FLOAT8(701) RowDescription；backfill registry/rollback 镜像 create_hnsw_index 锁内发布与失败清理；补 DELETE tombstone 统计窗口与 merge 物理清除测试；`INVERTED_AM_OID=9879`；pg-am-inverted 归 all-features test 分支。 |
| v1.3 | 2026-11-17 | Stage 0 审查回流：`@@` 非 TEXT 列在参数推断期拒绝且 RHS 固定 TEXT；stale 配置传入 planner；registry/planner 共享 analyzer/watermark 并校验 pg_am；补 doc-length、Posting/SegmentMeta/SegmentManifest 精确字节 golden；Wire 验收明确 Stage 0 静态编码与 Stage B 真实端到端结果的分工。 |
| v1.4 | 2026-11-18 | 二审深度修复：DML/loser compensation/typed lookup 全面禁止把 inverted 当 B+Tree；reopen 改为读取真实 manifest 和 active segment metadata，校验 analyzer hash，禁止伪造零水位；planner 补 `index_oid`，pg_am 同时校验 OID 与 `amname`。 |
| v1.5 | 2026-11-18 | 三审修复：bincode 解码增加嵌套读取/分配上限；manifest 水位必须等于 active 最大覆盖且允许零水位空集合；freshness 返回 consumed；`SegmentMeta` 补 `row_count`，Posting 校验频次和 position 严格递增；修复 BM25 alias 类型优先级及普通 `bm25_score` 列名兼容。 |
| v1.6 | 2026-11-18 | Stage A 实现：`DefaultAnalyzer` 英文 Snowball、jieba 中文与混合分析链，vendored `custom.dict` 与真实 SHA256 配置身份；新增 `InMemoryIndex`、精确 DAAT AND/OR、BM25 和有界 top-k；使用 Tantivy `Bm25Weight` 对拍并覆盖零文档、重复 term、并列分数及超大 LIMIT。 |
| v1.7 | 2026-11-18 | Stage A 审查回流：`InMemoryIndex<A: Analyzer>` 暴露索引持有的 Analyzer，查询必须复用该实例；`EngineConfig.inverted_analyzer` 支持 english/chinese/mixed 精确匹配 reopen；hash 纳入 `ANALYZER_FORMAT_VERSION`；补 `term_df`，不可能的 BM25 统计与缺失 doc-length 改为 `Corrupted`。 |
| v1.8 | 2026-11-18 | 深度审查修复：active manifest 拒绝 `Merging`/`Retired`；`insert_document` 拒绝无效 TID、空 token 和非递增 position；reopen 校验 `row_count/doc_count/posting_count/total_tokens/term_df` 一致性，避免生命周期与统计损坏进入查询。 |
| v1.9 | 2026-11-18 | 审查修复：`posting_count` 统一为 `(term,doc)` 记录数，reopen 改为 `term_df.len() <= posting_count <= total_tokens` 并补 golden 形状正例；OR 全查询去重保证幂等；存储 I/O 错误保留 Storage 变体；AnalyzerConfig 校验真实 hash，english hash 不再耦合中文词典；`or` 支持大小写；补全所有 reopen 分支测试并登记 CJK、词典、position、fail-stop 和 DAAT 性能风险。 |
| v1.10 | 2026-11-18 | 二轮审查修复：`insert_document` 增加 token 数量上限、u32 转换检查及先算后写，杜绝截断和失败残留；Stage 0 scratch probe 转为具体 AST/错误变体回归；精确钉死 jieba OOV 单字回退，并修正去重测试命名。 |
| v1.11 | 2026-11-18 | Stage B 实现：单 segment 原子持久化、空/非空 backfill、seal WAL/redo、锁内成功发布与失败回滚；接入精确 DAAT/BM25、heap MVCC 回表、TOAST、多谓词回退和 EXPLAIN。查询前同步按 XID 追赶 WAL（Commit 应用、Abort 丢弃、UPDATE 先删后插）；consumed 水位统一表示已覆盖 WAL 的 record-end 前缀，空 WAL 从 `Lsn::FIRST` 开始，后台 `WalTailReader`、跨 checkpoint 耐久水位和 merge 仍归后续 Stage。 |
| v1.12 | 2026-11-19 | Stage C 实现：`FlushedWalTail` 只读已 flush WAL、跨段及回收后续读；per-XID pending 限流；新 generation 原子替换和 SegmentId 不复用，metadata-only 记录不再触发 seal 自增长循环；TOAST 新/共享 group 及缺/重/乱序/超限/非法 UTF-8 均停水位；新增 lag 指标与真实 commit→visible p95 样本。当前仍由查询前同步驱动消费并立即 seal，后台 worker、动态退避和 merge 留待后续 Stage。 |
| v1.13 | 2026-11-19 | Stage C 审查修复：detoast 改为 Commit 时提交后快照执行，修复 in-flight/abort 毒化；TOAST 按 commit 仅校验受影响 valueid；pending 同时限制 action 数和 16MiB 字节；`allow_stale` 真实读取 sealed 旧代并标注；lag 动态计算且可见延迟按 index OID 记录；多 active segment 按 term 合并且拒绝重复文档；seal 返回 end LSN，避免把自身记录误计为 lag；p95 样本改用 >8KiB 多 chunk 文档。 |
| v1.14 | 2026-11-19 | 二轮深度审查修复：空 manifest 也持久化 Analyzer 身份，配置失配继续 fail-stop；强制 posting map key 等于 `Posting.tid`，拒绝同 segment 重复 DocLength，统计累加改为 checked；pending 新增 64 XID 全局上限；TOAST 删除身份无法安全恢复时改为全量校验，覆盖 vacuum 后停水位。 |
| v1.15 | 2026-11-19 | 三轮审查修复：提交快照改为 XID 不在 `xip` 才可用，避免 CLOG/活跃集间隙导致 TOAST 缺块；被 UPDATE 取代且 vacuum 回收的历史版本在重放时跳过，不再楔死；回收 WAL 段时直接报“rebuild required”，不再静默跳段；删除未接线的 `InMemoryWatermarkRegistry`，空 Commit 不取快照，LSN 改用 `record_size()`，删除不可达 HOT-TOAST 分支。风险登记：`persist_generation` 仍是全量重写且受 16MiB frame 上限约束，Stage D 必须引入增量分段而非指望 merge 消除悬崖；`pending_inverted` 全局锁和 index handle 锁跨重放的头阻塞留待 Stage E 并发治理。 |
| v1.16 | 2026-11-20 | Stage D 实现：seal 改为 upsert/tombstone 增量段，active IDs 严格按旧到新排列；merge 第一遍确定每个 TID 的最新版本，第二遍按 term k-way 流式读写并保持冻结字节格式，tombstone 在输出中物理清除，成功 merge 清理退休段；Engine 在 4 个 active 段时自动 merge，`SegmentMerge` redo 校验有序 inputs+target；补 target/manifest 崩溃窗口、SegmentId 防复用、UPDATE/DELETE/TOAST/reopen 和 reader×merge×DML 回归。doc-length/版本元数据仍驻留内存且受 16MiB frame 限制，全 crash matrix 与性能验收留 Stage E。 |
| v1.17 | 2026-11-20 | Stage D 审查修复：seal/merge 不再把 consumed 水位直接推到内部 marker end，避免跳过窗口内并发 DML；require-fresh planner 使用刚刷新数据水位，内部 marker 不再误触发 stale；reopen 以 doc-length 识别空 token upsert，多段统计 checked 防溢出；merge 只接受 Sealed inputs，明确 manifest-first、redo 仅校验的 111 契约；补真实 Engine 确定性交错、reopen/merge、溢出、非 Sealed 输入和 redo payload 回归。 |
| v1.18 | 2026-11-20 | Stage D 二轮审查修复：merge 要求 `max(inputs.applied_lsn) == manifest.applied_lsn`，防止水位下降和重复 WAL 重放；reopen、merge 输入与输出按 TID checked 累加 `term_freq` 并与 doc-length 对账，拒绝语义损坏统计；lag records/bytes 排除不改变文档集合的 SegmentSeal/SegmentMerge，消除内部 marker 导致的指标抖动；补水位覆盖缺口、词频偏大/偏小及显式 flush 后零数据 lag 回归，修正旧 reopen fixture 的词频但保持全部冻结 golden 字节不变。 |
| v1.19 | 2026-11-20 | Stage D 三轮审查修复：容量错误独立分类并触发进程级熔断和警告，auto-merge 不再楔死查询，后续 DML 继续多段服务；open 与 merge 统一拒绝重复/相交 tombstone；reopen 回收低于 active 高水位的非 active 文件且保留更新 ID 防复用；k-way 每个输出 term 仅克隆一次；登记多 target/per-term 格式根治、EXPLAIN 统计截面和 O(corpus) 服务内存架构。 |
| v1.20 | 2026-11-20 | Stage D 残留审查修复：delta seal 与 merge 对称处理 `Capacity`——独立 seal 熔断和警告，保留完整内存查询视图及 durable/consumed 水位差，seal 失败时禁止 merge 未密封变更，reopen 重放后同样降级而非楔死；补后续 DML 与 reopen 重放回归，登记 pending 字节限流及多 delta 拆分根治方向。 |
| v1.21 | 2026-11-20 | Stage E 首批实现：WAND 以精确 BM25 上界执行 OR top-k，与 DAAT 全量对拍并在 MVCC 过滤后全量修复；接入真实 jieba 词典和固定 SciFact 数据，NDCG 平均差异 0.00171049，WAND/Tantivy 平均延迟比 0.999、p95 比 0.971；新增 seal crash、DROP 重建和 TOAST 旧快照 vacuum 回归；`inverted_merge_enabled` 支持 merge 对照，`inverted_seal_batch_size=128` 减少查询驱动写放大，checkpoint 回收下界纳入 inverted durable 水位。 |
| v1.22 | 2026-11-20 | Stage E 性能验收未闭合：10k docs、16 clients、6000 commits 下 merge on/off 的 commit→visible p95 分别为 26.5177s/23.0327s，均超过 1s 目标，主要瓶颈为并发 refresh/可见性路径而非 merge；保留批量 seal 和空 fsync 优化，结果写入 `docs/phase3-benchmarks.md`，后续需后台 consumer 或等价降载方案，暂不宣称 Stage E 完成。 |
| v1.23 | 2026-11-23 | 场景决策：当前主场景确认为单 Agent、低频写入，16 clients 高频提交压测仅作压力参考，不作为当前产品的性能验收阻断项；异步优化仅登记为备选：每索引单写者后台 consumer + Condvar 按水位等待、超时精确回退、DROP 生命周期、动态退避、pending 限流和确定性恢复测试。若未来多 Agent 高并发写入成为主场景，再实施该方案；当前保留同步模型，不把 `allow_stale` 当性能开关。 |
| v1.24 | 2026-11-23 | 配置审查修复：`inverted_seal_batch_size=0` 在 `Engine::open` 返回 `InvalidArgument`，不再于创建 inverted index 时 panic；`SegmentStore::set_flush_threshold` 同步返回结构化错误并补零值回归。 |
| v1.25 | 2026-11-23 | 容量生命周期修复：seal/merge 的容量熔断由永久布尔标志改为 5 秒冷却重试，成功后自动清除；新增删除缩小后冷却恢复测试，避免索引缩小后仍永久停止 compaction。 |
| v1.26 | 2026-11-23 | Stage E 性能审查修复：`AnalyzerConfig` 以 `OnceLock` 缓存冻结配置 hash，`DefaultAnalyzer::from_config` 对精确内置身份共享 `Arc` 实例，消除每次 WHERE 的词典 SHA256 和每次 seal 的词典重载；`filter_to_predicate` 统一从 Engine 索引元数据选择 analyzer，补缓存身份及中文非词干谓词回归。重测 SciFact NDCG 差异仍为 0.00171049，WAND/Tantivy mean/p95 比为 1.242/1.532；Tier 2 merge on/off 的 6000 commits 均约 60.005s、99.99 commits/s，commit→visible p95 分别为 496.013ms/270.778ms，两种变体均通过 1s 目标。v1.22 的瓶颈结论受词典开销污染并由本次数据取代，当前同步模型决策不变。 |
| v1.27 | 2026-11-23 | Stage E P3 收尾：所有 `DefaultAnalyzer` 构造路径共享 `OnceLock<Arc<Jieba>>`，保留精确内置 Analyzer 实例缓存；OOV 回归改用连续 U+3400/U+3401 CJK 字符，真实覆盖 jieba 单字回退。seal 在 generation 发布后立即清 `dirty`、置 `wal_sealed`，marker 发射独立判定，补 marker 失败后的重复 seal、merge 和 reopen 回归。基准注明 NDCG 是质量可比而非评分器等价（本方停用词不计入 `doc_len`，Tantivy 保留于 `fieldnorm`）；登记 `inverted_lag_metrics` 每次 O(lag) 重读、逐记录高频轮询可退化为 O(lag²)，当前规模暂不优化。 |
