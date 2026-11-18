# Phase 3 技术选型：全文倒排索引（BM25 / Segment / Tier 2）

版本：v1.2（2026-11-17）
状态：技术选型定稿，供后续 coding plan 与 Stage 拆分直接引用
前置事实：Phase 1（存储/MVCC/B+Tree）与 Phase 2（HNSW）已交付；Phase 4a（SQL TP/ANN、Extended Query、TOAST、DataFusion AP、Sequence/SERIAL）已完成并合入。Phase 2 的 24h 压测、CI coverage 与 `phase2-m6` tag 继续并行，不阻塞本阶段。

---

## §1 范围与非目标

### 1.1 本阶段交付

1. 新的 `pg-am-inverted` 访问方法：term dictionary、posting list、BM25、WAND。
2. 中文 jieba 与英文 Snowball/stop-word 分析链，分析器可插拔且同一文本在索引与查询侧使用同一实现。
3. 带 term frequency 与 position 的有序 posting list，保留后续短语查询所需的物理信息，但本阶段不实现短语算子。
4. 内存 write buffer → 不可变 segment → 后台 merge 的完整生命周期。
5. Tier 2：后台 WAL 消费、per-index `applied_lsn`、查询新鲜度判断、可观测 lag 与 SeqScan 回退。
6. Segment merge 同时完成物理删除，建立 segment 自身的 GC，不另造一套在线 vacuum 协议。
7. SQL DDL、查询与 EXPLAIN 的单路倒排执行路径，以及 `content @@ 'term'` 和 BM25 排序。
8. Tantivy 对照、BEIR 检索质量、Tier 2 新鲜度、crash/recovery 和全 workspace 回归。

### 1.2 明确非目标

- MultiIndexScan、RRF、跨模态 planner（Phase 4b）。
- Phrase、prefix、fuzzy、通配符、字段级 boosting、完整 Tantivy query parser。
- 同步事务型倒排索引；本阶段是**异步 Tier 2**，不承诺“写入即可见于任意旧快照”。
- Autovacuum/多 AM GC 协调器（Phase 5b）及生产级压缩（Phase 7b）。
- 直接在生产执行路径中嵌入 Tantivy；Tantivy 只作为对照实现。
- 在不改变 SQL 结果的前提下用大表抽样替代精确回退。需要限制回退成本时使用**超时并响亮报错**，不得静默改变结果集。

### 1.3 对 ROADMAP 原文的有意修订（偏离登记）

1. **全局统计的持久化形态**：ROADMAP 原文要求 `doc_count / avgdl / term_df` 在 checkpoint 的 metadata file 中维护；本选型改为**按 segment 保存并汇总**，checkpoint 只作恢复优化，不是统计事实源。理由：全局计数无法与 heap 提交原子化，容易形成“统计已更新、行未提交”的分布式事务窗口；segment 统计与对应版本同生同灭，崩溃和 merge 都能重算验证。该偏离是刻意的，不是漏做 checkpoint metadata。
2. **大表回退限制**：ROADMAP 原文为 `timeout/sampling limits`；本选型只保留 timeout，**拒绝 sampling**。理由：采样会静默改变结果集和 BM25 排名，破坏与索引路径的语义一致性；超时后以结构化错误返回，不得用近似结果冒充精确回退。

---

## §2 现有扩展点与不可破坏的约束

| 已有能力 | 本阶段使用方式 | 硬约束 |
|---|---|---|
| `AccessMethod`（`pg-am-heap/src/access_method.rs`） | 实现 AM 名称、build、redo handlers、`freshness()` | 不把倒排 posting 伪装成 heap tuple；查询 API 单独提供 |
| `AM_REGISTRY + ScanRequest/ScanSpec`（`pg-engine/src/planner.rs`） | 新增 `AmKind::Inverted`、注册项、plan spec | 只增加注册与 AST/元数据，**不改 selector 核心分派结构** |
| `SegmentedStorage`（`pg-storage/src/segment.rs`） | 复用 `SegmentId` 与单向生命周期 | trait 只管生命周期，不规定 posting 编码；`SegmentId` 在索引实例内不复用 |
| `WalTailReader`（`pg-storage/src/tier2.rs`） | 从指定 LSN 拉取已 flush WAL | 拉取式背压，不另建无界 channel，不读取未 flush 记录 |
| `WatermarkRegistry` | 保存每个索引单调递增的 `applied_lsn` | 不允许旧 worker 把水位写回旧值 |
| `SegmentSeal=110` / `SegmentMerge=111` | 复用已有 WAL 判别式并注册 redo handler | 不新增 WAL 占号；`LogicalInverted=101` 继续保留，不滥用 |
| heap MVCC / `heap_tuple_visible` / `SnapshotGuard` | 回表裁决与 vacuum 防护 | 禁止用 `Snapshot::everything()` 代替真实查询快照，Guard 覆盖全部执行流 |
| Phase 4a SeqScan/EXPLAIN | 回退原因、计划输出与执行真相复用 | `ScanSpec` 是执行唯一真相，不能规划一套、执行另一套 |

**`AccessMethod` 必要方法的落地契约**：`build` 只负责创建 SegmentStore 与索引元数据；独立 `search` 负责倒排候选查询。由于写入走 heap WAL 的 Tier 2，而不是调用方直接写倒排 AM，`insert` / `delete` / `scan` 必须显式返回 `Unsupported`，pg-engine 也不得经这三个入口驱动倒排。禁止用空 Vec、空 Ok 或伪造 heap tuple 冒充合法实现；若未来拆分生命周期与 DML trait，再按新 trait 迁移。

**依赖方向**：`pg-am-inverted` 不得依赖 `pg-engine` 或 DataFusion；`pg-engine` 负责 SQL、目录、索引创建、DML/WAL 装配和回表。`pg-storage` 不依赖任何具体 AM。

---

## §3 总体架构与数据流

```text
SQL DML
  → heap + WAL（事实源）
  → Tier 2 tail（只读已 flush 记录）
  → per-XID pending（Commit 才应用，Abort 丢弃）
  → write buffer（仅已提交版本）
  → freeze/seal → immutable segment
  → background merge + physical delete

Query
  → SQL parser / AM registry
  → freshness + snapshot 检查
  → WAND/DAAT 候选 + BM25
  → heap MVCC 回表复核
  → 排序 + LIMIT
```

关键裁决：

- **heap + WAL 是唯一事实源**。Segment 是可重建的二级结构，不是独立数据库。
- 查询必须先经过快照与新鲜度检查，再决定是否使用倒排；索引只提供候选，最终可见性以 heap 为准。
- 异步索引允许陈旧，但**陈旧程度必须可观测且计划行为确定**，不得把陈旧索引冒充最新结果。
- 第一版优先可验证的格式与恢复语义，不做自研并发 LSM、FST、压缩或复杂 query parser。

---

## §4 数据模型与物理格式

### 4.1 逻辑数据模型

每个倒排索引只针对一个 TEXT 列（首版一个索引一个字段）：

```text
Term(bytes)
  → PostingList {
        (tid, term_freq, [position...])
    }

SegmentMeta {
    format_version,
    index_oid,
    segment_id,
    state,
    doc_count,
    total_tokens,
    term_df: BTreeMap<Term, u64>,
    created_from_lsn,
    applied_lsn,
    row_count/posting_count/byte_count,
    crc32
}
```

- `tid` 使用现有堆 `(page_id, slot_id)`，绝不把 `NodeId` 当作通用行标识。
- `term_freq` 是 term 在该文档该字段中的出现次数。
- `position` 是分析器输出的 token 序号（从 0 开始），用于后续短语能力；BM25 本阶段不使用 position，但**首版就持久化**，避免下一次格式迁移。
- 每个 segment 的 posting 以 `(term, tid, position)` 全序排列，便于二分与 k-way merge。
- `doc_count` 是 segment 中已提交文档数，`total_tokens` 是这些文档 token 总数，`term_df` 是含 term 的文档数。

### 4.2 Write buffer

```text
BTreeMap<Term, BTreeMap<Tid, Posting>>
```

- 使用 `BTreeMap`，首版不引入 FST 或自研哈希词典。
- 以**估算字节数 + posting 数**双重限额控制内存；阈值可配置，不能按表大小无界增长。
- 只容纳已提交版本；未提交事务留在 per-XID pending，不进入 buffer。
- 到达阈值即 `Active → Frozen → Sealed`；空 buffer 不产生空 segment。

### 4.3 Segment 文件

采用**版本化二进制容器**，不用无模式文本拼接：

- magic + `format_version` + index/segment identity；
- metadata、term dictionary、posting blocks、doc-length table；
- 所有长度在分配前检查上限，变长整数使用固定小端编码；
- 文件尾 CRC32，坏 magic、版本、长度、计数、CRC 都是结构化 `Corrupted`，不 panic；
- 临时文件写入 → 文件 fsync → 原子 rename → 父目录 fsync。

第一版不压缩 posting。固定宽度数组 + 有界二进制格式足以验证正确性；压缩归 Phase 7b。

### 4.4 全局统计的来源

`doc_count / total_tokens / term_df` **按 segment 保存**，查询时从当前可读 segment 汇总：

- 不维护一份需要与 heap 同事务提交的全局计数文件，避免“统计已更新、行未提交”的分布式事务问题。
- Merge 从输入 segment 汇总统计并校验与重算值一致。
- 删除/更新在 merge 时才从统计与 posting 中物理移除；merge 之间的统计代表 `applied_lsn` 所对应的历史截面，需与查询新鲜度语义一起解释。
- checkpoint 只加速恢复，不是统计事实源；segment metadata 才是。

---

## §5 分析器与依赖选择

### 5.1 统一 `Analyzer` 接口

```rust
trait Analyzer: Send + Sync {
    fn analyze(&self, text: &str) -> Vec<Token>;
}

struct Token {
    text: String,
    position: u32,
}
```

要求：

- 建索引与查询必须使用同一 `Analyzer` 实例及同一词典/停用词版本。
- 返回 token 原文及连续 position；空 token 被过滤，position 不重排。
- `english`：Unicode 分段 + Snowball stemming + 固定 stop-word 列表。
- `chinese`：jieba 词典切分；与英文/数字/空白规则串接，不能逐字拆散拉丁词与数字。
- 分析器配置写入索引元数据；配置不匹配时拒绝 open，不静默换分词器。

### 5.2 中文选择：`jieba-rs = 0.10.1`

选择 `jieba-rs = { version = "=0.10.1", default-features = false }`，并在 build dependencies 中锁定 `jieba-macros = "=0.10.1"`：

- `0.11.x` 使用 Rust 1.88 才稳定的 let-chain，不符合项目 MSRV 1.86。
- `0.10.2+` 同样存在 let-chain。
- `0.10.1` 的 `jieba-rs` 与 `jieba-macros` 必须**版本一致**；宏生成的数据类型若与库不一致，会出现 `EMIT_PROBS` 编译类型错误。
- `default-features=false` 关闭 `default-dict`，避免 `include-flate/libflate` 的 let-chain 与旧 `core2` yanked 依赖链。
- 词典作为带版本与 SHA256 的数据资产 vendored，运行时通过 `Jieba::empty()` + `load_dict(BufRead)` 加载；不在测试或生产中联网下载。

这是对 ROADMAP “jieba-rs” 的保留，同时修正为可实际通过 MSRV 的版本组合，**不是改用自研中文分词器**。

### 5.3 英文选择：`rust-stemmers = 1.2.0`

- 使用成熟 Snowball 实现，不手写 Porter 变体。
- stop-word 列表冻结并纳入索引配置 hash；查询侧相同。
- 英文与中文各配独立 golden cases，混合文本另有覆盖。

### 5.4 对照实现：`tantivy = 0.25.0`

- 0.26.x 声明的 `time >=0.3.47` 已将 MSRV 推到 1.88，0.27.0 自身也要求 1.88；因此基准使用 **0.25.0**。
- Tantivy 只进 benchmark/dev dependency，不进入生产依赖图。
- 其 BM25 实现与本选型一致：`K1=1.2`、`B=0.75`，idf 使用 `ln(1 + (N - df + 0.5)/(df + 0.5))`。

---

## §6 BM25 与排序语义

### 6.1 公式

对查询 term `q`：

```text
idf(q) = ln(1 + (N - df(q) + 0.5) / (df(q) + 0.5))

score(D, Q) = Σq idf(q) ×
    tf(D,q) × (k1 + 1) /
    (tf(D,q) + k1 × (1 - b + b × dl(D) / avgdl)

k1 = 1.2
b  = 0.75
```

- `N = doc_count`，`dl = 文档 token 数`，`avgdl = total_tokens / N`。
- 全部中间计算使用 `f64`，最终展示可转 `Float8`。
- `N=0`、`avgdl=0` 或 `df=0` 时对应 term 不贡献分数；除零不是 panic 理由。
- 同分按 `(score DESC, tid ASC)` 保证跨段、重启和 merge 后顺序确定。
- 不使用 `NaN` 作为正常分数；若统计损坏产生非有限值，返回 `Corrupted`。

### 6.2 统计与删除的关系

- merge 后统计与 posting 同步精确。
- merge 前的已删除文档仍可能贡献历史统计，这是本阶段明确登记的**近似统计窗口**，不是隐藏的精确承诺。
- 所有质量对比在固定 `flush + merge` 截面执行，确保与 Tantivy 使用同一统计截面。
- 结果行可见性永远由 heap 决定；统计陈旧只能影响排序，不能造成返回不存在/不可见行。

---

## §7 查询执行：DAAT 与 WAND

### 7.1 查询形态

首版 `@@` 右侧复用同一分析器，支持：

- 空白分隔的多 term **AND**；
- 大写 `OR` 分隔的 **OR**；
- 不包含引号短语、括号优先级、否定、prefix/fuzzy。

多 term AND 使用 DAAT 交集，OR 使用 WAND 剪枝；单 term 直接 posting scan。候选进入 BM25 前先做必要的文档存在性与统计检查，避免无意义打分。

### 7.2 WAND 要求

- 按当前 term upper-bound 排序，维护 pivot 与候选上界。
- upper-bound 使用 BM25 `max_score` 的安全上界，不能用负 IDF 或非法统计猜值。
- 仅在 `score_upper_bound < 当前第 k 名分数` 时跳过文档；等于时保留候选并按最终 tie-break 处理，不能因浮点边界或同分漏行。
- 无 LIMIT 的排序查询不走 WAND；使用全量精确评估，防止把 top-k 语义误用到全结果。

### 7.3 回表与 LIMIT

- 倒排只返回候选 `(tid, score)`；最终行必须经 heap MVCC 可见性复核。
- `ORDER BY score DESC LIMIT k` 使用有界 top-k 堆，容量 `min(k, 可见候选数)`，不按用户输入的超大 k 预分配。
- 无 LIMIT 时必须返回全部匹配行，不静默截断；使用精确全量评估，并受语句级超时配置约束，超时返回结构化错误而不是部分结果。
- 非排序 `WHERE content @@ ...` 只承担过滤；未指定排序时不承诺 BM25 顺序。

---

## §8 Segment 生命周期与 merge

### 8.1 生命周期

严格复用现有单向状态机：

```text
Active → Frozen → Sealed → Merging → Retired
```

- `SegmentedStorage` 只负责生命周期；posting 的读写由 `pg-am-inverted` 自有 `SegmentStore` 完成。
- `SegmentId` 由索引元数据单调分配，merge 目标使用新 ID，retired ID 永不复用。
- `SegmentSeal` 仅携带一个 `SegmentId`；`SegmentMerge` 携带有序输入 ID 列表与新目标 ID，符合既有 doc 契约。
- 失败 merge 必须让全部输入保持 `Sealed`，不得留下对外可见的 `Merging` 半成品。

### 8.2 `SegmentManifest` 与原子可见性

每个索引另有一份 `SegmentManifest`：

```text
generation: u64
active_segment_ids: Vec<SegmentId>
applied_lsn: Lsn
```

- 查询固定读取一个 manifest generation，之后按其中的 active IDs 读取 segment；不能遍历磁盘上所有 sealed 文件。
- merge 目标完成 durable write 后先处于“文件已 sealed、但未加入 manifest”的状态，此时对查询不可见。
- 唯一发布动作是**一次原子 manifest 替换**：安装 target、移除全部 inputs、generation + 1，并推进 durable_applied_lsn。manifest 替换与文件 fsync/父目录 fsync 的先后关系必须与 §11 的崩溃协议一致。
- 在替换前读者只看到旧 generation；替换后只看到新 generation。任何时刻都不得同时暴露 target 与 inputs，也不得暴露“target 已可见、inputs 未 retire”的混合状态。
- `Merging` 仅是内部工作状态，不作为查询读集合；如果替换失败，旧 manifest 保持有效，目标临时文件按泄漏清理，不能出现半发布。

### 8.3 k-way merge

- 输入按 `(term, tid, position)` 已排序，执行流式 k-way merge，不全量读入内存。
- 同一逻辑 entry 因恢复重放重复出现时按完整键去重；正常 UPDATE 产生新 TID，不与旧 TID 合并。backfill 则按 §12.1 的锁协议避免产生重复基线。
- tombstone 在 merge 输出中物理丢弃，源统计汇总与输出重算交叉校验。
- 目标 durable 后才允许执行 §8.2 的 manifest 替换，禁止先改 manifest 再写目标。

### 8.4 回退阈值与并发

- 后台 merge 按 segment 数/字节阈值触发，不另设全局 stop-the-world。
- 读者与 merge 通过 manifest generation 原子发布隔离；文件不可原地改写。
- 单索引一个 WAL consumer，merge worker 与 consumer 协调 manifest，禁止并发 seal 同一 ID 或并发发布互相覆盖的 generation/applied_lsn。

---

## §9 Tier 2：WAL 消费与 watermark

### 9.1 事务过滤是正确性的前提

WAL consumer 不能按记录到达顺序直接改索引：

```text
HeapInsert/Update/Delete(xid)
  → pending[xid]
TxnCommit(xid)
  → 按原 WAL 顺序应用 pending[xid]
  → 推进 applied_lsn
TxnAbort(xid)
  → 丢弃 pending[xid]
```

- **只有 Commit 才进入倒排**，Abort 永不进入。
- per-XID pending 有内存上限与溢出策略；大事务不得造成无界内存增长。溢出优先使用有界临时存储；未实现前必须响亮失败并暂停消费，不能强行部分应用。
- utility 记录无事务上下文时按既有 utility 语义处理；不得把 utility 的 XID 字段误当用户事务提交证据。
- `applied_lsn` 只在对应记录及其所有前置事务都已应用后推进，因此它是“已完整应用的 WAL 前缀”。

### 9.1.1 TOAST 大文本消费

主行 `HeapInsert/Update` 中的 TEXT 可能仅是 20B `ToastPointer`，因此 consumer 不能只解码主行就直接分词。提交应用按以下顺序处理：

1. 先按 §9.1 的规则收集同一 XID 的 `(valueid, chunk_seq, payload)` chunk 行；它们与主行同 XID，并且 WAL 先于主行。需要文本的 INSERT/UPDATE 在组装并校验成功后才进入 write buffer，DELETE 只需删除旧 posting，不需要 detoast。
2. **本事务新 group**：若 pending 中存在该 `va_valueid` 的 chunk，按 `chunk_seq` 排序并复用 Stage D 的组包/校验逻辑，检查 `va_toastrelid`、`va_rawsize == va_extsize`、seq 从 0 连续、单 chunk ≤2000B、总字节数与 pointer 完全一致，再做 UTF-8 解码。缺块、重块、乱序、尺寸不符、非法 UTF-8 都是 `Corrupted`，consumer 停在原 `durable_applied_lsn` 并响亮报错，不得跳过文档、部分入库或推进水位。
3. **共享旧 group**：若 pending 中没有该 valueid 的 chunk，说明 UPDATE 复用了未修改旧 pointer。此时用 commit 后新建的 MVCC snapshot + SnapshotGuard 读取 committed chunk，并调用 Stage D 的同一 detoast helper；不能用 `Snapshot::everything()` 或 physical detoast 代替。Guard 必须覆盖 chunk 扫描与组包，保证 vacuum 不提前回收旧 group。
4. 将 Stage D 的组包和校验提取为 pg-am-heap 可复用的 helper，禁止在 inverted 中另写一份不同语义的 detoast 实现。新 group 的所有 chunk 按 Stage D 契约必然与主行同事务，因此缺块不能按共享旧 group 处理。

崩溃恢复同样先恢复 committed chunk，再重建倒排；未提交主行/chunk 都随 pending 丢弃。损坏 chunk 必须保留现场供诊断，修复后从原水位重放，不能通过“下一条记录继续”把错误隐藏过去。

### 9.2 两种水位必须区分

| 水位 | 用途 | 持久性 |
|---|---|---|
| `consumed_lsn` | 当前进程内查询新鲜度；含 write buffer 已提交版本 | 易失，崩溃后从持久水位重放 |
| `durable_applied_lsn` | 崩溃恢复起点；已完成 durable seal/元数据发布的前缀 | 持久、单调 |

- `AccessMethod::freshness()` 返回当前进程的 `consumed_lsn`。
- `WatermarkRegistry` 负责 consumed 值的单调更新；segment metadata 负责 durable 值。
- 不允许 consumer 在数据尚未应用时先推进水位，也不允许先推进 durable 水位再写 segment。

### 9.3 查询新鲜度与回退

设查询快照对应 WAL 前沿为 `snapshot_frontier`。**当前快照统一取 `wal_writer.synced_lsn()`**：tail reader 只消费已 flush 记录，不能用包含未 flush 写入的 `current_lsn()` 冒充可消费前沿；后者只用于 WAL 写入监控。

- `consumed_lsn >= snapshot_frontier`：倒排在逻辑上覆盖该快照之前的已提交变更，可走索引。
- 不满足时，计划器必须选择以下一种**显式**行为：
  1. 默认 `require_fresh`：以 `WatermarkLag` 回退 SeqScan；
  2. 可配置 `allow_stale`：只读 sealed segment，并在计划中标注 `StaleIndex`，不读取未 sealed buffer。
- `wal_lag_records/bytes`、`applied_lsn`、`synced_lsn`、`last_progress_at`、`watermark_lag_seconds` 全部暴露。`last_progress_at` 是 consumer 最近一次推进 `consumed_lsn` 的单调时钟读数；`watermark_lag_seconds = max(0, now - last_progress_at)` 表示消费停滞秒数，不能用 LSN 相减得到。
- lag 超阈值时 consumer 缩短/取消退避并优先追赶；无新记录时使用有界退避，不 busy-loop。

### 9.4 新鲜度承诺

- 正常负载下，已提交行 **p95 < 1s 可搜索**。
- 验收基准必须分别记录引擎 commit 完成时间 `commit_finished_at` 与该行实际出现在查询结果中的 `visible_at`，用 `visible_at - commit_finished_at` 形成延迟分布。仅用 consumer 首次看到 WAL 的时间只能得到上界，不能冒充精确 commit 延迟。
- p95 不达标时首先暴露 lag 与 consumer 状态，不能通过降低可见性要求“优化”数字。

---

## §10 MVCC 与快照语义

### 10.1 默认只允许当前快照走倒排

首版采用与 eager-removal B+Tree 同级的保守规则：

- `snap.xmax == current_xid`；
- `xip` 除调用方自身外为空；
- `consumed_lsn` 覆盖快照前沿。

否则以 `SnapshotFallback` 回退 heap。这样可避免：

- 已物理删除的旧 posting 对旧快照不完整；
- 在飞事务尚未决定 Commit/Abort 时的幽灵候选；
- WAL 已 flush 但事务仍未提交造成的结果错觉。

支持完整历史倒排（xid/xmax 版本化 posting）属于后续工作，不在本阶段伪装实现。

### 10.2 回表与 Guard

- 候选必须经 `heap_tuple_visible` 复核；不能仅凭 posting 就返回行。
- `SnapshotGuard` 覆盖 WAND、merge 结果流、派生任务及 heap 回表全过程，错误与 LIMIT 提前退出也必须释放。
- heap 侧 `SnapshotGuard` 保证 vacuum 不会在读取期间提前回收旧版本；异步索引的 applied_lsn 不替代这个 Guard。
- `allow_stale` 的结果也必须经过同一快照回表；stale 只影响候选完整性与排序，不放宽 MVCC。

### 10.3 回退成本

- SeqScan 使用同一 Analyzer、同一 BM25 与同一可见性规则，保证与索引路径的语义一致。
- 大表全量打分使用语句级超时配置；超时是结构化错误，**不返回采样近似结果**。
- 无索引、无匹配索引列、watermark lag、旧快照或不支持的查询形态都回退，不 panic。

---

## §11 崩溃恢复

### 11.1 恢复来源与幂等

恢复顺序：

1. 读取 `SegmentManifest` 与 segment metadata，恢复 active generation、统计与 `durable_applied_lsn`。
2. 丢弃未完成的临时文件与未 sealed write buffer；已 durable 但未进入 manifest 的 merge 目标也按未发布临时文件处理，不能直接加入读集合。
3. 从 durable 水位调用 `tail_from(successor(durable_applied_lsn))`。
4. 重放 WAL，重建 per-XID pending 与 write buffer。

要求：

- 文件布局固定为 `inverted/<index_oid>/manifest.bin` 和 `inverted/<index_oid>/segments/<segment_id>.meta`。reopen 必须读取 manifest 中 active IDs 对应的真实 metadata；缺文件、ID 不匹配、跨 segment analyzer 不一致或 analyzer hash 与当前配置不符都拒绝打开，禁止用默认 analyzer 或零水位代替恢复。
- 已 seal 的内容不重复应用；未 seal 的内容可丢弃重建，因此无丢失、无重复。
- SegmentSeal/SegmentMerge redo 按 ID 幂等：重复退休或重复安装同一目标不改变结果。manifest 替换是唯一的读集合切换点， redo 同样不得制造 target 与 inputs 同时可见的窗口。
- 崩溃发生在临时文件写入、rename 前、rename 后、manifest 替换前/后均只能出现“旧 generation 完整”或“新 generation 完整”。目标文件 durable 但 manifest 未替换时仍只能看到旧 generation。

### 11.2 不新增 posting WAL

- DML 的 heap WAL 已足够重建 posting，**不再为每个 posting 增加一条 WAL**，避免双重写入与跨 WAL 协议。
- 只为 segment 生命周期复用 110/111，并注册其 redo handlers。
- `LogicalInverted=101` 作为未来倒排专属管理记录预留，不在本阶段占号。

### 11.3 故障注入矩阵

必须覆盖：

- WAL commit 前、commit 后未消费、消费后未 seal、seal 临时文件未 rename、seal 已 rename；
- merge 临时输出未发布、目标文件已 durable 但 manifest 未替换、manifest 已替换；
- backfill 与并发 INSERT/UPDATE/DELETE 互斥：结果行不得重复，统计不得把同一 TID 计为两个文档；
- TOAST 新 group 多 chunk、UPDATE 复用旧 group，以及缺块/重块/尺寸不符，均不得推进水位；
- pending 事务 Commit/Abort、大事务溢出、WAL 段切换、checkpoint 前后；
- 重复 open/reopen、索引删除后重建与 SegmentId 不复用。

---

## §12 SQL、DDL 与 planner

### 12.1 DDL

统一沿用 Phase 4a 的 `CREATE INDEX` 形态，只新增一种 AM 与可选 BM25 声明：

```sql
CREATE INDEX content_idx
ON agent_memory USING inverted (content)
WITH (fulltext_index = 'bm25');
```

- `WITH (fulltext_index='bm25')` 可省略，默认就是 BM25；其他值响亮 `Unsupported`。
- 不增加第二套 `CREATE FULLTEXT INDEX` AST。
- 首版每列至多一个 inverted 索引。backfill 必须使用 **`AccessExclusive` 覆盖整个创建过程**：取锁后重新取得快照与 `base_lsn = wal_writer.synced_lsn()`，扫描并构建初始 segment，随后发布初始 manifest 和 consumer 起点，最后释放锁。consumer 从 `successor(base_lsn)` 开始消费。锁期间没有其他 DML，因此不会产生“backfill 已含旧值、WAL 又写入新值”的重复基线；不能用 `AccessShare` 代替，也不能只按 `(term, tid, position)` 去重来掩盖竞态。
- backfill 的写停止窗口是首版明确接受的限制；大表增量 backfill / per-TID upsert 归后续优化，不在本阶段引入。
- 空表可建；非空表也允许，与 HNSW 的历史“仅空表可建”约束不同。
- DROP TABLE 使所属索引失效并停止 consumer；复名/重建必须获得新 index_oid 与新 SegmentId 序列。

### 12.2 查询与排序

```sql
SELECT id, bm25_score(content, 'contract') AS score
FROM agent_memory
WHERE content @@ 'contract'
ORDER BY bm25_score(content, 'contract') DESC
LIMIT 10;
```

- `@@` 是过滤谓词；`bm25_score` 是排序/投影表达式，两者必须复用同一查询文本与 Analyzer。Stage 0 必须显式定义 `SelectItem::Bm25Score` 与 `OrderByKind::Bm25Score` AST；`AS score` 仅是结果列标签，ORDER BY 直接重复评分表达式，**不引入 alias 解析依赖**。
- 两个位置的查询文本不一致时响亮拒绝，不能分别用不同文本计算候选与分数。
- `ORDER BY bm25_score(...) DESC LIMIT k` 走 WAND/BM25；未指定排序时只保证过滤语义。
- **额外谓词与 ANN 先例一致**：`content @@ 'x' AND id > 5` 首版一律以 `ExtraWhere` 回退到 SeqScan，执行“普通谓词过滤 → BM25 精确评分/排序 → LIMIT”。禁止“倒排候选 + post-filter”造成 top-k 候选不足或把近似组合当成精确计划。
- 参数化 `content @@ $1` 属于 Stage 0 必交付范围：AST 接受 `Literal::Param`，参数推断为 TEXT，`exec_with_params` 完成类型化绑定后才复用同一 Analyzer；pg-wire 的 Bind、参数描述和错误恢复复用 Stage C 路径，不得通过 SQL 文本替换实现。
- 无索引/不支持形态按现有 SeqScanReason 扩展回退，不能将 `@@` 静默解释为普通字符串相等；回退原因清单必须包含 `ExtraWhere`。
- `InvertedScanSpec` 至少携带 index_oid、列、查询文本、分析器配置、limit、freshness 与候选统计，是执行唯一真相。

### 12.3 EXPLAIN

输出至少包括：

```text
InvertedScan (index=content_idx, analyzer=chinese, limit=10,
              consumed_lsn=..., snapshot_frontier=..., plan=Fresh)
```

回退必须输出原因：`NoIndex`、`WatermarkLag`、`SnapshotFallback`、`MissingLimit` 或 `UnsupportedQuery`。不输出执行后才能知道的 WAND pivot/actual candidates；这些可作为独立执行统计。

---

## §13 删除、merge 与 vacuum

- heap DELETE/UPDATE 的 committed 版本在 Tier 2 中先形成 tombstone，随后 merge 物理清除。
- 不用 heap vacuum 的 TID 直接驱动索引删除来替代 WAL：WAL 已包含完整语义，避免 heap vacuum 与索引 consumer 竞争。
- heap vacuum 负责堆空间，segment merge 负责 posting 空间；二者可独立运行。
- merge 是唯一 posting GC 通道，指标包括：segments、postings、tombstones、merge 次数、merge 耗时、释放字节、当前最老 segment 年龄。
- Phase 5b 的多 AM GC 协调不在本阶段引入。

---

## §14 Crate 归属与依赖/MSRV

### 14.1 模块划分

新 crate `crates/pg-am-inverted`：

| 模块 | 责任 |
|---|---|
| `error.rs` | `InvertedError`，所有损坏/不支持/IO 均结构化返回 |
| `analyzer.rs` | Analyzer、token、英文/中文/混合文本实现 |
| `posting.rs` | Posting、doc length、统计、编码与有界解码 |
| `segment.rs` | write buffer、SegmentStore、生命周期、原子发布 |
| `bm25.rs` | BM25 公式、upper-bound、可解释统计 |
| `wand.rs` | DAAT/WAND、top-k、tie-break |
| `tier2.rs` | WAL consumer、pending txn、applied_lsn、lag |
| `index.rs` | AccessMethod、freshness、redo handlers、查询 API |
| `recovery.rs` | 元数据恢复、幂等 redo、临时文件清理 |

`pg-engine` 负责 DDL、backfill、planner/executor、回表与 query stats；`pg-wire` 只复用现有类型编码，新增 `Float8` 无需新协议类型。

### 14.2 生产依赖

```toml
jieba-rs = { version = "=0.10.1", default-features = false }
jieba-macros = "=0.10.1" # build dependency，锁定宏生成格式
rust-stemmers = "1.2.0"
```

其他 bincode/serde/crc32fast/thiserror 等沿用 workspace 既有版本；不引入 Tantivy、FST、新异步 runtime 或新压缩库。

### 14.3 对照依赖与锁定

```toml
tantivy = { version = "=0.25.0" } # benchmark only
time = "=0.3.45"                 # 避免 0.3.47+ 的 Rust 1.88 门槛
uuid = "=1.24.0"
bon = "=3.3.1"
darling = "=0.20.11"
```

`Cargo.lock` 必须保留这些版本，不能用一次无约束 `cargo update` 将它们升级。

### 14.4 已执行的 MSRV 验证

在隔离目录 `target/tmp/phase3-msrv-probe` 中，以 `jieba-rs/jieba-macros=0.10.1`、`rust-stemmers=1.2.0`、`tantivy=0.25.0` 及上述锁定传递依赖执行：

```bash
cargo +1.86.0 check
```

结果：**通过**。该探针不修改生产 workspace，也不构成对新 crate 功能已实现的声明。

---

## §15 验证方法论与验收标准

### 15.1 单元与格式测试

- 中英文及混合文本 golden tokens、position、停用词、大小写与词典 hash。
- Posting/segment golden bytes；坏 magic、版本、长度、计数、CRC、截断均为 `Corrupted`，无 panic。
- BM25 对 Tantivy `bm25.rs` 的已知答案、零文档、零 df、重复 term、长短文档与并列 tie-break。
- TOAST 消费：覆盖 >8KiB 多 chunk 新 group、UPDATE 复用旧 group、缺块/重块/乱序/尺寸错误/非法 UTF-8 全部 `Corrupted` 且不推进水位；Abort 不入库，查询路径禁用 `Snapshot::everything()` 和 physical detoast。
- WAND 与 DAAT 全量结果逐项相等；LIMIT 0/1/k/超大 k 不容量溢出。

### 15.2 MVCC 与 Tier 2

- Commit 可见、Abort 不可见、in-flight 不可见、UPDATE 的新旧版本按快照分别可见。
- 旧快照触发 `SnapshotFallback`；回表 Guard 阻止对应 vacuum 提前回收。
- 事务 pending 溢出、乱序 Commit/Abort、跨 WAL 段、checkpoint 前后均不重复、不遗漏。
- `applied_lsn` 单调；stale worker 不得回写旧水位。

### 15.3 Segment 与 crash

- freeze/seal/merge 全生命周期及非法状态迁移。
- merge 前后查询结果与 tie-break 一致；重复 redo 幂等；临时文件清理。
- 按 §11.3 的崩溃窗口逐项 kill -9 后 reopen，验证无重复、无丢失、无半成品。
- DROP/重建不复用 index_oid 或 SegmentId。

### 15.4 检索质量与性能

- **BEIR SciFact**（英文标准子集）+ 一组冻结的中文/混合文本语料与查询：
  - 同一分析器、同一统计截面下，NDCG@10 与 Tantivy 的差异目标 **±0.005**；
  - 延迟目标沿用 ROADMAP：相对 Tantivy **不超过 3×**，记录绝对延迟而非只报倍数；
  - WAND 相对 DAAT 必须有结果一致性证明，性能提升只作报告，不牺牲正确性。
- 中文集只用于验证本实现的分词与检索，不把英文 BEIR 数字冒充中文质量结论。
- **Tier 2 新鲜度基准固定负载**：release 构建，10,000 篇文档、16 个并发客户端、100 commits/s、持续 60s 且至少 1,000 个 commit 样本；查询包含高频/低频/空结果词共 100 个，分别报告冷缓存与热缓存。负载分两类：①普通档，平均字段长度约 1 KiB、不触发 TOAST；② TOAST 档，平均字段长度仍约 1 KiB，但每个测试批次至少包含一个 **>8 KiB** 文本样本，覆盖多 chunk 写入与共享旧 group。两档都覆盖 merge 关闭与开启，并记录机器、OS、Rust 版本、数据/查询 hash 和写入速率。
- 正常负载已提交行可搜索：**p95 < 1s**，按 §9.4 的 `commit_finished_at → visible_at` 样本计算；同时公布 `watermark_lag_seconds` 分布、consumer 速率及 merge 开销。性能对比必须与 Tantivy 使用同一机器、数据、查询和缓存口径。

### 15.5 全量门禁

- `cargo test --workspace` 与 `cargo test --workspace --release`；
- 新 crate 全 feature Clippy/rustdoc `-D warnings`；
- `cargo +1.86.0 check --workspace --all-features`；
- fmt、`git diff --check`、现有 TP/ANN/Wire/TOAST/DataFusion 全部回归；
- 两轮对抗审查：实现审查 → 修复 → 独立复核，重点为 pending 事务、水位、merge、旧快照与 crash。

覆盖率 job 若新增 `pg-am-inverted`，必须单独注册；不得沿用 pg-am-hnsw 的 90% 结果冒充本 crate 覆盖率。

---

## §16 建议 Stage 拆分

| Stage | 交付与退出条件 |
|---|---|
| Stage 0 | 格式/语义冻结：Analyzer、posting、segment、`SegmentManifest`、WAL/Tier 2、`Bm25Score` AST、`Literal::Param` 参数推断为 TEXT、`ExtraWhere` 回退、CI 注册；golden tests 先行 |
| Stage A | Analyzer + posting + BM25 + DAAT，内存模型与 Tantivy 已知答案一致 |
| Stage B | 单 segment 持久化、`AccessExclusive` backfill、初始 manifest、查询与 EXPLAIN、无索引回退；老目录兼容打开 |
| Stage C | Tier 2 consumer（含 TOAST 组包/复用 group）、Commit/Abort pending、watermark、SeqScan 回退、p95 < 1s；新增 doc-hidden `commit_finished_at` 测试 hook 记录 XID 完成时间，consumer 侧记录 `visible_at`，不使用伪造的生产观测点 |
| Stage D | k-way merge、物理删除、统计一致性、manifest 原子替换、SegmentId 与 rename/retire 协议 |
| Stage E | 全 crash matrix、并发 reader×merge×DML、WAND 与全量对拍、BEIR/中文基准 |
| Stage F | 两轮对抗审查、全 workspace debug/release、MSRV、文档与基准归档 |

任何阶段不得用“后续再补”为由跳过 pending 事务、快照回退、merge 原子性或 crash 测试。

---

## §17 风险与开放问题

1. **大事务 pending**：首版以有界临时存储为目标；未实现前响亮暂停消费。绝不能把未提交版本提前发布以释放内存。
2. **统计截面**：merge 间历史统计会影响排序，必须在文档与 EXPLAIN 保留可观察性；若业务要求更强一致性，后续引入按 LSN 的统计增量而不是隐藏差异。
3. **旧快照**：首版只允许当前快照走倒排，历史 posting 版本化是独立大工作，不在本阶段扩张。
4. **WAND upper-bound**：错误上界会直接漏行；必须以 DAAT 全量对拍为硬门，尤其并列分数与 NaN 边界。
5. **词典/分析器升级**：会改变 posting 与 BM25，必须通过配置 hash 触发显式重建，不能在线混用。
6. **Tantivy 版本漂移**：基准版本和 lockfile 一并归档，禁止无约束升级造成 MSRV 或质量数字失真。
7. **merge 与读者延迟**：大 segment 流式 merge 仍需测量 I/O 与页缓存污染；优化只能发生在正确性证据之后。
8. **backfill 写停止与重复基线**：首版用 `AccessExclusive` 换取简单且可证明的基线一致性；必须接受并记录锁窗口，后续若需在线 backfill，优先评估 per-TID upsert 而不是按 entry 去重。
9. **stale 读配置**：`allow_stale` 是有意放宽，不是默认优化；计划输出必须标注，调用方选择 `require_fresh` 时不得启用。
10. **TOAST 组包与复用 group**：大文本必须走 §9.1.1 的同一 detoast 语义；新 group 缺块与共享旧 group 可见性损坏都只能 `Corrupted` 停水位，不得通过忽略 chunk 来“提高”新鲜度。

---

## §18 参考资料

- 项目需求：[ROADMAP.md](../ROADMAP.md) Phase 3（BM25、segment、Tier 2、watermark、NDCG 与 crash 验收）。
- 现有契约：`pg-storage/src/{segment.rs,tier2.rs}`、`pg-am-heap/src/access_method.rs`、`pg-engine/src/planner.rs`。
- BM25 实现参考：[Tantivy `src/query/bm25.rs`](https://github.com/quickwit-oss/tantivy/blob/master/src/query/bm25.rs)。
- 中文分词：[messense/jieba-rs](https://github.com/messense/jieba-rs) 与 [`Jieba` API](https://docs.rs/jieba-rs/0.10.1/jieba_rs/struct.Jieba.html)。
- 英文 stemming：[CurrySoftware/rust-stemmers](https://github.com/CurrySoftware/rust-stemmers)。
- 检索质量基准：[BEIR paper](https://arxiv.org/abs/2104.08663) 与 [BEIR 数据集](https://github.com/BeIR/BeIR)。

---

## 修订记录

| 版本 | 日期 | 内容 |
|---|---|---|
| v1.0 | 2026-11-17 | 初稿：冻结倒排数据模型、segment/Tier 2、MVCC/回退、merge/recovery、SQL/planner、依赖与验收口径；明确 jieba/macros 0.10.1 与 Tantivy 0.25.0 的 MSRV 兼容选择。 |
| v1.1 | 2026-11-17 | 审查修复：backfill 改为 `AccessExclusive` 全锁协议，防止重复基线；新增 `SegmentManifest` 保证 merge 原子可见性；明确 `AccessMethod` 必要方法返回 `Unsupported` 的契约；`Bm25Score` 直接用于 ORDER BY 以避免 alias 依赖；补齐 lag 秒数定义、commit→visible 测量和固定性能负载。 |
| v1.2 | 2026-11-17 | 二审修复：新增 TOAST Tier 2 消费协议（同 XID 新 group 组包、共享旧 group 的 MVCC detoast、Corrupted 停水位）；`ExtraWhere` 统一走 SeqScan 精确过滤与评分；显式登记 per-segment 统计与拒绝 sampling 两项 ROADMAP 偏离；`content @@ $1` 参数化纳入 Stage 0；`commit_finished_at` 测试 hook 纳入 Stage C；基准增加 >8KiB TOAST 样本。 |
