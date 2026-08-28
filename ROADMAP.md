# pg_rust 开发 Roadmap

## 设计原则与纪律

1. **严格分层构建**：Layer 1 → Layer 2 → Layer 3，地基不稳上层必塌
2. **每阶段可交付**：每阶段结束时内部 Agent 团队能用 psql/gRPC/MCP 客户端连接使用
3. **AM 按优先级递进**：HNSW → FT → TS → Graph → Columnar，不追求一次性六种全做
4. **每加 AM 必带 Vacuum/GC**：不留技术债到最后
5. **AM 边界守得住**：内核提供原语（TTL/降采样/删除），策略由上层 SDK 实现；LLM 永远外置
6. **正确性 > 性能 > 功能**：宁可少一个功能，不可有一个 bug
7. **不跳阶段，不过度设计**：每阶段验证标准全部通过才进入下一阶段；接口可扩展但实现不提前
8. **每阶段一篇技术博客**：为对外推广积累素材

---

## 阶段总览

```
Phase 1：存储基座 + 行存 + 事务 + B+Tree
         ├── M1: 物理层（Page/WAL/BufferPool/LSN）
         ├── M2: 行存 + MVCC + B+Tree + 崩溃恢复
         │    ├── M2a: 单语句 auto-commit
         │    ├── M2b: 多语句事务 + MVCC
         │    └── M2c: 完整锁管理 + 并发
         └── M3: Vacuum（离线）+ 可观测性 + PG Wire 极简版
              （Vacuum 在线化明确归 Phase 5b 多 AM GC 协调器，见 Phase 5b 节）

Phase 2：HNSW 向量索引
         ├── 2a: In-memory HNSW
         ├── 2b: WAL + 持久化 + 崩溃恢复
         └── 2c: 并发控制 + Tier 1 同步

Phase 3：全文倒排索引
         ├── 倒排 AM + BM25
         ├── Tier 2 完整落地
         └── Segment merge + GC

Phase 4a：SQL 层 + PG Wire Protocol（基础）
Phase 4b：Multi-Path Fusion + 跨模态 Planner

Phase 5a：时序 + 列存 + 蒸馏 stub（Phase 1 门，可与 Phase 2/3/4a 同时起步）
         ├── 内核：TimeSeries AM + TTL、列存投影原型
         └── SDK 层：记忆蒸馏（stub 早期并行）

Phase 5b：图 + GC + 遗忘/分层 + Fusion 接入（需 4a / ≥2 AM / 4b 陆续进）
         ├── 内核：Graph AM（轻量版）+ 图查询语法、时间范围查询路由、多 AM GC 协调器
         ├── SDK 层：遗忘曲线、记忆分层（等 GC 协调器落地）
         └── Fusion 接入：图/时序参与 Fusion（等 4b）

Phase 6：完整协议层 + 多 Agent 隔离

Phase 7：生产化
         ├── 7a: 可观测性 + 备份恢复
         ├── 7b: 性能与压缩
         ├── 7c: 完整 CBO
         └── 7d: 高可用 + 高级特性
```

---

## 总工期估算

| Phase | 乐观 | P50（典型节奏） | 长尾（遇 P0 阻塞） | 风险等级 |
|---|---|---|---|---|
| Phase 1 (M1+M2+M3) | 6 个月 | 9–12 个月 | 15+ 个月 | 🔴 高（事务/恢复是核心） |
| Phase 2 (HNSW) | 9 个月 | 12–15 个月 | 24+ 个月 | 🔴 高（long pole） |
| Phase 3 (Inverted) | 4 个月 | 6–8 个月 | 10 个月 | 🟡 中（segment 模式新） |
| Phase 4a+4b (SQL+PG → Fusion) | 4 个月 | 6–8 个月 | 12 个月 | 🟡 中（DataFusion 集成） |
| Phase 5a (时序+列存+蒸馏stub，可与 2/3/4a 并行) | 1.5 个月 | 2–3 个月 | 4 个月 | 🟡 中（可与主干并行） |
| Phase 5b (图+GC+遗忘/分层+Fusion接入) | 2 个月 | 3–4 个月 | 6 个月 | 🟡 中（SDK/内核边界，门控多） |
| Phase 6 (协议+隔离) | 6 个月 | 9–12 个月 | 18+ 个月 | 🔴 高（完整协议） |
| Phase 7 (生产化) | 持续 | 12+ 个月 | 持续 | 🟢 低（持续优化） |

**合计 P50：55–75 个月 / 约 5–6 年（1–2 名高级 Rust 工程师）**

> 注：Phase 1 和 Phase 2 是风险最高的阶段，实际工期可能偏向区间上限。

---

## Phase × Layer 映射

| Phase | Layer 1（物理） | Layer 2（事务/可见性） | Layer 3（Access Methods） | 跨层能力 |
|---|---|---|---|---|
| Phase 1 M1 | ✓ Page/WAL/BufferPool/LSN/Checkpoint | — | — | （文件管理分散在各组件中，不独立暴露） |
| Phase 1 M2 | (扩展) Full Page Image | ✓ MVCC / Lock / Snapshot / Visibility | ✓ B+Tree（含 AccessMethod trait） | ARIES 崩溃恢复 |
| Phase 1 M3 | — | (扩展) Vacuum | — | PG Wire 极简版 + 可观测 |
| Phase 2 | (Tier 1 异步 IO) | (扩展) Per-tx delta | ✓ HNSW（Epoch + 节点锁） | HNSW Vacuum |
| Phase 3 | (Tier 2 异步 IO) | (扩展) Watermark | ✓ Inverted Index（BM25） | Segment merge |
| Phase 4a | — | — | (单路选择) | DataFusion + PG Wire Extended + EXPLAIN |
| Phase 4b | — | (扩展) Cost hooks | (多路融合) | MultiIndexScan + Fusion + Planner |
| Phase 5a | — | — | ✓ TimeSeries | 列存投影原型 |
| Phase 5b | — | (扩展) Multi-AM GC 协调 | ✓ Graph（轻量） | Fusion 接入时序+图 |
| Phase 6 | — | (扩展) RLS predicate | (稳定) | MCP Server + 完整 PG Wire |
| Phase 7a | — | — | — | 备份恢复 + 监控 |
| Phase 7b | (性能优化) | — | ✓ Columnar Projection | SIMD / io_uring / 大页 |
| Phase 7c | — | ✓ 完整 CBO + 统计 | (压缩) SQ/PQ | 跨模态代价模型 |
| Phase 7d | — | ✓ SSI | — | 高可用 + Jepsen |

---

## 阶段依赖关系

```
主干（必须线性）：
Phase 1 ──→ Phase 2 ──→ Phase 3 ──→ Phase 4a ──→ Phase 4b
(基座+行存)   (向量)      (全文)      (SQL+PG)     (Fusion)

可并行 track（不阻塞主干，有人力时可提前启动）：

Phase 5a（Phase 1 门，可与 Phase 2/3/4a 同时起步）：
  ├── 时序 AM + TTL ──────── 可与 Phase 2/3/4 并行；
  │                          注意：segment merge 逻辑需等 Phase 3 完成后复用，
  │                          或自行实现简易版（仅时间分区 seal + TTL 清理）
  ├── 列存投影原型 ────────── 可与 Phase 2/3 并行
  └── 记忆蒸馏 SDK ────────── 外部 LLM，可拿 stub 早期并行

Phase 5b · 4a 门（需 SQL 层，不需 4b）：
  ├── 图 AM（轻量版）+ 图查询语法 ── 需要 SQL 层就绪
  └── 时间范围查询路由 ───────────── planner 谓词路由 = 4a 的活

Phase 5b · ≥2 AM 门（与 4b 正交，是 vacuum 期的事）：
  └── 多 AM GC 协调器 ── ≥2 个 AM 落地即可起；
                          落地后遗忘曲线 / 记忆分层 SDK 才有真实 mark_for_gc trait 可用

Phase 5b · 4b 门（仅两条 Fusion 集成尾巴）：
  ├── 图参与 Fusion ──── 把 Graph 输出接进 4b 的 MultiIndexScan 算子
  └── 时序参与 Fusion ── 把时序输出接进 4b 的 MultiIndexScan 算子

Phase 6（4a 门，与 5b · 4a 门同批起）：
  ├── 完整 PG Protocol ─── 需要 Phase 4a 的 PG Wire 基础
  └── MCP Server ────────── 需要 Phase 4a 的 SQL 层

注：Phase 5b 与 4b 之间是【部分、单向】依赖：仅"图/时序参与 Fusion"两条尾巴等 4b，
其余 5b 交付物依赖 Phase 4a 或 ≥2 AM，可与 4b 并行起跑。
反过来 4b 不依赖 Phase 5（4b 只需 HNSW + 倒排 + planner）。
Graph AM 整体 gate 在 4a（遵循"接口可扩展但实现不提前"原则）；若团队接受 SQL 集成返工风险，
可把 Graph AM 存储层 + BFS/DFS 原语提到 5a 起步，仅图查询语法留 5b。

Phase 4b + Phase 5b + Phase 6 → Phase 7
```

---

## Phase 1：存储基座 + 行存 + 事务 + B+Tree

**目标：最小可用数据库，Agent 可以存取结构化元数据**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 6 个月 / P50 9–12 个月 / 长尾 15+ 个月
**风险等级**：🔴 高（事务/恢复是核心，决定整个项目成败）

### Milestone 1：物理层

| 模块 | 说明 |
|------|------|
| Page Allocator | 固定大小页分配/释放（8KB/16KB 可配置），freelist 管理，不假设页内容 |
| WAL Writer | append-only 日志，接受 (record_type, payload)，fsync 语义，CRC32 校验；物理 WAL 完整实现（before/after image）；逻辑 WAL 接口预留（Phase 2 HNSW 接入时实现） |
| Buffer Pool | page_id → in-memory frame 映射，LRU/CLOCK 替换，pin/unpin 协议，WAL 先行规则（刷页前确保相关 WAL 已持久化） |
| LSN Clock | 全局单调递增，所有组件共享 |
| 文件管理 | 数据文件/WAL/元数据文件操作分散在 PageAllocator/WalWriter/Superblock 中，不独立暴露；O_DIRECT 为 Phase 7b 优化项 |

**验证标准：**
- **正确性优先（不设硬性性能指标）**：
  - WAL 顺序写正确性：每条记录 crash 后能完整恢复
  - Buffer Pool 正确性：pin/unpin 不泄漏、eviction 不丢页
  - 单元测试覆盖率 ≥ 90%
  - proptest 验证 Page Allocator 正确性（无泄漏、无重叠）
- **性能参考基线（Phase 7b 再硬性要求）**：
  - WAL ≥ 200MB/s（顺序写，本地 SSD 物理上限的 60%）
  - Buffer Pool ≥ 50K ops/s（随机读 8KB page）
- **崩溃测试**：随机 kill -9 × 1000 次无数据丢失

### Milestone 2：行存 + 事务 + B+Tree + 崩溃恢复

#### M2 内部检查点

为避免一次性实现完整 MVCC/死锁检测/B+Tree 后才暴露架构问题，M2 拆为三个内部检查点：

| 检查点 | 目标 | 验证 |
|---|---|---|
| M2a | 单语句 auto-commit + 堆表 + 无并发 B+Tree | `INSERT` / `SELECT` / `UPDATE` / `DELETE` 单线程正确 |
| M2b | 多语句事务 + MVCC 快照 + 可见性判断 | 并发读写无脏读，RC/SI 快照正确 |
| M2c | 完整锁管理 + 死锁检测 + B+Tree 并发 | 100 并发 CRUD + 随机崩溃测试通过 |

#### 交付物

| 模块 | 说明 |
|------|------|
| Heap Storage | Slotted page 行存，支持变长字段，TOAST 溢出页（大对象/向量/JSONB 不放主行） |
| Tuple 格式 | 胖 header（xmin, xmax, agent_id, trace_id, flags）+ 定长标量列 + 列指针 |
| Transaction Manager | begin/commit/abort，事务 ID 分配（64-bit 无 wraparound） |
| Snapshot 机制 | **LSN-based snapshot**（与"单一 WAL + 单一 LSN"根契约一致）；snapshot = {xmin_lsn, xmax_lsn, active_xacts: ATT 快照}；无 XID wraparound 问题。默认隔离级别：Snapshot Isolation（SI）；RC（每语句新快照）可选；SSI 推迟到 Phase 7d |
| Visibility Oracle | is_visible(xmin, xmax, snapshot) 统一判断，所有 AM 共享 |
| Lock Manager | 行级锁（基于 tuple header xmax 字段，S/X 模式），表级锁（4 标准模式），等待队列 + 死锁检测（wait-for graph，100ms 周期）。IS/IX 意向锁推迟到 Phase 6 ALTER TABLE/VACUUM FULL 支持时再加 |
| B+Tree Index | Latch coupling 读，乐观/悲观插入，叶子页分裂，实现 AccessMethod trait |
| 崩溃恢复 | 完整 ARIES 变体：Analysis → Redo → Undo，CLR 保证嵌套崩溃安全 |
| Checkpoint | Fuzzy Checkpoint：收集 ATT+DPT，后台刷脏页，更新超级块 |
| Full Page Image | 每个 checkpoint 周期内页首次修改时记录完整页副本，防止 torn page |

**验证标准：**
- ACID 正确性：并发读写 + 随机 kill 进程，重启后数据始终一致
- 能跑简单的 Agent 元数据存取（session 记录、用户画像、配置）
- 并发性能：100 并发连接下 TPS ≥ 10K（简单 CRUD，单表，SSD，无网络协议开销）

### Milestone 3：基础 Vacuum + 可观测性 + PG Wire 极简版

| 模块 | 说明 |
|------|------|
| Vacuum | 扫描死元组（xmax 已提交且无活跃快照引用），回收空间，通知 B+Tree 清理对应条目 |
| 可观测性 | WAL dump 工具（人类可读）、活跃事务列表查询、锁等待关系查询、Buffer Pool 命中率统计、查询统计（pg_stat_statements 等价物：每 query 的延迟、行数、扫描路径） |
| PG Wire Protocol 极简版 | 仅支持 Simple Query + 文本结果格式，让 psql / 标准 PG 驱动能连上来跑 CREATE/INSERT/SELECT/UPDATE/DELETE |
| SegmentedStorage 接口预留 | Phase 3 (Inverted) 和 Phase 5 (TimeSeries) 都是 segment-based 架构，预留接口：SegmentedStorage trait (create_segment/freeze/seal/merge)、segment lifecycle、WAL 协议扩展 (SEGMENT_SEAL/SEGMENT_MERGE 记录类型) |
| Tier 2 接口预留 | WAL tail reader 接口、watermark registry 接口、planner 可感知索引新鲜度的 hook |
| ~~在线/渐进式 Vacuum~~ | **不在 M3 范围**。明确归属 Phase 5b（多 AM GC 协调器）：M3 只交付离线 `Engine::vacuum(table)`（`AccessExclusive` 一次完成）及其全部可复用地基（快照注册水位线、页内压实原语、索引通知路径、WAL 记录族）。若 M3 验收后停写窗口成为实际问题，以"分段离线"（按页区间分批持锁）缓解，不做中间态独立在线化 |

**验证标准：**
- Vacuum 后空间可被复用，无无限膨胀
- psql 能连接并执行基本 SQL（CREATE TABLE / INSERT / SELECT / UPDATE / DELETE）
- 主流 PG 驱动（psycopg2、node-postgres、rust-postgres）可正常连接
- 可通过命令行工具诊断事务和锁问题

### Phase 1 对 Agent 团队的价值

- 替代 SQLite/PG 存储 Agent 的结构化元数据
- session_id、agent_id、timestamp 等字段原生支持
- 行级 provenance（谁写的、什么时候写的）
- psql / Python / TypeScript 客户端直接可用

---

## Phase 2：HNSW 向量索引

**目标：支持向量存储与近邻检索，Agent 可以做语义记忆召回**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 9 个月 / P50 12–15 个月 / 长尾 24+ 个月
**风险等级**：🔴 高（long pole of the entire roadmap）

### 拆分理由

HNSW 是 6 种 AM 中工程量最大的：
- 并发控制（HNSW undo 是已知难题）
- WAL 集成（逻辑 WAL + 一致性修复）
- 崩溃恢复（Epoch snapshot + 增量重放）
- 可见性过滤（2x 候选 + top-K）

任一项卡住会延期整个 Phase。拆成 3 个 sub-phase，每个有独立 demo。

### Phase 2a：In-memory HNSW（3 个月）

**目标：** 能在内存里建图、查询；不要求持久化

| 模块 | 说明 |
|------|------|
| VECTOR(n) 类型 | 一等公民数据类型，DDL 级声明，支持 f32/f16/bf16 |
| HNSW 内存版 | 分层随机图、M/M_max、邻居选择（simple / robust prune）、贪心搜索 |
| 距离函数 | L2 / Cosine / Inner Product，基础实现正确；SIMD 优化作为 Phase 2a 末尾或 Phase 7b 的优化项 |
| 加载 API | 从磁盘加载预构建的图（用 hnswlib 格式互通） |

**验证标准：**
- 1M 768d 向量 recall@10 ≥ 95%（sift-128-euclidean, gist-960-euclidean）
- 1M 向量加载时间 < 5 分钟
- 搜索延迟 P99 < 20ms（ef_search=64）
- 单元测试 ≥ 90%

### Phase 2b：HNSW WAL + 持久化（4 个月）

**目标：** HNSW 变更进单一 WAL，崩溃后能完整恢复

| 模块 | 说明 |
|------|------|
| 逻辑 WAL 记录 | add_node(v, neighbors), connect(a, b), remove_node(tombstone) |
| On-disk 邻居列表 | 节点 page 写入 Buffer Pool，遵循 WAL 先行 |
| Checkpoint HNSW 快照 | 入口点、最大层数、节点计数 |
| 崩溃恢复 | 从 checkpoint + WAL 重放，验证图一致性 |
| 幂等 redo | 重放同一 WAL 记录 N 次结果一致 |

**验证标准：**
- 1M 向量随机 kill -9 × 1000 次，图结构语义一致
- 重启恢复时间 < 30 秒（checkpoint 后增量重放）

### Phase 2c：HNSW 并发控制 + Tier 1 同步（5–7 个月）

**目标：** 高并发下 HNSW 仍能正确维护

| 模块 | 说明 |
|------|------|
| Epoch-based reclamation | 防止 use-after-free 的安全回收 |
| 节点级细粒度锁 | 非页级，避免粒度过粗 |
| Tier 1 同步策略 | 事务内累积 delta，commit 时批量合并到 HNSW 图 |
| 可见性适配 | 搜索时多取 2x 候选，回表做 visibility 过滤后返回 top-K |
| 并发 undo | abort 时标记 tombstone + 后台修复邻居连通性 |
| Vacuum 扩展 | tombstone 比例监控 + 局部重建 |

**验证标准：**
- 100 并发 INSERT + DELETE 持续运行 24 小时，图结构语义一致：
  - 双向边不对称率 < 1%
  - recall@10 不低于崩溃前 95%
  - 无悬挂节点（所有节点可从入口点可达）
- 并发 abort 后 HNSW 搜索精度（recall@10）≥ 单线程基线的 95%
- 对标：与 pgvector (HNSW) 和 Qdrant 对比 recall/latency/throughput

### Phase 2 SQL 示例

```sql
-- 通过 PG Wire Protocol 调用（Phase 1 M3 的最小子集即可）
INSERT INTO memories (id, content, embedding) VALUES ('m1', 'hello', '[0.1, 0.2, ...]');

-- 纯向量检索
SELECT id, content FROM memories
ORDER BY embedding <=> $1
LIMIT 10;

-- 向量 + 结构化过滤组合（Phase 1 B+Tree + Phase 2 HNSW）
SELECT id, content FROM memories
WHERE agent_id = 'a1'
ORDER BY embedding <=> $1
LIMIT 10;
```

### Phase 2 对 Agent 团队的价值

- Agent 对话/文档的 embedding 存储与语义检索
- 与结构化过滤组合（WHERE team='sales' 先 B+Tree 过滤，再向量排序）
- 记忆的语义去重（近似向量检测）
- 单一事务保证：插入一条记忆 + 对应向量，要么同时成功要么同时失败

---

## Phase 3：全文倒排索引

**目标：支持 BM25 全文检索，Agent 可以做关键词记忆召回**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 4 个月 / P50 6–8 个月 / 长尾 10 个月
**风险等级**：🟡 中（segment 模式新，但有 Tantivy/Lucene 参考）

### 交付物

| 模块 | 说明 |
|------|------|
| 全文索引声明 | WITH (fulltext_index = 'bm25') DDL 级声明 |
| Inverted Index AM | 实现 AccessMethod trait，segment-based 架构 |
| 分词器 | 中文（jieba-rs）+ 英文（stemming + stop words），可插拔 |
| Posting List | 每个 term 对应一个有序 TID 列表 + term frequency + 位置信息 |
| BM25 评分 | 标准 BM25 公式（k1=1.2, b=0.75），支持 WAND 加速 |
| BM25 全局统计 | 维护 doc_count、avgdl、term_df，更新时原子递增；checkpoint 时持久化到元数据文件 |
| Segment 架构 | 写入缓冲（内存）→ flush 为不可变 segment → 后台 merge |
| Tier 2 完整落地 | 后台 worker 消费 WAL 流，维护 applied_lsn，planner 感知 watermark |
| Watermark 机制 | planner 查询时检查倒排索引的 applied_lsn 是否覆盖查询 snapshot |
| Watermark 监控 | watermark_lag_seconds 指标导出，planner 可据此判断是否 fallback；当 lag 超过可配置阈值时自动提高后台 worker 消费速率 |
| Fallback 策略 | watermark 落后时：1) 优先只查询已 flush 的不可变 segment，忽略未 flush 的 write buffer；2) 若查询需要最新数据且 segment 覆盖不足，再回退到 seqscan；3) 大表场景下 seqscan fallback 必须配合超时/采样限制，避免查询挂死 |
| Segment Merge | 后台合并小 segment，清理已删除的 posting 条目 |
| Vacuum 扩展 | segment merge 时物理清理已删除 TID 的 posting |
| 崩溃恢复 | 丢弃未 flush 的 write buffer，从 applied_lsn 继续追赶 |

### 验证标准

- 检索质量：标准 IR 数据集 NDCG@10 对齐 Tantivy 水平
- Tier 2 延迟：写入后 < 1 秒可检索到（正常负载下）
- 崩溃恢复：随机 kill 后从 applied_lsn 正确追赶，不丢不重
- 对标：与 Tantivy / Elasticsearch 对比检索质量和延迟

### Phase 3 对 Agent 团队的价值

- Agent 记忆的关键词检索（"上次讨论过合同纠纷"）
- 与向量检索互补（向量找语义相似，全文找精确关键词）
- 无需外接 ES，减少运维复杂度

---

## Phase 4a：SQL 层 + PG Wire Protocol（基础）

**目标：让 pg_rust 能被标准 SQL 和 PG 驱动完整访问**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 2 个月 / P50 3–4 个月 / 长尾 6 个月
**风险等级**：🟡 中（DataFusion 集成存在 TP 适配风险）

### DataFusion TP 适配性检查点

在 Phase 4a 开始时必须先完成以下验证：

1. DataFusion 能支持简单点查的延迟 < 5ms（单连接）
2. DataFusion 能正确传递事务上下文到 TableProvider
3. 若上述任一项不满足，必须启动"TP 路径绕过 DataFusion"的备用方案

备用方案概要：
- 简单点查/小范围查询直接走自研执行器
- 复杂分析查询走 DataFusion
- SQL 层统一路由，对上层透明

### 交付物

| 模块 | 说明 |
|------|------|
| DataFusion 集成 | SQL 解析、逻辑计划、物理计划；自定义 TableProvider 对接 pg_rust 存储 |
| 单路索引扫描 | 查询优化器能为单个谓词选择 B+Tree / HNSW / 倒排索引 |
| PG Wire Protocol 基础版 | Simple Query + Extended Query（参数绑定），文本/二进制结果格式 |
| EXPLAIN | 显示单路扫描计划 + 代价估算 |

### 验证标准

- psql 能连接并执行 CREATE/INSERT/SELECT/UPDATE/DELETE
- DataFusion 能正确下推 filter 到 pg_rust 的 TableProvider
- 主流 PG 驱动（psycopg2、node-postgres、rust-postgres）可正常连接
- 简单点查延迟 < 5ms（如备用方案生效则绕过 DataFusion）

### Phase 4a 不做

- MultiIndexScan / Fusion（放到 Phase 4b）
- Extended Query 模式的完整类型映射（放到 Phase 6）
- 跨模态 Planner（放到 Phase 4b）

---

## Phase 4b：Multi-Path Fusion + 跨模态 Planner

**目标：单条 SQL 同时走多个索引并融合结果 — pg_rust 的核心差异化能力**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 2 个月 / P50 3–4 个月 / 长尾 6 个月
**风险等级**：🟡 中

### 交付物

| 模块 | 说明 |
|------|------|
| MultiIndexScan 算子 | 并行启动多个 AM scan，按 fusion strategy 合并 TID，统一 visibility check，回表取行 |
| Fusion 策略 | 过滤式（A∩B∩C）、RRF 排序式（Reciprocal Rank Fusion）、混合式（硬过滤+软排序） |
| 跨模态 Planner（初版） | 识别多模态谓词，自动拆分为多条 AM scan 路径，选择 fusion 策略 |
| 基础 Cost Model | 各 AM 选择性估计：B+Tree 直方图、HNSW 距离分布、倒排 term frequency |
| EXPLAIN 增强 | 显示多路扫描计划 + 各路径代价 + fusion 策略 |

### 示例查询

```sql
SELECT * FROM agent_memory
WHERE team = 'sales'                    -- B+Tree 路径
  AND content @@ 'contract'             -- 倒排路径
ORDER BY embedding <=> $vec             -- HNSW 路径
LIMIT 10;
```

执行计划：
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

### 验证标准

- 正确性：融合结果与"分别查询再应用层合并"结果一致（100% 符合）
- 性能：多路并行 < 各路串行之和的 60%
- 质量：RRF 融合的 NDCG 优于单路最佳
- 对标：与"PG + pgvector + pg_search 分别查询 + 应用层合并"对比端到端延迟

### Phase 4b 末：开源 Alpha 准备

**为什么是这个节点：**
- Phase 4b 完成意味着"一条 SQL 完成混合召回"可 demo，最适合对外展示
- 早期用户反馈能在 Phase 5/6 进入开发前修正方向

**准备清单：**
- README + CONTRIBUTING + LICENSE（Apache 2.0 或 MIT）
- CI/CD 跑通（GitHub Actions：rust check + cargo test + benchmark）
- API 文档自动生成（cargo doc + mdbook）
- 性能 benchmark 公开可复现（标准数据集 + 脚本）
- 受众声明（明确范围：单机、Agent 记忆场景优先）

**不做的事（避免过度承诺）：**
- 不承诺跨平台（初期 macOS + Linux）
- 不承诺分布式
- 不承诺 PostgreSQL 完全兼容

### Phase 4 对 Agent 团队的价值

- **核心价值：** Agent 一次"回忆"调用 = 一条 SQL，同时利用语义+关键词+结构化
- 大幅简化 Agent 记忆召回的应用层代码（不再需要调用三个服务再合并）
- 事务保证：混合检索的结果是快照一致的
- **这是对外推广时最强的 demo 场景**

---

## Phase 5a：时序 + 列存 + 蒸馏 stub（与 Phase 2/3/4a 并行）

**目标：交付时间维度记忆管理的基础设施，Phase 1 完成即可独立起步**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 1.5 个月 / P50 2–3 个月 / 长尾 4 个月
**风险等级**：🟡 可与主干并行（Phase 1 门；segment merge 复用需等 Phase 3 的 Tier-2 异步链路，属增量）

### 5a.1 内核交付物（必做）

| 模块 | 说明 |
|------|------|
| TimeSeries AM | 时间分区存储（按天/小时自动分区），范围扫描，降采样聚合（segment merge 复用需等 Phase 3 的 Tier-2 异步链路就绪） |
| TTL 自动过期 | 声明式 TTL（WITH ts_partition = 'day', ttl = '90d'），后台自动清理过期分区 |
| 列存投影原型 | 时序/记忆分析场景的轻量列存物化视图，Tier 2 异步维护，验证 HTAP 架构可行性 |

### 5a.2 SDK 层交付物（必做，但不在内核）

| 模块 | 说明 |
|------|------|
| 记忆蒸馏 SDK | 多条细节记忆 → 一条摘要记忆（调用外部 LLM），作为后台异步 job 执行，不阻塞写入事务；蒸馏结果以新元组写入并重新建索引（Phase 5a 拿 stub 早期并行） |

### 验证标准

- 时序查询性能：100M 时间点，热分区范围查询延迟 < 5ms（SSD，无聚合）；带降采样聚合的查询延迟 < 50ms
- TTL 正确性：过期数据在后台清理后不可查询，空间可回收
- 蒸馏正确性：蒸馏后的摘要记忆可被向量和全文索引正确检索

---

## Phase 5b：图 + GC + 遗忘/分层 + Fusion 接入

**目标：补齐关联维度、统一 GC、遗忘/分层，并完成时序/图对 MultiIndexScan 的接入**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 2 个月 / P50 3–4 个月 / 长尾 6 个月
**风险等级**：🟡 门控多（4a / ≥2 AM / 4b 三道门陆续进）

### 5b.1 内核交付物（必做）

| 模块 | 说明 | 启动门 |
|------|------|--------|
| 时间范围查询 | WHERE created_at BETWEEN ... AND ... 自动路由到时序索引 | Phase 4a（planner 路由） |
| Graph AM（轻量版） | 邻接表存储，支持有向/无向边，边属性（JSONB），3 跳以内 BFS/DFS | Phase 4a（SQL 层就绪） |
| 图查询语法 | SQL 扩展（LATERAL 递归或类 Cypher 子句） | Phase 4a |
| 多 AM 统一 GC 协调器 | 统一的 Vacuum 协调：基于 oldest_active_snapshot 推进，回收死元组时通知所有引用该 TID 的索引。**含 Vacuum 在线化**：heap/B+Tree 的在线渐进式 vacuum 作为协调器的第一个消费者落地（页级 TOCTOU 判定重验证、节流/背压、增量进度追踪、后台调度生命周期）；M3 的离线 vacuum 交付物（水位线注册表、压实原语、索引通知路径、`HeapCleanup`/`PageFree` WAL 族）全部作为其底层能力直接复用 | ≥2 个 AM 落地（与 4b 正交） |
| 图参与 Fusion | 图遍历结果可与向量/全文/结构化联合检索 | Phase 4b（MultiIndexScan 算子） |
| 时序参与 Fusion | 时序索引加入 MultiIndexScan，支持"最近 7 天 + 语义相似 + 关键词匹配"组合 | Phase 4b |

### 5b.2 SDK 层交付物（必做，但不在内核）

| 模块 | 说明 | 启动门 |
|------|------|--------|
| 遗忘曲线 SDK | 基于访问频率 + 时间衰减的重要性评分，标记可淘汰记忆（应用层评分 + 触发内核 GC） | GC 协调器落地（≥2 AM） |
| 记忆分层 SDK | 短期（会话内）→ 工作（任务级）→ 长期（持久化）的视图抽象 | GC 协调器落地（≥2 AM） |

**Layer 边界说明：**
- 内核 trait 只暴露 `mark_for_gc(tids) / vacuum_range()` 这类原子能力（由 Phase 5b 的多 AM GC 协调器提供）
- 评分算法、LLM 调用、分层策略都是 SDK 层，不进内核
- Graph AM 保持整体在 5b（避免在 SQL 层未定时盲建实现）；若需提前并行，可将"邻接表存储 + 遍历算子"与"SQL/查询语法"拆为 storage / query 两段，storage 段下沉到 5a，但 query 段仍需 4a — 需承担 storage 接口返工风险

### 验证标准

- 图遍历：百万边规模，3 跳遍历 < 50ms
- GC 协调：多 AM 环境下无 TID 泄漏，无悬挂索引条目

### Phase 5（5a+5b）对 Agent 团队的价值

- "最近 7 天的所有交互按时间线展示" — 时序查询（5a）
- "用户 A 上周投诉了物流 → 关联到订单 X" — 图遍历（5b）
- 自动遗忘不重要的记忆，避免记忆库无限膨胀（5b）
- 记忆蒸馏：Agent 自动将碎片记忆整合为结构化知识（5a stub → 5b 完整）

---

## Phase 6：完整协议层 + 多 Agent 隔离

**目标：对外提供标准服务能力**

**时间估算（1–2 高级 Rust 工程师）：** 乐观 6 个月 / P50 9–12 个月 / 长尾 18+ 个月
**风险等级**：🔴 高（完整 PG 协议兼容 + RLS 正确性）

### 交付物

| 模块 | 说明 |
|------|------|
| 完整 PG Wire Protocol | Extended Query 完整支持、Prepared Statement、参数绑定、类型映射、错误码兼容 |
| MCP Server | 原生 MCP 工具接口，暴露记忆读写/检索/管理能力 |
| RLS（Row Level Security） | Agent 级别的行级安全策略，多 Agent 数据隔离 |
| 多租户配额 | Agent 维度的存储/查询配额限制 |
| IS/IX 意向锁 | 支持 ALTER TABLE / VACUUM FULL 等表级操作与行级操作协调 |

### 验证标准

- PG 协议兼容性：主流 PG 驱动（psycopg2、node-postgres、rust-postgres、asyncpg）完整测试矩阵通过
- MCP 集成：Claude Code / Dify 可通过 MCP 直接使用记忆能力
- RLS 正确性：Agent A 无法读取 Agent B 的数据（混合检索场景下也正确）
- Fusion 算子统一注入 RLS 谓词，per-tenant 隔离测试通过

### Phase 6 对 Agent 团队的价值

- 多 Agent 协作场景：共享记忆但有权限边界
- 对外推广：标准接口，第三方 Agent 框架可直接集成
- Claude Code / Dify / LangChain 等主流框架可通过 MCP 或 PG 协议直接使用

---

## Phase 7：生产化

**目标：达到可对外推广的生产质量**

**时间估算：** 持续 / 12+ 个月
**风险等级**：🟢 低（优化阶段，无架构风险）

### Phase 7a：可观测性 + 备份恢复（3–4 个月）

| 模块 | 说明 |
|------|------|
| 监控体系 | Prometheus metrics 导出、慢查询日志、查询热力图 |
| 物理备份 | 文件级快照（CoW 极简版，仅用作快照备份） |
| 逻辑备份 | 导出/导入工具 |
| PITR | 基于 WAL 的 Point-in-Time Recovery |
| WAL Shipping | 用于热备（不做主从自动切换） |
| Autovacuum 生产形态 | 后台自动 vacuum 调度（基于 Phase 5b 的在线 vacuum 能力：水位监控、节流参数、触发策略）+ VACUUM FULL/CLUSTER |

**前置技术债（来自 Phase 1 Stage B）**：
- **WAL LSN 空洞兼容性**：Phase 1 的 `LsnClock::reserve` 占位机制（用于 checkpoint
  FPI race 消除）允许在 WAL 流中产生合法的零字节空洞（`reserve` 后进程崩溃、未执行
  `append_at`）。当前 recovery scanner 将零字节视为 end-of-WAL，本地恢复行为正确。
  但 WAL Shipping 场景下，receiver 端扫到零字节无法区分"sender 尚未发送完毕"与
  "合法空洞即 WAL 结尾"。**解决方向**：引入类 PostgreSQL 的 WAL page header（含
  `xlp_pageaddr` 连续性校验 + page magic），使 receiver 能通过 page header 判断页面
  是否完整接收。此项必须在 WAL Shipping 实现前落地。

### Phase 7b：性能与压缩（3–4 个月）

| 模块 | 说明 |
|------|------|
| 列存投影生产版 | 通用 AP 场景的列式物化视图，Tier 2 异步维护，向量化扫描 |
| 向量压缩 | Scalar Quantization (SQ) / Product Quantization (PQ)，自动策略选择 |
| SIMD 全面优化 | 覆盖所有距离函数、B+Tree 比较、CRC 校验 |
| io_uring | Linux 异步 I/O |
| 大页内存 | 减少 TLB miss |

### Phase 7c：完整 CBO（4–6 个月）

| 模块 | 说明 |
|------|------|
| 跨模态统计信息收集 | 各 AM 统计形态：B+Tree 直方图、HNSW 距离分布、Inverted term frequency、TimeSeries bucket 分布 |
| 代价模型校准 | 基于真实 workload 的代价参数校准工具 |
| EXPLAIN 跨模态分解 | 每条访问路径的代价分解，可解释计划选择 |

### Phase 7d：高可用 + 高级特性（持续）

| 模块 | 说明 |
|------|------|
| Jepsen 测试 | 事务隔离 + 崩溃恢复的标准化测试 |
| SSI（Serializable） | 完整 SSI 冲突检测（谓词锁 + dangerous structure） |
| 主从切换 | 基于 WAL Shipping + Raft/Paxos 选主 |
| 文档与 SDK | Python/TypeScript/Go SDK + 完整 API 文档 + 示例项目 |

### Phase 7 验证标准（总）

- Jepsen 测试通过（事务隔离 + 崩溃恢复）
- 7×24 稳定运行测试（72 小时高并发压测无 OOM/死锁/数据丢失）
- 对标综合 benchmark：Agent 记忆场景端到端性能 ≥ PG+pgvector+ES 方案的 80%

---

## 里程碑与 Agent 团队对接节点

| 完成阶段 | Agent 团队可开始使用的能力 | 替代的外部组件 |
|----------|------------------------|--------------|
| Phase 1 M3 | 结构化元数据存取（psql / Python 客户端） | SQLite / PG（基础 CRUD） |
| Phase 2a | 语义记忆检索（内存版，重启丢失） | 开发阶段原型验证 |
| Phase 2c | 语义记忆检索（持久化 + 并发安全） | pgvector / Qdrant |
| Phase 3 | 关键词记忆检索（BM25 全文） | Elasticsearch / Meilisearch |
| Phase 4b | **一条 SQL 完成混合召回**（核心 demo） | 应用层多服务拼装 |
| Phase 5a | 时序记忆 + 蒸馏 stub（与 2/3/4a 并行交付） | — |
| Phase 5b | 完整记忆生命周期管理 + 图关联 + 遗忘/分层 | 自研遗忘/蒸馏逻辑 |
| Phase 6 | 标准协议对外服务 + 多 Agent 隔离 | — |
| Phase 7 | 生产级部署 | 整套多引擎架构 |

---

## 每阶段的 Benchmark 对标策略

| 阶段 | 对标对象 | 测试场景 | 达标线 |
|------|---------|---------|--------|
| Phase 1 | SQLite / PG | 纯 TP（CRUD 混合负载） | ≥ SQLite 性能的 50% |
| Phase 2 | pgvector / Qdrant | 纯向量检索（ANN benchmark） | recall@10 ≥ 95%, latency 差距 < 2x |
| Phase 3 | Tantivy / ES | 纯全文检索（标准 IR 数据集） | NDCG@10 对齐，latency 差距 < 3x |
| Phase 4b | PG+pgvector+ES 拼装 | 混合检索端到端 | 总延迟 < 拼装方案的 50% |
| Phase 5 | TimescaleDB / Neo4j | 时序范围 / 图遍历 | latency 差距 < 3x |

---

## 显式战略边界

| 能力 | 处理 | 阶段 | 备注 |
|---|---|---|---|
| HTAP（行+列统一引擎） | **做（原型 + 生产）** | Phase 5（列存投影原型）+ Phase 7b（生产版） | 满足"统一引擎"差异化定位 |
| CoW 快照（基础） | **做（极简）** | Phase 7a（备份恢复中实现） | 仅用作快照备份，不展开为通用特性 |
| CoW 秒级分支（Agent 沙箱） | **推迟** | 不在 7 阶段 roadmap 中 | 当前 Agent 场景不刚需 |
| Serverless / 计算存储分离 | **不做** | — | 战略选择：聚焦内核质量，不与 Neon 竞争 |
| 跨地域分布式事务 | **不做** | — | 单机路线 |
| 多租户 RLS | **做** | Phase 6 | 多 Agent 隔离刚需 |
| 多租户配额 | **做** | Phase 6 | 同上 |
| 动态脱敏（DLP） | **推迟** | Phase 6 之后评估 | 看需求 |

---

## 持续正确性保障

数据库的正确性不能靠手工测试保证。每个 Phase 应配备以下测试基础设施：

| 测试类型 | 引入阶段 | 工具 | 目标 |
|---|---|---|---|
| 单元测试 | Phase 1 M1 起 | cargo test | 覆盖率 ≥ 90%（核心模块） |
| 模糊测试 | Phase 1 M1 起 | proptest | Page Allocator 不泄漏、WAL 记录可 round-trip |
| 并发模型检查 | Phase 1 M2b 起 | loom | Lock-free 数据结构正确性、Latch 无死锁 |
| 随机崩溃测试 | Phase 1 M2a 起 | 自研 harness（kill -9 + 重启 + 校验） | ACID 正确性 |
| 确定性模拟测试 | Phase 2b 起 | 自研（I/O + 并发顺序注入） | 并发 bug 可复现 |
| Jepsen | Phase 7d | jepsen | 事务隔离 + 崩溃恢复 业界标准验证 |

---

## 风险登记

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Phase 1 工期失控，占项目总工期 40% 以上 | 后续 Phase 被迫压缩 | 设 M1/M2a/M2b/M2c 内部检查点，允许分期交付；若 M2 延期，优先保证 M2a 单语句可用 |
| HNSW 并发 bug 难以复现 | Phase 2c 延期 | Loom 并发模型检查 + 大量 stress test + 确定性模拟测试 |
| DataFusion 不适合 TP 路径 | Phase 4a 架构调整 | Phase 4a 前做 TP 点查原型验证；保留"TP 路径绕过 DataFusion"的备用方案 |
| Tier 2 watermark 长期落后导致查询退化到 seqscan | 全文/列存查询延迟暴增，Agent 体验降级 | 监控 watermark_lag_seconds；超过阈值时自动提高 worker 速率或临时升为 Tier 1；planner 在 EXPLAIN 中显示是否发生 fallback |
| 多 AM GC 协调器复杂度导致 TID 泄漏或过早清理 | 索引膨胀或查询结果缺失 | 统一基于 oldest_active_snapshot 推进；每个 AM 实现 reclaim_tid 并做 fuzz 测试 |
| 单人/小团队工程量过大 | 整体延期 | 严格 MVP 思维，每个 Phase 只做最小必要集 |
| HNSW undo 的 tombstone 累积 | 搜索精度下降 | Vacuum 中加入 tombstone 比例监控，超过阈值触发局部重建 |
| 类型系统演进（VECTOR/AGENT_ID 加列/删列/改类型） | online DDL 复杂 | online DDL 设计 + 向后兼容测试 |
| RLS 在混合检索下的正确性 | 权限泄漏 | Fusion 算子统一注入 RLS 谓词，per-tenant 隔离测试 |
| PG 协议兼容性陷阱（Prepared Statement / 类型映射 / 异常） | 驱动不兼容 | 用真实 PG 驱动做兼容性测试矩阵 |
| Segment-based AM 与 Buffer Pool 抽象的张力 | Phase 3 回头改 Layer 1 | Phase 1 M3 提前预留 SegmentedStorage 接口 |
| Agent 长会话的 snapshot 累积（百万级活跃事务） | 内存爆炸 | snapshot 老化 + oldest_active_snapshot 推进策略 |

---

## 开放问题

### 来自设计阶段自我批评

1. **回表开销被低估**：图遍历多跳每步都回表做可见性检查，延迟指数放大；FT posting 含百万 TID 不能逐一回表。缓解：HNSW 多取 2x 候选、Phase 2c 末评估 TID+XID 索引条目、posting 段级 snapshot 过滤。

2. **HNSW 并发控制**：HNSW 图结构的并发修改是已知难题，undo() 对图意味着"删除节点+修复所有邻居"可能破坏连通性。缓解：Epoch-based reclamation + 节点级细粒度锁（Phase 2c）；参考 Vamana/DiskANN 无锁设计（Phase 2 启动前研究任务）。

3. **列存投影 Tier 2 一致性窗口**：AP 查询若命中 Tier 2 投影，存在秒~分钟级滞后；fallback 到 seqscan 等于退化。缓解：精细增量合并（类似 Delta Lake Z-Order merge），watermark-aware planner 避免退化路径。

4. **跨模态 CBO 是"画饼"最重的部分**：无数学公式、无统计信息收集方案、HNSW 代价高度依赖数据分布。缓解：Phase 4b 用启发式策略选择，Phase 7c 再做完整 CBO；统计信息维护按 AM 单独设计。

5. **Multi-Path Fusion 正确性问题**：RRF 在候选集不重叠时退化为 union；融合策略自动选择没明确算法。缓解：Phase 4b 显式拆分"过滤式"（实）和"RRF"（虚）两个子阶段；可解释 EXPLAIN 让用户能 override。

### TODO（尚未设计完成）

6. **HNSW 并发协议**（参考 Vamana/DiskANN 无锁设计）：Phase 2c 启动前需补齐方案或选择"单写者简化路径"。

7. **增量 Vacuum 协议**（6 种 AM 的 GC watermark 推进机制）：Phase 5 多 AM GC 协调器实现前需补齐。

---

## 技术选型

| 组件 | 选择 | 理由 |
|------|------|------|
| 语言 | Rust | 内存安全、零成本抽象、SIMD 友好、async 生态成熟 |
| 异步运行时 | tokio | 业界标准，DataFusion 依赖 |
| SQL 引擎 | DataFusion（Phase 4a 引入） | Arrow 生态、可插拔、AP 向量化执行 |
| 中文分词 | jieba-rs | 轻量、无外部依赖 |
| 序列化 | bincode / postcard | 零拷贝、紧凑 |
| 测试框架 | proptest + loom + 自研崩溃注入 | 覆盖正确性、并发、崩溃三个维度 |
| 连接协议 | **PG Wire Protocol 最小子集（Phase 1 M3 起）+ Extended Query（Phase 4a）** | SQL 是 Agent 已经熟悉的接口（pgvector/Qdrant 都用 SQL）；Phase 1 M3 实现最小子集仅需 ~500 行 Rust 代码 |
| PG Wire 实现 | 自研（参考 PostgreSQL 官方协议文档 + pgwire crate） | 协议消息类型多但每条简单；~3000 行 Rust 可覆盖 psql/SQLAlchemy/Prisma 兼容；自研可控性高 |
| 图索引 V1 | 邻接表 + B+Tree（Phase 5） | 先验证图语义，避免过早做原生图存储；性能不足时再引入专用 Graph AM |
