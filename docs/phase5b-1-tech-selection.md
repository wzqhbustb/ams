# Phase 5b-1 技术选型：Graph AM 与 multi-AM GC coordinator

版本：v1.8（2026-11-25）
状态：技术选型草案，供后续 coding plan 与 Stage 拆分直接引用
前置事实：Phase 1（存储/MVCC/B+Tree）、Phase 2（HNSW）、Phase 3（inverted）与 Phase 4a（SQL TP/ANN、Extended Query、TOAST、DataFusion AP、Sequence/SERIAL）已完成。当前已满足 Graph 所需的 SQL 层和 multi-AM GC 所需的 ≥2 AM 条件。Phase 2 的 24h 压测、CI coverage 与 `phase2-m6` tag 继续并行，不阻塞本阶段。

---

## §1 范围与非目标

### 1.1 本阶段交付

1. 新的 `pg-am-graph` crate：轻量邻接列表、节点/边 DML、稳定 ID、属性校验、BFS/DFS。
2. 有向图与无向图，节点属性和边属性均使用真实 `JSONB` 类型，最多 3 跳遍历。
3. `CREATE GRAPH`、Graph DML 与 `GRAPH BFS/DFS` 查询语句，复用现有事务、heap WAL、快照和 PG Wire。
4. multi-AM GC coordinator：以 `oldest_snapshot_xmin()` 为统一水位，协调 heap、B+Tree、HNSW、inverted 和 Graph 的回收。
5. online incremental vacuum：按页推进、工作去重、backpressure、后台 worker 启停，以及确定性的 `gc_once` 同步入口。
6. B+Tree 空页合并、页内 dead-space compaction、页链 generation 检查和带版本的 `PageFree` 恢复协议。
7. 百万边规模下的 3 跳基准、跨 AM 无悬空索引项、crash matrix、全量门禁和两轮对抗审查。

### 1.2 明确非目标

- Phase 5b-2 的遗忘曲线、记忆分层与 `mark_for_gc` SDK（在 GC coordinator 和 trait 稳定后开展）。
- Phase 4b 的 MultiIndexScan、RRF 与跨模态 planner。
- Phase 5a 的 TimeSeries AM，以及 Graph/Time-series into Fusion（Phase 5b-3）。
- 通用 Cypher、递归 SQL、可变长路径、图模式匹配、路径最优化、图算法库。
- 全协议 JSONB 二进制编解码、完整 JSONPath/JSON 查询算子（归 Phase 6 类型扩展）。
- 为 Graph 另建一套页面格式、WAL 事实源或 MVCC 实现；heap + WAL 仍是唯一事实源。
- 在首版实现完全无锁的 B+Tree 页合并。首版允许短的关系级写停止窗口，但必须增量、可测量，不能退回整表长时间停写。

### 1.3 与 ROADMAP 的顺序约定

本阶段严格按当前 ROADMAP 的实施序号执行：

```text
5b-1 Graph AM + multi-AM GC
  → 5b-2 forgetting/layering
  → Phase 6 protocol/isolation
  → Phase 5a TimeSeries
  → Phase 4b MultiIndexScan + planner
  → Phase 5b-3 Fusion integration + final acceptance
```

只有 5b-1 范围全部验收后，才进入 5b-2。Graph into Fusion 和 Time-series into Fusion 留到 5a 与 4b 都完成后。

---

## §2 现有扩展点与不可破坏的约束

| 已有能力 | 本阶段使用方式 | 硬约束 |
|---|---|---|
| `AccessMethod`（`pg-am-heap/src/access_method.rs`） | Graph AM 注册、build、恢复入口 | `build` 初始化 GraphStore；`search` 仅 BFS/DFS；`insert/delete/scan` 显式 `Unsupported`，Graph DML 只走 GraphStore API；`freshness()` 返回 `None`（同步 AM，不走 Tier 2）。禁止空 Ok、空 Vec 或伪造 heap tuple |
| heap AM、slotted page、HOT、TOAST | GraphItem 的持久化、属性外置、版本可见性 | Graph 与所有其他表共用同一 heap/WAL/MVCC 事实源 |
| B+Tree（`pg-am-btree`） | `src` / `dst` 邻接索引、空页合并与清理 | 不引入第二套邻接索引导航；先索引清理，再堆物理回收 |
| `pg-txn` Snapshot / SnapshotGuard / visibility | Graph 遍历和 GC 的旧快照保护 | 禁止 `Snapshot::everything()` 代替真实快照；Guard 覆盖完整执行流 |
| `WalTailReader` / inverted watermark | 不直接复用为 Graph 遍历；用于说明各 AM 新鲜度差异 | 同步 Graph DML 不另造 Tier 2 consumer |
| `HeapCleanup` / `PageFree` WAL family | online reclaim 的基础记录族 | 不复用无版本 free 来处理跨代页复用；需要扩展版本化 payload |
| `Engine::vacuum` / `oldest_snapshot_xmin()` | 离线算法、统一水位、确定性测试入口 | 在线与离线必须共用回收语义，不得出现两套答案 |
| SequenceManager / WAL-first sequences | Graph edge ID 分配与图级 sequence 生命周期 | 允许崩溃/回滚产生 ID 空洞，禁止重用 |
| `LogicalGraph=102` / AM OID 分配 | Graph AM 预留接口与注册 | `GRAPH_AM_OID=9880`；邻接 DML 仍走 heap WAL，不滥用 102 记录图行 |
| PG Wire Extended Query | Graph 查询、参数化 start ID 和结果描述 | JSONB 首版仅文本协议；二进制 JSONB 归 Phase 6 |
| Phase 4a `AccessExclusive` / DDL 回滚 | `CREATE GRAPH`、DROP 和目录发布 | 锁内发布、失败回滚，禁止幽灵 registry/handle |

**依赖方向**：

- `pg-am-graph` 可依赖 `pg-storage`、`pg-txn`、`pg-am-heap`、`pg-am-btree`，不得依赖 `pg-engine` 或 DataFusion。
- `pg-engine` 负责 Graph DDL、目录、事务编排、Graph DML 和 GC 调度。
- `pg-storage::gc` 只定义与具体 AM 无关的 GC 请求、工作项和统计，不能反向依赖任何 AM。
- `pg-am-hnsw` 继续保持对 `pg-am-heap` 的独立边界；GC trait 放在 `pg-storage`，不得借 GC 名义让 HNSW 依赖 heap。
- 生产依赖 `serde_json = "1"`（当前 lockfile 为 1.0.150）用于 JSONB 写入、解码和参数校验，分别登记在 `pg-am-heap`、`pg-engine`、`pg-wire`、`pg-am-graph` 的 Cargo.toml。

---

## §3 总体架构与数据流

### 3.1 Graph DML 与遍历

```text
CREATE GRAPH / GRAPH ADD NODE / GRAPH ADD EDGE
  → pg-engine 事务编排
  → GraphItem（heap relation）
  → src/dst B+Tree 邻接索引（同事务、同 WAL）
  → graph metadata + edge-ID sequence

GRAPH BFS/DFS
  → SQL parser / graph registry / snapshot
  → 邻接索引逐跳读取
  → visited + depth bound + deterministic order
  → (node_id, depth, path_json)
```

关键裁决：

- 邻接记录与索引必须同事务提交，遍历只读取当前快照可见版本。
- 遍历不得每跳全表扫描；`OUT` 走 `src`，`IN` 走 `dst`，`BOTH` 合并去重。
- 属性只在结果需要时 detoast，路径搜索本身只处理 ID，避免遍历中的大 JSON 读放大。

### 3.2 multi-AM GC

```text
DML / DDL / queries / snapshots
  → GC coordinator 获取一次 oldest_snapshot_xmin
  → per-AM collect（只读，生成 GcWork）
  → per-AM index cleanup（先 durable）
  → heap compaction / page merge / file GC（后执行）
  → relation/page generation 复核
  → 完成或按新水位重新排队
```

关键裁决：

- 水位在一次 cycle 内固定，不因新快照出现而提前回收。
- 任何 AM 的索引项清理必须先于使 TID 不可读/不可寻址的物理操作。
- 页链或关系 generation 变化时放弃本轮并重新收集，不得在旧工作项上继续回收。
- 后台与 `gc_once` 必须走同一套 collect/reclaim 实现，测试不能靠另一套简化路径通过。

---

## §4 Graph 数据模型与物理格式

### 4.1 单一 GraphItem 邻接关系

每个 graph 对应一个内部 heap relation 和 **identity/out/in 三个 B+Tree 邻接索引**：

```text
GraphItem {
    record_type: Int4,       // 1 = node, 2 = edge
    id: Int8,               // node_id 或 edge_id
    src: Int8,
    dst: Int8,
    properties: Jsonb
}

node:  record_type=1, id=src=dst=node_id
edge:  record_type=2, id=edge_id, src=source_node_id, dst=target_node_id
```

**GraphKeyCodec（当前 B+Tree 只支持单列 key，禁止假设 composite Datum）**：

```text
identity = record_type:u32 + id:u64
out      = record_type:u32 + src:u64 + id:u64
in       = record_type:u32 + dst:u64 + id:u64
```

- 字段继续使用既有 sign-flipped big-endian，保证字节序等于原生整数序。
- `identity` 同时服务 `record_type=node AND id=node_id` 的节点查重和 `record_type=edge AND id=edge_id` 的精确删除，不再依赖 src/dst 扫描。
- `out`/`in` 的 `(neighbor_id,edge_id)` 顺序由索引天然提供，不需要额外内存排序；遍历时只取 `record_type=edge` 前缀，node 记录不会混入边结果。

选择单一 relation 而非 nodes/edges 两张表，原因：

- 节点、边和属性共享一个 WAL/快照/GC 生命周期，避免跨关系原子性协议。
- 节点删除可以在一个事务内级联删除全部关联边，杜绝 dangling edge。
- identity/out/in 三个 GraphKeyCodec 索引即可支持 OUT/IN/BOTH；`record_type` 区分节点和边，不与 HNSW 的 NodeId 或 heap TID 混用。**所有读取必须同时带 `record_type` 过滤**：节点查重/存在性用 `record_type=node AND id=node_id`，邻接与级联删除用 `record_type=edge AND (src=node_id OR dst=node_id)`，禁止把节点当成自环边。

**Graph 专用派生索引维护路径**：引擎现有 `collect_index_keys` 假设“一个索引对应一列的值”，不能直接复用于从整行派生字节键的三个 Graph 索引。Stage A 必须在 GraphStore 与 engine 的适配层提供独立维护入口，从完整 GraphItem 统一派生 identity/out/in key，并让建索引、DML 插入/更新/删除、失败补偿（loser cleanup）和 GC 清理全部复用同一 codec 与维护逻辑，禁止退化为按某一 Datum 列生成键。回收时先从尚未失效的 GraphItem 提取三套键，完成三个 B+Tree 的索引删除并 durable，之后才允许 heap compact、unlink 或 free；属性更新若三套键均未变化，不做无意义的索引删除/重插。

`GRAPH_AM_OID=9880` 注册到 `pg_am`。Graph OID 同时是内部 heap relation OID；GraphEntry 至少包含 `oid/name/directed/first_page/edge_sequence_oid`。

### 4.2 Graph side file 与 edge ID

采用 **Graph side file** `graph/<oid>/metadata.bin`，复用 inverted analyzer/manifest 的 version+CRC+原子写入先例，**不新增 `pg_graphs` 系统表、不改 bootstrap/catalog**：

```text
GraphMetadata {
    format_version,
    oid,
    name,
    directed,
    first_page,
    edge_sequence_oid,
}
```

- `CREATE GRAPH <name> [DIRECTED|UNDIRECTED]` 在一个 DDL 事务内创建 heap relation、identity/out/in 三个邻接索引、GraphEntry、metadata.bin 和 edge-ID sequence。**graph name 必须唯一**，重名 CREATE 返回 `IndexExists`，DROP 后允许复名。
- **sequence high_water 是 edge ID 的唯一权威**，side file 不另存 `next_edge_id`，避免双事实源。GraphEntry 只引用 `edge_sequence_oid`；崩溃或回滚允许空洞，禁止重用。
- sequence 与 graph 同生命周期，DROP 时按现有 SERIAL ownership 规则清理。
- 节点 ID 由调用方提供并强制唯一；edge ID 由 engine 分配。自环合法，BFS/DFS 由 visited 防死循环。
- side file 不存在按无 Graph 处理；损坏或格式版本不支持则 fail-stop，不得从 `pg_class` 猜 Graph 类型。
- `pg_rust_relpages.first_page` 是 heap 页链权威；side file 的 `first_page` 只保存 Graph 注册快照。Stage A reopen 必须比对两处，不一致按 `Corrupted` 拒绝，禁止用 side file 覆盖目录。

这样避免改动 `SYSTEM_TABLES` 固定页 1–6 的 bootstrap 协议，也不把 Graph 元数据建立在首页读取/页链兼容性的不确定性之上。

### 4.3 JSONB 类型

新增 `ColumnType::Jsonb` 与 `Datum::Jsonb(String)`：

- 写入时通过 `serde_json::from_str` 校验，非法 JSON 响亮拒绝；保留原始合法 JSON 文本，不做键排序或数值重写。
- 作为 varlena 复用现有 TOAST，大属性值可外置。
- PG Wire 使用 JSONB OID **114** 的文本表示；二进制 JSONB 不在 5b-1 范围内。
- 参数化 Graph start ID 使用 INT8；Graph properties 使用 JSONB 文本参数。
- **DataFusion/Arrow（归 Stage B）**：在 `arrow_column`、`array_to_value`、`df_filter` 白名单注册 JSONB，仅以 **Utf8 作为 JSON 文本传输类型**；输入/输出仍做合法性校验，禁止在 engine SQL 层隐式 TEXT 转换。按 plan schema 的字段元数据识别 JSONB，只允许纯投影（含表别名、派生表、CTE）与 `IS [NOT] NULL`；比较、CAST、函数、排序、分组、窗口、JOIN key、DISTINCT/UNION/CASE 等一律返回 `Unsupported`，不得借 Utf8 偷换语义。
- **输入兼容边界**：使用 `serde_json` 默认递归深度与数值范围限制；超过限制的输入（如约 200 层嵌套或 `1e400`）即使符合 JSON 标准也会被拒绝，错误沿用 `invalid JSONB literal`，不承诺与 PostgreSQL JSONB 的全部合法输入兼容。
- **二进制结果边界**：客户端对 JSONB 列请求二进制结果格式时，整条查询返回 `Unsupported`；不像 PostgreSQL 对无二进制发送函数的类型回退文本。常用驱动的默认文本路径不受影响。
- **旧目录边界**：缺少 JSONB `pg_type` 行的旧目录不能创建存储 JSONB 列，本阶段没有补写该行的迁移路径；错误只报告该目录格式不支持 JSONB，不暗示已有迁移能力。
- `jsonb` 与 TEXT、VECTOR 一样保留为类型名关键字，不能作标识符；本阶段不额外做标识符上下文化。
- `path` 结果由 engine 生成确定性 JSON 数组，例如 `[1,4,9]`，不要求调用方重新解析节点属性。

不引入 JSONB 比较、下标、路径查询或隐式 TEXT 转换。

---

## §5 DDL、DML 与查询语法

### 5.1 最小语句集

```sql
CREATE GRAPH g DIRECTED;
GRAPH ADD NODE g 1 PROPERTIES '{"name":"A"}';
GRAPH ADD NODE g 2 PROPERTIES '{"name":"B"}';
GRAPH ADD EDGE g 1 2 PROPERTIES '{"kind":"order"}';
GRAPH UPDATE NODE g 1 PROPERTIES '{"name":"A2"}';
GRAPH DELETE EDGE g 7;
GRAPH DELETE NODE g 2;

GRAPH BFS g 1 OUT MAX_DEPTH 3;
GRAPH DFS g 1 BOTH MAX_DEPTH 2;
```

约束：

- `DIRECTED` 为默认；`UNDIRECTED` 在遍历时把边视为双向，但不复制物理边。
- 方向必须显式选择 `OUT`、`IN` 或 `BOTH`；`MAX_DEPTH` 必须显式指定为 0–3，缺省方向或深度均响亮拒绝。
- `GRAPH UPDATE NODE` 只更新节点属性；修改节点 ID 必须显式删除并重建，避免隐式级联更新。
- `GRAPH DELETE NODE` 同事务级联删除全部关联边；`GRAPH DELETE EDGE` 按 `edge_id` 精确定位。
- Graph DML 使用普通 auto-commit 或显式事务，遵守现有隐式/显式事务失败语义。

**QueryStats 与 EXPLAIN**：Graph 查询进入 SQL text path 统计，新增 `ExecutionPath::GraphTraversal`；`EXPLAIN GRAPH BFS` 输出 direction/max_depth、identity/out/in 索引及估计候选数，不得漏报或伪装成普通 SeqScan。

**Graph 完整性与锁协议**：`GRAPH ADD NODE`、`GRAPH ADD EDGE`、`GRAPH DELETE NODE` 在整个 DML 临界区使用 `AccessExclusive`，序列化所有同类写操作，不能只依赖 B+Tree 去重或事务隔离：

- `ADD NODE` 以 `record_type=node AND id=node_id` 查重再插入；发现重复 node ID 即返回 `InvalidArgument`，检查与插入不得离开该锁，不能把同名 edge 当成节点。
- `ADD EDGE` 以 `record_type=node AND id=node_id` 验证 `src`、`dst` 两端节点均存在，再插入；任何一端不存在都拒绝，检查与插入不得离开该锁。
- `DELETE NODE` 以 `record_type=edge AND (src=node_id OR dst=node_id)` 枚举并级联删除关联边，再删除节点；其他事务不能在其后补插新边。
- `GRAPH UPDATE NODE` 与 `GRAPH DELETE EDGE` 使用 `RowExclusive`；锁顺序统一为 **table lock → sequence lock**，禁止反向获取。`AccessExclusive` 已覆盖 Graph 完整性写临界区，不再引入未定义的 graph write gate。
- 该串行化是首版有意接受的写并发降级。必须补“并发重复节点”“ADD EDGE 对不存在节点”“DELETE NODE 与并发 ADD EDGE”三组回归，证明不会产生重复节点或 dangling edge。

### 5.2 遍历结果

内部统一返回 `GraphHit { source_tid: Tid, node_id: Int8, depth: Int4, path: Jsonb }`，当前 SQL 投影仍为 `(node_id,depth,path)`：

- `source_tid` 是 GraphItem 的 heap TID，供未来 Fusion 做统一可见性回表，不把 node_id 当作通用 TID；当前 PG Wire schema 不变。
- BFS：按 `(depth,node_id)` 输出最短路径；同层按节点 ID 升序。
- DFS：按前序输出，邻接按 `(neighbor_id,edge_id)` 升序，确保字节级可复现。
- `path` 是从 start 到当前节点的节点 ID 数组，包含 start 和当前节点。
- 邻接项只在去重后进入队列；`visited` 使用有界 `HashSet/HashMap`，总大小受当前遍历触及的节点数限制。
- 不存在的 start、非法深度、非 Graph 名称均返回结构化错误；空图返回空结果集而非异常。**Stage B 必须在 engine 的执行与 EXPLAIN 两条路径都校验 graph 注册表，并把 wire 的 relation 存在性预检扩展到 `GraphBfs/GraphDfs` 及 EXPLAIN 包裹形态**；Stage 0 尚无注册表，只允许冻结计划形状，不得把当前 `EXPLAIN GRAPH ... nosuchgraph` 成功当作最终语义。

**遍历资源预算**：`GraphTraversalOptions { max_results, max_visited_nodes, max_response_bytes }` 是硬性预算，而不是“图有多大就能用多少内存”。默认上限分别为 100,000 条结果、1,000,000 个 visited 节点和 16 MiB 响应字节；构造 path、队列和响应前都先检查预算，超限返回 `Capacity` 并附可重试/分页提示，不把部分结果伪装成完整结果。预算配置只允许 engine config，不引入新的 SQL 语法。后续若支持分页，resume token 必须包含 `(direction,last_depth,last_node_id)`，且保持相同排序和去重语义。

### 5.3 Time-range query routing（接口预留）

ROADMAP 将 time-range routing 归为 Phase 4a 的 planner predicate routing。**规范化在 Stage B 实际落地**，不只是文档预留；但不提前实现 TimeSeries AM：

- `created_at BETWEEN low AND high` 规范化为 `created_at >= low AND created_at <= high`，两端均要求合法 TIMESTAMPTZ；开区间与 `NULL` 语义保持与现有 predicate 一致。
- 为未来的 `AmKind::TimeSeries` / `ScanSpec::TimeSeries` 保留注册位置，Phase 5a 落地前由精确 SeqScan 处理，不伪造 TimeSeries 路径、统计或索引结果。
- 多个时间范围条件只允许 AND 组合；OR、跨列比较和非 `BETWEEN` 的复杂表达式沿用 Phase 4a 的精确回退，不提前扩展 planner。

---

## §6 multi-AM GC coordinator

### 6.1 合约类型（`pg-storage::gc`）

`pg-storage` 只定义与 AM 无关的类型，避免依赖倒置：

```text
GcRelation { rel_oid, first_page }
GcRequest<'a> {
    relation: GcRelation,
    horizon: TxnId,
    base_generation: u64,
    max_pages: usize,
    max_estimated_bytes: u64,
    start_after: Option<PageId>,
    clog: &'a dyn ClogAccessor,
}
GcPageWork {
    page_id: PageId,
    dead_tids: Vec<Tid>,     // 仅当前页、严格升序，受页内 slot 数硬上限约束
    estimated_bytes: u64,
}
GcWork {
    relation: GcRelation,
    horizon: TxnId,
    base_generation: u64,
    pages: BTreeMap<PageId,GcPageWork>,  // 按预算增量收集，page_id 唯一，禁止重复回收
    next_page: Option<PageId>,           // 下一批起点；None 表示收集完成
}
GcStats {
    dead_tuples,
    index_entries_removed,
    heap_pages_compacted,
    heap_pages_merged,
    heap_pages_freed,
    toast_chunks_reclaimed,
    segment_files_removed,
    work_items,
}
GcParticipant {
    fn collect(&self, request: GcRequest) -> Result<GcWork>;
    fn reclaim(&self, work: GcWork) -> Result<GcStats>;
}
```

- `collect` 只读，负责 dead-TID 与各 AM 的清理工作收集，不得修改页或索引。收集按页/字节预算增量进行，任何时刻只保留当前批次，禁止先构造全关系 `dead_tids`。`pages` 以 page_id 去重并复用同一页序执行 reclaim，禁止重复 compact/unlink/free。
- `reclaim` 执行物理操作，必须先完成对应索引清理并使其 durable，再做 heap/page/file 失效操作。
- AM 内部可以使用自己的 schema、Datum、page 格式和统计；trait 不暴露这些类型，因此 `pg-storage` 不依赖 AM。

Heap 的 `scan_dead_tuples` / `collect_index_keys` / `reclaim` 语义原样复用；`GcParticipant` 是调度外壳，不重写既有算法。

### 6.2 调度与 backpressure

- 每个 Engine 一个 GC worker，Condvar 唤醒；无任务时动态退避，关闭时停止并 join。
- 有界 `BTreeMap<relation_oid, GcWorkItem>` 去重排队。同关系工作合并时取 `min(old_horizon,new_horizon)`，**必须重新 collect，禁止直接拼接 dead set**；这样不会因采用较新 horizon 而误删旧快照仍需的版本。
- `gc_once` 与 worker 互斥执行，测试可精确驱动每个阶段。
- 大 work item 按页拆批，每批有最大页数/字节数；完成后让出执行权，避免长事务和锁等待。
- DROP、relation generation 改变或 Engine shutdown 时取消对应工作；DROP 后不得重新发布旧 handle。
- 后台异常只记日志并保留待重试状态，不能把未完成回收伪装成成功。

### 6.3 水位与 TOCTOU 复核

每轮固定一次 `oldest_snapshot_xmin()`：

1. collect 前取 horizon；所有 dead 判定使用该 horizon。
2. 索引清理前复核 relation/page generation 未变。
3. 物理回收前再次复核；若 generation 已变，丢弃该页/关系本轮工作并重新排队。
4. relation drop 时提升 generation 并取消队列；页复用必须同时通过 owner、generation 和链可达性检查。

`relation_generation` 仅在 DROP 和页链结构变化（新页入链、unlink、free）时单调递增，普通 INSERT/UPDATE/DELETE 不递增，避免持续写入让每批 GC 都作废。页复用还必须有独立 `page_generation`，不能只靠 PageId 判断新租户。

---

## §7 online heap/B+Tree 回收协议

### 7.1 通用顺序

每个页或页组遵循：

```text
collect dead + index keys
→ 各 AM 删除索引项并 durable
→ heap page compact
→ 必要时 merge empty leaf / unlink
→ generation + chain reachability 复核
→ versioned PageFree
```

此顺序同时满足 §4.1 的“索引清理先于 TID 失效”和崩溃时的可恢复性。不得先 compact 再提取 key。

### 7.2 B+Tree 空页合并的安全降级

直接删除父 downlink 与合并左右 sibling 会破坏既有 DOWN/RIGHT 锁序，因此首版采用明确的安全降级：

- 普通 dead-entry 清理和页内 compaction 按页短锁执行。
- **只有空 leaf 需要修改父 separator/downlink 时**，进入短的关系级 `AccessExclusive` 窗口，暂停该关系新写入，完成 sibling 复核、父项更新、unlink 和 `PageFreeV2`。
- 窗口按单页/单批次限制并记录最大等待与暂停时间；失败立即释放锁并保留未合并状态，不能形成半合并页。
- 空 root、非空 sibling、父 separator 不存在或 sibling generation 变化时，不做合并，只保留空 leaf。

这是有意的正确性取舍：以可测的短停写换取父指针一致性，不在首版伪造“全锁-free B+Tree merge”。

### 7.3 `BTreePageCleanup=60` 与 `PageFreeV2` 版本化恢复

父 separator/downlink 不再放入 heap 专用 `HeapCleanup`。新增 **`BTreePageCleanup=60`**，专门承载 B+Tree 父页更新并避免混淆 redo 分派和 DPT 页分类：

```text
BTreePageCleanup {
    parent_page_id,
    parent_slot,
    new_separator_key,
    new_child_page_id,
    sibling_page_id,
    base_generation,
    post_unlink_generation,
    operation_id,
}
```

- 新占号 60 是有意的格式扩展，必须登记原因、golden bytes 和 redo 负例；既有判别值不得重编号。
- `parent_slot` 唯一定位父项，`new_child_page_id` 指定替代 downlink；`PageId::INVALID` 仅表示删除 downlink，此时 `new_separator_key` 必须为空。更新 separator 时允许空 key（最左 downlink），不得再用空 key 隐式表示删除。
- `sibling_page_id` 记录左 sibling 的 next 指针修改，Analysis/DPT 必须同时登记 parent 与 sibling，禁止只更新父页而遗漏 sibling。
- `HeapCleanup` 继续只负责 heap 页内 compact/unlink；B+Tree 父页更新只能走 `BTreePageCleanup`。
- `PageFreeV2` 仍只负责最终释放，不承担父 separator/downlink 修改。

### 7.4 `PageFreeV2` 版本化恢复

新增版本化 payload（不改变旧 `PageFree=41` 判别值；版本写入 payload 自身）：

```text
PageFreeV2 {
    version: 2,
    relation_oid,
    page_id,
    prev_page_id,
    next_page_id,
    base_generation,
    post_unlink_generation,
    operation_id,
}
```

**operation_id 与 generation**：

- `operation_id = (relation_oid, base_generation, page_id)`，在 collect 开始时确定，不依赖额外高水位。它唯一标识同一代际、同一页的本次回收；失败重试复用同一 ID，成功后不会再次收集到已释放页。
- `base_generation` 是 collect 开始时的关系代际；`post_unlink_generation` 是 unlink 完成后的关系代际，无 unlink 时等于 `base_generation`。
- 这样 redo 能区分“compact 后的旧链状态”和“unlink 后的新链状态”，不会把本次操作自身推进的代际误判为其他写者。

**逐状态恢复协议**：

| 状态 | 崩溃后的恢复动作 |
|---|---|
| 尚未 collect | 按当前 horizon 重新 collect，不执行回收 |
| index cleanup 已 durable、compact 未 durable | 幂等重放索引清理，再 compact；不得 free |
| compact 已 durable、unlink 未 durable | 页仍在链内，只在 `base_generation` 匹配时继续 merge/unlink；不得 free |
| unlink 已 durable、PageFreeV2 未 durable | 链已跳过目标页，只有当前代际等于 `post_unlink_generation` 且 prev/next 匹配时才 free |
| PageFreeV2 已 durable | 以 `operation_id` 幂等跳过，不重复 free、不重复改链 |

补充规则：

- 若未完成操作的当前代际已不同于预期代际，说明存在其他写者或 DROP：放弃并重新 collect，不能继续 free；只有 metadata、CRC 或链结构本身矛盾时才报 `Corrupted`。
- `HeapCleanup` 先改页内容和索引，`PageFreeV2` 只做最终释放。redo 不得根据旧 payload 盲目改链，也不得把“不可达”当成“可直接 free”的唯一证据。
- heap 的 `prev_page_id` 指前驱页，`next_page_id` 指后继页；B+Tree 对应左右 sibling。父 separator/downlink 在 `BTreePageCleanup=60` 中先更新，`PageFreeV2` 只验证，不再承担父指针修改。
- compact 与 unlink 之间页仍在链内，unlink 与 free 之间页已不可达，因此任意崩溃点都只产生可重放的中间态，不会因单页提前释放而让整链断裂。

旧 `PageFree` 继续用于历史记录；新 online merge 一律使用 V2。

---

## §8 各 AM 的回收分工

| AM | collect/reclaim 方式 | 明确边界 |
|---|---|---|
| Heap | 复用 dead scan、HOT 分组、compact/unlink/free | 部分死亡 HOT 链不裁剪；索引清理先于 compact |
| B+Tree | 删除死 key、页内压缩、空 leaf merge | 父指针修改只走短 AccessExclusive；不做不安全的双向锁序 |
| HNSW | 复用 tombstone、redirect、dead-row channel、map 水位清理 | 不重建图、不改变既有 recall；不新增 HNSW→heap 依赖 |
| Inverted | segment merge 清除 tombstone、manifest 原子替换、水位门控回收 | 不把 posting 当 heap tuple vacuum；不跳过 durable coverage |
| Graph | 专用入口从死 GraphItem 派生 identity/out/in 三键，先令三个 B+Tree 删除 durable，再走 heap compact/unlink/free | 禁止复用普通单列 `collect_index_keys`；节点删除级联边；Graph metadata/sequence 同步回收 |

统一 coordinator 只负责调度和水位，不把各 AM 的专用回收算法扁平化成不安全的公共操作。

---

## §9 并发、DROP 与故障语义

### 9.1 快照与读者

- Graph 遍历与 GC 都使用真实 Snapshot/Guard；旧快照下被删除的节点和边仍需按该快照可见。
- 锁-free reader 在页链变化前后检查 generation，变化则重读或回退到精确路径，不能读取新租户的旧版本。
- GC 不用旧快照“帮助”新查询，所有物理删除只对最老快照之后的新读者可见。

### 9.2 DROP 生命周期

- `DROP GRAPH` 与 `DROP TABLE` 共用 commit marker → registry/handle 清理 → 文件/页面回收顺序。
- 先停止新工作并等待 in-flight traversal/GC，再清理 GraphEntry、identity/out/in 索引、GraphItem、metadata 和 sequence。
- 清理失败保持 `leak, never corruption`，以 warning 和后续 GC 重试处理；不得让旧 graph 重新出现在 registry。
- 新建 graph 不复用旧 OID，因此旧目录和旧 sequence 不得复活。

### 9.3 故障分类

- 可重试 I/O、锁等待和容量不足：保留工作项，按 backpressure 重试。
- 元数据、链、payload 或 generation 不一致：`Corrupted`，停止相应关系的回收，不继续扩大损坏。
- WAL 已被其他回收下界覆盖：响亮报恢复/重建错误，不能静默跳过。

---

## §10 性能与验收口径

### 10.1 必测场景

- 固定 **15,625 nodes × 64 edges = 1,000,000 edges** 的邻接图，包含自环、重复边和无向图；枢纽节点度数上限 **256**，同时记录平均/最大度数与数据/查询 SHA256，避免以无限 hub 改变验收口径。
- BFS/DFS OUT/IN/BOTH、0/1/2/3 跳、空图、缺失 start、重复邻接与确定性输出；单节点零边不得出现自环，仅有 edge 记录不得误判 node 存在。
- 属性为 JSON `null`、空对象 `{}`、嵌套对象、大 JSON/TOAST、非法 JSON；SQL `NULL` 不是合法属性输入。
- 节点/边新增、更新、删除、级联删除、回滚、旧快照、DROP/重建。
- heap/B+Tree/HNSW/inverted/Graph 同时存在的跨 AM 回收。
- 长快照阻止回收、部分 HOT 链、页复用、父 separator 改变、崩溃于 compact/unlink/free 前后。
- worker 反复启停、队列上限、generation 改变、DROP 与回收竞争。

### 10.2 硬指标

| 项目 | 验收口径 |
|---|---|
| Graph 遍历 | 百万边，3 跳 **<50ms**（release，固定机器与数据集 hash） |
| 正确性 | 无 TID 泄漏、无 dangling index entry、无跨 AM 重复回收 |
| 恢复 | crash matrix 无丢行、无重复、无半合并页、无错误页释放 |
| GC 停写 | **单页合并窗口 p99 < 100ms**，同时记录总回收时间；不得退化为整表长暂停 |
| 资源 | visited、队列、每批页数/字节数有界；无后台线程泄漏 |
| 一致性 | BFS/DFS 输出、目录恢复、图与索引 reopen 结果确定 |

性能数字必须记录硬件、OS、Rust 版本、数据/查询 hash、并发度和是否 warm cache。不能用微基准小图结果冒充百万边验收。

---

## §11 测试与 CI 注册

- 新 crate `pg-am-graph` 加入 workspace、test/clippy/rustdoc matrix 和 all-features 分支；首版不声明 loom feature。
- `JSONB` 加入 heap tuple、TOAST、pg-wire、DataFusion/Arrow 的穷尽消费面检查，不能只在 Graph 路径验证。
- Graph 测试覆盖 DDL、DML、遍历、属性、恢复、旧快照、DROP、索引一致性与百万边基准。
- GC 测试使用真实多 AM 组合，验证 `collect` 先于 `reclaim`、index cleanup durable 先于 heap invalidation、generation 复核、父指针和 PageFreeV2。
- 使用独立 coverage job（若注册），不得沿用 pg-am-hnsw 的 90% 结果。
- 最终执行 workspace 默认 feature debug/release、新 crate debug/release、DataFusion debug/release、全 feature check、逐 crate Clippy/rustdoc、MSRV 1.86、fmt 和 `git diff --check`。

---

## §12 建议 Stage 拆分

| Stage | 交付与退出条件 |
|---|---|
| Stage 0 | 冻结 GraphItem、GraphKeyCodec、JSONB、Graph side file、`GRAPH_AM_OID=9880`、SQL AST、GcRequest/GcWork、PageFreeV2、`BTreePageCleanup=60` 和 CI；golden tests 先行 |
| Stage A | Graph DDL、GraphItem 持久化、节点/边 DML、identity/out/in 索引、edge-ID sequence、事务与回滚 |
| Stage B | BFS/DFS、属性/TOAST、查询 AST 与 PG Wire、DataFusion/Arrow JSONB、GraphTraversal EXPLAIN、Time-range 规范化、确定性结果、基础 reopen |
| Stage C | GcCoordinator、`gc_once`、heap 与 B+Tree dead-entry 回收、统一水位、后台 worker |
| Stage D | 空 leaf merge、父 downlink、generation 复核、`BTreePageCleanup=60`、PageFreeV2、崩溃恢复与 DROP 生命周期 |
| Stage E | HNSW/inverted/Graph 联合回收、百万边与 3 跳基准、全 crash matrix、压力与资源测试 |
| Stage F | 全量门禁、两轮对抗审查、文档与基准归档 |

任何阶段不得用“后续再补”为由跳过索引清理顺序、快照保护、页复用校验、manifest/chain 原子性或 crash 测试。

---

## §13 备选方案与取舍

| 方案 | 选择 | 理由 |
|---|---|---|
| nodes/edges 两张表 | **否** | 级联删除和统一 GC 更复杂，需额外跨表事务不变量 |
| 单一 GraphItem relation | **是** | 节点/边/属性同事务、同快照、同 GC，邻接索引足够轻量 |
| 自研图页格式 | **否** | 现有 heap/WAL/MVCC 已提供事实源和恢复，没必要再写一套 |
| 每跳全表扫描 | **否** | 百万边 3 跳目标无法保证；src/dst B+Tree 是必要索引 |
| 全锁-free 父指针合并 | **否（首版）** | 既有 DOWN/RIGHT 锁序不支持，短 AccessExclusive 更容易证明正确 |
| 直接复用旧 PageFree | **否** | 跨代页复用需要 owner、generation、链和 operation_id 共同验证 |
| Graph DML 使用 LogicalGraph=102 | **否** | 邻接记录就是 heap 数据，102 仅保留给未来图元数据扩展 |
| Graph 与 GC 同一 crate | **否** | GC 合约必须在 `pg-storage`，避免 HNSW 被迫依赖 heap |

---

## §14 风险与开放问题（登记，不阻塞）

- **Graph DML 串行化**：三类完整性 DML 使用 `AccessExclusive`，写吞吐有明确上限；只有证明该协议正确后，才评估是否需要更细粒度的写锁，不在首版引入未定义的 graph write gate。
- **关系级短停写**：空 leaf merge 的父指针更新需要 `AccessExclusive`，这是首版明确接受的正确性降级；必须测量窗口，不能描述成完全 online/no-pause。
- **多关系 GC 头阻塞**：单 worker 简单可靠，但大关系可能拖慢其他关系；后续按公平调度和页预算优化，不提前引入多 worker 锁竞争。
- **Graph 遍历内存**：已有结果数、visited 和响应字节硬预算，超限返回 `Capacity`，不静默截断；枢纽节点场景仍需后续确定性分页。
- **JSONB 语义**：首版只做合法性校验和文本输出，不保证对象键序、数值精度比较或 JSONPath。
- **R2 行格式升级与多索引 backfill**：交叉引用 `docs/architecture-risks.md` R2。该 RFC 必须单独进行，不在 5b-1 顺手实施；当前 HNSW 的“仅空表可建、每表至多一个索引、不 backfill、行内单 NodeId”约束继续有效，GC coordinator 不得借此扩大范围。
- **超大图遍历**：百万边是验收规模，不是无限扩展承诺；分布式遍历、分页和更复杂图算法不在本阶段。
- **老目录兼容**：side file 不存在时按无 Graph 处理；side file 损坏或遇到未知 `GRAPH_AM_OID` 按未知 AM 处理，不能误认 B+Tree。
- **覆盖率**：若新增 `pg-am-graph` coverage job，需独立阈值与报告，不能借用其他 crate 的数字。

---

## §15 参考资料

- 需求与验收：`ROADMAP.md` Phase 5b（Graph AM、multi-AM GC、3 跳 <50ms、无 TID 泄漏与悬空索引项）。
- AM 接口：`crates/pg-am-heap/src/access_method.rs`。
- 邻接索引与页面：`crates/pg-am-btree/src/{index.rs,page.rs,am.rs}`。
- 堆/TOAST/MVCC：`crates/pg-am-heap/src/{heap_am.rs,slotted_page.rs,toast.rs}`。
- GC 基础：`crates/pg-engine/src/engine.rs` 的 `vacuum`、`oldest_snapshot_xmin`、HNSW dead-row channel，及 `crates/pg-am-inverted/src/segment.rs` 的 merge/coverage。
- WAL 契约：`crates/pg-storage/src/wal/record.rs`、`crates/pg-storage/src/tier2.rs`。
- Sequence/SERIAL：`crates/pg-engine/src/sequence.rs` 与 Phase 4a Stage F 验收记录。

---

## §16 修订记录

| 版本 | 日期 | 内容 |
|---|---|---|
| v1.0 | 2026-11-24 | 初稿：GraphItem、JSONB、Graph DML/BFS/DFS、multi-AM GC、online heap/B+Tree 回收、PageFreeV2、性能与 Stage 拆分。 |
| v1.1 | 2026-11-24 | 审查修复：Graph DML 增加 `AccessExclusive` 唯一性/引用完整性/级联删除临界区及锁顺序；sequence high_water 成为 edge ID 唯一权威，移除 `pg_graphs.next_edge_id` 双事实源。GC 合并改为 `min(horizon)` 后重新 collect，收集按页/字节预算增量进行，禁止全量 dead set 和拼接。遍历增加结果数、visited、响应字节硬预算及 `Capacity` 语义。PageFreeV2 改为 `base_generation`/`post_unlink_generation` 双代际模型，明确 compact、unlink、free 各崩溃点的恢复动作与 `operation_id` 派生。 |
| v1.2 | 2026-11-24 | coding plan 审查修复：GraphItem 全部读取增加 `record_type` 过滤，避免节点被当成自环边、同名 edge 被当成节点；补 Graph AM 的 `build/search/insert/delete/scan/freshness` 契约。`GcWork.pages` 改为 `BTreeMap<PageId,GcPageWork>`，collect/reclaim 共用去重页序，禁止重复回收。内部结果增加 `source_tid` 供未来 Fusion 回表，当前 SQL 投影不变。 |
| v1.3 | 2026-11-24 | 外部审查修复：以 `graph/<oid>/metadata.bin` 替代 `pg_graphs` 系统表，避开 bootstrap/catalog 改动；冻结 identity/out/in 三套 GraphKeyCodec 与 edge_id 精确删除路径。新增 `BTreePageCleanup=60` 隔离 B+Tree 父 separator/downlink 与 heap cleanup。补 JSONB Arrow 映射、Time-range Stage B 归属、Graph name 唯一性、GraphTraversal EXPLAIN、p99<100ms 合并窗口、百万边度数上限与 R2 交叉登记。 |
| v1.4 | 2026-11-24 | 深度审查修复：GC 增加页/字节预算、resume cursor 与 collect-time generation；`BTreePageCleanup` 增加 `parent_slot/new_child_page_id` 并区分空 separator 与 downlink 删除。GraphStore 创建/构建改为幂等且禁止覆盖既有 metadata；DataFusion 显式拒绝 JSONB 比较；Graph 关键字仅在 Graph 语句中保留，普通 SQL 标识符恢复兼容。 |
| v1.5 | 2026-11-24 | 裁决 generation 仅在 DROP、新页入链、unlink、free 时递增，普通 DML 不递增，页复用另用 `page_generation`，避免持续写入导致 GC 活锁。`BTreePageCleanup` 增加 `sibling_page_id` 并纳入 DPT。明确方向/MAX_DEPTH 必填及 JSON `null`/`{}` 属性语义；补 Stage 0 实现修订与依赖锁定。 |
| v1.6 | 2026-11-24 | Stage 0 复审修复：DataFusion JSONB 列判定改为按表限定，修复同名 TEXT 列被误拦并补回归；窗口 partition/order、`~`/`~~`、UNION/CASE 的 JSONB→TEXT 静默降级登记为 Stage B 遗留事项。 |
| v1.7 | 2026-11-25 | Stage 0 复审修复：放弃表名匹配与表达式形状黑名单，改为按 plan schema 字段元数据执行“纯投影 + `IS [NOT] NULL`”白名单；覆盖表别名、派生表、CTE 且保持同名 TEXT 列兼容。窗口 partition/order、`~`/`~~`、UNION/CASE 及其他 JSONB 运算统一响亮拒绝，并补端到端回归。 |
| v1.8 | 2026-11-25 | Stage 0 复审修复：inverted 清理识别共享 `.pg_rust_tmp` 与真实 spool 文件名；CreateGraph 纳入 wire DDL 规则；Stage B 明确 graph registry 对执行/EXPLAIN/wire 的存在性校验。登记 JSONB 递归/数值输入、二进制结果、旧目录和类型关键字边界；GcPageWork 要求 dead_tids 严格升序。Stage A 冻结 Graph 整行派生三键的独立索引维护路径及“索引删除 durable 先于堆失效”顺序。 |
