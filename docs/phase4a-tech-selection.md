# Phase 4a 技术选型：SQL 层 + PG Wire 基础 + 向量 SQL 语义

版本：v1.3（2026-10-28）
状态：技术选型定稿，供 coding-plan 直接引用
前置事实：Phase 1（存储/事务/B+Tree）与 Phase 2（HNSW）已交付并过审；M6 收口项（24h 复跑、CI 覆盖率、tag）并行推进，与本阶段无技术依赖

---

## §1 范围与非目标

**范围（ROADMAP :423-432）**：
1. DataFusion 集成与 TP 适配（含自研 TP 执行器）；
2. 单路索引扫描（planner 为单个谓词选择 B+Tree / HNSW / 倒排（后续）);
3. PG Wire Protocol 基础版（Simple + Extended Query，文本/二进制结果）;
4. EXPLAIN（单路扫描计划 + 代价估算）;
5. heap TOAST chunk I/O（大值溢出存储）;
6. Sequence(SERIAL);
7. 向量 SQL 语义（DDL for HNSW + ANN 检索算子 + 路径 + EXPLAIN，本次补齐）。

**非目标（ROADMAP 同口径，不得越界）**：
- MultiIndexScan / Fusion（归 Phase 4b);
- Extended Query 的完整类型映射（归 Phase 6);
- 跨模态 Planner（归 Phase 4b);
- 向量算术/辅助函数（`array_to_vector`/`vector_dims` 等，归 Phase 6 类型映射完整化）;
- 距离阈值过滤谓词（`WHERE embedding <-> $vec < c`)——ANN 路径的必要性不强，登记为可选二期，不进 4a 验收。

---

## §2 Crate 归属与依赖方向

| 层 | 归属 | 内容 |
|---|---|---|
| SQL 层 | `pg-engine/src/sql.rs`（现有 Statement/Tokenizer 扩展）+ 新增 `pg-engine/src/planner.rs` | 语句 AST 扩展（向量 DDL/ANN 算子）、单路选择器、AM 注册表、执行计划节点 |
| 执行器 | `pg-engine/src/executor.rs`（新增） | 自研 TP 执行器（点查/小范围/ANN TopK）+ DataFusion AP 路径的路由 |
| DataFusion 适配 | `pg-engine/src/datafusion_provider.rs`（新增） | TableProvider 实现（heap 行读取 + `Snapshot`/`SnapshotGuard` 生命周期透传 + filter 下推） |
| 向量 AM 调用 | `pg-engine/src/engine.rs`（现有 `hnsw_search_visible`、`create_hnsw_index_on`） | ANN 执行路径直接复用，不另起炉灶 |
| PG Wire | `crates/pg-wire/src/{session.rs,codec.rs,types.rs}` | Extended Query 消息族、参数绑定、类型映射、二进制结果 |
| heap TOAST | `pg-am-heap/src/toast.rs`（扩展现有指针编解码）+ `pg-engine` 的 chunk 表注册 | 溢出 chunk 表读写 + 主行 20B 指针解析 + MVCC/回收规则 + WAL 复用 `HeapInsert`/`HeapDelete` |
| Sequence | `pg-engine/src/sequence.rs`（新增） | nextval/currval + 目录登记 + WAL 持久化高水位 + checkpoint 优化 |

**依赖方向硬约束**:planner/executor 不得反向依赖 DataFusion 的物理执行器——TP 路径必须在无 DataFusion 环境下可独立运行；`pg-am-*` 保持对上层 SQL 零依赖（与 M6 的 `pg-am-hnsw` 零 `pg_txn` 依赖同型）。**DataFusion 以可选 feature `datafusion` 引入**（默认 TP 路径不启用该 feature，自研执行器无此外部依赖即可编译运行；DataFusion 仅在 AP 路径启用时引入，不作为全 crate 的强制依赖）。

---

## §3 DataFusion 集成策略：TP 适配检查点与双路径执行器

**核心裁决：双路径执行器是一等公民，不是失败兜底。**

- **TP 路径（点查 / 小范围查询 / ANN TopK）**：走自研执行器（`executor.rs`)。点查/小范围职责 = 单个谓词选择 AM → 单路扫描 → 可见性过滤 → 回表；ANN 职责 = metric 匹配 → `hnsw_search_visible` → `LIMIT k`。此路径不经过 DataFusion 的规划链路，点查延迟目标 <5ms。
- **AP 路径（复杂分析查询）**：走 DataFusion。职责 = SQL 解析、逻辑计划、物理计划执行，TableProvider 对接 heap 存储。4a 的 DataFusion 路径不承担 ANN TopK；ANN 始终走自研 TP 执行器。

**TP 适配检查点（ROADMAP :410-421 原文要求，落实为可执行验证）**:
1. DataFusion 单连接点查延迟 <5ms —— 实测不过则 TP 全量走自研执行器；
2. DataFusion 能正确传递事务上下文及其注册生命周期到 TableProvider —— 见 §3.1，不过同样走自研执行器；
3. 任一不满足时启用备用路径，**并在文档里登记实测结果**，不得静默降级。

**偏离登记（对 ROADMAP :410-421 的有意修订）**:ROADMAP 原文是"DataFusion 不达标才启用备用方案"，本选型把双路径定为**默认 TP 就走自研执行器**——理由是 TP 路径的固定规划器成本在任何 DataFusion 达标形态下都不该由点查承担（不只是"不达标才绕开"，而是"本来就不该走")。即使 DataFusion 实测达标，TP 也保留自研执行器，不强制切换；AP 才走 DataFusion。此偏离在 §11 的验收口径中同步成立，不与 ROADMAP 原文的验收表述冲突。

### 3.1 事务上下文与快照注册生命周期（正确性红线）

- 所有 SQL 读路径必须使用 `TxnManager::snapshot` 返回的 **`(Snapshot, SnapshotGuard)` 对**，不得只复制 `xmin/xmax/xip/curcid` 字段，也不得退化为 `Snapshot::everything()`；
- repeatable-read 事务快照的 Guard 由事务/session 持有并与事务同寿；语句级快照的 Guard 必须持有到本次查询的所有 TableProvider scan、DataFusion execution stream 及其派生任务全部结束。TP 与 ANN 路径遵守同一规则；
- DataFusion 的 `SessionContext` 可以传递快照值用于规划，但 Guard 必须有明确的 session/transaction/query owner；开始执行后不得提前 drop，流提前取消或报错时也必须等派生读取任务退出后再释放；
- TableProvider 回表读取统一走 `heap_tuple_visible`（与 M6 `hnsw_search_visible` 的第 3 步同一函数），保证"走了索引"与"可见性正确"同时成立；
- **回表过滤语义以 engine 的 visibility oracle 为准，不由 DataFusion 重新解释**——这是 M6 已验证语义的复用，不重写。Guard 的存在同时保证 vacuum 的 `oldest_snapshot_xmin` 不会越过仍在执行的读者。

### 3.2 filter 下推与 ANN 边界

- B+Tree 谓词下推 = 直接走 B+Tree 的 `index_lookup`/`range_scan`;
- 其他谓词（等值/范围）下推失败时回退到 TableProvider 全扫 + 上层过滤，不静默错用索引；
- **ANN 不是 filter**：`ORDER BY distance LIMIT k` 是 TopK 计划形状，普通 TableProvider filter pushdown 不能表达它。4a 不把 ANN 塞入 DataFusion filter 接口，也不要求新增 DataFusion optimizer/physical plan；§5 的 ANN 一律经自研 TP 执行器调用 `hnsw_search_visible`。

---

## §4 SQL 层与单路索引扫描

### 4.1 AM 注册表可插拔（硬约束）

planner 以 **AM 注册表 + 单路选择器** 组织，禁止 if/else 按 AM 类型硬编码分发：

```rust
enum AmKind { BTree, Hnsw, Inverted /* Phase 3 注册 */ }
struct AmRegistration {
    kind: AmKind,
    matches: fn(&ScanRequest) -> bool,
    scan: fn(&ScanRequest) -> Result<ScanPlan>,
}
```

- Phase 4a 交付 `BTree`/`Hnsw` 两项注册；Phase 3 的倒排 AM 后续注册即插入，**无需修改 selector/executor 的核心分派结构**。新增 AM 所需的注册项、token/AST（例如 `@@`）和语法测试仍属于对应阶段工作（Phase 4a 验收不含倒排路径，倒排路径验收归 Phase 4b)。
- 每个注册项用 `matches` 自判谓词形状，selector 只遍历注册表并分派，不直接匹配 AM 专属 AST：等值/范围（B+Tree)、ANN 算子（HNSW)、全文 `@@`（倒排，后续）、时间范围（TimeSeries，后续）。`ScanRequest` 提供索引列和 AM 参数元数据，`ScanSpec` 唯一表示实际执行路径；Seq/BTree 节点自带 predicate/order_by/limit，HNSW 节点自带 query，执行器无需回读原始 Statement 恢复扫描形状。

### 4.2 单路选择 + cost hooks（非完整 CBO)

- 单谓词 → 单 AM 路径，按 ROADMAP 的"单路索引扫描"交付，不做多路代价比较；
- cost hooks 仅保留最小集合：B+Tree 基础统计（行数/页数）、HNSW ef/k 与候选池参数、后续 AM 的选择性估计接口预留。代价估算进 EXPLAIN，不做真实 CBO（归 Phase 7c)。

### 4.3 执行计划节点与 EXPLAIN

- 计划节点 = 单路扫描节点（`BTreeScan` / `HnswScan` / `SeqScan`)+ 过滤 + 回表；
- EXPLAIN 输出：路径类型、谓词、AM 参数（B+Tree 的页数估计、HNSW 的 ef/k/candidates/max_retry_rounds)、代价估算；HNSW 路径额外输出 `HnswScan (ef, k, candidates, max_retry_rounds=3)`（见 §5)。

---

## §5 向量 SQL 语义（本次补齐）

### 5.1 HNSW 索引 DDL

- 语法（唯一规范形式）:`CREATE INDEX <name> ON <table> USING hnsw ( <column> <opclass> )`，与 PostgreSQL/pgvector 的 `USING` 位置和 opclass 形态一致。不引入 `CREATE HNSW INDEX` 的第二 AST 形态（别名不支持，避免两套解析路径）。**统一 `CREATE INDEX` 文法**：`name` 与 `USING` 均可省略——缺省 `USING` 即 btree（兼容既有 `CREATE INDEX ON t (col)` 形态，PG 默认行为），`USING hnsw` 则强制显式 opclass；两者共用同一解析路径，不另起第二套语法。
- 4a 支持且要求显式指定 opclass：`vector_l2_ops` → `Metric::L2`、`vector_cosine_ops` → `Metric::Cosine`、`vector_ip_ops` → `Metric::InnerProduct`。未知 opclass 响亮报错，禁止默认成 L2 后创建错误 metric 的图。
- SQL/catalog 层登记 `<name>`，并将 opclass 映射为 `Metric` 后调用或适度扩展 `Engine::create_hnsw_index_on`；图构建、空表校验、每表至多一个向量索引、列类型/dim 校验继续复用现有 typed method，selection 默认 `NeighborSelection::Heuristic`，不另写第二套 AM 创建逻辑。
- **M6 约束如实继承**：仅空表可建、每表至多一个 HNSW 索引（行内单 NodeId 载体）、索引创建不 backfill（后续阶段再议，不归 4a)。

### 5.2 ANN 检索算子与 ORDER BY

| 算子 | SQL 距离语义（ASC 为最近者优先） | 执行路径 |
|---|---|---|
| `embedding <-> $vec` | 欧氏距离 `sqrt(Σ(aᵢ−bᵢ)²)`；HNSW 内部使用顺序等价的平方 L2 | metric 匹配的 HNSW ANN / SeqScan + 暴力欧氏距离排序 |
| `embedding <=> $vec` | Cosine 距离 `1 − cosine_similarity` | `Metric::Cosine` 的 HNSW ANN / SeqScan + 暴力 cosine 距离排序 |
| `embedding <#> $vec` | **负内积距离** `−a·b`，不是原始内积；ASC 等价于最大内积优先 | `Metric::InnerProduct` 的 HNSW ANN / SeqScan + 暴力负内积距离排序 |

- **metric 匹配是 ANN 路径硬门槛**：只有查询算子与索引 opclass/metric 一致时，`ORDER BY <distance> ASC LIMIT k` 才能调用 `hnsw_search_visible(table, query, k, ef, snap)`。无索引或 metric 不匹配一律走 SeqScan + 同一 SQL 距离语义的暴力排序，禁止借 L2 图回答 Cosine/IP 查询。
- 查询快照及其 Guard 遵守 §3.1：保留 M6 的 repeatable-read 语义，禁止退化为 `Snapshot::everything()`，Guard 存活至 ANN 回表和结果流结束。
- 若 SQL 直接投影距离表达式，返回值必须符合上表：`hnsw_search_visible` 返回的 `f64` 是**内部 metric 距离**（L2 下即平方 L2，`Metric::L2` 为 `l2_squared`），SQL 投影时 `<=>`/`<#>` 已是目标距离无需换算、投影 `<->` 须 `sqrt` 还原欧氏距离；平方 L2 仅作为图内排序键，绝不作 SQL 返回值。ANN 与 SeqScan 仅允许召回/性能策略不同，不得产生不同的距离值定义。
- `LIMIT k` 是 ANN 路径的必要参数：k 决定返回行数；ef 默认取索引的 `ef_search_default`，候选池按 M6 §7 的 2x 起步。**缺 LIMIT 时一律回退到 SeqScan + 暴力距离排序**；**`ORDER BY` 非 ASC（如 `DESC` 最远优先）同样回退 SeqScan**——HNSW 是最近邻索引无法服务最远优先，ANN 触发形仅限 `ASC`。
- 4a 不为带额外普通过滤谓词的查询构造“ANN TopK 后再 post-filter”的近似计划：凡同时含额外 `WHERE`，一律 SeqScan → 普通过滤 → 距离排序 → LIMIT，避免过滤后结果不足 k 或改变 SQL 语义。多谓词与 ANN overscan/补轮归后续阶段；距离阈值过滤仍按 §1 非目标处理。

### 5.3 EXPLAIN(HNSW 路径）

普通 EXPLAIN 输出 `HnswScan (ef, k, candidates, max_retry_rounds=3)`：索引 ef/opclass、k 值、首轮候选数、M6 五步管线的最大重试轮数，以及代价估算（§4.2 hooks)。实际 `retry_rounds` 只有执行后才能获得，4a 普通 EXPLAIN 不伪造该值；若后续增加执行统计，再以独立字段输出实际值。

---

## §6 PG Wire Protocol 基础版

在现有 Simple Query(session.rs 的 `'Q'` 状态机）之上补 Extended Query:

- **消息族**:Parse / Bind / Describe / Execute / Sync / Flush / Close（Prepared Statement 生命周期）；错误响应沿用 `WireError::sqlstate_of` 的 SQLSTATE 映射。
- **参数绑定**:`$1..$n`，类型映射 `Int4 / Int8 / Text / Vector(f32[])`(VECTOR 经 `[a,b,c]` 文本字面量编码为 f32 数组，与 Stage C 的 VECTOR 字面量同一解码路径）。Parse 提供参数类型 OID；**Bind 的 parameter format codes 决定参数编码**。
- **结果格式**：文本 + 二进制，**按 Bind 的 result format codes 选择**；Describe 只返回 ParameterDescription/RowDescription 等描述信息，不决定 Execute 的结果编码。Simple Query 保持现有文本格式不变。
- **statement/portal 生命周期**：新的 Parse 替换 unnamed statement，新的 Bind 替换 unnamed portal；named statement/portal 响应 Close，portal 在事务结束时清理，所有会话状态在连接结束时清理。Flush 只刷新输出，不代替 Sync。
- **错误恢复红线**：Extended Query 任一消息失败后，在 Sync 到达前忽略后续消息（Sync 本身除外）；处理 Sync 后发送携带正确事务状态的 ReadyForQuery。不得在错误后继续 Execute 同一批消息，也不得由 Close/Flush 隐式完成 Sync 恢复。
- **事务拦截沿用现有规则**:BEGIN/COMMIT/ROLLBACK 仍不进 `Engine::exec`（与 Simple Query 同型，见 session.rs 的事务拦截注释）。
- **非目标（归 Phase 6)**:COPY、TLS、认证、完整类型映射、取消请求处理。

---

## §7 heap TOAST chunk I/O

- **触发与装页**：满足任一条件即选择可溢出 attribute：①单值序列化后超过现有 `TOAST_TUPLE_THRESHOLD`（2048B）；②内联主行总长度超过 `MAX_TUPLE_BYTES`（堆层页容量扣除页头/槽位等开销后的实际上限）。对②必须逐个大值外置（优先大 attribute）并重新计算主行大小，直到能够装页；因此多个 attribute 各自低于阈值但合计超页也必须处理。全部可溢出值均外置后仍超页才返回 `TupleTooLarge`。
- **指针布局（沿用现有格式，冻结不得改动）**：20B = `vl_len_(u32，含 external tag) + va_rawsize(u32) + va_extsize(u32) + va_valueid(u32) + va_toastrelid(u32)`，字段偏移和 little-endian 编码以 `pg-am-heap/src/toast.rs` 为唯一实现来源。`va_toastrelid` 指向隐式 TOAST 关系，`va_valueid` 标识一个 chunk group；不得改成 `u64+u64+u32` 的新布局。
- **chunk 表结构**：**chunk 表为 `pg-am-heap/src/toast.rs` 的内部结构，不占系统目录行**(catalog 单页容量上限与每表一个隐藏 pg_toast 关系冲突的规避——不新增系统表，rel_oid 仅经 catalog 的 `oid_allocator` 分配）。chunk 行的逻辑键为 `(va_valueid, chunk_seq)`，`chunk_seq` 从 0 连续递增，单 chunk payload 不超过现有 `TOAST_MAX_CHUNK_SIZE`（2000B）。同一 valueid 下按 chunk_seq 排序拼接；缺少、重复或乱序 chunk 必须作为损坏响亮报错，不得静默返回截断值。
- **MVCC 与更新（正确性红线）**：chunk 是带标准 heap tuple 头的隐藏 heap 行，与主行使用**同一 xid、同一 Snapshot/visibility oracle**。UPDATE 产生新 valueid/chunk group，不得原地覆盖旧快照可见的 chunk；主行各版本通过各自指针引用对应 group。读路径必须使用当前查询快照逐行判断 chunk 可见性，不得使用 `Snapshot::everything()`。
- **写序（崩溃一致性红线）**：**chunk 表先写、主行后写**，二者属于同一事务。chunk 插入与主行插入是两条独立 WAL 记录（各自 WAL-first);WAL 前缀性质保证主行指针可达时对应 WAL 已持久化，redo 按同一顺序重建。该写序只解决崩溃悬挂指针，不替代上述 MVCC 规则。
- **abort 与 vacuum**：abort 的 chunk group 按事务状态不可见并可回收。已提交 group 只有在所有可能引用它的主行版本对全部活快照均不可见、主行版本已回收且不存在其他可见指针引用后才能回收；判定受 `oldest_snapshot_xmin` 约束。valueid 在 TOAST 关系内保持唯一，旧 group 未回收前不得复用，恢复后的分配也不得与已持久化指针冲突。
- **读路径**：主行 20B 指针 → 解析 toastrelid/valueid → 按快照读取 chunk → 按 chunk_seq 拼接并校验 rawsize/extsize；`Datum::External` 不得再响亮拒绝合法外部值。
- **衔接**:TOAST 压缩归 Phase 7b（本阶段只做本体，不压缩）。

---

## §8 Sequence(SERIAL)

- `nextval(seq_name)`：全局并发安全、单调递增且不重用的计数器，独立于事务 XID。**只随 checkpoint 持久化不足以保证崩溃安全**：任何值/预留块在对调用者可见前，必须先用 sequence WAL 记录持久化新的高水位；redo 恢复高水位，checkpoint 只作为后续优化。可以记录每次 nextval，也可以 WAL-first 预留一批值，但崩溃后允许出现空洞，绝不允许回退并复用已发放值。
- `nextval` 不因外层事务 ROLLBACK 而撤销，也不写事务 undo；并发 nextval 不得发放重复值。不能照搬 OID 分配器“checkpoint + 启动扫描已用值”的修正，因为 sequence 已发放值不一定出现在任何表中。
- `currval(seq_name)`:**会话内该 sequence 最近一次 `nextval` 的返回值**(per-session、per-sequence state，不是全局计数器的当前值）。本 session 从未对该 sequence 调用过 `nextval` 时返回错误；连接断开或进程重启后旧 session 的 currval 不恢复，新 session 必须先调用 nextval。checkpoint 不改变 currval 语义。
- 序列名注册进 catalog（新系统表 `pg_rust_sequences`,oid/name/current_value 三列）；current_value 的 WAL 高水位与 checkpoint 表示必须遵循同一恢复口径。
- `SERIAL` 列声明在 `CREATE TABLE` 时展开为 `INT + DEFAULT nextval('...')` 语义（声明期展开，不引入新的运行时依赖）。

---

## §9 验证方法论

1. **驱动矩阵**:psql / psycopg2 / node-postgres / rust-postgres 的连接与 CRUD + 事务（与 M3 的极简版矩阵同型）。Extended Query 必须真实覆盖 Parse/Bind/Describe/Execute/Sync、Bind 参数与结果 format codes、named/unnamed statement/portal，以及“错误后忽略至 Sync”的恢复序列。
2. **DataFusion 适配实测**：点查延迟 <5ms（不达标走自研执行器并登记实测值）；B+Tree 等值/范围 filter 下推正确性。ANN 不作为 DataFusion filter 验收项。另跑“旧快照查询执行中 UPDATE/DELETE + vacuum”，证明 SnapshotGuard 存活期间旧版本不会被回收。
3. **向量 SQL 端到端**：三种 opclass 建索引后，只有 metric 匹配的 `ORDER BY <distance> LIMIT k` 走 HNSW；ANN 返回行及顺序与 M6 API 路径**逐位一致**（同图同查询，ANN SQL 路径不得引入第二套召回逻辑），投影距离则按 §5.2 的 SQL 语义单独断言。覆盖无索引、metric 不匹配、缺 LIMIT、额外 WHERE 的 SeqScan 回退，以及 `<#>` 负内积和投影 `<->` 欧氏距离值。
4. **TOAST 大值与 MVCC**：>8KB 文本/JSONB、多 chunk、多个 attribute 合计超页的写入-读取-更新-删除；固定断言现有 20B 指针字节布局。覆盖旧快照读取期间 UPDATE/DELETE + vacuum、abort 回收、chunk_seq 损坏检测，以及 chunk WAL 后/主行 WAL 前等崩溃 rounds。
5. **Sequence**：并发 nextval 唯一且单调、事务 rollback 不回收值、WAL 高水位恢复、checkpoint 前后连续；在已发放值用于提交 INSERT 后崩溃，恢复后不得复用。新 session 未 nextval 时 currval 报错，重启后不恢复旧 session 的 currval。
6. **门禁沿用**:`cargo test --workspace`、fmt/clippy(双口径)/doc、默认无 `datafusion` feature 与启用该 feature 两种编译/测试、TOAST/Sequence WAL 崩溃 rounds、驱动兼容矩阵。

---

## §10 风险与开放问题

- **DataFusion 规划开销超标**（最大不确定项）——备用自研执行器必须存在且被验证；若 DataFusion 顺利达标，TP 路径也允许保留自研执行器（不强制切换）。
- **事务上下文透传的可见性错误**（读错行比慢更危险）——以 visibility oracle 为唯一裁决点，所有路径共享；SnapshotGuard 的所有权与异步流结束顺序是 coding-plan 的显式设计项。
- **TOAST 的 MVCC/WAL/crash 语义**——冻结现有指针格式，复用 HeapInsert/HeapDelete；除写序外，必须验证旧快照保留、更新版本链、abort/vacuum 回收和多 attribute 装页。
- **Sequence 高水位持久化**——checkpoint 不是崩溃唯一性的保证，nextval WAL 与事务 CLOG/heap WAL 的恢复顺序必须纳入崩溃 rounds。
- **AM 可插拔纪律**（Phase 3 核心零返工）——注册表 + 选择器是硬性结构，禁止 if/else 按 AM 类型硬编码分发。
- **ANN 与全扫描的边界**——metric 不匹配、无索引、缺 LIMIT、额外 WHERE 均回退暴力排序；不得悄悄错用图 metric 或先做 ANN TopK 再普通过滤。
- **M6 收口项并行**：与本阶段无依赖，24h 复跑/CI/tag 不阻塞。

---

## §11 验收标准（供 coding-plan 引用）

1. psql 与三类主流 PG 驱动可连接并执行 CREATE/INSERT/SELECT/UPDATE/DELETE；Extended Query 参数与结果格式均由 Bind 正确协商，错误-Sync 恢复和 statement/portal 生命周期测试通过。
2. DataFusion 能正确下推 B+Tree filter 到 pg_rust 的 TableProvider，SnapshotGuard 覆盖全部异步读取生命周期；TP 点查 <5ms（或备用路径生效且登记实测值）。ANN 明确走自研 TP，不要求 DataFusion TopK 下推。
3. 单路选择器能为单个谓词选择 B+Tree / HNSW（倒排在 Phase 3 后注册，不在本阶段验收）。
4. 三种 opclass 的 `CREATE INDEX ... USING hnsw` 均可创建对应 metric；metric 匹配的 `ORDER BY <distance> LIMIT k` 的返回行及顺序与 M6 API 逐位一致，其他规定形状正确回退 SeqScan；EXPLAIN 显示 HnswScan、代价估算与 `max_retry_rounds=3`。
5. heap TOAST 的单大值、多 attribute 合计超页、多 chunk 写入-读取-更新-删除全通过；旧快照/vacuum/abort/崩溃恢复不读错、不提前回收，20B 指针布局保持现状。
6. Sequence 在并发、rollback、checkpoint 和 SIGKILL 恢复后不重发已发放值；currval 严格遵守 per-session 语义。
7. 全量回归（debug + release)、fmt/clippy(双口径)/doc、默认与 `datafusion` feature 两种构建全绿；覆盖率口径承接既有 CI job。

---

## 修订记录

| 版本 | 日期 | 内容 |
|------|------|------|
| v1.0 | 2026-10-28 | 初稿：范围/归属、DataFusion 双路径执行器、AM 注册表可插拔、向量 SQL 语义（DDL + 三个距离算子 + ANN 路径 + EXPLAIN)、PG Wire Extended Query、heap TOAST、Sequence、验证与验收口径。 |
| v1.1 | 2026-10-28 | 复核修订：① §8 `currval` 更正为"会话内最近一次 nextval 的返回值"(per-session,PG 语义），与全局 nextval 分离；② §7 TOAST 补指针布局（rel_oid+chunk_id+len=20B)、写序（chunk 先写主行后写）与 redo 重建规则，阈值改 `MAX_TUPLE_BYTES`;③ §3 双路径登记为对 ROADMAP 的有意偏离（TP 默认自研执行器，非"不达标才绕开");④ §5.1 DDL 定 `CREATE INDEX ... USING hnsw` 为唯一规范形式，不引入第二 AST 形态；⑤ §5.2 补"缺 LIMIT 时回退 SeqScan + 暴力距离排序"规则；⑥ §2 补 DataFusion 以可选 feature 引入（TP 路径默认不启用）;⑦ §4.2 "直方图"改"基础统计"。 |
| v1.2 | 2026-10-28 | 对抗审查闭环：① §3 快照上下文必须连同 SnapshotGuard 持有至全部读取结束，ANN 明确只走自研 TP，DataFusion 只做普通 filter 下推；② §5 DDL 改为 PG/pgvector 的 USING/opclass 形态，补三种 metric、负内积/欧氏距离语义、metric 不匹配及额外 WHERE 回退，EXPLAIN 区分最大与实际重试轮数；③ §6 format codes 改由 Bind 决定，补 statement/portal 与错误-Sync 恢复；④ §7 恢复现有 5×u32 指针，补多 attribute 装页、chunk_seq、chunk MVCC、abort/vacuum 与唯一性规则；⑤ §8 nextval 改为 WAL-first 高水位且不可事务回滚；⑥ §9/§11 同步上述正确性与崩溃验收；⑦ 修正 toast.rs 归属与 AM“零修改”表述。 |
| v1.3 | 2026-10-28 | 复核修订：① §1/§3 修正 ROADMAP 引用行号（范围 :423-432、TP 检查点/偏离登记 :410-421）；② §5.1 补统一 `CREATE INDEX` 文法（name/USING 可省略，缺省 btree）；③ §5.2 钉死 `hnsw_search_visible` 返回内部 metric 距离、投影 `<->` 须 sqrt，并补 ORDER BY 非 ASC 回退 SeqScan；④ §1 “备用执行器”改“自研 TP 执行器”与 §3 口径一致。 |
| v1.4 | 2026-10-28 | 容量问题闭合：TOAST 隐藏 `pg_toast_<oid>` 关系改为 `pg-am-heap/src/toast.rs` 的 AM 内部结构（不占系统目录行，rel_oid 仅经 catalog 的 `oid_allocator` 分配，不新增系统表）——规避 catalog 单页容量上限与每表一个隐藏关系的冲突；依据行版本号同步为 v1.2。 |
