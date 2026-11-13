# Phase 4a Coding Plan：SQL 层 + PG Wire 基础 + 向量 SQL 语义

版本：v1.3（2026-10-28）
依据：`docs/phase4a-tech-selection.md` v1.3（双路径执行器、AM 注册表可插拔、向量 SQL 语义、PG Wire Extended Query、heap TOAST、Sequence)
前置：Phase 1/2 全部交付并过审；M6 收口项（24h 复跑、CI 覆盖率、tag）并行推进，不阻塞本计划。

---

## 硬约束（贯穿全阶段，违反即返工）

1. **双路径执行器是一等公民**:TP（点查/小范围/ANN TopK）走自研执行器，AP（复杂分析）走 DataFusion；备用路径不是失败兜底，DataFusion 以可选 feature `datafusion` 引入（TP 路径默认不启用）。ANN 不是 DataFusion filter，AP 路径不承担 ANN。
2. **AM 注册表可插拔**:planner 用 AM 注册表 + 单路选择器，禁止 if/else 按 AM 类型硬编码分发；Phase 3 倒排 AM 注册时不得修改 selector/executor 核心分派结构，但新增注册项、token/AST 和语法测试属于对应阶段工作。
3. **快照值与注册生命周期不可分割**:所有 SQL 读路径使用 `TxnManager::snapshot` 返回的 `(Snapshot, SnapshotGuard)`，禁止只传字段或退化为 `Snapshot::everything()`；Guard 必须覆盖事务/语句的全部 TP、ANN、TableProvider scan、DataFusion stream 与派生读取任务。
4. **可见性裁决唯一**:TableProvider/执行器回表读取统一走 `heap_tuple_visible`（与 M6 `hnsw_search_visible` 第 3 步同一函数），vacuum 验收必须证明活快照 Guard 阻止旧版本提前回收。
5. **ANN 路径只走既有管线且 metric 必须匹配**:ANN SQL 必须经自研 TP 调用 `hnsw_search_visible`，不得另起召回逻辑；opclass/Metric 与算子不匹配、无索引、缺 LIMIT、非 ASC 或带额外 WHERE 时一律 SeqScan + 同语义暴力排序。ANN 返回行及顺序与 M6 API 逐位一致，SQL 投影距离遵守 Stage B 的换算规则。
6. **TOAST 指针格式冻结**:20B = `vl_len_(u32，含 external tag) + va_rawsize(u32) + va_extsize(u32) + va_valueid(u32) + va_toastrelid(u32)`，以 `pg-am-heap/src/toast.rs` 为唯一实现来源，禁止改为 `u64+u64+u32`。
7. **TOAST MVCC 与写序均为红线**:chunk 是 **`pg-am-heap/src/toast.rs` 的内部结构，不占系统目录行**(catalog 单页容量上限与每表一个隐藏 pg_toast 关系冲突的规避——不新增系统表，rel_oid 仅经 catalog 的 `oid_allocator` 分配）,chunk 行与主行同 xid、同快照可见性；`(valueid, chunk_seq)` 稳定排序，UPDATE 不覆盖旧快照可见 group。**chunk 表先写、主行后写**，redo 同序；abort/vacuum 回收受快照 horizon 和主行引用约束，多 attribute 合计超页必须循环外置至可装页。
8. **TOAST 零新 WAL 记录类**:TOAST 溢出写复用 `HeapInsert`/`HeapDelete`/`HeapUpdate`，不新增 TOAST WAL 类型；TOAST 压缩归 Phase 7b。此约束不限制 Stage F 为 Sequence 高水位增加独立 WAL 类型。
9. **M6 约束如实继承**:HNSW 索引仅空表可建、每表至多一个、不 backfill；向量字面量与 Stage C 同一解码路径。
10. **DataFusion TP 适配检查点必须实测**:DataFusion 点查 <5ms、事务上下文及 SnapshotGuard 生命周期两项不过则 TP 全量走自研执行器并登记实测值，不得静默降级。
11. **Extended Query 遵守协议红线**:参数与结果 format codes 均来自 Bind；错误后忽略至 Sync，Sync 后恢复 ReadyForQuery；statement/portal 生命周期和 VECTOR OID/编码必须一致，不得靠自研两端对称 round-trip 掩盖协议错误。
12. **Sequence 崩溃后不得复用已发放值**:nextval 在值/预留块可见前 WAL-first 持久化高水位，不随事务 rollback 撤销；currval 是 pg-engine SQL session 的 per-session 状态，checkpoint 不承担 currval 持久化。
13. **验收不含倒排路径**：倒排 AM 的路径与验收归 Phase 4b,Phase 4a 不承诺。

---

## Stage 0：SQL AST 扩展 + AM 注册表骨架（1–2 天）

| 任务 | 交付物 |
|------|--------|
| Statement AST 扩展（`pg-engine/src/sql.rs`) | 在现有 `CreateIndex` 变体上增加 `name: Option<String>`、`method: Option<IndexMethod>`、`opclass: Option<VectorOpclass>`，不新增第二 AST 形态；统一支持既有 `CREATE INDEX ON t(col)` 与 `CREATE INDEX name ON t USING hnsw(col vector_l2_ops)`。`USING` 缺省为 BTree，HNSW 强制显式 opclass，非 HNSW 方法作用于 `VECTOR` 列时响亮拒绝；`OrderBy` 保持列名形态，新增 `OrderByExpr` 承载 `DistanceOp { op: L2/Cosine/InnerProduct, left: Expr, right: Expr }` 与 ASC/DESC，`Select.order_by` 改为 `Option<OrderByKind>`（sum-type `Column(OrderBy) | Expr(OrderByExpr)`，二者互斥），`Limit` 保留 |
| AM 注册表 + 单路选择器（新增 `pg-engine/src/planner.rs`) | `AmKind { BTree, Hnsw }` + `AmRegistration { kind, matches, scan }`（注册项自判请求形状）+ `plan_for_predicate(pred) -> ScanPlan`；倒排位置预留但不在本阶段注册。planner 负责 LIMIT/ASC/metric/WHERE 回退判定，不把执行计划规则塞进 tokenizer；`ScanRequest` 携带索引列、B+Tree 可用性和 HNSW ef 等元数据，`ScanSpec` 是唯一执行路径来源，Seq/BTree 节点携带 predicate/order_by/limit，HNSW 节点另携带 query 字面量 |
| SQL session/执行上下文（新增 `pg-engine/src/session.rs`) | 定义 Engine 侧 session 抽象，后续承载事务/语句 SnapshotGuard owner 与 per-session `currval`；pg-wire Session 只拥有并传递该上下文，不独占 SQL 语义 |
| EXPLAIN 骨架 | `ScanPlan` 可序列化输出（路径类型、谓词、AM 参数、代价估算占位），含 `SeqScan` 回退形态与 HNSW `max_retry_rounds=3` 字段 |
| 测试 | 解析器正/负例（三种 opclass、HNSW 缺 opclass/未知 opclass 响亮拒绝、三个算子及 ASC/DESC 解析、无空格负数比较）；planner 的 LIMIT 缺失、非 ASC、metric 不匹配、额外 WHERE、索引列不匹配、无 B+Tree 回退、ef 传递与候选数溢出；AM 注册表分派正确性；DDL 执行不静默丢弃 name/opclass |

**Stage 0 执行边界**：AST 可以表示完整 `CREATE INDEX` 形态，但 Stage B 前的 `exec` 对 HNSW、命名 B+Tree 以及非 HNSW vector opclass 响亮拒绝，不静默改写为自动命名的 B+Tree。ANN SELECT 在 `exec_select` 入口、谓词构造和堆扫描之前拒绝，且不得留下行锁；auto-commit 与显式事务的普通 ANN/FOR UPDATE 均有回归覆盖。

**验收**:`cargo test -p pg-engine --lib`(AST 与 planner 单测全绿）。

---

## Stage A：自研 TP 执行器（单路 + B+Tree + 可见性 + EXPLAIN)(2–3 天）

| 任务 | 交付物 |
|------|--------|
| 自研 TP 执行器（新增 `pg-engine/src/executor.rs`) | 单谓词 → 单路扫描执行：等值/范围谓词走 B+Tree `lookup`/`lookup_all`/`range_scan`（现有）,`SeqScan` 回退；回表统一 `heap_tuple_visible`。事务快照 Guard 与事务同寿，语句快照 Guard 持有到 scan/结果流全部结束，禁止只复制 Snapshot 字段 |
| EXPLAIN 执行输出 | `BTreeScan (predicate, est_rows/est_pages)`、`SeqScan` 两种计划节点的可读输出 + 基础统计（行数/页数） |
| 测试 | 点查延迟 <5ms（实测登记）、等值/范围查询正确性、SeqScan 回退、in-flight/aborted 不可见；旧快照扫描期间并发 UPDATE/DELETE + vacuum，Guard 存活时旧行仍可读、Guard drop 后才允许回收 |

**验收**：自研执行器与 `Engine::exec` 现有路径结果一致；点查 <5ms；可见性负例全绿。

**实现补充**：普通 B+Tree 的删除是 eager removal，因此执行器只在 `snap.xmax == current_xid` 且快照没有其他在飞行事务时使用 B+Tree，并在候选扫描后复核 XID；否则以 `SnapshotFallback` 改走 SeqScan，防止旧快照因旧索引条目已物理删除而漏行。查询 literal 超过索引条目存储上限时，执行与 EXPLAIN 同步以 `KeyTooLarge` 回退 SeqScan，因为堆谓词边界不受该存储限制。`EXPLAIN SELECT` 走独立只规划路径，`est_rows`/`est_pages` 是同一快照下的精确可见结果行数与关系页数（不是估算，也不代表计划实际触达页数），统计阶段会物化扫描，属于已知限制。实测登记见 `docs/phase4a-benchmarks.md`。

---

## Stage B：向量 SQL 语义（DDL for HNSW + ANN 算子 + EXPLAIN)(2–3 天）

| 任务 | 交付物 |
|------|--------|
| HNSW 索引 DDL | `CREATE INDEX <name> ON <table> USING hnsw (<column> <opclass>)` → SQL/catalog 登记名称并映射 `vector_l2_ops/vector_cosine_ops/vector_ip_ops` 到 `Metric::{L2,Cosine,InnerProduct}`，调用或适度扩展 `Engine::create_hnsw_index_on`；空表/每表一个/dim 校验沿用 M6，未知或缺失 opclass 响亮拒绝 |
| ANN 执行路径 | 仅 `ORDER BY <distance> ASC LIMIT k` 且算子与索引 metric 匹配时经自研 TP 调用 `hnsw_search_visible`；Guard 覆盖 ANN 回表与结果流。无索引、metric 不匹配、缺 LIMIT、DESC、额外 WHERE 均走 SeqScan + 同语义暴力排序，禁止 ANN TopK 后 post-filter |
| 距离语义 | `<->` SQL 返回欧氏距离，ANN 内部平方 L2 在投影时 `sqrt`；`<=>` 返回 cosine 距离；`<#>` 返回**负内积距离** `−a·b`，ASC 等价最大内积优先。ANN/SeqScan 共用同一 SQL 距离定义 |
| EXPLAIN for HnswScan | `HnswScan (ef, opclass, k, candidates, max_retry_rounds=3)`；普通 EXPLAIN 不输出执行后才能获知的实际 retry_rounds |
| 测试 | 三种 opclass 建索引及 metric 匹配查询；ANN 返回行及顺序与 M6 API **逐位一致**（含 siftsmall 全量语料 recall@10 = 0.9990，ef=64）；投影 `<->` sqrt 与 `<#>` 负内积 golden cases；无索引、metric 不匹配、缺 LIMIT、DESC、额外 WHERE 回退；重复索引名/重复表内 HNSW 响亮拒绝；in-flight/aborted 不可见与旧快照 vacuum 防护 |

**验收**:ANN SQL 返回行及顺序与 M6 API 逐位一致，投影距离符合 SQL 语义；全部门禁绿；可见性负例与旧快照 vacuum 防护通过。

**实现补充**：SQL HNSW 创建使用固定 seed 42 与默认 `HnswParams`，公共 M6 typed API 保持原签名，SQL 通过内部 named/core 路径登记自定义名称；live relation 重名检查同时覆盖 open-time catalog 与 fresh MVCC snapshot 的 `pg_class`，表、索引及系统目录共用同一命名空间。四类 HNSW catalog 行由单一构造器生成，图/map 页创建前完成空间预检。planner 仅在 ANN 规划时从 lazy/cached HNSW handle 读取 meta 中的 metric/ef，reopen 不 eager-open 图。SELECT 列表支持列与距离表达式混合投影，距离以 `FLOAT8`(OID 701）返回；NULL 距离保持 NULL，ASC NULLS LAST、DESC NULLS FIRST。ANN 保持 M6 API 行序，只有 SeqScan 回退重新做精确距离排序；当 ANN 命中不足 k 时，在同一 snapshot 下用 heap 扫描补足可见 NULL 向量行，确保 LIMIT 与 FOR UPDATE 不漏行。旧目录缺少 FLOAT8 `pg_type` 行时仍可用于距离计算，但禁止持久化 FLOAT8 列，避免写入不自描述的 `pg_attribute`。普通列 ORDER BY 同样采用 PG 默认 NULL 方向。基准登记见 `docs/phase4a-benchmarks.md`。

**二轮审查补充**：`hnsw_search_visible` 的 M6 默认 wrapper 与内部 policy 共用同一管线，候选数和重试轮次由 `HnswScanSpec` 真实驱动；候选数/ef 均受图 HWM 限制，结果容量为 `min(k,hwm)`，超大 LIMIT 不再 capacity overflow。索引仅在自身和父表均 live 时占用名称，drop 后可复名；B+Tree/HNSW commit 失败会回滚内存 registry/handle，auto-commit 的 op Err 同步清理 pending。ExtraWhere、缺 LIMIT、DESC 和其他列不再 lazy-open HNSW。普通 VECTOR ORDER BY 响亮拒绝，FLOAT8 NaN 大于所有非 NaN、NaN 之间相等且 ±0 相等。siftsmall 每个查询同时断言 `ExecutionPath::IndexLookup`，防止以精确 SeqScan 冒充 ANN 验收。真实旧目录通过物理删除 OID 701 后重开构造，证明兼容打开与拒绝 FLOAT8 存储建表并存。

**三轮审查补充**：`auto_commit_with_hooks` 在锁释放前执行成功/失败清理。DROP 只在事务内写 marker，commit 成功后才释放页面并清理 table/index registry 与 HNSW handle；commit 失败不释放页面，fresh MVCC 重建仍能读原表。清理先删除所有 live 入口再释放页面，free 失败也只泄漏而不复活；HNSW handle 按 `index_oid` 清除。创建索引的 registry/handle 回滚同样位于锁释放前，覆盖并发 waiter。`Datum` 手写 `PartialEq`，NaN 与 NaN 相等、±0 相等，和 `PartialOrd` 一致。

**相邻边界补充**：HNSW lazy-open 全程持有 table/index registry 读锁，并在 open 前后检查父表和索引均为 live，与 DROP 的写锁清理互斥，防止旧 handle 在 DROP 后重新发布。drop marker 提交后，`walk_chain`/`free_page` 失败仅告警并保留页面泄漏，DROP 按已提交返回成功，不再向调用方混淆为可重试的 commit failure。既有无锁 reader × DROP 页面释放/复用窗口明确保留为非目标；本阶段不引入读取锁、关系 lease 或 horizon 延迟释放，也不宣称该 DDL-vs-DML 边界已闭合。

---

## Stage C：PG Wire Extended Query(2–3 天）

| 任务 | 交付物 |
|------|--------|
| Extended Query 消息族（`crates/pg-wire/src/session.rs`、`codec.rs`) | Parse/Bind/Describe/Execute/Sync/Flush/Close 生命周期；**Bind parameter format codes 决定参数编码，Bind result format codes 决定结果编码**；Describe 只返回描述，不选择 Execute 格式 |
| statement/portal 与错误恢复 | 新 Parse 替换 unnamed statement，新 Bind 替换 unnamed portal；named 对象响应 Close，portal 随事务结束清理，连接结束清理全部 session。Extended Query 出错后忽略至 Sync，Sync 后发送正确事务状态的 ReadyForQuery；Flush 不代替 Sync。BEGIN/COMMIT/ROLLBACK 仍不进 `Engine::exec`，SQLSTATE 沿用 `WireError::sqlstate_of` |
| 标量类型映射（`types.rs`) | Int4/Int8/Float8/Text 的文本与二进制格式成对实现，二进制整数与 f64 使用 network byte order；**FLOAT8 文本需对齐 PostgreSQL `float8_out` 的科学计数法阈值、尾零及 `1e30`/`1.0` 等边界，不得仅以 Rust `f64::to_string` 作为兼容完成标准**；Bind format code 数量/值非法时响亮报错 |
| VECTOR 类型契约 | 文本保持 pgvector 形态 `[0.1,0.2,0.3]`；二进制 payload 采用 pgvector VECTOR 形态：`dim:u16 + unused:u16(0) + dim 个 f32`，整数与 f32 均按 network byte order，DataRow 外层 field length 不属于 payload。RowDescription/ParameterDescription 使用 pg_rust catalog 登记的 VECTOR OID，**不得继续上报 TEXT(25) 或冒称 FLOAT4[](1022)**；文本和二进制共用同一维度与数值解码校验 |
| 协议测试 | 独立 golden-byte 测试覆盖 Int4/Int8/Float8/VECTOR 文本与二进制、Bind 格式协商、null/非法维度/非法 format code；覆盖 named/unnamed 生命周期及“Parse/Bind/Execute 出错 → 后续消息 → Sync → ReadyForQuery”序列，禁止只做自研 encoder/decoder 对称 round-trip |
| 驱动矩阵测试 | psql / psycopg2 / node-postgres / rust-postgres：连接、CRUD、事务、参数绑定查询（含 ANN 路径）。**Simple Query 全链路距离投影必须覆盖有行/空结果两种形态，核对 FLOAT8 RowDescription OID 701 与 DataRow 文本值**。VECTOR 默认走文本以保证 stock driver 可用；二进制 VECTOR 用显式字节断言/自定义 decoder 验证，不宣称 stock driver 无扩展即可自动解码自定义 OID |

**验收**：psql、node-postgres、rust-postgres 的 Extended Query 全流程通过；psycopg2 的 CRUD、事务及 ANN 查询通过（其公开 cursor API 默认客户端插值，不谎报为服务端 Bind）。Bind/Sync/portal 与 VECTOR golden bytes 通过；ANN 参数绑定查询正确；Simple Query 的文本 payload、消息状态机与事务行为零回归（VECTOR RowDescription 从 TEXT OID 升级为独立 VECTOR OID 是本 Stage 的显式例外）。

**实现补充**：engine 新增 `Literal::Param` 与 `Literal::Float`，在 AST 中完成类型化绑定，不做 SQL 文本替换；`Engine::parameter_types` 负责按 schema 推断参数类型，`Engine::exec_with_params` 复用既有执行与 query-stat 路径。wire session 持有 prepared statement/portal，覆盖 OID 校验、format code 展开、文本/二进制参数、Describe、Execute 行数截断与恢复、Close、错误后忽略至 Sync。extended 隐式事务在 Sync 按成功/失败提交或回滚；空 Parse（含分号空语句）、EXPLAIN RowDescription、unnamed 失效、幂等 Close、整页挂起、u16 计数和消息体错误恢复均有回归。extended 隐式事务中的 DDL 一律响亮拒绝；Simple Query 先完整解析整条字符串，DML 使用统一隐式事务，后续错误一并回滚。显式事务错误立即 abort 并进入 failed 状态，普通语句返回 25P02，COMMIT 按 ROLLBACK 处理；重复 BEGIN 与空闲 COMMIT/ROLLBACK 使用 WARNING 而非 failed。malformed Sync 先置 extended_error 再恢复，解码错误同步触发 failed 状态，failed 下 Parse/Bind/DDL 均拒绝。TEXT OID 到 inferred VECTOR 的输入转换同时覆盖 Binary format 的文本载荷。驱动验收发现的两个 Stage B 语法缺口已修复：`FLOAT8` 现在是合法 DDL 类型名，普通小数 literal 不再被误当作单元素 VECTOR。psycopg2 的原始向量字面量使用 `AsIs`，避免其客户端插值把 VECTOR 变成 TEXT 字符串。

---

## Stage D：heap TOAST chunk I/O(2–3 天）

| 任务 | 交付物 |
|------|--------|
| chunk 表与冻结指针 | **chunk 表为 `pg-am-heap/src/toast.rs` 的内部结构（不占系统目录行）**；chunk 行逻辑键 `(va_valueid, chunk_seq)`，seq 从 0 连续递增，payload ≤ `TOAST_MAX_CHUNK_SIZE`(2000B)；rel_oid 仅经 catalog 的 `oid_allocator` 分配（不新增系统表，规避 catalog 单页容量上限）。主行沿用现有 `5×u32` 20B ToastPointer（含 external tag/rawsize/extsize/valueid/toastrelid），禁止新 `u64+u64+u32` 布局 |
| 触发与装页 | 单值超过 `TOAST_TUPLE_THRESHOLD`(2048B) 或内联主行超过 `MAX_TUPLE_BYTES` 时外置；后者循环选择大 attribute 并重算主行，直至可装页，覆盖多个 attribute 各自低于阈值但合计超页；全部外置仍超页才 `TupleTooLarge` |
| MVCC 与更新 | chunk 带标准 heap tuple 头，与主行同 xid、同 Snapshot/visibility oracle；修改外置值生成新 valueid/group，不覆盖旧快照可见 chunk，未修改值可继续共享引用。读路径按快照逐行判断 chunk 可见性，禁止 `Snapshot::everything()` |
| 写序与 WAL | chunk 表先写、主行后写且属于同一事务；复用 `HeapInsert`/`HeapDelete`/`HeapUpdate` 与 WAL-before-data，redo 按同序重建。写序只防崩溃悬挂指针，不替代 MVCC |
| abort、vacuum 与 valueid | abort group 不可见并可回收；已提交 group 仅在所有引用主行版本对活快照均不可见、主行版本已回收且无其他可见引用后回收，受 `oldest_snapshot_xmin` 约束。valueid 在 TOAST 关系内唯一，旧 group 未回收不得复用，恢复后分配不得碰撞已持久化指针 |
| 读路径 | 主行指针 → toastrelid/valueid → 可见 chunk → 按 chunk_seq 拼接并校验 rawsize/extsize；缺/重/乱序 chunk 响亮报 Corrupted；合法 `Datum::External` 在 engine/wire 读路径完成 detoast，不再响亮拒绝 |
| 测试 | 单值 >8KB、多 chunk、多 attribute 合计超页的文本/JSONB/超维向量往返；现有 20B 指针 golden bytes；旧快照 + UPDATE/DELETE + vacuum、共享引用、abort 回收、损坏 chunk_seq；chunk WAL 后/主行 WAL 前等 crash rounds |

**验收**：大值与多 attribute 装页全通过；现有指针格式冻结；旧快照不读错、abort/vacuum 不提前回收；崩溃恢复后主行-chunk 一致；TOAST 零新 WAL 记录类。**TOAST 溢出存储不改变 `MAX_VECTOR_DIM` 的类型级上限**——超 dim 的向量声明仍按 M6 的响亮拒绝处理，TOAST 只负责存储容量。

**实现补充**：每个含 TOASTable 列的用户表在 DDL 时分配一个隐藏 heap 和一个 OID，主表既有 `pg_class.reltoastrelid` 记录 OID，既有 `pg_rust_relpages.last_page` 记录隐藏链头（普通表 tail 原本只是 advisory 字段）；隐藏关系自身不新增任何目录行。pre-Stage-D 目录在 open 时经事务迁移，目录行均按当前快照选择 live 版本，relpages 读取取最新物理版本。写入先对全部超阈值值外置，再按最大 inline 值循环外置至装页；TEXT/BYTEA/VECTOR 共用 chunk 编解码，VECTOR 仍以 2000 维为类型上限。`t_infomask2` 新增 `HEAP_VECTOR_EXTERNAL(0x1000)` 统一表示全部非 NULL VECTOR attribute 均为 external；engine 在外置任一 VECTOR 时归一化同元组其他 VECTOR，codec 拒绝混合形态，因此不再根据 f32 payload 猜测指针。建表时 TOAST 注册位于提交前，commit 成功后才发布 table registry；注册失败会中止建表，commit 失败在锁释放前清理，避免“已提交但 API 报错”的幽灵表。valueid 在主表关系内单调分配，open 时扫描全部物理 chunk 恢复高水位，OID 启动校正同时纳入 `reltoastrelid`。普通查询只在主行/回表快照下 detoast；物理 detoast 仅用于 crash-loser 索引补偿和 vacuum，不返回用户结果。合法零长度值也必须存在一个 seq=0 空 chunk，零可见 chunk 一律按 Corrupted 处理，不再将 `va_extsize == 0` 作为合法例外。UPDATE 通过值比较共享未修改 group。vacuum 先回收主行，再回收不再被任何物理主行版本引用的 group，`VacuumStats.toast_chunks_reclaimed` 直接报告本轮回收量。`phase4a_toast.rs` 覆盖大值/多列/VECTOR、重启、旧快照 UPDATE/DELETE、共享引用、abort、损坏 chunk、旧目录迁移以及 chunk-only 和主行未提交两类 crash。当前没有 JSONB Datum/DDL 类型，故以 BYTEA 验证二进制 varlena，真正 JSONB 类型接入后的专项回归留作类型扩展边界，不冒充已验收。

---

## Stage E：DataFusion 集成与 TP 适配检查点（2–3 天）

| 任务 | 交付物 |
|------|--------|
| TableProvider(`pg-engine/src/datafusion_provider.rs`) | heap 行读取 + `(Snapshot, SnapshotGuard)` 透传 + 回表统一 `heap_tuple_visible`；SessionContext 可传快照值用于规划，但 Guard 有明确 session/transaction/query owner，覆盖所有 scan、execution stream、派生任务及取消/错误退出。只做 B+Tree/普通 filter 下推，**ANN 不进入 DataFusion** |
| TP 适配检查点实测 | DataFusion 点查延迟 <5ms 实测（不达标 TP 全量走自研执行器并登记实测值）；事务上下文与 Guard 生命周期验证（可见性负例 + 旧快照 vacuum 防护） |
| 可选 feature `datafusion` | 默认 TP 路径不启用该 feature;AP 路径启用时引入（`Cargo.toml` 可选依赖）；feature 关闭时 TP/ANN 完整可用，AP 请求响亮报 Unsupported，不静默改变查询语义 |
| 测试 | DataFusion B+Tree/普通 filter 下推正确性、in-flight/aborted 不可见、自研 TP 与 DataFusion 普通查询按行集相等（顺序不作位级断言）；旧快照流执行中 UPDATE/DELETE + vacuum；默认与 `datafusion` feature 两种编译/测试。ANN 仅验收自研 TP，不做虚构的 AP/TP ANN 对比 |

**验收**:DataFusion 适配检查点两项实测通过或备用路径生效并登记实测值；可见性与旧快照 vacuum 防护全绿；普通 TP/AP 查询行集一致，ANN 仅走自研 TP；默认与 `datafusion` feature 构建测试全绿。

**实现补充**：DataFusion 选用 **50.3.0**（MSRV 1.86，实际解析版本），以可选 feature 引入；默认 TP/ANN 不启用。公开入口 `Engine::exec_datafusion` 是显式 AP API，默认 `exec` 不改路由：普通 SELECT 仍走自研执行器，ANN 与 FOR UPDATE/SHARE 明确拒绝。`PgTableProvider` 将 heap schema 转为 Arrow schema，scan 复用自研执行器完成 B+Tree/普通 filter、回表可见性与 TOAST detoast，再转换 RecordBatch 为 `QueryResult`。provider/execution plan 持有 SnapshotGuard 至 execution stream 和派生任务结束，collect、LIMIT 与执行错误均释放 Guard；显式事务复用 TxnHandle 的快照、推进 curcid，并与自研 SELECT 一样先取 AccessShare，DROP 必须等待读者结束。Mini SQL 只负责拦截 ANN、行锁和非 SELECT，复杂 AP 语法交给 DataFusion；所有 live 表均注册以支持 JOIN，结果字段优先匹配原 ColumnType，匹配不到时回退 Arrow DataType，支持 AND、别名、聚合、计算列与 JOIN。Arrow Int4/Int8/Float8/Text/Bytea/Timestamptz/Uuid/Vector 均按原 ColumnType 转换，NULL vector 以 List null 表示。

---

## Stage F：Sequence(SERIAL)(2–3 天）

| 任务 | 交付物 |
|------|--------|
| WAL-first 序列实现（`pg-engine/src/sequence.rs` + `pg_rust_sequences` 系统表 + `pg-storage/src/wal/record.rs`/recovery） | nextval 全局并发安全、单调且不重用；值或预留块对调用者可见前，用独立 sequence WAL 类型持久化高水位，redo 恢复高水位，checkpoint 仅作优化。允许崩溃空洞，禁止回退复用；不得依赖“扫描表中已用值”修正；新 WAL discriminant 固定并加入 waldump/编解码测试 |
| 事务与会话语义 | nextval 不随外层事务 ROLLBACK 撤销、不写事务 undo；currval 保存于 Stage 0 的 pg-engine SQL session，按 sequence OID 分别记录该 session 各 sequence 最近一次 nextval，未调用则报错。pg-wire 只传递 session；Simple/Extended/嵌入式 Engine 调用共享同一语义，重启后不恢复旧 session currval |
| SERIAL 展开 | `CREATE TABLE` 时 `SERIAL` 列展开为 `INT + DEFAULT nextval('...')`（声明期展开，不引入新的运行时依赖）；DEFAULT 执行通过同一 SQL session 调用 nextval |
| 测试 | 并发 nextval 唯一/单调、rollback 不回收、跨 session currval 隔离、新 session 与重启后 currval 报错、SERIAL 展开；已发放值用于提交 INSERT 后 SIGKILL，恢复后不得复用；覆盖 sequence WAL 与 heap/CLOG 交错及 checkpoint 前后 crash rounds |

**验收**：并发、rollback、checkpoint 和 SIGKILL 恢复后均不重发已发放值；currval 严格 per-session；SERIAL 展开正确。

---

## Stage G：验收 + 对抗审查 ×2 + 归档（1–2 天）

| 任务 | 交付物 |
|------|--------|
| 全量回归 | workspace 默认 feature debug + release；pg-engine 额外跑 `datafusion` feature debug + release；loom-gated crate 遵守 CI 的 default-feature 测试规则。Clippy/doc 按 CI 逐 crate `--all-features`，不发明不受支持的 workspace all-features test 口径 |
| 覆盖率口径 | 既有 coverage job 继续作为 **pg-am-hnsw 专用** `--fail-under 90` 回归门；不得用它宣称 pg-engine/pg-wire/pg-am-heap 的 Phase 4a 覆盖率。若 Phase 4a 要新增覆盖率数字，必须另注册对应 crate 的覆盖率 job，否则只报告测试矩阵证据 |
| 对抗审查 ×2 | 第一轮实现审查 → 修复 → 第二轮复核；重点面：SnapshotGuard × vacuum、ANN metric/距离/回退、TOAST MVCC + 写序 + 回收、Sequence WAL/crash、Bind/Sync/portal、VECTOR golden bytes、默认与 DataFusion feature 构建 |
| 归档 | stage_spec Phase 4a 各节（交付内容/关键约束/审查回流/验证数字）+ benchmarks 更新（DataFusion 实测值、ANN SQL 与 M6 API 行序一致性证据、点查延迟） |

**验收命令（全阶段收口）**:
```bash
cargo test --workspace
cargo test --workspace --release
cargo test -p pg-engine --features datafusion
cargo test -p pg-engine --features datafusion --release
cargo check --workspace --all-features
for crate in pg-storage pg-txn pg-catalog pg-am-heap pg-am-btree pg-am-hnsw pg-engine pg-wire; do
  cargo clippy -p "$crate" --all-targets --all-features -- -D warnings || exit 1
done
cargo fmt --all -- --check
for crate in pg-storage pg-txn pg-catalog pg-am-heap pg-am-btree pg-am-hnsw pg-engine pg-wire; do
  RUSTDOCFLAGS="-D warnings" cargo doc -p "$crate" --all-features --no-deps || exit 1
done
```

---

## CI 注册清单（逐项：进哪个 workflow、什么条件下跑）

| # | 内容 | workflow/job | 运行条件 |
|---|------|-------------|---------|
| 1 | SQL AST/planner 单测（Stage 0) | ci.yml 既有 pg-engine test matrix | 每 push |
| 2 | 自研 TP 执行器测试（Stage A) | 同上 | 每 push |
| 3 | 向量 SQL 语义测试（Stage B) | 同上 | 每 push |
| 4 | PG Wire Extended Query、Bind/Sync/portal、VECTOR golden bytes 与驱动矩阵（Stage C) | ci.yml 既有 pg-wire test matrix | 每 push |
| 5 | heap TOAST MVCC/装页/回收测试 + crash rounds(Stage D) | ci.yml 既有 pg-am-heap/pg-engine matrix + crash-rounds 手动/nightly | 每 push（单元）+ 手动/nightly（崩溃轮次） |
| 6 | DataFusion 适配检查点与默认/feature 双构建（Stage E) | ci.yml 独立 `engine-default` job（默认 feature）+ 既有 pg-engine all-features matrix；5ms 点查改 `#[ignore]` 手动 release 验收 | 每 push（双路径功能回归）+ 手动性能验收 |
| 7 | Sequence 并发/会话测试 + WAL crash rounds(Stage F) | ci.yml 既有 pg-engine test matrix + crash-rounds 手动/nightly | 每 push（单元）+ 手动/nightly（崩溃轮次） |
| 8 | 既有 HNSW 覆盖率回归 | 既有 coverage job(--fail-under 90，仅 pg-am-hnsw) | 每 push；不得作为 Phase 4a 其他 crate 的覆盖率证明 |
| 9 | 全量回归 + 对抗审查 | 全量回归 debug+release + 两轮对抗审查 | Stage G 收口时 |

---

## 风险与开放问题（登记，不阻塞）

- **DataFusion 达标形态未定**：自研执行器必须在 Stage A 先行落地，DataFusion 检查点（Stage E）不通过时 TP 走自研执行器并登记实测值；即使 DataFusion 顺利，TP/ANN 仍保留自研路径，AP 不承担 ANN。
- **SnapshotGuard × 异步流生命周期**：必须在 Stage A/E 用旧快照 + vacuum 证明 Guard 没有提前 drop，不能只验证 Snapshot 字段可见性。
- **TOAST MVCC 与 crash 语义**：Stage D 除 crash rounds 外必须覆盖旧快照、UPDATE 版本链、共享引用、abort/vacuum 和多 attribute 装页，不能只靠大值 round-trip。
- **Sequence WAL 恢复顺序**：高水位 WAL 与 heap/CLOG/checkpoint 的交错必须进入 crash rounds；checkpoint-only 方案视为正确性回退。
- **PG Wire 类型契约**：VECTOR OID、文本和二进制必须一致，golden bytes 与真实驱动缺一不可；自研两端 round-trip 不能证明协议正确。
- **AM 可插拔纪律**:Phase 3 倒排 AM 注册时不得改 selector/executor 核心分派结构；若发现必须改，属结构回退，需在审查中拦截。
- **M6 收口项并行**:24h 复跑/CI 报告/tag 与本计划无依赖，随时可并行推进，不阻塞各 Stage。

---

## 修订记录

| 版本 | 日期 | 内容 |
|------|------|------|
| v1.0 | 2026-10-28 | 初稿：8 个阶段（0–G）与 10 条硬约束、CI 注册清单、风险登记。 |
| v1.1 | 2026-10-28 | 复核修订：① Stage D 的 chunk 表改为 `pg-am-heap/src/toast.rs` 内部结构（不占系统目录行，rel_oid 仅经 `oid_allocator` 分配，不新增系统表）;② Stage F 的 currval per-session 状态明确归属 `pg-wire/src/session.rs` 连接级，engine 仅提供全局 nextval;③ Stage E 的结果一致性口径收窄为"ANN 路径逐位一致，其他查询按集合相等";④ Stage 0 的 AST 改为在现有 `CreateIndex` 变体上加 `method: Option<IndexMethod>` 字段（不另起 AST 形态）;⑤ Stage C 的 VECTOR 文本格式（`'[0.1,0.2,0.3]'` 字符串）与二进制格式（`float4[]`）分开写，请求侧绑定与响应侧格式分开写；⑥ Stage D 补"TOAST 不改变 `MAX_VECTOR_DIM` 类型级上限"注记。 |
| v1.2 | 2026-10-28 | 对抗审查闭环并以技术选型 v1.3 为基线：① 全部读路径补 SnapshotGuard owner/生命周期与 vacuum 验收；② Stage 0/B 补索引名、三种 opclass、metric 匹配、负内积/欧氏距离及 ASC/LIMIT/WHERE 回退，EXPLAIN 改 max_retry_rounds；③ Stage C format codes 改由 Bind 决定，补 Sync/portal 恢复，VECTOR 采用明确 OID 与 pgvector binary/golden-byte 契约；④ Stage D 恢复现有 5×u32 指针和 catalog 隐藏关系，补 chunk_seq、MVCC、abort/vacuum、多 attribute 装页；⑤ Stage E 移除 DataFusion ANN，补 feature 双构建；⑥ Stage F 改 WAL-first 高水位，currval 归 pg-engine SQL session，补 SIGKILL 验收；⑦ Stage G/CI 对齐逐 crate Clippy/doc、loom 测试规则与 HNSW-only 覆盖率事实；⑧ 修正 AM“零修改”表述。 |
| v1.3 | 2026-10-28 | 对抗审查复核：① 依据版本号与技术选型 v1.3 对齐；② Stage 0 `Select.order_by` 钉为 sum-type `Option<OrderByKind>`（`Column(OrderBy) | Expr(OrderByExpr)` 互斥），并补非 HNSW 方法作用于 `VECTOR` 列的响亮拒绝；③ Stage A B+Tree 引用名改 `lookup`/`lookup_all`/`range_scan`；④ Stage B recall 措辞改为 siftsmall 全量语料 ef=64 实测 0.9990。 |
| v1.4 | 2026-10-28 | 容量问题闭合：TOAST 隐藏 `pg_toast_<oid>` 关系改为 `pg-am-heap/src/toast.rs` 的 AM 内部结构（不占系统目录行，rel_oid 仅经 catalog 的 `oid_allocator` 分配，不新增系统表）——规避 catalog 单页容量上限与每表一个隐藏关系的冲突；依据行版本号同步为 v1.2。 |
