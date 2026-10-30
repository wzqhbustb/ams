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

---

## Stage C：PG Wire Extended Query(2–3 天）

| 任务 | 交付物 |
|------|--------|
| Extended Query 消息族（`crates/pg-wire/src/session.rs`、`codec.rs`) | Parse/Bind/Describe/Execute/Sync/Flush/Close 生命周期；**Bind parameter format codes 决定参数编码，Bind result format codes 决定结果编码**；Describe 只返回描述，不选择 Execute 格式 |
| statement/portal 与错误恢复 | 新 Parse 替换 unnamed statement，新 Bind 替换 unnamed portal；named 对象响应 Close，portal 随事务结束清理，连接结束清理全部 session。Extended Query 出错后忽略至 Sync，Sync 后发送正确事务状态的 ReadyForQuery；Flush 不代替 Sync。BEGIN/COMMIT/ROLLBACK 仍不进 `Engine::exec`，SQLSTATE 沿用 `WireError::sqlstate_of` |
| 标量类型映射（`types.rs`) | Int4/Int8/Text 的文本与二进制格式成对实现，二进制整数使用 network byte order；Bind format code 数量/值非法时响亮报错 |
| VECTOR 类型契约 | 文本保持 pgvector 形态 `[0.1,0.2,0.3]`；二进制 payload 采用 pgvector VECTOR 形态：`dim:u16 + unused:u16(0) + dim 个 f32`，整数与 f32 均按 network byte order，DataRow 外层 field length 不属于 payload。RowDescription/ParameterDescription 使用 pg_rust catalog 登记的 VECTOR OID，**不得继续上报 TEXT(25) 或冒称 FLOAT4[](1022)**；文本和二进制共用同一维度与数值解码校验 |
| 协议测试 | 独立 golden-byte 测试覆盖 Int4/Int8/VECTOR 文本与二进制、Bind 格式协商、null/非法维度/非法 format code；覆盖 named/unnamed 生命周期及“Parse/Bind/Execute 出错 → 后续消息 → Sync → ReadyForQuery”序列，禁止只做自研 encoder/decoder 对称 round-trip |
| 驱动矩阵测试 | psql / psycopg2 / node-postgres / rust-postgres：连接、CRUD、事务、参数绑定查询（含 ANN 路径）。VECTOR 默认走文本以保证 stock driver 可用；二进制 VECTOR 用显式字节断言/自定义 decoder 验证，不宣称 stock driver 无扩展即可自动解码自定义 OID |

**验收**：psql 与三类驱动 Extended Query 全流程通过；Bind/Sync/portal 与 VECTOR golden bytes 通过；ANN 参数绑定查询正确；Simple Query 的文本 payload、消息状态机与事务行为零回归（VECTOR RowDescription 从 TEXT OID 升级为独立 VECTOR OID 是本 Stage 的显式例外）。

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

---

## Stage E：DataFusion 集成与 TP 适配检查点（2–3 天）

| 任务 | 交付物 |
|------|--------|
| TableProvider(`pg-engine/src/datafusion_provider.rs`) | heap 行读取 + `(Snapshot, SnapshotGuard)` 透传 + 回表统一 `heap_tuple_visible`；SessionContext 可传快照值用于规划，但 Guard 有明确 session/transaction/query owner，覆盖所有 scan、execution stream、派生任务及取消/错误退出。只做 B+Tree/普通 filter 下推，**ANN 不进入 DataFusion** |
| TP 适配检查点实测 | DataFusion 点查延迟 <5ms 实测（不达标 TP 全量走自研执行器并登记实测值）；事务上下文与 Guard 生命周期验证（可见性负例 + 旧快照 vacuum 防护） |
| 可选 feature `datafusion` | 默认 TP 路径不启用该 feature;AP 路径启用时引入（`Cargo.toml` 可选依赖）；feature 关闭时 TP/ANN 完整可用，AP 请求响亮报 Unsupported，不静默改变查询语义 |
| 测试 | DataFusion B+Tree/普通 filter 下推正确性、in-flight/aborted 不可见、自研 TP 与 DataFusion 普通查询按行集相等（顺序不作位级断言）；旧快照流执行中 UPDATE/DELETE + vacuum；默认与 `datafusion` feature 两种编译/测试。ANN 仅验收自研 TP，不做虚构的 AP/TP ANN 对比 |

**验收**:DataFusion 适配检查点两项实测通过或备用路径生效并登记实测值；可见性与旧快照 vacuum 防护全绿；普通 TP/AP 查询行集一致，ANN 仅走自研 TP；默认与 `datafusion` feature 构建测试全绿。

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
| 6 | DataFusion 适配检查点与默认/feature 双构建（Stage E) | ci.yml 既有 pg-engine matrix（新增默认与 `datafusion` feature 变体） | 每 push |
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
