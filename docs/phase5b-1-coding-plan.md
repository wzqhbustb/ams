# Phase 5b-1 Coding Plan：Graph AM 与 multi-AM GC coordinator

版本：v1.2（2026-11-24）
依据：`docs/phase5b-1-tech-selection.md` v1.3（含 Graph side file、GraphKeyCodec、Graph 完整性、增量 GC 预算、双代际 PageFreeV2）
前置：Phase 1/2/3 与 Phase 4a 已完成并合入。Phase 2 的 24h 压测、CI coverage、`phase2-m6` tag 并行推进，不阻塞本计划。

---

## 硬约束（贯穿所有 Stage，违反即返工）

1. **heap + WAL 是唯一事实源**：GraphItem 与所有索引变更必须同事务、同 WAL；不得另建图专用事实源或绕过快照。
2. **Graph 完整性必须在锁内成立**：`ADD NODE` 查重、`ADD EDGE` 两端存在性校验、`DELETE NODE` 级联删除都必须在 `AccessExclusive` 临界区内完成，禁止依赖 B+Tree 去重或事后补偿。
3. **锁顺序固定**：table lock → sequence lock，禁止反序；`GRAPH UPDATE NODE` 与 `GRAPH DELETE EDGE` 只用 `RowExclusive`。`AccessExclusive` 已覆盖 Graph 完整性写临界区，不再引入未定义的 graph write gate。
4. **sequence high_water 是 edge ID 唯一权威**：Graph side file 只保存 `edge_sequence_oid`，不得再维护独立 `next_edge_id`；崩溃/回滚允许空洞，禁止重用。
5. **快照与 Guard 不可分割**：Graph 遍历、GC、TOAST detoast 与派生任务全部使用真实 `(Snapshot, SnapshotGuard)`，禁止 `Snapshot::everything()`。
6. **索引清理先于物理失效**：每个 AM 的索引删除必须先 durable，再执行 heap compact、页链 unlink、segment/file 删除；不得先 compact 再提取 key。
7. **收集必须增量有界**：禁止先物化全关系 `dead_tids`；按页/字节预算收集，禁止同关系直接拼接 dead set。
8. **合并 horizon 必须回退后重建**：同关系工作合并使用 `min(old_horizon,new_horizon)` 并重新 collect，禁止采用较新 horizon 直接合并。
9. **B+Tree 父指针只在安全窗口修改**：空 leaf merge 需要父 separator/downlink 更新时，必须进入短的 `AccessExclusive` 窗口；不得伪造全锁-free 合并。
10. **PageFreeV2 必须双代际恢复**：`base_generation` 与 `post_unlink_generation` 不得混用；compact、unlink、free 每个崩溃点都要可重放、可跳过、不可误释放新租户页。
11. **Graph 遍历必须确定性且有预算**：BFS/DFS 的排序、去重、路径和 resume 语义固定；结果数、visited、响应字节超限返回 `Capacity`，不得静默截断。
12. **GC 与 DROP 生命周期不可复活**：DROP 先停止新工作并等待 in-flight traversal/GC，再清理 registry/handle/storage；旧 graph、旧 sequence、旧目录不得复活。
13. **测试先行**：GraphItem、JSONB、SQL AST、GC contract、PageFreeV2、generation 和崩溃矩阵必须先写 golden/负例，再写实现。
14. **不提前实现 Fusion/TimeSeries**：5b-3、Phase 4b、Phase 5a 不在本阶段展开；Time-range routing 只做接口预留，不能伪造 TimeSeries 路径或结果。

---

## Stage 0：格式冻结、crate 骨架、AST 与 CI（1–2 天）

| 任务 | 交付物 |
|---|---|
| crate 注册 | 新建 `crates/pg-am-graph`，加入 workspace members；模块骨架 `error.rs`、`model.rs`、`store.rs`、`traversal.rs`、`lib.rs`。依赖只允许 `pg-storage`、`pg-txn`、`pg-am-heap`、`pg-am-btree`，禁止依赖 `pg-engine` 或 DataFusion |
| GraphItem | 冻结 `record_type: Int4`、`id: Int8`、`src: Int8`、`dst: Int8`、`properties: Jsonb`；`node.record_type=1`、`edge.record_type=2`，node 的 `id=src=dst`。**所有读取必须同时带 `record_type` 过滤**：节点查重/存在性用 `record_type=node AND id=node_id`，邻接与级联删除用 `record_type=edge AND (src=node_id OR dst=node_id)`，禁止把节点当成自环边 |
| GraphEntry/目录 | 冻结 `GRAPH_AM_OID=9880` 与 **Graph side file** `graph/<oid>/metadata.bin { format_version,oid,name,directed,first_page,edge_sequence_oid }`，复用 inverted analyzer/manifest 的 version+CRC+atomic write 先例；**不新增 `pg_graphs` 系统表、不改 bootstrap**。Graph OID 与内部 heap relation OID 相同；`LogicalGraph=102` 仅保留给未来元数据，邻接 DML 继续使用 heap WAL |
| GraphKeyCodec | 冻结三个自定义字节 key（当前 B+Tree 只支持单列 key，禁止假设 composite Datum）：`identity = record_type:u32 + id:u64`、`out = record_type:u32 + src:u64 + id:u64`、`in = record_type:u32 + dst:u64 + id:u64`，字段使用既有 sign-flipped big-endian。identity 同时服务节点查重与 edge_id 精确删除；out/in 的索引序天然是 `(neighbor_id,edge_id)`，不需要额外内存排序 |
| Graph AM 方法契约 | `build` 初始化 GraphStore 与目录；`search` 只负责 BFS/DFS；`insert/delete/scan` 显式返回 `Unsupported`，Graph DML 只走 GraphStore API；`freshness()` 返回 `None`（同步 AM，不走 Tier 2），禁止空 Ok、空 Vec 或伪造 heap tuple |
| JSONB | 新增 `ColumnType::Jsonb`、`Datum::Jsonb(String)`、DDL `JSONB`、文本合法性与 TOAST varlena；PG Wire 使用 OID **114** 文本结果，不实现二进制 JSONB 或 JSONPath |
| SQL AST | 冻结 `CreateGraph`、`GraphAddNode`、`GraphAddEdge`、`GraphUpdateNode`、`GraphDeleteNode`、`GraphDeleteEdge`、`GraphBfs`、`GraphDfs`；方向 `OUT/IN/BOTH`、`MAX_DEPTH 0..3`、属性 JSONB literal |
| 遍历 AST/结果 | 内部结果 `GraphHit { source_tid: Tid, node_id: Int8, depth: Int4, path: Jsonb }`，`source_tid` 供未来 Fusion 统一回表；当前 SQL 投影仍为 `(node_id,depth,path)`，不改变 wire schema。冻结 BFS 最短路径与 DFS 前序、邻接 `(neighbor_id,edge_id)` 升序 |
| 遍历预算 | `GraphTraversalOptions { max_results=100_000, max_visited_nodes=1_000_000, max_response_bytes=16MiB }`，只从 engine config 读取；超限统一 `Capacity` |
| GC contract | 在 `pg-storage/src/gc.rs` 冻结 `GcRelation`、`GcRequest`、`GcPageWork`、`GcWork`、`GcStats`、`GcParticipant::collect/reclaim`；`GcPageWork` 只含当前页 `dead_tids` 与 `estimated_bytes`。`GcWork.pages` 使用 `BTreeMap<PageId,GcPageWork>`，collect 与 reclaim 共用同一去重页序，禁止重复 compact/unlink/free |
| PageFreeV2 | 冻结 `version=2`、`relation_oid`、`page_id`、`prev_page_id`、`next_page_id`、`base_generation`、`post_unlink_generation`、`operation_id`；`operation_id=(relation_oid,base_generation,page_id)` |
| BTreePageCleanup | 冻结新判别值 **60** 与 `parent_page_id/new_separator_key/base_generation/post_unlink_generation/operation_id`；只用于 B+Tree 父 separator/downlink，不与 heap 专用 `HeapCleanup` 混用；新占号须登记原因与 golden bytes |
| Time-range 预留 | 冻结 `created_at >= low AND created_at <= high` 的规范化与精确 SeqScan 回退；只为未来 TimeSeries 注册保留位置，不创建 `AmKind::TimeSeries` 实现。规范化归入 Stage B，必须保持 `extra_filter` 的 AND 与 NULL 语义 |
| CI 注册 | 新 crate 加入 clippy/test/doc matrix、fmt、MSRV workspace check；`pg-am-graph` 无 loom feature，归 all-features 分支。若需要覆盖率，另建 pg-am-graph 专用 job，不能沿用 hnsw 数字 |
| Golden tests | GraphItem codec、JSONB 合法性、Graph AST、遍历结果形状、GC contract、PageFreeV2 bytes、未知 AM、非法深度、非法 JSON、非法版本/长度/CRC |

**验收**：
- `cargo test -p pg-am-graph --lib`
- `cargo test -p pg-engine --lib`
- `cargo +1.86.0 check --workspace --all-features`

本阶段只冻结格式和接口，不提前实现图遍历、GC 回收或 TimeSeries 路径。

---

## Stage A：Graph DDL、GraphItem 与 DML（2–3 天）

| 任务 | 交付物 |
|---|---|
| `CREATE GRAPH` | 在一个 DDL 事务创建内部 heap relation、identity/out/in 三个 GraphKeyCodec 索引、GraphEntry 与 edge sequence；`DIRECTED` 默认，`UNDIRECTED` 可选；锁内发布 registry/handle，失败完整回滚。**graph name 必须唯一**，重名 CREATE 返回 `IndexExists`，DROP 后允许复名 |
| Graph registry | side file 持久化 `format_version/oid/name/directed/first_page/edge_sequence_oid`；内存 `GraphEntry` 从目录重建，禁止从 filename 猜图关系。side file 不存在按无 Graph 处理，损坏则 fail-stop；不得从 `pg_class` 猜 Graph 类型 |
| `GRAPH ADD NODE` | `AccessExclusive` 内以 `record_type=node AND id=node_id` 查重并插入 node；重复 node ID 返回 `InvalidArgument`，验证与插入不离开锁，不能把同名 edge 当成节点 |
| `GRAPH ADD EDGE` | `AccessExclusive` 内以 `record_type=node AND id=node_id` 验证两端节点存在并分配 edge ID 插入；任何一端不存在都拒绝；edge ID 只从 sequence 获取 |
| `GRAPH UPDATE NODE` | `RowExclusive` 更新节点属性，保持 node ID、src/dst 不变；属性更新复用 TOAST 和索引维护 |
| `GRAPH DELETE EDGE` | `RowExclusive` 按 `edge_id` 精确删除，禁止按 src/dst 猜测删除多条边 |
| `GRAPH DELETE NODE` | `AccessExclusive` 内以 `record_type=edge AND (src=node_id OR dst=node_id)` 枚举并级联删除关联边，再删除节点；锁内不得补插新边 |
| DML 事务 | auto-commit 与显式事务复用现有提交/回滚/失败语义；`CREATE GRAPH` 的 registry/sequence 回滚镜像 inverted/HNSW 先例，commit failure 后可重试 |
| 属性 | 节点与边属性支持 NULL、空对象、嵌套对象和大 JSON/TOAST；非法 JSON 响亮拒绝，不引入隐式 TEXT 转换 |

**验收**：
- 节点/边及 identity/out/in 索引在 reopen 后一致；节点删除无 dangling edge。
- 并发重复节点、ADD EDGE 对不存在节点、DELETE NODE 与并发 ADD EDGE 三组回归全部通过。
- DDL/DML commit failure 后无幽灵 registry、sequence 或索引，且可以重试。

**实现补充**：首版用 `AccessExclusive` 串行化这三类 DML，属于明确接受的写并发降级；不得为了吞吐改回无锁查重。

---

## Stage B：BFS/DFS、属性与 PG Wire（2–3 天）

| 任务 | 交付物 |
|---|---|
| 邻接读取 | `OUT` 使用 out codec 索引，`IN` 使用 in codec 索引，`BOTH` 合并去重；**所有邻接查询必须同时满足 `record_type=edge`**，禁止把 node 记录当成自环边；只读取当前快照可见 GraphItem，禁止每跳全表扫描 |
| BFS | 按 `(depth,node_id)` 输出最短路径，同层节点 ID 升序； visited 去重，自环不重复入队 |
| DFS | 按前序输出，邻接按 `(neighbor_id,edge_id)` 升序；结果与输入顺序无关、字节级可复现 |
| 路径结果 | 生成 `(node_id,depth,path)`；path 包含 start 与当前节点；大属性只在结果需要时 detoast |
| 预算与分页边界 | 实现结果数、visited、响应字节硬预算；构造 path、队列、响应前检查，超限返回 `Capacity`，不返回伪完整结果。分页 resume token 若实现，必须含 `(direction,last_depth,last_node_id)` |
| PG Wire | Graph 查询、参数化 start ID、RowDescription：node_id INT8、depth INT4、path JSONB 文本；Simple/Extended 空结果与非空结果一致 |
| DataFusion/Arrow | 在 `arrow_column`、`array_to_value`、`df_filter` 白名单注册 JSONB：仅以 **Utf8 作为 JSON 文本传输类型**，输入/输出仍经 JSONB 合法性校验，禁止在 engine SQL 层隐式 TEXT 转换；JSONB 专用谓词/函数下推返回 `Unsupported`，不得借 Utf8 偷换语义 |
| QueryStats/EXPLAIN | Graph 查询进入 SQL text path 统计并新增 `ExecutionPath::GraphTraversal`；`EXPLAIN GRAPH BFS` 输出 direction/max_depth/identity/out/in 索引与估计候选数，禁止漏报或伪装成普通 SeqScan |
| Time-range 规范化 | 在 Stage B 实现 `created_at BETWEEN low AND high` 到 `created_at >= low AND created_at <= high` 的改写；复用现有 extra_filter AND/NULL 语义与精确 SeqScan 回退，测试 extra_filter 语义不打架 |
| reopen | 节点、边、属性、遍历顺序与结果在 reopen 后保持一致；未知 graph/start/深度响亮报错，空图返回空集 |

**验收**：
- OUT/IN/BOTH、0–3 跳、空图、缺失 start、自环、重复邻接、无向图、非法深度结果全部正确。
- **单节点零边遍历不得出现自环；只有 edge 记录时不得误判 node 存在**，直接证明 `record_type` 过滤没有被遗漏。
- 大 JSON/TOAST 与非法 JSON 路径无 panic、无静默转换；超限预算均返回 `Capacity`，不发生全图无界物化。

---

## Stage C：multi-AM GC coordinator（3–4 天）

| 任务 | 交付物 |
|---|---|
| `pg-storage::gc` | 实现 `GcParticipant::collect/reclaim` 与错误分类；只暴露 relation/page/horizon/LSN/TID 类型，不引入 AM→storage 反向依赖 |
| Coordinator | 在 `pg-engine` 实现单 worker + Condvar 调度、动态退避、启停 join；`gc_once` 与 worker 互斥且共用同一代码路径 |
| 队列与合并 | `BTreeMap<relation_oid,GcWorkItem>` 去重；同关系取 `min(old_horizon,new_horizon)` 后重新 collect，禁止拼接 dead set |
| 增量收集 | 按页/字节预算生成 `GcPageWork`，批次完成后让出执行权；禁止先构造全关系 dead set |
| Heap 回收 | 复用 dead scan、HOT 分组、compact/unlink/free、TOAST 回收；索引 key 提取和 durable 清理先于 compact |
| B+Tree 回收 | dead key 删除、页内 dead-space compaction；dead-entry 清理不修改父指针 |
| Inverted | segment merge 清除 tombstone、manifest 原子替换与 durable coverage 水位；禁止按 heap posting vacuum 处理 |
| HNSW | 复用 tombstone、redirect、dead-row channel 与 map 水位清理；不新增 HNSW→heap 依赖、不重建图、不改变 recall |
| Graph | GraphItem 与 identity/out/in 索引走 heap+B+Tree 协议；Graph metadata 与 sequence 同步回收 |
| 故障与背压 | 可重试 I/O/锁等待保留工作项；元数据/链/payload/generation 矛盾返回 `Corrupted`，不得伪装成功 |

**验收**：
- 多 AM 同库时无 TID 泄漏、无 dangling index entry、无重复回收。
- 不同 horizon 工作合并必须重新 collect，不能采用较新 horizon 误删旧快照数据。
- `gc_once` 与后台结果一致，worker 启停无泄漏、无未 join 线程。

---

## Stage D：空 leaf merge、generation 与 PageFreeV2（3–4 天）

| 任务 | 交付物 |
|---|---|
| 安全合并窗口 | 空 leaf 需要修改父 separator/downlink 时进入短 `AccessExclusive`，按单页/单批限制；空 root、非空 sibling、缺失 separator 或 generation 变化时不合并 |
| relation/page generation | DML、DROP、页链结构变化时单调递增；collect、索引清理前、物理回收前各复核一次，变化则丢弃并重新 collect |
| 父指针 | 新增 **`BTreePageCleanup=60`** 承载 separator/downlink 更新并 durable，payload 含 `parent_page_id/new_separator_key/base_generation/post_unlink_generation/operation_id`；不得用 heap 专用 `HeapCleanup` 修改 B+Tree 父页。`PageFreeV2` 只负责最终释放，不承担父指针修改；60 为新占号，必须登记原因与 golden bytes |
| PageFreeV2 字段 | 实现 `base_generation`、`post_unlink_generation`、`operation_id`；无 unlink 时 post=base；不得用当前代际精确匹配掩盖本次操作自身推进的代际 |
| 崩溃状态机 | 实现“未 collect / index cleanup done / compact done / unlink done / free done”逐状态恢复；已完成 operation_id 跳过，代际不符则放弃并重新 collect，metadata/CRC/链矛盾才 `Corrupted` |
| heap 与 B+Tree 链语义 | heap prev=前驱、next=后继；B+Tree prev/next 为左右 sibling；父 downlink 在 cleanup 中先更新，free 前必须验证当前链和 generation |
| DROP 生命周期 | 停止新工作并等待 in-flight traversal/GC，再清理 GraphEntry、identity/out/in 索引、GraphItem、metadata、sequence 与目录；清理失败只 warning + 后续重试 |

**验收**：
- compact/unlink/free 任意崩溃点只见可重放中间态，无断链、无错误页释放、无重复 free。
- 页复用、父 separator 改变、代际变化、DROP 与回收竞争全部通过。
- 不能形成半合并页，不能因本次操作自身推进 generation 而误拒合法回收。

**实现补充**：`operation_id` 从 `(relation_oid,base_generation,page_id)` 派生，不新增 ID 高水位；已释放页不得再次进入 collect。

---

## Stage E：联合回收、百万边与全 crash matrix（3–4 天）

| 任务 | 交付物 |
|---|---|
| 联合场景 | heap/B+Tree/HNSW/inverted/Graph 同时存在，覆盖长快照、部分 HOT 链、TOAST 共享 group、空 token upsert、并发 DML 与 GC |
| Graph 数据规模 | 固定 **15,625 nodes × 64 edges = 1,000,000 edges** 的基准数据集，包含自环、重复边和无向图；枢纽节点度数上限 **256**，同时记录平均度数、最大度数与数据/查询 SHA256，避免用无限 hub 改变验收口径 |
| Graph 基准 | release 下 OUT/IN/BOTH 的 3 跳遍历 **<50ms**，记录绝对延迟、warm/cold cache、并发度与预算配置 |
| GC 基准 | 记录单页/关系合并窗口、总回收时间、队列长度、GC 延迟与 CPU；**单页合并窗口 p99 < 100ms**，且不能退化为整表长暂停 |
| 全 crash matrix | WAL commit 前后、collect/reclaim 前后、compact/unlink/free 前后、manifest/页链替换前后、pending 溢出、TOAST 缺块、DROP 后重建、重复 open/reopen |
| 并发验收 | reader×GC×DML、reader×merge×DML、长快照×页复用、GC×DROP、worker 启停、预算耗尽与 `Capacity` |
| 资源验收 | visited、队列、批次页数/字节数有界；无后台线程泄漏、无全关系 dead set、无未 join worker |

**验收**：
- 全 crash matrix 无丢行、无重复、无半成品、无悬空索引项。
- 百万边 3 跳 <50ms，GC 无 TID 泄漏与 dangling index entry；原始命令、数据/查询 hash、环境与结果写入 `docs/phase5b-1-benchmarks.md`。
- 资源与暂停窗口数据完整，不能只报通过而不报暂停成本。

---

## Stage F：全量门禁、两轮对抗审查与归档（1–2 天）

| 任务 | 交付物 |
|---|---|
| 全量回归 | workspace 默认 feature debug + release；`pg-am-graph` debug + release；保留 TP/ANN/full-text/Wire/TOAST/DataFusion 全部回归，不发明不受支持的 workspace all-features test 口径 |
| 静态门禁 | `cargo check --workspace --all-features`、MSRV 1.86 实编译、逐 crate Clippy/rustdoc、fmt、`git diff --check` |
| 对抗审查 ×2 | 第一轮实现审查 → 修复 → 第二轮复核；重点为 Graph 完整性、GC 合并、generation、PageFreeV2、旧快照、DROP、预算与 crash |
| 覆盖率 | 若新增覆盖率，必须另注册 `pg-am-graph` 专用 job；不得沿用 pg-am-hnsw 的 90% 结果冒充本 crate 覆盖率 |
| 归档 | `docs/stage_spec.md` 增加 Phase 5b-1 各节（交付内容/关键约束/审查回流/验证数字）+ `docs/phase5b-1-benchmarks.md` + 本计划的最终实现补充 |

**验收命令（全阶段收口）**：

```bash
cargo test --workspace
cargo test --workspace --release
cargo test -p pg-am-graph
cargo test -p pg-am-graph --release
cargo test -p pg-engine --features datafusion
cargo test -p pg-engine --features datafusion --release
cargo check --workspace --all-features
for crate in pg-storage pg-txn pg-catalog pg-am-heap pg-am-btree pg-am-hnsw pg-am-inverted pg-am-graph pg-engine pg-wire; do
  cargo clippy -p "$crate" --all-targets --all-features -- -D warnings || exit 1
done
cargo fmt --all -- --check
for crate in pg-storage pg-txn pg-catalog pg-am-heap pg-am-btree pg-am-hnsw pg-am-inverted pg-am-graph pg-engine pg-wire; do
  RUSTDOCFLAGS="-D warnings" cargo doc -p "$crate" --all-features --no-deps || exit 1
done
CARGO_TARGET_DIR=target/msrv-phase5b cargo +1.86.0 check --workspace --all-features
```

---

## CI 注册清单

| # | 内容 | workflow/job | 运行条件 |
|---|------|-------------|---------|
| 1 | crate 骨架、AST、GraphItem/JSONB/PageFreeV2 golden tests（Stage 0） | ci.yml 新增 `pg-am-graph` matrix + pg-engine 既有 matrix | 每 push |
| 2 | Graph DDL/DML、完整性与 sequence（Stage A） | `pg-am-graph` test matrix | 每 push |
| 3 | BFS/DFS、属性与 PG Wire（Stage B） | 同上 + engine/wire 既有 matrix | 每 push |
| 4 | multi-AM GC、增量收集与回收（Stage C） | 单元每 push + crash rounds 手动/nightly | 每 push + 手动 |
| 5 | 空 leaf merge、generation、PageFreeV2（Stage D） | 单元每 push + crash rounds 手动/nightly | 每 push + 手动 |
| 6 | 联合回收、百万边、全 crash matrix（Stage E） | 单元每 push + 手动 release benchmark/nightly | 每 push + 手动 |
| 7 | 全量回归与两轮对抗审查（Stage F） | 全 workspace debug/release + MSRV + 静态门禁 | Stage F 收口时 |
| 8 | 覆盖率 | 独立 `pg-am-graph` coverage job（若注册） | 每 push；不得复用 pg-am-hnsw 结果 |

---

## 风险与开放问题（登记，不阻塞）

- **Graph DML 串行化**：三类完整性 DML 使用 `AccessExclusive`，写吞吐有明确上限；只有证明该协议正确后，才评估是否需要更细粒度的写锁，不在首版引入未定义的 graph write gate。
- **关系级短停写**：空 leaf merge 的父指针更新需要 `AccessExclusive`，这是首版明确接受的正确性降级；必须测量窗口，不能描述成完全 online/no-pause。
- **多关系 GC 头阻塞**：单 worker 简单可靠，但大关系可能拖慢其他关系；后续按公平调度和页预算优化，不提前引入多 worker 锁竞争。
- **Graph 遍历内存**：已有结果数、visited 和响应字节硬预算，超限返回 `Capacity`，不静默截断；枢纽节点场景仍需后续确定性分页。
- **JSONB 语义**：首版只做合法性校验和文本输出，不保证对象键序、数值精度比较或 JSONPath。
- **R2 行格式升级与多索引 backfill**：交叉引用 `docs/architecture-risks.md` R2。该 RFC 必须单独进行，**不在 5b-1 顺手实施**；当前 HNSW 的“仅空表可建、每表至多一个索引、不 backfill、行内单 NodeId”约束继续有效，GC coordinator 不得借此扩大范围。
- **超大图遍历**：百万边是验收规模，不是无限扩展承诺；分布式遍历、分页和更复杂图算法不在本阶段。
- **老目录兼容**：side file 不存在时按无 Graph 处理；side file 损坏或遇到未知 `GRAPH_AM_OID` 按未知 AM 处理，不能误认 B+Tree。
- **GC 回收失败**：后台错误保留工作项并重试，不能将未完成回收伪装成成功；严格磁盘配额与重试指标后续补齐。
- **覆盖率**：若新增 `pg-am-graph` coverage job，需独立阈值与报告，不能借用其他 crate 的数字。

---

## 修订记录

| 版本 | 日期 | 内容 |
|---|---|---|
| v1.0 | 2026-11-24 | 初稿：按技术选型 v1.1 拆出 Stage 0–F 的交付物、验收、CI、依赖锁定和两轮审查；覆盖 Graph 完整性、增量 GC、空 leaf merge、双代际 PageFreeV2、百万边与 crash 验收。 |
| v1.1 | 2026-11-24 | 审查修复：GraphItem 全部读取增加 `record_type` 过滤，避免节点被当成自环边、同名 edge 被当成节点；Stage 0 明确 Graph AM 的 `build/search/insert/delete/scan/freshness` 契约。`GcWork.pages` 改为 `BTreeMap<PageId,GcPageWork>`，collect/reclaim 共用去重页序，禁止重复回收。内部结果增加 `source_tid` 供未来 Fusion 回表，当前 SQL 投影不变；补单节点零边与仅有 edge 的判别性回归。 |
| v1.2 | 2026-11-24 | 外部审查修复：以 `graph/<oid>/metadata.bin` 替代 `pg_graphs` 系统表，避开 bootstrap/catalog 改动；冻结 identity/out/in 三套 GraphKeyCodec 与 edge_id 精确删除路径。新增 `BTreePageCleanup=60` 隔离 B+Tree 父 separator/downlink 与 heap cleanup。补 JSONB Arrow 映射、Time-range Stage B 归属、Graph name 唯一性、GraphTraversal EXPLAIN、p99<100ms 合并窗口、百万边度数上限与 R2 交叉登记。 |
