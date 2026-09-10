# Phase 2 M5 技术选型（HNSW WAL + 持久化）

> **状态：草案 v1.11(2026-09-09，第十轮审查 1 P1 + 2 P2 已修复，待复核；v1.10
> 复核收敛判定：机制面已无未攻击角落，待用户终审）。** 本文档定义 Phase 2
> 第二个 milestone(M5 = ROADMAP.md Phase 2b,**HNSW 变更进单一 WAL、崩溃后完整
> 恢复**）落地前所有跨模块的技术选择，对应 ROADMAP.md:264-278。
>
> 承接 M4(phase2-m4-tech-selection.md,§ 编号在 M4 侧已被代码注释长期引用）:
> M4 交付了纯内存 HNSW（建图/搜索/快照/CI recall 门槛），本 milestone 把图落到
> 节点页并接入 Phase 1 的 WAL/Buffer Pool/Checkpoint 基建。目标与 M4 一致：
> 所有影响数据编码、跨模块契约、M6 衔接面的决策先敲定；每个选择给
> "选项 → 选择 → 理由 → 代价"。章节编号（§1…§13）供代码注释长期引用。
>
> 文档中的代码引用（`file:line`）均为撰写时（2026-09-09）核实的事实；若后续
> 实现与引用不符，以代码为准并修订本文档。

---

## §1 范围与非目标

**M5 交付四块内容**（与 ROADMAP.md:264-278 对应）：

| # | 模块 | 对应章节 |
|---|------|---------|
| 1 | HNSW 节点页布局（图落 Buffer Pool 页） | §7 |
| 2 | WAL 记录与 redo(add_node/connect,生理路线） | §3、§4、§8、§10 |
| 3 | HNSW meta page(dim/metric/selection/params/入口点/最大层/rng seed/目录链头；NodeId 高水位由目录链导出，§6/§8.1) | §6 |
| 4 | 崩溃恢复验收（checkpoint + WAL 重放，kill -9 ×1000) | §9、§11 |

**对 ROADMAP 2b 表格的一处范围切分**（理由见 §8.4):

- **remove_node(tombstone)**:ROADMAP 把 tombstone 列在 2b,但 vacuum/连通性
  修复在 2c。M5 只交付 tombstone 的**记录格式与 redo 位**(flags 字节 M4 已预留，
  pg-am-hnsw/src/encoding.rs:204),**不交付**删除的查询期过滤与空间回收——墓碑
  节点的搜索过滤语义与 M6 的并发/vacuum 强耦合，M5 做一半会产生两套口径。
  划清：M5 = 记录能写、能重放、能校验；M6 = 生效。

**非目标（明确不做）**：

- **并发控制**:M6(Phase 2c)。M5 的页协议设计为 M6 的稳定寻址留位（§7.3),
  但无锁无并发测试。
- **tombstone 生效与 vacuum**：如上，归 M6。
- **SQL/DDL/catalog/reloptions 贯通**:Phase 4;M5 用 meta page 自包含（§6)。
- **f16/bf16 存储编码**(M4 §11 O2）与 **超维溢出**(M4 §11 O1)：归 M5
  开放问题窗口（§12),O1 给候选方向，O2 维持缓议。注意 O1 的区间表述
  已被本文件收窄（v1.1 审查第二轮 P3-2):M4 tech-selection §3:153 的
  "可行域约 dim ≤ 2000" 经 §7.2 公式化重算后为**硬上限 dim ≤ 1791**
  （默认参数），区间 (1791, 2000] 才是 O1 的待决域；M4 文档不改，
  以本文件为准。
- **min(rec_lsn) 恢复起点优化**:M1–M3 一直未启用（redo 起点恒为 checkpoint
  点）,M5 不启用，代价在 §9.2 量化。

---

## §2 Crate 归属与依赖方向

**选择：不新建 crate;`pg-am-hnsw` 新增 `pg-storage` 依赖边。**

**理由**：

- M4 的依赖冻结注释已预留此边（crates/pg-am-hnsw/Cargo.toml:"the pg-storage
  edge is deferred to M5, when WAL/buffer-pool integration creates a real
  consumer")——M5 正是那个真实消费点。
- 依赖方向 `pg-am-hnsw → pg-storage`(wal/buffer_pool/page/page_allocator/
  recovery 类型），维持**不依赖** `pg-am-heap`（节点页布局自建，heap 的
  tuple/slot 语义与 HNSW 节点记录不同构）、不依赖 `pg-txn`/`pg-engine`
  (M6 才接可见性）。32B PageHeader 与 pd_lsn 契约直接复用
  pg-storage/src/page.rs:1-36，不重发明。
- redo handler 接线点对齐 btree 先例：pg-am-hnsw 导出
  `hnsw_redo_handlers()`(crate 内新增 `redo.rs`),pg-engine 在
  `Engine::open` 注册（pg-engine/src/engine.rs:687-689 现有
  heap+txn+btree 的 extend 链上追加一行）。由此 pg-engine 新增对
  pg-am-hnsw 的依赖——这是 M5 才出现的第二条依赖边，与 Phase 1
  "依赖逐 milestone 扩大"原则一致。

**代价**:pg-am-hnsw 的测试矩阵不再与 pg-storage 解耦（M4 的编译/测试快
优势减弱）;`cargo test -p pg-am-hnsw` 的页相关测试需要真实数据目录。
用 feature gate(`storage = ["dep:pg-storage"]`）隔离不值得——M5 起
持久化就是主路径，假隔离只会留两套口径。

---

## §3 WAL 总路线（M5 第一决策）

**选项**：
(a) **生理记录 on 节点页**——与全部现存 AM 同构：页+槽位寻址、pd_lsn
    权威幂等（pg-storage/src/wal/record.rs:209 的 heap 先例）、FPI/checkpoint/
    崩溃测试基建全部白拿；
(b) **纯逻辑重放**——重放 insert 操作流，确定性重跑建图算法；
(c) **混合**——页驻图 + 生理 WAL 为主线，M4 快照文件保留为逻辑归档/迁移/
    调试通道。

**选择：(c)，即 (a) 为主线 + 快照降级保留。**

**先把 (b) 的 HNSW 独有论证写透**（这是与一般 DB"逻辑复制不可行"结论不同的
地方，必须正面处置）:M4 的图构建是**确定**的——同一 seed + 同一 insert
序列产出**同构建/同平台位级同构**的图（pg-am-hnsw/src/graph.rs:865-883 的
`same_seed_same_graph_byte_level_structure` 钉死；v1.5 审查 P2-2 收窄：
level 抽取消耗 `ln`(rng.rs:85-97)、cosine 距离消耗 `sqrt`
(distance.rs:78)，浮点超越函数跨平台无位级保证；v1.7 复核 P2-2 再收窄
——跨平台连"拓扑不变"也不承诺）——确定性口径是
同平台位级、跨平台仅**格式兼容与算法语义一致**（拓扑可因 ln 跨平台
差异而不同）。因此"逻辑重放
insert 流"在理论上能**精确重建**崩溃前的图（同平台），不是近似。这条路在
三种意义上诱人：记录极小（每节点一条 vector)、无需页协议、恢复天然无
半成品。

**但 (b) 死在恢复时长上，实测数字说话**:M4 实测建图 10k×128d = 4.22s
release(docs/phase2-m4-benchmarks.md:46；对照组：合成 bench 的 criterion
存档均值 5.633s,target/criterion),1M 全量逻辑
重放的**线性下界** ≈ **420 秒**（4.22s×100)，是 ROADMAP 30 秒上限
(ROADMAP.md:278）的 ~14 倍——且线性外推是**低估**:HNSW 建图为
O(n log n)，单次 insert 的搜索成本随 n 对数增长（log 1M / log 10k ≈ 1.5×),
真实重放 ≈ 630–850s(21–28× 上限；v1.4 P3：标注为下界以免外推被挑）。要让
(b) 达标只剩"高频快照 + 小窗口逻辑重放"一条路：每 checkpoint 全量快照 1M
图的代价（save 10k = 8ms 实测外推 1M ≈ 0.8s+ I/O，且快照是 O(n) 全量写出）远
高于页级 checkpoint 的脏页刷写；而且逻辑重放的崩溃原子性仍然要 WAL 记录
事务边界（否则重放到半个 insert)——页协议的复杂度并没省掉，只是换成了
逻辑记录的边界协议。**结论：(b) 不是"更简单",是"把复杂度搬到别处还附赠
14× 超时"**。

**(a) 的多页协议复杂度如实评估**:HNSW insert 触达多层多节点（新节点页 +
每层若干邻居页，pg-am-hnsw/src/graph.rs:436-451),确实比 heap 单页插入复杂。
但有一个 B+Tree 不具备的简化支点：**幽灵节点容忍**。B+Tree split 需要
Prepare/Copy/Commit 三步 + CLR + IncompleteSplitTracker
(pg-storage/src/recovery.rs:130)，因为半个 split 会使键**不可达**——正确性
致命。HNSW 相反：一个"已初始化但尚未被任何边指向"的节点对搜索**不可达
即无害**(HNSW 搜索从入口点沿边走，幽灵节点不进任何搜索结果）；已连边
而反向边未连的形态只轻微降低该节点的被召回率，不产生错误答案。因此
insert 的 WAL 协议可以序贯化（§8)：目录分配与节点入页先行，连边逐条 redo，崩溃半成品
由"不可达即无害"兜住，空间泄漏归 M6 vacuum 清理。**不需要
IncompleteSplitTracker 等价物**——这是 HNSW 相对 btree 模板的真实简化，
其成立条件是 §8.3 的不变量论证。

**代价**：生理路线要求图的主要状态驻页，内存图变成缓冲池上的一层视图——
§7 的布局与 §10 的 redo 都建立在此上；M4 的纯内存 `Hnsw` struct 保留为
页驻图的构建/测试对照物（snapshot 往返等价测试矩阵继续钉逻辑层）。

---

## §4 WAL 记录设计

### 4.1 判别值：LogicalHnsw=100 的处置

**事实**:`WalRecordType::LogicalHnsw = 100` 已在 Stage 0 预留
(pg-storage/src/wal/record.rs:74)，字面含义是"逻辑"操作，与 §3 的生理路线
存在语义张力。判别值禁重编号，tests/wal_record_type_discriminant.rs 钉全表。

**选项**：
(a) 复用 100 单类型 + payload 内子操作码；
(b) 申请新判别值段（建议 121–127)，每操作一值；
(c) 把 100 重定义为生理记录。

**选择：(b)。**

**理由**:heap/btree 先例是**每操作一判别值**(record.rs 的 PageAlloc=40、
BTreeSplitCLR/Copy/Commit=50/51/52 等）,redo dispatch 按类型直派
(pg-storage/src/recovery.rs:303 未注册即硬失败）、重复注册 panic
(recovery.rs:295)——单类型+子操作码会把这套护栏降级为 handler 内部的
switch，注册表对"缺子操作 handler"失明。子操作码方案的唯一优点是省判别值
空间，但 u8 判别值空位充足。(c) 污染 Stage 0 的冻结语义，不取。

**100 的处置**：保留为**真正逻辑级**操作的预留位（M6 的事务性 remove、
维护操作候选），本 milestone 不注册 handler——恢复遇到 100 硬失败
(recovery.rs:303)，正是 SegmentSeal=110 先例的"先预留、后注册"模式
(record.rs:81-88 的 payload 契约注释模式照抄）。

### 4.2 生理记录集（M5 交付）

| 判别值（建议段） | 记录 | payload(bincode standard,对齐先例） |
|---|---|---|
| 121 | HnswNodeInit | (meta_page_id, page, slot, node_id, level, dim, vector)——节点条目按 level **定长预留**创建，state=INITIALIZING(§7.2/§8.1,v1.5) |
| 122 | HnswSetNeighbors | (meta_page_id, page, slot, level, count, 邻居内容）——**原位更新**（定长预留内改 count+内容，条目永不扩搬，v1.5;meta_page_id 供 redo 侧容量校验，v1.8) |
| 123 | HnswMetaUpdate | (meta_page_id, 字段后像：入口点/最大层） |
| 124 | HnswNodeTombstone | (page, slot, node_id) —— 格式交付，语义 M6 生效（§1) |
| 125 | HnswDirAppend | (目录尾页， node_id → (page u64, slot u16) 条目后像，条目宽 10B)——**真单页记录**(v1.2) |
| 126 | HnswDirLink | (旧尾页， next_page_id 链指针后像）——目录扩容专用，单页 |
| 127 | HnswPublishLive | (page, slot, node_id)——state 后像翻 LIVE(v1.7，见下） |

**LIVE 翻转的承载记录**(v1.7 审查 P1-2——v1.5/v1.6 把翻转"并入"步骤 6 的
SetNeighbors，但该记录 payload 没有 publish_live 标志，翻转实际上无 WAL
承载）。三案：① SetNeighbors 加标志位——翻转必须在**全部**自身层写完
之后，逐层多条记录里"哪条承担翻转"把记录语义耦合到步骤序，层数变化
即漂移，否；② 独立 HnswPublishLive 记录（选定）——语义最清晰，记录极小
（页+槽+id)，幂等天然（state 后像覆写）,§8.2 窗口表只需新增一行；
③ 一条记录写全部自身层——payload 变长（≤ m_max0 + L×m 个 id，预算内），
但把"写内容"与"翻状态"两个语义揉进一条记录，不如 ② 干净。
**state 位的语义定位**(v1.6 P2-1 已立，此处钉死）:INITIALIZING/LIVE 是
**审计与恢复统计标记**——全连通但 INITIALIZING 的节点是合法终态（崩溃
产物），搜索语义对两态一致；PublishLive 使"完整插入"在 WAL 中显式可辨
（崩溃轮次统计、M6 vacuum 的清理对象判定）。

**记录自包含规则**(v1.5 审查 P1-6):WalRecord 头无 page 字段
(record.rs:705-718),RedoContext 无索引定位能力（recovery.rs:210-229
字段集核实：buffer_pool/page_allocator/clog/att/dpt/incomplete_splits,
无索引注册表）——每条 HNSW 记录的 payload 自包含全部目标页；NodeInit、
SetNeighbors 与 MetaUpdate 额外携带 `meta_page_id`(NodeInit 的 apply
要按 meta 的 m 重算 L_max、SetNeighbors 的 apply 要按 meta 的 m/m_max0
校验 count ≤ 该层容量——层容量分解需要 m 与 m_max0，条目自描述无法
反解这个分裂，v1.7 第七轮 P3-2 修正；见 §10.1 纵深校验；对齐 btree
记录 payload 带 page_id 的先例
record.rs:891-904)。redo 时 meta page 经 ctx.buffer_pool 可读（Stage I 起
恒 Some),meta 创建于任何 insert 记录之前，redo 前缀序保证其时 meta
状态已合法。

**目录高水位由链导出，不落任何字段**(v1.1 审查第二轮新 P1 修复——v1.1
的 HnswDirAppend 携带 HWM 后像是**单记录写两页**（条目落尾页、HWM 落
头页）:WalRecord 头无 page_id 复数、pd_lsn 守卫按页判定，codebase 全部
先例都是单页模式，btree 正因为多页原子不可得才需要三步协议 + CLR——
v1.1 悄悄引入了同型两页写，朴素实现（头页守卫）会产生"HWM 已推进但
尾页条目丢失"的损坏态。v1.2 处置为**方向 (b)**：目录页自描述（页头区带
链序 ordinal + next 指针）,meta page 只持**链头 PageId**（创建后不变）;
HWM 不落字段，= **tail.ordinal × 813 + tail.count**（每页容量 813 是 §7.1
的格式常量；中间页恰满由 §11.3 的链结构四断言钉住，故公式精确）——
链尾由沿链遍历定位（条目定长 10B——v1.5 审查
P1-1 宽度修正：PageId 是 u64(types.rs:46),6B 的核算全错；槽位即序位，
M5 单线程写入保证链前缀一致——无空洞）。备选 (a)（明文两页应用语义：
头/尾各按各页 pd_lsn 守卫 + 后像绝对值收敛）不选：那是 codebase 没有的
新模式，要写透并配幂等测试，而 (b) 用既有单页先例就闭合了同一契约；
链尾定位成本实测有界（1M 节点 ≈ 1231 页 × 8KB ≈ 10MB(9.6MiB）一次性 open 遍历，
毫秒级；更大规模或需从页分配器高水位反推候选的优化登记为开工期实测点，
不硬编）。

节点页分配**复用 `PageAlloc = 40`**（页分配器统一 WAL 先例），不新设判别值
——新增段即 121–127，落码时同步 tests/wal_record_type_discriminant.rs 钉表。
**真相源唯一**:NodeId 分配历史 = 目录链内容本身（链只追加、条目覆写幂等，
各页按各自 pd_lsn 守卫）;meta page 与目录头页都不持有高水位字段。

**设计要点**：

- **连边/shrink 统一为原位更新**(HnswSetNeighbors;v1.5 审查 P1-2 语义
  改写）：节点条目在 NodeInit 时按 level **定长预留**(level 0 按 m_max0、
  上层按 m 满载容量，§7.2),SetNeighbors 只在预留空间内原位改
  count+内容——条目物理位置与大小终身不变，redo = 幂等覆写，与 heap 的
  post-image 先例（record.rs:189）同构。v1.1 选"整列后像"的理由
  （shrink 对称 + handler 无状态 + 无读时依赖；delta 编码在 LSN 序前缀态
  下其实可行，v1.1 P2-3 已照实登记）在定长预留下全部保留。**写放大
  核算**(dim=128 默认参数，稳态邻接近满；核算口径——页驻实现尚不存在）:
  每 insert ≈ NodeInit(512B 向量 + 开销 ≈ 522B;WAL 只记内容不记预留
  空位）+ DirAppend(~26B:dir_page u64 + node_id u32 + 目标 page u64 +
  slot u16 + bincode 开销，**只写尾页**——v1.2 后无头页热点；目录扩容
  摊销 ≈ 每 813 insert 一条 DirLink)+ level-0 的 ~16 个邻居页原位更新
  (16 × 130B ≈ 2.1KB)+ 自身列表（66B)+ 偶发上层与 meta 记录 ≈
  **2.8KB/insert ≈ 向量本体的 5.4×**(v1.0 审查者估算 4–5KB/≈9× 偏高——
  其把上层与双向 shrink 全量计入；本文以核算口径重算，系数随 m/m_max0
  线性增长。v1.10 nano:v1.8 给 SetNeighbors 加 meta_page_id 后
  +16×8B ≈ 128B，从 2.7KB 升至此值）。
  1M 全量建库 ≈ **2.8GB WAL**——接入 §9.2 的恢复预算讨论（checkpoint
  频率控制重放窗口；bulk load 后立即 checkpoint 是 §9.2 的硬要求）。
- **幂等锚 = pd_lsn**(`page.pd_lsn >= record.lsn` 跳过，heap 先例
  record.rs:209)；页内内容 authoritative，无读时依赖。
- **payload 版本**:payload 版本 nibble 在 **u8** flags 的高 4 位
  (`version = flags >> 4`,record.rs:638-643——注意 M1 冻结的 32B 记录头里
  flags 是 u8，不是 u16)。HNSW 全部新记录从 v0 起步。

### 4.3 payload 预算核算

上限 u16::MAX = 65535B(writer.rs:253/331/502 三处 append 路径校验）。
最大单记录 = HnswNodeInit @ dim 硬上限 1791(§7.2 的容量不变量，v1.1
P1-1 修正后）:vector 7164B + level(1B)+ 元组开销 < 8KB。§12 O1 的
超上限候选方向若落地，单记录 64KB 容纳 dim ≤ ~16000 的向量仍够；多页
节点链方案的首记录也在预算内。**预算无风险**。

---

## §5 续插语义裁决（正式关闭 M4 v1.6 开放问题）

**M4 遗留**:load 出的图 `insert` 报 `InvalidOperation`(graph.rs:393-401,
保守默认）;PRNG 状态不进快照/日志，续插的 level 来源未定（M4 §4.1 前提③)。

**选择：WAL insert 路径不重抽 level——drawn level 随记录走。**

**理由**:

- M4 tech-selection :202 已倾向此口径（"M5 的 WAL 路径天然无此问题
  （重放不重抽）")，本文档将其从倾向升为裁决。
- redo 必须在任意时刻、任意起点重放且结果一致：若 level 在重放时重抽，
  则重放依赖 rng 状态流，崩溃点前后不可衔接——生理记录要求 payload
  自包含，HnswNodeInit 携带 `level` 字段（§4.2）正是这一要求的直接推论。
- **正常（非重放）insert 仍由图的 rng 抽 level**(rng.rs:80 `next_level`),
  抽出后写入 WAL 记录再应用——"抽一次，记录一次，重放 N 次"。
- rng 状态因此**无需**进 WAL/快照/页，v1.6 开放问题的三处改点
  (from_parts 的 seed/read_only/selection,graph.rs:290-299）处置为：
  seed 保持惰性（页驻图的正常 insert 走 rng 实例，WAL 应用路径不碰）;
  read_only 语义随 M4 内存图保留（内存图 load 后仍只读，页驻图是另一条
  构建路径）;selection 由 meta page 持久化（§6)。
- **rng 种子来源与生命周期**(v1.1 审查 P2-2——v1.0 未指定）：种子在
  **索引创建时生成一次并钉进 meta page**(§6 字段表新增 `rng_seed`),
  非秘密、可展示；重放路径不消费它（level 随记录走，本节裁决）。
  备选（engine 级确定性 seed 链）不选：索引级自包含使 pg-am-hnsw 单测
  无需 engine，与 meta page 自包含原则（§6）一致。
- **恢复后 rng 流的接续**(v1.5 审查 P1-4——v1.1 的"用该 seed 重新拉起"
  会使流从头重复，三条路成本分析后选一）：种子只是初态
  (Xoshiro256StarStar::new 从 seed 回到初始状态，rng.rs:33-41),
  恢复后新 insert 的 level 必须从**流位置 n = 节点数**继续。三案：
  (a) 不接续（从头重复）：图质量无害——level 流是几何 i.i.d. 序列，
  复用流位置不与节点内容产生相关；但崩溃前后不再共享一条流世系，
  §11.1 的"同序列同 level 流"在崩溃轮次里不再成立，且"每次插入独立
  抽取"的语义叙事被破坏（无已知害处但有解释成本）;
  (b) 256-bit state 持久化进 meta page：每次抽取后更新 = meta 页每
  insert 都脏，+1 条 meta WAL 记录/insert 的写放大 + meta 页刷写热点，
  成本真实且持续；
  (c) **按节点数精确推进**（选定）:rng.rs 无 jump 函数（核实）,
  skip-ahead = open 时重放 `next_level(m)` × HWM 次——`next_level`
  自含 u==0.0 重抽（rng.rs:89-91)，按节点数重放精确复现消费（含重抽）;
  成本 O(HWM) 次抽取，xoshiro 每抽数次 CPU 操作，1M 节点 ≈ 毫秒级
  （开工期实测钉死）。零写放大、世系精确延续——恢复后续插等价于
  从未崩溃的运行的继续，§11.1 的 level 流可复现性由此**升级为跨崩溃
  成立**。

**代价**:HnswNodeInit 记录多带 1 字节 level；图 API 需要"指定 level 插入"
的内部入口（§10.2 的物理应用原语）——M4 深审已列为 M5 既定任务。

---

## §6 HNSW meta page（参数与运行时属性落点）

**选择：自建 HNSW meta page = 索引的 first_page,btree 先例
(pg-am-btree/src/index.rs:406-446;meta record 编码对齐
`pg_am_btree::page::encode_meta_record`,page.rs:426)。**

**与 btree meta 先例的差异声明**(v1.1 审查 P3-5，不隐含同构）:btree 的
meta 更新是 **append 新 slot 记录、最新者权威**(index.rs:565-575 的
`write_meta_record`);HNSW 的 HnswMetaUpdate 是**字段级覆写后像**（入口点/
最大层是单值字段，append 式历史对它们无意义）。meta page 物理形态同为
slotted page，更新语义不同，redo handler 不复用 btree 的。

**存放**（创建时钉死，加载/重放校验，错配硬失败）:

| 字段 | 来源/理由 |
|---|---|
| dim, m, m_max0, ef_construction | 与快照 header 同集（encoding.rs:84-100) |
| **metric** | M4 已知残留（快照不带）；页驻图必须自描述，否则重放/加载的 metric 错配静默毁图 |
| **neighbor_selection** | 同上（graph.rs:245-250 残留）；续插开放后必须持久化 |
| **rng_seed** | §5(v1.1 P2-2)：创建时钉死，非秘密；恢复正常 insert 的 level 流由此可复现 |
| ef_search_default | 查询期默认，进 meta page 便于展示与默认恢复；可被会话覆盖。**轻重之分**(v1.4 nano)：它只是性能旋钮——错配不毁图、不毁答案正确性，与 metric/selection/rng_seed 的"错配静默毁图"不同级；M4 快照恰恰是因此把它排除在外（load 时调用方供给）。此处收回的理由仅是页驻图自描述的完整性，校验时可放宽为 WARN 而非硬失败——开工时定 |
| entry_point, max_level | ROADMAP 的 "Checkpoint HNSW 快照"字段（ROADMAP.md:272) |
| **目录链头 PageId** | v1.2：目录页链的唯一入口（创建后不变）；链自身携带序位与 next 指针（§7.1) |
| snapshot 格式版本引用 | 与 M4 v1 逻辑归档格式的兼容声明 |

**NodeId 高水位不在此表，也不在任何字段**(v1.1 审查第二轮新 P1):HWM
由目录链**导出**（公式 `HWM = tail.ordinal × 813 + tail.count`,§7.1
v1.7 P3)，分配历史即链内容——任何
落字段的 HWM 都与链构成双真相源，崩溃后可分叉（v1.0 双写 meta page 是
双源；v1.1 的"记录携带 HWM 后像"则把 DirAppend 变成尾页+头页的两页
单记录，突破 pd_lsn 单页守卫先例）。M4 §3 "never reused" 冻结契约
(params.rs:5-7）由链的"只追加 + 条目覆写幂等"保证，无需独立字段。

**备选及不选**:catalog/reloptions 落点（Phase 4 才有 catalog 贯通，M5 等不起；
且 meta page 自包含使 pg-am-hnsw 单测无需 engine);piggyback 到 pg-storage
superblock(superblock 是全局单例，record.rs 判别值段与 superblock 字段都是
冻结面，索引级状态不该进全局页）。

**redo**:meta page 更新走 HnswMetaUpdate(§4.2)，页寻址生理记录，与数据页
同机制；payload 携带 `meta_page_id`（自包含规则，§4.2)——open 时 meta
page 的定位走 first_page 惯例（engine 侧已知）,redo 不依赖该惯例。

---

## §7 节点页布局

### 7.1 单页单节点 vs 单页多节点

**选项**:
(a) **单页单节点**——节点页 = NodeId 直接寻址，页内布局自由；
(b) **slotted 单页多节点**——复用 pg-storage 32B PageHeader + slot 间接层，
    节点地址 = (PageId, SlotId),NodeId→地址由目录结构解析。

**选择：(b),slotted 多节点 + NodeId 目录页。**

**理由（空间核算是决定性的）**:M4 冻结参数（M=16, m_max0=32）下 dim=128
节点的典型体量 = vector 512B + level_count 1B + level-0 邻接（满载
130B)+ 上层邻接——v1.1 审查 P3-2 更正：上层邻接的**无条件**期望 =
E[上层数] × 满载 66B = (1/15) × 66 ≈ **4.4B**(v1.0 写的 ≈64B 是"已知有
上层"条件下的条件期望量级，口径错误；结论不变）——典型条目 ≈ **0.65KB**,
上限 ~1KB/节点。单页单节点在 8KB 页（PAGE_SIZE 8192）下
利用率 ~8%,1M 节点 = 1M 页 ≈ **8GB** 数据文件——是向量本体（0.5GB）的
16 倍，直接顶穿 §11 R3 的内存/磁盘口径。slotted 多节点把利用率拉到
80%+,1M 节点 ≈ 0.9GB。slot 间接层同时满足 M6 稳定寻址：页内条目搬移/
压缩时 slot 号不变（heap 页已证明此模式）。

**NodeId→(PageId, SlotId) 解析**:NodeId 稠密递增（M4 §3 契约），目录用
**定长数组页链**(NodeId/每页条目数 = 页索引，余数 = 页内偏移）。
**目录页编码冻结**(v1.5 审查 P1-1 重算 + 格式常量纪律，v1.2 P3-3):
32B PageHeader + 目录自描述头（version u8 | flags u8 | reserved u16 |
ordinal u64 | count u32 | next u64 = 24B)= 56B；条目 = PageId u64 + SlotId
u16 = **10B**；每页条目 = ⌊(8192−56)/10⌋ = **813**（格式常量——"槽位即
序位"的序位算术依赖它，任何改动 = 格式修订，必须过修订记录）。
**PageId 全宽 u64、不截断**(P1-1 附带裁决）：截断 u32 的隐含上限 =
2^32 页 × 8KB = 32TB 数据文件——1M 节点库（~1GB）虽远在限内，但
PageId 全库统一分配（所有 relation 共享），截断制造"超限静默错址"的
隐性契约；目录总开销占比极小（见下），省 4B 不值。1M 节点 →
⌈1M/813⌉ = **1231 页 ≈ 10MB(9.6MiB)** 目录链。
目录页**自描述**(v1.2)：页头区携带链序 ordinal 与 next 指针（next =
INVALID 即链尾）,meta page 只持链头 PageId(§6)；扩容 = 新页 PageAlloc +
FPI（自描述头随之落盘）+ 旧尾页写链指针（HnswDirLink，单页记录）。
目录页走 PageAlloc/freelist 与 WAL(PageAlloc 先例）,redo 幂等同 pd_lsn
（链指针覆写与条目覆写各按各页守卫）。**恢复时的链尾定位**:open 后
从链头遍历至末页，**HWM = tail.ordinal × 813 + tail.count**(v1.7 P3
公式化；"读末页计数"的含糊口径废弃）——1M 节点 ≈ 1231 页 ≈ 10MB 一次性
遍历，毫秒级；更大规模的优化（如 checkpoint 后从页分配器高水位反推候选页）
登记为开工期实测点（v1.2，不硬编）。
**备选及不选**:meta page 内嵌目录（1M 节点放不下）；全局 B+Tree 当目录
（循环依赖 pg-am-btree，违反依赖方向）。

### 7.2 页内组织

- 32B PageHeader 起手，pd_lsn 权威（page.rs:29-36 契约：一切 WAL 下的修改
  经 set_page_pd_lsn);pin_mut 的 FPI 机制自动生效（buffer_pool.rs:297)。
- 节点条目**定长分档**(v1.5 审查 P1-2 重写——v1.4 的变长条目与 §7.3 给
  M6 承诺的 slot 稳定寻址存在协议冲突：slotted 插入只向连续空闲区放置
  元组（pg-am-heap/src/slotted_page.rs:270-329 实证），变长扩容必须搬移
  条目，slot 稳定性即破裂）。NodeInit 时按抽取的 level 一次性预留各层
  满载容量（level 0:2+4·m_max0；上层每层 2+4·m),state=INITIALIZING、
  各层 count=0；之后 SetNeighbors 只原位改 count+内容（§4.2)，条目位置
  与大小终身不变。与 M4 §3 记录布局的对应关系不变（vector 与 neighbors
  紧邻，M4 tech-selection :150 的探路兑现）。
- **容量不变量**(v1.1 审查 P1-1 重写；v1.5 P1-2 后公式由"最坏情况"变为
  **精确值**——每个条目都按满载预留）。页可用面 = PAGE_SIZE − 32
  (PageHeader)− 4(LinePointer,pg-am-heap/src/line_pointer.rs:16);
  8KB 页下 = **8156B**。节点条目字节数 =
  `4·dim + 1(level_count/state)+ (2 + 4·m_max0)+ L·(2 + 4·m)`
  (L = 抽取的上层数，全按满载预留计)。**1B 状态的位布局**
  (v1.7 审查 P1-1——v1.6 nano 的"level_count 4bit + state 1bit"打包在
  合法 M=2 下溢出：M=2 时 L_max = ⌊53·ln2/ln 2⌋ = 53,level_count 可达
  54,4bit(≤15）放不下）:

  | 位 | 字段 | 域 |
  |---|---|---|
  | 0–5 | top_level(= level_count − 1) | 0–63 |
  | 6 | state(0=INITIALIZING, 1=LIVE) | §8.1 状态机 |
  | 7 | tombstone | M6 生效（兑现 M4 的 flags 预留诉求，encoding.rs:204) |

  口径一致性（P1-1 附带核对）：快照格式 cap 的是 level_count ≤ 64
  (encoding.rs:79 MAX_LEVEL_COUNT)⟺ top_level ≤ 63 ⟺ **6bit 恰好放下**;
  页格式与快照格式同一条上限，无第二口径。最坏层数 53(M=2)与
  M=16 的 13 均在 0–63 内。**创建时硬校验**：按最坏层数
  L_max = ⌊53·ln2/ln M⌋(rng.rs:93-96 的重抽后硬上界，M=16 时 = 13)
  反解 dim 上限——默认参数、8KB 页下
  `dim ≤ ⌊(8156 − 1 − 130 − 13×66)/4⌋ = 1791`;dim/m/m_max0 作为
  **乘积联动校验**在索引创建时一并执行（meta page 落值前），超限即
  响亮拒绝。**(1791, 2000] 区间归 O1**(§12)。
  **定长预留的空间代价**(如实）：水平 0 预留浪费 = (m_max0 − 实际度数)×4B
  ≈ 0–64B/节点（默认参数，~0–10%);上层按抽取分档，不抽不付；1M×128d
  全库文件 ≈ 0.95–1.0GB(v1.4 变长口径 0.9GB,**+5–10%**)。买回的是：
  条目零搬移（碎裂消除）、slot 稳定（M6 承诺兑现）、redo 幂等原位覆写。
- **PAGE_SIZE 参数化**(v1.5 审查 P2-3):pg-storage 有 `page-size-16k`
  feature(types.rs:12-27,PAGE_SIZE 编译期常量，目录重建）。容量公式以
  `pg_storage::types::PAGE_SIZE` 为参（不硬编码 8192),16k 下上限按同公式
  自动放宽；但 **M5 验收矩阵只钉 8KB**(§13),16k 配置标为未验证——
  pg-am-hnsw 不做编译期拒绝（公式已参数化，拒绝无收益），未验证口径
  写进验收。
- "页满溢出到新页"与"条目不跨页"的关系（v1.1 澄清）：前者治**多个合规
  条目装不进一页**（新条目溢出到新页，slot 目录不变）；单条超页它治不了
  ——那是创建时校验的职责，运行期不存在该窗口。
- m_max0 的上限不再单列建议值，并入上述联动校验（创建时按公式判定，
  不写死 128；理由：写死值在不同 dim 下或过松或过紧，公式校验才是真
  不变量）。

### 7.3 为 M6 留的位

- slot 稳定寻址（7.1);tombstone flags 位（M4 已预留于记录格式）;
- **条目零搬移**(v1.5 P1-2 后升级为硬保证）：节点条目定长分档、原位
  更新（§7.2),INITIALIZING 条目 = 定长预留的零 count 形态（§8.1),
  insert 全程不搬移任何既有条目——M6 的 latch 化因此可以把"读 slot
  内容"设计为读 latch + 写 latch 只在原位更新窗口持有；页内压缩/碎片
  整理在定长分档下不再必要（无变长碎裂来源），显式压缩概念退役。

---

## §8 insert 的 WAL 记录序列与崩溃窗口语义

### 8.1 正常路径步骤

一次 insert(level 由 rng 抽取，§5)。**v1.7 形态**(v1.5 审查的 P1-3 分配
顺序 + P1-2 定长预留 + P1-5 入口点条件；本轮 P1-2 独立 PublishLive +
本轮 P1-4 的 meta 窗口闭合重排）:

1. （若节点页/目录页需新页）页初始化链（v1.9 审查 P1 补全——v1.8 只写
   "PageAlloc + FPI"，节点页的初始化协议不完整）:`new_page` → **初始化
   HNSW 页头**(32B PageHeader + 页类型标记；目录页另写自描述字段
   ordinal/next=INVALID/count=0——布局引用 §7.1/§7.2 的冻结格式）→
   `log_page_init`（对**初始化后**的页做 post-image
   FPI + stamp pd_lsn,§10.3 创建协议同款，btree index.rs:3299-3310
   先例）→ 首个细粒度记录（节点页 = NodeInit；目录页 = DirAppend——
   v1.10 nano:DirLink 写的是旧尾页，不是新页）。**回收页口径明文**:post-image FPI 的
   内容是初始化后的合法 HNSW 页（页头已写），**不是零页**——A1 契约
   (buffer_pool.rs:424-442）原文要求调用方自 log 初始化 post-image,
   因为回收页的旧租户映像还在盘上，pd_lsn 守卫的重放会把撕裂页误判为
   已应用；新分配页（全零）同样必须走此链——全零不是合法 HNSW 页
   (pd_lower=0 的 slotted 页头无效），两情形一视同仁，无例外分支；
2. （若目录尾页满）`HnswDirLink`：旧尾页写链指针指向新页（单页记录）;
3. `HnswNodeInit`：节点条目**先**在节点页 (page, slot) 创建——定长预留、
   state=INITIALIZING、各层 count=0（单页记录）。**槽先于映射占用**
   (v1.5 审查 P1-3 修复：v1.4 是先发目录映射后建节点槽，崩溃后目录指向
   空槽、后续分配复用该槽 → 两个 NodeId 指向同一节点；新顺序下目录发布
   时槽必已被占用，复用窗口结构性消除）;
4. `HnswDirAppend`：目录尾页发布映射 `node_id → (page, slot)`（单页记录）
   ——**id 分配即映射发布**;NodeId = 链上序位，无独立高水位步骤。
   补充语义（P1-3 配套）:**未发布的 NodeId 不构成分配**——若崩溃发生在
   3 后 4 前，redo 只重建 INITIALIZING 孤儿条目（无目录映射、不可寻址、
   记账归 M6)，链导出 HWM 不含它，该 NodeId 可被下次 insert 合法复用
   （它从未可观察；M4 §3 的 "never reused" 约束的是可寻址身份）;
5. 自顶层向 level 0，逐层对每个被选邻居：`HnswSetNeighbors`（邻居页，
   原位更新，含新节点）;shrink 同样原位更新；
6. 新节点自身各层邻接列表写（同属 HnswSetNeighbors，自身页原位更新）;
7. 若**空图或 level > max_level**:`HnswMetaUpdate`（入口点/最大层）。
   **meta 更新先于 PublishLive**(v1.7 审查 P1-4 的重排，论证见 §8.2
   窗口表末两行）：入口点永远只指向自身邻接已写完的节点（步骤 6 后）,
   不指向空列表的 INITIALIZING 节点（那会使搜索从死胡同出发丢答案）。
   （空图分支的依据：v1.5 审查 P1-5——条件只有 `level > max_level` 时
   空图首节点（level=0 = 初始 max_level）不触发，入口点永不发布；内存
   实现的空图特判 graph.rs:418-422 为对照实证。）
8. `HnswPublishLive`:state 翻 LIVE（独立记录 127,v1.7 P1-2)。

**成功边界与可见性**(v1.5 审查 P1-7，明文四条）:
① insert 的耐久边界 = `flush_to`（本 insert 全部记录的最后 LSN)——对齐
   group commit 惯例（pg-txn/src/manager.rs:14-16 的 append→flush_to 硬序；
   HNSW 无 CLOG 位，flush_to 即边界）;
② **未返回成功的 insert 恢复后允许可见**——索引记录 txn_id 恒 INVALID、
   无 undo;**措辞修正**(v1.6 复核 P3-4)：这不是"与 heap/btree 同口径"
   ——heap/btree 的未提交不可见由 CLOG 事务状态 + index-undo 达成，
   HNSW M5 无 undo 层，正确表述是**可见性 = 记录持久性前缀**（未达
   flush_to 的记录若被 checkpoint 的 WAL-first 刷盘捎带持久化，恢复后
   即可见，heap 同情形不可见）。M5 utility-only 范围内无实际后果；
   M6 开放事务写入时必须重议（loser 对接已登记 M6，见下）。调用方不得
   把"返回前崩溃"当作未插入的语义依据;
③ 状态机：INITIALIZING（创建至步骤 8 前）→ LIVE(步骤 8 的
   HnswPublishLive 翻转）。**搜索语义**(v1.6 复核 P2-1 修正——v1.5 的
   "搜索永不解析 INITIALIZING"与本节窗口表自相矛盾）:INITIALIZING 节点
   自步骤 5 起**可有入边、可被搜索召回**——有向量即合法部分连通答案
   (§8.2 交错态行），其 count=0 列表是有向死胡同；步骤 5 之前无入边、
   从入口点不可达。直接查找 API 的规则收窄：解析到 INITIALIZING 条目
   返回向量 + 状态标记（不报错——步骤 5–8 间该形态合法）;只有解析到
   目录映射指向的**空槽/越界槽**才响亮报错（到达即 bug/损坏）;
④ 见 §8.2 窗口表的交错态枚举。

WAL 先行由 buffer pool 保证（flush_frame 强制，buffer_pool.rs:876);
commit 硬序（append → flush_to → CLOG 位，pg-txn/src/manager.rs:14-16)
M5 不涉及。**loser 补偿段重写**(v1.1 审查 P2-1——v1.0 的"loser 补偿归
pg-engine index-undo"是假安全：index-undo 对 HNSW 不适用，且 M5 无删除
API,abort 的 insert 留下的不是 btree 式 loser 残留而是**可达节点**):
M5 的 HNSW 写入**不挂事务性 DML**——只以 utility/auto-commit 形态存在
（建库/bulk load 路径），不存在 abort 窗口；索引记录 txn_id 恒 INVALID
的 Phase 1 惯例沿用。pg-engine index-undo 的 HNSW 对接列为 **M6 前置
登记项**（那时 tombstone 生效、事务性写入开放，loser 补偿才有对象）。

### 8.2 崩溃窗口逐点分析（步骤编号对应 8.1 v1.9 形态）

| 崩溃点 | 盘上状态 | redo 行为 | 图语义 |
|---|---|---|---|
| 1 前 | 无痕迹 | 无 | 一致 |
| 1–2 间（目录扩容中） | 新页已 PageAlloc 未链接 | PageAlloc/FPI 幂等重放 | **孤儿页**（已分配未入链）：无害空间泄漏，记账归 M6；链尾仍是旧页 |
| 2 后 3 前 | 链已延长，新尾页空 | DirLink 幂等重放（各页 pd_lsn 守卫） | 一致（空尾页合法） |
| 3 后 4 前 | INITIALIZING 条目在页，目录无映射 | NodeInit 幂等重放 | **孤儿条目**（不可寻址，无入边），无害泄漏；该 NodeId 未发布 = 未分配，可合法复用（8.1④) |
| 4 后 5 前 | 目录映射已发布，节点 INITIALIZING | DirAppend 幂等重放 | 幽灵映射：搜索不可达（无入边）= 无害；直接查找返回向量 + INITIALIZING 标记（8.1③ 收窄后规则） |
| 5 中（交错态枚举，v1.5 P1-7④) | 部分邻居页已原位含新节点，其余未写 | 各 SetNeighbors 幂等重放 | 部分连通；搜索可能召回该节点（有向量、有部分边）——**答案仍合法**（它是真实插入过的向量） |
| 5 中（shrink 交错） | 某邻居页的 shrink 剔除已生效（v1.6 措辞修正：M4 shrink 是任意位置剔除 + 按 NodeId 重排，graph.rs:691-702，非尾部截断），但新边（或其他边）未连 | 同上 | 该邻居丢一条旧边：度数仍 ≤ cap，图无畸形；连通性受概率性影响——合法恢复结果，recall 门槛验收兜底（§13.7) |
| 5 后 6 前 | 反向边齐，自身列表空 | 重放补齐 | 可被指向但不指出——召回率轻微下降，无错误 |
| 6 后 7 前（v1.7 P1-4 残余窗口） | 全连通 INITIALIZING,meta 旧 | NodeInit/SetNeighbors 幂等重放 | 分两种：非首节点 = **隐藏高层节点**（其上层列表为空——M4 算法只向 ≤min(level, max_level) 层连边，graph.rs:436 实证；入口点旧，下降永不进入该层；level-0 边完整，搜索答案合法——良性，登记 §11.3 审计）。注意 max_level 就此**滞后**真实最高层且后续 MetaUpdate 只升不追溯（v1.7 第七轮 P2-1：不变量弱化见 8.3①);**首节点及步骤 4–6 间崩溃的同一检测态**(v1.7 第七轮 P3-3：不只"6 后 7 前")= 目录非空但 meta 无入口点 → 搜索不可行，open 时恢复修复闭合（§10.3) |
| 7 后 8 前 | meta 已新，节点 INITIALIZING | MetaUpdate 幂等重放 | 入口点指向**全连通** INITIALIZING 节点（步骤 6 已写完自身列表——这正是 meta 先于 PublishLive 的原因）——搜索正常；状态位由审计/PublishLive 语义覆盖，无错误答案 |

### 8.3 关键不变量（幽灵节点容忍的成立条件）

1. **入口点单调且只指向已写完的节点**(v1.7 P1-4 改写）：入口点只在
   "更高层节点完成自身邻接发布后"切换（步骤 7 在 6 后）;meta 更新在
   PublishLive（步骤 8）之前——入口点永不指向空列表节点（那会让搜索
   从死胡同出发）；空图首节点无条件发布（P1-5)。残余的 6–7 窗口
   （全连通但 meta 旧）由 open 时恢复修复闭合（§10.3)。
   **不变量弱化声明**(v1.7 第七轮 P2-1)：崩溃残态下 `max_level` 可以
   **滞后**于真实最高层（隐藏高层节点：其上层列表恒为空、搜索不可达、
   答案合法；后续 MetaUpdate 只升至"下一个更高抽取值"，永不追溯——
   "自然覆盖"不成立，此弱化是终态而非暂态）。弱化后的不变量 =
   `max_level == 入口点 top_level`，不含"无节点高于 max_level"——
   后者是 M4 快照清单第 9 条的口径（encoding.rs validate_entry_and_
   max_level)，页驻图在崩溃残态下不承诺它，§11.3① 的验收断言按弱化
   口径实现（否则 forget 窗口测试撞上自己的断言）;
2. **分配即映射发布**(v1.5 改写，吸收 v1.1 P1-2、第二轮新 P1、本轮
   P1-3):NodeId = 目录链上序位；槽先于映射占用（步骤 3 在 4 前）;
   高水位由链导出（无独立字段），链只追加 + 条目覆写幂等 ⇒ 恢复后
   任何**已发布** NodeId 至多被分配一次；未发布 id 的复用合法（从未
   可观察）——M4 §3 的 "never reused" 契约在崩溃下成立；
3. **不可达即无害**(v1.6 复核 P2-1 修正措辞）:INITIALIZING 在连边
   之前无入边、不可达；连边之后（步骤 5 起）它**可以被召回且答案合法**
   （有向量的部分连通节点，自身空列表 = 有向死胡同）——真正的不变量是
   **"幽灵形态不产生错误答案"**：孤儿条目/未连边的幽灵映射不进任何
   搜索结果，部分连通节点只可能贡献合法答案或轻微降低自身召回率;
4. **幂等锚唯一**:pd_lsn，按页判定（v1.2 起全部 HNSW 记录都是真单页
   记录，无跨页写）；同一记录重放 N 次结果一致（ROADMAP 验证标准）;
5. **空间泄漏记账**：幽灵形态 = 孤儿条目（步骤 3–4 间）、幽灵映射
   (4–6 间）、孤儿目录页（1–2 间）——清理归 M6 vacuum,M5 在恢复
   验收里只断言"幽灵不污染搜索结果"。

### 8.4 与 B+Tree split 协议的对比结论

split 需要三步协议 + CLR + IncompleteSplitTracker，因为半 split 使键不可达；
HNSW insert 的半完成态要么不可达（无害）、要么部分连通（合法答案）。
**M5 因此不立 CLR/tracker 等价物**——这是 §3 选 (a) 的复杂度上限的证据，
也是本文档接受审查的核心论断。

---

## §9 Checkpoint 与 M4 快照的关系

### 9.1 页驻图 = 标准 buffer-pool 公民

HNSW 页与普通页一样脏页进 DPT、FPI 对齐、checkpoint 时刷写——**无需
HNSW 专属 checkpoint 动作**。ROADMAP 的 "Checkpoint HNSW 快照（入口点/
最大层数/节点计数）"(ROADMAP.md:272）落到 meta page 与目录链：入口点/
最大层驻 meta page，节点计数由目录链导出（§6/§7.1)，本就驻页，
checkpoint 天然包含它们；checkpoint 记录本身沿用
CheckpointEnd v2 与 superblock redo 点（pg-storage/src/superblock.rs:79)。

### 9.2 M4 快照文件的降级与恢复时长

- M4 §7 快照（snapshot.rs)**降为逻辑归档/迁移/调试通道**：格式 v1 冻结
  不动（"M5 不重改格式"的冻结验收标准出自 M4 tech-selection §2 代价段
  :86，本文档兑现）;save/load API
  保留，benchmark harness(probe/gate）继续用它。
- **恢复时长估算**:redo 起点恒为 checkpoint 点（min(rec_lsn) 未启用——
  如实登记其代价：checkpoint 间隔越长，重放窗口越大）。生理重放是页级
  FPI 覆写 + 后像应用，无算法重跑，单条记录的应用成本与 heap/btree 同
  量级；恢复时长因此 ≈ 重放窗口的记录数 × 页应用成本，由 checkpoint
  频率直接控制。M1–M3 的 benchmark 文档没有 WAL 重放吞吐的既有数字可引
  （如实说明，不虚构）,30s 达标的举证责任在 §11.4 的实测——若实测逼近
  上限，第一调节旋钮是 checkpoint 频率（运维参数），不是启用
  min(rec_lsn)（那是 Phase 3 级别的恢复重构，不在 M5 发明）。
- **checkpoint 刷写放大与第二旋钮**(v1.1 审查 P3-1):1M 全量 bulk load
  后的首次 checkpoint 刷写 ≈ 全量脏页 ~0.9GB（与一次全量快照同量级，
  但走 buffer pool 刷写路径而非独立文件）；稳态增量窗口的刷写量 =
  窗口内 WAL 触碰的脏页数，由 §4.2 核算的 ~2.8KB/insert 写放大推算。
  第二旋钮登记：**脏页上限/增量 checkpoint**（脏页超阈值即触发部分
  刷写，削平单点刷写尖峰）——Phase 1 的 checkpoint 协调器是否已有
  该钩子待开工时核实，无则列为 M5 内的小型 pg-storage 增量。
- **bulk load 后必须立即 checkpoint——硬要求**(v1.4 P3，从暗示升为明文）:
  1M 全量建库 ≈ 2.8GB WAL(§4.2)，在建库途中、首次 checkpoint 之前
  崩溃，重放窗口 = 全量建库记录，恢复远超 30s——§13.2 的 <30s 验收
  **只对 checkpoint 之后的增量窗口成立**(§11.4 的测试方法"建库 →
  checkpoint → 注入增量 → SIGKILL"已正确如此构造）。因此：bulk load
  路径在完成时必须显式触发 checkpoint（不做 = 30s 保证不成立）;运维侧
  长窗口大批量写入同理（写量逼近 WAL 预算时先 checkpoint)。

---

## §10 崩溃恢复与 redo 幂等

### 10.1 redo 注册与 RedoContext 处置

- `hnsw_redo_handlers()` 导出 7 个 handler(§4.2 的 121–127),Engine::open 注册
  (engine.rs:687-689 链）。
- **RedoContext 封闭集合**(recovery.rs:210-229:buffer_pool Option /
  page_allocator / clog / att / dpt / incomplete_splits)——HNSW handler
  **无状态化**，只用 buffer_pool + page_allocator，不请求扩展 RedoContext
  （扩展 = 改 pg-storage 的 Stage-N 冻结面，能免则免）。incomplete_splits
  字段对 HNSW 闲置（§8.4)。
- **redo 侧纵深校验——冻结清单**(v1.1 审查第二轮 P3-3③ 立、v1.6 P3-3
  对称化、v1.7 P3-2 修正、v1.9 审查 P2-3 穷举冻结）。总原则：**正常路径
  天然合规，校验专防坏记录**(WAL 是外部输入面）;**层次明文**(v1.9
  P2-2):handler 先经 meta_page_id 读 meta 完成校验，再调物理原语——
  原语不重复校验（§10.2);pd_lsn 守卫先行（已应用
  即跳过，不重验），校验在 apply 前；对齐 M4 快照校验清单纪律
  (encoding.rs:409-431 的项别——finiteness、升序无重复无自环、度数 cap、
  端点越界、层级关系——逐项映射到记录级）。**可求值性约束**(v1.10
  复核 P2-1):redo 校验只保留**同页/同记录可判定**的项——目录链解析
  （序位→物理页须沿链遍历，1M 节点 = 1231 页/次）在 redo 路径上不可
  廉价求值（恢复窗口 10 万条记录 × 链遍历 = 亿次级页读，直接爆掉 30s
  预算），凡依赖链导出 HWM 或目录映射的校验一律降级到 §11.3 的 open
  后审计（redo 期 LSN 序已蕴含其恒真；审计期有一次性遍历预算）——
  handler 无状态化声明与恢复预算由此同时成立。逐记录类型：
  - **HnswNodeInit**:meta_page_id 指向合法 meta 页；dim == meta.dim;
    level ≤ L_max = ⌊53·ln2/ln m⌋（按 meta 的 m 重算）;vector 分量全有限;
    meta.metric == Cosine ⇒ vector 非零（对齐 M4 insert 漏斗的 ZeroVector
    响亮拒绝——distance.rs:76 校验 funnel、graph.rs:385 insert 入口同口径；
    L2/IP 零向量合法，M4 同）；目标 slot 为空闲或 INITIALIZING（幂等重放
    形态）;
  - **HnswSetNeighbors**:count == content.len();count ≤ 层容量（level 0 →
    m_max0，上层 → m，经 meta 分解——v1.8 P3-2：条目自描述反解不出，
    payload 必带 meta_page_id);level ≤ 目标条目 top_level；邻居列表升序、
    无重复、无自环;("每个被引 id < 链导出 HWM"降级到 §11.3 审计——
    redo 期 LSN 序已蕴含：同 insert 内步骤 4 在 5 前，旧节点更早；v1.10
    可求值性约束）;
  - **HnswDirAppend**：条目序位 == 尾页 count（追加位置精确）；目标
    (page, slot) 的条目存在且 state ∈ {INITIALIZING, LIVE}(NodeInit
    先行的协议前提；**不得要求 INITIALIZING 单值**——v1.11 审查 P1：
    节点页与目录页由 buffer pool 独立刷盘，节点页可携 LIVE（甚至
    全连通）先于目录页落盘，崩溃后重放 DirAppend 时目标条目恰为
    LIVE，单值校验会把合法残态误杀；redo 校验只可对**跨页可变状态**
    断言取值集合，不可断言单值——这是 pd_lsn 单页守卫之外的第二
    条跨页纪律）;
  - **HnswDirLink**：旧尾页 next == INVALID（未链接）;next 指向已分配页；
    新页 ordinal == 旧页 ordinal + 1;
  - **HnswMetaUpdate**:max_level == 入口点节点
    的 top_level（弱化口径，v1.8);("entry_point < 链导出 HWM"降级到
    §11.3 审计，v1.10 可求值性约束）;
  - **HnswPublishLive**：目标条目存在，state ∈ {INITIALIZING, LIVE}
    (LIVE = 幂等重放形态）;("node_id 与目录映射一致"降级到 §11.3
    审计，同上）;
  - **HnswNodeTombstone**：目标条目存在且 LIVE(M5 只重放不生效语义，
    §1);M6 生效时再按那时口径收紧。
  此清单为 M5 redo 校验的**冻结边界**：新增检查项 = 协议修订记录；更深
  的图级校验（连通性、目录一致性、HWM 导出核对——含上述三项降级项）
  不进 redo 路径（恢复时长预算 + v1.10 可求值性约束），归 open 后审计
  (§11.3)。
- `buffer_pool: Option` 的处置（v1.1 审查 P2-4 改写——v1.0 引用了"M1
  早期重放阶段 pool 不存在"的生产路径，该阶段已不存在）:**Stage I 起
  recovery 恒在 buffer pool 打开后进行**（pg-storage/src/engine.rs:236-243
  的阶段序注释；RedoContext 构造处 :646 恒传 `Some(buffer_pool)`),
  `None` 仅三处测试构造可达。HNSW handler 遇 None 直接硬失败因此是
  **纵深防御**而非真实分支；recovery 阶段序的测试保留（钉住 Stage I
  顺序不回归）。

**WAL 接入清单**(v1.5 审查 P2-1——落码时逐项打勾，缺一即破坏对应护栏）:

1. **from_u8 判别值注册**:record.rs:107-138 的 `from_u8` 加 121–127 分支
   （未知判别值现有行为 = WalReadFailed 硬失败，保持）;
   tests/wal_record_type_discriminant.rs 钉表同步新增；
2. **DPT touched-page 分类**:analysis.rs:267 的 `for_each_touched_page`
   为 7 个新类型注册分类（每条记录的 payload 目标页即 touched page,
   §4.2 自包含规则使它可以直接解出）;analysis.rs:796 的穷举测试
   (`every_record_type_is_classified_for_the_dpt`）机制自带——新类型
   未注册时该测试变红，无需另写护栏；
3. **pg-waldump 解码**:pg-waldump.rs:336 现状把 LogicalHnsw 列在
   reserved-hex 分支；新增 7 个类型的 payload 解码器并从 reserved 分支
   移出（对齐现有 heap/btree 解码臂的形态）。

### 10.2 pg-am-hnsw 侧的新增内部 API(M5 第一个代码任务）

M4 深审（2026-09-09）已立卡，本文档正式收编：

1. **访问器收口**:`search_layer`/`insert` 里的直接字段索引
   (graph.rs:578 的 `self.adjacency[...]` 等）全部改走 `neighbors()`/
   `vector()` funnel——页驻后这些 funnel 背后是页缓存查找；
2. **物理应用原语**(pub(crate) 起步，v1.7 形态——v1.7 审查 P2-1：签名
   补齐全部物理页定位参数，与"redo 与正常路径共用同一实现"的承诺对齐）:
   - `append_node(meta_page_id, node_page, slot, node_id, level, vector)` —
     指定 level(§5 裁决载体）；定长预留、INITIALIZING 态、节点页槽位
     分配在此发生（§8.1 步骤 3);
   - `set_neighbors(page, slot, level, count, content)` — 原位更新（步骤
     5/6);
   - `publish_live(page, slot, node_id)` — state 后像翻 LIVE（步骤 8);
   - `dir_append(dir_tail_page, node_id, node_page, slot)` — 尾页单页条目写
     （步骤 4);
   - `dir_link(old_tail_page, new_dir_page)` — 目录扩容（步骤 2);
   - `apply_meta(meta_page_id, entry_point, max_level)` — 步骤 7 与 §10.3
     的创建/修复共用;
   - `apply_tombstone(page, slot, node_id)` — HnswNodeTombstone(124)的
     redo 承载（v1.9 审查 P2-1 补登）;tombstone 语义 M6 才生效，但 §1
     划的 M5 范围是"能写、能重放、能校验"——重放承载的原语必须在
     清单内;
   **校验层次**(v1.9 审查 P2-2 明文）:redo handler 先经 meta_page_id 读
   meta 完成全部校验（§10.1 冻结清单），再调用原语；**原语本身不重复
   校验**——校验在 handler/正常路径的 funnel，原语是纯应用（单一实现
   纪律：正常路径与 redo 共用原语，校验规则单点定义在 funnel 层）;
   redo handler 与正常路径共用（M4 Stage B 教训）;insert
   成功边界 = `flush_to`（全部记录的最后 LSN,§8.1 成功边界①);
3. M4 纯内存 `Hnsw` struct 保留：snapshot 往返矩阵与 recall 门槛不动，
   页驻图作为新类型与其共享算法函数（select_neighbors/search_layer 的
   算法核心抽成对 funnel 泛型化的自由函数——这是 §7 落地的前置重构，
   风险中等，用既有 139 测试 + 快照往返矩阵做行为钉）。

### 10.3 索引创建协议与 open 时 meta 修复（v1.7 审查 P1-3 / P1-4)

**创建序列**(A1 契约对齐：buffer_pool.rs:424-442——回收页必须自 log
post-image FPI;btree 先例 index.rs:446-493 的 create 路径与 :3299-3310
的 `log_page_init`):

1. `new_page`（目录首页）→ 初始化自描述头（ordinal=0, next=INVALID,
   count=0)→ `log_page_init`(post-image FPI + stamp pd_lsn);
2. `new_page`(meta 页）→ 初始化全部参数字段（dim/m/m_max0/efC/metric/
   selection/rng_seed/链头=步骤 1 的页/entry_point=INVALID, max_level=0)
   → `log_page_init`;
3. engine 侧 first_page 登记：索引的 meta page 记入 `pg_rust_relpages`
   目录项（pg-engine/src/engine.rs:356-357 的 `first_page` 字段先例，
   btree 索引同此路径，:902)。**范围调和**(v1.7 第七轮 P3-1)：这是
   引擎内的最小登记——没有它 first_page 重启后不可定位，崩溃轮次无从
   谈起；它与 §1 非目标排除的"SQL/DDL/catalog/reloptions 用户面贯通"
   不是同一层（那指用户可见的 DDL 语句与 reloptions 参数面，归 Phase 4)。

**崩溃窗口**：步骤 1 后 2 前 = 孤儿目录页（记账，同 §8.2 孤儿页）;2 后
3 前 = 两页已初始化但索引不可见（无目录项）——创建是 utility 操作，失败
即重建，页泄漏记账归 M6;A1 契约保证回收页不会以旧租户映像恢复
（log_page_init 的 post-image FPI 是其唯一合法初始化路径）。

**open 时 meta 修复**(v1.7 P1-4 的选定方向——§8.2"6 后 7 前"窗口的
闭合）:redo 完成后、开放查询前，检测"目录链非空但 meta.entry_point =
INVALID"（首节点在步骤 4–7 间任一位置崩溃的同一检测态，v1.7 第七轮
P3-3 措辞修正——不只"6 后 7 前")，修复 = 从目录首条目
读出该节点页、读其 top_level，写一条 HnswMetaUpdate(entry_point =
首个已发布节点，max_level = 其 top_level)——幂等（后像覆写 +
pd_lsn)、确定性（目录序位即依据）、WAL 记录（正常记录，非修复特例）。
**备选及不选**(meta 更新先于连边，即入口点先指向 INITIALIZING 节点）:
入口点会指向空列表死胡同，搜索从入口点出发即丢全部答案——比"窗口
留待 open 修复"严重得多，否。非首节点的"隐藏高层节点"残态（8.2 窗口
表）搜索语义良性，不立修复；其代价 = max_level 永久滞后（8.3 不变量 1
的弱化声明——后续 MetaUpdate 只升不追溯，"自然覆盖"不成立，v1.7
第七轮 P2-1 修正）。

---

## §11 验证方法论（M5 特化）

### 11.1 崩溃测试基建复用

- **单步窗口**:mem::forget 模式（crates/pg-am-btree/tests/btree_split_crash.rs)
  ——HNSW insert 的每个 WAL 记录边界（§8.1 的 8 步；§8.2 窗口表 10 行
  含平凡行与交错态行，v1.7）各一枚 forget 测试，恢复后断言图语义。
  多步协议若需暴露内部步骤，对齐 SplitState 先例暴露 `InsertState` 式
  API（仅测试可见）。
- **真 SIGKILL 子进程**:m2b_crash_rounds.rs 模式
  (crates/pg-engine/tests/m2b_crash_rounds.rs:46-108)——HNSW 版
  `m5_hnsw_crash_rounds`,expectation.txt 前缀耐久断言照抄，
  `M5_CRASH_ROUNDS` 环境变量：CI 25 轮（先例出处 m2b_crash_rounds.rs:46-48)
  / 验收 1000 轮（ROADMAP.md:277)。轮次内 level 流**跨崩溃**可复现
  (v1.5 P1-4:seed 在 meta page 钉死 + open 时按节点数 skip-ahead 精确
  推进，恢复后续插的 level 流 = 未崩溃运行的延续；重放不消费 rng)。
  确定性口径收窄（v1.5 审查 P2-2;v1.7 复核 P2-2 再收窄——"拓扑不变"
  也过强：level 流依赖 `ln`，跨平台拓扑即可不同）:**同构建/同平台**
  承诺位级一致；跨平台只承诺**格式兼容与算法语义一致**（可互读、
  可重放、recall 口径在同平台验收钉死），不承诺拓扑逐位一致。

### 11.2 幂等测试

每条新记录类型：同一记录对同一页 redo N 次（N=3)，断言页字节全等——
pd_lsn 守卫（record.rs:209 模式）+ 后像覆写使这天然成立，测试是钉不是证。

### 11.3 图语义一致性断言（恢复后）

三层，由弱到强：① 结构不变量（度数 cap、层计数、入口点/max_level 关系
——按 v1.7 第七轮 P2-1 的**弱化口径**断言 `max_level == 入口点
top_level`（崩溃残态下允许存在 top_level > max_level 的隐藏节点，
其上层列表恒为空、搜索不可达；**不**断言 M4 快照口径的"无节点高于
max_level"，否则 forget 窗口测试撞上自己的断言）；含 **open 修复后
meta 与目录首节点一致**(§10.3,v1.7)、
链导出高水位与链内容一致、INITIALIZING/孤儿条目计数登记；**目录链结构
四断言**——v1.2 第三轮复核 P3-2:ordinal 连续自 0、中间页恰满（"仅尾页
满才 link"的协议前提钉）、next 唯一成链、末页计数 ∈ [0, 容量]——链导出
HWM 的正确性依赖这组前提被钉住，对齐 M4 邻接良构清单纪律）;**邻接
良构断言**(v1.11 审查 P2-2 补立——redo 冻结清单的可求值性约束把三类
依赖链导出的校验降级到这里，§11.3 必须自含地枚举它们，否则降级 =
消失）:a) 每个被引邻接 id < 链导出 HWM 且对应目录条目占用（端点存在，
对齐 M4 快照校验"邻居 < node_count");b) 每条 level-L 边的目标节点
top_level ≥ L（层级归属，对齐 M4 快照校验 encoding.rs:423 的"目标
拥有对应层"——v1.10 降级项"被引 id < HWM"只覆盖存在性，层级归属是
独立一维，漏检即 Stage C 重建遍历越界类风险）;c) MetaUpdate 的
entry_point < 链导出 HWM;d) PublishLive 的 node_id 与目录映射一致；②
siftsmall recall@10 ≥ 0.98（恢复后对 M4 门槛数据集重跑 probe 口径）;③
幽灵/孤儿统计输出（不阻断，只登记——8.3 不变量 5)。
外加 ④（v1.1 审查 P2-1 配套）:**loser 窗口断言**——M5 的 WAL 流中
HNSW 记录的 txn 归属恒为 INVALID(§8.1：无事务性 DML);**redo 期间
(replay 流内）**若发现任何携带有效 txn_id 的 HNSW 记录即硬失败
(v1.1 第二轮 P3-3② 措辞修正：判定在重放流内，非恢复后；该形态只可能
来自未来的 M6 路径，出现在 M5 即契约破坏）。

### 11.4 恢复 <30s 测量

1M 向量建库 → checkpoint → 注入固定增量（如 100k insert)→ SIGKILL →
计时 `Engine::open` 到可查询；机器规格随 docs/phase2-m5-benchmarks.md
落盘（M1–M4 benchmark 文档传统）。

### 11.5 与 M4 既有测试的关系

M4 的 139 枚测试（88 lib + 3 bruteforce + 5 properties + 3 recall_siftsmall
+ 40 snapshot_roundtrip)+ bench smoke 必须保持全绿；页驻图新增测试挂在
pg-am-hnsw（页布局/WAL 记录）与 pg-engine（崩溃 rounds）两侧。

---

## §12 风险与开放问题

- **O1（超上限维溢出，区间 (1791, 2000])**:M4 §11 既定窗口；v1.1 P1-1 重算后区间收窄为
  **(1791, 2000]**(§7.2 容量不变量的公式化硬上限 1791 是默认参数值，
  非默认参数按同公式反解）。候选方向两案——(i) 多页节点链
  （条目跨页，目录条目带链头；WAL 记录按链段生理化）;(ii) 维持
  公式化 dim 硬限制，超出创建即拒。**建议 (ii) 起步**:sift/gist 验收域
  最大 960 维，(i) 的协议复杂度（跨页条目的 FPI/幂等）无验收需求支撑；
  (ii) 把错误面做成响亮 InvalidArgument 即闭合。若 Phase 4 出现真实大维
  需求再升 (i)。
- **O2(f16/bf16 存储编码）**：维持缓议（M4 §3 口径）——页布局的 vector
  字段保持 f32；半精度是条目编码层的事，落地时只动 7.2 的一个字段解释。
- **payload 64KB 上限**:§4.3 已核算无风险；若 O1-(i) 未来落地，链段切分
  天然在预算内。
- **redo 起点恒为 checkpoint 点**:§9.2 已量化；min(rec_lsn) 不启用是
  Phase 1 传承口径，M5 用 checkpoint 频率对冲，实测超标再回推。
- **幽灵/孤儿空间泄漏量化**：崩溃轮次验收里统计（11.3③)；幽灵槽/半成品
  节点之外，v1.2 增列**孤儿目录页**（扩容窗口 1–2 间崩溃，§8.2 表首行）。
  若 1000 轮累积泄漏显著（>1% 节点）,M6 vacuum 的优先级上调——届时以
  实测数据回推。

---

## §13 验收标准草案（供 coding-plan 引用）

1. **崩溃一致性**:1M 向量，随机 kill -9 × 1000(`M5_CRASH_ROUNDS=1000`),
   每次恢复后图语义一致（§11.3 全部断言 ①–④ 过）。
2. **恢复时长**：恢复 < 30 秒（ROADMAP.md:278),§11.4 方法实测落盘。
3. **redo 幂等**：每记录类型 N 次重放字节全等（§11.2)。
4. **WAL 先行**：页刷写路径 100% 经 buffer pool（无绕过写）,code review
   + flush_frame 的 pd_lsn 断言兜底。
5. **meta page 校验**:metric/selection/params 错配硬失败（负例测试）。
6. **耐久边界**:insert 成功返回前 `flush_to`（全部记录的最后 LSN)——
   kill -9 后返回成功的插入必须全部可见（§8.1 成功边界①;mem::forget
   窗口测试断言）。
7. **回归**:M4 全部 139 测试 + workspace 全量双档绿；recall 门槛
   (siftsmall recall@10 ≥ 0.98）对页驻图同样成立。
8. **格式纪律**:M4 快照 v1 格式零改动（golden bytes 钉继续绿）;WAL
   判别值新增进 tests/wal_record_type_discriminant.rs 钉表；目录页编码
   常量（条目宽 10B、每页 813 条目、自描述头布局）进格式钉测试。
9. **WAL 接入清单**(§10.1):from_u8 分支、DPT touched-page 分类
   （穷举测试自动把守）、pg-waldump 解码，三项齐备。
10. **页尺寸口径**(v1.5 P2-3):M5 验收矩阵只钉 8KB(PAGE_SIZE 默认）;
    容量公式以 PAGE_SIZE 为参（§7.2),16k feature 标为未验证配置。
11. **文档**:docs/phase2-m5-benchmarks.md（恢复时长/崩溃轮次/机器规格）。

---

## 修订记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-09-09 | 初稿（草案，待对抗审查与用户终审）。基于：M4 九轮外审后的代码实证（pg-am-hnsw 5573 行通读 + M5 衔接面深审报告）、pg-storage/pg-am-btree/pg-txn 先例逐项核对（文件：行）、ROADMAP 2b 范围表。纠偏一处探索摘要：payload 版本 nibble 在 u8 flags 高 4 位（record.rs:638-643)，非 u16 |
| v1.1 | 2026-09-09 | 第一轮对抗审查回流（verdict FAIL → 全量修复）。P1-1:§7.2 容量不变量公式化重写——页可用面 8156B(32B PageHeader + 4B LinePointer),dim/m/m_max0 乘积联动创建时硬校验，默认参数硬上限 dim ≤ 1791（最坏 13 层满载反解）,(1791,2000] 归 O1;"页满溢出"与"单条超页"措辞区分。P1-2:§8.1 步骤重排——NodeId 分配与高水位推进并入 HnswDirAppend 同记录原子（修复崩溃窗口的 NodeId 复用，M4 §3 "never reused" 契约）,HWM 真相源唯一化到目录头页（meta page 字段表除名）,§8.2/§8.3 同步。P2-1:loser 段重写——M5 HNSW 不挂事务性 DML,index-undo 对接列 M6 前置登记，§11.3 加 loser 窗口断言。P2-2:rng seed 创建时钉进 meta page（非秘密），崩溃轮次 level 流可复现写入 §11.1。P2-3:delta 否决论证照实改写（LSN 序前缀态下 delta 其实可行，真实理由是 shrink 对称 + handler 无状态），补写放大核算（~2.7KB/insert ≈ 5.2× 向量，1M ≈ 2.7GB WAL，接入 §9.2)。P2-4:pool None 段改写为"Stage I 起 replay 恒在 pool 打开后（engine.rs:236-243/:646),None 仅测试可达，硬失败是纵深防御"。P3-1:§9.2 补 checkpoint 刷写放大估算 + 脏页上限/增量 checkpoint 第二旋钮登记。P3-2:§7.1 上层邻接期望更正为无条件期望 ≈4.4B。P3-3/勘误1:benchmarks.md:48→:46。P3-4:encode_meta_record 写全 pg_am_btree::page 路径（page.rs:426)。P3-5:§6 声明与 btree meta 先例的语义差异（append 最新者权威 vs 字段级覆写后像）。勘误 2-6:ROADMAP :271→:272（两处）、:277→:278（两处）、:276→:277 + CI 25 轮出处落 m2b_crash_rounds.rs:46-48、manager.rs:15-16→:14-16、"M4 §3 冻结验收标准"归属改 M4 tech-selection §2 代价段 :86；勘误 7："合成 bench ~5.4s" 改引 criterion 存档均值 5.633s |
| v1.2 | 2026-09-09 | 第二轮复核回流（verdict FAIL → 全量修复；第一轮 13 项复核全部成立未回退，§8.4 核心论断未被攻破）。**新 P1(HnswDirAppend 两页写）**:v1.1 的"记录携带 HWM 后像"在目录链长 >1 后是尾页+头页的单记录两页写，突破 pd_lsn 单页守卫先例（btree 正因多页原子不可得才需三步协议+CLR)；按建议方向 (b) 重构——目录页自描述（ordinal + next 指针）,meta page 只持链头 PageId,**HWM 由链导出、不落任何字段**;HnswDirAppend 退化为真单页记录，新增 HnswDirLink(126，目录扩容单页记录）;§8.1 步骤重排（分配即链位）,§8.2 表补目录扩容孤儿页窗口，§8.3 不变量 2 改写为"分配即链位";§4.2/§6/§7.1 的"头页唯一真相源"表述同步消除；链尾定位成本核算（1M ≈ 770 页 ≈ 6.3MB 一次性遍历，毫秒级；页分配器高水位反推候选的优化登记为开工期实测点）；备选 (a)（明文两页语义）不选的理由写入 §4.2。P3-1：删除 §12 遗留的"目录页链 WAL 粒度"bullet（与 DirAppend 已定形态矛盾），孤儿目录页并入泄漏量化 bullet。P3-2:§1 O1 表述收窄为 (1791,2000] 并注明 M4 "约 2000" 以本文件为准（M4 文档不动）。P3-3:§4.2 "稳态实测口径"→"核算口径";§11.3④ "恢复后若发现"→"redo 期间（replay 流内）";§10.1 新增 redo 侧纵深校验（NodeInit apply 按 meta 的 m 重算 L_max 超限响亮拒绝，对齐 M4 解析点前置 cap 纪律）。同步修正：handler 数 5→6(§10.1)、判别值段 121–126(§4.1/§4.2/§10.1)、§10.2 原语清单（dir_append 单页化 + dir_link)、§11.3① 高水位表述 |
| v1.3 | 2026-09-09 | 第三轮复核回流（verdict **PASS**，附 3 条 P3 收口，本轮已闭合）：v1.2 新机制（链导出 HWM / DirLink / 自描述目录页）经受六路攻击（链尾定位成本、recycle 缺段、扩容三窗口、重复 link、FPI 叠加序、孤儿页记账）全部成立。收口项：§11.1 步骤数 6→7 并注明 §8.2 窗口表 7 行；§8.2 表补回"1 前｜无痕迹"平凡行使窗口枚举完整；§11.3① 增列目录链结构四断言（ordinal 连续自 0、中间页恰满、next 唯一成链、末页计数 ∈ [0, 容量]）；§7.1 声明每页条目数为格式常量（与目录页头布局同冻结，改动即格式修订）。nano×2（dir_append 原语签名落码时补目录页参数；§13.5 meta 校验不含链头 PageId 存在性）登记不阻塞。草案三轮审查闭环，可提交用户终审 |
| v1.4 | 2026-09-09 | 终审观察 3 条回流（P3×2 + nano×1，不阻塞，已闭合）:① §3 的"420s ≈ 14×"标注为**线性下界**——HNSW 建图 O(n log n)，对数增长修正后真实逻辑重放 ≈ 630–850s(21–28× 上限），(b) 被否得更狠但数字口径须严谨；② §6 ef_search_default 补轻重之分——它是性能旋钮（错配不毁图），与 metric/selection/rng_seed 的正确性关键不同级，校验可放宽为 WARN（开工时定）,M4 快照排除它正是同理；③ §9.2 新增硬要求——**bulk load 完成后必须立即 checkpoint，否则 <30s 恢复保证不成立**(1M 建库 ≈2.7GB WAL，首 checkpoint 前崩溃重放窗口=全量；§13.2 验收只对 checkpoint 后增量窗口成立，§11.4 测试方法已如此构造）,§4.2 括注同步指向硬要求 |
| v1.5 | 2026-09-09 | 第四轮只读审查回流（verdict FAIL → 全量修复；7 P1 + 3 P2 逐条代码实证后全部属实）。P1-1(PageId u64 宽度，types.rs:46)：目录条目 6B→10B(PageId u64 + SlotId u16，全宽不截断——2^32 页 × 8KB = 32TB 的截断上限虽远超 1M 节点库，但 PageId 全库统一分配，截断即隐性错址契约），目录页编码整体冻结（32B PageHeader + 24B 自描述头，813 条目/页，1M → 1231 页 ≈ 9.8MB 链遍历），写放大核算同步（DirAppend ~26B)。P1-2（变长 vs 稳定槽位）:slotted_page.rs:270-329 实证变长扩容必须搬移；改定长分档预留（NodeInit 按抽取 level 预留各层满载容量），SetNeighbors 变原位 count+内容更新，§7.2 容量公式由"最坏情况"变精确值（dim ≤ 1791 数值不变），空间代价如实写（+5–10%)，碎裂消除、M6 稳定槽位承诺兑现。P1-3（幽灵槽复用）：步骤序改为 NodeInit(INITIALIZING、槽先占用）→ DirAppend（映射发布）;"未发布的 NodeId 不构成分配"明文（可观察性即分配），复用窗口结构性消除；INITIALIZING/LIVE 状态机与搜索/审计解析规则立文。P1-4(rng 重启）：三案成本分析后选 skip-ahead——rng.rs 无 jump 函数（实证）,open 时按节点数重放 next_level × HWM(1M ≈ 毫秒级，实测点登记），零写放大、跨崩溃世系精确延续；持久化 256-bit state 案因 meta 页每 insert 脏否决，seed-only 案因世系分叉否决（图质量无害性论证写入）。P1-5：入口点条件改"空图或 level > max_level"(graph.rs:418-422 空图特判实证）。P1-6(redo 自包含）:RedoContext 无索引定位能力（recovery.rs:210-229 实证）,HnswNodeInit/HnswMetaUpdate payload 携带 meta_page_id(btree payload 带 page_id 先例 record.rs:891-904)。P1-7：成功边界四条明文（flush_to 末条 LSN；未成功 insert 允许可见，heap/btree 同口径；INITIALIZING/LIVE 状态机；§8.2 表补 partial-shrink 交错态行，窗口表 9 行）。P2-1:WAL 接入清单补全——from_u8 分支 + 钉表（record.rs:107-138)、DPT touched-page 分类（analysis.rs:267，穷举测试 :796 自带把守）、pg-waldump 解码（:336 从 reserved-hex 移出），入 §10.1 与 §13.9。P2-2：位级同构收窄为同构建/同平台（ln/sqrt 跨平台无位级保证，rng.rs:85-97/distance.rs:77 实证）,§3/§11.1 同步；§3 (b) 否决论证不受影响（恢复时长）。P2-3：容量公式以 pg_storage::types::PAGE_SIZE 为参（types.rs:12-27 的 16k feature 实证），验收只钉 8KB,16k 标未验证，不做编译期拒绝。同步面：§4.2/§4.3/§5/§6/§7.1/§7.2/§7.3/§8.1/§8.2/§8.3/§10.1/§10.2/§11.1/§11.3/§13 全部随设计变更更新 |
| v1.6 | 2026-09-09 | 第五轮复核回流（verdict **PASS 附条件** → 条件项本轮闭合）：第四轮 10 项修复逐项实证成立，新机制（10B 目录条目/定长分档/INITIALIZING-LIVE/skip-ahead）经受独立攻击，无新 P1。闭合项：**P2-1（立文矛盾）**——§8.1③ 与 §8.3 不变量 3 的"搜索永不解析 INITIALIZING"与 §8.2 窗口表自相矛盾（步骤 5 起 INITIALIZING 可有入边、可被召回且答案合法）；重写为"INITIALIZING 自步骤 5 起可被召回（部分连通合法），不变量改为'幽灵形态不产生错误答案'"，直查 API 规则收窄为"INITIALIZING 返回向量+状态标记，仅空槽/越界槽响亮报错",§8.2 窗口表行同步。**P3-2**:§8.2 shrink 措辞修正（M4 shrink 是任意位置剔除 + NodeId 重排，graph.rs:691-702，非尾部截断——结论反而更成立）。**P3-3**:§10.1 补 SetNeighbors/目录类记录的 redo 侧对称校验（count ≤ 目标条目自描述容量，无需 meta；序位/next 合法性）。**P3-4**:§8.1② "与 heap/btree 同口径"措辞修正为"可见性 = 记录持久性前缀"(heap/btree 的不可见靠 CLOG+index-undo,HNSW M5 无 undo 层，M6 开放事务写入时必须重议）。nano×3:distance.rs:77→78;"9.8MB"→"≈10MB(9.6MiB)"三处；§7.2 公式注明 level_count/state 的 1B 打包（扩 2B 则 1791 不变） |
| v1.7 | 2026-09-09 | 第六轮审查回流（verdict FAIL → 全量修复；4 P1 + 3 P2/P3 逐条代码实证后全部属实）。**P1-1(level/state 打包溢出）**:M=2 合法（params.rs:77-82),L_max=53、level_count 可达 54,4bit 放不下——位布局表立文：top_level:6 + state:1 + tombstone:1(tombstone 位兑现 M4 的 flags 预留，encoding.rs:204)；口径核对：快照 cap level_count ≤ 64(encoding.rs:79)⟺ top_level ≤ 63 ⟺ 6bit 恰好，两格式同一上限。**P1-2(LIVE 翻转无承载记录）**：选独立 HnswPublishLive(127)——①SetNeighbors 加标志位把记录语义耦合到步骤序、②一条记录写全部自身层揉两种语义，均否；state 位钉死为审计/恢复统计标记（搜索对两态一致，v1.6 已立）;handler 数 6→7、判别值段 121–127、§8.1 步骤 8、§8.2 窗口表 10 行、§11.1 同步。**P1-3（创建协议缺失）**：新增 §10.3——new_page → init → log_page_init(post-image FPI + stamp pd_lsn,buffer_pool.rs:424-442 的 A1 契约与 btree index.rs:446-493/:3299-3310 先例）→ first_page 记 pg_rust_relpages(engine.rs:356-357/:902)；创建崩溃窗口（孤儿目录页/索引不可见）立文。**P1-4(6–7 窗口不闭合）**：选"重排 + open 修复"——meta 更新先于 PublishLive（步骤 7 在 8 前），入口点永不指向空列表节点（备选"meta 先于连边"会使入口点指向死胡同、搜索丢全部答案，否）;§10.3 立 open 时修复：目录非空但 meta.entry_point=INVALID → 从目录首条目重建（幂等、确定性、正常 WAL 记录）；非首节点的隐藏高层节点残态良性不立修复。§8.2 表末两行/§8.3 不变量 1/§11.3① 同步。**P2-1**:§10.2 原语签名补齐全部物理页参数（append_node/set_neighbors/publish_live/dir_append/dir_link/apply_meta)。**P2-2**：跨平台声称再收窄为"格式兼容 + 算法语义一致"（拓扑可因 ln 跨平台差异不同）,§3/§11.1 扫净。**P3**:HWM 公式化 `tail.ordinal × 813 + tail.count`(§4.2/§6/§7.1 三处），引用 §7.1 格式常量与 §11.3 链结构四断言。v1.3 遗留 nano"meta 校验不含链头 PageId 存在性"由 §10.3 创建协议闭合 |
| v1.8 | 2026-09-09 | 第七轮复核回流（verdict **PASS 附条件** → 条件项本轮闭合；第六轮修复逐项实证成立，PublishLive 独立记录与 meta 重排方向正确，无新 P1)。闭合项：**P2-1(max_level 弱化立文）**——"隐藏高层节点残态良性、下次 MetaUpdate 自然覆盖"的论证两处不实（自然更新只升至下一个更高抽取值、永不追溯旧峰值；"上层边暂不可达"实为上层列表恒空，graph.rs:436 实证）;§8.3 不变量 1 立弱化声明（崩溃残态下 max_level 可滞后真实最高层，弱化后口径 = `max_level == 入口点 top_level`，不承诺 M4 快照清单第 9 条的"无节点高于 max_level"),§11.3① 验收断言按弱化口径收窄（否则 forget 窗口测试撞上自己的断言）,§8.2 窗口行与 §10.3 同步。**P3-1**:§10.3 步骤 3(pg_rust_relpages 登记）注明是引擎内最小登记（重开可定位性前提），与 §1 非目标的 SQL/DDL 用户面分层调和。**P3-2**:SetNeighbors 对称校验的"无需 meta"不成立（层容量分解需要 m/m_max0，条目自描述无法反解）——SetNeighbors payload 同规携带 meta_page_id,§4.2 表/自包含规则/§10.1 三处同步。**P3-3**:§8.2"6 后 7 前"行措辞修正（首 insert 在步骤 4–7 间任一位置崩溃产生同一检测态，"唯一残态"收窄性失实已改；"上层边暂不可达"→"上层列表为空")。七轮审查轨迹 FAIL→FAIL→PASS→FAIL→PASS→PASS→PASS，可提交用户终审 |
| v1.9 | 2026-09-09 | 第八轮审查回流（verdict FAIL → 全量修复；1 P1 + 3 P2 逐条实证后全部属实）。**P1（节点页初始化协议不完整）**:§8.1 步骤 1 补全初始化链——new_page → 初始化 HNSW 页头（32B PageHeader + 页类型；目录页另写 ordinal/next=INVALID/count=0)→ log_page_init(post-image FPI + stamp pd_lsn)→ 首个 NodeInit/DirLink；回收页口径明文：post-image FPI 的内容 = 初始化后的合法 HNSW 页（非零页）,A1 契约（buffer_pool.rs:424-442）与 btree log_page_init(index.rs:3299-3310）语义核实；新分配全零页同样必须走此链（pd_lower=0 不是合法 slotted 页头），两情形无例外分支。**P2-1**:§10.2 原语清单补 `apply_tombstone(page, slot, node_id)`(HnswNodeTombstone=124 的 redo 承载；tombstone 语义 M6 生效但 §1 划的 M5 范围是"能写、能重放、能校验"，承载原语必须在清单内）。**P2-2**：校验层次明文——redo handler 先经 meta_page_id 读 meta 完成全部校验再调原语，原语不重复校验（单一实现纪律：校验在 funnel 层单点定义）,§10.1 总原则与 §10.2 同步。**P2-3**:§10.1 纵深校验扩为**冻结清单**（逐记录类型：NodeInit 的 meta 合法性/dim 一致/L_max/有限性/slot 态；SetNeighbors 的 count==content.len()/层容量/top_level/升序无重复无自环/被引 id < 链导出 HWM——LSN 序保证被引节点先发布；DirAppend 的追加位置精确/目标 INITIALIZING;DirLink 的未链接/ordinal+1;MetaUpdate 的 entry_point<HWM/max_level==入口点 top_level(v1.8 弱化口径）;PublishLive 的幂等态/目录一致；Tombstone 的目标 LIVE)，对齐 M4 encoding.rs:409-431 快照清单项别；pd_lsn 守卫先行（已应用即跳过不重验）；清单冻结——新增检查项 = 协议修订记录；图级深校验不进 redo 路径（恢复时长预算），归 §11.3 open 后审计 |
| v1.10 | 2026-09-09 | 第九轮复核回流（verdict **PASS 附条件** → 条件项本轮闭合；复核给出**收敛判定：机制面已无未攻击角落**)。闭合项：**P2-1(redo 校验可求值性）**——冻结清单中三项依赖链导出 HWM/目录映射的校验（SetNeighbors 被引 id < HWM、MetaUpdate entry_point < HWM、PublishLive 目录一致性）与"handler 无状态化 + 30s 恢复预算"三方冲突（序位→物理页须沿链遍历，恢复窗口 10 万条 × 1231 页 = 亿次级页读）：按复核建议方向 (b) 全部降级到 §11.3 open 后审计（redo 期 LSN 序已蕴含其恒真，审计期有一次性遍历预算）,§10.1 立"可求值性约束"（redo 只留同页/同记录可判定项）。nano×4:① §8.1 步骤 1 "首个 NodeInit/DirLink"→"NodeInit(节点页)/DirAppend(目录页）"(DirLink 写旧尾页）;② 写放大核算补 v1.8 的 meta_page_id(+16×8B ≈ 128B):2.7→**2.8KB/insert ≈ 5.4×**,1M ≈ 2.8GB WAL(§4.2/§9.2 两处同步）;③ §8.2 表头 "v1.7 形态"→"v1.9 形态";④ pd_lsn 跳过不重验补 FPI 前提一句（镜像含全部先序同页记录内容,"pd_lsn ≥ lsn 而本记录未应用"不可达）。九轮轨迹 FAIL→FAIL→PASS→FAIL→PASS→PASS→PASS→PASS→PASS，可定稿提交用户终审 |
| v1.11 | 2026-09-09 | 第十轮审查回流（1 P1 + 2 P2，逐条代码实证后全部属实并修复）。**P1(DirAppend 跨页单值断言误杀合法残态）**：冻结清单"目标条目存在且 INITIALIZING"改 state ∈ {INITIALIZING, LIVE}——节点页与目录页由 buffer pool 独立刷盘，节点页可携 LIVE 先于目录页落盘，崩溃后重放 DirAppend 时目标条目恰为 LIVE，单值校验会拒绝合法状态；由此立第二条跨页纪律：redo 校验对跨页可变状态只断言取值集合、不断言单值（pd_lsn 单页守卫只覆盖同页幂等）；同清单其余跨页读（MetaUpdate 读入口点 top_level）复核安全——top_level 不可变 + LSN 序保证条目已存在。**P2-1(NodeInit 缺 Cosine 零向量校验）**：补 meta.metric == Cosine ⇒ vector 非零，对齐 M4 insert 漏斗(distance.rs:76 的 ZeroVector 响亮拒绝、graph.rs:385 入口同口径；L2/IP 零向量合法，M4 同）——缺此校验则 Cosine 索引可经 WAL 写入搜索期 expect panic 的图（Stage C 已修过的同类边）。**P2-2(§11.3 邻接审计不自含）**:v1.10 把三类链导出校验降级到 §11.3，但 §11.3 正文未逐条枚举，降级近乎消失；补"邻接良构断言"四条——a) 端点存在（被引 id < 链导出 HWM 且目录条目占用）;b) 层级归属（level-L 边目标 top_level ≥ L，对齐 M4 快照校验 encoding.rs:423，与存在性是独立一维）;c) entry_point < HWM;d) PublishLive 的 node_id 与目录映射一致。十轮轨迹 FAIL→FAIL→PASS→FAIL→PASS→PASS→PASS→PASS→PASS→修复，待复核 |
