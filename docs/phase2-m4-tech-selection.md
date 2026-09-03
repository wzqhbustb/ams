# Phase 2 M4 技术选型（In-memory HNSW）

> 承接 Phase 1（M1+M2+M3:崩溃安全存储基座 + MVCC 事务 + 并发 B+Tree + Vacuum +
> 可观测性 + PG Wire 极简版），本文档定义 Phase 2 第一个 milestone（M4 =
> ROADMAP.md Phase 2a，**纯内存 HNSW**）落地前所有跨模块的技术选择，
> 对应 ROADMAP.md:247-262。
>
> 目标与 Phase 1 各 milestone 一致：所有影响数据编码、跨模块契约、M5/M6 衔接面
> 的决策先敲定；每个选择给"选项 → 选择 → 理由 → 代价"。章节编号（§1…§12）
> 供代码注释长期引用。
>
> 文档中的代码引用（`file:line`）均为撰写时（2026-08）核实的事实；若后续实现
> 与引用不符，以代码为准并修订本文档。

---

## §1 范围与非目标

**M4 交付三块内容**（与 ROADMAP.md:247-262 对应，有范围修正，见下）：

| # | 模块 | 对应章节 |
|---|------|---------|
| 1 | HNSW 内存版（分层随机图、插入、贪心搜索、邻居选择启发式） | §4、§6 |
| 2 | 距离函数（L2 / Cosine / Inner Product，正确性优先） | §5 |
| 3 | recall 基准 harness + 快照加载 API | §7、§8 |

**对 ROADMAP 2a 表格的两处范围修正**（理由见对应章节）：

- **VECTOR(n) "DDL 级声明"拆分交付**：M4 只交付向量类型的**内存表示与序列化
  编码冻结**（§3），不接 catalog/SQL DDL。DDL、`<=>` 操作符、executor 贯通
  归 M6 的 SQL 贯通 stage——M4 没有 engine 依赖，保持"纯算法 milestone"的
  可验证性。
- **f16/bf16 不在 M4 交付**：M4 统一 f32 计算路径；半精度只影响存储带宽与
  内存占用，不影响算法正确性，以"存储编码 + 读时转 f32"形式归 M5（节点页
  布局阶段一并定）。ROADMAP 列举的动机是支持多精度模型输出，M4 的价值密度
  不足以摊薄两套数值路径的测试成本。

**非目标（明确不做）**：

- **持久化 / WAL / 崩溃恢复**：归 M5（Phase 2b）。M4 的快照序列化（§7）是
  调试与基准加载手段，不是持久化契约。
- **并发**：图的可变状态单线程访问，无锁无原子；并发控制（epoch 回收、
  节点锁）归 M6（Phase 2c）。
- **删除 / tombstone**：M4 只有插入与查询；删除语义与并发 undo 归 M6。
- **SIMD 优化**：归 M4 末尾优化项或 Phase 7b（ROADMAP 原文口径），验收数字
  必须先由标量路径达成。
- **GPU / 量化（PQ/IVF）**：整个 Phase 2 不做。
- **索引内向量去重（v1.1 补充）**：M4 插入不去重——重复向量 = 独立
  节点，NodeId 各异。ROADMAP 价值清单的"记忆语义去重（近似向量检测）"
  是查询期/应用层后处理，任何阶段都不在索引内发明去重逻辑。

---

## §2 Crate 归属与依赖方向

**选项**：
(a) 新建 `pg-am-hnsw` crate；
(b) 塞进 `pg-am-btree`（"都是索引"）；
(c) 塞进 `pg-engine`（"迟早要贯通 SQL"）。

**选择：(a) 新建 `pg-am-hnsw`。**

**依赖落地的 v1.5 修正**：本节原表述"只依赖 `pg-storage`（类型与错误）"在
M4 没有消费者——编码/距离/PRNG 全手写，CRC 走 `crc32fast` 直依赖，错误类型
用 `thiserror` 自建（§10 的"提前对齐错误形态"诉求由 thiserror 惯例已满足）。
照原表述加依赖 = M4 全程未使用的空挂死依赖（M3 O4 清除并立规的对象）。
**修正为：M4 直依赖 `{thiserror, crc32fast}`;`pg-storage` 依赖缓至 M5**(
接入 WAL/Buffer Pool 时自然产生真实消费点）。"依赖逐 milestone 扩大"的
原理由不变，且由此更严格地兑现。

**理由**：

- 与 Phase 1 的分层纪律一致：AM 各成 crate（heap / btree 分立），依赖单向
  无环。HNSW 的页布局、WAL 记录、并发模型都与 B+Tree 完全不同，塞进
  `pg-am-btree` 只会污染后者的模块边界。
- M4 的依赖面刻意收窄：`pg-am-hnsw` 直依赖仅 `{thiserror, crc32fast}`（v1.14
  校正——本行原文"只依赖 `pg-storage`（类型与错误）"与上方 v1.5 修正块直接
  矛盾，系 v1.5 漏改的正文残留；`pg-storage` 依赖缓至 M5)——
  **不依赖** `pg-txn` / `pg-catalog` / `pg-engine`，使 M4 的测试矩阵与
  Phase 1 全量回归完全解耦，编译与测试都快。
- M5 接入 WAL/Buffer Pool 时再加 `pg-storage` 的 buffer_pool/wal 特性依赖，
  M6 才接 `pg-txn`（可见性、Tier 1 同步）。依赖逐 milestone 扩大，而不是
  一开始背全。

**代价**：序列化编码（§3）在 M4 冻结时还没有 engine 侧消费者，格式评审只能
靠文档与前瞻推演——用"M5 不重改格式"作为冻结验收标准来对冲。

---

## §3 向量内存表示与序列化编码（为 M5 冻结）

**内存表示**：

- 向量 = `&[f32]` / `Vec<f32>`，维度作为图实例级参数（`dim: u16`）在
  `Hnsw::new(dim, params)` 时固定，**不允许同图混维**——比逐节点存维度省
  内存，且维度错误在建图入口就失败。
- 节点标识 `NodeId(u32)`：稠密递增、永不复用（M4 无删除）、**序列化往返后
  稳定**。稳定性是 M5 的硬契约：M5 的节点页寻址与 WAL 记录都以 NodeId 为
  键，M4 若允许 compact/relabel，M5 的 WAL 记录语义全部要重定。

**序列化编码（快照格式，§7 使用；on-disk 节点页布局归 M5 另行设计，但逐节点
的字节流编码与此保持一致）**：

```
snapshot_header := magic:u32 | format_version:u16 | dim:u16 |
                   m:u16 | m_max0:u16 | ef_construction:u32 |
                   node_count:u32 | entry_point:u32 | max_level:u8
node := flags:u8 | reserved:u8 | vector:f32[dim] (LE) |
        level_count:u8 | per-level { neighbor_count:u16 | neighbors:NodeId[.] (LE u32) }
```

- 头部全字段定宽小端（v1.3 钉死：magic/version/node_count/max_level 的
  宽度如上，不再有松字段）。
- **params 语义（v1.3 钉死）**：入快照的只有**构造期参数**
  `m / m_max0 / ef_construction`——它们决定图形态，是图状态的一部分；
  `ef_search_default` 是查询期缺省，**不进快照**。load 策略（v1.14 校正）：
  采用快照内的构造参数还原图；`metric` 与 `ef_search_default` 由
  `load(path, metric, ef_search_default)` 入参供给（前者 §3 不进快照，
  错配静默改变语义但不可能 panic——Cosine+零向量在 load 处响亮拒绝；
  后者按快照的 `m` 重跑 §4.4 校验）。magic 不符 / format_version 未知 /
  参数越界（`m < 2`、`ef_construction < m` 等构造校验重跑）均响亮报错。

- 小端定长字段，手写编解码（§10 零依赖传统），不加自描述开销。
- **`dim` 只存快照头，节点记录不内嵌**——维度是图实例级固定参数（上文已
  声明同图不混维），逐节点存 dim 既是 2B/节点的纯冗余，编码语义也自相矛盾
  （v1.1 修正）。
- **位置即身份（v1.2）**：节点记录无显式 `NodeId` 字段——流中第 i 条记录
  即 `NodeId(i)`。该设计只在 NodeId 稠密时成立，因此把删除语义钉死在
  tombstone-in-place（NodeId 永不物理回收，见 §11 O3);M6 若改物理回收，
  本格式必须加显式 NodeId 字段（格式修订，过修订记录）。
- **`level_count` = 该节点出现的层数 = 最高层号 + 1**（层 0 必有，故
  `level_count ≥ 1`;v1.2 消歧）。与内存表示的关系：§6 的
  `levels[node]` 存最高层号 L，编码存层数 L+1，换算恒等式
  `level_count == levels[node] + 1` 进 load 校验。
- **`entry_point` 编码（v1.2 钉死）**:u32；空图（`node_count == 0`）用哨兵
  `u32::MAX`(`NodeId::INVALID`);`node_count > 0` 时 `entry_point` 必须
  `< node_count` 且 `max_level` 等于入口节点的最高层号，load 端逐条校验，
  违例响亮报错。
- **max_level 校验加严两条（v1.6，Stage A 实现比本节原文更严，以代码为准
  回改）**:① 空图强制 `max_level == 0`（无节点即无层，非 0 即响亮报错）;
  ② 任何节点的最高层（`level_count - 1`）不得超过 `max_level`——入口点是
  全图最高节点。实现位置：`encoding.rs` `decode_snapshot_body` 的 load 校验
  清单第 8/9 条。
- **`m_max0` 校验（v1.7 闭合，原 v1.6 契约空白登记）**:`m_max0 >= m` 已加入
  `HnswParams::new` 构造校验与快照 load 参数重跑（`m_max0 == 0` 或 `< m` 响亮
  报错；实现：`params.rs`、`encoding.rs` 快照头校验）。Stage B 的邻居列表
  shrink 逻辑（§4.2 的 `M_max0 = 2M` 口径）可直接消费，无契约空白。
- **`flags:u8` 预留 M6 删除（tombstone）位，M4 恒 0**(v1.2);load 时遇非 0
  flags 即未知版本内容，响亮报错而非静默忽略。`reserved:u8` 恒 0，同口径。
- `vector` 与 `neighbors` 同记录紧邻排布，为 M5"节点页 = 若干 node 记录"
  的布局探路。

**维度上限**：M4 不硬限（内存无页约束），但 M5 单页节点布局的可行域约为
`dim ≤ 2000`（8 KiB 页容纳向量 + 邻居列表）。验收数据集最大 960 维，落在
可行域内；**超 2000 维的溢出/TOAST 方案列为 M5 开放问题**（§11 O1），M4 不
为此设计。

**代价**：格式在只有单一生产者（M4 快照）时冻结，有过度承诺风险——缓解办法
是把编码写成独立小模块（`encoding.rs`），M5 若必须调整，改动面被限制在一个
文件内并过修订记录。

---

## §4 HNSW 核心算法与参数选型

基准文献：Malkov & Yashunin, *HNSW*（TPAMI 2018）。参数口径与两个事实标准
实现对齐：[hnswlib](https://github.com/nmslib/hnswlib)（算法原作者实现）与
[pgvector](https://github.com/pgvector/pgvector)（PG 生态对标，M6 验收对手）。

### 4.1 层级生成

**选择**：几何分布，`level = floor(-ln(u) * m_L)`,`u ~ Uniform(0,1)`,
`m_L = 1 / ln(M)`——论文 §3.1 原始方案，hnswlib/pgvector 同款。

**理由**：指数衰减的层分布是 HNSW 对数复杂度的来源；没有偏离论文的动机。
**代价**：无。

**u64→f64 转换口径与 u=0 处理（v1.2 钉死）**：转换方式本身就是确定性的
一部分，必须冻结——取 PRNG 输出的高 53 位：
`u = (r >> 11) as f64 * (1.0 / 9007199254740992.0)`(2⁵³ 倒数）,`u ∈ [0, 1)`。
`u == 0.0` 是合法抽样结果（概率 2⁻⁵³),`-ln(0)` 溢出为 +inf——**不是不变量
违例，断言 panic 是错的；处理为重抽（redraw)**：redraw 保持分布无偏，且
M=16 下 redraw 后 level ≤ floor(53·ln2/ln16) = 13,§11 R2 的上界断言由此
成为真正不可达的防御（而非兜底 u=0 的常规路径）。

**确定性要求**：随机源必须是**显式 seed 的 PRNG 实例**（按图实例持有，
不从全局熵池隐式取），同一插入序列 + 同一 seed → 字节级同构图。基准可复现、
回归测试可对拍、bug 报告可重放——这条是 M4 测试方法论（§9）的地基。
（注：Rust 不做浮点收缩，标量 f64 运算跨平台按 IEEE 754 精确舍入，
字节级同构在 SIMD 优化（§5）引入前跨平台成立。）

**确定性的实现前提（v1.3 钉死）**:

- ① **构造期并列决胜与查询侧同规**：候选堆/选中集的全部排序键为
  `(distance, NodeId 升序)`——§8.2 只钉了输出侧，构造期的候选队列
  同键决胜，否则同 seed 同输入仍会因并列展开顺序不同而发散。
- ② **全程禁止依赖 HashMap 迭代序**：visited 集等簿记用有序/位图结构
  （`BTreeSet` 或按 NodeId 索引的 bitset)；任何 `HashMap`/`HashSet` 的
  迭代序都是非确定源（SipHash 随机状态）。
- ③ **PRNG 状态不进快照**:save→load 后继续插入，与未中断路径的图不同
  （PRNG 位置丢失）。M4 口径下无害（快照用于基准加载，不用于续建），写明
  以免 M5 当 bug 追查；M5 的 WAL 路径天然无此问题（重放不重抽）。

### 4.2 M / M_max / ef_construction

| 参数 | 选择 | hnswlib 默认 | pgvector 默认 | 说明 |
|---|---|---|---|---|
| `M` | **16** | 16 | 16 | 每节点每层最大出边；recall/内存的主旋钮 |
| `M_max` | `M`(=16) | 同 | 同 | 非 0 层上限 |
| `M_max0` | `2M`(=32) | 同 | 同 | 第 0 层上限（承载全部节点，放宽保 recall) |
| `ef_construction` | **200** | 200 | 64 | 建图候选池；对 recall 影响大于 ef_search |

**选择理由**：默认值之间取 recall 优先的一侧。hnswlib 的 `ef_construction=200`
是 sift 上 recall@10 ≥ 95% 的充分条件（ef_search=64 口径下；硬门槛以 §12
为准——v1.3 补注）；pgvector 的 64 是建图速度优先的妥协
（其文档自认 recall 略低）。M4 的验收口径是 recall 达标（§12），建图慢可接受
（一次性成本）；M6 的对标阶段再测双方默认参数（含 pgvector `ef_search=40`）
下的 recall/latency 曲线。

**参数实例化**：`HnswParams { m, m_max0, ef_construction, ef_search_default }`,
构造时校验（`M ≥ 2`、`ef_construction ≥ M`），不接受运行时魔数。

### 4.3 邻居选择启发式

**选项**：(a) simple（取最近的 M 个，论文 Algorithm 3）;(b) robust prune /
heuristic（论文 Algorithm 4，带 `extend_candidates`、`keep_pruned` 两开关）。

**选择：(b) 论文 Algorithm 4,`extend_candidates = false`（全层）、
`keep_pruned = true`**（v1.3 修正归属与开关组合）。

**开关归属澄清（v1.3，原 v1.0 表述有误）**:`extend_candidates` /
`keep_pruned` 是**论文** Algorithm 4 的开关——论文只说 extend "set to false
by default"（聚类数据实验才开 true),**无任何"第 0 层例外"**；论文的第 0 层
特殊化是 `M_max0`（连接上限，§4.2 已正确收录）。hnswlib 的
`getNeighborsByHeuristic2` 实现的则是**无开关的固定启发式**（不做候选扩展，
被遮挡候选直接丢弃，选中集可少于 M)——v1.0 的"hnswlib 默认组合（仅第 0 层
extend=true)"不成立，疑似把 `M_max0` 的第 0 层特殊化错记到
`extend_candidates` 头上。照原字面实现，第 0 层（全节点层）做
neighbors-of-neighbors 扩展是每插入 O(度²) 的最贵解读，行为与成本同时偏离
两个参照实现。

**`keep_pruned = true` 的自证理由**（不引用外部出处）：被遮挡候选降级保留至
凑满 M，保证每节点尽量满 M 边——利好 §9① 连通性不变式与低密度区域的
recall;hnswlib 允许选中集少于 M 是其性能取向，我们取连通性优先一侧。

**理由**：启发式通过"候选邻居之间互斥遮挡"保证图的 navigability（长边保留），
是低维与高维数据上 recall 差距的主要来源；siftsmall 上对拍实验（§8 harness
天然支持开关 A/B）会先验证该结论在我们的实现上成立再冻结。注意启发式作用于
插入算法的**两侧**（v1.1 补充）：插入点的邻居选择，**以及新边加入后邻居列表
超过 `M_max`/`M_max0` 时的收缩（shrink)**——只做选择不做收缩会让邻居表
无界增长，recall 虚高且内存失控。
**代价**：实现复杂度高于 simple（候选堆 + 遮挡判定，选择 + 收缩两处接入），
但这是一次性成本；simple 路径保留为对照组——**运行时构造参数开关**
（`NeighborSelection` + `#[doc(hidden)] new_with_neighbor_selection`,v1.11
口径统一：reachable but unsupported——下游可达、隐藏于文档、零稳定性保证，
采完 A/B 数据即可删除；v1.0 的"编译期开关，不进公开 API"两处表述均不准）。

### 4.4 搜索（ef_search）

- 贪心搜索：从入口点逐层下降（每层 greedy 到局部最近点），第 0 层用
  `ef_search` 大小的候选堆做 beam search——论文 Algorithm 2/5 原样实现。
- `ef_search` 为**查询时参数**（`search(query, k, ef: Option<usize>)`，缺省
  用图状态里的 `ef_search_default`——它进 `HnswParams` 但**不进快照**（§3），
  load 时由调用方入参供给并按快照 `m` 重跑校验（v1.14 校正：原文"不进图
  状态"不准确——它不进的是快照，不是图状态；load 后图的查询缺省即入参值），
  M6 的 SQL 层需要按查询调。
- 不变式（v1.16 校正，对齐 coding plan Stage B v1.3 钉死口径）：**逐查询只校验
  `ef ≥ k`**（允许 ef < M）；`ef_search_default ≥ M` 约束的是**构造参数缺省**
  （`HnswParams::new` 与快照 load 重跑），不是查询路径不变式——两条互不混用。
  校验失败即参数错误，不静默截断。

---

## §5 距离函数

**交付三个，口径冻结如下**：

| 度量 | 定义 | 适用 | 实现要点 |
|---|---|---|---|
| L2 | **平方**欧氏距离 `Σ(aᵢ-bᵢ)²` | sift/gist 验收、通用 | 不开方——单调性不变，排序等价，省 sqrt |
| Cosine | `1 - (a·b)/(|a||b|)` | embedding 场景主力 | 零向量响亮报错（不静默返回 1.0) |
| Inner Product | `-a·b`(负内积) | MIPS 检索 | 取负把 max-IP 变成 min-距离，搜索代码零分支 |

**数值路径**：元素 f32、**累加器 f64**，手写标量循环。（v1.3 修正：f64
累加循环**不可**自动向量化——LLVM 在无 fast-math 时不对浮点做重结合，
这恰是 §4.1 跨平台确定性成立的机制；可向量化的只有元素级 f32 差/方，
收益有限。性能预期按纯标量估计，验收数字已留余量。)

**入口校验（v1.3 补充；v1.6 加严）**:`insert` / `search` 拒绝 **NaN** 与
**±inf** 分量（以及 `dim = 0` 的图构造）——NaN 让一切距离比较静默返回
false，图照常插入但 recall 悄悄劣化，且确定性不受影响所以 §8.3 的对拍抓不到
它；±inf 同型（cosine 的 `inf/inf`、L2 的 `inf−inf` 静默产出 NaN),v1.6 起
按 `!is_finite()` 一并拒绝。入口响亮报错是唯一防线。Cosine 的零向量报错
（上表）同为此类。

**理由**：

- f64 累加把 960 维浮点误差压到远低于排序噪声的水平；f32 累加在 960 维下的
  相对误差 ~1e-5 量级，对 recall 边缘（第 10 名与第 11 名差距）可能产生可测
  影响——正确性优先，性能归基准数据说话。
- SIMD（f32x8 累加）的加速与精度回退（f32 树状归约）绑在一起，归 M4 末尾
  优化项：切换前后必须过 recall 基准 + 对拍（§9），不允许"快了但悄悄变了
  结果序"。

**代价**：f64 累加器比纯 f32 标量慢 ~10-20%（可测但不影响 P99 < 20ms 目标
  的量级）；若基准显示宽裕，不再回头优化。

**实现注记（v1.8）**：三度量的分发枚举 `Metric { L2, Cosine, InnerProduct }`
位于 `graph.rs` 而非 `distance.rs`——§3 冻结快照头不含 metric 字段，它是图
实例的运行时属性（load 时由调用方重新供给），`distance.rs` 只承载三个纯
函数。以代码为准，本节登记归属事实。

---

## §6 内存布局与图表示

**选择：SoA（结构体数组）+ 分层邻接表**：

```
Hnsw {
  dim: u16, params: HnswParams, rng: Xoshiro256StarStar(seeded),
  vectors: Vec<f32>,            // 全部向量连续 arena,node i 占 [i*dim, (i+1)*dim)
  levels: Vec<u8>,              // node -> 最高层
  adjacency: Vec<Vec<Vec<NodeId>>>, // node -> level -> neighbors(层 0 必有)
  entry_point: Option<NodeId>, max_level: u8,
}
```

**理由**：

- `vectors` 连续 arena：距离计算是内存带宽瓶颈，连续布局对预取友好；
  per-node `Box<[f32]>` 的指针追逐在基准下可测地慢（hnswlib 同为连续布局）。
- 邻接表 `Vec<Vec<Vec<NodeId>>>` 而非按层分页的扁平数组：M4 无内存硬约束，
  读写简单优先；M5 落盘时按 NodeId 重排为页布局，内存表示不要求与页同构。
- 上层（level ≥ 1）节点稀疏，邻接表天然只为有该层的节点分配，无浪费。

**代价**：邻接表三层嵌套 Vec 的缓存局部性一般；M6 并发期大概率要换
（epoch 回收需要稳定的边存储代际），M6 选型时重估，M4 不为未来设计。

---

## §7 快照序列化与加载 API

**ROADMAP 原文**："从磁盘加载预构建的图（用 hnswlib 格式互通）"。

**选项**：(a) 兼容 hnswlib 二进制格式；(b) 自定义快照格式（§3 编码）。

**选择：(b) 自定义快照。**

**理由**：

- hnswlib 格式是其内存布局的直接转储（含其实现特定的 label 表与删除标记位），
  无版本化、无校验和，随其版本漂移；兼容它等于把 M4 的序列化正确性绑在外部
  实现的内部细节上——与 Phase 1"格式自研可控"传统冲突。
- 对标的正确姿势是**同数据集各自建图比 recall/延迟**（§8、M6)，不是互换
  二进制图。
- 自定义快照：§3 定宽头 + node 记录流 + **CRC32 前缀**(`crc32(4B) + body`,
  v1.3 修正——与 pg-storage 既有惯例一致：`checkpoint.rs` 快照与 FreelistMeta
  均为前缀 CRC，检测 bit-rot 而非静默产出"合法但错"的图）;API 为
  `save(graph, path)` / `load(path, metric, ef_search_default)`(v1.14 校正——
  v1.0–v1.13 写的 `load(path)` 是占位描述：metric 不进快照（§3）由调用方供给，
  `ef_search_default` 是查询期状态（§3 v1.3）由 load 入参重跑 §4.4 校验）,load
  全量校验（维度一致、NodeId 稠密、邻接端点存在、**非有限（NaN/±inf）分量拒绝**
  ——v1.4 闭环：load 也是图内容入口，我们自己的 save 产不出 NaN、CRC 挡位翻转，
  此条属 belt-and-suspenders，但"入口响亮报错"（§5）应对所有入口成立；v1.6 起与
  §5 同口径按 `!is_finite()` 拒绝 ±inf），坏文件响亮报错。

**代价**：放弃"直接加载 hnswlib 预构建图"的便利——该便利的唯一场景是省一次
建图时间，而 §8 的 harness 本来就要从 fvecs 原始数据建图，需求实际不存在。
ROADMAP 此条按本节的解释执行，修订记录留痕。

---

## §8 Recall 基准 harness

### 8.1 数据集

[TEXMEX corpus（irisa.fr）](http://corpus-texmex.irisa.fr/)，ANN 基准的
事实标准数据源（[ann-benchmarks](https://github.com/erikbern/ann-benchmarks)
同源）：

| 数据集 | 规模 | 维度 | 查询数 | ground truth | 用途 |
|---|---|---|---|---|---|
| **siftsmall** | 10k base | 128 | 100 | 100-NN ivecs | **CI 口径**（秒级，确定性） |
| **sift** | 1M base | 128 | 10k | 100-NN ivecs | 验收口径（ROADMAP 命名） |
| **gist** | 1M base | 960 | 1k | 100-NN ivecs | 验收口径（高维压力） |

下载（v1.4 事实修正——v1.3 的"HTTP 镜像"表述错误，已实测证伪）:**官方
分发渠道只有 FTP**(`ftp://ftp.irisa.fr/local/texmex/corpus/{siftsmall,sift,
gist}.tar.gz`)。2026-08-31 本机实测：`corpus-texmex.irisa.fr` 的 HTTP 站
只是落地页（数据集链接全部指向 ftp，直接 GET 文件 404),`ftp.irisa.fr` 的
HTTP 面 403(Apache 只放 FTP),FTP 协议在本网络超时——不存在官方 HTTP 镜像。
因此：**CI 方案 = ftp 直连 + artifact 缓存**（首选；Actions runner 对 irisa
FTP 的连通性需实测一次——列入 coding plan 任务，一次成功后缓存基本不再触网
）;**备选 = 一次性取回后自托管**（GitHub release / 自有 artifact，彻底去
ftp 依赖，代价是多一步托管）;**本地开发**在 ftp 不通的网络下手动取回放入
`datasets/`（不进 git)。fvecs/ivecs 解析器手写（格式 = `dim:i32 | payload`
重复，~40 行）。

**ROADMAP 文字校订**:"1M 768d recall@10 ≥ 95%" 的 768d 与其括号内数据集
（sift-128 / gist-960）不一致——以数据集名为准，768 维场景由 gist-960 覆盖
（同量级），修订记录留痕。

### 8.2 口径

- **recall@k** = mean over queries `|retrieved_top_k ∩ truth_top_k| / k`,
  k=10。**真值直接取 corpus ivecs 每行的前 10 个、保持原序，不做任何
  重排**（v1.1 修正）：ivecs 是 corpus 自身暴力算法算好的有序结果，其
  并列决胜规则未知且不可改，按我们的规则重排真值会在第 10/11 名并列处
  人为压低 recall。决胜规则（距离并列按 NodeId 升序）只作用于我们
  返回结果的内部排序，不碰真值侧。
- **延迟**:P50 / P99,ef_search=64,单线程,预热后 3 轮取中位。
- **建图时间 / 加载时间**：墙钟，机器规格随结果一并记录。
- harness 形态：`pg-am-hnsw` 的 **bench + example 双轨**——example
  (`m4_recall_probe`)跑真实数据集产数字（对齐 M3 的 wal_bytes_probe 先例）,
  criterion bench 跑合成小数据集防性能回退。
- **CI 策略**:siftsmall 全量进 CI（recall@10 ≥ 98% 硬门槛——小数据集
  应显著高于 1M 口径）;sift/gist 1M 归手动/nightly，结果落盘
  `docs/phase2-m4-benchmarks.md`（对齐 M1-M3 的 benchmark 文档传统）。
  数据集不进 git,CI 用缓存 artifact；下载脚本人 `scripts/`。
- **CI 注册任务（v1.1 补充，v1.3 校正计数，M3 pg-wire 的教训）**:ci.yml
  按 crate 显式枚举的是 **clippy / test / doc 三个 matrix**（fmt 是单 job
  无 matrix,stage_spec Stage F 的记录为准），新 crate `pg-am-hnsw` 落地时
  必须同步：① workspace 根 `Cargo.toml` 的 `members` 注册；② ci.yml 三个
  crate matrix 加 crate;③ 归类 loom 豁免分支（M4 无 loom 模型，走非 loom
  分支）;④ 核对既有 grep 护栏分支对新 crate 的适用性;⑤ **新建 coverage
  job**(v1.3 补充）：§12 的"覆盖率 ≥90% + tarpaulin artifact"在现有 CI 里
  **没有现成工具链**——需新 job + artifact 上传；且 tarpaulin 仅支持
  Linux(ptrace)，本机 macOS 跑不了，coding plan 排期不得按"本地可测"估。
  缺了任何一条，§12 的"CI 硬门槛"就是空话。

### 8.3 对拍（correctness oracle）

- siftsmall 全量 + sift 抽样 10k base 上，HNSW 以 `ef = base 节点数`
  （v1.3 修正：操作性条件就是 ef ≥ 节点数，原 `max(base, 256)` 中的 256
  在两个口径下均为死重）搜索的结果必须与**暴力扫描**完全一致。该性质的前提显式声明（v1.1 修正）：**第 0 层
  图连通（§9 不变式①）**——ef ≥ 节点数时 beam 从不淘汰候选，搜索洪泛整个
  连通分量；离开连通性前提，"ef 足够大即精确"不是 HNSW 的普适性质，对拍
  失败时先要查连通性而不是查距离函数。
- **查询集与数据同分布**（v1.12,Stage B 第四轮审查 P3）：基准方法论惯例
  （sift/gist 自带同分布查询集）；合成数据侧的实现 = 查询沿**同一个**
  MixtureGen 流继续抽（簇心在构造期抽取，另起生成器会把查询打到别的
  团块上——实测 recall@10 0.989 vs 同分布 1.000 @ ef=64）。
- **InnerProduct 的推广口径**（v1.12，同轮 P3）：IP 非度量，有向可达分量
  不完整是合法现象（实测 1889/2000，默认参数）——flood 等价模式对 IP 必须
  带连通性前提执行：ef = N 洪泛恰返回**有向可达分量**（不多不少）且按冻结
  序逐位全等；可达计数钉死为区间观测（非不变式）。图层面 IP 覆盖即此 cell,
  recall 质量验收仍归 M6（§12 v1.2）。
- 邻居选择启发式 A/B(§4.3):simple vs heuristic 在 siftsmall 上的 recall
  对照实验随 harness 交付，作为参数冻结的证据附件。

---

## §9 验证方法论（M4 特化）

Phase 1 的方法论（对抗审查、watchdog、红绿对照）全部沿用；M4 无并发无 WAL,
两个标志性手段的替代形态：

- **loom → 属性测试 + 对拍**：图不变式属性测试——① 全节点从入口点可达
  （连通性；**参数区间事实**：极端参数 M=2/M_max0=2/ef_c=4 下 shrink 可斩断
  最后桥接边致第 0 层不连通——Stage B 实测 N=300 有向可达仅 3，默认参数区间
  方成立，2026-08-31 Stage B 登记）;② 双向边不对称率统计（M6 验收 <1% 的
  口径在 M4 先建立测法；**Stage B 实测基线：默认参数聚合 15.35%，逐 cell
  8.9%–17.1%**——shrink 单侧删边的固有不对称，M6 的 <1% 已修订为相对该基线
  的增量口径，见 ROADMAP Phase 2 验证标准）;③ 层分布与几何期望的卡方拟合
  （seed 固定下确定性成立）;④ 搜索单调性：ef 增大 recall 不降。
- **崩溃注入 → 快照往返等价**:`save → load → search` 与内存原图逐查询
  结果全等（字节级同构图的直接推论）。
- 单测覆盖率 ≥ 90%(ROADMAP 口径）,tarpaulin 报告进 CI artifact。
- 距离函数用已知答案测试（手工算的三维/四维向量组）+ 与 f64 参考实现的
  容差对拍（1e-12 相对误差）。

---

## §10 依赖

**选择：零新运行时依赖**（与 M1-M3 传统一致）。

- **PRNG 手写 xoshiro256\*\***（~50 行，公开算法，seed 显式注入，§4.1
  确定性要求）;不引 `rand`——所需仅均匀 u64，rand 的 API 面（分布、
  线程rng)全部是冗余。
- fvecs/ivecs 解析与快照编解码手写；**CRC32 复用 workspace 既有的
  `crc32fast`**（pg-storage 的 WAL 校验同件，v1.1 修正：项目惯例是
  "格式手写、成熟原语用既有 crate"，不是一切手写）。
- **依赖口径澄清（v1.1;v1.5 改写）**:"零新运行时依赖"指 workspace 依赖图
  无新增 crate。**M4 实际依赖面（§2 v1.5 修正后的口径）**：直依赖只有
  `thiserror` + `crc32fast`，均为 workspace 既有件；原设想的 pg-storage
  依赖（会传递引入 parking_lot/bytes/serde/bincode/tracing）已缓至 M5——
  M4 对它无消费点，空挂即死依赖。M5 接入 WAL/Buffer Pool 时自然引入
  pg-storage，届时错误类型经 `#[from]` 桥接，集成一致性不受缓期影响。
- dev-dependency 不新增 workspace 外的 crate(v1.3 措辞修正）:criterion
  沿用既有 0.5（已是 5 个 crate 的 dev-dep，不扩 workspace 图）；数据集下载
  用 shell 脚本 + curl;tarpaulin 走 §8.2 新建的 coverage job(Linux only,
  见该条）。

**代价**：xoshiro 手写需自带已知答案测试（公开测试向量），一次性成本。

---

## §11 风险与开放问题

**风险**：

- **R1 gist-960 的 recall 达标难度**:960 维是 sift 的 7.5 倍，维数灾难下
  M=16/ef_construction=200 未必够到 95%。预案：允许 gist 单独上调参数
  （M=32)并在 benchmark 文档如实记录"分数据集参数"——口径诚实优先于
  参数统一；若仍不达标，升级 neighbors heuristic 的 `extend_candidates`。
- **R2 层级溢出**:`u8` 层号到 255 才溢出，几何分布下 P(level ≥ 16) ≈
  M⁻¹⁶,实际不可达；层级生成仍加显式上界断言（防御性，对齐 Stage S 的
  `ensure_root_promotion_fits` 先例）。v1.2 边界澄清：断言防的是真正的
  不变量违例；`u == 0.0` 是合法抽样结果，走 §4.1 的 redraw,不进断言。
- **R3 1M 建图内存**:1M × 960d × 4B ≈ 3.8 GB(gist)+ 邻接表 ~0.2 GB——
  开发机内存门槛写进 benchmark 文档，CI 只跑 siftsmall 不受影响。

**开放问题（归期明确）**：

- **O1 超 2000 维向量的节点页溢出/TOAST 方案** → M5（页布局设计时）。
- **O2 f16/bf16 存储编码** → M5(与 O1 同窗口）。
- **O3 删除的内存语义 → M6，但 M4 已把一半钉死**：§3 冻结格式是
  "位置即身份"（节点记录无显式 NodeId 字段），该设计只在 NodeId 稠密时
  成立，因此 **M6 的删除只能 tombstone-in-place**(NodeId 永不物理回收，
  删除位即 §3 预留的 `flags:u8`);M6 若想做物理回收/compact，冻结格式
  必须加显式 NodeId 字段——那是格式修订，过修订记录。边修剪策略
  (tombstone 节点的邻居连通性修复）仍完全归 M6。
- **O4 SIMD 切换的精度门禁** → M4 末尾优化项或 Phase 7b。

---

## §12 验收标准草案（供 coding-plan 引用）

**性能与质量门槛**（ROADMAP.md:259-262 口径，机器规格随数字记录）：

| 指标 | 门槛 | 口径 |
|---|---|---|
| recall@10 | sift ≥ 95%、gist ≥ 95% | §8.2,ef_search=64,全量 1M |
| 搜索延迟 P99 | < 20ms | ef_search=64，单线程，预热 3 轮中位 |
| 1M 加载时间 | < 5 分钟 | §7 快照 load，含校验 |
| CI 硬门槛 | siftsmall recall@10 ≥ 98%(**ef_search=64**,v1.3 钉死) | 全绿才算过；flaky 零容忍（确定性 seed 下无 flaky 借口） |
| 单测覆盖率 | ≥ 90% | tarpaulin,CI artifact |

**度量覆盖口径（v1.2 声明）**：上表 recall 基准只覆盖 **L2**(sift/gist
均为欧氏数据集）。Cosine 与负内积**不是度量**（三角不等式不成立）,HNSW
邻居选择依赖的距离几何在这两者下是未经验证的——M4 只保证它们的距离函数
**正确性**（§9 已知答案测试 + 容差对拍，ROADMAP "基础实现正确"口径）;
Cosine/IP 的 recall 质量归 M6 真用该度量的场景验证（届时补对应数据集基准，
如 glove-angular)。读者不应从上表推出"三个度量的 HNSW recall 都过了基准"。

**方法论门槛**（Phase 1 纪律的 M4 映射）:

- 属性测试四件套（§9）+ 暴力对拍全等 + 快照往返等价，全绿。
- 距离函数已知答案测试 + 容差对拍。
- 对抗审查至少两轮（P1 必修、P2 登记），结论归档 stage_spec。
- `docs/phase2-m4-benchmarks.md` 落盘:sift/gist 实测 + 参数冻结证据 +
  机器规格。

---

## 修订记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v1.0 | 2026-08-31 | 初版。对 ROADMAP 2a 的两处范围修正：VECTOR(n) DDL 拆分交付（§1)、快照格式自定义不兼容 hnswlib（§7);ROADMAP "1M 768d" 文字校订为以数据集名为准（§8.1) |
| v1.1 | 2026-08-31 | 第一轮对抗审查修复（P2×2 + P3×5):§8.2 补 CI 注册任务（ci.yml 四 matrix + workspace members + loom 豁免归类，M3 pg-wire 教训）与 ground truth 口径修正（直接取 ivecs 原序前 10，真值侧不重排）;§3 节点记录删除冗余 dim 字段（与"同图不混维"矛盾）;§8.3 精确搜索补第 0 层连通性前提；§4.3 补邻居列表收缩侧也走启发式；§10 CRC32 事实修正（crc32fast 既有件非手写）+ 依赖口径澄清；§1 补索引内不去重声明 |
| v1.2 | 2026-08-31 | 第二轮对抗审查修复（冻结格式专题）:§3 钉死四条——位置即身份与 M6 删除的交互（tombstone-in-place 为唯一兼容路径）、空图 entry_point 哨兵编码（u32::MAX)、level_count = 层数（L+1）消歧、flags/reserved 语义预留；§4.1 钉死 u64→f64 转换（高 53 位）与 u=0 redraw（非断言）;§11 R2 边界澄清、O3 补删除语义推论；§12 补度量覆盖口径声明（Cosine/IP 的 recall 质量归 M6 验证） |
| v1.3 | 2026-08-31 | 第三轮对抗审查修复（P2×2 + P3×6 + nano×5):§4.3 开关归属修正（extend/keep_pruned 是论文 Algorithm 4 开关、extend 全层 false;hnswlib 是无开关固定启发式——v1.0"hnswlib 默认组合、仅第 0 层 true"不成立，keep_pruned=true 改自证理由）;§8.2 CI 清单校正（matrix 实为 clippy/test/doc 三个、fmt 无 matrix）+ 新建 coverage job 入清单（tarpaulin 非现成工具链、Linux-only)+ 数据集下载走 HTTP 镜像（v1.4 证伪撤回）；§3 快照头全字段定宽 + params 语义钉死（构造期三参数入快照、ef_search_default 不进、load 采用快照参数）;§4.1 确定性实现前提钉死（构造期 (distance,NodeId) 决胜、禁 HashMap 迭代序、PRNG 状态不进快照）;§5 NaN/dim=0 入口拒绝 + 自动向量化表述修正（f64 累加不可重结合恰是确定性机制）;§7 CRC32 改前缀（对齐 checkpoint.rs/FreelistMeta 惯例）;§8.3 对拍条件改为 ef=节点数（256 死重）;§10 dev-dep 措辞与 §8.2 对齐（criterion 0.5 沿用）;§12 CI 门槛钉 ef_search=64;§4.2 充分条件补 ef 口径 |
| v1.4 | 2026-08-31 | 第四轮审查修复（P2×1 + nano×2):§8.1 下载渠道事实修正——实测证伪"HTTP 镜像"（官方仅 FTP;CI = ftp 直连 + artifact 缓存为首选、连通性实测归 coding plan,备选自托管 release，本地 ftp 不通时手动取回 datasets/);修订记录日期全部更正为 2026-08-31（文件创建时间佐证，v1.0-v1.3 均实际发生于该日）;§7 load 校验补 NaN 分量拒绝（belt-and-suspenders 闭环 §5 入口防线） |
| v1.5 | 2026-08-31 | 第五轮（coding-plan 审查回流）:§2 依赖落地修正——"只依赖 pg-storage"在 M4 无消费者，照原样加 = 空挂死依赖（M3 O4 立规对象）；修正为 M4 直依赖 {thiserror, crc32fast}、pg-storage 缓至 M5;§10 依赖口径澄清段同步改写（原 v1.1 段落描述的传递依赖取舍随 v1.5 不再存在，两节直接相反属冻结文档内部矛盾） |
| v1.6 | 2026-08-31 | 第六轮（Stage A 对抗审查回流，以代码为准回改）:§3 补 max_level 校验加严两条（空图强制 `max_level == 0`；任何节点最高层 ≤ max_level；实现位于 `encoding.rs` `decode_snapshot_body` 校验清单第 8/9 条）;§3 登记 `m_max0` 全链零校验的契约空白（归 Stage B 前评估，Stage B shrink 逻辑消费 m_max0);§5 入口校验与 §7 load 校验同步加严为 `!is_finite()`(NaN 与 ±inf 同口径拒绝，cosine 的 `inf/inf` 静默 NaN 收口） |
| v1.7 | 2026-08-31 | Stage A review 修复轮（用户确认）:§3 `m_max0` 契约空白闭合——`m_max0 >= m` 加入 `HnswParams::new` 构造校验与快照 load 参数重跑（`params.rs` / `encoding.rs` 快照头校验，负例测试两枚）；§3 v1.6 的"契约空白登记"条改写为闭合状态 |
| v1.8 | 2026-08-31 | Stage B 对抗审查回流（P1 零 / P2×1 / P3×3）:§9 ① 连通性改写为**参数区间事实**（极端参数 M=2/M_max0=2/ef_c=4 下 shrink 可斩断桥接边，N=300 有向可达仅 3；默认区间成立）；§9 ② 不对称率实测基线落盘（默认参数聚合 15.35%，逐 cell 8.9%–17.1%）并据此证伪 ROADMAP M6 "<1%" 字面口径——已回改 ROADMAP Phase 2 验证标准与 ROADMAP-changes A5/§3.1 为**增量口径**（P2-1，文档债 D10 登记清偿）；§5 补 `Metric` 枚举归属注记（graph.rs，快照头无 metric 字段，运行时属性）。P3-1 注释笔误（40→300 节点）与 P3-2 oracle 局限注记已落测试代码 |
| v1.9 | 2026-08-31 | Stage B 第二轮审查回流（换攻击面：负距离/退化数据/极端参数/确定性缝隙/整数边界/Stage C 读面——行为 bug 仍为零）:**P1-1** `HnswParams` 字段私有化（`pub` 字段可经结构字面量/事后变异完全绕过 §4.2/§4.4 校验，与 m_max0 空白同类），改私有 + getter，波及面 16 处 getter 化（含首轮漏改的 encoding/properties 9 处）；**P3** 手工推演注释块三处中间推理修正（step 5 幻影候选/遮挡归功、step 4 伪平局钉死、step 7 括号注矛盾——期望表与代码均正确，错在推导文字；prop3 seed 201 卡方 17.516 登记补落测试注释）；**nano**:rng `next_level` m≥2 前提 release 行为入 doc、入口追踪测试 else 分支补入口身份断言、Cand 注释维度上界改述 u16 上限、卡方临界表覆盖界注记 |
| v1.10 | 2026-08-31 | Stage B 第三轮审查回流（P1×1 + P2×1 + P3×3，以代码为准）:**P1** `search_layer` 准入改完整 (distance, NodeId) 决胜——原距离-only 比较在满 beam 平局时让结果集席位取决于发现序，与 §4.1 冻结全序冲突；等距小 id 候选现置换大 id worst,break 提前终止保持论文距离-only 语义不变（§4.4 语义细化，纸面行为变化仅限平局席位）；**P2** §3 load 校验清单第 10 条补层级归属半边（level-L 边要求目标 `level_count > L`——原清单"邻接端点存在"漏检，合法 CRC 的越层级边可通过 decode 并在 Stage C 重建后遍历 panic；新校验当场抓获 encoding 测试 fixture 自身的语义非法——node 2 的 level-1 边指向只有 level 0 的 node 1，fixture 随修）;**P3** prop1 补 directed 断言、prop2 加 [0.05, 0.30] 回归带守护 15.35% 基线、A/B 开关 `#[doc(hidden)]` 措辞精确化（reachable but unsupported） |
| v1.11 | 2026-08-31 | 复核遗留（nano，文档口径统一）:§4.3 "simple 路径保留为编译期可开关的对照组，不进公开 API"改写为**运行时构造参数开关 + reachable but unsupported**（v1.0 两处表述均不准：开关形式是运行时非编译期；"不进公开 API"与下游可调的事实矛盾）；coding-plan Stage B 任务行同条同步 |
| v1.12 | 2026-08-31 | Stage B 第四轮审查回流（P2×2 + P3×3，以代码为准）:**P2-1** §3 校验清单补第 11 条邻接表良构性（严格升序=无重复无降序/无自环/度数 ≤ m_max(level)——`push_edge` 的 binary_search 前提，缺失时 Stage C 重建静默插错位；畸形流实测可干净通过旧 decode）；**P2-2** 写入/读取对称：`validate_graph_data` + `SnapshotHeader::validate_construction_params` 提取为读写共用，encode 先验后写（原 encode 接受自己读不回的头部——pub 字段可构造 `node_count:0, entry_point:7` 之流）；**P3** prop4 ef 阶梯补 64（§12 门槛值）+ 0.95 绝对下限、查询集同分布修正入 §8.3、§8.3 补 IP 推广口径（flood 恰返回有向可达分量，可达计数 1889/2000 钉死为区间观测） |
| v1.13 | 2026-09-02 | Stage C 收口回流（实现期事实 + 对抗审查一轮，P1 零）:§7 落地形态——save 为**流式编码**(`BodyEncoder<W: io::Write>`：增量 CRC32 + 4B 占位 + finish 时 seek 回填，盘上字节布局与一次性 `wrap_crc32(body)` 完全一致，§3 冻结不动）+ 同目录临时文件原子 rename（进程内并发安全：独立临时名；跨进程不保证）,**不 fsync**（快照是基准/调试通道，持久化语义归 M5);§3 校验实现重构为读写共享三 helper（单一校验实现，decode 行为不变，既有负例单测原样全绿为证）;load 保守默认兑现（`insert` → `InvalidOperation`;graph `from_parts` 重建入口 + `read_only` 单点把关）；审查 P2-1：save 原全量物化峰值 ~3× 文件（1M gist ~4GB 快照 → ~16GB,OOM 风险）已随流式化消除，**load 剩余峰值 ≈2× 文件（NodeRecord 物化）登记为 Stage D 跑批前评估项**（连同 §11 R3 ~4GB 内存门槛与机器规格）；§3 格式自 Stage C 实际写盘起事实冻结（snapshot.rs 模块文档声明） |
| v1.14 | 2026-09-02 | Stage C 第三轮审查回流（用户外审，P1×2 + P2×4 + P3 批，以代码为准）:**P1-1** save 临时文件改 `OpenOptions::create_new`（O_EXCL 不跟随预置符号链接）+ 撞名重试上限 1024——原可预测名 + `File::create` 在共享可写目录下可覆写任意文件；**P1-2** load 新增校验第 6 段：Cosine 逐节点零向量检查（§5 入口校验覆盖 load 入口）——L2 快照以 Cosine load 曾在 search 路径 `expect` panic，metric 契约细化为"错配静默改变语义但不可能 panic";**P2** load 校验顺序重构（定长前缀预读 → header → `ef_search_default` 提前校验 → node_count 长度交叉检查（除法形式）→ 全量读 + CRC + 完整清单），校验清单新增第 12 条 `level_count ≤ 64`（几何分布硬上界 53 @ M=2,64 双倍余量；病态形态"255 空层 × 24B Vec 头"绝对值放大被封顶，~12× 比值系 SoA 固有，结构性修复归 Stage D 扁平 CSR 评估）,load 威胁模型声明（CRC 防 bit-rot 不防恶意，非对抗来源假设）,rename 原子替换 POSIX-only 口径；**文档勘误**:§7 `load(path)` 占位签名更正为 `load(path, metric, ef_search_default)`、§2 依赖句与 v1.5 修正块的矛盾正文残留清除、stage_spec "load 峰值 ≈2×" 改两段式口径;**P3** v1 golden bytes 格式钉（红 = 漂移必须升 FORMAT_VERSION)、往返断言位级化、自定义参数 cell |
| v1.15 | 2026-09-02 | Stage C 第四轮审查回流（用户外审，P2×2 + P3×1，以代码为准）:**P2-1** load 体积上限检查——`encoding::max_body_size` 由已验证 header 按第 11/12 条 cap 导出格式合法最大体积，超界（尾随垃圾/稀疏大文件）不读 body 即拒（[min, max] 区间不缩小合法接受集：encode 精确体积 + decode 本就拒尾随字节）;**P2-2** `level_count ≤ 64` 前移到 `decode_node_record` 解析点（原在 decode 完成后才检查，恶意 65–255 层记录可在拒绝前制造成倍嵌套 Vec 分配）——两道防线、一条规则、一个常量;**P3** prefix 读取错误分类（`UnexpectedEof` → Corrupted 截断；EISDIR/权限等 → Io，对齐 error.rs 声明）;文档勘误：§3 params 块与 §4.4 的 load/ef_search_default 语义按实际 API 更正（ef_search_default 进 HnswParams 图状态但不进快照、load 入参供给） |
| v1.16 | 2026-09-02 | Stage C 第五轮审查回流（用户外审，P1×1 + P2×1 + P3×1，以代码为准）:**P1** load 非普通文件闸门（`!metadata.is_file()` → `InvalidArgument`;FIFO/设备 metadata 长度不可信，原 `file_len - 29` 在 debug 下下溢 panic——违反"损坏输入不 panic"契约；长度差改 saturating_sub 作纵深）;**P2** 新增 `load_with_budget(path, metric, ef_search_default, max_bytes)`——调用方硬预算（读 body 前拒 + `take()` 封顶读，封死 metadata→read 的 TOCTOU 拉大窗口）,`load` 为其薄封装（不设预算、面向可信本地基准文件，威胁模型写明）;**P3** `encode_node_record` 补 `level_count ≤ 64`（公共 codec 写读对称——原 encode 接受 65 层而 decode 拒绝）;文档勘误：§4.4 不变式行按 v1.3 口径更正（逐查询只校验 `ef ≥ k`;`ef_search_default ≥ M` 是构造缺省约束）、stage_spec golden 钉为 174B（前三轮记录误写 336B)、save 复杂度表述改两遍线性 |
| v1.17 | 2026-09-02 | Stage C 第六轮审查回流（用户外审，P1×1 + P2×1 + P3×1，以代码为准）:**P1** 长度运算纯函数化全 saturating（读封顶路径仍有裸减法：陈旧 metadata / 预算 < 29B 可二次下溢）;**P2** §7 预算体系完善——`LoadBudget { max_file_bytes, max_memory_bytes }`：字节预算 ≠ 内存预算，物化放大由 `max_memory_estimate`（第 11/12 条 cap 导出的保守上界，按 64 层计费；1M gist 估 ~19.2GB ≈ 现实峰值 2.4×，宁严勿宽，虚高项注释写明）在 decode/物化前拦截;**P3** codec 写读对称最后一块：`encode_node_record` 补逐分量 `is_finite()`（原接受 NaN 而 decode 拒）——至此 encode/decode 校验面完全对齐 |
| v1.18 | 2026-09-02 | Stage C 第七轮审查回流（用户外审，口径/登记类×4，以代码为准）:并发测试的 `saves_in_flight` 采样前移到 load 返回 Ok 的瞬间、先于 `assert_identical`（"Ok 返回点仪表 > 0"的证明强度与措辞对齐）;§7 体积上限拆出 records-only 口径 `max_records_size`（= `max_body_size` − 25B header）——check 5 max 与 `read_cap` 此前跨口径比较，预读闸门松 25B（安全方向，CRC 兜底；修复后为精确上限，空图 + 1B 尾随即预读拒）;**NeighborSelection 不进 §3 快照格式**（`from_parts` 硬编码 Heuristic）补登残留——开放续插时 Simple 建的图会静默按 Heuristic 续插，与 metric 残留同类，续插定夺时一并裁决（格式升版 vs 显式拒绝）;save(目录) → Io 与 load(目录) → InvalidArgument 的分类不对称写入 save rustdoc（刻意：原子替换协议不预检目标，预检即 TOCTOU 谎言） |
