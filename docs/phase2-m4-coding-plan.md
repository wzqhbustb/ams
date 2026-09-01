# Phase 2 M4 编码顺序

> 基于 `docs/phase2-m4-tech-selection.md` v1.5（五轮对抗审查，修订见文末记录），按依赖
> 关系排列的 M4 阶段编码执行计划。M4 交付三块内容（ROADMAP.md:247-262，范围修正见
> 选型 §1）：**In-memory HNSW（插入/搜索/启发式）+ 距离函数三件套 + recall 基准
> harness（含快照加载 API）**。每个阶段必须先通过单元 / 属性 / 对拍测试与对抗性
> review，再进入下一阶段。
>
> ```
> 地基   → A (crate 骨架 + CI 注册 + 编码冻结 + 距离函数 + PRNG)   3–4 天
> 算法   → B (HNSW 核心:插入/搜索/启发式/收缩)                      4–6 天
> 序列化 → C (快照 save/load)                                        1–2 天（可与 B 后半并行）
> 基准   → D (recall harness + 1M 验收 + benchmarks 落盘)            3–4 天
> 收口   → E (覆盖率 + criterion + 审查 + 归档 + 出口 tag)           2–3 天
> ```
>
> **总计 5 个 stage，串行口径 13–19 天；C 与 B 并行后实际工期约 2.5–3 周**
> （1 名高级 Rust 工程师）。

---

## v1.0 硬约束速查

M4 开工前请通读 tech-selection §3 / §4 / §5 / §8，以下 9 条为**编码期每天都要对照**
的硬性约束（违反则退回该 stage 重做）。全部是选型文档四轮审查钉死的冻结契约，
引用的 v1.x 为该条定稿的版本：

- **确定性三前提（§4.1)**:① 构造期与查询侧全部排序键 =
  `(distance, NodeId 升序)`（候选堆、选中集、输出同规；v1.3"实现前提"①);②
  **全程禁止依赖 HashMap/HashSet 迭代序**（visited 用 bitset 或 BTreeSet;
  v1.3"实现前提"②);③ PRNG 显式 seed、按图实例持有（§4.1"确定性要求"段，
  v1.0 即有）+ **PRNG 状态不进快照**（v1.3"实现前提"③,Stage C 按保守默认
  处理）。三者缺一，"同 seed → 字节级同构图"即破，§9 全部可复现性
  随之失效。
- **层级生成口径（§4.1 v1.2）**:`u = (r >> 11) as f64 * 2⁻⁵³`（高 53 位）;
  `u == 0.0` **redraw 而非断言**（合法抽样，2⁻⁵³);`m_L = 1/ln(M)`;level
  上界断言只防真不变量违例（redraw 后 M=16 下 level ≤ 13)。
- **冻结格式（§3 v1.2/v1.3）**:`dim` 只在快照头；**位置即身份**（第 i 条
  node 记录 = `NodeId(i)`，无显式字段）;`level_count = 层数 = 最高层号 + 1`;
  空图 `entry_point = u32::MAX`;`flags`/`reserved` 恒 0(load 遇非 0 响亮报错
  );快照只带构造期三参数（`m`/`m_max0`/`ef_construction`),`ef_search_default`
  不进快照；**CRC32 前缀**（`crc32(4B) + body`，对齐 checkpoint.rs 惯例）。
- **NodeId 稳定契约（§3）**：稠密递增、永不复用、快照往返稳定——M5 的 WAL
  记录与节点页寻址以此为键；M4 不允许 compact/relabel;M6 删除只能是
  tombstone-in-place（§11 O3 已定界）。
- **邻居启发式（§4.3 v1.3）**：论文 Algorithm 4,`extend_candidates = false`
  **全层**（无任何第 0 层例外——v1.0 的错误表述已废弃）、`keep_pruned = true`
  （自证理由：满 M 边利好连通性与低密度 recall)；**选择 + 收缩两侧都走启发式**
  （只选择不收缩 = 邻居表无界增长）。hnswlib 是无开关固定启发式，不可引用为
  我们开关组合的依据。
- **距离函数（§5 v1.3）**:L2 用**平方**距离不开方；Cosine 零向量响亮报错；
  IP 取负;**f32 元素 + f64 累加器标量循环**(f64 累加不可重结合——这正是
  跨平台确定性的机制，禁止加 fast-math 类优化）;**insert/search 入口拒绝
  NaN 分量与 dim=0**（对拍抓不到的静默劣化，入口报错是唯一防线）;load 侧
  同口径拒绝 NaN(v1.4 闭环）。
- **参数与校验（§4.2/§4.4,v1.3 消歧）**:`M=16 / M_max0=2M / ef_construction=200 /
  ef_search_default=64`；构造校验三条：`M ≥ 2`、`ef_construction ≥ M`、
  **`ef_search_default ≥ M`**（约束构造参数缺省，64≥16 ✓——不是逐查询 ef
  下限）；查询校验只有 `ef ≥ k`（逐查询 ef 允许 < M）。参数经 `HnswParams`
  实例化，禁止运行时魔数。
- **依赖（§10 v1.3 + §2 v1.5 修正）**：零新运行时依赖（workspace 图无新增
  crate);**M4 直依赖只有 `thiserror` + `crc32fast`**（均已在 workspace 图
  内）;`pg-storage` 依赖**缓至 M5**——选型 §2 的"类型与错误"在 M4 的全部
  交付物中无消费者，照原样加是空挂死依赖（M3 O4 刚清除并立规的对象）;
  PRNG 手写 xoshiro256**（不引 rand）;dev-dependency 不新增 workspace 外
  crate(criterion 沿用 0.5)。
- **TOAST 决策（2026-08-31 Phase 2 前置决策，v1.5 补录）**：**M4/M5 不依赖
  heap TOAST**——向量自存于 `pg-am-hnsw` 自有节点存储（M4 内存连续 arena,
  M5 节点页布局）,`pg-am-hnsw` 与 `pg-am-heap` 零依赖，禁止为取向量回堆。
  M2 选型 §四"HNSW 存 4KB 向量必须走 TOAST"的假设已被本设计取代（该文档
  已加 supersede 注）。全链路口径：VECTOR(n) 堆内列值内联存储、dim ≤ 2000
  上限（与 M5 单页节点可行域对齐）,**超限响亮报错**；超 2000 维节点页溢出
  方案维持 O1 → M5;heap TOAST chunk I/O 本体（大文本/JSONB/超维向量）归
  **Phase 4a**，非 Phase 2 任何 milestone 的前置。完整记录见 ROADMAP.md
  "Phase 2 · TOAST 决策"。
- **验收口径（§8.2/§12）**:recall 真值 = corpus ivecs **原序前 10，不重排**;
  CI 硬门槛 siftsmall recall@10 ≥ 98% **@ ef_search=64**;sift/gist 全量 1M
  归手动/nightly 落盘 benchmark 文档；**flaky 零容忍**（确定性 seed 下没有
  flaky 借口）。

---

## 工程规则（每 stage 通用，继承 Phase 1 惯例，M4 适配）

- **每 stage 一个 commit**:message 前缀 `PHASE2-M4-StageX`;**未经用户确认不执行
  任何 git mutation**;author 固定 `wangzq23 <wy823034583@gmail.com>`。出口 tag
  (`phase2-m4`）只在用户确认后打。落库走分支 + PR(Phase 1 收口期确立的流程）。
- **stage_spec 归档**：每 stage 收口时在 `docs/stage_spec.md` 追加该 stage 的
  "交付内容 / 与 PG(pgvector）的 trade-off / 已知残留与后续归队"三小节。
- **对抗性 review**：每 stage 完成后一轮对抗审查（P1 必修、P2 登记）;M3 的文档
  四轮审查与 Stage G 的 slot-0 根治证明该门槛的价值，M4 维持。
- **M4 的验证形态映射**(Phase 1 手段的替代，选型 §9):loom → **属性测试四件套
  + 暴力对拍**；崩溃注入 → **快照往返等价**;watchdog 纪律保留（一切可能阻塞的
  测试带硬超时——M4 的阻塞面只有文件 I/O 与大规模建图）。
- **回归传承**：每 stage 出口 `cargo test --workspace` 全绿（M4 不动既有 crate,
  回归应保持零扰动；若不得不动，本 stage 说明理由并跑全量）;`pg-am-hnsw` 自身
  测试随 stage 递增。release 全量在 Stage E 强制。
- **冲突处理**：实现与 tech-selection 引用不符时，以代码为准并回改选型文档；
  决策层冲突不擅改，记入"开放问题与冲突标注"。

---

## 阶段 A：crate 骨架 + CI 注册 + 编码冻结 + 距离函数 + PRNG(3–4 天；+A1 回收页撕页修复约 0.5–1 天，v1.6 入 scope）

**归属**:M4 地基
**前置**:Phase 1 收口（`phase1-m3` tag 已打）;rustdoc/C2 审计 PR **已提待
merge(v1.3 修正——初稿误写"已 merge")——开工前先 merge；注意 M4 两份文档
当前 untracked 在审计分支的工作区，归属方案：审计 PR 只含 rustdoc + stage_spec
归档，M4 文档单独开第二个 PR（从 merge 后的 main 切出）**
**目标**：新 crate 落地且 CI 零盲区；§3 冻结格式的编解码原语、§5 距离函数、
§4.1 PRNG 三个"后续一切的地基"组件连同其已知答案测试一次到位。

| 任务 | 交付物 |
|------|--------|
| crate 骨架 | `crates/pg-am-hnsw/`:`Cargo.toml`(**直依赖只有 `thiserror` + `crc32fast`,pg-storage 缓至 M5——选型 §2 v1.5**)、`lib.rs`（模块布局：`error` / `params` / `encoding` / `distance` / `rng` / `graph` / `snapshot`)、`NodeId(u32)` newtype(`NodeId::INVALID = u32::MAX`)、`HnswParams { m, m_max0, ef_construction, ef_search_default }` + 构造校验（含 `ef_search_default ≥ M`)、`HnswError`(thiserror 沿用既有惯例） |
| **CI 注册五件事（§8.2 v1.3，一次做全）** | ① workspace 根 `Cargo.toml` `members` 加 `crates/pg-am-hnsw`;② ci.yml **三个** crate matrix(clippy/test/doc——fmt 单 job 无 matrix）加 crate;③ loom 豁免分支归类（M4 无 loom 模型，走非 loom 分支）;④ 核对 grep 护栏分支（sync-alias / Snapshot 构造等）对新 crate 的适用性（预期零命中，写明核对结论；**msrv job 为 workspace 级 `cargo check --workspace --all-features`,members 注册即自动覆盖，无 per-crate 注册点**——v1.1 补注;**具名 hazard(v1.3)**：快照模块的类型命名必须避开 `Snapshot`——CI 的 Snapshot 构造护栏 grep 覆盖 pg-txn 外全部 crates,`Snapshot {` 字面构造与 `impl Snapshot` 都会误伤，`SnapshotHeader`/`SnapshotFile` 等复合名安全）;⑤ coverage job **新建**（tarpaulin,Linux-only runner,artifact 上传——本机 macOS 不可跑，Stage E 的覆盖率判定以 CI 报告为准；**此 plumbing 单独可吃半天，已计入工期上修**) |
| 编码原语（§3 冻结） | `encoding.rs`:node 记录 encode/decode(`flags:u8 | reserved:u8 | vector:f32[dim] | level_count:u8 | per-level {count:u16 | NodeId 列表}`)、快照头定宽编解码（magic/format_version/dim/m/m_max0/ef_construction/node_count/entry_point/max_level)、CRC32 前缀封装（复用 crc32fast);**load 校验清单一次写全**:magic/version、参数校验重跑、`entry_point < node_count`（空图哨兵）、`max_level` == 入口节点最高层、`level_count == levels+1` 恒等式、NodeId 稠密、邻接端点存在、flags/reserved 为 0、**NaN 分量拒绝** |
| 距离函数（§5) | `distance.rs`:`l2_squared` / `cosine` / `negative_inner_product`,f32 元素 + f64 累加标量循环；已知答案测试（手工算的三维/四维向量组，含 Cosine 零向量报错、NaN 拒绝、dim=0 拒绝）+ 与 f64 参考实现的 1e-12 容差对拍——**对拍输入为 f32 值**（随机 f64 转 f32 会引入 ~1e-7 表示误差打穿容差，v1.3 钉死）；该测试的真实价值 = 证明"累加器没被意外写成 f32"(960 维下 f32 累加误差 ~1e-5 ≫ 1e-12，一抓一个准） |
| PRNG(§4.1) | `rng.rs`:xoshiro256\*\*（公开测试向量做已知答案测试）、`next_level(m)` 封装（高 53 位转换 + `u==0.0` redraw + level 上界断言）;**确定性测试**：同 seed 两实例产同一 level 序列 |
| **A1 回收页撕页修复（pg-storage,2026-08-31 用户决策入 M4 scope,ROADMAP 附录 A1)** | ✅ **已完成（2026-08-31）**：审计确认唯一未覆盖消费者为 btree 在线 split 右页，`split_prepare_on_guards` 单一收口点补 `log_page_init`（post-image FPI；`new_page` 统一处理因 FPI 双门控时序被论证否决，理由见 `buffer_pool.rs:424` 注释）；红→绿测试两枚（`btree_split_crash.rs`：复用右页 FPI 先于 Prepare + 手工撕页恢复），全量回归全绿。ROADMAP 附录 A1 已划销。~~原任务描述：freelist 复用页不再享受"新页无旧镜像"假设……~~ |

**关键约束**：
- 本 stage 交付的编码/距离/PRNG 是 B/C/D 的全部地基，**冻结契约逐条对应硬约束
  速查表**;decode 侧校验清单缺任何一条都算本 stage 未完成
- CI 五件事是新 crate 的"出生证明"(M3 pg-wire 教训），缺一项即 red

**验收标准**：
- `cargo test -p pg-am-hnsw` 全绿（编码往返、坏字节流逐条响亮报错、距离已知
  答案 + 容差对拍、PRNG 测试向量 + redraw 行为）
- CI 全 job 绿且新 crate 在 clippy/test/doc 三 matrix 中实际执行（检查 CI 日志
  而非只看绿勾——M3 的教训是"绿但没跑")
- `cargo doc -p pg-am-hnsw` 零警告（rustdoc 纪律从第一天守住，不欠账）

**验收命令**：
```bash
cargo test -p pg-am-hnsw
RUSTDOCFLAGS="-D warnings" cargo doc -p pg-am-hnsw --no-deps
# CI 矩阵实际执行核对:push 后查 Actions 日志中 pg-am-hnsw 的 clippy/test/doc 三条
```

---

## 阶段 B:HNSW 核心算法（4–6 天）

**归属**:M4 算法主体
**前置**:Stage A
**目标**：论文 Algorithm 1/2/4/5 的完整忠实实现 + 属性测试四件套 + 合成数据
对拍全绿。这是 M4 的"心脏 stage",review 权重最高。

| 任务 | 交付物 |
|------|--------|
| 图结构与层级生成 | `graph.rs`:§6 SoA 布局（`vectors` 连续 arena / `levels` / `adjacency` / `entry_point` / `max_level`);`insert` 入口：NodeId 分配（稠密递增）、层级抽签（A 的 `next_level`)、空图首节点成入口 |
| 搜索（Algorithm 2/5) | 逐层贪心下降 + 第 0 层 ef 束搜索；候选堆与结果堆排序键 = `(distance, NodeId)`;`search(query, k, ef: Option<usize>)`；不变式钉死（v1.3):**逐查询只校验 `ef ≥ k`**（允许 ef < M);`ef_search ≥ M` 约束的是构造参数缺省（`ef_search_default ≥ M` 已在 Stage A 的 HnswParams 校验），两条互不混用；visited 用 bitset（硬约束②) |
| 插入（Algorithm 1) | 各层邻居选择 + 双边连接 + 超限时**收缩**(shrink）同样走启发式（硬约束"两侧")；首层入口点/最大层更新 |
| 邻居选择启发式（Algorithm 4) | `extend_candidates = false` 全层 + `keep_pruned = true`;**simple（取最近 M）对照路径**并存（编译期/构造参数开关，不进公开 API,§4.3 A/B 实验用） |
| 已知小图逐步测试 | 手工构造的 5–10 节点插入序列：每步后的邻接表与论文手工推演逐步对拍（算法忠实度的最直接证据） |
| 属性测试四件套（§9) | ① 全节点入口可达（BFS on 第 0 层）;② 双向边不对称率统计（口径建立，M6 验收复用）;③ 层分布与几何期望卡方拟合（固定 seed 确定性成立；**尾部层面期望频数 < 5,必须合并尾箱再做卡方**——v1.1 补充，不合箱的检验在统计上无效）;④ ef 单调性（合成数据）——**断言粒度为聚合均值不降，不是逐查询不降**(v1.1 修正：beam 搜索的遍历集随 ef 变化，单查询 recall 允许偶发回落，逐查询硬断言是"确定性但设计错误的 flaky") |
| 合成数据对拍（§8.3) | 随机高斯/均匀混合数据（dim ∈ {2, 16, 128},N ∈ {1k, 10k}),`ef = 节点数` 结果与暴力扫描**完全一致**；连通性前提失败的诊断路径（先查连通性再查距离）写入测试注释 |

**关键约束**：
- 确定性三前提是本 stage 每一行代码的背景约束：评审时专门过一遍"有没有
  HashMap 迭代序进入算法路径"
- `keep_pruned` 凑满 M 的实现：被遮挡候选按距离升序补位至 M 或候选耗尽
- 收缩侧复用选择侧的同一启发式函数（入参角色不同），禁止写两份

**验收标准**：
- 已知小图逐步对拍全绿；属性测试四件套全绿（多 seed × 多 dim × 多 N 矩阵）
- 合成对拍全等（含 gist 量级的 960 维合成集冒烟）
- simple vs heuristic 的 A/B 开关可编译可运行（数据在 Stage D 采）

**验收命令**：
```bash
cargo test -p pg-am-hnsw
cargo test -p pg-am-hnsw --release   # 大图属性测试的耗时口径
```

---

## 阶段 C：快照序列化 + 加载（1–2 天，可与 B 后半并行）

**归属**:M4 序列化
**前置**:Stage A（编码原语）;B 的图结构 API 冻结后接口不再动
**目标**：§7 的 `save(path)` / `load(path)` 落地；往返等价成为 M4 的"崩溃注入
等价物"。

| 任务 | 交付物 |
|------|--------|
| save/load | `snapshot.rs`：图 → §3 字节流（头 + node 记录流，CRC32 前缀）;load 全量校验（A 的清单）后重建 SoA 图 |
| 往返等价测试（§9) | `save → load → search` 与内存原图逐查询结果全等（合成数据集 × 三度量 × 多 seed);空图、单节点图、多层图边界形态各一用例 |
| 损坏文件测试 | 位翻转（CRC 检出）、截断、伪造 header(magic/version/参数越界/entry_point 越界/flags 非 0/NaN 注入）逐条响亮报错 |
| PRNG 状态声明 | load 后图的续插语义**不在本 plan 现场发明**(v1.1 修正——选型 §4.1③ 只说"快照用于基准加载，不用于续建","load 后 insert 可用 + 新 seed"是超出口径的语义扩展）：本 stage 按**保守默认**实现——load 返回的图 `insert` 直接报错（`HnswError::InvalidOperation`)，续插能力若需要，回推选型文档升 v1.6 定语义（新 seed 续插 vs 显式拒绝）后再开放。该冲突已记入"开放问题与冲突标注" |

**关键约束**：
- 快照是 M5 前的唯一持久化形态，格式字节序/宽度在本 stage 实际写盘后**冻结生效**，
  之后再改 = 格式修订（过修订记录）
- load 校验必须快（1M 节点 < 5 分钟验收口径含校验，§12)

**验收命令**：
```bash
cargo test -p pg-am-hnsw --test snapshot_roundtrip
```

---

## 阶段 D:recall harness + 1M 验收 + benchmarks 落盘（3–4 天）

**归属**:M4 验收主体
**前置**:B + C
**目标**：全部 §12 数字产出并落盘；CI 硬门槛（siftsmall）成为常驻防线。

| 任务 | 交付物 |
|------|--------|
| **ftp 连通性实测（最先做，阻塞 CI 下载方案）** | 在 GitHub Actions runner 上实测 `ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz` 连通性（一个一次性 workflow 或 ci 内试跑）;通则方案 1(ftp 直连 + actions/cache 缓存数据集目录，key = 数据集名+大小）,不通则转方案 2（一次性取回 → GitHub release 自托管）。结论与实测记录进 benchmark 文档 |
| 数据集工具链 | `scripts/fetch_datasets.sh`（按 D-1 结论实现）+ fvecs/ivecs 手写解析器（含坏文件报错）;`datasets/` 入 .gitignore |
| recall probe | `examples/m4_recall_probe.rs`：参数（数据集路径/ef/k/seed/度量/开关）从环境变量读，输出 recall@10、P50/P99 延迟、建图/加载时间（对齐 m3_wal_bytes_probe 先例：测量工具非测试，无断言） |
| CI 硬门槛测试 | `tests/recall_siftsmall.rs`:siftsmall 建图（M=16/efC=200/seed 固定）→ recall@10 ≥ 98% @ ef=64；数据集缺失时的行为分环境钉死（v1.3 修正——`CI=true` 判定会自爆：GitHub Actions 在**所有** runner 上设 `CI=true`，常规 matrix job（无数据集 fetch）会编译并运行该测试 → 每 matrix job 必红）:**硬失败触发条件只保留 `M4_REQUIRE_DATASET=1`**——仅由带 fetch 的专用门槛步骤显式设置，门槛步骤不可能静默跳过（防"绿但没跑"的性质不变）,matrix job 的跳过无害。**运行档位**：专用步骤跑 `--release`(debug 下 10k × 128d 建图耗时未实测，不留超时悬案；debug 实测值随 benchmark 文档落盘） |
| **CI 数据管道手术（v1.3 新增显式交付物——对齐"CI 注册五件事"的制度化先例）** | ci.yml 新增带勾选清单的专项任务：① 数据集 fetch 步骤（ftp 方案 1 或自托管方案 2，按 D-1 实测结论）;② `actions/cache` 配置——cache key = 数据集名 + 字节数（内容寻址不了就用版本号，写明选择）,path = `datasets/`;③ 独立 release 门槛步骤的**归属 job**（挂在哪个 job 里还是新建 `recall-gate` job，写明）;④ `M4_REQUIRE_DATASET=1` 的设置点（步骤级 env，不外泄到其他 job);⑤ 预期零命中核对：既有 grep 护栏不受新增步骤影响。五项缺一件，门槛测试的"常驻防线"就不成立 |
| 1M 验收跑批 | sift + gist 全量：recall@10、P99(ef=64)、建图时间、加载时间；**任一数据集**若不达标按 §11 R1 预案上调参数（如 gist M=32）并如实记录"分数据集参数"(v1.3 措辞修正：sift 1M @ ef=64 的 95% 同为贴线数字，fallback 不限 gist);simple vs heuristic A/B 对照数据随跑批产出（§4.3 参数冻结证据） |
| benchmarks 落盘 | `docs/phase2-m4-benchmarks.md`：机器规格、全部数字、A/B 证据、ftp 实测结论、与 hnswlib/pgvector 的口径对齐说明（M6 正式对标的预备） |

**关键约束**：
- 真值口径：ivecs 原序前 10，不重排（硬约束）；我们的返回排序按
  `(distance, NodeId)` 决胜
- 1M gist 内存门槛 ~4 GB（§11 R3)，跑批机器规格写入文档
- 跑批全程用 release；数字必须可复现（命令 + 环境变量随文档落盘）

**验收命令**：
```bash
# CI 硬门槛
cargo test -p pg-am-hnsw --release --test recall_siftsmall
# 1M 验收(手动/nightly)
DATASET=datasets/sift cargo run -p pg-am-hnsw --release --example m4_recall_probe
DATASET=datasets/gist cargo run -p pg-am-hnsw --release --example m4_recall_probe
```

---

## 阶段 E:M4 收口（2–3 天）

**归属**:M4 出口
**前置**:D
**目标**：覆盖率达标、性能防回退设施就位、对抗审查两轮完成、文档归档、出口
tag。

| 任务 | 交付物 |
|------|--------|
| 覆盖率 ≥90% | Stage A 建的 coverage job 产出报告；不足 90% 的模块补测试（编码 decode 分支、错误路径、启发式边角是常见洼地）;**判定以 CI(Linux）报告为准** |
| criterion bench | 合成小数据集（10k × 128d）建图/查询吞吐 bench,`[[bench]]` 注册，防性能回退（对齐 pg-storage bench 先例） |
| 对抗审查 ×2 | 第一轮设计+实现审查（P1/P2/P3 分级）→ 修复 → 第二轮复核；**重点审查面**:Algorithm **1/2/4/5** 与论文的逐行对应（v1.3 补 2/5——搜索与插入同为算法核心，初审只列 1/4 是盲区）、确定性三前提的代码级落实、编码校验清单完备性 |
| stage_spec 归档 | Stage A–E(M4）各节：交付内容 / 与 pgvector·hnswlib 的 trade-off / 已知残留 |
| 全量回归 + release | `cargo test --workspace` debug + release 全绿（含 M1–M3 全部传承）;clippy/fmt/doc 三档绿 |
| 出口 tag | `phase2-m4`(**经用户确认后**打；分支 + PR 落库，tag 打在 main 的 merge commit 上——对齐 `phase1-m3` 先例) |

**验收命令**：
```bash
cargo test --workspace && cargo test --workspace --release
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test -p pg-am-hnsw --release --test recall_siftsmall
```

---

## 总时间估算

| 阶段 | 预估 | 依赖 |
|------|------|------|
| A 地基 | 3–4 天（v1.3 上修：coverage job plumbing 单独可吃半天） | 无 |
| B 算法核心 | 4–6 天 | A |
| C 快照 | 1–2 天 | A(B 后半并行） |
| D harness + 验收 | 3–4 天 | B + C |
| E 收口 | 2–3 天 | D |
| **串行** | **13–19 天** | C 并行后 **约 2.5–3 周** |

风险余量：gist 参数上调（R1）与 ftp 方案切换各预留 0.5–1 天，含在 D 的区间内。

## 依赖关系图

```
A (地基/CI/编码/距离/PRNG)
 ├── B (HNSW 核心) ────────┐
 └── C (快照,与 B 后半并行) ┤
                            ▼
                     D (harness + 1M 验收)
                            ▼
                     E (收口 + tag)
```

## 回归测试传承

- M4 全程不修改既有 7 个 crate 的 src（预期零扰动）;workspace 全量回归每 stage
  出口必跑，任何红都算本 stage 引入
- `pg-am-hnsw` 测试清单按 stage 累加：A（编码/距离/PRNG)→ B（算法单测 + 属性
  四件套 + 合成对拍）→ C（往返等价 + 损坏文件）→ D(siftsmall 硬门槛）
- M1–M3 的 loom / crash rounds / stress 等重测试不在 M4 常规出口内，Stage E
  收口轮跑一次全量（对齐 M3 Stage G 口径）

## 开放问题与冲突标注

- **O1–O4 归期**（选型 §11):O1/O2 → M5,O3 → M6,O4 → M4 末尾或 Phase 7b;
  coding 期不提前做
- **load 后续插语义（v1.1 登记）**：选型 §4.1③ 的口径是"快照用于基准加载，
  不用于续建",plan Stage C 初稿曾现场扩展为"load 后可续插 + 新 seed"——
  按工程规则不擅改决策层，已退回保守默认（load 后 insert 报错）。若 M4 后期
  需要续插能力，选型文档升 v1.6 定语义后开放
- **R1 gist 参数**:D 的跑批结果决定；上调即记录"分数据集参数"，口径诚实优先
- **ROADMAP 口径校订两处**（选型 §1/§8.1 已留痕）:VECTOR(n) DDL 归 M6;
  "1M 768d" 以数据集名为准。实现期若发现 ROADMAP 与选型新冲突，先入此节再动手

## 遗留与归队

- SIMD 距离优化（过 recall 门禁后）→ M4 末尾优化项或 Phase 7b
- Cosine/IP 的 recall 质量基准（glove-angular 等）→ M6
- VECTOR(n) DDL / `<=>` 操作符 / executor 贯通 → M6 SQL stage
- f16/bf16 存储编码、超 2000 维节点页方案 → M5
- heap TOAST chunk I/O（大文本/JSONB/超维向量的堆内溢出存储）→ **Phase 4a**
  （2026-08-31 决策：非 Phase 2 前置；M4/M5/M6 均不依赖，见硬约束速查 TOAST 条）

## review 修订记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v1.0 | 2026-08-31 | 初版，基于 tech-selection v1.4 |
| v1.1 | 2026-08-31 | 第一轮对抗审查修复（P2×2 + P3×3 + nano×3):Stage D 数据集缺失行为分环境钉死（CI 硬失败，消除"绿但没跑"复辟）+ CI 门槛运行档位钉 release 独立步骤；Stage B 属性④ ef 单调性改聚合均值断言（逐查询硬断言 = 设计错误的 flaky)、属性③ 补卡方尾箱合并规则；Stage C load 续插语义退回保守默认（insert 报错），决策回推选型 v1.5；补注 msrv job 为 workspace 级自动覆盖；gist 验收命令补全；出口 tag 明确打在 main merge commit 上（phase1-m3 先例） |
| v1.2 | 2026-08-31 | nano:硬约束速查"确定性三前提"的版本归属修正——③ 拆为"显式 seed(v1.0 确定性要求段）+ PRNG 状态不进快照（v1.3 实现前提③)",消除 plan 内"§4.1③"的双义引用（Stage C 引用的是后者）；后续 Stage 代码注释与 M5 追溯依赖该编号精确性 |
| v1.3 | 2026-08-31 | 第二轮对抗审查修复（P2×2 + P3×3 + nano×5):**P2-1** Stage D 硬失败触发条件去掉 `CI=true`（GitHub 全 runner 设 CI=true，常规 matrix job 会编译运行该测试 → 自爆），只保留 `M4_REQUIRE_DATASET=1` 由专用门槛步骤显式设置；**P2-2** 依赖落地修正——M4 直依赖只有 {thiserror, crc32fast},pg-storage 缓至 M5（照 §2 原表述加 = 空挂死依赖，M3 O4 立规对象），已回推选型 §2 升 v1.5;**P3-1** Stage A 前置更正（审计 PR 已提待 merge，非"已 merge";M4 文档归属 = 独立第二个 PR);**P3-2** `ef_search ≥ M` 钉死为构造参数缺省约束，逐查询只校验 `ef ≥ k`;**P3-3** Stage D 的 CI 数据管道手术升为带勾选清单的显式交付物（cache key/步骤归属/env 设置点/护栏核对）;nano:① Snapshot 命名具名 hazard(CI 护栏 grep 会误伤）入 CI 任务④;② 距离容差对拍钉 f32 输入（测试真实价值 = 证明累加器非 f32);③ Stage E 审查面补 Algorithm 2/5;④ R1 fallback 改"任一数据集不达标";⑤ Stage A 工期上修 3–4 天（coverage plumbing 半天），总估 13–19 天 / 约 2.5–3 周 |
| v1.4 | 2026-08-31 | 第三轮审查修复（P3×1 + nano×2):§10/§2 矛盾消解——tech-selection §10 依赖口径澄清段按 v1.5 改写（传递依赖取舍已随依赖缓期消失，冻结文档两节直接相反）;plan 头部引用升 tech-selection v1.5;两处"续插回推升 v1.5"改指 v1.6(v1.5 已被依赖修正消费，活指令版本号不可悬空） |
| v1.5 | 2026-08-31 | Phase 2 前置 TOAST 决策补录（非对抗审查轮，用户决策）:M4/M5 不依赖 heap TOAST（向量自存节点存储、与 pg-am-heap 零依赖）;VECTOR(n) 堆内列值内联、dim ≤ 2000 上限；heap TOAST chunk I/O 归 Phase 4a——硬约束速查新增 TOAST 条、遗留与归队登记 Phase 4a 归属；同步 ROADMAP.md"Phase 2 · TOAST 决策"与 M2 选型 §四 supersede 注 |
| v1.6 | 2026-08-31 | A1 回收页撕页修复入 M4 scope（用户决策，自 Phase 7a 加固专项提前）:Stage A 任务表新增 pg-storage 修复行（含动既有 crate 的理由与全量回归要求）;ROADMAP 附录 A1 归属同步改为 Phase 2 M4、7a 加固专项该项划销 |

## 第一周做什么

1. Stage A 全部（crate 骨架 + CI 五件事 + 编码 + 距离 + PRNG)——第二天结束
   前 CI 三 matrix 里必须看到 pg-am-hnsw 实际执行
2. Stage B 开工：图结构 + 层级生成 + 搜索（Algorithm 2/5)
3. 第一天就发起 ftp 连通性实测（一次性 workflow)，不阻塞 A/B，但结论影响 D
