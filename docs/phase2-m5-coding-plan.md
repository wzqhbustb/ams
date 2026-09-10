# Phase 2 M5 编码顺序

> 基于 `docs/phase2-m5-tech-selection.md` **v1.12**(十一轮对抗审查，修订见该文档
> 文末记录；本计划一切阶段任务、交付物、验收命令均可回溯到其 § 节，行内以
> 选型 §x.y 引用），按依赖关系排列的 M5 阶段编码执行计划。M5 交付四块内容
> (ROADMAP.md:264-278，范围切分见选型 §1):**HNSW 节点页布局 + WAL 记录与
> redo(生理路线)+ HNSW meta page + 崩溃恢复验收(kill -9 ×1000、恢复 <30s)**。
> 每个阶段必须先通过单元/集成测试与对抗性 review，再进入下一阶段。
>
> ```
> 基建   → 0 (WAL 判别值三件套 + 页初始化链 + CI 注册)          2–3 天
> 原语   → A (访问器收口 + 物理应用原语 ×7)                       3–4 天
> 布局   → B (节点页/目录链/meta page/创建协议)                   4–5 天
> 恢复   → C (WAL 记录 + redo handler ×7 + 正常写入路径)          4–5 天
> 崩溃   → D (forget 窗口矩阵 + SIGKILL 轮次 + §11.3 审计)        4–5 天
> 收口   → E (30s 实测 + benchmarks 落盘 + 归档 + 出口 tag)       3–4 天
> ```
>
> **总计 6 个 stage，串行口径 20–26 天（1 名高级 Rust 工程师）；选型已定稿
> 12 版，本计划不含设计返工余量，工艺风险集中在 C/D 两阶段**

---

## v1.0 硬约束速查

M5 开工前请通读 tech-selection §3/§4/§7/§8/§10。以下 9 条为**编码期每天都要
对照**的硬性约束（违反则退回该 stage 重做），全部是选型文档十轮审查钉死的
冻结契约，引用格式 = 选型 §节：

- **WAL 总路线（§3)**：生理记录 on 节点页为主线，M4 快照文件只做逻辑归档/
  迁移/调试通道——禁止在现场发明逻辑重放路径；(b) 已被 30s 恢复上限
  **估算否决**(1M 逻辑重放线性下界 ≈ 420s、对数修正后 ≈ 630–850s，选型
  §3 口径为外推估算而非实测，v1.1 审查 P3-4 措辞修正）。
- **单页记录纪律（§4.2/§8.3 不变量 4)**:HNSW 的 7 种 WAL 记录
  (121–127,HnswNodeInit/SetNeighbors/MetaUpdate/NodeTombstone/DirAppend/
  DirLink/PublishLive）全部是**真单页记录**,payload 自包含全部目标页
  (§4.2 自包含规则）;redo 幂等锚唯一 = pd_lsn（按页判定）。跨页原子
  不可得是设计前提（btree 三步协议的反面教材），引入任何两页写 =
  协议修订，必须回推选型升版。
- **redo 校验可求值性约束（§10.1,v1.10)**:redo 只保留**同页/同记录可
  判定**的校验；凡依赖链导出 HWM 或目录映射的校验一律归 §11.3 的 open
  后审计（目录链解析在 redo 路径上不可廉价求值——恢复窗口 10 万条 ×
  1231 页链遍历 = 亿次级页读，直接爆 30s 预算)。**跨页可变状态只断言
  取值集合、不断言单值**(v1.11 第二条跨页纪律：节点页可携 LIVE 先于
  目录页落盘，DirAppend 的 state 校验 ∈ {INITIALIZING, LIVE})。
- **八步写入序（§8.1)**:PageAlloc/初始化链 → DirLink → NodeInit
  (INITIALIZING、槽先占用）→ DirAppend（映射发布 = id 分配）→
  SetNeighbors（邻居页）→ SetNeighbors（自身页）→ MetaUpdate（空图或
  level > max_level，先于 PublishLive)→ PublishLive。次序即协议，任何
  重排都要重过 §8.2 崩溃窗口表。**meta 更新永不指向空列表节点**
  （入口点只指向自身邻接已写完的节点）。
- **未发布即未分配（§8.1④/§8.3 不变量 2)**:NodeId = 目录链上序位；
  高水位不落字段，`HWM = tail.ordinal × 813 + tail.count`(§7.1 格式常量
  导出）；未发布 NodeId 的复用合法（从未可观察）,M4 §3 "never reused"
  约束的是**已发布**可寻址身份。
- **页格式冻结（§7.1/§7.2)**：节点条目**定长分档**（按抽取 level 预留各层
  满载容量，SetNeighbors 只原位改 count+内容，条目永不扩搬）;1B 状态位
  布局 = top_level:6 + state:1 + tombstone:1(6bit 恰好放下快照
  MAX_LEVEL_COUNT=64 的同一上限）;dim/m/m_max0 乘积联动创建时硬校验，
  默认参数 8KB 页下 dim ≤ 1791;目录页 = 32B PageHeader + 24B 自描述头，
  条目 10B(PageId u64 全宽不截断）,813 条目/页——以上全是格式常量，
  改动即格式修订。
- **成功边界与可见性（§8.1 成功边界）**:insert 耐久边界 = `flush_to`
  （本 insert 全部记录的最后 LSN)；未返回成功的 insert 恢复后**允许可见**
  (txn_id 恒 INVALID、无 undo——可见性 = 记录持久性前缀，M5 无事务性
  DML);loser 窗口断言 = redo 流内发现带有效 txn_id 的 HNSW 记录即硬失败
  (§11.3④)。
- **恢复预算（§9.2/§13.2)**:redo 起点恒为 checkpoint 点；恢复 <30s **只对
  checkpoint 后的增量窗口成立**;**bulk load 完成后必须立即 checkpoint**
  （硬要求——1M 建库 ≈ 2.9GB WAL（v1.12)，首 checkpoint 前崩溃 = 重放全量）。
- **回归传承（§11.5)**:M4 的 139 枚测试（88 lib + 3 bruteforce +
  5 properties + 3 recall_siftsmall + 40 snapshot_roundtrip)+ bench smoke
  全程全绿；M4 recall 门槛（siftsmall recall@10 ≥ 0.98 @ ef=64）对页驻图
  同样成立；M4 快照 v1 格式零改动（golden bytes 钉继续绿）。

---

## 工程规则（每 stage 通用，继承 Phase 1/M4 惯例，M5 适配）

- **每 stage 一个 commit**:message 前缀 `PHASE2-M5-StageX`;**未经用户确认不
  执行任何 git mutation**;author 固定 `wangzq23 <wy823034583@gmail.com>`。
  出口 tag(`phase2-m5`）只在用户确认后打，打在 main 的 merge commit 上
  （对齐 `phase1-m3`/`phase2-m4` 先例）。落库走分支 + PR。
- **stage_spec 归档**：每 stage 收口时在 `docs/stage_spec.md` 追加该 stage 的
  "交付内容 / 与 pgvector·hnswlib 的 trade-off / 已知残留与后续归队"三小节
  (M4 Stage A 欠账的教训：从 Stage 0 起每 stage 当次写完，不后补）。
- **对抗性 review**：每 stage 完成后一轮对抗审查（P1 必修、P2 登记、P3 尽修）;
  M5 选型本身经十轮审查，coding 期审查面 = 实现与选型的逐行对应（记录
  payload 布局、冻结清单逐项、八步序、窗口表行序）。
- **M5 的验证形态映射**(Phase 1 手段对齐，选型 §11):loom 不适用（无并发）;
  崩溃注入 = mem::forget 单步窗口 + 真 SIGKILL 子进程轮次（§11.1);watchdog
  纪律保留（一切可能阻塞的测试带硬超时——M5 的阻塞面是文件 I/O、1M 建库、
  链遍历）。
- **回归传承**：每 stage 出口 `cargo test --workspace` 全绿；两条依赖边
  (pg-am-hnsw → pg-storage、pg-engine → pg-am-hnsw）在 Stage 0 落地
  （选型 §2，见 Stage 0 交付物行）；动到 pg-storage/pg-engine
  既有行为的改动必须在本 stage 说明理由并跑全量。
- **冲突处理**：实现与 tech-selection 引用不符时，以代码为准并回改选型文档；
  决策层冲突不擅改，记入"开放问题与冲突标注"。

---

## 阶段 0:WAL 基建三件套 + 页初始化链 + CI 注册（2–3 天）

**归属**:M5 地基
**前置**:M4 收口（`phase2-m4` tag 已打）;tech-selection v1.12 用户终审通过
**目标**:7 个 WAL 判别值在 pg-storage 全链接入（解码/DPT/工具）,HNSW 页
初始化链可复用，CI 对新代码面零盲区。

| 任务 | 交付物 |
|------|--------|
| 判别值注册三件套（选型 §10.1 WAL 接入清单） | ① `record.rs:107-138` 的 `from_u8` 加 121–127 分支 + `WalRecord` 构造器（每类型一个，bincode standard payload，对齐 `btree_insert` 先例 record.rs:891-904);`tests/wal_record_type_discriminant.rs` 钉表新增 7 行。② `analysis.rs:267` `for_each_touched_page` 注册 7 类型分类（payload 目标页即 touched page,§4.2 自包含规则直接解出）——`analysis.rs:796` 穷举测试自动把守（未注册即红）。③ `pg-waldump.rs:336` 新增 7 类型解码臂（从 reserved-hex 移出） |
| 页初始化链（选型 §8.1 步骤 1 / §10.3,v1.9 P1) | `pg-am-hnsw` 新增 `page.rs`:HNSW 页类型常量 + 页头初始化（32B PageHeader 起手；节点页/目录页/meta 页三类）+ `log_page_init` 复用模式（post-image FPI + stamp pd_lsn——**post-image 内容 = 初始化后的合法 HNSW 页头，不是零页**;A1 契约 buffer_pool.rs:424-442，回收页与新分配页同链无例外）。测试：回收页（freelist 先分配再释放）初始化后断电恢复，页头为 HNSW 初始化态而非旧租户映像 |
| payload 布局单元测试 | 7 种 payload 的 encode/decode 往返 + 逐字段断言（含 meta_page_id 自包含规则，§4.2);`HnswNodeTombstone` 只测格式（语义 M6 生效，§1) |
| **CI 注册** | ci.yml:pg-am-hnsw 已在 clippy/test/doc 三 matrix(M4 已注册），本 stage 只需核对 pg-storage 新增测试被既有 matrix 覆盖 + pg-waldump 编译随 pg-storage 构建；预期零新增 job（接入清单全落在既有 crate 内）——核对结论写进 stage_spec 归档（"绿但没跑"反例核对，对齐 M4 CI 五件事的核对纪律） |
| **依赖边落地（选型 §2,v1.1 审查 P3-5 从工程规则前移到交付物）** | `crates/pg-am-hnsw/Cargo.toml` 加 `pg-storage` 依赖（M4 冻结注释的既定消费点——页初始化链/页格式在本 stage 首次真实使用）;`crates/pg-engine/Cargo.toml` 加 `pg-am-hnsw` 依赖（redo handler 注册点的前置——handler 本体在 Stage C，本 stage 只落依赖边与空 `hnsw_redo_handlers()` 骨架注册，保证接线从第一天可编译） |

**关键约束**：
- 判别值禁重编号（record.rs Stage 0 冻结纪律）;LogicalHnsw=100 保持不注册
  （恢复遇之硬失败，选型 §4.1 的"先预留后注册"模式）
- payload 全部含目标页定位（自包含规则，选型 §4.2);NodeInit/SetNeighbors/
  MetaUpdate 必带 `meta_page_id`

**验收命令**：
```bash
cargo test -p pg-storage --test wal_record_type_discriminant
cargo test -p pg-storage
cargo run -p pg-storage --bin pg-waldump -- --help   # 编译通过即可,解码臂在 Stage C 有真实记录可验
cargo test --workspace
```

---

## 阶段 A：访问器收口 + 物理应用原语 ×7(3–4 天）

**归属**:M5 算法-存储衔接层
**前置**:Stage 0(payload 布局与页初始化链可用）
**目标**：内存图的直接字段索引全部收口到访问器 funnel;7 个物理应用原语
按选型签名落码，校验层次（funnel 单点）从第一天成型。

| 任务 | 交付物 |
|------|--------|
| 访问器收口（选型 §10.2 任务 1) | `graph.rs`:`search_layer`/`insert` 内的直接字段索引（:578 的 `self.adjacency[...]` 等）全部改走 `neighbors()`/`vector()` funnel——纯重构零行为变更，M4 既有 139 测试 + 快照往返矩阵为行为钉（选型 §10.2 任务 3 的重构纪律） |
| 物理应用原语（选型 §10.2 任务 2,v1.7 签名） | `pg-am-hnsw` 新增 `apply.rs`(pub(crate)):`append_node(meta_page_id, node_page, slot, node_id, level, vector)`(§8.1 步骤 3：定长预留 + INITIALIZING + 槽位分配）、`set_neighbors(page, slot, node_id, level, count, content)`（原位更新，步骤 5/6;node_id = owner，无自环校验载体，v1.12)、`publish_live(page, slot, node_id)`（步骤 8)、`dir_append(dir_tail_page, node_id, node_page, slot)`（步骤 4)、`dir_link(old_tail_page, new_dir_page)`（步骤 2)、`apply_meta(meta_page_id, entry_point, max_level)`（步骤 7)、`apply_tombstone(page, slot, node_id)`(124 的 redo 承载，v1.9 P2-1) |
| 校验 funnel(选型 §10.1/§10.2 层次明文，v1.9 P2-2) | `validate.rs`:redo handler 与正常路径共用的校验 funnel——handler 先经 meta_page_id 读 meta 完成 §10.1 冻结清单（本 stage 先落与页格式相关的子集：dim 一致、L_max、有限性、Cosine 零向量、count==content.len()、层容量、top_level、升序/无重复/无自环、slot 态 ∈ {INITIALIZING, LIVE}),**原语不重复校验**；**时序说明**(v1.1 审查 P3-1):meta.rs 是 Stage B 交付物，本 stage 的 funnel 以**内存 meta 结构**为参落码（meta 页化读取在 Stage B 随 meta.rs 换实现），负例矩阵不受影响；可求值性约束（§10.1 v1.10）从第一天划线：链导出 HWM/目录映射的校验不进 funnel 的 redo 路径，归 Stage D 的 §11.3 审计 |
| 原语单元测试 | 每原语：正常应用 + 幂等重放（同记录 N=3 页字节全等，§11.2 幂等测试模式）+ 坏输入逐条响亮拒绝（funnel 校验的负例矩阵） |

**关键约束**：
- 原语是纯应用，校验只在 funnel（单一实现纪律，M4 Stage B 教训）；禁止在
  原语里复制任何一条校验
- 本 stage 不动算法行为：收口重构若改变任一既有测试输出 = 退回

**验收命令**：
```bash
cargo test -p pg-am-hnsw
cargo test -p pg-am-hnsw --release
cargo clippy -p pg-am-hnsw --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc -p pg-am-hnsw --no-deps
```

---

## 阶段 B：页布局 + 目录链 + meta page + 创建协议（4–5 天）

**归属**:M5 存储格式主体
**前置**:Stage A（原语可调用）
**目标**:§7 全部冻结格式落码并可创建/打开真实索引；格式钉测试一次到位。

| 任务 | 交付物 |
|------|--------|
| 节点条目定长分档（选型 §7.2) | `node.rs`：条目编码（vector + 1B 状态位布局 top_level:6/state:1/tombstone:1 + 逐层满载预留）；按抽取 level 分档；INITIALIZING = 零 count 形态；**格式常量钉测试**（位布局、各档字节数、默认参数 dim ≤ 1791 的创建时硬校验——dim/m/m_max0 乘积联动，超限响亮拒绝；16k PAGE_SIZE 只编译不验收，§13.10) |
| 目录页链（选型 §7.1) | `dir.rs`：目录页编码（32B PageHeader + 24B 自描述头 version/flags/reserved/ordinal/count/next = 格式常量；条目 10B,813 条目/页）;append/link/HWM 导出（`HWM = tail.ordinal × 813 + tail.count`);**格式常量钉测试**(813 与头布局进 §13.8 的格式钉）;链结构四断言的可复用检查函数（ordinal 连续自 0、中间页恰满、next 唯一成链、末页计数 ∈ [0, 813]——Stage D 审计直接消费，§11.3) |
| meta page(选型 §6) | `meta.rs`：字段钉死（dim/m/m_max0/efC/metric/selection/rng_seed/ef_search_default/entry_point/max_level/目录链头/快照格式版本引用）;创建时写入，加载/重放校验，metric/selection/params 错配硬失败（ef_search_default 降级 WARN,v1.4 nano——开工时按选型 §6 字段注落定） |
| 创建协议（选型 §10.3) | `HnswIndex::create`:new_page（目录首页）→ init + log_page_init → new_page(meta)→ init + log_page_init → first_page 记入 `pg_rust_relpages`(pg-engine 侧，engine.rs:356-357/:902 先例）；**open 时 meta 修复**：目录链非空但 entry_point=INVALID → 从目录首条目重建（写正常 HnswMetaUpdate，幂等、确定性）;rng skip-ahead(open 时按 HWM 重放 `next_level(m)` × HWM 次，§5 (c) 案——1M ≈ 毫秒级，实测值随 benchmark 文档落盘） |
| 创建/open 测试 | 创建→open 往返（参数钉死校验负例：dim/m/metric/selection 错配各一）;open 修复测试（构造 6–7 窗口残态：节点全连通、meta 无入口点 → open 后入口点 = 首节点）;skip-ahead 测试（续插 level 流与未崩溃运行逐值一致） |

**关键约束**：
- 格式常量的任何改动 = 格式修订，过修订记录（§7.1 纪律）;813 不是可调参数
- 创建是 utility 操作（§8.1：无事务性 DML),first_page 登记是引擎内最小登记
  （非 SQL/DDL 用户面，§10.3 范围调和）

**验收命令**：
```bash
cargo test -p pg-am-hnsw
cargo test -p pg-engine
cargo test --workspace
```

---

## 阶段 C:WAL 记录 + redo handler ×7 + 正常写入路径（4–5 天）

**归属**:M5 恢复主体（工艺风险最高的 stage)
**前置**:Stage B（索引可创建/open，原语/页格式/校验 funnel 就位）
**目标**：八步写入序全程 WAL 先行；redo 七 handler 注册进 Engine::open;
冻结清单逐项落实；正常路径与 redo 共用原语。

| 任务 | 交付物 |
|------|--------|
| 正常写入路径（选型 §8.1 八步） | `insert` 的页驻实现：步骤 1–8 逐条 WAL 记录（PageAlloc（复用 40)+ 本 crate 初始化链（同链无例外，选型 §8.1 步骤 1 / v1.9 P1：回收页与新分配页同链）→ DirLink → NodeInit → DirAppend → SetNeighbors×N → MetaUpdate → PublishLive),`flush_to`（末条 LSN）为成功边界（§8.1 成功边界①);meta 更新先于 PublishLive（步骤 7 在 8 前，§8.1);M5 单线程写入，入口为 utility/auto-commit 形态（无事务性 DML,§8.1 loser 段） |
| **页驻查询路径（选型 §10.2 任务 3 / §11.4,v1.1 审查 P1-1 补登）** | `search(query, k, ef)` 的页驻实现——**算法核心泛型化在本行完成**(select_neighbors/search_layer 抽成自由函数，内存图与页驻图共享，选型 §10.2 任务 3 既定重构；v1.2 复核 nano:Stage A 只做任务 1 访问器收口且声明零行为变更，泛型化抽取的明确落点即本行）；这是 §11.4"计时 `Engine::open` 到可查询"与 §13.7"recall 门槛对页驻图同样成立"的承载：**无此交付物则 Stage E 的 30s 实测终点"可查询"无法成立** |
| redo handler ×7(选型 §10.1) | `redo.rs`:7 个 handler 本体填入 Stage 0 的空骨架 `hnsw_redo_handlers()`(v1.2 复核 nano:注册动作在 Stage 0 已随依赖边落地，本 stage 是填实现非二次注册；Engine::open extend 链 engine.rs:687-689，选型 §2);每 handler = pd_lsn 守卫先行（已应用即跳过不重验，FPI 前提：镜像含全部先序同页记录内容）→ funnel 校验（§10.1 冻结清单逐类型项）→ 调原语应用；**handler 无状态化**（只用 RedoContext.buffer_pool + page_allocator,§10.1;遇 None 硬失败 = 纵深防御） |
| 冻结清单落实（选型 §10.1,v1.12 终态） | 逐类型校验项与负例测试矩阵：NodeInit(dim 一致/L_max/有限性/**Cosine 零向量**,v1.11 P2-1/slot 态）;SetNeighbors(count==len/层容量/top_level/升序无重复/**无自环——经 payload owner node_id 判定**,v1.12);DirAppend（追加位置精确/state ∈ {INITIALIZING, LIVE}——**跨页可变状态只断言集合不断言单值**,v1.11 P1);DirLink（未链接/ordinal+1);MetaUpdate(max_level == 入口点 top_level，弱化口径 v1.8);PublishLive（存在 + 幂等态）;Tombstone（存在 + LIVE)。**可求值性约束落实**(v1.10)：被引 id < HWM / entry_point < HWM / PublishLive 目录一致性 / **SetNeighbors owner 目录一致性**(v1.12）四项**不进** redo 路径，留 Stage D 审计——本 stage 在 funnel 里注释钉死这条界线（改线 = 协议修订） |
| rng skip-ahead 接入（选型 §5 (c) 案） | open 后正常 insert 的 rng = seed(meta)+ skip-ahead(HWM 次）;Stage B 的 skip-ahead 测试扩展：恢复后续插 level 流与未崩溃运行逐值一致（跨崩溃可复现，§11.1) |
| 幂等与重放测试 | 每记录类型 N=3 重放字节全等（§11.2);合成 WAL 流（正常路径产出）→ 模拟崩溃点截断 → redo → 页字节与未截断运行一致（前缀确定性,§4.2) |

**关键约束**：
- WAL 先行 100% 经 buffer pool（无绕过写，§13.4);pd_lsn 是唯一幂等锚，
  禁止在任何记录里发明第二锚
- handler 不得触链遍历（可求值性约束）;恢复时长是自 D 起的实测纪律

**验收命令**：
```bash
cargo test -p pg-am-hnsw
cargo test -p pg-engine
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test recall_siftsmall   # M4 门槛不回归(§11.5)
cargo test --workspace
```

---

## 阶段 D：崩溃测试 harness + §11.3 审计（4–5 天）

**归属**:M5 验收主体
**前置**:Stage C（写入路径与 redo 完整）
**目标**：选型 §11 的全部验证形态落地；崩溃窗口矩阵与 SIGKILL 轮次成为
常驻防线。

| 任务 | 交付物 |
|------|--------|
| mem::forget 窗口矩阵（选型 §11.1) | `tests/m5_insert_crash.rs`(pg-am-hnsw 或 pg-engine，按调用层级定）:§8.1 八步 × §8.2 窗口表 10 行各一枚 forget 测试（对齐 btree_split_crash.rs 模式；多步协议需暴露内部步骤时按 SplitState 先例暴露 `InsertState` 式测试 API，仅测试可见）；每枚恢复后跑 §11.3 断言 |
| SIGKILL 轮次（选型 §11.1) | `crates/pg-engine/tests/m5_hnsw_crash_rounds.rs`:m2b_crash_rounds.rs 模式（:46-108)+ expectation.txt 前缀耐久断言；`M5_CRASH_ROUNDS` 环境变量，默认 25(CI, m2b_crash_rounds.rs:46-48 先例）、验收 1000(ROADMAP.md:277);轮次内 level 流跨崩溃可复现（§5 skip-ahead）使崩溃语义比对位级可行（同平台口径，§11.1) |
| §11.3 审计实现（v1.12 自含枚举） | `audit.rs`(pg-am-hnsw):open 后一次性全量审计——结构不变量（度数 cap、层计数、`max_level == 入口点 top_level` 弱化口径 v1.8、open 修复后 meta 与目录首节点一致）;目录链四断言（§11.3,v1.2 P3-2);**邻接良构断言 a–e**(a 端点存在 = 被引 id < 链导出 HWM 且目录条目占用；b 层级归属 = level-L 边目标 top_level ≥ L;c entry_point < HWM;d PublishLive 的 node_id 与目录映射一致，以上 v1.11 P2-2;**e SetNeighbors 的 owner node_id 与 (page,slot) 目录映射一致**,v1.12——同 d 的可求值性降级）;**§11.3 的 ②：恢复后页驻 recall@10 ≥ 0.98**(siftsmall,probe 口径——v1.1 审查 P1-1 补登：原枚举只有 ①③④ 与 a–d，漏了 ②，与 v1.11 刚修的"降级 ≠ 消失"同型）;幽灵/孤儿统计输出（§8.3 不变量 5，只登记不阻断）;loser 断言 ④ 的 redo 期部分已在 Stage C（流内 txn_id 检查），审计侧复核终态 |
| CI 注册（崩溃轮次） | ci.yml:**m2b_crash_rounds 先例的事实口径**(v1.1 审查 P3-2——实测 .github/workflows/ 无任何 crash-rounds 专用 job,m2b_crash_rounds 是随 pg-engine 既有 test matrix 以 25 轮默认值跑的）:m5_hnsw_crash_rounds 同路径，随 pg-engine 既有 test matrix 跑；如需隔离再新增专用 job（落码时核对写明归属与理由）;`M5_CRASH_ROUNDS` 不设 = 25；验收命令行 1000 轮归手动/nightly（预期耗时实测后写进 stage_spec) |

**关键约束**：
- forget 测试覆盖窗口表全部 10 行（含"1 前"平凡行与两条交错态行）;窗口表
  行序与代码步骤序逐行对应是审查面
- 审计是 redo 校验降级项的唯一落点（v1.10)：漏实现 a–e 任一条 = 降级变消失
- 轮次测试带硬超时（watchdog 纪律）;flaky 零容忍（确定性 seed 下没有 flaky 借口）

**验收命令**：
```bash
cargo test -p pg-am-hnsw --test m5_insert_crash
cargo test -p pg-engine --test m5_hnsw_crash_rounds                          # CI 口径 25 轮
M5_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m5_hnsw_crash_rounds --release -- --nocapture   # 验收口径
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test m5_recall_after_recovery   # §11.3 ② 恢复后 recall 门槛(本 stage 交付物)
cargo test --workspace
```

---

## 阶段 E:M5 收口（3–4 天）

**归属**:M5 出口
**前置**:Stage D(1000 轮验收口径跑通）
**目标**:30s 恢复实测落盘；硬要求验收；文档归档；出口 tag。

| 任务 | 交付物 |
|------|--------|
| 恢复 <30s 实测（选型 §11.4/§13.2) | 1M 向量建库 → **立即 checkpoint**(§9.2 硬要求的验收落实——bulk load 完成时显式触发，代码路径核对 + 测试断言：建库路径完成点存在 checkpoint 调用）→ 注入固定增量（100k insert)→ SIGKILL → 计时 `Engine::open` 到可查询；机器规格随文档落盘。**口径钉死**:<30s 只对 checkpoint 后增量窗口成立（§9.2/§13.2)，文档与报告不得含糊成全窗口承诺 |
| benchmarks 落盘 | `docs/phase2-m5-benchmarks.md`：机器规格、恢复时长（含重放窗口记录数）、崩溃轮次结果（25/1000)、skip-ahead 实测耗时、写放大实测 vs §4.2 核算（~2.9KB/insert ≈ 5.6×,v1.12)、M4 既有数字无回归对照 |
| 对抗审查 ×2 | 第一轮实现审查（P1/P2/P3 分级）→ 修复 → 第二轮复核；**重点审查面**:WAL 记录 payload 与 §4.2 表逐字段对应、冻结清单逐项、八步序与 §8.1 逐行对应、窗口表 10 行与 forget 测试逐一对应、可求值性界线（redo 路径无链遍历的 grep 级证明） |
| **覆盖率复核**(v1.1 审查 P3-3 补登） | 覆盖率报告复核 + 不足补测——M4 注册的 pg-am-hnsw tarpaulin job(ci.yml:145-165，口径 ≥90%,M4 Stage E 收口判项同型）继续承接；M5 新增页格式/redo/恢复路径代码会稀释覆盖率，**判定以 CI(Linux）报告为准**，不足模块（redo 负例、页编码分支、审计路径）补测试 |
| stage_spec 归档 | Stage 0–E(M5）各节：交付内容 / trade-off / 已知残留（tombstone 只格式、隐藏高层节点弱化口径、孤儿/幽灵泄漏记账、16k 未验证、min(rec_lsn) 未启用、O1/O2 归期——逐项与选型 §12 对账） |
| 全量回归 + release | `cargo test --workspace` debug + release 全绿（M1–M4 全部传承）;clippy/fmt/doc 三档绿；bench-nightly 的 criterion smoke 不回归 |
| 出口 tag | `phase2-m5`(**经用户确认后**打；分支 + PR 落库，tag 打在 main 的 merge commit 上——对齐 `phase1-m3`/`phase2-m4` 先例） |

**验收命令**：
```bash
cargo test --workspace && cargo test --workspace --release
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
M5_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m5_hnsw_crash_rounds --release -- --nocapture
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test recall_siftsmall
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test m5_recall_after_recovery   # §11.3 ②(Stage D 交付物,收口复核)
```

---

## CI 注册清单（逐项：进哪个 workflow、什么条件下跑）

| # | 内容 | workflow/job | 运行条件 |
|---|------|-------------|---------|
| 1 | wal_record_type_discriminant 钉表（新增 7 行） | ci.yml 既有 pg-storage test matrix | 每 push |
| 2 | DPT 穷举测试（analysis.rs:796，机制自带） | ci.yml 既有 pg-storage test matrix | 每 push；新类型未注册即红 |
| 3 | pg-waldump 编译 + 解码臂 | ci.yml 既有 pg-storage 构建/test | 每 push |
| 4 | pg-am-hnsw 新增测试（原语/页格式/meta/redo/幂等） | ci.yml 既有 pg-am-hnsw clippy/test/doc 三 matrix(M4 已注册） | 每 push |
| 5 | m5_insert_crash(forget 窗口矩阵） | ci.yml pg-am-hnsw 或 pg-engine test（按落码归属） | 每 push |
| 6 | m5_hnsw_crash_rounds(25 轮） | **无专用 job**——随 pg-engine 既有 test matrix 跑（m2b_crash_rounds 先例的事实口径，v1.1 审查 P3-2 实测 .github/workflows/ 无 crash-rounds 专用 job)；如需隔离再新增（落码核对写明） | 每 push；`M5_CRASH_ROUNDS` 不设 = 25 |
| 7 | 1000 轮验收 + 30s 实测 | 手动/nightly(bench-nightly.yml 或专用 workflow，落码定） | 手动触发；机器规格随 benchmarks 落盘 |
| 8 | M4 recall 门槛（recall-gate job,M4 已注册） | 既有 job 不变 | 每 push;M5 改动不得使其变红（§11.5) |
| 9 | 覆盖率（tarpaulin job,ci.yml:145-165,M4 已注册） | 既有 job 不变 | 每 push（按该 job 既有触发条件）;M5 新代码不得把覆盖率压过 90% 口径（Stage E 复核） |
| 10 | grep 护栏核对（新 crate 面零命中声明） | stage 0 归档时人工核对记录 | 一次性 + 新增 workflow 时复核 |
| 11 | m5_recall_after_recovery(§11.3 ②,Stage D 交付物） | **recall-gate job 与 recall_siftsmall 同跑**(v1.2 复核 P3-a——该测试复用 `M4_REQUIRE_DATASET=1` 硬失败语义，在普通 test matrix 中永远跳过；recall-gate 已就位数据集，siftsmall 规模增量成本小。不入此 job 则 §11.3 ② 在 CI 零覆盖，重演 M4"绿但没跑"形态） | 每 push（随 recall-gate 既有触发条件） |

---

## 总时间估算

| 阶段 | 预估 | 依赖 |
|------|------|------|
| 0 基建 | 2–3 天 | 无（tech-selection v1.12 终审） |
| A 原语 | 3–4 天 | 0 |
| B 布局 | 4–5 天 | A |
| C 恢复 | 4–5 天 | B |
| D 崩溃 harness | 4–5 天 | C |
| E 收口 | 3–4 天 | D |
| **串行** | **20–26 天** | A 与 B 前半（页格式）可适度交叠，其余硬串行 |

风险余量：redo/恢复路径的工艺风险集中在 C/D（已各按上限估）;30s 实测
若超标，第一旋钮是 checkpoint 频率（§9.2),buffer pool  warm-up 影响恢复
计时为已知测量变量（实测时记录）。

## 依赖关系图

```
0 (WAL 三件套 + 页初始化链 + CI)
 └── A (访问器收口 + 原语 ×7)
      └── B (节点页/目录链/meta/创建协议)
           └── C (WAL 记录 + redo ×7 + 写入路径)
                └── D (forget 矩阵 + SIGKILL 轮次 + §11.3 审计)
                     └── E (30s 实测 + benchmarks + 归档 + tag)
```
（B 的页格式可在 A 的原语完工前先行落码，交叠约 1–2 天；C 硬依赖 B 的
创建/open 与 A 的原语，不可提前。)

## 回归测试传承

- M4 的 139 枚测试 + bench smoke 全程全绿（§11.5);M4 快照 v1 格式零改动，
  golden bytes 钉每 stage 出口必绿
- M1–M3 的 loom / m2 系列 crash rounds / stress 不在 M5 常规出口内，Stage E
  收口轮跑一次全量（对齐 M3 Stage G / M4 Stage E 口径）
- 测试清单按 stage 累加：0（判别值/payload/初始化链）→ A（原语 + 幂等 +
  funnel 负例）→ B（格式钉 + 创建/open/修复/skip-ahead)→ C(redo 负例矩阵
  + 前缀确定性）→ D(forget ×10 + crash rounds + 审计）

## 开放问题与冲突标注

- **O1（超上限维溢出，(1791, 2000])** 与 **O2(f16/bf16)**：归 §12 既定
  窗口，coding 期不提前做；O1 建议 (ii)（硬限制报错）落地时只需创建校验
- **隐藏高层节点弱化口径**(v1.8):`max_level == 入口点 top_level` 是断言
  口径；M6 若要恢复 M4 快照的强口径需另立修复路径，本计划不承诺
- **tombstone 生效与 vacuum**:M5 只交格式 + redo 承载（§1);M6 生效时的
  搜索过滤与 loser 对接已登记为 M6 前置（§8.1 loser 段）
- **16k PAGE_SIZE**：公式参数化但验收只钉 8KB(§13.10);16k 配置任何问题
  不在本 milestone 修
- **redo 起点的 min(rec_lsn)**：不启用（Phase 1 传承）;30s 若实测超标按
  §9.2 的 checkpoint 频率旋钮处置，不现场发明恢复重构
- **技术选型文档内不一致登记**：撰写本计划时未发现（写放大数字在 §4.2/§9.2
  同步（v1.0 时 2.8KB/5.4×/2.8GB,v1.12 起为 2.9KB/5.6×/2.9GB——同步性
  本身历轮保持），步骤数 8 与窗口表 10 行在 §8.1/§8.2/
  §11.1 一致，handler 数 7 与判别值段 121–127 在 §4.1/§4.2/§10.1 一致，
  HWM 公式在 §4.2/§6/§7.1 三处一致——修订记录与正文无脱节；一致性快查
  结论为 v1.0 时（选型 v1.11）所核，v1.12 的数字口径变迁见本计划修订记录）。

## review 修订记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-09-10 | 初版，基于 tech-selection **v1.11**(十轮审查，含 v1.10 可求值性约束与 v1.11 三项修复）。阶段骨架 0–E 六段；硬约束速查 9 条全部可回溯选型 § 节；CI 注册清单 9 项逐项落 workflow；总估 20–26 天，依赖关系图与交叠说明；技术选型文档一致性快查（写放大/步骤数/窗口行数/handler 数/HWM 公式）未发现内部不一致 |
| v1.1 | 2026-09-10 | 第一轮对抗审查回流（agent-23,verdict FAIL → 全量修复）:**P1-1（交付物缺口）**——Stage C 补"页驻查询路径"交付物行（选型 §10.2 任务 3 / §11.4:search 页驻实现是 30s 实测"可查询"终点与 §13.7 的承载，无它 Stage E 不成立）;Stage D 的 audit.rs 行补 §11.3 的 ②（恢复后页驻 recall@10 ≥ 0.98，原枚举漏②，与 v1.11"降级≠消失"同型）;Stage D/E 验收命令各补 `m5_recall_after_recovery` 行。**P3-1**:Stage A funnel 行补时序说明（meta.rs 是 Stage B 交付物，funnel 先以内存 meta 结构为参，Stage B 换页化实现，负例矩阵不受影响）。**P3-2(CI 事实错误）**：实测 .github/workflows/ 无 crash-rounds 专用 job,m2b_crash_rounds 随 pg-engine 既有 test matrix 以 25 轮默认值跑——Stage D CI 行与清单 #6 改事实口径（同路径，如需隔离再新增）。**P3-3**:Stage E 补覆盖率复核判项（ci.yml:145-165 tarpaulin job M4 已注册，口径 ≥90%，判定以 CI 报告为准）;CI 清单新增覆盖率行（#9)。**P3-4**:Stage C 写入路径行"PageAlloc 复用 40 或本 crate 初始化链"→"PageAlloc(复用 40)+ 本 crate 初始化链（同链无例外）"("或"字重引入双路径，与 v1.9 P1 冻结矛盾）;硬约束速查 §3"(b) 已被实测否决"→"估算否决"（选型 §3 口径为线性下界外推非实测）。**P3-5**：修订记录 v1.0 删"终态"（选型 v1.11 自述待复核，用户终审未过；plan 的"终审通过"前置闸门保持）;两条 Cargo.toml 依赖边从工程规则前移为 Stage 0 交付物行（pg-am-hnsw → pg-storage 首用 = 页初始化链；pg-engine → pg-am-hnsw 先落空骨架注册保证接线可编译）。**tech-selection 勘误×2**(v1.11 P2-1 引入的 graph.rs:385 引用错误：:385 是 rustdoc，实际调用点 graph.rs:402 `validate_entry_vector(vector, "insert")`，定义 :503)——§10.1 NodeInit 冻结清单条目与文末 v1.11 修订记录行同步改 :402 |
| v1.2 | 2026-09-10 | 第二轮复核回流（agent-23,verdict **PASS 附条件** → 条件项本轮闭合）：第一轮 1 P1 + 5 P3 逐项实证修复成立，tech-selection 两处勘误落地，新内容无回退。闭合项：**P3-a(m5_recall_after_recovery 在 CI 零执行点）**——该测试复用 `M4_REQUIRE_DATASET=1` 硬失败语义，普通 test matrix 永远跳过、recall-gate 只跑 recall_siftsmall,§11.3 ② 在 CI 零覆盖（M4"绿但没跑"同型）;CI 清单补 #11：进 recall-gate job 与 recall_siftsmall 同跑（数据集已就位，siftsmall 规模增量成本小）。**nano×2**:① Stage C 页驻查询行写明"算法核心泛型化在本行完成"(Stage A 只做任务 1 收口且零行为变更，泛型化抽取原无明确落点）;② Stage C redo handler 行改"handler 本体填入 Stage 0 空骨架"（注册动作 Stage 0 已落地，消除"注册两次"读感）。两轮轨迹 FAIL→PASS，可进 Stage 0 |
| v1.3 | 2026-09-10 | tech-selection v1.12 同步（用户终审 P1:SetNeighbors "无自环"校验不可实现，payload/原语补 owner node_id):文首基线 v1.11→v1.12（十一轮；修订记录 v1.0 行基线表述不动，历史事实）;Stage C 冻结清单落实行 SetNeighbors 项补"无自环——经 payload owner node_id 判定（v1.12)"，可求值性约束降级项三项→四项（补 owner 目录一致性）;Stage D audit.rs 行邻接良构断言 a–d→a–e(e = owner 目录映射一致，同 d 的可求值性降级）;Stage E benchmarks 落盘行 ~2.8KB/5.4× → ~2.9KB/5.6×(v1.12:+4B/条 × ≈17 条/insert = +68B);硬约束速查与一致性登记处的 2.8GB/2.8KB 正文引用同步（历史修订记录行内旧数字保留——当轮事实） |
| v1.4 | 2026-09-10 | v1.3 同步的复核回流（verdict FAIL：修订记录声称已修复但正文四处仍旧，逐条属实并修复）:**P1**:Stage A 物理原语行 set_neighbors 签名仍缺 node_id → 补（page, slot, node_id, level, count, content)(:144);**P2**:Stage D 硬约束"漏实现 a–d 任一条"→ a–e(:239);**nano×4**：基线残留——"选型已定稿 11 版"→ 12 版（:20)、Stage 0 前置与时间估算表两处 tech-selection v1.11 → v1.12(:106/:304)、一致性登记处"v1.11 的修订记录与正文无脱节"注明为 v1.0 时核对结论（:355)。教训登记：跨文档同步的核对清单必须枚举**全部**携带旧版本号/旧签名的正文点位（本次漏了 Stage A 原语行——与 Stage C 清单行同内容不同行）,grep 模式要覆盖内容（签名/断言编号）而不只版本号 |
