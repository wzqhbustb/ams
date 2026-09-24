# Phase 2 M5 编码顺序

> 基于 `docs/phase2-m5-tech-selection.md` **v1.20**(多轮对抗审查闭环 + Stage 0/Stage A 落地回流，修订见该文档
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
> 13 版，本计划不含设计返工余量，工艺风险集中在 C/D 两阶段**

---

## v1.0 硬约束速查

M5 开工前请通读 tech-selection §3/§4/§7/§8/§10。以下 9 条为**编码期每天都要
对照**的硬性约束（违反则退回该 stage 重做），全部是选型文档十一轮审查钉死的
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
  M5 选型本身经十一轮审查，coding 期审查面 = 实现与选型的逐行对应（记录
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
**前置**:M4 收口（`phase2-m4` tag 已打）;tech-selection v1.13 用户终审通过
**目标**:7 个 WAL 判别值在 pg-storage 全链接入（解码/DPT/工具）,HNSW 页
初始化链可复用，CI 对新代码面零盲区。

| 任务 | 交付物 |
|------|--------|
| 判别值注册三件套（选型 §10.1 WAL 接入清单） | ① `record.rs:130` 的 `from_u8` 加 121–127 分支 + `WalRecord` 构造器（每类型一个，bincode standard payload，对齐 `btree_insert` 先例 record.rs:1223);`tests/wal_record_type_discriminant.rs` 钉表新增 7 行。② `analysis.rs:269` `for_each_touched_page` 注册 7 类型分类（payload 目标页即 touched page,§4.2 自包含规则直接解出）——`analysis.rs:854` 穷举测试自动把守（未注册即红）。③ `pg-waldump.rs:365` 新增 7 类型解码臂（从 reserved-hex 移出） |
| 页初始化链（选型 §8.1 步骤 1 / §10.3,v1.9 P1) | `pg-am-hnsw` 新增 `page.rs`:HNSW 页类型常量 + 页头初始化（32B PageHeader 起手；节点页/目录页/meta 页三类）+ `log_page_init` 复用模式（post-image FPI + stamp pd_lsn——**post-image 内容 = 初始化后的合法 HNSW 页头，不是零页**;A1 契约 buffer_pool.rs:424-442，回收页与新分配页同链无例外）。测试：回收页（freelist 先分配再释放）初始化后断电恢复，页头为 HNSW 初始化态而非旧租户映像 |
| payload 布局单元测试 | 7 种 payload 的 encode/decode 往返 + 逐字段断言（含 meta_page_id 自包含规则，§4.2);`HnswNodeTombstone` 只测格式（语义 M6 生效，§1) |
| **CI 注册** | ci.yml:pg-am-hnsw 已在 clippy/test/doc 三 matrix(M4 已注册），本 stage 只需核对 pg-storage 新增测试被既有 matrix 覆盖 + pg-waldump 随 pg-storage 构建编译（**七解码臂的执行验证由 `tests/waldump.rs` 承接**——随 pg-storage test matrix 跑，2026-09-14 四轮口径；初稿"仅随 crate 编译"口径过强已修）;预期零新增 job（接入清单全落在既有 crate 内）——核对结论写进 stage_spec 归档（"绿但没跑"反例核对，对齐 M4 CI 五件事的核对纪律） |
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
cargo run -p pg-storage --bin pg-waldump -- --help   # 打印 usage 且 exit 0;七解码臂的执行验证由 tests/waldump.rs 承接(2026-09-14 三轮复核口径)
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
| 物理应用原语（选型 §10.2 任务 2，落地签名 v1.19) | `pg-am-hnsw` 新增 `apply.rs`(pub(crate)):`select_slot(node_page, len) -> slot`（四轮 P1-1：非修改式选槽，WAL-first 次序的载体）、`append_node(node_page, node_id, top_level, geo, vector) -> slot`(§8.1 步骤 3：定长预留 + INITIALIZING;= select_slot + apply_node_at 组合；geo: NodeGeometry{dim, m, m_max0})、`apply_node_at(node_page, slot, node_id, top_level, geo, vector)`(redo 形态：slot 权威，空闲创建 / INITIALIZING 幂等覆写 / LIVE 或 gap 响亮）、`set_neighbors(node_page, slot, geo, level, content)`（原位更新，步骤 5/6——覆写 count+内容、预留尾部清零；owner node_id 的无自环校验在 funnel，原语不重复）、`publish_live(node_page, slot, dim)`（步骤 8)、`dir_append(dir_tail_page, node_id, target_page, target_slot) -> 条目位置`（步骤 4,10B 条目；node_id 键控幂等——hwm==id 追加 / hwm>id 幂等跳过且后像比对 / hwm<id 间隙响亮）、`dir_link(old_tail_page, new_dir_page)`（步骤 2)、`apply_meta(meta_page, entry_point, max_level)`（步骤 7)、`apply_tombstone(node_page, slot, dim)`(124 的 redo 承载，v1.9 P2-1);**124/127 的 payload wire 序为 (page, slot, node_id, dim, meta_page_id)**(v1.18——dim/meta_page_id 两轮追加均在末尾；meta_page_id 供 redo handler 自定位 meta;flags 高半字节版本化 v1,HNSW_STATE_VERSION_V1);**dim == meta.dim 自 v1.19 起为 redo 前置门**(handler 经 meta_page_id 读 meta 核对后应用，funnel 单点 `validate_state_dim`——审计拿不到历史 payload,v1.17/v1.18 的审计降级口径作废） |
| 校验 funnel(选型 §10.1/§10.2 层次明文，v1.9 P2-2) | `validate.rs`:redo handler 与正常路径共用的校验 funnel——handler 先经 meta_page_id 读 meta 完成 §10.1 冻结清单（本 stage 先落与页格式相关的子集：dim 一致、L_max、有限性、Cosine 零向量、count==content.len()、层容量、top_level、升序/无重复/无自环;**slot 态 ∈ {INITIALIZING, LIVE} 需页访问，归 Stage C handler 侧**——2026-09-14 Stage A 审查 P3-3 归属立文）,**原语不重复校验**；**时序说明**(v1.1 审查 P3-1):meta.rs 是 Stage B 交付物，本 stage 的 funnel 以**内存 meta 结构**为参落码（meta 页化读取在 Stage B 随 meta.rs 换实现），负例矩阵不受影响；可求值性约束（§10.1 v1.10）从第一天划线：链导出 HWM/目录映射的校验不进 funnel 的 redo 路径，归 Stage D 的 §11.3 审计；**124/127 的 dim == meta.dim 不是降级项**——v1.19 起为 redo 前置门（本 stage 落 `validate_state_dim` 单点；Stage C handler 经 payload meta_page_id 读 meta 后调用，审计拿不到历史 payload 故不可留审计） |
| 原语单元测试 | 每原语：正常应用 + 幂等重放 + 坏输入逐条响亮拒绝（funnel 校验的负例矩阵）。同记录 N=3 页字节全等（§11.2 幂等测试模式）适用于 redo 形态与后像型原语（apply_node_at / set_neighbors / publish_live / apply_tombstone / dir_append / dir_link / apply_meta);**分配型 append_node 每次调用占新槽，"每原语 N=3"对它不适用**——其重放幂等由 redo 形态 apply_node_at（同 slot 的 INITIALIZING 覆写，字节全等）与 handler 的 pd_lsn 守卫承担（v1.19 口径修正） |

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
| 创建协议（选型 §10.3) | `HnswIndex::create`:new_page（目录首页）→ init + log_page_init → new_page(meta)→ init + log_page_init → first_page 记入 `pg_rust_relpages`(pg-engine 侧，engine.rs:356-357/:902 先例）；**open 时 meta 修复**：目录链非空但 entry_point=INVALID → 从目录首条目重建（写正常 HnswMetaUpdate，幂等、确定性；**写序 pin_mut(FPI)→ append → apply → stamp pd_lsn，发布前查 top_level ≤ l_max(m)**,v1.30 六轮回流）;rng skip-ahead(open 时按 HWM 重放 `next_level(m)` × HWM 次，§5 (c) 案——1M ≈ 毫秒级，实测值随 benchmark 文档落盘） |
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
| redo handler ×7(选型 §10.1) | `redo.rs`:7 个 handler 本体填入 Stage 0 的空骨架 `hnsw_redo_handlers()`(v1.2 复核 nano:注册动作在 Stage 0 已随依赖边落地，本 stage 是填实现非二次注册；Engine::open extend 链 engine.rs:692-693，选型 §2);每 handler = pd_lsn 守卫先行（已应用即跳过不重验，FPI 前提：镜像含全部先序同页记录内容）→ funnel 校验（§10.1 冻结清单逐类型项）→ 调原语应用；**handler 无状态化**（只用 RedoContext.buffer_pool + page_allocator,§10.1;遇 None 硬失败 = 纵深防御） |
| 冻结清单落实（选型 §10.1,v1.19 终态） | 逐类型校验项与负例测试矩阵：NodeInit(dim 一致/L_max/有限性/**Cosine 零向量**,v1.11 P2-1/slot 态）;SetNeighbors(count==len/层容量/top_level/升序无重复/**无自环——经 payload owner node_id 判定**,v1.12);DirAppend（追加位置精确/state ∈ {INITIALIZING, LIVE}——**跨页可变状态只断言集合不断言单值**,v1.11 P1);DirLink（未链接/ordinal+1);MetaUpdate(max_level == 入口点 top_level，弱化口径 v1.8);PublishLive（存在 + 幂等态 + **dim == meta.dim 前置门**,v1.19——经 payload meta_page_id 读 meta,funnel `validate_state_dim`);Tombstone（存在 + LIVE + dim 前置门，v1.19 同上）。**可求值性约束落实**(v1.10/v1.19)：被引 id < HWM / entry_point < HWM / PublishLive 目录一致性 / **SetNeighbors owner 目录一致性**(v1.12）四项**不进** redo 路径，留 Stage D 审计——本 stage 在 funnel 里注释钉死这条界线（改线 = 协议修订）;Tombstone·PublishLive 的 dim ↔ meta.dim 一致性**自 v1.19 起移出本清单**（上移 redo 前置门——审计拿不到历史 payload，降级口径作废） |
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
| §11.3 审计实现（v1.19 自含枚举） | `audit.rs`(pg-am-hnsw):open 后一次性全量审计——结构不变量（度数 cap、层计数、`max_level == 入口点 top_level` 弱化口径 v1.8、open 修复后 meta 与目录首节点一致）;目录链四断言（§11.3,v1.2 P3-2);**邻接良构断言 a–e**(a 端点存在 = 被引 id < 链导出 HWM 且目录条目占用；b 层级归属 = level-L 边目标 top_level ≥ L;c entry_point < HWM;d PublishLive 的 node_id 与目录映射一致，以上 v1.11 P2-2;**e SetNeighbors 的 owner node_id 与 (page,slot) 目录映射一致**,v1.12——同 d 的可求值性降级；原 f 条（Tombstone/PublishLive 的 payload dim ↔ meta.dim）自 v1.19 起**上移 redo 前置门**,Stage C handler 经 payload meta_page_id 读 meta 核对后应用——审计拿不到历史 payload，不再是审计枚举项）;**§11.3 的 ②：恢复后页驻 recall@10 ≥ 0.98**(siftsmall,probe 口径——v1.1 审查 P1-1 补登：原枚举只有 ①③④ 与 a–d，漏了 ②，与 v1.11 刚修的"降级 ≠ 消失"同型）;幽灵/孤儿统计输出（§8.3 不变量 5，只登记不阻断）;loser 断言 ④ 的 redo 期部分已在 Stage C（流内 txn_id 检查），审计侧复核终态 |
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
| 2 | DPT 穷举测试（analysis.rs:854，机制自带） | ci.yml 既有 pg-storage test matrix | 每 push；新类型未注册即红 |
| 3 | pg-waldump 编译 + 七解码臂执行验证（tests/waldump.rs 逐字段断言，2026-09-14 四轮口径） | ci.yml 既有 pg-storage 构建/test | 每 push |
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
| 0 基建 | 2–3 天 | 无（tech-selection v1.13 终审） |
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
| v1.5 | 2026-09-11 | M5 Stage 0 对抗审查闭合登记（agent-23 verdict **PASS 附条件** → 条件项全部闭合）:**P3-1（主修）** pg-waldump `-h/--help` 改 POSIX 惯例（stdout usage + exit 0,unknown option exit 1)——Stage 0 验收命令 `cargo run -p pg-storage --bin pg-waldump -- --help` 保持原样，自此真实 exit 0;stage_spec Stage 0 验收行同步实测口径。**P3-2（裁断）**:`HnswError::Storage(String)` 基线接受，tech-selection §2 补错误通道行（v1.13)。**P3-3**:HNSW 构造器校验对齐——hnsw_set_neighbors 补 level ≤ 63 + 全部 9 个页字段补 PageId::INVALID 拒绝（共享 `reject_invalid_page_id`),负例矩阵扩展。nano×3:page.rs 改用 `PageHeader::write_to`（消除手写编码双份）;pd_flags 位分配登记入 pg-storage page.rs rustdoc + 选型 §7.1 同步；page_init.rs 断言语义改写（junk 被 tenant 自身 pre-image FPI 清零，承重断言 = init 内容存活 page_type≠0 + dir version 在位）。验证：pg-storage/pg-am-hnsw 全量 + 判别值钉表 + clippy/fmt/doc 全绿 |
| v1.6 | 2026-09-14 | M5 Stage 0 二轮审查回流（verdict FAIL → 全量修复；agent-21 三次超时后由主线接手收尾）。**P1（回收页未整页清零）**:init_node/meta/dir_page 只写头部，log_page_init 的整页后像把旧租户字节带进 FPI/WAL——三个 init_* 起手 `page.fill(0)`(page.rs:69/:76/:84)，补 junk 填充断言。**P2-1(waldump 无界解码）**:record.rs 立共享**有界、完整消费**解码 API `decode_hnsw_payload`（七种 payload 的 `decode` 全部改走；伪造长度前缀响亮拒绝无巨量分配、尾随字节拒绝；回归钉 `hnsw_bounded_decoders_reject_forged_lengths_and_trailing_bytes`),waldump 七臂接入。**P2-2(pd_lsn 契约未测试）**:page_init.rs 断言 log_page_init 返回 lsn == 页 pd_lsn、恢复后与 FPI 一致（删除 stamp 必红）。**P2-3(非法 NodeId)**：共享 `reject_invalid_node_id`,NodeInit.node_id/SetNeighbors owner+全部邻居/Tombstone/DirAppend/PublishLive 拒 u32::MAX;MetaUpdate 唯一例外 = 空图（entry_point=INVALID 且 max_level=0)，负例 +8。**P3-1**：基线 v1.12→v1.13(:3/:106/:304)。**P3-2(waldump 仅编译未执行）**：不选措辞弱化，补真执行验证——`tests/waldump.rs` 的 `dump_covers_every_record_family` 加 7 条 HNSW 记录 + 类型行与逐字段断言（meta=30 … vector=16B 等）;`lsn_filter_boundaries_are_inclusive` 的硬编码记录计数（filtered=15、lsns[19]）改按 `lsns.len()` 计算（新增记录使旧钉值失效的连带修复）。stage_spec Stage 0 归档同步（验收行 + 审查回流段 + CI 核对结论措辞） |
| v1.7 | 2026-09-14 | M5 Stage 0 三轮复核回流（verdict FAIL → 全量修复）。**P2（有界解码未真正实现）**：二轮版依赖 bincode serde 拒绝伪造长度，实证其在报错前仍按 cautious size_hint 预分配至 1 MiB——机制重写为**预解码闸门** `bounded_seq_gate`（标量前缀定游标 → 手读尾随 Vec varint 长度 → `claimed > 剩余/元素最小宽` 分配前拒绝）；首版实现取 u32 元素宽=4B，被 varint 单字节编码的合法邻居 payload 当场证伪（往返测试抓出），改元素最小宽 f32=4/varint=1；补解码侧契约校验（vector.len()==dim、neighbors.len()==count);P2-1 测试升级——伪造长度断言失败点必在闸门（错误消息含 "claims")。**P3×3**:① 本计划两处旧口径——"选型已定稿 12 版"→ 13 版（:20)、Stage 0 验收命令注释"解码臂延至 Stage C"→ waldump.rs 执行验证已承接（:128);② waldump.rs 补 MetaUpdate/Tombstone/DirLink/PublishLive 四臂逐字段断言（stage_spec"七臂逐字段"宣称自此属实）;③ tech-selection 头部状态矛盾消除（"草案/待复核"→ 已经用户终审，与本计划前置口径对齐）。验证：pg-storage lib 204 绿 + waldump 3/3 + clippy/fmt 绿；stage_spec 三轮回流段同步 |
| v1.8 | 2026-09-14 | M5 Stage 0 四轮复核回流（verdict FAIL（无 P1)→ 全量修复）。**P2-1（门禁漂移面）**：三轮版的两个 Prefix 结构体与手写 varint reader 是对 wire layout/编码规则的第二份维护——改为单一共享前缀 `HnswSeqPrefix`(bincode 位置编码，一构两用；加字段不同步则往返测试红，漂移不可静默）+ 线长经共享 `bincode_config()` 解码 u64（手写 reader 删除）。**P2-2**:SetNeighbors 伪造长度断言升级为必在预分配闸门（消息含 "claims")。**P3-1**：门禁补 wire 长度 == 声明 dim/count 的分配前比较（合法大小语义造假不再先分配后拒绝），补字节翻转测试。**P3×3**:① 本计划 Stage 0 CI 行"waldump 仅随 crate 编译"旧口径 → 执行验证承接（:115);② tech-selection 头部轮次表述统一（四轮闭环定稿）+ §13 标题去"草案";③ stage_spec 数字校准（lib 204、payload 测试 3 枚、pg-am-hnsw 144 分解）+ tarpaulin 口径读准（只插桩 pg-am-hnsw,pg-storage 侧无覆盖率背书）。验证：pg-storage lib 204 绿（含新闸门与翻转测试）+ clippy/fmt 绿；stage_spec 四轮回流段同步 |
| v1.9 | 2026-09-14 | M5 Stage 0 五轮复核回流（P3×3 → 全量修复）:① CI 清单 #3"waldump 编译 + 解码臂"未体现执行验证 → 补 tests/waldump.rs 七分支逐字段断言表述（:288);② page_init.rs 注释机制纠错——nano-3 注释虚构"tenant A pre-image FPI 清零页"（该 FPI 不存在，junk 无 WAL)：缺 init FPI 的真实终态是盘上 junk(page_type=0xABAB)，承重断言 = page_type == PAGE_TYPE_DIR，断言语义与注释对齐；③ pd_flags 偏移硬编码消除——pg-storage 新增 `page_pd_flags`/`set_page_pd_flags` 访问器（布局唯一属主，与 page_pd_lsn 同纪律）,pg-am-hnsw 改走访问器，12..14 字面量清零。验证：pg-am-hnsw 92+1 绿、pg-storage lib 相关 39 绿、clippy/fmt 绿；stage_spec 五轮回流段同步 |
| v1.10 | 2026-09-14 | M5 Stage 0 六轮复核回流（1 P2 + 3 P3 → 全量修复）。**P2（门禁漂移面根除）**：四轮的独立 `HnswSeqPrefix` 无法兑现"漂移不可静默"（误读长度巧合等于 tail 可静默通过）——结构性解法：payload struct 改为自身携带标量头（新增 pub `HnswSeqHead`,NodeInit/SetNeighbors 记录 = head + 序列，bincode 位置编码故 wire 布局逐字节不变），门禁解码记录自身包含的类型，加字段结构性同进退；构造器/waldump 两臂/往返测试同步（`r.dim()`/`r.count()` 访问器保可读性，`r.head.*`)。**P3×3**:① page.rs 单测残留 `page[12..14]` 字面量与"entry area 不属清零契约"注释（与二轮 P1 整页清零契约冲突）——断言改走访问器、注释对齐整页契约；② tech-selection 头部"四轮"vs 已登记五轮——头部状态行改非计数口径（"多轮审查闭环，逐轮回流见 stage_spec 归档"，消除每轮必改的 churn),stage_spec 状态行同步。验证：pg-storage lib 204 绿 + waldump 3/3 + pg-am-hnsw 92+1 绿 + clippy/fmt 绿；stage_spec 六轮回流段同步 |
| v1.11 | 2026-09-14 | M5 Stage 0 七轮复核回流（1 P2 + 3 P3 → 全量修复）。**P2(DPT 分析臂布局重复）**:NodeInit/SetNeighbors touched-page 解码手解两个 PageId（重复假定头布局，字段调整即追踪错页）→ `decode_prefix::<HnswSeqHead>` 解记录自有头取 `head.page_id`。**P3×3**:① 补 golden bytes 钉（`hnsw_payload_golden_bytes`——嵌套 head 与旧扁平布局逐字节相等的格式冻结，lib 204→205);② 本计划两处"十轮"→ 十一轮（:27/:88);③ tech-selection v1.13 修订记录 nano③ 保留已证伪的"tenant FPI 清零"机制 → 更正并交叉引用五轮回流段。验证：pg-storage lib 205 绿 + waldump 3/3 + analysis 9 绿 + clippy/fmt 绿；stage_spec 七轮回流段同步 |
| v1.12 | 2026-09-14 | M5 Stage 0 八轮复核回流（P3×3 → 全量修复）:① stage_spec 数字再校准（lib 204→205、payload 测试 3→4 枚，补登 golden 钉）;② DPT 穷举测试行号引用 :796→:854 四处（本计划 :112/:287、tech-selection :737、stage_spec 交付内容 ①;历史修订记录行保留当轮行号）;③ record.rs 两处手写 INVALID 检查（DirAppend.target_page/DirLink.next_page）改复用 `reject_invalid_page_id` 单一实现。验证：pg-storage lib 205 绿 + clippy/fmt 绿；stage_spec 八轮回流段同步 |
| v1.13 | 2026-09-14 | M5 Stage A 对抗审查回流（agent-23 verdict **PASS 附条件** → 条件项全部闭合，主线亲手修复）。**P2-1(append_node 缺 vector 长度守卫）**:dim+1 会静默写坏 state 字节/level-0 区域、更大长度 slice panic、dim−1 静默补零——入口处 `vector.len() != dim → Corrupted`（结构守卫，与 set_neighbors 容量守卫同级）。**P2-2(条目切片无边界守卫）**:set_neighbors/entry_neighbors 的 region 切片、publish_live/apply_tombstone/读侧访问器的 state 字节索引、entry_vector、append_node 的 pd_lower 下溢——全部补一行级 Corrupted 守卫（state_byte/state_byte_mut 助手）；注：in-bounds 的错误 dim 结构性不可检（条目不存 dim,§7.2)，归 funnel 的 meta.dim 校验，注释立文。**P3-1(redo 形态缺失）**：新增 `apply_node_at(slot, …)`——空闲创建/INITIALIZING 覆写幂等（§10.1 "目标 slot 空闲或 INITIALIZING"的实现载体）,append 形态留正常路径；LIVE 覆写与 gap slot 响亮拒绝。**P3-2**:meta page 形态收窄为"32B 头 + raw 字段区，LP 永不使用"(apply.rs 注释 + 选型 §6 回写）。**P3-3**:slot 态校验归属 Stage C handler 侧（validate.rs 注释 + 本表 funnel 行同步）。**P3-4**:DIR_ENTRIES_PER_PAGE 813 裸字面量 → 公式导出。**nano×2**:LP 引用 record.rs:262-269→:298;MetaView::l_max 补 m≥2 前提注。**裁断①**:选型 §10.2 补原语签名口径（页 buffer + NodeGeometry);**裁断②**:LP 再导出 + golden pin 防线（上移 pg-storage 登记 Stage B 候选）。**NodeGeometry 值对象**（clippy too_many_arguments 的结构性解法，非 allow)。新增测试 +5（守卫钉/geometry 误配/apply_node_at 三形态/813 公式/LP golden),lib 102→107；验证：pg-am-hnsw lib 107 绿 + clippy/fmt 绿；tech-selection v1.14、stage_spec Stage A 回流段同步 |
| v1.14 | 2026-09-14 | M5 Stage A 二轮复核回流（agent-23 verdict **PASS 附条件** → 条件项全部闭合，主线亲手修复；第一轮 2 P2 + 4 P3 + 2 nano + 2 裁断逐项实证成立）。**P2-1（页内容边界，一轮 P2-2 同类漏网）**:pd_lower/pd_upper/LP off+len 无 PAGE_SIZE 钳制——页无 checksum,bit-rot 页在 redo 路径 slice panic（探针实证 `&page[8000..16000]` panic，违反 error.rs no-panic 纪律）;read_lp 入口钳制（pd_lower ∈ [32, PAGE_SIZE] 且 LP 对齐）+ off/len 返回前钳制，append_node/apply_node_at 增 pd_upper 守卫。**P3-1**:tech-selection §10.2 原语 bullet 列表按落地签名重写（v1.7 遗物与口径段双重矛盾，Stage C handler 作者照写会全错）。**P3-2（裁断）**:set_neighbors/entry_neighbors 统一 NodeGeometry 签名（API 单约定，arity 反降）。**P3-3**:LP golden pin encode 断言改字面量 0x00C0_9F40（公式化期望与实现同式自指——M4 "oracle 局限"同型）。**nano**:l_max 补 m=2 → 53 边界钉。攻击未遂登记：entry_size 碰撞自洽（write_entry 按记录几何整写 + funnel dim 校验上游拦截）、INITIALIZING 覆写清 tombstone 合法（tombstone 只落 LIVE)、dir_append 腐 count 响亮。验证：pg-am-hnsw lib 107 绿 + clippy/fmt 绿；tech-selection v1.15、stage_spec 二轮回流段同步 |
| v1.15 | 2026-09-14 | M5 Stage A 三轮复核回流（用户终审 P3：源码行号漂移，逐项核实修复）。本计划两处：Stage 0 判别值注册三件套行（:112)from_u8 record.rs:107-138→:130、btree_insert 先例 :891-904→:1223、for_each_touched_page analysis.rs:267→:269、pg-waldump 解码臂 :336→:365(analysis.rs:854 穷举测试核实仍准确，不动）;Stage C redo handler 行（:202)Engine::open extend 链 engine.rs:687-689→:692-693。tech-selection v1.16、stage_spec :1171 及 pg-am-hnsw 两处源码注释（redo.rs:4、page.rs:105）同步；修订记录表历史行号保留当轮值 |
| v1.16 | 2026-09-15 | M5 Stage A 四轮复核回流（用户终审：2 P1 + 2 P2 + 4 P3 逐条代码实证后全部属实，主线修复；含两个前轮已判修复但未落入 c3f0ab1 的 P3 遗留）。**P1-1(append_node 违反 WAL-first)**：选槽与应用耦合——新增非修改式 `select_slot(node_page, len)`（正常路径：选槽 → 写进 WAL 记录 → append/flush → `apply_node_at`),append_node 改为组合（单一实现）。**P1-2(PublishLive/Tombstone payload 缺状态字节定位）**:redo 无状态，payload 仅 page/slot/node,4·dim 不可寻——两 payload 补 `dim: u16`(record.rs 格式修订：构造器 dim=0 响亮、golden 钉补 124/127 位置、waldump 两臂打印 dim、analysis.rs 零影响）;dim ↔ meta.dim 一致性同型降级 Stage D 审计（本计划 :203/:233 两项同步——可求值性约束四项→五项、邻接良构断言 a–e→a–f)。**P2-1(read_lp 元组区）**：补 [pd_upper, pd_special) 区界钳制（伪造 LP 指向 LP 数组/空闲区/越 pd_special 皆响亮）。**P2-2(pd_lower 对齐）**:append 路径共享 `append_bounds`（含 4 字节对齐）。**P3-1(top_level 静默掩码遗留）**:`& 0x3F` 改响亮 Corrupted（先于任何写入）。**P3-2(L_max 重复遗留）**:rng.rs 导出单一来源 `l_max(m)`,validate.rs 与 next_level debug_assert 委派。**P3-3(dir_append 幂等）**:node_id 键控（== HWM 追加 / > 跳过 / < 间隙响亮 / 早于页基址响亮）,:146 的"每原语 N=3 字节全等"对 dir_append 自此真正成立（非仅 handler pd_lsn)。**P3-4（热路径 Vec 分配）**:`neighbor_iter`/`vector_iter` 零分配迭代器为 Stage C 搜索形态，Vec 版改 .collect() 封装。新增测试 +8;pg-am-hnsw lib 107→115、pg-storage lib 205 绿、waldump 3/3、clippy -D warnings/fmt 绿；tech-selection v1.17、stage_spec 四轮回流段同步 |
| v1.17 | 2026-09-15 | M5 Stage A 五轮复核回流（用户终审：1 P1 + 3 P2 + 2 P3 逐条核实属实）。**P1(124/127 payload 补 meta_page_id)**:§11.3 审计（dim == meta.dim）须能从 payload 自定位 meta 页，无此字段则错误 dim 改写错误字节且无法可靠发现——两 payload 末尾补 `meta_page_id: PageId`,构造器首参加参过 `reject_invalid_page_id`;apply.rs 原语签名不变（纯页内手术）。**P2(flags 版本半字节）**：照 CheckpointEnd 先例新增 `HNSW_STATE_VERSION_V1`/`HNSW_STATE_V1_FLAGS`（覆盖 124/127;flags=0 = 预版本化开发格式，从未随 release 落盘，响亮拒绝不做兼容解码；其余五类型格式未变，flags=0 即隐式原始版）;decode 改 (payload, flags) 双分派,waldump 两臂传入 record.flags 并打印 meta_page_id。**P2(apply.rs 三条）**:append_bounds 补 pd_special ∈ [pd_upper, PAGE_SIZE] 钳制；dir_append 的 HWM 改 checked_add（溢出响亮 Corrupted);dir_append 幂等分支补后像比对（既有条目与记录 (target_page, target_slot) 不符即响亮 Corrupted——幂等重放必须字节同源）。**测试同步**:record.rs 调用点全量换签名 + 新增 flags 值断言/版本 0 与未知半字节拒绝/INVALID meta_page_id 拒绝/往返 meta_page_id 断言（新增 1 测试 + 扩展 2);apply.rs 新增 round5 三守卫用例（pd_special 坏值/hwm 溢出/后像相符通过与不符拒绝）;analysis.rs:816/:828 与 tests/waldump.rs:109/:112 同步。**P3（文档）**:本行 v1.17(Stage A 原语签名更新、:239 审计断言 a–e→a–f 遗留修正）;tech-selection v1.18(§4.2 表 124/127、:189 旧三字段表述、自包含规则段、§10.1 清单、§11.3 f 条自定位化）;stage_spec 五轮回流段。验证：pg-am-hnsw lib 116 绿、pg-storage lib 206 绿、waldump 3/3、判别值 3/3、clippy -D warnings/fmt/doc 绿 |
| v1.18 | 2026-09-15 | M5 Stage A 六轮复核回流（用户终审：1 P1 + 1 P2 + 1 P3 逐条核实属实）。**P1(dim == meta.dim 上移 redo 前置门）**:v1.17/v1.18 的审计降级口径实质不成立——redo 先于审计，错误 dim 在审计介入前已改写错误字节且审计拿不到历史 payload；本计划同步：Stage A 原语行补 wire 序 (page, slot, node_id, dim, meta_page_id) 与 redo 前置门口径、funnel 行补 `validate_state_dim` 单点、Stage C 冻结清单行 PublishLive/Tombstone 补 dim 前置门、可求值性约束五项→四项（dim 项移出）、Stage D 审计枚举 a–f→a–e(f 条上移）、硬约束速查 a–f→a–e。**P2(DPT flags 门）**:analysis.rs 两臂改记录自身版本化 decode + 负例测试（无本计划正文锚点）。**P3**:文首基线 v1.13→v1.19。tech-selection v1.19、stage_spec 六轮回流段同步。验证：pg-am-hnsw lib 117 绿、pg-storage lib 207 绿、clippy/fmt/doc 绿 |
| v1.19 | 2026-09-15 | M5 Stage A 七轮复核回流（用户终审 P3×5 文档漂移，本计划占③④):③ Stage A 原语签名行按落地签名重写（select_slot/apply_node_at 入表、geo: NodeGeometry、dir_append 幂等三态与返回值、无自环校验归 funnel;v1.7 旧签名废止）;④ 原语单元测试行"每原语 N=3"对分配型 append_node 不适用——改注 N=3 适用 redo 形态与后像型原语，append_node 幂等由 apply_node_at（同 slot INITIALIZING 覆写）+ handler pd_lsn 承担。tech-selection v1.20、stage_spec 七轮回流段同步。纯文档轮，代码零改动 |
| v1.20 | 2026-09-15 | M5 Stage B slice 1 落码登记:**LP 布局上移 pg-storage 兑现**(Stage A 裁断② 评估结论"做"——pg-storage/src/page.rs 新增 LINE_POINTER_SIZE/LP_NORMAL/encode_line_pointer/decode_line_pointer 为布局单一真源，golden 0x00C0_9F40 双侧钉；apply.rs 删本地常数改消费);**node.rs 新建**(§7.2 节点条目格式属主，自 apply.rs 迁入 + NODE_PAGE_USABLE + check_creation_geometry 乘积联动创建时硬校验——默认 dim ≤ 1791 精确值、公式化无写死 m_max0);**dir.rs 新建**(§7.1 目录链属主，DIR_ENTRIES_PER_PAGE 公式化迁入 + check_dir_chain 四断言 + HWM 导出单实现，fetch 注入无 I/O)。新增测试 6(pg-am-hnsw lib 117→123)、pg-storage +1(lib 207→208);clippy -D warnings/fmt/doc 绿;tech-selection v1.21、stage_spec Stage B 节同步 |
| v1.21 | 2026-09-15 | M5 Stage B slice 2 落码登记:**meta.rs 新建**(pub(crate),选型 §6 meta page 完整布局属主——冻结偏移表(entry_point@32/max_level@36 Stage A 冻结未动)+ MetaParams/write_meta/read_meta(11 项结构校验,check_creation_geometry 单一规则)+ check_expected(六硬错配 InvalidArgument 带字段名两侧值;ef_search_default 错配 → warnings Vec<String> 不失败,**v1.4 nano "WARN 而非硬失败"落地为字符串返回,不加 tracing 依赖**));graph.rs 两枚举判别值首次持久化(metric/selection discriminant + from_discriminant 未知响亮);apply.rs META_OFF 单一属主迁入;validate.rs `From<&MetaParams> for MetaView`(Stage C handler 页化 meta 进 funnel 不换形)。新增测试 3(lib 123→126);clippy -D warnings/fmt/doc 绿(限 pg-am-hnsw);tech-selection v1.22、stage_spec Stage B 节同步 |
| v1.22 | 2026-09-16 | M5 Stage B slice 3a 落码登记:**index.rs 新建**(pub,§10.3 实体)——create(几何硬校验 → dir 首页 init+FPI → meta 页 init+write_meta+FPI)、open(read_meta → check_expected → check_dir_chain(pin fetch)→ open 修复(目录条目 0 top_level → 正常 HnswMetaUpdate + apply_meta + pd_lsn,三性立文)→ rng skip-ahead(hwm 次重播));dir.rs 补 `dir_entry`;OpenOutcome.warnings 承载 ef_search_default WARN;Stage C 测试依赖登记(121–127 逻辑记录引擎重开重放验证归 redo handler 落地后)。测试 +3(lib 127、集成 m5_create_open 2)。tech-selection v1.23、stage_spec slice 3a 段同步。slice 3b(pg-engine first_page 登记)为 Stage B 最后一块 |
| v1.23 | 2026-09-16 | M5 Stage B slice 3b 落码登记(Stage B 落码面齐):**pg-engine first_page 登记三函数**——create_hnsw_index(ddl_lock + pg_am_hnsw::index::create + **只写 pg_rust_relpages 一行** `(oid, meta_page, meta_page, 1)`，最小登记口径实录：无 pg_class/pg_attribute/pg_index,M5 无 SQL 面，孤行被 registry 重建跳过策略兼容）;hnsw_index_first_page(**heap AM 扫描** pg_rust_relpages——初版用 Catalog 内存缓存，create 后立查为 None：缓存只在 engine open 加载一次，改扫描恒新鲜）;open_hnsw_index（薄封装，warnings 经 OpenOutcome 透出）;EngineError::Hnsw(#[from] pg_am_hnsw::HnswError) 变体，pg-am-hnsw lib.rs 补 NeighborSelection 再导出。**引擎重开耐久测试**（tests/m5_hnsw_create_open.rs 2 枚：create→first_page→Engine::open 重开行仍命中→open 全字段;dim 错硬失败 + ef_search_default WARN 恰 1 透出）。验证：pg-engine 新测试 2/2 绿、pg-am-hnsw lib 127 无回归、clippy -D warnings/fmt/doc 绿;tech-selection v1.24、stage_spec slice 3b 段同步 |
| v1.24 | 2026-09-16 | M5 Stage B 对抗审查回流(agent-23 verdict **PASS 附条件** → 条件项 P3×3 + nano×2 全量修复):P3-1 归档先行的 `From<&MetaParams> for MetaView` 补 impl(validate.rs);P3-2 `dir_entry` 页内容 count 钳制(count > 813 响亮 + 测试);P3-3 open-repair WAL 断言改 decode 逐字段(去一字节 varint 假设);nano×2(create 耐久边界立文、engine.rs 孤行措辞精确化)。stage_spec Stage B 三小节齐(交付内容/trade-off/已知残留)、状态改 ✅ 待 commit;tech-selection v1.25 同步。pg-am-hnsw lib 128 绿、m5_create_open 2 绿、m5_hnsw_create_open 2 绿 |
| v1.25 | 2026-09-16 | M5 Stage B 主线独立复核回流(verdict PASS 附条件 → 条件项修复):**P3-1** read_meta 补三条同班结构校验(max_level ≤ 63、INVALID ⇒ max_level == 0、ef_search_default ≥ m——写侧规则读侧镜像,负例三枚);**nano** check_creation_geometry 的 m ≥ 2 前提立文。重点复核成立:check_dir_chain 四断言对两合法崩溃瞬态不误杀。tech-selection v1.26、stage_spec 主线复核回流段同步。验证:pg-am-hnsw lib 128 绿、两套集成 2+2 绿、clippy/fmt/doc 绿 |
| v1.26 | 2026-09-16 | M5 Stage B 主线复核二轮回流(1 P2 + 1 P3 + 1 nano,逐条坐实):**P2-1(重开悬崖坐实)**:open 修复写 HnswMetaUpdate(123)后、Stage C handler 前,无 checkpoint 的重开以 UnknownRecord 硬失败(redo.rs 空骨架 + RedoRegistry::apply 硬错误,engine.rs:662 传播)——fail-loud 非数据损坏,checkpoint 或 Stage C 落地各自愈;处置 = 登记升级为 stage_spec 已知残留首条"重开限制" + redo.rs 注释前提更正(代码修复 = Stage C 第一个任务,不抢跑)。**P3-1(NodeId 空间护栏)**:链导出 hwm 可超 u32::MAX,Stage C 的 `hwm as u32` 截断即双 NodeId 静默撞车——check_dir_chain 补 check_hwm_node_id_space 一行护栏(边界直测,u32::MAX 过/+1 拒;合成链不可达故因子化)。**nano**:hnsw_index_first_page 的 Snapshot::everything 并发可见未提交行(M5 单线程良性,登记)。tech-selection v1.27、stage_spec 二轮回流段同步。验证:pg-am-hnsw lib 129 绿、clippy/fmt/doc 绿 |
| v1.27 | 2026-09-17 | M5 Stage B 用户终审三轮回流(1 P1 实测复现 + 2 P3 + 2 nano,逐条核实属实):**P1(跨重启 OID 碰撞)**:HNSW 最小登记是全库第一条"OID 落 catalog 页但不在 pg_class"的路径,Catalog::open 的 next_oid 回滚窗防御漏扫 relpages——无 checkpoint 重开后 OID 重发,第二索引按 OID 永不可达(用户实测两 session 同得 Oid(16384));修复 = pg-catalog catalog.rs 的 max_in_use 链补 relpages.rel_oid(一行)+ 回归测试转正(m5_hnsw_create_open 第 3 枚:无 checkpoint drop 重开后 OID 不重发且各自解析各自 meta)。**P3-1**:open 补已发布 entry_point < 链导出 HWM 校验(open 侧实例,hwm 现成;redo 按可求值性约束豁免)+ 单元测试。**P3-2**:meta.rs 的 snapshot_format_version 字面量 1 → 引 `encoding::FORMAT_VERSION` 单一真值。**nano×2 登记**:check_dir_chain visited O(n²)(开工期实测点顺带)、first_page 重复 oid 取首匹配(P1 修复后不再可能,不补码)。tech-selection v1.28、stage_spec 三轮回流段同步。验证:pg-am-hnsw lib 130 绿、集成 2+3 绿、pg-catalog 30 绿、clippy/fmt/doc/check --workspace 绿 |
| v1.28 | 2026-09-17 | M5 Stage B 用户终审四轮回流(P3×2 + nano×1,逐条核实属实):**P3-1** open-repair 目标页补 PAGE_TYPE_NODE 检查(此前唯一不查页型的读取入口;腐坏目录条目指向结构合法 slotted 页会静默发布垃圾入口点)+ 负例(合法 LP + 错页型响亮拒);**P3-2** 三轮 P1 修复的副作用收窄:relpages 绕过 validate_content 且 oid_of 只拒负数,腐坏巨大 rel_oid 可经 max_in_use 毒化 next_oid(checkpoint 永久化 + fetch_add 回绕 + as i64 变负不可开)——check_relpages_oid_bounds 立于 read_validated(超 i64::MAX = Int8 不可往返 = 定义性腐坏,响亮);pg_class 用户行同类暴露为先已存在类,登记不收;**nano 登记** create_hnsw_index 无 catalog-room 预检(2 页创建,预检理由不成立)。探查为净登记 5 项(read_lp 钳制/log_page_init pd_lsn/HnswParams 无旁路/entry_point<HWM 检查/OidCounter 单调性)。tech-selection v1.29、stage_spec 四轮回流段同步。验证:pg-am-hnsw lib 131 绿、pg-catalog 31 绿、集成 2+3 绿、clippy/fmt/doc 绿 |
| v1.29 | 2026-09-17 | M5 Stage B 用户终审五轮回流(P3×1,核实属实):**四轮 P3-2 的守卫是死守卫,边界差一位**——read_relpages 的解码链(int8_col i64 → oid_of)把生产路径的 rel_oid 恒钳在 ≤ i64::MAX,> i64::MAX 的界永不触发;而最坏可达毒值恰是 i64::MAX(max_in_use=i64::MAX → start=2^63 → as i64=i64::MIN → 下次重启 catalog 不可开),四轮测试还把它误钉为 Ok。修复:界改 `MAX_SANE_OID = 1 << 48`(OID 自 16384 起逐个消耗,2^48 之外即定义性垃圾),测试钉反转(i64::MAX 必 Err)。**教训登记**:守卫边界值要用"最坏可达毒值"反向推导并钉进测试。tech-selection v1.30、stage_spec 五轮回流段同步。验证:pg-catalog 31 绿、pg-engine 集成 3 绿、clippy/fmt/doc 绿 |
| v1.30 | 2026-09-17 | M5 Stage B 用户终审六轮回流(1 P1 + 1 P3 + 1 nano,逐条代码实证属实):**P1(open-repair WAL 写序反了,Stage C 会炸)**:修复路径原为先 append MetaUpdate 后 pin_mut——pin_mut 触发到期的 pre-image FPI(ensure_fpi 契约:FPI 必须先于本修改的记录),反序使 FPI 的 LSN 高于 MetaUpdate,redo 先放 MetaUpdate 再放修复前映像、修复被冲掉,且 stamp 的 pd_lsn 滞后于 FPI LSN(pd_lsn 权威违约);Stage B 测不出(123 无 redo handler),Stage C handler 落地后每次崩溃修复都被冲掉。修复 = 重排为 btree write_meta_record 同款(pin_mut → append → apply → stamp),§10.3 立写序明文;回归测试以 checkpoint 前置制造 FPI 到期,数值断言 FPI.lsn < MetaUpdate.lsn 且页 pd_lsn == MetaUpdate.lsn。**P3(max_level 只守 6-bit 格式上界,未守语义上界 l_max(m))**:top_level 由 next_level(m) 抽取,(l_max(m), 63] 定义性不可达——read_meta(m 已校验在手)与 open-repair 发布前(params.m 在手)各补语义上界检查,HnswMetaUpdate 构造器拿不到 m 保持 ≤63;负例(14 > l_max(16)=13 拒)+ 边界(13 过)双侧钉。**nano**:read_meta 文档注释自三轮回流后过时(已加结构检查仍写"NOT structurally checked")——更正并枚举现行检查面。tech-selection v1.31、stage_spec 六轮回流段同步。验证:pg-am-hnsw lib 133 绿、全套件 187 绿、集成 2+3 绿、clippy/fmt/doc 绿 |
| v1.31 | 2026-09-18 | M5 Stage C slice 1(redo handler ×7)落码 + 主线验收回流:redo.rs 七 handler 本体填入 Stage 0 空骨架(三段式:bounded decode → pin+页型检查+pd_lsn 守卫(已应用即跳过不重验)→ funnel 校验 → 原语应用 → stamp max(current, record.lsn));无状态化(仅 RedoContext.buffer_pool + page_allocator,None 硬失败为纵深防御);Engine::open 注册链 Stage 0 已接线,handler 本体落地即自动生效。**Stage B"重开限制"残留闭合**:`reopen_replays_hnsw_records` 引擎重开重放 121–127 转正(五记录 insert 序列 crash-无 checkpoint 重开,目录/节点/meta 三页语义状态逐字段断言)。幂等 N=3 字节全等(七类型)+ 截断前缀确定性(3/5 记录两独立目录字节全等)落定。主线验收补三件:① **MetaUpdate redo 侧补 l_max(m) 语义界**(六轮 P3 的第三写入方闭合——redo 经自指 meta 页读 m,可求值;选型 §10.1 MetaUpdate 项同步:max_level==top_level 明确归审计(目录解析依赖),l_max 界 redo 强制);② DirLink ordinal+1 改 checked_add(bit-rot 头 u64::MAX 不再 debug panic,no-panic 纪律);③ slot_is_occupied 文档精度(tombstoned 亦占用,物理存在性 = 集合断言的最强可求值形态)。**handler 级负例矩阵 7 枚**(每 handler 一条响亮拒绝 + 页面字节不变钉死"拒绝先于变更";构造器已拒类由 pg-storage 记录测试承接,不重复)。验证:pg-am-hnsw lib 137 绿、全套件 191 绿、pg-engine 全量绿、workspace check 绿、clippy/fmt/doc 绿;tech-selection v1.32、stage_spec Stage C 节同步 |
| v1.32 | 2026-09-18 | M5 Stage C slice 1 对抗审查一轮回流(agent-23 verdict **PASS 附条件** → 条件项修复清零):**P2-1(commit 前必条件)** DirAppend/DirLink 持 pin_mut 写守卫时 pin 同页 → 非重入 RwLock 死锁(探针实证挂起;腐坏记录 target_page==dir_tail_page / next_page==old_tail_page;redo 读原始字节构造器不覆盖)——两 handler pin 前各补不等判断 + 负例两枚(自链案经合法记录换 payload 构造,format 漂移则 fragment 断言响亮)。**P3-1 核实为已覆盖**(apply::dir_append 的 page_base/hwm 三路比较即"追加位置精确"的承载,层次明文不双写)。**P3-2** MetaUpdate 同记录自洽 INVALID⟺max_level==0 落于 **decode 侧**(codec 对称:decode 拒绝构造器拒绝的),pg-storage 测试双钉。**外溢登记**:pg-am-btree SplitCopy 同型双 pin_mut 面归 Phase 7a 加固。tech-selection v1.33、stage_spec 审查一轮回流段同步。验证:pg-am-hnsw lib 137 绿(矩阵内 +2 case)、pg-storage lib 209 绿(+1 decode 测试)、集成 3 绿、workspace check/clippy/fmt/doc 绿 |
| v1.33 | 2026-09-18 | M5 Stage C slice 1 主线复核二轮回流(1 P2 属实修复):**P2(meta_view 同页死锁——P2-1 同类面外推)**:四个读 meta 的 handler(121/122/124/127)pin_mut 目标页后由 meta_view 读-pin meta_page_id,腐坏记录 meta_page_id==page_id(构造器只拒 INVALID)即同型死锁——agent-23 的 P2-1 点修未外推到 meta 读取面。修复:`require_distinct_meta` 守卫立于四处 meta_view 调用前 + 负例 4 枚。**教训**:死锁类修复必须外推全部二次 pin 点(grep 级全清单核对),点修必漏同型面。验证:lib 137 绿、clippy/fmt/doc 绿;tech-selection v1.34、stage_spec 二轮回流段同步 |
| v1.34 | 2026-09-18 | M5 Stage C slice 1 用户终审三轮回流(2 P3 + 1 连带,逐条核实属实并修复):**P3-1** validate_set_neighbors 补 u32::MAX 邻居拒绝(镜像构造器——redo 读原始字节不经构造器,funnel 不镜像则 INVALID 端点原样写页);funnel 测试补 [1, u32::MAX] 拒绝(其余维度合法,仅该项可触发,fragment 钉)。**P3-2** DirLinkRedo 补"旧尾页已满"检查(dir_count == DIR_ENTRIES_PER_PAGE;未满链接会造出过不了 check_dir_chain 断言 2 的非法链;同页可求值)——**选型 §10.1 DirLink 冻结清单第四项新增,协议修订登记(v1.35)**。**连带**:两条在未满尾页上构造 DirLink 的测试按方案 (b) 重构(手工钉 DIR_OFF_COUNT=813 满页头,不触被测逻辑);负例矩阵补"未满尾页链接"第 14 枚。验证:pg-am-hnsw lib 137 绿、fmt/clippy/doc 绿;tech-selection v1.35、stage_spec 三轮回流段同步 |
| v1.35 | 2026-09-18 | M5 Stage C slice 1 用户终审四轮回流(nano ×3,无 P1/P2/P3,逐条核实属实并处置):**nano 1(负例矩阵两条分支无独立覆盖)**——124 的 LIVE 要求与 127 的存在性证明此前被 dim 前置门掩码(矩阵中 124/127 仅各一枚 dim 错配,正确 dim 下两分支永不触发);补两枚"正确 dim"负例:124 对 INITIALIZING 条目(good NodeInit 未发布,fragment "not LIVE")、127 对空 slot(fragment "not a live entry",entry_top_level 响亮读),均钉死"拒绝先于变更"(页面字节不变);矩阵 14→16 case,同函数字例 lib 仍 137。**nano 2(M6 规划登记一行)**:§10.1 冻结清单 Tombstone"目标存在且 LIVE"意味着 M6 若需回收崩溃遗留 INITIALIZING 孤条目,现行协议下 redo 拒绝该 Tombstone——届时须协议修订(冻结清单改线)或新记录类型,开工前先裁决;stage_spec 交接登记。**nano 3(措辞精度)**:v1.31"引擎重开"闭合为 **StorageEngine 层**(测试用 handler 集与 pg-storage Engine::open 注册链同为 hnsw_redo_handlers(),闭合成立);**pg-engine 层**公共 API reopen-with-HNSW-records 测试须等 slice 3 insert 路径方可经公共 API 构造——slice 3 顺手补一枚,stage_spec 登记。验证:pg-am-hnsw lib 137 绿、fmt/clippy/doc 绿;stage_spec 四轮段同步 |
| v1.36 | 2026-09-20 | M5 Stage C slice 1 用户终审五轮(1 nano,核实属实,**仅登记不改码**):**PublishLive 存在性证明接受 tombstoned 条目**——redo.rs 存在证明走 entry_top_level(物理占用)、publish_live 纯位 OR 无状态前置,严格读冻结清单"state ∈ {INITIALIZING, LIVE}"则 tombstoned 不在集合内;但合法流不可达(Tombstone 前置要求 LIVE、写路径 127 先于任何 124)、apply_tombstone 只 OR bit 7 不清 bit 6(LIVE+tombstoned 条目的"已 LIVE"幂等判定本就覆盖)、M5 只重放不生效语义——处置:不改代码(加检查 = 冻结清单新增项 = 协议修订,越冻结边界),登记入 stage_spec 的 M6 规划条目(Tombstone-LIVE 口径收紧处并案裁决)。零代码变更,无需复跑;stage_spec 四轮段同步 |
| v1.37 | 2026-09-20 | M5 Stage C slice 2(算法核心泛型化,§10.2 任务 3)落码 + 主线验收:GraphAccess trait 四方法(node_count/dist_to_query/dist_between/for_each_neighbor)+ search_layer/select_neighbors 泛型自由函数(逐字节搬运,仅 self.→g./邻居遍历闭包化/selection 参数化)+ Cand 升 pub(crate);Hnsw 两方法改薄封装,签名不变,零公共 API 变更(全 pub(crate))。**零行为变更验收**:既有 191 枚测试一枚未改全绿(137 lib + 3 对拍 + 5 属性 + 2+1+3+40 集成,主线亲跑),clippy -D warnings/fmt/doc 绿。§10.2 任务 1(访问器收口)核实现状:Stage A 已闭合,直接字段索引只剩五个 funnel 本体;insert 的 arena 增长归 slice 3。agent-21 落码、主线 diff 逐行验收;tech-selection v1.36、stage_spec Stage C 节同步。未 commit——等用户终审确认,前缀 PHASE2-M5-StageC |
| v1.38 | 2026-09-20 | M5 Stage C slice 2 对抗审查回流(agent-23 verdict **PASS**,1 nano 当轮已修):for_each_neighbor trait doc 的"按 NodeId 升序"被当轮判为"强于算法所需"而弱化为"枚举序非契约"——**该判断同日终审二轮被证伪推翻(search_layer 的 admit-then-evict 使扩展集对枚举序敏感),此处留存轮次事实,结论以 v1.39 为准**。探查为净:闭包化等价/inherent 优先无虚递归/泛型界恰当/零 API 变更/191 枚行为钉充分。验证:lib 137 绿、fmt/doc 绿;stage_spec slice 2 段同步 |
| v1.39 | 2026-09-20 | M5 Stage C slice 2 终审二轮(1 修正 + 2 nano,逐条核实属实):**修正(推翻同日 agent-23 nano 与主线修法)**——"两核 order-agnostic"断言为假:search_layer 的 admission 把候选同时推进 candidates 堆、evict 只出 results 堆,被逐出者仍会被扩展,扩展集乃至结果对枚举序敏感;for_each_neighbor 契约改回"枚举序是契约:NodeId 升序(两形态规范邻接序——in-memory 恒排序、页驻经 SetNeighbors funnel 校验升序写入;零成本满足,跨形态结果恒等可证)";v1.38 与 stage_spec slice 2 段的假断言登记已同步更正(留存轮次事实,结论以本行/终审二轮段为准)。**nano-1**:trait 距离方法补有限性前置条件(Cand::cmp 对 NaN panic;查询经 §5 入口校验、存储向量有限性由写路径/redo funnel/§11.3 审计保证——页驻实现以 trait 契约为准写距离函数)。**nano-2(二次标记)**:tech-selection 版本表 v1.31 行错位(v1.35 与 v1.36 之间)已移回 v1.30/v1.32 之间。验证:lib 137 绿、fmt/doc 绿;tech-selection v1.37、stage_spec slice 2 段同步 |
| v1.40 | 2026-09-20 | M5 Stage C slice 3(insert 写入路径)落码 + 主线验收 + 对抗审查(agent-23 PASS)回流:**PagedGraph**(paged.rs 新建——页驻只读视图 impl GraphAccess,resolve = dir_pages[id/813]+dir_entry 解析缓存,短共享 pin 死锁纪律,trait 有限性前提落地);**HnswIndex::insert**——§8.1 八步序全落(校验→抽层→节点页/目录页容量(init 链+DirLink)→NodeInit→DirAppend→搜索连边(每邻居一条 SetNeighbors 原位覆写含 shrink)→自身各层(§8.1 字面序,空列表零内容不写记录)→MetaUpdate(was_empty \|\| level>max_level)→PublishLive→flush_to 成功边界);每页触碰 pin_mut→append→apply→stamp(六轮 P1 定序)。DirChainInfo 增 pages(去 Copy);open 收口(head_cache 消二次 pin、末条目定 current_node_page、两个 dead_code expect 摘除);pg-engine hnsw_insert 薄封装。**测试 +6**:跨形态拓扑对拍(N=200 dim=4 + N=900 dim=1 双页溢出,逐字节等价)、首发 WAL 序(四记录 LSN 严格升序)、崩溃续插 level 流逐值一致(编码计划行 4 转正)、坏向量负例状态零变更、pg-engine reopen 经公共 API(nano-3 兑现,m5_hnsw_create_open 3→4)。**顺手项齐**。审查回流:P3-1/P3-2 口径入 §10.2(tech-selection v1.38),nano×2 处置。验证:pg-am-hnsw **196 绿**(142 lib+3+5+2+1+3+40)、pg-engine 4/4、clippy/fmt/doc/workspace check 全绿(主线亲跑)。stage_spec slice 3 段同步。未 commit——等用户终审确认,前缀 PHASE2-M5-StageC |
| v1.41 | 2026-09-21 | M5 Stage C slice 3 用户终审(2 P3 观察项,核实属实并处置):**P3-A** 搜索路径目录损坏 fail-stop panic 接受(M5 utility 范围;fail-stop ≠ 错答案)——paged.rs 模块头前提陈述改精确口径(open 校验链结构+DIR/META 页型,不校验条目目标页型),缺口由 §11.3 审计新增 f 条闭合(目录条目目标 = NODE 页,Stage D 落地),GraphAccess→Result 登记 Phase 4 加固选项。**P3-B** dist_* Vec 分配维持不改码,评估点锐化为 slice 4,Stage E 兜底。验证:fmt/doc 绿、lib 142 无回归;tech-selection v1.39、stage_spec slice 3 段同步 |
| v1.42 | 2026-09-21 | M5 Stage C slice 4(页驻查询 search,§10.2 任务 3 消费面/§11.4"可查询"承载)落码 + 主线验收:**`HnswIndex::search`**——镜像 graph.rs Hnsw::search 语义与错误优先级(dim → §5 入口校验 → ef ≥ k → 空图 → k=0),下降 + level-0 beam 走泛型核 + PagedGraph;ef=None 用 meta 钉死的 ef_search_default;INITIALIZING state 位不进搜索路径(§8.1③ 冻结语义落成,rustdoc 立文)。**P3-B 闭合**(slice 3 终审登记的评估点,结论 = 做):distance.rs 三度量迭代器变体(位级一致契约 = 同分量序/同 f64 提升/同累加器序列,to_bits 钉死;只校形状、有限性按 hot path 前提不重复,与 GraphAccess trait 契约呼应)+ `Metric::distance_iter` 单点分发;PagedGraph::dist_to_query 零分配、dist_between 2→1 分配(死锁纪律禁双 pin);unsafe 切片重解释方案否决。**测试 +6**(lib 142→148):跨形态 search 逐位对拍(L2/Cosine/IP × dim4/N200 + dim1/N900 双溢出,查询网格 4×3×4,Outcome 对拍——结果逐位等或错误变体同)、错误变体对拍、INITIALIZING 残态召回钉、空图/k=0 早退、distance 位级钉×2;pg-engine `hnsw_search` 薄封装 + 崩溃前后位级稳定测试(m5_hnsw_create_open 4→5)。验证:pg-am-hnsw **202 绿**(148 lib+3+5+2+1+3+40)、pg-engine 5/5、clippy -D warnings/fmt/doc/workspace check 全绿(主线亲跑)。tech-selection v1.40、stage_spec slice 4 段同步。未 commit——等用户终审确认,前缀 PHASE2-M5-StageC |
| v1.43 | 2026-09-21 | M5 Stage C slice 4 对抗审查回流(agent-23 verdict **PASS**,1 P3 + 1 nano 当轮闭合):**P3-1(错误优先级序只被单故障弱钉)**——Outcome 对拍的 (Err,Err) 只比 discriminant,复合故障下同变体不同消息可照绿(优先级序仅靠直读承载);`paged_search_error_parity` 补三枚复合故障用例(dim+ef<k / NaN+ef<k / NaN+dim),断言同变体 + 消息含首失败检查 fragment("components"/"non-finite")。**nano**:跨引擎句柄混用无防线(engine_B.hnsw_search(&index_from_A) 读错页,大概率响亮 Corrupted)——hnsw_insert/hnsw_search rustdoc 补"句柄不跨引擎"前提(与 BTreeIndex 同惯例),M5 范围不做防线。agent-23 自我更正登记(slice 2 终审"枚举序非契约"建议已被 v1.37/v1.39 终审正确推翻,留存轮次事实)。验证:lib 148 绿、pg-engine 5/5、clippy/fmt/doc 绿(主线亲跑);stage_spec slice 4 段同步 |
| v1.44 | 2026-09-21 | M5 Stage C slice 4 主线终审(二审,与 agent-23 攻击面不重复):**多 seed × 多度量 × 重开后 search 对拍探针**(临时集成测试,公共 API only,跑完即删)——4 seed × {L2 dim=4、Cosine dim=6、InnerProduct dim=5} + 4 组极端 m=2,N=300–400,每配置 54 查询点(6 查询 × k{1,5,20} × ef{None,k,2k+3})双断言(崩溃前页驻 vs 内存逐位 + forget 崩溃重开后 vs 崩溃前逐位),**864 对比较零分歧**——钉住的两枚固定 seed 之外无侥幸,且 slice 1 redo 重建页 × slice 4 搜索路径的正交面经公共 API 闭合。探查为净:腐坏邻居 id ≥ hwm 的 visited 位图越界 = 索引 panic,与 P3-A 同类的 fail-stop(合法流不可达:写路径选值恒 < hwm、redo funnel 校验被引 id < HWM);隐藏高层节点(max_level 滞后)下降区间以 meta 为准,与 §8.3① 弱化不变量一致;孤儿条目/幽灵映射不可达不污染答案(§8.3③);cosine 存储侧零向量在合法流不可达(插入入口 + NodeInit redo funnel 双闸);hwm as usize 的 64 位前提与内存形态同,非新面。验证:探针 1/1 绿(31.7s);stage_spec slice 4 段同步 |
| v1.45 | 2026-09-21 | M5 Stage C slice 4 终审二轮(组合态攻击面,与一轮不重复):**三枚新探针钉全绿**(临时集成测试,公共 API only,跑完即删):① **崩溃→重开→续插→搜索跨形态 parity**(150 insert → forget 崩溃 → 重开续插 150 → 与从未崩溃的内存孪生 45 查询点逐位对拍——slice 3 skip-ahead × slice 4 搜索的混合重放+新写图交集,committed 测试未覆盖);② **search WAL 静默钉**(搜索前后 WAL 记录数不变——只读契约);③ **空图错误优先级**(ef<k 先于空图早退,两形态同变体同消息)。首轮红为探针自身 bug(ef=None 默认 8 < k=20 网格点误用 unwrap,两形态同错属合法网格点),Outcome 对拍修正后全绿;非代码缺陷。验证:lib 148 绿(主线亲跑);stage_spec slice 4 段同步 |
| v1.46 | 2026-09-21 | M5 Stage C slice 4 终审三轮(用户终审 P3-nano,slice 3 遗留,第三次标记,核实属实顺手闭合):**同句柄 insert-Err 复用使 rng 流位超前 hwm 一格**——步骤 0 入口校验失败不抽层(零变更,既有负例钉),但步骤 1 抽层后的失败(页分配/WAL append/apply/flush)使 rng 消费一格而 hwm 不动;同句柄重试抽下一档——层分配合法但偏离"从未失败"参照流(内存孪生校验后无失败点,无此面);重开经 skip-ahead(恰 hwm 次)重新同步。处置:insert rustdoc 补 Err-after-draw 段立文(纯文档,零代码变更)。验证:doc 绿、lib 148 无回归;stage_spec slice 4 段同步 |
| v1.47 | 2026-09-21 | M5 Stage D slice 1(§11.3 审计 + ④ 补漏)落码 + 主线验收:**audit.rs**(pub)——不信句柄缓存全量重推导(meta 重读 + check_dir_chain 复用 + 条目逐条重读),断言 a–f 齐(a 前向占用+端点 <hwm、b 层级归属、c entry_point<hwm + 弱化 max_level 不变量(隐藏高层计数不拒绝)、d/e 归约为映射唯一性+占用性、f 目标页型),度数 cap 经 checked_count 顺带承载,§8.3⑤ 幽灵/孤儿/tombstone 统计不阻断;**④ txn_id redo 前置门补漏**(require_utility_txn 单点 ×7 handler,拒绝先于 decode 与页触碰——本计划曾虚报"④ 已在 Stage C",实未落地,实现缺口本轮回合,非协议修订);apply.rs 补 slot_count 读器;HnswIndex::audit 薄方法。**测试 +5**(lib 148→153):干净图精确报告/空图报告(entry_point=INVALID、计数全零)/孤儿幽灵分类计数/九连负例矩阵(fragment 钉死)/redo txn_id 七类型负例。验证:pg-am-hnsw **207 绿**、pg-engine **147 绿**无回归、clippy/fmt/doc 绿(主线亲跑);tech-selection v1.41(含 §11.3 d/e 可求值性归约的口径收窄登记)、stage_spec Stage D 节同步。未 commit——等用户终审确认,前缀 PHASE2-M5-StageD |
| v1.48 | 2026-09-21 | M5 Stage D slice 1 对抗审查回流(agent-23 verdict **PASS**,2 P3 当轮闭合,均为一行级口径立文):**P3-1(消费点未立文)**——audit_index 对 open-repair 前态(§8.2 6–7 窗:hwm>0 ∧ entry_point=INVALID)会被 c 条拒绝,而该残态合法;设计层序 redo → open(修复)→ audit 下无害,但 pub API 对"恢复了没 open 过"的索引会误拒——模块头 + audit_index rustdoc 补消费前提立文。**P3-2(孤儿扫描低估口径)**——只扫目录引用页:3–4 窗孤儿若独占新分配页则漏计(低估方向,喂 M6 vacuum 优先级);字段 rustdoc 补 referenced-pages-only 口径,全页分配器扫描评估后登记不做(审计走索引不走存储文件)。nano×2 探查为净(负例九连各自只触发目标分支、txn 门七点齐且在 pd_lsn 守卫前不影响幂等、slot_count 钳制与 read_lp 同纪律、干净图 hidden==0 推理成立、d/e 归约论证成立)。验证:lib 153 绿、clippy/fmt/doc 绿(主线亲跑);stage_spec slice 1 段同步 |
| v1.49 | 2026-09-21 | M5 Stage D slice 1 主线终审(与 agent-23 攻击面不重复——攻跨模块契约引用):**1 P2 属实修复**——graph.rs GraphAccess trait 契约(slice 2 终审 nano-1)明文"存储向量有限性由写路径/redo funnel/(§11.3 open 后审计)保证",而 audit 实现只读 state 字节与邻接,**从不校验向量内容**——引用为虚;页无 checksum,bit-rot 的 NaN/±inf 分量会直达搜索路径 Cand::cmp 的 panic。修复:audit pass 1 补存储向量校验(全分量有限 + cosine 零向量拒绝,§5 入口规则的存储侧镜像;页已 pin,边际成本 = 内存带宽扫描),引用自此为真。**测试 +4**(lib 153→157):NaN 注入拒绝(bit-rot 注入经 pg-storage LP 布局属主解码,不重推导)、cosine 零向量拒绝、**弱化不变量的接受侧**(合法 6–7 窗隐藏高层节点 audit 通过 + hidden 计数——此前只钉了拒绝侧)、**孤儿扫描口径钉**(未引用页上的孤儿计 0——P3-2 登记口径的行为钉)。验证:全套件 211 绿、clippy/fmt/doc 绿(主线亲跑);tech-selection v1.42、stage_spec slice 1 段同步 |
| v1.50 | 2026-09-21 | M5 Stage D slice 1 主线终审二轮(攻击面:状态机合法性 + 新代码判别力):**1 P3 属实修复**——audit 对 tombstoned 条目只计数不拒绝(一轮登记口径),但 `tombstoned ∧ ¬LIVE` 组合在任何合法记录流下不可达(Tombstone funnel 前置要求 LIVE、PublishLive 只置 LIVE 位、INITIALIZING 不可被 tombstone——M6 语义下同),纯页腐败才产得出;audit 补状态机合法性拒绝(Corrupted,非计数)。**测试 +1**(lib 157→158):tombstoned-but-not-LIVE 负例(经纯应用原语注入,funnel 前置不入 apply 层)。**探查为净**:hwm 的 u32 截断面不可达(链导出 hwm 受真实页数约束,check_dir_chain 的 ordinal 连续断言逼腐败者真供 40GB 目录页);邻居列表重复 id 被严格升序检查覆盖;u32::MAX 邻居被 a 条覆盖(nb ≥ hwm);孤儿条目不校验向量(不可达即无关,口径一致);meta/dir 页互指的链型腐败被 check_dir_chain 页型断言覆盖;④ 门与 FPI 记录无交集(FPI 非 HNSW handler 类)。验证:全套件 **212 绿**、clippy/fmt/doc 绿(主线亲跑);tech-selection v1.43、stage_spec slice 1 段同步 |
| v1.51 | 2026-09-22 | M5 Stage D slice 2(mem::forget 窗口矩阵)落码 + 主线验收:**设计裁决登记**——§11.1/:231 的 InsertState stepper 是条件句("多步协议若需暴露内部步骤");HNSW insert 刻意单体(§8.4 不立 CLR/tracker 等价物),改采**故障注入栅栏**:index.rs 加 ProbeMark 九变体(doc hidden)+ ProbeState(RefCell:marks/crash_after/logging)+ probe_* 访问器 + barrier(到界先 flush_to(lsn) 再 Err "crash probe"——崩溃可见性=持久前缀;LsnClock::current() 返回下一未分配 LSN=末记录末尾偏移,flush_to 取等合法),insert 的 9 类追加边界、10 个插桩点(NodePageInit 两个分支各一),记录序列与语义零改动(alloc_* 签名不动)。**矩阵抓到真 bug**:open() 的 current_node_page 解析用"链尾页+(hwm-1)%813",空链尾残态(§8.2"2 后 3 前"窗口)下 dir_entry 越界、open 响亮失败;修为 pages[(hwm-1)/813](与 PagedGraph::resolve 同公式,注释立文)。**tests/m5_insert_crash.rs 新建**:13 枚 = §8.2 表 10 行(1–2 行拆节点页/目录页孤儿两枚、6–7 行拆隐藏高层/首节点修复两枚)+ probe_log_shape(seed 0xC0FFEE 前两抽 [0,0] 的精确 mark 序列钉)。模式:目录 A 探针发现(全程 marks)→ 目录 B 同序重放(逐 insert marks 相等 sanity)→ crash_after → mem::forget(engine+index)→ redo 重开 + open + §11.3 audit → 行断言(孤儿/幽灵/hidden 计数、NodeId 复用、search 逐位稳定或合法性零容差距离断言)→ 二次重开重放幂等(报告相等)。验证:全套件 **225 绿**(lib 158 + 13 新,813 填充两测含于 ~22s)、clippy -D warnings / fmt / doc 绿(主线亲跑);stage_spec Stage D 节同步。未 commit——等用户终审确认,前缀 PHASE2-M5-StageD |
| v1.52 | 2026-09-22 | M5 Stage D slice 2 对抗审查回流(agent-23 verdict **PASS**,1 P3 当轮修复 + 2 nano):**P3-1(生产路径 marks 无界增长)**——barrier 在 crash_after=None 时仍 borrow_mut+push,长寿命句柄 1M insert ≈ 16–24MB 常驻,与"生产零行为变化"声称不符;修复:ProbeState 加 logging 门(默认关),barrier 在"未布防 ∧ 未开日志"时纯 no-op 分支返回,probe_set_logging 供测试发现模式——声称由虚转真。**nano①**:RefCell 使 HnswIndex 变 !Sync——struct rustdoc 点名(Send 保持,与 §8.4 单线程前提一致);**nano②**:OpenOutcome 无显式修复信号(可见状态即信号)登记为设计选择。**探查为净**(下轮勿重复):open() 修复公式在 check_dir_chain 不变量下索引恒在界内(含空尾/hwm=0/腐败链兜底);barrier borrow_mut 作用域先于 flush 释放无重入;13 枚与 §8.2 表逐行对应且 victim_pick 非平凡;recover_twice 报告相等为确定性断言(append 同步 write,fsync 仅 OS 崩溃语义——同进程二次重放必见首轮修复记录;v1.53 终审修正);DataDirLock reclaim 分支放行同进程 stale lock,13 枚同进程测试互不干扰。验证:lib 158 + m5_insert_crash 13 绿、clippy/fmt/doc 绿、全套件 225 复跑绿(主线亲跑);stage_spec slice 2 段同步 |
| v1.53 | 2026-09-22 | M5 Stage D slice 2 主线终审(攻击面与 agent-23 不重复:harness 级 WAL 并发面 + 窗口覆盖完备性):**两项探查为净**——① mem::forget 泄漏 engine 的 WAL worker 与二次重开 writer 无竞争:append 在状态锁内同步写字节(wal/writer.rs:266),worker 只 fsync(dup 句柄,作用于 inode),泄漏 worker 至多幂等 fsync;② "8 后 flush 前"非窗口表缺行:崩溃可见性=持久前缀(§8.1②),PublishLive 未持久 ⟺ 7–8 行残态,被 window_meta_before_publish 吸收。**措辞修正一处**:v1.52"recover_twice 已/未 flush 两路径"不精确(①下同进程重放必见修复记录,断言系确定性),本行与 stage_spec slice 2 段同步改正。零代码变更;lib 158 + m5_insert_crash 13 绿(主线亲跑) |
| v1.54 | 2026-09-22 | M5 Stage D slice 2 主线二审 + agent-23 对抗审查二轮(均 PASS,共 2 P3 + 2 nano 当轮闭合):**主线二审**——① P3:discover 的目录 A 泄漏(`let _ = crash(lab)` 丢弃路径,12 枚窗口测试各漏一个完整数据目录);修复:Discovery 携带 dir 字段、run_window 采收后即清。② nano:recover() 静默丢弃 open 的 warnings;补 is_empty 断言(测试配置与创建参数恒匹配,WARN 即失败)。**探查为净**:崩溃受害者的抽层不落盘,重开 skip-ahead 恰 hwm 次,续插重抽同层——与"未发布=未分配"一致;barrier 仅在触发时 flush(discovery 不拖慢);m=2 下 entry_size 无溢页面。**agent-23 二轮**——① P3-1(续插后无二次审计):orphan_node_page/orphan_dir_page/empty_linked_tail/orphan_entry 四枚续插后只钉 NodeId 复用;补续插后 audit 钉(node_count/live==victim+1、initializing==0、hidden==0、orphan_entry 分别 0/0/0/**1**——孤儿条目在续插后仍存留被计,§8.3⑤ 记账钉死)。② P3-2(edge_count 零断言):run_window 在 phase B 受害者前采 pre_report 审计基线,edge_delta 辅助(反向边 +1、shrank 净 0——受害者入边+逐出旧边),ghost_mapping(Δ=0)/partial_backward_edges(前缀恰 1 边)/shrink_interleave(前缀 #边−#shrink)/own_lists_empty(全部反向边、无自身层)四行钉精确 edge_count;hidden_high_level/meta_before_publish 含自身层写入(mark 不带列表长度)登记不钉精确值。**nano×2**:fresh_dir 不清理前次被杀运行的同名残目录(pid 复用×杀进程×同名 tag,概率可忽略,登记);probe_log_shape 的反向判别力 = 协议形状钉(步骤序/记录集合变更必红,语义相容变更照绿,既定口径)。验证:m5_insert_crash 13 绿、clippy/fmt/doc 绿(主线亲跑);stage_spec slice 2 段同步 |
| v1.55 | 2026-09-22 | M5 Stage D slice 2 终审三轮(用户审查回流,**1 P1 系统性预存属实修复** + 2 nano):**P1(flush_to 边界语义偏一)**——证据链逐环亲核属实(flush_to 文档承诺/早退条件 synced_lsn>=lsn/波次 cover_lsn=lsn_clock.current()/重开播种 synced=末完整记录末尾/append 返回记录起始/write_all 纯页缓存);受影响面盘清:commit/abort(manager.rs)、逐出守卫(buffer_pool.rs)、CheckpointEnd(checkpoint.rs)、insert 成功边界(index.rs)、probe barrier——真 OS 崩溃窗内 commit 记录可丢(恢复判 aborted 而客户端已见 committed),同进程/子进程 harness 不可达故历轮未现。修复 = 全库统一末边界(逐点明细见 tech-selection v1.45);新增钉测试 flush_to_start_boundary_does_not_cover_the_record(长 timeout+大 batch 排除 worker 竞态,确定性双钉:起始 flush 早退不覆盖、末边界 flush 覆盖)。**裁决登记**:txn/eviction 两跨 crate 点本轮同修不推迟(严格增强 + 全量回归兜底)。**nano×2**:stage_spec 测试行数 756→876(v1.54 增补后未更新);"9 个 WAL 追加点"计数修正为 9 类边界/10 插桩点(NodePageInit 两分支)。验证:pg-storage wal::writer 17 绿(含新钉)、pg-txn commit_hard_order 4 绿、pg-am-hnsw lib 158 + m5_insert_crash 13 绿、fmt 绿(主线亲跑);全 workspace 回归亲跑;stage_spec Stage D 节同步 |
| v1.56 | 2026-09-23 | M5 Stage D slice 3(SIGKILL 崩溃轮次)落码 + 主线验收 + 对抗审查(agent-23 verdict **PASS**,1 P3 当轮闭合 + 2 nano):**crates/pg-engine/tests/m5_hnsw_crash_rounds.rs 新建**(m2b_crash_rounds 模式:父 spawn 测试二进制自身为子(M5_CRASH_CHILD=1 门控),子跑确定性 insert 流(120+below(60) 次、每 47 次一 checkpoint、返回 NodeId 稠密钉),每次 insert Ok 后原子重写 expectation.txt(MODE/OPS/OID);偶数轮 kill 于 ready-to-die 后 = full 精确模式(计数全等 + 零残态 + **内存 Hnsw 孪生位级比对**:4 查询 × k{1,5,15} 的 (NodeId,dist.to_bits()) 序列逐位等),奇数轮杀在 OPS ≥ 30+(round%60) 进度点 = mid 前缀耐久模式(live/hwm ∈ {n,n+1}、残态各 ≤1、一致性钉 ghost⟹hwm=n+1 / orphan⟹hwm=n / hidden⟹ghost;零残态仍做孪生比对,有残态只做功能 sanity——幽灵半边连边合法偏离干净重放,注释立文)。selection 按种子奇偶交替 Heuristic/Simple 双路径覆盖;向量流序无关纯函数 vector_at(seed,seq) 父子逐字节一致。**主线实测**:25 轮 74.7s 全绿;13 奇数轮残态分布 = 10 幽灵 + 1 零残态 live=n+1(expectation 竞态形态)+ 2 干净;orphan/hidden 未命中(窗口窄,slice 2 矩阵已确定性覆盖,如实登记)。**agent-23 P3-1(时序论证环境相关)**:模块头"several fsyncs"不精确(实为 2 次:WAL flush + expectation sync),{n,n+1} 界成立前提 = 单次 insert+expectation 周期 > 2ms 轮询窗——真实磁盘恒成立,亚毫秒 fsync 环境(tmpfs/极快 NVMe)可 flaky;模块头改精确口径立文(m2b 同前提,接受)。**nano×2**:sync_all 与 rename 之间被杀留 expectation.tmp 残渣(父只读 .txt,无害,注释立文);子进程 stdout/stderr 置 null 的调试体验(m2b 同形,登记不改)。**探查为净**(下轮勿重复):零残态⟹完整前缀(八步序任何中点必留三类残态之一)、full 模式 hidden==0(was_empty 首节点 + checkpoint 刷盘两形态恒成立)、tmp+rename 原子性(SIGKILL 语义下目录项页缓存可读,缺目录 fsync 仅 OS 崩溃相关)、孪生前提(skip-ahead 按 hwm 重推,checkpoint 只截回放窗口不动 rng 流)、NodeId 稠密钉无误杀面、target 89 < 最小 op_count 120。CI 注册核对:ci.yml 无专用 job,随 pg-engine 既有 test matrix 以 25 轮默认跑(:291 事实口径,m2b 同路径),无需改动。验证:25 轮亲跑绿(74.7s)、fmt 后 4 轮复跑绿、clippy -D warnings / fmt 绿(主线亲跑);stage_spec Stage D 节同步。未 commit——等用户终审确认,前缀 PHASE2-M5-StageD |
| v1.57 | 2026-09-23 | M5 Stage D slice 3 用户终审四轮(外部 review verdict FAIL,**1 P1 + 2 P2 + 2 P3 全部属实**,当轮闭合):**P1(偶数 full 轮并未 SIGKILL 活引擎)**——`run_child` 返回后才写 ready-to-die,Engine/HnswIndex 在作用域结束时析构,`DataDirLock::drop` 删锁(外部探针实证:marker 存在、子进程活着、lock 已释放),12 个偶数轮实际杀的是"引擎已拆除"的进程,削弱覆盖;修复:marker+无限等待移入 `run_child`(`-> !` 契约立文),并补**锁存在性钉**(kill 后 lock 必须在——SIGKILL 下无 Drop 运行,锁在 = 杀的是活引擎,该断言使 P1 形态永不可回归)。**P2-a(v1.55 修订记录被覆盖)**:v1.56 落码时 Edit 误替换而非追加,v1.55(flush_to P1 修复史)丢失;从 git 恢复 v1.55 行并重新追加 v1.56,无信息损失。**P2-b(checkpoint 中途崩溃"实证"不成立)**:round 17 的 target=47 恰等于 checkpoint 点,但无 checkpoint 内 marker,kill 落在前/中/后不可判定;stage_spec 措辞改为"邻域命中",删除 Begin/End 之间实证声称(不加 checkpoint 内故障注入——窗口矩阵归 slice 2 职责)。**P2-c(M5_CRASH_ROUNDS=0 静默绿)**:0 轮 0.00s 全绿,vacuous pass;改响亮失败(非法值 panic + rounds ≥ 1 断言),与 m2b 先例分岔处登记。**P3-1**:verify_round 注释声称 warnings 空钉但代码 `.index` 丢弃 warnings——补显式 is_empty 断言。**P3-2**:行数引用 488→520。验证:全量 25 轮复跑绿(主线亲跑);stage_spec 四轮段同步 |
| v1.58 | 2026-09-23 | M5 Stage D slice 4(§11.3 ② 恢复后召回率门)落码 + 主线验收 + 对抗审查(agent-23 verdict **PASS**,1 P3 当轮修复 + 1 P3 登记 + 2 nano):**tests/m5_recall_after_recovery.rs 新建**——三阶段崩溃形态:建半段 5000 → mem::forget 崩溃 → redo 重开 → 续插至 10000(NodeId 稠密钉,skip-ahead 重同步)→ audit 精确钉 → 采崩溃前查询答案 → 再崩溃 → 重开 → audit 精确钉(10k、零残态)→ 100 查询逐查询 (NodeId,dist.to_bits()) **位级相等** + recall@10 ≥ 0.98(冻结 M4 参数,与内存门同参可比)。**实测:post-recovery recall@10 = 0.9990,与 M4 内存门逐位同值**——恢复对检索质量零损耗,位级钉使然。常驻伴随 synthetic(800×8d,0.9700 ≥ 0.90)保证无数据集矩阵 job 不做纯 skip 占位;门控 = recall_siftsmall 严格触发模式逐字复刻(NotUnicode 响亮 panic)。**CI 注册落地(清单 #11)**:recall-gate job 加第二步,与 recall_siftsmall 同 job 同触发(YAML 校验通过)。**agent-23 P3-1(临时目录不清理)**:每次运行在 /tmp 累计 ~40MB;修复:phase 3 末 shutdown + remove_dir_all(实测零残留)。**P3-2(证明结构非对称,登记不改码)**:pre_crash 答案采于第一次恢复后的索引,位级相等证"第二次恢复幂等",第一次恢复的正确性由 audit + recall 阈值背书(≤1.9pp 松弛内的稳定劣化理论在外;实测 0.9990 逐位锚定,收紧阈值引 flaky 风险,否决)。**nano×2**:M4_REQUIRE_DATASET 非 "1" 值静默 skip(与 recall_siftsmall 既有口径逐字一致,登记);"恢复过程自身被杀"(kill-during-redo)为未覆盖盲区,登记为后续候选(不加)。验证:release 两测全绿 88.9s(recall 数值亲验)、fmt/clippy 绿(主线亲跑);stage_spec slice 4 段同步。未 commit——等用户终审确认,前缀 PHASE2-M5-StageD |
| v1.59 | 2026-09-23 | M5 Stage D slice 4 主线终审(用户请求 review,攻击面与 agent-23 不重复):**P3-2 实质闭环(agent-23 登记项)**——pre_crash 答案采于第一次恢复后的索引,原证明结构只钉"第二次恢复幂等";补**未崩溃孪生位级钉**(phase 3 末:内存 Hnsw 同参数同种子重放全量向量流,全部查询与恢复后答案逐位等)——两次恢复自此都锚在从未崩溃的参照上,且在 **10k 真实数据规模**上首次实测跨形态位级 parity(此前最950/150 节点)。**实测:孪生钉全查询零分歧,recall 仍 0.9990**。**debug 档计时实测(新数据)**:两测 349.9s(孪生钉再加约 1 分钟)——本地带数据集跑 debug 全量套件的成本;CI 矩阵无数据集必 skip、recall-gate 走 release,不受影响;register 不改码(m2b crash rounds 25 轮 debug 同量级先例)。**nano**:phase 2 续插后的 audit 只钉 node_count 未钉全套(残态由 phase 3 的 recover 全钉兜底,登记)。验证:release 两测全绿 114.4s(含孪生钉)、debug 两测全绿 349.9s、clippy/fmt 绿(主线亲跑);stage_spec slice 4 段同步 |
| v1.60 | 2026-09-23 | M5 Stage D slice 4 终审四轮(用户请求再 review,主线独立审 + agent-23 第四轮对抗审查,双 verdict **PASS**,零 P1/P2/P3,3 nano 登记不改码):**nano①**——位级钉比较的是查询输出 (NodeId,dist.to_bits()) 而非全图拓扑;一个"过 audit、不改任何查询答案、recall ≥ 0.98"的边级劣化理论可存活,由审计(结构)+ recall(质量)+ Stage C slice 3/4 拓扑对拍(N=200/900 位级)组合兜底,本文件定位(§11.3 ② 召回门)下接受,10k 全拓扑对拍为可选增强(不要求)。**nano②**——phase 3 末清理(shutdown + remove_dir_all)只在成功路径执行;断言失败时 panic 跳过清理、/tmp 残留 ~40MB——失败调试反而需要现场,登记为有意行为,不改。**nano③**——ci.yml recall-gate job 无 timeout-minutes(GitHub 默认 360min ≫ 新增 ~2min,既有形状沿用);且 M4 门第一步失败时 M5 第二步被默认 if:success() 跳过,失去 M5 门诊断信号(job 已红,无假绿)——可选 if: always(),登记非必须。**第四轮亲验攻击面为净(下轮勿重复)**:假绿面(bits() 精确比较无退化相等路径、阈值方向、无早退、"四次恰同值"是确定性预期非未执行证据);孪生钉同参同种子由共享 Cfg 值对象代码级保证(非口头声明),ef 两侧均显式 Some(64);mem::forget 可见性模型与进程崩溃一致(成功边界 flush_to 使字节到 OS);两测试 tag/COUNTER/pid 隔离;步骤级 env 与 job 级 env 合并不遮蔽;文档(v1.59 + stage_spec 三段)与 371 行实现、ci.yml、实测数字(0.9990/0.9700、release 88.9~114.4s、debug 349.9s)逐字一致无漂移。验证:无需复跑(零代码变更);stage_spec slice 4 段同步 |
| v1.61 | 2026-09-23 | M5 Stage D slice 4 用户终审五轮(1 P3,核实属实当轮闭合):**P3-1(synthetic 真值用本地重复实现)**——m5_recall_after_recovery.rs 的本地 `l2_squared` 注释自称"crate 方法私有故重写",但只对 enum 方法成立;`pg_am_hnsw::distance::l2_squared` 本身是 pub 自由函数(lib.rs:68 `pub mod distance`、`Metric::L2` 委派的正是它),可直接导入。两份实现今日位级同(均 f64::from 逐分量 + d*d + 顺序 f64 累加),但 crate 累加纪律将来变动时 synthetic 真值与索引实际度量会静默分叉、只靠 0.90 松阈值兜住。修复:删本地 fn,改 `use pg_am_hnsw::distance::l2_squared` + `.unwrap()`(附带白得 validate_pair 的形状/有限性校验),注释改为"用 crate 自己的函数,真值永不分叉"。验证:fmt / clippy -D warnings 绿、release 两测复跑(主线亲跑);stage_spec slice 4 段同步 |
