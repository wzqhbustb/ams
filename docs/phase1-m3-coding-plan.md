# Phase 1 M3 编码顺序

> 基于 `docs/phase1-m3-tech-selection.md` v1.7（多轮 review / 编号修订见文末记录），按依赖
> 关系排列的 M3 阶段编码执行计划。M3 交付五块内容（对应 ROADMAP.md:203-217）：
> **基础 Vacuum（离线）+ 可观测性 + PG Wire 极简版 + SegmentedStorage 接口预留 +
> Tier 2 接口预留**。每个阶段必须先通过单元 / 集成 / 崩溃测试与对抗性 review，再进入
> 下一阶段。
>
> ```
> 地基     → A (快照注册表) / B (HeapCleanup WAL + §4.6 重构)   8–10 天
> Vacuum   → C (Vacuum 核心) / D (索引清理 + Engine::vacuum)     9–13 天
> 并行支线 → E (可观测性) / F (pg-wire)                         8–13 天（与 B–D 并行；F 含 +1–2 兼容余量）
> 收口     → G (接口预留 + M3 收口)                              3–4 天
> ```
>
> **总计 7 个 stage，串行口径 28–40 天；E/F 与 B–D 并行后实际工期约 5–6.5 周**
> （1 名高级 Rust 工程师）。

---

## v1.3 硬约束速查

M3 主线开工前请通读 tech-selection §2 / §3 / §4 / §11，以下 8 条为**编码期每天都要
对照**的硬性约束（违反则退回该 stage 重做）：

- **§3.2/§3.3（v1.1 六处枚举 + v1.3 原子化）**：快照注册覆盖**全部六处**调用点——
  `begin_txn` / `auto_commit` / 纯 SELECT / `Engine::scan` / `Engine::index_lookup` /
  `create_index` re-snapshot；缺一即 horizon 漏洞（无锁读者在飞时被 vacuum 回收
  正在读的版本）。且**注册必须沉入 `TxnManager::snapshot()` 临界区**（与 active
  set + clock 读取同一把锁），`snapshot()` 返回 `(Snapshot, SnapshotGuard)`；
  "先构造后由调用方包装注册"的 caller-wrapper 形态已否决（B1：构造—注册窗口使
  horizon 漏盖在飞快照，`AccessExclusive` 无法补救，取快照先于任何锁申请）。
- **§3.3**：horizon = registry 最小键；空则取 XID clock 当前值。vacuum 开始时取
  一次，全程使用同一 horizon。安全论证靠 XID 单调性（`新快照.xmin ≥ min(A_vacuum)
  ≥ horizon`），不允许改回"集合包含"式论证（v1.2 已证伪）。
- **§4.1 顺序不变量**：使 TID 失效的 WAL（HeapCleanup / PageFree）必须**晚于**该
  TID 的最后一条 BTreeDelete 落盘。五阶段顺序"收集 → 提取 key（只读）→ 索引清理 →
  页内压实 → 页释放"不可调换；未来任何并行化改造必须重新论证。
- **§4.1 阶段 3 / §4.3**：索引清理的 `EntryNotFound` **必须视为 Ok**（eager 索引
  维护下绝大多数条目早已不在树上）；当错误处理第一次运行就会在正常路径失败。
- **§4.1 阶段 4**：`SlottedPage::compact()` **LP 数组条目一律不移动、不重排**——
  slot 号是 TID 组成部分，被索引条目与 HOT `t_ctid` 引用；压实对象只是 tuple 数据
  区空洞与 dead slot 状态。
- **§4.5**：`HeapCleanup=8` 的 redo handler 与记录**同 stage 交付**（Stage 0 硬失败
  约定：未注册类型 recovery 报错），且 redo 必须调用与在线路径**同一个**
  `compact()`（同参数、dead_slots 升序），禁止另写"等价"重放逻辑。
- **§4.6（R4）**：insert 槽位寻址重构（`first_fit_slot` / `add_tuple_at`）必须
  **先行于** compact 落地并全量回归——否则 first-fit 命中 Unused slot 后 WAL 记录
  的 slot 号与实际写入分歧，redo 端必然 slot diverged。范围不止 insert：**四处在线路径**（insert / HOT update / 同页非 HOT update / 跨页 update）与**三个 redo handler**（HeapInsert / HeapUpdate 同页与跨页两分支 / HeapHotUpdate）全部改为"先选 slot → 写 WAL（slot 随记录承载）→ 按记录落位"；`acquire_page_with_room` 从尾部反向找页，compact 后中部页也会成为 update 新版本落点，同样走该路径。
- **§7.3 / §10**：pg-wire 用 std 线程模型 + 手写协议编解码，**零新运行时依赖**；
  `Engine: Send + Sync` 加编译期断言钉死（O5）。

---

## 工程规则（每 stage 通用，继承 M1/M2 惯例）

- **每 stage 一个 commit**：message 前缀 `PHASE1-M3-StageX`；**未经用户确认不执行
  任何 git mutation**（commit/tag/reset/rebase 均含）。出口 tag（`phase1-m3`）只在
  用户确认后打。
- **stage_spec 归档**：每 stage 收口时在 `docs/stage_spec.md` 追加该 stage 的
  "交付内容 / 与 PG 的 trade-off / 已知残留与后续归队"三小节（对齐 Stage L–T 的既有
  格式）。
- **对抗性 review**：每 stage 完成后做一轮对抗性 review（红→绿复现优先）。M2 每个
  stage 的 review 与压测都抓到过全绿测试发现不了的真 bug（Stage T 一次抓 5 个），
  M3 维持该门槛，不以"测试全绿"代替 review。
- **watchdog 式并发测试**：所有并发/压测用例必须带超时看门狗——回归以**失败**告终，
  不允许挂起（对齐 Stage T `m2c_stress.rs` 的既有设施风格）。
- **回归传承**：每 stage 出口必须继承通过全部前序回归（M1/M2 全量 + 前序 M3 stage
  新增用例），release 全量在 Stage G 强制，其余 stage 至少 debug 全量。
- **冲突处理**：实现与 tech-selection 引用不符时，以代码为准并回改选型文档
  （tech-selection 文头既有约定）；决策层冲突不擅改，记入"开放问题与冲突标注"。

---

## 阶段 A：快照注册表 + horizon（3–4 天）

**归属**：M3 地基
**前置**：无（M2c 出口 `phase1-m2c` 之上直接开工）
**目标**：把"最老活跃快照 xmin"从推导不出来的状态变成 `TxnManager` 的一等 API，
堵住 §3.1 的无锁读者漏洞；注册与快照构造原子化（B1），注册开销按 S2 测度协议
判定无统计显著回归。

| 任务 | 交付物 |
|------|--------|
| M2c 基线记录（S2） | 开工第一天：按固定负载/固定时长协议（继承 `m2c_stress` 配速模式，N=5 次取平均）实测 M2c churn 基线，记入 `docs/phase1-m2-benchmarks.md`（基线数字的权威落点）；Stage A/D/G 的回归判定均以该实测基线为准 |
| 注册与构造原子化（B1） | `TxnManager` 新增 `snapshot_xmins`（xmin → 引用计数，同 xmin 快照共存），与 active set、XID clock 合并到**同一把锁**的状态内；`TxnManager::snapshot()` 在读取 active set + clock 构造快照的**同一临界区**完成 xmin 注册，签名改为 `snapshot(current_xid) -> (Snapshot, SnapshotGuard)`，guard `Drop` 时计数减一、归零删键。**禁止**"先返回 Snapshot、调用方后注册"的包装形态——构造与注册之间的窗口会让 vacuum 取到漏掉在飞快照的 horizon（反例：U begin(xid=15) 取快照未注册 → vacuum 见空 registry 取 horizon=clock.current() → 删除者 D 提交 → U 必须见的 xmax=18 已提交行被回收；`AccessExclusive` 无法覆盖，因为取快照先于任何锁申请）。tech-selection §3.3 已按 v1.3 同步修订 |
| 反枚举护栏（B1 结构性修复） | `Snapshot` **字段**收为 pg-txn 私有（Rust 无"构造器"，防字面构造必须私有化字段）：`TxnManager::snapshot()` 成为全系统唯一的**注册**构造点——未来任何新调用点不可能绕过注册（v1.1 的 3→6 枚举遗漏类的结构解）。**注意 `Snapshot::everything()`（snapshot.rs:57，pub，xmin=0）是明确的不注册特例**：目录引导（engine.rs:854）与测试路径使用，绝不进 registry（其 xmin=0 一旦注册会把 horizon 钉死在 0，vacuum 永久失效）——实现时两个踩坑方向都要避开：把它一并私有化（引擎 open 断）或让它走注册（horizon 归零）。配套：① 字段私有化的机械重构——engine.rs 约 16 处 `snap.current_xid` 等字段访问改访问器方法（快照字段见 snapshot.rs:28-49）；② `#[cfg(test)]` 全局计数断言"存活 Snapshot 数 == registry 计数和"（everything() 不计入）；③ CI grep 同时覆盖 `Snapshot {` 字面构造与新增的 `Snapshot::everything()` 式关联构造函数（后者应收敛为唯一既有实现 + 显式 allow） |
| 六调用点适配新签名 | 事务级：`begin_txn`（engine.rs:2027，guard 随 `TxnHandle` 存活，`commit`/`abort`/`Drop` 注销，engine.rs:483-540）；语句级 auto-commit：`auto_commit`（engine.rs:2404，**成功与失败两条路径都注销**，engine.rs:2402-2428）、`create_index` re-snapshot（engine.rs:1202，随外层 auto_commit 闭包结束）；无锁读者：纯 SELECT（engine.rs:2129-2133）、`Engine::scan`（engine.rs:1571）、`Engine::index_lookup`（engine.rs:1404）——后三处 guard 随调用帧存活。全部改收 `(Snapshot, SnapshotGuard)` 返回值，生命周期语义不变（v1.1 六处清单仍是完整性检查表） |
| horizon API | `TxnManager::oldest_snapshot_xmin()`：registry 最小键；registry 空则取 **active set 最小 XID**（覆盖 begin→snapshot 窗口），二者皆空才取 `txn_id_clock.current()`（§3.3 v1.6 修正）；`Engine::oldest_snapshot_xmin()` 透传（供 §6.2 自省与 vacuum 使用） |
| panic 泄漏语义（O1） | **决定：不加进程级护栏**。默认 unwind 策略下 panic 会执行 guard `Drop` 正常注销（horizon 不受影响）；仅 `panic=abort`（本项目未启用）或 `mem::forget` 跳过 Drop 使 horizon 永久偏低（vacuum 退化为不回收，安全但失效），与 `auto_commit` 既有 panic 策略（panic 即泄漏 XID/锁，进程级失败）代价一致。语义写入代码注释与 stage_spec |
| 注册覆盖测试 | 六条路径各一用例：语句执行期间 registry 含预期 xmin（无锁读者路径用屏障/barrier 在 scan 进行中观察 registry） |
| 泄漏自由测试 | 每条路径结束后 registry 回到基线（含 `auto_commit` 失败路径、`TxnHandle::Drop` 不 commit 路径） |
| horizon 并发正确性 | 并发快照 + churn 下断言 `oldest_snapshot_xmin() ≤ 所有在飞快照.xmin`；vacuum 期间两类新进入者 horizon 均不破：① 新 BEGIN 进入 active set；② **auto-commit DML**（其 XID 分配与快照先于语句体的锁等待进入 active set/registry——tech-selection v1.4 修正的路径，最易漏测）（§3.3 单调性论证的测试化）；**原子性专项**：压 `snapshot()` 与 `oldest_snapshot_xmin()` 的交错（多线程反复构造快照 + 另一线程连续取 horizon），断言 horizon 永不越过任一已返回但未销毁快照的 xmin（B1 窗口的确定性回归） |

**关键约束**：
- §3.2/§3.3（v1.3）：注册沉入 `snapshot()` 临界区、与构造原子（B1）；
  caller-wrapper 形态否决；六调用点完整性靠反枚举护栏结构保证，不靠人肉枚举
- §3.3：horizon 空 registry 语义（无读者则一切已提交删除都可回收）
- §11 O1：panic 泄漏 horizon 的语义按上表决定落地并文档化
- §11 R2：registry 与 active set/clock 同锁后，临界区变长是新共享热点
  （v1.2 预期 <1%）；判定按 S2 测度协议，超标备选分片 registry
  （xmin 聚合仍取全局 min）

**验收标准**：
- **功能**：`oldest_snapshot_xmin()` 在空/单快照/多快照同 xmin/并发四类场景返回正确
- **正确性**：六路径注册覆盖测试全绿；泄漏自由测试全绿；horizon 并发断言与
  原子性专项（B1）在 watchdog 保护的 100 线程混合负载下全程成立；反枚举护栏
  （构造器私有化 + 计数断言 + CI grep）生效
- **并发**：100 线程短查询 churn，registry 无死锁、无计数漂移（终态归零）
- **性能（S2 测度协议）**：churn 固定负载/固定时长对比——继承 `m2c_stress`
  配速模式，M2c 基线与 A 出口各跑 N=5 次取平均；判定口径为"**无超出运行间
  噪声界（±3%）的统计显著回归**"（原"<1%"字面门槛不可测：criterion smoke 的
  运行间噪声远大于 1%；基线以开工时记入 `docs/phase1-m2-benchmarks.md` 的
  实测值为准）

**验收命令**：
```bash
cargo test -p pg-txn --test snapshot_registry
cargo test -p pg-engine --test m3_snapshot_coverage
# S2 协议：配速模式 ×5 次取平均，对比 docs/phase1-m2-benchmarks.md 基线
M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100 cargo test -p pg-engine --release --test m2c_stress -- --nocapture
```

---

## 阶段 B：HeapCleanup WAL + redo + §4.6 槽位寻址重构（5–6 天）

**归属**：M3 地基
**前置**：A（无硬依赖；排在 A 后串行，隔离性能回归归因）
**目标**：把 vacuum 的 WAL 载体一次到位：记录格式、redo handler、analysis 分类，
以及 §4.6 的前置重构——此后 slot 分配由 WAL 记录显式承载，redo 不再依赖在线
writer 的行为巧合。

| 任务 | 交付物 |
|------|--------|
| §4.6 槽位寻址重构 | `SlottedPage::first_fit_slot(page) -> Option<u16>`（纯读）+ `add_tuple_at(page, slot, bytes)`；`add_tuple` 退化为二者组合、对外行为不变。统一改为**先** `first_fit_slot`（或 `slot_count`）选定 slot → **再**写 WAL（slot 随记录承载）→ **最后** `add_tuple_at` 落位，删除"预测—断言"耦合。**范围 = 四处在线路径**（insert、HOT update、同页非 HOT update、跨页 update；`acquire_page_with_room` 从尾部反向找页，compact 后中部页也会成为 update 新版本落点）**+ 三个 redo handler**（HeapInsert、HeapUpdate 同页与跨页两分支、HeapHotUpdate），redo 全部改调 `add_tuple_at(page, rec.slot_id, ..)`（heap_am.rs:1278-1285 / redo.rs:86-96 仅为 insert 单点示例） |
| §4.6 全量回归 | 行为中性验证：m2b crash rounds + M1/M2 全量回归在重构后全绿（R4 门槛，不过不进 C） |
| `HeapCleanup=8` payload | `(page_id, dead_slots[](升序), chain unlink 信息)`：前驱页 id + 被摘除页的 `next_page` 重链目标，按需扩展（§4.5 允许"M2 规划 payload 上按需扩展"）；encode/decode roundtrip 单测（归 `pg-storage`） |
| `SlottedPage::compact()` 原语 | dead slot LP 置 `Unused`（`delete_tuple` 既有语义）、存活元组字节向页尾连续整理、回收中部空洞、重置 `pd_lower/pd_upper`；**LP 条目不移动不重排**（§4.1 阶段 4）。**前移至本 stage 交付**：§4.5 要求 redo handler 与记录同 stage 且 redo 调用同一 `compact()`，原语必须在 handler 之前存在（见"开放问题与冲突标注"#2） |
| redo handler 注册 | `HeapCleanupRedoHandler`：幂等守卫（`page_pd_lsn >= record.lsn` 跳过）后调**同一个** `compact()`（同参数）；handler 归 `heap_redo_handlers`（**pg-am-heap**，与既有 heap handler 同族），注册进 `RedoRegistry`（硬失败约定：handler 与记录同 stage 交付） |
| analysis 分类 | `analysis.rs` 的 `for_each_touched_page` 把 `HeapCleanup` 归入 DPT 触及页集合（决定 redo 起点覆盖；归 `pg-storage`） |
| 崩溃恢复测试 | 压实 WAL 的 replay 收敛：在线 compact 一次 vs 崩溃后 redo 重放，页字节级一致；同一 record 重放 10 次页不变（幂等）；压实 → 新插入走 first-fit → 崩溃 → redo 按记录 slot 复现（归 `pg-am-heap`，crash harness 场景可借 `pg-engine`） |

**关键约束**：
- §4.5：payload `dead_slots[]` 按升序写入，两侧输入一致则输出一致（重放收敛性）
- §4.5 / record.rs:18-20：未注册类型 recovery 硬失败 → 记录与 handler 不可拆 stage
- §4.6 / §11 R4：重构先行、全量回归通过后才允许 compact 制造 Unused slot
- §4.1 阶段 4：compact 只动数据区与 dead slot 状态，slot 号稳定

**验收标准**：
- **功能**：payload roundtrip；`first_fit_slot`/`add_tuple_at` 单测；`add_tuple`
  对外行为与重构前一致（含满页、空洞页）
- **正确性**：`test_heap_cleanup_redo_converges`（在线 vs 重放字节一致）；
  `test_heap_cleanup_redo_idempotent`；`test_slot_reuse_after_compact_redo`（压实
  产生 Unused → **四条在线路径各自**（insert / HOT update / 同页 update /
  跨页 update 落中部空洞页）走 first-fit → 崩溃 → redo 按记录 slot 复现，
  无 diverged）；
  analysis 对 HeapCleanup 的 DPT 分类正确
- **回归**：m2b crash rounds + 全量回归全绿（R4 硬门槛）
- **性能**：INSERT 路径相对 A 出口无可闻回归（`add_tuple` 组合退化的开销应不可测）

**验收命令**（S4 修正：按 crate 归属拆分）：
```bash
cargo test -p pg-am-heap --test slot_addressing
cargo test -p pg-storage --test heap_cleanup_wal      # payload roundtrip + analysis 分类
cargo test -p pg-am-heap --test heap_cleanup_redo     # redo 收敛/幂等/slot 复用（handler 归 heap_redo_handlers）
cargo test -p pg-engine --test m2b_crash_rounds
cargo test --workspace
```

---

## 阶段 C：Vacuum 核心（5–7 天）

**归属**：M3 Vacuum
**前置**：B（compact 原语 + HeapCleanup redo 就位）
**目标**：heap AM 侧的回收能力完整落地：链分组、key 提取、整链 HOT 回收、页内
压实接线、空页释放。

| 任务 | 交付物 |
|------|--------|
| `Vacuumable` trait 扩展 | `pg-am-heap`（`access_method.rs`，trait 实体所在 crate；pg-catalog 仅 re-export）按 §4.4 形状新增 `collect_index_keys`（只读）与 `reclaim`（纯物理）两个方法；`scan_dead_tuples` 不动。**拆两个方法不改回单一 `reclaim`**——返回列值的单一形状会强制压实先于索引清理，违反 §4.1 顺序不变量 |
| 链分组 helper | heap AM 内部共享 helper：沿 `t_ctid` 走链、成员全落在 dead 集合才算全死链；普通死元组返回自身 (tid, 列值)；全死链返回链根 (tid, 链根列值)；**部分死链一律不返回**（§4.2，不 prune、不重定向）。**出口为二**（Stage C 实施时修正，已记录于 stage_spec 交付内容第 8 条与选型 v1.7 注记）：`collect_index_keys`（只读出口）+ `reclaim` 内部重推导（结构保证"部分死链零回收"，不依赖调用方自律） |
| `collect_index_keys` 实现 | 从元组字节解码索引列值 `Vec<Option<Datum>>`；key 提取路径与 `Engine::delete_inner` 一致（`read_row_by_tid` + `encode_key`，NULL 键跳过，engine.rs:1701/1717/1718）；必须先于一切物理改写（页压实后 key 读不出） |
| `reclaim` 实现 | 按清单杀 slot：调 B 交付的 `compact()`（写 `HeapCleanup` WAL）；压实后全空页改写前驱 `next_page` 从页链摘除（unlink 信息进同条 HeapCleanup payload），调 `PageAllocator::free_page`（自带 `PageFree=41` WAL + freelist，page_allocator.rs:202；`drop_table` 同款先例 engine.rs:1113-1114）；heap AM 内存页列表缓存按单页粒度剔除（参照 `drop_relation` engine.rs:1119） |
| 整链 HOT 回收 | 链上所有版本均死 → 整链全部 slot 进 reclaim 清单（§4.2）；索引条目删链根一条（归 D 的 engine 侧循环） |
| crash-mid-vacuum 测试 | **崩溃窗口②**（§12.1）：压实（HeapCleanup）已落盘、`PageFree` 未落盘时注入崩溃 → 恢复后精确口径断言：**被摘除页脱离页链且未入 freelist（既定单页泄漏，记入 stage_spec 残留——unlink 与 free 是两笔 WAL，窗口内崩溃使该页对一切未来访问不可达；vacuum 走页链看不见它，freelist 也没有它）**；链上遍历与堆扫描与预期一致；`compact` 中途崩溃 redo 收敛（继承 B 的收敛测试到多页场景） |

**关键约束**：
- §4.2：整链回收必做（不做则 HOT 死版本堆空间永远膨胀）；部分死链不 prune
  （LP 重定向是 on-disk 格式变更，归格式演进，见"遗留与归队"）
- §4.4：索引清理不进 AM（`notify_indexes` 不落入 trait）；`Vec<Tid>` 物化沿用，
  在线化时再改迭代器（access_method.rs:158-160 TODO 维持）
- §4.1 阶段 5：空页释放走 `free_page` 既有路径，天然继承 checkpoint LSN 临界区
  修复（§11 R1）
- **释放顺序硬性约束**：unlink（进 HeapCleanup）→ `free_page`（PageFree）的顺序
  **不可反转**——先 free 后 unlink 会在窗口内崩溃后留下"页同时在 freelist 与页链
  上"的状态：页被复用后链上出现活元组，结构性损坏。窗口内泄漏（已摘未放）是
  可接受的既定取舍（见任务表窗口②口径）；记录备选设计：空页保留在链上不摘除
  也满足"空间复用"（`acquire_page_with_room` 反向扫描天然复用链上空页，scan
  快速跳过空页），零崩溃窗口；摘链+释放只是把页还给 allocator 供其他 relation
  复用的优化。要消除窗口只能把 unlink 并进 PageFree 记录（改既有 payload =
  on-disk 格式变更，判定不值，维持现顺序）
- §11 O3：与 M2 文档 §十八 "HOT prune 推迟到 M3" 的表述偏差维持 tech-selection
  决定（只做整链），不启动 LP 状态扩展

**验收标准**：
- **功能**：构造普通死元组 / 全死 HOT 链 / 部分死链 / 空页四类 fixture，
  `collect_index_keys` 输出与预期逐条相等；`reclaim` 后 dead slot 置 Unused、
  空洞回收、空页入 freelist
- **正确性**：部分死链零回收（可见版本与链根原样保留）；空页释放后新插入复用
  该页无串扰；崩溃窗口② 恢复后堆扫描与预期一致
- **并发**：reclaim 持页写 latch 期间并发 `pin` 读者只阻塞不读半压实页
  （§4.1 并发注记；watchdog 保护）
- **回归**：全量回归 + B 的 HeapCleanup 测试全绿

**验收命令**：
```bash
cargo test -p pg-am-heap --test vacuum_chain_grouping
cargo test -p pg-am-heap --test vacuum_reclaim
cargo test -p pg-am-heap --test vacuum_crash_windows
```

---

## 阶段 D：索引清理 + `Engine::vacuum` 端到端（4–6 天）

**归属**：M3 Vacuum（出口）
**前置**：C
**目标**：`Engine::vacuum(table)` 五阶段流水线一次完成，churn 下空间有界——
ROADMAP.md:216 的核心验收。

| 任务 | 交付物 |
|------|--------|
| `Engine::vacuum(table)` | 五阶段顺序（§4.1）：① 取 `AccessExclusive` 表锁（`lock_manager` 既有，CREATE/DROP TABLE 同模式）→ 取一次 horizon 全程使用 → ② `scan_dead_tuples(horizon)` → ③ `collect_index_keys`（只读）→ ④ 推模式索引清理 → ⑤ `reclaim`（压实 + 页释放）。锁释放即出口 |
| 锁的 XID 载体（S3） | 表锁以 XID 为键、事务结束由 `release_all(xid)` 释放，而 vacuum 不是用户事务——本项明确：vacuum 以 **auto-commit 风格维护 XID** 包裹全程（`create_table`/`drop_table` 同款模式：分配 XID → 进 active set → 结束走既有 `release_all(xid)` 释放路径），`AccessExclusive` 的持有与释放都挂在该 XID 上，锁生命周期 = 维护事务生命周期。**不死锁论证（一句话）**：vacuum 在**等待期间不持有任何锁**（`AccessExclusive` 与一切模式冲突，等待时对每个冲突持有者各有一条出边，但**入度恒为 0**——没有任何节点在等它持有的锁，因为它还没持有）；获准后不再申请第二把锁。等待图中入度为 0 的节点不可能出现在环上，环不可能经过 vacuum，死锁检测器只会看到普通等待 |
| 推模式索引清理 | 遍历 `Engine::indexes`（engine.rs:409），对每条 (key, tid) 逐索引调 `BTreeIndex::delete(key, tid)`（index.rs:2563，自带 WAL）；**`EntryNotFound` 视为 Ok**（§4.3：eager 维护下绝大多数条目早已不在树上；真正有活的删除对象基本只有崩溃 loser INSERT 的悬挂条目） |
| 顺序不变量测试化 | **崩溃窗口①**（§12.1）：索引清理 WAL 已落盘、HeapCleanup 未落盘时注入崩溃 → 恢复后索引扫描与堆扫描一致、无悬空 TID 读到错误行 |
| churn 验收 | 固定行数表上 N 轮 "UPDATE/DELETE 一批 + INSERT 一批"，每 K 轮跑一次 vacuum：断言数据文件页数有界（不随轮数线性增长）、vacuum 后 `scan_dead_tuples(horizon=最新)` 返回空（§12.1；崩溃注入轮并入窗口①②，O2 已由 Stage S 清偿，无单独口径） |
| 空间复用验证 | churn 后 freelist 非空 / 新插入复用已释放页与压实空间，页数稳态收敛（ROADMAP.md:216） |
| 并发共存正确性 | vacuum 进行中：纯 auto-commit SELECT（无锁）与新 BEGIN 不被锁挡住，但 horizon 防线保证其快照看不到被回收版本（§3.3 论证的端到端测试化）；显式事务 SELECT/DML 阻塞至 vacuum 结束（锁矩阵既有行为，断言超时有序而非死锁，watchdog 保护） |

**关键约束**：
- §4.1 顺序不变量：五阶段顺序不可调换；`collect_index_keys`（只读）与
  `reclaim`（物理）之间由 engine 执行索引清理，不变量由调用序列结构化保证
- §4.3：EntryNotFound→Ok；每死行每索引一次独立下探，离线模式性能可接受，
  批量接口归 Phase 5b
- §2：停写窗口是既定代价；`AccessExclusive` 挡不住无锁纯 SELECT 是水位线问题
  而非锁问题（已由 A 的注册解决，此处只验证）
- §5：btree 侧走现有在线 delete，页合并/页内压实不做

**验收标准**：
- **功能**：`Engine::vacuum` 对空表/无死行表/纯死行表/HOT 链表/多索引表五类
  fixture 全通；多索引表上各索引清理量正确；维护 XID 在 vacuum 结束后无锁残留
  （`table_lock_state` 断言空）
- **正确性**：churn 验收全绿（页数有界 + vacuum 后 dead 为空）；崩溃窗口①②
  恢复后堆↔索引一致；悬挂条目（崩溃 loser INSERT）被 vacuum 实际清除
- **并发**：vacuum × 并发纯 SELECT × 并发新 BEGIN 混合负载下结果无错乱
  （watchdog）；锁阻塞路径无死锁（死锁检测器对 vacuum 等待零误报）
- **性能**：churn 全程 TPS 按 S2 测度协议对比基线：无统计显著回归
  （§12.5 的 <5% 作为显著性上限；vacuum 分项叠加 A 的注册开销后度量）

**验收命令**：
```bash
cargo test -p pg-engine --test m3_vacuum_e2e
cargo test -p pg-engine --test m3_vacuum_churn
cargo test -p pg-engine --test m3_vacuum_crash_windows
```

---

## 阶段 E：可观测性（3–4 天）

**归属**：M3 可观测性
**前置**：A（自省 API 依赖 `oldest_snapshot_xmin`）；与 B–D、F 并行
**目标**：ROADMAP.md:208/217 的四项 + 查询统计，按 §6 拆三个交付物 + 一个 CLI
出口（S1）。

| 任务 | 交付物 |
|------|--------|
| pg-waldump | `pg-storage` 新增 `src/bin/pg-waldump.rs`（§6.1 选型 (a)：与格式同 crate 演进、零新依赖边）：人类可读 dump（LSN、记录类型、payload 关键字段逐条）+ `--start-lsn/--end-lsn` 范围过滤；reserved 类型（`SegmentSeal` 等无 handler 记录）打印原始 payload 字节而非报错（与 recovery 硬失败策略相反，诊断工具"尽量多展示"） |
| Engine 自省 API | `active_xids()`（manager.rs:485）、`wait_edges()`（manager.rs:400 + lock_manager.rs:421 合成 wait-for 图）、`table_lock_state(oid)`（lock_manager.rs:400）、`oldest_snapshot_xmin()`（A 交付，此处进自省面）、`clog_hit_rate()`（clog_buffer.rs:167）——全部只读拼装既有能力，零新增机制 |
| Buffer Pool 计数器 | **唯一实现缺口**（§6.2）：`BufferPool` 补 `hits/misses: AtomicU64 + hit_rate() -> f64`，Relaxed 序、pin 命中自增、读盘未命中自增；API 形状与 `clog_buffer.rs:156-173` 逐行对齐，不发明新风格 |
| 查询统计 ring buffer | `pg-engine` 内置 `QueryStats`：容量 1000（可配置），记录 `{query 文本, 延迟, 影响/返回行数, 执行路径, 时间戳}`；`Engine::exec`（engine.rs:2064）**单点**埋点，auto-commit 与显式事务天然同覆盖；typed API（`Engine::scan/insert/...`）不走 `exec` 不产生统计，README 与验收口径按 §6.3 表述 |
| 命令行诊断出口（S1） | `pg-engine` 新增 `src/bin/pg-diag.rs`：`txn` 子命令（active xids + horizon + clog 命中率）、`locks` 子命令（wait-for 图 + 表锁状态）——直接拼装 §6.2 自省 API，补齐 ROADMAP.md:217 "可通过命令行工具诊断事务和锁问题"。**不**挂进 pg-waldump：自省 API 在 `Engine`（pg-engine）上，pg-storage 的 bin 因依赖方向调不到；且独立进程打开数据目录看到的是自己的空引擎实例——M3 口径为"诊断面以 CLI 形式可用"（测试/单进程场景），跨进程诊断 live server 归 Phase 4a 经 pg-wire 暴露 |

**关键约束**：
- §6.1：waldump bin 挂 `pg-storage`，不为单个 CLI 新建 crate；pg-diag bin 挂
  `pg-engine`（S1，crate 归属由依赖方向决定）
- §6.3：ring buffer 内存形态，系统表化归 Phase 6；溢出丢最老条目是既定语义
- §10：零新运行时依赖

**验收标准**：
- **功能**：waldump 对含各类记录（heap/btree/txn/checkpoint/HeapCleanup/reserved）
  的 WAL 全量可解析；LSN 过滤边界正确；reserved 类型不崩（§12.3）；
  `pg-diag txn` / `pg-diag locks` 在构造的活跃事务/锁等待 fixture 上输出正确
  （§12.4 的命令行出口，ROADMAP.md:217）
- **正确性**：各自省 API 集成测试（§12.4）：构造锁等待环查 `wait_edges`、
  已知命中/未命中序列查两个 hit_rate、并发快照查 `oldest_snapshot_xmin`；
  ring buffer 容量溢出丢最老、exec 单点覆盖两条事务路径
- **性能**：统计埋点对 exec 路径开销不可测（churn 对比抽查）；BP 计数器
  Relaxed 无热路径退化

**验收命令**：
```bash
cargo test -p pg-storage --test waldump
cargo run -p pg-storage --bin pg-waldump -- --start-lsn <LSN> --end-lsn <LSN> <wal_dir>
cargo test -p pg-engine --test m3_introspection
cargo test -p pg-engine --test m3_query_stats
cargo test -p pg-engine --test m3_diag_cli
cargo run -p pg-engine --bin pg-diag -- txn    # fixture 数据目录
cargo run -p pg-engine --bin pg-diag -- locks
```

---

## 阶段 F：pg-wire（5–7 天，协议兼容边角可能 +1–2 天）

**归属**：M3 PG Wire
**前置**：无（M2c 出口即可；不依赖 vacuum 任何产物。排在 A 后启动仅为回归归因
隔离，与 B–E 完全并行）
**目标**：psql 与三大驱动能连上来跑 CRUD（ROADMAP.md:215-216、§7.4）。

| 任务 | 交付物 |
|------|--------|
| 新 crate | `crates/pg-wire`，依赖 `pg-engine`，workspace member；只做协议编解码 + 连接管理 + SQL 透传，不含执行逻辑（§7.1） |
| CI 接线（P2 修正） | 新 crate 进 workspace 后必须同步更新 `.github/workflows/ci.yml` 的 fmt / clippy / test / doc 四个 matrix（ci.yml:30/55/101 按 crate 枚举，不含 pg-wire 则 CI 零覆盖）；pg-wire 走默认 features 分支（参照 pg-storage/pg-am-btree 的 loom 豁免先例——若 pg-wire 引入任何 feature，同样要评估 `--all-features` 影响） |
| v3 最小闭集 | 启动：StartupMessage → `AuthenticationOk`（trust 无认证）→ `ParameterStatus`（server_version 等必需项）→ `ReadyForQuery`；SSLRequest 回 `N`、GSSENCRequest 忽略。查询：仅 Simple Query（`Q`），多语句串按序 auto-commit。结果：`RowDescription` + 文本 `DataRow` + `CommandComplete`（`SELECT n`/`INSERT 0 n`/`UPDATE n`/`DELETE n` 标签）；错误 → `ErrorResponse` + `ReadyForQuery`；`Terminate` 正常关连（§7.2） |
| 事务控制拦截 | `BEGIN`/`COMMIT`/`ROLLBACK` 在 pg-wire 层拦截映射到 `Engine::begin_txn` / `TxnHandle::commit` / `abort`——**不透传 exec**（engine.rs:2077-2082 硬报错，事务控制是程序式 API）；每连接至多持一个 `TxnHandle` |
| 类型文本编码 | INT4/INT8 十进制直出；TEXT 原样；NULL 协议空值标记；Timestamptz µs 整数 / Uuid 标准串 / Bytea `\x` hex（§7.2） |
| 线程模型 | `std::net::TcpListener` + 每连接一个 std 线程 + `Arc<Engine>`（§7.3；不引 tokio，全仓库 std::thread 现状一致）；每连接线程独占自己的 `TxnHandle`（`!Sync`，结构恰好满足） |
| Send+Sync 断言（O5） | pg-wire 接线处加裸 fn 编译期断言 `Engine: Send + Sync`（不引 `static_assertions` crate，零依赖），把字段结构推导钉成编译期性质 |
| 四客户端验收 | psql + psycopg2 + node-postgres + rust-postgres 各自连接跑通 CREATE TABLE / INSERT / SELECT / UPDATE / DELETE / BEGIN / COMMIT / ROLLBACK（§12.2）；驱动启动探针（`SET`、`pg_type` 查询、`SELECT version()`）无法支持的语句返回 `ErrorResponse` 但**不断连**（§7.4 / R3） |
| CI 口径（N6） | **CI 硬门槛只有 rust-postgres**（dev-dependency 进 `wire_clients` 测试，§10）；psql / psycopg2 / node-postgres 三家手动矩阵在 Stage G 收口前跑完，结果（客户端版本、通过项、探针报错清单）记录进 `docs/phase1-m3-benchmarks.md` 与 stage_spec Stage F 节 |

**关键约束**：
- §7.2：Extended Query / COPY / 认证 / CancelRequest / TLS 均为非目标
- §7.3 / §10：手写编解码，不引 `pgwire` crate（其 async 模型会把 tokio 拖进
  连接路径）；零新运行时依赖
- §11 R3：psql catalog 探针答不出是固有落差，验收口径为"报错不断连 + 基本
  CRUD 可用"，`\d` 等元命令不承诺

**验收标准**：
- **功能**：rust-postgres 自动化全绿（CI 硬门槛）；psql / psycopg2 /
  node-postgres 手动矩阵通过并留记录（N6）；多语句串按序执行；
  探针语句报错不断连
- **正确性**：文本编码 roundtrip（含 NULL / Timestamptz / Uuid / Bytea）；
  每连接单 TxnHandle 语义（事务中 BEGIN 报错不破坏现状）
- **并发**：多连接并发 CRUD（watchdog 保护），连接间互不影响；`Engine:
  Send + Sync` 断言编译期生效
- **回归**：`cargo build --workspace` 通过，新 crate 不污染既有依赖图
  （`cargo tree` 抽查零新运行时依赖）

**验收命令**：
```bash
cargo test -p pg-wire --test wire_protocol        # 编解码 + 事务拦截单测
cargo test -p pg-wire --test wire_clients          # rust-postgres（CI 硬门槛）；psql/psycopg2/node-postgres 手动矩阵（命令与结果记录见 N6）
```

---

## 阶段 G：接口预留 + M3 收口（3–4 天）

**归属**：M3 收口
**前置**：B / C / D（WAL 记录族与 AM 形态稳定后定契约）；E / F 完成后收口
**目标**：两个接口预留落纸面契约，清偿开放问题，全量回归 + 打 M3 出口。

| 任务 | 交付物 |
|------|--------|
| SegmentedStorage 预留 | `pg-storage` 新增 `SegmentedStorage` trait（`create_segment/freeze/seal/merge`）+ `SegmentState { Active, Frozen, Sealed, Merging, Retired }` 单向状态机（§8）；`SegmentSeal=110`/`SegmentMerge=111` 判别式 **M2 Stage C 已预留**（record.rs:80-83），本 stage 只写 payload doc 契约（segment id 列表、目标 id），**不新增占号、不注册 handler、不提供实现** |
| Tier 2 预留 | `WalTailReader` trait（`tail_from(start: Lsn)`，背压与断点续传语义入 doc）、`WatermarkRegistry` trait（`index_oid -> applied_lsn` 存取）落 `pg-storage`；`AccessMethod` 加 `fn freshness(&self) -> Option<Lsn> { None }` 默认方法（§9：默认实现 = 现有 AM 零改动）——三者只定 trait 不交付实现 |
| O2 验证清偿 | tech-selection 标 O2 "已由 Stage S 解决"（`HeapUndoHandler` recovery undo 阶段标 ATT 残余成员 ABORTED 于 CLOG，undo.rs:5-37 接线 engine.rs:631）：本 stage 补一条**显式**回归——崩溃孤儿插入（xmin 属崩溃事务）在恢复后被 `scan_dead_tuples` 规则 1 正常收集并被 vacuum 回收；已有覆盖则归档引用并关闭 O2 |
| O4 清理 | 移除 `pg-storage/Cargo.toml:12` 的 tokio 死依赖声明（全仓库 `.rs` 零使用，死依赖误导选型）；`cargo tree` 验证依赖图收缩、`cargo test --workspace` 全绿 |
| stage_spec 归档 | `docs/stage_spec.md` 追加 M3 Stage A–G 各节（交付内容 / PG trade-off / 已知残留），格式对齐 Stage L–T |
| benchmark 落盘 | `docs/phase1-m3-benchmarks.md`：churn 页数有界曲线、注册开销（A，S2 协议数字）、vacuum 叠加后 TPS（D）、waldump 吞吐、**WAL 字节量观测（N5：压实批量改页的 FPI 放大，§11 R1——无既有压测，本 stage 首次量化）**；每项 target + 实测 + 未达标归因（对齐 m2 benchmark 文档格式） |
| 手动矩阵归档（N6） | psql / psycopg2 / node-postgres 三家手动矩阵结果（版本、通过项、探针报错清单）落盘进 benchmark 文档与 stage_spec Stage F 节 |
| 全量回归 + release | debug + release 全量（含 loom、m2b crash rounds、M1 crash_recovery）；§12.5：100 并发 CRUD TPS 按 S2 协议对比 M2c 基线无统计显著回归（<5% 上限） |
| M3 出口 tag | `phase1-m3`（**经用户确认后**打 tag） |

**关键约束**：
- §8 / §9：预留即承诺——trait 签名改动要过修订记录；只定义不实现，不为未验证
  需求付实现成本
- §11 O2：本 stage 只做验证，不新做实现；若验证发现缺口，升级为 P1 修复而非
  绕过
- §12.5：全量回归不 regress 是出口硬门槛

**验收标准**：
- **功能**：两个 trait 族 + 生命周期枚举编译可用（doc 测试 / 空 impl 编译桩）；
  `freshness` 默认方法对 heap/btree 行为零影响（回归证明）
- **正确性**：O2 崩溃孤儿回归全绿；tokio 移除后全量回归全绿
- **回归**：release 全量 + crash rounds + loom 全绿；TPS 按 S2 协议无统计显著
  回归（<5% 上限，§12.5）
- **文档**：stage_spec M3 各节 + `phase1-m3-benchmarks.md`（含 WAL 字节量观测与
  手动矩阵记录）落盘

**验收命令**：
```bash
cargo test --workspace
cargo test --workspace --release
cargo test -p pg-engine --test m2b_crash_rounds
# loom 模型测试（cargo test --workspace 不启用 loom feature，必须单独跑）
LOOM_MAX_PREEMPTIONS=2 cargo test -p pg-am-btree --features loom --test btree_loom
# S2 协议：配速模式 ×5 次取平均，对比 docs/phase1-m2-benchmarks.md 基线
M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100 cargo test -p pg-engine --release --test m2c_stress -- --nocapture
```

---

## 总时间估算

| Stage | 内容 | 归属 | 时间（1 名高级 Rust 工程师） |
|-------|------|------|---------------------------|
| A | 快照注册表 + horizon（含 B1 原子化） | 地基 | 3–4 天 |
| B | HeapCleanup WAL + redo + §4.6 重构 | 地基 | 5–6 天（S5 上调：§4.6 触及在线 insert 与 HeapInsert redo 两条崩溃恢复核心路径，外加 R4 全量回归门槛，原 3–4 天为低估） |
| C | Vacuum 核心（trait / 链分组 / 压实 / 页释放） | Vacuum | 5–7 天 |
| D | 索引清理 + `Engine::vacuum` 端到端 | Vacuum | 4–6 天 |
| E | 可观测性（waldump / 自省 / 统计 / pg-diag） | 并行支线 | 3–4 天 |
| F | pg-wire | 并行支线 | 5–7 天（协议兼容边角 +1–2 天余量，S5） |
| G | 接口预留 + M3 收口 | 收口 | 3–4 天 |
| **串行合计** | | | **28–40 天** |
| **实际工期** | E/F 与 B–D 并行 | | **约 5–6.5 周** |

> 关键路径 = A → B → C → D → G（17–23 天 + G 3–4 天）；E（3–4 天）与
> F（5–7 天，+1–2 余量）合计 8–13 天，被 B–D 的 14–19 天窗口完全吸收。
> 体量勘误（S5）：此前"M3 约为 M2 的 1/4"算术错误——20 个 stage 是 M1+M2 合计
> （A–T），M2 主线为 9 个 stage（L–T）；7 vs 9 不构成 1/4。M3 的真实特征是
> 单 stage 平均工期更短、无新 on-disk 格式（HeapCleanup payload 是 Stage 0 预留
> 判别式的落地，非新增占号），主要风险集中在 B（§4.6 触及崩溃恢复核心）与
> F（协议兼容性边角，靠四客户端矩阵对冲）。

---

## 依赖关系图

```
                 ┌──────────────────────────┐
                 │ A (快照注册表 + horizon)  │
                 └────┬───────────┬─────────┘
                      │           │
                      ▼           ▼
        ┌───────────────────┐  ┌────────────────────┐
        │ B (HeapCleanup WAL│  │ E (可观测性 +       │  E 前置 A（horizon API）
        │  + §4.6 槽位重构) │  │     pg-diag CLI)   │
        └─────────┬─────────┘  └─────────┬──────────┘
                  ▼                      │
        ┌───────────────────┐            │
        │ C (Vacuum 核心)   │            │
        └─────────┬─────────┘            │
                  ▼                      │
        ┌───────────────────┐            │
        │ D (索引清理 +     │            │
        │   Engine::vacuum) │            │
        └─────────┬─────────┘            │
                  │                      │
   ┌──────────────┴──┐                   │
   ▼                 ▼                   ▼
┌─────────────────────────────────────────────┐
│ G (接口预留 + O2/O4 清偿 + 全量回归 + 收口) │  前置 B/C/D + E/F 完成
└───────────────────┬─────────────────────────┘
                    ▼
              tag phase1-m3

   F (pg-wire)：无前置（M2c 出口即可启动），与 B–E 完全并行，汇入 G
```

**关键并行边界**：
- A 严格先行：E 的自省 API 依赖 `oldest_snapshot_xmin`；B 虽无硬依赖，串行于 A 以
  隔离注册开销（R2）的性能归因
- B → C → D 严格顺序：compact 原语（B）是 reclaim（C）的前提；engine 五阶段（D）
  是 trait 两方法（C）的调用方
- E 与 B–D 并行、F 与一切并行：二者不进 vacuum 关键路径
- G 必须在 B–D 之后：接口预留的 WAL 契约引用 HeapCleanup 落地后的记录族形态

---

## 回归测试传承

| 出口 | 必须通过的历史回归 |
|------|------------------|
| Stage A | M1/M2 全量（debug）+ churn 基线对比（S2 协议） |
| Stage B | 上述 + m2b crash rounds（R4 硬门槛）+ M1 crash_recovery |
| Stage C | 上述 + HeapCleanup 收敛/幂等/崩溃窗口② |
| Stage D | 上述 + churn 页数有界 + 崩溃窗口① |
| Stage E | 上述 + §12.3/§12.4 集成测试 + pg-diag CLI 用例 |
| Stage F | 上述 + rust-postgres CI 门槛（+三家手动矩阵留记录） |
| Stage G（M3 出口） | 上述全部 + **release 全量** + loom + §12.5 TPS（S2 协议，<5% 上限） |

任何 stage 引入的改动破坏前序回归，回退到该 stage 的实现方案直至全绿（M2 惯例）。

---

## 开放问题与冲突标注

1. **§4.6 独立 stage vs 并入 Stage B**：tech-selection §4.6/R4 要求该重构"作为
   独立 stage 先行于 vacuum 落地"。本计划维持 7-stage 骨架，将其并入 Stage B 并
   以"B 出口必须全量回归（含 m2b crash rounds）通过后才进 C"满足 R4 的实质
   （先行 + 隔离回归）。若 review 认为必须物理拆 stage，将 B 拆为 B1（§4.6）/
   B2（HeapCleanup），工期不变。
2. **`compact()` 前移 Stage B**：§4.5 要求 redo handler 与记录同 stage 交付、且
   redo 调用与在线路径同一个 `compact()`——原语因此必须在 handler 所在 stage
   之前存在。本计划把 `SlottedPage::compact()`（纯页内原语，不依赖 vacuum 流水线）
   从 C 前移到 B；Stage C 收窄为 trait / 链分组 / 页释放。这是对选型文档的实践
   细化，不改变 §4.1 阶段 4 的任何语义。
3. **O2 只做验证**：tech-selection 标 O2 已由 Stage S 解决。Stage G 只补显式回归
   并归档；若验证发现缺口（如某崩溃时序下 ATT 成员未被标 ABORTED），升级为 P1
   修复，不在 G 的估算内，工期另计。
4. **O3 维持 tech-selection 决定**：HOT 部分死链不 prune（避免 LP 格式变更），
   与 M2 文档 §十八"HOT prune 推迟到 M3 vacuum"的表述存在偏差——以 tech-selection
   §4.2 为准，不启动 LP 状态扩展；M2 文档索引回改建议留 review 定夺。
5. **O6 Sequence（SERIAL）**：M2 §十八列在 M3、ROADMAP M3 五块未含——本计划
   **不做**。若确认不做，应回改 M2 文档索引避免口径漂移（留 review 定夺）。
6. **O1 已在本计划决定**：不加"panic 后拒绝 vacuum"护栏（Stage A 任务表），
   语义文档化。若 review 推翻，护栏实现约 +1 天。

---

## 遗留与归队

- **在线 / 渐进式 vacuum → Phase 5b**（v1.2 已锚定 ROADMAP）：heap/B+Tree 在线
  渐进 vacuum 作为多 AM GC 协调器的第一个消费者落地；autovacuum 后台调度归
  Phase 7a。M3 若验收后停写窗口成为实际问题，只做"分段离线"（按页区间分批持锁）
  缓解，不做中间态独立在线化。`Vec<Tid>` 物化的迭代器化随在线化一并处理
  （access_method.rs:158-160 TODO）。
- **HOT 部分死链 LP 重定向 → on-disk 格式演进**：`LP_REDIRECT` 是格式变更，
  M2 文档 §21 冻结页格式后任何 on-disk 变更须带 migration；归 Phase 7 页格式
  演进时与 prune 一并处理（§4.2 代价注记）。
- **psql 兼容落差 → Phase 4a/6**（R3）：catalog 探针、`\d` 元命令、Extended
  Query / COPY / 认证 / TLS 均不属 M3；极简版验收口径为"报错不断连 + 基本
  CRUD 可用"。**跨进程 live 诊断**（pg-diag 看到的是自己进程的空引擎实例）同样
  归 Phase 4a——经 pg-wire 管理通道暴露 §6.2 自省面（S1 注记）。
- **B+Tree 页合并与页内死空间压实 → Phase 5b/7**（§5）：长期 churn 下索引体积
  缓慢增长（只删不合），量化观测靠 §6 统计，治理归 GC 协调器/页格式演进。
- **挑战档 benchmark 归队**：churn 长跑加长档（更大 N/K、含崩溃注入的全时长档）
  与 100 并发 60min 级压测不阻塞 M3 出口，命令在 `phase1-m3-benchmarks.md`
  文档化，需要时手动执行（继承 Stage T 的口径惯例）。
- **其余已锚定归队**：VACUUM FULL / CLUSTER → Phase 7a；查询统计系统表化 →
  Phase 6；`WalTailReader` / `WatermarkRegistry` / `SegmentedStorage` 实现 →
  各自 Phase（Tier 2 索引 / Phase 3 / Phase 5）。

---

## review 修订记录

| 编号 | 级别 | 处置 |
|------|------|------|
| B1 | blocker | 快照注册沉入 `TxnManager::snapshot()` 临界区（与 active set + clock 读取同一把锁），签名改 `-> (Snapshot, SnapshotGuard)`；caller-wrapper 包装形态否决（构造—注册窗口使 horizon 漏盖在飞快照，`AccessExclusive` 无法补救）；反枚举护栏 = `Snapshot` 构造器 pg-txn 私有化 + `#[cfg(test)]` 计数断言 + CI grep。tech-selection §3.3 同步修订为 v1.3 |
| S1 | major | Stage E 新增 `pg-diag` bin（pg-engine，`txn`/`locks` 子命令）补齐 ROADMAP.md:217 命令行诊断出口；不挂 pg-waldump（crate 依赖方向 + 独立进程看不到 live 状态；跨进程诊断归 Phase 4a，已入"遗留与归队"） |
| S2 | major | <1% 字面门槛不可测（criterion smoke 运行间噪声远大于 1%）：改为固定负载/固定时长协议（`m2c_stress` 配速模式 ×5 次取平均），判定口径"噪声界（±3%）内无统计显著回归"；基线权威落点 `docs/phase1-m2-benchmarks.md`，Stage A 开工第一天实测记录；A/D/G 三处性能验收统一对齐 |
| S3 | major | Stage D 明确 vacuum 走 auto-commit 风格维护 XID（create_table/drop_table 同款），锁生命周期挂该 XID、复用 `release_all(xid)`；不死锁论证修正为"等待期间入度恒 0（零持有），环不可能经过 vacuum"（原"出度 ≤1"措辞被 P3 指出不准——`AccessExclusive` 等待时对每个冲突持有者各有一条出边） |
| S4 | minor | Stage B 验收命令按 crate 归属拆分：payload roundtrip + analysis 分类在 `pg-storage`；redo 收敛/幂等/slot 复用在 `pg-am-heap`（handler 归 `heap_redo_handlers`）；crash harness 场景借 `pg-engine` |
| S5 | minor | "M3 体量约 M2 的 1/4" 算术错误更正（20 = M1+M2 合计 A–T；M2 主线 9 stage L–T）；B 上调 5–6 天、F 注 +1–2 天兼容余量；串行合计 28–40 天、实际约 5–6.5 周 |
| N1 | nit | 空间复用验收引用 ROADMAP.md:214 → **216**（214 是被删除线的在线 vacuum 行） |
| N2 | nit | "第一周做什么"重排为 Stage A 4 天 + Stage B 开局，与估算表 3–4 天一致 |
| N5 | nit | Stage G benchmark 落盘新增 WAL 字节量观测（压实 FPI 放大，§11 R1，首次量化） |
| N6 | nit | Stage F 注明 CI 硬门槛仅 rust-postgres；psql/psycopg2/node-postgres 手动矩阵结果在 Stage G 收口落盘（benchmark 文档 + stage_spec） |
| P2-1 | major | 反枚举护栏表述更正：`snapshot()` 是唯一的**注册**构造点而非唯一构造点——`Snapshot::everything()`（snapshot.rs:57，pub，xmin=0）为明确的不注册特例（engine.rs:854 目录引导 + 测试），注册它会把 horizon 钉死在 0；两个踩坑方向（一并私有化 → open 断；让它走注册 → horizon 归零）均写入任务表警示；CI grep 扩为同时覆盖 `Snapshot {` 字面构造与新增关联构造函数。tech-selection §3.3 同步修订为 v1.4 |
| P2-2 | major | §3.3 step 2 措辞更正：vacuum 期间进入 active set 的是新 BEGIN **与 auto-commit DML**（auto_commit 先分配 XID 入 active set、再在语句体内申请表锁）；结论不变（XID 单调性对两类新进入者均成立）；Stage A horizon 并发测试补 auto-commit DML 专项（XID/快照先于锁等待进入，horizon 仍不破） |
| P3 | nit | ① "构造器私有化"实为**字段私有化**（Rust 无构造器；`Snapshot` 五个 pub 字段，snapshot.rs:28-49）：Stage A 任务表补机械重构量（engine.rs 约 16 处 `snap.current_xid` 字段访问改访问器）；② S3 死锁论证"出度 ≤1"修正为"等待期间入度恒 0" |
| Stage A 终审修正 | major | ① CI grep 护栏修复为 POSIX 可移植写法（`\b`/`\s` 在 BSD grep 静默失效；排除规则适配 `path:line:` 前缀），正反注入验证通过；② horizon 空 registry 回落改为"active 最小 XID 优先"（P2，begin→snapshot 窗口结构性覆盖，tech-selection v1.6）；③ CI 护栏补 `Snapshot::new_unregistered(` 拦截（P3-1）；④ atomicity_stress churn 线程改引擎真实形态 begin→snapshot→drop→commit（P3-2） |
| P1 | blocker | 验收命令环境变量名与格式全错（`M2C_STRESS_DURATION=300s`/`M2C_STRESS_CONN` vs 实际的 `M2C_STRESS_SECS`/`CONNS`/`TPS` 纯 usize 解析、失败静默回落默认值）——会静默跑成 CI 默认档，性能门槛形同虚设。已全部改为 `M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100 ... --release -- --nocapture` |
| P2-3 | major | §4.6 重构范围更正：不止 insert 单路径——"预测 slot → WAL → 落位"耦合存在于**四处在线路径**（insert / HOT update / 同页非 HOT update / 跨页 update）与**三个 redo handler**（HeapInsert / HeapUpdate 两分支 / HeapHotUpdate）；`acquire_page_with_room` 反向扫描使 compact 后中部页也成为 update 落点。硬约束速查、Stage B 任务表、`test_slot_reuse_after_compact_redo` 用例（扩为四路径）同步更正；支撑 B 上调 5–6 天的估算 |
| P2-4 | major | 崩溃窗口② 恢复语义更正：unlink→free 之间崩溃 = 被摘页脱链且未入 freelist（既定单页泄漏，记入 stage_spec 残留），原"无悬空页"断言不可写，改为精确口径（链遍历/堆扫描一致 + 泄漏明示）；关键约束新增**顺序不可逆**（先 free 后 unlink = 结构性损坏）与备选设计记录（空页留链零窗口；消除窗口需改 PageFree payload = on-disk 变更，判定不值） |
| P2-5 | major | Stage F 补 CI 接线任务：`.github/workflows/ci.yml` 四个 matrix（fmt/clippy/test/doc）按 crate 枚举，pg-wire 不更新则 CI 零覆盖（loom 豁免分支先例参照）；Stage G 验收命令补 loom 调用（`LOOM_MAX_PREEMPTIONS=2 cargo test -p pg-am-btree --features loom --test btree_loom`——`cargo test --workspace` 不启用 loom feature） |

---

## 第一周做什么

如果今天就开工，**第一周 = Stage A 收口（4 天）+ Stage B 开局**（与估算表
3–4 天一致，N2）：

1. **Day 1**：M2c 基线实测记录（S2 协议，落 `docs/phase1-m2-benchmarks.md`）+
   registry 与 active set/clock 合并锁状态 + `snapshot()` 临界区内原子注册
2. **Day 2**：六调用点适配新签名 + 反枚举护栏（构造器私有化 + 计数断言 +
   CI grep）
3. **Day 3**：注册覆盖 / 泄漏自由 / horizon 并发 / 原子性专项测试 + churn
   对比（S2 协议 ×5 取平均）
4. **Day 4**：Stage A 对抗性 review + stage_spec 归档 + `PHASE1-M3-StageA`
   commit（经用户确认）
5. **Day 5**：Stage B 开局——§4.6 `first_fit_slot` / `add_tuple_at` 拆分

Stage F（pg-wire）可从 Day 1 起由同一工程师间隙推进或第二人并行——它不依赖 A–E
任何产物，是天然的第一并行支线。
