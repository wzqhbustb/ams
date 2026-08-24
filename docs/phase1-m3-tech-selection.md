# Phase 1 M3 技术选型

> 承接 M2（Heap + MVCC + B+Tree + ARIES 变体崩溃恢复），本文档定义 M3 阶段
> （**基础 Vacuum + 可观测性 + PG Wire 极简版 + 两个接口预留**）落地前所有
> 跨模块的技术选择，对应 ROADMAP.md Milestone 3（ROADMAP.md:203-217）。
>
> 目标与 M1/M2 一致：所有影响 on-disk 格式、WAL 语义、跨模块契约的决策先敲定；
> 每个选择给"选项 → 选择 → 理由 → 代价"。章节编号（§1…§12）供代码注释长期引用。
>
> 文档中的代码引用（`file:line`）均为撰写时（2026-08）核实的事实；若后续实现
> 与引用不符，以代码为准并修订本文档。

---

## §1 范围与非目标

**M3 交付五块内容**（与 ROADMAP.md:203-217 一一对应）：

| # | 模块 | 对应章节 |
|---|------|---------|
| 1 | Vacuum（扫描死元组、回收空间、通知索引清理） | §2–§5 |
| 2 | 可观测性（WAL dump 工具、活跃事务/锁等待查询、Buffer Pool 命中率、查询统计） | §6 |
| 3 | PG Wire Protocol 极简版（Simple Query + 文本结果） | §7 |
| 4 | SegmentedStorage 接口预留（trait + segment 生命周期 + WAL 记录类型） | §8 |
| 5 | Tier 2 接口预留（WAL tail reader、watermark registry、索引新鲜度 hook） | §9 |

**非目标（明确不做）**：

- **在线 / 渐进式 vacuum**（autovacuum 后台线程、部分页扫描）：M3 只做离线全表
  vacuum（§2），在线化归 M3+。
- **Extended Query 协议**（Parse/Bind/Execute）、**认证**（SCRAM/MD5）、TLS：
  PG Wire 极简版只做 Simple Query + trust（§7）。
- **B+Tree 页合并（merge/再平衡）**：M2c 既定不做，M3 维持（§5）。
- **B+Tree 页内死空间压实**：归 M3+ 或 vacuum 扩展（§5）。
- **VACUUM FULL / CLUSTER**：M2 文档 §十八已划归 Phase 7a，维持。
- **Sequence（SERIAL）**：M2 文档 §十八列在 M3，但 ROADMAP M3 五块未包含；
  本文档不覆盖，如需做由 coding-plan 单独追加（见 §11 开放问题 O6）。
- **查询统计落系统表**（pg_stat_statements 表形态）：系统表化归 Phase 6，
  M3 只做内存 ring buffer（§6.3）。

---

## §2 Vacuum 模式选择

**选项**：

- (a) **离线 `Engine::vacuum(table)`**：程序式 API，对整个表持 `AccessExclusive`
  表锁，一次完成"收集 → 提取 key → 索引清理 → 页内压实 → 页释放"
  五阶段（§4.1）。
- (b) **在线渐进式 vacuum**：后台线程逐页扫描，与并发 DML 共存，只做页内
  清理不碰表锁（PG autovacuum 形态）。

**选择**：(a)。

**理由**：

- 单线程实现，无需处理"vacuum 扫到一半页被并发 INSERT 改写"的页级竞争；
  正确性论证退化为"持有 `AccessExclusive` 期间表上无其他写者"。
- 锁矩阵现成：`AccessExclusive` 已在 M2c Stage P 的表锁体系内
  （`Engine` 字段 `lock_manager`，crates/pg-engine/src/engine.rs:396；
  CREATE/DROP TABLE 已使用该模式，engine.rs:953 / 1073），vacuum 只是新调用方。
- M3 范围最小正确：ROADMAP 的验收是"vacuum 后空间可被复用，无无限膨胀"
  （ROADMAP.md:214），离线模式已满足。

**代价**：

- vacuum 期间表对所有**持锁**语句不可用：`AccessExclusive` 与一切锁模式冲突，
  显式事务的 SELECT（`AccessShare`，engine.rs:2242）与全部 DML
  （`RowExclusive`）都阻塞至 vacuum 结束。大表 vacuum 造成秒级停写。
- **例外**：纯 auto-commit SELECT **不持任何表锁**（engine.rs:2125-2133，
  "owns no transaction, so it takes no table lock"），它不会被 vacuum 的
  `AccessExclusive` 挡住 —— 这是水位线问题，不是锁问题，由 §3 的快照注册
  解决，而不是靠锁互斥。
- 在线化（(b)）是既定方向，**明确归属 Phase 5b（多 AM GC 协调器）**——
  heap/B+Tree 的在线渐进式 vacuum 作为协调器的第一个消费者落地（2026-08
  已锚定进 ROADMAP.md Phase 5b.1 与 M3 条目；autovacuum 后台调度归 Phase 7a）。
  若 M3 验收后停写窗口成为实际问题，以"分段离线"（按页区间分批持锁）缓解，
  不做中间态独立在线化。`Vacuumable` trait 注释中已留 TODO
  （crates/pg-am-heap/src/access_method.rs:158-160：`Vec<Tid>` 物化对大表
  不友好，在线化时考虑迭代器/回调）。

---

## §3 Vacuum 水位线（关键决策）

### 3.1 现状：没有快照注册表

死元组判定的核心是"**最老活跃快照 xmin**"（horizon）：`xmax` 已提交且
`xmax < horizon` 的元组对所有现存及未来快照不可见，可回收
（`scan_dead_tuples` 的判定即此语义，crates/pg-am-heap/src/heap_am.rs:1636-1693）。

但 `TxnManager` 目前**只注册事务，不注册快照**：

- `begin_txn` 把 XID 插入 active set，`commit_txn`/`abort_txn` 移除；
  `snapshot()` 在持锁瞬间读取 active set + XID clock 构造快照
  （crates/pg-txn/src/manager.rs:530-544），构造完即丢弃，**无任何
  "这张快照还活着"的记录**。
- 因此 active set 的最小 XID 只能覆盖**事务型读者**（显式事务、auto-commit
  DML），覆盖不了**无事务读者**。

**漏洞实例**：纯 auto-commit SELECT（engine.rs:2129-2133）用
`snapshot(TxnId::INVALID)` 取一张快照，不分自己 XID、不进 active set、
不持表锁。若 horizon 取 `min(active set)`，一个在飞的长 SELECT 完全不可见，
vacuum 可能回收它正在读的版本 —— 读到半截的 HOT 链、被压实的 slot，
表现为结果错乱甚至 `Corrupted` 报错。

### 3.2 选项

- (a) **只注册 DML/显式事务，接受只读语句无保护**：horizon = min(active set)。
  实现零改动，但在飞 SELECT 读到被回收版本的概率非零。
- (b) **全量快照注册**（PG 的解法）：每一处快照创建都向 registry 登记其
  `xmin`，快照生命周期结束注销；horizon = min(registry)（空则取 XID clock
  当前值）。

**选择**：(b) 全量快照注册。

**理由**：

- §2 的锁互斥论证在这里不成立：vacuum 的 `AccessExclusive` 能等走在途
  auto-commit DML（`RowExclusive` 冲突），却挡不住无锁的纯 SELECT（§3.1）。
  唯一能把无锁读者纳入视野的机制就是注册。
- PG 的 `OldestXmin` 同样来自全后端（含只读）的快照 xmin 登记；M3 照此
  语义实现，未来在线 vacuum（§2 方向 (b)）直接复用同一 registry，
  不需要二次设计。
- 注册点收敛：`TxnManager::snapshot()` 是全系统**唯一**的快照构造点，
  在 `snapshot()` 内部注册可保证无一遗漏。当前共有**六处**调用点
  （engine.rs 全文 grep 核实），按生命周期分三类：
  - **事务级**：`Engine::begin_txn`（engine.rs:2027，快照随 TxnHandle
    存活）；
  - **语句级（auto-commit）**：`Engine::auto_commit`（engine.rs:2404）、
    `create_index` 在 Exclusive 锁后的重取快照（engine.rs:1202）；
  - **语句级（无锁读者，§3.1 漏洞的同构形态）**：纯 SELECT
    （engine.rs:2130）、公开 typed API `Engine::scan`（engine.rs:1571）、
    `Engine::index_lookup`（engine.rs:1404）——三者都是
    `snapshot(TxnId::INVALID)`、不进 active set、不持表锁，是注册
    机制最不能遗漏的三处。

### 3.3 设计

- registry 落在 **`TxnManager` 侧**（不放在 engine）：快照语义归 pg-txn，
  engine 只是调用方；`TxnManager` 新增
  `snapshot_xmins: Mutex<BTreeMap<TxnId, usize>>`（xmin → 引用计数，
  同 xmin 快照可共存），与 active set、XID clock 合并到**同一把锁**的
  状态内。**（v1.3 修订）注册必须与快照构造原子**：`snapshot()` 在读取
  active set + clock 的**同一临界区**内完成 xmin 注册，签名改为
  `snapshot(current_xid) -> (Snapshot, SnapshotGuard)`，`Drop` 时注销。
  原"caller-wrapper（`register_snapshot(&Snapshot) -> SnapshotGuard`
  包装）"选项**否决**——构造返回与注册之间的窗口会让 vacuum 取到漏掉
  在飞快照的 horizon（反例：U begin(xid=15) 取快照未注册 → vacuum 见
  空 registry 取 horizon=clock.current() → 删除者提交 → U 必须见的
  已提交行被回收；`AccessExclusive` 无法覆盖，取快照先于任何锁申请）。
  配套结构性护栏：`Snapshot` 字段收为 pg-txn 私有（Rust 无"构造器"，
  防字面构造必须私有化字段，engine.rs 21 处字段访问随迁为访问器），
  `snapshot()` 成为全系统唯一的**注册**构造点。**注意：
  `Snapshot::everything()`（snapshot.rs:57，pub，xmin=0）是明确的
  不注册特例**——目录引导（engine.rs:854）与测试路径使用，绝不进入
  registry（其 xmin=0 一旦注册会把 horizon 钉死在 0，vacuum 永久失效）；
  护栏的 CI grep 需同时覆盖 `Snapshot {` 字面构造与新增的
  `Snapshot::everything()` 式关联构造函数。
- **注销点**（与六处创建点一一对应，缺一即泄漏）：
  - `Engine::auto_commit` 结束（engine.rs:2402-2428 的成功与失败两条路径）；
  - `TxnHandle::commit` / `abort` / `Drop`（engine.rs:483-540）；
  - 纯 auto-commit SELECT 的语句结束点（engine.rs:2129-2133）；
  - `Engine::scan` / `Engine::index_lookup` 的函数返回点
    （engine.rs:1566-1574 / 1395-1410；即时快照，guard 随调用帧存活）；
  - `create_index` 的 re-snapshot 在其 `auto_commit` 闭包结束点一并注销
    （engine.rs:1202，随外层事务的生命周期）。
- **horizon 计算**（v1.6 修正）：`TxnManager::oldest_snapshot_xmin()` =
  `registry` 最小键；registry 为空时取 **active set 的最小 XID**，二者皆空
  才取 `txn_id_clock.current()`。active set 参与回落是结构性必要的：
  `begin_txn` → `snapshot()` 之间存在"已入 active set、尚未注册快照"的
  窗口，窗口内若直接取 clock，horizon 可高于该事务即将注册的 xmin（其
  快照 xmin 可低至当前最老活跃 XID）。PG 的 OldestXmin 同样同时考虑
  backend xid 与快照 xmin。vacuum 开始时取一次，全程使用同一 horizon
  （与 §2 离线模式的"一次完成"语义一致）。
- **vacuum 进行中新建快照的安全性**：纯 SELECT 在 vacuum 进行中仍可开始
  （无锁），且新 BEGIN 也不受 `AccessExclusive` 阻挡（表锁在首条 DML
  语句上才申请）—— 因此 active set **不是只会收缩**，"集合包含"式论证
  不成立，改用 XID 单调性（注意：`新快照.xmin = min(active set)` 恒
  **≤** 任一活跃事务的 xid，方向不能写反）：
  1. `horizon ≤ min(A_vacuum)`：取 t* = min(A_vacuum)，t* 在 vacuum 开始
     时仍活跃，其 begin 时注册的快照满足 `xmin ≤ 自身 xid =
     min(A_vacuum)`，而 horizon 取全部注册 xmin 的最小值；
  2. XID clock 严格单调递增：A_vacuum 全体成员的 XID 均在 vacuum 开始前
     分配；vacuum 期间能进入 active set 的是**新 BEGIN 与 auto-commit
     DML**（`auto_commit` 先 `begin_txn` 分配 XID 并入 active set，
     再在语句体内申请表锁——被锁挡住的只是语句体，XID 与快照已先行
     进入），两类新进入者的 XID 均 > max(A_vacuum) ≥ min(A_vacuum)；
  3. 设 vacuum 开始后某时刻的活跃集 `A_new` = 幸存者 ∪ 新人：幸存者 ⊆
     A_vacuum，新人按第 2 步整体高于 min(A_vacuum)，故
     `min(A_new) ≥ min(A_vacuum)`；
  4. 新快照.xmin = min(A_new)（无事务读者同式）≥ min(A_vacuum) ≥
     horizon。
  被回收元组满足 `xmax committed && xmax < horizon ≤ 新快照.xmin`，即
  删除在新快照的逻辑瞬间之前已提交完成 —— 该元组对新快照本就不可见，
  回收安全。

**代价**：

- 每条语句一次 `Mutex<BTreeMap>` 插入 + 一次删除；相对语句执行成本可忽略，
  但高并发短查询下 registry mutex 是新的共享热点（预期 < 1% 影响，
  §12 用 churn 压测验证）。
- **泄漏风险**：默认 unwind 策略下，panic 在栈展开中会**正常执行** guard 的
  `Drop`，快照正常注销、horizon 不受影响；仅 `panic=abort`（本项目未启用，
  Cargo.toml 无相关 profile 配置）或 `mem::forget` 会跳过 `Drop` 使 horizon
  永久偏低（vacuum 退化为不回收，安全但失效）。`auto_commit` 的 panic 策略
  （engine.rs:2395-2401，panic 即泄漏 XID/锁，进程级失败）代价模型一致——
  即便走到 abort/forget，horizon 卡住的代价也不大于现状，可接受。
- horizon 偏低（长查询阻塞回收）是 MVCC 的固有性质，不是实现缺陷；
  §6 的自省 API 暴露当前 horizon 供诊断。

---

## §4 Vacuum 回收物理路径

### 4.1 五阶段流水线

**选择**：`Engine::vacuum(table)` 驱动，五阶段顺序执行，全部在
`AccessExclusive` 保护下完成：

```
收集(collect) → 提取key(extract，只读) → 索引清理(index cleanup) → 页内压实(compact) → 页释放(free)
```

1. **收集**：复用现有 `Vacuumable::scan_dead_tuples`（heap_am.rs:1623-1696），
   传入 §3 的 horizon 作为 `oldest_xmin`。判定规则（已实现，不重写）：
   - `t_xmin` aborted → 死（无条件，heap_am.rs:1673-1679）；
   - `t_xmax` 非 LOCK_ONLY、已提交、且 `< oldest_xmin` → 死
     （heap_am.rs:1680-1692）。
   注意现有实现是**逐元组**收集，不感知 HOT 链 —— 链分组在阶段 2 由
   heap AM 内部完成（§4.2）。
2. **提取 key（只读，不改任何页）**：对每条普通死元组、以及每条全死 HOT
   链的链根，从元组字节解码出索引列值 `Vec<Option<Datum>>`。key 提取
   路径与 `Engine::delete_inner` 完全一致（engine.rs:1701
   `read_row_by_tid` + engine.rs:1718 `encode_key`），NULL 键跳过
   （engine.rs:1717 既有语义）。提取必须先于一切物理改写：页一旦被
   压实，key 就再也读不出来。
3. **索引清理**（推模式，见 §4.3）：对每条 (key, tid) 调
   `BTreeIndex::delete(key, tid)`（crates/pg-am-btree/src/index.rs:2563，
   自带 WAL）。**EntryNotFound 必须视为 Ok**：现有 DML 是 eager 索引
   维护（`delete_inner` / `update_inner` 在语句执行时就物理删除条目，
   engine.rs:1719），死元组的条目绝大多数早已不在树上，而 `delete`
   对缺席条目返回 `Err(EntryNotFound)`（index.rs:2557-2562）—— 若
   vacuum 把它当错误，第一次运行就会在正常路径上失败。
4. **页内压实**：`SlottedPage` 新增 `compact(dead_slots: &[u16])`：把
   dead slot 的 LP 置 `Unused`（`delete_tuple` 的既有语义，
   slotted_page.rs:303-307）、存活元组的物理字节向页尾连续整理、回收
   中部空洞、重置 `pd_lower/pd_upper`。**LP 数组条目一律不移动、不
   重排**：TID 被索引条目和 HOT `t_ctid` 引用，slot 号是跨页标识符；
   压实的对象是 tuple 数据区的空洞与 dead slot 的可回收状态。现状
   没有任何压实原语 —— `add_tuple` 虽带 Unused slot 的 first-fit 回收
   （slotted_page.rs:242-253），但生产路径从不产生 Unused（heap 的
   delete 是逻辑删除，模块文档 heap_am.rs:56-64 已把"追加式 writer"
   列为 redo 复现 slot 的前提）。因此 `compact()` 落地前必须先完成
   §4.6 的槽位寻址重构，否则 first-fit 命中会让 insert 的 slot 预测
   与 WAL 记录里的 slot 号分歧（debug 构建断言炸、release 构建静默
   写错，redo 端必然报 slot diverged）。
5. **页释放**：压实后全空的页从页链摘除（改写前驱页 `next_page`，
   slotted_page.rs:146/164 的单向链）后调 `PageAllocator::free_page`
   （crates/pg-storage/src/page_allocator.rs:202，写 `PageFree=41` WAL +
   推 freelist）。`drop_table` 已有同款调用先例（engine.rs:1113-1114）。
   heap AM 的内存页列表缓存需同步失效（参照 `drop_relation`，
   engine.rs:1119 的机制，按单页粒度剔除而非整表）。

**顺序不变量（崩溃安全的核心）**：使某个 TID 失效的 WAL 记录
（`HeapCleanup` 的 dead-LP 标记、`PageFree`）必须**晚于**该 TID 的最后
一条 `BTreeDelete` 落盘。反窗口（条目已删、元组还在）无害 —— 索引少
一个条目只影响路径选择，死行本来也不可见；正窗口（元组已回收、条目
还在）会让恢复后的索引残留悬空 TID：页未释放时被堆可见性过滤兜住
（漏读死行，无错结果），**页释放并被新插入复用后则指向无关的活元组
—— 读到错误的行**。本流水线靠两条既有性质使不变量结构成立：(i)
阶段顺序保证索引清理的 WAL 先写（LSN 更小）；(ii) WAL flush 是前缀式
的（`synced_lsn` 单调水位，writer.rs:398-408：使 LSN X 落盘即保证
≤ X 的全部记录落盘），叠加 WAL-before-data（HeapCleanup/PageFree 的
数据页落盘前其自身 WAL 必先 flush），故堆侧记录落盘时索引侧记录必然
已落盘。任何未来"并行化 vacuum 阶段"的改造都必须重新论证此不变量。

**并发注记**：纯 auto-commit SELECT 不持表锁（§2 例外），但页级读写
仍过 BufferPool 的 pin / pin_mut 互斥（buffer_pool.rs:238/263）—— 压实
持写 latch 期间并发读者只会阻塞，不会读到半压实页。§3 的 horizon 是
可见性正确性防线，页 latch 只是物理完整性防线，两者缺一不可。

### 4.2 HOT 链处理

现状（Stage S）：`follow_hot_chain`（heap_am.rs:1157）沿 `t_ctid` 前向找
可见版本；`hot_chain_root`（heap_am.rs:1210）反向定位链根；链根持有全部
索引条目（`HEAP_ONLY_TUPLE` 版本无自己的条目）。engine 的 DML 索引维护
已按链根寻址（engine.rs:1653、1708）。

**选择**：

- **整链回收**：链上所有版本均按 §4.1 规则判定为死（无版本对任何快照
  可见）→ 回收整链全部 slot，并删除**链根**的索引条目（key 从链根元组
  字节提取，tid 用 `hot_chain_root` 的结果）。注意会膨胀的只有**堆
  空间**：链根的索引条目在"最终杀手"（DELETE / 非 HOT 更新，engine
  按链根寻址删除，engine.rs:1708）执行时已被 eager 删除，整链回收时
  这一步通常命中 EntryNotFound（§4.3）。整链回收是 M3 必须做的：
  不做则 HOT 更新的死版本堆空间永远膨胀。
- **部分死链不 prune**：链上仍有可见版本时，死版本（含链根本身）保留原位，
  不重定向 `t_ctid`、不回收。

**理由**：

- PG 对部分死链的解法是 LP 重定向（`LP_REDIRECT`，根 slot 保号、指向
  可见版本），这需要给 `LinePointer` 增加新的状态值 —— 是 **on-disk 格式
  变更**。M2 文档 §21 已冻结页格式并规定"M3+ 起任何 on-disk 变更必须提供
  migration"（docs/phase1-m2-tech-selection.md:1396-1405）；为一个优化
  引入格式迁移不划算。
- 部分死链的空间损失有界：链最长到页满为止（HOT 不出页），随整行被
  DELETE 后整链终归可回收。
- 整链回收不需要任何 on-disk 格式变更：dead-LP 标记、`t_ctid` 链遍历、
  页内压实（§4.1 阶段 4）都建立在既有页结构之上，只是新原语的组合。

**代价**：

- 高频 HOT 更新且长链的表，死版本滞留到整链死亡才回收，空间回收滞后；
  `follow_hot_chain` 的遍历成本同样滞留。归 M3+（LP 重定向 + 格式迁移）
  或 Phase 7 页格式演进时一并处理。
- 与 M2 文档 §十八 "HOT prune 推迟到 M3 vacuum" 的表述有偏差（M3 只做
  整链，不做链内 prune）—— 见 §11 开放问题 O3。

### 4.3 索引清理：推 vs 拉

**选项**：

- (a) **推（vacuum 驱动）**：vacuum 遍历该表全部注册索引
  （`Engine::indexes`，engine.rs:409），逐个 `delete(key, tid)`。
- (b) **拉（AM 自扫）**：索引 AM 自己扫描全树，对每个条目回表验证 tid
  死活（PG `ambulkdelete` 形态）。

**选择**：(a) 推模式。

**理由**：

- 简单直接：死元组清单阶段 1 已有，按 tid 分组后对每个索引各删一次，
  无需索引侧新增"全树扫描 + 回表验证"的大块逻辑。
- key 提取已有现成路径（§4.1 阶段 2），无新契约。
- 拉模式的真正价值在于索引侧可批量、按页合并删除，那是 Phase 5b GC
  协调器统一各 AM 垃圾回收时再做的事；M3 不为此预设抽象。

**工作负载真相（规模预期）**：现有 DML 是 eager 索引维护 ——
`delete_inner` / `update_inner` 在语句执行时就物理删除索引条目
（engine.rs:1719），运行时 abort 由内存 index_undo 补偿，崩溃 loser 的
DELETE/UPDATE 侧由恢复后的 `compensate_loser_index_entries` 重插条目
（engine.rs:1876）。因此 vacuum 索引清理**真正有活的删除对象基本只有
一类**：崩溃 loser 事务 INSERT 留下的悬挂条目（一直占着 btree 空间，
此前只靠堆可见性过滤兜底）。推模式的绝大多数 delete 会命中
EntryNotFound（§4.1 阶段 3 的 Ok 语义）—— 正常 churn 不产生索引膨胀，
会缓慢增长的只有 btree 页内删除留下的空洞（§5 既定不压实）。索引
清理的性能担忧因此从"每次 churn 都重删一遍"缩小到"崩溃注入后才有
实际删除量"。

**代价**：

- 每索引每死行一次独立的 btree 下探（含页 latch 与 WAL；EntryNotFound
  也要走完整下探），大表 vacuum 的索引清理是 O(死行数 × 索引数) 次
  随机下探。离线模式下无并发竞争者，性能可接受；批量接口归 Phase 5b。

### 4.4 `Vacuumable` trait 扩展

现状 trait 只有 `scan_dead_tuples`（access_method.rs:161-175），M2 文档 §15
草案中的 `reclaim` / `notify_indexes` 两个方法**并未落入实际代码**
（trait 注释明确写"deferred to M3"，access_method.rs:154-156）。

**选择**：按阶段扩展 trait，但保持最小：

```rust
pub trait Vacuumable {
    /// 已有：收集死元组（§4.1 阶段 1）。
    fn scan_dead_tuples(&self, rel: RelationDesc<'_>, oldest_xmin: TxnId,
                        clog: &dyn ClogAccessor) -> Result<Vec<Tid>>;
    /// M3 新增（只读）：从死元组清单推导"需要清理索引条目"的
    /// (tid, 列值) 对 —— 普通死元组返回自身；全死 HOT 链返回链根
    /// 与链根列值；部分死链一律不返回（§4.2）。
    fn collect_index_keys(&self, rel: RelationDesc<'_>,
                          dead: &[Tid]) -> Result<Vec<(Tid, Vec<Option<Datum>>)>>;
    /// M3 新增：页内压实 + 全空页释放（§4.1 阶段 4/5）。纯物理操作，
    /// 只处理清单内的 slot；调用前提是 collect_index_keys 产出的
    /// 条目其索引清理 WAL 已落盘（§4.1 顺序不变量）。
    fn reclaim(&self, rel: RelationDesc<'_>, dead: &[Tid]) -> Result<()>;
}
```

- **为什么拆成两个方法**：单一 `reclaim`（边压实边返回被移除链根的列值）
  会把索引清理挤到压实**之后**——engine 拿到返回值才能删索引，恰好违反
  §4.1 顺序不变量（HeapCleanup 的 WAL 先于 BTreeDelete 落盘，正窗口
  悬空 TID）。拆分后 engine 在两个调用之间执行索引清理，不变量由
  调用序列结构化保证，而非靠实现者自觉。
- **索引清理不进 AM**：`notify_indexes` 不落进 `Vacuumable`。索引知识在
  engine 层（`indexes` registry + `encode_key`），AM 层拿不到也不该拿；
  `collect_index_keys` 返回解码列值而非字节，engine 免去二次回读即将
  被压实的页。
- **链分组**（哪些 dead tid 构成全死链、链根是谁）在 heap AM 内部以共享
  helper 实现（沿 `t_ctid` 走链、成员全落在 dead 集合才算全死），
  `collect_index_keys` 是其唯一出口；`reclaim` 不需要链知识 —— 它只按
  清单杀 slot。
- `Vec<Tid>` 物化沿用（access_method.rs:158-160 的 TODO 维持）：离线 vacuum
  全表物化可接受，在线化时再改迭代器。

### 4.5 Vacuum 的 WAL 化

**选择**：vacuum 的一切页修改**全部走 WAL**，复用既有路径：

- 页内压实 / slot 移除 / 前驱页 `next_page` 重链 → **`HeapCleanup = 8`**
  （crates/pg-storage/src/wal/record.rs:40，Stage 0 已预留；M2 文档 §10.1
  已把 payload 规划为 `(page_id, dead_slots[])` 并标注实现期"M3"，
  docs/phase1-m2-tech-selection.md:770 —— 本阶段落地该规划，payload 可
  按需扩展链 unlink 信息，重放逻辑与记录同步交付）。
- 页释放 → `PageFree = 41`（`free_page` 自带，page_allocator.rs:202）。
- 索引条目删除 → 普通 `BTreeDelete`（btree 在线 delete 自带 WAL）。

**理由**：压实/删除重放必须是确定性的物理操作，崩溃在 vacuum 中途时
redo 能把页恢复到一致态；不走 WAL 的页修改直接违反 M1 的
WAL-before-data 协议（M2 文档 §11.5）。

**重放收敛性**：`HeapCleanup` 的 redo handler 必须调用与在线路径**同一
个** `compact()` 函数（同参数），而不是另写一份"等价"重放逻辑 ——
恢复后的状态收敛靠"重放 = 重执行同一物理操作"保证，与 Stage S 让
redo/undo 共享 `apply_split_clr` 是同一原则。为此 payload 的
`dead_slots[]` 需按确定序（升序）写入，两侧输入一致则输出一致。

**代价**：vacuum 产生与死行数量成正比的 WAL 流量；离线模式下无
group-commit 竞争，吞吐可接受。`HeapCleanup` redo handler 需注册进
`RedoRegistry`（M2 Stage 0 的硬失败约定：遇到未注册类型 recovery 报错，
record.rs:18-20 —— 因此 handler 与记录必须同 stage 交付，不能只写记录）。

### 4.6 前置重构：insert 路径的槽位寻址

**背景（load-bearing 不变量）**：heap 页的 slot 号是 TID 的组成部分，
被索引条目与 HOT `t_ctid` 引用。当前 slot 分配是**隐式追加**：
`HeapAM::insert` 先预测 `slot = slot_count(page)`、按它写 `HeapInsert`
WAL，再调 `SlottedPage::add_tuple` 并 `debug_assert` 两者一致
（heap_am.rs:1278-1285）；redo 端同样调 `add_tuple`，slot 分歧即硬报错
（crates/pg-am-heap/src/redo.rs:86-96）。模块文档已明示 redo 依赖
"追加式 writer 复现相同 slot"（heap_am.rs:56-64）。

**问题**：`add_tuple` 内部带 Unused slot 的 first-fit 回收
（slotted_page.rs:242-253），只是生产路径从不产生 Unused —— heap 的
delete 是逻辑删除（LP 保持 Normal），不变量今天**恰好**成立。M3 的
`compact()` 会批量制造 Unused slot，此后 first-fit 命中时 `add_tuple`
的实际返回值将小于 insert 的预测值：debug 构建断言炸，release 构建
把错误的 slot 写进 WAL，redo 端必然报 slot diverged（或更糟——静默
复用已有 entry 的 slot 造成 TID 冲突）。

**选择**：把槽位选择从 `add_tuple` 中拆出，显式两步：

- `SlottedPage::first_fit_slot(page) -> Option<u16>`：纯读，返回第一个
  Unused slot（无则 None）；
- `SlottedPage::add_tuple_at(page, slot: u16, bytes) -> Result<()>`：在
  指定 slot 写入；`add_tuple` 退化为
  `first_fit_slot().unwrap_or(slot_count)` + `add_tuple_at` 的组合，
  对外行为不变。

在线路径改为：**先** `first_fit_slot`（或 slot_count）选定 slot，**再**
写 WAL（slot 号已是记录内的事实），**最后** `add_tuple_at(page, slot,
..)`；redo 端改为 `add_tuple_at(page, rec.slot_id, ..)`，删除"预测—
断言"式耦合。slot 分配从此由 WAL 记录显式承载，redo 不再依赖在线
writer 的行为巧合。

**代价**：一次行为中性的重构（今天没有 Unused slot，两条路径等价），
但触及在线 insert 与 HeapInsert redo 两条崩溃恢复核心路径。应作为
独立 stage **先行于 vacuum 落地**并全量回归（含 m2b crash rounds），
coding-plan 据此排期（另见 §11 R4）。

---

## §5 B+Tree 侧的配合

**选择**：

- vacuum 的批量 `delete(key, tid)` **直接走现有在线 delete 路径**
  （index.rs:2563）。Stage Q 已使 btree 并发安全（latch coupling +
  Blink 右链），无需为 vacuum 开侧门。
- **页合并不做**：M2c 既定决策（死页/稀疏页靠后续插入自然复用 slot，
  树高不缩），M3 维持。
- **页内死空间压实不做**：btree 页内已删条目的空间归 M3+ 或 vacuum
  扩展；M3 验收只要求 heap 空间不膨胀。

**理由**：btree delete 的正确性已被 M2c 并发测试与 undo 路径
（engine.rs:585 `apply_index_undo` 也在批量调 delete）覆盖；vacuum
持 `AccessExclusive` 时表上无并发 DML，btree 侧竞争甚至比在线更低。

**代价**：长期 churn 下索引体积缓慢增长（只删不合）；量化观测归 §6
统计，治理归 Phase 5b/7。

---

## §6 可观测性

对应 ROADMAP.md:208 的四项 + 查询统计，拆三个交付物。

### 6.1 WAL dump 工具（pg-waldump）

**选项**：(a) 独立 bin 目标挂在 `pg-storage` crate（`src/bin/pg-waldump.rs`）；
(b) 新建 `pg-tools` crate 集中未来所有 CLI。

**选择**：(a)。

**理由**：WAL 记录格式、`WalRecordType` 解码、segment 文件布局全部定义在
`pg-storage` 内，bin 与格式同 crate 演进、零新依赖边；M3 只有这一个 CLI，
为它单建 crate 是过度组织。未来工具增多再迁 (b)。

**功能**：人类可读 dump（LSN、记录类型、payload 关键字段逐条打印）+
 `--start-lsn / --end-lsn` 范围过滤。遇到 reserved 类型（无 handler 的
`SegmentSeal` 等）打印原始 payload 字节而非报错 —— 诊断工具以"尽量多
展示"为原则，与 recovery 的硬失败策略（record.rs:18-20）相反。

### 6.2 Engine 自省 API

**选择**：`Engine` 新增只读自省方法，直接拼装既有组件的查询能力：

| API | 数据来源（已存在，零新增） |
|-----|---------------------------|
| `active_xids() -> Vec<TxnId>` | `TxnManager::active_xids`（crates/pg-txn/src/manager.rs:485） |
| `wait_edges() -> Vec<(TxnId, TxnId)>` | `TxnManager::wait_edges`（manager.rs:400）+ `LockManager::table_lock_states`（crates/pg-txn/src/lock_manager.rs:421）合成完整 wait-for 图 |
| `table_lock_state(oid)` | `LockManager::table_lock_state`（lock_manager.rs:400） |
| `oldest_snapshot_xmin()` | §3 新增 registry（M3 交付） |
| `clog_hit_rate()` | `ClogBuffer::hit_rate`（crates/pg-txn/src/clog_buffer.rs:167，hits/misses 于 :94-97） |
| `buffer_pool_hit_rate()` | **M3 新增计数器** |

**Buffer Pool 计数器**是唯一的实现缺口：`BufferPool` 现有原子量只有
`checkpoint_lsn / flush_gen / synced_gen`（crates/pg-storage/src/buffer_pool.rs:164-173），
无 hits/misses。**选择**：按 `ClogBuffer` 同构模式补
`hits/misses: AtomicU64 + hit_rate() -> f64`（Relaxed 序、pin 路径命中自增、
读盘未命中自增），API 形状与 clog_buffer.rs:156-173 逐行对齐，不发明新风格。

### 6.3 查询统计（pg_stat_statements 等价物）

**选项**：(a) 落系统表（pg_stat_statements 形态的堆表）；(b) exec 层内存
ring buffer + 程序式读取 API。

**选择**：(b)。

**理由**：

- 系统表化意味着统计写入要走 heap/WAL/MVCC 全家桶，自引用（统计表本身
  的查询也产生统计）与启动顺序问题会把 M3 拖进 Phase 6 的范畴；M2 文档
  §十八已把系统表扩展归 Phase 6。
- ring buffer 零 I/O、无锁化可控（分段 Mutex 或 per-shard），丢失最老
  条目的语义对"诊断最近慢查询"恰好够用。

**设计**：`pg-engine` 内置 `QueryStats`，默认容量 **1000 条**（可配置），
每条记录 `{query 文本, 延迟, 影响/返回行数, 执行路径(seq scan / index
lookup), 时间戳}`；在 `Engine::exec`（engine.rs:2064）单点埋点，
auto-commit 与显式事务路径天然同被覆盖。

**代价**：进程重启即失；容量溢出丢弃最老条目。均可接受（诊断工具定位）。
另注：typed API（`Engine::scan` / `insert` / `update` / `delete` 等
程序式接口）不走 `exec`，**不产生统计** —— 统计面只覆盖 SQL 文本
路径，README 与验收口径按此表述。

---

## §7 PG Wire 极简版

### 7.1 Crate 归属

**选择**：新建 workspace 成员 **`crates/pg-wire`**，依赖 `pg-engine`。

**理由**：Phase 4a 要在协议层扩展（Extended Query、更多类型、COPY），
独立 crate 让协议演进不污染 engine；命名与现有 `pg-*` 一族一致。
`pg-wire` 只做协议编解码 + 连接管理 + SQL 透传，不含执行逻辑。

### 7.2 协议范围

**选择**：PostgreSQL v3 协议的最小闭集：

- **启动**：StartupMessage → `AuthenticationOk`（**trust，无认证**）→
  `ParameterStatus`（server_version 等少量必需项）→ `ReadyForQuery`。
  忽略 SSLRequest（回 `N`）与 GSSENCRequest。
- **查询**：仅 **Simple Query**（`Q`）。多语句串按序执行（auto-commit）。
- **事务控制**：`BEGIN` / `COMMIT` / `ROLLBACK` 在 pg-wire 层**拦截**并映射到
  `Engine::begin_txn` / `TxnHandle::commit` / `abort` —— 不能透传给
  `exec`（engine.rs:2077-2082 对这三类语句硬报错，事务控制是程序式 API）。
  每连接持有至多一个 `TxnHandle`。
- **结果**：`RowDescription` + 文本格式 `DataRow` + `CommandComplete`
  （`SELECT n` / `INSERT 0 n` / `UPDATE n` / `DELETE n` 标签）；
  错误 → `ErrorResponse` + `ReadyForQuery`；`Terminate`（`X`）正常关连。
- **类型映射（文本编码）**：INT4/INT8 十进制直出；TEXT 原样；NULL 以
  协议空值标记。Timestamptz/Uuid/Bytea（crates/pg-am-heap/src/tuple.rs:205-217
  的其余类型）按文本可读形式编码（µs 整数 / 标准 UUID 串 / `\x` hex），
  在 §12 验收覆盖，但 psql 元命令兼容性不在 M3 承诺范围。

**非目标**（§1 已列）：Extended Query、COPY、认证、取消请求
（CancelRequest）、TLS。

### 7.3 线程模型与 Engine 共享

**选项**：(a) `std::net::TcpListener` + 每连接一个 std 线程；(b) 引入
tokio 异步运行时。

**选择**：(a)。

**理由**：

- 实现现状是**全仓库 std::thread**：WAL writer（crates/pg-storage/src/wal/writer.rs:186）、
  死锁检测器（crates/pg-txn/src/deadlock.rs:191 起）均如此；引入 tokio
  意味着把 runtime 塞进每条连接的执行路径，与 engine 的全同步 API
  （`exec` 是阻塞调用）错配 —— async 包装同步阻塞调用只会白付
  spawn_blocking 开销。
- `Arc<Engine>` 可安全共享：`Engine` 全部字段为 `Arc` / `RwLock` / `Mutex`
  族（engine.rs:375-424），无任何 `RefCell`；唯一的 `RefCell` 在
  `TxnHandle.snapshot`（engine.rs:449），故 `TxnHandle` 是 `!Sync` ——
  线程模型下每个连接线程独占自己的 handle，恰好满足。
  AM 层的 `Send + Sync` 约束由 trait 显式要求（access_method.rs:115），
  Stage Q 已落地。coding-plan 应补一条 `Engine: Send + Sync` 的
  编译期断言把这个性质钉死（现状无此断言，靠结构推导）。

**代价**：

- 每连接一线程，千级连接时线程数爆炸 —— M3 目标是"psql 与标准驱动能
  连上跑 CRUD"（ROADMAP.md:215-216），不是连接数压测；归 Phase 4a
  再评估 runtime。
- **选型冲突提示**：M1 文档曾把 tokio 1.x 列为已选依赖
  （docs/phase1-m1-tech-selection.md:595），且 `pg-storage/Cargo.toml:12`
  至今声明着 tokio —— 但全仓库 `.rs` 代码对 tokio **零使用**（声明是
  死依赖）。本决策与"实现现状"一致、与 M1 文档字面不一致，见 §11 O4。

### 7.4 验收面

psql + **psycopg2 + node-postgres + rust-postgres** 四客户端连接并跑通
CREATE TABLE / INSERT / SELECT / UPDATE / DELETE（ROADMAP.md:215-216）。
驱动的 startup 探针（如 `SET`、`.pg_type` 查询、`SELECT version()`）中
无法支持的语句返回 `ErrorResponse` 但不断连 —— 极简版允许"部分语句报错，
连接可用"。

---

## §8 SegmentedStorage 接口预留

Phase 3（Inverted）与 Phase 5（TimeSeries）均为 segment-based 架构
（ROADMAP.md:210），M3 只**定义契约，不提供实现**。

**选择**：

- **trait 形状**（落在 `pg-storage`，与 WAL/Page 同层，供未来 AM 实现）：

```rust
pub trait SegmentedStorage {
    fn create_segment(&self) -> Result<SegmentId>;
    fn freeze(&self, id: SegmentId) -> Result<()>;   // 停写，仍可读
    fn seal(&self, id: SegmentId) -> Result<()>;     // 不可变，可 compaction
    fn merge(&self, ids: &[SegmentId]) -> Result<SegmentId>;
}
```

- **生命周期枚举**：`SegmentState { Active, Frozen, Sealed, Merging, Retired }`，
  状态机单向（Active → Frozen → Sealed → Merging/Retired）。
- **WAL 判别式**：`SegmentSeal = 110`、`SegmentMerge = 111` **已在 Stage C
  预留**（record.rs:80-83，`from_u8` 可解析、recovery 因无 handler 硬失败）。
  M3 **无需新增占号**，只把 payload 约定（segment id 列表、目标 id）写成
  doc 契约；实现期再注册 redo handler。

**理由**：接口先行可锁住 Phase 3/5 的架构方向（segment 生命周期 + WAL
记录族），避免两个 phase 各自发明；只定义不实现，则 M3 不为未验证的
需求付实现成本。

**代价**：预留的 trait 签名可能在 Phase 3 落地时被证明不合适（如 merge
需要携带 LSN 区间）—— 预留即承诺，改签名要过一遍修订记录；接受此风险。

---

## §9 Tier 2 接口预留

对应 ROADMAP.md:211 的三件套，同样只定义不实现：

- **`WalTailReader` trait**：订阅 WAL 尾部（从指定 LSN 起按序吐出已 flush
  的记录），供 Tier 2 索引（HNSW/倒排等）异步跟随。落 `pg-storage`，
  签名约定 `fn tail_from(&self, start: Lsn) -> Box<dyn Iterator<Item = Result<WalRecord>>>`，
  背压与断点续传语义写入 doc，M3 不实现 reader 本体。
- **`WatermarkRegistry` trait**：per-索引新鲜度水位（`index_oid ->
  applied_lsn` 的存取），Tier 2 reader 推进水位、planner 查询水位。
  内存实现即可满足未来需求，但 M3 连内存实现也不交付，只定 trait。
- **索引新鲜度 hook**：`AccessMethod` 预留 `fn freshness(&self) ->
  Option<Lsn> { None }` 默认方法（**默认实现 = 不影响任何现有 AM**），
  planner/executor 未来可据此决定"走索引还是回退全表"。

**理由**：与 §8 同一原则 —— 契约先行锁住方向（WAL 跟随 + 水位 +
planner 感知），实现归各自 Phase。`freshness` 用带默认实现的方法而非新
trait，避免现有 AM（heap/btree）任何改动。

**代价**：同 §8，签名可能返工；`WalTailReader` 的 iterator 物化形状与
§2 在线 vacuum 的物化问题同源，实现期都可能改回调/流式。

---

## §10 依赖

**选择：零新运行时依赖。**

- PG Wire 协议**手写编解码**（消息头 1B 类型 + 4B 长度，payload 逐字段
  big-endian），不引 `pgwire` crate。
  - 选项讨论：(a) `pgwire` crate（成熟、省工，但把协议正确性交给外部
    维护节奏，且其 async 模型会反向把 tokio 拖进连接路径，与 §7.3
    冲突）；(b) 手写（v3 协议最小闭集仅十余种消息，文本格式无二进制
    编码工作量）。**选 (b)**，与 ROADMAP "自研可控" 与 M1/M2 "手写关键
    格式"（tuple 编码手写，M2 文档 §十七）的传统一致。
- 统计 ring buffer、buffer pool 计数器、快照 registry 全部用 std /
  parking_lot 既有件。
- **dev-dependency 可用测试驱动客户端**（如 rust-postgres 的同步封装）
  做 §12 的四客户端验收，不进运行时依赖图。

**代价**：手写协议意味着 psql 兼容性边角（启动参数协商、错误字段码）
靠自己踩，§12 验收是唯一防线 —— 用四客户端矩阵对冲。

---

## §11 风险与开放问题

**风险**：

- **R1 Vacuum × checkpoint/WAL 交互**：vacuum 的页修改全部 WAL 化
  （§4.5）后，checkpoint 的 FPI 规则（checkpoint 周期内页首次修改记
  全页像）自动覆盖 vacuum —— 但 vacuum 短时间内批量改页会放大 WAL
  体积与 checkpoint 尾部的 flush 压力。`free_page` 与 checkpoint 的并发
  协议（crates/pg-storage/src/checkpoint.rs:361-414 的 LSN 预留临界区）
  已在 Stage 0 修复并压测，vacuum 的 free 走同一路径天然继承该保证；
  压实产生的 FPI 放大无既有压测，归 §12 churn 压测观测。
- **R2 快照注册的性能影响**：§3 registry 使每条语句多两次 mutex 操作；
  纯 SELECT 从"零协调"变为"两次 registry 临界区"。预期 < 1%，若 churn
  压测显示回归，备选是分片 registry（xmin 聚合仍取全局 min）。
- **R3 psql 兼容性风险面**：psql 启动即发一批 catalog 查询（`pg_catalog.
  pg_type` 等），极简版答不出 → 只能保证"报错不断连 + 基本 CRUD 可用"。
  交互体验（`\d` 等反斜杠命令）M3 不承诺。这是 ROADMAP 验收（"psql 能
  连接并执行基本 SQL"）与完整兼容之间的固有落差，验收时按 §12 口径执行。
- **R4 insert 槽位寻址重构（§4.6）**：把 slot 选择显式化会同时动
  HeapInsert 的在线与 redo 两条路径，属"行为中性但触及崩溃恢复核心"
  的重构。缓解：独立 stage 先行落地，全量回归（含 m2b crash rounds）
  通过后再进 vacuum stage。

**开放问题**：

- **O1 panic 泄漏 horizon**：默认 unwind 策略下 panic **会**执行 guard 的
  `Drop`，快照正常注销、horizon 不受影响；仅 `panic=abort`（本项目未启用）
  或 `mem::forget` 会泄漏使 horizon 永久偏低（回收失效但不丢正确性）。
  因泄漏面比预想更小，"不加进程级护栏"的结论反而更稳（coding-plan 已按
  不加护栏决定）。
- **O2（已由 Stage S 解决）`scan_dead_tuples` 的崩溃孤儿**：原债务是
  "xmin 属于崩溃事务（CLOG 读作 InProgress）的插入元组永远不被收集"
  （heap_am.rs 原 1612-1622 建档："InProgress ≡ Aborted" 只对可见性
  成立）。Stage S 的 `HeapUndoHandler`
  （crates/pg-am-heap/src/undo.rs:5-37，接线于 engine.rs:631）在
  recovery undo 阶段把 ATT 残余成员直接标 ABORTED 于 CLOG，此后
  `scan_dead_tuples` 规则 1（`t_xmin` aborted → 死）正常收集崩溃
  孤儿。M3 vacuum 不再继承该限制，§12.1 的崩溃注入场景无需单独口径；
  heap_am.rs 的建档注释已随本 review 同步更新。
- **O3 HOT 部分死链不 prune 与 M2 文档的偏差**：M2 §十八写"HOT prune
  推迟到 M3 vacuum"，本方案 §4.2 只做整链回收（避免 LP 格式变更）。
  若后续 review 认为链内 prune 必须 M3 做，则需启动 LP 状态扩展 +
  格式迁移评估，工期另计。
- **O4 tokio 死依赖**：`pg-storage/Cargo.toml:12` 声明 tokio 但全仓库零
  使用（§7.3）。M3 是否顺手移除该声明？倾向移除（死依赖误导选型），
  但属范围外清理，留 review 定夺。
- **O5 Engine: Send + Sync 无编译期断言**：§7.3 依据字段结构推导成立，
  但任何未来字段（如某个 `Rc`/`RefCell`）都会静默破坏它。建议
  coding-plan 在 pg-wire 接线处加 `static_assertions` 式断言；不引
  `static_assertions` crate 的话用裸 fn 断言，零依赖。
- **O6 Sequence（SERIAL）**：M2 §十八列在 M3，ROADMAP M3 五块未含。
  若确认不做，应回改 M2 文档索引，避免两份文档口径漂移。

---

## §12 验收标准草案（供 coding-plan 引用）

对应 ROADMAP.md:213-217，逐条可操作化：

1. **Vacuum 空间复用**：churn 压测 —— 固定行数表上 N 轮
   "UPDATE/DELETE 一批 + INSERT 一批"，每 K 轮跑一次 `vacuum`；
   断言数据文件页数有界（不随轮数线性增长），且 vacuum 后
   `scan_dead_tuples(horizon=最新)` 返回空（含崩溃注入轮：O2 已由
   Stage S 清偿，无需单独口径）。**崩溃窗口回归**（覆盖 §4.1 顺序
   不变量与 §4.6 重构）：在"索引清理 WAL 已落盘、HeapCleanup 未落盘"
   与"压实已落盘、PageFree 未落盘"两个窗口各注入一次崩溃，恢复后
   断言索引扫描与堆扫描一致、无悬空 TID 读到错误行；再做一次
   "压实后 slot 复用 + 新插入 + 崩溃"确认 redo 按记录 slot 复现。
2. **psql + 三驱动 CRUD**：psql、psycopg2、node-postgres、rust-postgres
   各自连接，执行 CREATE TABLE / INSERT / SELECT / UPDATE / DELETE /
   BEGIN / COMMIT / ROLLBACK 全通过（ROADMAP.md:215-216）。
3. **waldump 可读**：`pg-waldump` 对含各类记录的 WAL dump 全量可解析，
   LSN 范围过滤结果与边界一致；reserved 类型不崩（§6.1）。
4. **统计 API 可用**：`active_xids` / `wait_edges` / `table_lock_state` /
   `clog_hit_rate` / `buffer_pool_hit_rate` / `oldest_snapshot_xmin` /
   query stats ring buffer 均有集成测试覆盖；命令行可诊断事务与锁问题
   （ROADMAP.md:217 —— 由 §6.2 API + 简单 CLI/SQL 出口满足）。
5. **全量回归不 regress**：M1/M2 全部集成测试 + crash_recovery 自动化
   通过；100 并发 CRUD TPS 相对 M2c 基线回归 < 5%（覆盖 R2 的注册
   开销）。

---

## 修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| v1.0 | 2026-08 | 初稿。五块内容（Vacuum / 可观测性 / PG Wire 极简版 / SegmentedStorage 预留 / Tier 2 预留）全部决策落定；§11 列出 3 项风险与 6 项开放问题待 review。 |
| v1.1 | 2026-08 | review P1 修正：§3.2/§3.3 快照调用点由"三条"更正为**六处**（补 `Engine::scan`/`index_lookup` 两个无锁读者与 `create_index` 的 re-snapshot，注销点同步补齐）；§3.3 安全论证的不等式方向写反（`新快照.xmin ≤ 活跃 xid`），改写为集合包含式证明。 |
| v1.2 | 2026-08 | 在线 vacuum 归属锚定：由模糊的"归 M3+"明确为 **Phase 5b（多 AM GC 协调器）**，autovacuum 生产形态归 Phase 7a；ROADMAP.md 四处（M3 结构图条目、M3 表格、Phase 5b.1 GC 协调器、Phase 7a 表格）已同步更新。 |
| v1.2 | 2026-08 | review P1/P2 修正：① §4.1 流水线由"压实→索引清理"改为五阶段"收集→提取key→索引清理→页内压实→页释放"，新增**顺序不变量**（使 TID 失效的 WAL 必须晚于该 TID 的 BTreeDelete 落盘；靠阶段顺序 + WAL 前缀式 flush 结构成立）与页 latch 并发注记；② §4.4 trait 拆为 `collect_index_keys`（只读）+ `reclaim`（纯物理）—— 原单一 `reclaim` 返回列值的形状会强制压实先于索引清理，违反①；③ 新增 §4.6 insert 槽位寻址重构（`first_fit_slot`/`add_tuple_at`）：`add_tuple` 的 first-fit 与"追加式预测 slot"不变量在 compact 制造 Unused slot 后必然分歧，须先行落地；④ §4.3 补工作负载真相（eager 索引维护下，vacuum 的实际删除对象只有崩溃 loser 的悬挂条目）与 EntryNotFound→Ok 硬性要求；⑤ §3.3 v1.1 的集合包含引理不成立（BEGIN 可在 vacuum 期间进入 active set），改用 XID 单调性重写证明；⑥ §4.2 更正膨胀面（HOT 只膨胀堆，索引条目由最终杀手 eager 删除）；⑦ §4.5 补重放收敛性（redo 调用同一 `compact()`）；⑧ §6.3 注明 typed API 不入统计；⑨ O2 标记为 Stage S 已解决（`HeapUndoHandler` 在 recovery undo 标 ATT 成员 ABORTED）；⑩ §11 新增 R4；⑪ §12.1 撤销崩溃注入豁免并新增两个崩溃窗口回归项；⑫ 同步更新 heap_am.rs 两处 stale 的 ATT 建档注释。 |
| v1.3 | 2026-08 | coding-plan 对抗性 review B1 修正：§3.3 快照注册由"guard/token 返回调用方或 caller-wrapper 包装"修订为**注册沉入 `snapshot()` 临界区、与快照构造原子**（签名 `snapshot() -> (Snapshot, SnapshotGuard)`，registry 与 active set/clock 合并同一把锁），并补 `Snapshot` 构造器 pg-txn 私有化的反枚举护栏；原 caller-wrapper 选项否决——构造—注册窗口使 horizon 漏盖在飞快照（反例见 §3.3），`AccessExclusive` 无法补救（取快照先于任何锁申请）。 |
| v1.4 | 2026-08 | coding-plan P2 修正：① 护栏表述更正——`snapshot()` 是唯一的**注册**构造点而非唯一构造点：`Snapshot::everything()`（xmin=0）为明确的不注册特例（目录引导/测试），注册它会把 horizon 钉死在 0；CI grep 需同时覆盖字面构造与新增关联构造函数；"构造器私有化"实为字段私有化，engine.rs 21 处字段访问随迁为访问器。② §3.3 step 2 措辞更正：vacuum 期间进入 active set 的是新 BEGIN **与 auto-commit DML**（auto_commit 先分配 XID 入 active set、再在语句体内申请表锁，被锁挡住的只是语句体）；结论不变（XID 单调性对两类新进入者均成立）。 |
| v1.5 | 2026-08 | panic 语义事实更正（§3 代价节与 §11 O1）：默认 unwind 策略下 panic **会**执行 guard 的 `Drop`（正常注销，horizon 不受影响），仅 `panic=abort`（本项目未启用）或 `mem::forget` 泄漏；"不加护栏"结论不变且更稳。与 Stage A 代码注释（review F1 修正）对齐。 |
| v1.6 | 2026-08 | Stage A review P2 修正：horizon 空 registry 回落由"直接取 clock"改为"先取 active set 最小 XID、皆空才取 clock"——begin→snapshot 窗口内（已入 active set、尚未注册快照）取 clock 可使 horizon 高于该事务即将注册的 xmin；PG OldestXmin 同构（backend xid 与快照 xmin 均参与水位）。测试 `horizon_empty_registry_uses_clock` 更名并重写为 `horizon_empty_registry_falls_back_to_min_active_then_clock`。 |
| v1.7 | 2026-08 | Stage C 设计终审注记：§4.4 原文"reclaim 不需要链知识，只按清单杀 slot"与实现存在有意偏差——`reclaim` 内部用同一个链分组 helper 重推导 kill 集，使"部分死链零回收"成为结构保证而非调用方自律（engine 会把 `scan_dead_tuples` 的原始输出直接传给 `reclaim`，其中含部分死链的死亡成员，盲杀即违反 `compact()` kill-list 契约）。判定为正确选择，coding-plan 的"唯一出口"措辞已同步修正。 |
