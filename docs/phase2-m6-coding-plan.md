# Phase 2 M6 编码顺序

> 基于 `docs/phase2-m6-tech-selection.md` **v1.4**(五轮审查闭环,修订见该文档
> 文末记录;本计划一切阶段任务、交付物、验收命令均可回溯到其 § 节,行内以
> 选型 §x.y 引用),按依赖关系排列的 M6 阶段编码执行计划。M6 交付三块内容
> (ROADMAP.md:280-299 Phase 2c,范围切分与两处偏离立文见选型 §1/§5/§6.1):
> **HNSW 并发协议(节点级 latch,读路径 latch-free)+ Tier 1 事务集成
> (图内立即写 + heap MVCC 可见性 + insert abort tombstone undo;
delete 图动作推迟到 commit 后)+
> 可见性过滤与 vacuum 扩展(tombstone 生效、局部重建;物理回收与 EBR 归
> 格式升级窗口,不在本计划)**。每个阶段必须先通过单元/集成测试与对抗性
> review,再进入下一阶段。
>
> ```
> 基建   → 0 (128 记录类三件套 + ColumnType::Vector + 可达性审计先导)   3–4 天
> 换序   → A (insert 5↔6 对调,单写者形态下落地 + 全量对拍重钉)         2–3 天
> 并发   → B (句柄 &self 化 + latch 表 + 临界区 + loom)                 4–6 天
> 事务   → C (行内 NodeId 载体 + 旁路映射 + DML hook + IndexUndo)        4–5 天
> 可见性 → D (过滤管线 + ef 放大重搜 + ground truth 口径)                3–4 天
> vacuum → E (泄漏账监控 + 局部重建重定向)                               3–4 天
> 收口   → F (100 并发 24h + 并发×SIGKILL + 对标 + 归档 + tag)           5–7 天
> ```
>
> **总计 7 个 stage,串行口径 23–32 天(1 名高级 Rust 工程师);选型已定稿
> v1.4,本计划不含设计返工余量,工艺风险集中在 B(并发协议)与 F(并发×
> 崩溃验收)两阶段**

---

## v1.0 硬约束速查

M6 开工前请通读 tech-selection §4/§6/§7/§10。以下 12 条为**编码期每天都要
对照**的硬性约束(违反则退回该 stage 重做),全部是选型文档四轮审查钉死的
冻结契约,引用格式 = 选型 §节:

- **M5 冻结边界(§1)**:WAL 记录 121–127、页格式、redo handler、审计口径
  一律不动;M6 **零新增记录类**(选型 §10,v1.4:un-tombstone 随 delete
  图动作推迟到 commit 后而作废,判别值 128 保持空闲);**xid 不进图
  WAL**(require_utility_txn 门不动,§6.2 零动作裁决)。任何"给既有
  记录加字段"的冲动 = 格式版本升级,回推选型升版。(计数口径:"8
  冻结" = 121–127 七 handler 类 + LogicalHnsw=100 预留位;M6 零新增)
- **新序基线(§4.2/§8.2)**:M6 的 insert 序 = 5↔6 对调后(先自身各层
  邻表,后改他人邻表);Stage A 在单写者形态下完成换序并重钉全部窗口/
  对拍/审计,Stage B 的并发化不得再动序。
- **hwm 临界区整段(§4.2 步骤 1)**:分配 NodeId → rng 抽层 → 备页
  (PageAlloc + FPI)→ NodeInit → DirAppend(含 DirLink 分支)**同临界区**;
  目录 position-is-identity,拆段即张冠李戴。该临界区是 insert 全局串行点
  (§13 风险已立文),选型层不承诺线性扩展。
- **meta 临界区重查(§4.2 步骤 7)**:空图首插判定必须在临界区内重查
  (并发首插回补正常连边);max_level/entry_point **只升不降**(临界区内
  重读取 max),防并发降级。
- **锁序(§4.3)**:表级 LockManager → 节点 latch(升序 NodeId,**同时
  最多一把**)→ buffer pool 帧 latch → WAL writer 内部锁;**beam-search
  遍历期不持任何节点 latch**,短命帧 pin 不带入节点 latch 临界区。
- **state 位不作搜索谓词(§4.2 澄清)**:M5 搜索从不检查状态位,M6 同;
  INITIALIZING/tombstoned 节点可承担路由,结果过滤全在回表层(§7)。
  禁止在图搜索路径发明"LIVE-only"过滤(对拍冲击未经评估即引入 = 退回)。
- **commit 记录单独决定可见性(§6.2)**:事务内 insert 的图记录零
  flush(commit 硬序覆盖,WAL 前缀性质);**delete 的图动作(tombstone
  + 映射删除)= commit 持久化后的 post-commit 清理**(幂等可重入,
  崩溃跳过由 §9 死行通道补刀);语句 Ok 不再承诺单条记录持久——M5
  §8.1 边界① 仅在非事务 utility 路径保留,两处口径不得混写。
- **崩溃恢复对图零动作(§6.2)**:recovery 的 undo 段不向图发任何记录;
  in-flight 节点的不可见性由 heap 行 MVCC 承载,残留归 §9 vacuum 记账。
  undo 仅在线路径(IndexUndo 扩展,engine.rs:188-209 挂载点,**仅
  insert 方向**——delete 的 abort 图零动作,v1.4)。in-flight delete
  没有图记录可持久化(delete 图动作在 commit 后才存在)——v1.4 P1
  形态(未提交 tombstone 经 WAL-before-data/前缀 flush 落盘,崩溃后
  活行配 tombstoned 节点)在结构上不可构造。
- **关联双通道(§7)**:heap 行内 NodeId = 权威载体(delete/vacuum 直达);
  旁路 B+Tree(NodeId→ctid)= 搜索正查;回表**行内 NodeId 复核,错配即弃**
  (损失一个候选,永不产生错结果)。禁止只建单向映射。
- **pg-am-hnsw 保持 MVCC-free(§2)**:不新增 → pg-txn 依赖边;Snapshot/
  ClogAccessor 只出现在 pg-engine 层。grep 护栏每 stage 出口核对。
- **审计 quiesce 后运行(§12.5)**:并发进行中审计会把合法中间态(半连接、
  INITIALIZING)误判腐坏;一切审计门在 stress 停止后跑。
- **物理回收与 EBR 不在 M6(§5/§9)**:槽位复用/页进 freelist 撞页格式
  冻结与 NodeInit 占用校验,归格式升级窗口;M6 只交付标记 + 重定向 +
  泄漏记账。编码期不得"顺手"实现回收。

---

## 工程规则(每 stage 通用,继承 M4/M5 惯例,M6 适配)

- **每 stage 一个 commit**:message 前缀 `PHASE2-M6-StageX`;**未经用户确认不
  执行任何 git mutation**;author 固定 `wangzq23 <wy823034583@gmail.com>`。
  出口 tag(`phase2-m6`)只在用户确认后打,打在 main 的 merge commit 上
  (对齐 `phase1-m3`/`phase2-m4` 先例;`phase2-m5` 的同型流程在途)。
- **stage_spec 归档**:每 stage 收口时在 `docs/stage_spec.md` 追加该 stage 的
  "交付内容 / 与 pgvector·hnswlib 的 trade-off / 已知残留与后续归队"三小节
  (M4 Stage A 欠账的教训:从 Stage 0 起每 stage 当次写完,不后补)。
- **对抗性 review**:每 stage 完成后一轮对抗审查(P1 必修、P2 登记、P3 尽修);
  M6 选型本身经四轮审查,coding 期审查面 = 实现与选型的逐行对应(临界区
  边界、锁序、新序与窗口表重编号、冻结清单、过滤管线步骤)。
- **M6 的验证形态映射**(选型 §12):**loom 回归适用**(并发首次进 HNSW——
  参照 btree_loom 形态,sync-alias 规则已在 pg-storage 立文);崩溃注入 =
  mem::forget 窗口矩阵(换序重编号)+ 真 SIGKILL 子进程轮次(并发版);
  watchdog 纪律保留(一切可能阻塞的测试带硬超时;loom 模型的 preemption
  bound 纪律同 ci.yml 既有 loom job)。
- **回归传承**:每 stage 出口 `cargo test --workspace` 全绿;动到
  pg-storage/pg-txn/pg-am-heap 既有行为的改动必须在本 stage 说明理由并跑
  全量;**M4/M5 全部门禁每 stage 出口必绿**(见"回归测试传承"节)。
- **冲突处理**:实现与 tech-selection 引用不符时,以代码为准并回改选型文档;
  决策层冲突不擅改,记入"开放问题与冲突标注"。

---

## 阶段 0:ColumnType::Vector/Datum + 可达性审计先导 + probe 落锤(2–3 天)

**归属**:M6 地基
**前置**:M5 收口(`phase2-m5` tag 已打——**若 Stage E 尚在途,本 stage 与
其仅剩的 CI/tag 出口项可并行,代码面无交集**);tech-selection v1.4 用户
终审通过
**目标**:VECTOR 列类型落 heap(ColumnType + Datum + Value 链路);
审计的入口点可达性扫描能力就绪(§8.3 先导——不落地则 §14.1 悬挂
节点判项不可用);probe 处置落锤。**M6 零新增 WAL 记录类**(v1.4),
本 stage 无 pg-storage 面。

| 任务 | 交付物 |
|------|--------|
| ColumnType::Vector(dim)(选型 §11) | pg-am-heap `tuple.rs:205` 枚举扩展 + `fixed_width`(:222)返回 `Some(dim*4)`(内联语义同步);**dim ≤ 2000 创建时硬校验,超限响亮报错**(选型 §11;超维归 M5 O1 既定窗口不动);encode/decode 往返 + 边界钉(dim=1/2000/2001);pg-catalog re-export 消费侧编译核对(builtin_types.rs:8 先例);**Datum::Vector(Vec<f32>) 变体同步**(tuple.rs:234,tuple 编码的入口类型)+ 引擎 `Value = Option<Datum>`(engine.rs:273)链路接通——encode/decode 往返经 Datum 层走全程 |
| 审计可达性扫描(选型 §8.3 先导) | pg-am-hnsw `audit.rs`:自入口点逐层 BFS 全图可达性扫描,报告"LIVE 而不可达"节点集(quiesce 前提,rustdoc 立文 §12.5 口径);空图/单节点/多层图正例 + 人为断边负例各一枚;扫描成本实测登记(喂 Stage F 的 24h 验收计时预算) |
| probe 处置落锤(选型 §4.1 登记项) | 侦察结论已现成:index.rs:115 `RefCell<ProbeState>` 无 cfg 门、全构建存在,生产路径为 no-op 分支(index.rs:71-75 注释自述)——直接落锤 **cfg(test) 剥离**;Stage B 施工,本 stage 在 stage_spec 立文 |

**关键约束**:
- 可达性扫描不碰写路径;只读 + quiesce 前提,不进 redo/搜索热路径

**验收命令**:
```bash
cargo test -p pg-am-heap
cargo test -p pg-am-hnsw
cargo test -p pg-engine
cargo test --workspace
```

---

## 阶段 A:insert 5↔6 对调(单写者形态)(2–3 天)

**归属**:幽灵陷阱消除(选型 §8.2 裁决落地)
**前置**:tech-selection v1.4 终审;**与 Stage 0 可并行**(代码面无交集;换序后残态判定用 M5 既有审计计数,Stage 0 的可达性扫描在 Stage F 验收前到位即可)
**目标**:M6 基线序在**单写者**形态下落地——先自身各层邻表、后改他人
邻表;全部受序影响的 M5 防线重钉;并发化(Stage B)面对的就是新序。

| 任务 | 交付物 |
|------|--------|
| 写入序对调(选型 §8.2) | pg-am-hnsw `index.rs` insert:自身各层 SetNeighbors 提前到他人邻表之前;记录序列与崩溃窗口语义按新序重推导,**"可达 ⟹ 出边完整"不变量**(§4.2)落成注释 + 审计断言挂钩 |
| 窗口矩阵重编号 + 审计负例(选型 §8.2) | `m5_insert_crash.rs` 十行窗口按新序重编号,标注与 M5 的对应关系(stage_spec 归档对账表);ProbeMark 九变体插桩点随新序重排;**审计负例同批更新**:残态注入用例随新序换序(幽灵形态 → 孤儿形态) |
| m5_ghost_trap 重排(选型 §8.2 测试联动) | ghost_trap 语义反转:旧序的"有入边空出边"幽灵**不再可构造**,测试改为断言新序下对应窗口产出"有出边无入边"孤儿 + 孤儿不影响搜索(recall 钉) |
| 位级对拍孪生重跑 | 内存孪生同步换序,两形态逐位等价重钉;`recall_siftsmall` 0.9990 门与 `m5_recall_after_recovery` 重跑——**recall 影响实测数据随 stage_spec 落盘**(选型 §8.2:理论近似中性,实测钉) |
| crash rounds 复跑 | 25 轮(CI 口径)复跑;残态期望表按新序更新(幽灵行 → 孤儿行) |

**关键约束**:
- 本 stage **单写者形态不动**(`&mut self` 句柄、无 latch)——序变更与并发化
  分两个 stage,任何一步回归都能归因
- 换序若使任一既有 recall/对拍门变红且非测试期望陈旧所致 = 退回并回推选型

**验收命令**:
```bash
cargo test -p pg-am-hnsw && cargo test -p pg-am-hnsw --release
cargo test -p pg-am-hnsw --test m5_insert_crash
cargo test -p pg-engine --test m5_hnsw_crash_rounds --test m5_ghost_trap
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test recall_siftsmall --test m5_recall_after_recovery
cargo test --workspace
```

---

## 阶段 B:并发协议本体(4–6 天)

**归属**:M6 第一决策(选型 §3/§4)的工艺主体——**工艺风险最高的 stage**
**前置**:Stage A(新序基线 + 全部对拍重钉)
**目标**:`HnswIndex` 共享句柄并发 insert/delete/search;latch 纪律与锁序
落码并经 loom 验证;单写者路径位级可复现性不破。

| 任务 | 交付物 |
|------|--------|
| 句柄形态变更(选型 §4.1) | `HnswIndex` 从 `&mut self` 单写者(index.rs:90-94 前提)改为 `&self` 共享句柄;hwm/rng/dir_pages/current_node_page 收进 latch 保护的 inner;probe 按 Stage 0 侦察结论处置;**单写者路径行为钉**:既有测试全套件**语义零改动**全绿(`&mut self`→`&self` 后 `let mut index` 绑定触发 unused_mut,clippy -D warnings 门下仅去 mut 的机械调整不可避免,不算语义变更)+ 单写者位级流与 Stage A 出口逐位一致(测试计数以落码时实测为准,不抄历史数字) |
| latch 表(选型 §4.1) | 进程级固定 4096 片 `Vec<RwLock<()>>`,NodeId 取模;一次性预分配,零 resize(分片数常量 = 互斥正确性前提,注释立文);hwm/meta 两临界区各一把全局 latch(与节点 latch 分层,§4.3 锁序) |
| 临界区化写入路径(选型 §4.2) | 步骤 0–4 整段进 hwm 临界区(含 DirLink 分支);meta 临界区重查(空图首插回补 + 只升不降);步骤 5/6 节点 latch 升序单持;PublishLive 原位翻转 |
| 读者目录快照自愈(选型 §4.1) | 解析命中"maps past the directory chain"时沿 next 链刷新快照重试一次,仍越界才 Corrupted;M5 目录解析错误文案同步调整;并发增长下的自愈测试(读者持旧快照 × 写者 DirLink) |
| 锁序立文 + loom 模型(选型 §4.3/§12.1) | **脚手架先行**:pg-am-hnsw Cargo.toml 加 `loom = ["pg-storage/loom", "dep:loom"]`(参照 pg-am-btree Cargo.toml:38 先例)+ 测试 required-features 门(ci.yml:83-85 同型);`hnsw_loom`(参照 btree_loom 形态):hwm 临界区全段交错、meta 首插竞争与只升不降、连边升序单持、PublishLive × 读者交错;preemption bound 纪律同既有 loom job;**遍历期不持节点 latch** 进 loom 断言面;**vacuum 重定向 × 并发 insert 面归 Stage E 扩展**(机制在 E 才存在,选型 §12.1 第三面) |
| delete 并发原语(选型 §8.1) | tombstone 的并发形态(节点 latch + 原位写);与 insert 的 latch 纪律同型;tombstone × 读者交错进 loom 清单 |

**关键约束**:
- 锁序违反 = P1;loom 模型即回归门(选型 §13)
- 并发结果的位级口径 = "合法形态集"(审计 + recall 门),不钉单一参照流;
  **单写者路径位级可复现性不破**(选型 §13,两口径不得混)
- hwm 临界区是已知串行点(§13),本 stage 不做批量预分配等缓释——先正确,
  缓释归 Stage F 实测后的调优项

**验收命令**:
```bash
cargo test -p pg-am-hnsw && cargo test -p pg-am-hnsw --release
cargo test -p pg-am-hnsw --test hnsw_loom --release   # loom tier 纪律
cargo test --workspace
```

---

## 阶段 C:事务集成 Tier 1(4–5 天)

**归属**:选型 §6/§7 写侧
**前置**:Stage 0(ColumnType::Vector/Datum)、Stage B(并发句柄——
hook 直接写在最终 &self API 上,避免二次改造)
**目标**:向量 DML 进事务;commit 不碰图;abort 对偶 undo;崩溃恢复图零
动作经测试钉死。

| 任务 | 交付物 |
|------|--------|
| **行内 NodeId 载体编码冻结(选型 §7,本 stage 首任务)** | 隐藏系统列 vs tuple 伴随字段,二选一立文(编码钉 + 格式影响面写进 stage_spec);heap 行读取侧访问器;**裁决点**:载体是 M2 tuple 格式的首处语义扩展,冻结前过用户终审 |
| 旁路映射(选型 §7) | 引擎层 NodeId→ctid 映射,复用 pg-am-btree(M2c 并发已验);insert/delete 同事务维护;事务性由 btree 既有 IndexUndo 对偶承接 |
| DML hook(选型 §11) | 引擎 insert/delete/update 行路径(engine.rs:1753/1842/1933)在表上有 hnsw 索引时触发:insert = 图 insert + 行内 NodeId 写回 + 映射条目;**delete statement 期 = heap xmax only(图与映射零动作),commit 持久化后 = post-commit 清理(tombstone + 映射删除,幂等可重入,失败/崩溃由 Stage E 死行通道补刀)**;**update 分两类**(非向量列 = 图零动作 + 映射 ctid 更新 + 新行携带同 NodeId;向量列 = delete+insert 组合,delete 半沿 post-commit 口径);hook 失败 = 语句失败即 abort(既有) |
| SQL 最小集(选型 §11) | `Literal::Vector(Vec<f32>)` 进 sql.rs(tokenizer/parser:`[0.1, 0.2]` 字面量;现状仅 Int/Str/Null 三变体)+ CREATE TABLE 列类型声明 `VECTOR(n)` → ColumnType::Vector(dim)(dim ≤ 2000 硬校验承接 Stage 0);`<=>` 运算符与 CREATE INDEX 归 Phase 4b 不动——约半天工作量,随 DML hook 同 stage 验证 |
| IndexUndo 扩展(选型 §6.2) | engine.rs:188-209 的 Inserted/Deleted 对偶扩展 HNSW 臂:**仅 insert 方向**——insert abort → HnswNodeTombstone(定位经行内 NodeId 直达,无反查);**delete 的 abort = 图零动作**(delete 自始不碰图,无逆操作需求;v1.1 的 un-tombstone/128 设计作废,v1.4) |
| flush 边界迁移(选型 §6.2/§10) | 事务内图记录零 flush;commit 硬序 flush 覆盖(WAL 前缀性质,钉测试:事务内 insert 不触发 flush、commit 后恢复可见);语句 Ok 不承诺持久的口径进 rustdoc;utility 路径的 M5 边界① 保持不动 |
| 崩溃零动作钉(选型 §6.2) | 崩溃轮次扩展形态:in-flight 事务 × SIGKILL → 恢复后**图 WAL 零 undo 记录**(waldump 断言)+ 不可见性由回表过滤钉 + 残留计数 ≤ 崩溃窗口 × 并发度(审计钉);**P1 回归钉**:in-flight delete × checkpoint(强制未提交记录落盘)× SIGKILL → 恢复后行可见 ∧ 节点 LIVE ∧ 映射在(v1.4 修复的形态,永不回归) |

**关键约束**:
- xid 不进图 WAL(121–127 全部 utility 形态,128 保持空闲);任何"给记录盖 xid"的
  局部冲动 = 回推选型
- update 的映射指向最新版本;窗口期候选丢失由 §7 复核 + ef 放大兜底,
  登记为 recall 观察项(stage_spec 落账)

**验收命令**:
```bash
cargo test -p pg-engine
cargo test -p pg-am-hnsw
cargo test --workspace && cargo test --workspace --release
```

---

## 阶段 D:可见性过滤管线 + tombstone 生效(3–4 天)

**归属**:选型 §7 读侧 + §8.1
**前置**:Stage C(映射与 hook 就位)
**目标**:搜索经过滤管线返回事务可见的 top-k;recall 门按过滤后口径计量。

| 任务 | 交付物 |
|------|--------|
| 过滤管线(选型 §7 五步) | 引擎层 `hnsw_search` 包装:2x 候选 → 批量映射正查 → 回表 MVCC 过滤(visibility.rs:160 既有,只读不取行锁)+ **行内 NodeId 复核(错配即弃)** → 不足 k 条 ef 倍增重搜(**≤3 轮封顶**,终止即返回可见子集,允许少于 k) |
| tombstone 读侧生效(选型 §8.1) | 映射已删 → 正查无命中(主通道);回表过滤兜底;tombstoned 节点可路由不进结果集;图内跳过优化不做(登记为 vacuum 前性能观察项) |
| ground truth 与 recall 口径(选型 §7/§12.3) | ground truth = **可见行集合上的暴力 top-k**;recall@10 过滤后 ≥ 单线程基线 95% 的常驻测试;病态放大(无可见行/全量 tombstoned)负例钉住封顶行为 |
| 并发 × 过滤集成测试 | 并发 insert/delete × 并发 search:结果集无 abort 行、无他人物理行(复核钉)、无重复;quiesce 后审计全绿(§12.5 口径) |

**关键约束**:
- pg-am-hnsw 保持 MVCC-free——过滤管线全部在 pg-engine;grep 护栏
  (`pg_am_hnsw.*pg_txn` 反向依赖)出口核对
- ef 放大的 2x 起步值在本 stage 由 recall 门自证;不达标先调候选倍数,
  仍不达标 = 回推选型(§7 优化登记:条目带 TID+XID,格式窗口)

**验收命令**:
```bash
cargo test -p pg-engine
M4_REQUIRE_DATASET=1 cargo test -p pg-engine --release --test m6_search_visibility   # 若独立成文件
cargo test --workspace
```

---

## 阶段 E:vacuum 扩展(3–4 天)

**归属**:选型 §8.3/§9
**前置**:Stage D(删除生命周期完整,泄漏形态全部可见)
**目标**:泄漏账监控 + 局部重建;**物理回收不做**(§9 收缩口径)。

| 任务 | 交付物 |
|------|--------|
| 泄漏账监控(选型 §9) | 审计计数输出:tombstoned + initializing/orphan + **孤儿映射条目**三项合计;比例阈值(默认 10%,可配)触发局部重建建议;计数进 stage_spec 口径表 |
| 局部重建(选型 §9) | tombstone 节点的邻居重定向(入边重定向到次近邻);**vacuum 线程独占执行**,不与并发 insert 并发(节点 latch 保证);重定向 WAL 化(复用 122 SetNeighbors 原位覆写,无新记录类) |
| 死行驱动补刀(选型 §9,post-commit 清理完成通道,v1.4) | heap vacuum 的 committed-dead 扫描读出死行行内 NodeId → 补 tombstone + 删映射(M3 Vacuumable collect_index_keys 先例同型);commit 后崩溃/失败跳过的 post-commit 清理在此收口——**正确性部件,非纯优化** |
| loom 扩展(选型 §12.1 第三面) | hnsw_loom 补 vacuum 重定向 × 并发 insert 交错——"独占执行"由模型证明(是机制不是断言) |
| 不对称率预算核验(选型 §9/§12.3) | 重定向引入的单侧边计入不对称率**增量**预算;重建前后不对称率实测对账,增量 <1% 门不破(超支 = 响亮报告,不静默放行) |
| 孤儿/INITIALIZING 残留处置 | 审计发现 → 记账 → 阈值触发清理通道(只标记/重定向,不释放页);残留上界 ≤ 崩溃窗口 × 并发度(§8.3)进审计断言 |

**关键约束**:
- 槽位复用/页进 freelist/EBR **一律不做**(撞页格式冻结与 NodeInit 占用
  校验,§5/§9);编码期"顺手回收" = 退回
- vacuum 与 heap vacuum 各管各的,互不阻塞(§9)

**验收命令**:
```bash
cargo test -p pg-am-hnsw && cargo test -p pg-engine
cargo test --workspace
```

---

## 阶段 F:验收 + 对标 + 收口(5–7 天)

**归属**:M6 出口(选型 §12/§14)
**前置**:Stage E(全部机制就位)
**目标**:§14 五项验收全绿;对标落盘;归档;出口 tag。

| 任务 | 交付物 |
|------|--------|
| 100 并发 24h stress(选型 §12.2/§14.1) | `m6_concurrent_stress`(pg-engine,m2c_100_conn.rs 形态):100 并发 INSERT+DELETE(+search 混合),24h 验收口径;CI 缩短版轮次参数化(**硬下界 ≥1,零值响亮失败**——M5 Stage D 教训);quiesce 后审计全绿 + 不对称率增量 <1% + recall@10(过滤后)≥ 95%;**重建介入口径立文**:24h stress 全程 vacuum 不介入(独占执行 = 写者暂停窗口,与并发验收分开),stress 结束 quiesce 后触发一次局部重建再复审审计 + recall;**30s 恢复门基数立文**:并发版数据集规模与 checkpoint 后增量窗口量级随 benchmarks 落盘(口径继承 M5 §13.2,只对该窗口成立) |
| 并发 × SIGKILL 叠加(选型 §12.4) | crash rounds 并发版:并发 stress 中 SIGKILL → 恢复 <30s 口径继承(checkpoint 后增量窗口)→ quiesce 审计 + recall 门;in-flight 残留 ≤ 崩溃窗口 × 并发度钉;**叠加 in-flight delete × checkpoint 窗口**(v1.4 P1 回归场景进轮次) |
| 对标(选型 §12.6/§14.5) | pgvector 最小 harness(docker compose;Qdrant 可选二期):recall/latency/throughput 对照落盘 `docs/phase2-m6-benchmarks.md`;口径对齐 M4 §6 传统;hwm 串行点的实测吞吐数据随文档落盘(§13 风险的实测回执) |
| 对抗审查 ×2 | 第一轮实现审查 → 修复 → 第二轮复核;**重点审查面**:临界区边界与 §4.2 逐行对应、锁序与 loom 断言面、IndexUndo 对偶完整性、过滤管线五步、审计 quiesce 前提、新序窗口表 |
| 覆盖率复核 | pg-am-hnsw tarpaulin ≥90% 口径承接(M4/M5 既有 job);pg-engine/pg-txn 侧新增代码由各 test matrix 承接;**判定以 CI(Linux)报告为准**(M5 Stage E 口径) |
| stage_spec 归档 + 全量回归 + 出口 tag | Stage 0–F(M6)各节三小节;workspace debug + release 全绿 + clippy/fmt/doc 三档 + bench smoke;`phase2-m6`(**经用户确认后**打,分支 + PR,tag 打在 main 的 merge commit 上) |

**验收命令**:
```bash
cargo test --workspace && cargo test --workspace --release
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
M6_STRESS_HOURS=24 cargo test -p pg-engine --test m6_concurrent_stress --release -- --nocapture   # 手动/nightly
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test recall_siftsmall --test m5_recall_after_recovery
```

---

## CI 注册清单(逐项:进哪个 workflow、什么条件下跑)

| # | 内容 | workflow/job | 运行条件 |
|---|------|-------------|---------|
| 1 | ColumnType::Vector/Datum 编码钉 | ci.yml 既有 pg-am-heap test matrix | 每 push |
| 2 | hnsw 新增测试(loom/临界区/自愈/undo/过滤/vacuum) | ci.yml 既有 pg-am-hnsw/pg-engine matrix | 每 push |
| 3 | hnsw_loom | 既有 loom job(参照 btree_loom 注册先例) | 每 push;preemption bound 纪律同 |
| 4 | m5 窗口矩阵/ghost_trap/crash rounds(换序重钉版) | 既有 matrix(M5 注册口径) | 每 push |
| 5 | m6_concurrent_stress(CI 缩短版,轮次 ≥1 硬下界) | pg-engine test matrix;24h 全量手动/nightly | 每 push(缩短版);手动(24h) |
| 6 | recall 双门 + 过滤后 recall 门 | 既有 recall-gate job(M4_REQUIRE_DATASET=1) | 每 push(随该 job 触发条件) |
| 7 | 覆盖率(tarpaulin,--fail-under 90 既有) | 既有 coverage job | 每 push;判定以 CI 报告为准 |
| 8 | 并发×SIGKILL 叠加轮次(含 in-flight delete × checkpoint P1 回归) | 手动/nightly(M5 crash rounds 1000 同型) | 手动触发;机器规格随 benchmarks 落盘 |
| 9 | pgvector 对标 harness | 手动(docker compose) | 手动触发 |
| 10 | grep 护栏(pg-am-hnsw 零 pg-txn 依赖、无 EBR/回收代码面) | stage 出口人工核对记录 | 每 stage 出口 |

---

## 总时间估算

| 阶段 | 预估 | 依赖 |
|------|------|------|
| 0 基建 | 2–3 天 | tech-selection v1.4 终审 |
| A 换序 | 2–3 天 | 无硬前置(与 0 交叠) |
| B 并发 | 4–6 天 | A(新序基线) |
| C 事务 | 4–5 天 | 0(Vector/Datum)+ B(并发句柄) |
| D 可见性 | 3–4 天 | C |
| E vacuum | 3–4 天 | D |
| F 收口 | 5–7 天 | E |
| **串行** | **23–32 天** | 0 与 A 可交叠(代码面无交集);B 与 C 之间是硬依赖(句柄形态) |

风险余量:B(并发协议)与 F(并发×崩溃验收)按上限估;hwm 串行点若实测
吞吐不达标,第一旋钮是候选批量/页池化(§13 登记,Stage F 后调优),不在
编码期提前做。

## 依赖关系图

```
A (5↔6 换序,单写者重钉;无硬前置,与 0 交叠)
 └── B (句柄 &self 化 + latch + 临界区 + loom)
      └── C (行内 NodeId + 旁路映射 + DML hook + IndexUndo;另依赖 0 的
           ColumnType::Vector/Datum)
           └── D (过滤管线 + tombstone 生效 + recall 口径)
                └── E (泄漏账 + 局部重建)
                     └── F (24h stress + 并发×SIGKILL + 对标 + tag)
0 (Vector/Datum + 可达性审计 + probe 落锤;独立支线,直供 C/F)
```
(0 与 A 可并行开工;B 硬依赖 A 的新序基线;C 硬依赖 B 的并发句柄形态与
0 的记录类/列类型;其余硬串行。)

---

## 回归测试传承

- **M4/M5 全部门禁每 stage 出口必绿**:recall_siftsmall 0.9990、
  m5_recall_after_recovery、m5_insert_crash 窗口矩阵(Stage A 起重编号版)、
  m5_hnsw_crash_rounds、m5_ghost_trap(Stage A 起语义反转版)、loom
  (btree 既有)、criterion smoke、tarpaulin ≥90%
- **单写者路径位级可复现性不破**(选型 §13):Stage A/B 出口各钉一次
  位级对拍;并发验收按"合法形态集"口径,两口径不得混
- 测试清单按 stage 累加:0(Vector/Datum + 可达性 + probe)→ A(换序重钉
  全套)→ B(loom + 自愈 + 临界区)→ C(undo 对偶 + flush 边界 + 零动作)
  → D(过滤管线 + recall 口径)→ E(重建 + 预算核验)→ F(stress + 叠加
  + 对标)

## 开放问题与冲突标注

- **行内 NodeId 载体编码**(选型 §7):隐藏系统列 vs tuple 伴随字段——
  Stage C 首任务冻结,**冻结前过用户终审**(M2 tuple 格式首处语义扩展);
  本计划不预判
- **probe 的 RefCell 处置**(选型 §4.1):Stage 0 侦察落锤(cfg(test) 剥离
  vs Mutex 化),Stage B 施工
- **hwm 串行点缓释**(选型 §13):批量预分配/页池化归 Stage F 实测后的
  调优项;选型层不承诺 insert 线性扩展
- **物理回收 + EBR + 条目带 TID+XID**(选型 §5/§7/§9):同归格式版本升级
  窗口,一并终审;本计划不含
- **两处 ROADMAP 字面偏离**(§6.1 偏离 #1 commit-合并 → 图内立即写;
  §5 偏离 #2 EBR 归窗口):随 tech-selection v1.4 待用户终审,本计划按
  偏离后口径排程
- **传承不阻塞项**(选型 §13):M4 D-1/D-6(sift/gist 1M)、M5 O1/O2、
  16k 页、min(rec_lsn)、M5 出口遗留(CI tarpaulin 终判 + `phase2-m5`
  tag)——维持原口径,不阻塞 M6 开工
- **技术选型文档内不一致登记**:撰写本计划时已核(v1.2 复核至 v1.4):记录类计数
  (8 冻结 + 0 新增,判别值段 121–127 + 预留 100 不变,v1.4)在 §1/§10 一致;新序与临界区在
  §4.2/§8.2 一致;审计 quiesce 口径在 §8.3/§12.5/§14.1 一致;泄漏预算
  ≤并发度/崩溃 在 §8.3/§14.2 一致;ROADMAP 行号(:286-291/:294)四轮
  已校正。

## review 修订记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-09-30 | 初版,基于 tech-selection **v1.2**(四轮审查闭环)。阶段骨架 0–F 七段(0 基建 / A 换序 / B 并发 / C 事务 / D 可见性 / E vacuum / F 收口);硬约束速查 12 条全部可回溯选型 § 节;CI 注册清单 13 项逐项落点（9 项进既有 workflow,2 项手动/nightly,1 项手动 docker compose,1 项 stage 出口人工核对）;总估 23–32 天,依赖关系图与交叠说明(0∥A 可交叠,B→C 硬依赖);技术选型文档一致性快查(记录类计数/新序/审计口径/泄漏预算/ROADMAP 行号)未发现内部不一致。关键排程决策:5↔6 换序独立成 Stage A(单写者形态先落,序变更与并发化分 stage 以便归因);IndexUndo 扩展与 un-tombstone(128)的消费归 Stage C;物理回收/EBR/条目带 TID 按选型口径整体归格式升级窗口,不入本计划 |
| v1.1 | 2026-09-30 | 第一轮对抗审查回流(主线 + agent-23,verdict PASS 附条件 → 条件项全量闭合):**P2-1(SQL 最小集漏排)**——选型 §11 的 Literal::Vector/VECTOR(n) 在计划零承接,Stage C 补任务行;**P2-2(vacuum×insert loom 面丢失)**——Stage B loom 行补脚手架(pg-am-hnsw loom feature,参照 pg-am-btree Cargo.toml:38 + ci.yml:83-85)与"vacuum 面归 Stage E"注,Stage E 补 loom 扩展行("独占执行"由模型证明)。**P3 尽修**:①Stage 0 补 Datum::Vector(tuple.rs:234)+ Value 链路(engine.rs:273)点名;②Stage A 补审计负例同批更新(选型 §8.2);③Stage B"零改动"改"语义零改动(仅去 mut)";④Stage F 补重建介入口径(stress 全程不介入、结束 quiesce 后重建复审)+ 30s 门基数立文;⑤CI #2 改实名"touched-page 分类穷举";⑥主线同轮先修三处(Stage A 验收命令 m5_insert_crash 归属 pg-am-hnsw 拆分、"191+"陈旧计数摘除、0∥A 依赖口径统一为无硬前置)。**nano×5**:probe 侦察改直接落锤 cfg(test) 剥离(index.rs:115/71-75 现状已足);"8 冻结 = 121–127 七 handler 类 + LogicalHnsw=100 预留位,8+1=9"口径点破;Stage F 估算 4–6 → 5–7 天(M5 千轮首跑即红先例),总估 24–33;tech-selection §2 引用漂移修复(stage_spec:584-585 → M5 选型 :584-585,选型同步 v1.3);基线引用 v1.2→v1.3 四处同步 |
| v1.2 | 2026-10-08 | 用户终审轮(外部复核 verdict FAIL,**P1 属实修复**——in-flight DELETE 崩溃恢复):未提交 tombstone 经 checkpoint WAL-before-data(checkpoint.rs:478-494,亲验)或他事务 commit 前缀 flush 持久化,崩溃后 redo 无条件重放 + ATT 判 abort → 活行配 tombstoned 节点 → vacuum 误当死节点重连入边;四轮选型审查与一轮计划审查均未覆盖该交错面(**教训登记**:攻击面清单补"未提交记录经 WAL-before-data/前缀 flush 持久化"类)。修复随 tech-selection v1.4:delete 图动作(tombstone + 映射删除)推迟到 commit 持久化后(post-commit 清理,幂等,Stage E 死行通道补刀),**HnswNodeUnTombstone(128) 作废、M6 零新增记录类**。本计划级联修订:Stage 0 摘除 128 三行(标题改 Vector/Datum + 可达性 + probe,3–4 → 2–3 天,总估回到 23–32);Stage C 的 delete hook(post-commit 口径)/IndexUndo(仅 insert 方向)/崩溃零动作钉(补 P1 回归钉)重排;Stage E 补死行驱动补刀行;Stage F 叠加轮次补 P1 场景;CI 清单 13 → 10(摘除 128 三行并重编号);硬约束 #1/#7/#8 改写。**nano 修复**:v1.1 的"基线同步四处"误中 v1.0 历史行("基于 v1.2"被改 v1.3),本轮回滚历史行并改正头部基线(:3)与 :24 的 v1.2 残留;"四轮闭环"措辞随 v1.4 升五轮 |
