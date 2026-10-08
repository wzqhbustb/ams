# Phase 2 M6 技术选型(HNSW 并发控制 + 事务/Tier 1 同步)

> **状态:v1.4(2026-10-08,五轮复核——in-flight DELETE 崩溃恢复 P1 修复——待用户终审)。** 本文档定义 Phase 2
> 第三个 milestone(M6 = ROADMAP.md Phase 2c,**HNSW 并发控制 + Tier 1 同步**,
> ROADMAP.md:280-299)落地前所有跨模块的技术选择。前置:M4(内存 HNSW 语义冻结)、
> M5(页驻 + WAL + 崩溃恢复)已收口。

承 M5:页驻图、八步 insert 序、WAL 记录集(121–127)、redo funnel、审计(§11.3)、
崩溃窗口语义(§8.2)均已冻结。M6 在其上加两个正交维度:**多线程并发改图**与
**事务 MVCC 可见性**。每个选择给"选项 → 选择 → 理由 → 代价"。章节编号(§1…§14)
供代码注释长期引用。

本文档中的代码引用(文件:行号)与文档引用均为撰写时核实的事实;若后续实现与
引用不符,以代码为准并修订本文档。M5 文档的 § 号引用默认指
docs/phase2-m5-tech-selection.md,本章内部引用指本文档。

---

## §1 范围与非目标

**范围(ROADMAP Phase 2c 交付物逐项承接)**:

1. **HNSW 并发协议**:多线程并发 insert/delete/search 下图结构不被改坏
   (节点级细粒度 latch,ROADMAP :286-287;EBR 随物理回收归格式升级
   窗口——偏离 #2,§5)。
2. **Tier 1 同步(事务集成)**:向量 DML 进入事务,commit/abort 语义正确
   (ROADMAP :288);崩溃恢复对 in-flight 事务的图修改有确定处置
   (裁决:图零动作,§6.2)。
3. **可见性适配**:搜索多取候选 + 回表 MVCC 过滤(ROADMAP :289)。
4. **删除语义**:tombstone 生效(M5 只冻结了格式与重放)、并发 undo
   (insert abort → tombstone;delete abort 图零动作——un-tombstone 作废
   v1.4)+ 邻居连通性修复(ROADMAP :290)、幽灵/孤儿记账。
5. **Vacuum 扩展**:tombstone 比例监控 + 局部重建(ROADMAP :291;
   物理回收归窗口,§9)。
6. **验证与对标**:100 并发 24h、recall/不对称率口径、pgvector/Qdrant 对标。

**非目标**:

- **ANN 的 SQL 运算符**(`ORDER BY vec <=> q`、索引条件下推)归 Phase 4b
  (ROADMAP "一条 SQL 完成混合召回");M6 的验收在 Engine API 层。
- **图节点布局变更**(条目携带 heap ctid 等)——M5 冻结布局不动,关联走
  行内 NodeId + 旁路(§7)。
- Cosine/IP 的 recall 质量验证承接口径(M4 已登记归 M6,§13 验收列出)。
- 16k 页形态(M5 传承,8KB 编译期常量口径不变)。
- Phase 5b 的遗忘/分层策略本体——M6 只交付它需要的 tombstone/vacuum 机制。

**M5 冻结边界重申**:M5 的八个 WAL 记录类、页格式、redo handler、审计口径
不因 M6 而改;M6 **零新增记录类**(§10 决策点,v1.4:un-tombstone
随 delete 图动作推迟到 commit 后而作废,判别值 128 保持空闲);
变更既有记录语义需格式版本升级(meta page 有
snapshot_format_version 位)并单独终审。

---

## §2 Crate 归属与依赖方向

| 内容 | 归属 | 理由 |
|------|------|------|
| 并发协议本体(节点 latch 表、并发 insert/delete) | **pg-am-hnsw** | 图结构知识不出 crate;latch 化是 §4 的核心 |
| 事务 hook(commit/abort 时索引动作、loser undo 对接) | **pg-engine** | "index-undo 的 HNSW 对接"在 M5 选型 :584-585 登记为 M6 前置;IndexUndo(engine.rs:188-209)是挂载点 |
| 可见性过滤(回表 MVCC 判定) | **pg-engine** | **pg-am-hnsw 保持 MVCC-free**(不新增 → pg-txn 依赖边):过滤需要 Snapshot/ClogAccessor,而 AM trait 的 context 本来就携带它们(pg-am-heap/src/access_method.rs:50-60 ScanContext;Update/DeleteContext :63-103 同型),引擎层是天然落点 |
| 行内 NodeId 权威载体 + NodeId→ctid 旁路映射 | **pg-engine**(heap 行伴随信息 + 复用 pg-am-btree) | 见 §7 裁决 |
| VECTOR(n) 列类型 | **pg-am-heap**(pg-catalog re-export) | 列类型枚举在 pg-am-heap/src/tuple.rs:205,pg-catalog 只是 use(builtin_types.rs:8);内联存储在 heap |
| loom 并发模型 | **pg-am-hnsw**(参照 btree_loom 形态) | sync-alias 规则已在 pg-storage 立文 |

**依赖方向不变量**:pg-am-hnsw → pg-storage(既有);pg-engine → 全部(既有);
**不新增 pg-am-hnsw → pg-txn**(MVCC 知识不入图 crate,与 M4/M5 的零依赖
扩张纪律一致)。

---

## §3 并发协议总路线(M6 第一决策)

**问题**:多线程同时 insert/delete/search,HNSW 图不被改坏,读者绝不看到
半成品节点。

**选项**:

- **(a) 单写者串行**(ROADMAP :775 明文退路):全局写锁,读者并发。
  实现最简,M5 的 `&mut self` insert 形态(index.rs:270)直接合规。
- **(b) 节点级细粒度 latch**(ROADMAP :286-287 主案):写者按节点
  latch,读者 latch-free(帧级 RwLock 兜底);删除只标记 tombstone,
  物理回收与 EBR 归格式升级窗口(§5/§9)。
- **(c) 全无锁(Vamana/DiskANN 式)**:CAS 邻表 + 无锁遍历。
  ROADMAP :765 定位为"启动前研究任务"。

**选择:(b),但读路径完全 latch-free(不写 latch、不读 latch 节点表)**——
读者只持 buffer pool 帧读锁(pin),看到的邻表是 SetNeighbors 原位覆写的
记录级原子版本;HNSW 的近似语义容忍"读到稍旧的邻表"(遍历质量由 ef 冗余
兜底)。写者之间用节点级 latch 互斥。

**理由**:

- (a) 把 §1 场景 1/2/3(多 Agent 并行写、实时摄取、事务性业务写)的写
  吞吐锁死,尾延迟随队列堆积;ROADMAP 自己也只把它列为退路。
- (c) 的正确性论证成本远超本项目阶段——Vamana 的无锁邻表 CAS 需要
  原子变长序列,M5 的页驻定长分档布局不天然支持;且 WAL 顺序语义
  (八步序)与无锁写路径的交错证明是研究级工作量。
- (b) 的关键使能条件 M5 已经备好:**条目零搬移 + 定长分档**(M5 §7.3
  明文"为 M6 留的位":读 slot 内容 = 读 latch,写 latch 只在原位更新
  窗口持有)、**INITIALIZING 状态字节**(半成品节点对审计/写者可识别;
  读者不检查状态位——§4.2 澄清)、**tombstone 位**(M4 已预留于记录格式)。
- 读路径 latch-free 的代价近似为零:M5 搜索本就只有帧读锁
  (index.rs:673 `&self`)。

**代价**:写者并发协议的证明与测试成本(loom 模型 + stress);insert 的
八步序要并发化重写(§4.2);latch 死锁避免规则要立文并 loom 验证(§4.3)。

---

## §4 节点级 latch 设计

### 4.1 latch 表形态

**选择:进程级 latch 表,编译期固定 4096 片 `Vec<RwLock<()>>`,按
NodeId 取模分片**。互斥正确性依赖"所有写者对同一 NodeId 算出同一把
latch"——**分片数必须是常量,绝不能是 hwm 的函数**(随 hwm 翻倍的分片
数会让两个写者在新旧分片数下算出不同分片,互斥即破)。4096 片一次性
预分配(KB 级,可忽略),生命周期 = 索引句柄,零 resize、零重映射;
latch 不随节点删除销毁——latch 是无限生命周期的同步对象;节点内容的
物理回收归格式升级窗口(§5/§9)。

- 选项对比:per-node latch 内嵌页内(侵入 M5 冻结布局,否决);全局
  `DashMap<NodeId, Arc<RwLock>>`(新增依赖,否决——依赖冻结纪律,且
  latch 生命周期管理复杂);**固定分片 Vec**(零依赖、零分配热路径、
  冲突率 = 1/4096,可接受)。
- **句柄形态变更**:`HnswIndex` 从 `&mut self` 单写者(index.rs:90-94
  明言 single-threaded premise + probe RefCell !Sync)改为共享句柄
  `&self` insert,内部可变状态(hwm、rng、dir_pages、current_node_page)
  收进 latch 保护的 inner;probe 的 RefCell 改 cfg(test) 编译期剥离
  或 Mutex 化(probe 只在测试构建存在——现状确认后定,登记开工期第一项)。
- rng 流位语义:M5 冻结"level 抽签流位 = hwm"(insert 时按 hwm 位置抽)。
  并发下 hwm 分配经 latch 串行化(§4.2 hwm 临界区),抽层仍在该临界区内
  ——**rng 语义不变**,位级可复现性保持(单写者路径)。
- **读者目录快照自愈**:句柄缓存的 dir_pages 快照可落后于 DirLink
  增长;读者解析 NodeId 命中"maps past the directory chain"时**不得当
  腐坏上报**——沿目录 next 链刷新快照重试一次,仍越界才 Corrupted
  (M5 目录解析错误文案的同步调整归 coding-plan)。

### 4.2 insert 序的并发化(M6 基线 = §8.2 对调后的新序)

M5 §8.1 八步序的记录序列与崩溃窗口语义保留,但步骤 5↔6 对调(§8.2
裁决):**先写自身各层邻表,再改他人邻表**。本节步骤编号按新序,
并发化只加 latch 纪律:

1. **hwm 临界区(步骤 0–4:校验/抽层/分配/初始化/目录追加)**:拿 hwm
   latch → 分配 NodeId(hwm 递增)→ rng 抽层(流位 = hwm,语义不变)→
   选/备节点页(current_node_page 是共享态;满则 PageAlloc + FPI 新页)
   → NodeInit → DirAppend(目录尾页共享;尾页满则 DirLink 链新页,同
   临界区内完成)→ 放锁。**整段必须同临界区**:目录
   position-is-identity(条目序号 = NodeId),DirAppend 落点与 hwm
   分配若不同序,条目即张冠李戴;NodeInit 落点(共享尾页的 slot)同理。
   此段只碰共享元数据页,不碰他人节点条目。
2. **步骤 5(自身各层邻表)**:自己的节点 latch。此时他者尚无法经图
   遍历发现本节点(尚无入边),但目录已发布(审计/vacuum 可见)。
3. **步骤 6(改他人邻表,并发冲突点)**:对待连边的每个邻居节点
   **按 NodeId 升序逐个** latch → pin_mut → SetNeighbors → 释放;
   同一时刻最多持一把节点 latch(改完即放,不批量持锁)。
4. **步骤 7(MetaUpdate,meta 临界区)**:复用 hwm latch;临界区内
   **重读 meta 再决策**——空图首插(M5 的 was_empty 分支)必须在此
   重查:两个写者并发首插时,后至者重查到入口已存在,回补正常连边
   流程(否则产生不可达 LIVE 节点);max_level/entry_point **只升不
   降**(new = max(current, candidate)),防并发降级竞争。
5. **步骤 8(PublishLive)**:状态字节翻转,与读者的关系见 §8.1。
6. **成功边界**:flush 从"每 insert flush_to"移到 commit
   (§6.2/§10——事务形态的持久性承诺在 commit 边界)。

**并发下的可达性不变量**(新序的结构红利):节点的首个入边只能由其
自己的步骤 6 产生,而步骤 6 排在其步骤 5 之后——**可达 ⟹ 出边完整**。
M5 的幽灵形态(有入边、空出边,stage_spec:1516 登记)不再新产生。

**M5 搜索语义澄清**(v1.0 误述订正):M5 搜索**从不检查状态位**,
INITIALIZING 节点可被召回为合法答案(M5 §8.1③);M6 保持同口径——
state 位不作搜索谓词,结果过滤全在回表层(§7)。因此并发化**不需要**
引入"LIVE-only 邻居选择"之类的新过滤:上述不变量保证 INITIALIZING
节点出边完整、向量真实,可安全承担路由;recall 与位级对拍零新增
过滤冲击。

### 4.3 latch 层级与死锁避免

**锁序立文**(由 loom 模型验证,§12):

```
表级 LockManager(既有,DDL/语句级)
  → 节点 latch(§4.1,升序 NodeId,同时最多一把)
    → buffer pool 帧 latch(pin/pin_mut)
      → WAL writer 内部锁
```

- 节点 latch 不进 LockManager(LockManager 只有表级四档,
  lock_manager.rs:79-90;节点 latch 是物理同步原语不是事务锁)。
- **同时最多持一把节点 latch**是死锁避免的结构性保证:不构成等待环。
  (连边时对多个邻居是"逐个 latch-改-放",不是"全 latch 再改";
  代价是连边过程中图可被观察到部分连接——合法,M5 §8.2 已论证
  半连接是合法形态。)
- **beam-search 遍历期不持任何节点 latch**:只持当前帧读 pin、逐跳
  随用随放;短命帧 pin 不带入节点 latch 临界区。
- heap 行锁(t_xmax 印记协议,heap_am.rs:1251)只出现在回表层,
  与图 latch 无交集(回表只读不取行锁,§7)。
- 死锁检测器(deadlock.rs:191)继续只管表级/行级事务锁;节点 latch
  层由"升序 + 单持"规则 + loom 证明,不进检测器。

---

## §5 Epoch-based reclamation(EBR)——归格式升级窗口(偏离 #2)

**裁决:EBR 本体随物理回收一同归格式版本升级窗口,M6 不落地。**

- EBR 的服务目标是物理回收(槽位复用、页面进 freelist)的读者安全;
  §9 已把物理回收收缩出 M6(页内无空闲位图 = 页格式变更,撞 §1 冻结
  边界;复用槽的重放撞 NodeInit 占用校验——apply.rs 对命中已占用
  LIVE 槽响亮拒绝,其"replay must be skipped by the pd_lsn guard"
  语义不覆盖复用)。**无回收即无 reclamation 危害**,EBR 提前落地
  是死重。
- M6 读者安全由两条既有机制承载:① buffer pool 帧 pin(读 pin 期间
  帧不可驱逐/复用);② 条目零搬移 + SetNeighbors 原位覆写(读者只见
  记录级原子的新旧版本,§3)。
- 对 ROADMAP :286 字面(EBR 列为 Phase 2c 交付物)的偏离在此立文,
  与 §6.1 的偏离 #1 同待用户终审。
- **窗口内落地时的前瞻笔记**:读者集合必须包含 inserter 的
  beam-search 遍历(不只纯搜索路径);retire 清单单消费者 =
  vacuum;epoch 卡住(长查询推迟回收)做可观测指标,不设硬门;
  loom 模型同步扩。

---

## §6 事务集成(Tier 1 同步)

**问题**:向量 DML 进入事务;commit 后可见、abort 后不可见、崩溃后
恢复一致。

### 6.1 总形态(与 ROADMAP 字面的一处显著偏离——偏离 #1)

ROADMAP :288 字面:"事务内累积 delta,commit 时批量合并到 HNSW 图"。

**选择:图内立即写 + heap MVCC 承载可见性 + abort/崩溃走 tombstone undo**
——即 ROADMAP :290 的"并发 undo"案承载事务语义,而非 :288 的
commit-合并案。

**v1.4 修正**:delete 的图动作(tombstone + 映射删除)不在 statement 期,
推迟到 commit 持久化之后(§6.2 P1 修复);"tombstone undo"自此仅指
insert 方向。

**理由**:

- commit-合并案要求 delta 本身 WAL 持久化(commit 先于合并,崩溃后
  需重放"已 commit 未合并"的 delta),等于在图之外再建一套二级写
  结构与恢复协议;且合并失败时 commit 已记录,处置语义模糊。
- 图内立即写完整复用 M5 的八步序、WAL 记录集、崩溃窗口分析(全部
  已冻结并千轮验收);未 commit 节点在图内但**搜索不可见**(回表
  过滤,§7),abort 时 tombstone(格式已冻结)+ 异步连通性修复(§9)。
- M5 已把"pg-engine index-undo 的 HNSW 对接"登记为 M6 前置
  (coding-plan:346-348)——本条即其选型落地。
- PG 先例:B+Tree 索引插入同样立即物理生效,可见性由 heap 行
  xmin/xmax 承载,abort 不清索引条目——本案与 PG 索引哲学一致。

**代价**:未提交/已 abort 的节点会短暂成为图的路由节点(其邻表完整、
向量真实——M5 §8.1③ 已论证幽灵是合法答案);tombstone 比例需要监控
(§9);recall 影响实测(§12)。

### 6.2 事务生命周期对接

- **insert(事务内)**:heap 行插入(xmin = xid,既有)+ 图 insert
  (八步序,立即生效)+ NodeId 写回 heap 行(§7 权威载体)+ 旁路
  映射条目(NodeId→ctid,同事务 btree insert)。失败即 abort 语义
  由既有 undo 承接。**不再逐语句 flush_to**——成功边界移到
  commit(见下)。
- **commit**:既有四步硬序(manager.rs:427 起)不动。
  **flush 边界立文**:commit 硬序的 flush 覆盖本事务全部图记录
  (WAL 前缀性质——任一 LSN 的 flush 顺带持久化此前追加的所有
  记录,含他人记录),故事务内 insert 的图记录零 flush 也安全;
  语义变化立文:语句 Ok 不再承诺单条记录已持久(持久性 = commit
  边界),M5 §8.1 边界①仅在非事务 utility 路径保留。**delete 的
  图动作(tombstone + 映射删除)是 commit 持久化之后的清理动作**
  (post-commit hook,幂等可重入;失败/崩溃跳过由 §9 vacuum 的死行
  驱动通道补刀)——关键性质精确化:**commit 记录单独决定可见性;
  图删除动作可滞后、可重做、可补刀,不存在"已 commit 未合并"的
  语义歧义,只有"已 commit 待清理"的良性残留**。
- **update(事务内)**:非向量列 UPDATE = **图零动作**(新行版本
  携带同一 NodeId;旁路映射的 ctid 同事务更新);向量列变更 =
  delete + insert 组合(旧节点 tombstone 随 delete 口径走
  post-commit;新节点八步序即时,新行携带新 NodeId;映射增随
  insert、删随 post-commit)。映射指向最新版本;更新窗口内并发
  快照看到旧版本而映射已移,候选丢失由 §7 的行内复核 + ef 放大
  兜底,登记为 recall 观察项。
- **abort(在线)**:由 IndexUndo(engine.rs:188-209,既有
  Inserted/Deleted 对偶)扩展驱动——**insert 的 abort** =
  tombstone 图节点(HnswNodeTombstone,M5 格式;定位经 heap 行内
  NodeId,无需反查);**delete 的 abort = 图零动作**(heap xmax
  由既有 heap undo 撤销;映射自始未被 delete 触碰,无逆操作需求
  ——v1.1–v1.3 的 un-tombstone 设计作废,见下条 P1 论证)。heap
  行侧 abort 标记既有;邻居连通性修复归 §9 的后台/延迟路径。
- **崩溃时 in-flight——裁决:恢复对图零动作**。recovery 的 undo
  段不对图发任何记录。**为什么 statement 期 tombstone 不成立**
  (v1.4 P1,coding-plan 审查发现、机制链亲验):未提交的 tombstone
  会经 checkpoint 的 WAL-before-data(checkpoint.rs:478-494 脏页
  强刷,flush() 内部强制 WAL 先行)或他事务 commit 的前缀 flush
  持久化;崩溃后 redo 无条件重放(utility 形态),ATT 判 delete
  事务 abort、heap 行回活——图侧若无逆动作则**活行配 tombstoned
  节点**,vacuum 将其当死节点重连入边 = 活数据断连。delete 图动作
  推迟到 commit 后,该形态在结构上不可构造:in-flight delete 没有
  图记录,崩溃残留只有 insert 方向的 LIVE/INITIALIZING 节点,不可
  见性由 heap 行 MVCC 承载(§7 回表过滤兜底),物理残留归 §9
  vacuum 记账。推论立文:**xid 不进图 WAL**(121–127 维持 utility
  形态,require_utility_txn 门不动);在线 abort 的定位不依赖 WAL
  xid(引擎 undo 持有行 → NodeId)。stage_spec:1376 并案:
  INITIALIZING 不写 Tombstone 的原裁决被本裁决吸收(零动作是其
  强化形式);tombstoned 节点不进结果集的机制 = 映射缺席(§8.1),
  state 位不作搜索谓词(§4.2)。

### 6.3 并发 undo 的连通性

abort tombstone 的节点仍滞留图内(有入边)。邻居连通性修复 =
vacuum 的局部重建(§9),**不在 abort 路径同步修边**(同步修边 =
多页写协议,复杂度爆炸且 M5 §8.4 的"不立 CLR"结论同型适用:tombstone
节点有完整邻表,可继续承担路由,半修复形态不是灾难)。

---

## §7 可见性过滤与回表(NodeId ↔ heap 行的关联裁决)

**问题**:图搜索返回 NodeId,事务可见性在 heap 行(xmin/xmax)。
NodeId 如何映射到 heap 行?

**选项**:

- **(a) 图节点携带 heap ctid**:M5 冻结布局无此字段(M5 §7.3 预留位
  是 latch/墓碑语义位,非数据字段)——需格式版本升级 + 全量编码钉
  重写。否决(M5 冻结边界,§1)。
- **(b) heap 行携带 NodeId**:行是 NodeId 的权威载体——delete/
  vacuum 由行直达 NodeId,无需反查。
- **(c) 旁路映射 B+Tree(NodeId → ctid)**:复用 M1 的 pg-am-btree
  (完整 WAL + M2c 并发已验),服务搜索正查。

**选择:(b)+(c) 组合——heap 行内 NodeId 为权威关联 + 旁路 B+Tree
(NodeId→ctid)服务搜索正查,回表以行内 NodeId 复核**。

- 单向旁路(仅 (c))被复核证伪:delete/vacuum 需 ctid→NodeId
  反查;崩溃孤儿映射 + heap 槽位复用会把 NodeId 张冠李戴到他人
  行上。行内 NodeId 使两个方向都有权威来源:delete/vacuum 由行
  直达;搜索正查的映射结果经行内复核——**错配即弃**(损失一个
  候选,永不产生错结果)。
- 行内 NodeId 的物理载体(隐藏系统列 / tuple 伴随字段)归
  coding-plan 首任务冻结;本裁决只冻结语义:"heap 行是 NodeId 的
  权威载体"。M5 图布局不动(§1 冻结边界)。
- **映射生命周期立文**:insert 的映射条目 = 旁路 btree 的事务内
  条目(M2c 的 IndexUndo 对偶既有:insert abort → 映射随 undo
  消失);**delete 的映射删除推迟到 commit 持久化之后**(与
  tombstone 同批 post-commit 清理——statement 期删除会让未提交
  的 delete 对他事务可见地抹掉映射,abort 后还需回滚映射,两个
  形态都不合法,v1.4)。崩溃孤儿映射由回表复核滤除;物理清理
  计入 §9 泄漏账,随 btree 既有 vacuum 通道回收。
- **优化登记**:若实测回表延迟不可接受(ROADMAP :763 风险 1),
  M6 末评估索引条目带 TID+XID(即格式升级 (a))——届时以实测数据
  回推,与 §9 的物理回收同窗评估、一并终审,不在初版做。

**过滤流程**(引擎层,pg-am-hnsw 不参与):

1. `hnsw_search(query, k, ef_search)` 取候选,候选数 = max(2k, ...)
   (ROADMAP 固定 2x 起步;自适应放大登记为调优项);
2. 批量查旁路映射 NodeId → ctid;
3. 回 heap 表按 Snapshot 过滤(pg_txn::visibility::is_visible,
   visibility.rs:160 既有;只读不取行锁)+ **行内 NodeId 复核**
   (错配即弃);
4. 不足 k 条时扩大 ef 重搜——**硬上限立文**:ef 倍增 ≤3 轮封顶,
   终止即返回可见子集(允许少于 k,不凑满;无可见行时的病态放大
   由测试钉住);
5. 返回 top-k heap 行。

**候选放大与 recall 的口径**:recall 验收(§14)以**过滤后**的 top-k
计量;**ground truth = 可见行集合上的暴力 top-k**(非全库暴力)。
放大不足导致 recall 跌破基线 95% 即验收红,这迫使 2x 起步值
在验收中自证;行内复核错配造成的候选损失计入放大调优。

---

## §8 删除语义:tombstone 生效 + 幽灵/孤儿记账

### 8.1 tombstone 生效

- **写侧**:delete(事务内)statement 期唯一动作 = heap 行 xmax
  标记(既有);commit 持久化后 = 图节点 tombstone
  (HnswNodeTombstone,M5 格式冻结,语义本里程碑生效)+ 旁路映射
  删除(同批 post-commit 清理,§6.2/§7)。并发语义:tombstone
  与 insert 同 latch 纪律(节点 latch + 原位写)。
- **读侧**:tombstoned 节点**可继续承担路由**(邻表完整)但**不进入
  结果集**(映射已删 → 正查无命中;兜底 = 回表过滤)。初始实现不做
  图内跳过优化(§4.2 澄清:state 位不作搜索谓词),图内跳过登记为
  vacuum 前的性能观察项。
- **redo 侧**:M5 已重放 tombstone(只重放不生效的口径在 M6 解除);
  审计的 `tombstoned ∧ ¬LIVE` 响亮规则保持,M6 落地时按
  stage_spec:1436/:1448 的登记把统计口径转正。

### 8.2 幽灵陷阱消除(步骤 5↔6 对调)

M5 1000 轮验收实证(round 521):先改他人邻表(旧步骤 5)再写自身邻表
(旧步骤 6),窗口内崩溃留下"有入边、空出边"幽灵,查询下降走入即被困
(合法残态,非腐坏)。M5 冻结不动并登记 M6 评估(stage_spec:1516)。

**M6 裁决:对调落地**——先写自身各层邻表,再改他人邻表;崩溃残留
变为"有出边、无入边"= 不可达孤儿(无害,审计已知形态)。新序即
§4.2 的 M6 基线序(本节是裁决出处)。配套联动立文:recall 影响与
位级对拍(孪生钉)必须随改动重跑,对拍对象同步换序,两形态继续
逐位等价;m5_ghost_trap.rs 的 ProbeMark 注入点随新序重排;崩溃窗口
矩阵(§8.2 十行)按新序重编号并标注与 M5 的对应关系;审计负例
同批更新。

### 8.3 幽灵/孤儿记账

- INITIALIZING 孤儿(崩溃残留):M6 内只标记记账(物理回收归窗口,
  §5/§9);audit 的 orphan/initializing 计数是发现手段。
- 幽灵(有入边 INITIALIZING):对调后不再新产生(§4.2 可达性
  不变量);存量幽灵的清理归 vacuum 同一通道。
- 泄漏预算:M5 单写者实测 ≤1 节点/崩溃事件;**M6 上界 = 并发度/崩溃
  事件**(in-flight insert 数 ≤ 并发写者数),vacuum 阈值按 §12 的
  tombstone + residue + 孤儿映射条目合计比例监控。
- **审计能力扩展登记**:§14.1 的"无悬挂节点(全部 LIVE 节点自入口点
  可达)"判项依赖全图可达性扫描——M5 审计无此遍历,新增能力归
  coding-plan 先导任务。

---

## §9 Vacuum 扩展

- **监控**:审计既有计数(tombstoned/initializing/orphan)+ 旁路映射
  孤儿条目计入泄漏账(§7)+ 比例阈值(默认 10%,可配)触发局部重建;
  1000 轮验收形态已证明残态有界。
- **局部重建**:tombstone 节点的邻居重连(把入边重定向到该节点的
  次近邻,近似语义由 recall 门兜底)——这是 ROADMAP :290"后台修复
  邻居连通性"的落点;**单写者/vacuum 线程独占执行**(不与并发
  insert 并发),并发安全由节点 latch 保证。**重定向引入的单侧边
  计入不对称率增量预算(§12.3)**,开工期实测评估,不得静默超支。
- **槽位与页回收——收缩出 M6**:物理回收(页内空闲位图、槽复用、
  页进 freelist)不交付。页内无位图 = 页格式变更(撞 §1 冻结边界);
  复用槽的重放撞 NodeInit 占用校验(apply.rs:命中已占用 LIVE 槽 =
  响亮拒绝,"replay must be skipped by the pd_lsn guard"语义不覆盖
  复用)。物理回收归格式版本升级窗口,与 §7 的条目带 TID 优化同窗
  评估、一并终审。**M6 只交付:标记、重定向、泄漏记账。**
- **死行驱动补刀(post-commit 清理的完成通道,v1.4)**:heap
  vacuum 的 committed-dead 扫描(M3 Vacuumable collect_index_keys
  先例同型)读出死行的行内 NodeId → 补 tombstone + 删映射——
  commit 后崩溃/失败跳过的 post-commit 清理在此收口;**该通道是
  正确性部件(泄漏账口径),不是纯优化**。
- **与 heap vacuum 的关系**:各管各的;图 vacuum 不阻塞 heap vacuum,
  反之亦然;旁路映射孤儿条目随 btree 既有 vacuum 通道回收(§7)。

---

## §10 并发下的 WAL 与恢复

- **记录集——决策点落实(v1.4 反转)**:M6 **零新增记录类**。
  v1.1–v1.3 的 HnswNodeUnTombstone(判别值 128)决策作废——
  delete 图动作推迟到 commit 后(§6.2 P1 修复),delete 的 abort
  无逆操作需求;128 从未注册,判别值段保持 121–127 + 预留 100
  不变。**xid 不进图 WAL**(§6.2 零动作裁决);M5 §4.1 预留的
  LogicalHnsw=100 位保留原义。并发 insert 的记录序列不变(新八步
  序),只是多写者交错——**redo 单线程按 LSN 序重放 + 页 pd_lsn
  门控,交错天然安全**(M5 的幂等论证不依赖单写者前提,依赖
  "每页 pd_lsn 单调"——并发下由帧 latch 保持,§4.3 锁序)。
- **undo 段**:仅在线路径(§6.2),且只覆盖 insert 方向(abort
  写 tombstone 走正常 WAL-first:pin_mut → append → apply);
  delete 的 abort 图零动作。**崩溃恢复对图零动作**(§6.2 裁决)。
- **commit/成功边界**:commit 记录单独决定可见性(§6.2),
  group commit 既有;**flush 边界 = commit 硬序**(§6.2 flush 段,
  WAL 前缀性质),事务内图记录零 flush 安全;delete 的图动作为
  post-commit 清理(幂等,vacuum 补刀)。
- **FPI 门控**:多写者并发 pin_mut 同页(邻接更新撞页)由帧 latch
  互斥,FPI 协议(needs_fpi + checkpoint_lsn)不感知写者数量,无改动。

---

## §11 类型与 SQL 面

- **ColumnType::Vector(dim)** 进 **pg-am-heap**(tuple.rs:205 既有
  枚举扩展;pg-catalog 仅 re-export,builtin_types.rs:8)——定宽
  分支同步:fixed_width(tuple.rs:222)返回 Some(dim*4),heap
  内联存储,**dim ≤ 2000 上限响亮报错**(ROADMAP-changes TOAST
  决策:M5 单页布局可行域对齐;超维归 M5 O1 的既定窗口,不在 M6 开)。
- **Literal::Vector(Vec<f32>)** 进 SQL 层最小集:`INSERT INTO t VALUES
  (1, [0.1, 0.2])` 可解析;列类型声明 `VECTOR(n)`。
- **ANN 运算符(`<=>`)与索引条件下推归 Phase 4b**;M6 验收走
  Engine API(hnsw_search + 过滤管线)。
- **索引创建 SQL**(`CREATE INDEX ... USING hnsw`)归 Phase 4b 同批;
  M6 保持 Engine API(create_hnsw_index,engine.rs:1348)。
- **DML hook**:引擎 insert/update/delete 行路径(engine.rs:1753/
  1842/1933)在表上有 hnsw 索引时触发 §6/§7 的图动作与旁路映射维护;
  hook 的失败语义 = 语句失败即 abort(既有)。

---

## §12 验证方法论(M6 特化)

1. **loom 并发模型**(ROADMAP :745 对策):节点 latch 锁序、连边
   交错、PublishLive 与读者的交错;**hwm 临界区全段交错**(分配 →
   NodeInit → DirAppend 含 DirLink 分支)、**meta 临界区首插竞争与
   只升不降**、**vacuum 重定向 × 并发 insert**;tier 参照
   btree_loom(2 变体,preemption bound 纪律同 ci.yml:75-83)。
2. **stress 验收**:100 并发 INSERT+DELETE 24h(ROADMAP :294);
   harness 复用 m2c_100_conn.rs 形态(pg-engine benches 既有先例)。
   24h 是验收口径;CI 跑缩短版(轮次参数化,m5_hnsw_crash_rounds
   的 M5_CRASH_ROUNDS 先例)——**轮次硬下界 ≥1,零值/非法值响亮
   失败**(M5 Stage D slice 3 的零轮静默通过教训);CI 默认轮次
   立文于 coding-plan。
3. **recall/不对称率口径**:recall@10 过滤后 ≥ 单线程基线 95%
   (ROADMAP :295-296;**ground truth = 可见行集合上的暴力 top-k**,
   §7);不对称率**增量** <1%(基线 = M4 实测聚合
   15.35%,逐 cell 8.9–17.1%,stage_spec:1076 口径——增量约束
   并发/修复引入的部分,含 vacuum 重定向单侧边(§9),不约束 HNSW
   固有单侧删边)。
4. **并发崩溃叠加**:并发 stress × SIGKILL(崩溃轮次的并发版)——
   恢复后审计 + recall 门同 M5 口径。
5. **loom 之外的确定性**:并发幽灵形态(§4.2 可达性不变量)由审计
   负例钉;**并发审计一律 quiesce 后运行**——进行中审计会把合法
   中间态(半连接、INITIALIZING)误判为腐坏,§14 的审计门同此口径。
6. **对标**:pgvector 最小 harness(docker compose,Qdrant 可选二期);
   口径对齐文档沿用 M4 §6 传统(phase2-m4-benchmarks.md:113-117)。

---

## §13 风险与开放问题

- **回表延迟放大**(ROADMAP :763 风险 1):多跳每步回表不可行——
  本方案只在**结果候选**回表(2k 规模),不在遍历中回表;行内复核
  错配的候选损失计入放大调优;若实测不达标,M6 末评估索引条目带
  TID+XID(格式升级,§7 登记)。
- **节点 latch 死锁**:结构性避免(升序 + 单持,§4.3)+ loom 证明;
  残余风险 = 规则被未来代码破坏——loom 模型即回归门。
- **hwm 临界区 = insert 全局串行点**(§4.2 步骤 1):分配 NodeId →
  抽层 → 备页 → NodeInit → DirAppend(含 DirLink)整段在单一
  hwm latch 内,所有 insert 在目录尾页/元数据处全局串行——这是
  position-is-identity 的正确性代价,不是可优化的疏忽。100 并发下
  它是 insert 吞吐天花板;§14 无吞吐量门(24h stress 只隐性覆盖,
  recall/不对称率门不计量吞吐),实测不达标时的缓释方向(hwm 批量
  预分配、节点页池化)归 coding-plan 调优项——选型层不承诺 insert
  线性扩展。
- **tombstone/residue/孤儿映射滞留膨胀**:物理回收归窗口(§9)的
  延期风险 = 膨胀上界由磁盘预算兜底;监控阈值 + 局部重建;泄漏账
  三项合计可观测(§8.3/§9)。
- **步骤 5↔6 对调的 recall 影响**:理论近似中性,实测钉(§8.2)。
- **并发下 rng 流位**:hwm 临界区串行化抽签(§4.2),但并发完成顺序
  不确定 ⇒ 图形态不再与单线程流逐位一致——**位级对拍口径降级为
  "并发结果 ∈ 合法形态集"(审计 + recall 门),不再钉单一参照流**。
  M5 的同平台位级复现承诺只在单写者路径保持(并发验收不继承)。
- **审计口径**:quiesce 后运行(§12.5);入口点可达性全图扫描为
  新增能力(§8.3),未落地前 §14.1 的悬挂节点判项不可用——
  coding-plan 排它为先导任务。
- **post-commit 清理滞后/跳过**(v1.4):commit 与清理间的崩溃 =
  死行配 LIVE 节点的良性残留(结果正确性由回表过滤承载),vacuum
  死行通道补刀(§9);残留上界与补刀时延入泄漏账监控。
- **传承不阻塞项**:M4 D-1/D-6(sift/gist 1M,数据未就位);M5 O1
  (1791,2000] 维区间、O2 f16/bf16 缓议、16k 页未验证、
  min(rec_lsn) 未启用——维持原口径。
- **CI tarpaulin 终判**:M5 出口遗留(push 后);M6 新代码继续承接
  pg-am-hnsw ≥90% 口径,新增 pg-engine/pg-txn 侧代码由各 test
  matrix 承接(沿用 M5 判项形态)。

---

## §14 验收标准(供 coding-plan 引用)

1. **并发正确性**:100 并发 INSERT+DELETE 24h(验收)/ CI 缩短版
   (参数化轮次,下界 §12.2),图结构语义一致:
   - 审计全绿(**quiesce 后**,§12.5;无悬挂节点 = 全部 LIVE 节点
     自入口点可达——依赖 §8.3 的可达性扫描能力;残态计数在预算内);
   - 双向边不对称率**增量** < 1%(基线 15.35%,§12.3 口径);
   - recall@10(过滤后,ground truth 见 §7)≥ 单线程基线 95%;
     并发 abort 后同门。
2. **事务语义**:并发 insert × 崩溃叠加恢复后,已 commit 可见、
   未 commit/abort 不可见(回表过滤 + tombstone 审计计数钉);
   in-flight INITIALIZING 残留 ≤ **崩溃窗口 × 并发度**(审计钉);
   **v1.4 P1 回归钉**:in-flight delete × checkpoint × SIGKILL 恢复后
   行可见 ∧ 节点 LIVE ∧ 映射在;committed delete × 即刻崩溃(清理
   前)由 vacuum 死行通道补刀后 tombstone 落位。
3. **崩溃恢复**:并发 stress × SIGKILL 后恢复 <30s 口径继承 M5
   §13.2(checkpoint 后增量窗口);恢复后 recall 门不破。
4. **无回归**:M4/M5 全部门禁(recall_siftsmall 0.9990、
   recall_after_recovery、crash rounds、loom、criterion smoke、
   tarpaulin ≥90%)保持绿;单写者路径位级可复现性不破。
5. **对标报告**:pgvector(Qdrant 可选)recall/latency/throughput
   对照落盘 docs/phase2-m6-benchmarks.md。

---

## 修订记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-09-30 | 初稿(草案,待对抗审查与用户终审)。基于:ROADMAP Phase 2c(:280-299)与风险/开放项(:745/:763/:765/:775);M5 冻结成果(页驻图、八步序、WAL 121–127、redo、审计)与 M5 登记的 M6 事项全清单(stage_spec:1076-1529、coding-plan:114-430);M5 选型骨架沿用(§1-§13 形态,M6 扩到 §14)。核心裁决:并发走节点级 latch + EBR(读路径 latch-free);事务走"图内立即写 + heap MVCC 可见性 + abort tombstone undo"(对 ROADMAP :287"commit 时批量合并"字面的一处显著偏离,理由 §6.1);关联走旁路 B+Tree 映射(M5 冻结布局不动);tombstone 生效 + 步骤 5↔6 对调落地;VECTOR(n) 列进 heap(≤2000 内联),ANN 运算符归 Phase 4b |
| v1.1 | 2026-09-30 | 三轮复核(主线 + 对抗审查)闭合:①§4.2 重写——hwm 临界区扩到 DirAppend 落盘(含 DirLink 分支;目录 position-is-identity,DirAppend 落点与 hwm 分配必须同序)、meta 临界区重查(空图并发首插回补、MetaUpdate 只升不降防降级)、基线序统一为 5↔6 对调后的新序(消除与 §8.2 的内部矛盾)、M5 搜索语义误述订正(state 位从不作搜索谓词,无需新增 LIVE-only 过滤,对拍零冲击);②§7 关联裁决改 (b)+(c) 组合——heap 行携带 NodeId 为权威载体 + 旁路 btree 正查 + 回表行内复核(错配即弃),映射生命周期立文(abort 由 btree undo 对偶撤销,孤儿映射复核滤除、计入泄漏账);③§6.2 崩溃恢复对图零动作裁决(xid 不进图 WAL,require_utility_txn 门不动,stage_spec:1376 并案)、delete abort 的 un-tombstone 落地(§10 决策点落实为恰好一个新增记录类 HnswNodeUnTombstone,挂载点 IndexUndo engine.rs:188-209)、UPDATE 路径定义(非向量列图零动作/向量列 delete+insert)、flush 边界从每 insert 移到 commit(WAL 前缀性质);④§4.1 latch 表固定 4096 片预分配(删"随 hwm 增长"——分片数非常量则互斥即破)、读者目录快照自愈;⑤§9 物理回收收缩出 M6(页内无位图 = 格式变更;复用槽撞 NodeInit 占用校验),§5 EBR 随归窗口(偏离 #2 立文);⑥引用修葺:ColumnType 归属 pg-am-heap(tuple.rs:205,fixed_width Some(dim*4) 同步)、ScanContext 出处 pg-am-heap/src/access_method.rs:50-60、ROADMAP Phase 2c 行号校正(:286-291、:294);⑦口径补全:锁序补短命帧 pin 规则、loom 清单扩三面(hwm 临界区/meta 竞争/vacuum×insert)、CI 缩短版轮次硬下界、审计 quiesce 口径 + 入口点可达性扫描登记、残态上界改 ≤并发度/崩溃、ef 重搜硬上限与 ground truth 口径、vacuum 重定向计入不对称率预算 |
| v1.2 | 2026-09-30 | 四轮复核 P3 闭合:§13 登记"hwm 临界区 = insert 全局串行点"风险(position-is-identity 的正确性代价;§14 无吞吐量门、24h stress 只隐性覆盖;缓释方向归 coding-plan,选型层不承诺线性扩展) |
| v1.3 | 2026-09-30 | coding-plan 一轮审查 nano 回流(agent-23):§2 事务 hook 行的"index-undo HNSW 对接"引用漂移——stage_spec:584-585 实指 M2c 残留条目,真实登记处在 M5 选型 :584-585(loser 补偿段重写,v1.1 审查 P2-1 配套),本行修正;零设计变更 |
| v1.4 | 2026-10-08 | 五轮复核 P1 修复(coding-plan 审查发现,机制链亲验属实):**in-flight DELETE 的 tombstone 可经 checkpoint WAL-before-data(checkpoint.rs:478-494)或他事务 commit 前缀 flush 在未提交状态持久化,崩溃后 redo 无条件重放 + ATT 判 abort → 活行配 tombstoned 节点 → vacuum 误当死节点重连入边**。修复 = delete 图动作(tombstone + 映射删除)推迟到 commit 持久化之后(post-commit 清理,幂等可重入,崩溃跳过由 §9 死行驱动通道补刀)——危险形态结构上不可构造;**HnswNodeUnTombstone(128) 决策作废,M6 零新增记录类**(§1/§10);delete 的 abort 改图零动作;"commit 不碰图"精确化为"commit 记录单独决定可见性,删除动作可滞后补刀"(§6.2);§7 映射生命周期、§8.1 写侧、§13 风险、§14.2 验收同步。教训登记:四轮审查未覆盖"未提交记录经 WAL-before-data/前缀 flush 持久化"交错面,攻击面清单补该类 |
