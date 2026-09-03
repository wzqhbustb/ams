# Stage Spec — 各阶段实现规格与 PG 对照

> 记录每个 Stage 的交付内容、设计决策理由、以及与 PostgreSQL 的取舍。
> 本文档与 `docs/phase1-m2-tech-selection.md`（设计选型）、`docs/phase1-m2-coding-plan.md`（编码计划）配套：选型文档记录"打算怎么做"，本文档记录"实际怎么做的"。

---

## Stage L：Snapshot + curcid + Disk ClogBuffer + VisibilityOracle

**状态**：✅ 完成（M2b，commit `ad0395a`）
**工期**：预估 7–10 天
**验收**：§7.2 六用例 oracle 测试 7/7；ClogBuffer 集成测试 13/13；TP 负载 8 帧命中率 ≥95%（criterion）

### 交付内容

1. **Snapshot（`pg-txn/snapshot.rs`）**
   - §7.1 全字段：`xmin / xmax / xip`（SmallVec 32 内联槽）/ `current_xid` / `curcid`
   - 纯 XID 判定，无 LSN 参与（v2 修订 P2-8 移除 v1 的 `snapshot_lsn`）
   - `TxnManager::snapshot()` 在同一把 active-set 锁内同读 XID 时钟与活跃集合，`xmin <= xip[i] < xmax` 结构不变式由构造保证

2. **Disk ClogBuffer（`clog_buffer.rs` + `clog_file.rs`）**
   - 段文件 `clog/clog-{segment:08}.log`，128 MiB/段 = 2.68 亿 XID；首次 touch 稀疏预分配，未触碰区域读零即 `InProgress`，无需存在性检查
   - 4-bit/XID（高 nibble = 偶数 XID）：`0=InProgress / 1=Committed / 2=Aborted / 3=SubCommitted`（M3 保留）
   - SLRU：8 KiB 页 × N 帧 clock-sweep（默认 8 帧 = 12.8 万 XID 窗口，可配 [4, 1024]）；第一轮只逐干净帧，脏帧给第二次机会，整圈无解才带写回逐脏帧
   - 持久性（§6.4 / v2.3-21）：`set_state` 只标脏；驱逐写回**不 fsync**；唯一 fsync 点是 checkpoint Begin/End 之间的 `flush_dirty()`（`ClogFlush` trait 钩子）；`unsynced_segments` 兜底"驱逐写回但从未 fsync"的段（L 评审 P1 修复）
   - 崩溃丢失的 bit 由 `TxnCommit/TxnAbort` WAL redo 幂等重建
   - 单把 `RwLock` 护全部帧（"正确但粗"，分片是 Phase 7b）；自带 hits/misses 可观测量

3. **VisibilityOracle（`visibility.rs`）**
   - `Visibility` 三态（`Visible / Invisible / Uncertain`）；`Uncertain` 为 M2c 行锁等待预留，M2b 永不返回
   - `PgVisibilityOracle` 实现 §7.2 全判定**含 curcid 分支**——但 L 时仅在 oracle 层 + 单测；heap 实际调用的自由函数 `is_visible` 仍是 M2a 兼容版（无 `t_cid` 参数），executor 接线归 Stage O
   - hint bits：四变体枚举 + `set_hint_bit` trait 就位，默认 no-op（回写通道 Phase 7）

4. **begin 原子性修复（L 评审 P1）**
   - XID 时钟分配与活跃集插入合并到同一把锁内（对标 PG"释放 XidGenLock 前先注册 ProcArray"），消除"alloc 了但没 insert"的 SI 违例窗口，附确定性回归测试

5. **Engine 集成**
   - 装配顺序：ClogBuffer → storage recovery（注入同一 CLOG，redo 幂等写入终态）→ checkpoint ClogFlush 钩子 → M2a `clog-snapshot.bin` 一次性迁移（缺失 = no-op，损坏 = 硬错误）→ Catalog → HeapAM/TxnManager
   - M2a 的内存 `TrackingClog`（225 行）整体删除
   - commit/checkpoint barrier 保留承重：防"commit WAL 落在 begin_lsn 之前（replay 不重放）但 `set_state` 落在 checkpoint CLOG flush 之后"的两不沾窗口

### 设计理由

**1. 为什么 4-bit/XID 而非 PG 的 2-bit？**
每 XID 独占 nibble，无位掩码竞态、无跨 XID 读改写。代价是段密度减半（128MB/2.68 亿 XID vs PG 32MB/2.56 亿），磁盘换简单。不存 commit_lsn——判定纯靠 XID 关系。

**2. 为什么独立 ClogBuffer 而不复用 M1 BufferPool？**
CLOG 页无 `pd_lsn`/`pd_checksum`，混进按 `PageId` 索引、带 checksum/redo 路径的 BufferPool 会破坏其语义。

**3. 为什么 fsync 收紧到 checkpoint 单点？**
commit 路径持久性全靠 WAL 记录；CLOG bit 的 fsync 全部摊到 checkpoint。配合 commit barrier 保证"已 checkpoint 回收的 WAL 前缀对应的 bit 必然已 fsync"。崩溃丢失由 redo 重建，语义闭合。

**4. 为什么 curcid 在语句开始前 +1？**
同语句 self-scan 共享同一 curcid，`t_cid < curcid` 对本语句写入为假 → 跳过自身，天然 Halloween 保护；下一语句递增后前语句写入变为可见（§7.1 Q4 / v2.3-3）。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage L) | 取舍理由 |
|---|---|---|---|
| XID | 32-bit + wraparound/freeze | **64-bit**，无 wraparound、永不 freeze | xmin/xmax 各 8B（PG 4B）；无 vacuum freeze 概念 |
| CLOG 编码 | 2-bit/XID，位级 CAS | **4-bit/XID**，nibble 直读直写 | 密度减半换无锁简单 |
| SLRU 结构 | pg_clog SLRU，commit 路径也可能写回 | 结构同构（8KB 页/段/clock-sweep），fsync 收紧 checkpoint 单点 | 崩溃丢失由 WAL redo 重建兜底 |
| 快照一致性 | ProcArray 细粒度协议 | 单把 active-set 互斥锁 | M2b 规模够用；分片 Phase 7b |
| cmin/cmax | 双字段 + combo CID | **单 `t_cid`** + 分支对称判定 | combo CID 场景（同语句插删对外可见性）不处理，M2b 裁剪 |
| SSI | 可选可串行化 | **不做** | Phase 7d |

### 已知残留与后续归队

- engine scan/auto-commit 仍用 `Snapshot::everything()`，oracle/curcid 协议就位但未接线 → **Stage O 落地**
- 自由函数 `is_visible` 无 curcid 分支、heap 不盖 `t_cid`（Halloween 保护在生产路径未激活）→ **Stage O 落地**
- `txn_manager()` 后门绕过 commit barrier（注释声明 UB）→ M2c 下沉 barrier 进 TxnManager
- ClogBuffer 全局单锁 → Phase 7b 分片；hint bit 回写 → Phase 7
- 无 checkpoint ATT 快照 → **Stage N 加 AttProvider**

---

## Stage M：B+Tree AM 单线程 + Split 三步 WAL + 阻塞式 CREATE INDEX

**状态**：✅ 完成（M2b，414 个测试全绿，含 3 个崩溃点恢复测试）
**工期**：预估 10–14 天
**验收**：`btree_split_crash` 3/3；100 万 INSERT + CREATE INDEX ~14.9s（≤ 30s）；redo 幂等重放 10 次一致

### 交付内容

1. **页格式与 key 编码**
   - 复用 Stage G 的 slotted page（"一种页格式服务所有 AM"兑现）
   - 16B special 区：`btpo_prev`(0..8) / `btpo_next`(8..16)（Blink 兄弟链）
   - `pd_flags` bit 8..11 = `btpo_level`（0=leaf）、bit 12..15 = `btpo_flags`（LEAF / ROOT / DELETED / SPLIT_INCOMPLETE）
   - 保序 key 编码：Int4/Int8 用 sign-bit 翻转大端序；Text/Bytea 用原始字节序
   - 内部页 entry = `key ++ child_page_id(8B)`；叶子页 entry = `key ++ tid(10B)`；无 64B TupleHeader（索引条目无 xmin/xmax，§7.3）
   - LP 数组按 entry 保序（支持页内二分），重复 key 以 `(key, tid)` 全序决胜

2. **Split 三步 WAL 协议**（核心交付）
   - `BTreeSplitPrepare=5`：锚点 + `SPLIT_INCOMPLETE` 置位 + `left_old_next`（补 spec 的洞：左页 post-Prepare 镜像落盘后原 next 读不回，必须随 payload 携带）
   - `BTreeSplitCopy=51`：payload 极简（`copy_start_slot + left_page_pre_lsn`），redo 从 left_page **重算**搬运内容，幂等锚点 `left_page.pd_lsn == pre_lsn`
   - `BTreeSplitCommit=52`：父页插入 `(separator_key, right_page)` 分隔键 + 清 `SPLIT_INCOMPLETE`
   - 落盘纪律：**右页先 flush 才释放左页 latch**——保证左页 pre-copy 镜像永远可恢复，"左已截断但右缺拷贝"在结构上不可能（出现即 `MetadataCorrupted` 硬失败，不静默）
   - Copy 应用 = 左页重建压实（非裸截断 LP 数组——裸截断的死空间会让触发 split 的 insert 仍放不下），在线/redo 共用同一函数保证字节级一致

3. **读/写路径**
   - 点查：meta → root → 下降到叶（latch coupling 骨架），叶子内二分
   - 范围扫：叶链 `btpo_next` walk；下降过程内部层右跳、叶层**双向 sibling walk**（stale 分隔键兜底——兄弟链是 ground truth，分隔键只是提示）
   - 内部页 slot 0 = ∅(-inf) 标记（PG P_HIKEY 惯例，见下"设计理由"）
   - 单线程全路径独占 latch；`validate()` 做全序子树边界校验（`last < first`）

4. **阻塞式 CREATE INDEX（bulk load）**
   - 全表扫 → `(key, tid)` 全序排序 → 叶子 100% 写满自左向右 → 内部层自底向上（slot 0 = ∅）→ 每页一条 post-image FPI（~22MB WAL / 1M entries）→ **meta 记录最后写**（中途崩溃只剩孤儿页，零半成品）
   - 实测：1M entries ≈ 1.02s，比逐条 insert 快约两个数量级

5. **Engine 集成**
   - `Engine::create_index(table, column) -> Oid` / `index_lookup(table, column, key) -> Option<Tid>`
   - catalog 写入：`pg_class`(relkind='i', relam=403) + `pg_attribute`(兼任"索引哪列") + `pg_index`(page 5) + `pg_rust_relpages`(meta 页位置)
   - **DML 事务内同步维护索引**（review 后补齐）：insert/delete/update 与被维护表的索引在同一 auto-commit 事务内原子完成（NULL key 跳过）
   - redo registry 追加 5 个 btree handler（Insert/Delete/SplitPrepare/SplitCopy/SplitCommit）

6. **Review 修复清单**（三轮对抗审查后）
   - DML 同步维护索引（P1：修复前索引"建成即陈旧"）
   - `split_prepare` 的 `SPLIT_INCOMPLETE` 防护（禁止对未完成分裂的页二次分裂，否则旧右孪生永久孤儿化、毁掉 M2c CLR 收尾前提）
   - root 代际校验（防止旧句柄创建第二 root 覆写 meta 导致半树不可达）
   - Copy redo 静默跳过分支硬化（右页已持拷贝→重建左页；双侧不符→硬失败）
   - CatalogFull 预检前移（build 前按真实编码预检 4 行）
   - `create_new_root` 的 level ≥ 0x0F 显式报错（4-bit 溢出变契约）
   - `set_flag` 静默吞错改 `Result`；`apply_prepare/commit_left` 补全零页 init 守卫

### 设计理由

**1. 为什么 split 用"声明意图 → 可重算执行 → 提交"三步？**
分裂是跨页多步操作（左截断 + 右搬运 + 父插入），崩溃可落任意中间点。三步把"原子性"翻译成 WAL 语义：Prepare 给幂等锚点；Copy 利用"给定左页状态 + 起始 slot，搬运内容确定"的事实让 payload 保持 O(20B)（半页数据可能 10KB，记进 WAL 太奢侈）；Commit 显式标记不归点。配合 `SPLIT_INCOMPLETE`，崩溃后的任何前缀都能被 redo 精确续上或安全跳过。

**2. 为什么 Copy 的 redo 是"重算"而非"搬运"？**
redo 不是把 WAL 里的数据抄回页面，而是重新执行搬运操作。WAL 体积最小化 + 重放天然幂等（`pd_lsn == pre_lsn` 说明"还没动过"才执行）。代价是必须严格维护"左页 pre-image 可恢复"的落盘纪律——这个前提在 review 后从注释升级为有硬失败背书的协议。

**3. 为什么内部页 slot 0 用 ∅(-inf) 标记？**
内部页分隔键会 stale（删除推高孩子 low key、分裂改变边界）。真实 low key 作标记会 stale-high（逆序插入时把孩子藏到标记左边），∅ 标记只会 stale-low，由叶层双向 sibling walk 兜底。Blink 的设计核心是"分隔键是提示不是真理，兄弟链才是 ground truth"——`validate()` 因此检查全序链而非父键区间。

**4. 为什么 bulk load 不用 split 协议而直接铺页 + FPI？**
split 的 Copy redo 语义是"从既有左页重算搬运"，bulk load 的页是全新内容，语义不符。每页一条 post-image FPI 比逐条 insert 快两个数量级；meta 最后写让中途崩溃只剩孤儿页、零半成品。

**5. 为什么索引条目不参与可见性判定？**
索引只有 `(key, tid)`，没有 xmin/xmax。悬空引用（指向已删/abort 的行）由 heap 层可见性判定天然屏蔽（§7.3 契约）。索引不复制 MVCC 状态。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage M) | 取舍理由 |
|---|---|---|---|
| Split WAL | 单条 `xl_btree_split` 记录，右页内容随记录携带（block data） | **三条记录**，右页内容不记、redo 重算 | WAL 体积更小；借鉴 PG 全部锚点语义（`firstrightoff`↔`copy_start_slot`、incomplete-split 标志）但骨架自构 |
| 并发 | Blink latch coupling 读 + 乐观/悲观写 | **M2b 全单线程**（整路径独占），M2c 才做 Blink 并发（Stage Q，含 loom） | 先把协议骨架和恢复正确性做对，并发后上 |
| 内部页分隔键 | high key 完整键 + tid tiebreaker（PG ≥12） | key-only 分隔 + ∅ 左脊柱 + sibling walk 兜底 | 简单；已知残留：重复 key 跨内部页边界（~20 万同 key）退化，M2c 上 tid 分隔符根治 |
| CREATE INDEX | 在线建索引（读写不阻塞） | **阻塞式**（全表扫+排序+bulk load），无表锁（扫描期间新写入不进索引，已文档声明） | M2b 规模下正确性优先；在线建索引归 Phase 7 |
| 页合并 | 有（balance merge） | **不做**（只分裂不合并） | M2b 明确裁剪；删除只回收 tuple 不回收页 |
| 唯一索引 | 执行层唯一性检查 | `indisunique` 字段就位**不执行** | 归 Stage O/后续 |
| 索引维护 | DML 自动维护所有索引 | review 前"建成即快照"，**修复后** DML 事务内同步 | 对抗审查抓出后补齐，避免语义炸弹 |
| DROP INDEX | 支持 | **不支持** | 后续 stage |
| MVCC 集成 | 索引无可见性，靠 heap 回查 | 同 PG（§7.3 契约） | 一致，无取舍 |

### 已知残留与后续归队

- 重复 key 跨内部页边界（~20 万+ 相同 key 门槛）：结构可读但退化 → ~~M2c tid 分隔符根治~~ **延期**（M2c 未交付；Stage T 改由"完整 (key,tid) 链序落位 + locate 空页左跳"在线兜底跨边界重复 run 场景；根治归 M2c+/Phase 7）
- ~~`BTreeSplitCLR` / `finish_incomplete_split`（未完成分裂收尾）→ M2c undo（Stage S）~~ **已交付**（Stage S + 修复轮的 Move/NoMove/Unlink 三态与级联）
- ~~BTreeDelete 在线语义（现在只有 redo handler；trait delete 为 O(n) 链扫兜底）→ M2c~~ **已交付**（Stage Q 并发化 delete：归属重验证 + 有界重试；Stage T 边界修复进一步加强）
- 唯一索引执行、DROP INDEX、多列索引 → 后续 stage
- SQL 层入口（`CREATE INDEX` 语句、planner 索引选择）→ Stage O

---

## Stage N：ARIES Analysis + Redo + CheckpointEnd v1/v2 迁移

**状态**：✅ 完成（M2b，commit `3435ce6`；448 测试全绿）
**工期**：预估 5–7 天
**验收**：`aries_analysis_redo` 8/8（10 万 record analysis+redo 2.24s ≤ 10s）；`checkpoint_v1_v2` 迁移测试通过

### 交付内容

1. **Analysis（`pg-storage/analysis.rs`）**
   - `find_latest_checkpoint_end`：从 superblock `checkpoint_lsn` 起扫，取最后一个**已完成**的 CheckpointEnd（悬空 Begin 天然忽略）
   - `run_analysis`：ATT/DPT 快照文件 seed 基线 + 扫到 WAL 尾；带 `txn_id` 的记录入 ATT、Commit/Abort 移除；DPT `or_insert` 保留首次脏页 LSN
   - `for_each_touched_page`：bincode 前缀解码只取 PageId，不解 tuple/FPI 全载荷（11 种 page-modifying 类型逐一比对字段序）

2. **Redo 统一分发**（Stage D 机制，本 stage 接线验证）：严格 LSN 序；FPI 与其他 handler 同一 `RedoRegistry`；未注册类型硬失败（v2.3-24）

3. **CheckpointEnd v2**
   - 6 字段：`checkpoint_lsn / next_page_id / next_txn_id / next_oid / att_file / dpt_file`
   - v1/v2 分派 `flags >> 4`（M1 冻结的记录头 `flags` 是 u8，版本号占高 4 位，低 4 位留记录级 flag）；未知版本硬错误（前向 crash 保护，v2.3-17）
   - v1 默认 `next_oid=16384`；权威源仍是 superblock，record 值永不消费（write-only，防未来误信）
   - v1 记录永不改写，升级靠 M2 自己 emit v2 自然完成

4. **ATT/DPT 快照文件**
   - `meta/{att,dpt}-{lsn:016}.snapshot`，bincode + CRC32，`write_atomic`（temp + fsync + rename + 目录 fsync）
   - 三步硬序：`fsync(快照文件) → wal.append(CheckpointEnd) → flush_to(end_lsn)`，superblock 最后更新（与 §3 P1-5 commit 硬序同风格）
   - prune 保留最近 3 组 + **superblock 当前组**（防连续夭折 checkpoint 的孤儿组挤出有效基线）；文件名解析接受任意长度数字串
   - 快照缺失/CRC 损坏 → 独立降级为空基线全扫描

5. **B+Tree split redo 四态硬化**（review 修复）
   - `== anchor` 正常重放，应用后 **`pool.flush(right)`**——redo 路径补齐线上"右页先落盘才放左 latch"纪律，关闭"恢复期间部分刷盘再崩溃变砖"窗口
   - both-past 幂等跳过（修掉旧代码误截 post-copy 插入的真 bug）；左落后右已有 → 重建左页；其余 → 硬失败
   - 删除错误的 `apply_split_move_to_right_only`（其前提状态不可达，可达时反把硬失败降级为静默损坏）

6. **WAL 全零洞防护**（Stage B 遗留，review 抓出）
   - `reserve_and_append` 原子化：单次锁持内完成时钟推进 + 写段，消除 reserve→append 窗口
   - reader 遇全零 header 前探一个 header 宽度，后有非零数据 → `MetadataCorrupted` 硬失败（不再静默截断、不再让新 WAL 覆盖洞后已提交记录）

7. **Buffer pool 两处竞态修复**
   - P2-1：`pin_mut` 持 guard 期间即标脏——fuzzy checkpoint 不再漏"已写 WAL 但 guard 未 drop"的页
   - P2-2：flush 失败 `first_dirty_lsn` 改 min-merge 恢复（取最旧锚点，防 rec_lsn 高估跳 redo）；`flush_frame` 对 replayed-LSN 页跳过 `flush_to`

8. **ATT 正确性接线**
   - `AttProvider` trait + `TxnManager` 实现；recovered ATT 在 redo 重建 CLOG 后过滤（去已知终结成员，闭合 §11.4 快照竞态）
   - pg-engine `commit_barrier`：commit 硬序与 checkpoint 互斥；无 barrier 的 storage-only 路径文档明确标注 unsafe（M2c 下沉进 TxnManager）

### 设计理由

**1. 为什么 redo LSN 恒等于 `checkpoint_lsn`（而非 `min(DPT.rec_lsn)`）？**
双向钳制（coding plan Stage N 实现修订注）：不能更晚——redo point 与首个脏页记录之间的 `TxnCommit/Abort` 必须重放以重建 CLOG；不能更早——DPT 快照摄于 CheckpointBegin，条目 rec_lsn 均 < begin_lsn，而完成的 checkpoint 在 emit End 前已将这些页全部刷盘，其 WAL 段可能已被回收。DPT 仍完整返回（观测 + 未来 Undo 用）。

**2. 为什么不做显式 Heap Undo？**
无终止记录的 XID 在重建 CLOG 中读作 `InProgress`，MVCC 下与显式 `Aborted` 等效——"过滤"替代"补偿"，省掉整个 undo/CLR 子系统（B+Tree 结构变更的 CLR 收尾归 M2c Stage S）。

**3. 为什么 ATT/DPT 存独立文件而非塞进 CheckpointEnd payload？**
大 ATT（10 万级 XID）进单条 WAL 记录太奢侈；CheckpointEnd 只带文件名引用，快照文件独立 atomic write + 独立降级。

**4. CheckpointEnd 新于 superblock 怎么办（crash 在 flush_to 与 superblock 写之间）？**
不用 WAL 里更新的 End，而是合成 v1 等价空基线锚点、从 superblock redo point 全量重建——保守但确保两个 redo point 之间的记录全覆盖。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage N) | 取舍理由 |
|---|---|---|---|
| 恢复模型 | 无 ATT/DPT 概念，从 checkpoint redo 点全量重放 | 教科书 ARIES Analysis 建 ATT/DPT，但 redo 起点恒等于 `checkpoint_lsn` | 效果与 PG 等价；DPT 为 M2c Undo/CLR 预留观测面 |
| Heap Undo | 不做（abort = CLOG 标记，非教科书 ARIES） | 同 PG：不做，`InProgress ≡ Aborted` | 语义等效，省掉补偿日志 |
| Checkpoint 元数据 | `pg_control` 内嵌 checkpoint 记录 | superblock（双副本）+ CheckpointEnd v2 + ATT/DPT 独立快照文件 | ATT/DPT 独立文件支持大事务数；PG 无此需求 |
| 格式迁移 | 大版本 `pg_upgrade` 离线迁移 | v1/v2 在线 decode 分派，v1 永不改写 | 单文件格式内渐进升级 |
| CLOG 持久化 | checkpoint 时 CheckPointCLOG | 同（Begin/End 之间 `flush_dirty`） | 一致，无取舍 |
| 恢复扫描 | 单遍 redo | 三遍（find_latest / analysis / replay） | 可读性优先；合并是已知优化项 |

### 已知残留与后续归队

- ATT 空基线降级对"begin 前已无 WAL 活动的空闲事务"不完整（崩溃后空闲事务无 WAL 记录可扫描；**核销说明（Stage T)**：该情形无实际危害——空闲事务无任何写入需要补偿，CLOG 读作 InProgress 语义等价，Stage S undo 落地后此边角无消费者；降序重试旧快照仍是可选项）
- `open_at` 起始段缺失时硬失败，文档承诺的 warn + 空基线降级未实现（灾难-only 路径，多段丢失无测试）
- `reserve_and_append` 时钟推进后 encode/IO 失败可留 >32B 洞，reader 前探（仅 32B）漏检（待修：推进前校验 payload 长度 + IO 失败毒化 writer）
- analysis/replay 的 catch-all 对 `MetadataCorrupted`/`WalReadFailed` 仍 warn+break（engine 路径由 writer open 先行拦截，pub API 直接调用有静默截断风险）
- 恢复三遍全量扫描可合并（find_latest 可短路）
- both-past 跳过的回归测试输入不含 post-copy 插入，对新旧行为不可区分（待补强）
- 测试缺口：小段 + 段回收、快照 CRC 损坏降级、孤儿快照、checkpoint 介入 split 三步、恢复中二次崩溃
- `evict_frame` 刷盘失败帧泄漏（pre-existing，非本 stage 引入）→ **已修复**（Stage Q 终审：改为先刷盘后摘映射）
- ~~coding plan Stage N 表格 `flags >> 12` 系笔误（实现为 `>> 4`），待回写~~ **已核实**：`>> 12` 出自 tech-selection §11.4（其假设 `WalRecord.flags: u16`，版本号占高 4 位）；M1 已冻结 32B 记录头为 `flags: u8`，实现实际取 u8 高 4 位 `flags >> 4`（`pg-storage/src/wal/record.rs` `CheckpointEndRecord::decode`，偏离原因已在 `CHECKPOINT_END_VERSION_V2` 文档注释注明），coding plan Stage N 表格已如实记录 `>> 4`，无需再回写
- ~~kill -9 撕裂尾部被误判 WAL corrupted~~ **已修复（Stage T 压测抓出）**：writer 被 kill 时记录只写了前缀、预分配段的未写部分读回全零 → CRC 失败被当中段损坏。修复：`is_torn_tail` 双重判定（header LSN == 读取位置 + 记录之后全零）放行撕裂尾部，中段真损坏维持硬报错；真实现场前后对照验证 + 两个确定性单测

---

## Stage O：SQL parser + M2b 综合验证（M2b 出口）

**状态**：✅ 完成（496 测试全绿；M2b 出口，`phase1-m2b` tag 待打）
**工期**：预估 7–10 天
**验收**：`m2b_integration` 20/20 + `si_isolation_50_txn`（50 线程 SI 硬断言）；§7.2 SQL 层 4 用例（2 个 RETURNING 用例子集 N/A，由 pg-txn 单测覆盖）；INSERT+COMMIT **4.24ms**（≤5ms）；索引点查 **~927K QPS**（≥100K，criterion `m2b_perf`）

### 交付内容

1. **硬编码 SQL parser（`pg-engine/sql.rs`）**
   - 子集：`BEGIN / COMMIT / ROLLBACK / CREATE TABLE / INSERT（多行，可选列清单）/ SELECT [WHERE eq/lt/gt] [ORDER BY 单列 ASC|DESC] [LIMIT N] / UPDATE (WHERE) / DELETE (WHERE) / CREATE INDEX`
   - tokenizer → AST → Datum 全程无字符串拼接（无注入面）；标识符统一折叠小写（同 PG 未加引号语义）；可选单末尾分号
   - 不支持清单（`--`/块注释、quoted identifier、多语句、RETURNING、JOIN、聚合、`<=/>=/<>`）在模块文档如实列出

2. **`exec(Option<&TxnHandle>, sql)` + `TxnHandle`**
   - auto-commit 传 `None`，显式事务传 handle；`commit/abort` consume self（abort 后不可用是**编译期**保证）；`RefCell<Snapshot>` 使 handle `!Sync`
   - Drop 自动 best-effort abort（持 `commit_barrier` + 失败 warn 日志）；`instance_id` 校验防跨 Engine 实例混用
   - `exec(None)` 收到 BEGIN/COMMIT/ROLLBACK 显式报错（review 修复：原静默 Ok 是"测试全绿、数据悄悄持久化"型 footgun）
   - 显式事务内 DDL 显式拒绝；语句中途失败无语句级回滚，唯一安全操作是 `abort()`（已文档化）

3. **curcid executor 接线**（Stage L 协议落地）
   - 每语句开始前 `advance_curcid()`；新 tuple `t_cid = curcid`；`stamp_deleted` 盖删除时 curcid
   - 自由函数 `is_visible` 重写为完整 §7.2（`xmin==self / xmax==self` 分支激活，v2.3-3 / Q4）
   - `begin_txn` 一次快照全事务复用（SI）；auto-commit 每语句新快照（等价 RC）

4. **Halloween 双保险**：UPDATE/DELETE 先物化全量扫描再逐行写 + `t_cid == curcid` 分支

5. **索引事务性**（review 拦路虎修复）
   - `index_lookup` 可见性掩码：`lookup_all` 走叶子链枚举重复 key 全部 TID，逐个回堆 §7.2 判定，第一个可见胜出
   - per-txn 索引 undo 日志：`Inserted/Deleted` 按 XID 记录（UPDATE = 两条），abort / Drop / auto-commit 失败三处**逆序**回放（Inserted → `(key,tid)` 精确删，Deleted → 重插）；commit 丢弃；undo 失败 best-effort + warn
   - **恢复侧 loser 补偿**（Stage T 压测发现，并发 crash 轮 + checkpoint 线程复现）：在线 abort 有 undo 日志兜底，但 kill -9 落在"索引维护已落 WAL、commit/abort 记录未落"的窗口时，恢复后 loser 的索引**删除**无补偿——索引条目物理消失（xid=0 记录无条件重放）而堆元组经 CLOG 过滤仍可见，可见行丢索引项（loser 的插入方向由可见性掩码兜底，无需处理）。修复：open 时扫描 WAL，收集 loser 的 HeapDelete/非 HOT HeapUpdate 受害 tid，按堆页字节重算 `(key, chain_root_tid)` 幂等重插（存在性检查防重复；回归 `m2b_index_txn.rs::crash_mid_delete/update_compensates_index_entry`，修复前红）。扫描起点（复核追加）：redo_start 只对 DPT 采样时仍脏的页回拉，受害页被驱逐后可落在 redo_start 之前、同一 retained 段内——记录可跨段、段首非边界，故经 CRC resync 探针回卷到段内首个记录（回归 `crash_loser_delete_before_redo_start_compensated`）。已知边界：记录所在段已被回收时不可补偿（需跨多 checkpoint 的手持长事务）
   - 8 个专项测试（`m2b_index_txn.rs`）：insert/delete/update-abort 三向发散全部钉死，修复前必然失败

6. **崩溃自动化**：`m2b_crash_rounds` 子进程 kill -9；偶数轮全量精确比对，奇数轮前缀持久性验证（"至多 1 行多余"由 seed 设计保证逻辑严密）；默认 25 轮（CI），`M2B_CRASH_ROUNDS=1000` 为验收配置（~30–60 分钟，手动）

7. **Review 修复清单**（四轮对抗审查后）
   - 索引事务性（见 5）；`exec(None)` 事务语句报错；i64→i32 静默截断改 `try_from` 报错（插入错值 + WHERE 查错行双危害）
   - Drop abort 补 barrier + warn；标识符大小写规则统一；尾部分号；`instance_id`；`lookup_all` 跨 7 叶 3000 重复项测试
   - 负例测试（畸形 SQL / 未知表 / 类型不匹配 / 事务内 DDL）；§7.2 用例注释去虚报（case2/3 标 N/A）；`exec_crash_recovery_basic` 正名 `exec_clean_shutdown_reopen`

### 设计理由

**1. 为什么硬编码 parser 而非引入 sqlparser crate？**
子集极小（约 10 种语句形态），零新增依赖，tokenizer→AST 直译。它是 Phase 4a DataFusion 到来前的一次性脚手架——控制依赖面优先于表达能力。

**2. 为什么 TxnHandle consume-self？**
commit/abort 后句柄不可用由类型系统保证，消灭"use after abort"整类错误；`!Sync` 阻止跨线程共享同一事务上下文。

**3. 为什么索引 undo 用逆操作回放而非 PG 式"留着等 vacuum"？**
pg_rust 的 DELETE 是物理删索引条目（M2b 无 vacuum 回收死条目），abort 必须能恢复条目；插入侧悬挂条目与 PG 同构（可见性掩码兜底）。逆序回放是必须的：同事务 INSERT(k,t)+DELETE(k,t) 正序回放会留下悬挂项。

**4. 为什么默认 SI 而非 PG 的 RC？**
Agent 场景长事务多读，SI 避免语句间视图漂移；M2b 实现上 auto-commit 每语句新快照即等价 RC，两种语义同构复用。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage O) | 取舍理由 |
|---|---|---|---|
| 默认隔离级别 | RC | **SI**（begin_txn 一次快照） | §8 决策；SSI 留 Phase 7d |
| cmin/cmax | 双字段 + combo CID | 单 `t_cid`（同语句插删对外判定为不可见，方向保守） | M2b 裁剪；扫描全物化使分歧不可达 |
| 语句级回滚 | 子事务 | **不做**：语句失败唯一安全操作是 abort() | M2b 无子事务，已文档化 |
| RETURNING | 支持 | **不支持**（§7.2 case2/3 由 pg-txn 单测覆盖） | 出口裁剪 |
| 索引删除 | DELETE 不动索引，vacuum 回收死条目 | DML 时物理删条目，abort 逆操作恢复 | M2b 无 vacuum；条目即删即净 |
| 索引可见性 | index scan 回堆判定 | `index_lookup` 回堆 §7.2 掩码（同构） | 一致；但 SQL SELECT 暂不走索引（无 planner），点查仅程序化 API |
| 索引 abort | 插入侧悬挂条目留待 vacuum | undo 回放立即清除；undo 失败仅 warn（掩码兜底） | M2b 无索引一致性修复工具 |
| 事务内 DDL | 事务性 DDL | **显式拒绝** | 目录改动无 undo，M2b 裁剪 |
| SQL 方言 | 完整 SQL | 硬编码子集 + 不支持清单 | Phase 4a DataFusion 前的脚手架 |

### 已知残留与后续归队

- `index_lookup` fresh-snapshot：显式事务内无 read-your-writes（文档 WARNING 已明示；上层逻辑应走 `exec`）
- undo 回放失败仅 warn，无索引一致性修复工具 → 后续 stage
- redo 不恢复 `t_cid`（良性：写入事务不可能活过崩溃；子事务/语句级回滚到来时重审）
- hint bit 回写仍未接线 → Phase 7；`set_hint_bit` 占位
- 唯一索引不执行、DROP INDEX、多列索引 → 后续 stage
- 1000 轮崩溃验收配置需手动执行（默认 25 轮过 CI）；mid-checkpoint kill 仅概率性覆盖，无确定性保证
- `exec_auto` 的 SELECT / CREATE INDEX 不走 commit barrier（只读路径，已注释说明）
- 锁管理器、行锁 xmax 协议、B+Tree 并发、HOT update、ARIES Undo/CLR → **M2c（Stage P–T）**

---

## Stage P：LockManager 表锁 + 行锁 xmax 协议 + SELECT FOR UPDATE

**状态**：✅ 完成（M2c 开篇，534 测试全绿；未提交）
**工期**：预估 5–7 天
**验收**：`lock_manager` 8/8（4×4 全矩阵）；`row_lock_wait_wake` 8/8；100 线程并发 UPDATE 同一行终值精确无 lost update；无冲突 UPDATE ~11.2K TPS（30K 未达——per-commit fsync 物理上限，与 M2a 无锁基线 11.6K 持平，证明锁路径零额外开销；bench 头部如实声明）

### 交付内容

1. **表级 LockManager（`pg-txn/lock_manager.rs`）**
   - 4 模式 grant 矩阵（§9.2）；键为 `pg_storage::Oid`（遵守"pg-txn 不依赖 pg-catalog"硬约束）
   - `LockEntry` = granted 集 + FIFO wait 队列；`can_grant` 三道闸门：已持同级或更强 → 幂等放行 / 队列非空且非队头 → 必等（**反饿死**：兼容模式也不许插队）/ 与其他持有者无冲突
   - 升级原地 max 强度；冲突升级保留旧授权排队（与 PG 一致，互升死锁归 Stage R）
   - `release_all(xid)` 清授权 + 清队列项（防死 XID 队头毒化）+ 顺序预授权兼容连续队头；2PL：持锁到事务结束，只升不降
   - `table_lock_state()` 内省快照 = Stage R 表锁半边 wait-for 图的输入

2. **行锁等待设施（`pg-txn/manager.rs`）**
   - `row_wait_registry: Mutex<HashMap<Xid, Xid>>`（waiter → waiting_on）+ Condvar
   - `wait_for(self, blocking)`：谓词直查 active set（不依赖注册边），吸收虚假唤醒，醒来自清边；self-wait 报错；锁序 registry→active（唯一合法嵌套方向，已注释）
   - `end_txn`：commit/abort 共用尾部，`set_state → active 移除 → notify_all` 严格序（广播必在 CLOG 置位之后，被唤醒者重读 CLOG 必见终态）
   - `RowWaiter` 窄 trait 供 heap 层注入（比传整个 TxnManager 窄，便于测试）

3. **commit barrier 下沉 TxnManager**（兑现 Stage L/N 的 M2c 计划）
   - `commit_txn/abort_txn` 内部对整个硬序持 barrier 读守卫；checkpoint "Phase 0" 持写守卫覆盖临界段（begin_lsn 捕获 → ATT/DPT 采样 → CLOG flush → WAL 回收），范围与下沉前逐点相同
   - pg-storage 侧沿用 AttProvider/ClogFlush 模式：`set_commit_barrier` 共享 `Arc<RwLock<()>>`
   - pg-engine 删除自有 barrier 字段及全部守卫点；`txn_manager()` 后门的 checkpoint UB 按构造消除（残余：裸调 commit 不释放表锁，已文档化）

4. **行锁 5 步 xmax 协议（`pg-am-heap/heap_am.rs`）**
   - `row_lock_gate` 在页写 latch 下判定：INVALID/self → Proceed；Committed → `TupleConcurrentlyUpdated`（新错误，与 TupleNotFound 明确区分）；Aborted → Proceed；InProgress → **latch 内注册等待边**后返回 Wait（5a 先于 5b，绝不丢唤醒）
   - delete/update/lock_tuple 改 restart 循环：Wait 时 drop 全部 latch → `wait_row_lock` → 重回步骤 1（"CAS"由页 latch 天然提供，无需原子指令）
   - 崩溃豁免：InProgress 且不在活跃集 → 重读一次 CLOG（happens-before 经 active mutex 传递闭合）后才认定崩溃覆盖——修掉了实现期发现的"CLOG 置位与活跃集移除之间的误判窗口"真 bug
   - 未安装 RowWaiter 时完全保留 M2b 旧行为（heap_abort_visibility 等既有测试原样通过）

5. **HEAP_XMAX_LOCK_ONLY + SELECT FOR UPDATE**
   - `HEAP_XMAX_LOCK_ONLY = 0x1000`（PG 同构）：xmax 置位但非删除——全部 t_xmax 读者（scan/live gate/vacuum 扫描/redo/engine 掩码）逐一核对正确屏蔽；真删除章清 LOCK_ONLY（live + redo 两侧）
   - `lock_tuple`：同一 5 步协议盖 lock-only 章，**不写 WAL**（与 PG 一致；带锁章页面落盘后崩溃，恢复出死 XID 锁章，读者屏蔽、写者经崩溃豁免覆盖，无永久阻塞）
   - parser：`FOR UPDATE` / `FOR SHARE` 子句（LIMIT 后、Eof 前）；exec：filter/ORDER BY/LIMIT 之后、投影之前逐行加锁（与 PG 一致只锁返回行）；FOR SHARE 报 `Unsupported`（multixact 占位）；auto-commit FOR UPDATE 锁随语句结束释放

6. **表锁接线（pg-engine）**
   - 全路径覆盖：exec 各臂 + 公共 DML/DDL；SELECT→AccessShare、DML+FOR UPDATE→RowExclusive、CREATE INDEX→Exclusive（整个 build 单事务化）、CREATE/DROP TABLE→AccessExclusive
   - 释放点：auto_commit 成功/失败双路径 + TxnHandle commit/abort/Drop 五处全核对
   - `lock_table_entry` helper：取锁成功后**重验 registry**（name→OID 一致），配合 drop_table"事务内摘除 registry 先于 commit 放锁"的排序，关闭 TOCTOU

7. **Review 修复清单**（三路对抗审查后）
   - **create_index 快照在 Exclusive 锁等待前获取 → 索引永久缺行**（高）：闭包内取锁后重取快照，测试补 `index_lookup(id=2)` 断言（修复前必红）
   - **跨页 UPDATE 双 latch AB/BA 死锁**（中）：两个 latch 一律按 PageId 升序获取，重 pin 后重查空间 + `new_slot` 最终持锁后计算
   - **table_entry → lock_table TOCTOU**（中）：向已 drop 表的已释放页写入；修复见 6
   - **自真实删除章被补 LOCK_ONLY 复活行**（中）：gate 加 `for_lock` 细分——自 LOCK_ONLY 重锁幂等放行，自真实删除章上锁报错
   - 文档类：wait_for 无超时语义、锁序注释、stamp_lock_only 的 t_cid 有损性、FOR UPDATE 值序加锁死锁面、auto_commit panic 策略

### 设计理由

**1. 为什么"CAS"不需要原子指令？**
页 write latch 使"读 xmax → 判定 → 盖戳"天然原子。§9.1 的 CAS 语义由 latch 串行化提供，等待路径只需保证"注册先于放 latch"。这与跨页 update 既有的"放 latch → 重验"结构同形，改动面最小。

**2. 为什么锁章不写 WAL？**
锁是纯内存语义：崩溃后事务不存在，锁无需恢复。带锁章页面落盘后崩溃，恢复出的死 XID 锁章对读者被 LOCK_ONLY 屏蔽、对写者经崩溃豁免覆盖——WAL-less 既正确又省去 FOR UPDATE 的写放大。XID 64 位单调无复用，排除"陈旧锁章撞上复用 XID"。

**3. 为什么 barrier 下沉用共享 `Arc<RwLock<()>>` 而非 trait+guard 对象？**
与 `set_att_provider`/`set_clog_flush` 的 setter 模式一致且最简单；guard 对象方案卡在生命周期上，收益只是类型层面的抽象。

**4. 为什么 FIFO 公平队列？**
无公平性则 AccessExclusive（DDL）在持续读流下饿死；代价是兼容模式也不许插队（并发 AccessShare 吞吐略降），M2c 规模下正确性优先。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage P) | 取舍理由 |
|---|---|---|---|
| 死锁 | wait-for graph + 100ms 检测 + victim abort | **无检测无超时**，锁环 = 挂起 | 归 Stage R；等待边结构（`wait_edges`/`table_lock_state`）已按可消费形状预留 |
| 行锁等待唤醒 | 锁队列按序授予 | 唤醒后与全新写者**平等竞争**盖戳，可饥饿 | 与 PG 行为一致；公平队列留待需要时 |
| 并发更新冲突 | EvalPlanQual 重查（RC）/ could-not-serialize 报错（RR+） | 无 EPQ，`TupleConcurrentlyUpdated` 由调用方新快照重试 | 等价 PG RR 语义；EPQ 是执行器工作，后续 stage |
| FOR SHARE | multixact 共享锁 | **解析后报 Unsupported** | multixact 是独立子系统，占位归后续 |
| 锁章持久化 | LOCK_ONLY 不写 WAL | 同 PG | 无取舍 |
| 表锁自省 | pg_locks 视图 | `table_lock_state()` 程序化 API | 系统表形态归 Phase 6 |
| 升级死锁 | 检测器兜底 | 存在且不处理（同 PG 语义，无检测器） | Stage R |
| 快照读 | 普通 SELECT 不需要表锁以上的东西 | 同；但 `Engine::scan`/`index_lookup`（无所属 XID 的裸 API）连 AccessShare 都不取 | 无 XID 无法 key 锁；DDL 竞态缺口已文档化 |

### 已知残留与后续归队

- **任何行锁/表锁等待环 = 永久挂起**（无检测无超时）→ **Stage R 死锁检测**（接口已预留：`wait_edges` + `table_lock_state`）
- 无冲突 UPDATE 30K TPS 未达（~11.2K，fsync 物理上限；group commit 批窗口/ramdisk 可证锁路径非瓶颈）→ 性能归 Phase 7b
- 唤醒后盖戳无公平性，高竞争下个别事务可饥饿 → 需要时再做
- `lock_tuple` 覆盖 `t_cid` 有损（同语句自插后自锁使行对本语句不可见；当前 executor 不重扫，不可达）→ 子事务/EPQ 到来时重审
- 闭包 panic 跳过 `release_all`（锁 + XID 泄漏，进程级故障策略）→ 已文档化，或后续 catch_unwind
- 裸 `txn_manager()` commit 不释放表锁 → 已文档化；Stage R 落地前考虑守卫包装
- B+Tree 并发（latch coupling + Blink 读写路径 + loom）→ **Stage Q**
- 跨页 UPDATE 空间复查重启无上界（实践中对手有进展必终止）→ 观察项

---

## Stage Q：B+Tree 并发（latch coupling + Blink）+ loom

**状态**：✅ 完成（549 测试全绿；未提交）
**工期**：预估 7–10 天
**验收**：`btree_concurrent` 8/8（含 100 线程 smoke + 小池驱逐风暴 + watchdog 防死锁）；loom 2 模型 20,393 个交错全绿（`LOOM_MAX_PREEMPTIONS=3` 命令通过，模型 1 自钳 2 档已披露）；soak smoke（60s × 32 写 + 4 扫，release）无 miss；TPS 对照臂证明 latch 非瓶颈（见下）

### 交付内容

1. **Latch 拓扑铁律**（index.rs 模块文档：死锁自由的根基）
   - 只向 **DOWN**（root→leaf）与 **RIGHT**（左→右兄弟）获取 latch；绝不向上；持有任何 latch 时绝不向左（左跳一律 drop-then-acquire）
   - Split 按 left→right 持双页；父页绝不在持有子页 latch 时新获取；pessimistic pass 全程持 root 写 latch ⇒ 在线 split 彼此完全串行，读者与乐观叶子插入仍并发

2. **读路径真 crabbing**
   - `descend_to_leaf_guard`：持父读 latch pin 子页再放父（修掉 Stage M"先放父再拿子"的并发窗口）
   - 耦合右跳：持当前页 pin 右兄弟再放当前页；空右孪生（Prepare 未 Copy）跳过语义保留；`MAX_CHAIN_HOPS` 保留
   - 读/扫路径全部消费下降返回的叶 guard（消除 drop-再-pin 窗口）

3. **写路径 optimistic**
   - 读耦合下降 → `pin_leaf_for_write` 在写 latch 下**重验证叶子归属**（并发 split 挪走 key 区间则耦合右跳重 pin；左边界被抬高则 drop 后左跳）→ 去重 + 插入同一 latch 持有期完成（无重复洞）
   - 无 upgrade API（parking_lot 未暴露，且 drop-and-re-pin 的重验证本就不可避免）

4. **写路径 pessimistic + 空间预留**
   - 叶满 → 放全部 latch → `refresh_root_from_meta` → `descend_write_path` 从根耦合写 latch 下行，每层重验证（ROOT 旗校验、SPLIT_INCOMPLETE → Retry、右属 → Retry）
   - **空间预留**：`reserve_split_page` 在触碰 split 对之前分配右页，失败 → 释放重启（不裸抛 Err）
   - 三步 WAL 协议逐字节保留（Prepare/Copy/Commit 公有包装 + `*_on_guards` 内部实现）；flush-right-before-release-left 纪律不变；`split_commit_guarded` 沿已持有路径上行 Commit，WAL 记录与 Stage M 同序同内容
   - 重试预算 `MAX_INSERT_RESTARTS=256`；错误文案区分三种耗尽原因（并发风暴瞬态 / post-crash 不完整 split / stale 内部分隔键间隙）

5. **loom 模型检查**（pg-storage `sync` cfg 别名层）
   - `not(loom)` = 原样 re-export parking_lot（零成本 no-op）；`loom` = loom 原语薄包装（~40+ 调用点零改动）；**Arc 刻意不别名**（`Arc<dyn Trait>` 协变在 stable 不可行，且引用计数非竞争面）
   - loom 下 stub：WAL 后台 worker 不启动、flush_to 内联无 fsync、flush_frame 状态迁移（保留 meta→content 嵌套调度点）、setup fsync no-op（macOS F_FULLFSYNC 是探索速度杀手）；**真实 latch 编排全部在模型中运行**
   - 模型 1（2 写 1 读线性一致）：6,551 交错全绿（自钳 2 档，测试头披露）；模型 2（2 写竞争 split + root 提升）：13,842 交错满 3 档全绿

6. **并发测试与 bench**
   - `btree_concurrent.rs` 8 个：disjoint inserts 逐 key 点查、split 风暴精确计数、并发 scan no-miss（先快照 committed 集再扫描，竞态安全）、重复键 lookup_all、root 分裂竞赛、分配失败注入重启、**小池驱逐风暴**（16 帧强制 split+CLOCK 驱逐交织）、1h soak（`#[ignore]`，env 可调）
   - 全部带 watchdog（死锁回归 = 测试失败而非挂起）
   - `m2c_btree_tps.rs`：auto-commit 100T ≈ 6.6K TPS；**single-txn 对照臂 100T ≈ 13.5K TPS**（摊掉 per-commit fsync，超 m2a 无索引基线）——证明 15K 未达由 fsync/组提交路径主导，**非 B+Tree latch 竞争**

7. **Review 修复清单**（三轮对抗审查后）
   - **delete 的 WAL 记录写错页面**（高，确定 bug）：`pin_leaf_for_write` hop 后 WAL 仍写下降时的旧 PageId → redo 静默丢删除或误删无辜条目；修复为 `guard.page_id()` 重绑定 + 并发 split 中删除 + 崩溃恢复测试
   - **内部层左跳跨父边界 → cascade 把 downlink 插进错误父页**（高，潜在）：cascade 用栈中父页前先验证其确实持有指向 left 的 downlink（不满足响亮报错）；写路径内部层左跳改 Retry；场景入 Known limitations
   - **cascade 中途分配失败 → 子树永久楔死**（中）：`BufferPoolFull` 折叠进重试预算；边界入文档
   - **split_copy 的 flush(st.right) 驱逐窗口冒泡 PageNotFound**（中）：视为成功（当时注释声称"驱逐器必先完成 WAL-before-data 刷盘才摘页表项"——终审发现该顺序描述与实际相反，见下条终审修复）
   - **flush() 干净页快路径破坏 split_copy 耐久契约**（高，第三轮）：并发 flusher 清 dirty 但 fsync 在途时第二个 flush 早退 → 掉电可致 left-past/right-missing 不可恢复（checkpoint 变体可静默陈旧）；修复 `FrameMeta.flushing` + Condvar——并发 flush 等待在途 flush 完成耐久决策后才返回
   - **evict_frame 摘页表项先于刷盘，PageNotFound 容忍失效**（高，终审阻断项）：驱逐顺序原为 ①置 evicting → ②摘映射 → ③flush_frame，窗口 [摘映射, fsync 完成] 内 split_copy 拿到 PageNotFound 提前放行 → 掉电可致 redo Commit downlink 指向空右页（索引静默丢 key）。修复 `evict_frame` 改为**先 flush_frame 后摘映射**（`evicting` 已拒新 pin，映射留着无碍）；flush 失败时清 `evicting` 保留映射传播错误——**顺带修复 Stage N 遗留的"flush 失败帧永久泄漏"**（旧顺序下失败即丢映射、脏内容永久不可达）。与 H1 的 flush_done 握手组合后：split_copy 的 flush 要么找到映射并等待在途驱逐刷盘完成、要么在驱逐者刷盘完成后才见 PageNotFound，两条路径都耐久
   - **根分裂复用 freelist 回收页无 FPI → 恢复后根页损坏**（高，第三轮）：回收页磁盘上是前任内容（`pd_upper != 0`），redo 的 `init_if_fresh` 失效；修复 `create_new_root` 补 `log_page_init`（与 `create` 同模式）
   - **分裂点按条数不按字节 + 不感知待插 entry → PageFull 楔死**（高，第三轮）：新增 `choose_split_slot` 按 PG `_bt_findsplitloc` 思路把 pending entry 字节纳入切点约束（含存在性论证）；父页 downlink 路径同理；side-choice 统一 `entry_cmp` 全序
   - **(key,child) tie 的页号单调假设被 freelist 复用打破**（中，第三轮）：right-ownership 命中时先查父页 downlink 存在性，有则耦合右跳（写楔死降级为多一跳）；insert 叶满一律升级悲观；validate 仅在分隔键相等时容忍乱序
   - **validate 用 handle 缓存 root → 静态树误报**（中，第三轮）：抽 `root_from_meta` 只读函数，open/refresh/validate 共用
   - **undo 重插继承 insert 虚假失败面**（中，第三轮）：`insert_with_budget` 参数化预算，undo 走独立大预算（1<<20），失败日志升 error
   - **split Commit 的周期 FPI 落在 Commit 记录之后 → FPI redo 回滚已提交 split**（P0，Stage T 压测发现）：`split_commit` / `split_commit_guarded` 原先**先 append Commit 记录、后 pin_mut 修改页**；checkpoint 在 Copy→Commit 间开启新 FPI 周期时，pin_mut 触发的周期 FPI 位于 Commit 之后却含提交前镜像（SPLIT_INCOMPLETE 未清）——违反"FPI 内容必须包含所有 LSN < FPI 位的修改"不变式。FPI redo 无条件整页恢复并把 pd_lsn 补过 Commit LSN，Commit redo 的 pd_lsn 守卫跳过清标志/插 downlink → undo H3 页扫描误判已提交 split 为未完成，发伪造 CLR 向父页重复插 downlink。修复：Commit 在固定记录 WAL 位置**之前**用 scoped `pin_mut` 预触其修改的每页（parent→left 降序）打出到期 FPI，append 后改用 `BufferPool::pin_mut_without_fpi` 重取做 apply（被否决的替代方案"持 left latch 跨 append/父页修改直到 apply"会与乐观路径 right-hop/coupling 编排死锁，btree_concurrent 2/2 挂死实证）。复核追加关闭第三方残余：乐观/悲观写路径（`pin_leaf_for_write`/`descend_write_path`）改 `pin_mut_without_fpi` 取锁，且对 SPLIT_INCOMPLETE 页**整段持有期跳过 `ensure_fpi`**——窗口内写按 Stage S 设计放行但绝不发 FPI（无 FPI 即无 stale 镜像；`btree_undo_clr` 两个 in-window 测试为此契约）。初版"flagged 且 FPI 到期才升级"的混合方案因 check-then-emit TOCTOU（判定读 checkpoint_lsn 与 ensure_fpi 重读之间可插入新周期）被复核否决，且 `ensure_fpi` 先补发再升级为时已晚。guarded 根分支对 new_root 的 apply 原先仍是裸 `pin_mut`（fresh 假设在 checkpoint 于 create_new_root→apply 窗口刷过新根时失效，stale FPI 会静默丢弃 slot-1 downlink 且 undo 无法修复），已补 new_root 预触 + `pin_mut_without_fpi` apply。确定性回归 `btree_split_crash.rs::test_btree_split_commit_fpi_precedes_commit_record` / `test_third_party_write_on_committing_leaf_escalates_without_fpi` / `test_btree_split_commit_guarded_root_branch_new_root_fpi_order`（test-hooks 钩子驱动在线根提升交错；均修复前红、修复后绿）
   - 测试类：注入钩子改 thread-local 防并行消耗、loom 桩补调度点、loom 注释修正、TPS 归因对照臂、**m2c_index_concurrent E2E**（索引表 + 并发 DML + 随机 abort + split + 周期 checkpoint + validate/对拍）、并发 flush 单测、混合大小 key 楔死场景、freelist 乱序写路径、回收页根分裂崩溃恢复

8. **CI 与工程化**（设计终审后）
   - CI 修复：`--all-features` 会启用 loom 致全部非模型测试 panic（提交必红）——pg-storage/pg-am-btree test 步骤改默认 features；新增 loom job（`LOOM_MAX_PREEMPTIONS=2`）+ parking_lot grep 守卫
   - `SPLIT_ALLOC_FAILURES` 注入钩子 feature 门控（`test-hooks`，默认关闭，dev-dep 自引用供测试）
   - MSRV 1.86 + `--all-features` 编译 loom 0.7 本机实证通过

### 设计理由

**1. 为什么读路径必须改成真 crabbing（而不是沿用"先放父再拿子"？**
单线程下放父拿子无妨；并发下 parent split 可在窗口内插入，下降会走错子树。crabbing 的代价是父子 latch 短暂重叠（DOWN 序，无死锁面），换来每一层决策都在 latch 保护下。

**2. 为什么乐观写不做 latch upgrade？**
parking_lot 未暴露 upgrade；更根本的是 drop-and-re-pin 之后**无论如何都要重验证**（叶子可能已被 split）——upgrade 省下的只是锁转换，省不掉重验证，引入新 API 得不偿失。

**3. 为什么 pessimistic 全路径写 latch 而不是"安全节点"优化？**
spec（§13.2）就是从根全路径 X latch；split 是稀有路径（乐观路径承担绝大多数插入），串行化 split 换取协议推演的简单性。root 写 latch 同时天然串行化 root 提升，代际校验得以保持简单。

**4. 为什么 loom 层不别名 Arc？**
loom 的 `Arc` 在 stable 上无法做 `Arc<dyn Trait>` 协变，强行别名会级联到全部下游 crate；引用计数不是竞争面，排除它让 cfg 层收敛在 pg-storage 一个 crate 内。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL (nbtree) | pg_rust (Stage Q) | 取舍理由 |
|---|---|---|---|
| 读并发 | latch coupling（同） | 同（crabbing + Blink 右跳） | 一致 |
| 写路径 | 乐观叶写 + 悲观重走（同） | 同（悲观为**全路径** X latch；PG 有"安全节点"提前释放优化） | 简单优先；split 稀有，性能归 Phase 7b |
| 模型检查 | 无 | loom 2 模型 2 万+ 交错 | PG 无此实践；覆盖 loom 能力边界（多级级联 split 未覆盖，由压力测试兜底） |
| 页合并/压实 | vacuum 触发 merge | **不做**；1-entry 页死空间仅 split 可回收（M2b 既有边界，审查中实测踩到） | 归 M2c+/Stage S |
| 未完成 split 收尾 | 无 CLR（PG 靠 _bt_finish_split 在线收尾） | **不做**（SPLIT_INCOMPLETE 在线拒二次分裂；cascade 中途失败 = 子树写不可用） | 归 Stage S（CLR） |
| 并发 TPS | — | 15K 未达（auto-commit 6.6K，fsync 主导；对照臂 13.5K 证明 latch 非瓶颈） | 硬件 fsync 天花板；batch commit 归 Phase 7b |
| validate | amcheck（可在线，带锁等级） | **静止态检查**（并发写入期 SPLIT_INCOMPLETE 非腐败） | M2c 语义差异已文档化 |

### 已知残留与后续归队

- loom 模型未覆盖多级树的父页递归 split（状态空间限制；由线程压力测试覆盖）→ 需要时专项模型
- 1h soak 未实际执行（60s smoke 通过；命令已文档化 `BTREE_SOAK_SECS=3600`）
- **insert 左跳越界修复（Stage T 压测抓出）**：insert 下降/落叶的左跳曾不受限，可把新条目写过分隔键到左侧页——churn 负载下孪生页被抽干成"黑洞空页"，探测在空页终止返回假 EntryNotFound。修复：insert 专用下降（`descend_to_leaf_for_insert` / `position_for_insert`），左跳限制在同键 run 内；定位型操作（lookup/delete）保持全左跳。回归测试 `btree_insert_left_hop.rs`（红→绿）
- `btpo_prev` 链接陈旧（split 不更新 `old_next.prev`，prev 恒指最旧左页）：右链是 ground truth 所以无正确性影响，但"born-left 重复键 + 空孪生"极端组合下读路径可能漏条目 → 后续 stage 评估
- 叶页死空间永不回收（`remove_entry_at` 只缩 LP 数组）：churn 负载下叶页因死空间反复分裂（上述左跳 bug 的放大器）→ 压缩回收涉及 slotted-page/WAL 不变式，单独评估
- stale 内部分隔键间隙（内部最左子被 delete 抬高 + probe 落间隙）→ 预算耗尽响亮 Unsupported，不自愈 → Known limitation，根治归 Stage S（CLR/分隔键维护）
- validate 的盲区：同父页下相等分隔键的两个子树被对调时不报警（无代码路径能产生；查找经链 hop 自愈）→ 接受
- commit barrier 写 guard 覆盖整个 checkpoint（commit 停顿随 split 脏页增多拉长；文档已对齐，收窄归 Phase 7b）
- CLOG 全局单锁（命中也写锁 + 锁内 I/O）；allocation_lock 下等组提交 fsync → 均归 Phase 7b 性能项
- 1-entry 页死空间压实（page compaction）→ 排入 M2c+ 路线图
- 死锁检测（表锁 × 行锁 × 页 latch 三层等待）→ **Stage R**
- CI：已加 loom job（`LOOM_MAX_PREEMPTIONS=2`，模型 1 自钳披露）与 parking_lot grep 守卫；1h soak 不进 CI → 需要时 nightly
- `SPLIT_ALLOC_FAILURES` 注入钩子已 feature 门控（`test-hooks`，默认关闭，dev-dep 自引用供测试）

---

## Stage R：死锁检测

**状态**：✅ 完成（M2c，工作区测试全绿；未提交）
**工期**：预估 3–5 天
**验收**：`deadlock_detection` 11/11（2/3/4 事务环 + 行锁环 + 混合环 + 共享 victim 双环 + churn soak）；检测延迟实测 99–106ms（≤200ms）；tick p99 ≈ 204–229µs（≤5ms）；检测线程 busy/wall ≈ 0.05–0.07%（<1%）

### 交付内容

1. **DeadlockDetector（`pg-txn/deadlock.rs`）**
   - 后台线程 100ms tick（`EngineConfig.deadlock_detector_interval` 可配）：快照双源 → 迭代式三色 DFS 找环（确定性按 XID 排序）→ 环内 max XID 为最年轻 victim → **撕裂快照复核**（重读双源，环上每边仍在且 victim 仍 active）→ mark + 双 condvar 广播
   - tick 整体 `catch_unwind` + panic 计数（线程不死）；tick 耗时环形缓冲供性能验收；`stop()` 幂等、Drop 兜底 join、stop flag + notify 即时唤醒（shutdown 不等一个 tick）
   - **硬边策略**：行边 = `wait_edges()`；表边 = 等待者 → 所有模式冲突的持有者；FIFO 排队顺序不产生边（PG soft-edge 队列重排超出 M2c，模块文档记录了该盲区与"硬边足以发现纯冲突环"的论证）

2. **Victim 中断通道**（兑现 Stage P 的 TODO）
   - `DeadlockVictims`（`Mutex<HashSet<Xid>>` 共享注册表，**叶锁**：合法嵌套只有 registry→victims 与 entries→victims）
   - `wait_for`：每轮迭代在 registry 锁内**先查 victim flag**——命中则消费 flag、清自己的等待边、返回 `TxnError::DeadlockVictim`
   - `LockManager::acquire`：同构——命中则消费、摘出等待队列、`regrant_heads`（不授予与 victim 仍持有锁冲突的请求）+ 广播，返回 `LockError::DeadlockVictim`；已授予锁保留（2PL），由调用方 abort 路径 `release_all`
   - 标记幂等：`end_txn` 清 stale flag；tick 开头清理已结束 XID 的残留 flag（覆盖 mark-晚于-clear 竞态）
   - 错误管线：`TxnError::DeadlockVictim` → `HeapError::DeadlockVictim`("deadlock detected")→ EngineError；显式事务语义同 PG——当前语句失败，调用方必须 abort

3. **引擎接线**
   - `Engine::open`：创建共享 victims 注册表 → TxnManager/LockManager 各装一份 → 最后启动检测器；`shutdown` 先停检测器再停 storage
   - auto-commit 路径的 deadlock 错误走既有"语句失败 → index undo → abort → release_all"通用通道，零新机制

4. **Review 修复清单**（三轮对抗审查，未发现高危）
   - `end_txn` 锁序注释补 registry→victims 嵌套（文档与代码矛盾）
   - victim 消费路径与 `try_acquire` 的 `entry().or_default()` 空 LockEntry 泄漏（改 `get_mut`）
   - 检测器性能：`table_lock_states` 跳过无等待者的表（克隆成本从总锁数降到竞争锁数）；同一 tick 的 re-verify 复用单次快照
   - 性能测试抖动修复：CPU 预算断言改按生产 100ms interval 实测 40 tick（原 10ms 加速口径余量太薄）
   - engine 模块文档补"虚假 DeadlockVictim 可能性"说明
   - `interval=0` 忙循环（第三轮）：`start` 入口钳制到 1ms 下限 + 测试钉住
   - Torn-snapshot 文档扩写（第三轮）：复核快照自身亦撕裂（never-coexisted 混合环可通过复核），误标概率非零但语义安全
   - `start` 的 `Arc::ptr_eq` 防呆断言（设计终审建议）：三处共享同一 victims 注册表从文档承诺变开发期断言
   - 测试补强（第三轮）：共享 victim 双环（钉住 is_marked 跳过分支）、churn soak-lite（8 线程 3 秒，实测 477 次 victim abort、panic=0、终态全排空）

### 设计理由

**1. 为什么 victim 标记制而非检测器直接 abort？**
victim 通常正阻塞在 wait 里，第三方线程直接拆它的事务会与 victim 自身执行并发。标记 + 唤醒 + victim 自报错（PG 同款形态）让所有清理（index undo、CLOG、release_all）走已有的调用方 abort 路径，零新机制。

**2. 为什么周期 tick 而非 PG 的惰性触发（deadlock_timeout 后阻塞者自查）？**
我们的等待图是双源（row_wait_registry + LockManager），让被阻塞事务自己合并快照会污染 hot path；周期 tick 换取 ≤200ms 的检测延迟（agent 场景敏感），成本被验收约束在 CPU <1%（实测 0.07%）。

**3. 为什么复核后仍接受残余误杀？**
双源快照非原子：复核通过到 mark 落地之间环可能消散且 victim 已合法拿锁继续运行——其下一次 wait 会虚假失败。语义上安全（可重试错误）、概率极低、与 PG 在 deadlock_timeout 竞争下的可观察行为一致；用`end_txn` 清理 + tick 开头清理把残留窗口压到最小。

**4. 为什么 FIFO 排队不产生边？**
环只能由 hold-and-wait 构成；排在前面的等待者本身不持有锁，等待者→等待者的边不构成死锁。PG 的 soft-edge 队列重排会破坏我们的反饿死公平性承诺，明确不做。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage R) | 取舍理由 |
|---|---|---|---|
| 触发 | 惰性（`deadlock_timeout` 默认 1s，阻塞者自查） | **周期 tick（100ms 后台线程）** | 检测延迟 ≤200ms vs 1s 级；成本常开但被验收约束 |
| 等待图数据源 | 统一锁表（XID 虚拟锁入同一锁表），大锁下一致快照 | 双源（row_wait_registry + LockManager），非原子快照 + 复核 | 行锁等待不进锁管理器是 Stage P 的热路径取舍 |
| 边类型 | 硬边 + 软边，软环尝试队列重排 | **仅硬边** | 软环罕见；重排破坏 FIFO 反饿死承诺 |
| Victim 选择 | 触发检测的进程自己 | **环内最年轻（最大 XID)** | 保护老事务；代价是跨线程标记竞态（幂等 + 清理兜底） |
| 检测成本 | 无死锁时零成本 | 常开（实测 0.07% CPU，p99 204µs） | 快照只克隆受竞争的表 |

### 已知残留与后续归队

- 虚假 DeadlockVictim 残余窗口（复核→mark 间隙）→ 接受，语义安全，已文档化
- 软边环（纯 FIFO 排队构成）不检测 → 观察项。**核销（Stage T)**：死锁注入压测 1000 环（含无环对照组）全绿，未出现软环形态；维持不检测
- `lock_timeout` / NOWAIT / SKIP LOCKED 无（wait_for 无超时参数）→ Phase 6 协议层需求出现时评估"XID 虚拟锁统一进 LockManager"的重构
- pg-txn 无 tracing 依赖，tick panic 只计数不告警 → 可观测性归 Phase 7a
- SQL 层表达不出纯表锁环（显式事务内 DDL 被拒，AS/RE 互不冲突）→ engine 级表锁环测试用原始 XID 驱动，已注明

---

## Stage S：HOT update + ARIES Undo（B+Tree CLR）+ Multixact 简版

**状态**：✅ 完成（M2c，已提交 ddc1ae6；其后两轮评审修复均已落地：第一轮 C1/C2/H3，第二轮 H1/H2/H4/H5 + B1–B10，工作区测试全绿）
**工期**：预估 7–10 天
**验收**：`btree_undo_clr` 13/13（含修复轮的 9 个状态矩阵测试）+ `btree_split_crash` 5/5 + `hot_update` 5/5 + `hot_chain` 4/4 + `m2c_locks` 16/16 + `m2c_index_concurrent`（并发 DML 抓出 HOT 链递归读锁死锁，见设计理由 6）全绿；`m2b_crash_rounds` 25 轮（CI 口径）全绿，每轮携带 B+Tree leaf/root split、HOT/非 HOT update、`FOR SHARE`；`cargo test --workspace` 601 测试 + `cargo clippy --workspace --all-targets -- -D warnings` 全绿

### 交付内容

三条互相独立的轨道 + 一条把三者压进同一个崩溃流的集成验收。

1. **Multixact 简版（`FOR SHARE`）**
   - `HEAP_XMAX_IS_SHARE = 0x4000`（`t_infomask`），始终与 `HEAP_XMAX_LOCK_ONLY` 同时置位：共享锁与排他锁的区分不引入独立 multixact 段，完整 multixact 推迟 Phase 6
   - `lock_tuple_shared` 走与 `lock_tuple` 同构的 §9.1 restart 门；LOCK_ONLY 位在可见性判定中屏蔽 `t_xmax`，被共享锁定的行照常可见
   - **share/share 真正共存（第二轮修复 H5）**：堆 AM 内维护内存持有者注册表 `(page_id, slot) → XID 集合`。行锁本就无 WAL，注册表同为易失状态，崩溃后与锁章语义一同消失（死 XID 章经崩溃豁免视为 aborted），自洽。首位持有者盖 `t_xmax` 章，后续 FOR SHARE 只注册不覆盖章（`ProceedNoStamp`）；写者/FOR UPDATE 请求者逐一等待**全部**存活持有者（每次 restart 重估集合）；同事务 FOR SHARE → FOR UPDATE/写升级等待其余持有者；对已持排他锁的行再 FOR SHARE 不降级。注册表惰性清理（持有者事务结束不主动移除，下次过门时修剪）
   - SQL 侧 `SELECT ... FOR SHARE`；`m2c_locks::for_share_locks_row_and_stays_visible` 钉住，H5 语义由 `for_share_coexists_with_for_share` / `writer_waits_for_all_share_holders` / `for_share_upgrades_within_same_txn` / `for_share_upgrade_deadlock_detected` 钉住

2. **HOT update**
   - `HEAP_HOT_UPDATED`（旧版本）/ `HEAP_ONLY_TUPLE`（新版本）+ `t_ctid` 前向链，链不跨页
   - `hot_eligible` 判据：表上**所有**索引列取值不变（由调用方断言）；旧页是否有空位由 AM 自行判定（第二轮修复 B7 文档澄清）——有空位则同页追加新版本并跳过索引维护，空位不足则回退跨页非 HOT 路径
   - `HeapHotUpdate` WAL 记录（`page_id, old_slot, new_slot, new_tuple, xmax`）+ pd_lsn 守卫的幂等 redo（10x 重放回归测试，B1）
   - 可见性侧：`index_lookup` 与 scan 在旧版本不可见时沿 `t_ctid` 跟链**直到链尾**（第二轮修复 H1：原先硬编码 8 跳上限，9+ 次同页 HOT 更新的行会从 scan 和 index_lookup 同时消失——无 vacuum 系统链只增长。三处链跟随统一为 pg-am-heap 的两个共享 helper：前向 `follow_hot_chain`、反向 `hot_chain_root`，均以页内 slot 数为环保护上界，越界报 `Corrupted`；`hot_chain_root` 顺带把每跳全页扫描改为一次 `t_ctid→slot` 映射，B6）
   - **索引维护缺陷修复**：`HEAP_ONLY_TUPLE` 自身从未获得索引项，因此对它的改键 update / delete 必须退掉**链根**的索引项。原实现按后代 TID 删除，报 `EntryNotFound` 并让索引与堆失去一致；新增 `Engine::hot_chain_root`（页内反向遍历 line pointer 到首个非 `HEAP_ONLY_TUPLE` 版本，HOT 链不跨页所以搜索是页局部的），回归测试 `dml_on_a_hot_descendant_retires_the_chain_root_entry`
   - **CREATE INDEX bulk load 缺陷修复（第二轮修复 H2）**：bulk load 的 heap scan 产出的是可见版本——HOT 链尾——按链尾建项会让后续改键 update / delete 经 `hot_chain_root` 删除时永远 `EntryNotFound`。修复：建项前把可见版本映射到链根（create_index 持 Exclusive，无并发写者竞态）。回归测试 `create_index_over_hot_chains_then_delete_is_consistent`

3. **ARIES Undo + B+Tree CLR**
   - `UndoHandler` trait + `UndoContext`（`pg-storage/recovery.rs`）：handler 由 `pg-engine` 注入，因为 `pg-storage` 不能反向依赖 AM crate
   - `HeapUndoHandler`：堆无需逐条撤销（MVCC 天然屏蔽），唯一动作是把 ATT 每个 XID 在 CLOG 盖 `Aborted`——崩溃可能根本没写出 `TxnAbort`（深审修复 B3 文档化：该盖章无 WAL 记录、无显式 flush，首次 checkpoint 的 `ClogFlush` 钩子落盘前仅存在于内存；此前崩溃无害——下次恢复从 WAL 重推同一 ATT 并重盖，标记幂等，且缺 CLOG 项本就读作 `InProgress`（MVCC 不可见），与 Stage N"无显式堆 undo"决策一致）
   - `IncompleteSplitTracker`：redo 期间由 Prepare/Copy/Commit/CLR handler 维护（`mark_prepare` / `mark_copy` / `clear`），redo 结束后剩下的就是需要 undo 补齐的 split；`mark_prepare` 记录 Prepare 的 LSN，填入 CLR 的诊断字段 `redo_ref_lsn`（第二轮修复 B8）
   - `BTreeUndoHandler`：按 level 降序（叶先于父，深审修复 B4：同级 split 再以左页 id 为决胜键，CLR 发出顺序跨恢复字节确定）对每个未完成 split 调 `finish_incomplete_split`；只到 Prepare 的 split 用 `choose_split_slot_readonly` 重算中点。**H3（第一轮修复）**：undo handler 无条件运行并额外**扫描已分配页上的 `SPLIT_INCOMPLETE` 标志**——Prepare 早于 checkpoint 的 split 在重放窗口内不留记录，但标志已持久化在页上
   - `BTreeSplitCLR` 记录（判别式 50）把 Copy + downlink + 清 `SPLIT_INCOMPLETE` 合成一条幂等记录；字段序为 `left_page / right_page / level / copy_start_slot / redo_ref_lsn（诊断用）/ parent_page / parent_insert_slot / new_root_page / meta_page / separator_key`（深审修复 B5：`separator_key` 移至**末尾**——analysis 阶段只对定长前缀做前缀解码，bincode 标准配置无长度上限，CRC 通过的坏长度前缀会触发无界分配；Stage S 新增判别式、pre-release，布局变更无迁移负担。`BTreeSplitCLRRecord::decode` 另加 `MAX_CLR_SEPARATOR_KEY_BYTES`（≈2698 + trailer）上限作纵深防御）
   - **C1 收尾计划按当前页内容决定（第一轮修复）**：右页有 entry = Copy 已完成（NoMove，永不重复搬运，分隔键取右页首项）；右页从未写过 = 搬运 `left[copy_start_slot..]`（Move）；右页写过但已空 = Copy→Commit 窗口内右半被删光，**unlink** 放弃分裂（把空右页摘出兄弟链、只清标志，CLR 的 parent/new_root/meta 置 INVALID）
   - **C2 级联（第一轮修复）**：父页放不下 downlink 时 undo 先分裂父页——递归至根——每级由自己的 CLR 完成；恢复单线程，任意级联前缀在下一次恢复时重新收敛
   - **`apply_split_clr` 单一实现**：undo 路径与 `BTreeSplitClrRedoHandler` 调同一个函数，逐页 pd_lsn 守卫，收敛性是结构性的而非两份手写代码的巧合
   - **`finish_incomplete_split` 改为 log-then-apply**：只读收集 → append+flush CLR 拿到 LSN → apply → 按 right→(new_root/parent/meta)→left 的顺序刷盘。任何页字节都不在 CLR 的 LSN 存在之前被改动
   - **undo 期页修改的撕页防护（第二轮修复 H4）**：undo 阶段在 `checkpoint_lsn` 播种**之前**运行是刻意的——提前播种会让 `pin_mut` 在 undo 期间发 FPI，而 FPI 会把 pd_lsn 顶过已 append 的 CLR 的 LSN，使 apply 的逐页幂等守卫跳过 CLR 欠下的修改（实测 `btree_undo_clr` 两个 H3 测试即如此失败）。正确做法：`emit_and_apply_clr` 在 append CLR **之前**为每个将被修改的页显式 append 前像 `FullPageImage` 记录——FPI 重放无条件恢复前像（正是撕页修复语义）并把 pd_lsn 补到 FPI 自己的 LSN（仍低于 CLR），CLR 再干净地重放其上。为保证逐轮恢复收敛（FPI 重放会把页 pd_lsn 顶到 FPI 的 LSN），CLR 的 NoMove/unlink 分支现在也给右页盖 CLR 的 LSN

4. **集成崩溃验收（`m2b_crash_rounds`）**
   - `ixt(id INT, name TEXT)` + `name` 上的 B+Tree；索引键宽约 500B → 每叶仅约 15 项，一轮几十次插入即产生多次叶分裂与一次根分裂，kill 落点因此可能正处于 split 协议中途
   - 同一 op 流混入 HOT update（只改无索引的 `id`）、非 HOT update（改索引列 `name`）、`FOR SHARE`；每个 op 最多新增一行，以维持父进程 mid 模式 `extras <= 1` 的前缀持久性不变式
   - 每轮恢复后除既有的行内容比对外，追加：`validate()` 通过、每个已提交 `ixt` 行都能经索引查到、`ixt` 行数 > 30 时 `tree_level() >= 1`（否则该轮没有真正跑到 split 恢复，宁可响亮失败）

### 设计理由

**1. 为什么 undo 只补齐 split，不回滚堆元组？**
§11.3 的简化 undo：堆的可见性判据是 `CLOG[xmin]`，把 ATT 成员盖成 `Aborted` 即让其全部写入不可见，逐条物理回滚是纯浪费。B+Tree 不同——`SPLIT_INCOMPLETE` 的右兄弟已在 `btpo_next` 链上但没有 downlink，这是**结构**破损，不是可见性问题，必须补齐。

**2. 为什么补齐（redo-style）而不是回滚 split？**
Prepare 已经把左页标 `SPLIT_INCOMPLETE`、右页初始化完毕，Copy 可能已把上半 entry 搬走。往回退需要把 entry 搬回并释放右页；往前推只需补 downlink。PG 同样选择"下一个访问者补齐"，我们只是把补齐时机固定在恢复期。

**3. 为什么 undo 也必须 log-then-apply？**
先改页再写 CLR 会留下"左页已 rebuild、右页已收到 entry、WAL 里没有 CLR"的状态；下一次崩溃恢复的 undo 会把同一批 entry 再搬一次。这正是本 stage 修掉的真实缺陷（右页 452 项而非 226 项，scan 返回 678 行而非 452 行），根因有两处叠加：`apply_split_copy` 是**追加**语义而 CLR redo 无条件传 `move_to_right = true`；`finish_incomplete_split` 从未给右页盖 CLR 的 pd_lsn，于是重放时 Prepare 的右页初始化被跳过、CLR 又追加一遍。
**幂等不变式**：`move_to_right = right_lsn < clr_lsn`。左页已过 CLR 而右页没有 = 搬走的 entry 无处可寻，报 `Corrupted` 而不是静默丢数据。

**4. 为什么 HOT 的索引维护要找链根，而不是给后代补一个索引项？**
给后代补索引项就等于不做 HOT。PG 用页内 line pointer 重定向解决同一问题；我们的链不跨页，所以按 `t_ctid` 反向搜一页即可定位链根，且链上所有版本共享同一索引键（HOT 的前提就是索引列不变），链根的索引项携带的正是调用方读回的那个键。

**5. 为什么用宽索引键而不是批量插入来制造 split？**
父进程的 mid 模式要求每个 op 最多多出一行，批量插入会直接破坏这条不变式。把键 padding 到 500B 让每叶只装约 15 项，于是单行插入也能密集触发分裂。

**6. 为什么 HOT 链遍历必须复用已 pin 的页，而不能再 pin 一次？**
`parking_lot::RwLock::read()` 不可重入：同一线程持读锁期间再取读锁，只要中间有写者排队就自锁，而它还攥着外层读锁不放，整个 buffer pool 随之雪崩。首版 HOT 链遍历（`HeapAM::scan` 与 `Engine::heap_tuple_visible`）对 `chain_tid.page_id` 重新 `pin`，在 `m2c_index_concurrent` 的 6 写者 + checkpoint 并发下必然死锁（实测卡满 600s 看门狗，同一测试在 Stage R 基线只需 6.1s）。修法是直接读外层已 pin 的页——`HEAP_HOT_UPDATED` 只由同页快路径 `stamp_hot_update` 盖，**HOT 链永不跨页**，因此复用是语义使然而非权宜；`t_ctid` 若指向别页即为损坏，直接终止遍历。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage S) | 取舍理由 |
|---|---|---|---|
| 共享行锁 | multixact 段（多持有者共存） | **`t_infomask` 单 bit（`HEAP_XMAX_IS_SHARE`）+ 堆 AM 内存持有者注册表（H5）** | 锁本就无 WAL，注册表同为易失状态、崩溃即消失，语义自洽；完整 multixact（持久段、成员溢出页内）推迟 Phase 6 |
| HOT 旧版本 | line pointer 转 REDIRECT（由 prune 回收） | **保留旧元组，靠 `t_ctid` 跟链**（LP REDIRECT 在本系统**没有生产者**——spec 偏差，B4 已注明；redirect 跟随分支加 slot 数上界防坏页栈溢出） | 无 vacuum/prune，页内死空间不回收 |
| HOT 索引项回收 | vacuum 回收，update 期间不删索引项 | **改键 update / delete 即时删链根索引项** | 无 vacuum，只能即时维护；代价是需要 `hot_chain_root` 页内搜索 |
| 未完成 split | 下一个访问该页的读者/写者补齐 | **恢复期 undo 阶段补齐 + CLR** | 在线路径不必处理"别人的半成品"；代价是恢复多一个阶段 |
| Undo 范围 | 逐条物理 undo（含 CLR 链） | **仅 CLOG 盖 Aborted + 结构补齐** | MVCC 屏蔽让堆 undo 变成空操作 |

### 已知残留与后续归队

- `BTreeSplitCLRRecord.redo_ref_lsn` 仅诊断用；redo 窗口内见到的 Prepare 其 LSN 已记入 `IncompleteSplit` 并填入 CLR（B8），仅 H3 页扫描发现的 split（Prepare 早于重放起点）保持 `Lsn::INVALID`；CLR 循环保护实际由逐页 pd_lsn 守卫承担 → 若将来出现嵌套 CLR，需要真正记录参考 LSN
- ~~HOT 链深度上限硬编码为 8；超长链的尾部版本在 `index_lookup` 中判不可见（scan 不受影响）~~ **已修复（H1）**：原说法有误——8 跳上限同时影响 scan 与 `index_lookup`（9+ 次同页 HOT 更新的行从两者一起消失）。现在两处前向跟随统一走 `follow_hot_chain`，跟到链尾、以页内 slot 数为环保护上界；回归测试 `hot_chain_deeper_than_8_hops_visible_via_scan_and_index`（engine 级 scan + index_lookup）与 `test_hot_update_chain_20_followed_to_end`（heap 级）
- 无 page prune / vacuum：HOT 链把页填满后即退化为跨页非 HOT update → M2c+ 路线图
- undo 阶段的两处全页扫描（`scan_split_incomplete_pages` O(allocated_pages)、`find_parent_page` O(N × allocated_pages)）：恢复单线程且罕见、只读 pin，阶段内可接受；大库恢复 I/O 可闻时再优化（P2-1/P2-2）
- share 持有者注册表（H5）条目在真实 delete/update 盖戳时由 gate 的 `note_stamp_overwrite` **即时摘除**（非惰性）；惰性修剪只覆盖崩溃持有者残留（死 XID 章视为 aborted 兜底）——盖章在摘除后失败会在两者间留一个无注册项的锁章，仅延迟等待、无正确性影响（P2-4 实测澄清）
- `m2b_crash_rounds` 默认 25 轮（CI 口径），plan 的 1000 轮口径需手工跑 → Stage T 的崩溃自动化承接
- ~~Multixact 简版记不住共享锁持有者集合，因此不支持"多个事务同时持共享锁后其中之一升级"~~ **已修复（H5）**：堆 AM 内存持有者注册表支持 share/share 共存与升级等待；仍归 Phase 6 的是完整 multixact（持久段、成员集合溢出的页外存储、崩溃后仍精确的持有者恢复——当前注册表崩溃即弃，靠"死 XID 章视为 aborted"兜底，与无 WAL 锁章的设计一致）
- **✅ C2 级联路径专项深审（2026-08-28,Phase 2 前置清偿）**：`finish_incomplete_split` / `ensure_downlink_slot` / `split_page_in_undo` / `apply_split_clr` 全路径逐行审计，**未发现正确性缺陷**。此前未独立验证的关键声明逐条复核成立：① `choose_split_slot` 返回值域限定 `(1..count)`（index.rs:3144），级联右页首项读取不存在越界面；② 落侧选择 `entry_cmp(.., false)` 走内部页 `(key, child)` 全序，与在线级联同规则；③ level 降序 + 左页 id 决胜键的排序使级联永不撞上仍带 `SPLIT_INCOMPLETE` 标志的页（`split_page_in_undo` 的响亮报错确为不可达防御）；④ 互为递归深度以 4-bit level 上界（`ensure_root_promotion_fits` 双调用点齐备）；⑤ 级联每级独立 CLR + 前像 FPI + 完整刷盘，崩溃于级联中途的再收敛（重放已完成 CLR 为幂等空转、原始 split 重推导落点）逻辑闭合；⑥ redo 与 undo 共享 `apply_split_clr` 且 CLR redo 清 tracker，与 H3 页扫描的标志判据互不矛盾。测试覆盖映射（`btree_undo_clr` 13/13 绿，57s）：三种收尾计划（Move/NoMove/Unlink）、C1 窗口内插入落左/落右、窗口内删除、级联父满/级联中途崩溃注入/多级级联、H3 扫描双形态——无覆盖缺口。两条非阻塞观察：**(a)** 级联产生的孤儿页（unlink 的空右页、崩溃轮次间重复分配的右孪/新根保留页）按轮泄漏数页，与"既定泄漏"哲学一致但未在泄漏清单中显式列名，已补记于此；**(b)** `choose_split_slot` 的 `PageFull` 兜底在 undo 期意味着恢复失败（库打不开）——键长上界论证其不可达，但它是"恢复响亮失败"面，若将来放宽 `MAX_INDEX_KEY_BYTES` 需重估

---

## Stage T：100 并发压测 + Benchmark + M2c 综合验证（M2c 出口）

**状态**：✅ 完成（release 全量 621 测试全绿；M2c 出口）
**工期**：预估 5–7 天
**验收**：见下"验收终账"；C 类全过，P 类按 §20 下调并归因落盘（`docs/phase1-m2-benchmarks.md`）

### 定位

验证型 stage：无新机制，交付压测/基准/文档，并用它们把前五个 stage（P/Q/R/S + 存储底座）第一次全部放在一起长时间压。**它抓出 5 个此前全绿测试发现不了的真 bug**——这是本 stage 的核心价值。

### 交付内容

1. **压测设施**（全部带 watchdog，回归=失败而非挂起）
   - `m2c_stress.rs`：N 连接混合负载（INSERT/HOT UPDATE/DELETE/点查 + 显式事务 + 后台 checkpoint），sleep 配速；终态堆↔簿记逐行比对 + 索引 validate + 泄漏三连查；env 可调（CI 30s/16conn）
   - `m2c_deadlock_stress.rs`：随机 2–4 事务环（Barrier 保证闭合）+ 无环对照组；断言恰好一个最年轻 victim、有界延迟、零误报、零泄漏
   - `m2b_crash_rounds.rs` 并发子进程变体：多线程写（含索引表/HOT/持续分裂）+ 后台 checkpoint，随机点 kill -9，逐线程前缀持久性 + 恢复一致性校验
2. **Benchmark 集合**：验收命令 5 个 bench 全部就位 + 补 `heap_mixed`、`btree_split`；`docs/phase1-m2-benchmarks.md` 落盘（每项 target + 实测 + 未达标归因 + 长跑实际执行记录）
3. **压测抓出并修复的 5 个 bug**（全部带红→绿确定性回归测试）
   - **checkpoint/FPI 竞态（P0）**：split Commit 先 append 后 pin_mut，新 FPI 周期在窗口开启 → FPI 落在 Commit 之后但含提交前镜像 → 恢复复活未完成 split。修复：Commit 修改页全部 pre-touch（FPI 先于 Commit 记录落 WAL）+ apply 走 `pin_mut_without_fpi`；第三方写者路径 flagged 页整段跳过 FPI（TOCTOU 安全）
   - **恢复侧 loser 索引补偿缺失（P0，预存）**：kill 落在索引维护与 commit 之间 → 可见行永久丢索引项（在线 abort 有 undo 日志，恢复路径无对应物）。修复：`compensate_loser_index_entries`（从 WAL 收集 loser 受害 tid，按堆页字节重算 (key, chain_root)，幂等重插 + CRC resync 回卷扫描起点）
   - **WAL 撕裂尾部误判 corrupted（高）**：预分配段的零填充使撕裂尾"看起来完整"但 CRC 失败 → 重开硬失败。修复：`is_torn_tail` 双重判定（header LSN == 位置 + 记录之后全零）；已知局限（payload_len bit-rot 放大场景）文档化，根治（header 自 CRC）归后续
   - **insert 左跳越界（高）**：新条目可写过分隔键，churn 把孪生页抽干成"黑洞空页" → 假 EntryNotFound。修复：insert 限同键 run 内左跳
   - **并发重复 run 跨边界乱序（高，release 50% 复现）**：陈旧 prev 链 + 边界落位乐观检查竞态 → (key,tid) 链序倒置且 validate 的相等-separator 豁免放过了它。修复：`nearest_nonempty_left`（prev 降级为提示、向右走 ground-truth 链找真左邻）+ slot-0 落位在左邻写闩内原子化 + 乐观/悲观两路径同一判定 + validate 新增全链 `last < first` 严格序检查

### 验收终账

| 验收项 | 结果 |
|---|---|
| 全量回归 | ✅ debug 618 + release 621 全绿（含 loom) |
| 1000 轮崩溃（含 split-in-progress） | ✅ 3043s |
| 并发崩溃 × 激进 checkpoint | ✅ 40 轮连跑全绿（修复后） |
| 保底压测 50conn×100tps×30min | ✅ 1802.7s，终态一致、零泄漏 |
| 挑战压测 100conn×60min | ⚠️ 未执行（命令已文档化） |
| 死锁注入 1000 环 | ✅ 每环恰好 1 victim、对照组 0 误报、延迟 ≤78ms |
| 性能 P 类 | 按 §20 下调：WAL 18.3MB/s、并发 INSERT 6.6K（均 fsync 封顶，对照臂证明非 latch/锁竞争）；读路径全部超标（点查 1.05M QPS、BP 随机读 2.1M ops/s、CLOG 命中 98.4%/100%） |
| 回归传承（M1/M2a/M2b） | ✅ 含在全量中，无弱化（抽查确认 Stage S/T 对既有测试只有增强） |

### 已知残留与后续归队

- 挑战档长跑与并发 crash 1000 轮未执行（时间成本；命令在 benchmark 文档）→ 需要时手动
- torn-tail 的 header 无自 CRC（payload_len 被 bit-rot 放大的残余误截窗口）→ 结构性修复归后续（PG xl_crc 模式）
- `btpo_prev` 陈旧链（prev 恒指最旧左页）已从"纯文档"升级为被 `nearest_nonempty_left` 实际依赖提示——残余窗口（左侧全空时的 TOCTOU）有 validate 全链检查兜底 → 后续 stage 评估 prev 维护
- 叶页死空间不回收（churn 分裂放大器）→ 压实/合并归 M2c+/vacuum(M3)
- **已知 flaky（M3 Stage A 期间发现，预存）**：`btree_concurrent` 套件在 release 下存在间歇性失败。观测记录：早期实测 15 跑出现 **5 个不同测试** flaky（`concurrent_disjoint_inserts_all_found` 丢 key 0 占 2/15，疑 root split 路径丢失更新）；边界落位修复（乱序治理）后复测 15 跑仅 `concurrent_duplicate_keys_lookup_all` 失败 1 次——说明多数 flaky 与该修复同源，但**仍有残余**。干净 HEAD worktree 复跑可复现，零依赖 pg-txn，非 Stage A 回归 → 单开修复会话处理（重点：剩余重复键并发路径），未阻塞 Stage A 收口
- 所有性能 P 类未达标项的根治（batch commit、O_DIRECT、io_uring）→ Phase 7b

---

## Stage A（M3）：快照注册表 + horizon

**状态**：✅ 完成（debug 全量 636 绿 + clippy/loom-check 全绿；S2 性能验收通过）
**工期**：预估 3–4 天
**验收**：六路径注册覆盖 / 泄漏自由 / horizon 并发与原子性专项 / 100 线程 churn 全部带 watchdog 落地；**性能（S2 协议，5×300s 取均值）：87.2 txn/s vs M2c 基线 86.8（+0.5%，噪声界 ±3% 内，无统计显著回归）——注册开销不可测，R2 通过**

### 交付内容

1. **注册与构造原子化（B1，tech-selection §3.3 v1.3）**
   - `TxnManager` 新增 `TxnShared { active, snapshot_xmins: BTreeMap<TxnId, usize> }`（xmin → 引用计数），active set 与 registry 合并进**同一把锁**（`crates/pg-txn/src/manager.rs`）；`snapshot()` 在读取 active set + XID clock 的**同一临界区**完成 xmin 注册，签名改为 `snapshot(current_xid) -> (Snapshot, SnapshotGuard)`，guard `Drop` 减计数、归零删键
   - caller-wrapper（先构造后注册）形态按 B1 否决，反例（U xid=15 未注册 → vacuum 取空 registry horizon → 删除者提交 → U 必见行被回收）写入 `snapshot()` 的 doc
2. **反枚举护栏（v1.4 口径）**
   - `Snapshot` 五个字段收为 `pub(crate)`；外部读取走访问器（`xmin()/xmax()/xip()/current_xid()/curcid()`），engine.rs 约 21 处与 heap_am.rs 17 处字段访问随迁
   - `Snapshot::everything()` 保持 pub，是明确的**不注册**特例（目录引导 `Engine::open` + 测试路径）；doc 明示 xmin=0 注册即 horizon 钉死
   - pg-txn 集成测试的全字段构造走 `#[doc(hidden)] Snapshot::new_unregistered`（crate 内实现，CI grep 不拦）；测试/bench 的 writer 场景改造用 `set_current_xid`/`set_curcid`
   - 计数断言：`TxnManager::live_registered_snapshots()`（AtomicUsize 镜像）== registry 引用计数和（静息点断言，`snapshot_registry.rs` / `m3_snapshot_coverage.rs`）
   - CI grep（loom job）：`Snapshot[[:space:]]*\{` 字面构造（豁免"整行即 `-> Snapshot {` 签名"）与 `impl Snapshot` 块均禁止出现在 `crates/pg-txn` 之外；模式只用 POSIX 字符类（`\b`/`\s` 在 BSD grep 下静默失效——设计终审发现原护栏从未真正生效，已修复并做正反注入验证：现状绿、注入 crate 外字面构造被拦）
3. **六调用点适配**：`begin_txn`（guard 随 `TxnHandle`，commit/abort/Drop 注销）、`auto_commit`（`_snap_guard` 绑到帧尾，成功与失败两路径都注销）、`create_index` re-snapshot（`_re_guard` 随外层闭包）、纯 SELECT / `Engine::scan` / `Engine::index_lookup`（guard 随调用帧）
4. **horizon API**：`TxnManager::oldest_snapshot_xmin()`（registry 最小键；registry 空则取 **active set 最小 XID**——覆盖 begin→snapshot 窗口的结构性修正（review P2，PG OldestXmin 同构：backend xid 与快照 xmin 都参与水位）；二者皆空才取 `txn_id_clock.current()`）+ `Engine::oldest_snapshot_xmin()` 透传
5. **panic 语义（O1 决定：不加护栏）**：默认 unwind 策略下 panic **会**执行 guard 的 `Drop`，快照正常注销、horizon 不受影响；仅 `panic=abort`/`mem::forget` 跳过 Drop → horizon 永久偏低（vacuum 退化为不回收，安全但失效），与 `auto_commit` 既有 panic 策略代价一致。写入 `SnapshotGuard` 与 `auto_commit` 的 doc（review F1 修正：初版误写为"panic 跳过 Drop"，与 unwind 实际行为相反）
6. **测试**：`pg-txn/tests/snapshot_registry.rs`（注册/注销/引用计数、空 registry→clock horizon、horizon=min key、everything() 不注册、并发注册注销无漂移、**B1 原子性专项**——horizon 永不越过任一已返回未销毁快照的 xmin）；`pg-engine/tests/m3_snapshot_coverage.rs`（六路径注册覆盖、auto_commit 失败路径与 TxnHandle::Drop 泄漏自由、**auto-commit DML 快照先于锁等待注册**（v1.4 路径）、100 线程 churn 归零）；全部并发用例带 watchdog

### 与 PG 的 trade-off

| 维度 | PG | 本实现 | 取舍 |
|---|---|---|---|
| 快照登记 | 每后端 ProcArray 自有槽位，无共享锁 | 全局单 mutex 的 `BTreeMap` registry | 临界区变长是新共享热点（§11 R2）；S2 判定无统计显著回归则维持，超标备选分片 registry（xmin 聚合仍取全局 min） |
| horizon | OldestXmin 综合 backend xid / KnownAssignedXids 等多源 | registry 最小键；空取 XID clock 当前值 | 单源语义更简单；安全论证靠 XID 单调性（§3.3 v1.2 重写版），不允许改回"集合包含"式 |
| panic 路径 | 后端退出即清理 proc 条目 | unwind 会执行 guard Drop 正常注销；仅 panic=abort/forget 使 horizon 永久偏低 | 决定（§11 O1）：与 auto_commit 既有 panic 泄漏 XID/锁的进程级失败策略代价一致，不加护栏（review F1 修正了初版的错误描述） |

### 已知残留与后续归队

- registry 全局单锁在高并发短查询下是共享热点（§11 R2）→ **S2 已判定通过**（87.2 vs 86.8，噪声界内；数字见 `docs/phase1-m2-benchmarks.md` 基线条目与本文"验收"行）；分片 registry 备选保留
- panic 泄漏 horizon 无护栏（已决定的语义）→ 若未来改进程级 panic 策略需重估
- `#[doc(hidden)] Snapshot::new_unregistered` / `set_current_xid` / `set_curcid` 是全字段构造/改写逃生口（供 pg-txn 集成测试与 AM 测试）；CI grep 只拦 crate 外的构造，crate 内新增关联构造函数靠 review 把关
- horizon 自省目前仅 API（`oldest_snapshot_xmin()`）；§6.2 的指标暴露归 Stage E（可观测性）
- vacuum 本体（取 horizon 一次、全程使用）归 Stage C/D

---

## Stage B（M3）：HeapCleanup WAL + redo + §4.6 槽位寻址重构

**状态**：✅ 完成（debug 全量 651 绿 / 1 已知 flaky（首次全量）；修复轮后复跑 654 绿 / 0 失败，含 F1–F5 新增用例；clippy 全绿；loom 用例不受影响——pg-am-heap/pg-storage 本无 loom feature，仅 pg-am-btree 的 loom 用例经 `cargo check --features loom` 验证不受影响；未 commit——等用户确认）
**工期**：预估 5–6 天
**验收**：payload roundtrip / `first_fit_slot`·`add_tuple_at` 单测 / `add_tuple` 组合行为等价（双页逐字节对比）/ `test_heap_cleanup_redo_converges`（在线 vs 重放整页 8192B 逐字节一致）/ `test_heap_cleanup_redo_idempotent`（同记录重放 10× 幂等，含链 unlink 变体）/ `test_slot_reuse_after_compact_redo_*` 四路径（insert / HOT / 同页非 HOT / 跨页落中部压实页）全部崩溃重放按 WAL 承载 slot 复现、无 diverged / analysis DPT 分类（压实页 + unlink 前驱页，不含重链目标页）全部落地。**回归**：m2b crash rounds 4/4 绿（R4 门槛在 §4.6 重构后、compact 落地前先行验证过一次，收口时复验）；首次全量 651 passed / 1 failed（失败项为已登记 flaky，详见下），**修复轮后终审复跑 654 passed / 0 failed**——失败项为 stage_spec 已登记的 `btree_concurrent` 预存 flaky（`concurrent_hundred_thread_smoke` 丢 key 0，与登记的 root-split 丢失更新疑似同源；本 stage 未触碰 pg-am-btree，pg-storage 改动纯增量；该测试单独重跑 5 次 4 过 1 挂，flaky 特征吻合）。**性能**：criterion 基对比不可用（`target/criterion` 基线来自更早 stage、机器又在并发跑全量回归：e2e -78% / no_fsync +26% 均为噪声，不可解释）；结构性论证——重构为纯重排，WAL 字节量与 I/O 不变，`first_fit_slot` 的 LP 扫描与被拆掉的 `add_tuple` 内部 first-fit 扫描同构，insert 路径净工作量不变

### 交付内容

1. **§4.6 槽位寻址重构（R4 前置，行为中性）**
   - `SlottedPage::first_fit_slot(page) -> Option<u16>`（纯读，corrupt header 下钳制 `pd_lower` 防越界）+ `add_tuple_at(page, slot, bytes)`（`slot == slot_count` 追加新 LP；`slot < slot_count` 仅允许回收 `Unused`；其余 `InvalidSlot` 硬错）（`crates/pg-am-heap/src/slotted_page.rs`）；`add_tuple` 退化为二者组合，对外行为不变（`slot_addressing.rs` 双页逐字节等价测试钉死）
   - 四处在线路径全部改为"先 `first_fit_slot`（或 `slot_count`）选 slot → 写 WAL（slot 随记录承载）→ `add_tuple_at` 落位"：insert、HOT update、同页非 HOT update、跨页 update（`heap_am.rs`；跨页路径 slot 选择保持在最终双 latch 落定之后，反向扫描使压实后中部页成为 update 落点）
   - 三个 redo handler 全部改调 `add_tuple_at(page, rec.slot, ..)`：HeapInsert、HeapUpdate（同页 + 跨页两分支）、HeapHotUpdate（`redo.rs`）；"预测—断言"耦合删除，slot 不可用即 `MetadataCorrupted` 硬失败（语义与旧 diverged 检查一致）
   - **WAL 布局核查结论**：`HeapInsertRecord.slot_id`、`HeapUpdateRecord.new_tid`、`HeapHotUpdateRecord.new_slot` 本就已承载 slot——**无磁盘格式变更**
2. **`HeapCleanup = 8` payload**（`pg-storage/src/wal/record.rs`）：`(page_id, unlink_prev_page, unlink_next_page, dead_slots[])`；判别式 8 即 Stage-0 预留值，未新增占号。字段顺序不变量沿用 CLR B5 先例：三个定长 page id 在前、`dead_slots` 定 LAST——analysis 只前缀解码定长字段，不信任变长 Vec 的长度前缀；`MAX_HEAP_CLEANUP_SLOTS = (PAGE_SIZE-32-16)/4` 防御性上界随 decode 强制。unlink 两字段取 `PageId::INVALID` 哨兵（Stage B 在线侧只产压实形态，unlink 字段为 Stage C 页回收预留）
3. **`SlottedPage::compact()`**（`slotted_page.rs`）：kill list 先校验后落笔（严格升序 / 越界 / 非 Normal·Dead 即硬错）；存活元组字节拷出→数据区清零→按 slot 升序向 `pd_special` 重排→LP 原位改指（flags/len 保留）；dead slot LP 置 `Unused`（`delete_tuple` 语义）；`pd_lower` 不动、`pd_upper` 吸收全部空洞。**LP 条目不移动不重排**（§4.1 阶段 4，slot 号是 TID 组成部分）
4. **`HeapCleanupRedoHandler`**（`pg-am-heap/src/redo.rs`）：压实页与前驱页各自独立 `pd_lsn` 幂等守卫（两页落盘时机可不同，同跨页 HeapUpdate 策略）→ 调**同一个** `compact()`（同参数、升序 dead_slots，§4.5 重放收敛="重放=重执行同一物理操作"）；unlink 分支 `set_next_page(prev, unlink_next_page)`。注册进 `heap_redo_handlers()`（Stage 0 硬失败约定：记录与 handler 同 stage 交付）；`Engine::open` 经该函数自动获得
5. **analysis DPT 分类**（`pg-storage/src/analysis.rs`）：`for_each_touched_page` 新增 `HeapCleanup` 臂——压实页 + unlink 前驱页（`INVALID` 过滤），重链目标页本身不被修改故不入 DPT；`every_record_type_is_classified_for_the_dpt` 护栏同步迁移
6. **测试**：`pg-am-heap/tests/slot_addressing.rs`（7）；`pg-storage/tests/heap_cleanup_wal.rs`（3：roundtrip / 构造器+decode 双层拒绝 / DPT 分类端到端走 `run_analysis`）；`pg-am-heap/tests/heap_cleanup_redo.rs`（8：converges / idempotent×10 / 四路径 slot 复用 / checkpoint-介于-之间 FPI 顺序收敛 / 非升序 kill list 恢复硬失败）
7. **对抗性 review 修复（F1–F5）**
   - **F1（中）在线 compact 模板 FPI 顺序倒置**：测试 helper 原为 append HeapCleanup → pin_mut；当压实是 checkpoint 周期内对该页首次触碰时，`pin_mut` 发的压实前 FPI 会获得比 HeapCleanup 更大的 LSN，恢复端 FPI 无条件回滚把压实静默撤销、后续按 slot 落位的 redo 撞 Normal 槽 `MetadataCorrupted` 硬失败（目录变砖）。修为 **pin_mut → append → compact → stamp pd_lsn**；补 `test_compact_after_checkpoint_replay_converges`（checkpoint 介于 seed 与 compact 之间），红绿验证成立：旧顺序下恢复以 `MetadataCorrupted("heap redo: invalid slot 1")` 硬失败
   - **F2（低-中）`set_next_page` 改返回 `Result`**（`slotted_page.rs`，与 `next_page` 对称，`checked_header` + `pd_special` 几何校验硬错）：redo 的 unlink 分支作用于磁盘恢复页（不受信来源），原 debug_assert 在 release 下可被损坏页触发越界 panic 或写进元组区；在线调用点（`heap_am.rs::extend_chain`）与测试一并迁移
   - **F3（低）`WalRecord::heap_cleanup` 构造器硬校验**：非升序 / 超 `MAX_HEAP_CLEANUP_SLOTS` 直接 `Err`（原仅 debug_assert；WAL-first 协议下毒记录先落盘、之后每次恢复硬失败变砖）；decode 上界保留为第二层防御
   - **F4（低）`AccessMethod::redo_handlers()` 死代码地雷**：一行委托 `heap_redo_handlers()`（原返回 3 个旧 handler，缺 HotUpdate/Cleanup）
   - **F5 测试补强**：F1 配套 checkpoint 用例 + 负面向（字节手术构造非升序 dead_slots 的 CRC 合法记录 → 恢复硬失败、非 panic、非静默，错误信息点名 ascending 契约）；`compact()` 文档新增 kill-list 契约——调用方（Stage C vacuum）保证不杀仍被 `t_ctid` 引用的 HOT 链成员，否则留下指向 `Unused`（会被回收复用）槽位的悬挂链；**第三轮审查扩写（N1）**：契约还须覆盖链根——"链上仍有存活成员时不得杀链根"（链根无 `t_ctid` 指向它、指向它的是索引条目；杀根 = 存活成员从 seqscan 与索引扫描双双不可达 + 槽回收后索引返回错行）。链活性判定归 `scan_dead_tuples` horizon + 链分组（tech-selection §4.4），当前无在线生产者触发路径，Stage C 照抄前已修订

### 与 PG 的 trade-off

| 维度 | PG | 本实现 | 取舍 |
|---|---|---|---|
| slot 分配 | `PageAddItem` 返回 offset number，调用方立即持锁写 WAL | 在线先选 slot 再写 WAL，slot 由记录显式承载 | redo 不依赖在线 writer 行为巧合；compact 产生 Unused 后必然分歧的旧"预测—断言"形态被 §4.6 禁掉 |
| 页内压实 | `compactify_tuples` 重排 LP 数组（itemid 按 offset 排序，靠 redirect 保 TID） | LP 数组一律不动，只移 tuple 字节 | 无需 LP_REDIRECT 机制，索引条目与 HOT t_ctid 天然稳定；代价是压实不回收 LP 本身（与 PG 一致） |
| 压实 WAL | PG 不单独记 vacuum WAL（HEAP2_CLEAN 记录含 redirect/死亡信息） | 单条 `HeapCleanup` 物理记录 + 同一 `compact()` 重放 | 重放收敛靠"同函数同参数"，无平行重放逻辑可漂移 |

### 已知残留与后续归队

- `HeapCleanup` 在线侧尚无生产者：Stage B 的"在线 compact"由测试直接驱动（模板为 **pin_mut（可能发该页本周期的 FPI，必须先于 HeapCleanup 落 WAL，否则恢复端 FPI 无条件回滚会把压实静默撤销、后续按 slot 落位的 redo 撞占用槽硬失败——review F1 修复）→ append HeapCleanup → compact() → stamp pd_lsn**，Stage C 必须照抄此顺序）；vacuum 本体（horizon → scan_dead → 索引清理 → reclaim 调 compact + unlink）归 Stage C/D
- 空页识别与 unlink 记录的在线产生（含 `unlink_prev/next` 填充）归 Stage C 页释放阶段；redo 侧本 stage 已支持并有幂等测试
- **Stage C 前向提醒（第三轮审查）**：在线 unlink 摘除页必须与 heap AM 的 `pages` 内存缓存剔除**同步**——否则 `acquire_page_with_room` 可能把新行插入已摘除页（物理 redo 仍收敛，但行逻辑不可达）；另：redo 的 unlink 分支不清被摘除页自身的 next 指针（"前驱旧镜像"状态下链读作 prev→P→X，P 为空页，walk 正常终止，无断言触发——此形态合法）
- criterion INSERT 基线对比本次不可用（基线陈旧 + 并发负载）→ 如需 S2 式正式验收，在干净机器上重存 Stage A 基线后复测
- `btree_concurrent` 预存 flaky 依旧（本次全量撞上 `concurrent_hundred_thread_smoke` 一例，单测重跑 4/5 过）→ 沿用 Stage A 残留条目，单开修复会话处理
- ~~`AccessMethod::redo_handlers()` 只返回三个旧 handler~~ → **review F4 已修**：一行委托 `heap_redo_handlers()`，单一事实源（该 trait 方法仍无调用点，但不再是落后两份的地雷）

---

## Stage C（M3）：Vacuum 核心（trait / 链分组 / 压实接线 / 页释放）

**状态**：✅ 完成（debug 全量 669 绿 / 0 失败（= Stage B 出口 654 + 本 stage 新增 15，含 R1/R2 回归）；clippy `--workspace --all-targets -D warnings` 全绿；loom 不受影响——pg-am-heap/pg-storage 本无 loom feature，`cargo check -p pg-am-btree --features loom --tests` 验证通过；rustdoc 无新增警告（8 个 pre-existing 私项链接警告与 HEAD 持平）；未 commit——等用户确认）
**工期**：预估 5–7 天
**验收**：`vacuum_chain_grouping`（5：普通死元组自身映射含 NULL 列 / 全死 HOT 链只回链根且为链根列值 / 部分死链零输出 / 全死页逐条输出 / aborted 插入按规则 1 分组）；`vacuum_reclaim`（7：slot 置 Unused + 空洞回收 + first-fit 复用 / 空页 unlink→freelist→缓存剔除→复用无串扰 / 空链头只压实不摘除 / 部分死链零回收 / **相邻双空页连续摘除回归**（红绿验证：naive 左邻实现下链头 next 指向已释放页，测试红）/ **页 id 序 ≠ 链序的泛化回归**（LIFO freelist 复用拼出链序 [1,4,3,2,5]，链相邻双空页按页 id 序处理，前驱解析仍按链序取最近仍在链左邻，红绿对照成立）/ 读者-压实并发 watchdog 用例——每次读要么全旧要么全新，绝无半压实页）；`vacuum_crash_windows`（3：**崩溃窗口②**——HeapCleanup 落盘、PageFree 未写 → 恢复后被摘页脱链且不在 freelist（既定单页泄漏，`free_page` 补 free 成功证明其不在 freelist）+ 链遍历与堆扫描一致 + `scan_dead_tuples` 为空；**多页压实中途崩溃收敛**——在线 reclaim（两页压实 + 一页 unlink+free）崩溃后重放，两存活页 8192B 逐字节一致、链重链 A→C 复现、被释放页回 freelist；**R2 回归**——checkpoint → reclaim 含 unlink → flush → 崩溃 → 重放收敛，前驱页 unlink 前镜像 FPI 必须先于 HeapCleanup，旧顺序下必红）

### 交付内容

1. **compact 模板提升（Task 0，单一事实源）**：`HeapAM::compact_page(page_id, dead_slots, unlink_prev, unlink_next)`（`crates/pg-am-heap/src/heap_am.rs`）——**pin_mut（本周期的压实前 FPI 必须先于 HeapCleanup 落 WAL，review F1）→ append HeapCleanup → 同一 `SlottedPage::compact()` → stamp pd_lsn**；unlink 时**前驱页的 pin_mut 提到 append 之前**（其 unlink 前镜像 FPI 同样必须先于记录，见 R2），随后在前驱页自有 latch 下 `set_next_page` + 同 LSN stamp（两页落盘时机可不同，redo 侧各自 pd_lsn 幂等守卫，同跨页 HeapUpdate 策略）。Stage B 测试 helper `online_compact` 已迁移为薄委托（`heap_cleanup_redo.rs`），FPI 顺序只存在于这一个地方
2. **`Vacuumable` trait 扩展**（`access_method.rs`，§4.4 形状）：`collect_index_keys`（只读）+ `reclaim`（纯物理）**两个方法，拒不合并**——融合形态会把索引清理挤到压实之后，违反 §4.1 顺序不变量；`scan_dead_tuples` 不动；`notify_indexes` 不进 trait（索引知识在 engine 层）
3. **链分组 helper**（`HeapAM::classify_dead_tuples`，单页单 pin）：按页分组输入 → 每 slot 读一次 header → `HEAP_ONLY_TUPLE` 成员经 `hot_chain_root` 反走定位链根 → 链根沿 `t_ctid` 前走收全链（slot_count 有界、越界/脱页即终止、耗尽即 Corrupted，与 `follow_hot_chain` 同契约）。**死性判定不重做**——`dead` 集合成员身份即逐成员裁决（`scan_dead_tuples` 已套用过 horizon 规则）；helper 只做结构判定：全链成员 ∈ dead set 才算全死链。产出 `DeadClassification { index_keys, kills }`，`collect_index_keys` 与 `reclaim` 消费**同一份**结果，只读侧与物理侧结构上不可能分歧
4. **`collect_index_keys` 实现**：普通死元组 → (自身 tid, 自身列值)；全死链 → (链根 tid, 链根列值)；部分死链一律不返回。列值经 `decode_tuple(bytes, rel.columns)` 全列解码（`Engine::delete_inner` 同路径的 AM 侧形态）；NULL 列保持 `None`，跳过发生在 engine 编码 key 处（既有约定）。全程只读 pin，必须先于 reclaim（压实后 key 读不出）
5. **`reclaim` 实现**：kills 按页（BTreeMap 稳定序）逐页 `compact_page`；压实后全空页（kill 数 == 非 Unused slot 数）且非链头 → 前驱取链序缓存左邻 → **unlink（进同条 HeapCleanup）→ 缓存剔除 → `free_page`（PageFree + freelist），顺序不可逆**：先 free 后 unlink 的崩溃窗口会留下"页同时在链上与 freelist"，页被复用后链上出现活元组 = 结构性损坏；现顺序最坏只是单页泄漏（窗口②口径）。**链头永不摘除/释放**（`RelationDesc::first_page` 是目录与 `seed_from_chain` 的锚），空链头只压实。所有页修改记录 `txn_id = INVALID`（vacuum 非事务，`WalRecord::heap_cleanup` / `page_free` 既有语义，重放不依赖任何事务结局、analysis 不进 ATT）
6. **缓存剔除（Stage B 前向提醒的落实）**：`HeapAM::evict_page` 按单页粒度从 `pages` 缓存移除已摘页（先于 `free_page`）；测试直接验证——摘除后的小插入经反向扫描**绝不**落进已摘页（若缓存残留，尾部空页必被选中）
7. **页分配器接线**：`HeapAM` 新增 `page_allocator: Option<Arc<pg_storage::sync::Mutex<PageAllocator>>>` + `set_page_allocator`（`set_row_waiter` 同款 install-once-before-sharing 形态；锁类型走 `pg_storage::sync` 别名层，crate 边界规则）；`Engine::open` 装配（engine.rs 5c）
8. **设计偏移（对 §4.4"reclaim 不需要链知识"的一处有意收紧）**：`reclaim` 内部用**同一个**分组 helper 重新推导 kill 集，而非盲信输入清单——engine（Stage D）会把 `scan_dead_tuples` 的原始输出直接传给 `reclaim`，其中含部分死链的死亡成员；不过滤则违反 `compact()` kill-list 契约（杀仍被 `t_ctid` 引用的成员 = 悬挂链）。过滤使"部分死链零回收"成为结构保证而非调用方自律；`vacuum_reclaim.rs::reclaim_never_touches_partially_dead_chains` 钉死
9. **对抗性 review 修复（R1，中）相邻空页连续摘除的前驱选择**：初版取链序快照的直接左邻为前驱——A→B→C 三页中 B、C 同轮皆空时，C 的 unlink 记录把**已摘除的 B** 的 next 改写为 None，而活链 A.next 仍指向随后被 free 的 C（allocator 复用后链上出现他人活元组 = 结构性损坏）。修为：摘除前驱 = 最近的**仍在链上**的左邻（`removed` 集合跳过本轮已摘页；链头永不摘除，扫描必终止）；回归测试 `consecutive_empty_pages_unlink_to_live_predecessor` 红绿验证成立（naive 实现下 A.next 残留指向已释放页，测试红）
10. **对抗性 review 修复（R2，高）unlink 前驱页的 FPI 顺序**：初版 `pin_mut(unlink_prev_page)` 排在 append HeapCleanup **之后**——前驱页是本 checkpoint 周期首次触碰的冷页时（vacuum 回收冷页是典型工况），其 unlink 前镜像 FPI 获得比 HeapCleanup 更大的 LSN；崩溃重放时 FPI 无条件覆盖、把前驱页回滚到"仍指向被摘页"，而 PageFree 照常重放把被摘页放上 freelist → 复用后链指向他人活页 = 结构性损坏。修为：unlink 时前驱页 pin_mut **先于** append（两 latch 同持无 AB/BA——vacuum 持表级 AccessExclusive 无并发写者、redo 单线程，已文档化）；确定性回归 `unlink_prev_fpi_precedes_heap_cleanup_across_checkpoint`（checkpoint → reclaim 含 unlink → flush → 崩溃 → 重放收敛）红绿验证成立（旧顺序下恢复后页 A 回滚到 unlink 前镜像、与在线字节不符，测试红）
11. **文档修正（review 低优先级两条）**：`Vacuumable::reclaim` trait doc 补**互斥前提**——`scan_dead_tuples`/`collect_index_keys`/`reclaim` 之间不得有并发写（Stage D 的 AccessExclusive 保证；无锁使用是调用方责任，并发插入会在流水线中途回收的 Unused slot 上复用，使 kill 清单与已解码 key 失配）；`reclaim` 内 allocator 前置检查的注释改写为真实理由（缺 allocator 是可前置判定的配置错误，保持错误原子性——压实+摘除后半 applied 的释放无法被重试干净恢复：killed slot 已 Unused，重分类找不到 kill 对象、永不再进 unlink/free 分支）

### 与 PG 的 trade-off

| 维度 | PG | 本实现 | 取舍 |
|---|---|---|---|
| HOT 死链回收 | 部分死链 LP_REDIRECT 重定向 + 整链回收 | 只做整链回收；部分死链原样保留 | LP 重定向是 on-disk 格式变更（页格式已冻结，变更须带 migration），归 Phase 7；代价是部分死链空间滞留到整链死亡（§4.2 已论证有界） |
| 空页释放 | `lazy_vacuum` 摘页还空间给 relation 内复用 | unlink → 缓存剔除 → `free_page` 还 allocator（可跨 relation 复用） | 两笔 WAL 间有崩溃窗口②（单页泄漏，既定取舍）；消除窗口需把 unlink 并进 PageFree payload = on-disk 变更，判定不值 |
| vacuum 事务性 | vacuum 在非事务的专用机制下运行，页修改 WAL 同样非事务 | 记录 `txn_id = INVALID`，redo 无条件重放 | 语义一致；analysis 不进 ATT，无 undo 负担 |
| 死元组清单 | `LVDeadTuples` 物化 + 内存上限触发多轮 | `Vec<Tid>` 全量物化（离线模式） | 在线化时改迭代器（access_method.rs TODO 维持，归 Phase 5b） |

### 已知残留与后续归队

- **崩溃窗口② 单页泄漏**（既定取舍）：unlink 与 free 两笔 WAL 之间崩溃 → 页脱链且不在 freelist，永久泄漏直至手工干预；`vacuum_crash_windows.rs` 钉死精确口径。备选已记录：空页留链零窗口；消窗需改 PageFree payload（on-disk 变更，不值）。**注**：review F1（记为 R2）曾在此窗口之外另发现"前驱页 FPI 排在 HeapCleanup 之后"的重放回滚通道（链指向已释放页 = 结构性损坏），已修复并有确定性回归（交付内容第 10 条）；现窗口② 的残留口径仅为单页泄漏，无一致性问题
- **部分死链空间滞留**（§4.2 代价）：死版本滞留到整链死亡才回收，`follow_hot_chain` 遍历成本同滞留 → 归 LP 重定向 + 格式演进（Phase 7）
- **reclaim 缺 allocator 即报错**：未 `set_page_allocator` 的 `HeapAM`（纯 AM 测试构造）遇空页释放时 `InvalidArgument` 硬错——压实部分不受影响；engine 装配路径已接线
- **索引清理 / `Engine::vacuum` 五阶段流水线**归 Stage D：`collect_index_keys` 产出的 (tid, 列值) 需 engine 侧逐索引 `delete`（EntryNotFound→Ok），本 stage 只交付 AM 能力
- **WAL 流量观测**：vacuum 产生与死行数成正比的 WAL（§4.5 代价）+ FPI 放大 → ~~量化归 Stage G benchmark（N5）~~ **已在 Stage G 落盘**（`docs/phase1-m3-benchmarks.md` N5 节：最坏档 FPI 占 64.1%、总 WAL 2.05× 于对照档）

---


## Stage D（M3）：索引清理 + `Engine::vacuum` 端到端

**状态**：✅ 完成（debug 全量 74 目标 684 绿 / 0 失败（= Stage C 出口 669 + 本 stage 新增 15，含 1 个 btree 回归用例）；clippy `--workspace --all-targets -D warnings` 全绿；loom 两模型红为**预存**（M2 出口提交 31fe4b8 同红，二分证据见残留条），与本 stage 改动无关；未 commit——等用户确认）
**工期**：预估 4–6 天
**性能（S2 协议，`M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100` release × 2）**：89 / 87 txn/s，均值 88.0 vs M2c 基线 86.8（+1.4%，噪声界 ±3% 内，无统计显著回归）——vacuum 叠加 Stage A 注册开销后度量通过
**验收**：`m3_vacuum_e2e`（11：空表 / 无死行表 / 纯死行表（页数 4→1、freelist +3、再插入零新页分配）/ HOT 链（全死链整链回收 + 部分死链零回收）/ 多索引表精确清理量（10 死行 × k 索引 + 6 非 NULL × v 索引 = 16 次 EntryNotFound 容忍，NULL 跳过）/ TableNotFound / **失败路径锁释放**（drop 先排队、vacuum 拿到已死 OID 的锁后 re-check 失败，无锁残留）/ **pin 住 horizon 的端到端防线**（显式事务注册快照钉住 horizon → vacuum 零回收；unpin 后再 vacuum 才回收）/ **阻塞顺序**（holder AS → vacuum AE → inserter RE 的 FIFO 队列，纯 SELECT 与新 BEGIN 期间照跑，无死锁，watchdog）/ **并发纯 SELECT 一致性**（4 读者 × 3 次 back-to-back vacuum，每次扫描恰好 600 行活行，绝无半压实页或复活行）/ **阶段④中途失败注入**（双索引表 + 各一条悬挂 loser 条目；腐蚀第二索引 meta 页使 open 失败——第一索引的删除已持久化、堆未动、维护 XID 出 active set、零锁残留；修复后下一轮 vacuum 以 EntryNotFound 容忍收尾并实际删除第二索引的悬挂条目，窗口①语义的非崩溃版））；`m3_vacuum_crash_windows`（2：**崩溃窗口①**——阶段④索引清理 WAL 落盘、HeapCleanup 未写 → 恢复后索引扫描与堆扫描一致、悬挂 TID 不解析到错误行、下一轮 vacuum 以 EntryNotFound 容忍收尾并 reclaim；**崩溃 loser INSERT 悬挂条目**——恢复后裸 `lookup_all` 见条目、`index_lookup` 被可见性屏蔽、vacuum 实际删除（`index_entries_removed == 1`）后裸探测为空）；`m3_vacuum_churn`（1：固定行数表 30 轮 UPDATE/DELETE/INSERT 批 + 每 5 轮 vacuum + 2 个崩溃注入轮（窗口①手驱 + 在飞 INSERT 批 loser 条目由恢复后 vacuum 实际清除），堆页数收敛（后半程 ≤ 前半程峰值 +3 且 ≤30 页）、每次 vacuum 后无可回收垃圾、freelist/压实复用（稳态高水位增长按**率**判定：≤0.25 页/轮 + 8 页绝对余量，随 soak 长度缩放——review 修复，原 ≤8 页绝对阈值按 30 轮标定、200 轮 soak 必败；实测漂移源为已声明的 btree 无页合并 ~0.18 页/轮）、终态扫描与簿记逐行相等；watchdog 保护；**200 轮 release soak 实测通过**（`M3_CHURN_ROUNDS=200`，8.4s；界 = 8 + 100/4 = 33 页，review 实测漂移 18 页 ≈ 0.18 页/轮在界内））。**churn 抓出并修复 1 个 M2 潜伏 bug**（见交付内容第 3 条，红→绿验证成立）

### 交付内容

1. **`Engine::vacuum(table) -> Result<VacuumStats>`**（`crates/pg-engine/src/engine.rs:1832`）：§4.1 五阶段流水线——① `auto_commit` 风格维护 XID 下取 `AccessExclusive` 表锁（`lock_table_entry` 含 post-lock registry re-check；`create_table`/`drop_table` 同款：分配 XID → 进 active set → 成功与失败两条路径都走 `release_all(xid)`），随后经 `oldest_snapshot_xmin()` **取一次 horizon 全程使用**（锁等待结束后取，锁持有者已全部退出；在飞无锁读者由快照注册表覆盖；XID 单调性保证未来快照 xmin ≥ horizon）→ ② `scan_dead_tuples(horizon)` → ③ `collect_index_keys`（只读）→ ④ 推模式索引清理：每 (tid, 列值) × 每注册索引 → 取索引列（NULL 跳过）→ `encode_key` → `BTreeIndex::delete(key, tid)`，**`EntryNotFound` 视为 Ok 且仅在此调用点**（§4.3：eager 维护早已删除常规死行条目，真实删除对象基本只有崩溃 loser 悬挂条目；btree delete 本身契约未动）；vacuum **不记 index undo**（物理维护操作，幂等，绝不被 reverse-apply）→ ⑤ `reclaim`（压实 + unlink + free，kill 集内部重推导，调用方不过滤）。**不死锁论证落地**：等待期间入度恒 0（零持有），获准后不再申请第二把表锁（阶段 2–5 只持短暂页 latch）。`VacuumStats`（engine.rs:377）：`dead_tuples / index_keys / index_entries_removed / index_entries_already_gone` 四计数，`lib.rs` 导出
2. **并发共存正确性**：vacuum 进行中纯 auto-commit SELECT / `Engine::scan` / 新 BEGIN 不被锁挡（它们本就不取表锁）；horizon 防线（Stage A 注册）保证其快照看不到被回收版本——pin/unpin 用例端到端钉死；显式事务 SELECT/DML 按 FIFO 有序阻塞至 vacuum 结束（死锁检测器对 vacuum 等待零误报——入度 0 不可能成环）；`table_lock_state` 断言 vacuum 后零锁残留
3. **churn 抓出的 M2 潜伏 bug（高，红→绿验证）——split-copy redo 的回收页 FPI 回归误报腐败**：churn 第二轮崩溃注入的恢复硬失败 `split copy redo: left page is past the copy ... but right page still lacks it; the moved entries are lost`。根因链：vacuum 是**首个 freelist 生产者** → split 右页可以是带前任身份磁盘历史的回收页，且其前任身份在重放窗口内留有**旧 FPI**；重放时旧 FPI 无条件整页恢复，把右页池内镜像**回退到其磁盘镜像之后**（前滚只重建到 Copy 记录处），而左页（窗口内无 FPI）直接载入了"已过 copy"的最新磁盘镜像——Copy redo 的不对称守卫把"左已过、右池内缺"误判为腐败。该守卫的"不可达"论证只覆盖**磁盘**状态（在线 flush 纪律：右页 post-copy 先于左页 post-copy 落盘），不覆盖被 FPI 回归的**池内**状态。修复：`BTreeSplitCopyHandler` 该分支改为 `BufferPool::force_reload_from_disk(right)`（pg-storage 新增，重做专用：池内镜像回退为盘上镜像、清 dirty 防回归镜像被回刷、逐字段对齐 `alloc_frame` 的 load-from-disk 约定）采纳耐久的 post-copy 盘上镜像并校验 `pd_lsn ≥ copy LSN`；盘上镜像也缺才维持硬失败（真腐败）。**为何 1000 轮 m2b crash rounds 没抓到**：无 vacuum 时新页全部来自高水位线、无前任身份 FPI；且 m2b 轮次持续 checkpoint，重放窗口短。确定性回归 `btree_split_crash.rs::split_copy_redo_right_page_regressed_by_stale_fpi`（构造：C1 checkpoint → 左叶生于 C1 后（窗口内无 FPI）→ P 的前任身份 FPI + free → split 复用 P 为右页 → 全量 flush → 崩溃 → 恢复；含二次崩溃幂等重放），红绿验证成立（回退修复后恢复硬失败、恢复修复后全绿）
4. **过时注释清理**：`heap_tuple_visible` 的"HeapAM never reclaims slots"假设改写为 vacuum 时代的不变式（slot 复用只经 reclaim，且 §4.1 顺序保证指向它的索引条目先删后放；窗口① 崩溃后由下一轮 vacuum 在复用前补删）
5. **Stage D review 修复清单**（终审后）：
   - churn 稳态高水位断言从固定 ≤8 页改为率式（≤0.25 页/轮 + 8 页余量，随轮数缩放；200 轮 soak 实测后半程涨 18 页 = 0.18 页/轮，为已声明的 btree 漂移，非堆泄漏）
   - watchdog 的 worker panic 消息改为先 downcast `&str`/`String` 再携带（原 `Any { .. }` 丢断言信息；`m3_vacuum_churn.rs` 与 `m3_vacuum_e2e.rs` 两处）
   - 新增阶段④中途失败注入用例（见验收行）
   - `BufferPool::force_reload_from_disk` 的注释出处从 `flush_frame` 更正为 `alloc_frame` 的 load-from-disk 路径（逐字段对齐对象）

### 与 PG 的 trade-off

| 维度 | PG | 本实现 | 取舍 |
|---|---|---|---|
| vacuum 锁 | `ShareUpdateExclusive`（在线，不挡 DML） | `AccessExclusive`（离线停写窗口） | §2 既定代价；在线/渐进 vacuum 归 Phase 5b |
| 锁载体 | 专用 VACUUM 伪事务 | auto-commit 风格维护 XID + `release_all` | 复用既有 DDL 先例，零新机制 |
| 索引清理 | `lazy_vacuum_index` 批量回调 | 推模式逐条 `BTreeIndex::delete`，EntryNotFound→Ok | §4.3：eager 维护下绝大多数条目早已不在树上；批量接口归 Phase 5b |
| 无锁读者与页回收 | PG 读者也取 AccessShare，被 AccessExclusive 挡住 | 纯 SELECT 无锁放行，靠快照注册表 horizon 防线 | tech-selection §2 既定：水位线问题由 Stage A 解决，非锁问题 |
| horizon | GetSnapshotData 时算 OldestXmin | 锁授予后取一次全程使用 | §3.3 单调性论证；vacuum 自身快照注册使 horizon 略保守（等待期 xmin 被钉住），安全方向 |

### 已知残留与后续归队

- **loom 模型测试预存红（非本 stage 回归，二分定位 + 已实证复核）**：`btree_loom` 两模型在当前工具链下失败（`loom_two_writers_one_reader_linearizable` 报 "page 2 entries out of order at slot 1"、`loom_split_with_concurrent_writers` 报 "key 0 lost across the split"）。二分证据：在 M2 出口提交 31fe4b8（Stage Q 记录"loom 2 模型 2 万+ 交错全绿"的同一提交）上**同样失败** → 失败前移至 M2 之后的环境/工具链漂移（Stage A–C 只 `cargo check` 未实际运行 loom），与本 stage 改动无关（本 stage 未触碰 latch/split 编排）。**实证复核（2026-08-24)**：当前树与 31fe4b8 的隔离 worktree 各跑 CI 命令（`LOOM_MAX_PREEMPTIONS=2`），两侧同红且耗时一致（~22s/侧，非状态空间爆炸，是快速硬失败）。**并档（2026-08-25)**：`btree_concurrent::concurrent_small_pool_split_eviction_storm` 的 solo flaky（实测 23 跑 2 败，~9%）失败信息 "scanner missed committed key 0" 与 loom 的 "key 0 lost across the split" **同族**——疑同一底层 bug（并发 split 丢 key 0），两个条目合并为同一单开修复会话的排查对象，不再按"容忍的 flaky"处理。根因定位与修复单开会话处理，不阻塞 Stage D
- **split-CLR redo 的同构不对称窗口**（本次未触发，理论残留）：`apply_split_clr` 的"左页已过 CLR 而右页没有"分支同样假设池内状态即磁盘状态；触发需"回收右页 + 窗口内旧 FPI + split 中途崩溃 + 恢复后再崩溃"四连，比 Copy 路径窄得多。修复模式相同（`force_reload_from_disk`），归后续 stage 按需处理
- **回收页的撕页暴露**：`new_page` 对复用页仍设 `needs_fpi=false`（"新页无旧镜像"假设对复用页不成立——其前任镜像在盘上）；Stage Q 只给 `create_new_root` 补了 `log_page_init`。kill -9 测试模型撕不了页缓存 pwrite，纯断电模型才暴露 → **不在本阶段处理，列入后续 checkpoint/FPI 加固专项的处理清单**（新建该 stage 时作为其输入项；候选修复：split_prepare 对复用右页补 log_page_init）
- **无锁读者撞上跨 relation 页复用的理论窗口**：纯 SELECT 走链时若读到"unlink 前旧 next 指针 → 页已释放并被他表复用"，MVCC 过滤（新元组 xmin 晚于读者快照）使其读不到错行，最坏是一次可重试的解码错误；本 stage 测试未构造出该交错 → 观察项，根治（读者侧 chain 代际校验）归 Phase 5b 在线 vacuum
- **churn 验收口径偏差**（对 coding-plan 任务表）：计划写"vacuum 后 `scan_dead_tuples(最新 horizon)` 返回空"——HOT 更新负载下部分死链的死前缀**合法滞留**（§4.2 不 prune），该字面口径不可达；精确化为"无可回收垃圾"（`collect_index_keys(scan_dead_tuples)` 为空）+ 滞留量有界（≤ 活行数），已写入 churn 测试注释
- **窗口① churn 轮次的阶段④ WAL 为空**：churn 的已提交删/改条目都被 eager 维护先行删除，手驱阶段④全部 EntryNotFound；"索引清理 WAL 有实质内容"的窗口① 变体由 `m3_vacuum_crash_windows.rs`（loser 条目）覆盖
- **性能口径**：vacuum 叠加 A 注册开销后的 churn TPS 对比见 `docs/phase1-m2-benchmarks.md` 基线条目与本 stage 验收行（S2 协议）
- **WAL 流量观测**（压实 FPI 放大，N5）~~归 Stage G benchmark~~ **已在 Stage G 落盘**（`docs/phase1-m3-benchmarks.md` N5 节）
- **(FPI, record) 间 checkpoint begin 微窗口**（2026-08-31 M4 Stage A 对抗审查登记）：checkpoint begin 落在 `log_page_init`（FPI）与 Prepare 两条相邻 append 之间时，redo 从 begin LSN 开始会跳过该 FPI——全系统所有 `ensure_fpi` 调用对同型存在（非 A1 修复引入），A1 已把暴露面收窄到纳秒级窗口；候选根治：redo 点 = min(begin_lsn, DPT 最小 rec_lsn)，或 FPI+owning record 对 checkpoint_lsn 发布原子化。随 Phase 7a 加固专项一并处理（ROADMAP 附录 A2）
- **`force_reload_from_disk` 信任撕裂盘镜像的 pd_lsn**（`crates/pg-am-btree/src/redo.rs:254-258`,Stage D 遗留，2026-08-31 M4 Stage A 对抗审查登记）:Copy redo 的 anchor-mismatch 分支以 `disk_lsn >= record.lsn` 放行盘上镜像，但撕裂页的 pd_lsn 本身不可信；根治需页校验和或 Copy 后对右页补 post-image FPI。随 Phase 7a 加固专项一并处理（ROADMAP 附录 A2）

---

## Stage E（M3）：可观测性（pg-waldump / Engine 自省 API / QueryStats / pg-diag）

**状态**：✅ 完成（debug 全量 709 绿 / 0 失败（= Stage D 出口 684 + 本 stage 新增 25：交付物测试 18 + 终审修复 F1–F4 测试 7；含 m2a/m2b crash rounds 与 crash_recovery 三个子进程崩溃 harness 回归）；clippy `--workspace --all-targets -D warnings` 全绿；fmt 全绿；loom 未实际运行（沿用 Stage D 登记的预存红条目）；未 commit——等用户确认）
**工期**：预估 3–4 天
**验收**：`pg-storage/tests/waldump.rs`（3：全记录族可解析（heap/btree/txn/checkpoint/HeapCleanup/reserved 逐条一行、关键字段解码、reserved 打印原始 payload 不报错）/ LSN 过滤边界精确包含 / 跨段读取 + 数据目录与段目录两种入参形态）；`pg-engine/tests/m3_introspection.rs`（4：表锁等待 + 行锁边 fixture → `wait_edges` 精确合成 / CLOG 与 BufferPool 已知命中序列计数精确、Engine 命中率与组件计数一致 / 并发快照钉住 `oldest_snapshot_xmin`、提交后水位按注册存活快照推进）；`pg-engine/tests/m3_query_stats.rs`（5：容量溢出丢最老 / exec 单点覆盖 auto-commit 与显式事务两路径（含失败语句 rows=0、parse 失败不入列）/ typed API 不产生条目 / capacity=0 关闭统计）；`pg-engine/tests/m3_diag_cli.rs`（2：活跃事务 + 锁等待 fixture 下 `txn`/`locks` 报告文本逐项断言 / 编译产物二进制双子命令冒烟（空引擎实例 = M3 单进程边界语义））

### 交付内容

1. **`pg-waldump`**（`crates/pg-storage/src/bin/pg-waldump.rs`，§6.1 选型 (a)：与 WAL 格式同 crate、零新依赖边）：人类可读 dump，每条记录一行 `lsn= prev= xid= type=Name(N) len= <关键字段>`；`--start-lsn/--end-lsn` 闭区间过滤（十进制或 `0x` 十六进制）；位置参数同时接受数据目录（自动取 `wal/` 子目录）与段目录；`--segment-size` 须与写入端 `wal_segment_size` 一致（默认 16 MiB）。遍历复用恢复同款 `WalReader`（段间跳转、撕裂尾部=干净 EOF 全部继承）；**错误策略与 recovery 相反**（§6.1"尽量多展示"）：reserved 类型（`SegmentSeal`=110/`SegmentMerge`=111 及 Phase-2+ 逻辑索引类型 100–103、`TxnBegin`=20）打印原始 payload 十六进制不报错；payload 解码失败仍打印头部字段 + 原始字节；只有"无法越过"的记录（未知判别式 = 更新版本二进制所写、中段 CRC 失败）才停在该 LSN 并报告——之前记录已全部输出
2. **Engine 自省 API**（`engine.rs`，§6.2 全部只读拼装、零新增机制）：`active_xids()` → `TxnManager::active_xids`；`wait_edges()` → **`pg_txn::deadlock::wait_for_edges` 提为 pub 并导出**（row 边 `TxnManager::wait_edges` + 表边 `LockManager::table_lock_states` 的合成为单一事实源——诊断面与死锁检测器消费同一份图，结构上不可能漂移）；`table_lock_state(oid)` → `LockManager::table_lock_state`；`oldest_snapshot_xmin()`（Stage A 透传已存在，本 stage 进自省面）；`clog_hit_rate()` → `ClogBuffer::hit_rate`；`buffer_pool_hit_rate()` → 下方新增计数器
3. **Buffer Pool 计数器**（§6.2 唯一实现缺口）：`BufferPool` 补 `hits/misses: AtomicU64` + `hits()/misses()/hit_rate()`（`buffer_pool.rs`），与 `clog_buffer.rs:156-175` 逐行同构（Relaxed 序、`0.0`-before-any-pin 语义）。计数点：命中 = `try_pin_resident` 成功（`locate_or_load` 快路径与 `alloc_frame` 双检路径共用此单点，每次成功 pin 恰计一次——双检成功即"并发 loader 抢先载入"，无读盘、是真命中）；未命中 = `alloc_frame` 的 load-from-disk 分支（读盘前自增，读失败也计——未命中度量的是"无驻留镜像"）。`new_page` 分配不计（无查找无读盘），redo 专用 `force_reload_from_disk` 不计（页本已驻留）
4. **`QueryStats` ring buffer**（`crates/pg-engine/src/query_stats.rs`，§6.3 选型 (b)）：容量默认 1000（`EngineConfig::query_stats_capacity` 可配，0 = 关闭统计），每条 `{query 文本, latency(Duration), rows(影响/返回行数), path(ExecutionPath), timestamp(SystemTime)}`；单把 `parking_lot::Mutex` 护 `VecDeque`，溢出 `pop_front` 丢最老；读 API `entries()/len()/is_empty()/capacity()`。**`Engine::exec` 单点埋点**（latency 覆盖整个 exec 调用（parse + 执行）；auto-commit 与显式事务两路径天然同覆盖；失败语句记 rows=0；parse 失败在埋点之前返回、不入列）。`ExecutionPath` 七变体（`SeqScan/IndexLookup/Insert/Update/Delete/Ddl/TxnControl`）；`IndexLookup` 为预留——M3 SQL 执行器只有 seq scan（`exec_select`→`scan_inner`），typed `Engine::index_lookup` 按 §6.3 约定**不**入统计
5. **`pg-diag`**（`crates/pg-engine/src/bin/pg-diag.rs` + 渲染逻辑在 lib 侧 `crates/pg-engine/src/diag.rs`，S1）：`txn` 子命令（active_xids + `oldest_snapshot_xmin` + clog/buffer-pool 命中率）、`locks` 子命令（完整 wait-for 图 + 竞争表的 granted/FIFO 等待队列）。渲染函数落 lib 使 CLI 精确输出可进程内 fixture 断言；二进制只是薄壳。**M3 边界文档化**：独立进程打开数据目录看到的是自己刚恢复的空闲引擎实例（单进程诊断）；跨进程 live 诊断归 Phase 4a 经 pg-wire 暴露同一 §6.2 面。指向 live server 数据目录的误用由 F1 的排他锁硬拒绝（见下）

### 与 PG 的 trade-off

| 维度 | PG | 本实现 | 取舍 |
|---|---|---|---|
| 数据目录排他 | `postmaster.pid` + kill(pid,0) 活性检测 | `{data_dir}/lock`（`create_new`/O_EXCL + holder pid，F1） | 零新依赖约束排除 libc/fs2（std 无 flock，MSRV 1.86）；崩溃残留需手动删，活性检测归后续 |
| WAL dump | `pg_waldump` 独立工具，rmgr 注册解码 | `pg-waldump` 挂在格式所属 crate（§6.1 (a)），未知/保留类型打印原始字节 | M3 只有一个 CLI，单建 tools crate 是过度组织；未来工具增多再迁 (b) |
| 查询统计 | `pg_stat_statements` 系统表（持久、跨进程） | exec 层内存 ring buffer（1000 条、溢出丢最老、重启即失） | 系统表化要走 heap/WAL/MVCC 全家桶且自引用（统计表的查询也产统计），归 Phase 6（§6.3）；ring 语义对"诊断最近慢查询"够用 |
| 统计覆盖 | 所有 utility/PL 路径 | 只覆盖 SQL 文本路径（`exec` 单点）；typed API 不入统计 | §6.3 既定口径；typed API 无 SQL 文本可记 |
| 锁/事务自省 | `pg_locks` / `pg_stat_activity` 视图（共享内存跨进程） | 进程内只读 API + CLI 薄壳；独立进程只见空引擎 | 单进程口径 S1 既定；跨进程 live 诊断归 Phase 4a（pg-wire 管理通道） |
| BP/CLOG 命中率 | `pg_stat_bgwriter` / `pg_statio` 计数器表 | 组件内 AtomicU64 + `hit_rate()` 只读方法 | 零新机制；计数不进目录表（同上行） |

### 终审修复（F1–F4）

- **F1（高）数据目录排他锁**：`pg-storage` 新增 `data_dir_lock` 模块——`StorageEngine` 打开时以 `create_new`（O_EXCL）创建 `{data_dir}/lock`（内容 `pid=<n>`，仅供错误消息与同 pid 判定），引擎值 Drop 时删除（声明为结构体最后字段，子系统全部释放后才放锁）。第二个**进程**打开同一目录干净报 `InvalidOperation`（"already in use … remove the stale lock file"）。**选型论证**：flock 语义（fd 生命周期=进程生命周期、崩溃自动释放）需要 libc/fs2，被 §10 零新依赖约束排除（std 的 `File::lock` 1.89 才稳定，MSRV 1.86）；且**任何同进程排除都会打破测试套件的崩溃惯用法**——约 100 个崩溃恢复测试以 `mem::forget(engine)` + 同进程重开模拟 kill -9，被遗忘引擎的 Drop 不执行，锁文件与 flock fd 都不会释放。因此同 pid 冲突（只能是该惯用法）视为残留锁**回收**（warn 级日志），异 pid 一律拒绝。真实跨进程崩溃残留（m2a/m2b crash rounds 与 crash_recovery 的子进程 SIGKILL 后立即重开）由 harness 在回收子进程后删除残留锁文件——恰好扮演文档化的运维清理动作。代价（文档化）：① 真崩溃残留需手动删文件（错误消息明示；kill(pid,0) 活性检测——PG postmaster.pid 的形态——需 libc，归后续）；② 同进程双开活引擎不拦截（F1 的实际危害面是第二进程，如 pg-diag 打向运行中 server）。接线点：`open_with_redo_and_clog`（ensure_data_dir 之后、superblock 探测之前）与 `recover_with_redo_handlers`（先取锁再读任何文件）；`recover` 系列拆出私有 `recover_inner` 承接已持有的锁。测试：`pg-storage/tests/data_dir_lock.rs`（3：异 pid 锁拒绝+手动清理后放行 / 干净关闭放锁可重开 / forget 重开同 pid 回收）+ 模块单测（2）+ `m3_diag_cli.rs::pg_diag_against_live_engine_dir_is_rejected`（pg-diag 子进程打向活引擎目录 → 非零退出 + "already in use"；holder 关闭后放行——真跨进程端到端）
- **F2（低）capacity=0 跳过 entry 构造**：`Engine::exec` 埋点在 `query_stats.capacity() == 0` 时不再构造 `QueryStatEntry`（避免每语句一次 String clone；`engine.rs`）
- **F3（低）查询文本无上限 → 截断 1 KiB**：`QueryStats::record` 单点截断至 `MAX_QUERY_TEXT_BYTES = 1024`（UTF-8 字符边界回退；ring 内存上界 ≈ capacity × 1 KiB）；lib 导出该常量；单测 `query_text_is_truncated_to_cap`（含多字节字符跨界）
- **F4（低）locks_report 非原子快照**：`diag.rs` 补 doc——边列表与表状态来自先后两次独立快照（与 `wait_for_edges` 内部分源加锁同构），并发下可瞬时错位，诊断用途可接受，死锁检测器同此性质

### 已知残留与后续归队

- **F1 残留①：崩溃残留锁需手动删**（既定第一版）：kill -9 后 `lock` 文件残留，下次打开报可操作错误（点名持有者 pid 与删除指引）；自动活性检测（kill(pid,0)）受零新依赖约束归后续——若未来允许 libc 边或 MSRV 升至 1.89（`File::lock`），改 flock/postmaster.pid 全语义
- **F1 残留②：同进程双开活引擎不拦截**（同 pid 回收是崩溃测试惯用法的前置）；进程内误开属编程错误，靠 review 把关
- **跨进程 live 诊断不可达**：`pg-diag` 独立进程看到的是自己的空引擎实例（M3 口径 = "诊断面以 CLI 形式可用"，测试/单进程场景）；live server 诊断归 **Phase 4a**（经 pg-wire 暴露 §6.2 自省面，coding-plan"遗留与归队"S1 注记）
- **waldump 撕裂/中段损坏覆盖注记**：撕裂尾部=干净 EOF、中段 CRC 失败=停止并报 LSN 的行为**继承自 `WalReader`**，由 `wal/reader.rs` 既有单测覆盖（`read_torn_tail_record_with_zero_remainder_returns_none` / `read_crc_failure_with_records_after_still_errors` 等）；`waldump.rs` 集成测试覆盖全记录族、LSN 过滤边界与 reserved 类型，未单独构造 bin 层撕裂 fixture（底层语义已有确定性测试，bin 只是透传）
- **真正未知判别式（更新版本二进制所写的 WAL）在 dump 中报错停止**（记录边界不可读，无法越过），与 §6.1 要求的 reserved 类型（判别式已知、仅无 handler）不报错不同——前者会打印已走到的全部记录后报 LSN
- **`ExecutionPath::IndexLookup` 预留无生产者**：SQL 执行器无索引访问路径（`exec_select` 恒 seq scan）；planner 索引选择归后续 stage
- **QueryStats 重启即失 + 溢出丢最老**（§6.3 既定代价）；系统表化归 Phase 6
- **waldump 需要显式 `--segment-size`** 匹配非默认段长的数据目录（段长不在段文件内自描述；superblock 化段长元数据归后续）
- **统计埋点性能口径**：验收为"对 exec 路径开销不可测"（churn 对比抽查）——埋点 = 一次 Mutex push + 两次时钟读，相对 WAL append/页 I/O 不可测；~~正式 S2 数字归 Stage G 收口 benchmark~~ **已在 Stage G 落盘**：收口 S2 86.0 vs 基线 86.8（-0.9%，噪声界内，`docs/phase1-m3-benchmarks.md`）
- **loom 两模型预存红**沿用 Stage D 登记条目（本 stage 未触碰 latch/WAL 编排；BufferPool 计数器走 `crate::sync` 别名层，loom 构建不受影响）
- **19 个 pre-existing rustdoc 警告（跨 6 crate：pg-storage 5 / pg-txn 2 / pg-catalog 1 / pg-am-heap 7 / pg-am-btree 3 / pg-engine 1）**：本 stage 零新增；CI doc job（`RUSTDOCFLAGS=-D warnings`）对其必红——**按用户节奏逐步消解，不阻塞本 stage 收口**；消解时顺带注意 pg-engine 的一处来自 Stage D 的 `Self::auto_commit` 私项链接


## Stage F（M3）：pg-wire（v3 最小协议 + 连接管理 + SQL 透传）

**状态**：✅ 完成（本 crate 28 绿：wire_protocol 24 + wire_clients 4；clippy `-p pg-wire --all-targets --all-features -D warnings` 绿；fmt 绿；doc job（`RUSTDOCFLAGS=-D warnings`）对 pg-wire 绿、零新增警告；`cargo tree` 抽查零新运行时依赖；终审修复 F1/F4 落地、F8 补强测试 2 条；F7 修复合入本 stage（commit 失败回退 abort + 双路径红绿回归测试，`m2b_index_txn` 13 绿）；**最终全量回归 739/0**（权威计数 740 = 739 可运行 + 1 ignored；早期一轮 736/1 的唯一失败为 `btree_concurrent::concurrent_small_pool_split_eviction_storm`；**注意（P3-1 修正）**：该测试 solo 实测 23 跑 2 败（~9%），失败信息 "scanner missed committed key 0" 与 loom 预存红（"key 0 lost across the split"）**同族**——"仅全量并行下偶发"的措辞不准确，它意味着 CI 的 pg-am-btree test job 约每 11 次红 1 次；已与 loom 红条目并档，疑同一底层 bug，单开修复会话排查而非容忍；与本 stage 无关（未触碰 pg-am-btree）；未 commit——等用户确认）
**工期**：预估 5–7 天（+1–2 兼容余量未动用）
**验收**：`pg-wire/tests/wire_protocol.rs`（24：启动四码 framing + 常规帧 `Q`/`X`/未知 tag + 畸形帧拒绝 / 超大声明长度在上限处拒绝为 Protocol 错而非分配（F1 红绿对照：旧实现会放行后撞 EOF-Io 错）/ `Q` 串 NUL 后多余字节报 Protocol 错（F4）/ 全部后端编码器 golden bytes + RowDescription/DataRow 结构断言 / 六类型文本编码 + NULL 协议标记 / BEGIN→begin_txn、COMMIT→commit、ROLLBACK→abort 映射 + exec 零到达（QueryStats 无 BEGIN/COMMIT 条目佐证）/ 事务中 BEGIN 报 25001 且句柄存活 / 无事务 COMMIT/ROLLBACK 报 25P01 / **带活事务断开连接 → XID 自动回收（F8）** / **CancelRequest 会话级关连（F8，真 socket）** / 多语句按序 auto-commit + 首错截断余串 / 探针语句 42601 报错不断连 / 空查询 EmptyQueryResponse / 全类型 SELECT 端到端（schema 驱动 OID + 文本值 + 5 NULL 标记））；`pg-wire/tests/wire_clients.rs`（4，rust-postgres 真 TCP 硬门槛：全 CRUD + BEGIN/COMMIT/ROLLBACK + 多语句串 / 探针 SET、SELECT version() 报错不断连 / 六类型文本 roundtrip / 4 并发客户端各自表 CRUD + 交错显式事务——全部挂 120s watchdog）

### 交付内容

1. **新 crate `crates/pg-wire`**（workspace member，依赖 pg-engine；§7.1）：只做协议编解码 + 连接管理 + SQL 透传，零执行逻辑。运行时依赖仅 pg-engine/thiserror/tracing（全部已在 workspace 依赖图内，§10 零新运行时依赖成立）；dev-dependency 引 `postgres` 0.19（rust-postgres 同步封装，§10 允许的测试驱动客户端）+ tempfile + uuid（fixture 构造，已在图内经 pg-am-heap）
2. **手写编解码**（`src/codec.rs`，§10 选型 (b)）：启动阶段无 tag 帧（v3.0=196608 / SSLRequest / GSSENCRequest / CancelRequest 四码分发）+ 常规阶段 1B tag + 4B 长度帧；长度字校验上限分档——启动包 10000（PG `MAX_STARTUP_PACKET_LENGTH` 同款）、常规消息 64 MiB，且**分配随读入 8 KiB 分块增长**（声明长度永不全额预分配，终审 F1 修复；声明超上限拒绝为 Protocol 错而非分配）；干净 EOF 区分于截断帧（前者=正常关连，后者=协议错误）。后端编码器全部追加进调用方 buffer，一个 ReadyForQuery 周期一次 `write_all` 落盘
3. **启动协商**（`src/session.rs`）：SSLRequest→`'N'`、GSSENCRequest→`'N'`（§7.2"忽略"落地为"拒绝加密但必回字节"——无 GSS 的 PG 也回 `'N'`，真不回字节 libpq 会永久阻塞）、CancelRequest→直接关连（§7.2 非目标，PG 消费完 cancel 包同样关连）、StartupMessage→AuthenticationOk（trust）→ParameterStatus 最小集（server_version=16.0 / server_encoding / client_encoding / DateStyle / integer_datetimes / standard_conforming_strings / is_superuser / session_authorization，外加回显 application_name）→ReadyForQuery
4. **事务拦截**（`src/session.rs`）：`BEGIN`/`COMMIT`/`ROLLBACK` 由 pg-wire 层解析（复用 engine 的 `sql::parse`——同一解析器保证拦截面与 exec 面零漂移）后映射到 `Engine::begin_txn`/`TxnHandle::commit`/`abort`，**不透传 exec**（engine.rs exec_auto/exec_txn 对三者硬报错）；每连接至多一个 `TxnHandle`；事务中 BEGIN 报 25001（PG 是 WARNING 续跑，此处硬错误——"报错不破坏现状"口径内且客户端更易观测）且句柄原样存活；无事务 COMMIT/ROLLBACK 报 25P01；连接断开/Terminate 时 `TxnHandle::drop` 自动 abort，XID 不泄漏
5. **多语句串**：`split_statements`（pub，可单测）按顶层 `';'` 切分，单引号字面量与 `''` 转义正确跳过；语句按序执行（auto-commit 各自独立，显式事务内共享该连接句柄）；首条错误截断余串（PG simple-protocol 语义）发 ErrorResponse 后照常 ReadyForQuery；全空串回 EmptyQueryResponse
6. **结果渲染**：RowDescription（schema 驱动类型——SELECT 的表名经 `Engine::describe_table` 解析出真实 table OID/attnum/列类型；解析不到时回退到首个非 NULL 值推断，再回退 TEXT）+ 文本 DataRow（NULL=长度 -1 协议标记）+ CommandComplete（`SELECT n`/`INSERT 0 n`/`UPDATE n`/`DELETE n`/`CREATE TABLE`/`CREATE INDEX`/`BEGIN`/`COMMIT`/`ROLLBACK`）。SQLSTATE 映射只保留客户端可行动的区分：42P01/42P07/0A000/42601，其余 XX000
7. **类型文本编码**（`src/types.rs`，§7.2）：INT4/INT8 十进制、TEXT 原样、NULL 协议空值、Bytea `\x`+小写 hex（恰为 PG 文本格式，OID 17 如实上报）、**Timestamptz 报 INT8 OID 配 µs 整数编码、Uuid 报 TEXT OID 配标准串编码**（见 trade-off 表）；`Datum::External`（TOAST 指针）响亮报错——M3 读路径不解析 TOAST
8. **线程模型**（`src/server.rs`，§7.3）：`std::net::TcpListener` + 每连接一个命名 std 线程 + `Arc<Engine>`，零 tokio；accept 后即 set_nodelay；会话失败（I/O 或协议违规）只关该连接并 warn 日志，accept 循环与其余连接不受影响；连接线程 detached（进程即生命周期属主，与全仓库一致）
9. **O5 编译期断言**（`src/lib.rs`）：裸 fn `assert_engine_send_sync` 钉死 `Engine: Send + Sync`（无 static_assertions crate）；该性质一旦失效线程模型即不健全，编译期直接拒
10. **CI 接线**（P2-5 修正）：`.github/workflows/ci.yml` 的 clippy/test/doc 三个 crate matrix 加入 pg-wire（fmt job 无 matrix，`cargo fmt --all` 天然覆盖；msrv job 为 `--workspace --all-features` 同样天然覆盖）；pg-wire 零 features，test job 走 all-features 分支（loom 豁免分支先例不涉及，已在 ci.yml 注释注明）

### 与 PG 的 trade-off

| 维度 | PG | 本实现 | 取舍 |
|---|---|---|---|
| Timestamptz/Uuid 的 RowDescription OID | 真 OID（1184/2950）配各自文本格式 | **OID 与线上字节自洽**：µs 整数报 INT8(20)、标准 UUID 串报 TEXT(25) | §7.2 只钉值编码不钉 OID；报真 OID 则无任何 stock 客户端能把 µs 整数解成 timestamptz（psycopg2 的 1184 cast 直接炸），roundtrip 验收不可能成立。真 OID + PG 文本时间戳格式归 Phase 4a |
| 协议面 | Extended Query / COPY / 认证 / CancelRequest / TLS 全家桶 | 仅 Simple Query 最小闭集（§7.2 既定非目标） | M3 目标是"驱动能连上跑 CRUD"；扩展面归 Phase 4a（独立 crate 的立意即协议演进不污染 engine） |
| 事务中 BEGIN | WARNING 级别，事务继续 | ERROR 25001，事务继续（句柄不动） | "报错不破坏现状"验收口径内；硬错误对客户端更易断言 |
| 语句失败后的显式事务 | 进入 aborted 态，只接受 ROLLBACK（ReadyForQuery 报 `'E'`） | 无 failed-txn 态：事务保持 `'T'`，后续语句照常执行（M2b 语义——失败语句已写行留在事务内，安全收口只有 abort） | engine 无子事务是既定 M2b 边界，wire 层不虚构 `'E'` 语义 |
| GSSENCRequest | 不支持时回 `'N'` | 回 `'N'`（§7.2 字面为"忽略"，落地为"不加密但必应答"） | 真忽略（零响应）libpq 永久阻塞——解释性落地，已注 session.rs 文档 |
| 连接数 | fork 进程/连接 | std 线程/连接 | §7.3 既定：千级连接线程爆炸归 Phase 4a 再评估 runtime；M3 不做连接数压测 |

### 终审修复（F1–F8）

- **F1（中）声明长度即分配 = 内存 DoS 面**：原实现长度校验（旧上限 1 GiB）通过后立即 `vec![0u8; len]`——5 字节头声明 1 GiB 即触发等额分配并阻塞持有。修复（`codec.rs` `read_body`）：① 上限收紧——启动包 10000（PG `MAX_STARTUP_PACKET_LENGTH` 同款，自 8.0 即此值）、常规消息 64 MiB（SQL 子集无大对象字面量，绰绰有余）；② 分配随读入 8 KiB 分块增长，声明长度永不全额预分配，任一时刻内存上界 = 对端实际已发字节 + 一块余量。测试 `oversized_declared_length_rejected_before_allocation`：1 GiB / 64 MiB+1 / 10001 声明均拒绝为 Protocol 错（红绿对照：旧实现会放行分配后撞 EOF-Io 错）；贴上限但 body 不到则 Io 错（上限放行、分块读取零大额预分配）
- **F4（nit）`Q` 消息 NUL 后多余字节静默忽略 → Protocol 错**（PG "invalid string in message" 同款）：cstring 必须占满整帧，拖尾字节 = 对端 framing bug，不静默丢弃。测试 `query_trailing_bytes_after_nul_rejected`
- **F8 测试补强**：`drop_session_mid_txn_reclaims_xid`（BEGIN 后 drop session，`TxnHandle::drop` 自动 abort → `active_xids` 归零，XID 不泄漏钉 horizon）；`cancel_request_closes_connection`（真 socket 发 CancelRequest → 服务器无应答关连，客户端读到干净 EOF，会话线程 Ok 返回）

### 已知残留与后续归队

- ~~三家手动矩阵未跑~~ **已归档（Stage G，2026-08-27）**：psql 19devel / psycopg2 2.9.12 / node-postgres pg 8.23.0 三家 CRUD + BEGIN/COMMIT/ROLLBACK 全部通过，探针报错清单与新发现（psycopg2 隐式事务包 DDL 撞 M2b 边界）落盘 `docs/phase1-m3-benchmarks.md` 手动矩阵节；CI 硬门槛维持只有 rust-postgres（`wire_clients.rs` 4 测试全绿）。手动 server 载体 = Stage G 新增的 `crates/pg-wire/src/bin/pg-server.rs`（`wire_clients.rs` 头部文档原建议的 "a tiny bin"）
- **psql catalog 探针固有落差**（§11 R3）：`\d` 等元命令与 `pg_type` 族查询答不出，口径 = "报错不断连 + 基本 CRUD 可用"，M3 不承诺交互体验
- **Extended Query 非目标**（§7.2）：rust-postgres 的 `query`/`execute`（走 Parse/Bind/Execute）不可用，测试一律走 `simple_query`/`batch_execute`；驱动侧参数化查询归 Phase 4a
- **TOAST 值读路径不可达**：`Datum::External` 上报 Encode 错误（响亮失败非静默错值）；TOAST 解析归 Stage I 既定归属
- **结果集整体缓冲（F2 点名）**：一个 `'Q'` 周期的全部响应进单个 Vec 再一次 `write_all`——大结果集峰值内存 ~2×（engine 物化的行 + 编码后的帧），且客户端中途断开要等全量算完才察觉。engine 本就物化全部行（`QueryResult::Rows`），无额外渐近开销；流式 DataRow 归 **Phase 4a**
- **slow-loris / 连接上限 / 线程不 join / 优雅关闭（F3 点名，§7.3 既定代价的具体形态）**：连接无读超时也无总数上限——慢速对端（slow-loris）可永久占住一个连接线程（线程模型下每连接一线程，占满即拒新连接）；连接线程 detached 不 join；无优雅关闭路径，且 accept 循环失败后 `Engine`（含 DataDirLock）要存续到最后一个连接线程退出才释放。M3 目标是"驱动能连上跑 CRUD"非公网服务；读超时/连接上限/关闭握手归 Phase 4a 与 runtime 再评估一并处理
- **只认 protocol 196608（F5 记录项）**：3.x 次版本请求硬错误而非回 `NegotiateProtocolVersion`——PG18 前 libpq 默认协商 3.0 不受影响；客户端显式 `max_protocol_version=latest` 会撞。归 Phase 4a 协议扩展面
- **database 参数不校验（F6 记录项）**：trust 模式下任何 dbname 均可连（单数据目录即库的既定形态）；多数据库/校验归后续阶段
- ~~`commit()` 失败路径泄漏 XID（F7 记录项，非本 stage 引入，登记）~~ **已修复（Stage F 收口期）**：`TxnHandle::commit` 在 `commit_txn` 失败时回退为 best-effort abort——先回放索引 undo（与 `abort()`/`Drop` 同纪律）再翻 CLOG 位，XID 不再泄漏、horizon 不再被钉死；注入钩子 `pg_txn::manager::test_hooks::set_commit_txn_force_fail`（doc-hidden thread-local）+ 回归测试 `m2b_index_txn::commit_failure_falls_back_to_abort_and_reclaims_xid`（红绿对照成立：修复前 XID 留在 active set 断言必红）
- **BackendKeyData 不发**（无 CancelRequest 支撑，§7.2 非目标）；个别驱动若强依赖 key data 会在取消路径上失败，正常查询路径不受影响
- **19 个 pre-existing rustdoc 警告**沿用 Stage E 登记条目（本 stage 零新增，pg-wire 的 doc job 单独验证为绿）

---

## Stage G（M3）：接口预留 + M3 收口（M3 出口）

**状态**：✅ 完成（回归与 S2 数字见下；未 commit——等用户确认；`phase1-m3` tag 经用户确认后打）
**工期**：预估 3–4 天
**验收**：debug 全量 **743 绿 / 0 失败 / 1 ignored**（= Stage F 出口 739+1 + 本 stage 新增 3 个编译桩/状态机测试 + 修复会话新增 1 个 slot-0 回归测试；743 为根治修复后收口轮实测）；release 全量 **743 绿 / 0 失败**（slot0 回归测试非 debug-only,release 档同计）；`m2b_crash_rounds` 4 绿（69.9s）；**loom 双模型绿（~250s,CI 口径 `LOOM_MAX_PREEMPTIONS=2`)**——`loom_two_writers_one_reader_linearizable` / `loom_split_with_concurrent_writers` 随 flaky 家族根治一并转绿（详见"已知残留"✅ 根治条目；此前 26s 快速硬失败的预存红登记保留作历史记录）；`m3_vacuum_crash_windows` 2 绿（O2 回归复核）；`m2b_index_txn` 13 绿（F7 复核）；**S2 收口（`M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100` release × 3）：86 / 86 / 86 txn/s，均值 86.0 vs M2c 基线 86.8（-0.9%，噪声界 ±3% 内，<5% 上限内）——注册 + vacuum + 统计埋点全栈无统计显著回归，§12.5 通过**；clippy `--workspace --all-targets -D warnings` 绿；`cargo fmt --all --check` 绿；200 轮 churn release soak 复跑绿（8.74s）；**release 档 flaky 清偿（终审发现，Stage C 引入、非本 stage 回归）**：`vacuum_reclaim::readers_during_compaction_see_no_half_compacted_page` 为 release-only 概率性 flaky（实测 solo release 10 跑 4 败，debug 5/5 过）——reader 线程缺启动栅栏，release 下 4-tuple 页的 reclaim 快于线程调度，reader 首次被调度时 `stop` 已置位、零迭代返回，`reads > 0` 断言 flake（测试设计缺陷，非产品 bug；页一致性断言本身只在有读时才有意义）。修复 = reader 首个完整读 pass 后发 `started` 信号、主线程等信号后再 `reclaim()`（watchdog 口径保持：等不到信号 FAIL 而非挂起）；修复后 solo release 30/30 + debug 5/5 全绿

### 交付内容

1. **SegmentedStorage 接口预留**（tech-selection §8，`crates/pg-storage/src/segment.rs` 新模块）：`SegmentId(u64)` newtype + `SegmentState { Active, Frozen, Sealed, Merging, Retired }` 单向状态机（唯一可执行件 = 纯谓词 `can_transition_to`，使单向性可测且实现期无法静默扩边）+ `SegmentedStorage` trait（`create_segment/freeze/seal/merge`，逐方法状态机前置条件入 doc）。**WAL payload 契约落 doc**（`wal/record.rs` 的 `SegmentSeal=110`/`SegmentMerge=111` 变体注释）：Seal 载单个 segment id；Merge 载输入 id 列表（按 merge 顺序）+ 目标 id，redo 幂等语义写明。判别式为 Stage 0（M1+M2 基线）既有预留（`from_u8` 可解析、无 handler 恢复硬失败），本 stage **零新增占号、零 handler、零实现**；编译桩测试（`stub_impl_compiles` / `state_machine_is_one_way`）证明 trait 形状可实现
2. **Tier 2 接口预留**（tech-selection §9，`crates/pg-storage/src/tier2.rs` 新模块）：`WalTailReader` trait（`tail_from(start: Lsn) -> Box<dyn Iterator<Item = Result<WalRecord>> + '_>`，§9 既定 iterator 形状；doc 契约钉死——只吐已 flush 记录、严格 LSN 升序恰好一次、拉取式天然背压（至多一条在飞、禁止内部无界缓冲）、到 flush 前沿返回 `None` 不阻塞、断点续传 = 以末条 LSN 后继再次 `tail_from`、物化 iterator 形状实现期可改回调/流式）+ `WatermarkRegistry` trait（`watermark(index_oid) -> Option<Lsn>` / `set_watermark(index_oid, lsn)`；doc 注记实现期需单调不回退）。`AccessMethod` 加 `fn freshness(&self) -> Option<Lsn> { None }` 默认方法（`pg-am-heap/src/access_method.rs`）——**默认 None = 现有 AM 零改动零行为变化**（heap/btree 为同步维护，恒新鲜，None 即正确答案；无调用方）
3. **O2 验证清偿（只验证不实现，计划既定）**：显式回归**已存在**，归档引用并关闭 O2——`pg-engine/tests/m3_vacuum_crash_windows.rs::crash_loser_insert_entry_removed_by_vacuum`（崩溃孤儿插入：事务在飞 kill -9 → 恢复（`HeapUndoHandler` 标 ATT 残余成员 ABORTED 于 CLOG）→ `scan_dead_tuples` 规则 1 收集（`stats.dead_tuples == 1`）→ vacuum 回收（`dead_tuples(u64::MAX)` 归零 + 悬挂索引条目实际删除）；红绿口径由 Stage D 建立）+ churn 的 `crash_with_inflight_inserts` 注入轮（批量形态，`index_entries_removed == BATCH`）。本 stage 复核两测试当前绿，O2 关闭
4. **F7 核销（Stage F 已修，本 stage 归档）**：commit 失败 XID 泄漏已在 Stage F 收口期修复（`engine.rs` `TxnHandle::commit` 失败回退 best-effort abort + auto_commit 同纪律）；回归 `m2b_index_txn.rs::commit_failure_falls_back_to_abort_and_reclaims_xid` / `auto_commit_commit_failure_reclaims_xid` 本 stage 复跑全绿（13/13），归档关闭
5. **O4 清理**：移除 `pg-storage/Cargo.toml` 的 tokio 死依赖声明（全仓库 `.rs` 零使用复核成立——grep `tokio` 于 src/tests/benches/build.rs 零命中）；`cargo tree --workspace --edges normal` 运行时依赖图 **tokio 归零**（残留 tokio 仅在 pg-wire 的 dev-dependency 链：rust-postgres 驱动 → tokio-postgres，§10 允许的测试驱动，不进运行时图）；全量回归双档全绿（见验收行）
6. **benchmark 落盘**：`docs/phase1-m3-benchmarks.md`（格式对齐 M2 文档）——churn 页数有界（30 轮 + 200 轮 soak）、注册开销（A：87.2 vs 86.8）、vacuum 叠加 TPS（D：88.0 vs 86.8）、waldump 吞吐（smoke ~249K records/s / ~66 MB/s）、**WAL 字节量观测（N5 首次量化：最坏档 FPI 占 64.1%、总 WAL 2.05× 于对照档；240 轮长程 62.7% 稳态复证）**、Stage G 收口 S2、手动三客户端矩阵。测量载体：`pg-engine/examples/m3_wal_bytes_probe.rs`（N5 探针，可复现）+ `pg-wire/src/bin/pg-server.rs`（手动矩阵固定端口 server，`wire_clients.rs` 头注原建议的 tiny bin 落地）
7. **手动矩阵归档（N6）**：psql 19devel / psycopg2 2.9.12 / node-postgres pg 8.23.0 三家 CRUD + 事务全过，探针报错清单（`\d`、`SELECT version()` 报错不断连）与新发现（psycopg2 默认隐式事务包 DDL 撞 "DDL inside explicit transactions is not supported in M2b" 边界，autocommit 模式全过——`wire_clients.rs` 头注已补 autocommit 指引）落盘 benchmark 文档；Stage F 残留条目标记已归档
8. **收口清单登记**（Stage E 终审建议）：① Stage E 性能抽查 S2 数字并入 Stage G 收口 S2 行（stats 埋点开销随全栈度量，低于噪声界）；② README 补 QueryStats 口径一句（typed API 不入统计，§6.3 另注）

### 与 PG 的 trade-off

| 维度 | PG | 本实现 | 取舍 |
|---|---|---|---|
| segment 生命周期 | 无对应物（PG 堆/索引非 segment 架构；LSM 系（如 RocksDB）有 compaction 状态机） | `SegmentedStorage` trait + 单向五态机，只定契约不实现 | §8 既定：接口先行锁 Phase 3/5 方向；签名返工风险接受（预留即承诺，改动过修订记录） |
| WAL 逻辑复制/订阅 | logical decoding slot + output plugin | `WalTailReader` 拉式 iterator 预留（断点 = 调用方自管 LSN） | §9：极简形态供 Tier 2 异步跟随；背压靠拉取模型，无 slot 状态；物化形状实现期可返工 |
| 索引新鲜度 | PG 索引同步维护、恒新鲜，无 freshness 概念 | `freshness()` 默认 None（同步 AM 恒新鲜即 None），Tier 2 实现期接 WatermarkRegistry | §9：带默认实现的方法而非新 trait，现有 AM 零改动 |

### 已知残留与后续归队

- ~~**loom 模型测试预存红照旧（诚实登记，验收命令暂不通过）**~~ **已根治（2026-08-26 修复会话）**：随 flaky 家族根治一并转绿，loom 双模型现 CI 口径全绿（~250s）——见下方 ✅ 根治条目。历史登记：`LOOM_MAX_PREEMPTIONS=2 cargo test -p pg-am-btree --features loom --test btree_loom` 两模型曾稳红（`loom_two_writers_one_reader_linearizable` / `loom_split_with_concurrent_writers`，"key 0 lost across the split" 族）；二分证据（M2 出口提交 31fe4b8 同红，M2c 之后工具链/环境漂移；Stage D 已并档 `btree_concurrent::concurrent_small_pool_split_eviction_storm` 的 solo flaky 为同一底层 bug 嫌疑）经根治会话证实——根因确为并发 split 丢 committed key（slot-0 落位判定陈旧），见下方根治条目的根因分析
- **flaky 家族成员清单（统一登记，替代此前分散条目）**：以下为同一底层 bug（并发 split 丢 committed key，签名均为 "scanner missed committed key" / "key 0 lost across the split"）的全部已知形态与实测频率——① `btree_loom::loom_two_writers_one_reader_linearizable`（稳红，~22s 快速失败）；② `btree_loom::loom_split_with_concurrent_writers`（稳红，同上）；③ `btree_concurrent::concurrent_small_pool_split_eviction_storm`（solo ~9%，实测 23 跑 2 败）；④ `btree_concurrent::concurrent_hundred_thread_smoke`（全量并行负载下偶发，solo 重跑 3/3 过，本次 Stage G 终审后又现 1 例：scanner missed committed key，btree_concurrent.rs:423）；⑤ `btree_concurrent::concurrent_duplicate_keys_lookup_all`（边界落位修复后 15 跑 1 败）。~~**家族意味着 CI 的 pg-am-btree test job 约每 11 次红 1 次**——登记不构成容忍，修复会话是该家族唯一的出口~~ **已根治（见下条 ✅ 条目）**：修复后家族四测试 release 各 20/20 连跑全绿 + 根治后全量回归 743/0 零失败，上述频率记录保留作历史
- **✅ 家族已根治（2026-08-26 修复会话）**：根因 = `pin_leaf_for_insert` 的 slot-0 落位判定在"释放闩锁探测左邻域 → 重新取锁"的窗口内变陈旧——并发插入恰在窗口内占领 cur 左沿（slot 0），按陈旧判定落位会（a）破坏页内 `(key,tid)` 序（loom 模型 1 的 "entries out of order")、(b) 进而跨分裂丢 key（模型 2 与家族全部签名）、(c) 把精确重复插入静默落位而非报 DuplicateKey。修复（index.rs `pin_leaf_for_insert`）：重取锁后**重验证 slot-0 判定**（cur 首条目仍大于探针才落位，否则重启落位——重算 slot 落内部位置直接返回）。确定性回归 `slot0_insert_revalidates_after_relatch_window`（test hook `SLOT0_WINDOW_PARK` 把并发插入钉死在窗口内）。验证：loom 双模型绿（250s,CI 口径）+ 家族四测试 release 各 20/20 + 全量回归（见 Stage G 收口轮）。此前各条目（预存红、~9%、每 11 次红 1 次）保留作历史记录
- **预留签名可能返工**（§8/§9 既定代价）：merge 或需携带 LSN 区间、`WalTailReader` 或改回调/流式——预留即承诺，改签名过修订记录
- **预留 trait 零调用方**（设计使然）：`SegmentedStorage`/`WalTailReader`/`WatermarkRegistry` 仅编译桩测试消费；`freshness` 默认 None 无覆盖需求（默认值即契约）。实现期（Phase 2/3/5）首批调用方落地时补真测试
- **手动矩阵不进 CI**（N6 既定）：客户端版本随环境漂移，CI 硬门槛维持 rust-postgres 一家；复跑命令在 benchmark 文档与 `wire_clients.rs` 头注
- **m3_wal_bytes_probe 为测量工具**：example 非测试，无断言；N5 数字为单次实测（负载确定性高，复跑方差小），非 CI 门槛

---

## Stage A（M4）：pg-am-hnsw 地基（encoding / distance / PRNG / 参数校验）+ A1 清偿

**状态**：✅ 完成（本 crate 52 绿；clippy/fmt/doc 全绿；全量回归见验收行；PHASE2-M4-StageA commit 随本次收口建立）
**工期**：预估 3–4 天（v1.3 上修，含 coverage plumbing 半天）
**验收**：`pg-am-hnsw` 52 测试全绿（encoding 编解码往返 + load-validation 负例全家 / distance 三度量 f64 累加器对拍 / rng 几何分布与显式种子确定性 / params 构造期校验负例——含收口期新增 m_max0 两枚）；A1 红→绿测试两枚（`pg-am-btree/tests/btree_split_crash.rs:550-712`：FPI 先于 Prepare 断言 + 手工撕页恢复）修复前红、修复后绿；clippy `-p pg-am-hnsw --all-targets -D warnings` 绿；`cargo fmt --all --check` 绿；doc（`RUSTDOCFLAGS=-D warnings`）绿；全量 workspace 回归 797 绿 / 0 失败（= M3 出口 743 + 本 crate 52 + A1 测试 2）。**验收第 2 条（CI 全 job 绿且新 crate 在三 matrix 实际执行，查日志非绿勾）需 push 后由 CI 闭环**——本地零命中护栏结论见交付内容 4，CI 侧覆盖在 commit 后自动生效（护栏走 `git grep`，untracked 不参与）

### 交付内容

1. **新 crate `crates/pg-am-hnsw`**（workspace member，1634 行 / 8 文件，52 测试全绿）：
   - `params.rs`（152 行）：`NodeId` newtype（§3 稳定性契约：稠密递增 / 永不复用 / 快照往返稳定）+ `HnswParams` 构造期校验（§4.2/§4.4）。**含 m_max0 契约空白闭合（2026-08-31 收口期，原登记"归 Stage B 开工前评估"提前清偿）**：`m_max0 >= m` 校验入 `HnswParams::new` 与快照头 load 双路径（`encoding.rs` 头校验重跑，违例报 `Corrupted`），负例测试两枚；tech-selection §3 登记条随 v1.7 闭合
   - `encoding.rs`（859 行）：冻结快照字节流原语（§3）——header / node-record 编解码、CRC32 前缀约定（§7，与 pg-storage WAL/checkpoint 同 crate `crc32fast`）、完整 load-validation 清单（`node_count` 上界校验、`level_count == top + 1` 恒等式、非有限分量拒绝等）；对抗 review 的 P1-1（decode `node_count` 上界）/ P1-2（`params_from_header` 的 `ef_search_default` 改显式入参）/ P2-1（`!is_finite()` 加固）落此
   - `distance.rs`（285 行）：L2² / cosine / 负内积，f32 元素 + f64 累加器标量循环（§5）——不可重结合 f64 链即跨平台确定性机制，禁 fast-math 类优化
   - `rng.rs`（215 行）：手写显式种子 xoshiro256** + 几何分布 level 抽取（§4.1）；不用 `rand` crate（§10）
   - `error.rs`（51 行）：`HnswError`（thiserror，workspace 惯例）
   - `graph.rs` / `snapshot.rs`：占位模块（Stage B / Stage C 主体，lib.rs 头注已写明归属）
2. **A1 清偿（回收页撕页暴露，ROADMAP 债表 A1 划销）**：审计确认唯一未覆盖消费者 = btree 在线 split 右页；在 `pg-am-btree/src/index.rs` `split_prepare_on_guards` 单一收口点补 `log_page_init`（post-image FPI）；`new_page` 统一处理方案经论证否决（FPI 双门控时序，理由见 `buffer_pool.rs:424` 注释）
3. **CI 注册五件事一次做全**（coding-plan §8.2 v1.3）：① workspace `members` 加 pg-am-hnsw；② clippy / test / doc 三 crate matrix 加 crate（fmt 单 job 无 matrix、msrv 为 workspace 级 `cargo check --workspace --all-features`，两者天然覆盖）；③ loom 豁免分支归类——本 crate 无 loom 模型、零 features，走非 loom 分支（ci.yml 注释注明）；④ 护栏核对（结论见下条）；⑤ coverage job 新建（tarpaulin，Linux-only runner，cobertura.xml artifact 上传——本机 macOS 不可跑，Stage E 覆盖率判定以 CI 报告为准）
4. **grep 护栏核对结论（CI 任务④"预期零命中，写明核对结论"落盘，本地实测含 untracked 新 crate）**：四条全零命中——① `use parking_lot` 直引（sync-alias 护栏适用范围）；② `Snapshot {` 字面构造；③ `impl Snapshot` 块；④ `Snapshot::new_unregistered(` 调用（快照模块类型命名用 `SnapshotHeader` / `SnapshotFile` 复合名，按 v1.3 具名 hazard 规避 Snapshot 护栏误伤）。另核 `HashMap` / `parking_lot` 全量：源码零命中（唯一命中为 `graph.rs:15` 注释 "HashMap/HashSet iteration order is banned"——即禁令条文本身）

### 与 pgvector·hnswlib 的 trade-off

| 维度 | pgvector / hnswlib | 本实现 | 取舍 |
|---|---|---|---|
| 存储形态 | pgvector 页式磁盘索引（接入 PG buffer/WAL）；hnswlib 纯内存 + 自有 save/load | Stage A 纯内存 + 冻结快照字节流格式；WAL/buffer-pool 接入归 M5 | §2 既定：先把确定性内核与冻结格式钉死，存储接入晚一个 milestone；pg-storage 依赖边缓至 M5（P2-2 修正，直依赖冻结 {thiserror, crc32fast}） |
| 随机数 | hnswlib 用 `std::mt19937` 默认种子 | 手写 xoshiro256**，显式种子贯穿 | §4.1/§10：确定性三前提之一（可复现测试 + 快照可重放），不引 `rand` crate |
| 距离计算 | hnswlib SIMD（SSE/AVX）+ f32 累加 | 标量循环 + f64 累加器，禁重结合优化 | §5：跨平台/跨编译器位级确定优先于速度；性能优化留待 benchmark 驱动另行评估 |
| 参数校验 | 构造期弱校验，非法参数运行期才暴露 | 构造期全量校验 + 快照 load 重跑头校验（`m_max0 < m` 拒载） | §4.2/§4.4：契约违例在边界处响亮失败，不进图结构 |
| 快照格式 | hnswlib 自有二进制（无版本化演进契约） | 冻结字节流 + CRC32 前缀 + 完整 load-validation 清单 | §3/§7：格式即契约，坏载响亮 `Corrupted` 而非静默错图 |

### 已知残留与后续归队

- **encode 侧两个拒绝分支实际不可达（nano 登记，本条即登记）**：`encoding.rs:190`（`level_count > 255`）与 `encoding.rs:205`（单级邻居数 > 65535）——几何分布层级上界与 m_max/m_max0 参数上界使两分支在合法参数下不可达；Stage E tarpaulin 报告会显示未覆盖，届时用 `#[cfg(test)]` 构造触达或在覆盖率门槛登记豁免
- **MSRV 1.86 本机不可验（P3-6 登记）**：开发机无 1.86 工具链，MSRV 仅靠 CI msrv job 把关；代码未用新语法/新 API，风险低
- **审计分支归属偏离 plan v1.3（P3-5 登记）**：方案要求"审计 PR 只含 rustdoc + stage_spec，M4 文档从 merge 后的 main 另开 PR"；实际 `444ab9b` 已把 M4 文档（coding-plan / tech-selection）提交到审计分支，且混入 `bench-nightly.yml`（D7）与 Phase 1 收尾残留。merge 策略（整支 merge 接受偏离 vs 拆分）待用户决策
- **graph / snapshot 为占位模块**：HNSW 核心算法（论文 Algorithm 1/2/4/5，§4/§6，含 shrink 逻辑消费 m_max0）归 Stage B；`save`/`load` 文件 API（§7）归 Stage C
- **pg-storage 依赖边缓至 M5**（P2-2 既定）：M4 直依赖冻结 {thiserror, crc32fast}，M5 WAL/buffer-pool 集成时才有真实消费者

---

## Stage B（M4）：HNSW 核心算法（graph.rs 算法主体 + 属性四件套 + 暴力对拍）

**状态**：✅ 完成（crate 71 测试双档全绿；clippy/fmt/doc 零警告；全量 workspace 回归见验收行；未 commit——等用户确认，message 前缀 `PHASE2-M4-StageB`）
**工期**：预估 4–6 天
**验收**：`cargo test -p pg-am-hnsw` **71 绿 / 0 失败**（Stage A 52 + B1 新增 12 + B2 新增 7）；`--release` 同绿（对拍套件 release 7.4s / debug 102.5s——release 为大图耗时口径）；已知小图逐步对拍全绿（8 节点手工推演 trace，含 shrink/遮挡/tie/断边逐步断言）；属性四件套全绿（矩阵见交付内容 3）；合成对拍全等（dim{2,16,128}×N{1k,10k} + 960 维冒烟）；simple vs heuristic A/B 开关可编译可运行（`new_with_neighbor_selection`，数据 Stage D 采）；clippy `-p pg-am-hnsw --all-targets -D warnings` 绿；fmt 绿；doc（`RUSTDOCFLAGS=-D warnings`）绿；`HashMap|HashSet|parking_lot` grep 算法路径零命中（唯一命中为模块文档禁令条文）；对抗审查一轮：**P1 零** / P2×1（文档侧，已修订）/ P3×3 / nano×2（见残留）

### 交付内容

1. **`graph.rs` 算法主体**（17 行占位 → 1168 行，实现 ~560 + 测试 ~600）：§6 SoA 布局（vectors 连续 arena / 每节点层级 / 逐层邻接表恒按 NodeId 升序 / entry_point / max_level）；`insert`（Algorithm 1：NodeId 稠密递增 + `next_level` 抽签 + 空图首节点成入口 + 逐层贪心下降 + 各层选择/双边连接/超限 shrink + 入口更新仅当新节点更高）；`search`（Algorithm 2/5：候选 min-heap `Reverse<Cand>` + 结果 max-heap，`Cand` 全序 = `(distance, NodeId ascending)`，break 条件距离-only 与论文一致；逐查询只校验 `ef ≥ k`，`ef=None` 走构造缺省）；`select_neighbors`（Algorithm 4：`extend_candidates=false` 全层 + `keep_pruned=true` 按距离升序补位；**选择侧与收缩侧同一函数**，`Cand::dist` 单点承载参考点角色——shrink 侧以 owner 向量重算距离）；visited = `Vec<u64>` bitset；入口校验 funnel 到 `metric.distance(v,v)` 自距离（复用 §5 冻结校验唯一实现，NaN/±inf/cosine 零向量/维度错全覆盖）；u32 NodeId 耗尽提前一格拒绝（保 INVALID 哨兵）
2. **A/B 对照开关**：`NeighborSelection { Heuristic, Simple }` + `#[doc(hidden)] new_with_neighbor_selection`（运行时构造参数，不进公开 API 契约——Stage D probe 同二进制环境变量切档用）；`Hnsw::new` 恒 Heuristic
3. **属性测试四件套**（`tests/hnsw_properties.rs` + `tests/common/mod.rs`，多 seed × 多 dim × 多 N 矩阵）：① 入口可达（默认参数区间 10 cell 全成立；**参数区间事实**入注释——极端参数 M=2/M_max0=2/ef_c=4 下 shrink 斩断桥接边，N=300 有向可达仅 3，另设钉死测试断言该已知行为）；② 不对称率口径建立（**默认参数聚合 15.35%，逐 cell 8.9%–17.1%**——M6 复用）；③ 层分布卡方（N=20k，M=4×2 seed + M=16×1 seed，尾箱按期望 <5 合并、df=bins−1、α=0.001 标准表，全过）；④ ef 单调性（k=10，ef 阶梯 {10..320}，3 cell × 64 查询，**聚合均值不降**口径）
4. **合成数据暴力对拍**（`tests/hnsw_bruteforce.rs`）：MixtureGen 高斯/均匀混合（复用 crate xoshiro，零新依赖）；`ef=节点数` flood 与暴力 oracle 全等（L2 主矩阵 + Cosine cell + k=10 前缀切片 + 960 维 N=1k 冒烟）；§8.3 诊断顺序（先连通性后距离）写入代码与注释
5. **对抗审查两轮 + 修复清零**：第一轮 P1 零（Algorithm 1/2/4/5 逐行无偏差——break 键、堆方向、shrink 参考点、下降起始层、M_max0/M_max 口径、keep_pruned 基准、入口更新时机逐项排除）；**P2-1（文档侧，已修订）**：M6「不对称率 <1%」字面口径被 ② 的实测基线证伪（shrink 单侧删边固有不对称，hnswlib/pgvector 同量级），ROADMAP Phase 2 验证标准 + ROADMAP-changes A5/§3.1 改**增量口径**，tech-selection §9 落盘基线，ROADMAP 债表 D10 登记清偿；**P3-1** 注释笔误（40→300 节点）已修；**P3-2** oracle 局限注记已落 `common/mod.rs`；**P3-3** visited 分配登记（见残留）。**第二轮（换攻击面：负距离/退化数据/极端参数/确定性缝隙/整数边界/Stage C 读面，行为 bug 仍为零）**：P1-1 `HnswParams` 字段私有化（pub 字段可经字面量/事后变异完全绕过 §4.2/§4.4 校验——与 m_max0 空白同类；16 处 getter 化，含首轮机械漏改的 9 处）；P3 手工推演注释块三处中间推理修正（期望表与代码均正确，错在推导文字：step 5 幻影候选 `(25,0)` 与遮挡归功、step 4 伪平局"钉死严格性"不成立、step 7 括号注自相矛盾）+ prop3 seed 201 卡方 17.516 登记补落测试注释；nano×6 全修（rng m≥2 release 行为入 doc / 入口追踪测试补入口身份断言 / simple 模式注释过强改写 / Cand 注释维度上界改述 / 卡方临界表覆盖界注记 / lib.rs 头注分期）。另：第二轮前的顺手修复——insert 去 clone（先连边后发布新节点邻接，语义等价论证入注释）、`into_sorted_vec` 化、邻接非扁平 CSR 登记（见残留首条）。**第三轮（beam 决胜序/快照层级校验/属性断言强度）**：P1 `search_layer` 准入改完整 (distance, NodeId) 决胜（满 beam 等距小 id 置换大 id worst——原距离-only 让平局席位取决于发现序；break 提前终止保持论文距离-only）+ 手工构造图边界测试（红→绿成立）；P2 §3 校验清单第 10 条补层级归属（level-L 边要求目标 `level_count > L`）+ 合法 CRC 越层级边负例——新校验当场抓获 encoding fixture 自身的语义非法（node 2 的 level-1 边指向只有 level 0 的 node 1，fixture 生而带病，随修）；P3×3：prop1 补 directed 断言（原名实不符：只断言 undirected）、prop2 加 [0.05, 0.30] 回归带守护 15.35% 基线（原 `missing <= total` 形同虚设）、A/B 开关 `#[doc(hidden)]` 措辞精确化（reachable but unsupported：下游可调但零稳定性保证）

6. **第四轮审查（快照良构性/读写对称/测试强度）+ 修复清零**：**P2-1** §3 校验清单补第 11 条——邻接表良构性（严格升序无重复无降序 / 无自环 / 度数 ≤ m_max(level)）：`push_edge` 的 binary_search 以规范序为前提，缺失时 Stage C 重建会静默插错位；实测 `[1,1,0]` 畸形流可干净通过旧 decode；**P2-2** 写入/读取对称——`validate_graph_data` + `SnapshotHeader::validate_construction_params` 提取为读写共用，encode 先验后写（原 encode 接受自己读不回的头部：`node_count:0, entry_point:7` 可正常编码、decode 才失败；负例 `encode_rejects_unloadable_header` 钉死）；**P3×3**：prop4 ef 阶梯补 64（§12 门槛值，原阶梯恰好跳过）+ 0.95 绝对下限（原只断言单调，均匀退化也能过）；对拍/属性的查询集改同分布（沿同一 MixtureGen 流续抽——另起生成器会重抽簇心，实测 recall@10 0.989 vs 1.000 @ ef=64）；IP 图层面覆盖补齐——`flood_search_ip_metric_respects_reachable_component`：ef=N 洪泛恰返回有向可达分量（1889/2000 钉死为区间观测，IP 非度量下部分可达合法）且逐位全等。四轮合计：P1×2 / P2×4 / P3 与 nano 若干，全部修复或登记清零。（过程如实登记：第四轮重构 `directed_reachable_count` 时曾引入 `.len()` 误用——计数恒等 node_count，被 prop1 极端参数钉死测试当场抓获（directed 300 > undirected 9 在数学上不可能），钉死测试正是为此而设；已修为逐位计数并复跑全绿）

### 与 pgvector·hnswlib 的 trade-off

| 维度 | pgvector / hnswlib | 本实现 | 取舍 |
|---|---|---|---|
| 排序决胜 | hnswlib 距离比较为主，并列行为实现定义 | 全链路 `(distance, NodeId ascending)` 全序（堆 Ord / 最终排序 / 补位） | §4.1 确定性三前提：同 seed 同插入序列 = 字节级同构图，测试钉死 |
| visited 结构 | hnswlib visited-list pool + 代际戳复用 | 每次 `search_layer` 调用分配 `Vec<u64>` bitset（1M 节点 = 125KB/次） | M4 无并发无复用需求，简单优先；Stage D 基准若显示分配可测再做代际戳复用（P3-3 登记） |
| 启发式开关 | hnswlib 无开关固定启发式 | `NeighborSelection` A/B 对照路径 doc-hidden 并存 | §4.3 A/B 实验用；不进公开 API，simple 路径随时可删 |
| 双向边不对称 | 同型固有（shrink 单侧删边），文档不承诺阈值 | 实测基线 15.35% 落盘，M6 验收改增量口径 | 口径冲突由 M4 实测提前引爆（P2-1），避免 M6 红灯误诊 |
| M_max0/M_max | hnswlib：select 侧 M、shrink 侧 M_max/M_max0 | 同语义全程统一（select 含第 0 层用 M，shrink 第 0 层 M_max0） | 忠实论文/hnswlib；审查逐项核对无偏差 |

### 已知残留与后续归队

- **邻接存储非扁平 CSR（第二轮 review D1 登记）**：`vectors` 是真连续 arena，但邻接为 `Vec<Vec<Vec<NodeId>>>`——每节点每层一个独立小堆分配（1M 节点 ≈ 百万级小 allocation），缓存局部性与建图耗时可能在 Stage D 的 1M benchmark 显形；届时用数据决定是否改扁平 CSR（offsets + 连续边数组），改动同时波及 §3 编码对应与 node_adjacency 读面，需过修订记录
- **visited bitset 每调用分配**（P3-3 登记）：insert 每层 O(N) 清零；release 全套 1.4s 实测无碍；Stage D 建图耗时进预算时改 `&mut` 复用 + 代际戳
- **暴力 oracle 经 `g.vector(id)` 取向量**（P3-2 登记，注记已落代码）：arena 索引 bug 会使对拍两侧同错（tautology 于向量内容；覆盖率/排序不受影响）；缓解件 = distance 已知答案测试 + 手工图独立 positions 对拍
- **nano×2 登记**：`search_layer` 多 entry point 初始 results 可超 ef 不修剪 + 重复 ep 重复入堆（当前唯一调用形态为单元素 slice，不可达；Stage C/D 若引入多入口调用需走同一 admission 路径）；`push_edge` 重复边 release 静默跳过（debug_assert 仅调试期，不可达防御）
- **prop3 seed 201 卡方统计量 17.516（p≈0.004）偏小概率侧**：α=0.001 下通过且确定性成立，已如实登记于测试注释
- **`search` 空图 + `k > ef` 返回 `Err("ef < k")` 而非空集**（2026-09-01 第三轮 review 登记）：graph.rs 先查 `ef < k` 再查空图，故文档"空图返回空向量"仅在 `ef ≥ k` 或 `k = 0` 时成立；语义自洽（`ef ≥ k` 是唯一逐查询不变式，先于空图校验生效），非 bug，属文档口径略含糊
- **`prop1_known_disconnect_at_extreme_params_pinned` 钉死精确计数（directed=3 / undirected=9）**（2026-09-01 第三轮 review 登记）：确定性强但脆弱，任何算法/PRNG 改动会击穿；意图（把 M=2 断裂固化为"已知行为"而非隐藏）正确，属可接受回归钉
- **Stage D 前置提醒（2026-09-01 第三轮 review 登记）**：`flood == brute-force` 对拍的前提是第 0 层连通；属性① 已把"全节点可达"如实收窄为默认/近默认参数区间事实，Stage D 的 recall harness 应保持"先验连通性、后验 recall"的诊断顺序（B2 已示范；R1 fallback 上调参数方向为增连通，无风险）
- **Stage C 需要 `pub(crate) fn from_parts(...)` 重建入口**（graph 私有字段对 snapshot 模块不可见）：Stage C 开工时加，B1 刻意未预埋
- **Cosine/IP 的 recall 质量验证归 M6**（§12 v1.2 既定）；M6 删除/并发/增量不对称率验收同归 M6
---

## Stage C（M4）：快照序列化 + 加载（snapshot.rs 文件 API + 往返等价 + 损坏文件套件）

**状态**：✅ 完成（2026-09-02；crate 123 测试双档全绿；workspace 868 绿 / 0 失败；clippy/fmt/doc 零警告；未 commit——等用户确认，message 前缀 `PHASE2-M4-StageC`；前置 Stage B commit 同样待确认；pg-engine F7 flake 修复拆独立 commit）
**工期**：预估 1–2 天
**验收**：`cargo test -p pg-am-hnsw --test snapshot_roundtrip` **40 绿 / 0 失败**（debug ~41s / release ~3.4s）；`cargo test -p pg-am-hnsw` 123 绿双档（75 lib + 3 bruteforce + 5 properties + 40 snapshot）；`cargo test --workspace` 868 绿 / 0 失败；clippy `-D warnings` / fmt / `RUSTDOCFLAGS=-D warnings` doc 全绿；无新依赖、无 HashMap/parking_lot/rand、无裸 `Snapshot` 类型（护栏 grep 零命中）；对抗审查**七轮**：第一轮 P2×1（内存峰值架构项）；第二轮 P1/P2 零；第三轮（用户外审）P1×2 + P2×4；第四轮（用户外审）P2×2 + P3×1；第五轮（用户外审）P1×1 + P2×1 + P3×1；第六轮（用户外审）P1×1 + P2×1 + P3×1；**第七轮（用户外审）口径×2**（均为安全方向、非行为缺陷），全部修复清零（见交付内容 4–11）

### 交付内容

1. **`snapshot.rs` 文件 API**（19 行占位 → 613 行）：`save(graph, path)`——levels 摘要（1B/node）+ `BodyEncoder` 逐节点流式直写（BufWriter + 增量 CRC32，两遍线性 O(n)），同目录 `.tmp-<pid>-<counter>` 临时文件（**`OpenOptions::create_new`**——O_EXCL 不跟随预置符号链接，撞名重试上限 1024，只清理自建文件）+ rename 原子发布（POSIX 替换语义）；**不 fsync**（M4 快照是 benchmark/reload 格式，持久化归 M5 WAL，rustdoc 写明）。`load(path, metric, ef_search_default)` / `load_with_budget(..., LoadBudget)`——**普通文件闸门**（FIFO/设备/目录响亮 `InvalidArgument`，根除 metadata 长度下溢 panic）+ **预算双闸门**（`max_file_bytes` 字节预算 + `max_memory_bytes` 内存预算——后者按 `encoding::max_memory_estimate` 的 cap 导出保守上界在物化前拒绝；`load` = unlimited 薄封装，可信基准便利；非可信来源必须用预算版）+ **六段校验序**（预读 29B 定长前缀 → header decode/参数重跑 → `ef_search_default` 提前校验 → node_count 体积交叉检查 **[min, max] 双侧**（min = 最小记录长除法，max = `max_records_size` 按第 11/12 条 cap 导出的 records-only 格式合法上限（第七轮口径修正：与 body_bytes 同口径，不再含 25B header——"声明小、实际大"的尾随垃圾不读就拒）→ 封顶全量读（`take(min(预算, 天花板)+1)`，封死 TOCTOU 拉大窗口）+ CRC + `decode_snapshot_body` 完整清单 → Cosine 零向量适配检查）→ 拆 SoA → `from_parts` 重建；**metric 不进快照（§3 冻结），调用方供给，错配静默改变语义但不可能 panic**（Cosine+零向量在 load 处响亮拒绝）；`load` 不设预算（可信本地基准便利封装），**非可信来源必须用 `load_with_budget`**——契约写入 rustdoc；decode 后立即 `drop(bytes)` 削 1× 文件峰值。模块文档新增**格式事实冻结声明**（Stage C 为第一个实际写盘生产者，§3 自此事实冻结，再改 = `FORMAT_VERSION` 升版 + 修订记录）与**威胁模型声明**（CRC 防 bit-rot 不防恶意）
2. **`graph.rs` 重建入口与只读契约**（1251 → 1356 行，纯增量）：`pub(crate) fn from_parts(...)`——Stage B 残留条"Stage C 需要 from_parts 重建入口"闭合（唯一调用点 `snapshot::load`；read_only=true、selection=Heuristic、rng 惰性 seed=0；仅廉价 debug_assert，真实校验全部由 decode 清单承载，证据链经审查逐项追过，release 下无缺失校验）；`read_only` 字段 + `insert` 单点把关 → `InvalidOperation`（全 crate `&mut` 路径仅 `insert`/`push_edge`/`shrink`，后两者私有且只被 `insert` 调用，单点即完备）；`pub fn is_read_only()`。`error.rs` 新增 `Io(#[from] std::io::Error)` 变体（Stage C 引入）
3. **测试套件 `tests/snapshot_roundtrip.rs`（40 枚）**：往返等价矩阵（三度量 × 3 seed × dim{2,16,128}，N≈1k + 自定义 params cell）——params/node_count/entry_point/max_level 全等 + 逐节点 vector（`to_bits` 位级，±0.0 可分）与 node_adjacency 全等 + 逐查询 `(NodeId, f64)` 序列全等（无 epsilon：同一冻结距离函数作用于位等 arena）；ef 真两档（每度量一个 cell 以 `ef_search_default=128` load）；边界形态空图/单节点/多层各一；只读契约；**`save(load(x)) == x` 字节级回归钉** + **v1 golden bytes 格式钉**（174B 固定 fixture，红 = 格式漂移必须升 FORMAT_VERSION）；损坏文件负例 15+ 条（位翻转/截断/短于前缀；合法 CRC 伪造：bad magic/version、参数越界、entry_point 越界、max_level 不符、空图非哨兵、flags/reserved 非 0、NaN/±inf、邻接五类畸形、level_count 超 64 cap（解析点前置拒绝）、node_count 巨大（长度交叉检查）、64MiB 尾随垃圾（max 侧不读就拒）+ **空图 + 1B 尾随的精确边界钉**（第七轮：records-only 天花板为 0，1 字节即越界，预读拒；旧口径下此用例走 ChecksumMismatch）、Cosine 零向量）——逐条 `matches!` 断变体 + 判别性消息子串，无 panic；decode 清单 12 项项项有负例；安全/并发：symlink 植入两枚、覆盖已存目标、失败无残留、I/O 错误分类（目录/FIFO/设备 → `InvalidArgument`，ENOENT → `Io`，截断 → `Corrupted`）、预算双闸门（字节 + 内存估计，探针复现）、8 线程并发 save + 并发 load（reader 活性 + **时间重叠证明**：Ok 返回点 `saves_in_flight > 0` 才计数——第七轮修正：采样严格在返回点、先于 `assert_identical`，断言期间才启动的 save 不计入重叠）
4. **对抗审查一轮 + 修复清零**（P1 零）：**P2-1** save/load 内存峰值 ~3× 文件（1M gist ~4GB 快照下 save 峰值 ~16GB，OOM 风险）——修复：encoding.rs 新增 `BodyEncoder<W: io::Write>` 流式编码器（增量 CRC + 4B 占位 + finish 时 seek 回填，盘上字节布局与 `wrap_crc32(body)` 完全一致 = 格式冻结不动），`validate_graph_data` 拆三个共享 helper（`validate_record_basics` / `validate_entry_and_max_level` / `validate_adjacency`），写读两路组合同一组实现——**单一编码器 + 单一校验实现**（Stage B「两侧同一函数」教训的同型纪律）：`encode_snapshot_body` 降为薄封装，新增单测断言流式输出 == 一次性输出、增量 CRC == 一次性 CRC；decode 行为一字未变（既有负例单测原样全绿为证；唯一行为细节：多重缺陷流的报错顺序可能前移，单缺陷语义与文案不变）；save 峰值降至 graph + 1B/node + 单条记录 + BufWriter 缓冲；load 侧做廉价一半（`drop(bytes)`），流式 decode 登记残留（见下）。**P3-3** 并发 save 同目标：临时名加进程内 `AtomicUsize` 计数，进程内并发安全（各自独立临时名，最后 rename 者赢），跨进程不保证入 rustdoc。**nano×3**：`.tmp-*` 崩溃残留入 rustdoc / `forged_max_level_mismatch` 测试 needle 改判别性子串（`"!= entry node's top level"`）/ "full validation checklist" 措辞对齐 decode-only 边界（magic/version/CRC/截断只在 decode 侧）
5. **主编排复核 + 对抗审查第二轮（均 P1/P2 行为缺陷零）**：主编排通读发现 **P3**——`BodyEncoder::push_record` 原不校验调用方 levels 摘要与实际记录的一致性（pub API 契约缺口：摘要撒谎会写出 decode 自己拒绝的 body，属第四轮"读写不对称"同型；两个现存调用点天然一致故不可达）——已修为真实校验（`neighbors.len() == levels[i]+1`，非 debug_assert）+ 负例 `body_encoder_rejects_summary_record_disagreement`（红→绿成立：初版负例误选本就 level-0-only 的 node 1 未触发，改对 node 0 撒谎后命中）。**第二轮（换攻击面：流式/decode verdict 差分 ×105 组缺陷、I/O 与文件系统边角实测、伪造头 OOM 探针、整数溢出路径、护栏与依赖冻结复跑）**：14 单缺陷 + 91 双缺陷组合 encode/decode 双路 verdict **105/105 一致全拒零发散**；I/O 边角（目录目标/缺父目录/只读文件覆盖/0644 权限与 pg-storage `write_atomic` 同口径/真实小卷 ENOSPC/`/dev/null`）全部响亮报错无 panic 无残留；`node_count=u32::MAX` 伪造头 25µs 内被除法上界拒绝（无乘法溢出路径、无 OOM）；仅余文档级——**P3** 归档计数滞后（97→98、842→843，即本条所在节）、**nano×3**（`saturating_sub` 注释机制描述被新一致性检查抢先、u32 守卫 "before→at" 措辞、并发读首次 save 见 ENOENT 补注）全部修复清零
6. **验证期抓获并修复一枚 Phase 1 存量 flake（与 Stage C 无关，如实登记）**：workspace 复跑中 `pg-engine --test m2b_index_txn` 的 `commit_failure_falls_back_to_abort_and_reclaims_xid` panic（"commit_txn failure hook already armed"）——F7 修复（Stage F）引入的进程级 `ARMED` 声明与同二进制内另一枚武装测试（`auto_commit_commit_failure_reclaims_xid`）在默认并行调度下竞态，latent 自 Stage F；修复：文件级 `Mutex` + RAII guard（`CommitFailHookGuard`，锁内武装、Drop 卸防、毒锁 `into_inner` 恢复）序列化两枚测试，15 连跑全绿；钩子的全局声明语义保留（仍防跨测试误武装）；**该修复与 Stage C 无关，commit 时拆独立提交**
7. **第三轮审查（用户外审）+ 修复清零**——两条 P1 均为**安全/健壮性级**，前两轮未覆盖这两个攻击面，如实记录：**P1-1** save 可预测临时名 + `File::create` 跟随预置符号链接 → 共享可写目录下任意文件覆写（外审探针实测 victim 被改）；修复：`OpenOptions create_new`(O_EXCL 绝不跟随/截断)+ 撞名重试上限 1024 + 只清理自建文件，cfg(unix) symlink 实测两枚（植入 513 个 symlink 全部存活、victim 逐字节不变；全区间植入 → 响亮 `InvalidOperation`)。**P1-2** L2 快照（合法含零向量）以 Cosine load → search 路径 `expect(ZeroVector)` panic——"arena 向量均过 metric 入口校验"不变式在 `from_parts` 重建路径缺失；修复：load 校验第 6 条（Cosine 逐节点自距离漏斗，响亮 `InvalidArgument`),metric 契约细化为"错配静默改变语义但**不可能 panic**"。**P2×4**:load 校验顺序重构为六段式（预读 29B 定长前缀 → header decode → **ef_search_default 提前校验** → node_count×最小记录长除法交叉检查 → 全量读 + CRC + 完整清单 → cosine 适配）——不可信大文件在 ef/长度检查处快速失败，不读 body；校验清单新增第 12 条 `level_count ≤ 64`（几何分布硬上界 53 @ M=2,64 双倍余量；掐灭"255 空层 × 24B Vec 头"的绝对值放大，病态形态 ~12× 比值系 SoA 逐层 Vec 固有，结构性修复 = 扁平 CSR，残留登记）;rename 覆盖语义平台口径入 rustdoc（POSIX 原子替换；Windows 目标已存在则 `Io` 失败——失败安全方向；M4 平台 = CI matrix)。**P3 批**：往返矩阵补自定义 params cell（防序列化硬编码）、**v1 golden bytes 钉**（174B 固定 fixture 逐字节常量，红 = 格式漂移必须升 FORMAT_VERSION)、`assert_identical` 位级化（`to_bits`,+0.0/−0.0 可分）、覆盖已存目标/失败无残留/8 线程并发 save+load 三枚测试。**文档同步修正**（本节前两条表述随第三轮证伪更新）："load 峰值 ≈2× 文件"改两段式（现实形态 ≈2× / 病态合法形态 ~12×,cap 后绝对值 ~1.5KB/node);"对抗审查两轮 P1/P2 零"改三轮实录
8. **第四轮审查（用户外审）+ 修复清零**：**P2-1** load 对剩余文件无上限 `read_to_end`（64MiB 尾随垃圾探针 RSS 峰值 ~68MB，稀疏超大文件可 OOM）——修复：**格式导出体积上限** `encoding::max_body_size`（按已验证 header 的 m/m_max0/dim/node_count + 第 11 条度数 cap + 第 12 条层级 cap 计算合法最大体积；超界即尾随垃圾，**不读 body 就拒**；合法性论证：encode 产出精确体积、decode 本就拒尾随字节，[min, max] 区间不缩小合法接受集，只把"读完再拒"提前为"不读就拒"）；实测：合法文件 + 64MiB 尾随 → 毫秒级 `Corrupted`，合法用例（含 golden、64 层 cap、27 cell 矩阵）无一误伤。**P2-2** level cap 校验过晚（decode_node_record 先按 ≤255 层分配嵌套 Vec，解完才查 64）——修复：cap 前移到解析点（读出 level_count 字节立即拒，零分配），与 `validate_record_basics` 的第 12 条构成"两道防线、一条规则、一个常量"。**P3** prefix `read_exact` 错误分类：`UnexpectedEof` 保持 `Corrupted`（截断），其他 kind（EISDIR/权限）归 `Io`（对齐 error.rs 声明；load(目录) 实测 `Io`）。**测试活性**：并发 reader 循环至至少一次 Ok（上限 10_000 次防死循环），消除"全程 ENOENT 空转通过"窗口。**文档措辞**：load 的 "single pass" 改"常数个串行线性遍（O(n) 总量）"（预读/decode/cosine 校验/拆分四遍）；本节前两条交付项的过时实现细节（`fs::read`/行数/测试数）随第四轮全部刷新
9. **第五轮审查（用户外审）+ 修复清零**：**P1** 非普通文件长度下溢 panic——FIFO/设备 metadata 长度为 0 但可读出前缀，原 `file_len - 29` 在 debug 下 `attempt to subtract with overflow`（违反"损坏输入不 panic"契约）；修复：open 后 `is_file` 闸门（响亮 `InvalidArgument`）+ `saturating_sub` 纵深，cfg(unix) mkfifo 探针实测 Err 不 panic（/dev/null 与目录随之归 `InvalidArgument`，ENOENT 仍 `Io`——分类变迁留痕测试注释）。**P2** 体积上限仍可由"声明大、实际真大"的不可信 header 放大（node_count=100k + 128MiB 稀疏文件探针 RSS ~135MB；极端字段组合上限 ~PB 级）——修复：`load_with_budget(path, metric, ef, max_bytes)` 调用方硬预算（读 body 前拒 + `take()` 封顶读），`load` 降为不设预算的薄封装（可信基准便利，威胁模型写明非可信来源必须用预算版）；探针复现用例（1MiB 预算拒 128MiB 稀疏文件）。**P3** 公共 codec 写读不对称：`encode_node_record` 原接受 65 层而 decoder 拒——补 `MAX_LEVEL_COUNT` 检查（"two guardrails, one rule, one constant"），负例单测钉边界（64 过 / 65 拒）。**测试**：并发读写改造为**保证重叠**（writer 不收工直到 reader 至少成功一次 Ok 且逐次全等，`total_saves ≥ 2` 断言重叠窗口内必有多次写）。**文档勘误**：golden 钉 174B（本节前记录误写 336B，外审核对抓获）、save "单遍" 改两遍线性、graph.rs 行数同步
10. **第六轮审查（用户外审）+ 修复清零**：**P1** 长度竞态二次下溢——读封顶路径仍有裸 `read_cap_total - 29`（陈旧 metadata / `max_bytes < 29` 触发）；修复：预算下限前置（< 29B 在 open 前拒）+ 全部长度运算抽纯函数（`body_len`/`read_cap`）全 saturating——单测先红后绿，还真抓到我方遗漏的一处 `29 + max_body` 在 u64::MAX 下的裸 `+` 溢出。**P2** 字节预算 ≠ 内存预算（合法 6.75MiB/64 层快照 RSS 90.1MiB，字节预算封不住 SoA 物化放大）——修复：`LoadBudget { max_file_bytes, max_memory_bytes }` 双闸门 + `encoding::max_memory_estimate`（cap 导出保守上界，物化前拒绝）；**口径决策（主编排接受，如实记录）**：估计按 64 层计费，1M gist 估 ~19.2GB ≈ 现实峰值 2.4×——预算闸门宁严勿宽，虚高项（逐层 Vec 头与邻接 id 按 cap 双计）注释写明，Stage D 若设内存预算按保守上界或 unlimited。**P3** codec 第三轮不对称：`encode_node_record` 接受 NaN 而 decoder 拒——补逐分量有限性检查（独立使用对称防线），NaN/±inf 负例钉死。**并发测试**：`total_saves ≥ 2` 不证明时间重叠——改 `saves_in_flight` 仪表 + "Ok 返回点仪表 > 0" 才计重叠，writer 收工以重叠发生为条件。长度算术纯函数化 + 边界单测（0/28/29/30/u64::MAX 组合全不 panic）
11. **第七轮审查（用户外审）+ 修复清零**——四条均为**口径/证明强度/登记类**问题，方向安全、无行为缺陷：**(a) 并发重叠证明弱于文档措辞**——`saves_in_flight` 采样点在 `assert_identical` 之后，断言期间才启动的 save 也会被计入重叠，与"Ok 返回点仪表 > 0"的声明不符；修复：采样严格前移到 load 返回 Ok 的瞬间（断言之前），测试注释同步。（b) **check 5 max 侧口径差 25B**——`max_body_size` 返回 header+records（`unwrap_crc32` 意义的 body），而 `body_bytes` 是 file_len − 29 的 records-only，比较跨口径使预读闸门松 25B（安全方向：多读 ≤25B 后被 CRC 拒，合法接受集不缩；但"精确格式上限"的声明不成立）；修复：encoding 新增 `max_records_size`（header-exclusive 对应物，pub + 单测钉 `max_body_size − 25` 与空图归零语义），check 5 max 与 `read_cap` 同用 records-only 口径（`read_cap` 本就自带 +29 前缀，此前等于 header 被双计）；空图 + 1 字节尾随的精确边界测试钉死（修复前 ChecksumMismatch、修复后预读 `Corrupted`，红→绿成立）。（c) **NeighborSelection 不进快照且残留未登记**——`from_parts` 硬编码 Heuristic（§3 格式无该字段）；今日无行为影响（只读图不走 selection），但开放续插后 Simple 建的图会静默按 Heuristic 续插——与 metric 残留同类；修复：残留清单"续插语义开放问题"条目扩为 seed/read_only/selection 三处改点 + from_parts rustdoc 留痕。（d) **save/load 目录分类不对称未写明**——load(目录) → `InvalidArgument`（is_file 闸门），save(目录) → `Io`（rename EISDIR）；均响亮，但 rustdoc 未解释；修复：save rustdoc 补分类说明（不对称是刻意的——原子替换协议不预检目标，预检即 TOCTOU 谎言），既有 `save_failure_leaves_no_temp_residue` 测试补"目录目标原样未动"断言。crate 121 → **123**（lib +1 单测、snapshot_roundtrip +1 边界测试），workspace 866 → **868**

### 与 pgvector·hnswlib 的 trade-off

| 维度 | pgvector / hnswlib | 本实现 | 取舍 |
|---|---|---|---|
| 快照格式 | hnswlib 内存布局转储（无版本化、无校验和，随版本漂移） | §3 自定义定宽格式 + CRC32 前缀 + 12 项 load 校验清单（第 12 条 level_count ≤ 64 为第三轮新增） | §7 既定：格式自研可控、坏文件响亮报错；对标走同数据集各自建图比 recall，不互换二进制 |
| 写盘方式 | hnswlib 一次性转储 | 流式编码（BodyEncoder）+ 临时文件原子 rename | 峰值内存 O(graph) 而非 O(3×文件)；不 fsync——持久化语义归 M5，快照只是基准/调试通道 |
| load 后续插 | hnswlib 允许（PRNG 状态丢失，图与未中断路径不同） | 保守拒绝（`InvalidOperation`） | coding plan 钉死：不在现场发明续插语义；开放需回推选型升版定语义（新 seed 续插 vs 显式拒绝） |
| load 内存峰值 | hnswlib 一次性读入 | NodeRecord 物化 + arenas：现实形态 ≈ 2× 文件；病态合法形态（dim 极小 + 大量空层）~12×（SoA 逐层 Vec 头固有，cap 64 后绝对值 ~1.5KB/node） | 廉价一半已做（drop 文件字节）；流式 decode + 扁平 CSR 归 Stage D 评估（见残留） |

### 已知残留与后续归队

- **load 内存峰值：现实形态 ≈ 2× 文件，病态合法形态 ~12×**（review P2-1 剩余半边 + 第三轮 P2-2 口径修正）：`decode_snapshot_body` 物化全部 NodeRecord 再拆 SoA；1M gist 快照（~4GB）属现实形态，峰值 ~8GB 量级；病态形态（dim=1 + 多空层）的放大来自 `Vec<Vec<Vec<NodeId>>>` 逐层 Vec 头（24B/层 vs 盘上 2B/层），校验第 12 条（level_count ≤ 64）已把绝对值封在 ~1.5KB/node，比值不变——**结构性修复 = 扁平 CSR 邻接 + 流式 decode，归 Stage D 跑批前评估**，连同 §11 R3 ~4GB 内存门槛与跑批机器规格一并定夺
- **load 威胁模型声明**（第三轮 P2-1 登记）：CRC 防 bit-rot 不防恶意篡改，load 假设非对抗来源；对抗加固（大小预算 / 流式校验）归后续需要时再立
- **pg-storage `io.rs::write_atomic` 同款 `File::create` 可预测临时名模式**（第三轮 P1-1 的外溢登记）：crash 路径语义（崩溃残留 tmp 的存在性假设）需单独评估，未随 Stage C 修改；归 Phase 7a 加固专项评估
- **续插语义开放问题**（tech-selection v1.6 待决）：load 后图只读；若未来开放，改点为 `from_parts` 的 seed=0、`read_only`、`selection` 三处（2026-09-02 第七轮补登记：**NeighborSelection 不进 §3 快照格式，`from_parts` 硬编码 Heuristic**——今日无行为影响（只读图不走 selection），但开放续插后 Simple 建的图会静默按 Heuristic 续插，与 metric 不进快照的残留同类，续插语义定夺时必须一并裁决：格式升版携带 selection vs 显式拒绝）
- **1M load < 5 分钟（§12 口径，含校验）未实测**：归 Stage D 跑批；代码路径无已知超线性项（save 两遍线性——levels 摘要 + 流式编码，O(n)；load 为常数个串行线性遍——预读/decode/cosine 校验/拆分，总量 O(n)）
- **`.tmp-*` 崩溃残留**：进程在 create 与 rename 间崩溃留下临时文件，下次运行不自动清理（无害，rustdoc 已写明清理模式）
- **跨进程并发 save 同目标不保证**（进程内已安全）：调用方串行化，rustdoc 已写明
- **metric 不进快照 = 静默错配不可检出**（§3 冻结的固有代价，rustdoc 与交付项 1 已声明，此处补登记）：load 的 `metric` 入参与建图不一致时，快照没有任何 bit 可检出——距离语义静默改变，recall 数字静默作废。**Stage D 的 recall harness / benchmark 加载器必须在调用点钉死 metric**（每个数据集固定映射：sift/gist → L2)，与"先验连通性、后验 recall"同级的诊断纪律；M5 若把 metric 落入 catalog/reloptions，此残留自然消解（格式不动，metric 是运行时属性）
