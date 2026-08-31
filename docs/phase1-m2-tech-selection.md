# Phase 1 M2 技术选型

> 承接 M1 物理层（Page/WAL/BufferPool/LSN/Checkpoint），本文档定义 M2 阶段
> （Heap + MVCC + B+Tree + ARIES 变体崩溃恢复）落地前所有跨模块的技术选择。
>
> 目标：所有影响 on-disk 格式、WAL 语义、跨模块契约的决策先敲定；每个选择给
> "选项 → 选择 → 理由 → 代价"，与 M1 tech-selection 风格保持一致。
>
> **v2.3 修订**：在 v2.2 之上，追加**八轮 review 反馈，共 35 条编号修订**（v2.3-1..v2.3-35），
> 按内容归为四大类：
> (a) 清理 3 处 `AbortedXidSet` 遗留（改 `ClogAccessor`）、M2a in-memory CLOG **禁止清理
>     ABORTED**、引入 `t_cid` + `Snapshot.curcid` 消除"同事务 UPDATE 后 SELECT 返回双行"
>     缺陷、Stage 0b 并行边界明确化（`RedoHandler`/`ClogAccessor` trait 前置到 0a）。
>     — v2.3-1..v2.3-4（第四轮）
> (b) 修正 §6.2 CLOG 段容量 512M→268M、§10.1 WAL discriminant 与 M1
>     `record.rs` 对齐（`FullPageImage=10 / CheckpointBegin=30 / CheckpointEnd=31` +
>     M1 已预留的 Phase 2/3 值）、Superblock v1→v2 布局与 M1 `superblock.rs` 实际 encode
>     对齐（`next_oid` 于 offset 40..48）、`BTreeSplitPrepare=5` 显式声明为 M1 `BTreeSplit=5`
>     的重命名。 — v2.3-5..v2.3-8（第五轮）
> (c) 与 M1 现状对齐 5 处描述失准（§0 债务 #1 group commit / §2 `pd_lsn` 方向 / §2
>     PageHeader 26→**32** 字节 / §11.5 WAL-before-data / §0 债务 #8 只缺 clog/）；§1
>     Oid、§6.1 TxnIdClock 明确标注为 M2 新增；补齐 §11.4 CheckpointEnd v1/v2 payload
>     迁移、§19 M2a 100 线程并发压测、§10.1 BTreeSplitCopy/Commit "Stage 0 追加" 标注、
>     §2 `pd_checksum` 保留理由；澄清 §7.1 curcid 时机、§7.2 Uncertain M2a 死码、§6.4
>     CLOG 持久化时机去重、§20 验证计划分类 C/P/S、`RedoRegistry` duplicate panic 约束、
>     `ClogBuffer` 8 帧 rationale；补 Q5 debt 交叉索引表；第七轮追加历史参考表同步
>     （附录 C S1 / S6 / M13、§21 兼容性承诺与内部接口表）与前一轮自引入不一致修正
>     （§6.3 CLOG Flush 改为引用 §6.4；t_cid 论据从"8B 对齐"改为"迁移代价 + memcpy/SIMD
>     友好"）。 — v2.3-9..v2.3-26（第六轮）+ v2.3-27..v2.3-32（第七轮）
> (d) 全文重读发现的三处遗留内部不一致：§2 slotted page ASCII 图 26B/2B/offset-28 与
>     正文 32B 结论对齐；§10.2 WAL record 版本号"0 = M2 v1"与 §11.4"0 = M1 legacy /
>     v1"相互矛盾，统一为 §11.4 语义；§7.2 M2a 简化段"self_xid 全部可见 (xmin case)"
>     与算法反向，改为说明 M2a 走 dead code path。 — v2.3-33..v2.3-35（第八轮）
> 见附录 C。
>
> **v2.2 修订**：在 v2.1 之上，追加 P0/P1/P2 review 反馈：is_visible 对 `xmax == self_xid`
> 修正为"仍见"、`AccessMethod::insert` 返回 `Result<()>` 由 out_tid 回填、M2a 用
> in-memory `ClogAccessor`（取代 AbortedXidSet）、Stage 0 拆 0a/0b、M2a 程序化 API、
> Context 结构定义、`BTreeSplitCopy` payload 极简化等。见附录 C。

---

## 0. Stage 0：M1 遗留技术债

M1 code review 沉淀的硬约束。M2 主线开工前一次还清，避免每个 stage 反复被
同一批债务牵制。

| # | 债务 | M2 影响 | 修复方案 |
|---|------|---------|---------|
| 1 | `WalWriter::append()` 内部隐式 `flush_to`（M1 `writer.rs:184`），上层无法 batch WAL + commit | M2 一组 redo + commit 记录无法攒批，事务提交延迟放大 | 让 `append()` 不再隐式 `flush_to`，上层 `BufferPool` / `PageAllocator` / 事务提交路径显式调 `flush_to(commit_lsn)` 触发 group commit。M1 的 group commit worker（`writer.rs:135` + `wal_group_commit_batch_size` / `wal_group_commit_timeout_ms`）与 `flush_to(lsn)` API（`writer.rs:207`）已存在，Stage 0 只改 `append` 签名，不重写 group commit 逻辑。同时新增 `LsnClock::reserve(size)` 显式占位 API（M1 只有 `next(size)`，`lsn_clock.rs:43`，且 `assert!(size > 0)`），用于债务 #3 checkpoint FPI race 修复。 |
| 2 | `FreelistMeta` 无 CRC + 损坏静默返回空 | M2 有 `free_page` 真实数据，静默丢弃 = page id 重用 = 数据损坏 | 头部加 CRC32；`read()` 损坏时返回 `MetadataCorrupted` 硬失败；`recover()` 从 WAL 的 `PageFree` 记录重建 freelist（超级块的 freelist 只是加速快照）。 |
| 3 | Checkpoint FPI race window | ARIES Redo 依赖 FPI 完整性；race 会导致 redo 后页面仍是脏的 | 用新的 `LsnClock::reserve(size)` 预留 begin_lsn：`checkpoint_lsn = LsnClock::reserve(CHECKPOINT_BEGIN_SIZE)` → `BufferPool::set_checkpoint_lsn(begin_lsn)` → 才 emit `CheckpointBegin` 记录写入该已占位的 LSN。窗口消失。 |
| 4 | Recovery `apply_record` 硬编码 match | M2 新增 8+ 种 redo 记录类型，match 会爆炸；跨 AM 复用困难 | 引入 `RedoHandler` trait（详见 §11.6）。`StorageEngine::recover` 持有 `HashMap<WalRecordType, Box<dyn RedoHandler>>`，各 AM 在 `Engine::open` 时**一次性**注册；每条 record 只匹配一个 handler；未注册的 record type 硬失败（避免静默丢 redo）。 |
| 5 | `Mutex<File>` 串行化磁盘 I/O | M2 并发 CRUD 目标 TPS ≥ 10K，数据文件锁是硬瓶颈 | 改用 `std::os::unix::fs::FileExt::{read_at, write_at}`（Windows fallback 用 `seek_read/seek_write`）。`data_file: Arc<File>` 无锁并发。 |
| 6 | Superblock 缺 `next_oid` | M2 catalog OID 分配需要持久化 | Superblock v1→v2 迁移：在 M1 已有的 `checkpoint_lsn(16..24) / next_page_id(24..32) / next_txn_id(32..40)` 之后插入 `next_oid: u64` 于 offset **40..48**（占用原 M1 `created_at` 位置）；`created_at` 后移到 48..56，CRC32 后移到 56..60，reserved 区域收缩为 60..512。写入路径复用双副本 + CRC 机制。M1 → M2 迁移一次：读到 v1（version=1）时按老 layout 解析、`next_oid` 初始化为 `16384`（PG 用户 OID 起点）、`created_at` 从 40..48 搬到 48..56，再以 v2 布局写回。 |
| 7 | `WalRecordType` 未预留 M2c/CLR discriminant | Stage 0 后每次新增 record type 都要动 enum 内存布局；混合旧新数据文件时可能冲突 | Stage 0 一次性在 `WalRecordType` enum 中**保留占位**：`HeapInsert=1`、`HeapUpdate=2`、`HeapDelete=3`、`BTreeInsert=4`、`BTreeSplitPrepare=5`（**复用 M1 已保留的 `BTreeSplit=5`，仅重命名**）、`BTreeDelete=6`、`HeapHotUpdate=7`、`HeapCleanup=8`、`TxnBegin=20`、`TxnCommit=21`、`TxnAbort=22`、`BTreeSplitCLR=50`、`BTreeSplitCopy=51`、`BTreeSplitCommit=52`、`PageFree=41`。M1 已有值 `FullPageImage=10`、`CheckpointBegin=30`、`CheckpointEnd=31`、`PageAlloc=40`、`Logical*=100..103`、`Segment*=110..111` 保持不变，Stage 0 只追加、不重编号。M1 recovery 遇到保留但未注册的 record type 硬失败（不会误 replay）。 |
| 8 | `ensure_data_dir` 未创建 `clog/` 子目录 | M2 CLOG 段文件目录不存在（M1 `io.rs:26-33` 已创建 `data/wal/meta/tmp` 四个子目录，缺 `clog/`） | `io::ensure_data_dir` 追加 `clog/` 子目录（幂等）。其余子目录 M1 已就绪。 |

**Stage 0 交付物**（v2.2 修订 P1-4：拆两期，工期 3–4 周合计）：

- **Stage 0a**（1.5–2 周，**阻塞 M2 主线**）：
  - 债务 #1（`WalWriter::append/flush_to` 拆分 + `LsnClock::reserve` 新 API）
  - 债务 #3（Checkpoint FPI race 修复；依赖 #1）
  - 债务 #6（Superblock v1→v2 迁移，`next_oid` 字段）
  - 债务 #7（`WalRecordType` 一次性预留所有 M2 discriminant）
  - 债务 #4a（`RedoHandler` trait + `RedoRegistry` + `ClogAccessor` **trait 定义**）
    —— v2.3 前置：trait 是纯接口，M2a 编译依赖它。
    **Q1 位置说明**：`ClogAccessor` trait 放在 `pg-storage::clog` 模块（不是 `pg-txn`），
    因为 `RedoContext` 在 pg-storage 里持有 `&dyn ClogAccessor`；具体实现 `ClogBuffer`
    才在 `pg-txn`（详见 §11.6 crate 依赖问题）。Stage 0a 只落 trait 定义 + doc comment，
    Stage 0b 交付 `NoOpClogAccessor`（M1 空实现），M2b 由 `pg-txn` 交付真实 `ClogBuffer`。
  - 债务 #8（`ensure_data_dir` 追加 `clog/` 子目录）
  - 交付：接口 / 保留位 / 目录都到位，M2a 可以基于稳定 API 开工

- **Stage 0b**（1.5–2 周，**可与 M2a 部分并行** —— 详见并行边界）：
  - 债务 #2（`FreelistMeta` CRC + WAL 重建）
  - 债务 #4b（`NoOpClogAccessor` M1 空实现 + `RedoRegistry` 注册到 `Engine::open`）
  - 债务 #5（`Mutex<File>` → `read_at`/`write_at`）
  - 交付：M1 全部集成测试 + crash_recovery 自动化 1000 轮通过

**v2.3 修订**：`RedoHandler` trait + `ClogAccessor` **trait 定义**（纯接口，无依赖）
从 0b 提前到 0a；Stage 0b 只负责 `NoOpClogAccessor` 具体实现与 `RedoRegistry` 装配。
这样 M2a 的 `TxnManager` / Visibility Oracle 从第一行代码起就能编译。

**Stage 0b 与 M2a 的并行边界**（v2.3 明确）：
- ✅ **可与 M2a 完全并行**：
  - Slotted Page + Tuple 编解码（`pg-am-heap` 页内逻辑）
  - Catalog bootstrap（`pg-catalog` 硬编码系统表定义）
  - Heap AM 页面级操作（INSERT/DELETE/UPDATE 修改页字节）
  - Heap redo handlers 骨架（编译时依赖 `RedoHandler` trait，已在 0a 就绪）
- ⚠️ **必须等 0b 交付后**：
  - `TxnManager` + 事务型 redo handler 的**集成测试**（依赖 `NoOpClogAccessor` 或
    M2a in-memory `ClogAccessor` 通过统一 trait 装配到 `RedoContext`）
  - Freelist 相关的**并发压测**（依赖 0b 的 CRC + `read_at/write_at`）
- 若 0b 阻塞，M2a 前 60% 工作量仍可推进；只有最后集成阶段需要 0b 就位。

**Stage 0 与 M2 主线的边界**：Stage 0 只做接口/行为改造与保留位，不加新功能；不引入
tuple、事务、B+Tree 等 M2 概念。Stage 0a 完成后打 tag `phase1-m1-debt-clean-0a`，
0b 完成后打 `phase1-m1-debt-clean`。

**Q5 债务交叉索引**（v2.3 新增，便于顺着 debt # 定位相关文档节）：

| Debt # | Stage | 关联章节 | 关键类型 / API |
|--------|-------|---------|---------------|
| 1 | 0a | §11.5 WAL-before-data；§10.2 编码 | `WalWriter::{append, flush_to}`、`LsnClock::reserve` |
| 2 | 0b | §12 Checkpoint（间接：freelist 归属 superblock） | `FreelistMeta` CRC + WAL 重建 |
| 3 | 0a | §11.4 Checkpoint；§11.5 | Checkpoint FPI race（先 reserve lsn 再 emit） |
| 4a | 0a | §11.6 RedoContext；§7.2 Oracle；§14 AM trait | `RedoHandler` trait、`RedoRegistry`、`ClogAccessor` trait |
| 4b | 0b | §11.6；§19 M2a | `NoOpClogAccessor`、`Engine::open` 装配 |
| 5 | 0b | §5 存储 I/O（隐含） | `File::{read_at, write_at}` |
| 6 | 0a | §12 Superblock 布局 | Superblock v1→v2 `next_oid` 迁移 |
| 7 | 0a | §10.1 WAL Record 清单 | `WalRecordType` 全枚举一次性保留 |
| 8 | 0a | §6.3 CLOG 段文件 | `io::ensure_data_dir` 补 `clog/` |

---

## 一、Workspace / Crate 划分

**背景**：M1 所有代码都在单一 `pg-storage` crate 内。M2 引入 heap、事务、B+Tree、
catalog 后，若继续膨胀会导致：（a）编译时间线性增长；（b）AM 边界不清晰，未来
Phase 2 HNSW / Phase 3 Inverted 无法平行开发；（c）测试互相干扰。

**选择**：拆分为 6 个 crate（+ M1 已有 `pg-storage`）。

| Crate | 职责 | 依赖 |
|-------|------|------|
| `pg-storage` | Page/WAL/BufferPool/LSN/Checkpoint（M1 全部）+ Stage 0 债务修复 + 通用类型 `Tid`/`PageId`/`Lsn` | — |
| `pg-txn` | XID 分配、事务状态、CLOG、Snapshot、Visibility Oracle、Lock Manager | `pg-storage` |
| `pg-catalog` | 系统表定义、bootstrap、schema 描述、relation 元数据；`AccessMethod` / `UpdatableAM` trait 定义 | `pg-storage`, `pg-txn` |
| `pg-am-heap` | Heap AM：slotted page tuple 编解码、TOAST、Heap redo handler；实现 `AccessMethod + UpdatableAM + Vacuumable` | `pg-storage`, `pg-txn`, `pg-catalog` |
| `pg-am-btree` | B+Tree AM：latch coupling、split/merge、`AccessMethod` 实现（不含 `UpdatableAM`） | `pg-storage`, `pg-txn`, `pg-catalog` |
| `pg-engine` | 顶层组装：`Engine::open` 装配 storage + txn + catalog + AMs，注册 redo handlers，暴露 CRUD API | 以上全部 |

**Tid 类型放在 `pg-storage`**（v2 修订）：`Tid = (PageId, u16)` 是纯物理定位，与 heap
语义无关；B+Tree 叶子存 Tid 是完全合法的，因此 `pg-am-btree` 不需要依赖 `pg-am-heap`。
两个 AM crate 平级。

- **内存表示**：`#[repr(C)] struct Tid { page: PageId, slot: u16 }` — 10 字节
- **磁盘/tuple 头表示**：12 字节（含 2 字节 padding，满足 8 字节对齐；见 §3 t_ctid 布局）
- **Oid 类型放 pg-storage**（v2 修订 P2-6；v2.3 澄清 M2 新增）：M1 现有物理类型仅
  `PageId / Lsn / TxnId / FrameId / Tid`（`types.rs`），**M2 新增** `pub struct Oid(pub u64);`
  与 `PageId / TxnId` 平级，作为 relation / type / AM 等系统对象的稳定标识；
  `TableOid` / `TypeOid` 等 newtype 别名在 pg-catalog 定义。`pg-txn::LockManager` 直接
  引用 `pg_storage::Oid` 作为 `TableOid`，避免 pg-txn → pg-catalog 依赖

**AccessMethod / UpdatableAM trait 放在 `pg-catalog`**（而非 `pg-storage`）：AM 需要
知道 relation schema 才能编码/解码 tuple；把 trait 放 catalog 层可以避免 pg-storage
反向依赖。详见 §14。

**理由**：
- AM 边界即 crate 边界，Phase 2 HNSW 直接以新 crate `pg-am-hnsw` 加入
- Heap 和 BTree 平级，任一 AM 可独立演化
- 每个 crate 独立单元测试 / clippy / doc 阈值
- 事务与 AM 解耦：`pg-txn` 不知道 heap/btree 的存在，只提供 `is_visible(xmin, xmax, snapshot) -> bool`

**代价**：
- workspace 结构变复杂，Cargo.toml/CI 需要相应改动
- Stage 0 期间要把 M1 的 `page_allocator.rs` / `buffer_pool.rs` API 稳定下来（后续 crate 依赖它们），API 大改成本高

---

## 二、Slotted Page 格式

**背景**：M2 需要在 8KB page 内存放多条变长 tuple，支持插入/删除/UPDATE-in-place
（HOT 更新）而不 rewrite 整页。

**选择**：Slotted Page（PG 启发的 line pointer 数组 + 向上生长的 tuple 数据）。

```
┌───────────────────────────────────────────────────┐
│ PageHeader (32 bytes total = 26 字段 + 6 padding) │
│   pd_lsn:              u64  offset 0              │
│   pd_checksum:         u32  offset 8              │
│   pd_flags:            u16  offset 12             │
│   pd_lower:            u16  offset 14  (LP 末尾)  │
│   pd_upper:            u16  offset 16  (tuple 起) │
│   pd_special:          u16  offset 18  (AM 私有)  │
│   pd_pagesize_version: u16  offset 20  (0x0001)   │
│   pd_prune_xid:        u32  offset 22  (HOT 用)   │
│   -- padding to 8-byte align (6 bytes) --  offset 26..32 │
├───────────────────────────────────────────────────┤
│ LinePointerArray (向下生长，从 offset 32 起)         │
│   LP(1) LP(2) LP(3) ...                            │
│                                                    │
│   每个 LP = 32 bits:                                │
│     bits 0..14   lp_off:   u15 (offset in page)    │
│     bits 15..16  lp_flags: u2                      │
│                    00 UNUSED  01 NORMAL            │
│                    10 REDIRECT (HOT)  11 DEAD      │
│     bits 17..31  lp_len:   u15 (tuple length)      │
├─────────── free space ────────────────────────────┤
│ (grows both ways: LP ↓, tuples ↑)                  │
├───────────────────────────────────────────────────┤
│ TupleData (向上生长)                                │
│   Tuple(N) ... Tuple(2) Tuple(1)                   │
├───────────────────────────────────────────────────┤
│ Special Space (可选，AM 自定义；B+Tree 用 16B)       │
└───────────────────────────────────────────────────┘
```

**PageHeader 大小说明**（v2.3 修订 P0-4）：字段合计 26 字节；为满足 tuple payload
的 **8 字节对齐**（tuple header 内含 `t_xmin: u64` 等 8 字节字段，起点必须 8-align），
实际 header 区占 **32 字节**（26 + 6 字节 padding）。M2 起 `pd_lower` 初始值 = 32，
LP 数组从 offset 32 起（4 字节对齐 ✅），tuple payload 起点也是 32（8 字节对齐 ✅）。
v2 早期版本曾写 28 字节；28 = 3.5×8 无法满足 tuple 8-align，会导致 `read_unaligned` 或
运行时 panic，v2.3 修正为 32。若未来发现 header 增长压力，Phase 7 再考虑
是否把 `pd_pagesize_version` 压入 `pd_flags` 高位。

**`pd_lsn` 权威性契约**（v2 修订 P1-2，v2.3 修订方向）：
- `pd_lsn` 是 **M2 新引入的 page 内字段**（offset 0..8，slotted page 的 PageHeader 起始 8 字节）。
- **M1 现状**：M1 page 是 8KB 纯字节 buffer，页内**没有** `pd_lsn` 字段；
  frame metadata 中的 `page_lsn`（`buffer_pool.rs:63`）是 M1 唯一的 page LSN 来源。
- **M2 契约**：引入 `pd_lsn` 后，`page[0..8]` 是**权威源**，frame metadata `page_lsn`
  改为只读缓存（从 `page[0..8]` 加载，避免双写不一致）；有条件时可考虑删除 frame
  metadata 字段直接由 helper 读页。
- **写入路径**：AM 每次修改页后**必须**在同一 latch 保护下写 `page[0..8] = record.lsn`
  （或 `record.lsn.max(old_pd_lsn)`），然后 `buffer_pool.mark_dirty(page_id, record.lsn)`
  维护 DPT。
- **FPI 判定**：checkpoint / recovery 逻辑读 `page.pd_lsn`（不再走 frame 缓存），与
  checkpoint_lsn 比较决定是否 emit FPI。
- **redo 幂等判定**：`handler.apply` 内部读 `page.pd_lsn` 与 `record.lsn` 比较（≥ 则跳过）。
- Stage 0 单元测试固化：`test_pd_lsn_authoritative`（任意 mutation 后 `frame.cached_lsn`
  与 `page.pd_lsn` 必须相等；不等即 assert）。

**`pd_checksum` 状态**（v2 修订 P2-1；v2.3 补充 P1-7 保留字段理由）：M2 写入时**恒填 0**
（不启用页 checksum，避免 hint bit 回写与 checksum 冲突）；**字段仍保留在 header 里**（占
offset 8..12 共 4 字节），不删除的理由：
1. **on-disk 兼容**：Phase 7 启用页 checksum 时无需改 PageHeader 布局、无需 v1→v2 迁移；
2. **对齐需要**：删除后 header 从 32 → 28 字节反而破坏 tuple 8 字节对齐（需重新加 padding）；
3. **debug 用途**：M2 阶段仍可离线工具计算并对比 checksum，只是 runtime 不校验。

Phase 7 启用时的切换点：runtime 读 page 时若 `pd_checksum != 0` 则做校验，否则跳过；
写路径改为始终计算并回写。hint bit 例外（如"计算 checksum 时把 infomask 的 hint 位掩为 0"）
在 Phase 7 与 checksum 一起引入。

**`pd_flags` 位分配**：
- bits 0..7：heap 页在 M2 全部保留（未使用）；M3 vacuum 可能启用
- bits 8..11：仅 B+Tree 页使用（`btpo_level`）
- bits 12..15：仅 B+Tree 页使用（`btpo_flags`：LEAF/ROOT/DELETED/SPLIT_INCOMPLETE）

**关键约束**：
- LP 数组只增不删（删除 = 标记 UNUSED/DEAD，位置可复用）→ TID 稳定
- `lp_off` u15 上限 32K，对 16K page 也够用
- `pd_prune_xid`：M2 保留字段，实际 HOT prune 逻辑推到 M3 vacuum

**理由**：
- TID = `(PageId, slot_id)` 稳定，B+Tree 索引可以放心存 TID
- LP 增删轻量，UPDATE 路径可以走 HOT（新 tuple 同页，旧 LP 转 REDIRECT）—— M2c 上
- 与 PG 语义一致，便于对照实现和调试

**代价**：
- LP 数组本身占空间（每 tuple 4 字节），密集小 tuple 场景内存开销 5%+
- HOT 逻辑复杂，M2a 先按 non-HOT 实现（UPDATE = insert new + old xmax）

---

## 三、Tuple 格式（胖 header）

**背景**：ROADMAP 明确 M2 tuple header 携带 `xmin, xmax, agent_id, trace_id, flags`。
这是 Agent-Native 数据库的核心特征——**每一行数据都自带 agent 归属和调用链**。

**选择**：64 字节固定 header + null bitmap + 定长/变长列 payload。字段顺序按
8 字节对齐排布（v2 修订）。

```
Tuple layout (M2)  —— 所有偏移 8 字节对齐
┌───────────────────────────────────────────────────┐
│ TupleHeader (64 bytes)                            │
│   t_xmin:      TxnId    u64      offset  0..8     │
│   t_xmax:      TxnId    u64      offset  8..16    │
│   t_agent_id:  u64               offset 16..24    │
│   t_trace_id:  [u8; 16]          offset 24..40    │
│   t_ctid:      Tid      12 bytes offset 40..52    │
│                (PageId u64: 40..48; slot u16: 48..50; pad u16: 50..52) │
│   t_infomask:  u16               offset 52..54    │
│   t_infomask2: u16               offset 54..56    │
│   t_hoff:      u16               offset 56..58    │
│   t_flags:     u16               offset 58..60    │
│   t_cid:       u32               offset 60..64    │  (v2.3: command id)
├───────────────────────────────────────────────────┤
│ NullBitmap (可选，1 bit/attr，向上取整字节)         │
├───────────────────────────────────────────────────┤
│ Attribute Data (8-byte aligned start)             │
│   - 定长列按 schema 顺序连续存放                    │
│   - 变长列：4 字节 varlena header + 数据           │
│   - > 2KB 的变长值走 TOAST 溢出页                  │
└───────────────────────────────────────────────────┘
```

**M2 null bitmap 语义（与 PG 相反，有意为之）**：bit i = 1 表示 column i **是 NULL**；
PG 惯例是 bit i = 1 = NOT NULL。M2 格式与 PG 不 dump 兼容（64B vs 23B header），
自洽即可；后续 dump 工具需自行反相。写 redo handler / 工具时不要按 PG 习惯想当然。

**t_infomask bit 定义**：
```
HEAP_HASNULL         0x0001
HEAP_HASVARWIDTH     0x0002
HEAP_HASEXTERNAL     0x0004  (TOAST)
HEAP_XMIN_COMMITTED  0x0100  (hint)
HEAP_XMIN_INVALID    0x0200  (hint)
HEAP_XMAX_COMMITTED  0x0400  (hint)
HEAP_XMAX_INVALID    0x0800  (hint)
HEAP_UPDATED         0x2000
```

**t_infomask2 bit 定义**：
```
natts:                 bits 0..10  (11 bit，最多 2047 列)
HEAP_KEYS_UPDATED:     0x2000
HEAP_HOT_UPDATED:      0x4000
HEAP_ONLY_TUPLE:       0x8000
```

**t_flags** 保留字段：低 4 位 = tuple 编码版本号（M2 = 0）；高 12 位 M3+ 用。

**t_cid**（v2.3 新增）：命令序号，语义：
- INSERT：写入 `= 当前语句的 curcid`（M2b 每语句 +1）；xmin=self 判定时用作 cmin
- DELETE：写入 `= 当前语句的 curcid`；xmax=self 判定时用作 cmax
- UPDATE：new tuple cid = curcid（作 cmin）；old tuple cid = curcid（作 cmax）
- M2a：单语句 auto-commit，curcid 恒为 0，`t_cid` 也恒为 0（不影响可见性判定）
  - **P2-3 讨论**：M2a 理论上可省 4 字节/tuple（不写 t_cid，header 60B）。**放弃理由**：
    (1) 引入 M2a→M2b on-disk 迁移逻辑（旧 tuple header 60B 缺 t_cid 字段，需扫表重写或
    加 header 版本位）；(2) tuple header 64 字节正好 8 字节对齐，便于 memcpy / SIMD /
    cache line 友好，60B 反而是 4 字节对齐。**结论：M2a 也写 t_cid=0**，格式统一。
- 本字段**不入 hint bit / WAL 特殊处理**，随 tuple 一起写入 heap page

**Agent 字段语义**：
- `t_agent_id`：写入该行的 agent 身份（M2 由客户端会话传入；M3 之后由 PG Wire 协议扩展位携带）
- `t_trace_id`：写入时刻的调用链 ID（可选，全 0 表示未追踪）
- M2 只做**存储和查询**，不做 RLS 过滤（推迟到 Phase 6 RLS predicate）

**hint bit 语义**：
- `HEAP_X*_COMMITTED/INVALID` 是**优化性**的：首次读取时若 CLOG 查询确认状态，回写
  hint bit 减少后续 CLOG 查询
- hint bit 更新**不写 WAL**（丢失只是性能损失，不影响正确性）
- 但要求 `pd_checksum` 计算时**跳过 hint bit 变化**（否则 hint 更新破坏页 checksum）
  → 决策：M2 不启用数据页 checksum（保持 M1 现状），hint bit 可以自由回写

**Commit 路径顺序约束**（v2 修订 P1-5，**正确性硬约束**）：为避免 "TxnCommit WAL 未落盘
但 hint 已回写 COMMITTED 到数据页，crash 后重建 CLOG 为 ABORTED，导致可见性错误" 的
bug，事务提交路径**必须**按以下顺序：

```
1. wal.append(TxnCommit{xid, commit_lsn}) → 返回 lsn
2. wal.flush_to(lsn)                       -- fsync commit 记录到 WAL
3. clog.set_state(xid, COMMITTED)          -- 更新内存 CLOG
4. txn_manager.remove_active(xid)          -- 从活跃集移除
5. -- 此刻起 reader 才允许回写 xmin=xid 的 hint bit
```

反之，`abort_txn` 也必须 `wal.append(TxnAbort) → flush_to → clog.set_state(xid, ABORTED)`。
hint 回写路径必须**先**读 CLOG（读到已提交或已回滚），才允许写 hint bit；这样 crash 后
若 CLOG 状态因 WAL 未 fsync 而"倒退"回 IN_PROGRESS，与 hint 是一致的（都缺）。

**理由**：
- 64 字节 header 精准满足所有字段 8 字节对齐（v2 修订：v1 版本字段顺序会把 u64 t_agent_id 放到 offset 36，导致对齐 padding 变成实质 68B header；本版重排后无对齐 hole）
- 64-bit xmin/xmax 彻底消除 PG 的 XID wraparound 复杂度（不需要 freeze）
- `t_ctid` 指向自己（普通行）或指向下一版本（UPDATE 链头，M2c HOT）

**代价**：
- Header 比 PG 的 24 字节大约 2.7×，小 tuple 场景空间放大明显
- 但 M2 目标是 Agent 元数据，行大小通常 > 200 字节，header 占比可接受

---

## 四、TOAST（The Oversized Attribute Storage Technique）

**背景**：向量、JSONB、大文本超过 page 大小的一部分（默认阈值 2KB）不能直接存主行。

**选择**：M2 引入 TOAST 溢出页机制，但简化版本。

- 阈值：单个 attribute 序列化后 > `TOAST_TUPLE_THRESHOLD`（默认 2KB）触发
- 存储：溢出数据切成 `TOAST_MAX_CHUNK_SIZE`（默认 2000 字节）小块，写入独立 TOAST 表
  - 每个用户表隐式关联一张 `pg_toast_<oid>` 表
- 主行只存 `TOAST pointer`（**20 字节**，v2 修订：5×u32=20，v1 写错为 18）：
  ```
  vl_len_:       u32  (标记 external，最高 2 位 = 01)     offset  0..4
  va_rawsize:    u32  (原始未压缩大小)                    offset  4..8
  va_extsize:    u32  (存储大小)                          offset  8..12
  va_valueid:    u32  (TOAST 表内 chunk group id)         offset 12..16
  va_toastrelid: u32  (TOAST 表 OID 低 32 位)             offset 16..20
  ```
- OID 底层是 u64，但 TOAST pointer 中仅存低 32 位（M2 全局 relation 数不会超过 42 亿；
  若超过则同一 TOAST 池切分，超出为 M3+ 议题）
- **M2 只支持 EXTERNAL（切块存储）**，不支持 COMPRESSED（LZ4/PGLZ）—— 压缩推迟到 Phase 7b

**理由**：Phase 2 HNSW 存 4KB 向量必须走 TOAST；不做 TOAST 则 M2 到 Phase 2 之间要
返工整个 tuple 编码。

（Supersede 注，2026-08-31：上述"必须走 TOAST"的假设已被 Phase 2 M4 设计取代——
HNSW 向量自存于 `pg-am-hnsw` 自有节点存储，与 `pg-am-heap` 零依赖，不走 heap TOAST；
且 M2 实际只交付了指针编解码，chunk 表 I/O 未实现、写路径从不触发。现状为大值内联至
单页容量、超限 `TupleTooLarge` 响亮报错。heap TOAST chunk I/O 本体归 Phase 4a，
完整决策记录见 ROADMAP.md "Phase 2 · TOAST 决策"。）

**代价**：Catalog 需要 `reltoastrelid` 字段；TOAST 表的可见性判断复用主表的 xmin/xmax
（TOAST tuple 头也是 64 字节，与主表一致）。

**WAL 记录复用**（v2 修订 P2-4）：TOAST 是隐式 heap 表，其 chunk 的写入/读取完全走
`HeapInsert` / `HeapDelete` 记录，**不引入新 record type**。主行的 TOAST pointer 变更
则包含在其所属主表的 `HeapInsert/Update` payload 中（TOAST pointer 只是 20 字节的 attribute
数据）。Recovery 顺序：Redo 阶段按 LSN 顺序重放，主表和 TOAST 表的记录天然交错在同一
WAL 流中，无需专门排序。

---

## 五、Catalog / Relation 抽象

**背景**：M2a 决定支持 `CREATE TABLE` + 多表。需要 catalog 存元数据。

**选择**：极简系统表 + 硬编码 bootstrap。

### 5.1 系统表清单（M2）

| 表 | OID | 存储 | 用途 |
|----|-----|------|------|
| `pg_class` | 1259 | heap | 所有 relation（表、索引、TOAST）: (oid, relname, relkind, relnatts, reltoastrelid, relam) |
| `pg_attribute` | 1249 | heap | 列定义: (attrelid, attname, atttypid, attlen, attnum, attnotnull, attnullable) |
| `pg_type` | 1247 | heap | 类型定义（M2 内置：int4/int8/text/bytea/timestamptz/uuid） |
| `pg_am` | 2601 | heap | AM 定义（M2 仅 heap 和 btree） |
| `pg_index` | 2610 | heap | 索引元数据（M2b 加，M2a 无索引） |

### 5.2 Bootstrap

- 全新数据目录 `init` 时：`pg-catalog` 硬编码写入所有系统表定义（含系统表**自己**的
  定义 —— pg_class 记录 pg_class 自身）
- 内置类型定义写死在 `pg-catalog::builtin_types.rs`，运行时不允许用户 CREATE TYPE
- 系统表 OID 保留区间 `[1, 9999]`，用户 OID 从 16384 开始（对齐 PG）

### 5.3 OID 分配

- 全局递增 `AtomicU64`（不是 u32——64 位无 wraparound）
- 持久化在 superblock 的 `next_oid` 字段（**Stage 0 债务 #6 已加**）

**理由**：M2a 就上 catalog 避免 heap file naming、redo record 的 relation 字段返工。
硬编码 bootstrap 比 initdb SQL 简单十倍，能跑通就够 M2。

**代价**：pg_class / pg_attribute 本身也是 heap 表，需要处理"读 catalog 依赖 catalog 已加载"
的启动 bootstrap 循环 —— 方案：Engine::open 时先按硬编码 schema 读 pg_class 页面
（第一个 heap file，OID 固定），再用读到的 schema 校验后续访问。

---

## 六、事务 ID 与 CLOG

**背景**：MVCC 需要判断每个 XID 的最终状态（COMMITTED/ABORTED/IN_PROGRESS）。

**选择**：64 位 XID + 独立 CLOG 文件 + 专用 SLRU 风格 cache（**不复用 BufferPool**）。

### 6.1 XID 分配

- `TxnIdClock`：**M2 新增类型**（沿用 M1 `LsnClock` 的 `AtomicU64` 设计模式，见
  `lsn_clock.rs:13-61`，但独立类型，不复用 `LsnClock` 代码）
- 分配点：`TxnManager::begin_txn()` 返回新 XID
- 起始值：`1`；`0` 保留为 `InvalidTxnId`
- 持久化：checkpoint 时把 `next_txn_id` 写入超级块（M1 已预留字段）
- **无 wraparound**：即便每秒 100M txn，64 位 XID 需要 5000 年耗尽 —— 直接放弃 PG 的
  freeze / wraparound 机制

### 6.2 CLOG 格式（v2 明确 bit 序）

- 文件：`{data_dir}/clog/clog-XXXXXXXX.log`（每段 128MB，可容纳约 2.68 亿（~268M）个事务状态；`128 MB × 2 XIDs/byte = 268,435,456`）
- 每个 XID 占 **4 bits**；每字节存 2 个 XID：
  ```
  byte N:
    高 4 bits (bits 4..7) → XID = (segment_base + 2N + 0)
    低 4 bits (bits 0..3) → XID = (segment_base + 2N + 1)
  ```
- 4-bit 状态编码：
  ```
  0b0000  IN_PROGRESS
  0b0001  COMMITTED
  0b0010  ABORTED
  0b0011  SUB_COMMITTED (M3 保留)
  ```
- 段号 = `xid / (128MB × 2)` = `xid / 268_435_456`；段内 offset = `(xid % 268_435_456) / 2`
- **不存 commit_lsn**：MVCC 判定用 XID 关系（xmin 在 snapshot.xip 中活跃 ⇒ 不可见）
  而非 LSN 比较

### 6.3 CLOG 缓存与 I/O 策略（v2 修订）

M1 的 `BufferPool` 只按 `PageId` 索引数据页，且 8KB 帧的 checksum/redo 路径不适合
CLOG 页（CLOG 页无 pd_lsn、无 pd_checksum）。因此 M2 引入独立 `ClogBuffer`：

```rust
pub struct ClogBuffer {
    // 简易 SLRU：N 个 clock-sweep 帧，每帧 8KB CLOG 页
    // 帧数可配置（v2.2 修订 P2-8；v2.3 补充 P2-5 默认值 rationale）：
    //   默认 8 帧 = 128K XIDs 窗口 —— 覆盖 100 并发事务的活跃窗口有余（§20 已验证）；
    //   但生产 TP 场景（≥ 1K TPS × 事务寿命 60s = 60K 活跃 XID + 冷路径回查提交 3 小时前
    //   XID）应上调至 64 帧（1M XIDs）；OLAP 长事务场景（跨小时的大 SELECT）建议 256 帧
    //   （4M XIDs），避免 hot-cold 抖动。
    // 通过 EngineConfig.clog_buffer_frames 调整；范围 [4, 1024]，非法值 → panic
    frames: Vec<ClogFrame>,
    // 页面 dirty 集合，checkpoint 时批量 fsync
    dirty:  BitSet,
}
```

- 读：miss 时按段号 + 段内 offset 用 `read_at` 拉入空闲帧
- 写：`TxnCommit`/`TxnAbort` redo 时更新对应 bit → 帧 dirty
- Flush：见 §6.4 单一 authoritative 定义（CheckpointBegin 之后、CheckpointEnd 之前 fsync dirty CLOG 页；别处不主动 flush）
- 崩溃恢复：Analysis 阶段扫 WAL 得到 TxnCommit/TxnAbort → Redo 阶段更新 CLOG bit（幂等）

**配置参数**：
```rust
pub struct EngineConfig {
    // ...
    /// CLOG buffer 帧数（默认 8）；每帧 8KB × 2 XIDs/byte = 16K XIDs
    pub clog_buffer_frames: usize,
}
```
默认 8：窗口 128K XIDs，覆盖 100 并发 × 数千事务/秒的 <10 秒窗口。压力测试或长事务
场景（例如 100 事务持续 > 30 秒）可上调至 64（1M XIDs 窗口）或 256（4M XIDs 窗口）。

### 6.4 CLOG 与 WAL 关系

- CLOG 写入**不写自身 WAL 记录**，而是 piggyback 在 `TxnCommit`/`TxnAbort` WAL 记录上
- Recovery Redo 阶段扫到 `TxnCommit(xid)` → 更新 CLOG 对应 bit
- CLOG 的持久化时机（v2.3 P2-1 归一化，避免文档多处重复表述发散）：
  - **单一 authoritative 定义**：`Checkpointer` 在 emit `CheckpointBegin` 之后、emit
    `CheckpointEnd` 之前，把 `ClogBuffer.dirty` 中所有帧 fsync；此外**别处不主动 flush
    CLOG**。commit path 只更新内存 bit，不触发 CLOG fsync（依赖 TxnCommit WAL 的 fsync
    保证持久性）。
  - 崩溃时未 fsync 的 bit 由 Redo 从 WAL 恢复（幂等）
  - §11.4 checkpoint 与 §6.3 flush 描述均**引用**此处，不再复述实现细节

**理由**：
- 64 位 XID 消除 freeze 复杂度 —— M1 沉没成本已经付了（LsnClock/TxnId 都是 u64）
- 独立 ClogBuffer 避免污染 M1 BufferPool 语义（M1 buffer 关联 `PageId`；CLOG 页面
  不是数据 page，混用会引发 pd_lsn 语义混乱）

**代价**：CLOG 缓存是新模块；M2b 需要专门的 Clog benchmark（读命中率、flush 延迟）。

---

## 七、MVCC / Snapshot / Visibility Oracle

**背景**：多个事务并发读写，需要一致的可见性判定，且判定逻辑对所有 AM 共享。

**选择**：**XID-based Snapshot Isolation**（v2 修订：从"LSN-based"改名，避免与
LSN 语义混淆）。快照 = { xmin, xmax, xip[], current_xid }。
Visibility Oracle 作为 `pg-txn` 的统一入口。

### 7.1 Snapshot 数据结构

```rust
pub struct Snapshot {
    /// 该快照之前所有已提交事务的 XID 都 < xmin（下界）
    pub xmin: TxnId,
    /// 该快照之后的所有 XID 都不可见（上界 = 快照建立时的 next_txn_id）
    pub xmax: TxnId,
    /// 快照建立时仍活跃的事务 XID 列表（xmin ≤ xip[i] < xmax）
    pub xip: SmallVec<[TxnId; 32]>,
    /// 持有该快照的事务自身的 XID（可见 "自己写的" 判定用）
    pub current_xid: TxnId,
    /// 当前命令序号（v2.3 新增，用于同事务内 self-INSERT/DELETE/UPDATE 可见性）
    /// M2a 恒为 0；M2b 每条 SQL 语句边界 +1
    /// **Q4 递增时机**（v2.3 明确）：curcid 在 SQL 语句**开始执行前**由 executor 递增
    /// （非 commit 时；非语句结束时）。语句执行途中所有 INSERT/UPDATE/DELETE 写入的
    /// tuple `t_cid = 递增后的 curcid`；同一语句内的 self-scan 用同一 curcid，从而
    /// `t_cid < curcid` 为 false，避免把本语句刚写的 tuple 再次扫到（UPDATE 循环）。
    /// 下一语句开始前再 +1，此时先前语句写的 tuple 变成 `t_cid < curcid`，可见性
    /// 转换为"先前命令的写入"。
    pub curcid: u32,
}
```

- 建快照：`TxnManager::snapshot(current_xid)` 原子读 `(next_txn_id, active_txns)`
- **完全 XID-based**：可见性判定不参与任何 LSN 比较
- **snapshot_lsn 字段已移除**（v2 修订 P2-8）：v1 曾计划用它做 "hint bit 回写边界"，
  但 hint bit 缓存的是 CLOG 查询结果（已提交/已回滚是幂等状态），后续 reader 仍走
  `xmin >= snapshot.xmax` / `xip.contains(xmin)` / `clog(xmin)` 判定，天然屏蔽"未来"
  事务，无需 LSN 边界。移除该字段简化 Snapshot 结构。

### 7.2 Visibility Oracle 判定

```rust
pub trait VisibilityOracle {
    /// 判定 tuple 版本对给定 snapshot 是否可见。
    /// - xmin / xmax：tuple header 中的事务 id
    /// - t_cid：tuple header 中的命令序号（v2.3 新增，M2a 恒为 0）
    /// current_xid / curcid 从 snapshot 取，无需单独参数。
    fn is_visible(
        &self,
        xmin: TxnId,
        xmax: TxnId,
        t_cid: u32,
        snapshot: &Snapshot,
    ) -> Visibility;

    /// 尝试异步刷新 tuple 的 hint bit（读路径可选调用；不返回错误）
    fn set_hint_bit(&self, tid: Tid, hint: HintBit);
}

pub enum Visibility {
    Visible,        // 该 tuple 版本对 snapshot 可见
    Invisible,      // 该 tuple 版本对 snapshot 不可见
    Uncertain,      // 该 tuple 版本被并发事务修改（xmax IN_PROGRESS），需检查锁（M2c 用）
                    // v2.3 P2-2 说明：M2a 单语句 auto-commit 无并发，本枚举值永远不返回；
                    // 保留是为了 M2b/M2c 引入 SI 快照后的行锁等待协议接口稳定
}
```

判定规则（PG 教科书版，v2.3 修订：引入 curcid + t_cid 区分同事务先前命令与当前命令）：
```
fn is_visible(xmin, xmax, t_cid, snapshot):
    let self_xid = snapshot.current_xid;
    let curcid   = snapshot.curcid;
    // 1. xmin 判定
    if xmin == self_xid:
        // 自己写的：只在 t_cid < curcid（先前命令写的）时可见；
        // 当前命令（t_cid == curcid）刚写的 tuple 不参与本命令扫描（避免 UPDATE 循环）
        if t_cid < curcid: 见（继续 xmax 判定）
        else: 不见（本命令自己刚写的）
    else if xmin >= snapshot.xmax: 未来事务 → 不见
    else if snapshot.xip.contains(xmin): 并发未提交 → 不见
    else if clog(xmin) != COMMITTED: 未提交/已回滚 → 不见
    // → 到这里 xmin 已提交且早于快照
    // 2. xmax 判定
    if xmax == INVALID: 未被删除 → 见
    if xmax == self_xid:
        // 自己删的：先前命令删的 → 不见；当前命令删的 → 仍见
        if t_cid < curcid: 不见（先前命令已删，对当前命令 SELECT 不可见）
        else: 见（当前命令删的，同命令内 DELETE ... RETURNING 需要读到自己刚删的行）
    if xmax >= snapshot.xmax: 未来删除 → 见
    if snapshot.xip.contains(xmax): 并发未提交删除 → 见 (Uncertain 若 M2c 需要写锁)
    if clog(xmax) != COMMITTED: 删除未提交/已回滚 → 见
    return 不见
```

**M2a 简化（v2.3-35 修订：澄清 v2.2 遗留错误）**：M2a 是单语句 auto-commit，事务边界与
语句边界重合，SELECT 扫到的 tuple 其 `xmin` / `xmax` **必然是历史已提交 XID**（当前 self_xid
在语句执行完就 commit 并从 active set 移除），因此 `xmin == self_xid` / `xmax == self_xid`
两条分支在 M2a 正常流程中**根本不触发**。M2a 阶段所有 tuple `t_cid = 0`、`snapshot.curcid = 0`
只是占位默认值（dead code path），行为与 v2.2 完全一致。**M2b 才启用 curcid 递增语义**：
一旦引入显式事务与命令边界，`xmin == self_xid` / `xmax == self_xid` 分支被激活，`t_cid < curcid`
判定即可消除 v2.2 遗留的"同事务内 UPDATE 后 SELECT 返回双行"缺陷。

**M2b 验证用例**（必须覆盖，与上述规则严格一致；符号约定：每条 SQL 语句边界 curcid+=1）：
- `BEGIN(T1) → INSERT r1(cid=1) → 语句提交后 curcid=2 → DELETE r1(xmax=T1, cid=2)
  → 语句提交后 curcid=3 → SELECT` → **不返回 r1**
  （xmin=T1、t_cid=1 < curcid=3 → xmin 见；xmax=T1、t_cid=2 < curcid=3 → 先前命令删除，不见）
- `BEGIN(T1) → INSERT r1(cid=1) → 同语句 RETURNING SELECT`（curcid 仍 = 1）
  → xmin=T1、t_cid=1，`t_cid < curcid` 为 `false` → 走 else 分支"不见"
  → **但**：RETURNING 走的是 INSERT 输出通道，不走 scan is_visible，因此该场景不受影响
- `BEGIN(T1) → INSERT r1(cid=1) → DELETE r1(xmax=T1, cid=1) → 同语句 RETURNING`
  同上，DELETE ... RETURNING 输出走的是 DELETE 通道
- `BEGIN(T1) → UPDATE r1 SET ...(cid=1)`：old r1 xmax=T1 cid=1；new r2 xmin=T1 cid=1
  → 同语句扫描其它 tuple 时会跳过 old（因 t_cid==curcid，先前分支为 else，走 xmax 判定 -
  但 xmin 分支已经返回不见）和 new（xmin 分支不见）——避免同一 UPDATE 语句反复更新
  自己刚写的 tuple（Halloween problem 保护）
- `BEGIN(T1) → DELETE r1；(未提交)`，另一事务 T2 `SELECT` → T2 **返回 r1**
  （xmax=T1 属于 xip，返回"删除未提交"分支 → 见）
- `BEGIN(T1) → DELETE r1 → COMMIT；BEGIN(T2) → SELECT` → T2 **不返回 r1**
  （xmax=T1 已提交、不在 xip → delete 生效）

**v2.3 vs v2.2**：v2.2 因缺少 command counter，`self.DELETE + self.SELECT` 会误返回
已删行；v2.3 引入 curcid 后语义与 PG 对齐（DELETE ... RETURNING 通过通道输出，
后续 SELECT 看不到）。完整命令边界语义（如 CTE、pl/pgsql）在 Phase 6 完善。

### 7.3 Oracle 与 AM 的契约

- **Heap AM 及其它带 xmin/xmax 的 AM**：取到 tuple 后**必须**调用
  `VisibilityOracle::is_visible` 再返回给上层。
- **纯索引 AM（B+Tree / HNSW / Inverted）**：索引条目 `(key, tid)` 没有 xmin/xmax，
  不做可见性判定；由上层拿 tid 到 heap AM 查 tuple 时再走可见性。
- Oracle 只依赖 `pg-txn` 内部的 `ClogAccessor` 和 `ActiveXactRegistry`，AM 不直接读 CLOG。

**理由**：把可见性判定收敛到单个 trait，Phase 2 HNSW 的 tuple 也走同一套判定，符合
ROADMAP "TID + XID 统一寻址" 契约。

**代价**：每次 tuple 访问一次 CLOG 查询（缓存不命中时是 cache miss）—— hint bit 优化
恰好解决这个热点。

---

## 八、隔离级别

**选择**：**默认 Snapshot Isolation (SI)**；Read Committed (RC) 通过"每语句新快照" 
实现；Serializable Snapshot Isolation (SSI) 推迟到 Phase 7d。

| 隔离级别 | 快照策略 | M2 支持 |
|---------|---------|--------|
| Read Uncommitted | 无快照，脏读 | ❌ 不支持 |
| Read Committed | 每语句 `TxnManager::snapshot()` | ✅ M2b |
| Repeatable Read = SI | 事务开始时快照，整个事务复用 | ✅ M2b（**默认**） |
| Serializable | SI + 依赖追踪 | ❌ 推迟 Phase 7d |

**理由**：SI 对 PG 风格 MVCC 是自然默认；RC 只是"更短生命周期的快照"，代码路径相同。

**代价**：SI 存在 write skew（跨行约束违反），M2 用户需要用 `SELECT ... FOR UPDATE`
显式加锁绕过（M2c 支持）。

---

## 九、Lock Manager

**背景**：MVCC 消除读锁但不消除写-写冲突。UPDATE 同一行 / 显式 `FOR UPDATE` / DDL 都要锁。

**选择**：**行级锁走 tuple.xmax**（不入 lock table）+ **表级锁 4 标准模式**（入 lock table）。

### 9.1 行级锁：xmax 协议（v2 明确等待/唤醒）

**适用范围**（v2 修订 P2-5）：以下所有写操作**共用**该协议，不区分显式/隐式：
- `INSERT`：新 tuple 的 xmin = self_xid，自身天然独占（无并发写者能看到）
- `UPDATE` / `DELETE`：旧 tuple 的 xmax = self_xid 走下述协议
- `SELECT ... FOR UPDATE` / `SELECT ... FOR NO KEY UPDATE`：显式抢锁，与 UPDATE 同路径
- `SELECT ... FOR SHARE`：**M2c 才支持**，用 multixact 结构（延后设计）

写者到达一行的流程：
1. 拿住页 latch，读 `t_xmax`
2. 若 `t_xmax == INVALID`：CAS 写入自身 XID，成功即拿到行锁
3. 若 `t_xmax != INVALID` 且 `clog(t_xmax) == COMMITTED`：说明该行已被删除/更新，
   走可见性判定（不见 → 报"tuple concurrently updated"或 restart）
4. 若 `t_xmax != INVALID` 且 `clog(t_xmax) == ABORTED`：CAS 覆盖 xmax = self，成功拿锁
5. 若 `t_xmax != INVALID` 且 `clog(t_xmax) == IN_PROGRESS`：
   a. 记 `waiter = (self_xid, waiting_on = t_xmax)` 进 `TxnManager::row_wait_registry`
   b. 释放页 latch
   c. 阻塞在 `TxnManager::wait_for(other_xid)` 上（该 API 内部用 `parking_lot::Condvar`
      或 `tokio::sync::Notify`；M2c 决定异步/同步）
   d. `other_xid` 提交/回滚时 `TxnManager::end_txn` 广播 Condvar → waiter 唤醒
   e. 重回步骤 1

（行 S 锁在 §9.1 适用范围小节已说明推迟到 M2c）

### 9.2 表级锁

- 4 种模式（M2 保留 PG 的一部分，简化）：

| Mode | 用途 | 冲突表 |
|------|------|--------|
| AccessShare | SELECT | 与 AccessExclusive 冲突 |
| RowExclusive | INSERT/UPDATE/DELETE | 与 AccessExclusive、Exclusive 冲突 |
| Exclusive | 索引创建（M2b/M3） | 与自身 + AccessExclusive 冲突 |
| AccessExclusive | DDL（DROP、ALTER） | 与所有冲突 |

- 意向锁 IS/IX **M2 不引入**（ROADMAP 已推迟到 Phase 6）
- 数据结构：`HashMap<TableOid, LockEntry>`，每个 entry 内含 grant 队列 + wait 队列

### 9.3 死锁检测

- Wait-for graph：节点 = XID，边 = 行锁等待（`row_wait_registry`）+ 表锁等待
- 后台线程 100ms 扫描一次
- 检测到环：选出 victim（最年轻事务）abort
- M2c 引入；M2a/M2b 只有单语句 auto-commit / 单事务，不会死锁

**理由**：
- 行锁走 tuple 是 PG 经典设计，与胖 header 天然契合
- 4 模式表锁足够 M2 DDL 和 CRUD，避免 IS/IX 的 8×8 冲突矩阵

**代价**：需要 lock manager 的公平性和可扩展性保证；M2c 是并发密集期。

---

## 十、WAL Record 扩展

**背景**：M1 只实现 4 种 record（PageAlloc / FullPageImage / CheckpointBegin/End）。
M2 需要为 heap、btree、txn 增加约 12 种。

**选择**：Stage 0 中一次性保留 M2 所有 discriminant，按 stage 递进实现 handler。

### 10.1 Record 类型清单

| Record Type | Discriminant | 保留 | Stage | Payload 关键字段 |
|-------------|:-:|:-:|:-:|-----------------|
| `PageAlloc` | 40 | M1 | M1 | (page_id) |
| `PageFree` | 41 | Stage 0 | Stage 0 | (page_id) |
| `FullPageImage` | 10 | M1 | M1 | (page_id, image) |
| `CheckpointBegin` | 30 | M1 | M1 | (checkpoint_lsn) |
| `CheckpointEnd` | 31 | M1 | M1 | (meta_ref) — 见 §11.4 |
| `HeapInsert` | 1 | Stage 0 | M2a | (page_id, slot_id, xmin, tuple_bytes) |
| `HeapUpdate` | 2 | Stage 0 | M2a | (old_tid, new_tid, xmax_old, xmin_new, new_tuple_bytes) |
| `HeapDelete` | 3 | Stage 0 | M2a | (tid, xmax) |
| `HeapHotUpdate` | 7 | Stage 0 | M2c | (page_id, old_slot→redirect, new_slot, new_tuple) |
| `HeapCleanup` | 8 | Stage 0 | M3 | (page_id, dead_slots[]) |
| `BTreeInsert` | 4 | Stage 0 | M2b | (leaf_page_id, slot, key_bytes, tid) |
| `BTreeSplitPrepare` | 5 | Stage 0 | M2b | (left_page, new_right_page, level, high_key) — 复用 M1 已保留的 `BTreeSplit=5` 判别子，v2.3 重命名为 `BTreeSplitPrepare` |
| `BTreeSplitCopy` | 51 | Stage 0 | M2b | (left_page, right_page, copy_start_slot, left_page_pre_lsn) — **Stage 0 追加**（非 M1 保留判别子；M1 `WalRecordType` 未定义 51） |
| `BTreeSplitCommit` | 52 | Stage 0 | M2b | (left_page, right_page, parent_page, separator_key) — **Stage 0 追加**（非 M1 保留判别子；M1 `WalRecordType` 未定义 52） |
| `BTreeDelete` | 6 | Stage 0 | M2c | (leaf_page_id, slot) |
| `BTreeSplitCLR` | 50 | Stage 0 | M2c | (target_incomplete_page, redo_ref_lsn) |
| `TxnBegin` | 20 | Stage 0 | M2b | (xid) |
| `TxnCommit` | 21 | Stage 0 | M2a | (xid, commit_lsn) |
| `TxnAbort` | 22 | Stage 0 | M2a | (xid) |
| `LogicalHnsw` | 100 | M1 | Phase 2 | 逻辑 HNSW 操作（M1 已预留） |
| `LogicalInverted` | 101 | M1 | Phase 3 | 逻辑倒排索引操作（M1 已预留） |
| `LogicalGraph` | 102 | M1 | Phase 3 | 逻辑图操作（M1 已预留） |
| `LogicalTimeSeries` | 103 | M1 | Phase 3 | 逻辑时序操作（M1 已预留） |
| `SegmentSeal` | 110 | M1 | Phase 3 | 段封存操作（M1 已预留） |
| `SegmentMerge` | 111 | M1 | Phase 3 | 段合并操作（M1 已预留） |

**说明**：discriminant 分配已在 Stage 0 完成保留。BTree 分裂三步 record 分别占用
5 / 51 / 52（其中 5 复用 M1 `BTreeSplit`，重命名为 `BTreeSplitPrepare`）；BTreeDelete
复用 6；BTreeSplitCLR 单独占 50。所有 record type 一次性在 Stage 0 的 `WalRecordType`
enum 中保留，M2b/c 各 stage 只增 handler、不改 enum。**M1 已存在的 discriminant
（10/30/31/40, 以及 100-111 Phase 2/3 预留）在 v2.3 中修正与 `crates/pg-storage/src/wal/record.rs:19-67`
保持一致**，Stage 0 债务 #7 只新增值、不重编号，保证 on-disk 兼容。

### 10.2 编码格式复用 M1

24B fixed header + payload + 8B alignment + CRC。**M2 不改 on-disk WAL 格式**。
Payload 用 bincode 2.x + serde；schema 演进走 `t_flags` 版本号（tuple 层面）与
`WalRecord.version`（record 层面，用 header flags 高 4 位）。

### 10.3 CLR（Compensation Log Record）

- M2 heap 层**不需要 CLR**（PG 风格 undo：uncommitted tuple 通过 visibility 天然屏蔽）
- M2c B+Tree Split **需要 CLR**：split 是物理结构变更，中途 crash 必须 redo 完成
  - `BTreeSplitCLR` 的 payload 只指向"需要完成的 incomplete page"，redo 阶段幂等地
    重新走 SplitCopy + SplitCommit（记录 last redo LSN 防止循环）

**理由**：judgement 全部集中在 Stage 0 保留，避免 M2b/c 各 stage 反复修 enum 判别子导致
crash recovery 兼容性问题；PG 风格 MVCC 消除大部分 CLR 需求。

**代价**：Payload schema 演进需要 versioning —— M2 通过 record header `flags: u16` 的**高 4 位**
做版本号（低 12 位保留给 record 自身 flags）。**版本号语义与 §11.4 一致**：`0 = M1 legacy / 隐式 v1`
（M1 已写入 `flags=0`），`1 = M2 v2 payload`。M2 emit 时写 `flags = (1 << 12)` 表示 v2；decode 分支
按版本号选 payload schema。

---

## 十一、ARIES 变体崩溃恢复

**背景**：M1 只做 replay（无 Analysis、无 Undo）。M2 引入事务后需要完整的三阶段恢复。

**选择**：**PG 风格 ARIES 变体**：Analysis（构建 ATT/CLOG）→ Redo（幂等重放）→ **无
显式 Heap Undo**（uncommitted 状态天然屏蔽）+ **仅 B+Tree 结构变更走 CLR**。

### 11.1 Analysis Phase

- 从 superblock 的 `checkpoint_lsn` 开始扫 WAL
- 构建 `ActiveXactTable (ATT)`：记录 crash 时仍未提交的 XID
- 构建 `DirtyPageTable (DPT)`：`page_id → rec_lsn`（该页第一次被弄脏的 WAL 位置）
- Redo LSN = `min(DPT.values().map(|e| e.rec_lsn))`（若 DPT 空 = checkpoint_lsn）

### 11.2 Redo Phase（v2 明确统一分发顺序）

- 从 Redo LSN 开始**严格 LSN 顺序**顺序扫 WAL
- 对每条 record 通过 `RedoRegistry` 查表分发到唯一的 `RedoHandler`：
  ```rust
  let handler = registry.get(record.rtype).ok_or(RecoveryError::UnknownRecord)?;
  handler.apply(record, &mut redo_ctx)?;
  ```
- FullPageImage 也走同一分发路径：`fpi_handler.apply` 内部覆盖整页并把 `page.pd_lsn = record.lsn`
- 幂等：所有 handler 内部检查 `page.pd_lsn >= record.lsn` 则跳过（除 CLR 特例：CLR
  按 `redo_ref_lsn` 判等）
- CLOG 更新走 piggyback：`TxnCommit` handler 内部更新 CLOG bit
- **不写新 WAL** 记录（CLR 除外，M2c 才引入 CLR 写路径）

### 11.3 Undo Phase（简化版）

- 遍历 ATT 中所有 XID：
  1. 在 CLOG 写入 `ABORTED`
  2. **不需要**逐条撤销 heap 修改：MVCC 可见性会自动屏蔽（xmin ABORTED → invisible）
  3. **需要**发出 B+Tree 的 CLR 完成任何未完成的 split（如果 crash 发生在 split 中间）
     - Analysis 阶段发现"split 已开始但未结束"（page flag `BTP_SPLIT_INCOMPLETE`），
       Undo 阶段调 `BTreeAM::finish_incomplete_split(page_id)` 完成它并 emit `BTreeSplitCLR`

### 11.4 Checkpoint 增强（v2 修订：ATT/DPT 挪出 record payload；v2.3 补齐 P1-4 v1/v2 迁移）

M1 `WalRecord::encode` 限制单条 payload ≤ 64KB，而 100K 活跃事务的 ATT 已达 800KB,
无法塞进 CheckpointEnd payload。因此 v2 拆分：

- **CheckpointEnd** payload 只存元信息与外部快照文件引用：
  ```
  checkpoint_lsn:   Lsn      -- 本次 checkpoint 的 begin lsn
  next_page_id:     PageId
  next_txn_id:      TxnId
  next_oid:         u64
  att_file:         String   -- 相对 data_dir 的路径，如 "meta/att-000123.snapshot"
  dpt_file:         String   -- 相对 data_dir 的路径
  ```

**v1 → v2 payload 版本迁移**（v2.3 P1-4）：
- M1 `CheckpointEndRecord`（`record.rs:122-130`）**v1 payload = 3 字段**：`checkpoint_lsn / next_page_id / next_txn_id`；M2 **v2 payload = 6 字段**（新增 `next_oid / att_file / dpt_file`）。
- **版本判定通道**：复用 `WalRecord.flags: u16` 的**高 4 位**作为 record payload version（低 12 位保留给 record 自身 flags）。M1 已写入 `flags=0`，故所有 M1 record 隐式为 v1。M2 emit CheckpointEnd 时写 `flags = (1 << 12)`（version=1，M2）。
- **decode 分支**：`CheckpointEndRecord::decode(bytes, flags)` 按 `flags >> 12` 分派：
  （实现偏离脚注，2026-08-31：M1 冻结的 32B 记录头实际为 `flags: u8`，代码以
  **u8 高 4 位 `flags >> 4`** 分派——`crates/pg-storage/src/wal/record.rs:676`；
  偏离原因见 record.rs `CHECKPOINT_END_VERSION_V2` 注释，coding-plan Stage N 表已如实记录）
  - `0`（v1，来自 M1）：只读 3 字段，`next_oid` 默认 `16384`（PG 保留 OID 上限），`att_file / dpt_file` 默认空串。Analysis 阶段遇到空 `att_file` 视作"无活跃事务快照"，从 checkpoint_lsn 起做 full scan 重建 ATT。
  - `1`（v2，来自 M2）：读全 6 字段。
- **前向 crash 保护**：M2 首次启动若读到 M1 v1 CheckpointEnd，走上述默认值路径，不写回 v2 格式（保持 recovery 只读性）；直到 M2 自己下一次 emit CheckpointEnd 才升级为 v2。
- **CheckpointBegin 不变**：v1/v2 都只有 `checkpoint_lsn` 一个字段，无需版本化。

- **ATT/DPT 快照文件**：`{data_dir}/meta/att-{checkpoint_lsn}.snapshot` +
  `dpt-{checkpoint_lsn}.snapshot`，内部为 bincode `Vec<TxnId>` / `Vec<(PageId, Lsn)>`；
  写入顺序：先 fsync 快照文件 → 再 emit CheckpointEnd record → 再 fsync WAL
- 崩溃恢复：Analysis 读 CheckpointEnd → 打开对应 snapshot 文件 → 从 att/dpt 出发扫描
  crash tail
- **旧 snapshot 文件清理**（v2 修订 P2-7）：由**下一次 checkpoint 的收尾阶段**同步删除
  `meta/` 目录中除最近 3 个 checkpoint 外的旧文件（`att-*.snapshot`/`dpt-*.snapshot`）。
  不引入独立后台线程；保留最近 3 个是为了在最新 checkpoint 的 snapshot 文件损坏时可
  回退到上一个（recovery 逻辑按 `checkpoint_lsn` 降序尝试）。

### 11.5 WAL-before-data flush 协议（v2 补齐 M10，v2.3 修订对齐 M1 现状）

任何将脏页写盘的路径**必须**先 `flush_to(page.pd_lsn)`：
- **M1 现状**：`BufferPool::flush(page_id)`（`buffer_pool.rs:311`）内部 `flush_frame`
  已实现协议（`buffer_pool.rs:529-531`）：`if page_lsn.is_valid() && synced_lsn() < page_lsn
  { flush_to(page_lsn) }`；`Checkpointer` 在 emit `CheckpointEnd` 后 `flush_to(end_lsn)`
  才更新 superblock（`checkpoint.rs:204`）。
- **Stage 0 需要**：
  (a) 验证 **evict 路径**（LRU 淘汰、非显式 flush）也走同一协议 —— 补充回归测试；
  (b) 引入 `pd_lsn` 后，`flush_frame` 从 `frame.page_lsn` 改为 `page[0..8]` 直读，
      避免 frame cache 与 page 内 pd_lsn 不一致；
  (c) 新增 `test_wal_before_data_on_evict` 覆盖 evict 场景，补齐 M1 已有的
      `test_flush_waits_for_wal` 系列的盲区。
- `PageAllocator::extend`：文件扩展前 `wal.flush_to(alloc_lsn)`
- CLOG flush（Checkpointer）：不需要 flush WAL 之前（CLOG 是 WAL 派生状态；恢复时可 redo）

违反该协议是 recovery 正确性 bug。**注**：本节前一版本写 `BufferPool::flush_page`，
与 M1 实际 API `BufferPool::flush` 不符，v2.3 全文统一改为 `flush`。

### 11.6 RedoHandler / RedoContext 契约

**Crate 依赖问题**（v2 修订 P1-1）：`RedoContext` 若直接持有 `pg-txn::ClogBuffer`，会导致
`pg-storage` → `pg-txn` 依赖，但 `pg-txn` 本身依赖 `pg-storage` → 循环。
解决：在 `pg-storage` 定义 `ClogAccessor` trait，`RedoContext` 只持有 `&dyn ClogAccessor`
的 trait object；`pg-txn::ClogBuffer` 在 M2b 实现该 trait。

```rust
// crate: pg-storage
pub trait ClogAccessor: Send + Sync {
    fn get_state(&self, xid: TxnId) -> TxnState;
    fn set_state(&self, xid: TxnId, state: TxnState);
}

pub trait RedoHandler: Send + Sync {
    fn kind(&self) -> WalRecordType;
    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext) -> Result<()>;
}

pub struct RedoContext<'a> {
    pub buffer_pool: &'a BufferPool,
    pub page_allocator: &'a Mutex<PageAllocator>,
    pub clog:          &'a dyn ClogAccessor,   // Stage 0 提供 NoOpClogAccessor 空实现
    pub att:           &'a mut ActiveXactTable,
    pub dpt:           &'a mut DirtyPageTable,
}
```

**Stage 0 阶段**：`RedoContext` 已定义完整字段；`clog` 字段传入 `NoOpClogAccessor`
（M1 无事务，`get_state` 总返回 `COMMITTED`）。M2b 引入真实 `ClogBuffer` 后替换。

**v2.3 修订**：删除原 `aborted_xids: &mut AbortedXidSet` 字段。M2a 起，redo handler
统一通过 `ctx.clog.get_state(xid)` 判断事务状态；`TxnAbort` handler 直接调
`ctx.clog.set_state(xid, ABORTED)`，无需独立 abort set。

- 每个 `WalRecordType` **恰好**注册一个 handler；重复注册 = panic on `Engine::open`
- 未注册的 record type = `RecoveryError::UnknownRecord` 硬失败（不静默跳过）
- Handler 内部所有对 buffer_pool 的写入都走 `buffer_pool.mark_dirty(page_id, record.lsn)`
  统一路径以维护 DPT

**理由**：PG 风格 MVCC 让 Heap Undo 大大简化；CLR 只用于必须原子的物理结构操作；
统一 dispatch + 硬失败避免 recovery 静默丢 redo。

**代价**：Undo 必须能识别所有"物理结构中断"的场景 —— 每种 AM 提供
`fn analyze_incomplete_ops(&self, wal_scan_result: &AnalysisResult) -> Vec<UndoAction>`。

---

## 十二、Checkpoint 增强

**变更点**（在 M1 fuzzy checkpoint 基础上）：

1. **CheckpointBegin 之前预分配 LSN**（Stage 0 债务 #3 修复的 FPI race）
2. **CheckpointEnd 携带 ATT/DPT 快照文件引用 + next_oid**（v2 修订，见 §11.4）
3. **CLOG 页 flush**：checkpoint 时刷 `ClogBuffer.dirty`（不复用 M1 BufferPool）
4. **超级块字段扩展**（Stage 0 债务 #6，v2.3 修订与 M1 实际布局对齐；所有 offset 均为 superblock 起始的**绝对偏移**）：
   - `magic: u32`          offset 0..4    （M1 已有）
   - `version: u32`        offset 4..8    （M1 已有；v1 → v2 迁移见债务 #6）
   - `page_size: u32`      offset 8..12   （M1 已有）
   - `padding: u32`        offset 12..16  （M1 已有，8 字节对齐）
   - `checkpoint_lsn: u64` offset 16..24  （M1 已有）
   - `next_page_id: u64`   offset 24..32  （M1 已有）
   - `next_txn_id: u64`    offset 32..40  （M1 已预留）
   - `next_oid: u64`       offset 40..48  （**Stage 0 债务 #6 新增**，占用原 M1 `created_at` 位置）
   - `created_at: u64`     offset 48..56  （M1 已有，v2 后移）
   - `crc32: u32`          offset 56..60  （M1 已有，v2 后移；覆盖 0..56 + 60..512）
   - `reserved: [u8; 452]` offset 60..512 （空闲，供 M3+ 扩展）
5. **触发条件**（M2 起）：
   - 时间：默认 30s（M1 已支持）
   - WAL 量：距上次 checkpoint 累积 > 64MB（**新增**）
   - 手动：`Engine::checkpoint()` API

**Superblock 迁移**（v2.3 修订）：Stage 0 读到 v1（`version=1`）时按 M1 老 layout 解析
（`created_at` 在 40..48、`crc32` 在 48..52），把 `next_oid` 初始化为 `16384`，
`created_at` 搬到 48..56、`crc32` 搬到 56..60，`version` 置为 `2`，再以 v2 布局
双副本 + CRC 写回。M1 → v2 只做一次；v2 之后 append-only。

---

## 十三、B+Tree 索引

**背景**：M2 需要索引支撑 `SELECT WHERE pk = ?` 和唯一约束。

**选择**：**Blink Tree 变体**（Lehman-Yao），latch coupling 读，pessimistic split。

### 13.1 页面格式

- 复用 slotted page 布局（第二章）
- `pd_special` 指向 `BTreePageSpecial`（16 字节）：
  ```
  btpo_prev:   PageId   // 左兄弟（内部页无用）    offset  0..8
  btpo_next:   PageId   // 右兄弟（Blink 关键）    offset  8..16
  ```
  额外的 level/flags 复用 `pd_flags` 的高 8 位与 `pd_flags` 相邻的保留位（避免 special 增长）：
  - `pd_flags` bit 8..11 = `btpo_level`（0=leaf；最多 16 层，足够 8KB page 存 2^64 行）
  - `pd_flags` bit 12..15 = `btpo_flags`（LEAF / ROOT / DELETED / SPLIT_INCOMPLETE）
- 内部页 tuple = (key, child_page_id)
- 叶子页 tuple = (key, tid)

### 13.2 并发控制

- **读**：Latch coupling —— 拿到子页 latch 后释放父页 latch
- **写（M2b 单线程版）**：整路径独占 latch，简化
- **写（M2c 并发版）**：
  - Optimistic：先按读路径下降拿叶子 X latch，若空间够直接插入
  - Pessimistic：需要 split 时 restart，从根开始拿全路径 X latch
- Blink Tree 的 `btpo_next` 允许 split 期间读者顺着 next 找到目标 key，不阻塞

### 13.3 Split 原子性（v2 明确 3 步 payload）

Split 分三步 WAL，顺序 emit：

1. **`BTreeSplitPrepare`**：
   ```
   left_page:      PageId  -- 溢出的原页
   new_right_page: PageId  -- 新分配的右兄弟（PageAllocator 先分配）
   level:          u8
   high_key_bytes: Vec<u8> -- 用于分裂点标记（redo 用来校验）
   ```
   Redo：把 `left_page.btpo_flags |= SPLIT_INCOMPLETE`；初始化 `new_right_page` 头
   （`btpo_prev = left_page`, `btpo_next = left_page.btpo_next`）；把 `left_page.btpo_next = new_right_page`

2. **`BTreeSplitCopy`**（v2.2 修订 P2-9：payload 极简，不存 moved_tuples）：
   ```
   left_page:  PageId
   right_page: PageId
   copy_start_slot: u16    -- left_page 中该 slot 之后（含）的 tuples 都搬到 right_page
   left_page_pre_lsn: Lsn  -- redo 幂等锚点：只有 left_page.pd_lsn == 该值时才 apply
   ```
   Redo：
   - 读 left_page（要求 left_page.pd_lsn == left_page_pre_lsn，否则说明已 replay 过，跳过）
   - 从 left_page slotted array 的 `copy_start_slot` 开始，逐 slot **重算**要搬的 tuples
     并追加到 right_page（`right_page.pd_lsn < record.lsn` 才执行）
   - 截断 left_page 的 LP 数组到 `copy_start_slot`
   - 更新两页的 `pd_lsn = record.lsn`

   **理由**：分裂通常搬 40–60% 的 tuple；若 key 大（如 100B）× 100 slot = 10KB payload
   超过合理 WAL 记录尺寸。让 redo handler 从 left_page 重算（要求 left_page 尚未被
   Copy 后续 mutation 覆盖）可让 payload 保持 O(20 字节)。对齐 PG 的 `xl_btree_split`
   （PG 也不存 moved_tuples，只存 firstright 起点）。

3. **`BTreeSplitCommit`**：
   ```
   left_page:      PageId
   right_page:     PageId
   parent_page:    PageId
   separator_key:  Vec<u8>
   parent_insert_slot: u16
   ```
   Redo：`parent_page` 插入 `(separator_key, right_page)`；清除 `left_page.btpo_flags` 的 `SPLIT_INCOMPLETE` 位

Crash 后 Analysis 阶段：扫到只有 SplitPrepare 无 Commit → 记 `incomplete_split_pages`；
Undo 阶段调 `BTreeAM::finish_incomplete_split(pid)` → 走 SplitCopy + SplitCommit 补齐
并 emit `BTreeSplitCLR` 记录完成动作。

### 13.4 AccessMethod trait 实现

见 §14。B+Tree 是第一个非 heap 的 AM 实现，其接口稳定性决定 Phase 2/3 的接入难度。
B+Tree 不实现 `UpdatableAM`（索引不 in-place update；索引更新 = delete + insert）。

---

## 十四、Access Method trait

**背景**：Heap 和 B+Tree 是 M2 两个 AM，Phase 2/3 还有 HNSW、Inverted。需要统一 trait。

**选择**：在 `pg-catalog` crate 定义**分层 trait**（v2 修订：拆 `UpdatableAM`）。

```rust
/// 所有 AM 的基础契约
pub trait AccessMethod: Send + Sync {
    /// AM 名称，对应 pg_am.amname
    fn name(&self) -> &'static str;

    /// 构建一个新的 relation（CREATE TABLE / CREATE INDEX）
    fn build(&self, ctx: &BuildContext) -> Result<()>;

    /// 插入一条 tuple（heap）或索引条目（btree）
    /// 返回 Result<()>。若调用者需要 tuple 的 TID（heap 场景），
    /// 应由 InsertContext 的 `out_tid: &mut Option<Tid>` 字段回填。
    /// v2.2 修订 P0-2：从 Result<Tid> 改为 Result<()> —— 对索引 AM 而言，
    /// "index entry 的物理位置" 不是 heap TID，返回 Tid 会误导上层。
    fn insert(&self, ctx: &mut InsertContext) -> Result<()>;

    /// 按 TID 定位（heap fetch）或按 key 扫描（btree lookup）
    fn scan(&self, ctx: &ScanContext) -> Result<Box<dyn TupleIterator>>;

    /// 删除（heap 逻辑删除标 xmax；index 物理删除）
    fn delete(&self, ctx: &DeleteContext) -> Result<()>;

    /// 返回该 AM 需要注册的 redo handler 列表
    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>>;
}

/// 支持 in-place update 的 AM（M2 只有 heap 实现）
pub trait UpdatableAM: AccessMethod {
    /// 更新（heap: 走 xmax + new tuple；btree 不实现此 trait）
    /// 新 tuple 的 TID 通过 UpdateContext.out_new_tid 回填
    fn update(&self, ctx: &mut UpdateContext) -> Result<()>;
}
```

### 14.1 Context 结构最小字段（v2.2 补齐 P2-6）

```rust
pub struct InsertContext<'a> {
    pub rel_oid:  Oid,
    pub snapshot: &'a Snapshot,
    pub tuple:    &'a [u8],           // AM 自行按 schema 解释
    pub out_tid:  Option<&'a mut Tid>, // heap 回填新 tuple 的物理 tid；索引传 None
}

pub struct ScanContext<'a> {
    pub rel_oid:   Oid,
    pub snapshot:  &'a Snapshot,
    pub predicate: Option<&'a dyn Fn(&[u8]) -> bool>, // M2 支持 = / < / > 单列
    pub start_key: Option<&'a [u8]>,                  // 仅索引 AM 用
    pub end_key:   Option<&'a [u8]>,
}

pub struct UpdateContext<'a> {
    pub rel_oid:      Oid,
    pub snapshot:     &'a Snapshot,
    pub old_tid:      Tid,
    pub new_tuple:    &'a [u8],
    pub out_new_tid:  &'a mut Tid,
}

pub struct DeleteContext<'a> {
    pub rel_oid:  Oid,
    pub snapshot: &'a Snapshot,
    pub tid:      Tid,
}

pub struct BuildContext<'a> {
    pub rel_oid:      Oid,
    pub schema:       &'a Schema,           // 列定义
    pub is_index:     bool,
    pub source_am:    Option<&'a dyn AccessMethod>, // CREATE INDEX 时读源表
}
```

Context 结构定义在 `pg-catalog`，字段可在 M2 后续 stage 追加（append-only）。

**关键契约**（v2 修订 P1-4 / v2.2 修订 P0-2）：
- `insert` 通过 `InsertContext.out_tid` 回填（heap 场景）；索引 AM 的 InsertContext 传
  `out_tid = None`
- Heap 回填的 TID 稳定（heap TID = tuple 物理位置；HOT 更新旧 TID 不变）
- `scan` 的可见性契约**按 AM 分类**：
  - **Heap AM**：`scan` 返回的 iterator 必须内部调用 `VisibilityOracle::is_visible` 过滤，
    上层看到的都是可见 tuple
  - **索引 AM**（B+Tree / 未来 HNSW / Inverted）：`scan` 返回 `(key_bytes, tid)`，
    **不做**可见性过滤（因为索引条目本身无 xmin/xmax）；上层拿 tid 到对应 heap AM
    `fetch_tuple(tid, snapshot)` 后再由 heap 层做可见性检查
- `redo_handlers` 在 `Engine::open` 时被收集并注册到 `RedoRegistry`
  - **P2-4 约束（v2.3 明确）**：`RedoRegistry::register(record_type, handler)` 若同一
    `WalRecordType` 已注册过，**立即 panic**（不静默覆盖、不返回 `Result`）—— 多个 AM
    错误声称拥有同一 record 类型是**配置 bug**，crash 后 redo 分派会不确定，必须开发期
    暴露；未注册的 record type 走 §11.2 "硬失败" 路径。同一 record 只允许一个 AM 拥有
    ownership，跨 AM 的通用 record（如 `FullPageImage`）由 `pg-storage` 自身注册。
- 上层执行 `UPDATE` SQL 时：先检查 `am.as_updatable()` → `Some(u)` 才走 in-place；
  索引则通过 delete + insert 组合

**索引条目一致性**：索引可能指向已 abort / 已 delete 的 heap tuple（索引 CLR /
vacuum 未完成）；heap 层可见性判定会天然屏蔽这些"悬空"引用，无需索引层保证。

**理由**：接口最小化，避免 B+Tree 被迫实现无意义的 `update`；未来 HNSW（append-only）
也可以只实现 `AccessMethod`。

**代价**：ScanContext 需要携带 snapshot、xid、可选 predicate；未来加 predicate
pushdown 时可能扩容。

---

## 十五、Vacuum 接口（M2 只留接口，M3 实现）

**选择**：M2 定义接口，不实现真实 vacuum。

```rust
pub trait Vacuumable {
    /// 扫描 dead tuple（xmax committed 且早于 oldest snapshot）
    fn scan_dead_tuples(&self, oldest_xmin: TxnId) -> Result<Vec<Tid>>;
    /// 回收 dead tuple 占用的空间
    fn reclaim(&self, tids: &[Tid]) -> Result<()>;
    /// 通知关联的索引 AM 清理对应条目
    fn notify_indexes(&self, tids: &[Tid]) -> Result<()>;
}
```

**M2 只做**：接口存在 + heap 的 `scan_dead_tuples` 实现（用于测试 MVCC 正确性）
**M2 不做**：`reclaim` 空间回收、autovacuum 后台线程

---

## 十六、依赖清单

新增依赖（在 M1 基础上）：

| Crate | 版本 | 用途 | 引入 stage |
|-------|------|------|-----------|
| `crossbeam` | 0.8 | Lock manager 的无锁等待队列、CLOG 读写并发 | M2b |
| `smallvec` | 1 | Snapshot.xip 通常 < 32 项，避免 heap 分配 | M2b |
| `uuid` | 1 | trace_id 编解码 | M2a |
| `arc-swap` | 1 | Catalog 快照原子换代（DDL 生效） | M2a |
| `loom` | 0.7 | 并发模型检查（B+Tree latch coupling） | **M2c**（M1 已推迟到这里） |

**继续不引入**：`anyhow`、`dashmap`（自研分区 HashMap）、`rocksdb/sled`。

---

## 十七、序列化与代码风格

- WAL payload 编码保持 M1 策略：bincode 2.x + serde，`encode_to_vec` / `decode_from_slice`
- Tuple 编码**手写**（不用 bincode）—— 需要精确控制 null bitmap 位序和列偏移
- Catalog 表内容也走标准 tuple 编码（catalog 就是 heap）
- 代码风格延续 M1：newtype、模块 doc、SAFETY 注释、`#[warn(missing_docs)]`
- **新增约定**：所有 crate 顶层 `Cargo.toml` 加 `[lints.rust] missing_docs = "warn"`

---

## 十八、M2 不做的事

明确推迟到后续阶段的内容：

- **HOT chain 完整实现**：M2c 有基础版；~~HOT prune 推迟到 M3 vacuum~~（口径回改，2026-08-31：M3 实际交付为整链回收、**部分死链不 prune**——M3 O3 决策，prune 需 LP_REDIRECT 页格式变更；归 Phase 7 随页格式演进，见 ROADMAP 附录技术债登记）
- **数据页 checksum**：M1 现状（无）延续到 M2；Phase 7 引入
- **Row-level Security predicate**：Phase 6
- **Sequence（自增列 SERIAL）**：~~M3~~（口径回改，2026-08-31：M3 O6 决定不做；归 Phase 4a，ROADMAP 已补录）
- **触发器 / 存储过程**：Phase 4a
- **CTAS / MATERIALIZED VIEW**：Phase 4a
- **VACUUM FULL / CLUSTER**：Phase 7a
- **并行查询**：Phase 4b+
- **Logical replication**：Phase 7d
- **SSI 完整实现**：Phase 7d
- **意向锁 IS/IX**：Phase 6
- **XID freeze**：**永不做**（64 位 XID）
- **TOAST 压缩**：Phase 7b
- **Multixact（共享行锁支持）**：M2c 简版；完整实现推迟 Phase 6

---

## 十九、M2 内部检查点

按 ROADMAP 的 M2a/M2b/M2c 三段划分：

### M2a：单语句 auto-commit（约 4–6 周）

- **前置**：Stage 0 债务清完
- **内容**：
  - Slotted page + Tuple 编解码
  - Catalog + 硬编码 bootstrap
  - Heap AM（INSERT/SELECT/UPDATE/DELETE 单线程）
  - Heap redo handlers (HeapInsert/Update/Delete)
  - **最小 TxnManager**：XID 分配（`begin_txn`/`end_txn`）+ TxnCommit/TxnAbort WAL 记录
    + auto-commit 每语句一个事务
  - **In-Memory ClogAccessor**（v2.2 修订 P1-5，取代 v2.1 的 `AbortedXidSet`）：
    实现同一 `ClogAccessor` trait（Stage 0b 定义），内部用
    `parking_lot::RwLock<HashMap<TxnId, TxnState>>` 存活状态。这样 M2a 的 Visibility
    Oracle 与 M2b 走**同一代码路径**，M2b 只需把 in-memory 版换成磁盘 SLRU 版
    （`ClogBuffer`），业务代码零改动。
    - `set_state(xid, COMMITTED/ABORTED)` 由 commit/abort 路径调用（顺序遵循 §3 P1-5
      Commit 硬约束）
    - `get_state(xid)`：查 HashMap；不存在时按"该 XID 早于 checkpoint 且默认已提交"处理
      —— M2a checkpoint 时**只清除 COMMITTED** 状态（节省内存），**ABORTED 必须保留**在
      HashMap 中；此后 xid < checkpoint_next_txn_id 且不在 HashMap → 视为 COMMITTED
      - v2.3 修订：明确禁止清理 ABORTED。若 ABORTED 也被清掉，crash 前提交的读者会因
        "xid 不在 HashMap" 误判为 COMMITTED，产生脏读。
      - 保底方案（M2a 单进程运行、事务量有限）：HashMap 只增不删，直到 M2b 换成磁盘
        SLRU 后彻底解决
    - Recovery：Analysis 扫 WAL，凡是 `TxnCommit` → `set_state(xid, COMMITTED)`；
      `TxnAbort` 或未 commit 的 XID → `set_state(xid, ABORTED)`
    - 内存开销：M2a 单事务顺序执行，HashMap 常态 O(未 checkpoint 事务数)
  - `pg_class`/`pg_attribute`/`pg_type`/`pg_am` 表能读写
- **验证**（v2.3 修订 P1-5、Q2）：
  - **正确性**：`CREATE TABLE` + 100 万条 INSERT + SELECT + crash 后数据一致；abort
    事务的 tuple 不可见
  - **并发压测**：**100 线程 × 1000 条 INSERT / 线程**（10 万总量）→ 校验 tuple 数量、
    xmin 单调、无 slot 冲突、CLOG 状态一致；M2a 虽是"单语句 auto-commit"，但多客户端
    并发提交必须走通同一 Visibility Oracle 路径
  - **时间线覆盖**：M2a 时间预算 4–6 周未包含 Stage 0a/0b 前置任务（Stage 0a 阻塞、
    Stage 0b 与 M2a 前 2 周并行）；见 §0 说明

### M2b：多语句事务 + MVCC + 单线程 B+Tree（约 6–8 周）

- **内容**：
  - Transaction Manager 完整版（begin/commit/abort，snapshot）
  - CLOG + Visibility Oracle + ClogBuffer
  - hint bit 回写
  - B+Tree AM（单线程版）+ redo handlers
  - `pg_index` 表 + `CREATE INDEX`（阻塞式）
  - ARIES Analysis + Redo 完整实现
- **验证**：BEGIN + 多语句 + COMMIT/ROLLBACK；SI 快照隔离；带索引扫描

### M2c：并发 + 死锁检测 + Blink Tree（约 6–8 周）

- **内容**：
  - Lock Manager（行锁 via xmax + 表锁 4 模式）+ 完整 row wait/wake 协议
  - 死锁检测（wait-for graph，100ms tick）
  - B+Tree 并发（latch coupling + Blink 变体）
  - HOT update 基础版
  - ARIES Undo（B+Tree CLR）
  - 100 并发压测
- **验证**：100 conn × 100 txn/s 无冲突；死锁能被检测并 abort 受害者

**M2 总计**：约 16–22 周（4–6 个月）；ROADMAP 给的 P50 范围内。

---

## 二十、决策验证计划

每条 M2 选型都要在实现中被验证。v2.3 P2-6 分类：**C = 正确性 must-pass**（不通过则 M2
不发布）；**P = 性能 target**（不通过标黄，允许下调或后续优化）；**S = 稳定性/回归**
（回归即失败）。

| 类别 | 决策 | 验证方法 |
|------|------|---------|
| P | 胖 tuple header 空间可接受 | M2a 结束：单表 1M 行小 tuple 场景，data_dir 大小 / tuple 大小 计算放大比 |
| C | XID-based snapshot 正确 | M2b：SI 快照测试（读者不看到并发写者未提交行） |
| P | Visibility Oracle 是热点 | M2b：`cargo bench` 加 tuple scan benchmark，若 CLOG 查询占比 > 30% 则调 hint bit |
| P | ClogBuffer 命中率 | M2b：bench 100K txn 混合负载，命中率应 > 95%（8 帧 × 8KB × 2 XIDs/byte = 128K XIDs 窗口，足够覆盖 100 并发事务的活跃窗口） |
| C | B+Tree Blink 变体正确 | M2c：并发 insert + concurrent scan，验证 range scan 无 miss |
| C | Split 3 步 CLR 正确恢复 | M2c：注入 crash 在 Prepare / Copy / Commit 后各一次，验证恢复后 B+Tree 有效 |
| P | 死锁检测 100ms 响应 | M2c：故意构造 2/3/4 事务环，测量检测延迟 |
| C | ARIES Undo 只需要处理 B+Tree | M2c：heap 层混沌测试（大量 uncommitted tuples）后 crash，验证 recovery 后 visibility 正确 |
| S | Stage 0 债务清完不引入回归 | Stage 0 结束：M1 全部集成测试 + crash_recovery 自动化 1000 轮通过 |
| C | WAL-before-data 协议不违反 | Stage 0：`test_wal_before_data_invariant` spy 单元测试固化 |

---

## 二十一、M2 交付物与接口边界

### 对外 API（`pg-engine` 暴露）

**v2.2 修订 P1-3**：M2a 先暴露程序化 API，避免和存储核心正确性抢时间；M2b 加硬编码
SQL parser（`BEGIN/COMMIT/ROLLBACK` + 简单 DML）；M3 接 PG Wire。

**M2a API（程序化，无 SQL parser）**：
```rust
impl Engine {
    pub fn open(data_dir: &Path, config: EngineConfig) -> Result<Self>;
    pub fn checkpoint(&self) -> Result<()>;

    // catalog / DDL
    pub fn create_table(&self, name: &str, schema: &[ColumnDef]) -> Result<Oid>;
    pub fn drop_table(&self, name: &str) -> Result<()>;

    // 单语句 auto-commit DML（M2a 无 begin_txn API）
    pub fn insert(&self, table: &str, values: &[Value]) -> Result<Tid>;
    pub fn scan(&self, table: &str, predicate: Option<Predicate>) -> Result<TupleIter>;
    pub fn update(&self, table: &str, tid: Tid, values: &[Value]) -> Result<Tid>;
    pub fn delete(&self, table: &str, tid: Tid) -> Result<()>;
}
```

**M2b API 追加（事务 + SQL 子集）**：
```rust
impl Engine {
    pub fn begin_txn(&self) -> Result<TxnHandle>;
    /// v2.2 修订 P2-7：auto-commit 场景传 None（不必先 begin_txn）
    pub fn exec(&self, txn: Option<&TxnHandle>, sql: &str) -> Result<QueryResult>;
}

impl TxnHandle {
    pub fn commit(self) -> Result<()>;
    pub fn abort(self) -> Result<()>;
}
```

M2b 硬编码 parser 支持的语句：`BEGIN` / `COMMIT` / `ROLLBACK` / `CREATE TABLE` /
`INSERT INTO` / `SELECT [WHERE eq/lt/gt] [ORDER BY 单列] [LIMIT N]` / `UPDATE`（带 WHERE） /
`DELETE`（带 WHERE） / `CREATE INDEX`。表达式仅支持列引用 + 字面量 + 单目算子；不支持
JOIN、subquery、聚合、类型转换。

### 内部关键接口（M3 起用）

| 模块 | 接口 |
|------|------|
| `pg-txn::TxnManager` | `begin/commit/abort/snapshot/wait_for` |
| `pg-txn::VisibilityOracle` | `is_visible(xmin, xmax, snapshot)` / `set_hint_bit` |
| `pg-txn::ClogBuffer` | `get_state(xid)` / `set_state(xid, state)` / `flush_dirty` |
| `pg-txn::LockManager` | `acquire_table_lock/release/detect_deadlock` |
| `pg-catalog::AccessMethod` | `name/build/insert/scan/delete/redo_handlers` |
| `pg-catalog::UpdatableAM` | `update` |
| `pg-am-heap::HeapAM` | 实现 AccessMethod + UpdatableAM + Vacuumable |
| `pg-am-btree::BTreeAM` | 实现 AccessMethod |
| `pg-storage::WalWriter` | `append(record) -> Lsn` (Stage 0 改签名，**不再隐式 flush_to**) / `flush_to(lsn)` (M1 已有) |
| `pg-storage::LsnClock` | `next(size) -> Lsn` (M1 已有，写入并推进) / `reserve(size) -> Lsn` (Stage 0 新增，**占位不写**) |
| `pg-storage::RedoRegistry` | `register(WalRecordType, Box<dyn RedoHandler>)` / `get` |

### Persistent Format 兼容性承诺

- M2 定型后：
  - PageHeader 26 字段字节 + 6 字节 padding = **32 字节**（v2.3-12 修订；早期文档曾写 28B 有误）布局固定
  - TupleHeader 64 字节布局固定
  - TOAST pointer 20 字节布局固定
  - WAL Record header 32 字节布局固定（M1 已定型）
  - Superblock v2 布局仅追加字段（复用双副本机制升级）
  - CLOG 4-bit bit order 固定（高 4 bit = 偶数 XID，低 4 bit = 奇数 XID）
- **M3+ 起，any on-disk 变更必须提供 migration 脚本或版本兼容读**

---

## 附录 A：与 M1 tech-selection 的差异

- M1 只关注**物理层**，M2 加入**逻辑层（tuple + txn + AM）**
- M1 单 crate；M2 六 crate 组织
- M1 允许简化（同步 fsync、Mutex<File>），M2 Stage 0 集中修复
- M1 无事务；M2 是完整 ACID 数据库的第一次真正落地

## 附录 B：从 M2 完成后能做什么

- Agent 元数据存取（session、user profile、config）—— ROADMAP Phase 1 目标
- 用于 Phase 2 HNSW 落地时的宿主环境（HNSW 节点也是 tuple）
- M3 加 PG Wire Extended 后，psql 可以连接跑基础 SQL

## 附录 C：v2 变更清单（对比 v1）

### 严重问题修正
1. **S1** PageHeader 字段合计 26B（非 24B），header 区实际 **32B**（26 + 6 字节 padding 满足 tuple 8 字节对齐；v2.3-12 修订后的最终值，v2 初稿曾写 28B 有误）；文档明确并写出 offset 表
2. **S2** TupleHeader 字段重排以满足 u64 8 字节对齐（t_agent_id 移到 offset 16，避免 v1 的 padding hole）
3. **S3** TOAST pointer 大小从 "18 字节" 更正为 "20 字节"（5×u32）
4. **S4** 命名从 "LSN-based Snapshot" 改为 "XID-based Snapshot Isolation"；`snapshot_lsn` 仅用于 hint bit 回写边界
5. **S5** `Snapshot` 新增 `current_xid` 字段；`VisibilityOracle::is_visible` 无需单独 current_xid 参数
6. **S6** Stage 0 新增 `LsnClock::reserve(size)` API（**占位不写**，用于 checkpoint FPI race 修复等场景先占 LSN 后 emit record）；M1 `next(size)` 是已有的"写入并推进"API，两者语义不同，共存不替代（v2.3-9 澄清）
7. **S7** CLOG bit 序显式规定：高 4 bit = 偶数 XID，低 4 bit = 奇数 XID
8. **S8** CLOG 采用独立 `ClogBuffer`（8 帧 SLRU），不复用 M1 BufferPool
9. **S9** ATT/DPT 快照从 CheckpointEnd payload 挪到 `meta/att-*.snapshot` / `meta/dpt-*.snapshot` 独立文件；CheckpointEnd 只存文件引用
10. **S10** Redo Phase 显式规定 "严格 LSN 顺序 + 统一分发（含 FPI）"；未注册 record type 硬失败

### 中等问题修正
- **M1** `AccessMethod` 拆分为 `AccessMethod` + `UpdatableAM`；B+Tree 不实现后者
- **M2** `Tid` 类型上移到 `pg-storage`；`pg-am-btree` 不再依赖 `pg-am-heap`
- **M4** BTreeSplit 拆为 Prepare/Copy/Commit 三条 record，各自 payload 完整列出
- **M5** M2a 引入最小 TxnManager（XID 分配 + TxnCommit/TxnAbort），非"零事务"
- **M6** Stage 0 债务清单追加 `next_oid` superblock 字段（offset 16..24）
- **M7** Stage 0 一次性保留所有 M2 record type discriminant（HeapHotUpdate=7、BTreeSplitCLR=50 等）
- **M8** `RedoContext` 结构体字段完整定义
- **M9** `RedoRegistry` 注册协议明确：一次注册、重复注册 panic、未注册硬失败
- **M10** §11.5 补齐 WAL-before-data flush 协议
- **M12** §9.1 补齐 xmax 行锁 waiter/wakeup 完整流程
- **M13** Stage 0 债务 #8：`ensure_data_dir` 追加 `clog/` 子目录（M1 已建 `data/wal/meta/tmp`，只缺 `clog/`；v2.3-13 修订）

### 其它调整
- Stage 0 工期从 1–2 周上调到 2–3 周（因追加 3 条债务）
- Superblock v1→v2 迁移路径显式规定
- 验证计划新增 `test_wal_before_data_invariant` 与 ClogBuffer 命中率指标
- Multixact 显式列入 §18 M2c 简版 + Phase 6 完整

### v2.1 追加修订（P1/P2 review 反馈）

**P1（正确性关键）**：
- **P1-1** `RedoContext.clog` 类型从 `&ClogBuffer` 改为 `&dyn ClogAccessor`（trait 定义在
  pg-storage），打破 `pg-storage ⇄ pg-txn` 循环依赖；Stage 0 提供 `NoOpClogAccessor` 占位
- **P1-2** §2 新增 `pd_lsn` 权威性契约：page 内 `pd_lsn` 是唯一权威源；BufferPool frame
  metadata 仅缓存；AM 修改后同 latch 内写 `page[0..8] = record.lsn`；Stage 0 加
  `test_pd_lsn_authoritative` 断言
- **P1-3** §19 M2a 补齐 `AbortedXidSet`（内存 HashSet + `TxnAbort` WAL 重建）—— M2a 不
  能仅靠 XID 大小判定，必须显式跟踪 aborted xid
- **P1-4** §14 `AccessMethod::scan` 的可见性契约按 AM 分类：heap 内部过滤；索引 AM
  返回 `(key, tid)` 不过滤，由上层 fetch heap 时再检查
- **P1-5** §3 新增 Commit 路径顺序硬约束：`wal.flush_to(commit_lsn)` → `clog.set_state` →
  `remove_active` → 允许 hint bit 回写；否则 WAL 丢失时 hint 与 CLOG 不一致导致可见性错误
- **P1-6** §20 ClogBuffer 窗口计算修正：8 帧 × 8KB × 2 XIDs/byte = 128K XIDs（v1 错为 32M）

**P2（补齐约束）**：
- **P2-1** §2 `pd_checksum` 明确 "M2 恒为 0，Phase 7 启用"
- **P2-2** §2 `pd_flags` 位分配表：bits 0..7 heap 保留未用；bits 8..15 仅 B+Tree 用
- **P2-3** §1 `Tid` 内存 10 字节 / 磁盘 12 字节区分标注
- **P2-4** §4 明确 TOAST chunk 复用 `HeapInsert`/`HeapDelete`，不引入新 record type
- **P2-5** §9.1 新增 "适用范围" 小节：所有写操作（含 INSERT/UPDATE/DELETE/FOR UPDATE）
  共用同一 xmax 协议
- **P2-6** §1 `Oid` 类型下沉到 `pg-storage`；`pg-txn::LockManager` 直接引用，避免
  `pg-txn → pg-catalog` 依赖
- **P2-7** §11.4 snapshot 文件清理明确 "下一次 checkpoint 的收尾阶段同步删除，保留最近 3 个"
- **P2-8** §7.1 `Snapshot.snapshot_lsn` 字段**移除**：hint bit 回写不需要 LSN 边界（后续
  reader 走 xip / xmax / CLOG 判定天然屏蔽未来事务）
- **P2-9** §10 `BTreeSplitCopy` discriminant 直接标 `51`（去掉 "6a→暂用" 表述）

### v2.2 追加修订（第三轮 P0/P1/P2 review 反馈）

**P0（必须修）**：
- **P0-1** §7.2 `is_visible` 对 `xmax == self_xid` 从"不见"改为"仍见"（M2 无 command
  counter 的简化取舍）；新增 M2b 4 个明确测试用例；显式说明 PG-兼容语义推迟到 Phase 6
- **P0-2** `AccessMethod::insert` 签名从 `Result<Tid>` 改为 `Result<()>`；heap 场景通过
  `InsertContext.out_tid: Option<&mut Tid>` 回填；索引 AM 传 None。避免误导上层"索引条目
  返回的 Tid 是 heap TID"

**P1（建议修）**：
- **P1-3**（对外 API）§21 M2a 改为暴露程序化 API（`create_table`/`insert`/`scan`/
  `update`/`delete`），M2b 才引入硬编码 SQL parser 支持 `BEGIN/COMMIT/ROLLBACK` + 简单 DML；
  M3 才接 PG Wire。同时 `exec` 签名改为 `exec(Option<&TxnHandle>, sql)` 支持 auto-commit
- **P1-4**（Stage 0 工期）§0 拆 Stage 0a（阻塞，1.5–2 周）+ Stage 0b（可与 M2a 并行，
  1.5–2 周）；合计 3–4 周。0a 交付接口/保留位/目录；0b 交付 Freelist CRC / RedoHandler /
  pwrite
- **P1-5**（M2a→M2b 切换成本）§19 M2a 用 `In-Memory ClogAccessor`（实现相同 trait），
  M2b 替换为 `ClogBuffer` 磁盘 SLRU，业务代码零改动。v2.1 的 `AbortedXidSet` 方案作废

**P2（锦上添花）**：
- **P2-6**（Context 结构）§14.1 补齐 `InsertContext`/`ScanContext`/`UpdateContext`/
  `DeleteContext`/`BuildContext` 的最小字段定义
- **P2-7**（exec auto-commit）签名改 `exec(txn: Option<&TxnHandle>, sql)`
- **P2-8**（ClogBuffer 可配置）§6.3 `frames: [ClogFrame; 8]` 改为 `Vec<ClogFrame>`；
  新增 `EngineConfig.clog_buffer_frames`（默认 8，可调至 64/256）
- **P2-9**（BTreeSplitCopy payload）§13.3 payload 从 `moved_tuples: Vec<Vec<u8>>` 改为
  `copy_start_slot: u16 + left_page_pre_lsn: Lsn`；redo handler 从 left_page 重算搬移
  内容；对齐 PG `xl_btree_split` 设计

### v2.3 追加修订（第四轮 P0-ish/P1 review 反馈）

**P0-ish（文档一致性）**：
- **v2.3-1** §3 Commit 路径注释、§7.3 Oracle 契约、§11.6 `RedoContext` 结构清理 3 处
  `AbortedXidSet` / `aborted_xids` 遗留 → 统一改为 `ClogAccessor` / `clog.set_state`；
  `RedoContext.aborted_xids` 字段删除，redo handler 通过 `ctx.clog.set_state(xid, ABORTED)`

**P1（正确性）**：
- **v2.3-2** §19 M2a in-memory CLOG 明确"**ABORTED 状态禁止清理**"：v2.2 写法会让
  crash 前提交的读者把 aborted xid 误判为 COMMITTED，产生脏读。checkpoint 只清 COMMITTED；
  保底建议 M2a HashMap 只增不删
- **v2.3-3** §3 `t_cid: u32` 字段（M2 tuple header 新增，offset 60..64；M1 未定义 tuple
  header，故非"占用原保留字段"，纯新增）+ §7.1 `Snapshot.curcid: u32` +
  §7.2 `is_visible` 签名新增 `t_cid` 参数、判定规则用 `t_cid < curcid` 区分同事务先前
  命令与当前命令。修复 v2.2 遗留的 "同事务 UPDATE 后 SELECT 返回旧+新双行" 缺陷。
  M2a `t_cid` 恒为 0、`curcid` 恒为 0，行为不变；M2b 每语句 curcid+=1 启用完整语义
- **v2.3-4** §0 Stage 0b 并行边界明确化：`RedoHandler` trait 和 `ClogAccessor` **trait
  定义**从 0b 提前到 0a（纯接口无依赖）；Stage 0b 只保留 `NoOpClogAccessor` 具体实现
  + `RedoRegistry` 装配。同时列出 M2a "可完全并行"和"必须等 0b" 的具体工作项

**v2.3 第五轮 review 补丁**：

**P0（必须修）**：
- **v2.3-5** §6.2 CLOG 段容量文字错误：`128MB × 2 XIDs/byte = 268,435,456` ≈ 2.68 亿，
  v2.2 误写 "512M"。修正为 "约 2.68 亿（~268M）个事务状态"
- **v2.3-6** §10.1 WAL Record discriminant 与 M1 `crates/pg-storage/src/wal/record.rs`
  对齐：`FullPageImage` 60→**10**、`CheckpointBegin` 80→**30**、`CheckpointEnd` 81→**31**；
  补充 M1 已预留的 `LogicalHnsw=100 / LogicalInverted=101 / LogicalGraph=102 /
  LogicalTimeSeries=103 / SegmentSeal=110 / SegmentMerge=111`。§0 债务 #7 同步声明
  "只追加、不重编号"

**P1（建议修）**：
- **v2.3-7** Superblock v1→v2 布局统一：债务 #6 与 §12 之前描述冲突。v2.3 与 M1
  `superblock.rs::encode()` 实际 layout 对齐：`next_oid` 放绝对 offset **40..48**（占用
  原 `created_at` 位置）；`created_at` 后移到 48..56、`crc32` 后移到 56..60。移除
  `data_len` 字段（M1 不存在）。迁移路径明确：v1 → v2 一次性搬字段
- **v2.3-8** §0 债务 #7 明确 `BTreeSplit=5` → `BTreeSplitPrepare=5` 是**重命名**而非新增
  discriminant；§10.1 表也同步加注

**v2.3 第六轮 review 补丁**（与 M1 现状对齐 + 描述失准修正 + 小澄清）：

**P0（必须修 — 影响 M2 实现的描述失准）**：
- **v2.3-9** §0 债务 #1 重写：M1 `WalWriter` 已实现 group commit（`writer.rs:135` worker
  + `wal_group_commit_batch_size` 配置），问题在 `append()` 内部隐式 `flush_to`
  （`writer.rs:184`）导致上层无法 batch WAL+commit。修复方式：拆 `append/flush_to` +
  新增 `LsnClock::reserve(size)`（M1 只有 `next(size)`，`lsn_clock.rs:43`）。
- **v2.3-10** §2 `pd_lsn` 权威性契约方向倒置：M1 **无** page 内 `pd_lsn` 字段（8KB 纯
  字节 buffer + frame metadata `page_lsn`）；M2 **新引入** page[0..8] `pd_lsn` 作为
  权威源，frame metadata 降级为只读缓存。方向澄清避免"契约"读起来像 M1 已存在。
- **v2.3-11** §11.5 WAL-before-data 协议：M1 `BufferPool::flush(page_id)`
  （`buffer_pool.rs:311`）已实现协议（`buffer_pool.rs:528-530`）；`Checkpointer` 在
  `CheckpointEnd` 后 `flush_to(end_lsn)`（`checkpoint.rs:204`）也已实现。Stage 0 只需
  (a) 验证 evict 路径; (b) 引入 `pd_lsn` 后从 `page[0..8]` 直读; (c) 新增
  `test_wal_before_data_on_evict`。同时修正方法名 `flush_page` → `flush`（M1 实际名）。
- **v2.3-12** §2 PageHeader 尺寸 26 → **32 字节**：v2.2/v2.3 早期误写 28 字节，28 =
  3.5×8 无法满足 tuple `u64` 字段 8 字节对齐。改为 26 字段字节 + **6 字节 padding** =
  32 字节；`pd_lower` 初值改为 32。
- **v2.3-13** §0 债务 #8 简化：M1 `io::ensure_data_dir`（`io.rs:26-33`）已创建
  `data/wal/meta/tmp` 四个子目录，v2.2 误报 "meta 不存在"。实际只缺 `clog/`，Stage 0a
  只需追加一行。

**P1（应该修 — 描述容易误导）**：
- **v2.3-14** §6.1 `TxnIdClock` 明确标注 "**M2 新增类型**"（沿用 M1 `LsnClock` 的
  `AtomicU64` 设计模式，`lsn_clock.rs:13-61`，但独立类型不复用代码）；避免误读为 M1 已存在。
- **v2.3-15** §1 `Oid` 类型明确标注 "**M2 新增**"：M1 `types.rs` 只有 `PageId / Lsn /
  TxnId / FrameId / Tid`，无 `Oid`。M2 新增 `pub struct Oid(pub u64);` 于 `pg-storage::types`。
- **v2.3-16** §3 `t_cid` 措辞修正：删除 "占用原 `_reserved[4]`" 误导语（M1 未定义
  tuple header，故非占用保留字段，纯新增）。附录 C v2.3-3 条目同步修正。
- **v2.3-17** §11.4 CheckpointEnd v1/v2 payload 迁移路径：M1 v1 payload = 3 字段
  （`record.rs:122-130`），M2 v2 payload = 6 字段（追加 `next_oid / att_file / dpt_file`）。
  版本判定通道 = `WalRecord.flags` 高 4 位（M1 = 0 → 隐式 v1）。decode 分支按版本
  分派；v1 默认 `next_oid=16384`、空 snapshot 文件路径 → Analysis 阶段做 full scan
  重建 ATT。前向 crash 保护：M2 首次启动读到 v1 不主动升级。
- **v2.3-18** §19 M2a 验证增补 "**100 线程 × 1000 条 INSERT 并发压测**"（10 万总量）：
  M2a 虽是单语句 auto-commit，但多客户端并发必须走通同一 Visibility Oracle 路径。
- **v2.3-19** §10.1 `BTreeSplitCopy=51` / `BTreeSplitCommit=52` 表格加注 "**Stage 0
  追加**（非 M1 保留判别子；M1 `WalRecordType` 未定义 51/52）"，与 v2.3-8 的
  `BTreeSplitPrepare=5` 重命名区分开。
- **v2.3-20** §2 `pd_checksum` 保留字段理由：M2 恒填 0 但**不删除字段**，理由：
  (1) on-disk 兼容 Phase 7 启用无需迁移；(2) 删除破坏 tuple 8 字节对齐；(3) debug 用途
  离线校验。

**P2（锦上添花）**：
- **v2.3-21** §6.4 CLOG 持久化时机去重：单一 authoritative 定义（Checkpointer 在
  CheckpointBegin 后 CheckpointEnd 前 fsync dirty CLOG，别处不主动 flush）；§11.4 §6.3
  仅引用不复述。
- **v2.3-22** §7.2 `Visibility::Uncertain` 注明 "M2a 单语句 auto-commit 无并发，本枚举
  永远不返回；保留为 M2b/M2c 行锁等待协议接口稳定"。
- **v2.3-23** §3 M2a t_cid 讨论：省 4 字节看似可行，但 (1) 引入 M2a→M2b on-disk 迁移
  逻辑（旧 header 60B 缺 t_cid 字段需扫表重写或加版本位）；(2) header 64 字节正好 8
  字节对齐，便于 memcpy / SIMD / cache line；60B 反而是 4 字节对齐。结论：M2a 也写
  `t_cid=0`，header 格式统一。
- **v2.3-24** §14 `RedoRegistry::register` duplicate 约束明确：同一 `WalRecordType` 二次
  注册立即 panic（非静默覆盖、非 `Result`）—— 多 AM 声称同一 record 类型是配置 bug。
- **v2.3-25** §6.3 `ClogBuffer` 8 帧默认值 rationale：默认覆盖 100 并发事务；生产 TP
  上调 64 帧（1M XIDs），OLAP 长事务 256 帧（4M XIDs）；范围 `[4, 1024]` 非法即 panic。
- **v2.3-26** §20 验证计划分类 **C / P / S**（Correctness / Performance / Stability），
  区分 must-pass 与 target。

**Q 小问题**：
- **v2.3-Q1** §0 债务 #4a 补 `ClogAccessor` 位置说明：trait 放 `pg-storage::clog`
  （因 `RedoContext` 持有 `&dyn ClogAccessor`），具体实现 `ClogBuffer` 在 `pg-txn`。
- **v2.3-Q2** §19 M2a 时间预算说明：4–6 周未包含 Stage 0a/0b 前置任务。
- **v2.3-Q3** §11.5 方法名 `flush_page` → `flush`（M1 实际名，与 v2.3-11 合并）。
- **v2.3-Q4** §7.1 `curcid` 递增时机文档化：语句**开始执行前** +1；同一语句内 self-scan
  共用同一 curcid（`t_cid < curcid` 为 false），避免 UPDATE 循环。
- **v2.3-Q5** §0 末尾新增"债务交叉索引表"，按 debt # 定位相关章节与关键 API。

**v2.3 第七轮 review 补丁**（历史漂移收尾 + 前一轮自引入不一致修正）：

**历史参考表同步**（第 6 轮修正正文但未同步这些位置）：
- **v2.3-27** 附录 C **S1** 从 "28B" 改为 "32B"（v2.3-12 一致）。
- **v2.3-28** 附录 C **M13** 从 "clog/ + meta/" 改为 "只 clog/"（M1 已建 meta/；v2.3-13 一致）。
- **v2.3-29** §21 兼容性承诺 PageHeader "26 字节（对齐后 28 字节）" 改为 "26 字段字节 + 6 字节 padding = 32 字节"（v2.3-12 一致）。
- **v2.3-30** §21 内部接口表拆分两个 API 描述：`WalWriter::append(record)` 加注"Stage 0 改签名，不再隐式 flush_to"、`flush_to(lsn)` 加注"M1 已有"；`LsnClock::next(size)`（M1 已有）与 `reserve(size)`（Stage 0 新增，占位不写）并列，删除误导性的"替代 next(0)"表述。附录 C **S6** 同步修正。

**前一轮自引入不一致修正**：
- **v2.3-31** §6.3 CLOG Flush 行改为"见 §6.4"引用，不再复述 CheckpointBegin/End 的时机（与 v2.3-21 声明的"仅引用不复述"一致）。
- **v2.3-32** §3 P2-3 与附录 C v2.3-23 论据修正：不再声称"省 4B 破坏 8B 对齐"（tuple 起点由 pd_upper 决定，与 header 长度无关）；改为 (1) M2a→M2b on-disk 迁移代价；(2) header 64B 天然 8 字节对齐便于 memcpy / SIMD / cache line。

**第八轮 review — 全文重读发现的三处内部不一致**：
- **v2.3-33** §2 slotted page ASCII 图与正文 32B 结论对齐：`PageHeader (26 bytes)` → `PageHeader (32 bytes total = 26 字段 + 6 padding)`；`padding (2 bytes) offset 26..28` → `padding (6 bytes) offset 26..32`；`LinePointerArray 从 offset 28 起` → `从 offset 32 起`。修复 v2.3-12 遗留的图文不一致。
- **v2.3-34** §10.2 WAL record 版本号语义与 §11.4 CheckpointEnd v1/v2 对齐：`0 = M2 v1` 改为 `0 = M1 legacy / 隐式 v1（M1 flags=0）`、`1 = M2 v2 payload`，M2 emit 时写 `flags = (1 << 12)`。避免"§10.2 用 0 表示 M2、§11.4 用 0 表示 M1"的相互矛盾。
- **v2.3-35** §7.2 M2a 简化段修正误导性表述："self_xid 全部可见 (xmin case) / 全部仍见 (xmax case)"在 M2a 单语句 auto-commit 下反而不成立——xmin/xmax 必然是历史已提交 XID，`xmin == self_xid` / `xmax == self_xid` 分支根本不触发。改为明确说明 M2a 走 dead code path、t_cid/curcid = 0 只是占位默认值，行为与 v2.2 一致；M2b 才激活 curcid 递增语义并消除 v2.2 缺陷。

