# Phase 1 M2 Benchmarks（Stage T 验收记录）

本文档记录 Phase 1 M2（pg_rust PostgreSQL 内核重写）Stage T 的 benchmark 清单、
目标值（coding-plan Stage T，P 类 target 按 v2.3 §20 允许下调）与实测值。

## 环境

| Item | Value |
|------|-------|
| Date | 2026-08-12 |
| OS | macOS / APFS（F_FULLFSYNC ≈ 4–7 ms） |
| CPU | Apple Silicon |
| Build profile | `bench`（criterion 默认优化） |

> ⚠️ 除标注外，下表数字为 **smoke 运行**（`--measurement-time 1..3 --sample-size 10`，
> 部分 bench 用 env 缩小规模）的单次采样，仅证明各 bench 可运行并给出量级；
> 正式验收数值需在长采样（默认参数）下重测。macOS APFS 上 fsync 绑定型负载
> 的运行间方差约 2×（见 phase1-m1-benchmarks.md）。

## 验收命令对照（coding-plan Stage T）

| 验收命令 | 文件 | 状态 |
|----------|------|------|
| `cargo bench -p pg-storage --bench wal_group_commit` | `crates/pg-storage/benches/wal_group_commit.rs` | 存在，可运行 |
| `cargo bench -p pg-storage --bench buffer_pool_concurrent_flush` | `crates/pg-storage/benches/buffer_pool_concurrent_flush.rs` | 存在，可运行 |
| `cargo bench -p pg-txn --bench clog_buffer_hit_rate` | `crates/pg-txn/benches/clog_buffer_hit_rate.rs` | 存在，可运行 |
| `cargo bench -p pg-am-btree --bench create_index` | `crates/pg-am-btree/benches/create_index.rs` | 存在，可运行 |
| `cargo bench -p pg-engine --bench m2c_100_conn` | `crates/pg-engine/benches/m2c_100_conn.rs` | **Stage T 新增**，可运行 |

清单外补充（spec 的 Benchmark 集合要求的另两项）：

| Benchmark | 文件 | 状态 |
|-----------|------|------|
| heap INSERT-UPDATE-DELETE 混合 | `crates/pg-am-heap/benches/heap_mixed.rs` | **Stage T 新增** |
| B+Tree split 吞吐 | `crates/pg-am-btree/benches/btree_split.rs` | **Stage T 新增**（`create_index` 只覆盖批量 build，不覆盖在线 split） |

## 目标 vs 实测

| Benchmark | Target（P 类） | Smoke 实测 | 状态 / 未达标原因 |
|-----------|---------------:|-----------:|-------------------|
| WAL 顺序写（`wal_group_commit`） | ≥ 200 MB/s（继承 M1） | 3.1 / 18.3 MiB/s（两个 arm） | **未达标**：M1 起已知 —— macOS F_FULLFSYNC ≈4–7 ms，group-commit 被 fsync 延迟封顶；非 Stage T 回归（见 phase1-m1-benchmarks.md 与各 bench 头注释） |
| BufferPool 随机读（`buffer_pool_random_read`） | ≥ 50K ops/s（继承 M1） | ~2.1M ops/s | 达标 |
| BufferPool 并发刷脏（`buffer_pool_concurrent_flush`） | —（无硬性 target） | 120–288 flush/s（各 arm） | 记录值，fsync 绑定 |
| ClogBuffer 命中率（`clog_buffer_hit_rate`） | ≥ 95%（8 帧）；≥ 99%（256 帧） | **98.44%**（8 帧，hits=1968821 misses=31179）；**100.00%**（256 帧） | 达标 |
| `create_index`（1M 行 INSERT + 阻塞建索引） | ≤ 30s | insert_1m = 15.96s + open+build ≈ 1.86s ≈ **17.8s** | 达标 |
| heap INSERT 单线程（`heap_insert`，Stage K 继承） | ≥ 20K TPS | 见 bench 头注释（no-fsync arm 为纯路径上限） | fsync 绑定；P 类 |
| heap INSERT 100 并发（`m2c_btree_tps`） | ≥ 15K TPS | 头注释记录：100T auto-commit ~6.6K TPS；single-txn arm ~13.5K TPS | **未达标**：fsync 封顶 ~11–12K TPS（group commit 路径），需 batch-commit 摊薄 fsync；P 类，已实测记录非伪造 |
| 索引点查（`index_lookup`） | ≥ 100K QPS | 由 `m2c_100_conn` 混合负载内含点查；独立 QPS 未单测 | P 类缺口（见下） |
| heap INSERT-UPDATE-DELETE 混合（`heap_mixed`，新增） | —（清单要求存在即可） | 1T ≈ 383 heap-ops/s；8T ≈ 2.38K；32T ≈ 9.21K（3 ops/txn，含 1 次 commit fsync） | 记录值；单线程受 commit fsync 限制，并发随 group commit 提升 |
| B+Tree split 吞吐（`btree_split`，新增） | —（清单要求存在即可） | ~522 wide-key insert/s（~500B key，约每 15 次插入一次叶分裂；结尾 `validate()` + `tree_level ≥ 1` 断言防空转） | 记录值；WAL append 在后台 fsync 持锁期间停顿（同 `heap_insert` 已知问题） |
| 100 并发混合读写（`m2c_100_conn`，新增） | 50conn×100txn/s×30min（保底）/ 100conn×100txn/s×60min（挑战）——稳定性目标 | smoke（8conn×6ops）≈ 1.09K ops/s | criterion 短采样 bench；稳定性验收由 `tests/m2c_stress.rs` 的 paced 长跑承担（命令见其头注释），不在 bench 内 |

## 缺口与说明

- **✅ Stage T 发现引擎缺陷 —— 已修复（pg-am-btree split Commit 的
  FPI-before-commit 排序）**：并发 crash 轮（`m2b_crash_rounds_concurrent`）
  开启 checkpoint 线程时曾稳定复现索引恢复损坏。精确根因（法医转储
  /tmp/conc_repro_35 的 WAL 时间线）：split 的 Copy（@1190944，左页置
  SPLIT_INCOMPLETE）之后、Commit 之前，checkpoint 开启新 FPI 周期
  （CheckpointBegin @1200480）；`split_commit` 当时**先 append
  BTreeSplitCommit 记录（@1209304）再 pin_mut left 页**，该 pin_mut 触发的
  周期 FPI 落在 Commit 之后（@1209856）却捕获提交前镜像（SPLIT_INCOMPLETE
  未清除）——违反 FPI 不变式（FPI 内容必须包含所有 LSN < FPI 位的修改）。
  FPI redo 无条件整页恢复并把 pd_lsn 补到 FPI 的 LSN，Commit redo 的 pd_lsn
  守卫于是跳过清标志/插 downlink，已提交 split 被回滚成"未完成"，undo 的
  H3 页扫描发出伪造 BTreeSplitCLR 向父页重复插入 downlink（`validate` 报
  entries out of order）。修复：split Commit 在固定记录的 WAL 位置**之前**
  用 scoped `pin_mut` 预触（pre-touch）其修改的每个页面（parent→left
  降序），使到期的周期 FPI 恒以小于 Commit 的 LSN 落 WAL；append 之后
  改用 `BufferPool::pin_mut_without_fpi` 重取做 apply，杜绝"预触→重取"
  窗口内 checkpoint 发布引发的第二个 stale FPI。（注意：曾被否决的
  hold-across 方案——持 left 页 latch 跨 append/父页修改直到 apply——会
  与乐观路径的 right-hop/coupling latch 编排死锁，btree_concurrent 全压下
  2/2 复现挂死。）复核后进一步关闭了第三方残余竞态：乐观/悲观写路径
  （`pin_leaf_for_write` / `descend_write_path`）改为
  `pin_mut_without_fpi` 取锁，且对 SPLIT_INCOMPLETE 页**整段持有期跳过
  `ensure_fpi`**——窗口内写按 Stage S 设计放行但绝不发 FPI（无 FPI 即无
  stale 镜像；`btree_undo_clr` 的两个 in-window 测试为此契约）。初版
  "flagged 且 FPI 到期才升级"方案因 check-then-emit TOCTOU 被复核否决；
  guarded 根分支对 new_root 的裸 `pin_mut` apply（checkpoint 于
  create_new_root→apply 窗口刷新根时会打出缺 slot-1 downlink 的 stale
  FPI，静默丢右孪且 undo 无法修复）已补预触 + `pin_mut_without_fpi`。
  确定性回归测试
  `pg-am-btree/tests/btree_split_crash.rs::test_btree_split_commit_fpi_precedes_commit_record`
  / `test_third_party_write_on_committing_leaf_escalates_without_fpi` /
  `test_btree_split_commit_guarded_root_branch_new_root_fpi_order`
  （均修复前红、修复后绿）。并发 crash 轮的 checkpoint 线程已默认开启
  （`M2B_CRASH_CONC_CKPT=1` 改为 20ms 激进档加压）。
- **✅ 激进 checkpoint 档追加暴露的预存引擎缺陷 —— 已修复（恢复侧 loser
  索引补偿）**：20ms 档压测下并发 crash 轮以 ~5/8 复现"行在堆中、索引不可达"
  （validate 绿）。法医转储（/tmp/conc_repro_round2）显示：kill -9 落在
  非 HOT UPDATE 的索引维护（BTreeDelete+BTreeInsert，xid=0）已落 WAL 而
  TxnCommit 未落的窗口内 → 恢复后 loser 事务的索引删除无补偿（在线 abort
  靠 per-txn index_undo 日志，恢复路径此前没有对应物），堆中仍可见的行丢了
  索引条目。修复：`Engine::open` 新增 loser 索引补偿（收集 loser 的
  HeapDelete/非 HOT HeapUpdate 受害 tid，按堆页字节重算键并幂等重插；
  pg-storage 暴露 `recovered_redo_start`，HeapAM::relation_pages 转 pub）。
  扫描起点经复核修正：redo_start 只对 DPT 采样时仍脏的页回拉，受害页若
  在 checkpoint 前被驱逐刷盘，其删除记录会落在 redo_start 之前、同一
  retained 段内——因记录可跨段、段首非记录边界，扫描起点经 CRC 校验的
  resync 探针回卷到 redo_start 所在段内的首个记录
  （`first_record_lsn_in_segment`）。
  回归 `m2b_index_txn.rs::crash_mid_delete/update_compensates_index_entry`
  （HEAD 上红、修复后绿——证明与 Stage T 其他改动无关）与
  `crash_loser_delete_before_redo_start_compensated`（小段+小池驱逐构造，
  去掉 resync 回卷即红）。已知边界（文档化）：删除记录所在 WAL 段已被
  回收时不可补偿（需跨多个 checkpoint 的手持长事务）。
- **索引点查 ≥100K QPS** 没有独立的 criterion bench：SQL 层点查混在
  `m2c_100_conn` 的混合负载中计量。如需要可在后续补一个 `index_lookup`
  循环 micro-bench（AM 层 `BTreeIndex::lookup` 在 `pg-am-btree` 功能测试
  之外亦无专用 bench）。
- WAL / 100 并发 INSERT 两个 fsync 绑定项自 M1/Stage Q 起即为已知硬件限制
  （macOS F_FULLFSYNC），按 v2.3 §20 P 类 target 允许下调处理；数字为实测，
  未达标原因已记录。
- 长跑验收（50/100 conn 稳定性、1000 轮 crash、1000 次死锁注入）由以下
  env 配置手动触发，CI 默认均为短配置：
  - `M2C_STRESS_CONNS=50 M2C_STRESS_TPS=100 M2C_STRESS_SECS=1800 cargo test -p pg-engine --test m2c_stress --release -- --nocapture`（保底）
  - `M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100 M2C_STRESS_SECS=3600 ...`（挑战）
  - `M2B_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m2b_crash_rounds -- --nocapture`
  - `M2B_CRASH_CONC_ROUNDS=1000 cargo test -p pg-engine --test m2b_crash_rounds m2b_crash_rounds_concurrent -- --nocapture`
  - `M2C_DEADLOCK_ITERS=2000 cargo test -p pg-engine --test m2c_deadlock_stress --release -- --nocapture`
- **长跑实际执行记录**（2026-08-13，本机 Apple Silicon）：
  - ✅ 保底压测 50 conn × 100 txn/s × 30min：通过（1802.7s，终态堆↔索引一致、无泄漏）
  - ✅ 1000 轮 crash（单线程 harness，含 split-in-progress）：通过（3043s）
  - ✅ 死锁注入 1000 环（2000 迭代）：通过（每环恰好 1 victim、无环对照组 0 误报；实测最大检测延迟 ~78ms，p99 远低于 200ms 验收线；具体 p99 未单独落盘——上界即 tick 间隔 100ms）
  - ⚠️ 挑战档 100 conn × 60min：**未执行**（保底档通过；时间成本考虑，可随时按上条命令补跑）
  - ⚠️ 并发 crash 1000 轮：**未执行**（执行了 40 轮 × 20ms 激进 checkpoint 档，修复两个 bug 后连跑全绿；1000 轮约需数小时，留给 Stage T 后续或 Phase 7 稳定性专项）
- **M2c churn 基线（M3 Stage A 的性能对比基线，S2 协议）**：2026-08-21 实测
  （worktree @ 2731a8b，release）：`M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100
  M2C_STRESS_TPS=100` × 5 轮取 achieved txn/s：84 / 86 / 87 / 88 / 89，
  **均值 86.8（区间 ±3%）**。M3 Stage A/D/G 的回归判定以此为准。

## 复现

```bash
# 全量（长采样）
cargo bench -p pg-storage --bench wal_group_commit
cargo bench -p pg-storage --bench buffer_pool_concurrent_flush
cargo bench -p pg-txn --bench clog_buffer_hit_rate
cargo bench -p pg-am-btree --bench create_index
cargo bench -p pg-am-btree --bench btree_split
cargo bench -p pg-am-heap --bench heap_mixed
cargo bench -p pg-engine --bench m2c_100_conn

# smoke（缩小规模 + 短采样）
M2C_BENCH_CONNS=50 M2C_BENCH_OPS=10 cargo bench -p pg-engine --bench m2c_100_conn -- --measurement-time 3 --sample-size 10
HEAP_MIXED_OPS=5 cargo bench -p pg-am-heap --bench heap_mixed -- --measurement-time 2 --sample-size 10
BTREE_SPLIT_KEYS=200 cargo bench -p pg-am-btree --bench btree_split -- --measurement-time 2 --sample-size 10
```
