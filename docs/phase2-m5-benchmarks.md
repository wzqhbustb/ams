# Phase 2 M5 Benchmarks(Stage E 落盘文档)

> 合同:docs/phase2-m5-coding-plan.md 阶段 E(253–278 行)。本文档收录机器规格、
> §13.2 恢复时间实测、写放大实测 vs §4.2 核算、崩溃轮次结果、skip-ahead 实测、
> 与 M4 既有数字的无回归对照。
> 状态:**待 CI tarpaulin 终判 + ⑧ 出口 tag**——§13.2 验收 PASS(2026-09-23
> 实测);1000 轮崩溃验收 1000/1000 绿(§4);Stage E ⑥ 全量回归六段全绿(§6);
> 覆盖率判定以 CI 报告为准(coding-plan:264,未闭合)。

## 1. 机器规格

| 项 | 值 |
|---|---|
| 本机(benchmark 主力) | Apple M3 Max(Mac15,9),48 GB RAM,macOS 14.5 (23F79),APFS SSD |
| 工具链 | rust stable;MSRV 1.86 由 CI msrv job 把关 |
| 页大小 / WAL 段 | 8 KiB(编译期常量)/ 16 MiB |
| 恢复实测引擎配置 | buffer pool 4096 MiB(默认 128 MiB 会把 1M 建库变成驱逐基准,非 §13.2 测量对象);group commit batch 64 / 2 ms;checkpoint 仅显式触发(生产路径从不启动后台 checkpoint 线程) |

## 2. §13.2 恢复 <30s 实测(首测 2026-09-23;终审修复后复跑 2026-09-30,均主线亲跑)

**口径钉死(§9.2/§13.2)**:<30s 只对 **checkpoint 之后的增量窗口**成立——
bulk load 完成点必须立即 checkpoint,否则该保证不成立。本测试同时是该截断的
实证:重放窗口字节 = 增量段 WAL(若 checkpoint 未截断,窗口将是全量 5.6 GB,
预算必破)。

**形态**:1M 向量建库(dim=128,HnswParams::default() M=16/m_max0=32/efC=200,
Heuristic,种子 42)→ 立即 checkpoint → 100k 增量 insert → 活引擎 SIGKILL
(锁文件在场钉死"杀的是活引擎")→ 计时 `Engine::open` 到可查询。

### 结果:**PASS,复跑 27.66s < 30s(余量 2.3s ≈ 7.8%)**

| 段 | 耗时 | 说明 |
|---|---|---|
| `Engine::open`(分析 + redo 重放 + redo 脏页批量 flush) | **27.65 s** | 重放窗口 1,149,520,976 B(LSN 4,450,247,784 → 5,599,768,760;自 CheckpointBegin 计,含 Begin/End 两记录 +152 B) |
| 重放窗口记录数 | **2,215,451(实测)** | 精确 WAL 扫描(harness 内置计数器,十字钉恰落 final_lsn;扫描 2.99s 在预算外)。比率 22.2 records/insert(含 FPI);旁证:20k 探针 426k、500 增量 smoke 10,657 |
| catalog 解析 + `open_hnsw_index` | 2.59 ms | **含 1.1M 步 skip-ahead 重推**——顺带成为 skip-ahead 实测耗时数据点(§5) |
| 首个 `hnsw_search`(k=10,§11.4"可查询"终点) | 1.56 ms | |
| **合计(预算 30s)** | **27.66 s** | |
| 预算外:§11.3 全量审计(1.1M 节点) | 708 ms | 计时后 sanity:node_count/live = 1,100,000,三类残态与 tombstone 全 0,hwm 十字钉 |

**首测留档(2026-09-23,flush 批量修复那轮)**:27.99s(余量 2.0s ≈ 6.7%),窗口
1,149,520,824 B(LSN 4,450,247,936 → 5,599,768,760,自 checkpoint 后 WAL 尾计);
catalog+open 2.99ms、首查 1.35ms、审计 659ms。复跑(2026-09-30)在 evict_frame
flushing 跳过、ChildGuard、计数器等终审修复之后进行,结论不变且略快;窗口字节
口径差 +152 B = CheckpointBegin/End 两条记录(计数器把 checkpoint_lsn 前移到
Begin 起点——Begin 记录的 append 位置,窗口自此含标记记录,与 redo 扫描窗口一致)。

`Engine::open` 内部分解(二轮审查裁决:4GB 池初始化是真实 open 成本,留在测量内,
在此分解以免误读为 redo 变慢):

| 成分 | 耗时 | 来源 |
|---|---|---|
| 固定成本(4GB 池 524,288 帧初始化 + 引擎/catalog/WAL 打开) | ≈ 0.46 s | 独立探针:同配置 2 条 insert 的 Engine::open 实测 455.6ms |
| redo 重放(读+解码+应用 ~1.15 GB 窗口,实测 2,215,451 条记录)+ 批量 flush | ≈ 27.2 s | 27.66 − 0.46(复跑) |

### flush_all_dirty 修复(本验收的前置,2026-09-23)

两点边际探针(5k/20k 增量)实测修复前 redo 边际 ~0.63 ms/insert,100k 窗口
外推 ~63s——预算将被 2× 击穿。临时插桩定位:426k 记录 read 544ms + apply
174ms,**收尾逐页 flush 9.94s = 80%**(`flush()` 的 group-fsync 合并只对并发
flush 者生效,顺序循环 = 每页一 fsync,实测 ~5.6 ms/页)。修复为
`BufferPool::flush_all_dirty`(批量 write + 单次 sync_all;H1 不变量保持;
loom 双变体同步),replay_wal 切换,checkpoint 路径不动(登记为后续候选)。

| 窗口 | 修复前 | 修复后 |
|---|---|---|
| 20k insert(72,124,080 B) | 12.44 s | **2.39 s**(5.2×;重放窗口字节逐位相同,建库路径零变更) |
| 100k insert(1.15 GB,2,215,451 条记录) | 外推 ~63 s(必挂) | **27.66 s(PASS,2026-09-30 复跑)** |

### 敏感性声明(诚实登记)

- **余量薄(复跑 7.8%,首测 6.7%)**:余量由窗口大小驱动,而窗口比设计估算大 ~4×(见 §3 FPI
  放大)。磁盘更慢的机器(或机械盘)同等窗口可能不过;反之若窗口符合 §4.2
  估算(~290 MB),重放约 7s,余量 4×。
- 口径仅覆盖进程崩溃(SIGKILL)形态:增量页多为刚写入,OS 页缓存命中;断电
  (页缓存丢失)形态的重放要慢(随机 8KB 读),不在 §13.2 承诺内。
- debug 档不作为验收口径(量级差异见 M4 §3 同型先例)。

## 3. 写放大实测 vs §4.2 核算

§4.2 核算:~2.9 KB/insert ≈ 5.6×(v1.12 口径,向量 512 B)。

| 段 | WAL 字节 | /insert | 放大倍数 |
|---|---|---|---|
| 建库段(1M insert,零 checkpoint → 零 FPI) | 4,450,247,936 B | **4.45 KB** | **≈ 8.7×** |
| 增量段(100k insert,checkpoint 后,含 Begin/End) | 1,149,520,976 B | **11.5 KB** | ≈ 22.4× |

**偏差登记(如实)**:稳态逻辑记录实测 4.45 KB/insert,比 §4.2 估算高 1.53×——
估算按典型邻接表长记账,1M 规模下 level-0 邻接表满 32 项、每次 insert 的
SetNeighbors 更新面(回边)更大,外加逐记录帧头;估算公式本身未随规模参数化。
增量段再加 ~7 KB/insert 的 FPI 放大(checkpoint 后每页首触全页像,~100k 页
× 8 KiB ≈ 800 MB,占窗口 ~70%)。§4.2 的数字口径(用于否决全页像方案 (b) 的
相对比较)不受影响;绝对值以本文档实测为准。

## 4. 崩溃轮次结果

| 配置 | 结果 | 耗时 | 来源 |
|---|---|---|---|
| 25 轮(CI 默认) | 25/25 绿 | 74.7 s | Stage D slice 3,2026-09-23 主线亲跑 |
| 1000 轮(验收) | 首跑 FAIL / 复跑 FAIL round 521 → 根因闭合后**第三跑 1000/1000 绿** | 首跑 1405.8s / 复跑 1119.0s / 第三跑 2274.5s | 见下"首轮红登记";复跑命令 `M5_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m5_hnsw_crash_rounds --release -- --nocapture` |

**首轮红登记（2026-09-29，如实）**：首跑 FAIL 且详情被 `tail -5` 截断（教训：长跑验收必须全量落日志）；复跑 FAIL 于 round 521(mid 模式）——k=5 查询在 live=71 的恢复图上仅 1 命中。确定性窗口探针实证：§8.1 步骤 5 内崩溃留下"有入边、空出边"的幽灵，查询下降走入即被困——**合法残态，非腐坏**(slice 2 矩阵早已用 count-无关断言钉过该窗口族；crash-rounds 的 exact-k 断言过严，其"live ≥ 30 ⟹ 够 k 条"前提被证伪）。修复零生产代码：sanity 改"非空 + 命中 < hwm"，常驻回归 `m5_ghost_trap.rs` 两枚钉死陷阱形态，M6 候选登记（步骤 5↔6 对调可消除陷阱，归 M6 幽灵回收协议评估）。细节：coding-plan v1.66 / stage_spec Stage E 节。

另有 Stage D slice 2 的 mem::forget 窗口矩阵 13 枚(§8.2 十行窗口 × 两形态
逐点覆盖)与 slice 4 的恢复后召回门(§6)作为互补证据。

## 5. skip-ahead 实测耗时

重开时 rng 流位按 hwm 精确推进(恰 hwm 次抽取,与崩溃前参照流逐位同步——
Stage C slice 3 机制)。实测:hwm = 1.1M 时,catalog 解析 + `open_hnsw_index`
(含 1.1M 次抽层推进 + 目录链尾定位 + meta 读取)**合计 2.59 ms**(§2 表内,
2026-09-30 复跑;首测 2.99ms 留档)。
skip-ahead 的 O(hwm) 成本在百万级可忽略。

## 6. M4 既有数字无回归对照

| 指标 | M4(内存形态) | M5(页驻+WAL) | 结论 |
|---|---|---|---|
| siftsmall recall@10(冻结参数 16/32/200/64/seed42/L2) | 0.9990 | **0.9990(恢复后,逐位同值)** | §11.3 ② 门 PASS,恢复对检索质量零损耗 |
| 跨形态位级 parity | — | 10k 真实规模 100 查询逐位相等(Stage D slice 4 孪生钉) | 页驻实现与内存参照行为恒等 |
| recall_siftsmall 硬门 | 0.9990 ≥ 0.98 | **0.9990 ≥ 0.98**(Stage E ⑥ 复跑,逐位同值;flush_all_dirty 改动在其后复跑确认) | 无回归 |

M4 其余数字(建图 4.22s/10k、P50/P99、快照 save/load)见
docs/phase2-m4-benchmarks.md §3;M5 未改动其代码路径(Stage C 泛型化抽取
零行为变更,经对拍套件逐位验证)。

## 7. 复现命令

```bash
# §13.2 恢复验收(手动/nightly;默认 1M+100k,~3.6h,大头在建库)
M5_RECOVERY_BENCH=1 cargo test -p pg-engine --test m5_recovery_time --release -- --nocapture
# 小规模 smoke(数秒)
M5_RECOVERY_BENCH=1 M5_RECOVERY_BASE=2000 M5_RECOVERY_INCREMENT=500 M5_RECOVERY_POOL_MB=256 \
  cargo test -p pg-engine --test m5_recovery_time --release -- --nocapture
# 1000 轮崩溃验收(~33-40 min)
M5_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m5_hnsw_crash_rounds --release -- --nocapture
# 恢复后召回门(需 siftsmall 数据集)
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test m5_recall_after_recovery
```
