# Phase 1 M3 Benchmarks（Stage G 收口记录）

本文档记录 Phase 1 M3（Vacuum / 可观测性 / pg-wire / 接口预留）Stage G 收口的
benchmark 清单、目标值与实测值，格式对齐 `phase1-m2-benchmarks.md`。
M3 的性能硬门槛只有一条（§12.5）：**100 并发 CRUD TPS 按 S2 协议对比 M2c
基线无统计显著回归（<5% 上限）**；其余为观测落盘项。

## 环境

| Item | Value |
|------|-------|
| Date | 2026-08-27 |
| OS | macOS / APFS（F_FULLFSYNC ≈ 4–7 ms） |
| CPU | Apple Silicon |
| Build profile | release（S2 / churn soak / waldump / WAL 字节探针）；全量回归 debug + release 双档 |

S2 基线（M2c，2026-08-21 实测，`docs/phase1-m2-benchmarks.md`）：
`M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100` × 5 轮
84 / 86 / 87 / 88 / 89，**均值 86.8 txn/s（区间 ±3%）**。

## 目标 vs 实测

| 项目 | Target | 实测 | 状态 |
|------|--------|------|------|
| churn 页数有界（ROADMAP.md:216，Stage D） | 堆页数收敛（不随轮数线性增长）+ 稳态高水位增长 ≤ 0.25 页/轮 + 8 页余量 | 30 轮默认档全绿；**200 轮 release soak 通过（8.74s；Stage D 时 8.4s）**，稳态漂移实测 ~0.18 页/轮（界 33 页内涨 18 页），漂移源 = 已声明的 btree 无页合并残留 | 达标 |
| 注册开销（Stage A，S2 协议） | 对比 M2c 基线 <5% 回归上限 | **87.2 txn/s vs 86.8（+0.5%，噪声界 ±3% 内）** —— 快照注册开销不可测，R2 通过 | 达标 |
| vacuum 叠加后 TPS（Stage D，S2 协议） | 同上 | 89 / 87，**均值 88.0 vs 86.8（+1.4%，噪声界内）** —— vacuum 叠加注册开销后仍无统计显著回归 | 达标 |
| Stage E 埋点抽查（QueryStats / 计数器，S2 协议） | 同上（"对 exec 路径开销不可测"） | 由 Stage G 收口 S2 行统一度量（QueryStats 埋点 = 一次 Mutex push + 两次时钟读，本就低于噪声界）：**86.0 vs 86.8（-0.9%）通过** | 达标 |
| **Stage G 收口 S2**（§12.5 硬门槛：注册 + vacuum + 统计埋点 + wire 全栈） | <5% 回归上限 | 86 / 86 / 86（300s×100conn×100txn/s 配速 × 3，各 26,181 / 26,162 / 25,932 txn），**均值 86.0 vs 86.8（-0.9%，噪声界 ±3% 内，无统计显著回归）** | 达标 |

> 注：表内 `achieved txn/s` = 总事务数 ÷ 含建联开销的 elapsed（故 >300s 的会话下绝对值随连接数浮动；跨档/跨基线对比才是有效口径，单看绝对值不可与目标配速直接相除）。
| waldump 吞吐（Stage E，smoke） | 无硬性 target（§12.3 只要求可读） | 62,222 条记录 / 16.48 MB WAL：输出 `/dev/null` 三次 0.271 / 0.255 / 0.249s ⇒ **~240–250K records/s**（三次 230K/244K/250K）、**~66 MB/s**（wall，含进程启动）；输出落盘 62,224 行 0.364s | 记录值 |
| WAL 字节量观测（**N5：压实 FPI 放大，§11 R1，首次量化**） | 无硬性 target（观测项） | 见下方专节 | 记录值 |

## N5：WAL 字节量观测（压实 FPI 放大，首次量化）

测量工具：`crates/pg-engine/examples/m3_wal_bytes_probe.rs`（本 stage 新增）——
固定行数 churn（60 轮 × DELETE/UPDATE/INSERT 各 40，每 5 轮 vacuum），随后用
`WalReader` 按 `align_up(32 + payload, 8)` 逐条归账各记录族字节数。
WAL 段文件预分配 16 MiB，字节量以 writer 的 current_lsn 为准（文件长度无意义）。

| 运行 | WAL 总量 | churn+vacuum 段 | FPI 记录数 / 字节 | FPI 占比 |
|------|---------:|----------------:|------------------:|---------:|
| vacuum 前 checkpoint（每轮 vacuum 都开新 FPI 周期，最坏档） | 4,543,160 B | 4,234,400 B | 354 / 2,914,128 B | **64.1%** |
| 对照：vacuum 落在长 FPI 周期内（仅 preload/收尾两次 checkpoint） | 2,220,008 B | 1,911,248 B | 72 / 592,704 B | 26.7% |
| 240 轮长程（最坏档，48 次 vacuum） | 16,484,488 B | 16,175,728 B | 1,255 / 10,331,160 B | 62.7% |

60 轮最坏档分族字节（对齐后）：FullPageImage 2,914,128 / HeapInsert 606,048 /
HeapUpdate 289,440 / HeapHotUpdate 228,960 / BTreeInsert 231,984 /
BTreeDelete 149,600 / HeapDelete 96,000 / **HeapCleanup 13,024（208 条）** /
PageFree 280（7 条）/ 其余 <10 KB。（口径：分族桶为全 WAL 聚合，含 preload
段的 checkpoint + INSERT FPI；阶段切分仅针对字节总量）

结论（§11 R1 的量化答案）：

- vacuum 的**逻辑** WAL 记录（HeapCleanup + PageFree）字节占比极小（<0.3%）；
  其字节代价几乎全部是**间接的 FPI 放大**——压实/页释放批量改页，每页在
  新 FPI 周期内首改时各写一条 ~8.2 KB 全页像。
- 最坏档（vacuum 前即开新周期，等价于激进 checkpoint 档压测形态）下总 WAL
  约为对照的 **2.05×**（4.54 MB vs 2.22 MB）；放大与"周期内页首次修改"绑定，
  与 vacuum 回收的垃圾量弱相关（240 轮占比 62.7% ≈ 60 轮的 64.1%，稳态）。
- 生产含义：vacuum 节奏与 checkpoint 周期对齐时 FPI 摊薄；该观测支撑
  §11 R1"checkpoint 尾部 flush 压力来自 FPI 而非逻辑记录"的定性判断。
- 附带观测：12 次 vacuum 累计 `dead_tuples=5357 / index_keys=3740 /
  index_entries_removed=0 / already_gone=3740` —— eager 索引维护下 phase ④
  的真实删除对象确实只剩崩溃 loser 悬挂条目（§4.3 工作负载真相，端到端复证）。

## 手动三客户端矩阵（N6，Stage F 收口项）

CI 硬门槛**只有 rust-postgres**（`pg-wire/tests/wire_clients.rs` 4 测试，
dev-dependency 驱动，§10）；以下三家为手动矩阵（2026-08-27 本机实测，
server = 本 stage 新增的 `crates/pg-wire/src/bin/pg-server.rs`，
`pg-server 127.0.0.1:55432 <data-dir>`）：

| 客户端 | 版本 | CREATE/INSERT/SELECT/UPDATE/DELETE | BEGIN/COMMIT/ROLLBACK | 探针行为 |
|--------|------|------------------------------------|-----------------------|----------|
| psql | 19devel | ✅ 全通过 | ✅ 通过 | `SELECT version()` → `ERROR: invalid argument: expected From, got LParen`；`\d t` → `ERROR: invalid argument: unexpected character '.' in SQL` —— 均**报错不断连**（§11 R3 既定落差） |
| psycopg2 | 2.9.12（psycopg2-binary） | ✅ 全通过 | ✅ 通过（autocommit off 走 BEGIN/COMMIT 拦截） | ① 默认隐式事务会把 DDL 包进显式事务 → `DDL inside explicit transactions is not supported in M2b`（M2b 既定边界，`autocommit=True` 后全过）；② `SELECT version()` → SyntaxError 后连接存活 |
| node-postgres | pg 8.23.0 | ✅ 全通过 | ✅ 通过 | `SELECT version()` 报错不断连 |

命令与脚本：`crates/pg-wire/tests/wire_clients.rs` 头部文档（psql 命令行 +
psycopg2 / node-postgres 脚本原文；psycopg2 需加 `c.autocommit = True` 处理
DDL，已在该文件头注记）。

## 复现

```bash
# churn 页数有界（默认 30 轮 / 200 轮 soak）
cargo test -p pg-engine --release --test m3_vacuum_churn -- --nocapture
M3_CHURN_ROUNDS=200 cargo test -p pg-engine --release --test m3_vacuum_churn -- --nocapture

# WAL 字节量观测（N5）
cargo run -p pg-engine --release --example m3_wal_bytes_probe -- /tmp/probe_dir
PROBE_PRE_VACUUM_CHECKPOINT=0 cargo run -p pg-engine --release --example m3_wal_bytes_probe -- /tmp/probe_dir2   # 对照档
PROBE_ROUNDS=240 cargo run -p pg-engine --release --example m3_wal_bytes_probe -- /tmp/probe_dir3              # 长程

# waldump 吞吐
cargo build -p pg-storage --release --bin pg-waldump
time target/release/pg-waldump /tmp/probe_dir > /dev/null

# S2 协议（Stage G 收口判定）
M2C_STRESS_SECS=300 M2C_STRESS_CONNS=100 M2C_STRESS_TPS=100 cargo test -p pg-engine --release --test m2c_stress -- --nocapture

# 手动矩阵 server
cargo build -p pg-wire --release --bin pg-server
target/release/pg-server 127.0.0.1:55432 /tmp/pg_rust_matrix
# 客户端命令见 crates/pg-wire/tests/wire_clients.rs 头部文档
```

## 缺口与说明

- 本机 macOS F_FULLFSYNC ≈ 4–7 ms：所有 fsync 绑定负载运行间方差约 2×
  （phase1-m1-benchmarks.md 既有注记）；S2 判定用配速模式（100 txn/s 上限）
  规避 fsync 封顶，度量的是"配速下达标率 + 协调开销"，不是绝对 TPS 上限。
- waldump 吞吐为 smoke 单次采样（输出 `/dev/null`，wall clock 含进程启动），
  只证量级；无 target。
- 手动矩阵不重复进 CI：三家客户端版本随本机环境漂移，CI 硬门槛维持
  rust-postgres 一家（N6 既定口径）。
