# Phase 2 M6 Benchmarks(2026-10-26)

状态:✅ 对标已落盘(pgvector v0.8.7 实测);24h stress 验收跑后台进行中

本文档对应 coding-plan 阶段 F 行 300 的对标要求与 §13 hwm 串行点的实测回执。数据集:siftsmall(10k × 128d,100 查询,ground-truth ivecs)。机器:macOS arm64 开发机(release 构建;pgvector 为 brew postgresql@17 + v0.8.7 源码编译)。

## 1. siftsmall 同口径对照(M6 事务化 HNSW 路径 vs pgvector)

| 指标 | pg_rust(M6) | pgvector(v0.8.7) |
|------|-------------|------------------|
| bulk insert(单事务) | 10000 行 14.40s = **694 rows/s** | 10000 行 2.67s = **3744 rows/s** |
| HNSW 索引构建(m=16, ef_construction=200) | 含于 bulk insert(增量建图) | **2.17s**(bulk 后一次性建) |
| auto-commit insert | 1000 行 19.24s = **52 rows/s** | 100 行 0.11s = **872 rows/s** |
| recall@10(ef=64) | **0.9990**(pre/post_rebuild 同值) | 0.9940 |
| 查询延迟 mean | **0.222ms**(pre)/ 0.217ms(post) | 2.871ms |
| 查询延迟 p50 | 0.213ms / 0.213ms | 2.988ms |
| 查询延迟 p95 | 0.410ms / 0.332ms | 3.561ms |
| 查询延迟 max | 0.462ms / 0.463ms | 9.045ms |

**读数(如实,不粉饰)**:
- **recall 我方略优**(0.9990 vs 0.9940,+0.5pp)——图质量同档,无回退。
- **插入吞吐 pgvector 明显更快**:bulk 3744 vs 694(约 5.4×),auto-commit 872 vs 52(约 17×)。pgvector 的"装载后一次性建索引"也比我的"插入即增量建图"省(bulk+build 合计 4.84s vs 我 14.40s,约 3×)。**这是当前实现的真实差距,主因是每次插入都做全量 WAL + 每行一次提交边界 flush + 增量搜索连接段的成本**。
- **查询延迟我方便宜一个数量级**(0.222ms vs 2.871ms)——**但口径不同,不可直接归功于引擎**:我方是进程内 `hnsw_search_visible` API 测量(图搜索 + 旁路映射 + 回表可见性),pgvector 是 psql `\\timing` 全 SQL 路径(parse + plan + execute)测量。pgvector 的 2.87ms 含协议与规划器开销及新实例冷缓存效应;两者不是同一层级的延迟对比,仅作方向性参考。

## 2. hwm 串行点实测回执(选型 §13)

hwm 临界区(分配 NodeId → 抽层 → 备页 → NodeInit → DirAppend)是所有 insert 的全局串行点。实测两种插入形态:

- **auto-commit(每行一个事务)**:52 rows/s —— 每行独立提交,提交延迟(fsync + 组提交等待)主导,串行点不是瓶颈;该数字是**每事务提交延迟**的测量,不是图 CPU 的测量。
- **单事务批量**:694 rows/s —— 提交成本被摊销后,hwm 串行区与图连接段的成本成为主要构成。该形态才是向量摄取(ingest)的吞吐指标。

**结论(待 24h 并发跑复核)**:M6 的 insert 吞吐受提交模型主导,不是 hwm 串行点线性扩展问题;§13 风险"100 并发下吞吐天花板"在 auto-commit 形态下被 fsync 掩盖,在批量形态下以 694 rows/s 体现。24h 跑将以 100 并发混合负载给出更完整的数字。与 pgvector 的差距说明:**提交与增量建图路径有实质优化空间**(批量缓冲/WAL 合并/连接段增量复用),归后续阶段调优项,不在 M6 验收口径内。

## 3. F-1 100 并发 stress(CI 缩短版)

- 100 conns × 400 iterations ≈ 4.7s(release);24h 验收跑后台进行中。
- recall@10(quiesce 与重建后)= **1.0000**(ef=128,splitmix64 伪随机向量 corpus)。
- 不对称率(LIVE–LIVE):B(quiesce)≈ 6.6% → C(重建后)≈ 6.4%,**修复腿增量为负**——双向优先替换的成效(初版最近邻替换在 50% tombstone 比率下 +4.66pp、~9% 下 +2.36pp,已被 §12.3 预算反噬修复)。
- 并发腿 B − M4 基线(15.35%)远小于 1pp;quiesce 审计全绿,无悬挂 LIVE 节点,三项残留(initializing/orphan_entry/orphan_map)在 quiesce heap vacuum 后为零。

## 4. F-2 并发 × SIGKILL crash rounds(CI 25 轮)

- **82s 全绿**(full/mid/v1.4-P1 三轮型)。恢复时间亚秒级(<30s 预算内)。
- 残留上界钉:`initializing/orphan_entry/hidden_high_level` 各 ≤ 并发度(16)——"崩溃窗口 × 并发度"断言落地(Stage E 延期项闭合)。
- committed delete × 清理跳过由 heap vacuum 死行通道收口:`orphan_map_entries == 0`。
- recall@10 恢复后与重建后均 1.0000;不对称率全部 < 30% 上界(round 8 的 4.9753% 为合法低值,非缺陷信号)。

## 5. 验证门

- pg-am-hnsw lib 185 绿、loom 四模型 bound-2 绿(1149s)、pg-engine 全套件 EXIT=0、workspace debug 全套件 EXIT=0(早期回归)。
- fmt / clippy(默认 + --all-features)/ doc 三档绿。
- 覆盖率:pg-am-hnsw tarpaulin `--fail-under 90` 以 CI(Linux)报告为准(M6 push 时取回)。

(待回填:24h 验收结果、release 全量回归、CI 覆盖率报告、出口 tag。)
