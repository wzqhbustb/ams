# Phase 2 M4 Benchmarks(Stage D 落盘文档)

> 合同:docs/phase2-m4-coding-plan.md 阶段 D(207–236 行)。本文档收录机器规格、
> 全部实测数字、A/B 证据、ftp 实测结论、与 hnswlib/pgvector 的口径对齐说明。
> 状态:**进行中**——1M sift/gist 跑批暂缓(2026-09-03 用户决策,见 §5)。

## 1. 机器规格

| 项 | 值 |
|---|---|
| 本机(benchmark 主力) | Apple M3 Max,16 核,48 GB RAM,macOS 14.5 (23F79) |
| 工具链 | rustc 1.97.1;MSRV 1.86 由 CI msrv job 把关 |
| CI runner | GitHub-hosted ubuntu-latest / macos-latest(规格见 §5 跑批记录) |

内存门槛口径:1M gist 快照文件 ~4 GB(§11 R3);`max_memory_estimate` 按 64 层
cap 计费的保守上界 ~19.2 GB(虚高项已在代码注释写明,tech-selection v1.17);
现实峰值 ≈ 2× 文件(Stage C 六轮审查口径)。

## 2. 数据集来源与 ftp 实测(D-1)

官方源:`ftp://ftp.irisa.fr/local/texmex/corpus/{siftsmall,sift,gist}.tar.gz`
(唯一官方渠道;corpus-texmex.irisa.fr 主站仅回链 ftp,无 HTTP 镜像)。

| 实测点 | 时间 | 结果 |
|---|---|---|
| 本机(macOS) ftp 21 端口 | 2026-09-03 | **不通**(TCP 连接超时,IPv4/IPv6 均失败);同主机 HTTP 403 |
| 本机 HTTPS 镜像(huggingface `vecdata/siftsmall`) | 2026-09-03 | 通;siftsmall.tar.gz 5,304,531 B,结构校验通过(base 10000×128 / query 100×128 / learn 25000×128 / gt 100×100,记录严格对齐) |
| GitHub runner | **待 D-1 探针** | `.github/workflows/m4-ftp-probe.yml`(一次性,push 触发 + workflow_dispatch),结论待回填 |

本地完整性交叉验证:siftsmall 的 recall@10 实测 0.9990(见 §3)——groundtruth 与
base/query 自洽(损坏或不匹配的数据集不可能与图检索结果达到该一致性)。

D-1 结论落定前,`scripts/fetch_datasets.sh` 保持方案无关(默认官方 ftp,
`M4_DATASET_URL_<NAME>` 可覆盖为任意镜像 URL)。

## 3. siftsmall CI 硬门槛数字(M=16, efC=200, ef=64, seed=42)

真值口径(硬约束):ivecs **原序前 10,不重排**;集合交集计分,第 10/11 名等距
并列不扩充(口径写在 `dataset::recall_at_k` rustdoc)。我们的返回排序按
`(distance, NodeId)` 决胜。

| 指标 | release | debug |
|---|---|---|
| recall@10 | **0.9990**(门槛 ≥ 0.98) | 同(release/debug 算法等价) |
| gate 套件耗时(3 测试) | 4.11 s(亲验) | 62.4 s(首次实测,coder 报告;复现命令见下) |
| 建图(10k × 128d) | 4.22 s | — |
| P50 / P99 查询延迟 | 0.128 ms / 0.173 ms(冻结口径,见 §3 注) | — |
| 快照 save / load(10k) | 0.008 s / 0.006 s | — |

> §3 注(2026-09-03 Stage D 外审两轮修复,本表为修复后重测值):
> ① `percentile()` 原实现 `idx = n*p/100` 向下取整当 0-indexed 用,P99
> 实际报的是**最大值**(n=100 时取 sorted[99]);已修为最近秩
> `idx = (n*p - 1)/100`(n=100 时 P99 = sorted[98])。② 查询延迟原仅一轮
> 冷查询,不符合 tech-selection.md:412 冻结口径"预热后 3 轮取中位";
> 已改为 1 轮预热 + 3 轮计时取中位(probe 另输出三轮原始值
> `p50_rounds_ms`/`p99_rounds_ms`)。两处修复前的旧数字一律作废。

复现:

```bash
M4_REQUIRE_DATASET=1 cargo test -p pg-am-hnsw --release --test recall_siftsmall
M4_DATASET=datasets/siftsmall M4_SNAPSHOT=1 \
  cargo run -p pg-am-hnsw --release --example m4_recall_probe
```

## 4. A/B 对照(simple vs heuristic,§4.3 参数冻结证据)

siftsmall,同参数(M=16/efC=200/ef=64/seed=42),release,本机:

| selection | recall@10 | build | P50 | P99 |
|---|---|---|---|---|
| heuristic(默认) | **0.9990** | 4.22 s | 0.128 ms | 0.173 ms |
| simple | 0.9870 | 2.12 s | 0.103 ms | 0.145 ms |

两轮 run 的 `params` 输出行逐字节相同、仅 `selection=` 行不同(probe 自
round 2 起打印全部生效参数),A/B 单变量可证。延迟为冻结口径(1 轮预热 +
3 轮取中位)。

结论(10k 规模初步):simple 建图 ~2× 快,recall 低 1.2pp——符合论文预期方向;
冻结默认 = heuristic 不变。1M 规模 A/B 随 §5 跑批补全。

## 5. 1M sift/gist 跑批 —— 暂缓(2026-09-03 用户决策)

阻塞点:sift(~160 MB)/ gist(~3.8 GB)官方仅 ftp 渠道,本机 ftp 被拦,且无
可信 HTTPS 镜像(HuggingFace 仅有 siftsmall)。两条出路,待 D-1 runner 结论:

- runner ftp 通 → 跑批做成手动/nightly workflow 在 runner 上执行;recall/P99/
  建图/加载时间四指标随 artifacts 落盘。**gist 内存判断已修正(2026-09-03
  外审 P2)**:原写"load 峰值 ~8 GB、16 GB 可扛"不成立——probe 曾同时持有
  base 物化(3.8 GB)+ 原图 arena(~4.5 GB)+ 快照加载(文件字节 ~4 GB 瞬时
  + NodeRecord + 新图 ~4.5 GB),峰值可超 16 GB。round 2 修复:probe 在
  recall 计分后 `drop(base)`,快照段峰值降为 旧图 + 新图 + 瞬时文件/记录
  ≈ 12–13 GB——16 GB runner 紧张但可能可行;若实测 OOM,gist 档拆为
  `M4_SNAPSHOT=0`(只测 recall/延迟/建图)或换大 runner。本机 48 GB 无压力,
  数据就位后也可本机跑批;
- runner ftp 也不通 → 方案 2:一次性取回 → GitHub release 自托管,CI 与本地统一
  走 release URL(`fetch_datasets.sh` 的 URL 覆盖点已预留;siftsmall 的 SHA-256
  已钉死,sift/gist 摘要在数据就位时补钉)。

跑批命令(数据就位后即可执行):

```bash
M4_DATASET=datasets/sift M4_SNAPSHOT=1 cargo run -p pg-am-hnsw --release --example m4_recall_probe
M4_DATASET=datasets/gist M4_SNAPSHOT=1 cargo run -p pg-am-hnsw --release --example m4_recall_probe
```

任一数据集不达标按 §11 R1 预案上调参数(如 gist M=32)并如实记录分数据集参数。

## 6. 与 hnswlib/pgvector 的口径对齐说明(M6 正式对标的预备)

- 数据集与真值:同 texmex corpus,gt 取 ivecs 原序前 k——与 ann-benchmarks /
  hnswlib 惯例一致,不重排、不归一化。
- 参数:M=16 / efC=200 / ef=64 是 hnswlib README 与 pgvector 文档的常用基准档,
  可直接对比;seed 固定(42)保证我方内部可复现,跨库对比时以 recall-延迟曲线
  为准(单点数字受实现细节影响)。
- 指标:recall@10(集合交集)、P50/P99 端到端查询延迟、建图墙钟时间;快照
  save/load 为我方 M4 特有项(hnswlib 对应物 = 索引转储/加载,口径近似)。
- 已知不可比项:我方快照格式自研(§3 冻结),不与 hnswlib 二进制互换;对标走
  同数据集各自建图比 recall(Stage C trade-off 表既定)。
