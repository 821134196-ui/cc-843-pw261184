# 轻量事件处理运行时（coalescing · per-key 串行 · 退避 · 租户公平）

同一个对象会连续收到大量更新，失败重试时还可能收到更新的版本。本项目实现一个
轻量的内存事件处理运行时，并提供：

- **队列合并（coalescing）**：每个 `(租户, key)` 只有一个待处理槽位，同对象的连续
  更新就地合并，最新版本胜出、最早入队时间保留。
- **按键串行（per-key serialization）**：同一 key 不会被并发处理，不同 key 仍并行。
- **指数退避（backoff + jitter）**：失败事件进入延迟堆，按 `base·2^(n-1)` 加对称抖动
  重试，超过最大尝试次数进入死信；已被新版本超越的失败尝试直接判过期，不再浪费重试。
- **租户公平（DRR + 并发上限）**：每租户独立 FIFO，Deficit Round Robin 按配额轮转，
  并限制单租户可占用的 worker 数，持续风暴不能饿死健康租户。
- **过期版本保护（version CAS）**：状态提交是版本号 compare-and-swap，重试的旧版本
  无法覆写已提交的新状态（`StaleVersionError` → 计入 `stale_dropped`）。

## 目录结构

```
event_runtime/
  events.py    # Event 数据类与 coalesce 合并规则
  store.py     # StateStore：按版本 CAS 提交
  backoff.py   # BackoffPolicy：指数退避 + 抖动
  metrics.py   # 线程安全指标（计数、延迟分位数、死信样本）
  base.py      # 共享 worker 循环：串行、重试/退避、过期保护
  smart.py     # SmartRuntime：每租户队列 + 合并 + DRR + 并发上限
  naive.py     # NaiveRuntime：单一 FIFO，不合并、不公平（对照基线）
bench/
  workload.py  # 固定、带种子的负载（风暴租户 + 健康租户 + 确定性失败场景）
  runner.py    # 同一负载依次跑两个运行时，采样积压并汇总指标
  cli.py       # 命令行对比报告
tests/
  test_runtime.py  # 8 个单元测试覆盖每条保证
webapp.py          # stdlib HTTP 服务（仪表盘 + /api/run）
static/index.html  # 仪表盘页面（原生 canvas，零前端依赖）
```

## 使用

### 命令行对比

```bash
python3 -m bench.cli          # 文本报告
python3 -m bench.cli --json   # 机器可读 JSON
```

### 网页仪表盘

```bash
python3 -m webapp             # http://127.0.0.1:8000/
PORT=9000 python3 -m webapp
```

点击「注入固定负载并运行对比」，页面会把同一份固定负载分别注入两个运行时，
展示 KPI、积压时间序列、延迟分布、租户 p95 公平性与正确性校验。

### 单元测试

```bash
python3 -m unittest tests.test_runtime -v
```

## 设计要点

### 合并（coalescing）

`SmartRuntime` 为每个租户维护一个 deque，并维护 `pending_keys` 集合。新事件入队时：

- 若该 key 已有待处理事件 → 调用 `Event.coalesce` 就地合并（新版本胜出，
  `merged_count` 累加），不新增队列槽位；
- 否则正常入队。

因此风暴下每个对象最多只有 1 条待处理更新；`NaiveRuntime` 不做合并，每条更新各占
一个 FIFO 槽位，作为对照。

### 按键串行

`_inflight` 集合记录正在处理的 key。调度器跳过 head 为 in-flight 的队列（smart 把
阻塞 head 旋转到队尾；naive 把事件暂时取出后插回队首），保证同 key 严格串行、
不同 key 并行。

### 退避与过期重试

失败事件带 `ready_at` 进入全局最小堆；worker 每次调度前把到期事件提升回租户队列。
两条过期防线：

1. **失败时**：若运行时水位 `_seen_version[key]` 已高于当前事件版本，说明已有更新的
   版本入队 → 直接记 `stale_dropped`，不进退避堆；
2. **提交时**：`StateStore.commit` 做版本 CAS，`event.version <= 已提交版本` 时抛
   `StaleVersionError`，旧重试永远无法覆写新状态。

### 租户公平

- **DRR**：每个租户带 deficit 计数器，轮到时补充 quantum、每派发一个事件扣 1；
  quantum 用尽后游标移到下一租户，风暴租户无法独占调度顺序。
- **并发上限**：`tenant_concurrency` 限制单租户同时占用的 handler 数（基准测试设为
  `workers // 2`），保证健康租户始终有空闲 worker。

## 固定负载（bench/workload.py）

- `tenant-storm`：2~8 个热 key，按「突发（burst）」节奏产生同对象连续更新，含随机
  注入失败（带种子、可复现）；
- 确定性场景：`storm-key-0` v1 失败进入退避，v2 在退避窗口内先提交 → v1 重试必须被
  过期丢弃；`storm-key-1` v1 永久失败 → 恰好产生 1 个死信；
- `tenant-a/b/c`：稀疏、规律、永不失败的健康租户，其 p95 延迟作为公平性指标。

两个运行时共享相同的安全保证（串行、CAS、退避），仅在**合并与公平调度**上有差异，
因此对比结果可直接归因到这两个机制。负载由固定种子生成，提交事件数与失败标记完全
确定；合并计数会因线程调度产生 ±0.1% 级别的波动（合并取决于事件到达时是否仍在排队），
但最终状态、死信数与延迟分位数在重复运行间保持稳定。典型结果（4 workers，6s 负载）：

| 指标 | naive-fifo | smart 合并+DRR |
|---|---|---|
| 平均端到端延迟 | ~13 ms | ~4 ms |
| 风暴租户 p95 | ~29 ms | ~6 ms |
| 健康租户 p95 | ~5–6 ms | ~3.3 ms |
| 峰值排队积压 | ~23 | 1 |
| 合并的冗余更新 | 0 | ~2900 |
| 正确性 | PASS（过期丢弃 + 唯一死信） | PASS |

具体数字随机器波动，可用 `python3 -m bench.cli` 复现。
