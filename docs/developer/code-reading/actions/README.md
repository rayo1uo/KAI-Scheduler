# Actions 深读地图

这一组文档专门解释 scheduler action。建议把它们和 [`../03-session-and-statement.md`](../03-session-and-statement.md) 一起读：`Session` 提供本轮快照和插件回调，`Statement` 提供可回滚的虚拟变更，action 负责决定本轮要尝试哪类调度动作。

如果对 `minSubGroup`、SubGroup tree、leaf queue、Queue fairness 还不熟，先读 [`../10-podgroup-queue-concepts.md`](../10-podgroup-queue-concepts.md)。这些概念会直接决定 action 初始化过滤、`GetTasksToAllocate()`、`GetTasksToEvict()` 和 `JobsOrderByQueues` 的行为。

## 默认执行顺序

默认配置来自 [`pkg/scheduler/conf_util/scheduler_conf_util.go`](../../../../pkg/scheduler/conf_util/scheduler_conf_util.go)：

```yaml
actions: "allocate, consolidation, reclaim, preempt, stalegangeviction"
```

注意区分两件事：

- [`pkg/scheduler/actions/factory.go`](../../../../pkg/scheduler/actions/factory.go) 只是把 action 注册到 framework registry。
- scheduler 真正执行的顺序来自配置里的 `actions` 字符串，解析入口是 `GetActionsFromConfig`。

因此阅读默认行为时，按这个顺序看更贴近真实调度：

```text
allocate
  -> consolidation
  -> reclaim
  -> preempt
  -> stalegangeviction
```

## 章节

| 章节 | 重点 | 入口源码 |
| --- | --- | --- |
| [01. Allocate](01-allocate.md) | 直接分配 pending job，递归处理 subgroup/podset/task，提交 BindRequest 或 Pipeline | [`pkg/scheduler/actions/allocate/allocate.go`](../../../../pkg/scheduler/actions/allocate/allocate.go) |
| [02. Reclaim](02-reclaim.md) | 跨 queue 的公平性资源回收，依赖 proportion/minruntime 和 victim solver | [`pkg/scheduler/actions/reclaim/reclaim.go`](../../../../pkg/scheduler/actions/reclaim/reclaim.go) |
| [03. Preempt](03-preempt.md) | 同 queue 内高优先级 job 抢占低优先级 job | [`pkg/scheduler/actions/preempt/preempt.go`](../../../../pkg/scheduler/actions/preempt/preempt.go) |
| [04. Consolidation](04-consolidation.md) | 通过搬迁/重排已有任务整理碎片，要求 victim 被重新安置 | [`pkg/scheduler/actions/consolidation/consolidation.go`](../../../../pkg/scheduler/actions/consolidation/consolidation.go) |
| [05. Stale Gang Eviction](05-stale-gang-eviction.md) | 清理长期 stale 的 gang job，不走 Statement solver | [`pkg/scheduler/actions/stalegangeviction/stalegangeviction.go`](../../../../pkg/scheduler/actions/stalegangeviction/stalegangeviction.go) |
| [06. Queue Order 与 Solver](06-queue-order-and-solver.md) | 所有 action 共用的 queue/job 排序、victim scenario 搜索和虚拟分配 | [`pkg/scheduler/actions/utils/job_order_by_queue.go`](../../../../pkg/scheduler/actions/utils/job_order_by_queue.go) |

## 一张对照表

| Action | 处理对象 | Victim 来源 | 是否用 solver | Statement 使用方式 | 主要插件入口 |
| --- | --- | --- | --- | --- | --- |
| `allocate` | pending、ready job | 无 | 否 | 每个 job 一个 statement，成功 `Commit`，失败 `Discard` | `QueueOrderFn`、`JobOrderFn`、`SubGroupOrderFn`、`TaskOrderFn`、`PrePredicateFn`、`PredicateFn`、`NodeOrderFn`、`GpuOrderFn`、capacity |
| `consolidation` | pending、ready、preemptible job | 任意 preemptible active allocated job，不包含自己 | 是 | solver 返回 live statement，action `Commit` | `JobOrderFn`、`VictimInvariantPrePredicateFn`、`PredicateFn`、`NodeOrderFn`、`allPodsReallocated` |
| `reclaim` | pending、ready job | 其他 queue 的 preemptible active allocated job | 是 | solver 返回 live statement，action `Commit` | `CanReclaimResourcesFn`、`ReclaimVictimFilterFn`、`ReclaimScenarioValidatorFn`、`QueueOrderFn` |
| `preempt` | pending、ready job | 同 queue、低 priority、preemptible active allocated job | 是 | solver 返回 live statement，action `Commit` | `PreemptVictimFilterFn`、`PreemptScenarioValidatorFn`、`IsNonPreemptibleJobOverQueueQuotaFn` |
| `stalegangeviction` | stale job | stale job 自己的 active allocated tasks | 否 | 不使用 statement，直接 `ssn.Evict` | scheduler params 的 stale grace period |

## 阅读时抓住三条线

第一条线是 job 顺序。每个面向 pending job 的 action 都先创建 `JobsOrderByQueues`，这个结构把多级 Queue 树、Queue 排序、Job 排序整合成一个 `PopNextJob()`。

第二条线是 task 分配。`allocate` 直接调用 [`pkg/scheduler/actions/common/allocate.go`](../../../../pkg/scheduler/actions/common/allocate.go)；`reclaim/preempt/consolidation` 在 solver 里也复用同一套 `AllocateJob(..., isPipelineOnly=true)`，所以最终能不能调度，仍然由 subgroup、predicate、node order、GPU order、capacity 等共同决定。

第三条线是副作用提交。action 阶段绝大多数操作只是改本轮 session 内存模型；只有 `Statement.Commit()` 或 `Session.Evict()` 才会进入 cache，进而创建 BindRequest、记录 Pipeline event 或驱逐 Pod。
