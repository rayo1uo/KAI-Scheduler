# 02. 公平性、Quota 与 Victim 插件

这一章关注 queue 公平性、quota/capacity、reclaim/preempt victim 过滤和 scenario 校验。核心插件是 `proportion` 和 `minruntime`。

## `proportion`

入口：[`pkg/scheduler/plugins/proportion/proportion.go`](../../../../pkg/scheduler/plugins/proportion/proportion.go)

`OnSessionOpen` 的注册点：

```go
pp.calculateResourcesProportion(ssn)
capacityPolicy := cp.New(pp.queues)

ssn.AddQueueOrderFn(pp.queueOrder)
ssn.AddCanReclaimResourcesFn(pp.CanReclaimResourcesFn)
ssn.AddReclaimScenarioValidatorFn(pp.reclaimableFn)
ssn.AddOnJobSolutionStartFn(pp.OnJobSolutionStartFn)

ssn.AddIsNonPreemptibleJobOverQueueQuotaFns(capacityPolicy.IsNonPreemptibleJobOverQuota)
ssn.AddIsJobOverCapacityFn(capacityPolicy.IsJobOverQueueCapacity)
ssn.AddIsTaskAllocationOnNodeOverCapacityFn(capacityPolicy.IsTaskAllocationOnNodeOverCapacity)

ssn.AddEventHandler(&framework.EventHandler{
    AllocateFunc:   pp.allocateHandlerFn(ssn),
    DeallocateFunc: pp.deallocateHandlerFn(ssn),
})

ssn.AddGetQueueAllocatedResourcesFn(pp.getQueueAllocatedResourceFn)
ssn.AddGetQueueDeservedResourcesFn(pp.getQueueDeservedResourcesFn)
ssn.AddGetQueueFairShareFn(pp.getQueueFairShareFn)
```

可以把它分成五个职责。

## 职责一：Queue 排序

`QueueOrderFn` 被 `JobsOrderByQueues` 调用：

- 普通 pending job queue：决定哪个 queue 先拿资源。
- victim queue：决定从哪个 queue 回收资源更合适。

比较时会拿到 queue、queue 当前最优 pending job、以及 victim queue 中已选择/即将选择的 victims。这让 proportion 可以按实际“资源需求”和“已回收资源”做更细的公平性排序。

## 职责二：Capacity policy

`capacityPolicy` 提供三类检查：

- `IsJobOverQueueCapacity`：`common.AllocateJob` 开始时检查整个 job 的待分配 tasks 是否会让 queue 超 capacity。
- `IsTaskAllocationOnNodeOverCapacity`：`predicates` 节点检查时可调用，判断单个 task 放到 node 后是否违反 queue capacity。
- `IsNonPreemptibleJobOverQuota`：`preempt` 进入 solver 前检查 non-preemptible quota。

这些检查解释了为什么“节点有资源”不等于“queue 可以继续调度”。

## 职责三：Reclaim 资格

`CanReclaimResourcesFn` 在 [`pkg/scheduler/actions/reclaim/reclaim.go`](../../../../pkg/scheduler/actions/reclaim/reclaim.go) 主循环中调用。它用 pending job 构造 reclaimer info，再交给 reclaimable 策略判断这个 queue 是否有资格从其他 queue 拿回资源。

如果这里返回 false，reclaim 不会进入 victim solver。

## 职责四：Reclaim scenario 校验

`reclaimableFn` 在 solver 找到一个可能方案后调用。它会：

1. 从 scenario 里取 reclaimer。
2. 汇总 victims 释放的资源，按 victim queue 分组。
3. 对 elastic/core victim 做不同资源聚合。
4. 调 reclaimable 策略判断该 victim 组合是否合理。

这个 validator 是 reclaim “不会乱驱逐”的关键。solver 只证明这个组合能让 pending job 调度上；proportion 进一步证明这个组合符合公平性目标。

## 职责五：虚拟资源状态同步

`proportion` 注册 event handler，所以 `Statement.Allocate/Pipeline/Evict/Rollback` 的虚拟操作会更新插件内部 queue 资源视图。`OnJobSolutionStartFn` 会复制一份 simulation queue state。

按当前源码，只有 `reclaim` 在进入 solver 前调用 `ssn.OnJobSolutionStart()`。`preempt` 和 `consolidation` 复用 `JobsSolver`，但 action 本身没有调用这个 hook。因此读 proportion 的 simulation 状态时，要把 reclaim 和其他 solver action 分开看。

这让 solver 在尝试多组 victims 时，可以让 queue allocated/deserved/fairshare 随虚拟操作变化，而不是只看 snapshot 初始状态。

## `minruntime`

入口：[`pkg/scheduler/plugins/minruntime/minruntime.go`](../../../../pkg/scheduler/plugins/minruntime/minruntime.go)

`OnSessionOpen` 注册四个回调：

```go
ssn.AddReclaimVictimFilterFn(mr.reclaimFilterFn)
ssn.AddPreemptVictimFilterFn(mr.preemptFilterFn)
ssn.AddReclaimScenarioValidatorFn(mr.reclaimScenarioValidatorFn)
ssn.AddPreemptScenarioValidatorFn(mr.preemptScenarioValidatorFn)
```

它的目标是保护运行时间不足的 victim。当前 `minruntime` 只注册 reclaim/preempt 的 victim filter 和 scenario validator；consolidation 的 victim queue 和 `allPodsReallocated` validator 不调用这些 minruntime 回调。

## 非 elastic victim：filter 阶段保护

对非 elastic job：

- reclaim victim 进入 victim queue 前会调用 `reclaimFilterFn`。
- preempt victim 进入 victim queue 前会调用 `preemptFilterFn`。

如果 victim 未达到对应最小运行时间，filter 返回 false，它不会进入 solver。

## Elastic victim：validator 阶段保护

elastic job 比较特殊。即使某个 elastic job 未达到 min runtime，也可能只驱逐它的额外 task，而不破坏 `minAvailable`。因此 minruntime 对 elastic job 允许先进入 solver，等具体 victim tasks 被选出来后，再由 scenario validator 判断是否安全。

这解释了为什么 minruntime 既注册 victim filter，又注册 scenario validator。

## Reclaim resolve method

`minruntime` 的 reclaim runtime 可以按 queue 或 LCA 解析。相关参数：

- `defaultReclaimMinRuntime`
- `defaultPreemptMinRuntime`
- `reclaimResolveMethod`: `lca` 或 `queue`

阅读 [`pkg/scheduler/plugins/minruntime/resolver.go`](../../../../pkg/scheduler/plugins/minruntime/resolver.go) 可以看到它如何沿 Queue hierarchy 找策略。

## 典型排障问题

| 现象 | 优先查看 |
| --- | --- |
| pending job 明明资源不足却没有 reclaim | `proportion.CanReclaimResourcesFn` |
| reclaim 找到 victim 后没有 commit | `proportion.reclaimableFn` 或 `minruntime.reclaimScenarioValidatorFn` |
| 同 queue 高优先级没有 preempt 低优先级 | `preempt` victim filter、`minruntime.preemptFilterFn`、quota check |
| consolidation 没有受 minruntime 保护 | 这是当前实现边界，consolidation 不走 minruntime reclaim/preempt 回调 |
| Queue 有资源但 job 被判 over capacity | `capacity_policy` 下的 quota/max allowed check |

## 建议测试阅读

- [`pkg/scheduler/plugins/proportion/proportion_test.go`](../../../../pkg/scheduler/plugins/proportion/proportion_test.go)
- [`pkg/scheduler/plugins/proportion/queue_order/queue_order_test.go`](../../../../pkg/scheduler/plugins/proportion/queue_order/queue_order_test.go)
- [`pkg/scheduler/plugins/proportion/capacity_policy/capacity_policy_test.go`](../../../../pkg/scheduler/plugins/proportion/capacity_policy/capacity_policy_test.go)
- [`pkg/scheduler/plugins/proportion/reclaimable/reclaimable_test.go`](../../../../pkg/scheduler/plugins/proportion/reclaimable/reclaimable_test.go)
- [`pkg/scheduler/plugins/minruntime/minruntime_test.go`](../../../../pkg/scheduler/plugins/minruntime/minruntime_test.go)
- [`pkg/scheduler/plugins/minruntime/resolver_test.go`](../../../../pkg/scheduler/plugins/minruntime/resolver_test.go)
