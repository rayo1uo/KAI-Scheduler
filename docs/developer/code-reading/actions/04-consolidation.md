# 04. Consolidation Action 源码阅读

`consolidation` 的目标是整理碎片：它可以临时驱逐已有任务，再尝试把它们重新安置，从而为 pending job 腾出更合适的资源组合。入口是 [`pkg/scheduler/actions/consolidation/consolidation.go`](../../../../pkg/scheduler/actions/consolidation/consolidation.go)。

## 开关和候选 job

第一层 guard：

```go
if ssn.GetMaxNumberConsolidationPreemptees() == 0 {
    return
}
```

`0` 表示禁用 consolidation；`-1` 表示不限制测试 victim 数量；正数用于限制测试的 preemptee job 数量。按当前源码，停止条件写作 `preempteeJobsCounter > maxPreempteesToTest`，且计数器在通过过滤后递增，所以不要把这个参数解读成严格“最多 N 个”的数学边界，读行为时以源码判断为准。

pending job 队列初始化时带：

```go
FilterNonPending:     true,
FilterUnready:        true,
FilterNonPreemptible: true,
MaxJobsQueueDepth:    ssn.GetJobsDepth(framework.Consolidation),
```

也就是说 consolidation 只为 preemptible 的 pending job 尝试整理资源。

## 执行链路

```text
Execute
  -> GetMaxNumberConsolidationPreemptees guard
  -> NewJobsOrderByQueues(FilterNonPending, FilterUnready, FilterNonPreemptible)
  -> scheduling signature 剪枝
  -> VictimInvariantPrePredicateFailureForTasks
  -> attemptToConsolidateForPreemptor
       -> utils.IsEnoughGPUsAllocatableForJob(job, ssn, false)
       -> attemptToConsolidatePreemptor
            -> common.FeasibleNodesForJob(...)
            -> solvers.NewJobsSolver(..., allPodsReallocated, ...)
            -> solver.Solve(...)
  -> stmt.Commit()
```

`IsEnoughGPUsAllocatableForJob` 是一个粗筛：如果整个集群可分配 GPU 总量都不够，就没有必要进入昂贵的 victim solver。

## Victim 选择

consolidation 的 victim filter 比 preempt/reclaim 更宽：

```go
if !job.IsPreemptibleJob() {
    return false
}
if preemptor.UID == job.UID {
    return false
}
if maxPreempteesToTest != -1 && preempteeJobsCounter > maxPreempteesToTest {
    return false
}
if job.GetActiveAllocatedTasksCount() == 0 {
    return false
}
return true
```

它不要求同 queue，也不要求低 priority。原因是 consolidation 的目的不是执行公平性或优先级语义，而是寻找一种“重新摆放后整体更可调度”的布局。

这里的 victim queue 同样来自 `utils.GetVictimsQueue(ssn, filter)`。因此 `JobsOrderInitOptions` 只负责启用 `VictimQueue` 反向排序和无限队列深度；“preemptible、不含自己、active allocated、数量限制”都由 `buildPreemptibleFilterFunc` 这层 action filter 实现。consolidation 没有注册或调用 `minruntime` 的 reclaim/preempt victim filter 和 scenario validator。

## 核心 validator：`allPodsReallocated`

consolidation 的 solver validator 是 action 自己提供的：

```go
func allPodsReallocated(scenario api.ScenarioInfo) bool {
    for _, victim := range scenario.GetVictims() {
        for _, task := range victim.Tasks {
            if task.Status == pod_status.Releasing {
                return false
            }
        }
    }
    return true
}
```

这段代码是 consolidation 与 preempt/reclaim 的最大差异。preempt/reclaim 可以接受 victim 最终处于 `Releasing`，因为它们就是要释放资源。consolidation 则要求所有 victims 最终都不再是 `Releasing`，也就是 solver 必须把 victim 重新 pipeline/allocate 到其他位置。

## 为什么仍然使用 JobsSolver

虽然 consolidation 的目标是“搬家”，但它仍然需要 solver：

- pending job 是否能放进释放后的资源，需要完整跑 `AllocateJob`。
- victim 是否能重新安置，也需要完整跑同一套 predicate、node order、topology、GPU sharing 逻辑。
- gang/subgroup 不能被拆碎，所以 victim 组合要按 job/podset 语义尝试。
- scenario builder 能用 GPU/topology/node affinity filter 剪枝，降低模拟次数。

## 和 preempt/reclaim 的对照

| 维度 | `consolidation` | `preempt` | `reclaim` |
| --- | --- | --- | --- |
| 目的 | 整理碎片，重排任务 | 同 queue priority | 跨 queue fair share |
| victim queue | preemptible active allocated，不含自己 | 同 queue、低 priority | 其他 queue |
| 成功条件 | pending job 可放，且 victim 都被重新安置 | preemptor 可 pipeline/allocate | reclaimer 可 pipeline/allocate 且公平性 validator 通过 |
| validator | `allPodsReallocated` | plugin validators | plugin validators |

## 建议测试阅读

- [`pkg/scheduler/actions/consolidation/consolidation_test.go`](../../../../pkg/scheduler/actions/consolidation/consolidation_test.go)：基础碎片整理。
- [`pkg/scheduler/actions/consolidation/consolidation_subgroups_test.go`](../../../../pkg/scheduler/actions/consolidation/consolidation_subgroups_test.go)：subgroup victim 重排。
- [`pkg/scheduler/actions/consolidation/consolidationGpuMemory_test.go`](../../../../pkg/scheduler/actions/consolidation/consolidationGpuMemory_test.go)：GPU memory 场景。
- [`pkg/scheduler/actions/consolidation/consolidation_victim_invariant_prefilter_test.go`](../../../../pkg/scheduler/actions/consolidation/consolidation_victim_invariant_prefilter_test.go)：victim-invariant prefilter。
