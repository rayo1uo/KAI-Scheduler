# 03. Preempt Action 源码阅读

`preempt` 是同 queue 内的优先级抢占。它和 `reclaim` 共用 solver，但 victim 选择语义完全不同：`preempt` 要求 victim 和 preemptor 在同一个 queue，且 victim priority 更低。入口是 [`pkg/scheduler/actions/preempt/preempt.go`](../../../../pkg/scheduler/actions/preempt/preempt.go)。

## 主循环

```text
Execute
  -> NewJobsOrderByQueues(FilterNonPending, FilterUnready, MaxJobsQueueDepth=GetJobsDepth(Preempt))
  -> PopNextJob()
  -> scheduling signature 剪枝
  -> VictimInvariantPrePredicateFailureForTasks
  -> attemptToPreemptForPreemptor
  -> statement.Commit()
```

和 `reclaim` 相比，`preempt` 没有 `CanReclaimResources` 这类公平性资格检查；它的核心资格来自 priority、queue、preemptibility 和 quota。`MaxJobsQueueDepth` 只限制每个 leaf queue 中进入 preemptor 候选队列的 pending job 深度，victim 搜索仍由后面的 victim queue 和 filter 决定。

## Preemptor 先过 quota 检查

进入 solver 前，代码会先检查：

```go
preemptorTasks := podgroup_info.GetTasksToAllocate(preemptor, ssn.SubGroupOrderFn, ssn.TaskOrderFn, false)
if result := ssn.IsNonPreemptibleJobOverQueueQuotaFn(preemptor, preemptorTasks); !result.IsSchedulable {
    return false, nil, nil
}
```

这个检查通常由 `proportion` 的 capacity policy 提供。它避免某些 non-preemptible quota 语义被 preempt 绕过。

## Victim 过滤条件

`buildFilterFuncForPreempt` 是理解 preempt 语义的最短入口：

```go
if !job.IsPreemptibleJob() {
    return false
}
if job.Priority >= preemptor.Priority {
    return false
}
if job.Queue != preemptor.Queue {
    return false
}
if preemptor.UID == job.UID {
    return false
}
if job.GetActiveAllocatedTasksCount() == 0 {
    return false
}
if !ssn.PreemptVictimFilter(preemptor, job) {
    return false
}
return true
```

逐条解释：

- 非 preemptible job 永远不能作为 victim。
- priority 相同或更高的 job 不会被抢。
- 不跨 queue；跨 queue 回收属于 `reclaim`。
- 不能抢自己。
- 没有 active allocated task 的 job 释放不了资源。
- 插件可以继续加限制，例如 `minruntime` 保护运行时间不足的 victim。

## Solver 复用

`preempt` 构造 solver 的方式和 `reclaim` 非常像：

```text
common.FeasibleNodesForJob(...)
solvers.NewJobsSolver(
    feasibleNodes,
    ssn.PreemptScenarioValidator,
    getOrderedVictimsQueue(ssn, preemptor),
    framework.Preempt,
)
solver.Solve(ssn, preemptor)
```

不同点是：

- scenario validator 用 `PreemptScenarioValidator`，不是 reclaim validator。
- victim queue 只包含同 queue 低优先级 job。
- eviction metadata 的 action 是 `preempt`，便于事件和 metrics 区分。

注意 preempt 的 victim queue 由 `utils.GetVictimsQueue(ssn, filter)` 生成。`GetVictimsQueue` 本身只要求 job 至少有 alive pod，并启用 `VictimQueue` 反向排序；真正的“同 queue、低 priority、preemptible、active allocated、通过插件 victim filter”都在 `buildFilterFuncForPreempt` 中完成。

## Minruntime 在 preempt 中的作用

`minruntime` 对 preempt 注册：

- `AddPreemptVictimFilterFn`
- `AddPreemptScenarioValidatorFn`

非 elastic victim 如果运行时间未达标，会在 filter 阶段直接被排除。elastic victim 的判断延后到 scenario validator，原因是 solver 需要知道具体驱逐哪些 task 后，才能判断是否破坏 minAvailable。

## Statement 里通常有什么

preempt 成功返回的 statement 可能包含：

- 低优先级 victim 的 `Evict`。
- 高优先级 preemptor 的 `Pipeline`。
- 若 solver 能把 victim 迁到其他释放资源上，可能包含 victim 的 `Pipeline`。

commit 后，preemptor 不一定立即 bind；通常先变成 `Pipelined`，等 victim 删除、资源释放后，后续 `allocate` 周期再创建 BindRequest。

## 和 reclaim 的对照

| 维度 | `preempt` | `reclaim` |
| --- | --- | --- |
| 资源目标 | 同 queue 内高优先级替换低优先级 | 跨 queue 按公平性回收 |
| victim queue | 同 queue、低 priority | 其他 queue |
| 是否需要公平性资格 | 不需要 `CanReclaimResources` | 需要 |
| validator | `PreemptScenarioValidator` | `ReclaimScenarioValidatorFn` |
| 主要插件 | `priority`、`minruntime`、capacity policy | `proportion`、`minruntime` |

## 建议测试阅读

- [`pkg/scheduler/actions/preempt/preempt_test.go`](../../../../pkg/scheduler/actions/preempt/preempt_test.go)：基础同 queue priority preemption。
- [`pkg/scheduler/actions/preempt/preemptGang_test.go`](../../../../pkg/scheduler/actions/preempt/preemptGang_test.go)：gang preemption。
- [`pkg/scheduler/actions/preempt/preempt_gang_full_drain_test.go`](../../../../pkg/scheduler/actions/preempt/preempt_gang_full_drain_test.go)：说明 solver 不能只做单节点局部 victim 判断。
- [`pkg/scheduler/actions/preempt/preempt_subgroups_test.go`](../../../../pkg/scheduler/actions/preempt/preempt_subgroups_test.go)：subgroup 抢占。
- [`pkg/scheduler/actions/preempt/preempt_elastic_test.go`](../../../../pkg/scheduler/actions/preempt/preempt_elastic_test.go)：elastic victim 与 minAvailable。
- [`pkg/scheduler/actions/preempt/preempt_victim_invariant_prefilter_test.go`](../../../../pkg/scheduler/actions/preempt/preempt_victim_invariant_prefilter_test.go)：victim-invariant pre-predicate 如何提前跳过无解 preemptor。
