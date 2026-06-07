# 02. Reclaim Action 源码阅读

`reclaim` 是跨 queue 的资源回收。它解决的不是“同一个 queue 内谁优先”，而是“某个 queue 按公平性应该拿回资源时，可以从其他 queue 回收哪些正在运行的任务”。入口是 [`pkg/scheduler/actions/reclaim/reclaim.go`](../../../../pkg/scheduler/actions/reclaim/reclaim.go)。

## 主循环

```text
Execute
  -> NewJobsOrderByQueues(FilterNonPending, FilterUnready, MaxJobsQueueDepth=GetJobsDepth(Reclaim))
  -> PopNextJob()
  -> ssn.CanReclaimResources(job)
  -> scheduling signature 剪枝
  -> VictimInvariantPrePredicateFailureForTasks
  -> attemptToReclaimForSpecificJob
  -> statement.Commit()
```

源码里的第一层过滤很重要：

- pending job 仍然按 Queue 树和 JobOrder 排序。
- `MaxJobsQueueDepth` 限制每个 leaf queue 在本 action 中进入 pending job priority queue 的深度。它影响“每个叶子队列最多拿多少个候选 reclaimer 来试”，不限制 solver 后面遍历 victim 的空间。
- `ssn.CanReclaimResources(job)` 通常由 `proportion` 插件提供。如果公平性判断认为这个 job 所在 queue 不应该 reclaim，本 action 不会继续构造 victim。
- scheduling signature 会记录“某类更小/更容易的 job 都失败了”，后续更难的同 queue job 可以跳过，减少 solver 成本。
- victim-invariant pre-predicate 用于提前发现“驱逐谁都无效”的情况，例如某些 task 的硬约束在当前集群里天然不成立。

## 为什么跨队列抢占在这里实现

代码中真正排除同 queue victim 的位置在 `getOrderedVictimsQueue`：

```go
for _, job := range ssn.ClusterInfo.PodGroupInfos {
    if job.Queue == reclaimer.Queue {
        continue
    }
    if !ssn.ReclaimVictimFilter(reclaimer, job) {
        continue
    }
    jobs[job.UID] = job
}
```

因此 KAI 里“跨队列抢占”应优先按 `reclaim` 理解。`preempt` 的 victim filter 反而要求 victim 和 preemptor 在同一个 queue。

## Victim 队列

`reclaim` 传给 solver 的 victim queue 带这些选项：

```go
utils.NewJobsOrderByQueues(ssn, utils.JobsOrderInitOptions{
    FilterNonPreemptible:     true,
    FilterNonActiveAllocated: true,
    VictimQueue:              true,
    MaxJobsQueueDepth:        scheduler_util.QueueCapacityInfinite,
})
```

含义：

- victim 必须是 preemptible job。
- victim 必须有 active allocated tasks，否则驱逐它不会释放资源。
- `VictimQueue: true` 会反转 queue/job 排序，并把已经 pop 出来的 victims 传给 `QueueOrderFn`。这让 `proportion` 可以基于“已经准备从某个 queue 回收多少”继续排序。
- victim 搜索深度是无限，因为 solver 要有完整候选空间。
- `getOrderedVictimsQueue` 还会先排除 reclaimer 自己所在 queue，再调用 `ssn.ReclaimVictimFilter(reclaimer, job)`。所以 reclaim 的“跨队列”不是 `JobsOrderInitOptions` 自己实现的，而是 action 自定义 filter 和 victim queue 初始化共同完成的。

## Solver 如何验证 reclaim 是否值得提交

`attemptToReclaimForSpecificJob` 的关键是：

```text
ssn.OnJobSolutionStart()
feasibleNodes := common.FeasibleNodesForJob(allNodes, reclaimer)
solver := solvers.NewJobsSolver(
    feasibleNodes,
    ssn.ReclaimScenarioValidatorFn,
    getOrderedVictimsQueue(ssn, reclaimer),
    framework.Reclaim,
)
solver.Solve(ssn, reclaimer)
```

这里 `ssn.OnJobSolutionStart()` 通常由 `proportion` 用来复制一份 queue 资源状态，后续 solver 在虚拟 allocate/deallocate 时不会污染插件的基准状态。

`JobsSolver` 不会一看到可驱逐 victim 就提交。它会：

1. 构造 potential victims。
2. 在 `Statement` 中虚拟 `Evict` victims。
3. 调用公共 `AllocateJob(..., isPipelineOnly=true)` 试着把 reclaimer 放到释放资源上。
4. 同时也尝试把部分 victim job pipeline 回去，减少真实驱逐数量。
5. 调用 `ssn.ReclaimScenarioValidatorFn` 校验这个组合是否符合公平性和最小运行时间等策略。
6. 成功才返回 live statement 给 action commit。

## Proportion 插件如何影响 reclaim

`proportion` 在 reclaim 中至少有四个入口：

- `CanReclaimResourcesFn`：pending job 是否有资格发起 reclaim。
- `QueueOrderFn`：pending job queue 和 victim queue 的排序。
- `ReclaimScenarioValidatorFn`：当前 victim 组合是否真的改善公平性，是否越过 quota/capacity。
- event handler：虚拟 allocate/deallocate 时更新插件内部 queue resource view。

这意味着 reclaim 的行为不是“谁占得多就驱逐谁”这么简单，而是结合 queue hierarchy、deserved、fair share、allocated、pending job 需求、victim resource、部门/父队列等状态做判断。

## Minruntime 插件如何保护 victim

`minruntime` 对 reclaim 注册了两个点：

- `AddReclaimVictimFilterFn`：非 elastic victim 若未满足最小运行时间，直接不进入 victim queue。
- `AddReclaimScenarioValidatorFn`：elastic victim 可以先进入 solver，但最终场景必须保证不会破坏其 minAvailable 保护。

因此有些 victim “看起来资源合适”，但会在 filter 或 validator 阶段被排除。

## Commit 后发生什么

reclaim 成功时，statement 中通常包含：

- victim task 的 `Evict` 操作，commit 后进入 `Cache.Evict()` 并最终删除 Pod。
- reclaimer task 的 `Pipeline` 操作，表示它将等待 victim 释放资源后被下一轮调度推进。
- 可能还有 victim task 的 `Pipeline` 操作，如果 solver 成功把一部分 victim 重新安置。

eviction message 会通过 [`pkg/scheduler/actions/utils/action.go`](../../../../pkg/scheduler/actions/utils/action.go) 带上 reclaimer/reclaimee queue 的 allocated/deserved/fair share 细节，方便从事件里解释为什么发生 reclaim。

## 建议测试阅读

- [`pkg/scheduler/actions/reclaim/reclaim_test.go`](../../../../pkg/scheduler/actions/reclaim/reclaim_test.go)：基础跨 queue reclaim。
- [`pkg/scheduler/actions/reclaim/reclaimDepartments_test.go`](../../../../pkg/scheduler/actions/reclaim/reclaimDepartments_test.go)：department/父队列公平性。
- [`pkg/scheduler/actions/reclaim/reclaimGang_test.go`](../../../../pkg/scheduler/actions/reclaim/reclaimGang_test.go)：gang job reclaim。
- [`pkg/scheduler/actions/reclaim/reclaim_sub_group_test.go`](../../../../pkg/scheduler/actions/reclaim/reclaim_sub_group_test.go)：subgroup reclaim。
- [`pkg/scheduler/actions/reclaim/reclaim_elastic_test.go`](../../../../pkg/scheduler/actions/reclaim/reclaim_elastic_test.go)：elastic victim 只回收 surplus task 与 minAvailable 保护。
- [`pkg/scheduler/actions/integration_tests/reclaim/reclaim_test.go`](../../../../pkg/scheduler/actions/integration_tests/reclaim/reclaim_test.go)：更接近端到端的 reclaim 行为。
