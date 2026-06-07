# 01. Allocate Action 源码阅读

`allocate` 是成本最低的 action：它不主动驱逐 victim，只尝试把 pending job 放到当前 idle 或 releasing 资源上。入口是 [`pkg/scheduler/actions/allocate/allocate.go`](../../../../pkg/scheduler/actions/allocate/allocate.go)。

## 主循环

源码骨架：

```go
jobsOrderByQueues := utils.NewJobsOrderByQueues(ssn, utils.JobsOrderInitOptions{
    FilterNonPending:  true,
    FilterUnready:     true,
    MaxJobsQueueDepth: ssn.GetJobsDepth(framework.Allocate),
})
jobsOrderByQueues.InitializeWithJobs(ssn.ClusterInfo.PodGroupInfos)

for !jobsOrderByQueues.IsEmpty() {
    job := jobsOrderByQueues.PopNextJob()
    stmt := ssn.Statement()
    alreadyAllocated := job.GetNumAllocatedTasks() > 0
    if ok, pipelined := attemptToAllocateJob(ssn, stmt, job); ok {
        err := stmt.Commit()
        if err == nil && !pipelined && !alreadyAllocated {
            setLastStartTimestamp(job)
        }
        if err == nil && podgroup_info.HasTasksToAllocate(job, true) {
            jobsOrderByQueues.PushJob(job)
            continue
        }
    } else {
        stmt.Discard()
    }
}
```

这段代码有几个细节：

- `FilterNonPending` 和 `FilterUnready` 把非 pending 或 PodGroup 未 ready 的 job 排除在本 action 外。
- `PopNextJob()` 的顺序不是简单 FIFO，而是 Queue 树排序和 Job 排序共同决定，详见 [06. Queue Order 与 Solver](06-queue-order-and-solver.md)。
- 每个 job 创建一个新的 `Statement`。成功后 commit，失败后 discard。
- 如果 job 是 elastic 或一次只调度部分 task，`HasTasksToAllocate(job, true)` 会让 job 重新入队，继续和其他 queue/job 竞争下一批资源。
- `LastStartTimestamp` 只在第一次真实分配成功且没有 pipeline 时设置，用于运行时长和状态判断。

## `attemptToAllocateJob`

入口函数先计算日志用的资源向量，然后把当前 session 中所有 node 交给公共分配逻辑：

```text
attemptToAllocateJob
  -> podgroup_info.GetTasksToAllocateInitResourceVector
  -> common.AllocateJob(ssn, stmt, nodes, job, false)
  -> job.ShouldPipelineJob()
  -> stmt.ConvertAllAllocatedToPipelined(job.UID)
```

`job.ShouldPipelineJob()` 是 gang/elastic 行为里的关键点：如果某些 task 只能 pipeline，那么整个 job 内刚 allocate 的 task 也会转换成 pipeline。这样可以避免一个 gang job 一部分真实 bind、一部分还在等资源，导致语义被打破。

## 公共分配链路

核心在 [`pkg/scheduler/actions/common/allocate.go`](../../../../pkg/scheduler/actions/common/allocate.go)：

```text
AllocateJob
  -> ssn.PreJobAllocation(job)
  -> podgroup_info.GetTasksToAllocate(...)
  -> ssn.IsJobOverQueueCapacityFn(job, tasksToAllocate)
  -> allocateSubGroupSet(root)
       -> ssn.SubsetNodesFn(...)
       -> stmt.Checkpoint()
       -> allocateMembersOnNodes(...)
            -> allocatePodSet(...) 或 allocateSubGroupSet(...)
                 -> allocateTasksOnNodeSet(...)
                      -> allocateTask(...)
                           -> ssn.PrePredicateFn(task, job)
                           -> ssn.OrderedNodesByTask(nodes, task)
                           -> ssn.FittingNode(task, node, writeFittingDelta)
                           -> allocateTaskToNode(...)
       -> stmt.Rollback(cp) on failed nodeSet
```

这条链路说明了为什么 `allocate` 不是一个简单的“找个节点放 pod”：

- `PreJobAllocation` 允许插件先做 job 级准备，例如 topology 插件清空上一轮 subgroup node score。
- `GetTasksToAllocate` 会尊重 gang、subgroup、podset、elastic、task order。
- `IsJobOverQueueCapacityFn` 通常由 proportion 插件的 capacity policy 提供。
- `SubsetNodesFn` 允许 topology 插件把候选 node 切成多个 topology domain 组合。
- `Checkpoint/Rollback` 让一个 node set 尝试失败后可以回到进入该 node set 前的状态，再试下一个 node set。

## PodGroup 内 task 如何被选出来

`GetTasksToAllocate` 的入口在 [`pkg/scheduler/api/podgroup_info/allocation_info.go`](../../../../pkg/scheduler/api/podgroup_info/allocation_info.go)。它不是简单返回所有 pending pods，而是按 SubGroup tree 分两种阶段收集：

```text
GetTasksToAllocate(job)
  -> collectTasksFromSubGroupSet(root)
       -> children 按 SubGroupOrderFn 排序
       -> gang phase: 当前 subgroup set 未满足 min 要求
            从前 K 个最优 child 收集 task，K = GetMinMembersToSatisfy()
       -> elastic phase: 当前 subgroup set 已满足 min 要求
            只从第一个仍有可分配 task 的最优 child 收集 task
```

到 PodSet 层后还会按 `TaskOrderFn` 排序，并根据是否已达到 `minAvailable` 决定一次拿多少个 task：

- 未达到 `minAvailable`：拿够满足 gang/minAvailable 所需的 task 数。
- 已达到 `minAvailable`：elastic 扩容阶段一次最多拿 1 个 task，让 queue fairness 有机会在 job 之间轮转。

这解释了为什么 `allocate` 成功后还会 `PushJob(job)`。一个 job 即使还有 pending task，也可能只被允许在本次 pop 中推进一个 subgroup 或一个 elastic task，然后重新回到 leaf queue 里竞争。

## 单个 task 如何落到节点

`allocateTask` 是 action 和 plugin 最密集的汇合点：

```text
allocateTask
  -> ssn.PrePredicateFn(task, job)
  -> ssn.OrderedNodesByTask(nodes, task)
       -> ssn.NodePreOrderFn(task, nodes)
       -> 并发调用 ssn.NodeOrderFn(task, node)
       -> 按 score 降序排序
  -> for node in orderedNodes
       -> ssn.FittingNode(task, node, !isPipelineOnly)
            -> node.IsTaskAllocatableOnReleasingOrIdle(task)
            -> ssn.PredicateFn(task, job, node)
       -> allocateTaskToNode
```

`allocateTaskToNode` 再分三种情况：

- fractional GPU 或 GPU memory request：走 `gpu_sharing.AllocateFractionalGPUTaskToNode`，内部会调用 `Session.FittingGPUs`，再由 `gpupack/gpuspread` 等 GPU order 插件打分。
- 普通资源且 idle 足够：`stmt.Allocate(task, node.Name)`，commit 后创建 BindRequest。
- 资源需要等待 releasing：`stmt.Pipeline(task, node.Name, ...)`，commit 后记录 pipeline 状态，不立即 bind。

## 哪些插件会影响 allocate

| 插件回调 | 调用点 | 影响 |
| --- | --- | --- |
| `QueueOrderFn` | `JobsOrderByQueues` | 哪个 queue 的 pending job 先被尝试，默认由 `proportion` 提供公平性顺序 |
| `JobOrderFn` | leaf queue 内部 | 同一 queue 内哪个 job 先尝试，常见来源是 `priority`、`elastic` |
| `SubGroupOrderFn` | `GetTasksToAllocate`、`orderedMembers` | subgroup/podset 的遍历顺序 |
| `TaskOrderFn` | `GetTasksToAllocate` | PodGroup 内 task 的分配顺序，如 Kubeflow master/Ray head |
| `PrePredicateFn` | `allocateTask` | task 级提前失败，不进入节点循环 |
| `PredicateFn` | `FittingNode` | task/node 组合约束，如 K8s predicates、DRA、volume binding |
| `NodePreOrderFn`/`NodeOrderFn` | `OrderedNodesByTask` | 节点打分，如 binpack/spread、CPU-only、nominated node |
| `GpuOrderFn` | `FittingGPUs` | shared GPU 内部 pack 或 spread |
| capacity policy | `AllocateJob`/`FittingNode` | queue quota/capacity 检查 |

## 常见失败路径

`allocate` 失败时会把原因写回 `job.TasksFitErrors` 或 `job.JobFitErrors`，最后在 `CloseSession()` 通过 cache/status updater 记录事件。典型失败包括：

- PodGroup 未 ready，根本不会进入 action。
- Queue capacity 不允许更多资源。
- `PrePredicateFn` 失败，例如 PVC/CSI/ResourceClaim 前置检查失败。
- 某个 subgroup/podset 需要 gang 满足，但只找到部分资源。
- 节点资源不足或 `PredicateFn` 失败。

## 建议测试阅读

- [`pkg/scheduler/actions/allocate/allocate_test.go`](../../../../pkg/scheduler/actions/allocate/allocate_test.go)：基础 allocate、queue share、多 pending job。
- [`pkg/scheduler/actions/allocate/allocateGang_test.go`](../../../../pkg/scheduler/actions/allocate/allocateGang_test.go)：gang 满足和失败。
- [`pkg/scheduler/actions/allocate/allocate_subgroups_test.go`](../../../../pkg/scheduler/actions/allocate/allocate_subgroups_test.go)：subgroup/podset 递归分配。
- [`pkg/scheduler/actions/allocate/allocateFractionalGpu_test.go`](../../../../pkg/scheduler/actions/allocate/allocateFractionalGpu_test.go)：fractional GPU 路径。
- [`pkg/scheduler/actions/allocate/allocateGpuMemory_test.go`](../../../../pkg/scheduler/actions/allocate/allocateGpuMemory_test.go)：GPU memory request 与最小 GPU memory 资源估算。
- [`pkg/scheduler/actions/allocate/allocateMIG_test.go`](../../../../pkg/scheduler/actions/allocate/allocateMIG_test.go)：MIG 资源请求如何进入 node/GPU 可行性判断。
- [`pkg/scheduler/actions/allocate/allocate_stuck_in_releasing_test.go`](../../../../pkg/scheduler/actions/allocate/allocate_stuck_in_releasing_test.go)：stuck releasing 对 pipeline/allocate 的影响。
- [`pkg/scheduler/actions/allocate/allocateTopology_test.go`](../../../../pkg/scheduler/actions/allocate/allocateTopology_test.go)：topology subset 对候选节点的影响。
