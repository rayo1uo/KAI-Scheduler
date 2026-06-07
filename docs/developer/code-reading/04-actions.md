# 04. Actions

Actions 是 scheduler 的执行阶段。它们决定本轮调度要尝试哪些 job、采用哪类操作、什么时候提交或丢弃 `Statement`。

读 action 前建议先看 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)。`allocate` 的 task 粒度、`reclaim/preempt` 的 victim 粒度、以及所有 action 的 queue 遍历，都依赖层级 PodGroup/SubGroup 和 leaf Queue 约束。

## 深读章节

本章保留 action 总览。逐 action 的详细源码讲解已经拆到子目录：

| 深读文档 | 内容 |
| --- | --- |
| [`actions/README.md`](actions/README.md) | Actions 总地图、默认执行顺序、差异对照 |
| [`actions/01-allocate.md`](actions/01-allocate.md) | `allocate` 主循环、公共分配链路、task 到 node 的完整过程 |
| [`actions/02-reclaim.md`](actions/02-reclaim.md) | 跨 queue reclaim、victim queue、proportion/minruntime 如何参与 |
| [`actions/03-preempt.md`](actions/03-preempt.md) | 同 queue priority preempt、victim filter、solver 复用 |
| [`actions/04-consolidation.md`](actions/04-consolidation.md) | 碎片整理、victim 重排、`allPodsReallocated` validator |
| [`actions/05-stale-gang-eviction.md`](actions/05-stale-gang-eviction.md) | stale gang grace period、直接 `Session.Evict` 路径 |
| [`actions/06-queue-order-and-solver.md`](actions/06-queue-order-and-solver.md) | `JobsOrderByQueues`、`JobsSolver`、scenario builder、虚拟分配 |

## 先打开这些文件

| 内容 | 文件 |
| --- | --- |
| Action 接口 | [`pkg/scheduler/framework/interface.go`](../../../pkg/scheduler/framework/interface.go) |
| Action 注册 | [`pkg/scheduler/actions/factory.go`](../../../pkg/scheduler/actions/factory.go) |
| Allocate | [`pkg/scheduler/actions/allocate/allocate.go`](../../../pkg/scheduler/actions/allocate/allocate.go) |
| 通用分配逻辑 | [`pkg/scheduler/actions/common/allocate.go`](../../../pkg/scheduler/actions/common/allocate.go) |
| Reclaim | [`pkg/scheduler/actions/reclaim/reclaim.go`](../../../pkg/scheduler/actions/reclaim/reclaim.go) |
| Preempt | [`pkg/scheduler/actions/preempt/preempt.go`](../../../pkg/scheduler/actions/preempt/preempt.go) |
| Consolidation | [`pkg/scheduler/actions/consolidation/consolidation.go`](../../../pkg/scheduler/actions/consolidation/consolidation.go) |
| Stale gang eviction | [`pkg/scheduler/actions/stalegangeviction/stalegangeviction.go`](../../../pkg/scheduler/actions/stalegangeviction/stalegangeviction.go) |
| Solver | [`pkg/scheduler/actions/common/solvers`](../../../pkg/scheduler/actions/common/solvers) |

## Action 接口

源码：[`pkg/scheduler/framework/interface.go`](../../../pkg/scheduler/framework/interface.go)

```go
type Action interface {
    // Name 必须能和 scheduler config 里的 action 名字对应。
    Name() ActionType

    // Execute 只应操作 Session。真正落地要通过 Statement.Commit。
    Execute(ssn *Session)
}
```

Action 接口很小，真正的能力来自：

- `Session` 中由插件注册的回调。
- `Statement` 的试算和提交。
- `actions/common` 中的通用调度算法。

## 默认 Action 注册

源码：[`pkg/scheduler/actions/factory.go`](../../../pkg/scheduler/actions/factory.go)

```go
func InitDefaultActions() {
    framework.RegisterAction(reclaim.New())
    framework.RegisterAction(allocate.New())
    framework.RegisterAction(preempt.New())
    framework.RegisterAction(consolidation.New())
    framework.RegisterAction(stalegangeviction.New())
}
```

注意：注册顺序不等于执行顺序。执行顺序来自 scheduler config，默认是：

```text
allocate, consolidation, reclaim, preempt, stalegangeviction
```

## Allocate Action

源码：[`pkg/scheduler/actions/allocate/allocate.go`](../../../pkg/scheduler/actions/allocate/allocate.go)

```go
func (alloc *allocateAction) Execute(ssn *framework.Session) {
    jobsOrderByQueues := utils.NewJobsOrderByQueues(ssn, utils.JobsOrderInitOptions{
        FilterNonPending:  true,
        FilterUnready:     true,
        MaxJobsQueueDepth: ssn.GetJobsDepth(framework.Allocate),
    })
    jobsOrderByQueues.InitializeWithJobs(ssn.ClusterInfo.PodGroupInfos)

    for !jobsOrderByQueues.IsEmpty() {
        job := jobsOrderByQueues.PopNextJob()
        stmt := ssn.Statement()

        if ok, pipelined := attemptToAllocateJob(ssn, stmt, job); ok {
            err := stmt.Commit()

            // job 还有可继续分配的 task 时，重新入队。
            if err == nil && podgroup_info.HasTasksToAllocate(job, true) {
                jobsOrderByQueues.PushJob(job)
                continue
            }
        } else {
            // 分配失败，丢弃本 job 的试算结果。
            stmt.Discard()
        }
    }
}
```

阅读重点：

- `JobsOrderByQueues` 是 Queue 排序和公平性进入 action 的位置。
- 每个 job 会新建一个 `Statement`。
- 成功后 `Commit`，失败后 `Discard`。
- 如果 job 还有更多可分配 task，可以再次入队。

## 通用分配逻辑

源码：[`pkg/scheduler/actions/common/allocate.go`](../../../pkg/scheduler/actions/common/allocate.go)

```go
func AllocateJob(ssn *framework.Session, stmt *framework.Statement,
    nodes []*node_info.NodeInfo, job *podgroup_info.PodGroupInfo, isPipelineOnly bool) bool {

    // 插件可在 job 分配前做准备。
    ssn.PreJobAllocation(job)

    // PodGroup/SubGroup 逻辑决定下一批必须调度的 tasks。
    tasksToAllocate := podgroup_info.GetTasksToAllocate(
        job, ssn.SubGroupOrderFn, ssn.TaskOrderFn, !isPipelineOnly)

    // Queue capacity check 通常由 proportion plugin 提供。
    result := ssn.IsJobOverQueueCapacityFn(job, tasksToAllocate)
    if !result.IsSchedulable {
        return false
    }

    // 从 RootSubGroupSet 开始递归处理层级 PodGroup。
    return allocateSubGroupSet(ssn, stmt, nodes, job, job.RootSubGroupSet, tasksToAllocate, isPipelineOnly)
}
```

建议按这个顺序继续读：

```text
AllocateJob
  -> allocateSubGroupSet
  -> allocateMembersOnNodes
  -> allocatePodSet
  -> allocateTasksOnNodeSet
  -> allocateTask
  -> allocateTaskToNode
```

最深处的 `allocateTask` 是多个策略的汇合点：

```go
func allocateTask(ssn *framework.Session, stmt *framework.Statement,
    nodes []*node_info.NodeInfo, task *pod_info.PodInfo, isPipelineOnly bool) bool {

    // pre-predicate 在节点打分前检查 task 是否可以继续。
    err := ssn.PrePredicateFn(task, job)

    // 插件决定节点排序。
    orderedNodes := ssn.OrderedNodesByTask(nodes, task)

    for _, node := range orderedNodes {
        // 资源 fit 和 Kubernetes predicate 都在这里发生。
        if !ssn.FittingNode(task, node, !isPipelineOnly) {
            continue
        }

        // 根据资源状态选择 Allocate、Pipeline 或 fractional GPU 逻辑。
        success = allocateTaskToNode(ssn, stmt, task, node, isPipelineOnly)
        if success {
            break
        }
    }
}
```

## Reclaim Action

源码：[`pkg/scheduler/actions/reclaim/reclaim.go`](../../../pkg/scheduler/actions/reclaim/reclaim.go)

Reclaim 是跨 Queue 的资源回收。它通常由公平性策略判断某个 queue 是否应该拿回资源。

```go
func (ra *reclaimAction) Execute(ssn *framework.Session) {
    jobsOrderByQueues := utils.NewJobsOrderByQueues(...)

    for !jobsOrderByQueues.IsEmpty() {
        job := jobsOrderByQueues.PopNextJob()

        // 通常由 proportion plugin 决定是否允许 reclaim。
        if !ssn.CanReclaimResources(job) {
            continue
        }

        succeeded, statement, victims := ra.attemptToReclaimForSpecificJob(ssn, job)
        if succeeded {
            statement.Commit()
        }
    }
}
```

Reclaim 的核心调用链：

```text
attemptToReclaimForSpecificJob()
  -> FeasibleNodesForJob()
  -> solvers.NewJobsSolver(...)
  -> solver.Solve(ssn, reclaimer)
```

## Preempt Action

源码：[`pkg/scheduler/actions/preempt/preempt.go`](../../../pkg/scheduler/actions/preempt/preempt.go)

Preempt 是同 Queue 内基于 priority 的抢占。

```go
func buildFilterFuncForPreempt(ssn *framework.Session, preemptor *podgroup_info.PodGroupInfo) func(*podgroup_info.PodGroupInfo) bool {
    return func(job *podgroup_info.PodGroupInfo) bool {
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
        if !ssn.PreemptVictimFilter(preemptor, job) {
            return false
        }
        return true
    }
}
```

Reclaim 和 Preempt 的区别：

| Action | victim 范围 | 主要策略来源 |
| --- | --- | --- |
| Reclaim | 其他 Queue | Queue fairness、deserved resources |
| Preempt | 同一个 Queue | workload priority、preemptibility |

## Solver 读法

源码：[`pkg/scheduler/actions/common/solvers/job_solver.go`](../../../pkg/scheduler/actions/common/solvers/job_solver.go)

```text
JobSolver.Solve()
  -> 计算 pending tasks
  -> searchMaxSolvableK()
     # 探测可以解决多少个 pending task。
  -> probeAtK()
  -> solvePartialJob()
     -> NewPodAccumulatedScenarioBuilder()
     -> 遍历 valid scenario:
          newByPodSolver(...).solve()
             -> EvictAllPreemptees()
             -> TryToVirtuallyAllocatePreemptorAndGetVictims()
             -> validate scenario
             -> 返回 Statement
```

关键点：solver 返回的不只是 victim 列表，而是一个包含虚拟驱逐和虚拟分配的 `Statement`。action 决定是否 commit。

## Consolidation 与 StaleGangEviction

在读完 allocate/reclaim/preempt 后再读：

- [`pkg/scheduler/actions/consolidation/consolidation.go`](../../../pkg/scheduler/actions/consolidation/consolidation.go)：通过迁移 workload 降低碎片，只有所有被移动的任务都有新位置时才提交。
- [`pkg/scheduler/actions/stalegangeviction/stalegangeviction.go`](../../../pkg/scheduler/actions/stalegangeviction/stalegangeviction.go)：清理长期违反 gang 要求的 job，避免死锁。

## 本章检查点

读完后应该能回答：

- 为什么 allocate 通常排在 reclaim/preempt 前？
- 哪个 action 会跨 Queue 找 victim？
- preempt 为什么要求 victim 和 preemptor 在同一个 Queue？
- solver 为什么返回 `Statement`，而不是直接驱逐 Pod？
