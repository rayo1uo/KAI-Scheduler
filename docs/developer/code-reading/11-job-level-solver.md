# 11. Job-Level Solver 源码导读

这一章专门解释 KAI Scheduler 的 job-level solver。它是 `reclaim`、`preempt`、`consolidation` 三个 action 共用的求解器，用来搜索一组 victim scenario，并用 `Statement` 做可回滚的虚拟调度。

阅读这一章前建议先看：

- [03. Session 与 Statement](03-session-and-statement.md)：理解虚拟副作用、rollback、commit。
- [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)：理解 `minAvailable`、`minSubGroup`、elastic phase、leaf queue。
- [actions/06. Queue Order 与 Solver](actions/06-queue-order-and-solver.md)：理解 pending queue 与 victim queue 如何产生。

## 先打开哪些文件

| 层次 | 源码入口 | 阅读重点 |
| --- | --- | --- |
| action 入口 | [`pkg/scheduler/actions/preempt/preempt.go`](../../../pkg/scheduler/actions/preempt/preempt.go)、[`pkg/scheduler/actions/reclaim/reclaim.go`](../../../pkg/scheduler/actions/reclaim/reclaim.go)、[`pkg/scheduler/actions/consolidation/consolidation.go`](../../../pkg/scheduler/actions/consolidation/consolidation.go) | 哪个 job 作为 preemptor/reclaimer，victim queue 怎样过滤 |
| solver 外层 | [`pkg/scheduler/actions/common/solvers/job_solver.go`](../../../pkg/scheduler/actions/common/solvers/job_solver.go) | partial probe、victim hints、完整 job 最终求解 |
| scenario 累计 | [`pkg/scheduler/actions/common/solvers/pod_scenario_builder.go`](../../../pkg/scheduler/actions/common/solvers/pod_scenario_builder.go) | 从 victim queue 累计 potential victims，并做粗筛 |
| 子场景生成 | [`pkg/scheduler/actions/common/solvers/sub_scenario_emitter.go`](../../../pkg/scheduler/actions/common/solvers/sub_scenario_emitter.go) | 按 victim-bearing nodes 切出更小场景，同时保持 gang victim batch 原子性 |
| scenario 模型 | [`pkg/scheduler/actions/common/solvers/scenario/base_scenario.go`](../../../pkg/scheduler/actions/common/solvers/scenario/base_scenario.go)、[`pkg/scheduler/actions/common/solvers/scenario/by_node_scenario.go`](../../../pkg/scheduler/actions/common/solvers/scenario/by_node_scenario.go) | preemptor、pending tasks、potential victims、recorded victims 的数据结构 |
| 逐 Pod 模拟 | [`pkg/scheduler/actions/common/solvers/by_pod_solver.go`](../../../pkg/scheduler/actions/common/solvers/by_pod_solver.go) | 先虚拟驱逐 victims，再跑完整 `AllocateJob(..., isPipelineOnly=true)` |
| 公共虚拟分配 | [`pkg/scheduler/actions/common/action.go`](../../../pkg/scheduler/actions/common/action.go)、[`pkg/scheduler/actions/common/allocate.go`](../../../pkg/scheduler/actions/common/allocate.go) | victim jobs 和 preemptor 在同一套调度流程里重新排队试算 |
| 事务语义 | [`pkg/scheduler/framework/statement.go`](../../../pkg/scheduler/framework/statement.go) | `Evict`、`Pipeline`、`Allocate` 的立即内存变更与最终 `Commit` |
| task 选择 | [`pkg/scheduler/api/podgroup_info/allocation_info.go`](../../../pkg/scheduler/api/podgroup_info/allocation_info.go)、[`pkg/scheduler/api/podgroup_info/eviction_info.go`](../../../pkg/scheduler/api/podgroup_info/eviction_info.go) | 当前必须分配哪些 tasks，以及 victim 该先剥离 elastic surplus 还是整组驱逐 |

## 一句话模型

job-level solver 不是“给单个 pending pod 找一个节点”，而是：

```text
给一个 pending PodGroup
  -> 取出当前必须整体推进的 tasksToAllocate
  -> 搜索一组 victim jobs/tasks
  -> 在 Statement 中虚拟驱逐 victims
  -> 重新运行完整 AllocateJob 调度流程
  -> 只有目标 PodGroup 的 gang/subgroup 语义被满足时才返回可 Commit 的 Statement
```

它仍然会在最内层逐 Pod、逐节点检查 predicate 和资源 fit，但外层成功条件是 job/gang/subgroup 级别的。

## 三个 action 如何接入 solver

### Preempt

入口在 [`preempt.go`](../../../pkg/scheduler/actions/preempt/preempt.go)。

```go
solver := solvers.NewJobsSolver(
    feasibleNodes,
    ssn.PreemptScenarioValidator,
    getOrderedVictimsQueue(ssn, preemptor),
    framework.Preempt,
)
return solver.Solve(ssn, preemptor)
```

`preempt` 的 victim filter 是同 queue、低 priority、preemptible、非自己、有 active allocated task，并且通过 `ssn.PreemptVictimFilter()`。因此它主要解决“同队列内高优先级 job 抢占低优先级 job”的问题。

额外的 quota 检查在进入 solver 之前完成：

```go
preemptorTasks := podgroup_info.GetTasksToAllocate(...)
if result := ssn.IsNonPreemptibleJobOverQueueQuotaFn(preemptor, preemptorTasks); !result.IsSchedulable {
    return false, nil, nil
}
```

这表示某些 non-preemptible 资源约束在 scenario 搜索前就能直接排除。

### Reclaim

入口在 [`reclaim.go`](../../../pkg/scheduler/actions/reclaim/reclaim.go)。

```go
solver := solvers.NewJobsSolver(
    feasibleNodes,
    ssn.ReclaimScenarioValidatorFn,
    getOrderedVictimsQueue(ssn, reclaimer),
    framework.Reclaim,
)
return solver.Solve(ssn, reclaimer)
```

`reclaim` 的 pending job 先要通过 `ssn.CanReclaimResources(job)`，通常由 `proportion` 插件判断该 queue 是否应该拿回资源。victim queue 会排除 reclaimer 同 queue 的 jobs，只考虑其他 queue 中 preemptible 且 active allocated 的 jobs，并经过 `ssn.ReclaimVictimFilter()`。

因此 reclaim 是跨 queue 的公平性回收，不是单纯 priority 抢占。

### Consolidation

入口在 [`consolidation.go`](../../../pkg/scheduler/actions/consolidation/consolidation.go)。

```go
solver := solvers.NewJobsSolver(
    feasibleNodes,
    allPodsReallocated,
    func() *utils.JobsOrderByQueues { return buildConsolidationVictimsQueue(ssn, preemptor) },
    framework.Consolidation,
)
```

`consolidation` 的 validator 是 `allPodsReallocated`：

```go
for _, victim := range scenario.GetVictims() {
    for _, task := range victim.Tasks {
        if task.Status == pod_status.Releasing {
            return false
        }
    }
}
return true
```

含义是：consolidation 不是为了最终驱逐 victim，而是为了把 victim 搬家、整理碎片，给 preemptor 腾出可用组合。因此所有 victim 最终都必须重新被 pipeline 到某个位置。

## `JobSolver` 的字段

源码：[`job_solver.go`](../../../pkg/scheduler/actions/common/solvers/job_solver.go)

```go
type JobSolver struct {
    feasibleNodes        []*node_info.NodeInfo
    solutionValidator    SolutionValidator
    generateVictimsQueue GenerateVictimsQueue
    actionType           framework.ActionType
}
```

这四个字段把通用 solver 和具体 action 解耦：

- `feasibleNodes` 是初始可考虑的节点集合。GPU job 会先过滤出有 idle/releasing GPU 的节点，非 GPU job 可以考虑所有节点。
- `solutionValidator` 是 action/plugin 对 scenario 的最终合法性判断，例如 reclaim fairshare、minruntime、consolidation victim 必须被重排。
- `generateVictimsQueue` 每次生成新的 victim job 队列。partial probe 期间会多次创建 builder，所以这里用函数而不是直接保存一个队列实例。
- `actionType` 会写进 eviction metadata 和日志，用来区分 `reclaim`、`preempt`、`consolidation`。

## `Solve()` 主流程

`Solve()` 是整个 job-level solver 的入口。

```go
func (s *JobSolver) Solve(
    ssn *framework.Session, pendingJob *podgroup_info.PodGroupInfo) (bool, *framework.Statement, []string) {
    state := solvingState{}
    originalNumActiveTasks := pendingJob.GetNumActiveUsedTasks()

    tasksToAllocate := podgroup_info.GetTasksToAllocate(pendingJob, ssn.SubGroupOrderFn, ssn.TaskOrderFn, false)
    n := len(tasksToAllocate)
    if n == 0 {
        return false, nil, calcVictimNames(state.recordedVictimsTasks)
    }

    maxSolvedK := s.searchMaxSolvableK(ssn, &state, pendingJob, tasksToAllocate)
    if maxSolvedK == 0 {
        return false, nil, calcVictimNames(state.recordedVictimsTasks)
    }

    result := s.probeAtK(ssn, &state, pendingJob, tasksToAllocate, n)
    if result == nil || !result.solved {
        return false, nil, calcVictimNames(state.recordedVictimsTasks)
    }

    numActiveTasks := pendingJob.GetNumActiveUsedTasks()
    jobSolved := pendingJob.IsGangSatisfied()
    if originalNumActiveTasks >= numActiveTasks {
        jobSolved = false
    }

    return jobSolved, result.statement, calcVictimNames(result.victimsTasks)
}
```

这段代码有几个边界要特别记住：

- `GetTasksToAllocate()` 返回空时，solver 不会为了 victim 搬迁而运行。
- `searchMaxSolvableK()` 只是搜索 hints，它的成功不等于最终成功。
- 最后必须 `probeAtK(..., n)`，也就是对完整任务集合再求一次解。
- 最后还要检查 `pendingJob.IsGangSatisfied()`。
- 本轮 active used task 数必须比进入 solver 前增加，否则即使 scenario 看似可行也不算解决。

这里的 `active used` 包含实际 allocated 和 pipelined 一类能占用调度视图的状态。它用来避免 solver 提交一个没有推进目标 job 的空解。

## 为什么先搜索 partial job

`searchMaxSolvableK()` 用指数增长加二分搜索最大可解的 K：

```text
k = 1, 2, 4, 8 ...
  -> 直到失败或达到 n
  -> 如果中间失败，在最后成功和首次失败之间二分
```

每个 probe 都会调用 `tryProbeAndDiscard()`：

```go
result := s.probeAtK(ssn, state, pendingJob, tasksToAllocate, k)
if result == nil || !result.solved {
    return false
}
state.recordedVictimsTasks = result.victimsTasks
state.recordedVictimsJobs = result.victimJobs
if result.statement != nil {
    result.statement.Discard()
}
return true
```

这一步的目的不是提交 partial 解，而是找到一组“很可能有用”的 recorded victims。后续更大的 K 可以先带着这些 victims 继续试，减少重复搜索。

注意 `Discard()` 很重要。probe 会真实修改本轮 session 的 node/job/resource 视图，如果不撤销，后续 scenario 会在污染后的状态上继续算。

## partial job representative

partial probe 不能直接拿原始 PodGroup 去试，因为原始 PodGroup 的 `minAvailable` 和 `minSubGroup` 可能要求完整 gang。只拿前 K 个 pending tasks 时，如果还要求完整 gang，就会把本该可用于搜索 hints 的 partial probe 判失败。

因此 `probeAtK()` 会构造 partial representative：

```go
pendingTasks := tasksToAllocate[:k]
partialPendingJob := getPartialJobRepresentative(pendingJob, pendingTasks)
return s.solvePartialJob(ssn, state, partialPendingJob)
```

`getPartialJobRepresentative()` 只保留两类 tasks：

```go
representativeTasks := append(job.GetAllAllocatedPods(), pendingTasks...)
jobRepresentative := job.CloneWithTasks(representativeTasks)

adjustSubGroupsMinAvailable(jobRepresentative)
adjustSubGroupsMinSubGroup(jobRepresentative.RootSubGroupSet)
```

也就是说，partial job 代表的是“已经在跑的部分 + 本次探测的前 K 个 pending tasks”。随后它会把 PodSet 的 `minAvailable` 降到不超过当前代表里实际拥有的 task 数，并递归把 `minSubGroup` 降到不超过非空 direct members 数量。

这个调整只作用在 clone 上，不会改变原始 job。最后完整 probe 仍然使用真实的 `n` 个 pending tasks。

## `solvePartialJob()` 如何启动 scenario 搜索

`solvePartialJob()` 会构造 `feasibleNodeMap`：

```go
feasibleNodeMap := map[string]*node_info.NodeInfo{}
for _, node := range s.feasibleNodes {
    feasibleNodeMap[node.Name] = node
}
for _, task := range state.recordedVictimsTasks {
    node := ssn.ClusterInfo.Nodes[task.NodeName]
    feasibleNodeMap[task.NodeName] = node
}
```

这里把 recorded victim 所在节点也加进来。原因是初始 `feasibleNodes` 可能只包含当前有 idle/releasing GPU 的节点，而 recorded victims 一旦被驱逐，它们所在节点也会释放资源，必须纳入后续模拟。

之后创建 scenario builder：

```go
scenarioBuilder := NewPodAccumulatedScenarioBuilder(
    ssn, partialPendingJob, state.recordedVictimsJobs, s.generateVictimsQueue(), feasibleNodeMap)
```

然后逐个取 scenario：

```go
for scenarioToSolve := scenarioBuilder.GetValidScenario(); scenarioToSolve != nil; scenarioToSolve =
    scenarioBuilder.GetNextScenario() {
    scenarioSolver := newByPodSolver(feasibleNodeMap, s.solutionValidator, ssn.AllowConsolidatingReclaim(),
        s.actionType)

    result := scenarioSolver.solve(ssn, scenarioToSolve)
    if result.solved {
        return result
    }
}
```

可以把这一层理解成：`JobSolver` 负责决定“试多少 pending tasks、带哪些 recorded victims、要不要继续搜索”，而 `byPodSolver` 负责判断某个具体 scenario 是否真的能放下。

## Scenario 的内部数据

`BaseScenario` 保存五类核心数据：

```go
type BaseScenario struct {
    preemptor             *podgroup_info.PodGroupInfo
    victims               map[common_info.PodGroupID]*api.VictimInfo
    pendingTasks          []*pod_info.PodInfo
    potentialVictimsTasks []*pod_info.PodInfo
    recordedVictimsJobs   []*podgroup_info.PodGroupInfo
    recordedVictimsTasks  []*pod_info.PodInfo

    victimsJobsTaskGroups map[common_info.PodGroupID][]*podgroup_info.PodGroupInfo
}
```

概念上可以这样区分：

- `preemptor`：当前要被满足的 pending/reclaimer/preemptor job。
- `pendingTasks`：这个 preemptor 当前必须整体推进的 tasks。
- `potentialVictimsTasks`：当前 scenario 新增尝试的 victims。
- `recordedVictimsJobs/Tasks`：前面 partial probe 已经证明有用的 victims。
- `victims`：按 victim job 聚合后给 validator 使用的视图。
- `victimsJobsTaskGroups`：同一个 victim job 可能被拆成多个 representative，例如 elastic victim 分批剥离时，需要能从 task 找回对应 representative。

`ByNodeScenario` 额外建立一个 node 到 victim job 的索引：

```go
potentialVictimsJobsByNode map[string][]common_info.PodGroupID
```

旧的 per-node 场景切分依赖这个索引。现在 `subScenarioEmitter` 更显式地按 victim batch 生成子场景，但这个结构仍然是理解 scenario 与 node 关系的重要入口。

## Scenario builder 如何累计 victims

`NewPodAccumulatedScenarioBuilder()` 会先创建一个只含 pending tasks 和 recorded victims 的 scenario，然后初始化三类 accumulated filters：

```go
nodeSelectorFilter := node_affinities.NewNodeAffinitiesFilter(scenario, feasibleNodes, session)
topologyAwareFilter := idle_gpus_filter.NewTopologyAwareIdleGpusFilter(scenario, session.ClusterInfo.Nodes)
idleGpusScenarioFilter := idle_gpus_filter.NewIdleGpusFilter(scenario, session.ClusterInfo.Nodes)
```

这三个 filter 是粗筛，不是最终正确性：

- `NodeAffinitiesFilter` 检查 pending pods 的 required node affinity/node selector 是否还有可能满足。
- `TopologyAwareIdleGpusFilter` 按 topology domain 估算 GPU 容量是否可能够。
- `IdleGpusFilter` 按集群 idle/releasing 加 victims freed GPU 估算总量是否可能够。

真正的 scenario 迭代在 `iterate()`：

```text
iterate(advanceFirst)
  -> 如果 subEmitter 还能吐子场景，先返回子场景
  -> 如果需要推进 victim queue，PopNextJob()
       -> GetTasksToEvict(nextVictimJob)
       -> elastic victim 如果还有剩余，CloneWithTasks(remainingTasks) 后 PushJob()
       -> 把本批 tasks 加入 lastScenario.potentialVictimsTasks
  -> 运行 accumulated filters
  -> 没有 potential victims 时返回 recorded-victims-only scenario
  -> 有 potential victims 时创建 subScenarioEmitter
```

最关键的是 `addNextPotentialVictims()`：

```go
potentialVictimTasks, jobHasMoreTasks := podgroup_info.GetTasksToEvict(
    nextVictimJob, asb.session.SubGroupOrderFn, asb.session.TaskOrderFn,
)

if jobHasMoreTasks {
    jobToPush := nextVictimJob.CloneWithTasks(remainingTasks)
    asb.victimsJobsQueue.PushJob(jobToPush)
}

asb.lastScenario.AddPotentialVictimsTasks(potentialVictimTasks)
```

这说明 victim queue 中的 job 不一定一次被完整消费。对 elastic job，solver 会先尝试剥离 surplus。如果不够，再逐步把剩余部分作为新的 representative 放回 victim queue。

## victim task 粒度从哪里来

victim task 粒度由 `GetTasksToEvict()` 决定：

```text
GetTasksToEvict(job)
  -> reverse TaskOrderFn / reverse SubGroupOrderFn
  -> collectElasticEvictionFromSubGroupSet()
       -> Phase 1: 递归寻找更深层 elastic surplus
       -> Phase 2: 如果当前 SubGroupSet 有 surplus direct child，驱逐低优先 child
  -> 如果没有 elastic surplus
       -> collectAllAllocatedTasksFromSubGroupSet()
```

这对应三个层次：

- 叶子 `PodSet` 如果 active allocated 超过 `minAvailable`，一次只取一个低优先 task。
- 中间 `SubGroupSet` 如果满足的 direct child 数超过 `minSubGroup`，可以驱逐一个低优先 child 的完整 subtree。
- 如果没有任何 elastic surplus，就进入 full gang/subtree eviction。

因此 KAI 的 victim solver 有 gang-aware 的倾向：它会尽量不破坏 victim 的最小 gang，只有被迫 fallback 时才整组释放。

## sub-scenario emitter 为什么存在

outer scenario 累计的是“到目前为止可能需要考虑的 victims”。如果直接把所有 potential victims 都交给 `byPodSolver`，会导致模拟很重，也可能驱逐过多。

`subScenarioEmitter` 做的是从 outer scenario 中生成更小的子场景：

```text
newSubScenarioEmitter(base)
  -> 统计 pending demand 和最小 pending task GPU
  -> 统计 recorded victims 已经能释放的资源
  -> buildVictimBatches()
  -> nodeCapacities()
  -> sortViableCandidates()
  -> baselineCapacity()
  -> smallestKCovering()
```

它按 victim-bearing nodes 的容量排序，从最小可覆盖 pending demand 的 top-K 节点开始试，然后 K 逐步扩大。

这里最重要的 gang 语义在 `next()`：

```go
for i := 0; i < k; i++ {
    for _, bi := range sse.nodeBatches[sse.sortedNodes[i]] {
        pickedBatches[bi] = true
    }
}

for bi := range pickedBatches {
    sub.AddPotentialVictimsTasks(sse.batches[bi].tasks)
}
```

`pickedBatches` 不是单个 pod，而是一批 victim tasks。batch 来自一次 accumulator pop 出来的 victim representative。一个多节点 gang victim 如果有 task 分布在多个节点上，只要某个节点被选中，最终加入的是该 batch 的完整 task 集合。

这避免了“只驱逐 gang victim 的半边节点”这种不完整场景。

## `byPodSolver.solve()` 的模拟步骤

`byPodSolver` 是具体 scenario 的执行器：

```go
func (s *byPodSolver) solve(session *framework.Session, scenario *scenario.ByNodeScenario) *solutionResult {
    statement := session.Statement()

    pendingJob := scenario.GetPreemptor()
    nextTaskToFindAllocation := scenario.PendingTasks()[len(scenario.PendingTasks())-1]

    allVictims := getVictimTasks(scenario.RecordedVictimsTasks(), scenario.PotentialVictimsTasks())

    if len(allVictims) == 0 {
        statement.Discard()
        return &solutionResult{false, nil, nil, nil}
    }

    checkpoint := statement.Checkpoint()
    if err := common.EvictAllPreemptees(session, allVictims, pendingJob, statement, s.actionType); err != nil {
        return handleSolveError(pendingJob, nextTaskToFindAllocation, err, statement)
    }
    newFeasibleNodes := s.updateFeasibleNodes(session, allVictims)

    result := s.runSimulation(session, scenario, statement, allVictims, maps.Values(s.feasibleNodes))
    if result != nil {
        return result
    }

    s.feasibleNodesRollback(newFeasibleNodes)
    if err := statement.Rollback(checkpoint); err != nil {
        return handleSolveError(pendingJob, nextTaskToFindAllocation, err, statement)
    }
    statement.Discard()
    return &solutionResult{false, nil, nil, nil}
}
```

读这段时抓住四个动作：

- 新建一个 `Statement`。
- 把 recorded victims 和 potential victims 合并。
- 先用 `Statement.Evict()` 虚拟驱逐所有 victims。
- 在同一个 statement 里重新尝试分配 preemptor 和相关 victim jobs。

如果失败，rollback 到 checkpoint，再 discard。成功时返回 live statement 给 action，action 后续 `Commit()`。

## 虚拟驱逐 victims

`EvictAllPreemptees()` 在 [`action.go`](../../../pkg/scheduler/actions/common/action.go)：

```go
messages := getEvictionMessages(ssn, preempteeTasks, preemptor, actionType)
for _, task := range preempteeTasks {
    err := stmt.Evict(task, message, eviction_info.EvictionMetadata{
        Action:           string(actionType),
        EvictionGangSize: len(preempteeTasks),
        Preemptor:        &types.NamespacedName{Namespace: preemptor.Namespace, Name: preemptor.Name},
    })
}
```

这里的 `EvictionGangSize` 是本 scenario 里一起驱逐的 victim task 数量，不一定等于 preemptor 的 gang size。它用于记录这次 eviction 是成组发生的。

`stmt.Evict()` 立刻把 task 状态更新成 `Releasing`，并更新 node/job 资源视图，同时触发 `DeallocateFunc` event handlers。此时还没有真正调用 Kubernetes eviction，真正外部化发生在 `Statement.Commit()`。

## 重新排队并虚拟分配

驱逐 victims 后，solver 调用：

```go
jobsToAllocate := common.GetJobsToAllocate(ssn, victimTasks, pendingJob)
isSuccessfulAllocations, _ :=
    common.TryToVirtuallyAllocatePreemptorAndGetVictims(ssn, statement, nodes, pendingJob,
        jobsToAllocate, victimTasks)
```

`GetJobsToAllocate()` 会把所有 pending jobs、victim jobs、preemptor 放进一个 `JobsOrderByQueues`：

```text
all pending jobs
  + victims 所属 jobs
  + preemptor
  -> JobsOrderByQueues
```

但 `TryToVirtuallyAllocatePreemptorAndGetVictims()` 实际只处理 potential victim jobs 和 preemptor：

```go
for !jobsToAllocate.IsEmpty() {
    jobToAllocate := jobsToAllocate.PopNextJob()
    if _, exits := potentialVictimsMap[jobToAllocate.UID]; !exits && jobToAllocate.UID != preemptor.UID {
        continue
    }

    if jobToAllocate.UID != preemptor.UID {
        if !AllocateJob(ssn, stmt, nodes, jobToAllocate, true) {
            tasksToAllocate := podgroup_info.GetTasksToAllocate(jobToAllocate, ...)
            newVictims = append(newVictims, tasksToAllocate...)
        }
        continue
    }

    success := AllocateJob(ssn, stmt, nodes, jobToAllocate, true)
    if !success {
        return false, []*pod_info.PodInfo{}
    }
    preemptorAllocated = true
}
```

这段是 solver 的关键：它不是只给 preemptor 找节点，而是把 victim jobs 也放回公平排序中尝试重新安置。如果 victim 能被 `Pipeline`，它就可能成为 `pipelinedVictims`，不一定最终被外部驱逐。

`isPipelineOnly=true` 表示这里是“依赖 Releasing 资源的虚拟调度”。即使 node 当前还没有真实空闲资源，也可以把任务 pipeline 到即将释放的资源上。

## `AllocateJob()` 为什么保证真实调度语义

`AllocateJob()` 复用 allocate action 的公共调度链：

```text
AllocateJob
  -> PreJobAllocation
  -> GetTasksToAllocate
  -> IsJobOverQueueCapacityFn
  -> allocateSubGroupSet
       -> SubsetNodesFn
       -> allocateMembersOnNodes
       -> allocatePodSet
            -> SubsetNodesFn
            -> allocateTasksOnNodeSet
                 -> allocateTask
                      -> PrePredicateFn
                      -> OrderedNodesByTask
                      -> FittingNode
                      -> Allocate/Pipeline/fractional GPU allocation
```

因此 solver 的成功不是简单的资源向量加减。下列因素都会进入判断：

- PodGroup/SubGroup 的 gang 与 elastic 语义。
- Queue capacity。
- Kubernetes predicates。
- node affinity、taint、resource fit。
- GPU sharing、GPU memory、MIG、DRA 等资源分支。
- topology plugin 通过 `SubsetNodesFn` 对 node set 的切分。
- node order 和 GPU order 插件。

这也是 KAI solver 和很多“只算释放多少 GPU”的抢占逻辑最大的区别。

## 成功场景如何收敛 victims

`tryScenarioWithEvictedVictims()` 成功后，会按 victim 当前状态分类：

```go
for _, victimTask := range victimTasks {
    switch victimTask.Status {
    case pod_status.Releasing:
        actualVictims.preemptedVictims = append(actualVictims.preemptedVictims, victimTask)
    case pod_status.Pipelined:
        actualVictims.pipelinedVictims = append(actualVictims.pipelinedVictims, victimTask)
    }
}
```

`handleScenarioSolution()` 再根据 `allowVictimConsolidation` 处理：

```go
victimsTasks := copy(preemptedVictims)
if !s.allowVictimConsolidation {
    victimsTasks = append(victimsTasks, pipelinedVictims...)
}

if s.solutionValidator != nil {
    validSolution := s.solutionValidator(scenario)
    if !validSolution {
        statement.Discard()
        return &solutionResult{false, nil, nil, nil}
    }
}

if s.allowVictimConsolidation {
    victimsTasks = append(victimsTasks, pipelinedVictims...)
}
```

读这里要注意 validator 的输入时机：

- 关闭 `allowVictimConsolidation` 时，pipelined victims 会提前纳入 victim 列表。
- 开启时，validator 先看较窄的 victim 集合，然后再把 pipelined victims 加回最终结果。

这个行为会影响 reclaim/preempt 场景下 validator 如何解释“真正被牺牲的 victim”和“被顺手搬家的 victim”。

## Plugin validator 如何介入

solver 本身不知道公平性和 min runtime 的业务含义，它只调用 `solutionValidator`。具体规则来自插件向 `Session` 注册的回调。

### Proportion

`proportion` 在 session open 时注册：

```go
ssn.AddCanReclaimResourcesFn(pp.CanReclaimResourcesFn)
ssn.AddReclaimScenarioValidatorFn(pp.reclaimableFn)
ssn.AddIsNonPreemptibleJobOverQueueQuotaFns(capacityPolicy.IsNonPreemptibleJobOverQuota)
ssn.AddIsJobOverCapacityFn(capacityPolicy.IsJobOverQueueCapacity)
ssn.AddIsTaskAllocationOnNodeOverCapacityFn(capacityPolicy.IsTaskAllocationOnNodeOverCapacity)
```

其中 `reclaimableFn()` 会读取 scenario victims，把各 victim queue 将释放的资源聚合起来，再让 reclaimable policy 判断这个跨队列 scenario 是否公平。

这解释了为什么 reclaim 不能只看“别的 queue 有空可抢”：必须通过 queue hierarchy、deserved/fairshare/capacity 等策略。

### Minruntime

`minruntime` 在 session open 时注册：

```go
ssn.AddReclaimVictimFilterFn(mr.reclaimFilterFn)
ssn.AddPreemptVictimFilterFn(mr.preemptFilterFn)
ssn.AddReclaimScenarioValidatorFn(mr.reclaimScenarioValidatorFn)
ssn.AddPreemptScenarioValidatorFn(mr.preemptScenarioValidatorFn)
```

普通 victim filter 会直接保护未达到最小运行时间的非 elastic job。elastic job 会先放行到 scenario validator，因为它可能只被剥离 surplus，而不破坏 `minAvailable`。

validator 的核心语义是：如果一个 elastic victim 仍在 minruntime 保护期内，那么 scenario 只能剥离不影响最小 gang 的部分。

## `Statement` 如何保证 all-or-nothing

`Statement` 在 solver 里不是单纯记录日志。`Evict`、`Pipeline`、`Allocate` 调用当下就会修改 session 内部视图：

| 操作 | 立即发生的内存变更 | Commit 时外部化 |
| --- | --- | --- |
| `Evict` | victim task 变 `Releasing`，node/job 资源视图更新，触发 `DeallocateFunc` | `Cache.Evict()` |
| `Pipeline` | task 变 `Pipelined`，写入目标 node，触发 `AllocateFunc` | `Cache.TaskPipelined()` |
| `Allocate` | task 变 `Allocated`，写入 node，触发 `AllocateFunc` | `Cache.Bind()`，后续 binder 创建真实绑定 |
| `Rollback/Discard` | 反向撤销 session 变更 | 不外部化 |

这让 solver 能在内存里运行完整调度逻辑。失败时，`Rollback(checkpoint)` 或 `Discard()` 把这轮试算从 session 中擦掉；成功时，把同一个 live statement 交给 action `Commit()`。

如果读日志时看到某个 task 先 `Releasing` 后又 `Pipelined`，不要立刻理解为真实 Kubernetes 已经执行了这些操作。它可能只是 solver 的一次虚拟 scenario。

## 多机多卡 gang 的阅读例子

假设一个大模型推理 PodGroup 有 4 个 pod，每个 pod 需要 2 张 GPU，`minAvailable=4`，分布式加载要求 4 个 pod 同时可运行。

solver 的行为可以这样展开：

```text
preempt/reclaim action 选中这个 PodGroup
  -> GetTasksToAllocate 返回 4 个 pending tasks
  -> searchMaxSolvableK 先试 1、2、4 个 task 的 partial representative
  -> scenario builder 从 victim queue 逐步累计 victim jobs/tasks
  -> subScenarioEmitter 按 victim-bearing nodes 生成子场景
  -> byPodSolver 虚拟驱逐 victims
  -> AllocateJob(..., isPipelineOnly=true) 尝试把 4 个 task 全部 pipeline 到节点
  -> pendingJob.IsGangSatisfied() 为 true 才返回成功
  -> action Commit，victims 进入 eviction/pipeline，preemptor 进入 pipeline
```

这里的“gang-aware”不是说每一步都不看单个 pod，而是最终提交前必须证明这 4 个 pod 作为一个 PodGroup 当前要求的 gang 集合能够整体被推进。单个 pod 成功不够。

## 与 Volcano task-by-task preemption 的关键差异

从源码形态上看，KAI solver 可以借鉴的点是：

- preemptor 是 `PodGroupInfo`，不是单个 task。
- `GetTasksToAllocate()` 先确定当前必须整体推进的 task 集合。
- victim scenario 是成组搜索，不是某个 pod 找不到节点时局部抢占。
- `Statement` 支持先虚拟驱逐，再完整调度，再整体 commit。
- victim side 也通过 `GetTasksToEvict()` 保留 elastic/gang 语义。
- solution validator 能把 queue fairness、minruntime 等策略放在最终 scenario 上判断。

如果要把 Volcano 改成 gang preemption，仅在 task-by-task preemption 外面套一个 PodGroup 检查是不够的。关键是要引入类似的事务化 scenario：先选出整个 gang 的待调度 tasks，模拟一组 victims，只有整个 gang 都能放下才提交。

## 常见调试入口

| 现象 | 先看哪里 | 可能原因 |
| --- | --- | --- |
| solver 根本没开始 | action 的 pending queue 初始化和前置 filter | job 不 ready、没有 pending task、queue 非 leaf、reclaim 不符合 fairness |
| `tasksToAllocate` 比预期少 | `allocation_info.go` | subgroup 已满足，进入 elastic phase，一次只推进一个 child 或一个 task |
| victim 没有整组被抢 | `eviction_info.go` | victim 是 elastic，solver 优先剥离 surplus |
| victim 被整组抢 | `collectAllAllocatedTasksFromSubGroupSet()` | 没有 elastic surplus，fallback 到 full gang/subtree eviction |
| scenario 被快速过滤 | accumulated filters | node affinity、topology 或 idle GPU 粗筛认为无解 |
| scenario 模拟失败 | `by_pod_solver.go` 和 `common/allocate.go` | predicate、capacity、topology、GPU sharing、DRA、MIG 等任一环失败 |
| scenario 找到但 validator 拒绝 | `minruntime` 或 `proportion` | victim 仍受保护，或跨队列 reclaim 不公平 |
| 试算看似成功但没有提交 | `Statement.Commit()` 调用方 | action 未收到 `succeeded=true`，或 commit 失败 |

## 建议测试阅读

| 测试 | 能验证什么 |
| --- | --- |
| [`pkg/scheduler/actions/preempt/preemptGang_test.go`](../../../pkg/scheduler/actions/preempt/preemptGang_test.go) | preempt 对 gang job 的基本行为 |
| [`pkg/scheduler/actions/preempt/preempt_subgroups_test.go`](../../../pkg/scheduler/actions/preempt/preempt_subgroups_test.go) | subgroup 语义下的抢占 |
| [`pkg/scheduler/actions/preempt/preempt_gang_full_drain_test.go`](../../../pkg/scheduler/actions/preempt/preempt_gang_full_drain_test.go) | 多节点 gang victim/full drain 相关边界 |
| [`pkg/scheduler/actions/reclaim/reclaimGang_test.go`](../../../pkg/scheduler/actions/reclaim/reclaimGang_test.go) | reclaim 场景里的 gang victim |
| [`pkg/scheduler/actions/consolidation/consolidation_subgroups_test.go`](../../../pkg/scheduler/actions/consolidation/consolidation_subgroups_test.go) | consolidation 与 subgroup 重排 |
| [`pkg/scheduler/actions/common/solvers/pod_scenario_builder_test.go`](../../../pkg/scheduler/actions/common/solvers/pod_scenario_builder_test.go) | scenario 累计、recorded victims、elastic victim 剩余任务回队列 |
| [`pkg/scheduler/actions/common/solvers/scenario/base_scenario_test.go`](../../../pkg/scheduler/actions/common/solvers/scenario/base_scenario_test.go) | scenario 数据结构和 victim representative |
| [`pkg/scheduler/actions/common/solvers/accumulated_scenario_filters/idle_gpus/idle_gpus_test.go`](../../../pkg/scheduler/actions/common/solvers/accumulated_scenario_filters/idle_gpus/idle_gpus_test.go) | GPU 粗筛 |
| [`pkg/scheduler/framework/statement_checkpoint_test.go`](../../../pkg/scheduler/framework/statement_checkpoint_test.go) | checkpoint、rollback、undo 操作对 session 状态的影响 |

## 阅读顺序建议

第一次读 solver，可以按这个顺序打开文件：

```text
preempt.go / reclaim.go / consolidation.go
  -> job_solver.go
     -> allocation_info.go
     -> pod_scenario_builder.go
        -> eviction_info.go
        -> sub_scenario_emitter.go
        -> scenario/base_scenario.go
     -> by_pod_solver.go
        -> action.go
        -> common/allocate.go
        -> framework/statement.go
     -> minruntime.go / proportion.go
```

这样读下来，会先知道 action 为什么调用 solver，再理解 solver 怎样生成 scenario，最后看到一个 scenario 如何在完整调度流程里被证明可行或失败。
