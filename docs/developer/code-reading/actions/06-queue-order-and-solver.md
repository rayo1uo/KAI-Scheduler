# 06. Queue Order 与 Victim Solver

这一章解释多个 action 共用的底层机制：`JobsOrderByQueues` 如何把多级 Queue 树变成一个可 pop 的 job 队列，以及 `JobsSolver` 如何为 `reclaim/preempt/consolidation` 搜索 victim scenario。

## `JobsOrderByQueues` 的职责

入口：[`pkg/scheduler/actions/utils/job_order_by_queue.go`](../../../../pkg/scheduler/actions/utils/job_order_by_queue.go)

它维护一棵 `queueNode` 树：

```text
rootNodes
  -> non-leaf queueNode
       -> non-leaf queueNode
            -> leaf queueNode
                 -> PodGroupInfo priority queue
```

leaf queue 的 children 是 job，非 leaf queue 的 children 是 queue node。`PopNextJob()` 会从 root 一路向下选择最佳 child，直到 leaf，再弹出 leaf queue 内的最佳 job。

## 初始化过滤

`InitializeWithJobs` 的过滤逻辑在 [`pkg/scheduler/actions/utils/input_jobs.go`](../../../../pkg/scheduler/actions/utils/input_jobs.go)。不同 action 通过 `JobsOrderInitOptions` 启用不同过滤：

| 选项 | 含义 | 常见 action |
| --- | --- | --- |
| `FilterNonPending` | 排除非 pending job | allocate/reclaim/preempt/consolidation |
| `FilterUnready` | 排除 PodGroup 未 ready 的 job | allocate/reclaim/preempt/consolidation |
| `FilterNonPreemptible` | 排除不可抢占 job | consolidation 和 victim queue |
| `FilterNonActiveAllocated` | 排除没有 active allocated task 的 job | victim queue |
| `VictimQueue` | 反转排序，并记录已 pop victims | reclaim/preempt/consolidation solver |
| `MaxJobsQueueDepth` | 限制每个 leaf queue 的候选 job 深度 | pending job queue |

此外，`InitializeWithJobs` 还有三条所有 action 都会经过的隐式过滤：

- job 所属 queue 不存在，跳过。
- queue 有 parent，但 parent queue 不存在，跳过。
- job 所属 queue 不是 leaf queue，跳过。只有 leaf queue 可以直接承载 PodGroup。

这三条不是 action 参数控制的，而是 Queue hierarchy 的基本约束。读“为什么 job 没进入 action”时，要先排除这里的结构性过滤，再看 action 自己的 `FilterNonPending`、`FilterUnready` 等选项。

## Pending queue 与 victim queue 的过滤矩阵

`JobsOrderInitOptions` 只描述 priority queue 初始化时的通用过滤。preempt/reclaim/consolidation 还会先构造 action 自己的 victim filter，再交给 `InitializeWithJobs`。把两层分开看会更清楚：

| 场景 | `JobsOrderInitOptions` | action 自定义过滤 |
| --- | --- | --- |
| `allocate` pending job | `FilterNonPending`、`FilterUnready`、`MaxJobsQueueDepth=GetJobsDepth(Allocate)` | 无 victim filter |
| `reclaim` pending job | `FilterNonPending`、`FilterUnready`、`MaxJobsQueueDepth=GetJobsDepth(Reclaim)` | `CanReclaimResources`、scheduling signature、victim-invariant pre-predicate |
| `reclaim` victim job | `FilterNonPreemptible`、`FilterNonActiveAllocated`、`VictimQueue`、无限深度 | 排除 reclaimer 同 queue，且必须通过 `ReclaimVictimFilter` |
| `preempt` pending job | `FilterNonPending`、`FilterUnready`、`MaxJobsQueueDepth=GetJobsDepth(Preempt)` | quota check、scheduling signature、victim-invariant pre-predicate |
| `preempt` victim job | `VictimQueue`、无限深度 | preemptible、低 priority、同 queue、非自己、有 active allocated task、通过 `PreemptVictimFilter` |
| `consolidation` pending job | `FilterNonPending`、`FilterUnready`、`FilterNonPreemptible`、`MaxJobsQueueDepth=GetJobsDepth(Consolidation)` | GPU 总量粗筛、scheduling signature、victim-invariant pre-predicate |
| `consolidation` victim job | `VictimQueue`、无限深度 | preemptible、非自己、有 active allocated task、受 `MaxNumberConsolidationPreemptees` 当前源码判断限制 |

## Queue 与 Job 排序如何组合

leaf queue 内部：

```go
if jo.options.VictimQueue {
    return !jo.ssn.JobOrderFn(l, r)
}
return jo.ssn.JobOrderFn(l, r)
```

queue node 之间：

```go
lPending, lVictims := jo.getBestJobFromNode(lNode)
rPending, rVictims := jo.getBestJobFromNode(rNode)

result := jo.ssn.QueueOrderFn(lNode.queue, rNode.queue, lPending, rPending, lVictims, rVictims)
if reverseOrder {
    return !result
}
return result
```

这解释了为什么 `QueueOrderFn` 的参数既有 pending job，也可能有 victims：普通调度时比较哪个 queue 更应该拿资源；victim queue 时比较从哪个 queue 回收更合适。

## `PushJob` 为什么存在

`PushJob` 有两个常见用途：

- `allocate` 成功分配 job 的一部分 task 后，如果 job 还有可分配 task，把它推回队列继续竞争。
- solver 处理 elastic victim 时，可能只拿 victim job 的一部分 task 作为 potential victims，剩余 task clone 成新 job 后推回 victim queue。

这让调度不是“一次 pop 处理完整 job 到底”，而是能在 queue fairness、elastic job 和 gang/subgroup 之间保持更细粒度的轮转。

## PodGroup 分配与驱逐的 task 粒度

solver 和 allocate 都依赖 PodGroup 层的 task 选择函数。

分配入口是 [`pkg/scheduler/api/podgroup_info/allocation_info.go`](../../../../pkg/scheduler/api/podgroup_info/allocation_info.go)：

```text
GetTasksToAllocate(job)
  -> collectTasksFromSubGroupSet(root)
       -> 未满足 min requirement:
            从排序后的前 K 个 child 收集 task，K = GetMinMembersToSatisfy()
       -> 已满足 min requirement:
            elastic phase 只从最优的未满足 child 收集一批 task
       -> PodSet 内按 TaskOrderFn 排序
```

驱逐入口是 [`pkg/scheduler/api/podgroup_info/eviction_info.go`](../../../../pkg/scheduler/api/podgroup_info/eviction_info.go)：

```text
GetTasksToEvict(victimJob)
  -> reverse SubGroupOrderFn / TaskOrderFn
  -> Phase 1: elastic recursive，优先找更深层 surplus
  -> Phase 2: elastic direct，父 subgroup 有 surplus child 时驱逐低优先 child
  -> Phase 3: fallback full gang eviction，收集整个 subtree 的 active allocated tasks
```

因此 victim solver 不一定一上来就整组驱逐。对 elastic job，它会先尝试只拿 surplus task；只有没有可用 elastic surplus 时，才回退到完整 gang/subtree 驱逐。

## `JobsSolver` 的整体思路

入口：[`pkg/scheduler/actions/common/solvers/job_solver.go`](../../../../pkg/scheduler/actions/common/solvers/job_solver.go)

`reclaim/preempt/consolidation` 都用它。核心伪代码：

```text
Solve(pendingJob)
  -> tasksToAllocate := GetTasksToAllocate(...)
  -> searchMaxSolvableK(...)
       -> tryProbeAndDiscard(k)
            -> probeAtK(k)
                 -> getPartialJobRepresentative(...)
                 -> solvePartialJob(...)
                      -> NewPodAccumulatedScenarioBuilder(...)
                      -> for scenario in scenarios
                           -> byPodSolver.solve(...)
  -> probeAtK(full n)
  -> 返回 live statement
```

`searchMaxSolvableK` 先用指数增长加二分，找到“最多能解决多少个 pending task”。搜索阶段成功也会 `statement.Discard()`，保证 session 不被探测污染。最后再用完整 task 数量做一次 probe，返回可 commit 的 live statement。

成功和失败边界很严格：

- `GetTasksToAllocate` 返回空时直接失败。
- `searchMaxSolvableK` 如果连 1 个 pending task 都无法解决，失败。
- 搜索阶段找到 partial 成功还不够，最后必须对完整 pending task 数 `n` 再 `probeAtK(n)` 成功。
- `byPodSolver.solve` 要求 scenario 至少有一个 victim；没有 victim 时直接失败。
- 最后还要求 pending job `IsGangSatisfied()`，并且本次模拟后的 active used task 数必须比进入 solver 前增加。

这些边界保证 reclaim/preempt/consolidation 不会为了“看起来释放了一点资源”就提交。必须证明目标 job 的完整 gang/subgroup 语义能被推进。

## 为什么要 partial job representative

`getPartialJobRepresentative` 会 clone job，只保留：

- 已经 allocated 的 task。
- 前 K 个 pending task。

然后调整 subgroup 的 `minAvailable` 和 `minSubGroup`。原因是 solver 在探测 K 个 task 时，不能仍然要求原始 job 的完整 gang 数量，否则 partial 探测会被错误判定失败。

## Scenario builder 如何累计 victims

入口：[`pkg/scheduler/actions/common/solvers/pod_scenario_builder.go`](../../../../pkg/scheduler/actions/common/solvers/pod_scenario_builder.go)

流程：

```text
NewPodAccumulatedScenarioBuilder
  -> 初始化 scenario：pending tasks + recorded victims
  -> 初始化 filters：
       NodeAffinitiesFilter
       TopologyAwareIdleGpusFilter
       IdleGpusFilter

GetValidScenario / GetNextScenario
  -> 如果 subEmitter 有子场景，先吐子场景
  -> 否则从 victim queue PopNextJob
  -> GetTasksToEvict(nextVictimJob)
  -> elastic/remaining tasks 可能 PushJob 回 victim queue
  -> outerScenarioValid(filters)
  -> newSubScenarioEmitter(...)
```

三个 accumulated filter 是轻量剪枝：

- `NodeAffinitiesFilter`：如果 pending pod 有 required node affinity/nodeSelector，而可用节点加 victim 节点仍无法满足，跳过 scenario。
- `TopologyAwareIdleGpusFilter`：按 required topology domain 做 GPU 容量粗筛。
- `IdleGpusFilter`：按集群 idle/releasing + victims freed GPU 做容量粗筛。

这些 filter 不负责最终正确性，只负责少跑一些昂贵的完整模拟。

## `byPodSolver` 如何模拟

入口：[`pkg/scheduler/actions/common/solvers/by_pod_solver.go`](../../../../pkg/scheduler/actions/common/solvers/by_pod_solver.go)

核心流程：

```text
byPodSolver.solve
  -> statement := session.Statement()
  -> allVictims := recordedVictims + potentialVictims
  -> checkpoint := statement.Checkpoint()
  -> common.EvictAllPreemptees(...)
  -> update feasible nodes with victim nodes
  -> tryScenarioWithEvictedVictims(...)
       -> common.GetJobsToAllocate(...)
       -> common.TryToVirtuallyAllocatePreemptorAndGetVictims(...)
            -> common.AllocateJob(..., isPipelineOnly=true)
  -> solutionValidator(scenario)
  -> 成功返回 statement，失败 rollback/discard
```

这里最关键的设计是：solver 不只看释放出来的资源数量，而是真的把 victims 标成 `Releasing`，再跑完整 `AllocateJob`。因此 predicates、capacity、topology、GPU sharing、gang/subgroup 都会参与判断。

## Statement 的虚拟副作用

`Statement` 不是只在内存里记一串操作，`Allocate/Pipeline/Evict` 调用当下就会修改 session 视图：

| 操作 | 立即修改 | 立即触发的 plugin event |
| --- | --- | --- |
| `stmt.Allocate` | task 变 `Allocated`，写入 node，扣减 node idle/used 视图 | `AllocateFunc` |
| `stmt.Pipeline` | task 变 `Pipelined`，写入目标 node，表示依赖 releasing 资源 | `AllocateFunc` |
| `stmt.Evict` | victim task 变 `Releasing`，node/job 资源视图更新 | `DeallocateFunc` |
| `Rollback/Discard` | 反向撤销上述 session 变更 | 触发相反方向的 event handler |
| `Commit` | 把仍然有效的操作外部化到 cache | `BindRequest`、`TaskPipelined`、`Evict` |

这也是为什么 solver 的 probe 必须 `Discard()`：如果不撤销，下一次 scenario 会在已经被污染的 queue resource、DRA claim、node resource 视图上继续试算。

## `allowVictimConsolidation`

`byPodSolver.handleScenarioSolution` 会区分两类 victim：

- `preemptedVictims`：最终仍是 `Releasing`。
- `pipelinedVictims`：被 solver 找到新位置，最终变成 `Pipelined`。

如果 `AllowConsolidatingReclaim` 开启，pipelined victim 可以先不算作最终被驱逐的 victim；关闭时，pipelined victims 也会被纳入 victim 列表。这个参数会影响 reclaim/preempt 场景下是否允许顺手重新安置 victim。

## 建议测试阅读

- [`pkg/scheduler/actions/utils/job_order_by_queue_test.go`](../../../../pkg/scheduler/actions/utils/job_order_by_queue_test.go)：Queue 树排序和 victim reverse order。
- [`pkg/scheduler/actions/common/solvers/pod_scenario_builder_test.go`](../../../../pkg/scheduler/actions/common/solvers/pod_scenario_builder_test.go)：scenario 累积、recorded victims、elastic victims。
- [`pkg/scheduler/actions/common/solvers/scenario/base_scenario_test.go`](../../../../pkg/scheduler/actions/common/solvers/scenario/base_scenario_test.go)：scenario 数据结构。
- [`pkg/scheduler/actions/common/solvers/accumulated_scenario_filters/idle_gpus/idle_gpus_test.go`](../../../../pkg/scheduler/actions/common/solvers/accumulated_scenario_filters/idle_gpus/idle_gpus_test.go)：GPU 粗筛。
- [`pkg/scheduler/framework/statement_test.go`](../../../../pkg/scheduler/framework/statement_test.go)：Statement commit/rollback 的基本语义。
- [`pkg/scheduler/framework/statement_checkpoint_test.go`](../../../../pkg/scheduler/framework/statement_checkpoint_test.go)：checkpoint/rollback 对多操作的影响。
- [`pkg/scheduler/actions/common/minimal_job_comparison_test.go`](../../../../pkg/scheduler/actions/common/minimal_job_comparison_test.go)：scheduling signature 剪枝。
