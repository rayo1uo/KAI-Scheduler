# 10. PodGroup、SubGroup 与 Queue 概念

这一章专门解释 KAI Scheduler 里最容易和 kube-batch 传统语义混淆的两组概念：`PodGroup/SubGroup` 和 `Queue`。建议在读 [`04. Actions`](04-actions.md) 和 [`05. Plugins`](05-plugins.md) 前先看本章，因为 allocate、reclaim、preempt、solver、proportion 都会把这些概念展开成具体调度行为。

## 本章先读哪些源码

| 主题 | 源码入口 | 读什么 |
| --- | --- | --- |
| PodGroup CRD | [`pkg/apis/scheduling/v2alpha2/podgroup_types.go`](../../../pkg/apis/scheduling/v2alpha2/podgroup_types.go) | `minMember`、`minSubGroup`、`subGroups`、`preemptibility`、topology、backoff |
| PodGroup webhook | [`pkg/apis/scheduling/v2alpha2/podgroup_webhook.go`](../../../pkg/apis/scheduling/v2alpha2/podgroup_webhook.go) | subgroup tree 的结构校验和兼容性 warning |
| Preemptibility 推导 | [`pkg/common/podgroup/preemptible.go`](../../../pkg/common/podgroup/preemptible.go) | 空 `preemptibility` 如何按 priority 转成 preemptible/non-preemptible |
| PodGroup 内部模型 | [`pkg/scheduler/api/podgroup_info/job_info.go`](../../../pkg/scheduler/api/podgroup_info/job_info.go) | `PodGroupInfo` 如何索引 task、subgroup、fit error、stale 状态 |
| PodGroup 状态反馈 | [`pkg/scheduler/cache/status_updater/default_status_updater.go`](../../../pkg/scheduler/cache/status_updater/default_status_updater.go) | unschedulable、not-ready、stale、last-start 如何写回事件/status/annotation |
| PodGroup backoff 工具 | [`pkg/scheduler/utils/pod_group_utils.go`](../../../pkg/scheduler/utils/pod_group_utils.go) | `schedulingBackoff` 和 `markUnschedulable` 默认值 |
| SubGroup tree | [`pkg/scheduler/api/podgroup_info/subgroup_info`](../../../pkg/scheduler/api/podgroup_info/subgroup_info) | CRD `SubGroup` 如何变成 `SubGroupSet` 和 `PodSet` |
| 分配粒度 | [`pkg/scheduler/api/podgroup_info/allocation_info.go`](../../../pkg/scheduler/api/podgroup_info/allocation_info.go) | `GetTasksToAllocate()` 如何按 subgroup tree 选择下一批 task |
| 驱逐粒度 | [`pkg/scheduler/api/podgroup_info/eviction_info.go`](../../../pkg/scheduler/api/podgroup_info/eviction_info.go) | `GetTasksToEvict()` 如何先驱逐 elastic surplus，再回退整组驱逐 |
| Queue CRD | [`pkg/apis/scheduling/v2/queue_types.go`](../../../pkg/apis/scheduling/v2/queue_types.go) | queue hierarchy、quota、limit、priority、runtime policy |
| Queue admission | [`pkg/admission/webhook/queuehooks/queue_validator.go`](../../../pkg/admission/webhook/queuehooks/queue_validator.go) | Queue resources 必填、父子 quota warning、删除保护 |
| Queue 内部模型 | [`pkg/scheduler/api/queue_info/queue_info.go`](../../../pkg/scheduler/api/queue_info/queue_info.go) | `QueueInfo` 只从 spec/metadata 构造调度输入 |
| Queue snapshot | [`pkg/scheduler/cache/cluster_info/queue.go`](../../../pkg/scheduler/cache/cluster_info/queue.go) | 队列树构造、orphan queue 清理、fairness level |
| Queue action 入口 | [`pkg/scheduler/actions/utils/job_order_by_queue.go`](../../../pkg/scheduler/actions/utils/job_order_by_queue.go) | `JobsOrderByQueues` 如何遍历多级 Queue 树 |
| 公平性插件 | [`pkg/scheduler/plugins/proportion/proportion.go`](../../../pkg/scheduler/plugins/proportion/proportion.go) | fair share、capacity、reclaim qualification |
| 最小运行时间插件 | [`pkg/scheduler/plugins/minruntime`](../../../pkg/scheduler/plugins/minruntime) | Queue 层级如何决定 preempt/reclaim 保护时间 |

## 和 kube-batch 传统语义的差异

如果带着 kube-batch 的扁平 PodGroup/Queue 模型来读 KAI，最容易漏掉的是“树”这个维度。

| 维度 | 传统阅读习惯 | KAI 源码里的实际语义 | 调度影响 |
| --- | --- | --- | --- |
| PodGroup | 一个 workload 的 gang 单元，核心是 `minMember` | 仍是 workload 调度单位，但可以声明 `minMember` 或 `minSubGroup`，还可以包含多级 `SubGroups` | action 不再简单调度前 N 个 pending pod，而是按 subgroup tree 决定下一批 task |
| SubGroup | kube-batch 扁平模型里通常没有这个层次 | KAI 把叶子 SubGroup 建成 `PodSet`，把中间 SubGroup 建成 `SubGroupSet` | readiness、allocate、evict、topology signature 都会按树递归 |
| `minMember` | PodGroup 级最小 pod 数 | PodGroup 无 SubGroup 时仍是扁平 gang；叶子 SubGroup 上表示该叶子需要的最小 pod 数 | `PodSet.IsReadyForScheduling()` 和 `PodSet.IsGangSatisfied()` 使用它 |
| `minSubGroup` | 传统模型里没有 | PodGroup 或中间 SubGroup 直接子节点中至少满足多少个 | `SubGroupSet.GetMinMembersToSatisfy()` 决定 ready 和 gang phase 的 child 数 |
| Queue | 一组 job 的队列 | KAI Queue 是可多级嵌套的资源策略树，有 quota/limit/over-quota weight、priority、runtime policy | `JobsOrderByQueues` 只让 leaf queue 承载 PodGroup，并沿队列树比较公平性 |
| Queue status | 容易误读成 scheduler 的实时输入 | `status` 主要是 QueueController 写出的观测面；scheduler 主要读 Queue spec，并在 session 里重新统计 usage | 排查公平性时要读 `proportion` 的 session 内计算，而不是只看 Queue status |
| 抢占/回收 | 只看 priority 或 queue quota | KAI 同时看 queue 层级、公平性、preemptibility、min runtime、subgroup elastic surplus | victim solver 会用完整 `AllocateJob()` 试算，而不是只做资源加减 |
| 拓扑 | 常见是 pod/node 约束 | PodGroup 和 SubGroup 都可以带 topology constraint | topology plugin 可以按 subgroup 选择 node subset；solver 剪枝也会读取 topology signature |

## PodGroup CRD

PodGroup 的类型定义在 [`pkg/apis/scheduling/v2alpha2/podgroup_types.go`](../../../pkg/apis/scheduling/v2alpha2/podgroup_types.go)。核心字段可以按三组理解。

第一组是 gang 门槛：

```go
type PodGroupSpec struct {
    MinMember *int32
    MinSubGroup *int32
    SubGroups []SubGroup
}
```

`MinMember` 是扁平 gang 语义：没有 SubGroup 时，默认 `PodSet` 的 `minAvailable` 会从它来。`MinSubGroup` 是层级 gang 语义：它不数 pod，而是数根节点下“直接 child SubGroup 中已经满足自身门槛的数量”。两者互斥。

第二组是调度上下文：

```go
type PodGroupSpec struct {
    Queue string
    PriorityClassName string
    Preemptibility Preemptibility
}
```

`Queue` 决定这个 PodGroup 进入哪棵 Queue 树。queue 不存在、parent 不存在、或者目标 queue 不是 leaf queue 时，`InitializeWithJobs()` 会直接跳过这个 job。`PriorityClassName` 会转换成内部 `PodGroupInfo.Priority`，影响 job order 和 preempt。`Preemptibility` 显式控制该 PodGroup 是否可以当 victim；为空时由 [`CalculatePreemptibility()`](../../../pkg/common/podgroup/preemptible.go) 按 priority 推导，当前阈值是 100：priority 小于 100 视为 `preemptible`，否则视为 `non-preemptible`。

第三组是调度反馈和特殊策略：

```go
type PodGroupSpec struct {
    TopologyConstraint TopologyConstraint
    MarkUnschedulable *bool
    SchedulingBackoff *int32
}
```

`TopologyConstraint` 会进入 `SubGroupSet` 根节点的 topology signature。`MarkUnschedulable` 和 `SchedulingBackoff` 更偏状态反馈和 backoff 策略，读 status updater、podgroup controller 和 unschedulable event 时再展开。

## MarkUnschedulable 与 SchedulingBackoff

这两个字段不是 kube-batch 传统 gang 语义的一部分，但会影响“失败后怎么反馈”和“下一轮还进不进当前 scheduler/node pool”。

默认值在 [`pkg/scheduler/utils/pod_group_utils.go`](../../../pkg/scheduler/utils/pod_group_utils.go)：

```text
DefaultMarkUnschedulable = true
DefaultSchedulingBackoff = -1

NoSchedulingBackoff = -1
SingleSchedulingBackoff = 1
```

`MarkUnschedulable` 主要在 [`recordUnschedulablePodsEvents()`](../../../pkg/scheduler/cache/status_updater/default_status_updater.go) 使用。scheduler 仍会给 Pod 记录 `Unschedulable` event；该字段控制是否同时更新 Pod 的 `PodScheduled=False/Unschedulable` condition。非法 SubGroup 的 Pod 会强制更新 condition，因为它是确定的配置错误。

`SchedulingBackoff` 的路径分成写和读两段：

```text
CloseSession()
  -> Cache.RecordJobStatusEvent(job)
  -> recordUnschedulablePodGroup()
  -> markPodGroupUnschedulable()
  -> 写 PodGroup.status.schedulingConditions[type=UnschedulableOnNodePool]

下一轮 Snapshot
  -> snapshotPodGroups()
  -> filterUnassignedPodGroups()
  -> isPodGroupUpForScheduler()
       -> schedulingBackoff == -1 时继续纳入 snapshot
       -> 否则如果最新 scheduling condition 属于当前 node pool，就跳过该 PodGroup
```

源码注释当前只声明支持 `-1` 和 `1`。读行为时可以这样记：`-1` 表示不因为失败 condition 做 backoff；`1` 表示当前 node pool 失败一次后，最新 `UnschedulableOnNodePool` condition 会让下一轮 snapshot 跳过该 PodGroup。`addNodePoolPrefixIfNeeded()` 还会在 `1` 场景下给错误信息加上 node pool 前缀，方便多 node pool 排障。

## SubGroup CRD 与校验

SubGroup 是 KAI 对 PodGroup 最重要的扩展：

```go
type SubGroup struct {
    Name string
    MinMember *int32
    MinSubGroup *int32
    Parent *string
    TopologyConstraint *TopologyConstraint
}
```

读这个结构时要先判断它是叶子还是中间节点：

- 叶子 SubGroup 没有 child，内部会变成 `PodSet`，应该使用 `minMember`。
- 中间 SubGroup 有 child，内部会变成 `SubGroupSet`，不能使用 `minMember`，可以使用 `minSubGroup`。
- `Parent == nil` 表示它是 PodGroup root 的直接 child。
- Pod 的 subgroup 归属不写在 PodGroup 里，而是由 Pod label 指向 leaf SubGroup。

webhook 校验在 [`pkg/apis/scheduling/v2alpha2/podgroup_webhook.go`](../../../pkg/apis/scheduling/v2alpha2/podgroup_webhook.go)。它先校验顶层 `minMember/minSubGroup` 互斥，再校验 SubGroup 图：

- SubGroup 名字不能重复。
- `parent` 必须引用存在的 SubGroup。
- parent/child 不能形成环。
- 顶层 `minSubGroup` 必须大于等于 1；如果超过 root 直接 child 数量，当前 create/update 会作为 warning 处理。
- 叶子 SubGroup 不能设置 `minSubGroup`，且必须设置 `minMember`。
- 中间 SubGroup 不能设置 `minMember`，应使用 `minSubGroup` 或省略以表示要求全部直接 children。
- SubGroup 级 `minSubGroup` 不能超过直接 child 数量；当前 create/update 会把这类问题作为 warning 处理，用于兼容某些升级场景。
- update 场景对部分历史形态更宽容，例如中间节点已有 `minMember` 时会 warning，而不是直接拒绝。

这些校验规则很重要，因为 scheduler 内部假设 SubGroup tree 是合法的。真的读 action 时，通常不需要再找一遍循环检测代码，先记住 webhook 已经把“树结构”清洗过。

## 从 CRD 到内部模型

PodGroup snapshot 后会变成 [`PodGroupInfo`](../../../pkg/scheduler/api/podgroup_info/job_info.go)：

```go
type PodGroupInfo struct {
    Queue common_info.QueueID
    Priority int32
    Preemptibility enginev2alpha2.Preemptibility

    RootSubGroupSet *subgroup_info.SubGroupSet
    PodSets map[string]*subgroup_info.PodSet
    InvalidSubGroupTasks pod_info.PodsMap

    PodStatusIndex map[pod_status.PodStatus]pod_info.PodsMap
    AllocatedVector resource_info.ResourceVector
    JobFitErrors []common_info.JobFitError
    TasksFitErrors map[common_info.PodID]*common_info.TasksFitErrors
}
```

`SetPodGroup()` 会调用 `setSubGroups()`，再进入 [`subgroup_info.FromPodGroup()`](../../../pkg/scheduler/api/podgroup_info/subgroup_info/factory.go)。转换规则是：

```text
PodGroup
  -> root SubGroupSet(name="")
       -> child SubGroup with children
            -> SubGroupSet
       -> child SubGroup without children
            -> PodSet
```

没有 `SubGroups` 时，会保留一个默认 `PodSet(default)`。它的 `minAvailable` 默认是 1；如果 PodGroup 设置了 `spec.minMember`，就用 `max(minMember, 1)`。这就是向 kube-batch 扁平 gang 行为兼容的路径。

`AddTaskInfo()` 把 Pod 放入 PodSet：

```text
PodInfo.SubGroupName 为空
  -> 放入 default PodSet

PodInfo.SubGroupName 非空且存在
  -> 放入同名 PodSet

PodInfo.SubGroupName 非空但不存在
  -> 放入 InvalidSubGroupTasks
  -> 写 TasksFitErrors
```

因此排查“Pod 明明有 PodGroup 但不调度”时，需要同时看 PodGroup CRD 中是否定义了对应 leaf SubGroup，以及 Pod 上的 subgroup label 是否拼写一致。

## `SubGroupSet` 与 `PodSet`

[`SubGroupSet`](../../../pkg/scheduler/api/podgroup_info/subgroup_info/subgroupset.go) 是中间节点。它只关心直接 child：

```text
SubGroupSet
  -> direct SubGroupSets
  -> direct PodSets
```

它有三个核心判断：

- `GetMinMembersToSatisfy()`：如果 `minSubGroup` 非空，返回它；否则返回直接 child 总数。
- `IsReadyForScheduling()`：直接 children 中 ready 的数量达到 `GetMinMembersToSatisfy()`。
- `IsMinRequirementSatisfied()`：直接 children 中 active allocated 满足的数量达到 `GetMinMembersToSatisfy()`。

[`PodSet`](../../../pkg/scheduler/api/podgroup_info/subgroup_info/podset.go) 是叶子节点。它的核心判断是：

- `IsReadyForScheduling()`：alive task 数减 gated task 数，至少达到 `minAvailable`。
- `IsGangSatisfied()`：active used task 数至少达到 `minAvailable`。
- `IsElastic()`：`minAvailable < pod 数`，说明有超过最小门槛的弹性 task。

这里有一个源码阅读提醒：`PodGroupInfo.IsReadyForScheduling()` 走 root `SubGroupSet`，能表达 `minSubGroup` 的层级 ready 语义；而 `PodGroupInfo.IsGangSatisfied()` 当前遍历所有 `PodSet` 的 `IsGangSatisfied()`。solver 做 partial 探测时会 clone job 并调整 `minAvailable/minSubGroup`，避免 partial job 被原始完整门槛卡死。不要只看函数名推断它一定等同于 root `minSubGroup`。

## SubGroup 如何影响 allocate

`allocate` 初始化候选 job 时会启用 `FilterUnready`。这个过滤最终调用：

```text
JobsOrderByQueues.InitializeWithJobs()
  -> job.IsReadyForScheduling()
  -> job.RootSubGroupSet.IsReadyForScheduling()
```

这一步发生在 action 尝试节点之前。也就是说，如果 PodGroup 的 root 或某个必须的 child SubGroup 没有足够 alive pods，后续 predicate、node order、GPU order 都不会运行。

真正选择“这次要调度哪些 task”的入口是 [`GetTasksToAllocate()`](../../../pkg/scheduler/api/podgroup_info/allocation_info.go)：

```text
GetTasksToAllocate(job)
  -> collectTasksFromSubGroupSet(root)
       -> children 按 SubGroupOrderFn 排序
       -> gang phase: parent 还没满足 min requirement
            从排序后的前 K 个 child 收集 task
            K = GetMinMembersToSatisfy()
       -> elastic phase: parent 已满足 min requirement
            只从第一个还有待分配 task 的 child 收集
       -> PodSet 内按 TaskOrderFn 排序
```

到 `PodSet` 层还有一个关键限制：

- 未达到 `minAvailable` 时，收集足够补齐最小 gang 的 task。
- 已达到 `minAvailable` 时，elastic 阶段一次最多收集 1 个 task。

这解释了 [`actions/01-allocate.md`](actions/01-allocate.md) 里 `allocate` 成功后会 `PushJob(job)` 的原因。一个层级 PodGroup 即使还有很多 pending pods，也可能在本轮只推进一个 leaf PodSet 的 gang 缺口，或只推进一个 elastic task，然后重新回到 leaf queue 里和其他 job 竞争。

## SubGroup 如何影响 topology

PodGroup 和 SubGroup 都能声明 topology constraint。转换到内部模型后：

- PodGroup 的 topology 放在 root `SubGroupSet` 上。
- 中间 SubGroup 的 topology 放在对应 `SubGroupSet` 上。
- 叶子 SubGroup 的 topology 放在对应 `PodSet` 上。

`PodSet.GetSchedulingConstraintsSignature()` 会把自身 topology、父链 topology，以及非 active-allocated pod 的 scheduling signature 一起 hash。这个 signature 被 solver 和 accumulated scenario filter 使用，用于判断某些 pending job/victim 组合是否值得继续完整模拟。

调度时，公共分配链路会调用 topology plugin 注册的 `SubsetNodesFn`：

```text
AllocateJob
  -> allocateSubGroupSet(root)
       -> ssn.SubsetNodesFn(...)
       -> allocateMembersOnNodes(...)
```

所以 topology 不是一个“最后打分”的附加条件，而是在 subgroup tree 的递归分配过程中就会改变候选 node set。

## SubGroup 如何影响 reclaim/preempt/consolidation

`reclaim`、`preempt`、`consolidation` 都会进入 solver。solver 不是只算释放资源是否足够，而是把 victims 虚拟变成 `Releasing`，再跑完整 `AllocateJob()`：

```text
byPodSolver.solve()
  -> statement := session.Statement()
  -> EvictAllPreemptees(...)
  -> TryToVirtuallyAllocatePreemptorAndGetVictims(...)
       -> AllocateJob(..., isPipelineOnly=true)
```

因此 subgroup 的影响会完整进入 solver：

- pending job 通过 `GetTasksToAllocate()` 决定要证明哪一批 task 可调度。
- victim job 通过 `GetTasksToEvict()` 决定先释放哪些 task。
- topology、predicate、GPU sharing、queue capacity 都在虚拟分配中重新参与判断。
- 最终 solution validator 还会检查 scenario 是否真的推进了目标 job。

victim task 的选择在 [`GetTasksToEvict()`](../../../pkg/scheduler/api/podgroup_info/eviction_info.go)：

```text
GetTasksToEvict(victimJob)
  -> reverse SubGroupOrderFn / TaskOrderFn
  -> Phase 1: elastic recursive
       优先在更深层找 surplus task
  -> Phase 2: elastic direct
       parent 满足数超过 min requirement 时，整组驱逐低优先 child
  -> Phase 3: fallback full gang eviction
       收集整棵 subtree 的 active allocated tasks
```

这就是层级 SubGroup 对抢占/回收影响最大的地方：victim solver 会优先寻找 elastic surplus，而不是一开始就把整个 PodGroup 或整个 SubGroup 全部驱逐。

## SubGroup 与 stale gang eviction

`stalegangeviction` 的 stale 判断来自 `PodGroupInfo.IsStale()`：

```text
IsStale()
  -> 如果已有 succeeded task，返回 false
  -> 如果 active used task 总数为 0，返回 false
  -> 任一 PodSet 未满足 IsGangSatisfied()，返回 true
```

这个 action 不走 `Statement`，而是直接 `Session.Evict()` active allocated tasks。读 [`actions/05-stale-gang-eviction.md`](actions/05-stale-gang-eviction.md) 时，把它理解成“清理已经部分占资源但长期不能满足 gang 的 job”。SubGroup 让 stale 的判定更细，因为任何 leaf PodSet 的最小门槛没满足都可能让 job 被视为 stale。

## Queue CRD

Queue 类型在 [`pkg/apis/scheduling/v2/queue_types.go`](../../../pkg/apis/scheduling/v2/queue_types.go)：

```go
type QueueSpec struct {
    DisplayName string
    ParentQueue string
    Resources *QueueResources
    Priority *int
    PreemptMinRuntime *metav1.Duration
    ReclaimMinRuntime *metav1.Duration
}
```

`ParentQueue` 让 Queue 成为一棵树。`Resources` 定义每类资源的 `quota`、`limit` 和 `overQuotaWeight`：

```go
type QueueResource struct {
    Quota float64
    OverQuotaWeight float64
    Limit float64
}
```

`Priority` 是 queue 级策略，不等同于 PodGroup priority。高 priority queue 在 over-quota 资源分配中更靠前，在 reclaim victim 排序中更不容易被拿走资源。`PreemptMinRuntime` 和 `ReclaimMinRuntime` 是 disruptive action 的最小运行时间保护。

Queue status 是观测面：

```go
type QueueStatus struct {
    ChildQueues []string
    Allocated v1.ResourceList
    AllocatedNonPreemptible v1.ResourceList
    Requested v1.ResourceList
}
```

这些字段主要由 QueueController 聚合和写回，用于 `kubectl`、metrics、排障。scheduler 的公平性计算不是直接读取这些 status 数字，而是在每轮 session 里重新基于 Queue spec、PodGroupInfo 和 task status 计算。

Queue admission 还有一层保护在 [`pkg/admission/webhook/queuehooks/queue_validator.go`](../../../pkg/admission/webhook/queuehooks/queue_validator.go)：

- create/update 时 `spec.resources` 必填。
- 开启 quota validation 后，child queue 的 quota 超过 parent quota、或所有 children quota 总和超过 parent quota，会返回 admission warning。
- 删除有 `status.childQueues` 的 Queue 会被拒绝。

这些检查不等同于 scheduler 的实时 capacity check。admission 保护对象结构和明显 quota 配置问题；scheduler 的 `proportion`/capacity policy 会在每轮 session 内继续按当前 usage 判断 job 是否能调度。

## QueueInfo 与队列树

Scheduler snapshot 中，Queue CR 会变成 [`QueueInfo`](../../../pkg/scheduler/api/queue_info/queue_info.go)：

```go
type QueueInfo struct {
    UID common_info.QueueID
    Name string
    ParentQueue common_info.QueueID
    ChildQueues []common_info.QueueID
    Resources QueueQuota
    Priority int
    CreationTimestamp metav1.Time
    PreemptMinRuntime *metav1.Duration
    ReclaimMinRuntime *metav1.Duration
}
```

`NewQueueInfo()` 只读 spec 和 metadata。注意两个细节：

- `UID` 使用 Queue CR 的 `metadata.name`，`Name` 可以被 `spec.displayName` 覆盖。
- `QueueInfo` 不读取 `Queue.status.requested/allocated` 作为公平性输入。

队列树构造在 [`pkg/scheduler/cache/cluster_info/queue.go`](../../../pkg/scheduler/cache/cluster_info/queue.go)：

```text
snapshotQueues()
  -> ListQueues()
  -> NewQueueInfo()
  -> UpdateQueueHierarchy()
       -> updateQueueChildren()
       -> cleanQueueOrphans()
```

`cleanQueueOrphans()` 会删除 parent 缺失的 queue 及其 children。后续 action 初始化时，还会再次过滤：

```text
InitializeWithJobs()
  -> job.Queue 不存在，跳过
  -> job.Queue 的 parent 不存在，跳过
  -> job.Queue 不是 leaf queue，跳过
```

所以 KAI 的 Queue tree 有一个强约束：只有 leaf queue 可以承载 PodGroup。非 leaf queue 是资源分配和公平性汇总节点。

## Fairness level

`snapshotQueues()` 还会根据 fairness level 改变队列树形态：

- `FullFairness`：读取所有 Queue CR，并保留它们声明的 parent/child 关系。
- `ProjectLevelFairness`：创建一个内部默认 parent queue `default`；只保留有 parent 的 queue，并把它们的 parent 重写成 `default`。

这意味着同一批 Queue CR 在不同 fairness level 下，进入 `proportion` 的树可能不同。排查 queue order 或 reclaim 行为时，要先确认 scheduler 当前的 fairness level。

## Queue 如何影响 allocate

所有面向 pending job 的 action 都会先构造 `JobsOrderByQueues`：

```text
rootNodes
  -> non-leaf queueNode
       -> non-leaf queueNode
            -> leaf queueNode
                 -> PodGroupInfo priority queue
```

`PopNextJob()` 从 root 开始，一层一层选择最佳 child，直到 leaf queue，再弹出 leaf 内部的最佳 job。比较 queue node 时会调用 `Session.QueueOrderFn()`，比较 leaf 内 job 时会调用 `Session.JobOrderFn()`。

默认 queue order 主要由 `proportion` plugin 注册。它在 [`queue_order.GetQueueOrderResult()`](../../../pkg/scheduler/plugins/proportion/queue_order/queue_order.go) 里按这些维度比较：

1. 是否超过 fair share。
2. 加上当前 job 后是否仍在 deserved quota 内。
3. queue priority。
4. 是否有 0 allocatable share 但仍要使用资源。
5. 加上 pending job 或 victims 后的 dominant resource share。
6. 不带 task 的 dominant resource share。
7. allocatable share。
8. creation timestamp。

这套比较会用 `GetTasksToAllocateInitResourceVector()` 估算当前 job 的“下一批 task”资源量。也就是说，PodGroup/SubGroup 的 task 选择会反过来影响 Queue 排序。

## Queue 如何影响 capacity

`proportion.OnSessionOpen()` 会创建 queue attributes：

```text
calculateResourcesProportion()
  -> setTotalResources()
  -> createQueueResourceAttrs()
  -> updateQueuesCurrentResourceUsage()
  -> setFairShare()
```

`createQueueResourceAttrs()` 从 `QueueInfo.Resources` 读 quota、limit、over-quota weight。`updateQueuesCurrentResourceUsage()` 遍历当前 session 的 `PodGroupInfos`：

- allocated task 用 `AcceptedResourceVector` 计入 allocated 和 requested。
- pending task 用 `ResReqVector` 计入 requested。
- 每个 task 的资源都会沿 leaf queue 到 root 的 parent 链累计。

随后 `setFairShare()` 先计算 top-level queues 的 fair share，再递归把父 queue 的 fair share 作为 children 的 total resources 继续分配。capacity policy 会注册这些回调：

- `IsJobOverQueueCapacityFn`
- `IsTaskAllocationOnNodeOverCapacityFn`
- `IsNonPreemptibleJobOverQueueQuotaFn`

这就是 Queue spec 最直接影响 allocate/preempt 的地方：job 不仅要在 node 上 fit，也要在 queue capacity policy 下 fit。

capacity check 会沿 leaf queue 一路检查到 root：

- [`resultsOverLimit()`](../../../pkg/scheduler/plugins/proportion/capacity_policy/max_allowed_check.go) 检查当前 queue 及所有祖先 queue 的 `limit`。
- [`resultsWithNonPreemptibleOverQuota()`](../../../pkg/scheduler/plugins/proportion/capacity_policy/quota_check.go) 检查 non-preemptible job 是否会让当前 queue 或祖先 queue 的 non-preemptible allocated 超过 `quota`。

因此 leaf queue 自己没超限，不代表 job 一定能过 capacity；祖先 queue 的 limit/quota 也会约束它。

## Queue 如何影响 reclaim

`reclaim` 是跨 queue 的资源回收。action 自己会排除同 queue victims，`proportion` 再判断回收是否公平：

```text
reclaim pending job
  -> ssn.CanReclaimResources(job)
       -> proportion.CanReclaimResourcesFn()
          -> reclaimable.CanReclaimResources()

victim scenario
  -> proportion.reclaimableFn()
     -> reclaimable.Reclaimable()
```

`CanReclaimResources()` 先看 reclaimer 加上所需资源后是否仍在 fair share 内；如果 reclaimer 是 non-preemptible，还要求 non-preemptible allocated 不超过 deserved quota。

`Reclaimable()` 会把 reclaimer queue 和 victim queue 拉到同一层级比较。源码里的 `getLeveledQueues()` 会找两条 queue path 的分叉点，用分叉后的兄弟 queue 做公平性判断。这样 reclaim 不是简单“谁资源多抢谁”，而是在层级 Queue 树下比较兄弟分支的剩余资源和 fair share 饱和度。

over-quota 资源分配也受 queue priority 影响。[`divideOverQuotaResource()`](../../../pkg/scheduler/plugins/proportion/resource_division/resource_division.go) 会先按 `QueueAttributes.Priority` 分组，并按 priority 从高到低处理；同一 priority 组内再结合 fair share、`overQuotaWeight`、历史 usage 和 `kValue` 分配剩余资源。

## Queue 如何影响 preempt 和 min runtime

`preempt` 本身是同 queue 内的 priority preemption。victim filter 主要看：

- victim 与 preemptor 同 queue。
- victim priority 更低。
- victim 是 preemptible。
- victim 有 active allocated task。
- victim 通过插件注册的 `PreemptVictimFilterFn`。

Queue 对 preempt 的一个重要影响来自 [`minruntime`](../../../pkg/scheduler/plugins/minruntime)。它会注册 reclaim/preempt victim filter 和 scenario validator。

preempt min runtime 的解析方式：

```text
victim leaf queue
  -> 向 parent 逐层查找第一个 PreemptMinRuntime
  -> 找不到则使用 plugin default
```

reclaim min runtime 有两种解析方式：

- `queue`：从 victim leaf queue 向上找第一个 `ReclaimMinRuntime`。
- `lca`：找到 reclaimer queue 与 victim queue 的 lowest common ancestor，再沿 victim 分支向上找 runtime policy。

所以 Queue hierarchy 不仅影响“谁先被调度/回收”，也影响“一个 victim 是否已经运行得足够久，可以被 disruptive action 动到”。

minruntime 判断用的是 `PodGroupInfo.LastStartTimestamp`。`allocate` 第一次真实分配成功且没有 pipeline 时会调用 [`setLastStartTimestamp()`](../../../pkg/scheduler/actions/allocate/allocate.go)，status updater 再把它写到 PodGroup annotation `kai.scheduler/last-start-timestamp`。下一轮 snapshot 解析 annotation 后，`minruntime` 才能判断 victim 是否仍在保护窗口内。

## 一个完整例子

假设有这样的 Queue 树：

```text
root
  -> research
       -> llm-training
       -> inference
  -> platform
       -> shared-gpu
```

`llm-training`、`inference`、`shared-gpu` 是 leaf queue，可以承载 PodGroup；`root`、`research`、`platform` 是非 leaf queue，只参与资源份额和层级策略。

再看一个层级 PodGroup：

```yaml
spec:
  queue: inference
  minSubGroup: 1
  subGroups:
    - name: prefill
      minSubGroup: 1
    - name: prefill-replica-0
      parent: prefill
      minMember: 8
    - name: prefill-replica-1
      parent: prefill
      minMember: 8
    - name: decode
      minSubGroup: 1
    - name: decode-replica-0
      parent: decode
      minMember: 4
    - name: decode-replica-1
      parent: decode
      minMember: 4
```

这棵 PodGroup 树可以读成：

```text
PodGroup root(minSubGroup=1)
  -> prefill(minSubGroup=1)
       -> prefill-replica-0(minMember=8)
       -> prefill-replica-1(minMember=8)
  -> decode(minSubGroup=1)
       -> decode-replica-0(minMember=4)
       -> decode-replica-1(minMember=4)
```

调度过程会这样展开：

1. `FilterUnready` 先判断 root 的直接 child 中是否至少有 1 个 ready。
2. 如果 `prefill` 和 `decode` 都 ready，`SubGroupOrderFn` 决定先尝试哪个分支。
3. 假设先选 `decode`，`decode` 再从自己的 replica children 里选至少 1 个满足 `minMember=4` 的 leaf。
4. `GetTasksToAllocate()` 只返回这次需要证明的那批 task，而不是整个 PodGroup 全部 pending pods。
5. `AllocateJob()` 对这些 task 跑 capacity、topology、predicate、node/GPU order。
6. 成功后 job 可能被 `PushJob()` 回队列，下一次再竞争 elastic task 或另一个 replica。
7. 如果后续 reclaim/preempt 需要拿这个 job 当 victim，`GetTasksToEvict()` 会优先找超过 min 门槛的 elastic task；没有 surplus 时才驱逐某个 subtree 或完整 gang。

这个例子也说明了 `minSubGroup` 的语义：它表达“至少多少个直接 child group 满足”，不是“至少多少个 descendant pods 满足”。

## PodGrouper 如何生成这些概念

很多 PodGroup 不是用户直接写 YAML，而是 PodGrouper 根据 workload 自动生成。入口是 [`pkg/podgrouper/pod_controller.go`](../../../pkg/podgrouper/pod_controller.go)：

```text
Pod event
  -> 找 top owner
  -> 选择 workload grouper plugin
  -> 生成 podgroup.Metadata
  -> PodGroupHandler.ApplyToCluster()
  -> 给 Pod 写 PodGroup annotation 和 subgroup label
```

[`PodGroupHandler.createPodGroupForMetadata()`](../../../pkg/podgrouper/podgroup/handler.go) 做字段映射：

- metadata 有 `MinSubGroup` 时写 `spec.minSubGroup`，否则写 `spec.minMember`。
- subgroup metadata 有 `MinSubGroup` 时写 `subGroups[].minSubGroup`，否则写 `subGroups[].minMember`。
- subgroup metadata 的 `Parent` 写成 `subGroups[].parent`。
- topology metadata 写成 PodGroup 或 SubGroup 的 topology constraint。

一些 workload plugin 会主动生成层级 SubGroup：

- [`pkg/podgrouper/podgrouper/plugins/jobset/jobset_grouper.go`](../../../pkg/podgrouper/podgrouper/plugins/jobset/jobset_grouper.go)：JobSet 会生成 parent-per-replicatedJob、leaf-per-replica 的两级结构。
- [`pkg/podgrouper/podgrouper/plugins/ray/ray_grouper.go`](../../../pkg/podgrouper/podgrouper/plugins/ray/ray_grouper.go)：Ray 会把 head 和 worker groups 映射成不同 SubGroup。
- [`pkg/podgrouper/podgrouper/plugins/leaderworkerset`](../../../pkg/podgrouper/podgrouper/plugins/leaderworkerset)：LeaderWorkerSet 会按 leader/workers 或 segment 构造 SubGroup。
- [`pkg/podgrouper/podgrouper/plugins/kubeflow/pytorch`](../../../pkg/podgrouper/podgrouper/plugins/kubeflow/pytorch)：Kubeflow PyTorch 会区分 master/worker 及 worker segment。

因此 KAI 的 SubGroup 不只是手写高级字段，更是 workload 集成层和 scheduler 之间传递结构化调度语义的契约。

## 排查问题时的阅读顺序

PodGroup/SubGroup 不调度：

1. 看 Pod 是否有 PodGroup annotation 和 subgroup label。
2. 看 PodGroup `spec.subGroups` 是否有对应 leaf SubGroup。
3. 看 webhook 是否曾对 `minMember/minSubGroup/parent` 给出 warning 或拒绝。
4. 看 `PodGroupInfo.InvalidSubGroupTasks` 和 `TasksFitErrors`。
5. 看 `job.IsReadyForScheduling()` 是否被 `FilterUnready` 排掉。
6. 看 `GetTasksToAllocate()` 实际返回的是哪一批 task。

Queue 不生效或 job 没进 action：

1. 看 Queue CR 是否存在。
2. 看 parent queue 是否存在，orphan queue 会被 `cleanQueueOrphans()` 删除。
3. 看 PodGroup 指向的 queue 是否 leaf queue。
4. 看 scheduler fairness level 是否改变了队列树。
5. 看 `proportion` session 内的 allocated/requested/fair share，而不是只看 Queue status。
6. 看 `QueueOrderFn` 和 capacity policy 的判断。

抢占/回收不符合预期：

1. 先区分 `reclaim` 是跨 queue，`preempt` 是同 queue。
2. 看 victim 是否 preemptible。
3. 看 `minruntime` 是否保护了 victim。
4. 看 `GetTasksToEvict()` 是否只拿了 elastic surplus。
5. 看 solver 是否完整跑过 `AllocateJob()` 并通过 scenario validator。
6. 看 queue hierarchy 下 reclaimer/victim 是否处于能公平回收的兄弟分支关系。

## 建议测试阅读

- [`pkg/apis/scheduling/v2alpha2/podgroup_webhook_test.go`](../../../pkg/apis/scheduling/v2alpha2/podgroup_webhook_test.go)：SubGroup 图校验、leaf/mid-level min 字段规则。
- [`pkg/scheduler/api/podgroup_info/subgroup_info/subgroupset_test.go`](../../../pkg/scheduler/api/podgroup_info/subgroup_info/subgroupset_test.go)：`minSubGroup` 和直接 child 满足数。
- [`pkg/scheduler/api/podgroup_info/allocation_info_test.go`](../../../pkg/scheduler/api/podgroup_info/allocation_info_test.go)：`GetTasksToAllocate()` 的 gang/elastic 行为。
- [`pkg/scheduler/api/podgroup_info/eviction_info_test.go`](../../../pkg/scheduler/api/podgroup_info/eviction_info_test.go)：elastic surplus 与 fallback full eviction。
- [`pkg/scheduler/actions/allocate/allocate_subgroups_test.go`](../../../pkg/scheduler/actions/allocate/allocate_subgroups_test.go)：allocate action 下的 subgroup 递归分配。
- [`pkg/scheduler/actions/utils/job_order_by_queue_test.go`](../../../pkg/scheduler/actions/utils/job_order_by_queue_test.go)：多级 Queue tree 和 victim reverse order。
- [`pkg/scheduler/plugins/proportion/proportion_test.go`](../../../pkg/scheduler/plugins/proportion/proportion_test.go)：fair share 和 queue usage 计算。
- [`pkg/scheduler/plugins/proportion/reclaimable/reclaimable_test.go`](../../../pkg/scheduler/plugins/proportion/reclaimable/reclaimable_test.go)：层级 queue 下 reclaim qualification。
- [`pkg/scheduler/plugins/minruntime/resolver_test.go`](../../../pkg/scheduler/plugins/minruntime/resolver_test.go)：preempt/reclaim min runtime 在 queue tree 上的解析。
- [`test/e2e/suites/allocate/subgroups`](../../../test/e2e/suites/allocate/subgroups)：SubGroup 分配 e2e。
- [`test/e2e/suites/allocate/min_subgroups`](../../../test/e2e/suites/allocate/min_subgroups)：`minSubGroup` 行为 e2e。

## 本章检查点

读完后应该能回答：

- KAI 的 `minSubGroup` 为什么不能按 pod 数理解？
- 叶子 SubGroup 和中间 SubGroup 在内部模型中分别对应什么类型？
- 为什么只有 leaf Queue 可以承载 PodGroup？
- Queue status 为什么不是 `proportion` 公平性计算的直接输入？
- `GetTasksToAllocate()` 和 `GetTasksToEvict()` 如何让层级 SubGroup 影响 allocate/reclaim/preempt？
- `reclaim` 和 `preempt` 分别在哪些地方使用 Queue hierarchy？
