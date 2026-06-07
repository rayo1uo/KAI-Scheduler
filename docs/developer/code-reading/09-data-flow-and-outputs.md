# 09. 调度数据流与输出链路

这一章把 Kubernetes 对象、scheduler 内部模型、action/plugin 决策和外围 controller 串起来。建议在读完 [`02-scheduler-main-loop.md`](02-scheduler-main-loop.md)、[`03-session-and-statement.md`](03-session-and-statement.md)、[`04-actions.md`](04-actions.md) 后阅读。

PodGroup/SubGroup 与 Queue 的概念解释见 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)。本章只说明它们在 snapshot、status 和输出链路中的位置。

## 总体链路

```text
Kubernetes objects
  -> SchedulerCache.Snapshot()
  -> api.ClusterInfo
  -> framework.OpenSession()
  -> plugins.OnSessionOpen()
  -> actions.Execute()
  -> Statement.Commit() / Session.Evict()
  -> BindRequest / Pipeline event / Eviction
  -> Binder 与外围 controllers 落回 Kubernetes
```

外围 controller 负责准备 scheduler 需要的 spec/annotation 输入、消费 scheduler 的输出，并维护面向观测的 status：

```text
PodGrouper
  -> 创建/更新 PodGroup spec
  -> 给 Pod 写 PodGroup annotation 和 subgroup label

PodGroupController
  -> 汇总 PodGroup.status.resourcesStatus

QueueController
  -> 聚合 Queue.status requested/allocated/childQueues

Scheduler
  -> 基于 snapshot 做 action/plugin 决策
  -> 创建 BindRequest 或驱逐 Pod

Binder
  -> 消费 BindRequest
  -> 真正调用 Kubernetes pods/binding

Operator
  -> 部署和配置 scheduler、binder、controllers 等组件
```

## Snapshot 如何构造内部模型

入口：

- [`pkg/scheduler/cache/cache.go`](../../../pkg/scheduler/cache/cache.go)
- [`pkg/scheduler/cache/cluster_info/cluster_info.go`](../../../pkg/scheduler/cache/cluster_info/cluster_info.go)
- [`pkg/scheduler/api/cluster_info.go`](../../../pkg/scheduler/api/cluster_info.go)

`SchedulerCache.Snapshot()` 委托 `ClusterInfo.Snapshot()` 构造 `api.ClusterInfo`。关键顺序：

1. `ListPods()` 先拿所有 Pod，但不马上建 PodGroup。
2. `snapshotNodes()` 读取 Node，构造 `node_info.NodeInfo`，初始化 allocatable、idle、used、releasing、GPU memory、MIG、DRA GPU、NodeResourceTopology 等信息。
3. `snapshotBindRequests()` 读取 BindRequest，按 `namespace/podName` 建 `BindRequestMap`。目标节点不存在的 BindRequest 会进入 `BindRequestsForDeletedNodes`，后续由 cache 清理。
4. `addTasksToNodes()` 用 Pod、BindRequest、DRA claims 构造 `pod_info.PodInfo`。如果 Pod 没有 `spec.nodeName` 但存在 BindRequest，`PodInfo.NodeName` 会使用 `BindRequest.Spec.SelectedNode`，状态会变成 `Binding`。
5. Node 只把 active used 状态的 Pod 计入资源，例如 `Running`、`Bound`、`Binding`、`Releasing`。Pending/Gated 不占节点资源。
6. `snapshotQueues()` 读取 Queue CR，构造 `queue_info.QueueInfo`，再 `UpdateQueueHierarchy()` 填 children，并删除 orphan queue。
7. `snapshotPodGroups()` 读取 PodGroup CR，过滤不属于当前 scheduler/node pool 的 PodGroup，构造 `podgroup_info.PodGroupInfo`，再通过 pod informer index 按 PodGroup annotation 找 Pod 并加入任务。
8. `existingPods` 让 NodeInfo 和 PodGroupInfo 尽量引用同一个 `PodInfo` 对象。这样 action 改 task 状态时，node 和 job 视图可以保持一致。

## 内部模型对应关系

| Kubernetes 对象 | Scheduler 模型 | 阅读入口 | 关注点 |
| --- | --- | --- | --- |
| Pod | `pod_info.PodInfo` | [`pkg/scheduler/api/pod_info/pod_info.go`](../../../pkg/scheduler/api/pod_info/pod_info.go) | PodGroup、SubGroup、资源请求、GPU fraction/memory/MIG/DRA、BindRequest、Pod 状态 |
| Node | `node_info.NodeInfo` | [`pkg/scheduler/api/node_info/node_info.go`](../../../pkg/scheduler/api/node_info/node_info.go) | idle/used/releasing/allocatable、GPU groups、node topology、PodInfo 集合 |
| PodGroup | `podgroup_info.PodGroupInfo` | [`pkg/scheduler/api/podgroup_info/job_info.go`](../../../pkg/scheduler/api/podgroup_info/job_info.go) | gang/subgroup、task 索引、allocated vector、fit errors、stale 信息 |
| Queue | `queue_info.QueueInfo` | [`pkg/scheduler/api/queue_info/queue_info.go`](../../../pkg/scheduler/api/queue_info/queue_info.go) | spec 中的 quota/limit/over-quota weight、parent/children、priority、runtime policy |
| BindRequest | `bindrequest_info.BindRequestInfo` | [`pkg/scheduler/api/bindrequest_info/binrequest_info.go`](../../../pkg/scheduler/api/bindrequest_info/binrequest_info.go) | selected node、GPU groups、DRA allocations、失败 backoff |

`PodGroupInfo` 和 `QueueInfo` 的字段不是 CRD 的机械拷贝：`PodGroupInfo` 会把 `subGroups` 展开成 root `SubGroupSet` 和多个 leaf `PodSet`；`QueueInfo` 只读取 Queue spec/metadata，再由 cache 填充 `ChildQueues`。这两个转换点是理解 action 行为的关键，详细解释见 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)。

## Action commit 后的三类输出

`Statement.Commit()` 在 [`pkg/scheduler/framework/statement.go`](../../../pkg/scheduler/framework/statement.go)。

### Allocate 输出

```text
Statement.Commit()
  -> commitAllocate()
  -> Session.BindPod()
  -> Cache.Bind()
  -> create BindRequest
```

BindRequest 包含：

- Pod 名字和 namespace。
- selected node。
- selected GPU groups。
- received resource type / received GPU。
- DRA ResourceClaim allocations，来自 `podInfo.ResourceClaimInfo.ToSlice()`。
- plugin 通过 `BindRequestMutateFn` 注入的 annotations。当前源码没有内置 scheduler plugin 注册这个回调，但扩展点已经存在。

`Cache.createBindRequest()` 创建对象后，会手动把 BindRequest Add 到 informer store，避免下一轮 snapshot 在 watch 尚未同步时重复调度同一个 Pod。

### Pipeline 输出

```text
Statement.Commit()
  -> commitPipeline()
  -> Cache.TaskPipelined()
```

Pipeline 不创建 BindRequest。它表示 task 被放到了即将释放的资源上，等相关 victim 释放后，后续调度周期再推进到真实 binding。

### Evict 输出

```text
Statement.Commit()
  -> commitEvict()
  -> Cache.Evict()
  -> Evictor.Evict()
  -> 删除 Pod / 记录 PodGroup eviction event
```

`Session.Evict()` 也会走 `Cache.Evict()`，但不通过 `Statement`。`stalegangeviction` 就是这个直接路径。

## Status updater

入口：[`pkg/scheduler/cache/status_updater/default_status_updater.go`](../../../pkg/scheduler/cache/status_updater/default_status_updater.go)

常见职责：

- `Bound()`：记录 Scheduled 或 FailedBinding event，失败时更新 PodScheduled condition。
- `Evicted()`：在 PodGroup 上记录带 annotations 的 eviction event，并打 metrics。
- `RecordJobStatusEvent()`：根据 fit errors、PodGroup readiness、stale 状态等记录 Pod/PodGroup 事件，必要时更新 PodGroup scheduling condition。
- `SyncPodGroupsWithPendingUpdates()`：下一次 snapshot 时合并尚未被 informer 反映出来的 PodGroup status/annotation 更新，减少异步写入带来的抖动。

## Binder 如何消费 BindRequest

入口：

- [`pkg/binder/controllers/bindrequest_controller.go`](../../../pkg/binder/controllers/bindrequest_controller.go)
- [`pkg/binder/binding/binder.go`](../../../pkg/binder/binding/binder.go)

流程：

```text
BindRequestReconciler.Reconcile()
  -> get BindRequest
  -> 跳过 deleting/succeeded
  -> get Pod
  -> get selected Node
  -> Binder.Bind(ctx, pod, node, bindRequest)
       -> resourceReservationService.SyncForNode(...)
       -> shared GPU reserveGPUs(...)
       -> plugins.PreBind(...)
       -> patch Pod received-resource-type annotation
       -> Kubernetes pods/binding subresource
       -> plugins.PostBind(...)
  -> 更新 BindRequest.status
  -> 更新 PodBound condition
```

如果绑定失败，controller 会写失败状态并按 backoff 重试。`UpdateStatus` 会把 BindRequest 标成 `Succeeded` 或 `Failed`，失败时写入 `Reason` 并递增 `FailedAttempts`。失败达到限制的 BindRequest 在 scheduler snapshot 中不再作为有效 binding 输入。

这里有一个当前源码行为值得单独记住：`updatePodCondition` 在 `err == nil || result.RequeueAfter != 0` 时会把 PodBound condition 写成 `True/Bound`。也就是说，如果绑定失败但仍处在 backoff 重试窗口内，Pod condition 仍可能表现为 Bound。排障时要同时看 BindRequest `status.phase/reason/failedAttempts`。

BindRequest 对 scheduler snapshot 的影响在 [`pkg/scheduler/api/bindrequest_info/binrequest_info.go`](../../../pkg/scheduler/api/bindrequest_info/binrequest_info.go)：

- `status.phase != Failed` 时可以作为有效 binding 输入。
- `status.phase == Failed` 且没有 `spec.backoffLimit` 时视为失败，不再用于 `GetBindRequestForPod`。
- `status.phase == Failed` 且 `failedAttempts >= backoffLimit` 时视为失败。
- `status.phase == Failed` 但还没达到 backoff limit 时仍不算最终失败，Pod 在 snapshot 中可能继续被看作 `Binding`。

## PodGrouper 如何生成 PodGroup

入口：

- [`pkg/podgrouper/pod_controller.go`](../../../pkg/podgrouper/pod_controller.go)
- [`pkg/podgrouper/podgrouper/podgrouper.go`](../../../pkg/podgrouper/podgrouper/podgrouper.go)
- [`pkg/podgrouper/podgroup/handler.go`](../../../pkg/podgrouper/podgroup/handler.go)

流程：

```text
Pod event
  -> 过滤非目标 schedulerName
  -> 找 top owner
  -> 根据 owner kind 选择 grouper plugin
  -> 生成 PodGroup metadata
       queue
       priorityClass
       minMember / minSubGroup
       subGroups
       topology
       preemptibility
  -> create/update PodGroup
  -> patch Pod annotation 和 subgroup label
```

scheduler 的 `snapshotPodGroups()` 后续依赖 PodGroup annotation index 找回属于某个 PodGroup 的 Pod。

## PodGroupController 与 QueueController

PodGroupController 入口：

- [`pkg/podgroupcontroller/controllers/pod_group_controller.go`](../../../pkg/podgroupcontroller/controllers/pod_group_controller.go)
- [`pkg/podgroupcontroller/controllers/status_updater.go`](../../../pkg/podgroupcontroller/controllers/status_updater.go)

它 watch PodGroup 和 Pod，汇总：

- `resourcesStatus.requested`
- `resourcesStatus.allocated`
- `resourcesStatus.allocatedNonPreemptible`

QueueController 入口：

- [`pkg/queuecontroller/controllers/queue_controller.go`](../../../pkg/queuecontroller/controllers/queue_controller.go)
- [`pkg/queuecontroller/controllers/resource_updater/resource_updater.go`](../../../pkg/queuecontroller/controllers/resource_updater/resource_updater.go)
- [`pkg/queuecontroller/controllers/childqueues_updater/childqueues_updater.go`](../../../pkg/queuecontroller/controllers/childqueues_updater/childqueues_updater.go)

它聚合 child queues 和 PodGroups，更新 Queue status 的 requested、allocated、allocatedNonPreemptible、childQueues。这些 Queue status 字段主要用于观测、metrics 和排障；不要把它们读成 scheduler fairness 的直接实时输入。

## Queue/PodGroup status 是否参与调度

按当前源码，`Queue.status.*` 和 `PodGroup.status.resourcesStatus` 不是 `proportion` 公平性和 quota/capacity 的直接输入。

调度侧的关键路径是：

```text
Queue CR spec
  -> queue_info.NewQueueInfo()
  -> QueueInfo.Resources / ParentQueue / Priority / runtime policy

PodGroup CR spec + Pods + BindRequests
  -> podgroup_info.PodGroupInfo
  -> task status index / allocated vectors / pending resources

proportion.OnSessionOpen()
  -> createQueueResourceAttrs() 从 QueueInfo spec 建 queue attributes
  -> updateQueuesCurrentResourceUsage() 从 PodGroupInfos 和 task status 重新累计 allocated/pending
```

因此 QueueController 和 PodGroupController 的 status 更新更多是观测面：

- QueueController 把 PodGroup `resourcesStatus` 和 child queue status 聚合到 Queue status，并导出 metrics。
- PodGroupController 根据相关 Pod 汇总 `resourcesStatus.requested/allocated/allocatedNonPreemptible`。
- scheduler 的 status updater 写 scheduling conditions、stale timestamp、last-start timestamp 等用于事件、状态展示和下一轮 snapshot 的一致性同步。

## PodGroup status 写入者矩阵

| 字段/信息 | 当前主要写入者 | 阅读入口 |
| --- | --- | --- |
| `status.resourcesStatus.requested` | PodGroupController | [`pkg/podgroupcontroller/controllers/patcher/pod_group.go`](../../../pkg/podgroupcontroller/controllers/patcher/pod_group.go) |
| `status.resourcesStatus.allocated` | PodGroupController | [`pkg/podgroupcontroller/controllers/patcher/pod_group.go`](../../../pkg/podgroupcontroller/controllers/patcher/pod_group.go) |
| `status.resourcesStatus.allocatedNonPreemptible` | PodGroupController | [`pkg/podgroupcontroller/controllers/patcher/pod_group.go`](../../../pkg/podgroupcontroller/controllers/patcher/pod_group.go) |
| `status.schedulingConditions` | Scheduler status updater | [`pkg/scheduler/cache/status_updater/default_status_updater.go`](../../../pkg/scheduler/cache/status_updater/default_status_updater.go) |
| stale timestamp annotation | Scheduler status updater | [`pkg/scheduler/cache/status_updater/default_status_updater.go`](../../../pkg/scheduler/cache/status_updater/default_status_updater.go) |
| last-start timestamp annotation | Scheduler status updater | [`pkg/scheduler/cache/status_updater/default_status_updater.go`](../../../pkg/scheduler/cache/status_updater/default_status_updater.go) |

`PodGroupStatus` type 里还有 `phase/running/pending/succeeded/failed` 字段，但当前 PodGroupController patcher 主要保留原 status 并更新 `resourcesStatus`。读源码时不要默认这些计数字段会由 PodGroupController 在每次 reconcile 中可靠刷新。

## Operator 如何写入 action/plugin 配置

入口：

- [`pkg/operator/controller/config_controller.go`](../../../pkg/operator/controller/config_controller.go)
- [`pkg/operator/controller/schedulingshard_controller.go`](../../../pkg/operator/controller/schedulingshard_controller.go)
- [`pkg/operator/operands/scheduler/plugin_action_merge.go`](../../../pkg/operator/operands/scheduler/plugin_action_merge.go)

Config reconciler 部署全局组件，例如 podgrouper、binder、queuecontroller、podgroupcontroller、node-scale-adjuster、admission、prometheus。

SchedulingShard reconciler 为每个 shard 部署 scheduler Deployment、ConfigMap、Service、EndpointSlice，并把 Config 中启用的 actions/plugins 按 priority 排序写进 scheduler 配置。

## 阅读检查点

读完本章后，应该能回答：

- Pending Pod 为什么可能在 snapshot 中变成 `Binding`？
- 为什么 action 不直接调用 Kubernetes bind？
- `Pipeline` 和 `BindRequest` 的区别是什么？
- PodGroup status 和 Queue status 分别由谁维护？
- `proportion` 的公平性输入为什么不是直接来自 Queue/PodGroup status？
