# 06. API 与 CRD

这一章把 scheduler 行为和 Kubernetes 自定义资源对上。建议先读类型定义，再读 controller 和 plugin。

如果要重点理解 KAI 相比 kube-batch 的概念扩展，先配合阅读 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)。本章偏字段入口，第 10 章偏“这些字段如何进入调度流程”。

## API 阅读顺序

1. [`pkg/apis/scheduling/v2/queue_types.go`](../../../pkg/apis/scheduling/v2/queue_types.go)
2. [`pkg/apis/scheduling/v2alpha2/podgroup_types.go`](../../../pkg/apis/scheduling/v2alpha2/podgroup_types.go)
3. [`pkg/apis/scheduling/v1alpha2/bindrequest_types.go`](../../../pkg/apis/scheduling/v1alpha2/bindrequest_types.go)
4. [`pkg/apis/kai/v1/config_types.go`](../../../pkg/apis/kai/v1/config_types.go)
5. [`pkg/apis/kai/v1/schedulingshard_types.go`](../../../pkg/apis/kai/v1/schedulingshard_types.go)
6. [`pkg/apis/kai/v1alpha1/topology_types.go`](../../../pkg/apis/kai/v1alpha1/topology_types.go)

生成后的 CRD YAML 在 [`deployments/kai-scheduler/crds`](../../../deployments/kai-scheduler/crds)。

## Queue

源码：[`pkg/apis/scheduling/v2/queue_types.go`](../../../pkg/apis/scheduling/v2/queue_types.go)

Queue 不只是 PodGroup 的字符串分类。KAI 的 Queue 是层级资源策略树，只有 leaf queue 可以承载 PodGroup；非 leaf queue 负责资源份额、priority、runtime policy 的继承和汇总。调度路径详见 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md#queue-crd)。

```go
type QueueSpec struct {
    DisplayName string

    // ParentQueue 让 Queue 可以形成层级结构。
    ParentQueue string

    // quota、limit、guarantee 等资源策略在这里。
    Resources *QueueResources

    // 高优先级 queue 会更早获得 over-quota 资源，并更晚被 reclaim。
    Priority *int

    // disruptive action 的最小运行时间保护。
    PreemptMinRuntime *metav1.Duration
    ReclaimMinRuntime *metav1.Duration
}
```

Queue `status` 是观测到的资源使用，主要由 QueueController 写入，用于观测、metrics 和排障：

```go
type QueueStatus struct {
    ChildQueues []string
    Allocated v1.ResourceList
    AllocatedNonPreemptible v1.ResourceList
    Requested v1.ResourceList
}
```

对照阅读：

- [`pkg/queuecontroller/controllers/queue_controller.go`](../../../pkg/queuecontroller/controllers/queue_controller.go)
- [`pkg/scheduler/plugins/proportion/proportion.go`](../../../pkg/scheduler/plugins/proportion/proportion.go)

注意 `queue_info.NewQueueInfo()` 主要读取 Queue `spec` 和 metadata：parent、resources、priority、runtime policy、creation timestamp。当前 `proportion` 在 session 内基于 `PodGroupInfo` 和 task status 重新累计 allocated/pending，不直接把 Queue `status.requested/allocated` 当作公平性输入。

## PodGroup

源码：[`pkg/apis/scheduling/v2alpha2/podgroup_types.go`](../../../pkg/apis/scheduling/v2alpha2/podgroup_types.go)

PodGroup 保留扁平 `minMember` gang 语义，同时扩展出 `minSubGroup` 和层级 `SubGroups`。读源码时要区分叶子 SubGroup 的 `minMember` 与中间节点的 `minSubGroup`，不要把 `minSubGroup` 读成 descendant pod 数。完整语义详见 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md#podgroup-crd)。

```go
type PodGroupSpec struct {
    // 扁平 gang scheduling 的最小成员数。
    // 和 MinSubGroup 互斥。
    MinMember *int32

    // 直接子 SubGroup 中至少满足多少个。
    MinSubGroup *int32

    // Queue 不存在时，PodGroup 不会被调度。
    Queue string

    // 映射 workload priority。
    PriorityClassName string

    // 显式控制是否可被抢占。
    Preemptibility Preemptibility

    // topology-aware scheduling 约束。
    TopologyConstraint TopologyConstraint

    // 更细粒度的 pod set。
    SubGroups []SubGroup
}
```

SubGroup 是层级 gang scheduling 的入口：

```go
type SubGroup struct {
    Name string
    MinMember *int32
    MinSubGroup *int32
    Parent *string
    TopologyConstraint *TopologyConstraint
}
```

PodGroup `status` 是观测和反馈面，字段写入者不完全相同：

```go
type PodGroupStatus struct {
    Phase PodGroupPhase
    Running int32
    Succeeded int32
    Failed int32
    Pending int32
    SchedulingConditions []SchedulingCondition
    ResourcesStatus PodGroupResourcesStatus
}
```

对照阅读：

- [`pkg/podgrouper/pod_controller.go`](../../../pkg/podgrouper/pod_controller.go)
- [`pkg/podgroupcontroller/controllers/pod_group_controller.go`](../../../pkg/podgroupcontroller/controllers/pod_group_controller.go)
- [`pkg/podgroupcontroller/controllers/patcher/pod_group.go`](../../../pkg/podgroupcontroller/controllers/patcher/pod_group.go)
- [`pkg/scheduler/cache/status_updater/default_status_updater.go`](../../../pkg/scheduler/cache/status_updater/default_status_updater.go)
- [`pkg/scheduler/api/podgroup_info/job_info.go`](../../../pkg/scheduler/api/podgroup_info/job_info.go)

当前源码中，PodGroupController patcher 主要更新 `resourcesStatus.requested/allocated/allocatedNonPreemptible`，并保留原有 status 其他字段。scheduler status updater 会写 `schedulingConditions`，以及 stale/last-start timestamp annotations。`phase/running/pending/succeeded/failed` 虽然存在于 API 类型里，但读实现时不要默认它们会被 PodGroupController 每轮可靠刷新。

## BindRequest

源码：[`pkg/apis/scheduling/v1alpha2/bindrequest_types.go`](../../../pkg/apis/scheduling/v1alpha2/bindrequest_types.go)

```go
type BindRequestSpec struct {
    // 要绑定的 Pod。
    PodName string

    // scheduler 选中的节点。
    SelectedNode string

    // Regular 或 Fraction。
    ReceivedResourceType string

    // GPU 数量和 portion。
    ReceivedGPU *ReceivedGPU

    // fractional GPU group。
    SelectedGPUGroups []string

    // DRA ResourceClaim 分配结果。
    ResourceClaimAllocations []ResourceClaimAllocation

    // binder 重试上限。
    BackoffLimit *int32
}
```

这是 scheduler 和 binder 的边界：

```text
Scheduler
  -> cache.Bind()
  -> 创建 BindRequest

Binder
  -> watch BindRequest
  -> reserve GPU / binder plugins / pods/binding
  -> 更新 BindRequest.status
```

对照阅读：

- [`pkg/scheduler/cache`](../../../pkg/scheduler/cache)
- [`pkg/binder/controllers/bindrequest_controller.go`](../../../pkg/binder/controllers/bindrequest_controller.go)
- [`pkg/binder/binding/binder.go`](../../../pkg/binder/binding/binder.go)

## Config

源码：[`pkg/apis/kai/v1/config_types.go`](../../../pkg/apis/kai/v1/config_types.go)

```go
type ConfigSpec struct {
    Namespace string
    Global *GlobalConfig
    PodGrouper *pod_grouper.PodGrouper
    Binder *binder.Binder
    Admission *admission.Admission
    Scheduler *scheduler.Scheduler
    QueueController *queue_controller.QueueController
    PodGroupController *pod_group_controller.PodGroupController
    NodeScaleAdjuster *node_scale_adjuster.NodeScaleAdjuster
    Prometheus *prometheus.Prometheus
}
```

`Config` 是 operator 的全局期望状态。operator 会把它转成各组件的 Deployment、Service、Webhook、ConfigMap 等。

对照阅读：

- [`pkg/operator/controller/config_controller.go`](../../../pkg/operator/controller/config_controller.go)
- [`pkg/operator/operands`](../../../pkg/operator/operands)
- [`deployments/kai-scheduler/templates/kai-config.yaml`](../../../deployments/kai-scheduler/templates/kai-config.yaml)

## SchedulingShard

源码：[`pkg/apis/kai/v1/schedulingshard_types.go`](../../../pkg/apis/kai/v1/schedulingshard_types.go)

```go
type SchedulingShardSpec struct {
    // scheduler CLI 参数覆盖。
    Args map[string]string

    // CPU/GPU binpack 或 spread。
    PlacementStrategy *PlacementStrategy

    // 调度分区 label value。
    PartitionLabelValue string

    // 每个 action 每个 queue 最多尝试多少 job。
    QueueDepthPerAction map[string]int

    // plugin enable/priority/arguments 覆盖。
    Plugins map[string]PluginConfig

    // action enable/priority 覆盖。
    Actions map[string]ActionConfig
}
```

关键点：

- 内置 plugin priority 保留默认插件顺序。
- 内置 action priority 保留 `allocate -> consolidation -> reclaim -> preempt -> stalegangeviction`。
- `PlacementStrategy` 会影响 `nodeplacement`、`gpupack`、`gpuspread` 等插件配置。

对照阅读：

- [`pkg/operator/controller/schedulingshard_controller.go`](../../../pkg/operator/controller/schedulingshard_controller.go)
- [`pkg/operator/operands/scheduler`](../../../pkg/operator/operands/scheduler)
- [`deployments/kai-scheduler/templates/default-shard.yaml`](../../../deployments/kai-scheduler/templates/default-shard.yaml)

## Topology

源码：[`pkg/apis/kai/v1alpha1/topology_types.go`](../../../pkg/apis/kai/v1alpha1/topology_types.go)

Topology 描述集群拓扑层级。PodGroup 通过 topology constraint 引用拓扑要求，scheduler 的 topology plugin 使用这些约束选择 node subset。

对照阅读：

- [`pkg/scheduler/plugins/topology`](../../../pkg/scheduler/plugins/topology)
- [`docs/topology/README.md`](../../topology/README.md)
- [`docs/developer/designs/topology-awareness/README.md`](../designs/topology-awareness/README.md)

## 调试 CRD 字段的读法

- YAML 字段被拒绝：读 Go type 和 `deployments/kai-scheduler/crds` 中生成的 CRD。
- 字段被接受但没效果：找哪个 controller 或 plugin 读取它。
- status 不符合预期：找写 status subresource 的 controller。
- scheduler 配置没效果：先判断字段属于 `Config` 还是 `SchedulingShard`。
- PodGroup/Queue 行为不符合 kube-batch 直觉：先读 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)，确认是否被层级 SubGroup 或层级 Queue 语义改变。

## 本章检查点

读完后应该能回答：

- 哪个 CRD 是 scheduler 到 binder 的契约？
- 哪个 CRD 控制 Queue 层级和公平性策略？
- 哪个 CRD 控制 actions/plugins 启用和顺序？
- PodGroup status 中哪些字段由 PodGroupController 写，哪些由 scheduler status updater 写？
