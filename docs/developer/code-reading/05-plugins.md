# 05. Plugins

Plugins 是 scheduler 的策略层。Action 决定调度阶段，Plugin 决定排序、过滤、配额、公平性、打分和 scenario 校验。

## 深读章节

本章保留 plugin 总览。逐类插件的详细源码讲解已经拆到子目录：

| 深读文档 | 内容 |
| --- | --- |
| [`plugins/README.md`](plugins/README.md) | Plugins 总地图、默认启用插件、逐插件速查 |
| [`plugins/00-session-callbacks.md`](plugins/00-session-callbacks.md) | `Session` 回调注册、调用点和多插件合并规则 |
| [`plugins/01-constraints-and-predicates.md`](plugins/01-constraints-and-predicates.md) | `predicates`、`dynamicresources`、`podaffinity` |
| [`plugins/02-fairness-quota-and-victims.md`](plugins/02-fairness-quota-and-victims.md) | `proportion`、`minruntime`、quota/capacity、reclaim/preempt validator |
| [`plugins/03-ordering-plugins.md`](plugins/03-ordering-plugins.md) | `priority`、`elastic`、`kubeflow`、`ray`、`taskorder`、`subgrouporder`、`reflectjoborder` |
| [`plugins/04-placement-gpu-topology-debug.md`](plugins/04-placement-gpu-topology-debug.md) | `nodeplacement`、`nodeavailability`、GPU order、`topology`、`snapshot` |

## 先打开这些文件

| 内容 | 文件 |
| --- | --- |
| Plugin 接口 | [`pkg/scheduler/framework/interface.go`](../../../pkg/scheduler/framework/interface.go) |
| Plugin registry | [`pkg/scheduler/framework/plugins.go`](../../../pkg/scheduler/framework/plugins.go) |
| 默认插件注册 | [`pkg/scheduler/plugins/factory.go`](../../../pkg/scheduler/plugins/factory.go) |
| Session 回调列表 | [`pkg/scheduler/framework/session_plugins.go`](../../../pkg/scheduler/framework/session_plugins.go) |
| Predicates 插件 | [`pkg/scheduler/plugins/predicates/predicates.go`](../../../pkg/scheduler/plugins/predicates/predicates.go) |
| Proportion 插件 | [`pkg/scheduler/plugins/proportion/proportion.go`](../../../pkg/scheduler/plugins/proportion/proportion.go) |
| Node placement 插件 | [`pkg/scheduler/plugins/nodeplacement/nodeplacement.go`](../../../pkg/scheduler/plugins/nodeplacement/nodeplacement.go) |
| Topology 插件 | [`pkg/scheduler/plugins/topology`](../../../pkg/scheduler/plugins/topology) |

## Plugin 接口

源码：[`pkg/scheduler/framework/interface.go`](../../../pkg/scheduler/framework/interface.go)

```go
type Plugin interface {
    Name() string

    // 每个调度 Session 打开时调用。
    OnSessionOpen(ssn *Session)

    // Session 关闭时调用。
    OnSessionClose(ssn *Session)
}
```

Plugin 会根据 scheduler config 在每轮 `OpenSession` 时实例化。

## Plugin registry

源码：[`pkg/scheduler/framework/plugins.go`](../../../pkg/scheduler/framework/plugins.go)

```go
type PluginBuilder func(PluginArguments) Plugin

var pluginBuilders = map[string]PluginBuilder{}

func RegisterPluginBuilder(name string, pc func(PluginArguments) Plugin) {
    pluginBuilders[name] = pc
}

func GetPluginBuilder(name string) (PluginBuilder, bool) {
    pb, found := pluginBuilders[name]
    return pb, found
}
```

区分两个动作：

- 注册：发生在 [`pkg/scheduler/plugins/factory.go`](../../../pkg/scheduler/plugins/factory.go)。
- 启用：发生在 `OpenSession` 读取 `config.Tiers` 时。

## 默认插件

源码：[`pkg/scheduler/plugins/factory.go`](../../../pkg/scheduler/plugins/factory.go)

```go
func InitDefaultPlugins() {
    framework.RegisterPluginBuilder("predicates", predicates.New)
    framework.RegisterPluginBuilder("priority", priority.New)
    framework.RegisterPluginBuilder("nodeplacement", nodeplacement.New)
    framework.RegisterPluginBuilder("nominatednode", nominatednode.New)
    framework.RegisterPluginBuilder("nodeavailability", nodeavailability.New)
    framework.RegisterPluginBuilder("gpusharingorder", gpusharingorder.New)
    framework.RegisterPluginBuilder("gpupack", gpupack.New)
    framework.RegisterPluginBuilder("gpuspread", gpuspread.New)
    framework.RegisterPluginBuilder("resourcetype", resourcetype.New)
    framework.RegisterPluginBuilder("podaffinity", podaffinity.New)
    framework.RegisterPluginBuilder("elastic", elastic.New)
    framework.RegisterPluginBuilder("kubeflow", kubeflow.New)
    framework.RegisterPluginBuilder("ray", ray.New)
    framework.RegisterPluginBuilder("taskorder", taskorder.New)
    framework.RegisterPluginBuilder("subgrouporder", subgrouporder.New)
    framework.RegisterPluginBuilder("dynamicresources", dynamicresources.New)
    framework.RegisterPluginBuilder("topology", topology.New)
    framework.RegisterPluginBuilder("proportion", proportion.New)
    framework.RegisterPluginBuilder("minruntime", minruntime.New)
    framework.RegisterPluginBuilder("snapshot", snapshot.New)
    framework.RegisterPluginBuilder("reflectjoborder", reflectjoborder.New)
}
```

阅读建议：不要一次性读完所有插件。先按行为挑插件：

- 调度不上：先读 `predicates`、`proportion`。
- 节点选择不符合预期：读 `nodeplacement`、`gpupack/gpuspread`。
- Queue 公平性：读 `proportion`。
- topology：读 `topology`。
- DRA：读 `dynamicresources`。

## 扩展点模式

大部分插件形态如下：

```go
func (p *somePlugin) OnSessionOpen(ssn *framework.Session) {
    ssn.AddJobOrderFn(...)
    ssn.AddTaskOrderFn(...)
    ssn.AddNodeOrderFn(...)
    ssn.AddPredicateFn(...)
    ssn.AddQueueOrderFn(...)
}
```

Action 不直接知道哪个插件提供策略，而是调用 `Session` 聚合方法：

```text
allocateTask()
  -> ssn.PrePredicateFn(task, job)
  -> ssn.OrderedNodesByTask(nodes, task)
  -> ssn.FittingNode(task, node, ...)

reclaim/preempt solver
  -> ssn.ReclaimScenarioValidatorFn(...)
  -> ssn.PreemptScenarioValidator(...)
  -> ssn.ReclaimVictimFilter(...)
  -> ssn.PreemptVictimFilter(...)
```

## Predicates 插件

源码：[`pkg/scheduler/plugins/predicates/predicates.go`](../../../pkg/scheduler/plugins/predicates/predicates.go)

```go
func (pp *predicatesPlugin) OnSessionOpen(ssn *framework.Session) {
    k8sPredicates := predicates.NewSessionPredicates(ssn)
    pp.initializeK8sNodeInfos(ssn)

    ssn.AddPrePredicateFn(func(task *pod_info.PodInfo, _ *podgroup_info.PodGroupInfo) error {
        return pp.evaluateTaskOnPrePredicate(task, k8sPredicates)
    })

    ssn.AddVictimInvariantPrePredicateFn(func(task *pod_info.PodInfo) *api.VictimInvariantPrePredicateFailure {
        return pp.evaluateTaskOnVictimInvariantPrePredicates(task, k8sPredicates)
    })

    ssn.AddPredicateFn(func(task *pod_info.PodInfo, job *podgroup_info.PodGroupInfo, node *node_info.NodeInfo) error {
        return pp.evaluateTaskOnPredicates(task, job, node, k8sPredicates, ...)
    })
}
```

阅读重点：

- 这个插件把 KAI scheduler 和 Kubernetes scheduler 风格的 predicate 连接起来。
- `PrePredicateFn` 在节点打分前执行。
- `PredicateFn` 在 task/node 组合上执行。
- victim-invariant pre-predicate 可以让 reclaim/preempt/consolidation 提前跳过不可能成功的场景。

## Proportion 插件

源码：[`pkg/scheduler/plugins/proportion/proportion.go`](../../../pkg/scheduler/plugins/proportion/proportion.go)

这是 Queue 公平性和 capacity policy 的核心插件。

```go
func (pp *proportionPlugin) OnSessionOpen(ssn *framework.Session) {
    pp.calculateResourcesProportion(ssn)

    capacityPolicy := cp.New(pp.queues)

    ssn.AddQueueOrderFn(pp.queueOrder)
    ssn.AddCanReclaimResourcesFn(pp.CanReclaimResourcesFn)
    ssn.AddReclaimScenarioValidatorFn(pp.reclaimableFn)
    ssn.AddOnJobSolutionStartFn(pp.OnJobSolutionStartFn)

    ssn.AddIsNonPreemptibleJobOverQueueQuotaFns(capacityPolicy.IsNonPreemptibleJobOverQuota)
    ssn.AddIsJobOverCapacityFn(capacityPolicy.IsJobOverQueueCapacity)
    ssn.AddIsTaskAllocationOnNodeOverCapacityFn(capacityPolicy.IsTaskAllocationOnNodeOverCapacity)

    ssn.AddEventHandler(&framework.EventHandler{
        AllocateFunc:   pp.allocateHandlerFn(ssn),
        DeallocateFunc: pp.deallocateHandlerFn(ssn),
    })
}
```

阅读重点：

- Queue 排序来自 `AddQueueOrderFn`。
- 是否允许 reclaim 来自 `AddCanReclaimResourcesFn`。
- allocate/preempt 的 capacity check 来自 `capacityPolicy`。
- event handler 会在虚拟 allocate/deallocate 时维护模拟资源状态。
- `OnJobSolutionStartFn` 虽然被注册在 session 上，但按当前源码只有 `reclaim` 在进入 solver 前调用它；preempt/consolidation 没有调用这个 hook。

## Node placement 插件

源码：[`pkg/scheduler/plugins/nodeplacement/nodeplacement.go`](../../../pkg/scheduler/plugins/nodeplacement/nodeplacement.go)

```go
func (pp *nodePlacementPlugin) OnSessionOpen(ssn *framework.Session) {
    pp.gpuTaskScoreFn = pp.nodeResourcePack(resource_info.GPUResourceName)
    pp.gpuPreOrderFn = pp.setBinpackPreOrder

    if pp.pluginArguments[constants.GPUResource] == constants.SpreadStrategy {
        pp.gpuPreOrderFn = noopPreOrderFn
        pp.gpuTaskScoreFn = nodeResourceSpread(resource_info.GPUResourceName)
    }

    ssn.AddNodePreOrderFn(pp.nodePreOrderFn)
    ssn.AddNodeOrderFn(pp.nodeOrderFn)
}
```

这说明 `SchedulingShard.spec.placementStrategy` 最终会影响节点打分。

## 插件族速查

| 插件 | 读什么 |
| --- | --- |
| `predicates` | Kubernetes 约束、prefilter、node fit error。 |
| `proportion` | Queue 公平性、reclaimability、capacity policy。 |
| `priority` | PodGroup priority 排序。 |
| `nodeplacement` | CPU/GPU binpack 或 spread。 |
| `gpupack`、`gpuspread`、`gpusharingorder` | GPU 选择偏好。 |
| `elastic` | 弹性 workload 的 min/max 行为。 |
| `subgrouporder`、`taskorder` | 层级 PodGroup 内部顺序。 |
| `dynamicresources` | DRA ResourceClaim 调度和绑定元数据。 |
| `topology` | topology-aware node subset 和 placement。 |
| `snapshot` | 调试快照。 |

## 本章检查点

读完后应该能回答：

- 为什么 plugin 在 `OpenSession` 时加载，而不是只在进程启动时加载？
- Queue 公平性通常由哪个插件提供？
- Kubernetes predicate 是通过哪个插件接入的？
- binpack/spread 节点打分来自哪里？
