# Plugins 深读地图

Plugins 是 scheduler 的策略层。Action 负责“这一阶段要做什么”，Plugin 通过 `Session` 回调决定“按什么顺序、哪些节点可用、哪个 victim 可被驱逐、哪个 scenario 合法、BindRequest 需要附带什么信息”。

## 注册与启用

默认插件 builder 注册在 [`pkg/scheduler/plugins/factory.go`](../../../../pkg/scheduler/plugins/factory.go)。注册只是让 framework 认识插件；真正启用由 scheduler config 的 `tiers.plugins` 决定。

这里要区分两个“默认”来源：

- scheduler 二进制在没有外部配置时使用 fallback config，来源是 [`pkg/scheduler/conf_util/scheduler_conf_util.go`](../../../../pkg/scheduler/conf_util/scheduler_conf_util.go)。
- operator 生成 `SchedulingShard` 配置时会调用 [`pkg/apis/kai/v1/schedulingshard_types.go`](../../../../pkg/apis/kai/v1/schedulingshard_types.go) 的 defaults，并按 plugin priority 写入 scheduler ConfigMap。

fallback config 默认启用的插件包括：

```text
predicates, proportion, priority, elastic, kubeflow, ray,
nodeavailability, gpusharingorder, gpupack, resourcetype,
subgrouporder, taskorder, nominatednode, dynamicresources,
nodeplacement, minruntime, topology, snapshot
```

在 operator 的 `SchedulingShard` defaults 中，`podaffinity` 也默认启用，并带有显式 priority。GPU placement 相关插件会随 `PlacementStrategy.GPU` 切换：

- GPU binpack：启用 `gpusharingorder` 和 `gpupack`，关闭 `gpuspread`。
- GPU spread：启用 `gpuspread`，关闭 `gpusharingorder` 和 `gpupack`。

`reflectjoborder` 已注册，但不在 fallback config 和 `SchedulingShard` 内置默认插件列表中，需要显式启用。

## 章节

| 章节 | 重点 |
| --- | --- |
| [00. Session 回调机制](00-session-callbacks.md) | `Add*Fn` 如何注册，action 在哪里调用，多插件结果如何合并 |
| [01. 约束与 Predicate 插件](01-constraints-and-predicates.md) | `predicates`、`dynamicresources`、`podaffinity` 如何让 task/node 不可调度 |
| [02. 公平性、Quota 与 Victim 插件](02-fairness-quota-and-victims.md) | `proportion`、`minruntime` 如何影响 queue 排序、capacity、reclaim/preempt |
| [03. Job、Task、SubGroup 排序插件](03-ordering-plugins.md) | `priority`、`elastic`、`kubeflow`、`ray`、`taskorder`、`subgrouporder`、`reflectjoborder` |
| [04. Placement、GPU、Topology 与调试插件](04-placement-gpu-topology-debug.md) | `nodeavailability`、`nodeplacement`、`resourcetype`、`nominatednode`、`gpusharingorder`、`gpupack`、`gpuspread`、`topology`、`snapshot` |

## 逐插件速查

| 插件 | 注册回调 | 主要影响 |
| --- | --- | --- |
| `predicates` | `PrePredicateFn`、`VictimInvariantPrePredicateFn`、`PredicateFn` | 接入 Kubernetes 风格 pre-filter/filter、volume、node affinity、storage、node pool 等硬约束 |
| `proportion` | `QueueOrderFn`、`CanReclaimResourcesFn`、`ReclaimScenarioValidatorFn`、capacity checks、event handler、queue resource getters | Queue 公平性、quota/capacity、reclaim 合法性、queue 资源状态 |
| `priority` | `JobOrderFn` | 高 priority PodGroup 先调度，也影响 victim queue 的反向排序 |
| `elastic` | `JobOrderFn` | 优先让低于 `minAvailable` 的 elastic/gang job 达标 |
| `kubeflow` | `TaskOrderFn` | Kubeflow master/launcher task 优先 |
| `ray` | `TaskOrderFn` | Ray head task 优先 |
| `nodeavailability` | `NodeOrderFn` | 当前 idle 资源足够的 node 得分更高，倾向立即 bind |
| `gpusharingorder` | `NodeOrderFn` | shared GPU task 倾向放到已有共享 GPU group 的 node |
| `gpupack` | `GpuOrderFn` | shared GPU 越满分越高，倾向 pack |
| `gpuspread` | `GpuOrderFn` | shared GPU 越空分越高，倾向 spread |
| `resourcetype` | `NodeOrderFn` | CPU-only task 优先 CPU-only node，避免占用 GPU node |
| `subgrouporder` | `SubGroupOrderFn` | 未满足 threshold/minAvailable 的 subgroup 优先 |
| `taskorder` | `TaskOrderFn` | 根据 task order label 决定 pod 分配顺序 |
| `nominatednode` | `NodeOrderFn` | Pod 的 `status.nominatedNodeName` 对应 node 得高分 |
| `dynamicresources` | `PrePredicateFn`、`PredicateFn`、event handler | Kubernetes DRA ResourceClaim 调度、分配状态维护和 BindRequest 数据 |
| `nodeplacement` | `NodePreOrderFn`、`NodeOrderFn` | CPU/GPU binpack 或 spread 节点打分 |
| `minruntime` | reclaim/preempt victim filters、scenario validators | 保护运行时间不足的 victim，尤其处理 elastic job 的 minAvailable |
| `topology` | `SubsetNodesFn`、`NodeOrderFn`、`PreJobAllocationFn` | topology domain 过滤、preferred topology 节点打分、subgroup score 缓存 |
| `snapshot` | HTTP handler | 导出调度快照，辅助复现 |
| `reflectjoborder` | HTTP handler | 暴露本轮 job order，默认未启用 |
| `podaffinity` | `NodePreOrderFn`、`NodeOrderFn` | 复用 K8s pod affinity score，fallback config 未启用，operator defaults 启用 |

## 先读哪几个

调度不上，先读 `predicates` 和 `proportion`。它们最常让 job 被过滤或被 quota/capacity 拦住。

节点选择不符合预期，先读 `nodeplacement`、`nodeavailability`、`resourcetype`、`nominatednode`。

shared GPU 行为不符合预期，读 `gpusharingorder`、`gpupack/gpuspread`，再看 `gpu_sharing` 包。

跨 queue reclaim 或同 queue preempt 不符合预期，读 `proportion`、`minruntime`，再回到 actions 的 [reclaim](../actions/02-reclaim.md) 和 [preempt](../actions/03-preempt.md)。
