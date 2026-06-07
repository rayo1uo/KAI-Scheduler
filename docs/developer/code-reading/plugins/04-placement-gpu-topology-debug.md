# 04. Placement、GPU、Topology 与调试插件

这一章关注“如果 task 已经可调度，scheduler 更倾向把它放在哪里”。这些插件大多通过 `NodeOrderFn`、`NodePreOrderFn`、`GpuOrderFn` 或 `SubsetNodesFn` 改变节点/GPU选择。

## `nodeplacement`

入口：[`pkg/scheduler/plugins/nodeplacement/nodeplacement.go`](../../../../pkg/scheduler/plugins/nodeplacement/nodeplacement.go)

注册：

```go
ssn.AddNodePreOrderFn(pp.nodePreOrderFn)
ssn.AddNodeOrderFn(pp.nodeOrderFn)
```

它根据 plugin arguments 为 CPU/GPU 分别选择 binpack 或 spread，默认都是 binpack：

```yaml
nodeplacement:
  arguments:
    cpu: binpack
    gpu: binpack
```

binpack 模式会先在 `NodePreOrderFn` 中计算候选节点剩余资源范围，再由 `NodeOrderFn` 打分。spread 模式直接按剩余比例打分。`task.IsCPUOnlyRequest()` 决定走 CPU 策略还是 GPU 策略。

`nodeplacement` 的最高单项分数是 `scores.MaxHighDensity = 9`。它是低尺度偏好，通常会被 `resourcetype`、`nodeavailability`、`topology`、`podaffinity`、`nominatednode` 等更高尺度分数压过。

建议配套读：

- [`pkg/scheduler/plugins/nodeplacement/pack.go`](../../../../pkg/scheduler/plugins/nodeplacement/pack.go)
- [`pkg/scheduler/plugins/nodeplacement/spread.go`](../../../../pkg/scheduler/plugins/nodeplacement/spread.go)
- [`pkg/scheduler/plugins/nodeplacement/nodepack_test.go`](../../../../pkg/scheduler/plugins/nodeplacement/nodepack_test.go)
- [`pkg/scheduler/plugins/nodeplacement/nodespread_test.go`](../../../../pkg/scheduler/plugins/nodeplacement/nodespread_test.go)

## `nodeavailability`

入口：[`pkg/scheduler/plugins/nodeavailability/nodeavailability.go`](../../../../pkg/scheduler/plugins/nodeavailability/nodeavailability.go)

注册：

```go
ssn.AddNodeOrderFn(...)
```

如果 node 当前 idle 资源已经足够真实分配 task，它给更高分。它的作用是偏向立即 bind，而不是偏向只能依赖 releasing 资源的 pipeline 节点。

这对减少“明明有空闲节点却先 pipeline 到正在释放的节点”很有用。

## `resourcetype`

入口：[`pkg/scheduler/plugins/resourcetype/resourcetype.go`](../../../../pkg/scheduler/plugins/resourcetype/resourcetype.go)

注册：

```go
ssn.AddNodeOrderFn(...)
```

CPU-only task 如果放到 CPU-only node 得分更高。设计目标是保护 GPU node，避免纯 CPU workload 占掉 GPU 机器上的 CPU/memory。

## `nominatednode`

入口：[`pkg/scheduler/plugins/nominatednode/nominatednode.go`](../../../../pkg/scheduler/plugins/nominatednode/nominatednode.go)

注册：

```go
ssn.AddNodeOrderFn(...)
```

如果 pod 的 `Status.NominatedNodeName` 等于当前 node，该 node 得高分。它让 KAI 能尊重 Kubernetes nominated node 语义，减少重复改变预期目标节点。

## `gpusharingorder`

入口：[`pkg/scheduler/plugins/gpusharingorder/gpusharingorder.go`](../../../../pkg/scheduler/plugins/gpusharingorder/gpusharingorder.go)

注册：

```go
ssn.AddNodeOrderFn(...)
```

它影响的是 node 选择，而不是具体 GPU group 选择。对于 shared GPU task，如果某个 node 上已有可复用的 shared GPU group，它会给 node 加分，倾向把共享任务放在已有共享上下文里。

## `gpupack`

入口：[`pkg/scheduler/plugins/gpupack/gpupack.go`](../../../../pkg/scheduler/plugins/gpupack/gpupack.go)

注册：

```go
ssn.AddGPUOrderFn(...)
```

shared GPU 分数与已使用比例相关，越满越优先。它倾向把 fractional/memory GPU task pack 到已有 GPU 上，减少碎片。whole GPU indicator 的分数是 `0`，shared GPU 的分数是当前 `usedGpuPortion`。

## `gpuspread`

入口：[`pkg/scheduler/plugins/gpuspread/gpuspread.go`](../../../../pkg/scheduler/plugins/gpuspread/gpuspread.go)

注册：

```go
ssn.AddGPUOrderFn(...)
```

shared GPU 分数倾向空闲 GPU，目标是 spread。whole GPU indicator 的分数是 `1`，shared GPU 的分数是 `1 - usedGpuPortion`。

该插件已注册，但 scheduler fallback config 未启用。operator 的 GPU spread 策略会启用它，并关闭 `gpupack/gpusharingorder`。启用它时要留意是否同时启用了 `gpupack`，因为 GPU 分数会相加。

## `topology`

入口：

- [`pkg/scheduler/plugins/topology/topology_plugin.go`](../../../../pkg/scheduler/plugins/topology/topology_plugin.go)
- [`pkg/scheduler/plugins/topology/job_filtering.go`](../../../../pkg/scheduler/plugins/topology/job_filtering.go)
- [`pkg/scheduler/plugins/topology/node_scoring.go`](../../../../pkg/scheduler/plugins/topology/node_scoring.go)

注册：

```go
ssn.AddSubsetNodesFn(t.subSetNodesFn)
ssn.AddNodeOrderFn(t.nodeOrderFn)
ssn.AddPreJobAllocationFn(t.preJobAllocationFn)
```

它有三层作用：

- `PreJobAllocationFn`：每个 job 分配前清空 subgroup node score 缓存。
- `SubsetNodesFn`：在 `allocateSubGroupSet/allocatePodSet` 中把候选节点切成 topology domain 子集。required topology 会在这里强约束候选范围。
- `NodeOrderFn`：preferred topology 会生成 node score，影响 domain 内节点排序。

因为 `SubsetNodesFn` 位于 subgroup/podset 递归层级，topology 对 gang/subgroup 的影响不是“最后给节点加点分”这么浅，而是会改变整个子树在哪些 node set 上尝试。

## `snapshot`

入口：[`pkg/scheduler/plugins/snapshot/snapshot.go`](../../../../pkg/scheduler/plugins/snapshot/snapshot.go)

注册：

```text
/get-snapshot
```

它不直接影响调度结果，而是导出 config、scheduler params、raw Kubernetes objects、discovery 信息等，通常用于复现和 snapshot-tool 调试。

## Placement 插件如何叠加

一次节点选择可能同时受到这些插件影响：

```text
OrderedNodesByTask
  -> NodePreOrderFn
       topology / nodeplacement / podaffinity ...
  -> NodeOrderFn 分数求和
       nodeavailability
       gpusharingorder
       resourcetype
       nominatednode
       nodeplacement
       topology
       podaffinity
  -> score desc
  -> FittingNode 再做资源和 predicate 硬过滤
```

因此排查“为什么选了这个 node”时，不能只看一个插件。要先看候选 node 是否被 topology/predicate 过滤，再看所有 node order 分数的叠加。

## 打分尺度速查

分数常量在 [`pkg/scheduler/plugins/scores/scores.go`](../../../../pkg/scheduler/plugins/scores/scores.go)：

| 常量 | 数值 | 常见来源 |
| --- | --- | --- |
| `NominatedNode` | `1000000` | `nominatednode` |
| `K8sPlugins` | `100000` | `podaffinity` |
| `Topology` | `10000` | `topology` |
| `GpuSharing` | `1000` | `gpusharingorder` |
| `Availability` | `100` | `nodeavailability` |
| `ResourceType` | `10` | `resourcetype` |
| `MaxHighDensity` | `9` | `nodeplacement` |

`NodeOrderFn` 是求和排序，但这些常量不是同一尺度。一个 nominated node 命中基本会压过 topology、availability、nodeplacement 等低尺度偏好；同理，topology preferred score 通常会比 nodeplacement 的 pack/spread 更强。

## 建议测试阅读

- [`pkg/scheduler/plugins/nodeavailability/nodeavailability_test.go`](../../../../pkg/scheduler/plugins/nodeavailability/nodeavailability_test.go)
- [`pkg/scheduler/plugins/resourcetype/resourcetype_test.go`](../../../../pkg/scheduler/plugins/resourcetype/resourcetype_test.go)
- [`pkg/scheduler/plugins/nominatednode/nominatednode_test.go`](../../../../pkg/scheduler/plugins/nominatednode/nominatednode_test.go)
- [`pkg/scheduler/plugins/gpupack/gpupack_test.go`](../../../../pkg/scheduler/plugins/gpupack/gpupack_test.go)
- [`pkg/scheduler/plugins/gpuspread/gpuspread_test.go`](../../../../pkg/scheduler/plugins/gpuspread/gpuspread_test.go)
- [`pkg/scheduler/plugins/topology/topology_plugin_test.go`](../../../../pkg/scheduler/plugins/topology/topology_plugin_test.go)
- [`pkg/scheduler/plugins/topology/job_filtering_test.go`](../../../../pkg/scheduler/plugins/topology/job_filtering_test.go)
- [`pkg/scheduler/plugins/snapshot/snapshot_test.go`](../../../../pkg/scheduler/plugins/snapshot/snapshot_test.go)
