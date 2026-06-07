# 00. Session 回调机制

`Session` 是 action 和 plugin 的协作面。插件在 `OnSessionOpen` 中调用 `ssn.Add*Fn(...)` 注册策略函数；action 不直接依赖具体插件，只调用 `Session` 的聚合方法。

## 生命周期

```text
framework.OpenSession
  -> cache.Snapshot()
  -> 创建 Session
  -> 根据 config.Tiers 逐个构造 plugin
  -> plugin.OnSessionOpen(ssn)
       -> ssn.AddJobOrderFn(...)
       -> ssn.AddPredicateFn(...)
       -> ssn.AddNodeOrderFn(...)
       -> ...

actions 执行
  -> 调用 ssn.JobOrderFn / PredicateFn / NodeOrderFn / ...

framework.CloseSession
  -> plugin.OnSessionClose(ssn)
  -> Cache.RecordJobStatusEvent(job)
```

入口文件：

- [`pkg/scheduler/framework/framework.go`](../../../../pkg/scheduler/framework/framework.go)
- [`pkg/scheduler/framework/session.go`](../../../../pkg/scheduler/framework/session.go)
- [`pkg/scheduler/framework/session_plugins.go`](../../../../pkg/scheduler/framework/session_plugins.go)

## 回调合并规则

不同类型回调的合并方式不同。

排序类回调：第一个给出非 0 比较结果的插件获胜。

```go
func (ssn *Session) JobOrderFn(l, r interface{}) bool {
    for _, jof := range ssn.JobOrderFns {
        if j := jof(l, r); j != 0 {
            return j < 0
        }
    }
    return fallbackByCreationTimestampAndUID(...)
}
```

过滤类回调：任一插件失败就失败。

```go
func (ssn *Session) PredicateFn(task, job, node) error {
    for _, pfn := range ssn.PredicateFns {
        if err := pfn(task, job, node); err != nil {
            return err
        }
    }
    return nil
}
```

打分类回调：多个插件分数相加。

```go
func (ssn *Session) NodeOrderFn(task, node) (float64, error) {
    score := 0.0
    for _, nodeOrderFn := range ssn.NodeOrderFns {
        pluginScore, err := nodeOrderFn(task, node)
        score += pluginScore
    }
    return score, nil
}
```

资源/资格类回调：很多函数只取第一个注册者的结果，例如 `CanReclaimResources`、capacity check、queue resource getters。默认配置里这些通常由 `proportion` 提供。

几个容易漏掉的聚合细节：

- `NodePreOrderFn` 的错误只会被记录日志，后续 pre-order 插件和调度流程仍继续。
- `SubsetNodesFn` 是串行扩展 node set。每个插件接收上一批 node sets，并为每个 node set 返回新的 subsets。
- `BindRequestMutateFn` 返回 annotations map，多个插件的结果用 `maps.Copy` 合并，后注册插件的同名 key 会覆盖先注册插件。当前源码没有内置 scheduler plugin 注册这个回调。
- `CanReclaimResources`、capacity checks、queue resource getters 都是“取第一个注册函数”的模式，不是全部求与或求和。
- reclaim/preempt victim filters 和 scenario validators 是“全部必须通过”。任一插件返回 false，victim 或 scenario 就会被拒绝。
- `NodeOrderFn` 和 `GpuOrderFn` 是分数求和。分数尺度由插件自己决定，所以高权重插件会压过低权重插件。

## 回调到 action 的调用点

| 回调 | 典型调用位置 | 行为 |
| --- | --- | --- |
| `QueueOrderFn` | `JobsOrderByQueues.buildNodeOrderFn` | 多级 Queue 树中哪个 queue/subtree 先出队 |
| `JobOrderFn` | leaf queue priority queue | 同 queue 内哪个 job 先出队；victim queue 中会反向使用 |
| `SubGroupOrderFn` | `GetTasksToAllocate`、`orderedMembers` | subgroup/podset 遍历顺序 |
| `TaskOrderFn` | `GetTasksToAllocate` | PodGroup 内 task 顺序 |
| `PreJobAllocationFn` | `common.AllocateJob` 开始 | job 级准备，例如清空 topology score 缓存 |
| `SubsetNodesFn` | `allocateSubGroupSet`、`allocatePodSet` | 把候选节点切成 topology/domain 子集 |
| `PrePredicateFn` | `allocateTask` | task 级预检查，失败则不进入 node loop |
| `VictimInvariantPrePredicateFn` | reclaim/preempt/consolidation 进入 solver 前 | 不依赖 victim 的提前失败 |
| `PredicateFn` | `Session.FittingNode` | task/node 组合检查 |
| `NodePreOrderFn` | `OrderedNodesByTask` 打分前 | 节点打分准备状态 |
| `NodeOrderFn` | `OrderedNodesByTask` | 节点分数，所有插件分数相加 |
| `GpuOrderFn` | `Session.FittingGPUs` | shared GPU/whole GPU 的选择顺序 |
| reclaim/preempt victim filters | victim queue 构造 | 哪些 running job 可以成为 victim |
| reclaim/preempt scenario validators | `byPodSolver.handleScenarioSolution` | 当前 victim 组合是否可提交 |
| `BindRequestMutateFn` | `Session.BindPod` | 创建 BindRequest 前注入 annotations；当前没有内置 scheduler plugin 注册 |
| `EventHandler` | `Statement.Allocate/Pipeline/Evict/Rollback`、`Session.Evict` | 插件同步本轮虚拟资源状态 |
| HTTP handler | `AddHttpHandler` | 调试接口，如 snapshot/job order |

## 节点排序和过滤的实际顺序

`allocateTask` 中的顺序是：

```text
ssn.PrePredicateFn(task, job)
orderedNodes := ssn.OrderedNodesByTask(nodes, task)
for node in orderedNodes:
  ssn.FittingNode(task, node, writeFittingDelta)
  allocateTaskToNode(...)
```

`OrderedNodesByTask` 内部先跑 `NodePreOrderFn`，再并发计算每个 node 的 `NodeOrderFn` 分数，最后按分数降序排列。

`FittingNode` 内部先检查 node 的 idle/releasing 资源是否可能满足 task，再跑 `PredicateFn`。这意味着节点打分可能给某个节点高分，但 predicate 仍然可以让它失败。

如果 `NodePreOrderFn` 失败，`Session.NodePreOrderFn` 只记录错误，不会中止 `OrderedNodesByTask`。所以 pre-order 适合准备打分所需的缓存或 K8s plugin pre-score 状态，但不能被当成硬过滤使用。

## GPU 排序的实际顺序

`Session.FittingGPUs` 先收集能满足 task 的 shared GPU group 和 whole GPU indicator，再对每个候选调用 `GpuOrderFn`：

```text
filterGpusByEnoughResources
  -> shared GPU groups with enough memory/fraction
  -> idle/releasing whole GPUs
sortGPUs
  -> sum all GpuOrderFn scores
  -> score desc
```

`gpupack` 和 `gpuspread` 不能同时作为“唯一策略”理解；如果同时启用，它们的分数会相加，最终行为取决于分数设计和启用顺序。

## EventHandler 为什么重要

`Statement` 的虚拟变更会触发 event handler，让插件内部状态跟着 session 变化。例如：

- `proportion` 用它维护 queue allocated/deserved/fairshare 的模拟视图。
- `dynamicresources` 用它维护 DRA claim allocation/reservedFor 的模拟状态。

因此 rollback/discard 的正确性不仅影响 `Session.ClusterInfo`，也影响插件内部状态。修改 action 或 statement 时要同时关注 event handler 是否被成对调用。
