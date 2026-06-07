# 01. 约束与 Predicate 插件

这一章关注“为什么一个 task/node 组合不可调度”。核心插件是 `predicates`，另外 `dynamicresources` 和 `podaffinity` 也会通过 pre-predicate、predicate 或 node score 影响结果。

## `predicates`

入口：[`pkg/scheduler/plugins/predicates/predicates.go`](../../../../pkg/scheduler/plugins/predicates/predicates.go)

`OnSessionOpen` 注册三类回调：

```go
ssn.AddPrePredicateFn(func(task, job) error {
    return pp.evaluateTaskOnPrePredicate(task, k8sPredicates)
})

ssn.AddVictimInvariantPrePredicateFn(func(task) *api.VictimInvariantPrePredicateFailure {
    return pp.evaluateTaskOnVictimInvariantPrePredicates(task, k8sPredicates)
})

ssn.AddPredicateFn(func(task, job, node) error {
    return pp.evaluateTaskOnPredicates(task, job, node, k8sPredicates, ...)
})
```

它的职责是把 KAI scheduler 的 `Session` 模型接到 Kubernetes scheduler 风格的 pre-filter/filter 上：

- `PrePredicateFn` 在节点循环前执行，适合做只依赖 task 或全局状态的检查。
- `VictimInvariantPrePredicateFn` 在 reclaim/preempt/consolidation 进入 solver 前执行，适合跳过“驱逐谁都无效”的 job。
- `PredicateFn` 在 `Session.FittingNode` 中执行，检查具体 task/node 组合。

阅读时重点看三个函数：

- `evaluateTaskOnPrePredicate`
- `evaluateTaskOnVictimInvariantPrePredicates`
- `evaluateTaskOnPredicates`

其中 pre-predicate cache 只缓存 victim-invariant 候选，避免 reclaim/preempt 大量重复试算时反复跑相同的 pre-filter。

## `predicates` 失败如何反馈

`allocateTask` 中如果 `PrePredicateFn` 失败，会把错误写到 task fit errors：

```text
PrePredicateFn failed
  -> common_info.NewFitErrors()
  -> job.AddTaskFitErrors(task, fitErrors)
  -> allocateTask 返回 false
```

`FittingNode` 中如果 `PredicateFn` 失败，错误按 node 记录：

```text
PredicateFn failed on node
  -> fitErrors.SetNodeError(node.Name, err)
  -> job.AddTaskFitErrors(task, fitErrors)
```

这些 fit errors 最终会在 `CloseSession()` 的 `Cache.RecordJobStatusEvent(job)` 中变成 Pod/PodGroup 事件或 scheduling condition。

## `dynamicresources`

入口：[`pkg/scheduler/plugins/dynamicresources/dynamicresources.go`](../../../../pkg/scheduler/plugins/dynamicresources/dynamicresources.go)

这个插件支持 Kubernetes DRA。它通常影响三处：

- `PrePredicateFn`：检查 feature gate、ResourceClaim、DeviceClass、queue label 等前置条件。
- `PredicateFn`：在某个 node 上尝试为 ResourceClaim 找到可用 device allocation。
- event handler：在虚拟 allocate/pipeline/evict/rollback 时维护 claim allocation 和 reservedFor 状态。

这意味着 DRA 不只是一个静态 predicate。solver 在模拟 victim 和 pending job 时，也会让 dynamicresources 跟着更新虚拟 claim 状态，防止“模拟可行，真实绑定时 claim 不一致”。

DRA 对 BindRequest 的影响不是通过 `BindRequestMutateFn` 完成的。`dynamicresources` 的 event handler 会在 session 内维护 `task.ResourceClaimInfo`，而 [`pkg/scheduler/cache/cache.go`](../../../../pkg/scheduler/cache/cache.go) 的 `createBindRequest` 会把 `podInfo.ResourceClaimInfo.ToSlice()` 写进 `Spec.ResourceClaimAllocations`。Binder 侧再由 DRA pre-bind 逻辑消费这份分配结果。

建议配套读：

- [`pkg/scheduler/plugins/dynamicresources/dynamicresources_test.go`](../../../../pkg/scheduler/plugins/dynamicresources/dynamicresources_test.go)
- [`pkg/scheduler/plugins/dynamicresources/dynamicresources_statement_test.go`](../../../../pkg/scheduler/plugins/dynamicresources/dynamicresources_statement_test.go)
- [`pkg/binder/plugins/k8s-plugins/dynamicresources/dra.go`](../../../../pkg/binder/plugins/k8s-plugins/dynamicresources/dra.go)

## `podaffinity`

入口：[`pkg/scheduler/plugins/podaffinity/podaffinity.go`](../../../../pkg/scheduler/plugins/podaffinity/podaffinity.go)

`podaffinity` 已注册。它在 scheduler fallback config 中未启用，但 operator `SchedulingShard` defaults 中默认启用。它注册的是打分回调：

- `NodePreOrderFn`：复用 Kubernetes pod affinity score plugin 的 pre-score，必要时标记 skip。
- `NodeOrderFn`：返回 K8s affinity score 乘以 KAI 权重。

它通常不让节点“不可调度”，而是改变节点排序。真正的 required pod affinity/anti-affinity 硬约束仍主要通过 `predicates` 进入 `PredicateFn`。

## 读代码时的判断方法

遇到“为什么 Pod 没调度上”时，可以按这个顺序查：

1. PodGroup 是否 ready，否则 action 初始化时就被 `FilterUnready` 排除。
2. `PrePredicateFn` 是否失败，这会让 task 不进入节点排序。
3. Queue capacity 是否失败，这通常来自 `proportion`。
4. `FittingNode` 是否资源不足。
5. `PredicateFn` 是否对每个 node 都失败。
6. 如果是 DRA，确认 statement event handler 和 binder DRA plugin 是否都处理了同一份 claim 信息。
