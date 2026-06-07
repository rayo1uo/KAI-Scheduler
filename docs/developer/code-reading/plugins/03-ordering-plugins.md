# 03. Job、Task、SubGroup 排序插件

排序插件不直接创建 BindRequest，也不直接驱逐 Pod。它们通过 `JobOrderFn`、`TaskOrderFn`、`SubGroupOrderFn` 改变 action 处理顺序，从而影响最终谁先拿资源、一个 job 内哪个 pod 先放、哪个 subgroup 先满足。

## 排序回调的合并方式

排序函数返回 `int`：

- `< 0`：左边优先。
- `> 0`：右边优先。
- `0`：本插件不做决定，交给下一个排序插件。

`Session` 遇到第一个非 0 结果就停止。fallback 规则按回调类型不同：

- `JobOrderFn`：先比 PodGroup `CreationTimestamp`，再比 PodGroup UID。
- `TaskOrderFn`：先比 Pod `CreationTimestamp`，再比 Pod UID。
- `QueueOrderFn`：先比 Queue `CreationTimestamp`，再比 Queue UID。
- `SubGroupOrderFn`：按 subgroup/member name 字典序。

## `priority`

入口：[`pkg/scheduler/plugins/priority/priority.go`](../../../../pkg/scheduler/plugins/priority/priority.go)

注册：

```go
ssn.AddJobOrderFn(JobOrderFn)
```

比较逻辑：

```text
lv.Priority > rv.Priority -> lv 优先
lv.Priority < rv.Priority -> rv 优先
相等 -> 返回 0，交给下一个插件或 fallback
```

影响位置：

- pending job queue：高 priority job 先被 allocate/reclaim/preempt/consolidation 尝试。
- victim queue：因为 `VictimQueue` 会反向使用 JobOrder，高 priority victim 更不容易先被选中。

## `elastic`

入口：[`pkg/scheduler/plugins/elastic/elastic.go`](../../../../pkg/scheduler/plugins/elastic/elastic.go)

注册：

```go
ssn.AddJobOrderFn(JobOrderFn)
```

它比较 job 当前是否低于、等于或高于各 subgroup 的 `minAvailable`：

- 低于 `minAvailable` 的 job 优先。
- 已经刚好满足 `minAvailable` 的 job 优先于超过 `minAvailable` 的 job。
- 都属于同一状态时返回 0。

这让 elastic/gang job 更倾向先达到最低可运行规模，而不是让已经有额外 task 的 job 持续扩张。

## `kubeflow`

入口：[`pkg/scheduler/plugins/kubeflow/kubeflow.go`](../../../../pkg/scheduler/plugins/kubeflow/kubeflow.go)

注册：

```go
ssn.AddTaskOrderFn(...)
```

它识别 Kubeflow 训练任务中的 master/launcher 角色，让这些 task 在 worker 之前调度。这样可以减少 worker 已经占资源但 master 不可用的场景。

建议配套读 [`pkg/scheduler/plugins/kubeflow/kubeflow_test.go`](../../../../pkg/scheduler/plugins/kubeflow/kubeflow_test.go)。

## `ray`

入口：[`pkg/scheduler/plugins/ray/ray.go`](../../../../pkg/scheduler/plugins/ray/ray.go)

注册：

```go
ssn.AddTaskOrderFn(...)
```

它识别 Ray head pod，让 head 在 worker 前调度。Ray workload 的启动顺序对可用性很敏感，这个插件把 workload 语义转成 scheduler 内部 task order。

建议配套读 [`pkg/scheduler/plugins/ray/ray_test.go`](../../../../pkg/scheduler/plugins/ray/ray_test.go)。

## `taskorder`

入口：[`pkg/scheduler/plugins/taskorder/task_order.go`](../../../../pkg/scheduler/plugins/taskorder/task_order.go)

注册：

```go
ssn.AddTaskOrderFn(...)
```

它读取 KAI task order label，数值越大越优先。具体规则：

- 有 label 的 task 优先于没有 label 的 task。
- 两边都没有 label 时返回 0，交给后续插件或 fallback。
- 左侧 label 解析失败时返回 `1`，即左侧排到右侧之后。
- 右侧 label 解析失败时返回 `-1`，即左侧排到右侧之前。
- 两边都能解析时，数值更大者优先。

因此“非法值降级为默认顺序”并不准确。非法值会在这个插件内产生确定的相对顺序。

建议配套读 [`pkg/scheduler/plugins/taskorder/task_order_test.go`](../../../../pkg/scheduler/plugins/taskorder/task_order_test.go)。

## `subgrouporder`

入口：[`pkg/scheduler/plugins/subgrouporder/subgroup_order.go`](../../../../pkg/scheduler/plugins/subgrouporder/subgroup_order.go)

注册：

```go
ssn.AddSubGroupOrderFn(...)
```

它影响 `GetTasksToAllocate` 和 `common.allocateMembersOnNodes` 的 subgroup/podset 遍历顺序。精确规则在 `orderByAllocationRatio`：

- 两边都低于 threshold/minAvailable 时返回 0，交给 fallback 或保持同级后续比较。
- 只有一边低于 threshold/minAvailable 时，低于阈值的一边优先。
- 两边都已满足时，required subgroup 或 podset，也就是 threshold 大于 0，优先于 optional。
- 两边都是 optional 时，active allocated 更少的一边优先。
- 两边都是 required 且已满足时，allocated/threshold 比例更低的一边优先。

这对分布式训练的角色组很重要。例如某个 worker subgroup 尚未满足最低数量时，scheduler 会优先尝试它，而不是先调度可选扩展 pod。

建议配套读 [`pkg/scheduler/plugins/subgrouporder/subgroup_order_test.go`](../../../../pkg/scheduler/plugins/subgrouporder/subgroup_order_test.go)。

## `reflectjoborder`

入口：[`pkg/scheduler/plugins/reflectjoborder/reflect_job_order.go`](../../../../pkg/scheduler/plugins/reflectjoborder/reflect_job_order.go)

这个插件默认未启用，且不直接改变调度决策。它在 session open 时计算一次 `JobsOrderByQueues`，并注册 HTTP handler：

```text
/get-job-order
```

用途是调试当前 session 的 job order：哪个 queue 先出队、queue 内哪个 job 先出队。注意源码里注册 key 是 `reflectjoborder`，但 `Name()` 返回的是 `joborder`，读配置和日志时要留意这个差异。

## 排序插件的阅读顺序建议

1. 先看 [`pkg/scheduler/framework/session_plugins.go`](../../../../pkg/scheduler/framework/session_plugins.go) 的 `JobOrderFn/TaskOrderFn/SubGroupOrderFn`，理解“第一个非 0 结果获胜”。
2. 再看 [`pkg/scheduler/actions/utils/job_order_by_queue.go`](../../../../pkg/scheduler/actions/utils/job_order_by_queue.go)，理解 job order 如何被 Queue 树使用。
3. 最后按 workload 类型看插件：普通 priority、elastic、Kubeflow、Ray、task label、subgroup。
