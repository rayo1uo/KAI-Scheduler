# 05. Stale Gang Eviction Action 源码阅读

`stalegangeviction` 是一个清理型 action。它不参与 solver，不寻找 victim queue，也不创建 `Statement`，而是直接驱逐 stale gang job 中已经占用资源的 task。入口是 [`pkg/scheduler/actions/stalegangeviction/stalegangeviction.go`](../../../../pkg/scheduler/actions/stalegangeviction/stalegangeviction.go)。

## 主循环

```text
Execute
  -> for job in ssn.ClusterInfo.PodGroupInfos
       -> if job.IsStale()
             handleStaleJob(ssn, job)
          else
             handleNonStaleJob(job)
```

stale 的判定来自 `PodGroupInfo` 内部状态。这个 action 只负责在 stale 状态持续超过 grace period 后做驱逐。

## Grace period 逻辑

`handleStaleJob` 的前半段：

```text
if job.StalenessInfo.TimeStamp == nil
  -> 设置当前时间

defaultGracePeriod := ssn.GetGlobalDefaultStalenessGracePeriod()
if defaultGracePeriod < 0
  -> 不驱逐

if time.Since(timestamp) < defaultGracePeriod
  -> 不驱逐

job.StalenessInfo.Stale = true
```

这里有两个设计点：

- 第一次看到 stale job 时只记录时间，不马上驱逐。
- grace period 为负数表示关闭 stale eviction。

## 只驱逐 active allocated task

真正收集 victim 时只看 active allocated 状态：

```go
for _, task := range job.GetAllPodsMap() {
    if pod_status.IsActiveAllocatedStatus(task.Status) {
        tasksToEvict = append(tasksToEvict, task)
    }
}
```

这里的 active allocated 集合来自 [`pkg/scheduler/api/pod_status/pod_status.go`](../../../../pkg/scheduler/api/pod_status/pod_status.go)：`Allocated | Pipelined | Binding | Bound | Running`。也就是说 `Pipelined` 会被 stale gang eviction 收集并驱逐，因为 scheduler 已经为它选择了目标节点并把它计入 active allocated 语义。

不会被这个 action 驱逐的是 `Pending`、`Gated`、`Releasing`、`StuckInReleasing`、`Succeeded`、`Failed`、`Deleted` 等非 active allocated 状态；这些 task 要么还没有占用调度视图中的目标资源，要么已经在释放或终态中。

## 直接调用 `Session.Evict`

和其他 action 最大不同：

```go
for _, task := range tasksToEvict {
    reason := api.GetGangEvictionMessage(task, job)
    ssn.Evict(task, reason, evictionMetadata)
}
```

`ssn.Evict` 立即进入：

```text
Session.Evict
  -> Cache.Evict(...)
  -> updatePodOnSession(task, Releasing)
  -> updatePodOnNode(task)
  -> eventHandlers.DeallocateFunc(...)
```

所以 stale gang eviction 没有 `Commit/Discard` 语义，也不会被 solver 回滚。它是本轮调度末尾的状态清理动作。

## 非 stale job 的状态恢复

如果 job 本轮不再 stale：

```go
if job.StalenessInfo.TimeStamp != nil {
    job.StalenessInfo.TimeStamp = nil
    job.StalenessInfo.Stale = false
}
```

这避免一个 job 曾经 stale 后，即使恢复可调度状态，旧 timestamp 仍然导致下一次直接被驱逐。

## 建议测试阅读

- [`pkg/scheduler/actions/stalegangeviction/stalegangeviction_test.go`](../../../../pkg/scheduler/actions/stalegangeviction/stalegangeviction_test.go)：grace period、非 active task 不驱逐、stale 状态清理。
- [`pkg/scheduler/actions/integration_tests/stalegangeviction/stalegangeviction_test.go`](../../../../pkg/scheduler/actions/integration_tests/stalegangeviction/stalegangeviction_test.go)：集成层行为。
