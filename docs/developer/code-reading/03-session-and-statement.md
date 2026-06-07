# 03. Session 与 Statement

这一章是核心 scheduler 最重要的一部分。`Session` 表示单轮调度上下文，`Statement` 提供事务式试算能力。

## 先打开这些文件

| 概念 | 文件 |
| --- | --- |
| framework 接口 | [`pkg/scheduler/framework/interface.go`](../../../pkg/scheduler/framework/interface.go) |
| Session 生命周期 | [`pkg/scheduler/framework/framework.go`](../../../pkg/scheduler/framework/framework.go) |
| Session 状态和 helper | [`pkg/scheduler/framework/session.go`](../../../pkg/scheduler/framework/session.go) |
| 插件回调注册 | [`pkg/scheduler/framework/session_plugins.go`](../../../pkg/scheduler/framework/session_plugins.go) |
| Statement 事务模型 | [`pkg/scheduler/framework/statement.go`](../../../pkg/scheduler/framework/statement.go) |
| Operation 类型 | [`pkg/scheduler/framework/operations.go`](../../../pkg/scheduler/framework/operations.go) |

## Session 生命周期

```text
framework.OpenSession()
  -> cache.Snapshot()
  -> 创建 Session
  -> 根据 config.Tiers 加载 plugin builders
  -> plugin.OnSessionOpen(ssn)
     -> plugins 往 ssn 注册各种回调

actions 执行
  -> ssn.Statement()
  -> stmt.Allocate / Pipeline / Evict
  -> stmt.Rollback / Discard / Commit

framework.CloseSession()
  -> plugin.OnSessionClose(ssn)
  -> cache.RecordJobStatusEvent(job)
  -> 清理 session 内存状态
```

## `Session` 结构源码注释

源码：[`pkg/scheduler/framework/session.go`](../../../pkg/scheduler/framework/session.go)

```go
type Session struct {
    ID    string
    Cache cache.Cache

    // 本轮调度的点时间快照。
    ClusterInfo *api.ClusterInfo

    // 插件扩展点。Plugin 在 OnSessionOpen 里往这些 slice 追加函数。
    GpuOrderFns       []api.GpuOrderFn
    NodePreOrderFns   []api.NodePreOrderFn
    NodeOrderFns      []api.NodeOrderFn
    JobOrderFns       []common_info.CompareFn
    TaskOrderFns      []common_info.CompareFn
    QueueOrderFns     []api.CompareQueueFn
    PredicateFns      []api.PredicateFn
    PrePredicateFns   []api.PrePredicateFn

    // reclaim/preempt 相关策略。
    CanReclaimResourcesFns      []api.CanReclaimResourcesFn
    ReclaimVictimFilterFns      []api.VictimFilterFn
    PreemptVictimFilterFns      []api.VictimFilterFn
    ReclaimScenarioValidatorFns []api.ScenarioValidatorFn
    PreemptScenarioValidatorFns []api.ScenarioValidatorFn

    Config          *conf.SchedulerConfiguration
    plugins         map[string]Plugin
    eventHandlers   []*EventHandler
    SchedulerParams conf.SchedulerParams
}
```

阅读重点：`Session` 不是单纯的数据容器，它也是 action 和 plugin 的协作面。action 调用 `ssn.OrderedNodesByTask`、`ssn.PredicateFn`、`ssn.QueueOrderFn` 等方法时，实际是在调用插件注册进来的函数。

## 打开 Session

源码：[`pkg/scheduler/framework/framework.go`](../../../pkg/scheduler/framework/framework.go)

```go
func OpenSession(cache cache.Cache, config *conf.SchedulerConfiguration,
    schedulerParams *conf.SchedulerParams, sessionId string, mux *http.ServeMux) (*Session, error) {

    ssn, err := openSession(cache, sessionId, *schedulerParams, mux)
    ssn.Config = config

    // 每一轮 Session 都根据配置重新实例化插件。
    for _, tier := range config.Tiers {
        for _, pluginOption := range tier.Plugins {
            pb, found := GetPluginBuilder(pluginOption.Name)
            plugin := pb(pluginOption.Arguments)
            ssn.plugins[plugin.Name()] = plugin

            // 插件在这里把排序、过滤、配额、打分等函数挂到 Session 上。
            plugin.OnSessionOpen(ssn)
        }
    }

    return ssn, nil
}
```

阅读重点：plugin registry 是进程级的，但 plugin callback 是 session 级的。

## 插件回调模式

源码：[`pkg/scheduler/framework/session_plugins.go`](../../../pkg/scheduler/framework/session_plugins.go)

```go
func (ssn *Session) AddNodeOrderFn(nof api.NodeOrderFn) {
    ssn.NodeOrderFns = append(ssn.NodeOrderFns, nof)
}

func (ssn *Session) AddPredicateFn(pf api.PredicateFn) {
    ssn.PredicateFns = append(ssn.PredicateFns, pf)
}

func (ssn *Session) JobOrderFn(l, r interface{}) bool {
    for _, jof := range ssn.JobOrderFns {
        if j := jof(l, r); j != 0 {
            return j < 0
        }
    }

    // 没有插件给出排序时，使用创建时间和 UID 兜底，保证稳定顺序。
    ...
}
```

常见模式：

1. plugin 通过 `Add...Fn` 注册回调。
2. action 通过 `Session` 的聚合方法调用这些回调。
3. 排序类回调通常遇到第一个非 0 结果就返回。
4. 没有插件结果时，framework 提供稳定兜底逻辑。

## Statement 的目的

源码：[`pkg/scheduler/framework/statement.go`](../../../pkg/scheduler/framework/statement.go)

```go
type Statement struct {
    operations []Operation
    ssn        *Session
    sessionID  string
}
```

`Statement` 记录一组可逆操作：

- `Allocate`：在 session 内把 pending task 虚拟分配到节点。
- `Pipeline`：在 session 内把 task 放到即将释放的资源上。
- `Evict`：在 session 内把 task 标记为 releasing。
- `Undo`：记录 rollback 历史。

## Checkpoint 与 Rollback

```go
func (s *Statement) Checkpoint() Checkpoint {
    return Checkpoint(len(s.operations))
}

func (s *Statement) Rollback(cp Checkpoint) error {
    for i := len(s.operations) - 1; i >= int(cp); i-- {
        if err := s.undoOperation(i); err != nil {
            return err
        }
    }
    s.operations = s.operations[:cp]
    return nil
}
```

阅读重点：

- checkpoint 本质上是当前 operation 数量。
- rollback 从后往前撤销 checkpoint 之后的操作。
- 这让 action 可以尝试一个 node set，失败后回滚，再尝试另一个 node set。

## Allocate、Pipeline、Evict 的区别

```text
stmt.Allocate(task, node)
  # session 中 task 变成 Allocated，节点资源被占用。
  # Commit 后通过 cache.Bind/BindPod 创建 BindRequest。

stmt.Pipeline(task, node)
  # session 中 task 变成 Pipelined。
  # 表示资源还依赖 releasing 等状态，暂时不能真实 bind。

stmt.Evict(task, message, metadata)
  # session 中 task 变成 Releasing。
  # Commit 后通过 cache.Evict 触发 Kubernetes 侧 eviction/status 逻辑。
```

这个区别解释了为什么 preempt/reclaim 可以先驱逐 victim，再让新 job pipeline 到即将释放的资源上。

## Statement 的即时副作用

`Statement` 不是等到 `Commit()` 才修改状态。`Allocate/Pipeline/Evict` 调用当下就会修改 `Session.ClusterInfo` 中的 job、node、task，并同步触发 plugin event handler：

| 操作 | 立即改变 | event handler |
| --- | --- | --- |
| `Allocate` | task 变 `Allocated`，加入目标 node，node idle/used 视图变化 | `AllocateFunc` |
| `Pipeline` | task 变 `Pipelined`，记录目标 node，表示等待 releasing 资源 | `AllocateFunc` |
| `Evict` | task 变 `Releasing`，job/node 视图释放资源 | `DeallocateFunc` |
| `Rollback/Discard` | 逆序撤销操作 | 触发相反方向的 handler |

所以 `Statement` 的“事务性”指的是可以回滚 session 内的虚拟副作用，而不是完全延迟执行。`Commit()` 的职责是把还有效的操作外部化到 cache：创建 BindRequest、记录 pipeline，或真正发起 eviction。

## Commit 源码注释

```go
func (s *Statement) Commit() error {
    for i, op := range s.operations {
        if !s.operationValid(i) {
            continue
        }

        switch op.Name() {
        case evict:
            s.commitEvict(...)
        case pipeline:
            s.commitPipeline(...)
        case allocate:
            err = s.commitAllocate(...)
        }
    }

    s.clearOperations()
    return err
}
```

allocation 的提交路径：

```text
Statement.Commit()
  -> commitAllocate()
     -> ssn.BindPod()
        -> ssn.Cache.Bind()
           -> 创建 BindRequest
```

## 修改 action 前要记住

- 试探性决策应使用 `Statement`。
- 失败时必须 `Rollback` 或 `Discard`。
- action 不应该直接调用 Kubernetes API 做 binding。
- `Session.ClusterInfo` 是本轮可变模型，不是长期真实状态。
- plugin event handler 会响应虚拟 allocate/deallocate，所以 rollback 正确性很重要。

## 本章检查点

读完后应该能回答：

- scheduler 为什么可以模拟 preemption，而不是马上驱逐 Pod？
- plugin callback 是在哪里挂到 session 上的？
- `Allocate` 和 `Pipeline` 的差异是什么？
- 哪条调用链把 allocation 变成 `BindRequest`？
