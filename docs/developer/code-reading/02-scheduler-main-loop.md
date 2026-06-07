# 02. Scheduler 主循环

这一章沿着 scheduler 二进制从启动到单轮调度周期完整走一遍。

## 先打开这些文件

| 步骤 | 文件 |
| --- | --- |
| 二进制入口 | [`cmd/scheduler/main.go`](../../../cmd/scheduler/main.go) |
| server 启动 | [`cmd/scheduler/app/server.go`](../../../cmd/scheduler/app/server.go) |
| CLI options | [`cmd/scheduler/app/options/options.go`](../../../cmd/scheduler/app/options/options.go) |
| Scheduler 对象和周期 | [`pkg/scheduler/scheduler.go`](../../../pkg/scheduler/scheduler.go) |
| 默认调度配置 | [`pkg/scheduler/conf_util/scheduler_conf_util.go`](../../../pkg/scheduler/conf_util/scheduler_conf_util.go) |
| Scheduler cache | [`pkg/scheduler/cache/cache.go`](../../../pkg/scheduler/cache/cache.go) |
| Cluster snapshot | [`pkg/scheduler/cache/cluster_info/cluster_info.go`](../../../pkg/scheduler/cache/cluster_info/cluster_info.go) |

## 启动调用链

```text
cmd/scheduler/main.go
  -> app.RunApp()
     -> options.NewServerOption()
     -> setupProfiling / setupLogging / setConfig
     -> clientconfig.GetConfigOrDie()
     -> Run(options, restConfig, mux)
        -> metrics.InitMetrics()
        -> actions.InitDefaultActions()
        -> plugins.InitDefaultPlugins()
        -> conf_util.ResolveConfigurationFromFile()
        -> scheduler.NewScheduler()
        -> leader election
        -> scheduler.Run()
```

## `RunApp` 源码注释

源码：[`cmd/scheduler/app/server.go`](../../../cmd/scheduler/app/server.go)

```go
func RunApp() error {
    so := options.NewServerOption()
    so.AddFlags(pflag.CommandLine)

    // 读取和校验命令行参数。后续会被 BuildSchedulerParams 转成 SchedulerParams。
    if err := so.ValidateOptions(); err != nil {
        ...
    }

    // 插件可以通过这个 mux 注册 HTTP handler。
    mux := http.NewServeMux()
    go func() {
        _ = http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", so.PluginServerPort), mux)
    }()

    setupProfiling(so)
    setupLogging(so)
    setConfig(so)

    // Kubernetes REST config，会设置 QPS/Burst。
    config := clientconfig.GetConfigOrDie()
    config.QPS = float32(so.QPS)
    config.Burst = so.Burst

    return Run(so, config, mux)
}
```

阅读重点：`RunApp` 主要负责进程级初始化，真正的调度在 `Run` 和 `Scheduler.Run` 里。

## `Run` 源码注释

源码：[`cmd/scheduler/app/server.go`](../../../cmd/scheduler/app/server.go)

```go
func Run(opt *options.ServerOption, config *restclient.Config, mux *http.ServeMux) error {
    metrics.InitMetrics(opt.MetricsNamespace)

    // 这里只是注册默认 actions/plugins 到全局 registry。
    // 实际启用哪些 action/plugin 由 scheduler configuration 决定。
    actions.InitDefaultActions()
    plugins.InitDefaultPlugins()

    // 没传 scheduler-conf 时，会使用 conf_util 里的 defaultSchedulerConf。
    schedConfig, err := conf_util.ResolveConfigurationFromFile(opt.SchedulerConf)

    scheduler, err := scheduler.NewScheduler(
        config,
        schedConfig,
        BuildSchedulerParams(opt),
        mux,
    )

    // 关闭 leader election 时直接运行。
    // 开启 leader election 时，只有 leader 会调用 scheduler.Run。
    run := func(ctx context.Context) {
        scheduler.Run(ctx.Done())
        <-ctx.Done()
    }
    ...
}
```

`BuildSchedulerParams` 很值得一起读，它把 CLI option 转成 scheduler/cache/framework 使用的运行参数，比如 scheduler name、node pool、CSI storage、fairness、status worker 数量等。

## 默认 scheduler config

源码：[`pkg/scheduler/conf_util/scheduler_conf_util.go`](../../../pkg/scheduler/conf_util/scheduler_conf_util.go)

```yaml
actions: "allocate, consolidation, reclaim, preempt, stalegangeviction"
tiers:
- plugins:
  - name: predicates
  - name: proportion
  - name: priority
  - name: elastic
  - name: kubeflow
  - name: ray
  - name: nodeavailability
  - name: gpusharingorder
  - name: gpupack
  - name: resourcetype
  - name: subgrouporder
  - name: taskorder
  - name: nominatednode
  - name: dynamicresources
  - name: nodeplacement
    arguments:
      cpu: binpack
      gpu: binpack
  - name: minruntime
  - name: topology
  - name: snapshot
```

这个配置回答两个问题：

- 每轮执行哪些 `Action`，顺序是什么？
- 每轮打开 `Session` 时加载哪些 `Plugin`？

## `NewScheduler` 源码注释

源码：[`pkg/scheduler/scheduler.go`](../../../pkg/scheduler/scheduler.go)

```go
func NewScheduler(
    config *rest.Config,
    schedulerConf *conf.SchedulerConfiguration,
    schedulerParams *conf.SchedulerParams,
    mux *http.ServeMux,
) (*Scheduler, error) {
    kubeClient, kubeAiSchedulerClient := newClients(config)

    // NRT、discovery、usage DB 都是为了补充调度输入。
    nrtClient, _ := nrtclientset.NewForConfig(config)
    discoveryClient, _ := discovery.NewDiscoveryClientForConfig(config)
    usageDBClient, _ := getUsageDBClient(schedulerConf.UsageDBConfig)

    schedulerCacheParams := &schedcache.SchedulerCacheParams{
        KubeClient:         kubeClient,
        KAISchedulerClient: kubeAiSchedulerClient,
        NRTClient:          nrtClient,
        SchedulerName:      schedulerParams.SchedulerName,
        ...
    }

    return &Scheduler{
        config:          schedulerConf,
        schedulerParams: schedulerParams,
        cache:           schedcache.New(schedulerCacheParams),
        schedulePeriod:  schedulerParams.SchedulePeriod,
        mux:             mux,
    }, nil
}
```

阅读重点：`Scheduler` 自身很小，长期状态主要在 `cache` 里。

## 调度周期源码注释

源码：[`pkg/scheduler/scheduler.go`](../../../pkg/scheduler/scheduler.go)

```go
func (s *Scheduler) Run(stopCh <-chan struct{}) {
    // 启动 informer 和后台 status worker。
    s.cache.Run(stopCh)
    s.cache.WaitForCacheSync(stopCh)

    // 周期性执行 runOnce。
    go func() {
        wait.Until(s.runOnce, s.schedulePeriod, stopCh)
    }()
}

func (s *Scheduler) runOnce() {
    sessionId := generateSessionID(6)

    // 每轮调度先打开一个 Session，里面包含本轮 snapshot 和 plugin callbacks。
    ssn, err := framework.OpenSession(s.cache, s.config, s.schedulerParams, sessionId, s.mux)
    if err != nil {
        return
    }
    defer framework.CloseSession(ssn)

    // action 列表从配置解析。
    actions, _ := conf_util.GetActionsFromConfig(s.config)
    for _, action := range actions {
        action.Execute(ssn)
    }
}
```

这是核心循环。仓库里很多代码要么在为这个循环准备输入，要么在执行这个循环输出的结果。

## Cache 负责什么

源码：[`pkg/scheduler/cache/cache.go`](../../../pkg/scheduler/cache/cache.go)

```go
func (sc *SchedulerCache) Run(stopCh <-chan struct{}) {
    // Kubernetes 内置资源：Pod、Node、PVC 等。
    sc.informerFactory.Start(stopCh)

    // KAI 资源：PodGroup、Queue、BindRequest 等。
    sc.kubeAiSchedulerInformerFactory.Start(stopCh)

    // 可选拓扑资源。
    if sc.nrtInformerFactory != nil {
        sc.nrtInformerFactory.Start(stopCh)
    }

    // 异步更新 PodGroup/status/event。
    sc.StatusUpdater.Run(stopCh)
}

func (sc *SchedulerCache) Snapshot() (*api.ClusterInfo, error) {
    // 构造本轮调度使用的 ClusterInfo。
    snapshot, err := sc.clusterInfo.Snapshot()

    // 顺手清理 stale BindRequest。
    sc.cleanStaleBindRequest(...)
    return snapshot, err
}
```

cache 的主要职责：

- 启动和同步 informer。
- 生成本轮调度 snapshot。
- 创建 `BindRequest`。
- 执行 Pod eviction。
- 记录 PodGroup status/event。
- 持有 Kubernetes 内置调度插件需要的共享状态。

## 本章检查点

读完后应该能回答：

- `server.go` 和 `scheduler.go` 的职责怎么分？
- 默认 actions/plugins 在哪里注册？
- action 执行顺序从哪里来？
- cache 为什么是 scheduler 的关键依赖？
- scheduler 为什么不直接绑定 Pod？
