# KAI Scheduler 源码阅读导航

这组文档是面向源码阅读的伴读笔记。它不替代现有概念文档，而是把“应该打开哪些文件、沿着哪条调用链读、关键代码该怎么看”串起来。

## 章节目录

| 章节 | 主题 | 推荐入口 |
| --- | --- | --- |
| [01. 整体架构](01-overall-architecture.md) | 组件边界、资源流、调度前后链路 | [`README.md`](../../../README.md) |
| [02. Scheduler 主循环](02-scheduler-main-loop.md) | 进程启动、配置解析、cache、周期调度 | [`cmd/scheduler/app/server.go`](../../../cmd/scheduler/app/server.go) |
| [03. Session 与 Statement](03-session-and-statement.md) | 单轮调度状态、插件回调、事务式试算 | [`pkg/scheduler/framework/session.go`](../../../pkg/scheduler/framework/session.go) |
| [04. Actions](04-actions.md) | allocate、consolidation、reclaim、preempt、stale gang eviction | [`pkg/scheduler/actions/allocate/allocate.go`](../../../pkg/scheduler/actions/allocate/allocate.go) |
| [05. Plugins](05-plugins.md) | 调度策略扩展点、默认插件、回调注册 | [`pkg/scheduler/plugins/factory.go`](../../../pkg/scheduler/plugins/factory.go) |
| [06. API 与 CRD](06-apis-and-crds.md) | Queue、PodGroup、BindRequest、Config、SchedulingShard | [`pkg/apis`](../../../pkg/apis) |
| [07. 控制器与外围组件](07-controllers-and-components.md) | PodGrouper、Binder、QueueController、Operator 等 | [`cmd`](../../../cmd) |
| [08. 部署与测试](08-deployment-and-tests.md) | Helm、operator 映射、测试目录如何辅助阅读 | [`deployments/kai-scheduler/values.yaml`](../../../deployments/kai-scheduler/values.yaml) |
| [09. 调度数据流与输出链路](09-data-flow-and-outputs.md) | Snapshot、内部模型、Statement commit、BindRequest、controller 状态流 | [`pkg/scheduler/cache/cache.go`](../../../pkg/scheduler/cache/cache.go) |
| [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md) | KAI 相比 kube-batch 的层级 PodGroup/SubGroup、leaf Queue、公平性与抢占语义 | [`pkg/apis/scheduling`](../../../pkg/apis/scheduling) |

## 第一次阅读建议

1. 先读 [01. 整体架构](01-overall-architecture.md)，建立组件图。
2. 再读 [02. Scheduler 主循环](02-scheduler-main-loop.md)，打开 [`pkg/scheduler/scheduler.go`](../../../pkg/scheduler/scheduler.go) 对照。
3. 然后读 [03. Session 与 Statement](03-session-and-statement.md)。这是理解 scheduler 为什么能“试算、回滚、提交”的核心。
4. 之后读 [04. Actions](04-actions.md)，先看 `allocate`，再看 `reclaim/preempt`。
5. 读 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)，把 KAI 的层级 gang 和层级 Queue 语义补齐。
6. 再读 [05. Plugins](05-plugins.md)，挑一个和当前问题相关的插件深入。
7. 最后读 [06. API 与 CRD](06-apis-and-crds.md) 和 [07. 控制器与外围组件](07-controllers-and-components.md)，把 Kubernetes 对象流补齐。
8. 读 [09. 调度数据流与输出链路](09-data-flow-and-outputs.md)，把 snapshot、Statement commit、BindRequest、binder/controller 串起来。
9. 需要验证行为时读 [08. 部署与测试](08-deployment-and-tests.md)。

## 一句话模型

```text
Pod/Workload
  -> PodGroup
     -> SubGroup / PodSet
  -> Queue
     -> Queue hierarchy
  -> Scheduler Session
  -> Action + Plugin callbacks
  -> BindRequest
  -> Binder
  -> Kubernetes Pod binding
```

## 配套概念文档

- [Scheduler Core Concepts](../scheduler-concepts.md)
- [Scheduler Actions Framework](../action-framework.md)
- [Scheduler Plugin Framework](../plugin-framework.md)
- [Binder](../binder.md)
- [Pod Grouper](../pod-grouper.md)
- [Scheduling Deep Dive](../../scheduling-deep-dive/README.md)
