# 01. 整体架构

这一章先建立仓库全局视角。KAI Scheduler 不是一个单独的 scheduler 进程，而是一组围绕 GPU/AI workload 调度协作的 Kubernetes controller 和服务。

## 先打开这些文件

| 目标 | 文件 |
| --- | --- |
| 项目总览 | [`README.md`](../../../README.md) |
| 调度器入口 | [`cmd/scheduler/app/server.go`](../../../cmd/scheduler/app/server.go) |
| 调度周期 | [`pkg/scheduler/scheduler.go`](../../../pkg/scheduler/scheduler.go) |
| Pod 生成 PodGroup | [`pkg/podgrouper/pod_controller.go`](../../../pkg/podgrouper/pod_controller.go) |
| Queue 状态汇总 | [`pkg/queuecontroller/controllers/queue_controller.go`](../../../pkg/queuecontroller/controllers/queue_controller.go) |
| BindRequest 消费 | [`pkg/binder/controllers/bindrequest_controller.go`](../../../pkg/binder/controllers/bindrequest_controller.go) |
| Operator 控制面 | [`pkg/operator/controller/config_controller.go`](../../../pkg/operator/controller/config_controller.go) |

## 组件流

```text
Admission
  -> PodGrouper
  -> PodGroupController
  -> QueueController
  -> Scheduler
  -> BindRequest
  -> Binder
  -> Kubernetes pods/binding
```

辅助组件：

- `Operator`：把 `Config` 和 `SchedulingShard` 变成 Deployment、Service、ConfigMap、Webhook、RBAC 等资源。
- `ResourceReservation`：帮助 fractional GPU 场景把 GPU group 映射到真实 GPU 设备。
- `NodeScaleAdjuster`：为 fractional GPU unschedulable pod 创建 scaling pod，辅助 autoscaler 扩容。

## 核心资源归属

| 资源 | 主要写入方 | 主要读取方 | 阅读重点 |
| --- | --- | --- | --- |
| `Pod` | 用户、admission、podgrouper、binder | podgrouper、podgroupcontroller、scheduler、binder | workload 实例，最终会被绑定到节点。 |
| `PodGroup` | podgrouper、外部用户 | podgroupcontroller、queuecontroller、scheduler | gang scheduling 的基本单位。 |
| `Queue` | 用户、Helm 默认资源 | queuecontroller、scheduler | 层级、公平性、quota、priority、reclaim 的上下文。 |
| `BindRequest` | scheduler | binder | scheduler 到 binder 的异步交接契约。 |
| `Config` | Helm 或用户 | operator | 全局组件配置。 |
| `SchedulingShard` | Helm 或用户 | operator、scheduler operand | 调度分片、actions/plugins、placement strategy。 |
| `Topology` | 用户或迁移 hook | scheduler | 拓扑感知调度输入。 |

## 带注释的运行路径

```text
Pod 被创建
  # admission 可能校验或注入 GPU sharing、HAMi、runtimeClass 等配置。

PodGrouper 观察到 Pod
  # 沿 ownerReferences 找 workload 顶层 owner。
  # 根据 workload 类型选择 grouper plugin。
  # 创建或更新 PodGroup，并给 Pod 写 PodGroup annotation / SubGroup label。

PodGroupController 更新 PodGroup.status
  # 汇总关联 Pod 的 phase、requested resources、allocated resources 等。

QueueController 更新 Queue.status
  # 汇总子队列和 PodGroup 的资源状态。

Scheduler 调度周期开始
  # 从 cache 拿 Pods、Nodes、Queues、PodGroups、BindRequests、storage、DRA、topology 的快照。

Actions 执行
  # allocate 先尝试无干扰调度。
  # consolidation/reclaim/preempt 会先试算，再决定是否提交驱逐或绑定。

Scheduler 提交绑定决策
  # 不直接 bind Pod，而是创建 BindRequest。

Binder 消费 BindRequest
  # 做 reservation、volume binding、DRA、GPU sharing 等准备，然后调用 Kubernetes pods/binding。
```

## 仓库目录按职责看

| 目录 | 作用 |
| --- | --- |
| [`cmd`](../../../cmd) | 所有二进制入口和 options。 |
| [`pkg/scheduler`](../../../pkg/scheduler) | 核心 scheduler：cache、内部 API、framework、actions、plugins。 |
| [`pkg/apis`](../../../pkg/apis) | KAI CRD Go 类型和生成的 client/informer/lister。 |
| [`pkg/podgrouper`](../../../pkg/podgrouper) | 把 workload/pod 转成 PodGroup。 |
| [`pkg/podgroupcontroller`](../../../pkg/podgroupcontroller) | 维护 PodGroup status。 |
| [`pkg/queuecontroller`](../../../pkg/queuecontroller) | 维护 Queue status 和 metrics。 |
| [`pkg/binder`](../../../pkg/binder) | 消费 BindRequest 并执行真实 Pod binding。 |
| [`pkg/admission`](../../../pkg/admission) | Mutating/validating webhooks。 |
| [`pkg/operator`](../../../pkg/operator) | 部署和配置控制面。 |
| [`deployments/kai-scheduler`](../../../deployments/kai-scheduler) | Helm chart、CRD、RBAC、默认 shard/queue。 |
| [`test/e2e`](../../../test/e2e) | 端到端行为场景。 |

## 阅读时要抓住的概念

- scheduler 负责做 placement 决策，binder 负责执行真实 binding。
- `PodGroup` 是调度单位，哪怕 workload 只有一个 Pod；KAI 还支持层级 `SubGroups` 和 `minSubGroup`，详见 [10. PodGroup、SubGroup 与 Queue 概念](10-podgroup-queue-concepts.md)。
- Queue 的 `spec` 是策略，Queue 的 `status` 是观测到的资源使用；KAI Queue 可以形成层级树，只有 leaf queue 承载 PodGroup。
- `Session` 是单轮调度上下文，不是全局状态。
- `Statement` 允许 action 在内存里试算、回滚、提交。
- scheduler plugin 通常不是独立服务，而是在 `Session` 上注册回调函数。

## 本章检查点

读完后应该能回答：

- 哪个组件创建 `BindRequest`？
- 哪个组件真正调用 Kubernetes `pods/binding`？
- 为什么 scheduler 同时需要 `PodGroup` 和 `Queue`？
- `Config` 和 `SchedulingShard` 分别控制什么？
