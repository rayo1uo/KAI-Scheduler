# 07. 控制器与外围组件

这一章阅读 scheduler 周边组件。它们负责准备调度输入、执行调度输出，以及维护 CRD status。

## 组件速查

| 组件 | 二进制 | 包 | 主要职责 |
| --- | --- | --- | --- |
| Admission | [`cmd/admission`](../../../cmd/admission) | [`pkg/admission`](../../../pkg/admission) | 校验和修改 Pod/Queue。 |
| PodGrouper | [`cmd/podgrouper`](../../../cmd/podgrouper) | [`pkg/podgrouper`](../../../pkg/podgrouper) | 把 Pod/workload 转成 PodGroup。 |
| PodGroupController | [`cmd/podgroupcontroller`](../../../cmd/podgroupcontroller) | [`pkg/podgroupcontroller`](../../../pkg/podgroupcontroller) | 维护 PodGroup status。 |
| QueueController | [`cmd/queuecontroller`](../../../cmd/queuecontroller) | [`pkg/queuecontroller`](../../../pkg/queuecontroller) | 维护 Queue status 和 metrics。 |
| Binder | [`cmd/binder`](../../../cmd/binder) | [`pkg/binder`](../../../pkg/binder) | 消费 BindRequest 并绑定 Pod。 |
| ResourceReservation | [`cmd/resourcereservation`](../../../cmd/resourcereservation) | [`pkg/resourcereservation`](../../../pkg/resourcereservation) | 发现并上报 reservation pod 的 GPU 细节。 |
| NodeScaleAdjuster | [`cmd/nodescaleadjuster`](../../../cmd/nodescaleadjuster) | [`pkg/nodescaleadjuster`](../../../pkg/nodescaleadjuster) | 为 fractional GPU 压力创建 scaling pod。 |
| Operator | [`cmd/operator`](../../../cmd/operator) | [`pkg/operator`](../../../pkg/operator) | 把 Config/SchedulingShard reconcile 成部署资源。 |

## Admission

入口：[`cmd/admission/app/app.go`](../../../cmd/admission/app/app.go)

```go
func (app *App) Run() error {
    ctrl.NewWebhookManagedBy(app.manager, &corev1.Pod{}).
        WithDefaulter(admissionhooks.NewPodMutator(...)).
        WithValidator(admissionhooks.NewPodValidator(...)).
        Complete()

    app.manager.Start(ctrl.SetupSignalHandler())
}
```

继续读：

- [`pkg/admission/plugins/plugins.go`](../../../pkg/admission/plugins/plugins.go)
- [`pkg/admission/webhook/v1alpha2/podhooks/pod_mutator.go`](../../../pkg/admission/webhook/v1alpha2/podhooks/pod_mutator.go)
- [`pkg/admission/webhook/v1alpha2/podhooks/pod_validator.go`](../../../pkg/admission/webhook/v1alpha2/podhooks/pod_validator.go)
- [`pkg/admission/webhook/v1alpha2/gpusharing`](../../../pkg/admission/webhook/v1alpha2/gpusharing)
- [`pkg/admission/webhook/queuehooks/queue_validator.go`](../../../pkg/admission/webhook/queuehooks/queue_validator.go)

阅读重点：

- Pod admission 会按 scheduler name 过滤。
- Mutator 和 validator 都走插件列表。
- GPU sharing 和 runtime enforcement 在 scheduler/binder 看到 Pod 前就准备好。

## PodGrouper

入口：[`pkg/podgrouper/pod_controller.go`](../../../pkg/podgrouper/pod_controller.go)

```text
PodReconciler.Reconcile()
  -> get Pod
  -> 跳过非目标 schedulerName
  -> 跳过 opt-out annotation
  -> podGrouper.GetPodOwners()
  -> podGrouper.GetPGMetadata()
  -> PodGroupHandler.ApplyToCluster()
  -> assignPodToGroupAndSubGroup()
```

源码注释片段：

```go
topOwner, allOwners, err := r.podGrouper.GetPodOwners(ctx, &pod)

// 根据 top owner GVK 选择 grouper plugin。
metadata, err := r.podGrouper.GetPGMetadata(ctx, &pod, topOwner, allOwners)

// 创建或更新 PodGroup。
err = r.PodGroupHandler.ApplyToCluster(ctx, *metadata)

// 给 Pod 写 PodGroup annotation 和 SubGroup label。
err = r.assignPodToGroupAndSubGroup(ctx, &pod, metadata)
```

继续读：

- [`pkg/podgrouper/podgrouper/podgrouper.go`](../../../pkg/podgrouper/podgrouper/podgrouper.go)
- [`pkg/podgrouper/podgrouper/hub/hub.go`](../../../pkg/podgrouper/podgrouper/hub/hub.go)
- [`pkg/podgrouper/podgroup/handler.go`](../../../pkg/podgrouper/podgroup/handler.go)
- [`pkg/podgrouper/podgrouper/plugins`](../../../pkg/podgrouper/podgrouper/plugins)

## PodGroupController

入口：[`pkg/podgroupcontroller/controllers/pod_group_controller.go`](../../../pkg/podgroupcontroller/controllers/pod_group_controller.go)

```text
PodGroupReconciler.Reconcile()
  -> get PodGroup
  -> 通过 PodGroup annotation 找相关 Pods
  -> 计算 pod metadata
  -> 计算 requested/received resources
  -> 更新 PodGroup.status
```

继续读：

- [`pkg/podgroupcontroller/controllers/status_updater.go`](../../../pkg/podgroupcontroller/controllers/status_updater.go)
- [`pkg/podgroupcontroller/controllers/metadata/pod.go`](../../../pkg/podgroupcontroller/controllers/metadata/pod.go)
- [`pkg/podgroupcontroller/controllers/metadata/pod_group.go`](../../../pkg/podgroupcontroller/controllers/metadata/pod_group.go)
- [`pkg/podgroupcontroller/controllers/resources/requested.go`](../../../pkg/podgroupcontroller/controllers/resources/requested.go)
- [`pkg/podgroupcontroller/controllers/resources/received.go`](../../../pkg/podgroupcontroller/controllers/resources/received.go)

阅读重点：

- PodGroup `spec` 表达期望的 gang 行为。
- PodGroup `status` 从相关 Pod 推导。
- QueueController 会消费 PodGroup `resourcesStatus` 来聚合 Queue status。
- scheduler 的实时调度决策不直接消费 PodGroup `resourcesStatus`；它在 snapshot/session 中根据 PodGroup spec、Pod、BindRequest 和 task status 重新构造内部模型。

## QueueController

入口：[`pkg/queuecontroller/controllers/queue_controller.go`](../../../pkg/queuecontroller/controllers/queue_controller.go)

```go
func (r *QueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    queue := &v2.Queue{}
    r.Get(ctx, req.NamespacedName, queue)
    originalQueue := queue.DeepCopy()

    // 汇总子 Queue 和 PodGroup 的资源状态。
    r.resourceUpdater.UpdateQueue(ctx, queue)

    // 刷新 status.childQueues。
    r.childQueuesUpdater.UpdateQueue(ctx, queue)

    r.Client.Status().Patch(ctx, queue, client.MergeFrom(originalQueue))
    metrics.SetQueueMetrics(queue)
}
```

继续读：

- [`pkg/queuecontroller/controllers/resource_updater/resource_updater.go`](../../../pkg/queuecontroller/controllers/resource_updater/resource_updater.go)
- [`pkg/queuecontroller/controllers/childqueues_updater/childqueues_updater.go`](../../../pkg/queuecontroller/controllers/childqueues_updater/childqueues_updater.go)
- [`pkg/queuecontroller/metrics/metrics.go`](../../../pkg/queuecontroller/metrics/metrics.go)

事件关系：

- Queue 变化会 enqueue 父 Queue。
- PodGroup 变化会 enqueue `podGroup.spec.queue`。
- Queue 删除后会 reset metrics。

QueueController 写出的 `status.requested/allocated/allocatedNonPreemptible/childQueues` 主要服务观测和 metrics。scheduler snapshot 会读取 Queue CR 构造 `QueueInfo`，但当前公平性计算来自 Queue spec 与 session 内 PodGroup/task 状态，而不是直接使用 Queue status 的资源数。

## Binder

入口：[`cmd/binder/app/app.go`](../../../cmd/binder/app/app.go)，再读 [`pkg/binder/controllers/bindrequest_controller.go`](../../../pkg/binder/controllers/bindrequest_controller.go)。

```text
BindRequestReconciler.Reconcile()
  -> get BindRequest
  -> 跳过 deleting/succeeded
  -> get Pod
  -> get selected Node
  -> binder.Bind(ctx, pod, node, bindRequest)
  -> 失败则 binder.Rollback(...)
  -> 更新 BindRequest.status
  -> 更新 PodBound condition
```

核心 binding 在 [`pkg/binder/binding/binder.go`](../../../pkg/binder/binding/binder.go)：

```go
func (b *Binder) Bind(ctx context.Context, pod *v1.Pod, node *v1.Node, bindRequest *v1alpha2.BindRequest) error {
    // bind 前先同步 reservation pod。
    b.resourceReservationService.SyncForNode(ctx, bindRequest.Spec.SelectedNode)

    // fractional GPU 分配要先 reserve 真实 GPU device。
    if common.IsSharedGPUAllocation(bindRequest) {
        reservedGPUIds, err = b.reserveGPUs(ctx, pod, bindRequest)
    }

    // volume binding、DRA、GPU sharing、HAMi 等插件在这里执行。
    b.plugins.PreBind(ctx, pod, node, bindRequest, bindingState)

    // 真正调用 Kubernetes pods/binding。
    b.kubeClient.SubResource("binding").Create(ctx, pod, binding)

    b.plugins.PostBind(ctx, pod, node, bindRequest, bindingState)
}
```

继续读：

- [`pkg/binder/plugins/plugins.go`](../../../pkg/binder/plugins/plugins.go)
- [`pkg/binder/plugins/factory.go`](../../../pkg/binder/plugins/factory.go)
- [`pkg/binder/plugins/gpusharing/gpu_sharing.go`](../../../pkg/binder/plugins/gpusharing/gpu_sharing.go)
- [`pkg/binder/plugins/k8s-plugins`](../../../pkg/binder/plugins/k8s-plugins)
- [`pkg/binder/binding/resourcereservation/resource_reservation.go`](../../../pkg/binder/binding/resourcereservation/resource_reservation.go)

## ResourceReservation

reservation binary 运行在 reservation pod 内。

继续读：

- [`cmd/resourcereservation/app/app.go`](../../../cmd/resourcereservation/app/app.go)
- [`pkg/resourcereservation/discovery/discovery.go`](../../../pkg/resourcereservation/discovery/discovery.go)
- [`pkg/resourcereservation/patcher/pod_patcher.go`](../../../pkg/resourcereservation/patcher/pod_patcher.go)
- [`pkg/resourcereservation/poddetails/pod_details.go`](../../../pkg/resourcereservation/poddetails/pod_details.go)

模型：

```text
reservation pod 启动
  -> 发现 GPU identity
  -> patch 自己的 pod annotation
  -> binder 读取 reservation 状态
  -> binder 把 selected GPU group 映射到真实 GPU ID
```

## NodeScaleAdjuster

入口：[`pkg/nodescaleadjuster/controller/reconciler.go`](../../../pkg/nodescaleadjuster/controller/reconciler.go)

```text
Pod event
  -> PodReconciler.Reconcile()
  -> ScaleAdjuster.Adjust()
  -> 清理不再需要的 scaling pods
  -> 找 unschedulable fractional GPU pods
  -> 计算缺口 GPU devices
  -> 创建 scaling pods
```

继续读：

- [`pkg/nodescaleadjuster/scale_adjuster/scale_adjuster.go`](../../../pkg/nodescaleadjuster/scale_adjuster/scale_adjuster.go)
- [`pkg/nodescaleadjuster/scale_adjuster/calculator.go`](../../../pkg/nodescaleadjuster/scale_adjuster/calculator.go)
- [`pkg/nodescaleadjuster/scaler/scaler.go`](../../../pkg/nodescaleadjuster/scaler/scaler.go)
- [`pkg/nodescaleadjuster/scaler/scaling_pod.go`](../../../pkg/nodescaleadjuster/scaler/scaling_pod.go)

## Operator

operator 有两个主要 reconciler：

- [`ConfigReconciler`](../../../pkg/operator/controller/config_controller.go)：部署全局组件。
- [`SchedulingShardReconciler`](../../../pkg/operator/controller/schedulingshard_controller.go)：部署 scheduler shard。

Config reconciler 形状：

```go
func (r *ConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (...) {
    // Config 是 singleton。
    if req.Name != known_types.SingletonInstanceName {
        return ctrl.Result{}, nil
    }

    r.Client.Get(ctx, req.NamespacedName, kaiConfig)
    kaiConfig.Spec.SetDefaultsWhereNeeded()

    r.UpdateStartReconcileStatus(...)

    // 部署 ConfigReconcilerOperands 中的组件。
    r.deployable.Deploy(ctx, r.Client, kaiConfig, kaiConfig)

    // 监控 readiness 和 dependencies。
    r.deployable.Monitor(ctx, r.Client, kaiConfig)
}
```

SchedulingShard reconciler 形状：

```go
func (r *SchedulingShardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (...) {
    r.Get(ctx, req.NamespacedName, shard)
    shard.Spec.SetDefaultsWhereNeeded()

    // shard 只部署 scheduler 相关 operand。
    deployable := deployable.New(r.shardOperandsForShard(shard), ...)

    // 仍然需要全局 Config 提供共享设置。
    r.Get(ctx, client.ObjectKey{Name: known_types.SingletonInstanceName}, kaiConfig)

    deployable.Deploy(ctx, r.Client, kaiConfig, shard)
}
```

继续读：

- [`pkg/operator/operands/deployable/deployable.go`](../../../pkg/operator/operands/deployable/deployable.go)
- [`pkg/operator/operands/scheduler`](../../../pkg/operator/operands/scheduler)
- [`pkg/operator/operands/binder`](../../../pkg/operator/operands/binder)
- [`pkg/operator/operands/pod_grouper`](../../../pkg/operator/operands/pod_grouper)
- [`pkg/operator/controller/status_reconciler/status_reconciler.go`](../../../pkg/operator/controller/status_reconciler/status_reconciler.go)

## 端到端组件路径

```text
1. Pod 创建
2. Admission mutate/validate Pod
3. PodGrouper 创建或更新 PodGroup
4. PodGroupController 更新 PodGroup.status，主要是 `resourcesStatus`
5. QueueController 更新 Queue.status，用于观测和 metrics
6. Scheduler 创建 BindRequest
7. Binder 预留资源并绑定 Pod
8. Binder 更新 BindRequest.status 和 Pod condition
9. workload 运行期间 Pod/PodGroup/Queue status 继续更新
```

## 本章检查点

读完后应该能回答：

- 哪个 controller 给 Pod 写 PodGroup membership？
- 哪个 controller 写 Queue `status.allocated`？
- 哪个组件调用 Kubernetes `pods/binding`？
- 哪个 operator reconciler 部署 scheduler shard？
