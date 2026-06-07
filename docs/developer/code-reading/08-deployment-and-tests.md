# 08. 部署与测试

这一章把源码和 Helm 部署、测试目录连起来。需要理解配置如何变成运行组件，或者想用测试反推行为时读这一章。

## 部署阅读顺序

1. [`deployments/kai-scheduler/values.yaml`](../../../deployments/kai-scheduler/values.yaml)
2. [`deployments/kai-scheduler/templates/kai-config.yaml`](../../../deployments/kai-scheduler/templates/kai-config.yaml)
3. [`deployments/kai-scheduler/templates/default-shard.yaml`](../../../deployments/kai-scheduler/templates/default-shard.yaml)
4. [`deployments/kai-scheduler/templates/default-queue.yaml`](../../../deployments/kai-scheduler/templates/default-queue.yaml)
5. [`deployments/kai-scheduler/templates/services/operator.yaml`](../../../deployments/kai-scheduler/templates/services/operator.yaml)
6. [`deployments/kai-scheduler/templates/rbac`](../../../deployments/kai-scheduler/templates/rbac)
7. [`pkg/operator`](../../../pkg/operator)

## Helm 到 Operator 的路径

```text
values.yaml
  -> templates/kai-config.yaml
     -> kai.scheduler/v1 Config
        -> ConfigReconciler
           -> 组件 Deployment、Service、Webhook、RBAC、ConfigMap

values.yaml
  -> templates/default-shard.yaml
     -> kai.scheduler/v1 SchedulingShard
        -> SchedulingShardReconciler
           -> scheduler shard Deployment、Service、ConfigMap、EndpointSlice

values.yaml
  -> templates/default-queue.yaml
     -> scheduling.run.ai/v2 Queue
        -> QueueController
           -> Queue status 和 metrics
```

## `values.yaml` 怎么读

[`deployments/kai-scheduler/values.yaml`](../../../deployments/kai-scheduler/values.yaml) 是用户侧部署入口。

重点区域：

- `global`：共享 image、replica、namespace、labels、scheduler name、autoscaling。
- `operator`：operator deployment。
- `scheduler`：scheduler args、placement strategy、plugins、actions。
- `binder`：binder 配置和 binder plugin 配置。
- `podgrouper`：workload 到 PodGroup 的行为。
- `queuecontroller`：queue controller 和 webhook 行为。
- `podgroupcontroller`：PodGroup status controller。
- `admission`：Pod admission webhooks。
- `nodescaleadjuster`：scaling pod 行为。
- `prometheus`：监控资源。
- `defaultQueue`：默认 Queue。

## Config 模板

源码：[`deployments/kai-scheduler/templates/kai-config.yaml`](../../../deployments/kai-scheduler/templates/kai-config.yaml)

这个模板把 Helm values 映射成 `kind: Config`。

常见映射：

```text
.Values.binder.enabled
  -> spec.binder.service.enabled

.Values.podgrouper.enabled
  -> spec.podGrouper.service.enabled

.Values.scheduler.enabled
  -> spec.scheduler.service.enabled

.Values.global.clusterAutoscaling
  -> spec.nodeScaleAdjuster.service.enabled

.Values.prometheus.enabled
  -> spec.prometheus.enabled
```

继续追：

- [`pkg/apis/kai/v1/config_types.go`](../../../pkg/apis/kai/v1/config_types.go)
- [`pkg/operator/controller/config_controller.go`](../../../pkg/operator/controller/config_controller.go)
- [`pkg/operator/operands`](../../../pkg/operator/operands)

## Default Shard 模板

源码：[`deployments/kai-scheduler/templates/default-shard.yaml`](../../../deployments/kai-scheduler/templates/default-shard.yaml)

这个模板创建默认 `SchedulingShard`。

重要映射：

```text
scheduler.args
  -> spec.args

scheduler.placementStrategy
  -> spec.placementStrategy

scheduler.plugins
  -> spec.plugins

scheduler.actions
  -> spec.actions

scheduler.queueDepthPerAction
  -> spec.queueDepthPerAction
```

继续追：

- [`pkg/apis/kai/v1/schedulingshard_types.go`](../../../pkg/apis/kai/v1/schedulingshard_types.go)
- [`pkg/operator/controller/schedulingshard_controller.go`](../../../pkg/operator/controller/schedulingshard_controller.go)
- [`pkg/operator/operands/scheduler`](../../../pkg/operator/operands/scheduler)

## Default Queue 模板

源码：[`deployments/kai-scheduler/templates/default-queue.yaml`](../../../deployments/kai-scheduler/templates/default-queue.yaml)

这个模板创建初始 Queue。对照阅读：

- [`pkg/apis/scheduling/v2/queue_types.go`](../../../pkg/apis/scheduling/v2/queue_types.go)
- [`pkg/queuecontroller/controllers/queue_controller.go`](../../../pkg/queuecontroller/controllers/queue_controller.go)
- [`pkg/scheduler/plugins/proportion`](../../../pkg/scheduler/plugins/proportion)

## 测试目录地图

测试是行为文档。修改代码前先找对应测试，能省很多来回。

| 行为 | 测试区域 |
| --- | --- |
| scheduler action 行为 | [`pkg/scheduler/actions`](../../../pkg/scheduler/actions) |
| scheduler plugins | [`pkg/scheduler/plugins`](../../../pkg/scheduler/plugins) |
| binder 行为 | [`pkg/binder`](../../../pkg/binder) |
| podgrouper 行为 | [`pkg/podgrouper`](../../../pkg/podgrouper) |
| queuecontroller 行为 | [`pkg/queuecontroller`](../../../pkg/queuecontroller) |
| controller envtest | [`pkg/env-tests`](../../../pkg/env-tests) |
| 端到端调度行为 | [`test/e2e/suites`](../../../test/e2e/suites) |
| Helm 渲染行为 | [`deployments/kai-scheduler/tests`](../../../deployments/kai-scheduler/tests) |

## E2E 目录怎么读

打开 [`test/e2e/suites`](../../../test/e2e/suites)：

- `allocate/quota`：Queue quota 和公平性。
- `allocate/resources`：CPU/GPU/resource 基础放置。
- `allocate/predicates`：Kubernetes 约束。
- `allocate/node_order`：打分和节点顺序。
- `allocate/priority`：workload priority。
- `allocate/topology`：topology-aware allocation。
- `allocate/subgroups`、`allocate/min_subgroups`：层级 gang scheduling。
- `allocate/elastic`：弹性 workload。
- `reclaim`：跨 Queue resource reclaim。
- `preempt`：同 Queue priority preemption。
- `consolidation`：碎片整理和迁移。
- `integrations`：Kubernetes 原生和第三方 workload 支持。
- `api`：CRD、config、events、readiness。
- `timeaware`：time-based fairshare。
- `upgrade`：升级场景。

## Helm tests

打开 [`deployments/kai-scheduler/tests`](../../../deployments/kai-scheduler/tests)：

- `enabled_test.yaml`：组件 enable/disable 映射。
- `binder_plugins_test.yaml`：binder plugin 渲染。
- `leader_election_test.yaml`：leader election values。
- `resources_test.yaml`：requests/limits 渲染。
- `prometheus_test.yaml`：监控资源。
- `affinity_test.yaml`：affinity 渲染。
- `image_pull_secrets_test.yaml`：pull secret 渲染。
- `crd_upgrader_test.yaml`：CRD upgrade hook。
- `post_delete_test.yaml`：删除后清理 hook。

## 常用命令

从仓库根目录运行：

```bash
make test
```

运行 scheduler action 测试：

```bash
ginkgo -v ./pkg/scheduler/actions/allocate
```

运行单个 focused 测试：

```bash
ginkgo -v --focus "TestHandleAllocation" ./pkg/scheduler/actions/allocate
```

运行 envtest：

```bash
make envtest
KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.34.0 -p path --bin-dir bin)" go test ./pkg/... -timeout 30m
```

运行 kind e2e：

```bash
./hack/run-e2e-kind.sh
```

运行 focused e2e：

```bash
ginkgo -r --randomize-all --focus "quota" ./test/e2e/suites
```

## 用测试辅助读源码

建议流程：

```text
1. 先确定要理解的行为。
2. 找对应 e2e 或 unit test。
3. 读测试里的 Queue、PodGroup、Pod、Node。
4. 看期望的事件、status、node placement。
5. 回到对应 action/plugin/controller 源码。
```

例子：

```text
想理解 Pod 为什么因为 quota 调度不上
  -> test/e2e/suites/allocate/quota
  -> pkg/scheduler/plugins/proportion
  -> pkg/scheduler/actions/common/allocate.go
  -> Queue / PodGroup API types
```

## 本章检查点

读完后应该能回答：

- 哪个 Helm template 创建默认 `SchedulingShard`？
- 哪个 CRD 配置 scheduler actions/plugins？
- 哪些测试最适合解释 queue quota 行为？
- 修改 Helm values 前应该看哪些测试？
