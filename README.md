# Raha 数据检测工程

## 工程定位

本工程是面向 FMDB 和 Spark 的单元格级数据检测组件，采用任务模型组织数据加载、字段画像、策略生成、特征组装、聚类采样、标签传播、模型训练、模型预测、规则检测、结果评估和结果登记。

当前工程只保留任务模型主线，不包含 UDF、文件任务队列、后台消费者和容器验收入口。业务调用方在同一进程内提交任务并直接执行，不需要先生成任务文件，再由另一个进程消费。

## 统一入口

训练、预测和采样统一通过 `RahaTaskApplicationService.execute` 执行。该入口负责：

1. 校验任务请求和任务类型。
2. 通过 `RahaJobOrchestrator.submit` 完成幂等建单。
3. 根据 `JobType` 选择对应工作流。
4. 创建阶段处理器并调用 `RahaJobOrchestrator.execute`。
5. 返回任务状态、阶段轨迹、业务输出和结果位置。

最小调用形式如下：

```java
RahaTaskExecutionRequest request = RahaTaskExecutionRequest.training(
        config,
        dataLoadRequest,
        trainConfig,
        labelPropagationConfig,
        labels);

RahaTaskExecutionResult result = applicationService.execute(request);
```

直接执行的调用链如下：

```text
业务调用方
  -> service.task.RahaTaskApplicationService
  -> service.task.RahaWorkflow
  -> job.execution.RahaJobOrchestrator
  -> job.stage.StageHandler
  -> service.* 和领域算法组件
  -> repository.port
```

`submit` 和 `execute` 仍然保留在任务编排层，分别负责建单和执行；业务方不再需要自行拼装这两个动作。统一入口不是队列消费者，也不会启动定时器或常驻线程。

## 支持的工作流

| 任务类型 | 工作流 | 主要阶段 | 主要输出 |
| --- | --- | --- | --- |
| `TRAINING` | `TrainingWorkflow` | 加载、画像、策略、特征、聚类、标签、传播、训练、评估、结果登记 | 候选模型和训练结果 |
| `DETECTION` | `DetectionWorkflow` | 加载、画像、策略、特征、已发布模型预测、评估、结果登记 | 检测结果 |
| `SAMPLING` | `SamplingWorkflow` | 加载、画像、策略、特征、聚类、主动采样、结果登记 | 采样任务和样本结果 |

训练阶段生成的是候选模型。模型是否成为生产可用模型，仍由 `model.release` 中的发布服务单独完成，这是审批边界，不是第二个任务消费进程。预测工作流只读取已发布模型，避免把训练候选模型直接当成生产模型。

## 任务模型和服务的关系

任务模型负责记录执行过程和生命周期，服务包负责完成具体业务动作：

| 模块 | 职责 |
| --- | --- |
| `job.domain` | 任务、阶段和任务运行结果领域对象 |
| `job.execution` | 幂等提交、阶段编排、重试、失败决策和终态管理 |
| `job.stage` | 阶段上下文、阶段结果和具体阶段适配器 |
| `service.task` | 统一业务入口、工作流注册和训练预测采样编排 |
| `service.prepare` | 数据画像、策略计划和特征准备 |
| `service.train` | 候选模型训练 |
| `service.detect` | 已发布模型预测 |
| `service.sample` | 主动采样和采样任务生成 |
| `repository.port` | 任务、阶段、结果和模型存储接口 |
| `repository.adapter` | 内存或持久化仓储实现 |

服务包不再自行维护一套 `RahaTaskType`、`RahaTaskStatus` 和 `RahaTaskResult`。任务类型和生命周期统一使用 `data.type.JobType`、`data.type.JobStatus`，服务返回统一使用 `service.common.RahaServiceResult`。

## 包结构

```text
com.fiberhome.ml.raha
  checkpoint
  cluster
    algorithm
    domain
  config
    core
    dto
    validation
  data
    domain
    loader
    profile
    type
  detection
    explanation
    scoring
    service
  evaluation
    metrics
    threshold
  feature
    assembly
    domain
  fmdb
    gateway
  job
    domain
    execution
    id
    stage
  label
    propagation
  model
    domain
    prediction
    release
    training
  repository
    adapter
    core
    port
  sampling
    domain
    service
  service
    common
    detect
    prepare
    sample
    task
    train
  strategy
    api
    domain
    execution
    impl
    plan
```

## 结果和状态

成功执行返回 `RahaTaskExecutionResult`。调用方可以读取：

- `getJob()`：任务标识、类型、状态和时间信息。
- `getStages()`：每个阶段的执行状态和失败信息。
- `getPayload()`：训练模型、检测结果或采样输出。
- `getResultLocation()`：业务结果的逻辑存储位置。

任务终态包括 `SUCCEEDED`、`PARTIAL_SUCCESS`、`FAILED` 和 `CANCELLED`。可容忍失败的阶段不会阻断后续阶段，但最终任务会标记为 `PARTIAL_SUCCESS`，避免把不完整结果误报为完全成功。

同一个幂等键重复提交时，入口返回已有任务及其阶段轨迹，不会重复执行阶段。当前仓储接口主要保存任务和阶段状态，重复请求不会重新恢复内存中的业务 payload；业务方应根据结果位置重新读取持久化结果。

## 配置

默认配置位于：

```text
src/main/resources/raha-defaults.properties
```

常用配置前缀如下：

| 前缀 | 用途 |
| --- | --- |
| `raha.job.*` | 任务行为和随机种子 |
| `raha.strategy.*` | 策略范围、优先级、数量和超时 |
| `raha.feature.*` | 特征归一化、上下文特征和特征规模 |
| `raha.model.*` | 分类器、阈值、训练参数和质量门禁 |
| `raha.clustering.*` | 聚类算法参数 |
| `raha.sampling.*` | 主动采样预算和任务有效期 |
| `raha.label.*` | 标签传播权重和多数比例 |
| `raha.resource.*` | 并行度、广播、缓存和阶段超时 |
| `raha.failure.*` | 失败容忍和重试次数 |

## 运行要求

- JDK 8
- Maven 3.8 或更高版本
- Spark 3.3.1
- Scala 2.12 对应的 Spark 依赖

编译、测试和打包命令：

```powershell
mvn clean compile
mvn clean test
mvn clean package
```

Spark 依赖使用 `provided` 范围，实际运行环境需要提供对应 Spark 运行时。生产调用方需要负责创建和关闭 `SparkSession`，并注入仓储、FMDB 网关、模型发布服务及各领域服务实现。

## 当前边界

- 工程不是 Spring Boot 应用，不提供默认 `Application` 启动类。
- 工程不包含常驻 Worker，不创建定时器，也不轮询文件目录。
- 统一入口是同步执行接口；需要异步执行时，应在工程外部使用已有调度系统调用该入口，并由外部系统管理重试、超时和并发。
- `EVALUATION`、`STRATEGY_ANALYSIS` 等类型暂未接入统一入口，需要先定义对应工作流后再注册。

## 相关文档

| 文档 | 用途 |
| --- | --- |
| `doc/20260718/Raha任务编排与训练预测统一入口分析-202607181400.md` | 统一入口、训练预测工作流和边界分析 |
| `doc/20260718/Raha统一任务编排入口代码落地报告-202607181450.md` | 本轮代码落地范围、验证结果和后续边界 |
