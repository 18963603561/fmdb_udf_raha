# Raha 数据检测工程

## 工程定位

本工程是面向 FMDB 和 Spark 的单元格级数据错误检测组件，采用任务模型组织数据加载、字段画像、策略生成、特征组装、聚类采样、标签传播、模型训练、检测评估和结果持久化。

当前工程只保留任务模型主线，不包含 UDF、文件任务队列、后台消费者和容器验收入口。调用方在同一进程内创建任务、组装阶段处理器并直接执行，任务状态和阶段状态由仓储端口统一管理。

## 核心执行方式

任务执行分为两个明确动作，但都在同一调用进程内完成，不依赖第二个消费者进程：

1. `RahaJobOrchestrator.submit` 校验配置、计算配置版本和幂等键，并创建或返回已有任务。
2. `RahaJobOrchestrator.execute` 按顺序执行阶段处理器，处理重试、可容忍失败、任务终止和状态持久化。

典型调用关系如下：

```text
外部调用方
  -> job.execution.RahaJobOrchestrator
  -> job.stage.StageHandler
  -> 领域服务和算法组件
  -> repository.port 仓储端口
  -> repository.adapter 或 fmdb 基础设施实现
```

最小调用形式：

```java
RahaJob job = orchestrator.submit(config);
JobRunResult result = orchestrator.execute(job, config, handlers);
```

`submit` 只负责幂等建单，`execute` 才真正运行阶段。调用方可以在一个方法中连续调用两者，实现直接提交并执行。

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
  error
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
  observability
  parallel
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
    train
  strategy
    api
    domain
    execution
    impl
      od
      pvd
    plan
  util
```

## 分层职责

| 包 | 职责 |
| --- | --- |
| `job.domain` | 任务、阶段和任务运行结果等任务领域模型 |
| `job.execution` | 幂等提交、阶段编排、失败决策和任务终态管理 |
| `job.stage` | 阶段上下文、阶段结果、属性键和具体阶段处理器 |
| `service.*` | 采样、训练、检测和特征准备等用例级服务 |
| `repository.core` | 通用仓储键、记录、事务和版本对象 |
| `repository.port` | 任务模型和领域服务依赖的仓储接口 |
| `repository.adapter` | 通用仓储适配器和内存实现 |
| `data.domain` | 数据集、字段、单元格、快照和检测结果 |
| `data.type` | 任务、阶段、策略、模型和特征相关枚举 |
| `strategy.*` | 策略契约、计划生成、执行模型和具体策略实现 |
| `model.*` | 列级模型、训练、预测和发布生命周期 |
| `fmdb.*` | FMDB 数据读取、表访问、结果写入和模型存储适配 |

接口、领域对象、请求结果和基础设施实现已经分开，新增实现时应继续遵守现有依赖方向，不要重新把接口和实现放回同一层包。

## 主要能力

- 支持 CSV、JSON、Parquet 和 FMDB 来源的数据加载。
- 支持稳定快照、行标识校验、模式摘要和字段画像。
- 支持 OD、PVD、RVD 检测策略及策略并行执行。
- 支持稀疏特征组装、列内聚类、主动采样和标签传播。
- 支持 Spark MLlib 逻辑回归、规则降级、质量门禁和模型发布。
- 支持检测评分、解释、评估切分、阈值比较和真值差异分析。
- 支持任务幂等、阶段状态、失败重试、检查点和内存事务回滚。
- 支持 FMDB 表网关、数据加载、结果写入和模型存储适配。

## 配置

默认配置位于：

```text
src/main/resources/raha-defaults.properties
```

配置按以下职责划分：

| 前缀 | 用途 |
| --- | --- |
| `raha.job.*` | 任务级行为和随机种子 |
| `raha.strategy.*` | 策略范围、优先级、数量和超时 |
| `raha.feature.*` | 值规范化、上下文特征和特征规模 |
| `raha.profile.*` | 字段画像参数 |
| `raha.model.*` | 分类器、阈值、训练和质量门禁 |
| `raha.clustering.*` | 聚类算法参数 |
| `raha.sampling.*` | 主动采样预算和任务有效期 |
| `raha.label.*` | 标签传播权重和多数比例 |
| `raha.resource.*` | 并行度、广播、缓存和阶段超时 |
| `raha.failure.*` | 失败容忍和重试次数 |

配置加载入口位于 `config.core.RahaConfigLoader`，配置对象位于 `config.dto`，校验和版本计算位于 `config.validation`。

## 构建要求

- JDK 8
- Maven 3.8 或更高版本
- Spark 3.3.1
- Scala 2.12 对应的 Spark 依赖

工程通过 Maven Enforcer 强制使用 JDK 8。Spark 依赖使用 `provided` 范围，由实际运行环境提供。

编译：

```powershell
mvn clean compile
```

运行测试：

```powershell
mvn clean test
```

打包：

```powershell
mvn clean package
```

主要产物：

| 文件 | 用途 |
| --- | --- |
| `target/fmdb-udf-raha-1.0.0-SNAPSHOT.jar` | 普通工程 Jar |
| `target/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar` | 包含非 Spark 依赖的完整交付 Jar |

## 测试范围

测试覆盖以下内容：

- 配置加载、配置校验和稳定版本计算。
- 数据加载、快照、字段画像和领域对象约束。
- 策略计划、OD、PVD、RVD 和并行恢复。
- 特征组装、聚类、采样和标签传播。
- 模型训练、预测、质量门禁、发布和回滚。
- 检测、解释、评估、阈值选择和真值对比。
- 任务状态机、阶段编排、检查点和失败策略。
- 仓储事务、FMDB 适配和 Spark 资源管理。

## 当前边界

- 工程当前是组件库，没有默认生产 `Application` 或依赖注入容器。
- 工程不包含常驻 Worker，也不需要文件队列消费者。
- 生产调用方需要负责创建 Spark 会话、选择仓储实现、组装服务和阶段处理器。
- `InMemoryRahaRepository` 只适合测试和单进程临时运行，生产环境应使用可持久化实现。
- `RahaJobOrchestrator.submit` 和 `execute` 是当前任务模型的统一主线，新增入口应调用该主线，不应重新引入 UDF 或文件队列。

## 相关文档

| 文档 | 用途 |
| --- | --- |
| `doc/20260718/Raha包结构分层重构分析-202607181231.md` | 包结构问题、分层原则和迁移依据 |
| `doc/20260718/Raha包结构分层重构落地报告-202607181320.md` | 实际迁移范围、验证结果和后续约束 |
