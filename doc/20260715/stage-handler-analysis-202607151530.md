# StageHandler 功能与调用流程分析

## 一、结论概览

`StageHandler` 是 Raha 任务流水线中单个阶段的统一扩展接口。
它只约定两个动作：返回阶段类型，以及接收执行上下文并执行阶段。
它不负责阶段排序、任务状态流转、失败重试、结果持久化或日志编排。

上述职责由 `RahaJobOrchestrator` 统一承担。编排器按照调用方传入的
`List<StageHandler>` 顺序执行处理器，因此阶段顺序不是由 `StageType` 枚举
自动排序产生的，而是由流水线组装代码显式决定。

当前源码中有 9 个具体实现：数据加载、列画像、策略计划、策略执行、
特征生成、检测预测、列内聚类、主动采样和真值标注。

## 二、StageHandler 接口

文件：`src/main/java/com/fiberhome/ml/raha/job/stage/StageHandler.java`

接口定义如下：

| 方法 | 作用 |
| --- | --- |
| `getStageType()` | 返回当前处理器对应的 `StageType`，用于生成阶段记录、日志和阶段标识。 |
| `execute(StageExecutionContext context)` | 执行具体业务并返回 `StageResult`。 |

`StageExecutionContext` 提供四类输入：

| 内容 | 用途 |
| --- | --- |
| `RahaJob job` | 当前任务快照，包括任务标识、配置版本、输入快照和任务状态。 |
| `RahaJobConfig config` | 当前任务的完整配置。 |
| `RahaStage stage` | 当前阶段快照，包括阶段标识、阶段类型和尝试次数。 |
| `attributes` | 当前任务内阶段间共享的数据容器。前一阶段写入，后一阶段读取。 |

阶段处理器通过 `StageResult` 表达三种结果：

* `SUCCESS`：阶段完成，编排器标记阶段成功并进入下一个阶段。
* `SKIPPED`：当前阶段没有可处理数据或任务，编排器标记阶段跳过，但仍继续后续阶段。
* `FAILED`：阶段失败。结果中还可以携带错误编码、失败比例和是否可恢复，供失败决策器判断重试、继续或终止。

## 三、具体实现类

| 实现类 | 阶段类型 | 依赖服务 | 主要处理 | 写入的共享属性 |
| --- | --- | --- | --- | --- |
| `DataLoadStageHandler` | `LOAD_DATA` | `RahaDatasetLoader` | 从外部输入加载数据集和快照；加载结果同时绑定任务实际快照。 | `RAHA_DATASET`、`DATASET_SNAPSHOT` |
| `ColumnProfileStageHandler` | `PROFILE` | `ColumnProfileService` | 校验数据集存在，生成列画像并保存新的数据集版本。 | 更新 `RAHA_DATASET` |
| `StrategyPlanStageHandler` | `GENERATE_STRATEGY` | `StrategyPlanService` | 根据列画像和策略配置生成并保存策略计划；无计划时跳过。 | `STRATEGY_PLANS` |
| `StrategyRunStageHandler` | `RUN_STRATEGY` | `StrategyExecutionService` | 执行策略计划，保存批量结果和策略命中；存在部分策略失败时返回可恢复失败。 | `STRATEGY_BATCH_RESULT`、`STRATEGY_HITS` |
| `FeatureStageHandler` | `GENERATE_FEATURE` | `FeatureService` | 将数据集、策略计划和策略命中组装为稀疏特征并保存；无特征行时跳过。 | `FEATURE_ASSEMBLY_RESULT` |
| `DetectionStageHandler` | `PREDICT` | `BasicDetectionService` | 使用规则加权模型生成最终检测结果并保存。 | `DETECTION_BATCH_RESULT` |
| `ClusterStageHandler` | `CLUSTER` | `ColumnClusteringService` | 对单列稀疏特征执行列内聚类并保存成员映射；无聚类成员时跳过。 | `CLUSTERING_BATCH_RESULT` |
| `SamplingStageHandler` | `SAMPLE` | `SamplingService` | 根据聚类结果、已有标签和采样配置生成预算内标注任务。 | `SAMPLING_BATCH_RESULT`、`ANNOTATION_TASKS` |
| `GroundTruthLabelStageHandler` | `LABEL` | `GroundTruthLabelAdapter`、`AnnotationTaskRepository` | 使用真值数据集完成标注任务，保存任务状态并生成单元格标签。 | 更新 `ANNOTATION_TASKS`、`CELL_LABELS` |

这些类的共同模式是：构造函数注入业务服务；`execute` 先从
`context.getAttributes()` 校验上游输入，再调用领域服务；领域服务的结果
写回共享属性，最后转换为 `StageResult`。因此它们更接近“阶段适配器”，领域算法
实际位于 loader、profile、strategy、feature、detection、cluster、sampling 和
label 包中的服务类内。

## 四、谁调用 StageHandler

直接调用者是：

`RahaJobOrchestrator.execute(...)`

编排器会先校验所有处理器非空、阶段类型非空，并禁止同一流水线重复出现同一个
`StageType`。之后在循环中为每个处理器创建 `RahaStage`，构造上下文，并调用：

```java
StageResult result = handler.execute(new StageExecutionContext(
        job.snapshot(), config, stage.snapshot(), attributes));
```

当前仓库中，9 个实现的完整组装调用明确出现在：

`src/test/java/com/fiberhome/ml/raha/job/Iteration5PipelineIntegrationTest.java`

该测试按固定顺序将 9 个处理器传给 `orchestrator.execute`。迭代 2、3、4 测试
分别组装了前一部分阶段，用于验证逐步扩展的流水线。

需要区分 UDF 提交链路：`AbstractRahaTableUdf` 调用的是
`RahaUdfJobSubmitter.submit`；`RepositoryBackedRahaUdfJobSubmitter` 当前源码的
职责是权限校验、幂等检查、创建 `RahaJob` 并写入 FMDB 任务表。它没有直接调用
`RahaJobOrchestrator.execute`，因此从现有源码只能确认“UDF 负责提交任务”，不能
确认一个生产后台执行器已经在本仓库内把提交后的任务组装成这些处理器并执行。

## 五、主要执行流程

```text
提交配置
  -> 校验配置、计算配置版本和幂等键
  -> 已存在则返回旧任务，否则创建 CREATED 任务
  -> 编排器校验阶段列表和阶段类型唯一性
  -> 按列表顺序执行阶段
  -> 每阶段创建 RahaStage 并置为 RUNNING
  -> 构造 StageExecutionContext，调用 StageHandler.execute
  -> 根据 StageResult 标记 SUCCESS、SKIPPED 或 FAILED
  -> FAILED 时按失败容忍配置决定 RETRY、CONTINUE 或 TERMINATE
  -> 全部阶段结束后任务置为 SUCCEEDED
```

以当前完整评测流水线为例，主数据流是：

```text
外部数据
  -> 数据集和输入快照
  -> 列画像数据集
  -> 策略计划
  -> 策略命中
  -> 稀疏特征
  -> 检测结果
  -> 列内聚类结果
  -> 标注任务
  -> 真值标签
```

共享属性是这条数据流在任务内的载体。它是一个编排器创建的
`LinkedHashMap<String, Object>`，所有阶段共用同一个 Map。处理器通常通过
新增键或替换已有键传递版本化结果；阶段仓储则保存每个阶段的状态、尝试次数和
`ArtifactVersion`，不依赖共享 Map 作为持久化存储。

## 六、失败、重试和状态行为

编排器对处理器抛出的运行时异常统一转换为 `STAGE_EXECUTION_ERROR`，并记录任务、
阶段和尝试次数。处理器返回空结果时转换为 `STAGE_RESULT_REQUIRED`。

失败决策由 `StageFailureDecider` 完成：

* 不可恢复、配置为快速失败，或失败比例超过阈值：终止任务。
* 可恢复且尚未超过最大重试次数：重新创建同一阶段的新尝试并再次调用处理器。
* 可恢复但不再重试：允许继续后续阶段。

阶段失败本身会被保存；任务只有在终止分支才置为 `FAILED`。如果阶段返回
`SKIPPED`，阶段被保存为跳过，但任务仍可最终成功。

## 七、值得注意的实现边界

1. 阶段顺序依赖调用方传入的 List，枚举中的 `INITIALIZE`、`TRAIN`、`EVALUATE`、`PERSIST_RESULT` 等类型目前没有对应的 `StageHandler` 实现。
2. `GroundTruthLabelStageHandler` 是评测流水线专用实现，依赖外部真值数据集；普通检测流水线不一定需要它。
3. `StrategyRunStageHandler` 的部分失败会先把批量结果和命中写入上下文，再由失败容忍策略决定是否重试或继续。
4. `DataLoadStageHandler` 是唯一通过 `StageResult.successWithSnapshot` 返回实际输入快照的阶段，编排器会据此绑定任务快照，并拒绝快照冲突。
5. 阶段处理器中的领域服务调用可能涉及文件系统、Spark、仓储或其他外部依赖；这些调用的实际持久化和算法细节应继续查看对应 service 类，不能仅从 `StageHandler` 接口推断。
