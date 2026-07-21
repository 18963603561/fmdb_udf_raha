# UDF 三函数 SQL 直接调用可行性与 Executor 嵌套 Spark 作业分析

生成时间：2026-07-21 15:02

## 一、分析背景

本文专门回答以下问题：

1. `F_DW_DETCOLLECT`、`F_DW_DETTRAIN`、`F_DW_DETRUN` 三个 UDF 是否可以让用户直接写 `select udf(...)` 查询并返回数据。
2. `doc/20260721/疑问解答与工程问题复盘-202607210929.md` 中“不能单独执行这 3 个函数”到底是什么意思。
3. 三个函数内部是否存在 Executor 再提交 Spark 作业的风险，如果有，具体来自哪些代码路径。
4. 当前需求是“只开放 UDF 函数给用户”，工程上能否调整，以及推荐如何调整。

本文基于当前工作区代码、现有验证文档和已经落盘的执行日志分析，没有重新执行三函数。

## 二、结论先行

当前三函数“可以注册成 Hive 或 Spark `GenericUDF`”，也“定义了返回字段”，但不建议把当前实现直接开放给用户用普通 `spark-sql` 写：

```sql
SELECT F_DW_DETCOLLECT('...');
```

或者：

```sql
SELECT inline(F_DW_DETCOLLECT('...'));
```

原因不是返回字段本身不能表达，而是当前三函数内部会执行完整 Raha 工作流，工作流里包含大量 Spark action、Spark SQL Catalog 读写、FMDB 表读写和文件发布动作。普通 Spark SQL 计算 UDF 表达式时，可能把 `GenericUDF` 初始化和执行放到 Executor 任务线程中。已有日志已经证明，直接执行 SQL 时失败在 Executor 任务线程获取 SparkSession：

```text
SparkSession should only be created and accessed on the driver.
```

中文解释：`SparkSession` 只能在 Driver 创建或访问，不能在执行任务的 Executor 线程中创建或访问。

```text
Raha UDF 需要当前线程存在活动 SparkSession
```

中文解释：当前 UDF 初始化线程没有可用的 Driver 侧 Spark 会话。

所以，“不能单独执行这 3 个函数”的准确含义应改写为：

这三个函数不能按普通 Executor 标量 UDF 方式直接在 Spark SQL 任务中执行；它们可以在 Driver 侧单独执行，例如当前已经验证成功的 `RahaUdfDriverApp`。

如果当前产品要求是“用户侧只看到 UDF 函数”，可以调整，但调整方向不应是继续让现有 `GenericUDF` 在 Executor 内跑完整 Raha 流程，而应把 UDF 改成 Driver 侧服务代理、Spark SQL Driver 命令，或平台存储过程式入口。

## 三、当前 UDF 返回形态

三个函数继承 `AbstractRahaGenericUdf`，公共基类在 `initialize` 中返回：

```text
list<struct<...>>
```

对应代码证据：

| 文件 | 关键点 |
| --- | --- |
| `src/main/java/com/fiberhome/ml/raha/udf/AbstractRahaGenericUdf.java:31` | 初始化入参和返回类型 |
| `src/main/java/com/fiberhome/ml/raha/udf/AbstractRahaGenericUdf.java:45` | 返回标准 list object inspector |
| `src/main/java/com/fiberhome/ml/raha/udf/AbstractRahaGenericUdf.java:51` | `evaluate` 执行业务逻辑 |
| `src/main/java/com/fiberhome/ml/raha/udf/RahaUdfFields.java:34` | 采集函数字段定义 |
| `src/main/java/com/fiberhome/ml/raha/udf/RahaUdfFields.java:51` | 训练函数字段定义 |
| `src/main/java/com/fiberhome/ml/raha/udf/RahaUdfFields.java:72` | 检测函数字段定义 |

当前字段数量如下：

| 函数 | 返回字段数量 | 典型返回行数 |
| --- | --- | --- |
| `F_DW_DETCOLLECT` | 31 | 通常 1 行 |
| `F_DW_DETTRAIN` | 35 | 按成功训练字段返回多行 |
| `F_DW_DETRUN` | 29 | 通常 1 行 |

因此，如果只从“返回结构”角度看：

1. `SELECT F_DW_DETCOLLECT('...')` 会返回一列复杂对象，形态类似 `array<struct<...>>`。
2. `SELECT inline(F_DW_DETCOLLECT('...'))` 才会把 `array<struct>` 展成多列。
3. 如果对 `inline(...)` 显式写别名，别名数量必须和结构字段数一致，否则会出现字段数量不匹配。
4. `SELECT * FROM F_DW_DETCOLLECT(...)` 是表值函数写法，当前类本身是 `GenericUDF`，不是 Spark SQL Table Valued Function，不能把这种写法当成当前实现的稳定契约。

结论：返回结构可以表达结果，但执行上下文不安全。

## 四、当前三函数的真实调用链

三个函数本身都是薄封装：

| 函数类 | 代码位置 | 实际调用 |
| --- | --- | --- |
| `F_DW_DETCOLLECT` | `src/main/java/com/fiberhome/ml/raha/udf/F_DW_DETCOLLECT.java:21` | `service.collect(argument)` |
| `F_DW_DETTRAIN` | `src/main/java/com/fiberhome/ml/raha/udf/F_DW_DETTRAIN.java:21` | `service.train(argument)` |
| `F_DW_DETRUN` | `src/main/java/com/fiberhome/ml/raha/udf/F_DW_DETRUN.java:21` | `service.detect(argument)` |

公共基类在 `evaluate` 中创建或复用 `RahaDetectionUdfService`：

| 文件 | 关键点 |
| --- | --- |
| `src/main/java/com/fiberhome/ml/raha/udf/AbstractRahaGenericUdf.java:59` | 根据 `SQLContext` 获取 UDF 服务 |
| `src/main/java/com/fiberhome/ml/raha/udf/RahaUdfRuntime.java:26` | 通过 `sqlContext.sparkSession()` 创建业务服务 |
| `src/main/java/com/fiberhome/ml/raha/udf/RahaDetectionUdfService.java:148` | 创建默认任务应用服务 |

三个业务入口都会进入统一任务服务：

| 函数 | 代码位置 | 动作 |
| --- | --- | --- |
| 采集 | `RahaDetectionUdfService.java:167` | `taskService.execute(taskRequest)` |
| 训练 | `RahaDetectionUdfService.java:280` | `taskService.execute(taskRequest)` |
| 检测 | `RahaDetectionUdfService.java:308` | `taskService.execute(taskRequest)` |

统一任务服务再提交到编排器：

| 文件 | 关键点 |
| --- | --- |
| `src/main/java/com/fiberhome/ml/raha/service/task/RahaTaskApplicationService.java:96` | 提交幂等任务 |
| `src/main/java/com/fiberhome/ml/raha/service/task/RahaTaskApplicationService.java:105` | 按任务类型选择工作流 |
| `src/main/java/com/fiberhome/ml/raha/service/task/RahaTaskApplicationService.java:107` | 执行阶段编排 |

## 五、三个函数内部会触发哪些 Spark 作业

### 1）共同准备阶段

采集、训练、检测都会经过共同数据准备阶段：

| 阶段 | 代码位置 | Spark 作业来源 |
| --- | --- | --- |
| 数据加载 | `AbstractRahaWorkflow.java:51` | 读取表、SQL 或文件 |
| 列画像 | `AbstractRahaWorkflow.java:52` | 聚合、分组、分位数、频率统计 |
| 策略计划 | `AbstractRahaWorkflow.java:53` | 主要是本地计划生成和持久化 |
| 策略执行 | `AbstractRahaWorkflow.java:54` | OD、PVD 策略读取数据、分组、筛选并收集候选 |
| 特征生成 | `AbstractRahaWorkflow.java:55` | 宽表展开、分组、频率 join、收集特征行 |

### 2）数据加载阶段

FMDB 表和 SQL 输入由 `FmdbDatasetLoader` 处理：

| 代码位置 | Spark 动作 |
| --- | --- |
| `FmdbDatasetLoader.java:85` | 表输入走 `readTable` |
| `FmdbDatasetLoader.java:87` | SQL 输入走 `readSql` |
| `FmdbDatasetLoader.java:130` | `sparkSession.table(tableName)` |
| `FmdbDatasetLoader.java:147` | `sparkSession.sql(sqlText)` |

表或 SQL 本身是惰性读取，但后续立即触发 action：

| 代码位置 | Spark action |
| --- | --- |
| `RowIdentityService.java:51` | `source.count()` |
| `RowIdentityService.java:93` | `deduplicated.count()` |
| `RowIdentityService.java:198` | 业务键空值校验 `count()` |
| `RowIdentityService.java:219` | 业务键冲突统计 `count()` |
| `RowIdValidator.java:33` | 行数校验 `count()` |
| `RowIdValidator.java:40` | 空行标识校验 `count()` |
| `RowIdValidator.java:47` | 重复行标识校验 `count()` |
| `DatasetContentFingerprinter.java:38` | `toLocalIterator()` 拉取排序后的内容指纹输入 |

### 3）列画像阶段

`ColumnProfiler` 对每个字段执行 Spark 聚合：

| 代码位置 | Spark action |
| --- | --- |
| `ColumnProfiler.java:87` | 单列聚合 |
| `ColumnProfiler.java:122` | `first()` 触发聚合执行 |
| `ColumnProfiler.java:135` | 生成高频值统计 |
| `ColumnProfiler.java:142` | `collectAsList()` 拉回高频值 |

这意味着字段数越多，画像阶段触发的 Spark action 越多。

### 4）策略执行阶段

`StrategyExecutor` 会给每个策略设置 Spark job group，然后执行策略：

| 代码位置 | 说明 |
| --- | --- |
| `StrategyExecutor.java:87` | 取数据集所属 `SparkSession` |
| `StrategyExecutor.java:88` | 设置 Spark job group |
| `StrategyExecutor.java:91` | 调用策略 `detect` |
| `StrategyExecutor.java:111` | 超时时取消 Spark job group |

OD 和 PVD 策略里确实存在 Spark action：

| 策略 | 代码位置 | Spark action |
| --- | --- | --- |
| OD 低频 | `LowFrequencyStrategy.java:41` | `groupBy(...).count()` |
| OD 低频 | `LowFrequencyStrategy.java:46` | `collectAsList()` |
| PVD 长度异常 | `LengthAnomalyStrategy.java:45` | `count()` |
| PVD 长度异常 | `LengthAnomalyStrategy.java:49` | `approxQuantile(...)` |
| PVD 长度异常 | `LengthAnomalyStrategy.java:57` | `collectAsList()` |
| PVD 长度异常 | `LengthAnomalyStrategy.java:74` | `collectAsList()` |

其他 OD、PVD 策略也类似，普遍存在 `count()`、`groupBy()`、`collectAsList()`。

### 5）特征生成阶段

`FeatureAssembler` 会把字段展开为单元格级特征：

| 代码位置 | Spark action |
| --- | --- |
| `FeatureAssembler.java:228` | `selectExpr` 展开字段为长表 |
| `FeatureAssembler.java:233` | 按字段和值哈希分组统计频率 |
| `FeatureAssembler.java:237` | 频率结果 join 回单元格 |
| `FeatureAssembler.java:245` | `collectAsList()` 拉回特征行 |

检测 1000 行、6 个字段时，这里会生成约 6000 个单元格级特征行。

### 6）采集函数专属阶段

采集函数工作流为：

| 阶段 | 代码位置 | 说明 |
| --- | --- | --- |
| 共同准备阶段 | `SamplingWorkflow.java:78` | 加载、画像、策略、特征 |
| 聚类 | `SamplingWorkflow.java:79` | 基于特征做列内聚类 |
| 采样任务 | `SamplingWorkflow.java:80` | 生成待标注任务 |
| 结果登记 | `SamplingWorkflow.java:83` | 确认采样结果持久化 |

采样专属 Spark action 主要来自采样行回取和 FMDB 写入：

| 代码位置 | Spark action |
| --- | --- |
| `SampleRecordService.java:182` | 过滤被选中的采样行 |
| `SampleRecordService.java:185` | `collectAsList()` 拉回完整采样行 |
| `SparkSqlFmdbTableGateway.java:150` | 写入前 `pending.count()` |
| `SparkSqlFmdbTableGateway.java:157` | `insertInto(...)` |
| `SparkSqlFmdbTableGateway.java:159` | `saveAsTable(...)` |

采集函数还会生成 Excel 和 ZIP，这属于文件系统操作，不是 Spark 作业，但仍属于外部依赖调用。

### 7）训练函数专属阶段

训练函数工作流为：

| 阶段 | 代码位置 | 说明 |
| --- | --- | --- |
| 共同准备阶段 | `TrainingWorkflow.java:104` | 加载、画像、策略、特征 |
| 训练输入合并 | `TrainingWorkflow.java:110` | 持久化采样和标注批次合并时启用 |
| 聚类 | `TrainingWorkflow.java:114` | 用于标签传播 |
| 直接标签 | `TrainingWorkflow.java:115` | 导入直接标注 |
| 标签传播 | `TrainingWorkflow.java:116` | 基于聚类传播标签 |
| 模型训练 | `TrainingWorkflow.java:118` | 训练列模型 |
| 结果登记 | `TrainingWorkflow.java:124` | 确认训练结果持久化 |

训练专属 Spark action 主要来自公共输入缓存、MLlib 训练和元数据持久化：

| 代码位置 | Spark action |
| --- | --- |
| `RahaTrainService.java:268` | `inputFrame.persist(...)` |
| `RahaTrainService.java:269` | 缓存后 `count()` |
| `SparkMllibLogisticRegressionTrainer.java:61` | 创建训练 DataFrame |
| `SparkMllibLogisticRegressionTrainer.java:63` | 构建逻辑回归 |
| `SparkMllibLogisticRegressionTrainer.java:74` | `fit(trainingFrame)` 触发 MLlib Spark 作业 |
| `ModelReleaseManager.java:47` | 候选模型元数据保存 |
| `ModelReleaseManager.java:71` | 发布模型元数据保存 |

训练函数还会查找 HDFS 标注文件、导入 Excel、生成训练报告 ZIP，这些属于 HDFS 或文件系统操作，不是普通内存计算。

### 8）检测函数专属阶段

检测函数工作流为：

| 阶段 | 代码位置 | 说明 |
| --- | --- | --- |
| 共同准备阶段 | `DetectionWorkflow.java:64` | 加载、画像、策略、特征 |
| 已发布模型预测 | `DetectionWorkflow.java:65` | 加载模型并预测 |
| 结果登记 | `DetectionWorkflow.java:70` | 确认检测结果持久化 |

当前 `RahaDetectService` 的实际打分使用 `ColumnModelPredictor` 在 JVM 中对已收集的稀疏特征行计算，但检测函数仍然有大量 Spark 作业来自共同准备阶段和结果写入：

| 代码位置 | Spark action |
| --- | --- |
| `FeatureAssembler.java:245` | 检测特征行 `collectAsList()` |
| `RahaDetectService.java:145` | 保存检测结果 |
| `SparkSqlFmdbResultWriter.java:192` | 创建检测结果 DataFrame |
| `SparkSqlFmdbResultWriter.java:194` | 直接追加写入 FMDB 错误结果表 |
| `SparkSqlFmdbResultWriter.java:216` | 回读任务状态 |
| `SparkSqlFmdbResultWriter.java:226` | `collectAsList()` 读取最新任务状态 |

检测函数还会生成检测明细 Excel 和 ZIP。

## 六、为什么普通 `select udf(...)` 会冲突

普通 Spark SQL 执行 UDF 的路径可以简化理解为：

```text
Driver 提交 SQL 查询
Executor 执行查询任务
Executor 初始化或执行 Hive GenericUDF
GenericUDF 内部尝试访问 Driver 侧 SparkSession
GenericUDF 内部继续触发 Dataset action 或写表
```

当前已落盘日志证明直接 `spark-sql` 执行时，UDF 初始化栈出现在：

```text
org.apache.spark.scheduler.ResultTask.runTask
org.apache.spark.scheduler.Task.run
org.apache.spark.executor.Executor$TaskRunner.run
```

中文解释：该 UDF 初始化发生在 Spark 任务执行线程，而不是普通 Driver 主流程。

即使日志中显示 `Starting executor ID driver`，也不能理解为“这就是 Driver 线程”。本地模式或单 JVM 模式下，Executor 可能和 Driver 在同一个进程里，但 Spark 仍然区分 Driver 侧调度线程和 Executor 任务线程。`SparkSession` 的创建和访问仍会被 Spark 限制在 Driver 侧。

因此，当前问题不是 `inline` 的别名问题，也不是字段数量问题，而是执行模型问题：

1. `inline` 只决定返回对象如何展开。
2. `SELECT F_DW_DETCOLLECT(...)` 和 `SELECT inline(F_DW_DETCOLLECT(...))` 都会触发同一个 UDF 执行路径。
3. 只要 UDF 在 Executor 任务线程执行，内部访问 `SparkSession` 和触发 Spark action 就有风险。
4. `spark.sql.udf.local.mode=true` 当前只是代码里设置的配置值，不能保证 Spark 把 `GenericUDF` 放到 Driver 执行。

## 七、当前 Driver 侧入口为什么能成功

`RahaUdfDriverApp` 的执行路径不同：

| 代码位置 | 说明 |
| --- | --- |
| `RahaUdfDriverApp.java:46` | Driver 主方法创建 `SparkSession` |
| `RahaUdfDriverApp.java:50` | 设置 active session |
| `RahaUdfDriverApp.java:51` | 设置 default session |
| `RahaUdfDriverApp.java:75` | 直接创建 UDF 实例 |
| `RahaUdfDriverApp.java:78` | 手工初始化 UDF |
| `RahaUdfDriverApp.java:79` | 手工调用 `evaluate` |

此时 Spark 作业提交路径是：

```text
Driver main 方法
Raha UDF 业务服务
Spark Dataset action
Driver 调度 Spark 作业
Executor 执行数据计算
Driver 收集结果并返回 JSON
```

这个路径和普通 SQL UDF 的关键区别是：完整 Raha 工作流运行在 Driver 控制流中，内部 Spark action 由 Driver 正常提交。

现有验证文档 `doc/20260721/Raha三函数远端ZIP发布落地验证报告-202607211058.md` 已记录：

1. 直接 `spark-sql` CLI 执行 `GenericUDF` 会失败。
2. 使用 `RahaUdfDriverApp` 驱动侧执行三函数成功。
3. 三个函数均生成 ZIP 并返回结果。

## 八、能否调整为“只开放 UDF 给用户”

可以调整，但要先明确“只开放 UDF”有两种不同含义。

| 含义 | 是否可行 | 说明 |
| --- | --- | --- |
| 用户只看到 `F_DW_DETCOLLECT`、`F_DW_DETTRAIN`、`F_DW_DETRUN` 三个函数名 | 可行 | 可以让函数变成服务代理或平台命令入口 |
| 当前 `GenericUDF` 继续在 Executor 内执行完整 Raha 工作流 | 不建议 | 会继续碰到 SparkSession、嵌套 Spark action、长任务和多次执行风险 |

### 方案一：保留当前 Driver App，作为正式执行入口

做法：

1. 固化 `RahaUdfDriverApp`。
2. 用户通过平台任务、脚本或按钮传入函数名和请求 JSON。
3. Driver App 执行完成后返回 JSON 或写入结果表。

优点：

1. 与当前成功验证路径一致。
2. 改动小。
3. Spark 作业模型正确。

缺点：

1. 用户不是直接写 `select udf(...)`。
2. 如果产品明确要求 SQL 函数形式，该方案需要外层平台包装。

结论：

这是当前最稳的短期方案，但不完全满足“只开放 SQL UDF”。

### 方案二：做 Spark SQL Driver 侧命令或表值函数

做法：

1. 通过 Spark SQL 扩展、平台存储过程或 Driver 侧命令实现三函数语义。
2. 用户仍然用 SQL 触发，例如 `CALL` 或平台约定的表值函数。
3. 命令在 Driver 侧执行 `RahaDetectionUdfService`。

优点：

1. 用户体验仍然是 SQL。
2. 业务逻辑在 Driver 侧执行，符合 Spark 作业模型。
3. 可以同步返回表格结果。

缺点：

1. 不再是普通 `CREATE TEMPORARY FUNCTION` 注册的标量 `GenericUDF`。
2. 需要平台支持 SQL 扩展、存储过程或命令插件。
3. 部署复杂度高于普通 jar 注册。

结论：

如果要求“一条 SQL 同步返回最终结果”，这是最推荐的正式方案。

### 方案三：把三个 UDF 改成远程服务代理

做法：

1. 用户仍注册并调用 `F_DW_DETCOLLECT`、`F_DW_DETTRAIN`、`F_DW_DETRUN`。
2. UDF 内部不再获取 `SparkSession`，不再直接调用 `RahaDetectionUdfService`。
3. UDF 只做参数校验，然后调用一个 Driver 侧服务或任务提交服务。
4. Driver 侧服务可以复用 `RahaUdfDriverApp` 或直接复用 `RahaDetectionUdfService`。
5. 服务完成后把结果按原 `array<struct>` 形态返回给 UDF。

优点：

1. 用户侧仍然只有三个 UDF。
2. 普通 Executor UDF 只发 HTTP 或 RPC 请求，不在 Executor 内提交 Spark 作业。
3. 可以保留现有返回字段。

缺点：

1. 需要新增常驻服务或任务提交服务。
2. 需要处理鉴权、超时、幂等、排队、重试和结果查询。
3. 如果同步等待时间过长，SQL 查询会长时间占用会话。
4. 如果用户把 UDF 写到带多行输入的 SQL 里，可能产生多次服务调用，必须限制。

必要保护：

1. 入参必须是常量字符串，禁止按表行逐行调用。
2. 每个 SQL 查询最多触发一次任务。
3. 必须要求 `requestId`、`forceRunId` 或幂等键清晰。
4. 超过同步等待阈值时返回 `RUNNING` 和 `jobId`，不要无限阻塞。
5. 服务端必须落库保存结果，UDF 返回结果只作为摘要。

结论：

如果产品坚持“用户只调用 UDF 函数名”，这是最符合诉求的方案，但需要把当前 UDF 从“执行器”改成“代理”。

### 方案四：UDF 只提交异步任务，另一个 UDF 查询结果

做法：

1. `F_DW_DETCOLLECT`、`F_DW_DETTRAIN`、`F_DW_DETRUN` 只提交任务并立即返回 `jobId`。
2. 新增或复用查询函数按 `jobId` 查询状态和结果。
3. 真正的 Spark 作业由 Driver 侧任务服务执行。

优点：

1. 避免长 SQL 阻塞。
2. 更适合采样、训练、检测这种分钟级甚至更长的任务。
3. 幂等、重试、失败恢复更清晰。

缺点：

1. 用户需要两步查询。
2. 不满足“一条 SQL 立即拿最终 ZIP 和指标”的体验。

结论：

生产稳定性最好，但产品交互需要接受异步语义。

### 方案五：继续使用当前 GenericUDF 直接跑完整流程

不建议。

即使尝试延迟获取 SparkSession、修改 `initialize`、增加 `SparkSession.builder().getOrCreate()` 兜底，也不能改变 Spark 对 Driver 和 Executor 职责的基本约束。

该方案会继续存在：

1. Executor 线程不能访问 Driver 侧 `SparkSession`。
2. UDF 内部 Spark action 嵌套在外层 SQL 作业中。
3. 长时间阻塞 SQL task。
4. 失败时外层只看到 Spark stage failure，业务错误不清晰。
5. 如果查询不小心关联真实表，多行调用会重复提交大量任务。

## 九、针对当前需求的推荐改造路线

### 短期建议

1. 不把当前 `GenericUDF` 直接开放给用户普通 `spark-sql` 查询。
2. 文档中把当前调用方式明确标为“Driver 侧执行入口”，而不是普通 SQL UDF。
3. 保留 `RahaUdfDriverApp` 作为当前可用执行方式。
4. 在当前 UDF 初始化处补充更清晰的失败提示，避免用户看到晦涩的 Spark stage failure。
5. README 中 `SELECT * FROM F_DW_DETCOLLECT(...)` 这类表值函数写法需要修正或加前提说明。

### 中期建议

如果必须让用户只看到 UDF，建议采用“UDF 代理 + Driver 侧任务服务”：

1. 新增 Driver 侧 Raha 执行服务，服务内部复用当前 `RahaDetectionUdfService` 或 `RahaUdfDriverApp`。
2. 三个 UDF 改成轻量代理，不再直接获取 `SparkSession`。
3. 代理调用服务接口，传入原始请求字符串。
4. 服务端按幂等键执行或复用任务。
5. UDF 返回原有 `array<struct>`，字段保持兼容。
6. 对超时任务返回 `RUNNING`、`jobId`、`idempotentKey` 和结果查询入口。
7. 增加常量入参校验，禁止按输入表逐行调用。

### 长期建议

如果目标是标准 SQL 同步体验，建议做 Spark SQL Driver 侧命令或平台存储过程：

```text
SQL 入口
  Driver 侧解析请求
  Driver 侧执行 Raha 工作流
  Driver 侧返回结果行
```

这样可以保留 SQL 交互，又避免把复杂 Spark 作业塞进 Executor UDF。

## 十、最终判断

对“udf 三个函数可以直接查询 `select udf(...)` 并返回数据吗”的回答是：

1. 从返回结构看，可以表达返回数据。
2. 从当前执行环境看，不建议直接开放普通 `select udf(...)`。
3. 已有日志证明直接 `spark-sql` 会在 Executor 任务线程初始化 UDF，并因访问 SparkSession 失败。
4. 三个函数内部确实包含 Spark 作业，且采集、训练、检测都有，不是只有某一个函数有。
5. 如果只开放 UDF 给用户，推荐把 UDF 改成代理，由 Driver 侧服务执行真正 Spark 作业。

对“不能单独执行这 3 个函数是什么意思”的准确回答是：

不是说三函数不能独立作为业务动作执行；它们可以独立执行。真正限制是：不能以普通 Executor 标量 UDF 方式执行完整业务流程。当前已经验证成功的独立执行方式是 Driver 侧执行。

对“能否调整”的回答是：

可以调整，但调整边界是把 Spark 作业从 UDF Executor 线程中移出去。最符合“只开放 UDF 给用户”的方案是保留三个函数名，把函数实现改成服务代理；最适合同步 SQL 返回结果的正式方案是 Driver 侧 SQL 命令或平台存储过程。

## 十一、建议验收点

后续如果启动改造，建议按以下点验收：

1. 用户 SQL 中只注册并调用三个函数名。
2. UDF 内部不再调用 `SparkSession.builder().getOrCreate()`。
3. UDF 内部不再调用 `Dataset.count()`、`collectAsList()`、`write()`、`spark.sql()`、`spark.table()`。
4. 用户误把 UDF 写成按行调用时，系统能拒绝并给出清晰错误。
5. 采集、训练、检测的返回字段和当前 `RahaUdfFields` 保持兼容。
6. 直接 SQL 调用不会再出现 `SparkSession should only be created and accessed on the driver.`。
7. 长任务能返回 `jobId`、`idempotentKey`、`reused`、`currentSelectRule` 等排查字段。
8. Driver 侧服务日志能看到完整业务链路和 Spark job group。
