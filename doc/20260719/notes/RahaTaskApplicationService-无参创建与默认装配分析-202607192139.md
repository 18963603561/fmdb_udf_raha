# RahaTaskApplicationService 无参创建与默认装配分析

## 一、问题结论

`RahaTaskApplicationService` 可以增加无参构造器，使调用方能够使用：

```java
RahaTaskApplicationService applicationService =
        new RahaTaskApplicationService();
RahaTaskExecutionResult result = applicationService.execute(request);
```

但是，无参构造器不能只创建一个简单的默认对象。
它必须委托给一个顶层装配工厂，完整创建以下运行依赖：

- 任务编排器 `RahaJobOrchestrator`
- 工作流注册器 `RahaWorkflowRegistry`
- 阶段仓储 `StageRepository`
- `TrainingWorkflow`、`SamplingWorkflow` 和 `DetectionWorkflow`
- 工作流共享的数据加载、画像、策略、特征等服务
- 训练、采样、检测各自的业务服务
- 任务仓储和结果仓储
- 需要时使用的 `SparkSession`、FMDB 网关和 FMDB 配置

因此，推荐的结构是：

```text
调用方
  -> new RahaTaskApplicationService()
  -> RahaTaskApplicationService 默认构造器
  -> RahaTaskApplicationServiceFactory.createDefault()
  -> 创建全部默认依赖
  -> 返回可执行的 RahaTaskApplicationService
```

无参构造器只适合做一层薄委托，默认对象的实际创建逻辑应放在工厂或启动装配类中。

## 二、当前代码现状

### 1. 当前服务没有无参构造器

源码文件：

`src/main/java/com/fiberhome/ml/raha/service/task/RahaTaskApplicationService.java:28`

当前构造器需要三个参数：

```java
public RahaTaskApplicationService(RahaJobOrchestrator jobOrchestrator,
                                  RahaWorkflowRegistry workflowRegistry,
                                  StageRepository stageRepository)
```

所以当前代码中的下面写法不能编译：

```java
new RahaTaskApplicationService();
```

另外，Java 类名必须使用完整且正确的大小写。示例中的：

```java
new applicationService();
```

应改为：

```java
new RahaTaskApplicationService();
```

### 2. 服务本身不是完整的默认装配容器

`RahaTaskApplicationService` 只保存并调用三个依赖：

- `RahaJobOrchestrator`
- `RahaWorkflowRegistry`
- `StageRepository`

但 `RahaJobOrchestrator` 还要求：

- `RahaConfigValidator`
- `ConfigVersioner`
- `IdempotencyKeyGenerator`
- `RahaIdGenerator`
- `StageFailureDecider`
- `JobRepository`
- `StageRepository`
- `Clock`

`RahaWorkflowRegistry` 还必须提前注册对应任务类型的工作流。
如果只在应用服务内部创建编排器，仍然无法解决工作流和工作流业务服务的创建问题。

## 三、`DataLoadRequest` 能否创建 `RahaDatasetLoader`

### 1. 当前接口的真实职责

`RahaDatasetLoader` 是加载器接口，当前定义为：

```java
public interface RahaDatasetLoader {

    LoadedDataset load(DataLoadRequest request);
}
```

当前设计是“加载器已经创建，加载请求作为参数传入”，而不是“加载请求创建加载器”。
因此，下面两种职责需要区分：

| 对象 | 负责内容 |
| --- | --- |
| `DataLoadRequest` | 描述数据集标识、输入引用、表名、格式、字段范围和快照信息 |
| `RahaDatasetLoader` | 按请求实际读取数据，并生成 `LoadedDataset` |
| 加载器工厂或路由器 | 根据运行环境和请求格式选择具体加载器 |

### 2. 当前已经存在的具体加载器

当前至少有两种实现：

- `FileRahaDatasetLoader`：读取 CSV、JSON、Parquet 等文件数据
- `FmdbDatasetLoader`：读取 FMDB 表或只读 FMDB SQL

`DataLoadRequest.getFormat()` 可以用于判断输入数据源类型，但它不能解决具体加载器的全部构造依赖。
两个加载器都需要 `SparkSession`，并且还需要行身份、模式哈希、快照元数据等辅助服务。

### 3. 推荐的加载器选择方式

建议增加一个加载器路由器，或者增加一个加载器工厂：

```java
public final class RoutingRahaDatasetLoader implements RahaDatasetLoader {

    private final RahaDatasetLoader fileLoader;
    private final RahaDatasetLoader fmdbLoader;

    @Override
    public LoadedDataset load(DataLoadRequest request) {
        if (request.getFormat().isFileFormat()) {
            return fileLoader.load(request);
        }
        return fmdbLoader.load(request);
    }
}
```

这样工作流只依赖一个 `RahaDatasetLoader`，每一次执行时由 `DataLoadRequest` 决定实际读取路径。
如果在工厂层根据请求创建加载器，则需要保证创建出的工作流或阶段能够拿到这个请求对应的加载器，现有 `AbstractRahaWorkflow` 和 `DataLoadStageHandler` 并不是这个形态。

所以从当前代码改造成本看，路由加载器比“每个请求重新创建一个加载器”更合适。

### 4. `DataLoadRequest` 不适合填充关键默认值

当前 `DataLoadRequest` 的以下字段是业务必填项：

- `datasetId`
- `inputReference`
- `tableName`
- `rowIdentityConfig`
- `format`

这些字段没有安全的通用默认值。
例如默认数据路径、默认表名或默认数据格式都可能导致任务读取错误数据。

可以为 `options`、字段集合或快照辅助信息提供空集合、空值等默认行为，但不应为输入数据身份和来源格式静默填值。

## 四、默认内存存储和 FMDB 存储能否切换

### 1. 当前已有内存实现

项目已有 `InMemoryRahaRepository`，它适合保存任务、阶段和业务记录的内存状态。
项目也已有 `InMemoryFmdbTableGateway`，它用于在 Spark 数据集基础上模拟 FMDB 表网关。

这两个类都不是全局自动生效的默认实现，必须由装配代码主动创建并注入。

### 2. 当前已有 FMDB 实现

项目已有 `SparkSqlFmdbTableGateway`，它通过 Spark SQL Catalog 读写 FMDB 表。
它的单参数构造器会调用：

```java
FmdbPersistenceConfig.fromDefaults()
```

`FmdbPersistenceConfig` 当前读取 `raha-defaults.properties` 中的：

- `raha.persistence.enabled`
- `raha.persistence.schema.auto-create`
- 各物理表持久化开关
- 各列级产物持久化开关

但这些配置主要表达“FMDB 是否允许持久化以及哪些表允许写入”，并不表达“整个任务运行时选择内存后端还是 FMDB 后端”。

特别是：

```properties
raha.persistence.enabled=false
```

更接近“关闭 FMDB 写入”的语义，不等价于“自动切换到所有内存实现”。
如果要支持完整的内存模式，任务仓储、阶段仓储、样本仓储、模型仓储、检测结果仓储和列级产物仓储都需要统一切换。

### 3. 建议新增独立的后端模式配置

建议在 `raha-defaults.properties` 增加一个明确表示后端选择的配置，例如：

```properties
raha.runtime.storage-mode=IN_MEMORY
```

建议支持两个值：

| 配置值 | 运行含义 |
| --- | --- |
| `IN_MEMORY` | 使用内存仓储和内存 FMDB 网关，适合单元测试、示例和短生命周期任务 |
| `FMDB` | 使用 Spark SQL FMDB 网关及 FMDB 仓储，适合生产运行和跨进程复用 |

这个配置应由 `RahaTaskApplicationServiceFactory` 或更上层的运行时装配类解析。
不建议让 `TrainingWorkflow`、`SamplingWorkflow` 和 `DetectionWorkflow` 自己读取这个配置。

### 4. 存储模式不能只切换一个网关

一个完整的运行模式至少要统一考虑：

- `JobRepository`
- `StageRepository`
- 样本记录仓储
- 标注记录仓储
- 模型元数据仓储
- 检测结果仓储
- 列画像、策略和特征等产物仓储
- `FmdbTableGateway`
- `RahaDatasetLoader`

如果只把 `FmdbTableGateway` 替换为 `InMemoryFmdbTableGateway`，但任务状态或模型仓储仍然使用 FMDB，系统会形成混合后端。
混合后端并非一定错误，但必须显式设计，不能由 `raha.persistence.enabled` 隐式产生。

## 五、无参构造器的可行方案

### 方案一：应用服务无参构造器委托默认工厂

可以采用下面的结构：

```java
private RahaTaskApplicationService(DefaultComponents components) {
    this(components.getJobOrchestrator(),
            components.getWorkflowRegistry(),
            components.getStageRepository());
}

public RahaTaskApplicationService() {
    this(RahaTaskApplicationServiceFactory.createDefaultComponents());
}
```

其中 `DefaultComponents` 应是工厂返回的默认装配结果，至少包含编排器、工作流注册器和阶段仓储。
这样无参构造器仍然只是薄委托，不会把一整套依赖创建逻辑散落到应用服务中。

### 方案二：保留显式构造器，增加无参静态工厂

更推荐对外提供：

```java
RahaTaskApplicationService applicationService =
        RahaTaskApplicationServiceFactory.createDefault(sparkSession);
```

同时保留现有构造器：

```java
new RahaTaskApplicationService(jobOrchestrator,
        workflowRegistry, stageRepository);
```

这种方式可以明确提醒调用方：默认对象需要运行环境，例如 `SparkSession` 和 FMDB Catalog 配置。

### 方案三：无参构造器自动查找全局 `SparkSession`

理论上可以在无参工厂中尝试查找当前活动的 `SparkSession`。
但这会带来以下问题：

- 没有活动会话时，构造阶段才失败，错误位置不直观
- 多个 Spark 会话并存时，可能拿到错误会话
- 测试之间可能共享会话和仓储状态
- FMDB Catalog、数据库连接和 Spark 配置由谁负责不清晰
- 应用服务构造行为依赖线程或进程全局状态

因此，不建议把查找全局 `SparkSession` 作为生产默认行为。
如果保留无参构造器，应明确要求运行环境已经完成 Spark 初始化，并在找不到会话时抛出带上下文的异常。

## 六、推荐的最终装配层次

建议把职责拆成四层：

```text
RahaTaskApplicationService
  -> 只负责接收请求并执行任务

RahaTaskApplicationServiceFactory
  -> 创建任务编排器、工作流注册器和阶段仓储

RahaRuntimeComponentFactory
  -> 根据 storage-mode 创建内存或 FMDB 仓储、网关和服务

RahaDatasetLoaderRouter
  -> 根据 DataLoadRequest.format 选择文件加载器或 FMDB 加载器
```

推荐的创建入口如下：

```java
RahaTaskApplicationService applicationService =
        RahaTaskApplicationServiceFactory.createDefault(sparkSession);
```

工厂内部的主要流程应为：

1. 读取 `RahaDefaultConfigProvider.properties()`。
2. 解析 `raha.runtime.storage-mode`。
3. 创建 `InMemoryRahaRepository` 或 FMDB 统一仓储实现。
4. 创建 `RahaDatasetLoaderRouter`。
5. 创建三个工作流及其业务服务。
6. 创建 `RahaWorkflowRegistry`。
7. 创建 `RahaJobOrchestrator`。
8. 创建并返回 `RahaTaskApplicationService`。

## 七、当前支持的简化调用

现在已经为 `RahaTaskExecutionRequest` 增加了简化训练入口。
你给出的五参数顺序可以直接使用：

```java
RahaTaskExecutionRequest request = RahaTaskExecutionRequest.training(
        config,
        dataLoadRequest,
        trainConfig,
        labelPropagationConfig,
        labels);
```

这个重载仍然要求调用方传入训练配置和标签传播配置，但会自动使用：

- `LabelPropagationMethod.HOMOGENEITY`
- 模型名称前缀 `raha`
- 不使用评估器
- 不使用持久化 c1 和标注批次输入

如果希望训练配置也全部使用默认值，可以使用：

```java
RahaTaskExecutionRequest request = RahaTaskExecutionRequest.training(
        config,
        dataLoadRequest,
        labels);
```

该入口会自动使用 `LabelPropagationConfig.defaults()` 和
`LogisticRegressionTrainingConfig.defaults()`。

完整参数入口仍然保留，适用于需要显式传播方式、模型名称前缀或评估器的场景：

```java
RahaTaskExecutionRequest request = RahaTaskExecutionRequest.training(
        config,
        dataLoadRequest,
        labels,
        propagationMethod,
        labelPropagationConfig,
        trainConfig,
        modelNamePrefix);
```

然后再执行：

```java
RahaTaskApplicationService applicationService =
        RahaTaskApplicationServiceFactory.createDefault(sparkSession);
RahaTaskExecutionResult result = applicationService.execute(request);
```

采样任务也增加了简化入口：

```java
RahaTaskExecutionRequest request = RahaTaskExecutionRequest.sampling(
        config,
        dataLoadRequest);
```

该入口使用空的已有标签集合和第 1 轮采样。
如果需要其他轮次或已有标签，继续使用原有四参数入口。

## 八、最终建议

1. 可以支持 `new RahaTaskApplicationService()`，但无参构造器只能委托默认装配工厂。
2. 更推荐 `RahaTaskApplicationServiceFactory.createDefault(sparkSession)`，因为它能显式表达运行环境依赖。
3. `RahaTaskExecutionRequest` 已提供简化训练和采样入口，但高级参数仍保留完整重载。
4. `DataLoadRequest` 可以作为加载器路由依据，但不应单独承担加载器创建职责。
5. 建议增加 `raha.runtime.storage-mode`，不要复用 `raha.persistence.enabled` 表达后端选择。
6. 内存和 FMDB 模式必须在装配层统一选择，不能只替换 `FmdbTableGateway`。
7. 保持三个工作流的显式构造器，不建议给它们增加隐藏整条依赖链的无参构造器。
8. 生产模式下应由调用方传入 `SparkSession`，不建议默认从全局环境隐式查找。

## 九、结论

目标调用方式可以实现，但推荐最终形态是：

```java
RahaTaskApplicationService applicationService =
        RahaTaskApplicationServiceFactory.createDefault(sparkSession);
```

如果产品层面必须保留 `new RahaTaskApplicationService()`，则应将它限定为一个便捷入口，并在内部调用同一个默认工厂；所有默认配置、存储模式、Spark 会话检查和依赖创建仍然集中在工厂中。

这样既能让调用方减少样板代码，也不会让应用服务和三个工作流承担基础设施装配职责。
