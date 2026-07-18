# SparkResourceManager 功能与场景示例

## 一、文件位置

源码文件：

`src/main/java/com/fiberhome/ml/raha/parallel/SparkResourceManager.java`

测试文件：

`src/test/java/com/fiberhome/ml/raha/parallel/SparkParallelResourceIntegrationTest.java`

## 二、它具体做什么

`SparkResourceManager` 是一个 Spark 资源保护工具类。它不负责具体算法，也不负责启动 Spark 任务，而是在调用方准备使用 Spark 广播变量或数据集缓存之前，先根据 `ResourceConfig` 做一次资源阈值判断。

它主要提供两个能力：

1. `broadcastIfAllowed`：对象估算大小不超过广播阈值时，才创建 Spark 广播变量。
2. `persistIfAllowed`：数据集估算大小不超过缓存阈值时，才按配置的缓存级别执行 `persist`。

换句话说，它的职责是把“能不能广播”“能不能缓存”这类资源判断集中到一个地方，避免业务代码里到处直接写：

```java
spark.sparkContext().broadcast(value);
dataset.persist(StorageLevel.MEMORY_AND_DISK());
```

## 三、依赖的资源配置

它依赖 `ResourceConfig` 中的三个字段：

| 配置字段 | 用途 |
| --- | --- |
| `broadcastThresholdBytes` | 控制允许广播的小对象最大估算字节数 |
| `cacheThresholdBytes` | 控制允许缓存的数据集最大估算字节数 |
| `cacheStorageLevel` | 控制缓存使用的 Spark 存储级别 |

`ResourceConfig` 同时还包含并行相关配置：

| 配置字段 | 用途 |
| --- | --- |
| `maxParallelStrategies` | 最大策略并发数 |
| `maxParallelColumns` | 最大字段并发数 |
| `stageTimeoutMillis` | 单阶段最大运行时间 |

不过 `SparkResourceManager` 本身只使用广播、缓存阈值和缓存级别，不负责线程并发控制。

## 四、方法一：broadcastIfAllowed

方法签名：

```java
public <T extends Serializable> Optional<Broadcast<T>> broadcastIfAllowed(
        SparkSession sparkSession,
        T value,
        long estimatedBytes,
        ResourceConfig config)
```

### 输入参数

| 参数 | 含义 |
| --- | --- |
| `sparkSession` | 当前 Spark 会话 |
| `value` | 准备广播到 Executor 的可序列化对象 |
| `estimatedBytes` | 调用方估算出的对象大小 |
| `config` | 资源配置 |

### 返回值

| 返回值 | 含义 |
| --- | --- |
| `Optional.of(broadcast)` | 对象大小未超过阈值，已经成功创建广播变量 |
| `Optional.empty()` | 对象大小超过阈值，拒绝广播 |

### 具体逻辑

1. 校验 `sparkSession`、`config` 不能为空，`estimatedBytes` 不能小于零。
2. 校验 `value` 不能为空。
3. 如果 `estimatedBytes > config.getBroadcastThresholdBytes()`，记录 `warn` 日志并返回 `Optional.empty()`。
4. 如果没有超过阈值，记录开始日志。
5. 通过 `JavaSparkContext.fromSparkContext(sparkSession.sparkContext()).broadcast(value)` 创建广播变量。
6. 记录完成日志，并返回 `Optional.of(broadcast)`。

### 示例一：小模型参数允许广播

假设要把列级模型参数广播给 Spark Executor：

```java
ResourceConfig config = new ResourceConfig(
        2, 2, 1024L * 1024L, "MEMORY_ONLY", 5000L);

ColumnModelArtifact model = loadModel("email");
long estimatedBytes = 64L * 1024L;

Optional<Broadcast<ColumnModelArtifact>> broadcast =
        new SparkResourceManager().broadcastIfAllowed(
                sparkSession, model, estimatedBytes, config);

if (broadcast.isPresent()) {
    ColumnModelArtifact modelOnExecutor = broadcast.get().value();
    // 后续可以在 Spark 算子或 UDF 中使用广播模型。
}
```

这个例子中，模型估算为 64KB，广播阈值为 1MB，所以允许广播。

### 示例二：大字典拒绝广播

假设某个字段的特征字典很大：

```java
FeatureDictionary dictionary = loadDictionary("address");
long estimatedBytes = 200L * 1024L * 1024L;

Optional<Broadcast<FeatureDictionary>> broadcast =
        manager.broadcastIfAllowed(sparkSession, dictionary, estimatedBytes, config);

if (!broadcast.isPresent()) {
    // 调用方应改用 join、分区处理或其他不依赖广播的方案。
}
```

如果配置里的 `broadcastThresholdBytes` 小于 200MB，这次广播会被拒绝，避免把大对象复制到每个 Executor 造成内存压力。

## 五、方法二：persistIfAllowed

方法签名：

```java
public boolean persistIfAllowed(Dataset<?> dataset,
                                long estimatedBytes,
                                ResourceConfig config)
```

### 输入参数

| 参数 | 含义 |
| --- | --- |
| `dataset` | 准备缓存的 Spark 数据集 |
| `estimatedBytes` | 调用方估算出的数据集大小 |
| `config` | 资源配置 |

### 返回值

| 返回值 | 含义 |
| --- | --- |
| `true` | 数据集大小未超过阈值，已经执行 `dataset.persist(...)` |
| `false` | 数据集大小超过阈值，拒绝缓存 |

### 具体逻辑

1. 校验 `dataset` 不能为空。
2. 通过 `dataset.sparkSession()` 获取 Spark 会话并复用公共校验。
3. 如果 `estimatedBytes > config.getCacheThresholdBytes()`，记录 `warn` 日志并返回 `false`。
4. 根据 `config.getCacheStorageLevel()` 转换 Spark 的 `StorageLevel`。
5. 调用 `dataset.persist(storageLevel)`。
6. 返回 `true`。

### 当前支持的缓存级别

| 配置值 | 对应 Spark 缓存级别 |
| --- | --- |
| `MEMORY_ONLY` | `StorageLevel.MEMORY_ONLY()` |
| `MEMORY_AND_DISK` | `StorageLevel.MEMORY_AND_DISK()` |
| `DISK_ONLY` | `StorageLevel.DISK_ONLY()` |

如果传入其他值，比如 `OFF_HEAP`，当前实现会抛出 `IllegalArgumentException`。

### 示例三：公共输入数据集允许缓存

在训练或特征准备阶段，同一个输入数据集会被策略执行、特征组装等流程反复扫描。如果数据集规模可控，可以缓存：

```java
Dataset<Row> inputFrame = request.getDataset().getDataFrame();
long estimatedBytes = estimateDatasetBytes(inputFrame);

boolean cached = manager.persistIfAllowed(
        inputFrame, estimatedBytes, request.getConfig().getResourceConfig());

if (cached) {
    inputFrame.count();
}
```

这里的 `count()` 用于触发 Spark 惰性计算，让缓存真正物化。

### 示例四：大数据集拒绝缓存

如果输入表非常大，缓存可能抢占大量 Executor 内存：

```java
long estimatedBytes = 50L * 1024L * 1024L * 1024L;

boolean cached = manager.persistIfAllowed(inputFrame, estimatedBytes, config);

if (!cached) {
    // 调用方继续走普通 Spark 惰性计算，不强制占用缓存资源。
}
```

超过 `cacheThresholdBytes` 时，方法返回 `false`，不会改变 `dataset.storageLevel()`。

## 六、当前项目中哪些场景已经用到

按当前代码检索结果，`SparkResourceManager` 在 `src/main/java` 生产代码中尚未被直接调用。

实际已用到的位置是集成测试：

`src/test/java/com/fiberhome/ml/raha/parallel/SparkParallelResourceIntegrationTest.java`

测试方法：

`shouldRefuseOversizedBroadcastAndCache`

这个测试验证了四件事：

1. 估算大小等于广播阈值时，允许广播。
2. 估算大小超过广播阈值时，拒绝广播并返回 `Optional.empty()`。
3. 估算大小超过缓存阈值时，拒绝缓存，数据集仍是 `StorageLevel.NONE()`。
4. 估算大小等于缓存阈值时，允许缓存，并按配置变成 `StorageLevel.MEMORY_ONLY()`。

测试里的配置是：

```java
ResourceConfig config = new ResourceConfig(
        2, 2, 10L, "MEMORY_ONLY", 20L, 5000L);
```

对应含义：

1. 最大策略并发数为 2。
2. 最大字段并发数为 2。
3. 广播阈值为 10 字节。
4. 缓存级别为 `MEMORY_ONLY`。
5. 缓存阈值为 20 字节。
6. 阶段超时为 5000 毫秒。

测试执行：

```java
Optional<Broadcast<String>> accepted = manager.broadcastIfAllowed(
        spark, "small", 10L, config);
Optional<Broadcast<String>> refused = manager.broadcastIfAllowed(
        spark, "large", 11L, config);
```

结果：

1. `"small"` 的估算大小等于 10 字节，允许广播。
2. `"large"` 的估算大小为 11 字节，超过阈值，拒绝广播。

测试还执行：

```java
Dataset<Row> dataset = spark.range(0L, 5L).toDF();
assertFalse(manager.persistIfAllowed(dataset, 21L, config));
assertTrue(manager.persistIfAllowed(dataset, 20L, config));
```

结果：

1. 21 字节超过缓存阈值 20 字节，拒绝缓存。
2. 20 字节等于缓存阈值，允许缓存。

## 七、当前项目中适合接入的场景

虽然当前生产代码没有直接调用 `SparkResourceManager`，但有几个地方非常适合接入它。

### 场景一：训练服务缓存公共输入

当前 `RahaTrainService` 中直接读取配置缓存级别并执行缓存：

```java
StorageLevel storageLevel = StorageLevel.fromString(
        request.getConfig().getResourceConfig().getCacheStorageLevel());
inputFrame.persist(storageLevel);
inputFrame.count();
```

适合改造成：

```java
long estimatedBytes = estimateDatasetBytes(inputFrame);
boolean cached = sparkResourceManager.persistIfAllowed(
        inputFrame, estimatedBytes, request.getConfig().getResourceConfig());
if (cached) {
    inputFrame.count();
}
```

适用原因：

1. 训练阶段会复用输入数据集。
2. 直接缓存大表有内存风险。
3. 使用 `cacheThresholdBytes` 后，可以让大数据集自动跳过缓存。

### 场景二：特征准备服务缓存公共输入

当前 `RahaFeaturePreparationService` 也会直接缓存输入数据：

```java
StorageLevel storageLevel = StorageLevel.fromString(
        request.getConfig().getResourceConfig().getCacheStorageLevel());
inputFrame.persist(storageLevel);
inputFrame.count();
```

适合用 `persistIfAllowed` 包一层资源阈值判断。

这类场景里，数据集会被多个策略或多个列任务重复读取，缓存能提高性能，但只有在数据量可控时才值得做。

### 场景三：批量 RVD 策略的中间数据缓存

`RvdBatchStrategyExecutor` 当前直接缓存长表中间结果：

```java
Dataset<Row> values = longValues(dataset).persist(StorageLevel.MEMORY_AND_DISK());
```

适合改造成受控缓存：

```java
Dataset<Row> values = longValues(dataset);
boolean cached = sparkResourceManager.persistIfAllowed(
        values, estimatedBytes, resourceConfig);
```

适用原因：

1. RVD 一对多策略可能生成较大的长表。
2. 多个规则共用同一份长表时，缓存收益明显。
3. 中间数据爆炸时，缓存反而会造成资源压力。

### 场景四：模型预测时广播列级模型

`PartitionedColumnModelPredictor` 当前把模型信息封进 UDF 对象里执行预测。对于较小模型，也可以通过广播变量传递模型参数。

适合的接入方式是：

```java
Optional<Broadcast<ColumnModelArtifact>> broadcast =
        sparkResourceManager.broadcastIfAllowed(
                featureFrame.sparkSession(), model, estimatedBytes, resourceConfig);

ColumnModelArtifact runtimeModel = broadcast.isPresent()
        ? broadcast.get().value()
        : model;
```

适用原因：

1. 小模型适合广播，减少闭包序列化成本。
2. 大模型不适合广播，可以继续使用现有分区预测或其他加载方式。
3. 返回 `Optional.empty()` 能让调用方显式选择降级路径。

### 场景五：特征字典或配置快照广播

某些特征组装或策略执行场景可能需要把小字典、小配置快照发到 Executor。

适合广播的对象包括：

1. 小型字段规则配置。
2. 小型特征编码字典。
3. 小型模型元数据。
4. 小型字段统计阈值表。

不适合广播的对象包括：

1. 大规模全表统计结果。
2. 超大字段字典。
3. 包含大量样本明细的数据结构。
4. 无法可靠估算大小的复杂对象。

## 八、为什么要返回 Optional 或 boolean

`broadcastIfAllowed` 返回 `Optional<Broadcast<T>>`，是为了表达“资源策略拒绝了这次广播，但这不一定是异常”。

例如：

```java
Optional<Broadcast<FeatureDictionary>> dictionaryBroadcast =
        manager.broadcastIfAllowed(spark, dictionary, estimatedBytes, config);

if (dictionaryBroadcast.isPresent()) {
    useBroadcast(dictionaryBroadcast.get());
} else {
    useJoinOrDriverSidePlan(dictionary);
}
```

`persistIfAllowed` 返回 `boolean`，是为了让调用方知道是否真的执行了缓存。

例如：

```java
boolean cached = manager.persistIfAllowed(dataset, estimatedBytes, config);
try {
    if (cached) {
        dataset.count();
    }
    runBusiness(dataset);
} finally {
    if (cached) {
        dataset.unpersist(false);
    }
}
```

这个模式能避免对没有缓存的数据集做不必要的释放，也能让日志和资源生命周期更清晰。

## 九、边界条件

| 场景 | 行为 |
| --- | --- |
| `sparkSession` 为空 | 抛出 `IllegalArgumentException` |
| `dataset` 为空 | 抛出 `IllegalArgumentException` |
| `value` 为空 | 抛出 `IllegalArgumentException` |
| `config` 为空 | 抛出 `IllegalArgumentException` |
| `estimatedBytes` 小于零 | 抛出 `IllegalArgumentException` |
| 估算大小等于阈值 | 允许广播或缓存 |
| 估算大小大于阈值 | 拒绝广播或缓存 |
| 不支持的缓存级别 | 抛出 `IllegalArgumentException` |

## 十、使用注意事项

1. `estimatedBytes` 由调用方传入，当前类不负责自动估算对象或数据集大小。
2. Spark 的 `persist` 是惰性的，调用 `persistIfAllowed` 后通常还需要一次 action，例如 `count()`，缓存才会物化。
3. `broadcastIfAllowed` 创建出的广播变量需要调用方在合适时机销毁，例如 `broadcast.destroy()`。
4. `persistIfAllowed` 只负责缓存，不负责最终 `unpersist`，调用方仍要管理释放。
5. 当前类支持的缓存级别比 `StorageLevel.fromString` 更窄，只有 `MEMORY_ONLY`、`MEMORY_AND_DISK`、`DISK_ONLY`。
6. 它是资源保护工具，不是性能万能开关；过小阈值会错过缓存收益，过大阈值会增加 Executor 内存压力。

## 十一、一句话总结

`SparkResourceManager` 的作用是把 Spark 广播和缓存从“直接执行”变成“先按资源配置判断，再决定是否执行”，当前主要被测试覆盖，生产代码中尚未直接接入；最适合接入训练、特征准备、RVD 中间数据缓存、模型预测广播等容易重复扫描或分发小对象的 Spark 场景。

