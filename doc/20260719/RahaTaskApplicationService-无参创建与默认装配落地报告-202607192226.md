# RahaTaskApplicationService 无参创建与默认装配落地报告

## 一、落地结论

已按 `doc/20260719/notes/RahaTaskApplicationService-无参创建与默认装配分析-202607192139.md` 的推荐方向完成落地。

当前支持以下调用方式：

```java
RahaTaskExecutionRequest request = RahaTaskExecutionRequest.training(
        config,
        dataLoadRequest,
        trainConfig,
        labelPropagationConfig,
        labels);

RahaTaskApplicationService applicationService =
        new RahaTaskApplicationService();

RahaTaskExecutionResult result = applicationService.execute(request);
```

同时保留更推荐的显式 Spark 会话入口：

```java
RahaTaskApplicationService applicationService =
        RahaTaskApplicationServiceFactory.createDefault(sparkSession);
```

## 二、主要改造点

1. `RahaTaskApplicationService` 增加无参构造器。

无参构造器不直接散落创建业务依赖，而是委托默认工厂创建组件。
如果当前线程没有活动 `SparkSession`，构造阶段会快速失败，并提示调用方使用显式 Spark 会话入口。

2. 增加 `RahaTaskApplicationServiceFactory`。

工厂集中创建默认运行依赖，包括任务编排器、工作流注册器、阶段仓储、数据加载器、训练服务、采样服务、检测服务、画像、策略、特征、聚类、标签传播、模型发布和 FMDB 结果写入相关组件。

3. 增加 `RahaStorageMode`。

新增默认运行时存储模式枚举：

| 模式 | 含义 |
| --- | --- |
| `IN_MEMORY` | 使用内存仓储和内存 FMDB 网关，适合开发、测试和短生命周期任务 |
| `FMDB` | 使用 Spark SQL FMDB 网关和 FMDB 相关仓储，适合接入 FMDB Catalog 的运行环境 |

4. 增加 `RoutingRahaDatasetLoader`。

工作流仍只依赖 `RahaDatasetLoader` 接口。
执行时由 `DataLoadRequest.getFormat()` 决定路由到文件加载器或 FMDB 加载器。

5. 增加默认配置项。

在 `src/main/resources/raha-defaults.properties` 中增加：

```properties
raha.runtime.storage-mode=IN_MEMORY
```

该配置独立于 `raha.persistence.enabled`，用于表达默认运行时装配时选择内存模式还是 FMDB 模式。

## 三、请求简化入口现状

`RahaTaskExecutionRequest` 已经支持以下简化训练入口：

```java
RahaTaskExecutionRequest.training(
        config,
        dataLoadRequest,
        trainConfig,
        labelPropagationConfig,
        labels);
```

也支持全部使用默认训练参数的入口：

```java
RahaTaskExecutionRequest.training(config, dataLoadRequest, labels);
```

采样任务支持：

```java
RahaTaskExecutionRequest.sampling(config, dataLoadRequest);
```

检测任务原有的简单入口保持不变：

```java
RahaTaskExecutionRequest.detection(config, dataLoadRequest);
```

## 四、验证结果

使用 JDK 8 执行以下命令通过：

```bash
mvn -q -DskipTests compile
```

使用 JDK 8 执行新增和相关测试通过：

```bash
mvn -q "-Dtest=RoutingRahaDatasetLoaderTest,RahaTaskApplicationServiceFactoryTest,RahaTaskExecutionRequestTest" test
```

使用 JDK 8 执行全量测试通过：

```bash
mvn -q test
```

全量测试输出中存在 Spark、Hive、Derby 和若干测试主动制造异常的日志噪声，但 Surefire 报告未发现失败或错误记录。

## 五、使用注意

1. `new RahaTaskApplicationService()` 依赖当前线程存在活动 `SparkSession`。

如果没有活动会话，应使用：

```java
RahaTaskApplicationServiceFactory.createDefault(sparkSession);
```

2. `DataLoadRequest` 只负责描述数据来源，不负责创建加载器。

加载器创建由默认工厂完成，实际加载时由 `RoutingRahaDatasetLoader` 根据 `DataFormat` 分派。

3. `IN_MEMORY` 与 `FMDB` 是默认装配模式，不等同于单独的持久化开关。

`raha.persistence.enabled` 继续控制 FMDB 物理表持久化行为，`raha.runtime.storage-mode` 控制默认装配时选择哪类运行时组件。

4. 当前工厂优先完成默认入口可用性和主链路装配。

后续如果要严格实现“所有仓储在 FMDB 模式下全部落 FMDB”，还需要继续补齐阶段仓储、策略仓储、特征仓储、画像仓储、聚类仓储和检测结果仓储的 FMDB 版本。
