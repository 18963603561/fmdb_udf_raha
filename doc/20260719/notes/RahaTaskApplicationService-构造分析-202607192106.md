# RahaTaskApplicationService 构造分析

## 结论

`RahaTaskApplicationService` 不适合自己内置一套默认对象后直接“无参 new”。
它当前是一个汇总型应用服务，构造时必须显式传入三个依赖：

- `RahaJobOrchestrator`
- `RahaWorkflowRegistry`
- `StageRepository`

源码里构造器已经把这三个参数全部设为必填，并且做了非空校验。

## 现状判断

### 1. 该类本身没有默认构造器

`src/main/java/com/fiberhome/ml/raha/service/task/RahaTaskApplicationService.java:28`

当前只有一个构造器，签名是：

```java
public RahaTaskApplicationService(RahaJobOrchestrator jobOrchestrator,
                                  RahaWorkflowRegistry workflowRegistry,
                                  StageRepository stageRepository)
```

这意味着：

- 不能直接无参实例化
- 也不存在“内部自动补默认实现”的逻辑
- 是否使用内存实现、持久化实现、测试实现，都由外部组装层决定

### 2. 它依赖的对象链比较长

`RahaTaskApplicationService` 只是入口编排层，不负责创建底层对象。
它调用链里的核心对象如下：

- `RahaJobOrchestrator`
- `RahaWorkflowRegistry`
- `StageRepository`

而 `RahaJobOrchestrator` 自身又需要：

- `RahaConfigValidator`
- `ConfigVersioner`
- `IdempotencyKeyGenerator`
- `RahaIdGenerator`
- `StageFailureDecider`
- `JobRepository`
- `StageRepository`
- `Clock`

所以如果想在这个类里硬塞默认值，后面还会继续补一串默认对象，最后会把“组装职责”压进业务服务里，不太合适。

## 目前项目里的装配方式

### 1. 测试里是手工组装

`src/test/java/com/fiberhome/ml/raha/service/task/RahaTaskApplicationServiceIntegrationTest.java:255-261`

测试中是这样做的：

- 先创建 `RahaJobOrchestrator`
- 再创建 `RahaWorkflowRegistry`
- 再创建 `RahaTaskApplicationService`

说明项目当前的默认风格就是“外层拼装，内层只接收依赖”。

### 2. 可直接复用的默认实现已经存在

下面这些类已经是现成默认实现，不需要再发明新对象：

- `DefaultStageRepository`
- `DefaultJobRepository`
- `InMemoryRahaRepository`
- 各个具体工作流 `TrainingWorkflow`、`SamplingWorkflow`、`DetectionWorkflow`

也就是说，缺的不是“业务默认能力”，而是“统一装配入口”。

## 是否建议在这里补默认对象

### 不建议直接加到 `RahaTaskApplicationService`

原因有三点：

1. 这个类的职责是执行任务，不是决定运行模式
2. 默认实现会让生产、测试、内存态、持久化态混在一起
3. 以后如果配置策略变化，改这个类会很重

### 更合适的做法

如果你确实希望外部能更省事地创建它，建议新增一个独立装配类，例如：

- `RahaTaskApplicationServiceFactory`
- 或者 `RahaTaskBootstrap`

由它来统一创建：

- 生产版 `RahaJobOrchestrator`
- 工作流注册器
- `StageRepository`

这样默认值只出现在装配层，不污染业务入口。

## 具体判断

### 如果你只是问“现在能不能直接 new”

可以，但必须自己准备好这三个参数。

### 如果你问“要不要给它补默认对象”

建议不要在这个类里补。
更推荐在外层增加一个工厂或启动装配类，把默认实现集中管理。

## 最终结论

`RahaTaskApplicationService` 不是默认对象创建点。
它应当保持“只消费依赖，不创建依赖”的形态；默认对象应该放到更外层的装配代码里。
