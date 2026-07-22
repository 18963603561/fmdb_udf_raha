package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.algorithm.ClusteringProviderResolver;
import com.fiberhome.ml.raha.cluster.algorithm.ColumnClusterer;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.core.RahaStorageMode;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.config.validation.ConfigVersioner;
import com.fiberhome.ml.raha.config.validation.RahaConfigValidator;
import com.fiberhome.ml.raha.data.loader.FileRahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.RoutingRahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.identity.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.metadata.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.metadata.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.metadata.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.job.execution.RahaJobOrchestrator;
import com.fiberhome.ml.raha.job.execution.StageFailureDecider;
import com.fiberhome.ml.raha.job.id.DefaultRahaIdGenerator;
import com.fiberhome.ml.raha.job.id.IdempotencyKeyGenerator;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.model.ColumnModelStore;
import com.fiberhome.ml.raha.model.FileColumnModelStore;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.release.ColumnModelCompatibilityValidator;
import com.fiberhome.ml.raha.model.release.ColumnModelMetadataFactory;
import com.fiberhome.ml.raha.model.release.ColumnModelVersioner;
import com.fiberhome.ml.raha.model.release.ModelReleaseManager;
import com.fiberhome.ml.raha.model.release.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.model.training.AdaptiveColumnModelTrainer;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainer;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataBuilder;
import com.fiberhome.ml.raha.model.training.SparkMllibLogisticRegressionTrainer;
import com.fiberhome.ml.raha.model.training.WeightedRuleFallbackTrainer;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.repository.adapter.DefaultAnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultCellLabelRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultModelSetRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultSnapshotCheckpointRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.SparkSqlFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbJobRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbAnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbSampleRecordRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbSnapshotCheckpointRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbStageRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingArtifactRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultPersistenceVerifier;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.DefaultFmdbSchemaResolver;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.CellLabelRepository;
import com.fiberhome.ml.raha.repository.port.ClusterRepository;
import com.fiberhome.ml.raha.repository.port.ColumnProfileRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.FeatureRepository;
import com.fiberhome.ml.raha.repository.port.JobRepository;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.ModelSetRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.sampling.service.SampleRecordService;
import com.fiberhome.ml.raha.sampling.service.SamplingService;
import com.fiberhome.ml.raha.sampling.service.SamplingVersioner;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
import com.fiberhome.ml.raha.service.train.RahaTrainService;
import com.fiberhome.ml.raha.service.train.TrainingArtifactMaterializationService;
import com.fiberhome.ml.raha.service.train.TrainingInputMergeService;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchExecutionCoordinator;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchPlanner;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchSchemaResolver;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceVerifier;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * 统一创建 Raha 任务应用服务及其默认运行时依赖。
 *
 * 工厂集中处理 Spark 会话、数据加载器、工作流、任务仓储和物理网关的装配，
 * 业务应用服务本身只保留执行职责。
 */
public final class RahaTaskApplicationServiceFactory {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RahaTaskApplicationServiceFactory.class);
    /** 默认模型文件目录。 */
    private static final Path DEFAULT_MODEL_DIRECTORY = Paths.get(
            System.getProperty("java.io.tmpdir"), "raha-models");

    /**
     * 工具类构造方法。
     *
     * <p>该工厂只提供静态装配入口，不允许外部实例化。</p>
     */
    private RahaTaskApplicationServiceFactory() {
    }

    /**
     * 使用当前活动 Spark 会话和默认模型目录创建应用服务。
     *
     * <p>适用于已经通过 {@link SparkSession#getActiveSession()} 注册活动会话的简单启动场景。
     * 如果当前线程没有活动 Spark 会话，会抛出 {@link IllegalStateException}。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * SparkSession spark = SparkSession.builder()
     *         .appName("raha")
     *         .master("local[*]")
     *         .getOrCreate();
     * RahaTaskApplicationService service =
     *         RahaTaskApplicationServiceFactory.createDefault();
     * }</pre>
     *
     * @return 使用默认组件装配完成的任务应用服务
     */
    public static RahaTaskApplicationService createDefault() {
        return new RahaTaskApplicationService(createDefaultComponents());
    }

    /**
     * 使用指定 Spark 会话和默认模型目录创建应用服务。
     *
     * <p>默认模型目录为系统临时目录下的 {@code raha-models}，仅内存存储模式下用于保存模型文件。
     * FMDB 存储模式会将模型产物写入物理表。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * SparkSession spark = SparkSession.builder()
     *         .appName("raha")
     *         .master("local[*]")
     *         .getOrCreate();
     * RahaTaskApplicationService service =
     *         RahaTaskApplicationServiceFactory.createDefault(spark);
     * }</pre>
     *
     * @param sparkSession Spark 会话，不能为空
     * @return 使用默认模型目录装配完成的任务应用服务
     */
    public static RahaTaskApplicationService createDefault(
            SparkSession sparkSession) {
        return new RahaTaskApplicationService(createDefaultComponents(
                sparkSession, DEFAULT_MODEL_DIRECTORY));
    }

    /**
     * 使用指定 Spark 会话和模型目录创建应用服务。
     *
     * <p>调用方可以显式控制内存模式下模型文件的落盘位置，便于测试、临时运行或隔离不同任务。
     * FMDB 存储模式下该参数仍要求非空，但不会作为模型产物最终存储位置。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * Path modelDirectory = Paths.get("/tmp/raha-models");
     * RahaTaskApplicationService service =
     *         RahaTaskApplicationServiceFactory.createDefault(spark, modelDirectory);
     * }</pre>
     *
     * @param sparkSession Spark 会话，不能为空
     * @param modelDirectory 模型文件目录，不能为空；仅内存模式实际使用
     * @return 使用指定模型目录装配完成的任务应用服务
     */
    public static RahaTaskApplicationService createDefault(
            SparkSession sparkSession, Path modelDirectory) {
        return new RahaTaskApplicationService(createDefaultComponents(
                sparkSession, modelDirectory));
    }

    /**
     * 使用显式存储模式创建应用服务，主要用于部署装配和测试。
     *
     * <p>该入口绕过配置文件中的 {@code raha.runtime.storage-mode}，直接按入参选择 FMDB 或内存模式。
     * 适用于集成测试、灰度验证或启动层已经完成配置解析的场景。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * RahaTaskApplicationService service =
     *         RahaTaskApplicationServiceFactory.createDefault(
     *                 spark, modelDirectory, RahaStorageMode.FMDB);
     * }</pre>
     *
     * @param sparkSession Spark 会话，不能为空
     * @param modelDirectory 模型文件目录，不能为空；仅内存模式实际使用
     * @param storageMode 物理存储模式，不能为空
     * @return 按指定存储模式装配完成的任务应用服务
     */
    public static RahaTaskApplicationService createDefault(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode) {
        return new RahaTaskApplicationService(createDefaultComponents(
                sparkSession, modelDirectory, storageMode));
    }

    /**
     * 使用已经组装好的依赖创建应用服务。
     *
     * <p>该入口适用于调用方完全接管依赖创建的场景，例如 Spring 容器装配、专项单元测试或
     * 自定义工作流扩展。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * RahaTaskApplicationService service =
     *         RahaTaskApplicationServiceFactory.create(
     *                 jobOrchestrator, workflowRegistry, stageRepository);
     * }</pre>
     *
     * @param jobOrchestrator 任务编排器，负责任务提交和阶段推进
     * @param workflowRegistry 工作流注册器，负责按任务类型查找工作流
     * @param stageRepository 阶段仓储，负责读取阶段状态
     * @return 使用外部依赖创建的统一任务应用服务
     */
    public static RahaTaskApplicationService create(
            RahaJobOrchestrator jobOrchestrator,
            RahaWorkflowRegistry workflowRegistry,
            StageRepository stageRepository) {
        return new RahaTaskApplicationService(jobOrchestrator,
                workflowRegistry, stageRepository);
    }

    /**
     * 使用工作流集合创建应用服务。
     *
     * <p>该方法会将工作流集合封装为 {@link RahaWorkflowRegistry}。
     * 工作流集合不能为空，否则无法根据任务类型分派执行链路。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * RahaTaskApplicationService service =
     *         RahaTaskApplicationServiceFactory.create(
     *                 jobOrchestrator, stageRepository, workflows);
     * }</pre>
     *
     * @param jobOrchestrator 任务编排器，负责任务生命周期控制
     * @param stageRepository 阶段仓储，负责阶段状态查询
     * @param workflows 工作流集合，不能为空且至少包含一个工作流
     * @return 使用工作流集合创建的统一任务应用服务
     */
    public static RahaTaskApplicationService create(
            RahaJobOrchestrator jobOrchestrator,
            StageRepository stageRepository,
            Collection<RahaWorkflow> workflows) {
        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("Raha 工作流集合不能为空");
        }
        return create(jobOrchestrator, new RahaWorkflowRegistry(workflows),
                stageRepository);
    }

    /**
     * 使用工作流数组创建应用服务。
     *
     * <p>该方法是集合入口的可变参数便利封装，适合直接传入少量自定义工作流。
     * 工作流数组不能为空，否则抛出 {@link IllegalArgumentException}。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * RahaTaskApplicationService service =
     *         RahaTaskApplicationServiceFactory.create(
     *                 jobOrchestrator, stageRepository,
     *                 trainingWorkflow, detectionWorkflow);
     * }</pre>
     *
     * @param jobOrchestrator 任务编排器，负责任务提交和状态推进
     * @param stageRepository 阶段仓储，负责阶段状态查询
     * @param workflows 工作流数组，不能为空且至少包含一个工作流
     * @return 使用工作流数组创建的统一任务应用服务
     */
    public static RahaTaskApplicationService create(
            RahaJobOrchestrator jobOrchestrator,
            StageRepository stageRepository,
            RahaWorkflow... workflows) {
        if (workflows == null || workflows.length == 0) {
            throw new IllegalArgumentException("Raha 工作流数组不能为空");
        }
        return create(jobOrchestrator, stageRepository,
                Arrays.asList(workflows));
    }

    /**
     * 创建无参应用服务所需的默认组件。
     *
     * <p>该方法会解析当前活动 Spark 会话，并使用默认模型目录和配置文件中的存储模式。</p>
     *
     * @return 默认组件集合，包含任务编排器、工作流注册器和运行时仓储
     */
    static DefaultComponents createDefaultComponents() {
        return createDefaultComponents(resolveActiveSparkSession(),
                DEFAULT_MODEL_DIRECTORY);
    }

    /**
     * 根据默认配置完成运行时装配。
     *
     * <p>该方法从默认配置读取 {@code raha.runtime.storage-mode}，再委托显式存储模式入口完成装配。
     * 入参为空会立即失败，避免后续 Spark、文件系统或 FMDB 调用出现难以定位的空指针问题。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * DefaultComponents components =
     *         RahaTaskApplicationServiceFactory.createDefaultComponents(
     *                 spark, Paths.get("/tmp/raha-models"));
     * }</pre>
     *
     * @param sparkSession Spark 会话，不能为空
     * @param modelDirectory 模型文件目录，不能为空
     * @return 按默认配置装配完成的组件集合
     */
    static DefaultComponents createDefaultComponents(
            SparkSession sparkSession, Path modelDirectory) {
        if (sparkSession == null || modelDirectory == null) {
            throw new IllegalArgumentException("默认运行时 Spark 会话和模型目录不能为空");
        }
        RahaStorageMode storageMode = RahaDefaultConfigProvider.properties()
                .getEnum("raha.runtime.storage-mode", RahaStorageMode.class);
        return createDefaultComponents(sparkSession, modelDirectory, storageMode);
    }

    /**
     * 使用显式存储模式完成默认运行时组件装配。
     *
     * <p>该方法是默认工厂的核心装配流程，依次创建基础设施、仓储、数据准备服务、任务服务、
     * 工作流注册器和任务编排器。FMDB 模式会装配物理表仓储，内存模式会装配进程内仓储并使用文件模型库。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * DefaultComponents components =
     *         RahaTaskApplicationServiceFactory.createDefaultComponents(
     *                 spark, modelDirectory, RahaStorageMode.IN_MEMORY);
     * }</pre>
     *
     * @param sparkSession Spark 会话，不能为空
     * @param modelDirectory 模型文件目录，不能为空；仅内存模式实际使用
     * @param storageMode 运行时存储模式，不能为空
     * @return 装配完成的默认组件集合
     */
    static DefaultComponents createDefaultComponents(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode) {
        validateDefaultRuntimeInputs(sparkSession, modelDirectory, storageMode);
        LOGGER.info("开始创建 Raha 默认运行时，storageMode={}，modelDirectory={}",
                storageMode, modelDirectory.toAbsolutePath().normalize());

        // 阶段一：创建运行时基础设施，决定时间源、FMDB 持久化配置和物理表网关。
        RuntimeInfrastructure infrastructure = createRuntimeInfrastructure(
                sparkSession, storageMode);

        // 阶段二：按存储模式创建六类运行时仓储，FMDB 模式不再注入默认内存适配器。
        RahaRepository repository = new InMemoryRahaRepository();
        RuntimeRepositories runtimeRepositories = createRuntimeRepositories(
                sparkSession, storageMode, infrastructure, repository);
        StageRepository stageRepository = runtimeRepositories.getStageRepository();

        // 阶段三：创建数据准备领域服务，训练、采样和检测工作流都会复用这些能力。
        PreparationServices preparationServices = createPreparationServices(
                sparkSession, repository, runtimeRepositories, infrastructure.getClock());

        // 阶段四：创建任务业务服务，按存储模式选择模型元数据和模型产物存储位置。
        TaskServices taskServices = createTaskServices(sparkSession, modelDirectory,
                storageMode, infrastructure, repository, runtimeRepositories,
                preparationServices);

        // 阶段五：创建数据加载路由器和三类工作流，形成任务类型到阶段链路的注册表。
        RahaWorkflowRegistry workflowRegistry = createWorkflowRegistry(
                sparkSession, storageMode, infrastructure, preparationServices,
                taskServices, runtimeRepositories);

        // 阶段六：创建任务编排器，负责幂等提交、阶段执行、状态推进和结果登记。
        RahaJobOrchestrator orchestrator = createJobOrchestrator(storageMode,
                repository, stageRepository, infrastructure);
        LOGGER.info("Raha 默认运行时创建完成，storageMode={}，workflowCount={}",
                storageMode, 3);
        ColumnBatchExecutionCoordinator columnBatchCoordinator =
                new ColumnBatchExecutionCoordinator(
                        new ColumnBatchSchemaResolver(sparkSession),
                        new ColumnBatchPlanner(), infrastructure.getClock());
        return new DefaultComponents(orchestrator, workflowRegistry,
                runtimeRepositories, taskServices.getRequestFactory(),
                columnBatchCoordinator);
    }

    /**
     * 校验默认运行时装配入口的必要参数。
     *
     * <p>该方法只检查不可为空的硬性边界，不负责校验目录是否存在或 FMDB 表是否可写；
     * 这些外部资源状态由后续具体适配器在访问时处理。</p>
     *
     * @param sparkSession Spark 会话，不能为空
     * @param modelDirectory 模型文件目录，不能为空
     * @param storageMode 运行时存储模式，不能为空
     */
    private static void validateDefaultRuntimeInputs(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode) {
        if (sparkSession == null || modelDirectory == null || storageMode == null) {
            throw new IllegalArgumentException("默认运行时 Spark 会话、模型目录和存储模式不能为空");
        }
    }

    /**
     * 创建运行时基础设施组件。
     *
     * <p>基础设施包含统一时间源、FMDB 持久化配置、物理表网关和结果写入器。
     * FMDB 模式使用 Spark SQL 网关访问真实物理表，内存模式使用进程内网关支撑本地测试。</p>
     *
     * @param sparkSession Spark 会话，供 Spark SQL 网关和结果写入器使用
     * @param storageMode 运行时存储模式，用于选择物理表网关实现
     * @return 运行时基础设施组件集合
     */
    private static RuntimeInfrastructure createRuntimeInfrastructure(
            SparkSession sparkSession,
            RahaStorageMode storageMode) {
        Clock clock = Clock.systemUTC();
        FmdbPersistenceConfig persistenceConfig = FmdbPersistenceConfig.fromDefaults();
        // FMDB 模式使用 Spark SQL Catalog 网关；内存模式使用进程内表网关承接测试和示例任务。
        FmdbTableGateway tableGateway = storageMode == RahaStorageMode.FMDB
                ? new SparkSqlFmdbTableGateway(sparkSession, persistenceConfig)
                : new InMemoryFmdbTableGateway(sparkSession);
        FmdbResultWriter resultWriter = resultWriter(
                sparkSession, tableGateway, clock, persistenceConfig);
        return new RuntimeInfrastructure(clock, persistenceConfig,
                tableGateway, resultWriter);
    }

    /**
     * 基于运行时基础设施创建运行时仓储集合。
     *
     * <p>该方法从 {@link RuntimeInfrastructure} 中取出物理依赖后，委托显式依赖入口完成仓储选择，
     * 便于测试时直接验证具体仓储类型。</p>
     *
     * @param sparkSession Spark 会话，供 FMDB 仓储执行 Spark SQL
     * @param storageMode 运行时存储模式
     * @param infrastructure 运行时基础设施组件，不能为空
     * @param repository 内存模式共享的聚合仓储，不能为空
     * @return 当前存储模式下实际使用的仓储集合
     */
    private static RuntimeRepositories createRuntimeRepositories(
            SparkSession sparkSession,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure,
            RahaRepository repository) {
        return createRuntimeRepositories(sparkSession, storageMode,
                infrastructure.getTableGateway(),
                infrastructure.getPersistenceConfig(),
                infrastructure.getResultWriter(), repository);
    }

    /**
     * 使用显式物理依赖创建六类运行时仓储。
     *
     * <p>FMDB 模式返回 FMDB 适配器仓储，数据写入物理表；内存模式返回默认内存适配器仓储，
     * 数据写入传入的 {@link RahaRepository}。所有依赖不能为空。</p>
     *
     * <p>示例：</p>
     * <pre>{@code
     * RuntimeRepositories repositories =
     *         RahaTaskApplicationServiceFactory.createRuntimeRepositories(
     *                 spark, RahaStorageMode.FMDB, tableGateway,
     *                 persistenceConfig, resultWriter, repository);
     * }</pre>
     *
     * @param sparkSession Spark 会话，供 FMDB 仓储使用
     * @param storageMode 运行时存储模式，不能为空
     * @param tableGateway FMDB 表网关，不能为空
     * @param persistenceConfig FMDB 持久化配置，不能为空
     * @param resultWriter 结果写入器，不能为空
     * @param repository 内存模式共享仓储，不能为空
     * @return 当前存储模式对应的六类运行时仓储
     */
    static RuntimeRepositories createRuntimeRepositories(
            SparkSession sparkSession,
            RahaStorageMode storageMode,
            FmdbTableGateway tableGateway,
            FmdbPersistenceConfig persistenceConfig,
            FmdbResultWriter resultWriter,
            RahaRepository repository) {
        if (sparkSession == null || storageMode == null || tableGateway == null
                || persistenceConfig == null || resultWriter == null
                || repository == null) {
            throw new IllegalArgumentException("运行时仓储装配依赖不能为空");
        }
        if (storageMode == RahaStorageMode.FMDB) {
            return new RuntimeRepositories(
                    new FmdbStageRepository(sparkSession,tableGateway, persistenceConfig),
                    new FmdbStrategyRepository(tableGateway, persistenceConfig),
                    new FmdbFeatureRepository(tableGateway, persistenceConfig),
                    new FmdbColumnProfileRepository(tableGateway, persistenceConfig),
                    new FmdbClusterRepository(tableGateway, persistenceConfig),
                    new FmdbDetectionResultRepository(resultWriter, tableGateway,
                            persistenceConfig),
                    new FmdbSnapshotCheckpointRepository(sparkSession,
                            tableGateway, persistenceConfig));
        }
        return new RuntimeRepositories(new DefaultStageRepository(repository),
                new DefaultStrategyRepository(repository),
                new DefaultFeatureRepository(repository),
                new DefaultColumnProfileRepository(repository),
                new DefaultClusterRepository(repository),
                new DefaultDetectionResultRepository(repository),
                new DefaultSnapshotCheckpointRepository());
    }

    /**
     * 创建训练、采样和检测共享的数据准备服务。
     *
     * <p>数据准备服务包含策略计划、策略执行、特征组装、列画像、列聚类和标签传播。
     * 这些服务在多个工作流之间复用，以保证同一任务链路中的中间产物语义一致。</p>
     *
     * @param sparkSession Spark 会话，供 Spark KMeans 聚类在 driver 侧提交作业
     * @param repository 内存标签仓储的底层聚合仓储，不能为空
     * @param runtimeRepositories 当前存储模式下的运行时仓储，不能为空
     * @param clock 统一时间源，不能为空
     * @return 数据准备阶段领域服务集合
     */
    private static PreparationServices createPreparationServices(
            SparkSession sparkSession,
            RahaRepository repository,
            RuntimeRepositories runtimeRepositories,
            Clock clock) {
        StrategyPlanService planService = planService(
                runtimeRepositories.getStrategyRepository(), clock);
        StrategyExecutionService executionService = executionService(
                runtimeRepositories.getStrategyRepository(), clock);
        FeatureService featureService = featureService(
                runtimeRepositories.getFeatureRepository(), clock);
        ColumnProfileService profileService = profileService(
                runtimeRepositories.getColumnProfileRepository(), clock);
        ColumnClusteringService clusteringService = clusteringService(
                sparkSession, runtimeRepositories.getClusterRepository(), clock);
        CellLabelRepository labelRepository = new DefaultCellLabelRepository(repository);
        LabelPropagationService propagationService = new LabelPropagationService(
                labelRepository, clock);
        return new PreparationServices(planService, executionService,
                featureService, profileService, clusteringService,
                propagationService);
    }

    /**
     * 创建任务工作流直接调用的业务服务集合。
     *
     * <p>该方法负责组装训练、采样、检测服务，并按存储模式选择模型元数据仓储、
     * 模型产物存储和 FMDB 专用的训练输入合并服务。</p>
     *
     * @param sparkSession Spark 会话，供模型训练和 FMDB 适配器使用
     * @param modelDirectory 模型文件目录，仅内存模式用于模型文件存储
     * @param storageMode 运行时存储模式
     * @param infrastructure 运行时基础设施组件
     * @param repository 内存模式共享仓储
     * @param runtimeRepositories 当前存储模式下的运行时仓储
     * @param preparationServices 数据准备阶段领域服务
     * @return 任务工作流调用的业务服务集合
     */
    private static TaskServices createTaskServices(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure,
            RahaRepository repository,
            RuntimeRepositories runtimeRepositories,
            PreparationServices preparationServices) {
        ModelMetadataRepository modelMetadataRepository = modelMetadataRepository(
                sparkSession, storageMode, infrastructure, repository);
        SampleRecordRepository sampleRecordRepository =
                new FmdbSampleRecordRepository(sparkSession,
                        infrastructure.getTableGateway(),
                        infrastructure.getPersistenceConfig());
        AnnotationRecordRepository annotationRecordRepository =
                new FmdbAnnotationRecordRepository(sparkSession,
                        infrastructure.getTableGateway(),
                        infrastructure.getPersistenceConfig());
        ModelSetRepository modelSetRepository = new DefaultModelSetRepository(
                modelMetadataRepository);
        ColumnModelStore modelStore = modelStore(sparkSession, modelDirectory,
                storageMode, infrastructure);
        TrainingInputMergeService inputMergeService = trainingInputMergeService(
                sparkSession, sampleRecordRepository,
                annotationRecordRepository, infrastructure.getClock());
        TrainingArtifactMaterializationService materializationService =
                trainingArtifactMaterializationService(sparkSession, storageMode,
                        infrastructure);
        RahaTrainService trainService = trainService(sparkSession, modelStore,
                modelMetadataRepository, preparationServices,
                inputMergeService, materializationService, infrastructure.getClock());
        SampleRecordService sampleRecordService = sampleRecordService(
                sampleRecordRepository, infrastructure.getClock());
        RahaSampleService sampleService = sampleService(repository,
                preparationServices, infrastructure.getClock());
        RahaDetectService detectService = detectService(modelMetadataRepository,
                modelStore, runtimeRepositories.getDetectionResultRepository(),
                infrastructure.getClock());
        RahaTaskRequestFactory requestFactory = new RahaTaskRequestFactory(
                RahaDefaultConfigProvider.factory(), sampleRecordRepository,
                annotationRecordRepository, modelSetRepository);
        return new TaskServices(trainService, sampleService,
                sampleRecordService, detectService, inputMergeService,
                requestFactory);
    }

    /**
     * 按存储模式选择模型元数据仓储。
     *
     * <p>FMDB 模式将模型元数据落到物理表；内存模式复用聚合仓储，便于本地运行和测试。</p>
     */
    private static ModelMetadataRepository modelMetadataRepository(
            SparkSession sparkSession,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure,
            RahaRepository repository) {
        if (storageMode == RahaStorageMode.FMDB) {
            return new FmdbModelMetadataRepository(sparkSession,
                    infrastructure.getTableGateway(),
                    infrastructure.getPersistenceConfig());
        }
        return new DefaultModelMetadataRepository(repository);
    }

    /**
     * 按存储模式选择模型产物存储。
     *
     * <p>FMDB 模式将模型与训练产物写入物理表；内存模式将模型写入本地文件目录。</p>
     */
    private static ColumnModelStore modelStore(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure) {
        if (storageMode == RahaStorageMode.FMDB) {
            return new FmdbModelStore(sparkSession,
                    infrastructure.getTableGateway(),
                    FmdbPhysicalTable.MODEL_ARTIFACT.getTableName(),
                    FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT.getTableName(),
                    infrastructure.getClock(),
                    infrastructure.getPersistenceConfig());
        }
        return new FileColumnModelStore(modelDirectory);
    }

    /**
     * 创建训练服务。
     *
     * <p>该服务负责把前置准备阶段产物转换为训练数据、触发训练器执行、
     * 生成模型版本并完成模型发布。</p>
     */
    private static RahaTrainService trainService(
            SparkSession sparkSession,
            ColumnModelStore modelStore,
            ModelMetadataRepository modelMetadataRepository,
            PreparationServices preparationServices,
            TrainingInputMergeService inputMergeService,
            TrainingArtifactMaterializationService materializationService,
            Clock clock) {
        ModelReleaseManager releaseManager = new ModelReleaseManager(
                modelMetadataRepository, clock);
        ColumnModelVersioner modelVersioner = new ColumnModelVersioner();
        ColumnModelTrainer trainer = new AdaptiveColumnModelTrainer(
                new SparkMllibLogisticRegressionTrainer(sparkSession, modelVersioner),
                new WeightedRuleFallbackTrainer(modelVersioner));
        return new RahaTrainService(
                preparationServices.getPlanService(),
                preparationServices.getExecutionService(),
                preparationServices.getFeatureService(),
                preparationServices.getClusteringService(),
                preparationServices.getPropagationService(),
                new ColumnTrainingDataBuilder(), trainer, modelStore,
                new ColumnModelMetadataFactory(clock), releaseManager, clock,
                new BoundedParallelExecutor(), inputMergeService,
                materializationService);
    }

    /**
     * 创建持久化采样批次训练使用的输入合并服务。
     *
     * <p>服务复用请求工厂使用的采样和标注仓储，确保批次解析与执行阶段读取同一份数据。</p>
     */
    private static TrainingInputMergeService trainingInputMergeService(
            SparkSession sparkSession,
            SampleRecordRepository sampleRecordRepository,
            AnnotationRecordRepository annotationRecordRepository,
            Clock clock) {
        return new TrainingInputMergeService(sparkSession,
                sampleRecordRepository, annotationRecordRepository, clock);
    }

    /**
     * 创建训练产物物化服务。
     *
     * <p>两种运行模式都通过当前表网关保存冻结产物，具体是否落盘由持久化配置控制。</p>
     */
    private static TrainingArtifactMaterializationService
            trainingArtifactMaterializationService(
            SparkSession sparkSession,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure) {
        // 两种运行模式都通过当前表网关保存冻结训练产物，内存模式由进程内网关承接。
        return new TrainingArtifactMaterializationService(
                new FmdbTrainingArtifactRepository(sparkSession,
                        infrastructure.getTableGateway(),
                        infrastructure.getPersistenceConfig()),
                infrastructure.getClock());
    }

    /**
     * 创建采样记录服务。
     *
     * <p>采样记录服务负责把采样过程中的宽表记录写入 FMDB，供后续审计和追踪。</p>
     */
    private static SampleRecordService sampleRecordService(
            SampleRecordRepository sampleRecordRepository,
            Clock clock) {
        return new SampleRecordService(sampleRecordRepository, clock);
    }

    /**
     * 创建采样任务服务。
     *
     * <p>该服务结合聚类结果和采样策略，从候选样本中挑选最具代表性的记录。</p>
     */
    private static RahaSampleService sampleService(
            RahaRepository repository,
            PreparationServices preparationServices,
            Clock clock) {
        AnnotationTaskRepository annotationRepository =
                new DefaultAnnotationTaskRepository(repository);
        return new RahaSampleService(preparationServices.getClusteringService(),
                new SamplingService(new ClusterCoverageScorer(), new TupleSampler(),
                        new SamplingVersioner(), annotationRepository, clock), clock);
    }

    /**
     * 创建检测任务服务。
     *
     * <p>该服务负责加载已发布模型、执行列级预测并写入检测结果。</p>
     */
    private static RahaDetectService detectService(
            ModelMetadataRepository modelMetadataRepository,
            ColumnModelStore modelStore,
            DetectionResultRepository detectionRepository,
            Clock clock) {
        return new RahaDetectService(
                new PublishedColumnModelLoader(modelMetadataRepository, modelStore,
                        new ColumnModelCompatibilityValidator()),
                new ColumnModelPredictor(), detectionRepository, clock);
    }

    /**
     * 创建三个任务工作流并注册到工作流注册器。
     *
     * <p>训练、采样、检测三条工作流共享同一数据加载器和前置准备服务。
     * FMDB 模式下会额外接入结果持久化校验器。</p>
     */
    private static RahaWorkflowRegistry createWorkflowRegistry(
            SparkSession sparkSession,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure,
            PreparationServices preparationServices,
            TaskServices taskServices,
            RuntimeRepositories runtimeRepositories) {
        RahaDatasetLoader loader = datasetLoader(sparkSession,
                infrastructure.getClock());
        ResultPersistenceVerifier verifier = new FmdbResultPersistenceVerifier(
                infrastructure.getTableGateway(), infrastructure.getPersistenceConfig());
        TrainingWorkflow trainingWorkflow = new TrainingWorkflow(loader,
                preparationServices.getProfileService(),
                preparationServices.getPlanService(),
                preparationServices.getExecutionService(),
                preparationServices.getFeatureService(),
                preparationServices.getClusteringService(),
                preparationServices.getPropagationService(),
                taskServices.getTrainService(),
                taskServices.getInputMergeService(),
                storageMode == RahaStorageMode.FMDB ? verifier : null,
                runtimeRepositories.getSnapshotCheckpointRepository());
        SamplingWorkflow samplingWorkflow = new SamplingWorkflow(loader,
                preparationServices.getProfileService(),
                preparationServices.getPlanService(),
                preparationServices.getExecutionService(),
                preparationServices.getFeatureService(),
                preparationServices.getClusteringService(),
                taskServices.getSampleService(),
                taskServices.getSampleRecordService(), verifier,
                runtimeRepositories.getSnapshotCheckpointRepository());
        DetectionWorkflow detectionWorkflow = new DetectionWorkflow(loader,
                preparationServices.getProfileService(),
                preparationServices.getPlanService(),
                preparationServices.getExecutionService(),
                preparationServices.getFeatureService(),
                taskServices.getDetectService(),
                storageMode == RahaStorageMode.FMDB ? verifier : null);
        return new RahaWorkflowRegistry(Arrays.asList(
                trainingWorkflow, samplingWorkflow, detectionWorkflow));
    }

    /**
     * 创建任务编排器。
     *
     * <p>编排器负责幂等控制、配置校验、阶段失败判定和任务状态推进，是整个任务生命周期的控制中心。</p>
     */
    private static RahaJobOrchestrator createJobOrchestrator(
            RahaStorageMode storageMode,
            RahaRepository repository,
            StageRepository stageRepository,
            RuntimeInfrastructure infrastructure) {
        JobRepository jobRepository = jobRepository(storageMode,
                repository, infrastructure);
        return new RahaJobOrchestrator(
                new RahaConfigValidator(), new ConfigVersioner(),
                new IdempotencyKeyGenerator(), new DefaultRahaIdGenerator(),
                new StageFailureDecider(), jobRepository,
                stageRepository, infrastructure.getClock());
    }

    /**
     * 按存储模式选择任务仓储。
     *
     * <p>FMDB 模式使用物理表仓储，内存模式使用默认仓储实现。</p>
     */
    private static JobRepository jobRepository(
            RahaStorageMode storageMode,
            RahaRepository repository,
            RuntimeInfrastructure infrastructure) {
        if (storageMode == RahaStorageMode.FMDB) {
            return new FmdbJobRepository(infrastructure.getResultWriter(),
                    infrastructure.getTableGateway(),
                    infrastructure.getPersistenceConfig());
        }
        return new DefaultJobRepository(repository);
    }

    /**
     * 创建数据集加载器路由器。
     *
     * <p>文件模式与 FMDB 模式使用不同的数据集加载实现，路由器会按输入源自动分派。</p>
     */
    private static RahaDatasetLoader datasetLoader(SparkSession sparkSession,
                                                   Clock clock) {
        RowIdentityService identityService = new RowIdentityService();
        RowIdValidator rowIdValidator = new RowIdValidator();
        SchemaHasher schemaHasher = new SchemaHasher();
        ColumnMetadataFactory metadataFactory = new ColumnMetadataFactory();
        SnapshotMetadataFactory snapshotFactory = new SnapshotMetadataFactory();
        return new RoutingRahaDatasetLoader(
                new FileRahaDatasetLoader(sparkSession, identityService,
                        rowIdValidator, schemaHasher, metadataFactory,
                        snapshotFactory, clock),
                new FmdbDatasetLoader(sparkSession, identityService,
                        rowIdValidator, schemaHasher,
                        new DefaultFmdbSchemaResolver(metadataFactory),
                        snapshotFactory, clock));
    }

    /**
     * 创建策略计划服务。
     *
     * <p>该服务生成策略执行计划，并把计划状态写入策略仓储。</p>
     */
    private static StrategyPlanService planService(StrategyRepository repository,
                                                   Clock clock) {
        return new StrategyPlanService(new StrategyPlanGenerator(),
                repository, clock);
    }

    /**
     * 创建策略执行服务。
     *
     * <p>该服务负责真正执行策略并记录命中结果。</p>
     */
    private static StrategyExecutionService executionService(
            StrategyRepository repository, Clock clock) {
        return new StrategyExecutionService(
                new StrategyExecutor(StrategyRegistry.defaults(), clock),
                repository, clock);
    }

    /**
     * 创建特征服务。
     *
     * <p>该服务把策略输出组装为特征字典和特征向量，并保存到特征仓储。</p>
     */
    private static FeatureService featureService(FeatureRepository repository,
                                                 Clock clock) {
        return new FeatureService(new FeatureAssembler(
                new FeatureDictionaryVersioner(), clock),
                repository, clock);
    }

    /**
     * 创建列画像服务。
     *
     * <p>该服务分析列的统计特征并持久化列画像结果。</p>
     */
    private static ColumnProfileService profileService(
            ColumnProfileRepository repository,
            Clock clock) {
        return new ColumnProfileService(new ColumnProfiler(),
                repository, clock);
    }

    /**
     * 创建列聚类服务。
     *
     * <p>该服务根据列画像结果计算相似列簇，用于后续采样和检测。
     * Spark KMeans provider 需要复用运行时 Spark 会话在 driver 侧提交作业。</p>
     */
    private static ColumnClusteringService clusteringService(
            SparkSession sparkSession, ClusterRepository repository, Clock clock) {
        ClusteringConfig clusteringConfig = RahaDefaultConfigProvider.factory()
                .clusteringConfig();
        ColumnClusterer clusterer = ClusteringProviderResolver.resolve(
                clusteringConfig.getProvider(), new ClusterVersioner(), clock,
                sparkSession);
        return new ColumnClusteringService(clusterer, repository, clock);
    }

    /**
     * 创建 FMDB 结果写入器。
     *
     * <p>结果写入器统一负责任务状态和检测结果的物理落盘，供外部系统查询。</p>
     */
    private static FmdbResultWriter resultWriter(SparkSession sparkSession,
                                                 FmdbTableGateway tableGateway,
                                                 Clock clock,
                                                 FmdbPersistenceConfig config) {
        return new SparkSqlFmdbResultWriter(sparkSession, tableGateway, clock, config);
    }

    /**
     * 获取当前活动 Spark 会话。
     *
     * <p>默认入口依赖外部已经初始化 Spark 环境，因此这里不兜底创建会话，只在缺失时直接失败。
     * 这样可以尽早暴露启动顺序错误。</p>
     *
     * @return 当前活动 Spark 会话
     */
    private static SparkSession resolveActiveSparkSession() {
        Option<SparkSession> active = SparkSession.getActiveSession();
        if (active == null || active.isEmpty()) {
            throw new IllegalStateException("默认 Raha 应用服务需要已初始化的活动 SparkSession，"
                    + "请使用 createDefault(sparkSession) 显式传入");
        }
        return active.get();
    }

    /** 默认运行时基础设施组件。 */
    private static final class RuntimeInfrastructure {

        /** 默认装配使用的统一时间源。 */
        private final Clock clock;
        /** FMDB 持久化和建表配置。 */
        private final FmdbPersistenceConfig persistenceConfig;
        /** 当前存储模式使用的 FMDB 表网关。 */
        private final FmdbTableGateway tableGateway;
        /** 任务状态和检测结果物理写入器。 */
        private final FmdbResultWriter resultWriter;

        private RuntimeInfrastructure(Clock clock,
                                      FmdbPersistenceConfig persistenceConfig,
                                      FmdbTableGateway tableGateway,
                                      FmdbResultWriter resultWriter) {
            this.clock = clock;
            this.persistenceConfig = persistenceConfig;
            this.tableGateway = tableGateway;
            this.resultWriter = resultWriter;
        }

        /**
         * 获取统一时间源。
         *
         * @return 统一时间源
         */
        Clock getClock() {
            return clock;
        }

        /**
         * 获取 FMDB 持久化配置。
         *
         * @return 持久化配置
         */
        FmdbPersistenceConfig getPersistenceConfig() {
            return persistenceConfig;
        }

        /**
         * 获取当前存储模式使用的表网关。
         *
         * @return 表网关
         */
        FmdbTableGateway getTableGateway() {
            return tableGateway;
        }

        /**
         * 获取结果写入器。
         *
         * @return 结果写入器
         */
        FmdbResultWriter getResultWriter() {
            return resultWriter;
        }
    }

    /** 按运行时存储模式统一选择的运行期仓储。 */
    static final class RuntimeRepositories {

        /** 阶段状态仓储。 */
        private final StageRepository stageRepository;
        /** 策略计划、摘要和临时命中仓储。 */
        private final StrategyRepository strategyRepository;
        /** 特征字典和向量仓储。 */
        private final FeatureRepository featureRepository;
        /** 列画像仓储。 */
        private final ColumnProfileRepository columnProfileRepository;
        /** 聚类摘要和成员仓储。 */
        private final ClusterRepository clusterRepository;
        /** 最终检测错误仓储。 */
        private final DetectionResultRepository detectionResultRepository;
        /** 采样快照检查点仓储。 */
        private final SnapshotCheckpointRepository snapshotCheckpointRepository;

        private RuntimeRepositories(StageRepository stageRepository,
                                    StrategyRepository strategyRepository,
                                    FeatureRepository featureRepository,
                                    ColumnProfileRepository columnProfileRepository,
                                    ClusterRepository clusterRepository,
                                    DetectionResultRepository detectionResultRepository,
                                    SnapshotCheckpointRepository snapshotCheckpointRepository) {
            if (stageRepository == null || strategyRepository == null
                    || featureRepository == null || columnProfileRepository == null
                    || clusterRepository == null || detectionResultRepository == null
                    || snapshotCheckpointRepository == null) {
                throw new IllegalArgumentException("运行时仓储不能为空");
            }
            this.stageRepository = stageRepository;
            this.strategyRepository = strategyRepository;
            this.featureRepository = featureRepository;
            this.columnProfileRepository = columnProfileRepository;
            this.clusterRepository = clusterRepository;
            this.detectionResultRepository = detectionResultRepository;
            this.snapshotCheckpointRepository = snapshotCheckpointRepository;
        }

        /**
         * 获取阶段仓储。
         *
         * @return 阶段仓储
         */
        StageRepository getStageRepository() {
            return stageRepository;
        }

        /**
         * 获取策略仓储。
         *
         * @return 策略仓储
         */
        StrategyRepository getStrategyRepository() {
            return strategyRepository;
        }

        /**
         * 获取特征仓储。
         *
         * @return 特征仓储
         */
        FeatureRepository getFeatureRepository() {
            return featureRepository;
        }

        /**
         * 获取列画像仓储。
         *
         * @return 列画像仓储
         */
        ColumnProfileRepository getColumnProfileRepository() {
            return columnProfileRepository;
        }

        /**
         * 获取聚类仓储。
         *
         * @return 聚类仓储
         */
        ClusterRepository getClusterRepository() {
            return clusterRepository;
        }

        /**
         * 获取检测结果仓储。
         *
         * @return 检测结果仓储
         */
        DetectionResultRepository getDetectionResultRepository() {
            return detectionResultRepository;
        }

        /**
         * 获取采样快照检查点仓储。
         *
         * @return 快照检查点仓储
         */
        SnapshotCheckpointRepository getSnapshotCheckpointRepository() {
            return snapshotCheckpointRepository;
        }
    }

    /** 数据准备阶段领域服务集合。 */
    private static final class PreparationServices {

        /** 策略计划生成和保存服务。 */
        private final StrategyPlanService planService;
        /** 策略执行和命中保存服务。 */
        private final StrategyExecutionService executionService;
        /** 特征字典和稀疏特征组装服务。 */
        private final FeatureService featureService;
        /** 列画像计算和保存服务。 */
        private final ColumnProfileService profileService;
        /** 列内聚类计算和保存服务。 */
        private final ColumnClusteringService clusteringService;
        /** 直接标签到聚类簇的标签传播服务。 */
        private final LabelPropagationService propagationService;

        private PreparationServices(StrategyPlanService planService,
                                    StrategyExecutionService executionService,
                                    FeatureService featureService,
                                    ColumnProfileService profileService,
                                    ColumnClusteringService clusteringService,
                                    LabelPropagationService propagationService) {
            this.planService = planService;
            this.executionService = executionService;
            this.featureService = featureService;
            this.profileService = profileService;
            this.clusteringService = clusteringService;
            this.propagationService = propagationService;
        }

        /**
         * 获取策略计划服务。
         *
         * @return 策略计划服务
         */
        StrategyPlanService getPlanService() {
            return planService;
        }

        /**
         * 获取策略执行服务。
         *
         * @return 策略执行服务
         */
        StrategyExecutionService getExecutionService() {
            return executionService;
        }

        /**
         * 获取特征服务。
         *
         * @return 特征服务
         */
        FeatureService getFeatureService() {
            return featureService;
        }

        /**
         * 获取列画像服务。
         *
         * @return 列画像服务
         */
        ColumnProfileService getProfileService() {
            return profileService;
        }

        /**
         * 获取列聚类服务。
         *
         * @return 列聚类服务
         */
        ColumnClusteringService getClusteringService() {
            return clusteringService;
        }

        /**
         * 获取标签传播服务。
         *
         * @return 标签传播服务
         */
        LabelPropagationService getPropagationService() {
            return propagationService;
        }
    }

    /** 任务工作流调用的业务服务集合。 */
    private static final class TaskServices {

        /** 训练任务业务服务。 */
        private final RahaTrainService trainService;
        /** 采样任务业务服务。 */
        private final RahaSampleService sampleService;
        /** 采样 c1 宽表物化服务。 */
        private final SampleRecordService sampleRecordService;
        /** 检测任务业务服务。 */
        private final RahaDetectService detectService;
        /** FMDB 模式下的持久化训练输入合并服务。 */
        private final TrainingInputMergeService inputMergeService;
        /** 面向调用方的最小任务请求工厂。 */
        private final RahaTaskRequestFactory requestFactory;

        private TaskServices(RahaTrainService trainService,
                             RahaSampleService sampleService,
                             SampleRecordService sampleRecordService,
                             RahaDetectService detectService,
                             TrainingInputMergeService inputMergeService,
                             RahaTaskRequestFactory requestFactory) {
            if (trainService == null || sampleService == null
                    || sampleRecordService == null || detectService == null
                    || inputMergeService == null || requestFactory == null) {
                throw new IllegalArgumentException("任务服务集合依赖不能为空");
            }
            this.trainService = trainService;
            this.sampleService = sampleService;
            this.sampleRecordService = sampleRecordService;
            this.detectService = detectService;
            this.inputMergeService = inputMergeService;
            this.requestFactory = requestFactory;
        }

        /**
         * 获取训练服务。
         *
         * @return 训练服务
         */
        RahaTrainService getTrainService() {
            return trainService;
        }

        /**
         * 获取采样服务。
         *
         * @return 采样服务
         */
        RahaSampleService getSampleService() {
            return sampleService;
        }

        /**
         * 获取采样记录服务。
         *
         * @return 采样记录服务
         */
        SampleRecordService getSampleRecordService() {
            return sampleRecordService;
        }

        /**
         * 获取检测服务。
         *
         * @return 检测服务
         */
        RahaDetectService getDetectService() {
            return detectService;
        }

        /**
         * 获取训练输入合并服务。
         *
         * @return 训练输入合并服务
         */
        TrainingInputMergeService getInputMergeService() {
            return inputMergeService;
        }

        /**
         * 获取共享仓储实例的最小任务请求工厂。
         *
         * @return 最小任务请求工厂
         */
        RahaTaskRequestFactory getRequestFactory() {
            return requestFactory;
        }
    }

    /** 默认应用服务依赖的最小组件集合。 */
    static final class DefaultComponents {

        /** 任务编排器。 */
        private final RahaJobOrchestrator jobOrchestrator;
        /** 工作流注册器。 */
        private final RahaWorkflowRegistry workflowRegistry;
        /** 当前存储模式下实际装配的六类仓储。 */
        private final RuntimeRepositories runtimeRepositories;
        /** 与默认工作流共享仓储的最小任务请求工厂。 */
        private final RahaTaskRequestFactory requestFactory;
        /** driver 侧训练和检测列批协调器。 */
        private final ColumnBatchExecutionCoordinator columnBatchCoordinator;

        private DefaultComponents(RahaJobOrchestrator jobOrchestrator,
                                  RahaWorkflowRegistry workflowRegistry,
                                  RuntimeRepositories runtimeRepositories,
                                  RahaTaskRequestFactory requestFactory,
                                  ColumnBatchExecutionCoordinator
                                          columnBatchCoordinator) {
            if (requestFactory == null || columnBatchCoordinator == null) {
                throw new IllegalArgumentException("默认最小任务请求工厂不能为空");
            }
            this.jobOrchestrator = jobOrchestrator;
            this.workflowRegistry = workflowRegistry;
            this.runtimeRepositories = runtimeRepositories;
            this.requestFactory = requestFactory;
            this.columnBatchCoordinator = columnBatchCoordinator;
        }

        /**
         * 获取任务编排器。
         *
         * @return 任务编排器
         */
        RahaJobOrchestrator getJobOrchestrator() {
            return jobOrchestrator;
        }

        /**
         * 获取工作流注册器。
         *
         * @return 工作流注册器
         */
        RahaWorkflowRegistry getWorkflowRegistry() {
            return workflowRegistry;
        }

        /**
         * 获取阶段仓储。
         *
         * @return 阶段仓储
         */
        StageRepository getStageRepository() {
            return runtimeRepositories.getStageRepository();
        }

        /**
         * 获取运行时仓储集合。
         *
         * @return 运行时仓储集合
         */
        RuntimeRepositories getRuntimeRepositories() {
            return runtimeRepositories;
        }

        /**
         * 获取最小任务请求工厂。
         *
         * @return 与默认运行时共享仓储的请求工厂
         */
        RahaTaskRequestFactory getRequestFactory() {
            return requestFactory;
        }

        /**
         * 获取默认 driver 侧列批协调器。
         *
         * @return 列批协调器
         */
        ColumnBatchExecutionCoordinator getColumnBatchCoordinator() {
            return columnBatchCoordinator;
        }
    }
}
