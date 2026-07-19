package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.algorithm.HierarchicalColumnClusterer;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.core.RahaStorageMode;
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
import com.fiberhome.ml.raha.repository.adapter.DefaultAnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultCellLabelRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.SparkSqlFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbJobRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbSampleRecordRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultPersistenceVerifier;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.DefaultFmdbSchemaResolver;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.port.CellLabelRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.JobRepository;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.sampling.service.SampleRecordService;
import com.fiberhome.ml.raha.sampling.service.SamplingService;
import com.fiberhome.ml.raha.sampling.service.SamplingVersioner;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
import com.fiberhome.ml.raha.service.train.RahaTrainService;
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

    private RahaTaskApplicationServiceFactory() {
    }

    /**
     * 使用当前活动 Spark 会话和默认模型目录创建应用服务。
     *
     * @return 默认任务应用服务
     */
    public static RahaTaskApplicationService createDefault() {
        return new RahaTaskApplicationService(createDefaultComponents());
    }

    /**
     * 使用指定 Spark 会话和默认模型目录创建应用服务。
     *
     * @param sparkSession Spark 会话
     * @return 默认任务应用服务
     */
    public static RahaTaskApplicationService createDefault(
            SparkSession sparkSession) {
        return new RahaTaskApplicationService(createDefaultComponents(
                sparkSession, DEFAULT_MODEL_DIRECTORY));
    }

    /**
     * 使用指定 Spark 会话和模型目录创建应用服务。
     *
     * @param sparkSession Spark 会话
     * @param modelDirectory 模型文件目录，仅内存模式使用
     * @return 默认任务应用服务
     */
    public static RahaTaskApplicationService createDefault(
            SparkSession sparkSession, Path modelDirectory) {
        return new RahaTaskApplicationService(createDefaultComponents(
                sparkSession, modelDirectory));
    }

    /**
     * 使用显式存储模式创建应用服务，主要用于部署装配和测试。
     *
     * @param sparkSession Spark 会话
     * @param modelDirectory 模型文件目录，仅内存模式使用
     * @param storageMode 物理存储模式
     * @return 默认任务应用服务
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
     * @param jobOrchestrator 任务编排器
     * @param workflowRegistry 工作流注册器
     * @param stageRepository 阶段仓储
     * @return 统一任务应用服务
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
     * @param jobOrchestrator 任务编排器
     * @param stageRepository 阶段仓储
     * @param workflows 工作流集合
     * @return 统一任务应用服务
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
     * @param jobOrchestrator 任务编排器
     * @param stageRepository 阶段仓储
     * @param workflows 工作流数组
     * @return 统一任务应用服务
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

    /** 创建无参应用服务所需的默认组件。 */
    static DefaultComponents createDefaultComponents() {
        return createDefaultComponents(resolveActiveSparkSession(),
                DEFAULT_MODEL_DIRECTORY);
    }

    /**
     * 根据默认配置完成运行时装配。
     *
     * @param sparkSession Spark 会话
     * @param modelDirectory 模型文件目录
     * @return 默认组件集合
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

    private static DefaultComponents createDefaultComponents(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode) {
        validateDefaultRuntimeInputs(sparkSession, modelDirectory, storageMode);
        LOGGER.info("开始创建 Raha 默认运行时，storageMode={}，modelDirectory={}",
                storageMode, modelDirectory.toAbsolutePath().normalize());

        // 阶段一：创建运行时基础设施，决定时间源、FMDB 持久化配置和物理表网关。
        RuntimeInfrastructure infrastructure = createRuntimeInfrastructure(
                sparkSession, storageMode);

        // 阶段二：创建默认逻辑仓储，当前承载阶段、策略、画像、特征和聚类等共享状态。
        RahaRepository repository = new InMemoryRahaRepository();
        StageRepository stageRepository = new DefaultStageRepository(repository);

        // 阶段三：创建数据准备领域服务，训练、采样和检测工作流都会复用这些能力。
        PreparationServices preparationServices = createPreparationServices(
                repository, infrastructure.getClock());

        // 阶段四：创建任务业务服务，按存储模式选择模型元数据和模型产物存储位置。
        TaskServices taskServices = createTaskServices(sparkSession, modelDirectory,
                storageMode, infrastructure, repository, preparationServices);

        // 阶段五：创建数据加载路由器和三类工作流，形成任务类型到阶段链路的注册表。
        RahaWorkflowRegistry workflowRegistry = createWorkflowRegistry(
                sparkSession, storageMode, infrastructure, preparationServices,
                taskServices);

        // 阶段六：创建任务编排器，负责幂等提交、阶段执行、状态推进和结果登记。
        RahaJobOrchestrator orchestrator = createJobOrchestrator(storageMode,
                repository, stageRepository, infrastructure);
        LOGGER.info("Raha 默认运行时创建完成，storageMode={}，workflowCount={}",
                storageMode, 3);
        return new DefaultComponents(orchestrator, workflowRegistry, stageRepository);
    }

    private static void validateDefaultRuntimeInputs(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode) {
        if (sparkSession == null || modelDirectory == null || storageMode == null) {
            throw new IllegalArgumentException("默认运行时 Spark 会话、模型目录和存储模式不能为空");
        }
    }

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

    private static PreparationServices createPreparationServices(
            RahaRepository repository,
            Clock clock) {
        StrategyPlanService planService = planService(repository, clock);
        StrategyExecutionService executionService = executionService(repository, clock);
        FeatureService featureService = featureService(repository, clock);
        ColumnProfileService profileService = profileService(repository, clock);
        ColumnClusteringService clusteringService = clusteringService(repository, clock);
        CellLabelRepository labelRepository = new DefaultCellLabelRepository(repository);
        LabelPropagationService propagationService = new LabelPropagationService(
                labelRepository, clock);
        return new PreparationServices(planService, executionService,
                featureService, profileService, clusteringService,
                propagationService);
    }

    private static TaskServices createTaskServices(
            SparkSession sparkSession,
            Path modelDirectory,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure,
            RahaRepository repository,
            PreparationServices preparationServices) {
        ModelMetadataRepository modelMetadataRepository = modelMetadataRepository(
                sparkSession, storageMode, infrastructure, repository);
        ColumnModelStore modelStore = modelStore(sparkSession, modelDirectory,
                storageMode, infrastructure);
        RahaTrainService trainService = trainService(sparkSession, modelStore,
                modelMetadataRepository, preparationServices,
                infrastructure.getClock());
        SampleRecordService sampleRecordService = sampleRecordService(
                sparkSession, infrastructure);
        RahaSampleService sampleService = sampleService(repository,
                preparationServices, infrastructure.getClock());
        RahaDetectService detectService = detectService(modelMetadataRepository,
                modelStore, repository, infrastructure.getClock());
        return new TaskServices(trainService, sampleService,
                sampleRecordService, detectService);
    }

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

    private static RahaTrainService trainService(
            SparkSession sparkSession,
            ColumnModelStore modelStore,
            ModelMetadataRepository modelMetadataRepository,
            PreparationServices preparationServices,
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
                new ColumnModelMetadataFactory(clock), releaseManager, clock);
    }

    private static SampleRecordService sampleRecordService(
            SparkSession sparkSession,
            RuntimeInfrastructure infrastructure) {
        return new SampleRecordService(
                new FmdbSampleRecordRepository(sparkSession,
                        infrastructure.getTableGateway(),
                        infrastructure.getPersistenceConfig()),
                infrastructure.getClock());
    }

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

    private static RahaDetectService detectService(
            ModelMetadataRepository modelMetadataRepository,
            ColumnModelStore modelStore,
            RahaRepository repository,
            Clock clock) {
        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(repository);
        return new RahaDetectService(
                new PublishedColumnModelLoader(modelMetadataRepository, modelStore,
                        new ColumnModelCompatibilityValidator()),
                new ColumnModelPredictor(), detectionRepository, clock);
    }

    private static RahaWorkflowRegistry createWorkflowRegistry(
            SparkSession sparkSession,
            RahaStorageMode storageMode,
            RuntimeInfrastructure infrastructure,
            PreparationServices preparationServices,
            TaskServices taskServices) {
        RahaDatasetLoader loader = datasetLoader(sparkSession,
                infrastructure.getClock());
        ResultPersistenceVerifier verifier = new FmdbResultPersistenceVerifier(
                infrastructure.getTableGateway(), infrastructure.getResultWriter(),
                infrastructure.getPersistenceConfig());
        TrainingWorkflow trainingWorkflow = new TrainingWorkflow(loader,
                preparationServices.getProfileService(),
                preparationServices.getPlanService(),
                preparationServices.getExecutionService(),
                preparationServices.getFeatureService(),
                preparationServices.getClusteringService(),
                preparationServices.getPropagationService(),
                taskServices.getTrainService());
        SamplingWorkflow samplingWorkflow = new SamplingWorkflow(loader,
                preparationServices.getProfileService(),
                preparationServices.getPlanService(),
                preparationServices.getExecutionService(),
                preparationServices.getFeatureService(),
                preparationServices.getClusteringService(),
                taskServices.getSampleService(),
                taskServices.getSampleRecordService(), verifier);
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

    private static StrategyPlanService planService(RahaRepository repository,
                                                   Clock clock) {
        return new StrategyPlanService(new StrategyPlanGenerator(),
                new DefaultStrategyRepository(repository), clock);
    }

    private static StrategyExecutionService executionService(
            RahaRepository repository, Clock clock) {
        return new StrategyExecutionService(
                new StrategyExecutor(StrategyRegistry.defaults(), clock),
                new DefaultStrategyRepository(repository), clock);
    }

    private static FeatureService featureService(RahaRepository repository,
                                                 Clock clock) {
        return new FeatureService(new FeatureAssembler(
                new FeatureDictionaryVersioner(), clock),
                new DefaultFeatureRepository(repository), clock);
    }

    private static ColumnProfileService profileService(RahaRepository repository,
                                                       Clock clock) {
        return new ColumnProfileService(new ColumnProfiler(),
                new DefaultColumnProfileRepository(repository), clock);
    }

    private static ColumnClusteringService clusteringService(
            RahaRepository repository, Clock clock) {
        return new ColumnClusteringService(new HierarchicalColumnClusterer(
                new ClusterVersioner(), clock),
                new DefaultClusterRepository(repository), clock);
    }

    private static FmdbResultWriter resultWriter(SparkSession sparkSession,
                                                 FmdbTableGateway tableGateway,
                                                 Clock clock,
                                                 FmdbPersistenceConfig config) {
        return new SparkSqlFmdbResultWriter(sparkSession, tableGateway, clock, config);
    }

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

        Clock getClock() {
            return clock;
        }

        FmdbPersistenceConfig getPersistenceConfig() {
            return persistenceConfig;
        }

        FmdbTableGateway getTableGateway() {
            return tableGateway;
        }

        FmdbResultWriter getResultWriter() {
            return resultWriter;
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

        StrategyPlanService getPlanService() {
            return planService;
        }

        StrategyExecutionService getExecutionService() {
            return executionService;
        }

        FeatureService getFeatureService() {
            return featureService;
        }

        ColumnProfileService getProfileService() {
            return profileService;
        }

        ColumnClusteringService getClusteringService() {
            return clusteringService;
        }

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

        private TaskServices(RahaTrainService trainService,
                             RahaSampleService sampleService,
                             SampleRecordService sampleRecordService,
                             RahaDetectService detectService) {
            this.trainService = trainService;
            this.sampleService = sampleService;
            this.sampleRecordService = sampleRecordService;
            this.detectService = detectService;
        }

        RahaTrainService getTrainService() {
            return trainService;
        }

        RahaSampleService getSampleService() {
            return sampleService;
        }

        SampleRecordService getSampleRecordService() {
            return sampleRecordService;
        }

        RahaDetectService getDetectService() {
            return detectService;
        }
    }

    /** 默认应用服务依赖的最小组件集合。 */
    static final class DefaultComponents {

        /** 任务编排器。 */
        private final RahaJobOrchestrator jobOrchestrator;
        /** 工作流注册器。 */
        private final RahaWorkflowRegistry workflowRegistry;
        /** 阶段仓储。 */
        private final StageRepository stageRepository;

        private DefaultComponents(RahaJobOrchestrator jobOrchestrator,
                                  RahaWorkflowRegistry workflowRegistry,
                                  StageRepository stageRepository) {
            this.jobOrchestrator = jobOrchestrator;
            this.workflowRegistry = workflowRegistry;
            this.stageRepository = stageRepository;
        }

        RahaJobOrchestrator getJobOrchestrator() {
            return jobOrchestrator;
        }

        RahaWorkflowRegistry getWorkflowRegistry() {
            return workflowRegistry;
        }

        StageRepository getStageRepository() {
            return stageRepository;
        }
    }
}
