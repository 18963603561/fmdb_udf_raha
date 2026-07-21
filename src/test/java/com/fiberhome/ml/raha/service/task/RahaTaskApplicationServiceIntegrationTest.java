package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.algorithm.HierarchicalColumnClusterer;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.config.dto.FailureToleranceConfig;
import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.config.validation.ConfigVersioner;
import com.fiberhome.ml.raha.config.validation.RahaConfigValidator;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.LoadedDataset;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityResult;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.job.execution.RahaJobOrchestrator;
import com.fiberhome.ml.raha.job.execution.StageFailureDecider;
import com.fiberhome.ml.raha.job.id.DefaultRahaIdGenerator;
import com.fiberhome.ml.raha.job.id.IdempotencyKeyGenerator;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.model.FileColumnModelStore;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.release.ColumnModelCompatibilityValidator;
import com.fiberhome.ml.raha.model.release.ColumnModelMetadataFactory;
import com.fiberhome.ml.raha.model.release.ColumnModelVersioner;
import com.fiberhome.ml.raha.model.release.ModelReleaseManager;
import com.fiberhome.ml.raha.model.release.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataBuilder;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
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
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbSampleRecordRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.port.CellLabelRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.sampling.service.SamplingService;
import com.fiberhome.ml.raha.sampling.service.SamplingVersioner;
import com.fiberhome.ml.raha.sampling.service.SampleRecordService;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import com.fiberhome.ml.raha.service.train.RahaTrainService;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证统一应用入口直接执行训练、采样和已发布模型预测工作流。
 */
class RahaTaskApplicationServiceIntegrationTest {

    @TempDir
    Path modelDirectory;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldExecuteTrainingSamplingAndDetectionThroughSingleEntry() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        RahaDataset dataset = dataset();
        RahaDatasetLoader loader = request -> new LoadedDataset(dataset,
                new DatasetSnapshot("dataset", "snapshot-v1", "memory", "dataset",
                        RowIdentityColumns.ROW_ID, "schema-v1", 8L, 2,
                        "source-v1", 1000L));
        InMemoryFmdbTableGateway fmdbGateway = new InMemoryFmdbTableGateway(
                SparkTestSession.get());
        SampleRecordService sampleRecordService = new SampleRecordService(
                new FmdbSampleRecordRepository(SparkTestSession.get(), fmdbGateway,
                        FmdbPersistenceConfig.fromDefaults()), clock);
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        DefaultStageRepository stageRepository = new DefaultStageRepository(storage);
        DefaultStrategyRepository strategyRepository =
                new DefaultStrategyRepository(storage);
        ColumnProfileService profileService = new ColumnProfileService(
                new ColumnProfiler(), new DefaultColumnProfileRepository(storage), clock);
        StrategyPlanService planService = new StrategyPlanService(
                new StrategyPlanGenerator(), strategyRepository, clock);
        StrategyExecutionService executionService = new StrategyExecutionService(
                new StrategyExecutor(StrategyRegistry.defaults(), clock),
                strategyRepository, clock);
        FeatureService featureService = new FeatureService(
                new FeatureAssembler(new FeatureDictionaryVersioner(), clock),
                new DefaultFeatureRepository(storage), clock);
        ColumnClusteringService clusteringService = new ColumnClusteringService(
                new HierarchicalColumnClusterer(new ClusterVersioner(), clock),
                new DefaultClusterRepository(storage), clock);
        CellLabelRepository labelRepository = new DefaultCellLabelRepository(storage);
        LabelPropagationService propagationService = new LabelPropagationService(
                labelRepository, clock);
        ModelMetadataRepository modelRepository =
                new DefaultModelMetadataRepository(storage);
        FileColumnModelStore modelStore = new FileColumnModelStore(modelDirectory);
        ModelReleaseManager releaseManager = new ModelReleaseManager(modelRepository, clock);
        RahaTrainService trainService = new RahaTrainService(
                planService, executionService, featureService, clusteringService,
                propagationService, new ColumnTrainingDataBuilder(),
                new WeightedRuleFallbackTrainer(new ColumnModelVersioner()),
                modelStore, new ColumnModelMetadataFactory(clock), releaseManager, clock);
        AnnotationTaskRepository annotationRepository =
                new DefaultAnnotationTaskRepository(storage);
        RahaSampleService sampleService = new RahaSampleService(clusteringService,
                new SamplingService(new ClusterCoverageScorer(), new TupleSampler(),
                        new SamplingVersioner(), annotationRepository, clock), clock);
        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(storage);
        RahaDetectService detectService = new RahaDetectService(
                new PublishedColumnModelLoader(modelRepository, modelStore,
                        new ColumnModelCompatibilityValidator()),
                new ColumnModelPredictor(), detectionRepository, clock);

        TrainingWorkflow trainingWorkflow = new TrainingWorkflow(loader,
                profileService, planService, executionService, featureService,
                clusteringService, propagationService, trainService);
        SamplingWorkflow samplingWorkflow = new SamplingWorkflow(loader,
                profileService, planService, executionService, featureService,
                clusteringService, sampleService, sampleRecordService);
        DetectionWorkflow detectionWorkflow = new DetectionWorkflow(loader,
                profileService, planService, executionService, featureService,
                detectService);
        RahaTaskApplicationService applicationService = applicationService(
                storage, stageRepository, clock, trainingWorkflow,
                samplingWorkflow, detectionWorkflow);

        RahaTaskExecutionRequest trainingRequest = RahaTaskExecutionRequest.training(
                config(JobType.TRAINING), loadRequest(), directLabels(dataset),
                LabelPropagationMethod.HOMOGENEITY,
                LabelPropagationConfig.defaults(),
                LogisticRegressionTrainingConfig.defaults(), "raha");
        RahaTaskExecutionResult trained = applicationService.execute(trainingRequest);
        RahaTrainOutput trainOutput = trained.getPayload(RahaTrainOutput.class);

        assertEquals(JobStatus.SUCCEEDED, trained.getJob().getStatus());
        assertEquals(StageType.TRAIN,
                trained.getStages().get(trained.getStages().size() - 2).getStageType());
        assertNotNull(trainOutput);
        assertEquals(1, trainOutput.getCandidateModels().size());
        assertNotNull(trained.getResultLocation());

        RahaTaskExecutionResult reused = applicationService.execute(trainingRequest);
        assertTrue(reused.isReused());
        assertEquals(trained.getJob().getJobId(), reused.getJob().getJobId());
        assertNull(reused.getPayload());

        RahaTaskExecutionResult sampled = applicationService.execute(
                RahaTaskExecutionRequest.sampling(config(JobType.SAMPLING),
                        loadRequest(), Collections.<CellLabel>emptyList(), 1));
        RahaSampleOutput sampleOutput = sampled.getPayload(RahaSampleOutput.class);
        assertEquals(JobStatus.SUCCEEDED, sampled.getJob().getStatus());
        assertNotNull(sampleOutput);
        assertFalse(sampleOutput.getSampling().getTasks().isEmpty());
        assertNotNull(sampleOutput.getSampleBatch());
        assertTrue(sampled.getResultLocation().startsWith("fmdb://"));
        assertEquals(sampleOutput.getSampleBatch().getRecords().size(),
                fmdbGateway.read(FmdbPhysicalTable.SAMPLE_RECORD.getTableName())
                        .count());

        RahaColumnModel candidate = trainOutput.getCandidateModels().get("code");
        assertEquals(com.fiberhome.ml.raha.data.type.ModelStatus.PUBLISHED,
                candidate.getStatus());
        RahaTaskExecutionResult detected = applicationService.execute(
                RahaTaskExecutionRequest.detection(
                        config(JobType.DETECTION), loadRequest()));
        RahaDetectOutput detectOutput = detected.getPayload(RahaDetectOutput.class);

        assertEquals(JobStatus.SUCCEEDED, detected.getJob().getStatus());
        assertNotNull(detectOutput);
        assertEquals(8, detectOutput.getResults().size());
        assertTrue(detectOutput.getFailedColumns().isEmpty());
        assertEquals(8, detectionRepository.findByJob(
                detected.getJob().getJobId()).size());
    }

    private static RahaTaskApplicationService applicationService(
            InMemoryRahaRepository storage,
            StageRepository stageRepository,
            Clock clock,
            RahaWorkflow... workflows) {
        RahaJobOrchestrator orchestrator = new RahaJobOrchestrator(
                new RahaConfigValidator(), new ConfigVersioner(),
                new IdempotencyKeyGenerator(), new DefaultRahaIdGenerator(),
                new StageFailureDecider(), new DefaultJobRepository(storage),
                stageRepository, clock);
        return new RahaTaskApplicationService(orchestrator,
                new RahaWorkflowRegistry(Arrays.asList(workflows)), stageRepository);
    }

    private static RahaJobConfig config(JobType jobType) {
        Set<String> strategyTypes = Collections.singleton(
                StrategyTypes.PVD_CHARACTER_SET);
        StrategyConfig strategyConfig = new StrategyConfig(
                EnumSet.of(StrategyFamily.PVD), 10,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                1, 60000L, false, strategyTypes,
                Collections.<String>emptySet(),
                Collections.<String, Integer>emptyMap());
        return new RahaJobConfig(jobType, "dataset", "snapshot-v1",
                "memory", RowIdentityConfig.sourceKey("id"), 20260715L,
                strategyConfig,
                FeatureConfig.defaults(),
                new ModelConfig(ClassifierType.WEIGHTED_RULE, 0.5d, true),
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 2, 100),
                new SamplingConfig(2, true, false, 60000L),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());
    }

    private static DataLoadRequest loadRequest() {
        return new DataLoadRequest("dataset", "memory", "dataset",
                RowIdentityConfig.sourceKey("id"),
                DataFormat.CSV, Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), "snapshot-v1", "source-v1");
    }

    private static RahaDataset dataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("2", "ABC"),
                RowFactory.create("3", "ABC"), RowFactory.create("4", "ABC"),
                RowFactory.create("5", "ABC"), RowFactory.create("6", "ABC"),
                RowFactory.create("7", "BAD#"), RowFactory.create("8", "BAD#"));
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true);
        RowIdentityResult identity = new RowIdentityService().identify(
                SparkTestSession.get().createDataFrame(rows, schema),
                RowIdentityConfig.sourceKey("id"));
        return new RahaDataset("dataset", "snapshot-v1", "dataset",
                RowIdentityColumns.ROW_ID,
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("code", 1, "string", true, true, false)),
                identity.getDataFrame(),
                "schema-v1", Collections.emptyMap());
    }

    private static List<CellLabel> directLabels(RahaDataset dataset) {
        return Arrays.asList(
                label(rowId(dataset, "1"), 0, 1000L),
                label(rowId(dataset, "7"), 1, 1001L));
    }

    private static String rowId(RahaDataset dataset, String businessId) {
        return dataset.getDataFrame().filter("id = '" + businessId + "'")
                .select(dataset.getRowIdColumn()).first().getString(0);
    }

    private static CellLabel label(String rowId, int value, long createdAt) {
        String cellId = new CellCoordinate(
                "dataset", "snapshot-v1", rowId, "code").toCellId();
        return new CellLabel(cellId, value, LabelSource.GROUND_TRUTH,
                1.0d, null, null, "test", createdAt);
    }
}
