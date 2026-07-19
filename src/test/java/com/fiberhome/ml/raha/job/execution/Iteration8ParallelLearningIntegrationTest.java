package com.fiberhome.ml.raha.job.execution;

import com.fiberhome.ml.raha.cluster.algorithm.HierarchicalColumnClusterer;
import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.config.dto.FailureToleranceConfig;
import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.RowIdentityConfig;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.FileColumnModelStore;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.release.ColumnModelCompatibilityValidator;
import com.fiberhome.ml.raha.model.release.ColumnModelMetadataFactory;
import com.fiberhome.ml.raha.model.release.ColumnModelVersioner;
import com.fiberhome.ml.raha.model.release.ModelReleaseManager;
import com.fiberhome.ml.raha.model.release.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataBuilder;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.model.training.WeightedRuleFallbackTrainer;
import com.fiberhome.ml.raha.repository.adapter.DefaultCellLabelRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.detect.RahaDetectRequest;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import com.fiberhome.ml.raha.service.train.RahaTrainRequest;
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
import java.util.ArrayList;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证两列 Raha 特征、聚类、训练和检测服务在受限并发下形成完整结果。
 */
class Iteration8ParallelLearningIntegrationTest {

    /** JUnit 提供的隔离模型目录。 */
    @TempDir
    Path modelDirectory;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldTrainAndDetectTwoColumnsWithConfiguredConcurrency() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        RahaDataset dataset = new ColumnProfileService(new ColumnProfiler(),
                new DefaultColumnProfileRepository(storage), clock)
                .profileAndSave(dataset(), version("profile-stage"));
        RahaJobConfig config = trainingConfig();
        StrategyRepository strategyRepository = new DefaultStrategyRepository(storage);
        ColumnClusteringService clusteringService = new ColumnClusteringService(
                new HierarchicalColumnClusterer(new ClusterVersioner(), clock),
                new DefaultClusterRepository(storage), clock);
        ModelMetadataRepository modelRepository =
                new DefaultModelMetadataRepository(storage);
        FileColumnModelStore modelStore = new FileColumnModelStore(modelDirectory);
        ModelReleaseManager releaseManager = new ModelReleaseManager(modelRepository, clock);
        RahaTrainService trainService = new RahaTrainService(
                new StrategyPlanService(new StrategyPlanGenerator(),
                        strategyRepository, clock),
                new StrategyExecutionService(new StrategyExecutor(
                        StrategyRegistry.defaults(), clock), strategyRepository, clock),
                new FeatureService(new FeatureAssembler(
                        new FeatureDictionaryVersioner(), clock),
                        new DefaultFeatureRepository(storage), clock),
                clusteringService,
                new LabelPropagationService(
                        new DefaultCellLabelRepository(storage), clock),
                new ColumnTrainingDataBuilder(),
                new WeightedRuleFallbackTrainer(new ColumnModelVersioner()),
                modelStore, new ColumnModelMetadataFactory(clock),
                releaseManager, clock);

        RahaServiceResult<RahaTrainOutput> trained = trainService.train(
                new RahaTrainRequest("parallel-train", "train-stage", dataset,
                        config, directLabels(), LabelPropagationMethod.HOMOGENEITY,
                        LabelPropagationConfig.defaults(),
                        LogisticRegressionTrainingConfig.defaults(), "raha",
                        version("train-stage")));

        assertEquals(JobStatus.SUCCEEDED, trained.getStatus());
        assertEquals(2, trained.getPayload().getFeatures().getDictionaries().size());
        assertEquals(2, trained.getPayload().getClustering().getResults().size());
        assertEquals(2, trained.getPayload().getCandidateModels().size());
        assertBoundedConcurrency(trained.getSummary().getDetails()
                .get("maxObservedColumnConcurrency"), 2);
        for (RahaColumnModel candidate
                : trained.getPayload().getCandidateModels().values()) {
            releaseManager.publish("parallel-dataset", candidate.getColumnName(),
                    candidate.getModelVersion(), version("publish-" + candidate.getColumnName()));
        }

        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(storage);
        RahaDetectService detectService = new RahaDetectService(
                new PublishedColumnModelLoader(modelRepository, modelStore,
                        new ColumnModelCompatibilityValidator()),
                new ColumnModelPredictor(), detectionRepository, clock);
        RahaServiceResult<RahaDetectOutput> detected = detectService.detect(
                new RahaDetectRequest("parallel-detect", "detect-stage", "config-v1",
                        dataset, trained.getPayload().getFeatures(),
                        trained.getPayload().getStrategyPlanVersion(),
                        version("detect-stage"), config.getResourceConfig()));

        assertEquals(JobStatus.SUCCEEDED, detected.getStatus());
        assertEquals(2, detected.getPayload().getModelVersions().size());
        assertEquals(16, detected.getPayload().getResults().size());
        assertBoundedConcurrency(detected.getSummary().getDetails()
                .get("maxObservedColumnConcurrency"), 2);
        assertTrue(detected.getPayload().getFailedColumns().isEmpty());
        assertEquals(16, detectionRepository.findByJob("parallel-detect").size());
    }

    private static void assertBoundedConcurrency(String observedValue,
                                                 int configuredMaximum) {
        int observed = Integer.parseInt(observedValue);
        // 集成任务很短，线程调度可能未饱和；这里只验证实际并发始终处于配置边界内。
        assertTrue(observed >= 1 && observed <= configuredMaximum);
    }

    private static RahaJobConfig trainingConfig() {
        Set<String> strategyTypes = Collections.singleton(StrategyTypes.PVD_CHARACTER_SET);
        StrategyConfig strategyConfig = new StrategyConfig(
                EnumSet.of(StrategyFamily.PVD), 10,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                1, 60000L, false, strategyTypes,
                Collections.<String>emptySet(), Collections.<String, Integer>emptyMap());
        ResourceConfig resourceConfig = new ResourceConfig(
                2, 2, 1024L, "MEMORY_AND_DISK", 4096L, 60000L);
        return new RahaJobConfig(JobType.TRAINING, "parallel-dataset", "snapshot-v1",
                "memory", RowIdentityConfig.sourceKey("id"), 20260715L,
                strategyConfig,
                FeatureConfig.defaults(),
                new ModelConfig(ClassifierType.WEIGHTED_RULE, 0.5d, true),
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 2, 100),
                new SamplingConfig(2, true, false, 60000L),
                resourceConfig, FailureToleranceConfig.defaults());
    }

    private static RahaDataset dataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "ABC", "SHANGHAI"),
                RowFactory.create("2", "ABC", "SHANGHAI"),
                RowFactory.create("3", "ABC", "SHANGHAI"),
                RowFactory.create("4", "ABC", "SHANGHAI"),
                RowFactory.create("5", "ABC", "SHANGHAI"),
                RowFactory.create("6", "ABC", "SHANGHAI"),
                RowFactory.create("7", "BAD#", "CITY#"),
                RowFactory.create("8", "BAD#", "CITY#"));
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true)
                .add("city", DataTypes.StringType, true);
        return new RahaDataset("parallel-dataset", "snapshot-v1", "parallel-table", "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("code", 1, "string", true, true, false),
                        new ColumnMetadata("city", 2, "string", true, true, false)),
                SparkTestSession.get().createDataFrame(rows, schema),
                "schema-v1", Collections.emptyMap());
    }

    private static List<CellLabel> directLabels() {
        List<CellLabel> labels = new ArrayList<CellLabel>();
        for (String columnName : Arrays.asList("code", "city")) {
            labels.add(label("1", columnName, 0));
            labels.add(label("7", columnName, 1));
        }
        return labels;
    }

    private static CellLabel label(String rowId, String columnName, int value) {
        String cellId = new CellCoordinate("parallel-dataset", "snapshot-v1",
                rowId, columnName).toCellId();
        return new CellLabel(cellId, value, LabelSource.GROUND_TRUTH,
                1.0d, null, null, "iteration-8-test", 1000L);
    }

    private static ArtifactVersion version(String stageId) {
        return new ArtifactVersion("config-v1", "snapshot-v1", stageId, 1);
    }
}
