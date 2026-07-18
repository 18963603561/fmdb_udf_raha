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
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.evaluation.GroundTruthDifferenceResult;
import com.fiberhome.ml.raha.evaluation.GroundTruthDifferenceService;
import com.fiberhome.ml.raha.evaluation.metrics.CellScore;
import com.fiberhome.ml.raha.evaluation.metrics.DetectionEvaluationMetrics;
import com.fiberhome.ml.raha.evaluation.metrics.DetectionEvaluationService;
import com.fiberhome.ml.raha.evaluation.threshold.ThresholdComparisonResult;
import com.fiberhome.ml.raha.evaluation.threshold.ThresholdComparisonService;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.FileColumnModelStore;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.prediction.ColumnPrediction;
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
import com.fiberhome.ml.raha.repository.adapter.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.port.CellLabelRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.service.SamplingService;
import com.fiberhome.ml.raha.sampling.service.SamplingVersioner;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.service.common.RahaTaskResult;
import com.fiberhome.ml.raha.service.common.RahaTaskStatus;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.detect.RahaDetectRequest;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleRequest;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 Raha 策略、聚类、传播、训练、阈值、发布、检测和评测学习闭环。
 */
class Iteration7RahaLearningPipelineIntegrationTest {

    /** JUnit 提供的隔离模型目录。 */
    @TempDir
    Path modelDirectory;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldRunRahaLearningAndEvaluationPipelineEndToEnd() {
        Clock clock = fixedClock();
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        ArtifactVersion profileVersion = version("profile-stage");
        RahaDataset dirty = new ColumnProfileService(new ColumnProfiler(),
                new DefaultColumnProfileRepository(storage), clock)
                .profileAndSave(dirtyDataset(), profileVersion);
        RahaDataset truthDataset = truthDataset();
        CellLabelRepository labelRepository = new DefaultCellLabelRepository(storage);
        GroundTruthDifferenceResult truth = new GroundTruthDifferenceService(
                labelRepository, clock).compareAndSave(
                "evaluation-job", dirty, truthDataset, version("truth-stage"));
        List<CellLabel> directLabels = directLabels(truth.getLabels());
        assertEquals(8, truth.getLabels().size());
        assertEquals(2L, truth.getPositiveCount());

        StrategyRepository strategyRepository = new DefaultStrategyRepository(storage);
        ColumnClusteringService clusteringService = new ColumnClusteringService(
                new HierarchicalColumnClusterer(new ClusterVersioner(), clock),
                new DefaultClusterRepository(storage), clock);
        AnnotationTaskRepository taskRepository =
                new DefaultAnnotationTaskRepository(storage);
        RahaSampleService sampleService = new RahaSampleService(clusteringService,
                new SamplingService(new ClusterCoverageScorer(), new TupleSampler(),
                        new SamplingVersioner(), taskRepository, clock), clock);

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
                clusteringService, new LabelPropagationService(labelRepository, clock),
                new ColumnTrainingDataBuilder(),
                new WeightedRuleFallbackTrainer(new ColumnModelVersioner()),
                modelStore, new ColumnModelMetadataFactory(clock),
                releaseManager, clock);
        RahaJobConfig config = trainingConfig();
        RahaTaskResult<RahaTrainOutput> trained = trainService.train(
                new RahaTrainRequest("train-job", "train-stage", dirty, config,
                        directLabels, LabelPropagationMethod.HOMOGENEITY,
                        LabelPropagationConfig.defaults(),
                        LogisticRegressionTrainingConfig.defaults(), "raha",
                        version("train-stage")));

        assertEquals(RahaTaskStatus.SUCCEEDED, trained.getStatus());
        assertNotNull(trained.getResultLocation());
        assertEquals(1, trained.getPayload().getStrategyPlans().size());
        assertEquals(StrategyTypes.PVD_CHARACTER_SET,
                trained.getPayload().getStrategyPlans().get(0)
                        .getConfiguration().get("strategyType"));
        assertEquals(2, trained.getPayload().getClustering()
                .getResults().get("code").getEffectiveClusterCount());
        assertTrue(trained.getPayload().getPropagation().getMetrics()
                .getPropagatedLabelCount() > 0L);
        assertEquals(1, trained.getPayload().getCandidateModels().size());
        RahaColumnModel candidate = trained.getPayload()
                .getCandidateModels().get("code");

        RahaTaskResult<RahaSampleOutput> sampled = sampleService.sample(
                new RahaSampleRequest("sample-job", trained.getPayload().getFeatures(),
                        Collections.<CellLabel>emptyList(), config.getClusteringConfig(),
                        config.getSamplingConfig(), 1, config.getRandomSeed(),
                        version("sample-stage")));
        assertEquals(RahaTaskStatus.SUCCEEDED, sampled.getStatus());
        assertNotNull(sampled.getResultLocation());
        assertEquals(2L, sampled.getSummary().getSuccessfulCount());
        assertEquals(2, sampled.getPayload().getSampling().getTasks().size());
        assertEquals(2, taskRepository.findByJob("sample-job").size());

        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(storage);
        RahaDetectService detectService = new RahaDetectService(
                new PublishedColumnModelLoader(modelRepository, modelStore,
                        new ColumnModelCompatibilityValidator()),
                new ColumnModelPredictor(), detectionRepository, clock);
        RahaDetectRequest beforePublishRequest = detectRequest(
                "detect-before-publish", dirty, trained.getPayload(), version("detect-before"));
        RahaTaskResult<RahaDetectOutput> beforePublish =
                detectService.detect(beforePublishRequest);
        assertEquals(RahaTaskStatus.FAILED, beforePublish.getStatus());
        assertEquals("NO_PUBLISHED_MODEL_RESULT", beforePublish.getErrorCode());

        List<ColumnPrediction> predictions = new ColumnModelPredictor().predict(
                modelStore.load(candidate.getModelPath()),
                trained.getPayload().getFeatures().getRowsByColumn("code"));
        List<CellScore> scores = new ArrayList<CellScore>();
        for (ColumnPrediction prediction : predictions) {
            scores.add(new CellScore(prediction.getCellId(), prediction.getScore()));
        }
        ThresholdComparisonResult threshold = new ThresholdComparisonService(
                new DetectionEvaluationService(), modelRepository, clock)
                .compareAndSave(candidate, scores, truth.getLabels(),
                        Arrays.asList(0.0d, 0.1d, 0.2d, 0.3d, 0.4d,
                                0.5d, 0.6d, 0.7d, 0.8d, 0.9d, 1.0d),
                        version("threshold-stage"));
        assertTrue(threshold.getUpdatedModel().getMetrics()
                .get("evaluation.f1") > 0.5d,
                () -> diagnostics(trained.getPayload(), predictions, threshold));
        releaseManager.publish("dataset", "code", candidate.getModelVersion(),
                version("publish-stage"));

        RahaTaskResult<RahaDetectOutput> detected = detectService.detect(detectRequest(
                "detect-job", dirty, trained.getPayload(), version("detect-stage")));
        assertEquals(RahaTaskStatus.SUCCEEDED, detected.getStatus());
        assertNotNull(detected.getResultLocation());
        assertEquals(1L, detected.getSummary().getSuccessfulCount());
        assertEquals(8, detected.getPayload().getResults().size());
        assertTrue(detected.getPayload().getFailedColumns().isEmpty());
        assertEquals(8, detectionRepository.findByJob("detect-job").size());
        DetectionEvaluationMetrics metrics = new DetectionEvaluationService().evaluate(
                detected.getPayload().getResults(), truth.getLabels());
        assertTrue(metrics.getPrecision() > 0.0d);
        assertTrue(metrics.getRecall() > 0.0d);
        assertTrue(metrics.getF1() > 0.5d);
        assertTrue(metrics.getAveragePrecision() > 0.5d);
        assertFalse(errorRows(detected.getPayload()).isEmpty());
    }

    private RahaDetectRequest detectRequest(String jobId,
                                            RahaDataset dataset,
                                            RahaTrainOutput output,
                                            ArtifactVersion version) {
        return new RahaDetectRequest(jobId, "detect-stage", "config-v1", dataset,
                output.getFeatures(), output.getStrategyPlanVersion(), version);
    }

    private static RahaJobConfig trainingConfig() {
        Set<String> strategyTypes = Collections.singleton(StrategyTypes.PVD_CHARACTER_SET);
        StrategyConfig strategyConfig = new StrategyConfig(
                EnumSet.of(StrategyFamily.PVD), 10,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                1, 60000L, false, strategyTypes,
                Collections.<String>emptySet(), Collections.<String, Integer>emptyMap());
        return new RahaJobConfig(JobType.TRAINING, "dataset", "snapshot-v1",
                "memory", "id", true, 20260715L, strategyConfig,
                FeatureConfig.defaults(),
                new ModelConfig(ClassifierType.WEIGHTED_RULE, 0.5d, true),
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 2, 100),
                new SamplingConfig(2, true, false, 60000L),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());
    }

    private static RahaDataset dirtyDataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("2", "ABC"),
                RowFactory.create("3", "ABC"), RowFactory.create("4", "ABC"),
                RowFactory.create("5", "ABC"), RowFactory.create("6", "ABC"),
                RowFactory.create("7", "BAD#"), RowFactory.create("8", "BAD#"));
        return dataset("dataset", "snapshot-v1", rows);
    }

    private static RahaDataset truthDataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("2", "ABC"),
                RowFactory.create("3", "ABC"), RowFactory.create("4", "ABC"),
                RowFactory.create("5", "ABC"), RowFactory.create("6", "ABC"),
                RowFactory.create("7", "ABC"), RowFactory.create("8", "ABC"));
        return dataset("truth", "truth-v1", rows);
    }

    private static RahaDataset dataset(String datasetId,
                                       String snapshotId,
                                       List<Row> rows) {
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true);
        return new RahaDataset(datasetId, snapshotId, datasetId, "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("code", 1, "string", true, true, false)),
                SparkTestSession.get().createDataFrame(rows, schema),
                "schema-v1", Collections.emptyMap());
    }

    private static List<CellLabel> directLabels(List<CellLabel> labels) {
        Set<String> selected = new LinkedHashSet<String>(Arrays.asList(
                new CellCoordinate("dataset", "snapshot-v1", "1", "code").toCellId(),
                new CellCoordinate("dataset", "snapshot-v1", "7", "code").toCellId()));
        List<CellLabel> direct = new ArrayList<CellLabel>();
        for (CellLabel label : labels) {
            if (selected.contains(label.getCellId())) {
                direct.add(label);
            }
        }
        return direct;
    }

    private static Set<String> errorRows(RahaDetectOutput output) {
        Set<String> rows = new LinkedHashSet<String>();
        output.getResults().stream().filter(result -> result.isError())
                .forEach(result -> rows.add(result.getCoordinate().getRowId()));
        return rows;
    }

    private static String diagnostics(RahaTrainOutput output,
                                      List<ColumnPrediction> predictions,
                                      ThresholdComparisonResult threshold) {
        StringBuilder text = new StringBuilder("selected=")
                .append(threshold.getSelectedThreshold()).append(",f1=")
                .append(threshold.getUpdatedModel().getMetrics().get("evaluation.f1"));
        List<com.fiberhome.ml.raha.feature.domain.SparseFeatureRow> rows =
                output.getFeatures().getRowsByColumn("code");
        for (int index = 0; index < rows.size(); index++) {
            text.append(",row=").append(rows.get(index).getCoordinate().getRowId())
                    .append(":").append(predictions.get(index).getScore())
                    .append(":").append(rows.get(index).getValues());
        }
        output.getClustering().getResults().get("code").getAssignments().forEach(
                assignment -> text.append(",cluster=")
                        .append(assignment.getCoordinate().getRowId()).append(":")
                        .append(assignment.getClusterId()));
        return text.toString();
    }

    private static ArtifactVersion version(String stageId) {
        return new ArtifactVersion("config-v1", "snapshot-v1", stageId, 1);
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
    }
}
