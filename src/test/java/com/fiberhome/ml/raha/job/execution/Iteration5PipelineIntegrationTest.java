package com.fiberhome.ml.raha.job.execution;

import com.fiberhome.ml.raha.cluster.algorithm.HierarchicalColumnClusterer;
import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
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
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.FileRahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.LoadedDataset;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.detection.scoring.WeightedRuleScoringRule;
import com.fiberhome.ml.raha.detection.service.BasicDetectionService;
import com.fiberhome.ml.raha.detection.service.DetectionBatchResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.id.DefaultRahaIdGenerator;
import com.fiberhome.ml.raha.job.id.IdempotencyKeyGenerator;
import com.fiberhome.ml.raha.job.stage.feature.ClusterStageHandler;
import com.fiberhome.ml.raha.job.stage.data.ColumnProfileStageHandler;
import com.fiberhome.ml.raha.job.stage.data.DataLoadStageHandler;
import com.fiberhome.ml.raha.job.stage.detection.RuleDetectionStageHandler;
import com.fiberhome.ml.raha.job.stage.feature.FeatureStageHandler;
import com.fiberhome.ml.raha.job.stage.label.GroundTruthLabelStageHandler;
import com.fiberhome.ml.raha.job.stage.sample.SamplingStageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.strategy.StrategyPlanStageHandler;
import com.fiberhome.ml.raha.job.stage.strategy.StrategyRunStageHandler;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.GroundTruthLabelAdapter;
import com.fiberhome.ml.raha.repository.adapter.DefaultAnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.port.ClusterRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.FeatureRepository;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTaskStatus;
import com.fiberhome.ml.raha.sampling.service.SamplingBatchResult;
import com.fiberhome.ml.raha.sampling.service.SamplingService;
import com.fiberhome.ml.raha.sampling.service.SamplingVersioner;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证固定数据从基础策略、检测到聚类、采样和真值标注的迭代 5 完整链路。
 */
class Iteration5PipelineIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldAlignPythonSemanticsAndRunEvaluationPipelineEndToEnd() throws Exception {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        Path dirtyPath = resourcePath("alignment/iteration5-dirty.csv");
        Path cleanPath = resourcePath("alignment/iteration5-clean.csv");
        Properties baseline = baseline();
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        FileRahaDatasetLoader loader = loader(clock);
        RahaDataset groundTruth = loader.load(loadRequest(
                "ground-truth", cleanPath, "clean-v1")).getDataset();
        RahaJobConfig config = config(dirtyPath);
        StrategyRepository strategyRepository = new DefaultStrategyRepository(storage);
        FeatureRepository featureRepository = new DefaultFeatureRepository(storage);
        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(storage);
        ClusterRepository clusterRepository = new DefaultClusterRepository(storage);
        AnnotationTaskRepository taskRepository =
                new DefaultAnnotationTaskRepository(storage);
        RahaJobOrchestrator orchestrator = new RahaJobOrchestrator(
                new RahaConfigValidator(), new ConfigVersioner(),
                new IdempotencyKeyGenerator(), new DefaultRahaIdGenerator(),
                new StageFailureDecider(), new DefaultJobRepository(storage),
                new DefaultStageRepository(storage), clock);
        ColumnProfileService profileService = new ColumnProfileService(
                new ColumnProfiler(), new DefaultColumnProfileRepository(storage), clock);
        StrategyPlanService planService = new StrategyPlanService(
                new StrategyPlanGenerator(), strategyRepository, clock);
        StrategyExecutionService executionService = new StrategyExecutionService(
                new StrategyExecutor(StrategyRegistry.defaults(), clock),
                strategyRepository, clock);
        FeatureService featureService = new FeatureService(
                new FeatureAssembler(new FeatureDictionaryVersioner(), clock),
                featureRepository, clock);
        BasicDetectionService detectionService = new BasicDetectionService(
                new WeightedRuleScoringRule(), detectionRepository, clock);
        ColumnClusteringService clusteringService = new ColumnClusteringService(
                new HierarchicalColumnClusterer(new ClusterVersioner(), clock),
                clusterRepository, clock);
        SamplingService samplingService = new SamplingService(
                new ClusterCoverageScorer(), new TupleSampler(), new SamplingVersioner(),
                taskRepository, clock);

        RahaJob job = orchestrator.submit(config);
        JobRunResult run = orchestrator.execute(job, config, Arrays.asList(
                new DataLoadStageHandler(loader, loadRequest("dataset", dirtyPath, "dirty-v1")),
                new ColumnProfileStageHandler(profileService),
                new StrategyPlanStageHandler(planService),
                new StrategyRunStageHandler(executionService),
                new FeatureStageHandler(featureService),
                new RuleDetectionStageHandler(detectionService),
                new ClusterStageHandler(clusteringService),
                new SamplingStageHandler(samplingService, 1),
                new GroundTruthLabelStageHandler(
                        new GroundTruthLabelAdapter(clock), taskRepository, groundTruth)));

        assertEquals(JobStatus.SUCCEEDED, run.getJob().getStatus());
        List<StrategyPlan> plans = castList(
                run.getAttributes().get(StageAttributeKeys.STRATEGY_PLANS));
        StrategyBatchResult strategies = (StrategyBatchResult) run.getAttributes()
                .get(StageAttributeKeys.STRATEGY_BATCH_RESULT);
        FeatureAssemblyResult features = (FeatureAssemblyResult) run.getAttributes()
                .get(StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        DetectionBatchResult detections = (DetectionBatchResult) run.getAttributes()
                .get(StageAttributeKeys.DETECTION_BATCH_RESULT);
        ClusteringBatchResult clustering = (ClusteringBatchResult) run.getAttributes()
                .get(StageAttributeKeys.CLUSTERING_BATCH_RESULT);
        SamplingBatchResult sampling = (SamplingBatchResult) run.getAttributes()
                .get(StageAttributeKeys.SAMPLING_BATCH_RESULT);
        List<AnnotationTask> tasks = castList(
                run.getAttributes().get(StageAttributeKeys.ANNOTATION_TASKS));
        List<CellLabel> labels = castList(
                run.getAttributes().get(StageAttributeKeys.CELL_LABELS));

        assertEquals(50, features.getRows().size());
        assertEquals(5, features.getDictionaries().size());
        assertTrue(!features.getDictionaries().containsKey("id"));
        assertEquals(50, detections.getResults().size());
        assertEquals(50L, clustering.getMetrics().getAssignmentCount());
        assertEquals(3, sampling.getTasks().size());
        assertEquals(3, tasks.size());
        assertEquals(15, labels.size());
        assertTrue(tasks.stream().allMatch(
                task -> task.getStatus() == AnnotationTaskStatus.COMPLETED));
        assertEquals(3, taskRepository.findByJob(job.getJobId()).size());
        RahaDataset loadedDataset = (RahaDataset) run.getAttributes()
                .get(StageAttributeKeys.RAHA_DATASET);
        Map<String, String> businessIds = businessIds(loadedDataset);

        assertEquals(cellSet(baseline.getProperty("rvd.code.city.cells")),
                rvdCodeCityCells(strategies.getHits(), businessIds));
        assertTrue(plans.size() < Integer.parseInt(
                baseline.getProperty("strategy.profile.count")));
        assertTrue(javaErrorCells(detections).containsAll(
                cellSet(baseline.getProperty("detected.cells"), businessIds)));
        // Python 按字符策略命中斜杠，Java 日期格式允许斜杠，因此不产生格式错误原因。
        assertFalse(hasReason(strategies.getHits(), businessIds.get("7"), "event_date",
                "PVD_FORMAT_MISMATCH"));
    }

    private static RahaJobConfig config(Path dirtyPath) {
        return new RahaJobConfig(JobType.EVALUATION, "dataset", null,
                dirtyPath.toString(), RowIdentityConfig.sourceKey("id"), 20260714L,
                StrategyConfig.defaults(), FeatureConfig.defaults(),
                new ModelConfig(com.fiberhome.ml.raha.data.type.ClassifierType.WEIGHTED_RULE,
                        0.5d, false),
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 3, 100),
                new SamplingConfig(3, true, false, 60000L),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());
    }

    private static FileRahaDatasetLoader loader(Clock clock) {
        return new FileRahaDatasetLoader(SparkTestSession.get(),
                new RowIdentityService(), new RowIdValidator(),
                new SchemaHasher(), new ColumnMetadataFactory(),
                new SnapshotMetadataFactory(), clock);
    }

    private static DataLoadRequest loadRequest(String datasetId,
                                               Path path,
                                               String sourceVersion) {
        return new DataLoadRequest(datasetId, path.toString(), datasetId,
                RowIdentityConfig.sourceKey("id"),
                DataFormat.CSV, csvOptions(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), Collections.singleton("email"),
                null, sourceVersion);
    }

    private static Map<String, String> csvOptions() {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put("header", "true");
        options.put("inferSchema", "false");
        return options;
    }

    private static Set<String> rvdCodeCityCells(
            List<StrategyHit> hits,
            Map<String, String> businessIds) {
        Map<String, String> rowIds = invert(businessIds);
        Set<String> cells = new LinkedHashSet<String>();
        for (StrategyHit hit : hits) {
            if ("RVD_ONE_TO_MANY_CONFLICT".equals(hit.getReasonCode())
                    && "code->city".equals(hit.getReasonDetails().get("dependency"))) {
                cells.add(rowIds.get(hit.getCoordinate().getRowId()) + ":"
                        + hit.getCoordinate().getColumnName());
            }
        }
        return cells;
    }

    private static Set<String> javaErrorCells(DetectionBatchResult detections) {
        Set<String> cells = new LinkedHashSet<String>();
        for (DetectionResult result : detections.getResults()) {
            if (result.isError()) {
                cells.add(result.getCoordinate().getRowId() + ":"
                        + result.getCoordinate().getColumnName());
            }
        }
        return cells;
    }

    private static boolean hasReason(List<StrategyHit> hits,
                                     String rowId,
                                     String columnName,
                                     String reasonCode) {
        for (StrategyHit hit : hits) {
            if (hit.getCoordinate().getRowId().equals(rowId)
                    && hit.getCoordinate().getColumnName().equals(columnName)
                    && hit.getReasonCode().equals(reasonCode)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> cellSet(String value) {
        return new LinkedHashSet<String>(Arrays.asList(value.split(",")));
    }

    private static Set<String> cellSet(String value,
                                       Map<String, String> businessIds) {
        Set<String> cells = new LinkedHashSet<String>();
        for (String cell : value.split(",")) {
            String[] parts = cell.split(":", 2);
            cells.add(businessIds.get(parts[0]) + ":" + parts[1]);
        }
        return cells;
    }

    private static Map<String, String> businessIds(RahaDataset dataset) {
        Map<String, String> ids = new LinkedHashMap<String, String>();
        for (org.apache.spark.sql.Row row : dataset.getDataFrame()
                .select("id", dataset.getRowIdColumn()).collectAsList()) {
            ids.put(row.getString(0), row.getString(1));
        }
        return ids;
    }

    private static Map<String, String> invert(Map<String, String> values) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            result.put(entry.getValue(), entry.getKey());
        }
        return result;
    }

    private static Properties baseline() throws Exception {
        Properties properties = new Properties();
        try (InputStream stream = Iteration5PipelineIntegrationTest.class
                .getClassLoader().getResourceAsStream(
                        "alignment/iteration5-python-baseline.properties")) {
            properties.load(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8));
        }
        return properties;
    }

    private static Path resourcePath(String resource) throws URISyntaxException {
        return Paths.get(Iteration5PipelineIntegrationTest.class
                .getClassLoader().getResource(resource).toURI());
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> castList(Object value) {
        return (List<T>) value;
    }
}
