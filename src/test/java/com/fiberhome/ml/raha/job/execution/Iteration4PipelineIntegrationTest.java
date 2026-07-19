package com.fiberhome.ml.raha.job.execution;

import com.fiberhome.ml.raha.config.dto.FailureToleranceConfig;
import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.config.validation.ConfigVersioner;
import com.fiberhome.ml.raha.config.validation.RahaConfigValidator;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.metadata.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.FileRahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.identity.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.metadata.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.metadata.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.detection.explanation.DetectionExplanation;
import com.fiberhome.ml.raha.detection.explanation.DetectionExplanationService;
import com.fiberhome.ml.raha.detection.scoring.WeightedRuleScoringRule;
import com.fiberhome.ml.raha.detection.service.BasicDetectionService;
import com.fiberhome.ml.raha.detection.service.DetectionBatchResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.id.DefaultRahaIdGenerator;
import com.fiberhome.ml.raha.job.id.IdempotencyKeyGenerator;
import com.fiberhome.ml.raha.job.stage.data.ColumnProfileStageHandler;
import com.fiberhome.ml.raha.job.stage.data.DataLoadStageHandler;
import com.fiberhome.ml.raha.job.stage.detection.RuleDetectionStageHandler;
import com.fiberhome.ml.raha.job.stage.feature.FeatureStageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.strategy.StrategyPlanStageHandler;
import com.fiberhome.ml.raha.job.stage.strategy.StrategyRunStageHandler;
import com.fiberhome.ml.raha.repository.adapter.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.FeatureRepository;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 RVD、特征组装、加权检测、解释和持久化的完整迭代 4 链路。
 */
class Iteration4PipelineIntegrationTest {

    @TempDir
    Path tempDir;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldRunRvdFeatureAndDetectionPipelineEndToEnd() throws IOException {
        Path csv = tempDir.resolve("iteration4.csv");
        Files.write(csv, Arrays.asList(
                "id,code,city",
                "1,A,X",
                "2,A,X",
                "3,A,Y",
                "4,B,Z",
                "5,B,Z",
                "6,C,Q"), StandardCharsets.UTF_8);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        StrategyRepository strategyRepository = new DefaultStrategyRepository(storage);
        FeatureRepository featureRepository = new DefaultFeatureRepository(storage);
        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(storage);
        RahaJobConfig config = config(csv);
        RahaJobOrchestrator orchestrator = new RahaJobOrchestrator(
                new RahaConfigValidator(), new ConfigVersioner(),
                new IdempotencyKeyGenerator(), new DefaultRahaIdGenerator(),
                new StageFailureDecider(), new DefaultJobRepository(storage),
                new DefaultStageRepository(storage), clock);
        FileRahaDatasetLoader loader = new FileRahaDatasetLoader(
                SparkTestSession.get(), new RowIdentityService(),
                new RowIdValidator(), new SchemaHasher(),
                new ColumnMetadataFactory(), new SnapshotMetadataFactory(), clock);
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
        DataLoadRequest loadRequest = new DataLoadRequest(
                "dataset", csv.toString(), "test_table",
                RowIdentityConfig.sourceKey("id"), DataFormat.CSV,
                csvOptions(), Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.singleton("city"), null, "source-v1");

        RahaJob job = orchestrator.submit(config);
        JobRunResult run = orchestrator.execute(job, config, Arrays.asList(
                new DataLoadStageHandler(loader, loadRequest),
                new ColumnProfileStageHandler(profileService),
                new StrategyPlanStageHandler(planService),
                new StrategyRunStageHandler(executionService),
                new FeatureStageHandler(featureService),
                new RuleDetectionStageHandler(detectionService)));

        assertEquals(JobStatus.SUCCEEDED, run.getJob().getStatus());
        List<StrategyPlan> plans = castList(
                run.getAttributes().get(StageAttributeKeys.STRATEGY_PLANS));
        StrategyBatchResult strategyBatch = (StrategyBatchResult) run.getAttributes()
                .get(StageAttributeKeys.STRATEGY_BATCH_RESULT);
        FeatureAssemblyResult features = (FeatureAssemblyResult) run.getAttributes()
                .get(StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        DetectionBatchResult detections = (DetectionBatchResult) run.getAttributes()
                .get(StageAttributeKeys.DETECTION_BATCH_RESULT);
        assertEquals(2, plans.size());
        assertEquals(6, strategyBatch.getHits().size());
        assertEquals(12, features.getRows().size());
        assertEquals(12, detections.getResults().size());
        assertEquals(6L, detections.getMetrics().getErrorCellCount());
        assertEquals(12, detectionRepository.findByJob(job.getJobId()).size());
        assertFalse(featureRepository.findRows(job.getJobId(), "code").isEmpty());

        RahaDataset loadedDataset = (RahaDataset) run.getAttributes()
                .get(StageAttributeKeys.RAHA_DATASET);
        String firstRowId = loadedDataset.getDataFrame().filter("id = 1")
                .select(loadedDataset.getRowIdColumn()).first().getString(0);
        DetectionResult cityResult = findResult(detections, firstRowId, "city");
        SparseFeatureRow cityFeature = findFeature(features, firstRowId, "city");
        DetectionExplanation explanation = new DetectionExplanationService().explain(
                cityResult, plans, strategyBatch.getHits(), cityFeature);
        assertTrue(cityResult.isError());
        assertNull(cityResult.getMaskedValue());
        assertEquals(1, explanation.getStrategies().size());
        assertEquals("RVD_ONE_TO_MANY_CONFLICT",
                explanation.getStrategies().get(0).getReasonCode());
        assertNotNull(cityResult.getConfigVersion());
        assertNotNull(cityResult.getFeatureDictionaryVersion());
    }

    private static RahaJobConfig config(Path csv) {
        StrategyConfig strategyConfig = new StrategyConfig(
                EnumSet.of(StrategyFamily.RVD), 10,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                2, 30000L, false,
                Collections.singleton(StrategyTypes.RVD_ONE_TO_MANY),
                Collections.<String>emptySet(), Collections.<String, Integer>emptyMap());
        return new RahaJobConfig(JobType.DETECTION, "dataset", null,
                csv.toString(), RowIdentityConfig.sourceKey("id"), 1L,
                strategyConfig, FeatureConfig.defaults(),
                new ModelConfig(com.fiberhome.ml.raha.data.type.ClassifierType.WEIGHTED_RULE,
                        0.5d, false),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());
    }

    private static Map<String, String> csvOptions() {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put("header", "true");
        options.put("inferSchema", "true");
        return options;
    }

    private static DetectionResult findResult(DetectionBatchResult batch,
                                              String rowId,
                                              String columnName) {
        for (DetectionResult result : batch.getResults()) {
            if (result.getCoordinate().getRowId().equals(rowId)
                    && result.getCoordinate().getColumnName().equals(columnName)) {
                return result;
            }
        }
        return null;
    }

    private static SparseFeatureRow findFeature(FeatureAssemblyResult result,
                                                String rowId,
                                                String columnName) {
        for (SparseFeatureRow row : result.getRows()) {
            if (row.getCoordinate().getRowId().equals(rowId)
                    && row.getColumnName().equals(columnName)) {
                return row;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> castList(Object value) {
        return (List<T>) value;
    }
}
