package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.config.ConfigVersioner;
import com.fiberhome.ml.raha.config.RahaConfigValidator;
import com.fiberhome.ml.raha.config.RahaJobConfig;
import com.fiberhome.ml.raha.data.JobStatus;
import com.fiberhome.ml.raha.data.JobType;
import com.fiberhome.ml.raha.data.loader.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.FileRahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.repository.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.StrategyRepository;
import com.fiberhome.ml.raha.strategy.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.StrategyPlanService;
import com.fiberhome.ml.raha.strategy.StrategyRegistry;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证文件读取、画像、策略计划、OD/PVD 执行和仓储的完整迭代 3 链路。
 */
class Iteration3PipelineIntegrationTest {

    @TempDir
    Path tempDir;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldRunStrategyPipelineEndToEnd() throws IOException {
        Path csv = tempDir.resolve("iteration3.csv");
        Files.write(csv, csvRows(), StandardCharsets.UTF_8);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        StrategyRepository strategyRepository = new DefaultStrategyRepository(storage);
        RahaJobConfig config = RahaJobConfig.defaults(
                JobType.DETECTION, "dataset", csv.toString(), "id");
        RahaJobOrchestrator orchestrator = new RahaJobOrchestrator(
                new RahaConfigValidator(), new ConfigVersioner(),
                new IdempotencyKeyGenerator(), new DefaultRahaIdGenerator(),
                new StageFailureDecider(), new DefaultJobRepository(storage),
                new DefaultStageRepository(storage), clock);
        FileRahaDatasetLoader loader = new FileRahaDatasetLoader(
                SparkTestSession.get(), new RowIdValidator(), new SchemaHasher(),
                new ColumnMetadataFactory(), new SnapshotMetadataFactory(), clock);
        ColumnProfileService profileService = new ColumnProfileService(
                new ColumnProfiler(), new DefaultColumnProfileRepository(storage), clock);
        StrategyPlanService planService = new StrategyPlanService(
                new StrategyPlanGenerator(), strategyRepository, clock);
        StrategyExecutionService executionService = new StrategyExecutionService(
                new StrategyExecutor(StrategyRegistry.defaults(), clock),
                strategyRepository, clock);
        DataLoadRequest loadRequest = new DataLoadRequest(
                "dataset", csv.toString(), "test_table", "id", DataFormat.CSV,
                csvOptions(), Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), null, "source-v1");

        RahaJob job = orchestrator.submit(config);
        JobRunResult result = orchestrator.execute(job, config, Arrays.asList(
                new DataLoadStageHandler(loader, loadRequest),
                new ColumnProfileStageHandler(profileService),
                new StrategyPlanStageHandler(planService),
                new StrategyRunStageHandler(executionService)));

        assertEquals(JobStatus.SUCCEEDED, result.getJob().getStatus());
        List<StrategyPlan> plans = castPlans(
                result.getAttributes().get(StageAttributeKeys.STRATEGY_PLANS));
        StrategyBatchResult batch = (StrategyBatchResult) result.getAttributes()
                .get(StageAttributeKeys.STRATEGY_BATCH_RESULT);
        assertFalse(plans.isEmpty());
        assertEquals(0L, batch.getFailedCount());
        assertFalse(batch.getHits().isEmpty());
        assertEquals(plans.size(), strategyRepository.findSummaries(job.getJobId()).size());
        assertEquals(batch.getHits().size(), strategyRepository.findHits(job.getJobId()).size());
        assertTrue(batch.getHits().stream()
                .allMatch(hit -> hit.getCoordinate().getSnapshotId()
                        .equals(result.getJob().getSnapshotId())));
    }

    private static List<String> csvRows() {
        List<String> rows = new ArrayList<String>();
        rows.add("id,amount,code,email");
        for (int index = 1; index <= 12; index++) {
            String amount = index == 10 ? "1000" : String.valueOf(10 + index % 4);
            String code = index == 10 ? "A1#" : "ABC";
            String email = index == 10 ? "bad-address" : "user" + index + "@example.com";
            rows.add(index + "," + amount + "," + code + "," + email);
        }
        return rows;
    }

    private static Map<String, String> csvOptions() {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put("header", "true");
        options.put("inferSchema", "true");
        return options;
    }

    @SuppressWarnings("unchecked")
    private static List<StrategyPlan> castPlans(Object value) {
        return (List<StrategyPlan>) value;
    }
}
