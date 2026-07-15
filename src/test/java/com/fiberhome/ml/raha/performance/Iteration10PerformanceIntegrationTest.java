package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.checkpoint.CheckpointRunResult;
import com.fiberhome.ml.raha.checkpoint.CheckpointTaskResult;
import com.fiberhome.ml.raha.checkpoint.StageCheckpointRunner;
import com.fiberhome.ml.raha.config.StrategyConfig;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.parallel.ParallelBatchResult;
import com.fiberhome.ml.raha.parallel.ParallelWorkItem;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultStageCheckpointRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyPlanGenerator;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证宽表 RVD 列对上限、并发限流、大表执行和可恢复失败重试。
 */
class Iteration10PerformanceIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldGenerateDeterministicBenchmarkDataWithConfiguredErrorRate() {
        BenchmarkDatasetSpec spec = new BenchmarkDatasetSpec(
                "local-error-rate", BenchmarkScale.SMALL,
                10000L, 12, 0.05d, 4, 17L);
        Dataset<Row> frame = new BenchmarkDatasetGenerator(
                SparkTestSession.get()).generate(spec);

        long errors = frame.filter("data_000 LIKE 'ERR_%'").count();

        assertEquals(10000L, frame.count());
        assertEquals(13, frame.columns().length);
        assertTrue(errors >= 350L && errors <= 650L);
        assertEquals(4, frame.rdd().getNumPartitions());
    }

    @Test
    void shouldLimitWideTableRvdPairsAndParallelConcurrency() {
        int maximumPairs = 25;
        RahaDataset wideDataset = wideDataset(80);
        StrategyConfig config = new StrategyConfig(
                EnumSet.of(StrategyFamily.RVD), 1000,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                maximumPairs, 30000L, false);

        List<StrategyPlan> plans = new StrategyPlanGenerator()
                .generate(wideDataset, config);
        ParallelBatchResult<Integer, Integer> batch = new BoundedParallelExecutor()
                .execute(workItems(100), 4, 30000L);

        assertEquals(maximumPairs, plans.size());
        assertEquals(100, batch.getSuccesses().size());
        assertTrue(batch.isSuccessful());
        assertTrue(batch.getMaxObservedConcurrency() <= 4);
    }

    @Test
    void shouldRunLargeFrameAndRecoverAfterTransientFailure() {
        BenchmarkDatasetSpec spec = new BenchmarkDatasetSpec(
                "local-large-recovery", BenchmarkScale.LARGE,
                50000L, 10, 0.02d, 8, 29L);
        Dataset<Row> frame = new BenchmarkDatasetGenerator(
                SparkTestSession.get()).generate(spec);
        StageCheckpointRunner runner = new StageCheckpointRunner(
                new DefaultStageCheckpointRepository(new InMemoryRahaRepository()),
                fixedClock());
        AtomicInteger invocations = new AtomicInteger();

        CheckpointRunResult<Long> recovered = runner.run(
                "job-large", StageType.RUN_STRATEGY,
                new ArtifactVersion("config-v1", "snapshot-large", "load", 1),
                "large-input-v1", 1, attemptId -> {
                    // 首次模拟可恢复的执行器故障，第二次重新触发 Spark 计算。
                    if (invocations.incrementAndGet() == 1) {
                        return CheckpointTaskResult.failure("EXECUTOR_LOST",
                                "执行器临时不可用", true,
                                Collections.singletonMap("attempt",
                                        String.valueOf(attemptId)));
                    }
                    long count = frame.filter("data_000 IS NOT NULL").count();
                    return CheckpointTaskResult.success(count,
                            "fmdb://checkpoint/job-large/strategy",
                            Collections.singletonMap("rowCount", String.valueOf(count)));
                });

        assertTrue(recovered.isSucceeded());
        assertEquals(2, recovered.getExecutedAttempts());
        assertEquals(50000L, recovered.getPayload().longValue());
        assertEquals(2, invocations.get());
    }

    private static RahaDataset wideDataset(int columnCount) {
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        Map<String, ColumnProfile> profiles = new LinkedHashMap<String, ColumnProfile>();
        for (int index = 0; index < columnCount; index++) {
            String name = String.format("column_%03d", index);
            columns.add(new ColumnMetadata(name, index, "string",
                    true, true, false));
            profiles.put(name, new ColumnProfile(name, 1000L, 0L,
                    100L, 1, 12, 0.0d,
                    Collections.<String, Long>emptyMap()));
        }
        return new RahaDataset("wide-dataset", "snapshot-v1", "wide_table",
                "column_000", columns, null, "schema-v1", profiles);
    }

    private static List<ParallelWorkItem<Integer, Integer>> workItems(int count) {
        List<ParallelWorkItem<Integer, Integer>> items =
                new ArrayList<ParallelWorkItem<Integer, Integer>>();
        for (int index = 0; index < count; index++) {
            final int value = index;
            items.add(new ParallelWorkItem<Integer, Integer>(index, () -> {
                Thread.sleep(2L);
                return value;
            }));
        }
        return items;
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
    }
}
