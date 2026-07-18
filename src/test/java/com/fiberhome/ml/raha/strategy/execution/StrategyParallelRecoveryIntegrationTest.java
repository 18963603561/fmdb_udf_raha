package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.strategy.api.DetectionStrategy;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.plan.StrategyCandidate;
import com.fiberhome.ml.raha.strategy.plan.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证策略并发限流、结果顺序和仅重跑失败策略的恢复语义。
 */
class StrategyParallelRecoveryIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldRunStrategiesConcurrentlyAndOnlyResumeFailedOne() {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger firstInvocations = new AtomicInteger();
        AtomicInteger flakyInvocations = new AtomicInteger();
        AtomicInteger thirdInvocations = new AtomicInteger();
        DetectionStrategy first = strategy("PARALLEL_FIRST", firstInvocations,
                active, peak, false);
        DetectionStrategy flaky = strategy("PARALLEL_FLAKY", flakyInvocations,
                active, peak, true);
        DetectionStrategy third = strategy("PARALLEL_THIRD", thirdInvocations,
                active, peak, false);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        StrategyRepository repository = new DefaultStrategyRepository(
                new InMemoryRahaRepository());
        StrategyExecutionService service = new StrategyExecutionService(
                new StrategyExecutor(new StrategyRegistry(
                        Arrays.asList(first, flaky, third)), clock), repository, clock);
        List<StrategyPlan> plans = Arrays.asList(
                plan("PARALLEL_FIRST"),
                plan("PARALLEL_FLAKY"),
                plan("PARALLEL_THIRD"));
        ArtifactVersion version = new ArtifactVersion(
                "config-v1", "snapshot-v1", "strategy-stage", 1);

        StrategyBatchResult initial = service.execute("job-parallel", "stage-1",
                dataset(), plans, 3000L, version, 2, 5000L);

        assertEquals(1L, initial.getFailedCount());
        assertEquals(2, peak.get());
        assertEquals(1, firstInvocations.get());
        assertEquals(1, flakyInvocations.get());
        assertEquals(1, thirdInvocations.get());
        assertEquals(planIds(plans), executionIds(initial));
        assertEquals(3, repository.findSummaries("job-parallel").size());

        StrategyBatchResult recovered = service.resumeFailed(
                "job-parallel", "stage-2", dataset(), plans, initial,
                3000L, version, 2, 5000L);

        assertEquals(0L, recovered.getFailedCount());
        assertEquals(1, firstInvocations.get());
        assertEquals(2, flakyInvocations.get());
        assertEquals(1, thirdInvocations.get());
        assertEquals(planIds(plans), executionIds(recovered));
        assertEquals(3, repository.findSummaries("job-parallel").size());
        assertTrue(repository.findHits("job-parallel").isEmpty());
    }

    private static DetectionStrategy strategy(String type,
                                               AtomicInteger invocations,
                                               AtomicInteger active,
                                               AtomicInteger peak,
                                               boolean failFirst) {
        return new DetectionStrategy() {
            @Override
            public String getStrategyType() {
                return type;
            }

            @Override
            public StrategyFamily getStrategyFamily() {
                return StrategyFamily.PVD;
            }

            @Override
            public List<StrategyCandidate> detect(StrategyExecutionContext context) {
                int invocation = invocations.incrementAndGet();
                int current = active.incrementAndGet();
                peak.updateAndGet(previous -> Math.max(previous, current));
                try {
                    Thread.sleep(120L);
                    // 不稳定策略仅首次失败，用于验证部分恢复不会重跑已有成功结果。
                    if (failFirst && invocation == 1) {
                        throw new IllegalStateException("模拟首次策略失败");
                    }
                    return Collections.emptyList();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("策略测试线程被中断", exception);
                } finally {
                    active.decrementAndGet();
                }
            }
        };
    }

    private static StrategyPlan plan(String type) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, type);
        List<String> columns = Collections.singletonList("value");
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                StrategyFamily.PVD, columns, configuration), StrategyFamily.PVD,
                columns, configuration, 1,
                com.fiberhome.ml.raha.data.type.StrategyStatus.PLANNED);
    }

    private static RahaDataset dataset() {
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true);
        List<Row> rows = Collections.singletonList(RowFactory.create("1", "value"));
        return new RahaDataset("dataset", "snapshot", "table", "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("value", 1, "string", true, true, false)),
                SparkTestSession.get().createDataFrame(rows, schema),
                "schema-hash", Collections.emptyMap());
    }

    private static List<String> planIds(List<StrategyPlan> plans) {
        List<String> ids = new ArrayList<String>();
        for (StrategyPlan plan : plans) {
            ids.add(plan.getStrategyId());
        }
        return ids;
    }

    private static List<String> executionIds(StrategyBatchResult result) {
        List<String> ids = new ArrayList<String>();
        for (StrategyExecutionResult execution : result.getExecutions()) {
            ids.add(execution.getSummary().getStrategyId());
        }
        return ids;
    }
}
