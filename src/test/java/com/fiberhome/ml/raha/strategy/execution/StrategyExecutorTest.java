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
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
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
 * 验证单策略异常和超时被转换为独立失败摘要。
 */
class StrategyExecutorTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldIsolateStrategyException() {
        DetectionStrategy failedStrategy = strategy("FAILED_TEST", context -> {
            throw new IllegalStateException("模拟策略失败");
        });
        StrategyExecutionResult result = executor(failedStrategy)
                .execute("job", "stage", dataset(), plan("FAILED_TEST"), 1000L);

        assertEquals(StrategyStatus.FAILED, result.getSummary().getStatus());
        assertEquals("STRATEGY_EXECUTION_FAILED", result.getSummary().getErrorCode());
        assertTrue(result.getHits().isEmpty());
    }

    @Test
    void shouldCancelTimedOutStrategy() {
        DetectionStrategy slowStrategy = strategy("SLOW_TEST", context -> {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            return Collections.emptyList();
        });
        StrategyExecutionResult result = executor(slowStrategy)
                .execute("job", "stage", dataset(), plan("SLOW_TEST"), 20L);

        assertEquals(StrategyStatus.FAILED, result.getSummary().getStatus());
        assertEquals("STRATEGY_TIMEOUT", result.getSummary().getErrorCode());
        assertTrue(result.getHits().isEmpty());
    }

    @Test
    void shouldPersistSuccessfulStrategyWhenAnotherStrategyFails() {
        DetectionStrategy failedStrategy = strategy("FAILED_TEST", context -> {
            throw new IllegalStateException("模拟策略失败");
        });
        DetectionStrategy successfulStrategy = strategy("SUCCESS_TEST", context ->
                Collections.singletonList(new StrategyCandidate(
                        "1", "value", HashUtils.md5Hex("value"),
                        "TEST_CANDIDATE", Collections.singletonMap("source", "test"), 1.0d)));
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        StrategyRepository repository = new DefaultStrategyRepository(storage);
        StrategyExecutionService service = new StrategyExecutionService(
                new StrategyExecutor(new StrategyRegistry(
                        Arrays.asList(failedStrategy, successfulStrategy)), clock),
                repository, clock);

        StrategyBatchResult result = service.execute("job", "stage", dataset(),
                Arrays.asList(plan("FAILED_TEST"), plan("SUCCESS_TEST")), 1000L,
                new ArtifactVersion("config", "snapshot", "stage", 1));

        assertEquals(1L, result.getFailedCount());
        assertEquals(1, result.getHits().size());
        assertEquals(2, repository.findSummaries("job").size());
        assertEquals(1, repository.findHits("job").size());
    }

    private static StrategyExecutor executor(DetectionStrategy strategy) {
        return new StrategyExecutor(new StrategyRegistry(Collections.singletonList(strategy)),
                Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC));
    }

    private static DetectionStrategy strategy(String type, StrategyBody body) {
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
                return body.detect(context);
            }
        };
    }

    private static StrategyPlan plan(String type) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, type);
        List<String> columns = Collections.singletonList("value");
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                StrategyFamily.PVD, columns, configuration), StrategyFamily.PVD,
                columns, configuration, 1, StrategyStatus.PLANNED);
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

    private interface StrategyBody {
        List<StrategyCandidate> detect(StrategyExecutionContext context);
    }
}
