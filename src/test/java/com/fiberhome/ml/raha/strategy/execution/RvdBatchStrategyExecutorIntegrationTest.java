package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.plan.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 验证 RVD 批量执行与现有单策略实现保持坐标和原因语义一致。
 */
class RvdBatchStrategyExecutorIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldMatchSingleStrategyResultsForAllDirectedPairs() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        RahaDataset dataset = dataset();
        List<StrategyPlan> plans = Arrays.asList(
                plan("code", "city"), plan("city", "code"),
                plan("code", "region"), plan("region", "city"));
        StrategyExecutor singleExecutor = new StrategyExecutor(
                StrategyRegistry.defaults(), clock);
        List<StrategyExecutionResult> expected = new ArrayList<StrategyExecutionResult>();
        for (StrategyPlan plan : plans) {
            expected.add(singleExecutor.execute("job", "stage", dataset, plan, 30000L));
        }

        // 使用两条计划的单批上限，验证跨批执行不会改变策略语义。
        List<StrategyExecutionResult> actual = new RvdBatchStrategyExecutor(clock, 2)
                .execute("job", "stage", dataset, plans, 30000L);

        assertEquals(signatures(expected), signatures(actual));
        assertEquals(strategyIds(plans), resultStrategyIds(actual));
    }

    private static RahaDataset dataset() {
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true)
                .add("city", DataTypes.StringType, true)
                .add("region", DataTypes.StringType, true);
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "A", "x", "r1"),
                RowFactory.create("2", "A", "x", "r1"),
                RowFactory.create("3", "B", "y", "r2"),
                RowFactory.create("4", "B", "z", "r2"),
                RowFactory.create("5", "C", "z", "r3"),
                RowFactory.create("6", null, "q", "r3"),
                RowFactory.create("7", "D", null, "r4"),
                RowFactory.create("8", " ", "p", "r4"));
        Dataset<Row> frame = SparkTestSession.get().createDataFrame(rows, schema);
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("id", 0, "string", false, false, false),
                new ColumnMetadata("code", 1, "string", true, true, false),
                new ColumnMetadata("city", 2, "string", true, true, false),
                new ColumnMetadata("region", 3, "string", true, true, false));
        return new RahaDataset("dataset", "snapshot", "table", "id",
                columns, frame, "schema-hash", Collections.emptyMap());
    }

    private static StrategyPlan plan(String left, String right) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE,
                StrategyTypes.RVD_ONE_TO_MANY);
        configuration.put(StrategyConfigurationKeys.LEFT_COLUMN, left);
        configuration.put(StrategyConfigurationKeys.RIGHT_COLUMN, right);
        List<String> columns = Arrays.asList(left, right);
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                StrategyFamily.RVD, columns, configuration), StrategyFamily.RVD,
                columns, configuration, 1, StrategyStatus.PLANNED);
    }

    private static List<String> signatures(List<StrategyExecutionResult> results) {
        List<String> values = new ArrayList<String>();
        for (StrategyExecutionResult result : results) {
            assertEquals(StrategyStatus.SUCCEEDED, result.getSummary().getStatus());
            for (StrategyHit hit : result.getHits()) {
                values.add(hit.getStrategyId() + "|" + hit.getCoordinate().toCellId()
                        + "|" + hit.getValueHash() + "|" + hit.getReasonCode()
                        + "|" + hit.getReasonDetails() + "|" + hit.getStrategyScore());
            }
        }
        Collections.sort(values, Comparator.naturalOrder());
        return values;
    }

    private static List<String> strategyIds(List<StrategyPlan> plans) {
        List<String> values = new ArrayList<String>();
        for (StrategyPlan plan : plans) {
            values.add(plan.getStrategyId());
        }
        return values;
    }

    private static List<String> resultStrategyIds(List<StrategyExecutionResult> results) {
        List<String> values = new ArrayList<String>();
        for (StrategyExecutionResult result : results) {
            values.add(result.getSummary().getStrategyId());
        }
        return values;
    }
}
