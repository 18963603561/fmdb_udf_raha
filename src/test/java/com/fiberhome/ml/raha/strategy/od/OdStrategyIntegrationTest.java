package com.fiberhome.ml.raha.strategy.od;

import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.data.StrategyStatus;
import com.fiberhome.ml.raha.strategy.StrategyCandidate;
import com.fiberhome.ml.raha.strategy.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.StrategyExecutionContext;
import com.fiberhome.ml.raha.strategy.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyTypes;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

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
 * 使用 Spark 本地模式验证 OD 正常、离群、空值和不可解析边界。
 */
class OdStrategyIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldDetectLowFrequencyCandidatesWithoutNullValues() {
        StrategyPlan plan = plan(StrategyTypes.OD_LOW_FREQUENCY,
                StrategyConfigurationKeys.MAX_FREQUENCY, "1");

        List<StrategyCandidate> candidates = new LowFrequencyStrategy().detect(context(plan));

        assertTrue(rowIds(candidates).contains("10"));
        assertTrue(rowIds(candidates).contains("11"));
        assertFalse(rowIds(candidates).contains("12"));
        assertTrue(reasons(candidates).contains("OD_LOW_FREQUENCY"));
    }

    @Test
    void shouldDetectNumericDistanceAndIgnoreUnparseableValues() {
        Map<String, String> configuration = base(StrategyTypes.OD_NUMERIC_DISTANCE);
        configuration.put(StrategyConfigurationKeys.NUMERIC_MEAN, "12");
        configuration.put(StrategyConfigurationKeys.NUMERIC_STANDARD_DEVIATION, "2");
        configuration.put(StrategyConfigurationKeys.Z_THRESHOLD, "2");

        List<StrategyCandidate> candidates = new NumericDistanceStrategy()
                .detect(context(plan(configuration)));

        assertEquals(Collections.singletonList("10"), rowIds(candidates));
        assertEquals("OD_NUMERIC_DISTANCE", candidates.get(0).getReasonCode());
    }

    @Test
    void shouldDetectQuantileOutlierForLongTailValues() {
        Map<String, String> configuration = base(StrategyTypes.OD_QUANTILE);
        configuration.put(StrategyConfigurationKeys.NUMERIC_Q1, "10");
        configuration.put(StrategyConfigurationKeys.NUMERIC_Q3, "14");
        configuration.put(StrategyConfigurationKeys.IQR_MULTIPLIER, "1.5");

        List<StrategyCandidate> candidates = new QuantileOutlierStrategy()
                .detect(context(plan(configuration)));

        assertEquals(Collections.singletonList("10"), rowIds(candidates));
        assertEquals("OD_QUANTILE_OUTLIER", candidates.get(0).getReasonCode());
    }

    private static StrategyExecutionContext context(StrategyPlan plan) {
        return new StrategyExecutionContext("job", "stage", dataset(), plan);
    }

    private static StrategyPlan plan(String strategyType, String key, String value) {
        Map<String, String> configuration = base(strategyType);
        configuration.put(key, value);
        return plan(configuration);
    }

    private static StrategyPlan plan(Map<String, String> configuration) {
        List<String> columns = Collections.singletonList("amount");
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                StrategyFamily.OD, columns, configuration), StrategyFamily.OD,
                columns, configuration, 1, StrategyStatus.PLANNED);
    }

    private static Map<String, String> base(String strategyType) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, strategyType);
        return configuration;
    }

    private static RahaDataset dataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "10"), RowFactory.create("2", "10"),
                RowFactory.create("3", "11"), RowFactory.create("4", "11"),
                RowFactory.create("5", "12"), RowFactory.create("6", "12"),
                RowFactory.create("7", "13"), RowFactory.create("8", "13"),
                RowFactory.create("9", "14"), RowFactory.create("10", "1000"),
                RowFactory.create("11", "bad"), RowFactory.create("12", null));
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("amount", DataTypes.StringType, true);
        Dataset<Row> dataFrame = SparkTestSession.get().createDataFrame(rows, schema);
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("id", 0, "string", false, false, false),
                new ColumnMetadata("amount", 1, "string", true, true, false));
        return new RahaDataset("dataset", "snapshot", "table", "id",
                columns, dataFrame, "schema-hash", Collections.emptyMap());
    }

    private static List<String> rowIds(List<StrategyCandidate> candidates) {
        List<String> ids = new ArrayList<String>();
        for (StrategyCandidate candidate : candidates) {
            ids.add(candidate.getRowId());
        }
        Collections.sort(ids);
        return ids;
    }

    private static List<String> reasons(List<StrategyCandidate> candidates) {
        List<String> reasons = new ArrayList<String>();
        for (StrategyCandidate candidate : candidates) {
            reasons.add(candidate.getReasonCode());
        }
        return reasons;
    }
}
