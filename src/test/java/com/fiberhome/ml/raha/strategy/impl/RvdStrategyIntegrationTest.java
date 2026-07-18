package com.fiberhome.ml.raha.strategy.impl;

import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionContext;
import com.fiberhome.ml.raha.strategy.plan.StrategyCandidate;
import com.fiberhome.ml.raha.strategy.plan.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanGenerator;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 RVD 列对上限、唯一映射、冲突定位和空值边界。
 */
class RvdStrategyIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldLimitDirectedColumnPairs() {
        StrategyConfig config = new StrategyConfig(
                EnumSet.of(StrategyFamily.RVD), 100,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                2, 1000L, false);

        List<StrategyPlan> plans = new StrategyPlanGenerator().generate(profiledDataset(), config);

        assertEquals(2, plans.size());
        assertTrue(plans.stream().allMatch(plan -> plan.getStrategyFamily() == StrategyFamily.RVD));
        assertTrue(plans.stream().allMatch(plan -> plan.getTargetColumns().size() == 2));
    }

    @Test
    void shouldLocateBothSidesOfOneToManyConflict() {
        List<StrategyCandidate> candidates = new OneToManyConflictStrategy()
                .detect(new StrategyExecutionContext("job", "stage", conflictDataset(), plan()));

        assertEquals(4, candidates.size());
        assertEquals(Arrays.asList("3:city", "3:code", "4:city", "4:code"),
                coordinates(candidates));
        assertTrue(candidates.stream().allMatch(candidate ->
                "code->city".equals(candidate.getReasonDetails().get("dependency"))));
        assertTrue(candidates.stream().allMatch(candidate ->
                "2".equals(candidate.getReasonDetails().get("distinctRightCount"))));
        assertTrue(candidates.stream().allMatch(candidate -> candidate.getScore() == 0.5d));
    }

    @Test
    void shouldIgnoreUniqueMappingsAndNullValues() {
        List<StrategyCandidate> candidates = new OneToManyConflictStrategy()
                .detect(new StrategyExecutionContext("job", "stage", uniqueDataset(), plan()));

        assertTrue(candidates.isEmpty());
    }

    private static StrategyPlan plan() {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, StrategyTypes.RVD_ONE_TO_MANY);
        configuration.put(StrategyConfigurationKeys.LEFT_COLUMN, "code");
        configuration.put(StrategyConfigurationKeys.RIGHT_COLUMN, "city");
        List<String> columns = Arrays.asList("code", "city");
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                StrategyFamily.RVD, columns, configuration), StrategyFamily.RVD,
                columns, configuration, 1, StrategyStatus.PLANNED);
    }

    private static RahaDataset conflictDataset() {
        return dataset(Arrays.asList(
                RowFactory.create("1", "A", "x"),
                RowFactory.create("2", "A", "x"),
                RowFactory.create("3", "B", "y"),
                RowFactory.create("4", "B", "z"),
                RowFactory.create("5", null, "q"),
                RowFactory.create("6", "C", null)));
    }

    private static RahaDataset uniqueDataset() {
        return dataset(Arrays.asList(
                RowFactory.create("1", "A", "x"),
                RowFactory.create("2", "A", "x"),
                RowFactory.create("3", "B", "y"),
                RowFactory.create("4", null, "z"),
                RowFactory.create("5", "C", null)));
    }

    private static RahaDataset dataset(List<Row> rows) {
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true)
                .add("city", DataTypes.StringType, true);
        Dataset<Row> dataFrame = SparkTestSession.get().createDataFrame(rows, schema);
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("id", 0, "string", false, false, false),
                new ColumnMetadata("code", 1, "string", true, true, false),
                new ColumnMetadata("city", 2, "string", true, true, false));
        return new RahaDataset("dataset", "snapshot", "table", "id",
                columns, dataFrame, "schema-hash", Collections.emptyMap());
    }

    private static RahaDataset profiledDataset() {
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("id", 0, "string", false, false, false),
                new ColumnMetadata("a", 1, "string", true, true, false),
                new ColumnMetadata("b", 2, "string", true, true, false),
                new ColumnMetadata("c", 3, "string", true, true, false),
                new ColumnMetadata("nested", 4, "struct<x:string>", true, true, false));
        Map<String, ColumnProfile> profiles = new LinkedHashMap<String, ColumnProfile>();
        profiles.put("a", profile("a"));
        profiles.put("b", profile("b"));
        profiles.put("c", profile("c"));
        profiles.put("nested", profile("nested"));
        return new RahaDataset("dataset", "snapshot", "table", "id",
                columns, null, "schema-hash", profiles);
    }

    private static ColumnProfile profile(String columnName) {
        return new ColumnProfile(columnName, 10L, 0L, 5L,
                1, 5, 0.0d, Collections.singletonMap("LETTER", 10L));
    }

    private static List<String> coordinates(List<StrategyCandidate> candidates) {
        List<String> coordinates = new ArrayList<String>();
        for (StrategyCandidate candidate : candidates) {
            coordinates.add(candidate.getRowId() + ":" + candidate.getColumnName());
        }
        Collections.sort(coordinates);
        return coordinates;
    }
}
