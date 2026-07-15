package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.config.FeatureConfig;
import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.data.StrategyStatus;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.FeatureRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.strategy.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.StrategyHit;
import com.fiberhome.ml.raha.strategy.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyTypes;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证策略二值特征、上下文特征、常量过滤、字典版本和稀疏持久化。
 */
class FeatureAssemblerIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldAssembleStableSparseFeaturesAndProtectSensitiveValues() {
        RahaDataset dataset = dataset();
        List<StrategyPlan> plans = plans();
        List<StrategyHit> hits = hits(plans);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        FeatureAssembler assembler = new FeatureAssembler(new FeatureDictionaryVersioner(), clock);

        FeatureAssemblyResult first = assembler.assemble(
                dataset, plans, hits, FeatureConfig.defaults());
        FeatureAssemblyResult second = assembler.assemble(
                dataset, plans, hits, FeatureConfig.defaults());

        assertEquals(8, first.getRows().size());
        assertEquals(2, first.getDictionaries().size());
        assertEquals(first.getDictionaries().get("code").getVersion(),
                second.getDictionaries().get("code").getVersion());
        assertTrue(first.getMetrics().getRemovedConstantFeatureCount() > 0L);
        assertDictionarySources(first.getDictionaries().get("code"), plans);

        SparseFeatureRow codeHit = find(first, "3", "code");
        SparseFeatureRow secretHit = find(first, "3", "secret");
        assertFalse(codeHit.getValues().isEmpty());
        assertEquals("1*********3", secretHit.getMaskedValue());
        assertFalse(secretHit.getMaskedValue().contains("13800000003"));
        assertEquals(HashUtils.sha256Hex("13800000003"), secretHit.getValueHash());

        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        FeatureRepository repository = new DefaultFeatureRepository(storage);
        repository.save("job", first,
                new ArtifactVersion("config", "snapshot", "feature-stage", 1), 1L);
        assertEquals(4, repository.findRows("job", "code").size());
        assertEquals(first.getDictionaries().get("code").getVersion(),
                repository.findDictionary("job", "code").get().getVersion());
    }

    @Test
    void shouldRetainAllCellsWhenHomogeneousColumnHasNoVariableFeatures() {
        RahaDataset dataset = homogeneousDataset();
        FeatureAssembler assembler = new FeatureAssembler(
                new FeatureDictionaryVersioner(), Clock.systemUTC());

        FeatureAssemblyResult result = assembler.assemble(dataset,
                Collections.<StrategyPlan>emptyList(),
                Collections.<StrategyHit>emptyList(), FeatureConfig.defaults());

        assertEquals(2, result.getRows().size());
        assertTrue(result.getDictionaries().get("code").getDefinitions().isEmpty());
        assertTrue(result.getRows().stream().allMatch(row -> row.getValues().isEmpty()));
        assertNotNull(find(result, "1", "code"));
        assertNotNull(find(result, "2", "code"));
    }

    private static void assertDictionarySources(FeatureDictionary dictionary,
                                                List<StrategyPlan> plans) {
        List<String> sources = new ArrayList<String>();
        for (FeatureDefinition definition : dictionary.getDefinitions().values()) {
            sources.add(definition.getSource());
        }
        assertTrue(sources.contains(plans.get(0).getStrategyId()));
        assertTrue(sources.contains(plans.get(2).getStrategyId()));
        assertFalse(sources.contains(plans.get(1).getStrategyId()));
    }

    private static SparseFeatureRow find(FeatureAssemblyResult result,
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

    private static List<StrategyPlan> plans() {
        StrategyPlan pvd = plan(StrategyFamily.PVD,
                Collections.singletonList("code"), StrategyTypes.PVD_CHARACTER_SET,
                Collections.singletonMap(StrategyConfigurationKeys.MINORITY_RATIO, "0.1"), 1);
        StrategyPlan noHit = plan(StrategyFamily.OD,
                Collections.singletonList("code"), StrategyTypes.OD_LOW_FREQUENCY,
                Collections.singletonMap(StrategyConfigurationKeys.MAX_FREQUENCY, "1"), 2);
        Map<String, String> rvdConfig = new LinkedHashMap<String, String>();
        rvdConfig.put(StrategyConfigurationKeys.LEFT_COLUMN, "code");
        rvdConfig.put(StrategyConfigurationKeys.RIGHT_COLUMN, "secret");
        StrategyPlan rvd = plan(StrategyFamily.RVD,
                Arrays.asList("code", "secret"), StrategyTypes.RVD_ONE_TO_MANY, rvdConfig, 3);
        return Arrays.asList(pvd, noHit, rvd);
    }

    private static StrategyPlan plan(StrategyFamily family,
                                     List<String> columns,
                                     String type,
                                     Map<String, String> extra,
                                     int priority) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, type);
        configuration.putAll(extra);
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                family, columns, configuration), family, columns,
                configuration, priority, StrategyStatus.PLANNED);
    }

    private static List<StrategyHit> hits(List<StrategyPlan> plans) {
        CellCoordinate code = new CellCoordinate("dataset", "snapshot", "3", "code");
        CellCoordinate secret = new CellCoordinate("dataset", "snapshot", "3", "secret");
        return Arrays.asList(
                hit(plans.get(0), code, "BAD#", "PVD_MINOR_CHARACTER_SET", 1.0d),
                hit(plans.get(2), code, "BAD#", "RVD_ONE_TO_MANY_CONFLICT", 0.5d),
                hit(plans.get(2), secret, "13800000003", "RVD_ONE_TO_MANY_CONFLICT", 0.5d));
    }

    private static StrategyHit hit(StrategyPlan plan,
                                   CellCoordinate coordinate,
                                   String value,
                                   String reason,
                                   double score) {
        return new StrategyHit("job", "stage", plan.getStrategyId(),
                plan.getStrategyFamily(), coordinate, HashUtils.sha256Hex(value), reason,
                Collections.singletonMap("test", "true"), score, 1L,
                StrategyStatus.SUCCEEDED);
    }

    private static RahaDataset dataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "ABC", "13800000001"),
                RowFactory.create("2", "ABC", "13800000002"),
                RowFactory.create("3", "BAD#", "13800000003"),
                RowFactory.create("4", "ABC", "13800000004"));
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true)
                .add("secret", DataTypes.StringType, true);
        Dataset<Row> dataFrame = SparkTestSession.get().createDataFrame(rows, schema);
        return new RahaDataset("dataset", "snapshot", "table", "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("code", 1, "string", true, true, false),
                        new ColumnMetadata("secret", 2, "string", true, true, true)),
                dataFrame, "schema-hash", Collections.emptyMap());
    }

    private static RahaDataset homogeneousDataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "ABC"),
                RowFactory.create("2", "ABC"));
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true);
        Dataset<Row> dataFrame = SparkTestSession.get().createDataFrame(rows, schema);
        return new RahaDataset("dataset", "snapshot", "table", "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("code", 1, "string", true, true, false)),
                dataFrame, "schema-hash", Collections.emptyMap());
    }
}
