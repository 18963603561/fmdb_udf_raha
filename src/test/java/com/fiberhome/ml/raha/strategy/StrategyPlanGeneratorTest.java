package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.config.StrategyConfig;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.util.HashUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证策略计划的稳定标识、适用性、过滤、优先级和数量上限。
 */
class StrategyPlanGeneratorTest {

    @Test
    void shouldGenerateStablePlansAndConfigurationHashes() {
        StrategyPlanGenerator generator = new StrategyPlanGenerator();
        RahaDataset firstDataset = dataset(false);
        RahaDataset secondDataset = dataset(true);

        List<StrategyPlan> first = generator.generate(firstDataset, StrategyConfig.defaults());
        List<StrategyPlan> second = generator.generate(secondDataset, StrategyConfig.defaults());

        assertEquals(first.size(), second.size());
        for (int index = 0; index < first.size(); index++) {
            assertEquals(first.get(index).getStrategyId(), second.get(index).getStrategyId());
            assertEquals(first.get(index).getConfigurationHash(),
                    second.get(index).getConfigurationHash());
            assertEquals(64, first.get(index).getStrategyId().length());
            assertEquals(64, first.get(index).getConfigurationHash().length());
        }
        assertTrue(types(first).contains(StrategyTypes.OD_LOW_FREQUENCY));
        assertTrue(types(first).contains(StrategyTypes.OD_NUMERIC_DISTANCE));
        assertTrue(types(first).contains(StrategyTypes.OD_QUANTILE));
        assertTrue(types(first).contains(StrategyTypes.PVD_CHARACTER_SET));
        assertTrue(types(first).contains(StrategyTypes.PVD_NULL_PLACEHOLDER));
        assertTrue(types(first).contains(StrategyTypes.PVD_TYPE_FORMAT));
    }

    @Test
    void shouldApplyStrategyTypeFilterPriorityAndLimit() {
        Map<String, Integer> priorities = new LinkedHashMap<String, Integer>();
        priorities.put(StrategyTypes.OD_LOW_FREQUENCY, 20);
        priorities.put(StrategyTypes.PVD_NULL_PLACEHOLDER, 1);
        StrategyConfig config = new StrategyConfig(
                EnumSet.of(StrategyFamily.OD, StrategyFamily.PVD), 1,
                Collections.singleton("amount"), Collections.<String>emptySet(),
                10, 1000L, false,
                new LinkedHashSet<String>(Arrays.asList(
                        StrategyTypes.OD_LOW_FREQUENCY,
                        StrategyTypes.PVD_NULL_PLACEHOLDER)),
                Collections.<String>emptySet(), priorities);

        List<StrategyPlan> plans = new StrategyPlanGenerator().generate(dataset(false), config);

        assertEquals(1, plans.size());
        assertEquals(StrategyTypes.PVD_NULL_PLACEHOLDER,
                plans.get(0).getConfiguration().get(StrategyConfigurationKeys.STRATEGY_TYPE));
        assertEquals(1, plans.get(0).getPriority());
    }

    @Test
    void shouldExcludeBlacklistedStrategyType() {
        StrategyConfig config = new StrategyConfig(
                EnumSet.of(StrategyFamily.OD, StrategyFamily.PVD), 20,
                Collections.singleton("amount"), Collections.<String>emptySet(),
                10, 1000L, false, Collections.<String>emptySet(),
                Collections.singleton(StrategyTypes.PVD_NULL_PLACEHOLDER),
                Collections.<String, Integer>emptyMap());

        List<StrategyPlan> plans = new StrategyPlanGenerator().generate(dataset(false), config);

        assertTrue(!types(plans).contains(StrategyTypes.PVD_NULL_PLACEHOLDER));
    }

    @Test
    void shouldNotGeneratePvdPlanWhenOnlyOdIsEnabledForAllNullColumn() {
        StrategyConfig config = new StrategyConfig(
                Collections.singleton(StrategyFamily.OD), 20,
                Collections.singleton("name"), Collections.<String>emptySet(),
                10, 1000L, false);
        ColumnProfile allNull = new ColumnProfile("name", 10L, 10L, 0L,
                -1, -1, 0.0d, Collections.singletonMap("NULL", 10L));
        RahaDataset dataset = new RahaDataset("dataset", "snapshot", "table", "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("name", 1, "string", true, true, false)),
                null, "schema-hash", Collections.singletonMap("name", allNull));

        assertTrue(new StrategyPlanGenerator().generate(dataset, config).isEmpty());
    }

    @Test
    void shouldApplyInjectedGenerationThresholdsAndPriorities() {
        StrategyGenerationConfig generationConfig = new StrategyGenerationConfig(
                0.2d, 3, 4.5d, 4, 2.5d, 0.2d, "AUTO", 0.9d,
                "NULL,N/A", 11, 12, 13, 21, 22, 23, 24, 31);

        List<StrategyPlan> plans = new StrategyPlanGenerator(generationConfig)
                .generate(dataset(false), StrategyConfig.defaults());
        Map<String, StrategyPlan> byType = plansByType(plans);

        assertEquals("20", byType.get(StrategyTypes.OD_LOW_FREQUENCY)
                .getConfiguration().get(StrategyConfigurationKeys.MAX_FREQUENCY));
        assertEquals("4.5", byType.get(StrategyTypes.OD_NUMERIC_DISTANCE)
                .getConfiguration().get(StrategyConfigurationKeys.Z_THRESHOLD));
        assertEquals("2.5", byType.get(StrategyTypes.OD_QUANTILE)
                .getConfiguration().get(StrategyConfigurationKeys.IQR_MULTIPLIER));
        assertEquals("0.2", byType.get(StrategyTypes.PVD_CHARACTER_SET)
                .getConfiguration().get(StrategyConfigurationKeys.MINORITY_RATIO));
        assertEquals("NULL,N/A", byType.get(StrategyTypes.PVD_NULL_PLACEHOLDER)
                .getConfiguration().get(StrategyConfigurationKeys.PLACEHOLDERS));
        assertEquals(11, byType.get(StrategyTypes.OD_LOW_FREQUENCY).getPriority());
        assertEquals(24, byType.get(StrategyTypes.PVD_TYPE_FORMAT).getPriority());
    }

    private static RahaDataset dataset(boolean reverseProfiles) {
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("id", 0, "string", false, false, false),
                new ColumnMetadata("amount", 1, "string", true, true, false),
                new ColumnMetadata("name", 2, "string", true, true, false));
        Map<String, ColumnProfile> profiles = new LinkedHashMap<String, ColumnProfile>();
        if (reverseProfiles) {
            profiles.put("name", textProfile());
            profiles.put("amount", numericProfile());
        } else {
            profiles.put("amount", numericProfile());
            profiles.put("name", textProfile());
        }
        return new RahaDataset("dataset", "snapshot", "table", "id",
                columns, null, "schema-hash", profiles);
    }

    private static ColumnProfile numericProfile() {
        Map<String, Long> typeCounts = new LinkedHashMap<String, Long>();
        typeCounts.put("INTEGER", 100L);
        Map<String, Long> frequencies = new LinkedHashMap<String, Long>();
        frequencies.put(HashUtils.sha256Hex("10"), 50L);
        frequencies.put(HashUtils.sha256Hex("20"), 50L);
        return new ColumnProfile("amount", 100L, 0L, 0L, 2L,
                2, 2, 2.0d, 100L, 1.0d,
                10.0d, 20.0d, 15.0d, 5.0d,
                10.0d, 15.0d, 20.0d, typeCounts, frequencies);
    }

    private static ColumnProfile textProfile() {
        return new ColumnProfile("name", 100L, 0L, 0L, 10L,
                2, 12, 5.0d, 0L, 0.0d,
                null, null, null, null, null, null, null,
                Collections.singletonMap("LETTER", 100L),
                Collections.singletonMap(HashUtils.sha256Hex("Alice"), 10L));
    }

    private static java.util.Set<String> types(List<StrategyPlan> plans) {
        java.util.Set<String> types = new LinkedHashSet<String>();
        for (StrategyPlan plan : plans) {
            types.add(plan.getConfiguration().get(StrategyConfigurationKeys.STRATEGY_TYPE));
        }
        return types;
    }

    private static Map<String, StrategyPlan> plansByType(List<StrategyPlan> plans) {
        Map<String, StrategyPlan> result = new LinkedHashMap<String, StrategyPlan>();
        for (StrategyPlan plan : plans) {
            result.put(plan.getConfiguration().get(
                    StrategyConfigurationKeys.STRATEGY_TYPE), plan);
        }
        return result;
    }
}
