package com.fiberhome.ml.raha.config.validation;

import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.config.dto.FailureToleranceConfig;
import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证任务配置默认值、错误编码和版本稳定性。
 */
class RahaConfigValidatorTest {

    /** 配置校验器。 */
    private final RahaConfigValidator validator = new RahaConfigValidator();

    @Test
    void shouldAcceptDefaultConfiguration() {
        RahaJobConfig config = RahaJobConfig.defaults(
                JobType.DETECTION, "dataset-1", "table-1", "id");

        assertDoesNotThrow(() -> validator.validate(config));
    }

    @Test
    void shouldReturnStableErrorCodeForMissingDataset() {
        RahaJobConfig config = RahaJobConfig.defaults(
                JobType.DETECTION, " ", "table-1", "id");

        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> validator.validate(config));

        assertEquals(ConfigErrorCode.DATASET_ID_REQUIRED, exception.getErrorCode());
    }

    @Test
    void shouldRejectColumnFilterConflict() {
        Set<String> includedColumns = new LinkedHashSet<String>(Arrays.asList("name", "city"));
        Set<String> excludedColumns = new LinkedHashSet<String>(Collections.singletonList("city"));
        StrategyConfig strategyConfig = new StrategyConfig(
                Collections.singleton(StrategyFamily.PVD), 10, includedColumns, excludedColumns,
                10, 1000L, false);
        RahaJobConfig config = new RahaJobConfig(
                JobType.DETECTION, "dataset-1", null, "table-1", "id",
                false, 1L, strategyConfig, FeatureConfig.defaults(),
                ModelConfig.defaults(), ResourceConfig.defaults(), FailureToleranceConfig.defaults());

        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> validator.validate(config));

        assertEquals(ConfigErrorCode.COLUMN_FILTER_CONFLICT, exception.getErrorCode());
    }

    @Test
    void shouldRejectInvalidModelThreshold() {
        RahaJobConfig config = new RahaJobConfig(
                JobType.TRAINING, "dataset-1", null, "table-1", "id",
                true, 1L, StrategyConfig.defaults(), FeatureConfig.defaults(),
                new ModelConfig(ClassifierType.WEIGHTED_RULE, 1.1d, true),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());

        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> validator.validate(config));

        assertEquals(ConfigErrorCode.MODEL_THRESHOLD_INVALID, exception.getErrorCode());
    }

    @Test
    void shouldRejectInvalidDetectionWeights() {
        Map<StrategyFamily, Double> weights =
                new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        weights.put(StrategyFamily.RVD, 1.1d);
        ModelConfig modelConfig = new ModelConfig(
                ClassifierType.WEIGHTED_RULE, 0.5d, true, weights, 0.2d);
        RahaJobConfig config = new RahaJobConfig(
                JobType.DETECTION, "dataset-1", null, "table-1", "id",
                false, 1L, StrategyConfig.defaults(), FeatureConfig.defaults(),
                modelConfig, ResourceConfig.defaults(), FailureToleranceConfig.defaults());

        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> validator.validate(config));

        assertEquals(ConfigErrorCode.MODEL_THRESHOLD_INVALID, exception.getErrorCode());
    }

    @Test
    void shouldRejectInvalidContextWeight() {
        ModelConfig modelConfig = new ModelConfig(
                ClassifierType.WEIGHTED_RULE, 0.5d, true,
                Collections.<StrategyFamily, Double>emptyMap(), -0.1d);
        RahaJobConfig config = new RahaJobConfig(
                JobType.DETECTION, "dataset-1", null, "table-1", "id",
                false, 1L, StrategyConfig.defaults(), FeatureConfig.defaults(),
                modelConfig, ResourceConfig.defaults(), FailureToleranceConfig.defaults());

        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> validator.validate(config));

        assertEquals(ConfigErrorCode.MODEL_THRESHOLD_INVALID, exception.getErrorCode());
    }

    @Test
    void shouldRejectInvalidClusteringAndSamplingConfiguration() {
        RahaJobConfig invalidClustering = new RahaJobConfig(
                JobType.SAMPLING, "dataset-1", null, "table-1", "id",
                false, 1L, StrategyConfig.defaults(), FeatureConfig.defaults(),
                ModelConfig.defaults(),
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 0, 100),
                SamplingConfig.defaults(), ResourceConfig.defaults(),
                FailureToleranceConfig.defaults());
        RahaJobConfig invalidSampling = new RahaJobConfig(
                JobType.SAMPLING, "dataset-1", null, "table-1", "id",
                false, 1L, StrategyConfig.defaults(), FeatureConfig.defaults(),
                ModelConfig.defaults(), ClusteringConfig.defaults(),
                new SamplingConfig(0, true, false, 1000L),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());

        assertEquals(ConfigErrorCode.CLUSTERING_CONFIG_INVALID,
                assertThrows(ConfigValidationException.class,
                        () -> validator.validate(invalidClustering)).getErrorCode());
        assertEquals(ConfigErrorCode.SAMPLING_CONFIG_INVALID,
                assertThrows(ConfigValidationException.class,
                        () -> validator.validate(invalidSampling)).getErrorCode());
    }

    @Test
    void shouldRejectInvalidCacheThresholdAndStorageLevel() {
        RahaJobConfig invalidThreshold = configWithResource(new ResourceConfig(
                2, 2, 1024L, "MEMORY_ONLY", 0L, 1000L));
        RahaJobConfig invalidStorageLevel = configWithResource(new ResourceConfig(
                2, 2, 1024L, "UNSUPPORTED", 1024L, 1000L));

        assertEquals(ConfigErrorCode.RESOURCE_CONFIG_INVALID,
                assertThrows(ConfigValidationException.class,
                        () -> validator.validate(invalidThreshold)).getErrorCode());
        assertEquals(ConfigErrorCode.RESOURCE_CONFIG_INVALID,
                assertThrows(ConfigValidationException.class,
                        () -> validator.validate(invalidStorageLevel)).getErrorCode());
    }

    @Test
    void shouldRejectStrategyTypeFilterConflict() {
        Set<String> strategyTypes = Collections.singleton("PVD_TYPE_FORMAT");
        StrategyConfig strategyConfig = new StrategyConfig(
                Collections.singleton(StrategyFamily.PVD), 10,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                10, 1000L, false, strategyTypes, strategyTypes,
                Collections.<String, Integer>emptyMap());
        RahaJobConfig config = new RahaJobConfig(
                JobType.DETECTION, "dataset-1", null, "table-1", "id",
                false, 1L, strategyConfig, FeatureConfig.defaults(),
                ModelConfig.defaults(), ResourceConfig.defaults(), FailureToleranceConfig.defaults());

        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> validator.validate(config));

        assertEquals(ConfigErrorCode.STRATEGY_FILTER_INVALID, exception.getErrorCode());
    }

    @Test
    void shouldGenerateStableVersionRegardlessOfSetOrder() {
        Set<StrategyFamily> firstFamilies = new LinkedHashSet<StrategyFamily>(
                Arrays.asList(StrategyFamily.OD, StrategyFamily.PVD, StrategyFamily.RVD));
        Set<StrategyFamily> secondFamilies = new LinkedHashSet<StrategyFamily>(
                Arrays.asList(StrategyFamily.RVD, StrategyFamily.OD, StrategyFamily.PVD));
        Set<String> firstColumns = new LinkedHashSet<String>(Arrays.asList("name", "city"));
        Set<String> secondColumns = new LinkedHashSet<String>(Arrays.asList("city", "name"));
        RahaJobConfig first = createConfig(firstFamilies, firstColumns, 0.5d);
        RahaJobConfig second = createConfig(secondFamilies, secondColumns, 0.5d);
        ConfigVersioner versioner = new ConfigVersioner();

        assertEquals(versioner.versionOf(first), versioner.versionOf(second));
        assertNotEquals(versioner.versionOf(first),
                versioner.versionOf(createConfig(firstFamilies, firstColumns, 0.6d)));
    }

    private static RahaJobConfig createConfig(Set<StrategyFamily> families,
                                               Set<String> columns,
                                               double threshold) {
        StrategyConfig strategyConfig = new StrategyConfig(
                families, 100, columns, Collections.<String>emptySet(), 50, 1000L, false);
        return new RahaJobConfig(
                JobType.DETECTION, "dataset-1", null, "table-1", "id",
                false, 1L, strategyConfig, FeatureConfig.defaults(),
                new ModelConfig(ClassifierType.WEIGHTED_RULE, threshold, true),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());
    }

    private static RahaJobConfig configWithResource(ResourceConfig resourceConfig) {
        return new RahaJobConfig(
                JobType.DETECTION, "dataset-1", null, "table-1", "id",
                false, 1L, StrategyConfig.defaults(), FeatureConfig.defaults(),
                ModelConfig.defaults(), resourceConfig, FailureToleranceConfig.defaults());
    }
}
