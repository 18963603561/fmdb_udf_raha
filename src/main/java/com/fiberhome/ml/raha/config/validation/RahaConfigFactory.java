package com.fiberhome.ml.raha.config.validation;

import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.config.core.ConfigTextUtils;
import com.fiberhome.ml.raha.config.core.RahaConfigurationException;
import com.fiberhome.ml.raha.config.core.RahaProperties;
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
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.strategy.plan.StrategyGenerationConfig;
import com.fiberhome.ml.raha.util.HashUtils;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 将统一属性转换为任务和算法配置对象。
 */
public final class RahaConfigFactory {

    /** 合并覆盖后的不可变属性。 */
    private final RahaProperties properties;

    public RahaConfigFactory(RahaProperties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("Raha 配置工厂属性不能为空");
        }
        this.properties = properties;
    }

    public RahaJobConfig jobConfig(JobType jobType,
                                   String datasetId,
                                   String inputReference,
                                   String rowIdColumn) {
        return new RahaJobConfig(jobType, datasetId, null, inputReference,
                rowIdColumn, properties.getBoolean("raha.job.save-intermediate"),
                properties.getLong("raha.job.random-seed"),
                strategyConfig(), featureConfig(), modelConfig(),
                clusteringConfig(), samplingConfig(), resourceConfig(),
                failureToleranceConfig(), executionFingerprint());
    }

    public StrategyConfig strategyConfig() {
        return new StrategyConfig(enumSet("raha.strategy.families",
                StrategyFamily.class), properties.getInt("raha.strategy.max-count"),
                properties.getCsvSet("raha.strategy.included-columns"),
                properties.getCsvSet("raha.strategy.excluded-columns"),
                properties.getInt("raha.strategy.max-rvd-column-pairs"),
                properties.getLong("raha.strategy.timeout-millis"),
                properties.getBoolean("raha.strategy.filtering-enabled"),
                properties.getCsvSet("raha.strategy.included-types"),
                properties.getCsvSet("raha.strategy.excluded-types"),
                integerMap("raha.strategy.priorities"));
    }

    public FeatureConfig featureConfig() {
        return new FeatureConfig(
                properties.getBoolean("raha.feature.trim-value"),
                properties.getBoolean("raha.feature.lower-case-value"),
                properties.getBoolean("raha.feature.normalize-width"),
                properties.getBoolean("raha.feature.context-enabled"),
                properties.getBoolean("raha.feature.remove-constant"),
                properties.getInt("raha.feature.max-count"),
                properties.getDouble("raha.feature.rare-value-ratio"));
    }

    public ModelConfig modelConfig() {
        return new ModelConfig(
                properties.getEnum("raha.model.classifier-type", ClassifierType.class),
                properties.getDouble("raha.model.threshold"),
                properties.getBoolean("raha.model.fallback-enabled"),
                modelStrategyFamilyWeights(),
                properties.getDouble("raha.model.context-weight"),
                modelDefaultStrategyFamilyWeight(),
                modelRareContextSignalWeight(),
                modelRvdConflictContextSignalWeight(),
                modelNullContextSignalWeight(),
                modelBlankContextSignalWeight(),
                modelMixedContextSignalWeight(),
                properties.getBoolean("raha.model.quality-gate.enabled"),
                properties.getDouble("raha.model.quality-gate.minimum-score-stddev"),
                properties.getDouble("raha.model.quality-gate.maximum-positive-ratio"),
                properties.getDouble("raha.model.quality-gate.minimum-f1"));
    }

    public Map<StrategyFamily, Double> modelStrategyFamilyWeights() {
        Map<String, String> configured = properties.getStringMap(
                "raha.model.strategy-family-weights");
        EnumMap<StrategyFamily, Double> result =
                new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        for (Map.Entry<String, String> entry : configured.entrySet()) {
            StrategyFamily family = parseEnum(
                    "raha.model.strategy-family-weights", entry.getKey(),
                    StrategyFamily.class);
            result.put(family, parseDouble(
                    "raha.model.strategy-family-weights", entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    public ClusteringConfig clusteringConfig() {
        return new ClusteringConfig(properties.getEnum(
                "raha.clustering.distance-metric", ClusteringDistanceMetric.class),
                properties.getInt("raha.clustering.target-cluster-count"),
                properties.getInt("raha.clustering.max-sample-count"));
    }

    public SamplingConfig samplingConfig() {
        return new SamplingConfig(
                properties.getInt("raha.sampling.labeling-budget"),
                properties.getBoolean("raha.sampling.clustering-based"),
                properties.getBoolean("raha.sampling.review-enabled"),
                properties.getLong("raha.sampling.task-ttl-millis"),
                samplingCoverageScoreExponentCap());
    }

    public ResourceConfig resourceConfig() {
        return new ResourceConfig(
                properties.getInt("raha.resource.max-parallel-strategies"),
                properties.getInt("raha.resource.max-parallel-columns"),
                properties.getLong("raha.resource.broadcast-threshold-bytes"),
                properties.getRequired("raha.resource.cache-storage-level"),
                properties.getLong("raha.resource.cache-threshold-bytes"),
                properties.getLong("raha.resource.stage-timeout-millis"));
    }

    public FailureToleranceConfig failureToleranceConfig() {
        return new FailureToleranceConfig(
                properties.getBoolean("raha.failure.fail-fast"),
                properties.getDouble("raha.failure.max-failed-strategy-ratio"),
                properties.getInt("raha.failure.max-retry-count"));
    }

    public LabelPropagationConfig labelPropagationConfig() {
        return new LabelPropagationConfig(
                properties.getDouble("raha.label.propagated-weight"),
                properties.getDouble("raha.label.minimum-majority-ratio"),
                labelMaxDirectWeightRatio());
    }

    public LogisticRegressionTrainingConfig logisticRegressionTrainingConfig() {
        return new LogisticRegressionTrainingConfig(
                properties.getBoolean("raha.model.logistic.class-balance-enabled"),
                properties.getInt("raha.model.logistic.max-iterations"),
                properties.getDouble("raha.model.logistic.regularization"),
                properties.getDouble("raha.model.logistic.elastic-net"));
    }

    public StrategyGenerationConfig strategyGenerationConfig() {
        return new StrategyGenerationConfig(
                properties.getDouble("raha.strategy.od.low-frequency-ratio"),
                properties.getInt("raha.strategy.od.minimum-numeric-count"),
                properties.getDouble("raha.strategy.od.z-threshold"),
                properties.getInt("raha.strategy.od.minimum-quantile-count"),
                properties.getDouble("raha.strategy.od.iqr-multiplier"),
                properties.getDouble("raha.strategy.pvd.minority-ratio"),
                properties.getRequired("raha.strategy.pvd.format-type"),
                properties.getDouble("raha.strategy.pvd.format-min-ratio"),
                placeholders(),
                properties.getInt("raha.strategy.priority.od-low-frequency"),
                properties.getInt("raha.strategy.priority.od-numeric-distance"),
                properties.getInt("raha.strategy.priority.od-quantile"),
                properties.getInt("raha.strategy.priority.pvd-character-set"),
                properties.getInt("raha.strategy.priority.pvd-length"),
                properties.getInt("raha.strategy.priority.pvd-null-placeholder"),
                properties.getInt("raha.strategy.priority.pvd-type-format"),
                properties.getInt("raha.strategy.priority.rvd-one-to-many"));
    }

    public int profileMaxValueFrequencyCount() {
        return positiveInt("raha.profile.max-value-frequency-count");
    }

    public int profileQuantileAccuracy() {
        return positiveInt("raha.profile.quantile-accuracy");
    }

    public double featureRareValueRatio() {
        return properties.getDouble("raha.feature.rare-value-ratio");
    }

    public long resourceCacheThresholdBytes() {
        return properties.getLong("raha.resource.cache-threshold-bytes");
    }

    public double modelContextWeight() {
        return properties.getDouble("raha.model.context-weight");
    }

    public double modelDefaultStrategyFamilyWeight() {
        return properties.getDouble("raha.model.default-strategy-family-weight");
    }

    public double modelRareContextSignalWeight() {
        return properties.getDouble("raha.model.context-signal.rare-weight");
    }

    public double modelRvdConflictContextSignalWeight() {
        return properties.getDouble("raha.model.context-signal.rvd-conflict-weight");
    }

    public double modelNullContextSignalWeight() {
        return properties.getDouble("raha.model.context-signal.null-weight");
    }

    public double modelBlankContextSignalWeight() {
        return properties.getDouble("raha.model.context-signal.blank-weight");
    }

    public double modelMixedContextSignalWeight() {
        return properties.getDouble("raha.model.context-signal.mixed-weight");
    }

    public double samplingCoverageScoreExponentCap() {
        return properties.getDouble("raha.sampling.coverage-score-exponent-cap");
    }

    public double labelMaxDirectWeightRatio() {
        return properties.getDouble("raha.label.max-direct-weight-ratio");
    }

    public RahaProperties getProperties() {
        return properties;
    }

    /**
     * 生成影响检测执行语义的资源配置指纹，供任务版本和检查点隔离使用。
     */
    public String executionFingerprint() {
        TreeMap<String, String> executionValues = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : properties.asMap().entrySet()) {
            if (isExecutionProperty(entry.getKey())) {
                executionValues.put(entry.getKey(), entry.getValue());
            }
        }
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<String, String> entry : executionValues.entrySet()) {
            canonical.append(ConfigTextUtils.token(entry.getKey()))
                    .append(ConfigTextUtils.token(entry.getValue()));
        }
        return HashUtils.sha256Hex(canonical.toString());
    }

    private int positiveInt(String key) {
        int value = properties.getInt(key);
        if (value <= 0) {
            throw new RahaConfigurationException(key,
                    "配置必须大于 0，propertyKey=" + key);
        }
        return value;
    }

    private static boolean isExecutionProperty(String key) {
        return key.startsWith("raha.job.")
                || key.startsWith("raha.strategy.")
                || key.startsWith("raha.feature.")
                || key.startsWith("raha.profile.")
                || key.startsWith("raha.model.")
                || key.startsWith("raha.clustering.")
                || key.startsWith("raha.sampling.")
                || key.startsWith("raha.label.")
                || key.startsWith("raha.resource.")
                || key.startsWith("raha.failure.");
    }

    private String placeholders() {
        String configured = properties.getRequired(
                "raha.strategy.pvd.placeholders");
        Set<String> values = new LinkedHashSet<String>();
        for (String item : configured.split("\\|", -1)) {
            if (item.isEmpty() || !values.add(item)) {
                throw new RahaConfigurationException(
                        "raha.strategy.pvd.placeholders",
                        "PVD 占位值不能包含空值或重复值");
            }
        }
        StringBuilder encoded = new StringBuilder();
        for (String value : values) {
            if (encoded.length() > 0) {
                encoded.append(',');
            }
            encoded.append(value);
        }
        return encoded.toString();
    }

    private Map<String, Integer> integerMap(String key) {
        Map<String, Integer> result = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, String> entry
                : properties.getStringMap(key).entrySet()) {
            try {
                result.put(entry.getKey(), Integer.parseInt(entry.getValue()));
            } catch (NumberFormatException exception) {
                throw new RahaConfigurationException(key,
                        "映射配置值必须为整数，propertyKey=" + key, exception);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private <E extends Enum<E>> Set<E> enumSet(String key, Class<E> enumType) {
        Set<String> configured = properties.getCsvSet(key);
        if (configured.isEmpty()) {
            return Collections.emptySet();
        }
        EnumSet<E> result = EnumSet.noneOf(enumType);
        for (String value : configured) {
            result.add(parseEnum(key, value, enumType));
        }
        return Collections.unmodifiableSet(result);
    }

    private static <E extends Enum<E>> E parseEnum(String key,
                                                    String value,
                                                    Class<E> enumType) {
        try {
            return Enum.valueOf(enumType, value.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new RahaConfigurationException(key,
                    "配置不是受支持的枚举值，propertyKey=" + key, exception);
        }
    }

    private static double parseDouble(String key, String value) {
        try {
            double parsed = Double.parseDouble(value);
            if (Double.isNaN(parsed) || Double.isInfinite(parsed)) {
                throw new NumberFormatException("非有限小数");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new RahaConfigurationException(key,
                    "映射配置值必须为有限小数，propertyKey=" + key, exception);
        }
    }
}
