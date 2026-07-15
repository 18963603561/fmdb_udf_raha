package com.fiberhome.ml.raha.config;

import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.StrategyFamily;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * 控制列级分类器、判断阈值和降级行为。
 */
public final class ModelConfig {

    /** 首选分类器类型。 */
    private final ClassifierType classifierType;
    /** 判定疑似错误的分数阈值。 */
    private final double threshold;
    /** 首选分类器不可用时是否允许规则加权降级。 */
    private final boolean fallbackEnabled;
    /** 各策略族在基础检测中的可靠度权重。 */
    private final Map<StrategyFamily, Double> strategyFamilyWeights;
    /** 上下文异常信号的最大融合权重。 */
    private final double contextWeight;

    public ModelConfig(ClassifierType classifierType, double threshold, boolean fallbackEnabled) {
        this(classifierType, threshold, fallbackEnabled, defaultWeights(), 0.2d);
    }

    public ModelConfig(ClassifierType classifierType,
                       double threshold,
                       boolean fallbackEnabled,
                       Map<StrategyFamily, Double> strategyFamilyWeights,
                       double contextWeight) {
        this.classifierType = classifierType;
        this.threshold = threshold;
        this.fallbackEnabled = fallbackEnabled;
        EnumMap<StrategyFamily, Double> weights =
                new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        if (strategyFamilyWeights != null) {
            weights.putAll(strategyFamilyWeights);
        }
        this.strategyFamilyWeights = Collections.unmodifiableMap(weights);
        this.contextWeight = contextWeight;
    }

    public static ModelConfig defaults() {
        return new ModelConfig(ClassifierType.WEIGHTED_RULE, 0.5d, true);
    }

    public ClassifierType getClassifierType() {
        return classifierType;
    }

    public double getThreshold() {
        return threshold;
    }

    public boolean isFallbackEnabled() {
        return fallbackEnabled;
    }

    public Map<StrategyFamily, Double> getStrategyFamilyWeights() {
        return strategyFamilyWeights;
    }

    public double getContextWeight() {
        return contextWeight;
    }

    String toCanonicalString() {
        return ConfigTextUtils.token(classifierType)
                + ConfigTextUtils.token(threshold)
                + ConfigTextUtils.token(fallbackEnabled)
                + ConfigTextUtils.sortedMapTokens(strategyFamilyWeights)
                + ConfigTextUtils.token(contextWeight);
    }

    private static Map<StrategyFamily, Double> defaultWeights() {
        Map<StrategyFamily, Double> weights =
                new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        weights.put(StrategyFamily.OD, 0.8d);
        weights.put(StrategyFamily.PVD, 0.7d);
        weights.put(StrategyFamily.RVD, 1.0d);
        weights.put(StrategyFamily.KBVD, 1.0d);
        weights.put(StrategyFamily.TFIDF, 0.5d);
        return weights;
    }
}
