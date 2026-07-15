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
    /** 未明确配置策略族时使用的默认可靠度权重。 */
    private final double defaultStrategyFamilyWeight;
    /** 稀有值上下文信号权重。 */
    private final double rareContextSignalWeight;
    /** RVD 冲突上下文信号权重。 */
    private final double rvdConflictContextSignalWeight;
    /** 空值上下文信号权重。 */
    private final double nullContextSignalWeight;
    /** 空白值上下文信号权重。 */
    private final double blankContextSignalWeight;
    /** 混合类型上下文信号权重。 */
    private final double mixedContextSignalWeight;

    public ModelConfig(ClassifierType classifierType, double threshold, boolean fallbackEnabled) {
        this(classifierType, threshold, fallbackEnabled,
                RahaDefaultConfigProvider.factory().modelStrategyFamilyWeights(),
                RahaDefaultConfigProvider.factory().modelContextWeight());
    }

    public ModelConfig(ClassifierType classifierType,
                       double threshold,
                       boolean fallbackEnabled,
                       Map<StrategyFamily, Double> strategyFamilyWeights,
                       double contextWeight) {
        this(classifierType, threshold, fallbackEnabled, strategyFamilyWeights,
                contextWeight,
                RahaDefaultConfigProvider.factory().modelDefaultStrategyFamilyWeight(),
                RahaDefaultConfigProvider.factory().modelRareContextSignalWeight(),
                RahaDefaultConfigProvider.factory().modelRvdConflictContextSignalWeight(),
                RahaDefaultConfigProvider.factory().modelNullContextSignalWeight(),
                RahaDefaultConfigProvider.factory().modelBlankContextSignalWeight(),
                RahaDefaultConfigProvider.factory().modelMixedContextSignalWeight());
    }

    public ModelConfig(ClassifierType classifierType,
                       double threshold,
                       boolean fallbackEnabled,
                       Map<StrategyFamily, Double> strategyFamilyWeights,
                       double contextWeight,
                       double defaultStrategyFamilyWeight,
                       double rareContextSignalWeight,
                       double rvdConflictContextSignalWeight,
                       double nullContextSignalWeight,
                       double blankContextSignalWeight,
                       double mixedContextSignalWeight) {
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
        this.defaultStrategyFamilyWeight = defaultStrategyFamilyWeight;
        this.rareContextSignalWeight = rareContextSignalWeight;
        this.rvdConflictContextSignalWeight = rvdConflictContextSignalWeight;
        this.nullContextSignalWeight = nullContextSignalWeight;
        this.blankContextSignalWeight = blankContextSignalWeight;
        this.mixedContextSignalWeight = mixedContextSignalWeight;
    }

    public static ModelConfig defaults() {
        return RahaDefaultConfigProvider.factory().modelConfig();
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

    public double getDefaultStrategyFamilyWeight() { return defaultStrategyFamilyWeight; }
    public double getRareContextSignalWeight() { return rareContextSignalWeight; }
    public double getRvdConflictContextSignalWeight() {
        return rvdConflictContextSignalWeight;
    }
    public double getNullContextSignalWeight() { return nullContextSignalWeight; }
    public double getBlankContextSignalWeight() { return blankContextSignalWeight; }
    public double getMixedContextSignalWeight() { return mixedContextSignalWeight; }

    String toCanonicalString() {
        return ConfigTextUtils.token(classifierType)
                + ConfigTextUtils.token(threshold)
                + ConfigTextUtils.token(fallbackEnabled)
                + ConfigTextUtils.sortedMapTokens(strategyFamilyWeights)
                + ConfigTextUtils.token(contextWeight)
                + ConfigTextUtils.token(defaultStrategyFamilyWeight)
                + ConfigTextUtils.token(rareContextSignalWeight)
                + ConfigTextUtils.token(rvdConflictContextSignalWeight)
                + ConfigTextUtils.token(nullContextSignalWeight)
                + ConfigTextUtils.token(blankContextSignalWeight)
                + ConfigTextUtils.token(mixedContextSignalWeight);
    }

}
