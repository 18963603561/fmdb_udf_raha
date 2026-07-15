package com.fiberhome.ml.raha.config;

/**
 * 控制值规范化、上下文特征和特征规模。
 */
public final class FeatureConfig {

    /** 是否去除值首尾空白。 */
    private final boolean trimValue;
    /** 是否将文本统一为小写。 */
    private final boolean lowerCaseValue;
    /** 是否统一全角和半角字符。 */
    private final boolean normalizeWidth;
    /** 是否生成值上下文和列内上下文特征。 */
    private final boolean contextFeaturesEnabled;
    /** 是否删除全零或常量特征。 */
    private final boolean removeConstantFeatures;
    /** 单列允许保留的最大特征数量。 */
    private final int maxFeatureCount;

    public FeatureConfig(boolean trimValue,
                         boolean lowerCaseValue,
                         boolean normalizeWidth,
                         boolean contextFeaturesEnabled,
                         boolean removeConstantFeatures,
                         int maxFeatureCount) {
        this.trimValue = trimValue;
        this.lowerCaseValue = lowerCaseValue;
        this.normalizeWidth = normalizeWidth;
        this.contextFeaturesEnabled = contextFeaturesEnabled;
        this.removeConstantFeatures = removeConstantFeatures;
        this.maxFeatureCount = maxFeatureCount;
    }

    public static FeatureConfig defaults() {
        return new FeatureConfig(true, false, true, true, true, 10000);
    }

    public boolean isTrimValue() {
        return trimValue;
    }

    public boolean isLowerCaseValue() {
        return lowerCaseValue;
    }

    public boolean isNormalizeWidth() {
        return normalizeWidth;
    }

    public boolean isContextFeaturesEnabled() {
        return contextFeaturesEnabled;
    }

    public boolean isRemoveConstantFeatures() {
        return removeConstantFeatures;
    }

    public int getMaxFeatureCount() {
        return maxFeatureCount;
    }

    String toCanonicalString() {
        return ConfigTextUtils.token(trimValue)
                + ConfigTextUtils.token(lowerCaseValue)
                + ConfigTextUtils.token(normalizeWidth)
                + ConfigTextUtils.token(contextFeaturesEnabled)
                + ConfigTextUtils.token(removeConstantFeatures)
                + ConfigTextUtils.token(maxFeatureCount);
    }
}
