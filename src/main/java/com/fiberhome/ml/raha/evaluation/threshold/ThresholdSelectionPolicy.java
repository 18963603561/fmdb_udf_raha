package com.fiberhome.ml.raha.evaluation.threshold;

/**
 * 定义召回约束下优先降低误报的字段阈值选择参数。
 */
public final class ThresholdSelectionPolicy {

    /** 验证集允许的最低召回率。 */
    private final double minimumRecall;
    /** 相对默认阈值允许的最大召回下降。 */
    private final double maximumRecallDrop;
    /** 用于计算相对召回下限的默认阈值。 */
    private final double baselineThreshold;

    public ThresholdSelectionPolicy(double minimumRecall,
                                    double maximumRecallDrop,
                                    double baselineThreshold) {
        if (Double.isNaN(minimumRecall) || minimumRecall < 0.0d || minimumRecall > 1.0d
                || Double.isNaN(maximumRecallDrop) || maximumRecallDrop < 0.0d
                || maximumRecallDrop > 1.0d
                || Double.isNaN(baselineThreshold) || baselineThreshold < 0.0d
                || baselineThreshold > 1.0d) {
            throw new IllegalArgumentException("阈值选择策略参数必须位于零到一之间");
        }
        this.minimumRecall = minimumRecall;
        this.maximumRecallDrop = maximumRecallDrop;
        this.baselineThreshold = baselineThreshold;
    }

    public double getMinimumRecall() { return minimumRecall; }
    public double getMaximumRecallDrop() { return maximumRecallDrop; }
    public double getBaselineThreshold() { return baselineThreshold; }
}
