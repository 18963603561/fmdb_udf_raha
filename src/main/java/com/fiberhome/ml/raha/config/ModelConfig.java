package com.fiberhome.ml.raha.config;

/**
 * 逻辑回归训练配置，首期不提供分类器类型选择。
 */
public final class ModelConfig {

    /** 最大迭代次数。 */
    private final int maxIterations;
    /** L2 正则化参数。 */
    private final double regularization;
    /** 未能自动选择阈值时使用的默认阈值。 */
    private final double defaultThreshold;

    public ModelConfig(int maxIterations, double regularization, double defaultThreshold) {
        if (maxIterations <= 0 || regularization < 0.0
                || defaultThreshold <= 0.0 || defaultThreshold >= 1.0) {
            throw new IllegalArgumentException("逻辑回归配置不合法");
        }
        this.maxIterations = maxIterations;
        this.regularization = regularization;
        this.defaultThreshold = defaultThreshold;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public double getRegularization() {
        return regularization;
    }

    public double getDefaultThreshold() {
        return defaultThreshold;
    }
}
