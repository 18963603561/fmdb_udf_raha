package com.fiberhome.ml.raha.model;

/**
 * 控制逻辑回归迭代次数、正则化和类别平衡行为。
 */
public final class LogisticRegressionTrainingConfig {

    /** 是否启用类别平衡权重。 */
    private final boolean classBalanceEnabled;
    /** 最大优化迭代次数。 */
    private final int maxIterations;
    /** L1 和 L2 总正则化参数。 */
    private final double regularization;
    /** 弹性网络中 L1 比例。 */
    private final double elasticNet;

    public LogisticRegressionTrainingConfig(boolean classBalanceEnabled,
                                            int maxIterations,
                                            double regularization,
                                            double elasticNet) {
        if (maxIterations <= 0 || Double.isNaN(regularization)
                || regularization < 0.0d || Double.isNaN(elasticNet)
                || elasticNet < 0.0d || elasticNet > 1.0d) {
            throw new IllegalArgumentException("逻辑回归训练配置非法");
        }
        this.classBalanceEnabled = classBalanceEnabled;
        this.maxIterations = maxIterations;
        this.regularization = regularization;
        this.elasticNet = elasticNet;
    }

    public static LogisticRegressionTrainingConfig defaults() {
        return new LogisticRegressionTrainingConfig(true, 100, 0.0d, 0.0d);
    }

    public boolean isClassBalanceEnabled() { return classBalanceEnabled; }
    public int getMaxIterations() { return maxIterations; }
    public double getRegularization() { return regularization; }
    public double getElasticNet() { return elasticNet; }
}
