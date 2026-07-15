package com.fiberhome.ml.raha.config;

/**
 * 控制任务遇到策略失败时的继续、重试和终止边界。
 */
public final class FailureToleranceConfig {

    /** 是否在首次可恢复失败时立即终止任务。 */
    private final boolean failFast;
    /** 允许失败的策略数量占比。 */
    private final double maxFailedStrategyRatio;
    /** 单阶段允许的最大重试次数。 */
    private final int maxRetryCount;

    public FailureToleranceConfig(boolean failFast, double maxFailedStrategyRatio, int maxRetryCount) {
        this.failFast = failFast;
        this.maxFailedStrategyRatio = maxFailedStrategyRatio;
        this.maxRetryCount = maxRetryCount;
    }

    public static FailureToleranceConfig defaults() {
        return RahaDefaultConfigProvider.factory().failureToleranceConfig();
    }

    public boolean isFailFast() {
        return failFast;
    }

    public double getMaxFailedStrategyRatio() {
        return maxFailedStrategyRatio;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    String toCanonicalString() {
        return ConfigTextUtils.token(failFast)
                + ConfigTextUtils.token(maxFailedStrategyRatio)
                + ConfigTextUtils.token(maxRetryCount);
    }
}
