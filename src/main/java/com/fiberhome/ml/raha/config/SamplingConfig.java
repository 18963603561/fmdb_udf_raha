package com.fiberhome.ml.raha.config;

/**
 * 控制聚类覆盖采样预算、任务有效期和复核行为。
 */
public final class SamplingConfig {

    /** 单次任务允许生成的最大待标注元组数量。 */
    private final int labelingBudget;
    /** 是否使用聚类覆盖分数选择元组。 */
    private final boolean clusteringBasedSampling;
    /** 是否允许重新采样已经完成标注的元组。 */
    private final boolean reviewEnabled;
    /** 待标注任务有效期，单位毫秒。 */
    private final long taskTtlMillis;
    /** 聚类覆盖累计分数进入指数函数前的上限。 */
    private final double coverageScoreExponentCap;

    public SamplingConfig(int labelingBudget,
                          boolean clusteringBasedSampling,
                          boolean reviewEnabled,
                          long taskTtlMillis) {
        this(labelingBudget, clusteringBasedSampling, reviewEnabled, taskTtlMillis,
                RahaDefaultConfigProvider.factory().samplingCoverageScoreExponentCap());
    }

    public SamplingConfig(int labelingBudget,
                          boolean clusteringBasedSampling,
                          boolean reviewEnabled,
                          long taskTtlMillis,
                          double coverageScoreExponentCap) {
        this.labelingBudget = labelingBudget;
        this.clusteringBasedSampling = clusteringBasedSampling;
        this.reviewEnabled = reviewEnabled;
        this.taskTtlMillis = taskTtlMillis;
        this.coverageScoreExponentCap = coverageScoreExponentCap;
    }

    public static SamplingConfig defaults() {
        return RahaDefaultConfigProvider.factory().samplingConfig();
    }

    public int getLabelingBudget() {
        return labelingBudget;
    }

    public boolean isClusteringBasedSampling() {
        return clusteringBasedSampling;
    }

    public boolean isReviewEnabled() {
        return reviewEnabled;
    }

    public long getTaskTtlMillis() {
        return taskTtlMillis;
    }

    public double getCoverageScoreExponentCap() {
        return coverageScoreExponentCap;
    }

    String toCanonicalString() {
        return ConfigTextUtils.token(labelingBudget)
                + ConfigTextUtils.token(clusteringBasedSampling)
                + ConfigTextUtils.token(reviewEnabled)
                + ConfigTextUtils.token(taskTtlMillis)
                + ConfigTextUtils.token(coverageScoreExponentCap);
    }
}
