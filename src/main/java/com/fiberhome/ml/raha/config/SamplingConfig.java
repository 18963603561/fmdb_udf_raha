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

    public SamplingConfig(int labelingBudget,
                          boolean clusteringBasedSampling,
                          boolean reviewEnabled,
                          long taskTtlMillis) {
        this.labelingBudget = labelingBudget;
        this.clusteringBasedSampling = clusteringBasedSampling;
        this.reviewEnabled = reviewEnabled;
        this.taskTtlMillis = taskTtlMillis;
    }

    public static SamplingConfig defaults() {
        return new SamplingConfig(20, true, false, 7L * 24L * 60L * 60L * 1000L);
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

    String toCanonicalString() {
        return ConfigTextUtils.token(labelingBudget)
                + ConfigTextUtils.token(clusteringBasedSampling)
                + ConfigTextUtils.token(reviewEnabled)
                + ConfigTextUtils.token(taskTtlMillis);
    }
}
