package com.fiberhome.ml.raha.config;

import com.fiberhome.ml.raha.cluster.ClusteringDistanceMetric;

/**
 * 控制列内聚类距离、目标簇数量和小表样本上限。
 */
public final class ClusteringConfig {

    /** 聚类距离度量。 */
    private final ClusteringDistanceMetric distanceMetric;
    /** 每列期望生成的簇数量。 */
    private final int targetClusterCount;
    /** 精确层次聚类允许处理的最大单列样本数。 */
    private final int maxSampleCount;

    public ClusteringConfig(ClusteringDistanceMetric distanceMetric,
                            int targetClusterCount,
                            int maxSampleCount) {
        this.distanceMetric = distanceMetric;
        this.targetClusterCount = targetClusterCount;
        this.maxSampleCount = maxSampleCount;
    }

    public static ClusteringConfig defaults() {
        return new ClusteringConfig(ClusteringDistanceMetric.COSINE, 2, 500);
    }

    public ClusteringDistanceMetric getDistanceMetric() {
        return distanceMetric;
    }

    public int getTargetClusterCount() {
        return targetClusterCount;
    }

    public int getMaxSampleCount() {
        return maxSampleCount;
    }

    String toCanonicalString() {
        return ConfigTextUtils.token(distanceMetric)
                + ConfigTextUtils.token(targetClusterCount)
                + ConfigTextUtils.token(maxSampleCount);
    }
}
