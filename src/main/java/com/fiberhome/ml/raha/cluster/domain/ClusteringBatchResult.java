package com.fiberhome.ml.raha.cluster.domain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一次任务中全部字段的聚类结果和指标。
 */
public final class ClusteringBatchResult {

    /** 按字段名称索引的聚类结果。 */
    private final Map<String, ColumnClusteringResult> results;
    /** 批次聚类指标。 */
    private final ClusteringMetrics metrics;

    public ClusteringBatchResult(Map<String, ColumnClusteringResult> results,
                                 ClusteringMetrics metrics) {
        if (results == null || metrics == null) {
            throw new IllegalArgumentException("聚类批次结果和指标不能为空");
        }
        this.results = Collections.unmodifiableMap(
                new LinkedHashMap<String, ColumnClusteringResult>(results));
        this.metrics = metrics;
    }

    public Map<String, ColumnClusteringResult> getResults() { return results; }
    public ClusteringMetrics getMetrics() { return metrics; }
}
