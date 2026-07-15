package com.fiberhome.ml.raha.label;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存直接标签、传播标签、聚类摘要和传播指标。
 */
public final class LabelPropagationResult {

    /** 输入直接标签和新增传播标签。 */
    private final List<CellLabel> labels;
    /** 每个聚类的传播摘要。 */
    private final List<ClusterPropagationSummary> summaries;
    /** 传播指标。 */
    private final LabelPropagationMetrics metrics;

    public LabelPropagationResult(List<CellLabel> labels,
                                  List<ClusterPropagationSummary> summaries,
                                  LabelPropagationMetrics metrics) {
        if (labels == null || summaries == null || metrics == null) {
            throw new IllegalArgumentException("标签传播结果参数不能为空");
        }
        this.labels = Collections.unmodifiableList(new ArrayList<CellLabel>(labels));
        this.summaries = Collections.unmodifiableList(
                new ArrayList<ClusterPropagationSummary>(summaries));
        this.metrics = metrics;
    }

    public List<CellLabel> getLabels() { return labels; }
    public List<ClusterPropagationSummary> getSummaries() { return summaries; }
    public LabelPropagationMetrics getMetrics() { return metrics; }
}
