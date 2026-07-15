package com.fiberhome.ml.raha.performance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 汇总一次基准运行的全部阶段指标，供验收报告或指标表持久化使用。
 */
public final class PerformanceBaselineReport {

    /** 基准数据集定义。 */
    private final BenchmarkDatasetSpec datasetSpec;
    /** 按执行顺序保存的阶段指标。 */
    private final List<StagePerformanceMetric> stageMetrics;
    /** 基准报告生成时间。 */
    private final long generatedAt;

    public PerformanceBaselineReport(BenchmarkDatasetSpec datasetSpec,
                                     List<StagePerformanceMetric> stageMetrics,
                                     long generatedAt) {
        if (datasetSpec == null || stageMetrics == null || stageMetrics.isEmpty()
                || generatedAt <= 0L) {
            throw new IllegalArgumentException("性能基准报告数据不能为空");
        }
        this.datasetSpec = datasetSpec;
        this.stageMetrics = Collections.unmodifiableList(
                new ArrayList<StagePerformanceMetric>(stageMetrics));
        this.generatedAt = generatedAt;
    }

    public long getTotalElapsedMillis() {
        long total = 0L;
        for (StagePerformanceMetric metric : stageMetrics) {
            total += metric.getElapsedMillis();
        }
        return total;
    }

    public BenchmarkDatasetSpec getDatasetSpec() { return datasetSpec; }
    public List<StagePerformanceMetric> getStageMetrics() { return stageMetrics; }
    public long getGeneratedAt() { return generatedAt; }
}
