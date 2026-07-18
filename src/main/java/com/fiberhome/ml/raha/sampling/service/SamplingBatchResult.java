package com.fiberhome.ml.raha.sampling.service;

import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.domain.TupleSamplingScore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存一次采样的全部评分、标注任务、版本和指标。
 */
public final class SamplingBatchResult {

    /** 本轮全部可选元组分数。 */
    private final List<TupleSamplingScore> scores;
    /** 预算内生成的待标注任务。 */
    private final List<AnnotationTask> tasks;
    /** 稳定采样版本。 */
    private final String samplingVersion;
    /** 采样指标。 */
    private final SamplingMetrics metrics;

    public SamplingBatchResult(List<TupleSamplingScore> scores,
                               List<AnnotationTask> tasks,
                               String samplingVersion,
                               SamplingMetrics metrics) {
        if (scores == null || tasks == null || samplingVersion == null || metrics == null) {
            throw new IllegalArgumentException("采样批次结果参数不能为空");
        }
        this.scores = Collections.unmodifiableList(
                new ArrayList<TupleSamplingScore>(scores));
        List<AnnotationTask> taskCopies = new ArrayList<AnnotationTask>(tasks.size());
        for (AnnotationTask task : tasks) {
            taskCopies.add(task.snapshot());
        }
        this.tasks = Collections.unmodifiableList(taskCopies);
        this.samplingVersion = samplingVersion;
        this.metrics = metrics;
    }

    public List<TupleSamplingScore> getScores() { return scores; }
    public List<AnnotationTask> getTasks() {
        List<AnnotationTask> copies = new ArrayList<AnnotationTask>(tasks.size());
        for (AnnotationTask task : tasks) {
            copies.add(task.snapshot());
        }
        return Collections.unmodifiableList(copies);
    }
    public String getSamplingVersion() { return samplingVersion; }
    public SamplingMetrics getMetrics() { return metrics; }
}
