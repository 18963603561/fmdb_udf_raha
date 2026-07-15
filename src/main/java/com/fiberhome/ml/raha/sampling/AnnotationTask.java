package com.fiberhome.ml.raha.sampling;

import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一个待标注元组的采样原因、聚类覆盖和状态，不包含纠正值。
 */
public final class AnnotationTask {

    /** 标注任务稳定标识。 */
    private final String taskId;
    /** 所属 Raha 任务标识。 */
    private final String jobId;
    /** 输入数据中的稳定行标识。 */
    private final String rowId;
    /** 采样轮次，从一开始。 */
    private final int samplingRound;
    /** 元组采样权重。 */
    private final double samplingScore;
    /** 当前行覆盖的字段和聚类版本标识。 */
    private final Map<String, String> coveredClusters;
    /** 本轮聚类、配置和排除集合生成的采样版本。 */
    private final String samplingVersion;
    /** 当前任务状态。 */
    private AnnotationTaskStatus status;
    /** 任务创建时间。 */
    private final long createdAt;
    /** 任务过期时间。 */
    private final long expiresAt;
    /** 任务进入终态的时间，待标注时为零。 */
    private long finishedAt;

    public AnnotationTask(String taskId,
                          String jobId,
                          String rowId,
                          int samplingRound,
                          double samplingScore,
                          Map<String, String> coveredClusters,
                          String samplingVersion,
                          long createdAt,
                          long expiresAt) {
        this.taskId = ValueUtils.requireNotBlank(taskId, "标注任务标识");
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        this.rowId = ValueUtils.requireNotBlank(rowId, "行标识");
        if (samplingRound <= 0) {
            throw new IllegalArgumentException("采样轮次必须大于 0");
        }
        if (Double.isNaN(samplingScore) || Double.isInfinite(samplingScore)
                || samplingScore < 0.0d) {
            throw new IllegalArgumentException("采样分数必须为非负有限数值");
        }
        if (coveredClusters == null || coveredClusters.isEmpty()) {
            throw new IllegalArgumentException("标注任务必须包含至少一个聚类覆盖");
        }
        this.samplingRound = samplingRound;
        this.samplingScore = samplingScore;
        this.coveredClusters = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(coveredClusters));
        this.samplingVersion = ValueUtils.requireNotBlank(samplingVersion, "采样版本");
        if (createdAt <= 0L || expiresAt <= createdAt) {
            throw new IllegalArgumentException("标注任务创建时间和过期时间非法");
        }
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.status = AnnotationTaskStatus.PENDING;
    }

    /**
     * 完成待标注任务，过期或其他终态任务不能重复提交。
     *
     * @param finishTime 完成时间
     */
    public synchronized void complete(long finishTime) {
        validatePendingFinish(finishTime);
        if (finishTime > expiresAt) {
            throw new IllegalStateException("已过期标注任务不能完成");
        }
        status = AnnotationTaskStatus.COMPLETED;
        finishedAt = finishTime;
    }

    /**
     * 在到达过期时间后关闭仍待处理的任务。
     *
     * @param finishTime 过期处理时间
     */
    public synchronized void expire(long finishTime) {
        validatePendingFinish(finishTime);
        if (finishTime < expiresAt) {
            throw new IllegalArgumentException("未到有效期的标注任务不能过期");
        }
        status = AnnotationTaskStatus.EXPIRED;
        finishedAt = finishTime;
    }

    /**
     * 取消仍待处理的任务。
     *
     * @param finishTime 取消时间
     */
    public synchronized void cancel(long finishTime) {
        validatePendingFinish(finishTime);
        status = AnnotationTaskStatus.CANCELLED;
        finishedAt = finishTime;
    }

    private void validatePendingFinish(long finishTime) {
        // 标注任务终态不可再次修改，复核必须生成新的采样任务。
        if (status != AnnotationTaskStatus.PENDING) {
            throw new IllegalStateException("只有待标注任务可以进入终态");
        }
        if (finishTime < createdAt) {
            throw new IllegalArgumentException("任务结束时间不能早于创建时间");
        }
    }

    public synchronized AnnotationTask snapshot() {
        AnnotationTask copy = new AnnotationTask(taskId, jobId, rowId, samplingRound,
                samplingScore, coveredClusters, samplingVersion, createdAt, expiresAt);
        copy.status = status;
        copy.finishedAt = finishedAt;
        return copy;
    }

    public String getTaskId() { return taskId; }
    public String getJobId() { return jobId; }
    public String getRowId() { return rowId; }
    public int getSamplingRound() { return samplingRound; }
    public double getSamplingScore() { return samplingScore; }
    public Map<String, String> getCoveredClusters() { return coveredClusters; }
    public String getSamplingVersion() { return samplingVersion; }
    public synchronized AnnotationTaskStatus getStatus() { return status; }
    public long getCreatedAt() { return createdAt; }
    public long getExpiresAt() { return expiresAt; }
    public synchronized long getFinishedAt() { return finishedAt; }
}
