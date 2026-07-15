package com.fiberhome.ml.raha.sampling;

/**
 * 汇总采样候选、排除元组和新建标注任务数量。
 */
public final class SamplingMetrics {

    /** 参与评分的候选元组数量。 */
    private final long candidateTupleCount;
    /** 因已有标签或任务被排除的元组数量。 */
    private final long excludedTupleCount;
    /** 本轮新建标注任务数量。 */
    private final long createdTaskCount;

    public SamplingMetrics(long candidateTupleCount,
                           long excludedTupleCount,
                           long createdTaskCount) {
        if (candidateTupleCount < 0L || excludedTupleCount < 0L || createdTaskCount < 0L) {
            throw new IllegalArgumentException("采样指标不能为负数");
        }
        this.candidateTupleCount = candidateTupleCount;
        this.excludedTupleCount = excludedTupleCount;
        this.createdTaskCount = createdTaskCount;
    }

    public long getCandidateTupleCount() { return candidateTupleCount; }
    public long getExcludedTupleCount() { return excludedTupleCount; }
    public long getCreatedTaskCount() { return createdTaskCount; }
}
