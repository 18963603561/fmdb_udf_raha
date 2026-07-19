package com.fiberhome.ml.raha.service.train;

/**
 * 保存 c1 与 o1 合并的可审计数量，不把重复数量当作训练权重。
 */
public final class TrainingMergeMetrics {

    /** c1 逻辑行数量。 */
    private final long sampleCount;
    /** o1 物理输入行数量。 */
    private final long originalCount;
    /** 合并后的逻辑行数量。 */
    private final long mergedCount;
    /** c1 与 o1 身份重叠组数量。 */
    private final long matchedIdentityCount;
    /** 去重组数量。 */
    private final long dedupGroupCount;
    /** 被去除的重复物理行数量。 */
    private final long dedupRowCount;
    /** 联合键内容冲突组数量。 */
    private final long keyContentConflictCount;
    /** c1 独有逻辑行数量。 */
    private final long c1OnlyCount;

    public TrainingMergeMetrics(long sampleCount,
                               long originalCount,
                               long mergedCount,
                               long matchedIdentityCount,
                               long dedupGroupCount,
                               long dedupRowCount,
                               long keyContentConflictCount,
                               long c1OnlyCount) {
        if (sampleCount < 0L || originalCount < 0L || mergedCount <= 0L
                || matchedIdentityCount < 0L || dedupGroupCount < 0L
                || dedupRowCount < 0L || keyContentConflictCount < 0L
                || c1OnlyCount < 0L) {
            throw new IllegalArgumentException("训练合并指标不能为负数");
        }
        this.sampleCount = sampleCount;
        this.originalCount = originalCount;
        this.mergedCount = mergedCount;
        this.matchedIdentityCount = matchedIdentityCount;
        this.dedupGroupCount = dedupGroupCount;
        this.dedupRowCount = dedupRowCount;
        this.keyContentConflictCount = keyContentConflictCount;
        this.c1OnlyCount = c1OnlyCount;
    }

    public long getSampleCount() { return sampleCount; }
    public long getOriginalCount() { return originalCount; }
    public long getMergedCount() { return mergedCount; }
    public long getMatchedIdentityCount() { return matchedIdentityCount; }
    public long getDedupGroupCount() { return dedupGroupCount; }
    public long getDedupRowCount() { return dedupRowCount; }
    public long getKeyContentConflictCount() { return keyContentConflictCount; }
    public long getC1OnlyCount() { return c1OnlyCount; }
}
