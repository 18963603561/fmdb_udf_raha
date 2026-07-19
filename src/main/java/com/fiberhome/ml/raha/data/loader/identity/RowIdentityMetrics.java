package com.fiberhome.ml.raha.data.loader.identity;

/**
 * 汇总行身份生成和逻辑去重结果，不包含任何原始业务值。
 */
public final class RowIdentityMetrics {

    /** 输入物理行数量。 */
    private final long sourceRowCount;
    /** 去重后逻辑行数量。 */
    private final long logicalRowCount;
    /** 静默折叠的重复物理行数量。 */
    private final long discardedDuplicateCount;
    /** 相同业务键对应多个内容版本的键数量。 */
    private final long keyConflictCount;

    public RowIdentityMetrics(long sourceRowCount,
                              long logicalRowCount,
                              long discardedDuplicateCount,
                              long keyConflictCount) {
        if (sourceRowCount <= 0L || logicalRowCount <= 0L
                || discardedDuplicateCount < 0L || keyConflictCount < 0L
                || logicalRowCount + discardedDuplicateCount != sourceRowCount) {
            throw new IllegalArgumentException("行身份去重指标不一致");
        }
        this.sourceRowCount = sourceRowCount;
        this.logicalRowCount = logicalRowCount;
        this.discardedDuplicateCount = discardedDuplicateCount;
        this.keyConflictCount = keyConflictCount;
    }

    public long getSourceRowCount() { return sourceRowCount; }
    public long getLogicalRowCount() { return logicalRowCount; }
    public long getDiscardedDuplicateCount() { return discardedDuplicateCount; }
    public long getKeyConflictCount() { return keyConflictCount; }
}
