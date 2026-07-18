package com.fiberhome.ml.raha.label.propagation;

/**
 * 汇总直接标签、传播标签、冲突聚类和无标签聚类数量。
 */
public final class LabelPropagationMetrics {

    /** 输入直接标签数量。 */
    private final long directLabelCount;
    /** 新增传播标签数量。 */
    private final long propagatedLabelCount;
    /** 发生冲突或无明确多数的聚类数量。 */
    private final long conflictClusterCount;
    /** 没有直接标签的聚类数量。 */
    private final long unlabeledClusterCount;

    public LabelPropagationMetrics(long directLabelCount,
                                   long propagatedLabelCount,
                                   long conflictClusterCount,
                                   long unlabeledClusterCount) {
        if (directLabelCount < 0L || propagatedLabelCount < 0L
                || conflictClusterCount < 0L || unlabeledClusterCount < 0L) {
            throw new IllegalArgumentException("标签传播指标不能为负数");
        }
        this.directLabelCount = directLabelCount;
        this.propagatedLabelCount = propagatedLabelCount;
        this.conflictClusterCount = conflictClusterCount;
        this.unlabeledClusterCount = unlabeledClusterCount;
    }

    public long getDirectLabelCount() { return directLabelCount; }
    public long getPropagatedLabelCount() { return propagatedLabelCount; }
    public long getConflictClusterCount() { return conflictClusterCount; }
    public long getUnlabeledClusterCount() { return unlabeledClusterCount; }
}
