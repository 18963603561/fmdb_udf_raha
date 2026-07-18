package com.fiberhome.ml.raha.feature;

/**
 * 汇总特征组装的单元格、原始特征和过滤数量。
 */
public final class FeatureAssemblyMetrics {

    /** 生成特征的单元格数量。 */
    private final long cellCount;
    /** 过滤前的候选特征数量。 */
    private final long candidateFeatureCount;
    /** 最终保留的特征数量。 */
    private final long retainedFeatureCount;
    /** 被识别为全零或常量的特征数量。 */
    private final long removedConstantFeatureCount;

    public FeatureAssemblyMetrics(long cellCount,
                                  long candidateFeatureCount,
                                  long retainedFeatureCount,
                                  long removedConstantFeatureCount) {
        if (cellCount < 0L || candidateFeatureCount < 0L || retainedFeatureCount < 0L
                || removedConstantFeatureCount < 0L) {
            throw new IllegalArgumentException("特征指标不能为负数");
        }
        this.cellCount = cellCount;
        this.candidateFeatureCount = candidateFeatureCount;
        this.retainedFeatureCount = retainedFeatureCount;
        this.removedConstantFeatureCount = removedConstantFeatureCount;
    }

    public long getCellCount() { return cellCount; }
    public long getCandidateFeatureCount() { return candidateFeatureCount; }
    public long getRetainedFeatureCount() { return retainedFeatureCount; }
    public long getRemovedConstantFeatureCount() { return removedConstantFeatureCount; }
}
