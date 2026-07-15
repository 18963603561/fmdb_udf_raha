package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存生产运行所需的分区、并发、RVD、缓存和结果保留建议。
 */
public final class ProductionResourceRecommendation {

    /** 数据容量分档。 */
    private final CapacityBand capacityBand;
    /** 建议 Spark 分区数量。 */
    private final int partitionCount;
    /** 建议策略并发上限。 */
    private final int strategyConcurrency;
    /** 建议字段并发上限。 */
    private final int columnConcurrency;
    /** 建议 RVD 列对上限。 */
    private final int maxRvdColumnPairs;
    /** 是否建议缓存画像和稀疏特征。 */
    private final boolean cacheEnabled;
    /** 建议 Spark 存储级别。 */
    private final String storageLevel;
    /** 建议中间结果保留天数。 */
    private final int intermediateRetentionDays;
    /** 建议检测结果保留天数。 */
    private final int detectionRetentionDays;
    /** 需要在真实 FMDB 环境核实的说明。 */
    private final String qualification;

    public ProductionResourceRecommendation(CapacityBand capacityBand,
                                            int partitionCount,
                                            int strategyConcurrency,
                                            int columnConcurrency,
                                            int maxRvdColumnPairs,
                                            boolean cacheEnabled,
                                            String storageLevel,
                                            int intermediateRetentionDays,
                                            int detectionRetentionDays,
                                            String qualification) {
        if (capacityBand == null || partitionCount <= 0 || strategyConcurrency <= 0
                || columnConcurrency <= 0 || maxRvdColumnPairs <= 0
                || intermediateRetentionDays <= 0 || detectionRetentionDays <= 0) {
            throw new IllegalArgumentException("生产资源建议数值必须有效");
        }
        this.capacityBand = capacityBand;
        this.partitionCount = partitionCount;
        this.strategyConcurrency = strategyConcurrency;
        this.columnConcurrency = columnConcurrency;
        this.maxRvdColumnPairs = maxRvdColumnPairs;
        this.cacheEnabled = cacheEnabled;
        this.storageLevel = ValueUtils.requireNotBlank(storageLevel, "Spark 存储级别");
        this.intermediateRetentionDays = intermediateRetentionDays;
        this.detectionRetentionDays = detectionRetentionDays;
        this.qualification = ValueUtils.requireNotBlank(
                qualification, "生产资源建议适用说明");
    }

    public CapacityBand getCapacityBand() { return capacityBand; }
    public int getPartitionCount() { return partitionCount; }
    public int getStrategyConcurrency() { return strategyConcurrency; }
    public int getColumnConcurrency() { return columnConcurrency; }
    public int getMaxRvdColumnPairs() { return maxRvdColumnPairs; }
    public boolean isCacheEnabled() { return cacheEnabled; }
    public String getStorageLevel() { return storageLevel; }
    public int getIntermediateRetentionDays() { return intermediateRetentionDays; }
    public int getDetectionRetentionDays() { return detectionRetentionDays; }
    public String getQualification() { return qualification; }
}
