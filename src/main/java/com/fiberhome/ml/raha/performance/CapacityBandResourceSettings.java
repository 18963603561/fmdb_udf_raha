package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一个生产容量分档的并发、RVD、缓存和保留建议参数。
 */
public final class CapacityBandResourceSettings {

    /** 建议策略并发。 */
    private final int strategyConcurrency;
    /** 建议字段并发。 */
    private final int columnConcurrency;
    /** 建议 RVD 列对上限。 */
    private final int maxRvdPairs;
    /** 是否建议缓存。 */
    private final boolean cacheEnabled;
    /** 建议存储级别。 */
    private final String storageLevel;
    /** 中间结果保留天数。 */
    private final int intermediateRetentionDays;
    /** 检测结果保留天数。 */
    private final int detectionRetentionDays;

    public CapacityBandResourceSettings(int strategyConcurrency,
                                        int columnConcurrency,
                                        int maxRvdPairs,
                                        boolean cacheEnabled,
                                        String storageLevel,
                                        int intermediateRetentionDays,
                                        int detectionRetentionDays) {
        if (strategyConcurrency <= 0 || columnConcurrency <= 0
                || maxRvdPairs <= 0 || intermediateRetentionDays <= 0
                || detectionRetentionDays <= 0) {
            throw new IllegalArgumentException("容量分档资源参数必须大于 0");
        }
        this.strategyConcurrency = strategyConcurrency;
        this.columnConcurrency = columnConcurrency;
        this.maxRvdPairs = maxRvdPairs;
        this.cacheEnabled = cacheEnabled;
        this.storageLevel = ValueUtils.requireNotBlank(storageLevel, "容量存储级别");
        this.intermediateRetentionDays = intermediateRetentionDays;
        this.detectionRetentionDays = detectionRetentionDays;
    }

    public int getStrategyConcurrency() { return strategyConcurrency; }
    public int getColumnConcurrency() { return columnConcurrency; }
    public int getMaxRvdPairs() { return maxRvdPairs; }
    public boolean isCacheEnabled() { return cacheEnabled; }
    public String getStorageLevel() { return storageLevel; }
    public int getIntermediateRetentionDays() { return intermediateRetentionDays; }
    public int getDetectionRetentionDays() { return detectionRetentionDays; }
}
