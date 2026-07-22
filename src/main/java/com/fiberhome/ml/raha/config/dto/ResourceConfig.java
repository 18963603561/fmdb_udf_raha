package com.fiberhome.ml.raha.config.dto;

import com.fiberhome.ml.raha.config.core.ConfigTextUtils;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;

/**
 * 控制 Spark 任务并发、广播、缓存和阶段超时。
 */
public final class ResourceConfig {

    /** 同时运行的最大策略任务数量。 */
    private final int maxParallelStrategies;
    /** 同时运行的最大列任务数量。 */
    private final int maxParallelColumns;
    /** 允许广播的小对象最大字节数。 */
    private final long broadcastThresholdBytes;
    /** Spark 缓存级别名称。 */
    private final String cacheStorageLevel;
    /** 允许缓存的数据最大估算字节数。 */
    private final long cacheThresholdBytes;
    /** 单阶段最大运行时间，单位毫秒。 */
    private final long stageTimeoutMillis;
    /** 是否启用批内字段特征并行组装。 */
    private final boolean featureParallelEnabled;
    /** 是否启用批内字段本地并行聚类。 */
    private final boolean clusteringParallelEnabled;

    public ResourceConfig(int maxParallelStrategies,
                          int maxParallelColumns,
                          long broadcastThresholdBytes,
                          String cacheStorageLevel,
                          long stageTimeoutMillis) {
        this(maxParallelStrategies, maxParallelColumns, broadcastThresholdBytes,
                cacheStorageLevel,
                RahaDefaultConfigProvider.factory().resourceCacheThresholdBytes(),
                stageTimeoutMillis, true, true);
    }

    public ResourceConfig(int maxParallelStrategies,
                          int maxParallelColumns,
                          long broadcastThresholdBytes,
                          String cacheStorageLevel,
                          long cacheThresholdBytes,
                          long stageTimeoutMillis) {
        this(maxParallelStrategies, maxParallelColumns,
                broadcastThresholdBytes, cacheStorageLevel,
                cacheThresholdBytes, stageTimeoutMillis, true, true);
    }

    public ResourceConfig(int maxParallelStrategies,
                          int maxParallelColumns,
                          long broadcastThresholdBytes,
                          String cacheStorageLevel,
                          long cacheThresholdBytes,
                          long stageTimeoutMillis,
                          boolean featureParallelEnabled,
                          boolean clusteringParallelEnabled) {
        this.maxParallelStrategies = maxParallelStrategies;
        this.maxParallelColumns = maxParallelColumns;
        this.broadcastThresholdBytes = broadcastThresholdBytes;
        this.cacheStorageLevel = cacheStorageLevel;
        this.cacheThresholdBytes = cacheThresholdBytes;
        this.stageTimeoutMillis = stageTimeoutMillis;
        this.featureParallelEnabled = featureParallelEnabled;
        this.clusteringParallelEnabled = clusteringParallelEnabled;
    }

    public static ResourceConfig defaults() {
        return RahaDefaultConfigProvider.factory().resourceConfig();
    }

    public int getMaxParallelStrategies() {
        return maxParallelStrategies;
    }

    public int getMaxParallelColumns() {
        return maxParallelColumns;
    }

    public long getBroadcastThresholdBytes() {
        return broadcastThresholdBytes;
    }

    public String getCacheStorageLevel() {
        return cacheStorageLevel;
    }

    public long getCacheThresholdBytes() {
        return cacheThresholdBytes;
    }

    public long getStageTimeoutMillis() {
        return stageTimeoutMillis;
    }

    public boolean isFeatureParallelEnabled() {
        return featureParallelEnabled;
    }

    public boolean isClusteringParallelEnabled() {
        return clusteringParallelEnabled;
    }

    String toCanonicalString() {
        return ConfigTextUtils.token(maxParallelStrategies)
                + ConfigTextUtils.token(maxParallelColumns)
                + ConfigTextUtils.token(broadcastThresholdBytes)
                + ConfigTextUtils.token(cacheStorageLevel)
                + ConfigTextUtils.token(cacheThresholdBytes)
                + ConfigTextUtils.token(stageTimeoutMillis)
                + ConfigTextUtils.token(featureParallelEnabled)
                + ConfigTextUtils.token(clusteringParallelEnabled);
    }
}
