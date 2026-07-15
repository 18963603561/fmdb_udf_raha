package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;

/**
 * 根据输入行数、字段数和估算字节数给出保守的生产资源初始建议。
 */
public final class ProductionResourceAdvisor {

    /** 分区计算、容量边界和各分档资源参数。 */
    private final ProductionResourceSettings settings;

    public ProductionResourceAdvisor() {
        this(RahaDefaultConfigProvider.factory().productionResourceSettings());
    }

    public ProductionResourceAdvisor(ProductionResourceSettings settings) {
        if (settings == null) {
            throw new IllegalArgumentException("生产资源建议配置不能为空");
        }
        this.settings = settings;
    }

    /**
     * 返回容量分档和初始资源参数，最终值必须以真实 FMDB 压测结果校准。
     */
    public ProductionResourceRecommendation recommend(long rowCount,
                                                       int dataColumnCount,
                                                       long estimatedInputBytes) {
        if (rowCount <= 0L || dataColumnCount <= 0 || estimatedInputBytes <= 0L) {
            throw new IllegalArgumentException("生产资源建议输入规模必须大于 0");
        }
        CapacityBand band = settings.classify(rowCount, dataColumnCount);
        int bytePartitions = saturatedInt(ceilDivide(
                estimatedInputBytes, settings.getTargetBytesPerPartition()));
        int rowPartitions = saturatedInt(ceilDivide(
                rowCount, settings.getTargetRowsPerPartition()));
        int partitions = clamp(Math.max(bytePartitions, rowPartitions),
                settings.getMinimumPartitions(), settings.getMaximumPartitions());
        CapacityBandResourceSettings bandSettings = settings.getBands().get(band);
        return new ProductionResourceRecommendation(band, partitions,
                bandSettings.getStrategyConcurrency(),
                bandSettings.getColumnConcurrency(),
                bandSettings.getMaxRvdPairs(), bandSettings.isCacheEnabled(),
                bandSettings.getStorageLevel(),
                bandSettings.getIntermediateRetentionDays(),
                bandSettings.getDetectionRetentionDays(),
                "该值是工程初始参数，必须使用目标 FMDB 集群、真实数据分布和并发负载复测");
    }

    private static long ceilDivide(long value, long divisor) {
        return value / divisor + (value % divisor == 0L ? 0L : 1L);
    }

    private static int saturatedInt(long value) {
        return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    private static int clamp(int value, int minimum, int maximum) {
        return Math.max(minimum, Math.min(maximum, value));
    }
}
