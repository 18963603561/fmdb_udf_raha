package com.fiberhome.ml.raha.performance;

/**
 * 根据输入行数、字段数和估算字节数给出保守的生产资源初始建议。
 */
public final class ProductionResourceAdvisor {

    /** 每个 Spark 分区建议处理的压缩前字节数。 */
    private static final long TARGET_BYTES_PER_PARTITION = 128L * 1024L * 1024L;

    /**
     * 返回容量分档和初始资源参数，最终值必须以真实 FMDB 压测结果校准。
     */
    public ProductionResourceRecommendation recommend(long rowCount,
                                                       int dataColumnCount,
                                                       long estimatedInputBytes) {
        if (rowCount <= 0L || dataColumnCount <= 0 || estimatedInputBytes <= 0L) {
            throw new IllegalArgumentException("生产资源建议输入规模必须大于 0");
        }
        CapacityBand band = classify(rowCount, dataColumnCount);
        int bytePartitions = saturatedInt(ceilDivide(
                estimatedInputBytes, TARGET_BYTES_PER_PARTITION));
        int rowPartitions = saturatedInt(ceilDivide(rowCount, 250000L));
        int partitions = clamp(Math.max(bytePartitions, rowPartitions), 8, 2000);
        switch (band) {
            case SMALL:
                return recommendation(band, partitions, 4, 2, 100,
                        false, "MEMORY_AND_DISK", 7, 90);
            case MEDIUM:
                return recommendation(band, partitions, 8, 4, 300,
                        true, "MEMORY_AND_DISK_SER", 7, 90);
            case LARGE:
                return recommendation(band, partitions, 12, 6, 500,
                        true, "MEMORY_AND_DISK_SER", 5, 180);
            case EXTRA_LARGE:
            default:
                return recommendation(band, partitions, 16, 8, 500,
                        true, "DISK_ONLY", 3, 180);
        }
    }

    private static CapacityBand classify(long rowCount, int dataColumnCount) {
        if (rowCount <= 1000000L && dataColumnCount <= 50) {
            return CapacityBand.SMALL;
        }
        if (rowCount <= 10000000L && dataColumnCount <= 100) {
            return CapacityBand.MEDIUM;
        }
        if (rowCount <= 100000000L && dataColumnCount <= 200) {
            return CapacityBand.LARGE;
        }
        return CapacityBand.EXTRA_LARGE;
    }

    private static ProductionResourceRecommendation recommendation(
            CapacityBand band,
            int partitions,
            int strategyConcurrency,
            int columnConcurrency,
            int maxRvdPairs,
            boolean cacheEnabled,
            String storageLevel,
            int intermediateRetentionDays,
            int detectionRetentionDays) {
        return new ProductionResourceRecommendation(band, partitions,
                strategyConcurrency, columnConcurrency, maxRvdPairs,
                cacheEnabled, storageLevel, intermediateRetentionDays,
                detectionRetentionDays,
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
