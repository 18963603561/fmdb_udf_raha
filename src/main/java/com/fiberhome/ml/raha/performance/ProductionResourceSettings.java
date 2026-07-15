package com.fiberhome.ml.raha.performance;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * 保存生产分区计算、容量边界和各分档资源建议配置。
 */
public final class ProductionResourceSettings {

    /** 每个分区目标字节数。 */
    private final long targetBytesPerPartition;
    /** 每个分区目标行数。 */
    private final long targetRowsPerPartition;
    /** 最小分区数。 */
    private final int minimumPartitions;
    /** 最大分区数。 */
    private final int maximumPartitions;
    /** 小型容量最大行数。 */
    private final long smallMaxRows;
    /** 小型容量最大字段数。 */
    private final int smallMaxColumns;
    /** 中型容量最大行数。 */
    private final long mediumMaxRows;
    /** 中型容量最大字段数。 */
    private final int mediumMaxColumns;
    /** 大型容量最大行数。 */
    private final long largeMaxRows;
    /** 大型容量最大字段数。 */
    private final int largeMaxColumns;
    /** 各容量分档资源参数。 */
    private final Map<CapacityBand, CapacityBandResourceSettings> bands;

    public ProductionResourceSettings(long targetBytesPerPartition,
                                      long targetRowsPerPartition,
                                      int minimumPartitions,
                                      int maximumPartitions,
                                      long smallMaxRows,
                                      int smallMaxColumns,
                                      long mediumMaxRows,
                                      int mediumMaxColumns,
                                      long largeMaxRows,
                                      int largeMaxColumns,
                                      Map<CapacityBand, CapacityBandResourceSettings> bands) {
        if (targetBytesPerPartition <= 0L || targetRowsPerPartition <= 0L
                || minimumPartitions <= 0 || maximumPartitions < minimumPartitions
                || smallMaxRows <= 0L || smallMaxColumns <= 0
                || mediumMaxRows < smallMaxRows || mediumMaxColumns < smallMaxColumns
                || largeMaxRows < mediumMaxRows || largeMaxColumns < mediumMaxColumns
                || bands == null || bands.size() != CapacityBand.values().length) {
            throw new IllegalArgumentException("生产资源分区和容量配置必须有效");
        }
        EnumMap<CapacityBand, CapacityBandResourceSettings> copied =
                new EnumMap<CapacityBand, CapacityBandResourceSettings>(CapacityBand.class);
        copied.putAll(bands);
        for (CapacityBand band : CapacityBand.values()) {
            if (copied.get(band) == null) {
                throw new IllegalArgumentException("生产资源配置缺少容量分档：" + band);
            }
        }
        this.targetBytesPerPartition = targetBytesPerPartition;
        this.targetRowsPerPartition = targetRowsPerPartition;
        this.minimumPartitions = minimumPartitions;
        this.maximumPartitions = maximumPartitions;
        this.smallMaxRows = smallMaxRows;
        this.smallMaxColumns = smallMaxColumns;
        this.mediumMaxRows = mediumMaxRows;
        this.mediumMaxColumns = mediumMaxColumns;
        this.largeMaxRows = largeMaxRows;
        this.largeMaxColumns = largeMaxColumns;
        this.bands = Collections.unmodifiableMap(copied);
    }

    public CapacityBand classify(long rowCount, int dataColumnCount) {
        if (rowCount <= smallMaxRows && dataColumnCount <= smallMaxColumns) {
            return CapacityBand.SMALL;
        }
        if (rowCount <= mediumMaxRows && dataColumnCount <= mediumMaxColumns) {
            return CapacityBand.MEDIUM;
        }
        if (rowCount <= largeMaxRows && dataColumnCount <= largeMaxColumns) {
            return CapacityBand.LARGE;
        }
        return CapacityBand.EXTRA_LARGE;
    }

    public long getTargetBytesPerPartition() { return targetBytesPerPartition; }
    public long getTargetRowsPerPartition() { return targetRowsPerPartition; }
    public int getMinimumPartitions() { return minimumPartitions; }
    public int getMaximumPartitions() { return maximumPartitions; }
    public Map<CapacityBand, CapacityBandResourceSettings> getBands() { return bands; }
}
