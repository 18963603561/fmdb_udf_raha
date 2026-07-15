package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 定义性能基准数据集的行数、字段数、错误率、分区数和随机种子。
 */
public final class BenchmarkDatasetSpec {

    /** 基准数据集名称。 */
    private final String name;
    /** 基准规模类型。 */
    private final BenchmarkScale scale;
    /** 数据行数。 */
    private final long rowCount;
    /** 行标识之外的数据字段数量。 */
    private final int dataColumnCount;
    /** 每个数据字段的确定性错误率。 */
    private final double errorRate;
    /** 生成数据使用的 Spark 分区数量。 */
    private final int partitionCount;
    /** 确定性错误注入种子。 */
    private final long seed;

    public BenchmarkDatasetSpec(String name,
                                BenchmarkScale scale,
                                long rowCount,
                                int dataColumnCount,
                                double errorRate,
                                int partitionCount,
                                long seed) {
        this.name = ValueUtils.requireNotBlank(name, "基准数据集名称");
        if (scale == null || rowCount <= 0L || dataColumnCount <= 0
                || Double.isNaN(errorRate) || errorRate < 0.0d || errorRate > 1.0d
                || partitionCount <= 0) {
            throw new IllegalArgumentException("基准数据集规模、错误率和分区必须有效");
        }
        this.scale = scale;
        this.rowCount = rowCount;
        this.dataColumnCount = dataColumnCount;
        this.errorRate = errorRate;
        this.partitionCount = partitionCount;
        this.seed = seed;
    }

    public String getName() { return name; }
    public BenchmarkScale getScale() { return scale; }
    public long getRowCount() { return rowCount; }
    public int getDataColumnCount() { return dataColumnCount; }
    public double getErrorRate() { return errorRate; }
    public int getPartitionCount() { return partitionCount; }
    public long getSeed() { return seed; }
}
