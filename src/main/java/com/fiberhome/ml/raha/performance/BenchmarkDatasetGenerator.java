package com.fiberhome.ml.raha.performance;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 Spark 惰性表达式生成可重复的规模、宽度和错误率基准数据。
 */
public final class BenchmarkDatasetGenerator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BenchmarkDatasetGenerator.class);
    /** 错误率离散计算精度。 */
    private static final int ERROR_BUCKET_COUNT = 1000000;
    /** Spark 会话。 */
    private final SparkSession sparkSession;

    public BenchmarkDatasetGenerator(SparkSession sparkSession) {
        if (sparkSession == null) {
            throw new IllegalArgumentException("基准数据生成器 Spark 会话不能为空");
        }
        this.sparkSession = sparkSession;
    }

    /**
     * 生成包含 row_id 和 data_000 起始数据字段的数据集，错误值以 ERR 标记。
     */
    public Dataset<Row> generate(BenchmarkDatasetSpec spec) {
        if (spec == null) {
            throw new IllegalArgumentException("基准数据集定义不能为空");
        }
        LOGGER.info("开始生成性能基准数据，name={}，scale={}，rowCount={}，"
                        + "dataColumnCount={}，errorRate={}，partitionCount={}",
                spec.getName(), spec.getScale(), spec.getRowCount(),
                spec.getDataColumnCount(), spec.getErrorRate(),
                spec.getPartitionCount());
        Dataset<Row> frame = sparkSession.range(0L, spec.getRowCount(), 1L,
                        spec.getPartitionCount())
                .withColumnRenamed("id", "row_id");
        int threshold = (int) Math.round(spec.getErrorRate() * ERROR_BUCKET_COUNT);
        for (int index = 0; index < spec.getDataColumnCount(); index++) {
            String columnName = String.format("data_%03d", index);
            Column bucket = functions.pmod(functions.hash(
                    functions.col("row_id"), functions.lit(index),
                    functions.lit(spec.getSeed())), functions.lit(ERROR_BUCKET_COUNT));
            Column normalValue = functions.concat(functions.lit("V" + index + "_"),
                    functions.pmod(functions.col("row_id"), functions.lit(100)));
            Column errorValue = functions.concat(functions.lit("ERR_" + index + "_"),
                    functions.col("row_id"));
            // 每列独立使用同一错误率和不同列种子，形成可重复但不完全重叠的错误位置。
            frame = frame.withColumn(columnName,
                    functions.when(bucket.lt(threshold), errorValue)
                            .otherwise(normalValue));
        }
        return frame;
    }
}
