package com.fiberhome.ml.raha.data.loader.identity;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 返回已经生成技术行身份并完成逻辑去重的数据集和汇总指标。
 */
public final class RowIdentityResult {

    /** 包含三个稳定技术字段的去重数据集。 */
    private final Dataset<Row> dataFrame;
    /** 行身份和去重指标。 */
    private final RowIdentityMetrics metrics;

    public RowIdentityResult(Dataset<Row> dataFrame,
                             RowIdentityMetrics metrics) {
        if (dataFrame == null || metrics == null) {
            throw new IllegalArgumentException("行身份结果和指标不能为空");
        }
        this.dataFrame = dataFrame;
        this.metrics = metrics;
    }

    public Dataset<Row> getDataFrame() { return dataFrame; }
    public RowIdentityMetrics getMetrics() { return metrics; }
}
