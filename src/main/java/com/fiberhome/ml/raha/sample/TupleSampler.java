package com.fiberhome.ml.raha.sample;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.support.JsonUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * 依据缺失、格式和低频覆盖度选择有限代表元组。
 */
public final class TupleSampler {

    /**
     * 在执行器端计算分数，只收集不超过预算的元组。
     *
     * @param dataset 算法数据集
     * @param budget 标注预算
     * @param batchId 采样批次标识
     * @param createdAt 创建时间
     * @param partitionDate 分区日期
     * @return 已选元组
     */
    public List<SampleTuple> sample(RahaDataset dataset, int budget, String batchId,
                                    long createdAt, String partitionDate) {
        Dataset<Row> scored = dataset.getRows().withColumn("__raha_score", lit(0.0d));
        for (String target : dataset.getTargetColumns()) {
            Column text = coalesce(col(target).cast("string"), lit(""));
            Column frequency = sum(col(RahaDataset.DUPLICATE_COUNT))
                    .over(Window.partitionBy(text));
            Column missing = when(trim(text).equalTo(""), lit(5.0d)).otherwise(lit(0.0d));
            Column mixedDigits = when(text.rlike(".*\\d.*")
                    .and(text.rlike("[-+]?\\d+(\\.\\d+)?").equalTo(false)), lit(1.5d))
                    .otherwise(lit(0.0d));
            scored = scored.withColumn("__raha_score",
                    col("__raha_score").plus(missing).plus(mixedDigits)
                            .plus(lit(1.0d).divide(frequency)));
        }
        List<Row> rows = scored.orderBy(col("__raha_score").desc(),
                        col(RahaDataset.ROW_ID).asc())
                .limit(budget)
                .select(RahaDataset.ROW_ID, RahaDataset.DUPLICATE_COUNT,
                        RahaDataset.ROW_JSON, "__raha_score")
                .collectAsList();
        List<SampleTuple> tuples = new ArrayList<SampleTuple>();
        int order = 1;
        for (Row row : rows) {
            Map<String, Object> reason = new LinkedHashMap<String, Object>();
            reason.put("coveredColumns", dataset.getTargetColumns());
            reason.put("score", row.getDouble(3));
            tuples.add(new SampleTuple(batchId, dataset.getDatasetId(),
                    dataset.getSnapshotId(), row.getString(0), row.getLong(1),
                    row.getString(2), order++, row.getDouble(3), JsonUtils.toJson(reason),
                    createdAt, partitionDate));
        }
        return tuples;
    }
}
