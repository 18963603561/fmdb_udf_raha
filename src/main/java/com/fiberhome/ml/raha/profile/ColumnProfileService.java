package com.fiberhome.ml.raha.profile;

import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * 基于 Spark 聚合生成轻量列画像，不收集全量输入。
 */
public final class ColumnProfileService implements ColumnProfiler {

    @Override
    public List<ColumnProfile> profile(RahaDataset dataset) {
        List<ColumnProfile> profiles = new ArrayList<ColumnProfile>();
        for (String columnName : dataset.getTargetColumns()) {
            Column text = coalesce(col(columnName).cast("string"), lit(""));
            Column weight = col(RahaDataset.DUPLICATE_COUNT);
            Row row = dataset.getRows().agg(
                    sum(weight).alias("row_count"),
                    countDistinct(text).alias("distinct_count"),
                    sum(when(trim(text).equalTo(""), weight).otherwise(lit(0L)))
                            .alias("missing_count"),
                    sum(when(text.rlike("[-+]?\\d+(\\.\\d+)?"), weight)
                            .otherwise(lit(0L))).alias("numeric_count"),
                    avg(length(text)).alias("average_length"))
                    .first();
            profiles.add(new ColumnProfile(columnName,
                    number(row, "row_count").longValue(),
                    number(row, "distinct_count").longValue(),
                    number(row, "missing_count").longValue(),
                    number(row, "numeric_count").longValue(),
                    number(row, "average_length").doubleValue()));
        }
        return profiles;
    }

    private static Number number(Row row, String field) {
        Object value = row.getAs(field);
        return value == null ? Integer.valueOf(0) : (Number) value;
    }
}
