package com.fiberhome.ml.raha.strategy.pvd;

import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.strategy.DetectionStrategy;
import com.fiberhome.ml.raha.strategy.SparkStrategySupport;
import com.fiberhome.ml.raha.strategy.StrategyCandidate;
import com.fiberhome.ml.raha.strategy.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.StrategyExecutionContext;
import com.fiberhome.ml.raha.strategy.StrategyTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.trim;

/**
 * 使用长度四分位距和少数长度分布识别异常候选。
 */
public final class LengthAnomalyStrategy implements DetectionStrategy {

    @Override
    public String getStrategyType() {
        return StrategyTypes.PVD_LENGTH;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.PVD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        double minorityRatio = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.MINORITY_RATIO);
        double multiplier = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.IQR_MULTIPLIER);
        Dataset<Row> values = SparkStrategySupport.values(context)
                .filter(col("raw_value").isNotNull().and(trim(col("text_value")).notEqual("")))
                .withColumn("value_length", length(col("text_value")));
        long total = values.count();
        if (total == 0L) {
            return java.util.Collections.emptyList();
        }
        double[] quantiles = values.stat().approxQuantile(
                "value_length", new double[]{0.25d, 0.75d}, 0.0d);
        double q1 = quantiles.length > 0 ? quantiles[0] : 0.0d;
        double q3 = quantiles.length > 1 ? quantiles[1] : q1;
        double iqr = q3 - q1;
        double lowerBound = q1 - multiplier * iqr;
        double upperBound = q3 + multiplier * iqr;

        List<Row> lengthCounts = values.groupBy("value_length").count().collectAsList();
        long dominantCount = -1L;
        for (Row row : lengthCounts) {
            dominantCount = Math.max(dominantCount, row.getLong(1));
        }
        Column condition = lit(false);
        if (iqr > 0.0d) {
            condition = col("value_length").lt(lowerBound).or(col("value_length").gt(upperBound));
        }
        for (Row row : lengthCounts) {
            long count = row.getLong(1);
            if (count < dominantCount && (double) count / total <= minorityRatio) {
                condition = condition.or(col("value_length").equalTo(row.getInt(0)));
            }
        }
        List<Row> rows = values.filter(condition)
                .select("row_id", "value_hash", "value_length")
                .collectAsList();
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>(rows.size());
        for (Row row : rows) {
            int actualLength = ((Number) row.getAs("value_length")).intValue();
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    "PVD_LENGTH_OUTLIER",
                    SparkStrategySupport.details("actualLength", String.valueOf(actualLength),
                            "lowerBound", String.valueOf(lowerBound),
                            "upperBound", String.valueOf(upperBound)), 1.0d));
        }
        return candidates;
    }
}
