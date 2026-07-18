package com.fiberhome.ml.raha.strategy.impl.od;

import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.strategy.api.DetectionStrategy;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.execution.SparkStrategySupport;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionContext;
import com.fiberhome.ml.raha.strategy.plan.StrategyCandidate;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * 使用四分位距边界识别长尾数值候选。
 */
public final class QuantileOutlierStrategy implements DetectionStrategy {

    @Override
    public String getStrategyType() {
        return StrategyTypes.OD_QUANTILE;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.OD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        double q1 = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.NUMERIC_Q1);
        double q3 = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.NUMERIC_Q3);
        double multiplier = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.IQR_MULTIPLIER);
        double iqr = q3 - q1;
        if (iqr <= 0.0d || multiplier <= 0.0d) {
            return java.util.Collections.emptyList();
        }
        double lowerBound = q1 - multiplier * iqr;
        double upperBound = q3 + multiplier * iqr;
        Column text = trim(col("text_value"));
        Dataset<Row> values = SparkStrategySupport.values(context)
                .filter(text.rlike(SparkStrategySupport.NUMERIC_PATTERN))
                .withColumn("numeric_value", text.cast("double"))
                .withColumn("outside_distance", when(col("numeric_value").lt(lowerBound),
                        lit(lowerBound).minus(col("numeric_value")))
                        .otherwise(col("numeric_value").minus(upperBound)));
        List<Row> rows = values.filter(col("numeric_value").lt(lowerBound)
                        .or(col("numeric_value").gt(upperBound)))
                .select("row_id", "value_hash", "numeric_value", "outside_distance")
                .collectAsList();
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>(rows.size());
        for (Row row : rows) {
            double distance = ((Number) row.getAs("outside_distance")).doubleValue();
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    "OD_QUANTILE_OUTLIER",
                    SparkStrategySupport.details("lowerBound", String.valueOf(lowerBound),
                            "upperBound", String.valueOf(upperBound),
                            "outsideDistance", String.valueOf(distance)),
                    SparkStrategySupport.boundedScore(distance / (iqr * 3.0d))));
        }
        return candidates;
    }
}
