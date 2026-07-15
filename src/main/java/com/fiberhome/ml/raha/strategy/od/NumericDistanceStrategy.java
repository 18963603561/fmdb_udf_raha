package com.fiberhome.ml.raha.strategy.od;

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

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

/**
 * 使用画像均值和总体标准差识别数值距离离群候选。
 */
public final class NumericDistanceStrategy implements DetectionStrategy {

    @Override
    public String getStrategyType() {
        return StrategyTypes.OD_NUMERIC_DISTANCE;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.OD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        double mean = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.NUMERIC_MEAN);
        double standardDeviation = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.NUMERIC_STANDARD_DEVIATION);
        double threshold = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.Z_THRESHOLD);
        if (standardDeviation <= 0.0d || threshold <= 0.0d) {
            return java.util.Collections.emptyList();
        }
        Column text = trim(col("text_value"));
        Dataset<Row> values = SparkStrategySupport.values(context)
                .filter(text.rlike(SparkStrategySupport.NUMERIC_PATTERN))
                .withColumn("numeric_value", text.cast("double"))
                .withColumn("distance_score",
                        abs(col("numeric_value").minus(mean)).divide(standardDeviation));
        List<Row> rows = values.filter(col("distance_score").gt(threshold))
                .select("row_id", "value_hash", "distance_score")
                .collectAsList();
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>(rows.size());
        for (Row row : rows) {
            double distance = ((Number) row.getAs("distance_score")).doubleValue();
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    "OD_NUMERIC_DISTANCE",
                    SparkStrategySupport.details("standardDistance", String.valueOf(distance),
                            "threshold", String.valueOf(threshold)),
                    SparkStrategySupport.boundedScore(distance / (threshold * 2.0d))));
        }
        return candidates;
    }
}
