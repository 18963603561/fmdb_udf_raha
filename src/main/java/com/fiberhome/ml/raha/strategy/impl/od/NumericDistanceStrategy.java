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
