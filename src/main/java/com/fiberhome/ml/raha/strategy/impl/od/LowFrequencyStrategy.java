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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * 按值哈希频率识别低频候选，不收集原始值。
 */
public final class LowFrequencyStrategy implements DetectionStrategy {

    @Override
    public String getStrategyType() {
        return StrategyTypes.OD_LOW_FREQUENCY;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.OD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        int maxFrequency = SparkStrategySupport.requiredInt(
                context.getPlan(), StrategyConfigurationKeys.MAX_FREQUENCY);
        if (maxFrequency <= 0) {
            throw new IllegalArgumentException("低频阈值必须大于 0");
        }
        Dataset<Row> values = SparkStrategySupport.values(context)
                .filter(col("raw_value").isNotNull());
        Dataset<Row> frequencies = values.groupBy("value_hash").count()
                .withColumnRenamed("count", "value_frequency");
        List<Row> rows = values.join(frequencies, "value_hash")
                .filter(col("value_frequency").leq(maxFrequency))
                .select("row_id", "value_hash", "value_frequency")
                .collectAsList();
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>(rows.size());
        for (Row row : rows) {
            long frequency = ((Number) row.getAs("value_frequency")).longValue();
            double score = SparkStrategySupport.boundedScore(
                    (double) (maxFrequency - frequency + 1L) / maxFrequency);
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    "OD_LOW_FREQUENCY",
                    SparkStrategySupport.details("frequency", String.valueOf(frequency),
                            "maxFrequency", String.valueOf(maxFrequency)), score));
        }
        return candidates;
    }
}
