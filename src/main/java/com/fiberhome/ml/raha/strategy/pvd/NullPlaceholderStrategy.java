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
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.when;

/**
 * 区分空值、空白值和特殊占位值候选。
 */
public final class NullPlaceholderStrategy implements DetectionStrategy {

    @Override
    public String getStrategyType() {
        return StrategyTypes.PVD_NULL_PLACEHOLDER;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.PVD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        String[] placeholders = SparkStrategySupport.required(
                context.getPlan(), StrategyConfigurationKeys.PLACEHOLDERS).split(",");
        Dataset<Row> values = SparkStrategySupport.values(context);
        Column blank = col("raw_value").isNotNull().and(trim(col("text_value")).equalTo(""));
        Column placeholder = col("raw_value").isNotNull()
                .and(upper(trim(col("text_value"))).isin((Object[]) placeholders));
        Column reason = when(col("raw_value").isNull(), lit("PVD_NULL_VALUE"))
                .when(blank, lit("PVD_BLANK_VALUE"))
                .otherwise(lit("PVD_PLACEHOLDER_VALUE"));
        List<Row> rows = values.filter(col("raw_value").isNull().or(blank).or(placeholder))
                .withColumn("reason_code", reason)
                .select("row_id", "value_hash", "reason_code")
                .collectAsList();
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>(rows.size());
        for (Row row : rows) {
            String reasonCode = row.getAs("reason_code");
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    reasonCode, SparkStrategySupport.details("valueCategory", reasonCode), 1.0d));
        }
        return candidates;
    }
}
