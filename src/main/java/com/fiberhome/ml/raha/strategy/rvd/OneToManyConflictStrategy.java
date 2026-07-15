package com.fiberhome.ml.raha.strategy.rvd;

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
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.trim;

/**
 * 检测同一左值对应多个右值的一对多依赖冲突。
 */
public final class OneToManyConflictStrategy implements DetectionStrategy {

    @Override
    public String getStrategyType() {
        return StrategyTypes.RVD_ONE_TO_MANY;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.RVD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        String leftColumn = SparkStrategySupport.required(
                context.getPlan(), StrategyConfigurationKeys.LEFT_COLUMN);
        String rightColumn = SparkStrategySupport.required(
                context.getPlan(), StrategyConfigurationKeys.RIGHT_COLUMN);
        if (leftColumn.equals(rightColumn)
                || !context.getPlan().getTargetColumns().equals(
                java.util.Arrays.asList(leftColumn, rightColumn))) {
            throw new IllegalArgumentException("RVD 依赖方向与目标字段不一致");
        }
        Column leftRaw = SparkStrategySupport.quotedColumn(leftColumn);
        Column rightRaw = SparkStrategySupport.quotedColumn(rightColumn);
        Column leftText = leftRaw.cast("string");
        Column rightText = rightRaw.cast("string");
        Dataset<Row> values = context.getDataset().getDataFrame().select(
                SparkStrategySupport.quotedColumn(context.getDataset().getRowIdColumn())
                        .cast("string").alias("row_id"),
                leftRaw.alias("left_raw"), rightRaw.alias("right_raw"),
                leftText.alias("left_text"), rightText.alias("right_text"),
                sha2(leftText, 256).alias("left_hash"),
                sha2(rightText, 256).alias("right_hash"))
                .filter(col("left_raw").isNotNull().and(col("right_raw").isNotNull())
                        .and(trim(col("left_text")).notEqual(""))
                        .and(trim(col("right_text")).notEqual("")));
        Dataset<Row> conflictGroups = values.groupBy("left_hash")
                .agg(countDistinct("right_hash").alias("distinct_right_count"),
                        count(lit(1)).alias("group_size"))
                .filter(col("distinct_right_count").gt(1L));
        List<Row> rows = values.join(conflictGroups, "left_hash")
                .select("row_id", "left_hash", "right_hash",
                        "distinct_right_count", "group_size")
                .collectAsList();
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>(rows.size() * 2);
        for (Row row : rows) {
            long distinctRightCount = ((Number) row.getAs("distinct_right_count")).longValue();
            long groupSize = ((Number) row.getAs("group_size")).longValue();
            double score = SparkStrategySupport.boundedScore(
                    1.0d - 1.0d / distinctRightCount);
            String dependency = leftColumn + "->" + rightColumn;
            candidates.add(new StrategyCandidate(row.getAs("row_id"), leftColumn,
                    row.getAs("left_hash"), "RVD_ONE_TO_MANY_CONFLICT",
                    SparkStrategySupport.details("dependency", dependency,
                            "targetSide", "LEFT", "distinctRightCount",
                            String.valueOf(distinctRightCount), "groupSize", String.valueOf(groupSize)),
                    score));
            candidates.add(new StrategyCandidate(row.getAs("row_id"), rightColumn,
                    row.getAs("right_hash"), "RVD_ONE_TO_MANY_CONFLICT",
                    SparkStrategySupport.details("dependency", dependency,
                            "targetSide", "RIGHT", "distinctRightCount",
                            String.valueOf(distinctRightCount), "groupSize", String.valueOf(groupSize)),
                    score));
        }
        return candidates;
    }
}
