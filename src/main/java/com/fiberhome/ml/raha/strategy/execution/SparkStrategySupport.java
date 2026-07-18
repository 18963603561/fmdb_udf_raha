package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.strategy.plan.StrategyCandidate;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.when;

/**
 * 提供基础策略共用的安全列引用、值哈希和配置读取能力。
 */
public final class SparkStrategySupport {

    /** 支持整数和小数的数值文本模式。 */
    public static final String NUMERIC_PATTERN = "^[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)$";

    private SparkStrategySupport() {
    }

    /**
     * 生成只包含行标识、值文本和值哈希的策略输入，不输出原始值到日志。
     *
     * @param context 策略上下文
     * @return 统一策略输入数据集
     */
    public static Dataset<Row> values(StrategyExecutionContext context) {
        Column raw = quotedColumn(context.getColumnName());
        Column text = raw.cast("string");
        Column hashInput = when(raw.isNull(), lit("<null>")).otherwise(text);
        return context.getDataset().getDataFrame().select(
                quotedColumn(context.getDataset().getRowIdColumn()).cast("string").alias("row_id"),
                raw.alias("raw_value"),
                text.alias("text_value"),
                sha2(hashInput, 256).alias("value_hash"));
    }

    public static Column quotedColumn(String columnName) {
        if (columnName == null || columnName.trim().isEmpty()) {
            throw new IllegalArgumentException("字段名称不能为空");
        }
        return col("`" + columnName.replace("`", "``") + "`");
    }

    public static int requiredInt(StrategyPlan plan, String key) {
        try {
            return Integer.parseInt(required(plan, key));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("策略整数配置非法：" + key, exception);
        }
    }

    public static double requiredDouble(StrategyPlan plan, String key) {
        try {
            double value = Double.parseDouble(required(plan, key));
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new NumberFormatException("非有限数值");
            }
            return value;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("策略数值配置非法：" + key, exception);
        }
    }

    public static String required(StrategyPlan plan, String key) {
        String value = plan.getConfiguration().get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("缺少策略配置：" + key);
        }
        return value;
    }

    public static Map<String, String> details(String... values) {
        if (values == null || values.length % 2 != 0) {
            throw new IllegalArgumentException("原因详情必须使用键值对");
        }
        Map<String, String> details = new LinkedHashMap<String, String>();
        for (int index = 0; index < values.length; index += 2) {
            details.put(values[index], values[index + 1]);
        }
        return details;
    }

    public static StrategyCandidate candidate(Row row,
                                              String columnName,
                                              String reasonCode,
                                              Map<String, String> details,
                                              Double score) {
        return new StrategyCandidate(row.getAs("row_id"), columnName,
                row.getAs("value_hash"), reasonCode, details, score);
    }

    public static double boundedScore(double value) {
        return Math.max(0.0d, Math.min(1.0d, value));
    }
}
