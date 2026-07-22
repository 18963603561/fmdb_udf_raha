package com.fiberhome.ml.raha.data.profile;

import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * 使用 Spark 聚合生成空值、不同值、长度、数值分位数和字符类型画像。
 */
public final class ColumnProfiler {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnProfiler.class);
    /** 单列最多采集的高频值哈希数量。 */
    private static final int MAX_VALUE_FREQUENCY_COUNT =
            RahaDefaultConfigProvider.factory().profileMaxValueFrequencyCount();
    /** Spark 近似分位数计算精度。 */
    private static final int QUANTILE_ACCURACY =
            RahaDefaultConfigProvider.factory().profileQuantileAccuracy();
    /** 支持整数和小数的数值文本模式。 */
    private static final String NUMERIC_PATTERN = "^[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)$";
    /** 整数文本模式。 */
    private static final String INTEGER_PATTERN = "^[+-]?[0-9]+$";
    /** 纯字母文本模式。 */
    private static final String LETTER_PATTERN = "^[\\p{L}]+$";
    /** 只包含字母和数字的文本模式。 */
    private static final String ALPHANUMERIC_PATTERN = "^[\\p{L}\\p{N}]+$";

    /**
     * 为数据集全部字段生成列画像。
     *
     * @param dataset 已加载且只读的数据集
     * @return 按字段名称索引的画像
     */
    public Map<String, ColumnProfile> profile(RahaDataset dataset) {
        return profile(dataset, false);
    }

    /**
     * 只为当前任务可检测字段生成列画像，供字段白名单和列批工作流使用。
     *
     * @param dataset 已加载且只读的数据集
     * @return 当前可检测字段画像
     */
    public Map<String, ColumnProfile> profileDetectable(RahaDataset dataset) {
        return profile(dataset, true);
    }

    private Map<String, ColumnProfile> profile(RahaDataset dataset,
                                               boolean detectableOnly) {
        if (dataset == null || dataset.getDataFrame() == null) {
            throw new IllegalArgumentException("列画像要求已绑定 Spark 数据集");
        }
        List<ColumnMetadata> targetColumns =
                new ArrayList<ColumnMetadata>();
        LOGGER.info("开始生成列画像，datasetId={}，snapshotId={}，"
                        + "columnCount={}，detectableOnly={}",
                dataset.getDatasetId(), dataset.getSnapshotId(),
                dataset.getColumns().size(), Boolean.valueOf(detectableOnly));
        for (ColumnMetadata column : dataset.getColumns()) {
            // 字段白名单和列批范围通过可检测属性下推，禁止重复画像非当前批次字段。
            if (detectableOnly && !column.isDetectable()) {
                continue;
            }
            targetColumns.add(column);
        }
        Map<String, ColumnProfile> profiles = profileColumns(
                dataset.getDataFrame(), targetColumns);
        for (ColumnProfile profile : profiles.values()) {
            LOGGER.debug("列画像生成完成，columnName={}，nullCount={}，distinctCount={}，numericRatio={}",
                    profile.getColumnName(), profile.getNullCount(),
                    profile.getDistinctCount(), profile.getNumericRatio());
        }
        LOGGER.info("列画像全部生成完成，datasetId={}，profileCount={}",
                dataset.getDatasetId(), profiles.size());
        return Collections.unmodifiableMap(profiles);
    }

    /**
     * 将多个字段统一展开为长表后执行两次分布式聚合，避免宽表逐列提交 Spark 作业。
     */
    private Map<String, ColumnProfile> profileColumns(
            Dataset<Row> dataFrame,
            List<ColumnMetadata> columns) {
        if (columns.isEmpty()) {
            return Collections.emptyMap();
        }
        Dataset<Row> values = longValues(dataFrame, columns);
        List<Row> aggregateRows = values.groupBy(col("column_name")).agg(
                count(lit(1)).alias("total_count"),
                sum(when(col("text_value").isNull(), 1L).otherwise(0L)).alias("null_count"),
                sum(when(col("text_value").isNotNull()
                        .and(trim(col("text_value")).equalTo("")), 1L).otherwise(0L)).alias("blank_count"),
                countDistinct(col("text_value")).alias("distinct_count"),
                min(length(col("text_value"))).alias("min_length"),
                max(length(col("text_value"))).alias("max_length"),
                avg(length(col("text_value"))).alias("average_length"),
                count(col("numeric_value")).alias("numeric_count"),
                min(col("numeric_value")).alias("numeric_min"),
                max(col("numeric_value")).alias("numeric_max"),
                avg(col("numeric_value")).alias("numeric_mean"),
                expr("stddev_pop(numeric_value)").alias("numeric_stddev"),
                expr("percentile_approx(numeric_value, array(0.25, 0.5, 0.75), "
                        + QUANTILE_ACCURACY + ")")
                        .alias("numeric_quantiles"),
                sum(countWhen(matches("text_value", INTEGER_PATTERN))).alias("integer_count"),
                sum(countWhen(matches("text_value", NUMERIC_PATTERN)
                        .and(not(matches("text_value", INTEGER_PATTERN))))).alias("decimal_count"),
                sum(countWhen(matches("text_value", LETTER_PATTERN))).alias("letter_count"),
                sum(countWhen(matches("text_value", ALPHANUMERIC_PATTERN)
                        .and(not(matches("text_value", LETTER_PATTERN)))
                        .and(not(matches("text_value", NUMERIC_PATTERN)))))
                        .alias("alphanumeric_count"),
                sum(countWhen(col("text_value").isNotNull()
                        .and(trim(col("text_value")).notEqual(""))
                        .and(not(matches("text_value", NUMERIC_PATTERN)))
                        .and(not(matches("text_value", ALPHANUMERIC_PATTERN)))))
                        .alias("mixed_count"),
                sum(contains("text_value", ".*[0-9].*")).alias("has_digit_count"),
                sum(contains("text_value", ".*[\\p{L}].*")).alias("has_letter_count"),
                sum(contains("text_value", ".*\\s.*")).alias("has_space_count"),
                sum(contains("text_value", ".*[^\\p{L}\\p{N}\\s].*"))
                        .alias("has_symbol_count")
        ).collectAsList();
        Map<String, Row> aggregateByColumn = new LinkedHashMap<String, Row>();
        for (Row row : aggregateRows) {
            aggregateByColumn.put(row.getAs("column_name"), row);
        }

        Dataset<Row> frequencyCounts = values
                .filter(col("text_value").isNotNull())
                .groupBy(col("column_name"),
                        org.apache.spark.sql.functions.md5(col("text_value"))
                                .alias("value_hash"))
                .count();
        List<Row> frequencyRows = frequencyCounts
                .withColumn("frequency_rank",
                        org.apache.spark.sql.functions.row_number().over(
                                Window.partitionBy(col("column_name"))
                                        .orderBy(col("count").desc(),
                                                col("value_hash").asc())))
                .filter(col("frequency_rank").leq(MAX_VALUE_FREQUENCY_COUNT))
                .orderBy(col("column_name").asc(),
                        col("frequency_rank").asc())
                .collectAsList();
        Map<String, Map<String, Long>> frequenciesByColumn =
                new LinkedHashMap<String, Map<String, Long>>();
        for (Row frequencyRow : frequencyRows) {
            String columnName = frequencyRow.getAs("column_name");
            Map<String, Long> frequencies = frequenciesByColumn.get(columnName);
            if (frequencies == null) {
                frequencies = new LinkedHashMap<String, Long>();
                frequenciesByColumn.put(columnName, frequencies);
            }
            frequencies.put((String) frequencyRow.getAs("value_hash"),
                    ((Number) frequencyRow.getAs("count")).longValue());
        }

        Map<String, ColumnProfile> profiles =
                new LinkedHashMap<String, ColumnProfile>();
        for (ColumnMetadata column : columns) {
            String columnName = column.getName();
            Map<String, Long> frequencies = frequenciesByColumn.get(columnName);
            profiles.put(columnName, toProfile(columnName,
                    aggregateByColumn.get(columnName), frequencies == null
                            ? Collections.<String, Long>emptyMap() : frequencies));
        }
        return Collections.unmodifiableMap(profiles);
    }

    private static Dataset<Row> longValues(Dataset<Row> dataFrame,
                                           List<ColumnMetadata> columns) {
        StringBuilder expression = new StringBuilder("stack(")
                .append(columns.size());
        for (ColumnMetadata column : columns) {
            expression.append(", '")
                    .append(column.getName().replace("'", "''"))
                    .append("', cast(`")
                    .append(column.getName().replace("`", "``"))
                    .append("` as string)");
        }
        expression.append(") as (column_name, text_value)");
        Dataset<Row> values = dataFrame.selectExpr(expression.toString());
        Column trimmed = trim(col("text_value"));
        return values.withColumn("numeric_value",
                when(trimmed.rlike(NUMERIC_PATTERN), trimmed.cast("double"))
                        .otherwise(lit(null).cast("double")));
    }

    private static ColumnProfile toProfile(String columnName,
                                           Row aggregate,
                                           Map<String, Long> valueHashFrequencies) {
        if (aggregate == null) {
            return emptyProfile(columnName, valueHashFrequencies);
        }
        long totalCount = longValue(aggregate, "total_count");
        long nullCount = longValue(aggregate, "null_count");
        long numericCount = longValue(aggregate, "numeric_count");
        List<Double> quantiles = aggregate.isNullAt(aggregate.fieldIndex("numeric_quantiles"))
                ? Collections.<Double>emptyList()
                : aggregate.<Double>getList(aggregate.fieldIndex("numeric_quantiles"));
        Double q1 = quantiles.size() > 0 ? quantiles.get(0) : null;
        Double median = quantiles.size() > 1 ? quantiles.get(1) : null;
        Double q3 = quantiles.size() > 2 ? quantiles.get(2) : null;
        long nonNullCount = totalCount - nullCount;

        Map<String, Long> typeCounts = new LinkedHashMap<String, Long>();
        typeCounts.put("NULL", nullCount);
        typeCounts.put("BLANK", longValue(aggregate, "blank_count"));
        typeCounts.put("INTEGER", longValue(aggregate, "integer_count"));
        typeCounts.put("DECIMAL", longValue(aggregate, "decimal_count"));
        typeCounts.put("LETTER", longValue(aggregate, "letter_count"));
        typeCounts.put("ALPHANUMERIC", longValue(aggregate, "alphanumeric_count"));
        typeCounts.put("MIXED", longValue(aggregate, "mixed_count"));
        typeCounts.put("HAS_DIGIT", longValue(aggregate, "has_digit_count"));
        typeCounts.put("HAS_LETTER", longValue(aggregate, "has_letter_count"));
        typeCounts.put("HAS_SPACE", longValue(aggregate, "has_space_count"));
        typeCounts.put("HAS_SYMBOL", longValue(aggregate, "has_symbol_count"));

        return new ColumnProfile(columnName, totalCount, nullCount,
                longValue(aggregate, "blank_count"), longValue(aggregate, "distinct_count"),
                intValueOrDefault(aggregate, "min_length", -1),
                intValueOrDefault(aggregate, "max_length", -1),
                doubleValueOrDefault(aggregate, "average_length", 0.0d),
                numericCount, nonNullCount == 0L ? 0.0d : (double) numericCount / nonNullCount,
                doubleValueOrNull(aggregate, "numeric_min"),
                doubleValueOrNull(aggregate, "numeric_max"),
                doubleValueOrNull(aggregate, "numeric_mean"),
                doubleValueOrNull(aggregate, "numeric_stddev"),
                q1, median, q3, typeCounts, valueHashFrequencies);
    }

    private static ColumnProfile emptyProfile(
            String columnName,
            Map<String, Long> valueHashFrequencies) {
        Map<String, Long> typeCounts = new LinkedHashMap<String, Long>();
        typeCounts.put("NULL", 0L);
        typeCounts.put("BLANK", 0L);
        typeCounts.put("INTEGER", 0L);
        typeCounts.put("DECIMAL", 0L);
        typeCounts.put("LETTER", 0L);
        typeCounts.put("ALPHANUMERIC", 0L);
        typeCounts.put("MIXED", 0L);
        typeCounts.put("HAS_DIGIT", 0L);
        typeCounts.put("HAS_LETTER", 0L);
        typeCounts.put("HAS_SPACE", 0L);
        typeCounts.put("HAS_SYMBOL", 0L);
        return new ColumnProfile(columnName, 0L, 0L, 0L, 0L,
                -1, -1, 0.0d, 0L, 0.0d,
                null, null, null, null, null, null, null,
                typeCounts, valueHashFrequencies);
    }

    private static Column matches(String columnName, String pattern) {
        return col(columnName).isNotNull().and(trim(col(columnName)).rlike(pattern));
    }

    private static Column countWhen(Column condition) {
        return when(condition, 1L).otherwise(0L);
    }

    private static Column contains(String columnName, String pattern) {
        return when(col(columnName).isNotNull().and(col(columnName).rlike(pattern)),
                1L).otherwise(0L);
    }

    private static long longValue(Row row, String fieldName) {
        Object value = row.get(row.fieldIndex(fieldName));
        return value == null ? 0L : ((Number) value).longValue();
    }

    private static int intValueOrDefault(Row row, String fieldName, int defaultValue) {
        Object value = row.get(row.fieldIndex(fieldName));
        return value == null ? defaultValue : ((Number) value).intValue();
    }

    private static double doubleValueOrDefault(Row row, String fieldName, double defaultValue) {
        Object value = row.get(row.fieldIndex(fieldName));
        return value == null ? defaultValue : ((Number) value).doubleValue();
    }

    private static Double doubleValueOrNull(Row row, String fieldName) {
        Object value = row.get(row.fieldIndex(fieldName));
        return value == null ? null : ((Number) value).doubleValue();
    }
}
