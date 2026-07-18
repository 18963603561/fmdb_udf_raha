package com.fiberhome.ml.raha.strategy.impl.pvd;

import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.strategy.api.DetectionStrategy;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.execution.SparkStrategySupport;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionContext;
import com.fiberhome.ml.raha.strategy.plan.StrategyCandidate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * 识别少数值类型，并支持日期、时间、电话、邮箱和编号格式检测。
 */
public final class TypeFormatStrategy implements DetectionStrategy {

    /** 受控格式名称到正则表达式的映射。 */
    private static final Map<String, String> FORMAT_PATTERNS = createFormatPatterns();

    @Override
    public String getStrategyType() {
        return StrategyTypes.PVD_TYPE_FORMAT;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.PVD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        double minorityRatio = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.MINORITY_RATIO);
        double formatMinRatio = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.FORMAT_MIN_RATIO);
        String configuredFormat = SparkStrategySupport.required(
                context.getPlan(), StrategyConfigurationKeys.FORMAT_TYPE).toUpperCase(Locale.ROOT);
        if (minorityRatio < 0.0d || minorityRatio > 1.0d
                || formatMinRatio < 0.0d || formatMinRatio > 1.0d) {
            throw new IllegalArgumentException("类型和格式比例必须位于 0 到 1 之间");
        }
        Dataset<Row> values = SparkStrategySupport.values(context)
                .filter(col("raw_value").isNotNull()
                        .and(trim(col("text_value")).notEqual("")))
                .withColumn("value_type", typeExpression(trim(col("text_value"))));
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>();
        addTypeCandidates(context, values, minorityRatio, candidates);
        addFormatCandidates(context, values, configuredFormat, formatMinRatio, candidates);
        return candidates;
    }

    private static void addTypeCandidates(StrategyExecutionContext context,
                                          Dataset<Row> values,
                                          double minorityRatio,
                                          List<StrategyCandidate> candidates) {
        List<Row> typeRows = values.groupBy("value_type").count().collectAsList();
        long total = 0L;
        long dominantCount = -1L;
        String dominantType = null;
        for (Row row : typeRows) {
            long count = row.getLong(1);
            total += count;
            String type = row.getString(0);
            if (count > dominantCount
                    || (count == dominantCount && dominantType != null
                    && type.compareTo(dominantType) < 0)) {
                dominantCount = count;
                dominantType = type;
            }
        }
        if (total == 0L || typeRows.size() <= 1) {
            return;
        }
        List<String> minorityTypes = new ArrayList<String>();
        for (Row row : typeRows) {
            long count = row.getLong(1);
            if (count < dominantCount && (double) count / total <= minorityRatio) {
                minorityTypes.add(row.getString(0));
            }
        }
        if (minorityTypes.isEmpty()) {
            return;
        }
        Dataset<Row> counts = values.groupBy("value_type").count()
                .withColumnRenamed("count", "type_count");
        List<Row> rows = values.join(counts, "value_type")
                .filter(col("value_type").isin(minorityTypes.toArray()))
                .select("row_id", "value_hash", "value_type", "type_count")
                .collectAsList();
        for (Row row : rows) {
            long count = ((Number) row.getAs("type_count")).longValue();
            double ratio = (double) count / total;
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    "PVD_TYPE_MISMATCH",
                    SparkStrategySupport.details("actualType", row.getAs("value_type"),
                            "dominantType", dominantType, "actualRatio", String.valueOf(ratio)),
                    SparkStrategySupport.boundedScore(1.0d - ratio)));
        }
    }

    private static void addFormatCandidates(StrategyExecutionContext context,
                                            Dataset<Row> values,
                                            String configuredFormat,
                                            double formatMinRatio,
                                            List<StrategyCandidate> candidates) {
        boolean automatic = "AUTO".equals(configuredFormat);
        String effectiveFormat = automatic
                ? inferFormat(context.getColumnName()) : configuredFormat;
        if (effectiveFormat == null) {
            return;
        }
        String pattern = FORMAT_PATTERNS.get(effectiveFormat);
        if (pattern == null) {
            throw new IllegalArgumentException("不支持的格式类型：" + effectiveFormat);
        }
        long total = values.count();
        if (total == 0L) {
            return;
        }
        long matched = values.filter(trim(col("text_value")).rlike(pattern)).count();
        double matchedRatio = (double) matched / total;
        // 自动格式只有达到适用阈值才生成候选，避免字段名称误导导致整列误报。
        if (automatic && matchedRatio < formatMinRatio) {
            return;
        }
        List<Row> rows = values.filter(not(trim(col("text_value")).rlike(pattern)))
                .select("row_id", "value_hash")
                .collectAsList();
        for (Row row : rows) {
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    "PVD_FORMAT_MISMATCH",
                    SparkStrategySupport.details("expectedFormat", effectiveFormat,
                            "formatMatchRatio", String.valueOf(matchedRatio)),
                    SparkStrategySupport.boundedScore(Math.max(0.5d, matchedRatio))));
        }
    }

    private static Column typeExpression(Column text) {
        return when(text.rlike("^[+-]?[0-9]+$"), lit("INTEGER"))
                .when(text.rlike(SparkStrategySupport.NUMERIC_PATTERN), lit("DECIMAL"))
                .when(text.rlike("^[A-Za-z]+$"), lit("LATIN"))
                .when(text.rlike("^[\\x{4E00}-\\x{9FFF}]+$"), lit("CHINESE"))
                .when(text.rlike("^[\\p{L}\\p{N}]+$"), lit("ALPHANUMERIC"))
                .otherwise(lit("MIXED"));
    }

    private static String inferFormat(String columnName) {
        String normalized = columnName.toLowerCase(Locale.ROOT);
        if (normalized.contains("email") || normalized.contains("mail")) {
            return "EMAIL";
        }
        if (normalized.contains("phone") || normalized.contains("mobile")
                || normalized.contains("tel")) {
            return "PHONE";
        }
        if (normalized.contains("date") || normalized.endsWith("day")) {
            return "DATE";
        }
        if (normalized.contains("time")) {
            return "TIME";
        }
        if (normalized.endsWith("id") || normalized.endsWith("code")
                || normalized.endsWith("number") || normalized.endsWith("no")) {
            return "IDENTIFIER";
        }
        return null;
    }

    private static Map<String, String> createFormatPatterns() {
        Map<String, String> patterns = new LinkedHashMap<String, String>();
        patterns.put("DATE", "^(?:[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}|[0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{4})$");
        patterns.put("TIME", "^(?:[01][0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9])?$");
        patterns.put("PHONE", "^\\+?[0-9][0-9() -]{5,19}$");
        patterns.put("EMAIL", "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
        patterns.put("IDENTIFIER", "^[A-Za-z]{1,20}[-_]?[0-9]{1,30}$");
        return patterns;
    }
}
