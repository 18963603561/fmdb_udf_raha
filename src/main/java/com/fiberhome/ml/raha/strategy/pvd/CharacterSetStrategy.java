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
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * 根据数字、拉丁字母、中文、空格和符号组合识别少数字符模式。
 */
public final class CharacterSetStrategy implements DetectionStrategy {

    @Override
    public String getStrategyType() {
        return StrategyTypes.PVD_CHARACTER_SET;
    }

    @Override
    public StrategyFamily getStrategyFamily() {
        return StrategyFamily.PVD;
    }

    @Override
    public List<StrategyCandidate> detect(StrategyExecutionContext context) {
        double minorityRatio = SparkStrategySupport.requiredDouble(
                context.getPlan(), StrategyConfigurationKeys.MINORITY_RATIO);
        if (minorityRatio < 0.0d || minorityRatio > 1.0d) {
            throw new IllegalArgumentException("少数字符模式比例必须位于 0 到 1 之间");
        }
        Column text = col("text_value");
        Column signature = concat(
                flag(text.rlike(".*[0-9].*"), "D"),
                flag(text.rlike(".*[A-Za-z].*"), "L"),
                flag(text.rlike(".*[\\x{4E00}-\\x{9FFF}].*"), "C"),
                flag(text.rlike(".*\\s.*"), "S"),
                flag(text.rlike(".*[^\\p{L}\\p{N}\\s].*"), "P"));
        Dataset<Row> values = SparkStrategySupport.values(context)
                .filter(col("raw_value").isNotNull().and(trim(text).notEqual("")))
                .withColumn("character_signature", signature);
        List<Row> patternRows = values.groupBy("character_signature").count().collectAsList();
        long total = 0L;
        long dominantCount = -1L;
        String dominantSignature = null;
        for (Row row : patternRows) {
            long count = row.getLong(1);
            total += count;
            if (count > dominantCount
                    || (count == dominantCount && row.getString(0).compareTo(dominantSignature) < 0)) {
                dominantCount = count;
                dominantSignature = row.getString(0);
            }
        }
        if (total == 0L || patternRows.size() <= 1) {
            return java.util.Collections.emptyList();
        }
        List<String> minoritySignatures = new ArrayList<String>();
        for (Row row : patternRows) {
            long count = row.getLong(1);
            if (count < dominantCount && (double) count / total <= minorityRatio) {
                minoritySignatures.add(row.getString(0));
            }
        }
        if (minoritySignatures.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        Dataset<Row> counts = values.groupBy("character_signature").count()
                .withColumnRenamed("count", "signature_count");
        List<Row> rows = values.join(counts, "character_signature")
                .filter(col("character_signature").isin(minoritySignatures.toArray()))
                .select("row_id", "value_hash", "character_signature", "signature_count")
                .collectAsList();
        List<StrategyCandidate> candidates = new ArrayList<StrategyCandidate>(rows.size());
        for (Row row : rows) {
            long count = ((Number) row.getAs("signature_count")).longValue();
            double ratio = (double) count / total;
            candidates.add(SparkStrategySupport.candidate(row, context.getColumnName(),
                    "PVD_MINOR_CHARACTER_SET",
                    SparkStrategySupport.details("actualSignature", row.getAs("character_signature"),
                            "dominantSignature", dominantSignature,
                            "actualRatio", String.valueOf(ratio)),
                    SparkStrategySupport.boundedScore(1.0d - ratio)));
        }
        return candidates;
    }

    private static Column flag(Column condition, String token) {
        return when(condition, lit(token)).otherwise(lit(""));
    }
}
