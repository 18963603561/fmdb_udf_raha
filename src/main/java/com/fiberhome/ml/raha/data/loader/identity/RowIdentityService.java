package com.fiberhome.ml.raha.data.loader.identity;

import com.fiberhome.ml.raha.data.loader.validation.DataValidationErrorCode;
import com.fiberhome.ml.raha.data.loader.validation.DataValidationException;
import com.fiberhome.ml.raha.util.HashUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 为任意 Spark 输入生成稳定行指纹、逻辑行标识并执行确定性去重。
 */
public final class RowIdentityService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RowIdentityService.class);
    /** 冲突调试日志最多记录的安全行标识数量。 */
    private static final int MAX_CONFLICT_SAMPLES = 10;

    /**
     * 生成行身份并折叠普通重复行。
     *
     * @param source 尚未添加 Raha 技术字段的输入数据
     * @param config 行身份配置
     * @return 去重数据及批次指标
     */
    public RowIdentityResult identify(Dataset<Row> source,
                                      RowIdentityConfig config) {
        if (source == null || config == null) {
            throw new IllegalArgumentException("行身份输入和配置不能为空");
        }
        StructType businessSchema = businessSchema(source.schema());
        validateKeyColumns(businessSchema, config);
        long sourceCount = source.count();
        if (sourceCount <= 0L) {
            throw new DataValidationException(DataValidationErrorCode.EMPTY_DATASET,
                    "输入数据不能为空");
        }
        if (config.getMode() == RowIdentityMode.SOURCE_KEY) {
            validateKeyValues(source, config.getKeyColumns());
        }
        LOGGER.info("开始生成逻辑行身份，mode={}，businessColumnCount={}，"
                        + "sourceRowCount={}，algorithm={}，normalizationVersion={}",
                config.getMode(), businessSchema.fields().length, sourceCount,
                config.getFingerprintAlgorithm().getStandardName(),
                config.getNormalizationVersion());

        StructType fingerprintSchema = sortedSchema(businessSchema);
        Dataset<Row> identified = source.withColumn(
                RowIdentityColumns.ROW_CONTENT_HASH,
                hashColumn(source, fingerprintSchema, config));
        // 无主键时逻辑行标识就是内容哈希；业务键模式单独对键字段序列化。
        if (config.getMode() == RowIdentityMode.CONTENT_HASH) {
            identified = identified.withColumn(RowIdentityColumns.ROW_ID,
                    functions.col(RowIdentityColumns.ROW_CONTENT_HASH));
        } else {
            StructType keySchema = selectSchema(businessSchema,
                    config.getKeyColumns());
            identified = identified.withColumn(RowIdentityColumns.ROW_ID,
                    hashColumn(source, keySchema, config));
        }

        long logicalCount = identified.select(
                        functions.col(RowIdentityColumns.ROW_ID))
                .distinct().count();
        Dataset<Row> deduplicated;
        long conflictCount;
        // 业务键全局唯一时不构造宽表排序窗口，避免 Spark 约束推导随字段数急剧膨胀。
        if (logicalCount == sourceCount) {
            deduplicated = identified.withColumn(
                    RowIdentityColumns.DUPLICATE_COUNT, functions.lit(1L));
            conflictCount = 0L;
            LOGGER.info("逻辑行标识全局唯一，跳过窗口去重，sourceRowCount={}",
                    sourceCount);
        } else {
            WindowSpec groupWindow = Window.partitionBy(
                    functions.col(RowIdentityColumns.ROW_ID));
            List<Column> ordering = representativeOrdering(businessSchema);
            WindowSpec representativeWindow = groupWindow.orderBy(
                    ordering.toArray(new Column[0]));
            Dataset<Row> ranked = identified
                    .withColumn(RowIdentityColumns.DUPLICATE_COUNT,
                            functions.count(functions.lit(1L)).over(groupWindow))
                    .withColumn(RowIdentityColumns.REPRESENTATIVE_ORDER,
                            functions.row_number().over(representativeWindow));
            deduplicated = ranked.filter(functions.col(
                            RowIdentityColumns.REPRESENTATIVE_ORDER).equalTo(1))
                    .drop(RowIdentityColumns.REPRESENTATIVE_ORDER);
            conflictCount = config.getMode() == RowIdentityMode.SOURCE_KEY
                    ? countKeyConflicts(identified) : 0L;
        }
        long discardedCount = sourceCount - logicalCount;
        RowIdentityMetrics metrics = new RowIdentityMetrics(sourceCount,
                logicalCount, discardedCount, conflictCount);
        if (conflictCount > 0L) {
            LOGGER.warn("业务键存在内容冲突，按最小内容哈希选择稳定代表行，"
                            + "conflictKeyCount={}，sourceRowCount={}，logicalRowCount={}",
                    conflictCount, sourceCount, logicalCount);
            logConflictSamples(identified);
        }
        LOGGER.info("逻辑行身份生成完成，mode={}，sourceRowCount={}，"
                        + "logicalRowCount={}，discardedDuplicateCount={}，"
                        + "keyConflictCount={}", config.getMode(), sourceCount,
                logicalCount, discardedCount, conflictCount);
        return new RowIdentityResult(deduplicated, metrics);
    }

    /**
     * 返回不含 Raha 技术字段的固定顺序业务模式。
     */
    public StructType businessSchema(StructType sourceSchema) {
        if (sourceSchema == null || sourceSchema.fields().length == 0) {
            throw new IllegalArgumentException("输入数据模式不能为空");
        }
        List<StructField> fields = new ArrayList<StructField>();
        for (StructField field : sourceSchema.fields()) {
            // 输入数据不允许占用保留技术字段，避免覆盖用户业务值。
            if (RowIdentityColumns.isTechnical(field.name())) {
                throw new DataValidationException(
                        DataValidationErrorCode.RESERVED_COLUMN_CONFLICT,
                        "输入数据占用 Raha 保留技术字段：" + field.name());
            }
            fields.add(field);
        }
        return DataTypes.createStructType(fields);
    }

    private static StructType sortedSchema(StructType source) {
        List<StructField> fields = new ArrayList<StructField>(
                Arrays.asList(source.fields()));
        // 查询投影顺序不属于业务内容，整行指纹统一按字段名稳定排序。
        Collections.sort(fields, new Comparator<StructField>() {
            @Override
            public int compare(StructField first, StructField second) {
                return first.name().compareTo(second.name());
            }
        });
        return DataTypes.createStructType(fields);
    }

    private static Column hashColumn(Dataset<Row> source,
                                     StructType schema,
                                     RowIdentityConfig config) {
        final CanonicalRowSerializer serializer = new CanonicalRowSerializer(
                schema, config.getNormalizationVersion());
        UDF1<Row, String> hash = new UDF1<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) {
                return HashUtils.md5Hex(serializer.serialize(row));
            }
        };
        Column[] columns = new Column[schema.fields().length];
        for (int index = 0; index < schema.fields().length; index++) {
            columns[index] = quoted(schema.fields()[index].name());
        }
        return functions.udf(hash, DataTypes.StringType)
                .apply(functions.struct(columns));
    }

    private static void validateKeyColumns(StructType schema,
                                           RowIdentityConfig config) {
        Set<String> available = new LinkedHashSet<String>(
                Arrays.asList(schema.fieldNames()));
        for (String keyColumn : config.getKeyColumns()) {
            if (!available.contains(keyColumn)) {
                throw new DataValidationException(
                        DataValidationErrorCode.ROW_KEY_COLUMN_MISSING,
                        "输入数据不存在业务键字段：" + keyColumn);
            }
        }
    }

    private static StructType selectSchema(StructType source,
                                           List<String> columns) {
        List<StructField> fields = new ArrayList<StructField>();
        for (String column : columns) {
            fields.add(source.apply(column));
        }
        return DataTypes.createStructType(fields);
    }

    private static void validateKeyValues(Dataset<Row> source,
                                          List<String> keyColumns) {
        Column invalid = null;
        for (String keyColumn : keyColumns) {
            Column value = quoted(keyColumn);
            Column current = value.isNull().or(
                    functions.trim(value.cast("string")).equalTo(""));
            invalid = invalid == null ? current : invalid.or(current);
        }
        // 业务键任一组成字段为空都会破坏跨批次关联，必须在生成哈希前拒绝。
        if (invalid != null && source.filter(invalid).limit(1).count() > 0L) {
            throw new DataValidationException(
                    DataValidationErrorCode.ROW_ID_NULL_OR_BLANK,
                    "单字段或联合业务键包含空值或空白值");
        }
    }

    private static List<Column> representativeOrdering(StructType schema) {
        List<Column> columns = new ArrayList<Column>();
        columns.add(functions.col(RowIdentityColumns.ROW_CONTENT_HASH).asc());
        // 内容哈希相同只可能是逻辑等价行；原字段文本用于处理极低概率哈希碰撞。
        for (StructField field : schema.fields()) {
            columns.add(quoted(field.name()).cast("string").asc_nulls_first());
        }
        return columns;
    }

    private static long countKeyConflicts(Dataset<Row> identified) {
        return identified.groupBy(functions.col(RowIdentityColumns.ROW_ID))
                .agg(functions.countDistinct(functions.col(
                        RowIdentityColumns.ROW_CONTENT_HASH)).alias("content_count"))
                .filter(functions.col("content_count").gt(1L)).count();
    }

    private static void logConflictSamples(Dataset<Row> identified) {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        List<Row> samples = identified.groupBy(functions.col(
                        RowIdentityColumns.ROW_ID))
                .agg(functions.countDistinct(functions.col(
                        RowIdentityColumns.ROW_CONTENT_HASH)).alias("content_count"))
                .filter(functions.col("content_count").gt(1L))
                .select(RowIdentityColumns.ROW_ID, "content_count")
                .limit(MAX_CONFLICT_SAMPLES).collectAsList();
        for (Row sample : samples) {
            LOGGER.debug("业务键内容冲突样例，rowId={}，contentVersionCount={}",
                    sample.getString(0), sample.getLong(1));
        }
    }

    private static Column quoted(String columnName) {
        return functions.col("`" + columnName.replace("`", "``") + "`");
    }
}
