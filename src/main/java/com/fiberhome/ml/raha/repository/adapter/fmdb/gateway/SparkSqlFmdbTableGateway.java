package com.fiberhome.ml.raha.repository.adapter.fmdb.gateway;

import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbSchemaInitializer;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Spark SQL Catalog 读写 FMDB 表，并在单个提交器内执行幂等追加。
 */
public final class SparkSqlFmdbTableGateway implements FmdbTableGateway {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SparkSqlFmdbTableGateway.class);
    /** 允许的库表标识格式。 */
    private static final Pattern TABLE_NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}");
    /** 允许的单字段标识格式。 */
    private static final Pattern COLUMN_NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*");
    /** FMDB 平台 Spark 会话。 */
    private final SparkSession sparkSession;
    /** 控制标准九张物理表是否允许写入。 */
    private final FmdbPersistenceConfig persistenceConfig;

    public SparkSqlFmdbTableGateway(SparkSession sparkSession) {
        this(sparkSession, FmdbPersistenceConfig.fromDefaults());
    }

    /**
     * 创建 FMDB 表网关并按配置初始化默认表。
     *
     * @param sparkSession FMDB 平台 Spark 会话
     * @param persistenceConfig 持久化和建表配置
     */
    public SparkSqlFmdbTableGateway(SparkSession sparkSession,
                                    FmdbPersistenceConfig persistenceConfig) {
        if (sparkSession == null) {
            throw new IllegalArgumentException("FMDB Spark 会话不能为空");
        }
        if (persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 持久化配置不能为空");
        }
        this.sparkSession = sparkSession;
        this.persistenceConfig = persistenceConfig;
        // 网关首次创建时初始化默认表，自定义表仍保留首次写入自动创建能力。
        new FmdbSchemaInitializer(sparkSession, persistenceConfig).initialize();
    }

    @Override
    public boolean tableExists(String tableName) {
        return sparkSession.catalog().tableExists(validateTableName(tableName));
    }

    @Override
    public Dataset<Row> read(String tableName) {
        String validated = validateTableName(tableName);
        LOGGER.debug("调用 FMDB Catalog 读取结果表，tableName={}", validated);
        return sparkSession.table(validated);
    }

    @Override
    public Dataset<Row> read(String tableName,
                             List<String> columns,
                             Column condition) {
        Dataset<Row> source = read(tableName);
        List<String> validatedColumns = validateProjection(source, columns);
        if (condition != null) {
            source = source.filter(condition);
        }
        LOGGER.debug("调用 FMDB Catalog 裁剪读取，tableName={}，columns={}，"
                        + "filtered={}", tableName, validatedColumns,
                condition != null);
        return source.selectExpr(validatedColumns.toArray(new String[0]));
    }

    @Override
    public synchronized long append(String tableName,
                                    Dataset<Row> rows,
                                    List<String> keyColumns,
                                    long expectedCount) {
        String validated = validateTableName(tableName);
        validateRowsAndKeys(rows, keyColumns);
        if (expectedCount < 0L) {
            throw new IllegalArgumentException("FMDB 预期写入行数不能小于 0");
        }
        FmdbPhysicalTable physicalTable = FmdbPhysicalTable.fromTableName(validated);
        // 标准九表在网关层再次执行开关，防止专用写入器遗漏判断。
        if (physicalTable != null && !persistenceConfig.shouldPersist(physicalTable)) {
            LOGGER.info("FMDB 物理表入库已关闭，跳过网关写入，tableName={}，configKey={}",
                    validated, physicalTable.getConfigKey());
            return 0L;
        }
        if (persistenceConfig.isDirectAppend()) {
            LOGGER.info("FMDB 全局写入模式为直接追加，跳过幂等主键扫描，tableName={}，keyColumns={}",
                    validated, keyColumns);
            return appendDirect(validated, rows, expectedCount);
        }
        return appendByKeyFilter(validated, rows, keyColumns);
    }

    private long appendByKeyFilter(String tableName,
                                   Dataset<Row> rows,
                                   List<String> keyColumns) {
        String validated = validateTableName(tableName);
        LOGGER.info("开始向 FMDB 表幂等写入，tableName={}，keyColumns={}",
                validated, keyColumns);
        try {
            Dataset<Row> pending = rows;
            boolean targetExists = tableExists(validated);
            if (targetExists) {
                Dataset<Row> existing = read(validated);
                if (!schemasCompatible(existing.schema(), rows.schema())) {
                    String difference = schemaDifference(existing.schema(), rows.schema());
                    throw new IllegalStateException("FMDB 目标表模式与写入模式不一致："
                            + validated + "，差异=" + difference);
                }
                // Spark 读取分区表时会把分区字段移动到末尾，追加写入前必须按目标表实际顺序重排。
                Dataset<Row> incoming = alignColumns(rows, existing.schema()).alias("incoming");
                Dataset<Row> scopedExisting = restrictExistingToIncomingPartitions(existing, rows);
                Dataset<Row> existingKeys = scopedExisting.selectExpr(
                        keyColumns.toArray(new String[0])).distinct().alias("existing");
                Column condition = null;
                for (String keyColumn : keyColumns) {
                    Column keyCondition = incoming.col(keyColumn)
                            .eqNullSafe(existingKeys.col(keyColumn));
                    condition = condition == null
                            ? keyCondition : condition.and(keyCondition);
                }
                pending = incoming.join(existingKeys, condition, "left_anti");
            }
            long count = pending.count();
            if (count == 0L) {
                LOGGER.info("FMDB 表写入命中幂等去重，tableName={}", validated);
                return 0L;
            }
            if (targetExists) {
                // 已存在的 FMDB 标准表以建表脚本格式为准，insertInto 可以避免默认 Parquet 与 ORC 表冲突。
                pending.write().mode(SaveMode.Append).insertInto(validated);
            } else {
                pending.write().mode(SaveMode.Append).saveAsTable(validated);
            }
            LOGGER.info("FMDB 表幂等写入完成，tableName={}，writtenCount={}",
                    validated, count);
            return count;
        } catch (RuntimeException exception) {
            // FMDB Catalog 或存储写入失败必须携带目标表和主键上下文。
            LOGGER.error("FMDB 表幂等写入失败，tableName={}，keyColumns={}",
                    validated, keyColumns, exception);
            throw new IllegalStateException("FMDB 表写入失败：" + validated, exception);
        }
    }

    @Override
    public synchronized long appendDirect(String tableName,
                                          Dataset<Row> rows,
                                          long expectedCount) {
        String validated = validateTableName(tableName);
        validateRows(rows);
        if (expectedCount < 0L) {
            throw new IllegalArgumentException("FMDB 直接追加预期行数不能小于 0");
        }
        FmdbPhysicalTable physicalTable = FmdbPhysicalTable.fromTableName(validated);
        // 标准九表在网关层再次执行开关，防止专用写入器遗漏判断。
        if (physicalTable != null && !persistenceConfig.shouldPersist(physicalTable)) {
            LOGGER.info("FMDB 物理表入库已关闭，跳过直接追加，tableName={}，configKey={}",
                    validated, physicalTable.getConfigKey());
            return 0L;
        }
        if (expectedCount == 0L) {
            LOGGER.info("FMDB 直接追加数据为空，跳过写入，tableName={}", validated);
            return 0L;
        }
        LOGGER.info("开始向 FMDB 表直接追加，tableName={}，expectedCount={}",
                validated, expectedCount);
        try {
            Dataset<Row> pending = rows;
            boolean targetExists = tableExists(validated);
            if (targetExists) {
                Dataset<Row> existing = read(validated);
                if (!schemasCompatible(existing.schema(), rows.schema())) {
                    String difference = schemaDifference(existing.schema(), rows.schema());
                    throw new IllegalStateException("FMDB 目标表模式与写入模式不一致："
                            + validated + "，差异=" + difference);
                }
                // Spark 读取分区表时可能移动分区字段顺序，直接追加前仍需按目标表重排列。
                pending = alignColumns(rows, existing.schema());
            }
            if (targetExists) {
                pending.write().mode(SaveMode.Append).insertInto(validated);
            } else {
                pending.write().mode(SaveMode.Append).saveAsTable(validated);
            }
            LOGGER.info("FMDB 表直接追加完成，tableName={}，writtenCount={}",
                    validated, expectedCount);
            return expectedCount;
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 表直接追加失败，tableName={}，expectedCount={}",
                    validated, expectedCount, exception);
            throw new IllegalStateException("FMDB 表直接追加失败：" + validated, exception);
        }
    }

    private static void validateRowsAndKeys(Dataset<Row> rows,
                                            List<String> keyColumns) {
        validateRows(rows);
        if (rows == null || keyColumns == null || keyColumns.isEmpty()) {
            throw new IllegalArgumentException("FMDB 写入数据和业务主键不能为空");
        }
        Set<String> columns = new LinkedHashSet<String>(Arrays.asList(rows.columns()));
        Set<String> uniqueKeys = new LinkedHashSet<String>();
        for (String key : keyColumns) {
            String validated = ValueUtils.requireNotBlank(key, "FMDB 业务主键字段");
            if (!columns.contains(validated) || !uniqueKeys.add(validated)) {
                throw new IllegalArgumentException("FMDB 业务主键缺失或重复：" + validated);
            }
        }
    }

    private static void validateRows(Dataset<Row> rows) {
        if (rows == null) {
            throw new IllegalArgumentException("FMDB 写入数据不能为空");
        }
    }

    private static List<String> validateProjection(Dataset<Row> rows,
                                                    List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("FMDB 查询投影字段不能为空");
        }
        Set<String> available = new LinkedHashSet<String>(Arrays.asList(rows.columns()));
        Set<String> unique = new LinkedHashSet<String>();
        for (String column : columns) {
            String validated = validateColumnName(column);
            if (!available.contains(validated) || !unique.add(validated)) {
                throw new IllegalArgumentException("FMDB 查询字段缺失或重复：" + validated);
            }
        }
        return new java.util.ArrayList<String>(unique);
    }

    private static Dataset<Row> restrictExistingToIncomingPartitions(
            Dataset<Row> existing,
            Dataset<Row> incoming) {
        Dataset<Row> scoped = existing;
        Set<String> existingColumns = new LinkedHashSet<String>(Arrays.asList(existing.columns()));
        Set<String> incomingColumns = new LinkedHashSet<String>(Arrays.asList(incoming.columns()));
        for (String partitionColumn : candidatePartitionColumns()) {
            if (!existingColumns.contains(partitionColumn) || !incomingColumns.contains(partitionColumn)) {
                continue;
            }
            List<Row> partitionValues = incoming.selectExpr(partitionColumn).distinct().collectAsList();
            if (partitionValues.isEmpty()) {
                continue;
            }
            List<Object> values = new ArrayList<Object>(partitionValues.size());
            for (Row row : partitionValues) {
                values.add(row.get(0));
            }
            // FMDB 标准表按数据集和时间分区，先按 incoming 分区收窄历史数据，避免幂等校验全表扫描。
            scoped = scoped.filter(scoped.col(partitionColumn).isin(values.toArray(new Object[0])));
        }
        return scoped;
    }

    private static List<String> candidatePartitionColumns() {
        return Arrays.asList("dataset_id", "partition_month", "partition_date", "training_batch_id");
    }

    private static Dataset<Row> alignColumns(Dataset<Row> rows, StructType targetSchema) {
        String[] columnExpressions = new String[targetSchema.fields().length];
        Set<String> available = new LinkedHashSet<String>(Arrays.asList(rows.columns()));
        for (int index = 0; index < targetSchema.fields().length; index++) {
            String columnName = targetSchema.fields()[index].name();
            if (!available.contains(columnName)) {
                throw new IllegalStateException("FMDB 写入数据缺少目标字段：" + columnName);
            }
            columnExpressions[index] = columnName;
        }
        return rows.selectExpr(columnExpressions);
    }

    private static boolean schemasCompatible(StructType first, StructType second) {
        if (first.fields().length != second.fields().length) {
            return false;
        }
        Map<String, StructField> firstFields = new LinkedHashMap<String, StructField>();
        for (StructField field : first.fields()) {
            firstFields.put(field.name(), field);
        }
        for (StructField field : second.fields()) {
            StructField matched = firstFields.remove(field.name());
            if (matched == null || !matched.dataType().equals(field.dataType())) {
                return false;
            }
        }
        return firstFields.isEmpty();
    }

    private static String schemaDifference(StructType targetSchema, StructType incomingSchema) {
        Map<String, StructField> targetFields = fieldsByName(targetSchema);
        Map<String, StructField> incomingFields = fieldsByName(incomingSchema);
        List<String> missingIncomingFields = new ArrayList<String>();
        List<String> missingTargetFields = new ArrayList<String>();
        List<String> typeDifferences = new ArrayList<String>();

        for (Map.Entry<String, StructField> entry : targetFields.entrySet()) {
            StructField incomingField = incomingFields.get(entry.getKey());
            if (incomingField == null) {
                missingIncomingFields.add(entry.getKey() + ":" + entry.getValue().dataType().simpleString());
            } else if (!entry.getValue().dataType().equals(incomingField.dataType())) {
                typeDifferences.add(entry.getKey() + ":" + entry.getValue().dataType().simpleString()
                        + "!=" + incomingField.dataType().simpleString());
            }
        }
        for (Map.Entry<String, StructField> entry : incomingFields.entrySet()) {
            if (!targetFields.containsKey(entry.getKey())) {
                missingTargetFields.add(entry.getKey() + ":" + entry.getValue().dataType().simpleString());
            }
        }
        // 结构差异用于定位 FMDB 旧表和当前建表脚本不一致的问题，避免只看到泛化失败信息。
        return "目标字段数=" + targetSchema.fields().length
                + "，写入字段数=" + incomingSchema.fields().length
                + "，写入缺少目标字段=" + missingIncomingFields
                + "，目标缺少写入字段=" + missingTargetFields
                + "，类型差异=" + typeDifferences;
    }

    private static Map<String, StructField> fieldsByName(StructType schema) {
        Map<String, StructField> fields = new LinkedHashMap<String, StructField>();
        for (StructField field : schema.fields()) {
            fields.put(field.name(), field);
        }
        return fields;
    }

    public static String validateTableName(String tableName) {
        String validated = ValueUtils.requireNotBlank(tableName, "FMDB 表名");
        if (!TABLE_NAME.matcher(validated).matches()) {
            throw new IllegalArgumentException("FMDB 表名格式非法：" + validated);
        }
        return validated;
    }

    public static String validateColumnName(String columnName) {
        String validated = ValueUtils.requireNotBlank(columnName, "FMDB 字段名");
        if (!COLUMN_NAME.matcher(validated).matches()) {
            throw new IllegalArgumentException("FMDB 字段名格式非法：" + validated);
        }
        return validated;
    }
}
