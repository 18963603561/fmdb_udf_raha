package com.fiberhome.ml.raha.fmdb.gateway;

import com.fiberhome.ml.raha.fmdb.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.fmdb.FmdbSchemaInitializer;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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
    public synchronized long appendIdempotent(String tableName,
                                              Dataset<Row> rows,
                                              List<String> keyColumns) {
        String validated = validateTableName(tableName);
        validateRowsAndKeys(rows, keyColumns);
        LOGGER.info("开始向 FMDB 表幂等写入，tableName={}，keyColumns={}",
                validated, keyColumns);
        try {
            Dataset<Row> pending = rows;
            if (tableExists(validated)) {
                Dataset<Row> existing = read(validated);
                if (!existing.schema().equals(rows.schema())) {
                    throw new IllegalStateException("FMDB 目标表模式与写入模式不一致：" + validated);
                }
                Dataset<Row> incoming = rows.alias("incoming");
                Dataset<Row> existingKeys = existing.selectExpr(
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
            pending.write().mode(SaveMode.Append).saveAsTable(validated);
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
    public synchronized long deleteOlderThan(String tableName,
                                             String timestampColumn,
                                             long cutoffExclusive) {
        String validatedTable = validateTableName(tableName);
        String validatedColumn = validateColumnName(timestampColumn);
        if (cutoffExclusive <= 0L) {
            throw new IllegalArgumentException("FMDB 清理截止时间必须大于 0");
        }
        LOGGER.info("开始清理 FMDB 过期数据，tableName={}，timestampColumn={}，"
                        + "cutoffExclusive={}",
                validatedTable, validatedColumn, cutoffExclusive);
        try {
            long count = read(validatedTable).filter(
                    org.apache.spark.sql.functions.col(validatedColumn)
                            .lt(cutoffExclusive)).count();
            if (count == 0L) {
                return 0L;
            }
            // 生产 FMDB 表必须支持标准删除语句，避免在 Driver 端搬运或覆盖整表。
            sparkSession.sql("DELETE FROM " + validatedTable + " WHERE "
                    + validatedColumn + " < " + cutoffExclusive);
            LOGGER.info("FMDB 过期数据清理完成，tableName={}，deletedCount={}",
                    validatedTable, count);
            return count;
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 过期数据清理失败，tableName={}，timestampColumn={}，"
                            + "cutoffExclusive={}",
                    validatedTable, validatedColumn, cutoffExclusive, exception);
            throw new IllegalStateException("FMDB 过期数据清理失败：" + validatedTable,
                    exception);
        }
    }

    private static void validateRowsAndKeys(Dataset<Row> rows,
                                            List<String> keyColumns) {
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
