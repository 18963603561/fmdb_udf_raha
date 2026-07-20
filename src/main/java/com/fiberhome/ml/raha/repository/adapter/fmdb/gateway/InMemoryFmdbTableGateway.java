package com.fiberhome.ml.raha.repository.adapter.fmdb.gateway;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 开发和测试使用的 Spark 临时视图网关，不依赖本地 Hadoop 文件权限工具。
 */
public final class InMemoryFmdbTableGateway implements FmdbTableGateway {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            InMemoryFmdbTableGateway.class);
    /** 当前有效表数据集。 */
    private final Map<String, Dataset<Row>> tables =
            new LinkedHashMap<String, Dataset<Row>>();

    public InMemoryFmdbTableGateway(SparkSession sparkSession) {
        if (sparkSession == null) {
            throw new IllegalArgumentException("内存 FMDB 网关 Spark 会话不能为空");
        }
    }

    @Override
    public synchronized boolean tableExists(String tableName) {
        return tables.containsKey(SparkSqlFmdbTableGateway.validateTableName(tableName));
    }

    @Override
    public synchronized Dataset<Row> read(String tableName) {
        String validated = SparkSqlFmdbTableGateway.validateTableName(tableName);
        Dataset<Row> table = tables.get(validated);
        if (table == null) {
            throw new IllegalStateException("内存 FMDB 表不存在：" + validated);
        }
        return table;
    }

    @Override
    public synchronized Dataset<Row> read(String tableName,
                                           List<String> columns,
                                           Column condition) {
        Dataset<Row> selected = read(tableName);
        List<String> validatedColumns = validateProjection(selected, columns);
        if (condition != null) {
            selected = selected.filter(condition);
        }
        return selected.selectExpr(validatedColumns.toArray(new String[0]));
    }

    @Override
    public synchronized long appendIdempotent(String tableName,
                                              Dataset<Row> rows,
                                              List<String> keyColumns) {
        String validated = SparkSqlFmdbTableGateway.validateTableName(tableName);
        validateRowsAndKeys(rows, keyColumns);
        Dataset<Row> existing = tables.get(validated);
        Dataset<Row> pending = rows;
        if (existing != null) {
            if (!schemasCompatible(existing.schema(), rows.schema())) {
                throw new IllegalStateException("内存 FMDB 表模式与写入模式不一致：" + validated);
            }
            Dataset<Row> incoming = rows.alias("incoming");
            Dataset<Row> existingKeys = existing.selectExpr(
                    keyColumns.toArray(new String[0])).distinct().alias("existing");
            Column condition = null;
            for (String key : keyColumns) {
                Column keyCondition = incoming.col(key).eqNullSafe(existingKeys.col(key));
                condition = condition == null ? keyCondition : condition.and(keyCondition);
            }
            pending = incoming.join(existingKeys, condition, "left_anti");
        }
        long count = pending.count();
        if (count == 0L) {
            return 0L;
        }
        Dataset<Row> combined = existing == null ? pending : existing.unionByName(pending);
        // 内存网关会连续追加模型和字典；立即物化并截断血缘，避免后续写入递归重算历史连接。
        Dataset<Row> materialized = combined.localCheckpoint(true);
        if (existing != null) {
            existing.unpersist(false);
        }
        tables.put(validated, materialized);
        return count;
    }

    @Override
    public synchronized long deleteOlderThan(String tableName,
                                             String timestampColumn,
                                             long cutoffExclusive) {
        String validatedTable = SparkSqlFmdbTableGateway.validateTableName(tableName);
        String validatedColumn = SparkSqlFmdbTableGateway.validateColumnName(
                timestampColumn);
        if (cutoffExclusive <= 0L) {
            throw new IllegalArgumentException("FMDB 清理截止时间必须大于 0");
        }
        Dataset<Row> existing = tables.get(validatedTable);
        if (existing == null) {
            throw new IllegalStateException("内存 FMDB 表不存在：" + validatedTable);
        }
        if (!Arrays.asList(existing.columns()).contains(validatedColumn)) {
            throw new IllegalArgumentException("FMDB 清理时间字段不存在：" + validatedColumn);
        }
        long before = existing.count();
        Dataset<Row> retained = existing.filter(
                functions.col(validatedColumn).isNull().or(
                        functions.col(validatedColumn).geq(cutoffExclusive)));
        long after = retained.count();
        tables.put(validatedTable, retained);
        LOGGER.info("内存 FMDB 过期数据清理完成，tableName={}，timestampColumn={}，"
                        + "cutoffExclusive={}，deletedCount={}",
                validatedTable, validatedColumn, cutoffExclusive, before - after);
        return before - after;
    }

    private static void validateRowsAndKeys(Dataset<Row> rows,
                                            List<String> keyColumns) {
        if (rows == null || keyColumns == null || keyColumns.isEmpty()) {
            throw new IllegalArgumentException("内存 FMDB 写入数据和业务主键不能为空");
        }
        Set<String> columns = new LinkedHashSet<String>(Arrays.asList(rows.columns()));
        Set<String> keys = new LinkedHashSet<String>();
        for (String key : keyColumns) {
            String validated = ValueUtils.requireNotBlank(key, "内存 FMDB 业务主键");
            if (!columns.contains(validated) || !keys.add(validated)) {
                throw new IllegalArgumentException("内存 FMDB 业务主键缺失或重复：" + validated);
            }
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
            String validated = SparkSqlFmdbTableGateway.validateColumnName(column);
            if (!available.contains(validated) || !unique.add(validated)) {
                throw new IllegalArgumentException("FMDB 查询字段缺失或重复：" + validated);
            }
        }
        return new java.util.ArrayList<String>(unique);
    }

    private static boolean schemasCompatible(
            org.apache.spark.sql.types.StructType first,
            org.apache.spark.sql.types.StructType second) {
        if (first.fields().length != second.fields().length) {
            return false;
        }
        for (int index = 0; index < first.fields().length; index++) {
            if (!first.fields()[index].name().equals(second.fields()[index].name())
                    || !first.fields()[index].dataType().equals(
                    second.fields()[index].dataType())) {
                return false;
            }
        }
        return true;
    }
}
