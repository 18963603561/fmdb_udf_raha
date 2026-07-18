package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.config.RahaConfig;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

/**
 * 标准 ORC 表的批量查询、幂等追加和存在性检查网关。
 */
public final class FmdbTableGateway {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FmdbTableGateway.class);
    /** 当前 Spark 会话。 */
    private final SparkSession sparkSession;

    public FmdbTableGateway(SparkSession sparkSession, RahaConfig config) {
        this.sparkSession = sparkSession;
        RahaTables.ensure(sparkSession, config);
    }

    public Dataset<Row> table(String tableName) {
        long startedAt = System.currentTimeMillis();
        try {
            Dataset<Row> result = sparkSession.table(tableName);
            LOGGER.debug("读取 FMDB 表完成，table={}，elapsedMillis={}", tableName,
                    System.currentTimeMillis() - startedAt);
            return result;
        } catch (RuntimeException exception) {
            LOGGER.error("读取 FMDB 表失败，table={}", tableName, exception);
            throw new RahaException(RahaErrorCode.STORAGE_ERROR,
                    "读取 FMDB 表失败：" + tableName, exception);
        }
    }

    public boolean exists(String tableName, String columnName, String value) {
        return table(tableName).filter(col(columnName).equalTo(value)).limit(1).count() > 0;
    }

    /**
     * 按逻辑主键执行反连接后批量追加。
     *
     * @param tableName 表名
     * @param incoming 待写数据
     * @param keyColumns 逻辑主键字段
     * @return 实际写入行数
     */
    public long appendDistinct(String tableName, Dataset<Row> incoming,
                               String... keyColumns) {
        long startedAt = System.currentTimeMillis();
        try {
            Dataset<Row> existing = table(tableName).select(columns(keyColumns))
                    .dropDuplicates().alias("stored");
            Dataset<Row> candidate = incoming.alias("candidate");
            org.apache.spark.sql.Column condition = candidate.col(keyColumns[0])
                    .equalTo(existing.col(keyColumns[0]));
            for (int index = 1; index < keyColumns.length; index++) {
                condition = condition.and(candidate.col(keyColumns[index])
                        .equalTo(existing.col(keyColumns[index])));
            }
            Dataset<Row> filtered = candidate.join(existing, condition, "left_anti");
            long count = filtered.count();
            if (count > 0) {
                String[] orderedColumns = table(tableName).columns();
                filtered.select(columns(orderedColumns)).write().mode(SaveMode.Append)
                        .insertInto(tableName);
            }
            LOGGER.info("FMDB 批量追加完成，table={}，keys={}，writtenRows={}，"
                            + "elapsedMillis={}", tableName, Arrays.toString(keyColumns),
                    count, System.currentTimeMillis() - startedAt);
            return count;
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 批量追加失败，table={}，keys={}", tableName,
                    Arrays.toString(keyColumns), exception);
            throw new RahaException(RahaErrorCode.STORAGE_ERROR,
                    "写入 FMDB 表失败：" + tableName, exception);
        }
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    private static org.apache.spark.sql.Column[] columns(String[] names) {
        org.apache.spark.sql.Column[] columns = new org.apache.spark.sql.Column[names.length];
        for (int index = 0; index < names.length; index++) {
            columns[index] = col(names[index]);
        }
        return columns;
    }
}
