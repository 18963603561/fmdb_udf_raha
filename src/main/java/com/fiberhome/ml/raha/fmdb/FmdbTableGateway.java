package com.fiberhome.ml.raha.fmdb;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * 抽象 FMDB 表读取和幂等追加能力，隔离核心适配器与平台私有 Catalog 实现。
 */
public interface FmdbTableGateway {

    boolean tableExists(String tableName);

    Dataset<Row> read(String tableName);

    /**
     * 只追加业务主键尚不存在的记录。
     *
     * @param tableName FMDB 目标表
     * @param rows 待写入数据
     * @param keyColumns 幂等业务主键字段
     * @return 实际追加行数
     */
    long appendIdempotent(String tableName,
                          Dataset<Row> rows,
                          List<String> keyColumns);

    /**
     * 删除时间字段早于截止时间的记录。
     *
     * @param tableName FMDB 目标表
     * @param timestampColumn 毫秒时间戳字段
     * @param cutoffExclusive 不包含的清理截止时间
     * @return 实际删除行数
     */
    long deleteOlderThan(String tableName,
                         String timestampColumn,
                         long cutoffExclusive);
}
