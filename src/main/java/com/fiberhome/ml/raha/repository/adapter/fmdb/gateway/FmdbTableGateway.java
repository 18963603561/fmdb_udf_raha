package com.fiberhome.ml.raha.repository.adapter.fmdb.gateway;

import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 抽象 FMDB 表读取和幂等追加能力，隔离核心适配器与平台私有 Catalog 实现。
 */
public interface FmdbTableGateway {

    boolean tableExists(String tableName);

    Dataset<Row> read(String tableName);

    /**
     * 按显式字段和过滤条件读取 FMDB，避免业务热路径扫描无关列。
     *
     * @param tableName FMDB 目标表
     * @param columns 需要读取的字段
     * @param condition 可选 Spark 过滤条件
     * @return 完成列裁剪和条件过滤的数据集
     */
    Dataset<Row> read(String tableName,
                      List<String> columns,
                      Column condition);

    /**
     * 按全局写入模式追加记录。
     *
     * @param tableName FMDB 目标表
     * @param rows 待写入数据
     * @param keyColumns 幂等模式使用的业务主键字段
     * @param expectedCount 调用方已知的待写入记录数
     * @return 实际追加行数
     */
    long append(String tableName,
                Dataset<Row> rows,
                List<String> keyColumns,
                long expectedCount);

    /**
     * 不读取目标表业务主键，直接追加写入数据。
     *
     * @param tableName FMDB 目标表
     * @param rows 待写入数据
     * @param expectedCount 调用方已知的待写入记录数
     * @return 追加提交的记录数
     */
    long appendDirect(String tableName,
                      Dataset<Row> rows,
                      long expectedCount);

}
