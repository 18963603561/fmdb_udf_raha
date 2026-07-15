package com.fiberhome.ml.raha.retention;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.fmdb.SparkSqlFmdbTableGateway;

/**
 * 定义一张 FMDB 中间结果表的时间字段和保留天数。
 */
public final class RetentionTableRule {

    /** 每天毫秒数。 */
    private static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;
    /** 待清理 FMDB 表名。 */
    private final String tableName;
    /** 毫秒时间戳字段名。 */
    private final String timestampColumn;
    /** 数据保留天数。 */
    private final int retentionDays;

    public RetentionTableRule(String tableName, String timestampColumn) {
        this(tableName, timestampColumn,
                RahaDefaultConfigProvider.factory().intermediateRetentionDays());
    }

    public RetentionTableRule(String tableName,
                              String timestampColumn,
                              int retentionDays) {
        if (retentionDays <= 0) {
            throw new IllegalArgumentException("数据保留天数必须大于 0");
        }
        this.tableName = SparkSqlFmdbTableGateway.validateTableName(tableName);
        this.timestampColumn = SparkSqlFmdbTableGateway.validateColumnName(
                timestampColumn);
        this.retentionDays = retentionDays;
    }

    public long cutoff(long now) {
        if (now <= 0L) {
            throw new IllegalArgumentException("清理当前时间必须大于 0");
        }
        long retentionMillis = Math.multiplyExact((long) retentionDays, MILLIS_PER_DAY);
        return Math.max(1L, now - retentionMillis);
    }

    public String getTableName() { return tableName; }
    public String getTimestampColumn() { return timestampColumn; }
    public int getRetentionDays() { return retentionDays; }
}
