package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

/**
 * 控制 FMDB 检测错误结果表的写入方式。
 */
public enum FmdbDetectionResultWriteMode {

    /**
     * 先按业务主键过滤目标表已有记录，再追加新记录。
     */
    IDEMPOTENT_BY_KEY,

    /**
     * 不读取目标表业务主键，直接向检测结果表追加记录。
     */
    DIRECT_APPEND
}
