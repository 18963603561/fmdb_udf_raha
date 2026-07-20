package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

/**
 * 控制 FMDB 标准物理表的全局追加写入方式。
 */
public enum FmdbWriteMode {

    /**
     * 不读取目标表业务主键，直接向目标表追加记录。
     */
    DIRECT_APPEND,

    /**
     * 先按业务主键过滤目标表已有记录，再追加新记录。
     */
    IDEMPOTENT_BY_KEY
}
