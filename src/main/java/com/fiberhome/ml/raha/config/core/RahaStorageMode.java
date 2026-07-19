package com.fiberhome.ml.raha.config.core;

/**
 * Raha 默认运行时使用的物理存储模式。
 */
public enum RahaStorageMode {
    /** 使用当前进程内存网关，适合开发、测试和短生命周期任务。 */
    IN_MEMORY,
    /** 使用 Spark SQL FMDB 网关，适合已经接入 FMDB Catalog 的运行环境。 */
    FMDB
}
