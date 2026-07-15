package com.fiberhome.ml.raha.performance;

/**
 * 隔离本地 JVM、Spark 监听器或 FMDB 指标平台的阶段资源采集实现。
 */
public interface StageResourceProbe {

    /**
     * 采集当前累计资源计数，不可用指标使用负一。
     */
    StageResourceSnapshot capture();
}
