package com.fiberhome.ml.raha.performance;

/**
 * 定义性能基准数据集覆盖的规模类型。
 */
public enum BenchmarkScale {
    /** 小规模功能和快速回归数据。 */
    SMALL,
    /** 中等规模容量基线数据。 */
    MEDIUM,
    /** 大规模生产预估数据。 */
    LARGE,
    /** 高字段数量的宽表数据。 */
    WIDE
}
