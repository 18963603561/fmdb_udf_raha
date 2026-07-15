package com.fiberhome.ml.raha.performance;

/**
 * 定义生产数据容量分档。
 */
public enum CapacityBand {
    /** 一百万行以内且字段数不超过五十。 */
    SMALL,
    /** 一千万行以内且字段数不超过一百。 */
    MEDIUM,
    /** 一亿行以内或字段数不超过二百。 */
    LARGE,
    /** 超过常规容量边界，需要专项压测。 */
    EXTRA_LARGE
}
