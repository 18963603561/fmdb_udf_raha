package com.fiberhome.ml.raha.error;

/**
 * Raha 异常分类，用于调用方区分参数、数据、策略和存储等失败来源。
 */
public enum RahaErrorCategory {
    /** 参数或配置错误。 */
    PARAMETER,
    /** 输入数据或快照错误。 */
    DATA,
    /** 策略计划或执行错误。 */
    STRATEGY,
    /** 特征生成错误。 */
    FEATURE,
    /** 检测评分或结果错误。 */
    DETECTION,
    /** 中间结果或最终结果存储错误。 */
    STORAGE,
    /** 未归类系统错误。 */
    SYSTEM
}
