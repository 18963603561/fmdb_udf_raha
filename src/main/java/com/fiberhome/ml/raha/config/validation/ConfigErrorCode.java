package com.fiberhome.ml.raha.config.validation;

/**
 * 配置校验错误编码，调用方可以根据编码定位参数问题。
 */
public enum ConfigErrorCode {
    /** 配置对象为空。 */
    CONFIG_REQUIRED,
    /** 任务类型为空。 */
    JOB_TYPE_REQUIRED,
    /** 数据集标识为空。 */
    DATASET_ID_REQUIRED,
    /** 输入数据引用为空。 */
    INPUT_REFERENCE_REQUIRED,
    /** 行标识字段为空。 */
    ROW_ID_COLUMN_REQUIRED,
    /** 策略配置为空。 */
    STRATEGY_CONFIG_REQUIRED,
    /** 策略族为空。 */
    STRATEGY_FAMILY_REQUIRED,
    /** 策略数量或超时配置非法。 */
    STRATEGY_LIMIT_INVALID,
    /** 字段白名单和黑名单发生冲突。 */
    COLUMN_FILTER_CONFLICT,
    /** 策略类型白名单和黑名单发生冲突或优先级非法。 */
    STRATEGY_FILTER_INVALID,
    /** 特征配置为空或限制非法。 */
    FEATURE_CONFIG_INVALID,
    /** 模型配置为空。 */
    MODEL_CONFIG_REQUIRED,
    /** 模型阈值非法。 */
    MODEL_THRESHOLD_INVALID,
    /** 聚类距离、簇数量或样本上限非法。 */
    CLUSTERING_CONFIG_INVALID,
    /** 采样预算或标注任务有效期非法。 */
    SAMPLING_CONFIG_INVALID,
    /** 资源配置为空或限制非法。 */
    RESOURCE_CONFIG_INVALID,
    /** 失败容忍配置为空或范围非法。 */
    FAILURE_TOLERANCE_INVALID
}
