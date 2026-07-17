package com.fiberhome.ml.raha.support;

/**
 * Raha 对外稳定错误码。
 */
public enum RahaErrorCode {
    /** 请求参数不完整或不合法。 */
    INVALID_REQUEST,
    /** 输入数据不满足算法约束。 */
    INVALID_DATA,
    /** 模型与输入数据不兼容。 */
    INCOMPATIBLE_MODEL,
    /** 算法计算失败。 */
    ALGORITHM_ERROR,
    /** FMDB 或文件系统读写失败。 */
    STORAGE_ERROR,
    /** 平台执行环境不满足要求。 */
    PLATFORM_ERROR
}
