package com.fiberhome.ml.raha.udf;

/**
 * UDF 同步执行终态。
 */
public enum RahaUdfExecutionStatus {
    /** 全部目标处理成功。 */
    SUCCEEDED,
    /** 部分目标成功且部分目标失败。 */
    PARTIAL_SUCCESS,
    /** 请求已解析但业务执行失败。 */
    FAILED,
    /** 请求在解析或参数校验阶段被拒绝。 */
    REJECTED
}
