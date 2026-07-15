package com.fiberhome.ml.raha.udf;

/**
 * 表级 UDF 异步提交状态。
 */
public enum RahaUdfSubmissionStatus {
    /** 新任务已接受并持久化。 */
    ACCEPTED,
    /** 相同幂等请求已存在，返回原任务。 */
    DUPLICATE,
    /** 参数或提交过程失败，任务未被接受。 */
    REJECTED
}
