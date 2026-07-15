package com.fiberhome.ml.raha.audit;

/**
 * 定义必须持久追溯的 Raha 生产操作。
 */
public enum RahaAuditAction {
    /** 提交训练、检测或采样任务。 */
    TASK_SUBMIT,
    /** 发布列级模型。 */
    MODEL_PUBLISH,
    /** 停用列级模型。 */
    MODEL_DISABLE,
    /** 回滚列级模型。 */
    MODEL_ROLLBACK,
    /** 读取检测结果。 */
    RESULT_ACCESS,
    /** 清理过期中间结果。 */
    RETENTION_CLEANUP
}
