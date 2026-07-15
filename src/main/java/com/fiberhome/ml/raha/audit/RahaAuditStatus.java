package com.fiberhome.ml.raha.audit;

/**
 * 定义审计事件执行状态。
 */
public enum RahaAuditStatus {
    /** 操作成功。 */
    SUCCEEDED,
    /** 操作因权限不足被拒绝。 */
    DENIED,
    /** 操作执行失败。 */
    FAILED
}
