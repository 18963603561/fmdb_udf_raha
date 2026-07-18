package com.fiberhome.ml.raha.service.common;

/**
 * Raha 服务任务终态。
 */
public enum RahaTaskStatus {
    /** 全部目标处理成功。 */
    SUCCEEDED,
    /** 部分字段成功且部分字段失败。 */
    PARTIAL_SUCCESS,
    /** 未产生可用结果或核心流程失败。 */
    FAILED
}
