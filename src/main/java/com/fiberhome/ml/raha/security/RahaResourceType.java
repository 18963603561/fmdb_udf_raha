package com.fiberhome.ml.raha.security;

/**
 * 定义权限和审计使用的生产资源类型。
 */
public enum RahaResourceType {
    /** Raha 异步任务。 */
    TASK,
    /** 输入数据表或受控查询。 */
    INPUT_DATA,
    /** 标注数据表。 */
    ANNOTATION_DATA,
    /** 检测结果表。 */
    RESULT_DATA,
    /** 列级模型。 */
    MODEL,
    /** 中间结果或检查点表。 */
    INTERMEDIATE_DATA,
    /** 审计记录表。 */
    AUDIT_DATA
}
