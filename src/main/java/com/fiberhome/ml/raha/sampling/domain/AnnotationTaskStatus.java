package com.fiberhome.ml.raha.sampling.domain;

/**
 * 元组标注任务状态。
 */
public enum AnnotationTaskStatus {
    /** 等待人工或评测适配器提交标签。 */
    PENDING,
    /** 当前元组标注已经完成。 */
    COMPLETED,
    /** 任务超过有效期且未完成。 */
    EXPIRED,
    /** 任务被调用方主动取消。 */
    CANCELLED
}
