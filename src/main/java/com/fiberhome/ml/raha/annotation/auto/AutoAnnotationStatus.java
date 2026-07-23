package com.fiberhome.ml.raha.annotation.auto;

/**
 * 描述一次自动标注任务的最终状态。
 */
public enum AutoAnnotationStatus {
    /** 未启用自动标注。 */
    DISABLED,
    /** 全部目标行标注成功。 */
    SUCCEEDED,
    /** 仅部分目标行标注成功。 */
    PARTIAL,
    /** 自动标注失败且没有可审核工作簿。 */
    FAILED
}
