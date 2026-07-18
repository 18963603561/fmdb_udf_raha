package com.fiberhome.ml.raha.data;

/**
 * 列级模型生命周期状态。
 */
public enum ModelStatus {
    /** 模型尚未完成评估。 */
    DRAFT,
    /** 模型通过基础评估，等待发布。 */
    CANDIDATE,
    /** 模型允许用于生产检测。 */
    PUBLISHED,
    /** 模型已经停用。 */
    DISABLED,
    /** 模型训练失败。 */
    FAILED
}
