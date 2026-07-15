package com.fiberhome.ml.raha.model;

/**
 * 列级模型训练结果状态。
 */
public enum ColumnModelTrainingStatus {
    /** 已生成可预测模型。 */
    TRAINED,
    /** 没有标签。 */
    NO_LABELS,
    /** 只有一个标签类别。 */
    SINGLE_CLASS,
    /** 没有有效特征。 */
    EMPTY_FEATURES,
    /** 标签冲突后无有效训练数据。 */
    LABEL_CONFLICT,
    /** 首选 MLlib 依赖不可用且未允许降级。 */
    MLLIB_UNAVAILABLE,
    /** 训练器执行失败。 */
    FAILED
}
