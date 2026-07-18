package com.fiberhome.ml.raha.model.training;

/**
 * 单列训练数据可用性状态。
 */
public enum ColumnTrainingStatus {
    /** 同时包含零一标签和有效特征，可进入训练器。 */
    TRAINABLE,
    /** 当前字段没有可关联标签。 */
    NO_LABELS,
    /** 当前字段只有一个标签类别。 */
    SINGLE_CLASS,
    /** 当前字段特征字典为空。 */
    EMPTY_FEATURES,
    /** 标签存在相互冲突，剔除后没有可训练样本。 */
    LABEL_CONFLICT
}
