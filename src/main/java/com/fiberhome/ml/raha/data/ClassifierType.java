package com.fiberhome.ml.raha.data;

/**
 * 列级检测模型类型。
 */
public enum ClassifierType {
    /** 不依赖 MLlib 的规则加权降级模型。 */
    WEIGHTED_RULE,
    /** Spark MLlib 逻辑回归。 */
    LOGISTIC_REGRESSION,
    /** Spark MLlib 决策树。 */
    DECISION_TREE,
    /** Spark MLlib 梯度提升树。 */
    GBT
}
