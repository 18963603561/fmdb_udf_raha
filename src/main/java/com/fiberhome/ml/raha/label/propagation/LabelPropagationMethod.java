package com.fiberhome.ml.raha.label.propagation;

/**
 * 列内聚类标签传播冲突处理方式。
 */
public enum LabelPropagationMethod {
    /** 只有簇内直接标签完全一致时才传播。 */
    HOMOGENEITY,
    /** 簇内存在明确多数时按多数标签传播。 */
    MAJORITY
}
