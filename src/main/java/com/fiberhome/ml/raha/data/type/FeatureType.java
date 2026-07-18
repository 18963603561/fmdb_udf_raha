package com.fiberhome.ml.raha.data.type;

/**
 * 单元格特征类型。
 */
public enum FeatureType {
    /** 是否命中策略等二值特征。 */
    BINARY,
    /** 频率、长度或距离等数值特征。 */
    NUMERIC,
    /** 离散类别编码特征。 */
    CATEGORICAL,
    /** 文本稀疏特征。 */
    TEXT
}
