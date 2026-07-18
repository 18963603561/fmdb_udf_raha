package com.fiberhome.ml.raha.cluster;

/**
 * 单列聚类的可解释结果状态。
 */
public enum ColumnClusteringStatus {
    /** 正常完成多样本聚类。 */
    SUCCEEDED,
    /** 当前字段没有单元格特征行。 */
    EMPTY_INPUT,
    /** 当前字段没有可用于计算距离的有效特征。 */
    EMPTY_FEATURES,
    /** 当前字段只有一个样本，已直接生成单成员簇。 */
    SINGLE_SAMPLE,
    /** 精确聚类样本超过配置上限。 */
    INPUT_LIMIT_EXCEEDED,
    /** 特征距离出现非有限数值。 */
    DISTANCE_UNDEFINED,
    /** 当前字段聚类发生已隔离异常。 */
    FAILED
}
