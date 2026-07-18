package com.fiberhome.ml.raha.label;

/**
 * 单个列内聚类的标签传播结果状态。
 */
public enum ClusterPropagationStatus {
    /** 已向未直接标注成员传播标签。 */
    PROPAGATED,
    /** 聚类没有直接标签。 */
    NO_LABELS,
    /** 同质性模式发现零一标签冲突。 */
    CONFLICT,
    /** 多数模式没有超过最低多数比例。 */
    NO_MAJORITY,
    /** 聚类成员均已直接标注，无需传播。 */
    NO_UNLABELED_MEMBERS
}
