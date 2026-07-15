package com.fiberhome.ml.raha.data;

/**
 * 单元格标签来源。
 */
public enum LabelSource {
    /** 人工直接标注。 */
    HUMAN,
    /** 评测真值自动生成。 */
    GROUND_TRUTH,
    /** 经过规则或业务确认。 */
    RULE_CONFIRMED,
    /** 由聚类标签传播生成。 */
    PROPAGATED,
    /** 从历史数据迁移得到。 */
    HISTORICAL
}
