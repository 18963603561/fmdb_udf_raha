package com.fiberhome.ml.raha.repository.core;

/**
 * 统一仓储中的业务数据命名空间。
 */
public enum RepositoryNamespace {
    /** 任务状态。 */
    JOB,
    /** 任务阶段状态。 */
    STAGE,
    /** 可恢复阶段检查点和尝试记录。 */
    STAGE_CHECKPOINT,
    /** 列画像。 */
    COLUMN_PROFILE,
    /** 策略计划。 */
    STRATEGY_PLAN,
    /** 策略命中。 */
    STRATEGY_HIT,
    /** 策略运行摘要。 */
    STRATEGY_RUN_SUMMARY,
    /** 特征字典。 */
    FEATURE_DICTIONARY,
    /** 单元格稀疏特征。 */
    SPARSE_FEATURE,
    /** 聚类成员。 */
    CLUSTER_ASSIGNMENT,
    /** 单列聚类运行结果。 */
    CLUSTER_RUN_SUMMARY,
    /** 待标注元组任务。 */
    ANNOTATION_TASK,
    /** 单元格标签。 */
    CELL_LABEL,
    /** 标签传播冲突和结果摘要。 */
    LABEL_PROPAGATION_SUMMARY,
    /** 列级模型。 */
    COLUMN_MODEL,
    /** 单元格检测结果。 */
    DETECTION_RESULT
}
