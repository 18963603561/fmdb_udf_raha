package com.fiberhome.ml.raha.data.type;

/**
 * Raha 流程阶段类型。
 */
public enum StageType {
    /** 初始化任务和配置。 */
    INITIALIZE,
    /** 读取输入数据和快照。 */
    LOAD_DATA,
    /** 合并持久化 c1、标注批次和当前 o1，生成训练快照。 */
    MERGE_TRAINING_INPUT,
    /** 生成数据画像。 */
    PROFILE,
    /** 生成策略计划。 */
    GENERATE_STRATEGY,
    /** 执行检测策略。 */
    RUN_STRATEGY,
    /** 生成单元格特征。 */
    GENERATE_FEATURE,
    /** 执行列内聚类。 */
    CLUSTER,
    /** 固化采样快照检查点，供后续训练复用。 */
    SNAPSHOT_CHECKPOINT,
    /** 从采样快照检查点恢复画像、策略、特征和聚类产物。 */
    RESTORE_SNAPSHOT_CHECKPOINT,
    /** 选择待标注元组。 */
    SAMPLE,
    /** 接收或生成直接标签。 */
    LABEL,
    /** 在聚类中传播标签。 */
    PROPAGATE,
    /** 训练列级模型。 */
    TRAIN,
    /** 预测疑似错误单元格。 */
    PREDICT,
    /** 评估检测质量。 */
    EVALUATE,
    /** 持久化最终结果。 */
    PERSIST_RESULT
}
