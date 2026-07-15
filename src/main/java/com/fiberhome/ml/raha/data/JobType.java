package com.fiberhome.ml.raha.data;

/**
 * Raha 任务类型。
 */
public enum JobType {
    /** 使用脏表和真值表评估检测效果。 */
    EVALUATION,
    /** 使用标签训练列级检测模型。 */
    TRAINING,
    /** 生成待人工标注的元组任务。 */
    SAMPLING,
    /** 使用已发布模型执行生产检测。 */
    DETECTION,
    /** 只运行策略并输出策略画像。 */
    STRATEGY_ANALYSIS
}
