package com.fiberhome.ml.raha.train;

/**
 * 模型训练模式。
 */
public enum TrainingMode {
    /** 不复用父模型契约。 */
    FULL,
    /** 复用父计划和特征字典并合并父样本。 */
    INCREMENTAL
}
