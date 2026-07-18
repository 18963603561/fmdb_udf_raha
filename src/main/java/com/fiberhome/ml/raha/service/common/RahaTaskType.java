package com.fiberhome.ml.raha.service.common;

/**
 * Raha 服务层统一任务类型。
 */
public enum RahaTaskType {
    /** 列级模型训练任务。 */
    TRAIN,
    /** 聚类覆盖采样任务。 */
    SAMPLE,
    /** 已发布模型检测任务。 */
    DETECT
}
