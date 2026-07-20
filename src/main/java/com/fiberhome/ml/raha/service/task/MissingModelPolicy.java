package com.fiberhome.ml.raha.service.task;

/**
 * 定义检测输入字段缺少指定模型集合模型时的任务处理方式。
 */
public enum MissingModelPolicy {
    /** 任一字段缺少模型或模型不兼容时使整个检测任务失败。 */
    FAIL,
    /** 保留成功字段结果，并把缺失或不兼容字段报告为部分失败。 */
    PARTIAL
}
