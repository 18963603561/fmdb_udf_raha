package com.fiberhome.ml.raha.job.stage;

/**
 * 阶段处理器执行结果类型。
 */
public enum StageOutcome {
    /** 阶段成功完成。 */
    SUCCESS,
    /** 阶段产生可用结果，但存在部分数据项失败。 */
    PARTIAL_SUCCESS,
    /** 阶段按条件跳过。 */
    SKIPPED,
    /** 阶段执行失败。 */
    FAILED
}
