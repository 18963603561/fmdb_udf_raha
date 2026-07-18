package com.fiberhome.ml.raha.job.execution;

/**
 * 阶段失败后的编排决策。
 */
public enum FailureDecision {
    /** 创建下一次阶段尝试。 */
    RETRY,
    /** 记录部分失败并继续后续阶段。 */
    CONTINUE,
    /** 终止整个任务。 */
    TERMINATE
}

