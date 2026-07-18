package com.fiberhome.ml.raha.checkpoint;

/**
 * 阶段检查点尝试状态。
 */
public enum StageCheckpointStatus {
    /** 当前尝试正在执行。 */
    RUNNING,
    /** 当前尝试成功且输出可复用。 */
    SUCCEEDED,
    /** 当前尝试失败并保留错误摘要。 */
    FAILED
}
