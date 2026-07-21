package com.fiberhome.ml.raha.data.type;

/**
 * Raha 任务阶段状态。
 */
public enum StageStatus {
    /** 阶段等待执行。 */
    PENDING,
    /** 阶段正在执行。 */
    RUNNING,
    /** 阶段执行成功。 */
    SUCCEEDED,
    /** 阶段产生可用结果，但存在部分数据项失败。 */
    PARTIAL_SUCCESS,
    /** 阶段执行失败。 */
    FAILED,
    /** 阶段因结果复用或条件不满足被跳过。 */
    SKIPPED,
    /** 阶段被取消。 */
    CANCELLED;

    /**
     * 判断是否允许转换到目标状态。
     *
     * @param target 目标状态
     * @return 允许转换返回 true
     */
    public boolean canTransitionTo(StageStatus target) {
        if (target == null || target == this) {
            return false;
        }
        // 等待阶段允许启动、跳过、失败或取消。
        if (this == PENDING) {
            return target == RUNNING || target == SKIPPED || target == FAILED || target == CANCELLED;
        }
        // 运行阶段允许由处理器返回跳过结果，例如当前没有可用标签或策略。
        if (this == RUNNING) {
            return target == SUCCEEDED || target == PARTIAL_SUCCESS
                    || target == FAILED || target == SKIPPED
                    || target == CANCELLED;
        }
        return false;
    }
}
