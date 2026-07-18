package com.fiberhome.ml.raha.data.type;

/**
 * Raha 任务状态，状态转换用于阻止成功或失败任务被再次修改。
 */
public enum JobStatus {
    /** 任务已经创建但尚未运行。 */
    CREATED,
    /** 任务正在运行。 */
    RUNNING,
    /** 任务已经成功完成。 */
    SUCCEEDED,
    /** 任务产生可用结果，但存在可容忍的阶段或数据项失败。 */
    PARTIAL_SUCCESS,
    /** 任务执行失败。 */
    FAILED,
    /** 任务被调用方取消。 */
    CANCELLED;

    /**
     * 判断是否允许转换到目标状态。
     *
     * @param target 目标状态
     * @return 允许转换返回 true
     */
    public boolean canTransitionTo(JobStatus target) {
        if (target == null || target == this) {
            return false;
        }
        // 已创建任务只能进入运行、失败或取消状态。
        if (this == CREATED) {
            return target == RUNNING || target == FAILED || target == CANCELLED;
        }
        // 运行中任务只能进入三种终态，终态之间禁止相互转换。
        if (this == RUNNING) {
            return target == SUCCEEDED || target == PARTIAL_SUCCESS
                    || target == FAILED || target == CANCELLED;
        }
        return false;
    }
}
