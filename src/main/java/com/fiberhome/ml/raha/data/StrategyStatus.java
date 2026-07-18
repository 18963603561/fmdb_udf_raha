package com.fiberhome.ml.raha.data;

/**
 * 策略计划和执行状态。
 */
public enum StrategyStatus {
    /** 策略已进入计划。 */
    PLANNED,
    /** 策略正在运行。 */
    RUNNING,
    /** 策略运行成功。 */
    SUCCEEDED,
    /** 策略被过滤或跳过。 */
    SKIPPED,
    /** 策略运行失败。 */
    FAILED
}
