package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.config.FailureToleranceConfig;

/**
 * 根据失败可恢复性、失败比例和重试次数决定任务是否继续。
 */
public final class StageFailureDecider {

    public FailureDecision decide(StageResult result,
                                  FailureToleranceConfig config,
                                  int attemptId) {
        if (result == null || config == null || attemptId <= 0) {
            throw new IllegalArgumentException("阶段结果、失败容忍配置和尝试序号必须有效");
        }
        if (result.getOutcome() != StageOutcome.FAILED) {
            return FailureDecision.CONTINUE;
        }
        // 不可恢复失败、快速失败或失败比例超限时必须终止任务。
        if (!result.isRecoverable()
                || config.isFailFast()
                || result.failedRatio() > config.getMaxFailedStrategyRatio()) {
            return FailureDecision.TERMINATE;
        }
        // 最大重试次数表示首次尝试之后允许创建的新尝试数量。
        if (attemptId <= config.getMaxRetryCount()) {
            return FailureDecision.RETRY;
        }
        return FailureDecision.CONTINUE;
    }
}

