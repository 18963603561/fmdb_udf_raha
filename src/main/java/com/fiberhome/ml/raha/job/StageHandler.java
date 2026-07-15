package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.data.StageType;

/**
 * 一个可由任务编排器执行的阶段处理器。
 */
public interface StageHandler {

    StageType getStageType();

    StageResult execute(StageExecutionContext context);
}

