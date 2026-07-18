package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.job.StageExecutionContext;
import com.fiberhome.ml.raha.job.StageResult;

/**
 * 一个可由任务编排器执行的阶段处理器。
 */
public interface StageHandler {

    StageType getStageType();

    StageResult execute(StageExecutionContext context);
}
