package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import java.util.List;

/**
 * 定义一种任务类型如何创建有序阶段处理器。
 */
public interface RahaWorkflow {

    JobType getJobType();

    List<StageHandler> createStageHandlers(RahaTaskExecutionRequest request);
}
