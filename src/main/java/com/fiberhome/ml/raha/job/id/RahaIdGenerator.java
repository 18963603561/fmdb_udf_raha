package com.fiberhome.ml.raha.job.id;

import com.fiberhome.ml.raha.data.type.StageType;

/**
 * 生成任务和阶段标识，测试可以替换为确定性实现。
 */
public interface RahaIdGenerator {

    String newJobId();

    String newStageId(String jobId, StageType stageType, int attemptId);
}

