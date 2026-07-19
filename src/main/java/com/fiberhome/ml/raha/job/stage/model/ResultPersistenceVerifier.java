package com.fiberhome.ml.raha.job.stage.model;

import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;

/** 校验或完成业务结果物理持久化，并返回可查询的真实位置。 */
public interface ResultPersistenceVerifier {

    String verify(StageExecutionContext context, RahaServiceResult<?> result);
}
