package com.fiberhome.ml.raha.job.stage.model;

import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;

/**
 * 确认业务服务已经持久化结果，并登记任务最终结果位置。
 */
public final class ResultPersistenceStageHandler implements StageHandler {

    /** 需要读取的服务结果属性键。 */
    private final String serviceResultAttributeKey;

    public ResultPersistenceStageHandler(String serviceResultAttributeKey) {
        if (serviceResultAttributeKey == null || serviceResultAttributeKey.trim().isEmpty()) {
            throw new IllegalArgumentException("服务结果属性键不能为空");
        }
        this.serviceResultAttributeKey = serviceResultAttributeKey;
    }

    @Override
    public StageType getStageType() {
        return StageType.PERSIST_RESULT;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object value = context.getAttributes().get(serviceResultAttributeKey);
        if (!(value instanceof RahaServiceResult)) {
            return StageResult.failure("PERSISTED_SERVICE_RESULT_REQUIRED",
                    "结果登记阶段缺少业务服务结果", false, 0L, 0L);
        }
        RahaServiceResult<?> result = (RahaServiceResult<?>) value;
        if (result.getResultLocation() == null || result.getResultLocation().trim().isEmpty()) {
            return StageResult.failure("RESULT_LOCATION_REQUIRED",
                    "业务服务没有返回持久化结果位置", false, 0L, 0L);
        }
        context.getAttributes().put(StageAttributeKeys.RESULT_LOCATION,
                result.getResultLocation());
        return StageResult.success();
    }
}
