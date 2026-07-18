package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;

/**
 * 将阶段内部服务结果转换为统一阶段结果。
 */
final class ServiceStageResultMapper {

    private ServiceStageResultMapper() {
    }

    static StageResult map(RahaServiceResult<?> result) {
        if (result == null) {
            return StageResult.failure("SERVICE_RESULT_REQUIRED",
                    "业务服务返回空结果", false, 0L, 0L);
        }
        if (result.getStatus() == JobStatus.SUCCEEDED) {
            return StageResult.success();
        }
        long failedCount = result.getSummary().getFailedCount();
        long totalCount = result.getSummary().getTotalCount();
        String errorCode = safe(result.getErrorCode(), "SERVICE_EXECUTION_FAILED");
        String message = safe(result.getErrorMessage(), "业务服务执行失败");
        if (result.getStatus() == JobStatus.PARTIAL_SUCCESS) {
            return StageResult.partialSuccess(errorCode, message,
                    failedCount, totalCount);
        }
        return StageResult.failure(errorCode, message, false,
                failedCount, totalCount);
    }

    private static String safe(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }
}
