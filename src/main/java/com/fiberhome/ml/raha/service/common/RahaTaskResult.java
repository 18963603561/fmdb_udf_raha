package com.fiberhome.ml.raha.service.common;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 统一返回服务状态、结果位置、摘要和类型化业务结果。
 *
 * @param <T> 训练、采样或检测输出类型
 */
public final class RahaTaskResult<T> {

    /** 调用方提供的任务标识。 */
    private final String taskId;
    /** 服务任务类型。 */
    private final RahaTaskType taskType;
    /** 服务任务终态。 */
    private final RahaTaskStatus status;
    /** 仓储结果逻辑位置，失败且无结果时允许为空。 */
    private final String resultLocation;
    /** 任务阶段摘要。 */
    private final RahaTaskSummary summary;
    /** 类型化业务输出，核心流程失败时允许为空。 */
    private final T payload;
    /** 失败错误码，成功时为空。 */
    private final String errorCode;
    /** 不包含敏感值的失败信息，成功时为空。 */
    private final String errorMessage;

    public RahaTaskResult(String taskId,
                          RahaTaskType taskType,
                          RahaTaskStatus status,
                          String resultLocation,
                          RahaTaskSummary summary,
                          T payload,
                          String errorCode,
                          String errorMessage) {
        this.taskId = ValueUtils.requireNotBlank(taskId, "服务任务标识");
        if (taskType == null || status == null || summary == null) {
            throw new IllegalArgumentException("服务任务类型、状态和摘要不能为空");
        }
        if (status != RahaTaskStatus.FAILED
                && (resultLocation == null || resultLocation.trim().isEmpty())) {
            throw new IllegalArgumentException("成功或部分成功任务必须包含结果位置");
        }
        if (status == RahaTaskStatus.SUCCEEDED
                && (errorCode != null || errorMessage != null)) {
            throw new IllegalArgumentException("成功任务不能包含错误信息");
        }
        this.taskType = taskType;
        this.status = status;
        this.resultLocation = resultLocation;
        this.summary = summary;
        this.payload = payload;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getTaskId() { return taskId; }
    public RahaTaskType getTaskType() { return taskType; }
    public RahaTaskStatus getStatus() { return status; }
    public String getResultLocation() { return resultLocation; }
    public RahaTaskSummary getSummary() { return summary; }
    public T getPayload() { return payload; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
}
