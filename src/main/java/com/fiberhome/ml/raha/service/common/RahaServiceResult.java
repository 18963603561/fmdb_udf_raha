package com.fiberhome.ml.raha.service.common;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存阶段内部业务服务的状态、摘要和类型化输出。
 *
 * <p>该对象不是独立任务模型，任务生命周期统一由 RahaJob 和 RahaStage 管理。</p>
 *
 * @param <T> 训练、采样或检测业务输出类型
 */
public final class RahaServiceResult<T> {

    /** 所属任务标识。 */
    private final String jobId;
    /** 所属任务类型。 */
    private final JobType jobType;
    /** 服务执行终态，使用统一任务状态枚举。 */
    private final JobStatus status;
    /** 仓储结果逻辑位置，失败且无结果时允许为空。 */
    private final String resultLocation;
    /** 服务执行摘要。 */
    private final RahaServiceSummary summary;
    /** 类型化业务输出，核心流程失败时允许为空。 */
    private final T payload;
    /** 失败错误码，成功时为空。 */
    private final String errorCode;
    /** 不包含敏感值的失败信息，成功时为空。 */
    private final String errorMessage;

    public RahaServiceResult(String jobId,
                             JobType jobType,
                             JobStatus status,
                             String resultLocation,
                             RahaServiceSummary summary,
                             T payload,
                             String errorCode,
                             String errorMessage) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (jobType == null || status == null || summary == null) {
            throw new IllegalArgumentException("任务类型、服务状态和摘要不能为空");
        }
        if (status != JobStatus.SUCCEEDED
                && status != JobStatus.PARTIAL_SUCCESS
                && status != JobStatus.FAILED) {
            throw new IllegalArgumentException("服务结果只能使用任务终态");
        }
        if (status != JobStatus.FAILED
                && (resultLocation == null || resultLocation.trim().isEmpty())) {
            throw new IllegalArgumentException("成功或部分成功服务必须包含结果位置");
        }
        if (status == JobStatus.SUCCEEDED
                && (errorCode != null || errorMessage != null)) {
            throw new IllegalArgumentException("成功服务不能包含错误信息");
        }
        this.jobType = jobType;
        this.status = status;
        this.resultLocation = resultLocation;
        this.summary = summary;
        this.payload = payload;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getJobId() { return jobId; }
    public JobType getJobType() { return jobType; }
    public JobStatus getStatus() { return status; }
    public String getResultLocation() { return resultLocation; }
    public RahaServiceSummary getSummary() { return summary; }
    public T getPayload() { return payload; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
}
