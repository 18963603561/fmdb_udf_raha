package com.fiberhome.ml.raha.job.domain;

import com.fiberhome.ml.raha.data.type.StageStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一个任务阶段的类型、尝试次数、状态和失败信息。
 */
public final class RahaStage {

    /** 阶段唯一标识。 */
    private final String stageId;
    /** 所属任务标识。 */
    private final String jobId;
    /** 阶段业务类型。 */
    private final StageType stageType;
    /** 当前阶段尝试序号，从一开始。 */
    private final int attemptId;
    /** 当前阶段状态。 */
    private StageStatus status;
    /** 阶段开始时间。 */
    private long startedAt;
    /** 阶段结束时间。 */
    private long finishedAt;
    /** 阶段失败编码。 */
    private String errorCode;
    /** 脱敏后的阶段失败说明。 */
    private String errorMessage;

    public RahaStage(String stageId, String jobId, StageType stageType, int attemptId) {
        this.stageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (stageType == null) {
            throw new IllegalArgumentException("阶段类型不能为空");
        }
        if (attemptId <= 0) {
            throw new IllegalArgumentException("阶段尝试序号必须大于 0");
        }
        this.stageType = stageType;
        this.attemptId = attemptId;
        this.status = StageStatus.PENDING;
    }

    /**
     * 启动等待中的阶段。
     *
     * @param startTime 开始时间
     */
    public synchronized void start(long startTime) {
        if (startTime <= 0L) {
            throw new IllegalArgumentException("阶段开始时间必须大于 0");
        }
        transitionTo(StageStatus.RUNNING);
        this.startedAt = startTime;
    }

    public synchronized void succeed(long finishTime) {
        validateFinishTime(finishTime);
        transitionTo(StageStatus.SUCCEEDED);
        finish(finishTime, null, null);
    }

    public synchronized void fail(String code, String message, long finishTime) {
        validateFinishTime(finishTime);
        String validatedCode = ValueUtils.requireNotBlank(code, "失败编码");
        String validatedMessage = ValueUtils.requireNotBlank(message, "失败说明");
        transitionTo(StageStatus.FAILED);
        finish(finishTime, validatedCode, validatedMessage);
    }

    public synchronized void skip(long finishTime) {
        validateFinishTime(finishTime);
        transitionTo(StageStatus.SKIPPED);
        finish(finishTime, null, null);
    }

    public synchronized void cancel(long finishTime) {
        validateFinishTime(finishTime);
        transitionTo(StageStatus.CANCELLED);
        finish(finishTime, null, null);
    }

    private void transitionTo(StageStatus target) {
        // 阶段终态不可再次修改，新的重试必须创建更大的 attemptId。
        if (!status.canTransitionTo(target)) {
            throw new IllegalStateException("阶段状态不允许从 " + status + " 转换为 " + target);
        }
        this.status = target;
    }

    private void finish(long finishTime, String code, String message) {
        validateFinishTime(finishTime);
        this.finishedAt = finishTime;
        this.errorCode = code;
        this.errorMessage = message;
    }

    private void validateFinishTime(long finishTime) {
        long earliestFinishTime = startedAt > 0L ? startedAt : 1L;
        if (finishTime < earliestFinishTime) {
            throw new IllegalArgumentException("阶段结束时间不能早于开始时间");
        }
    }

    /**
     * 创建当前阶段状态的独立副本，仓储层使用副本隔离外部状态修改。
     *
     * @return 当前阶段快照
     */
    public synchronized RahaStage snapshot() {
        RahaStage copy = new RahaStage(stageId, jobId, stageType, attemptId);
        copy.status = status;
        copy.startedAt = startedAt;
        copy.finishedAt = finishedAt;
        copy.errorCode = errorCode;
        copy.errorMessage = errorMessage;
        return copy;
    }

    public String getStageId() {
        return stageId;
    }

    public String getJobId() {
        return jobId;
    }

    public StageType getStageType() {
        return stageType;
    }

    public int getAttemptId() {
        return attemptId;
    }

    public synchronized StageStatus getStatus() {
        return status;
    }

    public synchronized long getStartedAt() {
        return startedAt;
    }

    public synchronized long getFinishedAt() {
        return finishedAt;
    }

    public synchronized String getErrorCode() {
        return errorCode;
    }

    public synchronized String getErrorMessage() {
        return errorMessage;
    }
}
