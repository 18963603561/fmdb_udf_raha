package com.fiberhome.ml.raha.job.domain;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存 Raha 任务的生命周期状态和失败上下文。
 *
 * <p>状态修改方法使用同步控制，后续任务编排层仍需结合仓储版本实现跨进程并发保护。</p>
 */
public final class RahaJob {

    /** 任务唯一标识。 */
    private final String jobId;
    /** 相同请求重复提交时使用的幂等键。 */
    private final String idempotentKey;
    /** 任务运行类型。 */
    private final JobType jobType;
    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** 输入快照标识，任务创建时允许为空。 */
    private String snapshotId;
    /** 完整任务配置版本。 */
    private final String configVersion;
    /** 任务创建时间。 */
    private final long createdAt;
    /** 当前任务状态。 */
    private JobStatus status;
    /** 当前正在执行或最后执行的阶段标识。 */
    private String currentStageId;
    /** 任务实际开始时间。 */
    private long startedAt;
    /** 任务结束时间。 */
    private long finishedAt;
    /** 任务失败编码。 */
    private String errorCode;
    /** 脱敏后的任务失败说明。 */
    private String errorMessage;

    public RahaJob(String jobId,
                   String idempotentKey,
                   JobType jobType,
                   String datasetId,
                   String snapshotId,
                   String configVersion,
                   long createdAt) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        this.idempotentKey = ValueUtils.requireNotBlank(idempotentKey, "幂等键");
        if (jobType == null) {
            throw new IllegalArgumentException("任务类型不能为空");
        }
        if (createdAt <= 0L) {
            throw new IllegalArgumentException("任务创建时间必须大于 0");
        }
        this.jobType = jobType;
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.snapshotId = snapshotId;
        this.configVersion = ValueUtils.requireNotBlank(configVersion, "配置版本");
        this.createdAt = createdAt;
        this.status = JobStatus.CREATED;
    }

    /**
     * 启动任务并绑定第一个阶段。
     *
     * @param stageId 首个阶段标识
     * @param startTime 开始时间
     */
    public synchronized void start(String stageId, long startTime) {
        if (startTime < createdAt) {
            throw new IllegalArgumentException("任务开始时间不能早于创建时间");
        }
        String validatedStageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
        transitionTo(JobStatus.RUNNING);
        this.currentStageId = validatedStageId;
        this.startedAt = startTime;
    }

    /**
     * 将运行中任务切换到下一个阶段。
     *
     * @param stageId 新阶段标识
     */
    public synchronized void moveToStage(String stageId) {
        if (status != JobStatus.RUNNING) {
            throw new IllegalStateException("只有运行中任务可以切换阶段");
        }
        this.currentStageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
    }

    /**
     * 在数据读取阶段绑定实际输入快照。
     *
     * @param newSnapshotId 实际快照标识
     */
    public synchronized void bindSnapshot(String newSnapshotId) {
        String validatedSnapshotId = ValueUtils.requireNotBlank(newSnapshotId, "快照标识");
        // 同一任务一旦绑定快照，只允许再次绑定相同值，防止任务中途切换输入数据。
        if (snapshotId != null && !snapshotId.trim().isEmpty()
                && !snapshotId.equals(validatedSnapshotId)) {
            throw new IllegalStateException("任务已经绑定其他输入快照");
        }
        if (status != JobStatus.CREATED && status != JobStatus.RUNNING) {
            throw new IllegalStateException("只有未结束任务可以绑定输入快照");
        }
        this.snapshotId = validatedSnapshotId;
    }

    /**
     * 将运行中任务标记为成功。
     *
     * @param finishTime 结束时间
     */
    public synchronized void succeed(long finishTime) {
        validateFinishTime(finishTime);
        transitionTo(JobStatus.SUCCEEDED);
        finish(finishTime, null, null);
    }

    /**
     * 将已创建或运行中任务标记为失败。
     *
     * @param code 失败编码
     * @param message 脱敏失败说明
     * @param finishTime 结束时间
     */
    public synchronized void fail(String code, String message, long finishTime) {
        validateFinishTime(finishTime);
        String validatedCode = ValueUtils.requireNotBlank(code, "失败编码");
        String validatedMessage = ValueUtils.requireNotBlank(message, "失败说明");
        transitionTo(JobStatus.FAILED);
        finish(finishTime, validatedCode, validatedMessage);
    }

    /**
     * 取消已创建或运行中的任务。
     *
     * @param finishTime 取消时间
     */
    public synchronized void cancel(long finishTime) {
        validateFinishTime(finishTime);
        transitionTo(JobStatus.CANCELLED);
        finish(finishTime, null, null);
    }

    private void transitionTo(JobStatus target) {
        // 任务终态不可再次修改，防止成功结果被后续失败尝试覆盖。
        if (!status.canTransitionTo(target)) {
            throw new IllegalStateException("任务状态不允许从 " + status + " 转换为 " + target);
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
        long earliestFinishTime = startedAt > 0L ? startedAt : createdAt;
        if (finishTime < earliestFinishTime) {
            throw new IllegalArgumentException("任务结束时间不能早于开始时间");
        }
    }

    /**
     * 创建当前任务状态的独立副本，仓储层使用副本隔离外部状态修改。
     *
     * @return 当前任务快照
     */
    public synchronized RahaJob snapshot() {
        RahaJob copy = new RahaJob(jobId, idempotentKey, jobType, datasetId,
                snapshotId, configVersion, createdAt);
        copy.status = status;
        copy.currentStageId = currentStageId;
        copy.startedAt = startedAt;
        copy.finishedAt = finishedAt;
        copy.errorCode = errorCode;
        copy.errorMessage = errorMessage;
        return copy;
    }

    public String getJobId() {
        return jobId;
    }

    public String getIdempotentKey() {
        return idempotentKey;
    }

    public JobType getJobType() {
        return jobType;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getConfigVersion() {
        return configVersion;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public synchronized JobStatus getStatus() {
        return status;
    }

    public synchronized String getCurrentStageId() {
        return currentStageId;
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
