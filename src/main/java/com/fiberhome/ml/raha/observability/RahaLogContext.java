package com.fiberhome.ml.raha.observability;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 统一日志关联字段，不包含原始业务值。
 */
public final class RahaLogContext {

    /** 任务标识。 */
    private final String jobId;
    /** 阶段标识。 */
    private final String stageId;
    /** 阶段尝试序号。 */
    private final int attemptId;
    /** 输入快照标识。 */
    private final String snapshotId;

    public RahaLogContext(String jobId, String stageId, int attemptId, String snapshotId) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
        if (attemptId <= 0) {
            throw new IllegalArgumentException("阶段尝试序号必须大于 0");
        }
        this.attemptId = attemptId;
        this.snapshotId = snapshotId == null ? "PENDING_SNAPSHOT" : snapshotId;
    }

    public String getJobId() { return jobId; }
    public String getStageId() { return stageId; }
    public int getAttemptId() { return attemptId; }
    public String getSnapshotId() { return snapshotId; }

    public String toLogText() {
        return "jobId=" + jobId + ",stageId=" + stageId
                + ",attemptId=" + attemptId + ",snapshotId=" + snapshotId;
    }
}
