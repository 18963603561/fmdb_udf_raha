package com.fiberhome.ml.raha.checkpoint;

import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一个阶段尝试的输入版本、输出位置、摘要、状态和错误审计信息。
 */
public final class StageCheckpoint {

    /** 检查点稳定标识。 */
    private final String checkpointId;
    /** 所属任务标识。 */
    private final String jobId;
    /** 阶段业务类型。 */
    private final StageType stageType;
    /** 尝试序号，从一开始。 */
    private final int attemptId;
    /** 阶段输入配置和快照版本。 */
    private final ArtifactVersion inputVersion;
    /** 输入内容和依赖的稳定指纹。 */
    private final String inputFingerprint;
    /** 当前检查点状态。 */
    private final StageCheckpointStatus status;
    /** 成功输出逻辑位置。 */
    private final String outputLocation;
    /** 不含敏感值的阶段摘要。 */
    private final Map<String, String> summary;
    /** 失败错误码。 */
    private final String errorCode;
    /** 安全失败摘要。 */
    private final String errorMessage;
    /** 当前尝试开始时间。 */
    private final long startedAt;
    /** 当前尝试结束时间，运行中为零。 */
    private final long completedAt;

    private StageCheckpoint(String jobId,
                            StageType stageType,
                            int attemptId,
                            ArtifactVersion inputVersion,
                            String inputFingerprint,
                            StageCheckpointStatus status,
                            String outputLocation,
                            Map<String, String> summary,
                            String errorCode,
                            String errorMessage,
                            long startedAt,
                            long completedAt) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "检查点任务标识");
        this.inputFingerprint = ValueUtils.requireNotBlank(
                inputFingerprint, "检查点输入指纹");
        if (stageType == null || attemptId <= 0 || inputVersion == null
                || status == null || startedAt <= 0L || completedAt < 0L
                || (completedAt > 0L && completedAt < startedAt)) {
            throw new IllegalArgumentException("检查点阶段、版本、尝试和时间必须有效");
        }
        if (status == StageCheckpointStatus.SUCCEEDED
                && (isBlank(outputLocation) || completedAt == 0L)) {
            throw new IllegalArgumentException("成功检查点必须包含输出位置和完成时间");
        }
        if (status == StageCheckpointStatus.FAILED
                && (isBlank(errorCode) || isBlank(errorMessage) || completedAt == 0L)) {
            throw new IllegalArgumentException("失败检查点必须包含错误信息和完成时间");
        }
        if (status == StageCheckpointStatus.RUNNING && completedAt != 0L) {
            throw new IllegalArgumentException("运行中检查点不能包含完成时间");
        }
        this.stageType = stageType;
        this.attemptId = attemptId;
        this.inputVersion = inputVersion;
        this.status = status;
        this.outputLocation = outputLocation;
        this.summary = summary == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(summary));
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.checkpointId = HashUtils.md5Hex(this.jobId + "|" + stageType.name()
                + "|" + attemptId + "|" + inputFingerprint);
    }

    public static StageCheckpoint running(String jobId,
                                          StageType stageType,
                                          int attemptId,
                                          ArtifactVersion inputVersion,
                                          String inputFingerprint,
                                          long startedAt) {
        return new StageCheckpoint(jobId, stageType, attemptId, inputVersion,
                inputFingerprint, StageCheckpointStatus.RUNNING, null, null,
                null, null, startedAt, 0L);
    }

    /**
     * 创建当前尝试的成功终态快照。
     */
    public StageCheckpoint succeed(String outputLocation,
                                   Map<String, String> summary,
                                   long completedAt) {
        if (status != StageCheckpointStatus.RUNNING) {
            throw new IllegalStateException("只有运行中检查点可以成功完成");
        }
        return new StageCheckpoint(jobId, stageType, attemptId, inputVersion,
                inputFingerprint, StageCheckpointStatus.SUCCEEDED, outputLocation,
                summary, null, null, startedAt, completedAt);
    }

    /**
     * 创建当前尝试的失败终态快照。
     */
    public StageCheckpoint fail(String errorCode,
                                String errorMessage,
                                Map<String, String> summary,
                                long completedAt) {
        if (status != StageCheckpointStatus.RUNNING) {
            throw new IllegalStateException("只有运行中检查点可以失败完成");
        }
        return new StageCheckpoint(jobId, stageType, attemptId, inputVersion,
                inputFingerprint, StageCheckpointStatus.FAILED, null, summary,
                errorCode, errorMessage, startedAt, completedAt);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public String getCheckpointId() { return checkpointId; }
    public String getJobId() { return jobId; }
    public StageType getStageType() { return stageType; }
    public int getAttemptId() { return attemptId; }
    public ArtifactVersion getInputVersion() { return inputVersion; }
    public String getInputFingerprint() { return inputFingerprint; }
    public StageCheckpointStatus getStatus() { return status; }
    public String getOutputLocation() { return outputLocation; }
    public Map<String, String> getSummary() { return summary; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public long getStartedAt() { return startedAt; }
    public long getCompletedAt() { return completedAt; }
}
