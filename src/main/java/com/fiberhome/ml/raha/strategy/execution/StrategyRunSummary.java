package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一次策略运行的输入规模、命中数量、耗时和失败摘要。
 */
public final class StrategyRunSummary {

    /** 所属任务标识。 */
    private final String jobId;
    /** 所属阶段标识。 */
    private final String stageId;
    /** 输入快照标识。 */
    private final String snapshotId;
    /** 策略标识。 */
    private final String strategyId;
    /** 策略配置哈希。 */
    private final String configurationHash;
    /** 策略族。 */
    private final StrategyFamily strategyFamily;
    /** 策略运行状态。 */
    private final StrategyStatus status;
    /** 输入单元格数量。 */
    private final long inputCount;
    /** 候选命中数量。 */
    private final long hitCount;
    /** 运行耗时，单位毫秒。 */
    private final long runtimeMillis;
    /** 失败编码。 */
    private final String errorCode;
    /** 脱敏后的失败摘要。 */
    private final String errorMessage;
    /** 完成时间。 */
    private final long completedAt;

    public StrategyRunSummary(String jobId,
                              String stageId,
                              String snapshotId,
                              String strategyId,
                              String configurationHash,
                              StrategyFamily strategyFamily,
                              StrategyStatus status,
                              long inputCount,
                              long hitCount,
                              long runtimeMillis,
                              String errorCode,
                              String errorMessage,
                              long completedAt) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
        this.snapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        this.strategyId = ValueUtils.requireNotBlank(strategyId, "策略标识");
        this.configurationHash = ValueUtils.requireNotBlank(configurationHash, "配置哈希");
        if (strategyFamily == null || status == null) {
            throw new IllegalArgumentException("策略族和运行状态不能为空");
        }
        if (inputCount < 0L || hitCount < 0L || runtimeMillis < 0L || completedAt <= 0L) {
            throw new IllegalArgumentException("策略运行统计不能为负数");
        }
        if (status == StrategyStatus.FAILED
                && (errorCode == null || errorCode.trim().isEmpty())) {
            throw new IllegalArgumentException("失败策略摘要必须包含错误编码");
        }
        this.strategyFamily = strategyFamily;
        this.status = status;
        this.inputCount = inputCount;
        this.hitCount = hitCount;
        this.runtimeMillis = runtimeMillis;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.completedAt = completedAt;
    }

    public String getJobId() { return jobId; }
    public String getStageId() { return stageId; }
    public String getSnapshotId() { return snapshotId; }
    public String getStrategyId() { return strategyId; }
    public String getConfigurationHash() { return configurationHash; }
    public StrategyFamily getStrategyFamily() { return strategyFamily; }
    public StrategyStatus getStatus() { return status; }
    public long getInputCount() { return inputCount; }
    public long getHitCount() { return hitCount; }
    public long getRuntimeMillis() { return runtimeMillis; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public long getCompletedAt() { return completedAt; }
}
