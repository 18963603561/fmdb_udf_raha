package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 描述阶段成功、跳过或失败结果及失败比例和快照信息。
 */
public final class StageResult {

    /** 阶段结果类型。 */
    private final StageOutcome outcome;
    /** 失败编码。 */
    private final String errorCode;
    /** 脱敏后的结果说明。 */
    private final String message;
    /** 失败是否允许重试或按阈值继续。 */
    private final boolean recoverable;
    /** 阶段失败数据项数量。 */
    private final long failedItemCount;
    /** 阶段处理数据项总数。 */
    private final long totalItemCount;
    /** 数据加载阶段生成的实际快照标识。 */
    private final String snapshotId;

    private StageResult(StageOutcome outcome,
                        String errorCode,
                        String message,
                        boolean recoverable,
                        long failedItemCount,
                        long totalItemCount,
                        String snapshotId) {
        if (outcome == null) {
            throw new IllegalArgumentException("阶段结果类型不能为空");
        }
        if (failedItemCount < 0L || totalItemCount < 0L || failedItemCount > totalItemCount) {
            throw new IllegalArgumentException("阶段失败数量和总数量非法");
        }
        if ((outcome == StageOutcome.FAILED || outcome == StageOutcome.PARTIAL_SUCCESS)
                && isBlank(errorCode)) {
            throw new IllegalArgumentException("失败或部分成功阶段必须包含错误编码");
        }
        this.outcome = outcome;
        this.errorCode = errorCode;
        this.message = message;
        this.recoverable = recoverable;
        this.failedItemCount = failedItemCount;
        this.totalItemCount = totalItemCount;
        this.snapshotId = snapshotId;
    }

    public static StageResult success() {
        return new StageResult(StageOutcome.SUCCESS, null, null,
                false, 0L, 0L, null);
    }

    public static StageResult successWithSnapshot(String snapshotId) {
        return new StageResult(StageOutcome.SUCCESS, null, null,
                false, 0L, 0L, ValueUtils.requireNotBlank(snapshotId, "快照标识"));
    }

    public static StageResult skipped(String message) {
        return new StageResult(StageOutcome.SKIPPED, null, message,
                false, 0L, 0L, null);
    }

    /**
     * 创建包含可用结果和部分失败摘要的阶段结果。
     *
     * @param errorCode 部分失败编码
     * @param message 脱敏后的部分失败说明
     * @param failedItemCount 失败数据项数量
     * @param totalItemCount 总处理数据项数量
     * @return 部分成功阶段结果
     */
    public static StageResult partialSuccess(String errorCode,
                                             String message,
                                             long failedItemCount,
                                             long totalItemCount) {
        return new StageResult(StageOutcome.PARTIAL_SUCCESS,
                ValueUtils.requireNotBlank(errorCode, "部分失败编码"), message,
                false, failedItemCount, totalItemCount, null);
    }

    public static StageResult failure(String errorCode,
                                      String message,
                                      boolean recoverable,
                                      long failedItemCount,
                                      long totalItemCount) {
        return new StageResult(StageOutcome.FAILED,
                ValueUtils.requireNotBlank(errorCode, "失败编码"), message,
                recoverable, failedItemCount, totalItemCount, null);
    }

    public StageOutcome getOutcome() {
        return outcome;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getMessage() {
        return message;
    }

    public boolean isRecoverable() {
        return recoverable;
    }

    public long getFailedItemCount() {
        return failedItemCount;
    }

    public long getTotalItemCount() {
        return totalItemCount;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public double failedRatio() {
        if (totalItemCount <= 0L) {
            return outcome == StageOutcome.FAILED ? 1.0d : 0.0d;
        }
        return (double) failedItemCount / (double) totalItemCount;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
