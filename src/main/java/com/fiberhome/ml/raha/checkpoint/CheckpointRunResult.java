package com.fiberhome.ml.raha.checkpoint;

/**
 * 汇总阶段检查点执行或复用结果。
 *
 * @param <T> 新执行成功时的内存结果载荷类型
 */
public final class CheckpointRunResult<T> {

    /** 阶段是否成功完成。 */
    private final boolean succeeded;
    /** 是否直接复用了历史成功检查点。 */
    private final boolean reused;
    /** 新执行成功时的结果载荷，复用时为空。 */
    private final T payload;
    /** 成功输出的逻辑持久化位置。 */
    private final String outputLocation;
    /** 本次调用实际执行的尝试次数。 */
    private final int executedAttempts;
    /** 本次调用最终关联的检查点。 */
    private final StageCheckpoint checkpoint;
    /** 最终失败错误码。 */
    private final String errorCode;
    /** 最终失败安全摘要。 */
    private final String errorMessage;

    private CheckpointRunResult(boolean succeeded,
                                boolean reused,
                                T payload,
                                String outputLocation,
                                int executedAttempts,
                                StageCheckpoint checkpoint,
                                String errorCode,
                                String errorMessage) {
        if (checkpoint == null || executedAttempts < 0) {
            throw new IllegalArgumentException("检查点运行结果参数无效");
        }
        this.succeeded = succeeded;
        this.reused = reused;
        this.payload = payload;
        this.outputLocation = outputLocation;
        this.executedAttempts = executedAttempts;
        this.checkpoint = checkpoint;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    static <T> CheckpointRunResult<T> succeeded(T payload,
                                                StageCheckpoint checkpoint,
                                                int executedAttempts) {
        return new CheckpointRunResult<T>(true, false, payload,
                checkpoint.getOutputLocation(), executedAttempts, checkpoint, null, null);
    }

    static <T> CheckpointRunResult<T> reused(StageCheckpoint checkpoint) {
        return new CheckpointRunResult<T>(true, true, null,
                checkpoint.getOutputLocation(), 0, checkpoint, null, null);
    }

    static <T> CheckpointRunResult<T> failed(StageCheckpoint checkpoint,
                                             int executedAttempts) {
        return new CheckpointRunResult<T>(false, false, null, null,
                executedAttempts, checkpoint,
                checkpoint.getErrorCode(), checkpoint.getErrorMessage());
    }

    public boolean isSucceeded() { return succeeded; }
    public boolean isReused() { return reused; }
    public T getPayload() { return payload; }
    public String getOutputLocation() { return outputLocation; }
    public int getExecutedAttempts() { return executedAttempts; }
    public StageCheckpoint getCheckpoint() { return checkpoint; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
}
