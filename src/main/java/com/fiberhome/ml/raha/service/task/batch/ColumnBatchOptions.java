package com.fiberhome.ml.raha.service.task.batch;

/**
 * 保存训练和检测入口的列批执行参数。
 */
public final class ColumnBatchOptions {

    /** 禁用列批时使用的固定参数。 */
    private static final ColumnBatchOptions DISABLED = new ColumnBatchOptions(
            0, 1, false, false);

    /** 每个子任务处理的最大业务字段数，非正数表示禁用列批。 */
    private final int columnBatchSize;
    /** 同时执行的列批数量，第一阶段仅允许串行。 */
    private final int maxParallelColumnBatches;
    /** 是否允许在当前列批内部生成 RVD 策略。 */
    private final boolean batchRvdEnabled;
    /** 单个列批失败后是否立即停止剩余列批。 */
    private final boolean failFastColumnBatch;

    public ColumnBatchOptions(int columnBatchSize,
                              int maxParallelColumnBatches,
                              boolean batchRvdEnabled,
                              boolean failFastColumnBatch) {
        if (maxParallelColumnBatches <= 0) {
            throw new IllegalArgumentException("列批并发数必须大于零");
        }
        if (maxParallelColumnBatches != 1) {
            throw new IllegalArgumentException("当前阶段列批并发数只支持 1");
        }
        this.columnBatchSize = columnBatchSize;
        this.maxParallelColumnBatches = maxParallelColumnBatches;
        this.batchRvdEnabled = batchRvdEnabled;
        this.failFastColumnBatch = failFastColumnBatch;
    }

    /**
     * 返回禁用列批的兼容参数。
     *
     * @return 禁用列批参数
     */
    public static ColumnBatchOptions disabled() {
        return DISABLED;
    }

    public int getColumnBatchSize() {
        return columnBatchSize;
    }

    public int getMaxParallelColumnBatches() {
        return maxParallelColumnBatches;
    }

    public boolean isBatchRvdEnabled() {
        return batchRvdEnabled;
    }

    public boolean isFailFastColumnBatch() {
        return failFastColumnBatch;
    }

    public boolean isEnabled() {
        return columnBatchSize > 0;
    }

    /**
     * 生成进入任务幂等指纹的稳定文本。
     *
     * @return 列批参数规范文本
     */
    public String toCanonicalString() {
        return columnBatchSize + "|" + maxParallelColumnBatches + "|"
                + batchRvdEnabled + "|IN_BATCH|" + failFastColumnBatch;
    }
}
