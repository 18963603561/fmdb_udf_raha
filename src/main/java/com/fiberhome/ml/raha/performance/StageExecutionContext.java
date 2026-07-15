package com.fiberhome.ml.raha.performance;

/**
 * 保存阶段性能指标所需的输入规模、策略数量、分区和缓存上下文。
 */
public final class StageExecutionContext {

    /** 阶段输入行数。 */
    private final long inputRowCount;
    /** 阶段输入数据字段数量。 */
    private final int dataColumnCount;
    /** 阶段执行策略数量。 */
    private final int strategyCount;
    /** 阶段 Spark 分区数量。 */
    private final int partitionCount;
    /** 阶段是否启用缓存。 */
    private final boolean cacheEnabled;

    public StageExecutionContext(long inputRowCount,
                                 int dataColumnCount,
                                 int strategyCount,
                                 int partitionCount,
                                 boolean cacheEnabled) {
        if (inputRowCount < 0L || dataColumnCount < 0 || strategyCount < 0
                || partitionCount <= 0) {
            throw new IllegalArgumentException("阶段性能上下文数量必须有效");
        }
        this.inputRowCount = inputRowCount;
        this.dataColumnCount = dataColumnCount;
        this.strategyCount = strategyCount;
        this.partitionCount = partitionCount;
        this.cacheEnabled = cacheEnabled;
    }

    public long getInputRowCount() { return inputRowCount; }
    public int getDataColumnCount() { return dataColumnCount; }
    public int getStrategyCount() { return strategyCount; }
    public int getPartitionCount() { return partitionCount; }
    public boolean isCacheEnabled() { return cacheEnabled; }
}
