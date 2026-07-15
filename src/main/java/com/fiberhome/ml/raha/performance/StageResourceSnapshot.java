package com.fiberhome.ml.raha.performance;

/**
 * 保存阶段资源探针采集的 Driver、Executor、磁盘和网络累计计数。
 */
public final class StageResourceSnapshot {

    /** 无法从当前运行环境采集指标时使用的固定值。 */
    public static final long UNAVAILABLE = -1L;
    /** Driver 已用堆内存字节数。 */
    private final long driverUsedHeapBytes;
    /** Executor 已用内存字节数。 */
    private final long executorUsedMemoryBytes;
    /** 累计磁盘读写字节数。 */
    private final long diskBytes;
    /** 累计网络收发字节数。 */
    private final long networkBytes;
    /** 快照采集时间。 */
    private final long capturedAt;

    public StageResourceSnapshot(long driverUsedHeapBytes,
                                 long executorUsedMemoryBytes,
                                 long diskBytes,
                                 long networkBytes,
                                 long capturedAt) {
        if (driverUsedHeapBytes < UNAVAILABLE
                || executorUsedMemoryBytes < UNAVAILABLE
                || diskBytes < UNAVAILABLE || networkBytes < UNAVAILABLE
                || capturedAt <= 0L) {
            throw new IllegalArgumentException("阶段资源快照计数必须有效");
        }
        this.driverUsedHeapBytes = driverUsedHeapBytes;
        this.executorUsedMemoryBytes = executorUsedMemoryBytes;
        this.diskBytes = diskBytes;
        this.networkBytes = networkBytes;
        this.capturedAt = capturedAt;
    }

    public long getDriverUsedHeapBytes() { return driverUsedHeapBytes; }
    public long getExecutorUsedMemoryBytes() { return executorUsedMemoryBytes; }
    public long getDiskBytes() { return diskBytes; }
    public long getNetworkBytes() { return networkBytes; }
    public long getCapturedAt() { return capturedAt; }
}
