package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一个检测阶段的耗时、输入规模、分区和 JVM 堆内存变化基线。
 */
public final class StagePerformanceMetric {

    /** 阶段名称。 */
    private final String stageName;
    /** 阶段执行上下文。 */
    private final StageExecutionContext context;
    /** 阶段耗时毫秒数。 */
    private final long elapsedMillis;
    /** 阶段开始前资源快照。 */
    private final StageResourceSnapshot resourceBefore;
    /** 阶段完成后资源快照。 */
    private final StageResourceSnapshot resourceAfter;
    /** 阶段是否成功。 */
    private final boolean succeeded;
    /** 指标采集完成时间。 */
    private final long measuredAt;

    public StagePerformanceMetric(String stageName,
                                  StageExecutionContext context,
                                  long elapsedMillis,
                                  long usedHeapBeforeBytes,
                                  long usedHeapAfterBytes,
                                  boolean succeeded,
                                  long measuredAt) {
        this(stageName, context, elapsedMillis,
                new StageResourceSnapshot(usedHeapBeforeBytes,
                        StageResourceSnapshot.UNAVAILABLE,
                        StageResourceSnapshot.UNAVAILABLE,
                        StageResourceSnapshot.UNAVAILABLE, measuredAt),
                new StageResourceSnapshot(usedHeapAfterBytes,
                        StageResourceSnapshot.UNAVAILABLE,
                        StageResourceSnapshot.UNAVAILABLE,
                        StageResourceSnapshot.UNAVAILABLE, measuredAt),
                succeeded, measuredAt);
    }

    public StagePerformanceMetric(String stageName,
                                  StageExecutionContext context,
                                  long elapsedMillis,
                                  StageResourceSnapshot resourceBefore,
                                  StageResourceSnapshot resourceAfter,
                                  boolean succeeded,
                                  long measuredAt) {
        this.stageName = ValueUtils.requireNotBlank(stageName, "性能阶段名称");
        if (context == null || elapsedMillis < 0L || resourceBefore == null
                || resourceAfter == null || measuredAt <= 0L) {
            throw new IllegalArgumentException("阶段性能指标必须有效");
        }
        this.context = context;
        this.elapsedMillis = elapsedMillis;
        this.resourceBefore = resourceBefore;
        this.resourceAfter = resourceAfter;
        this.succeeded = succeeded;
        this.measuredAt = measuredAt;
    }

    public long getHeapDeltaBytes() {
        return delta(resourceBefore.getDriverUsedHeapBytes(),
                resourceAfter.getDriverUsedHeapBytes());
    }

    public long getExecutorMemoryDeltaBytes() {
        return delta(resourceBefore.getExecutorUsedMemoryBytes(),
                resourceAfter.getExecutorUsedMemoryBytes());
    }

    public long getDiskDeltaBytes() {
        return delta(resourceBefore.getDiskBytes(), resourceAfter.getDiskBytes());
    }

    public long getNetworkDeltaBytes() {
        return delta(resourceBefore.getNetworkBytes(), resourceAfter.getNetworkBytes());
    }

    private static long delta(long before, long after) {
        return before == StageResourceSnapshot.UNAVAILABLE
                || after == StageResourceSnapshot.UNAVAILABLE
                ? StageResourceSnapshot.UNAVAILABLE : after - before;
    }

    public String getStageName() { return stageName; }
    public StageExecutionContext getContext() { return context; }
    public long getElapsedMillis() { return elapsedMillis; }
    public long getUsedHeapBeforeBytes() { return resourceBefore.getDriverUsedHeapBytes(); }
    public long getUsedHeapAfterBytes() { return resourceAfter.getDriverUsedHeapBytes(); }
    public StageResourceSnapshot getResourceBefore() { return resourceBefore; }
    public StageResourceSnapshot getResourceAfter() { return resourceAfter; }
    public boolean isSucceeded() { return succeeded; }
    public long getMeasuredAt() { return measuredAt; }
}
