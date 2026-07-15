package com.fiberhome.ml.raha.performance;

import java.time.Clock;

/**
 * 本地默认资源探针，只采集 Driver 堆内存，其他集群指标明确标记为不可用。
 */
public final class JvmStageResourceProbe implements StageResourceProbe {

    /** 提供可测试资源快照时间的时钟。 */
    private final Clock clock;

    public JvmStageResourceProbe(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("JVM 资源探针时钟不能为空");
        }
        this.clock = clock;
    }

    @Override
    public StageResourceSnapshot capture() {
        Runtime runtime = Runtime.getRuntime();
        long usedHeap = Math.max(0L,
                runtime.totalMemory() - runtime.freeMemory());
        return new StageResourceSnapshot(usedHeap,
                StageResourceSnapshot.UNAVAILABLE,
                StageResourceSnapshot.UNAVAILABLE,
                StageResourceSnapshot.UNAVAILABLE,
                Math.max(1L, clock.millis()));
    }
}
