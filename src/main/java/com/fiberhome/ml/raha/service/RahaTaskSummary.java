package com.fiberhome.ml.raha.service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存服务任务耗时、处理数量和不含原始值的结果摘要。
 */
public final class RahaTaskSummary {

    /** 任务开始时间。 */
    private final long startedAt;
    /** 任务完成时间。 */
    private final long completedAt;
    /** 计划处理对象数量。 */
    private final long totalCount;
    /** 成功处理对象数量。 */
    private final long successfulCount;
    /** 因不可训练等明确原因跳过的对象数量。 */
    private final long skippedCount;
    /** 处理失败对象数量。 */
    private final long failedCount;
    /** 不包含敏感值的阶段摘要。 */
    private final Map<String, String> details;

    public RahaTaskSummary(long startedAt,
                           long completedAt,
                           long totalCount,
                           long successfulCount,
                           long skippedCount,
                           long failedCount,
                           Map<String, String> details) {
        if (startedAt <= 0L || completedAt < startedAt || totalCount < 0L
                || successfulCount < 0L || skippedCount < 0L || failedCount < 0L
                || successfulCount + skippedCount + failedCount > totalCount) {
            throw new IllegalArgumentException("任务摘要时间和数量必须有效");
        }
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.totalCount = totalCount;
        this.successfulCount = successfulCount;
        this.skippedCount = skippedCount;
        this.failedCount = failedCount;
        this.details = details == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(details));
    }

    public long getStartedAt() { return startedAt; }
    public long getCompletedAt() { return completedAt; }
    public long getElapsedMillis() { return completedAt - startedAt; }
    public long getTotalCount() { return totalCount; }
    public long getSuccessfulCount() { return successfulCount; }
    public long getSkippedCount() { return skippedCount; }
    public long getFailedCount() { return failedCount; }
    public Map<String, String> getDetails() { return details; }
}
