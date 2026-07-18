package com.fiberhome.ml.raha.service.common;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一次业务服务执行的数量、耗时和诊断摘要。
 */
public final class RahaServiceSummary {

    /** 服务开始时间。 */
    private final long startedAt;
    /** 服务完成时间。 */
    private final long completedAt;
    /** 总处理数量。 */
    private final long totalCount;
    /** 成功处理数量。 */
    private final long successfulCount;
    /** 跳过处理数量。 */
    private final long skippedCount;
    /** 失败处理数量。 */
    private final long failedCount;
    /** 不包含敏感值的服务诊断详情。 */
    private final Map<String, String> details;

    public RahaServiceSummary(long startedAt,
                              long completedAt,
                              long totalCount,
                              long successfulCount,
                              long skippedCount,
                              long failedCount,
                              Map<String, String> details) {
        if (startedAt < 0L || completedAt < startedAt || totalCount < 0L
                || successfulCount < 0L || skippedCount < 0L || failedCount < 0L
                || successfulCount + skippedCount + failedCount > totalCount) {
            throw new IllegalArgumentException("服务摘要时间或数量非法");
        }
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.totalCount = totalCount;
        this.successfulCount = successfulCount;
        this.skippedCount = skippedCount;
        this.failedCount = failedCount;
        this.details = details == null ? Collections.<String, String>emptyMap()
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
