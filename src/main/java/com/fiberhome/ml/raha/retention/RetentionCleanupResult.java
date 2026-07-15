package com.fiberhome.ml.raha.retention;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一次保留策略执行的各表删除数量和总删除数量。
 */
public final class RetentionCleanupResult {

    /** 各表实际删除数量。 */
    private final Map<String, Long> deletedByTable;
    /** 所有表实际删除总数。 */
    private final long totalDeleted;

    public RetentionCleanupResult(Map<String, Long> deletedByTable) {
        if (deletedByTable == null) {
            throw new IllegalArgumentException("清理结果不能为空");
        }
        long total = 0L;
        for (Long count : deletedByTable.values()) {
            if (count == null || count < 0L) {
                throw new IllegalArgumentException("清理数量不能为负数");
            }
            total += count;
        }
        this.deletedByTable = Collections.unmodifiableMap(
                new LinkedHashMap<String, Long>(deletedByTable));
        this.totalDeleted = total;
    }

    public Map<String, Long> getDeletedByTable() { return deletedByTable; }
    public long getTotalDeleted() { return totalDeleted; }
}
