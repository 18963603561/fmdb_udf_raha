package com.fiberhome.ml.raha.parallel;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存确定性顺序的并行成功结果、失败摘要和实际并发峰值。
 *
 * @param <K> 工作项键类型
 * @param <V> 工作项结果类型
 */
public final class ParallelBatchResult<K, V> {

    /** 按输入顺序保存的成功结果。 */
    private final Map<K, V> successes;
    /** 按输入顺序保存的失败摘要。 */
    private final Map<K, ParallelFailure> failures;
    /** 执行期间实际观察到的最大并发数。 */
    private final int maxObservedConcurrency;

    public ParallelBatchResult(Map<K, V> successes,
                               Map<K, ParallelFailure> failures,
                               int maxObservedConcurrency) {
        if (successes == null || failures == null || maxObservedConcurrency < 0) {
            throw new IllegalArgumentException("并行批次结果参数非法");
        }
        this.successes = Collections.unmodifiableMap(new LinkedHashMap<K, V>(successes));
        this.failures = Collections.unmodifiableMap(
                new LinkedHashMap<K, ParallelFailure>(failures));
        this.maxObservedConcurrency = maxObservedConcurrency;
    }

    public Map<K, V> getSuccesses() { return successes; }
    public Map<K, ParallelFailure> getFailures() { return failures; }
    public int getMaxObservedConcurrency() { return maxObservedConcurrency; }
    public boolean isSuccessful() { return failures.isEmpty(); }
}
