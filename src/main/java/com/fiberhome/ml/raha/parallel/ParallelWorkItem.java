package com.fiberhome.ml.raha.parallel;

import java.util.concurrent.Callable;

/**
 * 保存一个具有稳定业务键的并行工作项。
 *
 * @param <K> 工作项键类型
 * @param <V> 工作项结果类型
 */
public final class ParallelWorkItem<K, V> {

    /** 工作项稳定键。 */
    private final K key;
    /** 实际业务调用。 */
    private final Callable<V> callable;

    public ParallelWorkItem(K key, Callable<V> callable) {
        if (key == null || callable == null) {
            throw new IllegalArgumentException("并行工作项键和调用不能为空");
        }
        this.key = key;
        this.callable = callable;
    }

    public K getKey() { return key; }
    public Callable<V> getCallable() { return callable; }
}
