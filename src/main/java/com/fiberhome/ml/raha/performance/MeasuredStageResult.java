package com.fiberhome.ml.raha.performance;

/**
 * 同时返回阶段业务结果和对应性能指标。
 *
 * @param <T> 阶段业务结果类型
 */
public final class MeasuredStageResult<T> {

    /** 阶段业务结果。 */
    private final T result;
    /** 阶段性能指标。 */
    private final StagePerformanceMetric metric;

    public MeasuredStageResult(T result, StagePerformanceMetric metric) {
        if (metric == null) {
            throw new IllegalArgumentException("阶段性能指标不能为空");
        }
        this.result = result;
        this.metric = metric;
    }

    public T getResult() { return result; }
    public StagePerformanceMetric getMetric() { return metric; }
}
