package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.data.StrategyStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 汇总一个策略阶段中的全部独立执行结果。
 */
public final class StrategyBatchResult {

    /** 独立策略执行结果。 */
    private final List<StrategyExecutionResult> executions;

    public StrategyBatchResult(List<StrategyExecutionResult> executions) {
        if (executions == null) {
            throw new IllegalArgumentException("策略执行结果集合不能为空");
        }
        this.executions = Collections.unmodifiableList(
                new ArrayList<StrategyExecutionResult>(executions));
    }

    public List<StrategyExecutionResult> getExecutions() {
        return executions;
    }

    public long getFailedCount() {
        long failed = 0L;
        for (StrategyExecutionResult execution : executions) {
            if (execution.getSummary().getStatus() == StrategyStatus.FAILED) {
                failed++;
            }
        }
        return failed;
    }

    /** 返回全部策略摘要中的命中总数，不创建命中对象汇总列表。 */
    public long getHitCount() {
        long count = 0L;
        for (StrategyExecutionResult execution : executions) {
            count += execution.getSummary().getHitCount();
        }
        return count;
    }

    /** 返回释放命中对象但保留摘要和命中数量的批次结果。 */
    public StrategyBatchResult withoutHits() {
        List<StrategyExecutionResult> summaries =
                new ArrayList<StrategyExecutionResult>(executions.size());
        for (StrategyExecutionResult execution : executions) {
            summaries.add(StrategyExecutionResult.withoutHits(execution));
        }
        return new StrategyBatchResult(summaries);
    }

    public List<StrategyHit> getHits() {
        List<StrategyHit> hits = new ArrayList<StrategyHit>();
        for (StrategyExecutionResult execution : executions) {
            hits.addAll(execution.getHits());
        }
        return Collections.unmodifiableList(hits);
    }
}
