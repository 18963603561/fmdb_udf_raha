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

    public List<StrategyHit> getHits() {
        List<StrategyHit> hits = new ArrayList<StrategyHit>();
        for (StrategyExecutionResult execution : executions) {
            hits.addAll(execution.getHits());
        }
        return Collections.unmodifiableList(hits);
    }
}
