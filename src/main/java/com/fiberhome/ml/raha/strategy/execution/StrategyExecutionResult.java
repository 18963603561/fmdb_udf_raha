package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 策略执行结果，失败策略不会返回部分命中，保证结果状态一致。
 */
public final class StrategyExecutionResult {

    /** 运行摘要。 */
    private final StrategyRunSummary summary;
    /** 候选命中列表。 */
    private final List<StrategyHit> hits;

    public StrategyExecutionResult(StrategyRunSummary summary,
                                   List<StrategyHit> hits) {
        this(summary, hits, true);
    }

    private StrategyExecutionResult(StrategyRunSummary summary,
                                    List<StrategyHit> hits,
                                    boolean hitsMaterialized) {
        if (summary == null || hits == null) {
            throw new IllegalArgumentException("策略摘要和命中列表不能为空");
        }
        if (summary.getStatus() != StrategyStatus.SUCCEEDED && !hits.isEmpty()) {
            throw new IllegalArgumentException("失败策略不能保留部分命中");
        }
        if (hitsMaterialized && summary.getHitCount() != hits.size()) {
            throw new IllegalArgumentException("策略摘要命中数量与命中列表不一致");
        }
        if (!hitsMaterialized && !hits.isEmpty()) {
            throw new IllegalArgumentException("未物化命中结果不能包含命中对象");
        }
        this.summary = summary;
        this.hits = Collections.unmodifiableList(new ArrayList<StrategyHit>(hits));
    }

    /** 返回只保留摘要和命中数量的策略结果，用于阶段边界释放命中对象。 */
    public static StrategyExecutionResult withoutHits(StrategyExecutionResult source) {
        if (source == null) {
            throw new IllegalArgumentException("原始策略结果不能为空");
        }
        return new StrategyExecutionResult(source.summary,
                Collections.<StrategyHit>emptyList(), false);
    }

    public StrategyRunSummary getSummary() {
        return summary;
    }

    public List<StrategyHit> getHits() {
        return hits;
    }
}
