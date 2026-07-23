package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.List;
import java.util.Set;

/**
 * 持久化策略计划、候选命中和运行摘要的统一仓储契约。
 */
public interface StrategyRepository {

    void savePlans(String datasetId,
                   String snapshotId,
                   List<StrategyPlan> plans,
                   ArtifactVersion version,
                   long updatedAt);

    List<StrategyPlan> findPlans(String datasetId, String snapshotId);

    void saveExecution(StrategyExecutionResult result,
                       ArtifactVersion version,
                       long updatedAt);

    List<StrategyHit> findHits(String jobId);

    List<StrategyRunSummary> findSummaries(String jobId);

    /**
     * 释放已经生成特征的策略命中缓存，保留轻量运行摘要。
     */
    default void releaseHits(String jobId, Set<String> strategyIds) {
        // 不维护任务级命中缓存的仓储无需处理。
    }
}
