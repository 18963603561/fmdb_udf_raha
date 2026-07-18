package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.strategy.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.StrategyHit;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyRunSummary;

import java.util.List;

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
}
