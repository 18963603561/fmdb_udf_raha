package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.strategy.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.StrategyPlan;

import java.util.List;

/**
 * 执行当前快照的策略计划并输出候选命中和失败比例。
 */
public final class StrategyRunStageHandler implements StageHandler {

    /** 策略执行服务。 */
    private final StrategyExecutionService executionService;

    public StrategyRunStageHandler(StrategyExecutionService executionService) {
        if (executionService == null) {
            throw new IllegalArgumentException("策略执行服务不能为空");
        }
        this.executionService = executionService;
    }

    @Override
    public StageType getStageType() {
        return StageType.RUN_STRATEGY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object datasetValue = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        Object planValue = context.getAttributes().get(StageAttributeKeys.STRATEGY_PLANS);
        if (!(datasetValue instanceof RahaDataset) || !(planValue instanceof List)) {
            return StageResult.failure("STRATEGY_INPUT_REQUIRED",
                    "策略执行阶段缺少数据集或策略计划", false, 0L, 0L);
        }
        RahaDataset dataset = (RahaDataset) datasetValue;
        List<StrategyPlan> plans = (List<StrategyPlan>) planValue;
        if (plans.isEmpty()) {
            return StageResult.skipped("当前任务没有待执行策略");
        }
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), dataset.getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        StrategyBatchResult result = executionService.execute(
                context.getJob().getJobId(), context.getStage().getStageId(), dataset, plans,
                context.getConfig().getStrategyConfig().getStrategyTimeoutMillis(), version);
        context.getAttributes().put(StageAttributeKeys.STRATEGY_BATCH_RESULT, result);
        context.getAttributes().put(StageAttributeKeys.STRATEGY_HITS, result.getHits());
        if (result.getFailedCount() > 0L) {
            // 单策略失败已被隔离并持久化摘要，由任务失败阈值决定是否继续后续阶段。
            return StageResult.failure("STRATEGY_PARTIAL_FAILURE",
                    "部分策略执行失败", true, result.getFailedCount(), plans.size());
        }
        return StageResult.success();
    }
}
