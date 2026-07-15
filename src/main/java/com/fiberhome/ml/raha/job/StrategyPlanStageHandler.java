package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyPlanService;

import java.util.List;

/**
 * 根据已生成的列画像创建并持久化确定性策略计划。
 */
public final class StrategyPlanStageHandler implements StageHandler {

    /** 策略计划服务。 */
    private final StrategyPlanService planService;

    public StrategyPlanStageHandler(StrategyPlanService planService) {
        if (planService == null) {
            throw new IllegalArgumentException("策略计划服务不能为空");
        }
        this.planService = planService;
    }

    @Override
    public StageType getStageType() {
        return StageType.GENERATE_STRATEGY;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object value = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        if (!(value instanceof RahaDataset)) {
            return StageResult.failure("DATASET_REQUIRED",
                    "策略计划阶段缺少已画像数据集", false, 0L, 0L);
        }
        RahaDataset dataset = (RahaDataset) value;
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), dataset.getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        List<StrategyPlan> plans = planService.generateAndSave(dataset,
                context.getConfig().getStrategyConfig(), version);
        context.getAttributes().put(StageAttributeKeys.STRATEGY_PLANS, plans);
        return plans.isEmpty() ? StageResult.skipped("当前画像没有适用的 OD 或 PVD 策略")
                : StageResult.success();
    }
}
