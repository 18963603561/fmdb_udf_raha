package com.fiberhome.ml.raha.job.stage.feature;

import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.List;

/**
 * 将策略命中和上下文转换为稀疏特征并持久化。
 */
public final class FeatureStageHandler implements StageHandler {

    /** 特征服务。 */
    private final FeatureService featureService;

    public FeatureStageHandler(FeatureService featureService) {
        if (featureService == null) {
            throw new IllegalArgumentException("特征服务不能为空");
        }
        this.featureService = featureService;
    }

    @Override
    public StageType getStageType() {
        return StageType.GENERATE_FEATURE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object datasetValue = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        Object planValue = context.getAttributes().get(StageAttributeKeys.STRATEGY_PLANS);
        Object hitValue = context.getAttributes().get(StageAttributeKeys.STRATEGY_HITS);
        if (!(datasetValue instanceof RahaDataset) || !(planValue instanceof List)
                || !(hitValue instanceof List)) {
            return StageResult.failure("FEATURE_INPUT_REQUIRED",
                    "特征阶段缺少数据集、策略计划或策略命中", false, 0L, 0L);
        }
        RahaDataset dataset = (RahaDataset) datasetValue;
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), dataset.getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        FeatureAssemblyResult result = featureService.assembleAndSave(
                context.getJob().getJobId(), dataset,
                (List<StrategyPlan>) planValue, (List<StrategyHit>) hitValue,
                context.getConfig().getFeatureConfig(), version);
        context.getAttributes().put(StageAttributeKeys.FEATURE_ASSEMBLY_RESULT, result);
        return result.getRows().isEmpty()
                ? StageResult.skipped("当前数据没有可区分特征") : StageResult.success();
    }
}
