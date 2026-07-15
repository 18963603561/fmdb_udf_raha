package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.detection.BasicDetectionService;
import com.fiberhome.ml.raha.detection.DetectionBatchResult;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.strategy.StrategyHit;

import java.util.List;

/**
 * 使用规则加权模型生成最终检测判断并持久化。
 */
public final class DetectionStageHandler implements StageHandler {

    /** 基础检测服务。 */
    private final BasicDetectionService detectionService;

    public DetectionStageHandler(BasicDetectionService detectionService) {
        if (detectionService == null) {
            throw new IllegalArgumentException("基础检测服务不能为空");
        }
        this.detectionService = detectionService;
    }

    @Override
    public StageType getStageType() {
        return StageType.PREDICT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object featureValue = context.getAttributes().get(StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        Object hitValue = context.getAttributes().get(StageAttributeKeys.STRATEGY_HITS);
        if (!(featureValue instanceof FeatureAssemblyResult) || !(hitValue instanceof List)) {
            return StageResult.failure("DETECTION_INPUT_REQUIRED",
                    "检测阶段缺少特征或策略命中", false, 0L, 0L);
        }
        FeatureAssemblyResult features = (FeatureAssemblyResult) featureValue;
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        DetectionBatchResult result = detectionService.detectAndSave(
                context.getJob().getJobId(), context.getJob().getConfigVersion(),
                context.getStage().getStageId(), features,
                (List<StrategyHit>) hitValue, context.getConfig().getModelConfig(), version);
        context.getAttributes().put(StageAttributeKeys.DETECTION_BATCH_RESULT, result);
        return StageResult.success();
    }
}
