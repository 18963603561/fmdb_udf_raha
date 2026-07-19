package com.fiberhome.ml.raha.job.stage.detection;

import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.detection.service.BasicDetectionService;
import com.fiberhome.ml.raha.detection.service.DetectionBatchResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import java.util.List;

/**
 * 使用规则加权评分生成检测结果，不加载已发布列级模型。
 */
public final class RuleDetectionStageHandler implements StageHandler {

    /** 基础规则检测服务。 */
    private final BasicDetectionService detectionService;

    public RuleDetectionStageHandler(BasicDetectionService detectionService) {
        if (detectionService == null) {
            throw new IllegalArgumentException("基础规则检测服务不能为空");
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
        Object featureValue = context.getAttributes().get(
                StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        Object hitValue = context.getAttributes().get(StageAttributeKeys.STRATEGY_HITS);
        if (!(featureValue instanceof FeatureAssemblyResult) || !(hitValue instanceof List)) {
            return StageResult.failure("DETECTION_INPUT_REQUIRED",
                    "规则检测阶段缺少特征或策略命中", false, 0L, 0L);
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
