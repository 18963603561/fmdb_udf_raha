package com.fiberhome.ml.raha.job.stage.detection;

import com.fiberhome.ml.raha.job.stage.core.ServiceStageResultMapper;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.detect.RahaDetectRequest;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;

/**
 * 加载兼容的已发布列模型并执行生产预测。
 */
public final class PublishedModelDetectionStageHandler implements StageHandler {

    /** 已发布模型检测服务。 */
    private final RahaDetectService detectService;

    public PublishedModelDetectionStageHandler(RahaDetectService detectService) {
        if (detectService == null) {
            throw new IllegalArgumentException("已发布模型检测服务不能为空");
        }
        this.detectService = detectService;
    }

    @Override
    public StageType getStageType() {
        return StageType.PREDICT;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object datasetValue = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        Object featureValue = context.getAttributes().get(
                StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        Object planVersionValue = context.getAttributes().get(
                StageAttributeKeys.STRATEGY_PLAN_VERSION);
        if (!(datasetValue instanceof RahaDataset)
                || !(featureValue instanceof FeatureAssemblyResult)
                || !(planVersionValue instanceof String)) {
            return StageResult.failure("PREDICTION_INPUT_REQUIRED",
                    "模型预测阶段缺少数据、特征或策略计划版本", false, 0L, 0L);
        }
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        RahaServiceResult<RahaDetectOutput> result = detectService.detect(
                new RahaDetectRequest(context.getJob().getJobId(),
                        context.getStage().getStageId(), context.getJob().getConfigVersion(),
                        (RahaDataset) datasetValue, (FeatureAssemblyResult) featureValue,
                        (String) planVersionValue, version,
                        context.getConfig().getResourceConfig()));
        context.getAttributes().put(StageAttributeKeys.DETECT_SERVICE_RESULT, result);
        if (result.getPayload() != null) {
            context.getAttributes().put(StageAttributeKeys.DETECT_OUTPUT, result.getPayload());
        }
        return ServiceStageResultMapper.map(result);
    }
}
