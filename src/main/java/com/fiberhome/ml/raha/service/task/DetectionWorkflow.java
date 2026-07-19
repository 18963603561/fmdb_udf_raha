package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.stage.model.EvaluationStageHandler;
import com.fiberhome.ml.raha.job.stage.detection.PublishedModelDetectionStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceStageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import java.util.Collections;
import java.util.List;

/**
 * 创建从数据加载到已发布模型预测的生产检测工作流。
 */
public final class DetectionWorkflow extends AbstractRahaWorkflow {

    /** 已发布模型检测服务。 */
    private final RahaDetectService detectService;

    public DetectionWorkflow(RahaDatasetLoader datasetLoader,
                             ColumnProfileService profileService,
                             StrategyPlanService planService,
                             StrategyExecutionService executionService,
                             FeatureService featureService,
                             RahaDetectService detectService) {
        super(datasetLoader, profileService, planService, executionService, featureService);
        if (detectService == null) {
            throw new IllegalArgumentException("检测工作流服务不能为空");
        }
        this.detectService = detectService;
    }

    @Override
    public JobType getJobType() {
        return JobType.DETECTION;
    }

    @Override
    public List<StageHandler> createStageHandlers(RahaTaskExecutionRequest request) {
        if (request == null || request.getConfig().getJobType() != JobType.DETECTION) {
            throw new IllegalArgumentException("检测工作流请求类型不匹配");
        }
        List<StageHandler> handlers = preparationStages(request);
        handlers.add(new PublishedModelDetectionStageHandler(detectService));
        if (request.getEvaluator() != null) {
            handlers.add(new EvaluationStageHandler(request.getEvaluator()));
        }
        handlers.add(new ResultPersistenceStageHandler(
                StageAttributeKeys.DETECT_SERVICE_RESULT));
        return Collections.unmodifiableList(handlers);
    }
}
