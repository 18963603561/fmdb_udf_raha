package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.stage.data.ColumnProfileStageHandler;
import com.fiberhome.ml.raha.job.stage.data.DataLoadStageHandler;
import com.fiberhome.ml.raha.job.stage.feature.FeatureStageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.strategy.StrategyPlanStageHandler;
import com.fiberhome.ml.raha.job.stage.strategy.StrategyRunStageHandler;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import java.util.ArrayList;
import java.util.List;

/**
 * 提供训练、检测和采样共享的数据准备阶段。
 */
abstract class AbstractRahaWorkflow implements RahaWorkflow {

    /** 数据加载器。 */
    private final RahaDatasetLoader datasetLoader;
    /** 字段画像服务。 */
    private final ColumnProfileService profileService;
    /** 策略计划服务。 */
    private final StrategyPlanService planService;
    /** 策略执行服务。 */
    private final StrategyExecutionService executionService;
    /** 特征组装服务。 */
    private final FeatureService featureService;

    AbstractRahaWorkflow(RahaDatasetLoader datasetLoader,
                         ColumnProfileService profileService,
                         StrategyPlanService planService,
                         StrategyExecutionService executionService,
                         FeatureService featureService) {
        if (datasetLoader == null || profileService == null || planService == null
                || executionService == null || featureService == null) {
            throw new IllegalArgumentException("工作流公共数据准备依赖不能为空");
        }
        this.datasetLoader = datasetLoader;
        this.profileService = profileService;
        this.planService = planService;
        this.executionService = executionService;
        this.featureService = featureService;
    }

    final List<StageHandler> preparationStages(RahaTaskExecutionRequest request) {
        List<StageHandler> handlers = basePreparationStages(request);
        handlers.add(new FeatureStageHandler(featureService));
        return handlers;
    }

    /**
     * 创建截至策略执行的公共准备阶段，供采样按列批生成特征。
     */
    final List<StageHandler> basePreparationStages(
            RahaTaskExecutionRequest request) {
        List<StageHandler> handlers = planningStages(request);
        handlers.add(new StrategyRunStageHandler(executionService));
        return handlers;
    }

    /**
     * 创建截至策略计划生成的公共准备阶段。
     */
    final List<StageHandler> planningStages(
            RahaTaskExecutionRequest request) {
        List<StageHandler> handlers = new ArrayList<StageHandler>();
        handlers.add(new DataLoadStageHandler(datasetLoader, request.getDataLoadRequest()));
        handlers.add(new ColumnProfileStageHandler(profileService,
                request.isColumnBatchChild()));
        handlers.add(new StrategyPlanStageHandler(planService));
        return handlers;
    }
}
