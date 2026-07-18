package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.stage.ClusterStageHandler;
import com.fiberhome.ml.raha.job.stage.DirectLabelStageHandler;
import com.fiberhome.ml.raha.job.stage.EvaluationStageHandler;
import com.fiberhome.ml.raha.job.stage.LabelPropagationStageHandler;
import com.fiberhome.ml.raha.job.stage.ModelTrainingStageHandler;
import com.fiberhome.ml.raha.job.stage.ResultPersistenceStageHandler;
import com.fiberhome.ml.raha.job.stage.StageHandler;
import com.fiberhome.ml.raha.job.stage.StageAttributeKeys;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.service.train.RahaTrainService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import java.util.Collections;
import java.util.List;

/**
 * 创建从数据加载到候选模型持久化的训练工作流。
 */
public final class TrainingWorkflow extends AbstractRahaWorkflow {

    /** 列内聚类服务。 */
    private final ColumnClusteringService clusteringService;
    /** 标签传播服务。 */
    private final LabelPropagationService propagationService;
    /** 模型训练服务。 */
    private final RahaTrainService trainService;

    public TrainingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            LabelPropagationService propagationService,
                            RahaTrainService trainService) {
        super(datasetLoader, profileService, planService, executionService, featureService);
        if (clusteringService == null || propagationService == null || trainService == null) {
            throw new IllegalArgumentException("训练工作流业务依赖不能为空");
        }
        this.clusteringService = clusteringService;
        this.propagationService = propagationService;
        this.trainService = trainService;
    }

    @Override
    public JobType getJobType() {
        return JobType.TRAINING;
    }

    @Override
    public List<StageHandler> createStageHandlers(RahaTaskExecutionRequest request) {
        if (request == null || request.getConfig().getJobType() != JobType.TRAINING) {
            throw new IllegalArgumentException("训练工作流请求类型不匹配");
        }
        List<StageHandler> handlers = preparationStages(request);
        handlers.add(new ClusterStageHandler(clusteringService));
        handlers.add(new DirectLabelStageHandler(request.getLabels()));
        handlers.add(new LabelPropagationStageHandler(propagationService,
                request.getPropagationMethod(), request.getPropagationConfig()));
        handlers.add(new ModelTrainingStageHandler(trainService,
                request.getTrainingConfig(), request.getModelNamePrefix(),
                request.getPropagationMethod(), request.getPropagationConfig()));
        if (request.getEvaluator() != null) {
            handlers.add(new EvaluationStageHandler(request.getEvaluator()));
        }
        handlers.add(new ResultPersistenceStageHandler(
                StageAttributeKeys.TRAIN_SERVICE_RESULT));
        return Collections.unmodifiableList(handlers);
    }
}
