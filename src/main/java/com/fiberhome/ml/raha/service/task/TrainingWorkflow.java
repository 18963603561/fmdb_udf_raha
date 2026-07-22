package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.stage.checkpoint.RestoreSnapshotCheckpointStageHandler;
import com.fiberhome.ml.raha.job.stage.feature.ClusterStageHandler;
import com.fiberhome.ml.raha.job.stage.data.TrainingInputMergeStageHandler;
import com.fiberhome.ml.raha.job.stage.label.DirectLabelStageHandler;
import com.fiberhome.ml.raha.job.stage.model.EvaluationStageHandler;
import com.fiberhome.ml.raha.job.stage.label.LabelPropagationStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ModelTrainingStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceVerifier;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.service.train.RahaTrainService;
import com.fiberhome.ml.raha.service.train.TrainingInputMergeService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import java.util.ArrayList;
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
    /** 可选 c1、标注批次和 o1 合并服务。 */
    private final TrainingInputMergeService inputMergeService;
    /** 可选最终物理结果回读验证器。 */
    private final ResultPersistenceVerifier resultPersistenceVerifier;
    /** 可选快照检查点仓储，用于训练复用采样前置产物。 */
    private final SnapshotCheckpointRepository snapshotCheckpointRepository;

    public TrainingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            LabelPropagationService propagationService,
                            RahaTrainService trainService) {
        this(datasetLoader, profileService, planService, executionService,
                featureService, clusteringService, propagationService,
                trainService, null, null, null);
    }

    /**
     * 创建支持持久化采样和标注批次的训练工作流。
     */
    public TrainingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            LabelPropagationService propagationService,
                            RahaTrainService trainService,
                            TrainingInputMergeService inputMergeService) {
        this(datasetLoader, profileService, planService, executionService,
                featureService, clusteringService, propagationService,
                trainService, inputMergeService, null, null);
    }

    public TrainingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            LabelPropagationService propagationService,
                            RahaTrainService trainService,
                            TrainingInputMergeService inputMergeService,
                            ResultPersistenceVerifier resultPersistenceVerifier) {
        super(datasetLoader, profileService, planService, executionService, featureService);
        if (clusteringService == null || propagationService == null || trainService == null) {
            throw new IllegalArgumentException("训练工作流业务依赖不能为空");
        }
        this.clusteringService = clusteringService;
        this.propagationService = propagationService;
        this.trainService = trainService;
        this.inputMergeService = inputMergeService;
        this.resultPersistenceVerifier = resultPersistenceVerifier;
        this.snapshotCheckpointRepository = null;
    }

    public TrainingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            LabelPropagationService propagationService,
                            RahaTrainService trainService,
                            TrainingInputMergeService inputMergeService,
                            ResultPersistenceVerifier resultPersistenceVerifier,
                            SnapshotCheckpointRepository snapshotCheckpointRepository) {
        super(datasetLoader, profileService, planService, executionService, featureService);
        if (clusteringService == null || propagationService == null
                || trainService == null) {
            throw new IllegalArgumentException(
                    "训练工作流业务依赖不能为空");
        }
        this.clusteringService = clusteringService;
        this.propagationService = propagationService;
        this.trainService = trainService;
        this.inputMergeService = inputMergeService;
        this.resultPersistenceVerifier = resultPersistenceVerifier;
        this.snapshotCheckpointRepository = snapshotCheckpointRepository;
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
        if (request.isReuseSnapshotCheckpoint()) {
            if (snapshotCheckpointRepository == null) {
                throw new IllegalStateException("快照检查点复用未配置仓储");
            }
            List<StageHandler> handlers = new ArrayList<StageHandler>();
            handlers.add(new RestoreSnapshotCheckpointStageHandler(
                    snapshotCheckpointRepository));
            handlers.add(new DirectLabelStageHandler(request.getLabels()));
            handlers.add(new LabelPropagationStageHandler(propagationService,
                    request.getPropagationMethod(),
                    request.getPropagationConfig()));
            handlers.add(new ModelTrainingStageHandler(trainService,
                    request.getTrainingConfig(), request.getModelNamePrefix(),
                    request.getPropagationMethod(),
                    request.getPropagationConfig(),
                    request.getModelSetVersionOverride(),
                    request.getModelCompatibilityVersionOverride()));
            if (request.getEvaluator() != null) {
                handlers.add(new EvaluationStageHandler(request.getEvaluator()));
            }
            handlers.add(new ResultPersistenceStageHandler(
                    StageAttributeKeys.TRAIN_SERVICE_RESULT,
                    resultPersistenceVerifier));
            return Collections.unmodifiableList(handlers);
        }
        List<StageHandler> handlers = preparationStages(request);
        if (request.hasPersistedTrainingInput()) {
            if (inputMergeService == null) {
                throw new IllegalStateException("持久化批次训练未配置输入合并服务");
            }
            // 合并必须紧随数据加载，确保画像及全部训练派生产物只处理新快照。
            handlers.add(1, new TrainingInputMergeStageHandler(inputMergeService,
                    request.getTrainingBatchReferences(),
                    request.getRowIdentityConfig(),
                    request.isColumnBatchChild()));
        }
        handlers.add(new ClusterStageHandler(clusteringService));
        handlers.add(new DirectLabelStageHandler(request.getLabels()));
        handlers.add(new LabelPropagationStageHandler(propagationService,
                request.getPropagationMethod(), request.getPropagationConfig()));
        handlers.add(new ModelTrainingStageHandler(trainService,
                request.getTrainingConfig(), request.getModelNamePrefix(),
                request.getPropagationMethod(), request.getPropagationConfig(),
                request.getModelSetVersionOverride(),
                request.getModelCompatibilityVersionOverride()));
        if (request.getEvaluator() != null) {
            handlers.add(new EvaluationStageHandler(request.getEvaluator()));
        }
        handlers.add(new ResultPersistenceStageHandler(
                StageAttributeKeys.TRAIN_SERVICE_RESULT,
                resultPersistenceVerifier));
        return Collections.unmodifiableList(handlers);
    }
}
