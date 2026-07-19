package com.fiberhome.ml.raha.job.stage.model;

import com.fiberhome.ml.raha.job.stage.core.ServiceStageResultMapper;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationResult;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.service.prepare.RahaFeaturePreparationResult;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import com.fiberhome.ml.raha.service.train.RahaTrainRequest;
import com.fiberhome.ml.raha.service.train.RahaTrainService;
import com.fiberhome.ml.raha.service.train.TrainingMergeResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.List;

/**
 * 复用前序阶段产物执行标签模型训练并登记候选模型。
 */
public final class ModelTrainingStageHandler implements StageHandler {

    /** 列级模型训练服务。 */
    private final RahaTrainService trainService;
    /** 逻辑回归训练参数。 */
    private final LogisticRegressionTrainingConfig trainingConfig;
    /** 候选模型名称前缀。 */
    private final String modelNamePrefix;
    /** 标签传播方式。 */
    private final LabelPropagationMethod propagationMethod;
    /** 标签传播配置。 */
    private final LabelPropagationConfig propagationConfig;

    public ModelTrainingStageHandler(RahaTrainService trainService,
                                     LogisticRegressionTrainingConfig trainingConfig,
                                     String modelNamePrefix,
                                     LabelPropagationMethod propagationMethod,
                                     LabelPropagationConfig propagationConfig) {
        if (trainService == null || trainingConfig == null
                || modelNamePrefix == null || modelNamePrefix.trim().isEmpty()
                || propagationMethod == null || propagationConfig == null) {
            throw new IllegalArgumentException("模型训练服务、配置和名称不能为空");
        }
        this.trainService = trainService;
        this.trainingConfig = trainingConfig;
        this.modelNamePrefix = modelNamePrefix;
        this.propagationMethod = propagationMethod;
        this.propagationConfig = propagationConfig;
    }

    @Override
    public StageType getStageType() {
        return StageType.TRAIN;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object datasetValue = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        Object planValue = context.getAttributes().get(StageAttributeKeys.STRATEGY_PLANS);
        Object batchValue = context.getAttributes().get(StageAttributeKeys.STRATEGY_BATCH_RESULT);
        Object featureValue = context.getAttributes().get(
                StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        Object clusteringValue = context.getAttributes().get(
                StageAttributeKeys.CLUSTERING_BATCH_RESULT);
        Object labelValue = context.getAttributes().get(StageAttributeKeys.CELL_LABELS);
        Object propagationValue = context.getAttributes().get(
                StageAttributeKeys.LABEL_PROPAGATION_RESULT);
        Object planVersionValue = context.getAttributes().get(
                StageAttributeKeys.STRATEGY_PLAN_VERSION);
        Object mergeValue = context.getAttributes().get(
                StageAttributeKeys.TRAINING_MERGE_RESULT);
        if (!(datasetValue instanceof RahaDataset) || !(planValue instanceof List)
                || !(batchValue instanceof StrategyBatchResult)
                || !(featureValue instanceof FeatureAssemblyResult)
                || !(clusteringValue instanceof ClusteringBatchResult)
                || !(labelValue instanceof List)
                || !(propagationValue instanceof LabelPropagationResult)
                || !(planVersionValue instanceof String)) {
            return StageResult.failure("TRAINING_INPUT_REQUIRED",
                    "模型训练阶段缺少数据、特征、聚类或传播结果", false, 0L, 0L);
        }
        RahaDataset dataset = (RahaDataset) datasetValue;
        RahaFeaturePreparationResult prepared = new RahaFeaturePreparationResult(
                dataset.getDatasetId(), dataset.getSnapshotId(),
                (List<StrategyPlan>) planValue, (StrategyBatchResult) batchValue,
                (FeatureAssemblyResult) featureValue, (String) planVersionValue,
                0L, (ClusteringBatchResult) clusteringValue);
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        RahaServiceResult<RahaTrainOutput> result = trainService.train(
                new RahaTrainRequest(context.getJob().getJobId(),
                        context.getStage().getStageId(), dataset, context.getConfig(),
                        (List<CellLabel>) labelValue, propagationMethod,
                        propagationConfig, trainingConfig, modelNamePrefix,
                        version, prepared, (LabelPropagationResult) propagationValue,
                        mergeValue instanceof TrainingMergeResult
                                ? (TrainingMergeResult) mergeValue : null));
        context.getAttributes().put(StageAttributeKeys.TRAIN_SERVICE_RESULT, result);
        if (result.getPayload() != null) {
            context.getAttributes().put(StageAttributeKeys.TRAIN_OUTPUT, result.getPayload());
        }
        return ServiceStageResultMapper.map(result);
    }
}
