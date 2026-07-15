package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.cluster.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.label.LabelPropagationResult;
import com.fiberhome.ml.raha.label.LabelPropagationService;
import com.fiberhome.ml.raha.model.ColumnModelMetadataFactory;
import com.fiberhome.ml.raha.model.ColumnModelStore;
import com.fiberhome.ml.raha.model.ColumnModelTrainer;
import com.fiberhome.ml.raha.model.ColumnModelTrainingRequest;
import com.fiberhome.ml.raha.model.ColumnModelTrainingResult;
import com.fiberhome.ml.raha.model.ColumnModelTrainingStatus;
import com.fiberhome.ml.raha.model.ColumnTrainingDataBuilder;
import com.fiberhome.ml.raha.model.ColumnTrainingDataset;
import com.fiberhome.ml.raha.model.ModelReleaseManager;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.parallel.ParallelBatchResult;
import com.fiberhome.ml.raha.parallel.ParallelFailure;
import com.fiberhome.ml.raha.parallel.ParallelWorkItem;
import com.fiberhome.ml.raha.strategy.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyPlanService;
import com.fiberhome.ml.raha.util.HashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 编排策略、特征、聚类、标签传播和列级模型训练并产出候选模型。
 */
public final class RahaTrainService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaTrainService.class);
    /** 策略计划服务。 */
    private final StrategyPlanService planService;
    /** 策略执行服务。 */
    private final StrategyExecutionService executionService;
    /** 特征组装服务。 */
    private final FeatureService featureService;
    /** 列内聚类服务。 */
    private final ColumnClusteringService clusteringService;
    /** 标签传播服务。 */
    private final LabelPropagationService propagationService;
    /** 列级训练数据构建器。 */
    private final ColumnTrainingDataBuilder dataBuilder;
    /** 自适应列级模型训练器。 */
    private final ColumnModelTrainer trainer;
    /** 模型参数文件存储。 */
    private final ColumnModelStore modelStore;
    /** 模型元数据工厂。 */
    private final ColumnModelMetadataFactory metadataFactory;
    /** 模型候选和发布管理器。 */
    private final ModelReleaseManager releaseManager;
    /** 提供可测试任务时间的时钟。 */
    private final Clock clock;
    /** 受限列训练并行执行器。 */
    private final BoundedParallelExecutor parallelExecutor;

    public RahaTrainService(StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            LabelPropagationService propagationService,
                            ColumnTrainingDataBuilder dataBuilder,
                            ColumnModelTrainer trainer,
                            ColumnModelStore modelStore,
                            ColumnModelMetadataFactory metadataFactory,
                            ModelReleaseManager releaseManager,
                            Clock clock) {
        this(planService, executionService, featureService, clusteringService,
                propagationService, dataBuilder, trainer, modelStore,
                metadataFactory, releaseManager, clock, new BoundedParallelExecutor());
    }

    public RahaTrainService(StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            LabelPropagationService propagationService,
                            ColumnTrainingDataBuilder dataBuilder,
                            ColumnModelTrainer trainer,
                            ColumnModelStore modelStore,
                            ColumnModelMetadataFactory metadataFactory,
                            ModelReleaseManager releaseManager,
                            Clock clock,
                            BoundedParallelExecutor parallelExecutor) {
        if (planService == null || executionService == null || featureService == null
                || clusteringService == null || propagationService == null
                || dataBuilder == null || trainer == null || modelStore == null
                || metadataFactory == null || releaseManager == null || clock == null
                || parallelExecutor == null) {
            throw new IllegalArgumentException("训练服务依赖不能为空");
        }
        this.planService = planService;
        this.executionService = executionService;
        this.featureService = featureService;
        this.clusteringService = clusteringService;
        this.propagationService = propagationService;
        this.dataBuilder = dataBuilder;
        this.trainer = trainer;
        this.modelStore = modelStore;
        this.metadataFactory = metadataFactory;
        this.releaseManager = releaseManager;
        this.clock = clock;
        this.parallelExecutor = parallelExecutor;
    }

    /**
     * 执行完整训练编排，成功模型保存参数文件并进入候选状态。
     *
     * @param request 训练服务输入
     * @return 包含状态、位置、摘要和候选模型的统一结果
     */
    public RahaTaskResult<RahaTrainOutput> train(RahaTrainRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("训练服务请求不能为空");
        }
        long startedAt = clock.millis();
        LOGGER.info("开始 Raha 训练服务，jobId={}，datasetId={}，directLabelCount={}",
                request.getJobId(), request.getDataset().getDatasetId(),
                request.getDirectLabels().size());
        try {
            List<StrategyPlan> plans = planService.generateAndSave(
                    request.getDataset(), request.getConfig().getStrategyConfig(),
                    request.getArtifactVersion());
            StrategyBatchResult strategyBatch = executionService.execute(
                    request.getJobId(), request.getStageId() + "-strategy",
                    request.getDataset(), plans,
                    request.getConfig().getStrategyConfig().getStrategyTimeoutMillis(),
                    request.getArtifactVersion(),
                    request.getConfig().getResourceConfig().getMaxParallelStrategies(),
                    request.getConfig().getResourceConfig().getStageTimeoutMillis());
            FeatureAssemblyResult features = featureService.assembleAndSaveParallel(
                    request.getJobId(), request.getDataset(), plans,
                    strategyBatch.getHits(), request.getConfig().getFeatureConfig(),
                    request.getArtifactVersion(),
                    request.getConfig().getResourceConfig().getMaxParallelColumns(),
                    request.getConfig().getResourceConfig().getStageTimeoutMillis());
            ClusteringBatchResult clustering = clusteringService.clusterAndSaveParallel(
                    request.getJobId(), features,
                    request.getConfig().getClusteringConfig(),
                    request.getConfig().getRandomSeed(), request.getArtifactVersion(),
                    request.getConfig().getResourceConfig().getMaxParallelColumns(),
                    request.getConfig().getResourceConfig().getStageTimeoutMillis());
            LabelPropagationResult propagation = propagationService.propagateAndSave(
                    request.getJobId(), assignments(clustering), request.getDirectLabels(),
                    request.getPropagationMethod(), request.getPropagationConfig(),
                    request.getArtifactVersion());
            String planVersion = strategyPlanVersion(plans);
            Map<String, ColumnModelTrainingResult> trainingResults =
                    new LinkedHashMap<String, ColumnModelTrainingResult>();
            Map<String, RahaColumnModel> candidates =
                    new LinkedHashMap<String, RahaColumnModel>();
            long skippedCount = 0L;
            long failedCount = 0L;
            List<ParallelWorkItem<String, ColumnTrainingOutcome>> trainingItems =
                    new ArrayList<ParallelWorkItem<String, ColumnTrainingOutcome>>();
            for (Map.Entry<String, FeatureDictionary> entry
                    : features.getDictionaries().entrySet()) {
                trainingItems.add(new ParallelWorkItem<String, ColumnTrainingOutcome>(
                        entry.getKey(), () -> trainColumn(request, features,
                        propagation, planVersion, entry.getKey(), entry.getValue())));
            }
            ParallelBatchResult<String, ColumnTrainingOutcome> parallelTraining =
                    parallelExecutor.execute(trainingItems,
                            request.getConfig().getResourceConfig().getMaxParallelColumns(),
                            request.getConfig().getResourceConfig().getStageTimeoutMillis());
            for (String columnName : features.getDictionaries().keySet()) {
                ColumnTrainingOutcome outcome = parallelTraining.getSuccesses().get(columnName);
                if (outcome == null) {
                    ParallelFailure failure = parallelTraining.getFailures().get(columnName);
                    trainingResults.put(columnName, new ColumnModelTrainingResult(
                            ColumnModelTrainingStatus.FAILED, null, false,
                            failure == null ? "列训练调度失败" : failure.getErrorType(), null));
                    failedCount++;
                    continue;
                }
                trainingResults.put(columnName, outcome.trainingResult);
                if (outcome.candidate != null) {
                    candidates.put(columnName, outcome.candidate);
                } else if (outcome.trainingResult.getStatus()
                        == ColumnModelTrainingStatus.FAILED
                        || outcome.trainingResult.getStatus()
                        == ColumnModelTrainingStatus.MLLIB_UNAVAILABLE) {
                    failedCount++;
                } else {
                    skippedCount++;
                }
            }
            RahaTrainOutput output = new RahaTrainOutput(plans, strategyBatch, features,
                    clustering, propagation, trainingResults, candidates, planVersion);
            long completedAt = clock.millis();
            Map<String, String> details = details(plans, strategyBatch, features,
                    propagation, candidates, planVersion,
                    parallelTraining.getMaxObservedConcurrency());
            RahaTaskSummary summary = new RahaTaskSummary(startedAt, completedAt,
                    features.getDictionaries().size(), candidates.size(), skippedCount,
                    failedCount, details);
            RahaTaskStatus status;
            String errorCode = null;
            String errorMessage = null;
            if (candidates.isEmpty()) {
                status = RahaTaskStatus.FAILED;
                errorCode = "NO_CANDIDATE_MODEL";
                errorMessage = "训练完成但没有字段产出候选模型";
            } else if (failedCount > 0L || strategyBatch.getFailedCount() > 0L) {
                status = RahaTaskStatus.PARTIAL_SUCCESS;
                errorCode = "PARTIAL_TRAINING_FAILURE";
                errorMessage = "部分策略或字段训练失败";
            } else {
                status = RahaTaskStatus.SUCCEEDED;
            }
            LOGGER.info("Raha 训练服务完成，jobId={}，status={}，candidateCount={}，"
                            + "skippedCount={}，failedCount={}",
                    request.getJobId(), status, candidates.size(), skippedCount, failedCount);
            return new RahaTaskResult<RahaTrainOutput>(request.getJobId(),
                    RahaTaskType.TRAIN, status,
                    "repository://column-model/" + request.getDataset().getDatasetId(),
                    summary, output, errorCode, errorMessage);
        } catch (RuntimeException | LinkageError exception) {
            // 核心训练编排异常必须转换为统一失败结果，并保留任务上下文和堆栈。
            LOGGER.error("Raha 训练服务失败，jobId={}，datasetId={}",
                    request.getJobId(), request.getDataset().getDatasetId(), exception);
            RahaTaskSummary summary = new RahaTaskSummary(startedAt, clock.millis(),
                    1L, 0L, 0L, 1L, Collections.<String, String>emptyMap());
            return new RahaTaskResult<RahaTrainOutput>(request.getJobId(),
                    RahaTaskType.TRAIN, RahaTaskStatus.FAILED, null, summary, null,
                    "TRAIN_SERVICE_FAILED", exception.getClass().getSimpleName());
        }
    }

    private static List<ClusterAssignment> assignments(ClusteringBatchResult clustering) {
        List<ClusterAssignment> assignments = new ArrayList<ClusterAssignment>();
        for (ColumnClusteringResult result : clustering.getResults().values()) {
            assignments.addAll(result.getAssignments());
        }
        return assignments;
    }

    private static String strategyPlanVersion(List<StrategyPlan> plans) {
        List<String> signatures = new ArrayList<String>(plans.size());
        for (StrategyPlan plan : plans) {
            signatures.add(plan.getStrategyId() + ":" + plan.getConfigurationHash());
        }
        Collections.sort(signatures);
        return HashUtils.sha256Hex(String.join("|", signatures));
    }

    private ColumnTrainingOutcome trainColumn(RahaTrainRequest request,
                                              FeatureAssemblyResult features,
                                              LabelPropagationResult propagation,
                                              String planVersion,
                                              String columnName,
                                              FeatureDictionary dictionary) {
        ColumnTrainingDataset trainingDataset = dataBuilder.build(
                columnName, dictionary, features.getRowsByColumn(columnName),
                propagation.getLabels(),
                request.getTrainingConfig().isClassBalanceEnabled());
        ColumnModelTrainingRequest trainingRequest = new ColumnModelTrainingRequest(
                request.getModelNamePrefix() + "-" + columnName,
                request.getDataset().getDatasetId(),
                request.getDataset().getSchemaHash(), planVersion, trainingDataset,
                request.getConfig().getModelConfig(), request.getTrainingConfig());
        ColumnModelTrainingResult trainingResult = trainer.train(trainingRequest);
        if (trainingResult.getStatus() != ColumnModelTrainingStatus.TRAINED) {
            return new ColumnTrainingOutcome(trainingResult, null);
        }
        String modelPath = modelStore.save(trainingResult.getArtifact());
        RahaColumnModel draft = metadataFactory.create(
                trainingRequest, trainingResult, modelPath);
        RahaColumnModel candidate = releaseManager.markCandidate(
                draft, request.getArtifactVersion());
        return new ColumnTrainingOutcome(trainingResult, candidate);
    }

    private static Map<String, String> details(
            List<StrategyPlan> plans,
            StrategyBatchResult strategyBatch,
            FeatureAssemblyResult features,
            LabelPropagationResult propagation,
            Map<String, RahaColumnModel> candidates,
            String planVersion,
            int maxObservedColumnConcurrency) {
        Map<String, String> details = new LinkedHashMap<String, String>();
        details.put("strategyPlanCount", String.valueOf(plans.size()));
        details.put("strategyFailureCount", String.valueOf(strategyBatch.getFailedCount()));
        details.put("featureRowCount", String.valueOf(features.getRows().size()));
        details.put("propagatedLabelCount", String.valueOf(
                propagation.getMetrics().getPropagatedLabelCount()));
        details.put("candidateModelCount", String.valueOf(candidates.size()));
        details.put("strategyPlanVersion", planVersion);
        details.put("maxObservedColumnConcurrency",
                String.valueOf(maxObservedColumnConcurrency));
        return details;
    }

    private static final class ColumnTrainingOutcome {
        /** 当前字段训练结果。 */
        private final ColumnModelTrainingResult trainingResult;
        /** 当前字段候选模型，不可训练或失败时为空。 */
        private final RahaColumnModel candidate;

        private ColumnTrainingOutcome(ColumnModelTrainingResult trainingResult,
                                      RahaColumnModel candidate) {
            this.trainingResult = trainingResult;
            this.candidate = candidate;
        }
    }
}
