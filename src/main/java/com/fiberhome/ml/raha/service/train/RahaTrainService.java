package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationResult;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.model.ColumnModelStore;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.domain.ModelPersistenceContext;
import com.fiberhome.ml.raha.model.release.ColumnModelMetadataFactory;
import com.fiberhome.ml.raha.model.release.ModelReadableVersioner;
import com.fiberhome.ml.raha.model.release.ModelSourceKey;
import com.fiberhome.ml.raha.model.release.ModelReleaseManager;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainer;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingRequest;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingResult;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingStatus;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataBuilder;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataset;
import com.fiberhome.ml.raha.model.training.ColumnTrainingStatus;
import com.fiberhome.ml.raha.model.training.ColumnTrainingExample;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.model.training.ModelQualityGate;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.parallel.ParallelBatchResult;
import com.fiberhome.ml.raha.parallel.ParallelFailure;
import com.fiberhome.ml.raha.parallel.ParallelWorkItem;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbColumnProfileCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbStrategyArtifactCodec;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.service.common.RahaServiceSummary;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.service.prepare.RahaFeaturePreparationRequest;
import com.fiberhome.ml.raha.service.prepare.RahaFeaturePreparationResult;
import com.fiberhome.ml.raha.service.prepare.RahaFeaturePreparationService;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /** 可由 SAMPLE 和 TRAIN 共同复用的策略及特征准备服务。 */
    private final RahaFeaturePreparationService preparationService;
    /** 可选持久化 c1/o1 合并服务。 */
    private final TrainingInputMergeService inputMergeService;
    /** 可选训练派生产物物化和冻结样本恢复服务。 */
    private final TrainingArtifactMaterializationService artifactMaterializationService;

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
                metadataFactory, releaseManager, clock, new BoundedParallelExecutor(),
                null, null);
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
        this(planService, executionService, featureService, clusteringService,
                propagationService, dataBuilder, trainer, modelStore,
                metadataFactory, releaseManager, clock, parallelExecutor, null);
    }

    /**
     * 创建支持持久化输入合并、训练产物物化和冻结样本恢复的训练服务。
     */
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
                            BoundedParallelExecutor parallelExecutor,
                            TrainingInputMergeService inputMergeService,
                            TrainingArtifactMaterializationService artifactMaterializationService) {
        this(planService, executionService, featureService, clusteringService,
                propagationService, dataBuilder, trainer, modelStore,
                metadataFactory, releaseManager, clock, parallelExecutor,
                inputMergeService, artifactMaterializationService, true);
    }

    /**
     * 创建支持持久化 c1/o1 合并的训练编排服务。
     */
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
                            BoundedParallelExecutor parallelExecutor,
                            TrainingInputMergeService inputMergeService) {
        this(planService, executionService, featureService, clusteringService,
                propagationService, dataBuilder, trainer, modelStore,
                metadataFactory, releaseManager, clock, parallelExecutor,
                inputMergeService, null, true);
    }

    private RahaTrainService(StrategyPlanService planService,
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
                             BoundedParallelExecutor parallelExecutor,
                             TrainingInputMergeService inputMergeService,
                             TrainingArtifactMaterializationService artifactMaterializationService,
                             boolean ignored) {
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
        this.inputMergeService = inputMergeService;
        this.artifactMaterializationService = artifactMaterializationService;
        this.preparationService = new RahaFeaturePreparationService(
                planService, executionService, featureService, clock);
    }

    /**
     * 执行完整训练编排，成功模型保存参数文件并进入候选状态。
     *
     * @param request 训练服务输入
     * @return 包含状态、位置、摘要和候选模型的统一结果
     */
    public RahaServiceResult<RahaTrainOutput> train(RahaTrainRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("训练服务请求不能为空");
        }
        long startedAt = clock.millis();
        TrainingMergeResult mergeResult = request.getTrainingMergeResult();
        if (request.hasPersistedInput()) {
            if (inputMergeService == null) {
                throw new IllegalStateException("训练请求引用了持久化批次，但未配置合并服务");
            }
            TrainingMergeResult merged = inputMergeService.merge(
                    new TrainingMergeRequest(request.getJobId(),
                            request.getDataset(), request.getRowIdentityConfig(),
                            request.getSampleBatchId(), request.getSamplePartitionMonth(),
                            request.getAnnotationBatchId(),
                            request.getAnnotationPartitionMonth(),
                            request.getConfig().getResourceConfig()
                                    .getBroadcastThresholdBytes()));
            request = request.withMergedInput(merged);
            mergeResult = merged;
            LOGGER.info("训练服务已切换到 c1/o1 合并快照，jobId={}，"
                            + "trainingBatchId={}，trainingSnapshotId={}，mergedCount={}",
                    request.getJobId(), merged.getTrainingBatchId(),
                    merged.getTrainingSnapshotId(), merged.getMetrics().getMergedCount());
        }
        Dataset<Row> inputFrame = request.getDataset().getDataFrame();
        boolean requiresInputFrame = request.getPreparedFeatures() == null;
        if (requiresInputFrame && inputFrame == null) {
            throw new IllegalArgumentException(
                    "未复用特征时训练数据集必须绑定 Spark DataFrame");
        }
        boolean ownsInputCache = requiresInputFrame
                && inputFrame.storageLevel().equals(StorageLevel.NONE());
        LOGGER.info("开始 Raha 训练服务，jobId={}，datasetId={}，directLabelCount={}",
                request.getJobId(), request.getDataset().getDatasetId(),
                request.getDirectLabels().size());
        try {
            if (ownsInputCache) {
                StorageLevel storageLevel = StorageLevel.fromString(
                        request.getConfig().getResourceConfig().getCacheStorageLevel());
                LOGGER.info("开始缓存训练公共输入，jobId={}，storageLevel={}",
                        request.getJobId(), storageLevel.description());
                inputFrame.persist(storageLevel);
                inputFrame.count();
            }
            RahaFeaturePreparationResult preparation = request.getPreparedFeatures();
            if (preparation == null) {
                preparation = preparationService.prepare(
                        new RahaFeaturePreparationRequest(request.getJobId(),
                                request.getStageId() + "-prepare", request.getDataset(),
                                request.getConfig(), request.getArtifactVersion()));
            } else {
                LOGGER.info("训练服务复用已准备特征，jobId={}，planVersion={}，featureRowCount={}",
                        request.getJobId(), preparation.getStrategyPlanVersion(),
                        preparation.getFeatures().getRows().size());
            }
            List<StrategyPlan> plans = preparation.getStrategyPlans();
            StrategyBatchResult strategyBatch = preparation.getStrategyBatch();
            FeatureAssemblyResult features = preparation.getFeatures();
            String planVersion = preparation.getStrategyPlanVersion();
            if (mergeResult != null) {
                // 训练字典版本必须绑定合并批次，避免新训练快照误复用采样或旧训练字典。
                features = versionTrainingFeatures(features,
                        mergeResult.getTrainingBatchId(), planVersion,
                        request.getConfig().getExecutionConfigFingerprint());
            }
            ClusteringBatchResult clustering = preparation.getClustering();
            if (clustering == null) {
                clustering = clusteringService.clusterAndSaveParallel(
                        request.getJobId(), features,
                        request.getConfig().getClusteringConfig(),
                        request.getConfig().getRandomSeed(), request.getArtifactVersion(),
                        request.getConfig().getResourceConfig().getMaxParallelColumns(),
                        request.getConfig().getResourceConfig().getStageTimeoutMillis());
            } else {
                LOGGER.info("训练服务复用已准备聚类，jobId={}，assignmentCount={}",
                        request.getJobId(), clustering.getMetrics().getAssignmentCount());
            }
            LabelPropagationResult propagation = request.getPreparedPropagation();
            if (propagation == null) {
                propagation = propagationService.propagateAndSave(
                        request.getJobId(), assignments(clustering), request.getDirectLabels(),
                        request.getPropagationMethod(), request.getPropagationConfig(),
                        request.getArtifactVersion());
            } else {
                LOGGER.info("训练服务复用标签传播结果，jobId={}，directLabelCount={}，"
                                + "propagatedLabelCount={}",
                        request.getJobId(), propagation.getMetrics().getDirectLabelCount(),
                        propagation.getMetrics().getPropagatedLabelCount());
            }
            final LabelPropagationResult trainingPropagation = propagation;
            ModelSourceKey modelSource = ModelSourceKey.fromDatasetAndTable(
                    request.getDataset().getDatasetId(),
                    request.getDataset().getTableName());
            String modelSetVersion = ModelReadableVersioner.modelSetVersion(
                    modelSource.getSourceName(), clock.millis(), request.getJobId());
            LOGGER.info("生成可读模型集合版本，jobId={}，datasetId={}，"
                            + "sourceName={}，modelSetVersion={}",
                    request.getJobId(), request.getDataset().getDatasetId(),
                    modelSource.getSourceName(), modelSetVersion);
            Map<String, ColumnTrainingDataset> builtDatasetsForTraining = null;
            TrainingArtifactMaterializationResult materialization = null;
            if (mergeResult != null) {
                if (artifactMaterializationService == null) {
                    throw new IllegalStateException("持久化训练未配置派生产物物化服务");
                }
                Map<String, ColumnTrainingDataset> builtDatasets = buildTrainingDatasets(request, features,
                        trainingPropagation, planVersion);
                builtDatasetsForTraining = builtDatasets;
                materialization =
                        artifactMaterializationService.materialize(mergeResult, features,
                        clustering, trainingPropagation, modelSetVersion, planVersion,
                        builtDatasets, profileJsonByColumn(request.getDataset()),
                        strategyPlanJsonByColumn(plans, strategyBatch));
                LOGGER.debug("训练审计记录已提交，jobId={}，modelSetVersion={}",
                        request.getJobId(), modelSetVersion);
            }
            final Map<String, ColumnTrainingDataset> trainingDatasets =
                    builtDatasetsForTraining;
            final FeatureAssemblyResult trainingFeatures = features;
            Map<String, ColumnModelTrainingResult> trainingResults =
                    new LinkedHashMap<String, ColumnModelTrainingResult>();
            Map<String, RahaColumnModel> candidates =
                    new LinkedHashMap<String, RahaColumnModel>();
            long skippedCount = 0L;
            long failedCount = 0L;
            final RahaTrainRequest trainingRequest = request;
            List<ParallelWorkItem<String, ColumnTrainingOutcome>> trainingItems =
                    new ArrayList<ParallelWorkItem<String, ColumnTrainingOutcome>>();
            for (Map.Entry<String, FeatureDictionary> entry
                    : trainingFeatures.getDictionaries().entrySet()) {
                trainingItems.add(new ParallelWorkItem<String, ColumnTrainingOutcome>(
                        entry.getKey(), () -> trainColumn(trainingRequest, trainingFeatures,
                        trainingPropagation, planVersion, modelSetVersion,
                        entry.getKey(), entry.getValue(), trainingDatasets)));
            }
            ParallelBatchResult<String, ColumnTrainingOutcome> parallelTraining =
                    parallelExecutor.execute(trainingItems,
                            request.getConfig().getResourceConfig().getMaxParallelColumns(),
                            request.getConfig().getResourceConfig().getStageTimeoutMillis());
            for (String columnName : trainingFeatures.getDictionaries().keySet()) {
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
                        == ColumnModelTrainingStatus.FAILED) {
                    failedCount++;
                } else {
                    skippedCount++;
                }
            }
            RahaTrainOutput output = new RahaTrainOutput(plans, strategyBatch, trainingFeatures,
                    clustering, propagation, trainingResults, candidates, planVersion,
                    modelSetVersion, materialization);
            long completedAt = clock.millis();
            Map<String, String> details = details(plans, strategyBatch, trainingFeatures,
                    propagation, candidates, planVersion,
                    parallelTraining.getMaxObservedConcurrency());
            RahaServiceSummary summary = new RahaServiceSummary(startedAt, completedAt,
                    trainingFeatures.getDictionaries().size(), candidates.size(), skippedCount,
                    failedCount, details);
            JobStatus status;
            String errorCode = null;
            String errorMessage = null;
            if (candidates.isEmpty()) {
                status = JobStatus.FAILED;
                errorCode = "NO_CANDIDATE_MODEL";
                errorMessage = "训练完成但没有字段产出候选模型";
            } else if (failedCount > 0L || strategyBatch.getFailedCount() > 0L) {
                status = JobStatus.PARTIAL_SUCCESS;
                errorCode = "PARTIAL_TRAINING_FAILURE";
                errorMessage = "部分策略或字段训练失败";
            } else {
                status = JobStatus.SUCCEEDED;
            }
            LOGGER.info("Raha 训练服务完成，jobId={}，status={}，candidateCount={}，"
                            + "skippedCount={}，failedCount={}",
                    request.getJobId(), status, candidates.size(), skippedCount, failedCount);
            return new RahaServiceResult<RahaTrainOutput>(request.getJobId(),
                    JobType.TRAINING, status,
                    "repository://column-model/" + request.getDataset().getDatasetId(),
                    summary, output, errorCode, errorMessage);
        } catch (RuntimeException | LinkageError exception) {
            // 核心训练编排异常必须转换为统一失败结果，并保留任务上下文和堆栈。
            LOGGER.error("Raha 训练服务失败，jobId={}，datasetId={}",
                    request.getJobId(), request.getDataset().getDatasetId(), exception);
            RahaServiceSummary summary = new RahaServiceSummary(startedAt, clock.millis(),
                    1L, 0L, 0L, 1L, Collections.<String, String>emptyMap());
            return new RahaServiceResult<RahaTrainOutput>(request.getJobId(),
                    JobType.TRAINING, JobStatus.FAILED, null, summary, null,
                    "TRAIN_SERVICE_FAILED", exception.getClass().getSimpleName());
        } finally {
            if (ownsInputCache) {
                inputFrame.unpersist(false);
                LOGGER.info("训练公共输入缓存已释放，jobId={}", request.getJobId());
            }
        }
    }

    private static List<ClusterAssignment> assignments(ClusteringBatchResult clustering) {
        List<ClusterAssignment> assignments = new ArrayList<ClusterAssignment>();
        for (ColumnClusteringResult result : clustering.getResults().values()) {
            assignments.addAll(result.getAssignments());
        }
        return assignments;
    }

    private ColumnTrainingOutcome trainColumn(RahaTrainRequest request,
                                              FeatureAssemblyResult features,
                                              LabelPropagationResult propagation,
                                              String planVersion,
                                              String modelSetVersion,
                                              String columnName,
                                              FeatureDictionary dictionary,
                                              Map<String, ColumnTrainingDataset> frozenDatasets) {
        ColumnTrainingDataset trainingDataset = frozenDatasets == null
                ? buildTrainingDataset(request, features, propagation, columnName, dictionary)
                : frozenDatasets.get(columnName);
        if (trainingDataset == null) {
            throw new IllegalStateException("冻结训练数据缺少字段：" + columnName);
        }
        if (frozenDatasets != null) {
            LOGGER.info("训练字段读取冻结样本，jobId={}，columnName={}，sampleCount={}，"
                            + "positiveCount={}，negativeCount={}", request.getJobId(),
                    columnName, trainingDataset.getExamples().size(),
                    trainingDataset.getPositiveCount(), trainingDataset.getNegativeCount());
        }
        ModelSourceKey modelSource = ModelSourceKey.fromDatasetAndTable(
                request.getDataset().getDatasetId(),
                request.getDataset().getTableName());
        ColumnModelTrainingRequest trainingRequest = new ColumnModelTrainingRequest(
                ModelReadableVersioner.modelName(request.getModelNamePrefix(),
                        modelSource.getSourceName(), columnName),
                request.getDataset().getDatasetId(),
                request.getDataset().getSchemaHash(), planVersion,
                modelSetVersion, modelSource.getSourceName(), trainingDataset,
                request.getConfig().getModelConfig(), request.getTrainingConfig());
        ColumnModelTrainingResult trainingResult = trainer.train(trainingRequest);
        trainingResult = ModelQualityGate.evaluate(trainingResult,
                trainingDataset, request.getConfig().getModelConfig());
        if (trainingResult.getStatus() != ColumnModelTrainingStatus.TRAINED) {
            return new ColumnTrainingOutcome(trainingResult, null);
        }
        ModelPersistenceContext persistenceContext = new ModelPersistenceContext(
                modelSetVersion, request.getDataset().getDatasetId(),
                request.getDataset().getSchemaHash(),
                request.getTrainingMergeResult() == null ? request.getJobId()
                        : request.getTrainingMergeResult().getTrainingBatchId(),
                ModelStatus.CANDIDATE, planVersion,
                "direct-input-v1", trainingResult.getMetrics(), clock.millis(), null,
                request.getConfig().getRowIdentityConfig());
        String modelPath = modelStore.save(
                trainingResult.getArtifact(), persistenceContext);
        RahaColumnModel draft = metadataFactory.create(
                trainingRequest, trainingResult, modelPath, modelSetVersion,
                request.getConfig().getRowIdentityConfig());
        RahaColumnModel candidate = releaseManager.markCandidate(
                draft, request.getArtifactVersion());
        // 三函数训练入口要求训练成功后自动发布，检测函数只加载已发布模型集合。
        RahaColumnModel published = releaseManager.publish(
                candidate.getDatasetId(), candidate.getColumnName(),
                candidate.getModelVersion(), request.getArtifactVersion());
        return new ColumnTrainingOutcome(trainingResult, published);
    }

    private ColumnTrainingDataset buildTrainingDataset(RahaTrainRequest request,
                                                       FeatureAssemblyResult features,
                                                       LabelPropagationResult propagation,
                                                       String columnName,
                                                       FeatureDictionary dictionary) {
        ColumnTrainingDataset trainingDataset = dataBuilder.build(
                columnName, dictionary, features.getRowsByColumn(columnName),
                propagation.getLabels(),
                request.getTrainingConfig().isClassBalanceEnabled());
        if (shouldUseDirectLabels(trainingDataset)) {
            ColumnTrainingDataset directDataset = dataBuilder.build(
                    columnName, dictionary, features.getRowsByColumn(columnName),
                    request.getDirectLabels(),
                    request.getTrainingConfig().isClassBalanceEnabled());
            // 传播结果出现单一类别或极端失衡时，保留主动采样的直接标签以避免模型退化。
            if (directDataset.getStatus()
                    == com.fiberhome.ml.raha.model.training.ColumnTrainingStatus.TRAINABLE) {
                LOGGER.warn("字段传播训练集不可用或类别极端失衡，回退直接标签，"
                                + "jobId={}，columnName={}，propagatedPositiveCount={}，"
                                + "propagatedNegativeCount={}，directPositiveCount={}，"
                                + "directNegativeCount={}",
                        request.getJobId(), columnName,
                        trainingDataset.getPositiveCount(),
                        trainingDataset.getNegativeCount(),
                        directDataset.getPositiveCount(),
                        directDataset.getNegativeCount());
                trainingDataset = directDataset;
            }
        }
        return trainingDataset;
    }

    private static boolean shouldUseDirectLabels(
            ColumnTrainingDataset trainingDataset) {
        if (trainingDataset.getStatus()
                != com.fiberhome.ml.raha.model.training.ColumnTrainingStatus.TRAINABLE) {
            return true;
        }
        int total = trainingDataset.getPositiveCount()
                + trainingDataset.getNegativeCount();
        if (total == 0) {
            return true;
        }
        double positiveRatio = (double) trainingDataset.getPositiveCount() / total;
        return positiveRatio < 0.05d || positiveRatio > 0.95d;
    }

    private Map<String, ColumnTrainingDataset> buildTrainingDatasets(
            RahaTrainRequest request,
            FeatureAssemblyResult features,
            LabelPropagationResult propagation,
            String planVersion) {
        Map<String, ColumnTrainingDataset> result =
                new LinkedHashMap<String, ColumnTrainingDataset>();
        for (Map.Entry<String, FeatureDictionary> entry
                : features.getDictionaries().entrySet()) {
            result.put(entry.getKey(), buildTrainingDataset(request, features,
                    propagation, entry.getKey(), entry.getValue()));
        }
        LOGGER.info("最终训练样本构建完成，jobId={}，columnCount={}，sampleCount={}",
                request.getJobId(), result.size(), sampleCount(result));
        return result;
    }

    private static long sampleCount(Map<String, ColumnTrainingDataset> datasets) {
        long count = 0L;
        for (ColumnTrainingDataset dataset : datasets.values()) {
            count += dataset.getExamples().size();
        }
        return count;
    }

    private static FeatureAssemblyResult versionTrainingFeatures(
            FeatureAssemblyResult source,
            String trainingBatchId,
            String planVersion,
            String executionConfigFingerprint) {
        Map<String, FeatureDictionary> dictionaries =
                new LinkedHashMap<String, FeatureDictionary>();
        Map<String, String> versions = new LinkedHashMap<String, String>();
        for (Map.Entry<String, FeatureDictionary> entry
                : source.getDictionaries().entrySet()) {
            String version = HashUtils.md5Hex("training-feature-v1|"
                    + trainingBatchId + "|" + planVersion + "|"
                    + executionConfigFingerprint + "|" + entry.getKey() + "|"
                    + entry.getValue().getVersion());
            versions.put(entry.getKey(), version);
            FeatureDictionary dictionary = entry.getValue();
            dictionaries.put(entry.getKey(), new FeatureDictionary(version,
                    dictionary.getColumnName(), dictionary.getDefinitions(),
                    dictionary.getCreatedAt()));
        }
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        for (SparseFeatureRow row : source.getRows()) {
            String version = versions.get(row.getColumnName());
            CellCoordinate coordinate = row.getCoordinate();
            rows.add(new SparseFeatureRow(row.getCellId(), row.getColumnName(),
                    coordinate, row.getValueHash(), row.getMaskedValue(), version,
                    row.getValues(), row.getSummary()));
        }
        return new FeatureAssemblyResult(dictionaries, rows,
                new FeatureAssemblyMetrics(source.getMetrics().getCellCount(),
                        source.getMetrics().getCandidateFeatureCount(),
                        source.getMetrics().getRetainedFeatureCount(),
                        source.getMetrics().getRemovedConstantFeatureCount()));
    }

    private static Map<String, String> profileJsonByColumn(RahaDataset dataset) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<String, ColumnProfile> entry : dataset.getProfiles().entrySet()) {
            result.put(entry.getKey(), FmdbColumnProfileCodec.write(entry.getValue()));
        }
        return result;
    }

    private static Map<String, String> strategyPlanJsonByColumn(
            List<StrategyPlan> plans,
            StrategyBatchResult strategyBatch) {
        Map<String, List<StrategyPlan>> grouped =
                new LinkedHashMap<String, List<StrategyPlan>>();
        Map<String, List<StrategyRunSummary>> summaries =
                new LinkedHashMap<String, List<StrategyRunSummary>>();
        Map<String, StrategyRunSummary> summaryByStrategy =
                new LinkedHashMap<String, StrategyRunSummary>();
        for (StrategyExecutionResult execution : strategyBatch.getExecutions()) {
            summaryByStrategy.put(execution.getSummary().getStrategyId(),
                    execution.getSummary());
        }
        for (StrategyPlan plan : plans) {
            for (String column : plan.getTargetColumns()) {
                if (!grouped.containsKey(column)) {
                    grouped.put(column, new ArrayList<StrategyPlan>());
                    summaries.put(column, new ArrayList<StrategyRunSummary>());
                }
                grouped.get(column).add(plan);
                if (summaryByStrategy.containsKey(plan.getStrategyId())) {
                    summaries.get(column).add(summaryByStrategy.get(plan.getStrategyId()));
                }
            }
        }
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<String, List<StrategyPlan>> entry : grouped.entrySet()) {
            result.put(entry.getKey(), FmdbStrategyArtifactCodec.write(
                    entry.getValue(), summaries.get(entry.getKey())));
        }
        return result;
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
        details.put("strategyFamilyPlanCounts", strategyFamilyPlanCounts(plans));
        details.put("strategyHitCount", String.valueOf(strategyBatch.getHitCount()));
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

    private static String strategyFamilyPlanCounts(List<StrategyPlan> plans) {
        Map<String, Integer> counts = new LinkedHashMap<String, Integer>();
        for (StrategyPlan plan : plans) {
            String family = plan.getStrategyFamily().name();
            counts.put(family, counts.containsKey(family)
                    ? counts.get(family) + 1 : 1);
        }
        return counts.toString();
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
