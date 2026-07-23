package com.fiberhome.ml.raha.job.stage.checkpoint;

import com.fiberhome.ml.raha.checkpoint.SnapshotCheckpointWriteSession;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringMetrics;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageAccumulator;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按逻辑列批生成特征和聚类、保存检查点并累积紧凑采样覆盖状态。
 */
public final class BatchedSnapshotCheckpointStageHandler
        implements StageHandler {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BatchedSnapshotCheckpointStageHandler.class);
    /** 单元格特征服务。 */
    private final FeatureService featureService;
    /** 批内策略执行服务。 */
    private final StrategyExecutionService executionService;
    /** 列内聚类服务。 */
    private final ColumnClusteringService clusteringService;
    /** 紧凑覆盖采样服务。 */
    private final RahaSampleService sampleService;
    /** 快照检查点仓储。 */
    private final SnapshotCheckpointRepository repository;
    /** 当前采样轮次。 */
    private final int samplingRound;
    /** 调用方提供的初始标签。 */
    private final List<CellLabel> initialLabels;

    public BatchedSnapshotCheckpointStageHandler(
            FeatureService featureService,
            StrategyExecutionService executionService,
            ColumnClusteringService clusteringService,
            RahaSampleService sampleService,
            SnapshotCheckpointRepository repository,
            int samplingRound,
            List<CellLabel> initialLabels) {
        if (featureService == null || executionService == null
                || clusteringService == null
                || sampleService == null || repository == null
                || samplingRound <= 0 || initialLabels == null) {
            throw new IllegalArgumentException("分批检查点阶段依赖和轮次必须有效");
        }
        this.featureService = featureService;
        this.executionService = executionService;
        this.clusteringService = clusteringService;
        this.sampleService = sampleService;
        this.repository = repository;
        this.samplingRound = samplingRound;
        this.initialLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(initialLabels));
    }

    @Override
    public StageType getStageType() {
        return StageType.SNAPSHOT_CHECKPOINT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object datasetValue = context.getAttributes().get(
                StageAttributeKeys.RAHA_DATASET);
        Object snapshotValue = context.getAttributes().get(
                StageAttributeKeys.DATASET_SNAPSHOT);
        Object planValue = context.getAttributes().get(
                StageAttributeKeys.STRATEGY_PLANS);
        Object planVersionValue = context.getAttributes().get(
                StageAttributeKeys.STRATEGY_PLAN_VERSION);
        if (!(datasetValue instanceof RahaDataset)
                || !(snapshotValue instanceof DatasetSnapshot)
                || !(planValue instanceof List)
                || !(planVersionValue instanceof String)) {
            return StageResult.failure("BATCHED_CHECKPOINT_INPUT_REQUIRED",
                    "分批检查点缺少数据或策略计划", false, 0L, 0L);
        }
        RahaDataset dataset = (RahaDataset) datasetValue;
        DatasetSnapshot snapshot = (DatasetSnapshot) snapshotValue;
        List<StrategyPlan> plans = (List<StrategyPlan>) planValue;
        List<CellLabel> labels = labels(context);
        SnapshotCheckpointWriteSession session = repository.begin(
                context.getJob().getJobId(), dataset, snapshot, plans,
                (String) planVersionValue,
                context.getConfig().getExecutionConfigFingerprint(),
                createdAt(context));
        try {
        List<ColumnMetadata> detectableColumns = detectableColumns(dataset);
        ClusterCoverageAccumulator accumulator =
                new ClusterCoverageAccumulator(labels,
                        context.getConfig().getSamplingConfig()
                                .getCoverageScoreExponentCap());
        Map<String, ColumnClusteringResult> summaries =
                new LinkedHashMap<String, ColumnClusteringResult>();
        long assignmentCount = 0L;
        long clusteredColumnCount = 0L;
        long exceptionalColumnCount = 0L;
        List<StrategyExecutionResult> strategyExecutions =
                new ArrayList<StrategyExecutionResult>();
        int columnBatchSize = repository.getColumnBatchSize();
        int batchIndex = 0;
        LOGGER.info("开始按列批生成并保存采样前置产物，jobId={}，columnCount={}，"
                        + "columnBatchSize={}", context.getJob().getJobId(),
                detectableColumns.size(), columnBatchSize);
        for (int offset = 0; offset < detectableColumns.size();
                offset += columnBatchSize) {
            batchIndex++;
            List<ColumnMetadata> batchColumns = new ArrayList<ColumnMetadata>(
                    detectableColumns.subList(offset, Math.min(
                            detectableColumns.size(), offset + columnBatchSize)));
            Set<String> columnNames = columnNames(batchColumns);
            RahaDataset batchDataset = isolate(dataset, batchColumns);
            ArtifactVersion version = new ArtifactVersion(
                    context.getJob().getConfigVersion(), dataset.getSnapshotId(),
                    context.getStage().getStageId(),
                    context.getStage().getAttemptId());
            List<StrategyPlan> batchPlans = plansForColumns(plans, columnNames);
            try {
                StrategyBatchResult strategyBatch = executeStrategies(context,
                        batchDataset, batchPlans, version);
                FeatureAssemblyResult features = assemble(context, batchDataset,
                        batchPlans, strategyBatch.getHits(), version);
                ClusteringBatchResult clustering = cluster(context, features,
                        version);
                repository.saveColumnBatch(session, batchIndex,
                        new ArrayList<String>(columnNames), features, clustering);
                accumulator.addBatch(clustering);
                strategyExecutions.addAll(
                        strategyBatch.withoutHits().getExecutions());
                assignmentCount += clustering.getMetrics().getAssignmentCount();
                clusteredColumnCount += clustering.getMetrics()
                        .getClusteredColumnCount();
                exceptionalColumnCount += clustering.getMetrics()
                        .getExceptionalColumnCount();
                for (ColumnClusteringResult result
                        : clustering.getResults().values()) {
                    summaries.put(result.getColumnName(), summaryOnly(result));
                }
                LOGGER.info("采样前置列批已保存，jobId={}，batchIndex={}，"
                                + "columns={}，featureRowCount={}，assignmentCount={}",
                        context.getJob().getJobId(), batchIndex, columnNames,
                        features.getRows().size(),
                        clustering.getMetrics().getAssignmentCount());
            } finally {
                // 无论列批成功还是失败，都清理三个任务级明细缓存，避免异常重试持续占用堆内存。
                executionService.releaseCachedHits(
                        context.getJob().getJobId(), batchPlans);
                featureService.releaseCachedBatch(
                        context.getJob().getJobId(), columnNames);
                clusteringService.releaseCachedBatch(
                        context.getJob().getJobId(), columnNames);
            }
        }
        StrategyBatchResult strategySummary =
                new StrategyBatchResult(strategyExecutions);
        double failedRatio = plans.isEmpty() ? 0.0d
                : ((double) strategySummary.getFailedCount()) / plans.size();
        if (strategySummary.getFailedCount() > 0L
                && (context.getConfig().getFailureToleranceConfig().isFailFast()
                || failedRatio > context.getConfig().getFailureToleranceConfig()
                .getMaxFailedStrategyRatio())) {
            repository.abort(session);
            return StageResult.failure("STRATEGY_PARTIAL_FAILURE",
                    "批内策略失败比例超过容忍阈值", true,
                    strategySummary.getFailedCount(), plans.size());
        }
        if (strategySummary.getFailedCount() > 0L) {
            LOGGER.warn("批内策略存在可容忍失败，继续提交检查点，jobId={}，"
                            + "failedCount={}，planCount={}，failedRatio={}",
                    context.getJob().getJobId(),
                    strategySummary.getFailedCount(), plans.size(), failedRatio);
        }
        ClusteringBatchResult summary = new ClusteringBatchResult(summaries,
                new ClusteringMetrics(summaries.size(), clusteredColumnCount,
                        assignmentCount, exceptionalColumnCount));
        ArtifactVersion samplingVersion = new ArtifactVersion(
                context.getJob().getConfigVersion(), dataset.getSnapshotId(),
                context.getStage().getStageId(),
                context.getStage().getAttemptId());
        RahaServiceResult<RahaSampleOutput> result =
                sampleService.samplePreparedScores(
                        context.getJob().getJobId(), samplingRound, summary,
                        accumulator,
                        context.getConfig().getSamplingConfig(),
                        context.getConfig().getRandomSeed(), samplingVersion);
        if (result.getStatus() != JobStatus.SUCCEEDED) {
            repository.abort(session);
            return StageResult.failure(result.getErrorCode(),
                    result.getErrorMessage(), false, 0L, 0L);
        }
        // 只有采样任务生成成功后才提交最终清单，避免训练恢复未完成业务快照。
        repository.complete(session, strategySummary);
        context.getAttributes().put(StageAttributeKeys.CLUSTERING_BATCH_RESULT,
                summary);
        context.getAttributes().put(StageAttributeKeys.STRATEGY_BATCH_RESULT,
                strategySummary);
        context.getAttributes().put(StageAttributeKeys.STRATEGY_HITS,
                Collections.emptyList());
        context.getAttributes().put(StageAttributeKeys.SAMPLE_SERVICE_RESULT,
                result);
        LOGGER.info("采样前置产物分批处理完成，jobId={}，columnBatchCount={}，"
                        + "candidateRowCount={}，assignmentCount={}",
                context.getJob().getJobId(), batchIndex,
                accumulator.getCandidateRowCount(), assignmentCount);
        return StageResult.success();
        } catch (RuntimeException exception) {
            try {
                repository.abort(session);
            } catch (RuntimeException abortException) {
                exception.addSuppressed(abortException);
                LOGGER.error("采样前置检查点中止失败，jobId={}，checkpointId={}",
                        context.getJob().getJobId(), session.getCheckpointId(),
                        abortException);
            }
            LOGGER.error("采样前置产物分批处理失败，jobId={}，checkpointId={}",
                    context.getJob().getJobId(), session.getCheckpointId(),
                    exception);
            return StageResult.failure("BATCHED_CHECKPOINT_EXECUTION_FAILED",
                    exception.getClass().getSimpleName(), false, 1L, 1L);
        }
    }

    private StrategyBatchResult executeStrategies(
            StageExecutionContext context,
            RahaDataset dataset,
            List<StrategyPlan> plans,
            ArtifactVersion version) {
        if (plans.isEmpty()) {
            return new StrategyBatchResult(
                    Collections.<StrategyExecutionResult>emptyList());
        }
        return executionService.execute(context.getJob().getJobId(),
                context.getStage().getStageId(), dataset, plans,
                context.getConfig().getStrategyConfig()
                        .getStrategyTimeoutMillis(), version,
                context.getConfig().getResourceConfig()
                        .getMaxParallelStrategies(),
                context.getConfig().getResourceConfig()
                        .getStageTimeoutMillis());
    }

    private FeatureAssemblyResult assemble(
            StageExecutionContext context,
            RahaDataset dataset,
            List<StrategyPlan> plans,
            List<StrategyHit> hits,
            ArtifactVersion version) {
        if (context.getConfig().getResourceConfig()
                .isFeatureParallelEnabled()) {
            return featureService.assembleAndSaveParallel(
                    context.getJob().getJobId(), dataset, plans, hits,
                    context.getConfig().getFeatureConfig(), version,
                    context.getConfig().getResourceConfig()
                            .getMaxParallelColumns(),
                    context.getConfig().getResourceConfig()
                            .getStageTimeoutMillis());
        }
        return featureService.assembleAndSave(context.getJob().getJobId(),
                dataset, plans, hits, context.getConfig().getFeatureConfig(),
                version);
    }

    private ClusteringBatchResult cluster(StageExecutionContext context,
                                          FeatureAssemblyResult features,
                                          ArtifactVersion version) {
        if (context.getConfig().getResourceConfig()
                .isClusteringParallelEnabled()) {
            return clusteringService.clusterAndSaveParallel(
                    context.getJob().getJobId(), features,
                    context.getConfig().getClusteringConfig(),
                    context.getConfig().getRandomSeed(), version,
                    context.getConfig().getResourceConfig()
                            .getMaxParallelColumns(),
                    context.getConfig().getResourceConfig()
                            .getStageTimeoutMillis());
        }
        return clusteringService.clusterAndSave(
                context.getJob().getJobId(), features,
                context.getConfig().getClusteringConfig(),
                context.getConfig().getRandomSeed(), version);
    }

    private List<CellLabel> labels(StageExecutionContext context) {
        Object value = context.getAttributes().get(
                StageAttributeKeys.CELL_LABELS);
        return value instanceof List ? (List<CellLabel>) value : initialLabels;
    }

    private static List<ColumnMetadata> detectableColumns(RahaDataset dataset) {
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        for (ColumnMetadata column : dataset.getColumns()) {
            if (column.isDetectable()) {
                columns.add(column);
            }
        }
        Collections.sort(columns,
                Comparator.comparingInt(ColumnMetadata::getOrdinal));
        return columns;
    }

    private static RahaDataset isolate(RahaDataset source,
                                       List<ColumnMetadata> batchColumns) {
        Set<String> included = columnNames(batchColumns);
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        Map<String, ColumnProfile> profiles =
                new LinkedHashMap<String, ColumnProfile>();
        for (ColumnMetadata column : source.getColumns()) {
            if (!column.isDetectable() || included.contains(column.getName())) {
                columns.add(column);
                if (source.getProfiles().containsKey(column.getName())) {
                    profiles.put(column.getName(),
                            source.getProfiles().get(column.getName()));
                }
            }
        }
        return new RahaDataset(source.getDatasetId(), source.getSnapshotId(),
                source.getTableName(), source.getRowIdColumn(), columns,
                source.getDataFrame(), source.getSchemaHash(), profiles);
    }

    private static Set<String> columnNames(List<ColumnMetadata> columns) {
        Set<String> names = new LinkedHashSet<String>();
        for (ColumnMetadata column : columns) {
            names.add(column.getName());
        }
        return names;
    }

    private static List<StrategyPlan> plansForColumns(
            List<StrategyPlan> plans,
            Set<String> columns) {
        List<StrategyPlan> result = new ArrayList<StrategyPlan>();
        for (StrategyPlan plan : plans) {
            if (columns.containsAll(plan.getTargetColumns())) {
                result.add(plan);
            }
        }
        return result;
    }

    private static ColumnClusteringResult summaryOnly(
            ColumnClusteringResult source) {
        return new ColumnClusteringResult(source.getColumnName(),
                source.getAlgorithm(), source.getDistanceMetric(),
                source.getRequestedClusterCount(),
                source.getEffectiveClusterCount(), source.getRandomSeed(),
                source.getClusterVersion(), source.getStatus(),
                source.getMessage(),
                Collections.emptyList(), source.getCreatedAt());
    }

    private static long createdAt(StageExecutionContext context) {
        return Math.max(1L, context.getStage().getStartedAt());
    }
}
