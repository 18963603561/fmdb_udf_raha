package com.fiberhome.ml.raha.job.stage.checkpoint;

import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在采样阶段完成聚类后固化可复用的快照前置产物。
 */
public final class SnapshotCheckpointStageHandler implements StageHandler {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SnapshotCheckpointStageHandler.class);
    /** 快照检查点仓储。 */
    private final SnapshotCheckpointRepository repository;

    public SnapshotCheckpointStageHandler(
            SnapshotCheckpointRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("快照检查点仓储不能为空");
        }
        this.repository = repository;
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
        Object strategyValue = context.getAttributes().get(
                StageAttributeKeys.STRATEGY_BATCH_RESULT);
        Object featureValue = context.getAttributes().get(
                StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        Object clusteringValue = context.getAttributes().get(
                StageAttributeKeys.CLUSTERING_BATCH_RESULT);
        if (!(datasetValue instanceof RahaDataset)
                || !(snapshotValue instanceof DatasetSnapshot)
                || !(planValue instanceof List)
                || !(planVersionValue instanceof String)
                || !(strategyValue instanceof StrategyBatchResult)
                || !(featureValue instanceof FeatureAssemblyResult)
                || !(clusteringValue instanceof ClusteringBatchResult)) {
            return StageResult.failure("SNAPSHOT_CHECKPOINT_INPUT_REQUIRED",
                    "快照检查点缺少画像、策略、特征或聚类产物", false, 0L, 0L);
        }
        RahaDataset dataset = (RahaDataset) datasetValue;
        DatasetSnapshot snapshot = (DatasetSnapshot) snapshotValue;
        repository.save(context.getJob().getJobId(), dataset, snapshot,
                (List<StrategyPlan>) planValue, (String) planVersionValue,
                (StrategyBatchResult) strategyValue,
                (FeatureAssemblyResult) featureValue,
                (ClusteringBatchResult) clusteringValue,
                context.getConfig().getExecutionConfigFingerprint(),
                createdAt(context));
        LOGGER.info("采样快照检查点阶段完成，jobId={}，datasetId={}，snapshotId={}",
                context.getJob().getJobId(), snapshot.getDatasetId(),
                snapshot.getSnapshotId());
        return StageResult.successWithSnapshot(snapshot.getSnapshotId());
    }

    private static long createdAt(StageExecutionContext context) {
        long startedAt = context.getStage().getStartedAt();
        return startedAt > 0L ? startedAt : Math.max(1L, System.currentTimeMillis());
    }
}
