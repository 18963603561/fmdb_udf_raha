package com.fiberhome.ml.raha.job.stage.checkpoint;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import java.util.Collections;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从采样快照检查点恢复训练前置产物。
 */
public final class RestoreSnapshotCheckpointStageHandler implements StageHandler {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSnapshotCheckpointStageHandler.class);
    /** 快照检查点仓储。 */
    private final SnapshotCheckpointRepository repository;

    public RestoreSnapshotCheckpointStageHandler(
            SnapshotCheckpointRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("快照检查点仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public StageType getStageType() {
        return StageType.RESTORE_SNAPSHOT_CHECKPOINT;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        String snapshotId = context.getConfig().getSnapshotId();
        if (snapshotId == null || snapshotId.trim().isEmpty()) {
            return StageResult.failure("SNAPSHOT_ID_REQUIRED",
                    "复用快照检查点时必须指定 snapshotId", false, 0L, 0L);
        }
        Optional<SnapshotPreparedArtifacts> artifacts = repository.restore(
                context.getConfig().getDatasetId(), snapshotId,
                context.getConfig().getExecutionConfigFingerprint());
        if (!artifacts.isPresent()) {
            return StageResult.failure("CHECKPOINT_NOT_FOUND",
                    "未找到匹配的采样快照检查点", false, 0L, 0L);
        }
        SnapshotPreparedArtifacts value = artifacts.get();
        context.getAttributes().put(StageAttributeKeys.RAHA_DATASET,
                value.getDataset());
        context.getAttributes().put(StageAttributeKeys.DATASET_SNAPSHOT,
                value.getSnapshot());
        context.getAttributes().put(StageAttributeKeys.STRATEGY_PLANS,
                value.getStrategyPlans());
        context.getAttributes().put(StageAttributeKeys.STRATEGY_PLAN_VERSION,
                value.getStrategyPlanVersion());
        context.getAttributes().put(StageAttributeKeys.STRATEGY_BATCH_RESULT,
                value.getStrategyBatch());
        context.getAttributes().put(StageAttributeKeys.STRATEGY_HITS,
                Collections.emptyList());
        context.getAttributes().put(StageAttributeKeys.FEATURE_ASSEMBLY_RESULT,
                value.getFeatures());
        context.getAttributes().put(StageAttributeKeys.CLUSTERING_BATCH_RESULT,
                value.getClustering());
        LOGGER.info("训练快照检查点恢复完成，jobId={}，checkpointId={}，datasetId={}，snapshotId={}",
                context.getJob().getJobId(), value.getCheckpointId(),
                value.getDataset().getDatasetId(),
                value.getDataset().getSnapshotId());
        return StageResult.successWithSnapshot(value.getSnapshot().getSnapshotId());
    }
}
