package com.fiberhome.ml.raha.job.stage.checkpoint;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在模型发布和结果校验成功后清理超过保留期的 HDFS 检查点明细。
 */
public final class SnapshotCheckpointCleanupStageHandler
        implements StageHandler {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SnapshotCheckpointCleanupStageHandler.class);
    /** 快照检查点仓储。 */
    private final SnapshotCheckpointRepository repository;

    public SnapshotCheckpointCleanupStageHandler(
            SnapshotCheckpointRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("快照检查点仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public StageType getStageType() {
        return StageType.CLEANUP_CHECKPOINT;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object value = context.getAttributes().get(
                StageAttributeKeys.TRAIN_SERVICE_RESULT);
        if (!(value instanceof RahaServiceResult)
                || ((RahaServiceResult<?>) value).getStatus()
                != JobStatus.SUCCEEDED) {
            return StageResult.skipped("模型未成功发布，不执行检查点清理");
        }
        long currentTimeMillis = Math.max(System.currentTimeMillis(),
                context.getStage().getStartedAt());
        try {
            repository.cleanupExpired(currentTimeMillis);
            LOGGER.info("训练成功后的过期检查点清理完成，jobId={}",
                    context.getJob().getJobId());
            return StageResult.success();
        } catch (RuntimeException exception) {
            // 清理失败不应回滚已经发布的模型，但必须作为可观测告警保留上下文。
            LOGGER.warn("训练成功后的过期检查点清理失败，jobId={}",
                    context.getJob().getJobId(), exception);
            return StageResult.skipped("过期检查点清理失败，已保留明细等待下次清理");
        }
    }
}
