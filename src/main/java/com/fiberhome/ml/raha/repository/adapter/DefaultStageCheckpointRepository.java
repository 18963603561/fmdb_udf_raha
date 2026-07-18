package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.checkpoint.StageCheckpoint;
import com.fiberhome.ml.raha.checkpoint.StageCheckpointStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.core.RepositoryKey;
import com.fiberhome.ml.raha.repository.core.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.core.RepositoryRecord;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.repository.port.StageCheckpointRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 基于统一仓储保存每次检查点尝试并执行严格输入版本复用判断。
 */
public final class DefaultStageCheckpointRepository
        implements StageCheckpointRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultStageCheckpointRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public SaveOutcome save(StageCheckpoint checkpoint, long updatedAt) {
        if (checkpoint == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("检查点和更新时间必须有效");
        }
        ArtifactVersion recordVersion = new ArtifactVersion(
                checkpoint.getInputVersion().getConfigVersion(),
                checkpoint.getInputVersion().getSnapshotId(),
                checkpoint.getCheckpointId(), checkpoint.getAttemptId());
        return repository.save(new RepositoryRecord<StageCheckpoint>(
                new RepositoryKey(RepositoryNamespace.STAGE_CHECKPOINT,
                        partition(checkpoint.getJobId(), checkpoint.getStageType()),
                        checkpoint.getCheckpointId()),
                recordVersion, checkpoint, updatedAt));
    }

    @Override
    public List<StageCheckpoint> findAttempts(String jobId, StageType stageType) {
        if (stageType == null) {
            throw new IllegalArgumentException("检查点阶段类型不能为空");
        }
        List<RepositoryRecord<StageCheckpoint>> records = repository.findByPartition(
                RepositoryNamespace.STAGE_CHECKPOINT,
                partition(ValueUtils.requireNotBlank(jobId, "检查点任务标识"), stageType),
                StageCheckpoint.class);
        List<StageCheckpoint> checkpoints = new ArrayList<StageCheckpoint>();
        for (RepositoryRecord<StageCheckpoint> record : records) {
            checkpoints.add(record.getPayload());
        }
        Collections.sort(checkpoints, Comparator.comparingInt(
                StageCheckpoint::getAttemptId));
        return Collections.unmodifiableList(checkpoints);
    }

    @Override
    public Optional<StageCheckpoint> findReusable(String jobId,
                                                  StageType stageType,
                                                  ArtifactVersion inputVersion,
                                                  String inputFingerprint) {
        if (inputVersion == null) {
            throw new IllegalArgumentException("检查点输入版本不能为空");
        }
        String fingerprint = ValueUtils.requireNotBlank(
                inputFingerprint, "检查点输入指纹");
        StageCheckpoint reusable = null;
        for (StageCheckpoint checkpoint : findAttempts(jobId, stageType)) {
            ArtifactVersion candidate = checkpoint.getInputVersion();
            // 只有配置、快照和输入内容全部一致的成功检查点才可跳过重复计算。
            if (checkpoint.getStatus() == StageCheckpointStatus.SUCCEEDED
                    && candidate.getConfigVersion().equals(inputVersion.getConfigVersion())
                    && candidate.getSnapshotId().equals(inputVersion.getSnapshotId())
                    && checkpoint.getInputFingerprint().equals(fingerprint)) {
                reusable = checkpoint;
            }
        }
        return Optional.ofNullable(reusable);
    }

    private static String partition(String jobId, StageType stageType) {
        return jobId.length() + ":" + jobId + stageType.name();
    }
}
