package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.job.RahaJob;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Optional;

/**
 * 基于统一仓储实现任务状态保存和幂等查询。
 */
public final class DefaultJobRepository implements JobRepository {

    /** 快照尚未生成时使用的占位版本。 */
    private static final String PENDING_SNAPSHOT = "PENDING_SNAPSHOT";
    /** 作业记录使用的固定产物作用域。 */
    private static final String JOB_ARTIFACT_SCOPE = "JOB";
    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultJobRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public SaveOutcome save(RahaJob job, long updatedAt) {
        if (job == null) {
            throw new IllegalArgumentException("任务不能为空");
        }
        String snapshotId = isBlank(job.getSnapshotId()) ? PENDING_SNAPSHOT : job.getSnapshotId();
        RepositoryKey key = key(job.getDatasetId(), job.getIdempotentKey());
        ArtifactVersion version = new ArtifactVersion(
                job.getConfigVersion(), snapshotId, JOB_ARTIFACT_SCOPE, 0);
        return repository.save(new RepositoryRecord<RahaJob>(
                key, version, job.snapshot(), updatedAt));
    }

    @Override
    public Optional<RahaJob> findByIdempotentKey(String datasetId, String idempotentKey) {
        Optional<RepositoryRecord<RahaJob>> record = repository.find(
                key(datasetId, idempotentKey), RahaJob.class);
        if (!record.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(record.get().getPayload().snapshot());
    }

    private static RepositoryKey key(String datasetId, String idempotentKey) {
        return new RepositoryKey(RepositoryNamespace.JOB,
                ValueUtils.requireNotBlank(datasetId, "数据集标识"),
                ValueUtils.requireNotBlank(idempotentKey, "幂等键"));
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
