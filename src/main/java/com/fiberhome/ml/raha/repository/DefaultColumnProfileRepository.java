package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于统一仓储实现列画像持久化。
 */
public final class DefaultColumnProfileRepository implements ColumnProfileRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultColumnProfileRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public SaveOutcome save(String datasetId,
                            String snapshotId,
                            ColumnProfile profile,
                            ArtifactVersion version,
                            long updatedAt) {
        if (profile == null || version == null) {
            throw new IllegalArgumentException("列画像和结果版本不能为空");
        }
        RepositoryKey key = key(datasetId, snapshotId, profile.getColumnName());
        return repository.save(new RepositoryRecord<ColumnProfile>(
                key, version, profile, updatedAt));
    }

    @Override
    public void saveAll(String datasetId,
                        String snapshotId,
                        Map<String, ColumnProfile> profiles,
                        ArtifactVersion version,
                        long updatedAt) {
        if (profiles == null || profiles.isEmpty() || version == null) {
            throw new IllegalArgumentException("列画像集合和结果版本不能为空");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (ColumnProfile profile : profiles.values()) {
                RepositoryKey key = key(datasetId, snapshotId, profile.getColumnName());
                transactionRepository.save(new RepositoryRecord<ColumnProfile>(
                        key, version, profile, updatedAt));
            }
        });
    }

    @Override
    public Optional<ColumnProfile> find(String datasetId, String snapshotId, String columnName) {
        Optional<RepositoryRecord<ColumnProfile>> record = repository.find(
                key(datasetId, snapshotId, columnName), ColumnProfile.class);
        return record.isPresent()
                ? Optional.of(record.get().getPayload())
                : Optional.<ColumnProfile>empty();
    }

    @Override
    public List<ColumnProfile> findBySnapshot(String datasetId, String snapshotId) {
        List<RepositoryRecord<ColumnProfile>> records = repository.findByPartition(
                RepositoryNamespace.COLUMN_PROFILE, partition(datasetId, snapshotId), ColumnProfile.class);
        List<ColumnProfile> profiles = new ArrayList<ColumnProfile>(records.size());
        for (RepositoryRecord<ColumnProfile> record : records) {
            profiles.add(record.getPayload());
        }
        return Collections.unmodifiableList(profiles);
    }

    private static RepositoryKey key(String datasetId, String snapshotId, String columnName) {
        return new RepositoryKey(RepositoryNamespace.COLUMN_PROFILE,
                partition(datasetId, snapshotId),
                ValueUtils.requireNotBlank(columnName, "字段名称"));
    }

    private static String partition(String datasetId, String snapshotId) {
        String validatedDatasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String validatedSnapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        return validatedDatasetId.length() + ":" + validatedDatasetId
                + validatedSnapshotId.length() + ":" + validatedSnapshotId;
    }
}
