package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于统一仓储事务保存特征字典和稀疏向量。
 */
public final class DefaultFeatureRepository implements FeatureRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultFeatureRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public void save(String jobId,
                     FeatureAssemblyResult result,
                     ArtifactVersion version,
                     long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (result == null || version == null) {
            throw new IllegalArgumentException("特征结果和版本不能为空");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (Map.Entry<String, FeatureDictionary> entry
                    : result.getDictionaries().entrySet()) {
                transactionRepository.save(new RepositoryRecord<FeatureDictionary>(
                        new RepositoryKey(RepositoryNamespace.FEATURE_DICTIONARY,
                                validatedJobId, entry.getKey()),
                        version, entry.getValue(), updatedAt));
            }
            for (SparseFeatureRow row : result.getRows()) {
                transactionRepository.save(new RepositoryRecord<SparseFeatureRow>(
                        new RepositoryKey(RepositoryNamespace.SPARSE_FEATURE,
                                rowPartition(validatedJobId, row.getColumnName()), row.getCellId()),
                        version, row, updatedAt));
            }
        });
    }

    @Override
    public Optional<FeatureDictionary> findDictionary(String jobId, String columnName) {
        Optional<RepositoryRecord<FeatureDictionary>> record = repository.find(
                new RepositoryKey(RepositoryNamespace.FEATURE_DICTIONARY,
                        ValueUtils.requireNotBlank(jobId, "任务标识"),
                        ValueUtils.requireNotBlank(columnName, "字段名称")),
                FeatureDictionary.class);
        return record.isPresent() ? Optional.of(record.get().getPayload())
                : Optional.<FeatureDictionary>empty();
    }

    @Override
    public List<SparseFeatureRow> findRows(String jobId, String columnName) {
        List<RepositoryRecord<SparseFeatureRow>> records = repository.findByPartition(
                RepositoryNamespace.SPARSE_FEATURE,
                rowPartition(ValueUtils.requireNotBlank(jobId, "任务标识"),
                        ValueUtils.requireNotBlank(columnName, "字段名称")),
                SparseFeatureRow.class);
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>(records.size());
        for (RepositoryRecord<SparseFeatureRow> record : records) {
            rows.add(record.getPayload());
        }
        return Collections.unmodifiableList(rows);
    }

    private static String rowPartition(String jobId, String columnName) {
        return jobId.length() + ":" + jobId + columnName.length() + ":" + columnName;
    }
}
