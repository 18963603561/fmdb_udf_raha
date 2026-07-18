package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.data.ModelStatus;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 基于统一仓储事务保存列级模型元数据和发布状态。
 */
public final class DefaultModelMetadataRepository implements ModelMetadataRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultModelMetadataRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public void saveAll(List<RahaColumnModel> models,
                        ArtifactVersion version,
                        long updatedAt) {
        if (models == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("模型元数据、版本和更新时间必须有效");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (RahaColumnModel model : models) {
                if (model == null) {
                    throw new IllegalArgumentException("模型元数据不能包含空值");
                }
                transactionRepository.save(new RepositoryRecord<RahaColumnModel>(
                        new RepositoryKey(RepositoryNamespace.COLUMN_MODEL,
                                partition(model.getDatasetId(), model.getColumnName()),
                                model.getModelVersion()),
                        version, model, updatedAt));
            }
        });
    }

    @Override
    public Optional<RahaColumnModel> find(String datasetId,
                                          String columnName,
                                          String modelVersion) {
        Optional<RepositoryRecord<RahaColumnModel>> record = repository.find(
                new RepositoryKey(RepositoryNamespace.COLUMN_MODEL,
                        partition(ValueUtils.requireNotBlank(datasetId, "数据集标识"),
                                ValueUtils.requireNotBlank(columnName, "字段名称")),
                        ValueUtils.requireNotBlank(modelVersion, "模型版本")),
                RahaColumnModel.class);
        return record.isPresent() ? Optional.of(record.get().getPayload())
                : Optional.<RahaColumnModel>empty();
    }

    @Override
    public List<RahaColumnModel> findByColumn(String datasetId, String columnName) {
        List<RepositoryRecord<RahaColumnModel>> records = repository.findByPartition(
                RepositoryNamespace.COLUMN_MODEL,
                partition(ValueUtils.requireNotBlank(datasetId, "数据集标识"),
                        ValueUtils.requireNotBlank(columnName, "字段名称")),
                RahaColumnModel.class);
        List<RahaColumnModel> models = new ArrayList<RahaColumnModel>(records.size());
        for (RepositoryRecord<RahaColumnModel> record : records) {
            models.add(record.getPayload());
        }
        Collections.sort(models, Comparator.comparingLong(RahaColumnModel::getCreatedAt)
                .thenComparing(RahaColumnModel::getModelVersion));
        return Collections.unmodifiableList(models);
    }

    @Override
    public Optional<RahaColumnModel> findPublished(String datasetId, String columnName) {
        RahaColumnModel published = null;
        for (RahaColumnModel model : findByColumn(datasetId, columnName)) {
            if (model.getStatus() == ModelStatus.PUBLISHED) {
                if (published != null) {
                    throw new IllegalStateException("同一字段存在多个已发布模型");
                }
                published = model;
            }
        }
        return Optional.ofNullable(published);
    }

    private static String partition(String datasetId, String columnName) {
        return datasetId.length() + ":" + datasetId
                + columnName.length() + ":" + columnName;
    }
}
