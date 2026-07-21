package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.core.RepositoryKey;
import com.fiberhome.ml.raha.repository.core.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.core.RepositoryRecord;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
                transactionRepository.save(new RepositoryRecord<RahaColumnModel>(
                        new RepositoryKey(RepositoryNamespace.MODEL_SET,
                                model.getModelSetVersion(),
                                model.getColumnName().length() + ":"
                                        + model.getColumnName()
                                        + model.getModelVersion().length() + ":"
                                        + model.getModelVersion()),
                        version, model, updatedAt));
                transactionRepository.save(new RepositoryRecord<RahaColumnModel>(
                        new RepositoryKey(RepositoryNamespace.MODEL_SET,
                                datasetPartition(model.getDatasetId()),
                                model.getModelSetVersion().length() + ":"
                                        + model.getModelSetVersion()
                                        + model.getColumnName().length() + ":"
                                        + model.getColumnName()
                                        + model.getModelVersion().length() + ":"
                                        + model.getModelVersion()),
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
    public List<RahaColumnModel> findByModelSetVersion(String modelSetVersion) {
        List<RepositoryRecord<RahaColumnModel>> records =
                repository.findByPartition(RepositoryNamespace.MODEL_SET,
                        ValueUtils.requireNotBlank(modelSetVersion, "模型集合版本"),
                        RahaColumnModel.class);
        List<RahaColumnModel> result =
                new ArrayList<RahaColumnModel>(records.size());
        for (RepositoryRecord<RahaColumnModel> record : records) {
            result.add(record.getPayload());
        }
        Collections.sort(result, Comparator.comparing(
                RahaColumnModel::getColumnName)
                .thenComparing(RahaColumnModel::getModelVersion));
        return Collections.unmodifiableList(result);
    }

    @Override
    public List<RahaColumnModel> findLatestPublishedModelSet(String datasetId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        List<RepositoryRecord<RahaColumnModel>> records =
                repository.findByPartition(RepositoryNamespace.MODEL_SET,
                        datasetPartition(dataset), RahaColumnModel.class);
        Map<String, List<RahaColumnModel>> bySet =
                new LinkedHashMap<String, List<RahaColumnModel>>();
        for (RepositoryRecord<RahaColumnModel> record : records) {
            RahaColumnModel model = record.getPayload();
            if (!dataset.equals(model.getDatasetId())) {
                throw new IllegalStateException("模型数据集索引存在错误记录");
            }
            List<RahaColumnModel> group = bySet.get(model.getModelSetVersion());
            if (group == null) {
                group = new ArrayList<RahaColumnModel>();
                bySet.put(model.getModelSetVersion(), group);
            }
            group.add(model);
        }
        List<RahaColumnModel> selected = null;
        for (List<RahaColumnModel> models : bySet.values()) {
            if (!allActivePublished(models)) {
                continue;
            }
            if (selected == null || compareModelSet(models, selected) > 0) {
                selected = models;
            }
        }
        if (selected == null) {
            return Collections.emptyList();
        }
        List<RahaColumnModel> result = new ArrayList<RahaColumnModel>(selected);
        Collections.sort(result, Comparator.comparing(
                RahaColumnModel::getColumnName)
                .thenComparing(RahaColumnModel::getModelVersion));
        return Collections.unmodifiableList(result);
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

    private static String datasetPartition(String datasetId) {
        return "dataset:" + datasetId.length() + ":" + datasetId;
    }

    private static boolean allActivePublished(List<RahaColumnModel> models) {
        if (models == null || models.isEmpty()) {
            return false;
        }
        for (RahaColumnModel model : models) {
            if (model.getStatus() != ModelStatus.PUBLISHED
                    || model.getPublishedAt() == null) {
                return false;
            }
        }
        return true;
    }

    private static int compareModelSet(List<RahaColumnModel> first,
                                       List<RahaColumnModel> second) {
        long firstPublished = latestPublishedAt(first);
        long secondPublished = latestPublishedAt(second);
        if (firstPublished != secondPublished) {
            return firstPublished < secondPublished ? -1 : 1;
        }
        long firstCreated = latestCreatedAt(first);
        long secondCreated = latestCreatedAt(second);
        if (firstCreated != secondCreated) {
            return firstCreated < secondCreated ? -1 : 1;
        }
        return first.get(0).getModelSetVersion().compareTo(
                second.get(0).getModelSetVersion());
    }

    private static long latestPublishedAt(List<RahaColumnModel> models) {
        long value = 0L;
        for (RahaColumnModel model : models) {
            value = Math.max(value, model.getPublishedAt().longValue());
        }
        return value;
    }

    private static long latestCreatedAt(List<RahaColumnModel> models) {
        long value = 0L;
        for (RahaColumnModel model : models) {
            value = Math.max(value, model.getCreatedAt());
        }
        return value;
    }
}
