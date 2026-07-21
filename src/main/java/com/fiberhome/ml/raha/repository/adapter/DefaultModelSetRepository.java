package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.model.domain.ModelSetManifest;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.ModelSetRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.List;
import java.util.Optional;

/**
 * 基于列模型元数据仓储组装不可变模型集合清单。
 */
public final class DefaultModelSetRepository implements ModelSetRepository {

    /** 提供模型集合版本查询的列模型元数据仓储。 */
    private final ModelMetadataRepository metadataRepository;

    public DefaultModelSetRepository(
            ModelMetadataRepository metadataRepository) {
        if (metadataRepository == null) {
            throw new IllegalArgumentException("模型集合元数据仓储不能为空");
        }
        this.metadataRepository = metadataRepository;
    }

    @Override
    public Optional<ModelSetManifest> find(String modelSetVersion) {
        String version = ValueUtils.requireNotBlank(
                modelSetVersion, "模型集合版本");
        List<RahaColumnModel> models = metadataRepository
                .findByModelSetVersion(version);
        return models.isEmpty()
                ? Optional.<ModelSetManifest>empty()
                : Optional.of(new ModelSetManifest(version, models));
    }

    @Override
    public Optional<ModelSetManifest> findLatestPublishedByDataset(
            String datasetId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        List<RahaColumnModel> models = metadataRepository
                .findLatestPublishedModelSet(dataset);
        return models.isEmpty()
                ? Optional.<ModelSetManifest>empty()
                : Optional.of(new ModelSetManifest(
                models.get(0).getModelSetVersion(), models));
    }
}
