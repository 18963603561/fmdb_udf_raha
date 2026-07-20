package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import java.util.List;
import java.util.Optional;

/**
 * 保存列级模型元数据、发布状态和依赖版本。
 */
public interface ModelMetadataRepository {

    void saveAll(List<RahaColumnModel> models,
                 ArtifactVersion version,
                 long updatedAt);

    Optional<RahaColumnModel> find(String datasetId,
                                   String columnName,
                                   String modelVersion);

    List<RahaColumnModel> findByColumn(String datasetId, String columnName);

    /**
     * 按不可变模型集合版本读取各字段最新状态的列模型。
     *
     * @param modelSetVersion 模型集合版本
     * @return 按字段名称排序的列模型
     */
    List<RahaColumnModel> findByModelSetVersion(String modelSetVersion);

    Optional<RahaColumnModel> findPublished(String datasetId, String columnName);
}
