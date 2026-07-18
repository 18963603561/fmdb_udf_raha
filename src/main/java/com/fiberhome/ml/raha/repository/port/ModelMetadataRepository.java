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

    Optional<RahaColumnModel> findPublished(String datasetId, String columnName);
}
