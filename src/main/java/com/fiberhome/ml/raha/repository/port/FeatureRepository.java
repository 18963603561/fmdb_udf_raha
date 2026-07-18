package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import java.util.List;
import java.util.Optional;

/**
 * 持久化按列特征字典和单元格稀疏向量的仓储契约。
 */
public interface FeatureRepository {

    void save(String jobId,
              FeatureAssemblyResult result,
              ArtifactVersion version,
              long updatedAt);

    Optional<FeatureDictionary> findDictionary(String jobId, String columnName);

    List<SparseFeatureRow> findRows(String jobId, String columnName);
}
