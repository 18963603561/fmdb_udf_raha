package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;

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
