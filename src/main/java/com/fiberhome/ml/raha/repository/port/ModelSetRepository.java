package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.model.domain.ModelSetManifest;
import java.util.Optional;

/**
 * 按不可变模型集合版本读取完整列模型清单和公共兼容信息。
 */
public interface ModelSetRepository {

    /**
     * 查询指定模型集合。
     *
     * @param modelSetVersion 不可变模型集合版本
     * @return 存在时返回经过一致性校验的模型集合清单
     */
    Optional<ModelSetManifest> find(String modelSetVersion);

    /**
     * 查询指定数据集当前最新的完整已发布模型集合。
     *
     * @param datasetId 数据集标识
     * @return 存在时返回最新完整已发布模型集合
     */
    Optional<ModelSetManifest> findLatestPublishedByDataset(String datasetId);
}
