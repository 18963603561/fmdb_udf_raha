package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.model.ColumnModelStore;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;

/**
 * 只加载已发布且与当前模式、特征和策略版本兼容的列级模型。
 */
public final class PublishedColumnModelLoader {

    /** 模型元数据仓储。 */
    private final ModelMetadataRepository repository;
    /** 模型参数文件存储。 */
    private final ColumnModelStore store;
    /** 模型兼容校验器。 */
    private final ColumnModelCompatibilityValidator validator;

    public PublishedColumnModelLoader(ModelMetadataRepository repository,
                                      ColumnModelStore store,
                                      ColumnModelCompatibilityValidator validator) {
        if (repository == null || store == null || validator == null) {
            throw new IllegalArgumentException("已发布模型加载器依赖不能为空");
        }
        this.repository = repository;
        this.store = store;
        this.validator = validator;
    }

    public ColumnModelArtifact load(String datasetId,
                                    String columnName,
                                    String schemaHash,
                                    String featureDictionaryVersion,
                                    String strategyPlanVersion) {
        RahaColumnModel metadata = repository.findPublished(datasetId, columnName)
                .orElseThrow(() -> new IllegalStateException("当前字段没有已发布模型"));
        ColumnModelArtifact artifact = store.load(metadata.getModelPath());
        validator.validate(metadata, artifact, schemaHash,
                featureDictionaryVersion, strategyPlanVersion);
        // 训练参数文件保持不可变，发布元数据中的评测阈值作为生产预测阈值生效。
        return artifact.withThreshold(metadata.getThreshold());
    }
}
