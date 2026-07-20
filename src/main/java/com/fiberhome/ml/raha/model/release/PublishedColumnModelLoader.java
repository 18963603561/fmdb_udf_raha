package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.model.ColumnModelStore;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * 从调用方明确指定的不可变模型集合中加载列模型。
     *
     * @param modelSetVersion 不可变模型集合版本
     * @param datasetId 逻辑数据集标识
     * @param columnName 目标字段
     * @param schemaHash 当前输入模式哈希
     * @param featureDictionaryVersion 当前特征字典版本
     * @param strategyPlanVersion 当前策略计划版本
     * @return 经过发布状态和兼容性校验的模型参数
     */
    public ColumnModelArtifact load(String modelSetVersion,
                                    String datasetId,
                                    String columnName,
                                    String schemaHash,
                                    String featureDictionaryVersion,
                                    String strategyPlanVersion) {
        String setVersion = ValueUtils.requireNotBlank(
                modelSetVersion, "检测模型集合版本");
        List<RahaColumnModel> matches = new ArrayList<RahaColumnModel>();
        for (RahaColumnModel model : repository.findByModelSetVersion(setVersion)) {
            if (datasetId.equals(model.getDatasetId())
                    && columnName.equals(model.getColumnName())) {
                matches.add(model);
            }
        }
        if (matches.size() != 1) {
            throw new IllegalStateException("指定模型集合未提供唯一字段模型："
                    + setVersion + "/" + columnName);
        }
        RahaColumnModel metadata = matches.get(0);
        if (metadata.getPublishedAt() == null
                || (metadata.getStatus() != ModelStatus.PUBLISHED
                && metadata.getStatus() != ModelStatus.DISABLED)) {
            throw new IllegalStateException("指定模型集合字段从未发布："
                    + setVersion + "/" + columnName);
        }
        ColumnModelArtifact artifact = store.load(metadata.getModelPath());
        validator.validate(metadata, artifact, schemaHash,
                featureDictionaryVersion, strategyPlanVersion);
        return artifact.withThreshold(metadata.getThreshold());
    }
}
