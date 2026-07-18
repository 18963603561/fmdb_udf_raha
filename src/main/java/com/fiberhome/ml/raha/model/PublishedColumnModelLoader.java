package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ModelStatus;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按精确版本或当前发布选择器加载兼容的列级模型。
 */
public final class PublishedColumnModelLoader {

    /** 按字段选择当前已发布模型集合的固定选择器。 */
    public static final String CURRENT_PUBLISHED_VERSION = "PUBLISHED";
    /** 模型加载日志。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PublishedColumnModelLoader.class);
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
                                    String modelVersion,
                                    String schemaHash,
                                    String featureDictionaryVersion,
                                    String strategyPlanVersion) {
        LOGGER.info("开始加载目标列模型，datasetId={}，columnName={}，modelVersion={}",
                datasetId, columnName, modelVersion);
        try {
            // 整表检测可选择每个字段当前发布版本，精确版本请求则禁止回退。
            RahaColumnModel metadata = CURRENT_PUBLISHED_VERSION.equals(modelVersion)
                    ? repository.findPublished(datasetId, columnName)
                    .orElseThrow(() -> new IllegalStateException(
                            "当前字段没有已发布模型"))
                    : repository.find(datasetId, columnName, modelVersion)
                    .orElseThrow(() -> new IllegalStateException(
                            "当前字段不存在指定模型版本"));
            if (metadata.getStatus() != ModelStatus.PUBLISHED) {
                throw new IllegalStateException("指定模型版本尚未发布");
            }
            // 模型文件存储属于外部数据端口，加载失败必须保留字段和版本上下文。
            ColumnModelArtifact artifact = store.load(metadata.getModelPath());
            validator.validate(metadata, artifact, schemaHash,
                    featureDictionaryVersion, strategyPlanVersion);
            LOGGER.info("目标列模型加载完成，datasetId={}，columnName={}，"
                            + "modelVersion={}",
                    datasetId, columnName, modelVersion);
            // 训练参数文件保持不可变，发布元数据中的评测阈值作为生产预测阈值生效。
            return artifact.withThreshold(metadata.getThreshold());
        } catch (RuntimeException exception) {
            LOGGER.error("目标列模型加载失败，datasetId={}，columnName={}，"
                            + "modelVersion={}",
                    datasetId, columnName, modelVersion, exception);
            throw exception;
        }
    }
}
