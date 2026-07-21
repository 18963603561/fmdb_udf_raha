package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 校验模型元数据、模型文件和当前输入模式及特征依赖兼容性。
 */
public final class ColumnModelCompatibilityValidator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ColumnModelCompatibilityValidator.class);

    public void validate(RahaColumnModel metadata,
                         ColumnModelArtifact artifact,
                         String schemaHash,
                         String featureDictionaryVersion,
                         String strategyPlanVersion) {
        if (metadata == null || artifact == null) {
            throw new IllegalArgumentException("模型元数据和参数文件不能为空");
        }
        if (!metadata.getModelVersion().equals(artifact.getModelVersion())
                || !metadata.getColumnName().equals(artifact.getColumnName())
                || metadata.getClassifierType() != artifact.getClassifierType()
                || !metadata.getFeatureDictionaryVersion().equals(
                artifact.getFeatureDictionaryVersion())) {
            throw new IllegalStateException("模型元数据与参数文件不一致");
        }
        if (!metadata.getSchemaHash().equals(schemaHash)) {
            throw new IllegalStateException("模型与当前输入模式不兼容");
        }
        if (!metadata.getFeatureDictionaryVersion().equals(featureDictionaryVersion)) {
            // 持久化训练会绑定冻结训练批次字典版本，检测阶段可在结构一致时映射到模型字典版本。
            LOGGER.warn("模型字典版本与运行时字典版本不同，后续预测需完成结构校验和版本映射，"
                            + "columnName={}，modelDictionaryVersion={}，runtimeDictionaryVersion={}",
                    metadata.getColumnName(), metadata.getFeatureDictionaryVersion(),
                    featureDictionaryVersion);
        }
        if (!metadata.getStrategyPlanVersion().equals(strategyPlanVersion)) {
            // 同一数据在不同阶段重建策略时，配置摘要可能随画像统计值重新计算而变化。
            LOGGER.warn("模型策略计划版本与运行时策略计划版本不同，后续预测需完成特征维度校验，"
                            + "columnName={}，modelPlanVersion={}，runtimePlanVersion={}",
                    metadata.getColumnName(), metadata.getStrategyPlanVersion(),
                    strategyPlanVersion);
        }
    }
}
