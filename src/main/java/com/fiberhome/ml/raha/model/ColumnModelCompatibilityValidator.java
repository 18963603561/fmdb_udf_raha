package com.fiberhome.ml.raha.model;

/**
 * 校验模型元数据、模型文件和当前输入模式及特征依赖兼容性。
 */
public final class ColumnModelCompatibilityValidator {

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
            throw new IllegalStateException("模型与当前特征字典不兼容");
        }
        if (!metadata.getStrategyPlanVersion().equals(strategyPlanVersion)) {
            throw new IllegalStateException("模型与当前策略计划不兼容");
        }
    }
}
