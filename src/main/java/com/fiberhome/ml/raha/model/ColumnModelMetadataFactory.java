package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ModelStatus;

import java.time.Clock;

/**
 * 根据训练请求、模型参数和文件路径创建草稿模型元数据。
 */
public final class ColumnModelMetadataFactory {

    /** 提供可测试模型创建时间的时钟。 */
    private final Clock clock;

    public ColumnModelMetadataFactory(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("模型元数据工厂时钟不能为空");
        }
        this.clock = clock;
    }

    public RahaColumnModel create(ColumnModelTrainingRequest request,
                                  ColumnModelTrainingResult trainingResult,
                                  String modelPath) {
        if (request == null || trainingResult == null
                || trainingResult.getStatus() != ColumnModelTrainingStatus.TRAINED
                || trainingResult.getArtifact() == null) {
            throw new IllegalArgumentException("只有训练成功结果可以创建模型元数据");
        }
        ColumnModelArtifact artifact = trainingResult.getArtifact();
        return new RahaColumnModel(artifact.getModelName(), artifact.getModelVersion(),
                request.getDatasetId(), artifact.getColumnName(), request.getSchemaHash(),
                artifact.getClassifierType(), artifact.getFeatureDictionaryVersion(),
                request.getStrategyPlanVersion(), artifact.getThreshold(), modelPath,
                ModelStatus.DRAFT, trainingResult.getMetrics(), clock.millis());
    }
}
