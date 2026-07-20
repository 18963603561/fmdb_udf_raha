package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingRequest;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingResult;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingStatus;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;

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
        return create(request, trainingResult, modelPath,
                trainingResult == null || trainingResult.getArtifact() == null
                        ? "unknown" : trainingResult.getArtifact().getModelVersion(),
                RowIdentityConfig.contentHash());
    }

    /**
     * 根据训练结果创建包含模型集合和行身份上下文的模型元数据。
     *
     * @param request 列训练请求
     * @param trainingResult 成功训练结果
     * @param modelPath 模型参数路径
     * @param modelSetVersion 不可变模型集合版本
     * @param rowIdentityConfig 训练输入行身份规则
     * @return 草稿列模型元数据
     */
    public RahaColumnModel create(ColumnModelTrainingRequest request,
                                  ColumnModelTrainingResult trainingResult,
                                  String modelPath,
                                  String modelSetVersion,
                                  RowIdentityConfig rowIdentityConfig) {
        if (request == null || trainingResult == null
                || trainingResult.getStatus() != ColumnModelTrainingStatus.TRAINED
                || trainingResult.getArtifact() == null) {
            throw new IllegalArgumentException("只有训练成功结果可以创建模型元数据");
        }
        ColumnModelArtifact artifact = trainingResult.getArtifact();
        Map<String, Double> metrics = new LinkedHashMap<String, Double>(
                trainingResult.getMetrics());
        metrics.put("fallback", trainingResult.isFallback() ? 1.0d : 0.0d);
        return new RahaColumnModel(artifact.getModelName(), artifact.getModelVersion(),
                request.getDatasetId(), artifact.getColumnName(), request.getSchemaHash(),
                artifact.getClassifierType(), artifact.getFeatureDictionaryVersion(),
                request.getStrategyPlanVersion(), artifact.getThreshold(), modelPath,
                ModelStatus.DRAFT, metrics, clock.millis(), null,
                modelSetVersion, rowIdentityConfig);
    }
}
