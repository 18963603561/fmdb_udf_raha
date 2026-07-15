package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 绑定单列训练数据、模式、策略、模型和优化配置。
 */
public final class ColumnModelTrainingRequest {

    /** 模型逻辑名称。 */
    private final String modelName;
    /** 训练数据集标识。 */
    private final String datasetId;
    /** 训练输入模式哈希。 */
    private final String schemaHash;
    /** 训练策略计划版本。 */
    private final String strategyPlanVersion;
    /** 单列训练数据。 */
    private final ColumnTrainingDataset dataset;
    /** 分类器、阈值和降级配置。 */
    private final ModelConfig modelConfig;
    /** 逻辑回归优化配置。 */
    private final LogisticRegressionTrainingConfig trainingConfig;

    public ColumnModelTrainingRequest(String modelName,
                                      String datasetId,
                                      String schemaHash,
                                      String strategyPlanVersion,
                                      ColumnTrainingDataset dataset,
                                      ModelConfig modelConfig,
                                      LogisticRegressionTrainingConfig trainingConfig) {
        this.modelName = ValueUtils.requireNotBlank(modelName, "模型名称");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "模式哈希");
        this.strategyPlanVersion = ValueUtils.requireNotBlank(
                strategyPlanVersion, "策略计划版本");
        if (dataset == null || modelConfig == null || trainingConfig == null) {
            throw new IllegalArgumentException("训练数据、模型配置和优化配置不能为空");
        }
        this.dataset = dataset;
        this.modelConfig = modelConfig;
        this.trainingConfig = trainingConfig;
    }

    public String getModelName() { return modelName; }
    public String getDatasetId() { return datasetId; }
    public String getSchemaHash() { return schemaHash; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public ColumnTrainingDataset getDataset() { return dataset; }
    public ModelConfig getModelConfig() { return modelConfig; }
    public LogisticRegressionTrainingConfig getTrainingConfig() { return trainingConfig; }
}
