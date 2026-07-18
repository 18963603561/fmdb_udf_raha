package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.config.ResourceConfig;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存已发布列级模型批量检测所需输入和版本。
 */
public final class RahaDetectRequest {

    /** 检测任务标识。 */
    private final String jobId;
    /** 检测阶段标识。 */
    private final String stageId;
    /** 当前完整配置版本。 */
    private final String configVersion;
    /** 精确模型版本或按字段选择当前发布模型的固定选择器。 */
    private final String modelVersion;
    /** 当前只读数据集及模式哈希。 */
    private final RahaDataset dataset;
    /** 当前数据的列级特征和字典。 */
    private final FeatureAssemblyResult features;
    /** 当前策略计划版本。 */
    private final String strategyPlanVersion;
    /** 检测结果仓储业务版本。 */
    private final ArtifactVersion artifactVersion;
    /** 列预测并发和阶段超时配置。 */
    private final ResourceConfig resourceConfig;

    public RahaDetectRequest(String jobId,
                             String stageId,
                             String configVersion,
                             String modelVersion,
                             RahaDataset dataset,
                             FeatureAssemblyResult features,
                             String strategyPlanVersion,
                             ArtifactVersion artifactVersion) {
        this(jobId, stageId, configVersion, modelVersion, dataset, features,
                strategyPlanVersion, artifactVersion, ResourceConfig.defaults());
    }

    public RahaDetectRequest(String jobId,
                             String stageId,
                             String configVersion,
                             String modelVersion,
                             RahaDataset dataset,
                             FeatureAssemblyResult features,
                             String strategyPlanVersion,
                             ArtifactVersion artifactVersion,
                             ResourceConfig resourceConfig) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "检测任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "检测阶段标识");
        this.configVersion = ValueUtils.requireNotBlank(configVersion, "检测配置版本");
        this.modelVersion = ValueUtils.requireNotBlank(modelVersion, "检测模型版本");
        this.strategyPlanVersion = ValueUtils.requireNotBlank(
                strategyPlanVersion, "检测策略计划版本");
        if (dataset == null || dataset.getDataFrame() == null
                || features == null || artifactVersion == null
                || resourceConfig == null) {
            throw new IllegalArgumentException("检测数据、特征和版本不能为空");
        }
        this.dataset = dataset;
        this.features = features;
        this.artifactVersion = artifactVersion;
        this.resourceConfig = resourceConfig;
    }

    public String getJobId() { return jobId; }
    public String getStageId() { return stageId; }
    public String getConfigVersion() { return configVersion; }
    public String getModelVersion() { return modelVersion; }
    public RahaDataset getDataset() { return dataset; }
    public FeatureAssemblyResult getFeatures() { return features; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public ArtifactVersion getArtifactVersion() { return artifactVersion; }
    public ResourceConfig getResourceConfig() { return resourceConfig; }
}
