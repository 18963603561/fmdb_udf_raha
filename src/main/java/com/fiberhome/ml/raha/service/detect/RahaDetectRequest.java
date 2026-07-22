package com.fiberhome.ml.raha.service.detect;

import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.service.task.MissingModelPolicy;
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
    /** 调用方显式选择的不可变模型集合版本，旧入口使用当前发布模型时为空。 */
    private final String modelSetVersion;
    /** 字段模型缺失或不兼容时的处理策略。 */
    private final MissingModelPolicy missingModelPolicy;
    /** 父级列批检测共享的检测批次标识，为空时使用当前任务标识。 */
    private final String detectionBatchIdOverride;

    public RahaDetectRequest(String jobId,
                             String stageId,
                             String configVersion,
                             RahaDataset dataset,
                             FeatureAssemblyResult features,
                             String strategyPlanVersion,
                             ArtifactVersion artifactVersion) {
        this(jobId, stageId, configVersion, dataset, features,
                strategyPlanVersion, artifactVersion, ResourceConfig.defaults(),
                null, MissingModelPolicy.PARTIAL, null);
    }

    public RahaDetectRequest(String jobId,
                             String stageId,
                             String configVersion,
                             RahaDataset dataset,
                             FeatureAssemblyResult features,
                             String strategyPlanVersion,
                             ArtifactVersion artifactVersion,
                             ResourceConfig resourceConfig) {
        this(jobId, stageId, configVersion, dataset, features,
                strategyPlanVersion, artifactVersion, resourceConfig,
                null, MissingModelPolicy.PARTIAL, null);
    }

    public RahaDetectRequest(String jobId,
                             String stageId,
                             String configVersion,
                             RahaDataset dataset,
                             FeatureAssemblyResult features,
                             String strategyPlanVersion,
                             ArtifactVersion artifactVersion,
                             ResourceConfig resourceConfig,
                             String modelSetVersion,
                             MissingModelPolicy missingModelPolicy) {
        this(jobId, stageId, configVersion, dataset, features,
                strategyPlanVersion, artifactVersion, resourceConfig,
                modelSetVersion, missingModelPolicy, null);
    }

    public RahaDetectRequest(String jobId,
                             String stageId,
                             String configVersion,
                             RahaDataset dataset,
                             FeatureAssemblyResult features,
                             String strategyPlanVersion,
                             ArtifactVersion artifactVersion,
                             ResourceConfig resourceConfig,
                             String modelSetVersion,
                             MissingModelPolicy missingModelPolicy,
                             String detectionBatchIdOverride) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "检测任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "检测阶段标识");
        this.configVersion = ValueUtils.requireNotBlank(configVersion, "检测配置版本");
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
        this.modelSetVersion = modelSetVersion == null
                || modelSetVersion.trim().isEmpty()
                ? null : modelSetVersion.trim();
        if (missingModelPolicy == null) {
            throw new IllegalArgumentException("检测缺失模型策略不能为空");
        }
        this.missingModelPolicy = missingModelPolicy;
        this.detectionBatchIdOverride = detectionBatchIdOverride == null
                || detectionBatchIdOverride.trim().isEmpty()
                ? null : detectionBatchIdOverride.trim();
    }

    public String getJobId() { return jobId; }
    public String getStageId() { return stageId; }
    public String getConfigVersion() { return configVersion; }
    public RahaDataset getDataset() { return dataset; }
    public FeatureAssemblyResult getFeatures() { return features; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public ArtifactVersion getArtifactVersion() { return artifactVersion; }
    public ResourceConfig getResourceConfig() { return resourceConfig; }
    public String getModelSetVersion() { return modelSetVersion; }
    public MissingModelPolicy getMissingModelPolicy() {
        return missingModelPolicy;
    }
    public String getDetectionBatchId() {
        return detectionBatchIdOverride == null
                ? jobId : detectionBatchIdOverride;
    }
}
