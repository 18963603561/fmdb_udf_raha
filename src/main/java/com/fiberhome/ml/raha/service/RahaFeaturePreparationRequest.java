package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.config.RahaJobConfig;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存策略和特征准备阶段所需的不可变输入。
 */
public final class RahaFeaturePreparationRequest {

    /** 任务标识。 */
    private final String jobId;
    /** 特征准备阶段标识。 */
    private final String stageId;
    /** 已加载并完成画像的数据集。 */
    private final RahaDataset dataset;
    /** 当前任务完整配置。 */
    private final RahaJobConfig config;
    /** 特征准备产物业务版本。 */
    private final ArtifactVersion artifactVersion;

    public RahaFeaturePreparationRequest(String jobId,
                                         String stageId,
                                         RahaDataset dataset,
                                         RahaJobConfig config,
                                         ArtifactVersion artifactVersion) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "特征准备任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "特征准备阶段标识");
        if (dataset == null || dataset.getDataFrame() == null
                || config == null || artifactVersion == null) {
            throw new IllegalArgumentException("特征准备数据集、配置和版本不能为空");
        }
        if (!dataset.getDatasetId().equals(config.getDatasetId())) {
            throw new IllegalArgumentException("特征准备数据集与任务配置标识不一致");
        }
        this.dataset = dataset;
        this.config = config;
        this.artifactVersion = artifactVersion;
    }

    public String getJobId() { return jobId; }
    public String getStageId() { return stageId; }
    public RahaDataset getDataset() { return dataset; }
    public RahaJobConfig getConfig() { return config; }
    public ArtifactVersion getArtifactVersion() { return artifactVersion; }
}
