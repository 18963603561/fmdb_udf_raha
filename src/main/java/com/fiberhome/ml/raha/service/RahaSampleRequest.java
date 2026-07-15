package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.config.ClusteringConfig;
import com.fiberhome.ml.raha.config.SamplingConfig;
import com.fiberhome.ml.raha.config.ResourceConfig;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存聚类和主动采样服务输入。
 */
public final class RahaSampleRequest {

    /** 采样任务标识。 */
    private final String jobId;
    /** 当前单元格特征。 */
    private final FeatureAssemblyResult features;
    /** 已有直接和传播标签。 */
    private final List<CellLabel> labels;
    /** 列内聚类配置。 */
    private final ClusteringConfig clusteringConfig;
    /** 主动采样配置。 */
    private final SamplingConfig samplingConfig;
    /** 当前采样轮次。 */
    private final int samplingRound;
    /** 聚类和采样随机种子。 */
    private final long randomSeed;
    /** 采样服务仓储业务版本。 */
    private final ArtifactVersion artifactVersion;
    /** 列并发和阶段超时资源配置。 */
    private final ResourceConfig resourceConfig;

    public RahaSampleRequest(String jobId,
                             FeatureAssemblyResult features,
                             List<CellLabel> labels,
                             ClusteringConfig clusteringConfig,
                             SamplingConfig samplingConfig,
                             int samplingRound,
                             long randomSeed,
                             ArtifactVersion artifactVersion) {
        this(jobId, features, labels, clusteringConfig, samplingConfig,
                samplingRound, randomSeed, artifactVersion, ResourceConfig.defaults());
    }

    public RahaSampleRequest(String jobId,
                             FeatureAssemblyResult features,
                             List<CellLabel> labels,
                             ClusteringConfig clusteringConfig,
                             SamplingConfig samplingConfig,
                             int samplingRound,
                             long randomSeed,
                             ArtifactVersion artifactVersion,
                             ResourceConfig resourceConfig) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "采样任务标识");
        if (features == null || labels == null || clusteringConfig == null
                || samplingConfig == null || samplingRound <= 0
                || artifactVersion == null || resourceConfig == null) {
            throw new IllegalArgumentException("采样服务输入、轮次和版本必须有效");
        }
        this.features = features;
        this.labels = Collections.unmodifiableList(new ArrayList<CellLabel>(labels));
        this.clusteringConfig = clusteringConfig;
        this.samplingConfig = samplingConfig;
        this.samplingRound = samplingRound;
        this.randomSeed = randomSeed;
        this.artifactVersion = artifactVersion;
        this.resourceConfig = resourceConfig;
    }

    public String getJobId() { return jobId; }
    public FeatureAssemblyResult getFeatures() { return features; }
    public List<CellLabel> getLabels() { return labels; }
    public ClusteringConfig getClusteringConfig() { return clusteringConfig; }
    public SamplingConfig getSamplingConfig() { return samplingConfig; }
    public int getSamplingRound() { return samplingRound; }
    public long getRandomSeed() { return randomSeed; }
    public ArtifactVersion getArtifactVersion() { return artifactVersion; }
    public ResourceConfig getResourceConfig() { return resourceConfig; }
}
