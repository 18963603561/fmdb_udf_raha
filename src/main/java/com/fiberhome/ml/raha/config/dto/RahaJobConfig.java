package com.fiberhome.ml.raha.config.dto;

import com.fiberhome.ml.raha.config.core.ConfigTextUtils;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;

/**
 * Raha 任务完整配置，是任务提交、幂等计算和后续阶段执行的统一输入。
 */
public final class RahaJobConfig {

    /** 任务运行模式。 */
    private final JobType jobType;
    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** 可选输入快照标识，未提供时由数据读取阶段生成。 */
    private final String snapshotId;
    /** 输入表、SQL 或文件等数据引用。 */
    private final String inputReference;
    /** 输入业务键或全字段内容哈希行身份配置。 */
    private final RowIdentityConfig rowIdentityConfig;
    /** 聚类、采样和模型使用的随机种子。 */
    private final long randomSeed;
    /** 策略生成和执行配置。 */
    private final StrategyConfig strategyConfig;
    /** 特征生成配置。 */
    private final FeatureConfig featureConfig;
    /** 列级模型配置。 */
    private final ModelConfig modelConfig;
    /** 列内聚类配置。 */
    private final ClusteringConfig clusteringConfig;
    /** 主动采样和标注任务配置。 */
    private final SamplingConfig samplingConfig;
    /** Spark 资源配置。 */
    private final ResourceConfig resourceConfig;
    /** 失败容忍和重试配置。 */
    private final FailureToleranceConfig failureToleranceConfig;
    /** 影响执行语义的资源配置指纹。 */
    private final String executionConfigFingerprint;
    /** 调用级批次、模型集合或其他执行输入指纹。 */
    private final String executionInputFingerprint;

    public RahaJobConfig(JobType jobType,
                         String datasetId,
                         String snapshotId,
                         String inputReference,
                         RowIdentityConfig rowIdentityConfig,
                         long randomSeed,
                         StrategyConfig strategyConfig,
                         FeatureConfig featureConfig,
                         ModelConfig modelConfig,
                         ResourceConfig resourceConfig,
                         FailureToleranceConfig failureToleranceConfig) {
        this(jobType, datasetId, snapshotId, inputReference, rowIdentityConfig,
                randomSeed,
                strategyConfig, featureConfig, modelConfig,
                ClusteringConfig.defaults(), SamplingConfig.defaults(),
                resourceConfig, failureToleranceConfig);
    }

    public RahaJobConfig(JobType jobType,
                         String datasetId,
                         String snapshotId,
                         String inputReference,
                         RowIdentityConfig rowIdentityConfig,
                         long randomSeed,
                         StrategyConfig strategyConfig,
                         FeatureConfig featureConfig,
                         ModelConfig modelConfig,
                         ClusteringConfig clusteringConfig,
                         SamplingConfig samplingConfig,
                         ResourceConfig resourceConfig,
                         FailureToleranceConfig failureToleranceConfig) {
        this(jobType, datasetId, snapshotId, inputReference, rowIdentityConfig,
                randomSeed,
                strategyConfig, featureConfig, modelConfig, clusteringConfig,
                samplingConfig, resourceConfig, failureToleranceConfig,
                RahaDefaultConfigProvider.factory().executionFingerprint());
    }

    public RahaJobConfig(JobType jobType,
                         String datasetId,
                         String snapshotId,
                         String inputReference,
                         RowIdentityConfig rowIdentityConfig,
                         long randomSeed,
                         StrategyConfig strategyConfig,
                         FeatureConfig featureConfig,
                         ModelConfig modelConfig,
                         ClusteringConfig clusteringConfig,
                         SamplingConfig samplingConfig,
                         ResourceConfig resourceConfig,
                         FailureToleranceConfig failureToleranceConfig,
                         String executionConfigFingerprint) {
        this(jobType, datasetId, snapshotId, inputReference, rowIdentityConfig,
                randomSeed, strategyConfig, featureConfig, modelConfig,
                clusteringConfig, samplingConfig, resourceConfig,
                failureToleranceConfig, executionConfigFingerprint, null);
    }

    public RahaJobConfig(JobType jobType,
                         String datasetId,
                         String snapshotId,
                         String inputReference,
                         RowIdentityConfig rowIdentityConfig,
                         long randomSeed,
                         StrategyConfig strategyConfig,
                         FeatureConfig featureConfig,
                         ModelConfig modelConfig,
                         ClusteringConfig clusteringConfig,
                         SamplingConfig samplingConfig,
                         ResourceConfig resourceConfig,
                         FailureToleranceConfig failureToleranceConfig,
                         String executionConfigFingerprint,
                         String executionInputFingerprint) {
        this.jobType = jobType;
        this.datasetId = datasetId;
        this.snapshotId = snapshotId;
        this.inputReference = inputReference;
        this.rowIdentityConfig = rowIdentityConfig;
        this.randomSeed = randomSeed;
        this.strategyConfig = strategyConfig;
        this.featureConfig = featureConfig;
        this.modelConfig = modelConfig;
        this.clusteringConfig = clusteringConfig;
        this.samplingConfig = samplingConfig;
        this.resourceConfig = resourceConfig;
        this.failureToleranceConfig = failureToleranceConfig;
        this.executionConfigFingerprint = executionConfigFingerprint;
        this.executionInputFingerprint = executionInputFingerprint;
    }

    /**
     * 创建包含首期推荐值的任务配置。
     *
     * @param jobType 任务类型
     * @param datasetId 数据集标识
     * @param inputReference 输入数据引用
     * @param rowIdentityConfig 行身份配置
     * @return 默认任务配置
     */
    public static RahaJobConfig defaults(JobType jobType,
                                         String datasetId,
                                         String inputReference,
                                         RowIdentityConfig rowIdentityConfig) {
        return RahaDefaultConfigProvider.factory().jobConfig(
                jobType, datasetId, inputReference, rowIdentityConfig);
    }

    public JobType getJobType() {
        return jobType;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getInputReference() {
        return inputReference;
    }

    public RowIdentityConfig getRowIdentityConfig() {
        return rowIdentityConfig;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    public StrategyConfig getStrategyConfig() {
        return strategyConfig;
    }

    public FeatureConfig getFeatureConfig() {
        return featureConfig;
    }

    public ModelConfig getModelConfig() {
        return modelConfig;
    }

    public ClusteringConfig getClusteringConfig() {
        return clusteringConfig;
    }

    public SamplingConfig getSamplingConfig() {
        return samplingConfig;
    }

    public ResourceConfig getResourceConfig() {
        return resourceConfig;
    }

    public FailureToleranceConfig getFailureToleranceConfig() {
        return failureToleranceConfig;
    }

    public String getExecutionConfigFingerprint() {
        return executionConfigFingerprint;
    }

    public String getExecutionInputFingerprint() {
        return executionInputFingerprint;
    }

    /**
     * 创建只覆盖快照标识的任务配置副本。
     *
     * @param newSnapshotId 可选平台快照标识
     * @return 保留其他配置的新对象
     */
    public RahaJobConfig withSnapshotId(String newSnapshotId) {
        return copy(newSnapshotId, samplingConfig, executionInputFingerprint);
    }

    /**
     * 创建只覆盖采样参数的任务配置副本。
     *
     * @param newSamplingConfig 新采样配置
     * @return 保留其他配置的新对象
     */
    public RahaJobConfig withSamplingConfig(SamplingConfig newSamplingConfig) {
        if (newSamplingConfig == null) {
            throw new IllegalArgumentException("任务采样配置不能为空");
        }
        return copy(snapshotId, newSamplingConfig, executionInputFingerprint);
    }

    /**
     * 创建包含调用级执行输入指纹的任务配置副本。
     *
     * <p>该指纹用于区分同一输入快照下的不同训练批次或模型集合，必须进入配置版本和任务幂等语义。</p>
     *
     * @param fingerprint 调用级稳定指纹
     * @return 保留其他配置的新对象
     */
    public RahaJobConfig withExecutionInputFingerprint(String fingerprint) {
        if (fingerprint == null || fingerprint.trim().isEmpty()) {
            throw new IllegalArgumentException("执行输入指纹不能为空");
        }
        return copy(snapshotId, samplingConfig, fingerprint.trim());
    }

    /**
     * 生成用于配置版本计算的规范文本。
     *
     * <p>该文本只用于稳定摘要和版本比较，不应作为外部展示格式或持久化协议。</p>
     *
     * @return 包含全部执行配置的稳定规范文本
     */
    public String toCanonicalString() {
        return ConfigTextUtils.token(jobType)
                + ConfigTextUtils.token(datasetId)
                + ConfigTextUtils.token(snapshotId)
                + ConfigTextUtils.token(inputReference)
                + ConfigTextUtils.token(rowIdentityConfig == null
                ? null : rowIdentityConfig.toCanonicalString())
                + ConfigTextUtils.token(randomSeed)
                + ConfigTextUtils.token(strategyConfig == null ? null : strategyConfig.toCanonicalString())
                + ConfigTextUtils.token(featureConfig == null ? null : featureConfig.toCanonicalString())
                + ConfigTextUtils.token(modelConfig == null ? null : modelConfig.toCanonicalString())
                + ConfigTextUtils.token(clusteringConfig == null
                ? null : clusteringConfig.toCanonicalString())
                + ConfigTextUtils.token(samplingConfig == null
                ? null : samplingConfig.toCanonicalString())
                + ConfigTextUtils.token(resourceConfig == null ? null : resourceConfig.toCanonicalString())
                + ConfigTextUtils.token(failureToleranceConfig == null
                ? null : failureToleranceConfig.toCanonicalString())
                + ConfigTextUtils.token(executionConfigFingerprint)
                + ConfigTextUtils.token(executionInputFingerprint);
    }

    private RahaJobConfig copy(String newSnapshotId,
                               SamplingConfig newSamplingConfig,
                               String newExecutionInputFingerprint) {
        return new RahaJobConfig(jobType, datasetId, newSnapshotId,
                inputReference, rowIdentityConfig, randomSeed, strategyConfig,
                featureConfig, modelConfig, clusteringConfig, newSamplingConfig,
                resourceConfig, failureToleranceConfig,
                executionConfigFingerprint, newExecutionInputFingerprint);
    }
}
