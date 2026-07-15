package com.fiberhome.ml.raha.config;

import com.fiberhome.ml.raha.data.JobType;

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
    /** 稳定唯一的行标识字段。 */
    private final String rowIdColumn;
    /** 是否保留阶段中间结果。 */
    private final boolean saveIntermediate;
    /** 聚类、采样和模型使用的随机种子。 */
    private final long randomSeed;
    /** 任务结果保留天数。 */
    private final int resultRetentionDays;
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

    public RahaJobConfig(JobType jobType,
                         String datasetId,
                         String snapshotId,
                         String inputReference,
                         String rowIdColumn,
                         boolean saveIntermediate,
                         long randomSeed,
                         int resultRetentionDays,
                         StrategyConfig strategyConfig,
                         FeatureConfig featureConfig,
                         ModelConfig modelConfig,
                         ResourceConfig resourceConfig,
                         FailureToleranceConfig failureToleranceConfig) {
        this(jobType, datasetId, snapshotId, inputReference, rowIdColumn,
                saveIntermediate, randomSeed, resultRetentionDays,
                strategyConfig, featureConfig, modelConfig,
                ClusteringConfig.defaults(), SamplingConfig.defaults(),
                resourceConfig, failureToleranceConfig);
    }

    public RahaJobConfig(JobType jobType,
                         String datasetId,
                         String snapshotId,
                         String inputReference,
                         String rowIdColumn,
                         boolean saveIntermediate,
                         long randomSeed,
                         int resultRetentionDays,
                         StrategyConfig strategyConfig,
                         FeatureConfig featureConfig,
                         ModelConfig modelConfig,
                         ClusteringConfig clusteringConfig,
                         SamplingConfig samplingConfig,
                         ResourceConfig resourceConfig,
                         FailureToleranceConfig failureToleranceConfig) {
        this.jobType = jobType;
        this.datasetId = datasetId;
        this.snapshotId = snapshotId;
        this.inputReference = inputReference;
        this.rowIdColumn = rowIdColumn;
        this.saveIntermediate = saveIntermediate;
        this.randomSeed = randomSeed;
        this.resultRetentionDays = resultRetentionDays;
        this.strategyConfig = strategyConfig;
        this.featureConfig = featureConfig;
        this.modelConfig = modelConfig;
        this.clusteringConfig = clusteringConfig;
        this.samplingConfig = samplingConfig;
        this.resourceConfig = resourceConfig;
        this.failureToleranceConfig = failureToleranceConfig;
    }

    /**
     * 创建包含首期推荐值的任务配置。
     *
     * @param jobType 任务类型
     * @param datasetId 数据集标识
     * @param inputReference 输入数据引用
     * @param rowIdColumn 行标识字段
     * @return 默认任务配置
     */
    public static RahaJobConfig defaults(JobType jobType,
                                         String datasetId,
                                         String inputReference,
                                         String rowIdColumn) {
        return new RahaJobConfig(jobType, datasetId, null, inputReference, rowIdColumn,
                false, 20260714L, 30, StrategyConfig.defaults(), FeatureConfig.defaults(),
                ModelConfig.defaults(), ResourceConfig.defaults(), FailureToleranceConfig.defaults());
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

    public String getRowIdColumn() {
        return rowIdColumn;
    }

    public boolean isSaveIntermediate() {
        return saveIntermediate;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    public int getResultRetentionDays() {
        return resultRetentionDays;
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

    String toCanonicalString() {
        return ConfigTextUtils.token(jobType)
                + ConfigTextUtils.token(datasetId)
                + ConfigTextUtils.token(snapshotId)
                + ConfigTextUtils.token(inputReference)
                + ConfigTextUtils.token(rowIdColumn)
                + ConfigTextUtils.token(saveIntermediate)
                + ConfigTextUtils.token(randomSeed)
                + ConfigTextUtils.token(resultRetentionDays)
                + ConfigTextUtils.token(strategyConfig == null ? null : strategyConfig.toCanonicalString())
                + ConfigTextUtils.token(featureConfig == null ? null : featureConfig.toCanonicalString())
                + ConfigTextUtils.token(modelConfig == null ? null : modelConfig.toCanonicalString())
                + ConfigTextUtils.token(clusteringConfig == null
                ? null : clusteringConfig.toCanonicalString())
                + ConfigTextUtils.token(samplingConfig == null
                ? null : samplingConfig.toCanonicalString())
                + ConfigTextUtils.token(resourceConfig == null ? null : resourceConfig.toCanonicalString())
                + ConfigTextUtils.token(failureToleranceConfig == null
                ? null : failureToleranceConfig.toCanonicalString());
    }
}
