package com.fiberhome.ml.raha.config;

/**
 * Raha 轻量化工程不可变配置根对象。
 */
public final class RahaConfig {

    /** 默认标注预算。 */
    private final int defaultLabelingBudget;
    /** 允许的最大标注预算。 */
    private final int maximumLabelingBudget;
    /** 特征字典允许收录的最大离散值数量。 */
    private final int maximumDictionaryValues;
    /** 确定性采样随机种子。 */
    private final long randomSeed;
    /** ORC 存储根目录。 */
    private final String storageRoot;
    /** ORC 分区时区。 */
    private final String partitionTimeZone;
    /** 算法版本。 */
    private final String algorithmVersion;
    /** 逻辑回归配置。 */
    private final ModelConfig modelConfig;

    public RahaConfig(int defaultLabelingBudget,
                      int maximumLabelingBudget,
                      int maximumDictionaryValues,
                      long randomSeed,
                      String storageRoot,
                      String partitionTimeZone,
                      String algorithmVersion,
                      ModelConfig modelConfig) {
        this.defaultLabelingBudget = defaultLabelingBudget;
        this.maximumLabelingBudget = maximumLabelingBudget;
        this.maximumDictionaryValues = maximumDictionaryValues;
        this.randomSeed = randomSeed;
        this.storageRoot = storageRoot;
        this.partitionTimeZone = partitionTimeZone;
        this.algorithmVersion = algorithmVersion;
        this.modelConfig = modelConfig;
    }

    /**
     * 返回首期生产默认配置，系统属性只允许覆盖存储根目录。
     *
     * @return 默认配置
     */
    public static RahaConfig defaults() {
        String root = System.getProperty("raha.storage.root", "/fmdb/raha");
        return new RahaConfig(20, 200, 5000, 20260717L, root,
                "Asia/Shanghai", "raha-1.0.0",
                new ModelConfig(200, 0.01d, 0.5d));
    }

    public int getDefaultLabelingBudget() {
        return defaultLabelingBudget;
    }

    public int getMaximumLabelingBudget() {
        return maximumLabelingBudget;
    }

    public int getMaximumDictionaryValues() {
        return maximumDictionaryValues;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    public String getStorageRoot() {
        return storageRoot;
    }

    public String getPartitionTimeZone() {
        return partitionTimeZone;
    }

    public String getAlgorithmVersion() {
        return algorithmVersion;
    }

    public ModelConfig getModelConfig() {
        return modelConfig;
    }
}
