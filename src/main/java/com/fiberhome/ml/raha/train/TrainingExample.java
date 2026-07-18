package com.fiberhome.ml.raha.train;

/**
 * 模型集合实际使用的不可变训练样本。
 */
public final class TrainingExample {

    /** 模型集合版本。 */
    private final String modelSetVersion;
    /** 数据集标识。 */
    private final String datasetId;
    /** 可选来源采样批次。 */
    private final String sourceSampleBatchId;
    /** 字段名。 */
    private final String columnName;
    /** 快照标识。 */
    private final String snapshotId;
    /** 行标识。 */
    private final String rowId;
    /** 内容组原始行数。 */
    private final long duplicateCount;
    /** 训练时原值摘要。 */
    private final String valueHash;
    /** 稠密特征向量。 */
    private final double[] featureVector;
    /** 零正常一错误。 */
    private final int label;
    /** 直接或传播。 */
    private final String labelSource;
    /** 训练权重。 */
    private final double sampleWeight;
    /** 模型集合创建时间。 */
    private final long createdAt;
    /** ORC 分区日期。 */
    private final String partitionDate;

    public TrainingExample(String modelSetVersion, String datasetId,
                           String sourceSampleBatchId, String columnName,
                           String snapshotId, String rowId, long duplicateCount,
                           String valueHash, double[] featureVector, int label,
                           String labelSource, double sampleWeight, long createdAt,
                           String partitionDate) {
        this.modelSetVersion = modelSetVersion;
        this.datasetId = datasetId;
        this.sourceSampleBatchId = sourceSampleBatchId;
        this.columnName = columnName;
        this.snapshotId = snapshotId;
        this.rowId = rowId;
        this.duplicateCount = duplicateCount;
        this.valueHash = valueHash;
        this.featureVector = featureVector.clone();
        this.label = label;
        this.labelSource = labelSource;
        this.sampleWeight = sampleWeight;
        this.createdAt = createdAt;
        this.partitionDate = partitionDate;
    }

    public String getModelSetVersion() { return modelSetVersion; }
    public String getDatasetId() { return datasetId; }
    public String getSourceSampleBatchId() { return sourceSampleBatchId; }
    public String getColumnName() { return columnName; }
    public String getSnapshotId() { return snapshotId; }
    public String getRowId() { return rowId; }
    public long getDuplicateCount() { return duplicateCount; }
    public String getValueHash() { return valueHash; }
    public double[] getFeatureVector() { return featureVector.clone(); }
    public int getLabel() { return label; }
    public String getLabelSource() { return labelSource; }
    public double getSampleWeight() { return sampleWeight; }
    public long getCreatedAt() { return createdAt; }
    public String getPartitionDate() { return partitionDate; }
}
