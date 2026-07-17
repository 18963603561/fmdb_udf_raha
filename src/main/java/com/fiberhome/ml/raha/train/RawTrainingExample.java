package com.fiberhome.ml.raha.train;

/**
 * 特征字典建立前的原始训练样本。
 */
public final class RawTrainingExample {

    /** 来源采样批次。 */
    private final String sampleBatchId;
    /** 快照标识。 */
    private final String snapshotId;
    /** 行标识。 */
    private final String rowId;
    /** 字段名。 */
    private final String columnName;
    /** 原始文本值。 */
    private final String value;
    /** 内容组原始行数。 */
    private final long duplicateCount;
    /** 零正常一错误。 */
    private final int label;
    /** 直接或传播。 */
    private final String labelSource;
    /** 样本权重。 */
    private final double sampleWeight;

    public RawTrainingExample(String sampleBatchId, String snapshotId, String rowId,
                              String columnName, String value, long duplicateCount,
                              int label, String labelSource, double sampleWeight) {
        this.sampleBatchId = sampleBatchId;
        this.snapshotId = snapshotId;
        this.rowId = rowId;
        this.columnName = columnName;
        this.value = value;
        this.duplicateCount = duplicateCount;
        this.label = label;
        this.labelSource = labelSource;
        this.sampleWeight = sampleWeight;
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public String getSnapshotId() { return snapshotId; }
    public String getRowId() { return rowId; }
    public String getColumnName() { return columnName; }
    public String getValue() { return value; }
    public long getDuplicateCount() { return duplicateCount; }
    public int getLabel() { return label; }
    public String getLabelSource() { return labelSource; }
    public double getSampleWeight() { return sampleWeight; }
}
