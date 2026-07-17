package com.fiberhome.ml.raha.data;

/**
 * 外部标注系统写入的直接单元格标签。
 */
public final class CellLabel {

    /** 来源采样批次。 */
    private final String sampleBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 快照标识。 */
    private final String snapshotId;
    /** 行标识。 */
    private final String rowId;
    /** 字段名。 */
    private final String columnName;
    /** 标注时原值摘要。 */
    private final String valueHash;
    /** 零表示正常，一表示错误。 */
    private final int label;
    /** 标注时间。 */
    private final long labeledAt;
    /** ORC 分区日期。 */
    private final String partitionDate;

    public CellLabel(String sampleBatchId, String datasetId, String snapshotId,
                     String rowId, String columnName, String valueHash,
                     int label, long labeledAt, String partitionDate) {
        if (label != 0 && label != 1) {
            throw new IllegalArgumentException("标签只能为零或一");
        }
        this.sampleBatchId = sampleBatchId;
        this.datasetId = datasetId;
        this.snapshotId = snapshotId;
        this.rowId = rowId;
        this.columnName = columnName;
        this.valueHash = valueHash;
        this.label = label;
        this.labeledAt = labeledAt;
        this.partitionDate = partitionDate;
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public String getDatasetId() { return datasetId; }
    public String getSnapshotId() { return snapshotId; }
    public String getRowId() { return rowId; }
    public String getColumnName() { return columnName; }
    public String getValueHash() { return valueHash; }
    public int getLabel() { return label; }
    public long getLabeledAt() { return labeledAt; }
    public String getPartitionDate() { return partitionDate; }
}
