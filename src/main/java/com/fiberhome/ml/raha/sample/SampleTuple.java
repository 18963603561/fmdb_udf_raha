package com.fiberhome.ml.raha.sample;

/**
 * 已选采样元组，保存标注需要的完整行快照。
 */
public final class SampleTuple {

    /** 采样批次。 */
    private final String sampleBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 快照标识。 */
    private final String snapshotId;
    /** 行标识。 */
    private final String rowId;
    /** 内容组原始行数。 */
    private final long duplicateCount;
    /** 完整行 JSON。 */
    private final String rowDataJson;
    /** 采样顺序。 */
    private final int selectionOrder;
    /** 覆盖评分。 */
    private final double selectionScore;
    /** 结构化选择原因。 */
    private final String reasonJson;
    /** 写入时间。 */
    private final long createdAt;
    /** ORC 分区日期。 */
    private final String partitionDate;

    public SampleTuple(String sampleBatchId, String datasetId, String snapshotId,
                       String rowId, long duplicateCount, String rowDataJson,
                       int selectionOrder, double selectionScore, String reasonJson,
                       long createdAt, String partitionDate) {
        this.sampleBatchId = sampleBatchId;
        this.datasetId = datasetId;
        this.snapshotId = snapshotId;
        this.rowId = rowId;
        this.duplicateCount = duplicateCount;
        this.rowDataJson = rowDataJson;
        this.selectionOrder = selectionOrder;
        this.selectionScore = selectionScore;
        this.reasonJson = reasonJson;
        this.createdAt = createdAt;
        this.partitionDate = partitionDate;
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public String getDatasetId() { return datasetId; }
    public String getSnapshotId() { return snapshotId; }
    public String getRowId() { return rowId; }
    public long getDuplicateCount() { return duplicateCount; }
    public String getRowDataJson() { return rowDataJson; }
    public int getSelectionOrder() { return selectionOrder; }
    public double getSelectionScore() { return selectionScore; }
    public String getReasonJson() { return reasonJson; }
    public long getCreatedAt() { return createdAt; }
    public String getPartitionDate() { return partitionDate; }
}
