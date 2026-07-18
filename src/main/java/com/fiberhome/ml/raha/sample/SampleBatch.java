package com.fiberhome.ml.raha.sample;

import com.fiberhome.ml.raha.data.RowIdentityMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 已提交采样批次头契约。
 */
public final class SampleBatch {

    /** 采样批次标识。 */
    private final String sampleBatchId;
    /** 完整请求指纹。 */
    private final String requestFingerprint;
    /** 数据集标识。 */
    private final String datasetId;
    /** 输入快照。 */
    private final String snapshotId;
    /** 输入引用。 */
    private final String inputReference;
    /** 来源类型。 */
    private final String sourceType;
    /** 行身份模式。 */
    private final RowIdentityMode rowIdentityMode;
    /** 行键字段。 */
    private final List<String> rowKeyColumns;
    /** 目标字段。 */
    private final List<String> targetColumns;
    /** 模式哈希。 */
    private final String schemaHash;
    /** 算法版本。 */
    private final String algorithmVersion;
    /** 算法配置 JSON。 */
    private final String configJson;
    /** 标注预算。 */
    private final int labelingBudget;
    /** 实际元组数。 */
    private final long selectedTupleCount;
    /** 提交时间。 */
    private final long createdAt;

    public SampleBatch(String sampleBatchId, String requestFingerprint, String datasetId,
                       String snapshotId, String inputReference, String sourceType,
                       RowIdentityMode rowIdentityMode, List<String> rowKeyColumns,
                       List<String> targetColumns, String schemaHash,
                       String algorithmVersion, String configJson, int labelingBudget,
                       long selectedTupleCount, long createdAt) {
        this.sampleBatchId = sampleBatchId;
        this.requestFingerprint = requestFingerprint;
        this.datasetId = datasetId;
        this.snapshotId = snapshotId;
        this.inputReference = inputReference;
        this.sourceType = sourceType;
        this.rowIdentityMode = rowIdentityMode;
        this.rowKeyColumns = immutable(rowKeyColumns);
        this.targetColumns = immutable(targetColumns);
        this.schemaHash = schemaHash;
        this.algorithmVersion = algorithmVersion;
        this.configJson = configJson;
        this.labelingBudget = labelingBudget;
        this.selectedTupleCount = selectedTupleCount;
        this.createdAt = createdAt;
    }

    private static List<String> immutable(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<String>(values));
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public String getRequestFingerprint() { return requestFingerprint; }
    public String getDatasetId() { return datasetId; }
    public String getSnapshotId() { return snapshotId; }
    public String getInputReference() { return inputReference; }
    public String getSourceType() { return sourceType; }
    public RowIdentityMode getRowIdentityMode() { return rowIdentityMode; }
    public List<String> getRowKeyColumns() { return rowKeyColumns; }
    public List<String> getTargetColumns() { return targetColumns; }
    public String getSchemaHash() { return schemaHash; }
    public String getAlgorithmVersion() { return algorithmVersion; }
    public String getConfigJson() { return configJson; }
    public int getLabelingBudget() { return labelingBudget; }
    public long getSelectedTupleCount() { return selectedTupleCount; }
    public long getCreatedAt() { return createdAt; }
}
