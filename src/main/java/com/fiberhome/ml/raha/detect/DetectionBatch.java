package com.fiberhome.ml.raha.detect;

import com.fiberhome.ml.raha.data.RowIdentityMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 已提交检测批次头。
 */
public final class DetectionBatch {

    /** 检测批次标识。 */
    private final String detectionBatchId;
    /** 完整请求指纹。 */
    private final String requestFingerprint;
    /** 数据集标识。 */
    private final String datasetId;
    /** 检测快照。 */
    private final String snapshotId;
    /** 输入引用。 */
    private final String inputReference;
    /** 来源类型。 */
    private final String sourceType;
    /** 行身份模式。 */
    private final RowIdentityMode rowIdentityMode;
    /** 行键字段。 */
    private final List<String> rowKeyColumns;
    /** 实际检测字段。 */
    private final List<String> targetColumns;
    /** 模式哈希。 */
    private final String schemaHash;
    /** 模型集合版本。 */
    private final String modelSetVersion;
    /** 是否只保存错误。 */
    private final boolean errorsOnly;
    /** 输入行数。 */
    private final long inputRowCount;
    /** 评估单元格数。 */
    private final long evaluatedCellCount;
    /** 疑似错误单元格数。 */
    private final long detectedCellCount;
    /** 提交时间。 */
    private final long createdAt;

    public DetectionBatch(String detectionBatchId, String requestFingerprint,
                          String datasetId, String snapshotId, String inputReference,
                          String sourceType, RowIdentityMode rowIdentityMode,
                          List<String> rowKeyColumns, List<String> targetColumns,
                          String schemaHash, String modelSetVersion, boolean errorsOnly,
                          long inputRowCount, long evaluatedCellCount,
                          long detectedCellCount, long createdAt) {
        this.detectionBatchId = detectionBatchId;
        this.requestFingerprint = requestFingerprint;
        this.datasetId = datasetId;
        this.snapshotId = snapshotId;
        this.inputReference = inputReference;
        this.sourceType = sourceType;
        this.rowIdentityMode = rowIdentityMode;
        this.rowKeyColumns = immutable(rowKeyColumns);
        this.targetColumns = immutable(targetColumns);
        this.schemaHash = schemaHash;
        this.modelSetVersion = modelSetVersion;
        this.errorsOnly = errorsOnly;
        this.inputRowCount = inputRowCount;
        this.evaluatedCellCount = evaluatedCellCount;
        this.detectedCellCount = detectedCellCount;
        this.createdAt = createdAt;
    }

    private static List<String> immutable(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<String>(values));
    }

    public String getDetectionBatchId() { return detectionBatchId; }
    public String getRequestFingerprint() { return requestFingerprint; }
    public String getDatasetId() { return datasetId; }
    public String getSnapshotId() { return snapshotId; }
    public String getInputReference() { return inputReference; }
    public String getSourceType() { return sourceType; }
    public RowIdentityMode getRowIdentityMode() { return rowIdentityMode; }
    public List<String> getRowKeyColumns() { return rowKeyColumns; }
    public List<String> getTargetColumns() { return targetColumns; }
    public String getSchemaHash() { return schemaHash; }
    public String getModelSetVersion() { return modelSetVersion; }
    public boolean isErrorsOnly() { return errorsOnly; }
    public long getInputRowCount() { return inputRowCount; }
    public long getEvaluatedCellCount() { return evaluatedCellCount; }
    public long getDetectedCellCount() { return detectedCellCount; }
    public long getCreatedAt() { return createdAt; }
}
