package com.fiberhome.ml.raha.data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 算法层只读数据集，包含解析后的身份、快照、模式和目标字段契约。
 */
public final class RahaDataset {

    /** 内部行标识字段。 */
    public static final String ROW_ID = "__raha_row_id";
    /** 内容组原始行数量字段。 */
    public static final String DUPLICATE_COUNT = "__raha_duplicate_count";
    /** 采样和标注使用的完整行 JSON 字段。 */
    public static final String ROW_JSON = "__raha_row_json";

    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** 快照标识。 */
    private final String snapshotId;
    /** 输入表、SQL 或文件引用。 */
    private final String inputReference;
    /** 输入来源类型。 */
    private final String sourceType;
    /** 行身份模式。 */
    private final RowIdentityMode rowIdentityMode;
    /** 稳定业务键字段。 */
    private final List<String> rowKeyColumns;
    /** 输入模式哈希。 */
    private final String schemaHash;
    /** 原始业务字段。 */
    private final List<String> columns;
    /** 本次调用目标字段。 */
    private final List<String> targetColumns;
    /** 去重后的算法输入行。 */
    private final Dataset<Row> rows;
    /** 原始输入行数。 */
    private final long inputRowCount;

    public RahaDataset(String datasetId,
                       String snapshotId,
                       String inputReference,
                       String sourceType,
                       RowIdentityMode rowIdentityMode,
                       List<String> rowKeyColumns,
                       String schemaHash,
                       List<String> columns,
                       List<String> targetColumns,
                       Dataset<Row> rows,
                       long inputRowCount) {
        this.datasetId = datasetId;
        this.snapshotId = snapshotId;
        this.inputReference = inputReference;
        this.sourceType = sourceType;
        this.rowIdentityMode = rowIdentityMode;
        this.rowKeyColumns = immutable(rowKeyColumns);
        this.schemaHash = schemaHash;
        this.columns = immutable(columns);
        this.targetColumns = immutable(targetColumns);
        this.rows = rows;
        this.inputRowCount = inputRowCount;
    }

    private static List<String> immutable(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<String>(values));
    }

    public String getDatasetId() { return datasetId; }
    public String getSnapshotId() { return snapshotId; }
    public String getInputReference() { return inputReference; }
    public String getSourceType() { return sourceType; }
    public RowIdentityMode getRowIdentityMode() { return rowIdentityMode; }
    public List<String> getRowKeyColumns() { return rowKeyColumns; }
    public String getSchemaHash() { return schemaHash; }
    public List<String> getColumns() { return columns; }
    public List<String> getTargetColumns() { return targetColumns; }
    public Dataset<Row> getRows() { return rows; }
    public long getInputRowCount() { return inputRowCount; }
}
