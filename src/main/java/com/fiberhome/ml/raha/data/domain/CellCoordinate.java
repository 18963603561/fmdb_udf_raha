package com.fiberhome.ml.raha.data.domain;

import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Objects;

/**
 * 使用数据集、快照、行和列稳定定位一个单元格。
 */
public final class CellCoordinate {

    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** 输入快照标识。 */
    private final String snapshotId;
    /** 输入表中的稳定行标识。 */
    private final String rowId;
    /** 字段名称。 */
    private final String columnName;

    public CellCoordinate(String datasetId, String snapshotId, String rowId, String columnName) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.snapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        this.rowId = ValueUtils.requireNotBlank(rowId, "行标识");
        this.columnName = ValueUtils.requireNotBlank(columnName, "字段名称");
    }

    /**
     * 生成不依赖 Spark 分区和物理行号的稳定单元格标识。
     *
     * @return 单元格 SHA-256 标识
     */
    public String toCellId() {
        return HashUtils.sha256Hex(datasetId.length() + ":" + datasetId
                + snapshotId.length() + ":" + snapshotId
                + rowId.length() + ":" + rowId
                + columnName.length() + ":" + columnName);
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getRowId() {
        return rowId;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof CellCoordinate)) {
            return false;
        }
        CellCoordinate that = (CellCoordinate) object;
        return datasetId.equals(that.datasetId)
                && snapshotId.equals(that.snapshotId)
                && rowId.equals(that.rowId)
                && columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetId, snapshotId, rowId, columnName);
    }

    @Override
    public String toString() {
        return datasetId + "/" + snapshotId + "/" + rowId + "/" + columnName;
    }
}

