package com.fiberhome.ml.raha.data.domain;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一次输入读取对应的来源、模式、规模和快照版本。
 */
public final class DatasetSnapshot {

    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** 输入快照标识。 */
    private final String snapshotId;
    /** 输入表、文件或 SQL 引用。 */
    private final String inputReference;
    /** 来源表或逻辑表名称。 */
    private final String tableName;
    /** 稳定行标识字段。 */
    private final String rowIdColumn;
    /** 输入模式哈希。 */
    private final String schemaHash;
    /** 输入行数。 */
    private final long rowCount;
    /** 输入列数。 */
    private final int columnCount;
    /** 数据源提供的版本或修改时间。 */
    private final String sourceVersion;
    /** 快照创建时间。 */
    private final long createdAt;

    public DatasetSnapshot(String datasetId,
                           String snapshotId,
                           String inputReference,
                           String tableName,
                           String rowIdColumn,
                           String schemaHash,
                           long rowCount,
                           int columnCount,
                           String sourceVersion,
                           long createdAt) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.snapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        this.inputReference = ValueUtils.requireNotBlank(inputReference, "输入数据引用");
        this.tableName = ValueUtils.requireNotBlank(tableName, "表名");
        this.rowIdColumn = ValueUtils.requireNotBlank(rowIdColumn, "行标识字段");
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "模式哈希");
        if (rowCount < 0L || columnCount <= 0 || createdAt <= 0L) {
            throw new IllegalArgumentException("快照行数、列数和创建时间非法");
        }
        this.rowCount = rowCount;
        this.columnCount = columnCount;
        this.sourceVersion = sourceVersion;
        this.createdAt = createdAt;
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

    public String getTableName() {
        return tableName;
    }

    public String getRowIdColumn() {
        return rowIdColumn;
    }

    public String getSchemaHash() {
        return schemaHash;
    }

    public long getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public String getSourceVersion() {
        return sourceVersion;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}

