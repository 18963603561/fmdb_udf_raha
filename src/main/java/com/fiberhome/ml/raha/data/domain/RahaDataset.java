package com.fiberhome.ml.raha.data.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Raha 检测核心使用的数据集对象，输入 Spark 数据集在整个任务中只读。
 */
public final class RahaDataset {

    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** 当前输入快照标识。 */
    private final String snapshotId;
    /** 来源表或逻辑表名称。 */
    private final String tableName;
    /** 稳定唯一的行标识字段。 */
    private final String rowIdColumn;
    /** 输入字段元数据，顺序与数据集模式一致。 */
    private final List<ColumnMetadata> columns;
    /** 只读 Spark 数据集，元数据准备阶段允许暂时为空。 */
    private final Dataset<Row> dataFrame;
    /** 输入模式的稳定哈希。 */
    private final String schemaHash;
    /** 按字段名称索引的列画像。 */
    private final Map<String, ColumnProfile> profiles;

    public RahaDataset(String datasetId,
                       String snapshotId,
                       String tableName,
                       String rowIdColumn,
                       List<ColumnMetadata> columns,
                       Dataset<Row> dataFrame,
                       String schemaHash,
                       Map<String, ColumnProfile> profiles) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.snapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        this.tableName = ValueUtils.requireNotBlank(tableName, "表名");
        this.rowIdColumn = ValueUtils.requireNotBlank(rowIdColumn, "行标识字段");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("数据集字段不能为空");
        }
        this.columns = Collections.unmodifiableList(new ArrayList<ColumnMetadata>(columns));
        this.dataFrame = dataFrame;
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "模式哈希");
        this.profiles = profiles == null
                ? Collections.<String, ColumnProfile>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, ColumnProfile>(profiles));
    }

    /**
     * 返回绑定 Spark 数据集的新对象，不修改当前对象。
     *
     * @param newDataFrame 只读 Spark 数据集
     * @return 绑定数据的新对象
     */
    public RahaDataset withDataFrame(Dataset<Row> newDataFrame) {
        if (newDataFrame == null) {
            throw new IllegalArgumentException("Spark 数据集不能为空");
        }
        return new RahaDataset(datasetId, snapshotId, tableName, rowIdColumn,
                columns, newDataFrame, schemaHash, profiles);
    }

    /**
     * 绑定新的训练快照和数据，不改变原数据集对象。
     */
    public RahaDataset withSnapshot(String newSnapshotId,
                                    Dataset<Row> newDataFrame) {
        return new RahaDataset(datasetId, newSnapshotId, tableName, rowIdColumn,
                columns, newDataFrame, schemaHash, profiles);
    }

    /**
     * 返回绑定列画像的新对象，不修改当前数据集和输入数据。
     *
     * @param newProfiles 按字段名称索引的列画像
     * @return 绑定画像的新对象
     */
    public RahaDataset withProfiles(Map<String, ColumnProfile> newProfiles) {
        if (newProfiles == null) {
            throw new IllegalArgumentException("列画像不能为空");
        }
        return new RahaDataset(datasetId, snapshotId, tableName, rowIdColumn,
                columns, dataFrame, schemaHash, newProfiles);
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRowIdColumn() {
        return rowIdColumn;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public Dataset<Row> getDataFrame() {
        return dataFrame;
    }

    public String getSchemaHash() {
        return schemaHash;
    }

    public Map<String, ColumnProfile> getProfiles() {
        return profiles;
    }
}
