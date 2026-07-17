package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.CellLabel;
import com.fiberhome.ml.raha.label.LabelStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * 直接单元格标签的 ORC 适配器。
 */
public final class FmdbLabelStore implements LabelStore {

    /** 标准表网关。 */
    private final FmdbTableGateway gateway;

    public FmdbLabelStore(FmdbTableGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public List<CellLabel> load(List<String> sampleBatchIds) {
        if (sampleBatchIds.isEmpty()) {
            return new ArrayList<CellLabel>();
        }
        List<Row> rows = gateway.table(RahaTables.CELL_LABEL)
                .filter(col("sample_batch_id").isin(sampleBatchIds.toArray()))
                .collectAsList();
        List<CellLabel> labels = new ArrayList<CellLabel>();
        for (Row row : rows) {
            labels.add(new CellLabel(row.getAs("sample_batch_id"),
                    row.getAs("dataset_id"), row.getAs("snapshot_id"),
                    row.getAs("row_id"), row.getAs("column_name"),
                    row.getAs("value_hash"), ((Number) row.getAs("label")).intValue(),
                    ((Number) row.getAs("labeled_at")).longValue(),
                    row.getAs("partition_date")));
        }
        return labels;
    }

    @Override
    public void save(List<CellLabel> labels) {
        if (labels.isEmpty()) {
            return;
        }
        List<Row> rows = new ArrayList<Row>();
        for (CellLabel label : labels) {
            rows.add(RowFactory.create(label.getSampleBatchId(), label.getDatasetId(),
                    label.getSnapshotId(), label.getRowId(), label.getColumnName(),
                    label.getValueHash(), label.getLabel(), label.getLabeledAt(),
                    label.getPartitionDate()));
        }
        Dataset<Row> frame = gateway.getSparkSession().createDataFrame(rows, schema());
        gateway.appendDistinct(RahaTables.CELL_LABEL, frame,
                "sample_batch_id", "row_id", "column_name");
    }

    private static StructType schema() {
        return new StructType()
                .add("sample_batch_id", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("snapshot_id", DataTypes.StringType, false)
                .add("row_id", DataTypes.StringType, false)
                .add("column_name", DataTypes.StringType, false)
                .add("value_hash", DataTypes.StringType, false)
                .add("label", DataTypes.IntegerType, false)
                .add("labeled_at", DataTypes.LongType, false)
                .add("partition_date", DataTypes.StringType, false);
    }
}
