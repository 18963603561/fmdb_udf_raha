package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.RowIdentityMode;
import com.fiberhome.ml.raha.detect.DetectionBatch;
import com.fiberhome.ml.raha.detect.DetectionStore;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.support.JsonUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * 检测批次和单元格结果的 ORC 适配器。
 */
public final class FmdbDetectionStore implements DetectionStore {

    /** 标准表网关。 */
    private final FmdbTableGateway gateway;

    public FmdbDetectionStore(FmdbTableGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public Optional<DetectionBatch> findBatch(String detectionBatchId) {
        List<Row> rows = gateway.table(RahaTables.DETECTION_BATCH)
                .filter(col("detection_batch_id").equalTo(detectionBatchId))
                .limit(1).collectAsList();
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Row row = rows.get(0);
        return Optional.of(new DetectionBatch(row.getAs("detection_batch_id"),
                row.getAs("request_fingerprint"), row.getAs("dataset_id"),
                row.getAs("snapshot_id"), row.getAs("input_reference"),
                row.getAs("source_type"),
                RowIdentityMode.valueOf((String) row.getAs("row_identity_mode")),
                JsonUtils.parseStringArray(row.getAs("row_key_columns_json")),
                JsonUtils.parseStringArray(row.getAs("target_columns_json")),
                row.getAs("schema_hash"), row.getAs("model_set_version"),
                (Boolean) row.getAs("errors_only"),
                ((Number) row.getAs("input_row_count")).longValue(),
                ((Number) row.getAs("evaluated_cell_count")).longValue(),
                ((Number) row.getAs("detected_cell_count")).longValue(),
                ((Number) row.getAs("created_at")).longValue()));
    }

    @Override
    public long appendResults(DetectionBatch batch, RahaColumnModel model,
                              Dataset<Row> resultRows, String partitionDate) {
        Dataset<Row> frame = resultRows
                .withColumn("detection_batch_id", lit(batch.getDetectionBatchId()))
                .withColumn("dataset_id", lit(batch.getDatasetId()))
                .withColumn("snapshot_id", lit(batch.getSnapshotId()))
                .withColumn("model_set_version", lit(batch.getModelSetVersion()))
                .withColumn("column_name", lit(model.getColumnName()))
                .withColumn("strategy_ids_json",
                        lit(JsonUtils.toJson(Collections.singletonList(
                                "model:" + model.getColumnName()))))
                .withColumn("model_version", lit(model.getModelVersion()))
                .withColumn("created_at", lit(batch.getCreatedAt()))
                .withColumn("partition_date", lit(partitionDate))
                .select("detection_batch_id", "dataset_id", "snapshot_id",
                        "model_set_version", "row_id", "column_name",
                        "duplicate_count", "value_hash", "is_error", "score",
                        "strategy_ids_json", "reason_json", "model_version",
                        "created_at", "partition_date");
        return gateway.appendDistinct(RahaTables.DETECTION_RESULT, frame,
                "detection_batch_id", "row_id", "column_name");
    }

    @Override
    public void saveBatch(DetectionBatch batch) {
        Dataset<Row> frame = gateway.getSparkSession().createDataFrame(
                Collections.singletonList(RowFactory.create(
                        batch.getDetectionBatchId(), batch.getRequestFingerprint(),
                        batch.getDatasetId(), batch.getSnapshotId(),
                        batch.getInputReference(), batch.getSourceType(),
                        batch.getRowIdentityMode().name(),
                        JsonUtils.toJson(batch.getRowKeyColumns()),
                        JsonUtils.toJson(batch.getTargetColumns()), batch.getSchemaHash(),
                        batch.getModelSetVersion(), batch.isErrorsOnly(),
                        batch.getInputRowCount(), batch.getEvaluatedCellCount(),
                        batch.getDetectedCellCount(), batch.getCreatedAt())), schema());
        gateway.appendDistinct(RahaTables.DETECTION_BATCH, frame,
                "detection_batch_id");
    }

    private static StructType schema() {
        return new StructType()
                .add("detection_batch_id", DataTypes.StringType, false)
                .add("request_fingerprint", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("snapshot_id", DataTypes.StringType, false)
                .add("input_reference", DataTypes.StringType, false)
                .add("source_type", DataTypes.StringType, false)
                .add("row_identity_mode", DataTypes.StringType, false)
                .add("row_key_columns_json", DataTypes.StringType, true)
                .add("target_columns_json", DataTypes.StringType, false)
                .add("schema_hash", DataTypes.StringType, false)
                .add("model_set_version", DataTypes.StringType, false)
                .add("errors_only", DataTypes.BooleanType, false)
                .add("input_row_count", DataTypes.LongType, false)
                .add("evaluated_cell_count", DataTypes.LongType, false)
                .add("detected_cell_count", DataTypes.LongType, false)
                .add("created_at", DataTypes.LongType, false);
    }
}
