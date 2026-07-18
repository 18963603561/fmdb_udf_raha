package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.RowIdentityMode;
import com.fiberhome.ml.raha.sample.SampleBatch;
import com.fiberhome.ml.raha.sample.SampleStore;
import com.fiberhome.ml.raha.sample.SampleTuple;
import com.fiberhome.ml.raha.support.JsonUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;

/**
 * 采样批次和采样元组的 ORC 适配器。
 */
public final class FmdbSampleStore implements SampleStore {

    /** 标准表网关。 */
    private final FmdbTableGateway gateway;

    public FmdbSampleStore(FmdbTableGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public Optional<SampleBatch> findBatch(String sampleBatchId) {
        List<Row> rows = gateway.table(RahaTables.SAMPLE_BATCH)
                .filter(col("sample_batch_id").equalTo(sampleBatchId)).limit(1).collectAsList();
        return rows.isEmpty() ? Optional.<SampleBatch>empty()
                : Optional.of(toBatch(rows.get(0)));
    }

    @Override
    public List<SampleBatch> loadBatches(List<String> sampleBatchIds) {
        if (sampleBatchIds.isEmpty()) {
            return new ArrayList<SampleBatch>();
        }
        List<Row> rows = gateway.table(RahaTables.SAMPLE_BATCH)
                .filter(col("sample_batch_id").isin(sampleBatchIds.toArray()))
                .collectAsList();
        List<SampleBatch> result = new ArrayList<SampleBatch>();
        for (Row row : rows) {
            result.add(toBatch(row));
        }
        return result;
    }

    @Override
    public List<SampleTuple> loadTuples(List<String> sampleBatchIds) {
        if (sampleBatchIds.isEmpty()) {
            return new ArrayList<SampleTuple>();
        }
        List<Row> rows = gateway.table(RahaTables.SAMPLE_TUPLE)
                .filter(col("sample_batch_id").isin(sampleBatchIds.toArray()))
                .collectAsList();
        List<SampleTuple> result = new ArrayList<SampleTuple>();
        for (Row row : rows) {
            result.add(new SampleTuple(row.getAs("sample_batch_id"),
                    row.getAs("dataset_id"), row.getAs("snapshot_id"),
                    row.getAs("row_id"), ((Number) row.getAs("duplicate_count")).longValue(),
                    row.getAs("row_data_json"),
                    ((Number) row.getAs("selection_order")).intValue(),
                    ((Number) row.getAs("selection_score")).doubleValue(),
                    row.getAs("reason_json"),
                    ((Number) row.getAs("created_at")).longValue(),
                    row.getAs("partition_date")));
        }
        return result;
    }

    @Override
    public void save(SampleBatch batch, List<SampleTuple> tuples) {
        if (findBatch(batch.getSampleBatchId()).isPresent()) {
            return;
        }
        List<Row> tupleRows = new ArrayList<Row>();
        for (SampleTuple tuple : tuples) {
            tupleRows.add(RowFactory.create(tuple.getSampleBatchId(), tuple.getDatasetId(),
                    tuple.getSnapshotId(), tuple.getRowId(), tuple.getDuplicateCount(),
                    tuple.getRowDataJson(), tuple.getSelectionOrder(),
                    tuple.getSelectionScore(), tuple.getReasonJson(), tuple.getCreatedAt(),
                    tuple.getPartitionDate()));
        }
        Dataset<Row> tupleFrame = gateway.getSparkSession()
                .createDataFrame(tupleRows, tupleSchema());
        gateway.appendDistinct(RahaTables.SAMPLE_TUPLE, tupleFrame,
                "sample_batch_id", "row_id");
        Dataset<Row> batchFrame = gateway.getSparkSession().createDataFrame(
                java.util.Collections.singletonList(RowFactory.create(
                        batch.getSampleBatchId(), batch.getRequestFingerprint(),
                        batch.getDatasetId(), batch.getSnapshotId(), batch.getInputReference(),
                        batch.getSourceType(), batch.getRowIdentityMode().name(),
                        JsonUtils.toJson(batch.getRowKeyColumns()),
                        JsonUtils.toJson(batch.getTargetColumns()), batch.getSchemaHash(),
                        batch.getAlgorithmVersion(), batch.getConfigJson(),
                        batch.getLabelingBudget(), batch.getSelectedTupleCount(),
                        batch.getCreatedAt())), batchSchema());
        gateway.appendDistinct(RahaTables.SAMPLE_BATCH, batchFrame, "sample_batch_id");
    }

    private static SampleBatch toBatch(Row row) {
        return new SampleBatch(row.getAs("sample_batch_id"),
                row.getAs("request_fingerprint"), row.getAs("dataset_id"),
                row.getAs("snapshot_id"), row.getAs("input_reference"),
                row.getAs("source_type"),
                RowIdentityMode.valueOf((String) row.getAs("row_identity_mode")),
                JsonUtils.parseStringArray(row.getAs("row_key_columns_json")),
                JsonUtils.parseStringArray(row.getAs("target_columns_json")),
                row.getAs("schema_hash"), row.getAs("algorithm_version"),
                row.getAs("config_json"),
                ((Number) row.getAs("labeling_budget")).intValue(),
                ((Number) row.getAs("selected_tuple_count")).longValue(),
                ((Number) row.getAs("created_at")).longValue());
    }

    private static StructType batchSchema() {
        return new StructType()
                .add("sample_batch_id", DataTypes.StringType, false)
                .add("request_fingerprint", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("snapshot_id", DataTypes.StringType, false)
                .add("input_reference", DataTypes.StringType, false)
                .add("source_type", DataTypes.StringType, false)
                .add("row_identity_mode", DataTypes.StringType, false)
                .add("row_key_columns_json", DataTypes.StringType, true)
                .add("target_columns_json", DataTypes.StringType, false)
                .add("schema_hash", DataTypes.StringType, false)
                .add("algorithm_version", DataTypes.StringType, false)
                .add("config_json", DataTypes.StringType, false)
                .add("labeling_budget", DataTypes.IntegerType, false)
                .add("selected_tuple_count", DataTypes.LongType, false)
                .add("created_at", DataTypes.LongType, false);
    }

    private static StructType tupleSchema() {
        return new StructType()
                .add("sample_batch_id", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("snapshot_id", DataTypes.StringType, false)
                .add("row_id", DataTypes.StringType, false)
                .add("duplicate_count", DataTypes.LongType, false)
                .add("row_data_json", DataTypes.StringType, false)
                .add("selection_order", DataTypes.IntegerType, false)
                .add("selection_score", DataTypes.DoubleType, false)
                .add("reason_json", DataTypes.StringType, false)
                .add("created_at", DataTypes.LongType, false)
                .add("partition_date", DataTypes.StringType, false);
    }
}
