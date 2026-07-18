package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.RowIdentityMode;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.FeatureVectorizer;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.model.RahaModelSet;
import com.fiberhome.ml.raha.model.ModelStore;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.train.TrainingExample;
import com.fiberhome.ml.raha.train.TrainingMode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;

/**
 * 模型集合、列模型和完整训练样本的 ORC 适配器。
 */
public final class FmdbModelStore implements ModelStore {

    /** 标准表网关。 */
    private final FmdbTableGateway gateway;
    /** 特征向量序列化工具。 */
    private final FeatureVectorizer vectorizer = new FeatureVectorizer();

    public FmdbModelStore(FmdbTableGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public Optional<RahaModelSet> findModelSet(String modelSetVersion) {
        List<Row> rows = gateway.table(RahaTables.MODEL_SET)
                .filter(col("model_set_version").equalTo(modelSetVersion))
                .limit(1).collectAsList();
        return rows.isEmpty() ? Optional.<RahaModelSet>empty()
                : Optional.of(toModelSet(rows.get(0)));
    }

    @Override
    public List<RahaColumnModel> loadColumnModels(String modelSetVersion) {
        List<Row> rows = gateway.table(RahaTables.COLUMN_MODEL)
                .filter(col("model_set_version").equalTo(modelSetVersion))
                .collectAsList();
        List<RahaColumnModel> models = new ArrayList<RahaColumnModel>();
        for (Row row : rows) {
            String columnName = row.getAs("column_name");
            FeatureDictionary dictionary = FeatureDictionary.fromJson(columnName,
                    row.getAs("feature_dictionary_json"));
            Map<String, String> payload = JsonUtils.parseStringMap(
                    row.getAs("model_payload_json"));
            double[] coefficients = parseCoefficients(payload.get("coefficients"));
            models.add(new RahaColumnModel(row.getAs("model_set_version"),
                    row.getAs("dataset_id"), row.getAs("model_version"),
                    row.getAs("parent_model_version"), columnName, dictionary,
                    ((Number) row.getAs("threshold")).doubleValue(),
                    Double.parseDouble(payload.get("intercept")), coefficients,
                    row.getAs("training_summary_json"),
                    ((Number) row.getAs("created_at")).longValue()));
        }
        return models;
    }

    @Override
    public List<TrainingExample> loadTrainingExamples(String modelSetVersion) {
        Map<String, Integer> dimensions = new HashMap<String, Integer>();
        for (RahaColumnModel model : loadColumnModels(modelSetVersion)) {
            dimensions.put(model.getColumnName(), model.getFeatureDictionary().size());
        }
        List<Row> rows = gateway.table(RahaTables.TRAINING_EXAMPLE)
                .filter(col("model_set_version").equalTo(modelSetVersion))
                .collectAsList();
        List<TrainingExample> examples = new ArrayList<TrainingExample>();
        for (Row row : rows) {
            String columnName = row.getAs("column_name");
            Integer dimension = dimensions.get(columnName);
            if (dimension == null) {
                continue;
            }
            examples.add(new TrainingExample(row.getAs("model_set_version"),
                    row.getAs("dataset_id"), row.getAs("source_sample_batch_id"),
                    columnName, row.getAs("snapshot_id"), row.getAs("row_id"),
                    ((Number) row.getAs("duplicate_count")).longValue(),
                    row.getAs("value_hash"), vectorizer.fromSparseJson(
                            row.getAs("feature_vector_json"), dimension),
                    ((Number) row.getAs("label")).intValue(),
                    row.getAs("label_source"),
                    ((Number) row.getAs("sample_weight")).doubleValue(),
                    ((Number) row.getAs("created_at")).longValue(),
                    row.getAs("partition_date")));
        }
        return examples;
    }

    @Override
    public void save(RahaModelSet modelSet, List<RahaColumnModel> models,
                     List<TrainingExample> examples) {
        if (findModelSet(modelSet.getModelSetVersion()).isPresent()) {
            return;
        }
        saveExamples(examples);
        saveModels(models);
        Dataset<Row> head = gateway.getSparkSession().createDataFrame(
                Collections.singletonList(RowFactory.create(
                        modelSet.getModelSetVersion(), modelSet.getRequestFingerprint(),
                        modelSet.getDatasetId(), modelSet.getTrainingSnapshotId(),
                        JsonUtils.toJson(modelSet.getSampleBatchIds()),
                        modelSet.getTrainingMode().name(),
                        modelSet.getParentModelSetVersion(),
                        JsonUtils.toJson(modelSet.getModelColumns()),
                        JsonUtils.toJson(modelSet.getTrainedColumns()),
                        modelSet.getRowIdentityMode().name(),
                        JsonUtils.toJson(modelSet.getRowKeyColumns()),
                        modelSet.getSchemaHash(), modelSet.getAlgorithmVersion(),
                        modelSet.getConfigJson(), modelSet.getStrategyPlanVersion(),
                        modelSet.getStrategyPlanJson(), modelSet.getNormalizationVersion(),
                        modelSet.getModelCount(), modelSet.getTrainingExampleCount(),
                        modelSet.getCreatedAt())), modelSetSchema());
        gateway.appendDistinct(RahaTables.MODEL_SET, head, "model_set_version");
    }

    private void saveModels(List<RahaColumnModel> models) {
        List<Row> rows = new ArrayList<Row>();
        for (RahaColumnModel model : models) {
            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("intercept", String.valueOf(model.getIntercept()));
            payload.put("coefficients", coefficients(model.getCoefficients()));
            rows.add(RowFactory.create(model.getModelSetVersion(), model.getDatasetId(),
                    model.getModelVersion(), model.getParentModelVersion(),
                    model.getColumnName(), model.getClassifierType().name(),
                    model.getFeatureDictionary().getVersion(),
                    model.getFeatureDictionary().toJson(),
                    model.getFeatureDictionary().size(), model.getThreshold(),
                    JsonUtils.toJson(payload), model.getTrainingSummaryJson(),
                    model.getCreatedAt()));
        }
        if (rows.isEmpty()) {
            return;
        }
        Dataset<Row> frame = gateway.getSparkSession().createDataFrame(rows,
                columnModelSchema());
        gateway.appendDistinct(RahaTables.COLUMN_MODEL, frame,
                "model_set_version", "column_name");
    }

    private void saveExamples(List<TrainingExample> examples) {
        List<Row> rows = new ArrayList<Row>();
        for (TrainingExample example : examples) {
            rows.add(RowFactory.create(example.getModelSetVersion(), example.getDatasetId(),
                    example.getSourceSampleBatchId(), example.getColumnName(),
                    example.getSnapshotId(), example.getRowId(),
                    example.getDuplicateCount(), example.getValueHash(),
                    vectorizer.toSparseJson(example.getFeatureVector()), example.getLabel(),
                    example.getLabelSource(), example.getSampleWeight(),
                    example.getCreatedAt(), example.getPartitionDate()));
        }
        if (rows.isEmpty()) {
            return;
        }
        Dataset<Row> frame = gateway.getSparkSession().createDataFrame(rows,
                trainingExampleSchema());
        gateway.appendDistinct(RahaTables.TRAINING_EXAMPLE, frame,
                "model_set_version", "column_name", "snapshot_id", "row_id");
    }

    private static RahaModelSet toModelSet(Row row) {
        return new RahaModelSet(row.getAs("model_set_version"),
                row.getAs("request_fingerprint"), row.getAs("dataset_id"),
                row.getAs("training_snapshot_id"),
                JsonUtils.parseStringArray(row.getAs("sample_batch_ids_json")),
                TrainingMode.valueOf((String) row.getAs("training_mode")),
                row.getAs("parent_model_set_version"),
                JsonUtils.parseStringArray(row.getAs("model_columns_json")),
                JsonUtils.parseStringArray(row.getAs("trained_columns_json")),
                RowIdentityMode.valueOf((String) row.getAs("row_identity_mode")),
                JsonUtils.parseStringArray(row.getAs("row_key_columns_json")),
                row.getAs("schema_hash"), row.getAs("algorithm_version"),
                row.getAs("config_json"), row.getAs("strategy_plan_version"),
                row.getAs("strategy_plan_json"), row.getAs("normalization_version"),
                ((Number) row.getAs("model_count")).intValue(),
                ((Number) row.getAs("training_example_count")).longValue(),
                ((Number) row.getAs("created_at")).longValue());
    }

    private static String coefficients(double[] values) {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < values.length; index++) {
            if (index > 0) {
                builder.append(',');
            }
            builder.append(values[index]);
        }
        return builder.toString();
    }

    private static double[] parseCoefficients(String value) {
        if (value == null || value.isEmpty()) {
            return new double[0];
        }
        String[] tokens = value.split(",");
        double[] result = new double[tokens.length];
        for (int index = 0; index < tokens.length; index++) {
            result[index] = Double.parseDouble(tokens[index]);
        }
        return result;
    }

    private static StructType modelSetSchema() {
        return new StructType()
                .add("model_set_version", DataTypes.StringType, false)
                .add("request_fingerprint", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("training_snapshot_id", DataTypes.StringType, false)
                .add("sample_batch_ids_json", DataTypes.StringType, false)
                .add("training_mode", DataTypes.StringType, false)
                .add("parent_model_set_version", DataTypes.StringType, true)
                .add("model_columns_json", DataTypes.StringType, false)
                .add("trained_columns_json", DataTypes.StringType, false)
                .add("row_identity_mode", DataTypes.StringType, false)
                .add("row_key_columns_json", DataTypes.StringType, true)
                .add("schema_hash", DataTypes.StringType, false)
                .add("algorithm_version", DataTypes.StringType, false)
                .add("config_json", DataTypes.StringType, false)
                .add("strategy_plan_version", DataTypes.StringType, false)
                .add("strategy_plan_json", DataTypes.StringType, false)
                .add("normalization_version", DataTypes.StringType, false)
                .add("model_count", DataTypes.IntegerType, false)
                .add("training_example_count", DataTypes.LongType, false)
                .add("created_at", DataTypes.LongType, false);
    }

    private static StructType columnModelSchema() {
        return new StructType()
                .add("model_set_version", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("model_version", DataTypes.StringType, false)
                .add("parent_model_version", DataTypes.StringType, true)
                .add("column_name", DataTypes.StringType, false)
                .add("classifier_type", DataTypes.StringType, false)
                .add("dictionary_version", DataTypes.StringType, false)
                .add("feature_dictionary_json", DataTypes.StringType, false)
                .add("feature_dimension", DataTypes.IntegerType, false)
                .add("threshold", DataTypes.DoubleType, false)
                .add("model_payload_json", DataTypes.StringType, false)
                .add("training_summary_json", DataTypes.StringType, false)
                .add("created_at", DataTypes.LongType, false);
    }

    private static StructType trainingExampleSchema() {
        return new StructType()
                .add("model_set_version", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("source_sample_batch_id", DataTypes.StringType, true)
                .add("column_name", DataTypes.StringType, false)
                .add("snapshot_id", DataTypes.StringType, false)
                .add("row_id", DataTypes.StringType, false)
                .add("duplicate_count", DataTypes.LongType, false)
                .add("value_hash", DataTypes.StringType, false)
                .add("feature_vector_json", DataTypes.StringType, false)
                .add("label", DataTypes.IntegerType, false)
                .add("label_source", DataTypes.StringType, false)
                .add("sample_weight", DataTypes.DoubleType, false)
                .add("created_at", DataTypes.LongType, false)
                .add("partition_date", DataTypes.StringType, false);
    }
}
