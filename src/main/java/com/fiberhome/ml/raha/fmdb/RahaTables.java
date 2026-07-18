package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.config.RahaConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 八张标准 ORC 表名称、字段顺序和建表语句。
 */
public final class RahaTables {

    public static final String SAMPLE_BATCH = "dw.raha_sample_batch";
    public static final String SAMPLE_TUPLE = "dw.raha_sample_tuple";
    public static final String CELL_LABEL = "dw.raha_cell_label";
    public static final String MODEL_SET = "dw.raha_model_set";
    public static final String COLUMN_MODEL = "dw.raha_column_model";
    public static final String TRAINING_EXAMPLE = "dw.raha_training_example";
    public static final String DETECTION_BATCH = "dw.raha_detection_batch";
    public static final String DETECTION_RESULT = "dw.raha_detection_result";

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaTables.class);

    private RahaTables() {
    }

    /**
     * 幂等创建标准 ORC 表。
     *
     * @param sparkSession 当前 Spark 会话
     * @param config 存储配置
     */
    public static void ensure(SparkSession sparkSession, RahaConfig config) {
        long startedAt = System.currentTimeMillis();
        String root = normalizeRoot(config.getStorageRoot());
        LOGGER.info("开始检查 Raha ORC 表，database=dw，storageRoot={}", root);
        // 动态分区参数必须在 Hadoop 配置中设置，Spark SQL SET 不会可靠地传递给 Hive。
        sparkSession.sparkContext().hadoopConfiguration()
                .set("hive.exec.dynamic.partition", "true");
        sparkSession.sparkContext().hadoopConfiguration()
                .set("hive.exec.dynamic.partition.mode", "nonstrict");
        sparkSession.sql("CREATE DATABASE IF NOT EXISTS dw");
        sparkSession.sql(createSampleBatch(root));
        sparkSession.sql(createSampleTuple(root));
        sparkSession.sql(createCellLabel(root));
        sparkSession.sql(createModelSet(root));
        sparkSession.sql(createColumnModel(root));
        sparkSession.sql(createTrainingExample(root));
        sparkSession.sql(createDetectionBatch(root));
        sparkSession.sql(createDetectionResult(root));
        LOGGER.info("Raha ORC 表检查完成，elapsedMillis={}",
                System.currentTimeMillis() - startedAt);
    }

    private static String createSampleBatch(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + SAMPLE_BATCH + " ("
                + "sample_batch_id STRING, request_fingerprint STRING, dataset_id STRING, "
                + "snapshot_id STRING, input_reference STRING, source_type STRING, "
                + "row_identity_mode STRING, row_key_columns_json STRING, "
                + "target_columns_json STRING, schema_hash STRING, algorithm_version STRING, "
                + "config_json STRING, labeling_budget INT, selected_tuple_count BIGINT, "
                + "created_at BIGINT) STORED AS ORC LOCATION '" + root
                + "/raha_sample_batch'";
    }

    private static String createSampleTuple(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + SAMPLE_TUPLE + " ("
                + "sample_batch_id STRING, dataset_id STRING, snapshot_id STRING, "
                + "row_id STRING, duplicate_count BIGINT, row_data_json STRING, "
                + "selection_order INT, selection_score DOUBLE, reason_json STRING, "
                + "created_at BIGINT) PARTITIONED BY (partition_date STRING) "
                + "STORED AS ORC LOCATION '" + root + "/raha_sample_tuple'";
    }

    private static String createCellLabel(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + CELL_LABEL + " ("
                + "sample_batch_id STRING, dataset_id STRING, snapshot_id STRING, "
                + "row_id STRING, column_name STRING, value_hash STRING, label INT, "
                + "labeled_at BIGINT) PARTITIONED BY (partition_date STRING) "
                + "STORED AS ORC LOCATION '" + root + "/raha_cell_label'";
    }

    private static String createModelSet(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + MODEL_SET + " ("
                + "model_set_version STRING, request_fingerprint STRING, dataset_id STRING, "
                + "training_snapshot_id STRING, sample_batch_ids_json STRING, "
                + "training_mode STRING, parent_model_set_version STRING, "
                + "model_columns_json STRING, trained_columns_json STRING, "
                + "row_identity_mode STRING, row_key_columns_json STRING, schema_hash STRING, "
                + "algorithm_version STRING, config_json STRING, strategy_plan_version STRING, "
                + "strategy_plan_json STRING, normalization_version STRING, model_count INT, "
                + "training_example_count BIGINT, created_at BIGINT) STORED AS ORC LOCATION '"
                + root + "/raha_model_set'";
    }

    private static String createColumnModel(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + COLUMN_MODEL + " ("
                + "model_set_version STRING, dataset_id STRING, model_version STRING, "
                + "parent_model_version STRING, column_name STRING, classifier_type STRING, "
                + "dictionary_version STRING, feature_dictionary_json STRING, "
                + "feature_dimension INT, threshold DOUBLE, model_payload_json STRING, "
                + "training_summary_json STRING, created_at BIGINT) STORED AS ORC LOCATION '"
                + root + "/raha_column_model'";
    }

    private static String createTrainingExample(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + TRAINING_EXAMPLE + " ("
                + "model_set_version STRING, dataset_id STRING, source_sample_batch_id STRING, "
                + "column_name STRING, snapshot_id STRING, row_id STRING, "
                + "duplicate_count BIGINT, value_hash STRING, feature_vector_json STRING, "
                + "label INT, label_source STRING, sample_weight DOUBLE, created_at BIGINT) "
                + "PARTITIONED BY (partition_date STRING) STORED AS ORC LOCATION '"
                + root + "/raha_training_example'";
    }

    private static String createDetectionBatch(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + DETECTION_BATCH + " ("
                + "detection_batch_id STRING, request_fingerprint STRING, dataset_id STRING, "
                + "snapshot_id STRING, input_reference STRING, source_type STRING, "
                + "row_identity_mode STRING, row_key_columns_json STRING, "
                + "target_columns_json STRING, schema_hash STRING, model_set_version STRING, "
                + "errors_only BOOLEAN, input_row_count BIGINT, evaluated_cell_count BIGINT, "
                + "detected_cell_count BIGINT, created_at BIGINT) STORED AS ORC LOCATION '"
                + root + "/raha_detection_batch'";
    }

    private static String createDetectionResult(String root) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + DETECTION_RESULT + " ("
                + "detection_batch_id STRING, dataset_id STRING, snapshot_id STRING, "
                + "model_set_version STRING, row_id STRING, column_name STRING, "
                + "duplicate_count BIGINT, value_hash STRING, is_error BOOLEAN, score DOUBLE, "
                + "strategy_ids_json STRING, reason_json STRING, model_version STRING, "
                + "created_at BIGINT) PARTITIONED BY (partition_date STRING) "
                + "STORED AS ORC LOCATION '" + root + "/raha_detection_result'";
    }

    private static String normalizeRoot(String root) {
        return root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
    }
}
