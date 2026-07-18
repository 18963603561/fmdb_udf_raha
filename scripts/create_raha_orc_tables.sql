CREATE DATABASE IF NOT EXISTS dw;

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_sample_batch (
    sample_batch_id STRING,
    request_fingerprint STRING,
    dataset_id STRING,
    snapshot_id STRING,
    input_reference STRING,
    source_type STRING,
    row_identity_mode STRING,
    row_key_columns_json STRING,
    target_columns_json STRING,
    schema_hash STRING,
    algorithm_version STRING,
    config_json STRING,
    labeling_budget INT,
    selected_tuple_count BIGINT,
    created_at BIGINT
) STORED AS ORC LOCATION '/fmdb/raha/raha_sample_batch';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_sample_tuple (
    sample_batch_id STRING,
    dataset_id STRING,
    snapshot_id STRING,
    row_id STRING,
    duplicate_count BIGINT,
    row_data_json STRING,
    selection_order INT,
    selection_score DOUBLE,
    reason_json STRING,
    created_at BIGINT
) PARTITIONED BY (partition_date STRING)
STORED AS ORC LOCATION '/fmdb/raha/raha_sample_tuple';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_cell_label (
    sample_batch_id STRING,
    dataset_id STRING,
    snapshot_id STRING,
    row_id STRING,
    column_name STRING,
    value_hash STRING,
    label INT,
    labeled_at BIGINT
) PARTITIONED BY (partition_date STRING)
STORED AS ORC LOCATION '/fmdb/raha/raha_cell_label';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_model_set (
    model_set_version STRING,
    request_fingerprint STRING,
    dataset_id STRING,
    training_snapshot_id STRING,
    sample_batch_ids_json STRING,
    training_mode STRING,
    parent_model_set_version STRING,
    model_columns_json STRING,
    trained_columns_json STRING,
    row_identity_mode STRING,
    row_key_columns_json STRING,
    schema_hash STRING,
    algorithm_version STRING,
    config_json STRING,
    strategy_plan_version STRING,
    strategy_plan_json STRING,
    normalization_version STRING,
    model_count INT,
    training_example_count BIGINT,
    created_at BIGINT
) STORED AS ORC LOCATION '/fmdb/raha/raha_model_set';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_column_model (
    model_set_version STRING,
    dataset_id STRING,
    model_version STRING,
    parent_model_version STRING,
    column_name STRING,
    classifier_type STRING,
    dictionary_version STRING,
    feature_dictionary_json STRING,
    feature_dimension INT,
    threshold DOUBLE,
    model_payload_json STRING,
    training_summary_json STRING,
    created_at BIGINT
) STORED AS ORC LOCATION '/fmdb/raha/raha_column_model';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_training_example (
    model_set_version STRING,
    dataset_id STRING,
    source_sample_batch_id STRING,
    column_name STRING,
    snapshot_id STRING,
    row_id STRING,
    duplicate_count BIGINT,
    value_hash STRING,
    feature_vector_json STRING,
    label INT,
    label_source STRING,
    sample_weight DOUBLE,
    created_at BIGINT
) PARTITIONED BY (partition_date STRING)
STORED AS ORC LOCATION '/fmdb/raha/raha_training_example';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_detection_batch (
    detection_batch_id STRING,
    request_fingerprint STRING,
    dataset_id STRING,
    snapshot_id STRING,
    input_reference STRING,
    source_type STRING,
    row_identity_mode STRING,
    row_key_columns_json STRING,
    target_columns_json STRING,
    schema_hash STRING,
    model_set_version STRING,
    errors_only BOOLEAN,
    input_row_count BIGINT,
    evaluated_cell_count BIGINT,
    detected_cell_count BIGINT,
    created_at BIGINT
) STORED AS ORC LOCATION '/fmdb/raha/raha_detection_batch';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.raha_detection_result (
    detection_batch_id STRING,
    dataset_id STRING,
    snapshot_id STRING,
    model_set_version STRING,
    row_id STRING,
    column_name STRING,
    duplicate_count BIGINT,
    value_hash STRING,
    is_error BOOLEAN,
    score DOUBLE,
    strategy_ids_json STRING,
    reason_json STRING,
    model_version STRING,
    created_at BIGINT
) PARTITIONED BY (partition_date STRING)
STORED AS ORC LOCATION '/fmdb/raha/raha_detection_result';
