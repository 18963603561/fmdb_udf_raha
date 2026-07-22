-- Raha 默认 FMDB 物理表，适用于 Spark SQL 3.3 和 ORC 表。
-- 所有语句必须保持幂等，服务初始化时会重复执行本脚本。

CREATE DATABASE IF NOT EXISTS dw;

-- 保存不可变采样行和待标注任务上下文。
CREATE TABLE IF NOT EXISTS dw.raha_sample_record (
    sample_batch_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    input_reference STRING NOT NULL,
    source_version STRING,
    row_identity_mode STRING NOT NULL,
    row_key_columns_json STRING,
    row_fingerprint_algorithm STRING NOT NULL,
    row_fingerprint_version STRING NOT NULL,
    row_id STRING NOT NULL,
    row_content_hash STRING NOT NULL,
    schema_hash STRING NOT NULL,
    column_schema_json STRING NOT NULL,
    row_data_json STRING NOT NULL,
    duplicate_count BIGINT NOT NULL,
    sampling_version STRING NOT NULL,
    sampling_context_json STRING NOT NULL,
    created_at BIGINT NOT NULL,
    partition_month STRING NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 追加保存标注批次元数据、原始行快照和用户标签。
CREATE TABLE IF NOT EXISTS dw.raha_annotation_record (
    annotation_batch_id STRING NOT NULL,
    sample_batch_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    annotation_task_id STRING,
    row_id STRING NOT NULL,
    row_content_hash STRING NOT NULL,
    row_data_json STRING NOT NULL,
    template_version STRING NOT NULL,
    file_name STRING,
    schema_hash STRING NOT NULL,
    annotation_json STRING NOT NULL,
    annotator STRING,
    batch_status STRING NOT NULL,
    batch_record_count BIGINT NOT NULL,
    valid_record_count BIGINT NOT NULL,
    invalid_record_count BIGINT NOT NULL,
    supersedes_batch_id STRING,
    annotated_at BIGINT NOT NULL,
    partition_month STRING NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存列画像、策略、特征字典、聚类和传播摘要。
-- 保存采样快照检查点，供训练阶段复用 PROFILE、RUN_STRATEGY、FEATURE 和 CLUSTER 产物。
CREATE TABLE IF NOT EXISTS dw.raha_snapshot_checkpoint (
    checkpoint_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    snapshot_id STRING NOT NULL,
    source_job_id STRING NOT NULL,
    sample_batch_id STRING,
    record_type STRING NOT NULL,
    record_scope STRING NOT NULL,
    column_name STRING,
    row_id STRING,
    cell_id STRING,
    cell_value STRING,
    artifact_version STRING,
    profile_json STRING,
    strategy_plan_json STRING,
    strategy_hit_json STRING,
    feature_dictionary_json STRING,
    feature_vector_json STRING,
    feature_summary_json STRING,
    cluster_version STRING,
    cluster_id STRING,
    cluster_distance DOUBLE,
    cluster_summary_json STRING,
    payload_json STRING,
    row_set_fingerprint STRING NOT NULL,
    config_fingerprint STRING NOT NULL,
    schema_hash STRING NOT NULL,
    source_version STRING,
    created_at BIGINT NOT NULL,
    partition_month STRING NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

CREATE TABLE IF NOT EXISTS dw.raha_training_column_artifact (
    training_batch_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    source_version STRING,
    schema_hash STRING NOT NULL,
    merge_algorithm_version STRING NOT NULL,
    training_context_json STRING NOT NULL,
    column_name STRING NOT NULL,
    profile_version STRING,
    profile_json STRING,
    strategy_plan_version STRING,
    strategy_plan_json STRING,
    feature_dictionary_version STRING,
    feature_dictionary_json STRING,
    cluster_version STRING,
    cluster_summary_json STRING,
    propagation_summary_json STRING,
    created_at BIGINT NOT NULL
)
USING ORC
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存全量训练单元格特征、聚类和标签传播结果。
CREATE TABLE IF NOT EXISTS dw.raha_training_cell (
    training_batch_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    training_snapshot_id STRING NOT NULL,
    row_id STRING NOT NULL,
    column_name STRING NOT NULL,
    cell_id STRING NOT NULL,
    cell_value STRING,
    feature_dictionary_version STRING NOT NULL,
    feature_vector_json STRING NOT NULL,
    feature_summary_json STRING NOT NULL,
    cluster_id STRING,
    cluster_distance DOUBLE,
    direct_label INT,
    propagated_label INT,
    label_source STRING,
    source_annotation_batch_id STRING,
    sample_weight DOUBLE,
    created_at BIGINT NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, training_batch_id)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存模型实际使用的单元格值、特征和最终标签。
CREATE TABLE IF NOT EXISTS dw.raha_training_example (
    model_set_version STRING NOT NULL,
    training_batch_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    row_id STRING NOT NULL,
    column_name STRING NOT NULL,
    cell_id STRING NOT NULL,
    cell_value STRING,
    feature_dictionary_version STRING NOT NULL,
    feature_vector_json STRING NOT NULL,
    label INT NOT NULL,
    label_source STRING NOT NULL,
    source_annotation_batch_id STRING,
    sample_weight DOUBLE NOT NULL,
    cluster_id STRING,
    created_at BIGINT NOT NULL,
    partition_month STRING NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存模型集合和列模型载荷。
CREATE TABLE IF NOT EXISTS dw.raha_model_artifact (
    model_set_version STRING NOT NULL,
    dataset_id STRING NOT NULL,
    schema_hash STRING NOT NULL,
    training_batch_id STRING NOT NULL,
    model_set_status STRING NOT NULL,
    state_version INT NOT NULL,
    strategy_plan_version STRING NOT NULL,
    merge_algorithm_version STRING NOT NULL,
    column_name STRING NOT NULL,
    model_version STRING NOT NULL,
    classifier_type STRING NOT NULL,
    feature_dictionary_version STRING NOT NULL,
    feature_dimension INT NOT NULL,
    threshold DOUBLE NOT NULL,
    model_path STRING,
    model_payload_json STRING NOT NULL,
    metrics_json STRING NOT NULL,
    created_at BIGINT NOT NULL,
    published_at BIGINT
)
USING ORC
TBLPROPERTIES ('raha.schema.version' = '1');

-- 只保存错误单元格、具体原值和完整错误行。
CREATE TABLE IF NOT EXISTS dw.raha_detection_result (
    detection_batch_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    input_reference STRING NOT NULL,
    model_set_version STRING NOT NULL,
    model_version STRING NOT NULL,
    row_id STRING NOT NULL,
    column_name STRING NOT NULL,
    cell_id STRING NOT NULL,
    original_value STRING,
    row_data_json STRING NOT NULL,
    score DOUBLE NOT NULL,
    threshold DOUBLE NOT NULL,
    error_reason_json STRING NOT NULL,
    detected_at BIGINT NOT NULL,
    partition_date STRING NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, partition_date)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 追加保存任务状态快照。
CREATE TABLE IF NOT EXISTS dw.raha_job_run (
    job_id STRING NOT NULL,
    state_version INT NOT NULL,
    dataset_id STRING NOT NULL,
    idempotent_key STRING NOT NULL,
    job_type STRING NOT NULL,
    snapshot_id STRING,
    config_version STRING NOT NULL,
    status STRING NOT NULL,
    current_stage_id STRING,
    result_summary_json STRING,
    error_code STRING,
    error_message STRING,
    created_at BIGINT NOT NULL,
    started_at BIGINT NOT NULL,
    finished_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    partition_month STRING NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 合并保存阶段状态、重试尝试和检查点信息。
CREATE TABLE IF NOT EXISTS dw.raha_job_stage_attempt (
    job_id STRING NOT NULL,
    dataset_id STRING NOT NULL,
    stage_id STRING NOT NULL,
    stage_type STRING NOT NULL,
    attempt_id INT NOT NULL,
    state_version INT NOT NULL,
    checkpoint_id STRING,
    status STRING NOT NULL,
    input_version_json STRING,
    input_fingerprint STRING,
    output_location STRING,
    summary_json STRING,
    error_code STRING,
    error_message STRING,
    started_at BIGINT NOT NULL,
    completed_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    partition_month STRING NOT NULL
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');
