-- Raha 默认 FMDB 物理表，适用于 Spark SQL 3.3 和 ORC 表。
-- 所有语句必须保持幂等，服务初始化时会重复执行本脚本。

CREATE DATABASE IF NOT EXISTS dw;

-- 保存不可变采样行和待标注任务上下文。
CREATE TABLE IF NOT EXISTS dw.raha_sample_record (
    sample_batch_id STRING,
    dataset_id STRING,
    input_reference STRING,
    source_version STRING,
    row_identity_mode STRING,
    row_key_columns_json STRING,
    row_fingerprint_algorithm STRING,
    row_fingerprint_version STRING,
    row_id STRING,
    row_content_hash STRING,
    schema_hash STRING,
    column_schema_json STRING,
    row_data_json STRING,
    duplicate_count BIGINT,
    sampling_version STRING,
    sampling_context_json STRING,
    created_at BIGINT,
    partition_month STRING
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 追加保存标注批次元数据、原始行快照和用户标签。
CREATE TABLE IF NOT EXISTS dw.raha_annotation_record (
    annotation_batch_id STRING,
    sample_batch_id STRING,
    dataset_id STRING,
    annotation_task_id STRING,
    row_id STRING,
    row_content_hash STRING,
    row_data_json STRING,
    template_version STRING,
    file_name STRING,
    schema_hash STRING,
    annotation_json STRING,
    annotator STRING,
    batch_status STRING,
    batch_record_count BIGINT,
    valid_record_count BIGINT,
    invalid_record_count BIGINT,
    supersedes_batch_id STRING,
    annotated_at BIGINT,
    partition_month STRING
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存列画像、策略、特征字典、聚类和传播摘要。
CREATE TABLE IF NOT EXISTS dw.raha_training_column_artifact (
    training_batch_id STRING,
    dataset_id STRING,
    source_version STRING,
    schema_hash STRING,
    merge_algorithm_version STRING,
    training_context_json STRING,
    column_name STRING,
    profile_version STRING,
    profile_json STRING,
    strategy_plan_version STRING,
    strategy_plan_json STRING,
    feature_dictionary_version STRING,
    feature_dictionary_json STRING,
    cluster_version STRING,
    cluster_summary_json STRING,
    propagation_summary_json STRING,
    created_at BIGINT
)
USING ORC
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存全量训练单元格特征、聚类和标签传播结果。
CREATE TABLE IF NOT EXISTS dw.raha_training_cell (
    training_batch_id STRING,
    dataset_id STRING,
    training_snapshot_id STRING,
    row_id STRING,
    column_name STRING,
    cell_id STRING,
    cell_value STRING,
    feature_dictionary_version STRING,
    feature_vector_json STRING,
    feature_summary_json STRING,
    cluster_id STRING,
    cluster_distance DOUBLE,
    direct_label INT,
    propagated_label INT,
    label_source STRING,
    source_annotation_batch_id STRING,
    sample_weight DOUBLE,
    created_at BIGINT
)
USING ORC
PARTITIONED BY (dataset_id, training_batch_id)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存模型实际使用的单元格值、特征和最终标签。
CREATE TABLE IF NOT EXISTS dw.raha_training_example (
    model_set_version STRING,
    training_batch_id STRING,
    dataset_id STRING,
    row_id STRING,
    column_name STRING,
    cell_id STRING,
    cell_value STRING,
    feature_dictionary_version STRING,
    feature_vector_json STRING,
    label INT,
    label_source STRING,
    source_annotation_batch_id STRING,
    sample_weight DOUBLE,
    cluster_id STRING,
    created_at BIGINT,
    partition_month STRING
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存模型集合和列模型载荷。
CREATE TABLE IF NOT EXISTS dw.raha_model_artifact (
    model_set_version STRING,
    dataset_id STRING,
    training_batch_id STRING,
    model_set_status STRING,
    strategy_plan_version STRING,
    merge_algorithm_version STRING,
    column_name STRING,
    model_version STRING,
    classifier_type STRING,
    feature_dictionary_version STRING,
    feature_dimension INT,
    threshold DOUBLE,
    model_path STRING,
    model_payload_json STRING,
    metrics_json STRING,
    created_at BIGINT,
    published_at BIGINT
)
USING ORC
TBLPROPERTIES ('raha.schema.version' = '1');

-- 只保存错误单元格、具体原值和完整错误行。
CREATE TABLE IF NOT EXISTS dw.raha_detection_result (
    detection_batch_id STRING,
    dataset_id STRING,
    input_reference STRING,
    model_set_version STRING,
    model_version STRING,
    row_id STRING,
    column_name STRING,
    cell_id STRING,
    original_value STRING,
    row_data_json STRING,
    score DOUBLE,
    threshold DOUBLE,
    error_reason_json STRING,
    detected_at BIGINT,
    partition_date STRING
)
USING ORC
PARTITIONED BY (dataset_id, partition_date)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 追加保存任务状态快照。
CREATE TABLE IF NOT EXISTS dw.raha_job_run (
    job_id STRING,
    state_version INT,
    dataset_id STRING,
    idempotent_key STRING,
    job_type STRING,
    snapshot_id STRING,
    config_version STRING,
    status STRING,
    current_stage_id STRING,
    result_summary_json STRING,
    error_code STRING,
    error_message STRING,
    created_at BIGINT,
    started_at BIGINT,
    finished_at BIGINT,
    updated_at BIGINT,
    partition_month STRING
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');

-- 合并保存阶段状态、重试尝试和检查点信息。
CREATE TABLE IF NOT EXISTS dw.raha_job_stage_attempt (
    job_id STRING,
    dataset_id STRING,
    stage_id STRING,
    stage_type STRING,
    attempt_id INT,
    state_version INT,
    checkpoint_id STRING,
    status STRING,
    input_version_json STRING,
    input_fingerprint STRING,
    output_location STRING,
    summary_json STRING,
    error_code STRING,
    error_message STRING,
    started_at BIGINT,
    completed_at BIGINT,
    updated_at BIGINT,
    partition_month STRING
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
TBLPROPERTIES ('raha.schema.version' = '1');
