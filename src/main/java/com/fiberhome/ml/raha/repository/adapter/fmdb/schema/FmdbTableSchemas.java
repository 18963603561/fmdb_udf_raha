package com.fiberhome.ml.raha.repository.adapter.fmdb.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 集中维护九张最终物理表的 Spark 模式和字段顺序。
 */
public final class FmdbTableSchemas {

    /** 采样记录模式。 */
    private static final StructType SAMPLE_RECORD = schema(
            field("sample_batch_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("input_reference", DataTypes.StringType, false),
            field("source_version", DataTypes.StringType, true),
            field("row_identity_mode", DataTypes.StringType, false),
            field("row_key_columns_json", DataTypes.StringType, true),
            field("row_fingerprint_algorithm", DataTypes.StringType, false),
            field("row_fingerprint_version", DataTypes.StringType, false),
            field("row_id", DataTypes.StringType, false),
            field("row_content_hash", DataTypes.StringType, false),
            field("schema_hash", DataTypes.StringType, false),
            field("column_schema_json", DataTypes.StringType, false),
            field("row_data_json", DataTypes.StringType, false),
            field("duplicate_count", DataTypes.LongType, false),
            field("sampling_version", DataTypes.StringType, false),
            field("sampling_context_json", DataTypes.StringType, false),
            field("created_at", DataTypes.LongType, false),
            field("partition_month", DataTypes.StringType, false));
    /** 标注记录模式。 */
    private static final StructType ANNOTATION_RECORD = schema(
            field("annotation_batch_id", DataTypes.StringType, false),
            field("sample_batch_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("annotation_task_id", DataTypes.StringType, true),
            field("row_id", DataTypes.StringType, false),
            field("row_content_hash", DataTypes.StringType, false),
            field("row_data_json", DataTypes.StringType, false),
            field("template_version", DataTypes.StringType, false),
            field("file_name", DataTypes.StringType, true),
            field("schema_hash", DataTypes.StringType, false),
            field("annotation_json", DataTypes.StringType, false),
            field("annotator", DataTypes.StringType, true),
            field("batch_status", DataTypes.StringType, false),
            field("batch_record_count", DataTypes.LongType, false),
            field("valid_record_count", DataTypes.LongType, false),
            field("invalid_record_count", DataTypes.LongType, false),
            field("supersedes_batch_id", DataTypes.StringType, true),
            field("annotated_at", DataTypes.LongType, false),
            field("partition_month", DataTypes.StringType, false));
    /** 训练列级产物模式。 */
    /** 采样快照检查点模式。 */
    private static final StructType SNAPSHOT_CHECKPOINT = schema(
            field("checkpoint_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("snapshot_id", DataTypes.StringType, false),
            field("source_job_id", DataTypes.StringType, false),
            field("sample_batch_id", DataTypes.StringType, true),
            field("record_type", DataTypes.StringType, false),
            field("record_scope", DataTypes.StringType, false),
            field("column_name", DataTypes.StringType, true),
            field("row_id", DataTypes.StringType, true),
            field("cell_id", DataTypes.StringType, true),
            field("cell_value", DataTypes.StringType, true),
            field("artifact_version", DataTypes.StringType, true),
            field("profile_json", DataTypes.StringType, true),
            field("strategy_plan_json", DataTypes.StringType, true),
            field("strategy_hit_json", DataTypes.StringType, true),
            field("feature_dictionary_json", DataTypes.StringType, true),
            field("feature_vector_json", DataTypes.StringType, true),
            field("feature_summary_json", DataTypes.StringType, true),
            field("cluster_version", DataTypes.StringType, true),
            field("cluster_id", DataTypes.StringType, true),
            field("cluster_distance", DataTypes.DoubleType, true),
            field("cluster_summary_json", DataTypes.StringType, true),
            field("payload_json", DataTypes.StringType, true),
            field("row_set_fingerprint", DataTypes.StringType, false),
            field("config_fingerprint", DataTypes.StringType, false),
            field("schema_hash", DataTypes.StringType, false),
            field("source_version", DataTypes.StringType, true),
            field("created_at", DataTypes.LongType, false),
            field("partition_month", DataTypes.StringType, false));
    private static final StructType TRAINING_COLUMN_ARTIFACT = schema(
            field("training_batch_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("source_version", DataTypes.StringType, true),
            field("schema_hash", DataTypes.StringType, false),
            field("merge_algorithm_version", DataTypes.StringType, false),
            field("training_context_json", DataTypes.StringType, false),
            field("column_name", DataTypes.StringType, false),
            field("profile_version", DataTypes.StringType, true),
            field("profile_json", DataTypes.StringType, true),
            field("strategy_plan_version", DataTypes.StringType, true),
            field("strategy_plan_json", DataTypes.StringType, true),
            field("feature_dictionary_version", DataTypes.StringType, true),
            field("feature_dictionary_json", DataTypes.StringType, true),
            field("cluster_version", DataTypes.StringType, true),
            field("cluster_summary_json", DataTypes.StringType, true),
            field("propagation_summary_json", DataTypes.StringType, true),
            field("created_at", DataTypes.LongType, false));
    /** 训练单元格模式。 */
    private static final StructType TRAINING_CELL = schema(
            field("training_batch_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("training_snapshot_id", DataTypes.StringType, false),
            field("row_id", DataTypes.StringType, false),
            field("column_name", DataTypes.StringType, false),
            field("cell_id", DataTypes.StringType, false),
            field("cell_value", DataTypes.StringType, true),
            field("feature_dictionary_version", DataTypes.StringType, false),
            field("feature_vector_json", DataTypes.StringType, false),
            field("feature_summary_json", DataTypes.StringType, false),
            field("cluster_id", DataTypes.StringType, true),
            field("cluster_distance", DataTypes.DoubleType, true),
            field("direct_label", DataTypes.IntegerType, true),
            field("propagated_label", DataTypes.IntegerType, true),
            field("label_source", DataTypes.StringType, true),
            field("source_annotation_batch_id", DataTypes.StringType, true),
            field("sample_weight", DataTypes.DoubleType, true),
            field("created_at", DataTypes.LongType, false));
    /** 最终训练样本模式。 */
    private static final StructType TRAINING_EXAMPLE = schema(
            field("model_set_version", DataTypes.StringType, false),
            field("training_batch_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("row_id", DataTypes.StringType, false),
            field("column_name", DataTypes.StringType, false),
            field("cell_id", DataTypes.StringType, false),
            field("cell_value", DataTypes.StringType, true),
            field("feature_dictionary_version", DataTypes.StringType, false),
            field("feature_vector_json", DataTypes.StringType, false),
            field("label", DataTypes.IntegerType, false),
            field("label_source", DataTypes.StringType, false),
            field("source_annotation_batch_id", DataTypes.StringType, true),
            field("sample_weight", DataTypes.DoubleType, false),
            field("cluster_id", DataTypes.StringType, true),
            field("created_at", DataTypes.LongType, false),
            field("partition_month", DataTypes.StringType, false));
    /** 模型产物模式。 */
    private static final StructType MODEL_ARTIFACT = schema(
            field("model_set_version", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("schema_hash", DataTypes.StringType, false),
            field("training_batch_id", DataTypes.StringType, false),
            field("model_set_status", DataTypes.StringType, false),
            field("state_version", DataTypes.IntegerType, false),
            field("strategy_plan_version", DataTypes.StringType, false),
            field("merge_algorithm_version", DataTypes.StringType, false),
            field("column_name", DataTypes.StringType, false),
            field("model_version", DataTypes.StringType, false),
            field("classifier_type", DataTypes.StringType, false),
            field("feature_dictionary_version", DataTypes.StringType, false),
            field("feature_dimension", DataTypes.IntegerType, false),
            field("threshold", DataTypes.DoubleType, false),
            field("model_path", DataTypes.StringType, true),
            field("model_payload_json", DataTypes.StringType, false),
            field("metrics_json", DataTypes.StringType, false),
            field("created_at", DataTypes.LongType, false),
            field("published_at", DataTypes.LongType, true));
    /** 检测结果模式。 */
    private static final StructType DETECTION_RESULT = schema(
            field("detection_batch_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("input_reference", DataTypes.StringType, false),
            field("model_set_version", DataTypes.StringType, false),
            field("model_version", DataTypes.StringType, false),
            field("row_id", DataTypes.StringType, false),
            field("column_name", DataTypes.StringType, false),
            field("cell_id", DataTypes.StringType, false),
            field("original_value", DataTypes.StringType, true),
            field("row_data_json", DataTypes.StringType, false),
            field("score", DataTypes.DoubleType, false),
            field("threshold", DataTypes.DoubleType, false),
            field("error_reason_json", DataTypes.StringType, false),
            field("detected_at", DataTypes.LongType, false),
            field("partition_date", DataTypes.StringType, false));
    /** 任务状态模式。 */
    private static final StructType JOB_RUN = schema(
            field("job_id", DataTypes.StringType, false),
            field("state_version", DataTypes.IntegerType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("idempotent_key", DataTypes.StringType, false),
            field("job_type", DataTypes.StringType, false),
            field("snapshot_id", DataTypes.StringType, true),
            field("config_version", DataTypes.StringType, false),
            field("status", DataTypes.StringType, false),
            field("current_stage_id", DataTypes.StringType, true),
            field("result_summary_json", DataTypes.StringType, true),
            field("error_code", DataTypes.StringType, true),
            field("error_message", DataTypes.StringType, true),
            field("created_at", DataTypes.LongType, false),
            field("started_at", DataTypes.LongType, false),
            field("finished_at", DataTypes.LongType, false),
            field("updated_at", DataTypes.LongType, false),
            field("partition_month", DataTypes.StringType, false));
    /** 阶段尝试模式。 */
    private static final StructType JOB_STAGE_ATTEMPT = schema(
            field("job_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false),
            field("stage_id", DataTypes.StringType, false),
            field("stage_type", DataTypes.StringType, false),
            field("attempt_id", DataTypes.IntegerType, false),
            field("state_version", DataTypes.IntegerType, false),
            field("checkpoint_id", DataTypes.StringType, true),
            field("status", DataTypes.StringType, false),
            field("input_version_json", DataTypes.StringType, true),
            field("input_fingerprint", DataTypes.StringType, true),
            field("output_location", DataTypes.StringType, true),
            field("summary_json", DataTypes.StringType, true),
            field("error_code", DataTypes.StringType, true),
            field("error_message", DataTypes.StringType, true),
            field("started_at", DataTypes.LongType, false),
            field("completed_at", DataTypes.LongType, false),
            field("updated_at", DataTypes.LongType, false),
            field("partition_month", DataTypes.StringType, false));

    private FmdbTableSchemas() {
    }

    /**
     * 返回标准物理表模式的不可变视图。
     *
     * @param table 物理表
     * @return Spark 表模式
     */
    public static StructType schema(FmdbPhysicalTable table) {
        if (table == null) {
            throw new IllegalArgumentException("FMDB 物理表不能为空");
        }
        switch (table) {
            case SAMPLE_RECORD:
                return SAMPLE_RECORD;
            case ANNOTATION_RECORD:
                return ANNOTATION_RECORD;
            case SNAPSHOT_CHECKPOINT:
                return SNAPSHOT_CHECKPOINT;
            case TRAINING_COLUMN_ARTIFACT:
                return TRAINING_COLUMN_ARTIFACT;
            case TRAINING_CELL:
                return TRAINING_CELL;
            case TRAINING_EXAMPLE:
                return TRAINING_EXAMPLE;
            case MODEL_ARTIFACT:
                return MODEL_ARTIFACT;
            case DETECTION_RESULT:
                return DETECTION_RESULT;
            case JOB_RUN:
                return JOB_RUN;
            case JOB_STAGE_ATTEMPT:
                return JOB_STAGE_ATTEMPT;
            default:
                throw new IllegalStateException("未注册 FMDB 物理表模式：" + table);
        }
    }

    /**
     * 创建严格按标准字段顺序排列的 Spark 行。
     *
     * @param table 物理表
     * @param values 字段值
     * @return 已按模式排序的 Spark 行
     */
    public static Row row(FmdbPhysicalTable table, Map<String, Object> values) {
        StructType type = schema(table);
        if (values == null) {
            throw new IllegalArgumentException("FMDB 行字段不能为空");
        }
        Object[] ordered = new Object[type.fields().length];
        for (int index = 0; index < type.fields().length; index++) {
            String name = type.fields()[index].name();
            if (!values.containsKey(name)) {
                throw new IllegalArgumentException("FMDB 行缺少字段：" + name);
            }
            ordered[index] = values.get(name);
            if (!type.fields()[index].nullable() && ordered[index] == null) {
                throw new IllegalArgumentException("FMDB 必填字段不能为空：" + name);
            }
        }
        if (values.size() != type.fields().length) {
            throw new IllegalArgumentException("FMDB 行包含未声明字段，table="
                    + table.getTableName());
        }
        return RowFactory.create(ordered);
    }

    /**
     * 返回标准表字段名，供查询列裁剪和模式测试使用。
     */
    public static List<String> columns(FmdbPhysicalTable table) {
        return Collections.unmodifiableList(Arrays.asList(schema(table).fieldNames()));
    }

    private static StructType schema(StructField... fields) {
        return DataTypes.createStructType(fields);
    }

    private static StructField field(String name, DataType type, boolean nullable) {
        return DataTypes.createStructField(name, type, nullable);
    }
}
