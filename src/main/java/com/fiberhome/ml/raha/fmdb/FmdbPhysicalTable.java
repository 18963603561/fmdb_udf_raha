package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * 定义 Raha 九张 FMDB 物理表及其独立持久化配置键。
 */
public enum FmdbPhysicalTable {

    /** 采样逻辑行表。 */
    SAMPLE_RECORD("dw.raha_sample_record",
            "raha.persistence.table.sample-record.enabled",
            "dataset_id", "partition_month"),
    /** 用户标注追加记录表。 */
    ANNOTATION_RECORD("dw.raha_annotation_record",
            "raha.persistence.table.annotation-record.enabled",
            "dataset_id", "partition_month"),
    /** 训练列级阶段产物表。 */
    TRAINING_COLUMN_ARTIFACT("dw.raha_training_column_artifact",
            "raha.persistence.table.training-column-artifact.enabled"),
    /** 全量训练单元格中间状态表。 */
    TRAINING_CELL("dw.raha_training_cell",
            "raha.persistence.table.training-cell.enabled",
            "dataset_id", "training_batch_id"),
    /** 模型实际训练样本表。 */
    TRAINING_EXAMPLE("dw.raha_training_example",
            "raha.persistence.table.training-example.enabled",
            "dataset_id", "partition_month"),
    /** 列模型产物表。 */
    MODEL_ARTIFACT("dw.raha_model_artifact",
            "raha.persistence.table.model-artifact.enabled"),
    /** 错误单元格检测结果表。 */
    DETECTION_RESULT("dw.raha_detection_result",
            "raha.persistence.table.detection-result.enabled",
            "dataset_id", "partition_date"),
    /** 任务状态快照表。 */
    JOB_RUN("dw.raha_job_run",
            "raha.persistence.table.job-run.enabled",
            "dataset_id", "partition_month"),
    /** 阶段尝试和检查点表。 */
    JOB_STAGE_ATTEMPT("dw.raha_job_stage_attempt",
            "raha.persistence.table.job-stage-attempt.enabled",
            "dataset_id", "partition_month");

    /** FMDB 完整物理表名。 */
    private final String tableName;
    /** 控制该表是否写入的配置键。 */
    private final String configKey;
    /** 用于物理裁剪的有序分区字段。 */
    private final List<String> partitionColumns;

    FmdbPhysicalTable(String tableName,
                      String configKey,
                      String... partitionColumns) {
        this.tableName = ValueUtils.requireNotBlank(tableName, "FMDB 物理表名");
        this.configKey = ValueUtils.requireNotBlank(configKey, "FMDB 表配置键");
        this.partitionColumns = Collections.unmodifiableList(
                Arrays.asList(partitionColumns.clone()));
    }

    public String getTableName() {
        return tableName;
    }

    public String getConfigKey() {
        return configKey;
    }

    /**
     * 返回 DDL 中声明的有序分区字段。
     *
     * @return 不可变分区字段列表；未分区表返回空列表
     */
    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    /**
     * 按标准物理表名查找持久化目标，自定义表不属于九表开关范围。
     *
     * @param tableName 已完成格式校验的物理表名
     * @return 匹配的标准物理表；自定义表返回空
     */
    public static FmdbPhysicalTable fromTableName(String tableName) {
        String normalized = ValueUtils.requireNotBlank(
                tableName, "FMDB 物理表名").toLowerCase(Locale.ROOT);
        for (FmdbPhysicalTable table : values()) {
            if (table.tableName.equals(normalized)) {
                return table;
            }
        }
        return null;
    }
}
