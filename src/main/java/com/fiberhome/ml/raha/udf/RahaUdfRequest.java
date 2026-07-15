package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.data.JobType;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.fmdb.SparkSqlFmdbTableGateway;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.util.FormDataCodec;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存训练、检测或采样表级 UDF 的统一异步任务参数。
 */
public final class RahaUdfRequest {

    /** 任务类型。 */
    private final RahaTaskType taskType;
    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** FMDB 表名或只读 SQL。 */
    private final String inputReference;
    /** FMDB 输入来源类型。 */
    private final DataFormat sourceType;
    /** 稳定行标识字段。 */
    private final String rowIdColumn;
    /** 可选输入快照。 */
    private final String snapshotId;
    /** 调用方提供的幂等键。 */
    private final String idempotencyKey;
    /** 调用人标识。 */
    private final String caller;
    /** 任务产物或结果表。 */
    private final String resultTable;
    /** 训练使用的标注表，其他任务为空。 */
    private final String annotationReference;
    /** 检测指定的模型版本，其他任务为空。 */
    private final String modelVersion;
    /** 采样预算，其他任务为零。 */
    private final int labelingBudget;

    RahaUdfRequest(RahaTaskType taskType,
                   String datasetId,
                   String inputReference,
                   DataFormat sourceType,
                   String rowIdColumn,
                   String snapshotId,
                   String idempotencyKey,
                   String caller,
                   String resultTable,
                   String annotationReference,
                   String modelVersion,
                   int labelingBudget) {
        if (taskType == null || (sourceType != DataFormat.FMDB_TABLE
                && sourceType != DataFormat.FMDB_SQL)) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 任务类型和 FMDB 来源类型必须有效");
        }
        this.taskType = taskType;
        this.datasetId = required(datasetId, "datasetId");
        this.inputReference = required(inputReference, "inputReference");
        this.sourceType = sourceType;
        this.rowIdColumn = required(rowIdColumn, "rowIdColumn");
        this.snapshotId = blankToNull(snapshotId);
        this.idempotencyKey = required(idempotencyKey, "idempotencyKey");
        this.caller = required(caller, "caller");
        this.resultTable = validateTable(required(resultTable, "resultTable"), "resultTable");
        this.annotationReference = blankToNull(annotationReference);
        this.modelVersion = blankToNull(modelVersion);
        this.labelingBudget = labelingBudget;
        if (sourceType == DataFormat.FMDB_TABLE) {
            validateTable(this.inputReference, "inputReference");
        }
        validateTaskParameters();
    }

    private void validateTaskParameters() {
        if (taskType == RahaTaskType.TRAIN && annotationReference == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "训练 UDF 必须提供 annotationReference");
        }
        if (taskType == RahaTaskType.TRAIN
                && (modelVersion != null || labelingBudget != 0)) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "训练 UDF 不能包含模型版本或采样预算");
        }
        if (taskType == RahaTaskType.DETECT && modelVersion == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "检测 UDF 必须提供 modelVersion");
        }
        if (taskType == RahaTaskType.DETECT
                && (annotationReference != null || labelingBudget != 0)) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "检测 UDF 不能包含标注表或采样预算");
        }
        if (taskType == RahaTaskType.SAMPLE && labelingBudget <= 0) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "采样 UDF 的 labelingBudget 必须大于零");
        }
        if (taskType == RahaTaskType.SAMPLE
                && (annotationReference != null || modelVersion != null)) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "采样 UDF 不能包含标注表或模型版本");
        }
    }

    /**
     * 转换为核心数据加载请求，供异步工作器直接调用 FMDB 加载器。
     */
    public DataLoadRequest toDataLoadRequest() {
        String tableName = sourceType == DataFormat.FMDB_TABLE
                ? inputReference : datasetId + "_query";
        return new DataLoadRequest(datasetId, inputReference, tableName,
                rowIdColumn, sourceType, Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), snapshotId, null);
    }

    /**
     * 生成不含调用人和幂等键的稳定配置文本，用于识别幂等键参数冲突。
     */
    public String toCanonicalConfiguration() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("taskType", taskType.name());
        values.put("datasetId", datasetId);
        values.put("inputReference", inputReference);
        values.put("sourceType", sourceType.name());
        values.put("rowIdColumn", rowIdColumn);
        values.put("snapshotId", snapshotId == null ? "" : snapshotId);
        values.put("resultTable", resultTable);
        values.put("annotationReference",
                annotationReference == null ? "" : annotationReference);
        values.put("modelVersion", modelVersion == null ? "" : modelVersion);
        values.put("labelingBudget", String.valueOf(labelingBudget));
        return FormDataCodec.encode(values);
    }

    /**
     * 生成能够由异步任务消费者重新解析的完整请求表单。
     *
     * @return 包含执行所需参数的表单编码文本
     */
    public String toEncodedRequest() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("datasetId", datasetId);
        values.put("inputReference", inputReference);
        values.put("sourceType", sourceType == DataFormat.FMDB_TABLE ? "TABLE" : "SQL");
        values.put("rowIdColumn", rowIdColumn);
        values.put("snapshotId", snapshotId == null ? "" : snapshotId);
        values.put("idempotencyKey", idempotencyKey);
        values.put("caller", caller);
        values.put("resultTable", resultTable);
        if (annotationReference != null) {
            values.put("annotationReference", annotationReference);
        }
        if (modelVersion != null) {
            values.put("modelVersion", modelVersion);
        }
        if (labelingBudget > 0) {
            values.put("labelingBudget", String.valueOf(labelingBudget));
        }
        return FormDataCodec.encode(values);
    }

    public JobType toJobType() {
        if (taskType == RahaTaskType.TRAIN) {
            return JobType.TRAINING;
        }
        if (taskType == RahaTaskType.DETECT) {
            return JobType.DETECTION;
        }
        return JobType.SAMPLING;
    }

    private static String required(String value, String field) {
        try {
            return ValueUtils.requireNotBlank(value, "UDF 参数 " + field);
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "缺少必填 UDF 参数：" + field, exception);
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private static String validateTable(String value, String field) {
        try {
            return SparkSqlFmdbTableGateway.validateTableName(value);
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 参数 " + field + " 不是合法 FMDB 表名", exception);
        }
    }

    public RahaTaskType getTaskType() { return taskType; }
    public String getDatasetId() { return datasetId; }
    public String getInputReference() { return inputReference; }
    public DataFormat getSourceType() { return sourceType; }
    public String getRowIdColumn() { return rowIdColumn; }
    public String getSnapshotId() { return snapshotId; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getCaller() { return caller; }
    public String getResultTable() { return resultTable; }
    public String getAnnotationReference() { return annotationReference; }
    public String getModelVersion() { return modelVersion; }
    public int getLabelingBudget() { return labelingBudget; }
}
