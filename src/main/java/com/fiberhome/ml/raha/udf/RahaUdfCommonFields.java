package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.fmdb.SparkSqlFmdbTableGateway;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存三个 UDF 入口共享的不可变输入字段，不包含任务类型或运行阶段状态。
 */
public final class RahaUdfCommonFields {

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
    /** 调用方标识。 */
    private final String caller;
    /** 任务产物或结果表。 */
    private final String resultTable;

    public RahaUdfCommonFields(String datasetId,
                               String inputReference,
                               DataFormat sourceType,
                               String rowIdColumn,
                               String snapshotId,
                               String idempotencyKey,
                               String caller,
                               String resultTable) {
        if (sourceType != DataFormat.FMDB_TABLE
                && sourceType != DataFormat.FMDB_SQL) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 的 FMDB 来源类型必须有效");
        }
        this.datasetId = required(datasetId, "datasetId");
        this.inputReference = required(inputReference, "inputReference");
        this.sourceType = sourceType;
        this.rowIdColumn = required(rowIdColumn, "rowIdColumn");
        this.snapshotId = blankToNull(snapshotId);
        this.idempotencyKey = required(idempotencyKey, "idempotencyKey");
        this.caller = required(caller, "caller");
        this.resultTable = validateTable(required(resultTable, "resultTable"),
                "resultTable");
        // 表来源必须在入口边界完成表名校验，SQL 来源由数据加载端口执行只读校验。
        if (sourceType == DataFormat.FMDB_TABLE) {
            validateTable(this.inputReference, "inputReference");
        }
    }

    /**
     * 转换为核心数据加载请求。
     *
     * @return 可直接交给 FMDB 数据加载端口的请求
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
     * 生成不含调用方和幂等键的公共配置字段，用于任务配置指纹计算。
     */
    Map<String, String> canonicalValues() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("datasetId", datasetId);
        values.put("inputReference", inputReference);
        values.put("sourceType", sourceType.name());
        values.put("rowIdColumn", rowIdColumn);
        values.put("snapshotId", snapshotId == null ? "" : snapshotId);
        values.put("resultTable", resultTable);
        return values;
    }

    static String required(String value, String field) {
        try {
            return ValueUtils.requireNotBlank(value, "UDF 参数 " + field);
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "缺少必填 UDF 参数：" + field, exception);
        }
    }

    static String validateTable(String value, String field) {
        try {
            return SparkSqlFmdbTableGateway.validateTableName(value);
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 参数 " + field + " 不是合法 FMDB 表名", exception);
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    public String getDatasetId() { return datasetId; }
    public String getInputReference() { return inputReference; }
    public DataFormat getSourceType() { return sourceType; }
    public String getRowIdColumn() { return rowIdColumn; }
    public String getSnapshotId() { return snapshotId; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getCaller() { return caller; }
    public String getResultTable() { return resultTable; }
}
