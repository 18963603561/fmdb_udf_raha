package com.fiberhome.ml.raha.sampling.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存标注页面和 Excel 导出所需的最小 c1 字段投影视图。
 */
public final class SampleAnnotationRow {

    /** 标注任务稳定标识。 */
    private final String annotationTaskId;
    /** 采样逻辑行标识。 */
    private final String rowId;
    /** 导入时校验业务值未被修改的内容哈希。 */
    private final String rowContentHash;
    /** 导入时校验模板字段模式的哈希。 */
    private final String schemaHash;
    /** 有序业务字段模式。 */
    private final Map<String, Object> columnSchema;
    /** 可信采样原始行。 */
    private final Map<String, Object> rowData;
    /** 采样原因和任务有效期。 */
    private final Map<String, Object> samplingContext;

    public SampleAnnotationRow(String annotationTaskId,
                               String rowId,
                               String rowContentHash,
                               String schemaHash,
                               Map<String, Object> columnSchema,
                               Map<String, Object> rowData,
                               Map<String, Object> samplingContext) {
        this.annotationTaskId = ValueUtils.requireNotBlank(
                annotationTaskId, "标注任务标识");
        this.rowId = ValueUtils.requireNotBlank(rowId, "采样逻辑行标识");
        this.rowContentHash = ValueUtils.requireNotBlank(
                rowContentHash, "采样行内容哈希");
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "采样模式哈希");
        this.columnSchema = immutableMap(columnSchema, "采样字段模式");
        this.rowData = immutableMap(rowData, "采样原始行");
        this.samplingContext = immutableMap(samplingContext, "采样任务上下文");
    }

    public String getAnnotationTaskId() { return annotationTaskId; }
    public String getRowId() { return rowId; }
    public String getRowContentHash() { return rowContentHash; }
    public String getSchemaHash() { return schemaHash; }
    public Map<String, Object> getColumnSchema() { return columnSchema; }
    public Map<String, Object> getRowData() { return rowData; }
    public Map<String, Object> getSamplingContext() { return samplingContext; }

    private static Map<String, Object> immutableMap(Map<String, Object> source,
                                                    String name) {
        if (source == null || source.isEmpty()) {
            throw new IllegalArgumentException(name + "不能为空");
        }
        return Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(source));
    }
}
