package com.fiberhome.ml.raha.annotation.excel;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存从标注数据工作表读取的一行原始文本，业务校验由导入服务执行。
 */
public final class AnnotationWorkbookRow {

    /** Excel 一开始的物理行号。 */
    private final int excelRowNumber;
    /** 可选标注任务标识。 */
    private final String annotationTaskId;
    /** 可选逻辑行标识。 */
    private final String rowId;
    /** 可选导出内容哈希。 */
    private final String rowContentHash;
    /** 按模板字段顺序读取的业务值文本。 */
    private final Map<String, String> businessValues;
    /** 用户填写的整行标签文本。 */
    private final String rowLabel;
    /** 用户填写的异常字段文本。 */
    private final String errorColumns;
    /** 用户填写的说明。 */
    private final String comment;

    public AnnotationWorkbookRow(int excelRowNumber,
                                 String annotationTaskId,
                                 String rowId,
                                 String rowContentHash,
                                 Map<String, String> businessValues,
                                 String rowLabel,
                                 String errorColumns,
                                 String comment) {
        if (excelRowNumber <= 1 || businessValues == null) {
            throw new IllegalArgumentException("标注工作簿行号和业务值必须有效");
        }
        this.excelRowNumber = excelRowNumber;
        this.annotationTaskId = annotationTaskId;
        this.rowId = rowId;
        this.rowContentHash = rowContentHash;
        this.businessValues = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(businessValues));
        this.rowLabel = rowLabel;
        this.errorColumns = errorColumns;
        this.comment = comment;
    }

    public int getExcelRowNumber() { return excelRowNumber; }
    public String getAnnotationTaskId() { return annotationTaskId; }
    public String getRowId() { return rowId; }
    public String getRowContentHash() { return rowContentHash; }
    public Map<String, String> getBusinessValues() { return businessValues; }
    public String getRowLabel() { return rowLabel; }
    public String getErrorColumns() { return errorColumns; }
    public String getComment() { return comment; }
}
