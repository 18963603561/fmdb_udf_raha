package com.fiberhome.ml.raha.annotation.domain;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一条 Excel 行校验错误，不包含业务原始值。
 */
public final class AnnotationRowError {

    /** Excel 一开始的物理行号。 */
    private final int excelRowNumber;
    /** 可选逻辑行标识。 */
    private final String rowId;
    /** 稳定错误编码。 */
    private final AnnotationImportErrorCode errorCode;
    /** 不含敏感原值的错误说明。 */
    private final String message;

    public AnnotationRowError(int excelRowNumber,
                              String rowId,
                              AnnotationImportErrorCode errorCode,
                              String message) {
        if (excelRowNumber <= 0 || errorCode == null) {
            throw new IllegalArgumentException("标注错误行号和编码必须有效");
        }
        this.excelRowNumber = excelRowNumber;
        this.rowId = rowId;
        this.errorCode = errorCode;
        this.message = ValueUtils.requireNotBlank(message, "标注错误说明");
    }

    public int getExcelRowNumber() { return excelRowNumber; }
    public String getRowId() { return rowId; }
    public AnnotationImportErrorCode getErrorCode() { return errorCode; }
    public String getMessage() { return message; }
}
