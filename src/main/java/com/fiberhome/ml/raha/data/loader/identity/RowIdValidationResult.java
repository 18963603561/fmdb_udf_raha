package com.fiberhome.ml.raha.data.loader.identity;

/**
 * 行标识校验通过后返回的数据规模摘要。
 */
public final class RowIdValidationResult {

    /** 输入总行数。 */
    private final long rowCount;

    public RowIdValidationResult(long rowCount) {
        if (rowCount <= 0L) {
            throw new IllegalArgumentException("输入行数必须大于 0");
        }
        this.rowCount = rowCount;
    }

    public long getRowCount() {
        return rowCount;
    }
}

