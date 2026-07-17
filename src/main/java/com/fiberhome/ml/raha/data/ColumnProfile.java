package com.fiberhome.ml.raha.data;

/**
 * 目标字段的轻量画像摘要。
 */
public final class ColumnProfile {

    /** 字段名。 */
    private final String columnName;
    /** 原始加权行数。 */
    private final long rowCount;
    /** 不同值数量。 */
    private final long distinctCount;
    /** 空值和空字符串数量。 */
    private final long missingCount;
    /** 数字格式数量。 */
    private final long numericCount;
    /** 平均文本长度。 */
    private final double averageLength;

    public ColumnProfile(String columnName, long rowCount, long distinctCount,
                         long missingCount, long numericCount, double averageLength) {
        this.columnName = columnName;
        this.rowCount = rowCount;
        this.distinctCount = distinctCount;
        this.missingCount = missingCount;
        this.numericCount = numericCount;
        this.averageLength = averageLength;
    }

    public String getColumnName() { return columnName; }
    public long getRowCount() { return rowCount; }
    public long getDistinctCount() { return distinctCount; }
    public long getMissingCount() { return missingCount; }
    public long getNumericCount() { return numericCount; }
    public double getAverageLength() { return averageLength; }
}
