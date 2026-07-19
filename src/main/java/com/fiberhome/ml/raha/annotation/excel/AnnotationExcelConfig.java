package com.fiberhome.ml.raha.annotation.excel;

/**
 * 限制 HSSF 标注工作簿的文件大小和最大数据行数。
 */
public final class AnnotationExcelConfig {

    /** 单个工作簿允许的最大标注行数量。 */
    private final int maximumRowCount;
    /** 单个导入文件允许的最大字节数。 */
    private final long maximumFileBytes;

    public AnnotationExcelConfig(int maximumRowCount,
                                 long maximumFileBytes) {
        if (maximumRowCount <= 0
                || maximumRowCount >= 65536
                || maximumFileBytes <= 0L) {
            throw new IllegalArgumentException("Excel 内存和规模限制必须有效");
        }
        this.maximumRowCount = maximumRowCount;
        this.maximumFileBytes = maximumFileBytes;
    }

    public static AnnotationExcelConfig defaults() {
        return new AnnotationExcelConfig(50000, 50L * 1024L * 1024L);
    }

    public int getMaximumRowCount() { return maximumRowCount; }
    public long getMaximumFileBytes() { return maximumFileBytes; }
}
