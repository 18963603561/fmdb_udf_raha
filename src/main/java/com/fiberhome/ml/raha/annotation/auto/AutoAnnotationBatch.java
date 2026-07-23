package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 描述一次模型调用处理的行集合和可检测字段窗口。
 */
public final class AutoAnnotationBatch {

    /** 稳定批次标识。 */
    private final String batchId;
    /** 当前列窗口编号，从一开始。 */
    private final int columnWindowIndex;
    /** 总列窗口数量。 */
    private final int columnWindowCount;
    /** 当前批次可检测字段。 */
    private final List<String> detectableColumns;
    /** 当前批次标注行，仅持有原工作簿行引用。 */
    private final List<AnnotationWorkbookRow> rows;
    /** 当前字段窗口基于完整目标样本计算的稳定摘要。 */
    private final Map<String, Object> columnSummary;
    /** 当前请求近似字符数。 */
    private final int estimatedChars;

    public AutoAnnotationBatch(String batchId, int columnWindowIndex,
                               int columnWindowCount,
                               List<String> detectableColumns,
                               List<AnnotationWorkbookRow> rows,
                               Map<String, Object> columnSummary,
                               int estimatedChars) {
        if (batchId == null || batchId.trim().isEmpty()
                || columnWindowIndex <= 0 || columnWindowCount <= 0
                || columnWindowIndex > columnWindowCount
                || detectableColumns == null || detectableColumns.isEmpty()
                || rows == null || rows.isEmpty() || columnSummary == null
                || estimatedChars <= 0) {
            throw new IllegalArgumentException("自动标注批次字段无效");
        }
        this.batchId = batchId.trim();
        this.columnWindowIndex = columnWindowIndex;
        this.columnWindowCount = columnWindowCount;
        this.detectableColumns = Collections.unmodifiableList(
                new ArrayList<String>(detectableColumns));
        this.rows = Collections.unmodifiableList(
                new ArrayList<AnnotationWorkbookRow>(rows));
        this.columnSummary = Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(columnSummary));
        this.estimatedChars = estimatedChars;
    }

    public String getBatchId() { return batchId; }
    public int getColumnWindowIndex() { return columnWindowIndex; }
    public int getColumnWindowCount() { return columnWindowCount; }
    public List<String> getDetectableColumns() { return detectableColumns; }
    public List<AnnotationWorkbookRow> getRows() { return rows; }
    public Map<String, Object> getColumnSummary() { return columnSummary; }
    public int getEstimatedChars() { return estimatedChars; }
}
