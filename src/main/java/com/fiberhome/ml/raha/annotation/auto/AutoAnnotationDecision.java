package com.fiberhome.ml.raha.annotation.auto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存模型对一个行列窗口给出的结构化标注结论。
 */
public final class AutoAnnotationDecision {

    /** 来源模型批次标识。 */
    private final String batchId;
    /** 采样逻辑行标识。 */
    private final String rowId;
    /** 整行标签，零为正常，一为异常。 */
    private final int rowLabel;
    /** 模型识别出的异常字段。 */
    private final List<String> errorColumns;
    /** 模型置信度。 */
    private final double confidence;
    /** 模型给出的简短原因。 */
    private final String reason;
    /** 是否存在未成功处理的列窗口。 */
    private final boolean requiresReview;

    public AutoAnnotationDecision(String batchId, String rowId, int rowLabel,
                                  List<String> errorColumns,
                                  double confidence, String reason) {
        this(batchId, rowId, rowLabel, errorColumns, confidence, reason, false);
    }

    public AutoAnnotationDecision(String batchId, String rowId, int rowLabel,
                                  List<String> errorColumns,
                                  double confidence, String reason,
                                  boolean requiresReview) {
        if (isBlank(batchId) || isBlank(rowId)
                || (rowLabel != 0 && rowLabel != 1)
                || errorColumns == null || Double.isNaN(confidence)
                || confidence < 0.0D || confidence > 1.0D) {
            throw new IllegalArgumentException("自动标注决策字段无效");
        }
        this.batchId = batchId.trim();
        this.rowId = rowId.trim();
        this.rowLabel = rowLabel;
        this.errorColumns = Collections.unmodifiableList(
                new ArrayList<String>(errorColumns));
        this.confidence = confidence;
        this.reason = reason == null ? "" : reason.trim();
        this.requiresReview = requiresReview;
    }

    /**
     * 转换为审计报告使用的稳定映射。
     *
     * @return 决策字段映射
     */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("batchId", batchId);
        result.put("rowId", rowId);
        result.put("rowLabel", Integer.valueOf(rowLabel));
        result.put("errorColumns", errorColumns);
        result.put("confidence", Double.valueOf(confidence));
        result.put("reason", reason);
        result.put("requiresReview", Boolean.valueOf(requiresReview));
        return result;
    }

    public String getBatchId() { return batchId; }
    public String getRowId() { return rowId; }
    public int getRowLabel() { return rowLabel; }
    public List<String> getErrorColumns() { return errorColumns; }
    public double getConfidence() { return confidence; }
    public String getReason() { return reason; }
    public boolean isRequiresReview() { return requiresReview; }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
