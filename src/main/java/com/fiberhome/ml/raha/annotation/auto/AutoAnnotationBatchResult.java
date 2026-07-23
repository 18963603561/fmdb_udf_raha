package com.fiberhome.ml.raha.annotation.auto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存单个模型批次的调用状态、重试次数和有效决策。
 */
public final class AutoAnnotationBatchResult {

    /** 来源批次。 */
    private final AutoAnnotationBatch batch;
    /** 本批是否成功。 */
    private final boolean succeeded;
    /** 实际调用次数。 */
    private final int attempts;
    /** 包含全部重试的总耗时。 */
    private final long elapsedMillis;
    /** 校验通过的模型决策。 */
    private final List<AutoAnnotationDecision> decisions;
    /** 已去除敏感信息的失败摘要。 */
    private final String errorMessage;

    public AutoAnnotationBatchResult(AutoAnnotationBatch batch,
                                     boolean succeeded, int attempts,
                                     long elapsedMillis,
                                     List<AutoAnnotationDecision> decisions,
                                     String errorMessage) {
        if (batch == null || attempts <= 0 || elapsedMillis < 0L
                || decisions == null) {
            throw new IllegalArgumentException("自动标注批次结果字段无效");
        }
        this.batch = batch;
        this.succeeded = succeeded;
        this.attempts = attempts;
        this.elapsedMillis = elapsedMillis;
        this.decisions = Collections.unmodifiableList(
                new ArrayList<AutoAnnotationDecision>(decisions));
        this.errorMessage = errorMessage == null ? null : errorMessage.trim();
    }

    /**
     * 转换为批次审计报告映射，不包含请求正文和业务值。
     *
     * @return 批次审计字段
     */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("batchId", batch.getBatchId());
        result.put("status", succeeded ? "SUCCEEDED" : "FAILED");
        result.put("attempts", Integer.valueOf(attempts));
        result.put("elapsedMillis", Long.valueOf(elapsedMillis));
        result.put("rowCount", Integer.valueOf(batch.getRows().size()));
        result.put("columnCount",
                Integer.valueOf(batch.getDetectableColumns().size()));
        result.put("estimatedChars",
                Integer.valueOf(batch.getEstimatedChars()));
        result.put("errorMessage", errorMessage);
        return result;
    }

    public AutoAnnotationBatch getBatch() { return batch; }
    public boolean isSucceeded() { return succeeded; }
    public int getAttempts() { return attempts; }
    public long getElapsedMillis() { return elapsedMillis; }
    public List<AutoAnnotationDecision> getDecisions() { return decisions; }
    public String getErrorMessage() { return errorMessage; }
}
