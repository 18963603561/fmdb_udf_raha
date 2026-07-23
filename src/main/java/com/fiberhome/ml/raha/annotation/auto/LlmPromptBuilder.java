package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 构造固定格式的模型提示词和单批结构化 JSON，控制请求上下文边界。
 */
public final class LlmPromptBuilder {

    /** 当前提示词协议版本，报告中用于复现调用语义。 */
    public static final String PROMPT_VERSION = "raha-auto-label-v1";
    /** 要求模型只输出 JSON 的系统提示。 */
    private static final String SYSTEM_PROMPT =
            "你是数据质量标注助手。请根据输入行和字段摘要判断数据是否异常。"
                    + "只能从 detectableColumns 中选择异常字段。请严格返回 JSON 对象，"
                    + "格式为 {\"batchId\":\"...\",\"items\":[{"
                    + "\"rowId\":\"...\",\"rowLabel\":0或1,"
                    + "\"errorColumns\":[\"字段名\"],\"confidence\":0到1,"
                    + "\"reason\":\"简短中文原因\"}]}。"
                    + "正常行 rowLabel 必须为 0 且 errorColumns 必须为空；"
                    + "异常行 rowLabel 必须为 1 且至少填写一个异常字段。"
                    + "不要输出 Markdown、解释文字或业务字段修改建议。";

    /**
     * 构造聊天接口的用户消息。
     *
     * @param datasetId 数据集标识
     * @param sampleBatchId 采样批次
     * @param batch 当前请求批次
     * @param maskSensitiveColumns 是否脱敏
     * @param sensitiveColumns 敏感字段集合
     * @return 结构化 JSON 用户消息
     */
    public String buildUserPrompt(String datasetId, String sampleBatchId,
                                  AutoAnnotationBatch batch,
                                  boolean maskSensitiveColumns,
                                  Set<String> sensitiveColumns,
                                  int maxValueChars) {
        return FmdbJsonCodec.write(buildPayload(datasetId, sampleBatchId,
                batch, maskSensitiveColumns, sensitiveColumns,
                maxValueChars));
    }

    /**
     * 构造模型请求正文映射。
     *
     * @return 不包含密钥的模型输入
     */
    public Map<String, Object> buildPayload(String datasetId,
                                            String sampleBatchId,
                                            AutoAnnotationBatch batch,
                                            boolean maskSensitiveColumns,
                                            Set<String> sensitiveColumns,
                                            int maxValueChars) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("task", "raha_auto_annotation");
        payload.put("datasetId", datasetId);
        payload.put("sampleBatchId", sampleBatchId);
        payload.put("batchId", batch.getBatchId());
        payload.put("detectableColumns", batch.getDetectableColumns());
        payload.put("columnSummary", batch.getColumnSummary());
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (AnnotationWorkbookRow row : batch.getRows()) {
            Map<String, Object> rowValue = new LinkedHashMap<String, Object>();
            rowValue.put("rowId", row.getRowId());
            Map<String, String> values = new LinkedHashMap<String, String>();
            for (String column : batch.getDetectableColumns()) {
                String value = row.getBusinessValues().get(column);
                values.put(column, maskSensitiveColumns
                        && sensitiveColumns != null
                        && sensitiveColumns.contains(column)
                        ? mask(value) : truncate(empty(value), maxValueChars));
            }
            rowValue.put("values", values);
            rows.add(rowValue);
        }
        payload.put("rows", rows);
        return payload;
    }

    public String getSystemPrompt() { return SYSTEM_PROMPT; }

    /**
     * 基于完整目标样本生成一个字段窗口的压缩摘要。
     *
     * @return 可直接写入模型请求的摘要映射
     */
    public Map<String, Object> buildColumnSummary(
            List<AnnotationWorkbookRow> rows, List<String> columns,
            boolean maskSensitiveColumns, Set<String> sensitiveColumns,
            int maxValueChars) {
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        for (String column : columns) {
            int emptyCount = 0;
            List<String> examples = new ArrayList<String>();
            List<Integer> lengths = new ArrayList<Integer>();
            for (AnnotationWorkbookRow row : rows) {
                String value = empty(row.getBusinessValues().get(column));
                if (value.isEmpty()) {
                    emptyCount++;
                } else {
                    if (examples.size() < 3) {
                        examples.add(maskSensitiveColumns
                                && sensitiveColumns != null
                                && sensitiveColumns.contains(column)
                                ? mask(value) : truncate(value, maxValueChars));
                    }
                    if (lengths.size() < 5) {
                        lengths.add(Integer.valueOf(value.length()));
                    }
                }
            }
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("emptyCount", Integer.valueOf(emptyCount));
            item.put("examples", examples);
            item.put("observedLengths", lengths);
            summary.put(column, item);
        }
        return summary;
    }

    private static String mask(String value) {
        String text = empty(value);
        return text.isEmpty() ? "" : "<MASKED:length=" + text.length() + ">";
    }

    private static String empty(String value) {
        return value == null ? "" : value;
    }

    private static String truncate(String value, int maximumChars) {
        if (value.length() <= maximumChars) {
            return value;
        }
        return value.substring(0, maximumChars) + "<TRUNCATED>";
    }
}
