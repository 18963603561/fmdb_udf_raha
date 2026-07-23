package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 严格校验模型返回的 JSON，防止模型越权修改字段或漏返回行。
 */
public final class LlmResponseValidator {

    /** 日志记录器，只记录批次和数量，不记录模型正文。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            LlmResponseValidator.class);

    /**
     * 校验并解析一个批次的模型正文。
     *
     * @param responseJson 模型返回 JSON
     * @param batch 发送给模型的批次
     * @return 与预期行一一对应的决策
     */
    @SuppressWarnings("unchecked")
    public List<AutoAnnotationDecision> validate(String responseJson,
                                                 AutoAnnotationBatch batch) {
        Map<String, Object> root;
        try {
            root = FmdbJsonCodec.readObject(normalizeJson(responseJson));
        } catch (IllegalArgumentException exception) {
            // JSON 解析器可能在异常中携带响应片段，统一替换为安全错误。
            throw new IllegalArgumentException("模型返回 JSON 格式无效");
        }
        String batchId = text(root.get("batchId"));
        if (!batch.getBatchId().equals(batchId)) {
            throw new IllegalArgumentException("模型返回 batchId 与请求不一致");
        }
        Object rawItems = root.get("items");
        if (!(rawItems instanceof List)) {
            throw new IllegalArgumentException("模型返回缺少 items 数组");
        }
        Set<String> expected = new LinkedHashSet<String>();
        for (com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow row
                : batch.getRows()) {
            expected.add(row.getRowId());
        }
        Set<String> actual = new HashSet<String>();
        int extraRowCount = 0;
        List<AutoAnnotationDecision> result = new ArrayList<AutoAnnotationDecision>();
        for (Object itemValue : (List<Object>) rawItems) {
            if (!(itemValue instanceof Map)) {
                throw new IllegalArgumentException("模型 items 包含非对象元素");
            }
            Map<String, Object> item = (Map<String, Object>) itemValue;
            String rowId = text(item.get("rowId"));
            if (!expected.contains(rowId)) {
                // 额外行不参与回写，避免模型生成内容污染原始模板。
                extraRowCount++;
                continue;
            }
            if (!actual.add(rowId)) {
                throw new IllegalArgumentException("模型重复返回 rowId：" + rowId);
            }
            int label = number(item.get("rowLabel"), "rowLabel");
            if (label != 0 && label != 1) {
                throw new IllegalArgumentException("模型 rowLabel 只能为 0 或 1");
            }
            List<String> errorColumns = list(item.get("errorColumns"));
            Set<String> allowed = new HashSet<String>(
                    batch.getDetectableColumns());
            for (String column : errorColumns) {
                if (!allowed.contains(column)) {
                    throw new IllegalArgumentException(
                            "模型返回了不可检测字段：" + column);
                }
            }
            if (label == 0 && !errorColumns.isEmpty()) {
                throw new IllegalArgumentException(
                        "正常行的 errorColumns 必须为空：" + rowId);
            }
            if (label == 1 && errorColumns.isEmpty()) {
                throw new IllegalArgumentException(
                        "异常行的 errorColumns 不能为空：" + rowId);
            }
            double confidence = decimal(item.get("confidence"), "confidence");
            if (confidence < 0.0D || confidence > 1.0D) {
                throw new IllegalArgumentException("模型 confidence 必须位于 0 到 1 之间");
            }
            result.add(new AutoAnnotationDecision(batch.getBatchId(), rowId,
                    label, errorColumns, confidence,
                    text(item.get("reason"))));
        }
        if (actual.size() != expected.size()) {
            throw new IllegalArgumentException("模型返回行不完整，期望 "
                    + expected.size() + " 行，实际 " + actual.size() + " 行");
        }
        if (extraRowCount > 0) {
            LOGGER.warn("模型返回额外行，已忽略，batchId={}，extraRowCount={}",
                    batch.getBatchId(), extraRowCount);
        }
        return result;
    }

    private static String normalizeJson(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("模型返回内容为空");
        }
        String text = value.trim();
        if (text.startsWith("```")) {
            int firstLine = text.indexOf('\n');
            int lastFence = text.lastIndexOf("```");
            if (firstLine > 0 && lastFence > firstLine) {
                text = text.substring(firstLine + 1, lastFence).trim();
            }
        }
        return text;
    }

    private static List<String> list(Object value) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException("模型 errorColumns 必须为数组");
        }
        List<String> result = new ArrayList<String>();
        for (Object item : (List<?>) value) {
            String text = text(item);
            if (text.isEmpty() || !result.contains(text)) {
                if (!text.isEmpty()) {
                    result.add(text);
                }
            }
        }
        return result;
    }

    private static int number(Object value, String name) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("模型 " + name + " 必须为数字");
        }
        double decimal = ((Number) value).doubleValue();
        if (decimal != Math.rint(decimal)
                || decimal < Integer.MIN_VALUE || decimal > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("模型 " + name + " 必须为整数");
        }
        return (int) decimal;
    }

    private static double decimal(Object value, String name) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("模型 " + name + " 必须为数字");
        }
        return ((Number) value).doubleValue();
    }

    private static String text(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }
}
