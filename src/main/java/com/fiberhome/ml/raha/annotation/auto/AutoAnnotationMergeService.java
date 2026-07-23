package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookData;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 按逻辑行合并行批和列窗口结果，并对失败窗口采用保守标注规则。
 */
public final class AutoAnnotationMergeService {

    /**
     * 合并所有成功批次，失败窗口对应的正常结论不会被写入工作簿。
     *
     * @param workbook 工作簿数据
     * @param batches 模型批次结果
     * @return 可安全回写的行级决策
     */
    public List<AutoAnnotationDecision> merge(
            AnnotationWorkbookData workbook,
            List<AutoAnnotationBatchResult> batches) {
        if (workbook == null || batches == null) {
            throw new IllegalArgumentException("工作簿和批次结果不能为空");
        }
        Map<String, Integer> expectedWindows = new HashMap<String, Integer>();
        Map<String, Integer> succeededWindows = new HashMap<String, Integer>();
        Map<String, List<AutoAnnotationDecision>> byRow =
                new HashMap<String, List<AutoAnnotationDecision>>();
        for (AutoAnnotationBatchResult batchResult : batches) {
            for (AnnotationWorkbookRow row : batchResult.getBatch().getRows()) {
                increment(expectedWindows, row.getRowId());
                if (batchResult.isSucceeded()) {
                    increment(succeededWindows, row.getRowId());
                }
            }
            if (batchResult.isSucceeded()) {
                for (AutoAnnotationDecision decision
                        : batchResult.getDecisions()) {
                    List<AutoAnnotationDecision> values = byRow.get(
                            decision.getRowId());
                    if (values == null) {
                        values = new ArrayList<AutoAnnotationDecision>();
                        byRow.put(decision.getRowId(), values);
                    }
                    values.add(decision);
                }
            }
        }
        List<AutoAnnotationDecision> merged =
                new ArrayList<AutoAnnotationDecision>();
        for (AnnotationWorkbookRow row : workbook.getRows()) {
            List<AutoAnnotationDecision> values = byRow.get(row.getRowId());
            if (values == null || values.isEmpty()) {
                continue;
            }
            boolean complete = expectedWindows.get(row.getRowId()).equals(
                    succeededWindows.get(row.getRowId()));
            Set<String> errors = new LinkedHashSet<String>();
            double minimumConfidence = 1.0D;
            List<String> reasons = new ArrayList<String>();
            for (AutoAnnotationDecision decision : values) {
                errors.addAll(decision.getErrorColumns());
                minimumConfidence = Math.min(minimumConfidence,
                        decision.getConfidence());
                if (!decision.getReason().isEmpty()
                        && reasons.size() < 3) {
                    reasons.add(decision.getReason());
                }
            }
            // 有失败窗口且已有窗口仅判断为正常时，保持空白等待人工复核。
            if (!complete && errors.isEmpty()) {
                continue;
            }
            List<String> orderedErrors = ordered(errors,
                    workbook.getDetectableColumns());
            int label = orderedErrors.isEmpty() ? 0 : 1;
            String reason = reasons.isEmpty() ? "模型未提供原因"
                    : join(reasons, "；");
            if (!complete) {
                reason = reason + "；存在失败字段窗口，请人工复核";
            }
            merged.add(new AutoAnnotationDecision("merged", row.getRowId(),
                    label, orderedErrors, minimumConfidence, reason,
                    !complete));
        }
        return Collections.unmodifiableList(merged);
    }

    private static void increment(Map<String, Integer> values, String key) {
        Integer current = values.get(key);
        values.put(key, Integer.valueOf(current == null ? 1
                : current.intValue() + 1));
    }

    private static List<String> ordered(Set<String> values,
                                        List<String> columnOrder) {
        List<String> result = new ArrayList<String>();
        for (String column : columnOrder) {
            if (values.contains(column)) {
                result.add(column);
            }
        }
        return result;
    }

    private static String join(List<String> values, String separator) {
        StringBuilder result = new StringBuilder();
        for (String value : values) {
            if (result.length() > 0) {
                result.append(separator);
            }
            result.append(value);
        }
        return result.toString();
    }
}
