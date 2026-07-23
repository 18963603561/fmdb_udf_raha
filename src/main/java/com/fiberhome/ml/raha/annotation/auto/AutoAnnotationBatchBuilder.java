package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookData;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 按字段窗口、行数和序列化字符数构造有界模型批次。
 */
public final class AutoAnnotationBatchBuilder {

    /** 提示词构造器，用于按真实 JSON 长度控制批次。 */
    private final LlmPromptBuilder promptBuilder;

    public AutoAnnotationBatchBuilder(LlmPromptBuilder promptBuilder) {
        if (promptBuilder == null) {
            throw new IllegalArgumentException("提示词构造器不能为空");
        }
        this.promptBuilder = promptBuilder;
    }

    /**
     * 构造全部行列批次，批次只保存原始行引用以控制内存占用。
     *
     * @param workbook 工作簿数据
     * @param datasetId 数据集标识
     * @param sampleBatchId 采样批次
     * @param sensitiveColumns 敏感字段
     * @param config 自动标注配置
     * @return 有序模型批次
     */
    public List<AutoAnnotationBatch> build(AnnotationWorkbookData workbook,
                                           String datasetId,
                                           String sampleBatchId,
                                           Set<String> sensitiveColumns,
                                           AutoAnnotationConfig config) {
        if (workbook == null || config == null) {
            throw new IllegalArgumentException("工作簿和自动标注配置不能为空");
        }
        List<AnnotationWorkbookRow> allRows = workbook.getRows();
        int targetCount = config.getMaxTotalRows() <= 0
                ? allRows.size() : Math.min(allRows.size(),
                config.getMaxTotalRows());
        if (targetCount <= 0 || workbook.getDetectableColumns().isEmpty()) {
            return Collections.emptyList();
        }
        List<AnnotationWorkbookRow> targetRows = allRows.subList(0, targetCount);
        List<List<String>> windows = columnWindows(
                workbook.getDetectableColumns(),
                config.getMaxColumnsPerBatch());
        List<AutoAnnotationBatch> batches = new ArrayList<AutoAnnotationBatch>();
        int batchSequence = 1;
        for (int windowIndex = 0; windowIndex < windows.size(); windowIndex++) {
            List<String> window = windows.get(windowIndex);
            Map<String, Object> columnSummary =
                    promptBuilder.buildColumnSummary(targetRows, window,
                            config.isMaskSensitiveColumns(), sensitiveColumns,
                            config.getMaxValueChars());
            List<AnnotationWorkbookRow> current =
                    new ArrayList<AnnotationWorkbookRow>();
            for (AnnotationWorkbookRow row : targetRows) {
                List<AnnotationWorkbookRow> candidate =
                        new ArrayList<AnnotationWorkbookRow>(current);
                candidate.add(row);
                int chars = estimateChars(datasetId, sampleBatchId, window,
                        candidate, windowIndex, windows.size(),
                        columnSummary, sensitiveColumns, config);
                boolean exceeds = candidate.size() > config.getMaxRowsPerBatch()
                        || chars > config.getMaxCharsPerBatch();
                if (exceeds && !current.isEmpty()) {
                    batches.add(createBatch(batchSequence++, windowIndex,
                            windows.size(), window, current, datasetId,
                            sampleBatchId, columnSummary, sensitiveColumns,
                            config));
                    current = new ArrayList<AnnotationWorkbookRow>();
                    current.add(row);
                } else {
                    current = candidate;
                }
                // 单行在截断后仍超限时无法安全发送，直接报告明确配置错误。
                if (current.size() == 1 && estimateChars(datasetId,
                        sampleBatchId, window, current, windowIndex,
                        windows.size(), columnSummary, sensitiveColumns, config)
                        > config.getMaxCharsPerBatch()) {
                    throw new IllegalArgumentException(
                            "单行模型请求超过 autoLabelMaxCharsPerBatch，"
                                    + "请提高字符上限或降低 autoLabelMaxValueChars");
                }
            }
            if (!current.isEmpty()) {
                batches.add(createBatch(batchSequence++, windowIndex,
                        windows.size(), window, current, datasetId,
                        sampleBatchId, columnSummary, sensitiveColumns,
                        config));
            }
        }
        return Collections.unmodifiableList(batches);
    }

    private AutoAnnotationBatch createBatch(int sequence, int windowIndex,
                                            int windowCount,
                                            List<String> columns,
                                            List<AnnotationWorkbookRow> rows,
                                            String datasetId,
                                            String sampleBatchId,
                                            Map<String, Object> columnSummary,
                                            Set<String> sensitiveColumns,
                                            AutoAnnotationConfig config) {
        String batchId = String.format(Locale.ROOT, "batch-%06d", sequence);
        AutoAnnotationBatch provisional = new AutoAnnotationBatch(batchId,
                windowIndex + 1, windowCount, columns, rows, columnSummary, 1);
        int chars = promptBuilder.getSystemPrompt().length()
                + promptBuilder.buildUserPrompt(datasetId, sampleBatchId,
                provisional, config.isMaskSensitiveColumns(),
                sensitiveColumns, config.getMaxValueChars()).length();
        return new AutoAnnotationBatch(batchId, windowIndex + 1, windowCount,
                columns, rows, columnSummary, chars);
    }

    private int estimateChars(String datasetId, String sampleBatchId,
                              List<String> columns,
                              List<AnnotationWorkbookRow> rows,
                              int windowIndex, int windowCount,
                              Map<String, Object> columnSummary,
                              Set<String> sensitiveColumns,
                              AutoAnnotationConfig config) {
        AutoAnnotationBatch provisional = new AutoAnnotationBatch(
                "batch-000000", windowIndex + 1, windowCount, columns, rows,
                columnSummary, 1);
        return promptBuilder.getSystemPrompt().length()
                + promptBuilder.buildUserPrompt(datasetId, sampleBatchId,
                provisional, config.isMaskSensitiveColumns(),
                sensitiveColumns, config.getMaxValueChars()).length();
    }

    private static List<List<String>> columnWindows(List<String> columns,
                                                    int maximumColumns) {
        List<List<String>> result = new ArrayList<List<String>>();
        for (int start = 0; start < columns.size(); start += maximumColumns) {
            int end = Math.min(columns.size(), start + maximumColumns);
            result.add(Collections.unmodifiableList(
                    new ArrayList<String>(columns.subList(start, end))));
        }
        return result;
    }
}
