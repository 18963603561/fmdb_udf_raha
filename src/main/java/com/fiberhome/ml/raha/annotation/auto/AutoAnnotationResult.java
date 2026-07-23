package com.fiberhome.ml.raha.annotation.auto;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存自动标注状态、统计信息和可加入采集 ZIP 的产物路径。
 */
public final class AutoAnnotationResult {

    /** 最终任务状态。 */
    private final AutoAnnotationStatus status;
    /** 原工作簿总记录数。 */
    private final int recordCount;
    /** 成功写入标签的记录数。 */
    private final int labeledCount;
    /** 未获得完整可用标签的记录数。 */
    private final int failedCount;
    /** 实际构造的模型批次数。 */
    private final int batchCount;
    /** 可选自动标注工作簿。 */
    private final Path workbookPath;
    /** 自动标注摘要报告。 */
    private final Path summaryPath;
    /** 行级决策报告。 */
    private final Path decisionsPath;
    /** 批次调用报告。 */
    private final Path batchesPath;
    /** 不含敏感信息的失败摘要。 */
    private final String errorMessage;

    public AutoAnnotationResult(AutoAnnotationStatus status, int recordCount,
                                int labeledCount, int failedCount,
                                int batchCount, Path workbookPath,
                                Path summaryPath, Path decisionsPath,
                                Path batchesPath, String errorMessage) {
        if (status == null || recordCount < 0 || labeledCount < 0
                || failedCount < 0 || batchCount < 0) {
            throw new IllegalArgumentException("自动标注结果统计无效");
        }
        this.status = status;
        this.recordCount = recordCount;
        this.labeledCount = labeledCount;
        this.failedCount = failedCount;
        this.batchCount = batchCount;
        this.workbookPath = normalize(workbookPath);
        this.summaryPath = normalize(summaryPath);
        this.decisionsPath = normalize(decisionsPath);
        this.batchesPath = normalize(batchesPath);
        this.errorMessage = errorMessage == null ? null : errorMessage.trim();
    }

    /**
     * 创建默认关闭结果。
     *
     * @return 关闭状态结果
     */
    public static AutoAnnotationResult disabled() {
        return new AutoAnnotationResult(AutoAnnotationStatus.DISABLED,
                0, 0, 0, 0, null, null, null, null, null);
    }

    /**
     * 生成 UDF 行尾部扩展字段。
     *
     * @return UDF 字段映射
     */
    public Map<String, Object> toUdfFields() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("autoAnnotationStatus", status.name());
        values.put("autoAnnotationExcelName", workbookPath == null ? null
                : workbookPath.getFileName().toString());
        values.put("autoAnnotationRecordCount", Long.valueOf(recordCount));
        values.put("autoAnnotationLabeledCount", Long.valueOf(labeledCount));
        values.put("autoAnnotationFailedCount", Long.valueOf(failedCount));
        values.put("autoAnnotationBatchCount", Integer.valueOf(batchCount));
        values.put("autoAnnotationReportName", summaryPath == null ? null
                : summaryPath.getFileName().toString());
        return values;
    }

    /**
     * 返回存在的自动标注审计文件，不包含工作簿。
     *
     * @return 审计文件列表
     */
    public List<Path> reportPaths() {
        List<Path> result = new ArrayList<Path>();
        add(result, summaryPath);
        add(result, decisionsPath);
        add(result, batchesPath);
        return Collections.unmodifiableList(result);
    }

    public AutoAnnotationStatus getStatus() { return status; }
    public int getRecordCount() { return recordCount; }
    public int getLabeledCount() { return labeledCount; }
    public int getFailedCount() { return failedCount; }
    public int getBatchCount() { return batchCount; }
    public Path getWorkbookPath() { return workbookPath; }
    public Path getSummaryPath() { return summaryPath; }
    public Path getDecisionsPath() { return decisionsPath; }
    public Path getBatchesPath() { return batchesPath; }
    public String getErrorMessage() { return errorMessage; }

    private static Path normalize(Path value) {
        return value == null ? null : value.toAbsolutePath().normalize();
    }

    private static void add(List<Path> values, Path value) {
        if (value != null) {
            values.add(value);
        }
    }
}
