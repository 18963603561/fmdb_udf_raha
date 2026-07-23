package com.fiberhome.ml.raha.annotation.auto;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 描述自动标注的输入工作簿、输出位置和数据归属。
 */
public final class AutoAnnotationRequest {

    /** 原始空白标注工作簿。 */
    private final Path inputWorkbook;
    /** 自动标注工作簿目标路径。 */
    private final Path outputWorkbook;
    /** 自动标注审计文件目录。 */
    private final Path reportDirectory;
    /** 数据集标识。 */
    private final String datasetId;
    /** 采样批次标识。 */
    private final String sampleBatchId;
    /** 不允许向模型发送原值的字段。 */
    private final Set<String> sensitiveColumns;

    public AutoAnnotationRequest(Path inputWorkbook, Path outputWorkbook,
                                 Path reportDirectory, String datasetId,
                                 String sampleBatchId,
                                 Set<String> sensitiveColumns) {
        if (inputWorkbook == null || outputWorkbook == null
                || reportDirectory == null || isBlank(datasetId)
                || isBlank(sampleBatchId)) {
            throw new IllegalArgumentException("自动标注请求字段不能为空");
        }
        this.inputWorkbook = inputWorkbook.toAbsolutePath().normalize();
        this.outputWorkbook = outputWorkbook.toAbsolutePath().normalize();
        this.reportDirectory = reportDirectory.toAbsolutePath().normalize();
        this.datasetId = datasetId.trim();
        this.sampleBatchId = sampleBatchId.trim();
        this.sensitiveColumns = sensitiveColumns == null
                ? Collections.<String>emptySet()
                : Collections.unmodifiableSet(
                new LinkedHashSet<String>(sensitiveColumns));
    }

    public Path getInputWorkbook() { return inputWorkbook; }
    public Path getOutputWorkbook() { return outputWorkbook; }
    public Path getReportDirectory() { return reportDirectory; }
    public String getDatasetId() { return datasetId; }
    public String getSampleBatchId() { return sampleBatchId; }
    public Set<String> getSensitiveColumns() { return sensitiveColumns; }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
