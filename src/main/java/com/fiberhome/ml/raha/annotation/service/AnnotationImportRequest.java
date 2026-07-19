package com.fiberhome.ml.raha.annotation.service;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.nio.file.Path;

/**
 * 描述 Excel 标注导入、错误工作簿和可选修订关系。
 */
public final class AnnotationImportRequest {

    /** 数据集标识。 */
    private final String datasetId;
    /** c1 月分区。 */
    private final String samplePartitionMonth;
    /** c1 采样批次。 */
    private final String sampleBatchId;
    /** 用户上传 Excel 文件。 */
    private final Path inputPath;
    /** 错误行回写文件，可为空。 */
    private final Path validationOutputPath;
    /** 可选标注人员。 */
    private final String annotator;
    /** 是否允许直接导入没有任务标识的标注行。 */
    private final boolean allowBlankTaskId;
    /** 可选被修订标注批次。 */
    private final String supersedesBatchId;
    /** 被修订批次所在月分区。 */
    private final String supersedesPartitionMonth;

    public AnnotationImportRequest(String datasetId,
                                   String samplePartitionMonth,
                                   String sampleBatchId,
                                   Path inputPath,
                                   Path validationOutputPath,
                                   String annotator,
                                   boolean allowBlankTaskId,
                                   String supersedesBatchId,
                                   String supersedesPartitionMonth) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "标注数据集标识");
        this.samplePartitionMonth = ValueUtils.requireNotBlank(
                samplePartitionMonth, "标注采样月分区");
        this.sampleBatchId = ValueUtils.requireNotBlank(
                sampleBatchId, "标注采样批次");
        if (inputPath == null) {
            throw new IllegalArgumentException("标注输入文件不能为空");
        }
        this.inputPath = inputPath.toAbsolutePath().normalize();
        this.validationOutputPath = validationOutputPath == null ? null
                : validationOutputPath.toAbsolutePath().normalize();
        this.annotator = annotator;
        this.allowBlankTaskId = allowBlankTaskId;
        this.supersedesBatchId = supersedesBatchId;
        this.supersedesPartitionMonth = supersedesPartitionMonth;
        if ((supersedesBatchId == null) != (supersedesPartitionMonth == null)) {
            throw new IllegalArgumentException("修订批次和月分区必须同时提供");
        }
    }

    public String getDatasetId() { return datasetId; }
    public String getSamplePartitionMonth() { return samplePartitionMonth; }
    public String getSampleBatchId() { return sampleBatchId; }
    public Path getInputPath() { return inputPath; }
    public Path getValidationOutputPath() { return validationOutputPath; }
    public String getAnnotator() { return annotator; }
    public boolean isAllowBlankTaskId() { return allowBlankTaskId; }
    public String getSupersedesBatchId() { return supersedesBatchId; }
    public String getSupersedesPartitionMonth() {
        return supersedesPartitionMonth;
    }
}
