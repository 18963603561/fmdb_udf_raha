package com.fiberhome.ml.raha.annotation.service;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.nio.file.Path;

/**
 * 描述从指定 c1 批次导出 Excel 标注模板所需的分区和目标文件。
 */
public final class AnnotationTemplateRequest {

    /** 数据集标识。 */
    private final String datasetId;
    /** c1 月分区。 */
    private final String samplePartitionMonth;
    /** c1 采样批次。 */
    private final String sampleBatchId;
    /** 采样快照标识，用于生成训练请求示例。 */
    private final String snapshotId;
    /** 模板输出路径。 */
    private final Path outputPath;

    public AnnotationTemplateRequest(String datasetId,
                                     String samplePartitionMonth,
                                     String sampleBatchId,
                                     Path outputPath) {
        this(datasetId, samplePartitionMonth, sampleBatchId, null, outputPath);
    }

    public AnnotationTemplateRequest(String datasetId,
                                     String samplePartitionMonth,
                                     String sampleBatchId,
                                     String snapshotId,
                                     Path outputPath) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "模板数据集标识");
        this.samplePartitionMonth = ValueUtils.requireNotBlank(
                samplePartitionMonth, "模板采样月分区");
        this.sampleBatchId = ValueUtils.requireNotBlank(
                sampleBatchId, "模板采样批次");
        this.snapshotId = snapshotId == null || snapshotId.trim().isEmpty()
                ? null : snapshotId.trim();
        if (outputPath == null) {
            throw new IllegalArgumentException("模板输出路径不能为空");
        }
        this.outputPath = outputPath.toAbsolutePath().normalize();
    }

    public String getDatasetId() { return datasetId; }
    public String getSamplePartitionMonth() { return samplePartitionMonth; }
    public String getSampleBatchId() { return sampleBatchId; }
    public String getSnapshotId() { return snapshotId; }
    public Path getOutputPath() { return outputPath; }
}
