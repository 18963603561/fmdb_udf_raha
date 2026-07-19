package com.fiberhome.ml.raha.annotation.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存最终标注宽表中的一条不可变行标注记录。
 */
public final class AnnotationRecord {

    /** 标注批次标识。 */
    private final String annotationBatchId;
    /** 来源采样批次。 */
    private final String sampleBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 用户整行标注。 */
    private final RowAnnotation annotation;
    /** 从可信 c1 回填的原始行。 */
    private final Map<String, Object> rowData;
    /** 模板版本。 */
    private final String templateVersion;
    /** 导入文件名。 */
    private final String fileName;
    /** 字段模式哈希。 */
    private final String schemaHash;
    /** 可选标注人员。 */
    private final String annotator;
    /** 批次状态，仅允许已导入或部分导入。 */
    private final AnnotationBatchStatus batchStatus;
    /** 文件标注行总数。 */
    private final long batchRecordCount;
    /** 有效标注行数量。 */
    private final long validRecordCount;
    /** 无效标注行数量。 */
    private final long invalidRecordCount;
    /** 可选被修订标注批次。 */
    private final String supersedesBatchId;
    /** 导入时间。 */
    private final long annotatedAt;
    /** UTC 月分区。 */
    private final String partitionMonth;
    /** 导入文件内容指纹。 */
    private final String importFingerprint;

    public AnnotationRecord(String annotationBatchId,
                            String sampleBatchId,
                            String datasetId,
                            RowAnnotation annotation,
                            Map<String, Object> rowData,
                            String templateVersion,
                            String fileName,
                            String schemaHash,
                            String annotator,
                            AnnotationBatchStatus batchStatus,
                            long batchRecordCount,
                            long validRecordCount,
                            long invalidRecordCount,
                            String supersedesBatchId,
                            long annotatedAt,
                            String partitionMonth,
                            String importFingerprint) {
        this.annotationBatchId = ValueUtils.requireNotBlank(
                annotationBatchId, "标注批次标识");
        this.sampleBatchId = ValueUtils.requireNotBlank(
                sampleBatchId, "来源采样批次");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "标注数据集标识");
        if (annotation == null || rowData == null || rowData.isEmpty()) {
            throw new IllegalArgumentException("整行标注和可信原始行不能为空");
        }
        this.annotation = annotation;
        this.rowData = Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(rowData));
        this.templateVersion = ValueUtils.requireNotBlank(
                templateVersion, "标注模板版本");
        this.fileName = ValueUtils.requireNotBlank(fileName, "标注文件名");
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "标注模式哈希");
        this.annotator = annotator;
        if (batchStatus != AnnotationBatchStatus.IMPORTED
                && batchStatus != AnnotationBatchStatus.PARTIAL) {
            throw new IllegalArgumentException("物理标注记录状态必须可入库");
        }
        if (batchRecordCount <= 0L || validRecordCount <= 0L
                || invalidRecordCount < 0L
                || validRecordCount + invalidRecordCount != batchRecordCount
                || annotatedAt <= 0L) {
            throw new IllegalArgumentException("标注批次数量和时间不一致");
        }
        this.batchStatus = batchStatus;
        this.batchRecordCount = batchRecordCount;
        this.validRecordCount = validRecordCount;
        this.invalidRecordCount = invalidRecordCount;
        this.supersedesBatchId = supersedesBatchId;
        this.annotatedAt = annotatedAt;
        this.partitionMonth = ValueUtils.requireNotBlank(
                partitionMonth, "标注月分区");
        this.importFingerprint = ValueUtils.requireNotBlank(
                importFingerprint, "标注导入指纹");
    }

    public String getAnnotationBatchId() { return annotationBatchId; }
    public String getSampleBatchId() { return sampleBatchId; }
    public String getDatasetId() { return datasetId; }
    public RowAnnotation getAnnotation() { return annotation; }
    public Map<String, Object> getRowData() { return rowData; }
    public String getTemplateVersion() { return templateVersion; }
    public String getFileName() { return fileName; }
    public String getSchemaHash() { return schemaHash; }
    public String getAnnotator() { return annotator; }
    public AnnotationBatchStatus getBatchStatus() { return batchStatus; }
    public long getBatchRecordCount() { return batchRecordCount; }
    public long getValidRecordCount() { return validRecordCount; }
    public long getInvalidRecordCount() { return invalidRecordCount; }
    public String getSupersedesBatchId() { return supersedesBatchId; }
    public long getAnnotatedAt() { return annotatedAt; }
    public String getPartitionMonth() { return partitionMonth; }
    public String getImportFingerprint() { return importFingerprint; }
}
