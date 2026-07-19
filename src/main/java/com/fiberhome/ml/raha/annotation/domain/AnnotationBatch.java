package com.fiberhome.ml.raha.annotation.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 保存一次不可变标注导入批次及其全部有效行记录。
 */
public final class AnnotationBatch {

    /** 标注批次标识。 */
    private final String annotationBatchId;
    /** 来源采样批次。 */
    private final String sampleBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 批次状态。 */
    private final AnnotationBatchStatus status;
    /** 批次导入时间。 */
    private final long annotatedAt;
    /** UTC 月分区。 */
    private final String partitionMonth;
    /** 可选被修订批次。 */
    private final String supersedesBatchId;
    /** 有效行标注记录。 */
    private final List<AnnotationRecord> records;

    public AnnotationBatch(String annotationBatchId,
                           String sampleBatchId,
                           String datasetId,
                           AnnotationBatchStatus status,
                           long annotatedAt,
                           String partitionMonth,
                           String supersedesBatchId,
                           List<AnnotationRecord> records) {
        this.annotationBatchId = ValueUtils.requireNotBlank(
                annotationBatchId, "标注批次标识");
        this.sampleBatchId = ValueUtils.requireNotBlank(
                sampleBatchId, "来源采样批次");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "标注数据集标识");
        this.partitionMonth = ValueUtils.requireNotBlank(
                partitionMonth, "标注月分区");
        if ((status != AnnotationBatchStatus.IMPORTED
                && status != AnnotationBatchStatus.PARTIAL)
                || annotatedAt <= 0L || records == null || records.isEmpty()) {
            throw new IllegalArgumentException("标注批次状态、时间和记录必须有效");
        }
        this.status = status;
        this.annotatedAt = annotatedAt;
        this.supersedesBatchId = supersedesBatchId;
        Set<String> rowIds = new LinkedHashSet<String>();
        List<AnnotationRecord> copy =
                new ArrayList<AnnotationRecord>(records.size());
        AnnotationRecord expected = records.get(0);
        for (AnnotationRecord record : records) {
            if (record == null
                    || !annotationBatchId.equals(record.getAnnotationBatchId())
                    || !sampleBatchId.equals(record.getSampleBatchId())
                    || !datasetId.equals(record.getDatasetId())
                    || status != record.getBatchStatus()
                    || annotatedAt != record.getAnnotatedAt()
                    || !partitionMonth.equals(record.getPartitionMonth())
                    || !equalsNullable(supersedesBatchId,
                    record.getSupersedesBatchId())
                    || expected.getBatchRecordCount()
                    != record.getBatchRecordCount()
                    || expected.getValidRecordCount()
                    != record.getValidRecordCount()
                    || expected.getInvalidRecordCount()
                    != record.getInvalidRecordCount()
                    || !expected.getTemplateVersion().equals(
                    record.getTemplateVersion())
                    || !expected.getSchemaHash().equals(record.getSchemaHash())
                    || !expected.getFileName().equals(record.getFileName())
                    || !expected.getImportFingerprint().equals(
                    record.getImportFingerprint())
                    || !rowIds.add(record.getAnnotation().getRowId())) {
                throw new IllegalArgumentException("标注批次记录归属或行标识不一致");
            }
            copy.add(record);
        }
        if (expected.getValidRecordCount() != records.size()) {
            throw new IllegalArgumentException("标注批次有效记录数量与明细不一致");
        }
        this.records = Collections.unmodifiableList(copy);
    }

    public String getAnnotationBatchId() { return annotationBatchId; }
    public String getSampleBatchId() { return sampleBatchId; }
    public String getDatasetId() { return datasetId; }
    public AnnotationBatchStatus getStatus() { return status; }
    public long getAnnotatedAt() { return annotatedAt; }
    public String getPartitionMonth() { return partitionMonth; }
    public String getSupersedesBatchId() { return supersedesBatchId; }
    public List<AnnotationRecord> getRecords() { return records; }

    private static boolean equalsNullable(Object first, Object second) {
        return first == null ? second == null : first.equals(second);
    }
}
