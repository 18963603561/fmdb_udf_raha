package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一次训练引用的采样批次、标注批次及其物理月份分区。
 */
public final class TrainingBatchReference {

    /** 采样批次标识。 */
    private final String sampleBatchId;
    /** 采样记录月份分区。 */
    private final String samplePartitionMonth;
    /** 选定的标注批次标识。 */
    private final String annotationBatchId;
    /** 标注记录月份分区。 */
    private final String annotationPartitionMonth;

    public TrainingBatchReference(String sampleBatchId,
                                  String samplePartitionMonth,
                                  String annotationBatchId,
                                  String annotationPartitionMonth) {
        this.sampleBatchId = ValueUtils.requireNotBlank(
                sampleBatchId, "训练采样批次");
        this.samplePartitionMonth = ValueUtils.requireNotBlank(
                samplePartitionMonth, "训练采样月份分区");
        this.annotationBatchId = ValueUtils.requireNotBlank(
                annotationBatchId, "训练标注批次");
        this.annotationPartitionMonth = ValueUtils.requireNotBlank(
                annotationPartitionMonth, "训练标注月份分区");
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public String getSamplePartitionMonth() { return samplePartitionMonth; }
    public String getAnnotationBatchId() { return annotationBatchId; }
    public String getAnnotationPartitionMonth() {
        return annotationPartitionMonth;
    }

    /**
     * 返回进入训练版本和幂等语义的稳定文本。
     *
     * @return 不依赖对象哈希码的批次文本
     */
    public String toCanonicalString() {
        return sampleBatchId.length() + ":" + sampleBatchId
                + samplePartitionMonth.length() + ":" + samplePartitionMonth
                + annotationBatchId.length() + ":" + annotationBatchId
                + annotationPartitionMonth.length() + ":"
                + annotationPartitionMonth;
    }
}
