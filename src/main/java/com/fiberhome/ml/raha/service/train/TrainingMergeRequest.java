package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 描述一次从持久化 c1 和当前 o1 生成训练快照的请求。
 */
public final class TrainingMergeRequest {

    /** 训练批次标识。 */
    private final String trainingBatchId;
    /** 当前训练读取的 o1 数据集。 */
    private final RahaDataset originalDataset;
    /** 与 c1 生成阶段相同的行身份配置。 */
    private final RowIdentityConfig rowIdentityConfig;
    /** c1 采样批次。 */
    private final String sampleBatchId;
    /** c1 月分区。 */
    private final String samplePartitionMonth;
    /** 选定的标注批次。 */
    private final String annotationBatchId;
    /** 标注记录月分区。 */
    private final String annotationPartitionMonth;
    /** c1 身份键允许广播的最大估算字节数。 */
    private final long broadcastThresholdBytes;

    public TrainingMergeRequest(String trainingBatchId,
                                RahaDataset originalDataset,
                                RowIdentityConfig rowIdentityConfig,
                                String sampleBatchId,
                                String samplePartitionMonth,
                                String annotationBatchId,
                                String annotationPartitionMonth) {
        this(trainingBatchId, originalDataset, rowIdentityConfig, sampleBatchId,
                samplePartitionMonth, annotationBatchId, annotationPartitionMonth,
                ResourceConfig.defaults().getBroadcastThresholdBytes());
    }

    public TrainingMergeRequest(String trainingBatchId,
                                RahaDataset originalDataset,
                                RowIdentityConfig rowIdentityConfig,
                                String sampleBatchId,
                                String samplePartitionMonth,
                                String annotationBatchId,
                                String annotationPartitionMonth,
                                long broadcastThresholdBytes) {
        this.trainingBatchId = ValueUtils.requireNotBlank(
                trainingBatchId, "训练批次标识");
        if (originalDataset == null || originalDataset.getDataFrame() == null
                || rowIdentityConfig == null) {
            throw new IllegalArgumentException("训练合并数据集和行身份配置不能为空");
        }
        this.originalDataset = originalDataset;
        this.rowIdentityConfig = rowIdentityConfig;
        this.sampleBatchId = ValueUtils.requireNotBlank(sampleBatchId,
                "训练采样批次");
        this.samplePartitionMonth = ValueUtils.requireNotBlank(
                samplePartitionMonth, "训练采样月分区");
        this.annotationBatchId = ValueUtils.requireNotBlank(annotationBatchId,
                "训练标注批次");
        this.annotationPartitionMonth = ValueUtils.requireNotBlank(
                annotationPartitionMonth, "训练标注月分区");
        if (broadcastThresholdBytes <= 0L) {
            throw new IllegalArgumentException("c1 广播阈值必须大于零");
        }
        this.broadcastThresholdBytes = broadcastThresholdBytes;
    }

    public String getTrainingBatchId() { return trainingBatchId; }
    public RahaDataset getOriginalDataset() { return originalDataset; }
    public RowIdentityConfig getRowIdentityConfig() { return rowIdentityConfig; }
    public String getSampleBatchId() { return sampleBatchId; }
    public String getSamplePartitionMonth() { return samplePartitionMonth; }
    public String getAnnotationBatchId() { return annotationBatchId; }
    public String getAnnotationPartitionMonth() { return annotationPartitionMonth; }
    public long getBroadcastThresholdBytes() { return broadcastThresholdBytes; }
}
