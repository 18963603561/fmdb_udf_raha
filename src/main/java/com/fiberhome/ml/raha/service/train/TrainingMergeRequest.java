package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    /** 按稳定顺序保存的一个或多个采样和标注批次引用。 */
    private final List<TrainingBatchReference> batchReferences;
    /** c1 身份键允许广播的最大估算字节数。 */
    private final long broadcastThresholdBytes;
    /** 是否只画像列批当前可检测字段。 */
    private final boolean columnBatchChild;

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
        this(trainingBatchId, originalDataset, rowIdentityConfig,
                Collections.singletonList(new TrainingBatchReference(
                        sampleBatchId, samplePartitionMonth, annotationBatchId,
                        annotationPartitionMonth)), broadcastThresholdBytes,
                false);
    }

    public TrainingMergeRequest(String trainingBatchId,
                                RahaDataset originalDataset,
                                RowIdentityConfig rowIdentityConfig,
                                List<TrainingBatchReference> batchReferences,
                                long broadcastThresholdBytes) {
        this(trainingBatchId, originalDataset, rowIdentityConfig,
                batchReferences, broadcastThresholdBytes, false);
    }

    public TrainingMergeRequest(String trainingBatchId,
                                RahaDataset originalDataset,
                                RowIdentityConfig rowIdentityConfig,
                                List<TrainingBatchReference> batchReferences,
                                long broadcastThresholdBytes,
                                boolean columnBatchChild) {
        this.trainingBatchId = ValueUtils.requireNotBlank(
                trainingBatchId, "训练批次标识");
        if (originalDataset == null || originalDataset.getDataFrame() == null
                || rowIdentityConfig == null) {
            throw new IllegalArgumentException("训练合并数据集和行身份配置不能为空");
        }
        this.originalDataset = originalDataset;
        this.rowIdentityConfig = rowIdentityConfig;
        this.batchReferences = immutableReferences(batchReferences);
        if (broadcastThresholdBytes <= 0L) {
            throw new IllegalArgumentException("c1 广播阈值必须大于零");
        }
        this.broadcastThresholdBytes = broadcastThresholdBytes;
        this.columnBatchChild = columnBatchChild;
    }

    public String getTrainingBatchId() { return trainingBatchId; }
    public RahaDataset getOriginalDataset() { return originalDataset; }
    public RowIdentityConfig getRowIdentityConfig() { return rowIdentityConfig; }
    public List<TrainingBatchReference> getBatchReferences() {
        return batchReferences;
    }
    public String getSampleBatchId() {
        return batchReferences.get(0).getSampleBatchId();
    }
    public String getSamplePartitionMonth() {
        return batchReferences.get(0).getSamplePartitionMonth();
    }
    public String getAnnotationBatchId() {
        return batchReferences.get(0).getAnnotationBatchId();
    }
    public String getAnnotationPartitionMonth() {
        return batchReferences.get(0).getAnnotationPartitionMonth();
    }
    public long getBroadcastThresholdBytes() { return broadcastThresholdBytes; }
    public boolean isColumnBatchChild() { return columnBatchChild; }

    private static List<TrainingBatchReference> immutableReferences(
            List<TrainingBatchReference> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("训练批次引用不能为空");
        }
        List<TrainingBatchReference> result =
                new ArrayList<TrainingBatchReference>(values.size());
        for (TrainingBatchReference value : values) {
            if (value == null) {
                throw new IllegalArgumentException("训练批次引用不能包含空值");
            }
            result.add(value);
        }
        return Collections.unmodifiableList(result);
    }
}
