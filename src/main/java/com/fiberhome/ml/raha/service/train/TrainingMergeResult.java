package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 返回 c1 优先去重后的训练快照、重映射标签和批次上下文。
 */
public final class TrainingMergeResult {

    /** 训练快照数据集。 */
    private final RahaDataset dataset;
    /** 已转换到训练快照坐标的直接标签。 */
    private final List<CellLabel> directLabels;
    /** 训练批次标识。 */
    private final String trainingBatchId;
    /** 新训练快照标识。 */
    private final String trainingSnapshotId;
    /** 合并规则版本。 */
    private final String mergeAlgorithmVersion;
    /** 合并数量指标。 */
    private final TrainingMergeMetrics metrics;
    /** 可持久化的训练来源上下文。 */
    private final Map<String, Object> trainingContext;

    public TrainingMergeResult(RahaDataset dataset,
                               List<CellLabel> directLabels,
                               String trainingBatchId,
                               String trainingSnapshotId,
                               String mergeAlgorithmVersion,
                               TrainingMergeMetrics metrics,
                               Map<String, Object> trainingContext) {
        if (dataset == null || directLabels == null || metrics == null
                || trainingContext == null) {
            throw new IllegalArgumentException("训练合并结果不能为空");
        }
        this.dataset = dataset;
        this.directLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(directLabels));
        this.trainingBatchId = ValueUtils.requireNotBlank(trainingBatchId,
                "训练批次标识");
        this.trainingSnapshotId = ValueUtils.requireNotBlank(trainingSnapshotId,
                "训练快照标识");
        this.mergeAlgorithmVersion = ValueUtils.requireNotBlank(
                mergeAlgorithmVersion, "训练合并版本");
        this.metrics = metrics;
        this.trainingContext = Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(trainingContext));
    }

    public RahaDataset getDataset() { return dataset; }
    public List<CellLabel> getDirectLabels() { return directLabels; }
    public String getTrainingBatchId() { return trainingBatchId; }
    public String getTrainingSnapshotId() { return trainingSnapshotId; }
    public String getMergeAlgorithmVersion() { return mergeAlgorithmVersion; }
    public TrainingMergeMetrics getMetrics() { return metrics; }
    public Map<String, Object> getTrainingContext() { return trainingContext; }
}
