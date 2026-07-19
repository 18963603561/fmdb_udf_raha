package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.LinkedHashMap;
import java.util.Map;

/** 保存一个训练快照单元格的特征、聚类和标签状态。 */
public final class FmdbTrainingCellRecord {

    /** 训练批次标识。 */
    private final String trainingBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 合并训练快照标识。 */
    private final String trainingSnapshotId;
    /** 逻辑行标识。 */
    private final String rowId;
    /** 目标字段。 */
    private final String columnName;
    /** 稳定单元格标识。 */
    private final String cellId;
    /** 可信训练快照中的原始值。 */
    private final String cellValue;
    /** 特征字典版本。 */
    private final String featureDictionaryVersion;
    /** 稀疏特征向量 JSON。 */
    private final String featureVectorJson;
    /** 特征解释摘要 JSON。 */
    private final String featureSummaryJson;
    /** 所属聚类标识。 */
    private final String clusterId;
    /** 到聚类中心的距离。 */
    private final Double clusterDistance;
    /** 直接标签。 */
    private final Integer directLabel;
    /** 传播标签。 */
    private final Integer propagatedLabel;
    /** 当前生效标签来源。 */
    private final String labelSource;
    /** 来源标注批次。 */
    private final String sourceAnnotationBatchId;
    /** 标签样本权重。 */
    private final Double sampleWeight;
    /** 物化时间。 */
    private final long createdAt;

    public FmdbTrainingCellRecord(String trainingBatchId,
                                  String datasetId,
                                  String trainingSnapshotId,
                                  String rowId,
                                  String columnName,
                                  String cellId,
                                  String cellValue,
                                  String featureDictionaryVersion,
                                  String featureVectorJson,
                                  String featureSummaryJson,
                                  String clusterId,
                                  Double clusterDistance,
                                  Integer directLabel,
                                  Integer propagatedLabel,
                                  String labelSource,
                                  String sourceAnnotationBatchId,
                                  Double sampleWeight,
                                  long createdAt) {
        this.trainingBatchId = ValueUtils.requireNotBlank(trainingBatchId,
                "训练单元格批次");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "训练单元格数据集");
        this.trainingSnapshotId = ValueUtils.requireNotBlank(trainingSnapshotId,
                "训练单元格快照");
        this.rowId = ValueUtils.requireNotBlank(rowId, "训练单元格行标识");
        this.columnName = ValueUtils.requireNotBlank(columnName, "训练单元格字段");
        this.cellId = ValueUtils.requireNotBlank(cellId, "训练单元格标识");
        this.featureDictionaryVersion = ValueUtils.requireNotBlank(
                featureDictionaryVersion, "训练单元格字典版本");
        this.featureVectorJson = ValueUtils.requireNotBlank(featureVectorJson,
                "训练单元格特征向量");
        this.featureSummaryJson = ValueUtils.requireNotBlank(featureSummaryJson,
                "训练单元格特征摘要");
        validateLabel(directLabel, "直接标签");
        validateLabel(propagatedLabel, "传播标签");
        if (clusterDistance != null && (Double.isNaN(clusterDistance)
                || Double.isInfinite(clusterDistance) || clusterDistance < 0.0d)) {
            throw new IllegalArgumentException("训练单元格聚类距离非法");
        }
        if (sampleWeight != null && (Double.isNaN(sampleWeight)
                || Double.isInfinite(sampleWeight) || sampleWeight <= 0.0d)) {
            throw new IllegalArgumentException("训练单元格样本权重非法");
        }
        if (createdAt <= 0L) {
            throw new IllegalArgumentException("训练单元格创建时间必须大于零");
        }
        this.cellValue = cellValue;
        this.clusterId = clusterId;
        this.clusterDistance = clusterDistance;
        this.directLabel = directLabel;
        this.propagatedLabel = propagatedLabel;
        this.labelSource = labelSource;
        this.sourceAnnotationBatchId = sourceAnnotationBatchId;
        this.sampleWeight = sampleWeight;
        this.createdAt = createdAt;
    }

    private static void validateLabel(Integer value, String name) {
        if (value != null && value.intValue() != 0 && value.intValue() != 1) {
            throw new IllegalArgumentException(name + "只能为 0 或 1");
        }
    }

    public String getTrainingBatchId() { return trainingBatchId; }
    public String getDatasetId() { return datasetId; }
    public String getTrainingSnapshotId() { return trainingSnapshotId; }
    public String getRowId() { return rowId; }
    public String getColumnName() { return columnName; }
    public String getCellId() { return cellId; }
    public String getCellValue() { return cellValue; }
    public String getFeatureDictionaryVersion() { return featureDictionaryVersion; }
    public String getFeatureVectorJson() { return featureVectorJson; }
    public String getFeatureSummaryJson() { return featureSummaryJson; }
    public String getClusterId() { return clusterId; }
    public Double getClusterDistance() { return clusterDistance; }
    public Integer getDirectLabel() { return directLabel; }
    public Integer getPropagatedLabel() { return propagatedLabel; }
    public String getLabelSource() { return labelSource; }
    public String getSourceAnnotationBatchId() { return sourceAnnotationBatchId; }
    public Double getSampleWeight() { return sampleWeight; }
    public long getCreatedAt() { return createdAt; }

    Map<String, Object> values() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("training_batch_id", trainingBatchId);
        values.put("dataset_id", datasetId);
        values.put("training_snapshot_id", trainingSnapshotId);
        values.put("row_id", rowId);
        values.put("column_name", columnName);
        values.put("cell_id", cellId);
        values.put("cell_value", cellValue);
        values.put("feature_dictionary_version", featureDictionaryVersion);
        values.put("feature_vector_json", featureVectorJson);
        values.put("feature_summary_json", featureSummaryJson);
        values.put("cluster_id", clusterId);
        values.put("cluster_distance", clusterDistance);
        values.put("direct_label", directLabel);
        values.put("propagated_label", propagatedLabel);
        values.put("label_source", labelSource);
        values.put("source_annotation_batch_id", sourceAnnotationBatchId);
        values.put("sample_weight", sampleWeight);
        values.put("created_at", createdAt);
        return values;
    }
}
