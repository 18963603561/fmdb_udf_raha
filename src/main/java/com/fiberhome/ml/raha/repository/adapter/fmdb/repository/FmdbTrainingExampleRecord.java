package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.LinkedHashMap;
import java.util.Map;

/** 保存一个实际进入列模型训练的最终样本。 */
public final class FmdbTrainingExampleRecord {

    /** 模型集合版本。 */
    private final String modelSetVersion;
    /** 训练批次标识。 */
    private final String trainingBatchId;
    /** 数据集标识。 */
    private final String datasetId;
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
    /** 最终稀疏特征向量 JSON。 */
    private final String featureVectorJson;
    /** 最终零一标签。 */
    private final int label;
    /** 最终标签来源。 */
    private final String labelSource;
    /** 来源标注批次。 */
    private final String sourceAnnotationBatchId;
    /** 实际入模样本权重。 */
    private final double sampleWeight;
    /** 所属聚类标识。 */
    private final String clusterId;
    /** 冻结时间。 */
    private final long createdAt;
    /** 月分区。 */
    private final String partitionMonth;

    public FmdbTrainingExampleRecord(String modelSetVersion,
                                     String trainingBatchId,
                                     String datasetId,
                                     String rowId,
                                     String columnName,
                                     String cellId,
                                     String cellValue,
                                     String featureDictionaryVersion,
                                     String featureVectorJson,
                                     int label,
                                     String labelSource,
                                     String sourceAnnotationBatchId,
                                     double sampleWeight,
                                     String clusterId,
                                     long createdAt,
                                     String partitionMonth) {
        this.modelSetVersion = ValueUtils.requireNotBlank(modelSetVersion,
                "训练样本模型集合");
        this.trainingBatchId = ValueUtils.requireNotBlank(trainingBatchId,
                "训练样本批次");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "训练样本数据集");
        this.rowId = ValueUtils.requireNotBlank(rowId, "训练样本行标识");
        this.columnName = ValueUtils.requireNotBlank(columnName, "训练样本字段");
        this.cellId = ValueUtils.requireNotBlank(cellId, "训练样本单元格");
        this.featureDictionaryVersion = ValueUtils.requireNotBlank(
                featureDictionaryVersion, "训练样本字典版本");
        this.featureVectorJson = ValueUtils.requireNotBlank(featureVectorJson,
                "训练样本特征向量");
        if (label != 0 && label != 1 || labelSource == null
                || labelSource.trim().isEmpty() || Double.isNaN(sampleWeight)
                || Double.isInfinite(sampleWeight) || sampleWeight <= 0.0d
                || createdAt <= 0L) {
            throw new IllegalArgumentException("训练样本标签、来源、权重和时间非法");
        }
        this.cellValue = cellValue;
        this.label = label;
        this.labelSource = labelSource;
        this.sourceAnnotationBatchId = sourceAnnotationBatchId;
        this.sampleWeight = sampleWeight;
        this.clusterId = clusterId;
        this.createdAt = createdAt;
        this.partitionMonth = ValueUtils.requireNotBlank(partitionMonth,
                "训练样本月分区");
    }

    public String getModelSetVersion() { return modelSetVersion; }
    public String getTrainingBatchId() { return trainingBatchId; }
    public String getDatasetId() { return datasetId; }
    public String getRowId() { return rowId; }
    public String getColumnName() { return columnName; }
    public String getCellId() { return cellId; }
    public String getCellValue() { return cellValue; }
    public String getFeatureDictionaryVersion() { return featureDictionaryVersion; }
    public String getFeatureVectorJson() { return featureVectorJson; }
    public int getLabel() { return label; }
    public String getLabelSource() { return labelSource; }
    public String getSourceAnnotationBatchId() { return sourceAnnotationBatchId; }
    public double getSampleWeight() { return sampleWeight; }
    public String getClusterId() { return clusterId; }
    public long getCreatedAt() { return createdAt; }
    public String getPartitionMonth() { return partitionMonth; }

    Map<String, Object> values() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("model_set_version", modelSetVersion);
        values.put("training_batch_id", trainingBatchId);
        values.put("dataset_id", datasetId);
        values.put("row_id", rowId);
        values.put("column_name", columnName);
        values.put("cell_id", cellId);
        values.put("cell_value", cellValue);
        values.put("feature_dictionary_version", featureDictionaryVersion);
        values.put("feature_vector_json", featureVectorJson);
        values.put("label", label);
        values.put("label_source", labelSource);
        values.put("source_annotation_batch_id", sourceAnnotationBatchId);
        values.put("sample_weight", sampleWeight);
        values.put("cluster_id", clusterId);
        values.put("created_at", createdAt);
        values.put("partition_month", partitionMonth);
        return values;
    }
}
