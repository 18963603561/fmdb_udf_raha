package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.RowIdentityMode;
import com.fiberhome.ml.raha.train.TrainingMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 检测入口加载的不可变模型集合头。
 */
public final class RahaModelSet {

    /** 模型集合版本。 */
    private final String modelSetVersion;
    /** 请求指纹。 */
    private final String requestFingerprint;
    /** 数据集标识。 */
    private final String datasetId;
    /** 训练快照。 */
    private final String trainingSnapshotId;
    /** 参与训练的采样批次。 */
    private final List<String> sampleBatchIds;
    /** 训练模式。 */
    private final TrainingMode trainingMode;
    /** 可选父模型集合。 */
    private final String parentModelSetVersion;
    /** 模型集合完整字段。 */
    private final List<String> modelColumns;
    /** 本次训练字段。 */
    private final List<String> trainedColumns;
    /** 行身份模式。 */
    private final RowIdentityMode rowIdentityMode;
    /** 行键字段。 */
    private final List<String> rowKeyColumns;
    /** 模式哈希。 */
    private final String schemaHash;
    /** 算法版本。 */
    private final String algorithmVersion;
    /** 配置 JSON。 */
    private final String configJson;
    /** 策略计划版本。 */
    private final String strategyPlanVersion;
    /** 冻结策略计划 JSON。 */
    private final String strategyPlanJson;
    /** 值规范化版本。 */
    private final String normalizationVersion;
    /** 列模型数量。 */
    private final int modelCount;
    /** 完整训练样本数量。 */
    private final long trainingExampleCount;
    /** 提交时间。 */
    private final long createdAt;

    public RahaModelSet(String modelSetVersion, String requestFingerprint,
                        String datasetId, String trainingSnapshotId,
                        List<String> sampleBatchIds, TrainingMode trainingMode,
                        String parentModelSetVersion, List<String> modelColumns,
                        List<String> trainedColumns, RowIdentityMode rowIdentityMode,
                        List<String> rowKeyColumns, String schemaHash,
                        String algorithmVersion, String configJson,
                        String strategyPlanVersion, String strategyPlanJson,
                        String normalizationVersion, int modelCount,
                        long trainingExampleCount, long createdAt) {
        this.modelSetVersion = modelSetVersion;
        this.requestFingerprint = requestFingerprint;
        this.datasetId = datasetId;
        this.trainingSnapshotId = trainingSnapshotId;
        this.sampleBatchIds = immutable(sampleBatchIds);
        this.trainingMode = trainingMode;
        this.parentModelSetVersion = parentModelSetVersion;
        this.modelColumns = immutable(modelColumns);
        this.trainedColumns = immutable(trainedColumns);
        this.rowIdentityMode = rowIdentityMode;
        this.rowKeyColumns = immutable(rowKeyColumns);
        this.schemaHash = schemaHash;
        this.algorithmVersion = algorithmVersion;
        this.configJson = configJson;
        this.strategyPlanVersion = strategyPlanVersion;
        this.strategyPlanJson = strategyPlanJson;
        this.normalizationVersion = normalizationVersion;
        this.modelCount = modelCount;
        this.trainingExampleCount = trainingExampleCount;
        this.createdAt = createdAt;
    }

    private static List<String> immutable(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<String>(values));
    }

    public String getModelSetVersion() { return modelSetVersion; }
    public String getRequestFingerprint() { return requestFingerprint; }
    public String getDatasetId() { return datasetId; }
    public String getTrainingSnapshotId() { return trainingSnapshotId; }
    public List<String> getSampleBatchIds() { return sampleBatchIds; }
    public TrainingMode getTrainingMode() { return trainingMode; }
    public String getParentModelSetVersion() { return parentModelSetVersion; }
    public List<String> getModelColumns() { return modelColumns; }
    public List<String> getTrainedColumns() { return trainedColumns; }
    public RowIdentityMode getRowIdentityMode() { return rowIdentityMode; }
    public List<String> getRowKeyColumns() { return rowKeyColumns; }
    public String getSchemaHash() { return schemaHash; }
    public String getAlgorithmVersion() { return algorithmVersion; }
    public String getConfigJson() { return configJson; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public String getStrategyPlanJson() { return strategyPlanJson; }
    public String getNormalizationVersion() { return normalizationVersion; }
    public int getModelCount() { return modelCount; }
    public long getTrainingExampleCount() { return trainingExampleCount; }
    public long getCreatedAt() { return createdAt; }
}
