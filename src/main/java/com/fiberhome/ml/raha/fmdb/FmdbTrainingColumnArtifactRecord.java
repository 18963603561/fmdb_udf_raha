package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.LinkedHashMap;
import java.util.Map;

/** 一批一列的训练画像、策略、字典、聚类和传播产物记录。 */
public final class FmdbTrainingColumnArtifactRecord {

    /** 训练批次标识。 */
    private final String trainingBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 外部输入来源版本。 */
    private final String sourceVersion;
    /** 训练输入模式哈希。 */
    private final String schemaHash;
    /** c1 优先合并算法版本。 */
    private final String mergeAlgorithmVersion;
    /** 训练批次上下文 JSON。 */
    private final String trainingContextJson;
    /** 目标字段。 */
    private final String columnName;
    /** 训练画像版本。 */
    private final String profileVersion;
    /** 训练画像 JSON。 */
    private final String profileJson;
    /** 策略计划版本。 */
    private final String strategyPlanVersion;
    /** 字段策略计划 JSON。 */
    private final String strategyPlanJson;
    /** 特征字典版本。 */
    private final String featureDictionaryVersion;
    /** 特征字典 JSON。 */
    private final String featureDictionaryJson;
    /** 聚类版本。 */
    private final String clusterVersion;
    /** 聚类摘要 JSON。 */
    private final String clusterSummaryJson;
    /** 标签传播摘要 JSON。 */
    private final String propagationSummaryJson;
    /** 产物创建时间。 */
    private final long createdAt;

    public FmdbTrainingColumnArtifactRecord(String trainingBatchId,
                                            String datasetId,
                                            String sourceVersion,
                                            String schemaHash,
                                            String mergeAlgorithmVersion,
                                            String trainingContextJson,
                                            String columnName,
                                            String profileVersion,
                                            String profileJson,
                                            String strategyPlanVersion,
                                            String strategyPlanJson,
                                            String featureDictionaryVersion,
                                            String featureDictionaryJson,
                                            String clusterVersion,
                                            String clusterSummaryJson,
                                            String propagationSummaryJson,
                                            long createdAt) {
        this.trainingBatchId = ValueUtils.requireNotBlank(trainingBatchId,
                "训练列产物批次");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "训练列产物数据集");
        this.sourceVersion = sourceVersion;
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "训练列产物模式");
        this.mergeAlgorithmVersion = ValueUtils.requireNotBlank(
                mergeAlgorithmVersion, "训练列产物合并版本");
        this.trainingContextJson = ValueUtils.requireNotBlank(
                trainingContextJson, "训练列产物上下文");
        this.columnName = ValueUtils.requireNotBlank(columnName, "训练列产物字段");
        this.profileVersion = profileVersion;
        this.profileJson = profileJson;
        this.strategyPlanVersion = strategyPlanVersion;
        this.strategyPlanJson = strategyPlanJson;
        this.featureDictionaryVersion = featureDictionaryVersion;
        this.featureDictionaryJson = featureDictionaryJson;
        this.clusterVersion = clusterVersion;
        this.clusterSummaryJson = clusterSummaryJson;
        this.propagationSummaryJson = propagationSummaryJson;
        if (createdAt <= 0L) {
            throw new IllegalArgumentException("训练列产物创建时间必须大于零");
        }
        this.createdAt = createdAt;
    }

    public String getTrainingBatchId() { return trainingBatchId; }
    public String getDatasetId() { return datasetId; }
    public String getSourceVersion() { return sourceVersion; }
    public String getSchemaHash() { return schemaHash; }
    public String getMergeAlgorithmVersion() { return mergeAlgorithmVersion; }
    public String getTrainingContextJson() { return trainingContextJson; }
    public String getColumnName() { return columnName; }
    public String getProfileVersion() { return profileVersion; }
    public String getProfileJson() { return profileJson; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public String getStrategyPlanJson() { return strategyPlanJson; }
    public String getFeatureDictionaryVersion() { return featureDictionaryVersion; }
    public String getFeatureDictionaryJson() { return featureDictionaryJson; }
    public String getClusterVersion() { return clusterVersion; }
    public String getClusterSummaryJson() { return clusterSummaryJson; }
    public String getPropagationSummaryJson() { return propagationSummaryJson; }
    public long getCreatedAt() { return createdAt; }

    Map<String, Object> values() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("training_batch_id", trainingBatchId);
        values.put("dataset_id", datasetId);
        values.put("source_version", sourceVersion);
        values.put("schema_hash", schemaHash);
        values.put("merge_algorithm_version", mergeAlgorithmVersion);
        values.put("training_context_json", trainingContextJson);
        values.put("column_name", columnName);
        values.put("profile_version", profileVersion);
        values.put("profile_json", profileJson);
        values.put("strategy_plan_version", strategyPlanVersion);
        values.put("strategy_plan_json", strategyPlanJson);
        values.put("feature_dictionary_version", featureDictionaryVersion);
        values.put("feature_dictionary_json", featureDictionaryJson);
        values.put("cluster_version", clusterVersion);
        values.put("cluster_summary_json", clusterSummaryJson);
        values.put("propagation_summary_json", propagationSummaryJson);
        values.put("created_at", createdAt);
        return values;
    }
}
