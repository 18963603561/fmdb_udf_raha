package com.fiberhome.ml.raha.model.domain;

import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存列模型写入最终模型表所需的训练批次和模型集合上下文。
 */
public final class ModelPersistenceContext {

    /** 模型集合版本。 */
    private final String modelSetVersion;
    /** 数据集标识。 */
    private final String datasetId;
    /** 模型训练输入模式哈希。 */
    private final String schemaHash;
    /** 训练批次标识。 */
    private final String trainingBatchId;
    /** 模型集合状态。 */
    private final ModelStatus status;
    /** 策略计划版本。 */
    private final String strategyPlanVersion;
    /** 合并算法版本。 */
    private final String mergeAlgorithmVersion;
    /** 不含原始训练值的模型指标。 */
    private final Map<String, Double> metrics;
    /** 模型创建时间。 */
    private final long createdAt;
    /** 首次发布时间，未发布时为空。 */
    private final Long publishedAt;
    /** 模型集合继承的行身份规则。 */
    private final RowIdentityConfig rowIdentityConfig;

    public ModelPersistenceContext(String modelSetVersion,
                                   String datasetId,
                                   String trainingBatchId,
                                   ModelStatus status,
                                   String strategyPlanVersion,
                                   String mergeAlgorithmVersion,
                                   Map<String, Double> metrics,
                                   long createdAt,
                                   Long publishedAt) {
        this(modelSetVersion, datasetId, "unknown", trainingBatchId, status,
                strategyPlanVersion, mergeAlgorithmVersion, metrics, createdAt,
                publishedAt, RowIdentityConfig.contentHash());
    }

    public ModelPersistenceContext(String modelSetVersion,
                                   String datasetId,
                                   String schemaHash,
                                   String trainingBatchId,
                                   ModelStatus status,
                                   String strategyPlanVersion,
                                   String mergeAlgorithmVersion,
                                   Map<String, Double> metrics,
                                   long createdAt,
                                   Long publishedAt) {
        this(modelSetVersion, datasetId, schemaHash, trainingBatchId, status,
                strategyPlanVersion, mergeAlgorithmVersion, metrics, createdAt,
                publishedAt, RowIdentityConfig.contentHash());
    }

    public ModelPersistenceContext(String modelSetVersion,
                                   String datasetId,
                                   String schemaHash,
                                   String trainingBatchId,
                                   ModelStatus status,
                                   String strategyPlanVersion,
                                   String mergeAlgorithmVersion,
                                   Map<String, Double> metrics,
                                   long createdAt,
                                   Long publishedAt,
                                   RowIdentityConfig rowIdentityConfig) {
        this.modelSetVersion = ValueUtils.requireNotBlank(
                modelSetVersion, "模型集合版本");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "模型数据集标识");
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "模型模式哈希");
        this.trainingBatchId = ValueUtils.requireNotBlank(
                trainingBatchId, "模型训练批次标识");
        if (status == null || createdAt <= 0L
                || (publishedAt != null && publishedAt <= 0L)) {
            throw new IllegalArgumentException("模型状态和时间必须有效");
        }
        if (status == ModelStatus.PUBLISHED && publishedAt == null) {
            throw new IllegalArgumentException("已发布模型集合必须包含发布时间");
        }
        this.status = status;
        this.strategyPlanVersion = ValueUtils.requireNotBlank(
                strategyPlanVersion, "模型策略计划版本");
        this.mergeAlgorithmVersion = ValueUtils.requireNotBlank(
                mergeAlgorithmVersion, "模型合并算法版本");
        this.metrics = metrics == null ? Collections.<String, Double>emptyMap()
                : Collections.unmodifiableMap(
                new LinkedHashMap<String, Double>(metrics));
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
        if (rowIdentityConfig == null) {
            throw new IllegalArgumentException("模型集合行身份规则不能为空");
        }
        this.rowIdentityConfig = rowIdentityConfig;
    }

    public String getModelSetVersion() { return modelSetVersion; }
    public String getDatasetId() { return datasetId; }
    public String getSchemaHash() { return schemaHash; }
    public String getTrainingBatchId() { return trainingBatchId; }
    public ModelStatus getStatus() { return status; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public String getMergeAlgorithmVersion() { return mergeAlgorithmVersion; }
    public Map<String, Double> getMetrics() { return metrics; }
    public long getCreatedAt() { return createdAt; }
    public Long getPublishedAt() { return publishedAt; }
    public RowIdentityConfig getRowIdentityConfig() { return rowIdentityConfig; }
}
