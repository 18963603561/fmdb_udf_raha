package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.ModelStatus;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存列级检测模型及其策略、特征和模式版本关系。
 */
public final class RahaColumnModel {

    /** 模型逻辑名称。 */
    private final String modelName;
    /** 不可变模型版本。 */
    private final String modelVersion;
    /** 训练数据集标识。 */
    private final String datasetId;
    /** 模型目标字段。 */
    private final String columnName;
    /** 训练输入模式哈希。 */
    private final String schemaHash;
    /** 分类器类型。 */
    private final ClassifierType classifierType;
    /** 训练使用的特征字典版本。 */
    private final String featureDictionaryVersion;
    /** 训练使用的策略计划版本。 */
    private final String strategyPlanVersion;
    /** 疑似错误判断阈值。 */
    private final double threshold;
    /** 模型文件存储路径。 */
    private final String modelPath;
    /** 模型生命周期状态。 */
    private final ModelStatus status;
    /** 模型评估指标。 */
    private final Map<String, Double> metrics;
    /** 模型创建时间。 */
    private final long createdAt;
    /** 模型首次发布时间，从未发布时为空。 */
    private final Long publishedAt;

    public RahaColumnModel(String modelName,
                           String modelVersion,
                           String datasetId,
                           String columnName,
                           String schemaHash,
                           ClassifierType classifierType,
                           String featureDictionaryVersion,
                           String strategyPlanVersion,
                           double threshold,
                           String modelPath,
                           ModelStatus status,
                           Map<String, Double> metrics,
                           long createdAt) {
        this(modelName, modelVersion, datasetId, columnName, schemaHash,
                classifierType, featureDictionaryVersion, strategyPlanVersion,
                threshold, modelPath, status, metrics, createdAt,
                status == ModelStatus.PUBLISHED ? createdAt : null);
    }

    public RahaColumnModel(String modelName,
                           String modelVersion,
                           String datasetId,
                           String columnName,
                           String schemaHash,
                           ClassifierType classifierType,
                           String featureDictionaryVersion,
                           String strategyPlanVersion,
                           double threshold,
                           String modelPath,
                           ModelStatus status,
                           Map<String, Double> metrics,
                           long createdAt,
                           Long publishedAt) {
        this.modelName = ValueUtils.requireNotBlank(modelName, "模型名称");
        this.modelVersion = ValueUtils.requireNotBlank(modelVersion, "模型版本");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.columnName = ValueUtils.requireNotBlank(columnName, "模型字段");
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "模式哈希");
        if (classifierType == null || status == null) {
            throw new IllegalArgumentException("分类器类型和模型状态不能为空");
        }
        if (Double.isNaN(threshold) || threshold < 0.0d || threshold > 1.0d) {
            throw new IllegalArgumentException("模型阈值必须位于 0 到 1 之间");
        }
        if (createdAt <= 0L) {
            throw new IllegalArgumentException("模型创建时间必须大于 0");
        }
        if (publishedAt != null && publishedAt <= 0L) {
            throw new IllegalArgumentException("模型发布时间必须大于 0");
        }
        if (status == ModelStatus.PUBLISHED && publishedAt == null) {
            throw new IllegalArgumentException("已发布模型必须记录首次发布时间");
        }
        if (publishedAt != null && status != ModelStatus.PUBLISHED
                && status != ModelStatus.DISABLED) {
            throw new IllegalArgumentException("未发布模型不能携带发布时间");
        }
        this.classifierType = classifierType;
        this.featureDictionaryVersion = ValueUtils.requireNotBlank(
                featureDictionaryVersion, "特征字典版本");
        this.strategyPlanVersion = ValueUtils.requireNotBlank(strategyPlanVersion, "策略计划版本");
        this.threshold = threshold;
        this.modelPath = modelPath;
        this.status = status;
        this.metrics = metrics == null
                ? Collections.<String, Double>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Double>(metrics));
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
    }

    public String getModelName() {
        return modelName;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getSchemaHash() {
        return schemaHash;
    }

    public ClassifierType getClassifierType() {
        return classifierType;
    }

    public String getFeatureDictionaryVersion() {
        return featureDictionaryVersion;
    }

    public String getStrategyPlanVersion() {
        return strategyPlanVersion;
    }

    public double getThreshold() {
        return threshold;
    }

    public String getModelPath() {
        return modelPath;
    }

    public ModelStatus getStatus() {
        return status;
    }

    public Map<String, Double> getMetrics() {
        return metrics;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public Long getPublishedAt() {
        return publishedAt;
    }

    /**
     * 为草稿或候选模型保存评测选定阈值和指标，不改变训练参数版本。
     *
     * @param selectedThreshold 已评测选定阈值
     * @param evaluationMetrics 需要合并的评测指标
     * @return 更新阈值和指标后的模型元数据
     */
    public RahaColumnModel withEvaluation(double selectedThreshold,
                                          Map<String, Double> evaluationMetrics) {
        if (status != ModelStatus.DRAFT && status != ModelStatus.CANDIDATE) {
            throw new IllegalStateException("只有草稿或候选模型可以更新评测阈值");
        }
        if (Double.isNaN(selectedThreshold) || selectedThreshold < 0.0d
                || selectedThreshold > 1.0d || evaluationMetrics == null) {
            throw new IllegalArgumentException("评测阈值和指标必须有效");
        }
        Map<String, Double> merged = new LinkedHashMap<String, Double>(metrics);
        for (Map.Entry<String, Double> entry : evaluationMetrics.entrySet()) {
            if (entry.getKey() == null || entry.getKey().trim().isEmpty()
                    || entry.getValue() == null || Double.isNaN(entry.getValue())
                    || Double.isInfinite(entry.getValue())) {
                throw new IllegalArgumentException("评测指标名称和值必须有效");
            }
            merged.put(entry.getKey(), entry.getValue());
        }
        return new RahaColumnModel(modelName, modelVersion, datasetId, columnName,
                schemaHash, classifierType, featureDictionaryVersion,
                strategyPlanVersion, selectedThreshold, modelPath, status, merged,
                createdAt, publishedAt);
    }

    /**
     * 根据模型发布状态机创建新的元数据快照。
     *
     * @param target 目标状态
     * @return 状态更新后的不可变模型元数据
     */
    public RahaColumnModel withStatus(ModelStatus target) {
        return withStatus(target, createdAt);
    }

    /**
     * 根据模型发布状态机和状态变更时间创建新的元数据快照。
     *
     * @param target 目标状态
     * @param transitionAt 状态变更时间
     * @return 状态更新后的不可变模型元数据
     */
    public RahaColumnModel withStatus(ModelStatus target, long transitionAt) {
        if (target == null || !canTransition(status, target)) {
            throw new IllegalStateException("模型状态不允许从 " + status + " 转换为 " + target);
        }
        if (transitionAt <= 0L) {
            throw new IllegalArgumentException("模型状态变更时间必须大于 0");
        }
        Long nextPublishedAt = publishedAt;
        // 首次进入已发布状态时固化发布时间，后续停用和回滚保留原发布历史。
        if (target == ModelStatus.PUBLISHED && nextPublishedAt == null) {
            nextPublishedAt = transitionAt;
        }
        return new RahaColumnModel(modelName, modelVersion, datasetId, columnName,
                schemaHash, classifierType, featureDictionaryVersion,
                strategyPlanVersion, threshold, modelPath, target, metrics, createdAt,
                nextPublishedAt);
    }

    private static boolean canTransition(ModelStatus source, ModelStatus target) {
        if (source == target) {
            return true;
        }
        switch (source) {
            case DRAFT:
                return target == ModelStatus.CANDIDATE
                        || target == ModelStatus.FAILED
                        || target == ModelStatus.DISABLED;
            case CANDIDATE:
                return target == ModelStatus.PUBLISHED
                        || target == ModelStatus.DISABLED
                        || target == ModelStatus.FAILED;
            case PUBLISHED:
                return target == ModelStatus.DISABLED;
            case DISABLED:
                return target == ModelStatus.PUBLISHED;
            case FAILED:
            default:
                return false;
        }
    }
}
