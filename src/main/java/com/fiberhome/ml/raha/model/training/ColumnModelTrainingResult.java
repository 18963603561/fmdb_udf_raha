package com.fiberhome.ml.raha.model.training;

import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存列级模型训练状态、模型参数、指标和降级标记。
 */
public final class ColumnModelTrainingResult {

    /** 训练状态。 */
    private final ColumnModelTrainingStatus status;
    /** 成功时生成的模型参数。 */
    private final ColumnModelArtifact artifact;
    /** 是否由首选分类器降级得到。 */
    private final boolean fallback;
    /** 不包含原始值的状态说明。 */
    private final String message;
    /** 标签和训练指标。 */
    private final Map<String, Double> metrics;

    public ColumnModelTrainingResult(ColumnModelTrainingStatus status,
                                     ColumnModelArtifact artifact,
                                     boolean fallback,
                                     String message,
                                     Map<String, Double> metrics) {
        if (status == null || (status == ColumnModelTrainingStatus.TRAINED) != (artifact != null)) {
            throw new IllegalArgumentException("模型训练状态与模型参数不一致");
        }
        this.status = status;
        this.artifact = artifact;
        this.fallback = fallback;
        this.message = message;
        this.metrics = metrics == null ? Collections.<String, Double>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Double>(metrics));
    }

    public static ColumnModelTrainingResult untrainable(ColumnTrainingDataset dataset) {
        ColumnModelTrainingStatus status;
        switch (dataset.getStatus()) {
            case NO_LABELS:
                status = ColumnModelTrainingStatus.NO_LABELS;
                break;
            case SINGLE_CLASS:
                status = ColumnModelTrainingStatus.SINGLE_CLASS;
                break;
            case EMPTY_FEATURES:
                status = ColumnModelTrainingStatus.EMPTY_FEATURES;
                break;
            case LABEL_CONFLICT:
                status = ColumnModelTrainingStatus.LABEL_CONFLICT;
                break;
            default:
                throw new IllegalArgumentException("训练数据状态并非不可训练状态");
        }
        return new ColumnModelTrainingResult(status, null, false,
                dataset.getMessage(), Collections.<String, Double>emptyMap());
    }

    public ColumnModelTrainingStatus getStatus() { return status; }
    public ColumnModelArtifact getArtifact() { return artifact; }
    public boolean isFallback() { return fallback; }
    public String getMessage() { return message; }
    public Map<String, Double> getMetrics() { return metrics; }

    /**
     * 使用补充质量指标创建训练结果副本。
     *
     * @param additionalMetrics 需要合并的质量指标
     * @return 指标已经合并的训练结果
     */
    public ColumnModelTrainingResult withMetrics(Map<String, Double> additionalMetrics) {
        if (additionalMetrics == null) {
            throw new IllegalArgumentException("补充训练指标不能为空");
        }
        Map<String, Double> merged = new LinkedHashMap<String, Double>(metrics);
        merged.putAll(additionalMetrics);
        return new ColumnModelTrainingResult(status, artifact, fallback, message, merged);
    }
}
