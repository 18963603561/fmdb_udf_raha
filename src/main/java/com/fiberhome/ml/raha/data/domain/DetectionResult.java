package com.fiberhome.ml.raha.data.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 表示一个单元格的最终检测判断，仅包含检测语义，不包含任何纠正值。
 */
public final class DetectionResult {

    /** 检测任务标识。 */
    private final String jobId;
    /** 完整任务配置版本。 */
    private final String configVersion;
    /** 生成检测结果的阶段标识。 */
    private final String stageId;
    /** 被检测单元格坐标。 */
    private final CellCoordinate coordinate;
    /** 输入原始值哈希。 */
    private final String valueHash;
    /** 可选脱敏展示值。 */
    private final String maskedValue;
    /** 是否被判断为疑似错误。 */
    private final boolean error;
    /** 模型输出的错误分数。 */
    private final double score;
    /** 当前模型使用的判断阈值。 */
    private final double threshold;
    /** 对该单元格产生信号的策略标识。 */
    private final List<String> strategyIds;
    /** 结构化检测原因。 */
    private final Map<String, String> reasons;
    /** 使用的模型名称。 */
    private final String modelName;
    /** 使用的模型版本。 */
    private final String modelVersion;
    /** 使用的特征字典版本。 */
    private final String featureDictionaryVersion;
    /** 检测结果生成时间。 */
    private final long detectedAt;

    public DetectionResult(String jobId,
                           CellCoordinate coordinate,
                           String valueHash,
                           String maskedValue,
                           boolean error,
                           double score,
                           double threshold,
                           List<String> strategyIds,
                           Map<String, String> reasons,
                           String modelName,
                           String modelVersion,
                           String featureDictionaryVersion,
                           long detectedAt) {
        this(jobId, "LEGACY", "LEGACY", coordinate, valueHash, maskedValue,
                error, score, threshold, strategyIds, reasons, modelName,
                modelVersion, featureDictionaryVersion, detectedAt);
    }

    public DetectionResult(String jobId,
                           String configVersion,
                           String stageId,
                           CellCoordinate coordinate,
                           String valueHash,
                           String maskedValue,
                           boolean error,
                           double score,
                           double threshold,
                           List<String> strategyIds,
                           Map<String, String> reasons,
                           String modelName,
                           String modelVersion,
                           String featureDictionaryVersion,
                           long detectedAt) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        this.configVersion = ValueUtils.requireNotBlank(configVersion, "配置版本");
        this.stageId = ValueUtils.requireNotBlank(stageId, "检测阶段标识");
        if (coordinate == null) {
            throw new IllegalArgumentException("单元格坐标不能为空");
        }
        if (Double.isNaN(score) || score < 0.0d || score > 1.0d
                || Double.isNaN(threshold) || threshold < 0.0d || threshold > 1.0d) {
            throw new IllegalArgumentException("检测分数和阈值必须位于 0 到 1 之间");
        }
        if (detectedAt <= 0L) {
            throw new IllegalArgumentException("检测时间必须大于 0");
        }
        this.coordinate = coordinate;
        this.valueHash = ValueUtils.requireNotBlank(valueHash, "值哈希");
        this.maskedValue = maskedValue;
        this.error = error;
        this.score = score;
        this.threshold = threshold;
        this.strategyIds = strategyIds == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(strategyIds));
        this.reasons = reasons == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(reasons));
        this.modelName = ValueUtils.requireNotBlank(modelName, "模型名称");
        this.modelVersion = ValueUtils.requireNotBlank(modelVersion, "模型版本");
        this.featureDictionaryVersion = ValueUtils.requireNotBlank(
                featureDictionaryVersion, "特征字典版本");
        this.detectedAt = detectedAt;
    }

    public String getJobId() {
        return jobId;
    }

    public String getConfigVersion() {
        return configVersion;
    }

    public String getStageId() {
        return stageId;
    }

    public CellCoordinate getCoordinate() {
        return coordinate;
    }

    public String getValueHash() {
        return valueHash;
    }

    public String getMaskedValue() {
        return maskedValue;
    }

    public boolean isError() {
        return error;
    }

    public double getScore() {
        return score;
    }

    public double getThreshold() {
        return threshold;
    }

    public List<String> getStrategyIds() {
        return strategyIds;
    }

    public Map<String, String> getReasons() {
        return reasons;
    }

    public String getModelName() {
        return modelName;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public String getFeatureDictionaryVersion() {
        return featureDictionaryVersion;
    }

    public long getDetectedAt() {
        return detectedAt;
    }
}
