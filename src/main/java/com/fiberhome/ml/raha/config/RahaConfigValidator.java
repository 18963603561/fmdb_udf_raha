package com.fiberhome.ml.raha.config;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 校验 Raha 任务配置的必填项、数值范围和字段过滤冲突。
 */
public final class RahaConfigValidator {

    /**
     * 校验任务配置，发现首个错误时抛出带稳定编码的异常。
     *
     * @param config 待校验配置
     */
    public void validate(RahaJobConfig config) {
        if (config == null) {
            fail(ConfigErrorCode.CONFIG_REQUIRED, "任务配置不能为空");
        }
        if (config.getJobType() == null) {
            fail(ConfigErrorCode.JOB_TYPE_REQUIRED, "任务类型不能为空");
        }
        requireNotBlank(config.getDatasetId(), ConfigErrorCode.DATASET_ID_REQUIRED, "数据集标识不能为空");
        requireNotBlank(config.getInputReference(), ConfigErrorCode.INPUT_REFERENCE_REQUIRED, "输入数据引用不能为空");
        requireNotBlank(config.getRowIdColumn(), ConfigErrorCode.ROW_ID_COLUMN_REQUIRED, "行标识字段不能为空");
        requireNotBlank(config.getExecutionConfigFingerprint(),
                ConfigErrorCode.CONFIG_REQUIRED, "执行配置指纹不能为空");
        if (config.getResultRetentionDays() <= 0) {
            fail(ConfigErrorCode.RESULT_RETENTION_INVALID, "结果保留天数必须大于 0");
        }
        validateStrategy(config.getStrategyConfig());
        validateFeature(config.getFeatureConfig());
        validateModel(config.getModelConfig());
        validateClustering(config.getClusteringConfig());
        validateSampling(config.getSamplingConfig());
        validateResource(config.getResourceConfig());
        validateFailureTolerance(config.getFailureToleranceConfig());
    }

    private void validateStrategy(StrategyConfig config) {
        if (config == null) {
            fail(ConfigErrorCode.STRATEGY_CONFIG_REQUIRED, "策略配置不能为空");
        }
        if (config.getStrategyFamilies().isEmpty()) {
            fail(ConfigErrorCode.STRATEGY_FAMILY_REQUIRED, "至少需要启用一个检测策略族");
        }
        // 策略数量、列对数量和超时必须为正数，避免生成无界或永不结束的任务。
        if (config.getMaxStrategyCount() <= 0
                || config.getMaxRvdColumnPairs() <= 0
                || config.getStrategyTimeoutMillis() <= 0L) {
            fail(ConfigErrorCode.STRATEGY_LIMIT_INVALID, "策略数量、RVD 列对数量和超时必须大于 0");
        }
        Set<String> conflicts = new HashSet<String>(config.getIncludedColumns());
        conflicts.retainAll(config.getExcludedColumns());
        if (!conflicts.isEmpty()) {
            fail(ConfigErrorCode.COLUMN_FILTER_CONFLICT, "字段不能同时出现在白名单和黑名单中：" + conflicts);
        }
        Set<String> strategyConflicts = new HashSet<String>(config.getIncludedStrategyTypes());
        strategyConflicts.retainAll(config.getExcludedStrategyTypes());
        if (!strategyConflicts.isEmpty()) {
            fail(ConfigErrorCode.STRATEGY_FILTER_INVALID,
                    "策略类型不能同时出现在白名单和黑名单中：" + strategyConflicts);
        }
        for (String strategyType : config.getIncludedStrategyTypes()) {
            if (isBlank(strategyType)) {
                fail(ConfigErrorCode.STRATEGY_FILTER_INVALID, "策略类型白名单不能包含空值");
            }
        }
        for (String strategyType : config.getExcludedStrategyTypes()) {
            if (isBlank(strategyType)) {
                fail(ConfigErrorCode.STRATEGY_FILTER_INVALID, "策略类型黑名单不能包含空值");
            }
        }
        for (Map.Entry<String, Integer> entry : config.getStrategyPriorities().entrySet()) {
            if (isBlank(entry.getKey()) || entry.getValue() == null || entry.getValue() < 0) {
                fail(ConfigErrorCode.STRATEGY_FILTER_INVALID,
                        "策略类型优先级必须使用非空类型和非负数值");
            }
        }
    }

    private void validateFeature(FeatureConfig config) {
        if (config == null || config.getMaxFeatureCount() <= 0
                || Double.isNaN(config.getRareValueRatio())
                || config.getRareValueRatio() < 0.0d
                || config.getRareValueRatio() > 1.0d) {
            fail(ConfigErrorCode.FEATURE_CONFIG_INVALID,
                    "特征配置不能为空，最大特征数量和稀有值比例必须有效");
        }
    }

    private void validateModel(ModelConfig config) {
        if (config == null || config.getClassifierType() == null) {
            fail(ConfigErrorCode.MODEL_CONFIG_REQUIRED, "模型配置和分类器类型不能为空");
        }
        // 检测分数约定在闭区间零到一内，阈值超出范围会使全部样本得到同一判断。
        if (Double.isNaN(config.getThreshold())
                || config.getThreshold() < 0.0d
                || config.getThreshold() > 1.0d) {
            fail(ConfigErrorCode.MODEL_THRESHOLD_INVALID, "模型阈值必须位于 0 到 1 之间");
        }
        if (Double.isNaN(config.getContextWeight()) || config.getContextWeight() < 0.0d
                || config.getContextWeight() > 1.0d) {
            fail(ConfigErrorCode.MODEL_THRESHOLD_INVALID, "上下文权重必须位于 0 到 1 之间");
        }
        if (Double.isNaN(config.getMinimumScoreStandardDeviation())
                || config.getMinimumScoreStandardDeviation() < 0.0d
                || Double.isNaN(config.getMaximumPositiveRatio())
                || config.getMaximumPositiveRatio() <= 0.5d
                || config.getMaximumPositiveRatio() >= 1.0d
                || Double.isNaN(config.getMinimumF1())
                || config.getMinimumF1() < 0.0d
                || config.getMinimumF1() > 1.0d) {
            fail(ConfigErrorCode.MODEL_THRESHOLD_INVALID,
                    "模型质量门禁阈值范围非法");
        }
        for (java.util.Map.Entry<com.fiberhome.ml.raha.data.StrategyFamily, Double> entry
                : config.getStrategyFamilyWeights().entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null
                    || Double.isNaN(entry.getValue()) || entry.getValue() < 0.0d
                    || entry.getValue() > 1.0d) {
                fail(ConfigErrorCode.MODEL_THRESHOLD_INVALID,
                        "策略族可靠度权重必须位于 0 到 1 之间");
            }
        }
        double[] scoringWeights = new double[]{
                config.getDefaultStrategyFamilyWeight(),
                config.getRareContextSignalWeight(),
                config.getRvdConflictContextSignalWeight(),
                config.getNullContextSignalWeight(),
                config.getBlankContextSignalWeight(),
                config.getMixedContextSignalWeight()};
        for (double scoringWeight : scoringWeights) {
            if (Double.isNaN(scoringWeight) || scoringWeight < 0.0d
                    || scoringWeight > 1.0d) {
                fail(ConfigErrorCode.MODEL_THRESHOLD_INVALID,
                        "规则评分权重必须位于 0 到 1 之间");
            }
        }
    }

    private void validateResource(ResourceConfig config) {
        if (config == null
                || config.getMaxParallelStrategies() <= 0
                || config.getMaxParallelColumns() <= 0
                || config.getBroadcastThresholdBytes() <= 0L
                || config.getCacheThresholdBytes() <= 0L
                || config.getStageTimeoutMillis() <= 0L
                || !isSupportedStorageLevel(config.getCacheStorageLevel())) {
            fail(ConfigErrorCode.RESOURCE_CONFIG_INVALID, "资源并发、广播阈值、缓存级别和阶段超时必须有效");
        }
    }

    private static boolean isSupportedStorageLevel(String value) {
        return "MEMORY_ONLY".equals(value)
                || "MEMORY_AND_DISK".equals(value)
                || "DISK_ONLY".equals(value);
    }

    private void validateClustering(ClusteringConfig config) {
        if (config == null || config.getDistanceMetric() == null
                || config.getTargetClusterCount() <= 0
                || config.getMaxSampleCount() <= 0) {
            fail(ConfigErrorCode.CLUSTERING_CONFIG_INVALID,
                    "聚类距离、目标簇数量和样本上限必须有效");
        }
    }

    private void validateSampling(SamplingConfig config) {
        if (config == null || config.getLabelingBudget() <= 0
                || config.getTaskTtlMillis() <= 0L
                || Double.isNaN(config.getCoverageScoreExponentCap())
                || config.getCoverageScoreExponentCap() <= 0.0d) {
            fail(ConfigErrorCode.SAMPLING_CONFIG_INVALID,
                    "采样预算、标注任务有效期和覆盖分数上限必须大于 0");
        }
    }

    private void validateFailureTolerance(FailureToleranceConfig config) {
        if (config == null
                || Double.isNaN(config.getMaxFailedStrategyRatio())
                || config.getMaxFailedStrategyRatio() < 0.0d
                || config.getMaxFailedStrategyRatio() > 1.0d
                || config.getMaxRetryCount() < 0) {
            fail(ConfigErrorCode.FAILURE_TOLERANCE_INVALID, "失败比例必须位于 0 到 1 之间，重试次数不能小于 0");
        }
    }

    private static void requireNotBlank(String value, ConfigErrorCode code, String message) {
        if (isBlank(value)) {
            fail(code, message);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void fail(ConfigErrorCode code, String message) {
        throw new ConfigValidationException(code, message);
    }
}
