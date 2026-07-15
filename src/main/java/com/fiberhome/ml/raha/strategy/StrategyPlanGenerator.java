package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.config.StrategyConfig;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.data.StrategyStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据数据画像和策略配置生成确定性的 OD、PVD、RVD 策略计划。
 */
public final class StrategyPlanGenerator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(StrategyPlanGenerator.class);
    /** 默认少数模式比例。 */
    private static final String DEFAULT_MINORITY_RATIO = "0.1";
    /** 默认四分位距倍数。 */
    private static final String DEFAULT_IQR_MULTIPLIER = "1.5";
    /** 默认格式适用最小匹配比例。 */
    private static final String DEFAULT_FORMAT_MIN_RATIO = "0.8";
    /** 空值和特殊占位值集合。 */
    private static final String DEFAULT_PLACEHOLDERS = "N/A,NULL,NONE,UNKNOWN,-,--,?";

    /**
     * 为一个数据集生成确定性策略计划。
     *
     * @param dataset 已完成画像的数据集
     * @param config 策略配置
     * @return 按优先级和策略标识排序的策略计划
     */
    public List<StrategyPlan> generate(RahaDataset dataset, StrategyConfig config) {
        if (dataset == null || config == null) {
            throw new IllegalArgumentException("策略计划生成参数不能为空");
        }
        List<StrategyPlan> plans = new ArrayList<StrategyPlan>();
        for (ColumnMetadata column : dataset.getColumns()) {
            if (!isColumnEnabled(column, config)) {
                continue;
            }
            ColumnProfile profile = dataset.getProfiles().get(column.getName());
            if (profile == null || profile.getNonNullCount() == 0L) {
                addNullPlaceholderPlan(plans, column, profile, config);
                continue;
            }
            if (config.getStrategyFamilies().contains(StrategyFamily.OD)) {
                addOdPlans(plans, column, profile, config);
            }
            if (config.getStrategyFamilies().contains(StrategyFamily.PVD)) {
                addPvdPlans(plans, column, profile, config);
            }
        }
        if (config.getStrategyFamilies().contains(StrategyFamily.RVD)) {
            addRvdPlans(plans, dataset, config);
        }
        Collections.sort(plans, new Comparator<StrategyPlan>() {
            @Override
            public int compare(StrategyPlan first, StrategyPlan second) {
                int priorityCompare = Integer.compare(first.getPriority(), second.getPriority());
                return priorityCompare == 0
                        ? first.getStrategyId().compareTo(second.getStrategyId())
                        : priorityCompare;
            }
        });
        if (plans.size() > config.getMaxStrategyCount()) {
            LOGGER.warn("策略数量超过上限，将按优先级截断，generatedCount={}，maxStrategyCount={}",
                    plans.size(), config.getMaxStrategyCount());
            plans = new ArrayList<StrategyPlan>(
                    plans.subList(0, config.getMaxStrategyCount()));
        }
        LOGGER.info("策略计划生成完成，datasetId={}，snapshotId={}，planCount={}",
                dataset.getDatasetId(), dataset.getSnapshotId(), plans.size());
        return Collections.unmodifiableList(plans);
    }

    private static void addOdPlans(List<StrategyPlan> plans,
                                   ColumnMetadata column,
                                   ColumnProfile profile,
                                   StrategyConfig config) {
        Map<String, String> frequency = base(StrategyTypes.OD_LOW_FREQUENCY);
        frequency.put(StrategyConfigurationKeys.MAX_FREQUENCY,
                String.valueOf(Math.max(1L, (long) Math.floor(profile.getNonNullCount() * 0.01d))));
        addPlan(plans, StrategyFamily.OD, column, frequency, 100, config);

        if (profile.getNumericCount() >= 3L
                && profile.getNumericMean() != null
                && profile.getNumericStandardDeviation() != null
                && profile.getNumericStandardDeviation() > 0.0d) {
            Map<String, String> distance = base(StrategyTypes.OD_NUMERIC_DISTANCE);
            distance.put(StrategyConfigurationKeys.NUMERIC_MEAN,
                    formatDouble(profile.getNumericMean()));
            distance.put(StrategyConfigurationKeys.NUMERIC_STANDARD_DEVIATION,
                    formatDouble(profile.getNumericStandardDeviation()));
            distance.put(StrategyConfigurationKeys.Z_THRESHOLD, "3.0");
            addPlan(plans, StrategyFamily.OD, column, distance, 110, config);
        }
        if (profile.getNumericCount() >= 4L
                && profile.getNumericQ1() != null
                && profile.getNumericQ3() != null
                && profile.getNumericQ3() > profile.getNumericQ1()) {
            Map<String, String> quantile = base(StrategyTypes.OD_QUANTILE);
            quantile.put(StrategyConfigurationKeys.NUMERIC_Q1,
                    formatDouble(profile.getNumericQ1()));
            quantile.put(StrategyConfigurationKeys.NUMERIC_Q3,
                    formatDouble(profile.getNumericQ3()));
            quantile.put(StrategyConfigurationKeys.IQR_MULTIPLIER, DEFAULT_IQR_MULTIPLIER);
            addPlan(plans, StrategyFamily.OD, column, quantile, 120, config);
        }
    }

    private static void addPvdPlans(List<StrategyPlan> plans,
                                    ColumnMetadata column,
                                    ColumnProfile profile,
                                    StrategyConfig config) {
        Map<String, String> character = base(StrategyTypes.PVD_CHARACTER_SET);
        character.put(StrategyConfigurationKeys.MINORITY_RATIO, DEFAULT_MINORITY_RATIO);
        addPlan(plans, StrategyFamily.PVD, column, character, 200, config);

        if (profile.getMinLength() >= 0 && profile.getMaxLength() > profile.getMinLength()) {
            Map<String, String> length = base(StrategyTypes.PVD_LENGTH);
            length.put(StrategyConfigurationKeys.MINORITY_RATIO, DEFAULT_MINORITY_RATIO);
            length.put(StrategyConfigurationKeys.IQR_MULTIPLIER, DEFAULT_IQR_MULTIPLIER);
            addPlan(plans, StrategyFamily.PVD, column, length, 210, config);
        }
        addNullPlaceholderPlan(plans, column, profile, config);

        Map<String, String> typeFormat = base(StrategyTypes.PVD_TYPE_FORMAT);
        typeFormat.put(StrategyConfigurationKeys.MINORITY_RATIO, DEFAULT_MINORITY_RATIO);
        typeFormat.put(StrategyConfigurationKeys.FORMAT_TYPE, "AUTO");
        typeFormat.put(StrategyConfigurationKeys.FORMAT_MIN_RATIO, DEFAULT_FORMAT_MIN_RATIO);
        addPlan(plans, StrategyFamily.PVD, column, typeFormat, 230, config);
    }

    private static void addNullPlaceholderPlan(List<StrategyPlan> plans,
                                               ColumnMetadata column,
                                               ColumnProfile profile,
                                               StrategyConfig config) {
        if (!config.getStrategyFamilies().contains(StrategyFamily.PVD)
                || profile == null || profile.getTotalCount() == 0L) {
            return;
        }
        Map<String, String> nullPlaceholder = base(StrategyTypes.PVD_NULL_PLACEHOLDER);
        nullPlaceholder.put(StrategyConfigurationKeys.PLACEHOLDERS, DEFAULT_PLACEHOLDERS);
        addPlan(plans, StrategyFamily.PVD, column, nullPlaceholder, 220, config);
    }

    private static void addRvdPlans(List<StrategyPlan> plans,
                                    RahaDataset dataset,
                                    StrategyConfig config) {
        if (!isStrategyTypeEnabled(StrategyTypes.RVD_ONE_TO_MANY, config)) {
            return;
        }
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        for (ColumnMetadata column : dataset.getColumns()) {
            ColumnProfile profile = dataset.getProfiles().get(column.getName());
            if (isColumnEnabled(column, config) && isRvdTypeSupported(column.getDataType())
                    && profile != null && profile.getNonNullCount() > 0L) {
                columns.add(column);
            }
        }
        int generated = 0;
        int possible = columns.size() * Math.max(0, columns.size() - 1);
        for (ColumnMetadata left : columns) {
            for (ColumnMetadata right : columns) {
                if (left.getName().equals(right.getName())) {
                    continue;
                }
                if (generated >= config.getMaxRvdColumnPairs()) {
                    LOGGER.warn("RVD 列对数量达到上限，停止继续枚举，possibleCount={}，maxPairCount={}",
                            possible, config.getMaxRvdColumnPairs());
                    return;
                }
                Map<String, String> configuration = base(StrategyTypes.RVD_ONE_TO_MANY);
                configuration.put(StrategyConfigurationKeys.LEFT_COLUMN, left.getName());
                configuration.put(StrategyConfigurationKeys.RIGHT_COLUMN, right.getName());
                addPairPlan(plans, left, right, configuration, 300, config);
                generated++;
            }
        }
    }

    private static void addPlan(List<StrategyPlan> plans,
                                StrategyFamily family,
                                ColumnMetadata column,
                                Map<String, String> configuration,
                                int defaultPriority,
                                StrategyConfig config) {
        String strategyType = configuration.get(StrategyConfigurationKeys.STRATEGY_TYPE);
        if (!isStrategyTypeEnabled(strategyType, config)) {
            return;
        }
        List<String> targetColumns = Collections.singletonList(column.getName());
        String strategyId = StrategyIdentityGenerator.strategyId(family, targetColumns, configuration);
        int priority = config.getStrategyPriorities().containsKey(strategyType)
                ? config.getStrategyPriorities().get(strategyType) : defaultPriority;
        plans.add(new StrategyPlan(strategyId, family, targetColumns, configuration,
                priority, StrategyStatus.PLANNED));
    }

    private static void addPairPlan(List<StrategyPlan> plans,
                                    ColumnMetadata left,
                                    ColumnMetadata right,
                                    Map<String, String> configuration,
                                    int defaultPriority,
                                    StrategyConfig config) {
        String strategyType = configuration.get(StrategyConfigurationKeys.STRATEGY_TYPE);
        List<String> targetColumns = java.util.Arrays.asList(left.getName(), right.getName());
        String strategyId = StrategyIdentityGenerator.strategyId(
                StrategyFamily.RVD, targetColumns, configuration);
        int priority = config.getStrategyPriorities().containsKey(strategyType)
                ? config.getStrategyPriorities().get(strategyType) : defaultPriority;
        plans.add(new StrategyPlan(strategyId, StrategyFamily.RVD, targetColumns,
                configuration, priority, StrategyStatus.PLANNED));
    }

    private static Map<String, String> base(String strategyType) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, strategyType);
        return configuration;
    }

    private static boolean isColumnEnabled(ColumnMetadata column, StrategyConfig config) {
        return column.isDetectable()
                && (config.getIncludedColumns().isEmpty()
                || config.getIncludedColumns().contains(column.getName()))
                && !config.getExcludedColumns().contains(column.getName());
    }

    private static boolean isStrategyTypeEnabled(String strategyType, StrategyConfig config) {
        return (config.getIncludedStrategyTypes().isEmpty()
                || config.getIncludedStrategyTypes().contains(strategyType))
                && !config.getExcludedStrategyTypes().contains(strategyType);
    }

    private static boolean isRvdTypeSupported(String dataType) {
        String normalized = dataType == null ? "" : dataType.toLowerCase(java.util.Locale.ROOT);
        return !normalized.contains("array") && !normalized.contains("map")
                && !normalized.contains("struct") && !normalized.contains("binary");
    }

    private static String formatDouble(double value) {
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
