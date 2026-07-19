package com.fiberhome.ml.raha.strategy.plan;

import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据数据画像和策略配置生成确定性的 OD、PVD、RVD 策略计划。
 */
public final class StrategyPlanGenerator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(StrategyPlanGenerator.class);
    /** 策略生成阈值、占位值和默认优先级。 */
    private final StrategyGenerationConfig generationConfig;

    public StrategyPlanGenerator() {
        this(RahaDefaultConfigProvider.factory().strategyGenerationConfig());
    }

    public StrategyPlanGenerator(StrategyGenerationConfig generationConfig) {
        if (generationConfig == null) {
            throw new IllegalArgumentException("策略生成配置不能为空");
        }
        this.generationConfig = generationConfig;
    }

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
            // 字段必须同时满足可检测、白名单和黑名单要求。
            // 例如 excludedColumns 包含 age 时，age 不再生成任何单列策略。
            if (!isColumnEnabled(column, config)) {
                continue;
            }
            ColumnProfile profile = dataset.getProfiles().get(column.getName());
            // 没有非空样本时无法推导低频、长度、数值分布等策略。
            // 此时只保留空值占位符这类仍有意义的 PVD 策略。
            if (profile == null || profile.getNonNullCount() == 0L) {
                addNullPlaceholderPlan(plans, column, profile, config);
                continue;
            }
            // OD 策略关注字段内的异常值，例如低频值、偏离均值的数值、超出四分位边界的数值。
            if (config.getStrategyFamilies().contains(StrategyFamily.OD)) {
                addOdPlans(plans, column, profile, config);
            }
            // PVD 策略关注字段模式是否异常，例如字符集、长度、空值占位符和类型格式。
            if (config.getStrategyFamilies().contains(StrategyFamily.PVD)) {
                addPvdPlans(plans, column, profile, config);
            }
        }
        // RVD 策略需要跨字段组合判断，所以在单列策略遍历完成后统一枚举字段对。
        if (config.getStrategyFamilies().contains(StrategyFamily.RVD)) {
            addRvdPlans(plans, dataset, config);
        }
        // 优先级数值越小越先执行；策略标识作为第二排序键，保证相同输入下计划顺序稳定。
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
        // 返回不可变列表，避免调用方在执行阶段误改计划顺序或内容。
        return Collections.unmodifiableList(plans);
    }

    private void addOdPlans(List<StrategyPlan> plans,
                            ColumnMetadata column,
                            ColumnProfile profile,
                            StrategyConfig config) {
        Map<String, String> frequency = base(StrategyTypes.OD_LOW_FREQUENCY);
        // 低频阈值按非空样本量比例推导。
        // 例如 nonNullCount=1000、ratio=0.01 时，maxFrequency=10。
        frequency.put(StrategyConfigurationKeys.MAX_FREQUENCY,
                String.valueOf(Math.max(1L, (long) Math.floor(
                        profile.getNonNullCount()
                                * generationConfig.getLowFrequencyRatio()))));
        addPlan(plans, StrategyFamily.OD, column, frequency,
                generationConfig.getOdLowFrequencyPriority(), config);

        // 数值距离策略依赖稳定的均值和标准差。
        // 标准差为 0 表示样本无离散度，无法计算有效 Z 分数。
        if (profile.getNumericCount() >= generationConfig.getMinimumNumericCount()
                && profile.getNumericMean() != null
                && profile.getNumericStandardDeviation() != null
                && profile.getNumericStandardDeviation() > 0.0d) {
            Map<String, String> distance = base(StrategyTypes.OD_NUMERIC_DISTANCE);
            distance.put(StrategyConfigurationKeys.NUMERIC_MEAN,
                    formatDouble(profile.getNumericMean()));
            distance.put(StrategyConfigurationKeys.NUMERIC_STANDARD_DEVIATION,
                    formatDouble(profile.getNumericStandardDeviation()));
            distance.put(StrategyConfigurationKeys.Z_THRESHOLD,
                    formatDouble(generationConfig.getZThreshold()));
            addPlan(plans, StrategyFamily.OD, column, distance,
                    generationConfig.getOdNumericDistancePriority(), config);
        }
        // 四分位策略要求 Q3 大于 Q1。
        // 例如 Q1=20、Q3=100、倍数=1.5 时，正常上界为 220。
        if (profile.getNumericCount() >= generationConfig.getMinimumQuantileCount()
                && profile.getNumericQ1() != null
                && profile.getNumericQ3() != null
                && profile.getNumericQ3() > profile.getNumericQ1()) {
            Map<String, String> quantile = base(StrategyTypes.OD_QUANTILE);
            quantile.put(StrategyConfigurationKeys.NUMERIC_Q1,
                    formatDouble(profile.getNumericQ1()));
            quantile.put(StrategyConfigurationKeys.NUMERIC_Q3,
                    formatDouble(profile.getNumericQ3()));
            quantile.put(StrategyConfigurationKeys.IQR_MULTIPLIER,
                    formatDouble(generationConfig.getIqrMultiplier()));
            addPlan(plans, StrategyFamily.OD, column, quantile,
                    generationConfig.getOdQuantilePriority(), config);
        }
    }

    private void addPvdPlans(List<StrategyPlan> plans,
                             ColumnMetadata column,
                             ColumnProfile profile,
                             StrategyConfig config) {
        Map<String, String> character = base(StrategyTypes.PVD_CHARACTER_SET);
        // 字符集策略用于识别少数异常字符模式；例如手机号字段中混入中文或特殊符号。
        character.put(StrategyConfigurationKeys.MINORITY_RATIO,
                formatDouble(generationConfig.getMinorityRatio()));
        addPlan(plans, StrategyFamily.PVD, column, character,
                generationConfig.getPvdCharacterSetPriority(), config);

        // 长度策略只在字段存在长度差异时生成；例如身份证号大多 18 位，少量 10 位或 30 位。
        if (profile.getMinLength() >= 0 && profile.getMaxLength() > profile.getMinLength()) {
            Map<String, String> length = base(StrategyTypes.PVD_LENGTH);
            length.put(StrategyConfigurationKeys.MINORITY_RATIO,
                    formatDouble(generationConfig.getMinorityRatio()));
            length.put(StrategyConfigurationKeys.IQR_MULTIPLIER,
                    formatDouble(generationConfig.getIqrMultiplier()));
            addPlan(plans, StrategyFamily.PVD, column, length,
                    generationConfig.getPvdLengthPriority(), config);
        }
        // 空值占位符策略对所有有画像数据的字段都适用。
        // 例如识别 NULL、N/A、UNKNOWN 等业务占位值。
        addNullPlaceholderPlan(plans, column, profile, config);

        Map<String, String> typeFormat = base(StrategyTypes.PVD_TYPE_FORMAT);
        // 类型格式策略可由执行器自动归纳主流格式。
        // 例如生日字段大多为 yyyy-MM-dd，少量为 MM/dd/yyyy。
        typeFormat.put(StrategyConfigurationKeys.MINORITY_RATIO,
                formatDouble(generationConfig.getMinorityRatio()));
        typeFormat.put(StrategyConfigurationKeys.FORMAT_TYPE,
                generationConfig.getFormatType());
        typeFormat.put(StrategyConfigurationKeys.FORMAT_MIN_RATIO,
                formatDouble(generationConfig.getFormatMinRatio()));
        addPlan(plans, StrategyFamily.PVD, column, typeFormat,
                generationConfig.getPvdTypeFormatPriority(), config);
    }

    private void addNullPlaceholderPlan(List<StrategyPlan> plans,
                                        ColumnMetadata column,
                                        ColumnProfile profile,
                                        StrategyConfig config) {
        // 只有 PVD 开启且字段存在总样本时才生成。
        // profile 缺失时无法确认该字段是否实际参与画像。
        if (!config.getStrategyFamilies().contains(StrategyFamily.PVD)
                || profile == null || profile.getTotalCount() == 0L) {
            return;
        }
        Map<String, String> nullPlaceholder = base(StrategyTypes.PVD_NULL_PLACEHOLDER);
        nullPlaceholder.put(StrategyConfigurationKeys.PLACEHOLDERS,
                generationConfig.getPlaceholders());
        addPlan(plans, StrategyFamily.PVD, column, nullPlaceholder,
                generationConfig.getPvdNullPlaceholderPriority(), config);
    }

    private void addRvdPlans(List<StrategyPlan> plans,
                             RahaDataset dataset,
                             StrategyConfig config) {
        // RVD 当前只支持一对多关系策略；若该具体类型被过滤，整个跨字段枚举可以直接跳过。
        if (!isStrategyTypeEnabled(StrategyTypes.RVD_ONE_TO_MANY, config)) {
            return;
        }
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        for (ColumnMetadata column : dataset.getColumns()) {
            ColumnProfile profile = dataset.getProfiles().get(column.getName());
            // 复杂类型不参与关系检测。
            // 例如 array、map、struct、binary 字段无法稳定作为关系键。
            if (isColumnEnabled(column, config) && isRvdTypeSupported(column.getDataType())
                    && profile != null && profile.getNonNullCount() > 0L) {
                columns.add(column);
            }
        }
        int generated = 0;
        int possible = columns.size() * Math.max(0, columns.size() - 1);
        for (ColumnMetadata left : columns) {
            for (ColumnMetadata right : columns) {
                // RVD 是两个不同字段之间的依赖关系。
                // 同一字段到自身没有业务检测意义。
                if (left.getName().equals(right.getName())) {
                    continue;
                }
                // 字段对数量按配置硬限制。
                // 这可以避免宽表上生成过多组合拖慢后续执行。
                if (generated >= config.getMaxRvdColumnPairs()) {
                    LOGGER.warn("RVD 列对数量达到上限，停止继续枚举，possibleCount={}，maxPairCount={}",
                            possible, config.getMaxRvdColumnPairs());
                    return;
                }
                Map<String, String> configuration = base(StrategyTypes.RVD_ONE_TO_MANY);
                // 字段对是有方向的。
                // 例如 zipcode -> city 和 city -> zipcode 会生成两个不同计划。
                configuration.put(StrategyConfigurationKeys.LEFT_COLUMN, left.getName());
                configuration.put(StrategyConfigurationKeys.RIGHT_COLUMN, right.getName());
                addPairPlan(plans, left, right, configuration,
                        generationConfig.getRvdOneToManyPriority(), config);
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
        // 具体策略类型仍需遵守白名单和黑名单。
        // 例如只开启 OD_LOW_FREQUENCY 时会过滤其它 OD 策略。
        if (!isStrategyTypeEnabled(strategyType, config)) {
            return;
        }
        List<String> targetColumns = Collections.singletonList(column.getName());
        // 策略标识由策略族、目标字段和配置共同生成。
        // 这能确保同一输入可以重复得到同一标识。
        String strategyId = StrategyIdentityGenerator.strategyId(family, targetColumns, configuration);
        // 配置中的优先级覆盖默认优先级。
        // 例如可将 PVD_NULL_PLACEHOLDER 调整到最先执行。
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
        // 字段顺序会进入策略标识。
        // 例如 A -> B 与 B -> A 的 targetColumns 不同，计划标识也不同。
        String strategyId = StrategyIdentityGenerator.strategyId(
                StrategyFamily.RVD, targetColumns, configuration);
        int priority = config.getStrategyPriorities().containsKey(strategyType)
                ? config.getStrategyPriorities().get(strategyType) : defaultPriority;
        plans.add(new StrategyPlan(strategyId, StrategyFamily.RVD, targetColumns,
                configuration, priority, StrategyStatus.PLANNED));
    }

    private static Map<String, String> base(String strategyType) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        // 所有策略配置都必须带 strategyType，执行器依赖它路由到具体策略实现。
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, strategyType);
        return configuration;
    }

    private static boolean isColumnEnabled(ColumnMetadata column, StrategyConfig config) {
        // includedColumns 为空表示不限制字段范围。
        // 非空时只有显式包含的字段才参与生成。
        return column.isDetectable()
                && (config.getIncludedColumns().isEmpty()
                || config.getIncludedColumns().contains(column.getName()))
                && !config.getExcludedColumns().contains(column.getName());
    }

    private static boolean isStrategyTypeEnabled(String strategyType, StrategyConfig config) {
        // includedStrategyTypes 为空表示不限制策略类型。
        // excludedStrategyTypes 始终拥有最终否决权。
        return (config.getIncludedStrategyTypes().isEmpty()
                || config.getIncludedStrategyTypes().contains(strategyType))
                && !config.getExcludedStrategyTypes().contains(strategyType);
    }

    private static boolean isRvdTypeSupported(String dataType) {
        String normalized = dataType == null ? "" : dataType.toLowerCase(java.util.Locale.ROOT);
        // RVD 只处理可比较的标量字段。
        // 复杂结构字段通常缺少稳定的一对多关系语义。
        return !normalized.contains("array") && !normalized.contains("map")
                && !normalized.contains("struct") && !normalized.contains("binary");
    }

    private static String formatDouble(double value) {
        // 去除多余尾随零。
        // 这样可以避免 3.0 和 3 这类等价值进入哈希后产生不必要差异。
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
