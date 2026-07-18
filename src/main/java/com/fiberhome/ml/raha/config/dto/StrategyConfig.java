package com.fiberhome.ml.raha.config.dto;

import com.fiberhome.ml.raha.config.core.ConfigTextUtils;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 控制策略族、策略数量、字段范围和单策略执行限制。
 */
public final class StrategyConfig {

    /** 启用的 Raha 策略族。 */
    private final Set<StrategyFamily> strategyFamilies;
    /** 一个任务允许生成的最大策略数量。 */
    private final int maxStrategyCount;
    /** 只允许参与检测的字段集合，为空表示不限制。 */
    private final Set<String> includedColumns;
    /** 禁止参与检测的字段集合。 */
    private final Set<String> excludedColumns;
    /** RVD 允许生成的最大列对数量。 */
    private final int maxRvdColumnPairs;
    /** 单策略执行超时时间，单位毫秒。 */
    private final long strategyTimeoutMillis;
    /** 是否启用历史策略过滤。 */
    private final boolean strategyFilteringEnabled;
    /** 只允许生成的策略类型，为空表示不限制。 */
    private final Set<String> includedStrategyTypes;
    /** 禁止生成的策略类型。 */
    private final Set<String> excludedStrategyTypes;
    /** 策略类型对应的优先级覆盖，数值越小越优先。 */
    private final Map<String, Integer> strategyPriorities;

    public StrategyConfig(Set<StrategyFamily> strategyFamilies,
                          int maxStrategyCount,
                          Set<String> includedColumns,
                          Set<String> excludedColumns,
                          int maxRvdColumnPairs,
                          long strategyTimeoutMillis,
                          boolean strategyFilteringEnabled) {
        this(strategyFamilies, maxStrategyCount, includedColumns, excludedColumns,
                maxRvdColumnPairs, strategyTimeoutMillis, strategyFilteringEnabled,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String, Integer>emptyMap());
    }

    public StrategyConfig(Set<StrategyFamily> strategyFamilies,
                          int maxStrategyCount,
                          Set<String> includedColumns,
                          Set<String> excludedColumns,
                          int maxRvdColumnPairs,
                          long strategyTimeoutMillis,
                          boolean strategyFilteringEnabled,
                          Set<String> includedStrategyTypes,
                          Set<String> excludedStrategyTypes,
                          Map<String, Integer> strategyPriorities) {
        this.strategyFamilies = immutableEnumSet(strategyFamilies);
        this.maxStrategyCount = maxStrategyCount;
        this.includedColumns = immutableStringSet(includedColumns);
        this.excludedColumns = immutableStringSet(excludedColumns);
        this.maxRvdColumnPairs = maxRvdColumnPairs;
        this.strategyTimeoutMillis = strategyTimeoutMillis;
        this.strategyFilteringEnabled = strategyFilteringEnabled;
        this.includedStrategyTypes = immutableStringSet(includedStrategyTypes);
        this.excludedStrategyTypes = immutableStringSet(excludedStrategyTypes);
        this.strategyPriorities = strategyPriorities == null
                ? Collections.<String, Integer>emptyMap()
                : Collections.unmodifiableMap(
                new LinkedHashMap<String, Integer>(strategyPriorities));
    }

    /**
     * 创建首期默认策略配置，启用 OD、PVD 和 RVD。
     *
     * @return 默认策略配置
     */
    public static StrategyConfig defaults() {
        return RahaDefaultConfigProvider.factory().strategyConfig();
    }

    public Set<StrategyFamily> getStrategyFamilies() {
        return strategyFamilies;
    }

    public int getMaxStrategyCount() {
        return maxStrategyCount;
    }

    public Set<String> getIncludedColumns() {
        return includedColumns;
    }

    public Set<String> getExcludedColumns() {
        return excludedColumns;
    }

    public int getMaxRvdColumnPairs() {
        return maxRvdColumnPairs;
    }

    public long getStrategyTimeoutMillis() {
        return strategyTimeoutMillis;
    }

    public boolean isStrategyFilteringEnabled() {
        return strategyFilteringEnabled;
    }

    public Set<String> getIncludedStrategyTypes() {
        return includedStrategyTypes;
    }

    public Set<String> getExcludedStrategyTypes() {
        return excludedStrategyTypes;
    }

    public Map<String, Integer> getStrategyPriorities() {
        return strategyPriorities;
    }

    String toCanonicalString() {
        return ConfigTextUtils.sortedTokens(strategyFamilies)
                + ConfigTextUtils.token(maxStrategyCount)
                + ConfigTextUtils.sortedTokens(includedColumns)
                + ConfigTextUtils.sortedTokens(excludedColumns)
                + ConfigTextUtils.token(maxRvdColumnPairs)
                + ConfigTextUtils.token(strategyTimeoutMillis)
                + ConfigTextUtils.token(strategyFilteringEnabled)
                + ConfigTextUtils.sortedTokens(includedStrategyTypes)
                + ConfigTextUtils.sortedTokens(excludedStrategyTypes)
                + ConfigTextUtils.sortedMapTokens(strategyPriorities);
    }

    private static Set<StrategyFamily> immutableEnumSet(Set<StrategyFamily> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(EnumSet.copyOf(values));
    }

    private static Set<String> immutableStringSet(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<String>(values));
    }
}
