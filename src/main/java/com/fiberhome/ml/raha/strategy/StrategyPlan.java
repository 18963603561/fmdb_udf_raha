package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.data.StrategyStatus;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * 描述一个可独立执行和重放的检测策略计划。
 */
public final class StrategyPlan {

    /** 策略稳定标识。 */
    private final String strategyId;
    /** 策略所属 Raha 策略族。 */
    private final StrategyFamily strategyFamily;
    /** 策略读取或判断的目标字段。 */
    private final List<String> targetColumns;
    /** 可重放的策略参数。 */
    private final Map<String, String> configuration;
    /** 策略配置的稳定哈希。 */
    private final String configurationHash;
    /** 策略执行优先级，数值越小越优先。 */
    private final int priority;
    /** 策略计划状态。 */
    private final StrategyStatus status;

    public StrategyPlan(String strategyId,
                        StrategyFamily strategyFamily,
                        List<String> targetColumns,
                        Map<String, String> configuration,
                        int priority,
                        StrategyStatus status) {
        this.strategyId = ValueUtils.requireNotBlank(strategyId, "策略标识");
        if (strategyFamily == null) {
            throw new IllegalArgumentException("策略族不能为空");
        }
        if (targetColumns == null || targetColumns.isEmpty()) {
            throw new IllegalArgumentException("策略目标字段不能为空");
        }
        LinkedHashSet<String> uniqueColumns = new LinkedHashSet<String>();
        for (String targetColumn : targetColumns) {
            uniqueColumns.add(ValueUtils.requireNotBlank(targetColumn, "策略目标字段"));
        }
        if (uniqueColumns.size() != targetColumns.size()) {
            throw new IllegalArgumentException("策略目标字段不能重复");
        }
        if (priority < 0) {
            throw new IllegalArgumentException("策略优先级不能小于 0");
        }
        if (status == null) {
            throw new IllegalArgumentException("策略状态不能为空");
        }
        this.strategyFamily = strategyFamily;
        this.targetColumns = Collections.unmodifiableList(new ArrayList<String>(uniqueColumns));
        Map<String, String> validatedConfiguration = new LinkedHashMap<String, String>();
        if (configuration != null) {
            for (Map.Entry<String, String> entry : configuration.entrySet()) {
                validatedConfiguration.put(
                        ValueUtils.requireNotBlank(entry.getKey(), "策略配置键"),
                        ValueUtils.requireNotBlank(entry.getValue(), "策略配置值"));
            }
        }
        this.configuration = Collections.unmodifiableMap(validatedConfiguration);
        this.configurationHash = StrategyIdentityGenerator.configurationHash(this.configuration);
        this.priority = priority;
        this.status = status;
    }

    public String getStrategyId() {
        return strategyId;
    }

    public StrategyFamily getStrategyFamily() {
        return strategyFamily;
    }

    public List<String> getTargetColumns() {
        return targetColumns;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    /**
     * 返回策略配置哈希，用于策略计划和结果去重。
     *
     * @return 配置 SHA-256 哈希
     */
    public String getConfigurationHash() {
        return configurationHash;
    }

    public int getPriority() {
        return priority;
    }

    public StrategyStatus getStatus() {
        return status;
    }
}
