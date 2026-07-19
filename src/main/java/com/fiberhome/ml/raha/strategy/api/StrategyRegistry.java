package com.fiberhome.ml.raha.strategy.api;

import com.fiberhome.ml.raha.strategy.impl.od.LowFrequencyStrategy;
import com.fiberhome.ml.raha.strategy.impl.od.NumericDistanceStrategy;
import com.fiberhome.ml.raha.strategy.impl.od.QuantileOutlierStrategy;
import com.fiberhome.ml.raha.strategy.impl.pvd.CharacterSetStrategy;
import com.fiberhome.ml.raha.strategy.impl.pvd.LengthAnomalyStrategy;
import com.fiberhome.ml.raha.strategy.impl.pvd.NullPlaceholderStrategy;
import com.fiberhome.ml.raha.strategy.impl.pvd.TypeFormatStrategy;
import com.fiberhome.ml.raha.strategy.impl.rvd.OneToManyConflictStrategy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 按策略类型分发策略实现，防止编排层依赖具体算法类。
 */
public final class StrategyRegistry {

    /** 策略类型到实现的映射。 */
    private final Map<String, DetectionStrategy> strategies;

    public StrategyRegistry(Iterable<DetectionStrategy> values) {
        if (values == null) {
            throw new IllegalArgumentException("策略实现集合不能为空");
        }
        Map<String, DetectionStrategy> registered = new LinkedHashMap<String, DetectionStrategy>();
        for (DetectionStrategy strategy : values) {
            if (strategy == null || strategy.getStrategyType() == null
                    || strategy.getStrategyFamily() == null) {
                throw new IllegalArgumentException("策略实现、类型和策略族不能为空");
            }
            if (registered.put(strategy.getStrategyType(), strategy) != null) {
                throw new IllegalArgumentException("策略类型重复注册：" + strategy.getStrategyType());
            }
        }
        this.strategies = Collections.unmodifiableMap(registered);
    }

    /**
     * 创建内置 OD、PVD 和 RVD 策略注册表。
     *
     * @return 内置策略注册表
     */
    public static StrategyRegistry defaults() {
        return new StrategyRegistry(java.util.Arrays.<DetectionStrategy>asList(
                new LowFrequencyStrategy(), new NumericDistanceStrategy(),
                new QuantileOutlierStrategy(), new CharacterSetStrategy(),
                new LengthAnomalyStrategy(), new NullPlaceholderStrategy(),
                new TypeFormatStrategy(), new OneToManyConflictStrategy()));
    }

    public DetectionStrategy get(String strategyType) {
        DetectionStrategy strategy = strategies.get(strategyType);
        if (strategy == null) {
            throw new IllegalArgumentException("未注册的策略类型：" + strategyType);
        }
        return strategy;
    }
}
