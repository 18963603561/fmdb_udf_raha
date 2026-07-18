package com.fiberhome.ml.raha.detection;

import com.fiberhome.ml.raha.data.StrategyFamily;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * 保存基础检测分数及可解释的策略族和上下文分量。
 */
public final class DetectionScore {

    /** 最终融合分数。 */
    private final double score;
    /** 策略信号融合分数。 */
    private final double strategyScore;
    /** 上下文异常信号。 */
    private final double contextSignal;
    /** 各策略族最强信号。 */
    private final Map<StrategyFamily, Double> familySignals;

    public DetectionScore(double score,
                          double strategyScore,
                          double contextSignal,
                          Map<StrategyFamily, Double> familySignals) {
        validate(score);
        validate(strategyScore);
        validate(contextSignal);
        EnumMap<StrategyFamily, Double> signals =
                new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        if (familySignals != null) {
            signals.putAll(familySignals);
        }
        for (Double value : signals.values()) {
            validate(value);
        }
        this.score = score;
        this.strategyScore = strategyScore;
        this.contextSignal = contextSignal;
        this.familySignals = Collections.unmodifiableMap(signals);
    }

    public double getScore() { return score; }
    public double getStrategyScore() { return strategyScore; }
    public double getContextSignal() { return contextSignal; }
    public Map<StrategyFamily, Double> getFamilySignals() { return familySignals; }

    private static void validate(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value < 0.0d || value > 1.0d) {
            throw new IllegalArgumentException("检测分量必须位于 0 到 1 之间");
        }
    }
}
