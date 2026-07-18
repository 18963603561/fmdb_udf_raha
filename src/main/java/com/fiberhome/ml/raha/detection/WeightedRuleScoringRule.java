package com.fiberhome.ml.raha.detection;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.feature.FeatureDefinition;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.strategy.StrategyHit;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用策略族可靠度和策略自身分数进行噪声或融合，并加入受限上下文信号。
 */
public final class WeightedRuleScoringRule implements DetectionScoringRule {

    @Override
    public DetectionScore score(SparseFeatureRow row,
                                FeatureDictionary dictionary,
                                List<StrategyHit> hits,
                                ModelConfig config) {
        if (row == null || dictionary == null || hits == null || config == null) {
            throw new IllegalArgumentException("检测评分参数不能为空");
        }
        Map<String, Double> strongestByStrategy = new LinkedHashMap<String, Double>();
        Map<StrategyFamily, Double> familySignals =
                new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        double strategyComplement = 1.0d;
        for (StrategyHit hit : hits) {
            double reliability = hit.getStrategyScore() == null ? 1.0d : hit.getStrategyScore();
            double familyWeight = config.getStrategyFamilyWeights().containsKey(hit.getStrategyFamily())
                    ? config.getStrategyFamilyWeights().get(hit.getStrategyFamily())
                    : config.getDefaultStrategyFamilyWeight();
            double signal = bounded(reliability * familyWeight);
            Double existing = strongestByStrategy.get(hit.getStrategyId());
            if (existing == null || signal > existing) {
                strongestByStrategy.put(hit.getStrategyId(), signal);
            }
            Double familyExisting = familySignals.get(hit.getStrategyFamily());
            if (familyExisting == null || signal > familyExisting) {
                familySignals.put(hit.getStrategyFamily(), signal);
            }
        }
        for (Double signal : strongestByStrategy.values()) {
            strategyComplement *= 1.0d - signal;
        }
        double strategyScore = bounded(1.0d - strategyComplement);
        Map<String, Double> featureValues = featureValues(row, dictionary);
        double contextSignal = contextSignal(featureValues, config);
        double contextContribution = bounded(config.getContextWeight() * contextSignal);
        double score = bounded(1.0d - (1.0d - strategyScore) * (1.0d - contextContribution));
        return new DetectionScore(score, strategyScore, contextSignal, familySignals);
    }

    private static Map<String, Double> featureValues(SparseFeatureRow row,
                                                     FeatureDictionary dictionary) {
        Map<String, Double> values = new LinkedHashMap<String, Double>();
        for (Map.Entry<Integer, FeatureDefinition> entry : dictionary.getDefinitions().entrySet()) {
            Double value = row.getValues().get(entry.getKey());
            values.put(entry.getValue().getName(),
                    value == null ? entry.getValue().getDefaultValue() : value);
        }
        return values;
    }

    private static double contextSignal(Map<String, Double> values,
                                        ModelConfig config) {
        double signal = 0.0d;
        signal = Math.max(signal, config.getRareContextSignalWeight()
                * binary(values, "context.column.frequency_bucket.rare"));
        signal = Math.max(signal, config.getRvdConflictContextSignalWeight()
                * positive(values, "context.neighbor.rvd.conflict_count"));
        signal = Math.max(signal, config.getNullContextSignalWeight()
                * binary(values, "context.value.is_null"));
        signal = Math.max(signal, config.getBlankContextSignalWeight()
                * binary(values, "context.value.is_blank"));
        signal = Math.max(signal, config.getMixedContextSignalWeight()
                * binary(values, "context.value.type.mixed"));
        return bounded(signal);
    }

    private static double binary(Map<String, Double> values, String name) {
        return values.containsKey(name) && values.get(name) > 0.0d ? 1.0d : 0.0d;
    }

    private static double positive(Map<String, Double> values, String name) {
        return values.containsKey(name) && values.get(name) > 0.0d ? 1.0d : 0.0d;
    }

    private static double bounded(double value) {
        return Math.max(0.0d, Math.min(1.0d, value));
    }
}
