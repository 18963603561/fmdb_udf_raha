package com.fiberhome.ml.raha.detection.explanation;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据检测结果反查策略配置、原因和特征摘要。
 */
public final class DetectionExplanationService {

    public DetectionExplanation explain(DetectionResult result,
                                        List<StrategyPlan> plans,
                                        List<StrategyHit> hits,
                                        SparseFeatureRow featureRow) {
        if (result == null || plans == null || hits == null || featureRow == null) {
            throw new IllegalArgumentException("检测解释输入不能为空");
        }
        if (!result.getCoordinate().toCellId().equals(featureRow.getCellId())) {
            throw new IllegalArgumentException("检测结果和特征行不是同一单元格");
        }
        Map<String, StrategyPlan> plansById = new HashMap<String, StrategyPlan>();
        for (StrategyPlan plan : plans) {
            plansById.put(plan.getStrategyId(), plan);
        }
        List<StrategyExplanation> explanations = new ArrayList<StrategyExplanation>();
        for (StrategyHit hit : hits) {
            if (!hit.getCoordinate().equals(result.getCoordinate())) {
                continue;
            }
            StrategyPlan plan = plansById.get(hit.getStrategyId());
            if (plan == null) {
                throw new IllegalStateException("检测结果关联的策略计划不存在");
            }
            explanations.add(new StrategyExplanation(hit.getStrategyId(),
                    hit.getStrategyFamily(), plan.getConfiguration().get(
                    StrategyConfigurationKeys.STRATEGY_TYPE), plan.getConfiguration(),
                    hit.getReasonCode(), hit.getReasonDetails(), hit.getStrategyScore()));
        }
        Collections.sort(explanations, new Comparator<StrategyExplanation>() {
            @Override
            public int compare(StrategyExplanation first, StrategyExplanation second) {
                int strategyCompare = first.getStrategyId().compareTo(second.getStrategyId());
                return strategyCompare == 0
                        ? first.getReasonCode().compareTo(second.getReasonCode()) : strategyCompare;
            }
        });
        return new DetectionExplanation(result, explanations, featureRow.getSummary());
    }
}
