package com.fiberhome.ml.raha.detection;

import com.fiberhome.ml.raha.data.DetectionResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 聚合检测结果、策略配置、命中原因和特征摘要。
 */
public final class DetectionExplanation {

    /** 被解释的检测结果。 */
    private final DetectionResult result;
    /** 关联策略解释。 */
    private final List<StrategyExplanation> strategies;
    /** 生成判断时的脱敏特征摘要。 */
    private final Map<String, String> featureSummary;

    public DetectionExplanation(DetectionResult result,
                                List<StrategyExplanation> strategies,
                                Map<String, String> featureSummary) {
        if (result == null || strategies == null || featureSummary == null) {
            throw new IllegalArgumentException("检测解释参数不能为空");
        }
        this.result = result;
        this.strategies = Collections.unmodifiableList(
                new ArrayList<StrategyExplanation>(strategies));
        this.featureSummary = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(featureSummary));
    }

    public DetectionResult getResult() { return result; }
    public List<StrategyExplanation> getStrategies() { return strategies; }
    public Map<String, String> getFeatureSummary() { return featureSummary; }
}
