package com.fiberhome.ml.raha.detection.explanation;

import com.fiberhome.ml.raha.data.type.StrategyFamily;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存检测结果关联策略的配置、原因和分数解释。
 */
public final class StrategyExplanation {

    /** 策略标识。 */
    private final String strategyId;
    /** 策略族。 */
    private final StrategyFamily strategyFamily;
    /** 策略类型。 */
    private final String strategyType;
    /** 可重放策略配置。 */
    private final Map<String, String> configuration;
    /** 命中原因编码。 */
    private final String reasonCode;
    /** 结构化原因详情。 */
    private final Map<String, String> reasonDetails;
    /** 策略候选分数。 */
    private final Double strategyScore;

    public StrategyExplanation(String strategyId,
                               StrategyFamily strategyFamily,
                               String strategyType,
                               Map<String, String> configuration,
                               String reasonCode,
                               Map<String, String> reasonDetails,
                               Double strategyScore) {
        if (strategyId == null || strategyFamily == null || strategyType == null
                || reasonCode == null) {
            throw new IllegalArgumentException("策略解释必填字段不能为空");
        }
        this.strategyId = strategyId;
        this.strategyFamily = strategyFamily;
        this.strategyType = strategyType;
        this.configuration = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(configuration));
        this.reasonCode = reasonCode;
        this.reasonDetails = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(reasonDetails));
        this.strategyScore = strategyScore;
    }

    public String getStrategyId() { return strategyId; }
    public StrategyFamily getStrategyFamily() { return strategyFamily; }
    public String getStrategyType() { return strategyType; }
    public Map<String, String> getConfiguration() { return configuration; }
    public String getReasonCode() { return reasonCode; }
    public Map<String, String> getReasonDetails() { return reasonDetails; }
    public Double getStrategyScore() { return strategyScore; }
}
