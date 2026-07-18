package com.fiberhome.ml.raha.strategy.domain;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 表示一个基础策略对某个单元格产生的候选错误信号。
 */
public final class StrategyHit {

    /** 所属 Raha 任务标识。 */
    private final String jobId;
    /** 所属策略执行阶段标识。 */
    private final String stageId;
    /** 产生信号的策略标识。 */
    private final String strategyId;
    /** 产生信号的策略族。 */
    private final StrategyFamily strategyFamily;
    /** 命中的单元格坐标。 */
    private final CellCoordinate coordinate;
    /** 命中时原始值的哈希。 */
    private final String valueHash;
    /** 稳定原因编码。 */
    private final String reasonCode;
    /** 可结构化保存的原因详情。 */
    private final Map<String, String> reasonDetails;
    /** 策略自身分数，没有分数时允许为空。 */
    private final Double strategyScore;
    /** 策略处理该结果所属任务的耗时，单位毫秒。 */
    private final long runtimeMillis;
    /** 策略执行状态。 */
    private final StrategyStatus status;

    public StrategyHit(String jobId,
                       String stageId,
                       String strategyId,
                       StrategyFamily strategyFamily,
                       CellCoordinate coordinate,
                       String valueHash,
                       String reasonCode,
                       Map<String, String> reasonDetails,
                       Double strategyScore,
                       long runtimeMillis,
                       StrategyStatus status) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
        this.strategyId = ValueUtils.requireNotBlank(strategyId, "策略标识");
        if (strategyFamily == null || coordinate == null || status == null) {
            throw new IllegalArgumentException("策略族、单元格坐标和策略状态不能为空");
        }
        if (runtimeMillis < 0L) {
            throw new IllegalArgumentException("策略耗时不能小于 0");
        }
        if (strategyScore != null && (Double.isNaN(strategyScore)
                || strategyScore < 0.0d || strategyScore > 1.0d)) {
            throw new IllegalArgumentException("策略分数必须位于 0 到 1 之间");
        }
        this.strategyFamily = strategyFamily;
        this.coordinate = coordinate;
        this.valueHash = ValueUtils.requireNotBlank(valueHash, "值哈希");
        this.reasonCode = ValueUtils.requireNotBlank(reasonCode, "原因编码");
        this.reasonDetails = reasonDetails == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(reasonDetails));
        this.strategyScore = strategyScore;
        this.runtimeMillis = runtimeMillis;
        this.status = status;
    }

    public String getJobId() {
        return jobId;
    }

    public String getStageId() {
        return stageId;
    }

    public String getStrategyId() {
        return strategyId;
    }

    public StrategyFamily getStrategyFamily() {
        return strategyFamily;
    }

    public CellCoordinate getCoordinate() {
        return coordinate;
    }

    public String getValueHash() {
        return valueHash;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public Map<String, String> getReasonDetails() {
        return reasonDetails;
    }

    public Double getStrategyScore() {
        return strategyScore;
    }

    public long getRuntimeMillis() {
        return runtimeMillis;
    }

    public StrategyStatus getStatus() {
        return status;
    }
}

