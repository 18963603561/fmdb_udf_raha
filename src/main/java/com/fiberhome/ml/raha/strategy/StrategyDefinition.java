package com.fiberhome.ml.raha.strategy;

/**
 * 可冻结并重复执行的单个策略定义。
 */
public final class StrategyDefinition {

    /** 确定性策略标识。 */
    private final String strategyId;
    /** 策略族。 */
    private final String family;
    /** 实现名称。 */
    private final String implementation;
    /** 目标字段。 */
    private final String targetColumn;
    /** 简单参数 JSON。 */
    private final String parametersJson;

    public StrategyDefinition(String strategyId, String family, String implementation,
                              String targetColumn, String parametersJson) {
        this.strategyId = strategyId;
        this.family = family;
        this.implementation = implementation;
        this.targetColumn = targetColumn;
        this.parametersJson = parametersJson;
    }

    public String getStrategyId() { return strategyId; }
    public String getFamily() { return family; }
    public String getImplementation() { return implementation; }
    public String getTargetColumn() { return targetColumn; }
    public String getParametersJson() { return parametersJson; }
}
