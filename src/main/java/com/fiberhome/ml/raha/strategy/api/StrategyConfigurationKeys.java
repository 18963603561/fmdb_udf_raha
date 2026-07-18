package com.fiberhome.ml.raha.strategy.api;

/**
 * 策略计划配置字段名称，配置会进入稳定哈希并支持结果重放。
 */
public final class StrategyConfigurationKeys {

    /** 策略实现类型。 */
    public static final String STRATEGY_TYPE = "strategyType";
    /** 低频最大允许次数。 */
    public static final String MAX_FREQUENCY = "maxFrequency";
    /** 标准差距离阈值。 */
    public static final String Z_THRESHOLD = "zThreshold";
    /** 画像均值。 */
    public static final String NUMERIC_MEAN = "numericMean";
    /** 画像标准差。 */
    public static final String NUMERIC_STANDARD_DEVIATION = "numericStandardDeviation";
    /** 第一四分位数。 */
    public static final String NUMERIC_Q1 = "numericQ1";
    /** 第三四分位数。 */
    public static final String NUMERIC_Q3 = "numericQ3";
    /** 四分位距倍数。 */
    public static final String IQR_MULTIPLIER = "iqrMultiplier";
    /** 少数模式比例阈值。 */
    public static final String MINORITY_RATIO = "minorityRatio";
    /** 空值和占位值集合，使用逗号分隔。 */
    public static final String PLACEHOLDERS = "placeholders";
    /** 格式类型，AUTO 表示从数据中选择适用格式。 */
    public static final String FORMAT_TYPE = "formatType";
    /** 格式最小匹配率。 */
    public static final String FORMAT_MIN_RATIO = "formatMinRatio";
    /** RVD 依赖左字段。 */
    public static final String LEFT_COLUMN = "leftColumn";
    /** RVD 依赖右字段。 */
    public static final String RIGHT_COLUMN = "rightColumn";

    private StrategyConfigurationKeys() {
    }
}
