package com.fiberhome.ml.raha.strategy;

/**
 * 迭代 3 支持的基础策略类型常量。
 */
public final class StrategyTypes {

    /** OD 值频和低频策略。 */
    public static final String OD_LOW_FREQUENCY = "OD_LOW_FREQUENCY";
    /** OD 标准差距离策略。 */
    public static final String OD_NUMERIC_DISTANCE = "OD_NUMERIC_DISTANCE";
    /** OD 四分位距策略。 */
    public static final String OD_QUANTILE = "OD_QUANTILE";
    /** PVD 字符集合策略。 */
    public static final String PVD_CHARACTER_SET = "PVD_CHARACTER_SET";
    /** PVD 长度异常策略。 */
    public static final String PVD_LENGTH = "PVD_LENGTH";
    /** PVD 空值和占位值策略。 */
    public static final String PVD_NULL_PLACEHOLDER = "PVD_NULL_PLACEHOLDER";
    /** PVD 类型和格式策略。 */
    public static final String PVD_TYPE_FORMAT = "PVD_TYPE_FORMAT";
    /** RVD 单列到单列的一对多冲突策略。 */
    public static final String RVD_ONE_TO_MANY = "RVD_ONE_TO_MANY";

    private StrategyTypes() {
    }
}
