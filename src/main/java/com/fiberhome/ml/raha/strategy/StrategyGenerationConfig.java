package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 控制策略计划生成阶段的算法阈值、占位值和默认优先级。
 */
public final class StrategyGenerationConfig {

    /** OD 低频值比例。 */
    private final double lowFrequencyRatio;
    /** OD 数值距离策略最小数值样本数。 */
    private final int minimumNumericCount;
    /** OD 数值距离标准差倍数阈值。 */
    private final double zThreshold;
    /** OD 四分位策略最小数值样本数。 */
    private final int minimumQuantileCount;
    /** OD 和 PVD 四分位距倍数。 */
    private final double iqrMultiplier;
    /** PVD 少数模式比例。 */
    private final double minorityRatio;
    /** PVD 自动格式类型。 */
    private final String formatType;
    /** PVD 格式适用最小比例。 */
    private final double formatMinRatio;
    /** 使用逗号编码的空值和特殊占位值。 */
    private final String placeholders;
    /** OD 低频策略默认优先级。 */
    private final int odLowFrequencyPriority;
    /** OD 数值距离策略默认优先级。 */
    private final int odNumericDistancePriority;
    /** OD 四分位策略默认优先级。 */
    private final int odQuantilePriority;
    /** PVD 字符集策略默认优先级。 */
    private final int pvdCharacterSetPriority;
    /** PVD 长度策略默认优先级。 */
    private final int pvdLengthPriority;
    /** PVD 空值占位策略默认优先级。 */
    private final int pvdNullPlaceholderPriority;
    /** PVD 类型格式策略默认优先级。 */
    private final int pvdTypeFormatPriority;
    /** RVD 一对多策略默认优先级。 */
    private final int rvdOneToManyPriority;

    public StrategyGenerationConfig(double lowFrequencyRatio,
                                    int minimumNumericCount,
                                    double zThreshold,
                                    int minimumQuantileCount,
                                    double iqrMultiplier,
                                    double minorityRatio,
                                    String formatType,
                                    double formatMinRatio,
                                    String placeholders,
                                    int odLowFrequencyPriority,
                                    int odNumericDistancePriority,
                                    int odQuantilePriority,
                                    int pvdCharacterSetPriority,
                                    int pvdLengthPriority,
                                    int pvdNullPlaceholderPriority,
                                    int pvdTypeFormatPriority,
                                    int rvdOneToManyPriority) {
        if (!ratio(lowFrequencyRatio) || minimumNumericCount <= 0
                || zThreshold <= 0.0d || minimumQuantileCount <= 0
                || iqrMultiplier <= 0.0d || !ratio(minorityRatio)
                || !ratio(formatMinRatio)) {
            throw new IllegalArgumentException("策略生成比例、样本数和阈值必须有效");
        }
        int[] priorities = new int[]{odLowFrequencyPriority,
                odNumericDistancePriority, odQuantilePriority,
                pvdCharacterSetPriority, pvdLengthPriority,
                pvdNullPlaceholderPriority, pvdTypeFormatPriority,
                rvdOneToManyPriority};
        for (int priority : priorities) {
            if (priority < 0) {
                throw new IllegalArgumentException("策略默认优先级不能小于 0");
            }
        }
        this.lowFrequencyRatio = lowFrequencyRatio;
        this.minimumNumericCount = minimumNumericCount;
        this.zThreshold = zThreshold;
        this.minimumQuantileCount = minimumQuantileCount;
        this.iqrMultiplier = iqrMultiplier;
        this.minorityRatio = minorityRatio;
        this.formatType = ValueUtils.requireNotBlank(formatType, "PVD 格式类型");
        this.formatMinRatio = formatMinRatio;
        this.placeholders = ValueUtils.requireNotBlank(placeholders, "PVD 占位值");
        this.odLowFrequencyPriority = odLowFrequencyPriority;
        this.odNumericDistancePriority = odNumericDistancePriority;
        this.odQuantilePriority = odQuantilePriority;
        this.pvdCharacterSetPriority = pvdCharacterSetPriority;
        this.pvdLengthPriority = pvdLengthPriority;
        this.pvdNullPlaceholderPriority = pvdNullPlaceholderPriority;
        this.pvdTypeFormatPriority = pvdTypeFormatPriority;
        this.rvdOneToManyPriority = rvdOneToManyPriority;
    }

    private static boolean ratio(double value) {
        return !Double.isNaN(value) && value >= 0.0d && value <= 1.0d;
    }

    public double getLowFrequencyRatio() { return lowFrequencyRatio; }
    public int getMinimumNumericCount() { return minimumNumericCount; }
    public double getZThreshold() { return zThreshold; }
    public int getMinimumQuantileCount() { return minimumQuantileCount; }
    public double getIqrMultiplier() { return iqrMultiplier; }
    public double getMinorityRatio() { return minorityRatio; }
    public String getFormatType() { return formatType; }
    public double getFormatMinRatio() { return formatMinRatio; }
    public String getPlaceholders() { return placeholders; }
    public int getOdLowFrequencyPriority() { return odLowFrequencyPriority; }
    public int getOdNumericDistancePriority() { return odNumericDistancePriority; }
    public int getOdQuantilePriority() { return odQuantilePriority; }
    public int getPvdCharacterSetPriority() { return pvdCharacterSetPriority; }
    public int getPvdLengthPriority() { return pvdLengthPriority; }
    public int getPvdNullPlaceholderPriority() { return pvdNullPlaceholderPriority; }
    public int getPvdTypeFormatPriority() { return pvdTypeFormatPriority; }
    public int getRvdOneToManyPriority() { return rvdOneToManyPriority; }
}
