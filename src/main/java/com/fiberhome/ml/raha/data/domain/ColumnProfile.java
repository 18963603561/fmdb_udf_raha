package com.fiberhome.ml.raha.data.domain;

import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存策略生成所需的列级统计画像。
 */
public final class ColumnProfile {

    /** 单列最多保存的高频值哈希数量，避免高基数字段占用过多内存。 */
    private static final int MAX_VALUE_FREQUENCY_COUNT =
            RahaDefaultConfigProvider.factory().profileMaxValueFrequencyCount();
    /** 画像对应的字段名称。 */
    private final String columnName;
    /** 字段总单元格数量。 */
    private final long totalCount;
    /** 字段空值数量。 */
    private final long nullCount;
    /** 字段空白值数量。 */
    private final long blankCount;
    /** 字段不同值数量。 */
    private final long distinctCount;
    /** 非空值最小长度，没有非空值时为负一。 */
    private final int minLength;
    /** 非空值最大长度，没有非空值时为负一。 */
    private final int maxLength;
    /** 非空值平均长度，没有非空值时为零。 */
    private final double averageLength;
    /** 可解析为数值的单元格数量。 */
    private final long numericCount;
    /** 可解析为数值的非空值占比。 */
    private final double numericRatio;
    /** 数值最小值，没有数值时为空。 */
    private final Double numericMin;
    /** 数值最大值，没有数值时为空。 */
    private final Double numericMax;
    /** 数值平均值，没有数值时为空。 */
    private final Double numericMean;
    /** 数值总体标准差，没有数值或兼容旧画像时为空。 */
    private final Double numericStandardDeviation;
    /** 数值第一四分位数，没有数值时为空。 */
    private final Double numericQ1;
    /** 数值中位数，没有数值时为空。 */
    private final Double numericMedian;
    /** 数值第三四分位数，没有数值时为空。 */
    private final Double numericQ3;
    /** 字符或值类型到出现数量的统计。 */
    private final Map<String, Long> typeCounts;
    /** 高频非空值的 MD5 哈希到出现数量的统计。 */
    private final Map<String, Long> valueHashFrequencies;

    public ColumnProfile(String columnName,
                         long totalCount,
                         long nullCount,
                         long distinctCount,
                         int minLength,
                         int maxLength,
                         double numericRatio,
                         Map<String, Long> typeCounts) {
        this(columnName, totalCount, nullCount, 0L, distinctCount,
                minLength, maxLength, 0.0d,
                Math.round((totalCount - nullCount) * numericRatio), numericRatio,
                null, null, null, null, null, null, null, typeCounts,
                Collections.<String, Long>emptyMap());
    }

    public ColumnProfile(String columnName,
                         long totalCount,
                         long nullCount,
                         long blankCount,
                         long distinctCount,
                         int minLength,
                         int maxLength,
                         double averageLength,
                         long numericCount,
                         double numericRatio,
                         Double numericMin,
                         Double numericMax,
                         Double numericMean,
                         Double numericQ1,
                         Double numericMedian,
                         Double numericQ3,
                         Map<String, Long> typeCounts) {
        this(columnName, totalCount, nullCount, blankCount, distinctCount,
                minLength, maxLength, averageLength, numericCount, numericRatio,
                numericMin, numericMax, numericMean, null, numericQ1, numericMedian, numericQ3,
                typeCounts, Collections.<String, Long>emptyMap());
    }

    public ColumnProfile(String columnName,
                         long totalCount,
                         long nullCount,
                         long blankCount,
                         long distinctCount,
                         int minLength,
                         int maxLength,
                         double averageLength,
                         long numericCount,
                         double numericRatio,
                         Double numericMin,
                         Double numericMax,
                         Double numericMean,
                         Double numericQ1,
                         Double numericMedian,
                         Double numericQ3,
                         Map<String, Long> typeCounts,
                         Map<String, Long> valueHashFrequencies) {
        this(columnName, totalCount, nullCount, blankCount, distinctCount,
                minLength, maxLength, averageLength, numericCount, numericRatio,
                numericMin, numericMax, numericMean, null, numericQ1, numericMedian, numericQ3,
                typeCounts, valueHashFrequencies);
    }

    public ColumnProfile(String columnName,
                         long totalCount,
                         long nullCount,
                         long blankCount,
                         long distinctCount,
                         int minLength,
                         int maxLength,
                         double averageLength,
                         long numericCount,
                         double numericRatio,
                         Double numericMin,
                         Double numericMax,
                         Double numericMean,
                         Double numericStandardDeviation,
                         Double numericQ1,
                         Double numericMedian,
                         Double numericQ3,
                         Map<String, Long> typeCounts,
                         Map<String, Long> valueHashFrequencies) {
        this.columnName = ValueUtils.requireNotBlank(columnName, "画像字段名称");
        // 画像统计必须满足基本数量关系，防止错误画像进入策略生成。
        if (totalCount < 0L || nullCount < 0L || blankCount < 0L || distinctCount < 0L
                || numericCount < 0L || nullCount > totalCount || blankCount > totalCount - nullCount
                || distinctCount > totalCount || numericCount > totalCount - nullCount) {
            throw new IllegalArgumentException("列画像数量统计非法");
        }
        boolean noLength = minLength == -1 && maxLength == -1;
        boolean validLength = minLength >= 0 && maxLength >= minLength;
        if (!noLength && !validLength) {
            throw new IllegalArgumentException("列画像长度范围非法");
        }
        if (Double.isNaN(averageLength) || averageLength < 0.0d
                || Double.isNaN(numericRatio) || numericRatio < 0.0d || numericRatio > 1.0d) {
            throw new IllegalArgumentException("平均长度和数值占比非法");
        }
        validateNumericStatistics(numericCount, numericMin, numericMax, numericMean,
                numericQ1, numericMedian, numericQ3);
        if (numericStandardDeviation != null
                && (Double.isNaN(numericStandardDeviation)
                || Double.isInfinite(numericStandardDeviation)
                || numericStandardDeviation < 0.0d)) {
            throw new IllegalArgumentException("数值标准差必须为有限非负数值");
        }
        this.totalCount = totalCount;
        this.nullCount = nullCount;
        this.blankCount = blankCount;
        this.distinctCount = distinctCount;
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.averageLength = averageLength;
        this.numericCount = numericCount;
        this.numericRatio = numericRatio;
        this.numericMin = numericMin;
        this.numericMax = numericMax;
        this.numericMean = numericMean;
        this.numericStandardDeviation = numericStandardDeviation;
        this.numericQ1 = numericQ1;
        this.numericMedian = numericMedian;
        this.numericQ3 = numericQ3;
        this.typeCounts = typeCounts == null
                ? Collections.<String, Long>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Long>(typeCounts));
        for (Map.Entry<String, Long> entry : this.typeCounts.entrySet()) {
            if (entry.getKey() == null || entry.getKey().trim().isEmpty()
                    || entry.getValue() == null || entry.getValue() < 0L) {
                throw new IllegalArgumentException("列画像类型计数必须使用非空名称和非负数量");
            }
        }
        this.valueHashFrequencies = valueHashFrequencies == null
                ? Collections.<String, Long>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Long>(valueHashFrequencies));
        if (this.valueHashFrequencies.size() > MAX_VALUE_FREQUENCY_COUNT) {
            throw new IllegalArgumentException("列画像高频值摘要不能超过配置上限："
                    + MAX_VALUE_FREQUENCY_COUNT);
        }
        for (Map.Entry<String, Long> entry : this.valueHashFrequencies.entrySet()) {
            // 频率摘要只能保存值哈希，禁止原始敏感值进入画像对象。
            if (entry.getKey() == null || !entry.getKey().matches("[0-9a-f]{32}")
                    || entry.getValue() == null || entry.getValue() <= 0L) {
                throw new IllegalArgumentException("列画像值频率必须使用 MD5 哈希和正数数量");
            }
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getNullCount() {
        return nullCount;
    }

    /**
     * 返回非空单元格数量。
     *
     * @return 非空单元格数量
     */
    public long getNonNullCount() {
        return totalCount - nullCount;
    }

    /**
     * 返回非空单元格占总单元格的比例，空数据集返回零。
     *
     * @return 范围为零到一的非空率
     */
    public double getNonNullRatio() {
        return totalCount == 0L ? 0.0d : (double) getNonNullCount() / totalCount;
    }

    public long getBlankCount() {
        return blankCount;
    }

    public long getDistinctCount() {
        return distinctCount;
    }

    /**
     * 返回非空值中除首次出现外的重复单元格数量。
     *
     * @return 重复单元格数量
     */
    public long getDuplicateCount() {
        return Math.max(0L, getNonNullCount() - distinctCount);
    }

    /**
     * 返回重复单元格占非空单元格的比例，无非空值时返回零。
     *
     * @return 范围为零到一的重复率
     */
    public double getDuplicateRatio() {
        return getNonNullCount() == 0L
                ? 0.0d
                : (double) getDuplicateCount() / getNonNullCount();
    }

    public int getMinLength() {
        return minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public double getAverageLength() {
        return averageLength;
    }

    public long getNumericCount() {
        return numericCount;
    }

    public double getNumericRatio() {
        return numericRatio;
    }

    public Double getNumericMin() {
        return numericMin;
    }

    public Double getNumericMax() {
        return numericMax;
    }

    public Double getNumericMean() {
        return numericMean;
    }

    public Double getNumericStandardDeviation() {
        return numericStandardDeviation;
    }

    public Double getNumericQ1() {
        return numericQ1;
    }

    public Double getNumericMedian() {
        return numericMedian;
    }

    public Double getNumericQ3() {
        return numericQ3;
    }

    public Map<String, Long> getTypeCounts() {
        return typeCounts;
    }

    /**
     * 返回最多二十项高频非空值的哈希及出现数量，不包含原始值。
     *
     * @return 不可修改的值哈希频率映射
     */
    public Map<String, Long> getValueHashFrequencies() {
        return valueHashFrequencies;
    }

    private static void validateNumericStatistics(long numericCount,
                                                  Double numericMin,
                                                  Double numericMax,
                                                  Double numericMean,
                                                  Double numericQ1,
                                                  Double numericMedian,
                                                  Double numericQ3) {
        Double[] values = new Double[]{numericMin, numericMax, numericMean,
                numericQ1, numericMedian, numericQ3};
        if (numericCount == 0L) {
            for (Double value : values) {
                if (value != null) {
                    throw new IllegalArgumentException("无数值单元格时数值统计必须为空");
                }
            }
            return;
        }
        boolean allNull = true;
        for (Double value : values) {
            allNull = allNull && value == null;
        }
        // 兼容仅提供数值占比的基础画像；完整画像必须一次性提供全部数值统计。
        if (allNull) {
            return;
        }
        for (Double value : values) {
            if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
                throw new IllegalArgumentException("存在数值单元格时数值统计必须完整且有限");
            }
        }
        if (numericMin > numericQ1 || numericQ1 > numericMedian
                || numericMedian > numericQ3 || numericQ3 > numericMax) {
            throw new IllegalArgumentException("数值分位数顺序非法");
        }
    }
}
