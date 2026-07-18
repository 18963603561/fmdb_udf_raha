package com.fiberhome.ml.raha.feature.domain;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一个单元格在指定特征字典下的稀疏特征向量。
 */
public final class SparseFeatureRow {

    /** 稳定单元格标识。 */
    private final String cellId;
    /** 单元格所属字段。 */
    private final String columnName;
    /** 单元格稳定坐标，兼容旧对象时允许为空。 */
    private final CellCoordinate coordinate;
    /** 单元格原始值哈希，兼容旧对象时允许为空。 */
    private final String valueHash;
    /** 可选脱敏展示值。 */
    private final String maskedValue;
    /** 使用的特征字典版本。 */
    private final String featureDictionaryVersion;
    /** 非默认特征编号和值。 */
    private final Map<Integer, Double> values;
    /** 用于结果解释的特征摘要。 */
    private final Map<String, String> summary;

    public SparseFeatureRow(String cellId,
                            String columnName,
                            String featureDictionaryVersion,
                            Map<Integer, Double> values,
                            Map<String, String> summary) {
        this(cellId, columnName, null, null, null,
                featureDictionaryVersion, values, summary);
    }

    public SparseFeatureRow(String cellId,
                            String columnName,
                            CellCoordinate coordinate,
                            String valueHash,
                            String maskedValue,
                            String featureDictionaryVersion,
                            Map<Integer, Double> values,
                            Map<String, String> summary) {
        this.cellId = ValueUtils.requireNotBlank(cellId, "单元格标识");
        this.columnName = ValueUtils.requireNotBlank(columnName, "字段名称");
        if (coordinate != null) {
            if (!coordinate.toCellId().equals(this.cellId)
                    || !coordinate.getColumnName().equals(this.columnName)) {
                throw new IllegalArgumentException("特征单元格坐标与标识不一致");
            }
            if (valueHash == null || !valueHash.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("特征值必须使用 SHA-256 哈希");
            }
        } else if (valueHash != null || maskedValue != null) {
            throw new IllegalArgumentException("缺少单元格坐标时不能保存值信息");
        }
        this.coordinate = coordinate;
        this.valueHash = valueHash;
        this.maskedValue = maskedValue;
        this.featureDictionaryVersion = ValueUtils.requireNotBlank(
                featureDictionaryVersion, "特征字典版本");
        validateValues(values);
        this.values = values == null
                ? Collections.<Integer, Double>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<Integer, Double>(values));
        this.summary = summary == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(summary));
    }

    private static void validateValues(Map<Integer, Double> values) {
        if (values == null) {
            return;
        }
        for (Map.Entry<Integer, Double> entry : values.entrySet()) {
            if (entry.getKey() == null || entry.getKey() < 0
                    || entry.getValue() == null
                    || Double.isNaN(entry.getValue())
                    || Double.isInfinite(entry.getValue())) {
                throw new IllegalArgumentException("稀疏特征编号和值必须有效");
            }
        }
    }

    public String getCellId() {
        return cellId;
    }

    public String getColumnName() {
        return columnName;
    }

    public CellCoordinate getCoordinate() {
        return coordinate;
    }

    public String getValueHash() {
        return valueHash;
    }

    public String getMaskedValue() {
        return maskedValue;
    }

    public String getFeatureDictionaryVersion() {
        return featureDictionaryVersion;
    }

    public Map<Integer, Double> getValues() {
        return values;
    }

    public Map<String, String> getSummary() {
        return summary;
    }
}
