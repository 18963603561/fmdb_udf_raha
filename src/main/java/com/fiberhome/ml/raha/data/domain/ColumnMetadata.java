package com.fiberhome.ml.raha.data.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Objects;

/**
 * 描述输入数据集中的一个字段及其检测属性。
 */
public final class ColumnMetadata {

    /** 字段名称。 */
    private final String name;
    /** 字段在输入模式中的序号，从零开始。 */
    private final int ordinal;
    /** Spark 或数据源返回的字段类型名称。 */
    private final String dataType;
    /** 字段是否允许为空。 */
    private final boolean nullable;
    /** 字段是否参与 Raha 检测。 */
    private final boolean detectable;
    /** 字段是否包含需要脱敏的敏感数据。 */
    private final boolean sensitive;

    public ColumnMetadata(String name,
                          int ordinal,
                          String dataType,
                          boolean nullable,
                          boolean detectable,
                          boolean sensitive) {
        this.name = ValueUtils.requireNotBlank(name, "字段名称");
        if (ordinal < 0) {
            throw new IllegalArgumentException("字段序号不能小于 0");
        }
        this.ordinal = ordinal;
        this.dataType = ValueUtils.requireNotBlank(dataType, "字段类型");
        this.nullable = nullable;
        this.detectable = detectable;
        this.sensitive = sensitive;
    }

    public String getName() {
        return name;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isDetectable() {
        return detectable;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ColumnMetadata)) {
            return false;
        }
        ColumnMetadata that = (ColumnMetadata) object;
        return ordinal == that.ordinal
                && nullable == that.nullable
                && detectable == that.detectable
                && sensitive == that.sensitive
                && name.equals(that.name)
                && dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ordinal, dataType, nullable, detectable, sensitive);
    }
}

