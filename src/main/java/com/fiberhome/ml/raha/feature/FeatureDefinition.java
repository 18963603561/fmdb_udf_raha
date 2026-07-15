package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.data.FeatureType;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 描述特征字典中的一个稳定特征编号和来源。
 */
public final class FeatureDefinition {

    /** 特征在稀疏向量中的非负编号。 */
    private final int index;
    /** 稳定且可解释的特征名称。 */
    private final String name;
    /** 特征数据类型。 */
    private final FeatureType featureType;
    /** 特征来源策略或上下文模块。 */
    private final String source;
    /** 单元格缺少该特征时使用的默认值。 */
    private final double defaultValue;

    public FeatureDefinition(int index,
                             String name,
                             FeatureType featureType,
                             String source,
                             double defaultValue) {
        if (index < 0) {
            throw new IllegalArgumentException("特征编号不能小于 0");
        }
        if (featureType == null) {
            throw new IllegalArgumentException("特征类型不能为空");
        }
        if (Double.isNaN(defaultValue) || Double.isInfinite(defaultValue)) {
            throw new IllegalArgumentException("特征默认值必须为有限数值");
        }
        this.index = index;
        this.name = ValueUtils.requireNotBlank(name, "特征名称");
        this.featureType = featureType;
        this.source = ValueUtils.requireNotBlank(source, "特征来源");
        this.defaultValue = defaultValue;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public FeatureType getFeatureType() {
        return featureType;
    }

    public String getSource() {
        return source;
    }

    public double getDefaultValue() {
        return defaultValue;
    }
}

