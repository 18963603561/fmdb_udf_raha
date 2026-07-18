package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.config.FeatureConfig;
import com.fiberhome.ml.raha.util.HashUtils;

import java.util.List;

/**
 * 根据字段、特征定义和规范化配置生成稳定字典版本。
 */
public final class FeatureDictionaryVersioner {

    public String versionOf(String columnName,
                            List<FeatureDefinition> definitions,
                            FeatureConfig config) {
        if (columnName == null || definitions == null || config == null) {
            throw new IllegalArgumentException("特征字典版本参数不能为空");
        }
        StringBuilder canonical = new StringBuilder();
        append(canonical, columnName);
        append(canonical, config.isTrimValue());
        append(canonical, config.isLowerCaseValue());
        append(canonical, config.isNormalizeWidth());
        append(canonical, config.isContextFeaturesEnabled());
        append(canonical, config.isRemoveConstantFeatures());
        append(canonical, config.getMaxFeatureCount());
        for (FeatureDefinition definition : definitions) {
            append(canonical, definition.getIndex());
            append(canonical, definition.getName());
            append(canonical, definition.getFeatureType());
            append(canonical, definition.getSource());
            append(canonical, definition.getDefaultValue());
        }
        return HashUtils.sha256Hex(canonical.toString());
    }

    private static void append(StringBuilder builder, Object value) {
        String text = String.valueOf(value);
        builder.append(text.length()).append(':').append(text);
    }
}
