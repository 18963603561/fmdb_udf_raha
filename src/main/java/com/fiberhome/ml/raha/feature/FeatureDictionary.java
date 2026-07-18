package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 保存单列特征名称到编号的冻结映射，训练和检测必须使用同一版本。
 */
public final class FeatureDictionary {

    /** 特征字典不可变版本。 */
    private final String version;
    /** 特征字典对应的字段。 */
    private final String columnName;
    /** 按特征编号索引的定义。 */
    private final Map<Integer, FeatureDefinition> definitions;
    /** 特征字典创建时间。 */
    private final long createdAt;

    public FeatureDictionary(String version,
                             String columnName,
                             Map<Integer, FeatureDefinition> definitions,
                             long createdAt) {
        this.version = ValueUtils.requireNotBlank(version, "特征字典版本");
        this.columnName = ValueUtils.requireNotBlank(columnName, "特征字典字段");
        if (definitions == null) {
            throw new IllegalArgumentException("特征定义集合不能为空");
        }
        if (createdAt <= 0L) {
            throw new IllegalArgumentException("特征字典创建时间必须大于 0");
        }
        validateDefinitions(definitions);
        this.definitions = Collections.unmodifiableMap(
                new LinkedHashMap<Integer, FeatureDefinition>(definitions));
        this.createdAt = createdAt;
    }

    private static void validateDefinitions(Map<Integer, FeatureDefinition> definitions) {
        Set<String> names = new HashSet<String>();
        for (Map.Entry<Integer, FeatureDefinition> entry : definitions.entrySet()) {
            FeatureDefinition definition = entry.getValue();
            // 映射键必须与定义编号一致，防止训练和检测阶段取到不同特征。
            if (entry.getKey() == null || definition == null
                    || entry.getKey().intValue() != definition.getIndex()) {
                throw new IllegalArgumentException("特征定义编号与映射键不一致");
            }
            if (!names.add(definition.getName())) {
                throw new IllegalArgumentException("特征名称不能重复：" + definition.getName());
            }
        }
    }

    public String getVersion() {
        return version;
    }

    public String getColumnName() {
        return columnName;
    }

    public Map<Integer, FeatureDefinition> getDefinitions() {
        return definitions;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
