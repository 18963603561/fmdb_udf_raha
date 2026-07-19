package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 定义特征字典在训练列级产物表中的稳定 JSON 协议。
 */
public final class FmdbFeatureDictionaryCodec {

    private FmdbFeatureDictionaryCodec() {
    }

    public static String write(FeatureDictionary dictionary) {
        if (dictionary == null) {
            throw new IllegalArgumentException("特征字典不能为空");
        }
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        root.put("columnName", dictionary.getColumnName());
        root.put("createdAt", dictionary.getCreatedAt());
        List<Map<String, Object>> definitions =
                new ArrayList<Map<String, Object>>();
        List<FeatureDefinition> sorted = new ArrayList<FeatureDefinition>(
                dictionary.getDefinitions().values());
        Collections.sort(sorted, Comparator.comparingInt(
                FeatureDefinition::getIndex));
        for (FeatureDefinition definition : sorted) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("defaultValue", definition.getDefaultValue());
            item.put("index", definition.getIndex());
            item.put("name", definition.getName());
            item.put("source", definition.getSource());
            item.put("type", definition.getFeatureType().name());
            definitions.add(item);
        }
        root.put("definitions", definitions);
        root.put("version", dictionary.getVersion());
        return FmdbJsonCodec.write(root);
    }

    @SuppressWarnings("unchecked")
    public static FeatureDictionary read(String json) {
        Map<String, Object> root = FmdbJsonCodec.readObject(json);
        String version = requiredText(root, "version");
        String columnName = requiredText(root, "columnName");
        long createdAt = requiredNumber(root, "createdAt").longValue();
        Object rawDefinitions = root.get("definitions");
        if (!(rawDefinitions instanceof List)) {
            throw new IllegalArgumentException("特征字典 JSON 缺少 definitions");
        }
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        for (Object raw : (List<Object>) rawDefinitions) {
            if (!(raw instanceof Map)) {
                throw new IllegalArgumentException("特征字典定义必须是 JSON 对象");
            }
            Map<String, Object> item = (Map<String, Object>) raw;
            int index = requiredNumber(item, "index").intValue();
            FeatureDefinition previous = definitions.put(index,
                    new FeatureDefinition(index, requiredText(item, "name"),
                            FeatureType.valueOf(requiredText(item, "type")),
                            requiredText(item, "source"),
                            requiredNumber(item, "defaultValue").doubleValue()));
            if (previous != null) {
                throw new IllegalArgumentException("特征字典 JSON 包含重复编号：" + index);
            }
        }
        return new FeatureDictionary(version, columnName, definitions, createdAt);
    }

    private static String requiredText(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
            throw new IllegalArgumentException("特征字典 JSON 缺少字段：" + key);
        }
        return (String) value;
    }

    private static Number requiredNumber(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("特征字典 JSON 缺少数值字段：" + key);
        }
        return (Number) value;
    }
}
