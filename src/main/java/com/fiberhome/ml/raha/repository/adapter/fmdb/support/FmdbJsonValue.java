package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 对解码后的通用 JSON 值执行严格类型转换和必填字段校验。
 */
public final class FmdbJsonValue {

    private FmdbJsonValue() {
    }

    public static String requiredText(Map<String, Object> values, String key) {
        String value = optionalText(values, key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("FMDB JSON 缺少文本字段：" + key);
        }
        return value;
    }

    public static String optionalText(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("FMDB JSON 字段类型不是文本：" + key);
        }
        return (String) value;
    }

    public static Number requiredNumber(Map<String, Object> values, String key) {
        Number value = optionalNumber(values, key);
        if (value == null) {
            throw new IllegalArgumentException("FMDB JSON 缺少数值字段：" + key);
        }
        return value;
    }

    public static Number optionalNumber(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("FMDB JSON 字段类型不是数值：" + key);
        }
        return (Number) value;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> objectList(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return Collections.emptyList();
        }
        if (!(value instanceof List)) {
            throw new IllegalArgumentException("FMDB JSON 字段类型不是数组：" + key);
        }
        return Collections.unmodifiableList(new ArrayList<Object>((List<Object>) value));
    }

    public static List<String> stringList(Map<String, Object> values, String key) {
        List<String> result = new ArrayList<String>();
        for (Object value : objectList(values, key)) {
            if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
                throw new IllegalArgumentException("FMDB JSON 数组包含非法文本：" + key);
            }
            result.add((String) value);
        }
        return Collections.unmodifiableList(result);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> objectMap(Object value, String name) {
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("FMDB JSON 字段类型不是对象：" + name);
        }
        return (Map<String, Object>) value;
    }

    public static Map<String, String> stringMap(Map<String, Object> values, String key) {
        Object raw = values.get(key);
        if (raw == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> source = objectMap(raw, key);
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (!(entry.getValue() instanceof String)) {
                throw new IllegalArgumentException("FMDB JSON 映射值不是文本：" + key);
            }
            result.put(entry.getKey(), (String) entry.getValue());
        }
        return Collections.unmodifiableMap(result);
    }

    public static Map<String, Long> longMap(Map<String, Object> values, String key) {
        Object raw = values.get(key);
        if (raw == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> source = objectMap(raw, key);
        Map<String, Long> result = new LinkedHashMap<String, Long>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (!(entry.getValue() instanceof Number)) {
                throw new IllegalArgumentException("FMDB JSON 映射值不是数值：" + key);
            }
            result.put(entry.getKey(), ((Number) entry.getValue()).longValue());
        }
        return Collections.unmodifiableMap(result);
    }
}
