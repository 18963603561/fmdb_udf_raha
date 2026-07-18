package com.fiberhome.ml.raha.config.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 提供严格类型解析的不可变 Raha 属性集合。
 */
public final class RahaProperties {

    /** 按配置键保存的不可变文本值。 */
    private final Map<String, String> values;

    public RahaProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("Raha 属性集合不能为空");
        }
        Map<String, String> copied = new LinkedHashMap<String, String>();
        for (String name : properties.stringPropertyNames()) {
            copied.put(name, properties.getProperty(name));
        }
        this.values = Collections.unmodifiableMap(copied);
    }

    public String getRequired(String key) {
        String value = values.get(key);
        if (value == null) {
            throw invalid(key, "缺少必填配置");
        }
        return value.trim();
    }

    public int getInt(String key) {
        try {
            return Integer.parseInt(getRequired(key));
        } catch (NumberFormatException exception) {
            throw invalid(key, "配置必须为整数", exception);
        }
    }

    public long getLong(String key) {
        try {
            return Long.parseLong(getRequired(key));
        } catch (NumberFormatException exception) {
            throw invalid(key, "配置必须为长整数", exception);
        }
    }

    public double getDouble(String key) {
        try {
            double value = Double.parseDouble(getRequired(key));
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw invalid(key, "配置必须为有限小数");
            }
            return value;
        } catch (NumberFormatException exception) {
            throw invalid(key, "配置必须为小数", exception);
        }
    }

    public boolean getBoolean(String key) {
        String value = getRequired(key);
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw invalid(key, "配置必须为 true 或 false");
    }

    public <E extends Enum<E>> E getEnum(String key, Class<E> enumType) {
        if (enumType == null) {
            throw new IllegalArgumentException("枚举配置类型不能为空");
        }
        try {
            return Enum.valueOf(enumType,
                    getRequired(key).toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw invalid(key, "配置不是受支持的枚举值", exception);
        }
    }

    public Set<String> getCsvSet(String key) {
        String value = getRequired(key);
        if (value.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new LinkedHashSet<String>();
        for (String item : value.split(",", -1)) {
            String normalized = item.trim();
            if (normalized.isEmpty() || !result.add(normalized)) {
                throw invalid(key, "逗号分隔配置包含空值或重复值");
            }
        }
        return Collections.unmodifiableSet(result);
    }

    public Map<String, String> getStringMap(String key) {
        String value = getRequired(key);
        if (value.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (String item : value.split(",", -1)) {
            int separator = item.indexOf(':');
            if (separator <= 0 || separator == item.length() - 1) {
                throw invalid(key, "映射配置必须使用 name:value 格式");
            }
            String name = item.substring(0, separator).trim();
            String mappedValue = item.substring(separator + 1).trim();
            if (name.isEmpty() || mappedValue.isEmpty()
                    || result.put(name, mappedValue) != null) {
                throw invalid(key, "映射配置包含空值或重复键");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, String> asMap() {
        return values;
    }

    private static RahaConfigurationException invalid(String key,
                                                       String message) {
        return new RahaConfigurationException(key,
                message + "，propertyKey=" + key);
    }

    private static RahaConfigurationException invalid(String key,
                                                       String message,
                                                       Throwable cause) {
        return new RahaConfigurationException(key,
                message + "，propertyKey=" + key, cause);
    }
}
