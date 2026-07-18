package com.fiberhome.ml.raha.config;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 生成配置稳定文本的内部工具，使用长度前缀避免分隔符导致摘要碰撞。
 */
final class ConfigTextUtils {

    private ConfigTextUtils() {
    }

    static String token(Object value) {
        String text = value == null ? "<null>" : String.valueOf(value);
        return text.length() + ":" + text;
    }

    static String sortedTokens(Collection<?> values) {
        if (values == null) {
            return token(null);
        }
        TreeSet<String> sortedValues = new TreeSet<String>();
        for (Object value : values) {
            sortedValues.add(String.valueOf(value));
        }
        StringBuilder builder = new StringBuilder();
        for (String value : sortedValues) {
            builder.append(token(value));
        }
        return token(builder.toString());
    }

    static String sortedMapTokens(Map<?, ?> values) {
        if (values == null) {
            return token(null);
        }
        TreeMap<String, String> sortedValues = new TreeMap<String, String>();
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            sortedValues.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : sortedValues.entrySet()) {
            builder.append(token(entry.getKey())).append(token(entry.getValue()));
        }
        return token(builder.toString());
    }
}
