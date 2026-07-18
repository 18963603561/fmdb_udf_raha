package com.fiberhome.ml.raha.config.core;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 生成配置稳定文本的公共工具，供配置对象和配置校验组件生成一致摘要。
 *
 * <p>所有文本片段使用长度前缀编码，避免配置值包含分隔符时发生摘要碰撞。</p>
 */
public final class ConfigTextUtils {

    private ConfigTextUtils() {
    }

    /**
     * 将单个配置值转换为带长度前缀的稳定文本。
     *
     * @param value 配置值，允许为空
     * @return 可参与摘要计算的稳定文本
     */
    public static String token(Object value) {
        String text = value == null ? "<null>" : String.valueOf(value);
        return text.length() + ":" + text;
    }

    /**
     * 对集合值排序后生成稳定文本，消除集合遍历顺序对摘要的影响。
     *
     * @param values 配置集合，允许为空
     * @return 排序并编码后的稳定文本
     */
    public static String sortedTokens(Collection<?> values) {
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

    /**
     * 对映射键排序后生成稳定文本，消除映射遍历顺序对摘要的影响。
     *
     * @param values 配置映射，允许为空
     * @return 排序并编码后的稳定文本
     */
    public static String sortedMapTokens(Map<?, ?> values) {
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
