package com.fiberhome.ml.raha.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用标准表单编码稳定传输 UDF 参数及结构化摘要。
 */
public final class FormDataCodec {

    /** 固定字符编码。 */
    private static final String UTF_8 = "UTF-8";

    private FormDataCodec() {
    }

    /**
     * 按键排序编码映射，避免相同参数因输入顺序不同产生不同摘要。
     */
    public static String encode(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        List<String> keys = new ArrayList<String>(values.keySet());
        Collections.sort(keys);
        StringBuilder text = new StringBuilder();
        for (String key : keys) {
            String value = values.get(key);
            if (key == null || value == null) {
                throw new IllegalArgumentException("表单编码键和值不能为空");
            }
            if (text.length() > 0) {
                text.append('&');
            }
            text.append(encodeComponent(key)).append('=').append(encodeComponent(value));
        }
        return text.toString();
    }

    /**
     * 解码标准表单文本并拒绝重复键，防止参数覆盖产生歧义。
     */
    public static Map<String, String> decode(String text) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("表单参数不能为空");
        }
        Map<String, String> values = new LinkedHashMap<String, String>();
        for (String pair : text.split("&", -1)) {
            int separator = pair.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("表单参数必须使用键值格式");
            }
            String key = ValueUtils.requireNotBlank(
                    decodeComponent(pair.substring(0, separator)), "表单参数键");
            String value = decodeComponent(pair.substring(separator + 1));
            if (values.put(key, value) != null) {
                throw new IllegalArgumentException("表单参数包含重复键：" + key);
            }
        }
        return Collections.unmodifiableMap(values);
    }

    /**
     * 按原顺序编码字符串列表。
     */
    public static String encodeList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        StringBuilder text = new StringBuilder();
        for (String value : values) {
            if (value == null) {
                throw new IllegalArgumentException("列表编码值不能为空");
            }
            if (text.length() > 0) {
                text.append('&');
            }
            text.append("value=").append(encodeComponent(value));
        }
        return text.toString();
    }

    private static String encodeComponent(String value) {
        try {
            return URLEncoder.encode(value, UTF_8);
        } catch (UnsupportedEncodingException exception) {
            throw new IllegalStateException("当前 Java 环境不支持 UTF-8", exception);
        }
    }

    private static String decodeComponent(String value) {
        try {
            return URLDecoder.decode(value, UTF_8);
        } catch (UnsupportedEncodingException exception) {
            throw new IllegalStateException("当前 Java 环境不支持 UTF-8", exception);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("表单参数包含非法转义", exception);
        }
    }
}
