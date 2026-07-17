package com.fiberhome.ml.raha.support;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 平台薄入口使用的 UTF-8 表单编码工具。
 */
public final class FormCodec {

    private FormCodec() {
    }

    /**
     * 解析键值请求，拒绝重复键和缺少等号的参数。
     *
     * @param value 表单编码文本
     * @return 参数映射
     */
    public static Map<String, String> decode(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST, "请求内容不能为空");
        }
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (String token : value.split("&")) {
            int separator = token.indexOf('=');
            if (separator <= 0) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                        "请求参数必须使用键值格式");
            }
            String key = decodeComponent(token.substring(0, separator));
            String item = decodeComponent(token.substring(separator + 1));
            if (result.containsKey(key)) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                        "请求参数不能重复：" + key);
            }
            result.put(key, item);
        }
        return result;
    }

    public static String encode(String value) {
        try {
            return URLEncoder.encode(value == null ? "" : value, "UTF-8");
        } catch (UnsupportedEncodingException exception) {
            throw new IllegalStateException("运行环境不支持 UTF-8", exception);
        }
    }

    private static String decodeComponent(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException exception) {
            throw new IllegalStateException("运行环境不支持 UTF-8", exception);
        } catch (IllegalArgumentException exception) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                    "请求不是合法表单编码", exception);
        }
    }
}
