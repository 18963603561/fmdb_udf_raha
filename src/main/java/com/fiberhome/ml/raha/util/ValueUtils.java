package com.fiberhome.ml.raha.util;

/**
 * 提供领域对象和配置对象共用的值校验方法。
 */
public final class ValueUtils {

    private ValueUtils() {
    }

    /**
     * 校验文本不为空白，并返回原文本。
     *
     * @param value 待校验文本
     * @param fieldName 字段名称
     * @return 校验通过的原文本
     */
    public static String requireNotBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + "不能为空");
        }
        return value;
    }
}
