package com.fiberhome.ml.raha.support;

/**
 * 统一训练和预测时的值规范化规则。
 */
public final class ValueNormalizer {

    private ValueNormalizer() {
    }

    /**
     * 将空值转换为空字符串并去除首尾空白，保留大小写和内部空格。
     *
     * @param value 原始值
     * @return 规范化文本
     */
    public static String normalize(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    /**
     * 判断文本是否为十进制数字。
     *
     * @param value 规范化文本
     * @return 是否为数字
     */
    public static boolean isNumeric(String value) {
        return value != null && value.matches("[-+]?\\d+(\\.\\d+)?");
    }
}
