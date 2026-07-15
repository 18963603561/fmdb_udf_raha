package com.fiberhome.ml.raha.util;

/**
 * 提供日志和检测结果使用的值哈希和基础脱敏能力。
 */
public final class ValueProtectionUtils {

    private ValueProtectionUtils() {
    }

    /**
     * 对任意值生成稳定哈希，空值使用固定标记。
     *
     * @param value 原始值
     * @return SHA-256 值哈希
     */
    public static String hashValue(Object value) {
        return HashUtils.sha256Hex(value == null ? "<null>" : String.valueOf(value));
    }

    /**
     * 保留指定数量的前缀和后缀字符，其余字符替换为星号。
     *
     * @param value 原始文本
     * @param visiblePrefix 可见前缀长度
     * @param visibleSuffix 可见后缀长度
     * @return 脱敏文本，输入为空时返回空
     */
    public static String mask(String value, int visiblePrefix, int visibleSuffix) {
        if (value == null) {
            return null;
        }
        if (visiblePrefix < 0 || visibleSuffix < 0) {
            throw new IllegalArgumentException("可见前缀和后缀长度不能小于 0");
        }
        // 原值过短时全部隐藏，避免可见部分组合后泄露完整值。
        if (value.length() <= visiblePrefix + visibleSuffix) {
            return repeat('*', value.length());
        }
        int hiddenLength = value.length() - visiblePrefix - visibleSuffix;
        return value.substring(0, visiblePrefix)
                + repeat('*', hiddenLength)
                + value.substring(value.length() - visibleSuffix);
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int index = 0; index < count; index++) {
            builder.append(value);
        }
        return builder.toString();
    }
}

