package com.fiberhome.ml.raha.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 提供稳定哈希能力，用于配置版本、策略标识和单元格标识。
 */
public final class HashUtils {

    /** 十六进制字符表。 */
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    private HashUtils() {
    }

    /**
     * 计算 UTF-8 文本的 SHA-256 十六进制摘要。
     *
     * @param value 待计算文本，不允许为空
     * @return 长度固定的十六进制摘要
     */
    public static String sha256Hex(String value) {
        if (value == null) {
            throw new IllegalArgumentException("待计算哈希的文本不能为空");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return toHex(bytes);
        } catch (NoSuchAlgorithmException exception) {
            // JDK 8 必须提供 SHA-256；若环境缺失则属于不可恢复的运行时异常。
            throw new IllegalStateException("当前 Java 环境不支持 SHA-256", exception);
        }
    }

    private static String toHex(byte[] bytes) {
        char[] chars = new char[bytes.length * 2];
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            chars[index * 2] = HEX_DIGITS[value >>> 4];
            chars[index * 2 + 1] = HEX_DIGITS[value & 0x0f];
        }
        return new String(chars);
    }
}
