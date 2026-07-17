package com.fiberhome.ml.raha.support;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 提供请求、模式、值和模型版本使用的确定性哈希。
 */
public final class HashUtils {

    private HashUtils() {
    }

    /**
     * 计算 UTF-8 文本的 SHA-256 十六进制摘要。
     *
     * @param value 待计算文本，空值按空字符串处理
     * @return 六十四位小写十六进制摘要
     */
    public static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest((value == null ? "" : value)
                    .getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte item : bytes) {
                builder.append(String.format("%02x", item & 0xff));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("运行环境不支持 SHA-256", exception);
        }
    }

    /**
     * 生成带业务前缀的短标识，短标识只用于可读版本，不替代完整指纹校验。
     *
     * @param prefix 业务前缀
     * @param value 指纹输入
     * @return 业务标识
     */
    public static String shortId(String prefix, String value) {
        return prefix + "_" + sha256(value).substring(0, 24);
    }
}
