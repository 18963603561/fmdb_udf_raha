package com.fiberhome.ml.raha.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * 统一生成可读、路径安全且稳定的业务标识片段。
 */
public final class ReadableIdUtils {

    /** 可读版本时间格式，固定 UTC 避免不同节点时区生成不同版本。 */
    private static final DateTimeFormatter VERSION_TIME =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSS")
                    .withZone(ZoneOffset.UTC);
    /** FMDB 表输入逻辑数据集前缀。 */
    private static final String FMDB_TABLE_PREFIX = "fmdb-table:";
    /** 缺少库名前缀时使用的默认库名。 */
    private static final String DEFAULT_DATABASE = "default";

    private ReadableIdUtils() {
    }

    /**
     * 生成可读版本时间片段。
     *
     * @param epochMillis 毫秒时间戳
     * @return 形如 yyyyMMddHHmmss.SSS 的 UTC 时间片段
     */
    public static String timestamp(long epochMillis) {
        if (epochMillis <= 0L) {
            throw new IllegalArgumentException("可读标识时间必须大于 0");
        }
        return VERSION_TIME.format(Instant.ofEpochMilli(epochMillis));
    }

    /**
     * 规范化库表或逻辑来源名称。
     *
     * <p>该方法会去除 FMDB 表前缀和常见引用符号，无库名前缀时补齐默认库名。</p>
     *
     * @param value 输入来源、表名或逻辑数据集标识
     * @return 可读且稳定的来源名称
     */
    public static String normalizeSourceName(String value) {
        String text = ValueUtils.requireNotBlank(value, "可读来源名称").trim();
        if (text.toLowerCase(Locale.ROOT).startsWith(FMDB_TABLE_PREFIX)) {
            text = text.substring(FMDB_TABLE_PREFIX.length());
        }
        String[] segments = text.split("\\s*\\.\\s*", -1);
        StringBuilder normalized = new StringBuilder();
        if (segments.length == 1) {
            normalized.append(DEFAULT_DATABASE).append('.');
        }
        for (int index = 0; index < segments.length; index++) {
            String segment = stripQuotes(segments[index].trim());
            if (segment.isEmpty()) {
                throw new IllegalArgumentException("可读来源名称包含空层级：" + value);
            }
            if (index > 0) {
                normalized.append('.');
            }
            normalized.append(safeToken(segment));
        }
        return normalized.toString();
    }

    /**
     * 生成仅由安全字符组成的标识片段。
     *
     * @param value 原始片段
     * @return 小写安全片段
     */
    public static String safeToken(String value) {
        String text = ValueUtils.requireNotBlank(value, "可读标识片段")
                .trim().toLowerCase(Locale.ROOT);
        text = stripQuotes(text);
        text = text.replaceAll("[\\\\/:*?\"<>|\\s]+", "_");
        text = text.replaceAll("[^a-z0-9._@\\-]+", "_");
        text = text.replaceAll("_+", "_");
        text = trimEdge(text, '_');
        text = trimEdge(text, '.');
        return text.isEmpty() ? "unknown" : text;
    }

    /**
     * 生成安全文件名，不包含平台路径分隔符。
     *
     * @param value 原始名称
     * @return 可作为单个文件名或路径片段的名称
     */
    public static String safeFileName(String value) {
        String token = safeToken(value);
        token = token.replace('@', '_');
        token = token.replaceAll("\\.+", ".");
        token = trimEdge(token, '.');
        return token.isEmpty() ? "unknown" : token;
    }

    /**
     * 按前缀、来源和时间生成可读业务标识。
     *
     * @param prefix 标识前缀
     * @param sourceName 来源表或逻辑来源
     * @param epochMillis 生成时间
     * @return 形如 prefix_source@yyyyMMddHHmmss.SSS 的标识
     */
    public static String prefixedVersion(String prefix,
                                         String sourceName,
                                         long epochMillis) {
        return safeToken(prefix) + "_" + normalizeSourceName(sourceName)
                + "@" + timestamp(epochMillis);
    }

    /**
     * 按前缀、普通业务片段和时间生成可读标识。
     *
     * @param prefix 标识前缀
     * @param sourceToken 普通业务片段，不按库表名补齐
     * @param epochMillis 生成时间
     * @return 形如 prefix_token@yyyyMMddHHmmss.SSS 的标识
     */
    public static String prefixedTokenVersion(String prefix,
                                              String sourceToken,
                                              long epochMillis) {
        return safeToken(prefix) + "_" + safeToken(sourceToken)
                + "@" + timestamp(epochMillis);
    }

    /**
     * 从可读模型集合版本提取时间后缀。
     *
     * @param version 可读版本
     * @return at 符号后的版本片段
     */
    public static String suffixAfterAt(String version) {
        String text = ValueUtils.requireNotBlank(version, "可读版本");
        int index = text.lastIndexOf('@');
        if (index < 0 || index == text.length() - 1) {
            throw new IllegalArgumentException("可读版本缺少时间后缀：" + version);
        }
        return text.substring(index + 1);
    }

    /**
     * 从可读模型集合版本提取来源片段。
     *
     * @param version 可读版本
     * @return at 符号前的来源片段
     */
    public static String sourceBeforeAt(String version) {
        String text = ValueUtils.requireNotBlank(version, "可读版本");
        int index = text.lastIndexOf('@');
        if (index <= 0) {
            throw new IllegalArgumentException("可读版本缺少来源片段：" + version);
        }
        return text.substring(0, index);
    }

    private static String stripQuotes(String value) {
        String text = value;
        boolean changed = true;
        while (changed && text.length() >= 2) {
            changed = false;
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '`' && last == '`')
                    || (first == '"' && last == '"')
                    || (first == '[' && last == ']')) {
                text = text.substring(1, text.length() - 1).trim();
                changed = true;
            }
        }
        return text;
    }

    private static String trimEdge(String value, char edge) {
        int start = 0;
        int end = value.length();
        while (start < end && value.charAt(start) == edge) {
            start++;
        }
        while (end > start && value.charAt(end - 1) == edge) {
            end--;
        }
        return value.substring(start, end);
    }
}
