package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.util.ReadableIdUtils;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 生成模型集合、字段模型和模型文件使用的可读唯一版本。
 */
public final class ModelReadableVersioner {

    private ModelReadableVersioner() {
    }

    /**
     * 生成表级模型集合版本。
     *
     * @param sourceName 来源表或逻辑来源
     * @param createdAt 训练创建时间
     * @return 可读模型集合版本
     */
    public static String modelSetVersion(String sourceName, long createdAt) {
        return ReadableIdUtils.normalizeSourceName(sourceName)
                + "@" + ReadableIdUtils.timestamp(createdAt);
    }

    /**
     * 生成带业务唯一后缀的表级模型集合版本。
     *
     * @param sourceName 来源表或逻辑来源
     * @param createdAt 训练创建时间
     * @param uniqueness 任务号、批次号等可读唯一片段
     * @return 可读且唯一的模型集合版本
     */
    public static String modelSetVersion(String sourceName,
                                         long createdAt,
                                         String uniqueness) {
        return modelSetVersion(sourceName, createdAt)
                + "-" + ReadableIdUtils.safeToken(uniqueness);
    }

    /**
     * 根据模型集合版本生成字段级模型版本。
     *
     * @param modelSetVersion 模型集合版本
     * @param columnName 字段名称
     * @return 可读字段模型版本
     */
    public static String columnModelVersion(String modelSetVersion,
                                            String columnName) {
        String source = ReadableIdUtils.sourceBeforeAt(modelSetVersion);
        String suffix = ReadableIdUtils.suffixAfterAt(modelSetVersion);
        return source + "." + ReadableIdUtils.safeToken(columnName)
                + "@" + suffix;
    }

    /**
     * 生成缺少模型集合上下文时的字段模型版本。
     *
     * @param sourceName 来源表或逻辑来源
     * @param columnName 字段名称
     * @param qualifier 可读版本后缀
     * @return 字段模型版本
     */
    public static String columnModelVersion(String sourceName,
                                            String columnName,
                                            String qualifier) {
        return ReadableIdUtils.normalizeSourceName(sourceName)
                + "." + ReadableIdUtils.safeToken(columnName)
                + "@" + ReadableIdUtils.safeToken(qualifier);
    }

    /**
     * 生成带库表字段上下文的模型名称。
     *
     * @param prefix 调用方模型名前缀
     * @param sourceName 来源表或逻辑来源
     * @param columnName 字段名称
     * @return 可读模型名称
     */
    public static String modelName(String prefix,
                                   String sourceName,
                                   String columnName) {
        return ReadableIdUtils.safeToken(
                ValueUtils.requireNotBlank(prefix, "模型名称前缀"))
                + "-" + ReadableIdUtils.normalizeSourceName(sourceName)
                + "-" + ReadableIdUtils.safeToken(columnName);
    }

    /**
     * 生成模型文件使用的安全名称。
     *
     * @param modelVersion 模型版本
     * @return 文件名安全片段
     */
    public static String safeFileToken(String modelVersion) {
        return ReadableIdUtils.safeFileName(modelVersion);
    }
}
