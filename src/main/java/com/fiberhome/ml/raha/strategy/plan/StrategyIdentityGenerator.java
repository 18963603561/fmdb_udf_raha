package com.fiberhome.ml.raha.strategy.plan;

import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.util.HashUtils;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 生成策略配置哈希和可稳定去重的策略标识。
 */
public final class StrategyIdentityGenerator {

    private StrategyIdentityGenerator() {
    }

    /**
     * 计算策略配置哈希，配置键按字典序编码。
     *
     * @param configuration 策略配置
     * @return SHA-256 配置哈希
     */
    public static String configurationHash(Map<String, String> configuration) {
        TreeMap<String, String> sorted = new TreeMap<String, String>();
        if (configuration != null) {
            sorted.putAll(configuration);
        }
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            appendToken(canonical, entry.getKey());
            appendToken(canonical, entry.getValue());
        }
        return HashUtils.sha256Hex(canonical.toString());
    }

    /**
     * 生成包含策略族、目标字段顺序和配置哈希的稳定标识。
     *
     * @param family 策略族
     * @param targetColumns 目标字段，关系策略的字段顺序有业务含义
     * @param configuration 策略配置
     * @return 稳定策略标识
     */
    public static String strategyId(StrategyFamily family,
                                    List<String> targetColumns,
                                    Map<String, String> configuration) {
        if (family == null || targetColumns == null || targetColumns.isEmpty()) {
            throw new IllegalArgumentException("策略族和目标字段不能为空");
        }
        StringBuilder canonical = new StringBuilder();
        appendToken(canonical, family.name());
        for (String targetColumn : targetColumns) {
            appendToken(canonical, targetColumn);
        }
        appendToken(canonical, configurationHash(configuration));
        return HashUtils.sha256Hex(canonical.toString());
    }

    private static void appendToken(StringBuilder builder, String value) {
        String text = value == null ? "<null>" : value;
        builder.append(text.length()).append(':').append(text);
    }
}
