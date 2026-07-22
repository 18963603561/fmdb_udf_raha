package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 聚类提供方解析器。
 *
 * <p>统一把配置中的 provider 解析为实际聚类实现。AUTO 会保留为自动路由模式，
 * 由聚类器在运行时按单列样本量选择 Smile 精确路径或可扩展近似路径。</p>
 */
public final class ClusteringProviderResolver {

    /** 默认使用的自动聚类提供方。 */
    public static final String DEFAULT_PROVIDER = "AUTO";

    /** 自动路由聚类提供方。 */
    private static final String AUTO_PROVIDER = "AUTO";
    /** 解析后的层次聚类提供方。 */
    private static final String HIERARCHICAL_PROVIDER = "HierarchicalColumnClusterer";
    /** 解析后的可扩展聚类提供方。 */
    private static final String SCALABLE_PROVIDER = "ScalableColumnClusterer";
    /** 解析后的 Smile 层次聚类提供方。 */
    private static final String SMILE_HIERARCHICAL_PROVIDER =
            "SmileHierarchicalColumnClusterer";

    /** 支持的 provider 别名到标准名称的映射。 */
    private static final Map<String, String> PROVIDER_ALIASES;

    static {
        Map<String, String> aliases = new HashMap<String, String>();
        registerAlias(aliases, null, DEFAULT_PROVIDER);
        registerAlias(aliases, "", DEFAULT_PROVIDER);
        registerAlias(aliases, AUTO_PROVIDER, AUTO_PROVIDER);
        registerAlias(aliases, HIERARCHICAL_PROVIDER, HIERARCHICAL_PROVIDER);
        registerAlias(aliases, SCALABLE_PROVIDER, SCALABLE_PROVIDER);
        registerAlias(aliases, SMILE_HIERARCHICAL_PROVIDER, SMILE_HIERARCHICAL_PROVIDER);
        registerAlias(aliases, "SmileHierarchical", SMILE_HIERARCHICAL_PROVIDER);
        registerAlias(aliases, "SmileAverage", SMILE_HIERARCHICAL_PROVIDER);
        registerAlias(aliases, "SmileAverageColumnClusterer", SMILE_HIERARCHICAL_PROVIDER);
        PROVIDER_ALIASES = Collections.unmodifiableMap(aliases);
    }

    private ClusteringProviderResolver() {
    }

    /**
     * 判断 provider 是否是当前工程支持的取值。
     *
     * @param provider 配置中的聚类提供方
     * @return 是否支持
     */
    public static boolean isSupported(String provider) {
        return PROVIDER_ALIASES.containsKey(normalize(provider));
    }

    /**
     * 把 provider 收敛为规范化名称。
     *
     * <p>空值和空串会归一到默认 AUTO；AUTO 会保留为独立模式，
     * 以便配置指纹能够体现自动路由语义。</p>
     *
     * @param provider 配置中的聚类提供方
     * @return 规范化后的 provider
     */
    public static String canonicalProvider(String provider) {
        String normalized = normalize(provider);
        String resolved = PROVIDER_ALIASES.get(normalized);
        return resolved == null ? trim(provider) : resolved;
    }

    /**
     * 创建实际的单列聚类实现。
     *
     * @param provider 配置中的聚类提供方
     * @param versioner 聚类版本生成器
     * @param clock 时间源
     * @return 可执行的聚类实现
     */
    public static ColumnClusterer resolve(String provider,
                                          ClusterVersioner versioner,
                                          Clock clock) {
        if (versioner == null || clock == null) {
            throw new IllegalArgumentException("聚类提供方解析依赖不能为空");
        }
        String canonical = canonicalProvider(provider);
        if (AUTO_PROVIDER.equals(canonical)) {
            return new AutoColumnClusterer(versioner, clock);
        }
        if (HIERARCHICAL_PROVIDER.equals(canonical)) {
            return new HierarchicalColumnClusterer(versioner, clock);
        }
        if (SCALABLE_PROVIDER.equals(canonical)) {
            return new ScalableColumnClusterer(versioner, clock);
        }
        if (SMILE_HIERARCHICAL_PROVIDER.equals(canonical)) {
            return new SmileHierarchicalColumnClusterer(versioner, clock);
        }
        throw new IllegalArgumentException("不支持的聚类提供方: " + provider);
    }

    /**
     * 返回默认聚类提供方。
     *
     * @return 默认 provider
     */
    public static String defaultProvider() {
        return DEFAULT_PROVIDER;
    }

    private static void registerAlias(Map<String, String> aliases,
                                      String alias,
                                      String canonical) {
        aliases.put(normalize(alias), canonical);
    }

    private static String normalize(String provider) {
        String text = trim(provider);
        if (text.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder(text.length());
        for (int index = 0; index < text.length(); index++) {
            char value = text.charAt(index);
            if (Character.isLetterOrDigit(value)) {
                builder.append(Character.toUpperCase(value));
            }
        }
        return builder.toString();
    }

    private static String trim(String provider) {
        return provider == null ? "" : provider.trim();
    }
}
