package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.core.RahaProperties;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存 FMDB 持久化总开关和默认表初始化配置。
 */
public final class FmdbPersistenceConfig {

    /** 是否启用 FMDB 物理表持久化。 */
    private final boolean enabled;
    /** 是否自动创建不存在的默认表。 */
    private final boolean autoCreateTables;
    /** 自动建表 SQL 的类路径资源。 */
    private final String schemaResource;

    /**
     * 创建 FMDB 持久化配置。
     *
     * @param enabled 是否启用持久化
     * @param autoCreateTables 是否自动创建默认表
     * @param schemaResource 建表 SQL 类路径资源
     */
    public FmdbPersistenceConfig(boolean enabled,
                                 boolean autoCreateTables,
                                 String schemaResource) {
        this.enabled = enabled;
        this.autoCreateTables = autoCreateTables;
        this.schemaResource = ValueUtils.requireNotBlank(
                schemaResource, "FMDB 建表脚本资源");
    }

    /**
     * 从项目默认配置和部署覆盖配置创建持久化配置。
     *
     * @return FMDB 持久化配置
     */
    public static FmdbPersistenceConfig fromDefaults() {
        RahaProperties properties = RahaDefaultConfigProvider.properties();
        return new FmdbPersistenceConfig(
                properties.getBoolean("raha.persistence.enabled"),
                properties.getBoolean("raha.persistence.schema.auto-create"),
                properties.getRequired("raha.persistence.schema.resource"));
    }

    /**
     * 判断指定产物是否应写入 FMDB。
     *
     * @param intermediateArtifact 是否属于可选中间产物
     * @param saveIntermediate 当前任务是否允许保存中间产物
     * @return 是否允许持久化
     */
    public boolean shouldPersist(boolean intermediateArtifact,
                                 boolean saveIntermediate) {
        // 总开关优先，中间明细还必须得到当前任务级开关授权。
        return enabled && (!intermediateArtifact || saveIntermediate);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isAutoCreateTables() {
        return autoCreateTables;
    }

    public String getSchemaResource() {
        return schemaResource;
    }
}
