package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 定义训练列级产物表中可以独立控制的 JSON 产物。
 */
public enum FmdbColumnArtifact {

    /** 列画像和统计信息。 */
    PROFILE("raha.persistence.column-artifact.profile.enabled"),
    /** 策略计划和版本信息。 */
    STRATEGY_PLAN("raha.persistence.column-artifact.strategy-plan.enabled"),
    /** 特征字典定义。 */
    FEATURE_DICTIONARY(
            "raha.persistence.column-artifact.feature-dictionary.enabled"),
    /** 聚类执行摘要。 */
    CLUSTER_SUMMARY("raha.persistence.column-artifact.cluster-summary.enabled"),
    /** 标签传播执行摘要。 */
    PROPAGATION_SUMMARY(
            "raha.persistence.column-artifact.propagation-summary.enabled");

    /** 控制该列级产物是否写入的配置键。 */
    private final String configKey;

    FmdbColumnArtifact(String configKey) {
        this.configKey = ValueUtils.requireNotBlank(configKey, "FMDB 列产物配置键");
    }

    public String getConfigKey() {
        return configKey;
    }
}
