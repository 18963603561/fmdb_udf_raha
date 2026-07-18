package com.fiberhome.ml.raha.config;

import com.fiberhome.ml.raha.util.HashUtils;

/**
 * 根据完整任务配置生成可复现的配置版本。
 */
public final class ConfigVersioner {

    /** 配置校验器，确保非法配置不会生成可用版本。 */
    private final RahaConfigValidator validator;

    public ConfigVersioner() {
        this(new RahaConfigValidator());
    }

    public ConfigVersioner(RahaConfigValidator validator) {
        if (validator == null) {
            throw new IllegalArgumentException("配置校验器不能为空");
        }
        this.validator = validator;
    }

    /**
     * 生成任务配置版本。
     *
     * @param config 已完整填写的任务配置
     * @return SHA-256 配置版本
     */
    public String versionOf(RahaJobConfig config) {
        // 只有校验通过的配置才允许进入任务幂等和阶段复用逻辑。
        validator.validate(config);
        return HashUtils.sha256Hex(config.toCanonicalString());
    }
}
