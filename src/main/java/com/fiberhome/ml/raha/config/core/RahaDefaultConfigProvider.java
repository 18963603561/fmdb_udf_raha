package com.fiberhome.ml.raha.config.core;

import com.fiberhome.ml.raha.config.validation.RahaConfigFactory;

/**
 * 延迟加载并共享类路径默认配置和配置工厂。
 */
public final class RahaDefaultConfigProvider {

    private RahaDefaultConfigProvider() {
    }

    public static RahaConfigFactory factory() {
        return Holder.FACTORY;
    }

    public static RahaProperties properties() {
        return Holder.PROPERTIES;
    }

    /**
     * 使用静态内部类保证首次访问时才读取配置文件。
     */
    private static final class Holder {
        /** 合并类路径、外部文件和系统属性后的默认配置。 */
        private static final RahaProperties PROPERTIES = new RahaConfigLoader().load();
        /** 基于默认配置的共享工厂。 */
        private static final RahaConfigFactory FACTORY =
                new RahaConfigFactory(PROPERTIES);
    }
}
