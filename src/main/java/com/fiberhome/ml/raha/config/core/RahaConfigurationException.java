package com.fiberhome.ml.raha.config.core;

/**
 * 表示资源配置缺失、格式非法或外部配置文件读取失败。
 */
public final class RahaConfigurationException extends IllegalStateException {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 发生错误的配置键，文件级错误时为空。 */
    private final String propertyKey;

    public RahaConfigurationException(String propertyKey, String message) {
        super(message);
        this.propertyKey = propertyKey;
    }

    public RahaConfigurationException(String propertyKey,
                                      String message,
                                      Throwable cause) {
        super(message, cause);
        this.propertyKey = propertyKey;
    }

    public String getPropertyKey() {
        return propertyKey;
    }
}
