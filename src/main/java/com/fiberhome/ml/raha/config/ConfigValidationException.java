package com.fiberhome.ml.raha.config;

/**
 * 表示任务配置不满足运行约束。
 */
public class ConfigValidationException extends IllegalArgumentException {

    /** 配置错误编码。 */
    private final ConfigErrorCode errorCode;

    /**
     * 创建配置校验异常。
     *
     * @param errorCode 错误编码
     * @param message 中文错误说明
     */
    public ConfigValidationException(ConfigErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ConfigErrorCode getErrorCode() {
        return errorCode;
    }
}
