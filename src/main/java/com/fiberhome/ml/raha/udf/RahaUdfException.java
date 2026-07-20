package com.fiberhome.ml.raha.udf;

/**
 * 表示可稳定返回给 UDF 调用方的业务错误。
 */
public final class RahaUdfException extends RuntimeException {

    /** 稳定错误码，取值遵循方案文档错误码约定。 */
    private final String errorCode;

    public RahaUdfException(String errorCode, String message) {
        super(message);
        if (errorCode == null || errorCode.trim().isEmpty()) {
            throw new IllegalArgumentException("UDF 错误码不能为空");
        }
        this.errorCode = errorCode;
    }

    public RahaUdfException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        if (errorCode == null || errorCode.trim().isEmpty()) {
            throw new IllegalArgumentException("UDF 错误码不能为空");
        }
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
