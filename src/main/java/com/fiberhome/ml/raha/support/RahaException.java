package com.fiberhome.ml.raha.support;

/**
 * Raha 同步用例异常，携带稳定错误码和原始异常链。
 */
public final class RahaException extends RuntimeException {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 稳定错误码。 */
    private final RahaErrorCode errorCode;

    public RahaException(RahaErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public RahaException(RahaErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public RahaErrorCode getErrorCode() {
        return errorCode;
    }
}
