package com.fiberhome.ml.raha.error;

/**
 * Raha 统一业务异常，携带稳定错误编码、分类和可恢复标识。
 */
public class RahaException extends RuntimeException {

    /** 稳定错误编码。 */
    private final RahaErrorCode errorCode;
    /** 是否允许任务重试或按阈值继续。 */
    private final boolean recoverable;

    public RahaException(RahaErrorCode errorCode, String message, boolean recoverable) {
        super(message);
        if (errorCode == null) {
            throw new IllegalArgumentException("错误编码不能为空");
        }
        this.errorCode = errorCode;
        this.recoverable = recoverable;
    }

    public RahaException(RahaErrorCode errorCode,
                         String message,
                         boolean recoverable,
                         Throwable cause) {
        super(message, cause);
        if (errorCode == null) {
            throw new IllegalArgumentException("错误编码不能为空");
        }
        this.errorCode = errorCode;
        this.recoverable = recoverable;
    }

    public RahaErrorCode getErrorCode() { return errorCode; }
    public RahaErrorCategory getCategory() { return errorCode.getCategory(); }
    public boolean isRecoverable() { return recoverable; }
}
