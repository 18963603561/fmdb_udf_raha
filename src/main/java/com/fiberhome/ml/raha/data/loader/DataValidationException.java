package com.fiberhome.ml.raha.data.loader;

/**
 * 表示输入数据或行标识不满足检测运行要求。
 */
public final class DataValidationException extends RuntimeException {

    /** 数据校验错误编码。 */
    private final DataValidationErrorCode errorCode;

    public DataValidationException(DataValidationErrorCode errorCode, String message) {
        super(message);
        if (errorCode == null) {
            throw new IllegalArgumentException("数据校验错误编码不能为空");
        }
        this.errorCode = errorCode;
    }

    public DataValidationException(DataValidationErrorCode errorCode,
                                   String message,
                                   Throwable cause) {
        super(message, cause);
        if (errorCode == null) {
            throw new IllegalArgumentException("数据校验错误编码不能为空");
        }
        this.errorCode = errorCode;
    }

    public DataValidationErrorCode getErrorCode() {
        return errorCode;
    }
}

