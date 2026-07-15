package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存 UDF 参数或提交失败的稳定错误码和安全摘要。
 */
public final class RahaUdfException extends RuntimeException {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 稳定错误码。 */
    private final String errorCode;

    public RahaUdfException(String errorCode, String message) {
        super(ValueUtils.requireNotBlank(message, "UDF 错误摘要"));
        this.errorCode = ValueUtils.requireNotBlank(errorCode, "UDF 错误码");
    }

    public RahaUdfException(String errorCode, String message, Throwable cause) {
        super(ValueUtils.requireNotBlank(message, "UDF 错误摘要"), cause);
        this.errorCode = ValueUtils.requireNotBlank(errorCode, "UDF 错误码");
    }

    public String getErrorCode() {
        return errorCode;
    }
}
