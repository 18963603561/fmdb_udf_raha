package com.fiberhome.ml.raha.parallel;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存并行工作项不含输入数据的失败类型和摘要。
 */
public final class ParallelFailure {

    /** 异常类型名称。 */
    private final String errorType;
    /** 安全失败摘要。 */
    private final String message;
    /** 是否由批次超时产生。 */
    private final boolean timeout;

    public ParallelFailure(String errorType, String message, boolean timeout) {
        this.errorType = ValueUtils.requireNotBlank(errorType, "并行失败类型");
        this.message = ValueUtils.requireNotBlank(message, "并行失败摘要");
        this.timeout = timeout;
    }

    public String getErrorType() { return errorType; }
    public String getMessage() { return message; }
    public boolean isTimeout() { return timeout; }
}
