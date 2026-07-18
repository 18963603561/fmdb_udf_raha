package com.fiberhome.ml.raha.checkpoint;

import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 描述一次阶段任务尝试的业务结果和是否允许重试。
 *
 * @param <T> 成功结果载荷类型
 */
public final class CheckpointTaskResult<T> {

    /** 本次尝试是否成功。 */
    private final boolean succeeded;
    /** 成功时返回给调用方的内存结果。 */
    private final T payload;
    /** 成功输出的逻辑持久化位置。 */
    private final String outputLocation;
    /** 不包含敏感信息的阶段结果摘要。 */
    private final Map<String, String> summary;
    /** 失败错误码。 */
    private final String errorCode;
    /** 失败安全摘要。 */
    private final String errorMessage;
    /** 失败后是否允许重新执行。 */
    private final boolean recoverable;

    private CheckpointTaskResult(boolean succeeded,
                                 T payload,
                                 String outputLocation,
                                 Map<String, String> summary,
                                 String errorCode,
                                 String errorMessage,
                                 boolean recoverable) {
        if (succeeded && (payload == null || isBlank(outputLocation))) {
            throw new IllegalArgumentException("成功任务必须包含结果载荷和输出位置");
        }
        if (!succeeded && (isBlank(errorCode) || isBlank(errorMessage))) {
            throw new IllegalArgumentException("失败任务必须包含错误码和错误摘要");
        }
        this.succeeded = succeeded;
        this.payload = payload;
        this.outputLocation = outputLocation;
        this.summary = summary == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(summary));
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.recoverable = recoverable;
    }

    /**
     * 创建成功结果。
     */
    public static <T> CheckpointTaskResult<T> success(T payload,
                                                       String outputLocation,
                                                       Map<String, String> summary) {
        return new CheckpointTaskResult<T>(true, payload,
                ValueUtils.requireNotBlank(outputLocation, "阶段输出位置"), summary,
                null, null, false);
    }

    /**
     * 创建失败结果，由调用方明确指定是否可恢复。
     */
    public static <T> CheckpointTaskResult<T> failure(String errorCode,
                                                       String errorMessage,
                                                       boolean recoverable,
                                                       Map<String, String> summary) {
        return new CheckpointTaskResult<T>(false, null, null, summary,
                ValueUtils.requireNotBlank(errorCode, "阶段错误码"),
                ValueUtils.requireNotBlank(errorMessage, "阶段错误摘要"), recoverable);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public boolean isSucceeded() { return succeeded; }
    public T getPayload() { return payload; }
    public String getOutputLocation() { return outputLocation; }
    public Map<String, String> getSummary() { return summary; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public boolean isRecoverable() { return recoverable; }
}
